// Copyright 2017 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"sort"
	"strconv"
	"time"

	"go.universe.tf/metallb/internal/bgp"
	"go.universe.tf/metallb/internal/config"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/go-kit/kit/log"
)

const (
	// K8s annotations which express node-specific BGP peer configuration.
	annotationHoldTime = "metallb.universe.tf/hold-time"
	annotationMyASN    = "metallb.universe.tf/my-asn"
	// TODO: Using this annotation will display the password in clear text
	// on the Node object.
	annotationPassword    = "metallb.universe.tf/password"
	annotationPeerAddress = "metallb.universe.tf/peer-address"
	annotationPeerASN     = "metallb.universe.tf/peer-asn"
	annotationPeerPort    = "metallb.universe.tf/peer-port"
	annotationRouterID    = "metallb.universe.tf/router-id"

	// K8s labels which express node-specific BGP peer configuration.
	labelHoldTime = "metallb.universe.tf/hold-time"
	labelMyASN    = "metallb.universe.tf/my-asn"
	// TODO: Using this label will display the password in clear text
	// on the Node object.
	labelPassword    = "metallb.universe.tf/password"
	labelPeerAddress = "metallb.universe.tf/peer-address"
	labelPeerASN     = "metallb.universe.tf/peer-asn"
	labelPeerPort    = "metallb.universe.tf/peer-port"
	labelRouterID    = "metallb.universe.tf/router-id"
)

type peer struct {
	cfg *config.Peer
	bgp session
}

type bgpController struct {
	logger          log.Logger
	myNode          string
	nodeAnnotations labels.Set
	nodeLabels      labels.Set
	nodePeers       map[string]*peer
	peers           []*peer
	svcAds          map[string][]*bgp.Advertisement
}

func (c *bgpController) SetConfig(l log.Logger, cfg *config.Config) error {
	newPeers := make([]*peer, 0, len(cfg.Peers))
newPeers:
	for _, p := range cfg.Peers {
		for i, ep := range c.peers {
			if ep == nil {
				continue
			}
			if reflect.DeepEqual(p, ep.cfg) {
				newPeers = append(newPeers, ep)
				c.peers[i] = nil
				continue newPeers
			}
		}
		// No existing peers match, create a new one.
		newPeers = append(newPeers, &peer{
			cfg: p,
		})
	}

	oldPeers := c.peers
	c.peers = newPeers

	for _, p := range oldPeers {
		if p == nil {
			continue
		}
		l.Log("event", "peerRemoved", "peer", p.cfg.Addr, "reason", "removedFromConfig", "msg", "peer deconfigured, closing BGP session")
		if p.bgp != nil {
			if err := p.bgp.Close(); err != nil {
				l.Log("op", "setConfig", "error", err, "peer", p.cfg.Addr, "msg", "failed to shut down BGP session")
			}
		}
	}

	return c.syncPeers(l)
}

// nodeHasHealthyEndpoint return true if this node has at least one healthy endpoint.
func nodeHasHealthyEndpoint(eps *v1.Endpoints, node string) bool {
	ready := map[string]bool{}
	for _, subset := range eps.Subsets {
		for _, ep := range subset.Addresses {
			if ep.NodeName == nil || *ep.NodeName != node {
				continue
			}
			if _, ok := ready[ep.IP]; !ok {
				// Only set true if nothing else has expressed an
				// opinion. This means that false will take precedence
				// if there's any unready ports for a given endpoint.
				ready[ep.IP] = true
			}
		}
		for _, ep := range subset.NotReadyAddresses {
			ready[ep.IP] = false
		}
	}

	for _, r := range ready {
		if r {
			// At least one fully healthy endpoint on this machine.
			return true
		}
	}
	return false
}

func healthyEndpointExists(eps *v1.Endpoints) bool {
	ready := map[string]bool{}
	for _, subset := range eps.Subsets {
		for _, ep := range subset.Addresses {
			if _, ok := ready[ep.IP]; !ok {
				// Only set true if nothing else has expressed an
				// opinion. This means that false will take precedence
				// if there's any unready ports for a given endpoint.
				ready[ep.IP] = true
			}
		}
		for _, ep := range subset.NotReadyAddresses {
			ready[ep.IP] = false
		}
	}

	for _, r := range ready {
		if r {
			// At least one fully healthy endpoint on this machine.
			return true
		}
	}
	return false
}

func (c *bgpController) ShouldAnnounce(l log.Logger, name string, svc *v1.Service, eps *v1.Endpoints) string {
	// Should we advertise?
	// Yes, if externalTrafficPolicy is
	//  Cluster && any healthy endpoint exists
	// or
	//  Local && there's a ready local endpoint.
	if svc.Spec.ExternalTrafficPolicy == v1.ServiceExternalTrafficPolicyTypeLocal && !nodeHasHealthyEndpoint(eps, c.myNode) {
		return "noLocalEndpoints"
	} else if !healthyEndpointExists(eps) {
		return "noEndpoints"
	}
	return ""
}

// Called when either the peer list or node labels have changed,
// implying that the set of running BGP sessions may need tweaking.
func (c *bgpController) syncPeers(l log.Logger) error {
	var totalErrs int
	var needUpdateAds int

	// Update peer BGP sessions.
	update, errs := c.syncBGPSessions(l, c.peers)
	needUpdateAds += update
	totalErrs += errs
	l.Log("op", "syncBGPSessions", "needUpdate", update, "errs", errs, "msg", "done syncing peer BGP sessions")

	// Update node peer BGP sessions.
	var np []*peer
	for _, p := range c.nodePeers {
		np = append(np, p)
	}
	update, errs = c.syncBGPSessions(l, np)
	needUpdateAds += update
	totalErrs += errs
	l.Log("op", "syncBGPSessions", "needUpdate", update, "errs", errs, "msg", "done syncing node peer BGP sessions")

	if needUpdateAds > 0 {
		// Some new sessions came up, resync advertisement state.
		if err := c.updateAds(); err != nil {
			l.Log("op", "updateAds", "error", err, "msg", "failed to update BGP advertisements")
			return err
		}
	}
	if errs > 0 {
		return fmt.Errorf("%d BGP sessions failed to start", errs)
	}
	return nil
}

func (c *bgpController) syncBGPSessions(l log.Logger, peers []*peer) (needUpdateAds int, errs int) {
	for _, p := range peers {
		// First, determine if the peering should be active for this
		// node.
		shouldRun := false
		for _, ns := range p.cfg.NodeSelectors {
			if ns.Matches(c.nodeLabels) {
				shouldRun = true
				break
			}
		}

		// Now, compare current state to intended state, and correct.
		if p.bgp != nil && !shouldRun {
			// Oops, session is running but shouldn't be. Shut it down.
			l.Log("event", "peerRemoved", "peer", p.cfg.Addr, "reason", "filteredByNodeSelector", "msg", "peer deconfigured, closing BGP session")
			if err := p.bgp.Close(); err != nil {
				l.Log("op", "syncBGPSessions", "error", err, "peer", p.cfg.Addr, "msg", "failed to shut down BGP session")
			}
			p.bgp = nil
		} else if p.bgp == nil && shouldRun {
			// Session doesn't exist, but should be running. Create
			// it.
			l.Log("event", "peerAdded", "peer", p.cfg.Addr, "msg", "peer configured, starting BGP session")
			var routerID net.IP
			if p.cfg.RouterID != nil {
				routerID = p.cfg.RouterID
			}
			s, err := newBGP(c.logger, net.JoinHostPort(p.cfg.Addr.String(), strconv.Itoa(int(p.cfg.Port))), p.cfg.MyASN, routerID, p.cfg.ASN, p.cfg.HoldTime, p.cfg.Password, c.myNode)
			if err != nil {
				l.Log("op", "syncBGPSessions", "error", err, "peer", p.cfg.Addr, "msg", "failed to create BGP session")
				errs++
			} else {
				p.bgp = s
				needUpdateAds++
			}
		}
	}
	return
}

func (c *bgpController) SetBalancer(l log.Logger, name string, lbIP net.IP, pool *config.Pool) error {
	c.svcAds[name] = nil
	for _, adCfg := range pool.BGPAdvertisements {
		m := net.CIDRMask(adCfg.AggregationLength, 32)
		ad := &bgp.Advertisement{
			Prefix: &net.IPNet{
				IP:   lbIP.Mask(m),
				Mask: m,
			},
			LocalPref: adCfg.LocalPref,
		}
		for comm := range adCfg.Communities {
			ad.Communities = append(ad.Communities, comm)
		}
		sort.Slice(ad.Communities, func(i, j int) bool { return ad.Communities[i] < ad.Communities[j] })
		c.svcAds[name] = append(c.svcAds[name], ad)
	}

	if err := c.updateAds(); err != nil {
		return err
	}

	l.Log("event", "updatedAdvertisements", "numAds", len(c.svcAds[name]), "msg", "making advertisements using BGP")

	return nil
}

func (c *bgpController) updateAds() error {
	var allAds []*bgp.Advertisement
	for _, ads := range c.svcAds {
		// This list might contain duplicates, but that's fine,
		// they'll get compacted by the session code when it's
		// calculating advertisements.
		//
		// TODO: be more intelligent about compacting advertisements
		// and detecting conflicting advertisements.
		allAds = append(allAds, ads...)
	}
	for _, peer := range c.peers {
		if peer.bgp == nil {
			continue
		}
		if err := peer.bgp.Set(allAds...); err != nil {
			return err
		}
	}
	for _, peer := range c.nodePeers {
		if peer.bgp == nil {
			continue
		}
		if err := peer.bgp.Set(allAds...); err != nil {
			return err
		}
	}
	return nil
}

func (c *bgpController) DeleteBalancer(l log.Logger, name, reason string) error {
	if _, ok := c.svcAds[name]; !ok {
		return nil
	}
	delete(c.svcAds, name)
	return c.updateAds()
}

type session interface {
	io.Closer
	Set(advs ...*bgp.Advertisement) error
}

func (c *bgpController) SetLeader(log.Logger, bool) {}

func (c *bgpController) SetNode(l log.Logger, node *v1.Node) error {
	nodeAnnotations := node.Annotations
	if nodeAnnotations == nil {
		nodeAnnotations = map[string]string{}
	}
	nodeLabels := node.Labels
	if nodeLabels == nil {
		nodeLabels = map[string]string{}
	}

	anns := labels.Set(nodeAnnotations)
	ns := labels.Set(nodeLabels)
	annotationsUnchanged := c.nodeAnnotations != nil && labels.Equals(c.nodeAnnotations, anns)
	labelsUnchanged := c.nodeLabels != nil && labels.Equals(c.nodeLabels, ns)
	if labelsUnchanged && annotationsUnchanged {
		// Node labels and annotations unchanged, no action required.
		return nil
	}
	c.nodeAnnotations = anns
	c.nodeLabels = ns

	// Attempt to create a BGP peer from node labels.
	p, err := peerFromLabels(l, node)
	ep, peerExists := c.nodePeers[node.Name]
	if err != nil {
		// Node has invalid/partial/missing peer config. If a BGP
		// peer exists for this node, we need to remove it.
		l.Log("op", "setNode", "error", err, "msg", "invalid node peer config")
		if peerExists {
			l.Log("op", "setNode", "node", node.Name, "msg", "removing old node peer")
			if ep.bgp != nil {
				if err := ep.bgp.Close(); err != nil {
					l.Log("op", "setNode", "error", err, "peer", ep.cfg.Addr, "msg", "failed to shut down BGP session")
				}
			}
			delete(c.nodePeers, node.Name)
		}
	} else {
		// Valid config.
		if peerExists && !reflect.DeepEqual(ep.cfg, p.cfg) {
			// Existing peer has an outdated config. Update it.
			l.Log("op", "setNode", "node", node.Name, "msg", "removing outdated node peer")
			if ep.bgp != nil {
				if err := ep.bgp.Close(); err != nil {
					l.Log("op", "setNode", "error", err, "peer", ep.cfg.Addr, "msg", "failed to shut down BGP session")
				}
			}
			c.nodePeers[node.Name] = p
		} else {
			// Peer doesn't exist. Create it.
			l.Log("op", "setNode", "node", node.Name, "msg", "creating node peer")
			c.nodePeers[node.Name] = p
		}
	}

	l.Log("event", "nodeChanged", "msg", "Node changed, resyncing BGP peers")
	return c.syncPeers(l)
}

// peerFromLabels looks for labels on a node and attempts to create a
// BGP peer from them.
func peerFromLabels(l log.Logger, node *v1.Node) (*peer, error) {
	var (
		myASN       uint32
		peerASN     uint32
		peerAddr    net.IP
		peerPort    uint16
		holdTime    time.Duration
		holdTimeRaw string
		routerID    net.IP
		password    string
	)

	for k, v := range node.Labels {
		switch k {
		case labelMyASN:
			asn, err := strconv.ParseUint(v, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("parsing local ASN: %v", err)
			}
			myASN = uint32(asn)
		case labelPeerASN:
			asn, err := strconv.ParseUint(v, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("parsing peer ASN: %v", err)
			}
			peerASN = uint32(asn)
		case labelPeerAddress:
			peerAddr = net.ParseIP(v)
			if peerAddr == nil {
				return nil, fmt.Errorf("invalid peer IP %q", v)
			}
		case labelPeerPort:
			port, err := strconv.ParseUint(v, 10, 16)
			if err != nil {
				return nil, fmt.Errorf("parsing peer port: %v", err)
			}
			peerPort = uint16(port)
		case labelHoldTime:
			holdTimeRaw = v
		case labelRouterID:
			routerID = net.ParseIP(v)
			if routerID == nil {
				return nil, fmt.Errorf("invalid router ID %q", v)
			}
		case labelPassword:
			password = v
		}
	}

	// Verify required peer config.
	if peerASN == 0 {
		return nil, errors.New("peer ASN must be set")
	}
	if peerAddr == nil {
		return nil, errors.New("peer address must be set")
	}
	if myASN == 0 {
		return nil, errors.New("local ASN must be set")
	}

	// Set default BGP port if unspecified by user.
	if peerPort == 0 {
		peerPort = 179
	}

	ht, err := parseHoldTime(holdTimeRaw)
	if err != nil {
		return nil, fmt.Errorf("parsing hold time: %v", err)
	}
	holdTime = ht

	// The peer is configured on a specific node object, so we want
	// to create a BGP session only on that node.
	// TODO: Is it legal to have a Node object without a hostname label?
	h := node.Labels[v1.LabelHostname]
	if h == "" {
		return nil, fmt.Errorf("label %s not found on node", v1.LabelHostname)
	}
	ns, err := labels.Parse(fmt.Sprintf("%s=%s", v1.LabelHostname, h))
	if err != nil {
		return nil, fmt.Errorf("parsing node selector: %v", err)
	}

	p := &peer{
		cfg: &config.Peer{
			MyASN:         myASN,
			ASN:           peerASN,
			Addr:          peerAddr,
			Port:          peerPort,
			HoldTime:      holdTime,
			RouterID:      routerID,
			NodeSelectors: []labels.Selector{ns},
			Password:      password,
		},
	}

	return p, nil
}

// peerFromAnnotations looks for annotations on a node and attempts to create a
// BGP peer from them.
func peerFromAnnotations(l log.Logger, node *v1.Node) (*peer, error) {
	var (
		myASN       uint32
		peerASN     uint32
		peerAddr    net.IP
		peerPort    uint16
		holdTime    time.Duration
		holdTimeRaw string
		routerID    net.IP
		password    string
	)

	for k, v := range node.Annotations {
		switch k {
		case annotationMyASN:
			asn, err := strconv.ParseUint(v, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("parsing local ASN: %v", err)
			}
			myASN = uint32(asn)
		case annotationPeerASN:
			asn, err := strconv.ParseUint(v, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("parsing peer ASN: %v", err)
			}
			peerASN = uint32(asn)
		case annotationPeerAddress:
			peerAddr = net.ParseIP(v)
			if peerAddr == nil {
				return nil, fmt.Errorf("invalid peer IP %q", v)
			}
		case annotationPeerPort:
			port, err := strconv.ParseUint(v, 10, 16)
			if err != nil {
				return nil, fmt.Errorf("parsing peer port: %v", err)
			}
			peerPort = uint16(port)
		case annotationHoldTime:
			holdTimeRaw = v
		case annotationRouterID:
			routerID = net.ParseIP(v)
			if routerID == nil {
				return nil, fmt.Errorf("invalid router ID %q", v)
			}
		case annotationPassword:
			password = v
		}
	}

	// Verify required peer config.
	if peerASN == 0 {
		return nil, errors.New("peer ASN must be set")
	}
	if peerAddr == nil {
		return nil, errors.New("peer address must be set")
	}
	if myASN == 0 {
		return nil, errors.New("local ASN must be set")
	}

	// Set default BGP port if unspecified by user.
	if peerPort == 0 {
		peerPort = 179
	}

	ht, err := parseHoldTime(holdTimeRaw)
	if err != nil {
		return nil, fmt.Errorf("parsing hold time: %v", err)
	}
	holdTime = ht

	// The peer is configured on a specific node object, so we want
	// to create a BGP session only on that node.
	// TODO: Is it legal to have a Node object without a hostname label?
	h := node.Labels[v1.LabelHostname]
	if h == "" {
		return nil, fmt.Errorf("label %s not found on node", v1.LabelHostname)
	}
	ns, err := labels.Parse(fmt.Sprintf("%s=%s", v1.LabelHostname, h))
	if err != nil {
		return nil, fmt.Errorf("parsing node selector: %v", err)
	}

	p := &peer{
		cfg: &config.Peer{
			MyASN:         myASN,
			ASN:           peerASN,
			Addr:          peerAddr,
			Port:          peerPort,
			HoldTime:      holdTime,
			RouterID:      routerID,
			NodeSelectors: []labels.Selector{ns},
			Password:      password,
		},
	}

	return p, nil
}

// TODO: Copied as-is from config package. Need to refactor to make this DRY.
func parseHoldTime(ht string) (time.Duration, error) {
	if ht == "" {
		return 90 * time.Second, nil
	}
	d, err := time.ParseDuration(ht)
	if err != nil {
		return 0, fmt.Errorf("invalid hold time %q: %s", ht, err)
	}
	rounded := time.Duration(int(d.Seconds())) * time.Second
	if rounded != 0 && rounded < 3*time.Second {
		return 0, fmt.Errorf("invalid hold time %q: must be 0 or >=3s", ht)
	}
	return rounded, nil
}

var newBGP = func(logger log.Logger, addr string, myASN uint32, routerID net.IP, asn uint32, hold time.Duration, password string, myNode string) (session, error) {
	return bgp.New(logger, addr, myASN, routerID, asn, hold, password, myNode)
}
