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

type peer struct {
	Cfg *config.Peer
	BGP session
}

type bgpController struct {
	logger          log.Logger
	myNode          string
	nodeAnnotations labels.Set
	nodeLabels      labels.Set
	cfg             *config.Config
	peers           []*peer
	svcAds          map[string][]*bgp.Advertisement
}

func (c *bgpController) SetConfig(l log.Logger, cfg *config.Config) error {
	if reflect.DeepEqual(c.cfg, cfg) {
		return nil
	}

	c.cfg = cfg

	l.Log("event", "configChanged", "msg", "config changed, resyncing BGP peers")
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

// syncPeers updates the BGP peer list based
func (c *bgpController) syncPeers(l log.Logger) error {
	cfg := c.cfg

	if cfg == nil {
		return nil
	}

	newPeers := make([]*peer, 0, len(cfg.Peers))
newPeers:
	for _, p := range cfg.Peers {
		for i, ep := range c.peers {
			if ep == nil {
				continue
			}
			if reflect.DeepEqual(p, ep.Cfg) {
				newPeers = append(newPeers, ep)
				c.peers[i] = nil
				continue newPeers
			}
		}
		// No existing peers match, create a new one.
		l.Log(
			"event", "peerConfigured",
			"localASN", p.MyASN,
			"peerASN", p.ASN,
			"peerAddress", p.Addr,
			"port", p.Port,
			"holdTime", p.HoldTime,
			"routerID", p.RouterID,
			"msg", "adding new peer configuration",
		)
		newPeers = append(newPeers, &peer{
			Cfg: p,
		})
	}

	oldPeers := c.peers
	c.peers = newPeers

	if np := c.discoverNodePeer(l); np != nil {
		// Add node peer to slice only if no identical peer exists with a node
		// selector that matches this node.
		if len(c.peers) == 0 {
			c.peers = append(c.peers, np)
		} else {
			var exists bool
			for _, p := range c.peers {
				if isSameSession(np.Cfg, p.Cfg, c.nodeLabels) {
					exists = true
					break
				}
			}

			if !exists {
				c.peers = append(c.peers, np)
			}
		}
	}

	for _, p := range oldPeers {
		if p == nil {
			continue
		}
		l.Log("event", "peerDeconfigured", "peer", p.Cfg.Addr, "reason", "removedFromConfig", "msg", "peer deconfigured, closing BGP session")
		if p.BGP != nil {
			if err := p.BGP.Close(); err != nil {
				l.Log("op", "setConfig", "error", err, "peer", p.Cfg.Addr, "msg", "failed to shut down BGP session")
			}
		}
	}

	return c.updatePeers(l)
}

// updatePeers is called when either the peer list or node annotations/labels
// have changed, implying that the set of running BGP sessions may need
// tweaking.
func (c *bgpController) updatePeers(l log.Logger) error {
	var totalErrs int
	var needUpdateAds int

	// Update peer BGP sessions.
	update, errs := c.syncBGPSessions(l, c.peers)
	needUpdateAds += update
	totalErrs += errs
	l.Log("op", "syncBGPSessions", "needUpdate", update, "errs", errs, "msg", "done syncing peer BGP sessions")

	if needUpdateAds > 0 {
		// Some new sessions came up, resync advertisement state.
		if err := c.updateAds(); err != nil {
			l.Log("op", "updateAds", "error", err, "msg", "failed to update BGP advertisements")
			return err
		}
	}
	if totalErrs > 0 {
		return fmt.Errorf("%d BGP sessions failed to start", errs)
	}
	return nil
}

// discoverNodePeer attempts to create a BGP peer from node annotations and/or
// labels if peer autodiscovery is configured. If a peer is successfully
// discovered, a pointer to it is returned.
func (c *bgpController) discoverNodePeer(l log.Logger) *peer {
	pad := c.cfg.PeerAutodiscovery

	if pad == nil {
		l.Log("op", "discoverNodePeer", "msg", "peer autodiscovery disabled")
		return nil
	}

	// If node labels don't match any peer autodiscovery node selector, we
	// shouldn't try to discover a peer for this node.
	if !selectorMatches(c.nodeLabels, pad.NodeSelectors) {
		// Peer autodiscovery is disabled for this node. If a node peer exists
		// for this node, remove it.
		l.Log("op", "discoverNodePeer", "msg", "node labels don't match autodiscovery selectors")
		return nil
	}

	// Parse (or re-parse) node peer configuration from the Node object.
	np, err := parseNodePeer(l, pad, c.nodeAnnotations, c.nodeLabels)
	if err != nil {
		// Node has invalid/partial/missing peer config.
		l.Log("op", "discoverNodePeer", "error", err, "msg", "no peer discovered")
		return nil
	}

	return np
}

func (c *bgpController) syncBGPSessions(l log.Logger, peers []*peer) (needUpdateAds int, errs int) {
	for _, p := range peers {
		// First, determine if the peering should be active for this
		// node.
		shouldRun := false
		for _, ns := range p.Cfg.NodeSelectors {
			if ns.Matches(c.nodeLabels) {
				shouldRun = true
				break
			}
		}

		// Now, compare current state to intended state, and correct.
		if p.BGP != nil && !shouldRun {
			// Oops, session is running but shouldn't be. Shut it down.
			l.Log("event", "peerRemoved", "peer", p.Cfg.Addr, "reason", "filteredByNodeSelector", "msg", "peer deconfigured, closing BGP session")
			if err := p.BGP.Close(); err != nil {
				l.Log("op", "syncBGPSessions", "error", err, "peer", p.Cfg.Addr, "msg", "failed to shut down BGP session")
			}
			p.BGP = nil
		} else if p.BGP == nil && shouldRun {
			// Session doesn't exist, but should be running. Create
			// it.
			l.Log(
				"event", "peerAdded",
				"localASN", p.Cfg.MyASN,
				"peerASN", p.Cfg.ASN,
				"peerAddress", p.Cfg.Addr,
				"port", p.Cfg.Port,
				"holdTime", p.Cfg.HoldTime,
				"routerID", p.Cfg.RouterID,
				"msg", "peer added, starting BGP session",
			)
			var routerID net.IP
			if p.Cfg.RouterID != nil {
				routerID = p.Cfg.RouterID
			}
			s, err := newBGP(c.logger, net.JoinHostPort(p.Cfg.Addr.String(), strconv.Itoa(int(p.Cfg.Port))), p.Cfg.MyASN, routerID, p.Cfg.ASN, p.Cfg.HoldTime, p.Cfg.Password, c.myNode)
			if err != nil {
				l.Log("op", "syncBGPSessions", "error", err, "peer", p.Cfg.Addr, "msg", "failed to create BGP session")
				errs++
			} else {
				p.BGP = s
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
		if peer.BGP == nil {
			continue
		}
		if err := peer.BGP.Set(allAds...); err != nil {
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
	ls := labels.Set(nodeLabels)
	annotationsUnchanged := c.nodeAnnotations != nil && labels.Equals(c.nodeAnnotations, anns)
	labelsUnchanged := c.nodeLabels != nil && labels.Equals(c.nodeLabels, ls)
	if labelsUnchanged && annotationsUnchanged {
		// Node labels and annotations unchanged, no action required.
		return nil
	}
	c.nodeAnnotations = anns
	c.nodeLabels = ls

	l.Log("event", "nodeChanged", "msg", "node changed, resyncing BGP peers")
	return c.syncPeers(l)
}

// parseNodePeer attempts to construct a BGP peer from information conveyed
// in node annotations and labels using the specified autodiscovery
// configuration.
func parseNodePeer(l log.Logger, pad *config.PeerAutodiscovery, anns labels.Set, ls labels.Set) (*peer, error) {
	var (
		myASN       uint32
		peerASN     uint32
		peerAddr    net.IP
		peerPort    uint16
		holdTime    time.Duration
		holdTimeRaw string
		routerID    net.IP
	)

	// Hardcoded defaults. Used only if a parameter isn't specified in the peer
	// autodiscovery defaults and also not in annotations/labels.
	peerPort = 179
	holdTime = 90 * time.Second

	// Method called with a nil or empty peer autodiscovery.
	if pad == nil {
		return nil, errors.New("nil peer autodiscovery")
	}

	// Set defaults. Parameter values read from labels/annotations override the
	// values set here.
	if pad.Defaults != nil {
		if pad.Defaults.ASN != 0 {
			peerASN = pad.Defaults.ASN
		}
		if pad.Defaults.MyASN != 0 {
			myASN = pad.Defaults.MyASN
		}
		if pad.Defaults.Address != nil {
			peerAddr = pad.Defaults.Address
		}
		if pad.Defaults.Port != 0 {
			peerPort = pad.Defaults.Port
		}
		if pad.Defaults.HoldTime != 0 {
			holdTime = pad.Defaults.HoldTime
		}
	}

	if pad.FromLabels != nil {
		for k, v := range ls {
			switch k {
			case pad.FromLabels.MyASN:
				asn, err := strconv.ParseUint(v, 10, 32)
				if err != nil {
					return nil, fmt.Errorf("parsing local ASN: %v", err)
				}
				myASN = uint32(asn)
			case pad.FromLabels.ASN:
				asn, err := strconv.ParseUint(v, 10, 32)
				if err != nil {
					return nil, fmt.Errorf("parsing peer ASN: %v", err)
				}
				peerASN = uint32(asn)
			case pad.FromLabels.Addr:
				peerAddr = net.ParseIP(v)
				if peerAddr == nil {
					return nil, fmt.Errorf("invalid peer IP %q", v)
				}
			case pad.FromLabels.Port:
				port, err := strconv.ParseUint(v, 10, 16)
				if err != nil {
					return nil, fmt.Errorf("parsing peer port: %v", err)
				}
				peerPort = uint16(port)
			case pad.FromLabels.HoldTime:
				holdTimeRaw = v
			case pad.FromLabels.RouterID:
				routerID = net.ParseIP(v)
				if routerID == nil {
					return nil, fmt.Errorf("invalid router ID %q", v)
				}
			}
		}
	}

	if pad.FromAnnotations != nil {
		for k, v := range anns {
			switch k {
			case pad.FromAnnotations.MyASN:
				asn, err := strconv.ParseUint(v, 10, 32)
				if err != nil {
					return nil, fmt.Errorf("parsing local ASN: %v", err)
				}
				myASN = uint32(asn)
			case pad.FromAnnotations.ASN:
				asn, err := strconv.ParseUint(v, 10, 32)
				if err != nil {
					return nil, fmt.Errorf("parsing peer ASN: %v", err)
				}
				peerASN = uint32(asn)
			case pad.FromAnnotations.Addr:
				peerAddr = net.ParseIP(v)
				if peerAddr == nil {
					return nil, fmt.Errorf("invalid peer IP %q", v)
				}
			case pad.FromAnnotations.Port:
				port, err := strconv.ParseUint(v, 10, 16)
				if err != nil {
					return nil, fmt.Errorf("parsing peer port: %v", err)
				}
				peerPort = uint16(port)
			case pad.FromAnnotations.HoldTime:
				holdTimeRaw = v
			case pad.FromAnnotations.RouterID:
				routerID = net.ParseIP(v)
				if routerID == nil {
					return nil, fmt.Errorf("invalid router ID %q", v)
				}
			}
		}
	}

	if myASN == 0 {
		return nil, errors.New("missing or invalid local ASN")
	}
	if peerASN == 0 {
		return nil, errors.New("missing or invalid peer ASN")
	}
	if peerAddr == nil {
		return nil, errors.New("missing or invalid peer address")
	}

	if holdTimeRaw != "" {
		ht, err := config.ParseHoldTime(holdTimeRaw)
		if err != nil {
			return nil, fmt.Errorf("parsing hold time: %v", err)
		}
		holdTime = ht
	}

	// The peer is configured on a specific node object, so we want to create a
	// BGP session only on that node.
	h := ls[v1.LabelHostname]
	if h == "" {
		return nil, fmt.Errorf("label %s not found on node", v1.LabelHostname)
	}
	ns, err := labels.Parse(fmt.Sprintf("%s=%s", v1.LabelHostname, h))
	if err != nil {
		return nil, fmt.Errorf("parsing node selector: %v", err)
	}

	p := &peer{
		Cfg: &config.Peer{
			MyASN:         myASN,
			ASN:           peerASN,
			Addr:          peerAddr,
			Port:          peerPort,
			HoldTime:      holdTime,
			RouterID:      routerID,
			NodeSelectors: []labels.Selector{ns},
			// BGP passwords aren't supported for node peers.
			Password: "",
		},
	}

	return p, nil
}

// isSameSession returns true if the peer configurations a and b would result
// in the same TCP session on a node.
//
// Two configs would result in the same TCP session if the node selectors of
// both a and b match the label set ls AND both the peer address and port are
// identical between a and b.
//
// TODO: Consider allowing multiple sessions to the same destination IP with
// different source IPs.
func isSameSession(a *config.Peer, b *config.Peer, ls labels.Set) bool {
	if !selectorMatches(ls, a.NodeSelectors) || !selectorMatches(ls, b.NodeSelectors) {
		return false
	}
	if !a.Addr.Equal(b.Addr) {
		return false
	}
	if a.Port != b.Port {
		return false
	}

	return true
}

// selectorMatches returns true if the label set ls matches any of the
// selectors specified in sel.
func selectorMatches(ls labels.Set, sel []labels.Selector) bool {
	for _, s := range sel {
		if s.Matches(ls) {
			return true
		}
	}
	return false
}

var newBGP = func(logger log.Logger, addr string, myASN uint32, routerID net.IP, asn uint32, hold time.Duration, password string, myNode string) (session, error) {
	return bgp.New(logger, addr, myASN, routerID, asn, hold, password, myNode)
}
