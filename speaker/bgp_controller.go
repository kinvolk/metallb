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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
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
	Cfg      *config.Peer
	BGP      session
	NodePeer bool
}

type bgpController struct {
	logger            log.Logger
	myNode            string
	nodeAnnotations   labels.Set
	nodeLabels        labels.Set
	peerAutodiscovery *config.PeerAutodiscovery
	peers             []*peer
	svcAds            map[string][]*bgp.Advertisement
}

func (c *bgpController) SetConfig(l log.Logger, cfg *config.Config) error {
	c.peerAutodiscovery = cfg.PeerAutodiscovery

	newPeers := []*peer{}

	// When no peers are specified in the configuration, we still want to keep
	// any discovered node peers.
	if len(cfg.Peers) == 0 {
		for i, ep := range c.peers {
			if ep.NodePeer {
				newPeers = append(newPeers, ep)
				c.peers[i] = nil
			}
		}
	}

newPeers:
	for _, p := range cfg.Peers {
		for i, ep := range c.peers {
			if ep == nil {
				continue
			}
			if ep.NodePeer {
				if bgpConfigEqual(p, ep.Cfg) {
					// Node peer is identical to current peer. Replace the node
					// peer with a new regular peer.
					newPeers = append(newPeers, &peer{Cfg: ep.Cfg, BGP: ep.BGP})
					c.peers[i] = nil
					continue newPeers
				}
				// Node peer differs from current peer. Keep the node peer and
				// continue iterating over the existing peers.
				newPeers = append(newPeers, ep)
				c.peers[i] = nil
				continue
			}
			if reflect.DeepEqual(p, ep.Cfg) {
				newPeers = append(newPeers, ep)
				c.peers[i] = nil
				continue newPeers
			}
		}
		// No existing peers match, create a new one.
		newPeers = append(newPeers, &peer{
			Cfg: p,
		})
	}

	oldPeers := c.peers
	c.peers = newPeers

	for _, p := range oldPeers {
		if p == nil {
			continue
		}
		l.Log("event", "peerRemoved", "peer", p.Cfg.Addr, "reason", "removedFromConfig", "msg", "peer deconfigured, closing BGP session")
		if p.BGP != nil {
			if err := p.BGP.Close(); err != nil {
				l.Log("op", "setConfig", "error", err, "peer", p.Cfg.Addr, "msg", "failed to shut down BGP session")
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

// Called when either the peer list or node annotations/labels have changed,
// implying that the set of running BGP sessions may need tweaking.
func (c *bgpController) syncPeers(l log.Logger) error {
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

// Attempt to create a BGP peer from node annotations and/or labels if peer
// autodiscovery is configured.
//
// TODO: This method is called only on changes to Node objects. This means that
// when peer autodiscovery config is changed, node peers won't be synced until
// the relevant Node object changes.
func (c *bgpController) discoverNodePeer(l log.Logger, node *v1.Node) {
	if c.peerAutodiscovery == nil {
		l.Log("op", "discoverNodePeer", "node", node.Name, "msg", "peer autodiscovery not configured")
		// TODO: Remove any existing node peer?
		return
	}

	// Parse (or re-parse) node peer configuration from the Node object.
	discovered, err := parseNodePeer(l, c.peerAutodiscovery, node)
	if err != nil {
		// Log an error without returning to let the user know why peer
		// autodiscovery failed for this node. We continue execution
		// because we still want to remove any outdated node peer which may
		// exist.
		l.Log("op", "discoverNodePeer", "node", node.Name, "error", err, "msg", "peer autodiscovery failed")
	}

	var np *peer
	npIndex := -1
	identicalPeerExists := false
	for i, p := range c.peers {
		// If current peer is a node peer, mark it as we may have to update or
		// delete it later.
		if p.NodePeer {
			np = p
			npIndex = i
			continue
		}

		// If a node peer was discovered, check if the current peer has an
		// identical config as the discovered peer.
		if discovered == nil {
			continue
		}
		if bgpConfigEqual(p.Cfg, discovered.Cfg) {
			// Remember that we have a regular peer with an identical config as
			// the discovered node peer. This means we shouldn't create a new
			// node peer (as this would result in a duplicate peer) and we also
			// have to remove the existing node peer, if any.
			identicalPeerExists = true
		}
	}
	nodePeerExists := np != nil

	if discovered == nil {
		// Node has invalid/partial/missing peer config. If a node peer exists
		// for this node, we need to remove it.
		if nodePeerExists {
			l.Log("op", "discoverNodePeer", "node", node.Name, "msg", "removing outdated node peer")
			if np.BGP != nil {
				if err := np.BGP.Close(); err != nil {
					l.Log("op", "discoverNodePeer", "error", err, "peer", np.Cfg.Addr, "msg", "failed to shut down BGP session")
				}
			}
			c.deletePeer(l, npIndex)
		}
		return
	}

	if identicalPeerExists {
		// We have a regular peer whose config is identical to the discovered
		// node peer config.
		if nodePeerExists {
			l.Log("op", "discoverNodePeer", "node", node.Name, "msg", "node peer is identical to another peer - removing node peer")
			c.deletePeer(l, npIndex)
		}

		// Not creating a new node peer as this would duplicate an existing
		// regular peer.
		return
	}

	if nodePeerExists {
		if !reflect.DeepEqual(np.Cfg, discovered.Cfg) {
			// The discovered node peer differs from the existing node peer.

			// Node peer has an outdated config. Update it.
			l.Log("op", "discoverNodePeer", "node", node.Name, "msg", "updating node peer config")
			if np.BGP != nil {
				if err := np.BGP.Close(); err != nil {
					l.Log("op", "discoverNodePeer", "error", err, "peer", np.Cfg.Addr, "msg", "failed to shut down BGP session")
				}
			}
			c.peers[npIndex] = discovered
		}

		// Node peer config is up to date. Nothing to do.
		return
	}

	// Peer doesn't exist. Create it.
	l.Log("op", "discoverNodePeer", "node", node.Name, "msg", "creating node peer")
	c.peers = append(c.peers, discovered)
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
			l.Log("event", "peerAdded", "peer", p.Cfg.Addr, "msg", "peer configured, starting BGP session")
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
	ls := labels.Set(nodeLabels)
	annotationsUnchanged := c.nodeAnnotations != nil && labels.Equals(c.nodeAnnotations, anns)
	labelsUnchanged := c.nodeLabels != nil && labels.Equals(c.nodeLabels, ls)
	if labelsUnchanged && annotationsUnchanged {
		// Node labels and annotations unchanged, no action required.
		return nil
	}
	c.nodeAnnotations = anns
	c.nodeLabels = ls

	c.discoverNodePeer(l, node)

	l.Log("event", "nodeChanged", "msg", "Node changed, resyncing BGP peers")
	return c.syncPeers(l)
}

func (c *bgpController) StatsHandler() func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Copy peers slice. We want to redact BGP passwords without modifying
		// the actual peers.
		peers := []peer{}
		for _, p := range c.peers {
			cp := peer{}

			cp.BGP = p.BGP
			cp.NodePeer = p.NodePeer

			cfgCopy := *p.Cfg
			cp.Cfg = &cfgCopy

			cp.Cfg.Password = "REDACTED"

			peers = append(peers, cp)
		}

		res := struct{ Peers []peer }{Peers: peers}
		j, err := json.MarshalIndent(res, "", "  ")
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to get stats: %s", err), 500)
			return
		}
		fmt.Fprint(w, string(j))
	}
}

// Removes the peer at index i from the peer list.
func (c *bgpController) deletePeer(l log.Logger, i int) {
	if i >= len(c.peers) {
		l.Log("op", "deletePeer", "msg", fmt.Sprintf("Index %d is out of bound", i))
		return
	}

	peers := append(c.peers[:i], c.peers[i+1:]...)
	c.peers = peers
}

// parseNodePeer attempts to construct a BGP peer from information conveyed
// in node annotations and labels using the specified autodiscovery
// configuration.
func parseNodePeer(l log.Logger, pad *config.PeerAutodiscovery, node *v1.Node) (*peer, error) {
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

	// Method called with a nil or empty peer autodiscovery.
	if pad == nil {
		return nil, errors.New("nil peer autodiscovery")
	}

	// If node labels don't match any peer autodiscovery node selector, we
	// shouldn't try to discover a peer for this node.
	shouldDiscover := false
	for _, ns := range pad.NodeSelectors {
		if ns.Matches(labels.Set(node.Labels)) {
			shouldDiscover = true
			break
		}
	}
	if !shouldDiscover {
		return nil, nil
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
		if pad.Defaults.Port != 0 {
			peerPort = pad.Defaults.Port
		}
		if pad.Defaults.HoldTime != 0 {
			holdTime = pad.Defaults.HoldTime
		}
	}

	if pad.FromLabels != nil {
		for k, v := range node.Labels {
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
		for k, v := range node.Annotations {
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

	// Verify required peer config. We shouldn't get errors here because we
	// validate the configuration. This check is here just for safety.
	if myASN == 0 {
		return nil, errors.New("missing local ASN")
	}
	if peerASN == 0 {
		return nil, errors.New("missing peer ASN")
	}
	if peerAddr == nil {
		return nil, errors.New("missing peer address")
	}

	// Set default BGP port if unspecified by user.
	if peerPort == 0 {
		peerPort = 179
	}

	if holdTime == 0 {
		// Hold time not specified in autodiscovery defaults - try to parse the
		// hold time from labels/annotations.
		ht, err := parseHoldTime(holdTimeRaw)
		if err != nil {
			return nil, fmt.Errorf("parsing hold time: %v", err)
		}
		holdTime = ht
	}

	// The peer is configured on a specific node object, so we want to create a
	// BGP session only on that node.
	h := node.Labels[v1.LabelHostname]
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
			Password:      password,
		},
		NodePeer: true,
	}

	return p, nil
}

// Returns true if the BGP config of a and b is identical.
//
// This function compares only BGP configuration - it ignores node selectors,
// BGP session status and the NodePeer field. It is helpful in cases where we
// need to check whether two peers are semantically identical with regards to
// BGP even if they differ in node selectors or if one peer is a node peer and
// the other isn't.
//
// The following parameters are compared: local ASN, peer ASN, peer address,
// peer port, hold time, router ID. BGP passwords are NOT checked.
//
// TODO: When adding BGP password support to node peers, add a check for
// passwords as well.
func bgpConfigEqual(a *config.Peer, b *config.Peer) bool {
	if a.ASN != b.ASN {
		return false
	}
	if !a.Addr.Equal(b.Addr) {
		return false
	}
	if a.MyASN != b.MyASN {
		return false
	}
	if a.Port != b.Port {
		return false
	}
	if a.HoldTime != b.HoldTime {
		return false
	}
	if !a.RouterID.Equal(b.RouterID) {
		return false
	}

	return true
}

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
