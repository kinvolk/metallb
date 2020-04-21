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

package config // import "go.universe.tf/metallb/internal/config"

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/mikioh/ipaddr"
	yaml "gopkg.in/yaml.v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// configFile is the configuration as parsed out of the ConfigMap,
// without validation or useful high level types.
type configFile struct {
	Peers             []peer
	PeerAutodiscovery *peerAutodiscovery `yaml:"peer-autodiscovery"`
	BGPCommunities    map[string]string  `yaml:"bgp-communities"`
	Pools             []addressPool      `yaml:"address-pools"`
}

type peer struct {
	MyASN         uint32         `yaml:"my-asn"`
	ASN           uint32         `yaml:"peer-asn"`
	Addr          string         `yaml:"peer-address"`
	Port          uint16         `yaml:"peer-port"`
	HoldTime      string         `yaml:"hold-time"`
	RouterID      string         `yaml:"router-id"`
	NodeSelectors []nodeSelector `yaml:"node-selectors"`
	Password      string         `yaml:"password"`
}

type peerAutodiscovery struct {
	Defaults        *peerAutodiscoveryDefaults `yaml:"defaults"`
	NodeSelectors   []nodeSelector             `yaml:"node-selectors"`
	FromAnnotations *peerAutodiscoveryMapping  `yaml:"from-annotations"`
	FromLabels      *peerAutodiscoveryMapping  `yaml:"from-labels"`
}

type peerAutodiscoveryDefaults struct {
	MyASN    uint32 `yaml:"my-asn"`
	ASN      uint32 `yaml:"peer-asn"`
	Address  string `yaml:"peer-address"`
	Port     uint16 `yaml:"peer-port"`
	HoldTime string `yaml:"hold-time"`
}

type peerAutodiscoveryMapping struct {
	MyASN    string `yaml:"my-asn"`
	ASN      string `yaml:"peer-asn"`
	Addr     string `yaml:"peer-address"`
	Port     string `yaml:"peer-port"`
	HoldTime string `yaml:"hold-time"`
	RouterID string `yaml:"router-id"`
}

type nodeSelector struct {
	MatchLabels      map[string]string      `yaml:"match-labels"`
	MatchExpressions []selectorRequirements `yaml:"match-expressions"`
}

type selectorRequirements struct {
	Key      string   `yaml:"key"`
	Operator string   `yaml:"operator"`
	Values   []string `yaml:"values"`
}

type addressPool struct {
	Protocol          Proto
	Name              string
	Addresses         []string
	AvoidBuggyIPs     bool               `yaml:"avoid-buggy-ips"`
	AutoAssign        *bool              `yaml:"auto-assign"`
	BGPAdvertisements []bgpAdvertisement `yaml:"bgp-advertisements"`
}

type bgpAdvertisement struct {
	AggregationLength *int `yaml:"aggregation-length"`
	LocalPref         *uint32
	Communities       []string
}

// Config is a parsed MetalLB configuration.
type Config struct {
	// Routers that MetalLB should peer with.
	Peers []*Peer
	// Peer autodiscovery configuration.
	PeerAutodiscovery *PeerAutodiscovery
	// Address pools from which to allocate load balancer IPs.
	Pools map[string]*Pool
}

// Proto holds the protocol we are speaking.
type Proto string

// MetalLB supported protocols.
const (
	BGP    Proto = "bgp"
	Layer2       = "layer2"
)

// Peer is the configuration of a BGP peering session.
type Peer struct {
	// AS number to use for the local end of the session.
	MyASN uint32
	// AS number to expect from the remote end of the session.
	ASN uint32
	// Address to dial when establishing the session.
	Addr net.IP
	// Port to dial when establishing the session.
	Port uint16
	// Requested BGP hold time, per RFC4271.
	HoldTime time.Duration
	// BGP router ID to advertise to the peer
	RouterID net.IP
	// Only connect to this peer on nodes that match one of these
	// selectors.
	NodeSelectors []labels.Selector
	// Authentication password for routers enforcing TCP MD5 authenticated sessions
	Password string
	// TODO: more BGP session settings
}

// PeerAutodiscoveryDefaults specifies BGP peering parameters which can be set
// globally for all autodiscovered peers. Node-specific BGP parameters such as
// router ID or peer address can't be configured globally and therefore aren't
// exposed.
type PeerAutodiscoveryDefaults struct {
	// AS number to use for the local end of the session.
	MyASN uint32
	// AS number to expect from the remote end of the session.
	ASN uint32
	// Address to dial when establishing the session.
	Address net.IP
	// Port to dial when establishing the session.
	Port uint16
	// Requested BGP hold time, per RFC4271.
	HoldTime time.Duration
}

// PeerAutodiscoveryMapping maps BGP peering configuration parameters to node
// annotations or labels to allow automatic discovery of BGP configuration
// from Node objects.
//
// All the fields are strings because the values of a PeerAutodiscoveryMapping
// are annotation/label keys rather than the BGP parameters themselves. The
// controller uses the PeerAutodiscoveryMapping to figure out which annotations
// or labels to get the BGP parameters from for a given node peer.
//
// For example, setting MyASN to example.com/my-asn means "look for an
// annotation or label with the key example.com/my-asn on a Node object and use
// its value as the local ASN of that node's BGP peer".
//
// Whether to use annotations or labels is out of scope for this type: since
// both annotations and labels use string keys, a PeerAutodiscoveryMapping can
// be used to map to both annotations and labels.
type PeerAutodiscoveryMapping struct {
	MyASN    string
	ASN      string
	Addr     string
	Port     string
	HoldTime string
	RouterID string
}

// PeerAutodiscovery defines automatic discovery of BGP peers using annotations
// and/or labels. It allows the user to tell MetalLB to retrieve BGP peering
// configuration dynamically rather than from a static configuration file.
type PeerAutodiscovery struct {
	// Defaults specifies BGP peering parameters which should be used for all
	// autodiscovered peers. The default value of a parameter is used when the
	// same parameter can't be retrieved from annotations/labels on the Node
	// object.
	Defaults *PeerAutodiscoveryDefaults
	// FromAnnotations tells MetalLB to retrieve BGP peering configuration for
	// a node by looking up specific annotations on the corresponding Node
	// object.
	FromAnnotations *PeerAutodiscoveryMapping
	// FromLabels tells MetalLB to retrieve BGP peering configuration for
	// a node by looking up specific labels on the corresponding Node object.
	FromLabels *PeerAutodiscoveryMapping
	// NodeSelectors indicates the nodes for which peer autodiscovery should be
	// enabled. When no selectors are specified, peer autodiscovery will be
	// attempted for any node.
	NodeSelectors []labels.Selector
}

// Pool is the configuration of an IP address pool.
type Pool struct {
	// Protocol for this pool.
	Protocol Proto
	// The addresses that are part of this pool, expressed as CIDR
	// prefixes. config.Parse guarantees that these are
	// non-overlapping, both within and between pools.
	CIDR []*net.IPNet
	// Some buggy consumer devices mistakenly drop IPv4 traffic for IP
	// addresses ending in .0 or .255, due to poor implementations of
	// smurf protection. This setting marks such addresses as
	// unusable, for maximum compatibility with ancient parts of the
	// internet.
	AvoidBuggyIPs bool
	// If false, prevents IP addresses to be automatically assigned
	// from this pool.
	AutoAssign bool
	// When an IP is allocated from this pool, how should it be
	// translated into BGP announcements?
	BGPAdvertisements []*BGPAdvertisement
}

// BGPAdvertisement describes one translation from an IP address to a BGP advertisement.
type BGPAdvertisement struct {
	// Roll up the IP address into a CIDR prefix of this
	// length. Optional, defaults to 32 (i.e. no aggregation) if not
	// specified.
	AggregationLength int
	// Value of the LOCAL_PREF BGP path attribute. Used only when
	// advertising to IBGP peers (i.e. Peer.MyASN == Peer.ASN).
	LocalPref uint32
	// Value of the COMMUNITIES path attribute.
	Communities map[uint32]bool
}

func parseNodeSelector(ns *nodeSelector) (labels.Selector, error) {
	if len(ns.MatchLabels)+len(ns.MatchExpressions) == 0 {
		return labels.Everything(), nil
	}

	// Convert to a metav1.LabelSelector so we can use the fancy
	// parsing function to create a Selector.
	//
	// Why not use metav1.LabelSelector in the raw config object?
	// Because metav1.LabelSelector doesn't have yaml tag
	// specifications.
	sel := &metav1.LabelSelector{
		MatchLabels: ns.MatchLabels,
	}
	for _, req := range ns.MatchExpressions {
		sel.MatchExpressions = append(sel.MatchExpressions, metav1.LabelSelectorRequirement{
			Key:      req.Key,
			Operator: metav1.LabelSelectorOperator(req.Operator),
			Values:   req.Values,
		})
	}

	return metav1.LabelSelectorAsSelector(sel)
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

// Parse loads and validates a Config from bs.
func Parse(bs []byte) (*Config, error) {
	var raw configFile
	if err := yaml.UnmarshalStrict(bs, &raw); err != nil {
		return nil, fmt.Errorf("could not parse config: %s", err)
	}

	cfg := &Config{Pools: map[string]*Pool{}}
	for i, p := range raw.Peers {
		peer, err := parsePeer(p)
		if err != nil {
			return nil, fmt.Errorf("parsing peer #%d: %s", i+1, err)
		}
		cfg.Peers = append(cfg.Peers, peer)
	}

	if raw.PeerAutodiscovery != nil {
		pad, err := parsePeerAutodiscovery(*raw.PeerAutodiscovery)
		if err != nil {
			return nil, fmt.Errorf("parsing peer autodiscovery: %s", err)
		}
		cfg.PeerAutodiscovery = pad
	}

	communities := map[string]uint32{}
	for n, v := range raw.BGPCommunities {
		c, err := parseCommunity(v)
		if err != nil {
			return nil, fmt.Errorf("parsing community %q: %s", n, err)
		}
		communities[n] = c
	}

	var allCIDRs []*net.IPNet
	for i, p := range raw.Pools {
		pool, err := parseAddressPool(p, communities)
		if err != nil {
			return nil, fmt.Errorf("parsing address pool #%d: %s", i+1, err)
		}

		// Check that the pool isn't already defined
		if cfg.Pools[p.Name] != nil {
			return nil, fmt.Errorf("duplicate definition of pool %q", p.Name)
		}

		// Check that all specified CIDR ranges are non-overlapping.
		for _, cidr := range pool.CIDR {
			for _, m := range allCIDRs {
				if cidrsOverlap(cidr, m) {
					return nil, fmt.Errorf("CIDR %q in pool %q overlaps with already defined CIDR %q", cidr, p.Name, m)
				}
			}
			allCIDRs = append(allCIDRs, cidr)
		}

		cfg.Pools[p.Name] = pool
	}

	return cfg, nil
}

func parsePeer(p peer) (*Peer, error) {
	if p.MyASN == 0 {
		return nil, errors.New("missing local ASN")
	}
	if p.ASN == 0 {
		return nil, errors.New("missing peer ASN")
	}
	ip := net.ParseIP(p.Addr)
	if ip == nil {
		return nil, fmt.Errorf("invalid peer IP %q", p.Addr)
	}
	holdTime, err := parseHoldTime(p.HoldTime)
	if err != nil {
		return nil, err
	}
	port := uint16(179)
	if p.Port != 0 {
		port = p.Port
	}
	// Ideally we would set a default RouterID here, instead of having
	// to do it elsewhere in the code. Unfortunately, we don't know
	// the node IP here.
	var routerID net.IP
	if p.RouterID != "" {
		routerID = net.ParseIP(p.RouterID)
		if routerID == nil {
			return nil, fmt.Errorf("invalid router ID %q", p.RouterID)
		}
	}

	// We use a non-pointer in the raw json object, so that if the
	// user doesn't provide a node selector, we end up with an empty,
	// but non-nil selector, which means "select everything".
	var nodeSels []labels.Selector
	if len(p.NodeSelectors) == 0 {
		nodeSels = []labels.Selector{labels.Everything()}
	} else {
		for _, sel := range p.NodeSelectors {
			nodeSel, err := parseNodeSelector(&sel)
			if err != nil {
				return nil, fmt.Errorf("parsing node selector: %s", err)
			}
			nodeSels = append(nodeSels, nodeSel)
		}
	}

	var password string
	if p.Password != "" {
		password = p.Password
	}
	return &Peer{
		MyASN:         p.MyASN,
		ASN:           p.ASN,
		Addr:          ip,
		Port:          port,
		HoldTime:      holdTime,
		RouterID:      routerID,
		NodeSelectors: nodeSels,
		Password:      password,
	}, nil
}

// Parse peer autodiscovery configuration.
//
// BGP peer configuration can be specified in either annotations or labels. If
// the same parameter is specified using both annotations and labels, the value
// specified in annotations is used. If a parameter isn't specified in
// annotations nor in labels, a default value - if configured - is used.
//
// If a required BGP parameter isn't specified in annotations, labels or
// defaults, an error is returned. Required parameters:
// - Local ASN
// - Peer ASN
// - Peer address
func parsePeerAutodiscovery(p peerAutodiscovery) (*PeerAutodiscovery, error) {
	pad := &PeerAutodiscovery{}

	if p.FromAnnotations == nil && p.FromLabels == nil {
		return nil, errors.New("missing from-annotations or from-labels")
	}

	if p.Defaults != nil {
		pad.Defaults = &PeerAutodiscoveryDefaults{
			ASN:     p.Defaults.ASN,
			MyASN:   p.Defaults.MyASN,
			Address: net.ParseIP(p.Defaults.Address),
			Port:    p.Defaults.Port,
		}

		if p.Defaults.HoldTime != "" {
			ht, err := parseHoldTime(p.Defaults.HoldTime)
			if err != nil {
				return nil, fmt.Errorf("parsing default hold time: %s", err)
			}
			pad.Defaults.HoldTime = ht
		}
	}

	if p.FromAnnotations != nil {
		pad.FromAnnotations = &PeerAutodiscoveryMapping{
			ASN:      p.FromAnnotations.ASN,
			Addr:     p.FromAnnotations.Addr,
			HoldTime: p.FromAnnotations.HoldTime,
			MyASN:    p.FromAnnotations.MyASN,
			Port:     p.FromAnnotations.Port,
			RouterID: p.FromAnnotations.RouterID,
		}
	}

	if p.FromLabels != nil {
		pad.FromLabels = &PeerAutodiscoveryMapping{
			ASN:      p.FromLabels.ASN,
			Addr:     p.FromLabels.Addr,
			HoldTime: p.FromLabels.HoldTime,
			MyASN:    p.FromLabels.MyASN,
			Port:     p.FromLabels.Port,
			RouterID: p.FromLabels.RouterID,
		}
	}

	// We use a non-pointer in the raw json object, so that if the
	// user doesn't provide a node selector, we end up with an empty,
	// but non-nil selector, which means "select everything".
	var nodeSels []labels.Selector
	if len(p.NodeSelectors) == 0 {
		nodeSels = []labels.Selector{labels.Everything()}
	} else {
		for _, sel := range p.NodeSelectors {
			nodeSel, err := parseNodeSelector(&sel)
			if err != nil {
				return nil, fmt.Errorf("parsing node selector: %s", err)
			}
			nodeSels = append(nodeSels, nodeSel)
		}
	}
	pad.NodeSelectors = nodeSels

	if err := validatePeerAutodiscovery(*pad); err != nil {
		return nil, fmt.Errorf("validating peer autodiscovery: %s", err)
	}

	return pad, nil
}

func parseAddressPool(p addressPool, bgpCommunities map[string]uint32) (*Pool, error) {
	if p.Name == "" {
		return nil, errors.New("missing pool name")
	}

	ret := &Pool{
		Protocol:      p.Protocol,
		AvoidBuggyIPs: p.AvoidBuggyIPs,
		AutoAssign:    true,
	}

	if p.AutoAssign != nil {
		ret.AutoAssign = *p.AutoAssign
	}

	if len(p.Addresses) == 0 {
		return nil, errors.New("pool has no prefixes defined")
	}
	for _, cidr := range p.Addresses {
		nets, err := parseCIDR(cidr)
		if err != nil {
			return nil, fmt.Errorf("invalid CIDR %q in pool %q: %s", cidr, p.Name, err)
		}
		ret.CIDR = append(ret.CIDR, nets...)
	}

	switch ret.Protocol {
	case Layer2:
		if len(p.BGPAdvertisements) > 0 {
			return nil, errors.New("cannot have bgp-advertisements configuration element in a layer2 address pool")
		}
	case BGP:
		ads, err := parseBGPAdvertisements(p.BGPAdvertisements, ret.CIDR, bgpCommunities)
		if err != nil {
			return nil, fmt.Errorf("parsing BGP communities: %s", err)
		}
		ret.BGPAdvertisements = ads
	case "":
		return nil, errors.New("address pool is missing the protocol field")
	default:
		return nil, fmt.Errorf("unknown protocol %q", ret.Protocol)
	}

	return ret, nil
}

func parseBGPAdvertisements(ads []bgpAdvertisement, cidrs []*net.IPNet, communities map[string]uint32) ([]*BGPAdvertisement, error) {
	if len(ads) == 0 {
		return []*BGPAdvertisement{
			{
				AggregationLength: 32,
				LocalPref:         0,
				Communities:       map[uint32]bool{},
			},
		}, nil
	}

	var ret []*BGPAdvertisement
	for _, rawAd := range ads {
		ad := &BGPAdvertisement{
			AggregationLength: 32,
			LocalPref:         0,
			Communities:       map[uint32]bool{},
		}

		if rawAd.AggregationLength != nil {
			ad.AggregationLength = *rawAd.AggregationLength
		}
		if ad.AggregationLength > 32 {
			return nil, fmt.Errorf("invalid aggregation length %q", ad.AggregationLength)
		}
		for _, cidr := range cidrs {
			o, _ := cidr.Mask.Size()
			if ad.AggregationLength < o {
				return nil, fmt.Errorf("invalid aggregation length %d: prefix %q in this pool is more specific than the aggregation length", ad.AggregationLength, cidr)
			}
		}

		if rawAd.LocalPref != nil {
			ad.LocalPref = *rawAd.LocalPref
		}

		for _, c := range rawAd.Communities {
			if v, ok := communities[c]; ok {
				ad.Communities[v] = true
			} else {
				v, err := parseCommunity(c)
				if err != nil {
					return nil, fmt.Errorf("invalid community %q in BGP advertisement: %s", c, err)
				}
				ad.Communities[v] = true
			}
		}

		ret = append(ret, ad)
	}

	return ret, nil
}

func parseCommunity(c string) (uint32, error) {
	fs := strings.Split(c, ":")
	if len(fs) != 2 {
		return 0, fmt.Errorf("invalid community string %q", c)
	}
	a, err := strconv.ParseUint(fs[0], 10, 16)
	if err != nil {
		return 0, fmt.Errorf("invalid first section of community %q: %s", fs[0], err)
	}
	b, err := strconv.ParseUint(fs[1], 10, 16)
	if err != nil {
		return 0, fmt.Errorf("invalid second section of community %q: %s", fs[0], err)
	}

	return (uint32(a) << 16) + uint32(b), nil
}

func parseCIDR(cidr string) ([]*net.IPNet, error) {
	if !strings.Contains(cidr, "-") {
		_, n, err := net.ParseCIDR(cidr)
		if err != nil {
			return nil, fmt.Errorf("invalid CIDR %q", cidr)
		}
		return []*net.IPNet{n}, nil
	}

	fs := strings.SplitN(cidr, "-", 2)
	if len(fs) != 2 {
		return nil, fmt.Errorf("invalid IP range %q", cidr)
	}
	start := net.ParseIP(strings.TrimSpace(fs[0]))
	if start == nil {
		return nil, fmt.Errorf("invalid IP range %q: invalid start IP %q", cidr, fs[0])
	}
	end := net.ParseIP(strings.TrimSpace(fs[1]))
	if end == nil {
		return nil, fmt.Errorf("invalid IP range %q: invalid end IP %q", cidr, fs[1])
	}

	var ret []*net.IPNet
	for _, pfx := range ipaddr.Summarize(start, end) {
		n := &net.IPNet{
			IP:   pfx.IP,
			Mask: pfx.Mask,
		}
		ret = append(ret, n)
	}
	return ret, nil
}

func cidrsOverlap(a, b *net.IPNet) bool {
	return cidrContainsCIDR(a, b) || cidrContainsCIDR(b, a)
}

func cidrContainsCIDR(outer, inner *net.IPNet) bool {
	ol, _ := outer.Mask.Size()
	il, _ := inner.Mask.Size()
	if ol == il && outer.IP.Equal(inner.IP) {
		return true
	}
	if ol < il && outer.Contains(inner.IP) {
		return true
	}
	return false
}

func isIPv4(ip net.IP) bool {
	return ip.To16() != nil && ip.To4() != nil
}

func isIPv6(ip net.IP) bool {
	return ip.To16() != nil && ip.To4() == nil
}

// Verify that peer autodiscovery config is specified for all required BGP
// params, or that default values are in place.
//
// Local ASN and peer ASN can be specified in annotations, labels or defaults.
// Peer address can be specified in annotations or labels.
func validatePeerAutodiscovery(p PeerAutodiscovery) error {
	var localASNOK bool
	var peerASNOK bool
	var peerAddressOK bool

	if d := p.Defaults; d != nil {
		if d.MyASN != 0 {
			localASNOK = true
		}
		if d.ASN != 0 {
			peerASNOK = true
		}
		if d.Address != nil {
			peerAddressOK = true
		}
	}

	if a := p.FromAnnotations; a != nil {
		if a.MyASN != "" {
			localASNOK = true
		}
		if a.ASN != "" {
			peerASNOK = true
		}
		if a.Addr != "" {
			peerAddressOK = true
		}
	}

	if l := p.FromLabels; l != nil {
		if l.MyASN != "" {
			localASNOK = true
		}
		if l.ASN != "" {
			peerASNOK = true
		}
		if l.Addr != "" {
			peerAddressOK = true
		}
	}

	if !localASNOK {
		return errors.New("local ASN missing and no default specified")
	}
	if !peerASNOK {
		return errors.New("peer ASN missing and no default specified")
	}
	if !peerAddressOK {
		return errors.New("peer address missing")
	}

	return nil
}
