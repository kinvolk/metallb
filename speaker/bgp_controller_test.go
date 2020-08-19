package main

import (
	"errors"
	"fmt"
	"net"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"go.universe.tf/metallb/internal/bgp"
	"go.universe.tf/metallb/internal/config"
	"go.universe.tf/metallb/internal/k8s"

	"github.com/go-kit/kit/log"
	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

func strptr(s string) *string {
	return &s
}

func mustSelector(s string) labels.Selector {
	res, err := labels.Parse(s)
	if err != nil {
		panic(err)
	}
	return res
}

func ipnet(s string) *net.IPNet {
	_, n, err := net.ParseCIDR(s)
	if err != nil {
		panic(err)
	}
	return n
}

func statusAssigned(ip string) v1.ServiceStatus {
	return v1.ServiceStatus{
		LoadBalancer: v1.LoadBalancerStatus{
			Ingress: []v1.LoadBalancerIngress{
				{
					IP: ip,
				},
			},
		},
	}
}

func sortAds(ads map[string][]*bgp.Advertisement) {
	if len(ads) == 0 {
		return
	}

	for _, v := range ads {
		if len(v) == 0 {
			continue
		}
		sort.Slice(v, func(i, j int) bool {
			a, b := v[i], v[j]
			if a.Prefix.String() != b.Prefix.String() {
				return a.Prefix.String() < b.Prefix.String()
			}
			if a.LocalPref != b.LocalPref {
				return a.LocalPref < b.LocalPref
			}
			if a.NextHop.String() != b.NextHop.String() {
				return a.NextHop.String() < b.NextHop.String()
			}
			if len(a.Communities) != len(b.Communities) {
				return len(a.Communities) < len(b.Communities)
			}
			sort.Slice(a.Communities, func(i, j int) bool { return a.Communities[i] < a.Communities[j] })
			sort.Slice(b.Communities, func(i, j int) bool { return b.Communities[i] < b.Communities[j] })
			for k := range a.Communities {
				if a.Communities[k] != b.Communities[k] {
					return a.Communities[k] < b.Communities[k]
				}
			}
			return false
		})
	}
}

type fakeBGP struct {
	t *testing.T

	sync.Mutex
	// peer IP -> advertisements
	gotAds map[string][]*bgp.Advertisement
}

func (f *fakeBGP) New(_ log.Logger, addr string, _ string, _ uint32, _ net.IP, _ uint32, _ time.Duration, _, _ string) (session, error) {
	f.Lock()
	defer f.Unlock()

	if _, ok := f.gotAds[addr]; ok {
		f.t.Errorf("Tried to create already existing BGP session to %q", addr)
		return nil, errors.New("invariant violation")
	}
	// Nil because we haven't programmed any routes for it yet, but
	// the key now exists in the map.
	f.gotAds[addr] = nil
	return &fakeSession{
		f:    f,
		addr: addr,
	}, nil
}

func (f *fakeBGP) Ads() map[string][]*bgp.Advertisement {
	ret := map[string][]*bgp.Advertisement{}

	f.Lock()
	defer f.Unlock()

	// Make a deep copy so that we can release the lock.
	for k, v := range f.gotAds {
		if v == nil {
			ret[k] = nil
			continue
		}
		s := []*bgp.Advertisement{}
		for _, ad := range v {
			adCopy := new(bgp.Advertisement)
			*adCopy = *ad
			s = append(s, adCopy)
		}
		ret[k] = s
	}

	return ret
}

type fakeSession struct {
	f    *fakeBGP
	addr string
}

func (f *fakeSession) Close() error {
	f.f.Lock()
	defer f.f.Unlock()

	if _, ok := f.f.gotAds[f.addr]; !ok {
		f.f.t.Errorf("Tried to close non-existent session to %q", f.addr)
		return errors.New("invariant violation")
	}

	delete(f.f.gotAds, f.addr)
	return nil
}

func (f *fakeSession) Set(ads ...*bgp.Advertisement) error {
	f.f.Lock()
	defer f.f.Unlock()

	if _, ok := f.f.gotAds[f.addr]; !ok {
		f.f.t.Errorf("Tried to set ads on non-existent session to %q", f.addr)
		return errors.New("invariant violation")
	}

	f.f.gotAds[f.addr] = ads
	return nil
}

// testK8S implements service by recording what the controller wants
// to do to k8s.
type testK8S struct {
	loggedWarning bool
	t             *testing.T
}

func (s *testK8S) Update(svc *v1.Service) (*v1.Service, error) {
	panic("never called")
}

func (s *testK8S) UpdateStatus(svc *v1.Service) error {
	panic("never called")
}

func (s *testK8S) Infof(_ *v1.Service, evtType string, msg string, args ...interface{}) {
	s.t.Logf("k8s Info event %q: %s", evtType, fmt.Sprintf(msg, args...))
}

func (s *testK8S) Errorf(_ *v1.Service, evtType string, msg string, args ...interface{}) {
	s.t.Logf("k8s Warning event %q: %s", evtType, fmt.Sprintf(msg, args...))
	s.loggedWarning = true
}

func TestBGPSpeaker(t *testing.T) {
	b := &fakeBGP{
		t:      t,
		gotAds: map[string][]*bgp.Advertisement{},
	}
	newBGP = b.New
	c, err := newController(controllerConfig{
		MyNode:        "pandora",
		DisableLayer2: true,
	})
	if err != nil {
		t.Fatalf("creating controller: %s", err)
	}
	c.client = &testK8S{t: t}

	tests := []struct {
		desc string

		balancer string
		config   *config.Config
		svc      *v1.Service
		eps      *v1.Endpoints

		wantAds map[string][]*bgp.Advertisement
	}{
		{
			desc:     "Service ignored, no config",
			balancer: "test1",
			svc: &v1.Service{
				Spec: v1.ServiceSpec{
					Type:                  "LoadBalancer",
					ExternalTrafficPolicy: "Cluster",
				},
				Status: statusAssigned("10.20.30.1"),
			},
			eps: &v1.Endpoints{
				Subsets: []v1.EndpointSubset{
					{
						Addresses: []v1.EndpointAddress{
							{
								IP:       "2.3.4.5",
								NodeName: strptr("iris"),
							},
						},
					},
				},
			},
			wantAds: map[string][]*bgp.Advertisement{},
		},

		{
			desc: "One peer, no services",
			config: &config.Config{
				Peers: []*config.Peer{
					{
						Addr:          net.ParseIP("1.2.3.4"),
						NodeSelectors: []labels.Selector{labels.Everything()},
					},
				},
				Pools: map[string]*config.Pool{
					"default": {
						Protocol: config.BGP,
						CIDR:     []*net.IPNet{ipnet("10.20.30.0/24")},
						BGPAdvertisements: []*config.BGPAdvertisement{
							{
								AggregationLength: 32,
							},
						},
					},
				},
			},
			wantAds: map[string][]*bgp.Advertisement{
				"1.2.3.4:0": nil,
			},
		},

		{
			desc:     "Add service, not an LB",
			balancer: "test1",
			svc: &v1.Service{
				Spec: v1.ServiceSpec{
					Type:                  "ClusterIP",
					ExternalTrafficPolicy: "Cluster",
				},
			},
			eps: &v1.Endpoints{
				Subsets: []v1.EndpointSubset{
					{
						Addresses: []v1.EndpointAddress{
							{
								IP:       "2.3.4.5",
								NodeName: strptr("pandora"),
							},
						},
					},
				},
			},
			wantAds: map[string][]*bgp.Advertisement{
				"1.2.3.4:0": nil,
			},
		},

		{
			desc:     "Add service, it's an LB!",
			balancer: "test1",
			svc: &v1.Service{
				Spec: v1.ServiceSpec{
					Type:                  "LoadBalancer",
					ExternalTrafficPolicy: "Cluster",
				},
				Status: statusAssigned("10.20.30.1"),
			},
			eps: &v1.Endpoints{
				Subsets: []v1.EndpointSubset{
					{
						Addresses: []v1.EndpointAddress{
							{
								IP:       "2.3.4.5",
								NodeName: strptr("iris"),
							},
						},
					},
				},
			},
			wantAds: map[string][]*bgp.Advertisement{
				"1.2.3.4:0": {
					{
						Prefix: ipnet("10.20.30.1/32"),
					},
				},
			},
		},

		{
			desc:     "LB switches to local traffic policy, endpoint isn't on our node",
			balancer: "test1",
			svc: &v1.Service{
				Spec: v1.ServiceSpec{
					Type:                  "LoadBalancer",
					ExternalTrafficPolicy: "Local",
				},
				Status: statusAssigned("10.20.30.1"),
			},
			eps: &v1.Endpoints{
				Subsets: []v1.EndpointSubset{
					{
						Addresses: []v1.EndpointAddress{
							{
								IP:       "2.3.4.5",
								NodeName: strptr("iris"),
							},
						},
					},
				},
			},
			wantAds: map[string][]*bgp.Advertisement{
				"1.2.3.4:0": nil,
			},
		},

		{
			desc:     "New endpoint, on our node",
			balancer: "test1",
			svc: &v1.Service{
				Spec: v1.ServiceSpec{
					Type:                  "LoadBalancer",
					ExternalTrafficPolicy: "Local",
				},
				Status: statusAssigned("10.20.30.1"),
			},
			eps: &v1.Endpoints{
				Subsets: []v1.EndpointSubset{
					{
						Addresses: []v1.EndpointAddress{
							{
								IP:       "2.3.4.5",
								NodeName: strptr("iris"),
							},
							{
								IP:       "2.3.4.6",
								NodeName: strptr("pandora"),
							},
						},
					},
				},
			},
			wantAds: map[string][]*bgp.Advertisement{
				"1.2.3.4:0": {
					{
						Prefix: ipnet("10.20.30.1/32"),
					},
				},
			},
		},

		{
			desc:     "Endpoint on our node has some unready ports",
			balancer: "test1",
			svc: &v1.Service{
				Spec: v1.ServiceSpec{
					Type:                  "LoadBalancer",
					ExternalTrafficPolicy: "Local",
				},
				Status: statusAssigned("10.20.30.1"),
			},
			eps: &v1.Endpoints{
				Subsets: []v1.EndpointSubset{
					{
						Addresses: []v1.EndpointAddress{
							{
								IP:       "2.3.4.5",
								NodeName: strptr("iris"),
							},
							{
								IP:       "2.3.4.6",
								NodeName: strptr("pandora"),
							},
						},
					},
					{
						Addresses: []v1.EndpointAddress{
							{
								IP:       "2.3.4.5",
								NodeName: strptr("iris"),
							},
						},
						NotReadyAddresses: []v1.EndpointAddress{
							{
								IP:       "2.3.4.6",
								NodeName: strptr("pandora"),
							},
						},
					},
				},
			},
			wantAds: map[string][]*bgp.Advertisement{
				"1.2.3.4:0": nil,
			},
		},

		{
			desc:     "Endpoint list is empty",
			balancer: "test1",
			svc: &v1.Service{
				Spec: v1.ServiceSpec{
					Type:                  "LoadBalancer",
					ExternalTrafficPolicy: "Cluster",
				},
				Status: statusAssigned("10.20.30.1"),
			},
			eps: &v1.Endpoints{},
			wantAds: map[string][]*bgp.Advertisement{
				"1.2.3.4:0": nil,
			},
		},

		{
			desc:     "Endpoint list contains only unhealthy endpoints",
			balancer: "test1",
			svc: &v1.Service{
				Spec: v1.ServiceSpec{
					Type:                  "LoadBalancer",
					ExternalTrafficPolicy: "Cluster",
				},
				Status: statusAssigned("10.20.30.1"),
			},
			eps: &v1.Endpoints{
				Subsets: []v1.EndpointSubset{
					{
						NotReadyAddresses: []v1.EndpointAddress{
							{
								IP:       "2.3.4.5",
								NodeName: strptr("iris"),
							},
						},
					},
				},
			},
			wantAds: map[string][]*bgp.Advertisement{
				"1.2.3.4:0": nil,
			},
		},

		{
			desc:     "Endpoint list contains some unhealthy endpoints",
			balancer: "test1",
			svc: &v1.Service{
				Spec: v1.ServiceSpec{
					Type:                  "LoadBalancer",
					ExternalTrafficPolicy: "Cluster",
				},
				Status: statusAssigned("10.20.30.1"),
			},
			eps: &v1.Endpoints{
				Subsets: []v1.EndpointSubset{
					{
						Addresses: []v1.EndpointAddress{
							{
								IP:       "2.3.4.5",
								NodeName: strptr("iris"),
							},
						},
						NotReadyAddresses: []v1.EndpointAddress{
							{
								IP:       "2.3.4.6",
								NodeName: strptr("pandora"),
							},
						},
					},
				},
			},
			wantAds: map[string][]*bgp.Advertisement{
				"1.2.3.4:0": {
					{
						Prefix: ipnet("10.20.30.1/32"),
					},
				},
			},
		},

		{
			desc: "Multiple advertisement config",
			config: &config.Config{
				Peers: []*config.Peer{
					{
						Addr:          net.ParseIP("1.2.3.4"),
						NodeSelectors: []labels.Selector{labels.Everything()},
					},
				},
				Pools: map[string]*config.Pool{
					"default": {
						Protocol: config.BGP,
						CIDR:     []*net.IPNet{ipnet("10.20.30.0/24")},
						BGPAdvertisements: []*config.BGPAdvertisement{
							{
								AggregationLength: 32,
								LocalPref:         100,
								Communities:       map[uint32]bool{1234: true, 2345: true},
							},
							{
								AggregationLength: 24,
								LocalPref:         1000,
							},
						},
					},
				},
			},
			balancer: "test1",
			svc: &v1.Service{
				Spec: v1.ServiceSpec{
					Type:                  "LoadBalancer",
					ExternalTrafficPolicy: "Cluster",
				},
				Status: statusAssigned("10.20.30.1"),
			},
			eps: &v1.Endpoints{
				Subsets: []v1.EndpointSubset{
					{
						Addresses: []v1.EndpointAddress{
							{
								IP:       "2.3.4.5",
								NodeName: strptr("iris"),
							},
						},
					},
				},
			},
			wantAds: map[string][]*bgp.Advertisement{
				"1.2.3.4:0": {
					{
						Prefix:      ipnet("10.20.30.1/32"),
						LocalPref:   100,
						Communities: []uint32{1234, 2345},
					},
					{
						Prefix:    ipnet("10.20.30.0/24"),
						LocalPref: 1000,
					},
				},
			},
		},

		{
			desc: "Multiple peers",
			config: &config.Config{
				Peers: []*config.Peer{
					{
						Addr:          net.ParseIP("1.2.3.4"),
						NodeSelectors: []labels.Selector{labels.Everything()},
					},
					{
						Addr:          net.ParseIP("1.2.3.5"),
						NodeSelectors: []labels.Selector{labels.Everything()},
					},
				},
				Pools: map[string]*config.Pool{
					"default": {
						Protocol: config.BGP,
						CIDR:     []*net.IPNet{ipnet("10.20.30.0/24")},
						BGPAdvertisements: []*config.BGPAdvertisement{
							{
								AggregationLength: 32,
							},
						},
					},
				},
			},
			balancer: "test1",
			svc: &v1.Service{
				Spec: v1.ServiceSpec{
					Type:                  "LoadBalancer",
					ExternalTrafficPolicy: "Cluster",
				},
				Status: statusAssigned("10.20.30.1"),
			},
			eps: &v1.Endpoints{
				Subsets: []v1.EndpointSubset{
					{
						Addresses: []v1.EndpointAddress{
							{
								IP:       "2.3.4.5",
								NodeName: strptr("iris"),
							},
						},
					},
				},
			},
			wantAds: map[string][]*bgp.Advertisement{
				"1.2.3.4:0": {
					{
						Prefix: ipnet("10.20.30.1/32"),
					},
				},
				"1.2.3.5:0": {
					{
						Prefix: ipnet("10.20.30.1/32"),
					},
				},
			},
		},

		{
			desc:     "Second balancer, no ingress assigned",
			balancer: "test2",
			svc: &v1.Service{
				Spec: v1.ServiceSpec{
					Type:                  "LoadBalancer",
					ExternalTrafficPolicy: "Cluster",
				},
			},
			eps: &v1.Endpoints{
				Subsets: []v1.EndpointSubset{
					{
						Addresses: []v1.EndpointAddress{
							{
								IP:       "2.3.4.5",
								NodeName: strptr("iris"),
							},
						},
					},
				},
			},
			wantAds: map[string][]*bgp.Advertisement{
				"1.2.3.4:0": {
					{
						Prefix: ipnet("10.20.30.1/32"),
					},
				},
				"1.2.3.5:0": {
					{
						Prefix: ipnet("10.20.30.1/32"),
					},
				},
			},
		},

		{
			desc:     "Second balancer, ingress gets assigned",
			balancer: "test2",
			svc: &v1.Service{
				Spec: v1.ServiceSpec{
					Type:                  "LoadBalancer",
					ExternalTrafficPolicy: "Cluster",
				},
				Status: statusAssigned("10.20.30.5"),
			},
			eps: &v1.Endpoints{
				Subsets: []v1.EndpointSubset{
					{
						Addresses: []v1.EndpointAddress{
							{
								IP:       "2.3.4.5",
								NodeName: strptr("iris"),
							},
						},
					},
				},
			},
			wantAds: map[string][]*bgp.Advertisement{
				"1.2.3.4:0": {
					{
						Prefix: ipnet("10.20.30.1/32"),
					},
					{
						Prefix: ipnet("10.20.30.5/32"),
					},
				},
				"1.2.3.5:0": {
					{
						Prefix: ipnet("10.20.30.1/32"),
					},
					{
						Prefix: ipnet("10.20.30.5/32"),
					},
				},
			},
		},

		{
			desc:     "Second balancer, ingress shared with first",
			balancer: "test2",
			svc: &v1.Service{
				Spec: v1.ServiceSpec{
					Type:                  "LoadBalancer",
					ExternalTrafficPolicy: "Cluster",
				},
				Status: statusAssigned("10.20.30.1"),
			},
			eps: &v1.Endpoints{
				Subsets: []v1.EndpointSubset{
					{
						Addresses: []v1.EndpointAddress{
							{
								IP:       "2.3.4.5",
								NodeName: strptr("iris"),
							},
						},
					},
				},
			},
			wantAds: map[string][]*bgp.Advertisement{
				"1.2.3.4:0": {
					// Prefixes duplicated because the dedupe happens
					// inside the real BGP session.
					{
						Prefix: ipnet("10.20.30.1/32"),
					},
					{
						Prefix: ipnet("10.20.30.1/32"),
					},
				},
				"1.2.3.5:0": {
					{
						Prefix: ipnet("10.20.30.1/32"),
					},
					{
						Prefix: ipnet("10.20.30.1/32"),
					},
				},
			},
		},

		{
			desc:     "Delete svc",
			balancer: "test1",
			wantAds: map[string][]*bgp.Advertisement{
				"1.2.3.4:0": {
					{
						Prefix: ipnet("10.20.30.1/32"),
					},
				},
				"1.2.3.5:0": {
					{
						Prefix: ipnet("10.20.30.1/32"),
					},
				},
			},
		},

		{
			desc: "Delete peer",
			config: &config.Config{
				Peers: []*config.Peer{
					{
						Addr:          net.ParseIP("1.2.3.5"),
						NodeSelectors: []labels.Selector{labels.Everything()},
					},
				},
				Pools: map[string]*config.Pool{
					"default": {
						Protocol: config.BGP,
						CIDR:     []*net.IPNet{ipnet("10.20.30.0/24")},
						BGPAdvertisements: []*config.BGPAdvertisement{
							{
								AggregationLength: 32,
							},
						},
					},
				},
			},
			balancer: "test2",
			svc: &v1.Service{
				Spec: v1.ServiceSpec{
					Type:                  "LoadBalancer",
					ExternalTrafficPolicy: "Cluster",
				},
				Status: statusAssigned("10.20.30.1"),
			},
			eps: &v1.Endpoints{
				Subsets: []v1.EndpointSubset{
					{
						Addresses: []v1.EndpointAddress{
							{
								IP:       "2.3.4.5",
								NodeName: strptr("iris"),
							},
						},
					},
				},
			},
			wantAds: map[string][]*bgp.Advertisement{
				"1.2.3.5:0": {
					{
						Prefix: ipnet("10.20.30.1/32"),
					},
				},
			},
		},

		{
			desc:     "Delete second svc",
			balancer: "test2",
			wantAds: map[string][]*bgp.Advertisement{
				"1.2.3.5:0": nil,
			},
		},
	}

	l := log.NewNopLogger()
	for _, test := range tests {
		if test.config != nil {
			if c.SetConfig(l, test.config) == k8s.SyncStateError {
				t.Errorf("%q: SetConfig failed", test.desc)
			}
		}
		if test.balancer != "" {
			if c.SetBalancer(l, test.balancer, test.svc, test.eps) == k8s.SyncStateError {
				t.Errorf("%q: SetBalancer failed", test.desc)
			}
		}

		gotAds := b.Ads()
		sortAds(test.wantAds)
		sortAds(gotAds)
		if diff := cmp.Diff(test.wantAds, gotAds); diff != "" {
			t.Errorf("%q: unexpected advertisement state (-want +got)\n%s", test.desc, diff)
		}
	}
}

func TestNodeSelectors(t *testing.T) {
	b := &fakeBGP{
		t:      t,
		gotAds: map[string][]*bgp.Advertisement{},
	}
	newBGP = b.New
	c, err := newController(controllerConfig{
		MyNode:        "pandora",
		DisableLayer2: true,
	})
	if err != nil {
		t.Fatalf("creating controller: %s", err)
	}
	c.client = &testK8S{t: t}

	pools := map[string]*config.Pool{
		"default": {
			Protocol: config.BGP,
			CIDR:     []*net.IPNet{ipnet("1.2.3.0/24")},
			BGPAdvertisements: []*config.BGPAdvertisement{
				{
					AggregationLength: 32,
				},
			},
		},
	}

	tests := []struct {
		desc    string
		config  *config.Config
		node    *v1.Node
		wantAds map[string][]*bgp.Advertisement
	}{
		{
			desc:    "No config, no advertisements",
			wantAds: map[string][]*bgp.Advertisement{},
		},

		{
			desc: "One peer, default node selector, no node labels",
			config: &config.Config{
				Peers: []*config.Peer{
					{
						Addr:          net.ParseIP("1.2.3.4"),
						NodeSelectors: []labels.Selector{labels.Everything()},
					},
				},
				Pools: pools,
			},
			wantAds: map[string][]*bgp.Advertisement{
				"1.2.3.4:0": nil,
			},
		},

		{
			desc: "Second peer, non-matching node selector",
			config: &config.Config{
				Peers: []*config.Peer{
					{
						Addr:          net.ParseIP("1.2.3.4"),
						NodeSelectors: []labels.Selector{labels.Everything()},
					},
					{
						Addr: net.ParseIP("2.3.4.5"),
						NodeSelectors: []labels.Selector{
							mustSelector("foo=bar"),
						},
					},
				},
				Pools: pools,
			},
			wantAds: map[string][]*bgp.Advertisement{
				"1.2.3.4:0": nil,
			},
		},

		{
			desc: "Add node label that matches",
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"foo": "bar",
					},
				},
			},
			wantAds: map[string][]*bgp.Advertisement{
				"1.2.3.4:0": nil,
				"2.3.4.5:0": nil,
			},
		},

		{
			desc: "Change node label so it no longer matches",
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"foo": "baz",
					},
				},
			},
			wantAds: map[string][]*bgp.Advertisement{
				"1.2.3.4:0": nil,
			},
		},

		{
			desc: "Change node selector so it matches again",
			config: &config.Config{
				Peers: []*config.Peer{
					{
						Addr:          net.ParseIP("1.2.3.4"),
						NodeSelectors: []labels.Selector{labels.Everything()},
					},
					{
						Addr: net.ParseIP("2.3.4.5"),
						NodeSelectors: []labels.Selector{
							mustSelector("foo in (bar, baz)"),
						},
					},
				},
				Pools: pools,
			},
			wantAds: map[string][]*bgp.Advertisement{
				"1.2.3.4:0": nil,
				"2.3.4.5:0": nil,
			},
		},

		{
			desc: "Change node label back, still matches",
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"foo": "bar",
					},
				},
			},
			wantAds: map[string][]*bgp.Advertisement{
				"1.2.3.4:0": nil,
				"2.3.4.5:0": nil,
			},
		},

		{
			desc: "Multiple node selectors, only one matches",
			config: &config.Config{
				Peers: []*config.Peer{
					{
						Addr:          net.ParseIP("1.2.3.4"),
						NodeSelectors: []labels.Selector{labels.Everything()},
					},
					{
						Addr: net.ParseIP("2.3.4.5"),
						NodeSelectors: []labels.Selector{
							mustSelector("host=frontend"),
							mustSelector("foo in (bar, baz)"),
						},
					},
				},
				Pools: pools,
			},
			wantAds: map[string][]*bgp.Advertisement{
				"1.2.3.4:0": nil,
				"2.3.4.5:0": nil,
			},
		},

		{
			desc: "Change node labels to match the other selector",
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"host": "frontend",
					},
				},
			},
			wantAds: map[string][]*bgp.Advertisement{
				"1.2.3.4:0": nil,
				"2.3.4.5:0": nil,
			},
		},
	}

	l := log.NewNopLogger()
	for _, test := range tests {
		if test.config != nil {
			if c.SetConfig(l, test.config) == k8s.SyncStateError {
				t.Errorf("%q: SetConfig failed", test.desc)
			}
		}

		if test.node != nil {
			if c.SetNode(l, test.node) == k8s.SyncStateError {
				t.Errorf("%q: SetNode failed", test.desc)
			}
		}

		gotAds := b.Ads()
		sortAds(test.wantAds)
		sortAds(gotAds)
		if diff := cmp.Diff(test.wantAds, gotAds); diff != "" {
			t.Errorf("%q: unexpected advertisement state (-want +got)\n%s", test.desc, diff)
		}
	}
}

func TestParseNodePeer(t *testing.T) {
	pam := &config.PeerAutodiscoveryMapping{
		MyASN:    "example.com/my-asn",
		ASN:      "example.com/asn",
		Addr:     "example.com/addr",
		Port:     "example.com/port",
		SrcAddr:  "example.com/src-addr",
		HoldTime: "example.com/hold-time",
		RouterID: "example.com/router-id",
	}

	tests := []struct {
		desc        string
		annotations labels.Set
		labels      labels.Set
		pad         *config.PeerAutodiscovery
		wantErr     bool
		wantPeer    *peer
	}{
		{
			desc: "Full config in annotations",
			annotations: labels.Set(map[string]string{
				"example.com/my-asn":    "65000",
				"example.com/asn":       "65001",
				"example.com/addr":      "10.0.0.1",
				"example.com/port":      "1179",
				"example.com/src-addr":  "10.0.0.2",
				"example.com/hold-time": "30s",
				"example.com/router-id": "10.0.0.2",
			}),
			labels: labels.Set(map[string]string{
				"kubernetes.io/hostname": "test",
			}),
			pad: &config.PeerAutodiscovery{
				NodeSelectors:   []labels.Selector{labels.Everything()},
				FromAnnotations: pam,
			},
			wantPeer: &peer{
				Cfg: &config.Peer{
					ASN:      65001,
					MyASN:    65000,
					Addr:     net.ParseIP("10.0.0.1"),
					HoldTime: 30 * time.Second,
					Port:     1179,
					SrcAddr:  net.ParseIP("10.0.0.2"),
					NodeSelectors: []labels.Selector{
						mustSelector(fmt.Sprintf("%s=%s", v1.LabelHostname, "test")),
					},
					RouterID: net.ParseIP("10.0.0.2"),
				},
			},
		},
		{
			desc:        "Full config in labels",
			annotations: labels.Set(map[string]string{}),
			labels: labels.Set(map[string]string{
				"kubernetes.io/hostname": "test",
				"example.com/my-asn":     "65000",
				"example.com/asn":        "65001",
				"example.com/addr":       "10.0.0.1",
				"example.com/port":       "1179",
				"example.com/src-addr":   "10.0.0.2",
				"example.com/hold-time":  "30s",
				"example.com/router-id":  "10.0.0.2",
			}),
			pad: &config.PeerAutodiscovery{
				NodeSelectors: []labels.Selector{labels.Everything()},
				FromLabels:    pam,
			},
			wantPeer: &peer{
				Cfg: &config.Peer{
					ASN:      65001,
					MyASN:    65000,
					Addr:     net.ParseIP("10.0.0.1"),
					HoldTime: 30 * time.Second,
					Port:     1179,
					SrcAddr:  net.ParseIP("10.0.0.2"),
					NodeSelectors: []labels.Selector{
						mustSelector(fmt.Sprintf("%s=%s", v1.LabelHostname, "test")),
					},
					RouterID: net.ParseIP("10.0.0.2"),
				},
			},
		},
		{
			desc: "Mixed - config in labels and annotations",
			annotations: labels.Set(map[string]string{
				"example.com/my-asn":    "65000",
				"example.com/addr":      "10.0.0.1",
				"example.com/hold-time": "30s",
				"example.com/src-addr":  "10.0.0.2",
			}),
			labels: labels.Set(map[string]string{
				"kubernetes.io/hostname": "test",
				"example.com/asn":        "65001",
				"example.com/port":       "1179",
				"example.com/router-id":  "10.0.0.2",
			}),
			pad: &config.PeerAutodiscovery{
				NodeSelectors: []labels.Selector{labels.Everything()},
				FromAnnotations: &config.PeerAutodiscoveryMapping{
					MyASN:    "example.com/my-asn",
					Addr:     "example.com/addr",
					SrcAddr:  "example.com/src-addr",
					HoldTime: "example.com/hold-time",
				},
				FromLabels: &config.PeerAutodiscoveryMapping{
					ASN:      "example.com/asn",
					Port:     "example.com/port",
					RouterID: "example.com/router-id",
				},
			},
			wantPeer: &peer{
				Cfg: &config.Peer{
					ASN:      65001,
					MyASN:    65000,
					Addr:     net.ParseIP("10.0.0.1"),
					HoldTime: 30 * time.Second,
					Port:     1179,
					SrcAddr:  net.ParseIP("10.0.0.2"),
					NodeSelectors: []labels.Selector{
						mustSelector(fmt.Sprintf("%s=%s", v1.LabelHostname, "test")),
					},
					RouterID: net.ParseIP("10.0.0.2"),
				},
			},
		},
		{
			desc: "Use all defaults",
			labels: labels.Set(map[string]string{
				"kubernetes.io/hostname": "test",
			}),
			pad: &config.PeerAutodiscovery{
				Defaults: &config.PeerAutodiscoveryDefaults{
					ASN:      65001,
					MyASN:    65000,
					Address:  net.ParseIP("10.0.0.1"),
					Port:     1179,
					HoldTime: 30 * time.Second,
				},
				FromAnnotations: pam,
				NodeSelectors:   []labels.Selector{labels.Everything()},
			},
			wantPeer: &peer{
				Cfg: &config.Peer{
					ASN:      65001,
					MyASN:    65000,
					Addr:     net.ParseIP("10.0.0.1"),
					HoldTime: 30 * time.Second,
					Port:     1179,
					NodeSelectors: []labels.Selector{
						mustSelector(fmt.Sprintf("%s=%s", v1.LabelHostname, "test")),
					},
				},
			},
		},
		{
			desc: "Nil peer autodiscovery",
			annotations: labels.Set(map[string]string{
				"example.com/my-asn":    "65000",
				"example.com/asn":       "65001",
				"example.com/addr":      "10.0.0.1",
				"example.com/port":      "1179",
				"example.com/src-addr":  "10.0.0.2",
				"example.com/hold-time": "30s",
				"example.com/router-id": "10.0.0.2",
			}),
			labels: labels.Set(map[string]string{
				"kubernetes.io/hostname": "test",
				"example.com/my-asn":     "65000",
				"example.com/asn":        "65001",
				"example.com/addr":       "10.0.0.1",
				"example.com/port":       "1179",
				"example.com/src-addr":   "10.0.0.2",
				"example.com/hold-time":  "30s",
				"example.com/router-id":  "10.0.0.2",
			}),
			wantErr: true,
		},
		{
			desc: "Verify annotations get precedence over labels",
			annotations: labels.Set(map[string]string{
				"example.com/my-asn":    "65000",
				"example.com/asn":       "65001",
				"example.com/addr":      "10.0.0.1",
				"example.com/port":      "1179",
				"example.com/src-addr":  "10.0.0.2",
				"example.com/hold-time": "30s",
				"example.com/router-id": "10.0.0.2",
			}),
			labels: labels.Set(map[string]string{
				"kubernetes.io/hostname": "test",
				"example.com/my-asn":     "65002",
				"example.com/asn":        "65003",
				"example.com/addr":       "10.0.0.3",
				"example.com/port":       "2179",
				"example.com/src-addr":   "10.0.0.4",
				"example.com/hold-time":  "120s",
				"example.com/router-id":  "10.0.0.4",
			}),
			pad: &config.PeerAutodiscovery{
				NodeSelectors:   []labels.Selector{labels.Everything()},
				FromAnnotations: pam,
				FromLabels:      pam,
			},
			wantPeer: &peer{
				Cfg: &config.Peer{
					ASN:      65001,
					MyASN:    65000,
					Addr:     net.ParseIP("10.0.0.1"),
					HoldTime: 30 * time.Second,
					Port:     1179,
					SrcAddr:  net.ParseIP("10.0.0.2"),
					NodeSelectors: []labels.Selector{
						mustSelector(fmt.Sprintf("%s=%s", v1.LabelHostname, "test")),
					},
					RouterID: net.ParseIP("10.0.0.2"),
				},
			},
		},
		{
			desc: "Empty annotations",
			annotations: labels.Set(map[string]string{
				"example.com/my-asn": "",
				"example.com/asn":    "",
				"example.com/addr":   "",
			}),
			labels: labels.Set(map[string]string{
				"kubernetes.io/hostname": "test",
			}),
			pad: &config.PeerAutodiscovery{
				NodeSelectors: []labels.Selector{
					mustSelector(fmt.Sprintf("%s=%s", v1.LabelHostname, "test")),
				},
				FromAnnotations: pam,
			},
			wantErr: true,
		},
		{
			desc:        "Empty labels",
			annotations: labels.Set(map[string]string{}),
			labels: labels.Set(map[string]string{
				"kubernetes.io/hostname": "test",
				"example.com/my-asn":     "",
				"example.com/asn":        "",
				"example.com/addr":       "",
			}),
			pad: &config.PeerAutodiscovery{
				NodeSelectors: []labels.Selector{
					mustSelector(fmt.Sprintf("%s=%s", v1.LabelHostname, "test")),
				},
				FromLabels: pam,
			},
			wantErr: true,
		},
		{
			desc: "Verify default hold time is overridden by annotations",
			annotations: labels.Set(map[string]string{
				"example.com/my-asn":    "65000",
				"example.com/asn":       "65001",
				"example.com/addr":      "10.0.0.1",
				"example.com/hold-time": "60s",
			}),
			labels: labels.Set(map[string]string{
				"kubernetes.io/hostname": "test",
			}),
			pad: &config.PeerAutodiscovery{
				NodeSelectors: []labels.Selector{
					mustSelector(fmt.Sprintf("%s=%s", v1.LabelHostname, "test")),
				},
				Defaults: &config.PeerAutodiscoveryDefaults{
					HoldTime: 30 * time.Second,
				},
				FromAnnotations: pam,
			},
			wantPeer: &peer{
				Cfg: &config.Peer{
					ASN:      65001,
					MyASN:    65000,
					Addr:     net.ParseIP("10.0.0.1"),
					HoldTime: 60 * time.Second,
					Port:     179,
					NodeSelectors: []labels.Selector{
						mustSelector(fmt.Sprintf("%s=%s", v1.LabelHostname, "test")),
					},
				},
			},
		},
		{
			desc: "Malformed local ASN",
			annotations: labels.Set(map[string]string{
				"example.com/my-asn": "oops",
				"example.com/asn":    "65001",
				"example.com/addr":   "10.0.0.1",
			}),
			labels: labels.Set(map[string]string{
				"kubernetes.io/hostname": "test",
			}),
			pad: &config.PeerAutodiscovery{
				NodeSelectors: []labels.Selector{
					mustSelector(fmt.Sprintf("%s=%s", v1.LabelHostname, "test")),
				},
				FromAnnotations: pam,
			},
			wantErr: true,
		},
		{
			desc: "Malformed peer ASN",
			annotations: labels.Set(map[string]string{
				"example.com/my-asn": "65000",
				"example.com/asn":    "oops",
				"example.com/addr":   "10.0.0.1",
			}),
			labels: labels.Set(map[string]string{
				"kubernetes.io/hostname": "test",
			}),
			pad: &config.PeerAutodiscovery{
				NodeSelectors: []labels.Selector{
					mustSelector(fmt.Sprintf("%s=%s", v1.LabelHostname, "test")),
				},
				FromAnnotations: pam,
			},
			wantErr: true,
		},
		{
			desc: "Malformed peer address",
			annotations: labels.Set(map[string]string{
				"example.com/my-asn": "65000",
				"example.com/asn":    "65001",
				"example.com/addr":   "oops",
			}),
			labels: labels.Set(map[string]string{
				"kubernetes.io/hostname": "test",
			}),
			pad: &config.PeerAutodiscovery{
				NodeSelectors: []labels.Selector{
					mustSelector(fmt.Sprintf("%s=%s", v1.LabelHostname, "test")),
				},
				FromAnnotations: pam,
			},
			wantErr: true,
		},
		{
			desc: "Malformed port",
			annotations: labels.Set(map[string]string{
				"example.com/my-asn": "65000",
				"example.com/asn":    "65001",
				"example.com/addr":   "10.0.0.1",
				"example.com/port":   "oops",
			}),
			labels: labels.Set(map[string]string{
				"kubernetes.io/hostname": "test",
			}),
			pad: &config.PeerAutodiscovery{
				NodeSelectors: []labels.Selector{
					mustSelector(fmt.Sprintf("%s=%s", v1.LabelHostname, "test")),
				},
				FromAnnotations: pam,
			},
			wantErr: true,
		},
		{
			desc: "Malformed source IP",
			annotations: labels.Set(map[string]string{
				"example.com/my-asn":   "65000",
				"example.com/asn":      "65001",
				"example.com/addr":     "10.0.0.1",
				"example.com/src-addr": "oops",
			}),
			labels: labels.Set(map[string]string{
				"kubernetes.io/hostname": "test",
			}),
			pad: &config.PeerAutodiscovery{
				NodeSelectors: []labels.Selector{
					mustSelector(fmt.Sprintf("%s=%s", v1.LabelHostname, "test")),
				},
				FromAnnotations: pam,
			},
			wantErr: true,
		},
		{
			desc: "Malformed hold time",
			annotations: labels.Set(map[string]string{
				"example.com/my-asn":    "65000",
				"example.com/asn":       "65001",
				"example.com/addr":      "10.0.0.1",
				"example.com/hold-time": "oops",
			}),
			labels: labels.Set(map[string]string{
				"kubernetes.io/hostname": "test",
			}),
			pad: &config.PeerAutodiscovery{
				NodeSelectors: []labels.Selector{
					mustSelector(fmt.Sprintf("%s=%s", v1.LabelHostname, "test")),
				},
				FromAnnotations: pam,
			},
			wantErr: true,
		},
		{
			desc: "Malformed router ID",
			annotations: labels.Set(map[string]string{
				"example.com/my-asn":    "65000",
				"example.com/asn":       "65001",
				"example.com/addr":      "10.0.0.1",
				"example.com/router-id": "oops",
			}),
			labels: labels.Set(map[string]string{
				"kubernetes.io/hostname": "test",
			}),
			pad: &config.PeerAutodiscovery{
				NodeSelectors: []labels.Selector{
					mustSelector(fmt.Sprintf("%s=%s", v1.LabelHostname, "test")),
				},
				FromAnnotations: pam,
			},
			wantErr: true,
		},
		{
			desc: "Invalid values in annotations, valid values in labels",
			annotations: labels.Set(map[string]string{
				"example.com/my-asn":    "oops",
				"example.com/asn":       "oops",
				"example.com/addr":      "oops",
				"example.com/port":      "oops",
				"example.com/hold-time": "oops",
				"example.com/router-id": "oops",
			}),
			labels: labels.Set(map[string]string{
				"kubernetes.io/hostname": "test",
				"example.com/my-asn":     "65000",
				"example.com/asn":        "65001",
				"example.com/addr":       "10.0.0.1",
				"example.com/port":       "1179",
				"example.com/hold-time":  "30s",
				"example.com/router-id":  "10.0.0.2",
			}),
			pad: &config.PeerAutodiscovery{
				NodeSelectors: []labels.Selector{
					mustSelector(fmt.Sprintf("%s=%s", v1.LabelHostname, "test")),
				},
				FromAnnotations: pam,
				FromLabels:      pam,
			},
			wantErr: true,
		},
	}

	comparer := func(a, b *peer) bool {
		return reflect.DeepEqual(a, b)
	}

	l := log.NewNopLogger()
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			gotPeer, err := parseNodePeer(l, test.pad, test.annotations, test.labels)
			if test.wantErr {
				// We expected an error but didn't get one.
				if err == nil {
					t.Errorf("%q: Expected an error but got nil", test.desc)
				}
				// We expected an error and got one. No need to check the peer.
				return
			}
			if err != nil {
				// We didn't expect an error.
				t.Errorf("%q: Expected no error but got %q", test.desc, err.Error())
			}
			if diff := cmp.Diff(test.wantPeer, gotPeer, cmp.Comparer(comparer)); diff != "" {
				t.Errorf("%q: Unexpected peer (-want +got)\n%s", test.desc, diff)
			}
		})
	}
}

func TestDiscoverNodePeer(t *testing.T) {
	anns := map[string]string{
		"example.com/my-asn":       "100",
		"example.com/peer-asn":     "200",
		"example.com/peer-address": "10.0.0.1",
		"example.com/src-address":  "10.0.0.5",
	}
	ls := map[string]string{
		"kubernetes.io/hostname": "test",
	}
	pad := &config.PeerAutodiscovery{
		FromAnnotations: &config.PeerAutodiscoveryMapping{
			MyASN:   "example.com/my-asn",
			ASN:     "example.com/peer-asn",
			Addr:    "example.com/peer-address",
			SrcAddr: "example.com/src-address",
		},
		NodeSelectors: []labels.Selector{
			mustSelector(fmt.Sprintf("%s=%s", v1.LabelHostname, "test")),
		},
	}

	tests := []struct {
		desc        string
		annotations map[string]string
		labels      map[string]string
		wantPeer    *peer
	}{
		{
			desc:        "Node peer discovered",
			annotations: anns,
			labels:      ls,
			wantPeer: &peer{
				Cfg: &config.Peer{
					MyASN:         100,
					ASN:           200,
					Addr:          net.ParseIP("10.0.0.1"),
					Port:          179,
					SrcAddr:       net.ParseIP("10.0.0.5"),
					HoldTime:      90 * time.Second,
					NodeSelectors: []labels.Selector{mustSelector("kubernetes.io/hostname=test")},
				},
			},
		},
		{
			desc:        "No node peer discovered",
			annotations: map[string]string{},
			labels:      ls,
		},
		{
			desc:        "Node labels don't match selector",
			annotations: anns,
			labels: labels.Set(map[string]string{
				"kubernetes.io/hostname": "foo",
			}),
		},
	}

	comparer := func(a, b *peer) bool {
		return reflect.DeepEqual(a, b)
	}

	l := log.NewNopLogger()
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			c := &bgpController{
				logger: l,
				myNode: "pandora",
				svcAds: make(map[string][]*bgp.Advertisement),
				cfg: &config.Config{
					PeerAutodiscovery: pad,
				},
				nodeAnnotations: labels.Set(test.annotations),
				nodeLabels:      labels.Set(test.labels),
			}

			p := c.discoverNodePeer(l)

			if diff := cmp.Diff(test.wantPeer, p, cmp.Comparer(comparer)); diff != "" {
				t.Errorf("%q: Unexpected peers (-want +got)\n%s", test.desc, diff)
			}
		})
	}
}

// Verify correct interaction between regular peers and node peers.
func TestNodePeers(t *testing.T) {
	nodePeer := &config.Peer{
		MyASN:    100,
		ASN:      200,
		Addr:     net.ParseIP("10.0.0.1"),
		Port:     179,
		SrcAddr:  net.ParseIP("10.0.0.5"),
		HoldTime: 90 * time.Second,
		NodeSelectors: []labels.Selector{
			mustSelector(fmt.Sprintf("%s=%s", v1.LabelHostname, "test")),
		},
	}
	p2 := &config.Peer{
		MyASN:         100,
		ASN:           200,
		Addr:          net.ParseIP("10.0.0.2"),
		Port:          179,
		HoldTime:      90 * time.Second,
		NodeSelectors: []labels.Selector{labels.Everything()},
	}
	p3 := &config.Peer{
		MyASN:         100,
		ASN:           200,
		Addr:          net.ParseIP("10.0.0.3"),
		Port:          179,
		HoldTime:      90 * time.Second,
		NodeSelectors: []labels.Selector{labels.Everything()},
	}
	pad := &config.PeerAutodiscovery{
		FromAnnotations: &config.PeerAutodiscoveryMapping{
			MyASN:   "example.com/my-asn",
			ASN:     "example.com/asn",
			Addr:    "example.com/addr",
			SrcAddr: "example.com/src-addr",
		},
		NodeSelectors: []labels.Selector{labels.Everything()},
	}

	tests := []struct {
		desc      string
		peers     []*peer
		cfg       *config.Config
		wantPeers []*peer
	}{
		{
			desc: "No peers, node peer discovered",
			cfg: &config.Config{
				PeerAutodiscovery: pad,
			},
			wantPeers: []*peer{{Cfg: nodePeer}},
		},
		{
			desc:  "Existing peer, node peer discovered",
			peers: []*peer{{Cfg: p2}},
			cfg: &config.Config{
				Peers:             []*config.Peer{p2},
				PeerAutodiscovery: pad,
			},
			wantPeers: []*peer{
				{Cfg: p2},
				{Cfg: nodePeer},
			},
		},
		{
			desc:  "Existing peer, no node peer discovered",
			peers: []*peer{{Cfg: p2}},
			cfg: &config.Config{
				Peers: []*config.Peer{p2},
			},
			wantPeers: []*peer{
				{Cfg: p2},
			},
		},
		{
			desc: "Peer autodiscovery disabled, node peer removed",
			peers: []*peer{
				{Cfg: p2},
				{Cfg: nodePeer},
			},
			cfg: &config.Config{
				Peers: []*config.Peer{p2},
			},
			wantPeers: []*peer{{Cfg: p2}},
		},
		{
			desc: "No peers in config, node peer remains intact",
			peers: []*peer{
				{Cfg: nodePeer},
			},
			cfg: &config.Config{
				PeerAutodiscovery: pad,
			},
			wantPeers: []*peer{
				{Cfg: nodePeer},
			},
		},
		{
			desc: "Regular peer modified, node peer remains intact",
			peers: []*peer{
				{Cfg: p2},
				{Cfg: nodePeer},
			},
			cfg: &config.Config{
				Peers:             []*config.Peer{p3},
				PeerAutodiscovery: pad,
			},
			wantPeers: []*peer{
				{Cfg: p3},
				{Cfg: nodePeer},
			},
		},
		{
			desc: "Regular peer modified to be identical to node peer",
			peers: []*peer{
				{Cfg: p2},
				{Cfg: p3},
				{Cfg: nodePeer},
			},
			cfg: &config.Config{
				Peers: []*config.Peer{
					{
						MyASN:         nodePeer.MyASN,
						ASN:           nodePeer.ASN,
						Addr:          nodePeer.Addr,
						Port:          nodePeer.Port,
						HoldTime:      nodePeer.HoldTime,
						NodeSelectors: []labels.Selector{labels.Everything()},
					},
					p3,
				},
				PeerAutodiscovery: pad,
			},
			wantPeers: []*peer{
				{Cfg: nodePeer},
				{Cfg: p3},
			},
		},
		{
			desc: "Regular peer identical to node peer, selectors match",
			peers: []*peer{
				{Cfg: &config.Peer{
					MyASN:         nodePeer.MyASN,
					ASN:           nodePeer.ASN,
					Addr:          nodePeer.Addr,
					Port:          nodePeer.Port,
					HoldTime:      nodePeer.HoldTime,
					NodeSelectors: []labels.Selector{labels.Everything()},
				}},
			},
			cfg: &config.Config{
				Peers: []*config.Peer{
					{
						MyASN:         nodePeer.MyASN,
						ASN:           nodePeer.ASN,
						Addr:          nodePeer.Addr,
						Port:          nodePeer.Port,
						HoldTime:      nodePeer.HoldTime,
						NodeSelectors: []labels.Selector{labels.Everything()},
					},
				},
				PeerAutodiscovery: pad,
			},
			wantPeers: []*peer{
				{Cfg: nodePeer},
			},
		},
		{
			desc: "Regular peer identical to node peer, selectors don't match",
			peers: []*peer{
				{Cfg: nodePeer},
			},
			cfg: &config.Config{
				Peers: []*config.Peer{
					{
						MyASN:    nodePeer.MyASN,
						ASN:      nodePeer.ASN,
						Addr:     nodePeer.Addr,
						Port:     nodePeer.Port,
						HoldTime: nodePeer.HoldTime,
						NodeSelectors: []labels.Selector{
							mustSelector(fmt.Sprintf("%s=%s", v1.LabelHostname, "foo")),
						},
					},
				},
				PeerAutodiscovery: pad,
			},
			wantPeers: []*peer{
				{Cfg: &config.Peer{
					MyASN:    nodePeer.MyASN,
					ASN:      nodePeer.ASN,
					Addr:     nodePeer.Addr,
					Port:     nodePeer.Port,
					HoldTime: nodePeer.HoldTime,
					NodeSelectors: []labels.Selector{
						mustSelector(fmt.Sprintf("%s=%s", v1.LabelHostname, "foo")),
					},
				}},
				{Cfg: nodePeer},
			},
		},
		{
			desc: "Regular peer identical to node peer except source address",
			peers: []*peer{
				{Cfg: nodePeer},
			},
			cfg: &config.Config{
				Peers: []*config.Peer{
					{
						MyASN:         nodePeer.MyASN,
						ASN:           nodePeer.ASN,
						Addr:          nodePeer.Addr,
						Port:          nodePeer.Port,
						HoldTime:      nodePeer.HoldTime,
						NodeSelectors: nodePeer.NodeSelectors,
					},
				},
				PeerAutodiscovery: pad,
			},
			wantPeers: []*peer{
				{Cfg: nodePeer},
			},
		},
	}

	comparer := func(a, b *peer) bool {
		return reflect.DeepEqual(a.Cfg, b.Cfg)
	}

	l := log.NewNopLogger()
	c := &bgpController{
		logger: l,
		myNode: "pandora",
		svcAds: make(map[string][]*bgp.Advertisement),
		nodeAnnotations: labels.Set(map[string]string{
			"example.com/my-asn":   "100",
			"example.com/asn":      "200",
			"example.com/addr":     "10.0.0.1",
			"example.com/src-addr": "10.0.0.5",
		}),
		nodeLabels: labels.Set(map[string]string{
			"kubernetes.io/hostname": "test",
		}),
	}
	for _, test := range tests {
		// Reset the BGP session status before each test. The fakeBGP type
		// preserves BGP session state between tests, which leads to unexpected
		// results.
		b := &fakeBGP{
			t:      t,
			gotAds: map[string][]*bgp.Advertisement{},
		}
		newBGP = b.New

		c.cfg = nil
		c.peers = test.peers

		if err := c.SetConfig(l, test.cfg); err != nil {
			t.Error("SetConfig failed")
		}

		if diff := cmp.Diff(test.wantPeers, c.peers, cmp.Comparer(comparer)); diff != "" {
			t.Errorf("%q: Unexpected peers (-want +got)\n%s", test.desc, diff)
		}
	}
}

func TestSetNode(t *testing.T) {
	tests := []struct {
		desc        string
		annotations map[string]string
		labels      map[string]string
		peers       []*peer
		node        *v1.Node
		wantPeers   []*peer
	}{
		{
			desc: "Node labels change, peer is updated",
			labels: map[string]string{
				"example.com/my-asn":       "100",
				"example.com/peer-asn":     "200",
				"example.com/peer-address": "10.0.0.1",
				"kubernetes.io/hostname":   "test",
			},
			peers: []*peer{
				{
					Cfg: &config.Peer{
						MyASN:    100,
						ASN:      200,
						Addr:     net.ParseIP("10.0.0.1"),
						Port:     179,
						HoldTime: 90 * time.Second,
						NodeSelectors: []labels.Selector{
							mustSelector(fmt.Sprintf("%s=%s", v1.LabelHostname, "test")),
						},
					},
				},
			},
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"example.com/my-asn":       "100",
						"example.com/peer-asn":     "200",
						"example.com/peer-address": "10.0.0.2",
						"kubernetes.io/hostname":   "test",
					},
				},
			},
			wantPeers: []*peer{
				{
					Cfg: &config.Peer{
						MyASN:    100,
						ASN:      200,
						Addr:     net.ParseIP("10.0.0.2"),
						Port:     179,
						HoldTime: 90 * time.Second,
						NodeSelectors: []labels.Selector{
							mustSelector(fmt.Sprintf("%s=%s", v1.LabelHostname, "test")),
						},
					},
				},
			},
		},
		{
			desc: "Node annotations change, peer is updated",
			annotations: map[string]string{
				"example.com/my-asn":       "100",
				"example.com/peer-asn":     "200",
				"example.com/peer-address": "10.0.0.1",
			},
			labels: map[string]string{
				"kubernetes.io/hostname": "test",
			},
			peers: []*peer{
				{
					Cfg: &config.Peer{
						MyASN:    100,
						ASN:      200,
						Addr:     net.ParseIP("10.0.0.1"),
						Port:     179,
						HoldTime: 90 * time.Second,
						NodeSelectors: []labels.Selector{
							mustSelector(fmt.Sprintf("%s=%s", v1.LabelHostname, "test")),
						},
					},
				},
			},
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						"example.com/my-asn":       "100",
						"example.com/peer-asn":     "200",
						"example.com/peer-address": "10.0.0.2",
					},
					Labels: map[string]string{
						"kubernetes.io/hostname": "test",
					},
				},
			},
			wantPeers: []*peer{
				{
					Cfg: &config.Peer{
						MyASN:    100,
						ASN:      200,
						Addr:     net.ParseIP("10.0.0.2"),
						Port:     179,
						HoldTime: 90 * time.Second,
						NodeSelectors: []labels.Selector{
							mustSelector(fmt.Sprintf("%s=%s", v1.LabelHostname, "test")),
						},
					},
				},
			},
		},
		{
			desc: "Required annotation is removed, peer is removed",
			annotations: map[string]string{
				"example.com/my-asn":       "100",
				"example.com/peer-asn":     "200",
				"example.com/peer-address": "10.0.0.1",
			},
			labels: map[string]string{
				"kubernetes.io/hostname": "test",
			},
			peers: []*peer{
				{
					Cfg: &config.Peer{
						MyASN:    100,
						ASN:      200,
						Addr:     net.ParseIP("10.0.0.1"),
						Port:     179,
						HoldTime: 90 * time.Second,
						NodeSelectors: []labels.Selector{
							mustSelector(fmt.Sprintf("%s=%s", v1.LabelHostname, "test")),
						},
					},
				},
			},
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						"example.com/my-asn":   "100",
						"example.com/peer-asn": "200",
					},
					Labels: map[string]string{
						"kubernetes.io/hostname": "test",
					},
				},
			},
			wantPeers: []*peer{},
		},
	}

	comparer := func(a, b *peer) bool {
		return reflect.DeepEqual(a.Cfg, b.Cfg)
	}

	l := log.NewNopLogger()
	c := &bgpController{
		cfg: &config.Config{
			PeerAutodiscovery: &config.PeerAutodiscovery{
				FromAnnotations: &config.PeerAutodiscoveryMapping{
					MyASN: "example.com/my-asn",
					ASN:   "example.com/peer-asn",
					Addr:  "example.com/peer-address",
				},
				FromLabels: &config.PeerAutodiscoveryMapping{
					MyASN: "example.com/my-asn",
					ASN:   "example.com/peer-asn",
					Addr:  "example.com/peer-address",
				},
				NodeSelectors: []labels.Selector{labels.Everything()},
			},
		},
		logger: l,
		myNode: "pandora",
		svcAds: make(map[string][]*bgp.Advertisement),
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			// Reset the BGP session status before each test. The fakeBGP type
			// preserves BGP session state between tests, which leads to unexpected
			// results.
			b := &fakeBGP{
				t:      t,
				gotAds: map[string][]*bgp.Advertisement{},
			}
			newBGP = b.New

			c.nodeAnnotations = test.annotations
			c.nodeLabels = test.labels
			c.peers = test.peers

			if err := c.SetNode(l, test.node); err != nil {
				t.Error("SetNode failed")
			}

			if diff := cmp.Diff(test.wantPeers, c.peers, cmp.Comparer(comparer)); diff != "" {
				t.Errorf("%q: Unexpected peers (-want +got)\n%s", test.desc, diff)
			}
		})
	}
}
