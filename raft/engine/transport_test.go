// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"crypto/tls"
	"crypto/x509"
	"net/url"
	"testing"
	"time"
)

func TestPeerCertificateMustMatchClaimedMember(t *testing.T) {
	for _, tc := range []struct {
		name             string
		uris             []string
		verified, accept bool
	}{
		{"canonical", []string{"lavik://meta/2"}, true, true},
		{"wrong-member", []string{"lavik://meta/3"}, true, false},
		{"multiple-identities", []string{"lavik://meta/2", "lavik://meta/3"}, true, false},
		{"unrelated-extra-uri", []string{"lavik://meta/2", "https://example.test"}, true, false},
		{"missing-identity", nil, true, false},
		{"unverified-chain", []string{"lavik://meta/2"}, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			leaf := &x509.Certificate{}
			for _, text := range tc.uris {
				uri, err := url.Parse(text)
				if err != nil {
					t.Fatal(err)
				}
				leaf.URIs = append(leaf.URIs, uri)
			}
			state := tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}}
			if tc.verified {
				state.VerifiedChains = [][]*x509.Certificate{{leaf}}
			}
			if accepted := verifyPrincipal(state, "lavik://meta/2") == nil; accepted != tc.accept {
				t.Fatalf("accepted=%v", accepted)
			}
		})
	}
}

func TestLoopbackNetworkElectionAndCommit(t *testing.T) {
	members := testMembers()
	networks := make([]*Network, 3)
	nodes := make([]*Runtime, 3)
	t.Cleanup(func() {
		for _, r := range nodes {
			if r != nil {
				r.Close()
			}
		}
		for _, n := range networks {
			if n != nil {
				n.Close()
			}
		}
	})
	for i := range networks {
		cfg := Config{Local: members[i], Listen: "127.0.0.1:0"}
		n, err := NewNetwork(cfg)
		if err != nil {
			t.Fatal(err)
		}
		networks[i] = n
		members[i].Raft = n.listener.Addr().String()
	}
	for i, n := range networks {
		cfg := Config{Local: members[i], Initial: members, Dir: t.TempDir(), Heartbeat: 100 * time.Millisecond, ElectionTicks: 3}
		r, err := Open(cfg, &testApp{}, n, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = r
		n.Start(r.Step)
	}
	leader := -1
	eventually(t, "TCP leader", func() bool {
		for i, r := range nodes {
			if r.Status().CaughtUp {
				leader = i
				return true
			}
		}
		return false
	})
	result, err := nodes[leader].Propose([]byte("TCP replicated command"))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case outcome := <-result:
		if outcome.Err != nil {
			t.Fatal(outcome.Err)
		}
	case <-time.After(5 * time.Second):
		for _, r := range nodes {
			t.Logf("%+v", r.Status())
		}
		t.Fatal("network commit timeout")
	}
}
