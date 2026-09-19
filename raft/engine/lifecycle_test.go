// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAdmissionSealsBeforeDrainingExistingSubmitters(t *testing.T) {
	g := admissionGate{drained: make(chan struct{})}
	if !g.enter() {
		t.Fatal("initial admission rejected")
	}
	g.seal()
	if g.enter() {
		t.Fatal("admitted after seal")
	}
	select {
	case <-g.drained:
		t.Fatal("drained before the admitted enqueue finished")
	default:
	}
	g.leave()
	select {
	case <-g.drained:
	default:
		t.Fatal("last submitter did not release drain")
	}
	// Repeated seal must not close the completion channel twice.
	g.seal()
}

func TestShutdownRevokesAuthorityBeforeBlockedWriterJoins(t *testing.T) {
	members := testMembers()[:1]
	network := &testNetwork{nodes: map[uint64]*Runtime{}}
	var armed atomic.Bool
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	cfg := Config{Local: members[0], Initial: members, Dir: t.TempDir(), Heartbeat: 20 * time.Millisecond, ElectionTicks: 3,
		beforeSave: func() error {
			if armed.CompareAndSwap(true, false) {
				close(entered)
				<-release
			}
			return nil
		}}
	r, err := Open(cfg, &testApp{}, &testLink{network: network, id: 1}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { unblock(); r.Close() })
	eventually(t, "single voter leader", func() bool { return r.Status().CaughtUp })
	armed.Store(true)
	result, err := r.Propose([]byte("pending at shutdown"))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("writer barrier not entered")
	}
	closed := make(chan struct{})
	go func() { r.Close(); close(closed) }()
	eventually(t, "authority revoked while writer blocked", func() bool { return !r.Status().IsLeader })
	select {
	case out := <-result:
		if !errors.Is(out.Err, ErrStopped) {
			t.Fatalf("unexpected pending outcome: %v", out.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending callback stranded behind disk")
	}
	select {
	case <-closed:
		t.Fatal("shutdown released an active disk owner")
	default:
	}
	for range 100 {
		if _, err := r.Propose([]byte("closed")); !errors.Is(err, ErrStopped) {
			t.Fatalf("propose after shutdown: %v", err)
		}
		if _, err := r.Snapshot(); !errors.Is(err, ErrStopped) {
			t.Fatalf("snapshot after shutdown: %v", err)
		}
		if _, err := r.ChangeMember(members[0], false, false); !errors.Is(err, ErrStopped) {
			t.Fatalf("membership after shutdown: %v", err)
		}
	}
	unblock()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not join released writer")
	}
	if r.proposalBytes.used.Load() != 0 {
		t.Fatal("proposal budget leaked during shutdown")
	}
}

func TestResignGenerationAcknowledgedBeforeSoleVoterReopens(t *testing.T) {
	members := testMembers()[:1]
	network := &testNetwork{nodes: map[uint64]*Runtime{}}
	roles := make(chan Role, 32)
	r, err := Open(Config{Local: members[0], Initial: members, Dir: t.TempDir(), Heartbeat: 20 * time.Millisecond, ElectionTicks: 3},
		&testApp{}, &testLink{network: network, id: 1}, func(role Role) { roles <- role }, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	eventually(t, "sole voter ready", func() bool { return r.Status().CaughtUp })
	issued := time.Now()
	r.RequestResign(7)
	deadline := time.After(2 * time.Second)
	var acknowledged time.Time
	for {
		select {
		case role := <-roles:
			if role.ResignIndex < 7 {
				continue
			}
			if !role.IsLeader && acknowledged.IsZero() {
				acknowledged = time.Now()
			}
			if role.IsLeader {
				if acknowledged.IsZero() || time.Since(issued) < 60*time.Millisecond {
					t.Fatal("sole voter skipped its active-time quarantine")
				}
				return
			}
		case <-deadline:
			t.Fatal("resignation generation never reopened")
		}
	}
}
