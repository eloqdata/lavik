// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"testing"
	"time"
)

func TestWaitingJoinerLearnerPromotionAndRemoval(t *testing.T) {
	t.Run("full-log", func(t *testing.T) { testWaitingJoiner(t, false) })
	t.Run("compacted-log", func(t *testing.T) { testWaitingJoiner(t, true) })
}

func testWaitingJoiner(t *testing.T, compact bool) {
	members := testMembers()[:2]
	networks := make([]*Network, 2)
	nodes := make([]*Runtime, 2)
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
		n, err := NewNetwork(Config{Local: members[i], Listen: "127.0.0.1:0"})
		if err != nil {
			t.Fatal(err)
		}
		networks[i] = n
		members[i].Raft = n.listener.Addr().String()
	}
	for i := range nodes {
		cfg := Config{Local: members[i], Dir: t.TempDir(), Heartbeat: 30 * time.Millisecond, ElectionTicks: 5}
		if i == 0 {
			cfg.Initial = members[:1]
		}
		r, err := Open(cfg, &testApp{}, networks[i], nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = r
		networks[i].Start(r.Step)
	}
	eventually(t, "genesis leader", func() bool { return nodes[0].Status().CaughtUp })
	if compact {
		result, err := nodes[0].Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		select {
		case outcome := <-result:
			if outcome.Err != nil {
				t.Fatal(outcome.Err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("initial snapshot timed out")
		}
		eventually(t, "genesis log compacted", func() bool { return nodes[0].Status().FirstIndex > 1 })
	}
	if s := nodes[1].Status(); s.IsLeader || s.Term != 0 {
		t.Fatalf("waiting joiner elected: %+v", s)
	}
	result, err := nodes[0].ChangeMember(members[1], false, false)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case outcome := <-result:
		if outcome.Err != nil {
			t.Fatal(outcome.Err)
		}
	case <-time.After(8 * time.Second):
		for _, r := range nodes {
			t.Logf("%+v", r.Status())
		}
		t.Fatal("learner promotion timed out")
	}
	if s := nodes[0].Status(); len(s.Members) != 2 || s.Learners[2] {
		t.Fatalf("not promoted: %+v", s)
	}
	result, err = nodes[0].Propose([]byte("joined write"))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case outcome := <-result:
		if outcome.Err != nil {
			t.Fatal(outcome.Err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("joined write failed")
	}
	result, err = nodes[0].ChangeMember(members[1], true, false)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case outcome := <-result:
		if outcome.Err != nil {
			t.Fatal(outcome.Err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("remove failed")
	}
	eventually(t, "removed descriptor", func() bool { return len(nodes[0].Status().Members) == 1 })
}
