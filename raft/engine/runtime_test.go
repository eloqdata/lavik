// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"bytes"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
)

type testApp struct {
	mu   sync.Mutex
	data [][]byte
}

func (a *testApp) Apply(_ uint64, data []byte) ([]byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.data = append(a.data, append([]byte(nil), data...))
	return append([]byte(nil), data...), nil
}
func (a *testApp) Install(_ uint64, data []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.data = [][]byte{append([]byte(nil), data...)}
	return nil
}
func (a *testApp) Capture(_ uint64) ([]byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return bytes.Join(a.data, nil), nil
}

type testNetwork struct {
	mu    sync.RWMutex
	nodes map[uint64]*Runtime
	hb    [4]atomic.Uint64
}
type testLink struct {
	network *testNetwork
	id      uint64
}

func (l *testLink) Send(m *pb.Message) bool {
	if m.GetType() == pb.MsgHeartbeat || m.GetType() == pb.MsgHeartbeatResp {
		l.network.hb[l.id].Add(1)
	}
	l.network.mu.RLock()
	r := l.network.nodes[m.GetTo()]
	l.network.mu.RUnlock()
	return r != nil && r.Step(m) == nil
}
func (*testLink) Update([]Member) {}
func (*testLink) Close()          {}

func eventually(t *testing.T, what string, f func() bool) {
	t.Helper()
	deadline := time.After(8 * time.Second)
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		if f() {
			return
		}
		select {
		case <-deadline:
			t.Fatal("timeout: " + what)
		case <-tick.C:
		}
	}
}

func TestRuntimeSlowMajorityPreservesHeartbeatsAndDurability(t *testing.T) {
	for _, stalledCount := range []int{1, 2, 3} {
		t.Run(string(rune('0'+stalledCount)), func(t *testing.T) {
			members := testMembers()
			network := &testNetwork{nodes: map[uint64]*Runtime{}}
			var stalled [3]atomic.Bool
			entered := make(chan int, 3)
			release := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			nodes := make([]*Runtime, 3)
			for i := range nodes {
				cfg := Config{Local: members[i], Initial: members, Dir: t.TempDir(), Heartbeat: 100 * time.Millisecond, ElectionTicks: 3}
				cfg.beforeSave = func() error {
					if stalled[i].CompareAndSwap(true, false) {
						entered <- i
						<-release
					}
					return nil
				}
				r, err := Open(cfg, &testApp{}, &testLink{network: network, id: members[i].ID}, nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				nodes[i] = r
				network.mu.Lock()
				network.nodes[members[i].ID] = r
				network.mu.Unlock()
			}
			t.Cleanup(func() {
				unblock()
				for _, r := range nodes {
					r.Close()
				}
			})
			leader := -1
			eventually(t, "caught-up leader", func() bool {
				for i, r := range nodes {
					s := r.Status()
					if s.IsLeader && s.CaughtUp {
						leader = i
						return true
					}
				}
				return false
			})
			first, err := nodes[leader].Propose([]byte("baseline"))
			if err != nil {
				t.Fatal(err)
			}
			select {
			case result := <-first:
				if result.Err != nil {
					t.Fatal(result.Err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("baseline proposal timed out")
			}
			eventually(t, "replicated baseline", func() bool {
				index := nodes[leader].Status().Applied
				for _, r := range nodes {
					if r.Status().Applied != index {
						return false
					}
				}
				return true
			})
			term := nodes[leader].Status().Term
			// Include the leader so application success also proves that local
			// storage, not only follower acknowledgements, remains pending.
			for offset := 0; offset < stalledCount; offset++ {
				stalled[(leader+offset)%3].Store(true)
			}
			result, err := nodes[leader].Propose([]byte("after disk barrier"))
			if err != nil {
				t.Fatal(err)
			}
			for range stalledCount {
				select {
				case <-entered:
				case <-time.After(5 * time.Second):
					t.Fatal("disk barrier was not entered")
				}
			}
			var before [3]uint64
			for i := range before {
				before[i] = network.hb[i+1].Load()
			}
			observe := time.NewTimer(2 * time.Second)
			tick := time.NewTicker(20 * time.Millisecond)
		loop:
			for {
				select {
				case early := <-result:
					t.Fatalf("proposal completed during blocked leader persistence: %+v", early)
				case <-observe.C:
					break loop
				case <-tick.C:
					for _, r := range nodes {
						if s := r.Status(); s.Term != term || s.Failure != "" {
							t.Fatalf("Raft changed during disk wait: %+v", s)
						}
					}
				}
			}
			tick.Stop()
			for i := range before {
				if got := network.hb[i+1].Load() - before[i]; got < 10 {
					t.Fatalf("node %d sent only %d heartbeat messages during disk wait", i+1, got)
				}
			}
			if !nodes[leader].Status().IsLeader {
				t.Fatal("leader resigned under disk wait")
			}
			unblock()
			select {
			case committed := <-result:
				if committed.Err != nil {
					t.Fatal(committed.Err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("no progress after disk release")
			}
		})
	}
}

func TestRuntimeRestartReplaysCommittedCommands(t *testing.T) {
	members := testMembers()[:1]
	cfg := Config{Local: members[0], Initial: members, Dir: t.TempDir(), Heartbeat: 10 * time.Millisecond, ElectionTicks: 3}
	network := &testNetwork{nodes: map[uint64]*Runtime{}}
	start := func(a *testApp) *Runtime {
		r, err := Open(cfg, a, &testLink{network: network, id: 1}, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	r := start(&testApp{})
	eventually(t, "single leader", func() bool { return r.Status().CaughtUp })
	result, err := r.Propose([]byte("survives restart"))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-result:
		if result.Err != nil {
			t.Fatal(result.Err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("proposal timeout")
	}
	r.Close()
	cfg.Initial = nil
	app := &testApp{}
	r = start(app)
	defer r.Close()
	eventually(t, "recovered command", func() bool {
		app.mu.Lock()
		defer app.mu.Unlock()
		return len(app.data) == 1 && bytes.Equal(app.data[0], []byte("survives restart"))
	})
}

func TestStorageFailureRevokesRoleBeforeShutdown(t *testing.T) {
	members := testMembers()[:1]
	var fail atomic.Bool
	failure := make(chan error, 1)
	roles := make(chan Role, 32)
	cfg := Config{Local: members[0], Initial: members, Dir: t.TempDir(), Heartbeat: 10 * time.Millisecond, ElectionTicks: 3, beforeSave: func() error {
		if fail.Load() {
			return errors.New("injected sync failure")
		}
		return nil
	}}
	r, err := Open(cfg, &testApp{}, &testLink{network: &testNetwork{nodes: map[uint64]*Runtime{}}, id: 1}, func(role Role) { roles <- role }, func(err error) { failure <- err })
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	eventually(t, "leader", func() bool { return r.Status().CaughtUp })
	fail.Store(true)
	result, err := r.Propose([]byte("must fail"))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-failure:
		if r.Status().IsLeader {
			t.Fatal("failure left authority live")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no failure notification")
	}
	if outcome := <-result; outcome.Err == nil {
		t.Fatal("failed persistence returned success")
	}
}
