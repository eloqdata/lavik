// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
)

type filteredLink struct {
	testLink
	drop *atomic.Bool
}

func (l *filteredLink) Send(m *pb.Message) bool {
	if l.drop.Load() && controlMessage(m.GetType()) {
		return true
	}
	return l.testLink.Send(m)
}

func openTestCluster(t *testing.T, customize func(int, *Config) Application, drop *atomic.Bool) []*Runtime {
	t.Helper()
	members := testMembers()
	network := &testNetwork{nodes: map[uint64]*Runtime{}}
	var nodes []*Runtime
	if drop == nil {
		drop = &atomic.Bool{}
	}
	for i := range members {
		cfg := Config{Local: members[i], Initial: members, Dir: t.TempDir(), Heartbeat: 100 * time.Millisecond, ElectionTicks: 3}
		var app Application = &testApp{}
		if customize != nil {
			app = customize(i, &cfg)
		}
		r, err := Open(cfg, app, &filteredLink{testLink: testLink{network: network, id: members[i].ID}, drop: drop}, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		nodes = append(nodes, r)
		t.Cleanup(r.Close)
		network.mu.Lock()
		network.nodes[members[i].ID] = r
		network.mu.Unlock()
	}
	return nodes
}

func clusterLeader(t *testing.T, nodes []*Runtime) *Runtime {
	t.Helper()
	var leader *Runtime
	eventually(t, "cluster leader", func() bool {
		for _, r := range nodes {
			if s := r.Status(); s.IsLeader && s.CaughtUp {
				leader = r
				return true
			}
		}
		return false
	})
	return leader
}

func appliedProposal(t *testing.T, r *Runtime, data string) Result {
	t.Helper()
	result, err := r.Propose([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-result:
		if got.Err != nil {
			t.Fatal(got.Err)
		}
		return got
	case <-time.After(5 * time.Second):
		t.Fatal("proposal timed out")
	}
	return Result{}
}

func TestEveryStorageExecutorStageLeavesStableTermLive(t *testing.T) {
	for _, stage := range []string{"wal-write", "wal-sync", "snapshot-file", "snapshot-rename", "snapshot-directory", "snapshot-publication", "snapshot-release", "wal-gc", "snapshot-gc"} {
		t.Run(stage, func(t *testing.T) {
			var target atomic.Uint64
			entered, release := make(chan struct{}), make(chan struct{})
			var enterOnce, releaseOnce sync.Once
			nodes := openTestCluster(t, func(i int, cfg *Config) Application {
				cfg.beforeIO = func(point string) error {
					if target.Load() == uint64(i+1) && point == stage {
						enterOnce.Do(func() { close(entered); <-release })
					}
					return nil
				}
				return &testApp{}
			}, nil)
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			leader := clusterLeader(t, nodes)
			baseline := appliedProposal(t, leader, "before barrier")
			eventually(t, "baseline applied everywhere", func() bool {
				for _, r := range nodes {
					if r.Status().Applied < baseline.Index {
						return false
					}
				}
				return true
			})
			term := leader.Status().Term
			target.Store(leader.cfg.Local.ID)
			var result <-chan Result
			var err error
			if stage == "wal-write" || stage == "wal-sync" {
				result, err = leader.Propose([]byte("blocked durable write"))
			} else {
				result, err = leader.Snapshot()
			}
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("fault stage not reached")
			}
			deadline := time.NewTimer(2 * time.Second)
			defer deadline.Stop()
			tick := time.NewTicker(20 * time.Millisecond)
			defer tick.Stop()
		loop:
			for {
				select {
				case <-deadline.C:
					break loop
				case <-tick.C:
					for _, r := range nodes {
						if s := r.Status(); s.Term != term || s.Failure != "" {
							t.Fatalf("disk wait changed consensus: %+v", s)
						}
					}
					if !leader.Status().CaughtUp {
						t.Fatal("disk wait revoked live majority")
					}
				}
			}
			if stage == "wal-write" || stage == "wal-sync" || stage == "snapshot-publication" || stage == "snapshot-release" {
				select {
				case got := <-result:
					t.Fatalf("durability response escaped blocked stage: %+v", got)
				default:
				}
			}
			unblock()
			select {
			case got := <-result:
				if got.Err != nil {
					t.Fatal(got.Err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("no completion after releasing storage")
			}
			appliedProposal(t, leader, "after barrier")
		})
	}
}

func TestControlPartitionRevokesAuthorityDespiteLiveLogConnections(t *testing.T) {
	var drop atomic.Bool
	nodes := openTestCluster(t, nil, &drop)
	leader := clusterLeader(t, nodes)
	appliedProposal(t, leader, "baseline")
	drop.Store(true)
	deadline := time.Now().Add(600 * time.Millisecond)
	for time.Now().Before(deadline) && leader.Status().IsLeader {
		time.Sleep(5 * time.Millisecond)
	}
	if leader.Status().IsLeader || leader.Status().CaughtUp {
		t.Fatal("replication-only traffic sustained authority")
	}
	result, err := leader.Propose([]byte("must not acknowledge"))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-result:
		if got.Err == nil {
			t.Fatal("partitioned leader acknowledged write")
		}
	case <-time.After(time.Second):
		t.Fatal("proposal admission did not revoke")
	}
	drop.Store(false)
	leader = clusterLeader(t, nodes)
	appliedProposal(t, leader, "after control reconnect")
}

type blockingApp struct {
	testApp
	pause   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (a *blockingApp) Apply(index uint64, data []byte) ([]byte, error) {
	if a.pause.CompareAndSwap(true, false) {
		close(a.entered)
		<-a.release
	}
	return a.testApp.Apply(index, data)
}

func TestFollowerApplyStallDoesNotChangeOrdinaryMajorityCompletion(t *testing.T) {
	var apps [3]*blockingApp
	nodes := openTestCluster(t, func(i int, _ *Config) Application {
		a := &blockingApp{entered: make(chan struct{}), release: make(chan struct{})}
		apps[i] = a
		return a
	}, nil)
	leader := clusterLeader(t, nodes)
	baseline := appliedProposal(t, leader, "before apply stall")
	eventually(t, "replicated baseline", func() bool {
		for _, r := range nodes {
			if r.Status().Applied < baseline.Index {
				return false
			}
		}
		return true
	})
	victim := int(leader.cfg.Local.ID % 3)
	apps[victim].pause.Store(true)
	defer close(apps[victim].release)
	got := appliedProposal(t, leader, "quorum need not wait for follower application")
	select {
	case <-apps[victim].entered:
	case <-time.After(time.Second):
		t.Fatal("apply barrier not reached")
	}
	if nodes[victim].Status().Applied >= got.Index {
		t.Fatal("published unapplied entry")
	}
	term := leader.Status().Term
	time.Sleep(2 * time.Second)
	if leader.Status().Term != term || !leader.Status().CaughtUp {
		t.Fatal("follower apply stalled consensus")
	}
	if leader.Status().PeerApplied[uint64(victim+1)] >= got.Index {
		t.Fatal("heartbeat reported durable/commit as applied")
	}
}

func TestInputBudgetPreservesControlCapacity(t *testing.T) {
	r := &Runtime{cfg: Config{Local: testMembers()[0], MaxPendingBytes: 2 << 20}, ingress: make(chan *pb.Message, 8), control: make(chan *pb.Message, 8), stop: make(chan struct{})}
	message := &pb.Message{Type: pb.MsgApp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Entries: []*pb.Entry{{Data: make([]byte, 1<<20)}}}
	for i := 0; i < 8; i++ {
		if err := r.Step(message); err != nil {
			t.Fatal(err)
		}
	}
	if !errors.Is(r.Step(message), ErrBusy) {
		t.Fatal("unbounded ingress")
	}
	if err := r.Step(&pb.Message{Type: pb.MsgHeartbeat.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(1))}); err != nil {
		t.Fatal("log saturation consumed control budget", err)
	}
	if r.inputBytes[laneLog].used.Load() > laneBudget(laneLog) {
		t.Fatal("budget overrun")
	}
	if err := r.Step(&pb.Message{Type: pb.MsgStorageAppend.Enum(), From: new(uint64(2)), To: new(uint64(1))}); err == nil {
		t.Fatal("remote forged storage completion")
	}
}
