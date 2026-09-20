// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"fmt"
	"testing"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

func testMembers() []Member {
	out := make([]Member, 3)
	for i := range out {
		id := uint64(i + 1)
		out[i] = Member{ID: id, Raft: fmt.Sprintf("127.0.0.1:%d", 7100+i), Data: fmt.Sprintf("127.0.0.1:%d", 7300+i), Admin: fmt.Sprintf("127.0.0.1:%d", 7200+i), Principal: fmt.Sprintf("lavik://meta/%d", id)}
	}
	return out
}

func testCore(t *testing.T, id uint64) *core {
	t.Helper()
	members := testMembers()
	data, err := encodeSnapshot(members, nil)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := &pb.Snapshot{Data: data, Metadata: &pb.SnapshotMetadata{Index: new(uint64(1)), Term: new(uint64(1)), ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}}}}
	c, err := newCore(Config{Local: members[id-1], ElectionTicks: 3, MaxPendingBytes: 4 << 20}, genesis{Initial: members}, recovered{snapshot: snapshot, hard: &pb.HardState{Term: new(uint64(1)), Commit: new(uint64(1))}})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func stepCore(t *testing.T, c *core, m *pb.Message) {
	t.Helper()
	if err := c.raw.Step(m); err != nil {
		t.Fatal(err)
	}
}

func TestHeartbeatDoesNotAcknowledgePendingAppend(t *testing.T) {
	c := testCore(t, 2)
	stepCore(t, c, &pb.Message{Type: pb.MsgApp.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(1)), Index: new(uint64(1)), LogTerm: new(uint64(1)), Commit: new(uint64(1)), Entries: []*pb.Entry{{Index: new(uint64(2)), Term: new(uint64(1)), Data: []byte("unpersisted")}}})
	heartbeats, deferred := 0, 0
	for range 21 {
		stepCore(t, c, &pb.Message{Type: pb.MsgHeartbeat.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(1)), Commit: new(uint64(1))})
		if c.raw.HasReady() {
			for _, m := range c.raw.Ready().Messages {
				switch m.GetType() {
				case pb.MsgHeartbeatResp:
					heartbeats++
				case pb.MsgAppResp:
					t.Fatal("append acknowledged before persistence")
				case pb.MsgStorageAppend:
					for _, response := range m.Responses {
						if response.GetType() == pb.MsgHeartbeatResp {
							t.Fatal("heartbeat blocked on disk")
						}
						if response.GetType() == pb.MsgAppResp {
							deferred++
						}
					}
				}
			}
		}
		c.raw.Tick()
	}
	if heartbeats != 21 || deferred == 0 || c.raw.BasicStatus().GetTerm() != 1 || c.raw.BasicStatus().RaftState != raft.StateFollower {
		t.Fatalf("unexpected hb=%d deferred=%d status=%+v", heartbeats, deferred, c.status())
	}
}

func TestVoteWaitsForDurabilityAcrossRestart(t *testing.T) {
	members := testMembers()
	cfg := Config{Local: members[1], Initial: members, Dir: t.TempDir(), ElectionTicks: 3, MaxPendingBytes: 4 << 20}
	disk, _, err := openDisk(cfg)
	if err != nil {
		t.Fatal(err)
	}
	c := testCore(t, 2)
	snap, _ := c.memory.Snapshot()
	if err := disk.save(&pb.Message{Snapshot: snap, Term: new(uint64(1)), Commit: new(uint64(1))}); err != nil {
		t.Fatal(err)
	}
	stepCore(t, c, &pb.Message{Type: pb.MsgVote.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(2)), Index: new(uint64(1)), LogTerm: new(uint64(1))})
	granted := false
	for _, m := range c.raw.Ready().Messages {
		if m.GetType() == pb.MsgVoteResp {
			t.Fatal("vote escaped storage dependency")
		}
		if m.GetType() == pb.MsgStorageAppend {
			if err := disk.save(m); err != nil {
				t.Fatal(err)
			}
			for _, r := range m.Responses {
				if r.GetType() == pb.MsgVoteResp && !r.GetReject() {
					granted = true
				}
			}
		}
	}
	if !granted {
		t.Fatal("missing durable vote")
	}
	if err := disk.wal.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Initial = nil
	disk, rec, err := openDisk(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer disk.wal.Close()
	c, err = newCore(cfg, disk.genesis, rec)
	if err != nil {
		t.Fatal(err)
	}
	if c.raw.BasicStatus().GetVote() != 1 {
		t.Fatalf("lost persisted vote: %+v", c.raw.BasicStatus())
	}
	stepCore(t, c, &pb.Message{Type: pb.MsgVote.Enum(), From: new(uint64(3)), To: new(uint64(2)), Term: new(uint64(2)), Index: new(uint64(1)), LogTerm: new(uint64(1))})
	for _, m := range c.raw.Ready().Messages {
		all := append([]*pb.Message{m}, m.Responses...)
		for _, r := range all {
			if r.GetType() == pb.MsgVoteResp && !r.GetReject() {
				t.Fatal("double vote after restart")
			}
		}
	}
}

func appendTask(t *testing.T, c *core) *pb.Message {
	t.Helper()
	var task *pb.Message
	for _, m := range c.raw.Ready().Messages {
		if m.GetType() == pb.MsgStorageAppend {
			if task != nil {
				t.Fatal("unexpected multiple append tasks in one Ready")
			}
			task = m
			c.noteAppend(m)
		}
	}
	if task == nil {
		t.Fatal("missing append dependency")
	}
	return task
}

func finishAppend(t *testing.T, c *core, task *pb.Message) {
	t.Helper()
	if err := c.persisted(task); err != nil {
		t.Fatal(err)
	}
	for _, response := range task.Responses {
		if response.GetTo() == c.id {
			stepCore(t, c, response)
		}
	}
}

func TestOldAppendCompletionCannotProveReplacementDurable(t *testing.T) {
	c := testCore(t, 2)
	stepCore(t, c, &pb.Message{Type: pb.MsgApp.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(1)), Index: new(uint64(1)), LogTerm: new(uint64(1)), Commit: new(uint64(1)), Entries: []*pb.Entry{
		{Index: new(uint64(2)), Term: new(uint64(1)), Data: []byte("old-2")},
		{Index: new(uint64(3)), Term: new(uint64(1)), Data: []byte("old-3")},
	}})
	old := appendTask(t, c)
	// The next term replaces the suffix while the previous writer is blocked.
	// Its eventual completion is real disk progress, but for the wrong branch.
	stepCore(t, c, &pb.Message{Type: pb.MsgApp.Enum(), From: new(uint64(3)), To: new(uint64(2)), Term: new(uint64(2)), Index: new(uint64(1)), LogTerm: new(uint64(1)), Commit: new(uint64(1)), Entries: []*pb.Entry{
		{Index: new(uint64(2)), Term: new(uint64(2)), Data: []byte("replacement")},
	}})
	replacement := appendTask(t, c)
	finishAppend(t, c, old)
	if c.durable != 3 || c.status().Durable != 1 || c.status().Term != 2 {
		t.Fatalf("old branch completion crossed replacement fence: %+v", c.status())
	}
	finishAppend(t, c, replacement)
	if c.status().Durable != 2 {
		t.Fatalf("replacement did not become durable: %+v", c.status())
	}
	term, err := c.memory.Term(2)
	if err != nil || term != 2 {
		t.Fatalf("wrong recovered branch term=%d error=%v", term, err)
	}
}

func TestPendingSnapshotKeepsOldCompletionBehindInstalledPrefix(t *testing.T) {
	c := testCore(t, 2)
	stepCore(t, c, &pb.Message{Type: pb.MsgApp.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(1)), Index: new(uint64(1)), LogTerm: new(uint64(1)), Entries: []*pb.Entry{
		{Index: new(uint64(2)), Term: new(uint64(1))},
		{Index: new(uint64(3)), Term: new(uint64(1))},
	}})
	old := appendTask(t, c)
	data, err := encodeSnapshot(testMembers(), []byte("application at index 5"))
	if err != nil {
		t.Fatal(err)
	}
	stepCore(t, c, &pb.Message{Type: pb.MsgSnap.Enum(), From: new(uint64(3)), To: new(uint64(2)), Term: new(uint64(2)), Snapshot: &pb.Snapshot{
		Data: data, Metadata: &pb.SnapshotMetadata{Index: new(uint64(5)), Term: new(uint64(2)), ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}}},
	}})
	install := appendTask(t, c)
	finishAppend(t, c, old)
	if c.status().Durable != 1 || c.status().Applied != 1 {
		t.Fatalf("pending snapshot exposed uninstalled state: %+v", c.status())
	}
	finishAppend(t, c, install)
	if c.status().Durable != 5 || c.status().Applied != 5 || c.status().Snapshot != 5 {
		t.Fatalf("snapshot completion did not publish its exact cut: %+v", c.status())
	}
}

func TestCompactedSnapshotParticipatesInVoteFreshness(t *testing.T) {
	for _, candidate := range []struct {
		name        string
		index, term uint64
		reject      bool
	}{
		{"shorter-same-term", 99, 5, true},
		{"equal-index-older-term", 100, 4, true},
		{"longer-older-term", 101, 4, true},
		{"equal-boundary", 100, 5, false},
		{"longer-same-term", 101, 5, false},
		{"shorter-newer-term", 99, 6, false},
	} {
		t.Run(candidate.name, func(t *testing.T) {
			members := testMembers()
			data, err := encodeSnapshot(members, nil)
			if err != nil {
				t.Fatal(err)
			}
			c, err := newCore(Config{Local: members[1], ElectionTicks: 3, MaxPendingBytes: 4 << 20}, genesis{Initial: members}, recovered{
				snapshot: &pb.Snapshot{Data: data, Metadata: &pb.SnapshotMetadata{Index: new(uint64(100)), Term: new(uint64(5)), ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}}}},
				hard:     &pb.HardState{Term: new(uint64(6)), Commit: new(uint64(100))},
			})
			if err != nil {
				t.Fatal(err)
			}
			stepCore(t, c, &pb.Message{Type: pb.MsgVote.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(7)), Index: new(candidate.index), LogTerm: new(candidate.term)})
			found := false
			for _, m := range c.raw.Ready().Messages {
				if m.GetType() == pb.MsgVoteResp && !m.GetReject() {
					t.Fatal("vote granted before persistence")
				}
				for _, response := range append([]*pb.Message{m}, m.Responses...) {
					if response.GetType() == pb.MsgVoteResp {
						found = true
						if response.GetReject() != candidate.reject {
							t.Fatalf("wrong snapshot vote decision: %v", response)
						}
					}
				}
			}
			if !found {
				t.Fatal("missing vote decision")
			}
		})
	}
}
