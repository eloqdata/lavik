// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// core is deliberately synchronous and contains no file, socket, or application
// calls. Its owner must deliver append completions in submission order. Ready
// responses remain attached to their task until that task has really finished.
type core struct {
	raw      *raft.RawNode
	memory   *raft.MemoryStorage
	id       uint64
	conf     *pb.ConfState
	members  map[uint64]Member
	applied  uint64
	durable  uint64
	snapshot uint64
	// RawNode restores configuration when it accepts a snapshot, before disk
	// persistence and application installation advance snapshot/applied.
	restoreIndex      uint64
	configIndex       uint64
	logBytes          map[uint64]uint64
	retainedBytes     uint64
	logicalLast       uint64
	pendingOverwrites map[*pb.Message]uint64
}

func newCore(cfg Config, g genesis, rec recovered) (*core, error) {
	c := &core{id: cfg.Local.ID, memory: raft.NewMemoryStorage(), members: map[uint64]Member{}, conf: &pb.ConfState{}, logBytes: map[uint64]uint64{}, pendingOverwrites: map[*pb.Message]uint64{}}
	for _, m := range g.Initial {
		c.members[m.ID] = m
	}
	if !raft.IsEmptySnap(rec.snapshot) {
		if err := c.memory.ApplySnapshot(rec.snapshot); err != nil {
			return nil, err
		}
		members, _, err := decodeSnapshot(rec.snapshot.Data)
		if err != nil {
			return nil, err
		}
		c.members = members
		c.conf = rec.snapshot.Metadata.ConfState
		if err := validateSnapshotConfig(c.conf, c.members); err != nil {
			return nil, err
		}
		c.applied = rec.snapshot.Metadata.GetIndex()
		c.snapshot = c.applied
		c.restoreIndex = c.applied
		c.configIndex = c.applied
	}
	if err := c.memory.Append(rec.entries); err != nil {
		return nil, err
	}
	c.accountEntries(rec.entries)
	if !raft.IsEmptyHardState(rec.hard) {
		if err := c.memory.SetHardState(rec.hard); err != nil {
			return nil, err
		}
	}
	c.durable, _ = c.memory.LastIndex()
	c.logicalLast = c.durable
	raw, err := raft.NewRawNode(&raft.Config{
		ID: cfg.Local.ID, ElectionTick: cfg.ElectionTicks, HeartbeatTick: 1,
		Storage: c.memory, Applied: c.applied, MaxSizePerMsg: 1 << 20,
		MaxCommittedSizePerReady: 1 << 20, MaxUncommittedEntriesSize: cfg.MaxPendingBytes,
		MaxInflightMsgs: 16, MaxInflightBytes: 16 << 20,
		CheckQuorum: true, PreVote: true, DisableProposalForwarding: true,
		AsyncStorageWrites: true, Logger: quietLogger{},
	})
	if err != nil {
		return nil, err
	}
	c.raw = raw
	if c.durable == 0 && raft.IsEmptyHardState(rec.hard) && len(g.Initial) > 0 {
		peers := make([]raft.Peer, 0, len(g.Initial))
		for _, m := range g.Initial {
			data, err := json.Marshal(m)
			if err != nil {
				return nil, err
			}
			peers = append(peers, raft.Peer{ID: m.ID, Context: data})
		}
		if err := raw.Bootstrap(peers); err != nil {
			return nil, err
		}
	}
	return c, nil
}

func (c *core) status() Status {
	b := c.raw.BasicStatus()
	// Catch-up is the current-term application fence, not perpetual equality
	// with Commit. Normal pipelined writes must not repeatedly withdraw Data
	// authority while their committed entries are waiting to apply.
	appliedTerm, _ := c.memory.Term(c.applied)
	learners := map[uint64]bool{}
	for _, id := range c.conf.GetLearners() {
		learners[id] = true
	}
	first, _ := c.memory.FirstIndex()
	return Status{Role: Role{Term: b.GetTerm(), Leader: b.Lead, IsLeader: b.RaftState == raft.StateLeader,
		CaughtUp: b.RaftState == raft.StateLeader && c.applied > 0 && appliedTerm == b.GetTerm()},
		Commit: b.GetCommit(), Applied: c.applied, Durable: c.durableFrontier(), Snapshot: c.snapshot, Members: c.memberList(), ConfigIndex: c.configIndex, Learners: learners, FirstIndex: first, RetainedBytes: c.retainedBytes}
}

// The memory store can temporarily hold an older branch while a replacement
// is queued for ordered persistence. Report only the still-proven prefix;
// neither a larger old completion nor its former tail establishes durability
// on the new branch. RawNode independently qualifies its attached responses.
func (c *core) noteAppend(m *pb.Message) {
	if !raft.IsEmptySnap(m.Snapshot) {
		// Only a Ready carrying an actual storage snapshot establishes this
		// fence. Rejected snapshots and log-matching commit fast-forwards do
		// not restore RawNode's configuration.
		c.restoreIndex = max(c.restoreIndex, m.Snapshot.Metadata.GetIndex())
		c.pendingOverwrites[m] = min(c.applied, c.durable)
		c.logicalLast = m.Snapshot.Metadata.GetIndex()
	}
	if len(m.Entries) > 0 {
		first := m.Entries[0].GetIndex()
		if first <= c.logicalLast {
			floor := first - 1
			if previous, ok := c.pendingOverwrites[m]; ok {
				floor = min(floor, previous)
			}
			c.pendingOverwrites[m] = floor
		}
		c.logicalLast = m.Entries[len(m.Entries)-1].GetIndex()
	}
}

func (c *core) durableFrontier() uint64 {
	index := min(c.durable, c.logicalLast)
	for _, floor := range c.pendingOverwrites {
		index = min(index, floor)
	}
	return index
}

func (c *core) memberList() []Member {
	out := make([]Member, 0, len(c.members))
	for _, m := range c.members {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// persisted updates only the memory view. Disk and snapshot installation have
// already completed on their executors; only now may attached replies escape.
func (c *core) persisted(m *pb.Message) error {
	if !raft.IsEmptySnap(m.Snapshot) {
		if err := c.memory.ApplySnapshot(m.Snapshot); err != nil {
			return err
		}
		members, _, err := decodeSnapshot(m.Snapshot.Data)
		if err != nil {
			return err
		}
		c.members = members
		c.conf = m.Snapshot.Metadata.ConfState
		if err := validateSnapshotConfig(c.conf, c.members); err != nil {
			return err
		}
		c.applied = m.Snapshot.Metadata.GetIndex()
		c.snapshot = c.applied
		c.configIndex = c.applied
		c.logBytes = map[uint64]uint64{}
		c.retainedBytes = 0
	}
	if err := c.memory.Append(m.Entries); err != nil {
		return err
	}
	c.accountEntries(m.Entries)
	hs := &pb.HardState{Term: m.Term, Vote: m.Vote, Commit: m.Commit}
	if !raft.IsEmptyHardState(hs) {
		if err := c.memory.SetHardState(hs); err != nil {
			return err
		}
	}
	c.durable, _ = c.memory.LastIndex()
	delete(c.pendingOverwrites, m)
	return nil
}

func validateSnapshotConfig(conf *pb.ConfState, members map[uint64]Member) error {
	ids := map[uint64]bool{}
	for _, set := range [][]uint64{conf.GetVoters(), conf.GetLearners(), conf.GetVotersOutgoing(), conf.GetLearnersNext()} {
		for _, id := range set {
			if _, ok := members[id]; !ok {
				return errors.New("snapshot configuration has no member descriptor")
			}
			ids[id] = true
		}
	}
	if len(ids) != len(members) || len(conf.GetVoters()) == 0 {
		return errors.New("snapshot membership contradicts Raft configuration")
	}
	return nil
}

// Account the retained logical log, including the reserved replication suffix.
// Pending I/O bytes alone are not a safe snapshot-failure disk-growth signal.
func (c *core) accountEntries(entries []*pb.Entry) {
	if len(entries) == 0 {
		return
	}
	first, _ := c.memory.FirstIndex()
	for index := entries[0].GetIndex(); index <= c.durable; index++ {
		c.retainedBytes -= c.logBytes[index]
		delete(c.logBytes, index)
	}
	for _, e := range entries {
		if e.GetIndex() < first {
			continue
		}
		size := uint64(proto.Size(e) + 32) // framing/checksum overhead, conservatively rounded
		c.logBytes[e.GetIndex()] = size
		c.retainedBytes += size
	}
}

func (c *core) compact(index uint64) error {
	first, _ := c.memory.FirstIndex()
	if err := c.memory.Compact(index); err != nil {
		return err
	}
	for i := first; i <= index; i++ {
		c.retainedBytes -= c.logBytes[i]
		delete(c.logBytes, i)
	}
	return nil
}

// appliedEntry runs on the core only after the ordered application executor
// confirms the entry. A snapshot accepted by RawNode supersedes covered
// configuration changes even while its application installation is pending.
func (c *core) appliedEntry(e *pb.Entry) error {
	if e.GetIndex() != c.applied+1 {
		return fmt.Errorf("apply gap: have %d, got %d", c.applied, e.GetIndex())
	}
	if e.GetIndex() <= c.restoreIndex {
		// The application executor must still drain and acknowledge its old
		// ordered jobs. Their membership effects belong to a prefix RawNode
		// has already replaced: replaying ApplyConfChange would corrupt the
		// restored voter/learner sets. Installation publishes the new member
		// descriptors and configuration together; capture waits for that cut.
		c.applied = e.GetIndex()
		return nil
	}
	switch e.GetType() {
	case pb.EntryConfChange:
		var cc pb.ConfChange
		if err := proto.Unmarshal(e.Data, &cc); err != nil {
			return err
		}
		if err := c.updateMember(cc.GetNodeId(), cc.GetType(), cc.Context); err != nil {
			return err
		}
		c.conf = c.raw.ApplyConfChange(&cc)
		c.configIndex = e.GetIndex()
	case pb.EntryConfChangeV2:
		var cc pb.ConfChangeV2
		if err := proto.Unmarshal(e.Data, &cc); err != nil {
			return err
		}
		// Lavik serializes one learner/add/remove intent at a time. Joint
		// changes need a corresponding durable workflow before being enabled.
		if len(cc.Changes) > 1 {
			return errors.New("unsupported multi-member configuration change")
		}
		for _, change := range cc.Changes {
			if err := c.updateMember(change.GetNodeId(), change.GetType(), cc.Context); err != nil {
				return err
			}
		}
		c.conf = c.raw.ApplyConfChange(&cc)
		c.configIndex = e.GetIndex()
	}
	c.applied = e.GetIndex()
	return nil
}

func (c *core) updateMember(id uint64, kind pb.ConfChangeType, data []byte) error {
	if kind == pb.ConfChangeRemoveNode {
		delete(c.members, id)
		return nil
	}
	if len(data) == 0 {
		if _, ok := c.members[id]; !ok {
			return errors.New("member has no durable descriptor")
		}
		return nil
	}
	var m Member
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	if err := m.validate(); err != nil {
		return err
	}
	if m.ID != id {
		return errors.New("configuration descriptor ID mismatch")
	}
	c.members[id] = m
	return nil
}

func encodeSnapshot(members []Member, app []byte) ([]byte, error) {
	meta, err := json.Marshal(members)
	if err != nil {
		return nil, err
	}
	if len(meta) > 1<<20 || len(app) > 512<<20 {
		return nil, errors.New("snapshot exceeds budget")
	}
	out := make([]byte, 8+len(meta)+len(app))
	copy(out, "LMS1")
	binary.LittleEndian.PutUint32(out[4:], uint32(len(meta)))
	copy(out[8:], meta)
	copy(out[8+len(meta):], app)
	return out, nil
}

func decodeSnapshot(data []byte) (map[uint64]Member, []byte, error) {
	if len(data) < 8 || string(data[:4]) != "LMS1" {
		return nil, nil, errors.New("invalid Meta snapshot envelope")
	}
	n := uint64(binary.LittleEndian.Uint32(data[4:]))
	if n > 1<<20 || n > uint64(len(data)-8) || uint64(len(data))-8-n > 512<<20 {
		return nil, nil, errors.New("invalid snapshot lengths")
	}
	var members []Member
	if err := json.Unmarshal(data[8:8+n], &members); err != nil {
		return nil, nil, err
	}
	if len(members) > 1024 {
		return nil, nil, errors.New("too many snapshot members")
	}
	out := map[uint64]Member{}
	for _, m := range members {
		if err := m.validate(); err != nil {
			return nil, nil, err
		}
		if _, ok := out[m.ID]; ok {
			return nil, nil, errors.New("duplicate snapshot member")
		}
		out[m.ID] = m
	}
	return out, data[8+n:], nil
}

// Never log synchronously from the consensus loop. Fatal consistency failures
// still fail stop instead of allowing a malformed state machine to continue.
type quietLogger struct{}

func (quietLogger) Debug(...interface{})              {}
func (quietLogger) Debugf(string, ...interface{})     {}
func (quietLogger) Info(...interface{})               {}
func (quietLogger) Infof(string, ...interface{})      {}
func (quietLogger) Warning(...interface{})            {}
func (quietLogger) Warningf(string, ...interface{})   {}
func (quietLogger) Error(...interface{})              {}
func (quietLogger) Errorf(string, ...interface{})     {}
func (quietLogger) Fatal(v ...interface{})            { panic(fmt.Sprint(v...)) }
func (quietLogger) Fatalf(f string, v ...interface{}) { panic(fmt.Sprintf(f, v...)) }
func (quietLogger) Panic(v ...interface{})            { panic(fmt.Sprint(v...)) }
func (quietLogger) Panicf(f string, v ...interface{}) { panic(fmt.Sprintf(f, v...)) }
