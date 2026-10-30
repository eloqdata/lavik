// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"fmt"
	"testing"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
)

// Exercise the actual owner publication and detached control-plane read without
// disk or network noise. These are allocation/CPU benchmarks, not proposal QPS.
func BenchmarkRuntimeObservation(b *testing.B) {
	for _, count := range []int{3, 5, 9} {
		b.Run(fmt.Sprintf("members=%d", count), func(b *testing.B) {
			members := make([]Member, count)
			voters := make([]uint64, count)
			for i := range members {
				id := uint64(i + 1)
				members[i] = Member{ID: id, Raft: fmt.Sprintf("127.0.0.1:%d", 7100+i), Data: fmt.Sprintf("127.0.0.1:%d", 7300+i), Admin: fmt.Sprintf("127.0.0.1:%d", 7200+i), Principal: fmt.Sprintf("lavik://meta/%d", id)}
				voters[i] = id
			}
			data, err := encodeSnapshot(members, nil)
			if err != nil {
				b.Fatal(err)
			}
			cfg := Config{Local: members[0], Heartbeat: time.Hour, ElectionTicks: 3, MaxPendingBytes: 4 << 20}
			g := genesis{Initial: members}
			snapshot := &pb.Snapshot{Data: data, Metadata: &pb.SnapshotMetadata{Index: new(uint64(1)), Term: new(uint64(1)), ConfState: &pb.ConfState{Voters: voters}}}
			c, err := newCore(cfg, g, recovered{snapshot: snapshot, hard: &pb.HardState{Term: new(uint64(1)), Commit: new(uint64(1))}})
			if err != nil {
				b.Fatal(err)
			}
			if err := c.raw.Campaign(); err != nil {
				b.Fatal(err)
			}
			for _, kind := range []pb.MessageType{pb.MsgPreVoteResp, pb.MsgVoteResp} {
				for _, id := range voters[1:] {
					if err := c.raw.Step(&pb.Message{Type: kind.Enum(), From: new(id), To: new(uint64(1)), Term: new(uint64(2))}); err != nil {
						b.Fatal(err)
					}
				}
			}
			if !c.status().IsLeader {
				b.Fatal("benchmark requires leader progress")
			}
			r := &Runtime{cfg: cfg, core: c, disk: &diskStore{genesis: g}, protocolLeader: true, protocolTerm: 2, liveness: map[uint64]*peerLiveness{}}
			for _, id := range voters[1:] {
				r.liveness[id] = &peerLiveness{confirmed: time.Now(), applied: 1}
			}
			r.publish()
			b.Run("Status", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					_ = r.Status()
				}
			})
			b.Run("Publish", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					r.publish()
				}
			})
		})
	}
}
