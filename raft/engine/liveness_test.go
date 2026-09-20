// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"encoding/binary"
	"testing"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type generationLink struct{ tx, rx uint64 }

func (l *generationLink) Generations(uint64) (uint64, uint64) { return l.tx, l.rx }
func (*generationLink) Send(*pb.Message) bool                 { return true }
func (*generationLink) Update([]Member)                       {}
func (*generationLink) Close()                                {}

func TestHeartbeatFreshness(t *testing.T) {
	for _, scenario := range []string{"fresh", "duplicate", "expired", "old-term", "old-boot", "old-outgoing-connection", "old-incoming-connection", "revoked"} {
		t.Run(scenario, func(t *testing.T) {
			link := &generationLink{tx: 1, rx: 1}
			r := &Runtime{cfg: Config{Heartbeat: time.Millisecond * 100, ElectionTicks: 3}, core: testCore(t, 1), transport: link, liveness: map[uint64]*peerLiveness{}}
			m := r.stampHeartbeat(&pb.Message{Type: pb.MsgHeartbeat.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(1)), Context: []byte("opaque read index")})
			m.Type = pb.MsgHeartbeatResp.Enum()
			m.From = new(uint64(2))
			m.To = new(uint64(1))
			binary.BigEndian.PutUint64(m.Context[40:], 55)
			switch scenario {
			case "duplicate":
				if !r.acceptHeartbeat(proto.Clone(m).(*pb.Message)) {
					t.Fatal("first reply rejected")
				}
			case "expired":
				p := r.liveness[2].challenges[1]
				p.sent = time.Now().Add(-time.Second)
				r.liveness[2].challenges[1] = p
			case "old-term":
				m.Term = new(uint64(0))
			case "old-boot":
				m.Context[24] ^= 1
			case "old-outgoing-connection":
				link.tx++
			case "old-incoming-connection":
				link.rx++
			case "revoked":
				r.epochRevoked = true
			}
			accepted := r.acceptHeartbeat(m)
			if accepted != (scenario == "fresh") {
				t.Fatalf("accepted=%v", accepted)
			}
			if accepted && (r.liveness[2].applied != 55 || string(m.Context) != "opaque read index") {
				t.Fatal("lost actual apply progress or original Raft context")
			}
		})
	}
}

func TestPublishedEvidenceExpiresOnRevocationAndReconnect(t *testing.T) {
	for _, scenario := range []string{"retired", "removed", "descriptor-changed", "reconnected"} {
		t.Run(scenario, func(t *testing.T) {
			link := &generationLink{tx: 1, rx: 1}
			r := &Runtime{cfg: Config{Local: testMembers()[0], Heartbeat: 100 * time.Millisecond, ElectionTicks: 3}, core: testCore(t, 1), disk: &diskStore{}, transport: link, liveness: map[uint64]*peerLiveness{}}
			m := r.stampHeartbeat(&pb.Message{Type: pb.MsgHeartbeat.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(1))})
			m.Type, m.From, m.To = pb.MsgHeartbeatResp.Enum(), new(uint64(2)), new(uint64(1))
			if !r.acceptHeartbeat(m) || !r.quorumLive(time.Now()) {
				t.Fatal("missing initial quorum evidence")
			}
			switch scenario {
			case "retired", "descriptor-changed":
				r.checkIdentities = true
				bindings := make([]Binding, 0, 3)
				for _, member := range testMembers() {
					bindings = append(bindings, Binding{ID: member.ID, Principal: member.Principal, Data: member.Data, Admin: member.Admin})
				}
				if scenario == "retired" {
					bindings[1].Retired = true
				} else {
					bindings[1].Data = "127.0.0.1:9999"
				}
				r.setBindings(bindings)
			case "removed":
				delete(r.core.members, 2)
			case "reconnected":
				link.rx++
			}
			if r.quorumLive(time.Now()) || r.peerLive(2, time.Now()) {
				t.Fatal("invalidated evidence still contributes to quorum")
			}
		})
	}
}

func TestStorageSaturationCannotHideHigherTerm(t *testing.T) {
	r := &Runtime{cfg: Config{Local: testMembers()[0], Heartbeat: 100 * time.Millisecond, ElectionTicks: 3, QueueCapacity: 8, MaxPendingBytes: 2 << 20},
		core: testCore(t, 1), disk: &diskStore{}, transport: &generationLink{}, protocolLeader: true, protocolTerm: 1,
		leaderSince: time.Now(), pendingTasks: 40, liveness: map[uint64]*peerLiveness{2: {confirmed: time.Now()}}}
	role := Role{Term: 1, IsLeader: true, CaughtUp: true}
	if !r.authorityRole(role).IsLeader {
		t.Fatal("missing initial authority")
	}
	for _, term := range []uint64{2, 5, 3} {
		if err := r.step(&pb.Message{Type: pb.MsgHeartbeat.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(term)}); err != nil {
			t.Fatal(err)
		}
	}
	if r.deferredTerm == nil || r.deferredTerm.GetTerm() != 5 || r.authorityRole(role).IsLeader {
		t.Fatal("full storage hid a higher term or retained the wrong observation")
	}
	if r.core.status().Term != 1 {
		t.Fatal("saturated queue admitted another persistence task")
	}
	// A concurrent committed removal cannot make an authenticated, already
	// observed term disappear when the older disk completion finally arrives.
	r.core.conf = r.core.raw.ApplyConfChange(&pb.ConfChange{Type: pb.ConfChangeRemoveNode.Enum(), NodeId: new(uint64(2))})
	delete(r.core.members, 2)
	r.pendingTasks = 0
	if err := r.advanceDeferredTerm(); err != nil {
		t.Fatal(err)
	}
	if r.core.status().Term != 5 || r.core.status().IsLeader {
		t.Fatal("deferred term did not demote before processing more work")
	}
}

func TestHigherTermHeartbeatReplyDemotesWithoutCountingStaleProof(t *testing.T) {
	r := &Runtime{cfg: Config{Local: testMembers()[0], QueueCapacity: 8, MaxPendingBytes: 2 << 20}, core: testCore(t, 1), liveness: map[uint64]*peerLiveness{}}
	if err := r.step(&pb.Message{Type: pb.MsgHeartbeatResp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(2)), Context: []byte("stale")}); err != nil {
		t.Fatal(err)
	}
	if r.core.status().Term != 2 || len(r.liveness) != 0 {
		t.Fatal("higher term ignored or stale reply counted as quorum evidence")
	}
}
