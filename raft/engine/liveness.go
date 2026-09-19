// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"bytes"
	"encoding/binary"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const heartbeatHeader = 48

type heartbeatProof struct {
	sent   time.Time
	term   uint64
	tx, rx uint64
}
type peerLiveness struct {
	challenges map[uint64]heartbeatProof
	confirmed  time.Time
	applied    uint64
	tx, rx     uint64
}
type connectionGenerations interface{ Generations(uint64) (uint64, uint64) }

func (r *Runtime) generations(id uint64) (uint64, uint64) {
	if t, ok := r.transport.(connectionGenerations); ok {
		return t.Generations(id)
	}
	return 0, 0
}

func (r *Runtime) livenessWindow() time.Duration {
	return r.cfg.Heartbeat * time.Duration(r.cfg.ElectionTicks)
}

// stampHeartbeat retains Raft's opaque ReadIndex context after our header. The
// timestamp is local and deliberately precedes network enqueue, so queueing can
// only shorten validity. A delayed response never buys a fresh full window.
func (r *Runtime) stampHeartbeat(m *pb.Message) *pb.Message {
	if m.GetType() == pb.MsgHeartbeat {
		now := time.Now()
		r.heartbeatSeq++
		p := r.liveness[m.GetTo()]
		if p == nil {
			p = &peerLiveness{challenges: map[uint64]heartbeatProof{}}
			r.liveness[m.GetTo()] = p
		}
		for seq, proof := range p.challenges {
			if now.Sub(proof.sent) >= r.livenessWindow() {
				delete(p.challenges, seq)
			}
		}
		if len(p.challenges) >= r.cfg.ElectionTicks+1 {
			return nil
		}
		tx, rx := r.generations(m.GetTo())
		p.challenges[r.heartbeatSeq] = heartbeatProof{sent: now, term: m.GetTerm(), tx: tx, rx: rx}
		cloned := proto.Clone(m).(*pb.Message)
		cloned.Context = make([]byte, heartbeatHeader+len(m.Context))
		copy(cloned.Context, "LHB1")
		binary.BigEndian.PutUint64(cloned.Context[8:], r.heartbeatSeq)
		binary.BigEndian.PutUint64(cloned.Context[16:], m.GetTerm())
		copy(cloned.Context[24:40], r.heartbeatNonce[:])
		copy(cloned.Context[heartbeatHeader:], m.Context)
		return cloned
	}
	if m.GetType() == pb.MsgHeartbeatResp {
		if len(m.Context) < heartbeatHeader || string(m.Context[:4]) != "LHB1" {
			return nil
		}
		cloned := proto.Clone(m).(*pb.Message)
		binary.BigEndian.PutUint64(cloned.Context[40:], r.core.applied)
		return cloned
	}
	return m
}

func (r *Runtime) acceptHeartbeat(m *pb.Message) bool {
	if m.GetType() != pb.MsgHeartbeatResp {
		return true
	}
	if len(m.Context) < heartbeatHeader || string(m.Context[:4]) != "LHB1" || !bytes.Equal(m.Context[24:40], r.heartbeatNonce[:]) {
		return false
	}
	if r.epochRevoked {
		return false
	}
	p := r.liveness[m.GetFrom()]
	if p == nil {
		return false
	}
	seq := binary.BigEndian.Uint64(m.Context[8:])
	proof, ok := p.challenges[seq]
	if !ok {
		return false
	}
	delete(p.challenges, seq)
	tx, rx := r.generations(m.GetFrom())
	now := time.Now()
	if proof.term != m.GetTerm() || proof.term != r.core.raw.BasicStatus().GetTerm() || binary.BigEndian.Uint64(m.Context[16:]) != proof.term || proof.tx != tx || proof.rx != rx || now.Sub(proof.sent) >= r.livenessWindow() {
		return false
	}
	if proof.sent.After(p.confirmed) {
		p.confirmed = proof.sent
		p.applied = binary.BigEndian.Uint64(m.Context[40:])
		p.tx, p.rx = proof.tx, proof.rx
	}
	m.Context = m.Context[heartbeatHeader:]
	return true
}

// Authorization and connection replacement invalidate earlier evidence too.
// Checking only incoming replies would leave an already retired identity in
// the quorum and genesis-apply observations until its old timeout expired.
func (r *Runtime) peerLive(id uint64, now time.Time) bool {
	p := r.liveness[id]
	if p == nil || !r.knownPeer(id) || now.Sub(p.confirmed) >= r.livenessWindow() {
		return false
	}
	tx, rx := r.generations(id)
	return p.tx == tx && p.rx == rx
}

func (r *Runtime) quorumLive(now time.Time) bool {
	majority := func(ids []uint64) bool {
		if len(ids) == 0 {
			return true
		}
		active := 0
		for _, id := range ids {
			if !r.knownPeer(id) {
				continue
			}
			if id == r.cfg.Local.ID {
				active++
				continue
			}
			if r.peerLive(id, now) {
				active++
			}
		}
		return active > len(ids)/2
	}
	return len(r.core.conf.GetVoters()) > 0 && majority(r.core.conf.GetVoters()) && majority(r.core.conf.GetVotersOutgoing())
}

// authorityRole makes liveness a separate condition from log replication and
// durable application. Once expired, an epoch stays revoked until Raft leaves
// leadership; subsequent delayed traffic cannot resurrect its C++ authority.
func (r *Runtime) authorityRole(role Role) Role {
	now := time.Now()
	if !r.singleQuarantineUntil.IsZero() && !now.Before(r.singleQuarantineUntil) {
		r.singleQuarantineUntil = time.Time{}
		r.epochRevoked = false
		r.leaderSince = now
	}
	if role.IsLeader && (!r.protocolLeader || r.protocolTerm != role.Term) {
		r.leaderSince = now
		r.epochRevoked = false
		r.liveness = map[uint64]*peerLiveness{}
	}
	r.protocolLeader = role.IsLeader
	r.protocolTerm = role.Term
	if !role.IsLeader {
		r.epochRevoked = false
		return role
	}
	if !r.quorumLive(now) {
		role.CaughtUp = false
		if now.Sub(r.leaderSince) >= r.livenessWindow() {
			r.epochRevoked = true
		}
	}
	if r.epochRevoked || (r.deferredTerm != nil && r.deferredTerm.GetTerm() > role.Term) || !r.knownPeer(r.cfg.Local.ID) {
		role.IsLeader = false
		role.CaughtUp = false
	}
	return role
}
