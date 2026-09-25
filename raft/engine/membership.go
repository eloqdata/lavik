// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"encoding/json"
	"errors"
	"time"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
)

type memberRequest struct {
	member      Member
	remove      bool
	learner     bool
	result      chan Result
	term        uint64
	lastAttempt time.Time
}

// ChangeMember serializes a single member intent. A requested voter is added
// as a learner first and promoted only after actual application catches up.
func (r *Runtime) ChangeMember(member Member, remove, learner bool) (<-chan Result, error) {
	return r.ChangeMemberInTerm(member, remove, learner, r.Status().Term)
}

// ChangeMemberInTerm keeps the caller's leadership identity through queueing
// and learner promotion; a later leader cannot adopt an obsolete request.
func (r *Runtime) ChangeMemberInTerm(member Member, remove, learner bool, term uint64) (<-chan Result, error) {
	if !r.admission.enter() {
		return nil, ErrStopped
	}
	defer r.admission.leave()
	if member.ID == 0 || member.ID > 0x7fffffff {
		return nil, errors.New("invalid member ID")
	}
	if !remove {
		if err := member.validate(); err != nil {
			return nil, err
		}
	}
	request := &memberRequest{term: term, member: member, remove: remove, learner: learner, result: make(chan Result, 1)}
	select {
	case <-r.stop:
		return nil, ErrStopped
	default:
	}
	select {
	case r.memberRequests <- request:
		return request.result, nil
	case <-r.stop:
		return nil, ErrStopped
	default:
		return nil, ErrBusy
	}
}

// Resign withdraws the currently observed Raft term. A delayed request cannot
// resign a leader elected in a later term.
func (r *Runtime) Resign() { r.RequestResign(r.Status().Term) }

// RequestResign carries the actual Raft term, not a second authority epoch.
func (r *Runtime) RequestResign(term uint64) {
	for current := r.resignTerm.Load(); current < term; current = r.resignTerm.Load() {
		if r.resignTerm.CompareAndSwap(current, term) {
			break
		}
	}
	select {
	case r.resignRequests <- struct{}{}:
	case <-r.stop:
	default:
	}
}

func (r *Runtime) resign() {
	if r.core.status().Term <= r.resignTerm.Load() {
		r.stepDown()
	}
	r.publish()
}

func (r *Runtime) advanceMembership() error {
	op := r.memberChange
	if op == nil {
		return nil
	}
	status := r.core.status()
	if !status.IsLeader || r.deferredTerm != nil || status.Term != op.term {
		op.result <- Result{Err: ErrStopped}
		r.memberChange = nil
		return nil
	}
	current, exists := r.core.members[op.member.ID]
	if !op.remove && !exists && len(r.core.members) >= 1024 {
		op.result <- Result{Err: ErrBusy}
		r.memberChange = nil
		return nil
	}
	if (op.remove && !exists) || (!op.remove && exists && current == op.member && (op.learner || !status.Learners[op.member.ID])) {
		// A caller observing successful membership completion must already
		// be able to read that exact applied configuration from Status.
		r.publish()
		op.result <- Result{Index: r.core.configIndex}
		r.memberChange = nil
		return nil
	}
	if time.Since(op.lastAttempt) < time.Second || r.pendingTasks >= r.cfg.QueueCapacity || r.pendingBytes >= r.cfg.MaxPendingBytes {
		return nil
	}
	if exists && !op.remove && current != op.member {
		op.result <- Result{Err: ErrBusy}
		r.memberChange = nil
		return nil
	}
	kind := pb.ConfChangeRemoveNode
	if !op.remove {
		kind = pb.ConfChangeAddLearnerNode
		if exists && !op.learner {
			caught := false
			r.core.raw.WithProgress(func(id uint64, _ raft.ProgressType, p tracker.Progress) {
				if id == op.member.ID && p.Match >= status.Commit {
					caught = true
				}
			})
			peer := r.liveness[op.member.ID]
			if !caught || !r.peerLive(op.member.ID, time.Now()) || peer.applied < status.Commit {
				return nil
			}
			kind = pb.ConfChangeAddNode
		}
	}
	var data []byte
	if !op.remove {
		var err error
		data, err = json.Marshal(op.member)
		if err != nil {
			return err
		}
	}
	op.lastAttempt = time.Now()
	return r.core.raw.ProposeConfChange(&pb.ConfChange{Type: kind.Enum(), NodeId: new(op.member.ID), Context: data})
}

// stepDown retires this Raft term, including for a sole voter. RawNode has no
// public resignation primitive. Use its higher-term, reject-only local event
// (also used for deferred term observations) to become a follower without
// granting a vote or manufacturing a quorum. Ready persists the new term before
// dependent output; normal ticks and elections alone may establish a leader.
func (r *Runtime) stepDown() {
	s := r.core.status()
	if !s.IsLeader {
		return
	}
	term := s.Term + 1
	if term == 0 {
		panic("Raft term exhausted")
	}
	r.liveness = map[uint64]*peerLiveness{}
	if r.deferredTerm == nil || r.deferredTerm.GetTerm() < term {
		r.deferredTerm = &pb.Message{Type: pb.MsgVoteResp.Enum(), From: new(raft.LocalAppendThread), To: new(r.cfg.Local.ID), Term: new(term), Reject: new(true)}
	}
	if err := r.advanceDeferredTerm(); err != nil {
		panic(err)
	}
}
