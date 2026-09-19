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
	request := &memberRequest{member: member, remove: remove, learner: learner, result: make(chan Result, 1)}
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

// Resign immediately withdraws the application's leadership generation. Raft
// attempts transfer and CheckQuorum retires the old protocol epoch. A sole
// voter reopens only after a full election interval, retaining the C++ 2D guard.
func (r *Runtime) Resign() {
	r.RequestResign(r.resignIssued.Add(1))
}

// RequestResign carries a monotonically increasing foreign-owner generation.
// Callers withdraw local authority before enqueueing it and ignore older role
// callbacks until the protocol owner acknowledges that generation.
func (r *Runtime) RequestResign(index uint64) {
	for current := r.resignIssued.Load(); current < index; current = r.resignIssued.Load() {
		if r.resignIssued.CompareAndSwap(current, index) {
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
	r.resignApplied = r.resignIssued.Load()
	r.epochRevoked = true
	if len(r.core.conf.GetVoters()) == 1 && r.core.conf.GetVoters()[0] == r.cfg.Local.ID {
		r.singleQuarantineUntil = time.Now().Add(r.livenessWindow())
	} else {
		for _, id := range r.core.conf.GetVoters() {
			if id != r.cfg.Local.ID {
				r.core.raw.TransferLeader(id)
				break
			}
		}
	}
	r.publish()
}

func (r *Runtime) advanceMembership() error {
	op := r.memberChange
	if op == nil {
		return nil
	}
	status := r.core.status()
	if !status.IsLeader || r.epochRevoked || r.deferredTerm != nil || status.Term != op.term {
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
