// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	pb "go.etcd.io/raft/v3/raftpb"
)

// joinSeed authorizes a waiting node's bounded catch-up window. Index is a
// committed configuration cut supplied by the inviting member; reaching it
// permanently ends grace, including after later removal of the local member.
type joinSeed struct {
	Index   uint64   `json:"index"`
	Members []Member `json:"members"`
}
type joinRequest struct {
	seed   joinSeed
	result chan error
}
type joiningTransport interface {
	SetJoin(func(joinSeed) error)
	SetConfigIndex(uint64)
}

func (r *Runtime) join(seed joinSeed) error {
	if !r.admission.enter() {
		return ErrStopped
	}
	request := &joinRequest{seed: seed, result: make(chan error, 1)}
	select {
	case r.joinRequests <- request:
	case <-r.stop:
		r.admission.leave()
		return ErrStopped
	default:
		r.admission.leave()
		return ErrBusy
	}
	r.admission.leave()
	select {
	case err := <-request.result:
		return err
	case <-r.stop:
		return ErrStopped
	}
}

func (r *Runtime) startJoin(request *joinRequest) error {
	if len(r.disk.genesis.Initial) != 0 || r.core.applied != 0 || r.disk.join != nil || r.joining != nil {
		request.result <- ErrBusy
		return nil
	}
	if err := request.seed.validate(r.cfg.Local); err != nil {
		request.result <- err
		return nil
	}
	data, err := json.Marshal(request.seed)
	if err != nil {
		return err
	}
	r.joining = request
	task := &pb.Message{Type: pb.MsgProp.Enum(), Context: data}
	r.appendQueue = append(r.appendQueue, task)
	r.pendingTasks++
	return nil
}

func (seed joinSeed) validate(local Member) error {
	if seed.Index == 0 || len(seed.Members) == 0 || len(seed.Members) > 1024 {
		return errors.New("invalid join invitation")
	}
	found := false
	seen := map[uint64]bool{}
	for _, m := range seed.Members {
		if err := m.validate(); err != nil {
			return err
		}
		if seen[m.ID] {
			return errors.New("duplicate invited member")
		}
		seen[m.ID] = true
		if m.ID == local.ID {
			found = m.Principal == local.Principal
		}
	}
	if !found {
		return errors.New("invitation omits local member")
	}
	return nil
}

func (s *diskStore) saveJoin(data []byte) error {
	path := filepath.Join(s.dir, "JOIN")
	if err := writeExclusive(path, data); err != nil {
		return err
	}
	return syncDirectory(s.dir)
}

func (r *Runtime) finishJoin(done *diskCompletion) error {
	if r.joining == nil {
		return errors.New("unexpected invitation completion")
	}
	r.disk.join = &r.joining.seed
	r.refreshTransport()
	r.joining.result <- nil
	r.joining = nil
	r.pendingTasks--
	close(done.ack)
	return nil
}

func (r *Runtime) refreshTransport() {
	members := r.core.memberList()
	if seed := r.disk.join; seed != nil && r.core.applied < seed.Index {
		// The immutable seed is a transport baseline, not an installed voter
		// configuration. The RawNode stays election-disabled until its logs
		// actually apply the local member's configuration.
		members = append([]Member(nil), seed.Members...)
	}
	if transport, ok := r.transport.(joiningTransport); ok {
		transport.SetConfigIndex(r.core.configIndex)
	}
	allowed := make([]Member, 0, len(members))
	for _, member := range members {
		if r.authorizedMember(member) {
			allowed = append(allowed, member)
		}
	}
	r.transport.Update(allowed)
}

func (r *Runtime) knownPeer(id uint64) bool {
	if member, ok := r.core.members[id]; ok {
		return r.authorizedMember(member)
	}
	if seed := r.disk.join; seed != nil && r.core.applied < seed.Index {
		for _, m := range seed.Members {
			if m.ID == id {
				return r.authorizedMember(m)
			}
		}
	}
	return false
}

func (s *diskStore) readJoin() error {
	data, err := readBounded(filepath.Join(s.dir, "JOIN"), 1<<20)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(data) > 1<<20 || len(s.genesis.Initial) != 0 {
		return errors.New("invalid join recovery evidence")
	}
	var seed joinSeed
	if err := json.Unmarshal(data, &seed); err != nil {
		return err
	}
	if err := seed.validate(s.genesis.Local); err != nil {
		return err
	}
	s.join = &seed
	return nil
}
