// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"time"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type snapshotCapture struct {
	metadata *pb.SnapshotMetadata
	members  []Member
}
type snapshotWork struct {
	image    *pb.Snapshot
	incoming *pb.Message
}
type snapshotPrepared struct {
	snapshotWork
	err error
}

// Snapshot returns once publication is durable. Capturing is serialized with
// apply at an exact index; encoding and syncing never run on the event loop.
func (r *Runtime) Snapshot() (<-chan Result, error) {
	if !r.admission.enter() {
		return nil, ErrStopped
	}
	defer r.admission.leave()
	result := make(chan Result, 1)
	select {
	case <-r.stop:
		return nil, ErrStopped
	default:
	}
	select {
	case r.snapshotRequests <- result:
		return result, nil
	case <-r.stop:
		return nil, ErrStopped
	default:
		return nil, ErrBusy
	}
}

func (r *Runtime) maybeCapture() {
	if r.snapshotInProgress || r.applyInProgress || r.core.restoreIndex > r.core.snapshot {
		// Covered application completions cannot update the restored Raft
		// configuration. Wait for the incoming image to install the matching
		// application/descriptor/configuration cut before capturing again.
		return
	}
	if r.core.applied <= r.core.snapshot {
		// An incoming image can satisfy a request queued behind an old apply
		// job. Do not strand that waiter when there is no newer cut to capture.
		if r.snapshotWaiter != nil {
			r.finishSnapshot(r.core.snapshot, nil)
		}
		return
	}
	first, _ := r.core.memory.FirstIndex()
	// A waiting learner cannot restore an older snapshot that omits its ID.
	// Once the prefix is compacted, refresh the image after a configuration
	// change even if no application traffic reaches the ordinary distance.
	// Otherwise joining an idle cluster would wait forever for missing logs.
	configurationCut := first > 1 && r.core.configIndex > r.core.snapshot
	distanceReached := r.cfg.SnapshotDistance > 0 && r.core.applied-r.core.snapshot >= r.cfg.SnapshotDistance
	automatic := (configurationCut || distanceReached) && time.Since(r.snapshotAttempt) >= time.Second
	if r.snapshotWaiter == nil && !automatic {
		return
	}
	term, err := r.core.memory.Term(r.core.applied)
	if err != nil {
		r.finishSnapshot(0, err)
		return
	}
	r.snapshotInProgress = true
	r.snapshotAttempt = time.Now()
	job := &applyJob{capture: &snapshotCapture{metadata: &pb.SnapshotMetadata{Index: new(r.core.applied), Term: new(term), ConfState: proto.Clone(r.core.conf).(*pb.ConfState)}, members: r.core.memberList()}}
	// No apply is in flight, and the capture goes before any queued future
	// apply. Thus the C++ image cannot be labelled with an older cut.
	r.applyQueue = append([]*applyJob{job}, r.applyQueue...)
}

func (r *Runtime) finishSnapshot(index uint64, err error) {
	r.snapshotInProgress = false
	if err != nil {
		r.snapshotFailures++
	} else {
		r.snapshotFailures = 0
	}
	if r.snapshotWaiter != nil {
		r.snapshotWaiter <- Result{Index: index, Err: err}
		r.snapshotWaiter = nil
	}
}

func (r *Runtime) publishLocalSnapshot(done *diskCompletion) error {
	image := done.task.Snapshot
	index := image.Metadata.GetIndex()
	// A newer received snapshot may already have superseded this local cut.
	// Its durable marker is harmless, but must not regress the live floor.
	if index > r.core.snapshot {
		_, err := r.core.memory.CreateSnapshot(index, image.Metadata.ConfState, image.Data)
		if err != nil && err != raft.ErrSnapOutOfDate {
			return err
		}
		first, _ := r.core.memory.FirstIndex()
		if index > r.cfg.ReservedLogItems {
			cut := index - r.cfg.ReservedLogItems
			if cut >= first {
				if err := r.core.compact(cut); err != nil {
					return err
				}
			}
		}
		r.core.snapshot = index
	}
	r.pendingBytes -= uint64(proto.Size(done.task))
	r.pendingTasks--
	r.finishSnapshot(index, nil)
	close(done.ack)
	return nil
}

func (r *Runtime) bulkLoop() {
	defer r.workers.Done()
	for {
		select {
		case <-r.stop:
			return
		case work := <-r.bulkJobs:
			done := snapshotPrepared{snapshotWork: work, err: r.disk.prepareSnapshot(work.image)}
			select {
			case r.bulkDone <- done:
			case <-r.stop:
				return
			}
		}
	}
}
