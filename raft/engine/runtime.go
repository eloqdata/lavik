// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type proposal struct {
	data   []byte
	key    [16]byte
	result chan Result
}
type diskCompletion struct {
	task *pb.Message
	err  error
	ack  chan struct{}
}
type applyJob struct {
	task    *pb.Message
	install *diskCompletion
	capture *snapshotCapture
}
type applyCompletion struct {
	job      *applyJob
	results  []Result
	err      error
	ack      chan struct{}
	snapshot *pb.Snapshot
	bindings []Binding
}

// Runtime owns an event loop and independent disk and apply executors. Close
// joins all owners; callers must keep Application and callbacks alive until it
// returns, including when a disk syscall prevents prompt shutdown.
type Runtime struct {
	cfg               Config
	core              *core
	disk              *diskStore
	app               Application
	transport         Transport
	onRole            func(Role)
	onFailure         func(error)
	status            atomic.Pointer[Status]
	inputBytes        [laneCount]byteBudget
	proposalBytes     byteBudget
	receivingSnapshot atomic.Bool
	gcFailures        atomic.Uint64
	ingress           chan *pb.Message
	control           chan *pb.Message
	proposals         chan proposal
	diskJobs          chan []*pb.Message
	diskDone          chan *diskCompletion
	applyJobs         chan *applyJob
	applyDone         chan *applyCompletion
	snapshotRequests  chan chan Result
	bulkJobs          chan snapshotWork
	bulkDone          chan snapshotPrepared
	memberRequests    chan *memberRequest
	resignRequests    chan struct{}
	resignIssued      atomic.Uint64
	joinRequests      chan *joinRequest
	stop              chan struct{}
	done              chan struct{}
	closeOnce         sync.Once
	admission         admissionGate
	workers           sync.WaitGroup
	// The following fields belong only to the protocol loop.
	appendQueue           []*pb.Message
	applyQueue            []*applyJob
	waiters               map[[16]byte]chan Result
	pendingBytes          uint64
	pendingTasks          int
	lastRole              Role
	roleSet               bool
	heartbeatNonce        [16]byte
	heartbeatSeq          uint64
	liveness              map[uint64]*peerLiveness
	protocolLeader        bool
	protocolTerm          uint64
	leaderSince           time.Time
	epochRevoked          bool
	applyInProgress       bool
	snapshotInProgress    bool
	snapshotWaiter        chan Result
	snapshotFailures      uint64
	snapshotAttempt       time.Time
	memberChange          *memberRequest
	singleQuarantineUntil time.Time
	joining               *joinRequest
	replayTarget          uint64
	replayDone            chan struct{}
	replayed              bool
	checkIdentities       bool
	bindings              map[uint64]Binding
	snapshotCandidate     bool
	voteRejections        uint64
	voteGrants            uint64
	bulkQueue             []snapshotWork
	preparedIncoming      *pb.Message
	resignApplied         uint64
	deferredTerm          *pb.Message
}

// Open recovers local state before starting any timers. Transport must not
// deliver inbound messages until Open returns. Both callbacks must be bounded
// and nonblocking; onFailure must arrange process fail-stop in production.
func Open(cfg Config, app Application, transport Transport, onRole func(Role), onFailure func(error)) (*Runtime, error) {
	if app == nil || transport == nil {
		return nil, errors.New("application and transport are required")
	}
	if cfg.Heartbeat == 0 {
		cfg.Heartbeat = 100 * time.Millisecond
	}
	if cfg.ElectionTicks == 0 {
		cfg.ElectionTicks = 3
	}
	if cfg.ElectionTicks < 3 || cfg.ElectionTicks > 60 || cfg.Heartbeat <= 0 {
		return nil, errors.New("invalid Raft timing")
	}
	if cfg.QueueCapacity == 0 {
		cfg.QueueCapacity = 256
	}
	if cfg.MaxPendingBytes == 0 {
		cfg.MaxPendingBytes = 64 << 20
	}
	if cfg.QueueCapacity < 8 || cfg.QueueCapacity > 4096 || cfg.MaxPendingBytes < 2<<20 || cfg.MaxPendingBytes > 1<<30 {
		return nil, errors.New("invalid Raft queue budget")
	}
	disk, rec, err := openDisk(cfg)
	if err != nil {
		return nil, err
	}
	c, err := newCore(cfg, disk.genesis, rec)
	if err != nil {
		_ = disk.wal.Close()
		return nil, err
	}
	if !raft.IsEmptySnap(rec.snapshot) {
		_, data, err := decodeSnapshot(rec.snapshot.Data)
		if err == nil {
			err = app.Install(rec.snapshot.Metadata.GetIndex(), data)
		}
		if err != nil {
			_ = disk.wal.Close()
			return nil, err
		}
	}
	r := &Runtime{cfg: cfg, core: c, disk: disk, app: app, transport: transport, onRole: onRole, onFailure: onFailure,
		ingress: make(chan *pb.Message, cfg.QueueCapacity), control: make(chan *pb.Message, cfg.QueueCapacity),
		proposals: make(chan proposal, cfg.QueueCapacity), diskJobs: make(chan []*pb.Message), diskDone: make(chan *diskCompletion),
		applyJobs: make(chan *applyJob), applyDone: make(chan *applyCompletion), stop: make(chan struct{}), done: make(chan struct{}),
		waiters: map[[16]byte]chan Result{}}
	r.snapshotRequests = make(chan chan Result, 1)
	r.admission.drained = make(chan struct{})
	r.bulkJobs = make(chan snapshotWork)
	r.bulkDone = make(chan snapshotPrepared)
	r.memberRequests = make(chan *memberRequest, 1)
	r.resignRequests = make(chan struct{}, 1)
	r.joinRequests = make(chan *joinRequest, 1)
	r.replayTarget = rec.hard.GetCommit()
	r.replayDone = make(chan struct{})
	if source, ok := app.(IdentitySource); ok {
		bindings, err := source.Identities()
		if err != nil {
			_ = disk.wal.Close()
			return nil, err
		}
		r.checkIdentities = true
		r.setBindings(bindings)
	}
	if t, ok := transport.(joiningTransport); ok {
		t.SetJoin(r.join)
	}
	r.liveness = map[uint64]*peerLiveness{}
	if _, err := rand.Read(r.heartbeatNonce[:]); err != nil {
		_ = disk.wal.Close()
		return nil, err
	}
	r.publish()
	r.refreshTransport()
	r.workers.Add(4)
	go r.gcLoop()
	go r.bulkLoop()
	go r.diskLoop()
	go r.applyLoop()
	go r.run()
	select {
	case <-r.replayDone:
	case <-r.done:
		return nil, errors.New("Raft startup replay failed: " + r.Status().Failure)
	}
	return r, nil
}

// Status returns a detached observation; callers cannot mutate the published
// member view shared with the protocol owner.
func (r *Runtime) Status() Status {
	current := r.status.Load()
	s := *current
	s.Members = append([]Member(nil), s.Members...)
	s.PeerApplied = make(map[uint64]uint64, len(current.PeerApplied))
	s.PeerAgeMicros = make(map[uint64]uint64, len(current.PeerAgeMicros))
	for id, index := range current.PeerApplied {
		s.PeerApplied[id] = index
		s.PeerAgeMicros[id] = current.PeerAgeMicros[id]
	}
	s.Learners = make(map[uint64]bool, len(current.Learners))
	for id, value := range current.Learners {
		s.Learners[id] = value
	}
	s.Genesis = append([]uint64(nil), current.Genesis...)
	return s
}

// Step copies the network-owned protobuf before enqueueing it. Local-only
// messages and forged local storage completions can never enter this API.
func (r *Runtime) Step(m *pb.Message) error {
	if !r.admission.enter() {
		return ErrStopped
	}
	defer r.admission.leave()
	if m == nil || raft.IsLocalMsg(m.GetType()) || raft.IsLocalMsgTarget(m.GetFrom()) || m.GetTo() != r.cfg.Local.ID || m.GetFrom() == 0 || m.GetFrom() == r.cfg.Local.ID {
		return errors.New("invalid remote Raft message")
	}
	size := uint64(proto.Size(m))
	lane := messageLane(m)
	if size > 2<<20 && lane != laneSnapshot {
		return ErrBusy
	}
	if size > maxSnapshotMessage || (lane == laneControl && size > 64<<10) || !r.inputBytes[lane].take(size, laneBudget(lane)) {
		return ErrBusy
	}
	accepted := false
	defer func() {
		if !accepted {
			r.inputBytes[lane].release(size)
		}
	}()
	if lane == laneSnapshot {
		// Keep the reservation until installation, not just dequeue. A second
		// received image cannot displace an in-flight durable/apply barrier.
		if !r.receivingSnapshot.CompareAndSwap(false, true) {
			return ErrBusy
		}
		defer func() {
			if !accepted {
				r.receivingSnapshot.Store(false)
			}
		}()
	}
	ch := r.ingress
	if controlMessage(m.GetType()) {
		ch = r.control
	}
	copy := proto.Clone(m).(*pb.Message)
	select {
	case <-r.stop:
		return ErrStopped
	default:
	}
	select {
	case ch <- copy:
		accepted = true
		return nil
	case <-r.stop:
		return ErrStopped
	default:
		return ErrBusy
	}
}

func controlMessage(kind pb.MessageType) bool {
	switch kind {
	case pb.MsgHeartbeat, pb.MsgHeartbeatResp, pb.MsgVote, pb.MsgVoteResp, pb.MsgPreVote, pb.MsgPreVoteResp, pb.MsgTimeoutNow:
		return true
	}
	return false
}

// Propose accepts a bounded copy. Its result is buffered and completed exactly
// once; abandoning the receiver does not block Raft or release its reservation.
func (r *Runtime) Propose(data []byte) (<-chan Result, error) {
	if !r.admission.enter() {
		return nil, ErrStopped
	}
	defer r.admission.leave()
	if len(data) > 1<<20 || !r.proposalBytes.take(uint64(len(data)), r.cfg.MaxPendingBytes) {
		return nil, ErrBusy
	}
	accepted := false
	defer func() {
		if !accepted {
			r.proposalBytes.release(uint64(len(data)))
		}
	}()
	p := proposal{data: append([]byte(nil), data...), result: make(chan Result, 1)}
	if _, err := rand.Read(p.key[:]); err != nil {
		return nil, err
	}
	select {
	case <-r.stop:
		return nil, ErrStopped
	default:
	}
	select {
	case r.proposals <- p:
		accepted = true
		return p.result, nil
	case <-r.stop:
		return nil, ErrStopped
	default:
		return nil, ErrBusy
	}
}

// Close revokes role authority before waiting on possibly blocked storage.
func (r *Runtime) Close() {
	r.stopAdmission()
	<-r.done
}

func (r *Runtime) stopAdmission() {
	r.closeOnce.Do(func() { r.admission.seal(); close(r.stop) })
}

func (r *Runtime) publish() {
	s := r.core.status()
	s.Role = r.authorityRole(s.Role)
	s.ResignIndex = r.resignApplied
	s.PeerApplied = map[uint64]uint64{}
	s.PeerAgeMicros = map[uint64]uint64{}
	for id, peer := range r.liveness {
		if r.peerLive(id, time.Now()) {
			s.PeerApplied[id] = peer.applied
			s.PeerAgeMicros[id] = uint64(time.Since(peer.confirmed).Microseconds())
		}
	}
	s.PendingBytes = r.pendingBytes
	s.GCFailures = r.gcFailures.Load()
	s.VoteRejections = r.voteRejections
	s.VoteGrants = r.voteGrants
	if n, ok := r.transport.(interface{ Failures() uint64 }); ok {
		s.RPCFailures = n.Failures()
	}
	s.SnapshotFailures = r.snapshotFailures
	for _, m := range r.disk.genesis.Initial {
		s.Genesis = append(s.Genesis, m.ID)
	}
	r.status.Store(&s)
	if r.roleSet && r.lastRole.IsLeader && (!s.IsLeader || s.Term != r.lastRole.Term) {
		// Losing leadership makes accepted proposals uncertain. Release local
		// callbacks; later replay still applies their committed effects exactly
		// once and never fabricates a successful client result.
		for key, waiter := range r.waiters {
			waiter <- Result{Err: ErrUncertain}
			delete(r.waiters, key)
		}
	}
	if !r.roleSet || s.Role != r.lastRole {
		r.lastRole = s.Role
		r.roleSet = true
		if r.onRole != nil {
			r.onRole(s.Role)
		}
	}
}

func (r *Runtime) run() {
	ticker := time.NewTicker(r.cfg.Heartbeat)
	var failure error
	defer func() {
		ticker.Stop()
		r.stopAdmission()
		s := r.Status()
		s.IsLeader = false
		s.CaughtUp = false
		if failure != nil {
			s.Failure = failure.Error()
		}
		r.status.Store(&s)
		if r.onRole != nil {
			r.onRole(s.Role)
		}
		if failure != nil && r.onFailure != nil {
			r.onFailure(failure)
		}
		<-r.admission.drained
		for _, w := range r.waiters {
			w <- Result{Err: ErrStopped}
		}
		if r.snapshotWaiter != nil {
			r.snapshotWaiter <- Result{Err: ErrStopped}
		}
		if r.memberChange != nil {
			r.memberChange.result <- Result{Err: ErrStopped}
		}
		select {
		case op := <-r.memberRequests:
			op.result <- Result{Err: ErrStopped}
		default:
		}
		select {
		case w := <-r.snapshotRequests:
			w <- Result{Err: ErrStopped}
		default:
		}
		for {
			select {
			case p := <-r.proposals:
				r.proposalBytes.release(uint64(len(p.data)))
				p.result <- Result{Err: ErrStopped}
			case m := <-r.control:
				r.inputBytes[messageLane(m)].release(uint64(proto.Size(m)))
			case m := <-r.ingress:
				r.inputBytes[messageLane(m)].release(uint64(proto.Size(m)))
			case request := <-r.joinRequests:
				request.result <- ErrStopped
			default:
				goto drained
			}
		}
	drained:
		if r.joining != nil {
			r.joining.result <- ErrStopped
			r.joining = nil
		}
		r.transport.Close()
		r.workers.Wait()
		_ = r.disk.wal.Close()
		r.receivingSnapshot.Store(false)
		r.appendQueue, r.applyQueue, r.bulkQueue = nil, nil, nil
		r.preparedIncoming = nil
		close(r.done)
	}()
	for {
		if failure = r.advanceDeferredTerm(); failure != nil {
			return
		}
		if !r.replayed && r.core.applied >= r.replayTarget {
			r.replayed = true
			close(r.replayDone)
		}
		if failure = r.ready(); failure != nil {
			return
		}
		r.maybeCapture()
		if failure = r.advanceMembership(); failure != nil {
			return
		}
		r.publish()
		var diskOut chan []*pb.Message
		var diskTask []*pb.Message
		if len(r.appendQueue) > 0 {
			if raft.IsEmptySnap(r.appendQueue[0].Snapshot) || r.appendQueue[0].GetType() == pb.MsgSnap || r.preparedIncoming == r.appendQueue[0] {
				diskOut = r.diskJobs
			}
			count := 1
			if r.appendQueue[0].GetType() == pb.MsgStorageAppend && raft.IsEmptySnap(r.appendQueue[0].Snapshot) {
				for count < len(r.appendQueue) && count < 32 && r.appendQueue[count].GetType() == pb.MsgStorageAppend && raft.IsEmptySnap(r.appendQueue[count].Snapshot) {
					count++
				}
			}
			diskTask = append([]*pb.Message(nil), r.appendQueue[:count]...)
		}
		var bulkOut chan snapshotWork
		var bulkWork snapshotWork
		if len(r.bulkQueue) > 0 {
			bulkOut = r.bulkJobs
			bulkWork = r.bulkQueue[0]
		}
		var applyOut chan *applyJob
		var application *applyJob
		if len(r.applyQueue) > 0 {
			application = r.applyQueue[0]
			if !r.applyInProgress && (application.capture != nil || application.task.GetType() != pb.MsgStorageAppend || application.install != nil) {
				applyOut = r.applyJobs
			}
		}
		// Give an already queued control message one turn before bulk input.
		// The main select still services ticks/completions under a control flood.
		select {
		case m := <-r.control:
			if failure = r.consume(m); failure != nil {
				return
			}
			r.publish()
		default:
		}
		select {
		case <-r.stop:
			return
		case <-ticker.C:
			if r.replayed {
				if transport, ok := r.transport.(interface{ SnapshotResults(func(uint64, bool)) }); ok {
					transport.SnapshotResults(func(id uint64, success bool) {
						result := raft.SnapshotFailure
						if success {
							result = raft.SnapshotFinish
						}
						r.core.raw.ReportSnapshot(id, result)
					})
				}
				// A blocked vote writer must not accumulate unlimited new local
				// election terms. Stable leaders still tick/send/check quorum;
				// followers continue servicing same-term heartbeat traffic.
				if r.deferredTerm == nil && (!r.storageFull() || r.core.status().IsLeader) {
					r.core.raw.Tick()
				}
			}
		case <-r.resignRequests:
			r.resign()
		case request := <-r.joinRequests:
			failure = r.startJoin(request)
		case op := <-r.memberRequests:
			if r.memberChange != nil {
				op.result <- Result{Err: ErrBusy}
			} else if !r.status.Load().CaughtUp || (op.remove && op.member.ID == r.cfg.Local.ID) {
				op.result <- Result{Err: ErrNotLeader}
			} else {
				op.term = r.core.status().Term
				r.memberChange = op
			}
		case waiter := <-r.snapshotRequests:
			if r.snapshotWaiter != nil || r.snapshotInProgress {
				waiter <- Result{Err: ErrBusy}
			} else if r.core.applied == r.core.snapshot {
				waiter <- Result{Index: r.core.snapshot}
			} else {
				r.snapshotWaiter = waiter
			}
		case prepared := <-r.bulkDone:
			if prepared.incoming != nil {
				if prepared.err != nil {
					failure = prepared.err
				} else {
					r.preparedIncoming = prepared.incoming
				}
				break
			}
			if prepared.err != nil {
				r.finishSnapshot(0, prepared.err)
				break
			}
			task := &pb.Message{Type: pb.MsgSnap.Enum(), Snapshot: prepared.image}
			r.appendQueue = append(r.appendQueue, task)
			r.pendingBytes += uint64(proto.Size(task))
			r.pendingTasks++
		case m := <-r.control:
			failure = r.consume(m)
		case m := <-r.ingress:
			failure = r.consume(m)
		case p := <-r.proposals:
			r.proposalBytes.release(uint64(len(p.data)))
			if !r.status.Load().IsLeader || !r.status.Load().CaughtUp {
				p.result <- Result{Err: ErrNotLeader}
				break
			}
			if r.pendingBytes+uint64(len(p.data)) > r.cfg.MaxPendingBytes || r.pendingTasks >= r.cfg.QueueCapacity || len(r.waiters) >= r.cfg.QueueCapacity || r.logFull() {
				p.result <- Result{Err: ErrBusy}
				break
			}
			data := make([]byte, 24+len(p.data))
			copy(data, "LPR1")
			copy(data[4:20], p.key[:])
			binary.LittleEndian.PutUint32(data[20:], uint32(len(p.data)))
			copy(data[24:], p.data)
			if err := r.core.raw.Propose(data); err != nil {
				p.result <- Result{Err: err}
			} else {
				r.waiters[p.key] = p.result
			}
		case bulkOut <- bulkWork:
			r.bulkQueue[0] = snapshotWork{}
			r.bulkQueue = r.bulkQueue[1:]
		case diskOut <- diskTask:
			clear(r.appendQueue[:len(diskTask)])
			r.appendQueue = r.appendQueue[len(diskTask):]
		case applyOut <- application:
			r.applyInProgress = true
			r.applyQueue[0] = nil
			r.applyQueue = r.applyQueue[1:]
		case done := <-r.diskDone:
			if done.err != nil {
				failure = done.err
				break
			}
			if done.task.GetType() == pb.MsgSnap {
				failure = r.publishLocalSnapshot(done)
				break
			}
			if done.task.GetType() == pb.MsgProp {
				failure = r.finishJoin(done)
				break
			}
			if !raft.IsEmptySnap(done.task.Snapshot) {
				found := false
				for _, job := range r.applyQueue {
					if job.task == done.task {
						job.install = done
						found = true
						break
					}
				}
				if !found {
					failure = errors.New("lost snapshot apply barrier")
				}
			} else {
				failure = r.finishDisk(done)
			}
		case done := <-r.applyDone:
			r.applyInProgress = false
			if done.job.capture != nil {
				if done.err != nil {
					r.finishSnapshot(0, done.err)
				} else {
					r.bulkQueue = append(r.bulkQueue, snapshotWork{image: done.snapshot})
				}
				close(done.ack)
				break
			}
			if done.err != nil {
				failure = done.err
				break
			}
			if done.job.install != nil {
				if r.checkIdentities {
					r.setBindings(done.bindings)
				}
				failure = r.finishDisk(done.job.install)
			} else {
				if r.checkIdentities {
					r.setBindings(done.bindings)
				}
				for i, e := range done.job.task.Entries {
					if failure = r.core.appliedEntry(e); failure != nil {
						break
					}
					if len(e.Data) > 0 && e.GetType() == pb.EntryNormal {
						key, _, err := decodeProposal(e.Data)
						if err != nil {
							failure = err
							break
						}
						if w, ok := r.waiters[key]; ok {
							w <- done.results[i]
							delete(r.waiters, key)
						}
					}
				}
				if failure == nil {
					failure = r.responses(done.job.task)
				}
			}
			if failure == nil {
				r.refreshTransport()
				close(done.ack)
			}
		}
		if failure != nil {
			return
		}
	}
}

func (r *Runtime) logFull() bool {
	return r.core.retainedBytes+r.pendingBytes >= 4*r.cfg.MaxPendingBytes || len(r.core.logBytes) >= 262144
}

func (r *Runtime) storageFull() bool {
	return r.pendingTasks >= r.cfg.QueueCapacity+32 || r.pendingBytes >= r.cfg.MaxPendingBytes+2<<20
}

func (r *Runtime) consume(m *pb.Message) error {
	r.inputBytes[messageLane(m)].release(uint64(proto.Size(m)))
	if m.GetType() == pb.MsgSnap {
		r.snapshotCandidate = true
	}
	return r.step(m)
}

func (r *Runtime) step(m *pb.Message) error {
	// Reserve a bounded amount of protocol work beyond proposal admission.
	// Stable-term heartbeats consume no append task and remain admissible.
	s := r.core.status()
	if !r.knownPeer(m.GetFrom()) {
		return nil
	}
	if r.deferredTerm != nil && m.GetTerm() <= r.deferredTerm.GetTerm() {
		// Do not grant a lower-term vote or revive old protocol work while
		// the observed higher term is waiting for its metadata reservation.
		return nil
	}
	// Pre-vote requests and successful pre-vote replies describe a prospective
	// term, not evidence that the peer has advanced its durable term.
	higherTerm := m.GetTerm() > s.Term && m.GetType() != pb.MsgPreVote &&
		!(m.GetType() == pb.MsgPreVoteResp && !m.GetReject())
	if higherTerm && (r.storageFull() || (m.GetType() == pb.MsgApp && len(m.Entries) > 0 && r.logFull())) {
		// The bounded mandatory-state reservation may be exhausted. Revoke
		// application authority now and retain only the maximum observed term.
		// Once space returns, a reject-only local observation lets RawNode
		// demote/persist in order without retaining this RPC or granting a vote.
		if r.deferredTerm == nil || m.GetTerm() > r.deferredTerm.GetTerm() {
			r.deferredTerm = &pb.Message{Type: pb.MsgVoteResp.Enum(), From: new(raft.LocalAppendThread), To: new(r.cfg.Local.ID), Term: m.Term, Reject: new(true)}
		}
		return nil
	}
	if higherTerm && m.GetType() == pb.MsgHeartbeatResp {
		// A stale challenge is not a liveness proof, but an authenticated
		// higher term must still demote us before its persistence completes.
		err := r.core.raw.Step(m)
		if errors.Is(err, raft.ErrStepPeerNotFound) {
			return nil
		}
		return err
	}
	if m.GetType() == pb.MsgApp && len(m.Entries) > 0 && r.logFull() {
		return nil
	}
	if m.GetType() == pb.MsgVote || m.GetType() == pb.MsgPreVote {
		eligible := false
		for _, ids := range [][]uint64{r.core.conf.GetVoters(), r.core.conf.GetLearners(), r.core.conf.GetVotersOutgoing()} {
			for _, id := range ids {
				if id == r.cfg.Local.ID {
					eligible = true
				}
			}
		}
		if !eligible || !r.knownPeer(r.cfg.Local.ID) {
			return nil
		}
	}
	if m.GetType() == pb.MsgHeartbeatResp && !r.acceptHeartbeat(m) {
		return nil
	}
	// Replication traffic cannot independently renew leader liveness. This
	// also lets upstream CheckQuorum demote an expired authority epoch even
	// if its log socket continues receiving replies after control loss.
	if s.IsLeader && m.GetType() == pb.MsgAppResp && m.GetTerm() <= s.Term {
		if r.epochRevoked || !r.peerLive(m.GetFrom(), time.Now()) {
			return nil
		}
	}
	if r.storageFull() {
		if (m.GetType() != pb.MsgHeartbeat && m.GetType() != pb.MsgHeartbeatResp) || m.GetTerm() != s.Term {
			return nil
		}
		if m.GetType() == pb.MsgHeartbeat {
			// The heartbeat remains a liveness proof, but its commit update
			// cannot consume another mandatory disk-completion reservation.
			m.Commit = new(s.Commit)
		}
	}
	err := r.core.raw.Step(m)
	if errors.Is(err, raft.ErrStepPeerNotFound) {
		return nil
	}
	return err
}

func (r *Runtime) advanceDeferredTerm() error {
	if r.deferredTerm == nil || r.storageFull() {
		return nil
	}
	message := r.deferredTerm
	r.deferredTerm = nil
	if message.GetTerm() <= r.core.status().Term {
		return nil
	}
	// Use the local executor address so removal of the original peer cannot
	// erase an already authenticated term observation. This reject-only message
	// is stepped strictly in a higher term: Raft demotes before dispatching it,
	// so it cannot count as a vote, acknowledge storage, or elect a candidate.
	return r.core.raw.Step(message)
}

func (r *Runtime) ready() error {
	adoptedSnapshot := false
	defer func() {
		if r.snapshotCandidate {
			if !adoptedSnapshot {
				r.receivingSnapshot.Store(false)
			}
			r.snapshotCandidate = false
		}
	}()
	if !r.core.raw.HasReady() {
		return nil
	}
	rd := r.core.raw.Ready()
	for _, m := range rd.Messages {
		switch m.GetType() {
		case pb.MsgStorageAppend:
			r.core.noteAppend(m)
			r.appendQueue = append(r.appendQueue, m)
			r.pendingBytes += uint64(proto.Size(m))
			r.pendingTasks++
			if !raft.IsEmptySnap(m.Snapshot) {
				adoptedSnapshot = true
				r.bulkQueue = append(r.bulkQueue, snapshotWork{image: m.Snapshot, incoming: m})
				r.applyQueue = append(r.applyQueue, &applyJob{task: m})
			}
		case pb.MsgStorageApply:
			r.applyQueue = append(r.applyQueue, &applyJob{task: m})
		default:
			r.send(m)
		}
	}
	// AsyncStorageWrites owns advancement through the attached responses;
	// calling RawNode.Advance here would falsely acknowledge unfinished work.
	return nil
}

func (r *Runtime) finishDisk(done *diskCompletion) error {
	if err := r.core.persisted(done.task); err != nil {
		return err
	}
	if err := r.responses(done.task); err != nil {
		return err
	}
	r.pendingBytes -= uint64(proto.Size(done.task))
	r.pendingTasks--
	if !raft.IsEmptySnap(done.task.Snapshot) {
		r.receivingSnapshot.Store(false)
		r.preparedIncoming = nil
	}
	close(done.ack)
	return nil
}

func (r *Runtime) responses(task *pb.Message) error {
	for _, m := range task.Responses {
		if m.GetTo() == r.cfg.Local.ID {
			if err := r.core.raw.Step(m); err != nil {
				return err
			}
		} else {
			r.send(m)
		}
	}
	return nil
}

func (r *Runtime) send(m *pb.Message) {
	if m.GetType() == pb.MsgVoteResp || m.GetType() == pb.MsgPreVoteResp {
		if m.GetReject() {
			r.voteRejections++
		} else {
			r.voteGrants++
		}
	}
	m = r.stampHeartbeat(m)
	if m == nil {
		return
	}
	if !r.transport.Send(m) {
		r.core.raw.ReportUnreachable(m.GetTo())
		if m.GetType() == pb.MsgSnap {
			r.core.raw.ReportSnapshot(m.GetTo(), raft.SnapshotFailure)
		}
	}
}

func (r *Runtime) diskLoop() {
	defer r.workers.Done()
	for {
		select {
		case <-r.stop:
			return
		case tasks := <-r.diskJobs:
			task := tasks[0]
			if len(tasks) > 1 {
				// Ordered group commit: persist the final HardState and every
				// entry in submission order, then release the original responses
				// in that same order. No task is acknowledged on Save alone.
				merged := &pb.Message{Type: pb.MsgStorageAppend.Enum()}
				for _, pending := range tasks {
					merged.Entries = append(merged.Entries, pending.Entries...)
					if pending.GetTerm() != 0 || pending.GetVote() != 0 || pending.GetCommit() != 0 {
						merged.Term, merged.Vote, merged.Commit = pending.Term, pending.Vote, pending.Commit
					}
				}
				task = merged
			}
			var err error
			if task.GetType() == pb.MsgSnap {
				err = r.disk.publishSnapshotMarker(task.Snapshot)
			} else if task.GetType() == pb.MsgProp {
				err = r.disk.saveJoin(task.Context)
			} else {
				err = r.disk.savePrepared(task, true)
			}
			for _, original := range tasks {
				done := &diskCompletion{task: original, err: err, ack: make(chan struct{})}
				select {
				case r.diskDone <- done:
				case <-r.stop:
					return
				}
				select {
				case <-done.ack:
				case <-r.stop:
					return
				}
			}
		}
	}
}

func (r *Runtime) applyLoop() {
	defer r.workers.Done()
	for {
		select {
		case <-r.stop:
			return
		case job := <-r.applyJobs:
			done := &applyCompletion{job: job, ack: make(chan struct{})}
			if job.capture != nil {
				data, err := r.app.Capture(job.capture.metadata.GetIndex())
				if err == nil {
					data, err = encodeSnapshot(job.capture.members, data)
				}
				done.err = err
				if err == nil {
					done.snapshot = &pb.Snapshot{Metadata: job.capture.metadata, Data: data}
				}
			} else if job.install != nil {
				_, data, err := decodeSnapshot(job.task.Snapshot.Data)
				if err == nil {
					err = r.app.Install(job.task.Snapshot.Metadata.GetIndex(), data)
				}
				done.err = err
			} else {
				for _, e := range job.task.Entries {
					result := Result{Index: e.GetIndex()}
					if e.GetType() == pb.EntryNormal && len(e.Data) > 0 {
						_, data, err := decodeProposal(e.Data)
						if err == nil {
							result.Data, err = r.app.Apply(e.GetIndex(), data)
						}
						if err != nil {
							done.err = err
							break
						}
					} else if app, ok := r.app.(interface{ Advance(uint64) }); ok {
						app.Advance(e.GetIndex())
					}
					done.results = append(done.results, result)
				}
			}
			if done.err == nil && r.checkIdentities && job.capture == nil {
				done.bindings, done.err = r.app.(IdentitySource).Identities()
			}
			select {
			case r.applyDone <- done:
			case <-r.stop:
				return
			}
			select {
			case <-done.ack:
			case <-r.stop:
				return
			}
		}
	}
}

func decodeProposal(data []byte) ([16]byte, []byte, error) {
	var key [16]byte
	if len(data) < 24 || string(data[:4]) != "LPR1" || uint64(binary.LittleEndian.Uint32(data[20:])) != uint64(len(data)-24) {
		return key, nil, fmt.Errorf("corrupt committed proposal envelope")
	}
	copy(key[:], data[4:20])
	return key, data[24:], nil
}
