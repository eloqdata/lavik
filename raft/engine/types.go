// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

// Package engine owns the Meta Raft protocol, independently of the C++ state
// machine and the Bycorf worker. Only the event loop may access RawNode.
package engine

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
)

var (
	ErrStopped   = errors.New("raft stopped")
	ErrBusy      = errors.New("raft queue capacity exceeded")
	ErrNotLeader = errors.New("not leader")
	// ErrUncertain follows Raft admission: the proposal may still commit or
	// apply after demotion. ErrNotLeader is reserved for pre-append rejection.
	ErrUncertain = errors.New("raft proposal outcome is uncertain")
)

// Member is the durable descriptor associated with a Raft member ID. IDs and
// their canonical certificate principals are never reused after removal.
type Member struct {
	ID        uint64 `json:"id"`
	Raft      string `json:"raft"`
	Data      string `json:"data"`
	Admin     string `json:"admin"`
	Principal string `json:"principal"`
}

func (m Member) validate() error {
	if m.ID == 0 || m.ID > 0x7fffffff || m.Principal != fmt.Sprintf("lavik://meta/%d", m.ID) {
		return errors.New("invalid member identity")
	}
	for _, addr := range []string{m.Raft, m.Data, m.Admin} {
		host, port, err := net.SplitHostPort(addr)
		p, parseErr := strconv.ParseUint(port, 10, 16)
		ip := net.ParseIP(host)
		if err != nil || ip == nil || ip.IsUnspecified() || parseErr != nil || p == 0 {
			return fmt.Errorf("invalid member endpoint %q", addr)
		}
	}
	return nil
}

// Config is immutable after Open. Initial is used only on a pristine directory;
// an empty Initial creates a waiting joiner, never a single-node cluster.
type Config struct {
	Local            Member        `json:"local"`
	Initial          []Member      `json:"initial"`
	Dir              string        `json:"dir"`
	Listen           string        `json:"listen"`
	TLSCA            string        `json:"tls_ca"`
	TLSCert          string        `json:"tls_cert"`
	TLSKey           string        `json:"tls_key"`
	Heartbeat        time.Duration `json:"-"`
	ElectionTicks    int           `json:"election_ticks"`
	QueueCapacity    int           `json:"queue_capacity"`
	MaxPendingBytes  uint64        `json:"max_pending_bytes"`
	SnapshotDistance uint64        `json:"snapshot_distance"`
	ReservedLogItems uint64        `json:"reserved_log_items"`
	// Tests install the hook before any executor starts and change its
	// behavior through their own atomic barrier, never by racing the owner.
	beforeSave func() error
	beforeIO   func(string) error
}

// Application is called by one dedicated executor, never the protocol loop.
// Apply must return only after the C++ state machine has actually applied the
// entry. Install must atomically replace the state at the supplied snapshot cut.
// Failure is fatal: continuing after uncertain apply would fork replicas.
type Application interface {
	Apply(index uint64, data []byte) ([]byte, error)
	Install(index uint64, data []byte) error
	Capture(index uint64) ([]byte, error)
}

// Transport.Send never waits for the peer or a socket. A false result denotes
// bounded-queue backpressure, not an acknowledgement. Raft retries replication.
type Transport interface {
	Send(*pb.Message) bool
	Update([]Member)
	Close()
}

// Role notifications must be nonblocking. In particular a demotion must revoke
// C++ authority synchronously, before any queued apply or snapshot event.
type Role struct {
	Term     uint64
	Leader   uint64
	IsLeader bool
	CaughtUp bool
	// ResignIndex acknowledges the caller's revocation generation. A foreign
	// role callback sampled before that request cannot restore its authority.
	ResignIndex uint64
}

// Status is an immutable observation, not a linearizable read certificate.
type Status struct {
	Role
	Commit           uint64
	Applied          uint64
	Durable          uint64
	Snapshot         uint64
	Members          []Member
	PeerApplied      map[uint64]uint64
	PeerAgeMicros    map[uint64]uint64
	PendingBytes     uint64
	RetainedBytes    uint64
	GCFailures       uint64
	RPCFailures      uint64
	VoteRejections   uint64
	VoteGrants       uint64
	FirstIndex       uint64
	Failure          string
	SnapshotFailures uint64
	ConfigIndex      uint64
	Learners         map[uint64]bool
	Genesis          []uint64
}

// Result reports an actually applied proposal, including a domain rejection
// encoded by the C++ state machine. A local timeout cannot retract a proposal.
type Result struct {
	Index uint64
	Data  []byte
	Err   error
}
