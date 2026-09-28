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
	"strings"
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
	Sentinel  string `json:"sentinel"`
	Principal string `json:"principal"`
}

func (m Member) validate() error {
	if m.ID == 0 || m.ID > 0x7fffffff || m.Principal != fmt.Sprintf("lavik://meta/%d", m.ID) {
		return errors.New("invalid member identity")
	}
	endpoints := []string{m.Raft, m.Data, m.Admin}
	if m.Sentinel != "" && !validSentinelEndpoint(m.Sentinel) {
		return fmt.Errorf("invalid Sentinel endpoint %q", m.Sentinel)
	}
	for _, addr := range endpoints {
		host, port, err := net.SplitHostPort(addr)
		p, parseErr := strconv.ParseUint(port, 10, 16)
		ip := net.ParseIP(host)
		if err != nil || ip == nil || ip.IsUnspecified() || parseErr != nil || p == 0 {
			return fmt.Errorf("invalid member endpoint %q", addr)
		}
	}
	return nil
}

// Sentinel addresses are application routes. Keep their transport and DNS name;
// unlike control endpoints, they may refer to a TLS proxy or a stable hostname.
func validSentinelEndpoint(address string) bool {
	if len(address) > 256 {
		return false
	}
	if strings.HasPrefix(address, "tcp://") {
		address = strings.TrimPrefix(address, "tcp://")
	} else if strings.HasPrefix(address, "tls://") {
		address = strings.TrimPrefix(address, "tls://")
	}
	host, port, err := net.SplitHostPort(address)
	p, perr := strconv.ParseUint(port, 10, 16)
	if err != nil || perr != nil || p == 0 || host == "" {
		return false
	}
	if ip := net.ParseIP(host); ip != nil {
		return !ip.IsUnspecified()
	}
	if len(host) > 253 {
		return false
	}
	host = strings.TrimSuffix(host, ".")
	hasLetter := false
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, c := range label {
			letter := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
			hasLetter = hasLetter || letter
			if !letter && !(c >= '0' && c <= '9') && c != '-' {
				return false
			}
		}
	}
	return hasLetter
}

// A proxy changes host/port but cannot change the required listener transport.
// Zero infers the legacy one-listener configuration for in-process callers.
func supportsSentinelRoute(route, local string, transports uint8) bool {
	if route == "" {
		return true
	}
	if transports == 0 && local != "" {
		transports = 1
		if strings.HasPrefix(local, "tls://") {
			transports = 2
		}
	}
	required := uint8(1)
	if strings.HasPrefix(route, "tls://") {
		required = 2
	}
	return transports&required != 0
}

// Config is immutable after Open. Initial is used only on a pristine directory;
// an empty Initial creates a waiting joiner, never a single-node cluster.
type Config struct {
	// Startup-only capabilities, separate from the immutable advertised route.
	SentinelTransports uint8         `json:"sentinel_transports"`
	Local              Member        `json:"local"`
	Initial            []Member      `json:"initial"`
	Dir                string        `json:"dir"`
	Listen             string        `json:"listen"`
	TLSCA              string        `json:"tls_ca"`
	TLSCert            string        `json:"tls_cert"`
	TLSKey             string        `json:"tls_key"`
	Heartbeat          time.Duration `json:"-"`
	ElectionTicks      int           `json:"election_ticks"`
	QueueCapacity      int           `json:"queue_capacity"`
	MaxPendingBytes    uint64        `json:"max_pending_bytes"`
	SnapshotDistance   uint64        `json:"snapshot_distance"`
	ReservedLogItems   uint64        `json:"reserved_log_items"`
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
