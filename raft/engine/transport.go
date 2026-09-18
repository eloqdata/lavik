// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	laneControl = iota
	laneLog
	laneSnapshot
	laneCount
)

type peerSender struct {
	member         Member
	queues         [laneCount]chan *pb.Message
	stop           chan struct{}
	once           sync.Once
	txGeneration   atomic.Uint64
	rxGeneration   atomic.Uint64
	snapshotResult atomic.Uint32
}
type peerTable struct{ peers map[uint64]*peerSender }

// Network separates control, log replication, and snapshots into independent
// TCP connections. A slow write/reconnect in one lane cannot acquire a lock or
// consume queue capacity used by another lane. Send only publishes a pointer to
// an immutable message; encoding and TLS always run on the lane's goroutine.
type Network struct {
	cfg       Config
	tls       *tls.Config
	listener  net.Listener
	peers     atomic.Pointer[peerTable]
	stop      chan struct{}
	ctx       context.Context
	cancel    context.CancelFunc
	workers   sync.WaitGroup
	closeOnce sync.Once
	// Only connection goroutines use this mutex. The protocol loop neither
	// acquires it nor waits for a connection to close during member changes.
	connectionsMu sync.Mutex
	connections   map[net.Conn]struct{}
	accepted      map[[2]uint64]net.Conn
	slots         chan struct{}
	receiver      atomic.Pointer[networkReceiver]
	joinReceiver  atomic.Pointer[networkJoinReceiver]
	configIndex   atomic.Uint64
	local         atomic.Pointer[Member]
	sendBytes     [laneCount]byteBudget
	receiveBytes  [laneCount]byteBudget
	failures      atomic.Uint64
}
type networkReceiver struct{ step func(*pb.Message) error }
type networkJoinReceiver struct{ join func(joinSeed) error }

// NewNetwork validates TLS and binds the listener but does not accept inbound
// traffic before Start. Partial TLS configuration is always an error.
func NewNetwork(cfg Config) (*Network, error) {
	tlsConfig, err := makeTLS(cfg)
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	n := &Network{cfg: cfg, tls: tlsConfig, listener: listener, ctx: ctx, cancel: cancel, stop: make(chan struct{}), connections: map[net.Conn]struct{}{}, accepted: map[[2]uint64]net.Conn{}, slots: make(chan struct{}, 128)}
	n.peers.Store(&peerTable{peers: map[uint64]*peerSender{}})
	local := cfg.Local
	n.local.Store(&local)
	return n, nil
}

func (n *Network) SetJoin(join func(joinSeed) error) {
	n.joinReceiver.Store(&networkJoinReceiver{join: join})
}
func (n *Network) SetConfigIndex(index uint64) { n.configIndex.Store(index) }

// Start must be called once after the runtime's recovery has completed.
func (n *Network) Start(step func(*pb.Message) error) {
	if !n.receiver.CompareAndSwap(nil, &networkReceiver{step: step}) {
		panic("Raft listener started twice")
	}
	n.workers.Add(1)
	go func() {
		defer n.workers.Done()
		for {
			conn, err := n.listener.Accept()
			if err != nil {
				return
			}
			select {
			case n.slots <- struct{}{}:
			default:
				_ = conn.Close()
				continue
			}
			if !n.track(conn) {
				<-n.slots
				return
			}
			n.workers.Add(1)
			go func() {
				defer n.workers.Done()
				defer func() { <-n.slots; n.untrack(conn); _ = conn.Close() }()
				n.receive(conn)
			}()
		}
	}()
}

func messageLane(m *pb.Message) int {
	if controlMessage(m.GetType()) {
		return laneControl
	}
	if m.GetType() == pb.MsgSnap {
		return laneSnapshot
	}
	return laneLog
}

func (n *Network) Send(m *pb.Message) bool {
	peer := n.peers.Load().peers[m.GetTo()]
	if peer == nil {
		return false
	}
	select {
	case <-n.stop:
		return false
	case <-peer.stop:
		return false
	default:
	}
	lane := messageLane(m)
	size := uint64(proto.Size(m))
	if !n.sendBytes[lane].take(size, laneBudget(lane)) {
		return false
	}
	select {
	case peer.queues[lane] <- m:
		return true
	default:
		n.sendBytes[lane].release(size)
		return false
	}
}

// Generations invalidates queued liveness proofs across either direction's
// reconnect. The event loop reads atomics only; no socket lock is involved.
func (n *Network) Failures() uint64 { return n.failures.Load() }

func (n *Network) Generations(id uint64) (uint64, uint64) {
	if p := n.peers.Load().peers[id]; p != nil {
		return p.txGeneration.Load(), p.rxGeneration.Load()
	}
	return 0, 0
}

// SnapshotResults is drained by the protocol owner. Each peer has one active
// snapshot transfer in Raft; a retained atomic completion cannot be lost when
// the ordinary inbound/control queues are full.
func (n *Network) SnapshotResults(report func(uint64, bool)) {
	for id, peer := range n.peers.Load().peers {
		if result := peer.snapshotResult.Swap(0); result != 0 {
			report(id, result == 1)
		}
	}
}

// Update is called only by the protocol owner. It publishes one immutable peer
// table; removed descriptors cannot be reused by either new sends or receives.
func (n *Network) Update(members []Member) {
	old := n.peers.Load()
	next := &peerTable{peers: make(map[uint64]*peerSender, len(members))}
	for _, member := range members {
		if member.ID == n.cfg.Local.ID {
			local := member
			n.local.Store(&local)
			continue
		}
		if previous := old.peers[member.ID]; previous != nil && previous.member == member {
			next.peers[member.ID] = previous
			continue
		}
		p := &peerSender{member: member, stop: make(chan struct{})}
		// Snapshot messages can retain up to the object cap. One queued image
		// per destination prevents reconnect storms from multiplying that cap.
		p.queues = [laneCount]chan *pb.Message{make(chan *pb.Message, 32), make(chan *pb.Message, 16), make(chan *pb.Message, 1)}
		next.peers[member.ID] = p
		for lane := range laneCount {
			n.workers.Add(1)
			go n.sendLoop(p, lane)
		}
	}
	n.peers.Store(next)
	for id, p := range old.peers {
		if next.peers[id] != p {
			p.once.Do(func() { close(p.stop) })
		}
	}
}

func (n *Network) Close() {
	n.closeOnce.Do(func() {
		close(n.stop)
		n.cancel()
		_ = n.listener.Close()
		n.connectionsMu.Lock()
		for conn := range n.connections {
			_ = conn.Close()
		}
		n.connectionsMu.Unlock()
		n.workers.Wait()
	})
}

func (n *Network) track(conn net.Conn) bool {
	n.connectionsMu.Lock()
	defer n.connectionsMu.Unlock()
	select {
	case <-n.stop:
		_ = conn.Close()
		return false
	default:
	}
	n.connections[conn] = struct{}{}
	return true
}
func (n *Network) untrack(conn net.Conn) {
	n.connectionsMu.Lock()
	delete(n.connections, conn)
	n.connectionsMu.Unlock()
}

func (n *Network) sendLoop(peer *peerSender, lane int) {
	defer n.workers.Done()
	defer func() {
		for {
			select {
			case m := <-peer.queues[lane]:
				n.sendBytes[lane].release(uint64(proto.Size(m)))
			default:
				return
			}
		}
	}()
	var conn net.Conn
	defer func() {
		if conn != nil {
			n.untrack(conn)
			_ = conn.Close()
		}
	}()
	for {
		var m *pb.Message
		select {
		case <-n.stop:
			return
		case <-peer.stop:
			return
		case m = <-peer.queues[lane]:
		}
		func() {
			defer n.sendBytes[lane].release(uint64(proto.Size(m)))
			sent := false
			if lane == laneSnapshot {
				defer func() {
					if sent {
						peer.snapshotResult.Store(1)
					} else {
						peer.snapshotResult.Store(2)
					}
				}()
			}
			if n.peers.Load().peers[peer.member.ID] != peer {
				return
			}
			if conn == nil {
				var err error
				conn, err = n.dial(peer.member, lane)
				if err != nil {
					n.failures.Add(1)
					// Failed network messages are never local storage completions.
					// Raft retransmits using subsequent ticks on this same lane.
					timer := time.NewTimer(50 * time.Millisecond)
					select {
					case <-n.stop:
						timer.Stop()
						return
					case <-peer.stop:
						timer.Stop()
						return
					case <-timer.C:
					}
					return
				}
				if lane == laneControl {
					peer.txGeneration.Add(1)
				}
			}
			data, err := proto.Marshal(m)
			if err != nil {
				panic(err)
			}
			var size [4]byte
			binary.BigEndian.PutUint32(size[:], uint32(len(data)))
			deadline := time.Second
			if lane == laneSnapshot {
				deadline = 30 * time.Second
			}
			_ = conn.SetWriteDeadline(time.Now().Add(deadline))
			if err = writeAll(conn, size[:]); err == nil {
				err = writeAll(conn, data)
			}
			if err != nil {
				n.failures.Add(1)
				if lane == laneControl {
					peer.txGeneration.Add(1)
				}
				n.untrack(conn)
				_ = conn.Close()
				conn = nil
			}
			sent = err == nil
		}()
	}
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func (n *Network) dial(member Member, lane int) (net.Conn, error) {
	dialer := net.Dialer{Timeout: time.Second, KeepAlive: time.Second}
	conn, err := dialer.DialContext(n.ctx, "tcp", member.Raft)
	if err != nil {
		return nil, err
	}
	if !n.track(conn) {
		return nil, ErrStopped
	}
	failed := true
	defer func() {
		if failed {
			n.untrack(conn)
			_ = conn.Close()
		}
	}()
	if n.tls != nil {
		config := n.tls.Clone()
		host, _, _ := net.SplitHostPort(member.Raft)
		config.ServerName = host
		config.VerifyConnection = func(state tls.ConnectionState) error { return verifyPrincipal(state, member.Principal) }
		secure := tls.Client(conn, config)
		ctx, cancel := context.WithTimeout(n.ctx, time.Second)
		err = secure.HandshakeContext(ctx)
		cancel()
		if err != nil {
			return nil, err
		}
		n.untrack(conn)
		conn = secure
		if !n.track(conn) {
			return nil, ErrStopped
		}
	}
	var hello [40]byte
	copy(hello[:], "LRT1")
	hello[4] = byte(lane)
	hello[5] = 1 // Versioned invitation metadata precedes ordinary Raft frames.
	binary.BigEndian.PutUint64(hello[8:16], n.cfg.Local.ID)
	binary.BigEndian.PutUint64(hello[16:24], member.ID)
	if _, err = rand.Read(hello[24:]); err != nil {
		return nil, err
	}
	_ = conn.SetWriteDeadline(time.Now().Add(time.Second))
	if err = writeAll(conn, hello[:]); err != nil {
		return nil, err
	}
	seed := joinSeed{Index: n.configIndex.Load(), Members: []Member{*n.local.Load()}}
	for _, peer := range n.peers.Load().peers {
		seed.Members = append(seed.Members, peer.member)
	}
	metadata, err := json.Marshal(seed)
	if err != nil {
		return nil, err
	}
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(metadata)))
	if err = writeAll(conn, size[:]); err == nil {
		err = writeAll(conn, metadata)
	}
	if err != nil {
		return nil, err
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	var ack [1]byte
	if _, err = io.ReadFull(conn, ack[:]); err != nil {
		return nil, err
	}
	if ack[0] != 1 {
		return nil, errors.New("Raft peer rejected connection")
	}
	failed = false
	return conn, nil
}

func (n *Network) receive(raw net.Conn) {
	conn := raw
	var tlsState *tls.ConnectionState
	if n.tls != nil {
		secure := tls.Server(raw, n.tls)
		ctx, cancel := context.WithTimeout(n.ctx, time.Second)
		err := secure.HandshakeContext(ctx)
		cancel()
		if err != nil {
			return
		}
		conn = secure
		state := secure.ConnectionState()
		tlsState = &state
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	var hello [40]byte
	if _, err := io.ReadFull(conn, hello[:]); err != nil {
		return
	}
	if string(hello[:4]) != "LRT1" || hello[4] >= laneCount || hello[5] != 1 || hello[6] != 0 || hello[7] != 0 {
		return
	}
	lane := int(hello[4])
	from := binary.BigEndian.Uint64(hello[8:16])
	to := binary.BigEndian.Uint64(hello[16:24])
	peer := n.peers.Load().peers[from]
	if to != n.cfg.Local.ID || from == 0 || from == to {
		return
	}
	var metadataSize [4]byte
	if _, err := io.ReadFull(conn, metadataSize[:]); err != nil {
		return
	}
	size := binary.BigEndian.Uint32(metadataSize[:])
	if size == 0 || size > 1<<20 {
		return
	}
	metadata := make([]byte, size)
	if _, err := io.ReadFull(conn, metadata); err != nil {
		return
	}
	if peer == nil {
		if lane != laneControl {
			return
		}
		if tlsState != nil && verifyPrincipal(*tlsState, fmt.Sprintf("lavik://meta/%d", from)) != nil {
			return
		}
		var seed joinSeed
		if json.Unmarshal(metadata, &seed) != nil {
			return
		}
		found := false
		for _, m := range seed.Members {
			if m.ID == from {
				found = true
			}
		}
		if !found {
			return
		}
		join := n.joinReceiver.Load()
		if join == nil || join.join(seed) != nil {
			return
		}
		peer = n.peers.Load().peers[from]
		if peer == nil {
			return
		}
	}
	if tlsState != nil && verifyPrincipal(*tlsState, peer.member.Principal) != nil {
		return
	}
	_ = conn.SetWriteDeadline(time.Now().Add(time.Second))
	if writeAll(conn, []byte{1}) != nil {
		return
	}
	key := [2]uint64{from, uint64(lane)}
	n.connectionsMu.Lock()
	if old := n.accepted[key]; old != nil {
		_ = old.Close()
	}
	n.accepted[key] = raw
	if lane == laneControl {
		peer.rxGeneration.Add(1)
	}
	n.connectionsMu.Unlock()
	defer func() {
		n.connectionsMu.Lock()
		if n.accepted[key] == raw {
			delete(n.accepted, key)
			if lane == laneControl {
				peer.rxGeneration.Add(1)
			}
		}
		n.connectionsMu.Unlock()
	}()
	for {
		limit := uint32(2 << 20)
		deadline := 3 * time.Second
		if lane == laneControl {
			limit = 64 << 10
		}
		if lane == laneSnapshot {
			limit = 514 << 20
			deadline = 30 * time.Second
		}
		_ = conn.SetReadDeadline(time.Now().Add(deadline))
		var size [4]byte
		if _, err := io.ReadFull(conn, size[:]); err != nil {
			return
		}
		length := binary.BigEndian.Uint32(size[:])
		if length == 0 || length > limit {
			return
		}
		if !n.receiveBytes[lane].take(uint64(length), laneBudget(lane)) {
			return
		}
		ok := func() bool {
			defer n.receiveBytes[lane].release(uint64(length))
			data := make([]byte, length)
			if _, err := io.ReadFull(conn, data); err != nil {
				return false
			}
			var m pb.Message
			if err := proto.Unmarshal(data, &m); err != nil {
				return false
			}
			if m.GetFrom() != from || m.GetTo() != to || messageLane(&m) != lane || len(m.Responses) != 0 {
				return false
			}
			if n.peers.Load().peers[from] != peer {
				return false
			}
			n.connectionsMu.Lock()
			current := n.accepted[key] == raw
			n.connectionsMu.Unlock()
			if !current {
				return false
			}
			_ = n.receiver.Load().step(&m)
			return true
		}()
		if !ok {
			return
		}
	}
}

func makeTLS(cfg Config) (*tls.Config, error) {
	if cfg.TLSCA == "" && cfg.TLSCert == "" && cfg.TLSKey == "" {
		return nil, nil
	}
	if cfg.TLSCA == "" || cfg.TLSCert == "" || cfg.TLSKey == "" {
		return nil, errors.New("Raft TLS requires CA, certificate, and key")
	}
	ca, err := os.ReadFile(cfg.TLSCA)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, errors.New("invalid Raft TLS CA")
	}
	cert, err := tls.LoadX509KeyPair(cfg.TLSCert, cfg.TLSKey)
	if err != nil {
		return nil, err
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return nil, err
	}
	if len(leaf.URIs) != 1 || leaf.URIs[0].String() != cfg.Local.Principal {
		return nil, errors.New("local Raft certificate principal mismatch")
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}, RootCAs: roots, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert}, nil
}

func verifyPrincipal(state tls.ConnectionState, expected string) error {
	if len(state.VerifiedChains) == 0 || len(state.PeerCertificates) == 0 {
		return errors.New("unverified Raft certificate")
	}
	uris := state.PeerCertificates[0].URIs
	if len(uris) != 1 || uris[0].String() != expected {
		return fmt.Errorf("Raft certificate principal mismatch for %s", expected)
	}
	return nil
}
