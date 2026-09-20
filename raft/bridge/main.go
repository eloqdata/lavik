// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

package main

/*
#cgo CFLAGS: -I${SRCDIR}/../../include
#include "callbacks.h"
*/
import "C"

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/eloqdata/lavik/raft/engine"
)

var handles sync.Map
var nextHandle atomic.Uint64

type instance struct {
	runtime   *engine.Runtime
	owner     C.uintptr_t
	callbacks C.LavikRaftCallbacks
	// Serializes bounded enqueue/WaitGroup.Add against close. C++ invokes
	// submission from its proposal executor, never a Bycorf worker. This lock
	// never covers callbacks, protocol execution, disk, or waiting for results.
	mu      sync.Mutex
	closed  bool
	waiters sync.WaitGroup
}

func (i *instance) Apply(index uint64, data []byte) ([]byte, error) {
	input := C.CBytes(data)
	defer C.free(input)
	var out C.LavikRaftBytes
	code := C.call_apply(&i.callbacks, i.owner, C.uint64_t(index), input, C.uint64_t(len(data)), &out)
	defer C.free(out.data)
	if code != 0 {
		return nil, errors.New("C++ committed apply failed")
	}
	return copyOutput(out)
}
func (i *instance) Advance(index uint64) { C.call_advance(&i.callbacks, i.owner, C.uint64_t(index)) }
func (i *instance) Identities() ([]engine.Binding, error) {
	var out C.LavikRaftBytes
	code := C.call_identities(&i.callbacks, i.owner, &out)
	defer C.free(out.data)
	if code != 0 {
		return nil, errors.New("C++ identity projection failed")
	}
	data, err := copyOutput(out)
	if err != nil {
		return nil, err
	}
	var bindings []engine.Binding
	if err = json.Unmarshal(data, &bindings); err != nil {
		return nil, err
	}
	return bindings, nil
}
func (i *instance) Install(index uint64, data []byte) error {
	input := C.CBytes(data)
	defer C.free(input)
	if C.call_install(&i.callbacks, i.owner, C.uint64_t(index), input, C.uint64_t(len(data))) != 0 {
		return errors.New("C++ snapshot install failed")
	}
	return nil
}
func (i *instance) Capture(index uint64) ([]byte, error) {
	var out C.LavikRaftBytes
	code := C.call_capture(&i.callbacks, i.owner, C.uint64_t(index), &out)
	defer C.free(out.data)
	if code != 0 {
		return nil, errors.New("C++ snapshot capture failed")
	}
	return copyOutput(out)
}
func copyOutput(out C.LavikRaftBytes) ([]byte, error) {
	if out.size > 512<<20 || (out.size != 0 && out.data == nil) {
		return nil, errors.New("invalid C++ callback buffer")
	}
	return C.GoBytes(out.data, C.int(out.size)), nil
}
func output(out *C.LavikRaftBytes, data []byte) {
	out.data = C.CBytes(data)
	out.size = C.uint64_t(len(data))
}
func flag(v bool) C.int {
	if v {
		return 1
	}
	return 0
}
func (i *instance) role(role engine.Role) {
	C.call_role(&i.callbacks, i.owner, C.uint64_t(role.Term), C.uint64_t(role.Leader), flag(role.IsLeader), flag(role.CaughtUp), C.uint64_t(role.ResignIndex))
}
func (i *instance) fatal(err error) {
	data := []byte(err.Error())
	ptr := C.CBytes(data)
	defer C.free(ptr)
	C.call_fatal(&i.callbacks, i.owner, ptr, C.uint64_t(len(data)))
}

//export lavik_raft_open
func lavik_raft_open(config unsafe.Pointer, size C.uint64_t, owner C.uintptr_t, callbacks *C.LavikRaftCallbacks, errOut *C.LavikRaftBytes) C.uint64_t {
	if size > 1<<20 || config == nil || callbacks == nil || errOut == nil {
		return 0
	}
	var cfg struct {
		engine.Config
		HeartbeatMS int `json:"heartbeat_ms"`
	}
	if err := json.Unmarshal(C.GoBytes(config, C.int(size)), &cfg); err != nil {
		output(errOut, []byte(err.Error()))
		return 0
	}
	cfg.Heartbeat = time.Duration(cfg.HeartbeatMS) * time.Millisecond
	network, err := engine.NewNetwork(cfg.Config)
	if err != nil {
		output(errOut, []byte(err.Error()))
		return 0
	}
	i := &instance{owner: owner, callbacks: *callbacks}
	i.runtime, err = engine.Open(cfg.Config, i, network, i.role, i.fatal)
	if err != nil {
		network.Close()
		output(errOut, []byte(err.Error()))
		return 0
	}
	handle := nextHandle.Add(1)
	handles.Store(handle, i)
	network.Start(i.runtime.Step)
	return C.uint64_t(handle)
}

func lookup(handle C.uint64_t) *instance {
	value, ok := handles.Load(uint64(handle))
	if !ok {
		return nil
	}
	return value.(*instance)
}

func (i *instance) deliver(ticket C.uint64_t, result <-chan engine.Result) {
	defer i.waiters.Done()
	out := <-result
	code := C.int(0)
	if out.Err != nil {
		code = 1
		if errors.Is(out.Err, engine.ErrNotLeader) {
			code = 2
		}
		if errors.Is(out.Err, engine.ErrBusy) {
			code = 3
		}
	}
	data := C.CBytes(out.Data)
	defer C.free(data)
	C.call_result(&i.callbacks, i.owner, ticket, C.uint64_t(out.Index), code, data, C.uint64_t(len(out.Data)))
}

//export lavik_raft_propose
func lavik_raft_propose(handle C.uint64_t, ticket C.uint64_t, data unsafe.Pointer, size C.uint64_t) C.int {
	i := lookup(handle)
	if i == nil {
		return 1
	}
	if size > 1<<20 || (size != 0 && data == nil) {
		return 3
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.closed {
		return 1
	}
	result, err := i.runtime.Propose(C.GoBytes(data, C.int(size)))
	if err != nil {
		return 3
	}
	i.waiters.Add(1)
	go i.deliver(ticket, result)
	return 0
}

//export lavik_raft_snapshot
func lavik_raft_snapshot(handle C.uint64_t, ticket C.uint64_t) C.int {
	i := lookup(handle)
	if i == nil {
		return 1
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.closed {
		return 1
	}
	result, err := i.runtime.Snapshot()
	if err != nil {
		return 3
	}
	i.waiters.Add(1)
	go i.deliver(ticket, result)
	return 0
}

//export lavik_raft_member
func lavik_raft_member(handle C.uint64_t, ticket C.uint64_t, data unsafe.Pointer, size C.uint64_t, remove C.int, learner C.int) C.int {
	i := lookup(handle)
	if i == nil {
		return 1
	}
	if size > 64<<10 || data == nil {
		return 3
	}
	var member engine.Member
	if err := json.Unmarshal(C.GoBytes(data, C.int(size)), &member); err != nil {
		return 3
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.closed {
		return 1
	}
	result, err := i.runtime.ChangeMember(member, remove != 0, learner != 0)
	if err != nil {
		return 3
	}
	i.waiters.Add(1)
	go i.deliver(ticket, result)
	return 0
}

//export lavik_raft_resign
func lavik_raft_resign(handle C.uint64_t, index C.uint64_t) {
	if i := lookup(handle); i != nil {
		i.runtime.RequestResign(uint64(index))
	}
}

//export lavik_raft_status
func lavik_raft_status(handle C.uint64_t, out *C.LavikRaftBytes) C.int {
	i := lookup(handle)
	if i == nil || out == nil {
		return 1
	}
	s := i.runtime.Status()
	var data bytes.Buffer
	put := func(value uint64) { _ = binary.Write(&data, binary.LittleEndian, value) }
	text := func(value string) { put(uint64(len(value))); data.WriteString(value) }
	put(1)
	put(s.Term)
	put(s.Leader)
	put(uint64(flag(s.IsLeader)))
	put(uint64(flag(s.CaughtUp)))
	put(s.Commit)
	put(s.Applied)
	put(s.Durable)
	put(s.Snapshot)
	put(s.RetainedBytes)
	put(s.FirstIndex)
	put(s.PendingBytes)
	put(s.GCFailures)
	put(s.RPCFailures)
	put(s.VoteRejections)
	put(s.VoteGrants)
	put(s.SnapshotFailures)
	put(s.ConfigIndex)
	put(uint64(len(s.Genesis)))
	for _, id := range s.Genesis {
		put(id)
	}
	put(uint64(len(s.Members)))
	for _, m := range s.Members {
		put(m.ID)
		text(m.Raft)
		text(m.Data)
		text(m.Admin)
		text(m.Principal)
		put(s.PeerApplied[m.ID])
		age, ok := s.PeerAgeMicros[m.ID]
		if !ok {
			age = ^uint64(0)
		}
		put(age)
		put(uint64(flag(s.Learners[m.ID])))
	}
	text(s.Failure)
	output(out, data.Bytes())
	return 0
}

//export lavik_raft_close
func lavik_raft_close(handle C.uint64_t) {
	i := lookup(handle)
	if i == nil {
		return
	}
	i.mu.Lock()
	if i.closed {
		i.mu.Unlock()
		return
	}
	i.closed = true
	i.mu.Unlock()
	i.runtime.Close()
	i.waiters.Wait()
	handles.Delete(uint64(handle))
}

func main() {}
