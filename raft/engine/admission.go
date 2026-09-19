// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

package engine

import "sync/atomic"

const admissionClosed = uint64(1) << 63

// admissionGate closes the enqueue-versus-drain race without a request-path
// mutex. A select may choose a writable queue even after stop has closed. The
// shutdown owner therefore seals admission, waits for existing submitters to
// leave, and only then drains queues. Submitters never wait on shutdown or I/O.
type admissionGate struct {
	state   atomic.Uint64
	drained chan struct{}
}

func (g *admissionGate) enter() bool {
	for {
		state := g.state.Load()
		if state&admissionClosed != 0 {
			return false
		}
		if g.state.CompareAndSwap(state, state+1) {
			return true
		}
	}
}

func (g *admissionGate) leave() {
	if g.state.Add(^uint64(0)) == admissionClosed {
		close(g.drained)
	}
}

func (g *admissionGate) seal() {
	if g.state.Or(admissionClosed) == 0 {
		close(g.drained)
	}
}
