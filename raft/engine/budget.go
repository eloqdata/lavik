// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

package engine

import "sync/atomic"

const maxSnapshotMessage = 514 << 20

// byteBudget reserves before allocating or retaining a message. Saturation is
// backpressure, never a reason to acknowledge work that has not completed.
type byteBudget struct{ used atomic.Uint64 }

func (b *byteBudget) take(n, limit uint64) bool {
	for {
		old := b.used.Load()
		if n > limit || old > limit-n {
			return false
		}
		if b.used.CompareAndSwap(old, old+n) {
			return true
		}
	}
}

func (b *byteBudget) release(n uint64) { b.used.Add(^(n - 1)) }

func laneBudget(lane int) uint64 {
	switch lane {
	case laneControl:
		return 8 << 20
	case laneSnapshot:
		return maxSnapshotMessage
	default:
		return 64 << 20
	}
}
