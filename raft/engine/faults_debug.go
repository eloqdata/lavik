//go:build lavik_faults

// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

package engine

import "os"

// Debug-only process crash cuts, matching the C++ fault-test exit contract.
// These hooks execute on storage executors, never the heartbeat owner.
func crashPoint(point string) {
	if os.Getenv("LAVIK_CRASH_POINT") == point {
		os.Exit(86)
	}
}
