/*
 * Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <cstddef>

namespace lavik::storage {

// Flush eligibility is independent of submission size: a small dirty prefix
// need not wait for this much data, while an accumulated snapshot can use it
// immediately without paying an I/O completion round trip per trigger-sized
// piece. The bound also limits how much one submission puts ahead of readers.
inline constexpr std::size_t kMaxFlushWriteBytes = 1024 * 1024;

}  // namespace lavik::storage
