/*
 * Copyright (C) 2026 EloqData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once
#include <chrono>
#include <cstdint>

#include "lavik/latency_clock.h"
#ifndef LAVIK_ENABLE_TRACE
#define LAVIK_ENABLE_TRACE 0
#endif
namespace lavik {
// Full payload exists only in a selected online sidecar or a trace build.
struct ReadLatencyTrace : LatencySegmentClock {
  std::uint64_t request_start_ns_ = 0;
  std::uint64_t owner_start_ns_ = 0;
  std::uint64_t lookup_done_ns_ = 0;
  std::uint64_t buffer_acquire_start_ns_ = 0;
  std::uint64_t buffer_acquired_ns_ = 0;
  std::uint64_t io_submit_ns_ = 0;
  std::uint64_t io_complete_ns_ = 0;
  std::uint64_t decode_done_ns_ = 0;
  std::uint64_t owner_done_ns_ = 0;
  std::uint64_t origin_resume_ns_ = 0;
  std::uint64_t send_start_ns_ = 0;
  std::uint64_t send_complete_ns_ = 0;
  bool remote_ = false;
  bool hit_ = false;
  bool disk_read_ = false;
  bool heap_read_buffer_ = false;
  std::uint64_t key_lock_start_ns_ = 0;
  std::uint64_t key_lock_acquired_ns_ = 0;
};
#if LAVIK_ENABLE_TRACE
using LegacyReadLatencyTrace = ReadLatencyTrace;
inline std::uint64_t ReadTraceNowNanos() noexcept {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}
#else
struct LegacyReadLatencyTrace {};
inline constexpr std::uint64_t ReadTraceNowNanos() noexcept { return 0; }
#endif
}  // namespace lavik
