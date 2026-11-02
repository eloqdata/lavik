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
#include <cstdint>

#include "lavik/latency_clock.h"
#include "lavik/latency_stage.h"
namespace lavik {
// Physical operation state is allocated only for selected diagnostics.
struct ReadLatencyTrace : LatencySegmentClock {
  OnlineLatencyTrace* diagnostics_ = nullptr;
  // Selected pages share only the root's worker-local clock, never mutable
  // physical payloads. Standalone storage observers use their own anchor.
  std::uint64_t NowNanos() noexcept {
    return diagnostics_ ? TraceNowNanos(diagnostics_)
                        : LatencySegmentClock::NowNanos();
  }

  void RecordBuffer(std::uint64_t start, std::uint64_t end) noexcept {
    // Online output uses the root; standalone observers keep local intervals.
    if (diagnostics_)
      RecordTraceInterval(diagnostics_, TraceStage::kReadBuffer, start, end);
    else
      buffer_timing_.Add(start, end);
  }
  // Records actual submitted I/O through completion, excluding buffer waits.
  void RecordIo(std::uint64_t start, std::uint64_t end) noexcept {
    if (diagnostics_)
      RecordTraceInterval(diagnostics_, TraceStage::kReadIo, start, end);
    else
      io_timing_.Add(start, end);
  }

  LatencyIntervals buffer_timing_;
  LatencyIntervals io_timing_;
  std::uint64_t owner_start_ns_ = 0;
  std::uint64_t lookup_done_ns_ = 0;
  std::uint64_t buffer_acquire_start_ns_ = 0;
  std::uint64_t buffer_acquired_ns_ = 0;
  std::uint64_t io_submit_ns_ = 0;
  std::uint64_t io_complete_ns_ = 0;
  std::uint64_t decode_done_ns_ = 0;
  std::uint64_t owner_done_ns_ = 0;
  bool remote_ = false;
  bool disk_read_ = false;
  std::uint64_t key_lock_start_ns_ = 0;
  std::uint64_t key_lock_acquired_ns_ = 0;
};
}  // namespace lavik
