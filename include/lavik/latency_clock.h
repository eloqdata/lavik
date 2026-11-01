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
#include <limits>

#include "bycorf/runtime/cycle_clock.h"
#include "bycorf/runtime/worker.h"
namespace lavik {
// Only sampled operations read this clock. Each worker segment anchors TSC
// to steady_clock, so cross-worker offsets never assume synchronized counters.
// Local durations use the process-cached counter frequency. Migration rebases
// the anchor before reading a new worker's counter.
struct LatencySegmentClock {
  std::uint64_t NowNanos() noexcept {
    const auto worker = bycorf::ThisWorker().id_;
    if (worker != clock_worker_) {
      clock_ns_ = std::chrono::duration_cast<std::chrono::nanoseconds>(
                      std::chrono::steady_clock::now().time_since_epoch())
                      .count();
      clock_ticks_ = bycorf::ReadCycleCounter();
      clock_worker_ = worker;
    }
    return clock_ns_ + static_cast<std::uint64_t>(
                           (bycorf::ReadCycleCounter() - clock_ticks_) *
                           (1000000000.0 / bycorf::CycleCounterFrequency()));
  }

 private:
  unsigned clock_worker_ = std::numeric_limits<unsigned>::max();
  std::uint64_t clock_ns_ = 0, clock_ticks_ = 0;
};
}  // namespace lavik
