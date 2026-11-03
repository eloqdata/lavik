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
#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <limits>

#include "bycorf/runtime/cycle_clock.h"
#include "bycorf/runtime/worker.h"
namespace lavik {
// Repeated waits/I/O contribute one per-command aggregate and up to eight
// exact intervals for detail. Truncating detail never truncates the aggregate.
struct LatencyIntervals {
  struct Interval {
    std::uint64_t start_ns = 0, end_ns = 0;
  };
  std::array<Interval, 8> intervals{};
  std::uint64_t first_ns = 0, last_ns = 0, total_ns = 0;
  std::uint64_t count = 0;
  void Add(std::uint64_t start, std::uint64_t end) noexcept {
    if (!start || end < start) return;
    first_ns = count ? std::min(first_ns, start) : start;
    last_ns = std::max(last_ns, end);
    total_ns += end - start;
    if (count < intervals.size()) intervals[count] = {start, end};
    ++count;
  }
  // Merge only on the owning worker. Parallel children may finish out of
  // order; keep the earliest retained detail while summing all resource time.
  void Merge(const LatencyIntervals& other) noexcept {
    if (!other.count) return;
    first_ns = count ? std::min(first_ns, other.first_ns) : other.first_ns;
    last_ns = std::max(last_ns, other.last_ns);
    total_ns += other.total_ns;
    std::array<Interval, 16> candidates{};
    const auto kept = std::min<std::uint64_t>(count, intervals.size());
    const auto incoming =
        std::min<std::uint64_t>(other.count, other.intervals.size());
    std::copy_n(intervals.begin(), kept, candidates.begin());
    std::copy_n(other.intervals.begin(), incoming, candidates.begin() + kept);
    const auto size = kept + incoming;
    std::sort(
        candidates.begin(), candidates.begin() + size,
        [](const auto& a, const auto& b) { return a.start_ns < b.start_ns; });
    std::copy_n(candidates.begin(),
                std::min<std::uint64_t>(size, intervals.size()),
                intervals.begin());
    count += other.count;
  }
};

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
