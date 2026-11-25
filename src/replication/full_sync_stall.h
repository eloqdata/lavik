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
#include <vector>

namespace lavik::detail {

// Control-worker-owned deadlines. A barrier waiter depends on its peers;
// an active transfer must itself advance even if another flow is healthy.
// Observe every flow in a poll before checking any deadline.
class FullSyncStallWatchdog {
 public:
  using Clock = std::chrono::steady_clock;
  FullSyncStallWatchdog(unsigned flows, Clock::time_point now,
                        Clock::duration timeout)
      : flows_(flows, Flow{0, now}), last_progress_(now), timeout_(timeout) {}

  void Observe(unsigned flow, std::uint64_t generation, Clock::time_point now) {
    auto& state = flows_[flow];
    if (state.generation == generation) return;
    state.generation = generation;
    state.last_progress = now;
    last_progress_ = now;
  }

  bool Stalled(unsigned flow, bool waiting_for_peers,
               Clock::time_point now) const {
    const auto last =
        waiting_for_peers ? last_progress_ : flows_[flow].last_progress;
    return now - last >= timeout_;
  }

 private:
  struct Flow {
    std::uint64_t generation;
    Clock::time_point last_progress;
  };
  std::vector<Flow> flows_;
  Clock::time_point last_progress_;
  Clock::duration timeout_;
};

}  // namespace lavik::detail
