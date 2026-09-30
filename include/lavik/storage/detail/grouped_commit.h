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

#include <atomic>
#include <cstdint>
#include <limits>

namespace lavik::storage {

// An incremental successor inherits untouched groups from this decision.
// The successor must not become independently durable before this decision
// does. Same-transaction commands can reuse the view without waiting.
// State can cross worker owners. A standalone command's decision completes
// on its key owner and can use that owner's durability notification; borrowed
// transactions may complete elsewhere and retain the cross-worker wait path.
struct GroupedCommitDecision {
  enum class State : std::uint8_t { kPending, kDurable, kFailed };
  static constexpr std::uint16_t kRemoteCompletion =
      std::numeric_limits<std::uint16_t>::max();
  explicit GroupedCommitDecision(
      std::uint64_t txid, std::uint16_t completion_owner = kRemoteCompletion)
      : txid_(txid), completion_owner_(completion_owner) {}
  const std::uint64_t txid_;
  const std::uint16_t completion_owner_;
  std::atomic<State> state_{State::kPending};

  void FailPending() noexcept {
    State pending = State::kPending;
    state_.compare_exchange_strong(pending, State::kFailed,
                                   std::memory_order_release,
                                   std::memory_order_relaxed);
  }
};

}  // namespace lavik::storage
