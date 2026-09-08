#pragma once

#include <atomic>
#include <cstdint>

namespace keylane::storage {

// An incremental successor inherits untouched groups from this decision.
// The successor must not become independently durable before this decision
// does. Same-transaction commands can reuse the view without waiting.
// State crosses worker owners; no worker-local notification is shared here.
struct GroupedCommitDecision {
  enum class State : std::uint8_t { kPending, kDurable, kFailed };
  explicit GroupedCommitDecision(std::uint64_t txid) : txid_(txid) {}
  const std::uint64_t txid_;
  std::atomic<State> state_{State::kPending};

  void FailPending() noexcept {
    State pending = State::kPending;
    state_.compare_exchange_strong(pending, State::kFailed,
                                   std::memory_order_release,
                                   std::memory_order_relaxed);
  }
};

}  // namespace keylane::storage
