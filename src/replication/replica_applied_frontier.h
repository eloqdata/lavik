#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace keylane::detail {

class ReplicaAppliedFrontierTestPeer;

// Chooses the complete initial frontier for one reconnect handshake. Resume
// coordinates are reused only after the caller proves the whole source
// context and flow layout match; every mismatch resets every coordinate to
// one, never an old prefix.
absl::StatusOr<std::vector<std::uint64_t>> InitialAppliedNextLsnsForReconnect(
    unsigned source_flow_count,
    std::span<const std::uint64_t> requested_next_lsns,
    bool exact_resume_context_matches);

// Publishes the next unapplied LSN of every native source flow.
//
// A transport fragment is not an applied unit: callers advance a flow only
// after its complete logical event has been decoded and committed. Independent
// flow publications may be sampled at adjacent instants, but vector snapshots
// never expose one batch partially. Each publisher id is single-writer, and
// callers must not overlap ordinary and batch publication for the same flow.
class ReplicaAppliedFrontier {
 public:
  struct FlowApplied {
    unsigned flow_id_ = 0;
    std::uint64_t applied_lsn_ = 0;
  };

  // publisher_count is the number of target workers that may publish a
  // cross-flow transaction or control barrier.
  ReplicaAppliedFrontier(unsigned flow_count, unsigned publisher_count);
  ReplicaAppliedFrontier(const ReplicaAppliedFrontier&) = delete;
  ReplicaAppliedFrontier& operator=(const ReplicaAppliedFrontier&) = delete;

  std::size_t size() const noexcept { return flow_count_; }

  // Publishes one completely applied event. Repeating the exact completed LSN
  // is idempotent; gaps, regressions and UINT64_MAX are rejected.
  absl::Status AdvanceAfterApply(unsigned flow_id,
                                 std::uint64_t applied_lsn) noexcept;

  // Atomically publishes one completed cross-flow logical effect. Validation
  // finishes before the odd publication sequence is visible, and publication
  // itself cannot allocate, suspend or fail.
  absl::Status AdvanceBatchAfterApply(
      unsigned publisher_id, std::span<const FlowApplied> updates) noexcept;

  // Installs an exact lifecycle vector, including a reset to all ones. The
  // caller must already have withdrawn Ready and drained every apply; this is
  // the only operation allowed to lower a coordinate.
  absl::Status InstallNextLsns(
      std::span<const std::uint64_t> next_lsns) noexcept;

  // Returns a coherent vector or Unavailable after a bounded number of
  // concurrent-batch observations. A poisoned frontier fails closed. The
  // returned vector owns its storage independently from scratch reused by
  // later calls on that thread.
  absl::StatusOr<std::vector<std::uint64_t>> TrySnapshot() const;

  // Samples a UINT64_MAX-saturating total for INFO/ROLE diagnostics without
  // allocations or retries. The total may straddle batch publication or a
  // lifecycle reset; it is never a candidate, resume, or promotion proof.
  std::uint64_t ApproximateTotalNextLsn() const noexcept;

  bool poisoned() const noexcept {
    return poisoned_.load(std::memory_order_acquire);
  }

 private:
  struct alignas(64) FlowSlot {
    std::atomic<std::uint64_t> next_lsn_{1};
  };

  struct alignas(64) PublisherSlot {
    std::atomic<std::uint64_t> published_{0};
    // Single-writer state. Avoiding fetch_add keeps batch publication free of
    // a lock-prefixed RMW while published_ remains observable by snapshots.
    std::uint64_t next_sequence_ = 0;
  };

  static_assert(std::atomic<std::uint64_t>::is_always_lock_free,
                "replica Applied publication requires lock-free uint64 atomics");
  static constexpr unsigned kSnapshotAttempts = 64;

  absl::Status ValidateAdvance(unsigned flow_id,
                               std::uint64_t applied_lsn) const noexcept;
  absl::Status BeginPublication(unsigned publisher_id) noexcept;
  void StoreNextLsn(unsigned flow_id, std::uint64_t next_lsn) noexcept;
  void EndPublication(unsigned publisher_id) noexcept;

  friend class ReplicaAppliedFrontierTestPeer;

  const unsigned flow_count_;
  const unsigned publisher_count_;
  std::unique_ptr<FlowSlot[]> flows_;
  std::unique_ptr<PublisherSlot[]> publishers_;
  std::atomic<bool> poisoned_{false};
};

}  // namespace keylane::detail
