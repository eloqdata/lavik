#include "replica_applied_frontier.h"

#include <algorithm>
#include <limits>
#include <string>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"

namespace keylane::detail {

absl::StatusOr<std::vector<std::uint64_t>> InitialAppliedNextLsnsForReconnect(
    unsigned source_flow_count,
    std::span<const std::uint64_t> requested_next_lsns,
    bool exact_resume_context_matches) {
  if (source_flow_count == 0) {
    return absl::InvalidArgumentError(
        "replica reconnect source flow count must be nonzero");
  }
  if (!exact_resume_context_matches) {
    return std::vector<std::uint64_t>(source_flow_count, std::uint64_t{1});
  }
  if (requested_next_lsns.size() != source_flow_count) {
    return absl::FailedPreconditionError(
        "replica reconnect resume vector has the wrong flow count");
  }
  if (std::ranges::any_of(requested_next_lsns,
                          [](std::uint64_t lsn) { return lsn == 0; })) {
    return absl::InvalidArgumentError(
        "replica reconnect resume vector contains zero");
  }
  return std::vector<std::uint64_t>(requested_next_lsns.begin(),
                                    requested_next_lsns.end());
}

ReplicaAppliedFrontier::ReplicaAppliedFrontier(unsigned flow_count,
                                               unsigned publisher_count)
    : flow_count_(flow_count),
      publisher_count_(std::max(1u, publisher_count)),
      flows_(std::make_unique<FlowSlot[]>(flow_count_)),
      publishers_(std::make_unique<PublisherSlot[]>(publisher_count_)) {}

absl::Status ReplicaAppliedFrontier::ValidateAdvance(
    unsigned flow_id, std::uint64_t applied_lsn) const noexcept {
  if (flow_id >= flow_count_) {
    return absl::InvalidArgumentError("replica Applied flow is out of range");
  }
  if (applied_lsn == 0) {
    return absl::InvalidArgumentError("replica Applied LSN must be nonzero");
  }
  if (applied_lsn == std::numeric_limits<std::uint64_t>::max()) {
    return absl::OutOfRangeError(
        "replica Applied LSN cannot advance past UINT64_MAX");
  }
  if (poisoned()) {
    return absl::FailedPreconditionError("replica Applied frontier is poisoned");
  }
  const std::uint64_t current =
      flows_[flow_id].next_lsn_.load(std::memory_order_acquire);
  if (current == applied_lsn || current == applied_lsn + 1) {
    return absl::OkStatus();
  }
  return absl::FailedPreconditionError(absl::StrCat(
      "replica Applied LSN is gapped or stale: expected ", current, ", got ",
      applied_lsn));
}

absl::Status ReplicaAppliedFrontier::AdvanceAfterApply(
    unsigned flow_id, std::uint64_t applied_lsn) noexcept {
  absl::Status valid = ValidateAdvance(flow_id, applied_lsn);
  if (!valid.ok()) return valid;
  const std::uint64_t current =
      flows_[flow_id].next_lsn_.load(std::memory_order_acquire);
  if (current == applied_lsn + 1) return absl::OkStatus();
  StoreNextLsn(flow_id, applied_lsn + 1);
  return absl::OkStatus();
}

absl::Status ReplicaAppliedFrontier::AdvanceBatchAfterApply(
    unsigned publisher_id, std::span<const FlowApplied> updates) noexcept {
  if (updates.empty()) {
    return absl::InvalidArgumentError(
        "replica Applied batch must not be empty");
  }
  if (publisher_id >= publisher_count_) {
    return absl::InvalidArgumentError(
        "replica Applied publisher is out of range");
  }
  for (std::size_t i = 0; i < updates.size(); ++i) {
    absl::Status valid =
        ValidateAdvance(updates[i].flow_id_, updates[i].applied_lsn_);
    if (!valid.ok()) return valid;
    for (std::size_t j = 0; j < i; ++j) {
      if (updates[j].flow_id_ == updates[i].flow_id_) {
        return absl::InvalidArgumentError(
            "replica Applied batch repeats a flow");
      }
    }
  }

  absl::Status begun = BeginPublication(publisher_id);
  if (!begun.ok()) return begun;
  for (const FlowApplied& update : updates) {
    const std::uint64_t current =
        flows_[update.flow_id_].next_lsn_.load(std::memory_order_relaxed);
    if (current == update.applied_lsn_) {
      StoreNextLsn(update.flow_id_, update.applied_lsn_ + 1);
    }
  }
  EndPublication(publisher_id);
  return absl::OkStatus();
}

absl::Status ReplicaAppliedFrontier::InstallNextLsns(
    std::span<const std::uint64_t> next_lsns) noexcept {
  if (next_lsns.size() != flow_count_) {
    return absl::InvalidArgumentError(
        "replica Applied install vector has the wrong flow count");
  }
  if (std::ranges::any_of(next_lsns,
                          [](std::uint64_t lsn) { return lsn == 0; })) {
    return absl::InvalidArgumentError(
        "replica Applied install vector contains zero");
  }
  if (poisoned()) {
    return absl::FailedPreconditionError("replica Applied frontier is poisoned");
  }
  absl::Status begun = BeginPublication(0);
  if (!begun.ok()) return begun;
  for (unsigned flow = 0; flow < flow_count_; ++flow) {
    StoreNextLsn(flow, next_lsns[flow]);
  }
  EndPublication(0);
  return absl::OkStatus();
}

absl::StatusOr<std::vector<std::uint64_t>>
ReplicaAppliedFrontier::TrySnapshot() const {
  return TrySnapshot(kSnapshotAttempts);
}

absl::StatusOr<std::vector<std::uint64_t>>
ReplicaAppliedFrontier::TrySnapshotOnce() const {
  return TrySnapshot(1);
}

absl::StatusOr<std::vector<std::uint64_t>> ReplicaAppliedFrontier::TrySnapshot(
    unsigned attempts) const {
  if (poisoned()) {
    return absl::FailedPreconditionError("replica Applied frontier is poisoned");
  }
  // Sampling never suspends or invokes callbacks. Each caller thread can
  // reuse sequence scratch across frontiers without sharing mutable state
  // with another worker. Output remains independently owned for async sends.
  thread_local std::vector<std::uint64_t> before;
  before.resize(publisher_count_);
  std::vector<std::uint64_t> result;
  for (unsigned attempt = 0; attempt < attempts; ++attempt) {
    bool stable = true;
    for (unsigned publisher = 0; publisher < publisher_count_; ++publisher) {
      before[publisher] = publishers_[publisher].published_.load(
          std::memory_order_acquire);
      if ((before[publisher] & 1u) != 0) {
        stable = false;
        break;
      }
    }
    if (!stable) continue;
    // An already-busy publisher needs no output allocation or flow scan.
    result.resize(flow_count_);
    for (unsigned flow = 0; flow < flow_count_; ++flow) {
      result[flow] =
          flows_[flow].next_lsn_.load(std::memory_order_acquire);
    }
    for (unsigned publisher = 0; publisher < publisher_count_; ++publisher) {
      const std::uint64_t after = publishers_[publisher].published_.load(
          std::memory_order_acquire);
      if (after != before[publisher] || (after & 1u) != 0) {
        stable = false;
        break;
      }
    }
    if (stable && !poisoned()) return result;
  }
  return absl::UnavailableError(
      "replica Applied frontier remained busy during snapshot");
}

std::uint64_t ReplicaAppliedFrontier::ApproximateTotalNextLsn() const noexcept {
  std::uint64_t total = 0;
  for (unsigned flow = 0; flow < flow_count_; ++flow) {
    // This scalar publishes no storage visibility or cross-flow proof, so
    // per-cell atomicity is sufficient even while a batch sequence is odd.
    const std::uint64_t next_lsn =
        flows_[flow].next_lsn_.load(std::memory_order_relaxed);
    if (next_lsn > std::numeric_limits<std::uint64_t>::max() - total) {
      return std::numeric_limits<std::uint64_t>::max();
    }
    total += next_lsn;
  }
  return total;
}

absl::Status ReplicaAppliedFrontier::BeginPublication(
    unsigned publisher_id) noexcept {
  if (publisher_id >= publisher_count_) {
    return absl::InvalidArgumentError(
        "replica Applied publisher is out of range");
  }
  PublisherSlot& publisher = publishers_[publisher_id];
  if (publisher.next_sequence_ >
      std::numeric_limits<std::uint64_t>::max() - 2) {
    poisoned_.store(true, std::memory_order_release);
    return absl::ResourceExhaustedError(
        "replica Applied publication sequence is exhausted");
  }
  ++publisher.next_sequence_;
  publisher.published_.store(publisher.next_sequence_,
                             std::memory_order_release);
  return absl::OkStatus();
}

void ReplicaAppliedFrontier::StoreNextLsn(unsigned flow_id,
                                          std::uint64_t next_lsn) noexcept {
  flows_[flow_id].next_lsn_.store(next_lsn, std::memory_order_release);
}

void ReplicaAppliedFrontier::EndPublication(unsigned publisher_id) noexcept {
  PublisherSlot& publisher = publishers_[publisher_id];
  ++publisher.next_sequence_;
  publisher.published_.store(publisher.next_sequence_,
                             std::memory_order_release);
}

}  // namespace keylane::detail
