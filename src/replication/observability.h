/* Copyright (C) 2026 EloqData Inc. SPDX-License-Identifier: Apache-2.0 */
#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <memory>
#include <vector>

#include "absl/status/status.h"
#include "lavik/replication_observability.h"

namespace lavik::detail {

ReplicationFailure ClassifyReplicationFailure(const absl::Status& status);

// Pure sampled clock: an idle interval cannot become the age of newly pending
// work. The production sampler runs once a second; tests supply their own
// clock.
struct ReplicationProgressClock {
  void Sample(bool pending, std::uint64_t progress, std::uint64_t now) {
    if (!pending || !pending_ || progress != progress_) since_ = now;
    pending_ = pending;
    progress_ = progress;
  }
  double Age(bool pending, std::uint64_t now) const {
    return pending && pending_ && now >= since_ ? (now - since_) / 1e9 : 0;
  }
  bool pending_ = false;
  std::uint64_t progress_ = 0;
  std::uint64_t since_ = 0;
};

// Plain data accessed only on the flow's owner worker, including copies made
// through SubmitTo. No allocation, clock call, or shared RMW on online events.
struct NativeFlowObservation {
  std::uint64_t session_id_ = 0;
  std::string_view history_id_;  // immutable session-owned identity
  unsigned flow_id_ = 0;
  ReplicationSide side_ = ReplicationSide::kTarget;
  std::uint64_t initial_lsn_ = 0;
  bool connected_ = false;
  bool online_ = false;
  FullSyncPhase phase_ = FullSyncPhase::kConnecting;
  std::uint64_t phase_started_ = 0;
  std::uint64_t received_ = 0;
  std::uint64_t ack_sent_ = 0;
  std::uint64_t bytes_ = 0;
  std::uint64_t events_ = 0;
  std::uint64_t full_items_ = 0;
  std::uint64_t partitions_ = 0;
  absl::Status failure_;
  FullSyncPhase failure_phase_ = FullSyncPhase::kConnecting;

  void Phase(FullSyncPhase phase, std::uint64_t now);
  std::uint64_t failure_at_ = 0;
  ReplicationFailure failure_reason_ = ReplicationFailure::kNone;
  void Fail(const absl::Status& status,
            ReplicationFailure reason = ReplicationFailure::kNone);
};

// Sampler state belongs solely to worker zero, separately from the hot flow.
struct NativeFlowSample {
  std::uint64_t applied_ = 0;
  std::array<ReplicationProgressClock, 4> clocks_{};
  std::uint64_t bytes_ = 0;
  std::uint64_t events_ = 0;
  std::uint64_t full_items_ = 0;
  std::uint64_t partitions_ = 0;
};

struct NativeSessionObservation {
  // Initialized before any flow launches. Target layout is installed by worker
  // zero; samplers retain its shared allocation across asynchronous collection.
  std::shared_ptr<std::vector<NativeFlowObservation>> flows_;
  std::vector<NativeFlowSample> samples_;  // worker zero only
  std::string
      peer_id_;  // target control owner captures accepted source identity
  std::atomic<std::uint64_t> full_started_{0};
  std::atomic<std::uint64_t> full_succeeded_{0};
  std::atomic<bool> promotion_complete_{false};
  bool result_recorded_ = false;  // worker zero only
  // Set by worker zero after a sample that began with every flow drained.
  // A session completing during collection needs one final sample before GC.
  bool drained_sampled_ = false;
  absl::Status control_failure_;  // read only after control completion
  std::atomic<bool> expected_cancel_{false};
  bool control_done_ =
      false;  // target: worker zero; source uses its existing flag

  // Exactly one attempt start, including a late FULL fallback.
  void BeginFull(std::uint64_t now, ReplicationSide side, std::uint64_t id,
                 std::string_view history);
};

}  // namespace lavik::detail
