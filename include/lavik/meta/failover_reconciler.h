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
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/statusor.h"
#include "bycorf/runtime/foreign_executor.h"
#include "lavik/meta/coordinator.h"

namespace lavik::meta {

// Leader-local elapsed-time proof that a committed grantless term can no
// longer overlap a prior finite lease. A fenced group starts the wait without
// requiring a new Owner lease challenge. Callers serialize access and reset on
// every leadership edge; wall-clock recovery deadlines never substitute for it.
class MetaFencedAuthorityGuard {
 public:
  explicit MetaFencedAuthorityGuard(std::uint64_t quarantine_ms)
      : quarantine_ms_(quarantine_ms) {}
  // Observing a different term starts a fresh wait. Missing or invalid clock
  // evidence is conservative; only an uninterrupted full elapsed window passes.
  bool ObserveFence(std::string_view group_id, std::uint64_t target_term,
                    std::int64_t now_lease_clock_ms);
  void Reset() noexcept { entries_.clear(); }

 private:
  struct Entry {
    std::string group_id_;
    std::uint64_t target_term_ = 0;
    std::int64_t since_ms_ = 0;
  };
  std::uint64_t quarantine_ms_;
  std::vector<Entry> entries_;
};

// One fixed planner instant. The clock and id source are injected so recovery
// decisions can be tested without process-local workflow state. IDs are drawn
// only when the returned step needs them; a wait decision consumes none.
struct MetaFailoverPlannerContext {
  // One wall-clock cut shared by every TTL and deadline decision in this pass.
  std::int64_t now_unix_ms_ = 0;
  // Start of the current leadership tenure. Missing observations are not
  // interpreted as failures until the bounded warmup below has elapsed.
  std::int64_t leadership_started_unix_ms_ = 0;
  // Shared bounded window for new-leader observation warmup and Source
  // disconnect/absence recovery. An explicit Candidate failure bypasses it.
  std::int64_t observation_grace_ms_ = 30'000;
  // CSPRNG-backed identity source. A waiting planner step consumes no ids.
  std::function<absl::StatusOr<MetaRequestId>()> next_id_;
  // Absent means no exclusion evidence. This gates only the first recovery
  // start; a committed cutoff already records the prior exclusion decision.
  std::function<bool(std::string_view, std::uint64_t)> authority_excluded_;
};

// Derives at most one typed failover mutation from one committed view and the
// leader-local observations current at `now_unix_ms_`. A missing command means
// wait: committed changes may wake the caller early, while periodic polling
// bounds how long fresh volatile observations wait to be re-evaluated.
// Proposal completion is never an input to this function.
absl::StatusOr<std::optional<MetaCommand>> PlanFailoverStep(
    const MetaCommittedView& view, const MetaObservationStore& observations,
    const MetaFailoverPlannerContext& context);

struct MetaFailoverReconcilerOptions {
  // Bounded observation recovery window passed to every planner cut.
  std::int64_t observation_grace_ms_ = 30'000;
  // Maximum delay before rechecking volatile observations and wall-clock
  // TTL/deadline boundaries. A committed update may wake the reconciler
  // earlier; observations themselves are consumed by the next poll.
  std::chrono::milliseconds poll_interval_{25};
  // Optional injectable wall clock and CSPRNG identity source. Production
  // defaults are installed by the constructor; tests provide deterministic
  // functions.
  std::function<std::int64_t()> now_unix_ms_;
  std::function<absl::StatusOr<MetaRequestId>()> next_id_;
  // Production wires the same maximum lease + margin as lease handoff.
  // The default remains conservative for embeddings without a tighter bound.
  std::uint64_t authority_exclusion_ms_ = 2 * kMaximumAuthorityLeaseDurationMs;
  std::function<std::int64_t()> now_lease_clock_ms_;
};

// Leader-scoped, level-triggered owner of committed Failover Transitions.
// Every Start reconstructs work from committed state and fresh observations;
// cancellation joins only local planning/proposal work, leaving a committed
// transition for the next Meta Leader.
class MetaFailoverReconciler final : public MetaReconciler {
 public:
  MetaFailoverReconciler(bycorf::ForeignExecutor executor,
                         MetaFailoverReconcilerOptions options);
  ~MetaFailoverReconciler() override;

  // Register before leadership starts. Rechecks the elapsed authority wait
  // and the latched first-proposal cutoff immediately before Raft append.
  MetaValidateHook validation_hook() const;

  // Starts one leadership tenure. Calling Start twice without a joined Cancel
  // is a lifecycle violation; committed work is always rediscovered here.
  void Start(MetaLeaderContext& context) override;
  // Cancels and joins only leader-local work. It never rolls back a committed
  // transition and may be followed by another Start after re-election.
  void CancelAndWait() override;
  // Permanently joins the reconciler. Idempotent and required before its
  // executor/runtime is destroyed.
  void Shutdown();
  // True until permanent shutdown. Observation handlers use this lifecycle
  // gate even between leadership tenures; polling, not this flag, schedules
  // the next volatile recheck.
  bool accepting() const;

 private:
  struct Core;
  static bycorf::Task<absl::Status> Run(
      std::shared_ptr<Core> core, MetaLeaderContext* context,
      std::int64_t leadership_started_unix_ms);
  void Stop(bool permanent);

  std::shared_ptr<Core> core_;
};

}  // namespace lavik::meta
