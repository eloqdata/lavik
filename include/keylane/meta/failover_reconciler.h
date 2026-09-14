#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>

#include "absl/status/statusor.h"
#include "celer/runtime/foreign_executor.h"
#include "keylane/meta/coordinator.h"

namespace keylane::meta {

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
};

// Leader-scoped, level-triggered owner of committed Failover Transitions.
// Every Start reconstructs work from committed state and fresh observations;
// cancellation joins only local planning/proposal work, leaving a committed
// transition for the next Meta Leader.
class MetaFailoverReconciler final : public MetaReconciler {
 public:
  MetaFailoverReconciler(celer::ForeignExecutor executor,
                         MetaFailoverReconcilerOptions options);
  ~MetaFailoverReconciler() override;

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
  static celer::Task<absl::Status> Run(std::shared_ptr<Core> core,
                                       MetaLeaderContext* context,
                                       std::int64_t leadership_started_unix_ms);
  void Stop(bool permanent);

  std::shared_ptr<Core> core_;
};

}  // namespace keylane::meta
