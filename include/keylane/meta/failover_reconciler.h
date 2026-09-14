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
  std::int64_t now_unix_ms_ = 0;
  std::int64_t leadership_started_unix_ms_ = 0;
  std::int64_t observation_grace_ms_ = 30'000;
  std::function<absl::StatusOr<MetaRequestId>()> next_id_;
};

// Derives at most one typed failover mutation from one committed view and the
// leader-local observations current at `now_unix_ms_`. A missing command means
// wait: the caller must wake on commits and periodically re-evaluate volatile
// observations. Proposal completion is never an input to this function.
absl::StatusOr<std::optional<MetaCommand>> PlanFailoverStep(
    const MetaCommittedView& view, const MetaObservationStore& observations,
    const MetaFailoverPlannerContext& context);

struct MetaFailoverReconcilerOptions {
  std::int64_t observation_grace_ms_ = 30'000;
  std::chrono::milliseconds poll_interval_{25};
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

  void Start(MetaLeaderContext& context) override;
  void CancelAndWait() override;
  void Shutdown();
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
