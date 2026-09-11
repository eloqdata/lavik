#pragma once

// Durable owner for the single-attempt, operator-triggered controlled
// failover workflow. The pure planner reconstructs every next effect from the
// committed operation/recovery records plus current Data observations; the
// leader-scoped service contributes only bounded disconnect revalidation and
// proposal ownership.

#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include "absl/status/statusor.h"
#include "celer/runtime/foreign_executor.h"
#include "keylane/meta/coordinator.h"
#include "keylane/meta/data_control_runtime_status.h"
#include "keylane/meta/failover.h"

namespace keylane::meta {

namespace detail {

// Missing sessions are not automatically failures: a Meta leadership change
// clears volatile runtime before Data reconnects. The leader owner sets these
// bits only after a bounded same-incarnation grace, immediately for a replaced
// boot, assignment, or replication history, or when the operation-wide attempt
// deadline expires. Exact directive failures remain durable planner inputs
// rather than volatile liveness state.
struct ControlledFailoverLiveness {
  bool former_owner_unavailable_ = false;
  bool candidate_unavailable_ = false;
  // Set from the leader owner's monotonic, non-refreshing per-operation timer.
  // It is a liveness input only; committed authority order remains the safety
  // boundary when an expiry races another proposal.
  bool attempt_deadline_expired_ = false;
};

// Phase-scoped evidence used by the leader owner to drive the bounded
// revalidation grace. A live node/boot alone is insufficient: an acknowledged
// FDS can still name the wrong term/assignment, and candidate progress can
// expire while the control session remains connected. Assignment replacement
// is separated from temporary absence because it ends the attempt
// immediately instead of consuming the reconnect grace.
struct ControlledFailoverReadiness {
  bool former_owner_ready_ = false;
  bool candidate_ready_ = false;
  bool former_owner_replaced_ = false;
  bool candidate_replaced_ = false;
};

ControlledFailoverReadiness EvaluateControlledFailoverReadiness(
    const MetaCommittedView& view, const MetaObservationStore& observations,
    const MetaDataControlRuntimeSnapshot& runtime,
    const MetaOperationRecord& operation, const FailoverIntent& intent,
    const std::optional<FailoverPhase>& phase, std::int64_t now_unix_ms);

// Conservatively downgrades an explicit recovery handoff after its exact old
// source incarnation, assignment, replication history, or held projection is
// definitively gone. This planner is group-record-owned rather than
// operation-owned so the invariant continues after terminal archival.
std::optional<SetFailoverRecovery> PlanFailoverRecoveryAvailabilityStep(
    const MetaFailoverRecoveryRecord& recovery,
    const MetaDataControlRuntimeSnapshot& runtime,
    bool former_owner_unavailable);

// Plans at most one durable effect. Terminal operations are included because
// source-hold release is intentionally asynchronous after the user-visible
// result. A missing command means wait for exact Data evidence; a non-OK
// status means committed anchors conflict and no speculative mutation is safe.
absl::StatusOr<std::optional<MetaCommand>> PlanControlledFailoverStep(
    const MetaCommittedView& view, const MetaOperationRecord& operation,
    const MetaObservationStore& observations,
    const MetaDataControlRuntimeSnapshot& runtime,
    ControlledFailoverLiveness liveness, std::int64_t now_unix_ms);

}  // namespace detail

class MetaControlledFailoverReconciler final : public MetaReconciler {
 public:
  MetaControlledFailoverReconciler(
      celer::ForeignExecutor executor,
      std::shared_ptr<MetaMembershipGate> membership_gate,
      std::shared_ptr<MetaObservationStore> observations,
      std::shared_ptr<MetaDataControlRuntimeStatus> runtime_status,
      std::uint32_t revalidation_grace_ms = 5000);
  ~MetaControlledFailoverReconciler() override;

  void Start(MetaLeaderContext& context) override;
  void CancelAndWait() override;
  // Permanent process stop; call before draining Admin/Data listeners while
  // the worker and proposal executor can still complete accepted proposals.
  void Shutdown();
  // Thread-safe ingress check; shutdown closes it before joining proposals.
  bool accepting() const;

 private:
  struct Core;
  static celer::Task<absl::Status> Run(std::shared_ptr<Core> core,
                                       MetaLeaderContext* context);
  void Stop(bool permanent);
  std::shared_ptr<Core> core_;
};

}  // namespace keylane::meta
