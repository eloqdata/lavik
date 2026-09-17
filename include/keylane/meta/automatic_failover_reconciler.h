#pragma once

// Leader-scoped adapter for automatic uncontrolled failover. It joins one
// committed Group/Policy cut with current authenticated observations, feeds
// the pure detector state machine, and submits the existing durable
// BeginUncontrolledFailover command. Timers, pending proposals, and admission
// records are deliberately process-local and are discarded at demotion.

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>

#include "absl/status/statusor.h"
#include "bycorf/runtime/foreign_executor.h"
#include "keylane/meta/automatic_failover_detector.h"
#include "keylane/meta/coordinator.h"
#include "keylane/meta/data_control_runtime_status.h"

namespace keylane::meta {

struct MetaAutomaticFailoverReconcilerOptions {
  std::shared_ptr<MetaDataControlRuntimeStatus> data_control_runtime_status_;
  std::shared_ptr<MetaAutomaticFailoverDiagnosticsRegistry> diagnostics_;
  std::uint32_t observation_ttl_ms_ = 30'000;
  // A new or newly re-eligible leader must reacquire observations for this
  // full interval before absence becomes failure evidence.
  std::uint64_t observation_grace_ms_ = 30'000;
  std::chrono::milliseconds poll_interval_{25};

  // Production uses the same steady-clock domain for Owner observation TTLs,
  // debounce, and retry backoff. Other observation-store TTLs retain their
  // existing Unix receive clock outside this reconciler.
  std::function<std::uint64_t()> now_steady_ms_;
  std::function<absl::StatusOr<MetaRequestId>()> next_id_;
};

// The automatic detector is intentionally separate from the committed
// transition executor. Register this reconciler after Data-control and before
// MetaFailoverReconciler so publisher observations exist before detection and
// a committed Begin is then consumed by the ordinary executor.
class MetaAutomaticFailoverReconciler final : public MetaReconciler {
 public:
  // Opaque shared state is exposed only so translation-unit helpers can name
  // it; callers receive no definition and cannot inspect it.
  struct Core;

  MetaAutomaticFailoverReconciler(
      bycorf::ForeignExecutor executor,
      MetaAutomaticFailoverReconcilerOptions options);
  ~MetaAutomaticFailoverReconciler() override;

  MetaAutomaticFailoverReconciler(const MetaAutomaticFailoverReconciler&) =
      delete;
  MetaAutomaticFailoverReconciler& operator=(
      const MetaAutomaticFailoverReconciler&) = delete;

  // Must be registered as a coordinator validation hook before leadership can
  // start. A first submission is revalidated against the coordinator's exact
  // pre-append committed cut; retries after an uncertain append reuse the
  // stable decision and rely on deterministic command CAS.
  MetaValidateHook validation_hook() const;

  // Starts one leadership tenure with empty local clocks and admissions.
  // Calling Start twice without a joined Cancel is a lifecycle violation.
  void Start(MetaLeaderContext& context) override;
  // Cancels and joins detector/proposal work for this tenure. Committed Begin
  // state is untouched, and a later election may call Start again.
  void CancelAndWait() override;
  // Permanently and idempotently joins the reconciler before its executor and
  // shared diagnostic/runtime registries are destroyed.
  void Shutdown();

 private:
  static bycorf::Task<absl::Status> Run(std::shared_ptr<Core> core,
                                        MetaLeaderContext* context);
  void Stop(bool permanent);

  std::shared_ptr<Core> core_;
};

}  // namespace keylane::meta
