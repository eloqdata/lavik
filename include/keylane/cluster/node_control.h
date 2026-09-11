#pragma once

// Data-side control installation seam. Protocol clients and the static-file
// adapter submit complete domain messages here; only this module publishes a
// ServingState, changes live authority, revokes source capabilities, or
// dispatches storage-mutating directives.

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "celer/runtime/task.h"
#include "keylane/cluster/authority.h"
#include "keylane/cluster/topology.h"

namespace keylane::cluster {

struct PreparedMemberAssignment {
  NodeId node_id_;
  AssignmentId assignment_id_;

  friend bool operator==(const PreparedMemberAssignment&,
                         const PreparedMemberAssignment&) = default;
};

// Member-specific identities retained beside the routing-optimized
// ServingState. GroupView carries the current serving owner's assignment;
// rebuild and readiness proofs instead name the target member's assignment.
struct PreparedGroupControlIdentity {
  std::string group_id_;
  std::uint64_t group_term_ = 0;
  std::uint64_t authority_version_ = 0;
  std::uint64_t grant_revision_ = 0;
  std::uint64_t config_epoch_ = 0;
  std::uint64_t manifest_revision_ = 0;
  Sha256Digest manifest_digest_{};
  std::uint64_t partition_replication_epoch_ = 0;
  std::vector<PreparedMemberAssignment> members_;

  friend bool operator==(const PreparedGroupControlIdentity&,
                         const PreparedGroupControlIdentity&) = default;
};

struct PreparedFullState {
  std::shared_ptr<const ServingState> serving_state_;
  // SHA-256 of the complete wire object, including its diagnostic source
  // index. This detects same-index equivocation independently of the semantic
  // projection hash.
  Sha256Digest object_hash_{};
  // Exact member incarnations and manifest binding from the same decoded FDS.
  // Static topology leaves this empty because it has no Meta directives or
  // boot-local population proof to validate.
  std::vector<PreparedGroupControlIdentity> control_groups_;
};

struct AuthorityMessage {
  enum class Kind : std::uint8_t { kLeaseGrant, kFence };

  Kind kind_ = Kind::kLeaseGrant;
  SessionIdentity session_;
  ProjectionBasis projection_;
  AuthorityAnchor anchor_;
  // Lease expiry is derived from challenge send time, never receive time.
  MonotonicTime sent_at_;
  MonotonicDuration granted_duration_{};
};

struct NodeManifestEntry {
  std::uint32_t partition_id_ = 0;
  std::uint64_t logical_epoch_ = 0;

  friend bool operator==(const NodeManifestEntry&,
                         const NodeManifestEntry&) = default;
};

// Boot-local proof identity reported by ReplicationManager. Absence means the
// local population is not currently safe to serve; presence can make only the
// exactly matching assignment ready.
struct PopulationReadiness {
  std::string group_id_;
  AssignmentId assignment_id_;
  std::uint64_t group_term_ = 0;
  std::uint64_t manifest_revision_ = 0;
  Sha256Digest manifest_digest_{};
  std::uint64_t partition_replication_epoch_ = 0;

  friend bool operator==(const PopulationReadiness&,
                         const PopulationReadiness&) = default;
};

// Fully normalized execution request. Transport clients resolve the source
// endpoint and referenced manifest from the same FullDesiredState that
// supplied `projection_`; the action adapter therefore never looks through a
// second mutable topology/control cache while starting destructive work.
struct NodeDirective {
  enum class Kind : std::uint8_t {
    kReplication,
    kAuthorizeSource,
    kRevokeSources,
    kInitializeEmptyPopulation,
  };

  ProjectionBasis projection_;
  AuthorityAnchor anchor_;
  OperationId operation_id_;
  DirectiveId directive_id_;
  AttemptId attempt_id_;
  std::uint64_t directive_revision_ = 0;
  Kind kind_ = Kind::kReplication;
  NodeId target_node_id_;
  NodeId target_boot_id_;
  NodeId source_node_id_;
  AssignmentId source_assignment_id_;
  NodeId source_boot_id_;
  NodeId source_replication_history_id_;
  std::string source_host_;
  std::uint16_t source_port_ = 0;
  std::uint32_t flow_count_ = 0;
  std::uint64_t manifest_revision_ = 0;
  Sha256Digest manifest_digest_{};
  std::uint64_t partition_replication_epoch_ = 0;
  std::vector<NodeManifestEntry> manifest_entries_;
  // V1 uses payload only to bind empty-population initialization to the
  // authenticated target history id; preconditions remain reserved.
  std::string payload_;
  std::string preconditions_;
  // Active V1 classification: population directives set this. It drives
  // non-serving-target admission and cross-operation mutation exclusion.
  bool storage_mutating_ = false;
  // Reserved in V1 and rejected when true before entering NodeControlActions.
  bool force_ = false;

  friend bool operator==(const NodeDirective&, const NodeDirective&) = default;
};

// Pollable terminal outcome for one exact directive attempt. Starting and
// observing are deliberately separate: a later FDS directive must be able to
// enter ReplicationManager and supersede an in-progress population mutation,
// while the original wire identity continues to own its terminal result.
class NodeDirectiveCompletion {
 public:
  using Poll = std::function<std::optional<absl::Status>()>;

  NodeDirectiveCompletion() = default;
  // A directly constructed completion represents work that crossed the
  // NodeControl admission boundary. Tests and native adapters use this form
  // for deferred execution; validation failures must use Rejected().
  explicit NodeDirectiveCompletion(Poll poll)
      : poll_(std::move(poll)), started_(true) {}

  // Constructs a terminal controller/admission rejection. `result` must be a
  // failure; an accidental success is converted to an internal error.
  static NodeDirectiveCompletion Rejected(absl::Status result);
  // Constructs work that started and reached a terminal result before its
  // completion handle was returned.
  static NodeDirectiveCompletion StartedTerminal(absl::Status result);

  bool valid() const noexcept { return static_cast<bool>(poll_); }
  bool started() const noexcept { return started_; }
  std::optional<absl::Status> result() const;
  celer::Task<absl::Status> Await() const;

 private:
  NodeDirectiveCompletion(Poll poll, bool started)
      : poll_(std::move(poll)), started_(started) {}

  Poll poll_;
  bool started_ = false;
};

// Narrow internal seam implemented by the ReplicationManager adapter in the
// server. Tests use a recording adapter; static-only deployments use the
// no-op adapter because they receive no Meta directives or source grants.
// The two revocation entry points are intentional: static topology reload is
// a non-suspending signal path and can use only the synchronous operation,
// while Meta transitions must await ReplicationManager cleanup. A dynamic
// adapter fails the synchronous entry rather than detach cleanup and report a
// false success.
class NodeControlActions {
 public:
  virtual ~NodeControlActions() = default;
  // False only for a static adapter that can never receive or retain target
  // population work. This permits its non-suspending readiness reload path;
  // directive-capable adapters must use the runtime storage-loss barrier.
  virtual bool ReceivesDirectives() const noexcept { return true; }
  virtual absl::Status RevokeSourceAuthorizations() = 0;
  // Dynamic Meta transitions await this operation before acknowledging the
  // transition. The default preserves the synchronous static/test adapter;
  // adapters backed by asynchronous subsystems must override it rather than
  // detach work and report completion early.
  virtual celer::Task<absl::Status> RevokeSourceAuthorizationsAndWait();
  // Session replacement closes live source exports without advancing the
  // committed directive/fence floor, allowing the new authenticated session
  // to replay the exact current authorize directive from its FDS.
  virtual celer::Task<absl::Status>
  ClearSourceAuthorizationsForSessionReplacementAndWait();
  // Retires an in-progress or ready target population unless it still names
  // the desired local assignment, term, manifest, and partition replication
  // epoch. Completion includes
  // native-flow join and partial-root abort, making FullStateApplied a real
  // population invalidation barrier.
  virtual celer::Task<absl::Status> ReconcilePopulation(
      std::optional<PopulationReadiness> desired,
      bool population_transition_expected);
  // Session loss cancels a destructive attempt whose terminal result is no
  // longer observable on that wire, but preserves an already Ready population
  // for an equal FDS on reconnect.
  virtual celer::Task<absl::Status> CancelInProgressPopulation();
  // Graceful process shutdown must resolve even an attempt whose directive
  // executor is waiting for terminal native cleanup. ReplicationManager uses
  // its stronger shutdown cancellation; other adapters may reuse ordinary
  // in-progress cancellation.
  virtual celer::Task<absl::Status> CancelPopulationForShutdown();
  // The default adapts actions whose admission and completion are one short
  // operation. ReplicationManager overrides this for population mutations so
  // admission returns a pollable exact-attempt completion without awaiting
  // readiness.
  virtual celer::Task<NodeDirectiveCompletion> StartDirective(
      NodeDirective directive);
  // Non-suspending, non-mutating lookup: only an exact, still-valid completed
  // population may return its original completion. A miss falls through to
  // normal mutation admission; implementations must never initiate a reset.
  virtual std::optional<NodeDirectiveCompletion> FindCompletedPopulation(
      const NodeDirective& /*directive*/) const {
    return std::nullopt;
  }
  virtual celer::Task<absl::Status> ApplyDirective(NodeDirective directive) = 0;
  // Optional post-counter hook for adapters that retain assignment-scoped
  // state outside ServingState. ReplicationManager currently needs no extra
  // work here because its source sessions were joined by async revocation and
  // NodeControlInstaller owns the replaced snapshot's request counters.
  virtual absl::Status DrainAssignment(const AuthorityAnchor& anchor) = 0;
};

class NullNodeControlActions final : public NodeControlActions {
 public:
  bool ReceivesDirectives() const noexcept override { return false; }
  absl::Status RevokeSourceAuthorizations() override;
  celer::Task<absl::Status> ApplyDirective(NodeDirective directive) override;
  absl::Status DrainAssignment(const AuthorityAnchor& anchor) override;
};

class NodeControlInstaller {
 public:
  // Lease grants create worker-owned expiration tasks that retain this
  // installer's references. The owner must destroy the Celer worker (and thus
  // its detached tasks) before destroying the installer; the destructor
  // asserts that contract in debug builds. ClusterRuntime satisfies it by
  // remaining installed until Server::WaitUntilStopped returns.
  NodeControlInstaller(TopologyCache& topology, AuthorityGuard& authority,
                       NodeControlActions& actions);
  ~NodeControlInstaller();
  NodeControlInstaller(const NodeControlInstaller&) = delete;
  NodeControlInstaller& operator=(const NodeControlInstaller&) = delete;

  // Installs one completely decoded and validated snapshot through a static
  // adapter that never receives directives. Directive-capable adapters must
  // use InstallFullStateTransition(), even when a particular snapshot appears
  // to require no cleanup: concurrent admission is what makes the synchronous
  // path unsafe. The adapter owns serialization: Meta transitions run on
  // worker 0, while static startup/reload holds StaticClusterControl's refresh
  // mutex across this call and worker-0 readiness changes. Lower source
  // indices and same-assignment counter regressions fail closed; same
  // index/hash replay is idempotent.
  absl::Status InstallFullState(PreparedFullState prepared_state,
                                ProjectionBasis projection_basis);

  // Applies a live lease at the caller's suspend-aware clock cut, or a
  // committed fence only for a static adapter that never receives directives.
  // Requiring `now` prevents a delayed caller from reviving an already-expired
  // same-anchor lease. ReplicationManager-backed Meta grants/fences use the
  // asynchronous transitions below so cleanup is joined. In every case the
  // projection and complete authority anchor must still match installed state.
  absl::Status ApplyAuthority(const AuthorityMessage& authority_message,
                              MonotonicTime now);

  // Meta-only grant boundary. Installs the grant and schedules expiry with
  // bounded relative waits that repeatedly check its suspend-aware deadline.
  // Admission and renewal also compare that clock at their own cut, so a
  // delayed worker timer cannot revive an expired lease. Expiry invalidates
  // only that lease instance and joins source-authorization revocation before
  // completing.
  celer::Task<absl::Status> ApplyLeaseGrantTransition(
      const AuthorityMessage& authority_message);

  // Meta-only FDS boundary. In addition to installing the immutable state,
  // this invalidates and joins older directive admissions, then joins any
  // source authorizations inherited from the prior session. Authority-changing
  // snapshots also wait for mutations admitted through the replaced
  // ServingState before returning to the wire client.
  celer::Task<absl::Status> InstallFullStateTransition(
      PreparedFullState prepared_state, ProjectionBasis projection_basis,
      bool local_population_transition_expected = false);

  // Meta-only fence barrier. New authority and directive admission are blocked
  // synchronously; success is returned only after earlier action registration,
  // final population cancellation, source capability cleanup, and mutations
  // admitted through every retired snapshot for the group have drained.
  celer::Task<absl::Status> ApplyFenceTransition(
      const AuthorityMessage& authority_message);

  // Validates projection and authority before handing a storage mutation to
  // the ReplicationManager adapter. The admission token spans every await and
  // action registration so an invalidating control transition can join it.
  celer::Task<NodeDirectiveCompletion> StartDirective(NodeDirective directive);
  celer::Task<absl::Status> ApplyDirective(NodeDirective directive);

  // Synchronous first half of session loss. Call as soon as transport loss is
  // known, before awaiting control tasks, so their cleanup cannot extend the
  // old session's write lease.
  absl::Status InvalidateSessionNow(const SessionIdentity& session_identity);

  // Graceful-shutdown pre-join barrier. It closes all memory authority and
  // directive admission synchronously, joins registration already crossing
  // that boundary, then asks the action adapter to resolve every target
  // population (including Ready online tails). The control client may join
  // directive executors only after this returns. It is also safe when Stop
  // arrives between sessions and there is no current SessionIdentity.
  celer::Task<absl::Status> CancelPopulationForShutdownTransition();

  // Synchronous static/test session-loss path. Memory authority is invalidated
  // even when a directive-capable adapter rejects the remaining cleanup; Meta
  // callers then use LoseSessionTransition(). It intentionally does not
  // synthesize or publish a fenced topology, because Meta remains the sole
  // committed source of topology truth.
  absl::Status LoseSession(const SessionIdentity& session_identity,
                           std::string_view reason);

  // Records every current local assignment as draining before awaiting
  // source-capability cleanup. A replacement session can install an FDS, but
  // lease, rebuild, and source authorization remain blocked by those drains.
  celer::Task<absl::Status> LoseSessionTransition(
      const SessionIdentity& session_identity, std::string_view reason);

  // Publishes the current ReplicationManager proof into ServingState. Losing
  // a previously ready proof invalidates leases synchronously and joins source
  // revocation before returning.
  celer::Task<absl::Status> SetPopulationReadinessTransition(
      std::optional<PopulationReadiness> readiness);

  // Idempotent async barrier used after a concurrently executing directive is
  // joined, ensuring it could not resurrect a capability behind a fence or
  // local proof loss.
  celer::Task<absl::Status> RevokeSourceAuthorizationsTransition();

  // Publishes startup/static local storage readiness together with the current
  // committed topology. Runtime true-to-false transitions must use
  // LoseStorageReadinessTransition(), whose cancellation work can suspend.
  // A process whose storage has failed cannot become ready again before
  // restart.
  absl::Status SetStorageReady(bool ready);

  // Permanently fences this boot after runtime storage loss. Authority and
  // directive admission close before storage-unready is published; completion
  // waits for older action registration, source revocation, target population
  // cancellation, and request drains. Repeating a successfully completed loss
  // is a no-op; an uncertain cleanup failure remains the result for this boot.
  celer::Task<absl::Status> LoseStorageReadinessTransition();

  const std::optional<ProjectionBasis>& projection_basis() const noexcept {
    return projection_basis_;
  }

  // Process-local storage readiness is meaningful even before Meta assigns a
  // group. Heartbeat health must not infer it from an empty group set, where
  // an all-of check would be vacuously true.
  bool storage_ready() const noexcept { return storage_ready_; }

 private:
  class ControlTransitionGuard {
   public:
    explicit ControlTransitionGuard(NodeControlInstaller& owner,
                                    bool active = true);
    ~ControlTransitionGuard();
    ControlTransitionGuard(const ControlTransitionGuard&) = delete;
    ControlTransitionGuard& operator=(const ControlTransitionGuard&) = delete;

    std::uint64_t id() const noexcept { return id_; }

   private:
    NodeControlInstaller* owner_ = nullptr;
    std::uint64_t id_ = 0;
  };

  struct RejectThrough {
    AssignmentId assignment_id_;
    std::uint64_t group_term_ = 0;
    std::uint64_t authority_version_ = 0;
    std::uint64_t grant_revision_ = 0;
  };

  struct PendingDrain {
    std::shared_ptr<const ServingState> state_;
    std::string group_id_;
    AuthorityAnchor retired_anchor_;
  };

  struct FullStateEffects {
    bool revoke_sources_ = false;
    std::vector<AuthorityAnchor> retired_;
  };

  struct LeaseExpirySchedule {
    SessionIdentity session_;
    AuthorityAnchor anchor_;
    MonotonicTime deadline_;
    MonotonicDuration recheck_interval_;
    std::uint64_t timer_generation_ = 1;
    bool active_ = true;
  };

  // A token is held only by one worker-owned timer frame. Weak references let
  // the destructor verify that every such frame was reclaimed without making
  // the installer own or attempt to synchronously cancel worker-affine work.
  struct LeaseTimerLifetime {};

  absl::Status ValidateProjection(const ProjectionBasis& basis) const;
  absl::Status ValidateAnchor(const AuthorityAnchor& anchor,
                              bool require_local_owner) const;
  absl::Status ValidateDirectiveAnchor(const NodeDirective& directive) const;
  absl::Status ValidateDirectiveForStart(const NodeDirective& directive,
                                         bool replay_lookup = false);
  absl::StatusOr<std::optional<PopulationReadiness>> DesiredLocalPopulation()
      const;
  const PreparedGroupControlIdentity* FindControlGroup(
      std::string_view group_id) const;
  const PreparedMemberAssignment* FindMemberAssignment(
      const PreparedGroupControlIdentity& group, const NodeId& node_id) const;
  bool RejectedByFence(const AuthorityAnchor& anchor) const;
  bool DrainPending(std::string_view group_id);
  // Invalidating transitions advance the generation before publishing their
  // new boundary, then wait for every earlier admission to either reject on
  // revalidation or finish registering with NodeControlActions. The final
  // revoke/cancel pass can therefore observe all work that crossed the seam.
  void InvalidateDirectiveAdmissions();
  celer::Task<absl::Status> WaitForDirectiveAdmissions();
  // Storage loss is terminal for the boot. It waits only for transitions that
  // were already active at its cut, then performs one final cleanup pass; work
  // admitted after the cut is already constrained by storage_failed_.
  celer::Task<absl::Status> WaitForControlTransitionsBefore(
      std::uint64_t transition_id);
  celer::Task<absl::Status> SetPopulationReadinessTransitionImpl(
      std::optional<PopulationReadiness> readiness,
      bool invalidate_directive_admissions);
  celer::Task<absl::Status> WaitForPendingDrains(
      std::span<const AuthorityAnchor> anchors);
  celer::Task<absl::Status> ExpireLeaseAt(
      std::shared_ptr<LeaseExpirySchedule> schedule,
      std::uint64_t timer_generation,
      std::shared_ptr<const LeaseTimerLifetime> lifetime);
  // Completes one exact due schedule before any replacement grant is installed.
  // It invalidates the old lease generation synchronously, retires the timer,
  // and joins source/directive cleanup; callers remain fail-closed on failure.
  celer::Task<absl::Status> FinishExpiredLeaseTransition(
      std::shared_ptr<LeaseExpirySchedule> schedule, MonotonicTime now);
  void RememberDrain(std::shared_ptr<const ServingState> state,
                     const AuthorityAnchor& anchor);
  std::vector<AuthorityAnchor> RememberCurrentLocalDrains();
  absl::Status InstallFullStateLocal(PreparedFullState prepared_state,
                                     ProjectionBasis projection_basis,
                                     FullStateEffects* effects);
  absl::StatusOr<bool> ApplyFenceLocal(
      const AuthorityMessage& authority_message);
  std::shared_ptr<const ServingState> WithStorageReady(
      const ServingState& state, bool ready) const;
  std::shared_ptr<const ServingState> WithFence(
      const ServingState& state, const AuthorityAnchor& anchor) const;
  std::shared_ptr<const ServingState> WithPopulationReadiness(
      const ServingState& state,
      const std::optional<PopulationReadiness>& readiness) const;

  TopologyCache& topology_;
  AuthorityGuard& authority_;
  NodeControlActions& actions_;
  bool storage_ready_ = false;
  // Unlike the normal startup-not-ready state, a runtime storage failure is
  // irreversible for this boot. Keeping the two facts separate prevents a
  // later FDS or directive from treating failure as rebuildable readiness.
  bool storage_failed_ = false;
  // Empty while the first storage-loss coroutine is still joining cleanup;
  // afterwards it preserves success or the exact uncertain failure so a
  // repeated call can never manufacture a successful barrier.
  std::optional<absl::Status> storage_loss_result_;
  std::optional<ProjectionBasis> projection_basis_;
  std::optional<Sha256Digest> object_hash_;
  std::vector<PreparedGroupControlIdentity> control_groups_;
  std::unordered_map<std::string, RejectThrough> reject_through_;
  std::vector<PendingDrain> pending_drains_;
  std::unordered_map<std::string, std::shared_ptr<LeaseExpirySchedule>>
      lease_expiry_schedules_;
  std::vector<std::weak_ptr<const LeaseTimerLifetime>> lease_timer_lifetimes_;
  std::uint64_t directive_admission_generation_ = 1;
  std::uint64_t directive_admissions_in_flight_ = 0;
  // Awaited source revocation is a global ReplicationManager transition. No
  // new lease or directive may overtake it and then be cleared by the older
  // coroutine when that join completes.
  unsigned source_revocation_transitions_ = 0;
  std::uint64_t next_control_transition_id_ = 0;
  std::set<std::uint64_t> active_control_transitions_;
};

}  // namespace keylane::cluster
