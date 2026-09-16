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

// Data-side control installation seam. The Meta protocol client submits
// complete domain messages here; only this module publishes a ServingState,
// changes live authority, revokes source capabilities, or dispatches
// storage-mutating directives.

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
#include "bycorf/runtime/task.h"
#include "lavik/cluster/authority.h"
#include "lavik/cluster/topology.h"
#include "lavik/replication_group.h"

namespace lavik::cluster {

struct PreparedMemberAssignment {
  NodeId node_id_;
  AssignmentId assignment_id_;

  friend bool operator==(const PreparedMemberAssignment&,
                         const PreparedMemberAssignment&) = default;
};

// Complete committed client endpoint for a Group Owner. Keeping both ports
// lets the process-local replication policy choose plain TCP or TLS without
// leaking that runtime choice into Meta's committed desired state.
struct PreparedReplicationEndpoint {
  NodeId node_id_;
  std::string host_;
  std::uint16_t port_ = 0;
  std::uint16_t tls_port_ = 0;

  friend bool operator==(const PreparedReplicationEndpoint&,
                         const PreparedReplicationEndpoint&) = default;
};

// Member-specific identities retained beside the routing-optimized
// ServingState. GroupView carries the current serving owner's assignment;
// rebuild and readiness proofs instead name the target member's assignment.
struct PreparedGroupControlIdentity {
  std::string group_id_;
  std::uint64_t group_term_ = 0;
  std::uint64_t manifest_revision_ = 0;
  Sha256Digest manifest_digest_{};
  std::uint64_t partition_replication_epoch_ = 0;
  std::vector<PreparedMemberAssignment> members_;

  friend bool operator==(const PreparedGroupControlIdentity&,
                         const PreparedGroupControlIdentity&) = default;
};

struct FailoverTransitionIdTag;
struct FailoverActionIdTag;
// Opaque identities keep a durable transition distinct from each replaceable
// candidate action while preserving the exact 128-bit Meta representation.
using FailoverTransitionId = ControlId128<FailoverTransitionIdTag>;
using FailoverActionId = ControlId128<FailoverActionIdTag>;

// Execution semantics projected from the committed transition. Controlled
// actions preserve the current Owner's authority until Cutover; Uncontrolled
// actions run after that authority has been fenced.
enum class PreparedFailoverMode : std::uint8_t {
  kControlled,
  kUncontrolled,
};

// Operator-visible durability guarantee attached to an authorized promotion.
// Unknown permits recovery from the best available replica without asserting
// that it contains every acknowledged write.
enum class PreparedFailoverLoss : std::uint8_t {
  kNone,
  kUnknown,
};

// Exact process incarnation selected for promotion. All three identities must
// still match before NodeControl may install or activate the action.
struct PreparedFailoverCandidate {
  NodeId node_id_;
  AssignmentId assignment_id_;
  NodeId boot_id_;

  friend bool operator==(const PreparedFailoverCandidate&,
                         const PreparedFailoverCandidate&) = default;
};

// Source lineage in which candidate progress is comparable. LSN vectors from
// different domains must never be ranked against one another; flow_count also
// fixes the vector shape expected by the promotion adapter.
struct PreparedFailoverCompatibilityDomain {
  std::uint64_t source_group_term_ = 0;
  NodeId source_node_id_;
  AssignmentId source_assignment_id_;
  NodeId source_boot_id_;
  NodeId source_history_id_;
  std::uint32_t flow_count_ = 0;

  friend bool operator==(const PreparedFailoverCompatibilityDomain&,
                         const PreparedFailoverCompatibilityDomain&) = default;
};

// Revision-scoped permission to begin promotion preparation, including the
// loss classification that a Cutover using this authorization must report.
struct PreparedFailoverAuthorization {
  std::uint64_t authorized_revision_ = 0;
  PreparedFailoverLoss loss_if_cutover_ = PreparedFailoverLoss::kUnknown;

  friend bool operator==(const PreparedFailoverAuthorization&,
                         const PreparedFailoverAuthorization&) = default;
};

// One replaceable candidate attempt within a durable transition. Absence of
// authorization keeps the selected candidate inert while Meta gathers the
// observations needed to authorize preparation.
struct PreparedFailoverAction {
  FailoverActionId action_id_;
  PreparedFailoverCandidate candidate_;
  PreparedFailoverCompatibilityDomain domain_;
  std::optional<PreparedFailoverAuthorization> authorization_;

  bool operator_recovery_ = false;

  friend bool operator==(const PreparedFailoverAction&,
                         const PreparedFailoverAction&) = default;
};

// Wire-independent committed execution subset. Operator/deadline workflow data
// does not drive Data behavior and intentionally does not cross this seam.
struct PreparedFailoverTransition {
  FailoverTransitionId transition_id_;
  std::uint64_t revision_ = 0;
  PreparedFailoverMode mode_ = PreparedFailoverMode::kUncontrolled;
  std::uint64_t target_term_ = 0;
  std::optional<PreparedFailoverAction> candidate_action_;

  friend bool operator==(const PreparedFailoverTransition&,
                         const PreparedFailoverTransition&) = default;
};

// Complete, normalized desired control for one Group. It is kept separate
// from ServingState because a fenced Group still has an Owner and useful
// replication relationships even though it has no routable serving slots.
struct DesiredClusterControl {
  PreparedGroupControlIdentity identity_;
  std::optional<PreparedMemberAssignment> owner_;
  bool grant_active_ = false;
  std::optional<FailoverActionId> activation_action_id_;
  std::optional<PreparedFailoverTransition> failover_transition_;
  // These are derived from the same complete local control as identity_. They
  // make the post-Cutover Follow Owner relationship self-contained at the one
  // NodeControlActions seam; no adapter may consult a second topology view.
  std::optional<PreparedReplicationEndpoint> owner_endpoint_;
  std::vector<PopulationManifestEntry> manifest_entries_;
  // Committed lifecycle gate projected in the local control. False keeps
  // Genesis on its explicit population workflow instead of starting a second
  // ingress.
  bool steady_replication_enabled_ = false;
  // Derived from current_directives in the same local control. While true, the
  // exact population directive owns target ingress; ordinary Follow Owner is
  // held until a later complete local control removes the directive.
  bool population_transition_expected_ = false;

  friend bool operator==(const DesiredClusterControl&,
                         const DesiredClusterControl&) = default;
};

// Exact, wire-independent promotion activation derived from a
// failover-installed current Grant and the control session that delivered it.
// Replication owns the boot-local prepared context; NodeControl owns the
// ordering that keeps this action provisional until finite expiration and
// request authority are installed against the same local control.
struct PreparedFailoverActivation {
  FailoverActionId action_id_;
  std::string group_id_;
  NodeId candidate_node_id_;
  AssignmentId candidate_assignment_id_;
  NodeId candidate_boot_id_;
  std::uint64_t target_term_ = 0;
  std::uint64_t manifest_revision_ = 0;
  Sha256Digest manifest_digest_{};
  std::uint64_t partition_replication_epoch_ = 0;

  friend bool operator==(const PreparedFailoverActivation&,
                         const PreparedFailoverActivation&) = default;
};

// Stable, local control-derived portion of an already authenticated source
// export. Source boot/history remain owned by the native live session: they are
// Observation, so committed local control must neither invent nor replace them.
// Within one Data boot this scope decides whether that live session may be
// retained.
struct EstablishedExportScope {
  std::string group_id_;
  PreparedMemberAssignment source_;
  std::uint64_t manifest_revision_ = 0;
  Sha256Digest manifest_digest_{};
  std::uint64_t partition_replication_epoch_ = 0;
  std::vector<PreparedMemberAssignment> downstream_members_;

  friend bool operator==(const EstablishedExportScope&,
                         const EstablishedExportScope&) = default;
};

// Authority counters, lease state, and failover control are deliberately not
// export identity. Changing only those values must close new admission without
// tearing down an already ONLINE compatible data flow.
bool SameEstablishedExportScope(const DesiredClusterControl& left,
                                const DesiredClusterControl& right);

struct PreparedFullState {
  std::shared_ptr<const ServingState> serving_state_;
  // Exact effective duration for every Authority Lease issued against this
  // local control. Policy identity and Meta-local leadership limits stay
  // outside Data.
  std::uint32_t authority_lease_duration_ms_ = 0;
  // Exact member incarnations and manifest binding from the same decoded local
  // control. Test-only projections may leave this empty when they exercise
  // routing and authority without directives or boot-local population proof.
  // Full level-triggered execution intent is deliberately separate from the
  // stable population identity above. Dynamic Meta projections populate this;
  // static topology leaves it empty.
  std::vector<DesiredClusterControl> desired_cluster_controls_;
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
  // Source boot/history layout decoded from committed rebuild intent. This
  // is independent of the receiving node's local worker count.
  std::uint32_t flow_count_ = 0;
  std::uint64_t manifest_revision_ = 0;
  Sha256Digest manifest_digest_{};
  std::uint64_t partition_replication_epoch_ = 0;
  std::vector<NodeManifestEntry> manifest_entries_;
  // Rebuild/authorize-source payloads are decoded into flow_count_ above.
  // Empty-population initialization retains its authenticated target history
  // in payload_; other V1 kinds require it to be empty.
  std::string payload_;

  friend bool operator==(const NodeDirective&, const NodeDirective&) = default;
};

// Pollable terminal outcome for one exact directive attempt. Starting and
// observing are deliberately separate: a later local control directive must be
// able to enter ReplicationManager and supersede an in-progress population
// mutation, while the original wire identity continues to own its terminal
// result.
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
  bycorf::Task<absl::Status> Await() const;

 private:
  NodeDirectiveCompletion(Poll poll, bool started)
      : poll_(std::move(poll)), started_(started) {}

  Poll poll_;
  bool started_ = false;
};

// Narrow internal seam implemented by the ReplicationManager adapter in the
// server. Tests may use a recording or no-op adapter. Meta transitions await
// ReplicationManager cleanup; a dynamic adapter fails the synchronous entry
// rather than detach cleanup and report a false success.
class NodeControlActions {
 public:
  virtual ~NodeControlActions() = default;
  // False only for a test adapter that can never receive or retain target
  // population work. Directive-capable adapters must use the runtime
  // storage-loss barrier.
  virtual bool ReceivesDirectives() const noexcept { return true; }
  virtual absl::Status RevokeSourceAuthorizations() = 0;
  // Dynamic Meta transitions await this operation before acknowledging the
  // transition. The default preserves the synchronous test adapter;
  // adapters backed by asynchronous subsystems must override it rather than
  // detach work and report completion early.
  virtual bycorf::Task<absl::Status> RevokeSourceAuthorizationsAndWait();
  // Clears source admission without advancing the committed directive/fence
  // floor. A control-session replacement may preserve already-online population
  // exports while write authority is invalid and the installed topology and
  // authority are unchanged. New handshakes remain closed until an
  // authenticated replacement projection replays their admission.
  virtual bycorf::Task<absl::Status>
  ClearSourceAuthorizationsForSessionReplacementAndWait(
      bool preserve_established_exports = false);
  // A live local control replacement replays source capabilities while an
  // unchanged finite lease may remain active. Unlike session replacement, this
  // clears capabilities without closing that lease-admission gate and may
  // retain all already-published POPULATION sessions when the exact export
  // scope survives the replacement; those sessions need not have reached ONLINE
  // yet. The expected replay count keeps the intervening admission gap
  // retryable.
  virtual bycorf::Task<absl::Status>
  RefreshSourceAuthorizationsForFdsReplacementAndWait(
      bool preserve_current_population_exports = false,
      std::size_t expected_authorization_replays = 0);
  // Converges all failover and steady-state replication capabilities for the
  // one Group assigned to this Data process. Replacement/removal is expressed
  // by a changed value/nullopt, never by a one-shot cleanup directive. The
  // task must return only after superseded admission and result publication
  // can no longer escape the FullStateApplied barrier.
  virtual bycorf::Task<absl::Status> ReconcileClusterControl(
      std::optional<DesiredClusterControl> desired);
  // Opens the exact boot-local promotion prepared by `action_id`. This grants
  // neither a write lease nor active expiration authority; NodeControl does
  // both only after a post-await local control/session/deadline recheck.
  virtual bycorf::Task<absl::Status> ActivatePreparedPromotion(
      PreparedFailoverActivation activation);
  // Installs the same absolute CLOCK_BOOTTIME deadline used by the request
  // lease. Returning success means expiration can run only until that finite
  // cut; it does not imply request authority.
  virtual bycorf::Task<absl::Status> EnableExpirationAuthorityUntil(
      MonotonicTime deadline);
  // Opens new POPULATION source handshakes after the request lease itself is
  // installed. Implementations must serialize this with native admission.
  virtual bycorf::Task<absl::Status> EnableSourceAdmissionForLease(
      MonotonicTime deadline);
  // Closes active expiration and joins work that entered before the close.
  // Every asynchronous authority-loss barrier invokes this before returning.
  virtual bycorf::Task<absl::Status> RevokeExpirationAuthority();
  // Retires an in-progress or ready target population unless it still names
  // the desired local assignment, term, manifest, and partition replication
  // epoch. Completion includes
  // native-flow join and partial-root abort, making FullStateApplied a real
  // population invalidation barrier.
  virtual bycorf::Task<absl::Status> ReconcilePopulation(
      std::optional<PopulationReadiness> desired,
      bool population_transition_expected);
  // Cancels destructive target work after an authority transition. Session
  // replacement sets `preserve_current_follow_attempt`: a session-scoped
  // directive loses its result channel, but an exact live level-triggered
  // FollowOwner attempt remains valid. FollowOwner FULL rotates local history
  // and therefore causes this same control-session replacement before it can
  // become Ready. Fences pass false and retain no such exception.
  virtual bycorf::Task<absl::Status> CancelInProgressPopulation(
      bool preserve_current_follow_attempt);
  // Graceful process shutdown must resolve even an attempt whose directive
  // executor is waiting for terminal native cleanup. ReplicationManager uses
  // its stronger shutdown cancellation; other adapters may reuse ordinary
  // in-progress cancellation.
  virtual bycorf::Task<absl::Status> CancelPopulationForShutdown();
  // The default adapts actions whose admission and completion are one short
  // operation. ReplicationManager overrides this for population mutations so
  // admission returns a pollable exact-attempt completion without awaiting
  // readiness.
  virtual bycorf::Task<NodeDirectiveCompletion> StartDirective(
      NodeDirective directive);
  // Non-suspending, non-mutating lookup: only an exact, still-valid completed
  // population may return its original completion. A miss falls through to
  // normal mutation admission; implementations must never initiate a reset.
  virtual std::optional<NodeDirectiveCompletion> FindCompletedPopulation(
      const NodeDirective& /*directive*/) const {
    return std::nullopt;
  }
  virtual bycorf::Task<absl::Status> ApplyDirective(
      NodeDirective directive) = 0;
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
  bycorf::Task<absl::Status> ApplyDirective(NodeDirective directive) override;
  absl::Status DrainAssignment(const AuthorityAnchor& anchor) override;
};

class NodeControlInstaller {
 public:
  // Lease grants create worker-owned expiration tasks that retain this
  // installer's references. The owner must destroy the Bycorf worker (and thus
  // its detached tasks) before destroying the installer; the destructor
  // asserts that contract in debug builds. ClusterRuntime satisfies it by
  // remaining installed until Server::WaitUntilStopped returns.
  NodeControlInstaller(TopologyCache& topology, AuthorityGuard& authority,
                       NodeControlActions& actions);
  ~NodeControlInstaller();
  NodeControlInstaller(const NodeControlInstaller&) = delete;
  NodeControlInstaller& operator=(const NodeControlInstaller&) = delete;

  // Publishes routing on worker 0 while preserving exact local control,
  // task admission, exports, and leases. Rejects any local control change.
  absl::Status InstallRouting(PreparedFullState prepared_state);

  // Installs one completely decoded and validated snapshot through a test
  // adapter that never receives directives. Directive-capable adapters must
  // use InstallFullStateTransition(), even when a particular snapshot appears
  // to require no cleanup: concurrent admission is what makes the synchronous
  // path unsafe. Meta transitions run on worker 0. Lower source indices and
  // same-assignment counter regressions fail closed; equal-index replay is
  // idempotent.
  absl::Status InstallFullState(PreparedFullState prepared_state,
                                ProjectionBasis projection_basis);

  // Applies a live lease at the caller's suspend-aware clock cut, or a
  // committed fence through the synchronous test seam.
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
  // only that lease instance and closes new source admission. A POPULATION
  // session already published under the exact capability continues; authority
  // or desired-state replacement remains responsible for retiring it.
  bycorf::Task<absl::Status> ApplyLeaseGrantTransition(
      const AuthorityMessage& authority_message);

  // Meta-only local control boundary. In addition to installing the immutable
  // state, this invalidates and joins older directive admissions, then
  // refreshes the source-capability ledger. An exact population scope preserves
  // published exports and reserves its expected authorization replays; a
  // changed scope joins the exports it invalidates. Authority-changing
  // snapshots also wait for mutations admitted through the replaced
  // ServingState before returning to the wire client.
  bycorf::Task<absl::Status> InstallFullStateTransition(
      PreparedFullState prepared_state, ProjectionBasis projection_basis,
      bool local_population_transition_expected = false,
      std::size_t expected_source_authorization_replays = 0);

  // Meta-only fence barrier. New authority and directive admission are blocked
  // synchronously; success is returned only after earlier action registration,
  // final population cancellation, source capability cleanup, and mutations
  // admitted through every retired snapshot for the group have drained.
  bycorf::Task<absl::Status> ApplyFenceTransition(
      const AuthorityMessage& authority_message);

  // Validates projection and authority before handing a storage mutation to
  // the ReplicationManager adapter. The admission token spans every await and
  // action registration so an invalidating control transition can join it.
  bycorf::Task<NodeDirectiveCompletion> StartDirective(NodeDirective directive);
  bycorf::Task<absl::Status> ApplyDirective(NodeDirective directive);

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
  bycorf::Task<absl::Status> CancelPopulationForShutdownTransition();

  // Synchronous test session-loss path. Memory authority is invalidated
  // even when a directive-capable adapter rejects the remaining cleanup; Meta
  // callers then use LoseSessionTransition(). It intentionally does not
  // synthesize or publish a fenced topology, because Meta remains the sole
  // committed source of topology truth.
  absl::Status LoseSession(const SessionIdentity& session_identity,
                           std::string_view reason);

  // Records every current local assignment as draining before awaiting
  // session-scoped cleanup. Session loss closes write authority and new source
  // admission, but exact already-online exports retain their immutable
  // population snapshots; a replacement local control must replay authorization
  // before new exports can start. Lease, rebuild, and new source authorization
  // remain blocked by the drains meanwhile.
  bycorf::Task<absl::Status> LoseSessionTransition(
      const SessionIdentity& session_identity, std::string_view reason);

  // Publishes the current ReplicationManager proof into ServingState. Losing
  // a previously ready proof invalidates leases synchronously and joins source
  // revocation before returning.
  bycorf::Task<absl::Status> SetPopulationReadinessTransition(
      std::optional<PopulationReadiness> readiness);

  // Idempotent async barrier used after a concurrently executing directive is
  // joined, ensuring it could not resurrect a capability behind a fence or
  // local proof loss.
  bycorf::Task<absl::Status> RevokeSourceAuthorizationsTransition();

  // Publishes initial local storage readiness together with the current
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
  bycorf::Task<absl::Status> LoseStorageReadinessTransition();

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
  };

  struct PendingDrain {
    std::shared_ptr<const ServingState> state_;
    std::string group_id_;
    AuthorityAnchor retired_anchor_;
  };

  struct FullStateEffects {
    bool revoke_sources_ = false;
    // Same-scope local control refresh preserves every already-published
    // population export, including a session that has not reached ONLINE yet.
    bool preserve_current_population_exports_ = false;
    std::vector<AuthorityAnchor> retired_;
    // A Controlled Pause retires only the old mutation admission snapshot,
    // not authority or the established replication export.
    std::vector<AuthorityAnchor> pause_drains_;
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
  absl::StatusOr<std::optional<DesiredClusterControl>>
  DesiredLocalClusterControl() const;
  absl::StatusOr<std::optional<DesiredClusterControl>>
  ValidateLeaseGrantContext(const AuthorityMessage& message, MonotonicTime now);
  bycorf::Task<absl::Status> FailClosedLeaseGrantTransition(
      const AuthorityMessage& message, absl::Status failure);
  void RetireLeaseSchedule(std::string_view group_id);
  void RetireAllLeaseSchedules();
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
  bycorf::Task<absl::Status> WaitForDirectiveAdmissions();
  // Storage loss is terminal for the boot. It waits only for transitions that
  // were already active at its cut, then performs one final cleanup pass; work
  // admitted after the cut is already constrained by storage_failed_.
  bycorf::Task<absl::Status> WaitForControlTransitionsBefore(
      std::uint64_t transition_id);
  bycorf::Task<absl::Status> SetPopulationReadinessTransitionImpl(
      std::optional<PopulationReadiness> readiness,
      bool invalidate_directive_admissions);
  bycorf::Task<absl::Status> WaitForPendingDrains(
      std::span<const AuthorityAnchor> anchors);
  bycorf::Task<absl::Status> ExpireLeaseAt(
      std::shared_ptr<LeaseExpirySchedule> schedule,
      std::uint64_t timer_generation,
      std::shared_ptr<const LeaseTimerLifetime> lifetime);
  // Completes one exact due schedule before any replacement grant is installed.
  // It invalidates the old lease generation synchronously, retires the timer,
  // closes new source admission, joins directive admission, and drains the
  // assignment. Current source capabilities and published exports remain for a
  // stronger fence, session-loss, or population-identity transition to retire.
  bycorf::Task<absl::Status> FinishExpiredLeaseTransition(
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
  // later local control or directive from treating failure as rebuildable
  // readiness.
  bool storage_failed_ = false;
  // Empty while the first storage-loss coroutine is still joining cleanup;
  // afterwards it preserves success or the exact uncertain failure so a
  // repeated call can never manufacture a successful barrier.
  std::optional<absl::Status> storage_loss_result_;
  std::optional<ProjectionBasis> projection_basis_;
  std::uint32_t authority_lease_duration_ms_ = 0;
  std::vector<DesiredClusterControl> desired_cluster_controls_;
  std::optional<DesiredClusterControl> reconciled_cluster_control_;
  bool cluster_control_reconciled_ = false;
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

}  // namespace lavik::cluster
