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

#include <array>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "bycorf/net/tcp_stream.h"
#include "bycorf/runtime/task.h"
#include "lavik/replication_group.h"

namespace bycorf {
class TlsContext;
class Worker;
}  // namespace bycorf

namespace lavik::storage {
class StorageEngine;
}  // namespace lavik::storage

namespace lavik {

namespace detail {
class ClusterRebuildCompletionState;
class ClusterPromotionPrepareCompletionState;
}  // namespace detail

struct ReplicaOfConfig {
  std::string host_;
  std::uint16_t port_ = 0;

  bool operator==(const ReplicaOfConfig&) const = default;
};

// One committed Group membership incarnation used by steady native
// replication. Node ids identify authenticated peers; assignment ids prevent
// remove/re-add of the same node from inheriting an old relationship.
struct ClusterReplicationMember {
  std::string node_id_;
  std::string assignment_id_;

  bool operator==(const ClusterReplicationMember&) const = default;
};

// Level-triggered steady relationship installed from one complete FDS. The
// same value is meaningful on both sides: the Owner authorizes the listed
// downstream incarnations, while every other local member connects to the
// exact Owner endpoint. Source boot/history are intentionally absent because
// they are learned from the authenticated live handshake, not committed
// topology.
struct DesiredClusterUpstream {
  std::string group_id_;
  std::uint64_t group_term_ = 0;
  std::string local_node_id_;
  std::string local_assignment_id_;
  std::string local_boot_id_;
  std::string owner_node_id_;
  std::string owner_assignment_id_;
  std::optional<ReplicaOfConfig> owner_endpoint_;
  std::uint64_t manifest_revision_ = 0;
  PopulationManifestId manifest_id_;
  std::vector<PopulationManifestEntry> manifest_entries_;
  std::uint64_t partition_replication_epoch_ = 0;
  std::vector<ClusterReplicationMember> members_;

  bool operator==(const DesiredClusterUpstream&) const = default;
};

struct ReplicationOptions {
  // Cluster mode is always Meta-managed. It disables standalone upstream
  // control and Redis PSYNC export; native export requires an exact population
  // grant from the active Meta session.
  bool cluster_enabled_ = false;
  // Set by the Meta control adapter to its validated 160-bit data-node
  // identity. Standalone deployments use a fresh CSPRNG identity each boot.
  std::optional<std::string> node_id_override_;
  // Consulted only while this node has an upstream. REPLICAOF NO ONE opens
  // writes only after the shared promotion durability path succeeds.
  bool replica_read_only_ = true;
  // Redis Sentinel promotes only replicas with a nonzero priority and prefers
  // lower values. This is runtime mutable through CONFIG SET.
  unsigned replica_priority_ = 100;
  // Advertised to the source during the native control handshake so INFO
  // and CLUSTER NODES can identify the replica's Redis endpoint.
  std::uint16_t listen_port_ = 6379;
  bool use_tls_ = false;
  std::shared_ptr<bycorf::TlsContext> tls_context_;
  std::string masteruser_ = "default";
  std::string masterauth_;
  // Global in-memory history quota. Chunks are allocated lazily and distributed
  // across source-worker flows without multiplying this value by worker count.
  std::size_t backlog_size_bytes_ = 1ULL * 1024 * 1024 * 1024;
  // Preserve an online consumer's unacknowledged history by backpressuring
  // source writes at the backlog limit. Operators may disable this at runtime
  // to prefer primary availability and force lagging consumers to full-sync.
  bool backlog_backpressure_ = true;
  // Bounded source publisher staging memory on each worker. A single larger
  // command may exceed this waterline only while it is the exclusive item.
  std::size_t publish_queue_bytes_per_worker_ = 16ULL * 1024 * 1024;
  // Compatibility override declaring that the initial upstream speaks Redis
  // PSYNC. Ordinary replicaof performs safe protocol detection instead.
  bool redis_psync_ = false;
  // Retain the post-cut Redis export cursor. When disabled (the default), a
  // slow Redis replica is disconnected after it falls behind the bounded
  // backlog instead of applying backpressure to foreground writes.
  bool redis_export_backpressure_ = false;
  // Number of keys one source flow admits into a snapshot scheduling round.
  // Sampled for every round so CONFIG SET takes effect during full sync.
  std::size_t snapshot_batch_size_ = 64;
};

inline constexpr unsigned kDefaultReplicationSnapshotReadConcurrency = 16;
inline constexpr unsigned kMaxReplicationSnapshotReadConcurrency = 128;
inline constexpr std::size_t kMaxReplicationSnapshotBatchSize = 4096;

enum class ReplicationRole : std::uint8_t {
  kMaster,
  kConnecting,
  kSyncing,
  kOnline,
};

struct DownstreamReplicaStatus {
  std::string node_id_;
  std::string host_;
  std::uint16_t port_ = 0;
  bool online_ = false;
  std::uint64_t min_lsn_ = 0;
};

struct RedisSourceStatus {
  ReplicaOfConfig upstream_;
  std::string node_id_;
  std::string slots_;
  std::optional<std::string> replid_;
  std::uint64_t offset_ = 0;
  bool link_up_ = false;
  bool dataset_valid_ = false;
};

// Identity used to bind control sessions. The local source history may change
// within one process boot, so an established session must revalidate it.
struct ReplicationIdentity {
  std::string local_node_id_;
  std::string boot_id_;
  std::string local_history_id_;
};

struct ReplicationStatus {
  ReplicationRole role_ = ReplicationRole::kMaster;
  std::optional<ReplicaOfConfig> upstream_;
  std::uint64_t role_epoch_ = 0;
  std::uint64_t session_id_ = 0;
  unsigned source_worker_count_ = 0;
  unsigned connected_flows_ = 0;
  std::string local_node_id_;
  std::string group_id_;
  std::string boot_id_;
  std::string replica_incarnation_;
  std::string local_history_id_;
  std::optional<std::string> upstream_node_id_;
  std::optional<std::string> upstream_history_id_;
  std::vector<DownstreamReplicaStatus> downstream_replicas_;
  std::vector<RedisSourceStatus> redis_sources_;
  std::uint64_t replica_repl_offset_ = 0;
  std::uint64_t master_repl_offset_ = 0;
  std::uint64_t master_link_down_since_seconds_ = 0;
  std::uint64_t master_last_io_seconds_ago_ = 0;
  unsigned replica_priority_ = 100;
  bool redis_cluster_ = false;
  bool redis_topology_fault_ = false;
  // A current-boot terminal latch for a target-side outcome whose storage
  // effects cannot be proven. The node remains LOADING and rejects role
  // changes until restart rather than retrying or becoming writable.
  bool failed_stopped_ = false;
  // Nonempty exactly when failed_stopped_ is true and describes the outcome
  // whose effects could not be proven.
  std::string failure_reason_;
};

// Typed current-boot result exposed to the Data-side node controller. A
// missing ready token means the population must not be reported as readable
// or candidate-eligible even if partial records exist on disk.
struct ClusterPopulationStatus {
  // The control adapter needs both values before it can construct a directive;
  // the node identity names this process in the current native implementation,
  // while the boot identity scopes every readiness proof and authorization.
  std::string local_node_id_;
  std::string local_boot_id_;
  ReplicationGroupState state_ = ReplicationGroupState::kNotReady;
  std::optional<ReadyToken> ready_token_;
  // A best-effort coherent snapshot of the live next-unapplied LSN frontier.
  // It is present only when the Ready population and frontier still agree;
  // heartbeat construction omits candidate evidence when sampling is busy.
  std::optional<std::vector<std::uint64_t>> applied_next_lsns_;
  // A terminal failover action suppresses the same boot-local population and
  // compatibility domain from candidate selection. A changed population or
  // domain is eligible again; a process restart naturally drops the latch.
  bool failover_candidate_eligible_ = true;
  // Recovery may nominate a candidate but cannot renew a previous Owner
  // grant. Cleared after fresh action-scoped promotion preparation succeeds.
  bool recovered_ = false;
  // Readable persisted scope without a certified frontier is exposed only to
  // an explicit operator-recovery action, never automatic candidate ranking.
  std::optional<RebuildIdentity> operator_recovery_identity_;
  // Nonempty exactly while state_ is kFailedStopped.
  std::string failure_reason_;
};

// Opaque identities copied from committed Meta state. A transition may span
// candidate replacement, while an action identifies one exact candidate
// attempt; ReplicationManager only compares them for equality.
using ClusterFailoverTransitionId = std::array<std::uint8_t, 16>;
using ClusterFailoverActionId = std::array<std::uint8_t, 16>;

// Opaque boot-local handle and digest for retained prepared resources. They
// are observations rather than durable identities and are discarded on
// restart or action replacement.
using ClusterPreparedContextId = std::array<std::uint8_t, 16>;

// Wire-independent execution semantics derived from committed state.
// Controlled actions prepare while the old owner remains authoritative;
// Uncontrolled actions prepare only after that authority has been fenced.
enum class ClusterFailoverMode : std::uint8_t {
  kControlled,
  kUncontrolled,
};

// Exact committed controlled-failover intent for the current source. Mutation
// admission is drained by NodeControl before this reaches ReplicationManager;
// this layer owns only the nestable active-expiration pause and stable native
// replication frontier.
struct DesiredClusterSourcePause {
  ClusterFailoverTransitionId transition_id_{};
  std::uint64_t transition_revision_ = 0;
  std::string group_id_;
  std::string source_node_id_;
  std::string source_assignment_id_;
  std::string source_boot_id_;
  std::string source_history_id_;
  std::uint64_t source_group_term_ = 0;
  std::uint32_t flow_count_ = 0;
  std::uint64_t manifest_revision_ = 0;
  PopulationManifestId manifest_id_;
  std::uint64_t partition_replication_epoch_ = 0;

  bool operator==(const DesiredClusterSourcePause&) const = default;
};

// Boot-local observation for one desired source pause. A missing stable vector
// means the desired intent is retained but has not produced SourcePaused; the
// caller may replay it after a transient capture failure without opening an
// expiration window.
struct ClusterSourcePauseStatus {
  std::optional<DesiredClusterSourcePause> desired_;
  std::optional<std::vector<std::uint64_t>> stable_next_lsns_;
  std::string failure_detail_;
};

// Exact source lineage within which Meta compared candidate progress. Data
// validates this against its live Ready population but never receives the
// volatile SourcePaused frontier that authorized the action.
struct ClusterFailoverCompatibilityDomain {
  std::uint64_t source_group_term_ = 0;
  std::string source_node_id_;
  std::string source_assignment_id_;
  std::string source_boot_id_;
  std::string source_history_id_;
  std::uint32_t flow_count_ = 0;

  bool operator==(const ClusterFailoverCompatibilityDomain&) const = default;
};

// Wire-independent, level-triggered execution subset of one committed
// candidate action. A missing authorized_revision pins the intent without
// allowing promotion preparation. Population fields come from the same FDS
// and prevent an action from crossing assignment or immutable data identity.
struct DesiredClusterFailoverAction {
  ClusterFailoverTransitionId transition_id_{};
  ClusterFailoverActionId action_id_{};
  std::uint64_t transition_revision_ = 0;
  ClusterFailoverMode mode_ = ClusterFailoverMode::kControlled;
  std::uint64_t target_term_ = 0;
  // Current authority cut from the same FDS. Controlled preparation occurs
  // while the preceding term may still be granted; Uncontrolled preparation
  // requires target_term already installed and grantless.
  std::uint64_t committed_group_term_ = 0;
  bool committed_grant_active_ = false;
  std::optional<std::uint64_t> authorized_revision_;
  std::string group_id_;
  std::string candidate_node_id_;
  std::string candidate_assignment_id_;
  std::string candidate_boot_id_;
  ClusterFailoverCompatibilityDomain domain_;
  std::uint64_t manifest_revision_ = 0;
  PopulationManifestId manifest_id_;
  std::uint64_t partition_replication_epoch_ = 0;

  bool operator_recovery_ = false;
  std::vector<PopulationManifestEntry> manifest_entries_;
  // Meta fixes this once for the entire transition, including replacements.
  std::optional<std::uint64_t> recovery_deadline_unix_ms_;

  bool operator==(const DesiredClusterFailoverAction&) const = default;
};

struct ClusterRecoveryPeer {
  ClusterReplicationMember member_;
  ReplicaOfConfig endpoint_;  // An absent transport is represented by port 0.
  bool operator==(const ClusterRecoveryPeer&) const = default;
};

// Read-only recovery scope installed on every member from one current FDS.
// Donor boot and actual coverage are discovered directly; Meta owns no donor
// inventory or transfer journal. It grants neither serving nor Owner export.
struct DesiredClusterRecovery {
  DesiredClusterFailoverAction action_;
  std::string local_node_id_;
  std::string local_assignment_id_;
  std::string local_boot_id_;
  std::vector<ClusterRecoveryPeer> members_;
  bool operator==(const DesiredClusterRecovery&) const = default;
};

// Exact boot-local promotion-preparation input derived from an installed
// desired Candidate Action. RebuildIdentity is reused for the candidate,
// source, manifest, transition, action, and attempt anchors; the additional
// fields bind committed authority exclusion and the live parent frontier that
// preparation must freeze.
struct ClusterPromotionPrepareDirective {
  RebuildIdentity identity_;
  std::string parent_history_id_;
  std::vector<std::uint64_t> required_applied_next_lsns_;
  std::uint64_t excluded_group_term_ = 0;

  bool operator==(const ClusterPromotionPrepareDirective&) const = default;
};

// Boot-local result returned after the shared promotion kernel has made the
// parent frontier durable, committed PromotionBase, retired the parent
// history, and created the child publisher. It grants no serving authority.
struct ClusterPromotionPrepared {
  std::string parent_history_id_;
  std::vector<std::uint64_t> frozen_applied_next_lsns_;
  std::uint64_t population_generation_ = 0;
  std::uint64_t population_digest_ = 0;
  std::uint64_t catalog_generation_ = 0;
  std::uint64_t catalog_dump_crc64_ = 0;
  std::string child_history_id_;

  bool operator==(const ClusterPromotionPrepared&) const = default;
};

// Boot-local proof that the exact transition action completed promotion
// preparation. The action identifies the retained replication resources;
// the opaque context id distinguishes this boot-local preparation report.
struct ClusterFailoverPreparedContext {
  ClusterFailoverTransitionId transition_id_{};
  ClusterFailoverActionId action_id_{};
  ClusterPreparedContextId context_id_{};
  ClusterPromotionPrepared promotion_;

  bool operator==(const ClusterFailoverPreparedContext&) const = default;
};

// Exact cutover intent presented to the boot-local promotion adapter after a
// finite lease has been validated provisionally by NodeControl. The adapter
// opens only the prepared storage role; lease and expiration authority remain
// separate final-commit steps owned by NodeControl.
struct ClusterFailoverActivation {
  ClusterFailoverActionId action_id_{};
  std::string group_id_;
  std::string candidate_node_id_;
  std::string candidate_assignment_id_;
  std::string candidate_boot_id_;
  std::uint64_t target_term_ = 0;
  std::uint64_t manifest_revision_ = 0;
  PopulationManifestId manifest_id_;
  std::uint64_t partition_replication_epoch_ = 0;

  bool operator==(const ClusterFailoverActivation&) const = default;
};

// Observable lifecycle of the currently installed candidate action. States
// are boot-local and level-triggered; replacing or removing the desired action
// discards this progress instead of carrying it into the next attempt. None
// means no action is installed for this boot, not an unknown terminal outcome.
enum class ClusterFailoverActionState : std::uint8_t {
  kNone,
  kWaitingForAuthorization,
  kWaitingForPopulation,
  kPreparing,
  kRetrying,
  kPrepared,
  kFailed,
  kRecovering,
  kRecoveryComplete,
};

struct ClusterCandidateRecoveryResult {
  std::vector<std::uint64_t> applied_next_lsns_;
  std::string completion_reason_;
  bool operator==(const ClusterCandidateRecoveryResult&) const = default;
};

// Boot-local heartbeat input for the current committed action. A replacement
// or removal first makes the old status unobservable, then joins its local
// admission before FullStateApplied may be acknowledged.
struct ClusterFailoverActionStatus {
  ClusterFailoverActionState state_ = ClusterFailoverActionState::kNone;
  std::optional<DesiredClusterFailoverAction> action_;
  std::optional<ClusterFailoverPreparedContext> prepared_;
  std::string failure_class_;
  std::string failure_detail_;
  std::optional<ClusterCandidateRecoveryResult> recovery_;
};

// FDS-owned subset of population identity. Assignment and immutable manifest
// plus the Meta partition-replication epoch decide whether a completed local
// population still belongs to the group; a term additionally scopes an
// in-progress population-transition directive. BeginGroupTerm fences authority
// but does not mutate bytes, so a completed population or the exact live
// steady FollowOwner copy may be re-anchored to a later committed term without
// another destructive rebuild.
struct DesiredClusterPopulation {
  std::string group_id_;
  std::string assignment_id_;
  std::uint64_t term_ = 0;
  std::uint64_t manifest_revision_ = 0;
  PopulationManifestId manifest_id_;
  std::uint64_t partition_replication_epoch_ = 0;
  // False means the FDS removed the live population-transition directive: an
  // in-progress attempt must retire even when its population identity still
  // matches. Both replication rebuild and source-less initialization use this
  // lifecycle bit.
  bool population_transition_expected_ = false;

  friend bool operator==(const DesiredClusterPopulation&,
                         const DesiredClusterPopulation&) = default;
};

// A boot-local handle for one exact Meta rebuild attempt. Starting a rebuild
// and observing its terminal outcome are separate so reconciliation may
// supersede an in-progress attempt without treating admission as completion.
// Await() is repeatable and returns only after the attempt is Ready or after
// cancellation/failure cleanup has made its partial storage effects unusable.
class ClusterRebuildCompletion {
 public:
  ClusterRebuildCompletion() = default;

  bycorf::Task<absl::Status> Await() const;
  // Lock-safe nonblocking observation used by a control session whose wire
  // lifetime may end before the underlying rebuild attempt does.
  std::optional<absl::Status> result() const;
  bool valid() const noexcept { return state_ != nullptr; }

 private:
  explicit ClusterRebuildCompletion(
      std::shared_ptr<detail::ClusterRebuildCompletionState> state)
      : state_(std::move(state)) {}

  friend class ReplicationManager;
  std::shared_ptr<detail::ClusterRebuildCompletionState> state_;
};

// Pollable completion for one exact promotion-preparation attempt. Exact
// Candidate Action replay shares this state, so repeated FDS reconciliation
// cannot repeat durability or history-creation side effects.
class ClusterPromotionPrepareCompletion {
 public:
  using Result = absl::StatusOr<ClusterPromotionPrepared>;

  ClusterPromotionPrepareCompletion() = default;

  bycorf::Task<Result> Await() const;
  std::optional<Result> result() const;
  bool valid() const noexcept { return state_ != nullptr; }

 private:
  explicit ClusterPromotionPrepareCompletion(
      std::shared_ptr<detail::ClusterPromotionPrepareCompletionState> state)
      : state_(std::move(state)) {}

  friend class ReplicationManager;
  std::shared_ptr<detail::ClusterPromotionPrepareCompletionState> state_;
};

// A source-history-local cut across Lavik's worker replication logs. Native
// replicas acknowledge one independent LSN stream per source worker, so a
// scalar Redis-style byte offset cannot represent the same delivery boundary.
struct NativeReplicationWatermark {
  std::string history_id_;
  std::vector<std::uint64_t> next_lsns_;
};

struct ReplicationDirective {
  // kSetUpstream consumes upstream_; an empty endpoint requests promotion.
  // kAddUpstream requires upstream_ and adds another Redis Cluster source to
  // this node's sole group. The remaining kinds consume value_ in bytes,
  // commands, or unitless counts as named by the kind.
  enum class Kind : std::uint8_t {
    kSetUpstream,
    kAddUpstream,
    kBacklogBytes,
    kBacklogBackpressure,
    kPublishQueueBytes,
    kSnapshotReadConcurrency,
    kSnapshotBatchSize,
    kReplicaPriority,
  };

  Kind kind_ = Kind::kSetUpstream;
  std::optional<ReplicaOfConfig> upstream_;
  std::uint64_t value_ = 0;
};

// Owns exactly one replication group. Replica connections are initiated on
// worker 0 for control and on one target worker per source flow. Source-side
// accepted flow sockets are adopted by the matching source worker.
// Mutable control state belongs to worker 0. Asynchronous APIs may be called
// on any runtime worker and preserve caller affinity across the owner hop.
class ReplicationManager {
 public:
  ReplicationManager(storage::StorageEngine* storage,
                     ReplicationOptions options,
                     std::optional<ReplicaOfConfig> initial_upstream);
  ReplicationManager(const ReplicationManager&) = delete;
  ReplicationManager& operator=(const ReplicationManager&) = delete;
  ~ReplicationManager();

  void StorageReady(bycorf::Worker& worker);

  // The deep group interface: administrative changes enter as directives,
  // peer sockets enter through the handlers below, and Observe returns one
  // coherent control-plane snapshot without blocking the caller's runtime
  // worker while another worker updates the native session registry.
  bycorf::Task<absl::Status> ApplyDirective(ReplicationDirective directive);
  bycorf::Task<ReplicationStatus> Observe() const;

  // Copies the current node, boot, and local history identities without
  // collecting replication progress or downstream session status.
  bycorf::Task<ReplicationIdentity> ObserveIdentity() const;

  // Copies the immutable desired-upstream snapshot. Runtime workers cache it
  // locally; unchanged reads require no cross-worker hop or shared refcount
  // update. This is not an admission proof: retain role and mode checks.
  std::optional<ReplicaOfConfig> upstream() const;

  // Starts one already-validated Meta full-rebuild directive and returns the
  // exact attempt's boot-local completion. Admission is not a terminal result:
  // callers that acknowledge a directive must Await() the returned handle.
  // Exact replay shares the original completion, while supersession resolves
  // the older handle only after cancellation/join/abort has finished.
  bycorf::Task<absl::StatusOr<ClusterRebuildCompletion>>
  StartClusterRebuildDirective(ReplicaOfConfig upstream,
                               RebuildDirective directive,
                               PopulationManifest manifest);

  // Starts source-less initialization of the first Meta-owned population.
  // The target history carried by identity is checked against this process
  // before any reset. Exact replay of an active or still-valid completed
  // attempt shares its original completion without resetting storage again;
  // an invalidated proof requires a fresh attempt identity. Success resolves
  // only after durable root promotion and ReadyToken publication.
  bycorf::Task<absl::StatusOr<ClusterRebuildCompletion>>
  StartEmptyPopulationInitialization(RebuildIdentity identity,
                                     PopulationManifest manifest);

  // Worker-zero-only, non-suspending exact replay lookup. Returns a still-valid
  // Ready attempt's original completion, never starts/restarts work or clears
  // readiness. NodeControl uses this before new-mutation admission so a lost
  // result can be replayed while the completed population is already serving.
  // NodeControl runs on that same owner, making its validation and lookup one
  // uninterrupted decision; cross-worker callers must explicitly submit it.
  std::optional<ClusterRebuildCompletion> FindCompletedClusterPopulation(
      const RebuildDirective& directive) const;

  // Starts the prepare half of a Meta-authorized promotion. Success preserves
  // LOADING, write fencing, and disabled expiration authority. A separate
  // authority workflow may activate only after a later FDS plus current-session
  // lease. Exact replay returns the original completion and evidence without
  // repeating local side effects.
  bycorf::Task<absl::StatusOr<ClusterPromotionPrepareCompletion>>
  StartClusterPromotionPrepareDirective(
      ClusterPromotionPrepareDirective directive);

  // Reconciles the controlled source's transition-scoped pause after
  // NodeControl has published paused mutation admission and drained earlier
  // work. Replacement retains the existing expiration pause while recapturing
  // exact evidence; null releases exactly the pause owned by this context.
  bycorf::Task<absl::Status> ReconcileClusterSourcePause(
      std::optional<DesiredClusterSourcePause> desired);

  // Returns SourcePaused input only after the native history and all flow
  // frontiers match the current desired source incarnation.
  bycorf::Task<ClusterSourcePauseStatus> cluster_source_pause_status() const;

  // Reconciles the current committed candidate action. Authorization is a
  // one-way gate; exact replay is a no-op. Replacement/removal withdraws old
  // progress immediately and returns only after its prepare admission can no
  // longer publish and any abandoned prepared child history has been joined
  // and disabled. Cutover may name the exact prepared action as a pending
  // activation; that context and its child log are retained but no longer
  // reported as transition progress. Catch-up remains owned by the ordinary
  // population coordinator and does not delay this desired-state boundary.
  bycorf::Task<absl::Status> ReconcileClusterFailoverAction(
      std::optional<DesiredClusterFailoverAction> desired,
      std::optional<ClusterFailoverActionId> pending_activation_action_id =
          std::nullopt);

  // Installs or revokes the read-only donor scope from the same current FDS.
  // Replacement closes old transfers and joins accepted candidate apply;
  // it does not grant ordinary source admission or change the deadline.
  bycorf::Task<absl::Status> ReconcileClusterRecovery(
      std::optional<DesiredClusterRecovery> desired);

  // Returns a coherent boot-local action observation. Meta may publish a
  // matching RecoveryComplete (actual complete frontier after safe drain),
  // Prepared, or Failed result. Waiting/retrying states are local diagnostics;
  // no action observation is durable workflow progress.
  bycorf::Task<ClusterFailoverActionStatus> cluster_failover_action_status()
      const;

  // Returns the private boot-local context retained across a successful
  // Cutover FDS. This is an activation precondition lookup, not a heartbeat
  // observation: transition progress disappears as soon as the transition is
  // removed, while only the grant's exact action id may retrieve the context.
  bycorf::Task<std::optional<ClusterFailoverPreparedContext>>
  FindClusterFailoverPreparedContext(
      const ClusterFailoverActionId& action_id) const;

  // Activates only a retained prepared context whose action, population, boot,
  // target term, and live child history all still match. Exact replay is a
  // no-op. This never resumes expiration or installs lease authority.
  bycorf::Task<absl::Status> ActivateClusterPreparedPromotion(
      ClusterFailoverActivation activation);

  // Installs finite active-expiration authority after NodeControl's final
  // lease/FDS recheck. The absolute deadline uses CLOCK_BOOTTIME semantics.
  bycorf::Task<absl::Status> EnableClusterExpirationAuthorityUntil(
      std::chrono::nanoseconds deadline_since_boot);

  // Revokes future active-expiration work and drains any already-entered
  // cycle without disturbing an outer controlled-source pause.
  bycorf::Task<absl::Status> RevokeClusterExpirationAuthority();

  // Reconciles the ordinary post-Cutover relationship without a Meta rebuild
  // operation. Exact replay leaves a healthy coordinator/export untouched;
  // replacement first cancels and joins the old relationship. A follower
  // preserves its usable population until the new Owner has authenticated and
  // published an export-ready native incarnation, then the existing
  // CONTINUE/FULL machinery decides whether replacement is necessary.
  bycorf::Task<absl::Status> ReconcileClusterFollowOwner(
      std::optional<DesiredClusterUpstream> desired);

  // Convenience wrapper that starts and awaits one full rebuild. Production
  // NodeControl uses StartClusterRebuildDirective so wire admission and later
  // terminal observation remain distinct; this wrapper returns success only
  // after the exact attempt publishes its ReadyToken following promotion.
  bycorf::Task<absl::Status> ApplyClusterRebuildDirective(
      ReplicaOfConfig upstream, RebuildDirective directive,
      PopulationManifest manifest);

  // Process-shutdown barrier for Meta-managed target rebuilds. It closes the
  // native session immediately on worker zero, then returns only after the
  // coordinator has joined its flows and retired any partial candidate root.
  // This must run while the Bycorf runtime and StorageEngine are still alive.
  bycorf::Task<absl::Status> CancelClusterRebuildForShutdown();

  // Consumes durable recovery evidence after all storage/catalog recovery,
  // before announcing storage readiness to Meta. Every boot remains fenced.
  bycorf::Task<absl::Status> RecoverClusterPopulation();

  // Thread-safe first half of process shutdown. It closes outbound target
  // handshakes/sessions and inbound native/Redis source sockets immediately.
  // Source flow teardown releases retained backlog cursors, so callers must
  // invoke this before waiting for admitted client writes to drain.
  void RequestShutdown() noexcept;

  // Process-wide replication shutdown barrier. Prevents native and Redis
  // targets from reconnecting, joins their active apply flows, aborts an
  // incomplete replacement root, and retires source egress/history before
  // storage freezes its index. RequestShutdown must be called first by a
  // non-runtime waiter; calling this coroutine also performs it idempotently.
  bycorf::Task<absl::Status> QuiesceForShutdown();

  // Reconciles the runtime-only target population with an installed FDS.
  // A mismatch (or null desired identity) closes serving immediately, joins
  // native flows, aborts a partial root, retires the ReadyToken/attempt, and
  // resolves its completion. Unlike shutdown cancellation, later directives
  // remain admissible.
  bycorf::Task<absl::Status> ReconcileClusterPopulation(
      std::optional<DesiredClusterPopulation> desired);

  // Transport loss cannot leave an unobserved destructive directive running.
  // A completed Ready population is retained. The caller may additionally
  // preserve the exact live level-triggered FollowOwner attempt whose history
  // rotation caused a Meta-session replacement; strong fences pass false.
  bycorf::Task<absl::Status> CancelInProgressClusterPopulation(
      bool preserve_current_follow_attempt);

  // Returns one coherent boot-scoped population snapshot for heartbeat
  // candidate reporting and directive validation.
  bycorf::Task<ClusterPopulationStatus> cluster_population_status() const;

  // Installs one safe-source authorization delivered through the node
  // controller. A cluster node exports a population only when it is itself
  // ready and activated as the local primary with no upstream, and the incoming
  // native handshake presents this exact rebuild identity. Revisions are
  // monotonic: a newer one revokes and joins older exports before becoming
  // active, and a revoked version cannot be replayed. Until a committed
  // primary-activation transition supplies its authority fence, export stays
  // fail-closed.
  bycorf::Task<absl::Status> AuthorizeClusterRebuildSource(
      RebuildDirective directive);

  // Revokes every downstream destructive-reset capability and reconnect lease
  // (for example, when this node loses primary authority). An accepted
  // directive remains the version watermark, preventing its replay after
  // revocation; revoking an empty ledger is an idempotent no-op. Standalone
  // managers reject this cluster-only transition without disturbing ordinary
  // downstream replication sessions.
  bycorf::Task<absl::Status> RevokeClusterRebuildSourceAuthorizations();

  // Opens the O(1) lease gate for new POPULATION handshakes. This never
  // creates a capability; the exact current FDS must already authorize one or
  // replay it after a live projection refresh.
  bycorf::Task<absl::Status> EnableClusterRebuildSourceAdmissionUntil(
      std::chrono::nanoseconds deadline_since_boot);

  // Clears capabilities inherited from an older desired-state projection
  // without advancing the committed revoke floor. A disconnected control
  // session may preserve only already-ONLINE exports: source admission is
  // still cleared, and NodeControl invalidates the write lease until a
  // replacement FDS validates their group. Live FDS replacement has a
  // separate, stronger retention rule below.
  bycorf::Task<absl::Status>
  ClearClusterRebuildSourceAuthorizationsForSessionReplacement(
      bool preserve_established_exports = false);

  // Clears capabilities for one live FDS refresh without closing an otherwise
  // unchanged lease-admission gate. The expected replay count keeps the
  // projection-to-directive gap retryable and retains the named history until
  // all current capabilities arrive. When the replacement proves the exact
  // export scope unchanged, every already-published POPULATION session is
  // retained, including sessions between control admission and ONLINE.
  bycorf::Task<absl::Status>
  RefreshClusterRebuildSourceAuthorizationsForFdsReplacement(
      bool preserve_current_population_exports = false,
      std::size_t expected_authorization_replays = 0);

  // Current runtime settings; all mutations enter through ApplyDirective.
  unsigned snapshot_read_concurrency() const noexcept;
  std::size_t snapshot_batch_size() const noexcept;

  std::size_t backlog_size_bytes() const noexcept;
  bool backlog_backpressure() const noexcept;
  std::size_t publish_queue_bytes_per_worker() const noexcept;
  unsigned replica_priority() const noexcept;

  // Native control, flow, recovery and parent-export handshakes arrive as
  // isolated RESP commands on the ordinary Redis port.
  static bool IsNativeHandshake(std::span<const std::string> args) noexcept;
  bycorf::Task<absl::Status> ServeNativeConnection(
      bycorf::TcpStream& stream, std::vector<std::string> args,
      std::uint64_t client_id, std::string client_address, bool tls);
  bycorf::Task<absl::Status> ServeRedisExportConnection(
      bycorf::TcpStream& stream, std::vector<std::string> args,
      std::uint64_t client_id, std::string client_address, bool tls,
      bool eof_capable);

  // Captures all source commands already queued on every worker. A missing
  // value means no native replication history is currently active; callers
  // may retry if they are waiting for a replica to connect.
  bycorf::Task<absl::StatusOr<std::optional<NativeReplicationWatermark>>>
  CaptureNativeReplicationWatermark();
  // Returns nullopt when the watermark belongs to an obsolete source history.
  // A replica counts only after every native flow acknowledges the cut.
  bycorf::Task<std::optional<std::uint64_t>> CountAcknowledgedNativeReplicas(
      const NativeReplicationWatermark& watermark) const;
  // The initial per-connection replication offset precedes every source
  // event, so every online native replica satisfies it without a log fence.
  bycorf::Task<std::uint64_t> CountOnlineNativeReplicas() const;
  bool is_replica() const noexcept;
  bool is_loading() const noexcept;
  bool reject_writes() const noexcept;
  // Changes before a topology directive retires or creates a publication
  // history. Command admission uses it to reject writes delayed across that
  // boundary.
  std::uint64_t role_epoch() const noexcept;
  // Returns one opaque packed generation/open token for lock-free client
  // admission. Zero means the dataset is not open for data commands.
  std::uint64_t CaptureServingGeneration() const noexcept;
  // A command may touch storage only while its captured nonzero token still
  // exactly matches the current packed generation/open state.
  bool ServingGenerationMatches(std::uint64_t generation) const noexcept;
  // Native Lavik replicas participate in cluster-style redirection. A
  // standalone Redis PSYNC follower instead serves its local read-only copy.
  bool redirects_clients_to_upstream() const noexcept;
  bool replica_read_only() const noexcept {
    return options_.replica_read_only_;
  }

 private:
  class ReplicationGroup;
  // Data-command admission reads this twice per command. Keep the packed
  // token directly in the public manager rather than behind ReplicationGroup's
  // pImpl pointer; transitions remain cold and receive this atomic by address.
  std::atomic<std::uint64_t> serving_generation_{3};
  std::unique_ptr<ReplicationGroup> group_;
  ReplicationOptions options_;
};

std::string_view ReplicationRoleName(ReplicationRole role) noexcept;

}  // namespace lavik
