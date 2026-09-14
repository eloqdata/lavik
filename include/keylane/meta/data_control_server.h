#pragma once

// Process-lifetime Meta -> Data control listener and its leader-scoped
// publisher. The listener remains bound on followers so seeds can always
// return the committed Meta directory; only the reconciler installed through
// MetaCoordinator::RunAsLeader may create authority-bearing sessions.

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "celer/runtime/foreign_executor.h"
#include "keylane/cluster/control_protocol.h"
#include "keylane/meta/coordinator.h"
#include "keylane/meta/data_control_runtime_status.h"

namespace nuraft {
class raft_server;
}

namespace celer {
struct Connection;
class TcpStream;
}  // namespace celer

namespace keylane::meta {

class MetaObservationStore;
class MetaCommittedFacts;

namespace detail {

// Worker-local admission gate for sockets that have not yet authenticated and
// supplied a valid ClientHello. A permit is move-only and releases itself on
// every coroutine exit path. Followers retain it through their redirect
// write; leaders retain it until the connection has claimed its committed
// node's single post-authentication session slot.
class PendingHandshakeLimiter {
 public:
  class Permit {
   public:
    Permit(Permit&& other) noexcept;
    Permit& operator=(Permit&& other) noexcept;
    ~Permit();

    Permit(const Permit&) = delete;
    Permit& operator=(const Permit&) = delete;

    void Release() noexcept;

   private:
    friend class PendingHandshakeLimiter;
    explicit Permit(PendingHandshakeLimiter* owner) : owner_(owner) {}
    PendingHandshakeLimiter* owner_ = nullptr;
  };

  explicit PendingHandshakeLimiter(std::size_t limit) : limit_(limit) {}

  PendingHandshakeLimiter(const PendingHandshakeLimiter&) = delete;
  PendingHandshakeLimiter& operator=(const PendingHandshakeLimiter&) = delete;
  PendingHandshakeLimiter(PendingHandshakeLimiter&&) = delete;
  PendingHandshakeLimiter& operator=(PendingHandshakeLimiter&&) = delete;

  std::optional<Permit> TryAcquire();
  std::size_t pending() const noexcept { return pending_; }
  std::size_t limit() const noexcept { return limit_; }

 private:
  void Release() noexcept;

  const std::size_t limit_;
  std::size_t pending_ = 0;
};

// Worker-local first-owner registry for post-authentication leader sessions.
// Callers claim only after validating a committed active node binding. That
// makes the number of projection/FDS holders no larger than the committed
// node domain, while exact-owner release prevents a rejected duplicate from
// erasing the incumbent's slot during coroutine cleanup.
class BoundNodeSessionRegistry {
 public:
  bool TryClaim(std::string_view node_id, celer::Connection* connection);
  void Release(std::string_view node_id,
               celer::Connection* connection) noexcept;
  std::size_t size() const noexcept { return sessions_.size(); }

 private:
  std::map<std::string, celer::Connection*, std::less<>> sessions_;
};

// Worker-local weighted budget for decoded-plus-encoded node projections.
// Unlike a session-count cap, charging retained capacities prevents a small
// number of maximum-sized FDS owners from multiplying memory without bound.
class RetainedProjectionLimiter {
 public:
  class Permit {
   public:
    Permit(Permit&& other) noexcept;
    Permit& operator=(Permit&& other) noexcept;
    ~Permit();

    Permit(const Permit&) = delete;
    Permit& operator=(const Permit&) = delete;

    absl::Status Resize(std::size_t bytes);
    void Release() noexcept;
    std::size_t bytes() const noexcept { return bytes_; }

   private:
    friend class RetainedProjectionLimiter;
    Permit(RetainedProjectionLimiter* owner, std::size_t bytes)
        : owner_(owner), bytes_(bytes) {}
    RetainedProjectionLimiter* owner_ = nullptr;
    std::size_t bytes_ = 0;
  };

  explicit RetainedProjectionLimiter(std::size_t limit) : limit_(limit) {}

  RetainedProjectionLimiter(const RetainedProjectionLimiter&) = delete;
  RetainedProjectionLimiter& operator=(const RetainedProjectionLimiter&) =
      delete;

  std::optional<Permit> TryAcquire(std::size_t bytes);
  std::size_t retained_bytes() const noexcept { return retained_bytes_; }
  std::size_t limit() const noexcept { return limit_; }

 private:
  friend class Permit;
  bool Resize(Permit& permit, std::size_t bytes) noexcept;
  void Release(std::size_t bytes) noexcept;

  const std::size_t limit_;
  std::size_t retained_bytes_ = 0;
};

// Worker-local immutable view cache shared by all Data sessions. A Meta
// commit may wake thousands of sessions, but the seven committed stores are
// copied only once for each new applied high-water. Not thread-safe: the
// data-control server owns and accesses it exclusively on its Celer worker.
class MetaCommittedViewCache {
 public:
  using Loader = std::function<MetaCommittedView()>;

  explicit MetaCommittedViewCache(Loader loader);
  std::shared_ptr<const MetaCommittedView> Adopt(MetaCommittedView view);
  absl::StatusOr<std::shared_ptr<const MetaCommittedView>> Get(
      std::uint64_t minimum_applied_index);

 private:
  Loader loader_;
  std::shared_ptr<const MetaCommittedView> cached_;
};

// Transfer chunks consult this predicate before rebuilding a node projection.
// Recording an equivalent committed view makes the remaining chunks O(1)
// until either the callback cursor or coordinator high-water advances again.
bool TransferBoundaryNeedsProjectionValidation(
    std::uint64_t published_index, std::uint64_t committed_high_water,
    std::uint64_t validated_index) noexcept;
void RecordEquivalentTransferBoundary(std::uint64_t applied_index,
                                      std::uint64_t* validated_index) noexcept;

enum class MetaPublisherTransferDisposition : std::uint8_t {
  kApplied,
  kRetryBeforeApplyInSession,
  kAwaitExactAppliedAndRetryInSession,
};

// Once a direct FDS frame or TransferEnd is visible, Data may already be
// installing it and its exact FullStateApplied remains part of the stream.
// Earlier supersession aborts an active object, if any. Both paths retry on the
// authenticated session rather than converting projection churn into a node
// disconnect.
MetaPublisherTransferDisposition ClassifyPublisherSupersession(
    bool receiver_can_apply) noexcept;

// Extracts the exact candidate action, if any, from the already-applied FDS
// that underlies a node heartbeat. The authenticated session supplies `boot`;
// Data cannot claim this marker in the heartbeat wire payload.
absl::StatusOr<std::optional<MetaObservedFailoverProjection>>
FailoverProjectionForHeartbeat(
    const cluster::control::FullDesiredState& installed,
    std::string_view node_id, const MetaBootIncarnation& boot);

}  // namespace detail

struct MetaHeartbeatObservationResult {
  cluster::control::ObservationStatus status =
      cluster::control::ObservationStatus::kRejected;
  std::string detail;
};

// Processes one authenticated heartbeat under a single observation-store lock.
// Boot and health are admitted independently. After identity/current-generation
// admission, candidate and transition evidence are replace-or-clear, so
// absence or component rejection clears the matching prior fact; rejecting a
// stale session identity leaves replacement-session state untouched. The
// longest overload also replaces the trusted candidate-action marker derived
// from the exact installed FDS; nullopt clears that marker. The shorter
// overloads intentionally clear failover evidence and/or the marker they
// cannot supply. Reporter history comes from ClientHello, while candidate
// payloads carry their independent rebuild-source lineage. The aggregate
// result reports any component rejection without rolling back valid
// boot/health data.
MetaHeartbeatObservationResult IngestHeartbeatObservations(
    MetaObservationStore& observations, const MetaCommittedFacts& facts,
    std::string_view node_id, const MetaBootIncarnation& boot,
    const MetaReplicationHistoryId& session_history, std::uint64_t generation,
    const cluster::control::HeartbeatHealth& health,
    const cluster::control::HeartbeatRoleInformation& role_information,
    std::int64_t now_unix_ms);
MetaHeartbeatObservationResult IngestHeartbeatObservations(
    MetaObservationStore& observations, const MetaCommittedFacts& facts,
    std::string_view node_id, const MetaBootIncarnation& boot,
    const MetaReplicationHistoryId& session_history, std::uint64_t generation,
    const cluster::control::HeartbeatHealth& health,
    const cluster::control::HeartbeatRoleInformation& role_information,
    const std::optional<cluster::control::FailoverObservation>&
        failover_observation,
    std::int64_t now_unix_ms);
MetaHeartbeatObservationResult IngestHeartbeatObservations(
    MetaObservationStore& observations, const MetaCommittedFacts& facts,
    std::string_view node_id, const MetaBootIncarnation& boot,
    const MetaReplicationHistoryId& session_history, std::uint64_t generation,
    const cluster::control::HeartbeatHealth& health,
    const cluster::control::HeartbeatRoleInformation& role_information,
    const std::optional<cluster::control::FailoverObservation>&
        failover_observation,
    std::optional<MetaObservedFailoverProjection> failover_projection,
    std::int64_t now_unix_ms);

// Validates a typed operation-evidence envelope against the authenticated
// session, then ingests it as volatile leader-local evidence. The reporter
// node is derived from the connection; self-reported boot, assignment,
// operation, population, history, and content hash are all exact anchors.
absl::Status IngestOperationEvidenceObservation(
    MetaObservationStore& observations, const MetaCommittedFacts& facts,
    std::string_view node_id, const MetaBootIncarnation& boot,
    std::uint64_t generation, const cluster::control::WireId128& session_id,
    const cluster::control::OperationEvidence& evidence,
    std::int64_t now_unix_ms);

struct MetaDataControlServerOptions {
  std::uint32_t server_id_ = 0;
  std::string bind_host_;
  std::uint16_t port_ = 0;
  // Process-local administrative TCP listener. Every process supplies one
  // even when it also exposes UDS; the durable advertised route may name a
  // proxy instead.
  std::string local_ctl_endpoint_;
  std::shared_ptr<MetaDataControlRuntimeStatus> runtime_status_;

  // Data control deliberately reuses the Meta Raft identity. An empty triple
  // selects explicitly trusted plaintext; a partial triple is invalid.
  std::string tls_ca_cert_file_;
  std::string tls_cert_file_;
  std::string tls_key_file_;

  std::uint32_t heartbeat_interval_ms_ = 1000;
  std::uint32_t observation_ttl_ms_ = 30000;
  std::uint32_t session_progress_timeout_ms_ = 10000;
  // Upper bound supplied by process assembly from NuRaft's configured
  // leadership-expiry window. A committed grant may request less.
  std::uint32_t leadership_validity_ms_ = 0;
  // Added to the maximum prior lease before a replacement authority may be
  // granted. Process assembly supplies at least one full maximum-lease window,
  // making quarantine Q >= 2D for maximum lease D. Thus an old Data clock has
  // advanced at least D when the Meta clock has advanced Q, provided Meta's
  // suspend-aware clock runs no more than twice as fast as Data's. Scheduling
  // can only delay a grant, and Data rechecks the same suspend-aware deadline
  // synchronously before every client mutation. The deliberately loose 2:1
  // rate bound is derived from D rather than an unrelated millisecond guess.
  std::uint32_t lease_handoff_safety_margin_ms_ = 0;
  // Before a leader claims a committed node's single session slot (or a
  // follower finishes its redirect), there is no durable owner with which to
  // deduplicate a socket. The default equals the maximum projected Data-node
  // population, and validation forbids raising it beyond that domain cap.
  std::size_t max_pending_handshakes_ = cluster::control::kMaxProjectedNodes;
  // Four frame-sized lanes: authority, reliable, bulk, and soft. Large
  // objects stream one frame at a time and do not consume an object-sized
  // allocation here.
  std::size_t max_write_queue_bytes_ = 4 * cluster::control::kMaxFrameBytes;
  // Two projection generations may coexist during atomic replacement. Each
  // gets two canonical-FDS size classes: one for encoded bytes and one for
  // the owning decoded graph. Exact retained capacities are charged, so a
  // pathologically structural object may still be rejected below its wire
  // cap instead of escaping this process-wide bound.
  std::size_t max_retained_projection_bytes_ =
      4 * static_cast<std::size_t>(cluster::control::kMaxFullDesiredStateBytes);
};

struct MetaDataControlMetricsSnapshot {
  // Includes handshaking and follower-redirect tasks. Shutdown reaches zero
  // only as every SessionLoop frame is destroyed, including a task rejected
  // before its coroutine body starts.
  std::uint64_t live_session_tasks_ = 0;
  // Sessions bound to an accepted leadership generation, including initial
  // FDS handshakes not yet counted in active_sessions_.
  std::uint64_t live_authority_session_tasks_ = 0;
  // Leader-start membership reconciliation producers. Demotion and shutdown
  // join these before releasing MetaLeaderContext.
  std::uint64_t live_leader_tasks_ = 0;
  std::uint64_t active_sessions_ = 0;
  std::uint64_t accepted_sessions_ = 0;
  std::uint64_t redirected_sessions_ = 0;
  std::uint64_t rejected_sessions_ = 0;
  std::uint64_t protocol_errors_ = 0;
  std::uint64_t full_states_sent_ = 0;
  std::uint64_t lease_grants_ = 0;
  std::uint64_t lease_denials_ = 0;
  std::uint64_t observations_accepted_ = 0;
  std::uint64_t observations_rejected_ = 0;
  std::uint64_t directive_results_committed_ = 0;
};

// Pure lease-decision inputs. Keeping this policy separate from transport is
// what lets tests prove that a malformed challenge cannot suppress the
// heartbeat's independent observation path.
struct MetaLeaseEvaluation {
  bool leader_valid_ = false;
  std::uint32_t server_id_ = 0;
  std::uint64_t raft_term_ = 0;
  std::uint64_t leadership_generation_ = 0;
  std::uint32_t leadership_validity_ms_ = 0;
  std::string node_id_;
  std::string boot_id_;
  cluster::control::WireHash256 applied_projection_hash_{};
  const cluster::control::FullDesiredState* desired_ = nullptr;
};

// Evaluates only the optional lease challenge. Heartbeat observation
// admission is intentionally performed by the caller before invoking this
// function and its status never changes that observation verdict.
cluster::control::LeaseDecision EvaluateLeaseChallenge(
    const std::optional<cluster::control::LeaseChallenge>& challenge,
    const cluster::control::HeartbeatHealth& health,
    const MetaLeaseEvaluation& evaluation);

// Volatile leader-local exclusion barrier between successive authority
// holders. The first otherwise-valid grant for a group/boot/anchor identity
// starts a maximum prior lease plus explicit safety-margin quarantine; only
// the same identity observed at or after that suspend-aware deadline may pass.
// Resetting leadership clears all evidence and therefore conservatively starts
// a fresh quarantine.
//
// This state is deliberately not durable: after process or leader restart a
// full new wait is safer than recovering a wall-clock deadline whose elapsed
// time cannot be trusted. Callers must serialize access on the Meta control
// worker.
class MetaLeaseHandoffGuard {
 public:
  MetaLeaseHandoffGuard(std::uint32_t maximum_prior_lease_ms,
                        std::uint32_t safety_margin_ms)
      : quarantine_ms_(static_cast<std::uint64_t>(maximum_prior_lease_ms) +
                       safety_margin_ms) {}

  cluster::control::LeaseDecision Enforce(
      cluster::control::LeaseDecision decision, std::string_view node_id,
      std::int64_t now_lease_clock_ms);
  void Reset() noexcept { entries_.clear(); }

 private:
  struct Entry {
    cluster::control::WireAuthorityAnchor authority_;
    std::string node_id_;
    std::string data_boot_id_;
    std::uint64_t leadership_generation_ = 0;
    std::int64_t eligible_after_ms_ = 0;
  };

  std::uint64_t quarantine_ms_ = 0;
  std::vector<Entry> entries_;
};

enum class MetaLeaderRuntimeDisposition : std::uint8_t {
  kEligible,
  kQuarantined,
  kQuarantineStarted,
};

// Suspend-aware validity barrier layered over NuRaft's active-monotonic
// leadership expiry. A host pause can let another Meta member win an election
// while the old process's CLOCK_MONOTONIC-based peer timers stand still. Once
// CLOCK_BOOTTIME has advanced by one leadership-validity window beyond the
// active clock, authority remains quarantined until the old process itself has
// run for one full validity window. That active interval gives NuRaft's peer
// liveness check time to expire or observe the newer term before this process
// can issue another authority-bearing message.
//
// Callers serialize this volatile state on the Meta control worker. Clock
// values need only share their own domains; neither epoch is compared with the
// other. Reset begins a new genuine leadership generation.
class MetaLeaderRuntimeGuard {
 public:
  explicit MetaLeaderRuntimeGuard(std::uint32_t leadership_validity_ms)
      : leadership_validity_ms_(leadership_validity_ms) {}

  void Reset(std::int64_t now_suspend_clock_ms,
             std::int64_t now_active_clock_ms) noexcept;
  MetaLeaderRuntimeDisposition Observe(
      std::int64_t now_suspend_clock_ms,
      std::int64_t now_active_clock_ms) noexcept;

 private:
  std::uint64_t leadership_validity_ms_ = 0;
  std::int64_t baseline_suspend_clock_ms_ = 0;
  std::int64_t baseline_active_clock_ms_ = 0;
  std::int64_t eligible_active_clock_ms_ = 0;
  bool initialized_ = false;
  bool quarantined_ = false;
};

// Builds the redirect/Hello directory solely from committed, active Meta
// member records. Malformed committed endpoints fail closed.
absl::StatusOr<std::vector<cluster::control::WireMetaEndpoint>>
BuildCommittedMetaDirectory(const MetaCommittedView& view);

enum class MetaDirectiveDelivery : std::uint8_t {
  kFrame,
  kTransfer,
};

// Classifies the encoded live envelope, not just its opaque payload. This
// keeps a directive just over the frame boundary from being rejected by the
// normal writer while preserving the same canonical Directive codec inside a
// streamed object.
absl::StatusOr<MetaDirectiveDelivery> ClassifyDirectiveDelivery(
    const cluster::control::WireProjectedDirective& directive,
    const cluster::control::WireId128& session_id);

// Returns installed local-primary authority anchors that no longer exist
// unchanged in `latest`, excluding anchors already fenced in this session.
// A null latest projection means the node was removed or cannot be projected,
// so every installed local authority is superseded.
std::vector<cluster::control::WireAuthorityAnchor>
UnfencedSupersededAuthorities(
    const cluster::control::FullDesiredState& installed,
    const cluster::control::FullDesiredState* latest, std::string_view node_id,
    std::span<const cluster::control::WireAuthorityAnchor> already_fenced);

enum class MetaReplacementDisposition : std::uint8_t {
  kContinue,
  kAbortSuperseded,
};

// Compares the semantic projection being transferred with the newest atomic
// committed projection. A superseded object must not reach directive dispatch:
// abort it while it is still incomplete, or consume its exact Applied before
// publishing the latest replacement if Data could already install it.
MetaReplacementDisposition EvaluateReplacementDisposition(
    const cluster::control::FullDesiredState& replacement,
    const cluster::control::FullDesiredState& latest);

// Volatile per-session dispatch and receipt state. Rebuild resets the accepted
// identities to the newly installed projection. A semantic no-op commit keeps
// this state so a sender interrupted at the commit boundary resumes only work
// that was never completely written. Observe accepts exact stage replay and
// either Accepted -> Started -> Completed for admitted work or
// Accepted -> Completed for a pre-start rejection. ValidateResult binds the
// terminal status to the path actually observed on this session.
class MetaDirectiveReceiptTracker {
 public:
  absl::Status Rebuild(
      std::span<const cluster::control::WireProjectedDirective> directives,
      std::string_view node_id, std::string_view boot_id);

  // Dispatch is recorded only after the complete frame or object transfer has
  // been written. Unknown identities fail closed because they cannot belong to
  // the installed projection for this node incarnation.
  absl::StatusOr<bool> NeedsDispatch(
      const cluster::control::WireDirectiveIdentity& identity) const;
  absl::Status MarkDispatched(
      const cluster::control::WireDirectiveIdentity& identity);
  bool HasUndispatched() const noexcept;

  absl::Status Observe(const cluster::control::WireDirectiveIdentity& identity,
                       cluster::control::DirectiveReceiptStage stage);
  absl::Status ValidateResult(
      const cluster::control::WireDirectiveIdentity& identity,
      cluster::control::DirectiveResultStatus status) const;
  std::size_t size() const noexcept { return entries_.size(); }

 private:
  struct Entry {
    cluster::control::WireDirectiveIdentity identity_;
    std::uint8_t stage_ = 0;
    bool dispatched_ = false;
    bool started_ = false;
  };
  std::vector<Entry> entries_;
};

class MetaDataControlServer final : public MetaReconciler {
 public:
  // Opaque shared state is public only so translation-unit helpers can name
  // it; callers receive no definition and cannot inspect it.
  struct Core;

  static absl::Status ValidateOptions(
      const MetaDataControlServerOptions& options);

  // The coordinator is retained by reference and must outlive every session
  // and leader task, through a completed Shutdown/CancelAndWait drain.
  static absl::StatusOr<std::shared_ptr<MetaDataControlServer>> Create(
      celer::ForeignExecutor foreign_executor,
      nuraft::ptr<nuraft::raft_server> server, MetaCoordinator& coordinator,
      std::shared_ptr<MetaObservationStore> observations,
      MetaDataControlServerOptions options);

  // Destruction performs the same blocking drain as Shutdown. Unless a prior
  // Shutdown completed, destroy this object outside its owning Celer worker
  // while that worker's executor can still make progress.
  ~MetaDataControlServer() override;
  MetaDataControlServer(const MetaDataControlServer&) = delete;
  MetaDataControlServer& operator=(const MetaDataControlServer&) = delete;

  // Listener lifecycle. Start binds once and accepts on leaders and
  // followers; Shutdown synchronously closes ingress and live sessions. It
  // must run outside the owning Celer worker while that worker's executor can
  // still make progress. An executor rejection before the drain completes is
  // fail-stop because returning would falsely advertise a safe process-
  // teardown boundary.
  void StartListener();
  void Shutdown();
  absl::Status status() const;
  MetaDataControlMetricsSnapshot metrics() const noexcept;

  // MetaReconciler: Start is called only after leader state-machine catch-up.
  // CancelAndWait does not return until the worker has revoked the context,
  // closed and joined every authority-bearing session from that leadership
  // epoch, and joined its leader-scoped tasks.
  // It blocks and must run outside the owning Celer worker while that worker's
  // executor can still make progress. Failure to deliver either leader edge is
  // fail-stop; after a completed full Shutdown the cancellation barrier is
  // already satisfied and becomes a no-op.
  void Start(MetaLeaderContext& context) override;
  void CancelAndWait() override;

 private:
  friend class MetaDataControlServerTestPeer;
  class SessionConnectionBorrow;
  using CorePtr = std::shared_ptr<Core>;

  explicit MetaDataControlServer(CorePtr core) : core_(std::move(core)) {}

  // Friend-only deterministic harness for the lifecycle failure policy. It
  // never enters production assembly and avoids requiring tests to induce
  // allocation failure in ForeignExecutor::Notify.
  static std::shared_ptr<MetaDataControlServer> LifecycleHarnessForTest(
      celer::ForeignExecutor foreign_executor, bool shutdown_complete);

  // Shared by the public leader callback and the rejected-executor lifecycle
  // harness. Tests pass null only with an executor that cannot accept the
  // closure, so no synthetic MetaLeaderContext is needed to cover fail-stop.
  void StartOnExecutor(MetaLeaderContext* context);

  static celer::Task<absl::Status> AcceptLoop(CorePtr core);
  static celer::Task<absl::Status> SessionLoop(
      CorePtr core, celer::TcpStream stream, celer::Connection* connection,
      detail::PendingHandshakeLimiter::Permit handshake_permit,
      SessionConnectionBorrow borrow);

  CorePtr core_;
};

}  // namespace keylane::meta
