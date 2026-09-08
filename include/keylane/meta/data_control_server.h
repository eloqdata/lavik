#pragma once

// Process-lifetime Meta -> Data control listener and its leader-scoped
// publisher. The listener remains bound on followers so seeds can always
// return the committed Meta directory; only the reconciler installed through
// MetaCoordinator::RunAsLeader may create authority-bearing sessions.

#include <cstddef>
#include <cstdint>
#include <functional>
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

}  // namespace detail

struct MetaHeartbeatObservationResult {
  cluster::control::ObservationStatus status =
      cluster::control::ObservationStatus::kRejected;
  std::string detail;
};

// Ingests the independently useful boot, health, and optional candidate
// reports carried by one authenticated heartbeat at one receive timestamp.
// Candidate history is bound to the history declared by ClientHello for this
// session. A rejected candidate never rolls back the boot or health reports;
// status and detail aggregate all rejected components for the wire Ack.
MetaHeartbeatObservationResult IngestHeartbeatObservations(
    MetaObservationStore& observations, const MetaCommittedFacts& facts,
    std::string_view node_id, const MetaBootIncarnation& boot,
    const MetaReplicationHistoryId& session_history, std::uint64_t generation,
    const cluster::control::HeartbeatHealth& health,
    const std::optional<cluster::control::CandidateProgress>& candidate,
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
  // Four frame-sized lanes: authority, reliable, bulk, and soft. Large
  // objects stream one frame at a time and do not consume an object-sized
  // allocation here.
  std::size_t max_write_queue_bytes_ = 4 * cluster::control::kMaxFrameBytes;
};

struct MetaDataControlMetricsSnapshot {
  // Includes handshaking and follower-redirect tasks. Shutdown reaches zero
  // only after every SessionLoop has run its completion path.
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
// starts a full maximum-lease quarantine; only the same identity observed at
// or after that monotonic deadline may pass. Resetting leadership clears all
// evidence and therefore conservatively starts a fresh quarantine.
//
// This state is deliberately not durable: after process or leader restart a
// full new wait is safer than recovering a wall-clock deadline whose elapsed
// time cannot be trusted. Callers must serialize access on the Meta control
// worker.
class MetaLeaseHandoffGuard {
 public:
  explicit MetaLeaseHandoffGuard(std::uint32_t maximum_prior_lease_ms)
      : maximum_prior_lease_ms_(maximum_prior_lease_ms) {}

  cluster::control::LeaseDecision Enforce(
      cluster::control::LeaseDecision decision, std::string_view node_id,
      std::int64_t now_monotonic_ms);
  void Reset() noexcept { entries_.clear(); }

 private:
  struct Entry {
    cluster::control::WireAuthorityAnchor authority_;
    std::string node_id_;
    std::string data_boot_id_;
    std::uint64_t leadership_generation_ = 0;
    std::int64_t eligible_after_ms_ = 0;
  };

  std::uint32_t maximum_prior_lease_ms_ = 0;
  std::vector<Entry> entries_;
};

// Builds the redirect/Hello directory solely from committed, active Meta
// member records. Malformed committed endpoints fail closed.
absl::StatusOr<std::vector<cluster::control::WireMetaEndpoint>>
BuildCommittedMetaDirectory(const MetaCommittedView& view);

enum class MetaLocalMemberBindingDisposition : std::uint8_t {
  kAlreadyBound,
  kNeedsBind,
};

// Pure leader-start decision for the local bootstrap member. Missing state is
// repairable by one idempotent BindMetaMember proposal; retirement or a
// principal/endpoint conflict is terminal and remains fail-closed.
absl::StatusOr<MetaLocalMemberBindingDisposition>
EvaluateLocalMetaMemberBinding(const MetaCommittedView& view,
                               std::uint32_t server_id,
                               std::string_view principal,
                               std::string_view data_control_endpoint);

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
// committed projection. A superseded object must not reach its apply wait or
// directive dispatch: abort it while active, or close the session if its End
// frame has already committed at the receiver.
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

  static absl::StatusOr<std::shared_ptr<MetaDataControlServer>> Create(
      celer::ForeignExecutor foreign_executor,
      nuraft::ptr<nuraft::raft_server> server, MetaCoordinator& coordinator,
      std::shared_ptr<MetaObservationStore> observations,
      MetaDataControlServerOptions options);

  ~MetaDataControlServer() override;
  MetaDataControlServer(const MetaDataControlServer&) = delete;
  MetaDataControlServer& operator=(const MetaDataControlServer&) = delete;

  // Listener lifecycle. Start binds once and accepts on leaders and
  // followers; Shutdown synchronously closes ingress and live sessions. An
  // executor rejection before that drain completes is fail-stop because
  // returning would falsely advertise a safe process-teardown boundary.
  void StartListener();
  void Shutdown();
  absl::Status status() const;
  MetaDataControlMetricsSnapshot metrics() const noexcept;

  // MetaReconciler: Start is called only after leader state-machine catch-up.
  // CancelAndWait does not return until the worker has revoked the context and
  // begun closing every authority-bearing session from that leadership epoch.
  // Failure to deliver either leader edge is fail-stop; after a completed full
  // Shutdown the cancellation barrier is already satisfied and becomes a
  // no-op.
  void Start(MetaLeaderContext& context) override;
  void CancelAndWait() override;

 private:
  friend class MetaDataControlServerTestPeer;
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
  static celer::Task<absl::Status> SessionLoop(CorePtr core,
                                               celer::TcpStream stream,
                                               celer::Connection* connection);

  CorePtr core_;
};

}  // namespace keylane::meta
