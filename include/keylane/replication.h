#pragma once

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
#include "celer/net/tcp_stream.h"
#include "celer/runtime/task.h"
#include "keylane/replication_group.h"

namespace celer {
class TlsContext;
class Worker;
}  // namespace celer

namespace keylane::storage {
class StorageEngine;
}  // namespace keylane::storage

namespace keylane {

namespace detail {
class ClusterRebuildCompletionState;
}  // namespace detail

struct ReplicaOfConfig {
  std::string host_;
  std::uint16_t port_ = 0;

  bool operator==(const ReplicaOfConfig&) const = default;
};

struct ReplicationOptions {
  // Any Redis Cluster data plane disables standalone upstream control and
  // every Redis PSYNC export. Static topology also disables native export;
  // Meta-managed native export instead requires an exact population grant.
  bool cluster_enabled_ = false;
  // Meta-managed replication is fail-closed and assigns this process to at
  // most one replication group. It must not infer recovered storage as an
  // activated population; only NodeControl may install the boot-local proof.
  bool cluster_population_managed_ = false;
  // Set only by the Meta control adapter to its validated 160-bit data-node
  // identity. A missing value keeps standalone and static-file deployments on
  // a fresh CSPRNG identity for each process boot.
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
  std::shared_ptr<celer::TlsContext> tls_context_;
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
  // Nonempty exactly while state_ is kFailedStopped.
  std::string failure_reason_;
};

// FDS-owned subset of population identity. Assignment and immutable manifest
// plus the Meta partition-replication epoch decide whether a completed local
// population still belongs to the group; a term additionally scopes an
// in-progress attempt. BeginGroupTerm fences authority but does not mutate
// bytes, so a completed population may be re-anchored to a later committed
// term without another destructive rebuild.
struct DesiredClusterPopulation {
  std::string group_id_;
  std::string assignment_id_;
  std::uint64_t term_ = 0;
  std::uint64_t manifest_revision_ = 0;
  PopulationManifestId manifest_id_;
  std::uint64_t partition_replication_epoch_ = 0;
  // False means the FDS removed the live rebuild directive: an in-progress
  // attempt must retire even when its population identity still matches.
  bool rebuild_expected_ = false;

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

  celer::Task<absl::Status> Await() const;
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

// A source-history-local cut across Keylane's worker replication logs. Native
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
class ReplicationManager {
 public:
  ReplicationManager(storage::StorageEngine* storage,
                     ReplicationOptions options,
                     std::optional<ReplicaOfConfig> initial_upstream);
  ReplicationManager(const ReplicationManager&) = delete;
  ReplicationManager& operator=(const ReplicationManager&) = delete;
  ~ReplicationManager();

  void StorageReady(celer::Worker& worker);

  // The deep group interface: administrative changes enter as directives,
  // peer sockets enter through the handlers below, and Observe returns one
  // coherent control-plane snapshot without blocking the caller's runtime
  // worker while another worker updates the native session registry.
  celer::Task<absl::Status> ApplyDirective(ReplicationDirective directive);
  celer::Task<ReplicationStatus> Observe() const;

  // Copies the current node, boot, and local history identities without
  // collecting replication progress or downstream session status.
  celer::Task<ReplicationIdentity> ObserveIdentity() const;

  // Copies the desired upstream under the role-state lock. This endpoint is
  // not an admission proof; callers must retain their role and mode checks.
  std::optional<ReplicaOfConfig> upstream() const;

  // Starts one already-validated Meta full-rebuild directive and returns the
  // exact attempt's boot-local completion. Admission is not a terminal result:
  // callers that acknowledge a directive must Await() the returned handle.
  // Exact replay shares the original completion, while supersession resolves
  // the older handle only after cancellation/join/abort has finished.
  celer::Task<absl::StatusOr<ClusterRebuildCompletion>>
  StartClusterRebuildDirective(ReplicaOfConfig upstream,
                               RebuildDirective directive,
                               PopulationManifest manifest);

  // Convenience wrapper that starts and awaits one full rebuild. Production
  // NodeControl uses StartClusterRebuildDirective so wire admission and later
  // terminal observation remain distinct; this wrapper returns success only
  // after the exact attempt publishes its ReadyToken following promotion.
  celer::Task<absl::Status> ApplyClusterRebuildDirective(
      ReplicaOfConfig upstream, RebuildDirective directive,
      PopulationManifest manifest);

  // Process-shutdown barrier for Meta-managed target rebuilds. It closes the
  // native session immediately on worker zero, then returns only after the
  // coordinator has joined its flows and retired any partial candidate root.
  // This must run while the Celer runtime and StorageEngine are still alive.
  celer::Task<absl::Status> CancelClusterRebuildForShutdown();

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
  celer::Task<absl::Status> QuiesceForShutdown();

  // Reconciles the runtime-only target population with an installed FDS.
  // A mismatch (or null desired identity) closes serving immediately, joins
  // native flows, aborts a partial root, retires the ReadyToken/attempt, and
  // resolves its completion. Unlike shutdown cancellation, later directives
  // remain admissible.
  celer::Task<absl::Status> ReconcileClusterPopulation(
      std::optional<DesiredClusterPopulation> desired);

  // Transport loss cannot leave an unobserved destructive attempt running.
  // A completed Ready population is retained so reconnecting with the same
  // FDS does not force another full rebuild.
  celer::Task<absl::Status> CancelInProgressClusterPopulation();

  // Returns one coherent boot-scoped population snapshot for heartbeat
  // candidate reporting and directive validation.
  celer::Task<ClusterPopulationStatus> cluster_population_status() const;

  // Installs one safe-source authorization delivered through the node
  // controller. A cluster node exports a population only when it is itself
  // ready and activated as the local primary with no upstream, and the incoming
  // native handshake presents this exact rebuild identity. Revisions are
  // monotonic: a newer one revokes and joins older exports before becoming
  // active, and a revoked version cannot be replayed. Until a committed
  // primary-activation transition supplies its authority fence, export stays
  // fail-closed.
  celer::Task<absl::Status> AuthorizeClusterRebuildSource(
      RebuildDirective directive);

  // Revokes every downstream destructive-reset capability and reconnect lease
  // (for example, when this node loses primary authority). An accepted
  // directive remains the version watermark, preventing its replay after
  // revocation; revoking an empty ledger is an idempotent no-op. Standalone
  // managers reject this cluster-only transition without disturbing ordinary
  // downstream replication sessions.
  celer::Task<absl::Status> RevokeClusterRebuildSourceAuthorizations();

  // Joins capabilities inherited from a disconnected Meta control session
  // without advancing the committed revoke floor. The replacement session
  // may replay the exact current FDS only after the caller has prevented every
  // continuation from the old session from dispatching more directives.
  celer::Task<absl::Status>
  ClearClusterRebuildSourceAuthorizationsForSessionReplacement();

  // Current runtime settings; all mutations enter through ApplyDirective.
  unsigned snapshot_read_concurrency() const noexcept;
  std::size_t snapshot_batch_size() const noexcept;

  std::size_t backlog_size_bytes() const noexcept;
  bool backlog_backpressure() const noexcept;
  std::size_t publish_queue_bytes_per_worker() const noexcept;
  unsigned replica_priority() const noexcept;

  // KLPSYNC and KLFLOW arrive as RESP commands on the ordinary Redis port.
  static bool IsNativeHandshake(std::span<const std::string> args) noexcept;
  celer::Task<absl::Status> ServeNativeConnection(celer::TcpStream& stream,
                                                  std::vector<std::string> args,
                                                  std::uint64_t client_id,
                                                  std::string client_address,
                                                  bool tls);
  celer::Task<absl::Status> ServeRedisExportConnection(
      celer::TcpStream& stream, std::vector<std::string> args,
      std::uint64_t client_id, std::string client_address, bool tls,
      bool eof_capable);

  // Captures all source commands already queued on every worker. A missing
  // value means no native replication history is currently active; callers
  // may retry if they are waiting for a replica to connect.
  celer::Task<absl::StatusOr<std::optional<NativeReplicationWatermark>>>
  CaptureNativeReplicationWatermark();
  // Returns nullopt when the watermark belongs to an obsolete source history.
  // A replica counts only after every native flow acknowledges the cut.
  celer::Task<std::optional<std::uint64_t>> CountAcknowledgedNativeReplicas(
      const NativeReplicationWatermark& watermark) const;
  // The initial per-connection replication offset precedes every source
  // event, so every online native replica satisfies it without a log fence.
  celer::Task<std::uint64_t> CountOnlineNativeReplicas() const;
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
  // Native Keylane replicas participate in cluster-style redirection. A
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

}  // namespace keylane
