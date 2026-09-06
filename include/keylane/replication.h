#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
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

struct ReplicaOfConfig {
  std::string host_;
  std::uint16_t port_ = 0;

  bool operator==(const ReplicaOfConfig&) const = default;
};

struct ReplicationOptions {
  // Cluster-managed replication is fail-closed and assigns this process to at
  // most one replication group. The manager must not infer standalone
  // REPLICAOF or import state as an activated cluster population.
  bool cluster_enabled_ = false;
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

// Typed current-boot result exposed to the future Meta control adapter. A
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
  // Nonempty exactly while state_ is kFailedStopped.
  std::string failure_reason_;
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

  // Applies one already-validated Meta full-rebuild directive to the single
  // local replication group. This is the #16 runtime orchestration seam: it
  // validates the complete identity and safe-source capability, forces a
  // fresh native FULL, and drives the existing reset/snapshot/tail/promote/
  // abort path. #20 supplies the control transport and calls this method.
  celer::Task<absl::Status> ApplyClusterRebuildDirective(
      ReplicaOfConfig upstream, RebuildDirective directive,
      PopulationManifest manifest);

  // Returns one coherent boot-scoped population snapshot for the future
  // control adapter.
  celer::Task<ClusterPopulationStatus> cluster_population_status() const;

  // Installs one safe-source authorization delivered by the future Meta
  // adapter. A cluster node exports a population only when it is itself ready
  // and activated as the local primary with no upstream, and the incoming
  // native handshake presents this exact rebuild identity. Revisions are
  // monotonic: a newer one revokes and joins older exports before becoming
  // active, and a revoked version cannot be replayed. #21 supplies the
  // primary-activation transition and its authority fence.
  celer::Task<absl::Status> AuthorizeClusterRebuildSource(
      RebuildDirective directive);

  // Revokes every downstream destructive-reset capability and reconnect lease
  // (for example, when this node loses primary authority). An accepted
  // directive remains the version watermark, preventing its replay after
  // revocation; revoking an empty ledger is an idempotent no-op. Standalone
  // managers reject this cluster-only transition without disturbing ordinary
  // downstream replication sessions.
  celer::Task<absl::Status> RevokeClusterRebuildSourceAuthorizations();

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
