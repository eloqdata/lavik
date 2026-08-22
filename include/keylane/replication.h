#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/status.h"
#include "celer/net/tcp_stream.h"
#include "celer/runtime/task.h"

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
  // Consulted only while this node has an upstream. REPLICAOF NO ONE makes
  // the node writable immediately.
  bool replica_read_only_ = true;
  // Advertised to the source during the version-1 control handshake so INFO
  // and CLUSTER NODES can identify the replica's Redis endpoint.
  std::uint16_t listen_port_ = 6379;
  bool use_tls_ = false;
  std::shared_ptr<celer::TlsContext> tls_context_;
  std::string masteruser_ = "default";
  std::string masterauth_;
  // Global in-memory history quota. Chunks are allocated lazily and distributed
  // across source-worker flows without multiplying this value by worker count.
  std::size_t backlog_size_bytes_ = 1ULL * 1024 * 1024 * 1024;
  // Bounded source publisher staging memory on each worker. A single larger
  // command may exceed this waterline only while it is the exclusive item.
  std::size_t publish_queue_bytes_per_worker_ = 16ULL * 1024 * 1024;
  // The initial upstream speaks Redis PSYNC rather than Keylane's native
  // multi-flow protocol. REPLICAOF NO ONE may later detach it.
  bool redis_psync_ = false;
};

inline constexpr unsigned kMaxReplicationSnapshotReadConcurrency = 128;

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

struct ReplicationStatus {
  ReplicationRole role_ = ReplicationRole::kMaster;
  std::optional<ReplicaOfConfig> upstream_;
  std::uint64_t role_epoch_ = 0;
  std::uint64_t session_id_ = 0;
  unsigned source_worker_count_ = 0;
  unsigned connected_flows_ = 0;
  std::string local_node_id_;
  std::string local_history_id_;
  std::optional<std::string> upstream_node_id_;
  std::optional<std::string> upstream_history_id_;
  std::vector<DownstreamReplicaStatus> downstream_replicas_;
};

// Owns replication role and connection lifetime. Replica connections are
// initiated on worker 0 for control and on one target worker per source flow.
// Source-side accepted flow sockets are adopted by the matching source worker.
class ReplicationManager {
 public:
  ReplicationManager(storage::StorageEngine* storage,
                     ReplicationOptions options,
                     std::optional<ReplicaOfConfig> initial_upstream);
  ReplicationManager(const ReplicationManager&) = delete;
  ReplicationManager& operator=(const ReplicationManager&) = delete;
  ~ReplicationManager();

  void StorageReady(celer::Worker& worker);

  // Atomically replaces the desired upstream. A null upstream implements
  // REPLICAOF NO ONE. Connection establishment continues asynchronously.
  celer::Task<absl::Status> SetUpstream(
      std::optional<ReplicaOfConfig> upstream);

  // Controls the number of snapshot value reads each source flow may keep in
  // flight during a full sync. The value is sampled for every snapshot batch,
  // so CONFIG SET takes effect without reconnecting the replica.
  absl::Status SetSnapshotReadConcurrency(unsigned concurrency) noexcept;
  unsigned snapshot_read_concurrency() const noexcept;

  // Changes the global in-memory backlog quota. Growth preserves the current
  // history; shrinkage may advance individual flow floors at event boundaries.
  celer::Task<absl::Status> SetBacklogSizeBytes(std::size_t bytes);
  std::size_t backlog_size_bytes() const noexcept;

  celer::Task<absl::Status> SetPublishQueueBytesPerWorker(std::size_t bytes);
  std::size_t publish_queue_bytes_per_worker() const noexcept;

  // KLPSYNC and KLFLOW arrive as RESP commands on the ordinary Redis port.
  static bool IsNativeHandshake(std::span<const std::string> args) noexcept;
  celer::Task<absl::Status> ServeNativeConnection(celer::TcpStream& stream,
                                                  std::vector<std::string> args,
                                                  std::uint64_t client_id,
                                                  std::string client_address,
                                                  bool tls);

  ReplicationStatus status() const;
  bool is_replica() const noexcept;
  bool is_loading() const noexcept;
  bool reject_writes() const noexcept;
  // Native Keylane replicas participate in cluster-style redirection. A
  // standalone Redis PSYNC follower instead serves its local read-only copy.
  bool redirects_clients_to_upstream() const noexcept;
  bool replica_read_only() const noexcept {
    return options_.replica_read_only_;
  }

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
  ReplicationOptions options_;
};

std::string_view ReplicationRoleName(ReplicationRole role) noexcept;

}  // namespace keylane
