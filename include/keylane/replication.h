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
};

enum class ReplicationRole : std::uint8_t {
  kMaster,
  kConnecting,
  kHandshake,
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
  std::uint64_t generation_ = 0;
  std::uint64_t session_id_ = 0;
  unsigned source_worker_count_ = 0;
  unsigned connected_flows_ = 0;
  std::string local_node_id_;
  std::optional<std::string> upstream_node_id_;
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

  // KLPSYNC and KLFLOW arrive as RESP commands on the ordinary Redis port.
  static bool IsNativeHandshake(std::span<const std::string> args) noexcept;
  celer::Task<absl::Status> ServeNativeConnection(
      celer::TcpStream& stream, std::vector<std::string> args);

  ReplicationStatus status() const;
  bool is_replica() const noexcept;
  bool reject_writes() const noexcept;
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
