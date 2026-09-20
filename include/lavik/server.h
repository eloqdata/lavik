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

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "lavik/logging.h"
#include "lavik/memory.h"
#include "lavik/replication.h"
#include "lavik/resp.h"
#include "lavik/slowlog.h"
#include "lavik/storage/format.h"

namespace lavik {

inline constexpr long kDefaultMimallocPurgeDelayMs = 60'000;
inline constexpr std::uint64_t kDefaultMaxClients = 10'000;

struct ServerOptions {
  // Startup-only choices; compiled-in capabilities remain optional.
  std::string network_backend_ = "kernel";
  std::string storage_backend_ = "uring";
  std::string config_file_;
  LoggingOptions logging_;
  std::vector<std::string> bind_addresses_{"127.0.0.1"};
  std::uint16_t port_ = 6379;
  std::uint16_t tls_port_ = 0;
  std::uint16_t metrics_port_ = 0;
  std::string tls_cert_file_;
  std::string tls_key_file_;
  std::string tls_ca_cert_file_;
  std::string tls_auth_clients_ = "no";
  bool tls_replication_ = false;
  std::string requirepass_;
  std::string masteruser_ = "default";
  std::string masterauth_;
  unsigned thread_count_ = 1;
  std::uint64_t max_clients_ = kDefaultMaxClients;
  bool pin_workers_ = true;
  int idle_timeout_ms_ = -1;
  unsigned recv_buffer_count_ = 1024;
  unsigned busy_poll_us_ = 20;
  unsigned foreground_budget_us_ = 1000;
  unsigned background_budget_us_ = 10;
  unsigned background_warrant_percent_ = 1;
  unsigned spdk_max_completions_per_poll_ = 8;
  unsigned spdk_foreground_pre_poll_us_ = 5;
  long mimalloc_purge_delay_ms_ = kDefaultMimallocPurgeDelayMs;
  std::int64_t slowlog_log_slower_than_us_ = kDefaultSlowLogThresholdMicros;
  std::size_t slowlog_max_len_ = kDefaultSlowLogMaxLen;
  std::uint64_t lua_time_limit_ms_ = 5000;
  std::size_t registered_buffer_bytes_ = 64ULL * 1024 * 1024;
  unsigned storage_write_buffer_count_ = 4;
  std::size_t storage_read_buffer_bytes_ = 1ULL * 1024 * 1024;
  std::size_t replication_publish_queue_bytes_ = 16ULL * 1024 * 1024;
  std::uint64_t max_memory_bytes_ = 0;
  ClientBufferLimit maxmemory_clients_;
  std::size_t client_query_buffer_limit_bytes_ = kDefaultClientQueryBufferLimit;
  std::size_t inline_key_max_bytes_ = storage::kDefaultInlineKeyBytes;
  // Bound partial-block age without waiting for size-triggered submission.
  std::uint32_t flush_max_ms_ = 100;
  std::size_t flush_size_bytes_ = 128ULL * 1024;
  std::vector<std::string> data_files_{"lavik.data"};
  // Build and publish an index checkpoint after a clean shutdown drain. The
  // default keeps the existing recovery and shutdown cost unchanged.
  bool shutdown_checkpoint_ = false;
  // RDB output always uses a normal filesystem directory, independently of
  // whether data_files_ names regular files, block devices, or SPDK devices.
  std::string rdb_dir_{"."};
  std::string dbfilename_{"dump.rdb"};
  // One-shot logical import performed after storage recovery and before any
  // listener opens. The target Lavik dataset must be empty.
  std::string load_rdb_file_;
  // Explicitly discard every configured storage device before load-rdb.
  // This is destructive and is valid only when load_rdb_file_ is set.
  bool load_rdb_replace_ = false;
  std::uint32_t tomb_raider_interval_ms_ = 86'400'000;
  std::uint32_t tomb_raider_sleep_ms_ = 10;
  unsigned defrag_max_active_per_device_ = 8;
  std::uint32_t defrag_sleep_ms_ = 0;
  std::uint32_t defrag_record_sleep_us_ = 0;
  bool defrag_paused_ = false;
  std::optional<ReplicaOfConfig> replicaof_;
  // Redis Cluster data plane. All startup-only.
  bool cluster_enabled_ = false;
  // Meta is the only supported control source. The directory learned from
  // Meta is process-local and is never persisted by the data node.
  std::vector<std::string> cluster_meta_seeds_;
  // Stable 160-bit data-node identity. Boot/session incarnations remain
  // volatile and are generated separately.
  std::string cluster_node_id_;
  // Advertised client endpoints. 0 port = follow port_ / tls_port_; an empty
  // announce ip keeps the wildcard-bind startup-node convention for self.
  std::string cluster_announce_ip_;
  std::uint16_t cluster_announce_port_ = 0;
  std::uint16_t cluster_announce_tls_port_ = 0;
  // Explicit Redis PSYNC compatibility alias. It may name a standalone Redis
  // server or the first master of a Redis Cluster.
  std::optional<ReplicaOfConfig> redis_replicaof_;
  ReplicationOptions replication_options_;
};

// Selects the configured I/O backends before starting workers and blocks until
// shutdown. Call before other Bycorf I/O freezes the process-wide backend
// choice. Returns nonzero on configuration, initialization, or runtime failure.
int RunServer(ServerOptions options);

}  // namespace lavik
