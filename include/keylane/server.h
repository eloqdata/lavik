#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "keylane/logging.h"
#include "keylane/replication.h"
#include "keylane/slowlog.h"
#include "keylane/storage/format.h"

namespace keylane {

inline constexpr long kDefaultMimallocPurgeDelayMs = 60'000;
inline constexpr std::uint64_t kDefaultMaxClients = 10'000;

struct ServerOptions {
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
  std::size_t registered_buffer_bytes_ = 256ULL * 1024 * 1024;
  unsigned storage_write_buffer_count_ = 4;
  std::size_t storage_read_buffer_bytes_ = 1ULL * 1024 * 1024;
  std::size_t replication_publish_queue_bytes_ = 16ULL * 1024 * 1024;
  std::uint64_t max_memory_bytes_ = 0;
  std::size_t inline_key_max_bytes_ = storage::kDefaultInlineKeyBytes;
  std::uint32_t flush_max_ms_ = 1000;
  std::size_t flush_size_bytes_ = 128ULL * 1024;
  std::vector<std::string> data_files_{"keylane.data"};
  // RDB output always uses a normal filesystem directory, independently of
  // whether data_files_ names regular files, block devices, or SPDK devices.
  std::string rdb_dir_{"."};
  std::string dbfilename_{"dump.rdb"};
  // One-shot logical import performed after storage recovery and before any
  // listener opens. The target Keylane dataset must be empty.
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
  // Explicit Redis PSYNC compatibility alias. It may name a standalone Redis
  // server or the first master of a Redis Cluster.
  std::optional<ReplicaOfConfig> redis_replicaof_;
  ReplicationOptions replication_options_;
};

int RunServer(ServerOptions options);

}  // namespace keylane
