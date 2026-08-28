#include <mimalloc.h>
#include <sched.h>

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "keylane/CLI11.hpp"
#include "keylane/config.h"
#include "keylane/logging.h"
#include "keylane/server.h"
#include "keylane/version.h"

namespace {

unsigned DefaultWorkerThreadCount() {
  cpu_set_t allowed;
  CPU_ZERO(&allowed);
  if (::sched_getaffinity(0, sizeof(allowed), &allowed) == 0) {
    const int count = CPU_COUNT(&allowed);
    if (count > 0) {
      return static_cast<unsigned>(count);
    }
  }
  const unsigned count = std::thread::hardware_concurrency();
  return count == 0 ? 1U : count;
}

}  // namespace

int main(int argc, char** argv) {
  // Compile-time defaults make THP and eager commit effective during
  // mimalloc's process constructor. Reassert both before application
  // allocations. Leave purge_delay at mimalloc's native default throughout
  // recovery; the configured online value is applied after recovery finishes.
  mi_option_set(mi_option_arena_eager_commit, 1);
  mi_option_set(mi_option_allow_thp, 0);

  CLI::App app{"keylane — high-performance Redis-compatible storage"};
  app.set_version_flag("--version",
                       "keylane " + std::string(keylane::kVersion));

  keylane::ServerOptions options;
  options.thread_count_ = DefaultWorkerThreadCount();
  std::string config_file;
  if (argc > 1 && argv[1][0] != '-') {
    config_file = argv[1];
    const absl::Status loaded =
        keylane::LoadRedisConfigFile(config_file, &options);
    if (!loaded.ok()) {
      std::cerr << "Configuration error: " << loaded.message() << '\n';
      return 1;
    }
  }
  unsigned registered_buffer_mb = static_cast<unsigned>(
      options.registered_buffer_bytes_ / (1024ULL * 1024));
  unsigned replication_publish_queue_mb = static_cast<unsigned>(
      options.replication_publish_queue_bytes_ / (1024ULL * 1024));
  unsigned storage_read_buffer_kb =
      static_cast<unsigned>(options.storage_read_buffer_bytes_ / 1024ULL);
  unsigned flush_size_kb = 128;
  std::vector<std::string> redis_replicaof_cli;

  app.add_option("config", config_file,
                 "Redis-style configuration file (must be the first argument)");
  app.add_option("-b,--bind", options.bind_addresses_,
                 "Bind address or hostname; repeat for multiple addresses")
      ->capture_default_str();
  app.add_option("-p,--port", options.port_, "Listen port")
      ->capture_default_str();
  app.add_option("--tls-port", options.tls_port_,
                 "TLS listen port (0 disables)")
      ->capture_default_str()
      ->check(CLI::NonNegativeNumber);
  app.add_option("--tls-cert-file", options.tls_cert_file_,
                 "TLS certificate chain PEM file");
  app.add_option("--tls-key-file", options.tls_key_file_,
                 "TLS private key PEM file");
  app.add_option("--tls-ca-cert-file", options.tls_ca_cert_file_,
                 "TLS trusted CA PEM file");
  app.add_option("--tls-auth-clients", options.tls_auth_clients_,
                 "TLS client certificate authentication: no, optional, yes")
      ->capture_default_str()
      ->check(CLI::IsMember({"no", "optional", "yes"}));
  app.add_flag("--tls-replication,!--no-tls-replication",
               options.tls_replication_,
               "Use TLS for outgoing replication connections")
      ->capture_default_str();
  app.add_option("--requirepass", options.requirepass_,
                 "Password required by AUTH");
  app.add_option("--masteruser", options.masteruser_,
                 "Username used to authenticate to the replication source")
      ->capture_default_str();
  app.add_option("--masterauth", options.masterauth_,
                 "Password used to authenticate to the replication source");
  app.add_option("--redis-replicaof", redis_replicaof_cli,
                 "Explicitly follow Redis using PSYNC: HOST PORT")
      ->expected(2);
  app.add_option("--metrics-port", options.metrics_port_,
                 "Prometheus HTTP listen port (0 disables)")
      ->capture_default_str()
      ->check(CLI::NonNegativeNumber);
  app.add_flag("--logtostderr,!--nologtostderr",
               options.logging_.log_to_stderr_,
               "Write logs only to stderr instead of log files")
      ->capture_default_str();
  app.add_flag("--alsologtostderr,!--noalsologtostderr",
               options.logging_.also_log_to_stderr_,
               "Write logs to stderr in addition to log files")
      ->capture_default_str();
  app.add_option("--log-dir,--log_dir", options.logging_.log_dir_,
                 "Directory containing keylane.log")
      ->capture_default_str();
  app.add_option("--max-log-size-mb,--max_log_size_mb",
                 options.logging_.max_log_size_mb_,
                 "Maximum size of each log file in MiB")
      ->capture_default_str()
      ->check(CLI::PositiveNumber);
  app.add_option("--max-log-files,--max_log_files",
                 options.logging_.max_log_files_,
                 "Maximum log files retained, including the active file")
      ->capture_default_str()
      ->check(CLI::PositiveNumber);
  app.add_option("-t,--threads", options.thread_count_, "Worker thread count")
      ->capture_default_str()
      ->check(CLI::PositiveNumber);
  app.add_option("--maxclients", options.max_clients_,
                 "Maximum concurrent client connections")
      ->capture_default_str()
      ->check(CLI::PositiveNumber);
  app.add_flag("--pin-workers,!--no-pin-workers", options.pin_workers_,
               "Pin workers one-to-one to CPUs in the inherited affinity mask")
      ->capture_default_str();
  app.add_option("-i,--idle-timeout", options.idle_timeout_ms_,
                 "Idle timeout in ms (-1 = disabled)")
      ->capture_default_str();
  app.add_option("--recv-buffers-per-worker", options.recv_buffer_count_,
                 "Multishot recv buffer-ring entries per worker (0 disables)")
      ->capture_default_str()
      ->check(CLI::NonNegativeNumber);
  app.add_option("--busy-poll-us", options.busy_poll_us_,
                 "Busy-poll CQ and cross-core mailboxes before parking")
      ->capture_default_str()
      ->check(CLI::NonNegativeNumber);
  app.add_option(
         "--slowlog-log-slower-than", options.slowlog_log_slower_than_us_,
         "Log commands slower than this many microseconds (-1 disables)")
      ->capture_default_str()
      ->check(CLI::Range(std::int64_t{-1},
                         std::numeric_limits<std::int64_t>::max()));
  app.add_option("--slowlog-max-len", options.slowlog_max_len_,
                 "Maximum number of slow commands retained")
      ->capture_default_str()
      ->check(CLI::NonNegativeNumber);
  app.add_option("--lua-time-limit,--busy-reply-threshold",
                 options.lua_time_limit_ms_,
                 "Milliseconds before a Lua script enters BUSY mode")
      ->capture_default_str()
      ->check(CLI::NonNegativeNumber);
  app.add_option("--foreground-budget-us", options.foreground_budget_us_,
                 "Maximum worker foreground slice in microseconds")
      ->capture_default_str()
      ->check(CLI::PositiveNumber);
  app.add_option("--background-budget-us", options.background_budget_us_,
                 "Maximum worker background slice in microseconds")
      ->capture_default_str()
      ->check(CLI::PositiveNumber);
  app.add_option(
         "--background-warrant-percent", options.background_warrant_percent_,
         "Maximum rolling worker CPU share guaranteed to background tasks")
      ->capture_default_str()
      ->check(CLI::Range(1U, 100U));
  app.add_option("--replication-snapshot-batch-size",
                 options.replication_options_.snapshot_batch_size_,
                 "Maximum keys processed by each full-sync scheduling round")
      ->capture_default_str()
      ->check(CLI::Range(std::size_t{1},
                         keylane::kMaxReplicationSnapshotBatchSize));
  app.add_flag("--redis-export-backpressure,!--no-redis-export-backpressure",
               options.replication_options_.redis_export_backpressure_,
               "Backpressure writes when a Redis PSYNC export falls behind")
      ->capture_default_str();
  app.add_option("--replica-priority",
                 options.replication_options_.replica_priority_,
                 "Redis Sentinel replica promotion priority (0 disables)")
      ->capture_default_str()
      ->check(CLI::NonNegativeNumber);
  app.add_option(
         "--spdk-max-completions-per-poll",
         options.spdk_max_completions_per_poll_,
         "Maximum SPDK completions processed per worker poll (0 = unlimited)")
      ->capture_default_str()
      ->check(CLI::NonNegativeNumber);
  app.add_option("--spdk-foreground-pre-poll-us",
                 options.spdk_foreground_pre_poll_us_,
                 "Foreground worker slice before each SPDK completion poll")
      ->capture_default_str()
      ->check(CLI::NonNegativeNumber);
  app.add_option("--mimalloc-purge-delay-ms", options.mimalloc_purge_delay_ms_,
                 "Online mimalloc purge delay in ms, applied after recovery "
                 "(-1 disables purging)")
      ->capture_default_str()
      ->check(CLI::Range(-1L, std::numeric_limits<long>::max()));
  app.add_option("--registered-buffer-mb-per-worker", registered_buffer_mb,
                 "Registered storage buffer budget in MiB per worker")
      ->capture_default_str()
      ->check(CLI::PositiveNumber);
  app.add_option("--storage-write-buffers-per-worker",
                 options.storage_write_buffer_count_,
                 "Registered storage write buffers per worker")
      ->capture_default_str()
      ->check(CLI::PositiveNumber);
  app.add_option("--storage-read-buffer-kb", storage_read_buffer_kb,
                 "Registered storage read payload size in KiB")
      ->capture_default_str()
      ->check(CLI::PositiveNumber);
  app.add_option("--replication-publish-queue-mb-per-worker",
                 replication_publish_queue_mb,
                 "Replication publisher staging budget in MiB per worker")
      ->capture_default_str()
      ->check(CLI::PositiveNumber);
  app.add_option("--repl-backlog-size",
                 options.replication_options_.backlog_size_bytes_,
                 "Global lazy in-memory replication backlog quota")
      ->capture_default_str()
      ->transform(CLI::AsSizeValue(false))
      ->check(CLI::PositiveNumber);
  app.add_option("--max-memory,--maxmemory", options.max_memory_bytes_,
                 "Maximum process memory (0 uses 80% of memory capacity)")
      ->capture_default_str()
      ->transform(CLI::AsSizeValue(false));
  app.add_option("--flush-max-ms", options.flush_max_ms_,
                 "Maximum age of a partial write block before flush")
      ->capture_default_str()
      ->check(CLI::PositiveNumber);
  app.add_option("--flush-size-kb", flush_size_kb,
                 "Maximum size of each storage write submission in KiB")
      ->capture_default_str()
      ->check(CLI::PositiveNumber);
  app.add_option("--tomb-raider-interval-ms", options.tomb_raider_interval_ms_,
                 "Interval between tombstone-reclaim disk sweeps (0 disables)")
      ->capture_default_str()
      ->check(CLI::NonNegativeNumber);
  app.add_option("--tomb-raider-sleep-ms", options.tomb_raider_sleep_ms_,
                 "Pause after each block the tombstone sweep reads")
      ->capture_default_str()
      ->check(CLI::NonNegativeNumber);
  app.add_option("--defrag-max-active-per-device",
                 options.defrag_max_active_per_device_,
                 "Maximum concurrent block relocations per storage device")
      ->capture_default_str()
      ->check(CLI::Range(1U, 8U));
  app.add_option("--defrag-sleep-ms", options.defrag_sleep_ms_,
                 "Asynchronous cooldown after each relocated block")
      ->capture_default_str()
      ->check(CLI::NonNegativeNumber);
  app.add_option("--defrag-record-sleep-us", options.defrag_record_sleep_us_,
                 "Asynchronous pause after each record examined by defrag")
      ->capture_default_str()
      ->check(CLI::NonNegativeNumber);
  app.add_flag("--defrag-paused", options.defrag_paused_,
               "Queue defrag candidates without running relocation jobs");
  app.add_option(
         "--data-file", options.data_files_,
         "Existing data file or block device; repeat for multiple paths")
      ->capture_default_str();
  app.add_option("--rdb-dir", options.rdb_dir_,
                 "Filesystem directory for Redis-compatible RDB backups")
      ->capture_default_str();
  app.add_option("--dbfilename", options.dbfilename_,
                 "Filename written by SAVE/BGSAVE inside --rdb-dir")
      ->capture_default_str();
  app.add_option("--load-rdb", options.load_rdb_file_,
                 "Import a complete Redis RDB into an empty dataset before "
                 "opening listeners");
  app.add_flag("--load-rdb-replace", options.load_rdb_replace_,
               "Erase all configured data files before importing --load-rdb");
  app.add_option("--inline-key-max-bytes", options.inline_key_max_bytes_,
                 "Largest key retained complete in the in-memory index")
      ->check(CLI::Range(std::size_t{1}, keylane::storage::MaxInlineKeyBytes()))
      ->capture_default_str();
  try {
    app.parse(argc, argv);
  } catch (const CLI::ParseError& e) {
    return app.exit(e);
  }
  if (!config_file.empty() && config_file != options.config_file_) {
    std::cerr << "Configuration error: the configuration file must be the "
                 "first argument\n";
    return 2;
  }
  if (!redis_replicaof_cli.empty()) {
    const absl::Status configured = keylane::ApplyRedisConfigDirective(
        {"redis-replicaof", redis_replicaof_cli[0], redis_replicaof_cli[1]},
        &options);
    if (!configured.ok()) {
      std::cerr << "Configuration error: " << configured.message() << '\n';
      return 2;
    }
  }
  const absl::Status validated = keylane::ValidateServerOptions(options);
  if (!validated.ok()) {
    std::cerr << "Configuration error: " << validated.message() << '\n';
    return 2;
  }

  constexpr std::size_t kMiB = 1024 * 1024;
  constexpr std::size_t kKiB = 1024;
  if (registered_buffer_mb > std::numeric_limits<std::size_t>::max() / kMiB ||
      replication_publish_queue_mb >
          std::numeric_limits<std::size_t>::max() / kMiB ||
      storage_read_buffer_kb > std::numeric_limits<std::size_t>::max() / kKiB ||
      flush_size_kb > std::numeric_limits<std::size_t>::max() / kKiB) {
    return 2;
  }
  options.registered_buffer_bytes_ =
      static_cast<std::size_t>(registered_buffer_mb) * kMiB;
  options.replication_publish_queue_bytes_ =
      static_cast<std::size_t>(replication_publish_queue_mb) * kMiB;
  options.storage_read_buffer_bytes_ =
      static_cast<std::size_t>(storage_read_buffer_kb) * kKiB;
  options.flush_size_bytes_ = static_cast<std::size_t>(flush_size_kb) * kKiB;
  const absl::Status logging_status =
      keylane::InitializeLogging(options.logging_);
  if (!logging_status.ok()) {
    std::cerr << "Logging error: " << logging_status.message() << '\n';
    return 1;
  }
  const int exit_code = keylane::RunServer(std::move(options));
  keylane::ShutdownLogging();
  return exit_code;
}
