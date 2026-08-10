#include <mimalloc.h>

#include <charconv>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <utility>

#include "keylane/CLI11.hpp"
#include "keylane/server.h"

int main(int argc, char** argv) {
  // Compile-time defaults make THP and eager commit effective during
  // mimalloc's process constructor. Reassert all three before application
  // allocations.
  mi_option_set(mi_option_purge_delay, -1);
  mi_option_set(mi_option_arena_eager_commit, 1);
  mi_option_set(mi_option_allow_thp, 0);

  CLI::App app{"keylane — high-performance Redis-compatible storage"};

  keylane::ServerOptions options;
  unsigned registered_buffer_mb = 16;
  unsigned flush_size_kb = 8192;
  bool disable_read_crc = false;
  std::string replicate_to;

  app.add_option("-b,--bind", options.bind_ip_, "Bind address")
      ->capture_default_str();
  app.add_option("-p,--port", options.port_, "Listen port")
      ->capture_default_str();
  app.add_option("--metrics-port", options.metrics_port_,
                 "Prometheus HTTP listen port (0 disables)")
      ->capture_default_str()
      ->check(CLI::NonNegativeNumber);
  app.add_option("-t,--threads", options.thread_count_, "Worker thread count")
      ->capture_default_str()
      ->check(CLI::PositiveNumber);
  app.add_option("-i,--idle-timeout", options.idle_timeout_ms_,
                 "Idle timeout in ms (-1 = disabled)")
      ->capture_default_str();
  app.add_option("--recv-buffers", options.recv_buffer_count_,
                 "Multishot recv buffer-ring entries per worker (0 disables)")
      ->capture_default_str()
      ->check(CLI::NonNegativeNumber);
  app.add_option("--busy-poll-us", options.busy_poll_us_,
                 "Busy-poll CQ and cross-core mailboxes before parking")
      ->capture_default_str()
      ->check(CLI::NonNegativeNumber);
  app.add_option("--registered-buffer-mb", registered_buffer_mb,
                 "Registered storage buffer budget in MiB per worker")
      ->capture_default_str()
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
  app.add_flag("--disable-read-crc", disable_read_crc,
               "Skip payload CRC32C verification on GET reads");
  app.add_option("--tomb-raider-interval-ms", options.tomb_raider_interval_ms_,
                 "Interval between tombstone-reclaim disk sweeps (0 disables)")
      ->capture_default_str()
      ->check(CLI::NonNegativeNumber);
  app.add_option("--tomb-raider-sleep-ms", options.tomb_raider_sleep_ms_,
                 "Pause after each block the tombstone sweep reads")
      ->capture_default_str()
      ->check(CLI::NonNegativeNumber);
  app.add_option(
         "--data-file", options.data_files_,
         "Existing data file or block device; repeat for multiple paths")
      ->capture_default_str();
  app.add_option("--inline-key-max-bytes", options.inline_key_max_bytes_,
                 "Largest key retained complete in the in-memory index")
      ->check(CLI::Range(std::size_t{1}, keylane::storage::MaxInlineKeyBytes()))
      ->capture_default_str();
  app.add_option("--replication-port",
                 options.replication_options_.listen_port_,
                 "Internal replication listen port (0 disables receiver)")
      ->capture_default_str()
      ->check(CLI::NonNegativeNumber);
  app.add_option("--replicate-to", replicate_to,
                 "Static replica endpoint as IPv4:port");
  app.add_flag("--replica-read-only",
               options.replication_options_.replica_read_only_,
               "Reject mutating Redis commands on this replica");

  try {
    app.parse(argc, argv);
  } catch (const CLI::ParseError& e) {
    return app.exit(e);
  }

  constexpr std::size_t kMiB = 1024 * 1024;
  constexpr std::size_t kKiB = 1024;
  if (registered_buffer_mb > std::numeric_limits<std::size_t>::max() / kMiB ||
      flush_size_kb > std::numeric_limits<std::size_t>::max() / kKiB) {
    return 2;
  }
  options.registered_buffer_bytes_ =
      static_cast<std::size_t>(registered_buffer_mb) * kMiB;
  options.flush_size_bytes_ = static_cast<std::size_t>(flush_size_kb) * kKiB;
  options.verify_read_crc_ = !disable_read_crc;
  if (!replicate_to.empty()) {
    const std::size_t separator = replicate_to.rfind(':');
    unsigned parsed_port = 0;
    if (separator == std::string::npos || separator == 0 ||
        separator + 1 == replicate_to.size()) {
      return 2;
    }
    const char* begin = replicate_to.data() + separator + 1;
    const char* end = replicate_to.data() + replicate_to.size();
    auto [parsed_end, error] = std::from_chars(begin, end, parsed_port);
    if (error != std::errc{} || parsed_end != end || parsed_port == 0 ||
        parsed_port > std::numeric_limits<std::uint16_t>::max()) {
      return 2;
    }
    options.replication_options_.target_ip_ = replicate_to.substr(0, separator);
    options.replication_options_.target_port_ =
        static_cast<std::uint16_t>(parsed_port);
  }
  return keylane::RunServer(std::move(options));
}
