#include <charconv>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "keylane/CLI11.hpp"
#include "keylane/server.h"

int main(int argc, char** argv) {
  CLI::App app{"keylane — high-performance Redis-compatible storage"};

  std::string bind_ip = "127.0.0.1";
  std::uint16_t port = 6379;
  std::uint16_t metrics_port = 0;
  unsigned threads = 1;
  int idle_timeout_ms = -1;
  unsigned recv_buffer_count = 1024;
  unsigned busy_poll_us = 0;
  unsigned registered_buffer_mb = 16;
  std::uint64_t max_memory_bytes = 0;
  std::uint32_t flush_max_ms = 1000;
  unsigned flush_size_kb = 8192;
  bool disable_read_crc = false;
  std::uint32_t tomb_raider_interval_ms = 600'000;
  std::uint32_t tomb_raider_sleep_ms = 10;
  std::vector<std::string> data_files{"keylane.data"};
  std::uint16_t replication_port = 0;
  std::string replicate_to;
  bool replica_read_only = false;

  app.add_option("-b,--bind", bind_ip, "Bind address")->capture_default_str();
  app.add_option("-p,--port", port, "Listen port")->capture_default_str();
  app.add_option("--metrics-port", metrics_port,
                 "Prometheus HTTP listen port (0 disables)")
      ->capture_default_str()
      ->check(CLI::NonNegativeNumber);
  app.add_option("-t,--threads", threads, "Worker thread count")
      ->capture_default_str()
      ->check(CLI::PositiveNumber);
  app.add_option("-i,--idle-timeout", idle_timeout_ms,
                 "Idle timeout in ms (-1 = disabled)")
      ->capture_default_str();
  app.add_option("--recv-buffers", recv_buffer_count,
                 "Multishot recv buffer-ring entries per worker (0 disables)")
      ->capture_default_str()
      ->check(CLI::NonNegativeNumber);
  app.add_option("--busy-poll-us", busy_poll_us,
                 "Busy-poll CQ and cross-core mailboxes before parking")
      ->capture_default_str()
      ->check(CLI::NonNegativeNumber);
  app.add_option("--registered-buffer-mb", registered_buffer_mb,
                 "Registered storage buffer budget in MiB per worker")
      ->capture_default_str()
      ->check(CLI::PositiveNumber);
  app.add_option("--max-memory,--maxmemory", max_memory_bytes,
                 "Maximum process memory (0 uses 80% of memory capacity)")
      ->capture_default_str()
      ->transform(CLI::AsSizeValue(false));
  app.add_option("--flush-max-ms", flush_max_ms,
                 "Maximum age of a partial write block before flush")
      ->capture_default_str()
      ->check(CLI::PositiveNumber);
  app.add_option("--flush-size-kb", flush_size_kb,
                 "Maximum size of each storage write submission in KiB")
      ->capture_default_str()
      ->check(CLI::PositiveNumber);
  app.add_flag("--disable-read-crc", disable_read_crc,
               "Skip payload CRC32C verification on GET reads");
  app.add_option("--tomb-raider-interval-ms", tomb_raider_interval_ms,
                 "Interval between tombstone-reclaim disk sweeps (0 disables)")
      ->capture_default_str()
      ->check(CLI::NonNegativeNumber);
  app.add_option("--tomb-raider-sleep-ms", tomb_raider_sleep_ms,
                 "Pause after each block the tombstone sweep reads")
      ->capture_default_str()
      ->check(CLI::NonNegativeNumber);
  app.add_option(
         "--data-file", data_files,
         "Existing data file or block device; repeat for multiple paths")
      ->capture_default_str();
  app.add_option("--replication-port", replication_port,
                 "Internal replication listen port (0 disables receiver)")
      ->capture_default_str()
      ->check(CLI::NonNegativeNumber);
  app.add_option("--replicate-to", replicate_to,
                 "Static replica endpoint as IPv4:port");
  app.add_flag("--replica-read-only", replica_read_only,
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
  keylane::ReplicationOptions replication_options;
  replication_options.listen_port_ = replication_port;
  replication_options.replica_read_only_ = replica_read_only;
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
    replication_options.target_ip_ = replicate_to.substr(0, separator);
    replication_options.target_port_ = static_cast<std::uint16_t>(parsed_port);
  }
  return keylane::RunServer(
      bind_ip, port, metrics_port, threads, idle_timeout_ms, recv_buffer_count,
      busy_poll_us, static_cast<std::size_t>(registered_buffer_mb) * kMiB,
      max_memory_bytes, flush_max_ms,
      static_cast<std::size_t>(flush_size_kb) * kKiB, !disable_read_crc,
      data_files, tomb_raider_interval_ms, tomb_raider_sleep_ms,
      std::move(replication_options));
}
