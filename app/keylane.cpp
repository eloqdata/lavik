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
  unsigned threads = 1;
  int idle_timeout_ms = -1;
  unsigned recv_buffer_count = 1024;
  unsigned busy_poll_us = 0;
  unsigned registered_buffer_mb = 16;
  std::uint32_t flush_max_ms = 1000;
  bool disable_read_crc = false;
  std::vector<std::string> data_files{"keylane.data"};
  std::uint64_t data_file_size_mb = 1024;

  app.add_option("-b,--bind", bind_ip, "Bind address")->capture_default_str();
  app.add_option("-p,--port", port, "Listen port")->capture_default_str();
  app.add_option("-t,--threads", threads, "Worker thread count")
      ->capture_default_str()
      ->check(CLI::PositiveNumber);
  app.add_option("-i,--idle-timeout", idle_timeout_ms, "Idle timeout in ms (-1 = disabled)")
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
  app.add_option("--flush-max-ms", flush_max_ms,
                 "Maximum age of a partial write block before flush")
      ->capture_default_str()
      ->check(CLI::PositiveNumber);
  app.add_flag("--disable-read-crc", disable_read_crc,
               "Skip payload CRC32C verification on GET reads");
  app.add_option("--data-file", data_files,
                 "Data file path; repeat for multiple files")
      ->capture_default_str();
  app.add_option("--data-file-size-mb", data_file_size_mb,
                 "Preallocated size of each data file in MiB")
      ->capture_default_str()
      ->check(CLI::PositiveNumber);

  try {
    app.parse(argc, argv);
  } catch (const CLI::ParseError& e) {
    return app.exit(e);
  }

  constexpr std::size_t kMiB = 1024 * 1024;
  if (registered_buffer_mb > std::numeric_limits<std::size_t>::max() / kMiB ||
      data_file_size_mb >
          std::numeric_limits<std::uint64_t>::max() / kMiB) {
    return 2;
  }
  return keylane::RunServer(bind_ip, port, threads, idle_timeout_ms,
                            recv_buffer_count, busy_poll_us,
                            static_cast<std::size_t>(registered_buffer_mb) * kMiB,
                            flush_max_ms, !disable_read_crc,
                            data_files, data_file_size_mb * kMiB);
}
