#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "keylane/replication.h"
#include "keylane/storage/format.h"

namespace keylane {

struct ServerOptions {
  std::string bind_ip_ = "127.0.0.1";
  std::uint16_t port_ = 6379;
  std::uint16_t metrics_port_ = 0;
  unsigned thread_count_ = 1;
  int idle_timeout_ms_ = -1;
  unsigned recv_buffer_count_ = 1024;
  unsigned busy_poll_us_ = 0;
  std::size_t registered_buffer_bytes_ = 16ULL * 1024 * 1024;
  std::uint64_t max_memory_bytes_ = 0;
  std::size_t inline_key_max_bytes_ = storage::kDefaultInlineKeyBytes;
  std::uint32_t flush_max_ms_ = 1000;
  std::size_t flush_size_bytes_ = 8192ULL * 1024;
  bool verify_read_crc_ = true;
  std::vector<std::string> data_files_{"keylane.data"};
  std::uint32_t tomb_raider_interval_ms_ = 600'000;
  std::uint32_t tomb_raider_sleep_ms_ = 10;
  ReplicationOptions replication_options_;
};

int RunServer(ServerOptions options);

}  // namespace keylane
