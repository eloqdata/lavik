#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "keylane/replication.h"

namespace keylane {

int RunServer(std::string_view bind_ip, std::uint16_t port, unsigned thread_count,
              int idle_timeout_ms, unsigned recv_buffer_count,
              unsigned busy_poll_us,
              std::size_t registered_buffer_bytes,
              std::uint32_t flush_max_ms, std::size_t flush_size_bytes,
              bool verify_read_crc,
              const std::vector<std::string>& data_files,
              std::uint64_t data_file_size_bytes,
              ReplicationOptions replication_options = {});

}  // namespace keylane
