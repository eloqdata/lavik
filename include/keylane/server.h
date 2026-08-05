#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace keylane {

int RunServer(std::string_view bind_ip, std::uint16_t port, unsigned thread_count,
              int idle_timeout_ms, unsigned recv_buffer_count,
              std::size_t registered_buffer_bytes,
              const std::vector<std::string>& data_files,
              std::uint64_t data_file_size_bytes);

}  // namespace keylane
