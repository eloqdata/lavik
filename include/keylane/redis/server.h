#pragma once

#include <cstdint>
#include <string_view>

namespace keylane {

int RunServer(std::string_view bind_ip, std::uint16_t port, unsigned thread_count,
              int idle_timeout_ms);

}  // namespace keylane
