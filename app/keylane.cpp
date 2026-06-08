#include <cstdint>
#include <cstdlib>
#include <string>
#include <string_view>

#include "celer/base/log.h"
#include "keylane/server.h"

int main(int argc, char** argv) {
  std::string_view bind_ip = "127.0.0.1";
  std::uint16_t port = 6379;
  unsigned thread_count = 1;
  int idle_timeout_ms = -1;

  if (argc >= 2) {
    bind_ip = argv[1];
  }
  if (argc >= 3) {
    port = static_cast<std::uint16_t>(std::stoi(argv[2]));
  }
  if (argc >= 4) {
    thread_count = static_cast<unsigned>(std::stoul(argv[3]));
    if (thread_count == 0) {
      CELER_LOG_ERROR << "thread_count must be >= 1";
      return 1;
    }
  }
  if (argc >= 5) {
    idle_timeout_ms = std::stoi(argv[4]);
  }

  return keylane::RunServer(bind_ip, port, thread_count, idle_timeout_ms);
}
