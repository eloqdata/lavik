#include <cstdint>
#include <string>

#include "keylane/CLI11.hpp"
#include "keylane/server.h"

int main(int argc, char** argv) {
  CLI::App app{"keylane — high-performance Redis-compatible storage"};

  std::string bind_ip = "127.0.0.1";
  std::uint16_t port = 6379;
  unsigned threads = 1;
  int idle_timeout_ms = -1;
  unsigned recv_buffer_count = 1024;

  app.add_option("-b,--bind", bind_ip, "Bind address")->capture_default_str();
  app.add_option("-p,--port", port, "Listen port")->capture_default_str();
  app.add_option("-t,--threads", threads, "Worker thread count")
      ->capture_default_str()
      ->check(CLI::PositiveNumber);
  app.add_option("-i,--idle-timeout", idle_timeout_ms, "Idle timeout in ms (-1 = disabled)")
      ->capture_default_str();
  app.add_option("--recv-buffers", recv_buffer_count,
                 "Multishot recv buffer-ring entries per worker")
      ->capture_default_str()
      ->check(CLI::PositiveNumber);

  try {
    app.parse(argc, argv);
  } catch (const CLI::ParseError& e) {
    return app.exit(e);
  }

  return keylane::RunServer(bind_ip, port, threads, idle_timeout_ms, recv_buffer_count);
}
