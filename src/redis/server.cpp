#include "keylane/server.h"

#include <array>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <atomic>
#include <poll.h>
#include <span>
#include <string>
#include <string_view>
#include <sys/eventfd.h>
#include <unistd.h>

#include "celer/net/tcp_server-inl.h"
#include "spdlog/spdlog.h"
#include "celer/net/tcp_stream.h"
#include "keylane/command.h"
#include "keylane/db.h"
#include "keylane/resp.h"

namespace keylane {
using namespace celer;

namespace {

std::atomic<bool> g_shutdown_requested = false;
volatile sig_atomic_t g_last_shutdown_signal = 0;
int g_signal_event_fd = -1;

void ShutdownSignalHandler(int signal) {
  g_last_shutdown_signal = signal;
  if (g_signal_event_fd < 0) {
    return;
  }
  const std::uint64_t wake = 1;
  (void)write(g_signal_event_fd, &wake, sizeof(wake));
}

Status InstallShutdownSignalHandler() {
  g_shutdown_requested.store(false, std::memory_order_release);
  g_last_shutdown_signal = 0;
  g_signal_event_fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
  if (g_signal_event_fd < 0) {
    return Status(StatusCode::kInternal, "eventfd setup failed");
  }

  struct sigaction action {};
  sigemptyset(&action.sa_mask);
  action.sa_handler = ShutdownSignalHandler;
  if (sigaction(SIGINT, &action, nullptr) != 0 ||
      sigaction(SIGTERM, &action, nullptr) != 0) {
    close(g_signal_event_fd);
    g_signal_event_fd = -1;
    return Status(StatusCode::kInternal, "sigaction setup failed");
  }
  return Status::Ok();
}

void CleanupShutdownSignalHandler() noexcept {
  struct sigaction action {};
  sigemptyset(&action.sa_mask);
  action.sa_handler = SIG_DFL;
  (void)sigaction(SIGINT, &action, nullptr);
  (void)sigaction(SIGTERM, &action, nullptr);
  if (g_signal_event_fd >= 0) {
    close(g_signal_event_fd);
    g_signal_event_fd = -1;
  }
}

enum class WaitResult {
  kSignal,
  kStopped,
};

template <typename Server>
WaitResult WaitForSignalOrServerStop(const Server& server) {
  pollfd fds[2] = {
      {.fd = g_signal_event_fd, .events = POLLIN, .revents = 0},
      {.fd = server.completion_fd(), .events = POLLIN, .revents = 0},
  };

  while (true) {
    const int rc = poll(fds, 2, -1);
    if (rc < 0) [[unlikely]] {
      if (errno == EINTR) {
        continue;
      }
      spdlog::warn("poll failed errno={}", errno);
      return WaitResult::kStopped;
    }

    if ((fds[0].revents & POLLIN) != 0) {
      std::uint64_t wake = 0;
      (void)read(g_signal_event_fd, &wake, sizeof(wake));
      g_shutdown_requested.store(true, std::memory_order_release);
      return WaitResult::kSignal;
    }
    if ((fds[1].revents & POLLIN) != 0) {
      std::uint64_t wake = 0;
      (void)read(server.completion_fd(), &wake, sizeof(wake));
      return WaitResult::kStopped;
    }
  }
}

class RedisHandler {
 public:
  Task<Status> HandleRequests(TcpStream stream);
};

thread_local DbShard tls_db_;

Task<StatusOr<RespCommand>> ReadNextCommand(TcpStream& stream, std::string* pending) {
  std::array<std::byte, 4096> buffer{};
  while (true) {
    RespParseResult parsed = ParseRespCommand(*pending);
    if (parsed.state == RespParseState::kOk) {
      RespCommand command = std::move(parsed.command);
      pending->erase(0, parsed.consumed);
      co_return command;
    }
    if (parsed.state == RespParseState::kError) {
      co_return parsed.status;
    }

    auto read_result = co_await stream.ReadSome(buffer);
    if (!read_result.ok()) [[unlikely]] {
      co_return read_result.status();
    }
    if (*read_result == 0) [[unlikely]] {
      co_return Status(StatusCode::kUnavailable, "peer closed connection");
    }
    pending->append(reinterpret_cast<const char*>(buffer.data()), *read_result);
  }
}

bool ShutdownRequested() {
  return g_shutdown_requested.load(std::memory_order_acquire);
}

Task<Status> RedisHandler::HandleRequests(TcpStream stream) {
  std::string pending;

  while (stream.IsOpen()) {
    if (ShutdownRequested()) [[unlikely]] {
      co_return Status::Ok();
    }

    auto command_result = co_await ReadNextCommand(stream, &pending);
    if (!command_result.ok()) [[unlikely]] {
      if (command_result.status().code() == StatusCode::kUnavailable) [[unlikely]] {
        co_return Status::Ok();
      }

      std::string encoded = EncodeError("ERR " + command_result.status().message());
      auto write_status = co_await stream.WriteAll(
          std::span<const std::byte>(reinterpret_cast<const std::byte*>(encoded.data()),
                                     encoded.size()));
      if (!write_status.ok()) [[unlikely]] {
        co_return write_status;
      }
      co_return command_result.status();
    }

    auto request_result = BuildCommandRequest(std::move(*command_result));
    std::string reply;
    bool close_connection = false;
    if (!request_result.ok()) [[unlikely]] {
      reply = EncodeError("ERR " + request_result.status().message());
    } else {
      CommandReply executed = ExecuteCommand(&tls_db_, *request_result);
      reply = std::move(executed.encoded);
      close_connection = executed.close_connection;
    }

    auto write_status = co_await stream.WriteAll(
        std::span<const std::byte>(reinterpret_cast<const std::byte*>(reply.data()),
                                   reply.size()));
    if (!write_status.ok()) [[unlikely]] {
      co_return write_status;
    }
    if (close_connection || ShutdownRequested()) [[unlikely]] {
      stream.Close();
      co_return Status::Ok();
    }
  }

  co_return Status::Ok();
}

}  // namespace

int RunServer(std::string_view bind_ip, std::uint16_t port, unsigned thread_count,
              int idle_timeout_ms) {
  constexpr RecvMode recv_mode = kDefaultRecvMode;
  spdlog::info("keylane listening on {}:{} threads={} idle_timeout_ms={} recv_mode={}",
               bind_ip, port, thread_count, idle_timeout_ms,
               (recv_mode == RecvMode::kMultishot ? "multishot" : "registered_buf"));

  const auto signal_status = InstallShutdownSignalHandler();
  if (!signal_status.ok()) [[unlikely]] {
    spdlog::error("signal setup failed: {}", signal_status.message());
    return 1;
  }

  TcpServerOptions options;
  options.bind_ip = std::string(bind_ip);
  options.port = port;
  options.thread_count = thread_count;
  options.idle_timeout_ms = idle_timeout_ms;
  options.recv_mode = recv_mode;

  RedisHandler handler;
  TcpServer<RedisHandler> server;
  auto start_status = server.Start(options, std::move(handler));
  if (!start_status.ok()) [[unlikely]] {
    spdlog::error("server start failed: {}", start_status.message());
    CleanupShutdownSignalHandler();
    return 1;
  }

  const WaitResult wait_result = WaitForSignalOrServerStop(server);
  if (wait_result == WaitResult::kSignal) {
    const int signal = static_cast<int>(g_last_shutdown_signal);
    spdlog::info("shutdown requested by signal {}",
                 (signal == 0 ? "unknown" : std::to_string(signal)));
    server.RequestStop();
  }
  server.WaitUntilStopped();
  const int exit_code = server.exit_code();
  CleanupShutdownSignalHandler();
  return exit_code;
}

}  // namespace keylane

template class celer::TcpServer<keylane::RedisHandler>;
