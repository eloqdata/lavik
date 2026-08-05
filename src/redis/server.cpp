#include "keylane/server.h"

#include <array>
#include <cstddef>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <atomic>
#include <poll.h>
#include <span>
#include <memory>
#include <string>
#include <string_view>
#include <sys/eventfd.h>
#include <unistd.h>

#include "celer/net/server.h"
#include "celer/net/tcp_service.h"
#include "spdlog/spdlog.h"
#include "celer/net/tcp_stream.h"
#include "keylane/command.h"
#include "keylane/resp.h"
#include "keylane/storage/engine.h"

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

class RedisService final : public TcpService {
 public:
  RedisService(std::uint16_t port, storage::StorageEngine* storage)
      : TcpService(port), storage_(storage) {}

  void Prepare(unsigned thread_count) override;
  Task<Status> Run(Worker& worker, ServiceContext ctx) override;
  bool startup_failed() const noexcept {
    return startup_failed_.load(std::memory_order_acquire);
  }
  void StopAcceptingRequests() noexcept;
  void WaitForRequestsDrained() const noexcept;

 protected:
  Task<Status> Serve(TcpStream stream) override;

 private:
  class RequestGuard {
   public:
    explicit RequestGuard(RedisService* service) : service_(service) {}
    RequestGuard(const RequestGuard&) = delete;
    RequestGuard& operator=(const RequestGuard&) = delete;
    ~RequestGuard() { service_->EndRequest(); }

   private:
    RedisService* service_;
  };

  bool TryBeginRequest() noexcept;
  void EndRequest() noexcept;

  static constexpr std::uint64_t kRequestsClosed = 1ULL << 63;
  static constexpr std::uint64_t kRequestCountMask = ~kRequestsClosed;
  storage::StorageEngine* storage_;
  std::atomic<bool> startup_failed_{false};
  std::atomic<std::uint64_t> request_gate_{0};
};

void RedisService::Prepare(unsigned thread_count) {
  TcpService::Prepare(thread_count);
}

void RedisService::StopAcceptingRequests() noexcept {
  request_gate_.fetch_or(kRequestsClosed, std::memory_order_acq_rel);
  request_gate_.notify_all();
}

void RedisService::WaitForRequestsDrained() const noexcept {
  std::uint64_t state = request_gate_.load(std::memory_order_acquire);
  while ((state & kRequestCountMask) != 0) {
    request_gate_.wait(state, std::memory_order_acquire);
    state = request_gate_.load(std::memory_order_acquire);
  }
}

bool RedisService::TryBeginRequest() noexcept {
  std::uint64_t state = request_gate_.load(std::memory_order_acquire);
  while ((state & kRequestsClosed) == 0) {
    if (request_gate_.compare_exchange_weak(
            state, state + 1, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      return true;
    }
  }
  return false;
}

void RedisService::EndRequest() noexcept {
  request_gate_.fetch_sub(1, std::memory_order_acq_rel);
  request_gate_.notify_all();
}

Task<Status> RedisService::Run(Worker& worker, ServiceContext ctx) {
  Status status = co_await storage_->InitializeWorker(worker);
  if (!status.ok()) [[unlikely]] {
    startup_failed_.store(true, std::memory_order_release);
    spdlog::error("worker[{}] storage initialization failed: {}", worker.id(),
                  status.message());
    worker.RequestStop();
    co_return status;
  }

  spdlog::info("worker[{}] direct-IO storage initialized", worker.id());
  co_return co_await TcpService::Run(worker, ctx);
}

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

Task<Status> RedisService::Serve(TcpStream stream) {
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

    if (!TryBeginRequest()) [[unlikely]] {
      std::string encoded = EncodeError("ERR server is shutting down");
      auto write_status = co_await stream.WriteAll(std::span<const std::byte>(
          reinterpret_cast<const std::byte*>(encoded.data()), encoded.size()));
      stream.Close();
      co_return write_status;
    }
    RequestGuard request_guard(this);

    auto request_result = BuildCommandRequest(std::move(*command_result));
    CommandReply reply;
    if (!request_result.ok()) [[unlikely]] {
      reply.encoded = EncodeError("ERR " + request_result.status().message());
    } else {
      reply = co_await ExecuteCommand(*request_result);
    }

    Status write_status;
    if (reply.disk_value.has_value()) {
      write_status =
          co_await stream.WriteAll(reply.disk_value->network_bytes());
    } else {
      write_status = co_await stream.WriteAll(std::span<const std::byte>(
          reinterpret_cast<const std::byte*>(reply.encoded.data()),
          reply.encoded.size()));
    }
    if (!write_status.ok()) [[unlikely]] {
      co_return write_status;
    }
    if (reply.close_connection || ShutdownRequested()) [[unlikely]] {
      stream.Close();
      co_return Status::Ok();
    }
  }

  co_return Status::Ok();
}

}  // namespace

int RunServer(std::string_view bind_ip, std::uint16_t port, unsigned thread_count,
              int idle_timeout_ms, unsigned recv_buffer_count,
              std::size_t registered_buffer_bytes,
              std::uint32_t flush_max_ms,
              const std::vector<std::string>& data_files,
              std::uint64_t data_file_size_bytes) {
  spdlog::info(
      "keylane listening on {}:{} threads={} idle_timeout_ms={} registered_buffer_bytes={} per worker flush_max_ms={}",
      bind_ip, port, thread_count, idle_timeout_ms, registered_buffer_bytes,
      flush_max_ms);

  const auto signal_status = InstallShutdownSignalHandler();
  if (!signal_status.ok()) [[unlikely]] {
    spdlog::error("signal setup failed: {}", signal_status.message());
    return 1;
  }

  storage::StorageEngineOptions storage_options;
  storage_options.data_files = data_files;
  storage_options.file_size_bytes = data_file_size_bytes;
  storage_options.flush_max_ms = flush_max_ms;
  storage_options.buffers.registered_bytes = registered_buffer_bytes;
  storage::StorageEngine storage(std::move(storage_options));
  Status storage_status = storage.Prepare(thread_count);
  if (!storage_status.ok()) [[unlikely]] {
    spdlog::error("storage prepare failed: {}", storage_status.message());
    CleanupShutdownSignalHandler();
    return 1;
  }
  InitStorage(&storage);

  ServerOptions options;
  options.bind_ip = std::string(bind_ip);
  options.thread_count = thread_count;
  options.idle_timeout_ms = idle_timeout_ms;
  options.recv_buffer_count = recv_buffer_count;

  RedisService redis(port, &storage);
  Server server;
  server.AddService(&redis);
  auto start_status = server.Start(options);
  if (!start_status.ok()) [[unlikely]] {
    spdlog::error("server start failed: {}", start_status.message());
    CleanupShutdownSignalHandler();
    return 1;
  }

  const WaitResult wait_result = WaitForSignalOrServerStop(server);
  int shutdown_exit_code = 0;
  if (wait_result == WaitResult::kSignal) {
    const int signal = static_cast<int>(g_last_shutdown_signal);
    spdlog::info("shutdown requested by signal {}",
                 (signal == 0 ? "unknown" : std::to_string(signal)));

    redis.StopAcceptingRequests();
    server.StopAccepting();
    redis.WaitForRequestsDrained();
    spdlog::info("all active requests drained; flushing storage buffers");
    Status flush_status = storage.FlushForShutdown();
    if (!flush_status.ok()) {
      spdlog::error("shutdown storage flush failed: {}",
                    flush_status.message());
      shutdown_exit_code = 1;
    } else {
      spdlog::info("all storage buffers durably flushed");
    }
    server.RequestStop();
  }
  server.WaitUntilStopped();
  const int exit_code = redis.startup_failed() || shutdown_exit_code != 0
                            ? 1
                            : server.exit_code();
  CleanupShutdownSignalHandler();
  return exit_code;
}

}  // namespace keylane
