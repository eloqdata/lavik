#include "celer/redis/server.h"

#include <array>
#include <cctype>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <cerrno>
#include <atomic>
#include <chrono>
#include <ctime>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <memory>
#include <pthread.h>
#include <vector>

#include "celer/base/log.h"
#include "celer/net/tcp_listener.h"
#include "celer/net/tcp_stream.h"
#include "celer/redis/command.h"
#include "celer/redis/db.h"
#include "celer/redis/resp.h"

namespace celer::redis {

namespace {

std::atomic<bool> g_shutdown_requested = false;
std::atomic<int> g_last_shutdown_signal = 0;

sigset_t ShutdownSignalSet() {
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGINT);
  sigaddset(&set, SIGTERM);
  return set;
}

void BlockShutdownSignals() {
  g_shutdown_requested.store(false, std::memory_order_release);
  g_last_shutdown_signal.store(0, std::memory_order_relaxed);
  const sigset_t set = ShutdownSignalSet();
  pthread_sigmask(SIG_BLOCK, &set, nullptr);
}

void WaitForShutdownSignal(std::atomic<bool>* running) {
  const sigset_t set = ShutdownSignalSet();
  while (running->load(std::memory_order_acquire)) {
    timespec timeout{
        .tv_sec = 0,
        .tv_nsec = 100 * 1000 * 1000,
    };
    const int signal = sigtimedwait(&set, nullptr, &timeout);
    if (signal == SIGINT || signal == SIGTERM) {
      g_last_shutdown_signal.store(signal, std::memory_order_relaxed);
      g_shutdown_requested.store(true, std::memory_order_release);
      return;
    }
    if (signal < 0 && errno != EAGAIN && errno != EINTR) {
      CELER_LOG_WARN << "sigtimedwait failed errno=" << errno;
    }
  }
}

struct RedisWorkerRuntime {
  Worker worker;
  TcpListener listener;
  std::atomic<bool> stop_requested = false;
  std::atomic<bool> accept_loop_done = false;
  std::atomic<bool> finished = false;
  std::atomic<int> exit_code = 0;

  void BeginShutdown() {
    stop_requested.store(true, std::memory_order_release);
    listener.Close();
  }

  void RequestStop() {
    BeginShutdown();
    worker.RequestStop();
  }
};

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
    if (!read_result.ok()) {
      co_return read_result.status();
    }
    if (*read_result == 0) {
      co_return Status(StatusCode::kUnavailable, "peer closed connection");
    }
    pending->append(reinterpret_cast<const char*>(buffer.data()), *read_result);
  }
}

Task<Status> AcceptLoop(RedisWorkerRuntime& runtime) {
  while (!runtime.stop_requested.load(std::memory_order_acquire)) {
    auto accepted = co_await runtime.listener.Accept();
    if (!accepted.ok()) {
      const auto code = accepted.status().code();
      if (runtime.stop_requested.load(std::memory_order_acquire) ||
          code == StatusCode::kCancelled ||
          code == StatusCode::kFailedPrecondition) {
        runtime.accept_loop_done.store(true, std::memory_order_release);
        co_return Status::Ok();
      }
      if (code != StatusCode::kUnavailable) {
        CELER_LOG_WARN << "accept failed: " << accepted.status().message();
      }
      continue;
    }

    runtime.worker.Spawn(RedisSession(runtime.worker, *accepted));
  }
  runtime.accept_loop_done.store(true, std::memory_order_release);
  co_return Status::Ok();
}

void RunRedisWorker(std::string bind_ip, std::uint16_t port, RecvMode recv_mode,
                    int idle_timeout_ms, bool reuse_port, unsigned worker_index,
                    RedisWorkerRuntime* runtime) {
  WorkerOptions worker_options;
  worker_options.recv_mode = recv_mode;
  worker_options.idle_timeout_ms = idle_timeout_ms;

  auto init_status = runtime->worker.Init(worker_options);
  if (!init_status.ok()) {
    CELER_LOG_ERROR << "worker[" << worker_index << "] init failed: " << init_status.message();
    runtime->exit_code.store(1, std::memory_order_release);
    runtime->finished.store(true, std::memory_order_release);
    return;
  }

  auto bind_status = runtime->listener.Bind(&runtime->worker, bind_ip, port, 128, reuse_port);
  if (!bind_status.ok()) {
    CELER_LOG_ERROR << "worker[" << worker_index << "] bind failed: " << bind_status.message();
    runtime->exit_code.store(1, std::memory_order_release);
    runtime->finished.store(true, std::memory_order_release);
    runtime->worker.RequestStop();
    return;
  }

  runtime->worker.Spawn(AcceptLoop(*runtime));
  runtime->worker.Run();
  if (!runtime->stop_requested.load(std::memory_order_acquire) &&
      !g_shutdown_requested.load(std::memory_order_acquire)) {
    runtime->exit_code.store(1, std::memory_order_release);
  }
  runtime->finished.store(true, std::memory_order_release);
}

}  // namespace

Task<Status> RedisSession(Worker& worker, Connection* connection) {
  static DbShard db;
  TcpStream stream(connection);
  std::string pending;

  while (stream.IsOpen()) {
    auto command_result = co_await ReadNextCommand(stream, &pending);
    if (!command_result.ok()) {
      if (command_result.status().code() == StatusCode::kUnavailable) {
        worker.BeginClose(connection, Status::Ok(), CloseMode::kPeerClosed);
        co_return Status::Ok();
      }

      std::string encoded = EncodeError("ERR " + command_result.status().message());
      auto write_status = co_await stream.WriteAll(
          std::span<const std::byte>(reinterpret_cast<const std::byte*>(encoded.data()),
                                     encoded.size()));
      worker.BeginClose(connection, command_result.status(), CloseMode::kLocalError);
      if (!write_status.ok()) {
        co_return write_status;
      }
      co_return command_result.status();
    }

    auto request_result = BuildCommandRequest(std::move(*command_result));
    std::string reply;
    bool close_connection = false;
    if (!request_result.ok()) {
      reply = EncodeError("ERR " + request_result.status().message());
    } else {
      CommandReply executed = ExecuteCommand(&db, *request_result);
      reply = std::move(executed.encoded);
      close_connection = executed.close_connection;
    }

    auto write_status = co_await stream.WriteAll(
        std::span<const std::byte>(reinterpret_cast<const std::byte*>(reply.data()),
                                   reply.size()));
    if (!write_status.ok()) {
      worker.BeginClose(connection, write_status, CloseMode::kLocalError);
      co_return write_status;
    }
    if (close_connection) {
      worker.BeginClose(connection, Status::Ok(), CloseMode::kLocalError);
      co_return Status::Ok();
    }
  }

  worker.BeginClose(connection, Status::Ok(), CloseMode::kPeerClosed);
  co_return Status::Ok();
}

int RunServer(std::string_view bind_ip, std::uint16_t port, unsigned thread_count,
              int idle_timeout_ms) {
  constexpr RecvMode recv_mode = kDefaultRecvMode;
  CELER_LOG_INFO << "keylane listening on " << bind_ip << ':' << port
                 << " threads=" << thread_count
                 << " idle_timeout_ms=" << idle_timeout_ms
                 << " recv_mode="
                 << (recv_mode == RecvMode::kMultishot ? "multishot" : "registered_buf");

  const bool reuse_port = thread_count > 1;
  BlockShutdownSignals();
  std::atomic<bool> signal_wait_running = true;
  std::thread signal_waiter(WaitForShutdownSignal, &signal_wait_running);

  std::vector<std::unique_ptr<RedisWorkerRuntime>> runtimes;
  runtimes.reserve(thread_count);
  std::vector<std::thread> threads;
  threads.reserve(thread_count);
  for (unsigned i = 0; i < thread_count; ++i) {
    runtimes.push_back(std::make_unique<RedisWorkerRuntime>());
    threads.emplace_back(RunRedisWorker, std::string(bind_ip), port, recv_mode,
                         idle_timeout_ms, reuse_port, i, runtimes.back().get());
  }

  bool failed = false;
  while (true) {
    bool all_finished = true;
    for (const auto& runtime : runtimes) {
      if (!runtime->finished.load(std::memory_order_acquire)) {
        all_finished = false;
      }
      if (runtime->exit_code.load(std::memory_order_acquire) != 0) {
        failed = true;
      }
    }

    if (g_shutdown_requested.load(std::memory_order_acquire) || failed || all_finished) {
      break;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  signal_wait_running.store(false, std::memory_order_release);
  signal_waiter.join();

  if (g_shutdown_requested.load(std::memory_order_acquire)) {
    const int signal = g_last_shutdown_signal.load(std::memory_order_relaxed);
    CELER_LOG_INFO << "shutdown requested by signal "
                   << (signal == 0 ? "unknown" : std::to_string(signal));
  }

  if (g_shutdown_requested.load(std::memory_order_acquire) || failed) {
    for (const auto& runtime : runtimes) {
      runtime->BeginShutdown();
    }

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(50);
    while (std::chrono::steady_clock::now() < deadline) {
      bool all_accept_loops_done = true;
      for (const auto& runtime : runtimes) {
        if (!runtime->accept_loop_done.load(std::memory_order_acquire) &&
            !runtime->finished.load(std::memory_order_acquire)) {
          all_accept_loops_done = false;
          break;
        }
      }
      if (all_accept_loops_done) {
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    for (const auto& runtime : runtimes) {
      runtime->worker.RequestStop();
    }
  }
  for (auto& thread : threads) {
    thread.join();
  }

  for (const auto& runtime : runtimes) {
    if (runtime->exit_code.load(std::memory_order_acquire) != 0) {
      failed = true;
    }
  }
  return failed ? 1 : 0;
}

}  // namespace celer::redis
