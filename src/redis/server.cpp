#include "keylane/server.h"

#include <array>
#include <algorithm>
#include <chrono>
#include <cstddef>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <atomic>
#include <poll.h>
#include <span>
#include <sys/socket.h>
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
#include "keylane/session.h"
#include "keylane/tx/tx_shard.h"
#include "keylane/replication.h"
#include "keylane/storage/engine.h"

namespace keylane {
using namespace celer;

namespace {

constexpr std::array<std::uint64_t, 28> kLatencyBucketUpperUs{
    1, 2, 3, 4, 5, 8, 10, 15, 20, 30, 40, 50, 75, 100,
    150, 200, 300, 500, 750, 1000, 1500, 2000, 3000, 5000,
    8000, 10000, 20000, 50000};

struct LatencyDistribution {
  std::uint64_t sum_ns = 0;
  std::array<std::uint64_t, kLatencyBucketUpperUs.size()> buckets{};

  void Add(std::uint64_t ns) noexcept {
    sum_ns += ns;
    const std::uint64_t us = (ns + 999) / 1000;
    const auto it = std::lower_bound(kLatencyBucketUpperUs.begin(),
                                     kLatencyBucketUpperUs.end(), us);
    const std::size_t index =
        it == kLatencyBucketUpperUs.end()
            ? kLatencyBucketUpperUs.size() - 1
            : static_cast<std::size_t>(it - kLatencyBucketUpperUs.begin());
    ++buckets[index];
  }

  double AverageUs(std::uint64_t count) const noexcept {
    return count == 0 ? 0.0
                      : static_cast<double>(sum_ns) /
                            (1000.0 * static_cast<double>(count));
  }

  std::uint64_t PercentileUpperUs(std::uint64_t count,
                                  double percentile) const noexcept {
    if (count == 0) {
      return 0;
    }
    const std::uint64_t target = static_cast<std::uint64_t>(
        static_cast<double>(count) * percentile + 0.999999);
    std::uint64_t cumulative = 0;
    for (std::size_t i = 0; i < buckets.size(); ++i) {
      cumulative += buckets[i];
      if (cumulative >= target) {
        return kLatencyBucketUpperUs[i];
      }
    }
    return kLatencyBucketUpperUs.back();
  }
};

struct ReadLatencyStats {
  std::uint64_t count = 0;
  std::uint64_t remote = 0;
  std::uint64_t hits = 0;
  std::uint64_t disk_reads = 0;
  std::uint64_t heap_buffers = 0;
  std::uint64_t next_report_ns = 0;
  LatencyDistribution total;
  LatencyDistribution non_network;
  LatencyDistribution route_out;
  LatencyDistribution lookup;
  LatencyDistribution buffer;
  LatencyDistribution io;
  LatencyDistribution decode;
  LatencyDistribution route_back;
  LatencyDistribution send;
};

std::uint64_t Elapsed(std::uint64_t end, std::uint64_t start) noexcept {
  return end >= start && start != 0 ? end - start : 0;
}

void RecordReadLatency(const ReadLatencyTrace& trace) {
  static thread_local ReadLatencyStats stats;
  if (trace.request_start_ns == 0 || trace.send_complete_ns == 0) {
    return;
  }
  ++stats.count;
  stats.remote += trace.remote;
  stats.hits += trace.hit;
  stats.disk_reads += trace.disk_read;
  stats.heap_buffers += trace.heap_read_buffer;
  stats.total.Add(Elapsed(trace.send_complete_ns, trace.request_start_ns));
  stats.non_network.Add(Elapsed(trace.send_start_ns, trace.request_start_ns));
  stats.route_out.Add(Elapsed(trace.owner_start_ns, trace.request_start_ns));
  stats.lookup.Add(Elapsed(trace.lookup_done_ns, trace.owner_start_ns));
  stats.buffer.Add(
      Elapsed(trace.buffer_acquired_ns, trace.buffer_acquire_start_ns));
  stats.io.Add(Elapsed(trace.io_complete_ns, trace.io_submit_ns));
  stats.decode.Add(Elapsed(trace.decode_done_ns, trace.io_complete_ns));
  stats.route_back.Add(Elapsed(trace.origin_resume_ns, trace.owner_done_ns));
  stats.send.Add(Elapsed(trace.send_complete_ns, trace.send_start_ns));

  const std::uint64_t now = trace.send_complete_ns;
  if (stats.next_report_ns == 0) {
    stats.next_report_ns = now + 10'000'000'000ULL;
    return;
  }
  if (now < stats.next_report_ns) {
    return;
  }

  const auto avg = [&](const LatencyDistribution& value) {
    return value.AverageUs(stats.count);
  };
  const auto p999 = [&](const LatencyDistribution& value) {
    return value.PercentileUpperUs(stats.count, 0.999);
  };
  const auto p9999 = [&](const LatencyDistribution& value) {
    return value.PercentileUpperUs(stats.count, 0.9999);
  };
  const auto wake_stats = ThisWorker().self->TakeWakeStats();
  const auto scheduler_stats = ThisWorker().self->TakeSchedulerStats();
  spdlog::info(
      "read-latency worker={} n={} remote={:.1f}% hit={:.1f}% disk={:.1f}% "
      "heap-buffer={:.1f}% avg-us total={:.1f} route-out={:.1f} lookup={:.1f} "
      "buffer={:.1f} io={:.1f} decode={:.1f} route-back={:.1f} send={:.1f} "
      "p99.9-us total<={} route-out<={} lookup<={} buffer<={} io<={} "
      "decode<={} route-back<={} send<={} wake-sent={}/{}",
      ThisWorker().id, stats.count,
      100.0 * static_cast<double>(stats.remote) / stats.count,
      100.0 * static_cast<double>(stats.hits) / stats.count,
      100.0 * static_cast<double>(stats.disk_reads) / stats.count,
      100.0 * static_cast<double>(stats.heap_buffers) / stats.count,
      avg(stats.total), avg(stats.route_out), avg(stats.lookup), avg(stats.buffer),
      avg(stats.io), avg(stats.decode), avg(stats.route_back), avg(stats.send),
      p999(stats.total), p999(stats.route_out), p999(stats.lookup),
      p999(stats.buffer), p999(stats.io), p999(stats.decode),
      p999(stats.route_back), p999(stats.send),
      wake_stats.sent, wake_stats.checks);
  const auto cycles_to_us = [&](std::uint64_t cycles) {
    return scheduler_stats.cycles_per_second == 0.0
               ? 0.0
               : static_cast<double>(cycles) * 1'000'000.0 /
                     scheduler_stats.cycles_per_second;
  };
  const std::uint64_t scheduled_cycles =
      scheduler_stats.foreground_cycles + scheduler_stats.background_cycles;
  spdlog::info(
      "scheduler worker={} rounds={} avg-round-us={:.2f} max-round-us={:.2f} "
      "fg-resumes={} fg-us={:.1f} max-fg-us={:.1f} fg-overruns={} "
      "bg-resumes={} bg-us={:.1f} max-bg-us={:.1f} bg-overruns={} bg-share={:.1f}%",
      ThisWorker().id, scheduler_stats.rounds,
      scheduler_stats.rounds == 0
          ? 0.0
          : cycles_to_us(scheduler_stats.round_cycles) /
                static_cast<double>(scheduler_stats.rounds),
      cycles_to_us(scheduler_stats.max_round_cycles),
      scheduler_stats.foreground_resumes,
      cycles_to_us(scheduler_stats.foreground_cycles),
      cycles_to_us(scheduler_stats.max_foreground_cycles),
      scheduler_stats.foreground_overruns,
      scheduler_stats.background_resumes,
      cycles_to_us(scheduler_stats.background_cycles),
      cycles_to_us(scheduler_stats.max_background_cycles),
      scheduler_stats.background_overruns,
      scheduled_cycles == 0
          ? 0.0
          : 100.0 * static_cast<double>(scheduler_stats.background_cycles) /
                static_cast<double>(scheduled_cycles));
  spdlog::info(
      "read-latency-p99.99 worker={} n={} non-network-us<={} total-us<={} "
      "storage-io-us<={} route-out-us<={} lookup-us<={} buffer-us<={} "
      "decode-us<={} route-back-us<={} send-us<={}",
      ThisWorker().id, stats.count, p9999(stats.non_network),
      p9999(stats.total), p9999(stats.io), p9999(stats.route_out),
      p9999(stats.lookup), p9999(stats.buffer), p9999(stats.decode),
      p9999(stats.route_back), p9999(stats.send));
  stats = ReadLatencyStats{};
  stats.next_report_ns = now + 10'000'000'000ULL;
}

std::atomic<bool> g_shutdown_requested = false;
volatile sig_atomic_t g_last_shutdown_signal = 0;
int g_signal_event_fd = -1;

void ShutdownSignalHandler(int signal) {
  g_last_shutdown_signal = signal;
  if (g_signal_event_fd < 0) {
    return;
  }
  const std::uint64_t wake = 1;
  const ssize_t result = write(g_signal_event_fd, &wake, sizeof(wake));
  (void)result;
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
      const ssize_t result = read(g_signal_event_fd, &wake, sizeof(wake));
      if (result < 0 && errno != EAGAIN) {
        spdlog::warn("signal eventfd read failed errno={}", errno);
      }
      g_shutdown_requested.store(true, std::memory_order_release);
      return WaitResult::kSignal;
    }
    if ((fds[1].revents & POLLIN) != 0) {
      std::uint64_t wake = 0;
      const ssize_t result =
          read(server.completion_fd(), &wake, sizeof(wake));
      if (result < 0 && errno != EAGAIN) {
        spdlog::warn("server completion eventfd read failed errno={}", errno);
      }
      return WaitResult::kStopped;
    }
  }
}

class RedisService final : public TcpService {
 public:
  RedisService(std::uint16_t port, storage::StorageEngine* storage,
               ReplicationManager* replication)
      : TcpService(port), storage_(storage), replication_(replication) {}

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
  Task<Status> Serve(TcpStream& stream, ConnectionContext& ctx);

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
  ReplicationManager* replication_;
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
  tx::TxRuntime::Get()->shard(worker.id()).Bind(worker);
  Status status = co_await storage_->InitializeWorker(worker);
  if (!status.ok()) [[unlikely]] {
    startup_failed_.store(true, std::memory_order_release);
    spdlog::error("worker[{}] storage initialization failed: {}", worker.id(),
                  status.message());
    worker.RequestStop();
    co_return status;
  }

  spdlog::info("worker[{}] direct-IO storage initialized", worker.id());
  replication_->StorageReady(worker);
  co_return co_await TcpService::Run(worker, ctx);
}

Task<StatusOr<RespCommand>> ReadNextCommand(TcpStream& stream, std::string* pending) {
  // Hard ceiling on one connection's accumulated request bytes. The per-frame
  // limits (1024 args of up to 512 MiB each) still admit a claimed frame far
  // larger than RAM, and the buffer grows until the frame completes — without
  // a cap, one client streaming an oversized frame runs the process out of
  // memory. 1 GiB matches Redis's query buffer limit and comfortably fits
  // any legitimate command.
  constexpr std::size_t kMaxPendingBytes = 1ULL * 1024 * 1024 * 1024;
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
    if (pending->size() >= kMaxPendingBytes) {
      co_return Status(StatusCode::kResourceExhausted,
                       "client request exceeds the query buffer limit");
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
  static std::atomic<std::uint64_t> next_connection_id{1};
  ConnectionContext ctx;
  ctx.conn_id = next_connection_id.fetch_add(1, std::memory_order_relaxed);
  ConnectionOpened();
  const Status status = co_await Serve(stream, ctx);
  // Single connection-scoped cleanup point: every disconnect path funnels
  // through this co_return.
  co_await ReleaseConnectionWatches(ctx);
  ConnectionClosed();
  co_return status;
}

// A gate-holding streamed reply (KEYS) is paced by the peer: a client that
// stops reading parks the chunk write in io_uring indefinitely while the
// database gate stays closed and graceful shutdown cannot drain. Redis
// bounds the analogous exposure with client output-buffer limits that
// disconnect the offender; the streaming equivalent is a stall deadline —
// no forward progress on the socket for this long ends the connection.
constexpr auto kStreamStallLimit = std::chrono::seconds(30);

struct StreamStallState {
  std::chrono::steady_clock::time_point last_progress;
  bool done = false;  // same-worker access only
};

// Watchdog for one streamed reply. shutdown() rather than close: it fails
// the parked write immediately without releasing the descriptor out from
// under the pending io_uring operation, and the serve loop's normal
// teardown then reopens the gate and closes the socket.
Task<Status> BreakStalledStream(std::shared_ptr<StreamStallState> state,
                                int fd) {
  while (!state->done) {
    const auto deadline = state->last_progress + kStreamStallLimit;
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) {
      ::shutdown(fd, SHUT_RDWR);
      co_return Status::Ok();
    }
    Status slept = co_await celer::SleepFor(
        *ThisWorker().self,
        std::chrono::duration_cast<std::chrono::milliseconds>(deadline -
                                                              now) +
            std::chrono::milliseconds(1));
    if (!slept.ok()) {
      co_return slept;  // worker shutting down
    }
  }
  co_return Status::Ok();
}

Task<Status> RedisService::Serve(TcpStream& stream, ConnectionContext& ctx) {
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

    auto request_result =
        BuildCommandRequest(std::move(*command_result), ctx.selected_db);
    CommandReply reply;
    if (!request_result.ok()) [[unlikely]] {
      reply.encoded = EncodeError("ERR " + request_result.status().message());
    } else {
      reply = co_await DispatchCommand(ctx, std::move(*request_result));
    }
    if (reply.selected_db.has_value()) {
      ctx.selected_db = *reply.selected_db;
    }

    // Streamed replies hold the database gate at the peer's pace; arm the
    // stall watchdog for the whole stream, header included. The scope guard
    // retires it on every exit path, including error co_returns.
    std::shared_ptr<StreamStallState> stall;
    struct RetireStall {
      std::shared_ptr<StreamStallState> state;
      ~RetireStall() {
        if (state != nullptr) {
          state->done = true;
        }
      }
    } retire_stall;
    if (reply.chunks) {
      stall = std::make_shared<StreamStallState>();
      stall->last_progress = std::chrono::steady_clock::now();
      retire_stall.state = stall;
      ThisWorker().self->Spawn(
          BreakStalledStream(stall, stream.NativeFd()));
    }

    Status write_status;
    if (reply.read_trace.request_start_ns != 0) {
      reply.read_trace.send_start_ns = ReadTraceNowNanos();
    }
    if (reply.disk_value.has_value()) {
      write_status =
          co_await stream.WriteAll(reply.disk_value->network_bytes());
    } else {
      write_status = co_await stream.WriteAll(std::span<const std::byte>(
          reinterpret_cast<const std::byte*>(reply.encoded.data()),
          reply.encoded.size()));
    }
    // Streamed continuation (KEYS): drain bounded chunks onto the socket.
    // The reply header already committed the element count, so a chunk
    // failure can only end the connection.
    while (write_status.ok() && reply.chunks) {
      if (stall != nullptr) {
        stall->last_progress = std::chrono::steady_clock::now();
      }
      auto chunk = co_await reply.chunks();
      if (!chunk.ok()) {
        co_return chunk.status();
      }
      if (chunk->empty()) {
        break;
      }
      write_status = co_await stream.WriteAll(std::span<const std::byte>(
          reinterpret_cast<const std::byte*>(chunk->data()), chunk->size()));
    }
    if (reply.read_trace.request_start_ns != 0) {
      reply.read_trace.send_complete_ns = ReadTraceNowNanos();
      RecordReadLatency(reply.read_trace);
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
              unsigned busy_poll_us,
              std::size_t registered_buffer_bytes,
              std::uint32_t flush_max_ms, std::size_t flush_size_bytes,
              bool verify_read_crc,
              const std::vector<std::string>& data_files,
              ReplicationOptions replication_options) {
  spdlog::info(
      "keylane listening on {}:{} threads={} idle_timeout_ms={} busy_poll_us={} "
      "registered_buffer_bytes={} per worker flush_max_ms={} flush_size_bytes={} "
      "verify_read_crc={}",
      bind_ip, port, thread_count, idle_timeout_ms, busy_poll_us,
      registered_buffer_bytes, flush_max_ms, flush_size_bytes, verify_read_crc);

  const auto signal_status = InstallShutdownSignalHandler();
  if (!signal_status.ok()) [[unlikely]] {
    spdlog::error("signal setup failed: {}", signal_status.message());
    return 1;
  }

  storage::StorageEngineOptions storage_options;
  storage_options.data_files = data_files;
  storage_options.flush_max_ms = flush_max_ms;
  storage_options.flush_size_bytes = flush_size_bytes;
  storage_options.verify_read_crc = verify_read_crc;
  // A node accepting an upstream replication stream must not create local
  // expiration mutation sequences. It still hides expired values by their
  // absolute deadline and applies the primary's replicated tombstone.
  storage_options.expiration_authority = replication_options.listen_port == 0;
  storage_options.buffers.registered_bytes = registered_buffer_bytes;
  storage::StorageEngine storage(std::move(storage_options));
  Status storage_status = storage.Prepare(thread_count);
  if (!storage_status.ok()) [[unlikely]] {
    spdlog::error("storage prepare failed: {}", storage_status.message());
    CleanupShutdownSignalHandler();
    return 1;
  }
  ReplicationManager replication(&storage, replication_options);
  InitStorage(&storage, replication.replica_read_only());
  SetServerInfo(port, thread_count);
  tx::TxRuntime::Create(thread_count);

  ServerOptions options;
  options.bind_ip = std::string(bind_ip);
  options.thread_count = thread_count;
  options.idle_timeout_ms = idle_timeout_ms;
  options.recv_buffer_count = recv_buffer_count;
  options.busy_poll_us = busy_poll_us;

  RedisService redis(port, &storage, &replication);
  Server server;
  server.AddService(&redis);
  if (celer::Service* replication_service = replication.service();
      replication_service != nullptr) {
    server.AddService(replication_service);
  }
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
