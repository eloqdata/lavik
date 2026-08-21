#include "keylane/server.h"

#include <mimalloc.h>
#include <openssl/crypto.h>
#include <openssl/sha.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>

#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "celer/net/server.h"
#include "celer/net/tcp_service.h"
#include "celer/net/tcp_stream.h"
#include "celer/net/tls.h"
#include "celer/runtime/sync.h"
#include "keylane/command.h"
#include "keylane/config.h"
#include "keylane/memory.h"
#include "keylane/metrics.h"
#include "keylane/replication.h"
#include "keylane/resp.h"
#include "keylane/session.h"
#include "keylane/storage/engine.h"
#include "keylane/tx/tx_shard.h"
#include "spdlog/spdlog.h"

namespace keylane {
using namespace celer;

namespace {

constexpr std::array<std::uint64_t, 28> kLatencyBucketUpperUs{
    1,    2,    3,    4,    5,    8,     10,    15,   20,  30,
    40,   50,   75,   100,  150,  200,   300,   500,  750, 1000,
    1500, 2000, 3000, 5000, 8000, 10000, 20000, 50000};

struct LatencyDistribution {
  std::uint64_t sum_ns_ = 0;
  std::array<std::uint64_t, kLatencyBucketUpperUs.size()> buckets_{};

  void Add(std::uint64_t ns) noexcept {
    sum_ns_ += ns;
    const std::uint64_t us = (ns + 999) / 1000;
    const auto it = std::lower_bound(kLatencyBucketUpperUs.begin(),
                                     kLatencyBucketUpperUs.end(), us);
    const std::size_t index =
        it == kLatencyBucketUpperUs.end()
            ? kLatencyBucketUpperUs.size() - 1
            : static_cast<std::size_t>(it - kLatencyBucketUpperUs.begin());
    ++buckets_[index];
  }

  double AverageUs(std::uint64_t count) const noexcept {
    return count == 0 ? 0.0
                      : static_cast<double>(sum_ns_) /
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
    for (std::size_t i = 0; i < buckets_.size(); ++i) {
      cumulative += buckets_[i];
      if (cumulative >= target) {
        return kLatencyBucketUpperUs[i];
      }
    }
    return kLatencyBucketUpperUs.back();
  }
};

struct ReadLatencyStats {
  std::uint64_t count_ = 0;
  std::uint64_t remote_ = 0;
  std::uint64_t hits_ = 0;
  std::uint64_t disk_reads_ = 0;
  std::uint64_t heap_buffers_ = 0;
  std::uint64_t next_report_ns_ = 0;
  LatencyDistribution total_;
  LatencyDistribution non_network_;
  LatencyDistribution route_out_;
  LatencyDistribution lookup_;
  LatencyDistribution buffer_;
  LatencyDistribution io_;
  LatencyDistribution decode_;
  LatencyDistribution route_back_;
  LatencyDistribution send_;
};

std::uint64_t Elapsed(std::uint64_t end, std::uint64_t start) noexcept {
  return end >= start && start != 0 ? end - start : 0;
}

void RecordReadLatency(const ReadLatencyTrace& trace) {
  static thread_local ReadLatencyStats stats;
  if (trace.request_start_ns_ == 0 || trace.send_complete_ns_ == 0) {
    return;
  }
  ++stats.count_;
  stats.remote_ += trace.remote_;
  stats.hits_ += trace.hit_;
  stats.disk_reads_ += trace.disk_read_;
  stats.heap_buffers_ += trace.heap_read_buffer_;
  stats.total_.Add(Elapsed(trace.send_complete_ns_, trace.request_start_ns_));
  stats.non_network_.Add(
      Elapsed(trace.send_start_ns_, trace.request_start_ns_));
  stats.route_out_.Add(Elapsed(trace.owner_start_ns_, trace.request_start_ns_));
  stats.lookup_.Add(Elapsed(trace.lookup_done_ns_, trace.owner_start_ns_));
  stats.buffer_.Add(
      Elapsed(trace.buffer_acquired_ns_, trace.buffer_acquire_start_ns_));
  stats.io_.Add(Elapsed(trace.io_complete_ns_, trace.io_submit_ns_));
  stats.decode_.Add(Elapsed(trace.decode_done_ns_, trace.io_complete_ns_));
  stats.route_back_.Add(Elapsed(trace.origin_resume_ns_, trace.owner_done_ns_));
  stats.send_.Add(Elapsed(trace.send_complete_ns_, trace.send_start_ns_));

  const std::uint64_t now = trace.send_complete_ns_;
  if (stats.next_report_ns_ == 0) {
    // Keep worker reports out of the same logger critical section. Synchronous
    // bursts from every worker otherwise become an artificial tail-latency
    // event in the trace build itself.
    stats.next_report_ns_ =
        now + 10'000'000'000ULL + 100'000'000ULL * ThisWorker().id_;
    return;
  }
  if (now < stats.next_report_ns_) {
    return;
  }

  const auto avg = [&](const LatencyDistribution& value) {
    return value.AverageUs(stats.count_);
  };
  const auto p999 = [&](const LatencyDistribution& value) {
    return value.PercentileUpperUs(stats.count_, 0.999);
  };
  const auto p9999 = [&](const LatencyDistribution& value) {
    return value.PercentileUpperUs(stats.count_, 0.9999);
  };
  const auto wake_stats = ThisWorker().self_->TakeWakeStats();
  const auto scheduler_stats = ThisWorker().self_->TakeSchedulerStats();
  spdlog::info(
      "read-latency worker={} n={} remote={:.1f}% hit={:.1f}% disk={:.1f}% "
      "heap-buffer={:.1f}% avg-us total={:.1f} route-out={:.1f} lookup={:.1f} "
      "buffer={:.1f} io={:.1f} decode={:.1f} route-back={:.1f} send={:.1f} "
      "p99.9-us total<={} route-out<={} lookup<={} buffer<={} io<={} "
      "decode<={} route-back<={} send<={} wake-sent={}/{}",
      ThisWorker().id_, stats.count_,
      100.0 * static_cast<double>(stats.remote_) / stats.count_,
      100.0 * static_cast<double>(stats.hits_) / stats.count_,
      100.0 * static_cast<double>(stats.disk_reads_) / stats.count_,
      100.0 * static_cast<double>(stats.heap_buffers_) / stats.count_,
      avg(stats.total_), avg(stats.route_out_), avg(stats.lookup_),
      avg(stats.buffer_), avg(stats.io_), avg(stats.decode_),
      avg(stats.route_back_), avg(stats.send_), p999(stats.total_),
      p999(stats.route_out_), p999(stats.lookup_), p999(stats.buffer_),
      p999(stats.io_), p999(stats.decode_), p999(stats.route_back_),
      p999(stats.send_), wake_stats.sent_, wake_stats.checks_);
  const auto cycles_to_us = [&](std::uint64_t cycles) {
    return scheduler_stats.cycles_per_second_ == 0.0
               ? 0.0
               : static_cast<double>(cycles) * 1'000'000.0 /
                     scheduler_stats.cycles_per_second_;
  };
  const std::uint64_t scheduled_cycles =
      scheduler_stats.foreground_cycles_ + scheduler_stats.background_cycles_;
  spdlog::info(
      "scheduler worker={} rounds={} avg-round-us={:.2f} max-round-us={:.2f} "
      "fg-resumes={} fg-us={:.1f} max-fg-us={:.1f} fg-overruns={} "
      "bg-resumes={} bg-us={:.1f} max-bg-us={:.1f} bg-overruns={} "
      "bg-share={:.1f}% spdk-polls={} empty={:.1f}% completions={} "
      "max-batch={} avg-poll-us={:.3f} max-poll-us={:.1f}",
      ThisWorker().id_, scheduler_stats.rounds_,
      scheduler_stats.rounds_ == 0
          ? 0.0
          : cycles_to_us(scheduler_stats.round_cycles_) /
                static_cast<double>(scheduler_stats.rounds_),
      cycles_to_us(scheduler_stats.max_round_cycles_),
      scheduler_stats.foreground_resumes_,
      cycles_to_us(scheduler_stats.foreground_cycles_),
      cycles_to_us(scheduler_stats.max_foreground_cycles_),
      scheduler_stats.foreground_overruns_, scheduler_stats.background_resumes_,
      cycles_to_us(scheduler_stats.background_cycles_),
      cycles_to_us(scheduler_stats.max_background_cycles_),
      scheduler_stats.background_overruns_,
      scheduled_cycles == 0
          ? 0.0
          : 100.0 * static_cast<double>(scheduler_stats.background_cycles_) /
                static_cast<double>(scheduled_cycles),
      scheduler_stats.storage_poll_calls_,
      scheduler_stats.storage_poll_calls_ == 0
          ? 0.0
          : 100.0 * static_cast<double>(scheduler_stats.storage_poll_empty_) /
                static_cast<double>(scheduler_stats.storage_poll_calls_),
      scheduler_stats.storage_completions_,
      scheduler_stats.storage_max_completions_,
      scheduler_stats.storage_poll_calls_ == 0
          ? 0.0
          : cycles_to_us(scheduler_stats.storage_poll_cycles_) /
                static_cast<double>(scheduler_stats.storage_poll_calls_),
      cycles_to_us(scheduler_stats.storage_max_poll_cycles_));
  spdlog::info(
      "read-latency-p99.99 worker={} n={} non-network-us<={} total-us<={} "
      "storage-io-us<={} route-out-us<={} lookup-us<={} buffer-us<={} "
      "decode-us<={} route-back-us<={} send-us<={}",
      ThisWorker().id_, stats.count_, p9999(stats.non_network_),
      p9999(stats.total_), p9999(stats.io_), p9999(stats.route_out_),
      p9999(stats.lookup_), p9999(stats.buffer_), p9999(stats.decode_),
      p9999(stats.route_back_), p9999(stats.send_));
  stats = ReadLatencyStats{};
  stats.next_report_ns_ =
      now + 10'000'000'000ULL + 100'000'000ULL * ThisWorker().id_;
}

struct SetLatencyStats {
  std::uint64_t count_ = 0;
  std::uint64_t remote_ = 0;
  std::uint64_t replication_ = 0;
  std::uint64_t allocated_blocks_ = 0;
  std::uint64_t next_report_ns_ = 0;
  LatencyDistribution total_;
  LatencyDistribution non_network_;
  LatencyDistribution route_out_;
  LatencyDistribution owner_;
  LatencyDistribution key_lock_;
  LatencyDistribution store_lock_;
  LatencyDistribution lookup_;
  LatencyDistribution append_;
  LatencyDistribution block_;
  LatencyDistribution encode_;
  LatencyDistribution index_;
  LatencyDistribution replication_publish_;
  LatencyDistribution route_back_;
  LatencyDistribution send_;
};

void RecordSetLatency(const SetLatencyTrace& trace) {
  static thread_local SetLatencyStats stats;
  if (trace.request_start_ns_ == 0 || trace.send_complete_ns_ == 0) return;
  ++stats.count_;
  stats.remote_ += trace.remote_;
  stats.replication_ += trace.replication_;
  stats.allocated_blocks_ += trace.allocated_block_;
  stats.total_.Add(Elapsed(trace.send_complete_ns_, trace.request_start_ns_));
  stats.non_network_.Add(
      Elapsed(trace.send_start_ns_, trace.request_start_ns_));
  stats.route_out_.Add(Elapsed(trace.owner_start_ns_, trace.request_start_ns_));
  stats.owner_.Add(Elapsed(trace.owner_done_ns_, trace.owner_start_ns_));
  stats.key_lock_.Add(
      Elapsed(trace.key_lock_acquired_ns_, trace.key_lock_start_ns_));
  stats.store_lock_.Add(
      Elapsed(trace.store_lock_acquired_ns_, trace.store_lock_start_ns_));
  stats.lookup_.Add(
      Elapsed(trace.lookup_done_ns_, trace.store_lock_acquired_ns_));
  stats.append_.Add(Elapsed(trace.append_done_ns_, trace.append_start_ns_));
  stats.block_.Add(Elapsed(trace.block_ready_ns_, trace.block_wait_start_ns_));
  stats.encode_.Add(Elapsed(trace.encode_done_ns_, trace.block_ready_ns_));
  stats.index_.Add(Elapsed(trace.index_done_ns_, trace.encode_done_ns_));
  stats.replication_publish_.Add(
      Elapsed(trace.replication_done_ns_, trace.append_done_ns_));
  stats.route_back_.Add(Elapsed(trace.origin_resume_ns_, trace.owner_done_ns_));
  stats.send_.Add(Elapsed(trace.send_complete_ns_, trace.send_start_ns_));

  const std::uint64_t now = trace.send_complete_ns_;
  if (stats.next_report_ns_ == 0) {
    stats.next_report_ns_ =
        now + 10'000'000'000ULL + 100'000'000ULL * ThisWorker().id_;
    return;
  }
  if (now < stats.next_report_ns_) return;

  const auto avg = [&](const LatencyDistribution& value) {
    return value.AverageUs(stats.count_);
  };
  const auto percentile = [&](const LatencyDistribution& value,
                              double requested) {
    return value.PercentileUpperUs(stats.count_, requested);
  };
  spdlog::info(
      "set-latency worker={} n={} remote={:.1f}% replication={:.1f}% "
      "block-alloc={:.3f}% avg-us total={:.1f} non-network={:.1f} "
      "route-out={:.1f} owner={:.1f} key-lock={:.1f} store-lock={:.1f} "
      "lookup={:.1f} append={:.1f} block={:.1f} encode={:.1f} index={:.1f} "
      "repl-publish={:.1f} route-back={:.1f} send={:.1f}",
      ThisWorker().id_, stats.count_,
      100.0 * static_cast<double>(stats.remote_) / stats.count_,
      100.0 * static_cast<double>(stats.replication_) / stats.count_,
      100.0 * static_cast<double>(stats.allocated_blocks_) / stats.count_,
      avg(stats.total_), avg(stats.non_network_), avg(stats.route_out_),
      avg(stats.owner_), avg(stats.key_lock_), avg(stats.store_lock_),
      avg(stats.lookup_), avg(stats.append_), avg(stats.block_),
      avg(stats.encode_), avg(stats.index_), avg(stats.replication_publish_),
      avg(stats.route_back_), avg(stats.send_));
  const auto log_percentile = [&](std::string_view label, double requested) {
    spdlog::info(
        "set-latency-{} worker={} n={} total-us<={} non-network-us<={} "
        "route-out-us<={} owner-us<={} key-lock-us<={} store-lock-us<={} "
        "lookup-us<={} append-us<={} block-us<={} encode-us<={} "
        "index-us<={} repl-publish-us<={} route-back-us<={} send-us<={}",
        label, ThisWorker().id_, stats.count_,
        percentile(stats.total_, requested),
        percentile(stats.non_network_, requested),
        percentile(stats.route_out_, requested),
        percentile(stats.owner_, requested),
        percentile(stats.key_lock_, requested),
        percentile(stats.store_lock_, requested),
        percentile(stats.lookup_, requested),
        percentile(stats.append_, requested),
        percentile(stats.block_, requested),
        percentile(stats.encode_, requested),
        percentile(stats.index_, requested),
        percentile(stats.replication_publish_, requested),
        percentile(stats.route_back_, requested),
        percentile(stats.send_, requested));
  };
  log_percentile("p99", 0.99);
  log_percentile("p99.9", 0.999);
  log_percentile("p99.99", 0.9999);
  stats = SetLatencyStats{};
  stats.next_report_ns_ =
      now + 10'000'000'000ULL + 100'000'000ULL * ThisWorker().id_;
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

absl::Status InstallShutdownSignalHandler() {
  g_shutdown_requested.store(false, std::memory_order_release);
  g_last_shutdown_signal = 0;
  g_signal_event_fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
  if (g_signal_event_fd < 0) {
    return absl::Status(absl::StatusCode::kInternal, "eventfd setup failed");
  }

  struct sigaction action {};
  sigemptyset(&action.sa_mask);
  action.sa_handler = ShutdownSignalHandler;
  if (sigaction(SIGINT, &action, nullptr) != 0 ||
      sigaction(SIGTERM, &action, nullptr) != 0) {
    close(g_signal_event_fd);
    g_signal_event_fd = -1;
    return absl::Status(absl::StatusCode::kInternal, "sigaction setup failed");
  }
  return absl::OkStatus();
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

Task<absl::Status> SampleMemory(Worker& worker);

class PasswordAuthenticator {
 public:
  explicit PasswordAuthenticator(std::string_view password)
      : required_(!password.empty()) {
    SHA256(reinterpret_cast<const unsigned char*>(password.data()),
           password.size(), digest_.data());
  }

  bool required() const noexcept { return required_; }

  bool Authenticate(std::string_view username,
                    std::string_view password) const noexcept {
    std::array<unsigned char, SHA256_DIGEST_LENGTH> candidate{};
    SHA256(reinterpret_cast<const unsigned char*>(password.data()),
           password.size(), candidate.data());
    const bool password_matches =
        CRYPTO_memcmp(candidate.data(), digest_.data(), digest_.size()) == 0;
    return required_ && username == "default" && password_matches;
  }

 private:
  bool required_ = false;
  std::array<unsigned char, SHA256_DIGEST_LENGTH> digest_{};
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
      const ssize_t result = read(server.completion_fd(), &wake, sizeof(wake));
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
               ReplicationManager* replication,
               long online_mimalloc_purge_delay_ms,
               std::string_view requirepass)
      : TcpService(port),
        storage_(storage),
        replication_(replication),
        online_mimalloc_purge_delay_ms_(online_mimalloc_purge_delay_ms),
        authenticator_(requirepass) {}

  void Prepare(unsigned thread_count) override;
  Task<absl::Status> Run(Worker& worker, ServiceContext ctx) override;
  bool startup_failed() const noexcept {
    return startup_failed_.load(std::memory_order_acquire);
  }
  void StopAcceptingRequests() noexcept;
  void WaitForRequestsDrained() const noexcept;

 protected:
  Task<absl::Status> Serve(TcpStream stream) override;

 private:
  Task<absl::Status> Serve(TcpStream& stream, ConnectionContext& ctx);

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
  long online_mimalloc_purge_delay_ms_;
  PasswordAuthenticator authenticator_;
  std::unique_ptr<CoroutineBarrier> recovery_ready_barrier_;
  std::unique_ptr<CoroutineBarrier> recovery_collect_barrier_;
  std::unique_ptr<CoroutineBarrier> online_allocator_barrier_;
  std::atomic<bool> startup_failed_{false};
  std::atomic<std::uint64_t> request_gate_{0};
};

void RedisService::Prepare(unsigned thread_count) {
  TcpService::Prepare(thread_count);
  recovery_ready_barrier_ = std::make_unique<CoroutineBarrier>(thread_count);
  recovery_collect_barrier_ = std::make_unique<CoroutineBarrier>(thread_count);
  online_allocator_barrier_ = std::make_unique<CoroutineBarrier>(thread_count);
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
    if (request_gate_.compare_exchange_weak(state, state + 1,
                                            std::memory_order_acq_rel,
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

Task<absl::Status> RedisService::Run(Worker& worker, ServiceContext ctx) {
  BindMemoryAccountingShard(worker.id());
  tx::TxRuntime::Get()->shard(worker.id()).Bind(worker);
  absl::Status status = co_await storage_->InitializeWorker(worker);
  if (!status.ok()) [[unlikely]] {
    startup_failed_.store(true, std::memory_order_release);
    spdlog::error("worker[{}] storage initialization failed: {}", worker.id(),
                  status.message());
    worker.RequestStop();
    co_return status;
  }

  // mi_collect is local to the calling thread's heap. First wait until every
  // recovery coroutine (and its temporary allocations) has been destroyed,
  // then collect on every worker while mimalloc still uses its native purge
  // delay. Switch the process-wide option only after all collectors finish.
  status = co_await recovery_ready_barrier_->Wait(worker);
  if (!status.ok()) [[unlikely]] {
    co_return status;
  }
  mi_collect(true);
  status = co_await recovery_collect_barrier_->Wait(worker);
  if (!status.ok()) [[unlikely]] {
    co_return status;
  }
  if (worker.id() == 0) {
    mi_option_set(mi_option_purge_delay, online_mimalloc_purge_delay_ms_);
    spdlog::info("mimalloc recovery collection complete; online purge_delay={}",
                 mi_option_get(mi_option_purge_delay));
  }
  status = co_await online_allocator_barrier_->Wait(worker);
  if (!status.ok()) [[unlikely]] {
    co_return status;
  }

  spdlog::info("worker[{}] direct-IO storage initialized", worker.id());
  if (worker.id() == 0) {
    worker.SpawnBackground(SampleMemory(worker));
  }
  replication_->StorageReady(worker);
  co_return co_await TcpService::Run(worker, ctx);
}

Task<absl::StatusOr<RespCommand>> ReadNextCommand(TcpStream& stream,
                                                  std::string* pending) {
  // Hard ceiling on one connection's accumulated request bytes. The per-frame
  // limits (INT_MAX args of up to 512 MiB each) still admit a claimed frame far
  // larger than RAM, and the buffer grows until the frame completes — without
  // a cap, one client streaming an oversized frame runs the process out of
  // memory. 1 GiB matches Redis's query buffer limit and comfortably fits
  // any legitimate command.
  constexpr std::size_t kMaxPendingBytes = 1ULL * 1024 * 1024 * 1024;
  std::array<std::byte, 4096> buffer{};
  while (true) {
    RespParseResult parsed = ParseRespCommand(*pending);
    if (parsed.state_ == RespParseState::kOk) {
      RespCommand command = std::move(parsed.command_);
      pending->erase(0, parsed.consumed_);
      co_return command;
    }
    if (parsed.state_ == RespParseState::kError) {
      co_return parsed.status_;
    }
    // Drop skipped filler (blank lines, empty multibulks) even while the
    // next real command is still incomplete: retaining it would grow the
    // buffer without bound and re-scan it from the start on every refill.
    if (parsed.consumed_ != 0) {
      pending->erase(0, parsed.consumed_);
    }
    if (pending->size() >= kMaxPendingBytes) {
      co_return absl::Status(absl::StatusCode::kResourceExhausted,
                             "client request exceeds the query buffer limit");
    }

    auto read_result = co_await stream.ReadSome(buffer);
    if (!read_result.ok()) [[unlikely]] {
      co_return read_result.status();
    }
    if (*read_result == 0) [[unlikely]] {
      co_return absl::Status(absl::StatusCode::kUnavailable,
                             "peer closed connection");
    }
    pending->append(reinterpret_cast<const char*>(buffer.data()), *read_result);
  }
}

bool ShutdownRequested() {
  return g_shutdown_requested.load(std::memory_order_acquire);
}

Task<absl::Status> SampleMemory(Worker& worker) {
  while (!worker.stop_requested()) {
    RefreshMemoryStats();
    absl::Status slept =
        co_await celer::SleepFor(worker, std::chrono::milliseconds(100));
    if (!slept.ok()) {
      co_return absl::OkStatus();
    }
  }
  co_return absl::OkStatus();
}

Task<absl::Status> RedisService::Serve(TcpStream stream) {
  static std::atomic<std::uint64_t> next_connection_id{1};
  ConnectionContext ctx;
  ctx.authenticated_ = !authenticator_.required();
  ctx.conn_id_ = next_connection_id.fetch_add(1, std::memory_order_relaxed);
  auto peer_address = stream.PeerAddress();
  const std::string address =
      peer_address.ok() ? std::move(*peer_address) : std::string("?:0");
  const bool tls = stream.IsTls();
  RegisterClientConnection(ctx.conn_id_, stream.NativeFd(), address, tls);
  ConnectionOpened();
  const absl::Status status = co_await Serve(stream, ctx);
  // Single connection-scoped cleanup point: every disconnect path funnels
  // through this co_return.
  UnregisterClientConnection(ctx.conn_id_);
  co_await ReleaseConnectionWatches(ctx);
  if (ctx.counted_as_client_) ConnectionClosed();
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
  std::chrono::steady_clock::time_point last_progress_;
  bool done_ = false;  // same-worker access only
};

// Watchdog for one streamed reply. shutdown() rather than close: it fails
// the parked write immediately without releasing the descriptor out from
// under the pending io_uring operation, and the serve loop's normal
// teardown then reopens the gate and closes the socket.
Task<absl::Status> BreakStalledStream(std::shared_ptr<StreamStallState> state,
                                      int fd) {
  while (!state->done_) {
    const auto deadline = state->last_progress_ + kStreamStallLimit;
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) {
      ::shutdown(fd, SHUT_RDWR);
      co_return absl::OkStatus();
    }
    absl::Status slept = co_await celer::SleepFor(
        *ThisWorker().self_,
        std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now) +
            std::chrono::milliseconds(1));
    if (!slept.ok()) {
      co_return slept;  // worker shutting down
    }
  }
  co_return absl::OkStatus();
}

Task<absl::Status> RedisService::Serve(TcpStream& stream,
                                       ConnectionContext& ctx) {
  std::string pending;

  while (stream.IsOpen()) {
    ctx.reply_builder_.Reset();
    if (ShutdownRequested()) [[unlikely]] {
      co_return absl::OkStatus();
    }

    auto command_result = co_await ReadNextCommand(stream, &pending);
    if (!command_result.ok()) [[unlikely]] {
      if (command_result.status().code() == absl::StatusCode::kUnavailable)
          [[unlikely]] {
        co_return absl::OkStatus();
      }

      const std::string_view encoded = ctx.reply_builder_.AppendError(
          absl::StrCat("ERR ", command_result.status().message()));
      auto write_status = co_await stream.WriteAll(std::span<const std::byte>(
          reinterpret_cast<const std::byte*>(encoded.data()), encoded.size()));
      if (!write_status.ok()) [[unlikely]] {
        co_return write_status;
      }
      co_return command_result.status();
    }

    const auto& args = command_result->args_;
    if (!args.empty() && absl::EqualsIgnoreCase(args.front(), "AUTH")) {
      std::string_view encoded;
      if (args.size() != 2 && args.size() != 3) {
        encoded = ctx.reply_builder_.AppendError(
            "ERR wrong number of arguments for 'auth' command");
      } else if (!authenticator_.required()) {
        encoded = ctx.reply_builder_.AppendError(
            "ERR AUTH called without any password configured for the default "
            "user. Are you sure your configuration is correct?");
      } else {
        const std::string_view username =
            args.size() == 2 ? std::string_view("default") : args[1];
        const std::string_view password = args.back();
        if (authenticator_.Authenticate(username, password)) {
          ctx.authenticated_ = true;
          encoded = ctx.reply_builder_.AppendSimpleString("OK");
        } else {
          encoded = ctx.reply_builder_.AppendError(
              "WRONGPASS invalid username-password pair or user is "
              "disabled.");
        }
      }
      absl::Status written =
          co_await stream.WriteAll(std::span<const std::byte>(
              reinterpret_cast<const std::byte*>(encoded.data()),
              encoded.size()));
      if (!written.ok()) co_return written;
      continue;
    }

    if (!ctx.authenticated_) {
      const std::string_view encoded =
          ctx.reply_builder_.AppendError("NOAUTH Authentication required.");
      absl::Status written =
          co_await stream.WriteAll(std::span<const std::byte>(
              reinterpret_cast<const std::byte*>(encoded.data()),
              encoded.size()));
      if (!written.ok()) co_return written;
      continue;
    }

    if (ReplicationManager::IsNativeHandshake(command_result->args_)) {
      if (!pending.empty()) {
        co_return absl::InvalidArgumentError(
            "replication handshake must be the first isolated command");
      }
      ConnectionClosed();
      ctx.counted_as_client_ = false;
      auto peer_address = stream.PeerAddress();
      const std::string address =
          peer_address.ok() ? std::move(*peer_address) : std::string("?:0");
      const bool tls = stream.IsTls();
      UnregisterClientConnection(ctx.conn_id_);
      co_return co_await replication_->ServeNativeConnection(
          stream, std::move(command_result->args_), ctx.conn_id_, address, tls);
    }

    if (!TryBeginRequest()) [[unlikely]] {
      const std::string_view encoded =
          ctx.reply_builder_.AppendError("ERR server is shutting down");
      auto write_status = co_await stream.WriteAll(std::span<const std::byte>(
          reinterpret_cast<const std::byte*>(encoded.data()), encoded.size()));
      stream.Close().IgnoreError();
      co_return write_status;
    }
    RequestGuard request_guard(this);

    auto request_result =
        BuildCommandRequest(std::move(*command_result), ctx.selected_db_);
    CommandReply reply;
    if (!request_result.ok()) [[unlikely]] {
      reply.encoded_ = ctx.reply_builder_.AppendError(
          absl::StrCat("ERR ", request_result.status().message()));
    } else {
      reply = co_await DispatchCommand(ctx, std::move(*request_result),
                                       ctx.reply_builder_);
    }
    if (reply.selected_db_.has_value()) {
      ctx.selected_db_ = *reply.selected_db_;
    }

    // Streamed replies hold the database gate at the peer's pace; arm the
    // stall watchdog for the whole stream, header included. The scope guard
    // retires it on every exit path, including error co_returns.
    std::shared_ptr<StreamStallState> stall;
    struct RetireStall {
      std::shared_ptr<StreamStallState> state_;
      ~RetireStall() {
        if (state_ != nullptr) {
          state_->done_ = true;
        }
      }
    } retire_stall;
    if (reply.chunks_) {
      stall = std::make_shared<StreamStallState>();
      stall->last_progress_ = std::chrono::steady_clock::now();
      retire_stall.state_ = stall;
      ThisWorker().self_->Spawn(BreakStalledStream(stall, stream.NativeFd()));
    }

    absl::Status write_status;
    if (reply.read_trace_.request_start_ns_ != 0) {
      reply.read_trace_.send_start_ns_ = ReadTraceNowNanos();
    }
    if (reply.set_trace_.request_start_ns_ != 0) {
      reply.set_trace_.send_start_ns_ = SetTraceNowNanos();
    }
    if (reply.disk_value_.has_value()) {
      write_status =
          co_await stream.WriteAll(reply.disk_value_->network_bytes());
    } else {
      write_status = co_await stream.WriteAll(std::span<const std::byte>(
          reinterpret_cast<const std::byte*>(reply.encoded_.data()),
          reply.encoded_.size()));
    }
    // Streamed continuation (KEYS): drain bounded chunks onto the socket.
    // The reply header already committed the element count, so a chunk
    // failure can only end the connection.
    while (write_status.ok() && reply.chunks_) {
      if (stall != nullptr) {
        stall->last_progress_ = std::chrono::steady_clock::now();
      }
      auto chunk = co_await reply.chunks_();
      if (!chunk.ok()) {
        co_return chunk.status();
      }
      if (chunk->empty()) {
        break;
      }
      // Write in bounded segments and stamp progress after each one: the
      // watchdog then judges liveness per segment, so a client draining a
      // large chunk at a modest rate is never mistaken for a stalled one.
      constexpr std::size_t kWriteSegmentBytes = 256 * 1024;
      std::span<const std::byte> remaining(
          reinterpret_cast<const std::byte*>(chunk->data()), chunk->size());
      while (write_status.ok() && !remaining.empty()) {
        const std::size_t segment =
            std::min(kWriteSegmentBytes, remaining.size());
        write_status = co_await stream.WriteAll(remaining.first(segment));
        remaining = remaining.subspan(segment);
        if (stall != nullptr) {
          stall->last_progress_ = std::chrono::steady_clock::now();
        }
      }
    }
    if (reply.read_trace_.request_start_ns_ != 0) {
      reply.read_trace_.send_complete_ns_ = ReadTraceNowNanos();
      RecordReadLatency(reply.read_trace_);
    }
    if (reply.set_trace_.request_start_ns_ != 0) {
      reply.set_trace_.send_complete_ns_ = SetTraceNowNanos();
      RecordSetLatency(reply.set_trace_);
    }
    if (!write_status.ok()) [[unlikely]] {
      co_return write_status;
    }
    if (reply.close_connection_ || ShutdownRequested()) [[unlikely]] {
      stream.Close().IgnoreError();
      co_return absl::OkStatus();
    }
  }

  co_return absl::OkStatus();
}

}  // namespace

int RunServer(ServerOptions options) {
  const absl::Status validated = ValidateServerOptions(options);
  if (!validated.ok()) {
    spdlog::error("configuration error: {}", validated.message());
    return 1;
  }
  const std::string bind_display = absl::StrJoin(options.bind_addresses_, ",");
  std::string advertised_bind = options.bind_addresses_.front();
  if (advertised_bind == "*") advertised_bind = "0.0.0.0";
  const std::uint16_t advertised_port =
      options.port_ != 0 ? options.port_ : options.tls_port_;

  std::shared_ptr<celer::TlsContext> tls_server_context;
  if (options.tls_port_ != 0) {
    celer::TlsClientAuth client_auth = celer::TlsClientAuth::kNo;
    if (options.tls_auth_clients_ == "optional") {
      client_auth = celer::TlsClientAuth::kOptional;
    } else if (options.tls_auth_clients_ == "yes") {
      client_auth = celer::TlsClientAuth::kRequired;
    }
    auto created = celer::TlsContext::CreateServer(celer::TlsServerOptions{
        .cert_file_ = options.tls_cert_file_,
        .key_file_ = options.tls_key_file_,
        .ca_cert_file_ = options.tls_ca_cert_file_,
        .client_auth_ = client_auth,
    });
    if (!created.ok()) {
      spdlog::error("TLS server setup failed: {}", created.status().message());
      return 1;
    }
    tls_server_context = std::move(*created);
  }

  std::shared_ptr<celer::TlsContext> tls_client_context;
  if (options.tls_replication_) {
    auto created = celer::TlsContext::CreateClient(celer::TlsClientOptions{
        .ca_cert_file_ = options.tls_ca_cert_file_,
        .cert_file_ = options.tls_cert_file_,
        .key_file_ = options.tls_key_file_,
    });
    if (!created.ok()) {
      spdlog::error("TLS replication setup failed: {}",
                    created.status().message());
      return 1;
    }
    tls_client_context = std::move(*created);
  }
  spdlog::info(
      "mimalloc recovery_purge_delay={} online_purge_delay={} "
      "arena_eager_commit={} allow_thp={}",
      mi_option_get(mi_option_purge_delay), options.mimalloc_purge_delay_ms_,
      mi_option_get(mi_option_arena_eager_commit),
      mi_option_get(mi_option_allow_thp));
  spdlog::info(
      "keylane listening on {}:{} tls_port={} metrics_port={} threads={} "
      "pin_workers={} "
      "idle_timeout_ms={} "
      "busy_poll_us={} background_budget_us={} "
      "background_warrant_percent={} "
      "spdk_max_completions_per_poll={} spdk_foreground_pre_poll_us={} "
      "registered_buffer_bytes={} per worker "
      "storage_write_buffers={} "
      "storage_read_buffer_bytes={} "
      "replication_publish_queue_bytes={} per worker max_memory={} "
      "flush_max_ms={} "
      "flush_size_bytes={} "
      "inline_key_max_bytes={} verify_read_crc={} "
      "defrag_max_active_per_device={} defrag_sleep_ms={} "
      "defrag_record_sleep_us={} defrag_paused={}",
      bind_display, options.port_, options.tls_port_, options.metrics_port_,
      options.thread_count_, options.pin_workers_, options.idle_timeout_ms_,
      options.busy_poll_us_, options.background_budget_us_,
      options.background_warrant_percent_,
      options.spdk_max_completions_per_poll_,
      options.spdk_foreground_pre_poll_us_, options.registered_buffer_bytes_,
      options.storage_write_buffer_count_, options.storage_read_buffer_bytes_,
      options.replication_publish_queue_bytes_, options.max_memory_bytes_,
      options.flush_max_ms_, options.flush_size_bytes_,
      options.inline_key_max_bytes_, options.verify_read_crc_,
      options.defrag_max_active_per_device_, options.defrag_sleep_ms_,
      options.defrag_record_sleep_us_, options.defrag_paused_);

  const absl::Status memory_status =
      InitMemoryLimit(options.max_memory_bytes_, options.thread_count_);
  if (!memory_status.ok()) {
    spdlog::error("memory limit setup failed: {}", memory_status.message());
    return 1;
  }
  const MemoryStats initial_memory = GetMemoryStats();
  spdlog::info("memory limit={} ({}) initial_used={} initial_rss={}",
               initial_memory.max_bytes_,
               HumanReadableMemory(initial_memory.max_bytes_),
               initial_memory.used_bytes_, initial_memory.rss_bytes_);

  const auto signal_status = InstallShutdownSignalHandler();
  if (!signal_status.ok()) [[unlikely]] {
    spdlog::error("signal setup failed: {}", signal_status.message());
    return 1;
  }

  storage::StorageEngineOptions storage_options;
  storage_options.data_files_ = std::move(options.data_files_);
  storage_options.flush_max_ms_ = options.flush_max_ms_;
  storage_options.flush_size_bytes_ = options.flush_size_bytes_;
  storage_options.replication_publish_queue_bytes_ =
      options.replication_publish_queue_bytes_;
  storage_options.verify_read_crc_ = options.verify_read_crc_;
  storage_options.inline_key_max_bytes_ = options.inline_key_max_bytes_;
  // A node configured with an upstream must not create local
  // expiration mutation sequences. It still hides expired values by their
  // absolute deadline and applies the primary's replicated tombstone.
  storage_options.expiration_authority_ = !options.replicaof_.has_value();
  storage_options.tomb_raider_interval_ms_ = options.tomb_raider_interval_ms_;
  storage_options.tomb_raider_sleep_ms_ = options.tomb_raider_sleep_ms_;
  storage_options.defrag_max_active_per_device_ =
      options.defrag_max_active_per_device_;
  storage_options.defrag_sleep_ms_ = options.defrag_sleep_ms_;
  storage_options.defrag_record_sleep_us_ = options.defrag_record_sleep_us_;
  storage_options.defrag_paused_ = options.defrag_paused_;
  storage_options.buffers_.registered_bytes_ = options.registered_buffer_bytes_;
  storage_options.buffers_.storage_write_buffer_count_ =
      options.storage_write_buffer_count_;
  storage_options.buffers_.read_payload_bytes_ =
      options.storage_read_buffer_bytes_;
  storage::StorageEngine storage(std::move(storage_options));
  absl::Status storage_status = storage.Prepare(options.thread_count_);
  if (!storage_status.ok()) [[unlikely]] {
    spdlog::error("storage prepare failed: {}", storage_status.message());
    CleanupShutdownSignalHandler();
    return 1;
  }
  options.replication_options_.listen_port_ = options.port_;
  if (options.tls_replication_ && options.tls_port_ != 0) {
    options.replication_options_.listen_port_ = options.tls_port_;
  }
  options.replication_options_.use_tls_ = options.tls_replication_;
  options.replication_options_.tls_context_ = std::move(tls_client_context);
  options.replication_options_.masteruser_ = options.masteruser_;
  options.replication_options_.masterauth_ = options.masterauth_;
  options.replication_options_.publish_queue_bytes_per_worker_ =
      options.replication_publish_queue_bytes_;
  ReplicationManager replication(&storage,
                                 std::move(options.replication_options_),
                                 std::move(options.replicaof_));
  InitStorage(&storage, &replication);
  InitWorkerMetrics(options.thread_count_);
  SetServerInfo(std::move(advertised_bind), advertised_port,
                options.thread_count_);
  tx::TxRuntime::Create(options.thread_count_);

  celer::ServerOptions runtime_options;
  runtime_options.bind_addresses_ = options.bind_addresses_;
  runtime_options.thread_count_ = options.thread_count_;
  runtime_options.pin_workers_ = options.pin_workers_;
  runtime_options.idle_timeout_ms_ = options.idle_timeout_ms_;
  runtime_options.recv_buffer_count_ = options.recv_buffer_count_;
  runtime_options.busy_poll_us_ = options.busy_poll_us_;
  runtime_options.background_budget_us_ = options.background_budget_us_;
  runtime_options.background_warrant_percent_ =
      options.background_warrant_percent_;
  runtime_options.spdk_max_completions_per_poll_ =
      options.spdk_max_completions_per_poll_;
  runtime_options.spdk_foreground_pre_poll_us_ =
      options.spdk_foreground_pre_poll_us_;

  RedisService redis(options.port_, &storage, &replication,
                     options.mimalloc_purge_delay_ms_, options.requirepass_);
  if (tls_server_context != nullptr) {
    redis.AddTlsEndpoint(options.tls_port_, std::move(tls_server_context));
  }
  std::unique_ptr<Service> metrics;
  Server server;
  server.AddService(&redis);
  if (options.metrics_port_ != 0) {
    metrics = CreateMetricsService(options.metrics_port_, &storage);
    server.AddService(metrics.get());
  }
  auto start_status = server.Start(runtime_options);
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
    absl::Status flush_status = storage.FlushForShutdown();
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
