/*
 * Copyright (C) 2026 EloqData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "lavik/server.h"

#include <mimalloc.h>
#include <openssl/crypto.h>
#include <openssl/sha.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>

#include "absl/container/inlined_vector.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "backup.h"
#include "blocking_wait.h"
#include "bycorf/io/backend_options.h"
#include "bycorf/net/server.h"
#include "bycorf/net/tcp_service.h"
#include "bycorf/net/tcp_stream.h"
#include "bycorf/net/tls.h"
#include "bycorf/runtime/sync.h"
#include "client_limit.h"
#include "function_catalog.h"
#include "lavik/cluster/meta_client.h"
#include "lavik/cluster/runtime.h"
#include "lavik/command.h"
#include "lavik/command_table.h"
#include "lavik/config.h"
#include "lavik/memory.h"
#include "lavik/metrics.h"
#include "lavik/monitor.h"
#include "lavik/pubsub.h"
#include "lavik/rdb.h"
#include "lavik/replication.h"
#include "lavik/resp.h"
#include "lavik/session.h"
#include "lavik/slowlog.h"
#include "lavik/storage/engine.h"
#include "lavik/tx/tx_shard.h"
#include "lavik/version.h"
#include "lua_eval.h"
#include "request_gate.h"
#include "spdlog/spdlog.h"

namespace lavik {
using namespace bycorf;

namespace {

#if BYCORF_ENABLE_CROSS_CORE_LATENCY_TRACE
constexpr std::uint64_t kReadLatencyReportIntervalNs = 45'000'000'000ULL;
#else
constexpr std::uint64_t kReadLatencyReportIntervalNs = 10'000'000'000ULL;
#endif

constexpr std::array<std::uint64_t, 28> kLatencyBucketUpperUs{
    1,    2,    3,    4,    5,    8,     10,    15,   20,  30,
    40,   50,   75,   100,  150,  200,   300,   500,  750, 1000,
    1500, 2000, 3000, 5000, 8000, 10000, 20000, 50000};

// Client sockets must never consume the descriptors needed by listeners,
// io_uring, storage, replication, metrics, logging, and transient maintenance
// work. This is deliberately larger than Redis's reserve because Lavik has
// several multi-worker subsystems that keep descriptors open.
constexpr std::uint64_t kMaxClientsFileDescriptorReserve = 256;

CommandRequest BuildParsedCommandRequest(RespCommand command,
                                         std::uint8_t db_id) {
  // RespCommandParser reports kOk only after producing at least the command
  // name. Keep the checked builder for synthesized and wire-replay commands,
  // but do not wrap every client request in StatusOr merely to recheck this
  // parser invariant in the hottest coroutine frame.
  assert(!command.args_.empty());
  CommandRequest request;
  request.spec_ = FindCommand(command.args_.front());
  request.kind_ =
      request.spec_ != nullptr ? request.spec_->kind_ : CommandKind::kUnknown;
  request.db_id_ = db_id;
  request.args_ = std::move(command.args_);
  return request;
}

absl::StatusOr<std::uint64_t> MaxClientsAllowedByFileLimit(
    std::uint64_t requested) {
  rlimit limit{};
  if (::getrlimit(RLIMIT_NOFILE, &limit) != 0) {
    return absl::InternalError(
        absl::StrCat("unable to read RLIMIT_NOFILE: ", std::strerror(errno)));
  }
  if (limit.rlim_cur == RLIM_INFINITY) return requested;

  const rlim_t rlim_max = std::numeric_limits<rlim_t>::max();
  const bool wanted_is_infinite =
      requested > rlim_max - kMaxClientsFileDescriptorReserve;
  const rlim_t wanted =
      wanted_is_infinite
          ? RLIM_INFINITY
          : static_cast<rlim_t>(requested + kMaxClientsFileDescriptorReserve);
  if (limit.rlim_cur < wanted) {
    const rlim_t target = limit.rlim_max == RLIM_INFINITY
                              ? wanted
                              : std::min(wanted, limit.rlim_max);
    if (target > limit.rlim_cur) {
      rlimit raised = limit;
      raised.rlim_cur = target;
      if (::setrlimit(RLIMIT_NOFILE, &raised) == 0) {
        limit.rlim_cur = target;
      } else {
        spdlog::warn("unable to raise RLIMIT_NOFILE from {} to {}: {}",
                     limit.rlim_cur, target, std::strerror(errno));
      }
    }
  }

  if (limit.rlim_cur == RLIM_INFINITY) return requested;
  if (limit.rlim_cur <= kMaxClientsFileDescriptorReserve) return 0;
  return std::min<std::uint64_t>(
      requested, limit.rlim_cur - kMaxClientsFileDescriptorReserve);
}

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
        now + kReadLatencyReportIntervalNs + 100'000'000ULL * ThisWorker().id_;
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
#if BYCORF_ENABLE_CROSS_CORE_LATENCY_TRACE
  const auto cross_core_stats = ThisWorker().self_->TakeCrossCoreLatencyStats();
  const auto log_cross_core = [&](std::string_view name,
                                  const Worker::LatencySampleStats& value) {
    spdlog::info(
        "cross-core-latency worker={} kind={} n={} avg-us={:.2f} "
        "p99.9-us<={} p99.99-us<={} max-us={:.2f}",
        ThisWorker().id_, name, value.count_, value.AverageUs(),
        value.PercentileUpperUs(0.999), value.PercentileUpperUs(0.9999),
        static_cast<double>(value.max_ns_) / 1000.0);
  };
  log_cross_core("wake-batch", cross_core_stats.wake_batch_wait_);
  log_cross_core("parked-wake-batch", cross_core_stats.parked_wake_batch_wait_);
  log_cross_core("request-queue", cross_core_stats.request_queue_);
  log_cross_core("reply-queue", cross_core_stats.reply_queue_);
#endif
  stats = ReadLatencyStats{};
  stats.next_report_ns_ =
      now + kReadLatencyReportIntervalNs + 100'000'000ULL * ThisWorker().id_;
}

struct SetLatencyStats {
  std::uint64_t count_ = 0;
  std::uint64_t remote_ = 0;
  std::uint64_t replication_ = 0;
  std::uint64_t allocated_blocks_ = 0;
  std::uint64_t standby_blocks_ = 0;
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
  stats.standby_blocks_ += trace.standby_block_;
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
      "block-alloc={:.3f}% standby-hit={:.3f}% avg-us total={:.1f} "
      "non-network={:.1f} "
      "route-out={:.1f} owner={:.1f} key-lock={:.1f} store-lock={:.1f} "
      "lookup={:.1f} append={:.1f} block={:.1f} encode={:.1f} index={:.1f} "
      "repl-publish={:.1f} route-back={:.1f} send={:.1f}",
      ThisWorker().id_, stats.count_,
      100.0 * static_cast<double>(stats.remote_) / stats.count_,
      100.0 * static_cast<double>(stats.replication_) / stats.count_,
      100.0 * static_cast<double>(stats.allocated_blocks_) / stats.count_,
      100.0 * static_cast<double>(stats.standby_blocks_) / stats.count_,
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

  struct sigaction action{};
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
  struct sigaction action{};
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
    if (!required_) return username == "default";
    std::array<unsigned char, SHA256_DIGEST_LENGTH> candidate{};
    SHA256(reinterpret_cast<const unsigned char*>(password.data()),
           password.size(), candidate.data());
    const bool password_matches =
        CRYPTO_memcmp(candidate.data(), digest_.data(), digest_.size()) == 0;
    return username == "default" && password_matches;
  }

 private:
  bool required_ = false;
  std::array<unsigned char, SHA256_DIGEST_LENGTH> digest_{};
};

bool ValidClientName(std::string_view name) {
  return std::none_of(name.begin(), name.end(), [](unsigned char c) {
    return c == 0 || std::isspace(c);
  });
}

std::string_view ExecuteHello(const PasswordAuthenticator& authenticator,
                              ReplicationManager* replication,
                              ConnectionContext& ctx,
                              std::span<const std::string> args,
                              ReplyBuilder& reply) {
  RespVersion requested = ctx.resp_version();
  std::optional<std::pair<std::string_view, std::string_view>> credentials;
  std::optional<std::string_view> client_name;

  if (args.size() > 1) {
    if (args[1] == "2") {
      requested = RespVersion::k2;
    } else if (args[1] == "3") {
      requested = RespVersion::k3;
    } else {
      return reply.AppendError("NOPROTO unsupported protocol version");
    }
    for (std::size_t i = 2; i < args.size();) {
      if (absl::EqualsIgnoreCase(args[i], "AUTH")) {
        if (credentials.has_value() || i + 2 >= args.size()) {
          return reply.AppendError("ERR syntax error");
        }
        credentials.emplace(args[i + 1], args[i + 2]);
        i += 3;
      } else if (absl::EqualsIgnoreCase(args[i], "SETNAME")) {
        if (client_name.has_value() || i + 1 >= args.size()) {
          return reply.AppendError("ERR syntax error");
        }
        client_name = args[i + 1];
        i += 2;
      } else {
        return reply.AppendError("ERR syntax error");
      }
    }
  }

  if (client_name.has_value() && !ValidClientName(*client_name)) {
    return reply.AppendError(
        "ERR Client names cannot contain spaces, newlines or special "
        "characters.");
  }
  if (credentials.has_value() &&
      !authenticator.Authenticate(credentials->first, credentials->second)) {
    return reply.AppendError(
        "WRONGPASS invalid username-password pair or user is disabled.");
  }
  if (!ctx.authenticated_ && !credentials.has_value()) {
    return reply.AppendError(
        "NOAUTH HELLO must be called with the client already authenticated, "
        "otherwise the HELLO AUTH <user> <pass> option can be used to "
        "authenticate the client and select the RESP protocol version at the "
        "same time");
  }

  // Nothing above this point mutates connection state. A failed HELLO leaves
  // authentication, name and protocol exactly as they were.
  if (credentials.has_value()) ctx.authenticated_ = true;
  if (client_name.has_value()) {
    ctx.client_name_ = *client_name;
    SetClientName(ctx.conn_id_, ctx.client_name_);
  }
  ctx.SetRespVersion(requested);
  SetClientRespVersion(ctx.conn_id_, requested);

  reply.SetVersion(requested);
  reply.AppendMapHeader(7);
  reply.AppendBulkString("server");
  reply.AppendBulkString("lavik");
  reply.AppendBulkString("version");
  reply.AppendBulkString(kVersion);
  reply.AppendBulkString("proto");
  reply.AppendInteger(static_cast<unsigned>(requested));
  reply.AppendBulkString("id");
  reply.AppendInteger(static_cast<long long>(std::min<std::uint64_t>(
      ctx.conn_id_, std::numeric_limits<long long>::max())));
  reply.AppendBulkString("mode");
  reply.AppendBulkString(cluster::ClusterEnabled() ? "cluster" : "standalone");
  reply.AppendBulkString("role");
  reply.AppendBulkString(
      replication != nullptr && replication->is_replica() ? "slave" : "master");
  reply.AppendBulkString("modules");
  reply.AppendArrayHeader(0);
  return reply.View();
}

std::string_view ExecuteHelloFromContext(const void* authenticator,
                                         void* replication,
                                         ConnectionContext& ctx,
                                         std::span<const std::string> args,
                                         ReplyBuilder& reply) {
  return ExecuteHello(*static_cast<const PasswordAuthenticator*>(authenticator),
                      static_cast<ReplicationManager*>(replication), ctx, args,
                      reply);
}

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

class RequestInputBuffer;
class CommandBatch;

class RedisService final : public TcpService, public ClientLimit {
 public:
  RedisService(std::uint16_t port, storage::StorageEngine* storage,
               ReplicationManager* replication,
               long online_mimalloc_purge_delay_ms,
               std::string_view requirepass, std::string load_rdb_file,
               std::uint64_t max_clients,
               std::size_t client_query_buffer_limit_bytes)
      : TcpService(port),
        storage_(storage),
        replication_(replication),
        online_mimalloc_purge_delay_ms_(online_mimalloc_purge_delay_ms),
        authenticator_(requirepass),
        load_rdb_file_(std::move(load_rdb_file)),
        max_clients_(max_clients),
        client_query_buffer_limit_bytes_(client_query_buffer_limit_bytes) {}

  void Prepare(unsigned thread_count) override;
  Task<absl::Status> Run(Worker& worker, ServiceContext ctx) override;
  void FinalizeWorker(Worker& worker) noexcept override {
    storage_->FinalizeWorker(worker);
  }
  bool startup_failed() const noexcept {
    return startup_failed_.load(std::memory_order_acquire);
  }
  bool runtime_failure_cleanup_failed() const noexcept {
    return runtime_failure_cleanup_failed_.load(std::memory_order_acquire);
  }
  bool ready() const noexcept { return ready_.load(std::memory_order_acquire); }
  void BindServer(Server* server) noexcept { server_ = server; }
  void StopAcceptingRequests() noexcept;
  void WaitForRequestsDrained() const noexcept;
  absl::Status WaitForReplicationQuiesced();
  std::uint64_t max_clients() const noexcept override {
    return max_clients_.load(std::memory_order_acquire);
  }
  absl::Status SetMaxClients(std::uint64_t value) override;
  std::size_t client_query_buffer_limit() const noexcept override {
    return client_query_buffer_limit_bytes_.load(std::memory_order_acquire);
  }
  absl::Status SetClientQueryBufferLimit(std::size_t value) override;

 protected:
  bool AdmitConnection(int fd, bool tls_endpoint) noexcept override;
  void OnConnectionClosed() noexcept override;
  Task<absl::Status> Serve(TcpStream stream) override;

 private:
  Task<absl::Status> Serve(TcpStream& stream, ConnectionContext& ctx);
  Task<absl::Status> ReadSubscribedCommands(
      TcpStream& stream, ConnectionContext& ctx, RequestInputBuffer* input,
      RespCommandParser* parser, CommandBatch* ready,
      ClientBufferReservation* client_buffers,
      std::size_t* unassigned_input_bytes, std::size_t* multi_input_bytes,
      std::optional<absl::Status>* deferred_read_error,
      std::shared_ptr<PubSubSession> session);
  Task<absl::Status> ServeSubscribed(
      TcpStream& stream, ConnectionContext& ctx, RequestInputBuffer* input,
      RespCommandParser* parser, CommandBatch* ready,
      ClientBufferReservation* client_buffers,
      std::size_t* unassigned_input_bytes, std::size_t* multi_input_bytes,
      std::optional<absl::Status>* deferred_read_error);
  Task<absl::Status> ImportRdb();
  Task<absl::Status> MonitorRuntimeHealth(Worker& worker);

  class RequestGuard {
   public:
    explicit RequestGuard(RedisService* service) : service_(service) {}
    RequestGuard(const RequestGuard&) = delete;
    RequestGuard& operator=(const RequestGuard&) = delete;
    ~RequestGuard() {
      if (service_ != nullptr) service_->EndRequest();
    }

    void Release() noexcept {
      if (service_ == nullptr) return;
      service_->EndRequest();
      service_ = nullptr;
    }

   private:
    RedisService* service_;
  };

  bool TryBeginRequest() noexcept;
  void EndRequest() noexcept;

  storage::StorageEngine* storage_;
  ReplicationManager* replication_;
  long online_mimalloc_purge_delay_ms_;
  PasswordAuthenticator authenticator_;
  std::string load_rdb_file_;
  // Only RedisService owns these counters. Other TcpService users, including
  // the metrics HTTP service, never participate in maxclients admission.
  std::atomic<std::uint64_t> max_clients_;
  // Loaded once per socket-read parsing round, not once per RESP token.
  std::atomic<std::size_t> client_query_buffer_limit_bytes_;
  std::atomic<std::uint64_t> active_clients_{0};
  std::unique_ptr<CoroutineBarrier> recovery_ready_barrier_;
  std::unique_ptr<CoroutineBarrier> recovery_collect_barrier_;
  std::unique_ptr<CoroutineBarrier> online_allocator_barrier_;
  std::unique_ptr<CoroutineBarrier> rdb_import_barrier_;
  std::mutex rdb_import_status_mutex_;
  absl::Status rdb_import_status_;
  std::atomic<bool> startup_failed_{false};
  std::atomic<bool> runtime_failure_cleanup_failed_{false};
  std::atomic<bool> ready_{false};
  // The process main thread requests this barrier only after closing client
  // admission. Worker zero performs the coroutine-affine cancellation and
  // joins every native/Redis target root before the main thread may checkpoint
  // storage.
  std::atomic<bool> replication_quiesce_requested_{false};
  std::mutex replication_quiesce_mutex_;
  std::condition_variable replication_quiesce_cv_;
  bool replication_quiesce_monitor_available_ = false;
  bool replication_quiesce_complete_ = false;
  absl::Status replication_quiesce_status_;
  RequestGate request_gate_;
  Server* server_ = nullptr;
};

bool RedisService::AdmitConnection(int fd, bool tls_endpoint) noexcept {
  std::uint64_t active = active_clients_.load(std::memory_order_relaxed);
  while (active < max_clients_.load(std::memory_order_acquire)) {
    if (active_clients_.compare_exchange_weak(active, active + 1,
                                              std::memory_order_acq_rel,
                                              std::memory_order_relaxed)) {
      return true;
    }
  }

  if (tls_endpoint) return false;

  constexpr std::string_view kMaxClientsError =
      "-ERR max number of clients reached\r\n";
  std::string_view remaining = kMaxClientsError;
  while (!remaining.empty()) {
    const ssize_t sent = ::send(fd, remaining.data(), remaining.size(),
                                MSG_DONTWAIT | MSG_NOSIGNAL);
    if (sent > 0) {
      remaining.remove_prefix(static_cast<std::size_t>(sent));
      continue;
    }
    if (sent < 0 && errno == EINTR) continue;
    return false;
  }
  return false;
}

absl::Status RedisService::SetMaxClients(std::uint64_t value) {
  auto allowed = MaxClientsAllowedByFileLimit(value);
  if (!allowed.ok()) return allowed.status();
  if (*allowed < value) {
    return absl::ResourceExhaustedError(absl::StrCat(
        "maxclients ", value, " cannot preserve ",
        kMaxClientsFileDescriptorReserve,
        " file descriptors with the current RLIMIT_NOFILE; maximum is ",
        *allowed));
  }
  max_clients_.store(value, std::memory_order_release);
  return absl::OkStatus();
}

absl::Status RedisService::SetClientQueryBufferLimit(std::size_t value) {
  if (value < kMinimumClientQueryBufferLimit ||
      value > static_cast<std::size_t>(std::numeric_limits<long>::max())) {
    return absl::InvalidArgumentError(
        "client-query-buffer-limit must be between 1mb and LONG_MAX bytes");
  }
  client_query_buffer_limit_bytes_.store(value, std::memory_order_release);
  return absl::OkStatus();
}

void RedisService::OnConnectionClosed() noexcept {
  const std::uint64_t previous =
      active_clients_.fetch_sub(1, std::memory_order_acq_rel);
  assert(previous != 0);
  (void)previous;
}

void RedisService::Prepare(unsigned thread_count) {
  active_clients_.store(0, std::memory_order_relaxed);
  request_gate_.Prepare(thread_count);
  TcpService::Prepare(thread_count);
  PrepareMonitor(thread_count);
  PreparePubSub(thread_count);
  recovery_ready_barrier_ = std::make_unique<CoroutineBarrier>(thread_count);
  recovery_collect_barrier_ = std::make_unique<CoroutineBarrier>(thread_count);
  online_allocator_barrier_ = std::make_unique<CoroutineBarrier>(thread_count);
  rdb_import_barrier_ = std::make_unique<CoroutineBarrier>(thread_count);
}

void RedisService::StopAcceptingRequests() noexcept { request_gate_.Close(); }

void RedisService::WaitForRequestsDrained() const noexcept {
  request_gate_.WaitUntilEmpty();
}

absl::Status RedisService::WaitForReplicationQuiesced() {
  // This idempotent synchronous half is safe from the process main thread and
  // wakes any transport that appeared before the earlier request-drain fence.
  // The monitor performs coroutine-affine joins and storage-root retirement.
  replication_->RequestShutdown();
  replication_quiesce_requested_.store(true, std::memory_order_release);
  std::unique_lock lock(replication_quiesce_mutex_);
  if (!replication_quiesce_monitor_available_) {
    return absl::FailedPreconditionError(
        "replication shutdown monitor did not finish startup");
  }
  replication_quiesce_cv_.wait(
      lock, [this] { return replication_quiesce_complete_; });
  return replication_quiesce_status_;
}

bool RedisService::TryBeginRequest() noexcept {
  return request_gate_.TryEnter(ThisWorker().id_);
}

void RedisService::EndRequest() noexcept {
  request_gate_.Leave(ThisWorker().id_);
}

Task<absl::Status> RedisService::ImportRdb() {
  for (std::uint8_t db_id = 0; db_id < storage::kLogicalDatabaseCount;
       ++db_id) {
    for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
      const std::size_t keys = co_await SubmitTo(
          worker, [this, db_id] { return storage_->LocalSize(db_id); });
      if (keys != 0) {
        co_return absl::FailedPreconditionError(
            "load-rdb requires an empty Lavik dataset");
      }
    }
  }

  auto reader = rdb::FileReader::Open(load_rdb_file_);
  if (!reader.ok()) co_return reader.status();

  // Validate every object before mutating storage. The open file is then
  // rewound and decoded a second time through bounded reads during application.
  std::uint64_t entry_count = 0;
  std::uint64_t skipped_count = 0;
  std::vector<std::string> function_libraries;
  while (true) {
    auto entry = reader->NextStreaming();
    if (!entry.ok()) co_return entry.status();
    if (!entry->has_value()) break;
    auto drained = reader->DrainCollection();
    if (!drained.ok()) co_return drained;
    if ((**entry).kind_ == rdb::FileEntryKind::kValue) {
      ++entry_count;
    } else if ((**entry).kind_ == rdb::FileEntryKind::kFunctionLibrary) {
      function_libraries.push_back((**entry).function_code_);
    } else {
      ++skipped_count;
    }
  }
  absl::Status functions_validated =
      co_await ValidateLuaFunctionCatalog(function_libraries);
  if (!functions_validated.ok()) co_return functions_validated;
  reader->Rewind();

  std::uint64_t imported = 0;
  std::uint64_t expired = 0;
  while (true) {
    auto next = reader->NextStreaming();
    if (!next.ok()) co_return next.status();
    if (!next->has_value()) break;
    rdb::FileEntry entry = std::move(**next);
    if (entry.kind_ != rdb::FileEntryKind::kValue) {
      if (entry.kind_ == rdb::FileEntryKind::kFunctionLibrary) {
        continue;
      } else if (entry.kind_ == rdb::FileEntryKind::kSkippedModuleValue) {
        spdlog::warn(
            "skipping unsupported Redis Module value from RDB db={} "
            "key-bytes={}",
            entry.db_id_, entry.key_.size());
      } else if (entry.kind_ == rdb::FileEntryKind::kSkippedModuleAux) {
        spdlog::warn(
            "skipping unsupported Redis Module auxiliary data from "
            "RDB");
      }
      continue;
    }
    auto result = co_await rdb::RestoreFileEntry(storage_, &*reader, entry);
    if (!result.ok() || result->busy_) {
      absl::Status failure =
          result.ok() ? absl::AlreadyExistsError("duplicate key in RDB file")
                      : result.status();
      // Advancing all DB epochs makes any records written by this failed
      // attempt unreachable and keeps a retry from recovering half an import.
      absl::Status discarded = co_await storage_->FlushAllDetach();
      if (!discarded.ok()) {
        co_return absl::InternalError(absl::StrCat(
            "RDB import failed: ", failure.message(),
            "; failed to discard partial import: ", discarded.message()));
      }
      co_return failure;
    }
    if (result->changed_) {
      ++imported;
    } else {
      ++expired;
    }
  }

  absl::Status functions_installed =
      co_await ReplaceLuaFunctionCatalog(function_libraries);
  if (!functions_installed.ok()) {
    absl::Status discarded = co_await storage_->FlushAllDetach();
    if (!discarded.ok()) {
      co_return absl::InternalError(absl::StrCat(
          "RDB Function import failed: ", functions_installed.message(),
          "; failed to discard imported keys: ", discarded.message()));
    }
    co_return functions_installed;
  }

  spdlog::info(
      "loaded RDB file '{}' version={} entries={} imported={} expired={} "
      "unsupported-skipped={}",
      load_rdb_file_, reader->version(), entry_count, imported, expired,
      skipped_count);
  // Startup import establishes the persisted baseline; loading the snapshot
  // itself must not make INFO report unsaved changes.
  for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
    co_await SubmitTo(worker, [] {
      MarkLocalDatasetChangesSaved(LocalDatasetChangesTotal());
      return true;
    });
  }
  co_return absl::OkStatus();
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

  if (worker.id() == 0) {
    absl::Status imported = co_await GlobalFunctionCatalog().RecoverAtStartup();
    if (imported.ok())
      imported = co_await replication_->RecoverClusterPopulation();
    if (imported.ok() && !load_rdb_file_.empty()) {
      imported = co_await ImportRdb();
    }
    std::lock_guard lock(rdb_import_status_mutex_);
    rdb_import_status_ = std::move(imported);
  }
  status = co_await rdb_import_barrier_->Wait(worker);
  if (!status.ok()) [[unlikely]] {
    co_return status;
  }
  {
    std::lock_guard lock(rdb_import_status_mutex_);
    status = rdb_import_status_;
  }
  if (!status.ok()) [[unlikely]] {
    startup_failed_.store(true, std::memory_order_release);
    if (worker.id() == 0) {
      spdlog::error("startup catalog/RDB recovery failed: {}",
                    status.message());
    }
    worker.RequestStop();
    co_return status;
  }

  if (worker.id() == 0) {
    {
      std::lock_guard lock(replication_quiesce_mutex_);
      // No coroutine suspension occurs between publishing this bit and
      // spawning MonitorRuntimeHealth below. A main-thread waiter may arrive
      // in that interval; it can safely block until the worker starts the
      // requested barrier.
      replication_quiesce_monitor_available_ = true;
    }
    // Every worker has completed recovery and online allocator setup before
    // this boundary. Publish readiness only after an optional startup RDB
    // import has also completed successfully on every worker.
    ready_.store(true, std::memory_order_release);
    if (cluster::GetClusterRuntime() != nullptr) {
      // Meta topology may not have arrived yet. The installer remembers this
      // process-local readiness bit and folds it into the first complete FDS;
      // it is never persisted as authority.
      const absl::Status published =
          cluster::GetClusterRuntime()->node_control_installer_.SetStorageReady(
              true);
      if (!published.ok()) {
        spdlog::error("cluster storage-ready publication failed: {}",
                      published.message());
      }
    }
  }

  spdlog::info("worker[{}] direct-IO storage initialized", worker.id());
  if (worker.id() == 0) {
    // Keep this periodic maintenance tree in the same background task class as
    // the memory sampler it replaced. The synchronous storage-failure latch
    // already closes request and replication gates, so asynchronous cluster
    // cleanup need not compete with request processing for foreground budget.
    worker.SpawnBackground(MonitorRuntimeHealth(worker));
  }
  replication_->StorageReady(worker);
  co_return co_await TcpService::Run(worker, ctx);
}

class RequestInputBuffer {
 public:
  std::string_view View() const noexcept {
    if (begin_ == end_) return {};
    return std::string_view(
        reinterpret_cast<const char*>(storage_.get() + begin_), end_ - begin_);
  }

  std::span<std::byte> AppendBuffer() {
    constexpr std::size_t kReadBytes = 4096;
    if (capacity_ - end_ < kReadBytes && begin_ != 0) Compact();
    if (capacity_ - end_ < kReadBytes) {
      const std::size_t wanted = end_ + kReadBytes;
      std::size_t capacity = capacity_ == 0 ? kReadBytes : capacity_;
      while (capacity < wanted) capacity *= 2;
      auto storage = std::make_unique_for_overwrite<std::byte[]>(capacity);
      if (end_ != begin_) {
        std::memcpy(storage.get(), storage_.get() + begin_, end_ - begin_);
      }
      end_ -= begin_;
      begin_ = 0;
      storage_ = std::move(storage);
      capacity_ = capacity;
    }
    return std::span<std::byte>(storage_.get() + end_, capacity_ - end_);
  }

  void Commit(std::size_t bytes) noexcept {
    assert(bytes <= capacity_ - end_);
    end_ += bytes;
  }

  void Consume(std::size_t bytes) noexcept {
    assert(bytes <= end_ - begin_);
    begin_ += bytes;
    if (begin_ == end_) begin_ = end_ = 0;
  }

 private:
  void Compact() noexcept {
    if (begin_ == 0) return;
    if (begin_ != end_) {
      std::memmove(storage_.get(), storage_.get() + begin_, end_ - begin_);
    }
    end_ -= begin_;
    begin_ = 0;
  }

  std::unique_ptr<std::byte[]> storage_;
  std::size_t capacity_ = 0;
  std::size_t begin_ = 0;
  std::size_t end_ = 0;
};

class CommandBatch {
 public:
  static constexpr std::size_t kMaxCommands = 128;

  struct BufferedCommand {
    RespCommand command_;
    std::size_t input_bytes_ = 0;
  };

  bool empty() const noexcept { return next_ == commands_.size(); }
  std::size_t size() const noexcept { return commands_.size() - next_; }

  void Push(RespCommand command, std::size_t input_bytes) {
    assert(commands_.size() < kMaxCommands);
    commands_.push_back(BufferedCommand{std::move(command), input_bytes});
  }

  BufferedCommand PopFront() {
    assert(!empty());
    BufferedCommand command = std::move(commands_[next_++]);
    if (next_ == commands_.size()) {
      commands_.clear();
      next_ = 0;
    }
    return command;
  }

 private:
  // Most clients send one command at a time. Keep short pipelines allocation
  // free while retaining contiguous storage for larger batches.
  absl::InlinedVector<BufferedCommand, 4> commands_;
  std::size_t next_ = 0;
};

class CommandBufferGuard {
 public:
  CommandBufferGuard(ClientBufferReservation* reservation,
                     std::size_t bytes) noexcept
      : reservation_(reservation), bytes_(bytes) {}
  CommandBufferGuard(const CommandBufferGuard&) = delete;
  CommandBufferGuard& operator=(const CommandBufferGuard&) = delete;
  ~CommandBufferGuard() { Release(); }

  std::size_t Detach() noexcept { return std::exchange(bytes_, 0); }

  void Release() noexcept {
    if (bytes_ == 0) return;
    reservation_->Release(bytes_);
    bytes_ = 0;
  }

 private:
  ClientBufferReservation* reservation_;
  std::size_t bytes_;
};

Task<absl::Status> ReadCommandBatch(
    TcpStream& stream, RequestInputBuffer* input, RespCommandParser* parser,
    CommandBatch* ready, ClientBufferReservation* client_buffers,
    const std::atomic<std::size_t>* query_buffer_limit,
    std::size_t* unassigned_input_bytes,
    std::optional<absl::Status>* deferred_error) {
  while (ready->empty()) {
    // CONFIG SET may run on another worker while this coroutine is suspended
    // in ReadSome(). Refresh once per parsing round; token-level loads would
    // add needless atomic traffic to every request.
    parser->SetQueryBufferLimit(
        query_buffer_limit->load(std::memory_order_acquire));
    while (!input->View().empty() &&
           ready->size() < CommandBatch::kMaxCommands) {
      RespParseResult parsed = parser->Parse(input->View());
      input->Consume(parsed.consumed_);
      assert(parsed.consumed_ <=
             std::numeric_limits<std::size_t>::max() - *unassigned_input_bytes);
      *unassigned_input_bytes += parsed.consumed_;
      if (parsed.state_ == RespParseState::kError) {
        if (!ready->empty()) {
          deferred_error->emplace(std::move(parsed.status_));
          co_return absl::OkStatus();
        }
        co_return parsed.status_;
      }
      if (parsed.state_ == RespParseState::kOk) {
        ready->Push(std::move(parsed.command_), *unassigned_input_bytes);
        *unassigned_input_bytes = 0;
        continue;
      }
      if (parser->idle()) {
        // Blank lines and empty arrays retain no parser state. Retire them
        // immediately instead of letting meaningless traffic consume the
        // worker's client-buffer quota until disconnect.
        client_buffers->Release(*unassigned_input_bytes);
        *unassigned_input_bytes = 0;
      }
      // A trailing CR is deliberately left unread until its LF arrives.
      // Everything before it is now parser-owned and already consumed.
      break;
    }
    if (!ready->empty()) co_return absl::OkStatus();

    std::span<std::byte> buffer = input->AppendBuffer();
    auto read_result = co_await stream.ReadSome(buffer);
    if (!read_result.ok()) [[unlikely]] {
      co_return read_result.status();
    }
    if (*read_result == 0) [[unlikely]] {
      co_return absl::UnavailableError("peer closed connection");
    }
    // Read into the already allocated socket buffer first, then charge once
    // for the bytes that the parser may retain. This keeps admission out of
    // every string append while bounding stalled and pipelined clients.
    if (!client_buffers->TryAcquire(*read_result)) [[unlikely]] {
      RecordMemoryRejection();
      co_return absl::ResourceExhaustedError(
          "client request buffers exceed the memory limit");
    }
    input->Commit(*read_result);
  }
  co_return absl::OkStatus();
}

constexpr std::size_t kMaximumBatchedReplyBytes = 64 * 1024;

struct PendingReplyBatch {
  std::string bytes_;
  std::vector<ReadLatencyTrace> read_traces_;
  std::vector<SetLatencyTrace> set_traces_;

  bool empty() const noexcept { return bytes_.empty(); }

  void Append(std::string_view bytes, ReadLatencyTrace read_trace = {},
              SetLatencyTrace set_trace = {}) {
    bytes_.append(bytes);
    if (read_trace.request_start_ns_ != 0) {
      read_traces_.push_back(std::move(read_trace));
    }
    if (set_trace.request_start_ns_ != 0) {
      set_traces_.push_back(std::move(set_trace));
    }
  }
};

Task<absl::Status> FlushReplyBatch(TcpStream& stream,
                                   PendingReplyBatch* batch) {
  if (batch->empty()) co_return absl::OkStatus();

  for (ReadLatencyTrace& trace : batch->read_traces_) {
    trace.send_start_ns_ = ReadTraceNowNanos();
  }
  for (SetLatencyTrace& trace : batch->set_traces_) {
    trace.send_start_ns_ = SetTraceNowNanos();
  }
  absl::Status status = co_await stream.WriteAll(std::span<const std::byte>(
      reinterpret_cast<const std::byte*>(batch->bytes_.data()),
      batch->bytes_.size()));
  for (ReadLatencyTrace& trace : batch->read_traces_) {
    trace.send_complete_ns_ = ReadTraceNowNanos();
    RecordReadLatency(trace);
  }
  for (SetLatencyTrace& trace : batch->set_traces_) {
    trace.send_complete_ns_ = SetTraceNowNanos();
    RecordSetLatency(trace);
  }
  batch->bytes_.clear();
  batch->read_traces_.clear();
  batch->set_traces_.clear();
  co_return status;
}

Task<absl::Status> WriteOrBatchReply(TcpStream& stream,
                                     std::string_view encoded,
                                     bool more_commands,
                                     PendingReplyBatch* batch,
                                     ReadLatencyTrace read_trace = {},
                                     SetLatencyTrace set_trace = {}) {
  if (encoded.size() > kMaximumBatchedReplyBytes) {
    absl::Status flushed = co_await FlushReplyBatch(stream, batch);
    if (!flushed.ok()) co_return flushed;
  } else {
    if (!batch->empty() &&
        encoded.size() > kMaximumBatchedReplyBytes - batch->bytes_.size()) {
      absl::Status flushed = co_await FlushReplyBatch(stream, batch);
      if (!flushed.ok()) co_return flushed;
    }
    if (more_commands || !batch->empty()) {
      batch->Append(encoded, std::move(read_trace), std::move(set_trace));
      if (!more_commands || batch->bytes_.size() >= kMaximumBatchedReplyBytes) {
        co_return co_await FlushReplyBatch(stream, batch);
      }
      co_return absl::OkStatus();
    }
  }

  if (read_trace.request_start_ns_ != 0) {
    read_trace.send_start_ns_ = ReadTraceNowNanos();
  }
  if (set_trace.request_start_ns_ != 0) {
    set_trace.send_start_ns_ = SetTraceNowNanos();
  }
  absl::Status status = co_await stream.WriteAll(std::span<const std::byte>(
      reinterpret_cast<const std::byte*>(encoded.data()), encoded.size()));
  if (read_trace.request_start_ns_ != 0) {
    read_trace.send_complete_ns_ = ReadTraceNowNanos();
    RecordReadLatency(read_trace);
  }
  if (set_trace.request_start_ns_ != 0) {
    set_trace.send_complete_ns_ = SetTraceNowNanos();
    RecordSetLatency(set_trace);
  }
  co_return status;
}

bool ShutdownRequested() {
  return g_shutdown_requested.load(std::memory_order_acquire);
}

Task<absl::Status> RedisService::MonitorRuntimeHealth(Worker& worker) {
  bool storage_failure_handled = false;
  bool replication_quiesce_handled = false;
  while (!worker.stop_requested()) {
    if (!replication_quiesce_handled &&
        replication_quiesce_requested_.load(std::memory_order_acquire)) {
      absl::Status quiesced = co_await replication_->QuiesceForShutdown();
      {
        std::lock_guard lock(replication_quiesce_mutex_);
        replication_quiesce_status_ = std::move(quiesced);
        replication_quiesce_complete_ = true;
      }
      replication_quiesce_cv_.notify_all();
      replication_quiesce_handled = true;
    }
    RefreshMemoryStats();
    if (!storage_failure_handled && !ShutdownRequested() &&
        storage_->RuntimeFailureLatched()) {
      storage_failure_handled = true;
      ready_.store(false, std::memory_order_release);
      cluster::ClusterRuntime* runtime = cluster::GetClusterRuntime();
      if (runtime != nullptr) {
        const absl::Status fenced = co_await runtime->node_control_installer_
                                        .LoseStorageReadinessTransition();
        if (!fenced.ok()) {
          runtime_failure_cleanup_failed_.store(true,
                                                std::memory_order_release);
          spdlog::critical(
              "runtime storage failure cleanup is uncertain; stopping the "
              "process: {}",
              fenced.message());
          if (server_ != nullptr) {
            server_->RequestStop();
          } else {
            worker.RequestStop();
          }
          co_return fenced;
        }
        spdlog::error(
            "runtime storage failure fenced cluster authority and "
            "replication capabilities until restart");
      } else {
        // LatchRuntimeFailure already closed the process-wide request gate.
        // Standalone mode has no cluster capabilities left to join.
        spdlog::error(
            "runtime storage failure fenced request serving until restart");
      }
    }
    absl::Status slept =
        co_await bycorf::SleepFor(worker, std::chrono::milliseconds(100));
    if (!slept.ok()) {
      co_return absl::OkStatus();
    }
  }
  if (!replication_quiesce_handled &&
      replication_quiesce_requested_.load(std::memory_order_acquire)) {
    {
      std::lock_guard lock(replication_quiesce_mutex_);
      replication_quiesce_status_ = absl::CancelledError(
          "replication shutdown monitor stopped before target quiescence");
      replication_quiesce_complete_ = true;
    }
    replication_quiesce_cv_.notify_all();
  }
  co_return absl::OkStatus();
}

Task<absl::Status> RedisService::Serve(TcpStream stream) {
  static std::atomic<std::uint64_t> next_connection_id{1};
  ConnectionContext ctx;
  ctx.hello_authenticator_ = &authenticator_;
  ctx.hello_replication_ = replication_;
  ctx.hello_handler_ = &ExecuteHelloFromContext;
  ctx.authenticated_ = !authenticator_.required();
  ctx.authentication_required_ = authenticator_.required();
  ctx.conn_id_ = next_connection_id.fetch_add(1, std::memory_order_relaxed);
  ctx.socket_fd_ = stream.NativeFd();
  auto peer_address = stream.PeerAddress();
  const std::string address =
      peer_address.ok() ? std::move(*peer_address) : std::string("?:0");
  ctx.peer_address_ = address;
  const bool tls = stream.IsTls();
  RegisterClientConnection(ctx.conn_id_, stream.NativeFd(), address, tls);
  ConnectionOpened();
  absl::Status observed = stream.SetPeerDisconnectCallback(
      [](void* context) noexcept {
        const auto* connection = static_cast<const ConnectionContext*>(context);
        (void)CancelBlockedClientOnCurrentWorker(connection->conn_id_);
      },
      &ctx);
  if (!observed.ok()) {
    UnregisterClientConnection(ctx.conn_id_);
    ConnectionClosed();
    co_return observed;
  }
  const absl::Status status = co_await Serve(stream, ctx);
  stream.ClearPeerDisconnectCallback();
  // Single connection-scoped cleanup point: every disconnect path funnels
  // through this co_return.
  UnregisterMonitorSession(ctx.monitor_session_);
  UnregisterPubSubSession(ctx.pubsub_session_);
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
  // Both holders run on the connection's worker. The count protects lifetime
  // across the detached watchdog without paying std::shared_ptr's atomic RMWs.
  std::size_t references_ = 1;
  std::chrono::steady_clock::time_point last_progress_;
  bool done_ = false;  // same-worker access only
};

class StreamStallRef {
 public:
  StreamStallRef() = default;

  static StreamStallRef Make() { return StreamStallRef(new StreamStallState); }

  StreamStallRef(const StreamStallRef& other) noexcept : state_(other.state_) {
    Retain();
  }

  StreamStallRef& operator=(const StreamStallRef& other) noexcept {
    if (this != &other) {
      Release();
      state_ = other.state_;
      Retain();
    }
    return *this;
  }

  StreamStallRef(StreamStallRef&& other) noexcept
      : state_(std::exchange(other.state_, nullptr)) {}

  StreamStallRef& operator=(StreamStallRef&& other) noexcept {
    if (this != &other) {
      Release();
      state_ = std::exchange(other.state_, nullptr);
    }
    return *this;
  }

  ~StreamStallRef() { Release(); }

  explicit operator bool() const noexcept { return state_ != nullptr; }
  StreamStallState* operator->() const noexcept { return state_; }

 private:
  explicit StreamStallRef(StreamStallState* state) noexcept : state_(state) {}

  void Retain() noexcept {
    if (state_ != nullptr) ++state_->references_;
  }

  void Release() noexcept {
    StreamStallState* state = std::exchange(state_, nullptr);
    if (state != nullptr && --state->references_ == 0) delete state;
  }

  StreamStallState* state_ = nullptr;
};

// Watchdog for one streamed reply. shutdown() rather than close: it fails
// the parked write immediately without releasing the descriptor out from
// under the pending io_uring operation, and the serve loop's normal
// teardown then reopens the gate and closes the socket.
Task<absl::Status> BreakStalledStream(StreamStallRef state, int fd) {
  while (!state->done_) {
    const auto deadline = state->last_progress_ + kStreamStallLimit;
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) {
      ::shutdown(fd, SHUT_RDWR);
      co_return absl::OkStatus();
    }
    absl::Status slept = co_await bycorf::SleepFor(
        *ThisWorker().self_,
        std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now) +
            std::chrono::milliseconds(1));
    if (!slept.ok()) {
      co_return slept;  // worker shutting down
    }
  }
  co_return absl::OkStatus();
}

Task<absl::Status> RedisService::ReadSubscribedCommands(
    TcpStream& stream, ConnectionContext& ctx, RequestInputBuffer* input,
    RespCommandParser* parser, CommandBatch* ready,
    ClientBufferReservation* client_buffers,
    std::size_t* unassigned_input_bytes, std::size_t* multi_input_bytes,
    std::optional<absl::Status>* deferred_read_error,
    std::shared_ptr<PubSubSession> session) {
  struct ReaderDone {
    std::shared_ptr<PubSubSession> session_;
    ~ReaderDone() { MarkPubSubReaderDone(session_); }
  } reader_done{session};

  ReplyBuilder builder(ctx.resp_version());
  auto enqueue_command_reply = [&](CommandReply reply) -> Task<absl::Status> {
    if (reply.disk_value_.valid()) {
      const auto bytes = reply.disk_value_.network_bytes();
      EnqueuePubSubReply(
          session, std::string(reinterpret_cast<const char*>(bytes.data()),
                               bytes.size()));
      co_return absl::OkStatus();
    }
    if (!reply.encoded_.empty()) {
      EnqueuePubSubReply(session, std::string(reply.encoded_));
    }
    while (reply.chunks_) {
      auto chunk = co_await (*reply.chunks_)();
      if (!chunk.ok()) co_return chunk.status();
      if (chunk->empty()) break;
      EnqueuePubSubReply(session, std::move(*chunk));
    }
    co_return absl::OkStatus();
  };
  while (stream.IsOpen() && PubSubSubscriptionCount(session) != 0) {
    if (ShutdownRequested()) {
      ClosePubSubSession(session);
      co_return absl::OkStatus();
    }
    absl::Status read_status = absl::OkStatus();
    if (ready->empty()) {
      if (deferred_read_error->has_value()) {
        read_status = std::move(**deferred_read_error);
        deferred_read_error->reset();
      } else {
        read_status = co_await ReadCommandBatch(
            stream, input, parser, ready, client_buffers,
            &client_query_buffer_limit_bytes_, unassigned_input_bytes,
            deferred_read_error);
      }
    }
    if (!read_status.ok()) {
      if (read_status.code() != absl::StatusCode::kUnavailable) {
        builder.Reset();
        EnqueuePubSubReply(session,
                           std::string(builder.AppendError(
                               absl::StrCat("ERR ", read_status.message()))));
        ExitPubSubMode(session);
        co_return read_status;
      }
      ClosePubSubSession(session);
      co_return absl::OkStatus();
    }
    CommandBatch::BufferedCommand buffered = ready->PopFront();
    CommandBufferGuard command_memory(client_buffers, buffered.input_bytes_);
    RespCommand command = std::move(buffered.command_);

    if (!TryBeginRequest()) [[unlikely]] {
      builder.Reset();
      EnqueuePubSubReply(
          session,
          std::string(builder.AppendError("ERR server is shutting down")));
      ExitPubSubMode(session);
      co_return absl::OkStatus();
    }
    RequestGuard request_guard(this);
    CommandRequest request =
        BuildParsedCommandRequest(std::move(command), ctx.selected_db_);
    request.connection_tls_ = stream.IsTls();
    builder.SetVersion(ctx.resp_version());
    builder.Reset();
    const CommandKind kind = request.kind_;
    if (HasMonitorSessions()) [[unlikely]] {
      PublishMonitorMessage(PrepareMonitorMessage(
          request.db_id_, ctx.peer_address_, request.args_, &request));
    }
    if (kind == CommandKind::kQuit || kind == CommandKind::kReset) {
      const std::size_t queued_before = ctx.queued_.size();
      CommandReply reply = co_await DispatchCommand(ctx, request, builder);
      if (ctx.queued_.size() > queued_before) {
        *multi_input_bytes += command_memory.Detach();
      } else if (queued_before != 0 && ctx.queued_.empty()) {
        client_buffers->Release(*multi_input_bytes);
        *multi_input_bytes = 0;
      }
      const bool succeeded =
          reply.encoded_.empty() || reply.encoded_.front() != '-';
      (void)co_await enqueue_command_reply(std::move(reply));
      if (!succeeded) continue;
      ResetPubSubSubscriptions(session);
      SetClientPubSubCounts(ctx.conn_id_, 0, 0);
      ctx.close_after_pubsub_ = kind == CommandKind::kQuit;
      ExitPubSubMode(session);
      co_return absl::OkStatus();
    }
    if (kind == CommandKind::kPing && ctx.resp_version() == RespVersion::k2) {
      if (request.args_.size() > 2) {
        EnqueuePubSubReply(
            session, std::string(builder.AppendError(
                         "ERR wrong number of arguments for 'ping' command")));
      } else {
        builder.AppendPushHeader(2);
        builder.AppendBulkString("pong");
        builder.AppendBulkString(request.args_.size() == 2
                                     ? std::string_view(request.args_[1])
                                     : std::string_view{});
        EnqueuePubSubReply(session, std::string(builder.View()));
      }
      continue;
    }

    if (ctx.resp_version() == RespVersion::k2 &&
        kind != CommandKind::kSubscribe && kind != CommandKind::kUnsubscribe &&
        kind != CommandKind::kPSubscribe &&
        kind != CommandKind::kPUnsubscribe) {
      if (kind == CommandKind::kUnknown) {
        EnqueuePubSubReply(
            session,
            std::string(builder.AppendError("ERR unknown command '" +
                                            request.args_.front() + "'")));
      } else {
        std::string name(CommandCanonicalName(kind));
        EnqueuePubSubReply(
            session,
            std::string(builder.AppendError(
                "ERR Can't execute '" + name +
                "': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / "
                "RESET are allowed in this context")));
      }
      continue;
    }

    const std::size_t queued_before = ctx.queued_.size();
    CommandReply reply = co_await DispatchCommand(ctx, request, builder);
    if (ctx.queued_.size() > queued_before) {
      *multi_input_bytes += command_memory.Detach();
    } else if (queued_before != 0 && ctx.queued_.empty()) {
      client_buffers->Release(*multi_input_bytes);
      *multi_input_bytes = 0;
    }
    const bool succeeded =
        reply.encoded_.empty() || reply.encoded_.front() != '-';
    if (reply.selected_db_.has_value()) ctx.selected_db_ = *reply.selected_db_;
    absl::Status enqueued = co_await enqueue_command_reply(std::move(reply));
    if (!enqueued.ok()) {
      ClosePubSubSession(session);
      co_return enqueued;
    }
    if (succeeded) {
      SetPubSubRespVersion(session, ctx.resp_version());
      builder.SetVersion(ctx.resp_version());
    }
    if (PubSubSubscriptionCount(session) == 0) {
      ExitPubSubMode(session);
      co_return absl::OkStatus();
    }
  }
  ExitPubSubMode(session);
  co_return absl::OkStatus();
}

Task<absl::Status> RedisService::ServeSubscribed(
    TcpStream& stream, ConnectionContext& ctx, RequestInputBuffer* input,
    RespCommandParser* parser, CommandBatch* ready,
    ClientBufferReservation* client_buffers,
    std::size_t* unassigned_input_bytes, std::size_t* multi_input_bytes,
    std::optional<absl::Status>* deferred_read_error) {
  std::shared_ptr<PubSubSession> session = ctx.pubsub_session_;
  MarkPubSubReaderStarted(session);
  ThisWorker().self_->Spawn(ReadSubscribedCommands(
      stream, ctx, input, parser, ready, client_buffers, unassigned_input_bytes,
      multi_input_bytes, deferred_read_error, session));

  absl::Status streamed = co_await StreamPubSubMessages(stream, session);
  const bool normal_exit = PubSubSubscriptionCount(session) == 0;
  ClosePubSubSession(session);
  if (!streamed.ok() || !normal_exit) {
    (void)::shutdown(stream.NativeFd(), SHUT_RDWR);
  }
  absl::Status joined = co_await WaitPubSubReaderDone(session);
  UnregisterPubSubSession(session);
  ctx.pubsub_session_.reset();
  if (!streamed.ok()) co_return streamed;
  if (ctx.close_after_pubsub_) {
    stream.Close().IgnoreError();
  }
  co_return joined;
}

Task<absl::Status> RedisService::Serve(TcpStream& stream,
                                       ConnectionContext& ctx) {
  RequestInputBuffer input;
  RespCommandParser parser(client_query_buffer_limit());
  CommandBatch ready;
  ClientBufferReservation client_buffers;
  std::size_t unassigned_input_bytes = 0;
  std::size_t multi_input_bytes = 0;
  std::optional<absl::Status> deferred_read_error;
  PendingReplyBatch pending_replies;

  while (stream.IsOpen()) {
    ctx.reply_builder_.Reset();
    if (ShutdownRequested()) [[unlikely]] {
      co_return co_await FlushReplyBatch(stream, &pending_replies);
    }

    if (ready.empty()) {
      absl::Status read_status;
      if (deferred_read_error.has_value()) {
        read_status = std::move(*deferred_read_error);
        deferred_read_error.reset();
      } else {
        read_status = co_await ReadCommandBatch(
            stream, &input, &parser, &ready, &client_buffers,
            &client_query_buffer_limit_bytes_, &unassigned_input_bytes,
            &deferred_read_error);
      }
      if (!read_status.ok()) [[unlikely]] {
        if (read_status.code() == absl::StatusCode::kUnavailable) [[unlikely]] {
          co_return absl::OkStatus();
        }

        const std::string_view encoded = ctx.reply_builder_.AppendError(
            absl::StrCat("ERR ", read_status.message()));
        auto write_status = co_await stream.WriteAll(std::span<const std::byte>(
            reinterpret_cast<const std::byte*>(encoded.data()),
            encoded.size()));
        if (!write_status.ok()) [[unlikely]] {
          co_return write_status;
        }
        co_return read_status;
      }
    }
    CommandBatch::BufferedCommand buffered = ready.PopFront();
    CommandBufferGuard command_memory(&client_buffers, buffered.input_bytes_);
    RespCommand command = std::move(buffered.command_);

    const auto& args = command.args_;
    if (!args.empty() && absl::EqualsIgnoreCase(args.front(), "HELLO") &&
        !ctx.in_multi_) {
      const std::string_view encoded = ExecuteHello(
          authenticator_, replication_, ctx, args, ctx.reply_builder_);
      absl::Status written = co_await WriteOrBatchReply(
          stream, encoded, !ready.empty(), &pending_replies);
      if (!written.ok()) co_return written;
      continue;
    }
    if (!args.empty() && absl::EqualsIgnoreCase(args.front(), "AUTH")) {
      std::shared_ptr<const std::string> monitor_message;
      if (HasMonitorSessions()) [[unlikely]] {
        if (args.size() == 2 || args.size() == 3) {
          monitor_message =
              PrepareMonitorMessage(ctx.selected_db_, ctx.peer_address_, args);
        }
      }
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
      if (monitor_message != nullptr) [[unlikely]] {
        PublishMonitorMessage(std::move(monitor_message));
      }
      absl::Status written = co_await WriteOrBatchReply(
          stream, encoded, !ready.empty(), &pending_replies);
      if (!written.ok()) co_return written;
      continue;
    }

    if (!ctx.authenticated_) {
      const std::string_view encoded =
          ctx.reply_builder_.AppendError("NOAUTH Authentication required.");
      absl::Status written = co_await WriteOrBatchReply(
          stream, encoded, !ready.empty(), &pending_replies);
      if (!written.ok()) co_return written;
      continue;
    }

    if (!args.empty() && absl::EqualsIgnoreCase(args.front(), "REPLCONF")) {
      if (args.size() < 3 || (args.size() & 1U) == 0) {
        const std::string_view encoded = ctx.reply_builder_.AppendError(
            "ERR wrong number of arguments for 'replconf' command");
        absl::Status written = co_await WriteOrBatchReply(
            stream, encoded, !ready.empty(), &pending_replies);
        if (!written.ok()) co_return written;
        continue;
      }
      for (std::size_t index = 1; index + 1 < args.size(); index += 2) {
        if (absl::EqualsIgnoreCase(args[index], "capa") &&
            absl::EqualsIgnoreCase(args[index + 1], "eof")) {
          ctx.redis_replica_eof_ = true;
        }
      }
      const std::string_view encoded =
          ctx.reply_builder_.AppendSimpleString("OK");
      absl::Status written = co_await WriteOrBatchReply(
          stream, encoded, !ready.empty(), &pending_replies);
      if (!written.ok()) co_return written;
      continue;
    }

    if (!args.empty() && absl::EqualsIgnoreCase(args.front(), "PSYNC")) {
      if (!ready.empty() || !input.View().empty() || !parser.idle()) {
        co_return absl::InvalidArgumentError(
            "PSYNC handshake must be an isolated command");
      }
      absl::Status flushed = co_await FlushReplyBatch(stream, &pending_replies);
      if (!flushed.ok()) co_return flushed;
      ConnectionClosed();
      ctx.counted_as_client_ = false;
      auto peer_address = stream.PeerAddress();
      const std::string address =
          peer_address.ok() ? std::move(*peer_address) : std::string("?:0");
      const bool tls = stream.IsTls();
      UnregisterClientConnection(ctx.conn_id_);
      command_memory.Release();
      co_return co_await replication_->ServeRedisExportConnection(
          stream, std::move(command.args_), ctx.conn_id_, address, tls,
          ctx.redis_replica_eof_);
    }

    if (ReplicationManager::IsNativeHandshake(command.args_)) {
      if (!ready.empty() || !input.View().empty() || !parser.idle()) {
        co_return absl::InvalidArgumentError(
            "replication handshake must be the first isolated command");
      }
      absl::Status flushed = co_await FlushReplyBatch(stream, &pending_replies);
      if (!flushed.ok()) co_return flushed;
      ConnectionClosed();
      ctx.counted_as_client_ = false;
      auto peer_address = stream.PeerAddress();
      const std::string address =
          peer_address.ok() ? std::move(*peer_address) : std::string("?:0");
      const bool tls = stream.IsTls();
      UnregisterClientConnection(ctx.conn_id_);
      command_memory.Release();
      co_return co_await replication_->ServeNativeConnection(
          stream, std::move(command.args_), ctx.conn_id_, address, tls);
    }

    if (!TryBeginRequest()) [[unlikely]] {
      const std::string_view encoded =
          ctx.reply_builder_.AppendError("ERR server is shutting down");
      absl::Status write_status =
          co_await FlushReplyBatch(stream, &pending_replies);
      if (write_status.ok()) {
        write_status = co_await WriteOrBatchReply(stream, encoded, false,
                                                  &pending_replies);
      }
      stream.Close().IgnoreError();
      co_return write_status;
    }
    RequestGuard request_guard(this);

    CommandRequest request =
        BuildParsedCommandRequest(std::move(command), ctx.selected_db_);
    // Cluster MOVED/discovery replies select the TLS port for connections
    // that arrived over TLS (Redis getNodeClientPort semantics).
    request.connection_tls_ = stream.IsTls();
    const std::size_t queued_before = ctx.queued_.size();
    CommandReply reply;
    std::shared_ptr<const std::string> monitor_message;
    bool publish_monitor_after_dispatch = false;
    {
      // A reply batch must never cross an unbounded command boundary. A
      // pipelined blocking command can park this connection before the loop
      // reaches its ordinary write path, otherwise replies for commands that
      // already completed remain invisible to the client indefinitely.
      // Blocking commands queued by MULTI execute non-blocking at EXEC time,
      // so preserve batching while they are only being queued.
      const bool may_block = !ctx.in_multi_ && request.spec_ != nullptr &&
                             (request.spec_->flags_ & kCmdMayBlock) != 0;
      if (may_block && !pending_replies.empty()) {
        absl::Status flushed =
            co_await FlushReplyBatch(stream, &pending_replies);
        if (!flushed.ok()) co_return flushed;
      }

      // Valkey emits queued MULTI children only when EXEC reaches them. The
      // transaction implementation publishes those children after WATCH and
      // other pre-execution checks pass.
      if (HasMonitorSessions()) [[unlikely]] {
        const CommandKind kind = request.kind_;
        publish_monitor_after_dispatch = kind == CommandKind::kExec;
        const bool defer_to_exec =
            ctx.in_multi_ && kind != CommandKind::kExec &&
            kind != CommandKind::kDiscard && kind != CommandKind::kMulti &&
            kind != CommandKind::kWatch;
        if (!defer_to_exec) {
          monitor_message = PrepareMonitorMessage(
              request.db_id_, ctx.peer_address_, request.args_, &request);
        }
      }
      if (monitor_message != nullptr && !publish_monitor_after_dispatch)
          [[unlikely]] {
        PublishMonitorMessage(std::move(monitor_message));
      }
      reply = co_await DispatchCommand(ctx, request, ctx.reply_builder_);
    }
    if (ctx.queued_.size() > queued_before) {
      multi_input_bytes += command_memory.Detach();
    } else if (queued_before != 0 && ctx.queued_.empty()) {
      client_buffers.Release(multi_input_bytes);
      multi_input_bytes = 0;
    }
    if (monitor_message != nullptr) [[unlikely]] {
      PublishMonitorMessage(std::move(monitor_message));
    }
    if (reply.start_monitoring_) [[unlikely]] {
      ctx.monitor_session_ = RegisterMonitorSession(stream.NativeFd());
    }
    if (reply.selected_db_.has_value()) {
      ctx.selected_db_ = *reply.selected_db_;
    }

    // Streamed replies hold the database gate at the peer's pace; arm the
    // stall watchdog for the whole stream, header included. The scope guard
    // retires it on every exit path, including error co_returns.
    StreamStallRef stall;
    struct RetireStall {
      StreamStallRef state_;
      ~RetireStall() {
        if (state_) {
          state_->done_ = true;
        }
      }
    } retire_stall;
    if (reply.chunks_) {
      stall = StreamStallRef::Make();
      stall->last_progress_ = std::chrono::steady_clock::now();
      retire_stall.state_ = stall;
      ThisWorker().self_->Spawn(BreakStalledStream(stall, stream.NativeFd()));
    }

    absl::Status write_status;
    // An empty batch needs no ordering flush. Check before creating the
    // coroutine so direct/streamed replies avoid a frame and symmetric transfer
    // when no encoded replies precede them; nonempty batches still flush first.
    if (reply.disk_value_.valid()) {
      if (!pending_replies.empty()) {
        write_status = co_await FlushReplyBatch(stream, &pending_replies);
      }
      if (write_status.ok()) {
        if (reply.read_trace_.request_start_ns_ != 0) {
          reply.read_trace_.send_start_ns_ = ReadTraceNowNanos();
        }
        if (reply.set_trace_.request_start_ns_ != 0) {
          reply.set_trace_.send_start_ns_ = SetTraceNowNanos();
        }
        write_status =
            co_await stream.WriteAll(reply.disk_value_.network_bytes());
      }
    } else if (reply.chunks_) {
      if (!pending_replies.empty()) {
        write_status = co_await FlushReplyBatch(stream, &pending_replies);
      }
      if (write_status.ok()) {
        if (reply.read_trace_.request_start_ns_ != 0) {
          reply.read_trace_.send_start_ns_ = ReadTraceNowNanos();
        }
        if (reply.set_trace_.request_start_ns_ != 0) {
          reply.set_trace_.send_start_ns_ = SetTraceNowNanos();
        }
        write_status = co_await stream.WriteAll(std::span<const std::byte>(
            reinterpret_cast<const std::byte*>(reply.encoded_.data()),
            reply.encoded_.size()));
      }
    } else {
      write_status = co_await WriteOrBatchReply(
          stream, reply.encoded_, !ready.empty(), &pending_replies,
          std::exchange(reply.read_trace_, {}),
          std::exchange(reply.set_trace_, {}));
    }
    // Streamed continuation (KEYS): drain bounded chunks onto the socket.
    // The reply header already committed the element count, so a chunk
    // failure can only end the connection.
    while (write_status.ok() && reply.chunks_) {
      if (stall) {
        stall->last_progress_ = std::chrono::steady_clock::now();
      }
      auto chunk = co_await (*reply.chunks_)();
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
        if (stall) {
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
    if (reply.start_monitoring_) [[unlikely]] {
      // MONITOR is no longer an in-flight request while its connection waits
      // indefinitely for asynchronously published messages.
      absl::Status flushed = co_await FlushReplyBatch(stream, &pending_replies);
      if (!flushed.ok()) co_return flushed;
      request_guard.Release();
      command_memory.Release();
      co_return co_await StreamMonitorMessages(stream, ctx.monitor_session_);
    }
    if (PubSubSubscriptionCount(ctx.pubsub_session_) != 0) [[unlikely]] {
      // The first SUBSCRIBE/EXEC response is already on the wire. Messages
      // published during that write have only been queued, so the dedicated
      // single writer preserves confirmation-before-message ordering.
      absl::Status flushed = co_await FlushReplyBatch(stream, &pending_replies);
      if (!flushed.ok()) co_return flushed;
      request_guard.Release();
      command_memory.Release();
      absl::Status subscribed = co_await ServeSubscribed(
          stream, ctx, &input, &parser, &ready, &client_buffers,
          &unassigned_input_bytes, &multi_input_bytes, &deferred_read_error);
      if (!subscribed.ok()) co_return subscribed;
      continue;
    }
    if (reply.close_connection_ || ShutdownRequested()) [[unlikely]] {
      absl::Status flushed = co_await FlushReplyBatch(stream, &pending_replies);
      if (!flushed.ok()) co_return flushed;
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
  const auto backends = bycorf::ConfigureIoBackends(
      {.dpdk_network = options.network_backend_ == "dpdk",
       .spdk_storage = options.storage_backend_ == "spdk"});
  if (!backends.ok()) {
    spdlog::error("I/O backend configuration failed: {}", backends.message());
    return 1;
  }
  // Freeze before metadata probes or DMA buffers, which can precede workers.
  bycorf::FreezeIoBackends();
  spdlog::info("I/O backends: network={} storage={} rings=one-per-worker",
               options.network_backend_, options.storage_backend_);
  // Cluster mode delegates population lifecycle to Meta/NodeControl and
  // disables standalone replication control and export.
  options.replication_options_.cluster_enabled_ = options.cluster_enabled_;
  auto allowed_max_clients = MaxClientsAllowedByFileLimit(options.max_clients_);
  if (!allowed_max_clients.ok()) {
    spdlog::error("maxclients file-descriptor setup failed: {}",
                  allowed_max_clients.status().message());
    return 1;
  }
  if (*allowed_max_clients == 0) {
    spdlog::error(
        "RLIMIT_NOFILE cannot preserve the {} file descriptors reserved "
        "outside maxclients",
        kMaxClientsFileDescriptorReserve);
    return 1;
  }
  if (*allowed_max_clients < options.max_clients_) {
    spdlog::warn(
        "reducing maxclients from {} to {} to preserve {} file descriptors "
        "under RLIMIT_NOFILE",
        options.max_clients_, *allowed_max_clients,
        kMaxClientsFileDescriptorReserve);
    options.max_clients_ = *allowed_max_clients;
  }
  if (options.load_rdb_replace_) {
    // Never erase storage for a missing, corrupt, or unsupported source.
    // ImportRdb validates again immediately before application so a source
    // changed during startup cannot silently produce a partial dataset.
    auto reader = rdb::FileReader::Open(options.load_rdb_file_);
    if (!reader.ok()) {
      spdlog::error("RDB replacement preflight failed: {}",
                    reader.status().message());
      return 1;
    }
    std::uint64_t entries = 0;
    std::uint64_t skipped = 0;
    while (true) {
      auto entry = reader->NextStreaming();
      if (!entry.ok()) {
        spdlog::error("RDB replacement preflight failed: {}",
                      entry.status().message());
        return 1;
      }
      if (!entry->has_value()) break;
      auto drained = reader->DrainCollection();
      if (!drained.ok()) {
        spdlog::error("RDB replacement preflight failed: {}",
                      drained.message());
        return 1;
      }
      if ((**entry).kind_ == rdb::FileEntryKind::kValue) {
        ++entries;
      } else {
        ++skipped;
      }
    }
    struct stat rdb_info{};
    if (::stat(options.load_rdb_file_.c_str(), &rdb_info) != 0) {
      spdlog::error("cannot identify replacement RDB '{}': {}",
                    options.load_rdb_file_, std::strerror(errno));
      return 1;
    }
    std::vector<struct stat> storage_identities;
    storage_identities.reserve(options.data_files_.size());
    for (std::size_t i = 0; i < options.data_files_.size(); ++i) {
      const std::string& path = options.data_files_[i];
      if (bycorf::IsSpdkStoragePath(path)) {
        if (std::find(options.data_files_.begin(),
                      options.data_files_.begin() + i,
                      path) != options.data_files_.begin() + i) {
          spdlog::error(
              "duplicate storage path configured for replacement: '{}'", path);
          return 1;
        }
        continue;
      }
      struct stat info{};
      if (::stat(path.c_str(), &info) != 0) {
        spdlog::error("cannot identify replacement storage path '{}': {}", path,
                      std::strerror(errno));
        return 1;
      }
      if (info.st_dev == rdb_info.st_dev && info.st_ino == rdb_info.st_ino) {
        spdlog::error(
            "replacement RDB and data-file resolve to the same file: '{}'",
            path);
        return 1;
      }
      const auto duplicate = std::find_if(
          storage_identities.begin(), storage_identities.end(),
          [&info](const struct stat& prior) {
            if (S_ISBLK(info.st_mode) && S_ISBLK(prior.st_mode)) {
              return info.st_rdev == prior.st_rdev;
            }
            return info.st_dev == prior.st_dev && info.st_ino == prior.st_ino;
          });
      if (duplicate != storage_identities.end()) {
        spdlog::error(
            "duplicate storage target configured for replacement: '{}'", path);
        return 1;
      }
      storage_identities.push_back(info);
    }
    spdlog::info(
        "validated replacement RDB '{}' version={} entries={} "
        "unsupported-skipped={}",
        options.load_rdb_file_, reader->version(), entries, skipped);
  }
  const std::string bind_display = absl::StrJoin(options.bind_addresses_, ",");
  std::string advertised_bind = options.bind_addresses_.front();
  if (advertised_bind == "*") advertised_bind = "0.0.0.0";
  const std::uint16_t advertised_port =
      options.port_ != 0 ? options.port_ : options.tls_port_;

  std::shared_ptr<bycorf::TlsContext> tls_server_context;
  if (options.tls_port_ != 0) {
    bycorf::TlsClientAuth client_auth = bycorf::TlsClientAuth::kNo;
    if (options.tls_auth_clients_ == "optional") {
      client_auth = bycorf::TlsClientAuth::kOptional;
    } else if (options.tls_auth_clients_ == "yes") {
      client_auth = bycorf::TlsClientAuth::kRequired;
    }
    auto created = bycorf::TlsContext::CreateServer(bycorf::TlsServerOptions{
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

  std::shared_ptr<bycorf::TlsContext> tls_client_context;
  if (options.tls_replication_) {
    auto created = bycorf::TlsContext::CreateClient(bycorf::TlsClientOptions{
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
      "lavik version={} listening on {}:{} tls_port={} metrics_port={} "
      "threads={} "
      "maxclients={} maxclients_fd_reserve={} "
      "pin_workers={} "
      "idle_timeout_ms={} "
      "busy_poll_us={} foreground_budget_us={} background_budget_us={} "
      "background_warrant_percent={} "
      "spdk_max_completions_per_poll={} spdk_foreground_pre_poll_us={} "
      "registered_buffer_bytes={} per worker "
      "storage_write_buffers={} "
      "storage_read_buffer_bytes={} "
      "replication_publish_queue_bytes={} per worker max_memory={} "
      "maxmemory_clients={} "
      "client_query_buffer_limit={} "
      "flush_max_ms={} "
      "flush_size_bytes={} "
      "inline_key_max_bytes={} "
      "defrag_max_active_per_device={} defrag_sleep_ms={} "
      "defrag_record_sleep_us={} defrag_paused={} shutdown_checkpoint={} "
      "replication_backlog_backpressure={}",
      kVersion, bind_display, options.port_, options.tls_port_,
      options.metrics_port_, options.thread_count_, options.max_clients_,
      kMaxClientsFileDescriptorReserve, options.pin_workers_,
      options.idle_timeout_ms_, options.busy_poll_us_,
      options.foreground_budget_us_, options.background_budget_us_,
      options.background_warrant_percent_,
      options.spdk_max_completions_per_poll_,
      options.spdk_foreground_pre_poll_us_, options.registered_buffer_bytes_,
      options.storage_write_buffer_count_, options.storage_read_buffer_bytes_,
      options.replication_publish_queue_bytes_, options.max_memory_bytes_,
      FormatClientBufferLimit(options.maxmemory_clients_),
      options.client_query_buffer_limit_bytes_, options.flush_max_ms_,
      options.flush_size_bytes_, options.inline_key_max_bytes_,
      options.defrag_max_active_per_device_, options.defrag_sleep_ms_,
      options.defrag_record_sleep_us_, options.defrag_paused_,
      options.shutdown_checkpoint_,
      options.replication_options_.backlog_backpressure_);

  const absl::Status memory_status =
      InitMemoryLimit(options.max_memory_bytes_, options.thread_count_,
                      options.maxmemory_clients_);
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
  storage_options.database_count_ =
      options.cluster_enabled_ ? 1 : storage::kLogicalDatabaseCount;
  storage_options.data_files_ = std::move(options.data_files_);
  storage_options.reset_data_files_ = options.load_rdb_replace_;
  storage_options.shutdown_checkpoint_ = options.shutdown_checkpoint_;
  storage_options.flush_max_ms_ = options.flush_max_ms_;
  storage_options.flush_size_bytes_ = options.flush_size_bytes_;
  storage_options.replication_publish_queue_bytes_ =
      options.replication_publish_queue_bytes_;
  storage_options.replication_backlog_backpressure_ =
      options.replication_options_.backlog_backpressure_;
  storage_options.inline_key_max_bytes_ = options.inline_key_max_bytes_;
  // A node configured with an upstream must not create local
  // expiration mutation sequences. It still hides expired values by their
  // absolute deadline and applies the primary's replicated tombstone.
  storage_options.expiration_authority_ = !options.cluster_enabled_ &&
                                          !options.replicaof_.has_value() &&
                                          !options.redis_replicaof_.has_value();
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
  // Meta control reuses this exact client identity when mTLS is enabled, so
  // retain a shared owner while also passing it to replication.
  options.replication_options_.tls_context_ = tls_client_context;
  options.replication_options_.masteruser_ = options.masteruser_;
  options.replication_options_.masterauth_ = options.masterauth_;
  options.replication_options_.publish_queue_bytes_per_worker_ =
      options.replication_publish_queue_bytes_;
  options.replication_options_.redis_psync_ =
      options.redis_replicaof_.has_value();
  if (options.cluster_enabled_) {
    // The Meta session and native replication protocol must name the same
    // stable data node; boot and history incarnations remain manager-owned.
    options.replication_options_.node_id_override_ = options.cluster_node_id_;
  }
  std::optional<ReplicaOfConfig> replication_upstream =
      options.redis_replicaof_.has_value() ? std::move(options.redis_replicaof_)
                                           : std::move(options.replicaof_);
  ReplicationManager replication(&storage,
                                 std::move(options.replication_options_),
                                 std::move(replication_upstream));
  InitStorage(&storage, &replication);
  InitRdbBackup(
      &storage,
      absl::StrCat(options.rdb_dir_, options.rdb_dir_.ends_with('/') ? "" : "/",
                   options.dbfilename_));
  InitWorkerMetrics(options.thread_count_);
  InitSlowLog(options.thread_count_, options.slowlog_log_slower_than_us_,
              options.slowlog_max_len_);
  SetLuaScriptBusyThresholdMs(options.lua_time_limit_ms_);
  SetServerInfo(std::move(advertised_bind), advertised_port,
                options.thread_count_, options.config_file_);
  tx::TxRuntime::Create(options.thread_count_);

  // Redis Cluster data plane: install the process-wide runtime before any
  // listener accepts a client. It starts without serving topology and stays
  // fail-closed until Meta supplies an authenticated complete state. Storage
  // also starts unready, so non-whitelisted commands answer LOADING until
  // recovery completes.
  std::unique_ptr<cluster::MetaControlClientService> meta_control_client;
  if (options.cluster_enabled_) {
    std::unique_ptr<cluster::NodeControlActions> control_actions =
        cluster::CreateReplicationNodeControlActions(replication,
                                                     options.tls_replication_);
    auto runtime =
        std::make_unique<cluster::ClusterRuntime>(std::move(control_actions));
    // Announce-address defaults: an explicit announce ip wins; otherwise the
    // first non-wildcard bind address; a wildcard bind stays empty so
    // discovery self entries keep the "use the startup node" convention.
    runtime->announce_ip_ = options.cluster_announce_ip_;
    if (runtime->announce_ip_.empty()) {
      const std::string& first_bind = options.bind_addresses_.front();
      const bool wildcard =
          first_bind == "*" || first_bind == "0.0.0.0" || first_bind == "::";
      if (!wildcard) runtime->announce_ip_ = first_bind;
    }
    runtime->announce_port_ = options.cluster_announce_port_ != 0
                                  ? options.cluster_announce_port_
                                  : options.port_;
    runtime->announce_tls_port_ = options.cluster_announce_tls_port_ != 0
                                      ? options.cluster_announce_tls_port_
                                      : options.tls_port_;
    cluster::InstallClusterRuntime(std::move(runtime));
    auto created = cluster::MetaControlClientService::Create(
        cluster::MetaControlClientOptions{
            .seeds_ = options.cluster_meta_seeds_,
            .node_id_ = options.cluster_node_id_,
            .request_worker_count_ = options.thread_count_,
            .tls_context_ =
                options.tls_replication_ ? tls_client_context : nullptr,
        },
        cluster::GetClusterRuntime()->node_control_installer_,
        cluster::GetClusterRuntime()->topology_cache_, replication);
    if (!created.ok()) {
      spdlog::error("Meta control client setup failed: {}",
                    created.status().message());
      cluster::InstallClusterRuntime(nullptr);
      CleanupShutdownSignalHandler();
      return 1;
    }
    meta_control_client = std::move(*created);
  }

  bycorf::ServerOptions runtime_options;
  runtime_options.bind_addresses_ = options.bind_addresses_;
  runtime_options.thread_count_ = options.thread_count_;
  runtime_options.pin_workers_ = options.pin_workers_;
  runtime_options.idle_timeout_ms_ = options.idle_timeout_ms_;
  runtime_options.recv_buffer_count_ = options.recv_buffer_count_;
  runtime_options.busy_poll_us_ = options.busy_poll_us_;
  runtime_options.foreground_budget_us_ = options.foreground_budget_us_;
  runtime_options.background_budget_us_ = options.background_budget_us_;
  runtime_options.background_warrant_percent_ =
      options.background_warrant_percent_;
  runtime_options.spdk_max_completions_per_poll_ =
      options.spdk_max_completions_per_poll_;
  runtime_options.spdk_foreground_pre_poll_us_ =
      options.spdk_foreground_pre_poll_us_;

  RedisService redis(options.port_, &storage, &replication,
                     options.mimalloc_purge_delay_ms_, options.requirepass_,
                     std::move(options.load_rdb_file_), options.max_clients_,
                     options.client_query_buffer_limit_bytes_);
  InitClientLimit(&redis);
  if (tls_server_context != nullptr) {
    redis.AddTlsEndpoint(options.tls_port_, std::move(tls_server_context));
  }
  std::unique_ptr<Service> metrics;
  Server server;
  redis.BindServer(&server);
  server.AddService(&redis);
  if (meta_control_client != nullptr) {
    server.AddService(meta_control_client.get());
  }
  if (options.metrics_port_ != 0) {
    metrics = CreateMetricsService(options.metrics_port_, &storage,
                                   [&redis] { return redis.ready(); });
    server.AddService(metrics.get());
  }
  auto start_status = server.Start(runtime_options);
  if (!start_status.ok()) [[unlikely]] {
    spdlog::error("server start failed: {}", start_status.message());
    cluster::InstallClusterRuntime(nullptr);
    CleanupShutdownSignalHandler();
    return 1;
  }

  const WaitResult wait_result = WaitForSignalOrServerStop(server);
  int shutdown_exit_code = 0;
  bool fast_process_exit = false;
  if (wait_result == WaitResult::kSignal) {
    const int signal = static_cast<int>(g_last_shutdown_signal);
    spdlog::info("shutdown requested by signal {}",
                 (signal == 0 ? "unknown" : std::to_string(signal)));

    redis.StopAcceptingRequests();
    server.StopAccepting();
    // A native or Redis downstream can pin a full backlog and suspend an
    // already-admitted publisher. Close replication transports before either
    // the Meta-control join or request drain; worker-zero cleanup and history
    // retirement remain deferred until accepted control/client work is done.
    replication.RequestShutdown();
    redis.WaitForRequestsDrained();
    absl::Status control_quiesce = absl::OkStatus();
    if (meta_control_client != nullptr) {
      // The shutdown checkpoint must describe a state after all accepted Meta
      // directives and the final fail-closed transition. Stop() only starts
      // that worker-affine drain; join it while the Runtime can still service
      // socket cancellation and replication/storage completions.
      control_quiesce = meta_control_client->WaitUntilQuiesced();
      if (!control_quiesce.ok()) {
        spdlog::error(
            "Meta control shutdown cleanup failed; normal storage checkpoint "
            "is unsafe: {}",
            control_quiesce.message());
        shutdown_exit_code = 1;
      } else {
        spdlog::info("Meta control client quiesced before storage flush");
      }
    }
    absl::Status replication_quiesce = redis.WaitForReplicationQuiesced();
    if (!replication_quiesce.ok()) {
      spdlog::error(
          "replication target shutdown cleanup failed; normal storage "
          "checkpoint is unsafe: {}",
          replication_quiesce.message());
      shutdown_exit_code = 1;
    } else {
      spdlog::info("replication targets quiesced before storage flush");
    }
    WaitForRdbBackupDrained();
    if (!control_quiesce.ok() || !replication_quiesce.ok()) {
      // Replication cleanup may still own a candidate root or native flow.
      // Do not bless that uncertain state with a clean-shutdown checkpoint.
      spdlog::error(
          "skipping normal storage flush after uncertain control or "
          "replication cleanup");
    } else {
      spdlog::info("all active requests drained; flushing storage buffers");
      absl::Status flush_status = storage.FlushForShutdown();
      if (!flush_status.ok()) {
        spdlog::error("shutdown storage flush failed: {}",
                      flush_status.message());
        shutdown_exit_code = 1;
      } else {
        spdlog::info("all storage buffers durably flushed");
        fast_process_exit = storage.AbandonWorkerStateForProcessExit();
        if (fast_process_exit) {
          spdlog::info(
              "durable shutdown checkpoint permits OS-reclaimed worker "
              "state");
        }
      }
    }
    server.RequestStop();
  }
  server.WaitUntilStopped();
  InitClientLimit(nullptr);
  const int exit_code = redis.startup_failed() ||
                                redis.runtime_failure_cleanup_failed() ||
                                shutdown_exit_code != 0
                            ? 1
                            : server.exit_code();
  cluster::InstallClusterRuntime(nullptr);
  CleanupShutdownSignalHandler();
  if (fast_process_exit) {
    // All worker IO backends and coroutine frames are already quiescent. A
    // normal return would only run process-lifetime destructors after the
    // deliberately abandoned stores, so flush the synchronous logger and make
    // the process-only contract explicit. ASan builds never arm this branch.
    if (auto logger = spdlog::default_logger(); logger != nullptr) {
      logger->flush();
    }
    std::_Exit(exit_code);
  }
  return exit_code;
}

}  // namespace lavik
