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

#pragma once

#include "lavik/read_trace.h"
#include "lavik/set_trace.h"

// Keep diagnostic helpers and their thread-local state out of disabled builds,
// independently of dead-code elimination. Prometheus metrics are separate.
#if LAVIK_ENABLE_TRACE
#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <string_view>

#include "bycorf/runtime/worker.h"
#include "spdlog/spdlog.h"

namespace lavik::trace {

// Inline definitions share function-local thread_local statistics across
// translation units: each worker still has exactly one read and one write set.
#if BYCORF_ENABLE_CROSS_CORE_LATENCY_TRACE
inline constexpr std::uint64_t kReadLatencyReportIntervalNs = 45'000'000'000ULL;
#else
inline constexpr std::uint64_t kReadLatencyReportIntervalNs = 10'000'000'000ULL;
#endif
inline constexpr std::array<std::uint64_t, 28> kLatencyBucketUpperUs{
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

inline std::uint64_t Elapsed(std::uint64_t end, std::uint64_t start) noexcept {
  return end >= start && start != 0 ? end - start : 0;
}

// Records a completed read on its originating worker and periodically logs it.
inline void RecordReadLatency(const ReadLatencyTrace& trace) {
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
    stats.next_report_ns_ = now + kReadLatencyReportIntervalNs +
                            100'000'000ULL * bycorf::ThisWorker().id_;
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
  const auto wake_stats = bycorf::ThisWorker().self_->TakeWakeStats();
  const auto scheduler_stats = bycorf::ThisWorker().self_->TakeSchedulerStats();
  spdlog::info(
      "read-latency worker={} n={} remote={:.1f}% hit={:.1f}% disk={:.1f}% "
      "heap-buffer={:.1f}% avg-us total={:.1f} route-out={:.1f} lookup={:.1f} "
      "buffer={:.1f} io={:.1f} decode={:.1f} route-back={:.1f} send={:.1f} "
      "p99.9-us total<={} route-out<={} lookup<={} buffer<={} io<={} "
      "decode<={} route-back<={} send<={} wake-sent={}/{}",
      bycorf::ThisWorker().id_, stats.count_,
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
      bycorf::ThisWorker().id_, scheduler_stats.rounds_,
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
      bycorf::ThisWorker().id_, stats.count_, p9999(stats.non_network_),
      p9999(stats.total_), p9999(stats.io_), p9999(stats.route_out_),
      p9999(stats.lookup_), p9999(stats.buffer_), p9999(stats.decode_),
      p9999(stats.route_back_), p9999(stats.send_));
#if BYCORF_ENABLE_CROSS_CORE_LATENCY_TRACE
  const auto cross_core_stats =
      bycorf::ThisWorker().self_->TakeCrossCoreLatencyStats();
  const auto log_cross_core =
      [&](std::string_view name,
          const bycorf::Worker::LatencySampleStats& value) {
        spdlog::info(
            "cross-core-latency worker={} kind={} n={} avg-us={:.2f} "
            "p99.9-us<={} p99.99-us<={} max-us={:.2f}",
            bycorf::ThisWorker().id_, name, value.count_, value.AverageUs(),
            value.PercentileUpperUs(0.999), value.PercentileUpperUs(0.9999),
            static_cast<double>(value.max_ns_) / 1000.0);
      };
  log_cross_core("wake-batch", cross_core_stats.wake_batch_wait_);
  log_cross_core("parked-wake-batch", cross_core_stats.parked_wake_batch_wait_);
  log_cross_core("request-queue", cross_core_stats.request_queue_);
  log_cross_core("reply-queue", cross_core_stats.reply_queue_);
#endif
  stats = ReadLatencyStats{};
  stats.next_report_ns_ = now + kReadLatencyReportIntervalNs +
                          100'000'000ULL * bycorf::ThisWorker().id_;
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

// Records a completed write on its originating worker and periodically logs it.
inline void RecordSetLatency(const SetLatencyTrace& trace) {
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
        now + 10'000'000'000ULL + 100'000'000ULL * bycorf::ThisWorker().id_;
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
      bycorf::ThisWorker().id_, stats.count_,
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
        label, bycorf::ThisWorker().id_, stats.count_,
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
      now + 10'000'000'000ULL + 100'000'000ULL * bycorf::ThisWorker().id_;
}

}  // namespace lavik::trace
#endif  // LAVIK_ENABLE_TRACE
