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

#include "lavik/metrics.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "absl/strings/str_cat.h"
#include "bycorf/net/connection_stats.h"
#include "bycorf/net/http_service.h"
#include "bycorf/runtime/cross_core.h"
#include "bycorf/runtime/cycle_clock.h"
#include "bycorf/runtime/worker.h"
#include "lavik/command_table.h"
#include "lavik/memory.h"
#include "lavik/storage/engine.h"

namespace lavik {
namespace {

constexpr std::size_t ToIndex(CommandKind kind) noexcept {
  return static_cast<std::size_t>(kind);
}

// Transport copies use ordinary alignment: this data can live in a SubmitTo
// result or a coroutine frame, whose allocator guarantees only max_align_t.
// Cache-line isolation belongs to the live shard wrapper, not its snapshots.
struct WorkerMetricsData {
  std::array<CommandMetricTotals, kCommandKindCount> commands_{};
  // Command latencies are strongly clustered under steady load. This is a
  // routing hint only; every miss still performs the exact lower_bound, so
  // Prometheus histogram semantics and bucket boundaries remain unchanged.
  std::array<std::uint8_t, kCommandKindCount> last_latency_buckets_{};
  std::uint64_t connected_clients_ = 0;
  std::uint64_t blocked_clients_ = 0;
  std::uint64_t replication_control_connections_ = 0;
  std::uint64_t replication_flow_connections_ = 0;
  std::uint64_t defrag_successes_ = 0;
  std::uint64_t defrag_resource_exhausted_ = 0;
  std::uint64_t defrag_failures_ = 0;
  std::uint64_t active_defrags_ = 0;
  std::uint64_t pending_defrags_ = 0;
  std::uint64_t dataset_changes_total_ = 0;
  std::uint64_t dataset_changes_saved_ = 0;
};

// Written only by the owning worker. Keep neighboring live shards on separate
// cache lines without imposing that alignment on cross-worker return values.
struct alignas(64) WorkerMetricsShard : WorkerMetricsData {};

static_assert(alignof(WorkerMetricsShard) == 64);
static_assert(sizeof(WorkerMetricsShard) % 64 == 0);
static_assert(alignof(WorkerMetricsData) <= alignof(std::max_align_t));
static_assert(alignof(WorkerMetricsSnapshot) <= alignof(std::max_align_t));
static_assert(alignof(bycorf::Worker::StorageIoStats) <=
              alignof(std::max_align_t));

std::unique_ptr<WorkerMetricsShard[]> g_worker_metrics;
unsigned g_worker_metrics_count = 0;
std::array<std::uint64_t, kCommandLatencyBucketUpperUs.size()>
    g_latency_bucket_upper_ticks{};
double g_counter_frequency = 1.0;

struct ClusterControlMetrics {
  struct Wait {
    std::array<std::atomic<std::uint64_t>, kControlWaitBoundsUs.size() + 1>
        buckets{};
    std::atomic<std::uint64_t> sum_us{0};
  };
  std::array<Wait, 2> waits;
  std::atomic<std::uint64_t> connected_{0};
  std::atomic<std::uint64_t> reconnects_{0};
  std::atomic<std::uint64_t> protocol_errors_{0};
  std::atomic<std::uint64_t> full_states_applied_{0};
  std::atomic<std::uint64_t> lease_grants_{0};
  std::atomic<std::uint64_t> lease_denials_{0};
  std::atomic<std::uint64_t> lease_expirations_{0};
  std::atomic<std::uint64_t> directive_successes_{0};
  std::atomic<std::uint64_t> directive_failures_{0};
};

ClusterControlMetrics g_cluster_control_metrics;

}  // namespace

void SetClusterControlConnected(bool connected) noexcept {
  g_cluster_control_metrics.connected_.store(connected ? 1 : 0,
                                             std::memory_order_relaxed);
}

void RecordClusterControlReconnect() noexcept {
  g_cluster_control_metrics.reconnects_.fetch_add(1, std::memory_order_relaxed);
}

void RecordClusterControlProtocolError() noexcept {
  g_cluster_control_metrics.protocol_errors_.fetch_add(
      1, std::memory_order_relaxed);
}

void RecordClusterControlFullStateApplied() noexcept {
  g_cluster_control_metrics.full_states_applied_.fetch_add(
      1, std::memory_order_relaxed);
}

void RecordClusterControlLeaseGrant() noexcept {
  g_cluster_control_metrics.lease_grants_.fetch_add(1,
                                                    std::memory_order_relaxed);
}

void RecordClusterControlLeaseDenial() noexcept {
  g_cluster_control_metrics.lease_denials_.fetch_add(1,
                                                     std::memory_order_relaxed);
}

void RecordClusterControlLeaseExpiration() noexcept {
  g_cluster_control_metrics.lease_expirations_.fetch_add(
      1, std::memory_order_relaxed);
}

void RecordClusterControlDirectiveResult(bool succeeded) noexcept {
  std::atomic<std::uint64_t>& counter =
      succeeded ? g_cluster_control_metrics.directive_successes_
                : g_cluster_control_metrics.directive_failures_;
  counter.fetch_add(1, std::memory_order_relaxed);
}

void RecordClusterControlWait(ClusterControlWait stage,
                              std::uint64_t us) noexcept {
  auto& wait = g_cluster_control_metrics.waits[static_cast<unsigned>(stage)];
  const auto bucket = std::lower_bound(kControlWaitBoundsUs.begin(),
                                       kControlWaitBoundsUs.end(), us) -
                      kControlWaitBoundsUs.begin();
  wait.sum_us.fetch_add(us, std::memory_order_relaxed);
  wait.buckets[bucket].fetch_add(1, std::memory_order_relaxed);
}

ClusterControlMetricsSnapshot GetClusterControlMetrics() noexcept {
  ClusterControlMetricsSnapshot snapshot{
      .connected_ =
          g_cluster_control_metrics.connected_.load(std::memory_order_relaxed),
      .reconnects_ =
          g_cluster_control_metrics.reconnects_.load(std::memory_order_relaxed),
      .protocol_errors_ = g_cluster_control_metrics.protocol_errors_.load(
          std::memory_order_relaxed),
      .full_states_applied_ =
          g_cluster_control_metrics.full_states_applied_.load(
              std::memory_order_relaxed),
      .lease_grants_ = g_cluster_control_metrics.lease_grants_.load(
          std::memory_order_relaxed),
      .lease_denials_ = g_cluster_control_metrics.lease_denials_.load(
          std::memory_order_relaxed),
      .lease_expirations_ = g_cluster_control_metrics.lease_expirations_.load(
          std::memory_order_relaxed),
      .directive_successes_ =
          g_cluster_control_metrics.directive_successes_.load(
              std::memory_order_relaxed),
      .directive_failures_ = g_cluster_control_metrics.directive_failures_.load(
          std::memory_order_relaxed),
  };
  for (unsigned stage = 0; stage < snapshot.waits_.size(); ++stage) {
    const auto& source = g_cluster_control_metrics.waits[stage];
    auto& target = snapshot.waits_[stage];
    for (unsigned bucket = 0; bucket < target.buckets_.size(); ++bucket) {
      target.buckets_[bucket] =
          source.buckets[bucket].load(std::memory_order_relaxed);
    }
    target.sum_us_ = source.sum_us.load(std::memory_order_relaxed);
  }
  return snapshot;
}

std::uint64_t WorkerMetricsSnapshot::TotalCalls() const noexcept {
  std::uint64_t total = 0;
  for (const CommandMetricTotals& command : commands_) {
    total += command.calls_;
  }
  return total;
}

void InitWorkerMetrics(unsigned worker_count) {
  g_worker_metrics = std::make_unique<WorkerMetricsShard[]>(worker_count);
  g_worker_metrics_count = worker_count;
  g_counter_frequency = std::max(1.0, bycorf::CycleCounterFrequency());
  for (std::size_t i = 0; i < kCommandLatencyBucketUpperUs.size(); ++i) {
    const long double ticks =
        static_cast<long double>(g_counter_frequency) *
        static_cast<long double>(kCommandLatencyBucketUpperUs[i]) /
        1'000'000.0L;
    g_latency_bucket_upper_ticks[i] =
        std::max<std::uint64_t>(1, static_cast<std::uint64_t>(ticks + 0.999L));
  }
}

void RecordConnectionOpened() noexcept {
  ++g_worker_metrics[bycorf::ThisWorker().id_].connected_clients_;
}

void RecordConnectionClosed() noexcept {
  --g_worker_metrics[bycorf::ThisWorker().id_].connected_clients_;
}

void RecordClientBlocked() noexcept {
  ++g_worker_metrics[bycorf::ThisWorker().id_].blocked_clients_;
}

void RecordClientUnblocked() noexcept {
  WorkerMetricsShard& shard = g_worker_metrics[bycorf::ThisWorker().id_];
  assert(shard.blocked_clients_ != 0);
  --shard.blocked_clients_;
}

void RecordDatasetChanges(std::uint64_t count) noexcept {
  g_worker_metrics[bycorf::ThisWorker().id_].dataset_changes_total_ += count;
}

std::uint64_t LocalDatasetChangesTotal() noexcept {
  return g_worker_metrics[bycorf::ThisWorker().id_].dataset_changes_total_;
}

void MarkLocalDatasetChangesSaved(std::uint64_t total) noexcept {
  WorkerMetricsShard& shard = g_worker_metrics[bycorf::ThisWorker().id_];
  // Only one RDB job is active today, but max keeps this correct if completed
  // jobs are ever allowed to retire out of order.
  shard.dataset_changes_saved_ =
      std::max(shard.dataset_changes_saved_,
               std::min(total, shard.dataset_changes_total_));
}

bycorf::Task<std::uint64_t> CollectDatasetChangesSinceLastSave() {
  std::uint64_t changes = 0;
  for (unsigned worker = 0; worker < g_worker_metrics_count; ++worker) {
    changes += co_await bycorf::SubmitTo(worker, [worker] {
      const WorkerMetricsShard& shard = g_worker_metrics[worker];
      return shard.dataset_changes_total_ - shard.dataset_changes_saved_;
    });
  }
  co_return changes;
}

void RecordReplicationConnectionOpened(
    ReplicationConnectionKind kind) noexcept {
  WorkerMetricsShard& shard = g_worker_metrics[bycorf::ThisWorker().id_];
  if (kind == ReplicationConnectionKind::kControl) {
    ++shard.replication_control_connections_;
  } else {
    ++shard.replication_flow_connections_;
  }
}

void RecordReplicationConnectionClosed(
    ReplicationConnectionKind kind) noexcept {
  WorkerMetricsShard& shard = g_worker_metrics[bycorf::ThisWorker().id_];
  if (kind == ReplicationConnectionKind::kControl) {
    --shard.replication_control_connections_;
  } else {
    --shard.replication_flow_connections_;
  }
}

void RecordDefragMetric(DefragMetricResult result) noexcept {
  WorkerMetricsShard& shard = g_worker_metrics[bycorf::ThisWorker().id_];
  switch (result) {
    case DefragMetricResult::kSuccess:
      ++shard.defrag_successes_;
      break;
    case DefragMetricResult::kResourceExhausted:
      ++shard.defrag_resource_exhausted_;
      break;
    case DefragMetricResult::kFailure:
      ++shard.defrag_failures_;
      break;
  }
}

void SetDefragActive(bool active) noexcept {
  g_worker_metrics[bycorf::ThisWorker().id_].active_defrags_ = active;
}

void SetDefragPending(bool pending) noexcept {
  g_worker_metrics[bycorf::ThisWorker().id_].pending_defrags_ = pending;
}

void RecordCommandMetric(CommandKind kind,
                         std::uint64_t elapsed_ticks) noexcept {
  const unsigned worker = bycorf::ThisWorker().id_;
  std::size_t command_index = ToIndex(kind);
  if (worker >= g_worker_metrics_count) [[unlikely]] {
    return;
  }
  if (command_index >= kCommandKindCount) [[unlikely]] {
    command_index = ToIndex(CommandKind::kUnknown);
  }
  CommandMetricTotals& metric =
      g_worker_metrics[worker].commands_[command_index];
  ++metric.calls_;
  metric.latency_ticks_ += elapsed_ticks;
  WorkerMetricsShard& shard = g_worker_metrics[worker];
  std::size_t bucket = shard.last_latency_buckets_[command_index];
  const bool above_lower =
      bucket == 0 || elapsed_ticks > g_latency_bucket_upper_ticks[bucket - 1];
  const bool below_upper =
      bucket == g_latency_bucket_upper_ticks.size() ||
      elapsed_ticks <= g_latency_bucket_upper_ticks[bucket];
  if (!above_lower || !below_upper) {
    bucket = static_cast<std::size_t>(
        std::lower_bound(g_latency_bucket_upper_ticks.begin(),
                         g_latency_bucket_upper_ticks.end(), elapsed_ticks) -
        g_latency_bucket_upper_ticks.begin());
    shard.last_latency_buckets_[command_index] =
        static_cast<std::uint8_t>(bucket);
  }
  ++metric.latency_bins_[bucket];
}

bycorf::Task<WorkerMetricsSnapshot> CollectWorkerMetrics() {
  WorkerMetricsSnapshot result;
  result.counter_frequency_ = g_counter_frequency;
  for (unsigned worker = 0; worker < g_worker_metrics_count; ++worker) {
    // Copy on the owner rather than reading its live cache lines remotely.
    // Slice off the live shard's alignment so it does not over-align the
    // coroutine frame holding the transfer value.
    const auto shard =
        co_await bycorf::SubmitTo(worker, [worker]() -> WorkerMetricsData {
          return static_cast<const WorkerMetricsData&>(
              g_worker_metrics[worker]);
        });
    const bycorf::Worker::StorageIoStats storage_io = co_await bycorf::SubmitTo(
        worker, [] { return bycorf::ThisWorker().self_->storage_io_stats(); });
    result.connected_clients_ += shard.connected_clients_;
    result.blocked_clients_ += shard.blocked_clients_;
    result.replication_control_connections_ +=
        shard.replication_control_connections_;
    result.replication_flow_connections_ += shard.replication_flow_connections_;
    result.defrag_successes_ += shard.defrag_successes_;
    result.defrag_resource_exhausted_ += shard.defrag_resource_exhausted_;
    result.defrag_failures_ += shard.defrag_failures_;
    result.active_defrags_ += shard.active_defrags_;
    result.pending_defrags_ += shard.pending_defrags_;
    result.rdb_changes_since_last_save_ +=
        shard.dataset_changes_total_ - shard.dataset_changes_saved_;
    result.storage_reads_.operations_ += storage_io.read_operations_;
    result.storage_reads_.bytes_ += storage_io.read_bytes_;
    result.storage_writes_.operations_ += storage_io.write_operations_;
    result.storage_writes_.bytes_ += storage_io.write_bytes_;
    result.storage_fdatasyncs_.operations_ += storage_io.fdatasync_operations_;
    result.storage_fdatasyncs_.bytes_ += storage_io.fdatasync_bytes_;
    for (std::size_t command = 0; command < kCommandKindCount; ++command) {
      CommandMetricTotals& destination = result.commands_[command];
      const CommandMetricTotals& source = shard.commands_[command];
      destination.calls_ += source.calls_;
      destination.latency_ticks_ += source.latency_ticks_;
      for (std::size_t bucket = 0; bucket < source.latency_bins_.size();
           ++bucket) {
        destination.latency_bins_[bucket] += source.latency_bins_[bucket];
      }
    }
  }
  // Includes outgoing Meta streams without scheduling a metrics task on the
  // control worker. This counter is updated only at stream open/close.
  result.connections_ = bycorf::ProcessActiveConnectionCount();
  co_return result;
}

bycorf::Task<absl::Status> ResetCommandMetrics() {
  for (unsigned worker = 0; worker < g_worker_metrics_count; ++worker) {
    co_await bycorf::SubmitTo(worker, [worker] {
      g_worker_metrics[worker].commands_ = {};
      return true;
    });
  }
  co_return absl::OkStatus();
}

std::string_view CommandMetricName(CommandKind kind) noexcept {
  return CommandCanonicalName(kind);
}

namespace {

void AppendEscapedLabel(std::string_view value, std::string* output) {
  for (const char c : value) {
    switch (c) {
      case '\\':
        output->append("\\\\");
        break;
      case '"':
        output->append("\\\"");
        break;
      case '\n':
        output->append("\\n");
        break;
      default:
        output->push_back(c);
        break;
    }
  }
}

std::string SecondsFromMicroseconds(std::uint64_t microseconds) {
  const std::uint64_t seconds = microseconds / 1'000'000;
  std::uint64_t remainder = microseconds % 1'000'000;
  if (remainder == 0) {
    return std::to_string(seconds);
  }
  std::string fraction = std::to_string(remainder + 1'000'000).substr(1);
  while (fraction.back() == '0') {
    fraction.pop_back();
  }
  return absl::StrCat(seconds, ".", fraction);
}

}  // namespace

bycorf::Task<absl::Status> RenderPrometheusMetrics(
    const storage::StorageEngine& storage, bool server_ready,
    std::string* output_ptr) {
  std::string& output = *output_ptr;
  output.clear();
  output.reserve(server_ready ? 32 * 1024 : 256);
  absl::StrAppend(
      &output,
      "# HELP lavik_server_ready Whether storage recovery is complete and "
      "Redis requests are being accepted.\n"
      "# TYPE lavik_server_ready gauge\n"
      "lavik_server_ready ",
      server_ready ? 1 : 0, "\n");
  if (!server_ready) {
    co_return absl::OkStatus();
  }

  RefreshMemoryDiagnostics();
  const WorkerMetricsSnapshot worker_metrics = co_await CollectWorkerMetrics();
  const ClusterControlMetricsSnapshot control_metrics =
      GetClusterControlMetrics();
  const storage::StorageMetricsSnapshot storage_metrics =
      co_await storage.CollectMetrics();
  const storage::DefragTotals defrag = storage.DefragStats();
  const MemoryStats memory_metrics = GetMemoryStats();
  const std::uint64_t current_memory = memory_metrics.used_bytes_;
  absl::StrAppend(&output,
                  "# HELP lavik_commands_total Completed Redis commands.\n"
                  "# TYPE lavik_commands_total counter\n"
                  "lavik_commands_total ",
                  worker_metrics.TotalCalls(),
                  "\n"
                  "# HELP lavik_command_calls_total Completed Redis commands "
                  "by command.\n"
                  "# TYPE lavik_command_calls_total counter\n");
  for (std::size_t index = 0; index < worker_metrics.commands_.size();
       ++index) {
    const CommandMetricTotals& command = worker_metrics.commands_[index];
    if (command.calls_ == 0) {
      continue;
    }
    const std::string_view name =
        CommandMetricName(static_cast<CommandKind>(index));
    absl::StrAppend(&output, "lavik_command_calls_total{command=\"", name,
                    "\"} ", command.calls_, "\n");
  }

  output.append(
      "# HELP lavik_command_duration_seconds Redis command execution "
      "latency, excluding socket response writes.\n"
      "# TYPE lavik_command_duration_seconds histogram\n");
  for (std::size_t index = 0; index < worker_metrics.commands_.size();
       ++index) {
    const CommandMetricTotals& command = worker_metrics.commands_[index];
    if (command.calls_ == 0) {
      continue;
    }
    const std::string_view name =
        CommandMetricName(static_cast<CommandKind>(index));
    std::uint64_t cumulative = 0;
    for (std::size_t bucket = 0; bucket < kCommandLatencyBucketUpperUs.size();
         ++bucket) {
      cumulative += command.latency_bins_[bucket];
      absl::StrAppend(
          &output, "lavik_command_duration_seconds_bucket{command=\"", name,
          "\",le=\"",
          SecondsFromMicroseconds(kCommandLatencyBucketUpperUs[bucket]), "\"} ",
          cumulative, "\n");
    }
    cumulative += command.latency_bins_.back();
    const double latency_seconds = static_cast<double>(command.latency_ticks_) /
                                   worker_metrics.counter_frequency_;
    absl::StrAppend(&output, "lavik_command_duration_seconds_bucket{command=\"",
                    name, "\",le=\"+Inf\"} ", cumulative, "\n",
                    "lavik_command_duration_seconds_sum{command=\"", name,
                    "\"} ", latency_seconds, "\n",
                    "lavik_command_duration_seconds_count{command=\"", name,
                    "\"} ", command.calls_, "\n");
  }

  absl::StrAppend(
      &output,
      "# HELP lavik_storage_io_operations_total Completed storage I/O "
      "operations.\n"
      "# TYPE lavik_storage_io_operations_total counter\n"
      "lavik_storage_io_operations_total{operation=\"read\"} ",
      worker_metrics.storage_reads_.operations_, "\n",
      "lavik_storage_io_operations_total{operation=\"write\"} ",
      worker_metrics.storage_writes_.operations_, "\n",
      "lavik_storage_io_operations_total{operation=\"fdatasync\"} ",
      worker_metrics.storage_fdatasyncs_.operations_, "\n",
      "# HELP lavik_storage_io_bytes_total Bytes completed by storage "
      "reads and writes, or covered by successful fdatasync barriers.\n"
      "# TYPE lavik_storage_io_bytes_total counter\n"
      "lavik_storage_io_bytes_total{operation=\"read\"} ",
      worker_metrics.storage_reads_.bytes_, "\n",
      "lavik_storage_io_bytes_total{operation=\"write\"} ",
      worker_metrics.storage_writes_.bytes_, "\n",
      "lavik_storage_io_bytes_total{operation=\"fdatasync\"} ",
      worker_metrics.storage_fdatasyncs_.bytes_, "\n",
      "# HELP lavik_connections Current TCP connections, including Redis "
      "clients, metrics scrapes, replication, and Meta control.\n"
      "# TYPE lavik_connections gauge\n"
      "lavik_connections ",
      worker_metrics.connections_, "\n",
      "# HELP lavik_connected_clients Current Redis client connections.\n"
      "# TYPE lavik_connected_clients gauge\n"
      "lavik_connected_clients ",
      worker_metrics.connected_clients_, "\n",
      "# HELP lavik_blocked_clients Redis clients waiting in blocking "
      "commands.\n"
      "# TYPE lavik_blocked_clients gauge\n"
      "lavik_blocked_clients ",
      worker_metrics.blocked_clients_, "\n",
      "# HELP lavik_replication_control_connections Current native "
      "replication control connections.\n"
      "# TYPE lavik_replication_control_connections gauge\n"
      "lavik_replication_control_connections ",
      worker_metrics.replication_control_connections_, "\n",
      "# HELP lavik_replication_flow_connections Current native replication "
      "data-flow connections.\n"
      "# TYPE lavik_replication_flow_connections gauge\n"
      "lavik_replication_flow_connections ",
      worker_metrics.replication_flow_connections_, "\n",
      "# HELP lavik_cluster_control_connected Whether the control worker "
      "currently "
      "holds an accepted Meta control session.\n"
      "# TYPE lavik_cluster_control_connected gauge\n"
      "lavik_cluster_control_connected ",
      control_metrics.connected_, "\n",
      "# HELP lavik_cluster_control_reconnects_total Meta control reconnect "
      "rounds after the initial attempt.\n"
      "# TYPE lavik_cluster_control_reconnects_total counter\n"
      "lavik_cluster_control_reconnects_total ",
      control_metrics.reconnects_, "\n",
      "# HELP lavik_cluster_control_protocol_errors_total Control sessions "
      "closed for invalid framing or protocol state.\n"
      "# TYPE lavik_cluster_control_protocol_errors_total counter\n"
      "lavik_cluster_control_protocol_errors_total ",
      control_metrics.protocol_errors_, "\n",
      "# HELP lavik_cluster_control_full_states_applied_total Complete Meta "
      "projections installed by the data node.\n"
      "# TYPE lavik_cluster_control_full_states_applied_total counter\n"
      "lavik_cluster_control_full_states_applied_total ",
      control_metrics.full_states_applied_, "\n",
      "# HELP lavik_cluster_control_lease_decisions_total Lease decisions "
      "received from Meta.\n"
      "# TYPE lavik_cluster_control_lease_decisions_total counter\n"
      "lavik_cluster_control_lease_decisions_total{decision=\"granted\"} ",
      control_metrics.lease_grants_, "\n",
      "lavik_cluster_control_lease_decisions_total{decision=\"denied\"} ",
      control_metrics.lease_denials_, "\n",
      "# HELP lavik_cluster_control_lease_expirations_total Locally detected "
      "control lease expirations.\n"
      "# TYPE lavik_cluster_control_lease_expirations_total counter\n"
      "lavik_cluster_control_lease_expirations_total ",
      control_metrics.lease_expirations_, "\n",
      "# HELP lavik_cluster_control_directive_results_total Data-side "
      "directive execution results.\n"
      "# TYPE lavik_cluster_control_directive_results_total counter\n"
      "lavik_cluster_control_directive_results_total{result=\"success\"} ",
      control_metrics.directive_successes_, "\n",
      "lavik_cluster_control_directive_results_total{result=\"failure\"} ",
      control_metrics.directive_failures_, "\n",
      "# HELP lavik_storage_defrag_runs_total Completed defrag attempts.\n"
      "# TYPE lavik_storage_defrag_runs_total counter\n"
      "lavik_storage_defrag_runs_total{result=\"success\"} ",
      worker_metrics.defrag_successes_, "\n",
      "lavik_storage_defrag_runs_total{result=\"resource_exhausted\"} ",
      worker_metrics.defrag_resource_exhausted_, "\n",
      "lavik_storage_defrag_runs_total{result=\"error\"} ",
      worker_metrics.defrag_failures_, "\n",
      "# HELP lavik_storage_defrag_active Currently running defrag jobs.\n"
      "# TYPE lavik_storage_defrag_active gauge\n"
      "lavik_storage_defrag_active ",
      worker_metrics.active_defrags_, "\n",
      "# HELP lavik_storage_defrag_pending Queued defrag jobs.\n"
      "# TYPE lavik_storage_defrag_pending gauge\n"
      "lavik_storage_defrag_pending ",
      worker_metrics.pending_defrags_, "\n",
      "# HELP lavik_storage_defrag_paused Whether relocation jobs are "
      "paused while candidates remain queued.\n"
      "# TYPE lavik_storage_defrag_paused gauge\n"
      "lavik_storage_defrag_paused ",
      defrag.paused_ ? 1 : 0, "\n",
      "# HELP lavik_storage_defrag_max_active_per_device Runtime maximum "
      "concurrent relocations per device.\n"
      "# TYPE lavik_storage_defrag_max_active_per_device gauge\n"
      "lavik_storage_defrag_max_active_per_device ",
      defrag.max_active_per_device_, "\n",
      "# HELP lavik_storage_defrag_block_sleep_seconds Runtime cooldown "
      "after each relocated block.\n"
      "# TYPE lavik_storage_defrag_block_sleep_seconds gauge\n"
      "lavik_storage_defrag_block_sleep_seconds ",
      static_cast<double>(defrag.block_sleep_ms_) / 1000.0, "\n",
      "# HELP lavik_storage_defrag_record_sleep_seconds Runtime pause "
      "after each record examined by defrag.\n"
      "# TYPE lavik_storage_defrag_record_sleep_seconds gauge\n"
      "lavik_storage_defrag_record_sleep_seconds ",
      static_cast<double>(defrag.record_sleep_us_) / 1'000'000.0, "\n",
      "# HELP lavik_memory_current_bytes Current process memory used for "
      "limit enforcement.\n"
      "# TYPE lavik_memory_current_bytes gauge\n"
      "lavik_memory_current_bytes ",
      current_memory, "\n",
      "# HELP lavik_memory_used_bytes Allocator usable bytes used for "
      "limit enforcement.\n"
      "# TYPE lavik_memory_used_bytes gauge\n"
      "lavik_memory_used_bytes ",
      memory_metrics.used_bytes_, "\n",
      "# HELP lavik_memory_rss_bytes Resident process memory.\n"
      "# TYPE lavik_memory_rss_bytes gauge\n"
      "lavik_memory_rss_bytes ",
      memory_metrics.rss_bytes_, "\n",
      "# HELP lavik_memory_committed_bytes Memory committed by the "
      "configured allocator.\n"
      "# TYPE lavik_memory_committed_bytes gauge\n"
      "lavik_memory_committed_bytes ",
      memory_metrics.committed_bytes_, "\n",
      "# HELP lavik_memory_reserved_bytes Address space reserved by the "
      "configured allocator.\n"
      "# TYPE lavik_memory_reserved_bytes gauge\n"
      "lavik_memory_reserved_bytes ",
      memory_metrics.reserved_bytes_, "\n",
      "# HELP lavik_memory_max_bytes Configured process memory limit.\n"
      "# TYPE lavik_memory_max_bytes gauge\n"
      "lavik_memory_max_bytes ",
      memory_metrics.max_bytes_, "\n",
      "# HELP lavik_client_request_buffer_limit_bytes Effective ordinary "
      "client request-buffer limit.\n"
      "# TYPE lavik_client_request_buffer_limit_bytes gauge\n"
      "lavik_client_request_buffer_limit_bytes ",
      memory_metrics.client_buffer_limit_bytes_, "\n",
      "# HELP lavik_client_buffered_request_bytes Ordinary client request "
      "bytes read but not yet retired.\n"
      "# TYPE lavik_client_buffered_request_bytes gauge\n"
      "lavik_client_buffered_request_bytes ",
      memory_metrics.client_buffered_bytes_, "\n",
      "# HELP lavik_fullsync_reserved_memory_bytes Memory headroom "
      "reserved for active full-sync coverage maps.\n"
      "# TYPE lavik_fullsync_reserved_memory_bytes gauge\n"
      "lavik_fullsync_reserved_memory_bytes ",
      memory_metrics.fullsync_reserved_bytes_, "\n",
      "# HELP lavik_memory_admission_pending_bytes Worker-local headroom "
      "held while slow-path allocations become allocator-visible.\n"
      "# TYPE lavik_memory_admission_pending_bytes gauge\n"
      "lavik_memory_admission_pending_bytes ",
      memory_metrics.admission_pending_bytes_, "\n",
      "# HELP lavik_memory_rejected_commands_total Commands rejected by "
      "the memory limit.\n"
      "# TYPE lavik_memory_rejected_commands_total counter\n"
      "lavik_memory_rejected_commands_total ",
      memory_metrics.rejected_commands_, "\n",
      "# HELP lavik_storage_capacity_bytes Usable data capacity.\n"
      "# TYPE lavik_storage_capacity_bytes gauge\n"
      "# HELP lavik_storage_available_bytes Space available to foreground "
      "writes after preserving the defrag reserve.\n"
      "# TYPE lavik_storage_available_bytes gauge\n"
      "# HELP lavik_filesystem_available_bytes Space available on the data "
      "file's filesystem.\n"
      "# TYPE lavik_filesystem_available_bytes gauge\n");
  output.append(
      "# HELP lavik_worker_retained_memory_bytes Retained bytes charged "
      "to this worker's admission share.\n"
      "# TYPE lavik_worker_retained_memory_bytes gauge\n"
      "# HELP lavik_worker_memory_admission_pending_bytes Headroom held "
      "by this worker while retained allocations become visible.\n"
      "# TYPE lavik_worker_memory_admission_pending_bytes gauge\n"
      "# HELP lavik_worker_fullsync_reserved_memory_bytes Retained "
      "headroom reserved by this worker's active full-sync coverage maps.\n"
      "# TYPE lavik_worker_fullsync_reserved_memory_bytes gauge\n"
      "# HELP lavik_worker_client_buffered_request_bytes Client request "
      "bytes currently charged to this worker's separate buffer quota.\n"
      "# TYPE lavik_worker_client_buffered_request_bytes gauge\n"
      "# HELP lavik_worker_memory_limit_bytes Retained-memory admission "
      "limit assigned to this worker.\n"
      "# TYPE lavik_worker_memory_limit_bytes gauge\n");
  absl::StrAppend(
      &output,
      "# HELP lavik_cluster_control_wait_seconds Time spent collecting data "
      "observations or installing leases, including cross-worker waits.\n"
      "# TYPE lavik_cluster_control_wait_seconds histogram\n");
  for (unsigned stage = 0; stage < control_metrics.waits_.size(); ++stage) {
    const char* label = stage == 0 ? "observation" : "lease_installation";
    const auto& wait = control_metrics.waits_[stage];
    std::uint64_t count = 0;
    for (unsigned bucket = 0; bucket < wait.buckets_.size(); ++bucket) {
      count += wait.buckets_[bucket];
      const std::string bound =
          bucket == kControlWaitBoundsUs.size()
              ? "+Inf"
              : absl::StrCat(kControlWaitBoundsUs[bucket] / 1e6);
      absl::StrAppend(&output,
                      "lavik_cluster_control_wait_seconds_bucket{stage=\"",
                      label, "\",le=\"", bound, "\"} ", count, "\n");
    }
    absl::StrAppend(
        &output, "lavik_cluster_control_wait_seconds_count{stage=\"", label,
        "\"} ", count, "\n", "lavik_cluster_control_wait_seconds_sum{stage=\"",
        label, "\"} ", wait.sum_us_ / 1e6, "\n");
  }
  const unsigned memory_workers = MemoryAccountingWorkerCount();
  for (unsigned worker = 0; worker < memory_workers; ++worker) {
    const WorkerMemoryStats memory = GetWorkerMemoryStats(worker);
    absl::StrAppend(&output, "lavik_worker_retained_memory_bytes{worker=\"",
                    worker, "\"} ", memory.retained_bytes_, "\n",
                    "lavik_worker_memory_admission_pending_bytes{worker=\"",
                    worker, "\"} ", memory.admission_pending_bytes_, "\n",
                    "lavik_worker_fullsync_reserved_memory_bytes{worker=\"",
                    worker, "\"} ", memory.fullsync_reserved_bytes_, "\n",
                    "lavik_worker_client_buffered_request_bytes{worker=\"",
                    worker, "\"} ", memory.client_buffered_bytes_, "\n",
                    "lavik_worker_memory_limit_bytes{worker=\"", worker, "\"} ",
                    memory.retained_limit_bytes_, "\n");
  }
  output.append(
      "# HELP lavik_replication_backlog_bytes Allocated shared in-memory "
      "replication backlog bytes.\n"
      "# TYPE lavik_replication_backlog_bytes gauge\n"
      "# HELP lavik_replication_backlog_capacity_bytes Configured shared "
      "in-memory replication backlog capacity.\n"
      "# TYPE lavik_replication_backlog_capacity_bytes gauge\n"
      "# HELP lavik_replication_backlog_chunks Allocated 8 MiB replication "
      "backlog chunks.\n"
      "# TYPE lavik_replication_backlog_chunks gauge\n"
      "# HELP lavik_replication_backlog_floor_lsn Oldest retained flow LSN.\n"
      "# TYPE lavik_replication_backlog_floor_lsn gauge\n"
      "# HELP lavik_replication_backlog_tail_lsn Newest published flow LSN.\n"
      "# TYPE lavik_replication_backlog_tail_lsn gauge\n"
      "# HELP lavik_replication_backlog_pinned_cursors Live replica ACK "
      "coverage claims.\n"
      "# TYPE lavik_replication_backlog_pinned_cursors gauge\n"
      "# HELP lavik_replication_backlog_active Whether this worker's shared "
      "replication history is active.\n"
      "# TYPE lavik_replication_backlog_active gauge\n"
      "# HELP lavik_replication_backlog_coverage_revocations_total Lagging "
      "coverage claims revoked at the hard cap when backpressure is disabled.\n"
      "# TYPE lavik_replication_backlog_coverage_revocations_total counter\n"
      "# HELP lavik_replication_backlog_backpressure_waits_total Backlog "
      "capacity conflicts that waited for retained cursor progress.\n"
      "# TYPE lavik_replication_backlog_backpressure_waits_total counter\n"
      "# HELP lavik_replication_backlog_backpressured Whether this worker "
      "is currently waiting for retained cursor progress.\n"
      "# TYPE lavik_replication_backlog_backpressured gauge\n"
      "# HELP lavik_replication_publish_queue_bytes Commands staged before "
      "the shared backlog.\n"
      "# TYPE lavik_replication_publish_queue_bytes gauge\n"
      "# HELP lavik_replication_publish_queue_capacity_bytes Configured "
      "publisher staging capacity.\n"
      "# TYPE lavik_replication_publish_queue_capacity_bytes gauge\n"
      "# HELP lavik_fullsync_publish_queue_bytes Commands awaiting durable "
      "target ACK in active full-sync sessions.\n"
      "# TYPE lavik_fullsync_publish_queue_bytes gauge\n"
      "# HELP lavik_fullsync_publish_queue_admitted_bytes Bytes reserved by "
      "writes that have not finished publication.\n"
      "# TYPE lavik_fullsync_publish_queue_admitted_bytes gauge\n"
      "# HELP lavik_fullsync_publish_queue_capacity_bytes Aggregate "
      "configured capacity of active full-sync session queues.\n"
      "# TYPE lavik_fullsync_publish_queue_capacity_bytes gauge\n"
      "# HELP lavik_fullsync_sessions Active full-sync sessions on this "
      "worker.\n"
      "# TYPE lavik_fullsync_sessions gauge\n"
      "# HELP lavik_fullsync_publish_queue_backpressure_waits_total Writes "
      "that waited for full-sync queue credit.\n"
      "# TYPE lavik_fullsync_publish_queue_backpressure_waits_total "
      "counter\n");
  for (const storage::StorageReplicationLogMetrics& log :
       storage_metrics.replication_logs_) {
    const std::string labels = absl::StrCat("worker=\"", log.worker_id_, "\"");
    absl::StrAppend(
        &output, "lavik_replication_backlog_bytes{", labels, "} ",
        log.chunk_count_ * storage::kStorageBlockBytes, "\n",
        "lavik_replication_backlog_capacity_bytes{", labels, "} ",
        log.capacity_bytes_, "\n", "lavik_replication_backlog_chunks{", labels,
        "} ", log.chunk_count_, "\n", "lavik_replication_backlog_floor_lsn{",
        labels, "} ", log.floor_lsn_, "\n",
        "lavik_replication_backlog_tail_lsn{", labels, "} ", log.tail_lsn_,
        "\n", "lavik_replication_backlog_pinned_cursors{", labels, "} ",
        log.pinned_cursors_, "\n", "lavik_replication_backlog_active{", labels,
        "} ", log.active_ ? 1 : 0, "\n",
        "lavik_replication_backlog_coverage_revocations_total{", labels, "} ",
        log.coverage_revocations_, "\n",
        "lavik_replication_backlog_backpressure_waits_total{", labels, "} ",
        log.backpressure_waits_, "\n",
        "lavik_replication_backlog_backpressured{", labels, "} ",
        log.backpressured_ ? 1 : 0, "\n",
        "lavik_replication_publish_queue_bytes{", labels, "} ",
        log.publish_queue_bytes_, "\n",
        "lavik_replication_publish_queue_capacity_bytes{", labels, "} ",
        log.publish_queue_capacity_bytes_, "\n",
        "lavik_fullsync_publish_queue_bytes{", labels, "} ",
        log.fullsync_publish_queue_bytes_, "\n",
        "lavik_fullsync_publish_queue_admitted_bytes{", labels, "} ",
        log.fullsync_publisher_admitted_bytes_, "\n",
        "lavik_fullsync_publish_queue_capacity_bytes{", labels, "} ",
        log.fullsync_publish_queue_capacity_bytes_, "\n",
        "lavik_fullsync_sessions{", labels, "} ", log.fullsync_session_count_,
        "\n", "lavik_fullsync_publish_queue_backpressure_waits_total{", labels,
        "} ", log.fullsync_backpressure_waits_, "\n");
  }
  for (const storage::StorageDeviceMetrics& device : storage_metrics.devices_) {
    std::string labels =
        absl::StrCat("device=\"", device.device_id_, "\",path=\"");
    AppendEscapedLabel(device.path_, &labels);
    labels.append("\"");
    absl::StrAppend(&output, "lavik_storage_capacity_bytes{", labels, "} ",
                    device.capacity_bytes_, "\n",
                    "lavik_storage_available_bytes{", labels, "} ",
                    device.available_bytes_, "\n");
    if (device.filesystem_available_bytes_.has_value()) {
      absl::StrAppend(&output, "lavik_filesystem_available_bytes{", labels,
                      "} ", *device.filesystem_available_bytes_, "\n");
    }
  }
  co_return absl::OkStatus();
}

std::unique_ptr<bycorf::Service> CreateMetricsService(
    std::uint16_t port, const storage::StorageEngine* storage,
    std::function<bool()> server_ready) {
  auto service = std::make_unique<bycorf::HttpService>(port);
  service->RegisterGet(
      "/metrics",
      [storage, server_ready = std::move(server_ready)](
          const bycorf::HttpRequest&,
          bycorf::HttpResponse* response) -> bycorf::Task<absl::Status> {
        response->content_type_ = "text/plain; version=0.0.4; charset=utf-8";
        co_return co_await RenderPrometheusMetrics(*storage, server_ready(),
                                                   &response->body_);
      });
  return service;
}

}  // namespace lavik
