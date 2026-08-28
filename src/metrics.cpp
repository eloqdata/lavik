#include "keylane/metrics.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "absl/strings/str_cat.h"
#include "celer/net/http_service.h"
#include "celer/runtime/cross_core.h"
#include "celer/runtime/cycle_clock.h"
#include "celer/runtime/worker.h"
#include "keylane/command_table.h"
#include "keylane/memory.h"
#include "keylane/storage/engine.h"

namespace keylane {
namespace {

constexpr std::size_t ToIndex(CommandKind kind) noexcept {
  return static_cast<std::size_t>(kind);
}

// Written only by the owning worker. Alignment and the size assertion keep
// adjacent workers from sharing a cache line.
struct alignas(64) WorkerMetricsShard {
  std::array<CommandMetricTotals, kCommandKindCount> commands_{};
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

static_assert(alignof(WorkerMetricsShard) == 64);
static_assert(sizeof(WorkerMetricsShard) % 64 == 0);

std::unique_ptr<WorkerMetricsShard[]> g_worker_metrics;
unsigned g_worker_metrics_count = 0;
std::array<std::uint64_t, kCommandLatencyBucketUpperUs.size()>
    g_latency_bucket_upper_ticks{};
double g_counter_frequency = 1.0;

}  // namespace

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
  g_counter_frequency = std::max(1.0, celer::CycleCounterFrequency());
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
  ++g_worker_metrics[celer::ThisWorker().id_].connected_clients_;
}

void RecordConnectionClosed() noexcept {
  --g_worker_metrics[celer::ThisWorker().id_].connected_clients_;
}

void RecordClientBlocked() noexcept {
  ++g_worker_metrics[celer::ThisWorker().id_].blocked_clients_;
}

void RecordClientUnblocked() noexcept {
  WorkerMetricsShard& shard = g_worker_metrics[celer::ThisWorker().id_];
  assert(shard.blocked_clients_ != 0);
  --shard.blocked_clients_;
}

void RecordDatasetChanges(std::uint64_t count) noexcept {
  g_worker_metrics[celer::ThisWorker().id_].dataset_changes_total_ += count;
}

std::uint64_t LocalDatasetChangesTotal() noexcept {
  return g_worker_metrics[celer::ThisWorker().id_].dataset_changes_total_;
}

void MarkLocalDatasetChangesSaved(std::uint64_t total) noexcept {
  WorkerMetricsShard& shard = g_worker_metrics[celer::ThisWorker().id_];
  // Only one RDB job is active today, but max keeps this correct if completed
  // jobs are ever allowed to retire out of order.
  shard.dataset_changes_saved_ =
      std::max(shard.dataset_changes_saved_,
               std::min(total, shard.dataset_changes_total_));
}

void RecordReplicationConnectionOpened(
    ReplicationConnectionKind kind) noexcept {
  WorkerMetricsShard& shard = g_worker_metrics[celer::ThisWorker().id_];
  if (kind == ReplicationConnectionKind::kControl) {
    ++shard.replication_control_connections_;
  } else {
    ++shard.replication_flow_connections_;
  }
}

void RecordReplicationConnectionClosed(
    ReplicationConnectionKind kind) noexcept {
  WorkerMetricsShard& shard = g_worker_metrics[celer::ThisWorker().id_];
  if (kind == ReplicationConnectionKind::kControl) {
    --shard.replication_control_connections_;
  } else {
    --shard.replication_flow_connections_;
  }
}

void RecordDefragMetric(DefragMetricResult result) noexcept {
  WorkerMetricsShard& shard = g_worker_metrics[celer::ThisWorker().id_];
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
  g_worker_metrics[celer::ThisWorker().id_].active_defrags_ = active;
}

void SetDefragPending(bool pending) noexcept {
  g_worker_metrics[celer::ThisWorker().id_].pending_defrags_ = pending;
}

void RecordCommandMetric(CommandKind kind,
                         std::uint64_t elapsed_ticks) noexcept {
  const unsigned worker = celer::ThisWorker().id_;
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
  const auto bucket =
      std::lower_bound(g_latency_bucket_upper_ticks.begin(),
                       g_latency_bucket_upper_ticks.end(), elapsed_ticks);
  ++metric.latency_bins_[static_cast<std::size_t>(
      bucket - g_latency_bucket_upper_ticks.begin())];
}

celer::Task<WorkerMetricsSnapshot> CollectWorkerMetrics() {
  WorkerMetricsSnapshot result;
  result.counter_frequency_ = g_counter_frequency;
  for (unsigned worker = 0; worker < g_worker_metrics_count; ++worker) {
    // Copy on the owner rather than reading its live cache lines remotely.
    const auto [shard, connections] =
        co_await celer::SubmitTo(worker, [worker] {
          return std::pair{g_worker_metrics[worker],
                           celer::ThisWorker().self_->ActiveConnectionCount()};
        });
    const celer::Worker::StorageIoStats storage_io = co_await celer::SubmitTo(
        worker, [] { return celer::ThisWorker().self_->storage_io_stats(); });
    result.connections_ += connections;
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
  co_return result;
}

celer::Task<absl::Status> ResetCommandMetrics() {
  for (unsigned worker = 0; worker < g_worker_metrics_count; ++worker) {
    co_await celer::SubmitTo(worker, [worker] {
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

celer::Task<absl::Status> RenderPrometheusMetrics(
    const storage::StorageEngine& storage, bool server_ready,
    std::string* output_ptr) {
  std::string& output = *output_ptr;
  output.clear();
  output.reserve(server_ready ? 32 * 1024 : 256);
  absl::StrAppend(
      &output,
      "# HELP keylane_server_ready Whether storage recovery is complete and "
      "Redis requests are being accepted.\n"
      "# TYPE keylane_server_ready gauge\n"
      "keylane_server_ready ",
      server_ready ? 1 : 0, "\n");
  if (!server_ready) {
    co_return absl::OkStatus();
  }

  RefreshMemoryDiagnostics();
  const WorkerMetricsSnapshot worker_metrics = co_await CollectWorkerMetrics();
  const storage::StorageMetricsSnapshot storage_metrics =
      co_await storage.CollectMetrics();
  const storage::DefragTotals defrag = storage.DefragStats();
  const MemoryStats memory_metrics = GetMemoryStats();
  const std::uint64_t current_memory = memory_metrics.used_bytes_;
  absl::StrAppend(&output,
                  "# HELP keylane_commands_total Completed Redis commands.\n"
                  "# TYPE keylane_commands_total counter\n"
                  "keylane_commands_total ",
                  worker_metrics.TotalCalls(),
                  "\n"
                  "# HELP keylane_command_calls_total Completed Redis commands "
                  "by command.\n"
                  "# TYPE keylane_command_calls_total counter\n");
  for (std::size_t index = 0; index < worker_metrics.commands_.size();
       ++index) {
    const CommandMetricTotals& command = worker_metrics.commands_[index];
    if (command.calls_ == 0) {
      continue;
    }
    const std::string_view name =
        CommandMetricName(static_cast<CommandKind>(index));
    absl::StrAppend(&output, "keylane_command_calls_total{command=\"", name,
                    "\"} ", command.calls_, "\n");
  }

  output.append(
      "# HELP keylane_command_duration_seconds Redis command execution "
      "latency, excluding socket response writes.\n"
      "# TYPE keylane_command_duration_seconds histogram\n");
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
          &output, "keylane_command_duration_seconds_bucket{command=\"", name,
          "\",le=\"",
          SecondsFromMicroseconds(kCommandLatencyBucketUpperUs[bucket]), "\"} ",
          cumulative, "\n");
    }
    cumulative += command.latency_bins_.back();
    const double latency_seconds = static_cast<double>(command.latency_ticks_) /
                                   worker_metrics.counter_frequency_;
    absl::StrAppend(&output,
                    "keylane_command_duration_seconds_bucket{command=\"", name,
                    "\",le=\"+Inf\"} ", cumulative, "\n",
                    "keylane_command_duration_seconds_sum{command=\"", name,
                    "\"} ", latency_seconds, "\n",
                    "keylane_command_duration_seconds_count{command=\"", name,
                    "\"} ", command.calls_, "\n");
  }

  absl::StrAppend(
      &output,
      "# HELP keylane_storage_io_operations_total Completed storage I/O "
      "operations.\n"
      "# TYPE keylane_storage_io_operations_total counter\n"
      "keylane_storage_io_operations_total{operation=\"read\"} ",
      worker_metrics.storage_reads_.operations_, "\n",
      "keylane_storage_io_operations_total{operation=\"write\"} ",
      worker_metrics.storage_writes_.operations_, "\n",
      "keylane_storage_io_operations_total{operation=\"fdatasync\"} ",
      worker_metrics.storage_fdatasyncs_.operations_, "\n",
      "# HELP keylane_storage_io_bytes_total Bytes completed by storage "
      "reads and writes, or covered by successful fdatasync barriers.\n"
      "# TYPE keylane_storage_io_bytes_total counter\n"
      "keylane_storage_io_bytes_total{operation=\"read\"} ",
      worker_metrics.storage_reads_.bytes_, "\n",
      "keylane_storage_io_bytes_total{operation=\"write\"} ",
      worker_metrics.storage_writes_.bytes_, "\n",
      "keylane_storage_io_bytes_total{operation=\"fdatasync\"} ",
      worker_metrics.storage_fdatasyncs_.bytes_, "\n",
      "# HELP keylane_connections Current TCP connections, including Redis "
      "clients, metrics scrapes, and replication.\n"
      "# TYPE keylane_connections gauge\n"
      "keylane_connections ",
      worker_metrics.connections_, "\n",
      "# HELP keylane_connected_clients Current Redis client connections.\n"
      "# TYPE keylane_connected_clients gauge\n"
      "keylane_connected_clients ",
      worker_metrics.connected_clients_, "\n",
      "# HELP keylane_blocked_clients Redis clients waiting in blocking "
      "commands.\n"
      "# TYPE keylane_blocked_clients gauge\n"
      "keylane_blocked_clients ",
      worker_metrics.blocked_clients_, "\n",
      "# HELP keylane_replication_control_connections Current native "
      "replication control connections.\n"
      "# TYPE keylane_replication_control_connections gauge\n"
      "keylane_replication_control_connections ",
      worker_metrics.replication_control_connections_, "\n",
      "# HELP keylane_replication_flow_connections Current native replication "
      "data-flow connections.\n"
      "# TYPE keylane_replication_flow_connections gauge\n"
      "keylane_replication_flow_connections ",
      worker_metrics.replication_flow_connections_, "\n",
      "# HELP keylane_storage_defrag_runs_total Completed defrag attempts.\n"
      "# TYPE keylane_storage_defrag_runs_total counter\n"
      "keylane_storage_defrag_runs_total{result=\"success\"} ",
      worker_metrics.defrag_successes_, "\n",
      "keylane_storage_defrag_runs_total{result=\"resource_exhausted\"} ",
      worker_metrics.defrag_resource_exhausted_, "\n",
      "keylane_storage_defrag_runs_total{result=\"error\"} ",
      worker_metrics.defrag_failures_, "\n",
      "# HELP keylane_storage_defrag_active Currently running defrag jobs.\n"
      "# TYPE keylane_storage_defrag_active gauge\n"
      "keylane_storage_defrag_active ",
      worker_metrics.active_defrags_, "\n",
      "# HELP keylane_storage_defrag_pending Queued defrag jobs.\n"
      "# TYPE keylane_storage_defrag_pending gauge\n"
      "keylane_storage_defrag_pending ",
      worker_metrics.pending_defrags_, "\n",
      "# HELP keylane_storage_defrag_paused Whether relocation jobs are "
      "paused while candidates remain queued.\n"
      "# TYPE keylane_storage_defrag_paused gauge\n"
      "keylane_storage_defrag_paused ",
      defrag.paused_ ? 1 : 0, "\n",
      "# HELP keylane_storage_defrag_max_active_per_device Runtime maximum "
      "concurrent relocations per device.\n"
      "# TYPE keylane_storage_defrag_max_active_per_device gauge\n"
      "keylane_storage_defrag_max_active_per_device ",
      defrag.max_active_per_device_, "\n",
      "# HELP keylane_storage_defrag_block_sleep_seconds Runtime cooldown "
      "after each relocated block.\n"
      "# TYPE keylane_storage_defrag_block_sleep_seconds gauge\n"
      "keylane_storage_defrag_block_sleep_seconds ",
      static_cast<double>(defrag.block_sleep_ms_) / 1000.0, "\n",
      "# HELP keylane_storage_defrag_record_sleep_seconds Runtime pause "
      "after each record examined by defrag.\n"
      "# TYPE keylane_storage_defrag_record_sleep_seconds gauge\n"
      "keylane_storage_defrag_record_sleep_seconds ",
      static_cast<double>(defrag.record_sleep_us_) / 1'000'000.0, "\n",
      "# HELP keylane_memory_current_bytes Current process memory used for "
      "limit enforcement.\n"
      "# TYPE keylane_memory_current_bytes gauge\n"
      "keylane_memory_current_bytes ",
      current_memory, "\n",
      "# HELP keylane_memory_used_bytes Allocator usable bytes used for "
      "limit enforcement.\n"
      "# TYPE keylane_memory_used_bytes gauge\n"
      "keylane_memory_used_bytes ",
      memory_metrics.used_bytes_, "\n",
      "# HELP keylane_memory_rss_bytes Resident process memory.\n"
      "# TYPE keylane_memory_rss_bytes gauge\n"
      "keylane_memory_rss_bytes ",
      memory_metrics.rss_bytes_, "\n",
      "# HELP keylane_memory_committed_bytes Memory committed by the "
      "configured allocator.\n"
      "# TYPE keylane_memory_committed_bytes gauge\n"
      "keylane_memory_committed_bytes ",
      memory_metrics.committed_bytes_, "\n",
      "# HELP keylane_memory_reserved_bytes Address space reserved by the "
      "configured allocator.\n"
      "# TYPE keylane_memory_reserved_bytes gauge\n"
      "keylane_memory_reserved_bytes ",
      memory_metrics.reserved_bytes_, "\n",
      "# HELP keylane_memory_max_bytes Configured process memory limit.\n"
      "# TYPE keylane_memory_max_bytes gauge\n"
      "keylane_memory_max_bytes ",
      memory_metrics.max_bytes_, "\n",
      "# HELP keylane_fullsync_reserved_memory_bytes Memory headroom "
      "reserved for active full-sync coverage maps.\n"
      "# TYPE keylane_fullsync_reserved_memory_bytes gauge\n"
      "keylane_fullsync_reserved_memory_bytes ",
      memory_metrics.fullsync_reserved_bytes_, "\n",
      "# HELP keylane_memory_admission_pending_bytes Worker-local headroom "
      "held while slow-path allocations become allocator-visible.\n"
      "# TYPE keylane_memory_admission_pending_bytes gauge\n"
      "keylane_memory_admission_pending_bytes ",
      memory_metrics.admission_pending_bytes_, "\n",
      "# HELP keylane_memory_rejected_commands_total Commands rejected by "
      "the memory limit.\n"
      "# TYPE keylane_memory_rejected_commands_total counter\n"
      "keylane_memory_rejected_commands_total ",
      memory_metrics.rejected_commands_, "\n",
      "# HELP keylane_storage_capacity_bytes Usable data capacity.\n"
      "# TYPE keylane_storage_capacity_bytes gauge\n"
      "# HELP keylane_storage_available_bytes Space available to foreground "
      "writes after preserving the defrag reserve.\n"
      "# TYPE keylane_storage_available_bytes gauge\n"
      "# HELP keylane_filesystem_available_bytes Space available on the data "
      "file's filesystem.\n"
      "# TYPE keylane_filesystem_available_bytes gauge\n");
  output.append(
      "# HELP keylane_replication_backlog_bytes Allocated shared in-memory "
      "replication backlog bytes.\n"
      "# TYPE keylane_replication_backlog_bytes gauge\n"
      "# HELP keylane_replication_backlog_capacity_bytes Configured shared "
      "in-memory replication backlog capacity.\n"
      "# TYPE keylane_replication_backlog_capacity_bytes gauge\n"
      "# HELP keylane_replication_backlog_chunks Allocated 8 MiB replication "
      "backlog chunks.\n"
      "# TYPE keylane_replication_backlog_chunks gauge\n"
      "# HELP keylane_replication_backlog_floor_lsn Oldest retained flow LSN.\n"
      "# TYPE keylane_replication_backlog_floor_lsn gauge\n"
      "# HELP keylane_replication_backlog_tail_lsn Newest published flow LSN.\n"
      "# TYPE keylane_replication_backlog_tail_lsn gauge\n"
      "# HELP keylane_replication_backlog_pinned_cursors Live replica ACK "
      "cursors pinning history.\n"
      "# TYPE keylane_replication_backlog_pinned_cursors gauge\n"
      "# HELP keylane_replication_backlog_backpressured Whether the worker is "
      "waiting for ACK progress below the low watermark.\n"
      "# TYPE keylane_replication_backlog_backpressured gauge\n"
      "# HELP keylane_replication_backlog_active Whether this worker's shared "
      "replication history is active.\n"
      "# TYPE keylane_replication_backlog_active gauge\n"
      "# HELP keylane_replication_backlog_backpressure_waits_total Backlog "
      "high-watermark backpressure episodes.\n"
      "# TYPE keylane_replication_backlog_backpressure_waits_total counter\n"
      "# HELP keylane_replication_publish_queue_bytes Commands staged before "
      "the shared backlog.\n"
      "# TYPE keylane_replication_publish_queue_bytes gauge\n"
      "# HELP keylane_replication_publish_queue_capacity_bytes Configured "
      "publisher staging capacity.\n"
      "# TYPE keylane_replication_publish_queue_capacity_bytes gauge\n"
      "# HELP keylane_fullsync_publish_queue_bytes Commands awaiting durable "
      "target ACK in active full-sync sessions.\n"
      "# TYPE keylane_fullsync_publish_queue_bytes gauge\n"
      "# HELP keylane_fullsync_publish_queue_admitted_bytes Bytes reserved by "
      "writes that have not finished publication.\n"
      "# TYPE keylane_fullsync_publish_queue_admitted_bytes gauge\n"
      "# HELP keylane_fullsync_publish_queue_capacity_bytes Aggregate "
      "configured capacity of active full-sync session queues.\n"
      "# TYPE keylane_fullsync_publish_queue_capacity_bytes gauge\n"
      "# HELP keylane_fullsync_sessions Active full-sync sessions on this "
      "worker.\n"
      "# TYPE keylane_fullsync_sessions gauge\n"
      "# HELP keylane_fullsync_publish_queue_backpressure_waits_total Writes "
      "that waited for full-sync queue credit.\n"
      "# TYPE keylane_fullsync_publish_queue_backpressure_waits_total "
      "counter\n");
  for (const storage::StorageReplicationLogMetrics& log :
       storage_metrics.replication_logs_) {
    const std::string labels = absl::StrCat("worker=\"", log.worker_id_, "\"");
    absl::StrAppend(
        &output, "keylane_replication_backlog_bytes{", labels, "} ",
        log.chunk_count_ * storage::kStorageBlockBytes, "\n",
        "keylane_replication_backlog_capacity_bytes{", labels, "} ",
        log.capacity_bytes_, "\n", "keylane_replication_backlog_chunks{",
        labels, "} ", log.chunk_count_, "\n",
        "keylane_replication_backlog_floor_lsn{", labels, "} ", log.floor_lsn_,
        "\n", "keylane_replication_backlog_tail_lsn{", labels, "} ",
        log.tail_lsn_, "\n", "keylane_replication_backlog_pinned_cursors{",
        labels, "} ", log.pinned_cursors_, "\n",
        "keylane_replication_backlog_backpressured{", labels, "} ",
        log.capacity_backpressured_ ? 1 : 0, "\n",
        "keylane_replication_backlog_active{", labels, "} ",
        log.active_ ? 1 : 0, "\n",
        "keylane_replication_backlog_backpressure_waits_total{", labels, "} ",
        log.backpressure_waits_, "\n",
        "keylane_replication_publish_queue_bytes{", labels, "} ",
        log.publish_queue_bytes_, "\n",
        "keylane_replication_publish_queue_capacity_bytes{", labels, "} ",
        log.publish_queue_capacity_bytes_, "\n",
        "keylane_fullsync_publish_queue_bytes{", labels, "} ",
        log.fullsync_publish_queue_bytes_, "\n",
        "keylane_fullsync_publish_queue_admitted_bytes{", labels, "} ",
        log.fullsync_publisher_admitted_bytes_, "\n",
        "keylane_fullsync_publish_queue_capacity_bytes{", labels, "} ",
        log.fullsync_publish_queue_capacity_bytes_, "\n",
        "keylane_fullsync_sessions{", labels, "} ", log.fullsync_session_count_,
        "\n", "keylane_fullsync_publish_queue_backpressure_waits_total{",
        labels, "} ", log.fullsync_backpressure_waits_, "\n");
  }
  for (const storage::StorageDeviceMetrics& device : storage_metrics.devices_) {
    std::string labels =
        absl::StrCat("device=\"", device.device_id_, "\",path=\"");
    AppendEscapedLabel(device.path_, &labels);
    labels.append("\"");
    absl::StrAppend(&output, "keylane_storage_capacity_bytes{", labels, "} ",
                    device.capacity_bytes_, "\n",
                    "keylane_storage_available_bytes{", labels, "} ",
                    device.available_bytes_, "\n");
    if (device.filesystem_available_bytes_.has_value()) {
      absl::StrAppend(&output, "keylane_filesystem_available_bytes{", labels,
                      "} ", *device.filesystem_available_bytes_, "\n");
    }
  }
  co_return absl::OkStatus();
}

std::unique_ptr<celer::Service> CreateMetricsService(
    std::uint16_t port, const storage::StorageEngine* storage,
    std::function<bool()> server_ready) {
  auto service = std::make_unique<celer::HttpService>(port);
  service->RegisterGet(
      "/metrics",
      [storage, server_ready = std::move(server_ready)](
          const celer::HttpRequest&,
          celer::HttpResponse* response) -> celer::Task<absl::Status> {
        response->content_type_ = "text/plain; version=0.0.4; charset=utf-8";
        co_return co_await RenderPrometheusMetrics(
            *storage, server_ready(), &response->body_);
      });
  return service;
}

}  // namespace keylane
