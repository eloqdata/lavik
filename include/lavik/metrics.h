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

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

#include "bycorf/runtime/task.h"
#include "lavik/command.h"

namespace lavik {

namespace storage {
class StorageEngine;
}

inline constexpr std::array<std::uint64_t, 31> kCommandLatencyBucketUpperUs{
    1,         2,         4,        8,       16,      32,      64,
    128,       256,       512,      1'000,   1'250,   1'500,   2'000,
    2'500,     3'000,     3'500,    4'000,   5'000,   6'000,   8'000,
    16'000,    32'000,    64'000,   128'000, 256'000, 512'000, 1'000'000,
    2'000'000, 4'000'000, 8'000'000};

inline constexpr std::size_t kCommandKindCount =
    static_cast<std::size_t>(CommandKind::kCount);

struct CommandMetricTotals {
  std::uint64_t calls_ = 0;
  std::uint64_t latency_ticks_ = 0;
  // The final bin is +Inf; finite bins are non-cumulative internally.
  std::array<std::uint64_t, kCommandLatencyBucketUpperUs.size() + 1>
      latency_bins_{};
};

struct StorageIoMetricTotals {
  std::uint64_t operations_ = 0;
  std::uint64_t bytes_ = 0;
};

struct WorkerMetricsSnapshot {
  std::array<CommandMetricTotals, kCommandKindCount> commands_{};
  std::uint64_t connections_ = 0;
  std::uint64_t connected_clients_ = 0;
  std::uint64_t blocked_clients_ = 0;
  std::uint64_t replication_control_connections_ = 0;
  std::uint64_t replication_flow_connections_ = 0;
  std::uint64_t defrag_successes_ = 0;
  std::uint64_t defrag_resource_exhausted_ = 0;
  std::uint64_t defrag_failures_ = 0;
  std::uint64_t active_defrags_ = 0;
  std::uint64_t pending_defrags_ = 0;
  std::uint64_t rdb_changes_since_last_save_ = 0;
  StorageIoMetricTotals storage_reads_{};
  StorageIoMetricTotals storage_writes_{};
  StorageIoMetricTotals storage_fdatasyncs_{};
  double counter_frequency_ = 1.0;

  std::uint64_t TotalCalls() const noexcept;
};

// Process-level Meta control-session metrics. The client is owned by worker 0,
// while the HTTP renderer may run on any worker, so these values use atomics
// instead of per-worker shards. Deliberately no node, session, assignment, or
// group labels are exposed: those identities are high-cardinality and belong
// in structured logs rather than Prometheus series.
enum class ClusterControlWait { kObservation, kLeaseInstallation };
inline constexpr std::array<std::uint64_t, 10> kControlWaitBoundsUs{
    100, 500, 1000, 5000, 10000, 50000, 100000, 300000, 1000000, 5000000};
struct ClusterControlWaitSnapshot {
  // Disjoint buckets; the last is overflow. Export converts to cumulative.
  std::array<std::uint64_t, kControlWaitBoundsUs.size() + 1> buckets_{};
  std::uint64_t sum_us_ = 0;
};

struct ClusterControlMetricsSnapshot {
  std::uint64_t connected_ = 0;
  std::uint64_t reconnects_ = 0;
  std::uint64_t protocol_errors_ = 0;
  std::uint64_t full_states_applied_ = 0;
  std::uint64_t lease_grants_ = 0;
  std::uint64_t lease_denials_ = 0;
  std::uint64_t lease_expirations_ = 0;
  std::uint64_t directive_successes_ = 0;
  std::uint64_t directive_failures_ = 0;
  std::array<ClusterControlWaitSnapshot, 2> waits_{};
};

void SetClusterControlConnected(bool connected) noexcept;
void RecordClusterControlReconnect() noexcept;
void RecordClusterControlProtocolError() noexcept;
void RecordClusterControlFullStateApplied() noexcept;
void RecordClusterControlLeaseGrant() noexcept;
void RecordClusterControlLeaseDenial() noexcept;
void RecordClusterControlLeaseExpiration() noexcept;
void RecordClusterControlDirectiveResult(bool succeeded) noexcept;
void RecordClusterControlWait(ClusterControlWait stage,
                              std::uint64_t us) noexcept;
ClusterControlMetricsSnapshot GetClusterControlMetrics() noexcept;

void InitWorkerMetrics(unsigned worker_count);
void RecordCommandMetric(CommandKind kind,
                         std::uint64_t elapsed_ticks) noexcept;
void RecordConnectionOpened() noexcept;
void RecordConnectionClosed() noexcept;
void RecordClientBlocked() noexcept;
void RecordClientUnblocked() noexcept;

// Dataset change accounting is worker-local on the write path. INFO sums the
// shards, avoiding a contended global counter. RDB backup captures and later
// installs one saved baseline per worker so writes after the cut remain dirty.
void RecordDatasetChanges(std::uint64_t count = 1) noexcept;
[[nodiscard]] std::uint64_t LocalDatasetChangesTotal() noexcept;
void MarkLocalDatasetChangesSaved(std::uint64_t total) noexcept;
// Collects the exact process-wide unsaved-change count by reading each shard
// on its owning worker. This is intentionally a low-frequency control-path
// operation; ordinary writes update only worker-local counters.
bycorf::Task<std::uint64_t> CollectDatasetChangesSinceLastSave();

enum class ReplicationConnectionKind : std::uint8_t {
  kControl,
  kFlow,
};

void RecordReplicationConnectionOpened(ReplicationConnectionKind kind) noexcept;
void RecordReplicationConnectionClosed(ReplicationConnectionKind kind) noexcept;

enum class DefragMetricResult : std::uint8_t {
  kSuccess,
  kResourceExhausted,
  kFailure,
};

void RecordDefragMetric(DefragMetricResult result) noexcept;
void SetDefragActive(bool active) noexcept;
void SetDefragPending(bool pending) noexcept;
bycorf::Task<WorkerMetricsSnapshot> CollectWorkerMetrics();
// Clears Redis command counters on their owning workers. Live gauges and
// dataset persistence accounting are deliberately preserved.
bycorf::Task<absl::Status> ResetCommandMetrics();
std::string_view CommandMetricName(CommandKind kind) noexcept;

bycorf::Task<absl::Status> RenderPrometheusMetrics(
    const storage::StorageEngine& storage, bool server_ready,
    std::string* output);

}  // namespace lavik

namespace bycorf {
class Service;
}

namespace lavik {

std::unique_ptr<bycorf::Service> CreateMetricsService(
    std::uint16_t port, const storage::StorageEngine* storage,
    std::function<bool()> server_ready);

}  // namespace lavik
