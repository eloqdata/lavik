#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

#include "celer/runtime/task.h"
#include "keylane/command.h"

namespace keylane {

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
  StorageIoMetricTotals storage_reads_{};
  StorageIoMetricTotals storage_writes_{};
  StorageIoMetricTotals storage_fdatasyncs_{};
  double counter_frequency_ = 1.0;

  std::uint64_t TotalCalls() const noexcept;
};

void InitWorkerMetrics(unsigned worker_count);
void RecordCommandMetric(CommandKind kind,
                         std::uint64_t elapsed_ticks) noexcept;
void RecordConnectionOpened() noexcept;
void RecordConnectionClosed() noexcept;
void RecordClientBlocked() noexcept;
void RecordClientUnblocked() noexcept;

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
celer::Task<WorkerMetricsSnapshot> CollectWorkerMetrics();
std::string_view CommandMetricName(CommandKind kind) noexcept;

celer::Task<absl::Status> RenderPrometheusMetrics(
    const storage::StorageEngine& storage, bool server_ready,
    std::string* output);

}  // namespace keylane

namespace celer {
class Service;
}

namespace keylane {

std::unique_ptr<celer::Service> CreateMetricsService(
    std::uint16_t port, const storage::StorageEngine* storage,
    std::function<bool()> server_ready);

}  // namespace keylane
