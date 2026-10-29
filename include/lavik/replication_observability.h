/* Copyright (C) 2026 EloqData Inc. SPDX-License-Identifier: Apache-2.0 */
#pragma once

#include <array>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace lavik {

// These enums are a bounded diagnostic vocabulary, never protocol state or
// authorization inputs. Native ACKs prove apply, not crash-durable replay.
enum class ReplicationSide { kSource, kTarget };
enum class FullSyncPhase {
  kConnecting,
  kReset,
  kTransfer,
  kCut,
  kPromote,
  kActivate,
  kOnline,
  kCleanup,
};
enum class ReplicationFailure {
  kNone,
  kNetwork,
  kTimeout,
  kAuthentication,
  kProtocol,
  kResource,
  kStorage,
  kCancelled,
  kFullBusy,
  kAuthority,
  kOther,
  kCount,
};
enum class FullSyncResult { kSuccess, kFailure, kCancelled };
inline constexpr std::size_t kReplicationFailureCount =
    static_cast<std::size_t>(ReplicationFailure::kCount);
inline constexpr std::array<double, 9> kFullSyncDurationBounds{
    1, 5, 10, 30, 60, 300, 900, 3600, 14400};
inline constexpr std::array<std::string_view, 4> kReplicationProgressSteps{
    "send", "apply", "ack", "full"};

std::string_view ReplicationSideName(ReplicationSide side) noexcept;
std::string_view FullSyncPhaseName(FullSyncPhase phase) noexcept;
std::string_view ReplicationFailureName(ReplicationFailure reason) noexcept;
std::string_view FullSyncResultName(FullSyncResult result) noexcept;

// Coordinates are meaningful only within this exact session/history/flow.
// Identity strings are INFO-only; exporters must never use them as labels.
struct ReplicationFlowObservation {
  ReplicationSide side_ = ReplicationSide::kTarget;
  unsigned worker_ = 0;
  unsigned flow_ = 0;
  std::uint64_t session_id_ = 0;
  std::string history_id_;
  std::string peer_id_;
  bool connected_ = false;
  bool full_ = false;
  bool comparable_ = false;
  FullSyncPhase phase_ = FullSyncPhase::kConnecting;
  double phase_seconds_ = 0;
  double full_seconds_ = 0;
  // Absent during FULL, disconnect, or an incoherent frontier snapshot.
  std::optional<std::uint64_t> published_next_lsn_;
  std::optional<std::uint64_t> received_next_lsn_;
  std::optional<std::uint64_t> applied_next_lsn_;
  std::optional<std::uint64_t> ack_next_lsn_;
  std::uint64_t pending_events_ = 0;
  std::array<bool, 4> pending_{};
  std::array<double, 4> no_progress_seconds_{};
  std::uint64_t bytes_ = 0;
  std::uint64_t events_ = 0;
  std::uint64_t full_items_ = 0;
  std::uint64_t partitions_ = 0;
};

struct FullSyncObservationResult {
  ReplicationSide side_ = ReplicationSide::kTarget;
  FullSyncResult result_ = FullSyncResult::kFailure;
  ReplicationFailure reason_ = ReplicationFailure::kNone;
  FullSyncPhase phase_ = FullSyncPhase::kConnecting;
  std::uint64_t session_id_ = 0;
  std::string history_id_;
  std::string peer_id_;
  std::string detail_;
  std::optional<bool> promotion_complete_;  // target-local durable root fact
  double seconds_ = 0;
};

struct ReplicationProgressTotals {
  std::uint64_t applied_events_ = 0;
  ReplicationSide side_ = ReplicationSide::kTarget;
  unsigned worker_ = 0;
  std::uint64_t bytes_ = 0;
  std::uint64_t events_ = 0;
  std::uint64_t full_items_ = 0;
  std::uint64_t partitions_ = 0;
};

struct FullSyncObservationTotals {
  std::array<std::uint64_t, 3> results_{};
  std::array<std::uint64_t, kReplicationFailureCount> errors_{};
  std::array<std::uint64_t, kFullSyncDurationBounds.size() + 1>
      duration_bins_{};
  double duration_sum_ = 0;
};

// A best-effort, generation-validated native replication observation. A
// missing coherent cut is explicit, never a healthy zero or an old history.
// Counters live for this process; last_full_ is intentionally not persisted.
struct ReplicationObservabilitySnapshot {
  bool available_ = false;
  bool following_ = false;
  bool connected_ = false;
  bool population_ready_ = false;
  bool catalog_ready_ = false;
  bool readable_ = false;
  bool loading_ = false;
  bool failed_stopped_ = false;
  bool waiting_ = false;
  ReplicationFailure wait_reason_ = ReplicationFailure::kNone;
  std::optional<bool> full_promotion_complete_;
  double loading_no_progress_seconds_ = 0;
  std::string failure_detail_;
  std::vector<ReplicationFlowObservation> flows_;
  std::vector<ReplicationProgressTotals> totals_;
  std::array<FullSyncObservationTotals, 2> full_totals_{};
  std::array<std::uint64_t, kReplicationFailureCount> retries_{};
  std::optional<FullSyncObservationResult> last_full_;
};

// Appends only native diagnostic fields, preserving Redis compatibility
// fields. Arbitrary error text is escaped to keep one INFO field per line.
void AppendReplicationProgressInfo(const ReplicationObservabilitySnapshot& view,
                                   std::string* output);
void AppendReplicationProgressMetrics(
    const ReplicationObservabilitySnapshot& view, std::string* output);

}  // namespace lavik
