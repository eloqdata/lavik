/* Copyright (C) 2026 EloqData Inc. SPDX-License-Identifier: Apache-2.0 */
#include <algorithm>
#include <map>
#include <tuple>

#include "absl/strings/str_cat.h"
#include "lavik/replication_observability.h"

namespace lavik {
std::string_view ReplicationSideName(ReplicationSide side) noexcept {
  return side == ReplicationSide::kSource ? "source" : "target";
}
std::string_view FullSyncPhaseName(FullSyncPhase phase) noexcept {
  constexpr std::array names{"connecting", "reset",    "transfer", "cut",
                             "promote",    "activate", "online",   "cleanup"};
  return names[static_cast<unsigned>(phase)];
}
std::string_view ReplicationFailureName(ReplicationFailure reason) noexcept {
  constexpr std::array names{"none",           "network",   "timeout",
                             "authentication", "protocol",  "resource",
                             "storage",        "cancelled", "full_busy",
                             "authority",      "other"};
  return names[static_cast<unsigned>(reason)];
}
std::string_view FullSyncResultName(FullSyncResult result) noexcept {
  constexpr std::array names{"success", "failure", "cancelled"};
  return names[static_cast<unsigned>(result)];
}

void ReplicationFlowMetrics::Add(const ReplicationFlowObservation& flow) {
  connected_ += flow.connected_;
  comparable_ += flow.comparable_;
  pending_events_ = std::max(pending_events_, flow.pending_events_);
  for (unsigned step = 0; step != pending_.size(); ++step) {
    pending_[step] += flow.pending_[step];
    ages_[step] = std::max(ages_[step], flow.no_progress_seconds_[step]);
  }
  if (flow.full_) {
    const auto phase = static_cast<unsigned>(flow.phase_);
    ++phases_[phase];
    phase_seconds_[phase] =
        std::max(phase_seconds_[phase], flow.phase_seconds_);
    full_seconds_ = std::max(full_seconds_, flow.full_seconds_);
  }
  if (flow.published_next_lsn_) published_ = flow.published_next_lsn_;
  if (flow.side_ == ReplicationSide::kTarget) {
    received_ = flow.received_next_lsn_;
    applied_ = flow.applied_next_lsn_;
    ack_ = flow.ack_next_lsn_;
  }
}

namespace {
std::string InfoText(std::string_view value) {
  std::string result;
  for (unsigned char c : value) {
    if (c == '\r')
      result += "\\r";
    else if (c == '\n')
      result += "\\n";
    else if (c == '\\')
      result += "\\\\";
    else if (c < 32 || c == 127)
      result += '?';
    else
      result += static_cast<char>(c);
  }
  return result;
}
std::string Cursor(std::optional<std::uint64_t> value) {
  return value ? absl::StrCat(*value) : "unavailable";
}
}  // namespace

void AppendReplicationProgressInfo(const ReplicationObservabilitySnapshot& view,
                                   std::string* output) {
  auto field = [&](std::string_view name, const auto& value) {
    absl::StrAppend(output, "lavik_replication_", name, ":", value, "\r\n");
  };
  field("observation_available", view.available_ ? 1 : 0);
  if (view.available_) {
    field("native_following", view.following_ ? 1 : 0);
    field("population_ready", view.population_ready_ ? 1 : 0);
    field("catalog_ready", view.catalog_ready_ ? 1 : 0);
    field("loading", view.loading_ ? 1 : 0);
    field("waiting", view.waiting_ ? 1 : 0);
    field("wait_reason", ReplicationFailureName(view.wait_reason_));
    field("full_promotion_complete",
          view.full_promotion_complete_
              ? (*view.full_promotion_complete_ ? "1" : "0")
              : "unavailable");
    field("loading_no_progress_seconds", view.loading_no_progress_seconds_);
    field("failure_reason", InfoText(view.failure_detail_));
    field("progress_flows", view.flows_.size());
    for (std::size_t index = 0; index < view.flows_.size(); ++index) {
      const auto& flow = view.flows_[index];
      field(
          absl::StrCat("progress", index),
          absl::StrCat(
              "side=", ReplicationSideName(flow.side_),
              ",worker=", flow.worker_, ",flow=", flow.flow_,
              ",session=", flow.session_id_, ",history=", flow.history_id_,
              ",peer=", flow.peer_id_, ",connected=", flow.connected_ ? 1 : 0,
              ",comparable=", flow.comparable_ ? 1 : 0, ",full=",
              flow.full_ ? 1 : 0, ",phase=", FullSyncPhaseName(flow.phase_),
              ",phase_seconds=", flow.phase_seconds_,
              ",full_seconds=", flow.full_seconds_,
              ",published_next_lsn=", Cursor(flow.published_next_lsn_),
              ",received_next_lsn=", Cursor(flow.received_next_lsn_),
              ",applied_next_lsn=", Cursor(flow.applied_next_lsn_),
              ",ack_next_lsn=", Cursor(flow.ack_next_lsn_), ",pending_events=",
              flow.comparable_ ? absl::StrCat(flow.pending_events_)
                               : "unavailable",
              ",bytes=", flow.bytes_, ",events=", flow.events_, ",full_items=",
              flow.full_items_, ",partitions=", flow.partitions_));
      for (unsigned step = 0; step != kReplicationProgressSteps.size(); ++step)
        field(absl::StrCat("progress", index, "_",
                           kReplicationProgressSteps[step],
                           "_no_progress_seconds"),
              flow.no_progress_seconds_[step]);
    }
  }
  field("last_full_available", view.last_full_.has_value() ? 1 : 0);
  if (view.last_full_) {
    const auto& last = *view.last_full_;
    field("last_full_side", ReplicationSideName(last.side_));
    field("last_full_session", last.session_id_);
    field("last_full_history", last.history_id_);
    field("last_full_peer", last.peer_id_);
    field("last_full_result", FullSyncResultName(last.result_));
    field("last_full_phase", FullSyncPhaseName(last.phase_));
    field("last_full_seconds", last.seconds_);
    field("last_full_reason", ReplicationFailureName(last.reason_));
    field("last_full_detail", InfoText(last.detail_));
    field("last_full_promotion_complete",
          last.promotion_complete_ ? (*last.promotion_complete_ ? "1" : "0")
                                   : "unavailable");
  }
}

void AppendReplicationProgressMetrics(
    const ReplicationObservabilitySnapshot& view, std::string* output) {
  auto type = [&](std::string_view name, std::string_view kind,
                  std::string_view help) {
    absl::StrAppend(output, "# HELP lavik_replication_", name, " ", help,
                    "\n# TYPE lavik_replication_", name, " ", kind, "\n");
  };
  auto metric = [&](std::string_view name, std::string_view labels,
                    const auto& value) {
    absl::StrAppend(output, "lavik_replication_", name, labels, " ", value,
                    "\n");
  };
  type("observation_available", "gauge",
       "Whether a current native replication observation is available.");
  metric("observation_available", "", view.available_ ? 1 : 0);
  type("transfer_bytes_total", "counter",
       "Native data-frame bytes successfully sent by source or received by "
       "target, including framing.");
  type("transfer_events_total", "counter",
       "Complete ONLINE events sent by source or accepted by target; excludes "
       "FULL and is not durable ACK progress.");
  type("applied_events_total", "counter",
       "Complete native ONLINE events proven applied on target; sampled within "
       "each history and excludes FULL cut installation.");
  type("fullsync_items_total", "counter",
       "Native FULL snapshot record payloads and complete incremental commands "
       "processed; includes snapshot fragments, not an estimated completion "
       "percentage.");
  type("fullsync_partitions_total", "counter",
       "Completed native FULL partition handoffs across attempts.");
  auto emit_total = [&](const ReplicationProgressTotals& total) {
    const auto labels =
        absl::StrCat("{side=\"", ReplicationSideName(total.side_),
                     "\",worker=\"", total.worker_, "\"}");
    metric("transfer_bytes_total", labels, total.bytes_);
    metric("transfer_events_total", labels, total.events_);
    metric("fullsync_items_total", labels, total.full_items_);
    if (total.side_ == ReplicationSide::kTarget)
      metric("applied_events_total", labels, total.applied_events_);
    metric("fullsync_partitions_total", labels, total.partitions_);
  };
  if (view.worker_count_ != 0) {
    // Collection preserves side/worker order while dropping only all-zero
    // rows. Reconstruct those rows here, preserving the exporter series set
    // even before any relationship has existed or after it has retired.
    auto next = view.totals_.begin();
    for (unsigned side = 0; side != 2; ++side) {
      for (unsigned worker = 0; worker != view.worker_count_; ++worker) {
        const auto current_side = static_cast<ReplicationSide>(side);
        if (next != view.totals_.end() && next->side_ == current_side &&
            next->worker_ == worker) {
          emit_total(*next++);
        } else {
          emit_total({.side_ = current_side, .worker_ = worker});
        }
      }
    }
  } else {
    for (const auto& total : view.totals_) emit_total(total);
  }
  type("fullsync_results_total", "counter",
       "Terminal native FULL attempts; successful activation is counted once "
       "per session.");
  type("fullsync_failures_total", "counter",
       "Native FULL failures by bounded cause, excluding cancellation and "
       "admission waits.");
  type("fullsync_duration_seconds", "histogram",
       "Duration of terminal native FULL attempts, including failed and "
       "cancelled attempts.");
  for (unsigned side = 0; side != 2; ++side) {
    const auto name = ReplicationSideName(static_cast<ReplicationSide>(side));
    const auto& totals = view.full_totals_[side];
    for (unsigned result = 0; result != 3; ++result)
      metric(
          "fullsync_results_total",
          absl::StrCat("{side=\"", name, "\",result=\"",
                       FullSyncResultName(static_cast<FullSyncResult>(result)),
                       "\"}"),
          totals.results_[result]);
    for (unsigned reason = 0; reason != kReplicationFailureCount; ++reason)
      metric("fullsync_failures_total",
             absl::StrCat("{side=\"", name, "\",reason=\"",
                          ReplicationFailureName(
                              static_cast<ReplicationFailure>(reason)),
                          "\"}"),
             totals.errors_[reason]);
    std::uint64_t count = 0;
    for (unsigned bucket = 0; bucket != totals.duration_bins_.size();
         ++bucket) {
      count += totals.duration_bins_[bucket];
      const auto bound = bucket < kFullSyncDurationBounds.size()
                             ? absl::StrCat(kFullSyncDurationBounds[bucket])
                             : "+Inf";
      metric("fullsync_duration_seconds_bucket",
             absl::StrCat("{side=\"", name, "\",le=\"", bound, "\"}"), count);
    }
    const auto labels = absl::StrCat("{side=\"", name, "\"}");
    metric("fullsync_duration_seconds_count", labels, count);
    metric("fullsync_duration_seconds_sum", labels, totals.duration_sum_);
  }
  type("retries_total", "counter",
       "Native target reconnect rounds by bounded cause; not FULL failures.");
  for (unsigned reason = 0; reason != kReplicationFailureCount; ++reason)
    metric("retries_total",
           absl::StrCat(
               "{reason=\"",
               ReplicationFailureName(static_cast<ReplicationFailure>(reason)),
               "\"}"),
           view.retries_[reason]);
  if (!view.available_) return;
  const std::array<std::pair<std::string_view, bool>, 8> flags{
      {{"native_following", view.following_},
       {"native_connected", view.connected_},
       {"population_ready", view.population_ready_},
       {"catalog_ready", view.catalog_ready_},
       {"data_readable", view.readable_},
       {"loading", view.loading_},
       {"failed_stopped", view.failed_stopped_},
       {"waiting", view.waiting_}}};
  for (const auto& [name, value] : flags) {
    type(name, "gauge",
         "Native diagnostic state; never a serving-authority grant.");
    metric(name, "", value ? 1 : 0);
  }
  type("wait_reason", "gauge",
       "Current native upstream admission/authority wait; not a replication "
       "stall.");
  for (auto reason :
       {ReplicationFailure::kFullBusy, ReplicationFailure::kAuthority})
    metric("wait_reason",
           absl::StrCat("{reason=\"", ReplicationFailureName(reason), "\"}"),
           view.waiting_ && view.wait_reason_ == reason ? 1 : 0);
  type("fullsync_promotion_complete", "gauge",
       "Target-local durable root promotion during an active FULL; not "
       "population readiness or ONLINE. Omitted when not applicable.");
  if (view.full_promotion_complete_)
    metric("fullsync_promotion_complete", "",
           *view.full_promotion_complete_ ? 1 : 0);
  type("loading_no_progress_seconds", "gauge",
       "Time without effective population recovery progress while native "
       "recovery is required and not awaiting admission.");
  metric("loading_no_progress_seconds", "", view.loading_no_progress_seconds_);

  // Diagnostic snapshots remain renderable for tools/tests. HTTP collection
  // already supplies numeric aggregates and needs no per-row map allocation.
  std::map<std::tuple<ReplicationSide, unsigned, unsigned>,
           ReplicationFlowMetrics>
      rows;
  for (const auto& flow : view.flows_) {
    auto& row = rows[{flow.side_, flow.worker_, flow.flow_}];
    row.side_ = flow.side_;
    row.worker_ = flow.worker_;
    row.flow_ = flow.flow_;
    row.Add(flow);
  }
  type("native_flow_connections", "gauge",
       "Active native connections per flow; source aggregates peers.");
  type("comparable_flows", "gauge",
       "Connections with coherent ONLINE cursors in one current history/flow; "
       "FULL is not applicable.");
  type("pending_events", "gauge",
       "Target receive-to-apply distance or maximum comparable source "
       "publish-to-ACK event distance; not bytes.");
  type("pending_work", "gauge",
       "Connections with known work at the named step.");
  type("no_progress_seconds", "gauge",
       "Longest sampled no-progress interval among connections with known work "
       "at the named step.");
  type("cursor_next_lsn", "gauge",
       "Native next-LSN within one current history/flow; unavailable cursors "
       "omitted. Never use cross-node arithmetic or rate.");
  type("fullsync_phase", "gauge",
       "Active native FULL flows in a locally observed phase; source "
       "aggregates peers.");
  type("fullsync_phase_seconds", "gauge",
       "Maximum elapsed time in the current locally observed FULL phase.");
  type("fullsync_elapsed_seconds", "gauge",
       "Maximum current native FULL attempt duration; not a timeout or "
       "percentage.");
  auto emit_flow = [&](const ReplicationFlowMetrics& row) {
    const auto prefix =
        absl::StrCat("{side=\"", ReplicationSideName(row.side_), "\",worker=\"",
                     row.worker_, "\",flow=\"", row.flow_, "\"");
    metric("native_flow_connections", prefix + "}", row.connected_);
    metric("comparable_flows", prefix + "}", row.comparable_);
    if (row.comparable_ != 0)
      metric("pending_events", prefix + "}", row.pending_events_);
    for (unsigned step = 0; step != 4; ++step) {
      if (step != 3 && row.comparable_ == 0) continue;
      const auto labels = absl::StrCat(prefix, ",step=\"",
                                       kReplicationProgressSteps[step], "\"}");
      metric("pending_work", labels, row.pending_[step]);
      metric("no_progress_seconds", labels, row.ages_[step]);
    }
    for (const auto& [name, cursor] :
         std::array<std::pair<std::string_view, std::optional<std::uint64_t>>,
                    4>{{{"published", row.published_},
                        {"received", row.received_},
                        {"applied", row.applied_},
                        {"ack_sent", row.ack_}}})
      if (cursor)
        metric("cursor_next_lsn",
               absl::StrCat(prefix, ",cursor=\"", name, "\"}"), *cursor);
    for (unsigned phase = 0; phase != row.phases_.size(); ++phase) {
      const auto labels = absl::StrCat(
          prefix, ",phase=\"",
          FullSyncPhaseName(static_cast<FullSyncPhase>(phase)), "\"}");
      metric("fullsync_phase", labels, row.phases_[phase]);
      if (row.phases_[phase] != 0)
        metric("fullsync_phase_seconds", labels, row.phase_seconds_[phase]);
    }
    if (std::any_of(row.phases_.begin(), row.phases_.end(),
                    [](unsigned count) { return count != 0; }))
      metric("fullsync_elapsed_seconds", prefix + "}", row.full_seconds_);
  };
  for (const auto& row : view.metric_flows_) emit_flow(row);
  for (const auto& [key, row] : rows) emit_flow(row);
}
}  // namespace lavik
