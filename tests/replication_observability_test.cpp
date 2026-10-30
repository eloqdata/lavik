/* Copyright (C) 2026 EloqData Inc. SPDX-License-Identifier: Apache-2.0 */
#include "lavik/replication_observability.h"

#include <algorithm>
#include <sstream>

#include "gtest/gtest.h"
#include "observability.h"

namespace lavik {
namespace {
constexpr std::uint64_t Seconds(unsigned n) { return n * 1'000'000'000ULL; }

std::vector<std::string> MetricLines(
    const ReplicationObservabilitySnapshot& snapshot) {
  std::string text;
  AppendReplicationProgressMetrics(snapshot, &text);
  std::istringstream input(text);
  std::vector<std::string> lines;
  for (std::string line; std::getline(input, line);)
    lines.push_back(std::move(line));
  // Logical-flow collection order can differ from the diagnostic renderer's
  // side/worker/flow order when source and target worker counts differ.
  std::sort(lines.begin(), lines.end());
  return lines;
}

TEST(ReplicationObservability, IdleTimeIsNotPendingWorkAge) {
  detail::ReplicationProgressClock clock;
  clock.Sample(false, 10, Seconds(1));
  EXPECT_EQ(clock.Age(false, Seconds(600)), 0);
  clock.Sample(true, 10, Seconds(600));
  EXPECT_EQ(clock.Age(true, Seconds(601)), 1);
  clock.Sample(true, 10, Seconds(900));
  EXPECT_EQ(clock.Age(true, Seconds(900)), 300);
  clock.Sample(true, 11, Seconds(901));
  EXPECT_EQ(clock.Age(true, Seconds(901)), 0);
  clock.Sample(false, 11, Seconds(902));
  EXPECT_EQ(clock.Age(false, Seconds(1800)), 0);
  clock.Sample(true, 11, Seconds(1800));
  EXPECT_EQ(clock.Age(true, Seconds(1800)), 0);
}

TEST(ReplicationObservability, SlowProgressAndNewSessionsNeverInheritAStall) {
  detail::ReplicationProgressClock clock;
  clock.Sample(true, 100, Seconds(1));
  clock.Sample(true, 101, Seconds(299));
  EXPECT_EQ(clock.Age(true, Seconds(300)), 1);
  // A replacement owns a fresh clock even when it has the same cursor value.
  clock = {};
  clock.Sample(true, 101, Seconds(600));
  EXPECT_EQ(clock.Age(true, Seconds(600)), 0);
}

TEST(ReplicationObservability,
     UnavailableSnapshotOmitsActiveValuesButKeepsCounters) {
  ReplicationObservabilitySnapshot snapshot;
  snapshot.full_totals_[1].results_[1] = 3;
  snapshot.flows_.push_back(
      {.connected_ = true, .comparable_ = true, .published_next_lsn_ = 99});
  std::string metrics;
  AppendReplicationProgressMetrics(snapshot, &metrics);
  EXPECT_NE(metrics.find("observation_available 0\n"), std::string::npos);
  EXPECT_EQ(metrics.find("cursor_next_lsn"), std::string::npos);
  EXPECT_EQ(metrics.find("pending_events"), std::string::npos);
  EXPECT_NE(
      metrics.find(
          "fullsync_results_total{side=\"target\",result=\"failure\"} 3\n"),
      std::string::npos);
}

TEST(ReplicationObservability, FullWithoutOnlineCoordinatesIsNotCaughtUp) {
  ReplicationObservabilitySnapshot snapshot;
  snapshot.available_ = true;
  snapshot.flows_.push_back(
      {.connected_ = true, .full_ = true, .phase_ = FullSyncPhase::kTransfer});
  std::string metrics, info;
  AppendReplicationProgressMetrics(snapshot, &metrics);
  AppendReplicationProgressInfo(snapshot, &info);
  EXPECT_NE(
      metrics.find(
          "comparable_flows{side=\"target\",worker=\"0\",flow=\"0\"} 0\n"),
      std::string::npos);
  EXPECT_EQ(metrics.find("pending_events{side="), std::string::npos);
  EXPECT_EQ(metrics.find("cursor_next_lsn{side="), std::string::npos);
  EXPECT_NE(info.find("pending_events=unavailable"), std::string::npos);
}

TEST(ReplicationObservability, SourceAggregatesDistancesWithoutPeerLabels) {
  ReplicationObservabilitySnapshot snapshot;
  snapshot.available_ = true;
  auto& first = snapshot.flows_.emplace_back();
  first.side_ = ReplicationSide::kSource;
  first.connected_ = first.comparable_ = true;
  first.session_id_ = 123456;
  first.history_id_ = "private-history";
  first.peer_id_ = "private-peer-a";
  first.pending_events_ = 7;
  first.pending_[2] = true;
  first.no_progress_seconds_[2] = 10;
  auto second = first;
  second.peer_id_ = "private-peer-b";
  second.pending_events_ = 11;
  second.no_progress_seconds_[2] = 20;
  snapshot.flows_.push_back(second);
  std::string metrics;
  AppendReplicationProgressMetrics(snapshot, &metrics);
  EXPECT_EQ(metrics.find("private-"), std::string::npos);
  EXPECT_EQ(metrics.find("123456"), std::string::npos);
  EXPECT_NE(metrics.find(
                "pending_events{side=\"source\",worker=\"0\",flow=\"0\"} 11\n"),
            std::string::npos);
  EXPECT_NE(metrics.find("no_progress_seconds{side=\"source\",worker=\"0\","
                         "flow=\"0\",step=\"ack\"} 20\n"),
            std::string::npos);
  auto compact = snapshot;
  compact.flows_.clear();
  auto& row = compact.metric_flows_.emplace_back();
  row.side_ = ReplicationSide::kSource;
  for (const auto& flow : snapshot.flows_) row.Add(flow);
  EXPECT_EQ(MetricLines(compact), MetricLines(snapshot));
}

TEST(ReplicationObservability, SparseTotalsPreserveZeroSeriesAndRetiredWork) {
  ReplicationObservabilitySnapshot dense, sparse;
  sparse.worker_count_ = 3;
  for (auto side : {ReplicationSide::kSource, ReplicationSide::kTarget})
    for (unsigned worker = 0; worker != 3; ++worker)
      dense.totals_.push_back({.side_ = side, .worker_ = worker});
  // Before any relationship exists, zero counters still have every label set.
  EXPECT_EQ(MetricLines(sparse), MetricLines(dense));
  dense.totals_[1].bytes_ = 123;
  dense.totals_[3].applied_events_ = 7;
  dense.totals_[5].partitions_ = 2;
  sparse.totals_ = {dense.totals_[1], dense.totals_[3], dense.totals_[5]};
  // Neither a retired session nor an unavailable active snapshot discards
  // historical work. Omitted workers still export zeros, not missing data.
  for (bool available : {false, true}) {
    dense.available_ = sparse.available_ = available;
    EXPECT_EQ(MetricLines(sparse), MetricLines(dense));
    std::string metrics;
    AppendReplicationProgressMetrics(sparse, &metrics);
    EXPECT_NE(
        metrics.find("transfer_bytes_total{side=\"source\",worker=\"0\"} 0\n"),
        std::string::npos);
    EXPECT_NE(metrics.find(
                  "transfer_bytes_total{side=\"source\",worker=\"1\"} 123\n"),
              std::string::npos);
    EXPECT_NE(
        metrics.find("applied_events_total{side=\"target\",worker=\"0\"} 7\n"),
        std::string::npos);
    EXPECT_NE(
        metrics.find(
            "fullsync_partitions_total{side=\"target\",worker=\"2\"} 2\n"),
        std::string::npos);
  }
}

TEST(ReplicationObservability, CompactMetricsPreserveFullAndOnlineSemantics) {
  ReplicationObservabilitySnapshot detailed, compact;
  detailed.available_ = compact.available_ = true;
  ReplicationFlowObservation transfer{.side_ = ReplicationSide::kSource,
                                      .connected_ = true,
                                      .full_ = true,
                                      .phase_ = FullSyncPhase::kTransfer,
                                      .phase_seconds_ = 10,
                                      .full_seconds_ = 12};
  transfer.pending_[3] = true;
  transfer.no_progress_seconds_[3] = 4;
  auto promote = transfer;
  promote.phase_ = FullSyncPhase::kPromote;
  promote.phase_seconds_ = 2;
  promote.full_seconds_ = 20;
  promote.no_progress_seconds_[3] = 1;
  detailed.flows_ = {transfer, promote};
  auto& source = compact.metric_flows_.emplace_back();
  source.side_ = ReplicationSide::kSource;
  source.Add(transfer);
  source.Add(promote);
  // Three source flows distributed over two target owners, with FULL on one
  // flow and applicable ONLINE coordinates only on the others.
  for (unsigned flow = 0; flow != 3; ++flow) {
    ReplicationFlowObservation target{.worker_ = flow % 2,
                                      .flow_ = flow,
                                      .connected_ = true,
                                      .full_ = flow == 1,
                                      .comparable_ = flow != 1};
    if (flow != 1) {
      target.received_next_lsn_ = 30 + flow;
      target.applied_next_lsn_ = 29 + flow;
      target.ack_next_lsn_ = 28 + flow;
      target.pending_events_ = 1;
    }
    detailed.flows_.push_back(target);
    auto& row = compact.metric_flows_.emplace_back();
    row.worker_ = target.worker_;
    row.flow_ = flow;
    row.Add(target);
  }
  EXPECT_EQ(MetricLines(compact), MetricLines(detailed));
  std::string metrics;
  AppendReplicationProgressMetrics(compact, &metrics);
  EXPECT_NE(
      metrics.find("fullsync_phase{side=\"source\",worker=\"0\",flow=\"0\","
                   "phase=\"transfer\"} 1\n"),
      std::string::npos);
  EXPECT_NE(
      metrics.find("fullsync_phase{side=\"source\",worker=\"0\",flow=\"0\","
                   "phase=\"promote\"} 1\n"),
      std::string::npos);
  EXPECT_NE(
      metrics.find("fullsync_elapsed_seconds{side=\"source\",worker=\"0\","
                   "flow=\"0\"} 20\n"),
      std::string::npos);
  EXPECT_NE(metrics.find("no_progress_seconds{side=\"source\",worker=\"0\","
                         "flow=\"0\",step=\"full\"} 4\n"),
            std::string::npos);
  EXPECT_EQ(
      metrics.find("cursor_next_lsn{side=\"target\",worker=\"1\",flow=\"1\""),
      std::string::npos);
  EXPECT_NE(
      metrics.find("cursor_next_lsn{side=\"target\",worker=\"0\",flow=\"2\","
                   "cursor=\"ack_sent\"} 30\n"),
      std::string::npos);
  compact.available_ = detailed.available_ = false;
  EXPECT_EQ(MetricLines(compact), MetricLines(detailed));
  metrics.clear();
  AppendReplicationProgressMetrics(compact, &metrics);
  EXPECT_EQ(metrics.find("cursor_next_lsn"), std::string::npos);
  EXPECT_EQ(metrics.find("fullsync_phase"), std::string::npos);
}

TEST(ReplicationObservability, ErrorLabelsAreBoundedAndInfoCannotInjectFields) {
  EXPECT_EQ(detail::ClassifyReplicationFailure(absl::DataLossError("CRC")),
            ReplicationFailure::kProtocol);
  EXPECT_EQ(detail::ClassifyReplicationFailure(absl::CancelledError("role")),
            ReplicationFailure::kCancelled);
  ReplicationObservabilitySnapshot snapshot;
  snapshot.last_full_ =
      FullSyncObservationResult{.detail_ = "failed\r\ninjected:1"};
  std::string info, metrics;
  AppendReplicationProgressInfo(snapshot, &info);
  AppendReplicationProgressMetrics(snapshot, &metrics);
  EXPECT_NE(info.find("last_full_detail:failed\\r\\ninjected:1\r\n"),
            std::string::npos);
  EXPECT_EQ(metrics.find("injected"), std::string::npos);
}
}  // namespace
}  // namespace lavik
