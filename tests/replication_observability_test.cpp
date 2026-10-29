/* Copyright (C) 2026 EloqData Inc. SPDX-License-Identifier: Apache-2.0 */
#include "lavik/replication_observability.h"

#include "gtest/gtest.h"
#include "observability.h"

namespace lavik {
namespace {
constexpr std::uint64_t Seconds(unsigned n) { return n * 1'000'000'000ULL; }

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
