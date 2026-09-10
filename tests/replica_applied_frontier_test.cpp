#include "replica_applied_frontier.h"

#include <cstdint>
#include <limits>
#include <span>
#include <vector>

#include "absl/status/status.h"
#include "gtest/gtest.h"

namespace keylane::detail {

class ReplicaAppliedFrontierTestPeer {
 public:
  static absl::Status BeginPublication(ReplicaAppliedFrontier& frontier,
                                       unsigned publisher) {
    return frontier.BeginPublication(publisher);
  }

  static void StoreNextLsn(ReplicaAppliedFrontier& frontier, unsigned flow,
                           std::uint64_t next_lsn) {
    frontier.StoreNextLsn(flow, next_lsn);
  }

  static void EndPublication(ReplicaAppliedFrontier& frontier,
                             unsigned publisher) {
    frontier.EndPublication(publisher);
  }

  static void SetPublisherSequence(ReplicaAppliedFrontier& frontier,
                                   unsigned publisher,
                                   std::uint64_t sequence) {
    frontier.publishers_[publisher].next_sequence_ = sequence;
    frontier.publishers_[publisher].published_.store(
        sequence, std::memory_order_relaxed);
  }
};

namespace {

TEST(ReplicaAppliedFrontierTest, ReconnectLayoutMismatchResetsEveryFlow) {
  const std::vector<std::uint64_t> old_layout{11, 22};
  auto reset = InitialAppliedNextLsnsForReconnect(
      /*source_flow_count=*/3, old_layout,
      /*exact_resume_context_matches=*/false);
  ASSERT_TRUE(reset.ok()) << reset.status();
  EXPECT_EQ(*reset, (std::vector<std::uint64_t>{1, 1, 1}));

  auto resumed = InitialAppliedNextLsnsForReconnect(
      /*source_flow_count=*/2, old_layout,
      /*exact_resume_context_matches=*/true);
  ASSERT_TRUE(resumed.ok()) << resumed.status();
  EXPECT_EQ(*resumed, old_layout);
}

TEST(ReplicaAppliedFrontierTest, AdvancesOnlyTheCompletedEvent) {
  ReplicaAppliedFrontier frontier(/*flow_count=*/3, /*publisher_count=*/2);
  ASSERT_EQ(frontier.TrySnapshot().value(),
            (std::vector<std::uint64_t>{1, 1, 1}));

  EXPECT_TRUE(frontier.AdvanceAfterApply(/*flow=*/1, /*applied_lsn=*/1).ok());
  EXPECT_TRUE(frontier.AdvanceAfterApply(/*flow=*/1, /*applied_lsn=*/1).ok());
  EXPECT_EQ(frontier.TrySnapshot().value(),
            (std::vector<std::uint64_t>{1, 2, 1}));

  EXPECT_EQ(frontier.AdvanceAfterApply(1, 3).code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(frontier.AdvanceAfterApply(1, 0).code(),
            absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(frontier.AdvanceAfterApply(
                1, std::numeric_limits<std::uint64_t>::max())
                .code(),
            absl::StatusCode::kOutOfRange);
  EXPECT_EQ(frontier.AdvanceAfterApply(3, 1).code(),
            absl::StatusCode::kInvalidArgument);
}

TEST(ReplicaAppliedFrontierTest, BatchValidationPrecedesPublication) {
  ReplicaAppliedFrontier frontier(/*flow_count=*/3, /*publisher_count=*/2);
  const std::vector<ReplicaAppliedFrontier::FlowApplied> duplicate{
      {.flow_id_ = 0, .applied_lsn_ = 1},
      {.flow_id_ = 0, .applied_lsn_ = 1},
  };
  EXPECT_EQ(frontier.AdvanceBatchAfterApply(0, duplicate).code(),
            absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(frontier.TrySnapshot().value(),
            (std::vector<std::uint64_t>{1, 1, 1}));

  const std::vector<ReplicaAppliedFrontier::FlowApplied> updates{
      {.flow_id_ = 0, .applied_lsn_ = 1},
      {.flow_id_ = 2, .applied_lsn_ = 1},
  };
  EXPECT_TRUE(frontier.AdvanceBatchAfterApply(1, updates).ok());
  EXPECT_EQ(frontier.TrySnapshot().value(),
            (std::vector<std::uint64_t>{2, 1, 2}));
}

TEST(ReplicaAppliedFrontierTest, SnapshotNeverAcceptsHalfPublishedBatch) {
  ReplicaAppliedFrontier frontier(/*flow_count=*/2, /*publisher_count=*/1);
  ASSERT_TRUE(
      ReplicaAppliedFrontierTestPeer::BeginPublication(frontier, 0).ok());
  ReplicaAppliedFrontierTestPeer::StoreNextLsn(frontier, 0, 2);

  const auto busy = frontier.TrySnapshot();
  EXPECT_EQ(busy.status().code(), absl::StatusCode::kUnavailable);

  ReplicaAppliedFrontierTestPeer::StoreNextLsn(frontier, 1, 2);
  ReplicaAppliedFrontierTestPeer::EndPublication(frontier, 0);
  EXPECT_EQ(frontier.TrySnapshot().value(),
            (std::vector<std::uint64_t>{2, 2}));
}

TEST(ReplicaAppliedFrontierTest, LifecycleInstallPublishesOneVector) {
  ReplicaAppliedFrontier frontier(/*flow_count=*/3, /*publisher_count=*/2);
  ASSERT_TRUE(frontier.AdvanceAfterApply(0, 1).ok());
  ASSERT_TRUE(frontier.AdvanceAfterApply(1, 1).ok());

  const std::vector<std::uint64_t> installed{7, 8, 9};
  EXPECT_TRUE(frontier.InstallNextLsns(installed).ok());
  EXPECT_EQ(frontier.TrySnapshot().value(), installed);

  EXPECT_EQ(frontier.InstallNextLsns(std::vector<std::uint64_t>{1, 2}).code(),
            absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(frontier.InstallNextLsns(std::vector<std::uint64_t>{1, 0, 2})
                .code(),
            absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(frontier.TrySnapshot().value(), installed);
}

TEST(ReplicaAppliedFrontierTest, SingleAttemptOmitsBusyBatch) {
  ReplicaAppliedFrontier frontier(/*flow_count=*/2, /*publisher_count=*/2);
  ASSERT_TRUE(
      ReplicaAppliedFrontierTestPeer::BeginPublication(frontier, 1).ok());
  ReplicaAppliedFrontierTestPeer::StoreNextLsn(frontier, 0, 2);
  EXPECT_EQ(frontier.TrySnapshotOnce().status().code(),
            absl::StatusCode::kUnavailable);

  ReplicaAppliedFrontierTestPeer::StoreNextLsn(frontier, 1, 2);
  ReplicaAppliedFrontierTestPeer::EndPublication(frontier, 1);
  EXPECT_EQ(frontier.TrySnapshotOnce().value(),
            (std::vector<std::uint64_t>{2, 2}));
}

TEST(ReplicaAppliedFrontierTest,
     ScratchReuseKeepsLayoutsAndResultsIndependent) {
  ReplicaAppliedFrontier small(/*flow_count=*/2, /*publisher_count=*/1);
  ASSERT_TRUE(small.InstallNextLsns(std::vector<std::uint64_t>{100, 200}).ok());
  const auto retained = small.TrySnapshotOnce();
  ASSERT_TRUE(retained.ok()) << retained.status();

  ReplicaAppliedFrontier large(/*flow_count=*/3, /*publisher_count=*/4);
  EXPECT_EQ(large.TrySnapshotOnce().value(),
            (std::vector<std::uint64_t>{1, 1, 1}));
  ASSERT_TRUE(ReplicaAppliedFrontierTestPeer::BeginPublication(large, 3).ok());
  ReplicaAppliedFrontierTestPeer::StoreNextLsn(large, 2, 2);
  EXPECT_EQ(large.TrySnapshotOnce().status().code(),
            absl::StatusCode::kUnavailable);

  // A busy publisher from another layout must not contaminate the next
  // sample, or overwrite an earlier vector still owned by a queued message.
  ASSERT_TRUE(small.AdvanceAfterApply(/*flow=*/0, /*applied_lsn=*/100).ok());
  EXPECT_EQ(small.TrySnapshotOnce().value(),
            (std::vector<std::uint64_t>{101, 200}));
  EXPECT_EQ(*retained, (std::vector<std::uint64_t>{100, 200}));

  ReplicaAppliedFrontierTestPeer::EndPublication(large, 3);
  EXPECT_EQ(large.TrySnapshotOnce().value(),
            (std::vector<std::uint64_t>{1, 1, 2}));
}

TEST(ReplicaAppliedFrontierTest, DiagnosticOffsetRemainsAvailableDuringBatch) {
  ReplicaAppliedFrontier frontier(/*flow_count=*/2, /*publisher_count=*/1);
  ASSERT_TRUE(
      frontier.InstallNextLsns(std::vector<std::uint64_t>{100, 200}).ok());
  EXPECT_EQ(frontier.ApproximateTotalNextLsn(), 300U);

  // Model a publisher preempted halfway through publishing a completed
  // transaction. A proof must fail closed, but diagnostics still have progress.
  ASSERT_TRUE(
      ReplicaAppliedFrontierTestPeer::BeginPublication(frontier, 0).ok());
  ReplicaAppliedFrontierTestPeer::StoreNextLsn(frontier, 0, 101);
  EXPECT_EQ(frontier.TrySnapshot().status().code(),
            absl::StatusCode::kUnavailable);
  EXPECT_EQ(frontier.ApproximateTotalNextLsn(), 301U);

  ReplicaAppliedFrontierTestPeer::StoreNextLsn(frontier, 1, 201);
  ReplicaAppliedFrontierTestPeer::EndPublication(frontier, 0);
  EXPECT_EQ(frontier.TrySnapshot().value(),
            (std::vector<std::uint64_t>{101, 201}));
  EXPECT_EQ(frontier.ApproximateTotalNextLsn(), 302U);
}

TEST(ReplicaAppliedFrontierTest, DiagnosticOffsetSaturatesWithoutWrapping) {
  ReplicaAppliedFrontier frontier(/*flow_count=*/2, /*publisher_count=*/1);
  constexpr std::uint64_t maximum = std::numeric_limits<std::uint64_t>::max();
  ASSERT_TRUE(
      frontier.InstallNextLsns(std::vector<std::uint64_t>{maximum - 1, 1})
          .ok());
  EXPECT_EQ(frontier.ApproximateTotalNextLsn(), maximum);

  ASSERT_TRUE(frontier.AdvanceAfterApply(/*flow=*/1, /*applied_lsn=*/1).ok());
  EXPECT_EQ(frontier.ApproximateTotalNextLsn(), maximum);
}

TEST(ReplicaAppliedFrontierTest, DiagnosticOffsetReflectsResetAndReplacement) {
  ReplicaAppliedFrontier frontier(/*flow_count=*/2, /*publisher_count=*/1);
  ASSERT_TRUE(
      frontier.InstallNextLsns(std::vector<std::uint64_t>{100, 200}).ok());
  EXPECT_EQ(frontier.ApproximateTotalNextLsn(), 300U);

  ASSERT_TRUE(frontier.InstallNextLsns(std::vector<std::uint64_t>{1, 1}).ok());
  EXPECT_EQ(frontier.ApproximateTotalNextLsn(), 2U);

  ReplicaAppliedFrontier replacement(/*flow_count=*/3, /*publisher_count=*/1);
  EXPECT_EQ(replacement.ApproximateTotalNextLsn(), 3U);
}

TEST(ReplicaAppliedFrontierTest, PublicationSequenceNeverWraps) {
  ReplicaAppliedFrontier frontier(/*flow_count=*/2, /*publisher_count=*/1);
  ReplicaAppliedFrontierTestPeer::SetPublisherSequence(
      frontier, 0, std::numeric_limits<std::uint64_t>::max() - 1);
  const std::vector<ReplicaAppliedFrontier::FlowApplied> updates{
      {.flow_id_ = 0, .applied_lsn_ = 1},
      {.flow_id_ = 1, .applied_lsn_ = 1},
  };
  EXPECT_EQ(frontier.AdvanceBatchAfterApply(0, updates).code(),
            absl::StatusCode::kResourceExhausted);
  EXPECT_TRUE(frontier.poisoned());
  EXPECT_EQ(frontier.TrySnapshot().status().code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(frontier.TrySnapshotOnce().status().code(),
            absl::StatusCode::kFailedPrecondition);
}

}  // namespace
}  // namespace keylane::detail
