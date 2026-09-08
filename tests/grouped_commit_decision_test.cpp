#include <array>

#include "gtest/gtest.h"
#include "keylane/storage/detail/grouped_commit.h"
#include "keylane/storage/engine.h"

namespace keylane::storage {
namespace {

TEST(GroupedCommitDecisionTest, PrePublicationCheckDoesNotPromiseDurability) {
  std::array<TxShardWrites, 2> writes;
  EXPECT_TRUE(StorageEngine::ValidateTxCommit(writes).ok());
  writes[1].grouped_decision_ = std::make_shared<GroupedCommitDecision>(17);
  EXPECT_TRUE(StorageEngine::ValidateTxCommit(writes).ok());
  EXPECT_EQ(writes[1].grouped_decision_->state_.load(),
            GroupedCommitDecision::State::kPending);
}

TEST(GroupedCommitDecisionTest, OneFailedParticipantRejectsTheWholeCommit) {
  std::array<TxShardWrites, 3> writes;
  writes[0].grouped_decision_ = std::make_shared<GroupedCommitDecision>(17);
  writes[2].grouped_decision_ = std::make_shared<GroupedCommitDecision>(17);
  writes[2].grouped_decision_->FailPending();
  EXPECT_TRUE(
      absl::IsFailedPrecondition(StorageEngine::ValidateTxCommit(writes)));
}

TEST(GroupedCommitDecisionTest, DurableDecisionCannotBeReversedByLateFailure) {
  std::array<TxShardWrites, 1> writes;
  writes[0].grouped_decision_ = std::make_shared<GroupedCommitDecision>(17);
  writes[0].grouped_decision_->state_.store(
      GroupedCommitDecision::State::kDurable);
  writes[0].grouped_decision_->FailPending();
  EXPECT_TRUE(StorageEngine::ValidateTxCommit(writes).ok());
  EXPECT_EQ(writes[0].grouped_decision_->state_.load(),
            GroupedCommitDecision::State::kDurable);
}

}  // namespace
}  // namespace keylane::storage
