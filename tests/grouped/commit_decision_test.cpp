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

#include <array>

#include "gtest/gtest.h"
#include "lavik/storage/detail/grouped/commit.h"
#include "lavik/storage/engine.h"

namespace lavik::storage {
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
}  // namespace lavik::storage
