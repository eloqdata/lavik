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

#include "full_sync_handoff.h"

#include "gtest/gtest.h"

namespace lavik::detail {
namespace {
TEST(FullSyncHandoffProgressTest,
     OutOfOrderAckDoesNotHideUnsentOrPendingPartitions) {
  FullSyncHandoffProgress progress(5, 0, 2);
  EXPECT_EQ(progress.unfinished(), 3);
  ASSERT_TRUE(progress.Begin(0, 4).ok());
  ASSERT_TRUE(progress.Begin(2, 8).ok());
  EXPECT_EQ(progress.inflight(), 2);
  ASSERT_TRUE(progress.Acknowledge(2, 8).value());
  EXPECT_EQ(progress.unfinished(), 2);
  ASSERT_TRUE(progress.Acknowledge(0, 4).value());
  EXPECT_EQ(progress.inflight(), 0);
  EXPECT_EQ(progress.unfinished(), 1);
  ASSERT_TRUE(progress.Begin(4, 11).ok());
  ASSERT_TRUE(progress.Acknowledge(4, 11).value());
  EXPECT_EQ(progress.unfinished(), 0);
}

TEST(FullSyncHandoffProgressTest,
     RejectsDuplicateSendAndAckWithoutAdvancingCounts) {
  FullSyncHandoffProgress progress(3, 1, 2);
  EXPECT_FALSE(progress.Begin(0, 1).ok());
  EXPECT_FALSE(progress.Begin(3, 1).ok());
  EXPECT_FALSE(progress.Begin(1, 0).ok());
  EXPECT_FALSE(progress.Acknowledge(1, 1).value());
  ASSERT_TRUE(progress.Begin(1, 2).ok());
  EXPECT_FALSE(progress.Begin(1, 3).ok());
  EXPECT_FALSE(progress.Acknowledge(1, 3).value());
  EXPECT_EQ(progress.unfinished(), 1);
  ASSERT_TRUE(progress.Acknowledge(1, 2).value());
  EXPECT_FALSE(progress.Acknowledge(1, 2).ok());
  EXPECT_FALSE(progress.Begin(1, 4).ok());
  EXPECT_EQ(progress.unfinished(), 0);
  EXPECT_EQ(progress.inflight(), 0);
}

TEST(FullSyncHandoffProgressTest,
     SessionReplacementAndOtherFlowsHaveIndependentProgress) {
  FullSyncHandoffProgress old_session(4, 0, 2);
  FullSyncHandoffProgress replacement(4, 0, 2);
  FullSyncHandoffProgress peer_flow(4, 1, 2);
  ASSERT_TRUE(old_session.Begin(0, 1).ok());
  ASSERT_TRUE(old_session.Acknowledge(0, 1).value());
  EXPECT_EQ(replacement.unfinished(), 2);
  EXPECT_FALSE(replacement.Acknowledge(0, 1).value());
  EXPECT_FALSE(peer_flow.Acknowledge(0, 1).value());
  EXPECT_EQ(peer_flow.unfinished(), 2);
  FullSyncHandoffProgress empty_flow(1, 1, 2);
  EXPECT_EQ(empty_flow.unfinished(), 0);
}
}  // namespace
}  // namespace lavik::detail
