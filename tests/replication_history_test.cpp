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

#include "lavik/replication_history.h"

#include "gtest/gtest.h"

namespace lavik {
namespace {

TEST(ReplicationHistoryTest,
     EffectManifestIsCompleteAndDoesNotPinEvictedPayload) {
  ReplicationHistory history(4096);
  ASSERT_TRUE(history.Reset("parent", 2).ok());
  ASSERT_TRUE(
      history.TryRetain("parent", {{0, 7, "payload"}, {1, 9, "marker"}}));
  auto manifest = history.DescribeEffect("parent", 1, 9);
  ASSERT_TRUE(manifest.ok()) << manifest.status();
  EXPECT_EQ(*manifest,
            (std::vector<NativeHistoryRecordInfo>{{0, 7, 7}, {1, 9, 6}}));
  history.SetCapacity(0);
  EXPECT_FALSE(history.Read("parent", 0, 7, 0, 1024).ok());
  EXPECT_FALSE(history.DescribeEffect("parent", 1, 9).ok());
}

TEST(ReplicationHistoryTest, PrimaryPublicationEvictsWholeSecondaryEffects) {
  ReplicationHistory history(4096);
  ASSERT_TRUE(history.Reset("parent", 2).ok());
  ASSERT_TRUE(
      history.TryRetain("parent", {{0, 10, "payload"}, {1, 20, "marker"}}));
  ASSERT_TRUE(history.TryRetain("parent", {{0, 11, "next"}}));
  auto coverage = history.Coverage("parent");
  ASSERT_EQ(coverage.size(), 2);
  ASSERT_EQ(coverage[0].size(), 1);
  EXPECT_EQ(coverage[0][0], (NativeHistoryRange{10, 12}));
  ASSERT_EQ(coverage[1].size(), 1);
  EXPECT_EQ(coverage[1][0], (NativeHistoryRange{20, 21}));
  auto primary = history.ChargePrimary(4096);
  EXPECT_EQ(history.primary_bytes(), 4096);
  EXPECT_EQ(history.secondary_bytes(), 0);
  EXPECT_TRUE(history.Coverage("parent")[0].empty());
  EXPECT_TRUE(history.Coverage("parent")[1].empty());
  EXPECT_FALSE(history.TryRetain("parent", {{0, 12, "no-space"}}));
  primary.Reset();
  EXPECT_EQ(history.primary_bytes(), 0);
  EXPECT_TRUE(history.TryRetain("parent", {{0, 12, "after-release"}}));
}

TEST(ReplicationHistoryTest, ExportCopiesBoundedChunksWithoutPinningRetention) {
  ReplicationHistory history(4096);
  ASSERT_TRUE(history.Reset("parent", 1).ok());
  ASSERT_TRUE(history.TryRetain("parent", {{0, 10, "abcdefgh"}}));
  auto chunk = history.Read("parent", 0, 10, 2, 3);
  ASSERT_TRUE(chunk.ok()) << chunk.status();
  EXPECT_EQ(chunk->total_bytes_, 8);
  EXPECT_EQ(chunk->bytes_, "cde");
  history.SetCapacity(0);
  EXPECT_FALSE(history.Read("parent", 0, 10, 5, 3).ok());
  EXPECT_EQ(chunk->bytes_, "cde");
  EXPECT_EQ(history.secondary_bytes(), 0);
}

TEST(ReplicationHistoryTest,
     GapsAndHistoryChangesNeverAdvertiseInventedCoverage) {
  ReplicationHistory history(4096);
  ASSERT_TRUE(history.Reset("parent", 2).ok());
  ASSERT_TRUE(history.TryRetain("parent", {{0, 10, "a"}}));
  ASSERT_TRUE(history.TryRetain("parent", {{0, 12, "b"}}));
  const auto coverage = history.Coverage("parent");
  ASSERT_EQ(coverage[0].size(), 2);
  EXPECT_EQ(coverage[0][0], (NativeHistoryRange{10, 11}));
  EXPECT_EQ(coverage[0][1], (NativeHistoryRange{12, 13}));
  EXPECT_FALSE(
      history.TryRetain("parent", {{0, 12, "duplicate"}, {1, 30, "marker"}}));
  EXPECT_TRUE(history.Coverage("parent")[1].empty());
  auto primary = history.ChargePrimary(128);
  ASSERT_TRUE(history.Reset("child", 3).ok());
  EXPECT_EQ(history.primary_bytes(), 128);
  EXPECT_EQ(history.secondary_bytes(), 0);
  EXPECT_TRUE(history.Coverage("parent").empty());
  EXPECT_FALSE(history.TryRetain("parent", {{0, 13, "stale"}}));
  EXPECT_EQ(history.Coverage("child").size(), 3);
}

TEST(ReplicationHistoryTest,
     CapacityShrinkPreservesExistingPrimaryUntilRelease) {
  ReplicationHistory history(4096);
  ASSERT_TRUE(history.Reset("parent", 1).ok());
  auto primary = history.ChargePrimary(2048);
  ASSERT_TRUE(history.TryRetain("parent", {{0, 10, "tail"}}));
  history.SetCapacity(1024);
  EXPECT_EQ(history.primary_bytes(), 2048);
  EXPECT_EQ(history.secondary_bytes(), 0);
  EXPECT_FALSE(history.TryRetain("parent", {{0, 11, "new-tail"}}));
  primary.Reset();
  EXPECT_TRUE(history.TryRetain("parent", {{0, 11, "new-tail"}}));
}

}  // namespace
}  // namespace lavik
