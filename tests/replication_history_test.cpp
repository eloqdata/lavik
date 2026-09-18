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
#include "log_block.h"

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

TEST(ReplicationHistoryTest, RolloverWithdrawsEveryParticipantOfAnEffect) {
  ReplicationHistory history(256);
  ASSERT_TRUE(history.Reset("parent", 2).ok());
  ASSERT_TRUE(history.TryRetain(
      "parent", {{0, 7, std::string(64, 'a')}, {1, 9, "marker"}}));
  auto copy = history.Read("parent", 1, 9, 0, 64);
  ASSERT_TRUE(copy.ok());
  // The next complete effect fits the quota, but not the remaining block.
  ASSERT_TRUE(history.TryRetain("parent", {{0, 8, std::string(128, 'b')}}));
  EXPECT_FALSE(history.DescribeEffect("parent", 0, 7).ok());
  EXPECT_FALSE(history.Read("parent", 1, 9, 0, 64).ok());
  EXPECT_EQ(copy->bytes_, "marker");
  EXPECT_EQ(history.Coverage("parent")[0],
            (std::vector<NativeHistoryRange>{{8, 9}}));
  EXPECT_TRUE(history.Coverage("parent")[1].empty());
  EXPECT_EQ(history.Read("parent", 0, 8, 127, 64)->bytes_, "b");
}

TEST(ReplicationHistoryTest, ExportIndexHandlesAppendsGapsAndLineageReset) {
  ReplicationHistory history(32768);
  ASSERT_TRUE(history.Reset("parent", 2).ok());
  ASSERT_TRUE(history.TryRetain("parent", {{0, 1, "one"}}));
  ASSERT_TRUE(history.Read("parent", 0, 1, 0, 64).ok());
  for (std::uint64_t lsn = 2; lsn < 160; ++lsn) {
    ASSERT_TRUE(history.TryRetain("parent", {{0, lsn * 2, std::to_string(lsn)},
                                             {1, lsn * 3, "participant"}}));
    auto manifest = history.DescribeEffect("parent", 1, lsn * 3);
    ASSERT_TRUE(manifest.ok()) << manifest.status();
    ASSERT_EQ(manifest->size(), 2);
    EXPECT_EQ((*manifest)[0].lsn_, lsn * 2);
    auto copy = history.Read("parent", 0, lsn * 2, 0, 64);
    ASSERT_TRUE(copy.ok());
    EXPECT_EQ(copy->bytes_, std::to_string(lsn));
    EXPECT_FALSE(history.Read("parent", 0, lsn * 2 - 1, 0, 64).ok());
  }
  EXPECT_EQ(history.Read("parent", 0, 1, 0, 64)->bytes_, "one");
  ASSERT_TRUE(history.Reset("child", 2).ok());
  ASSERT_TRUE(history.TryRetain("child", {{0, 1, "child-one"}}));
  EXPECT_FALSE(history.Read("parent", 0, 1, 0, 64).ok());
  EXPECT_EQ(history.Read("child", 0, 1, 0, 64)->bytes_, "child-one");
}

TEST(ReplicationHistoryTest, LargeEffectRemainsWholeAcrossNormalBlockSize) {
  constexpr auto kMiB = 1024 * 1024;
  ReplicationHistory history(24 * kMiB);
  ASSERT_TRUE(history.Reset("parent", 2).ok());
  ASSERT_TRUE(history.TryRetain(
      "parent", {{0, 7, std::string(8 * kMiB, 'x')}, {1, 9, "marker"}}));
  auto tail = history.Read("parent", 0, 7, 8 * kMiB - 2, 64);
  ASSERT_TRUE(tail.ok());
  EXPECT_EQ(tail->bytes_, "xx");
  EXPECT_EQ(tail->total_bytes_, 8 * kMiB);
  auto manifest = history.DescribeEffect("parent", 1, 9);
  ASSERT_TRUE(manifest.ok());
  ASSERT_EQ(manifest->size(), 2);
  auto primary = history.ChargePrimary(24 * kMiB);
  EXPECT_FALSE(history.Read("parent", 1, 9, 0, 64).ok());
  EXPECT_EQ(history.secondary_bytes(), 0);
}

TEST(ReplicationLogBlockTest, ReuseResetsSparseCursorsWithoutReallocating) {
  auto block = detail::ReplicationLogBlock::Allocate(16384);
  ASSERT_TRUE(block.ok());
  auto* allocation = block->bytes_.get();
  for (std::uint64_t lsn = 1; lsn <= 130; ++lsn) {
    auto output = block->AppendBuffer(64);
    output.front() = std::byte(lsn);
    block->CommitAppend(lsn, 0, output.size());
  }
  EXPECT_EQ(block->FindOffset(65), 64 * 64);
  EXPECT_EQ(block->FindOffset(130), 128 * 64);
  block->Reset();
  EXPECT_EQ(block->bytes_.get(), allocation);
  auto output = block->AppendBuffer(64);
  output.front() = std::byte{42};
  block->CommitAppend(900, 0, output.size());
  EXPECT_EQ(block->FindOffset(900), 0);
  EXPECT_EQ(block->first_lsn_, 900);
  EXPECT_EQ(block->frame_count_, 1);
}

}  // namespace
}  // namespace lavik
