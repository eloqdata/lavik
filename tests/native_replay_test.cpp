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

#include "native_replay.h"

#include <array>
#include <limits>
#include <thread>

#include "gtest/gtest.h"
#include "native_recovery.h"

namespace lavik::detail {
namespace {
TEST(NativeRecoveryTest,
     EnvelopeAndAppliedRemainDistinctWhenDonorCoverageDisappears) {
  NativeRecoveryAdvertisement first{
      "donor-a", {15, 11}, {{{10, 15}}, {{10, 11}}}};
  NativeRecoveryAdvertisement second{
      "donor-b", {11, 16}, {{{10, 11}}, {{10, 16}}}};
  const std::vector<std::uint64_t> applied{10, 10};
  auto target = RecoveryTarget(applied, std::vector{first, second});
  ASSERT_TRUE(target.ok());
  EXPECT_EQ(*target, (std::vector<std::uint64_t>{15, 16}));
  auto wire = EncodeRecoveryAdvertisement(first);
  ASSERT_TRUE(wire.ok());
  auto report = DecodeRecoveryAdvertisement(*wire);
  ASSERT_TRUE(report.ok());
  EXPECT_EQ(report->applied_, first.applied_);
  EXPECT_TRUE(RecoveryCovers(*report, 0, 14));
  report->coverage_[0].clear();
  EXPECT_FALSE(RecoveryCovers(*report, 0, 14));
  EXPECT_EQ(*target, (std::vector<std::uint64_t>{15, 16}));
  EXPECT_EQ(applied, (std::vector<std::uint64_t>{10, 10}));
  EXPECT_FALSE(
      DecodeRecoveryAdvertisement(wire->substr(0, wire->size() - 1)).ok());
  first.coverage_[0] = {{10, 16}};
  EXPECT_FALSE(EncodeRecoveryAdvertisement(first).ok());
}

TEST(NativeRecoveryTest,
     EffectMetadataRejectsTruncationDuplicatesAndOversizedReceive) {
  const std::vector<NativeHistoryRecordInfo> records{{0, 7, 100}, {2, 9, 10}};
  auto wire = EncodeRecoveryEffectManifest(records);
  ASSERT_TRUE(wire.ok());
  auto decoded = DecodeRecoveryEffectManifest(*wire);
  ASSERT_TRUE(decoded.ok());
  EXPECT_EQ(*decoded, records);
  EXPECT_FALSE(
      DecodeRecoveryEffectManifest(wire->substr(0, wire->size() - 1)).ok());
  EXPECT_FALSE(EncodeRecoveryEffectManifest(
                   std::vector<NativeHistoryRecordInfo>{{0, 7, 1}, {0, 8, 1}})
                   .ok());
  EXPECT_FALSE(
      EncodeRecoveryEffectManifest(std::vector<NativeHistoryRecordInfo>{
                                       {0, 7, kRecoveryReceiveBytes + 1}})
          .ok());
}

// Canonical native wire fixture; the production payload source is asynchronous.
std::string Command(std::initializer_list<std::string> args) {
  std::string raw = "LRC1";
  raw.push_back(1);
  raw.push_back(0);
  raw.push_back(static_cast<char>(args.size()));
  raw.push_back(0);
  for (const auto& arg : args)
    for (unsigned shift = 0; shift < 32; shift += 8)
      raw.push_back(static_cast<char>(arg.size() >> shift));
  for (const auto& arg : args) raw.append(arg);
  return raw;
}

TEST(NativeReplayTest,
     IncompleteTransactionNeverPublishesAndWholeEffectDeduplicates) {
  auto frontier = std::make_shared<ReplicaAppliedFrontier>(2, 1);
  ASSERT_TRUE(
      frontier->InstallNextLsns(std::vector<std::uint64_t>{10, 20}).ok());
  auto history = std::make_shared<ReplicationHistory>(4096);
  ASSERT_TRUE(history->Reset("parent", 2).ok());
  NativeReplay replay(frontier, {history}, "parent");
  auto envelope = EncodeReplicationTransactionEnvelope({17, 0, {0, 1}});
  ASSERT_TRUE(envelope.ok());
  const std::vector<NativeHistoryRecord> records{
      {0, 10, Command({*envelope, "SET", "key", "value"})},
      {1, 20, Command({*envelope})}};
  EXPECT_FALSE(replay.PrepareEffect({records[0]}).ok());
  EXPECT_EQ(*frontier->TrySnapshot(), (std::vector<std::uint64_t>{10, 20}));
  auto complete = replay.PrepareEffect(records);
  ASSERT_TRUE(complete.ok()) << complete.status();
  EXPECT_EQ(complete->disposition_, NativeReplayDisposition::kReady);
  EXPECT_EQ(complete->command_.args_,
            (std::vector<std::string>{"SET", "key", "value"}));
  ASSERT_TRUE(replay
                  .PublishAfterApply(0, complete->updates_,
                                     std::move(complete->records_))
                  .ok());
  EXPECT_EQ(*frontier->TrySnapshot(), (std::vector<std::uint64_t>{11, 21}));
  EXPECT_FALSE(history->Coverage("parent")[0].empty());
  complete = replay.PrepareEffect(records);
  ASSERT_TRUE(complete.ok());
  EXPECT_EQ(complete->disposition_, NativeReplayDisposition::kDuplicate);
}

TEST(NativeReplayTest,
     OptionalRetentionPressureDropsTheWholeAppliedTransaction) {
  auto frontier = std::make_shared<ReplicaAppliedFrontier>(2, 1);
  auto history = std::make_shared<ReplicationHistory>(4096);
  ASSERT_TRUE(history->Reset("parent", 2).ok());
  NativeReplay replay(frontier, {history}, "parent");
  const std::vector<ReplicaAppliedFrontier::FlowApplied> complete_applied{
      {0, 1}, {1, 1}};
  ASSERT_TRUE(replay
                  .PublishAfterApply(0, complete_applied,
                                     {{0, 1, Command({"SET", "key", "value"})}})
                  .ok());
  EXPECT_EQ(*frontier->TrySnapshot(), (std::vector<std::uint64_t>{2, 2}));
  EXPECT_TRUE(history->Coverage("parent")[0].empty());
  EXPECT_TRUE(history->Coverage("parent")[1].empty());
}

TEST(NativeReplayTest, WorkerQuotaReclaimsOnlyItsOwnCompleteEffects) {
  auto frontier = std::make_shared<ReplicaAppliedFrontier>(2, 2);
  auto first = std::make_shared<ReplicationHistory>(4096);
  auto second = std::make_shared<ReplicationHistory>(4096);
  ASSERT_TRUE(first->Reset("parent", 2).ok());
  ASSERT_TRUE(second->Reset("parent", 2).ok());
  NativeReplay replay(frontier, {first, second}, "parent");
  auto primary = first->ChargePrimary(4096);
  const std::array updates{ReplicaAppliedFrontier::FlowApplied{0, 1},
                           ReplicaAppliedFrontier::FlowApplied{1, 1}};
  ASSERT_TRUE(
      replay
          .PublishAfterApply(1, updates, {{0, 1, "payload"}, {1, 1, "marker"}})
          .ok());
  EXPECT_TRUE(first->Coverage("parent")[0].empty());
  const auto effect = second->DescribeEffect("parent", 0, 1);
  ASSERT_TRUE(effect.ok());
  EXPECT_EQ(effect->size(), 2);
  auto other_primary = second->ChargePrimary(4096);
  EXPECT_FALSE(second->DescribeEffect("parent", 0, 1).ok());
  EXPECT_FALSE(second->DescribeEffect("parent", 1, 1).ok());
  EXPECT_EQ(*frontier->TrySnapshot(), (std::vector<std::uint64_t>{2, 2}));
  EXPECT_EQ(first->primary_bytes(), 4096);
}

TEST(NativeReplayTest, ConcurrentPublishersUseIndependentWorkerCaches) {
  auto frontier = std::make_shared<ReplicaAppliedFrontier>(2, 2);
  std::vector<std::shared_ptr<ReplicationHistory>> histories{
      std::make_shared<ReplicationHistory>(4096),
      std::make_shared<ReplicationHistory>(4096)};
  NativeReplay replay(frontier, histories, "parent");
  std::array<std::vector<std::vector<NativeHistoryRange>>, 2> coverage;
  auto publish = [&](unsigned worker) {
    ASSERT_TRUE(histories[worker]->Reset("parent", 2).ok());
    for (std::uint64_t lsn = 1; lsn <= 1000; ++lsn) {
      ASSERT_TRUE(
          replay.PublishAfterApply(worker, {worker, lsn, "canonical"}).ok());
      if (lsn % 16 == 0) {
        auto primary = histories[worker]->ChargePrimary(4096);
        EXPECT_EQ(histories[worker]->secondary_bytes(), 0);
      }
    }
    coverage[worker] = histories[worker]->Coverage("parent");
  };
  std::jthread first(publish, 0);
  std::jthread second(publish, 1);
  first.join();
  second.join();
  EXPECT_EQ(*frontier->TrySnapshot(), (std::vector<std::uint64_t>{1001, 1001}));
  ASSERT_EQ(coverage[0].size(), 2);
  ASSERT_EQ(coverage[1].size(), 2);
  EXPECT_TRUE(coverage[0][1].empty());
  EXPECT_TRUE(coverage[1][0].empty());
  ASSERT_FALSE(coverage[0][0].empty());
  ASSERT_FALSE(coverage[1][1].empty());
  EXPECT_EQ(coverage[0][0].back().end_lsn_, 1001);
  EXPECT_EQ(coverage[1][1].back().end_lsn_, 1001);
}

TEST(NativeReplayTest, SingleFlowCompletionDoesNotRequireOptionalRetention) {
  auto frontier = std::make_shared<ReplicaAppliedFrontier>(1, 1);
  auto history = std::make_shared<ReplicationHistory>(4096);
  ASSERT_TRUE(history->Reset("parent", 1).ok());
  NativeReplay replay(frontier, {history}, "parent");
  ASSERT_TRUE(replay.PublishAfterApply(0, {0, 1, "canonical"}).ok());
  EXPECT_EQ(history->Read("parent", 0, 1, 0, 64)->bytes_, "canonical");
  ASSERT_TRUE(replay.PublishAfterApply(0, {0, 2, {}}).ok());
  EXPECT_FALSE(history->DescribeEffect("parent", 0, 2).ok());
  auto primary = history->ChargePrimary(4096);
  ASSERT_TRUE(replay.PublishAfterApply(0, {0, 3, "no-space"}).ok());
  EXPECT_FALSE(history->DescribeEffect("parent", 0, 3).ok());
  EXPECT_FALSE(replay.PublishAfterApply(1, {0, 4, "wrong-owner"}).ok());
  EXPECT_EQ(*frontier->TrySnapshot(), (std::vector<std::uint64_t>{4}));
}

TEST(NativeRecoveryTest, MergeWorkerCoverageBeforeLimitingAdvertisedRanges) {
  ReplicationHistory first(16384), second(16384);
  ASSERT_TRUE(first.Reset("parent", 1).ok());
  ASSERT_TRUE(second.Reset("parent", 1).ok());
  for (std::uint64_t lsn = 1; lsn <= 32; ++lsn)
    ASSERT_TRUE((lsn % 2 ? first : second)
                    .TryRetain("parent", {{0, lsn, "canonical"}}));
  auto ranges =
      first.Coverage("parent", std::numeric_limits<std::size_t>::max());
  auto other =
      second.Coverage("parent", std::numeric_limits<std::size_t>::max());
  ranges[0].insert(ranges[0].end(), other[0].begin(), other[0].end());
  auto merged = MergeWorkerHistoryCoverage(std::move(ranges));
  EXPECT_EQ(merged[0], (std::vector<NativeHistoryRange>{{1, 33}}));
  EXPECT_LE(merged[0].capacity(), 8);
  for (std::uint64_t lsn = 40; lsn < 60; lsn += 2)
    merged[0].push_back({lsn, lsn + 1});
  merged = MergeWorkerHistoryCoverage(std::move(merged));
  ASSERT_EQ(merged[0].size(), 8);
  EXPECT_EQ(merged[0].front(), (NativeHistoryRange{44, 45}));
  EXPECT_EQ(merged[0].back(), (NativeHistoryRange{58, 59}));
}

TEST(NativeReplayTest, MissingPredecessorIsNotACompleteAppliedCut) {
  auto frontier = std::make_shared<ReplicaAppliedFrontier>(2, 1);
  ASSERT_TRUE(
      frontier->InstallNextLsns(std::vector<std::uint64_t>{10, 19}).ok());
  NativeReplay replay(frontier, {}, "parent");
  auto envelope = EncodeReplicationTransactionEnvelope({17, 0, {0, 1}});
  ASSERT_TRUE(envelope.ok());
  auto complete =
      replay.PrepareEffect({{0, 10, Command({*envelope, "DEL", "key"})},
                            {1, 20, Command({*envelope})}});
  ASSERT_TRUE(complete.ok()) << complete.status();
  EXPECT_EQ(complete->disposition_, NativeReplayDisposition::kNeedsPredecessor);
  EXPECT_EQ(*frontier->TrySnapshot(), (std::vector<std::uint64_t>{10, 19}));
}

TEST(NativeReplayTest, ControlBarrierRequiresEveryOriginFlowAndExactIdentity) {
  auto frontier = std::make_shared<ReplicaAppliedFrontier>(2, 1);
  NativeReplay replay(frontier, {}, "parent");
  EXPECT_FALSE(
      replay.PrepareEffect({{0, 1, Command({"FLUSHDB", "7", "2"})}}).ok());
  EXPECT_FALSE(replay
                   .PrepareEffect({{0, 1, Command({"FLUSHDB", "7", "2"})},
                                   {1, 1, Command({"FLUSHDB", "8", "2"})}})
                   .ok());
  auto complete =
      replay.PrepareEffect({{0, 1, Command({"FLUSHDB", "7", "2"})},
                            {1, 1, Command({"FLUSHDB", "7", "2"})}});
  ASSERT_TRUE(complete.ok()) << complete.status();
  EXPECT_EQ(complete->disposition_, NativeReplayDisposition::kReady);
}

}  // namespace
}  // namespace lavik::detail
