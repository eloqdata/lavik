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
  NativeReplay replay(frontier, history, "parent");
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
  NativeReplay replay(frontier, history, "parent");
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

TEST(NativeReplayTest, MissingPredecessorIsNotACompleteAppliedCut) {
  auto frontier = std::make_shared<ReplicaAppliedFrontier>(2, 1);
  ASSERT_TRUE(
      frontier->InstallNextLsns(std::vector<std::uint64_t>{10, 19}).ok());
  NativeReplay replay(frontier, nullptr, "parent");
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
  NativeReplay replay(frontier, nullptr, "parent");
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
