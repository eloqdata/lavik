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

#include "lavik/replication_command.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "lavik/command.h"
#include "lavik/memory.h"

namespace {

class CaptureMemoryScope {
 public:
  CaptureMemoryScope() : previous_(lavik::CurrentMemoryAccountingShard()) {
    EXPECT_TRUE(lavik::InitMemoryLimit(1024ULL * 1024 * 1024, 1).ok());
    lavik::BindMemoryAccountingShard(0);
  }
  ~CaptureMemoryScope() {
    EXPECT_TRUE(lavik::InitMemoryLimit(1024ULL * 1024 * 1024, 1).ok());
    lavik::BindMemoryAccountingShard(previous_ == 0 ? lavik::kMaxMemoryWorkers
                                                    : previous_ - 1);
  }

 private:
  unsigned previous_;
};

TEST(ReplicationCommandTest, PreparedCaptureChargeFollowsMovedEffects) {
  CaptureMemoryScope memory;
  const auto baseline = lavik::WorkerMemoryAccountingBytes(0);
  {
    std::vector<lavik::CapturedReplicationCommand> pending;
    {
      lavik::ReplicationCommandCapture capture;
      ASSERT_TRUE(capture.ReserveAdditionalCommands(2, 4096).ok());
      capture.Record(0, {"DEL", "destination"});
      capture.Record(0, {"SADD", "destination", std::string(2048, 'v')});
      auto effects = capture.Take();
      pending = std::move(effects.commands_);
    }
    ASSERT_EQ(pending.size(), 2);
    ASSERT_NE(pending[0].retained_charge_, nullptr);
    EXPECT_EQ(pending[0].retained_charge_, pending[1].retained_charge_);
    EXPECT_GE(lavik::WorkerMemoryAccountingBytes(0) - baseline, 4096);
    EXPECT_EQ(pending[1].args_.back(), std::string(2048, 'v'));
  }
  EXPECT_EQ(lavik::WorkerMemoryAccountingBytes(0), baseline);
}

TEST(ReplicationCommandTest, CapturePreparationOomPreservesEarlierEffects) {
  CaptureMemoryScope memory;
  const auto baseline = lavik::WorkerMemoryAccountingBytes(0);
  {
    lavik::ReplicationCommandCapture capture;
    ASSERT_TRUE(capture.ReserveAdditionalCommands(1, 1024).ok());
    capture.Record(0, {"SET", "prefix", "kept"});
    const auto charged = lavik::WorkerMemoryAccountingBytes(0);
    EXPECT_EQ(
        capture.ReserveAdditionalCommands(1, 1024ULL * 1024 * 1024).code(),
        absl::StatusCode::kResourceExhausted);
    EXPECT_EQ(
        capture
            .ReserveAdditionalCommands(std::numeric_limits<std::size_t>::max())
            .code(),
        absl::StatusCode::kResourceExhausted);
    EXPECT_EQ(lavik::WorkerMemoryAccountingBytes(0), charged);
    auto effects = capture.Take();
    ASSERT_EQ(effects.commands_.size(), 1);
    EXPECT_EQ(effects.commands_[0].args_,
              (std::vector<std::string>{"SET", "prefix", "kept"}));
  }
  EXPECT_EQ(lavik::WorkerMemoryAccountingBytes(0), baseline);
}

TEST(ReplicationCommandTest, UnusedCapturePreparationDoesNotBurdenNextCommand) {
  CaptureMemoryScope memory;
  const auto baseline = lavik::WorkerMemoryAccountingBytes(0);
  lavik::ReplicationCommandCapture capture;
  ASSERT_TRUE(capture.ReserveAdditionalCommands(2, 4096).ok());
  capture.MarkHandled();
  ASSERT_GT(lavik::WorkerMemoryAccountingBytes(0), baseline);
  capture.ReleaseUnusedPreparation();
  EXPECT_EQ(lavik::WorkerMemoryAccountingBytes(0), baseline);
  auto empty = capture.Take();
  EXPECT_TRUE(empty.handled_);
  EXPECT_TRUE(empty.commands_.empty());

  ASSERT_TRUE(capture.ReserveAdditionalCommands(1, 1024).ok());
  capture.Record(0, {"SET", "kept", "value"});
  const auto charged = lavik::WorkerMemoryAccountingBytes(0);
  capture.ReleaseUnusedPreparation();
  EXPECT_EQ(lavik::WorkerMemoryAccountingBytes(0), charged);
  EXPECT_EQ(capture.Take().commands_.size(), 1);
}

TEST(ReplicationCommandTest, SetAlreadyCarriesFinalExpirationSemantics) {
  std::vector<std::string> plain{"SET", "key", "value"};
  lavik::AppendReplicationExpirationEffect(&plain, 0, 0, "key", true, 0);
  EXPECT_EQ(plain, (std::vector<std::string>{"SET", "key", "value"}));

  std::vector<std::string> expiring{"SET", "key", "value", "PXAT", "123456"};
  lavik::AppendReplicationExpirationEffect(&expiring, 0, 0, "key", true,
                                           123456);
  EXPECT_EQ(expiring, (std::vector<std::string>{"SET", "key", "value", "PXAT",
                                                "123456"}));
}

TEST(ReplicationCommandTest, OtherWritesStillReceiveExpirationEffect) {
  std::vector<std::string> persistent{"HSET", "key", "field", "value"};
  lavik::AppendReplicationExpirationEffect(&persistent, 2, 2, "key", true, 0);
  EXPECT_EQ(persistent,
            (std::vector<std::string>{
                std::string(lavik::kReplicatedExecCommand), "2", "2", "4",
                "HSET", "key", "field", "value", "2", "2", "PERSIST", "key"}));

  std::vector<std::string> expiring{"HSET", "key", "field", "value"};
  lavik::AppendReplicationExpirationEffect(&expiring, 3, 3, "key", true,
                                           123456);
  EXPECT_EQ(expiring, (std::vector<std::string>{
                          std::string(lavik::kReplicatedExecCommand), "2", "3",
                          "4", "HSET", "key", "field", "value", "3", "3",
                          "PEXPIREAT", "key", "123456"}));
}

TEST(ReplicationCommandTest,
     ClassifiesOnlyCompletePublishEffectsAsPartitionless) {
  EXPECT_TRUE(lavik::IsPublishOnlyReplicationCommand(
      {0, {"pUbLiSh", "channel", "message"}}));
  EXPECT_TRUE(lavik::IsPublishOnlyReplicationCommand(
      {0, lavik::EncodeReplicationCommandEffects(
              {{0, {"PUBLISH", "first", "SET"}},
               {15, {"publish", "second", "__LAVIK_EXEC_V1"}}})}));

  // A PUBLISH child does not exempt the storage effects beside it, regardless
  // of their position in the envelope. TTL wrappers must retain their source
  // sequence for every child.
  EXPECT_FALSE(lavik::IsPublishOnlyReplicationCommand(
      {0, lavik::EncodeReplicationCommandEffects(
              {{0, {"PUBLISH", "channel", "message"}},
               {0, {"SET", "key", "value"}}})}));
  EXPECT_FALSE(lavik::IsPublishOnlyReplicationCommand(
      {0, lavik::EncodeReplicationCommandEffects(
              {{0, {"SET", "key", "value"}},
               {0, {"PUBLISH", "channel", "message"}}})}));
  for (const std::uint64_t deadline : {0ULL, 123456ULL}) {
    std::vector<std::string> args{"HSET", "key", "field", "value"};
    lavik::AppendReplicationExpirationEffect(&args, 0, 0, "key", true,
                                             deadline);
    EXPECT_FALSE(lavik::IsPublishOnlyReplicationCommand({0, std::move(args)}));
  }
  EXPECT_FALSE(
      lavik::IsPublishOnlyReplicationCommand({0, {"FUNCTION", "FLUSH"}}));
}

TEST(ReplicationCommandTest,
     MalformedPublishEnvelopesCannotBypassPartitionReset) {
  const auto valid = lavik::EncodeReplicationCommandEffects(
      {{0, {"PUBLISH", "channel", "message"}}});
  for (std::size_t size = 0; size < valid.size(); ++size) {
    SCOPED_TRACE(size);
    auto truncated = valid;
    truncated.resize(size);
    EXPECT_FALSE(lavik::IsPublishOnlyReplicationCommand({0, truncated}));
  }
  const std::vector<std::pair<std::size_t, std::string>> corruptions{
      {1, "0"},
      {1, "2"},
      {1, "-1"},
      {1, "1suffix"},
      {1, "18446744073709551616"},
      {2, "16"},
      {2, "-1"},
      {2, "0suffix"},
      {3, "2"},
      {3, "4"},
      {3, "3suffix"},
      {4, "__LAVIK_EXEC_V1"}};
  for (const auto& [index, value] : corruptions) {
    SCOPED_TRACE(value);
    auto malformed = valid;
    malformed[index] = value;
    EXPECT_FALSE(lavik::IsPublishOnlyReplicationCommand({0, malformed}));
  }
  auto trailing = valid;
  trailing.push_back("extra");
  EXPECT_FALSE(lavik::IsPublishOnlyReplicationCommand({0, trailing}));
  EXPECT_FALSE(lavik::IsPublishOnlyReplicationCommand({16, valid}));
  EXPECT_FALSE(
      lavik::IsPublishOnlyReplicationCommand({0, {"PUBLISH", "channel"}}));
  EXPECT_FALSE(lavik::IsPublishOnlyReplicationCommand(
      {0, {"PUBLISH", "channel", "message", "extra"}}));
}

TEST(ReplicationCommandTest, StagingBudgetIncludesShortArgumentOwners) {
  // A fixed staging budget is allocator-independent, but high-arity commands
  // must still pay for each owned std::string element rather than only their
  // encoded length fields.
  std::vector<std::string> args(1024);
  const auto bytes = lavik::storage::ReplicationCommandStagingBytes(args);
  ASSERT_TRUE(bytes.has_value());
  EXPECT_GE(*bytes, args.size() * sizeof(std::string));
}

TEST(ReplicationCommandTest, TransactionReservationCoversMaterializedPrefix) {
  std::vector<std::string> command_args{"SET", "key", std::string(1024, 'v')};
  constexpr std::size_t kParticipantCapacity = 8;
  const auto reserved = lavik::storage::ReplicationTransactionReservationBytes(
      kParticipantCapacity, 2, command_args);
  ASSERT_TRUE(reserved.has_value());

  auto encoded = lavik::EncodeReplicationTransactionEnvelope(
      lavik::ReplicationTransactionEnvelope{
          .id_ = std::numeric_limits<std::uint64_t>::max(),
          .payload_flow_ = 0,
          .participants_ = {0, 1},
      });
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  std::vector<std::string> prefix{std::move(*encoded)};
  const auto materialized =
      lavik::storage::ReplicationTransactionAllocationBytes(
          kParticipantCapacity, prefix, command_args);
  ASSERT_TRUE(materialized.has_value());
  EXPECT_GE(*reserved, *materialized);
}

TEST(ReplicationCommandTest, TransactionEnvelopeRoundTripsCanonicalBitmap) {
  auto encoded = lavik::EncodeReplicationTransactionEnvelope(
      lavik::ReplicationTransactionEnvelope{
          .id_ = 0x8877665544332211ULL,
          .payload_flow_ = 3,
          .participants_ = {7, 0, 3},
      });
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  EXPECT_TRUE(lavik::IsReplicationTransactionEnvelope(*encoded));
  auto decoded = lavik::DecodeReplicationTransactionEnvelope(*encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(decoded->id_, 0x8877665544332211ULL);
  EXPECT_EQ(decoded->payload_flow_, 3U);
  EXPECT_EQ(decoded->participants_, (std::vector<unsigned>{0, 3, 7}));

  std::string noncanonical = *encoded;
  noncanonical.push_back('\0');
  noncanonical[14] = 2;
  EXPECT_FALSE(lavik::DecodeReplicationTransactionEnvelope(noncanonical).ok());
}

TEST(ReplicationCommandTest, EnforcesCompleteEncodingLimit) {
  const std::string one_mebibyte(1024 * 1024, 'x');
  std::vector<std::string_view> args(1024, one_mebibyte);
  constexpr std::size_t kHeaderBytes = 8 + 1024 * sizeof(std::uint32_t);
  args.back() = std::string_view(one_mebibyte).substr(kHeaderBytes);

  auto exact = lavik::ReplicationCommandPayloadSource::Create(0, args);
  ASSERT_TRUE(exact.ok()) << exact.status();
  EXPECT_EQ(exact->size(), lavik::kMaxNativeReplicationEventBytes);

  args.back() = std::string_view(one_mebibyte).substr(kHeaderBytes - 1);
  auto oversized = lavik::ReplicationCommandPayloadSource::Create(0, args);

  EXPECT_FALSE(oversized.ok());
  EXPECT_EQ(oversized.status().code(), absl::StatusCode::kResourceExhausted);
}

}  // namespace
