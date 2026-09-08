#include "keylane/replication_command.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "keylane/command.h"
#include "keylane/memory.h"

namespace {

class CaptureMemoryScope {
 public:
  CaptureMemoryScope() : previous_(keylane::CurrentMemoryAccountingShard()) {
    EXPECT_TRUE(keylane::InitMemoryLimit(1024ULL * 1024 * 1024, 1).ok());
    keylane::BindMemoryAccountingShard(0);
  }
  ~CaptureMemoryScope() {
    EXPECT_TRUE(keylane::InitMemoryLimit(1024ULL * 1024 * 1024, 1).ok());
    keylane::BindMemoryAccountingShard(
        previous_ == 0 ? keylane::kMaxMemoryWorkers : previous_ - 1);
  }

 private:
  unsigned previous_;
};

TEST(ReplicationCommandTest, PreparedCaptureChargeFollowsMovedEffects) {
  CaptureMemoryScope memory;
  const auto baseline = keylane::WorkerMemoryAccountingBytes(0);
  {
    std::vector<keylane::CapturedReplicationCommand> pending;
    {
      keylane::ReplicationCommandCapture capture;
      ASSERT_TRUE(capture.ReserveAdditionalCommands(2, 4096).ok());
      capture.Record(0, {"DEL", "destination"});
      capture.Record(0, {"SADD", "destination", std::string(2048, 'v')});
      auto effects = capture.Take();
      pending = std::move(effects.commands_);
    }
    ASSERT_EQ(pending.size(), 2);
    ASSERT_NE(pending[0].retained_charge_, nullptr);
    EXPECT_EQ(pending[0].retained_charge_, pending[1].retained_charge_);
    EXPECT_GE(keylane::WorkerMemoryAccountingBytes(0) - baseline, 4096);
    EXPECT_EQ(pending[1].args_.back(), std::string(2048, 'v'));
  }
  EXPECT_EQ(keylane::WorkerMemoryAccountingBytes(0), baseline);
}

TEST(ReplicationCommandTest, CapturePreparationOomPreservesEarlierEffects) {
  CaptureMemoryScope memory;
  const auto baseline = keylane::WorkerMemoryAccountingBytes(0);
  {
    keylane::ReplicationCommandCapture capture;
    ASSERT_TRUE(capture.ReserveAdditionalCommands(1, 1024).ok());
    capture.Record(0, {"SET", "prefix", "kept"});
    const auto charged = keylane::WorkerMemoryAccountingBytes(0);
    EXPECT_EQ(
        capture.ReserveAdditionalCommands(1, 1024ULL * 1024 * 1024).code(),
        absl::StatusCode::kResourceExhausted);
    EXPECT_EQ(
        capture
            .ReserveAdditionalCommands(std::numeric_limits<std::size_t>::max())
            .code(),
        absl::StatusCode::kResourceExhausted);
    EXPECT_EQ(keylane::WorkerMemoryAccountingBytes(0), charged);
    auto effects = capture.Take();
    ASSERT_EQ(effects.commands_.size(), 1);
    EXPECT_EQ(effects.commands_[0].args_,
              (std::vector<std::string>{"SET", "prefix", "kept"}));
  }
  EXPECT_EQ(keylane::WorkerMemoryAccountingBytes(0), baseline);
}

TEST(ReplicationCommandTest, UnusedCapturePreparationDoesNotBurdenNextCommand) {
  CaptureMemoryScope memory;
  const auto baseline = keylane::WorkerMemoryAccountingBytes(0);
  keylane::ReplicationCommandCapture capture;
  ASSERT_TRUE(capture.ReserveAdditionalCommands(2, 4096).ok());
  capture.MarkHandled();
  ASSERT_GT(keylane::WorkerMemoryAccountingBytes(0), baseline);
  capture.ReleaseUnusedPreparation();
  EXPECT_EQ(keylane::WorkerMemoryAccountingBytes(0), baseline);
  auto empty = capture.Take();
  EXPECT_TRUE(empty.handled_);
  EXPECT_TRUE(empty.commands_.empty());

  ASSERT_TRUE(capture.ReserveAdditionalCommands(1, 1024).ok());
  capture.Record(0, {"SET", "kept", "value"});
  const auto charged = keylane::WorkerMemoryAccountingBytes(0);
  capture.ReleaseUnusedPreparation();
  EXPECT_EQ(keylane::WorkerMemoryAccountingBytes(0), charged);
  EXPECT_EQ(capture.Take().commands_.size(), 1);
}

TEST(ReplicationCommandTest, SetAlreadyCarriesFinalExpirationSemantics) {
  std::vector<std::string> plain{"SET", "key", "value"};
  keylane::AppendReplicationExpirationEffect(&plain, 0, 0, "key", true, 0);
  EXPECT_EQ(plain, (std::vector<std::string>{"SET", "key", "value"}));

  std::vector<std::string> expiring{"SET", "key", "value", "PXAT", "123456"};
  keylane::AppendReplicationExpirationEffect(&expiring, 0, 0, "key", true,
                                             123456);
  EXPECT_EQ(expiring, (std::vector<std::string>{"SET", "key", "value", "PXAT",
                                                "123456"}));
}

TEST(ReplicationCommandTest, OtherWritesStillReceiveExpirationEffect) {
  std::vector<std::string> persistent{"HSET", "key", "field", "value"};
  keylane::AppendReplicationExpirationEffect(&persistent, 2, 2, "key", true, 0);
  EXPECT_EQ(persistent,
            (std::vector<std::string>{
                std::string(keylane::kReplicatedExecCommand), "2", "2", "4",
                "HSET", "key", "field", "value", "2", "2", "PERSIST", "key"}));

  std::vector<std::string> expiring{"HSET", "key", "field", "value"};
  keylane::AppendReplicationExpirationEffect(&expiring, 3, 3, "key", true,
                                             123456);
  EXPECT_EQ(expiring, (std::vector<std::string>{
                          std::string(keylane::kReplicatedExecCommand), "2",
                          "3", "4", "HSET", "key", "field", "value", "3", "3",
                          "PEXPIREAT", "key", "123456"}));
}

TEST(ReplicationCommandTest, StagingBudgetIncludesShortArgumentOwners) {
  // A fixed staging budget is allocator-independent, but high-arity commands
  // must still pay for each owned std::string element rather than only their
  // encoded length fields.
  std::vector<std::string> args(1024);
  const auto bytes = keylane::storage::ReplicationCommandStagingBytes(args);
  ASSERT_TRUE(bytes.has_value());
  EXPECT_GE(*bytes, args.size() * sizeof(std::string));
}

TEST(ReplicationCommandTest, TransactionReservationCoversMaterializedPrefix) {
  std::vector<std::string> command_args{"SET", "key", std::string(1024, 'v')};
  constexpr std::size_t kParticipantCapacity = 8;
  const auto reserved =
      keylane::storage::ReplicationTransactionReservationBytes(
          kParticipantCapacity, 2, command_args);
  ASSERT_TRUE(reserved.has_value());

  auto encoded = keylane::EncodeReplicationTransactionEnvelope(
      keylane::ReplicationTransactionEnvelope{
          .id_ = std::numeric_limits<std::uint64_t>::max(),
          .payload_flow_ = 0,
          .participants_ = {0, 1},
      });
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  std::vector<std::string> prefix{std::move(*encoded)};
  const auto materialized =
      keylane::storage::ReplicationTransactionAllocationBytes(
          kParticipantCapacity, prefix, command_args);
  ASSERT_TRUE(materialized.has_value());
  EXPECT_GE(*reserved, *materialized);
}

TEST(ReplicationCommandTest, TransactionEnvelopeRoundTripsCanonicalBitmap) {
  auto encoded = keylane::EncodeReplicationTransactionEnvelope(
      keylane::ReplicationTransactionEnvelope{
          .id_ = 0x8877665544332211ULL,
          .payload_flow_ = 3,
          .participants_ = {7, 0, 3},
      });
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  EXPECT_TRUE(keylane::IsReplicationTransactionEnvelope(*encoded));
  auto decoded = keylane::DecodeReplicationTransactionEnvelope(*encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(decoded->id_, 0x8877665544332211ULL);
  EXPECT_EQ(decoded->payload_flow_, 3U);
  EXPECT_EQ(decoded->participants_, (std::vector<unsigned>{0, 3, 7}));

  std::string noncanonical = *encoded;
  noncanonical.push_back('\0');
  noncanonical[14] = 2;
  EXPECT_FALSE(
      keylane::DecodeReplicationTransactionEnvelope(noncanonical).ok());
}

TEST(ReplicationCommandTest, EnforcesCompleteEncodingLimit) {
  const std::string one_mebibyte(1024 * 1024, 'x');
  std::vector<std::string_view> args(1024, one_mebibyte);
  constexpr std::size_t kHeaderBytes = 8 + 1024 * sizeof(std::uint32_t);
  args.back() = std::string_view(one_mebibyte).substr(kHeaderBytes);

  auto exact = keylane::ReplicationCommandPayloadSource::Create(0, args);
  ASSERT_TRUE(exact.ok()) << exact.status();
  EXPECT_EQ(exact->size(), keylane::kMaxNativeReplicationEventBytes);

  args.back() = std::string_view(one_mebibyte).substr(kHeaderBytes - 1);
  auto oversized = keylane::ReplicationCommandPayloadSource::Create(0, args);

  EXPECT_FALSE(oversized.ok());
  EXPECT_EQ(oversized.status().code(), absl::StatusCode::kResourceExhausted);
}

}  // namespace
