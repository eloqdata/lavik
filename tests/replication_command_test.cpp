#include "keylane/replication_command.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

namespace {

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

  std::vector<std::string> prefix{"__KEYLANE_TX_V1", "18446744073709551615",
                                  "2", "0", "1"};
  const auto materialized =
      keylane::storage::ReplicationTransactionAllocationBytes(
          kParticipantCapacity, prefix, command_args);
  ASSERT_TRUE(materialized.has_value());
  EXPECT_GE(*reserved, *materialized);
}

}  // namespace
