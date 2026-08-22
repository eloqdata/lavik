#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "keylane/replication_command.h"

namespace {

TEST(ReplicationCommandTest, SetAlreadyCarriesFinalExpirationSemantics) {
  std::vector<std::string> plain{"SET", "key", "value"};
  keylane::AppendReplicationExpirationEffect(&plain, 0, 0, "key", true, 0);
  EXPECT_EQ(plain,
            (std::vector<std::string>{"SET", "key", "value"}));

  std::vector<std::string> expiring{"SET", "key", "value", "PXAT",
                                    "123456"};
  keylane::AppendReplicationExpirationEffect(&expiring, 0, 0, "key", true,
                                              123456);
  EXPECT_EQ(expiring, (std::vector<std::string>{"SET", "key", "value",
                                                "PXAT", "123456"}));
}

TEST(ReplicationCommandTest, OtherWritesStillReceiveExpirationEffect) {
  std::vector<std::string> persistent{"HSET", "key", "field", "value"};
  keylane::AppendReplicationExpirationEffect(&persistent, 2, 2, "key", true,
                                              0);
  EXPECT_EQ(persistent,
            (std::vector<std::string>{
                std::string(keylane::kReplicatedExecCommand), "2", "2", "4",
                "HSET", "key", "field", "value", "2", "2", "PERSIST",
                "key"}));

  std::vector<std::string> expiring{"HSET", "key", "field", "value"};
  keylane::AppendReplicationExpirationEffect(&expiring, 3, 3, "key", true,
                                              123456);
  EXPECT_EQ(expiring,
            (std::vector<std::string>{
                std::string(keylane::kReplicatedExecCommand), "2", "3", "4",
                "HSET", "key", "field", "value", "3", "3", "PEXPIREAT",
                "key", "123456"}));
}

}  // namespace
