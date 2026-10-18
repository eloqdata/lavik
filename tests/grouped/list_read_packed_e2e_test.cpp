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

#include <algorithm>
#include <string>
#include <vector>

#include "write_e2e_support.h"

namespace {
using namespace grouped_e2e;

std::vector<std::string> ListValues(unsigned count) {
  std::vector<std::string> values;
  for (unsigned i = 0; i < count; ++i) {
    values.push_back(
        i == 0 ? "" : std::string(128, 'a' + i % 23) + std::to_string(i));
  }
  values[1] = std::string("binary\0\r\n", 9);
  return values;
}

void ExpectList(const Reply& reply, const std::vector<std::string>& expected) {
  ASSERT_EQ(reply.kind_, '*');
  ASSERT_EQ(reply.items_.size(), expected.size());
  for (std::size_t i = 0; i < expected.size(); ++i) {
    ASSERT_EQ(reply.items_[i].kind_, '$') << i;
    EXPECT_EQ(reply.items_[i].text_, expected[i]) << i;
  }
}

TEST(PackedListReadE2e,
     CompactAndGroupedRangesOwnBytesAcrossExecLuaAndRecovery) {
  PrivateDisk disk;
  disk.PreserveOnFailure();
  {
    Server server(disk, 3);
    server.PreserveOnFailure();
    Client client(server.port());
    ASSERT_EQ(client.Command({"SET", "wrong-type", "unchanged"}).text_, "OK");
    for (const unsigned count : {10u, 512u}) {
      const std::string key = "packed-list-" + std::to_string(count);
      const auto expected = ListValues(count);
      std::vector<std::string> seed{"RPUSH", key};
      seed.insert(seed.end(), expected.begin(), expected.end());
      const auto reseed = [&] {
        ASSERT_EQ(client.Command({"DEL", key}).kind_, ':');
        ASSERT_EQ(client.Command(seed).text_, std::to_string(count));
      };
      for (const bool resp3 : {false, true}) {
        SCOPED_TRACE(std::to_string(count) + (resp3 ? " RESP3" : " RESP2"));
        ASSERT_EQ(client.Command({"HELLO", resp3 ? "3" : "2"}).kind_,
                  resp3 ? '%' : '*');
        ASSERT_NO_FATAL_FAILURE(reseed());
        ExpectList(client.Command({"LRANGE", key, "0", "-1"}), expected);
        ExpectList(client.Command({"LRANGE", key, "-3", "-1"}),
                   {expected.end() - 3, expected.end()});
        ExpectList(client.Command({"LRANGE", key, "1", "3"}),
                   {expected.begin() + 1, expected.begin() + 4});
        ExpectList(client.Command({"LRANGE", key, "-100000", "100000"}),
                   expected);
        ExpectList(client.Command({"LRANGE", key, "4", "2"}), {});
        ExpectList(client.Command({"LRANGE", key, "100000", "100001"}), {});
        ExpectList(client.Command({"LRANGE", "missing", "0", "-1"}), {});
        const auto wrong = client.Command({"LRANGE", "wrong-type", "0", "-1"});
        EXPECT_EQ(wrong.kind_, '-');
        EXPECT_TRUE(wrong.text_.starts_with("WRONGTYPE"));
        EXPECT_EQ(client.Command({"LINDEX", key, "1"}).text_, expected[1]);

        // EXEC finishes the replacement before draining the earlier range
        // reply. Its bytes must therefore survive the original key and lease.
        ASSERT_EQ(client.Command({"MULTI"}).text_, "OK");
        ASSERT_EQ(client.Command({"LRANGE", key, "0", "-1"}).text_, "QUEUED");
        ASSERT_EQ(client.Command({"DEL", key}).text_, "QUEUED");
        ASSERT_EQ(client.Command({"SET", key, "after-exec"}).text_, "QUEUED");
        const auto exec = client.Command({"EXEC"});
        ASSERT_EQ(exec.kind_, '*');
        ASSERT_EQ(exec.items_.size(), 3U);
        ExpectList(exec.items_[0], expected);
        EXPECT_EQ(exec.items_[1].text_, "1");
        EXPECT_EQ(exec.items_[2].text_, "OK");
        EXPECT_EQ(client.Command({"GET", key}).text_, "after-exec");

        ASSERT_NO_FATAL_FAILURE(reseed());
        const auto lua = client.Command(
            {"EVAL",
             "redis.setresp(tonumber(ARGV[1])); "
             "local reply=redis.call('LRANGE',KEYS[1],0,-1); "
             "redis.call('DEL',KEYS[1]); "
             "redis.call('SET',KEYS[1],'after-lua'); return reply",
             "1", key, resp3 ? "3" : "2"});
        ExpectList(lua, expected);
        EXPECT_EQ(client.Command({"GET", key}).text_, "after-lua");

        ASSERT_NO_FATAL_FAILURE(reseed());
        // Mutable consumers still own individual strings, including binary
        // and empty values; the read-only packed option must not leak to pop.
        EXPECT_EQ(client.Command({"LPOP", key}).text_, expected.front());
        EXPECT_EQ(client.Command({"RPOP", key}).text_, expected.back());
        ExpectList(client.Command({"LRANGE", key, "0", "-1"}),
                   {expected.begin() + 1, expected.end() - 1});
        ASSERT_EQ(client.Command({"PEXPIRE", key, "-1"}).text_, "1");
        ExpectList(client.Command({"LRANGE", key, "0", "-1"}), {});
        ASSERT_NO_FATAL_FAILURE(reseed());
      }
    }
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  EXPECT_EQ(disk.LatestRootGrouped("packed-list-10"), false);
  EXPECT_EQ(disk.LatestRootGrouped("packed-list-512"), true);
  Server recovered(disk, 3);
  Client client(recovered.port());
  for (const unsigned count : {10u, 512u}) {
    ExpectList(client.Command({"LRANGE", "packed-list-" + std::to_string(count),
                               "0", "-1"}),
               ListValues(count));
  }
  ASSERT_EQ(recovered.Wait(true), 0) << recovered.Log();
}
}  // namespace

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  if (argc != 2) return 2;
  grouped_e2e::server_binary = argv[1];
  return RUN_ALL_TESTS();
}
