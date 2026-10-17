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
#include <numeric>

#include "write_e2e_support.h"

namespace {
using namespace grouped_e2e;

std::vector<std::string> Members(std::size_t count) {
  std::vector<std::string> members{"", std::string("binary\0\r\n", 9)};
  for (std::size_t i = members.size(); i < count; ++i)
    members.push_back("member" + std::to_string(i) + std::string(128, 'm'));
  return members;
}

std::vector<std::string> Seed(std::string key,
                              const std::vector<std::string>& members) {
  std::vector<std::string> command{"ZADD", std::move(key)};
  for (std::size_t i = 0; i < members.size(); ++i) {
    command.push_back(std::to_string(i));
    command.push_back(members[i]);
  }
  return command;
}

void ExpectRange(const Reply& reply, const std::vector<std::string>& members,
                 const std::vector<std::size_t>& selected, bool scores,
                 unsigned protocol) {
  ASSERT_EQ(reply.kind_, '*') << reply.text_;
  const bool nested = scores && protocol == 3;
  ASSERT_EQ(reply.items_.size(), selected.size() * (scores && !nested ? 2 : 1));
  for (std::size_t i = 0; i < selected.size(); ++i) {
    const auto index = selected[i];
    if (nested) {
      ASSERT_EQ(reply.items_[i].kind_, '*');
      ASSERT_EQ(reply.items_[i].items_.size(), 2);
      EXPECT_EQ(reply.items_[i].items_[0].text_, members[index]);
      EXPECT_EQ(reply.items_[i].items_[1].kind_, ',');
      EXPECT_EQ(reply.items_[i].items_[1].text_, std::to_string(index));
    } else {
      EXPECT_EQ(reply.items_[i * (scores ? 2 : 1)].text_, members[index]);
      if (scores)
        EXPECT_EQ(reply.items_[2 * i + 1].text_, std::to_string(index));
    }
  }
}

TEST(SortedSetPackedReadE2e,
     RangesKeepOrderAndScoresAcrossLayoutsAndProtocols) {
  PrivateDisk disk;
  for (unsigned pass = 0; pass < 2; ++pass) {
    Server server(disk, pass == 0 ? 2 : 3);
    Client client(server.port());
    for (const std::size_t count : {6U, 256U}) {
      const auto key = "packed-zset-" + std::to_string(count);
      const auto members = Members(count);
      if (pass == 0)
        ASSERT_EQ(client.Command(Seed(key, members)).text_,
                  std::to_string(count));
      std::vector<std::size_t> order(count);
      std::iota(order.begin(), order.end(), 0);
      for (const unsigned protocol : {2U, 3U}) {
        SCOPED_TRACE("count=" + std::to_string(count) +
                     " protocol=" + std::to_string(protocol) +
                     " pass=" + std::to_string(pass));
        ASSERT_NE(client.Command({"HELLO", std::to_string(protocol)}).kind_,
                  '-');
        ExpectRange(client.Command({"ZRANGE", key, "0", "-1", "WITHSCORES"}),
                    members, order, true, protocol);
        auto reverse = order;
        std::reverse(reverse.begin(), reverse.end());
        ExpectRange(client.Command({"ZREVRANGE", key, "0", "-1"}), members,
                    reverse, false, protocol);
        ExpectRange(client.Command({"ZRANGE", key, "-3", "-1", "WITHSCORES"}),
                    members, {count - 3, count - 2, count - 1}, true, protocol);
        ExpectRange(client.Command({"ZRANGE", key, "-inf", "+inf", "BYSCORE",
                                    "LIMIT", "1", "3", "WITHSCORES"}),
                    members, {1, 2, 3}, true, protocol);
        ExpectRange(client.Command({"ZREVRANGEBYSCORE", key, "+inf", "-inf",
                                    "LIMIT", "1", "3", "WITHSCORES"}),
                    members, {count - 2, count - 3, count - 4}, true, protocol);
        ExpectRange(client.Command({"ZRANGE", key, "3", "(0", "BYSCORE", "REV",
                                    "LIMIT", "0", "-1"}),
                    members, {3, 2, 1}, false, protocol);
        auto lexical = order;
        std::sort(lexical.begin(), lexical.end(),
                  [&](auto a, auto b) { return members[a] < members[b]; });
        ExpectRange(
            client.Command({"ZRANGEBYLEX", key, "-", "+", "LIMIT", "1", "3"}),
            members, {lexical[1], lexical[2], lexical[3]}, false, protocol);
        std::reverse(lexical.begin(), lexical.end());
        ExpectRange(client.Command({"ZRANGE", key, "+", "-", "BYLEX", "REV",
                                    "LIMIT", "1", "3"}),
                    members, {lexical[1], lexical[2], lexical[3]}, false,
                    protocol);
        for (const auto& command : std::vector<std::vector<std::string>>{
                 {"ZRANGE", key, "1", "0"},
                 {"ZRANGE", key, "-inf", "+inf", "BYSCORE", "LIMIT", "0", "0"},
                 {"ZRANGE", "missing-zset", "0", "-1", "WITHSCORES"}})
          ExpectRange(client.Command(command), members, {}, false, protocol);
        ASSERT_EQ(client.Command({"SET", "wrong-zset", "string"}).text_, "OK");
        const auto wrong = client.Command({"ZRANGE", "wrong-zset", "0", "-1"});
        EXPECT_EQ(wrong.kind_, '-');
        EXPECT_TRUE(wrong.text_.starts_with("WRONGTYPE"));
        // An all-empty block exercises offsets after an SSO block move.
        ASSERT_EQ(client.Command({"ZADD", "empty-zset", "0", ""}).kind_, ':');
        ExpectRange(client.Command({"ZRANGE", "empty-zset", "0", "-1"}), {""},
                    {0}, false, protocol);
      }
    }
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
    EXPECT_EQ(disk.LatestRootGrouped("packed-zset-6"), false);
    EXPECT_EQ(disk.LatestRootGrouped("packed-zset-256"), true);
  }
}

TEST(SortedSetPackedReadE2e, RepliesSurviveExecAndLuaReplacingTheSource) {
  PrivateDisk disk;
  Server server(disk, 3);
  Client client(server.port());
  for (const std::size_t count : {6U, 256U}) {
    const auto members = Members(count);
    std::vector<std::size_t> order(count);
    std::iota(order.begin(), order.end(), 0);
    const auto seed = Seed("source", members);
    for (const unsigned protocol : {2U, 3U}) {
      ASSERT_NE(client.Command({"HELLO", std::to_string(protocol)}).kind_, '-');
      for (const bool scores : {false, true}) {
        SCOPED_TRACE("count=" + std::to_string(count) +
                     " protocol=" + std::to_string(protocol) +
                     " scores=" + std::to_string(scores));
        ASSERT_EQ(client.Command({"DEL", "source"}).kind_, ':');
        ASSERT_EQ(client.Command(seed).text_, std::to_string(count));
        std::vector<std::string> range{"ZRANGE", "source", "0", "-1"};
        if (scores) range.push_back("WITHSCORES");
        ASSERT_EQ(client.Command({"MULTI"}).text_, "OK");
        ASSERT_EQ(client.Command(range).text_, "QUEUED");
        ASSERT_EQ(client.Command({"DEL", "source"}).text_, "QUEUED");
        ASSERT_EQ(client.Command({"SET", "source", "replacement"}).text_,
                  "QUEUED");
        const auto executed = client.Command({"EXEC"});
        ASSERT_EQ(executed.items_.size(), 3);
        ExpectRange(executed.items_[0], members, order, scores, protocol);
        EXPECT_EQ(executed.items_[1].text_, "1");
        EXPECT_EQ(executed.items_[2].text_, "OK");
        ASSERT_EQ(client.Command({"DEL", "source"}).text_, "1");
        ASSERT_EQ(client.Command(seed).text_, std::to_string(count));
        const auto script = "redis.setresp(" + std::to_string(protocol) +
                            "); "
                            "local r=redis.call('ZRANGE',KEYS[1],0,-1" +
                            std::string(scores ? ",'WITHSCORES'" : "") +
                            "); redis.call('DEL',KEYS[1]); "
                            "redis.call('SET',KEYS[1],'replacement'); return r";
        ExpectRange(client.Command({"EVAL", script, "1", "source"}), members,
                    order, scores, protocol);
        EXPECT_EQ(client.Command({"GET", "source"}).text_, "replacement");
      }
      ASSERT_EQ(client.Command({"DEL", "source"}).text_, "1");
      ASSERT_EQ(client.Command(seed).text_, std::to_string(count));
      ASSERT_EQ(client.Command({"PEXPIRE", "source", "-1"}).text_, "1");
      ExpectRange(client.Command({"ZRANGE", "source", "0", "-1", "WITHSCORES"}),
                  members, {}, true, protocol);
    }
  }
}

}  // namespace

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  if (argc != 2) return 2;
  grouped_e2e::server_binary = argv[1];
  return RUN_ALL_TESTS();
}
