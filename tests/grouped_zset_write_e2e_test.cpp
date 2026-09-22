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
#include <cstdlib>
#include <limits>
#include <optional>

#include "grouped_write_e2e_support.h"

namespace {
using namespace grouped_e2e;

// Preserve the failing command and live worker stacks before Server's RAII
// teardown. A socket timeout and a crashed child otherwise share the same
// "response ended early" exception and lose the evidence during unwinding.
class DiagnosedZSetClient : public Client {
 public:
  DiagnosedZSetClient(Server& server, const PrivateDisk& disk)
      : Client(server.port()), server_(server), disk_(disk) {}
  Reply Command(const std::vector<std::string>& args) {
    try {
      return Client::Command(args);
    } catch (const std::exception& error) {
      const int socket_error = errno;
      const std::string context =
          "command=" + args.front() + " argc=" + std::to_string(args.size()) +
          (args.size() > 2 ? " arg2-prefix=" + args[2].substr(0, 32) : "") +
          " errno=" + std::to_string(socket_error) + " disk=" + disk_.path();
      server_.RecordDiagnostics(context);
      throw std::runtime_error(context + ": " + error.what() + "\n" +
                               server_.Log());
    }
  }

 private:
  Server& server_;
  const PrivateDisk& disk_;
};

std::vector<std::string> ZSetSeed(std::string key) {
  std::vector<std::string> command{"ZADD", std::move(key)};
  for (unsigned i = 0; i < 256; ++i) {
    command.push_back(std::to_string(i));
    command.push_back(std::to_string(i) + std::string(128, 'm'));
  }
  return command;
}

TEST(GroupedSortedSetWriteE2e, BatchedMemberIndexChangesMatchOrderedPages) {
  PrivateDisk disk;
  std::map<std::string, double> expected;
  auto member = [](unsigned i) {
    if (i % 3 == 0) return "m" + std::to_string(i);
    if (i % 3 == 1) return std::string(128, 'm') + std::to_string(i);
    return std::string("m\0", 2) + std::to_string(i);
  };
  auto verify = [&](Client& client) {
    std::vector<std::string> lookup{"ZMSCORE", "batched-members"};
    std::vector<std::pair<double, std::string>> ordered;
    for (const auto& [name, score] : expected) {
      lookup.push_back(name);
      ordered.emplace_back(score, name);
    }
    // Prefix reads and ordered reads must agree after updates, insertions and
    // removals spanning many leaves; checking only one graph misses divergence.
    const auto scores = client.Command(lookup);
    ASSERT_EQ(scores.items_.size(), expected.size());
    std::size_t i = 0;
    for (const auto& [name, score] : expected) {
      ASSERT_EQ(scores.items_[i].kind_, '$');
      EXPECT_EQ(std::stod(scores.items_[i++].text_), score) << name;
    }
    std::sort(ordered.begin(), ordered.end());
    const auto range =
        client.Command({"ZRANGE", "batched-members", "0", "-1", "WITHSCORES"});
    ASSERT_EQ(range.items_.size(), 2 * ordered.size());
    for (i = 0; i < ordered.size(); ++i) {
      EXPECT_EQ(range.items_[2 * i].text_, ordered[i].second);
      EXPECT_EQ(std::stod(range.items_[2 * i + 1].text_), ordered[i].first);
    }
    EXPECT_EQ(client.Command({"ZCARD", "batched-members"}).text_,
              std::to_string(expected.size()));
  };
  {
    Server server(disk, 1);
    Client client(server.port());
    std::vector<std::string> seed{"ZADD", "batched-members"};
    for (unsigned i = 0; i < 1024; ++i) {
      seed.insert(seed.end(), {std::to_string(i % 8), member(i)});
      expected[member(i)] = i % 8;
    }
    ASSERT_EQ(client.Command(seed).text_, "1024");
    std::vector<std::string> write{"ZADD", "batched-members"};
    for (unsigned i = 0; i < 512; ++i) {
      write.insert(write.end(), {"20", member(i), "-10", member(i + 1024)});
      expected[member(i)] = 20;
      expected[member(i + 1024)] = -10;
    }
    // Revisit both old and newly inserted identities after many other inputs.
    write.insert(write.end(), {"-30", member(0), "30", member(1024)});
    expected[member(0)] = -30;
    expected[member(1024)] = 30;
    ASSERT_EQ(client.Command(write).text_, "512");
    verify(client);
    ASSERT_EQ(client.Command(write).text_, "0");
    std::vector<std::string> remove{"ZREM", "batched-members"};
    for (unsigned i = 256; i < 768; ++i) {
      remove.push_back(member(i));
      expected.erase(member(i));
    }
    remove.insert(remove.end(), {member(256), "missing"});
    ASSERT_EQ(client.Command({"MULTI"}).text_, "OK");
    ASSERT_EQ(client.Command(remove).text_, "QUEUED");
    const auto executed = client.Command({"EXEC"});
    ASSERT_EQ(executed.items_.size(), 1);
    EXPECT_EQ(executed.items_[0].text_, "512");
    EXPECT_EQ(client.Command({"ZSCORE", "batched-members", member(256)}).text_,
              "-1");
    verify(client);
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  ASSERT_FALSE(disk.Auxiliaries("batched-members").empty());
  Server recovered(disk, 3);
  Client client(recovered.port());
  verify(client);
}

TEST(GroupedSortedSetWriteE2e, MemberScoresUsePrefixPagesWithoutOrderedReads) {
#if !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires ordered-read failure injection";
#endif
  PrivateDisk disk;
  {
    Server server(disk, 2);
    Client client(server.port());
    ASSERT_EQ(client.Command(ZSetSeed("indexed")).text_, "256");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  const auto records = disk.Auxiliaries("indexed");
  ASSERT_FALSE(records.empty());
  std::size_t ordered = 0, prefixes = 0;
  for (const auto& [id, bits] : records.rbegin()->second)
    (bits == 0 && id != 0 ? ordered : prefixes)++;
  EXPECT_GT(ordered, 1);
  EXPECT_GT(prefixes, 1);

  struct Fault {
    std::optional<std::string> previous_;
    Fault() {
      if (const char* value = std::getenv("LAVIK_FAIL_ZSET_ORDERED_READ_KEY"))
        previous_ = value;
      ::setenv("LAVIK_FAIL_ZSET_ORDERED_READ_KEY", "indexed", 1);
    }
    ~Fault() {
      if (previous_)
        ::setenv("LAVIK_FAIL_ZSET_ORDERED_READ_KEY", previous_->c_str(), 1);
      else
        ::unsetenv("LAVIK_FAIL_ZSET_ORDERED_READ_KEY");
    }
  } fault;
  Server recovered(disk, 3);
  Client client(recovered.port());
  const auto member = "128" + std::string(128, 'm');
  EXPECT_EQ(client.Command({"ZSCORE", "indexed", member}).text_, "128");
  auto scores =
      client.Command({"ZMSCORE", "indexed", member, "missing", member});
  ASSERT_EQ(scores.items_.size(), 3);
  EXPECT_EQ(scores.items_[0].text_, "128");
  EXPECT_EQ(scores.items_[1].text_, "-1");
  EXPECT_EQ(scores.items_[2].text_, "128");
  const auto range = client.Command({"ZRANGE", "indexed", "0", "0"});
  EXPECT_EQ(range.kind_, '-');
  EXPECT_NE(range.text_.find("injected ordered-page"), std::string::npos);
  EXPECT_EQ(client.Command({"ZCARD", "indexed"}).text_, "256");
  EXPECT_EQ(recovered.Wait(true), 0) << recovered.Log();
}

TEST(GroupedSortedSetWriteE2e, ScoreBoundsSkipUnrelatedPagesAfterRecovery) {
#if !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires selected ordered-page failure injection";
#endif
  PrivateDisk disk;
  {
    Server server(disk, 1);
    Client client(server.port());
    ASSERT_EQ(client.Command(ZSetSeed("routed")).text_, "256");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  struct Fault {
    std::optional<std::string> key_, page_;
    Fault() {
      if (const char* value = std::getenv("LAVIK_FAIL_ZSET_ORDERED_READ_KEY"))
        key_ = value;
      if (const char* value = std::getenv("LAVIK_FAIL_ZSET_ORDERED_READ_PAGE"))
        page_ = value;
      ::setenv("LAVIK_FAIL_ZSET_ORDERED_READ_KEY", "routed", 1);
      ::setenv("LAVIK_FAIL_ZSET_ORDERED_READ_PAGE", "1", 1);
    }
    ~Fault() {
      if (key_)
        ::setenv("LAVIK_FAIL_ZSET_ORDERED_READ_KEY", key_->c_str(), 1);
      else
        ::unsetenv("LAVIK_FAIL_ZSET_ORDERED_READ_KEY");
      if (page_)
        ::setenv("LAVIK_FAIL_ZSET_ORDERED_READ_PAGE", page_->c_str(), 1);
      else
        ::unsetenv("LAVIK_FAIL_ZSET_ORDERED_READ_PAGE");
    }
  } fault;
  // Changing worker count forces recovery/physical owner reassignment too.
  Server recovered(disk, 3);
  Client client(recovered.port());
  const auto member = "220" + std::string(128, 'm');
  EXPECT_EQ(client.Command({"ZINCRBY", "routed", "0.25", member}).text_,
            "220.25");
  auto range = client.Command({"ZRANGEBYSCORE", "routed", "220", "221"});
  ASSERT_EQ(range.items_.size(), 2) << range.text_;
  EXPECT_EQ(range.items_[0].text_, member);
  EXPECT_EQ(client.Command({"ZCOUNT", "routed", "200", "230"}).text_, "31");
  EXPECT_EQ(
      client.Command({"ZREM", "routed", "240" + std::string(128, 'm')}).text_,
      "1");
  // Negative control: the fault must be armed, not merely skipped by Release.
  const auto first = client.Command({"ZRANGE", "routed", "0", "0"});
  EXPECT_EQ(first.kind_, '-');
  EXPECT_NE(first.text_.find("injected ordered-page"), std::string::npos);
  client.Durable();
  EXPECT_EQ(recovered.Wait(true), 0) << recovered.Log();
}

TEST(GroupedSortedSetWriteE2e, ScoreBoundsHandleLongTieRunsAndMixedBatchMoves) {
  PrivateDisk disk;
  std::vector<std::string> members;
  std::map<std::string, double> expected;
  std::vector<std::string> seed{"ZADD", "ties"};
  for (unsigned i = 0; i < 64; ++i) {
    // Every member exceeds the page target: the equal-score run necessarily
    // spans many pages, rather than only exercising a page-local tie search.
    members.push_back(std::to_string(1000 + i) + std::string(20 * 1024, 'x'));
    const double score = i < 8 ? -1 : i < 56 ? 7 : 20;
    expected[members.back()] = score;
    seed.push_back(std::to_string(score));
    seed.push_back(members.back());
  }
  auto check = [&](Client& client) {
    std::vector<std::pair<double, std::string>> sorted;
    for (const auto& [member, score] : expected)
      sorted.emplace_back(score, member);
    std::sort(sorted.begin(), sorted.end());
    const auto all =
        client.Command({"ZRANGE", "ties", "0", "-1", "WITHSCORES"});
    ASSERT_EQ(all.kind_, '*') << all.text_;
    ASSERT_EQ(all.items_.size(), 2 * sorted.size());
    for (std::size_t i = 0; i < sorted.size(); ++i) {
      EXPECT_EQ(all.items_[2 * i].text_, sorted[i].second) << "rank " << i;
      EXPECT_EQ(std::stod(all.items_[2 * i + 1].text_), sorted[i].first);
    }
    const auto tied = client.Command({"ZRANGEBYSCORE", "ties", "7", "7"});
    std::vector<std::string> at_seven;
    std::size_t above_seven = 0;
    for (const auto& [score, member] : sorted) {
      if (score == 7) at_seven.push_back(member);
      if (score > 7 && score <= 20) ++above_seven;
    }
    ASSERT_EQ(tied.items_.size(), at_seven.size()) << tied.text_;
    for (std::size_t i = 0; i < at_seven.size(); ++i)
      EXPECT_EQ(tied.items_[i].text_, at_seven[i]);
    EXPECT_EQ(client.Command({"ZCOUNT", "ties", "(7", "20"}).text_,
              std::to_string(above_seven));
    EXPECT_EQ(client.Command({"ZCOUNT", "ties", "7", "(7"}).text_, "0");
    EXPECT_EQ(client.Command({"ZCARD", "ties"}).text_,
              std::to_string(expected.size()));
  };
  {
    Server server(disk, 1);
    Client client(server.port());
    ASSERT_EQ(client.Command(seed).text_, "64");
    check(client);
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  {
    Server server(disk, 3);
    Client client(server.port());
    check(client);
    // Preserve binary member ordering when inserting into an existing tie run.
    const auto fresh = members[30] + std::string(1, '\0');
    ASSERT_EQ(client
                  .Command({"ZADD", "ties", "CH", "-inf", members[63], "7",
                            members[0], "20", members[10], "7", fresh})
                  .text_,
              "4");
    expected[members[63]] = -std::numeric_limits<double>::infinity();
    expected[members[0]] = 7;
    expected[members[10]] = 20;
    expected[fresh] = 7;
    check(client);
    ASSERT_EQ(client
                  .Command({"ZADD", "ties", "CH", "7", members[10], "20",
                            members[10], "7", members[10]})
                  .text_,
              "3");
    expected[members[10]] = 7;
    ASSERT_EQ(client
                  .Command({"ZREM", "ties", members[24], members[31],
                            members[24], "missing"})
                  .text_,
              "2");
    expected.erase(members[24]);
    expected.erase(members[31]);
    ASSERT_EQ(client
                  .Command({"ZADD", "ties", "CH", "+inf", members[62], "-0",
                            members[3], "+0", members[4]})
                  .text_,
              "3");
    expected[members[62]] = std::numeric_limits<double>::infinity();
    expected[members[3]] = expected[members[4]] = 0;
    ASSERT_EQ(client.Command({"ZINCRBY", "ties", "1", members[20]}).text_, "8");
    expected[members[20]] = 8;
    check(client);
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server recovered(disk, 2);
  Client client(recovered.port());
  check(client);
  ASSERT_EQ(client.Command({"ZADD", "ties", "CH", "6.5", members[48]}).text_,
            "1");
  expected[members[48]] = 6.5;
  check(client);
}

TEST(GroupedSortedSetWriteE2e, ScoreBoundsRecoverAfterMultiExtentParentKey) {
  PrivateDisk disk;
  // The parent key consumes an entire extent before the ordered encoding
  // begins. Recovery must checksum but not feed those key bytes to the score
  // decoder, including when the runtime assigns new physical owners.
  const std::string key(9 * 1024 * 1024, 'K');
  const std::string first(9000, 'a'), middle(9000, 'b'), last(9000, 'c');
  {
    Server server(disk, 1);
    Client client(server.port());
    ASSERT_EQ(
        client.Command({"ZADD", key, "10", first, "20", middle, "30", last})
            .text_,
        "3");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  {
    Server recovered(disk, 3);
    Client client(recovered.port());
    ASSERT_EQ(client.Command({"ZINCRBY", key, "1", last}).text_, "31");
    const auto range = client.Command({"ZRANGEBYSCORE", key, "30", "32"});
    ASSERT_EQ(range.items_.size(), 1) << range.text_;
    EXPECT_EQ(range.items_[0].text_, last);
    EXPECT_EQ(client.Command({"ZCOUNT", key, "(10", "31"}).text_, "2");
    client.Durable();
    ASSERT_EQ(recovered.Wait(true), 0) << recovered.Log();
  }
  Server recovered(disk, 2);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"ZINCRBY", key, "-1", last}).text_, "30");
  EXPECT_EQ(client.Command({"ZCARD", key}).text_, "3");
}

TEST(GroupedSortedSetWriteE2e, FailedMemberWriteCannotCommitOrderedHalf) {
#if !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires auxiliary-write failure injection";
#endif
  PrivateDisk disk;
  const std::string member(9 * 1024 * 1024, 'I');
  {
    Server server(disk, 2);
    Client client(server.port());
    ASSERT_EQ(client.Command({"ZADD", "indexed{undo}", "1", member}).text_,
              "1");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  {
    // One ordered page is staged first. Fail on the member-index page, then
    // commit a later EXEC command: the staged half must never become visible.
    Server server(disk, 3, {}, "indexed{undo}", false, 2);
    Client client(server.port());
    ASSERT_EQ(client.Command({"MULTI"}).text_, "OK");
    ASSERT_EQ(client.Command({"ZINCRBY", "indexed{undo}", "10", member}).text_,
              "QUEUED");
    ASSERT_EQ(client.Command({"SET", "guard{undo}", "after"}).text_, "QUEUED");
    auto reply = client.Command({"EXEC"});
    ASSERT_EQ(reply.items_.size(), 2) << reply.text_ << server.Log();
    EXPECT_EQ(reply.items_[0].kind_, '-');
    EXPECT_TRUE(reply.items_[0].text_.starts_with("OOM"));
    EXPECT_EQ(reply.items_[1].text_, "OK");
    EXPECT_EQ(client.Command({"ZSCORE", "indexed{undo}", member}).text_, "1");
    auto range =
        client.Command({"ZRANGE", "indexed{undo}", "0", "0", "WITHSCORES"});
    ASSERT_EQ(range.items_.size(), 2);
    EXPECT_EQ(range.items_[0].text_, member);
    EXPECT_EQ(range.items_[1].text_, "1");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server recovered(disk, 4);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"ZSCORE", "indexed{undo}", member}).text_, "1");
  EXPECT_EQ(client.Command({"GET", "guard{undo}"}).text_, "after");
  EXPECT_EQ(client.Command({"ZREM", "indexed{undo}", member}).text_, "1");
  EXPECT_EQ(client.Command({"ZADD", "indexed{undo}", "2", "replacement"}).text_,
            "1");
  EXPECT_EQ(client.Command({"ZSCORE", "indexed{undo}", member}).text_, "-1");
  EXPECT_EQ(recovered.Wait(true), 0) << recovered.Log();
}

TEST(GroupedSortedSetWriteE2e, PointFlagsDuplicatesScoresAndAtomicNan) {
  PrivateDisk disk;
  Server server(disk);
  Client client(server.port());
  ASSERT_EQ(client.Command(ZSetSeed("zset")).text_, "256");
  ASSERT_EQ(client.Command({"EXPIRE", "zset", "3600"}).text_, "1");
  EXPECT_EQ(client.Command({"ZADD", "zset", "CH", "1", "x", "2", "x"}).text_,
            "2");
  EXPECT_EQ(client.Command({"ZADD", "zset", "NX", "3", "x"}).text_, "0");
  EXPECT_EQ(client.Command({"ZADD", "zset", "XX", "3", "missing"}).text_, "0");
  EXPECT_EQ(client.Command({"ZADD", "zset", "GT", "CH", "1", "x"}).text_, "0");
  EXPECT_EQ(client.Command({"ZADD", "zset", "LT", "CH", "1", "x"}).text_, "1");
  EXPECT_EQ(client.Command({"ZADD", "zset", "NX", "INCR", "2", "x"}).text_,
            "-1");
  EXPECT_EQ(client.Command({"ZINCRBY", "zset", "2", "x"}).text_, "3");
  auto scores = client.Command({"ZMSCORE", "zset", "x", "missing", "x"});
  ASSERT_EQ(scores.items_.size(), 3);
  EXPECT_EQ(scores.items_[0].text_, "3");
  EXPECT_EQ(scores.items_[1].text_, "-1");
  EXPECT_EQ(scores.items_[2].text_, "3");
  EXPECT_EQ(client.Command({"ZADD", "zset", "inf", "infinity"}).text_, "1");
  EXPECT_EQ(client.Command({"ZINCRBY", "zset", "-inf", "infinity"}).kind_, '-');
  EXPECT_EQ(client.Command({"ZSCORE", "zset", "infinity"}).text_, "inf");
  EXPECT_EQ(client.Command({"ZREM", "zset", "x", "x", "missing"}).text_, "1");
  EXPECT_EQ(client.Command({"ZCARD", "zset"}).text_, "257");
  EXPECT_GT(std::stoll(client.Command({"TTL", "zset"}).text_), 0);
  EXPECT_EQ(client.Command({"SET", "string", "wrongtype"}).text_, "OK");
  EXPECT_TRUE(
      client.Command({"ZSCORE", "string", "x"}).text_.starts_with("WRONGTYPE"));
}

TEST(GroupedSortedSetWriteE2e, PointScoreMovesRewriteOnlyEndpointPages) {
  PrivateDisk disk;
  const auto low = "0" + std::string(128, 'm');
  const auto high = "255" + std::string(128, 'm');
  {
    Server server(disk);
    Client client(server.port());
    ASSERT_EQ(client.Command(ZSetSeed("zset")).text_, "256");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  const auto baseline = disk.Auxiliaries("zset").rbegin()->first;
  {
    Server server(disk, 3);
    Client client(server.port());
    ASSERT_EQ(client.Command({"ZINCRBY", "zset", "10000", low}).text_, "10000");
    ASSERT_EQ(client.Command({"ZINCRBY", "zset", "-10000", high}).text_,
              "-9745");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  unsigned mutations = 0;
  for (const auto& [revision, ids] : disk.Auxiliaries("zset")) {
    if (revision <= baseline) continue;
    ++mutations;
    EXPECT_LE(ids.size(), 6);
  }
  EXPECT_EQ(mutations, 2);
  Server recovered(disk, 4);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"ZSCORE", "zset", low}).text_, "10000");
  EXPECT_EQ(client.Command({"ZSCORE", "zset", high}).text_, "-9745");
  auto range = client.Command({"ZRANGE", "zset", "0", "0"});
  ASSERT_EQ(range.items_.size(), 1);
  EXPECT_EQ(range.items_[0].text_, high);
}

TEST(GroupedSortedSetWriteE2e, BoundedRangeAliasesRanksBoundsAndMixedScoreLex) {
  PrivateDisk disk;
  Server server(disk);
  Client client(server.port());
  ASSERT_EQ(client.Command(ZSetSeed("zset")).text_, "256");
  ASSERT_EQ(
      client.Command({"ZADD", "zset", "9", "a", "1", "b", "5", "c"}).text_,
      "3");
  auto lexical = client.Command({"ZRANGE", "zset", "[a", "[c", "BYLEX"});
  ASSERT_EQ(lexical.items_.size(), 3);
  EXPECT_EQ(lexical.items_[0].text_, "a");
  EXPECT_EQ(lexical.items_[1].text_, "b");
  EXPECT_EQ(lexical.items_[2].text_, "c");
  auto reverse_lex =
      client.Command({"ZREVRANGEBYLEX", "zset", "[c", "[a", "LIMIT", "1", "1"});
  ASSERT_EQ(reverse_lex.items_.size(), 1);
  EXPECT_EQ(reverse_lex.items_[0].text_, "b");
  auto score = client.Command({"ZRANGE", "zset", "9", "(8", "BYSCORE", "REV",
                               "LIMIT", "1", "1", "WITHSCORES"});
  ASSERT_EQ(score.items_.size(), 2);
  EXPECT_EQ(score.items_[0].text_, "9" + std::string(128, 'm'));
  EXPECT_EQ(score.items_[1].text_, "9");
  auto legacy_score = client.Command(
      {"ZRANGEBYSCORE", "zset", "(8", "9", "WITHSCORES", "LIMIT", "1", "1"});
  ASSERT_EQ(legacy_score.items_.size(), 2);
  EXPECT_EQ(legacy_score.items_[0].text_, "a");
  EXPECT_EQ(legacy_score.items_[1].text_, "9");
  auto last = client.Command({"ZRANGE", "zset", "-1", "-1", "WITHSCORES"});
  ASSERT_EQ(last.items_.size(), 2);
  EXPECT_EQ(last.items_[0].text_, "255" + std::string(128, 'm'));
  EXPECT_EQ(last.items_[1].text_, "255");
  auto rank = client.Command(
      {"ZREVRANK", "zset", "255" + std::string(128, 'm'), "WITHSCORE"});
  ASSERT_EQ(rank.items_.size(), 2);
  EXPECT_EQ(rank.items_[0].text_, "0");
  EXPECT_EQ(rank.items_[1].text_, "255");
  EXPECT_EQ(client.Command({"ZRANK", "zset", "missing"}).text_, "-1");
  EXPECT_EQ(client.Command({"ZCOUNT", "zset", "(8", "9"}).text_, "2");
  EXPECT_EQ(client.Command({"ZLEXCOUNT", "zset", "(a", "[c"}).text_, "2");
  EXPECT_TRUE(client
                  .Command({"ZRANGE", "zset", "-inf", "+inf", "BYSCORE",
                            "LIMIT", "-1", "1"})
                  .items_.empty());
  EXPECT_TRUE(
      client.Command({"ZRANGE", "zset", "-", "+", "BYLEX", "LIMIT", "0", "0"})
          .items_.empty());
  EXPECT_EQ(
      client.Command({"ZRANGE", "zset", "0", "1", "LIMIT", "0", "1"}).kind_,
      '-');
  EXPECT_EQ(
      client.Command({"ZRANGE", "zset", "-", "+", "BYLEX", "WITHSCORES"}).kind_,
      '-');
}

TEST(GroupedSortedSetWriteE2e, ScanCursorsAndAllPopEntryPointsRecover) {
  PrivateDisk disk;
  auto member = [](unsigned i) {
    return std::to_string(i) + std::string(128, 'm');
  };
  {
    Server server(disk, 3);
    Client client(server.port());
    ASSERT_EQ(client.Command(ZSetSeed("zset")).text_, "256");
    EXPECT_EQ(client.Command({"EXPIRE", "zset", "3600"}).text_, "1");
    std::set<std::string> seen;
    std::uint64_t cursor = 0;
    for (unsigned step = 0; step < 256; ++step) {
      auto scan = client.Command(
          {"ZSCAN", "zset", std::to_string(cursor), "COUNT", "7"});
      ASSERT_EQ(scan.items_.size(), 2) << scan.text_;
      ASSERT_EQ(scan.items_[1].items_.size() % 2, 0);
      for (std::size_t i = 0; i < scan.items_[1].items_.size(); i += 2) {
        const auto& entry = scan.items_[1].items_[i];
        EXPECT_TRUE(seen.insert(entry.text_).second);
        EXPECT_EQ(entry.text_,
                  member(std::stoul(scan.items_[1].items_[i + 1].text_)));
      }
      const auto next = std::stoull(scan.items_[0].text_);
      if (next == 0) break;
      EXPECT_GT(next, cursor);
      cursor = next;
    }
    EXPECT_EQ(seen.size(), 256);
    auto filtered = client.Command(
        {"ZSCAN", "zset", "0", "COUNT", "1", "MATCH", "not-present*"});
    ASSERT_EQ(filtered.items_.size(), 2);
    EXPECT_NE(filtered.items_[0].text_, "0");
    EXPECT_TRUE(filtered.items_[1].items_.empty());
    EXPECT_EQ(client.Command({"ZSCAN", "zset", "bad"}).kind_, '-');
    EXPECT_EQ(client.Command({"ZSCAN", "zset", "0", "COUNT", "0"}).kind_, '-');
    EXPECT_TRUE(client.Command({"ZPOPMIN", "zset", "0"}).items_.empty());
    EXPECT_EQ(client.Command({"ZPOPMAX", "zset", "-1"}).kind_, '-');
    auto minimum = client.Command({"ZPOPMIN", "zset", "2"});
    ASSERT_EQ(minimum.items_.size(), 4);
    EXPECT_EQ(minimum.items_[0].text_, member(0));
    EXPECT_EQ(minimum.items_[2].text_, member(1));
    auto maximum = client.Command({"ZPOPMAX", "zset", "2"});
    ASSERT_EQ(maximum.items_.size(), 4);
    EXPECT_EQ(maximum.items_[0].text_, member(255));
    EXPECT_EQ(maximum.items_[2].text_, member(254));
    auto multi =
        client.Command({"ZMPOP", "2", "absent", "zset", "MAX", "COUNT", "2"});
    ASSERT_EQ(multi.items_.size(), 2);
    EXPECT_EQ(multi.items_[0].text_, "zset");
    ASSERT_EQ(multi.items_[1].items_.size(), 2);
    EXPECT_EQ(multi.items_[1].items_[0].items_[0].text_, member(253));
    auto blocking = client.Command({"BZPOPMIN", "absent", "zset", "0.1"});
    ASSERT_EQ(blocking.items_.size(), 3);
    EXPECT_EQ(blocking.items_[1].text_, member(2));
    auto blocking_multi =
        client.Command({"BZMPOP", "0.1", "2", "absent", "zset", "MIN"});
    ASSERT_EQ(blocking_multi.items_.size(), 2);
    EXPECT_EQ(blocking_multi.items_[1].items_[0].items_[0].text_, member(3));
    EXPECT_EQ(client.Command({"MULTI"}).text_, "OK");
    EXPECT_EQ(client.Command({"ZPOPMIN", "zset", "2"}).text_, "QUEUED");
    EXPECT_EQ(client.Command({"ZMPOP", "1", "zset", "MAX"}).text_, "QUEUED");
    EXPECT_EQ(client.Command({"ZSCAN", "zset", "0", "COUNT", "1"}).text_,
              "QUEUED");
    auto executed = client.Command({"EXEC"});
    ASSERT_EQ(executed.items_.size(), 3);
    EXPECT_EQ(executed.items_[0].items_[0].text_, member(4));
    EXPECT_EQ(executed.items_[1].items_[1].items_[0].items_[0].text_,
              member(251));
    EXPECT_EQ(executed.items_[2].items_.size(), 2);
    EXPECT_EQ(client.Command({"ZCARD", "zset"}).text_, "245");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server recovered(disk, 4);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"ZCARD", "zset"}).text_, "245");
  EXPECT_GT(std::stoll(client.Command({"TTL", "zset"}).text_), 0);
  auto minimum = client.Command({"ZPOPMIN", "zset"});
  ASSERT_EQ(minimum.items_.size(), 2);
  EXPECT_EQ(minimum.items_[0].text_, member(6));
  EXPECT_TRUE(client.Command({"ZPOPMAX", "absent"}).items_.empty());
  EXPECT_EQ(client.Command({"SET", "wrong", "type"}).text_, "OK");
  EXPECT_TRUE(
      client.Command({"ZSCAN", "wrong", "0"}).text_.starts_with("WRONGTYPE"));
}

TEST(GroupedSortedSetWriteE2e, LegacyFullImageAndRandomReplyOomAreAtomic) {
  PrivateDisk disk;
  const std::string payload(4 * 1024 * 1024, 'O');
  auto member = [&](unsigned i) { return std::to_string(i) + payload; };
  const std::string small(128 * 1024, 's');
  {
    Server server(disk);
    Client client(server.port());
    for (unsigned i = 0; i < 4; ++i)
      ASSERT_EQ(
          client.Command({"ZADD", "large", std::to_string(i), member(i)}).text_,
          "1");
    ASSERT_EQ(client.Command({"ZADD", "small", "1", small}).text_, "1");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  {
    // The legacy loader alone could admit this 16 MiB value. The decoded
    // callback/planner copies must also fit, while bounded score/card reads
    // and ordinary String commands remain usable on the same worker budget.
    Server server(disk, 2, {}, {}, false, 2, "128M", {}, "128M");
    Client client(server.port());
    auto expect_oom = [&](const std::vector<std::string>& command) {
      const auto reply = client.Command(command);
      EXPECT_EQ(reply.kind_, '-') << command.front() << ": " << reply.text_;
      EXPECT_TRUE(reply.text_.starts_with("OOM "))
          << command.front() << ": " << reply.text_;
    };
    for (const auto* operation : {"ZREMRANGEBYRANK", "ZREMRANGEBYSCORE"})
      expect_oom({operation, "large", "0", "0"});
    expect_oom({"ZREMRANGEBYLEX", "large", "-", "+"});
    expect_oom({"ZRANDMEMBER", "large"});
    expect_oom({"GEOPOS", "large", member(0)});
    expect_oom({"GEOSEARCH", "large", "FROMLONLAT", "0", "0", "BYRADIUS", "1",
                "km", "COUNT", "1"});
    EXPECT_EQ(client.Command({"ZCARD", "large"}).text_, "4");
    EXPECT_EQ(client.Command({"ZSCORE", "large", member(0)}).text_, "0");
    // Up to 1000 samples use the materialized reply adapter; larger negative
    // counts use a separate bounded stream and are not a total-wire OOM.
    const auto overflow = client.Command(
        {"ZRANDMEMBER", "small", "-9223372036854775807", "WITHSCORES"});
    EXPECT_EQ(overflow.kind_, '-');
    expect_oom({"ZRANDMEMBER", "small", "-1000"});
    auto empty = client.Command({"ZRANDMEMBER", "small", "0"});
    EXPECT_EQ(empty.kind_, '*');
    EXPECT_TRUE(empty.items_.empty());
    auto repeated =
        client.Command({"ZRANDMEMBER", "small", "-3", "WITHSCORES"});
    ASSERT_EQ(repeated.items_.size(), 6);
    for (unsigned i = 0; i < 3; ++i) {
      EXPECT_EQ(repeated.items_[2 * i].text_, small);
      EXPECT_EQ(repeated.items_[2 * i + 1].text_, "1");
    }
    ASSERT_EQ(client.Command({"MULTI"}).text_, "OK");
    ASSERT_EQ(client.Command({"ZREMRANGEBYRANK", "large", "0", "0"}).text_,
              "QUEUED");
    ASSERT_EQ(client.Command({"SET", "after-oom", "ok"}).text_, "QUEUED");
    auto executed = client.Command({"EXEC"});
    ASSERT_EQ(executed.items_.size(), 2);
    EXPECT_EQ(executed.items_[0].kind_, '-');
    EXPECT_TRUE(executed.items_[0].text_.starts_with("OOM "));
    EXPECT_EQ(executed.items_[1].text_, "OK");
    EXPECT_EQ(client.Command({"GET", "after-oom"}).text_, "ok");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server recovered(disk, 3);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"ZCARD", "large"}).text_, "4");
  for (unsigned i = 0; i < 4; ++i)
    EXPECT_EQ(client.Command({"ZSCORE", "large", member(i)}).text_,
              std::to_string(i));
  EXPECT_EQ(client.Command({"GET", "after-oom"}).text_, "ok");
}

TEST(GroupedSortedSetWriteE2e,
     MultiKeyAggregateOomPreservesStoreAndExecPrefix) {
  PrivateDisk disk;
  disk.PreserveOnFailure();
  const std::string payload(1024 * 1024, 'A');
  std::vector<std::string> sources;
  for (unsigned i = 0; i < 8; ++i)
    sources.push_back("source-" + std::to_string(i));
  sources.push_back("set-source");
  auto aggregate = [&](std::string operation, bool store) {
    std::vector<std::string> command{std::move(operation)};
    if (store) command.push_back("target");
    command.push_back(std::to_string(sources.size()));
    command.insert(command.end(), sources.begin(), sources.end());
    return command;
  };
  {
    Server server(disk, 3);
    server.PreserveOnFailure();
    DiagnosedZSetClient client(server, disk);
    for (unsigned i = 0; i < 8; ++i)
      ASSERT_EQ(client
                    .Command({"ZADD", sources[i], std::to_string(i),
                              std::to_string(i) + payload})
                    .text_,
                "1");
    ASSERT_EQ(client.Command({"SADD", sources.back(), "set-" + payload}).text_,
              "1");
    auto stored = aggregate("ZUNIONSTORE", true);
    stored[1] = "large-output";
    ASSERT_EQ(client.Command(stored).text_, "9");
    ASSERT_EQ(client.Command({"ZCARD", "large-output"}).text_, "9");
    ASSERT_EQ(client.Command({"SET", "target", "before"}).text_, "OK");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  {
    // Every source individually fits; their retained inputs plus the map and
    // result must still be admitted on the computing worker before STORE.
    Server server(disk, 2, {}, {}, false, 2, "64M", {}, "32M");
    server.PreserveOnFailure();
    DiagnosedZSetClient client(server, disk);
    for (const auto* operation : {"ZUNION", "ZINTER", "ZDIFF"}) {
      const auto reply = client.Command(aggregate(operation, false));
      EXPECT_EQ(reply.kind_, '-') << reply.text_;
      EXPECT_TRUE(reply.text_.starts_with("OOM ")) << reply.text_;
      EXPECT_NE(reply.text_.find("aggregate admission"), std::string::npos)
          << reply.text_;
    }
    for (const auto* operation : {"ZUNIONSTORE", "ZINTERSTORE", "ZDIFFSTORE"}) {
      const auto reply = client.Command(aggregate(operation, true));
      EXPECT_EQ(reply.kind_, '-') << reply.text_;
      EXPECT_TRUE(reply.text_.starts_with("OOM ")) << reply.text_;
      EXPECT_EQ(client.Command({"GET", "target"}).text_, "before");
    }
    for (const auto& command : std::vector<std::vector<std::string>>{
             {"ZRANGESTORE", "target", "large-output", "0", "0"},
             {"GEOSEARCHSTORE", "target", "large-output", "FROMLONLAT", "0",
              "0", "BYRADIUS", "20000", "km", "COUNT", "1"}}) {
      const auto reply = client.Command(command);
      EXPECT_EQ(reply.kind_, '-') << reply.text_;
      EXPECT_TRUE(reply.text_.starts_with("OOM ")) << reply.text_;
      EXPECT_EQ(client.Command({"GET", "target"}).text_, "before");
    }
    ASSERT_EQ(client.Command({"MULTI"}).text_, "OK");
    ASSERT_EQ(client.Command({"SET", "target", "outer-prefix"}).text_,
              "QUEUED");
    ASSERT_EQ(client.Command(aggregate("ZUNIONSTORE", true)).text_, "QUEUED");
    ASSERT_EQ(client.Command({"SET", "after-multi-oom", "ok"}).text_, "QUEUED");
    const auto result = client.Command({"EXEC"});
    ASSERT_EQ(result.items_.size(), 3);
    EXPECT_EQ(result.items_[0].text_, "OK");
    EXPECT_EQ(result.items_[1].kind_, '-');
    EXPECT_TRUE(result.items_[1].text_.starts_with("OOM "))
        << result.items_[1].text_;
    EXPECT_EQ(result.items_[2].text_, "OK");
    EXPECT_EQ(client.Command({"GET", "target"}).text_, "outer-prefix");
    EXPECT_EQ(client.Command({"ZCARD", "large-output"}).text_, "9");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server recovered(disk, 4);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"GET", "target"}).text_, "outer-prefix");
  EXPECT_EQ(client.Command({"GET", "after-multi-oom"}).text_, "ok");
  EXPECT_EQ(client.Command({"ZCARD", "large-output"}).text_, "9");
  for (unsigned i = 0; i < 8; ++i)
    EXPECT_EQ(client.Command({"ZCARD", sources[i]}).text_, "1");
  EXPECT_EQ(client.Command({"SCARD", sources.back()}).text_, "1");
}

TEST(GroupedSortedSetWriteE2e, AggregateAbove512MiBRemainsBoundedAndRecovers) {
  // A private, RAII-owned sparse file, never an existing user device.
  PrivateDisk disk(4ULL * 1024 * 1024 * 1024);
  disk.PreserveOnFailure();
  auto member = [](unsigned i) {
    return std::to_string(i) + ":" + std::string(9 * 1024 * 1024, 'L');
  };
  {
    // The request-buffer limit is a startup option, not a mutable CONFIG key.
    // Keep it independent of the intentionally constrained storage budget.
    Server server(disk, 2, {}, {}, false, 2, "512M", {}, "128M");
    server.PreserveOnFailure();
    DiagnosedZSetClient client(server, disk);
    for (unsigned i = 0; i < 64; ++i)
      ASSERT_EQ(
          client.Command({"ZADD", "large", std::to_string(i), member(i)}).text_,
          "1")
          << "member=" << i << server.Log();
    ASSERT_EQ(client.Command({"ZCARD", "large"}).text_, "64");
    ASSERT_EQ(client.Command({"ZINCRBY", "large", "1000", member(0)}).text_,
              "1000");
    ASSERT_EQ(client.Command({"ZREM", "large", member(32)}).text_, "1");
    ASSERT_EQ(client.Command({"ZADD", "large", "-10", member(64)}).text_, "1");
    auto scores = client.Command({"ZMSCORE", "large", member(0), "missing"});
    ASSERT_EQ(scores.items_.size(), 2);
    EXPECT_EQ(scores.items_[0].text_, "1000");
    EXPECT_EQ(scores.items_[1].text_, "-1");
    EXPECT_EQ(client.Command({"ZSCORE", "large", member(64)}).text_, "-10");
    auto first = client.Command({"ZRANGE", "large", "0", "0", "WITHSCORES"});
    ASSERT_EQ(first.items_.size(), 2);
    EXPECT_EQ(first.items_[0].text_, member(64));
    EXPECT_EQ(first.items_[1].text_, "-10");
    auto score_range = client.Command(
        {"ZRANGE", "large", "-inf", "+inf", "BYSCORE", "LIMIT", "1", "1"});
    ASSERT_EQ(score_range.items_.size(), 1);
    EXPECT_EQ(score_range.items_[0].text_, member(1));
    auto lexical = client.Command(
        {"ZRANGE", "large", "-", "+", "BYLEX", "LIMIT", "1", "1"});
    ASSERT_EQ(lexical.items_.size(), 1);
    EXPECT_EQ(lexical.items_[0].text_, member(10));
    auto reverse_lex = client.Command(
        {"ZRANGE", "large", "+", "-", "BYLEX", "REV", "LIMIT", "0", "1"});
    ASSERT_EQ(reverse_lex.items_.size(), 1);
    EXPECT_EQ(reverse_lex.items_[0].text_, member(9));
    EXPECT_EQ(client.Command({"ZRANK", "large", member(0)}).text_, "63");
    EXPECT_EQ(client.Command({"ZREVRANK", "large", member(0)}).text_, "0");
    EXPECT_EQ(client.Command({"ZCOUNT", "large", "(1", "3"}).text_, "2");
    EXPECT_EQ(client.Command({"ZLEXCOUNT", "large", "-", "+"}).text_, "64");
    auto scan = client.Command({"ZSCAN", "large", "0", "COUNT", "1"});
    ASSERT_EQ(scan.items_.size(), 2);
    ASSERT_EQ(scan.items_[1].items_.size(), 2);
    EXPECT_NE(scan.items_[0].text_, "0");
    const auto returned_id = std::stoul(scan.items_[1].items_[0].text_);
    EXPECT_EQ(scan.items_[1].items_[0].text_, member(returned_id));
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  {
    // One page fits; the cross-end write's selected-page scratch does not.
    // Reject before staging any auxiliary/root and keep the previous view.
    Server limited(disk, 2, {}, {}, false, 2, "256M", {}, "128M");
    limited.PreserveOnFailure();
    DiagnosedZSetClient client(limited, disk);
    EXPECT_EQ(client.Command({"ZCARD", "large"}).text_, "64");
    auto rejected = client.Command({"ZINCRBY", "large", "-2000", member(0)});
    EXPECT_EQ(rejected.kind_, '-');
    EXPECT_TRUE(rejected.text_.starts_with("OOM ")) << rejected.text_;
    EXPECT_NE(rejected.text_.find("grouped operation scratch admission"),
              std::string::npos)
        << rejected.text_;
    EXPECT_EQ(client.Command({"ZSCORE", "large", member(0)}).text_, "1000");
    auto oversized_pop = client.Command({"ZPOPMAX", "large", "64"});
    EXPECT_EQ(oversized_pop.kind_, '-');
    EXPECT_TRUE(oversized_pop.text_.starts_with("OOM ")) << oversized_pop.text_;
    EXPECT_EQ(client.Command({"ZCARD", "large"}).text_, "64");
    auto oversized_reply = client.Command({"ZRANGE", "large", "0", "-1"});
    EXPECT_EQ(oversized_reply.kind_, '-');
    EXPECT_TRUE(oversized_reply.text_.starts_with("OOM "))
        << oversized_reply.text_;
    auto bounded_reply = client.Command({"ZRANGE", "large", "0", "0"});
    ASSERT_EQ(bounded_reply.items_.size(), 1);
    EXPECT_EQ(bounded_reply.items_[0].text_, member(64));
    ASSERT_EQ(limited.Wait(true), 0) << limited.Log();
  }
  Server recovered(disk, 3, {}, {}, false, 2, "512M", {}, "128M");
  recovered.PreserveOnFailure();
  DiagnosedZSetClient client(recovered, disk);
  EXPECT_EQ(client.Command({"ZCARD", "large"}).text_, "64");
  EXPECT_EQ(client.Command({"ZSCORE", "large", member(0)}).text_, "1000");
  EXPECT_EQ(client.Command({"ZSCORE", "large", member(32)}).text_, "-1");
  EXPECT_EQ(client.Command({"ZSCORE", "large", member(64)}).text_, "-10");
  auto recovered_first = client.Command({"ZRANGE", "large", "0", "0"});
  ASSERT_EQ(recovered_first.items_.size(), 1);
  EXPECT_EQ(recovered_first.items_[0].text_, member(64));
  auto maximum = client.Command({"ZPOPMAX", "large"});
  ASSERT_EQ(maximum.items_.size(), 2);
  EXPECT_EQ(maximum.items_[0].text_, member(0));
  EXPECT_EQ(maximum.items_[1].text_, "1000");
  auto multi = client.Command({"ZMPOP", "1", "large", "MAX"});
  ASSERT_EQ(multi.items_.size(), 2);
  EXPECT_EQ(multi.items_[1].items_[0].items_[0].text_, member(63));
  client.Durable();
  ASSERT_EQ(recovered.Wait(true), 0) << recovered.Log();
  Server final_recovery(disk, 4, {}, {}, false, 2, "512M", {}, "128M");
  final_recovery.PreserveOnFailure();
  DiagnosedZSetClient final_client(final_recovery, disk);
  EXPECT_EQ(final_client.Command({"ZCARD", "large"}).text_, "62");
  EXPECT_EQ(final_client.Command({"ZSCORE", "large", member(0)}).text_, "-1");
  EXPECT_EQ(final_client.Command({"ZSCORE", "large", member(63)}).text_, "-1");
}

}  // namespace
