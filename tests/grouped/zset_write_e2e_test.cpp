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

#include "write_e2e_support.h"

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

TEST(GroupedSortedSetWriteE2e, TypedChangesDoNotRereadOrderedMemberDiff) {
#if !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires member-diff read failure injection";
#endif
  PrivateDisk disk;
  const std::string key = "checked-delta";
  auto member = [](unsigned i) {
    return std::to_string(i) + std::string(128, 'd');
  };
  {
    Server server(disk, 1);
    Client client(server.port());
    std::vector<std::string> seed{"ZADD", key};
    for (unsigned i = 0; i < 256; ++i) {
      seed.push_back(std::to_string(i));
      seed.push_back(member(i));
    }
    ASSERT_EQ(client.Command(seed).text_, "256");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  ScopedEnvironment fault("LAVIK_FAIL_ZSET_MEMBER_DIFF_READ_KEY", key.c_str());
  {
    Server server(disk, 3);
    Client client(server.port());
    ASSERT_EQ(client
                  .Command({"ZADD", key, "CH", "500", member(100), "400",
                            member(256)})
                  .text_,
              "2");
    ASSERT_EQ(client.Command({"ZINCRBY", key, "2", member(101)}).text_, "103");
    ASSERT_EQ(client.Command({"ZREM", key, member(102), member(103), "missing"})
                  .text_,
              "2");
    ASSERT_EQ(client.Command({"ZPOPMIN", key, "2"}).items_.size(), 4);
    ASSERT_EQ(client.Command({"ZPOPMAX", key, "2"}).items_.size(), 4);
    ASSERT_EQ(client.Command({"ZREMRANGEBYSCORE", key, "2", "3"}).text_, "2");
    ASSERT_EQ(client.Command({"MULTI"}).text_, "OK");
    ASSERT_EQ(client.Command({"ZREMRANGEBYRANK", key, "0", "1"}).text_,
              "QUEUED");
    ASSERT_EQ(client.Command({"ZADD", key, "700", member(257)}).text_,
              "QUEUED");
    auto executed = client.Command({"EXEC"});
    ASSERT_EQ(executed.items_.size(), 2);
    EXPECT_EQ(executed.items_[0].text_, "2");
    EXPECT_EQ(executed.items_[1].text_, "1");
    EXPECT_EQ(client.Command({"ZCARD", key}).text_, "248");
    for (unsigned i : {0U, 1U, 2U, 3U, 4U, 5U, 100U, 102U, 103U, 256U})
      EXPECT_EQ(client.Command({"ZSCORE", key, member(i)}).text_, "-1");
    EXPECT_EQ(client.Command({"ZSCORE", key, member(101)}).text_, "103");
    EXPECT_EQ(client.Command({"ZSCORE", key, member(257)}).text_, "700");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server recovered(disk, 2);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"ZCARD", key}).text_, "248");
  EXPECT_EQ(client.Command({"ZSCORE", key, member(101)}).text_, "103");
  EXPECT_EQ(client.Command({"ZSCORE", key, member(257)}).text_, "700");
  EXPECT_EQ(client.Command({"ZRANGE", key, "0", "-1"}).items_.size(), 248);
}

TEST(GroupedSortedSetWriteE2e,
     PointWritesReuseMemberLeafAndKeepBatchFailureAtomic) {
#if !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires member-index leaf read failure injection";
#endif
  PrivateDisk disk;
  const std::string key = "member-probe";
  auto member = [](unsigned i) {
    return std::to_string(i) + std::string(128, 'm');
  };
  {
    Server server(disk, 1);
    Client client(server.port());
    ASSERT_EQ(client.Command(ZSetSeed(key)).text_, "256");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  {
    ScopedEnvironment fault("LAVIK_FAIL_ZSET_MEMBER_LEAF_READ_KEY",
                            key.c_str());
    Server server(disk, 3);
    Client client(server.port());
    // Recovered pages have no command-local decoded state. The initial
    // lookup must supply the checked leaf for replacement, insertion and
    // deletion without the second read disabled by this fault.
    ASSERT_EQ(client.Command({"ZINCRBY", key, "2", member(101)}).text_, "103");
    ASSERT_EQ(client.Command({"ZADD", key, "500", member(256)}).text_, "1");
    ASSERT_EQ(client.Command({"ZREM", key, member(100)}).text_, "1");
    // Multi-member writes keep bounded sequential lookup and the ordinary
    // prepare read. Failure after planning must leave both graphs unchanged.
    const auto failed =
        client.Command({"ZADD", key, "900", member(10), "901", member(11)});
    EXPECT_EQ(failed.kind_, '-');
    EXPECT_NE(failed.text_.find("member-index leaf read failure"),
              std::string::npos);
    EXPECT_EQ(client.Command({"ZSCORE", key, member(10)}).text_, "10");
    EXPECT_EQ(client.Command({"ZSCORE", key, member(11)}).text_, "11");
    ASSERT_EQ(client.Command({"MULTI"}).text_, "OK");
    ASSERT_EQ(client.Command({"ZINCRBY", key, "1", member(101)}).text_,
              "QUEUED");
    ASSERT_EQ(client.Command({"ZINCRBY", key, "1", member(101)}).text_,
              "QUEUED");
    const auto executed = client.Command({"EXEC"});
    ASSERT_EQ(executed.items_.size(), 2);
    EXPECT_EQ(executed.items_[0].text_, "104");
    EXPECT_EQ(executed.items_[1].text_, "105");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server recovered(disk, 2);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"ZCARD", key}).text_, "256");
  EXPECT_EQ(client.Command({"ZSCORE", key, member(100)}).text_, "-1");
  EXPECT_EQ(client.Command({"ZSCORE", key, member(101)}).text_, "105");
  EXPECT_EQ(client.Command({"ZSCORE", key, member(256)}).text_, "500");
  EXPECT_EQ(client.Command({"ZSCORE", key, member(10)}).text_, "10");
  EXPECT_EQ(client.Command({"ZSCORE", key, member(11)}).text_, "11");
  EXPECT_EQ(client.Command({"ZRANGE", key, "0", "-1"}).items_.size(), 256);
}

TEST(GroupedSortedSetWriteE2e, AdjacentScoreMovesReusePagesAcrossRecovery) {
  PrivateDisk disk;
  auto member = [](unsigned i) { return std::string(1024, 'a' + i); };
  auto verify = [&](Client& client, bool moved) {
    EXPECT_EQ(client.Command({"ZCARD", "adjacent"}).text_, "21");
    auto reply =
        client.Command({"ZRANGE", "adjacent", "0", "-1", "WITHSCORES"});
    ASSERT_EQ(reply.items_.size(), 42);
    std::vector<std::pair<double, std::string>> expected;
    for (unsigned i = 0; i < 21; ++i) {
      const double score = i == 6 && moved ? 7.5 : double(i);
      expected.emplace_back(score, member(i));
      EXPECT_EQ(
          std::stod(client.Command({"ZSCORE", "adjacent", member(i)}).text_),
          score);
    }
    std::sort(expected.begin(), expected.end());
    for (std::size_t i = 0; i < expected.size(); ++i) {
      EXPECT_EQ(reply.items_[2 * i].text_, expected[i].second);
      EXPECT_EQ(std::stod(reply.items_[2 * i + 1].text_), expected[i].first);
    }
  };
  auto page_ids = [&] {
    std::set<std::uint64_t> ids;
    for (const auto& [sequence, groups] : disk.Auxiliaries("adjacent"))
      for (const auto& [prefix, bits] : groups)
        if (bits == 0 && prefix != 0) ids.insert(prefix);
    return ids;
  };
  {
    Server server(disk);
    Client client(server.port());
    std::vector<std::string> seed{"ZADD", "adjacent"};
    for (unsigned i = 0; i < 21; ++i)
      seed.insert(seed.end(), {std::to_string(i), member(i)});
    ASSERT_EQ(client.Command(seed).text_, "21");
    verify(client, false);
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  const auto ids = page_ids();
  ASSERT_EQ(ids.size(), 3);
  {
    Server server(disk, 3);
    Client client(server.port());
    for (unsigned i = 0; i < 12; ++i) {
      ASSERT_EQ(client.Command({"ZINCRBY", "adjacent", "1.5", member(6)}).text_,
                "7.5");
      verify(client, true);
      ASSERT_EQ(client.Command({"MULTI"}).text_, "OK");
      ASSERT_EQ(client.Command({"ZADD", "adjacent", "6", member(6)}).text_,
                "QUEUED");
      ASSERT_EQ(client.Command({"EXEC"}).items_.size(), 1);
      verify(client, false);
    }
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  EXPECT_EQ(page_ids(), ids);
  Server recovered(disk, 4);
  Client client(recovered.port());
  verify(client, false);
}

TEST(GroupedSortedSetWriteE2e, PlannerSkipsUnchangedNeighborButChecksNewLinks) {
#if !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires plan-page read failure injection";
#endif
  PrivateDisk disk;
  const std::string new_member(1024, 'z');
  auto member = [](unsigned i) { return std::string(1024, 'a' + i); };
  {
    Server server(disk);
    Client client(server.port());
    std::vector<std::string> seed{"ZADD", "lazy-neighbor"};
    for (unsigned i = 0; i < 21; ++i)
      seed.insert(seed.end(), {std::to_string(i), member(i)});
    ASSERT_EQ(client.Command(seed).text_, "21");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  struct Fault {
    ScopedEnvironment key_{"LAVIK_FAIL_ZSET_PLAN_READ_KEY", "lazy-neighbor"};
    ScopedEnvironment page_;
    explicit Fault(const char* page)
        : page_("LAVIK_FAIL_ZSET_PLAN_READ_PAGE", page) {}
  };
  {
    // The third page is selected for adjacency but neither payload nor link
    // changes when the two modified pages redistribute. A fault there must
    // remain untouched, even though it is already included in admission.
    Fault fault("3");
    Server server(disk, 3);
    Client client(server.port());
    EXPECT_EQ(
        client.Command({"ZINCRBY", "lazy-neighbor", "1.5", member(6)}).text_,
        "7.5");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  {
    // Real growth splits page one and changes page two's previous link. Its
    // load must still fail atomically before either index can publish.
    Fault fault("2");
    Server server(disk, 4);
    Client client(server.port());
    const auto failed =
        client.Command({"ZADD", "lazy-neighbor", "-1", new_member});
    EXPECT_EQ(failed.kind_, '-');
    EXPECT_NE(failed.text_.find("plan page read failure"), std::string::npos);
    EXPECT_EQ(client.Command({"ZCARD", "lazy-neighbor"}).text_, "21");
    EXPECT_EQ(client.Command({"ZSCORE", "lazy-neighbor", new_member}).text_,
              "-1");
    EXPECT_EQ(client.Command({"ZSCORE", "lazy-neighbor", member(6)}).text_,
              "7.5");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server recovered(disk, 2);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"ZCARD", "lazy-neighbor"}).text_, "21");
  EXPECT_EQ(client.Command({"ZSCORE", "lazy-neighbor", new_member}).text_,
            "-1");
  EXPECT_EQ(client.Command({"ZSCORE", "lazy-neighbor", member(6)}).text_,
            "7.5");
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

  ScopedEnvironment fault("LAVIK_FAIL_ZSET_ORDERED_READ_KEY", "indexed");
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
  ScopedEnvironment key_fault("LAVIK_FAIL_ZSET_ORDERED_READ_KEY", "routed");
  ScopedEnvironment page_fault("LAVIK_FAIL_ZSET_ORDERED_READ_PAGE", "1");
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
  EXPECT_EQ(client.Command({"ZREMRANGEBYSCORE", "routed", "250", "255"}).text_,
            "6");
  EXPECT_EQ(client.Command({"ZREMRANGEBYRANK", "routed", "-1", "-1"}).text_,
            "1");
  // Negative control: the fault must be armed, not merely skipped by Release.
  const auto first = client.Command({"ZRANGE", "routed", "0", "0"});
  EXPECT_EQ(first.kind_, '-');
  EXPECT_NE(first.text_.find("injected ordered-page"), std::string::npos);
  client.Durable();
  EXPECT_EQ(recovered.Wait(true), 0) << recovered.Log();
}

void CheckScoreBoundsTieMoves(std::size_t member_bytes) {
  PrivateDisk disk;
  std::vector<std::string> members;
  std::map<std::string, double> expected;
  std::vector<std::string> seed{"ZADD", "ties"};
  for (unsigned i = 0; i < 64; ++i) {
    // Distinguishing bytes follow a NUL so both source matching and tie
    // routing must compare the complete binary member. The large variant
    // puts each member beyond the page target; the small variant also checks
    // several members sharing one borrowed page lease.
    members.push_back(std::string("tie\0", 4) + std::to_string(1000 + i) +
                      std::string(member_bytes, 'x'));
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
    std::vector<std::string> lookup{"ZMSCORE", "ties"};
    for (const auto& [score, member] : sorted) lookup.push_back(member);
    const auto scores = client.Command(lookup);
    ASSERT_EQ(scores.items_.size(), sorted.size()) << scores.text_;
    for (std::size_t i = 0; i < sorted.size(); ++i)
      EXPECT_EQ(std::stod(scores.items_[i].text_), sorted[i].first);
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

TEST(GroupedSortedSetWriteE2e, ScoreBoundsHandleLongTieRunsAndMixedBatchMoves) {
  CheckScoreBoundsTieMoves(20 * 1024);
}

TEST(GroupedSortedSetWriteE2e, WriteProbesPreserveBinaryMembersWithinPages) {
  // Stay above grouped promotion while retaining many entries per page.
  CheckScoreBoundsTieMoves(512);
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
  // Span the prefix directory in reverse order so page traversal cannot
  // substitute its order for the command's output positions.
  std::vector<std::string> lookup{"ZMSCORE", "zset"};
  for (int i = 255; i >= 0; --i)
    lookup.push_back(std::to_string(i) + std::string(128, 'm'));
  auto all_scores = client.Command(lookup);
  ASSERT_EQ(all_scores.items_.size(), 256);
  for (std::size_t i = 0; i < all_scores.items_.size(); ++i)
    EXPECT_EQ(all_scores.items_[i].text_, std::to_string(255 - i));
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

TEST(GroupedSortedSetWriteE2e, PageScanRepliesOwnBinaryMembersAcrossRecovery) {
  PrivateDisk disk;
  std::vector<std::string> members;
  std::vector<std::string> seed{"ZADD", "borrowed-pages"};
  for (unsigned i = 0; i < 128; ++i) {
    members.push_back(std::string("m\0", 2) + std::to_string(i) +
                      std::string(i == 64 ? 128 * 1024 : 1024, 'a' + i % 26));
    seed.push_back(std::to_string(i));
    seed.push_back(members.back());
  }
  for (unsigned pass = 0; pass < 2; ++pass) {
    Server server(disk, pass == 0 ? 2 : 3);
    Client client(server.port());
    if (pass == 0) ASSERT_EQ(client.Command(seed).text_, "128");
    // Many subsequent page reads reuse read buffers before the reply is sent.
    // The large middle member also exercises the external-value lease.
    auto all =
        client.Command({"ZRANGE", "borrowed-pages", "0", "-1", "WITHSCORES"});
    ASSERT_EQ(all.items_.size(), members.size() * 2);
    for (std::size_t i = 0; i < members.size(); ++i) {
      EXPECT_EQ(all.items_[2 * i].text_, members[i]);
      EXPECT_EQ(all.items_[2 * i + 1].text_, std::to_string(i));
    }
    auto reverse =
        client.Command({"ZRANGE", "borrowed-pages", "60", "68", "REV"});
    ASSERT_EQ(reverse.items_.size(), 9);
    for (std::size_t i = 0; i < reverse.items_.size(); ++i)
      EXPECT_EQ(reverse.items_[i].text_, members[67 - i]);
    EXPECT_EQ(client.Command({"ZRANK", "borrowed-pages", members.back()}).text_,
              "127");
    EXPECT_EQ(client.Command({"ZCOUNT", "borrowed-pages", "(60", "68"}).text_,
              "8");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
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

TEST(GroupedSortedSetWriteE2e, FullImageAndRandomReplyOomAreAtomic) {
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
    // The full-image adapter alone could admit this 16 MiB value. The decoded
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
    const auto random = client.Command({"ZRANDMEMBER", "large"});
    ASSERT_EQ(random.kind_, '$') << random.text_;
    ASSERT_EQ(random.text_.size(), payload.size() + 1);
    EXPECT_GE(random.text_[0], '0');
    EXPECT_LE(random.text_[0], '3');
    EXPECT_EQ(random.text_.substr(1), payload);
    // GEO point reads must fit the same budget as ZSCORE. Neither a 4 MiB
    // requested member nor unrelated pages justify a full-image reservation.
    const auto position = client.Command({"GEOPOS", "large", member(0)});
    ASSERT_EQ(position.kind_, '*') << position.text_;
    ASSERT_EQ(position.items_.size(), 1);
    ASSERT_EQ(position.items_[0].items_.size(), 2);
    const auto hash = client.Command({"GEOHASH", "large", member(0)});
    ASSERT_EQ(hash.items_.size(), 1) << hash.text_;
    EXPECT_EQ(hash.items_[0].text_.size(), 11);
    const auto distance =
        client.Command({"GEODIST", "large", member(0), member(1), "km"});
    EXPECT_EQ(distance.kind_, '$') << distance.text_;
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
    ASSERT_EQ(client
                  .Command({"GEOSEARCH", "large", "FROMLONLAT", "0", "0",
                            "BYRADIUS", "1", "km", "COUNT", "1"})
                  .text_,
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

TEST(GroupedSortedSetWriteE2e, RangeRemovalFitsSelectedPagesAndRecovers) {
  PrivateDisk disk;
  const std::string payload(1024 * 1024, 'r');
  auto member = [&](unsigned i) { return std::to_string(i) + payload; };
  {
    Server server(disk);
    Client client(server.port());
    for (unsigned i = 0; i < 16; ++i)
      ASSERT_EQ(
          client.Command({"ZADD", "remove-range", std::to_string(i), member(i)})
              .text_,
          "1");
    ASSERT_EQ(client.Command({"EXPIRE", "remove-range", "3600"}).text_, "1");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  {
    // The old 6x full-image reservation exceeds this per-worker budget. Only
    // matching members and neighbouring rewrite pages should be admitted.
    Server server(disk, 2, {}, {}, false, 2, "128M", {}, "128M");
    Client client(server.port());
    EXPECT_EQ(
        client.Command({"ZREMRANGEBYRANK", "remove-range", "0", "0"}).text_,
        "1");
    EXPECT_EQ(
        client.Command({"ZREMRANGEBYSCORE", "remove-range", "(0", "1"}).text_,
        "1");
    EXPECT_EQ(client
                  .Command({"ZREMRANGEBYLEX", "remove-range", "[" + member(2),
                            "[" + member(2)})
                  .text_,
              "1");
    EXPECT_EQ(client.Command({"ZCARD", "remove-range"}).text_, "13");
    EXPECT_GT(std::stoi(client.Command({"TTL", "remove-range"}).text_), 0);
    EXPECT_NE(client.Command({"INFO", "MEMORY"})
                  .text_.find("memory_admission_pending:0\r\n"),
              std::string::npos);
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server recovered(disk, 3);
  Client client(recovered.port());
  for (unsigned i = 0; i < 16; ++i)
    EXPECT_EQ(client.Command({"ZSCORE", "remove-range", member(i)}).text_,
              i < 3 ? "-1" : std::to_string(i));
  EXPECT_EQ(client.Command({"ZCARD", "remove-range"}).text_, "13");
  EXPECT_EQ(client.Command({"ZREMRANGEBYSCORE", "remove-range", "-inf", "+inf"})
                .text_,
            "13");
  EXPECT_EQ(client.Command({"EXISTS", "remove-range"}).text_, "0");
}

TEST(GroupedSortedSetWriteE2e,
     FailedRangeRemovalPreservesExecPrefixAndRecovery) {
#if !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires auxiliary-write failure injection";
#endif
  PrivateDisk disk;
  {
    Server server(disk, 2);
    Client client(server.port());
    ASSERT_EQ(client.Command(ZSetSeed("range{undo}")).text_, "256");
    ASSERT_EQ(client.Command({"EXPIRE", "range{undo}", "3600"}).text_, "1");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  const std::string first = "0" + std::string(128, 'm');
  for (const std::vector<std::string> command :
       {std::vector<std::string>{"ZREMRANGEBYRANK", "range{undo}", "0", "0"},
        std::vector<std::string>{"ZREMRANGEBYSCORE", "range{undo}", "0", "0"},
        std::vector<std::string>{"ZREMRANGEBYLEX", "range{undo}", "[" + first,
                                 "[" + first}}) {
    // Reject a staged auxiliary write, then commit another command in EXEC.
    // Both indexes and TTL must retain the old value through cold recovery.
    Server server(disk, 3, {}, "range{undo}", false, 2);
    Client client(server.port());
    ASSERT_EQ(client.Command({"MULTI"}).text_, "OK");
    ASSERT_EQ(client.Command({"SET", "before{undo}", "prefix"}).text_,
              "QUEUED");
    ASSERT_EQ(client.Command(command).text_, "QUEUED");
    ASSERT_EQ(client.Command({"SET", "after{undo}", "suffix"}).text_, "QUEUED");
    auto result = client.Command({"EXEC"});
    ASSERT_EQ(result.items_.size(), 3);
    EXPECT_EQ(result.items_[0].text_, "OK");
    EXPECT_TRUE(result.items_[1].text_.starts_with("OOM"))
        << result.items_[1].text_;
    EXPECT_EQ(result.items_[2].text_, "OK");
    EXPECT_EQ(client.Command({"ZCARD", "range{undo}"}).text_, "256");
    EXPECT_EQ(client.Command({"ZSCORE", "range{undo}", first}).text_, "0");
    const auto range = client.Command({"ZRANGE", "range{undo}", "0", "0"});
    ASSERT_EQ(range.items_.size(), 1);
    EXPECT_EQ(range.items_[0].text_, first);
    EXPECT_GT(std::stoi(client.Command({"TTL", "range{undo}"}).text_), 0);
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server recovered(disk, 4);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"ZCARD", "range{undo}"}).text_, "256");
  EXPECT_EQ(client.Command({"ZSCORE", "range{undo}", first}).text_, "0");
  EXPECT_EQ(client.Command({"GET", "before{undo}"}).text_, "prefix");
  EXPECT_EQ(client.Command({"GET", "after{undo}"}).text_, "suffix");
  EXPECT_EQ(client.Command({"ZREMRANGEBYRANK", "range{undo}", "0", "0"}).text_,
            "1");
}

TEST(GroupedSortedSetWriteE2e, RandomAndRangeRemovalPreserveSemantics) {
  PrivateDisk disk;
  Server server(disk, 2);
  Client client(server.port());
  // Include binary and empty members, ties, and mixed lexical/score order.
  const std::string binary("b\0x", 3);
  for (const std::string key : {"compact", "grouped"}) {
    const std::string padding = key == "grouped" ? std::string(1024, 'p') : "";
    const std::vector<std::string> names{"", "a" + padding, binary + padding,
                                         "c" + padding, "d" + padding};
    auto seed = [&] {
      ASSERT_EQ(client.Command({"DEL", key}).kind_, ':');
      ASSERT_EQ(client
                    .Command({"ZADD", key, "3", names[0], "1", names[1], "1",
                              names[2], "0", names[3], "2", names[4]})
                    .text_,
                "5");
    };
    seed();
    auto all = client.Command({"ZRANDMEMBER", key, "100", "WITHSCORES"});
    ASSERT_EQ(all.items_.size(), 10);
    std::map<std::string, std::string> expected{{names[0], "3"},
                                                {names[1], "1"},
                                                {names[2], "1"},
                                                {names[3], "0"},
                                                {names[4], "2"}};
    std::set<std::string> unique;
    for (std::size_t i = 0; i < all.items_.size(); i += 2) {
      EXPECT_TRUE(expected.contains(all.items_[i].text_));
      EXPECT_EQ(expected[all.items_[i].text_], all.items_[i + 1].text_);
      EXPECT_TRUE(unique.insert(all.items_[i].text_).second);
    }
    auto repeated = client.Command({"ZRANDMEMBER", key, "-32", "WITHSCORES"});
    ASSERT_EQ(repeated.items_.size(), 64);
    for (std::size_t i = 0; i < repeated.items_.size(); i += 2) {
      EXPECT_TRUE(expected.contains(repeated.items_[i].text_));
      EXPECT_EQ(expected[repeated.items_[i].text_],
                repeated.items_[i + 1].text_);
    }
    EXPECT_TRUE(client.Command({"ZRANDMEMBER", key, "0"}).items_.empty());
    EXPECT_EQ(client.Command({"ZREMRANGEBYRANK", key, "3", "1"}).text_, "0");
    EXPECT_EQ(client.Command({"ZREMRANGEBYRANK", key, "-2", "-1"}).text_, "2");
    EXPECT_EQ(client.Command({"ZREMRANGEBYSCORE", key, "(0", "1"}).text_, "2");
    EXPECT_EQ(client.Command({"ZREMRANGEBYLEX", key, "-", "+"}).text_, "1");
    EXPECT_EQ(client.Command({"EXISTS", key}).text_, "0");
    seed();
    EXPECT_EQ(
        client.Command({"ZREMRANGEBYLEX", key, "(" + names[1], "[" + names[3]})
            .text_,
        "2");
    EXPECT_EQ(client.Command({"ZCARD", key}).text_, "3");
    ASSERT_EQ(client.Command({"MULTI"}).text_, "OK");
    ASSERT_EQ(client.Command({"ZREMRANGEBYSCORE", key, "1", "2"}).text_,
              "QUEUED");
    ASSERT_EQ(client.Command({"ZRANDMEMBER", key, "10"}).text_, "QUEUED");
    const auto exec = client.Command({"EXEC"});
    ASSERT_EQ(exec.items_.size(), 2);
    EXPECT_EQ(exec.items_[0].text_, "2");
    ASSERT_EQ(exec.items_[1].items_.size(), 1);
    EXPECT_EQ(exec.items_[1].items_[0].text_, "");
    EXPECT_EQ(client
                  .Command({"EVAL",
                            "redis.call('ZADD',KEYS[1],5,'lua'); return "
                            "redis.call('ZREMRANGEBYRANK',KEYS[1],0,-1)",
                            "1", key})
                  .text_,
              "2");
  }
  EXPECT_EQ(client.Command({"ZRANDMEMBER", "absent"}).text_, "-1");
  EXPECT_TRUE(client.Command({"ZRANDMEMBER", "absent", "2"}).items_.empty());
  EXPECT_EQ(client.Command({"SET", "wrong", "value"}).text_, "OK");
  for (const auto* cmd : {"ZREMRANGEBYRANK", "ZREMRANGEBYSCORE"}) {
    EXPECT_EQ(client.Command({cmd, "absent", "0", "1"}).text_, "0");
    EXPECT_TRUE(client.Command({cmd, "wrong", "0", "1"})
                    .text_.starts_with("WRONGTYPE"));
  }
  EXPECT_TRUE(client.Command({"ZRANDMEMBER", "wrong", "0"})
                  .text_.starts_with("WRONGTYPE"));
  EXPECT_EQ(client.Command({"ZREMRANGEBYLEX", "absent", "-", "+"}).text_, "0");
  EXPECT_TRUE(client.Command({"ZREMRANGEBYLEX", "wrong", "-", "+"})
                  .text_.starts_with("WRONGTYPE"));
  ASSERT_EQ(client.Command(ZSetSeed("sample-pages")).text_, "256");
  auto sample =
      client.Command({"ZRANDMEMBER", "sample-pages", "7", "WITHSCORES"});
  ASSERT_EQ(sample.items_.size(), 14);
  std::set<std::string> sampled;
  for (std::size_t i = 0; i < sample.items_.size(); i += 2) {
    const auto score = std::stoi(sample.items_[i + 1].text_);
    EXPECT_GE(score, 0);
    EXPECT_LT(score, 256);
    EXPECT_EQ(sample.items_[i].text_,
              std::to_string(score) + std::string(128, 'm'));
    EXPECT_TRUE(sampled.insert(sample.items_[i].text_).second);
  }
}

TEST(GroupedSortedSetWriteE2e, GeoPointReadsPreserveRepliesAndExecVisibility) {
  PrivateDisk disk;
  const std::string binary_member("binary\0member", 13);
  for (const unsigned workers : {2u, 3u}) {
    Server server(disk, workers);
    Client client(server.port());
    if (workers == 2) {
      ASSERT_EQ(client
                    .Command({"GEOADD", "geo", "13.361389", "38.115556",
                              "Palermo", "15.087269", "37.502669", "Catania",
                              "13.361389", "38.115556", binary_member,
                              "15.087269", "37.502669", ""})
                    .text_,
                "4");
      ASSERT_EQ(client.Command({"EXPIRE", "geo", "3600"}).text_, "1");
      ASSERT_EQ(client
                    .Command({"ZADD", "invalid-geo", "inf", "invalid", "-1",
                              "wrapped"})
                    .text_,
                "2");
      ASSERT_EQ(client.Command({"SET", "wrong-geo", "value"}).text_, "OK");
    }
    auto positions = client.Command(
        {"GEOPOS", "geo", "Palermo", "missing", binary_member, ""});
    ASSERT_EQ(positions.items_.size(), 4);
    ASSERT_EQ(positions.items_[0].items_.size(), 2);
    EXPECT_EQ(positions.items_[0].items_[0].text_, "13.36138933897018433");
    EXPECT_EQ(positions.items_[0].items_[1].text_, "38.11555639549629859");
    EXPECT_EQ(positions.items_[1].kind_, '*');
    EXPECT_EQ(positions.items_[1].text_, "-1");
    ASSERT_EQ(positions.items_[2].items_.size(), 2);
    ASSERT_EQ(positions.items_[3].items_.size(), 2);
    EXPECT_EQ(positions.items_[2].items_[0].text_,
              positions.items_[0].items_[0].text_);
    EXPECT_EQ(positions.items_[3].items_[0].text_, "15.08726745843887329");
    auto hashes = client.Command(
        {"GEOHASH", "geo", "Palermo", "missing", "Palermo", "Catania"});
    ASSERT_EQ(hashes.items_.size(), 4);
    EXPECT_EQ(hashes.items_[0].text_, "sqc8b49rny0");
    EXPECT_EQ(hashes.items_[1].kind_, '$');
    EXPECT_EQ(hashes.items_[1].text_, "-1");
    EXPECT_EQ(hashes.items_[2].text_, hashes.items_[0].text_);
    EXPECT_EQ(hashes.items_[3].text_, "sqdtr74hyu0");
    EXPECT_EQ(
        client.Command({"GEODIST", "geo", "Palermo", "Catania", "km"}).text_,
        "166.2742");
    EXPECT_EQ(client.Command({"GEODIST", "geo", "Palermo", "Palermo"}).text_,
              "0.0000");
    for (const auto* operation : {"GEOPOS", "GEOHASH"}) {
      EXPECT_TRUE(client.Command({operation, "geo"}).items_.empty());
      auto missing = client.Command({operation, "absent-geo", "member"});
      ASSERT_EQ(missing.items_.size(), 1);
      EXPECT_EQ(missing.items_[0].text_, "-1");
      auto invalid =
          client.Command({operation, "invalid-geo", "invalid", "wrapped"});
      ASSERT_EQ(invalid.items_.size(), 2);
      EXPECT_EQ(invalid.items_[0].text_, "-1");
      EXPECT_NE(invalid.items_[1].text_, "-1");
      EXPECT_TRUE(client.Command({operation, "wrong-geo"})
                      .text_.starts_with("WRONGTYPE"));
    }
    EXPECT_EQ(client.Command({"GEODIST", "geo", "Palermo", "missing"}).text_,
              "-1");
    EXPECT_EQ(
        client.Command({"GEODIST", "invalid-geo", "invalid", "wrapped"}).text_,
        "-1");
    EXPECT_TRUE(client.Command({"GEODIST", "geo", "Palermo", "Catania", "bad"})
                    .text_.starts_with("ERR "));
    EXPECT_TRUE(client.Command({"GEODIST", "wrong-geo", "a", "b", "km"})
                    .text_.starts_with("WRONGTYPE"));
    // Syntax validation runs before storage access, including for wrong types.
    EXPECT_TRUE(client.Command({"GEODIST", "wrong-geo", "a", "b", "bad"})
                    .text_.starts_with("ERR "));
    EXPECT_GT(std::stoi(client.Command({"TTL", "geo"}).text_), 0);

    // Reads inside EXEC must see the preceding staged grouped mutation; two
    // separate point requests would also lose GEODIST's single-read boundary.
    ASSERT_EQ(client.Command({"MULTI"}).text_, "OK");
    ASSERT_EQ(client
                  .Command({"GEOADD", "geo-exec", "13.361389", "38.115556",
                            "Palermo", "15.087269", "37.502669", "Catania"})
                  .text_,
              "QUEUED");
    ASSERT_EQ(client.Command({"GEOPOS", "geo-exec", "Palermo"}).text_,
              "QUEUED");
    ASSERT_EQ(client.Command({"GEOHASH", "geo-exec", "Catania"}).text_,
              "QUEUED");
    ASSERT_EQ(
        client.Command({"GEODIST", "geo-exec", "Palermo", "Catania", "km"})
            .text_,
        "QUEUED");
    auto executed = client.Command({"EXEC"});
    ASSERT_EQ(executed.items_.size(), 4);
    ASSERT_EQ(executed.items_[1].items_.size(), 1);
    ASSERT_EQ(executed.items_[1].items_[0].items_.size(), 2);
    EXPECT_EQ(executed.items_[1].items_[0].items_[0].text_,
              "13.36138933897018433");
    ASSERT_EQ(executed.items_[2].items_.size(), 1);
    EXPECT_EQ(executed.items_[2].items_[0].text_, "sqdtr74hyu0");
    EXPECT_EQ(executed.items_[3].text_, "166.2742");
    EXPECT_EQ(
        client
            .Command({"EVAL",
                      "redis.call('GEOADD',KEYS[1],13.361389,38.115556,'a',"
                      "15.087269,37.502669,'b'); "
                      "return redis.call('GEODIST',KEYS[1],'a','b','km')",
                      "1", "geo-lua"})
            .text_,
        "166.2742");
    EXPECT_NE(client.Command({"INFO", "MEMORY"})
                  .text_.find("memory_admission_pending:0\r\n"),
              std::string::npos);
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
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
