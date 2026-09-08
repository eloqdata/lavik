#include <cstdlib>
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

TEST(GroupedSortedSetWriteE2e, MemberScoresUsePrefixPagesWithoutOrderedReads) {
#if !KEYLANE_TEST_FAULTS_AVAILABLE
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
      if (const char* value = std::getenv("KEYLANE_FAIL_ZSET_ORDERED_READ_KEY"))
        previous_ = value;
      ::setenv("KEYLANE_FAIL_ZSET_ORDERED_READ_KEY", "indexed", 1);
    }
    ~Fault() {
      if (previous_)
        ::setenv("KEYLANE_FAIL_ZSET_ORDERED_READ_KEY", previous_->c_str(), 1);
      else
        ::unsetenv("KEYLANE_FAIL_ZSET_ORDERED_READ_KEY");
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

TEST(GroupedSortedSetWriteE2e, FailedMemberWriteCannotCommitOrderedHalf) {
#if !KEYLANE_TEST_FAULTS_AVAILABLE
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
