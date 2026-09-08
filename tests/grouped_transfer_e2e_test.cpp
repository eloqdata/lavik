#include "grouped_write_e2e_support.h"

namespace {
using namespace grouped_e2e;

void PopulateTransfer(Client& client, std::string_view type,
                      const std::string& key, const std::string& huge) {
  std::vector<std::string> command;
  if (type == "hash") command = {"HSET", key};
  if (type == "set") command = {"SADD", key};
  if (type == "list") command = {"RPUSH", key};
  if (type == "zset") command = {"ZADD", key};
  for (unsigned i = 0; i < 256; ++i) {
    const auto member = "item" + std::to_string(i) + std::string(128, 'm');
    if (type == "hash") command.insert(command.end(), {member, "value"});
    if (type == "set" || type == "list") command.push_back(member);
    if (type == "zset")
      command.insert(command.end(), {std::to_string(i), member});
  }
  ASSERT_EQ(client.Command(command).text_, "256");
  if (type == "hash")
    ASSERT_EQ(client.Command({"HSET", key, "huge", huge}).text_, "1");
  if (type == "set") ASSERT_EQ(client.Command({"SADD", key, huge}).text_, "1");
  if (type == "list")
    ASSERT_EQ(client.Command({"RPUSH", key, huge}).text_, "257");
  if (type == "zset")
    ASSERT_EQ(client.Command({"ZADD", key, "1000", huge}).text_, "1");
  ASSERT_EQ(client.Command({"EXPIRE", key, "3600"}).text_, "1");
}

void VerifyTransfer(Client& client, std::string_view type,
                    const std::string& key, const std::string& huge) {
  if (type == "hash") {
    EXPECT_EQ(client.Command({"HLEN", key}).text_, "257");
    EXPECT_EQ(client.Command({"HGET", key, "huge"}).text_, huge);
  }
  if (type == "set") {
    EXPECT_EQ(client.Command({"SCARD", key}).text_, "257");
    EXPECT_EQ(client.Command({"SISMEMBER", key, huge}).text_, "1");
  }
  if (type == "list") {
    EXPECT_EQ(client.Command({"LLEN", key}).text_, "257");
    EXPECT_EQ(client.Command({"LINDEX", key, "256"}).text_, huge);
  }
  if (type == "zset") {
    EXPECT_EQ(client.Command({"ZCARD", key}).text_, "257");
    EXPECT_EQ(client.Command({"ZSCORE", key, huge}).text_, "1000");
  }
  const auto ttl = client.Command({"PTTL", key});
  ASSERT_EQ(ttl.kind_, ':') << ttl.text_;
  EXPECT_GT(std::stoll(ttl.text_), 0);
}

TEST(GroupedTransferE2e, FourTypesCopyRenameAcrossDatabasesAndWorkersRecover) {
  const std::string huge(9 * 1024 * 1024, 'T');
  // One owner covers the single-shard shortcut; three cover source pin
  // lifetime while another owner deletes the source during the write hop.
  for (const unsigned workers : {1U, 3U}) {
    SCOPED_TRACE(workers);
    PrivateDisk disk;
    const int fd = ::open(disk.path().c_str(), O_RDWR);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(::ftruncate(fd, 2LL * 1024 * 1024 * 1024), 0);
    ::close(fd);
    {
      Server server(disk, workers);
      Client client(server.port());
      for (const auto* type : {"hash", "set", "list", "zset"}) {
        SCOPED_TRACE(type);
        const std::string source = std::string(type) + ":source";
        const std::string destination = std::string(type) + ":renamed";
        const std::string copied = std::string(type) + ":copied";
        PopulateTransfer(client, type, source, huge);
        ASSERT_EQ(client.Command({"COPY", source, copied, "DB", "2"}).text_,
                  "1")
            << server.Log();
        ASSERT_EQ(client.Command({"SELECT", "2"}).text_, "OK");
        VerifyTransfer(client, type, copied, huge);
        ASSERT_EQ(client.Command({"SELECT", "0"}).text_, "OK");
        ASSERT_EQ(client.Command({"SET", destination, "occupied"}).text_, "OK");
        ASSERT_EQ(client.Command({"RENAMENX", source, destination}).text_, "0");
        ASSERT_EQ(client.Command({"RENAME", source, destination}).text_, "OK")
            << server.Log();
        ASSERT_EQ(client.Command({"EXISTS", source}).text_, "0");
        VerifyTransfer(client, type, destination, huge);
      }
      client.Durable();
      ASSERT_EQ(server.Wait(true), 0) << server.Log();
    }
    Server recovered(disk, 4);
    Client client(recovered.port());
    for (const auto* type : {"hash", "set", "list", "zset"}) {
      VerifyTransfer(client, type, std::string(type) + ":renamed", huge);
      ASSERT_EQ(client.Command({"SELECT", "2"}).text_, "OK");
      VerifyTransfer(client, type, std::string(type) + ":copied", huge);
      ASSERT_EQ(client.Command({"SELECT", "0"}).text_, "OK");
    }
    ASSERT_EQ(recovered.Wait(true), 0) << recovered.Log();
  }
}

TEST(GroupedTransferE2e, FailedExecCopyKeepsOldGraphAndCommitsLaterCommand) {
#if !KEYLANE_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires group auxiliary failure injection";
#endif
  PrivateDisk disk;
  const std::string old_value(9 * 1024 * 1024, 'o');
  const std::string new_value(9 * 1024 * 1024, 'n');
  // A shared hash tag exercises destination ingestion after a previous write
  // on the same EXEC owner without conflating two physical disk populations.
  const std::string source = "source{transfer-undo}";
  const std::string destination = "destination{transfer-undo}";
  const std::string guard = "guard{transfer-undo}";
  {
    Server server(disk, 2);
    Client client(server.port());
    PopulateTransfer(client, "hash", source, new_value);
    PopulateTransfer(client, "hash", destination, old_value);
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  {
    Server server(disk, 3, {}, destination);
    Client client(server.port());
    ASSERT_EQ(client.Command({"MULTI"}).text_, "OK");
    ASSERT_EQ(client.Command({"SET", guard, "before"}).text_, "QUEUED");
    ASSERT_EQ(client.Command({"COPY", source, destination, "REPLACE"}).text_,
              "QUEUED");
    ASSERT_EQ(client.Command({"SET", guard, "after"}).text_, "QUEUED");
    const auto result = client.Command({"EXEC"});
    ASSERT_EQ(result.kind_, '*') << result.text_ << server.Log();
    ASSERT_EQ(result.items_.size(), 3U);
    EXPECT_EQ(result.items_[0].text_, "OK");
    EXPECT_EQ(result.items_[1].kind_, '-') << server.Log();
    EXPECT_EQ(result.items_[2].text_, "OK");
    VerifyTransfer(client, "hash", destination, old_value);
    VerifyTransfer(client, "hash", source, new_value);
    ASSERT_EQ(client.Command({"GET", guard}).text_, "after");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server recovered(disk, 4);
  Client client(recovered.port());
  VerifyTransfer(client, "hash", destination, old_value);
  VerifyTransfer(client, "hash", source, new_value);
  EXPECT_EQ(client.Command({"GET", guard}).text_, "after");
  ASSERT_EQ(recovered.Wait(true), 0) << recovered.Log();
}

TEST(GroupedTransferE2e, FullImageScratchOomPreservesValuesAndMetadataReads) {
  PrivateDisk disk;
  const std::string huge(9 * 1024 * 1024, 'M');
  {
    Server server(disk, 2);
    Client client(server.port());
    PopulateTransfer(client, "hash", "hash", huge);
    PopulateTransfer(client, "list", "list", huge);
    ASSERT_EQ(client.Command({"SET", "string", "unchanged"}).text_, "OK");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  {
    Server server(disk, 4, {}, {}, false, 2, "128M");
    Client client(server.port());
    for (const std::vector<std::string> command :
         {std::vector<std::string>{"HGETALL", "hash"},
          std::vector<std::string>{"LRANGE", "list", "0", "-1"}}) {
      const auto result = client.Command(command);
      EXPECT_EQ(result.kind_, '-');
      EXPECT_TRUE(result.text_.starts_with("OOM")) << result.text_;
    }
    EXPECT_EQ(client.Command({"HLEN", "hash"}).text_, "257");
    EXPECT_EQ(client.Command({"LLEN", "list"}).text_, "257");
    EXPECT_EQ(client.Command({"GET", "string"}).text_, "unchanged");
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server recovered(disk, 3);
  Client client(recovered.port());
  VerifyTransfer(client, "hash", "hash", huge);
  VerifyTransfer(client, "list", "list", huge);
  ASSERT_EQ(recovered.Wait(true), 0) << recovered.Log();
}

TEST(GroupedTransferE2e, ScanUsesOneLeafAndSurvivesDeletingEarlierFields) {
  PrivateDisk disk;
  const std::string huge(9 * 1024 * 1024, 'S');
  {
    Server server(disk, 2);
    Client client(server.port());
    PopulateTransfer(client, "hash", "hash", huge);
    PopulateTransfer(client, "set", "set", huge);
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  {
    // Recovery changes the process digest seed. Grouped scan must use its
    // persisted routing seed, not the compact decoder's current-process hash.
    Server server(disk, 4, {}, {}, false, 2, "128M");
    Client client(server.port());
    for (const bool hash : {true, false}) {
      const std::string key = hash ? "hash" : "set";
      std::string cursor = "0";
      std::set<std::string> seen;
      unsigned calls = 0;
      do {
        const auto reply =
            client.Command({hash ? "HSCAN" : "SSCAN", key, cursor, "MATCH",
                            "item*", "COUNT", "1"});
        ASSERT_EQ(reply.kind_, '*') << reply.text_ << server.Log();
        ASSERT_EQ(reply.items_.size(), 2U);
        const auto& values = reply.items_[1];
        ASSERT_EQ(values.kind_, '*');
        for (std::size_t i = 0; i < values.items_.size(); i += hash ? 2 : 1) {
          const auto& field = values.items_[i].text_;
          EXPECT_TRUE(seen.insert(field).second);
          // Rank-based cursors would now skip an unchanged later field.
          ASSERT_EQ(client.Command({hash ? "HDEL" : "SREM", key, field}).text_,
                    "1");
        }
        const auto next = reply.items_[0].text_;
        if (next != "0") EXPECT_GT(std::stoull(next), std::stoull(cursor));
        cursor = next;
        ASSERT_LT(++calls, 1024U);
      } while (cursor != "0");
      EXPECT_EQ(seen.size(), 256U);
      EXPECT_EQ(client.Command({hash ? "HLEN" : "SCARD", key}).text_, "1");
    }
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server recovered(disk, 3);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"HGET", "hash", "huge"}).text_, huge);
  EXPECT_EQ(client.Command({"SISMEMBER", "set", huge}).text_, "1");
  ASSERT_EQ(recovered.Wait(true), 0) << recovered.Log();
}

TEST(GroupedTransferE2e, ListSearchRetainsOnePageAndInsertOnlyItsInterval) {
  PrivateDisk disk;
  const std::string huge(9 * 1024 * 1024, 'L');
  const std::string pivot = "item10" + std::string(128, 'm');
  {
    Server server(disk, 2);
    Client client(server.port());
    PopulateTransfer(client, "list", "list", huge);
    ASSERT_EQ(client.Command({"RPUSH", "list", "needle", "needle"}).text_,
              "259");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  {
    // Four owner shares cannot admit a four-copy image of the oversized
    // unrelated item. Searching still needs only one decoded page at a time.
    Server server(disk, 4, {}, {}, false, 2, "128M");
    Client client(server.port());
    ASSERT_NE(client.Command({"INFO", "MEMORY"})
                  .text_.find("maxmemory:134217728\r\n"),
              std::string::npos);
    EXPECT_EQ(client.Command({"LPOS", "list", "needle"}).text_, "257");
    EXPECT_EQ(client.Command({"LPOS", "list", "needle", "RANK", "-1"}).text_,
              "258");
    const auto all = client.Command({"LPOS", "list", "needle", "COUNT", "0"});
    ASSERT_EQ(all.kind_, '*') << all.text_ << server.Log();
    ASSERT_EQ(all.items_.size(), 2U);
    EXPECT_EQ(all.items_[0].text_, "257");
    EXPECT_EQ(all.items_[1].text_, "258");
    const auto reverse = client.Command(
        {"LPOS", "list", "needle", "RANK", "-2", "COUNT", "0", "MAXLEN", "2"});
    ASSERT_EQ(reverse.kind_, '*') << reverse.text_;
    ASSERT_EQ(reverse.items_.size(), 1U);
    EXPECT_EQ(reverse.items_[0].text_, "257");
    EXPECT_EQ(client.Command({"LPOS", "list", "absent"}).kind_, '$');
    EXPECT_EQ(
        client.Command({"LINSERT", "list", "BEFORE", "absent", "x"}).text_,
        "-1");
    ASSERT_EQ(
        client.Command({"LINSERT", "list", "BEFORE", pivot, "inserted"}).text_,
        "260")
        << server.Log();
    EXPECT_EQ(client.Command({"LINDEX", "list", "10"}).text_, "inserted");
    EXPECT_EQ(client.Command({"LINDEX", "list", "11"}).text_, pivot);
    EXPECT_EQ(client.Command({"LPOS", "list", "needle"}).text_, "258");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server recovered(disk, 3);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"LLEN", "list"}).text_, "260");
  EXPECT_EQ(client.Command({"LINDEX", "list", "257"}).text_, huge);
  EXPECT_EQ(client.Command({"LINDEX", "list", "10"}).text_, "inserted");
  ASSERT_EQ(recovered.Wait(true), 0) << recovered.Log();
}

TEST(GroupedTransferE2e, RandomFieldsAndSetPopDoNotMaterializeTheCollection) {
  PrivateDisk disk;
  const std::string huge(9 * 1024 * 1024, 'H');
  std::set<std::string> members;
  for (unsigned i = 0; i < 256; ++i)
    members.insert(std::string(96 * 1024, 's') + std::to_string(i));
  {
    Server server(disk, 2);
    Client client(server.port());
    PopulateTransfer(client, "hash", "hash", huge);
    std::vector<std::string> command{"SADD", "set"};
    command.insert(command.end(), members.begin(), members.end());
    ASSERT_EQ(client.Command(command).text_, "256");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  std::set<std::string> removed;
  {
    Server server(disk, 4, {}, {}, false, 2, "128M");
    Client client(server.port());
    ASSERT_NE(client.Command({"INFO", "MEMORY"})
                  .text_.find("maxmemory:134217728\r\n"),
              std::string::npos);
    const auto fields = client.Command({"HRANDFIELD", "hash", "257"});
    ASSERT_EQ(fields.kind_, '*') << fields.text_ << server.Log();
    ASSERT_EQ(fields.items_.size(), 257U);
    std::set<std::string> field_names;
    for (const auto& field : fields.items_)
      EXPECT_TRUE(field_names.insert(field.text_).second);
    EXPECT_TRUE(field_names.contains("huge"));
    const auto repeats = client.Command({"HRANDFIELD", "hash", "-500"});
    ASSERT_EQ(repeats.kind_, '*') << repeats.text_;
    ASSERT_EQ(repeats.items_.size(), 500U);
    for (const auto& field : repeats.items_)
      EXPECT_TRUE(field_names.contains(field.text_));
    const auto pairs =
        client.Command({"HRANDFIELD", "hash", "257", "WITHVALUES"});
    ASSERT_EQ(pairs.kind_, '*') << pairs.text_ << server.Log();
    ASSERT_EQ(pairs.items_.size(), 514U);
    for (std::size_t i = 0; i < pairs.items_.size(); i += 2)
      EXPECT_EQ(pairs.items_[i + 1].text_,
                pairs.items_[i].text_ == "huge" ? huge : "value");
    const auto sampled = client.Command({"SRANDMEMBER", "set", "3"});
    ASSERT_EQ(sampled.kind_, '*') << sampled.text_;
    ASSERT_EQ(sampled.items_.size(), 3U);
    std::set<std::string> sampled_names;
    for (const auto& member : sampled.items_) {
      EXPECT_TRUE(members.contains(member.text_));
      EXPECT_TRUE(sampled_names.insert(member.text_).second);
    }
    // A command changing the whole graph must still reject insufficient
    // admission atomically; a small pop subsequently needs only selected pages.
    const auto exhausted = client.Command({"SPOP", "set", "9999"});
    ASSERT_EQ(exhausted.kind_, '-') << exhausted.text_;
    EXPECT_NE(exhausted.text_.find("OOM"), std::string::npos);
    EXPECT_EQ(client.Command({"SCARD", "set"}).text_, "256");
    const auto popped = client.Command({"SPOP", "set", "16"});
    ASSERT_EQ(popped.kind_, '*') << popped.text_ << server.Log();
    ASSERT_EQ(popped.items_.size(), 16U);
    for (const auto& member : popped.items_) {
      EXPECT_TRUE(members.contains(member.text_));
      EXPECT_TRUE(removed.insert(member.text_).second);
      EXPECT_EQ(client.Command({"SISMEMBER", "set", member.text_}).text_, "0");
    }
    EXPECT_EQ(client.Command({"SCARD", "set"}).text_, "240");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server recovered(disk, 3);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"SCARD", "set"}).text_, "240");
  for (const auto& member : removed)
    EXPECT_EQ(client.Command({"SISMEMBER", "set", member}).text_, "0");
  EXPECT_EQ(client.Command({"HGET", "hash", "huge"}).text_, huge);
  ASSERT_EQ(recovered.Wait(true), 0) << recovered.Log();
}

TEST(GroupedTransferE2e, SetMultiKeyOomLeavesDestinationAndHoldsIntact) {
  PrivateDisk disk;
  std::vector<std::string> members;
  for (unsigned i = 0; i < 160; ++i) {
    members.push_back(std::to_string(i) + std::string(32 * 1024, 's'));
  }
  const std::string first = "{set-aggregate}:first";
  const std::string second = "{set-aggregate}:second";
  const std::string destination = "{set-aggregate}:destination";
  {
    Server server(disk, 2);
    Client client(server.port());
    for (const auto& key : {first, second}) {
      std::vector<std::string> add{"SADD", key};
      add.insert(add.end(), members.begin(), members.end());
      ASSERT_EQ(client.Command(add).text_, "160") << server.Log();
    }
    ASSERT_EQ(client.Command({"SADD", destination, "sentinel"}).text_, "1");
    ASSERT_EQ(client.Command({"SET", "wrong-type", "original"}).text_, "OK");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  {
    Server server(disk, 4, {}, {}, false, 2, "128M");
    Client client(server.port());
    // Each source is readable independently. Their simultaneously retained
    // inputs plus the legacy owning aggregation copies exceed an owner share.
    // Repetition also catches a charge or transaction-hold leak on rejection.
    for (unsigned attempt = 0; attempt < 3; ++attempt) {
      for (const auto& command : std::vector<std::vector<std::string>>{
               {"SUNION", first, second},
               {"SINTERSTORE", destination, first, second},
               {"SUNIONSTORE", destination, first, second}}) {
        const auto exhausted = client.Command(command);
        ASSERT_EQ(exhausted.kind_, '-') << exhausted.text_ << server.Log();
        EXPECT_NE(exhausted.text_.find("OOM"), std::string::npos);
        EXPECT_EQ(client.Command({"SCARD", destination}).text_, "1");
        EXPECT_EQ(client.Command({"SISMEMBER", destination, "sentinel"}).text_,
                  "1");
        EXPECT_EQ(client.Command({"SCARD", first}).text_, "160");
        EXPECT_EQ(client.Command({"SCARD", second}).text_, "160");
      }
    }
    ASSERT_EQ(client.Command({"MULTI"}).text_, "OK");
    ASSERT_EQ(client.Command({"SET", "set-prefix", "kept"}).text_, "QUEUED");
    ASSERT_EQ(client.Command({"SINTERSTORE", destination, first, second}).text_,
              "QUEUED");
    ASSERT_EQ(client.Command({"SMOVE", first, destination, members[1]}).text_,
              "QUEUED");
    ASSERT_EQ(client.Command({"SET", "set-after-exec", "ok"}).text_, "QUEUED");
    const auto executed = client.Command({"EXEC"});
    ASSERT_EQ(executed.kind_, '*') << executed.text_ << server.Log();
    ASSERT_EQ(executed.items_.size(), 4U);
    EXPECT_EQ(executed.items_[0].text_, "OK");
    EXPECT_EQ(executed.items_[1].kind_, '-');
    EXPECT_NE(executed.items_[1].text_.find("OOM"), std::string::npos);
    EXPECT_EQ(executed.items_[2].text_, "1");
    EXPECT_EQ(executed.items_[3].text_, "OK");
    const auto scripted = client.Command(
        {"EVAL",
         "return {redis.pcall('SUNIONSTORE',KEYS[1],KEYS[2],KEYS[3]),"
         "redis.call('SET',KEYS[4],'ok')}",
         "4", destination, first, second, "set-after-lua"});
    ASSERT_EQ(scripted.kind_, '*') << scripted.text_ << server.Log();
    ASSERT_EQ(scripted.items_.size(), 2U);
    EXPECT_EQ(scripted.items_[0].kind_, '-');
    EXPECT_NE(scripted.items_[0].text_.find("OOM"), std::string::npos);
    EXPECT_EQ(scripted.items_[1].text_, "OK");
    EXPECT_EQ(client.Command({"SISMEMBER", destination, "sentinel"}).text_,
              "1");
    // Destination validation fails before removing the source member. A later
    // successful cross-key move and STORE prove rejection released the holds.
    const auto wrong =
        client.Command({"SMOVE", first, "wrong-type", members.front()});
    EXPECT_EQ(wrong.kind_, '-');
    EXPECT_NE(wrong.text_.find("WRONGTYPE"), std::string::npos);
    EXPECT_EQ(client.Command({"SISMEMBER", first, members.front()}).text_, "1");
    ASSERT_EQ(
        client.Command({"SMOVE", first, destination, members.front()}).text_,
        "1")
        << server.Log();
    ASSERT_EQ(client.Command({"SADD", "small-set", "small"}).text_, "1");
    ASSERT_EQ(client.Command({"SINTERSTORE", "small-copy", "small-set"}).text_,
              "1")
        << server.Log();
    ASSERT_EQ(client.Command({"SET", "after-set-oom", "ok"}).text_, "OK");
    EXPECT_EQ(client.Command({"GET", "after-set-oom"}).text_, "ok");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server recovered(disk, 3);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"SCARD", first}).text_, "158");
  EXPECT_EQ(client.Command({"SCARD", second}).text_, "160");
  EXPECT_EQ(client.Command({"SCARD", destination}).text_, "3");
  EXPECT_EQ(client.Command({"SISMEMBER", destination, "sentinel"}).text_, "1");
  EXPECT_EQ(client.Command({"SISMEMBER", destination, members.front()}).text_,
            "1");
  EXPECT_EQ(client.Command({"SISMEMBER", destination, members[1]}).text_, "1");
  EXPECT_EQ(client.Command({"SISMEMBER", "small-copy", "small"}).text_, "1");
  EXPECT_EQ(client.Command({"GET", "wrong-type"}).text_, "original");
  EXPECT_EQ(client.Command({"GET", "after-set-oom"}).text_, "ok");
  EXPECT_EQ(client.Command({"GET", "set-prefix"}).text_, "kept");
  EXPECT_EQ(client.Command({"GET", "set-after-exec"}).text_, "ok");
  EXPECT_EQ(client.Command({"GET", "set-after-lua"}).text_, "ok");
  ASSERT_EQ(recovered.Wait(true), 0) << recovered.Log();
}

}  // namespace
