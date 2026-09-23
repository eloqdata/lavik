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

#include "grouped_write_e2e_support.h"
#include "lavik/storage/detail/collection_limits.h"

namespace {
using namespace grouped_e2e;

void PopulateStream(Client& client, std::string_view key, unsigned count,
                    unsigned field_bytes = 8192) {
  const std::string field("f\0x", 3);
  const std::string value(field_bytes, 'v');
  for (unsigned i = 1; i <= count; ++i) {
    const auto id = std::to_string(i) + "-0";
    ASSERT_EQ(
        client.Command({"XADD", std::string(key), id, field, value}).text_, id)
        << "entry " << i;
  }
}

TEST(GroupedStreamE2e, SmallStreamKeepsCompactStorage) {
  PrivateDisk disk;
  {
    Server server(disk);
    Client client(server.port());
    PopulateStream(client, "small", 32, 256);
    EXPECT_EQ(client.Command({"XLEN", "small"}).text_, "32");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  EXPECT_TRUE(disk.Auxiliaries("small").empty());
}

TEST(GroupedStreamE2e, PromotesAtSharedSizeBoundaryAndRecovers) {
  PrivateDisk disk;
  // LXS1 with one message, field "f", one logical node and no groups has
  // 89 bytes of framing. Exercise immediately below, at and above promotion.
  const std::string below(kCollectionPromotionBytes - 89 - 1, 'v');
  const std::string at(kCollectionPromotionBytes - 89, 'v');
  const std::string above(kCollectionPromotionBytes - 89 + 1, 'v');
  {
    Server server(disk);
    Client client(server.port());
    for (const auto& [key, value] :
         {std::pair{"below", below}, {"at", at}, {"above", above}}) {
      ASSERT_EQ(client.Command({"XADD", key, "1-0", "f", value}).text_, "1-0");
    }
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  EXPECT_TRUE(disk.Auxiliaries("below").empty());
  EXPECT_FALSE(disk.Auxiliaries("at").empty());
  EXPECT_FALSE(disk.Auxiliaries("above").empty());
  {
    Server server(disk, 3);
    Client client(server.port());
    for (const auto& [key, value] :
         {std::pair{"below", below}, {"at", at}, {"above", above}}) {
      auto range = client.Command({"XRANGE", key, "-", "+"});
      ASSERT_EQ(range.kind_, '*') << range.text_;
      ASSERT_EQ(range.items_.size(), 1);
      ASSERT_EQ(range.items_[0].items_.size(), 2);
      ASSERT_EQ(range.items_[0].items_[1].items_.size(), 2);
      EXPECT_EQ(range.items_[0].items_[1].items_[1].text_, value);
    }
    ASSERT_EQ(client.Command({"XADD", "below", "2-0", "f", "tail"}).text_,
              "2-0");
    // A grouped Stream stays grouped after shrinking below promotion size.
    ASSERT_EQ(client.Command({"XDEL", "below", "1-0"}).text_, "1");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  EXPECT_FALSE(disk.Auxiliaries("below").empty());
  Server recovered(disk);
  Client client(recovered.port());
  auto range = client.Command({"XRANGE", "below", "-", "+"});
  ASSERT_EQ(range.kind_, '*') << range.text_;
  ASSERT_EQ(range.items_.size(), 1);
  EXPECT_EQ(range.items_[0].items_[0].text_, "2-0");
  EXPECT_EQ(range.items_[0].items_[1].items_[1].text_, "tail");
}

TEST(GroupedStreamE2e, AppendOnlyRewritesBoundedPagesAndRecovers) {
  PrivateDisk disk;
  {
    Server server(disk);
    Client client(server.port());
    PopulateStream(client, "stream", 800);
    ASSERT_EQ(client.Command({"XGROUP", "CREATE", "stream", "g", "0"}).text_,
              "OK");
    ASSERT_EQ(client
                  .Command({"XREADGROUP", "GROUP", "g", "c", "COUNT", "200",
                            "STREAMS", "stream", ">"})
                  .kind_,
              '*');
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  const auto before = disk.Auxiliaries("stream");
  ASSERT_FALSE(before.empty());
  {
    Server server(disk, 3);
    Client client(server.port());
    ASSERT_EQ(client.Command({"XADD", "stream", "801-0", "f", "tail"}).text_,
              "801-0");
    ASSERT_EQ(client.Command({"XLEN", "stream"}).text_, "801");
    auto range = client.Command({"XRANGE", "stream", "800-0", "+"});
    ASSERT_EQ(range.kind_, '*') << range.text_;
    ASSERT_EQ(range.items_.size(), 2);
    EXPECT_EQ(range.items_[1].items_[0].text_, "801-0");
    auto pending = client.Command({"XPENDING", "stream", "g"});
    ASSERT_EQ(pending.kind_, '*') << pending.text_;
    EXPECT_EQ(pending.items_[0].text_, "200");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  const auto after = disk.Auxiliaries("stream");
  ASSERT_FALSE(after.empty());
  ASSERT_GT(after.rbegin()->first, before.rbegin()->first);
  EXPECT_LE(after.rbegin()->second.size(), 7);
  Server recovered(disk);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"XLEN", "stream"}).text_, "801");
  EXPECT_EQ(client.Command({"XACK", "stream", "g", "1-0", "200-0"}).text_, "2");
}

TEST(GroupedStreamE2e, DeleteRoutesIdsAndPreservesPendingAndNodeBoundaries) {
  PrivateDisk disk;
  {
    Server server(disk);
    Client client(server.port());
    ASSERT_EQ(
        client.Command({"CONFIG", "SET", "stream-node-max-entries", "3"}).text_,
        "OK");
    PopulateStream(client, "s", 200);
    ASSERT_EQ(client.Command({"XGROUP", "CREATE", "s", "g", "0"}).text_, "OK");
    ASSERT_EQ(client
                  .Command({"XREADGROUP", "GROUP", "g", "c", "COUNT", "10",
                            "STREAMS", "s", ">"})
                  .kind_,
              '*');
    ASSERT_EQ(client
                  .Command({"XDEL", "s", "1-0", "1-0", "4-0", "5-0", "6-0",
                            "199-0", "200-0", "999-0"})
                  .text_,
              "6");
    ASSERT_EQ(client.Command({"XLEN", "s"}).text_, "194");
    auto pending = client.Command({"XPENDING", "s", "g"});
    ASSERT_EQ(pending.kind_, '*') << pending.text_;
    ASSERT_EQ(pending.items_[0].text_, "10");
    // The first node now has two messages. Changing the configuration must
    // not redefine it, nor may deleting a whole node merge its neighbours.
    ASSERT_EQ(
        client.Command({"CONFIG", "SET", "stream-node-max-entries", "100"})
            .text_,
        "OK");
    EXPECT_EQ(client.Command({"XTRIM", "s", "MAXLEN", "~", "193", "LIMIT", "0"})
                  .text_,
              "0");
    EXPECT_EQ(client.Command({"XTRIM", "s", "MAXLEN", "~", "192", "LIMIT", "0"})
                  .text_,
              "2");
    EXPECT_EQ(
        client.Command({"XACK", "s", "g", "1-0", "4-0", "5-0", "6-0"}).text_,
        "4");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server server(disk, 3);
  Client client(server.port());
  EXPECT_EQ(client.Command({"XLEN", "s"}).text_, "192");
  auto first = client.Command({"XRANGE", "s", "-", "+", "COUNT", "1"});
  ASSERT_EQ(first.kind_, '*') << first.text_;
  ASSERT_EQ(first.items_.size(), 1);
  EXPECT_EQ(first.items_[0].items_[0].text_, "7-0");
}

TEST(GroupedStreamE2e, EmptyStreamRetainsGroupsAndPendingAcrossRecovery) {
  PrivateDisk disk;
  {
    Server server(disk);
    Client client(server.port());
    PopulateStream(client, "stream", 150);
    ASSERT_EQ(client.Command({"XGROUP", "CREATE", "stream", "g", "0"}).text_,
              "OK");
    ASSERT_EQ(client
                  .Command({"XREADGROUP", "GROUP", "g", "c", "COUNT", "10",
                            "STREAMS", "stream", ">"})
                  .kind_,
              '*');
    ASSERT_EQ(client.Command({"XTRIM", "stream", "MAXLEN", "0"}).text_, "150");
    ASSERT_EQ(client.Command({"XLEN", "stream"}).text_, "0");
    EXPECT_EQ(client.Command({"TYPE", "stream"}).text_, "stream");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server recovered(disk, 3);
  Client client(recovered.port());
  auto pending = client.Command({"XPENDING", "stream", "g"});
  ASSERT_EQ(pending.kind_, '*') << pending.text_;
  EXPECT_EQ(pending.items_[0].text_, "10");
  auto deleted = client.Command({"XREADGROUP", "GROUP", "g", "c", "COUNT", "1",
                                 "STREAMS", "stream", "0"});
  ASSERT_EQ(deleted.kind_, '*') << deleted.text_;
  EXPECT_EQ(client.Command({"XADD", "stream", "151-0", "f", "new"}).text_,
            "151-0");
  EXPECT_EQ(client.Command({"XLEN", "stream"}).text_, "1");
  EXPECT_EQ(client.Command({"XACK", "stream", "g", "1-0"}).text_, "1");
}

TEST(GroupedStreamE2e, BinaryNamesLargeIdsAndApproximateNodes) {
  PrivateDisk disk;
  Server server(disk);
  Client client(server.port());
  ASSERT_EQ(
      client.Command({"CONFIG", "SET", "stream-node-max-entries", "7"}).text_,
      "OK");
  const std::string group("g\0z", 3), consumer("c\0z", 3);
  for (unsigned i = 1; i <= 150; ++i) {
    const auto id = "9007199254740993-" + std::to_string(i);
    ASSERT_EQ(
        client.Command({"XADD", "s", id, "f", std::string(8192, 'a')}).text_,
        id);
  }
  ASSERT_EQ(client.Command({"XGROUP", "CREATE", "s", group, "0"}).text_, "OK");
  ASSERT_EQ(client
                .Command({"XREADGROUP", "GROUP", group, consumer, "COUNT", "3",
                          "STREAMS", "s", ">"})
                .kind_,
            '*');
  ASSERT_EQ(client.Command({"XTRIM", "s", "MAXLEN", "~", "100"}).text_, "49");
  EXPECT_EQ(client.Command({"XLEN", "s"}).text_, "101");
  EXPECT_EQ(client.Command({"XACK", "s", group, "9007199254740993-1"}).text_,
            "1");
  auto dump = client.Command({"DUMP", "s"});
  ASSERT_EQ(dump.kind_, '$') << dump.text_;
  ASSERT_EQ(client.Command({"RESTORE", "copy", "0", dump.text_}).text_, "OK");
  EXPECT_EQ(client.Command({"XLEN", "copy"}).text_, "101");
  auto info = client.Command({"XPENDING", "copy", group});
  ASSERT_EQ(info.kind_, '*') << info.text_;
  EXPECT_EQ(info.items_[0].text_, "2");
}
TEST(GroupedStreamE2e, MultipleConsumersSparseAcksCopyAndRename) {
  PrivateDisk disk;
  {
    Server server(disk);
    Client client(server.port());
    PopulateStream(client, "s", 400);
    ASSERT_EQ(client.Command({"XGROUP", "CREATE", "s", "g", "0"}).text_, "OK");
    ASSERT_EQ(client.Command({"XGROUP", "CREATE", "s", "other", "$"}).text_,
              "OK");
    for (unsigned i = 0; i < 20; ++i) {
      auto read = client.Command({"XREADGROUP", "GROUP", "g", i % 2 ? "a" : "b",
                                  "COUNT", "5", "STREAMS", "s", ">"});
      ASSERT_EQ(read.kind_, '*') << read.text_;
      ASSERT_EQ(read.items_.size(), 1);
      ASSERT_EQ(read.items_[0].items_[1].items_.size(), 5);
    }
    EXPECT_EQ(
        client.Command({"XACK", "s", "g", "1-0", "1-0", "50-0", "999-0"}).text_,
        "2");
    auto pending = client.Command({"XPENDING", "s", "g"});
    ASSERT_EQ(pending.kind_, '*') << pending.text_;
    EXPECT_EQ(pending.items_[0].text_, "98");
    auto consumers = client.Command({"XINFO", "CONSUMERS", "s", "g"});
    ASSERT_EQ(consumers.kind_, '*') << consumers.text_;
    EXPECT_EQ(consumers.items_.size(), 2);
    ASSERT_EQ(client.Command({"PEXPIRE", "s", "600000"}).text_, "1");
    ASSERT_EQ(client.Command({"COPY", "s", "copy"}).text_, "1");
    ASSERT_EQ(client.Command({"RENAME", "copy", "renamed"}).text_, "OK");
    ASSERT_EQ(client.Command({"XLEN", "renamed"}).text_, "400");
    EXPECT_EQ(client.Command({"XACK", "renamed", "g", "2-0"}).text_, "1");
    EXPECT_EQ(client.Command({"XACK", "s", "g", "2-0"}).text_, "1");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server recovered(disk, 3);
  Client client(recovered.port());
  for (const std::string key : {"s", "renamed"}) {
    EXPECT_EQ(client.Command({"XLEN", key}).text_, "400");
    auto pending = client.Command({"XPENDING", key, "g"});
    ASSERT_EQ(pending.kind_, '*') << pending.text_;
    EXPECT_EQ(pending.items_[0].text_, "97");
    EXPECT_GT(std::stoll(client.Command({"PTTL", key}).text_), 0);
  }
}

TEST(GroupedStreamE2e, FailedAppendCannotLeakIntoFollowingAckInExec) {
#if !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires grouped auxiliary fault hook";
#endif
  PrivateDisk disk;
  {
    Server server(disk);
    Client client(server.port());
    PopulateStream(client, "s", 200);
    ASSERT_EQ(client.Command({"XGROUP", "CREATE", "s", "g", "0"}).text_, "OK");
    ASSERT_EQ(client
                  .Command({"XREADGROUP", "GROUP", "g", "a", "COUNT", "100",
                            "STREAMS", "s", ">"})
                  .kind_,
              '*');
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  {
    Server server(disk, 2, {}, "s", false, 2);
    Client client(server.port());
    ASSERT_EQ(client.Command({"MULTI"}).text_, "OK");
    ASSERT_EQ(client.Command({"XADD", "s", "201-0", "f", "failed"}).text_,
              "QUEUED");
    ASSERT_EQ(client.Command({"XACK", "s", "g", "1-0"}).text_, "QUEUED");
    auto reply = client.Command({"EXEC"});
    ASSERT_EQ(reply.kind_, '*') << reply.text_;
    ASSERT_EQ(reply.items_.size(), 2);
    EXPECT_EQ(reply.items_[0].kind_, '-');
    EXPECT_EQ(reply.items_[1].text_, "1");
    EXPECT_EQ(client.Command({"XLEN", "s"}).text_, "200");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server recovered(disk, 3);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"XLEN", "s"}).text_, "200");
  auto pending = client.Command({"XPENDING", "s", "g"});
  ASSERT_EQ(pending.kind_, '*') << pending.text_;
  EXPECT_EQ(pending.items_[0].text_, "99");
}

TEST(GroupedStreamE2e, RewoundNoAckAndForceClaimPreserveUnloadedPending) {
  PrivateDisk disk;
  Server server(disk);
  Client client(server.port());
  PopulateStream(client, "s", 200);
  ASSERT_EQ(client.Command({"XGROUP", "CREATE", "s", "g", "0"}).text_, "OK");
  ASSERT_EQ(client
                .Command({"XREADGROUP", "GROUP", "g", "a", "COUNT", "50",
                          "STREAMS", "s", ">"})
                .kind_,
            '*');
  ASSERT_EQ(
      client.Command({"XGROUP", "SETID", "s", "g", "0", "ENTRIESREAD", "0"})
          .text_,
      "OK");
  ASSERT_EQ(client
                .Command({"XREADGROUP", "GROUP", "g", "b", "COUNT", "2",
                          "NOACK", "STREAMS", "s", ">"})
                .kind_,
            '*');
  auto claimed = client.Command({"XCLAIM", "s", "g", "c", "0", "100-0", "FORCE",
                                 "JUSTID", "RETRYCOUNT", "7"});
  ASSERT_EQ(claimed.kind_, '*') << claimed.text_;
  ASSERT_EQ(claimed.items_.size(), 1);
  EXPECT_EQ(claimed.items_[0].text_, "100-0");
  auto pending = client.Command({"XPENDING", "s", "g"});
  ASSERT_EQ(pending.kind_, '*') << pending.text_;
  EXPECT_EQ(pending.items_[0].text_, "51");
  EXPECT_EQ(client.Command({"XACK", "s", "g", "1-0", "50-0", "100-0"}).text_,
            "3");
  EXPECT_EQ(client.Command({"XLEN", "s"}).text_, "200");
}

TEST(GroupedStreamE2e, ExpirationDeletionAndFlushCannotResurrectOldGraphs) {
  PrivateDisk disk;
  {
    Server server(disk);
    Client client(server.port());
    PopulateStream(client, "s", 150);
    ASSERT_EQ(client.Command({"XGROUP", "CREATE", "s", "g", "0"}).text_, "OK");
    ASSERT_EQ(client.Command({"COPY", "s", "expired"}).text_, "1");
    ASSERT_EQ(client.Command({"COPY", "s", "deleted"}).text_, "1");
    ASSERT_EQ(client.Command({"PEXPIREAT", "expired", "1"}).text_, "1");
    ASSERT_EQ(client.Command({"DEL", "deleted"}).text_, "1");
    ASSERT_EQ(client.Command({"EXISTS", "expired", "deleted"}).text_, "0");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  {
    Server server(disk, 3);
    Client client(server.port());
    ASSERT_EQ(client.Command({"EXISTS", "expired", "deleted"}).text_, "0");
    ASSERT_EQ(client.Command({"XLEN", "s"}).text_, "150");
    ASSERT_EQ(client.Command({"FLUSHDB"}).text_, "OK");
    ASSERT_EQ(client.Command({"XADD", "s", "1-0", "f", "fresh"}).text_, "1-0");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server server(disk);
  Client client(server.port());
  EXPECT_EQ(client.Command({"XLEN", "s"}).text_, "1");
  auto groups = client.Command({"XINFO", "GROUPS", "s"});
  ASSERT_EQ(groups.kind_, '*') << groups.text_;
  EXPECT_TRUE(groups.items_.empty());
  EXPECT_EQ(client.Command({"EXISTS", "expired", "deleted"}).text_, "0");
}

TEST(GroupedStreamE2e, AggregateAbove512MiBKeepsHotPathsAndRecoveryBounded) {
  PrivateDisk disk(4ULL * 1024 * 1024 * 1024);
  {
    Server server(disk, 2, {}, {}, false, 2, "256M");
    Client client(server.port());
    ASSERT_EQ(
        client.Command({"XGROUP", "CREATE", "s", "g", "0", "MKSTREAM"}).text_,
        "OK");
    // One entry fits the admitted page workspace; the complete Stream exceeds
    // both maxmemory and the old compact image's 512 MiB encoding limit.
    PopulateStream(client, "s", 2050, 256 * 1024);
    ASSERT_EQ(client.Command({"XLEN", "s"}).text_, "2050");
    ASSERT_EQ(client.Command({"XGROUP", "CREATE", "s", "new", "$"}).text_,
              "OK");
    ASSERT_EQ(client.Command({"XGROUP", "DESTROY", "s", "new"}).text_, "1");
    ASSERT_EQ(
        client.Command({"XSETID", "s", "2050-0", "ENTRIESADDED", "2050"}).text_,
        "OK");
    EXPECT_EQ(
        client.Command({"XSETID", "s", "2050-0", "ENTRIESADDED", "1"}).kind_,
        '-');
    EXPECT_EQ(client.Command({"XINFO", "STREAM", "s"}).kind_, '*');
    EXPECT_EQ(
        client.Command({"XINFO", "STREAM", "s", "FULL", "COUNT", "1"}).kind_,
        '*');
    auto range = client.Command({"XRANGE", "s", "2049-0", "+", "COUNT", "1"});
    ASSERT_EQ(range.kind_, '*') << range.text_;
    ASSERT_EQ(range.items_.size(), 1);
    EXPECT_EQ(range.items_[0].items_[0].text_, "2049-0");
    EXPECT_EQ(range.items_[0].items_[1].items_[1].text_,
              std::string(256 * 1024, 'v'));
    auto delivered = client.Command(
        {"XREADGROUP", "GROUP", "g", "c", "COUNT", "1", "STREAMS", "s", ">"});
    ASSERT_EQ(delivered.kind_, '*') << delivered.text_;
    ASSERT_EQ(delivered.items_.size(), 1);
    EXPECT_EQ(delivered.items_[0].items_[1].items_[0].items_[0].text_, "1-0");
    EXPECT_EQ(client.Command({"XPENDING", "s", "g"}).kind_, '*');
    EXPECT_EQ(client.Command({"XPENDING", "s", "g", "-", "+", "1"}).kind_, '*');
    EXPECT_EQ(client.Command({"XINFO", "GROUPS", "s"}).kind_, '*');
    EXPECT_EQ(client.Command({"XINFO", "CONSUMERS", "s", "g"}).kind_, '*');
    EXPECT_EQ(client
                  .Command({"XREADGROUP", "GROUP", "g", "c", "COUNT", "1",
                            "STREAMS", "s", "0"})
                  .kind_,
              '*');
    EXPECT_EQ(client
                  .Command({"XAUTOCLAIM", "s", "g", "other", "0", "0-0",
                            "COUNT", "1", "JUSTID"})
                  .kind_,
              '*');
    EXPECT_EQ(
        client.Command({"XGROUP", "DELCONSUMER", "s", "g", "other"}).text_,
        "1");
    ASSERT_EQ(client.Command({"XACK", "s", "g", "1-0"}).text_, "0");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  {
    Server server(disk, 3, {}, {}, false, 2, "256M");
    Client client(server.port());
    ASSERT_EQ(client.Command({"XLEN", "s"}).text_, "2050");
    ASSERT_EQ(client.Command({"XADD", "s", "2051-0", "f", "tail"}).text_,
              "2051-0");
    auto delivered = client.Command(
        {"XREADGROUP", "GROUP", "g", "c", "COUNT", "1", "STREAMS", "s", ">"});
    ASSERT_EQ(delivered.kind_, '*') << delivered.text_;
    ASSERT_EQ(delivered.items_.size(), 1);
    EXPECT_EQ(delivered.items_[0].items_[1].items_[0].items_[0].text_, "2-0");
    EXPECT_EQ(client.Command({"XACK", "s", "g", "2-0"}).text_, "1");
    EXPECT_EQ(client.Command({"XDEL", "s", "3-0", "1000-0", "3-0"}).text_, "2");
    EXPECT_EQ(
        client.Command({"XADD", "s", "MAXLEN", "2000", "2052-0", "f", "trim"})
            .text_,
        "2052-0");
    EXPECT_EQ(client.Command({"XLEN", "s"}).text_, "2000");
    EXPECT_EQ(client.Command({"XTRIM", "s", "MINID", "2050-0"}).text_, "1997");
    EXPECT_EQ(client.Command({"XLEN", "s"}).text_, "3");
    EXPECT_EQ(client.Command({"XTRIM", "s", "MAXLEN", "0"}).text_, "3");
    EXPECT_EQ(
        client.Command({"XADD", "s", "MAXLEN", "0", "2053-0", "f", "gone"})
            .text_,
        "2053-0");
    EXPECT_EQ(client.Command({"XLEN", "s"}).text_, "0");
    EXPECT_EQ(client.Command({"DEL", "s"}).text_, "1");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server server(disk);
  Client client(server.port());
  EXPECT_EQ(client.Command({"EXISTS", "s"}).text_, "0");
}

TEST(GroupedStreamE2e, LargeRepliesKeepSnapshotsAndDeletedHistory) {
  PrivateDisk disk(2ULL * 1024 * 1024 * 1024);
  Server server(disk, 2, {}, {}, false, 2, "256M", {}, "128M");
  server.PreserveOnFailure();
  Client client(server.port());
  PopulateStream(client, "s", 256, 256 * 1024);
  {
    auto range = client.Command({"XRANGE", "s", "-", "+"});
    ASSERT_EQ(range.kind_, '*') << range.text_;
    ASSERT_EQ(range.items_.size(), 256);
    EXPECT_EQ(range.items_.back().items_[0].text_, "256-0");
    auto reverse =
        client.Command({"XREVRANGE", "s", "(200-0", "(197-0", "COUNT", "2"});
    ASSERT_EQ(reverse.items_.size(), 2);
    EXPECT_EQ(reverse.items_[0].items_[0].text_, "199-0");
    EXPECT_EQ(reverse.items_[1].items_[0].text_, "198-0");
  }
  ASSERT_EQ(client.Command({"XGROUP", "CREATE", "s", "g", "0"}).text_, "OK");
  {
    auto read =
        client.Command({"XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">"});
    ASSERT_EQ(read.kind_, '*') << read.text_;
    ASSERT_EQ(read.items_.size(), 1);
    EXPECT_EQ(read.items_[0].items_[1].items_.size(), 256);
  }
  ASSERT_EQ(client.Command({"XDEL", "s", "1-0", "128-0"}).text_, "2");
  {
    auto history =
        client.Command({"XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", "0"});
    ASSERT_EQ(history.kind_, '*') << history.text_;
    const auto& entries = history.items_[0].items_[1].items_;
    ASSERT_EQ(entries.size(), 256);
    EXPECT_EQ(entries[0].items_[1].text_, "-1");
    EXPECT_EQ(entries[127].items_[1].text_, "-1");
    EXPECT_EQ(entries.back().items_[1].items_[1].text_.size(), 256 * 1024);
  }
  {
    auto info = client.Command({"XINFO", "STREAM", "s", "FULL", "COUNT", "0"});
    ASSERT_EQ(info.kind_, '*') << info.text_;
    EXPECT_EQ(info.items_[15].items_.size(), 254);
  }
  {
    auto dump = client.Command({"DUMP", "s"});
    ASSERT_EQ(dump.kind_, '$') << dump.text_;
    EXPECT_GT(dump.text_.size(), 64 * 1000 * 1000);
    // The imported deleted-message PEL survives value-only framing too.
    ASSERT_EQ(client.Command({"RESTORE", "copy", "0", dump.text_}).text_, "OK");
    EXPECT_EQ(client.Command({"XLEN", "copy"}).text_, "254");
    EXPECT_EQ(client.Command({"XPENDING", "copy", "g"}).items_[0].text_, "256");
  }
  EXPECT_EQ(client.Command({"MULTI"}).text_, "OK");
  EXPECT_EQ(client.Command({"XREAD", "STREAMS", "s", "0"}).text_, "QUEUED");
  EXPECT_EQ(client.Command({"XRANGE", "s", "-", "+", "COUNT", "1"}).text_,
            "QUEUED");
  EXPECT_EQ(client.Command({"DEL", "s"}).text_, "QUEUED");
  auto transaction = client.Command({"EXEC"});
  ASSERT_EQ(transaction.kind_, '*') << transaction.text_;
  ASSERT_EQ(transaction.items_.size(), 3);
  ASSERT_EQ(transaction.items_[0].items_[0].items_[1].items_.size(), 254);
  EXPECT_EQ(transaction.items_[1].items_[0].items_[0].text_, "2-0");
  EXPECT_EQ(transaction.items_[2].text_, "1");
}

TEST(GroupedStreamE2e, LargeRdbRoundTripKeepsMessagesAndDeletedPendingBounded) {
  PrivateDisk source(4ULL * 1024 * 1024 * 1024);
  PrivateDisk target(4ULL * 1024 * 1024 * 1024);
  const auto input = target.path() + ".input.rdb";
  struct Cleanup {
    std::string path;
    ~Cleanup() { ::unlink(path.c_str()); }
  } cleanup{input};
  {
    Server server(source, 2, {}, {}, false, 2, "256M");
    Client client(server.port());
    PopulateStream(client, "s", 2050, 256 * 1024);
    ASSERT_EQ(client.Command({"XGROUP", "CREATE", "s", "g", "0"}).text_, "OK");
    ASSERT_EQ(client
                  .Command({"XREADGROUP", "GROUP", "g", "c", "COUNT", "10",
                            "STREAMS", "s", ">"})
                  .kind_,
              '*');
    ASSERT_EQ(client.Command({"XDEL", "s", "1-0"}).text_, "1");
    ASSERT_EQ(client.Command({"BGSAVE"}).kind_, '+');
    const auto deadline = std::chrono::steady_clock::now() + 180s;
    while (server.Log().find("RDB backup completed:") == std::string::npos &&
           server.Running() && std::chrono::steady_clock::now() < deadline)
      std::this_thread::sleep_for(100ms);
    ASSERT_NE(server.Log().find("RDB backup completed:"), std::string::npos)
        << server.Log();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
    ASSERT_EQ(::rename((source.path() + ".rdb").c_str(), input.c_str()), 0);
  }
  {
    Server server(target, 3, {}, {}, false, 2, "256M", input);
    const auto deadline = std::chrono::steady_clock::now() + 180s;
    while (server.Log().find("loaded RDB file") == std::string::npos &&
           server.Running() && std::chrono::steady_clock::now() < deadline)
      std::this_thread::sleep_for(100ms);
    ASSERT_NE(server.Log().find("loaded RDB file"), std::string::npos)
        << server.Log();
    Client client(server.port());
    EXPECT_EQ(client.Command({"XLEN", "s"}).text_, "2049");
    auto pending = client.Command({"XPENDING", "s", "g"});
    ASSERT_EQ(pending.kind_, '*') << pending.text_;
    ASSERT_EQ(pending.items_[0].text_, "10");
    auto history = client.Command(
        {"XREADGROUP", "GROUP", "g", "c", "COUNT", "1", "STREAMS", "s", "0"});
    ASSERT_EQ(history.kind_, '*') << history.text_;
    ASSERT_EQ(history.items_[0].items_[1].items_[0].items_[0].text_, "1-0");
    EXPECT_EQ(history.items_[0].items_[1].items_[0].items_[1].kind_, '*');
    EXPECT_EQ(history.items_[0].items_[1].items_[0].items_[1].text_, "-1");
    auto range = client.Command({"XRANGE", "s", "2050-0", "+", "COUNT", "1"});
    ASSERT_EQ(range.kind_, '*') << range.text_;
    ASSERT_EQ(range.items_.size(), 1);
    EXPECT_EQ(range.items_[0].items_[1].items_[1].text_,
              std::string(256 * 1024, 'v'));
    EXPECT_EQ(client.Command({"XACK", "s", "g", "1-0"}).text_, "1");
    EXPECT_EQ(client.Command({"XADD", "s", "2051-0", "f", "new"}).text_,
              "2051-0");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server server(target, 2, {}, {}, false, 2, "256M");
  Client client(server.port());
  EXPECT_EQ(client.Command({"XLEN", "s"}).text_, "2050");
}

class GroupedStreamCrashE2e : public testing::TestWithParam<const char*> {};
TEST_P(GroupedStreamCrashE2e, InterruptedAppendKeepsPreviousGraph) {
#if !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires grouped crash hooks";
#endif
  PrivateDisk disk;
  {
    Server server(disk);
    Client client(server.port());
    PopulateStream(client, "s", 150);
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  {
    Server server(disk, 2, GetParam());
    Client client(server.port());
    ASSERT_EQ(client.Command({"MULTI"}).text_, "OK");
    ASSERT_EQ(client.Command({"XADD", "s", "151-0", "f", "new"}).text_,
              "QUEUED");
    EXPECT_THROW(client.Command({"EXEC"}), std::runtime_error);
    EXPECT_EQ(server.Wait(), 86) << server.Log();
  }
  Server recovered(disk, 3);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"XLEN", "s"}).text_, "150");
  EXPECT_EQ(client.Command({"XADD", "s", "151-0", "f", "retry"}).text_,
            "151-0");
}
TEST_P(GroupedStreamCrashE2e, InterruptedTrimKeepsMessagesAndPending) {
#if !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires grouped crash hooks";
#endif
  PrivateDisk disk;
  {
    Server server(disk);
    Client client(server.port());
    PopulateStream(client, "s", 150);
    ASSERT_EQ(client.Command({"XGROUP", "CREATE", "s", "g", "0"}).text_, "OK");
    ASSERT_EQ(client
                  .Command({"XREADGROUP", "GROUP", "g", "c", "COUNT", "2",
                            "STREAMS", "s", ">"})
                  .kind_,
              '*');
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  {
    Server server(disk, 2, GetParam());
    Client client(server.port());
    ASSERT_EQ(client.Command({"MULTI"}).text_, "OK");
    ASSERT_EQ(client.Command({"XTRIM", "s", "MAXLEN", "1"}).text_, "QUEUED");
    EXPECT_THROW(client.Command({"EXEC"}), std::runtime_error);
    EXPECT_EQ(server.Wait(), 86) << server.Log();
  }
  Server recovered(disk, 3);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"XLEN", "s"}).text_, "150");
  EXPECT_EQ(client.Command({"XPENDING", "s", "g"}).items_[0].text_, "2");
  EXPECT_EQ(client.Command({"XRANGE", "s", "-", "+", "COUNT", "1"})
                .items_[0]
                .items_[0]
                .text_,
            "1-0");
  EXPECT_EQ(client.Command({"XTRIM", "s", "MAXLEN", "1"}).text_, "149");
}

INSTANTIATE_TEST_SUITE_P(
    BatchWindows, GroupedStreamCrashE2e,
    testing::Values("group-batch-before-root",
                    "group-root-staged-before-batch-decision",
                    "group-batch-durable-before-outer-decision"));

}  // namespace
