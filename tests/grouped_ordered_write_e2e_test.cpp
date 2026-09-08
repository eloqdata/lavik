#include <algorithm>
#include <cmath>
#include <tuple>

#include "grouped_write_e2e_support.h"
#include "keylane/rdb.h"
#include "keylane/storage/detail/hash_codec.h"
#include "keylane/storage/detail/ordered_compact_codec.h"

namespace {
using namespace grouped_e2e;

std::vector<std::string> Items(unsigned count = 256) {
  std::vector<std::string> items;
  for (unsigned i = 0; i < count; ++i) {
    auto item = "item" + std::to_string(i);
    item.resize(128, 'x');
    items.push_back(std::move(item));
  }
  return items;
}

std::vector<std::string> Push(std::string key,
                              const std::vector<std::string>& items) {
  std::vector<std::string> command{"RPUSH", std::move(key)};
  command.insert(command.end(), items.begin(), items.end());
  return command;
}

void ExpectList(Client& client, const std::string& key,
                const std::vector<std::string>& items) {
  auto result = client.Command({"LRANGE", key, "0", "-1"});
  ASSERT_EQ(result.kind_, '*') << result.text_;
  ASSERT_EQ(result.items_.size(), items.size());
  for (std::size_t i = 0; i < items.size(); ++i)
    EXPECT_EQ(result.items_[i].text_, items[i]) << "rank " << i;
  EXPECT_EQ(client.Command({"LLEN", key}).text_, std::to_string(items.size()));
}

TEST(GroupedOrderedWriteE2e, ListPointSetOnlyRewritesTargetAndNeighbours) {
  PrivateDisk disk;
  auto items = Items();
  {
    Server server(disk);
    Client client(server.port());
    ASSERT_EQ(client.Command(Push("list", items)).text_, "256");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  const auto before = disk.Auxiliaries("list");
  ASSERT_FALSE(before.empty());
  EXPECT_GT(before.rbegin()->second.size(), 3);
  {
    Server server(disk, 3);
    Client client(server.port());
    items[128].assign(128, 'n');
    ASSERT_EQ(client.Command({"LSET", "list", "128", items[128]}).text_, "OK");
    ExpectList(client, "list", items);
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  const auto after = disk.Auxiliaries("list");
  ASSERT_FALSE(after.empty());
  EXPECT_GT(after.rbegin()->first, before.rbegin()->first);
  EXPECT_LE(after.rbegin()->second.size(), 3);
  Server recovered(disk);
  Client client(recovered.port());
  ExpectList(client, "list", items);
}

TEST(GroupedOrderedWriteE2e, ListCommandSurfaceAndEmptyRecreation) {
  PrivateDisk disk;
  auto items = Items();
  {
    Server server(disk);
    Client client(server.port());
    ASSERT_EQ(client.Command(Push("list", items)).text_, "256");
    EXPECT_EQ(client.Command({"LPUSHX", "missing", "x"}).text_, "0");
    EXPECT_EQ(client.Command({"RPUSHX", "missing", "x"}).text_, "0");
    EXPECT_EQ(client.Command({"LPUSH", "list", "a", "b"}).text_, "258");
    items.insert(items.begin(), {"b", "a"});
    EXPECT_EQ(client.Command({"RPUSHX", "list", "tail"}).text_, "259");
    items.push_back("tail");
    EXPECT_EQ(client.Command({"LPOP", "list"}).text_, "b");
    items.erase(items.begin());
    EXPECT_EQ(client.Command({"RPOP", "list"}).text_, "tail");
    items.pop_back();
    const auto pivot = items[128];
    EXPECT_EQ(
        client.Command({"LINSERT", "list", "BEFORE", pivot, "inserted"}).text_,
        "258");
    items.insert(items.begin() + 128, "inserted");
    EXPECT_EQ(client.Command({"LPOS", "list", "inserted"}).text_, "128");
    EXPECT_EQ(client.Command({"LINDEX", "list", "128"}).text_, "inserted");
    EXPECT_EQ(client.Command({"LREM", "list", "0", "inserted"}).text_, "1");
    items.erase(items.begin() + 128);
    EXPECT_EQ(client.Command({"LSET", "list", "128", "changed"}).text_, "OK");
    items[128] = "changed";
    EXPECT_EQ(client.Command({"LTRIM", "list", "10", "-11"}).text_, "OK");
    items = std::vector<std::string>(items.begin() + 10, items.end() - 10);
    EXPECT_EQ(client.Command({"LMOVE", "list", "list", "RIGHT", "LEFT"}).text_,
              items.back());
    std::rotate(items.begin(), items.end() - 1, items.end());
    EXPECT_EQ(client.Command({"RPOPLPUSH", "list", "list"}).text_,
              items.back());
    std::rotate(items.begin(), items.end() - 1, items.end());
    EXPECT_EQ(
        client.Command({"BLMOVE", "list", "list", "LEFT", "RIGHT", "1"}).text_,
        items.front());
    std::rotate(items.begin(), items.begin() + 1, items.end());
    auto popped = client.Command({"LMPOP", "1", "list", "LEFT", "COUNT", "2"});
    ASSERT_EQ(popped.items_.size(), 2);
    ASSERT_EQ(popped.items_[1].items_.size(), 2);
    EXPECT_EQ(popped.items_[1].items_[0].text_, items[0]);
    EXPECT_EQ(popped.items_[1].items_[1].text_, items[1]);
    items.erase(items.begin(), items.begin() + 2);
    popped =
        client.Command({"BLMPOP", "0", "1", "list", "RIGHT", "COUNT", "2"});
    ASSERT_EQ(popped.items_.size(), 2);
    EXPECT_EQ(popped.items_[1].items_[0].text_, items.back());
    items.resize(items.size() - 2);
    popped = client.Command({"BLPOP", "list", "1"});
    ASSERT_EQ(popped.items_.size(), 2);
    EXPECT_EQ(popped.items_[1].text_, items.front());
    items.erase(items.begin());
    popped = client.Command({"BRPOP", "list", "1"});
    ASSERT_EQ(popped.items_.size(), 2);
    EXPECT_EQ(popped.items_[1].text_, items.back());
    items.pop_back();
    const auto moved = items.front();
    EXPECT_EQ(
        client.Command({"LMOVE", "list", "destination", "LEFT", "RIGHT"}).text_,
        moved);
    items.erase(items.begin());
    ExpectList(client, "list", items);
    EXPECT_EQ(client.Command({"LINDEX", "destination", "0"}).text_, moved);
    EXPECT_EQ(client.Command({"LTRIM", "list", "1", "0"}).text_, "OK");
    EXPECT_EQ(client.Command({"EXISTS", "list"}).text_, "0");
    items = Items();
    items[0] = "recreated";
    EXPECT_EQ(client.Command(Push("list", items)).text_, "256");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server recovered(disk, 3);
  Client client(recovered.port());
  ExpectList(client, "list", items);
}

TEST(GroupedOrderedWriteE2e, LargeListItemUsesExtentsWithoutRewritingAllPages) {
  PrivateDisk disk;
  auto items = Items();
  items[128].assign(9 * 1024 * 1024, 'L');
  {
    Server server(disk);
    Client client(server.port());
    ASSERT_EQ(client.Command(Push("list", Items())).text_, "256");
    ASSERT_EQ(client.Command({"LSET", "list", "128", items[128]}).text_, "OK");
    ASSERT_EQ(client.Command({"LSET", "list", "129", "neighbor"}).text_, "OK");
    items[129] = "neighbor";
    EXPECT_EQ(client.Command({"LINDEX", "list", "128"}).text_, items[128]);
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server recovered(disk, 3);
  Client client(recovered.port());
  ExpectList(client, "list", items);
}

TEST(GroupedOrderedWriteE2e, FailedListBatchCannotLeakIntoLaterExecCommand) {
#if !KEYLANE_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires Debug/fault command-local auxiliary hook";
#endif
  PrivateDisk disk;
  auto items = Items();
  {
    Server server(disk);
    Client client(server.port());
    ASSERT_EQ(client.Command(Push("list", items)).text_, "256");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  {
    Server server(disk, 2, {}, "list", false, 4);
    Client client(server.port());
    EXPECT_EQ(client.Command({"MULTI"}).text_, "OK");
    EXPECT_EQ(client.Command(Push("list", Items(512))).text_, "QUEUED");
    EXPECT_EQ(client.Command({"LSET", "list", "0", "survivor"}).text_,
              "QUEUED");
    auto result = client.Command({"EXEC"});
    ASSERT_EQ(result.items_.size(), 2);
    EXPECT_EQ(result.items_[0].kind_, '-');
    EXPECT_EQ(result.items_[1].text_, "OK");
    items[0] = "survivor";
    ExpectList(client, "list", items);
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server recovered(disk, 3);
  Client client(recovered.port());
  ExpectList(client, "list", items);
}

std::vector<std::string> Zadd(std::string key,
                              const std::vector<std::string>& members,
                              unsigned first_score = 0) {
  std::vector<std::string> command{"ZADD", std::move(key)};
  for (std::size_t i = 0; i < members.size(); ++i) {
    command.push_back(std::to_string(first_score + i));
    command.push_back(members[i]);
  }
  return command;
}

TEST(GroupedOrderedWriteE2e, SortedSetPointUpdateOnlyRewritesNearbyPages) {
  PrivateDisk disk;
  const auto members = Items();
  {
    Server server(disk);
    Client client(server.port());
    ASSERT_EQ(client.Command(Zadd("zset", members)).text_, "256");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  const auto before = disk.Auxiliaries("zset");
  ASSERT_FALSE(before.empty());
  EXPECT_GT(before.rbegin()->second.size(), 3);
  {
    Server server(disk, 3);
    Client client(server.port());
    EXPECT_EQ(client.Command({"ZINCRBY", "zset", "0.25", members[128]}).text_,
              "128.25");
    EXPECT_EQ(client.Command({"ZRANK", "zset", members[128]}).text_, "128");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  const auto after = disk.Auxiliaries("zset");
  ASSERT_FALSE(after.empty());
  EXPECT_GT(after.rbegin()->first, before.rbegin()->first);
  EXPECT_LE(after.rbegin()->second.size(), 3);
  Server recovered(disk);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"ZSCORE", "zset", members[128]}).text_, "128.25");
  EXPECT_EQ(client.Command({"ZCARD", "zset"}).text_, "256");
  EXPECT_EQ(client.Command({"ZRANGE", "zset", "0", "-1"}).items_.size(), 256);
}

TEST(GroupedOrderedWriteE2e, SortedSetCrossEndMovesDoNotRewriteMiddlePages) {
  PrivateDisk disk;
  const auto members = Items(2048);
  {
    Server server(disk);
    Client client(server.port());
    ASSERT_EQ(client.Command(Zadd("zset", members)).text_, "2048");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  const auto before = disk.Auxiliaries("zset");
  ASSERT_FALSE(before.empty());
  ASSERT_GT(before.rbegin()->second.size(), 6);
  {
    Server server(disk, 3);
    Client client(server.port());
    EXPECT_EQ(client.Command({"ZINCRBY", "zset", "10000", members[0]}).text_,
              "10000");
    EXPECT_EQ(client.Command({"ZRANK", "zset", members[0]}).text_, "2047");
    auto first = client.Command({"ZRANGE", "zset", "0", "0"});
    ASSERT_EQ(first.items_.size(), 1);
    EXPECT_EQ(first.items_[0].text_, members[1]);
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  const auto at_tail = disk.Auxiliaries("zset");
  ASSERT_FALSE(at_tail.empty());
  EXPECT_GT(at_tail.rbegin()->first, before.rbegin()->first);
  EXPECT_LE(at_tail.rbegin()->second.size(), 6);
  {
    Server server(disk);
    Client client(server.port());
    EXPECT_EQ(client.Command({"ZRANK", "zset", members[0]}).text_, "2047");
    EXPECT_EQ(client.Command({"ZINCRBY", "zset", "-20000", members[0]}).text_,
              "-10000");
    EXPECT_EQ(client.Command({"ZRANK", "zset", members[0]}).text_, "0");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  const auto at_head = disk.Auxiliaries("zset");
  ASSERT_FALSE(at_head.empty());
  EXPECT_GT(at_head.rbegin()->first, at_tail.rbegin()->first);
  EXPECT_LE(at_head.rbegin()->second.size(), 6);
  Server recovered(disk, 3);
  Client client(recovered.port());
  auto range = client.Command({"ZRANGE", "zset", "0", "-1"});
  ASSERT_EQ(range.items_.size(), members.size());
  for (std::size_t i = 0; i < members.size(); ++i)
    EXPECT_EQ(range.items_[i].text_, members[i]);
  EXPECT_EQ(client.Command({"ZSCORE", "zset", members[0]}).text_, "-10000");
}

TEST(GroupedOrderedWriteE2e, ExpirationOnlyChangesRootsForAllCollectionTypes) {
  PrivateDisk disk;
  const auto members = Items();
  const std::vector<std::string> keys{"hash", "set", "list", "zset"};
  {
    Server server(disk);
    Client client(server.port());
    std::vector<std::string> hash{"HSET", "hash"};
    std::vector<std::string> set{"SADD", "set"};
    for (const auto& member : members) {
      hash.insert(hash.end(), {member, "value"});
      set.push_back(member);
    }
    ASSERT_EQ(client.Command(hash).text_, "256");
    ASSERT_EQ(client.Command(set).text_, "256");
    ASSERT_EQ(client.Command(Push("list", members)).text_, "256");
    ASSERT_EQ(client.Command(Zadd("zset", members)).text_, "256");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  using Records = decltype(disk.Auxiliaries("hash"));
  std::map<std::string, Records> before;
  for (const auto& key : keys) {
    before[key] = disk.Auxiliaries(key);
    ASSERT_FALSE(before[key].empty()) << key;
  }
  const auto deadline =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          (std::chrono::system_clock::now() + 1h).time_since_epoch())
          .count();
  {
    Server server(disk, 3);
    Client client(server.port());
    for (const auto& key : keys) {
      EXPECT_EQ(client.Command({"EXPIRE", key, "3600"}).text_, "1") << key;
      EXPECT_GT(std::stoll(client.Command({"PTTL", key}).text_), 0) << key;
      EXPECT_EQ(client.Command({"PERSIST", key}).text_, "1") << key;
      EXPECT_EQ(client.Command({"PTTL", key}).text_, "-1") << key;
      EXPECT_EQ(
          client.Command({"PEXPIREAT", key, std::to_string(deadline)}).text_,
          "1")
          << key;
    }
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  for (const auto& key : keys)
    EXPECT_EQ(disk.Auxiliaries(key), before.at(key)) << key;
  Server recovered(disk);
  Client client(recovered.port());
  for (const auto& key : keys) {
    EXPECT_EQ(client.Command({"PEXPIRETIME", key}).text_,
              std::to_string(deadline))
        << key;
    EXPECT_GT(std::stoll(client.Command({"PTTL", key}).text_), 0) << key;
  }
  EXPECT_EQ(client.Command({"HGET", "hash", members[0]}).text_, "value");
  EXPECT_EQ(client.Command({"SISMEMBER", "set", members[0]}).text_, "1");
  EXPECT_EQ(client.Command({"LINDEX", "list", "0"}).text_, members[0]);
  EXPECT_EQ(client.Command({"ZSCORE", "zset", members[0]}).text_, "0");
}

class GroupedFullDiskExpirationE2e
    : public ::testing::TestWithParam<std::tuple<ValueType, bool>> {};

TEST_P(GroupedFullDiskExpirationE2e, ReclaimsGraphAndRecovers) {
  const auto [type, external_key] = GetParam();
  // Inline groups occupy the only foreground block on the minimum device.
  // Oversized groups instead consume two extents each; external parent keys
  // make those extents depend on retirement of their transaction record block.
  // Indexed Sorted Sets persist the member in two graphs. Keep inline bytes
  // per key unchanged, and give the external case its six extra extent blocks;
  // the final 1 MiB/9 MiB SET below must still prove the device is actually
  // full.
  const bool indexed = type == ValueType::kSortedSet;
  PrivateDisk disk((external_key ? (indexed ? 176ULL : 128ULL) : 80ULL) * 1024 *
                   1024);
  const std::string member(
      external_key ? 9 * 1024 * 1024 : (indexed ? 512 : 1024) * 1024, 'v');
  absl::StatusOr<std::string> compact;
  if (type == ValueType::kHash || type == ValueType::kSet) {
    HashValue value;
    value.entries_.push_back(
        {.field_ = type == ValueType::kHash ? "f" : member,
         .value_ = type == ValueType::kHash ? member : ""});
    compact = EncodeHashValue(value);
  } else {
    const std::vector<OrderedCollectionEntry> entries{{.value_ = member}};
    compact = EncodeOrderedCompactValue(type == ValueType::kList
                                            ? OrderedCollectionKind::kList
                                            : OrderedCollectionKind::kSortedSet,
                                        entries);
  }
  ASSERT_TRUE(compact.ok()) << compact.status();
  auto dump = keylane::rdb::EncodeDump(RawValue{.encoded_ = std::move(*compact),
                                                .logical_size_ = 1,
                                                .value_type_ = type});
  ASSERT_TRUE(dump.ok()) << dump.status();
  std::vector<std::string> expired_keys;
  const auto key_count = external_key ? 3 : 7;
  for (int i = 0; i < key_count; ++i) {
    auto key = "expiring:" + std::to_string(i);
    if (external_key) key.resize(32 * 1024, 'k');
    expired_keys.push_back(std::move(key));
  }
  const std::string replacement = external_key ? member : "space reclaimed";
  {
    Server server(disk, 1);
    Client client(server.port());
    ASSERT_EQ(
        client.Command({"CONFIG", "SET", "tx-cleaner-cooldown-ms", "0"}).text_,
        "OK");
    const auto deadline =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            (std::chrono::system_clock::now() + 10s).time_since_epoch())
            .count();
    for (const auto& key : expired_keys) {
      // RESTORE creates a grouped root with its final TTL in one publication.
      // Adding TTL afterward would create a shielding successor, which must
      // never take the full-disk, non-durable expiration escape valve.
      ASSERT_EQ(client
                    .Command({"RESTORE", key, std::to_string(deadline), *dump,
                              "ABSTTL"})
                    .text_,
                "OK");
    }
    ASSERT_EQ(client.Command({"MULTI"}).text_, "OK");
    ASSERT_EQ(
        client
            .Command({"SET", "cannot-fit",
                      external_key ? member : std::string(1024 * 1024, 'v')})
            .text_,
        "QUEUED");
    const auto exhausted = client.Command({"EXEC"});
    ASSERT_EQ(exhausted.items_.size(), 1);
    ASSERT_EQ(exhausted.items_[0].kind_, '-');
    ASSERT_NE(exhausted.items_[0].text_.find("out of disk space"),
              std::string::npos);
    std::vector<std::string> exists{"EXISTS"};
    exists.insert(exists.end(), expired_keys.begin(), expired_keys.end());
    ASSERT_EQ(client.Command(exists).text_, std::to_string(key_count));

    const auto expiry_timeout = std::chrono::steady_clock::now() + 20s;
    while (client.Command({"DBSIZE"}).text_ != "0" &&
           std::chrono::steady_clock::now() < expiry_timeout) {
      (void)client.Command(exists);  // Also exercise lazy candidate enqueueing.
      std::this_thread::sleep_for(20ms);
    }
    ASSERT_EQ(client.Command({"DBSIZE"}).text_, "0") << server.Log();
    ASSERT_EQ(client.Command(exists).text_, "0");

    ASSERT_EQ(
        client.Command({"CONFIG", "SET", "tx-cleaner-cooldown-ms", "1"}).text_,
        "OK");
    const auto stat = [](const std::string& info, std::string_view name) {
      const auto offset = info.find(std::string(name) + ':');
      Check(offset != std::string::npos, "missing cleaner statistic");
      return std::stoull(info.substr(offset + name.size() + 1));
    };
    std::string stats;
    const auto reclaim_timeout = std::chrono::steady_clock::now() + 10s;
    do {
      stats = client.Command({"INFO", "STATS"}).text_;
      if (stat(stats, "tx_cleaner_retired_blocks") != 0) break;
      std::this_thread::sleep_for(20ms);
    } while (std::chrono::steady_clock::now() < reclaim_timeout);
    ASSERT_GT(stat(stats, "tx_cleaner_retired_blocks"), 0)
        << stats << server.Log();
    ASSERT_EQ(stat(stats, "tx_cleaner_failures"), 0) << stats << server.Log();

    // Extent debt settles asynchronously after its source block retires.
    Reply written;
    const auto write_timeout = std::chrono::steady_clock::now() + 10s;
    do {
      written = client.Command({"SET", "after-expiry", replacement});
      if (written.text_ == "OK") break;
      ASSERT_NE(written.text_.find("out of disk space"), std::string::npos)
          << written.text_ << server.Log();
      std::this_thread::sleep_for(20ms);
    } while (std::chrono::steady_clock::now() < write_timeout);
    ASSERT_EQ(written.text_, "OK") << server.Log();
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server recovered(disk, 2);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"DBSIZE"}).text_, "1");
  for (const auto& key : expired_keys)
    EXPECT_EQ(client.Command({"EXISTS", key}).text_, "0");
  EXPECT_EQ(client.Command({"GET", "after-expiry"}).text_, replacement);
  ASSERT_EQ(recovered.Wait(true), 0) << recovered.Log();
}

INSTANTIATE_TEST_SUITE_P(
    AllTypes, GroupedFullDiskExpirationE2e,
    ::testing::Combine(::testing::Values(ValueType::kHash, ValueType::kSet,
                                         ValueType::kList,
                                         ValueType::kSortedSet),
                       ::testing::Bool()),
    [](const ::testing::TestParamInfo<GroupedFullDiskExpirationE2e::ParamType>&
           info) {
      const auto type = std::get<0>(info.param);
      const auto external = std::get<1>(info.param);
      std::string name = type == ValueType::kHash   ? "Hash"
                         : type == ValueType::kSet  ? "Set"
                         : type == ValueType::kList ? "List"
                                                    : "SortedSet";
      return name + (external ? "External" : "Inline");
    });

TEST(GroupedOrderedWriteE2e, SortedSetRemovalPopAndStoreCommandSurface) {
  PrivateDisk disk;
  const auto members = Items();
  {
    Server server(disk);
    Client client(server.port());
    ASSERT_EQ(client.Command(Zadd("zset", members)).text_, "256");
    EXPECT_EQ(client.Command({"ZADD", "zset", "NX", "99", members[0]}).text_,
              "0");
    EXPECT_EQ(
        client.Command({"ZADD", "zset", "XX", "CH", "0.5", members[0]}).text_,
        "1");
    EXPECT_EQ(client.Command({"ZREM", "zset", members[0]}).text_, "1");
    EXPECT_EQ(client.Command({"ZREMRANGEBYSCORE", "zset", "1", "3"}).text_,
              "3");
    EXPECT_EQ(client.Command({"ZREMRANGEBYRANK", "zset", "0", "1"}).text_, "2");
    auto popped = client.Command({"ZPOPMIN", "zset", "2"});
    ASSERT_EQ(popped.items_.size(), 4);
    EXPECT_EQ(popped.items_[0].text_, members[6]);
    EXPECT_EQ(popped.items_[2].text_, members[7]);
    popped = client.Command({"ZPOPMAX", "zset", "2"});
    ASSERT_EQ(popped.items_.size(), 4);
    EXPECT_EQ(popped.items_[0].text_, members[255]);
    EXPECT_EQ(popped.items_[2].text_, members[254]);
    EXPECT_EQ(client.Command({"ZCARD", "zset"}).text_, "246");
    EXPECT_EQ(client.Command({"ZCOUNT", "zset", "8", "10"}).text_, "3");
    auto range =
        client.Command({"ZRANGE", "zset", "8", "10", "BYSCORE", "WITHSCORES"});
    ASSERT_EQ(range.items_.size(), 6);
    EXPECT_EQ(range.items_[0].text_, members[8]);
    EXPECT_EQ(client.Command({"ZUNIONSTORE", "union", "1", "zset"}).text_,
              "246");
    EXPECT_EQ(
        client.Command({"ZINTERSTORE", "intersection", "2", "zset", "union"})
            .text_,
        "246");
    EXPECT_EQ(client.Command({"ZSCORE", "intersection", members[8]}).text_,
              "16");
    EXPECT_EQ(client.Command({"ZDIFFSTORE", "difference", "2", "zset", "union"})
                  .text_,
              "0");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server recovered(disk, 3);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"ZCARD", "zset"}).text_, "246");
  EXPECT_EQ(client.Command({"ZCARD", "union"}).text_, "246");
  EXPECT_EQ(client.Command({"ZSCORE", "intersection", members[8]}).text_, "16");
  EXPECT_EQ(client.Command({"EXISTS", "difference"}).text_, "0");
}

TEST(GroupedOrderedWriteE2e, LargeSortedSetMemberUsesExtents) {
  PrivateDisk disk;
  const auto members = Items();
  const std::string huge(9 * 1024 * 1024, 'M');
  {
    Server server(disk);
    Client client(server.port());
    ASSERT_EQ(client.Command(Zadd("zset", members)).text_, "256");
    EXPECT_EQ(client.Command({"ZADD", "zset", "128.5", huge}).text_, "1");
    const auto score = client.Command({"ZINCRBY", "zset", "0.1", members[128]});
    ASSERT_EQ(score.kind_, '$') << score.text_;
    EXPECT_NEAR(std::stod(score.text_), 128.1, 0.0000001);
    EXPECT_EQ(client.Command({"ZRANK", "zset", huge}).text_, "129");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server recovered(disk, 3);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"ZSCORE", "zset", huge}).text_, "128.5");
  auto range = client.Command({"ZRANGEBYSCORE", "zset", "128.5", "128.5"});
  ASSERT_EQ(range.items_.size(), 1);
  EXPECT_EQ(range.items_[0].text_, huge);
  EXPECT_EQ(client.Command({"ZCARD", "zset"}).text_, "257");
}

TEST(GroupedOrderedWriteE2e, GeoUsesGroupedSortedSetAndStoreSurvivesRecovery) {
  PrivateDisk disk;
  const auto members = Items();
  {
    Server server(disk);
    Client client(server.port());
    std::vector<std::string> insert{"GEOADD", "geo"};
    for (std::size_t i = 0; i < members.size(); ++i) {
      insert.push_back(std::to_string(10.0 + (i % 10) * 0.01));
      insert.push_back(std::to_string(20.0 + (i / 10) * 0.01));
      insert.push_back(members[i]);
    }
    ASSERT_EQ(client.Command(insert).text_, "256");
    auto positions = client.Command({"GEOPOS", "geo", members[0]});
    ASSERT_EQ(positions.items_.size(), 1);
    ASSERT_EQ(positions.items_[0].items_.size(), 2);
    EXPECT_NEAR(std::stod(positions.items_[0].items_[0].text_), 10, 0.00001);
    EXPECT_NEAR(std::stod(positions.items_[0].items_[1].text_), 20, 0.00001);
    auto search = client.Command({"GEOSEARCH", "geo", "FROMMEMBER", members[0],
                                  "BYRADIUS", "100", "km", "COUNT", "5"});
    EXPECT_EQ(search.items_.size(), 5);
    EXPECT_EQ(client
                  .Command({"GEOSEARCHSTORE", "nearby", "geo", "FROMMEMBER",
                            members[0], "BYRADIUS", "100", "km"})
                  .text_,
              "256");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  ASSERT_FALSE(disk.Auxiliaries("geo").empty());
  Server recovered(disk, 3);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"ZCARD", "geo"}).text_, "256");
  EXPECT_EQ(client.Command({"ZCARD", "nearby"}).text_, "256");
  EXPECT_EQ(client.Command({"GEOPOS", "nearby", members[0]}).items_.size(), 1);
}

TEST(GroupedOrderedWriteE2e,
     FailedSortedSetBatchCannotLeakIntoLaterExecCommand) {
#if !KEYLANE_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires Debug/fault command-local auxiliary hook";
#endif
  PrivateDisk disk;
  const auto members = Items();
  {
    Server server(disk);
    Client client(server.port());
    ASSERT_EQ(client.Command(Zadd("zset", members)).text_, "256");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  {
    Server server(disk, 2, {}, "zset", false, 4);
    Client client(server.port());
    auto extra = Items(512);
    for (auto& member : extra) member.append("-new");
    EXPECT_EQ(client.Command({"MULTI"}).text_, "OK");
    EXPECT_EQ(client.Command(Zadd("zset", extra, 1000)).text_, "QUEUED");
    EXPECT_EQ(client.Command({"ZINCRBY", "zset", "0.25", members[0]}).text_,
              "QUEUED");
    auto result = client.Command({"EXEC"});
    ASSERT_EQ(result.items_.size(), 2);
    EXPECT_EQ(result.items_[0].kind_, '-');
    EXPECT_EQ(result.items_[1].text_, "0.25");
    EXPECT_EQ(client.Command({"ZCARD", "zset"}).text_, "256");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server recovered(disk, 3);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"ZCARD", "zset"}).text_, "256");
  EXPECT_EQ(client.Command({"ZSCORE", "zset", members[0]}).text_, "0.25");
}

class GroupedSortedSetCrashE2e : public testing::TestWithParam<const char*> {};

TEST_P(GroupedSortedSetCrashE2e, InterruptedSortedSetBatchKeepsPreviousValue) {
#if !KEYLANE_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires Debug/fault crash hooks";
#endif
  PrivateDisk disk;
  const auto members = Items();
  {
    Server server(disk);
    Client client(server.port());
    ASSERT_EQ(client.Command(Zadd("zset", members)).text_, "256");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  {
    Server server(disk, 2, GetParam());
    Client client(server.port());
    EXPECT_EQ(client.Command({"MULTI"}).text_, "OK");
    if (std::string_view(GetParam()) == "group-extents-durable-before-record") {
      EXPECT_EQ(client
                    .Command({"ZADD", "zset", "128.5",
                              std::string(9 * 1024 * 1024, 'X')})
                    .text_,
                "QUEUED");
    } else {
      EXPECT_EQ(client.Command({"ZINCRBY", "zset", "0.25", members[128]}).text_,
                "QUEUED");
    }
    EXPECT_THROW(client.Command({"EXEC"}), std::runtime_error);
    EXPECT_EQ(server.Wait(), 86) << server.Log();
  }
  Server recovered(disk, 3);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"ZCARD", "zset"}).text_, "256");
  EXPECT_EQ(client.Command({"ZSCORE", "zset", members[128]}).text_, "128");
}

INSTANTIATE_TEST_SUITE_P(
    BatchWindows, GroupedSortedSetCrashE2e,
    testing::Values("group-batch-before-root",
                    "group-root-staged-before-batch-decision",
                    "group-batch-durable-before-outer-decision",
                    "group-extents-durable-before-record"));

class GroupedListCrashE2e : public testing::TestWithParam<const char*> {};

TEST_P(GroupedListCrashE2e, InterruptedListBatchRestoresCompletePreviousValue) {
#if !KEYLANE_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires Debug/fault crash hooks";
#endif
  PrivateDisk disk;
  const auto items = Items();
  {
    Server server(disk);
    Client client(server.port());
    ASSERT_EQ(client.Command(Push("list", items)).text_, "256");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  {
    Server server(disk, 2, GetParam());
    Client client(server.port());
    EXPECT_EQ(client.Command({"MULTI"}).text_, "OK");
    const std::string replacement =
        std::string_view(GetParam()) == "group-extents-durable-before-record"
            ? std::string(9 * 1024 * 1024, 'X')
            : "uncommitted";
    EXPECT_EQ(client.Command({"LSET", "list", "128", replacement}).text_,
              "QUEUED");
    EXPECT_THROW(client.Command({"EXEC"}), std::runtime_error);
    EXPECT_EQ(server.Wait(), 86) << server.Log();
  }
  Server recovered(disk, 3);
  Client client(recovered.port());
  ExpectList(client, "list", items);
}

INSTANTIATE_TEST_SUITE_P(
    BatchWindows, GroupedListCrashE2e,
    testing::Values("group-batch-before-root",
                    "group-root-staged-before-batch-decision",
                    "group-batch-durable-before-outer-decision",
                    "group-extents-durable-before-record"));

}  // namespace

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  if (argc != 2) return 2;
  grouped_e2e::server_binary = argv[1];
  return RUN_ALL_TESTS();
}
