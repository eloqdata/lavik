#include <cerrno>
#include <memory>
#include <optional>

#include "grouped_write_e2e_support.h"
#include "keylane/storage/detail/grouped_hash.h"

namespace {
using namespace grouped_e2e;

std::vector<std::string> HashCommand(std::string key, char value = 'v') {
  std::vector<std::string> args{"HSET", std::move(key)};
  for (unsigned i = 0; i < 256; ++i) {
    args.push_back("field" + std::to_string(i));
    args.emplace_back(128, value);
  }
  return args;
}

struct HashDiskLayout {
  GroupedHashRoot root_;
  bool empty_tail_ = false;
};

HashDiskLayout InspectHashLayout(const PrivateDisk& disk,
                                 std::string_view key) {
  std::ifstream input(disk.path(), std::ios::binary);
  std::vector<std::byte> bytes(kStorageBlockBytes);
  HashDiskLayout result;
  std::uint64_t root_sequence = 0;
  std::vector<RecordHeader> auxiliary;
  while (input.read(reinterpret_cast<char*>(bytes.data()), bytes.size())) {
    BlockHeader block;
    if (!DecodeBlockHeaderPages(std::span<const std::byte, kBlockHeaderBytes>(
                                    bytes.data(), kBlockHeaderBytes),
                                &block) ||
        (block.kind_ != BlockKind::kRecords &&
         block.kind_ != BlockKind::kTransaction))
      continue;
    for (std::size_t offset = kBlockHeaderBytes;
         offset < block.committed_bytes_;) {
      RecordHeader record;
      std::string_view stored_key;
      if (!DecodeRecordHeader(
              std::span(bytes).subspan(offset, block.committed_bytes_ - offset),
              &record, &stored_key)) {
        offset = (offset / kDirectIoAlignment + 1) * kDirectIoAlignment;
        continue;
      }
      if (stored_key == key && record.grouped_ && !record.hash_group_ &&
          record.mutation_sequence_ >= root_sequence) {
        Check(!record.external_ && !record.key_external_,
              "fixture root must be inline");
        auto root = DecodeGroupedHashRoot(std::string_view(
            reinterpret_cast<const char*>(bytes.data() + offset +
                                          record.header_bytes_),
            record.payload_bytes_));
        Check(root.ok(), "fixture grouped root is invalid");
        result.root_ = *root;
        root_sequence = record.mutation_sequence_;
      }
      if (stored_key == key && record.hash_group_) auxiliary.push_back(record);
      Check(record.total_disk_bytes_ != 0, "fixture record has zero size");
      offset += record.total_disk_bytes_;
    }
  }
  Check(root_sequence != 0, "fixture has no grouped Hash root");
  std::map<std::pair<std::uint64_t, unsigned>, RecordHeader> newest;
  for (const auto& record : auxiliary) {
    if (record.group_incarnation_ != result.root_.incarnation_ ||
        record.mutation_sequence_ > result.root_.revision_)
      continue;
    auto& winner = newest[{record.group_prefix_, record.group_prefix_bits_}];
    if (winner.mutation_sequence_ <= record.mutation_sequence_) winner = record;
  }
  for (auto it = newest.rbegin(); it != newest.rend(); ++it) {
    if (!it->second.group_retired_) {
      result.empty_tail_ = it->second.logical_size_ == 0;
      break;
    }
  }
  return result;
}

TEST(GroupedHashWriteE2e, PromotionAndPointUpdateOnlyRewriteOneGroup) {
  PrivateDisk disk;
  {
    Server server(disk);
    Client client(server.port());
    ASSERT_EQ(client.Command(HashCommand("hash")).text_, "256");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  const auto before = disk.Auxiliaries("hash");
  ASSERT_FALSE(before.empty());
  EXPECT_GT(before.rbegin()->second.size(), 1);
  {
    Server server(disk, 3);
    Client client(server.port());
    EXPECT_EQ(
        client.Command({"HSET", "hash", "field0", std::string(128, 'u')}).text_,
        "0");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  const auto after = disk.Auxiliaries("hash");
  ASSERT_FALSE(after.empty());
  EXPECT_GT(after.rbegin()->first, before.rbegin()->first);
  EXPECT_EQ(after.rbegin()->second.size(), 1);
  Server recovered(disk);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"HGET", "hash", "field0"}).text_,
            std::string(128, 'u'));
  EXPECT_EQ(client.Command({"HGET", "hash", "field1"}).text_,
            std::string(128, 'v'));
  EXPECT_EQ(client.Command({"HLEN", "hash"}).text_, "256");
}

TEST(GroupedHashWriteE2e, ConditionalIncrementDeleteAndNewIncarnation) {
  PrivateDisk disk;
  {
    Server server(disk);
    Client client(server.port());
    ASSERT_EQ(client.Command(HashCommand("hash")).text_, "256");
    EXPECT_EQ(client.Command({"HSETNX", "hash", "field0", "wrong"}).text_, "0");
    EXPECT_EQ(client.Command({"HINCRBY", "hash", "counter", "2"}).text_, "2");
    EXPECT_EQ(client.Command({"HINCRBY", "hash", "counter", "3"}).text_, "5");
    std::vector<std::string> erase{"HDEL", "hash", "counter"};
    for (unsigned i = 0; i < 256; ++i)
      erase.push_back("field" + std::to_string(i));
    EXPECT_EQ(client.Command(erase).text_, "257");
    EXPECT_EQ(client.Command({"EXISTS", "hash"}).text_, "0");
    EXPECT_EQ(client.Command(HashCommand("hash", 'n')).text_, "256");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server recovered(disk, 3);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"HGET", "hash", "field0"}).text_,
            std::string(128, 'n'));
  EXPECT_EQ(client.Command({"HEXISTS", "hash", "counter"}).text_, "0");
  EXPECT_EQ(client.Command({"HLEN", "hash"}).text_, "256");
}

TEST(GroupedHashWriteE2e,
     FailedAuxiliaryBatchCannotReappearAfterLaterExecWrite) {
#if !KEYLANE_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires Debug/fault auxiliary hook";
#endif
  PrivateDisk disk;
  {
    Server server(disk);
    Client client(server.port());
    ASSERT_EQ(client.Command(HashCommand("hash")).text_, "256");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  {
    Server server(disk, 2, {}, "hash");
    Client client(server.port());
    EXPECT_EQ(client.Command({"MULTI"}).text_, "OK");
    EXPECT_EQ(client.Command(HashCommand("hash", 'x')).text_, "QUEUED");
    EXPECT_EQ(
        client.Command({"HSET", "hash", "field255", std::string(128, 's')})
            .text_,
        "QUEUED");
    auto reply = client.Command({"EXEC"});
    ASSERT_EQ(reply.kind_, '*');
    ASSERT_EQ(reply.items_.size(), 2);
    EXPECT_EQ(reply.items_[0].kind_, '-');
    EXPECT_EQ(reply.items_[1].text_, "0");
    EXPECT_EQ(client.Command({"HGET", "hash", "field0"}).text_,
              std::string(128, 'v'));
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server recovered(disk, 3);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"HGET", "hash", "field0"}).text_,
            std::string(128, 'v'));
  EXPECT_EQ(client.Command({"HGET", "hash", "field255"}).text_,
            std::string(128, 's'));
}

TEST(GroupedHashWriteE2e, OversizedFieldSplitsIntoExtentBackedLeaf) {
  PrivateDisk disk;
  const std::string large(9 * 1024 * 1024, 'L');
  {
    Server server(disk);
    Client client(server.port());
    ASSERT_EQ(client.Command(HashCommand("hash")).text_, "256");
    ASSERT_EQ(client.Command({"HSET", "hash", "field0", large}).text_, "0");
    EXPECT_EQ(client.Command({"HSTRLEN", "hash", "field0"}).text_,
              std::to_string(large.size()));
    EXPECT_EQ(client.Command({"HSET", "hash", "field1", "neighbor"}).text_,
              "0");
    EXPECT_EQ(client.Command({"HGET", "hash", "field0"}).text_, large);
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server recovered(disk, 3);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"HGET", "hash", "field0"}).text_, large);
  EXPECT_EQ(client.Command({"HGET", "hash", "field1"}).text_, "neighbor");
  EXPECT_EQ(client.Command({"HLEN", "hash"}).text_, "256");
}

TEST(GroupedHashWriteE2e, SetUsesSameGroupedLifecycleWithSetType) {
  PrivateDisk disk;
  const std::string prefix(128, 'm');
  {
    Server server(disk);
    Client client(server.port());
    std::vector<std::string> insert{"SADD", "set"};
    for (unsigned i = 0; i < 256; ++i)
      insert.push_back(prefix + std::to_string(i));
    ASSERT_EQ(client.Command(insert).text_, "256");
    EXPECT_EQ(client.Command({"SREM", "set", prefix + "0"}).text_, "1");
    EXPECT_EQ(client.Command({"SADD", "set", "new"}).text_, "1");
    EXPECT_EQ(client.Command({"SMEMBERS", "set"}).items_.size(), 256);
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  ASSERT_FALSE(disk.Auxiliaries("set").empty());
  Server recovered(disk, 3);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"TYPE", "set"}).text_, "set");
  EXPECT_EQ(client.Command({"SCARD", "set"}).text_, "256");
  EXPECT_EQ(client.Command({"SISMEMBER", "set", prefix + "0"}).text_, "0");
  EXPECT_EQ(client.Command({"SISMEMBER", "set", "new"}).text_, "1");
}

TEST(GroupedHashWriteE2e, NativeFullSyncTailAllowsRepeatedHashAndMixedWrites) {
#if !KEYLANE_TEST_FAULTS_AVAILABLE
  GTEST_SKIP()
      << "requires a Debug/fault server for the acknowledged handoff pause";
#endif
  PrivateDisk source_disk;
  PrivateDisk replica_disk;
  std::string tag;
  for (unsigned i = 0;; ++i) {
    tag = "{grouped-replay-" + std::to_string(i) + "}";
    if (RedisSlot(tag) == 0) break;
  }
  const std::string hash = tag + "hash";
  const std::string mixed = tag + "mixed";
  const std::string compact = tag + "compact";
  {
    Server source(source_disk, 1, {}, {}, true);
    Server replica(replica_disk, 2);
    Client writer(source.port());
    Client follower(replica.port());
    ASSERT_EQ(writer.Command(HashCommand(hash)).text_, "256");
    ASSERT_EQ(writer.Command(HashCommand(mixed)).text_, "256");
    ASSERT_EQ(writer.Command(HashCommand(compact)).text_, "256");
    ASSERT_EQ(
        follower
            .Command({"REPLICAOF", "127.0.0.1", std::to_string(source.port())})
            .text_,
        "OK");
    const auto handoff_deadline = std::chrono::steady_clock::now() + 30s;
    while (source.Log().find("acknowledged handoff partition 0") ==
               std::string::npos &&
           std::chrono::steady_clock::now() < handoff_deadline) {
      std::this_thread::sleep_for(10ms);
    }
    ASSERT_NE(source.Log().find("acknowledged handoff partition 0"),
              std::string::npos)
        << source.Log() << replica.Log();
    // These effects share one native full-sync command sequence on the target.
    // The streamed baseline is grouped. Consecutive point writes must mutate
    // that view rather than disappear behind an equal-C guard.
    ASSERT_EQ(writer.Command({"MULTI"}).text_, "OK");
    ASSERT_EQ(writer.Command({"HSET", hash, "field0", "first"}).text_,
              "QUEUED");
    ASSERT_EQ(writer.Command({"HSET", hash, "field1", "second"}).text_,
              "QUEUED");
    ASSERT_EQ(writer.Command({"HINCRBY", hash, "counter", "3"}).text_,
              "QUEUED");
    ASSERT_EQ(writer.Command({"HINCRBY", hash, "counter", "4"}).text_,
              "QUEUED");
    ASSERT_EQ(writer.Command({"HSET", mixed, "field0", "discarded"}).text_,
              "QUEUED");
    ASSERT_EQ(writer.Command({"SET", mixed, "compact"}).text_, "QUEUED");
    ASSERT_EQ(writer.Command({"DEL", mixed}).text_, "QUEUED");
    ASSERT_EQ(writer.Command(HashCommand(mixed, 'n')).text_, "QUEUED");
    ASSERT_EQ(writer.Command({"HSET", mixed, "field1", "rebuilt"}).text_,
              "QUEUED");
    ASSERT_EQ(writer.Command({"HSET", compact, "field0", "discarded"}).text_,
              "QUEUED");
    ASSERT_EQ(writer.Command({"SET", compact, "final-string"}).text_, "QUEUED");
    auto result = writer.Command({"EXEC"});
    ASSERT_EQ(result.items_.size(), 11);
    for (const auto& reply : result.items_)
      EXPECT_NE(reply.kind_, '-') << reply.text_;
    const auto online_deadline = std::chrono::steady_clock::now() + 60s;
    std::string info;
    do {
      info = follower.Command({"INFO", "replication"}).text_;
      if (info.find("keylane_replication_state:online") != std::string::npos)
        break;
      std::this_thread::sleep_for(10ms);
    } while (std::chrono::steady_clock::now() < online_deadline);
    ASSERT_NE(info.find("keylane_replication_state:online"), std::string::npos)
        << source.Log() << replica.Log();
    ASSERT_EQ(follower.Command({"READONLY"}).text_, "OK");
    EXPECT_EQ(follower.Command({"HGET", hash, "field0"}).text_, "first");
    EXPECT_EQ(follower.Command({"HGET", hash, "field1"}).text_, "second");
    EXPECT_EQ(follower.Command({"HGET", hash, "counter"}).text_, "7");
    EXPECT_EQ(follower.Command({"HGET", mixed, "field0"}).text_,
              std::string(128, 'n'));
    EXPECT_EQ(follower.Command({"HGET", mixed, "field1"}).text_, "rebuilt");
    EXPECT_EQ(follower.Command({"GET", compact}).text_, "final-string");
    ASSERT_EQ(follower.Command({"REPLICAOF", "NO", "ONE"}).text_, "OK");
    follower.Durable();
    ASSERT_EQ(replica.Wait(true), 0) << replica.Log();
  }
  Server recovered(replica_disk, 3);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"HGET", hash, "field0"}).text_, "first");
  EXPECT_EQ(client.Command({"HGET", hash, "field1"}).text_, "second");
  EXPECT_EQ(client.Command({"HGET", hash, "counter"}).text_, "7");
  EXPECT_EQ(client.Command({"HGET", mixed, "field1"}).text_, "rebuilt");
  EXPECT_EQ(client.Command({"GET", compact}).text_, "final-string");
}

TEST(GroupedHashWriteE2e,
     NativeStreamsFourCollectionsAndEmptyHashTailAcrossWorkers) {
  PrivateDisk source_disk;
  PrivateDisk replica_disk;
  const std::string huge(9 * 1024 * 1024, 'X');
  std::vector<std::string> members;
  for (unsigned i = 0; i < 256; ++i)
    members.push_back("member" + std::to_string(i) + std::string(128, 'm'));
  {
    Server source(source_disk, 2);
    Client client(source.port());
    ASSERT_EQ(client.Command(HashCommand("hash")).text_, "256");
    std::vector<std::string> set{"SADD", "set"};
    std::vector<std::string> list{"RPUSH", "list"};
    std::vector<std::string> sorted{"ZADD", "zset"};
    for (unsigned i = 0; i < members.size(); ++i) {
      set.push_back(members[i]);
      list.push_back(members[i]);
      sorted.insert(sorted.end(), {std::to_string(i), members[i]});
    }
    ASSERT_EQ(client.Command(set).text_, "256");
    ASSERT_EQ(client.Command(list).text_, "256");
    ASSERT_EQ(client.Command(sorted).text_, "256");
    client.Durable();
    ASSERT_EQ(source.Wait(true), 0) << source.Log();
  }
  const auto layout = InspectHashLayout(source_disk, "hash");
  std::string survivor;
  std::uint64_t lowest = UINT64_MAX;
  for (unsigned i = 0; i < 256; ++i) {
    const auto field = "field" + std::to_string(i);
    const auto hash = ComputeDigest(field, layout.root_.seed_).value_;
    if (hash < lowest) {
      lowest = hash;
      survivor = field;
    }
  }
  {
    Server source(source_disk, 2);
    Client client(source.port());
    ASSERT_EQ(client.Command({"HSET", "hash", survivor, huge}).text_, "0");
    std::vector<std::string> erase{"HDEL", "hash"};
    for (unsigned i = 0; i < 256; ++i) {
      const auto field = "field" + std::to_string(i);
      if (field != survivor) erase.push_back(field);
    }
    ASSERT_EQ(client.Command(erase).text_, "255");
    ASSERT_EQ(client.Command({"SADD", "set", huge}).text_, "1");
    ASSERT_EQ(client.Command({"LSET", "list", "128", huge}).text_, "OK");
    ASSERT_EQ(client.Command({"ZADD", "zset", "128.5", huge}).text_, "1");
    // An ordinary extent source shares the same admitted pinned-source map.
    ASSERT_EQ(client.Command({"SET", "string", huge}).text_, "OK");
    for (const auto* key : {"hash", "set", "list", "zset"})
      ASSERT_EQ(client.Command({"EXPIRE", key, "3600"}).text_, "1");
    client.Durable();
    ASSERT_EQ(source.Wait(true), 0) << source.Log();
  }
  ASSERT_TRUE(InspectHashLayout(source_disk, "hash").empty_tail_);
  auto verify = [&](Client& client) {
    EXPECT_EQ(client.Command({"HLEN", "hash"}).text_, "1");
    EXPECT_EQ(client.Command({"HGET", "hash", survivor}).text_, huge);
    EXPECT_EQ(client.Command({"SCARD", "set"}).text_, "257");
    EXPECT_EQ(client.Command({"SISMEMBER", "set", huge}).text_, "1");
    EXPECT_EQ(client.Command({"LLEN", "list"}).text_, "256");
    EXPECT_EQ(client.Command({"LINDEX", "list", "128"}).text_, huge);
    EXPECT_EQ(client.Command({"LINDEX", "list", "127"}).text_, members[127]);
    EXPECT_EQ(client.Command({"ZCARD", "zset"}).text_, "257");
    EXPECT_EQ(client.Command({"ZSCORE", "zset", huge}).text_, "128.5");
    EXPECT_EQ(client.Command({"GET", "string"}).text_, huge);
    for (const auto* key : {"hash", "set", "list", "zset"})
      EXPECT_GT(std::stoll(client.Command({"PTTL", key}).text_), 0) << key;
  };
  {
    Server source(source_disk, 2);
    Server replica(replica_disk, 3);
    Client follower(replica.port());
    ASSERT_EQ(
        follower
            .Command({"REPLICAOF", "127.0.0.1", std::to_string(source.port())})
            .text_,
        "OK");
    const auto deadline = std::chrono::steady_clock::now() + 90s;
    std::string info;
    do {
      info = follower.Command({"INFO", "replication"}).text_;
      if (info.find("keylane_replication_state:online") != std::string::npos)
        break;
      std::this_thread::sleep_for(10ms);
    } while (std::chrono::steady_clock::now() < deadline);
    ASSERT_NE(info.find("keylane_replication_state:online"), std::string::npos)
        << source.Log() << replica.Log();
    ASSERT_EQ(follower.Command({"READONLY"}).text_, "OK");
    verify(follower);
    ASSERT_EQ(follower.Command({"REPLICAOF", "NO", "ONE"}).text_, "OK")
        << source.Log() << replica.Log();
    follower.Durable();
    ASSERT_EQ(replica.Wait(true), 0) << replica.Log();
  }
  for (const auto* key : {"hash", "set", "list", "zset"})
    ASSERT_FALSE(replica_disk.Auxiliaries(key).empty()) << key;
  Server recovered(replica_disk, 4);
  Client client(recovered.port());
  verify(client);
}

// Child servers inherit only this scoped fault setting; the target never sees
// it, and restoring the parent environment also covers fixture exceptions.
class ScopedSourceFault {
 public:
  ScopedSourceFault(const char* variable, const char* key)
      : variable_(variable) {
    if (const auto* old = std::getenv(variable)) old_ = old;
    ::setenv(variable, key, 1);
  }
  ~ScopedSourceFault() {
    if (old_)
      ::setenv(variable_, old_->c_str(), 1);
    else
      ::unsetenv(variable_);
  }

 private:
  const char* variable_;
  std::optional<std::string> old_;
};

std::uint64_t MemoryInfoField(Client& client, std::string_view field) {
  const auto info = client.Command({"INFO", "memory"}).text_;
  const auto prefix = std::string(field) + ":";
  const auto at = info.find(prefix);
  Check(at != std::string::npos, "missing memory metric");
  return std::stoull(info.substr(at + prefix.size()));
}

bool AwaitSourceLog(const Server& server, std::string_view marker) {
  const auto deadline = std::chrono::steady_clock::now() + 30s;
  do {
    if (server.Log().find(marker) != std::string::npos) return true;
    std::this_thread::sleep_for(10ms);
  } while (std::chrono::steady_clock::now() < deadline);
  return false;
}

// Split send/read so the fixture can inspect other keys while one command
// awaits disk I/O. No timing guess is needed to start the concurrent work:
// the production Debug hook announces that the state mutex is already free.
class PendingHashWrite {
 public:
  explicit PendingHashWrite(std::uint16_t port) {
    fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    Check(fd_ >= 0, "pending Hash socket failed");
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::connect(fd_, reinterpret_cast<sockaddr*>(&address),
                  sizeof(address)) != 0) {
      ::close(fd_);
      fd_ = -1;
      throw std::runtime_error("pending Hash connect failed");
    }
    const timeval timeout{.tv_sec = 15, .tv_usec = 0};
    ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
  }
  ~PendingHashWrite() {
    if (fd_ >= 0) ::close(fd_);
  }
  void Send(const std::vector<std::string>& args) {
    std::string wire = "*" + std::to_string(args.size()) + "\r\n";
    for (const auto& arg : args)
      wire += "$" + std::to_string(arg.size()) + "\r\n" + arg + "\r\n";
    std::string_view remaining(wire);
    while (!remaining.empty()) {
      const auto sent =
          ::send(fd_, remaining.data(), remaining.size(), MSG_NOSIGNAL);
      if (sent < 0 && errno == EINTR) continue;
      Check(sent > 0, "pending Hash send failed");
      remaining.remove_prefix(sent);
    }
  }
  bool HasReply() {
    char byte;
    ssize_t received;
    do {
      received = ::recv(fd_, &byte, 1, MSG_PEEK | MSG_DONTWAIT);
    } while (received < 0 && errno == EINTR);
    if (received < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return false;
    Check(received > 0, "pending Hash connection ended early");
    return true;
  }
  std::string ReadStatus() {
    std::string line;
    while (!line.ends_with("\r\n")) {
      char byte;
      const auto received = ::recv(fd_, &byte, 1, 0);
      if (received < 0 && errno == EINTR) continue;
      Check(received > 0, "pending Hash response ended early");
      line += byte;
      Check(line.size() < 4096, "unexpected pending Hash response");
    }
    return line;
  }

 private:
  int fd_ = -1;
};

std::vector<std::string> SmallHashCommand(const std::string& key) {
  std::vector<std::string> command{"HSET", key};
  for (unsigned i = 0; i < 10; ++i) {
    command.push_back("field" + std::to_string(i));
    command.emplace_back(128, 'v');
  }
  return command;
}

TEST(GroupedHashWriteE2e, ColdCompactWritesReleaseStateButKeepTheKeyLocked) {
#if !KEYLANE_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires Debug/fault compact Hash write pause hook";
#endif
  for (const auto* verb : {"HSET", "HMSET"}) {
    SCOPED_TRACE(verb);
    PrivateDisk disk;
    const std::string key = "compact-cold";
    {
      Server server(disk, 1);
      Client client(server.port());
      ASSERT_EQ(client.Command(SmallHashCommand(key)).text_, "10");
      ASSERT_EQ(client.Command({"HSET", "other-hash", "seed", "keep"}).text_,
                "1");
      ASSERT_EQ(client.Command({"SET", "other-string", "before"}).text_, "OK");
      client.Durable();
      ASSERT_EQ(server.Wait(true), 0) << server.Log();
    }
    ASSERT_TRUE(disk.Auxiliaries(key).empty());
    {
      ScopedSourceFault paused_key("KEYLANE_COMPACT_HASH_WRITE_PAUSE_KEY",
                                   key.c_str());
      ScopedSourceFault pause_ms("KEYLANE_COMPACT_HASH_WRITE_PAUSE_MS", "3000");
      // One worker makes all three keys share the same WorkerStore mutex.
      // No value read precedes A after restart, so A loads the old disk value.
      Server server(disk, 1);
      Client client(server.port());
      PendingHashWrite first(server.port()), second(server.port());
      first.Send({verb, key, "field0", "first"});
      ASSERT_TRUE(
          AwaitSourceLog(server, "compact hash write pause armed key=" + key))
          << server.Log();
      second.Send({"HSET", key, "second", "second"});
      EXPECT_EQ(client.Command({"SET", "other-string", "after"}).text_, "OK");
      EXPECT_EQ(
          client.Command({"HSET", "other-hash", "new", "independent"}).text_,
          "1");
      EXPECT_EQ(client.Command({"GET", "other-string"}).text_, "after");
      EXPECT_EQ(client.Command({"HGET", "other-hash", "new"}).text_,
                "independent");
      // Success after A resumed would not prove independence. Check the
      // explicit completion marker as well as both still-pending responses.
      EXPECT_EQ(
          server.Log().find("compact hash write pause complete key=" + key),
          std::string::npos)
          << server.Log();
      EXPECT_FALSE(first.HasReply());
      EXPECT_FALSE(second.HasReply());
      EXPECT_EQ(first.ReadStatus(),
                std::string_view(verb) == "HSET" ? ":0\r\n" : "+OK\r\n");
      EXPECT_EQ(second.ReadStatus(), ":1\r\n");
      EXPECT_EQ(client.Command({"HGET", key, "field0"}).text_, "first");
      EXPECT_EQ(client.Command({"HGET", key, "second"}).text_, "second");
      EXPECT_EQ(client.Command({"HLEN", key}).text_, "11");
      client.Durable();
      ASSERT_EQ(server.Wait(true), 0) << server.Log();
    }
    Server recovered(disk, 1);
    Client client(recovered.port());
    EXPECT_EQ(client.Command({"HGET", key, "field0"}).text_, "first");
    EXPECT_EQ(client.Command({"HGET", key, "second"}).text_, "second");
    for (unsigned i = 1; i < 10; ++i)
      EXPECT_EQ(
          client.Command({"HGET", key, "field" + std::to_string(i)}).text_,
          std::string(128, 'v'));
    EXPECT_EQ(client.Command({"HGET", "other-hash", "seed"}).text_, "keep");
    ASSERT_EQ(recovered.Wait(true), 0) << recovered.Log();
  }
}

TEST(GroupedHashWriteE2e,
     ColdCompactNoopWatchHmsetTtlAndPromotionKeepSemantics) {
  PrivateDisk disk;
  const std::string key = "compact-semantics";
  {
    Server server(disk, 1);
    Client client(server.port());
    ASSERT_EQ(client.Command(SmallHashCommand(key)).text_, "10");
    ASSERT_EQ(client.Command({"EXPIRE", key, "600"}).text_, "1");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  ASSERT_TRUE(disk.Auxiliaries(key).empty());
  {
    Server server(disk, 1);
    Client client(server.port()), watcher(server.port());
    const auto ttl = std::stoll(client.Command({"PTTL", key}).text_);
    ASSERT_GT(ttl, 0);
    ASSERT_EQ(watcher.Command({"WATCH", key}).text_, "OK");
    ASSERT_EQ(
        client.Command({"HSET", key, "field0", std::string(128, 'v')}).text_,
        "0");
    ASSERT_EQ(watcher.Command({"MULTI"}).text_, "OK");
    ASSERT_EQ(watcher.Command({"HGET", key, "field0"}).text_, "QUEUED");
    const auto aborted = watcher.Command({"EXEC"});
    // Redis dirties WATCH for a successful HSET even when bytes do not change.
    EXPECT_EQ(aborted.kind_, '*');
    EXPECT_EQ(aborted.text_, "-1");
    ASSERT_EQ(
        client.Command({"HMSET", key, "field0", "changed", "extra", "kept"})
            .text_,
        "OK");
    EXPECT_EQ(client.Command({"HGET", key, "field0"}).text_, "changed");
    EXPECT_EQ(client.Command({"HLEN", key}).text_, "11");
    const auto after_hmset = std::stoll(client.Command({"PTTL", key}).text_);
    EXPECT_GT(after_hmset, 0);
    EXPECT_LE(after_hmset, ttl);
    // This starts compact but must rejoin the existing locked promotion path.
    ASSERT_EQ(client.Command(HashCommand(key, 'p')).text_, "246");
    EXPECT_EQ(client.Command({"HLEN", key}).text_, "257");
    EXPECT_EQ(client.Command({"HGET", key, "extra"}).text_, "kept");
    const auto after_promotion =
        std::stoll(client.Command({"PTTL", key}).text_);
    EXPECT_GT(after_promotion, 0);
    EXPECT_LE(after_promotion, after_hmset);
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  ASSERT_FALSE(disk.Auxiliaries(key).empty());
  Server recovered(disk, 1);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"HLEN", key}).text_, "257");
  EXPECT_EQ(client.Command({"HGET", key, "field255"}).text_,
            std::string(128, 'p'));
  EXPECT_EQ(client.Command({"HGET", key, "extra"}).text_, "kept");
  EXPECT_GT(std::stoll(client.Command({"PTTL", key}).text_), 0);
  ASSERT_EQ(recovered.Wait(true), 0) << recovered.Log();
}

TEST(GroupedHashWriteE2e, PublicationHandoffOomFailsClosedAndRecoversOldGraph) {
#if !KEYLANE_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires allocation failure after grouped root staging";
#endif
  const std::string old_value(20 * 1024, 'o');
  const std::string new_value(20 * 1024, 'n');
  struct Case {
    std::vector<std::string> initial_, mutation_, read_;
    std::string expected_;
  };
  const std::vector<Case> cases{{{"HSET", "handoff", "field", old_value},
                                 {"HSET", "handoff", "field", new_value},
                                 {"HGET", "handoff", "field"},
                                 old_value},
                                {{"SADD", "handoff", old_value},
                                 {"SADD", "handoff", new_value},
                                 {"SCARD", "handoff"},
                                 "1"},
                                {{"RPUSH", "handoff", old_value},
                                 {"LSET", "handoff", "0", new_value},
                                 {"LINDEX", "handoff", "0"},
                                 old_value},
                                {{"ZADD", "handoff", "1", old_value},
                                 {"ZADD", "handoff", "2", old_value},
                                 {"ZSCORE", "handoff", old_value},
                                 "1"},
                                {{"HSET", "handoff", "field", old_value},
                                 {"PEXPIRE", "handoff", "1"},
                                 {"HGET", "handoff", "field"},
                                 old_value}};
  for (const auto& test : cases) {
    SCOPED_TRACE(test.mutation_.front());
    PrivateDisk disk;
    {
      Server server(disk, 1);
      Client client(server.port());
      ASSERT_NE(client.Command(test.initial_).kind_, '-');
      client.Durable();
      ASSERT_EQ(server.Wait(true), 0) << server.Log();
    }
    ASSERT_FALSE(disk.Auxiliaries("handoff").empty());
    {
      std::unique_ptr<Server> server;
      {
        ScopedSourceFault fault("KEYLANE_FAIL_GROUP_HANDOFF_KEY", "handoff");
        server = std::make_unique<Server>(disk, 1);
      }
      Client client(server->port());
      const auto failed = client.Command(test.mutation_);
      ASSERT_EQ(failed.kind_, '-') << failed.text_;
      EXPECT_NE(failed.text_.find("OOM"), std::string::npos);
      EXPECT_EQ(client.Command(test.read_).kind_, '-');
      EXPECT_EQ(client.Command({"TYPE", "handoff"}).kind_, '-');
      // The fixture terminates this fail-stopped child without committing its
      // undecided root. Recovery must select the preceding complete graph.
    }
    Server recovered(disk, 3);
    Client client(recovered.port());
    EXPECT_EQ(client.Command(test.read_).text_, test.expected_);
    EXPECT_EQ(client.Command({"PTTL", "handoff"}).text_, "-1");
    EXPECT_EQ(client.Command({"SET", "after-recovery", "ok"}).text_, "OK");
    client.Durable();
    EXPECT_EQ(recovered.Wait(true), 0) << recovered.Log();
  }
}

TEST(GroupedHashWriteE2e, NativeCancelReleasesActiveHugePageAndExactPins) {
#if !KEYLANE_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires Debug source cancellation hook";
#endif
  PrivateDisk source_disk;
  PrivateDisk replica_disk;
  const std::string huge(9 * 1024 * 1024, 'C');
  std::unique_ptr<Server> source;
  {
    ScopedSourceFault fault("KEYLANE_PAUSE_FULLSYNC_COLLECTION_CHUNK_KEY",
                            "cancel-hash");
    source = std::make_unique<Server>(source_disk, 2);
  }
  Server replica(replica_disk, 3);
  Client writer(source->port());
  Client follower(replica.port());
  ASSERT_EQ(writer.Command({"HSET", "cancel-hash", "large", huge}).text_, "1");
  writer.Durable();
  ASSERT_EQ(
      follower
          .Command({"REPLICAOF", "127.0.0.1", std::to_string(source->port())})
          .text_,
      "OK");
  ASSERT_TRUE(AwaitSourceLog(
      *source, "paused full-sync collection chunk key=cancel-hash"))
      << source->Log() << replica.Log();
  // Closing the target cancels its session while a read owns the borrowed page,
  // not after EOF. NO ONE would request promotion of this deliberately partial
  // candidate, which is a different operation and is correctly inadmissible.
  ASSERT_EQ(replica.Wait(true), 0) << replica.Log();
  ASSERT_TRUE(
      AwaitSourceLog(*source, "released full-sync collection key=cancel-hash"))
      << source->Log();
  EXPECT_NE(source->Log().find("held=0"), std::string::npos);
  EXPECT_EQ(source->Log().find("key=cancel-hash pins=0"), std::string::npos);
  const auto deadline = std::chrono::steady_clock::now() + 10s;
  while (MemoryInfoField(writer, "fullsync_reserved_memory") != 0 &&
         std::chrono::steady_clock::now() < deadline)
    std::this_thread::sleep_for(10ms);
  EXPECT_EQ(MemoryInfoField(writer, "fullsync_reserved_memory"), 0);
  EXPECT_EQ(MemoryInfoField(writer, "memory_admission_pending"), 0);
  // Inspect the actual owner's synchronous page-charge release, not a net
  // process-wide used_memory delta across network waits. Connection/IO caches
  // can grow concurrently and mask a correctly released 9 MiB page.
  const auto log = source->Log();
  const auto release_at =
      log.find("released full-sync collection key=cancel-hash ");
  ASSERT_NE(release_at, std::string::npos);
  const auto release =
      log.substr(release_at, log.find('\n', release_at) - release_at);
  auto release_field = [&](std::string_view name) {
    const auto at = release.find(std::string(name) + "=");
    Check(at != std::string::npos, "missing source release memory field");
    return std::stoll(release.substr(at + name.size() + 1));
  };
  const auto page_bytes = release_field("page_bytes");
  const auto page_charge = release_field("page_charge");
  EXPECT_GE(page_bytes, static_cast<std::int64_t>(huge.size()));
  EXPECT_GE(page_charge, page_bytes);
  EXPECT_GE(release_field("released_page_charge"), page_charge);
  EXPECT_EQ(writer.Command({"HGET", "cancel-hash", "large"}).text_, huge);
  EXPECT_EQ(writer.Command({"SET", "after-cancel", "ok"}).text_, "OK");
  writer.Durable();
  EXPECT_EQ(source->Wait(true), 0) << source->Log();
}

TEST(GroupedHashWriteE2e, NativeShutdownWaitsForUnpublishedPinnedScan) {
#if !KEYLANE_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires Debug source pre-scan hook";
#endif
  PrivateDisk source_disk;
  PrivateDisk replica_disk;
  const std::string huge(9 * 1024 * 1024, 'S');
  {
    std::unique_ptr<Server> source;
    {
      ScopedSourceFault fault("KEYLANE_PAUSE_FULLSYNC_COLLECTION_SCAN_KEY",
                              "shutdown-hash");
      source = std::make_unique<Server>(source_disk, 2);
    }
    Server replica(replica_disk, 3);
    Client writer(source->port());
    Client follower(replica.port());
    ASSERT_EQ(writer.Command({"HSET", "shutdown-hash", "large", huge}).text_,
              "1");
    writer.Durable();
    ASSERT_EQ(
        follower
            .Command({"REPLICAOF", "127.0.0.1", std::to_string(source->port())})
            .text_,
        "OK");
    ASSERT_TRUE(AwaitSourceLog(
        *source, "paused full-sync collection pre-scan key=shutdown-hash"))
        << source->Log() << replica.Log();
    ASSERT_EQ(source->Wait(true), 0) << source->Log();
    EXPECT_NE(
        source->Log().find("released full-sync collection key=shutdown-hash"),
        std::string::npos)
        << source->Log();
    EXPECT_NE(source->Log().find("held=0"), std::string::npos);
    EXPECT_EQ(source->Log().find("key=shutdown-hash pins=0"),
              std::string::npos);
  }
  Server recovered(source_disk, 4);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"HGET", "shutdown-hash", "large"}).text_, huge);
}

class GroupedHashWriteCrashE2e : public testing::TestWithParam<const char*> {};

TEST_P(GroupedHashWriteCrashE2e, UncommittedOuterNeverPublishesPartialHash) {
#if !KEYLANE_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires Debug/fault crash hook";
#endif
  PrivateDisk disk;
  {
    Server server(disk);
    Client client(server.port());
    ASSERT_EQ(client.Command(HashCommand("hash")).text_, "256");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  {
    Server server(disk, 2, GetParam());
    Client client(server.port());
    EXPECT_EQ(client.Command({"MULTI"}).text_, "OK");
    if (std::string_view(GetParam()) == "group-extents-durable-before-record") {
      EXPECT_EQ(client
                    .Command({"HSET", "hash", "field0",
                              std::string(9 * 1024 * 1024, 'X')})
                    .text_,
                "QUEUED");
    } else {
      EXPECT_EQ(client.Command(HashCommand("hash", 'x')).text_, "QUEUED");
    }
    EXPECT_THROW(client.Command({"EXEC"}), std::runtime_error);
    EXPECT_EQ(server.Wait(), 86) << server.Log();
  }
  Server recovered(disk, 3);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"HLEN", "hash"}).text_, "256");
  for (unsigned i = 0; i < 256; ++i) {
    EXPECT_EQ(
        client.Command({"HGET", "hash", "field" + std::to_string(i)}).text_,
        std::string(128, 'v'));
  }
}

INSTANTIATE_TEST_SUITE_P(
    BatchWindows, GroupedHashWriteCrashE2e,
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
