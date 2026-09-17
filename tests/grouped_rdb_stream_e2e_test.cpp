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

#include <array>
#include <cstdlib>
#include <fstream>
#include <optional>
#include <set>

#include "grouped_write_e2e_support.h"
#include "lavik/rdb.h"
#include "lavik/rdb_collection.h"
#include "lavik/storage/detail/ordered_compact_codec.h"

namespace {
using namespace grouped_e2e;

struct ScopedEnvironment {
  explicit ScopedEnvironment(const char* name, const char* value)
      : name_(name) {
    if (const char* old = std::getenv(name)) old_ = old;
    ::setenv(name, value, 1);
  }
  ~ScopedEnvironment() {
    if (old_)
      ::setenv(name_, old_->c_str(), 1);
    else
      ::unsetenv(name_);
  }
  const char* name_;
  std::optional<std::string> old_;
};

// This uses the real diskless PSYNC exporter, not the native replication
// transport. Keep only a possible EOF-token prefix between reads; the target
// subsequently consumes the captured file through its ordinary RDB importer.
void CapturePsyncRdb(std::uint16_t port, const std::string& path) {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  Check(fd >= 0, "PSYNC socket failed");
  struct CloseFd {
    int fd;
    ~CloseFd() { ::close(fd); }
  } close{fd};
  sockaddr_in address{.sin_family = AF_INET,
                      .sin_port = htons(port),
                      .sin_addr = {.s_addr = htonl(INADDR_LOOPBACK)}};
  Check(::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) ==
            0,
        "PSYNC connect failed");
  timeval timeout{.tv_sec = 45};
  ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
  auto send = [&](std::string_view bytes) {
    while (!bytes.empty()) {
      const auto n = ::send(fd, bytes.data(), bytes.size(), MSG_NOSIGNAL);
      if (n < 0 && errno == EINTR) continue;
      Check(n > 0, "PSYNC send failed");
      bytes.remove_prefix(n);
    }
  };
  auto line = [&]() {
    std::string value;
    while (!value.ends_with("\r\n")) {
      char next;
      const auto n = ::recv(fd, &next, 1, 0);
      if (n < 0 && errno == EINTR) continue;
      Check(n == 1 && value.size() < 512, "PSYNC response header failed");
      value.push_back(next);
    }
    value.resize(value.size() - 2);
    return value;
  };
  send("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$3\r\neof\r\n");
  Check(line() == "+OK", "PSYNC EOF capability rejected");
  send("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n");
  Check(line().starts_with("+FULLRESYNC "), "PSYNC full sync rejected");
  const auto header = line();
  Check(header.starts_with("$EOF:") && header.size() == 45,
        "PSYNC EOF framing malformed");
  const auto marker = header.substr(5);
  std::ofstream file(path, std::ios::binary | std::ios::trunc);
  Check(file.good(), "PSYNC capture file failed");
  std::array<char, 256 * 1024> buffer;
  std::string pending;
  for (;;) {
    const auto n = ::recv(fd, buffer.data(), buffer.size(), 0);
    if (n < 0 && errno == EINTR) continue;
    Check(n > 0, "PSYNC RDB ended before marker");
    pending.append(buffer.data(), n);
    if (const auto end = pending.find(marker); end != std::string::npos) {
      file.write(pending.data(), end);
      file.close();
      Check(file.good(), "PSYNC capture write failed");
      return;
    }
    if (pending.size() >= marker.size()) {
      const auto bytes = pending.size() - marker.size() + 1;
      file.write(pending.data(), bytes);
      Check(file.good(), "PSYNC capture write failed");
      pending.erase(0, bytes);
    }
  }
}

TEST(GroupedRdbStreamE2e, FourTypesMultiPageLargeItemsAndMultipleWorkers) {
  PrivateDisk disk;
  const std::string large(9 * 1024 * 1024, 'v');
  {
    Server server(disk, 4);
    Client client(server.port());
    std::vector<std::string> hash{"HSET", "hash"};
    std::vector<std::string> set{"SADD", "set"};
    std::vector<std::string> list{"RPUSH", "list"};
    std::vector<std::string> sorted{"ZADD", "sorted"};
    for (unsigned n = 0; n < 256; ++n) {
      auto member = "member-" + std::to_string(n);
      member.resize(128, 'x');
      hash.push_back(member);
      hash.emplace_back(128, 'h');
      set.push_back(member);
      list.push_back(member);
      sorted.push_back(std::to_string(n));
      sorted.push_back(member);
    }
    ASSERT_EQ(client.Command(hash).text_, "256");
    ASSERT_EQ(client.Command(set).text_, "256");
    ASSERT_EQ(client.Command(list).text_, "256");
    ASSERT_EQ(client.Command(sorted).text_, "256");
    ASSERT_EQ(client.Command({"HSET", "hash", "large", large}).text_, "1");
    ASSERT_EQ(client.Command({"SADD", "set", large}).text_, "1");
    ASSERT_EQ(client.Command({"RPUSH", "list", large}).text_, "257");
    ASSERT_EQ(client.Command({"ZADD", "sorted", "300", large}).text_, "1");
    // More keys than one index scan bucket exercises the scan count overshoot
    // contract while producers on different workers compete for output leases.
    for (unsigned n = 0; n < 256; ++n)
      ASSERT_EQ(client
                    .Command({"SET", "plain-" + std::to_string(n),
                              "value-" + std::to_string(n)})
                    .text_,
                "OK");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  for (auto key : {"hash", "set", "list", "sorted"}) {
    const auto groups = disk.Auxiliaries(key);
    ASSERT_FALSE(groups.empty()) << key;
    std::size_t identities = 0;
    for (const auto& [revision, ids] : groups) identities += ids.size();
    EXPECT_GT(identities, 2) << key;
  }
  Server server(disk, 4);
  Client client(server.port());
  ASSERT_EQ(client.Command({"BGSAVE"}).kind_, '+');
  const auto deadline = std::chrono::steady_clock::now() + 30s;
  while (server.Log().find("RDB backup completed:") == std::string::npos &&
         std::chrono::steady_clock::now() < deadline)
    std::this_thread::sleep_for(10ms);
  ASSERT_NE(server.Log().find("RDB backup completed:"), std::string::npos)
      << server.Log();
  auto reader = lavik::rdb::FileReader::Open(disk.path() + ".rdb");
  ASSERT_TRUE(reader.ok()) << reader.status();
  std::set<std::string> seen;
  for (;;) {
    auto next = reader->Next();
    ASSERT_TRUE(next.ok()) << next.status();
    if (!next->has_value()) break;
    auto& value = **next;
    ASSERT_TRUE(seen.insert(value.key_).second) << value.key_;
    if (value.key_.starts_with("plain-")) {
      EXPECT_EQ(value.value_.value_type_, ValueType::kString);
      EXPECT_EQ(value.value_.encoded_, "value-" + value.key_.substr(6));
    } else {
      EXPECT_EQ(value.value_.logical_size_, 257) << value.key_;
      const auto expected_type = value.key_ == "hash"   ? ValueType::kHash
                                 : value.key_ == "set"  ? ValueType::kSet
                                 : value.key_ == "list" ? ValueType::kList
                                                        : ValueType::kSortedSet;
      EXPECT_EQ(value.value_.value_type_, expected_type);
      ASSERT_NE(value.value_.encoded_.find(large), std::string::npos)
          << value.key_;
      if (expected_type == ValueType::kList ||
          expected_type == ValueType::kSortedSet) {
        auto decoded =
            DecodeOrderedCompactValue(expected_type == ValueType::kList
                                          ? OrderedCollectionKind::kList
                                          : OrderedCollectionKind::kSortedSet,
                                      value.value_.encoded_, 257);
        ASSERT_TRUE(decoded.ok()) << decoded.status();
        ASSERT_EQ(decoded->size(), 257);
        EXPECT_EQ(decoded->back().value_, large);
      }
    }
  }
  EXPECT_EQ(seen.size(), 260);
  EXPECT_TRUE(seen.contains("hash"));
  EXPECT_TRUE(seen.contains("set"));
  EXPECT_TRUE(seen.contains("list"));
  EXPECT_TRUE(seen.contains("sorted"));
  ASSERT_EQ(client.Command({"FLUSHDB", "SYNC"}).text_, "OK");
  ASSERT_EQ(server.Wait(true), 0) << server.Log();
}

TEST(GroupedRdbStreamE2e, LaterPageFailurePreservesPreviousDump) {
#if !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires the Debug RDB page failure hook";
#endif
  for (const char* injected : {"1", "alloc:1"}) {
    PrivateDisk disk;
    ScopedEnvironment fault("LAVIK_FAIL_RDB_COLLECTION_PAGE", injected);
    Server server(disk);
    Client client(server.port());
    std::vector<std::string> hash{"HSET", "hash"};
    for (unsigned n = 0; n < 256; ++n) {
      hash.push_back("field-" + std::to_string(n));
      hash.emplace_back(128, 'v');
    }
    ASSERT_EQ(client.Command(hash).text_, "256");
    client.Durable();
    const std::string path = disk.path() + ".rdb";
    {
      auto writer = lavik::rdb::FileWriter::Open(path);
      ASSERT_TRUE(writer.ok()) << writer.status();
      auto entry = lavik::rdb::EncodeFileEntry(
          0, "previous-dump",
          RawValue{.encoded_ = "sentinel",
                   .logical_size_ = 8,
                   .value_type_ = ValueType::kString});
      ASSERT_TRUE(entry.ok()) << entry.status();
      ASSERT_TRUE(writer->WriteFragment(*entry).ok());
      ASSERT_TRUE(writer->Finish().ok());
    }
    ASSERT_EQ(client.Command({"BGSAVE"}).kind_, '+');
    const auto deadline = std::chrono::steady_clock::now() + 20s;
    while (server.Log().find("RDB backup failed:") == std::string::npos &&
           std::chrono::steady_clock::now() < deadline)
      std::this_thread::sleep_for(10ms);
    ASSERT_NE(
        server.Log().find(std::string_view(injected).starts_with("alloc:")
                              ? "OOM RDB collection page allocation"
                              : "injected RDB collection page read failure"),
        std::string::npos)
        << server.Log();
    ASSERT_NE(server.Log().find("RDB backup failed:"), std::string::npos)
        << server.Log();
    auto reader = lavik::rdb::FileReader::Open(path);
    ASSERT_TRUE(reader.ok()) << reader.status();
    auto previous = reader->Next();
    ASSERT_TRUE(previous.ok());
    ASSERT_TRUE(previous->has_value());
    EXPECT_EQ((**previous).key_, "previous-dump");
    EXPECT_EQ((**previous).value_.encoded_, "sentinel");
    auto end = reader->Next();
    ASSERT_TRUE(end.ok());
    EXPECT_FALSE(end->has_value());
    ASSERT_EQ(client.Command({"HLEN", "hash"}).text_, "256");
    ASSERT_EQ(client.Command({"FLUSHDB", "SYNC"}).text_, "OK");
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
}

TEST(GroupedRdbStreamE2e,
     PageAdmissionOomPreservesPreviousDumpAndReleasesPins) {
  PrivateDisk disk;
  const std::string large(32 * 1024 * 1024, 'v');
  {
    // The ordinary request-buffer budget is also worker-local (5% by
    // default); leave enough for this setup command, independently of the
    // much smaller retained-memory quota exercised after cold recovery.
    Server initial(disk, 2, {}, {}, false, 2, "2G");
    Client client(initial.port());
    ASSERT_EQ(client.Command({"HSET", "hash", "field", large}).text_, "1");
    client.Durable();
    ASSERT_EQ(initial.Wait(true), 0) << initial.Log();
  }
  ASSERT_FALSE(disk.Auxiliaries("hash").empty());
  // Recovery admits only the routing envelope. The 32 MiB decoded page cannot
  // fit a four-worker share of 128 MiB (28.8 MiB retained allowance), although
  // its root/token/graph metadata fits. This uses the real admission gate,
  // with neither an injected OOM response nor a runtime maxmemory override.
  Server server(disk, 4, {}, {}, false, 2, "128M");
  Client client(server.port());
  ASSERT_EQ(client.Command({"HLEN", "hash"}).text_, "1");
  const std::string path = disk.path() + ".rdb";
  {
    auto writer = lavik::rdb::FileWriter::Open(path);
    ASSERT_TRUE(writer.ok()) << writer.status();
    auto entry = lavik::rdb::EncodeFileEntry(
        0, "previous-dump",
        RawValue{.encoded_ = "sentinel",
                 .logical_size_ = 8,
                 .value_type_ = ValueType::kString});
    ASSERT_TRUE(entry.ok());
    ASSERT_TRUE(writer->WriteFragment(*entry).ok());
    ASSERT_TRUE(writer->Finish().ok());
  }
  ASSERT_EQ(client.Command({"BGSAVE"}).kind_, '+');
  auto deadline = std::chrono::steady_clock::now() + 20s;
  while (server.Log().find("RDB backup failed:") == std::string::npos &&
         std::chrono::steady_clock::now() < deadline)
    std::this_thread::sleep_for(10ms);
  ASSERT_NE(server.Log().find("OOM RDB collection page retention rejected"),
            std::string::npos)
      << server.Log();
  auto reader = lavik::rdb::FileReader::Open(path);
  ASSERT_TRUE(reader.ok()) << reader.status();
  auto previous = reader->Next();
  ASSERT_TRUE(previous.ok());
  ASSERT_TRUE(previous->has_value());
  EXPECT_EQ((**previous).key_, "previous-dump");
  EXPECT_EQ((**previous).value_.encoded_, "sentinel");
  auto end = reader->Next();
  ASSERT_TRUE(end.ok());
  EXPECT_FALSE(end->has_value());
  ASSERT_EQ(client.Command({"HLEN", "hash"}).text_, "1");
  ASSERT_EQ(server.Wait(true), 0) << server.Log();
  Server recovered(disk);
  Client verify(recovered.port());
  ASSERT_EQ(verify.Command({"HGET", "hash", "field"}).text_, large);
  ASSERT_EQ(verify.Command({"FLUSHDB", "SYNC"}).text_, "OK");
  ASSERT_EQ(recovered.Wait(true), 0) << recovered.Log();
}

TEST(GroupedRdbStreamE2e,
     ConcurrentEndWaitsForAdmittedPageAndAllowsTheNextBackup) {
#if !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires the Debug concurrent RDB End hook";
#endif
  PrivateDisk disk;
  const std::string large(9 * 1024 * 1024, 'v');
  {
    Server initial(disk);
    Client client(initial.port());
    ASSERT_EQ(client.Command({"HSET", "hash", "field", large}).text_, "1");
    client.Durable();
    ASSERT_EQ(initial.Wait(true), 0) << initial.Log();
  }
  ASSERT_FALSE(disk.Auxiliaries("hash").empty());
  ScopedEnvironment cancel("LAVIK_RDB_CANCEL_ADMITTED_PAGE", "hash");
  Server server(disk, 4);
  Client client(server.port());
  const std::string path = disk.path() + ".rdb";
  {
    auto writer = lavik::rdb::FileWriter::Open(path);
    ASSERT_TRUE(writer.ok()) << writer.status();
    auto entry = lavik::rdb::EncodeFileEntry(
        0, "previous-dump",
        RawValue{.encoded_ = "sentinel",
                 .logical_size_ = 8,
                 .value_type_ = ValueType::kString});
    ASSERT_TRUE(entry.ok());
    ASSERT_TRUE(writer->WriteFragment(*entry).ok());
    ASSERT_TRUE(writer->Finish().ok());
  }
  ASSERT_EQ(client.Command({"BGSAVE"}).kind_, '+');
  auto deadline = std::chrono::steady_clock::now() + 20s;
  while ((server.Log().find("RDB backup failed:") == std::string::npos ||
          server.Log().find("RDB page cancellation completed:") ==
              std::string::npos) &&
         std::chrono::steady_clock::now() < deadline)
    std::this_thread::sleep_for(10ms);
  const auto log = server.Log();
  ASSERT_NE(log.find("RDB page cancellation waiting: readers=1"),
            std::string::npos)
      << log;
  ASSERT_NE(log.find("RDB page cancellation completed:"), std::string::npos)
      << log;
  ASSERT_NE(log.find("remaining=0 returned-page-bytes="), std::string::npos)
      << log;
  ASSERT_NE(log.find("RDB backup failed:"), std::string::npos) << log;
  {
    auto reader = lavik::rdb::FileReader::Open(path);
    ASSERT_TRUE(reader.ok()) << reader.status();
    auto previous = reader->Next();
    ASSERT_TRUE(previous.ok());
    ASSERT_TRUE(previous->has_value());
    EXPECT_EQ((**previous).key_, "previous-dump");
    EXPECT_EQ((**previous).value_.encoded_, "sentinel");
    auto end = reader->Next();
    ASSERT_TRUE(end.ok());
    EXPECT_FALSE(end->has_value());
  }
  // The hook is once per process. A new cut exercises the same worker and
  // pin lifecycle after both concurrent End callers have completed.
  ASSERT_EQ(client.Command({"HGET", "hash", "field"}).text_, large);
  ASSERT_EQ(client.Command({"BGSAVE"}).kind_, '+');
  deadline = std::chrono::steady_clock::now() + 20s;
  while (server.Log().find("RDB backup completed:") == std::string::npos &&
         std::chrono::steady_clock::now() < deadline)
    std::this_thread::sleep_for(10ms);
  ASSERT_NE(server.Log().find("RDB backup completed:"), std::string::npos)
      << server.Log();
  auto reader = lavik::rdb::FileReader::Open(path);
  ASSERT_TRUE(reader.ok()) << reader.status();
  auto value = reader->Next();
  ASSERT_TRUE(value.ok());
  ASSERT_TRUE(value->has_value());
  EXPECT_EQ((**value).key_, "hash");
  EXPECT_EQ((**value).value_.value_type_, ValueType::kHash);
  EXPECT_NE((**value).value_.encoded_.find(large), std::string::npos);
  ASSERT_EQ(server.Wait(true), 0) << server.Log();
  Server recovered(disk);
  Client verify(recovered.port());
  ASSERT_EQ(verify.Command({"HGET", "hash", "field"}).text_, large);
  ASSERT_EQ(verify.Command({"FLUSHDB", "SYNC"}).text_, "OK");
  ASSERT_EQ(recovered.Wait(true), 0) << recovered.Log();
}

TEST(GroupedRdbStreamE2e, RestoreStreamsFourTypesInsideAndOutsideExec) {
  PrivateDisk disk;
  {
    Server server(disk);
    Client client(server.port());
    // Packed objects and quicklists announce blobs/nodes, not an aggregate
    // item count. Redis 7.2 fixtures cover unknown-count ingestion through
    // ordinary RESTORE and its EXEC integration, not just the pure decoder.
    const std::pair<const char*, const char*> packed[] = {
        {"packed-list",
         "1201020f0f00000003008161028162028001ff0b0014a88c640000c4d8"},
        {"packed-set",
         "1411110000000200836f6e65048374776f04ff0b00095f1e4ecafc45ae"},
        {"packed-hash",
         "101717000000040082663103827631038266320382763203ff0b006a27f17fe84b4b3"
         "5"},
        {"packed-sorted",
         "1115150000000400816202dffe0281610283312e3504ff0b0005ea2085ce4f62d4"}};
    for (const auto& [key, hex] : packed) {
      std::string payload;
      const std::string_view encoded(hex);
      auto nibble = [](char digit) {
        return digit >= 'a' ? digit - 'a' + 10 : digit - '0';
      };
      for (std::size_t i = 0; i < encoded.size(); i += 2)
        payload.push_back(static_cast<char>((nibble(encoded[i]) << 4) |
                                            nibble(encoded[i + 1])));
      ASSERT_EQ(client.Command({"RESTORE", key, "0", payload}).text_, "OK");
      ASSERT_EQ(client.Command({"MULTI"}).text_, "OK");
      ASSERT_EQ(
          client.Command({"RESTORE", std::string(key) + "-exec", "0", payload})
              .text_,
          "QUEUED");
      auto executed = client.Command({"EXEC"});
      ASSERT_EQ(executed.items_.size(), 1);
      ASSERT_EQ(executed.items_[0].text_, "OK");
    }
    std::vector<std::string> hash{"HSET", "hash"};
    std::vector<std::string> set{"SADD", "set"};
    std::vector<std::string> list{"RPUSH", "list"};
    std::vector<std::string> sorted{"ZADD", "sorted"};
    for (unsigned i = 0; i < 700; ++i) {
      auto member = std::to_string(i);
      member.resize(2048, 'x');
      hash.push_back(std::to_string(i));
      hash.emplace_back(2048, 'v');
      set.push_back(member);
      list.push_back(member);
      sorted.push_back(std::to_string(700 - i));
      sorted.push_back(member);
    }
    for (const auto& command : {hash, set, list, sorted})
      ASSERT_EQ(client.Command(command).text_, "700");
    for (std::string key : {"hash", "set", "list", "sorted"}) {
      auto dumped = client.Command({"DUMP", key});
      ASSERT_EQ(dumped.kind_, '$');
      ASSERT_GT(dumped.text_.size(), 1024 * 1024);
      ASSERT_EQ(
          client.Command({"RESTORE", key + "-copy", "0", dumped.text_}).text_,
          "OK");
      ASSERT_EQ(client.Command({"MULTI"}).text_, "OK");
      ASSERT_EQ(
          client.Command({"RESTORE", key + "-exec", "0", dumped.text_}).text_,
          "QUEUED");
      auto committed = client.Command({"EXEC"});
      ASSERT_EQ(committed.kind_, '*');
      ASSERT_EQ(committed.items_.size(), 1);
      ASSERT_EQ(committed.items_[0].text_, "OK");
      ASSERT_EQ(client.Command({"SET", "invalid-target", "previous"}).text_,
                "OK");
      dumped.text_.back() ^= 1;
      EXPECT_EQ(client
                    .Command({"RESTORE", "invalid-target", "0", dumped.text_,
                              "REPLACE"})
                    .kind_,
                '-');
      EXPECT_EQ(client.Command({"GET", "invalid-target"}).text_, "previous");
    }
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server recovered(disk, 3);
  Client client(recovered.port());
  for (const std::string suffix : {"", "-exec"}) {
    EXPECT_EQ(client.Command({"HLEN", "packed-hash" + suffix}).text_, "2");
    EXPECT_EQ(client.Command({"SCARD", "packed-set" + suffix}).text_, "2");
    EXPECT_EQ(client.Command({"LLEN", "packed-list" + suffix}).text_, "3");
    EXPECT_EQ(client.Command({"ZCARD", "packed-sorted" + suffix}).text_, "2");
  }
  for (const std::string suffix : {"-copy", "-exec"}) {
    EXPECT_EQ(client.Command({"HLEN", "hash" + suffix}).text_, "700");
    EXPECT_EQ(client.Command({"HGET", "hash" + suffix, "699"}).text_,
              std::string(2048, 'v'));
    EXPECT_EQ(client.Command({"SCARD", "set" + suffix}).text_, "700");
    EXPECT_EQ(client.Command({"LLEN", "list" + suffix}).text_, "700");
    EXPECT_EQ(client.Command({"ZCARD", "sorted" + suffix}).text_, "700");
    std::string member = "699";
    member.resize(2048, 'x');
    EXPECT_EQ(client.Command({"ZSCORE", "sorted" + suffix, member}).text_, "1");
  }
  ASSERT_EQ(client.Command({"FLUSHDB", "SYNC"}).text_, "OK");
  ASSERT_EQ(recovered.Wait(true), 0) << recovered.Log();
}

TEST(GroupedRdbStreamE2e, StartupImportStreamsPagesAndSortsUnorderedZsetInput) {
  PrivateDisk disk;
  const std::string input = disk.path() + ".input.rdb";
  struct RemoveInput {
    const std::string& path;
    ~RemoveInput() { ::unlink(path.c_str()); }
  } cleanup{input};
  auto writer = lavik::rdb::FileWriter::Open(input);
  ASSERT_TRUE(writer.ok());
  for (auto type : {ValueType::kHash, ValueType::kSet, ValueType::kList,
                    ValueType::kSortedSet}) {
    const std::string key = type == ValueType::kHash   ? "hash"
                            : type == ValueType::kSet  ? "set"
                            : type == ValueType::kList ? "list"
                                                       : "sorted";
    auto encoder =
        lavik::rdb::CollectionFileEncoder::Create(0, key, type, 700, 0);
    ASSERT_TRUE(encoder.ok());
    while (auto fragment = encoder->Next())
      ASSERT_TRUE(writer->WriteFragment(*fragment).ok());
    for (unsigned n = 0; n < 7; ++n) {
      lavik::storage::CollectionPage page{
          .value_type_ = type, .next_cursor_ = n + 1, .done_ = n == 6};
      for (unsigned i = n * 100; i < (n + 1) * 100; ++i) {
        auto member = std::to_string(i);
        member.resize(2048, 'x');
        if (type == ValueType::kHash)
          page.fields_.push_back({std::to_string(i), std::string(2048, 'v')});
        else if (type == ValueType::kSortedSet)
          page.scored_members_.push_back({std::move(member), double(700 - i)});
        else
          page.elements_.push_back(std::move(member));
      }
      ASSERT_TRUE(encoder->StartPage(page).ok());
      while (auto fragment = encoder->Next())
        ASSERT_TRUE(writer->WriteFragment(*fragment).ok());
    }
    ASSERT_TRUE(encoder->Finish().ok());
  }
  ASSERT_TRUE(writer->Finish().ok());
  {
    Server imported(disk, 2, {}, {}, false, 2, "1G", input);
    const auto deadline = std::chrono::steady_clock::now() + 45s;
    while (imported.Log().find("loaded RDB file") == std::string::npos &&
           std::chrono::steady_clock::now() < deadline)
      std::this_thread::sleep_for(10ms);
    ASSERT_NE(imported.Log().find("loaded RDB file"), std::string::npos)
        << imported.Log();
    Client client(imported.port());
    EXPECT_EQ(client.Command({"HLEN", "hash"}).text_, "700");
    EXPECT_EQ(client.Command({"SCARD", "set"}).text_, "700");
    EXPECT_EQ(client.Command({"LLEN", "list"}).text_, "700");
    auto first = client.Command({"ZRANGE", "sorted", "0", "0", "WITHSCORES"});
    ASSERT_EQ(first.items_.size(), 2);
    std::string member = "699";
    member.resize(2048, 'x');
    EXPECT_EQ(first.items_[0].text_, member);
    EXPECT_EQ(first.items_[1].text_, "1");
    client.Durable();
    ASSERT_EQ(imported.Wait(true), 0) << imported.Log();
  }
  Server recovered(disk, 3);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"HGET", "hash", "699"}).text_,
            std::string(2048, 'v'));
  EXPECT_EQ(client.Command({"ZCARD", "sorted"}).text_, "700");
  ASSERT_EQ(client.Command({"FLUSHDB", "SYNC"}).text_, "OK");
  ASSERT_EQ(recovered.Wait(true), 0) << recovered.Log();
}

TEST(GroupedRdbStreamE2e, DisklessPsyncStreamsFourTypesThenImportsAndRestarts) {
  PrivateDisk source_disk;
  PrivateDisk target_disk;
  const std::string input = target_disk.path() + ".psync.rdb";
  struct RemoveInput {
    const std::string& path;
    ~RemoveInput() { ::unlink(path.c_str()); }
  } cleanup{input};
  const std::string large(9 * 1024 * 1024, 'p');
  std::string expiration;
  {
    Server source(source_disk, 4);
    Client writer(source.port());
    std::vector<std::string> hash{"HSET", "hash"};
    std::vector<std::string> set{"SADD", "set"};
    std::vector<std::string> list{"RPUSH", "list"};
    std::vector<std::string> sorted{"ZADD", "sorted"};
    for (unsigned n = 0; n < 256; ++n) {
      auto member = std::to_string(n);
      member.resize(128, 'x');
      hash.push_back(member);
      hash.emplace_back(128, 'h');
      set.push_back(member);
      list.push_back(member);
      sorted.push_back(std::to_string(n));
      sorted.push_back(member);
    }
    ASSERT_EQ(writer.Command(hash).text_, "256");
    ASSERT_EQ(writer.Command(set).text_, "256");
    ASSERT_EQ(writer.Command(list).text_, "256");
    ASSERT_EQ(writer.Command(sorted).text_, "256");
    ASSERT_EQ(writer.Command({"HSET", "hash", "large", large}).text_, "1");
    ASSERT_EQ(writer.Command({"SADD", "set", large}).text_, "1");
    ASSERT_EQ(writer.Command({"RPUSH", "list", large}).text_, "257");
    ASSERT_EQ(writer.Command({"ZADD", "sorted", "300", large}).text_, "1");
    ASSERT_EQ(writer.Command({"PEXPIRE", "hash", "3600000"}).text_, "1");
    expiration = writer.Command({"PEXPIRETIME", "hash"}).text_;
    for (unsigned n = 0; n < 128; ++n)
      ASSERT_EQ(
          writer.Command({"SET", "plain-" + std::to_string(n), "v"}).text_,
          "OK");
    writer.Durable();
    ASSERT_NO_THROW(CapturePsyncRdb(source.port(), input)) << source.Log();
    auto reader = lavik::rdb::FileReader::Open(input);
    ASSERT_TRUE(reader.ok()) << reader.status();
    unsigned keys = 0;
    for (;;) {
      auto next = reader->NextStreaming();
      ASSERT_TRUE(next.ok()) << next.status();
      if (!next->has_value()) break;
      ASSERT_TRUE(reader->DrainCollection().ok());
      ++keys;
    }
    EXPECT_EQ(keys, 132);
    ASSERT_EQ(source.Wait(true), 0) << source.Log();
  }
  {
    Server imported(target_disk, 2, {}, {}, false, 2, "1G", input);
    const auto deadline = std::chrono::steady_clock::now() + 60s;
    while (imported.Log().find("loaded RDB file") == std::string::npos &&
           std::chrono::steady_clock::now() < deadline)
      std::this_thread::sleep_for(10ms);
    ASSERT_NE(imported.Log().find("loaded RDB file"), std::string::npos)
        << imported.Log();
    Client client(imported.port());
    EXPECT_EQ(client.Command({"HLEN", "hash"}).text_, "257");
    EXPECT_EQ(client.Command({"SCARD", "set"}).text_, "257");
    EXPECT_EQ(client.Command({"LLEN", "list"}).text_, "257");
    EXPECT_EQ(client.Command({"ZCARD", "sorted"}).text_, "257");
    EXPECT_EQ(client.Command({"PEXPIRETIME", "hash"}).text_, expiration);
    client.Durable();
    ASSERT_EQ(imported.Wait(true), 0) << imported.Log();
  }
  Server recovered(target_disk, 3);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"HGET", "hash", "large"}).text_, large);
  EXPECT_EQ(client.Command({"SISMEMBER", "set", large}).text_, "1");
  EXPECT_EQ(client.Command({"LINDEX", "list", "-1"}).text_, large);
  EXPECT_EQ(client.Command({"ZSCORE", "sorted", large}).text_, "300");
  EXPECT_EQ(client.Command({"PEXPIRETIME", "hash"}).text_, expiration);
  EXPECT_EQ(client.Command({"GET", "plain-127"}).text_, "v");
  ASSERT_EQ(client.Command({"FLUSHDB", "SYNC"}).text_, "OK");
  ASSERT_EQ(recovered.Wait(true), 0) << recovered.Log();
}

TEST(GroupedRdbStreamE2e,
     LargeListOverOneGiBImportsAndExportsWithoutAggregate) {
  if (std::getenv("LAVIK_RUN_LARGE_RDB") == nullptr)
    GTEST_SKIP() << "opt-in private /mnt/dev 4 GiB image and >1 GiB RDB";
  constexpr std::uint64_t count = 140000;
  constexpr std::size_t member_bytes = 8192;
  static_assert(count * member_bytes > 1024ULL * 1024 * 1024);
  // One worker owns this entire key. Its admission share must cover old and
  // new undo receipts while they are merged during atomic import. Leave room
  // for that transient peak while keeping the process budget below the value
  // size; fixed I/O buffers have a separate budget.
  constexpr std::string_view max_memory = "640M";
  PrivateDisk disk(4ULL * 1024 * 1024 * 1024, "/mnt/dev");
  // A failed multi-minute run must retain its only recovery/timeout evidence.
  // Successful runs still remove all private artifacts.
  disk.PreserveOnFailure();
  const std::string input = disk.path() + ".input.rdb";
  struct RemoveInput {
    const std::string& path;
    const std::string& disk;
    ~RemoveInput() {
      if (!::testing::Test::HasFailure() && std::uncaught_exceptions() == 0) {
        ::unlink(path.c_str());
        ::unlink((disk + ".export.rdb").c_str());
        ::unlink((disk + ".import.log").c_str());
      }
    }
  } cleanup{input, disk.path()};
  auto member = [](std::uint64_t n) {
    auto value = std::to_string(n);
    value.resize(member_bytes, 'l');
    return value;
  };
  {
    auto writer = lavik::rdb::FileWriter::Open(input);
    ASSERT_TRUE(writer.ok());
    auto encoder = lavik::rdb::CollectionFileEncoder::Create(
        0, "large-list", ValueType::kList, count, 0);
    ASSERT_TRUE(encoder.ok());
    while (auto fragment = encoder->Next())
      ASSERT_TRUE(writer->WriteFragment(*fragment).ok());
    for (std::uint64_t begin = 0, cursor = 0; begin < count; begin += 128) {
      const auto end = std::min(begin + 128, count);
      lavik::storage::CollectionPage page{.value_type_ = ValueType::kList,
                                          .next_cursor_ = ++cursor,
                                          .done_ = end == count};
      for (auto i = begin; i < end; ++i) page.elements_.push_back(member(i));
      ASSERT_TRUE(encoder->StartPage(page).ok());
      while (auto fragment = encoder->Next())
        ASSERT_TRUE(writer->WriteFragment(*fragment).ok());
    }
    ASSERT_TRUE(encoder->Finish().ok());
    ASSERT_TRUE(writer->Finish().ok());
  }
  {
    Server imported(disk, 2, {}, {}, false, 2, max_memory, input);
    imported.PreserveOnFailure();
    const auto deadline = std::chrono::steady_clock::now() + 10min;
    while (imported.Log().find("loaded RDB file") == std::string::npos &&
           imported.Running() && std::chrono::steady_clock::now() < deadline)
      std::this_thread::sleep_for(250ms);
    ASSERT_NE(imported.Log().find("loaded RDB file"), std::string::npos)
        << imported.Log();
    Client client(imported.port());
    ASSERT_EQ(client.Command({"LLEN", "large-list"}).text_,
              std::to_string(count));
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "0"}).text_, member(0));
    EXPECT_EQ(client.Command({"LINDEX", "large-list", "-1"}).text_,
              member(count - 1));
    ASSERT_EQ(client.Command({"BGSAVE"}).kind_, '+');
    const auto saved = std::chrono::steady_clock::now() + 10min;
    while (imported.Log().find("RDB backup completed:") == std::string::npos &&
           imported.Running() && std::chrono::steady_clock::now() < saved)
      std::this_thread::sleep_for(250ms);
    ASSERT_NE(imported.Log().find("RDB backup completed:"), std::string::npos)
        << imported.Log();
    auto reader = lavik::rdb::FileReader::Open(disk.path() + ".rdb");
    ASSERT_TRUE(reader.ok()) << reader.status();
    auto entry = reader->NextStreaming();
    ASSERT_TRUE(entry.ok() && entry->has_value());
    ASSERT_TRUE((**entry).collection_stream_);
    std::uint64_t seen = 0;
    for (;;) {
      auto page = reader->ReadCollectionPage();
      ASSERT_TRUE(page.ok()) << page.status();
      for (const auto& value : page->elements_)
        ASSERT_EQ(value, member(seen++));
      if (page->done_) break;
    }
    EXPECT_EQ(seen, count);
    client.Durable();
    imported.RecordDiagnostics("large RDB shutdown begins",
                               client.Command({"INFO", "STATS"}).text_);
    // A later cold-start failure must retain the already verified export and
    // first process's shutdown evidence, not only the last process's log.
    std::filesystem::rename(disk.path() + ".rdb", disk.path() + ".export.rdb");
    struct PreserveImportLog {
      Server& server;
      const std::string& disk;
      ~PreserveImportLog() {
        std::ofstream output(disk + ".import.log");
        output << server.Log();
      }
    } import_log{imported, disk.path()};
    ASSERT_EQ(imported.Wait(true, 120s), 0) << imported.Log();
  }
  Server recovered(disk, 3, {}, {}, false, 2, max_memory);
  recovered.PreserveOnFailure();
  // Rebuilding hundreds of thousands of physical records can outlast the
  // generic client's 20-second connection deadline. Observe actual recovery
  // readiness before connecting, with a bounded allowance only for this case.
  const auto recovered_deadline = std::chrono::steady_clock::now() + 120s;
  while (recovered.Log().find("direct-IO storage initialized") ==
             std::string::npos &&
         recovered.Running() &&
         std::chrono::steady_clock::now() < recovered_deadline)
    std::this_thread::sleep_for(250ms);
  ASSERT_NE(recovered.Log().find("direct-IO storage initialized"),
            std::string::npos)
      << recovered.Log();
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"LLEN", "large-list"}).text_,
            std::to_string(count));
  EXPECT_EQ(client.Command({"LINDEX", "large-list", "-1"}).text_,
            member(count - 1));
  ASSERT_EQ(client.Command({"FLUSHDB", "SYNC"}).text_, "OK");
  ASSERT_EQ(recovered.Wait(true, 120s), 0) << recovered.Log();
}

}  // namespace
