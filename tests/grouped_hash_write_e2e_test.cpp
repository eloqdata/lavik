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

#include <cerrno>
#include <memory>
#include <optional>

#include "grouped_write_e2e_support.h"
#include "lavik/storage/detail/grouped_hash.h"

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
      if (stored_key == key && record.grouped_ && !record.auxiliary_group_ &&
          record.mutation_sequence_ >= root_sequence) {
        Check(!record.external_ && !record.key_indirect_,
              "fixture root must be inline");
        auto root = DecodeGroupedHashRoot(std::string_view(
            reinterpret_cast<const char*>(bytes.data() + offset +
                                          record.header_bytes_),
            record.payload_bytes_));
        Check(root.ok(), "fixture grouped root is invalid");
        result.root_ = *root;
        root_sequence = record.mutation_sequence_;
      }
      if (stored_key == key && record.auxiliary_group_)
        auxiliary.push_back(record);
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

TEST(GroupedHashWriteE2e,
     DemotesBelowStrictGroupPayloadThresholdAfterRecovery) {
  PrivateDisk disk;
  {
    Server server(disk);
    Client client(server.port());
    ASSERT_EQ(
        client.Command({"HSET", "demote-hash", "f", std::string(17000, 'a')})
            .text_,
        "1");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  ASSERT_EQ(disk.LatestRootGrouped("demote-hash"), true);
  // One Hash page is 48-byte group envelope, 32-byte compact header,
  // 8-byte entry framing, one field byte and the value bytes.
  {
    Server server(disk);
    Client client(server.port());
    ASSERT_EQ(
        client.Command({"HSET", "demote-hash", "f", std::string(8103, 'b')})
            .text_,
        "0");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  ASSERT_EQ(disk.LatestRootGrouped("demote-hash"), true);
  {
    Server server(disk);
    Client client(server.port());
    ASSERT_EQ(client.Command({"MULTI"}).text_, "OK");
    ASSERT_EQ(
        client.Command({"HSET", "demote-hash", "f", std::string(8102, 'c')})
            .text_,
        "QUEUED");
    const auto executed = client.Command({"EXEC"});
    ASSERT_EQ(executed.items_.size(), 1);
    ASSERT_EQ(executed.items_[0].text_, "0");
    ASSERT_EQ(client.Command({"HGET", "demote-hash", "f"}).text_,
              std::string(8102, 'c'));
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  ASSERT_EQ(disk.LatestRootGrouped("demote-hash"), false);
  Server recovered(disk);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"HGET", "demote-hash", "f"}).text_,
            std::string(8102, 'c'));
}

TEST(GroupedHashWriteE2e, BatchedFieldsPreserveOrderAcrossGrowthAndRecovery) {
  PrivateDisk disk;
  std::map<std::string, std::string> expected;
  auto field = [](unsigned i) {
    // Exercise short strings that move with vector growth, heap strings and
    // binary identities that must compare by their complete byte sequence.
    if (i % 3 == 0) return "f" + std::to_string(i);
    if (i % 3 == 1) return std::string(80, 'f') + std::to_string(i);
    return std::string("f\0", 2) + std::to_string(i);
  };
  auto verify = [&](Client& client) {
    EXPECT_EQ(client.Command({"HLEN", "batched"}).text_,
              std::to_string(expected.size()));
    std::vector<std::string> read{"HMGET", "batched"};
    for (const auto& [name, value] : expected) read.push_back(name);
    const auto reply = client.Command(read);
    ASSERT_EQ(reply.items_.size(), expected.size());
    std::size_t i = 0;
    for (const auto& [name, value] : expected) {
      EXPECT_EQ(reply.items_[i++].text_, value) << name;
    }
  };
  {
    Server server(disk, 1);
    Client client(server.port()), watcher(server.port());
    ASSERT_EQ(client
                  .Command({"HSET", "batched", field(0), "first", field(0),
                            "last", field(1), "keep"})
                  .text_,
              "2");
    expected[field(0)] = "last";
    expected[field(1)] = "keep";
    // The first update starts compact; subsequent batches span prefix groups.
    // Repeat both an existing and a new field after hundreds of appends.
    for (unsigned batch = 0; batch < 4; ++batch) {
      const unsigned begin = 2 + batch * 512;
      std::vector<std::string> write{"HSET", "batched", field(0), "early"};
      for (unsigned i = begin; i < begin + 512; ++i) {
        write.push_back(field(i));
        write.emplace_back(128, 'a' + batch);
        expected[field(i)] = write.back();
      }
      write.insert(write.end(), {field(0), "updated", field(begin), "last"});
      expected[field(0)] = "updated";
      expected[field(begin)] = "last";
      if (batch == 0) {
        // A missing key takes unlocked creation's periodic Yield while the
        // index is live; duplicate fields must still resolve after resumption.
        auto create = write;
        create[1] = "fresh-batched";
        ASSERT_EQ(client.Command(create).text_, "513");
        EXPECT_EQ(client.Command({"HGET", "fresh-batched", field(0)}).text_,
                  "updated");
        EXPECT_EQ(client.Command({"HGET", "fresh-batched", field(begin)}).text_,
                  "last");
      }
      ASSERT_EQ(client.Command(write).text_, "512");
      ASSERT_EQ(client.Command(write).text_, "0");
      verify(client);
    }
    EXPECT_EQ(client.Command({"HSETNX", "batched", field(0), "ignored"}).text_,
              "0");
    EXPECT_EQ(client.Command({"HSETNX", "batched", "nx", "inserted"}).text_,
              "1");
    expected["nx"] = "inserted";
    ASSERT_EQ(watcher.Command({"WATCH", "batched"}).text_, "OK");
    ASSERT_EQ(client.Command({"MULTI"}).text_, "OK");
    ASSERT_EQ(client
                  .Command({"HSET", "batched", field(0), "transient", "tx",
                            "first", field(0), "updated", "tx", "last"})
                  .text_,
              "QUEUED");
    const auto committed = client.Command({"EXEC"});
    ASSERT_EQ(committed.items_.size(), 1);
    EXPECT_EQ(committed.items_[0].text_, "1");
    expected["tx"] = "last";
    ASSERT_EQ(watcher.Command({"MULTI"}).text_, "OK");
    ASSERT_EQ(watcher.Command({"HLEN", "batched"}).text_, "QUEUED");
    EXPECT_EQ(watcher.Command({"EXEC"}).text_, "-1");
    verify(client);
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  ASSERT_FALSE(disk.Auxiliaries("batched").empty());
  Server recovered(disk, 1);
  Client client(recovered.port());
  verify(client);
  EXPECT_EQ(client.Command({"HLEN", "fresh-batched"}).text_, "513");
  EXPECT_EQ(client.Command({"HGET", "fresh-batched", field(0)}).text_,
            "updated");
  EXPECT_EQ(client.Command({"HGET", "fresh-batched", field(2)}).text_, "last");
}

TEST(HashReplaceE2e, SemanticsTtlBinaryFieldsAndUnchangedStandardCommands) {
  PrivateDisk disk;
  Server server(disk);
  Client client(server.port());
  EXPECT_EQ(client.Command({"LAVIK.HREPLACE", "missing", "f", "v"}).text_,
            "-1");
  EXPECT_EQ(client.Command({"EXISTS", "missing"}).text_, "0");
  EXPECT_EQ(client.Command({"SET", "string", "value"}).text_, "OK");
  EXPECT_TRUE(client.Command({"LAVIK.HREPLACE", "string", "f", "v"})
                  .text_.starts_with("WRONGTYPE"));
  EXPECT_EQ(client.Command({"GET", "string"}).text_, "value");
  EXPECT_EQ(client.Command({"HSET", "hash", "a", "1", "b", "2"}).text_, "2");
  EXPECT_EQ(client.Command({"HMSET", "hash", "a", "3"}).text_, "OK");
  EXPECT_EQ(client.Command({"HGET", "hash", "b"}).text_, "2");
  EXPECT_EQ(client.Command({"EXPIRE", "hash", "3600"}).text_, "1");
  const auto expiry = client.Command({"PEXPIRETIME", "hash"}).text_;
  const std::string binary("f\0x", 3), value("v\0y", 3);
  EXPECT_EQ(client
                .Command({"lavik.hreplace", "hash", "a", "first", "a", "last",
                          binary, value})
                .text_,
            "OK");
  EXPECT_EQ(client.Command({"HLEN", "hash"}).text_, "2");
  EXPECT_EQ(client.Command({"HGET", "hash", "a"}).text_, "last");
  EXPECT_EQ(client.Command({"HGET", "hash", "b"}).text_, "-1");
  EXPECT_EQ(client.Command({"HGET", "hash", binary}).text_, value);
  EXPECT_EQ(client.Command({"PEXPIRETIME", "hash"}).text_, expiry);
  EXPECT_EQ(client.Command({"LAVIK.HREPLACE", "hash", "odd"}).kind_, '-');
  EXPECT_EQ(client.Command({"LAVIK.HREPLACE", "hash", "a", "v", "odd"}).kind_,
            '-');
  EXPECT_EQ(client.Command({"HLEN", "hash"}).text_, "2");
  EXPECT_EQ(client.Command({"PEXPIREAT", "hash", "1"}).text_, "1");
  EXPECT_EQ(client.Command({"LAVIK.HREPLACE", "hash", "f", "v"}).text_, "-1");
  EXPECT_EQ(client.Command({"EXISTS", "hash"}).text_, "0");
}

TEST(HashReplaceE2e, DifferentRequestSizesPreserveLastDuplicateAfterRecovery) {
  PrivateDisk disk;
  std::map<std::string, std::map<std::string, std::string>> expected;
  {
    Server server(disk);
    Client client(server.port());
    // Cover different argument counts, including duplicate empty/binary fields
    // and values that do and do not fit the string implementation's SSO buffer.
    for (unsigned count : {1, 15, 16, 17, 65}) {
      const auto key = "replace-" + std::to_string(count);
      ASSERT_EQ(client.Command({"HSET", key, "old-field", "old"}).text_, "1");
      std::vector<std::string> args{"LAVIK.HREPLACE", key};
      for (unsigned i = 0; i < count; ++i) {
        std::string field =
            i % 3 == 0 ? "" : std::string("f\0", 2) + std::to_string(i % 7);
        std::string value = i % 4 == 0 ? "" : std::string(i * 3, 'v');
        expected[key][field] = value;
        args.push_back(std::move(field));
        args.push_back(std::move(value));
      }
      ASSERT_EQ(client.Command(args).text_, "OK");
      EXPECT_EQ(client.Command({"HLEN", key}).text_,
                std::to_string(expected[key].size()));
      EXPECT_EQ(client.Command({"HEXISTS", key, "old-field"}).text_, "0");
      for (const auto& [field, value] : expected[key])
        EXPECT_EQ(client.Command({"HGET", key, field}).text_, value);
    }
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server recovered(disk, 3);
  Client client(recovered.port());
  for (const auto& [key, fields] : expected) {
    EXPECT_EQ(client.Command({"HLEN", key}).text_,
              std::to_string(fields.size()));
    EXPECT_EQ(client.Command({"HEXISTS", key, "old-field"}).text_, "0");
    for (const auto& [field, value] : fields)
      EXPECT_EQ(client.Command({"HGET", key, field}).text_, value);
  }
}

TEST(HashReplaceE2e, PromotionUsesFinalDeduplicatedBytes) {
  PrivateDisk disk;
  constexpr std::size_t boundary =
      kCollectionPromotionBytes - kHashValueHeaderBytes - 9;
  {
    Server server(disk);
    Client client(server.port());
    for (const std::size_t size : {boundary - 1, boundary, boundary + 1}) {
      const auto key = "boundary-" + std::to_string(size);
      ASSERT_EQ(client.Command({"HSET", key, "old", "old"}).text_, "1");
      // Only the final duplicate participates in promotion. The overwritten
      // large value must neither force grouping nor leak into the after-image.
      ASSERT_EQ(
          client
              .Command({"LAVIK.HREPLACE", key, "x", std::string(32 * 1024, 'd'),
                        "x", std::string(size, 'v')})
              .text_,
          "OK");
      EXPECT_EQ(client.Command({"HLEN", key}).text_, "1");
      EXPECT_EQ(client.Command({"HGET", key, "x"}).text_,
                std::string(size, 'v'));
    }
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  for (const std::size_t size : {boundary - 1, boundary, boundary + 1}) {
    const auto key = "boundary-" + std::to_string(size);
    EXPECT_EQ(disk.Auxiliaries(key).empty(), size < boundary);
  }
  Server recovered(disk, 3);
  Client client(recovered.port());
  for (const std::size_t size : {boundary - 1, boundary, boundary + 1}) {
    const auto key = "boundary-" + std::to_string(size);
    EXPECT_EQ(client.Command({"HLEN", key}).text_, "1");
    EXPECT_EQ(client.Command({"HGET", key, "x"}).text_, std::string(size, 'v'));
    EXPECT_EQ(client.Command({"HEXISTS", key, "old"}).text_, "0");
  }
}

TEST(HashReadOwnershipE2e, FullReadsPreserveCompactAndGroupedValues) {
  PrivateDisk disk;
  std::map<std::string, std::map<std::string, std::string>> hashes;
  for (const unsigned count : {10u, 256u}) {
    auto& fields = hashes["hash-" + std::to_string(count)];
    fields[""] = "";
    fields[std::string("binary\0field", 12)] = std::string("v\0x", 3);
    fields[std::string(80, 'f')] = std::string(128, 'l');
    for (unsigned i = 0; i < count; ++i)
      fields["field" + std::to_string(i)] = std::string(128, 'a' + i % 26);
  }
  auto check_reads = [&](Client& client) {
    for (const auto& [key, fields] : hashes) {
      // Alternating full reads must not consume persistent/staged data. The
      // returned strings survive destruction of each private decoded snapshot.
      for (unsigned repeat = 0; repeat < 3; ++repeat) {
        const auto all = client.Command({"HGETALL", key});
        ASSERT_EQ(all.kind_, '*');
        ASSERT_EQ(all.items_.size(), fields.size() * 2);
        std::map<std::string, std::string> actual;
        for (std::size_t i = 0; i < all.items_.size(); i += 2)
          actual.emplace(all.items_[i].text_, all.items_[i + 1].text_);
        EXPECT_EQ(actual, fields);
        const auto keys = client.Command({"HKEYS", key});
        const auto values = client.Command({"HVALS", key});
        ASSERT_EQ(keys.items_.size(), fields.size());
        ASSERT_EQ(values.items_.size(), fields.size());
        std::set<std::string> actual_keys, expected_keys;
        std::multiset<std::string> actual_values, expected_values;
        for (const auto& item : keys.items_) actual_keys.insert(item.text_);
        for (const auto& item : values.items_) actual_values.insert(item.text_);
        for (const auto& [field, value] : fields) {
          expected_keys.insert(field);
          expected_values.insert(value);
        }
        EXPECT_EQ(actual_keys, expected_keys);
        EXPECT_EQ(actual_values, expected_values);
      }
      EXPECT_EQ(client.Command({"HLEN", key}).text_,
                std::to_string(fields.size()));
    }
  };
  {
    Server server(disk, 3);
    Client client(server.port()), watcher(server.port());
    ASSERT_EQ(client.Command({"SET", "string", "unchanged"}).text_, "OK");
    for (const auto& [key, fields] : hashes) {
      std::vector<std::string> args{"HSET", key};
      for (const auto& [field, value] : fields) {
        args.push_back(field);
        args.push_back(value);
      }
      ASSERT_EQ(client.Command(args).text_, std::to_string(fields.size()));
      ASSERT_EQ(watcher.Command({"WATCH", key}).text_, "OK");
    }
    check_reads(client);
    ASSERT_EQ(watcher.Command({"MULTI"}).text_, "OK");
    for (const auto& [key, fields] : hashes) {
      ASSERT_EQ(watcher.Command({"HGETALL", key}).text_, "QUEUED");
      ASSERT_EQ(watcher.Command({"HKEYS", key}).text_, "QUEUED");
      ASSERT_EQ(watcher.Command({"HVALS", key}).text_, "QUEUED");
    }
    const auto replies = watcher.Command({"EXEC"});
    ASSERT_EQ(replies.items_.size(), hashes.size() * 3);
    std::size_t position = 0;
    for (const auto& [key, fields] : hashes) {
      EXPECT_EQ(replies.items_[position++].items_.size(), fields.size() * 2);
      EXPECT_EQ(replies.items_[position++].items_.size(), fields.size());
      EXPECT_EQ(replies.items_[position++].items_.size(), fields.size());
    }
    EXPECT_EQ(client.Command({"GET", "string"}).text_, "unchanged");
    for (const auto* command : {"HGETALL", "HKEYS", "HVALS"}) {
      EXPECT_TRUE(client.Command({command, "missing"}).items_.empty());
      EXPECT_TRUE(
          client.Command({command, "string"}).text_.starts_with("WRONGTYPE"));
    }
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  // Cold recovery and a different worker layout exercise disk-backed reads,
  // not just the append buffers observed immediately after HSET.
  Server recovered(disk, 2);
  Client client(recovered.port());
  check_reads(client);
  EXPECT_EQ(client.Command({"GET", "string"}).text_, "unchanged");
}

TEST(HashReplaceE2e, WatchExecAndLua) {
  PrivateDisk disk;
  Server server(disk);
  Client writer(server.port()), watcher(server.port());
  ASSERT_EQ(writer.Command({"HSET", "hash", "a", "1", "b", "2"}).text_, "2");
  ASSERT_EQ(watcher.Command({"WATCH", "hash"}).text_, "OK");
  ASSERT_EQ(
      writer.Command({"LAVIK.HREPLACE", "hash", "a", "1", "b", "2"}).text_,
      "OK");
  ASSERT_EQ(watcher.Command({"MULTI"}).text_, "OK");
  ASSERT_EQ(watcher.Command({"HLEN", "hash"}).text_, "QUEUED");
  EXPECT_EQ(watcher.Command({"EXEC"}).text_, "-1");
  ASSERT_EQ(writer.Command({"MULTI"}).text_, "OK");
  ASSERT_EQ(writer.Command({"LAVIK.HREPLACE", "hash", "c", "3"}).text_,
            "QUEUED");
  ASSERT_EQ(writer.Command({"HMSET", "hash", "d", "4"}).text_, "QUEUED");
  auto result = writer.Command({"EXEC"});
  ASSERT_EQ(result.items_.size(), 2);
  EXPECT_EQ(result.items_[0].text_, "OK");
  EXPECT_EQ(result.items_[1].text_, "OK");
  EXPECT_EQ(writer.Command({"HLEN", "hash"}).text_, "2");
  EXPECT_EQ(writer.Command({"HGET", "hash", "a"}).text_, "-1");
  EXPECT_EQ(writer
                .Command({"EVAL",
                          "return redis.call('LAVIK.HREPLACE',KEYS[1],'e','5')",
                          "1", "hash"})
                .text_,
            "OK");
  EXPECT_EQ(writer.Command({"HLEN", "hash"}).text_, "1");
  EXPECT_EQ(writer.Command({"HGET", "hash", "e"}).text_, "5");
}

TEST(HashReplaceE2e, GroupedReplacementDemotionExtentsAndColdRecovery) {
  PrivateDisk disk;
  const std::string huge(9 * 1024 * 1024, 'x');
  {
    Server server(disk);
    Client client(server.port());
    ASSERT_EQ(client.Command(HashCommand("hash")).text_, "256");
    auto replacement = HashCommand("hash", 'r');
    replacement[0] = "LAVIK.HREPLACE";
    ASSERT_EQ(client.Command(replacement).text_, "OK");
    client.Durable();
    ASSERT_EQ(client.Command({"LAVIK.HREPLACE", "hash", "only", huge}).text_,
              "OK");
    EXPECT_EQ(client.Command({"HLEN", "hash"}).text_, "1");
    EXPECT_EQ(client.Command({"HGET", "hash", "only"}).text_, huge);
    EXPECT_EQ(client.Command({"HGET", "hash", "field0"}).text_, "-1");
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  {
    Server server(disk);
    Client client(server.port());
    EXPECT_EQ(client.Command({"HLEN", "hash"}).text_, "1");
    EXPECT_EQ(client.Command({"HGET", "hash", "only"}).text_, huge);
    ASSERT_EQ(client.Command({"LAVIK.HREPLACE", "hash", "small", "v"}).text_,
              "OK");
    EXPECT_EQ(client.Command({"HGET", "hash", "only"}).text_, "-1");
    EXPECT_EQ(client.Command({"DEFRAG", "RESUME"}).kind_, '+');
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server recovered(disk, 3);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"HGET", "hash", "small"}).text_, "v");
  EXPECT_EQ(client.Command({"HLEN", "hash"}).text_, "1");
  EXPECT_EQ(client.Command({"HSET", "hash", "new", "field"}).text_, "1");
}

TEST(HashReplaceE2e, AuxiliaryOomPreservesOldHashInsideExec) {
#if !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires Debug/fault server";
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
    auto replacement = HashCommand("hash", 'r');
    replacement[0] = "LAVIK.HREPLACE";
    ASSERT_EQ(client.Command({"MULTI"}).text_, "OK");
    ASSERT_EQ(client.Command(replacement).text_, "QUEUED");
    ASSERT_EQ(client.Command({"SET", "after", "survives"}).text_, "QUEUED");
    auto result = client.Command({"EXEC"});
    ASSERT_EQ(result.items_.size(), 2);
    EXPECT_TRUE(result.items_[0].text_.starts_with("OOM"));
    EXPECT_EQ(result.items_[1].text_, "OK");
    EXPECT_EQ(client.Command({"HGET", "hash", "field0"}).text_,
              std::string(128, 'v'));
    client.Durable();
    ASSERT_EQ(server.Wait(true), 0) << server.Log();
  }
  Server recovered(disk);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"HLEN", "hash"}).text_, "256");
  EXPECT_EQ(client.Command({"HGET", "hash", "field0"}).text_,
            std::string(128, 'v'));
  EXPECT_EQ(client.Command({"GET", "after"}).text_, "survives");
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
#if !LAVIK_TEST_FAULTS_AVAILABLE
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

TEST(HashReplaceE2e, ColdReplacementDoesNotLoadOldPayload) {
#if !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires payload read fault hook";
#endif
  for (const bool grouped : {false, true}) {
    SCOPED_TRACE(grouped ? "grouped" : "compact");
    PrivateDisk disk;
    {
      Server server(disk);
      Client client(server.port());
      auto seed = grouped
                      ? HashCommand("hash")
                      : std::vector<std::string>{"HSET", "hash", "old", "v"};
      ASSERT_NE(client.Command(seed).kind_, '-');
      client.Durable();
      ASSERT_EQ(server.Wait(true), 0) << server.Log();
    }
    {
      std::unique_ptr<Server> server;
      {
        ScopedSourceFault fault("LAVIK_FAIL_VALUE_READ_KEY", "hash");
        server = std::make_unique<Server>(disk);
      }
      Client client(server->port());
      EXPECT_NE(client.Command({"HMSET", "hash", "old", "changed"})
                    .text_.find("injected value payload read failure"),
                std::string::npos);
      ASSERT_EQ(
          client.Command({"LAVIK.HREPLACE", "hash", "new", "image"}).text_,
          "OK");
      EXPECT_EQ(client.Command({"HLEN", "hash"}).text_, "1");
      client.Durable();
      ASSERT_EQ(server->Wait(true), 0) << server->Log();
    }
    Server recovered(disk);
    Client client(recovered.port());
    EXPECT_EQ(client.Command({"HLEN", "hash"}).text_, "1");
    EXPECT_EQ(client.Command({"HGET", "hash", "new"}).text_, "image");
  }
}

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
#if !LAVIK_TEST_FAULTS_AVAILABLE
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
      ScopedSourceFault paused_key("LAVIK_COMPACT_HASH_WRITE_PAUSE_KEY",
                                   key.c_str());
      ScopedSourceFault pause_ms("LAVIK_COMPACT_HASH_WRITE_PAUSE_MS", "3000");
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
#if !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires allocation failure after grouped root staging";
#endif
  const std::string old_value(20 * 1024, 'o');
  const std::string new_value(20 * 1024, 'n');
  // The deadline must still be in the future when storage selects the grouped
  // metadata path. An elapsed deadline takes the tombstone path instead and
  // never reaches the publication-handoff fault this test is exercising.
  constexpr auto expiration_ttl = 5000ms;
  const std::string expiration_ms = std::to_string(expiration_ttl.count());
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
                                 {"PEXPIRE", "handoff", expiration_ms},
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
        ScopedSourceFault fault("LAVIK_FAIL_GROUP_HANDOFF_KEY", "handoff");
        server = std::make_unique<Server>(disk, 1);
      }
      Client client(server->port());
      const auto failed = client.Command(test.mutation_);
      ASSERT_EQ(failed.kind_, '-') << failed.text_;
      EXPECT_NE(failed.text_.find("OOM"), std::string::npos);
      EXPECT_EQ(client.Command(test.read_).kind_, '-');
      EXPECT_EQ(client.Command({"TYPE", "handoff"}).kind_, '-');
      if (test.mutation_.front() == "PEXPIRE") {
        // A failed published root must remain unreadable even after its
        // tentative TTL elapses; expiry must not hide the failed decision.
        std::this_thread::sleep_for(expiration_ttl + 100ms);
        EXPECT_EQ(client.Command(test.read_).kind_, '-');
        EXPECT_EQ(client.Command({"TYPE", "handoff"}).kind_, '-');
      }
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

class GroupedHashWriteCrashE2e : public testing::TestWithParam<const char*> {};

TEST_P(GroupedHashWriteCrashE2e, UncommittedOuterNeverPublishesPartialHash) {
#if !LAVIK_TEST_FAULTS_AVAILABLE
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

TEST_P(GroupedHashWriteCrashE2e, ReplacementNeverRevivesPartialOrOldFields) {
#if !LAVIK_TEST_FAULTS_AVAILABLE
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
    auto replacement = HashCommand("hash", 'r');
    replacement[0] = "LAVIK.HREPLACE";
    if (std::string_view(GetParam()) == "group-extents-durable-before-record")
      replacement = {"LAVIK.HREPLACE", "hash", "only",
                     std::string(9 * 1024 * 1024, 'r')};
    ASSERT_EQ(client.Command({"MULTI"}).text_, "OK");
    ASSERT_EQ(client.Command(replacement).text_, "QUEUED");
    EXPECT_THROW(client.Command({"EXEC"}), std::runtime_error);
    EXPECT_EQ(server.Wait(), 86) << server.Log();
  }
  Server recovered(disk, 3);
  Client client(recovered.port());
  EXPECT_EQ(client.Command({"HLEN", "hash"}).text_, "256");
  EXPECT_EQ(client.Command({"HGET", "hash", "only"}).text_, "-1");
  for (unsigned i = 0; i < 256; ++i)
    EXPECT_EQ(
        client.Command({"HGET", "hash", "field" + std::to_string(i)}).text_,
        std::string(128, 'v'));
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
