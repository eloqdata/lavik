/*
 * Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>

#include <ranges>
#include <string>

#include "hash_command.h"
#include "lavik/memory.h"
#include "lavik/resp.h"
#include "lavik/storage/detail/hash_codec.h"

namespace lavik {
namespace {

class CompactHashReplyTest : public testing::Test {
 protected:
  void SetUp() override {
    previous_shard_ = CurrentMemoryAccountingShard();
    ASSERT_TRUE(InitMemoryLimit(1024ULL * 1024 * 1024, 1).ok());
    BindMemoryAccountingShard(0);
  }
  void TearDown() override {
    BindMemoryAccountingShard(previous_shard_ == 0 ? kMaxMemoryWorkers
                                                   : previous_shard_ - 1);
  }
  unsigned previous_shard_ = 0;
};

storage::CompactReadResult View(const std::string& bytes, std::size_t count) {
  return std::optional<storage::CompactValueView>{
      {.encoded_ = bytes, .logical_size_ = count}};
}

TEST_F(CompactHashReplyTest, MatchesOwnedProjectionAndOwnsReplyBytes) {
  for (const auto version : {RespVersion::k2, RespVersion::k3}) {
    // Exercise both sides of each inline projection boundary, as well as
    // binary/empty components and an indivisible value larger than a page.
    for (const std::size_t count : {1, 8, 16, 17, 32, 33}) {
      storage::HashValue hash;
      for (std::size_t i = 0; i < count; ++i)
        hash.entries_.push_back(
            {.field_ = i == 0 ? "" : std::string("f\0", 2) + std::to_string(i),
             .value_ =
                 i % 3 == 0 ? "" : std::string(i == 1 ? 9000 : 128, '\xff')});
      auto encoded = storage::EncodeHashValue(hash);
      ASSERT_TRUE(encoded.ok()) << encoded.status();
      for (const auto kind :
           {CommandKind::kHGetAll, CommandKind::kHKeys, CommandKind::kHVals}) {
        auto source = *encoded;
        auto input = View(source, count);
        storage::HashOperation op{
            .kind_ = kind == CommandKind::kHGetAll
                         ? storage::HashOperationKind::kGetAll
                     : kind == CommandKind::kHKeys
                         ? storage::HashOperationKind::kKeys
                         : storage::HashOperationKind::kValues,
            .packed_output_ = true};
        auto owned =
            storage::ReadCompactHash(input, op, storage::ValueType::kHash);
        ASSERT_TRUE(owned.ok()) << owned.status();
        ReplyBuilder expected(version);
        if (kind == CommandKind::kHGetAll)
          expected.AppendMapHeader(count);
        else
          expected.AppendArrayHeader(count);
        expected.AppendBulkStrings(
            std::views::iota(std::size_t{0}, owned->value_count()),
            [&owned](std::size_t i) noexcept { return owned->value_view(i); });
        ReplyBuilder actual(version);
        auto reply = TryExecuteCompactHashRead(kind, input, actual);
        ASSERT_TRUE(reply.has_value());
        source.assign(source.size(), 'x');
        EXPECT_EQ(reply->encoded_, expected.View());
        EXPECT_EQ(actual.View(), expected.View());
      }
    }
  }
}

TEST_F(CompactHashReplyTest, MissingKeysKeepProtocolContainerShape) {
  for (const auto version : {RespVersion::k2, RespVersion::k3}) {
    for (const auto kind :
         {CommandKind::kHGetAll, CommandKind::kHKeys, CommandKind::kHVals}) {
      ReplyBuilder builder(version);
      auto reply = TryExecuteCompactHashRead(kind, std::nullopt, builder);
      ASSERT_TRUE(reply);
      EXPECT_EQ(reply->encoded_,
                kind == CommandKind::kHGetAll && version == RespVersion::k3
                    ? "%0\r\n"
                    : "*0\r\n");
    }
  }
}

TEST_F(CompactHashReplyTest, RejectsMalformedTailBeforeWritingContainerHeader) {
  storage::HashValue hash;
  hash.entries_ = {{.field_ = "first", .value_ = "one"},
                   {.field_ = "last", .value_ = "two"}};
  auto encoded = storage::EncodeHashValue(hash);
  ASSERT_TRUE(encoded.ok());
  auto broken = *encoded;
  // Corrupt the second field's declared length while retaining a valid outer
  // envelope. HKEYS must also reject a malformed value it would not emit.
  broken[storage::kHashValueHeaderBytes + 8 + 5 + 3] = '\xff';
  for (const auto kind :
       {CommandKind::kHGetAll, CommandKind::kHKeys, CommandKind::kHVals}) {
    for (const auto& source : {broken, *encoded + "x"}) {
      ReplyBuilder builder;
      auto reply = TryExecuteCompactHashRead(kind, View(source, 2), builder);
      ASSERT_TRUE(reply);
      EXPECT_TRUE(reply->encoded_.starts_with("-ERR ")) << reply->encoded_;
    }
    ReplyBuilder builder;
    auto reply = TryExecuteCompactHashRead(kind, View(*encoded, 1), builder);
    ASSERT_TRUE(reply);
    EXPECT_TRUE(reply->encoded_.starts_with("-ERR ")) << reply->encoded_;
  }
}

TEST_F(CompactHashReplyTest,
       PreservesStorageErrorsAndLeavesOtherCommandsAlone) {
  for (const auto& status :
       {absl::InternalError("read failed"),
        absl::ResourceExhaustedError("OOM read buffer"),
        absl::InvalidArgumentError("WRONGTYPE expected Hash")}) {
    ReplyBuilder builder;
    auto reply =
        TryExecuteCompactHashRead(CommandKind::kHGetAll, status, builder);
    ASSERT_TRUE(reply);
    EXPECT_EQ(reply->encoded_,
              status.message().starts_with("OOM ") ||
                      status.message().starts_with("WRONGTYPE ")
                  ? "-" + std::string(status.message()) + "\r\n"
                  : "-ERR " + std::string(status.message()) + "\r\n");
  }
  ReplyBuilder builder;
  builder.AppendSimpleString("prefix");
  EXPECT_FALSE(TryExecuteCompactHashRead(
      CommandKind::kHSet, absl::InternalError("must not consume"), builder));
  EXPECT_EQ(builder.View(), "+prefix\r\n");
}

}  // namespace
}  // namespace lavik
