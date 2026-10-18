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

#include "lavik/resp.h"

#include <gtest/gtest.h>

#include <array>
#include <cstring>
#include <forward_list>
#include <limits>
#include <ranges>
#include <string>
#include <string_view>
#include <vector>

#include "lavik/memory.h"

namespace lavik {
namespace {

TEST(RespParserTest, AcceptsMoreThan1024ArrayElements) {
  constexpr std::size_t kArgumentCount = 1025;
  std::string request = "*" + std::to_string(kArgumentCount) + "\r\n";
  for (std::size_t i = 0; i < kArgumentCount; ++i) {
    request.append("$0\r\n\r\n");
  }

  RespParseResult result = ParseRespCommand(request);

  EXPECT_EQ(result.state_, RespParseState::kOk);
  EXPECT_EQ(result.consumed_, request.size());
  ASSERT_EQ(result.command_.args_.size(), kArgumentCount);
  // The first element beyond the Valkey-compatible initial capacity grows the
  // index geometrically; an exact 1025-slot allocation would make subsequent
  // arguments repeatedly reallocate and move the whole vector.
  EXPECT_GE(result.command_.args_.capacity(), 2048);
  EXPECT_EQ(result.command_.args_.front(), "");
  EXPECT_EQ(result.command_.args_.back(), "");
}

TEST(RespParserTest, ParsingDoesNotConsumeMaxmemoryAdmission) {
  ASSERT_TRUE(InitMemoryLimit(/*configured_max_bytes=*/1,
                              /*worker_count=*/1)
                  .ok());
  BindMemoryAccountingShard(0);
  std::string request = "*1\r\n$48000\r\n";
  request.append(48000, 'x');
  request.append("\r\n");

  RespCommandParser parser;
  RespParseResult parsed = parser.Parse(request);
  ASSERT_EQ(parsed.state_, RespParseState::kOk);
  ASSERT_EQ(parsed.command_.args_.size(), 1);
  EXPECT_EQ(parsed.command_.args_[0].size(), 48000);

  // Restore fallback accounting for unrelated tests on this runner thread.
  ASSERT_TRUE(InitMemoryLimit(1024ULL * 1024 * 1024, 1).ok());
  BindMemoryAccountingShard(1);
}

TEST(RespParserTest, DoesNotAllocateDeclaredBulkBeforePayloadArrives) {
  ASSERT_TRUE(InitMemoryLimit(1024ULL * 1024 * 1024, 1).ok());
  BindMemoryAccountingShard(0);
  const std::int64_t before = WorkerMemoryAccountingBytes(0);

  RespCommandParser parser;
  RespParseResult parsed = parser.Parse("*1\r\n$536870912\r\n");

  EXPECT_EQ(parsed.state_, RespParseState::kNeedMoreData);
  const std::int64_t after = WorkerMemoryAccountingBytes(0);
  ASSERT_GE(after, before);
  EXPECT_LT(after - before, 1024 * 1024);
  BindMemoryAccountingShard(1);
}

TEST(RespParserTest, EnforcesConfiguredQueryBufferLimitAcrossFragments) {
  const std::string request = "*2\r\n$3\r\nSET\r\n$5\r\nvalue\r\n";
  RespCommandParser parser(request.size() - 1);

  RespParseResult first = parser.Parse(request.substr(0, 12));
  ASSERT_EQ(first.state_, RespParseState::kNeedMoreData);
  RespParseResult second = parser.Parse(request.substr(12));

  EXPECT_EQ(second.state_, RespParseState::kError);
  EXPECT_EQ(second.status_.code(), absl::StatusCode::kResourceExhausted);
  EXPECT_EQ(second.status_.message(),
            "client request exceeds the query buffer limit");
}

TEST(RespParserTest, AcceptsCommandExactlyAtConfiguredQueryBufferLimit) {
  const std::string request = "*1\r\n$4\r\nPING\r\n";
  RespCommandParser parser(request.size());

  RespParseResult parsed = parser.Parse(request);

  ASSERT_EQ(parsed.state_, RespParseState::kOk) << parsed.status_.message();
  EXPECT_EQ(parsed.command_.args_, (std::vector<std::string>{"PING"}));
}

TEST(RespParserTest, AppliesLowerRuntimeLimitToIncompleteCommand) {
  RespCommandParser parser(1024);
  RespParseResult partial = parser.Parse("*1\r\n$20\r\n123456789012345");
  ASSERT_EQ(partial.state_, RespParseState::kNeedMoreData);

  parser.SetQueryBufferLimit(16);
  RespParseResult rejected = parser.Parse("6");

  EXPECT_EQ(rejected.state_, RespParseState::kError);
  EXPECT_EQ(rejected.status_.code(), absl::StatusCode::kResourceExhausted);
}

TEST(RespParserTest, DirectBulkReadsRequirePayloadAndValidateWindows) {
  RespCommandParser parser;
  EXPECT_EQ(parser.BulkReadReservationSize(), 0);
  EXPECT_TRUE(parser.BulkReadBuffer().empty());
  EXPECT_EQ(parser.CommitBulkRead(0).code(),
            absl::StatusCode::kInvalidArgument);
  ASSERT_EQ(parser.Parse("*1\r\n$536870912\r\n").state_,
            RespParseState::kNeedMoreData);
  EXPECT_EQ(parser.BulkReadReservationSize(), 0);
  EXPECT_EQ(parser.PrepareBulkRead(1).code(),
            absl::StatusCode::kInvalidArgument);
  ASSERT_EQ(parser.Parse("x").state_, RespParseState::kNeedMoreData);
  const auto reservation = parser.BulkReadReservationSize();
  EXPECT_EQ(reservation, 1024 * 1024);
  EXPECT_EQ(parser.PrepareBulkRead(0).code(),
            absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(parser.PrepareBulkRead(reservation + 1).code(),
            absl::StatusCode::kInvalidArgument);
  ASSERT_TRUE(parser.PrepareBulkRead(reservation).ok());
  EXPECT_EQ(parser.BulkReadReservationSize(), 0);
  EXPECT_EQ(parser.PrepareBulkRead(1).code(),
            absl::StatusCode::kInvalidArgument);
  auto window = parser.BulkReadBuffer();
  EXPECT_EQ(window.size(), 256 * 1024);
  EXPECT_EQ(parser.CommitBulkRead(window.size() + 1).code(),
            absl::StatusCode::kInvalidArgument);
  ASSERT_TRUE(parser.CommitBulkRead(0).ok());
  EXPECT_EQ(parser.BulkReadBuffer().data(), window.data());
  window[0] = 'y';
  ASSERT_TRUE(parser.CommitBulkRead(1).ok());
  EXPECT_EQ(parser.BulkReadBuffer().data(), window.data() + 1);
  parser.Reset();
  ASSERT_EQ(parser.Parse("*1\r\n$32767\r\nx").state_,
            RespParseState::kNeedMoreData);
  EXPECT_EQ(parser.BulkReadReservationSize(), 0);
}

TEST(RespParserTest, DirectBulkReadsUseBoundedWindowsForMultiMegabytePayloads) {
  std::string body(2 * 1024 * 1024 + 13, '\0');
  for (std::size_t i = 0; i < body.size(); ++i) body[i] = i % 251;
  const std::string header = "*1\r\n$" + std::to_string(body.size()) + "\r\n";
  RespCommandParser parser(header.size() + body.size() + 2);
  ASSERT_EQ(parser.Parse(header + body.substr(0, 1)).state_,
            RespParseState::kNeedMoreData);
  std::size_t position = 1, reservations = 0;
  while (position < body.size()) {
    if (parser.BulkReadBuffer().empty()) {
      const auto bytes = parser.BulkReadReservationSize();
      ASSERT_GT(bytes, 0);
      ASSERT_LE(bytes, 1024 * 1024);
      ASSERT_TRUE(parser.PrepareBulkRead(bytes).ok());
      ++reservations;
    }
    const auto window = parser.BulkReadBuffer();
    ASSERT_FALSE(window.empty());
    ASSERT_LE(window.size(), 256 * 1024);
    ASSERT_LE(window.size(), body.size() - position);
    std::memcpy(window.data(), body.data() + position, window.size());
    ASSERT_TRUE(parser.CommitBulkRead(window.size()).ok());
    position += window.size();
  }
  EXPECT_EQ(reservations, 3);
  EXPECT_TRUE(parser.BulkReadBuffer().empty());
  EXPECT_EQ(parser.BulkReadReservationSize(), 0);
  EXPECT_EQ(parser.CommitBulkRead(0).code(),
            absl::StatusCode::kInvalidArgument);
  auto parsed = parser.Parse("\r\n");
  ASSERT_EQ(parsed.state_, RespParseState::kOk) << parsed.status_;
  ASSERT_EQ(parsed.command_.args_.size(), 1);
  EXPECT_EQ(parsed.command_.args_[0], body);
  EXPECT_TRUE(parser.idle());
}

TEST(RespParserTest, DirectBulkReadsAllowPartialCommitsAndParseFallback) {
  std::string body(48000, '\0');
  for (std::size_t i = 0; i < body.size(); ++i) body[i] = i % 251;
  RespCommandParser parser;
  ASSERT_EQ(parser.Parse("*1\r\n$48000\r\n" + body.substr(0, 19)).state_,
            RespParseState::kNeedMoreData);
  ASSERT_TRUE(parser.PrepareBulkRead(512).ok());
  auto window = parser.BulkReadBuffer();
  std::memcpy(window.data(), body.data() + 19, 7);
  ASSERT_TRUE(parser.CommitBulkRead(7).ok());
  ASSERT_EQ(parser.BulkReadBuffer().size(), 505);
  auto partial = parser.Parse(std::string_view(body).substr(26, 100));
  ASSERT_EQ(partial.state_, RespParseState::kNeedMoreData);
  EXPECT_TRUE(partial.command_.args_.empty());
  EXPECT_EQ(parser.BulkReadBuffer().data(), window.data() + 107);
  EXPECT_EQ(parser.BulkReadBuffer().size(), 405);
  // The same call fills the remaining prepared tail, then appends beyond it.
  auto parsed = parser.Parse(body.substr(126) + "\r\n");
  ASSERT_EQ(parsed.state_, RespParseState::kOk) << parsed.status_;
  ASSERT_EQ(parsed.command_.args_.size(), 1);
  EXPECT_EQ(parsed.command_.args_[0], body);
  EXPECT_TRUE(parser.BulkReadBuffer().empty());
}

TEST(RespParserTest, DirectBulkReadsPreserveMultipleArgumentsAndPipelining) {
  RespCommandParser parser;
  const std::string first(32768, 'a'),
      second = "txt:" + std::string(48000, 'b');
  ASSERT_EQ(parser.Parse("*2\r\n$32768\r\na").state_,
            RespParseState::kNeedMoreData);
  for (const auto* body : {&first, &second}) {
    ASSERT_EQ(parser.BulkReadReservationSize(), body->size() - 1);
    ASSERT_TRUE(parser.PrepareBulkRead(body->size() - 1).ok());
    auto window = parser.BulkReadBuffer();
    ASSERT_EQ(window.size(), body->size() - 1);
    std::memcpy(window.data(), body->data() + 1, window.size());
    ASSERT_TRUE(parser.CommitBulkRead(window.size()).ok());
    ASSERT_EQ(parser.Parse("\r").state_, RespParseState::kNeedMoreData);
    if (body == &first)
      ASSERT_EQ(parser.Parse("\n=48004\r\nt").state_,
                RespParseState::kNeedMoreData);
  }
  const std::string pipeline = "\n*1\r\n$4\r\nPING\r\n";
  auto parsed = parser.Parse(pipeline);
  ASSERT_EQ(parsed.state_, RespParseState::kOk) << parsed.status_;
  EXPECT_EQ(parsed.consumed_, 1);
  EXPECT_EQ(parsed.command_.args_,
            (std::vector<std::string>{first, second.substr(4)}));
  auto next = parser.Parse(std::string_view(pipeline).substr(parsed.consumed_));
  ASSERT_EQ(next.state_, RespParseState::kOk);
  EXPECT_EQ(next.command_.args_, (std::vector<std::string>{"PING"}));
}

TEST(RespParserTest, DirectBulkReadsHonorQueryLimitsAtPreparationAndCommit) {
  const std::string prefix = "*1\r\n$48000\r\nx";
  RespCommandParser parser(prefix.size() + 10);
  ASSERT_EQ(parser.Parse(prefix).state_, RespParseState::kNeedMoreData);
  EXPECT_EQ(parser.BulkReadReservationSize(), 10);
  EXPECT_EQ(parser.PrepareBulkRead(11).code(),
            absl::StatusCode::kInvalidArgument);
  ASSERT_TRUE(parser.PrepareBulkRead(10).ok());
  auto window = parser.BulkReadBuffer();
  std::memset(window.data(), 'y', window.size());
  ASSERT_TRUE(parser.CommitBulkRead(3).ok());
  parser.SetQueryBufferLimit(prefix.size() + 5);
  // A runtime change preserves the prepaid storage across a suspended read.
  EXPECT_EQ(parser.BulkReadBuffer().size(), 7);
  EXPECT_EQ(parser.CommitBulkRead(3).code(),
            absl::StatusCode::kResourceExhausted);
  EXPECT_EQ(parser.BulkReadBuffer().data(), window.data() + 3);
  ASSERT_TRUE(parser.CommitBulkRead(2).ok());
  parser.SetQueryBufferLimit(prefix.size() + 4);
  EXPECT_EQ(parser.CommitBulkRead(1).code(),
            absl::StatusCode::kResourceExhausted);
  EXPECT_EQ(parser.BulkReadBuffer().data(), window.data() + 5);
  auto fallback = parser.Parse("z");
  EXPECT_EQ(fallback.state_, RespParseState::kError);
  EXPECT_EQ(fallback.status_.code(), absl::StatusCode::kResourceExhausted);
}

TEST(RespParserTest, DirectBulkResetDiscardsUnwrittenBody) {
  RespCommandParser parser;
  ASSERT_EQ(parser.Parse("*1\r\n$48000\r\nx").state_,
            RespParseState::kNeedMoreData);
  ASSERT_TRUE(parser.PrepareBulkRead(47999).ok());
  parser.BulkReadBuffer()[0] = 'y';
  ASSERT_TRUE(parser.CommitBulkRead(1).ok());
  parser.Reset();
  EXPECT_TRUE(parser.idle());
  EXPECT_EQ(parser.BulkReadReservationSize(), 0);
  EXPECT_TRUE(parser.BulkReadBuffer().empty());
  EXPECT_EQ(parser.CommitBulkRead(1).code(),
            absl::StatusCode::kInvalidArgument);
  auto parsed = parser.Parse("*1\r\n$4\r\nPING\r\n");
  ASSERT_EQ(parsed.state_, RespParseState::kOk);
  EXPECT_EQ(parsed.command_.args_, (std::vector<std::string>{"PING"}));
}

TEST(RespParserTest, DirectBulkReadsRetainTerminatorAndVerbatimValidation) {
  for (const char type : {'$', '='}) {
    RespCommandParser parser;
    ASSERT_EQ(
        parser.Parse("*1\r\n" + std::string(1, type) + "48000\r\nx").state_,
        RespParseState::kNeedMoreData);
    ASSERT_TRUE(parser.PrepareBulkRead(47999).ok());
    auto window = parser.BulkReadBuffer();
    std::memset(window.data(), 'x', window.size());
    ASSERT_TRUE(parser.CommitBulkRead(window.size()).ok());
    const auto parsed = parser.Parse(type == '$' ? "\rx" : "\r\n");
    EXPECT_EQ(parsed.state_, RespParseState::kError);
    EXPECT_EQ(parsed.status_.code(), absl::StatusCode::kInvalidArgument);
    EXPECT_TRUE(parsed.command_.args_.empty());
  }
}

TEST(RespParserTest, RejectsArrayLengthsAboveValkeyLimit) {
  const auto too_many =
      static_cast<unsigned long long>(std::numeric_limits<int>::max()) + 1;
  const std::string request = "*" + std::to_string(too_many) + "\r\n";

  RespParseResult result = ParseRespCommand(request);

  EXPECT_EQ(result.state_, RespParseState::kError);
  EXPECT_EQ(result.status_.code(), absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(result.status_.message(), "invalid RESP array length");
}

TEST(RespParserTest, DoesNotEagerlyAllocateDeclaredArrayLength) {
  const std::string request =
      "*" + std::to_string(std::numeric_limits<int>::max()) + "\r\n";

  RespParseResult result = ParseRespCommand(request);

  EXPECT_EQ(result.state_, RespParseState::kNeedMoreData);
  EXPECT_TRUE(result.command_.args_.empty());
}

TEST(RespParserTest, IncrementallyParsesEveryByteExactlyOnce) {
  const std::string request =
      "*4\r\n$3\r\nSET\r\n$3\r\nkey\r\n$11\r\nhello world\r\n$2\r\nNX\r\n";
  RespCommandParser parser;
  std::string pending;
  RespParseResult completed;
  std::size_t largest_pending = 0;

  for (char byte : request) {
    pending.push_back(byte);
    while (!pending.empty()) {
      RespParseResult parsed = parser.Parse(pending);
      ASSERT_NE(parsed.state_, RespParseState::kError)
          << parsed.status_.message();
      pending.erase(0, parsed.consumed_);
      largest_pending = std::max(largest_pending, pending.size());
      if (parsed.state_ == RespParseState::kOk) {
        completed = std::move(parsed);
        break;
      }
      if (parsed.consumed_ == 0) break;
    }
  }

  EXPECT_TRUE(pending.empty());
  EXPECT_TRUE(parser.idle());
  EXPECT_LE(largest_pending, 1);
  EXPECT_EQ(completed.state_, RespParseState::kOk);
  EXPECT_EQ(completed.command_.args_,
            (std::vector<std::string>{"SET", "key", "hello world", "NX"}));
}

TEST(RespParserTest, ReturnsPipelinedCommandsInWireOrder) {
  const std::string first = "*1\r\n$4\r\nPING\r\n";
  const std::string second = "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
  RespCommandParser parser;

  RespParseResult one = parser.Parse(first + second);
  ASSERT_EQ(one.state_, RespParseState::kOk);
  EXPECT_EQ(one.command_.args_, (std::vector<std::string>{"PING"}));

  RespParseResult two =
      parser.Parse(std::string_view(first + second).substr(one.consumed_));
  ASSERT_EQ(two.state_, RespParseState::kOk);
  EXPECT_EQ(two.command_.args_, (std::vector<std::string>{"GET", "key"}));
  EXPECT_TRUE(parser.idle());
}

TEST(RespParserTest, ConsumesFillerAndEmptyArraysAcrossFragments) {
  RespCommandParser parser;
  RespParseResult skipped = parser.Parse("\r\n*0\r");
  EXPECT_EQ(skipped.state_, RespParseState::kNeedMoreData);
  EXPECT_EQ(skipped.consumed_, 4);

  RespParseResult parsed = parser.Parse("\r\n*1\r\n$4\r\nPING\r\n");
  ASSERT_EQ(parsed.state_, RespParseState::kOk);
  EXPECT_EQ(parsed.command_.args_, (std::vector<std::string>{"PING"}));
}

TEST(RespParserTest, RejectsSplitMalformedBulkTerminator) {
  RespCommandParser parser;
  RespParseResult partial = parser.Parse("*1\r\n$3\r\nGET\r");
  EXPECT_EQ(partial.state_, RespParseState::kNeedMoreData);

  RespParseResult malformed = parser.Parse("x");
  EXPECT_EQ(malformed.state_, RespParseState::kError);
  EXPECT_EQ(malformed.status_.message(),
            "malformed RESP bulk string terminator");
}

TEST(RespParserTest, AcceptsInlineCommandsQuotesAndEscapesIncrementally) {
  RespCommandParser parser;
  RespParseResult first = parser.Parse("SET inline \"hello\\x20");
  ASSERT_EQ(first.state_, RespParseState::kNeedMoreData);
  RespParseResult second = parser.Parse("world\" NX\r\n");
  ASSERT_EQ(second.state_, RespParseState::kOk) << second.status_.message();
  EXPECT_EQ(second.command_.args_,
            (std::vector<std::string>{"SET", "inline", "hello world", "NX"}));
}

TEST(RespParserTest, AcceptsResp3ScalarAndVerbatimArguments) {
  const std::string request =
      "*6\r\n+SET\r\n$3\r\nkey\r\n=9\r\ntxt:value\r\n:42\r\n"
      ",1.5\r\n#t\r\n";
  RespParseResult parsed = ParseRespCommand(request);
  ASSERT_EQ(parsed.state_, RespParseState::kOk) << parsed.status_.message();
  EXPECT_EQ(
      parsed.command_.args_,
      (std::vector<std::string>{"SET", "key", "value", "42", "1.5", "1"}));
}

TEST(ReplyBuilderTest, EncodesScalarAndCompositeReplies) {
  ReplyBuilder builder;

  EXPECT_EQ(builder.AppendSimpleString("OK"), "+OK\r\n");
  builder.Reset();
  EXPECT_EQ(builder.AppendInteger(-42), ":-42\r\n");
  builder.Reset();
  EXPECT_EQ(builder.AppendError("ERR failed"), "-ERR failed\r\n");
  builder.Reset();
  EXPECT_EQ(builder.AppendError("ERR ", "failed"), "-ERR failed\r\n");
  builder.Reset();

  const char binary[] = {'a', '\0', 'b'};
  EXPECT_EQ(builder.AppendBulkString(std::string_view(binary, sizeof(binary))),
            std::string_view("$3\r\na\0b\r\n", 9));

  builder.Reset();
  builder.AppendArrayHeader(2);
  builder.AppendBulkString("first");
  builder.AppendNullBulkString();
  EXPECT_EQ(builder.View(), "*2\r\n$5\r\nfirst\r\n$-1\r\n");
}

TEST(ReplyBuilderTest, ReusesBoundedCapacityAndReleasesOversizedBuffer) {
  ReplyBuilder builder;
  builder.Reserve(4096);
  const std::size_t retained_capacity = builder.Capacity();
  builder.AppendError("ERR failed");
  builder.Reset();
  EXPECT_EQ(builder.Capacity(), retained_capacity);

  builder.Reserve(128 * 1024);
  ASSERT_GT(builder.Capacity(), 64 * 1024);
  builder.Reset();
  EXPECT_LE(builder.Capacity(), 64 * 1024);
}

TEST(ReplyBuilderTest, BulkElementsPreservePrefixAndBinaryPayloads) {
  const std::vector<std::string> values{"", std::string("a\0b\r\n", 5), "tail"};
  ReplyBuilder builder;
  builder.AppendArrayHeader(values.size());
  const char wire[] = "*3\r\n$0\r\n\r\n$5\r\na\0b\r\n\r\n$4\r\ntail\r\n";
  const std::string expected(wire, sizeof(wire) - 1);
  EXPECT_EQ(builder.AppendBulkStrings(values), expected);
  builder.AppendInteger(7);
  EXPECT_EQ(builder.View(), expected + ":7\r\n");
  EXPECT_EQ(values[1], std::string("a\0b\r\n", 5));
}

TEST(ReplyBuilderTest, BulkElementsDistinguishNullFromEmptyInBothProtocols) {
  const std::vector<std::optional<std::string>> values{
      std::nullopt, std::string{}, "value", std::nullopt};
  ReplyBuilder builder;
  builder.AppendMapHeader(2);
  EXPECT_EQ(builder.AppendBulkStrings(values),
            "*4\r\n$-1\r\n$0\r\n\r\n$5\r\nvalue\r\n$-1\r\n");
  builder.Reset();
  builder.SetVersion(RespVersion::k3);
  builder.AppendMapHeader(2);
  EXPECT_EQ(builder.AppendBulkStrings(values),
            "%2\r\n_\r\n$0\r\n\r\n$5\r\nvalue\r\n_\r\n");
}

TEST(ReplyBuilderTest, BulkViewsPreserveEmptyAndBorrowedBinaryValues) {
  const std::string payload("a\0b\r\n", 5);
  const std::array<std::string_view, 3> values{std::string_view{}, payload,
                                               "tail"};
  for (const auto version : {RespVersion::k2, RespVersion::k3}) {
    ReplyBuilder expected(version);
    ReplyBuilder builder(version);
    expected.AppendArrayHeader(values.size());
    builder.AppendArrayHeader(values.size());
    for (const auto value : values) expected.AppendBulkString(value);
    EXPECT_EQ(builder.AppendBulkStrings(values), expected.View());
    EXPECT_EQ(builder.AppendBulkStrings(std::span<const std::string_view>{}),
              expected.View());
  }
}

TEST(ReplyBuilderTest, ProjectedBulkElementsAcceptOwnedAndBorrowedMembers) {
  struct OwnedMember {
    std::string member_;
    OwnedMember(const OwnedMember&) = delete;
    OwnedMember& operator=(const OwnedMember&) = delete;
    explicit OwnedMember(std::string member) : member_(std::move(member)) {}
  };
  const std::array<OwnedMember, 3> members{OwnedMember(""),
                                           OwnedMember(std::string("a\0b", 3)),
                                           OwnedMember(std::string(4096, 'x'))};
  ReplyBuilder expected;
  ReplyBuilder builder;
  expected.AppendArrayHeader(members.size());
  builder.AppendArrayHeader(members.size());
  for (const auto& member : members) expected.AppendBulkString(member.member_);
  EXPECT_EQ(builder.AppendBulkStrings(members, &OwnedMember::member_),
            expected.View());

  // Non-contiguous forward ranges use the same encoder without materializing
  // a span or taking ownership of their source bytes.
  const std::forward_list<std::string_view> views{
      members[0].member_, members[1].member_, members[2].member_};
  builder.Reset();
  builder.Reserve(expected.View().size());
  builder.AppendArrayHeader(members.size());
  const auto* data = builder.View().data();
  std::size_t projections = 0;
  EXPECT_EQ(builder.AppendBulkStrings(views,
                                      [&](std::string_view value) noexcept {
                                        ++projections;
                                        return value;
                                      }),
            expected.View());
  EXPECT_EQ(projections, 2 * members.size());
  EXPECT_EQ(builder.View().data(), data);
}

TEST(ReplyBuilderTest, IndexedProjectionPreservesNullableBinaryViews) {
  const std::string binary("a\0b\r\n", 5);
  const std::array<std::optional<std::string_view>, 5> values{
      std::nullopt, std::string_view{}, binary, "tail", std::nullopt};
  const char resp2[] =
      "*5\r\n$-1\r\n$0\r\n\r\n$5\r\na\0b\r\n\r\n$4\r\ntail\r\n$-1\r\n";
  const char resp3[] =
      "*5\r\n_\r\n$0\r\n\r\n$5\r\na\0b\r\n\r\n$4\r\ntail\r\n_\r\n";
  for (const auto version : {RespVersion::k2, RespVersion::k3}) {
    ReplyBuilder builder(version);
    builder.AppendArrayHeader(values.size());
    std::size_t projections = 0;
    const auto project = [&](std::size_t index) noexcept {
      ++projections;
      return values[index];
    };
    const auto expected = version == RespVersion::k3
                              ? std::string_view(resp3, sizeof(resp3) - 1)
                              : std::string_view(resp2, sizeof(resp2) - 1);
    EXPECT_EQ(builder.AppendBulkStrings(
                  std::views::iota(std::size_t{0}, values.size()), project),
              expected);
    EXPECT_EQ(projections, 2 * values.size());
    const auto* data = builder.View().data();
    const auto capacity = builder.Capacity();
    EXPECT_EQ(builder.AppendBulkStrings(
                  std::views::iota(std::size_t{0}, std::size_t{0}), project),
              expected);
    EXPECT_EQ(projections, 2 * values.size());
    EXPECT_EQ(builder.View().data(), data);
    EXPECT_EQ(builder.Capacity(), capacity);
  }
}

TEST(ReplyBuilderTest, EmptyBulkElementsLeaveReplyAndCapacityUnchanged) {
  ReplyBuilder builder;
  builder.Reserve(4096);
  builder.AppendArrayHeader(0);
  const auto* data = builder.View().data();
  const auto capacity = builder.Capacity();
  EXPECT_EQ(builder.AppendBulkStrings(std::span<const std::string>{}),
            "*0\r\n");
  EXPECT_EQ(
      builder.AppendBulkStrings(std::span<const std::optional<std::string>>{}),
      "*0\r\n");
  EXPECT_EQ(builder.View().data(), data);
  EXPECT_EQ(builder.Capacity(), capacity);
}

TEST(ReplyBuilderTest, BulkElementsHandleDecimalBoundariesAndRetainedCapacity) {
  std::vector<std::string> values;
  ReplyBuilder expected;
  for (const std::size_t length : {0, 1, 9, 10, 99, 100, 999, 1000}) {
    values.emplace_back(length, static_cast<char>('a' + values.size()));
    expected.AppendBulkString(values.back());
  }
  ReplyBuilder builder;
  builder.Reserve(expected.View().size() + 32);
  builder.AppendArrayHeader(values.size());
  const auto* data = builder.View().data();
  const auto capacity = builder.Capacity();
  EXPECT_EQ(builder.AppendBulkStrings(values),
            std::string("*8\r\n") + std::string(expected.View()));
  EXPECT_EQ(builder.View().data(), data);
  EXPECT_EQ(builder.Capacity(), capacity);
}

TEST(ReplyBuilderTest, BulkElementsGrowForLargeRepliesAndAppendSeveralBatches) {
  std::string binary(256, '\0');
  for (std::size_t i = 0; i < binary.size(); ++i)
    binary[i] = static_cast<char>(i);
  const std::vector<std::string> values(4096, binary);
  ReplyBuilder expected;
  expected.AppendArrayHeader(values.size());
  for (const auto& value : values) expected.AppendBulkString(value);
  ReplyBuilder builder;
  builder.AppendArrayHeader(values.size());
  const auto split = std::span<const std::string>(values);
  builder.AppendBulkStrings(split.first(17));
  builder.AppendBulkStrings(split.subspan(17));
  EXPECT_EQ(builder.View(), expected.View());
  ASSERT_GT(builder.Capacity(), 64 * 1024);
  builder.Reset();
  EXPECT_TRUE(builder.View().empty());
  EXPECT_LE(builder.Capacity(), 64 * 1024);
}

TEST(ReplyBuilderTest, EncodesProtocolSpecificResp3Types) {
  ReplyBuilder builder(RespVersion::k3);
  builder.AppendNull();
  builder.AppendMapHeader(1);
  builder.AppendBulkString("key");
  builder.AppendBulkString("value");
  builder.AppendSetHeader(1);
  builder.AppendBulkString("member");
  builder.AppendPushHeader(2);
  builder.AppendBulkString("message");
  builder.AppendBulkString("payload");
  builder.AppendBoolean(true);
  builder.AppendDouble(1.5);
  builder.AppendDoubleText("2.50");
  builder.AppendBigNumber("12345678901234567890");
  builder.AppendVerbatimString("txt", "hello");
  EXPECT_EQ(builder.View(),
            "_\r\n%1\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
            "~1\r\n$6\r\nmember\r\n"
            ">2\r\n$7\r\nmessage\r\n$7\r\npayload\r\n"
            "#t\r\n,1.5\r\n,2.50\r\n"
            "(12345678901234567890\r\n=9\r\ntxt:hello\r\n");
}

TEST(ReplyBuilderTest, DegradesSemanticTypesToResp2) {
  ReplyBuilder builder;
  builder.AppendNull();
  builder.AppendMapHeader(1);
  builder.AppendSetHeader(2);
  builder.AppendPushHeader(3);
  builder.AppendBoolean(false);
  builder.AppendDouble(1.5);
  builder.AppendBigNumber("12345678901234567890");
  builder.AppendVerbatimString("txt", "hello");
  EXPECT_EQ(builder.View(),
            "$-1\r\n*2\r\n*2\r\n*3\r\n:0\r\n$3\r\n1.5\r\n"
            "$20\r\n12345678901234567890\r\n$5\r\nhello\r\n");
}

// The cluster error helpers emit Redis 7.2's texts verbatim: clients dispatch
// on the first token (MOVED/CROSSSLOT/CLUSTERDOWN/TRYAGAIN), so a drifted
// message is a protocol bug, not a cosmetic one.
TEST(ClusterErrorTest, MovedErrorCarriesSlotAndConcreteEndpoint) {
  ReplyBuilder builder;
  EXPECT_EQ(AppendMovedError(builder, 3998, "10.0.0.2", 6380),
            "-MOVED 3998 10.0.0.2:6380\r\n");
  builder.Reset();
  EXPECT_EQ(AppendMovedError(builder, 0, "192.168.1.1", 0),
            "-MOVED 0 192.168.1.1:0\r\n");
}

TEST(ClusterErrorTest, CrossSlotErrorIsVerbatim) {
  ReplyBuilder builder;
  EXPECT_EQ(AppendCrossSlotError(builder),
            "-CROSSSLOT Keys in request don't hash to the same slot\r\n");
}

TEST(ClusterErrorTest, ClusterDownUnboundErrorIsVerbatim) {
  ReplyBuilder builder;
  EXPECT_EQ(AppendClusterDownUnboundError(builder),
            "-CLUSTERDOWN Hash slot not served\r\n");
}

TEST(ClusterErrorTest, TryAgainErrorWrapsMessage) {
  ReplyBuilder builder;
  EXPECT_EQ(AppendTryAgainError(builder, "database flush is in progress"),
            "-TRYAGAIN database flush is in progress\r\n");
}

// The free-standing message builders feed paths without a ReplyBuilder (the
// blocking wait loop); both spellings must agree with the builder forms above.
TEST(ClusterErrorTest, MessageBuildersMatchBuilderForms) {
  EXPECT_EQ(ClusterMovedMessage(42, "10.0.0.9", 7379),
            "MOVED 42 10.0.0.9:7379");
  EXPECT_EQ(kClusterCrossSlotMessage,
            "CROSSSLOT Keys in request don't hash to the same slot");
  EXPECT_EQ(kClusterDownUnboundMessage, "CLUSTERDOWN Hash slot not served");
  EXPECT_EQ(ClusterTryAgainMessage("x"), "TRYAGAIN x");
  ReplyBuilder builder(RespVersion::k3);
  // Cluster errors are simple error lines with no RESP2/RESP3 divergence.
  EXPECT_EQ(AppendMovedError(builder, 42, "10.0.0.9", 7379),
            "-MOVED 42 10.0.0.9:7379\r\n");
  builder.Reset();
  EXPECT_EQ(AppendCrossSlotError(builder),
            "-CROSSSLOT Keys in request don't hash to the same slot\r\n");
}

}  // namespace
}  // namespace lavik
