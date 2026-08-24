#include "keylane/resp.h"

#include <gtest/gtest.h>

#include <limits>
#include <string>
#include <string_view>
#include <vector>

namespace keylane {
namespace {

TEST(RespParserTest, AcceptsMoreThan1024ArrayElements) {
  constexpr std::size_t kArgumentCount = 2048;
  std::string request = "*" + std::to_string(kArgumentCount) + "\r\n";
  for (std::size_t i = 0; i < kArgumentCount; ++i) {
    request.append("$0\r\n\r\n");
  }

  RespParseResult result = ParseRespCommand(request);

  EXPECT_EQ(result.state_, RespParseState::kOk);
  EXPECT_EQ(result.consumed_, request.size());
  ASSERT_EQ(result.command_.args_.size(), kArgumentCount);
  EXPECT_EQ(result.command_.args_.front(), "");
  EXPECT_EQ(result.command_.args_.back(), "");
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
  EXPECT_EQ(parsed.command_.args_,
            (std::vector<std::string>{"SET", "key", "value", "42", "1.5",
                                      "1"}));
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
  EXPECT_EQ(builder.View(),
            "_\r\n%1\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
            "~1\r\n$6\r\nmember\r\n"
            ">2\r\n$7\r\nmessage\r\n$7\r\npayload\r\n"
            "#t\r\n,1.5\r\n,2.50\r\n");
}

TEST(ReplyBuilderTest, DegradesSemanticTypesToResp2) {
  ReplyBuilder builder;
  builder.AppendNull();
  builder.AppendMapHeader(1);
  builder.AppendSetHeader(2);
  builder.AppendPushHeader(3);
  builder.AppendBoolean(false);
  builder.AppendDouble(1.5);
  EXPECT_EQ(builder.View(),
            "$-1\r\n*2\r\n*2\r\n*3\r\n:0\r\n$3\r\n1.5\r\n");
}

}  // namespace
}  // namespace keylane
