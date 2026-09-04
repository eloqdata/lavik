#include "keylane/resp.h"

#include <gtest/gtest.h>

#include <limits>
#include <string>
#include <string_view>
#include <vector>

#include "keylane/memory.h"

namespace keylane {
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
}  // namespace keylane
