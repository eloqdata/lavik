#include "keylane/resp.h"

#include <gtest/gtest.h>

#include <limits>
#include <string>
#include <string_view>

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

}  // namespace
}  // namespace keylane
