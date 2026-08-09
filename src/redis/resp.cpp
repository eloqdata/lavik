#include "keylane/resp.h"

#include <cassert>
#include <charconv>
#include <limits>
#include <string_view>

namespace keylane {
using namespace celer;

namespace {

constexpr std::size_t kMaxArrayLen = 1024;
constexpr std::size_t kMaxBulkLen = 512ULL * 1024 * 1024;

}  // namespace

RespParseResult ParseRespCommand(std::string_view input) {
  RespParseResult result;

  // Whatever gets skipped below is reported as consumed even when no
  // complete command follows: a stream of nothing but filler would
  // otherwise be retained forever and re-scanned from the start on every
  // refill — quadratic work for bytes that carry no command.
  std::size_t skipped = 0;
  std::size_t pos = 0;
  std::size_t count = 0;
  for (;;) {
    // An empty line is a command that does nothing, and clients rely on it:
    // redis-cli --pipe sends a bare CRLF to terminate any half-written
    // command before its final handshake. Consume it silently instead of
    // failing the connection, which would strand every command the client
    // sends afterwards.
    std::size_t leading = 0;
    while (leading < input.size() &&
           (input[leading] == '\r' || input[leading] == '\n')) {
      ++leading;
    }
    input.remove_prefix(leading);
    skipped += leading;

    if (input.empty() || input.front() != '*') {
      if (!input.empty()) {
        result.state_ = RespParseState::kError;
        result.status_ = absl::Status(absl::StatusCode::kInvalidArgument,
                                      "expected RESP array");
      }
      result.consumed_ = skipped;
      return result;
    }

    // Parse array length: *<N>\r\n
    pos = 1;
    const std::size_t crlf = input.find("\r\n", pos);
    if (crlf == std::string_view::npos) {
      result.consumed_ = skipped;
      return result;
    }

    long long array_len = 0;
    auto [ptr, ec] =
        std::from_chars(input.data() + pos, input.data() + crlf, array_len);
    if (ec != std::errc{} || ptr != input.data() + crlf || array_len < 0) {
      result.state_ = RespParseState::kError;
      result.status_ = absl::Status(absl::StatusCode::kInvalidArgument,
                                    "invalid RESP array length");
      return result;
    }
    if (array_len > static_cast<long long>(kMaxArrayLen)) {
      result.state_ = RespParseState::kError;
      result.status_ = absl::Status(absl::StatusCode::kOutOfRange,
                                    "too many RESP array elements");
      return result;
    }
    // An empty multibulk is a command that does nothing: real Redis consumes
    // it without sending anything back, and replying (even an error) would
    // shift the client's request/reply pairing off by one for the rest of
    // the connection. Every spelling of zero (*0, *000, *-0) qualifies, so
    // the decision is made on the parsed count, not the raw bytes.
    if (array_len == 0) {
      input.remove_prefix(crlf + 2);
      skipped += crlf + 2;
      continue;
    }

    count = static_cast<std::size_t>(array_len);
    pos = crlf + 2;
    break;
  }
  result.command_.args_.reserve(count);
  // From here on, a truncated command still reports the skipped prefix as
  // consumed: the filler can be dropped while the rest is awaited.
  result.consumed_ = skipped;

  // Parse each bulk string: $<N>\r\n<data>\r\n
  for (std::size_t i = 0; i < count; ++i) {
    // Running out of input between arguments is truncation, not corruption:
    // a pipelined stream splits wherever the socket happens to split it, and
    // landing exactly on an argument boundary is ordinary. Report it the same
    // way as every other short read so the caller waits for the rest instead
    // of failing the connection.
    if (pos >= input.size()) return result;
    if (input[pos] != '$') {
      result.state_ = RespParseState::kError;
      result.status_ = absl::Status(absl::StatusCode::kInvalidArgument,
                                    "expected RESP bulk string");
      return result;
    }
    ++pos;

    const std::size_t crlf = input.find("\r\n", pos);
    if (crlf == std::string_view::npos) return result;

    long long bulk_len = 0;
    auto [p2, ec2] =
        std::from_chars(input.data() + pos, input.data() + crlf, bulk_len);
    if (ec2 != std::errc{} || p2 != input.data() + crlf || bulk_len < 0) {
      result.state_ = RespParseState::kError;
      result.status_ = absl::Status(absl::StatusCode::kInvalidArgument,
                                    "invalid RESP bulk string length");
      return result;
    }
    if (bulk_len > static_cast<long long>(kMaxBulkLen)) {
      result.state_ = RespParseState::kError;
      result.status_ = absl::Status(absl::StatusCode::kOutOfRange,
                                    "RESP bulk string too large");
      return result;
    }

    const auto data_len = static_cast<std::size_t>(bulk_len);
    pos = crlf + 2;
    if (input.size() < pos + data_len + 2) return result;
    if (input[pos + data_len] != '\r' || input[pos + data_len + 1] != '\n') {
      result.state_ = RespParseState::kError;
      result.status_ = absl::Status(absl::StatusCode::kInvalidArgument,
                                    "malformed RESP bulk string terminator");
      return result;
    }

    result.command_.args_.emplace_back(input.substr(pos, data_len));
    pos += data_len + 2;
  }

  result.state_ = RespParseState::kOk;
  result.consumed_ = skipped + pos;
  return result;
}

namespace {

void AppendUnsigned(std::string& output, std::uint64_t value) {
  char digits[std::numeric_limits<std::uint64_t>::digits10 + 2];
  const auto [end, error] =
      std::to_chars(digits, digits + sizeof(digits), value);
  assert(error == std::errc{});
  output.append(digits, end);
}

}  // namespace

void ReplyBuilder::Reset() {
  constexpr std::size_t kMaximumRetainedCapacity = 64 * 1024;
  if (buffer_.capacity() > kMaximumRetainedCapacity) {
    std::string{}.swap(buffer_);
  } else {
    buffer_.clear();
  }
}

void ReplyBuilder::Reserve(std::size_t capacity) { buffer_.reserve(capacity); }

std::string_view ReplyBuilder::AppendSimpleString(std::string_view value) {
  buffer_.push_back('+');
  buffer_.append(value);
  buffer_.append("\r\n");
  return buffer_;
}

std::string_view ReplyBuilder::AppendBulkString(std::string_view value) {
  buffer_.push_back('$');
  AppendUnsigned(buffer_, value.size());
  buffer_.append("\r\n");
  buffer_.append(value);
  buffer_.append("\r\n");
  return buffer_;
}

std::string_view ReplyBuilder::AppendNullBulkString() {
  buffer_.append("$-1\r\n");
  return buffer_;
}

std::string_view ReplyBuilder::AppendInteger(long long value) {
  char digits[std::numeric_limits<long long>::digits10 + 3];
  const auto [end, error] =
      std::to_chars(digits, digits + sizeof(digits), value);
  assert(error == std::errc{});
  buffer_.push_back(':');
  buffer_.append(digits, end);
  buffer_.append("\r\n");
  return buffer_;
}

std::string_view ReplyBuilder::AppendError(std::string_view message) {
  return AppendError({}, message);
}

std::string_view ReplyBuilder::AppendError(std::string_view prefix,
                                           std::string_view message) {
  buffer_.push_back('-');
  buffer_.append(prefix);
  buffer_.append(message);
  buffer_.append("\r\n");
  return buffer_;
}

std::string_view ReplyBuilder::AppendArrayHeader(std::uint64_t count) {
  buffer_.push_back('*');
  AppendUnsigned(buffer_, count);
  buffer_.append("\r\n");
  return buffer_;
}

std::string_view ReplyBuilder::AppendRaw(std::string_view encoded) {
  buffer_.append(encoded);
  return buffer_;
}

std::string EncodeSimpleString(std::string_view value) {
  ReplyBuilder builder;
  builder.Reserve(value.size() + 3);
  builder.AppendSimpleString(value);
  return std::move(builder).Release();
}

std::string EncodeBulkString(std::string_view value) {
  ReplyBuilder builder;
  builder.Reserve(value.size() + 32);
  builder.AppendBulkString(value);
  return std::move(builder).Release();
}

std::string EncodeNullBulkString() { return "$-1\r\n"; }

std::string EncodeInteger(long long value) {
  ReplyBuilder builder;
  builder.Reserve(32);
  builder.AppendInteger(value);
  return std::move(builder).Release();
}

std::string EncodeError(std::string_view message) {
  ReplyBuilder builder;
  builder.Reserve(message.size() + 3);
  builder.AppendError(message);
  return std::move(builder).Release();
}

std::string_view EncodeScanReply(ReplyBuilder& builder, std::uint64_t cursor,
                                 const std::vector<std::string>& keys) {
  char cursor_digits[std::numeric_limits<std::uint64_t>::digits10 + 2];
  const auto [cursor_end, error] = std::to_chars(
      cursor_digits, cursor_digits + sizeof(cursor_digits), cursor);
  assert(error == std::errc{});
  builder.Reserve(builder.View().size() + 64 + keys.size() * 16);
  builder.AppendArrayHeader(2);
  builder.AppendBulkString(std::string_view(
      cursor_digits, static_cast<std::size_t>(cursor_end - cursor_digits)));
  builder.AppendArrayHeader(keys.size());
  for (const std::string& key : keys) {
    builder.AppendBulkString(key);
  }
  return builder.View();
}

std::string EncodeScanReply(std::uint64_t cursor,
                            const std::vector<std::string>& keys) {
  ReplyBuilder builder;
  EncodeScanReply(builder, cursor, keys);
  return std::move(builder).Release();
}

}  // namespace keylane
