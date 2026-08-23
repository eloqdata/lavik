#include "keylane/resp.h"

#include <algorithm>
#include <cassert>
#include <charconv>
#include <limits>
#include <string_view>

namespace keylane {
using namespace celer;

namespace {

// Valkey accepts multibulk lengths up to INT_MAX. Its parser uses 1024 only
// as the initial argv capacity and grows the array as arguments arrive; it is
// not a protocol limit.
constexpr long long kMaxArrayLen = std::numeric_limits<int>::max();
constexpr std::size_t kInitialArgCapacity = 1024;
constexpr std::size_t kMaxBulkLen = 512ULL * 1024 * 1024;
constexpr std::size_t kMaxCommandBytes = 1ULL * 1024 * 1024 * 1024;
constexpr std::size_t kMaxLengthTextBytes = 32;

}  // namespace

void RespCommandParser::ResetCommand() {
  state_ = State::kArrayStart;
  length_text_.clear();
  command_ = RespCommand{};
  current_argument_.clear();
  arguments_remaining_ = 0;
  bulk_remaining_ = 0;
  terminator_bytes_ = 0;
  command_bytes_ = 0;
}

void RespCommandParser::Reset() { ResetCommand(); }

bool RespCommandParser::Account(std::size_t bytes) {
  if (bytes > kMaxCommandBytes - command_bytes_) return false;
  command_bytes_ += bytes;
  return true;
}

RespParseResult RespCommandParser::Error(absl::Status status,
                                         std::size_t consumed) {
  RespParseResult result;
  result.state_ = RespParseState::kError;
  result.consumed_ = consumed;
  result.status_ = std::move(status);
  return result;
}

RespParseResult RespCommandParser::Parse(std::string_view input) {
  RespParseResult result;
  std::size_t pos = 0;

  const auto consume = [&](std::size_t bytes) {
    pos += bytes;
    return state_ == State::kArrayStart || Account(bytes);
  };

  while (pos < input.size()) {
    switch (state_) {
      case State::kArrayStart: {
        while (pos < input.size() &&
               (input[pos] == '\r' || input[pos] == '\n')) {
          ++pos;
        }
        if (pos == input.size()) break;
        if (input[pos] != '*') {
          return Error(absl::InvalidArgumentError("expected RESP array"), pos);
        }
        command_bytes_ = 1;
        ++pos;
        state_ = State::kArrayLength;
        break;
      }

      case State::kArrayLength:
      case State::kBulkLength: {
        const bool array_length = state_ == State::kArrayLength;
        while (pos < input.size()) {
          const char value = input[pos];
          if (value == '\r') {
            if (pos + 1 == input.size()) {
              result.consumed_ = pos;
              return result;
            }
            if (input[pos + 1] != '\n') {
              return Error(
                  absl::InvalidArgumentError(
                      array_length ? "invalid RESP array length"
                                   : "invalid RESP bulk string length"),
                  pos);
            }
            if (!consume(2)) {
              return Error(absl::ResourceExhaustedError(
                               "client request exceeds the query buffer limit"),
                           pos);
            }

            long long parsed = 0;
            const auto [end, error] = std::from_chars(
                length_text_.data(), length_text_.data() + length_text_.size(),
                parsed);
            if (length_text_.empty() || error != std::errc{} ||
                end != length_text_.data() + length_text_.size() ||
                parsed < 0) {
              return Error(
                  absl::InvalidArgumentError(
                      array_length ? "invalid RESP array length"
                                   : "invalid RESP bulk string length"),
                  pos);
            }
            length_text_.clear();

            if (array_length) {
              if (parsed > kMaxArrayLen) {
                return Error(
                    absl::InvalidArgumentError("invalid RESP array length"),
                    pos);
              }
              if (parsed == 0) {
                ResetCommand();
                break;
              }
              arguments_remaining_ = static_cast<std::size_t>(parsed);
              command_.args_.reserve(
                  std::min(arguments_remaining_, kInitialArgCapacity));
              state_ = State::kBulkStart;
            } else {
              if (parsed > static_cast<long long>(kMaxBulkLen)) {
                return Error(
                    absl::OutOfRangeError("RESP bulk string too large"), pos);
              }
              bulk_remaining_ = static_cast<std::size_t>(parsed);
              current_argument_.clear();
              state_ = State::kBulkData;
            }
            break;
          }
          if (value == '\n' || length_text_.size() >= kMaxLengthTextBytes) {
            return Error(absl::InvalidArgumentError(
                             array_length ? "invalid RESP array length"
                                          : "invalid RESP bulk string length"),
                         pos);
          }
          length_text_.push_back(value);
          if (!consume(1)) {
            return Error(absl::ResourceExhaustedError(
                             "client request exceeds the query buffer limit"),
                         pos);
          }
        }
        break;
      }

      case State::kBulkStart:
        if (input[pos] != '$') {
          return Error(absl::InvalidArgumentError("expected RESP bulk string"),
                       pos);
        }
        if (!consume(1)) {
          return Error(absl::ResourceExhaustedError(
                           "client request exceeds the query buffer limit"),
                       pos);
        }
        state_ = State::kBulkLength;
        break;

      case State::kBulkData: {
        const std::size_t available = input.size() - pos;
        const std::size_t take = std::min(available, bulk_remaining_);
        if (take != 0) {
          current_argument_.append(input.substr(pos, take));
          bulk_remaining_ -= take;
          if (!consume(take)) {
            return Error(absl::ResourceExhaustedError(
                             "client request exceeds the query buffer limit"),
                         pos);
          }
        }
        if (bulk_remaining_ == 0) {
          terminator_bytes_ = 0;
          state_ = State::kBulkTerminator;
        }
        break;
      }

      case State::kBulkTerminator:
        while (pos < input.size() && terminator_bytes_ < 2) {
          const char expected = terminator_bytes_ == 0 ? '\r' : '\n';
          if (input[pos] != expected) {
            return Error(absl::InvalidArgumentError(
                             "malformed RESP bulk string terminator"),
                         pos);
          }
          ++terminator_bytes_;
          if (!consume(1)) {
            return Error(absl::ResourceExhaustedError(
                             "client request exceeds the query buffer limit"),
                         pos);
          }
        }
        if (terminator_bytes_ != 2) break;

        command_.args_.push_back(std::move(current_argument_));
        current_argument_.clear();
        --arguments_remaining_;
        if (arguments_remaining_ != 0) {
          state_ = State::kBulkStart;
          break;
        }

        result.state_ = RespParseState::kOk;
        result.consumed_ = pos;
        result.command_ = std::move(command_);
        ResetCommand();
        return result;
    }
  }

  result.consumed_ = pos;
  return result;
}

RespParseResult ParseRespCommand(std::string_view input) {
  RespCommandParser parser;
  return parser.Parse(input);
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
