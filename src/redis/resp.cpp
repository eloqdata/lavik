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

absl::StatusOr<std::vector<std::string>> ParseInlineArguments(
    std::string_view line) {
  std::vector<std::string> args;
  std::size_t pos = 0;
  while (pos < line.size()) {
    while (pos < line.size() && (line[pos] == ' ' || line[pos] == '\t')) ++pos;
    if (pos == line.size()) break;
    std::string argument;
    char quote = 0;
    if (line[pos] == '\'' || line[pos] == '"') quote = line[pos++];
    bool closed = quote == 0;
    while (pos < line.size()) {
      char value = line[pos++];
      if (quote != 0 && value == quote) {
        closed = true;
        break;
      }
      if (quote == 0 && (value == ' ' || value == '\t')) break;
      if (value != '\\') {
        argument.push_back(value);
        continue;
      }
      if (pos == line.size())
        return absl::InvalidArgumentError("unterminated inline escape");
      const char escaped = line[pos++];
      switch (escaped) {
        case 'n': argument.push_back('\n'); break;
        case 'r': argument.push_back('\r'); break;
        case 't': argument.push_back('\t'); break;
        case 'b': argument.push_back('\b'); break;
        case 'a': argument.push_back('\a'); break;
        case 'x': {
          if (pos + 2 > line.size())
            return absl::InvalidArgumentError("invalid inline hex escape");
          unsigned byte = 0;
          const auto parsed = std::from_chars(line.data() + pos,
                                              line.data() + pos + 2, byte, 16);
          if (parsed.ec != std::errc{} || parsed.ptr != line.data() + pos + 2)
            return absl::InvalidArgumentError("invalid inline hex escape");
          argument.push_back(static_cast<char>(byte));
          pos += 2;
          break;
        }
        default: argument.push_back(escaped); break;
      }
    }
    if (!closed)
      return absl::InvalidArgumentError("unterminated inline quote");
    if (quote != 0 && pos < line.size() && line[pos] != ' ' && line[pos] != '\t')
      return absl::InvalidArgumentError("characters after inline quote");
    args.push_back(std::move(argument));
  }
  return args;
}

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
          command_bytes_ = 0;
          current_argument_.clear();
          state_ = State::kInline;
          break;
        }
        command_bytes_ = 1;
        ++pos;
        state_ = State::kArrayLength;
        break;
      }

      case State::kInline: {
        const std::size_t newline = input.find('\n', pos);
        const std::size_t end = newline == std::string_view::npos
                                    ? input.size()
                                    : newline + 1;
        current_argument_.append(input.substr(pos, end - pos));
        if (!consume(end - pos)) {
          return Error(absl::ResourceExhaustedError(
                           "client request exceeds the query buffer limit"),
                       pos);
        }
        if (newline == std::string_view::npos) break;
        current_argument_.pop_back();
        if (!current_argument_.empty() && current_argument_.back() == '\r')
          current_argument_.pop_back();
        auto parsed = ParseInlineArguments(current_argument_);
        if (!parsed.ok()) return Error(parsed.status(), pos);
        if (parsed->empty()) {
          ResetCommand();
          break;
        }
        result.state_ = RespParseState::kOk;
        result.consumed_ = pos;
        result.command_.args_ = std::move(*parsed);
        ResetCommand();
        return result;
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
              state_ = State::kArgumentStart;
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

      case State::kArgumentStart:
        argument_type_ = input[pos];
        if (argument_type_ != '$' && argument_type_ != '=' &&
            argument_type_ != '+' && argument_type_ != ':' &&
            argument_type_ != ',' && argument_type_ != '(' &&
            argument_type_ != '#') {
          return Error(absl::InvalidArgumentError(
                           "expected RESP string or scalar argument"),
                       pos);
        }
        if (!consume(1)) {
          return Error(absl::ResourceExhaustedError(
                           "client request exceeds the query buffer limit"),
                       pos);
        }
        current_argument_.clear();
        state_ = argument_type_ == '$' || argument_type_ == '='
                     ? State::kBulkLength
                     : State::kLineArgument;
        break;

      case State::kLineArgument: {
        const std::size_t newline = input.find('\n', pos);
        const std::size_t end = newline == std::string_view::npos
                                    ? input.size()
                                    : newline + 1;
        current_argument_.append(input.substr(pos, end - pos));
        if (!consume(end - pos)) {
          return Error(absl::ResourceExhaustedError(
                           "client request exceeds the query buffer limit"),
                       pos);
        }
        if (newline == std::string_view::npos) break;
        if (current_argument_.size() < 2 ||
            current_argument_[current_argument_.size() - 2] != '\r') {
          return Error(absl::InvalidArgumentError(
                           "malformed RESP scalar terminator"),
                       pos);
        }
        current_argument_.resize(current_argument_.size() - 2);
        if (argument_type_ == '#') {
          if (current_argument_ == "t")
            current_argument_ = "1";
          else if (current_argument_ == "f")
            current_argument_ = "0";
          else
            return Error(absl::InvalidArgumentError("invalid RESP boolean"),
                         pos);
        }
        command_.args_.push_back(std::move(current_argument_));
        current_argument_.clear();
        --arguments_remaining_;
        if (arguments_remaining_ != 0) {
          state_ = State::kArgumentStart;
          break;
        }
        result.state_ = RespParseState::kOk;
        result.consumed_ = pos;
        result.command_ = std::move(command_);
        ResetCommand();
        return result;
      }

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

        if (argument_type_ == '=') {
          if (current_argument_.size() < 4 || current_argument_[3] != ':') {
            return Error(
                absl::InvalidArgumentError("invalid RESP verbatim string"),
                pos);
          }
          current_argument_.erase(0, 4);
        }

        command_.args_.push_back(std::move(current_argument_));
        current_argument_.clear();
        --arguments_remaining_;
        if (arguments_remaining_ != 0) {
          state_ = State::kArgumentStart;
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

std::string_view ReplyBuilder::AppendNullArray() {
  if (version_ == RespVersion::k3) {
    buffer_.append("_\r\n");
  } else {
    buffer_.append("*-1\r\n");
  }
  return buffer_;
}

std::string_view ReplyBuilder::AppendNull() {
  if (version_ == RespVersion::k3) {
    buffer_.append("_\r\n");
    return buffer_;
  }
  return AppendNullBulkString();
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

std::string_view ReplyBuilder::AppendBoolean(bool value) {
  if (version_ == RespVersion::k3) {
    buffer_.append(value ? "#t\r\n" : "#f\r\n");
    return buffer_;
  }
  return AppendInteger(value ? 1 : 0);
}

std::string_view ReplyBuilder::AppendDouble(double value) {
  char digits[64];
  const auto [end, error] =
      std::to_chars(digits, digits + sizeof(digits), value);
  assert(error == std::errc{});
  if (version_ == RespVersion::k3) {
    buffer_.push_back(',');
  } else {
    buffer_.push_back('$');
    AppendUnsigned(buffer_, static_cast<std::uint64_t>(end - digits));
    buffer_.append("\r\n");
  }
  buffer_.append(digits, end);
  buffer_.append("\r\n");
  return buffer_;
}

std::string_view ReplyBuilder::AppendDoubleText(std::string_view value) {
  if (version_ == RespVersion::k2) return AppendBulkString(value);
  buffer_.push_back(',');
  buffer_.append(value);
  buffer_.append("\r\n");
  return buffer_;
}

std::string_view ReplyBuilder::AppendBigNumber(std::string_view value) {
  if (version_ == RespVersion::k2) return AppendBulkString(value);
  buffer_.push_back('(');
  buffer_.append(value);
  buffer_.append("\r\n");
  return buffer_;
}

std::string_view ReplyBuilder::AppendVerbatimString(std::string_view format,
                                                    std::string_view value) {
  if (version_ == RespVersion::k2) return AppendBulkString(value);
  if (format.size() != 3) format = "txt";
  buffer_.push_back('=');
  AppendUnsigned(buffer_, value.size() + 4);
  buffer_.append("\r\n");
  buffer_.append(format.substr(0, 3));
  buffer_.push_back(':');
  buffer_.append(value);
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

std::string_view ReplyBuilder::AppendMapHeader(std::uint64_t count) {
  if (version_ == RespVersion::k2) {
    return AppendArrayHeader(count * 2);
  }
  buffer_.push_back('%');
  AppendUnsigned(buffer_, count);
  buffer_.append("\r\n");
  return buffer_;
}

std::string_view ReplyBuilder::AppendSetHeader(std::uint64_t count) {
  if (version_ == RespVersion::k2) return AppendArrayHeader(count);
  buffer_.push_back('~');
  AppendUnsigned(buffer_, count);
  buffer_.append("\r\n");
  return buffer_;
}

std::string_view ReplyBuilder::AppendPushHeader(std::uint64_t count) {
  if (version_ == RespVersion::k2) return AppendArrayHeader(count);
  buffer_.push_back('>');
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
