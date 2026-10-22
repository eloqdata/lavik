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

#include <algorithm>
#include <cassert>
#include <charconv>
#include <cstring>
#include <limits>
#include <new>
#include <stdexcept>
#include <string_view>

#include "absl/strings/str_cat.h"

namespace lavik {

namespace {

// Valkey accepts multibulk lengths up to INT_MAX. Its parser uses 1024 only
// as the initial argv capacity and grows the array as arguments arrive; it is
// not a protocol limit.
constexpr long long kMaxArrayLen = std::numeric_limits<int>::max();
constexpr std::size_t kInitialArgCapacity = 1024;
constexpr std::size_t kMaxBulkLen = 512ULL * 1024 * 1024;
constexpr std::size_t kDirectBulkThreshold = 32 * 1024;
constexpr std::size_t kDirectBulkReservation = 1024 * 1024;
constexpr std::size_t kDirectBulkReadBytes = 256 * 1024;
constexpr std::size_t kMaxLengthTextBytes = 32;
absl::Status AppendArgument(CommandArgs* output, std::string_view value) {
  try {
    output->AppendLast(value);
  } catch (const std::length_error&) {
    return absl::ResourceExhaustedError("client argument is too large");
  }
  return absl::OkStatus();
}

absl::Status ReserveArguments(CommandArgs* output, std::size_t desired) {
  if (desired <= output->capacity()) return absl::OkStatus();
  std::size_t allocation_capacity = desired;
  if (output->capacity() <= std::numeric_limits<std::size_t>::max() / 2) {
    allocation_capacity = std::max(allocation_capacity, output->capacity() * 2);
  } else {
    return absl::ResourceExhaustedError("too many client arguments");
  }
  if (allocation_capacity >
      std::numeric_limits<std::size_t>::max() / sizeof(OffsetAndLength)) {
    return absl::ResourceExhaustedError("too many client arguments");
  }
  try {
    // Keep argument-index growth geometric. Reserving only `desired` here
    // makes every argument after the initial 1024 move the complete vector.
    output->reserve(allocation_capacity);
  } catch (const std::length_error&) {
    return absl::ResourceExhaustedError("too many client arguments");
  }
  return absl::OkStatus();
}

absl::Status ParseInlineArguments(CommandArgs& args) {
  const std::string_view line(args.bytes.data(), args.bytes.size());
  std::size_t output = 0;
  std::size_t pos = 0;
  while (pos < line.size()) {
    while (pos < line.size() && (line[pos] == ' ' || line[pos] == '\t')) ++pos;
    if (pos == line.size()) break;
    if (args.size() == args.capacity()) [[unlikely]] {
      absl::Status reserved = ReserveArguments(&args, args.size() + 1);
      if (!reserved.ok()) return reserved;
    }
    args.args.push_back({output, 0});
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
        args.bytes[output++] = value;
        ++args.args.back().length;
        continue;
      }
      if (pos == line.size())
        return absl::InvalidArgumentError("unterminated inline escape");
      const char escaped = line[pos++];
      switch (escaped) {
        case 'n':
          value = '\n';
          break;
        case 'r':
          value = '\r';
          break;
        case 't':
          value = '\t';
          break;
        case 'b':
          value = '\b';
          break;
        case 'a':
          value = '\a';
          break;
        case 'x': {
          if (pos + 2 > line.size())
            return absl::InvalidArgumentError("invalid inline hex escape");
          unsigned byte = 0;
          const auto parsed = std::from_chars(line.data() + pos,
                                              line.data() + pos + 2, byte, 16);
          if (parsed.ec != std::errc{} || parsed.ptr != line.data() + pos + 2)
            return absl::InvalidArgumentError("invalid inline hex escape");
          value = static_cast<char>(byte);
          pos += 2;
          break;
        }
        default:
          value = escaped;
          break;
      }
      args.bytes[output++] = value;
      ++args.args.back().length;
    }
    if (!closed) return absl::InvalidArgumentError("unterminated inline quote");
    if (quote != 0 && pos < line.size() && line[pos] != ' ' &&
        line[pos] != '\t')
      return absl::InvalidArgumentError("characters after inline quote");
  }
  // Decoding only removes framing/escapes, so output never overtakes input.
  // No byte-buffer growth or argument string allocation occurs while decoding.
  args.bytes.resize(output);
  return absl::OkStatus();
}

}  // namespace

void RespCommandParser::ResetCommand() {
  state_ = State::kArrayStart;
  length_text_.clear();
  // A completed command has already transferred its handle. An abandoned
  // command releases its storage and both packed buffers' overflow heaps.
  command_.args_.Reset();
  arguments_remaining_ = 0;
  bulk_remaining_ = 0;
  bulk_prepared_ = 0;
  terminator_bytes_ = 0;
  command_bytes_ = 0;
}

void RespCommandParser::Reset() { ResetCommand(); }

// Keep the storage-selection wrapper inline even when its allocation fallback
// exceeds the compiler's automatic inlining budget. Existing-capacity parsing
// should not pay an out-of-line call just to obtain the argument owner.
[[gnu::always_inline]] inline CommandArgs& RespCommandParser::Arguments() {
  if (!command_.args_.HasStorage() && args_storage_ != nullptr)
    command_.args_ = CommandArgsHandle::TryBorrow(*args_storage_);
  return command_.args_.Get();
}

std::size_t RespCommandParser::BulkReadReservationSize() const noexcept {
  if (state_ != State::kBulkData || command_.args_.back().empty() ||
      bulk_prepared_ != 0 ||
      command_.args_.back().size() + bulk_remaining_ < kDirectBulkThreshold ||
      command_bytes_ >= query_buffer_limit_)
    return 0;
  return std::min({bulk_remaining_, kDirectBulkReservation,
                   query_buffer_limit_ - command_bytes_});
}

absl::Status RespCommandParser::PrepareBulkRead(std::size_t bytes) {
  if (bytes == 0 || bytes > BulkReadReservationSize())
    return absl::InvalidArgumentError("invalid RESP bulk read reservation");
  // Resize makes the whole writable range valid byte storage. Tracking its
  // unread tail prevents initialized but unreceived bytes becoming payload.
  // Allocation failure follows Parse's process-fatal allocation policy.
  command_.args_.Get().ResizeLast(command_.args_.back().size() + bytes);
  bulk_prepared_ = bytes;
  return absl::OkStatus();
}

std::span<char> RespCommandParser::BulkReadBuffer() noexcept {
  if (state_ != State::kBulkData) return {};
  const auto bytes = std::min(bulk_prepared_, kDirectBulkReadBytes);
  return {command_.args_.Get().MutableLast().data() +
              command_.args_.back().size() - bulk_prepared_,
          bytes};
}

absl::Status RespCommandParser::CommitBulkRead(std::size_t bytes) {
  if (state_ != State::kBulkData || bulk_prepared_ == 0 ||
      bytes > std::min(bulk_prepared_, kDirectBulkReadBytes))
    return absl::InvalidArgumentError("invalid RESP bulk read length");
  // The query limit can change while a socket read is suspended. Recheck
  // accounting before advancing any logical state, even for an earlier span.
  if (!Account(bytes))
    return absl::ResourceExhaustedError(
        "client request exceeds the query buffer limit");
  bulk_prepared_ -= bytes;
  bulk_remaining_ -= bytes;
  if (bulk_remaining_ == 0) {
    terminator_bytes_ = 0;
    state_ = State::kBulkTerminator;
  }
  return absl::OkStatus();
}

bool RespCommandParser::Account(std::size_t bytes) {
  if (bytes > query_buffer_limit_ ||
      command_bytes_ > query_buffer_limit_ - bytes) {
    return false;
  }
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
        const std::size_t end =
            newline == std::string_view::npos ? input.size() : newline + 1;
        auto& bytes = Arguments().bytes;
        const auto part = input.substr(pos, end - pos);
        if (part.size() > bytes.max_size() - bytes.size())
          return Error(
              absl::ResourceExhaustedError("client argument is too large"),
              pos);
        bytes.insert(bytes.end(), part.begin(), part.end());
        if (!consume(end - pos)) {
          return Error(absl::ResourceExhaustedError(
                           "client request exceeds the query buffer limit"),
                       pos);
        }
        if (newline == std::string_view::npos) break;
        bytes.pop_back();
        if (!bytes.empty() && bytes.back() == '\r') bytes.pop_back();
        auto parsed = ParseInlineArguments(command_.args_.Get());
        if (!parsed.ok()) return Error(parsed, pos);
        if (command_.args_.empty()) {
          ResetCommand();
          break;
        }
        result.state_ = RespParseState::kOk;
        result.consumed_ = pos;
        result.command_ = std::move(command_);
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
              auto& args = Arguments();
              const auto desired =
                  std::min(arguments_remaining_, kInitialArgCapacity);
              if (desired > args.capacity()) [[unlikely]] {
                absl::Status reserved = ReserveArguments(&args, desired);
                if (!reserved.ok()) return Error(reserved, pos);
              }
              state_ = State::kArgumentStart;
            } else {
              if (parsed > static_cast<long long>(kMaxBulkLen) ||
                  static_cast<unsigned long long>(parsed) >
                      command_.args_.Get().bytes.max_size()) {
                return Error(
                    absl::OutOfRangeError("RESP bulk string too large"), pos);
              }
              bulk_remaining_ = static_cast<std::size_t>(parsed);
              assert(bulk_prepared_ == 0);
              command_.args_.Get().ResizeLast(0);
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

      case State::kArgumentStart: {
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
        // The nonempty array header already acquired storage. Only invoke the
        // growth helper after exhausting its index capacity, not per argument.
        auto& args = command_.args_.Get();
        if (args.size() == args.capacity()) [[unlikely]] {
          absl::Status reserved = ReserveArguments(&args, args.size() + 1);
          if (!reserved.ok()) return Error(reserved, pos);
        }
        args.StartArgument();
        state_ = argument_type_ == '$' || argument_type_ == '='
                     ? State::kBulkLength
                     : State::kLineArgument;
        break;
      }

      case State::kLineArgument: {
        const std::size_t newline = input.find('\n', pos);
        const std::size_t end =
            newline == std::string_view::npos ? input.size() : newline + 1;
        absl::Status appended =
            AppendArgument(&command_.args_.Get(), input.substr(pos, end - pos));
        if (!appended.ok()) return Error(appended, pos);
        if (!consume(end - pos)) {
          return Error(absl::ResourceExhaustedError(
                           "client request exceeds the query buffer limit"),
                       pos);
        }
        if (newline == std::string_view::npos) break;
        if (command_.args_.back().size() < 2 ||
            command_.args_.back()[command_.args_.back().size() - 2] != '\r') {
          return Error(
              absl::InvalidArgumentError("malformed RESP scalar terminator"),
              pos);
        }
        command_.args_.Get().ResizeLast(command_.args_.back().size() - 2);
        if (argument_type_ == '#') {
          const auto value = command_.args_.back();
          if (value != "t" && value != "f")
            return Error(absl::InvalidArgumentError("invalid RESP boolean"),
                         pos);
          const char digit = value == "t" ? '1' : '0';
          command_.args_.Get().MutableLast()[0] = digit;
        }
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
          // bulk_remaining_ bounds this argument; AppendLast also checks the
          // total packed size because earlier arguments share the same buffer.
          // Allocation failure remains process-fatal through the coroutine's
          // unhandled-exception policy.
          if (bulk_prepared_ == 0) [[likely]] {
            command_.args_.Get().AppendLast(input.substr(pos, take));
          } else {
            const auto prepared = std::min(take, bulk_prepared_);
            std::memcpy(command_.args_.Get().MutableLast().data() +
                            command_.args_.back().size() - bulk_prepared_,
                        input.data() + pos, prepared);
            bulk_prepared_ -= prepared;
            if (take > prepared)
              command_.args_.Get().AppendLast(
                  input.substr(pos + prepared, take - prepared));
          }
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
          if (command_.args_.back().size() < 4 ||
              command_.args_.back()[3] != ':') {
            return Error(
                absl::InvalidArgumentError("invalid RESP verbatim string"),
                pos);
          }
          command_.args_.Get().RemoveLastPrefix(4);
        }

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

std::string_view ReplyBuilder::AppendBulkStrings(
    std::span<const std::string> values) {
  return AppendBulkStrings(values, [](const std::string& value) noexcept {
    return std::string_view(value);
  });
}

std::string_view ReplyBuilder::AppendBulkStrings(
    std::span<const std::string_view> values) {
  return AppendBulkStrings(values, std::identity{});
}

std::string_view ReplyBuilder::AppendBulkStrings(
    std::span<const std::optional<std::string>> values) {
  return AppendBulkStrings(
      values,
      [](const std::optional<std::string>& value) noexcept
          -> std::optional<std::string_view> {
        return value ? std::optional<std::string_view>(*value) : std::nullopt;
      });
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

std::string ClusterMovedMessage(std::uint16_t slot, std::string_view host,
                                std::uint16_t port) {
  return absl::StrCat("MOVED ", slot, " ", host, ":", port);
}

std::string ClusterTryAgainMessage(std::string_view message) {
  return absl::StrCat("TRYAGAIN ", message);
}

std::string_view AppendMovedError(ReplyBuilder& builder, std::uint16_t slot,
                                  std::string_view host, std::uint16_t port) {
  return builder.AppendError(ClusterMovedMessage(slot, host, port));
}

std::string_view AppendCrossSlotError(ReplyBuilder& builder) {
  return builder.AppendError(kClusterCrossSlotMessage);
}

std::string_view AppendClusterDownUnboundError(ReplyBuilder& builder) {
  return builder.AppendError(kClusterDownUnboundMessage);
}

std::string_view AppendTryAgainError(ReplyBuilder& builder,
                                     std::string_view message) {
  return builder.AppendError(ClusterTryAgainMessage(message));
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

}  // namespace lavik
