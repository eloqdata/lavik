#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/statusor.h"

namespace keylane {

struct RespCommand {
  std::vector<std::string> args_;
};

enum class RespParseState {
  kOk,
  kNeedMoreData,
  kError,
};

struct RespParseResult {
  RespParseState state_ = RespParseState::kNeedMoreData;
  std::size_t consumed_ = 0;
  absl::Status status_ = absl::OkStatus();
  RespCommand command_;
};

// Incremental RESP command parser. Bytes reported in consumed_ are retained in
// parser-owned state when a command is incomplete and may be discarded by the
// caller. One parser belongs to one connection.
class RespCommandParser {
 public:
  RespParseResult Parse(std::string_view input);

  void Reset();
  [[nodiscard]] bool idle() const noexcept {
    return state_ == State::kArrayStart;
  }

 private:
  enum class State : std::uint8_t {
    kArrayStart,
    kArrayLength,
    kBulkStart,
    kBulkLength,
    kBulkData,
    kBulkTerminator,
  };

  RespParseResult Error(absl::Status status, std::size_t consumed);
  bool Account(std::size_t bytes);
  void ResetCommand();

  State state_ = State::kArrayStart;
  std::string length_text_;
  RespCommand command_;
  std::string current_argument_;
  std::size_t arguments_remaining_ = 0;
  std::size_t bulk_remaining_ = 0;
  std::size_t terminator_bytes_ = 0;
  std::size_t command_bytes_ = 0;
};

// Convenience wrapper for callers that already hold one complete contiguous
// request. Incremental network paths should retain a RespCommandParser.
RespParseResult ParseRespCommand(std::string_view input);

// Reuses one contiguous response buffer for the lifetime of a connection.
// A reply remains valid until Reset() is called for the next request.
class ReplyBuilder {
 public:
  void Reset();
  void Reserve(std::size_t capacity);

  std::string_view AppendSimpleString(std::string_view value);
  std::string_view AppendBulkString(std::string_view value);
  std::string_view AppendNullBulkString();
  std::string_view AppendInteger(long long value);
  std::string_view AppendError(std::string_view message);
  std::string_view AppendError(std::string_view prefix,
                               std::string_view message);
  std::string_view AppendArrayHeader(std::uint64_t count);
  std::string_view AppendRaw(std::string_view encoded);

  [[nodiscard]] std::string_view View() const noexcept { return buffer_; }
  [[nodiscard]] std::size_t Capacity() const noexcept {
    return buffer_.capacity();
  }
  [[nodiscard]] std::string Release() && { return std::move(buffer_); }

 private:
  std::string buffer_;
};

std::string EncodeSimpleString(std::string_view value);
std::string EncodeBulkString(std::string_view value);
std::string EncodeNullBulkString();
std::string EncodeInteger(long long value);
std::string EncodeError(std::string_view message);
std::string_view EncodeScanReply(ReplyBuilder& builder, std::uint64_t cursor,
                                 const std::vector<std::string>& keys);
std::string EncodeScanReply(std::uint64_t cursor,
                            const std::vector<std::string>& keys);

}  // namespace keylane
