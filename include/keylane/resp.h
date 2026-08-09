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
  std::vector<std::string> args;
};

enum class RespParseState {
  kOk,
  kNeedMoreData,
  kError,
};

struct RespParseResult {
  RespParseState state = RespParseState::kNeedMoreData;
  std::size_t consumed = 0;
  absl::Status status = absl::OkStatus();
  RespCommand command;
};

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
