#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "celer/base/status.h"

namespace keylane {
using namespace celer;

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
  Status status = Status::Ok();
  RespCommand command;
};

RespParseResult ParseRespCommand(std::string_view input);

std::string EncodeSimpleString(std::string_view value);
std::string EncodeBulkString(std::string_view value);
std::string EncodeNullBulkString();
std::string EncodeInteger(long long value);
std::string EncodeError(std::string_view message);
std::string EncodeScanReply(std::uint64_t cursor,
                            const std::vector<std::string>& keys);

}  // namespace keylane
