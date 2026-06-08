#include "celer/redis/resp.h"

#include <charconv>
#include <string_view>

namespace keylane {
using namespace celer;

namespace {

constexpr std::size_t kMaxArrayLen = 1024;
constexpr std::size_t kMaxBulkLen = 1024 * 1024;

bool ParseSignedNumber(std::string_view input, long long* value) {
  const char* begin = input.data();
  const char* end = input.data() + input.size();
  auto [ptr, ec] = std::from_chars(begin, end, *value);
  return ec == std::errc{} && ptr == end;
}

std::size_t FindCrlf(std::string_view input, std::size_t from) {
  return input.find("\r\n", from);
}

}  // namespace

RespParseResult ParseRespCommand(std::string_view input) {
  RespParseResult result;

  if (input.empty()) {
    return result;
  }
  if (input.front() != '*') {
    result.state = RespParseState::kError;
    result.status = Status(StatusCode::kInvalidArgument, "expected RESP array");
    return result;
  }

  std::size_t pos = 1;
  const std::size_t array_end = FindCrlf(input, pos);
  if (array_end == std::string_view::npos) {
    return result;
  }

  long long array_len_ll = 0;
  if (!ParseSignedNumber(input.substr(pos, array_end - pos), &array_len_ll) || array_len_ll < 0) {
    result.state = RespParseState::kError;
    result.status = Status(StatusCode::kInvalidArgument, "invalid RESP array length");
    return result;
  }
  if (array_len_ll > static_cast<long long>(kMaxArrayLen)) {
    result.state = RespParseState::kError;
    result.status = Status(StatusCode::kOutOfRange, "too many RESP array elements");
    return result;
  }

  const std::size_t array_len = static_cast<std::size_t>(array_len_ll);
  pos = array_end + 2;
  result.command.args.reserve(array_len);

  for (std::size_t i = 0; i < array_len; ++i) {
    if (pos >= input.size()) {
      return result;
    }
    if (input[pos] != '$') {
      result.state = RespParseState::kError;
      result.status = Status(StatusCode::kInvalidArgument, "expected RESP bulk string");
      return result;
    }
    ++pos;

    const std::size_t bulk_end = FindCrlf(input, pos);
    if (bulk_end == std::string_view::npos) {
      return result;
    }

    long long bulk_len_ll = 0;
    if (!ParseSignedNumber(input.substr(pos, bulk_end - pos), &bulk_len_ll) || bulk_len_ll < 0) {
      result.state = RespParseState::kError;
      result.status = Status(StatusCode::kInvalidArgument, "invalid RESP bulk string length");
      return result;
    }
    if (bulk_len_ll > static_cast<long long>(kMaxBulkLen)) {
      result.state = RespParseState::kError;
      result.status = Status(StatusCode::kOutOfRange, "RESP bulk string too large");
      return result;
    }

    const std::size_t bulk_len = static_cast<std::size_t>(bulk_len_ll);
    pos = bulk_end + 2;
    if (input.size() < pos + bulk_len + 2) {
      return result;
    }
    if (input[pos + bulk_len] != '\r' || input[pos + bulk_len + 1] != '\n') {
      result.state = RespParseState::kError;
      result.status = Status(StatusCode::kInvalidArgument, "malformed RESP bulk string terminator");
      return result;
    }

    result.command.args.emplace_back(input.substr(pos, bulk_len));
    pos += bulk_len + 2;
  }

  result.state = RespParseState::kOk;
  result.consumed = pos;
  return result;
}

std::string EncodeSimpleString(std::string_view value) {
  std::string out;
  out.reserve(value.size() + 3);
  out.push_back('+');
  out.append(value);
  out.append("\r\n");
  return out;
}

std::string EncodeBulkString(std::string_view value) {
  std::string out;
  out.reserve(value.size() + 32);
  out.push_back('$');
  out.append(std::to_string(value.size()));
  out.append("\r\n");
  out.append(value);
  out.append("\r\n");
  return out;
}

std::string EncodeNullBulkString() {
  return "$-1\r\n";
}

std::string EncodeInteger(long long value) {
  std::string out;
  out.reserve(32);
  out.push_back(':');
  out.append(std::to_string(value));
  out.append("\r\n");
  return out;
}

std::string EncodeError(std::string_view message) {
  std::string out;
  out.reserve(message.size() + 3);
  out.push_back('-');
  out.append(message);
  out.append("\r\n");
  return out;
}

}  // namespace keylane
