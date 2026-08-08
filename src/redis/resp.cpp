#include "keylane/resp.h"

#include <charconv>
#include <string_view>

namespace keylane {
using namespace celer;

namespace {

constexpr std::size_t kMaxArrayLen = 1024;
constexpr std::size_t kMaxBulkLen = 512ULL * 1024 * 1024;

}  // namespace

RespParseResult ParseRespCommand(std::string_view input) {
  RespParseResult result;

  // An empty multibulk (*0\r\n) is likewise a command that does nothing:
  // real Redis consumes it without sending anything back, and replying (even
  // an error) would shift the client's request/reply pairing off by one for
  // the rest of the connection. Skip them iteratively, interleaved with the
  // blank lines below.
  std::size_t skipped = 0;
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
    if (input.size() >= 4 && input.substr(0, 4) == "*0\r\n") {
      input.remove_prefix(4);
      skipped += 4;
      continue;
    }
    break;
  }

  if (input.empty() || input.front() != '*') {
    if (!input.empty()) {
      result.state = RespParseState::kError;
      result.status = Status(StatusCode::kInvalidArgument, "expected RESP array");
    }
    return result;
  }

  // Parse array length: *<N>\r\n
  std::size_t pos = 1;
  std::size_t crlf = input.find("\r\n", pos);
  if (crlf == std::string_view::npos) return result;

  long long array_len = 0;
  auto [ptr, ec] = std::from_chars(input.data() + pos, input.data() + crlf, array_len);
  if (ec != std::errc{} || ptr != input.data() + crlf || array_len < 0) {
    result.state = RespParseState::kError;
    result.status = Status(StatusCode::kInvalidArgument, "invalid RESP array length");
    return result;
  }
  if (array_len > static_cast<long long>(kMaxArrayLen)) {
    result.state = RespParseState::kError;
    result.status = Status(StatusCode::kOutOfRange, "too many RESP array elements");
    return result;
  }

  const auto count = static_cast<std::size_t>(array_len);
  pos = crlf + 2;
  result.command.args.reserve(count);

  // Parse each bulk string: $<N>\r\n<data>\r\n
  for (std::size_t i = 0; i < count; ++i) {
    // Running out of input between arguments is truncation, not corruption:
    // a pipelined stream splits wherever the socket happens to split it, and
    // landing exactly on an argument boundary is ordinary. Report it the same
    // way as every other short read so the caller waits for the rest instead
    // of failing the connection.
    if (pos >= input.size()) return result;
    if (input[pos] != '$') {
      result.state = RespParseState::kError;
      result.status =
          Status(StatusCode::kInvalidArgument, "expected RESP bulk string");
      return result;
    }
    ++pos;

    crlf = input.find("\r\n", pos);
    if (crlf == std::string_view::npos) return result;

    long long bulk_len = 0;
    auto [p2, ec2] = std::from_chars(input.data() + pos, input.data() + crlf, bulk_len);
    if (ec2 != std::errc{} || p2 != input.data() + crlf || bulk_len < 0) {
      result.state = RespParseState::kError;
      result.status = Status(StatusCode::kInvalidArgument, "invalid RESP bulk string length");
      return result;
    }
    if (bulk_len > static_cast<long long>(kMaxBulkLen)) {
      result.state = RespParseState::kError;
      result.status = Status(StatusCode::kOutOfRange, "RESP bulk string too large");
      return result;
    }

    const auto data_len = static_cast<std::size_t>(bulk_len);
    pos = crlf + 2;
    if (input.size() < pos + data_len + 2) return result;
    if (input[pos + data_len] != '\r' || input[pos + data_len + 1] != '\n') {
      result.state = RespParseState::kError;
      result.status = Status(StatusCode::kInvalidArgument, "malformed RESP bulk string terminator");
      return result;
    }

    result.command.args.emplace_back(input.substr(pos, data_len));
    pos += data_len + 2;
  }

  result.state = RespParseState::kOk;
  result.consumed = skipped + pos;
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

std::string EncodeNullBulkString() { return "$-1\r\n"; }

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

std::string EncodeScanReply(std::uint64_t cursor,
                            const std::vector<std::string>& keys) {
  std::string out;
  out.reserve(64 + keys.size() * 16);
  out.append("*2\r\n");
  const std::string encoded_cursor = std::to_string(cursor);
  out.push_back('$');
  out.append(std::to_string(encoded_cursor.size()));
  out.append("\r\n");
  out.append(encoded_cursor);
  out.append("\r\n*");
  out.append(std::to_string(keys.size()));
  out.append("\r\n");
  for (const std::string& key : keys) {
    out.push_back('$');
    out.append(std::to_string(key.size()));
    out.append("\r\n");
    out.append(key);
    out.append("\r\n");
  }
  return out;
}

}  // namespace keylane
