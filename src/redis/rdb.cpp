#include "keylane/rdb.h"

#include <algorithm>
#include <array>
#include <bit>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "absl/strings/str_cat.h"
#include "keylane/storage/format.h"

namespace keylane::rdb {
namespace {

// Redis 7.2 RDB constants. The format and CRC implementation are derived from
// Redis 7.2, distributed under the three-clause BSD license.
constexpr std::uint8_t kString = 0;
constexpr std::uint8_t kList = 1;
constexpr std::uint8_t kSet = 2;
constexpr std::uint8_t kZSet = 3;
constexpr std::uint8_t kHash = 4;
constexpr std::uint8_t kZSet2 = 5;
constexpr std::uint8_t kModulePreGa = 6;
constexpr std::uint8_t kModule2 = 7;
constexpr std::uint8_t kHashZipmap = 9;
constexpr std::uint8_t kListZiplist = 10;
constexpr std::uint8_t kSetIntset = 11;
constexpr std::uint8_t kZSetZiplist = 12;
constexpr std::uint8_t kHashZiplist = 13;
constexpr std::uint8_t kListQuicklist = 14;
constexpr std::uint8_t kStreamListpacks = 15;
constexpr std::uint8_t kHashListpack = 16;
constexpr std::uint8_t kZSetListpack = 17;
constexpr std::uint8_t kListQuicklist2 = 18;
constexpr std::uint8_t kStreamListpacks2 = 19;
constexpr std::uint8_t kSetListpack = 20;
constexpr std::uint8_t kStreamListpacks3 = 21;

constexpr std::uint64_t kCrcPolynomial = 0xad93d23594c935a9ULL;
constexpr std::string_view kListMagic = "KLL1";
constexpr std::string_view kZSetMagic = "KZS1";
constexpr std::string_view kStreamMagic = "KXS1";

absl::Status Bad(std::string_view detail = {}) {
  return absl::InvalidArgumentError(
      detail.empty() ? "Bad data format"
                     : absl::StrCat("Bad data format: ", detail));
}

std::uint64_t Reflect64(std::uint64_t value) {
  std::uint64_t result = value & 1;
  for (unsigned bit = 1; bit < 64; ++bit) {
    value >>= 1;
    result = (result << 1) | (value & 1);
  }
  return result;
}

std::uint64_t Crc64(std::string_view input) {
  std::uint64_t crc = 0;
  for (unsigned char byte : input) {
    for (unsigned mask = 1; mask <= 0x80; mask <<= 1) {
      bool high = (crc & (std::uint64_t{1} << 63)) != 0;
      if ((byte & mask) != 0) high = !high;
      crc <<= 1;
      if (high) crc ^= kCrcPolynomial;
    }
  }
  return Reflect64(crc);
}

void PutLe16(std::string* out, std::uint16_t value) {
  out->push_back(static_cast<char>(value));
  out->push_back(static_cast<char>(value >> 8));
}
void PutLe32(std::string* out, std::uint32_t value) {
  for (unsigned i = 0; i < 4; ++i)
    out->push_back(static_cast<char>(value >> (8 * i)));
}
void PutLe64(std::string* out, std::uint64_t value) {
  for (unsigned i = 0; i < 8; ++i)
    out->push_back(static_cast<char>(value >> (8 * i)));
}
void PutBe64(std::string* out, std::uint64_t value) {
  for (unsigned i = 0; i < 8; ++i)
    out->push_back(static_cast<char>(value >> (56 - 8 * i)));
}

class Reader {
 public:
  explicit Reader(std::string_view input) : input_(input) {}
  std::size_t remaining() const { return input_.size() - at_; }
  std::size_t position() const { return at_; }
  bool done() const { return at_ == input_.size(); }
  bool AccountExpanded(std::uint64_t bytes) {
    if (bytes > storage::kMaxStringBytes - expanded_bytes_) return false;
    expanded_bytes_ += bytes;
    return true;
  }

  bool Byte(std::uint8_t* value) {
    if (at_ == input_.size()) return false;
    *value = static_cast<std::uint8_t>(input_[at_++]);
    return true;
  }
  bool Bytes(std::size_t size, std::string_view* value) {
    if (size > remaining()) return false;
    *value = input_.substr(at_, size);
    at_ += size;
    return true;
  }
  bool Le16(std::uint16_t* value) {
    std::string_view bytes;
    if (!Bytes(2, &bytes)) return false;
    *value =
        static_cast<unsigned char>(bytes[0]) |
        (static_cast<std::uint16_t>(static_cast<unsigned char>(bytes[1])) << 8);
    return true;
  }
  bool Le32(std::uint32_t* value) {
    std::string_view bytes;
    if (!Bytes(4, &bytes)) return false;
    *value = 0;
    for (unsigned i = 0; i < 4; ++i)
      *value |= static_cast<std::uint32_t>(static_cast<unsigned char>(bytes[i]))
                << (8 * i);
    return true;
  }
  bool Le64(std::uint64_t* value) {
    std::string_view bytes;
    if (!Bytes(8, &bytes)) return false;
    *value = 0;
    for (unsigned i = 0; i < 8; ++i)
      *value |= static_cast<std::uint64_t>(static_cast<unsigned char>(bytes[i]))
                << (8 * i);
    return true;
  }
  bool Be32(std::uint32_t* value) {
    std::string_view bytes;
    if (!Bytes(4, &bytes)) return false;
    *value = 0;
    for (unsigned char byte : bytes) *value = (*value << 8) | byte;
    return true;
  }
  bool Be64(std::uint64_t* value) {
    std::string_view bytes;
    if (!Bytes(8, &bytes)) return false;
    *value = 0;
    for (unsigned char byte : bytes) *value = (*value << 8) | byte;
    return true;
  }

 private:
  std::string_view input_;
  std::size_t at_ = 0;
  std::uint64_t expanded_bytes_ = 0;
};

struct Length {
  std::uint64_t value = 0;
  bool encoded = false;
};

absl::StatusOr<Length> ReadLength(Reader* reader) {
  std::uint8_t first = 0;
  if (!reader->Byte(&first)) return Bad("truncated length");
  const unsigned kind = first >> 6;
  if (kind == 0) {
    return Length{static_cast<std::uint64_t>(first & 0x3f), false};
  }
  if (kind == 1) {
    std::uint8_t second = 0;
    if (!reader->Byte(&second)) return Bad("truncated length");
    return Length{static_cast<std::uint64_t>(first & 0x3f) << 8 | second,
                  false};
  }
  if (kind == 3) {
    return Length{static_cast<std::uint64_t>(first & 0x3f), true};
  }
  if (first == 0x80) {
    std::uint32_t value = 0;
    if (!reader->Be32(&value)) return Bad("truncated 32-bit length");
    return Length{value, false};
  }
  if (first == 0x81) {
    std::uint64_t value = 0;
    if (!reader->Be64(&value)) return Bad("truncated 64-bit length");
    return Length{value, false};
  }
  return Bad("invalid length encoding");
}

void WriteLength(std::string* out, std::uint64_t value) {
  if (value < 64) {
    out->push_back(static_cast<char>(value));
  } else if (value < 16384) {
    out->push_back(static_cast<char>(0x40 | (value >> 8)));
    out->push_back(static_cast<char>(value));
  } else if (value <= UINT32_MAX) {
    out->push_back(static_cast<char>(0x80));
    for (unsigned i = 0; i < 4; ++i)
      out->push_back(static_cast<char>(value >> (24 - 8 * i)));
  } else {
    out->push_back(static_cast<char>(0x81));
    for (unsigned i = 0; i < 8; ++i)
      out->push_back(static_cast<char>(value >> (56 - 8 * i)));
  }
}

bool LzfDecompress(std::string_view compressed, std::string* output) {
  const auto* input = reinterpret_cast<const unsigned char*>(compressed.data());
  std::size_t ip = 0, op = 0;
  while (ip < compressed.size()) {
    unsigned ctrl = input[ip++];
    if (ctrl < 32) {
      const std::size_t length = ctrl + 1;
      if (length > compressed.size() - ip || length > output->size() - op)
        return false;
      std::memcpy(output->data() + op, input + ip, length);
      ip += length;
      op += length;
      continue;
    }
    std::size_t length = ctrl >> 5;
    std::size_t distance = (ctrl & 0x1f) << 8;
    if (ip == compressed.size()) return false;
    if (length == 7) {
      length += input[ip++];
      if (ip == compressed.size()) return false;
    }
    distance += input[ip++] + 1;
    length += 2;
    if (distance > op || length > output->size() - op) return false;
    for (std::size_t i = 0; i < length; ++i)
      (*output)[op + i] = (*output)[op - distance + i];
    op += length;
  }
  return op == output->size();
}

absl::StatusOr<std::string> ReadString(Reader* reader) {
  auto length = ReadLength(reader);
  if (!length.ok()) return length.status();
  if (!length->encoded) {
    if (length->value > storage::kMaxStringBytes ||
        length->value > reader->remaining())
      return Bad("string length exceeds payload");
    if (!reader->AccountExpanded(length->value)) {
      return Bad("expanded value exceeds Keylane limits");
    }
    std::string_view value;
    reader->Bytes(static_cast<std::size_t>(length->value), &value);
    return std::string(value);
  }
  if (length->value <= 2) {
    const unsigned bytes = length->value == 0 ? 1 : length->value == 1 ? 2 : 4;
    std::string_view encoded;
    if (!reader->Bytes(bytes, &encoded)) return Bad("truncated integer string");
    std::uint32_t raw = 0;
    for (unsigned i = 0; i < bytes; ++i)
      raw |= static_cast<std::uint32_t>(static_cast<unsigned char>(encoded[i]))
             << (8 * i);
    std::int64_t value = bytes == 1   ? static_cast<std::int8_t>(raw)
                         : bytes == 2 ? static_cast<std::int16_t>(raw)
                                      : static_cast<std::int32_t>(raw);
    std::string output = std::to_string(value);
    if (!reader->AccountExpanded(output.size())) {
      return Bad("expanded value exceeds Keylane limits");
    }
    return output;
  }
  if (length->value != 3) return Bad("unknown encoded string");
  auto compressed_size = ReadLength(reader);
  auto output_size = ReadLength(reader);
  if (!compressed_size.ok()) return compressed_size.status();
  if (!output_size.ok()) return output_size.status();
  if (compressed_size->encoded || output_size->encoded ||
      output_size->value > storage::kMaxStringBytes ||
      compressed_size->value > reader->remaining())
    return Bad("invalid LZF lengths");
  if (!reader->AccountExpanded(output_size->value)) {
    return Bad("expanded value exceeds Keylane limits");
  }
  std::string_view compressed;
  reader->Bytes(static_cast<std::size_t>(compressed_size->value), &compressed);
  std::string output(static_cast<std::size_t>(output_size->value), '\0');
  if (!LzfDecompress(compressed, &output)) return Bad("invalid LZF data");
  return output;
}

void WriteString(std::string* out, std::string_view value) {
  WriteLength(out, value.size());
  out->append(value);
}

struct LpValue {
  bool integer = false;
  std::int64_t number = 0;
  std::string text;
  std::string String() const { return integer ? std::to_string(number) : text; }
  std::optional<std::int64_t> Integer() const {
    if (integer) return number;
    std::int64_t value = 0;
    auto parsed =
        std::from_chars(text.data(), text.data() + text.size(), value);
    if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size())
      return std::nullopt;
    return value;
  }
};

unsigned BackLengthBytes(std::uint64_t length) {
  return length <= 127        ? 1
         : length < 16383     ? 2
         : length < 2097151   ? 3
         : length < 268435455 ? 4
                              : 5;
}

void AppendBackLength(std::string* out, std::uint64_t length) {
  const unsigned bytes = BackLengthBytes(length);
  for (unsigned i = 0; i < bytes; ++i) {
    const unsigned shift = 7 * (bytes - i - 1);
    std::uint8_t byte = static_cast<std::uint8_t>((length >> shift) & 0x7f);
    if (i != 0) byte |= 0x80;
    out->push_back(static_cast<char>(byte));
  }
}

absl::StatusOr<std::vector<LpValue>> DecodeListpack(std::string_view input) {
  if (input.size() < 7 || input.size() > storage::kMaxStringBytes)
    return Bad("invalid listpack size");
  Reader header(input);
  std::uint32_t total = 0;
  std::uint16_t declared = 0;
  if (!header.Le32(&total) || !header.Le16(&declared) ||
      total != input.size() || static_cast<unsigned char>(input.back()) != 0xff)
    return Bad("invalid listpack header");
  std::size_t at = 6;
  std::vector<LpValue> values;
  while (at < input.size() - 1) {
    const std::size_t start = at;
    const auto byte = static_cast<std::uint8_t>(input[at++]);
    LpValue value;
    std::size_t payload = 0;
    unsigned integer_bytes = 0;
    unsigned integer_bits = 0;
    std::uint64_t integer_value = 0;
    if ((byte & 0x80) == 0) {
      value.integer = true;
      value.number = byte;
    } else if ((byte & 0xc0) == 0x80) {
      payload = byte & 0x3f;
    } else if ((byte & 0xe0) == 0xc0) {
      if (at == input.size() - 1) return Bad("truncated listpack integer");
      integer_value = (static_cast<std::uint64_t>(byte & 0x1f) << 8) |
                      static_cast<unsigned char>(input[at++]);
      integer_bits = 13;
    } else if ((byte & 0xf0) == 0xe0) {
      if (at == input.size() - 1) return Bad("truncated listpack string");
      payload = (static_cast<std::size_t>(byte & 0x0f) << 8) |
                static_cast<unsigned char>(input[at++]);
    } else if (byte == 0xf0) {
      if (input.size() - 1 - at < 4) return Bad("truncated listpack string");
      payload =
          static_cast<unsigned char>(input[at]) |
          (static_cast<std::uint32_t>(static_cast<unsigned char>(input[at + 1]))
           << 8) |
          (static_cast<std::uint32_t>(static_cast<unsigned char>(input[at + 2]))
           << 16) |
          (static_cast<std::uint32_t>(static_cast<unsigned char>(input[at + 3]))
           << 24);
      at += 4;
    } else if (byte >= 0xf1 && byte <= 0xf4) {
      integer_bytes = byte == 0xf1   ? 2
                      : byte == 0xf2 ? 3
                      : byte == 0xf3 ? 4
                                     : 8;
      integer_bits = integer_bytes * 8;
      if (input.size() - 1 - at < integer_bytes)
        return Bad("truncated listpack integer");
      for (unsigned i = 0; i < integer_bytes; ++i)
        integer_value |=
            static_cast<std::uint64_t>(static_cast<unsigned char>(input[at++]))
            << (8 * i);
    } else {
      return Bad("unknown listpack encoding");
    }
    if (payload != 0 || ((byte & 0xc0) == 0x80) || ((byte & 0xf0) == 0xe0) ||
        byte == 0xf0) {
      if (payload > input.size() - 1 - at)
        return Bad("truncated listpack string");
      value.text.assign(input.substr(at, payload));
      at += payload;
    } else if (integer_bits != 0) {
      value.integer = true;
      const std::uint64_t sign = std::uint64_t{1} << (integer_bits - 1);
      if ((integer_value & sign) != 0 && integer_bits < 64)
        integer_value |= (~std::uint64_t{0}) << integer_bits;
      value.number = static_cast<std::int64_t>(integer_value);
    }
    const std::size_t encoded = at - start;
    const unsigned back_bytes = BackLengthBytes(encoded);
    if (back_bytes > input.size() - 1 - at)
      return Bad("truncated listpack back length");
    std::uint64_t back = 0;
    for (unsigned i = 0; i < back_bytes; ++i) {
      const std::uint8_t item = static_cast<std::uint8_t>(input[at++]);
      if ((i == 0 && (item & 0x80) != 0) || (i != 0 && (item & 0x80) == 0))
        return Bad("invalid listpack back length");
      back = (back << 7) | (item & 0x7f);
    }
    if (back != encoded) return Bad("listpack back length mismatch");
    values.push_back(std::move(value));
  }
  if (at != input.size() - 1 ||
      (declared != UINT16_MAX && declared != values.size()))
    return Bad("listpack count mismatch");
  return values;
}

void AppendLpInteger(std::string* body, std::int64_t value) {
  const std::size_t start = body->size();
  if (value >= 0 && value <= 127) {
    body->push_back(static_cast<char>(value));
  } else if (value >= -4096 && value <= 4095) {
    const std::uint64_t encoded =
        value < 0 ? (std::uint64_t{1} << 13) + value : value;
    body->push_back(static_cast<char>(0xc0 | (encoded >> 8)));
    body->push_back(static_cast<char>(encoded));
  } else {
    body->push_back(static_cast<char>(0xf4));
    PutLe64(body, static_cast<std::uint64_t>(value));
  }
  AppendBackLength(body, body->size() - start);
}

void AppendLpString(std::string* body, std::string_view value) {
  const std::size_t start = body->size();
  if (value.size() < 64) {
    body->push_back(static_cast<char>(0x80 | value.size()));
  } else if (value.size() < 4096) {
    body->push_back(static_cast<char>(0xe0 | (value.size() >> 8)));
    body->push_back(static_cast<char>(value.size()));
  } else {
    body->push_back(static_cast<char>(0xf0));
    PutLe32(body, static_cast<std::uint32_t>(value.size()));
  }
  body->append(value);
  AppendBackLength(body, body->size() - start);
}

std::string FinishListpack(std::string body, std::size_t count) {
  std::string output;
  output.reserve(6 + body.size() + 1);
  PutLe32(&output, static_cast<std::uint32_t>(6 + body.size() + 1));
  PutLe16(&output,
          count < UINT16_MAX ? static_cast<std::uint16_t>(count) : UINT16_MAX);
  output += body;
  output.push_back(static_cast<char>(0xff));
  return output;
}

absl::StatusOr<std::vector<std::string>> DecodeZiplist(std::string_view input) {
  if (input.size() < 11 || input.size() > storage::kMaxStringBytes)
    return Bad("invalid ziplist size");
  Reader header(input);
  std::uint32_t bytes = 0, tail = 0;
  std::uint16_t count = 0;
  if (!header.Le32(&bytes) || !header.Le32(&tail) || !header.Le16(&count) ||
      bytes != input.size() || tail >= input.size() ||
      static_cast<unsigned char>(input.back()) != 0xff)
    return Bad("invalid ziplist header");
  std::size_t at = 10, previous = 0;
  std::vector<std::string> values;
  while (at < input.size() - 1) {
    const std::size_t start = at;
    std::uint32_t prev = static_cast<unsigned char>(input[at++]);
    if (prev == 254) {
      if (input.size() - 1 - at < 4) return Bad("truncated ziplist prevlen");
      prev =
          static_cast<unsigned char>(input[at]) |
          (static_cast<std::uint32_t>(static_cast<unsigned char>(input[at + 1]))
           << 8) |
          (static_cast<std::uint32_t>(static_cast<unsigned char>(input[at + 2]))
           << 16) |
          (static_cast<std::uint32_t>(static_cast<unsigned char>(input[at + 3]))
           << 24);
      at += 4;
    }
    if (prev != previous) return Bad("ziplist prevlen mismatch");
    if (at == input.size() - 1) return Bad("truncated ziplist entry");
    const std::uint8_t encoding = static_cast<std::uint8_t>(input[at++]);
    std::size_t length = 0;
    if ((encoding >> 6) == 0) {
      length = encoding & 0x3f;
    } else if ((encoding >> 6) == 1) {
      if (at == input.size() - 1) return Bad("truncated ziplist string");
      length = (static_cast<std::size_t>(encoding & 0x3f) << 8) |
               static_cast<unsigned char>(input[at++]);
    } else if (encoding == 0x80) {
      if (input.size() - 1 - at < 4) return Bad("truncated ziplist string");
      length =
          (static_cast<std::uint32_t>(static_cast<unsigned char>(input[at]))
           << 24) |
          (static_cast<std::uint32_t>(static_cast<unsigned char>(input[at + 1]))
           << 16) |
          (static_cast<std::uint32_t>(static_cast<unsigned char>(input[at + 2]))
           << 8) |
          static_cast<unsigned char>(input[at + 3]);
      at += 4;
    } else {
      unsigned width = 0;
      std::int64_t number = 0;
      if (encoding == 0xfe)
        width = 1;
      else if (encoding == 0xc0)
        width = 2;
      else if (encoding == 0xf0)
        width = 3;
      else if (encoding == 0xd0)
        width = 4;
      else if (encoding == 0xe0)
        width = 8;
      else if (encoding >= 0xf1 && encoding <= 0xfd)
        number = (encoding & 0x0f) - 1;
      else
        return Bad("unknown ziplist encoding");
      if (width != 0) {
        if (width > input.size() - 1 - at)
          return Bad("truncated ziplist integer");
        std::uint64_t raw = 0;
        for (unsigned i = 0; i < width; ++i)
          raw |= static_cast<std::uint64_t>(
                     static_cast<unsigned char>(input[at++]))
                 << (8 * i);
        const unsigned bits = width * 8;
        if ((raw & (std::uint64_t{1} << (bits - 1))) != 0 && bits < 64)
          raw |= ~std::uint64_t{0} << bits;
        number = static_cast<std::int64_t>(raw);
      }
      values.push_back(std::to_string(number));
      previous = at - start;
      continue;
    }
    if (length > input.size() - 1 - at) return Bad("truncated ziplist string");
    values.emplace_back(input.substr(at, length));
    at += length;
    previous = at - start;
  }
  if (at != input.size() - 1 || (count != UINT16_MAX && count != values.size()))
    return Bad("ziplist count mismatch");
  return values;
}

absl::StatusOr<std::vector<std::string>> DecodeIntset(std::string_view input) {
  Reader reader(input);
  std::uint32_t width = 0, count = 0;
  if (!reader.Le32(&width) || !reader.Le32(&count) ||
      (width != 2 && width != 4 && width != 8) ||
      count > reader.remaining() / width ||
      reader.remaining() != static_cast<std::size_t>(count) * width)
    return Bad("invalid intset");
  std::vector<std::string> output;
  output.reserve(count);
  std::int64_t previous = std::numeric_limits<std::int64_t>::min();
  for (std::uint32_t i = 0; i < count; ++i) {
    std::string_view bytes;
    reader.Bytes(width, &bytes);
    std::uint64_t raw = 0;
    for (unsigned j = 0; j < width; ++j)
      raw |= static_cast<std::uint64_t>(static_cast<unsigned char>(bytes[j]))
             << (8 * j);
    const unsigned bits = width * 8;
    if ((raw & (std::uint64_t{1} << (bits - 1))) != 0 && bits < 64)
      raw |= ~std::uint64_t{0} << bits;
    const auto value = static_cast<std::int64_t>(raw);
    if (i != 0 && value <= previous) return Bad("unordered intset");
    previous = value;
    output.push_back(std::to_string(value));
  }
  return output;
}

absl::StatusOr<std::vector<std::string>> DecodeZipmap(std::string_view input) {
  if (input.size() < 2) return Bad("invalid zipmap");
  std::size_t at = 1;
  auto length = [&](std::uint32_t* value) -> bool {
    if (at >= input.size()) return false;
    std::uint8_t first = static_cast<std::uint8_t>(input[at++]);
    if (first < 253) {
      *value = first;
      return true;
    }
    if (first != 253 || input.size() - at < 4) return false;
    *value =
        static_cast<unsigned char>(input[at]) |
        (static_cast<std::uint32_t>(static_cast<unsigned char>(input[at + 1]))
         << 8) |
        (static_cast<std::uint32_t>(static_cast<unsigned char>(input[at + 2]))
         << 16) |
        (static_cast<std::uint32_t>(static_cast<unsigned char>(input[at + 3]))
         << 24);
    at += 4;
    return true;
  };
  std::vector<std::string> output;
  while (at < input.size() && static_cast<unsigned char>(input[at]) != 255) {
    std::uint32_t key_size = 0, value_size = 0;
    if (!length(&key_size) || key_size > input.size() - at)
      return Bad("truncated zipmap key");
    output.emplace_back(input.substr(at, key_size));
    at += key_size;
    if (!length(&value_size) || at == input.size())
      return Bad("truncated zipmap value");
    const std::uint8_t free = static_cast<std::uint8_t>(input[at++]);
    if (value_size > input.size() - at || free > input.size() - at - value_size)
      return Bad("truncated zipmap value");
    output.emplace_back(input.substr(at, value_size));
    at += value_size + free;
  }
  if (at + 1 != input.size() || static_cast<unsigned char>(input[at]) != 255 ||
      output.empty())
    return Bad("invalid zipmap terminator");
  return output;
}

struct Id {
  std::uint64_t ms = 0, seq = 0;
  friend auto operator<=>(const Id&, const Id&) = default;
};

bool AddStreamIdDelta(std::uint64_t base, std::int64_t delta,
                      std::uint64_t* result) {
  if (delta >= 0) {
    const std::uint64_t amount = static_cast<std::uint64_t>(delta);
    if (amount > UINT64_MAX - base) return false;
    *result = base + amount;
    return true;
  }
  const std::uint64_t amount = static_cast<std::uint64_t>(-(delta + 1)) + 1;
  if (amount > base) return false;
  *result = base - amount;
  return true;
}

struct Entry {
  Id id;
  std::vector<std::string> fields;
};
struct Pending {
  Id id;
  std::string consumer;
  std::uint64_t delivery = 0, count = 1;
};
struct Consumer {
  std::string name;
  std::uint64_t seen = 0, active = 0;
};
struct Group {
  std::string name;
  Id last;
  std::int64_t entries_read = -1;
  std::vector<Consumer> consumers;
  std::vector<Pending> pending;
};
struct Stream {
  Id last, max_deleted;
  std::uint64_t entries_added = 0;
  std::vector<Entry> entries;
  std::vector<Group> groups;
};
struct ZElement {
  std::string member;
  double score = 0;
};
using Strings = std::vector<std::string>;
using Pairs = std::vector<std::pair<std::string, std::string>>;
using ZElements = std::vector<ZElement>;
using Logical = std::variant<std::string, Strings, Pairs, ZElements, Stream>;
struct LogicalValue {
  storage::ValueType type = storage::ValueType::kNone;
  Logical value;
};

absl::Status UniqueStrings(const Strings& values) {
  std::set<std::string_view> seen;
  for (const auto& value : values)
    if (!seen.insert(value).second) return Bad("duplicate member");
  return absl::OkStatus();
}
absl::Status UniquePairs(const Pairs& values) {
  std::set<std::string_view> seen;
  for (const auto& [key, value] : values) {
    (void)value;
    if (!seen.insert(key).second) return Bad("duplicate field");
  }
  return absl::OkStatus();
}

absl::StatusOr<Stream> DecodeStreamRdb(Reader* reader, std::uint8_t type) {
  auto listpack_count = ReadLength(reader);
  if (!listpack_count.ok() || listpack_count->encoded ||
      listpack_count->value > UINT32_MAX)
    return Bad("invalid Stream listpack count");
  Stream stream;
  std::set<Id> ids;
  std::set<std::string> node_keys;
  for (std::uint64_t node = 0; node < listpack_count->value; ++node) {
    auto key = ReadString(reader);
    auto blob = ReadString(reader);
    if (!key.ok()) return key.status();
    if (!blob.ok()) return blob.status();
    if (key->size() != 16 || !node_keys.insert(*key).second)
      return Bad("invalid Stream node key");
    Reader key_reader(*key);
    Id master;
    if (!key_reader.Be64(&master.ms) || !key_reader.Be64(&master.seq))
      return Bad("invalid Stream node ID");
    auto listpack = DecodeListpack(*blob);
    if (!listpack.ok()) return listpack.status();
    std::size_t at = 0;
    auto integer = [&](std::int64_t* out) {
      if (at >= listpack->size()) return false;
      auto value = (*listpack)[at++].Integer();
      if (!value) return false;
      *out = *value;
      return true;
    };
    std::int64_t live = 0, deleted = 0, master_fields = 0;
    if (!integer(&live) || !integer(&deleted) || !integer(&master_fields) ||
        live < 0 || deleted < 0 || master_fields < 0 ||
        static_cast<std::uint64_t>(master_fields) > listpack->size() - at)
      return Bad("invalid Stream listpack header");
    Strings field_names;
    for (std::int64_t i = 0; i < master_fields; ++i)
      field_names.push_back((*listpack)[at++].String());
    std::int64_t zero = 0;
    if (!integer(&zero) || zero != 0)
      return Bad("invalid Stream master terminator");
    const std::uint64_t records =
        static_cast<std::uint64_t>(live) + static_cast<std::uint64_t>(deleted);
    for (std::uint64_t record = 0; record < records; ++record) {
      std::int64_t flags = 0, ms_delta = 0, seq_delta = 0;
      Id id;
      if (!integer(&flags) || !integer(&ms_delta) || !integer(&seq_delta) ||
          flags < 0 || (flags & ~3) != 0 ||
          !AddStreamIdDelta(master.ms, ms_delta, &id.ms) ||
          !AddStreamIdDelta(master.seq, seq_delta, &id.seq))
        return Bad("invalid Stream entry header");
      Entry entry{.id = id, .fields = {}};
      std::int64_t fields = master_fields;
      const bool same_fields = (flags & 2) != 0;
      if (!same_fields &&
          (!integer(&fields) || fields < 0 ||
           static_cast<std::uint64_t>(fields) > listpack->size() - at))
        return Bad("invalid Stream field count");
      for (std::int64_t i = 0; i < fields; ++i) {
        if (!same_fields) {
          if (at >= listpack->size()) return Bad("truncated Stream field");
          entry.fields.push_back((*listpack)[at++].String());
        } else {
          entry.fields.push_back(field_names[static_cast<std::size_t>(i)]);
        }
        if (at >= listpack->size()) return Bad("truncated Stream value");
        entry.fields.push_back((*listpack)[at++].String());
      }
      std::int64_t lp_count = 0;
      const std::int64_t expected = same_fields ? fields + 3 : fields * 2 + 4;
      if (!integer(&lp_count) || lp_count != expected)
        return Bad("invalid Stream lp-count");
      if ((flags & 1) == 0) {
        if (!ids.insert(entry.id).second) return Bad("duplicate Stream ID");
        stream.entries.push_back(std::move(entry));
      }
    }
    if (at != listpack->size()) return Bad("trailing Stream listpack data");
  }
  auto length = ReadLength(reader);
  auto last_ms = ReadLength(reader);
  auto last_seq = ReadLength(reader);
  if (!length.ok() || !last_ms.ok() || !last_seq.ok() || length->encoded ||
      last_ms->encoded || last_seq->encoded)
    return Bad("invalid Stream metadata");
  stream.last = {last_ms->value, last_seq->value};
  if (type >= kStreamListpacks2) {
    for (unsigned field = 0; field < 5; ++field) {
      auto value = ReadLength(reader);
      if (!value.ok() || value->encoded) return Bad("invalid Stream metadata");
      if (field == 2)
        stream.max_deleted.ms = value->value;
      else if (field == 3)
        stream.max_deleted.seq = value->value;
      else if (field == 4)
        stream.entries_added = value->value;
    }
  } else {
    stream.entries_added = length->value;
  }
  std::sort(stream.entries.begin(), stream.entries.end(),
            [](const Entry& a, const Entry& b) { return a.id < b.id; });
  if (stream.entries.size() != length->value ||
      (!stream.entries.empty() && stream.entries.back().id > stream.last))
    return Bad("Stream length mismatch");
  auto group_count = ReadLength(reader);
  if (!group_count.ok() || group_count->encoded ||
      group_count->value > UINT32_MAX)
    return Bad("invalid Stream group count");
  std::set<std::string> group_names;
  for (std::uint64_t gi = 0; gi < group_count->value; ++gi) {
    Group group;
    auto name = ReadString(reader);
    auto ms = ReadLength(reader);
    auto seq = ReadLength(reader);
    if (!name.ok() || !ms.ok() || !seq.ok() || ms->encoded || seq->encoded ||
        !group_names.insert(*name).second)
      return Bad("invalid Stream group");
    group.name = std::move(*name);
    group.last = {ms->value, seq->value};
    if (type >= kStreamListpacks2) {
      auto read = ReadLength(reader);
      if (!read.ok() || read->encoded)
        return Bad("invalid Stream group offset");
      group.entries_read = std::bit_cast<std::int64_t>(read->value);
    }
    auto pel_count = ReadLength(reader);
    if (!pel_count.ok() || pel_count->encoded || pel_count->value > UINT32_MAX)
      return Bad("invalid Stream PEL count");
    std::map<Id, Pending> pending;
    for (std::uint64_t pi = 0; pi < pel_count->value; ++pi) {
      std::string_view raw;
      std::uint64_t delivery = 0;
      if (!reader->Bytes(16, &raw) || !reader->Le64(&delivery))
        return Bad("truncated Stream PEL");
      Reader id_reader(raw);
      Id id;
      id_reader.Be64(&id.ms);
      id_reader.Be64(&id.seq);
      auto deliveries = ReadLength(reader);
      if (!deliveries.ok() || deliveries->encoded ||
          !pending.emplace(id, Pending{id, {}, delivery, deliveries->value})
               .second)
        return Bad("duplicate Stream PEL ID");
    }
    auto consumer_count = ReadLength(reader);
    if (!consumer_count.ok() || consumer_count->encoded ||
        consumer_count->value > UINT32_MAX)
      return Bad("invalid Stream consumer count");
    std::set<std::string> consumer_names;
    for (std::uint64_t ci = 0; ci < consumer_count->value; ++ci) {
      auto cname = ReadString(reader);
      std::uint64_t seen = 0, active = 0;
      if (!cname.ok() || !reader->Le64(&seen) ||
          !consumer_names.insert(*cname).second)
        return Bad("invalid Stream consumer");
      active = seen;
      if (type >= kStreamListpacks3 && !reader->Le64(&active))
        return Bad("truncated Stream active time");
      group.consumers.push_back(Consumer{std::move(*cname), seen, active});
      auto local_count = ReadLength(reader);
      if (!local_count.ok() || local_count->encoded ||
          local_count->value > pending.size())
        return Bad("invalid Stream consumer PEL");
      for (std::uint64_t li = 0; li < local_count->value; ++li) {
        std::string_view raw;
        if (!reader->Bytes(16, &raw))
          return Bad("truncated Stream consumer PEL");
        Reader id_reader(raw);
        Id id;
        id_reader.Be64(&id.ms);
        id_reader.Be64(&id.seq);
        auto found = pending.find(id);
        if (found == pending.end() || !found->second.consumer.empty())
          return Bad("dangling Stream consumer PEL");
        found->second.consumer = group.consumers.back().name;
      }
    }
    for (auto& [id, item] : pending) {
      (void)id;
      if (item.consumer.empty()) return Bad("unowned Stream PEL entry");
      group.pending.push_back(std::move(item));
    }
    stream.groups.push_back(std::move(group));
  }
  return stream;
}

absl::StatusOr<LogicalValue> DecodeRdbObject(Reader* reader,
                                             std::uint8_t type) {
  if (type == kString) {
    auto value = ReadString(reader);
    if (!value.ok()) return value.status();
    return LogicalValue{storage::ValueType::kString, std::move(*value)};
  }
  if (type == kList || type == kSet || type == kHash || type == kZSet ||
      type == kZSet2) {
    auto count = ReadLength(reader);
    if (!count.ok() || count->encoded || count->value == 0 ||
        count->value > UINT32_MAX)
      return Bad("invalid collection length");
    if (type == kList || type == kSet) {
      Strings values;
      values.reserve(static_cast<std::size_t>(count->value));
      for (std::uint64_t i = 0; i < count->value; ++i) {
        auto value = ReadString(reader);
        if (!value.ok()) return value.status();
        values.push_back(std::move(*value));
      }
      if (type == kSet) {
        auto unique = UniqueStrings(values);
        if (!unique.ok()) return unique;
      }
      return LogicalValue{
          type == kList ? storage::ValueType::kList : storage::ValueType::kSet,
          std::move(values)};
    }
    if (type == kHash) {
      Pairs values;
      values.reserve(static_cast<std::size_t>(count->value));
      for (std::uint64_t i = 0; i < count->value; ++i) {
        auto field = ReadString(reader);
        auto value = ReadString(reader);
        if (!field.ok()) return field.status();
        if (!value.ok()) return value.status();
        values.emplace_back(std::move(*field), std::move(*value));
      }
      auto unique = UniquePairs(values);
      if (!unique.ok()) return unique;
      return LogicalValue{storage::ValueType::kHash, std::move(values)};
    }
    ZElements values;
    values.reserve(static_cast<std::size_t>(count->value));
    std::set<std::string_view> members;
    for (std::uint64_t i = 0; i < count->value; ++i) {
      auto member = ReadString(reader);
      if (!member.ok()) return member.status();
      double score = 0;
      if (type == kZSet2) {
        std::uint64_t bits = 0;
        if (!reader->Le64(&bits)) return Bad("truncated Zset score");
        score = std::bit_cast<double>(bits);
      } else {
        std::uint8_t size = 0;
        if (!reader->Byte(&size)) return Bad("truncated Zset score");
        if (size == 253)
          score = std::numeric_limits<double>::quiet_NaN();
        else if (size == 254)
          score = std::numeric_limits<double>::infinity();
        else if (size == 255)
          score = -std::numeric_limits<double>::infinity();
        else {
          std::string_view text;
          if (!reader->Bytes(size, &text)) return Bad("truncated Zset score");
          auto parsed =
              std::from_chars(text.data(), text.data() + text.size(), score);
          if (parsed.ec != std::errc{} ||
              parsed.ptr != text.data() + text.size())
            return Bad("invalid Zset score");
        }
      }
      values.push_back({std::move(*member), score});
      if (std::isnan(score) || !members.insert(values.back().member).second)
        return Bad("invalid Zset member");
    }
    return LogicalValue{storage::ValueType::kSortedSet, std::move(values)};
  }
  if (type == kListZiplist || type == kHashZiplist || type == kZSetZiplist ||
      type == kHashListpack || type == kZSetListpack || type == kSetListpack ||
      type == kSetIntset || type == kHashZipmap) {
    auto blob = ReadString(reader);
    if (!blob.ok()) return blob.status();
    Strings flat;
    if (type == kSetIntset) {
      auto decoded = DecodeIntset(*blob);
      if (!decoded.ok()) return decoded.status();
      flat = std::move(*decoded);
    } else if (type == kHashZipmap) {
      auto decoded = DecodeZipmap(*blob);
      if (!decoded.ok()) return decoded.status();
      flat = std::move(*decoded);
    } else if (type == kListZiplist || type == kHashZiplist ||
               type == kZSetZiplist) {
      auto decoded = DecodeZiplist(*blob);
      if (!decoded.ok()) return decoded.status();
      flat = std::move(*decoded);
    } else {
      auto decoded = DecodeListpack(*blob);
      if (!decoded.ok()) return decoded.status();
      for (auto& item : *decoded) flat.push_back(item.String());
    }
    if (flat.empty()) return Bad("empty collection");
    if (type == kListZiplist)
      return LogicalValue{storage::ValueType::kList, std::move(flat)};
    if (type == kSetIntset || type == kSetListpack) {
      auto unique = UniqueStrings(flat);
      if (!unique.ok()) return unique;
      return LogicalValue{storage::ValueType::kSet, std::move(flat)};
    }
    if (flat.size() % 2 != 0) return Bad("odd collection pair count");
    if (type == kHashZipmap || type == kHashZiplist || type == kHashListpack) {
      Pairs pairs;
      for (std::size_t i = 0; i < flat.size(); i += 2)
        pairs.emplace_back(std::move(flat[i]), std::move(flat[i + 1]));
      auto unique = UniquePairs(pairs);
      if (!unique.ok()) return unique;
      return LogicalValue{storage::ValueType::kHash, std::move(pairs)};
    }
    ZElements values;
    std::set<std::string> members;
    for (std::size_t i = 0; i < flat.size(); i += 2) {
      double score = 0;
      auto parsed = std::from_chars(
          flat[i + 1].data(), flat[i + 1].data() + flat[i + 1].size(), score);
      if (parsed.ec != std::errc{} ||
          parsed.ptr != flat[i + 1].data() + flat[i + 1].size() ||
          std::isnan(score) || !members.insert(flat[i]).second)
        return Bad("invalid packed Zset");
      values.push_back({std::move(flat[i]), score});
    }
    return LogicalValue{storage::ValueType::kSortedSet, std::move(values)};
  }
  if (type == kListQuicklist || type == kListQuicklist2) {
    auto count = ReadLength(reader);
    if (!count.ok() || count->encoded || count->value == 0 ||
        count->value > UINT32_MAX)
      return Bad("invalid quicklist length");
    Strings values;
    for (std::uint64_t i = 0; i < count->value; ++i) {
      std::uint64_t container = 2;
      if (type == kListQuicklist2) {
        auto encoded = ReadLength(reader);
        if (!encoded.ok() || encoded->encoded ||
            (encoded->value != 1 && encoded->value != 2))
          return Bad("invalid quicklist container");
        container = encoded->value;
      }
      auto blob = ReadString(reader);
      if (!blob.ok()) return blob.status();
      if (container == 1)
        values.push_back(std::move(*blob));
      else if (type == kListQuicklist) {
        auto node = DecodeZiplist(*blob);
        if (!node.ok()) return node.status();
        values.insert(values.end(), std::make_move_iterator(node->begin()),
                      std::make_move_iterator(node->end()));
      } else {
        auto node = DecodeListpack(*blob);
        if (!node.ok()) return node.status();
        for (auto& item : *node) values.push_back(item.String());
      }
    }
    if (values.empty()) return Bad("empty quicklist");
    return LogicalValue{storage::ValueType::kList, std::move(values)};
  }
  if (type == kStreamListpacks || type == kStreamListpacks2 ||
      type == kStreamListpacks3) {
    auto stream = DecodeStreamRdb(reader, type);
    if (!stream.ok()) return stream.status();
    return LogicalValue{storage::ValueType::kStream, std::move(*stream)};
  }
  if (type == kModulePreGa || type == kModule2)
    return Bad("Redis Module values are unsupported");
  return Bad("unknown object type");
}

absl::StatusOr<LogicalValue> DecodeRaw(const storage::RawValue& raw) {
  const std::string_view input = raw.encoded_;
  if (raw.value_type_ == storage::ValueType::kString)
    return LogicalValue{raw.value_type_, std::string(input)};
  if (raw.value_type_ == storage::ValueType::kList) {
    if (!input.starts_with(kListMagic) || input.size() < 8)
      return Bad("invalid Keylane List");
    Reader reader(input.substr(4));
    std::uint32_t count = 0;
    if (!reader.Le32(&count) || count != raw.logical_size_ || count == 0)
      return Bad("invalid Keylane List count");
    Strings values;
    values.reserve(count);
    for (std::uint32_t i = 0; i < count; ++i) {
      std::uint32_t size = 0;
      std::string_view value;
      if (!reader.Le32(&size) || !reader.Bytes(size, &value))
        return Bad("truncated Keylane List");
      values.emplace_back(value);
    }
    if (!reader.done()) return Bad("trailing Keylane List data");
    return LogicalValue{raw.value_type_, std::move(values)};
  }
  if (raw.value_type_ == storage::ValueType::kSet ||
      raw.value_type_ == storage::ValueType::kHash) {
    if (input.size() < 32) return Bad("invalid Keylane Hash");
    Reader reader(input);
    std::uint64_t magic = 0, bytes = 0;
    std::uint32_t version = 0, header = 0, count = 0, reserved = 0;
    if (!reader.Le64(&magic) || !reader.Le32(&version) ||
        !reader.Le32(&header) || !reader.Le32(&count) ||
        !reader.Le32(&reserved) || !reader.Le64(&bytes) ||
        magic != storage::kHashValueMagic ||
        version != storage::kStorageFormatVersion || header != 32 ||
        reserved != 0 || bytes != input.size() || count == 0 ||
        count != raw.logical_size_)
      return Bad("invalid Keylane Hash header");
    Pairs pairs;
    pairs.reserve(count);
    for (std::uint32_t i = 0; i < count; ++i) {
      std::string_view digest_bytes, field, value;
      std::uint32_t fs = 0, vs = 0;
      if (!reader.Bytes(sizeof(storage::Digest), &digest_bytes) ||
          !reader.Le32(&fs) || !reader.Le32(&vs) || !reader.Bytes(fs, &field) ||
          !reader.Bytes(vs, &value))
        return Bad("truncated Keylane Hash");
      storage::Digest digest{};
      std::memcpy(&digest, digest_bytes.data(), sizeof(digest));
      if (digest != storage::ComputeDigest(field))
        return Bad("invalid Keylane Hash digest");
      pairs.emplace_back(std::string(field), std::string(value));
    }
    if (!reader.done()) return Bad("trailing Keylane Hash data");
    if (raw.value_type_ == storage::ValueType::kSet) {
      Strings values;
      values.reserve(pairs.size());
      for (auto& [member, unused] : pairs) {
        if (!unused.empty()) return Bad("invalid Keylane Set value");
        values.push_back(std::move(member));
      }
      return LogicalValue{raw.value_type_, std::move(values)};
    }
    return LogicalValue{raw.value_type_, std::move(pairs)};
  }
  if (raw.value_type_ == storage::ValueType::kSortedSet) {
    if (!input.starts_with(kZSetMagic) || input.size() < 8)
      return Bad("invalid Keylane Zset");
    Reader reader(input.substr(4));
    std::uint32_t count = 0;
    if (!reader.Le32(&count) || count != raw.logical_size_ || count == 0)
      return Bad("invalid Keylane Zset count");
    ZElements values;
    values.reserve(count);
    for (std::uint32_t i = 0; i < count; ++i) {
      std::uint64_t bits = 0;
      std::uint32_t size = 0;
      std::string_view member;
      if (!reader.Le64(&bits) || !reader.Le32(&size) ||
          !reader.Bytes(size, &member))
        return Bad("truncated Keylane Zset");
      double score = std::bit_cast<double>(bits);
      if (std::isnan(score)) return Bad("NaN Keylane Zset score");
      values.push_back({std::string(member), score});
    }
    if (!reader.done()) return Bad("trailing Keylane Zset data");
    return LogicalValue{raw.value_type_, std::move(values)};
  }
  if (raw.value_type_ == storage::ValueType::kStream) {
    if (!input.starts_with(kStreamMagic) || input.size() < 48)
      return Bad("invalid Keylane Stream");
    Reader reader(input.substr(4));
    Stream stream;
    std::uint32_t entries = 0, groups = 0;
    if (!reader.Le64(&stream.last.ms) || !reader.Le64(&stream.last.seq) ||
        !reader.Le64(&stream.max_deleted.ms) ||
        !reader.Le64(&stream.max_deleted.seq) ||
        !reader.Le64(&stream.entries_added) || !reader.Le32(&entries) ||
        entries != raw.logical_size_)
      return Bad("invalid Keylane Stream header");
    auto read_text = [&](std::string* value) {
      std::uint32_t size = 0;
      std::string_view text;
      if (!reader.Le32(&size) || !reader.Bytes(size, &text)) return false;
      value->assign(text);
      return true;
    };
    for (std::uint32_t i = 0; i < entries; ++i) {
      Entry entry;
      std::uint32_t fields = 0;
      if (!reader.Le64(&entry.id.ms) || !reader.Le64(&entry.id.seq) ||
          !reader.Le32(&fields) || fields % 2)
        return Bad("invalid Keylane Stream entry");
      for (std::uint32_t f = 0; f < fields; ++f) {
        std::string text;
        if (!read_text(&text)) return Bad("truncated Keylane Stream field");
        entry.fields.push_back(std::move(text));
      }
      stream.entries.push_back(std::move(entry));
    }
    if (!reader.Le32(&groups)) return Bad("truncated Keylane Stream groups");
    for (std::uint32_t i = 0; i < groups; ++i) {
      Group group;
      std::uint64_t read_bits = 0;
      std::uint32_t consumers = 0, pending = 0;
      if (!read_text(&group.name) || !reader.Le64(&group.last.ms) ||
          !reader.Le64(&group.last.seq) || !reader.Le64(&read_bits) ||
          !reader.Le32(&consumers))
        return Bad("truncated Keylane Stream group");
      group.entries_read = std::bit_cast<std::int64_t>(read_bits);
      for (std::uint32_t c = 0; c < consumers; ++c) {
        Consumer consumer;
        if (!read_text(&consumer.name) || !reader.Le64(&consumer.seen) ||
            !reader.Le64(&consumer.active))
          return Bad("truncated Keylane Stream consumer");
        group.consumers.push_back(std::move(consumer));
      }
      if (!reader.Le32(&pending)) return Bad("truncated Keylane Stream PEL");
      for (std::uint32_t p = 0; p < pending; ++p) {
        Pending item;
        if (!reader.Le64(&item.id.ms) || !reader.Le64(&item.id.seq) ||
            !read_text(&item.consumer) || !reader.Le64(&item.delivery) ||
            !reader.Le64(&item.count))
          return Bad("truncated Keylane Stream PEL");
        group.pending.push_back(std::move(item));
      }
      stream.groups.push_back(std::move(group));
    }
    if (!reader.done()) return Bad("trailing Keylane Stream data");
    return LogicalValue{raw.value_type_, std::move(stream)};
  }
  return Bad("unsupported Keylane type");
}

void AppendHashRaw(std::string* output, const Pairs& pairs) {
  std::uint64_t bytes = 32;
  for (const auto& [field, value] : pairs)
    bytes += sizeof(storage::Digest) + 8 + field.size() + value.size();
  PutLe64(output, storage::kHashValueMagic);
  PutLe32(output, storage::kStorageFormatVersion);
  PutLe32(output, 32);
  PutLe32(output, static_cast<std::uint32_t>(pairs.size()));
  PutLe32(output, 0);
  PutLe64(output, bytes);
  for (const auto& [field, value] : pairs) {
    const storage::Digest digest = storage::ComputeDigest(field);
    output->append(reinterpret_cast<const char*>(&digest), sizeof(digest));
    PutLe32(output, static_cast<std::uint32_t>(field.size()));
    PutLe32(output, static_cast<std::uint32_t>(value.size()));
    output->append(field);
    output->append(value);
  }
}

absl::StatusOr<storage::RawValue> EncodeRaw(LogicalValue logical) {
  auto add_size = [](std::uint64_t* total, std::uint64_t amount) {
    if (amount > storage::kMaxStringBytes - *total) return false;
    *total += amount;
    return true;
  };
  auto text_size = [&](std::uint64_t* total, std::string_view value) {
    return value.size() <= UINT32_MAX && add_size(total, 4 + value.size());
  };

  storage::RawValue raw;
  raw.value_type_ = logical.type;
  if (logical.type == storage::ValueType::kString) {
    raw.encoded_ = std::move(std::get<std::string>(logical.value));
    raw.logical_size_ = raw.encoded_.size();
  } else if (logical.type == storage::ValueType::kList) {
    auto values = std::move(std::get<Strings>(logical.value));
    std::uint64_t bytes = 8;
    for (const auto& value : values) {
      if (!text_size(&bytes, value)) return Bad("value exceeds Keylane limits");
    }
    raw.logical_size_ = values.size();
    raw.encoded_ = std::string(kListMagic);
    raw.encoded_.reserve(bytes);
    PutLe32(&raw.encoded_, static_cast<std::uint32_t>(values.size()));
    for (const auto& value : values) {
      PutLe32(&raw.encoded_, static_cast<std::uint32_t>(value.size()));
      raw.encoded_ += value;
    }
  } else if (logical.type == storage::ValueType::kSet) {
    auto values = std::move(std::get<Strings>(logical.value));
    std::sort(values.begin(), values.end());
    Pairs pairs;
    for (auto& value : values)
      pairs.emplace_back(std::move(value), std::string());
    std::uint64_t bytes = 32;
    for (const auto& [field, value] : pairs) {
      if (!add_size(&bytes, sizeof(storage::Digest) + 8) ||
          !add_size(&bytes, field.size()) || !add_size(&bytes, value.size()) ||
          field.size() > UINT32_MAX || value.size() > UINT32_MAX)
        return Bad("value exceeds Keylane limits");
    }
    raw.logical_size_ = pairs.size();
    raw.encoded_.reserve(bytes);
    AppendHashRaw(&raw.encoded_, pairs);
  } else if (logical.type == storage::ValueType::kHash) {
    auto pairs = std::move(std::get<Pairs>(logical.value));
    std::sort(pairs.begin(), pairs.end());
    std::uint64_t bytes = 32;
    for (const auto& [field, value] : pairs) {
      if (!add_size(&bytes, sizeof(storage::Digest) + 8) ||
          !add_size(&bytes, field.size()) || !add_size(&bytes, value.size()) ||
          field.size() > UINT32_MAX || value.size() > UINT32_MAX)
        return Bad("value exceeds Keylane limits");
    }
    raw.logical_size_ = pairs.size();
    raw.encoded_.reserve(bytes);
    AppendHashRaw(&raw.encoded_, pairs);
  } else if (logical.type == storage::ValueType::kSortedSet) {
    auto values = std::move(std::get<ZElements>(logical.value));
    std::sort(values.begin(), values.end(),
              [](const ZElement& a, const ZElement& b) {
                return a.score < b.score ||
                       (a.score == b.score && a.member < b.member);
              });
    std::uint64_t bytes = 8;
    for (const auto& value : values) {
      if (!add_size(&bytes, 12) || !add_size(&bytes, value.member.size()) ||
          value.member.size() > UINT32_MAX)
        return Bad("value exceeds Keylane limits");
    }
    raw.logical_size_ = values.size();
    raw.encoded_ = std::string(kZSetMagic);
    raw.encoded_.reserve(bytes);
    PutLe32(&raw.encoded_, values.size());
    for (const auto& value : values) {
      PutLe64(&raw.encoded_, std::bit_cast<std::uint64_t>(value.score));
      PutLe32(&raw.encoded_, value.member.size());
      raw.encoded_ += value.member;
    }
  } else if (logical.type == storage::ValueType::kStream) {
    auto stream = std::move(std::get<Stream>(logical.value));
    std::uint64_t bytes = 48;
    for (const auto& entry : stream.entries) {
      if (!add_size(&bytes, 20) || entry.fields.size() > UINT32_MAX)
        return Bad("value exceeds Keylane limits");
      for (const auto& field : entry.fields) {
        if (!text_size(&bytes, field))
          return Bad("value exceeds Keylane limits");
      }
    }
    if (!add_size(&bytes, 4)) return Bad("value exceeds Keylane limits");
    for (const auto& group : stream.groups) {
      if (!text_size(&bytes, group.name) || !add_size(&bytes, 28) ||
          group.consumers.size() > UINT32_MAX ||
          group.pending.size() > UINT32_MAX)
        return Bad("value exceeds Keylane limits");
      for (const auto& consumer : group.consumers) {
        if (!text_size(&bytes, consumer.name) || !add_size(&bytes, 16))
          return Bad("value exceeds Keylane limits");
      }
      if (!add_size(&bytes, 4)) return Bad("value exceeds Keylane limits");
      for (const auto& pending : group.pending) {
        if (!add_size(&bytes, 16) || !text_size(&bytes, pending.consumer) ||
            !add_size(&bytes, 16))
          return Bad("value exceeds Keylane limits");
      }
    }
    raw.logical_size_ = stream.entries.size();
    raw.encoded_ = std::string(kStreamMagic);
    raw.encoded_.reserve(bytes);
    PutLe64(&raw.encoded_, stream.last.ms);
    PutLe64(&raw.encoded_, stream.last.seq);
    PutLe64(&raw.encoded_, stream.max_deleted.ms);
    PutLe64(&raw.encoded_, stream.max_deleted.seq);
    PutLe64(&raw.encoded_, stream.entries_added);
    PutLe32(&raw.encoded_, stream.entries.size());
    auto put_text = [&](std::string_view text) {
      PutLe32(&raw.encoded_, text.size());
      raw.encoded_.append(text);
    };
    for (const auto& entry : stream.entries) {
      PutLe64(&raw.encoded_, entry.id.ms);
      PutLe64(&raw.encoded_, entry.id.seq);
      PutLe32(&raw.encoded_, entry.fields.size());
      for (const auto& field : entry.fields) put_text(field);
    }
    PutLe32(&raw.encoded_, stream.groups.size());
    for (const auto& group : stream.groups) {
      put_text(group.name);
      PutLe64(&raw.encoded_, group.last.ms);
      PutLe64(&raw.encoded_, group.last.seq);
      PutLe64(&raw.encoded_, std::bit_cast<std::uint64_t>(group.entries_read));
      PutLe32(&raw.encoded_, group.consumers.size());
      for (const auto& consumer : group.consumers) {
        put_text(consumer.name);
        PutLe64(&raw.encoded_, consumer.seen);
        PutLe64(&raw.encoded_, consumer.active);
      }
      PutLe32(&raw.encoded_, group.pending.size());
      for (const auto& pending : group.pending) {
        PutLe64(&raw.encoded_, pending.id.ms);
        PutLe64(&raw.encoded_, pending.id.seq);
        put_text(pending.consumer);
        PutLe64(&raw.encoded_, pending.delivery);
        PutLe64(&raw.encoded_, pending.count);
      }
    }
  }
  if (raw.encoded_.size() > storage::kMaxStringBytes ||
      raw.logical_size_ > UINT32_MAX)
    return Bad("value exceeds Keylane limits");
  return raw;
}

void EncodeStreamRdb(std::string* out, const Stream& stream) {
  WriteLength(out, stream.entries.size());
  for (const auto& entry : stream.entries) {
    std::string key;
    PutBe64(&key, entry.id.ms);
    PutBe64(&key, entry.id.seq);
    WriteString(out, key);
    std::string body;
    const std::size_t fields = entry.fields.size() / 2;
    AppendLpInteger(&body, 1);
    AppendLpInteger(&body, 0);
    AppendLpInteger(&body, fields);
    for (std::size_t i = 0; i < fields; ++i)
      AppendLpString(&body, entry.fields[i * 2]);
    AppendLpInteger(&body, 0);
    AppendLpInteger(&body, 2);
    AppendLpInteger(&body, 0);
    AppendLpInteger(&body, 0);
    for (std::size_t i = 0; i < fields; ++i)
      AppendLpString(&body, entry.fields[i * 2 + 1]);
    AppendLpInteger(&body, fields + 3);
    const std::string listpack =
        FinishListpack(std::move(body), 8 + fields * 2);
    WriteString(out, listpack);
  }
  WriteLength(out, stream.entries.size());
  WriteLength(out, stream.last.ms);
  WriteLength(out, stream.last.seq);
  const Id first = stream.entries.empty() ? Id{} : stream.entries.front().id;
  WriteLength(out, first.ms);
  WriteLength(out, first.seq);
  WriteLength(out, stream.max_deleted.ms);
  WriteLength(out, stream.max_deleted.seq);
  WriteLength(out, stream.entries_added);
  WriteLength(out, stream.groups.size());
  for (const auto& group : stream.groups) {
    WriteString(out, group.name);
    WriteLength(out, group.last.ms);
    WriteLength(out, group.last.seq);
    WriteLength(out, std::bit_cast<std::uint64_t>(group.entries_read));
    WriteLength(out, group.pending.size());
    for (const auto& pending : group.pending) {
      PutBe64(out, pending.id.ms);
      PutBe64(out, pending.id.seq);
      PutLe64(out, pending.delivery);
      WriteLength(out, pending.count);
    }
    WriteLength(out, group.consumers.size());
    for (const auto& consumer : group.consumers) {
      WriteString(out, consumer.name);
      PutLe64(out, consumer.seen);
      PutLe64(out, consumer.active);
      std::vector<Id> ids;
      for (const auto& pending : group.pending)
        if (pending.consumer == consumer.name) ids.push_back(pending.id);
      WriteLength(out, ids.size());
      for (Id id : ids) {
        PutBe64(out, id.ms);
        PutBe64(out, id.seq);
      }
    }
  }
}

absl::StatusOr<std::string> EncodeRdbObject(const LogicalValue& logical) {
  std::string out;
  if (logical.type == storage::ValueType::kString) {
    out.push_back(kString);
    WriteString(&out, std::get<std::string>(logical.value));
  } else if (logical.type == storage::ValueType::kList ||
             logical.type == storage::ValueType::kSet) {
    out.push_back(logical.type == storage::ValueType::kList ? kList : kSet);
    auto values = std::get<Strings>(logical.value);
    if (logical.type == storage::ValueType::kSet)
      std::sort(values.begin(), values.end());
    WriteLength(&out, values.size());
    for (const auto& value : values) WriteString(&out, value);
  } else if (logical.type == storage::ValueType::kHash) {
    out.push_back(kHash);
    auto values = std::get<Pairs>(logical.value);
    std::sort(values.begin(), values.end());
    WriteLength(&out, values.size());
    for (const auto& [field, value] : values) {
      WriteString(&out, field);
      WriteString(&out, value);
    }
  } else if (logical.type == storage::ValueType::kSortedSet) {
    out.push_back(kZSet2);
    auto values = std::get<ZElements>(logical.value);
    std::sort(values.begin(), values.end(),
              [](const ZElement& a, const ZElement& b) {
                return a.score > b.score ||
                       (a.score == b.score && a.member > b.member);
              });
    WriteLength(&out, values.size());
    for (const auto& value : values) {
      WriteString(&out, value.member);
      PutLe64(&out, std::bit_cast<std::uint64_t>(value.score));
    }
  } else if (logical.type == storage::ValueType::kStream) {
    out.push_back(kStreamListpacks3);
    EncodeStreamRdb(&out, std::get<Stream>(logical.value));
  } else
    return Bad("unsupported Keylane type");
  PutLe16(&out, kVersion);
  PutLe64(&out, Crc64(out));
  return out;
}

}  // namespace

absl::StatusOr<std::string> EncodeDump(const storage::RawValue& value) {
  auto logical = DecodeRaw(value);
  if (!logical.ok()) return logical.status();
  return EncodeRdbObject(*logical);
}

absl::StatusOr<storage::RawValue> DecodeDump(std::string_view payload) {
  if (payload.size() < 10) {
    return absl::InvalidArgumentError(
        "DUMP payload version or checksum are wrong");
  }
  if (payload.size() > storage::kMaxStringBytes + 32) {
    return Bad("payload size");
  }
  Reader footer(payload.substr(payload.size() - 10));
  std::uint16_t version = 0;
  std::uint64_t expected = 0;
  if (!footer.Le16(&version) || !footer.Le64(&expected) || version > kVersion ||
      Crc64(payload.substr(0, payload.size() - 8)) != expected)
    return absl::InvalidArgumentError(
        "DUMP payload version or checksum are wrong");
  Reader reader(payload.substr(0, payload.size() - 10));
  std::uint8_t type = 0;
  if (!reader.Byte(&type)) return Bad();
  auto logical = DecodeRdbObject(&reader, type);
  if (!logical.ok()) return logical.status();
  if (!reader.done()) return Bad("trailing object data");
  return EncodeRaw(std::move(*logical));
}

}  // namespace keylane::rdb
