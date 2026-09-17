#include "lavik/rdb.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cassert>
#include <cerrno>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <map>
#include <memory>
#include <new>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "absl/strings/str_cat.h"
#include "lavik/memory.h"
#include "lavik/storage/format.h"

namespace lavik::rdb {
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

constexpr std::uint8_t kFunction2 = 245;
constexpr std::uint8_t kFunctionPreGa = 246;
constexpr std::uint8_t kModuleAux = 247;
constexpr std::uint8_t kIdle = 248;
constexpr std::uint8_t kFreq = 249;
constexpr std::uint8_t kAux = 250;
constexpr std::uint8_t kResizeDb = 251;
constexpr std::uint8_t kExpireTimeMs = 252;
constexpr std::uint8_t kExpireTime = 253;
constexpr std::uint8_t kSelectDb = 254;
constexpr std::uint8_t kEof = 255;

constexpr std::uint64_t kCrcPolynomial = 0xad93d23594c935a9ULL;
constexpr std::string_view kListMagic = "LVL1";

constexpr std::string_view kZSetMagic = "LZS1";
// Lavik's v1 Stream layout includes macro-node counts; it is independent of
// the standard Redis RDB version and has no legacy development decoder.
constexpr std::string_view kStreamMagic = "LXS1";
constexpr std::uint32_t kDefaultStreamNodeMaxEntries = 100;

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

std::uint64_t UpdateCrc64(std::uint64_t crc, std::string_view input) {
  for (unsigned char byte : input) {
    for (unsigned mask = 1; mask <= 0x80; mask <<= 1) {
      bool high = (crc & (std::uint64_t{1} << 63)) != 0;
      if ((byte & mask) != 0) high = !high;
      crc <<= 1;
      if (high) crc ^= kCrcPolynomial;
    }
  }
  return crc;
}

std::uint64_t Crc64(std::string_view input) {
  return Reflect64(UpdateCrc64(0, input));
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

// All file cursors borrow this one cache. Measurement and duplicate checks
// retain offsets, never cache views, so revisiting old input cannot pin a
// growing set of buffers. pread also leaves each copied cursor independent.
// The file must remain immutable throughout validation and application.
class FileInput {
 public:
  FileInput() = default;
  FileInput(const FileInput&) = delete;
  FileInput& operator=(const FileInput&) = delete;
  ~FileInput() {
    if (fd_ >= 0) ::close(fd_);
  }

  absl::Status Open(const std::string& path) {
    fd_ = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd_ < 0) {
      const int error = errno;
      const std::string message = absl::StrCat("cannot open RDB file '", path,
                                               "': ", std::strerror(error));
      return error == ENOENT ? absl::NotFoundError(message)
                             : absl::InternalError(message);
    }
    struct stat info{};
    if (::fstat(fd_, &info) != 0) {
      const int error = errno;
      return absl::InternalError(absl::StrCat("cannot stat RDB file '", path,
                                              "': ", std::strerror(error)));
    }
    if (!S_ISREG(info.st_mode) || info.st_size <= 0 ||
        static_cast<std::uintmax_t>(info.st_size) >
            std::numeric_limits<std::size_t>::max()) {
      return absl::InvalidArgumentError(absl::StrCat(
          "RDB path is not a nonempty regular file: '", path, "'"));
    }
    size_ = static_cast<std::size_t>(info.st_size);
    return absl::OkStatus();
  }

  std::size_t size() const { return size_; }
  const absl::Status& status() const { return status_; }
  void Reset() {
    cached_bytes_ = 0;
    status_ = absl::OkStatus();
  }

  // The returned span is valid only until another offset is requested. No
  // parser-facing API exposes it; scalar and string reads copy before refill.
  std::string_view Chunk(std::size_t at) {
    if (!status_.ok() || at >= size_) return {};
    if (at < cached_at_ || at - cached_at_ >= cached_bytes_) {
      cached_at_ = at / buffer_.size() * buffer_.size();
      cached_bytes_ = 0;
      const auto bytes = std::min(buffer_.size(), size_ - cached_at_);
      while (cached_bytes_ < bytes) {
        const ssize_t count =
            ::pread(fd_, buffer_.data() + cached_bytes_, bytes - cached_bytes_,
                    static_cast<off_t>(cached_at_ + cached_bytes_));
        if (count < 0) {
          const int error = errno;
          if (error == EINTR) continue;
          status_ = absl::InternalError(absl::StrCat(
              "cannot read RDB file at offset ", cached_at_ + cached_bytes_,
              ": ", std::strerror(error)));
          return {};
        }
        if (count == 0) {
          status_ = Bad("RDB file was truncated while reading");
          return {};
        }
        cached_bytes_ += static_cast<std::size_t>(count);
      }
    }
    const auto offset = at - cached_at_;
    return {buffer_.data() + offset, cached_bytes_ - offset};
  }

  bool Read(std::size_t at, std::size_t size, char* output) {
    if (!status_.ok()) return false;
    while (size != 0) {
      const auto part = Chunk(at).substr(0, size);
      if (part.empty()) return false;
      std::memcpy(output, part.data(), part.size());
      at += part.size();
      output += part.size();
      size -= part.size();
    }
    return true;
  }

 private:
  int fd_ = -1;
  std::size_t size_ = 0;
  // Fixed per-reader I/O scratch, independent of file and value sizes.
  std::array<char, 1024 * 1024> buffer_;
  std::size_t cached_at_ = 0;
  std::size_t cached_bytes_ = 0;
  absl::Status status_;
};

class Reader {
 public:
  explicit Reader(std::string_view input) : input_(input) {}
  explicit Reader(FileInput* file, std::size_t at = 0) : file_(file), at_(at) {}
  std::size_t remaining() const {
    return (file_ ? file_->size() : input_.size()) - at_;
  }
  bool done() const { return remaining() == 0; }
  bool AccountExpanded(std::uint64_t bytes) {
    if (bytes > storage::kMaxStringBytes - expanded_bytes_) return false;
    expanded_bytes_ += bytes;
    return true;
  }
  void ResetExpandedAccounting() { expanded_bytes_ = 0; }

  bool Byte(std::uint8_t* value) {
    return Read(1, reinterpret_cast<char*>(value));
  }
  bool Read(std::size_t size, char* value) {
    if (size > remaining()) return false;
    if (file_) {
      if (!file_->Read(at_, size, value)) return false;
    } else if (size != 0) {
      std::memcpy(value, input_.data() + at_, size);
    }
    at_ += size;
    return true;
  }
  bool Skip(std::size_t size) {
    if (size > remaining()) return false;
    at_ += size;
    return true;
  }
  // Only packed, memory-backed inputs can lend stable views across reads.
  bool View(std::size_t size, std::string_view* value) {
    assert(file_ == nullptr);
    if (size > remaining()) return false;
    *value = input_.substr(at_, size);
    at_ += size;
    return true;
  }
  bool Le16(std::uint16_t* value) {
    char bytes[2];
    if (!Read(sizeof(bytes), bytes)) return false;
    *value =
        static_cast<unsigned char>(bytes[0]) |
        (static_cast<std::uint16_t>(static_cast<unsigned char>(bytes[1])) << 8);
    return true;
  }
  bool Le32(std::uint32_t* value) {
    char bytes[4];
    if (!Read(sizeof(bytes), bytes)) return false;
    *value = 0;
    for (unsigned i = 0; i < 4; ++i)
      *value |= static_cast<std::uint32_t>(static_cast<unsigned char>(bytes[i]))
                << (8 * i);
    return true;
  }
  bool Le64(std::uint64_t* value) {
    char bytes[8];
    if (!Read(sizeof(bytes), bytes)) return false;
    *value = 0;
    for (unsigned i = 0; i < 8; ++i)
      *value |= static_cast<std::uint64_t>(static_cast<unsigned char>(bytes[i]))
                << (8 * i);
    return true;
  }
  bool Be32(std::uint32_t* value) {
    char bytes[4];
    if (!Read(sizeof(bytes), bytes)) return false;
    *value = 0;
    for (unsigned char byte : bytes) *value = (*value << 8) | byte;
    return true;
  }
  bool Be64(std::uint64_t* value) {
    char bytes[8];
    if (!Read(sizeof(bytes), bytes)) return false;
    *value = 0;
    for (unsigned char byte : bytes) *value = (*value << 8) | byte;
    return true;
  }

 private:
  std::string_view input_;
  FileInput* file_ = nullptr;
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

// Decode compressed input directly into the admitted output. File reads may
// cross any cache boundary, without retaining a second compressed blob.
bool LzfDecompress(Reader* input, std::size_t remaining, std::string* output) {
  auto byte = [&](std::uint8_t* value) {
    if (remaining == 0) return false;
    --remaining;
    return input->Byte(value);
  };
  std::size_t op = 0;
  while (remaining != 0) {
    std::uint8_t ctrl = 0;
    if (!byte(&ctrl)) return false;
    if (ctrl < 32) {
      const std::size_t length = ctrl + 1;
      if (length > remaining || length > output->size() - op) return false;
      if (!input->Read(length, output->data() + op)) return false;
      remaining -= length;
      op += length;
      continue;
    }
    std::size_t length = ctrl >> 5;
    std::size_t distance = (ctrl & 0x1f) << 8;
    std::uint8_t extra = 0;
    if (length == 7) {
      if (!byte(&extra)) return false;
      length += extra;
    }
    if (!byte(&extra)) return false;
    distance += extra + 1;
    length += 2;
    if (distance > op || length > output->size() - op) return false;
    for (std::size_t i = 0; i < length; ++i)
      (*output)[op + i] = (*output)[op - distance + i];
    op += length;
  }
  return op == output->size();
}

// RDB strings become owned values at this boundary. One catch covers plain,
// integer-expanded, and decompressed representations without adding exception
// handling to each string operation.
absl::StatusOr<std::string> ReadString(Reader* reader) try {
  auto length = ReadLength(reader);
  if (!length.ok()) return length.status();
  if (!length->encoded) {
    if (length->value > storage::kMaxStringBytes ||
        length->value > reader->remaining())
      return Bad("string length exceeds payload");
    if (!reader->AccountExpanded(length->value)) {
      return Bad("expanded value exceeds Lavik limits");
    }
    std::string value(static_cast<std::size_t>(length->value), '\0');
    if (!reader->Read(value.size(), value.data()))
      return Bad("truncated string");
    return value;
  }
  if (length->value <= 2) {
    const unsigned bytes = length->value == 0 ? 1 : length->value == 1 ? 2 : 4;
    char encoded[4];
    if (!reader->Read(bytes, encoded)) return Bad("truncated integer string");
    std::uint32_t raw = 0;
    for (unsigned i = 0; i < bytes; ++i)
      raw |= static_cast<std::uint32_t>(static_cast<unsigned char>(encoded[i]))
             << (8 * i);
    std::int64_t value = bytes == 1   ? static_cast<std::int8_t>(raw)
                         : bytes == 2 ? static_cast<std::int16_t>(raw)
                                      : static_cast<std::int32_t>(raw);
    std::string output = std::to_string(value);
    if (!reader->AccountExpanded(output.size())) {
      return Bad("expanded value exceeds Lavik limits");
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
    return Bad("expanded value exceeds Lavik limits");
  }
  std::string output(static_cast<std::size_t>(output_size->value), '\0');
  if (!LzfDecompress(reader, static_cast<std::size_t>(compressed_size->value),
                     &output))
    return Bad("invalid LZF data");
  return output;
} catch (const std::length_error&) {
  return absl::ResourceExhaustedError("RDB string is too large");
}

absl::Status SkipModuleBody(Reader* reader) {
  while (true) {
    auto opcode = ReadLength(reader);
    if (!opcode.ok()) return opcode.status();
    if (opcode->encoded) return Bad("encoded Redis Module opcode");
    if (opcode->value == 0) return absl::OkStatus();
    if (opcode->value == 1 || opcode->value == 2) {
      auto value = ReadLength(reader);
      if (!value.ok()) return value.status();
      if (value->encoded) return Bad("encoded Redis Module integer");
      continue;
    }
    if (opcode->value == 3 || opcode->value == 4) {
      const std::size_t bytes = opcode->value == 3 ? 4 : 8;
      if (!reader->Skip(bytes)) {
        return Bad("truncated Redis Module floating-point value");
      }
      continue;
    }
    if (opcode->value == 5) {
      reader->ResetExpandedAccounting();
      auto value = ReadString(reader);
      if (!value.ok()) return value.status();
      continue;
    }
    return Bad("unknown Redis Module opcode");
  }
}

absl::Status SkipModuleValue(Reader* reader) {
  auto module_id = ReadLength(reader);
  if (!module_id.ok()) return module_id.status();
  if (module_id->encoded) return Bad("encoded Redis Module id");
  return SkipModuleBody(reader);
}

absl::Status SkipModuleAux(Reader* reader) {
  auto module_id = ReadLength(reader);
  auto when_opcode = ReadLength(reader);
  auto when = ReadLength(reader);
  if (!module_id.ok()) return module_id.status();
  if (!when_opcode.ok()) return when_opcode.status();
  if (!when.ok()) return when.status();
  if (module_id->encoded || when_opcode->encoded || when->encoded ||
      when_opcode->value != 2) {
    return Bad("invalid Redis Module auxiliary header");
  }
  return SkipModuleBody(reader);
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

struct PackedMeasurement {
  std::uint64_t count = 0;
  std::uint64_t strings = 0;
  void Add(std::size_t bytes) {
    ++count;
    strings += std::max<std::size_t>(bytes, 32) + 1;
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

absl::StatusOr<std::vector<LpValue>> DecodeListpack(
    std::string_view input, PackedMeasurement* measured = nullptr) {
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
  std::uint64_t actual_count = 0;
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
      if (measured == nullptr) value.text.assign(input.substr(at, payload));
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
    ++actual_count;
    if (measured != nullptr)
      measured->Add(value.integer ? 32 : payload);
    else
      values.push_back(std::move(value));
  }
  if (at != input.size() - 1 ||
      (declared != UINT16_MAX && declared != actual_count))
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

absl::StatusOr<std::vector<std::string>> DecodeZiplist(
    std::string_view input, PackedMeasurement* measured = nullptr) {
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
  std::uint64_t actual_count = 0;
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
      ++actual_count;
      if (measured != nullptr)
        measured->Add(32);
      else
        values.push_back(std::to_string(number));
      previous = at - start;
      continue;
    }
    if (length > input.size() - 1 - at) return Bad("truncated ziplist string");
    ++actual_count;
    if (measured != nullptr)
      measured->Add(length);
    else
      values.emplace_back(input.substr(at, length));
    at += length;
    previous = at - start;
  }
  if (at != input.size() - 1 || (count != UINT16_MAX && count != actual_count))
    return Bad("ziplist count mismatch");
  return values;
}

absl::StatusOr<std::vector<std::string>> DecodeIntset(
    std::string_view input, PackedMeasurement* measured = nullptr) {
  Reader reader(input);
  std::uint32_t width = 0, count = 0;
  if (!reader.Le32(&width) || !reader.Le32(&count) ||
      (width != 2 && width != 4 && width != 8) ||
      count > reader.remaining() / width ||
      reader.remaining() != static_cast<std::size_t>(count) * width)
    return Bad("invalid intset");
  std::vector<std::string> output;
  if (measured == nullptr) output.reserve(count);
  std::int64_t previous = std::numeric_limits<std::int64_t>::min();
  for (std::uint32_t i = 0; i < count; ++i) {
    std::string_view bytes;
    reader.View(width, &bytes);
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
    if (measured != nullptr)
      measured->Add(32);
    else
      output.push_back(std::to_string(value));
  }
  return output;
}

absl::StatusOr<std::vector<std::string>> DecodeZipmap(
    std::string_view input, PackedMeasurement* measured = nullptr) {
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
    if (measured != nullptr)
      measured->Add(key_size);
    else
      output.emplace_back(input.substr(at, key_size));
    at += key_size;
    if (!length(&value_size) || at == input.size())
      return Bad("truncated zipmap value");
    const std::uint8_t free = static_cast<std::uint8_t>(input[at++]);
    if (value_size > input.size() - at || free > input.size() - at - value_size)
      return Bad("truncated zipmap value");
    if (measured != nullptr)
      measured->Add(value_size);
    else
      output.emplace_back(input.substr(at, value_size));
    at += value_size + free;
  }
  if (at + 1 != input.size() || static_cast<unsigned char>(input[at]) != 255 ||
      (measured != nullptr ? measured->count == 0 : output.empty()))
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
  std::vector<std::uint32_t> node_entries;
  std::vector<Group> groups;
};

void SynthesizeStreamNodes(Stream* stream) {
  stream->node_entries.clear();
  std::size_t remaining = stream->entries.size();
  while (remaining != 0) {
    const auto count = static_cast<std::uint32_t>(
        std::min<std::size_t>(remaining, kDefaultStreamNodeMaxEntries));
    stream->node_entries.push_back(count);
    remaining -= count;
  }
}

bool ValidStreamNodes(const Stream& stream) {
  std::size_t total = 0;
  for (const std::uint32_t count : stream.node_entries) {
    if (count == 0 || total > stream.entries.size() ||
        count > stream.entries.size() - total)
      return false;
    total += count;
  }
  return total == stream.entries.size();
}
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
      Id id;
      std::uint64_t delivery = 0;
      if (!reader->Be64(&id.ms) || !reader->Be64(&id.seq) ||
          !reader->Le64(&delivery))
        return Bad("truncated Stream PEL");
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
        Id id;
        if (!reader->Be64(&id.ms) || !reader->Be64(&id.seq))
          return Bad("truncated Stream consumer PEL");
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
          char text[252];
          if (!reader->Read(size, text)) return Bad("truncated Zset score");
          auto parsed = std::from_chars(text, text + size, score);
          if (parsed.ec != std::errc{} || parsed.ptr != text + size)
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

storage::ValueType CollectionType(std::uint8_t type) {
  using storage::ValueType;
  switch (type) {
    case kHash:
    case kHashZipmap:
    case kHashZiplist:
    case kHashListpack:
      return ValueType::kHash;
    case kSet:
    case kSetIntset:
    case kSetListpack:
      return ValueType::kSet;
    case kList:
    case kListZiplist:
    case kListQuicklist:
    case kListQuicklist2:
      return ValueType::kList;
    case kZSet:
    case kZSet2:
    case kZSetZiplist:
    case kZSetListpack:
      return ValueType::kSortedSet;
    default:
      return ValueType::kNone;
  }
}

// Inspect encoded lengths without constructing a string. A decompressed
// string is still bounded individually; the collection has no aggregate
// string limit. The actual decode below validates the LZF contents.
absl::StatusOr<std::size_t> MeasureString(Reader* reader) {
  auto length = ReadLength(reader);
  if (!length.ok()) return length.status();
  std::uint64_t encoded = length->value;
  std::uint64_t decoded = encoded;
  if (length->encoded) {
    if (length->value <= 2) {
      encoded = length->value == 0 ? 1 : length->value == 1 ? 2 : 4;
      decoded = 32;
    } else if (length->value == 3) {
      auto input = ReadLength(reader);
      auto output = ReadLength(reader);
      if (!input.ok()) return input.status();
      if (!output.ok()) return output.status();
      if (input->encoded || output->encoded) return Bad("invalid LZF lengths");
      encoded = input->value;
      decoded = output->value;
    } else
      return Bad("unknown encoded string");
  }
  if (decoded > storage::kMaxStringBytes || encoded > reader->remaining() ||
      !reader->Skip(static_cast<std::size_t>(encoded)))
    return Bad("string length exceeds payload");
  return static_cast<std::size_t>(decoded);
}

absl::StatusOr<double> ReadCollectionScore(Reader* reader, std::uint8_t type) {
  double score = 0;
  if (type == kZSet2) {
    std::uint64_t bits = 0;
    if (!reader->Le64(&bits)) return Bad("truncated Zset score");
    score = std::bit_cast<double>(bits);
  } else {
    std::uint8_t size = 0;
    if (!reader->Byte(&size)) return Bad("truncated Zset score");
    if (size == 253) return Bad("invalid Zset score");
    if (size == 254) return std::numeric_limits<double>::infinity();
    if (size == 255) return -std::numeric_limits<double>::infinity();
    char text[252];
    if (!reader->Read(size, text)) return Bad("truncated Zset score");
    auto parsed = std::from_chars(text, text + size, score);
    if (parsed.ec != std::errc{} || parsed.ptr != text + size)
      return Bad("invalid Zset score");
  }
  if (std::isnan(score)) return Bad("invalid Zset score");
  return score;
}

class CollectionInput {
 public:
  static absl::StatusOr<std::unique_ptr<CollectionInput>> Open(
      Reader* reader, std::uint8_t type) try {
    auto result = std::unique_ptr<CollectionInput>(new CollectionInput(type));
    if (result->plain_ || result->quick_) {
      auto count = ReadLength(reader);
      if (!count.ok()) return count.status();
      if (count->encoded || count->value == 0)
        return Bad("invalid collection length");
      result->remaining_ = count->value;
      if (result->plain_) result->expected_ = count->value;
    }
    return result;
  } catch (const std::bad_alloc&) {
    return Oom();
  }
  storage::ValueType type() const { return CollectionType(type_); }
  std::optional<std::uint64_t> expected() const { return expected_; }

  absl::StatusOr<storage::CollectionPage> Next(Reader* input) try {
    if (done_) return Bad("collection stream is complete");
    if (!owner_) {
      owner_ = CurrentMemoryAccountingShard();
      identities_charge_.Account(*owner_, 0);
    } else if (*owner_ != CurrentMemoryAccountingShard()) {
      return absl::FailedPreconditionError("RDB collection changed page owner");
    }
    if (!plain_) return Packed(input);
    Reader measure = *input;
    std::size_t bytes = 4096;
    std::size_t count = 0;
    do {
      auto first = MeasureString(&measure);
      if (!first.ok()) return first.status();
      bytes += *first + 256;
      if (type_ == kHash) {
        auto second = MeasureString(&measure);
        if (!second.ok()) return second.status();
        bytes += *second;
      } else if (type() == storage::ValueType::kSortedSet) {
        auto score = ReadCollectionScore(&measure, type_);
        if (!score.ok()) return score.status();
      }
      ++count;
    } while (count < remaining_ && bytes < 1024 * 1024);
    auto reservation = TryReserveMemory(bytes);
    if (!reservation) return Oom();
    storage::CollectionPage page{.value_type_ = type()};
    if (type_ == kHash)
      page.fields_.reserve(count);
    else if (type() == storage::ValueType::kSortedSet)
      page.scored_members_.reserve(count);
    else
      page.elements_.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
      Reader origin = *input;
      input->ResetExpandedAccounting();
      auto first = ReadString(input);
      if (!first.ok()) return first.status();
      if (type_ != kList) {
        auto unique = Remember(*first, origin);
        if (!unique.ok()) return unique;
      }
      if (type_ == kHash) {
        input->ResetExpandedAccounting();
        auto second = ReadString(input);
        if (!second.ok()) return second.status();
        page.fields_.push_back({std::move(*first), std::move(*second)});
      } else if (type() == storage::ValueType::kSortedSet) {
        auto score = ReadCollectionScore(input, type_);
        if (!score.ok()) return score.status();
        page.scored_members_.push_back({std::move(*first), *score});
      } else
        page.elements_.push_back(std::move(*first));
    }
    remaining_ -= count;
    page.done_ = done_ = remaining_ == 0;
    page.next_cursor_ = ++cursor_;
    if (page.RetainedBytes() > bytes) return Bad("RDB page budget mismatch");
    page.retained_charge_.Adopt(&*reservation, page.RetainedBytes());
    return page;
  } catch (const std::bad_alloc&) {
    return Oom();
  } catch (const std::length_error&) {
    return Oom();
  }

 private:
  explicit CollectionInput(std::uint8_t type)
      : type_(type),
        plain_(type <= kZSet2),
        quick_(type == kListQuicklist || type == kListQuicklist2) {}
  static absl::Status Oom() {
    RecordMemoryRejection();
    return absl::ResourceExhaustedError("OOM RDB collection decode admission");
  }
  absl::Status Remember(std::string_view member, Reader origin) {
    const auto digest = storage::ComputeDigest(member).value_;
    const auto range = identities_.equal_range(digest);
    for (auto it = range.first; it != range.second; ++it) {
      Reader original = it->second;
      Reader measure = original;
      auto size = MeasureString(&measure);
      if (!size.ok()) return size.status();
      auto reservation = TryReserveMemory(*size + 64);
      if (!reservation) return Oom();
      original.ResetExpandedAccounting();
      auto old = ReadString(&original);
      if (!old.ok()) return old.status();
      if (*old == member) return Bad("duplicate collection member or field");
    }
    // Keep only digest+borrowed input position, not another copy of every
    // member. A collision re-decodes the original string before comparison.
    constexpr std::size_t node_budget = sizeof(Reader) + 8 * sizeof(void*) + 32;
    auto reservation = TryReserveMemory(node_budget);
    if (!reservation) return Oom();
    identities_.emplace(digest, origin);
    identities_charge_.Resize(identities_charge_.bytes() + node_budget);
    return absl::OkStatus();
  }
  absl::StatusOr<storage::CollectionPage> Packed(Reader* input) {
    std::uint64_t container = 2;
    if (type_ == kListQuicklist2) {
      auto flag = ReadLength(input);
      if (!flag.ok()) return flag.status();
      if (flag->encoded || (flag->value != 1 && flag->value != 2))
        return Bad("invalid quicklist container");
      container = flag->value;
    }
    Reader measure = *input;
    auto expanded = MeasureString(&measure);
    if (!expanded.ok()) return expanded.status();
    auto blob_reservation = TryReserveMemory(*expanded + 64);
    if (!blob_reservation) return Oom();
    Reader origin = *input;
    input->ResetExpandedAccounting();
    auto blob = ReadString(input);
    if (!blob.ok()) return blob.status();
    // The allocation-free measurement pass checks every packed entry rather
    // than trusting a possibly saturated/corrupt count in the packed header.
    // Only then reserve decoded vectors, string expansion and duplicate-check
    // scratch. Long strings no longer imply a worst-case entry per byte.
    PackedMeasurement measured;
    absl::Status measured_status;
    if (quick_ && container == 1)
      measured.Add(blob->size());
    else if (type_ == kSetIntset)
      measured_status = DecodeIntset(*blob, &measured).status();
    else if (type_ == kHashZipmap)
      measured_status = DecodeZipmap(*blob, &measured).status();
    else if (type_ == kListZiplist || type_ == kHashZiplist ||
             type_ == kZSetZiplist || type_ == kListQuicklist)
      measured_status = DecodeZiplist(*blob, &measured).status();
    else
      measured_status = DecodeListpack(*blob, &measured).status();
    if (!measured_status.ok()) return measured_status;
    if (measured.count > (SIZE_MAX - *expanded - 4096) / 192) return Oom();
    const auto headers = measured.count * 192 + *expanded + 4096;
    if (measured.strings > (SIZE_MAX - headers) / 3) return Oom();
    const auto budget = headers + measured.strings * 3;
    auto reservation = TryReserveMemory(budget);
    if (!reservation) return Oom();
    storage::CollectionPage page{.value_type_ = type()};
    if (quick_) {
      if (container == 1)
        page.elements_.push_back(std::move(*blob));
      else if (type_ == kListQuicklist) {
        auto values = DecodeZiplist(*blob);
        if (!values.ok()) return values.status();
        page.elements_ = std::move(*values);
      } else {
        auto values = DecodeListpack(*blob);
        if (!values.ok()) return values.status();
        page.elements_.reserve(values->size());
        for (auto& value : *values) page.elements_.push_back(value.String());
      }
      --remaining_;
    } else {
      origin.ResetExpandedAccounting();
      auto logical = DecodeRdbObject(&origin, type_);
      if (!logical.ok()) return logical.status();
      if (type() == storage::ValueType::kHash) {
        auto& pairs = std::get<Pairs>(logical->value);
        page.fields_.reserve(pairs.size());
        for (auto& pair : pairs)
          page.fields_.push_back(
              {std::move(pair.first), std::move(pair.second)});
      } else if (type() == storage::ValueType::kSortedSet) {
        auto& members = std::get<ZElements>(logical->value);
        page.scored_members_.reserve(members.size());
        for (auto& member : members)
          page.scored_members_.push_back(
              {std::move(member.member), member.score});
      } else
        page.elements_ = std::move(std::get<Strings>(logical->value));
      remaining_ = 0;
    }
    page.done_ = done_ = remaining_ == 0;
    page.next_cursor_ = ++cursor_;
    if (page.size() > UINT64_MAX - total_items_)
      return Bad("collection count overflow");
    total_items_ += page.size();
    if (page.done_ && total_items_ == 0) return Bad("empty collection");
    if (page.RetainedBytes() > budget)
      return Bad("packed RDB page budget mismatch");
    page.retained_charge_.Adopt(&*reservation, page.RetainedBytes());
    return page;
  }
  RetainedMemoryCharge identities_charge_;
  std::multimap<std::uint64_t, Reader> identities_;
  std::optional<unsigned> owner_;
  std::uint8_t type_;
  bool plain_;
  bool quick_;
  bool done_ = false;
  std::uint64_t remaining_ = 1;
  std::uint64_t cursor_ = 0;
  std::uint64_t total_items_ = 0;
  std::optional<std::uint64_t> expected_;
};

absl::StatusOr<LogicalValue> DecodeRaw(const storage::RawValue& raw) {
  const std::string_view input = raw.encoded_;
  if (raw.value_type_ == storage::ValueType::kString)
    return LogicalValue{raw.value_type_, std::string(input)};
  if (raw.value_type_ == storage::ValueType::kList) {
    if (!input.starts_with(kListMagic) || input.size() < 8)
      return Bad("invalid Lavik List");
    Reader reader(input.substr(4));
    std::uint32_t count = 0;
    if (!reader.Le32(&count) || count != raw.logical_size_ || count == 0)
      return Bad("invalid Lavik List count");
    Strings values;
    values.reserve(count);
    for (std::uint32_t i = 0; i < count; ++i) {
      std::uint32_t size = 0;
      std::string_view value;
      if (!reader.Le32(&size) || !reader.View(size, &value))
        return Bad("truncated Lavik List");
      values.emplace_back(value);
    }
    if (!reader.done()) return Bad("trailing Lavik List data");
    return LogicalValue{raw.value_type_, std::move(values)};
  }
  if (raw.value_type_ == storage::ValueType::kSet ||
      raw.value_type_ == storage::ValueType::kHash) {
    if (input.size() < 32) return Bad("invalid Lavik Hash");
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
      return Bad("invalid Lavik Hash header");
    Pairs pairs;
    pairs.reserve(count);
    for (std::uint32_t i = 0; i < count; ++i) {
      std::string_view field, value;
      std::uint32_t fs = 0, vs = 0;
      if (!reader.Le32(&fs) || !reader.Le32(&vs) || !reader.View(fs, &field) ||
          !reader.View(vs, &value))
        return Bad("truncated Lavik Hash");
      pairs.emplace_back(std::string(field), std::string(value));
    }
    if (!reader.done()) return Bad("trailing Lavik Hash data");
    if (raw.value_type_ == storage::ValueType::kSet) {
      Strings values;
      values.reserve(pairs.size());
      for (auto& [member, unused] : pairs) {
        if (!unused.empty()) return Bad("invalid Lavik Set value");
        values.push_back(std::move(member));
      }
      return LogicalValue{raw.value_type_, std::move(values)};
    }
    return LogicalValue{raw.value_type_, std::move(pairs)};
  }
  if (raw.value_type_ == storage::ValueType::kSortedSet) {
    if (!input.starts_with(kZSetMagic) || input.size() < 8)
      return Bad("invalid Lavik Zset");
    Reader reader(input.substr(4));
    std::uint32_t count = 0;
    if (!reader.Le32(&count) || count != raw.logical_size_ || count == 0)
      return Bad("invalid Lavik Zset count");
    ZElements values;
    values.reserve(count);
    for (std::uint32_t i = 0; i < count; ++i) {
      std::uint64_t bits = 0;
      std::uint32_t size = 0;
      std::string_view member;
      if (!reader.Le64(&bits) || !reader.Le32(&size) ||
          !reader.View(size, &member))
        return Bad("truncated Lavik Zset");
      double score = std::bit_cast<double>(bits);
      if (std::isnan(score)) return Bad("NaN Lavik Zset score");
      values.push_back({std::string(member), score});
    }
    if (!reader.done()) return Bad("trailing Lavik Zset data");
    return LogicalValue{raw.value_type_, std::move(values)};
  }
  if (raw.value_type_ == storage::ValueType::kStream) {
    if (!input.starts_with(kStreamMagic) || input.size() < 48)
      return Bad("invalid Lavik Stream");
    Reader reader(input.substr(4));
    Stream stream;
    std::uint32_t entries = 0, groups = 0;
    if (!reader.Le64(&stream.last.ms) || !reader.Le64(&stream.last.seq) ||
        !reader.Le64(&stream.max_deleted.ms) ||
        !reader.Le64(&stream.max_deleted.seq) ||
        !reader.Le64(&stream.entries_added) || !reader.Le32(&entries) ||
        entries != raw.logical_size_)
      return Bad("invalid Lavik Stream header");
    auto read_text = [&](std::string* value) {
      std::uint32_t size = 0;
      std::string_view text;
      if (!reader.Le32(&size) || !reader.View(size, &text)) return false;
      value->assign(text);
      return true;
    };
    for (std::uint32_t i = 0; i < entries; ++i) {
      Entry entry;
      std::uint32_t fields = 0;
      if (!reader.Le64(&entry.id.ms) || !reader.Le64(&entry.id.seq) ||
          !reader.Le32(&fields) || fields % 2)
        return Bad("invalid Lavik Stream entry");
      for (std::uint32_t f = 0; f < fields; ++f) {
        std::string text;
        if (!read_text(&text)) return Bad("truncated Lavik Stream field");
        entry.fields.push_back(std::move(text));
      }
      stream.entries.push_back(std::move(entry));
    }
    std::uint32_t nodes = 0;
    if (!reader.Le32(&nodes) || nodes > stream.entries.size())
      return Bad("invalid Lavik Stream nodes");
    stream.node_entries.reserve(nodes);
    for (std::uint32_t i = 0; i < nodes; ++i) {
      std::uint32_t count = 0;
      if (!reader.Le32(&count)) return Bad("truncated Lavik Stream nodes");
      stream.node_entries.push_back(count);
    }
    if (!ValidStreamNodes(stream))
      return Bad("invalid Lavik Stream node counts");
    if (!reader.Le32(&groups)) return Bad("truncated Lavik Stream groups");
    for (std::uint32_t i = 0; i < groups; ++i) {
      Group group;
      std::uint64_t read_bits = 0;
      std::uint32_t consumers = 0, pending = 0;
      if (!read_text(&group.name) || !reader.Le64(&group.last.ms) ||
          !reader.Le64(&group.last.seq) || !reader.Le64(&read_bits) ||
          !reader.Le32(&consumers))
        return Bad("truncated Lavik Stream group");
      group.entries_read = std::bit_cast<std::int64_t>(read_bits);
      for (std::uint32_t c = 0; c < consumers; ++c) {
        Consumer consumer;
        if (!read_text(&consumer.name) || !reader.Le64(&consumer.seen) ||
            !reader.Le64(&consumer.active))
          return Bad("truncated Lavik Stream consumer");
        group.consumers.push_back(std::move(consumer));
      }
      if (!reader.Le32(&pending)) return Bad("truncated Lavik Stream PEL");
      for (std::uint32_t p = 0; p < pending; ++p) {
        Pending item;
        if (!reader.Le64(&item.id.ms) || !reader.Le64(&item.id.seq) ||
            !read_text(&item.consumer) || !reader.Le64(&item.delivery) ||
            !reader.Le64(&item.count))
          return Bad("truncated Lavik Stream PEL");
        group.pending.push_back(std::move(item));
      }
      stream.groups.push_back(std::move(group));
    }
    if (!reader.done()) return Bad("trailing Lavik Stream data");
    return LogicalValue{raw.value_type_, std::move(stream)};
  }
  return Bad("unsupported Lavik type");
}

void AppendHashRaw(std::string* output, const Pairs& pairs) {
  std::uint64_t bytes = 32;
  for (const auto& [field, value] : pairs)
    bytes += 8 + field.size() + value.size();
  PutLe64(output, storage::kHashValueMagic);
  PutLe32(output, storage::kStorageFormatVersion);
  PutLe32(output, 32);
  PutLe32(output, static_cast<std::uint32_t>(pairs.size()));
  PutLe32(output, 0);
  PutLe64(output, bytes);
  for (const auto& [field, value] : pairs) {
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
      if (!text_size(&bytes, value)) return Bad("value exceeds Lavik limits");
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
      if (!add_size(&bytes, 8) || !add_size(&bytes, field.size()) ||
          !add_size(&bytes, value.size()) || field.size() > UINT32_MAX ||
          value.size() > UINT32_MAX)
        return Bad("value exceeds Lavik limits");
    }
    raw.logical_size_ = pairs.size();
    raw.encoded_.reserve(bytes);
    AppendHashRaw(&raw.encoded_, pairs);
  } else if (logical.type == storage::ValueType::kHash) {
    auto pairs = std::move(std::get<Pairs>(logical.value));
    std::sort(pairs.begin(), pairs.end());
    std::uint64_t bytes = 32;
    for (const auto& [field, value] : pairs) {
      if (!add_size(&bytes, 8) || !add_size(&bytes, field.size()) ||
          !add_size(&bytes, value.size()) || field.size() > UINT32_MAX ||
          value.size() > UINT32_MAX)
        return Bad("value exceeds Lavik limits");
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
        return Bad("value exceeds Lavik limits");
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
    if (stream.node_entries.empty() && !stream.entries.empty())
      SynthesizeStreamNodes(&stream);
    if (!ValidStreamNodes(stream)) return Bad("invalid Lavik Stream nodes");
    std::uint64_t bytes = 48;
    for (const auto& entry : stream.entries) {
      if (!add_size(&bytes, 20) || entry.fields.size() > UINT32_MAX)
        return Bad("value exceeds Lavik limits");
      for (const auto& field : entry.fields) {
        if (!text_size(&bytes, field)) return Bad("value exceeds Lavik limits");
      }
    }
    if (!add_size(&bytes, 4 + 4 * stream.node_entries.size()))
      return Bad("value exceeds Lavik limits");
    if (!add_size(&bytes, 4)) return Bad("value exceeds Lavik limits");
    for (const auto& group : stream.groups) {
      if (!text_size(&bytes, group.name) || !add_size(&bytes, 28) ||
          group.consumers.size() > UINT32_MAX ||
          group.pending.size() > UINT32_MAX)
        return Bad("value exceeds Lavik limits");
      for (const auto& consumer : group.consumers) {
        if (!text_size(&bytes, consumer.name) || !add_size(&bytes, 16))
          return Bad("value exceeds Lavik limits");
      }
      if (!add_size(&bytes, 4)) return Bad("value exceeds Lavik limits");
      for (const auto& pending : group.pending) {
        if (!add_size(&bytes, 16) || !text_size(&bytes, pending.consumer) ||
            !add_size(&bytes, 16))
          return Bad("value exceeds Lavik limits");
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
    PutLe32(&raw.encoded_, stream.node_entries.size());
    for (const std::uint32_t count : stream.node_entries)
      PutLe32(&raw.encoded_, count);
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
    return Bad("value exceeds Lavik limits");
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
    return Bad("unsupported Lavik type");
  PutLe16(&out, kVersion);
  PutLe64(&out, Crc64(out));
  return out;
}

}  // namespace

struct FileReader::Impl {
  void Rewind() {
    collection_.reset();
    input_.Reset();
    reader_ = Reader(&input_, 9);
    db_id_ = 0;
    expire_at_ms_.reset();
    entry_metadata_ = false;
    finished_ = false;
  }

  FileInput input_;
  unsigned version_ = 0;
  Reader reader_{&input_};
  std::uint8_t db_id_ = 0;
  std::optional<std::uint64_t> expire_at_ms_;
  bool entry_metadata_ = false;
  bool finished_ = false;
  std::unique_ptr<CollectionInput> collection_;
};

FileReader::FileReader(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

FileReader::FileReader(FileReader&&) noexcept = default;
FileReader& FileReader::operator=(FileReader&&) noexcept = default;
FileReader::~FileReader() = default;

absl::StatusOr<FileReader> FileReader::Open(const std::string& path) try {
  // Allocate the stable source before opening its descriptor. Impl owns both
  // on every error path, and moving FileReader never invalidates its cursors.
  auto impl = std::make_unique<Impl>();
  auto status = impl->input_.Open(path);
  if (!status.ok()) return status;
  auto& input = impl->input_;
  char header[9];
  if (input.size() < 10) {
    return absl::InvalidArgumentError("invalid RDB file header");
  }
  if (!input.Read(0, sizeof(header), header)) return input.status();
  if (!std::string_view(header, sizeof(header)).starts_with("REDIS")) {
    return absl::InvalidArgumentError("invalid RDB file header");
  }
  unsigned version = 0;
  const char* version_begin = header + 5;
  const char* version_end = version_begin + 4;
  const auto parsed = std::from_chars(version_begin, version_end, version);
  if (parsed.ec != std::errc{} || parsed.ptr != version_end || version == 0 ||
      version > kVersion) {
    return absl::InvalidArgumentError(
        absl::StrCat("unsupported RDB file version '",
                     std::string_view(version_begin, 4), "'"));
  }

  // The checksum footer was introduced with RDB version 5. Redis writes a
  // zero checksum when checksum generation is disabled, which loaders accept.
  if (version >= 5) {
    if (input.size() < 18) {
      return absl::InvalidArgumentError("truncated RDB checksum footer");
    }
    Reader footer(&input, input.size() - 8);
    std::uint64_t expected = 0;
    if (!footer.Le64(&expected)) return input.status();
    if (expected != 0) {
      std::uint64_t crc = 0;
      const auto end = input.size() - 8;
      for (std::size_t at = 0; at < end;) {
        const auto part = input.Chunk(at).substr(0, end - at);
        if (!input.status().ok()) return input.status();
        crc = UpdateCrc64(crc, part);
        at += part.size();
      }
      if (Reflect64(crc) != expected) {
        return absl::InvalidArgumentError("RDB file checksum is invalid");
      }
    }
  }
  impl->version_ = version;
  impl->Rewind();
  return FileReader(std::move(impl));
} catch (const std::bad_alloc&) {
  RecordMemoryRejection();
  return absl::ResourceExhaustedError("OOM RDB file reader");
}

absl::StatusOr<std::optional<FileEntry>> FileReader::Next() try {
  auto entry = NextImpl(false);
  if (!impl_->input_.status().ok()) return impl_->input_.status();
  return entry;
} catch (const std::bad_alloc&) {
  RecordMemoryRejection();
  return absl::ResourceExhaustedError("OOM RDB file entry");
}

absl::StatusOr<std::optional<FileEntry>> FileReader::NextStreaming() try {
  auto entry = NextImpl(true);
  if (!impl_->input_.status().ok()) return impl_->input_.status();
  return entry;
} catch (const std::bad_alloc&) {
  RecordMemoryRejection();
  return absl::ResourceExhaustedError("OOM RDB file entry");
}

absl::StatusOr<storage::CollectionPage> FileReader::ReadCollectionPage() {
  if (!impl_->input_.status().ok()) return impl_->input_.status();
  if (!impl_->collection_) return Bad("no active RDB collection");
  auto page = impl_->collection_->Next(&impl_->reader_);
  if (!impl_->input_.status().ok()) return impl_->input_.status();
  if (page.ok() && page->done_) impl_->collection_.reset();
  return page;
}

absl::Status FileReader::DrainCollection() {
  while (impl_->collection_) {
    auto page = ReadCollectionPage();
    if (!page.ok()) return page.status();
  }
  return absl::OkStatus();
}

absl::StatusOr<std::optional<FileEntry>> FileReader::NextImpl(
    bool stream_collections) {
  if (!impl_->input_.status().ok()) return impl_->input_.status();
  if (impl_->collection_) return Bad("RDB collection must be drained first");
  if (impl_->finished_) return std::optional<FileEntry>();

  while (true) {
    std::uint8_t type = 0;
    if (!impl_->reader_.Byte(&type)) return Bad("RDB file has no EOF opcode");

    if (type == kEof) {
      const std::size_t footer_bytes = impl_->version_ >= 5 ? 8 : 0;
      if (impl_->entry_metadata_ ||
          impl_->reader_.remaining() != footer_bytes) {
        return Bad("trailing data or incomplete key before RDB EOF");
      }
      impl_->finished_ = true;
      return std::optional<FileEntry>();
    }

    if (type == kExpireTimeMs) {
      if (impl_->expire_at_ms_.has_value()) {
        return Bad("duplicate RDB expiration metadata");
      }
      std::uint64_t raw = 0;
      if (!impl_->reader_.Le64(&raw)) {
        return Bad("truncated millisecond expiration");
      }
      const std::int64_t deadline = std::bit_cast<std::int64_t>(raw);
      impl_->expire_at_ms_ =
          deadline <= 0 ? 1 : static_cast<std::uint64_t>(deadline);
      impl_->entry_metadata_ = true;
      continue;
    }
    if (type == kExpireTime) {
      if (impl_->expire_at_ms_.has_value()) {
        return Bad("duplicate RDB expiration metadata");
      }
      std::uint32_t raw = 0;
      if (!impl_->reader_.Le32(&raw)) {
        return Bad("truncated second expiration");
      }
      const std::int32_t deadline = std::bit_cast<std::int32_t>(raw);
      impl_->expire_at_ms_ =
          deadline <= 0 ? 1 : static_cast<std::uint64_t>(deadline) * 1000;
      impl_->entry_metadata_ = true;
      continue;
    }
    if (type == kIdle) {
      auto idle = ReadLength(&impl_->reader_);
      if (!idle.ok() || idle->encoded) return Bad("invalid RDB idle time");
      impl_->entry_metadata_ = true;
      continue;
    }
    if (type == kFreq) {
      std::uint8_t ignored = 0;
      if (!impl_->reader_.Byte(&ignored)) return Bad("truncated RDB frequency");
      impl_->entry_metadata_ = true;
      continue;
    }

    if (impl_->entry_metadata_) {
      // Expiry/LRU/LFU metadata must be followed immediately by an object.
      if (type >= kAux) return Bad("RDB key metadata is not followed by a key");
    }
    if (type == kAux) {
      impl_->reader_.ResetExpandedAccounting();
      auto key = ReadString(&impl_->reader_);
      impl_->reader_.ResetExpandedAccounting();
      auto value = ReadString(&impl_->reader_);
      if (!key.ok()) return key.status();
      if (!value.ok()) return value.status();
      continue;
    }
    if (type == kResizeDb) {
      auto keys = ReadLength(&impl_->reader_);
      auto expires = ReadLength(&impl_->reader_);
      if (!keys.ok()) return keys.status();
      if (!expires.ok()) return expires.status();
      if (keys->encoded || expires->encoded) {
        return Bad("invalid RDB resize hint");
      }
      continue;
    }
    if (type == kSelectDb) {
      auto db = ReadLength(&impl_->reader_);
      if (!db.ok()) return db.status();
      if (db->encoded || db->value >= storage::kLogicalDatabaseCount) {
        return Bad("RDB database is outside Lavik's DB range");
      }
      impl_->db_id_ = static_cast<std::uint8_t>(db->value);
      continue;
    }
    if (type == kFunction2) {
      impl_->reader_.ResetExpandedAccounting();
      auto code = ReadString(&impl_->reader_);
      if (!code.ok()) return code.status();
      return std::optional<FileEntry>(FileEntry{
          .kind_ = FileEntryKind::kFunctionLibrary,
          .db_id_ = impl_->db_id_,
          .key_ = {},
          .value_ = {},
          .function_code_ = std::move(*code),
      });
    }
    if (type == kFunctionPreGa) {
      return Bad("pre-release Redis Function format is not skippable");
    }
    if (type == kModuleAux) {
      absl::Status skipped = SkipModuleAux(&impl_->reader_);
      if (!skipped.ok()) return skipped;
      return std::optional<FileEntry>(FileEntry{
          .kind_ = FileEntryKind::kSkippedModuleAux,
          .db_id_ = impl_->db_id_,
          .key_ = {},
          .value_ = {},
          .function_code_ = {},
      });
    }

    impl_->reader_.ResetExpandedAccounting();
    auto key = ReadString(&impl_->reader_);
    if (!key.ok()) return key.status();
    if (type == kModule2) {
      absl::Status skipped = SkipModuleValue(&impl_->reader_);
      if (!skipped.ok()) return skipped;
      FileEntry entry{.kind_ = FileEntryKind::kSkippedModuleValue,
                      .db_id_ = impl_->db_id_,
                      .key_ = std::move(*key),
                      .value_ = {},
                      .function_code_ = {}};
      impl_->expire_at_ms_.reset();
      impl_->entry_metadata_ = false;
      return std::optional<FileEntry>(std::move(entry));
    }
    if (type == kModulePreGa) {
      return Bad("pre-release Redis Module format is not skippable");
    }
    if (key->size() > storage::MaxKeyBytes()) {
      return Bad("RDB key exceeds Lavik limits");
    }
    impl_->reader_.ResetExpandedAccounting();
    if (stream_collections &&
        CollectionType(type) != storage::ValueType::kNone) {
      auto collection = CollectionInput::Open(&impl_->reader_, type);
      if (!collection.ok()) return collection.status();
      impl_->collection_ = std::move(*collection);
      FileEntry entry{
          .kind_ = FileEntryKind::kValue,
          .db_id_ = impl_->db_id_,
          .key_ = std::move(*key),
          .value_ =
              storage::RawValue{
                  .encoded_ = {},
                  .logical_size_ = impl_->collection_->expected().value_or(0),
                  .expire_at_ms_ = impl_->expire_at_ms_.value_or(0),
                  .value_type_ = impl_->collection_->type()},
          .function_code_ = {},
          .collection_stream_ = true,
          .expected_items_ = impl_->collection_->expected()};
      impl_->expire_at_ms_.reset();
      impl_->entry_metadata_ = false;
      return std::optional<FileEntry>(std::move(entry));
    }
    auto logical = DecodeRdbObject(&impl_->reader_, type);
    if (!logical.ok()) return logical.status();
    auto value = EncodeRaw(std::move(*logical));
    if (!value.ok()) return value.status();
    value->expire_at_ms_ = impl_->expire_at_ms_.value_or(0);

    FileEntry entry{.kind_ = FileEntryKind::kValue,
                    .db_id_ = impl_->db_id_,
                    .key_ = std::move(*key),
                    .value_ = std::move(*value),
                    .function_code_ = {}};
    impl_->expire_at_ms_.reset();
    impl_->entry_metadata_ = false;
    return std::optional<FileEntry>(std::move(entry));
  }
}

void FileReader::Rewind() { impl_->Rewind(); }

unsigned FileReader::version() const noexcept { return impl_->version_; }

struct FileWriter::Impl {
  ~Impl() {
    if (fd_ >= 0) ::close(fd_);
    if (!temporary_path_.empty()) ::unlink(temporary_path_.c_str());
  }

  absl::Status Write(std::string_view bytes, bool checksum = true) {
    while (!bytes.empty()) {
      const ssize_t written = ::write(fd_, bytes.data(), bytes.size());
      if (written < 0) {
        if (errno == EINTR) continue;
        return absl::InternalError(absl::StrCat(
            "cannot write RDB temporary file: ", std::strerror(errno)));
      }
      if (written == 0) {
        return absl::InternalError("short write to RDB temporary file");
      }
      const std::string_view part =
          bytes.substr(0, static_cast<std::size_t>(written));
      if (checksum) crc_ = UpdateCrc64(crc_, part);
      bytes.remove_prefix(static_cast<std::size_t>(written));
    }
    return absl::OkStatus();
  }

  int fd_ = -1;
  std::string target_path_;
  std::string temporary_path_;
  std::string directory_;
  std::uint64_t crc_ = 0;
  bool finished_ = false;
};

StreamEncoder::StreamEncoder(unsigned version) {
  const std::string encoded_version = std::to_string(version);
  header_ = absl::StrCat("REDIS", std::string(4 - encoded_version.size(), '0'),
                         encoded_version);
  crc_ = UpdateCrc64(crc_, header_);
}

void StreamEncoder::Account(std::string_view fragment) noexcept {
  if (!finished_) crc_ = UpdateCrc64(crc_, fragment);
}

std::string StreamEncoder::Finish() {
  if (finished_) return {};
  finished_ = true;
  std::string trailer(1, static_cast<char>(kEof));
  crc_ = UpdateCrc64(crc_, trailer);
  PutLe64(&trailer, Reflect64(crc_));
  return trailer;
}

FileWriter::FileWriter(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
FileWriter::FileWriter(FileWriter&&) noexcept = default;
FileWriter& FileWriter::operator=(FileWriter&&) noexcept = default;
FileWriter::~FileWriter() = default;

absl::StatusOr<FileWriter> FileWriter::Open(std::string target_path) {
  if (target_path.empty()) {
    return absl::InvalidArgumentError("RDB target path is empty");
  }
  const std::size_t slash = target_path.find_last_of('/');
  const std::string directory =
      slash == std::string::npos
          ? "."
          : (slash == 0 ? "/" : target_path.substr(0, slash));
  const std::string basename =
      slash == std::string::npos ? target_path : target_path.substr(slash + 1);
  if (basename.empty() || basename == "." || basename == "..") {
    return absl::InvalidArgumentError("invalid RDB target filename");
  }
  struct stat directory_info{};
  if (::stat(directory.c_str(), &directory_info) != 0 ||
      !S_ISDIR(directory_info.st_mode)) {
    return absl::InvalidArgumentError(
        absl::StrCat("RDB directory is not accessible: ", directory));
  }

  std::string temporary =
      absl::StrCat(directory, "/.", basename, ".tmp.XXXXXX");
  std::vector<char> path(temporary.begin(), temporary.end());
  path.push_back('\0');
  const int fd = ::mkstemp(path.data());
  if (fd < 0) {
    return absl::InternalError(
        absl::StrCat("cannot create RDB temporary file in '", directory,
                     "': ", std::strerror(errno)));
  }
  (void)::fcntl(fd, F_SETFD, FD_CLOEXEC);
  auto impl = std::make_unique<Impl>();
  impl->fd_ = fd;
  impl->target_path_ = std::move(target_path);
  impl->temporary_path_ = path.data();
  impl->directory_ = directory;
  FileWriter writer(std::move(impl));
  const std::string version = std::to_string(kVersion);
  const std::string header =
      absl::StrCat("REDIS", std::string(4 - version.size(), '0'), version);
  absl::Status written = writer.impl_->Write(header);
  if (!written.ok()) return written;
  return writer;
}

absl::Status FileWriter::WriteFragment(std::string_view fragment) {
  if (impl_ == nullptr || impl_->fd_ < 0 || impl_->finished_) {
    return absl::FailedPreconditionError("RDB writer is not open");
  }
  return impl_->Write(fragment);
}

absl::Status FileWriter::Finish() {
  if (impl_ == nullptr || impl_->fd_ < 0 || impl_->finished_) {
    return absl::FailedPreconditionError("RDB writer is not open");
  }
  const char eof = static_cast<char>(kEof);
  absl::Status status = impl_->Write(std::string_view(&eof, 1));
  if (status.ok()) {
    std::string checksum;
    PutLe64(&checksum, Reflect64(impl_->crc_));
    status = impl_->Write(checksum, false);
  }
  if (status.ok() && ::fdatasync(impl_->fd_) != 0) {
    status = absl::InternalError(
        absl::StrCat("cannot sync RDB temporary file: ", std::strerror(errno)));
  }
  if (::close(impl_->fd_) != 0 && status.ok()) {
    status = absl::InternalError(absl::StrCat(
        "cannot close RDB temporary file: ", std::strerror(errno)));
  }
  impl_->fd_ = -1;
  if (!status.ok()) return status;
  if (::rename(impl_->temporary_path_.c_str(), impl_->target_path_.c_str()) !=
      0) {
    return absl::InternalError(
        absl::StrCat("cannot replace RDB file: ", std::strerror(errno)));
  }
  impl_->temporary_path_.clear();
  const int directory_fd =
      ::open(impl_->directory_.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (directory_fd < 0) {
    return absl::InternalError(absl::StrCat(
        "cannot open RDB directory for sync: ", std::strerror(errno)));
  }
  const int sync_result = ::fsync(directory_fd);
  const int sync_error = errno;
  ::close(directory_fd);
  if (sync_result != 0) {
    return absl::InternalError(
        absl::StrCat("cannot sync RDB directory: ", std::strerror(sync_error)));
  }
  impl_->finished_ = true;
  return absl::OkStatus();
}

absl::StatusOr<std::string> EncodeDump(const storage::RawValue& value) {
  auto logical = DecodeRaw(value);
  if (!logical.ok()) return logical.status();
  return EncodeRdbObject(*logical);
}

absl::StatusOr<std::string> EncodeFileEntry(std::uint8_t db_id,
                                            std::string_view key,
                                            const storage::RawValue& value) {
  if (db_id >= storage::kLogicalDatabaseCount ||
      key.size() > storage::MaxKeyBytes()) {
    return absl::InvalidArgumentError("invalid RDB file entry");
  }
  auto dump = EncodeDump(value);
  if (!dump.ok()) return dump.status();
  if (dump->size() < 11) {
    return absl::InternalError("encoded RDB object is truncated");
  }
  std::string output;
  output.reserve(key.size() + dump->size() + 24);
  output.push_back(static_cast<char>(kSelectDb));
  WriteLength(&output, db_id);
  if (value.expire_at_ms_ != 0) {
    output.push_back(static_cast<char>(kExpireTimeMs));
    PutLe64(&output, value.expire_at_ms_);
  }
  output.push_back((*dump)[0]);
  WriteString(&output, key);
  output.append(dump->data() + 1, dump->size() - 11);
  return output;
}

std::string EncodeFunctionLibraryEntry(std::string_view code) {
  std::string out(1, static_cast<char>(kFunction2));
  WriteString(&out, code);
  return out;
}

std::optional<std::size_t> FunctionDumpEncodedSize(
    std::span<const std::string> libraries) noexcept {
  // The footer is two version bytes followed by the eight-byte CRC64. Each
  // library adds one FUNCTION2 opcode and Redis' length prefix before its
  // source. Compute this before staging so an impossible catalog cannot be
  // multiplied across every worker's hidden Lua runtime.
  std::size_t bytes = 10;
  constexpr std::size_t maximum = std::numeric_limits<std::size_t>::max();
  for (const std::string& code : libraries) {
    const std::size_t length_bytes = code.size() < 64            ? 1
                                     : code.size() < 16384       ? 2
                                     : code.size() <= UINT32_MAX ? 5
                                                                 : 9;
    if (bytes > maximum - 1 - length_bytes) return std::nullopt;
    bytes += 1 + length_bytes;
    if (code.size() > maximum - bytes) return std::nullopt;
    bytes += code.size();
  }
  return bytes;
}

std::string EncodeFunctionDump(std::span<const std::string> libraries) {
  std::string payload;
  if (const auto bytes = FunctionDumpEncodedSize(libraries);
      bytes.has_value()) {
    payload.reserve(*bytes);
  }
  for (const std::string& code : libraries) {
    payload += EncodeFunctionLibraryEntry(code);
  }
  PutLe16(&payload, kVersion);
  PutLe64(&payload, Crc64(payload));
  return payload;
}

absl::StatusOr<std::vector<std::string>> DecodeFunctionDump(
    std::string_view payload) {
  if (payload.size() < 10) return Bad("truncated FUNCTION DUMP payload");
  Reader footer(payload.substr(payload.size() - 10));
  std::uint16_t version = 0;
  std::uint64_t checksum = 0;
  if (!footer.Le16(&version) || !footer.Le64(&checksum) || version == 0 ||
      version > kVersion ||
      Crc64(payload.substr(0, payload.size() - 8)) != checksum) {
    return Bad("FUNCTION DUMP payload version or checksum is invalid");
  }

  Reader reader(payload.substr(0, payload.size() - 10));
  std::vector<std::string> libraries;
  while (!reader.done()) {
    std::uint8_t type = 0;
    if (!reader.Byte(&type)) return Bad("truncated FUNCTION DUMP payload");
    if (type == kFunctionPreGa) {
      return Bad("pre-release Redis Function format is not supported");
    }
    if (type != kFunction2) {
      return Bad("FUNCTION DUMP payload contains a non-function entry");
    }
    reader.ResetExpandedAccounting();
    auto code = ReadString(&reader);
    if (!code.ok()) return code.status();
    libraries.push_back(std::move(*code));
  }
  return libraries;
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

struct DumpReader::Impl {
  explicit Impl(std::string_view input) : input_(input), reader_(input) {}
  absl::Status Initialize() {
    reader_ = Reader(input_);
    if (!reader_.Byte(&type_)) return Bad();
    collection_.reset();
    if (CollectionType(type_) != storage::ValueType::kNone) {
      auto opened = CollectionInput::Open(&reader_, type_);
      if (!opened.ok()) return opened.status();
      collection_ = std::move(*opened);
    }
    complete_ = false;
    return absl::OkStatus();
  }
  std::string_view input_;
  Reader reader_;
  std::uint8_t type_ = 0;
  bool complete_ = false;
  std::unique_ptr<CollectionInput> collection_;
};

DumpReader::DumpReader(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
DumpReader::DumpReader(DumpReader&&) noexcept = default;
DumpReader& DumpReader::operator=(DumpReader&&) noexcept = default;
DumpReader::~DumpReader() = default;

absl::StatusOr<DumpReader> DumpReader::Open(std::string_view payload) try {
  if (payload.size() < 10)
    return absl::InvalidArgumentError(
        "DUMP payload version or checksum are wrong");
  Reader footer(payload.substr(payload.size() - 10));
  std::uint16_t version = 0;
  std::uint64_t expected = 0;
  if (!footer.Le16(&version) || !footer.Le64(&expected) || version > kVersion ||
      Crc64(payload.substr(0, payload.size() - 8)) != expected)
    return absl::InvalidArgumentError(
        "DUMP payload version or checksum are wrong");
  auto impl = std::make_unique<Impl>(payload.substr(0, payload.size() - 10));
  auto status = impl->Initialize();
  if (!status.ok()) return status;
  return DumpReader(std::move(impl));
} catch (const std::bad_alloc&) {
  RecordMemoryRejection();
  return absl::ResourceExhaustedError("OOM DUMP reader");
}

bool DumpReader::collection() const noexcept {
  return CollectionType(impl_->type_) != storage::ValueType::kNone;
}
storage::ValueType DumpReader::value_type() const noexcept {
  const auto collection = CollectionType(impl_->type_);
  if (collection != storage::ValueType::kNone) return collection;
  if (impl_->type_ == kString) return storage::ValueType::kString;
  if (impl_->type_ == kStreamListpacks || impl_->type_ == kStreamListpacks2 ||
      impl_->type_ == kStreamListpacks3)
    return storage::ValueType::kStream;
  return storage::ValueType::kNone;
}
std::optional<std::uint64_t> DumpReader::expected_items() const noexcept {
  return impl_->collection_ ? impl_->collection_->expected() : std::nullopt;
}
absl::StatusOr<storage::CollectionPage> DumpReader::ReadCollectionPage() {
  if (!impl_->collection_ || impl_->complete_)
    return Bad("no active DUMP collection");
  auto page = impl_->collection_->Next(&impl_->reader_);
  if (page.ok() && page->done_) {
    if (!impl_->reader_.done()) return Bad("trailing object data");
    impl_->complete_ = true;
  }
  return page;
}
absl::StatusOr<storage::RawValue> DumpReader::ReadRawValue() {
  if (collection() || impl_->complete_)
    return Bad("DUMP raw value is not active");
  auto logical = DecodeRdbObject(&impl_->reader_, impl_->type_);
  if (!logical.ok()) return logical.status();
  if (!impl_->reader_.done()) return Bad("trailing object data");
  impl_->complete_ = true;
  return EncodeRaw(std::move(*logical));
}
absl::Status DumpReader::Rewind() { return impl_->Initialize(); }

}  // namespace lavik::rdb
