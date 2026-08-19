#include "string_command.h"

#include <algorithm>
#include <bit>
#include <cmath>
#include <limits>
#include <new>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <vector>

#include "absl/strings/str_cat.h"
#include "celer/runtime/cross_core.h"
#include "keylane/expiration.h"
#include "keylane/memory.h"
#include "keylane/redis_parse.h"
#include "keylane/resp.h"
#include "keylane/storage/format.h"
#include "keylane/tx/transaction.h"
#include "keylane/tx/tx_shard.h"

namespace keylane {

namespace {

storage::StorageEngine* g_storage = nullptr;

CommandReply Built(std::string_view encoded) {
  CommandReply reply;
  reply.encoded_ = encoded;
  return reply;
}

std::string StorageError(const absl::Status& status) {
  return status.message().starts_with("WRONGTYPE ")
             ? EncodeError(status.message())
             : EncodeError(absl::StrCat("ERR ", status.message()));
}

absl::Status WrongType() {
  return absl::InvalidArgumentError(
      "WRONGTYPE Operation against a key holding the wrong kind of value");
}

celer::Task<absl::StatusOr<std::optional<storage::RawValue>>>
ReadOptionalStringLocked(std::uint8_t db_id, std::string_view key,
                         const storage::Digest& digest) {
  auto value = co_await g_storage->ReadRawValueLocked(db_id, key, digest);
  if (!value.ok()) {
    if (value.status().code() == absl::StatusCode::kNotFound) {
      co_return std::optional<storage::RawValue>{};
    }
    co_return value.status();
  }
  if (value->value_type_ != storage::ValueType::kString) co_return WrongType();
  co_return std::optional<storage::RawValue>(std::move(*value));
}

absl::StatusOr<std::uint64_t> ParseExpireAt(std::string_view text, bool seconds,
                                            bool absolute,
                                            std::string_view command) {
  return ParseRedisExpirationDeadline(
      text, seconds, absolute, command,
      PastExpirationPolicy::kRejectNonPositive);
}

std::string_view Range(std::string_view value, std::int64_t start,
                       std::int64_t stop) {
  if (start < 0 && stop < 0 && start > stop) return {};
  const std::uint64_t size = value.size();
  auto normalize = [size](std::int64_t index) {
    if (index < 0 && size <= static_cast<std::uint64_t>(INT64_MAX)) {
      if (index < -static_cast<std::int64_t>(size)) return std::int64_t{0};
      return static_cast<std::int64_t>(size) + index;
    }
    return index;
  };
  start = normalize(start);
  stop = normalize(stop);
  if (start < 0) start = 0;
  if (stop < 0) stop = 0;
  if (value.empty() || start > stop ||
      static_cast<std::uint64_t>(start) >= value.size()) {
    return {};
  }
  const std::uint64_t end = std::min<std::uint64_t>(
      static_cast<std::uint64_t>(stop), value.size() - 1);
  return value.substr(static_cast<std::size_t>(start),
                      static_cast<std::size_t>(end - start + 1));
}

constexpr std::uint64_t kMaximumBitmapOffset = storage::kMaxStringBytes * 8 - 1;

absl::StatusOr<std::uint64_t> ParseBitOffset(std::string_view text,
                                             bool allow_hash = false,
                                             unsigned width = 0) {
  bool multiply = false;
  if (allow_hash && !text.empty() && text.front() == '#') {
    multiply = true;
    text.remove_prefix(1);
  }
  std::int64_t parsed = 0;
  if (!ParseRedisInt64(text, &parsed) || parsed < 0) {
    return absl::InvalidArgumentError(
        "bit offset is not an integer or out of range");
  }
  std::uint64_t offset = static_cast<std::uint64_t>(parsed);
  if (multiply) {
    if (width == 0 || offset > kMaximumBitmapOffset / width) {
      return absl::InvalidArgumentError(
          "bit offset is not an integer or out of range");
    }
    offset *= width;
  }
  if (offset > kMaximumBitmapOffset) {
    return absl::InvalidArgumentError(
        "bit offset is not an integer or out of range");
  }
  return offset;
}

bool BitmapBit(std::string_view value, std::uint64_t offset) {
  const std::uint64_t byte = offset >> 3;
  if (byte >= value.size()) return false;
  const unsigned bit = 7 - static_cast<unsigned>(offset & 7);
  return (static_cast<unsigned char>(value[byte]) & (1u << bit)) != 0;
}

void SetBitmapBit(std::string* value, std::uint64_t offset, bool bit_value) {
  const std::size_t byte = static_cast<std::size_t>(offset >> 3);
  const unsigned bit = 7 - static_cast<unsigned>(offset & 7);
  unsigned char current = static_cast<unsigned char>((*value)[byte]);
  current &= static_cast<unsigned char>(~(1u << bit));
  current |= static_cast<unsigned char>(bit_value ? 1u << bit : 0);
  (*value)[byte] = static_cast<char>(current);
}

std::int64_t NormalizeBitmapIndex(std::int64_t index, std::int64_t length,
                                  bool clamp_high) {
  if (index < 0) index += length;
  if (index < 0) return 0;
  if (clamp_high && index >= length) return length - 1;
  return index;
}

long long CountBitmapBits(std::string_view value, std::int64_t start,
                          std::int64_t end, bool bit_unit) {
  if (start < 0 && end < 0 && start > end) return 0;
  const std::int64_t length = bit_unit
                                  ? static_cast<std::int64_t>(value.size() * 8)
                                  : static_cast<std::int64_t>(value.size());
  if (length == 0) return 0;
  start = NormalizeBitmapIndex(start, length, false);
  end = NormalizeBitmapIndex(end, length, true);
  if (start > end) return 0;

  std::uint64_t first_bit = static_cast<std::uint64_t>(start);
  std::uint64_t last_bit = static_cast<std::uint64_t>(end);
  if (!bit_unit) {
    first_bit *= 8;
    last_bit = last_bit * 8 + 7;
  }
  const std::size_t first_byte = static_cast<std::size_t>(first_bit >> 3);
  const std::size_t last_byte = static_cast<std::size_t>(last_bit >> 3);
  long long count = 0;
  for (std::size_t byte = first_byte; byte <= last_byte; ++byte) {
    unsigned char selected = static_cast<unsigned char>(value[byte]);
    if (byte == first_byte) {
      selected &= static_cast<unsigned char>(0xffu >> (first_bit & 7));
    }
    if (byte == last_byte) {
      selected &= static_cast<unsigned char>(0xffu << (7 - (last_bit & 7)));
    }
    count += std::popcount(selected);
  }
  return count;
}

long long FindBitmapBit(std::string_view value, int wanted, std::int64_t start,
                        std::int64_t end, bool bit_unit, bool end_given) {
  const std::int64_t units = bit_unit
                                 ? static_cast<std::int64_t>(value.size() * 8)
                                 : static_cast<std::int64_t>(value.size());
  if (units == 0) return -1;
  start = NormalizeBitmapIndex(start, units, false);
  end = NormalizeBitmapIndex(end, units, true);
  if (start > end) return -1;

  std::uint64_t first_bit = static_cast<std::uint64_t>(start);
  std::uint64_t last_bit = static_cast<std::uint64_t>(end);
  if (!bit_unit) {
    first_bit *= 8;
    last_bit = last_bit * 8 + 7;
  }
  const std::size_t first_byte = static_cast<std::size_t>(first_bit >> 3);
  const std::size_t last_byte = static_cast<std::size_t>(last_bit >> 3);
  for (std::size_t byte = first_byte; byte <= last_byte; ++byte) {
    const std::uint64_t byte_first = static_cast<std::uint64_t>(byte) * 8;
    const unsigned begin =
        byte == first_byte ? static_cast<unsigned>(first_bit - byte_first) : 0;
    const unsigned finish =
        byte == last_byte ? static_cast<unsigned>(last_bit - byte_first) : 7;
    const unsigned char current = static_cast<unsigned char>(value[byte]);
    if (begin == 0 && finish == 7 &&
        ((wanted == 1 && current == 0) || (wanted == 0 && current == 0xff))) {
      continue;
    }
    for (unsigned bit = begin; bit <= finish; ++bit) {
      const bool set = (current & (1u << (7 - bit))) != 0;
      if (set == (wanted != 0)) {
        return static_cast<long long>(byte_first + bit);
      }
    }
  }
  if (wanted == 0 && !end_given) {
    return static_cast<long long>(value.size() * 8);
  }
  return -1;
}

enum class BitFieldOpcode : std::uint8_t { kGet, kSet, kIncrement };
enum class BitFieldOverflow : std::uint8_t { kWrap, kSaturate, kFail };

struct BitFieldOperation {
  BitFieldOpcode opcode_ = BitFieldOpcode::kGet;
  BitFieldOverflow overflow_ = BitFieldOverflow::kWrap;
  std::uint64_t offset_ = 0;
  std::int64_t operand_ = 0;
  unsigned width_ = 0;
  bool signed_ = false;
};

struct BitFieldPlan {
  std::vector<BitFieldOperation> operations_;
  bool writes_ = false;
  std::uint64_t highest_write_bit_ = 0;
};

absl::StatusOr<std::pair<bool, unsigned>> ParseBitFieldType(
    std::string_view text) {
  if (text.size() < 2 || (text.front() != 'i' && text.front() != 'u')) {
    return absl::InvalidArgumentError(
        "Invalid bitfield type. Use something like i16 u8. Note that u64 is "
        "not supported but i64 is.");
  }
  const bool is_signed = text.front() == 'i';
  std::int64_t width = 0;
  if (!ParseRedisInt64(text.substr(1), &width) || width < 1 ||
      width > (is_signed ? 64 : 63)) {
    return absl::InvalidArgumentError(
        "Invalid bitfield type. Use something like i16 u8. Note that u64 is "
        "not supported but i64 is.");
  }
  return std::pair<bool, unsigned>{is_signed, static_cast<unsigned>(width)};
}

absl::StatusOr<BitFieldPlan> ParseBitFieldPlan(const CommandRequest& request) {
  BitFieldPlan plan;
  BitFieldOverflow overflow = BitFieldOverflow::kWrap;
  const auto& args = request.args_;
  for (std::size_t i = 2; i < args.size();) {
    if (RedisEqualsIgnoreCase(args[i], "overflow")) {
      if (i + 1 >= args.size())
        return absl::InvalidArgumentError("syntax error");
      if (RedisEqualsIgnoreCase(args[i + 1], "wrap"))
        overflow = BitFieldOverflow::kWrap;
      else if (RedisEqualsIgnoreCase(args[i + 1], "sat"))
        overflow = BitFieldOverflow::kSaturate;
      else if (RedisEqualsIgnoreCase(args[i + 1], "fail"))
        overflow = BitFieldOverflow::kFail;
      else
        return absl::InvalidArgumentError("Invalid OVERFLOW type specified");
      i += 2;
      continue;
    }

    BitFieldOpcode opcode;
    std::size_t required = 0;
    if (RedisEqualsIgnoreCase(args[i], "get")) {
      opcode = BitFieldOpcode::kGet;
      required = 3;
    } else if (RedisEqualsIgnoreCase(args[i], "set")) {
      opcode = BitFieldOpcode::kSet;
      required = 4;
    } else if (RedisEqualsIgnoreCase(args[i], "incrby")) {
      opcode = BitFieldOpcode::kIncrement;
      required = 4;
    } else {
      return absl::InvalidArgumentError("syntax error");
    }
    if (args.size() - i < required)
      return absl::InvalidArgumentError("syntax error");
    auto type = ParseBitFieldType(args[i + 1]);
    if (!type.ok()) return type.status();
    auto offset = ParseBitOffset(args[i + 2], true, type->second);
    if (!offset.ok()) return offset.status();

    BitFieldOperation operation{
        .opcode_ = opcode,
        .overflow_ = overflow,
        .offset_ = *offset,
        .operand_ = 0,
        .width_ = type->second,
        .signed_ = type->first,
    };
    if (opcode != BitFieldOpcode::kGet) {
      if (!ParseRedisInt64(args[i + 3], &operation.operand_)) {
        return absl::InvalidArgumentError(
            "value is not an integer or out of range");
      }
      const std::uint64_t last = operation.offset_ + operation.width_ - 1;
      plan.writes_ = true;
      plan.highest_write_bit_ = std::max(plan.highest_write_bit_, last);
    }
    plan.operations_.push_back(operation);
    i += required;
  }
  if (request.kind_ == CommandKind::kBitFieldRo && plan.writes_) {
    return absl::InvalidArgumentError(
        "BITFIELD_RO only supports the GET subcommand");
  }
  return plan;
}

std::uint64_t ReadUnsignedBitField(std::string_view value, std::uint64_t offset,
                                   unsigned width) {
  std::uint64_t result = 0;
  for (unsigned i = 0; i < width; ++i) {
    result = (result << 1) | (BitmapBit(value, offset + i) ? 1 : 0);
  }
  return result;
}

std::int64_t ReadSignedBitField(std::string_view value, std::uint64_t offset,
                                unsigned width) {
  const std::uint64_t raw = ReadUnsignedBitField(value, offset, width);
  if (width == 64) return std::bit_cast<std::int64_t>(raw);
  if ((raw & (std::uint64_t{1} << (width - 1))) == 0)
    return static_cast<std::int64_t>(raw);
  return static_cast<std::int64_t>(raw - (std::uint64_t{1} << width));
}

void WriteUnsignedBitField(std::string* value, std::uint64_t offset,
                           unsigned width, std::uint64_t raw) {
  for (unsigned i = 0; i < width; ++i) {
    const bool bit = (raw & (std::uint64_t{1} << (width - 1 - i))) != 0;
    SetBitmapBit(value, offset + i, bit);
  }
}

struct BitFieldResult {
  bool failed_ = false;
  std::int64_t reply_ = 0;
  std::uint64_t raw_ = 0;
};

BitFieldResult ApplySignedBitField(std::int64_t old,
                                   const BitFieldOperation& operation) {
  const unsigned width = operation.width_;
  const std::int64_t maximum =
      width == 64
          ? std::numeric_limits<std::int64_t>::max()
          : static_cast<std::int64_t>((std::uint64_t{1} << (width - 1)) - 1);
  const std::int64_t minimum =
      width == 64 ? std::numeric_limits<std::int64_t>::min() : -maximum - 1;
  bool overflow = false;
  bool above = false;
  std::int64_t value = operation.operand_;
  if (operation.opcode_ == BitFieldOpcode::kIncrement) {
    if (operation.operand_ > 0 && old > maximum - operation.operand_) {
      overflow = above = true;
    } else if (operation.operand_ < 0 && old < minimum - operation.operand_) {
      overflow = true;
    } else {
      value = old + operation.operand_;
    }
  } else if (value > maximum) {
    overflow = above = true;
  } else if (value < minimum) {
    overflow = true;
  }
  if (overflow && operation.overflow_ == BitFieldOverflow::kFail)
    return {.failed_ = true};
  if (overflow && operation.overflow_ == BitFieldOverflow::kSaturate) {
    value = above ? maximum : minimum;
  } else if (overflow) {
    const std::uint64_t mask = width == 64
                                   ? std::numeric_limits<std::uint64_t>::max()
                                   : (std::uint64_t{1} << width) - 1;
    std::uint64_t raw = operation.opcode_ == BitFieldOpcode::kIncrement
                            ? static_cast<std::uint64_t>(old) +
                                  static_cast<std::uint64_t>(operation.operand_)
                            : static_cast<std::uint64_t>(operation.operand_);
    raw &= mask;
    if (width == 64) {
      value = std::bit_cast<std::int64_t>(raw);
    } else if ((raw & (std::uint64_t{1} << (width - 1))) != 0) {
      value = static_cast<std::int64_t>(raw - (std::uint64_t{1} << width));
    } else {
      value = static_cast<std::int64_t>(raw);
    }
  }
  return {.failed_ = false,
          .reply_ = operation.opcode_ == BitFieldOpcode::kSet ? old : value,
          .raw_ = static_cast<std::uint64_t>(value)};
}

BitFieldResult ApplyUnsignedBitField(std::uint64_t old,
                                     const BitFieldOperation& operation) {
  const std::uint64_t maximum = (std::uint64_t{1} << operation.width_) - 1;
  bool overflow = false;
  bool above = false;
  std::uint64_t value = 0;
  if (operation.opcode_ == BitFieldOpcode::kIncrement) {
    if (operation.operand_ >= 0) {
      const std::uint64_t increment =
          static_cast<std::uint64_t>(operation.operand_);
      if (increment > maximum - old) {
        overflow = above = true;
      } else {
        value = old + increment;
      }
    } else {
      const std::uint64_t decrement =
          static_cast<std::uint64_t>(-(operation.operand_ + 1)) + 1;
      if (decrement > old) {
        overflow = true;
      } else {
        value = old - decrement;
      }
    }
  } else if (operation.operand_ < 0) {
    overflow = true;
  } else {
    value = static_cast<std::uint64_t>(operation.operand_);
    if (value > maximum) overflow = above = true;
  }
  if (overflow && operation.overflow_ == BitFieldOverflow::kFail)
    return {.failed_ = true};
  if (overflow && operation.overflow_ == BitFieldOverflow::kSaturate) {
    value = above ? maximum : 0;
  } else if (overflow) {
    const std::uint64_t raw =
        operation.opcode_ == BitFieldOpcode::kIncrement
            ? old + static_cast<std::uint64_t>(operation.operand_)
            : static_cast<std::uint64_t>(operation.operand_);
    value = raw & maximum;
  }
  return {.failed_ = false,
          .reply_ = operation.opcode_ == BitFieldOpcode::kSet
                        ? static_cast<std::int64_t>(old)
                        : static_cast<std::int64_t>(value),
          .raw_ = value};
}

absl::StatusOr<bool> ParseBitmapUnit(std::string_view unit) {
  if (RedisEqualsIgnoreCase(unit, "bit")) return true;
  if (RedisEqualsIgnoreCase(unit, "byte")) return false;
  return absl::InvalidArgumentError("syntax error");
}

celer::Task<std::string> RunBitmapLocked(const CommandRequest& request,
                                         const storage::Digest& digest,
                                         storage::TxShardWrites* tx) {
  auto replication = tx == nullptr ? PrepareReplicationCommand(request)
                                   : std::nullopt;
  const auto& args = request.args_;
  const std::uint8_t db = request.db_id_;
  const std::string_view key = args[1];

  std::uint64_t bit_offset = 0;
  int bit_value = 0;
  if (request.kind_ == CommandKind::kGetBit ||
      request.kind_ == CommandKind::kSetBit) {
    auto parsed_offset = ParseBitOffset(args[2]);
    if (!parsed_offset.ok())
      co_return EncodeError(
          absl::StrCat("ERR ", parsed_offset.status().message()));
    bit_offset = *parsed_offset;
  }
  if (request.kind_ == CommandKind::kSetBit) {
    std::int64_t parsed = 0;
    if (!ParseRedisInt64(args[3], &parsed) || (parsed != 0 && parsed != 1)) {
      co_return EncodeError("ERR bit is not an integer or out of range");
    }
    bit_value = static_cast<int>(parsed);
  }
  if (request.kind_ == CommandKind::kBitPos) {
    std::int64_t parsed = 0;
    if (!ParseRedisInt64(args[2], &parsed)) {
      co_return EncodeError("ERR value is not an integer or out of range");
    }
    if (parsed != 0 && parsed != 1) {
      co_return EncodeError("ERR The bit argument must be 1 or 0.");
    }
    bit_value = static_cast<int>(parsed);
  }

  std::optional<BitFieldPlan> bitfield;
  if (request.kind_ == CommandKind::kBitField ||
      request.kind_ == CommandKind::kBitFieldRo) {
    auto parsed = ParseBitFieldPlan(request);
    if (!parsed.ok())
      co_return EncodeError(absl::StrCat("ERR ", parsed.status().message()));
    bitfield = std::move(*parsed);
  }

  const bool read_only = request.kind_ == CommandKind::kGetBit ||
                         request.kind_ == CommandKind::kBitCount ||
                         request.kind_ == CommandKind::kBitPos ||
                         (bitfield.has_value() && !bitfield->writes_);
  std::string reply;
  auto callback = [&](std::optional<storage::CompactValueView> current)
      -> absl::StatusOr<storage::CompactValueUpdate> {
    const bool exists = current.has_value();
    const std::string_view old =
        exists ? current->encoded_ : std::string_view{};
    if (request.kind_ == CommandKind::kGetBit) {
      reply = EncodeInteger(BitmapBit(old, bit_offset) ? 1 : 0);
      return storage::CompactValueUpdate{};
    }
    if (request.kind_ == CommandKind::kSetBit) {
      const bool previous = BitmapBit(old, bit_offset);
      reply = EncodeInteger(previous ? 1 : 0);
      const std::size_t required =
          static_cast<std::size_t>((bit_offset >> 3) + 1);
      if (required <= old.size() && previous == (bit_value != 0)) {
        return storage::CompactValueUpdate{};
      }
      std::string next(old);
      next.resize(std::max(next.size(), required), '\0');
      SetBitmapBit(&next, bit_offset, bit_value != 0);
      return storage::CompactValueUpdate{
          .changed_ = true,
          .encoded_ = std::move(next),
          .logical_size_ = required > old.size() ? required : old.size(),
          .expire_at_ms_ = std::nullopt,
      };
    }
    if (request.kind_ == CommandKind::kBitCount) {
      if (!exists) {
        reply = EncodeInteger(0);
        return storage::CompactValueUpdate{};
      }
      if (args.size() != 2 && args.size() != 4 && args.size() != 5)
        return absl::InvalidArgumentError("syntax error");
      std::int64_t start = 0;
      std::int64_t end = static_cast<std::int64_t>(old.size()) - 1;
      bool bit_unit = false;
      if (args.size() >= 4 && (!ParseRedisInt64(args[2], &start) ||
                               !ParseRedisInt64(args[3], &end))) {
        return absl::InvalidArgumentError(
            "value is not an integer or out of range");
      }
      if (args.size() == 5) {
        auto unit = ParseBitmapUnit(args[4]);
        if (!unit.ok()) return unit.status();
        bit_unit = *unit;
      }
      reply = EncodeInteger(CountBitmapBits(old, start, end, bit_unit));
      return storage::CompactValueUpdate{};
    }
    if (request.kind_ == CommandKind::kBitPos) {
      if (!exists) {
        reply = EncodeInteger(bit_value == 0 ? 0 : -1);
        return storage::CompactValueUpdate{};
      }
      if (args.size() < 3 || args.size() > 6) {
        return absl::InvalidArgumentError("syntax error");
      }
      std::int64_t start = 0;
      std::int64_t end = static_cast<std::int64_t>(old.size()) - 1;
      bool bit_unit = false;
      const bool end_given = args.size() >= 5;
      if (args.size() >= 4 && !ParseRedisInt64(args[3], &start)) {
        return absl::InvalidArgumentError(
            "value is not an integer or out of range");
      }
      if (end_given && !ParseRedisInt64(args[4], &end)) {
        return absl::InvalidArgumentError(
            "value is not an integer or out of range");
      }
      if (args.size() == 6) {
        auto unit = ParseBitmapUnit(args[5]);
        if (!unit.ok()) return unit.status();
        bit_unit = *unit;
        if (!end_given) return absl::InvalidArgumentError("syntax error");
      }
      if (bit_unit && !end_given) {
        end = static_cast<std::int64_t>(old.size() * 8) - 1;
      }
      reply = EncodeInteger(
          FindBitmapBit(old, bit_value, start, end, bit_unit, end_given));
      return storage::CompactValueUpdate{};
    }

    ReplyBuilder builder;
    builder.AppendArrayHeader(bitfield->operations_.size());
    std::string next = bitfield->writes_ ? std::string(old) : std::string{};
    if (bitfield->writes_) {
      const std::size_t required =
          static_cast<std::size_t>((bitfield->highest_write_bit_ >> 3) + 1);
      next.resize(std::max(next.size(), required), '\0');
    }
    for (const BitFieldOperation& operation : bitfield->operations_) {
      if (operation.opcode_ == BitFieldOpcode::kGet) {
        if (operation.signed_) {
          builder.AppendInteger(ReadSignedBitField(
              bitfield->writes_ ? std::string_view(next) : old,
              operation.offset_, operation.width_));
        } else {
          builder.AppendInteger(static_cast<long long>(ReadUnsignedBitField(
              bitfield->writes_ ? std::string_view(next) : old,
              operation.offset_, operation.width_)));
        }
        continue;
      }
      BitFieldResult result;
      if (operation.signed_) {
        result = ApplySignedBitField(
            ReadSignedBitField(next, operation.offset_, operation.width_),
            operation);
      } else {
        result = ApplyUnsignedBitField(
            ReadUnsignedBitField(next, operation.offset_, operation.width_),
            operation);
      }
      if (result.failed_) {
        builder.AppendNullBulkString();
        continue;
      }
      builder.AppendInteger(result.reply_);
      WriteUnsignedBitField(&next, operation.offset_, operation.width_,
                            result.raw_);
    }
    reply = std::move(builder).Release();
    if (!bitfield->writes_ || (exists && next == old)) {
      return storage::CompactValueUpdate{};
    }
    const std::size_t logical_size = next.size();
    return storage::CompactValueUpdate{
        .changed_ = true,
        .encoded_ = std::move(next),
        .logical_size_ = logical_size,
        .expire_at_ms_ = std::nullopt,
    };
  };

  const absl::Status status = co_await g_storage->ExecuteCompactLocked(
      db, key, digest, storage::ValueType::kString, read_only, callback, tx, 0,
      replication ? &*replication : nullptr);
  co_return status.ok() ? reply : StorageError(status);
}

celer::Task<std::string> RunStringLocked(const CommandRequest& request,
                                         const storage::Digest& digest,
                                         storage::TxShardWrites* tx) {
  const auto& args = request.args_;
  const std::uint8_t db = request.db_id_;
  const std::string_view key = args[1];
  std::vector<std::string> canonical_args;
  switch (request.kind_) {
    case CommandKind::kSetEx:
    case CommandKind::kPSetEx:
      canonical_args = {"SET", args[1], args[3]};
      break;
    case CommandKind::kSetNx:
    case CommandKind::kGetSet:
      canonical_args = {"SET", args[1], args[2]};
      break;
    case CommandKind::kGetDel:
      canonical_args = {"DEL", args[1]};
      break;
    default:
      break;
  }
  auto replication = tx == nullptr && request.kind_ != CommandKind::kGetEx
                         ? PrepareReplicationCommand(request,
                                                     std::move(canonical_args))
                         : std::nullopt;
  std::optional<std::uint64_t> getex_deadline;

  if (request.kind_ == CommandKind::kSetEx ||
      request.kind_ == CommandKind::kPSetEx) {
    const bool seconds = request.kind_ == CommandKind::kSetEx;
    auto expire_at =
        ParseExpireAt(args[2], seconds, false, seconds ? "setex" : "psetex");
    if (!expire_at.ok()) {
      co_return EncodeError(absl::StrCat("ERR ", expire_at.status().message()));
    }
    storage::SetOptions options;
    options.expire_at_ms_ = *expire_at;
    auto result =
        co_await g_storage->SetLocked(db, key, digest, args[3], options, tx,
                                      replication ? &*replication : nullptr);
    co_return result.ok() ? EncodeSimpleString("OK")
                          : StorageError(result.status());
  }

  if (request.kind_ == CommandKind::kSetNx) {
    storage::SetOptions options;
    options.condition_ = storage::SetCondition::kIfAbsent;
    auto result =
        co_await g_storage->SetLocked(db, key, digest, args[2], options, tx,
                                      replication ? &*replication : nullptr);
    co_return result.ok() ? EncodeInteger(result->applied_ ? 1 : 0)
                          : StorageError(result.status());
  }

  if (request.kind_ == CommandKind::kGetSet) {
    storage::SetOptions options;
    options.return_old_value_ = true;
    auto result =
        co_await g_storage->SetLocked(db, key, digest, args[2], options, tx,
                                      replication ? &*replication : nullptr);
    if (!result.ok()) co_return StorageError(result.status());
    if (!result->old_value_) co_return EncodeNullBulkString();
    const auto bytes = result->old_value_->network_bytes();
    co_return std::string(reinterpret_cast<const char*>(bytes.data()),
                          bytes.size());
  }

  std::int64_t first_integer = 0;
  if ((request.kind_ == CommandKind::kSetRange ||
       request.kind_ == CommandKind::kIncrBy ||
       request.kind_ == CommandKind::kDecrBy) &&
      !ParseRedisInt64(args[2], &first_integer)) {
    co_return EncodeError("ERR value is not an integer or out of range");
  }
  if (request.kind_ == CommandKind::kSetRange && first_integer < 0) {
    co_return EncodeError("ERR offset is out of range");
  }
  if (request.kind_ == CommandKind::kGetEx) {
    const bool persist =
        args.size() == 3 && RedisEqualsIgnoreCase(args[2], "persist");
    const bool expiration =
        args.size() == 4 && (RedisEqualsIgnoreCase(args[2], "ex") ||
                             RedisEqualsIgnoreCase(args[2], "px") ||
                             RedisEqualsIgnoreCase(args[2], "exat") ||
                             RedisEqualsIgnoreCase(args[2], "pxat"));
    if (args.size() != 2 && !persist && !expiration) {
      co_return EncodeError("ERR syntax error");
    }
    if (tx == nullptr && persist) {
      replication =
          PrepareReplicationCommand(request, {"PERSIST", args[1]});
    } else if (expiration) {
      const bool ex = RedisEqualsIgnoreCase(args[2], "ex");
      const bool exat = RedisEqualsIgnoreCase(args[2], "exat");
      const bool pxat = RedisEqualsIgnoreCase(args[2], "pxat");
      auto parsed =
          ParseExpireAt(args[3], ex || exat, exat || pxat, "getex");
      if (!parsed.ok()) {
        co_return EncodeError(absl::StrCat("ERR ", parsed.status().message()));
      }
      getex_deadline = *parsed;
      if (tx == nullptr) {
        replication = PrepareReplicationCommand(
            request, {"PEXPIREAT", args[1], std::to_string(*parsed)});
      }
    }
  }

  std::string reply;
  auto callback = [&](std::optional<storage::CompactValueView> current)
      -> absl::StatusOr<storage::CompactValueUpdate> {
    const bool exists = current.has_value();
    const std::string_view old =
        exists ? current->encoded_ : std::string_view{};
    const std::uint64_t ttl = exists ? current->expire_at_ms_ : 0;
    auto changed = [](std::string encoded,
                      std::optional<std::uint64_t> expire_at = std::nullopt) {
      const std::size_t size = encoded.size();
      return storage::CompactValueUpdate{
          .changed_ = true,
          .encoded_ = std::move(encoded),
          .logical_size_ = size,
          .expire_at_ms_ = expire_at,
      };
    };

    if (request.kind_ == CommandKind::kGetDel) {
      reply = exists ? EncodeBulkString(old) : EncodeNullBulkString();
      return exists ? storage::CompactValueUpdate{.changed_ = true,
                                                  .erase_ = true,
                                                  .encoded_ = {},
                                                  .logical_size_ = 0,
                                                  .expire_at_ms_ = std::nullopt}
                    : storage::CompactValueUpdate{};
    }

    if (request.kind_ == CommandKind::kGetEx) {
      if (!exists) {
        reply = EncodeNullBulkString();
        return storage::CompactValueUpdate{};
      }
      reply = EncodeBulkString(old);
      if (args.size() == 2) return storage::CompactValueUpdate{};
      if (args.size() == 3) {
        if (ttl == 0) return storage::CompactValueUpdate{};
        return storage::CompactValueUpdate{
            .changed_ = true,
            .reuse_encoded_ = true,
            .encoded_ = {},
            .logical_size_ = old.size(),
            .expire_at_ms_ = 0,
        };
      }
      if (!getex_deadline.has_value()) {
        return absl::InternalError("GETEX deadline was not parsed");
      }
      if (*getex_deadline <= RedisUnixTimeMillis()) {
        return storage::CompactValueUpdate{.changed_ = true,
                                           .erase_ = true,
                                           .encoded_ = {},
                                           .logical_size_ = 0,
                                           .expire_at_ms_ = std::nullopt};
      }
      return storage::CompactValueUpdate{
          .changed_ = true,
          .reuse_encoded_ = true,
          .encoded_ = {},
          .logical_size_ = old.size(),
          .expire_at_ms_ = *getex_deadline,
      };
    }

    if (request.kind_ == CommandKind::kSetRange) {
      if (args[3].empty()) {
        reply = EncodeInteger(static_cast<long long>(old.size()));
        return storage::CompactValueUpdate{};
      }
      const std::uint64_t offset = static_cast<std::uint64_t>(first_integer);
      if (offset > storage::kMaxStringBytes ||
          args[3].size() > storage::kMaxStringBytes - offset) {
        return absl::OutOfRangeError(
            "string exceeds maximum allowed size (proto-max-bulk-len)");
      }
      std::string next(old);
      if (next.size() < offset + args[3].size()) {
        next.resize(static_cast<std::size_t>(offset + args[3].size()), '\0');
      }
      next.replace(static_cast<std::size_t>(offset), args[3].size(), args[3]);
      reply = EncodeInteger(static_cast<long long>(next.size()));
      return changed(std::move(next));
    }

    if (request.kind_ == CommandKind::kAppend) {
      if (old.size() > storage::kMaxStringBytes ||
          args[2].size() > storage::kMaxStringBytes - old.size()) {
        return absl::OutOfRangeError(
            "string exceeds maximum allowed size (proto-max-bulk-len)");
      }
      std::string next(old);
      next.append(args[2]);
      reply = EncodeInteger(static_cast<long long>(next.size()));
      return changed(std::move(next));
    }

    if (request.kind_ == CommandKind::kIncrByFloat) {
      long double previous = 0;
      long double increment = 0;
      if ((exists && !ParseRedisLongDouble(old, &previous)) ||
          !ParseRedisLongDouble(args[2], &increment)) {
        return absl::InvalidArgumentError("value is not a valid float");
      }
      const long double result = previous + increment;
      if (!std::isfinite(result)) {
        return absl::InvalidArgumentError(
            "increment would produce NaN or Infinity");
      }
      std::string formatted;
      if (!FormatRedisLongDouble(result, &formatted)) {
        return absl::InternalError("failed to format String float");
      }
      reply = EncodeBulkString(formatted);
      if (replication.has_value()) {
        replication->args_ = {"SET", std::string(key), formatted, "KEEPTTL"};
      }
      return changed(std::move(formatted));
    }

    std::int64_t previous = 0;
    if (exists && !ParseRedisInt64(old, &previous)) {
      return absl::InvalidArgumentError(
          "value is not an integer or out of range");
    }
    std::int64_t delta = 0;
    switch (request.kind_) {
      case CommandKind::kIncr:
        delta = 1;
        break;
      case CommandKind::kDecr:
        delta = -1;
        break;
      case CommandKind::kIncrBy:
        delta = first_integer;
        break;
      case CommandKind::kDecrBy:
        if (first_integer == std::numeric_limits<std::int64_t>::min()) {
          return absl::InvalidArgumentError("decrement would overflow");
        }
        delta = -first_integer;
        break;
      default:
        return absl::InvalidArgumentError("unsupported String command path");
    }
    std::int64_t result = 0;
    if (__builtin_add_overflow(previous, delta, &result)) {
      return absl::InvalidArgumentError(
          "increment or decrement would overflow");
    }
    reply = EncodeInteger(result);
    std::string formatted = std::to_string(result);
    if (replication.has_value()) {
      replication->args_ = {"SET", std::string(key), formatted, "KEEPTTL"};
    }
    return changed(std::move(formatted));
  };

  const absl::Status status = co_await g_storage->ExecuteCompactLocked(
      db, key, digest, storage::ValueType::kString, false, callback, tx, 0,
      replication ? &*replication : nullptr);
  co_return status.ok() ? reply : StorageError(status);
}

struct LcsOptions {
  bool length_only_ = false;
  bool indexes_ = false;
  bool with_match_length_ = false;
  std::uint32_t minimum_match_length_ = 0;
};

absl::StatusOr<LcsOptions> ParseLcsOptions(
    const std::vector<std::string>& args) {
  LcsOptions options;
  for (std::size_t i = 3; i < args.size(); ++i) {
    if (RedisEqualsIgnoreCase(args[i], "len")) {
      options.length_only_ = true;
    } else if (RedisEqualsIgnoreCase(args[i], "idx")) {
      options.indexes_ = true;
    } else if (RedisEqualsIgnoreCase(args[i], "withmatchlen")) {
      options.with_match_length_ = true;
    } else if (RedisEqualsIgnoreCase(args[i], "minmatchlen") &&
               i + 1 < args.size()) {
      std::int64_t parsed = 0;
      if (!ParseRedisInt64(args[++i], &parsed)) {
        return absl::InvalidArgumentError(
            "value is not an integer or out of range");
      }
      if (parsed > 0) {
        options.minimum_match_length_ = static_cast<std::uint32_t>(
            std::min<std::int64_t>(parsed, UINT32_MAX));
      }
    } else {
      return absl::InvalidArgumentError("syntax error");
    }
  }
  if (options.length_only_ && options.indexes_) {
    return absl::InvalidArgumentError(
        "If you want both the length and indexes, please just use IDX.");
  }
  return options;
}

struct LcsMatch {
  std::uint32_t a_start_ = 0;
  std::uint32_t a_end_ = 0;
  std::uint32_t b_start_ = 0;
  std::uint32_t b_end_ = 0;
  std::uint32_t length_ = 0;
};

absl::StatusOr<std::string> BuildLcsReply(std::string_view a,
                                          std::string_view b,
                                          const LcsOptions& options) {
  if (a.size() >= UINT32_MAX - 1 || b.size() >= UINT32_MAX - 1) {
    return absl::OutOfRangeError("String too long for LCS");
  }
  if (options.length_only_) {
    if (b.size() > a.size()) std::swap(a, b);
    const std::size_t row_cells = b.size() + 1;
    if (row_cells > std::numeric_limits<std::size_t>::max() / 2 ||
        row_cells * 2 > storage::kMaxStringBytes / sizeof(std::uint32_t)) {
      return absl::ResourceExhaustedError(
          "Insufficient memory, transient memory for LCS exceeds "
          "proto-max-bulk-len");
    }
    const std::size_t bytes = row_cells * 2 * sizeof(std::uint32_t);
    if (WouldExceedMemoryLimit(bytes)) {
      RecordMemoryRejection();
      return absl::ResourceExhaustedError(
          "Insufficient memory, failed allocating transient memory for LCS");
    }
    std::vector<std::uint32_t> previous;
    std::vector<std::uint32_t> current;
    try {
      previous.assign(row_cells, 0);
      current.assign(row_cells, 0);
    } catch (const std::bad_alloc&) {
      return absl::ResourceExhaustedError(
          "Insufficient memory, failed allocating transient memory for LCS");
    } catch (const std::length_error&) {
      return absl::ResourceExhaustedError(
          "Insufficient memory, failed allocating transient memory for LCS");
    }
    for (char left : a) {
      current[0] = 0;
      for (std::size_t column = 1; column <= b.size(); ++column) {
        current[column] = left == b[column - 1]
                              ? previous[column - 1] + 1
                              : std::max(previous[column], current[column - 1]);
      }
      previous.swap(current);
    }
    return EncodeInteger(previous.back());
  }
  const std::uint64_t rows = a.size() + 1;
  const std::uint64_t columns = b.size() + 1;
  if (rows > std::numeric_limits<std::size_t>::max() / columns) {
    return absl::ResourceExhaustedError(
        "Insufficient memory, failed allocating transient memory for LCS");
  }
  const std::size_t cells = static_cast<std::size_t>(rows * columns);
  if (cells > storage::kMaxStringBytes / sizeof(std::uint32_t)) {
    return absl::ResourceExhaustedError(
        "Insufficient memory, transient memory for LCS exceeds "
        "proto-max-bulk-len");
  }
  const std::size_t bytes = cells * sizeof(std::uint32_t);
  if (WouldExceedMemoryLimit(bytes)) {
    RecordMemoryRejection();
    return absl::ResourceExhaustedError(
        "Insufficient memory, failed allocating transient memory for LCS");
  }
  std::vector<std::uint32_t> table;
  try {
    table.assign(cells, 0);
  } catch (const std::bad_alloc&) {
    return absl::ResourceExhaustedError(
        "Insufficient memory, failed allocating transient memory for LCS");
  } catch (const std::length_error&) {
    return absl::ResourceExhaustedError(
        "Insufficient memory, failed allocating transient memory for LCS");
  }
  auto at = [&](std::size_t row, std::size_t column) -> std::uint32_t& {
    return table[column + row * columns];
  };
  for (std::size_t i = 1; i <= a.size(); ++i) {
    for (std::size_t j = 1; j <= b.size(); ++j) {
      at(i, j) = a[i - 1] == b[j - 1] ? at(i - 1, j - 1) + 1
                                      : std::max(at(i - 1, j), at(i, j - 1));
    }
  }
  const std::uint32_t length = at(a.size(), b.size());
  std::string result(length, '\0');
  std::uint32_t result_index = length;
  std::uint32_t i = a.size();
  std::uint32_t j = b.size();
  const std::uint32_t unset = a.size();
  std::uint32_t a_start = unset, a_end = 0, b_start = 0, b_end = 0;
  std::vector<LcsMatch> matches;
  while (i > 0 && j > 0) {
    bool emit = false;
    if (a[i - 1] == b[j - 1]) {
      result[result_index - 1] = a[i - 1];
      if (a_start == unset) {
        a_start = a_end = i - 1;
        b_start = b_end = j - 1;
      } else if (a_start == i && b_start == j) {
        --a_start;
        --b_start;
      } else {
        emit = true;
      }
      if (a_start == 0 || b_start == 0) emit = true;
      --result_index;
      --i;
      --j;
    } else {
      if (at(i - 1, j) > at(i, j - 1))
        --i;
      else
        --j;
      if (a_start != unset) emit = true;
    }
    if (emit) {
      const std::uint32_t match_length = a_end - a_start + 1;
      if (options.minimum_match_length_ == 0 ||
          match_length >= options.minimum_match_length_) {
        matches.push_back({a_start, a_end, b_start, b_end, match_length});
      }
      a_start = unset;
    }
  }
  if (!options.indexes_) return EncodeBulkString(result);

  ReplyBuilder builder;
  builder.AppendArrayHeader(4);
  builder.AppendBulkString("matches");
  builder.AppendArrayHeader(matches.size());
  for (const LcsMatch& match : matches) {
    builder.AppendArrayHeader(options.with_match_length_ ? 3 : 2);
    builder.AppendArrayHeader(2);
    builder.AppendInteger(match.a_start_);
    builder.AppendInteger(match.a_end_);
    builder.AppendArrayHeader(2);
    builder.AppendInteger(match.b_start_);
    builder.AppendInteger(match.b_end_);
    if (options.with_match_length_) builder.AppendInteger(match.length_);
  }
  builder.AppendBulkString("len");
  builder.AppendInteger(length);
  return std::move(builder).Release();
}

struct LcsReadContext {
  const CommandRequest* request_ = nullptr;
  std::string values_[2];
};

celer::Task<absl::Status> LcsReadCallback(void* opaque,
                                          const tx::ShardSlice& slice) {
  auto* context = static_cast<LcsReadContext*>(opaque);
  for (const tx::TxKey& key : slice.keys_) {
    auto value = co_await ReadOptionalStringLocked(
        context->request_->db_id_, context->request_->args_[key.arg_index_],
        key.digest_);
    if (!value.ok()) {
      if (value.status().message().starts_with("WRONGTYPE ")) {
        co_return absl::InvalidArgumentError(
            "The specified keys must contain string values");
      }
      co_return value.status();
    }
    context->values_[key.arg_index_ - 1] =
        value->has_value() ? std::move((**value).encoded_) : std::string{};
    if (context->request_->args_[1] == context->request_->args_[2]) {
      context->values_[0] = context->values_[key.arg_index_ - 1];
      context->values_[1] = context->values_[key.arg_index_ - 1];
    }
  }
  co_return absl::OkStatus();
}

enum class BitOp : std::uint8_t { kAnd, kOr, kXor, kNot };

absl::StatusOr<BitOp> ParseBitOp(const CommandRequest& request) {
  const std::string_view name = request.args_[1];
  BitOp operation;
  if (RedisEqualsIgnoreCase(name, "and"))
    operation = BitOp::kAnd;
  else if (RedisEqualsIgnoreCase(name, "or"))
    operation = BitOp::kOr;
  else if (RedisEqualsIgnoreCase(name, "xor"))
    operation = BitOp::kXor;
  else if (RedisEqualsIgnoreCase(name, "not"))
    operation = BitOp::kNot;
  else
    return absl::InvalidArgumentError("syntax error");
  if (operation == BitOp::kNot && request.args_.size() != 4) {
    return absl::InvalidArgumentError(
        "BITOP NOT must be called with a single source key.");
  }
  return operation;
}

std::string ComputeBitOp(BitOp operation,
                         const std::vector<std::string>& inputs) {
  std::size_t maximum = 0;
  for (std::size_t i = 3; i < inputs.size(); ++i)
    maximum = std::max(maximum, inputs[i].size());
  std::string output(maximum, '\0');
  for (std::size_t byte = 0; byte < maximum; ++byte) {
    unsigned char result = byte < inputs[3].size()
                               ? static_cast<unsigned char>(inputs[3][byte])
                               : 0;
    if (operation == BitOp::kNot) {
      result = static_cast<unsigned char>(~result);
    } else {
      for (std::size_t input = 4; input < inputs.size(); ++input) {
        const unsigned char value =
            byte < inputs[input].size()
                ? static_cast<unsigned char>(inputs[input][byte])
                : 0;
        if (operation == BitOp::kAnd)
          result &= value;
        else if (operation == BitOp::kOr)
          result |= value;
        else
          result ^= value;
      }
    }
    output[byte] = static_cast<char>(result);
  }
  return output;
}

struct BitOpContext {
  const CommandRequest* request_ = nullptr;
  BitOp operation_ = BitOp::kAnd;
  std::vector<std::string> inputs_;
  std::string output_;
};

celer::Task<absl::Status> ReadBitOpSources(BitOpContext* context,
                                           const tx::ShardSlice& slice) {
  for (const tx::TxKey& key : slice.keys_) {
    if (key.arg_index_ < 3) continue;
    auto value = co_await ReadOptionalStringLocked(
        context->request_->db_id_, context->request_->args_[key.arg_index_],
        key.digest_);
    if (!value.ok()) co_return value.status();
    context->inputs_[key.arg_index_] =
        value->has_value() ? std::move((**value).encoded_) : std::string{};
  }
  co_return absl::OkStatus();
}

celer::Task<absl::Status> BitOpReadCallback(void* opaque,
                                            const tx::ShardSlice& slice) {
  co_return co_await ReadBitOpSources(static_cast<BitOpContext*>(opaque),
                                      slice);
}

celer::Task<absl::Status> WriteBitOpDestination(BitOpContext* context,
                                                const storage::Digest& digest,
                                                storage::TxShardWrites* tx) {
  const auto& request = *context->request_;
  if (context->output_.empty()) {
    auto deleted = co_await g_storage->DeleteLocked(
        request.db_id_, request.args_[2], digest, tx);
    co_return deleted.ok() ? absl::OkStatus() : deleted.status();
  }
  auto written = co_await g_storage->SetLocked(
      request.db_id_, request.args_[2], digest, context->output_, {}, tx);
  co_return written.ok() ? absl::OkStatus() : written.status();
}

celer::Task<absl::Status> BitOpWriteCallback(void* opaque,
                                             const tx::ShardSlice& slice) {
  auto* context = static_cast<BitOpContext*>(opaque);
  for (const tx::TxKey& key : slice.keys_) {
    if (key.arg_index_ == 2) {
      co_return co_await WriteBitOpDestination(context, key.digest_, nullptr);
    }
  }
  co_return absl::OkStatus();
}

celer::Task<absl::Status> BitOpSingleShardCallback(
    void* opaque, const tx::ShardSlice& slice) {
  auto* context = static_cast<BitOpContext*>(opaque);
  absl::Status read = co_await ReadBitOpSources(context, slice);
  if (!read.ok()) co_return read;
  context->output_ = ComputeBitOp(context->operation_, context->inputs_);
  for (const tx::TxKey& key : slice.keys_) {
    if (key.arg_index_ == 2) {
      co_return co_await WriteBitOpDestination(context, key.digest_, nullptr);
    }
  }
  co_return absl::InternalError("BITOP destination key routing is incomplete");
}

}  // namespace

void InitStringCommandStorage(storage::StorageEngine* engine) {
  g_storage = engine;
}

celer::Task<CommandReply> ExecuteStringCommand(const CommandRequest& request,
                                               ReplyBuilder& reply_builder) {
  const storage::Digest digest = storage::ComputeDigest(request.args_[1]);
  const bool read_only = request.kind_ == CommandKind::kGetRange ||
                         request.kind_ == CommandKind::kSubstr;
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      request.db_id_, tx::FingerprintOf(digest),
      read_only ? tx::LockMode::kShared : tx::LockMode::kExclusive);
  co_return co_await ExecuteStringCommandLocked(request, digest, nullptr,
                                                reply_builder);
}

celer::Task<CommandReply> ExecuteStringCommandLocked(
    const CommandRequest& request, const storage::Digest& digest,
    storage::TxShardWrites* tx, ReplyBuilder& reply_builder) {
  if (request.kind_ == CommandKind::kGetRange ||
      request.kind_ == CommandKind::kSubstr) {
    std::int64_t start = 0;
    std::int64_t stop = 0;
    if (!ParseRedisInt64(request.args_[2], &start) ||
        !ParseRedisInt64(request.args_[3], &stop)) {
      co_return Built(reply_builder.AppendError(
          "ERR value is not an integer or out of range"));
    }
    auto current = co_await ReadOptionalStringLocked(
        request.db_id_, request.args_[1], digest);
    if (!current.ok()) {
      co_return Built(reply_builder.AppendRaw(StorageError(current.status())));
    }
    const std::string_view value =
        current->has_value() ? (**current).encoded_ : std::string_view{};
    co_return Built(reply_builder.AppendBulkString(Range(value, start, stop)));
  }
  std::string encoded = co_await RunStringLocked(request, digest, tx);
  co_return Built(reply_builder.AppendRaw(encoded));
}

celer::Task<CommandReply> ExecuteBitmapCommand(const CommandRequest& request,
                                               ReplyBuilder& reply_builder) {
  const storage::Digest digest = storage::ComputeDigest(request.args_[1]);
  const bool read_only = request.kind_ != CommandKind::kSetBit &&
                         request.kind_ != CommandKind::kBitField;
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      request.db_id_, tx::FingerprintOf(digest),
      read_only ? tx::LockMode::kShared : tx::LockMode::kExclusive);
  co_return co_await ExecuteBitmapCommandLocked(request, digest, nullptr,
                                                reply_builder);
}

celer::Task<CommandReply> ExecuteBitmapCommandLocked(
    const CommandRequest& request, const storage::Digest& digest,
    storage::TxShardWrites* tx, ReplyBuilder& reply_builder) {
  std::string encoded = co_await RunBitmapLocked(request, digest, tx);
  co_return Built(reply_builder.AppendRaw(encoded));
}

celer::Task<CommandReply> ExecuteBitOpCommand(const CommandRequest& request,
                                              ReplyBuilder& reply_builder) {
  auto operation = ParseBitOp(request);
  if (!operation.ok()) {
    co_return Built(reply_builder.AppendError(
        absl::StrCat("ERR ", operation.status().message())));
  }
  tx::Transaction transaction;
  transaction.AddKey(g_storage->OwnerForKey(request.args_[2]), request.db_id_,
                     storage::ComputeDigest(request.args_[2]), 2,
                     tx::LockMode::kExclusive);
  for (std::size_t argument = 3; argument < request.args_.size(); ++argument) {
    transaction.AddKey(
        g_storage->OwnerForKey(request.args_[argument]), request.db_id_,
        storage::ComputeDigest(request.args_[argument]),
        static_cast<std::uint32_t>(argument), tx::LockMode::kShared);
  }
  transaction.Seal();
  ReplicationTransactionGuard replication(request, &transaction);
  absl::Status status = co_await transaction.Schedule();
  if (!status.ok()) {
    co_return Built(
        reply_builder.AppendError(absl::StrCat("ERR ", status.message())));
  }
  BitOpContext context{
      .request_ = &request,
      .operation_ = *operation,
      .inputs_ = std::vector<std::string>(request.args_.size()),
      .output_ = {},
  };
  if (transaction.single_shard()) {
    status =
        co_await transaction.Execute(&BitOpSingleShardCallback, &context, true);
  } else {
    status = co_await transaction.Execute(&BitOpReadCallback, &context, false);
    if (status.ok()) {
      context.output_ = ComputeBitOp(context.operation_, context.inputs_);
      status =
          co_await transaction.Execute(&BitOpWriteCallback, &context, true);
    } else if (!transaction.releasing()) {
      (void)co_await transaction.Release();
    }
  }
  if (!status.ok()) {
    co_return Built(reply_builder.AppendRaw(StorageError(status)));
  }
  replication.Commit();
  co_return Built(reply_builder.AppendInteger(context.output_.size()));
}

celer::Task<std::string> ExecuteBitOpLocked(
    const CommandRequest& request, std::span<const StringExecKey> locked_keys,
    std::vector<storage::TxShardWrites>& tx_writes) {
  auto operation = ParseBitOp(request);
  if (!operation.ok())
    co_return EncodeError(absl::StrCat("ERR ", operation.status().message()));
  BitOpContext context{
      .request_ = &request,
      .operation_ = *operation,
      .inputs_ = std::vector<std::string>(request.args_.size()),
      .output_ = {},
  };
  auto find_key = [&](std::size_t argument) -> const StringExecKey* {
    for (const StringExecKey& key : locked_keys) {
      if (key.arg_ == argument) return &key;
    }
    return nullptr;
  };
  for (std::size_t argument = 3; argument < request.args_.size(); ++argument) {
    const StringExecKey* key = find_key(argument);
    if (key == nullptr)
      co_return EncodeError("ERR BITOP source key is missing");
    auto read = [&request, key, argument]() {
      return ReadOptionalStringLocked(request.db_id_, request.args_[argument],
                                      key->digest_);
    };
    auto value = key->owner_ == celer::ThisWorker().id_
                     ? co_await read()
                     : co_await celer::SubmitTaskTo(key->owner_, read);
    if (!value.ok()) co_return StorageError(value.status());
    context.inputs_[argument] =
        value->has_value() ? std::move((**value).encoded_) : std::string{};
  }
  context.output_ = ComputeBitOp(context.operation_, context.inputs_);
  const StringExecKey* destination = find_key(2);
  if (destination == nullptr)
    co_return EncodeError("ERR BITOP destination key is missing");
  auto write = [&context, destination, &tx_writes]() {
    return WriteBitOpDestination(&context, destination->digest_,
                                 &tx_writes[destination->owner_]);
  };
  absl::Status status =
      destination->owner_ == celer::ThisWorker().id_
          ? co_await write()
          : co_await celer::SubmitTaskTo(destination->owner_, write);
  co_return status.ok() ? EncodeInteger(context.output_.size())
                        : StorageError(status);
}

celer::Task<CommandReply> ExecuteLcsCommand(const CommandRequest& request,
                                            ReplyBuilder& reply_builder) {
  tx::Transaction transaction;
  for (std::size_t argument = 1; argument <= 2; ++argument) {
    transaction.AddKey(
        g_storage->OwnerForKey(request.args_[argument]), request.db_id_,
        storage::ComputeDigest(request.args_[argument]),
        static_cast<std::uint32_t>(argument), tx::LockMode::kShared);
  }
  transaction.Seal();
  absl::Status status = co_await transaction.Schedule();
  if (!status.ok()) {
    co_return Built(
        reply_builder.AppendError(absl::StrCat("ERR ", status.message())));
  }
  LcsReadContext context{.request_ = &request, .values_ = {}};
  status = co_await transaction.Execute(&LcsReadCallback, &context, true);
  if (!status.ok()) {
    co_return Built(
        reply_builder.AppendError(absl::StrCat("ERR ", status.message())));
  }
  auto options = ParseLcsOptions(request.args_);
  if (!options.ok()) {
    co_return Built(reply_builder.AppendError(
        absl::StrCat("ERR ", options.status().message())));
  }
  auto result = BuildLcsReply(context.values_[0], context.values_[1], *options);
  co_return result.ok() ? Built(reply_builder.AppendRaw(*result))
                        : Built(reply_builder.AppendError(
                              absl::StrCat("ERR ", result.status().message())));
}

celer::Task<std::string> ExecuteLcsLocked(
    const CommandRequest& request, std::span<const StringExecKey> locked_keys) {
  std::string values[2];
  for (std::size_t argument = 1; argument <= 2; ++argument) {
    const StringExecKey* key = nullptr;
    for (const StringExecKey& candidate : locked_keys) {
      if (candidate.arg_ == argument) {
        key = &candidate;
        break;
      }
    }
    if (key == nullptr) co_return EncodeError("ERR LCS key is missing");
    auto read = [&request, key, argument]() {
      return ReadOptionalStringLocked(request.db_id_, request.args_[argument],
                                      key->digest_);
    };
    auto value = key->owner_ == celer::ThisWorker().id_
                     ? co_await read()
                     : co_await celer::SubmitTaskTo(key->owner_, read);
    if (!value.ok()) {
      if (value.status().message().starts_with("WRONGTYPE ")) {
        co_return EncodeError(
            "ERR The specified keys must contain string values");
      }
      co_return StorageError(value.status());
    }
    values[argument - 1] =
        value->has_value() ? std::move((**value).encoded_) : std::string{};
  }
  if (request.args_[1] == request.args_[2]) values[1] = values[0];
  auto options = ParseLcsOptions(request.args_);
  if (!options.ok()) {
    co_return EncodeError(absl::StrCat("ERR ", options.status().message()));
  }
  auto result = BuildLcsReply(values[0], values[1], *options);
  co_return result.ok()
      ? std::move(*result)
      : EncodeError(absl::StrCat("ERR ", result.status().message()));
}

}  // namespace keylane
