#include <algorithm>
#include <charconv>
#include <cstring>
#include <iterator>
#include <limits>

#include "keylane/replication_command.h"

namespace keylane {
namespace {

constexpr std::string_view kMagic = "KRC1";
constexpr std::uint8_t kVersion = 1;
constexpr std::size_t kFixedHeaderBytes = 8;
constexpr std::size_t kMaxArgumentCount = 1024;
constexpr std::size_t kMaxArgumentBytes = 1024ULL * 1024 * 1024;

void PutU16(std::string* output, std::uint16_t value) {
  output->push_back(static_cast<char>(value));
  output->push_back(static_cast<char>(value >> 8));
}

void PutU32(std::string* output, std::uint32_t value) {
  for (unsigned shift = 0; shift < 32; shift += 8) {
    output->push_back(static_cast<char>(value >> shift));
  }
}

bool ReadU16(std::string_view input, std::size_t* offset,
             std::uint16_t* value) {
  if (*offset > input.size() || input.size() - *offset < 2) return false;
  *value =
      static_cast<std::uint8_t>(input[*offset]) |
      (static_cast<std::uint16_t>(static_cast<std::uint8_t>(input[*offset + 1]))
       << 8);
  *offset += 2;
  return true;
}

bool ReadU32(std::string_view input, std::size_t* offset,
             std::uint32_t* value) {
  if (*offset > input.size() || input.size() - *offset < 4) return false;
  std::uint32_t decoded = 0;
  for (unsigned byte = 0; byte < 4; ++byte) {
    decoded |= static_cast<std::uint32_t>(
                   static_cast<std::uint8_t>(input[*offset + byte]))
               << (byte * 8);
  }
  *offset += 4;
  *value = decoded;
  return true;
}

void CopySegment(std::string_view segment, std::uint64_t* offset,
                 std::span<std::byte>* output) {
  if (output->empty()) return;
  if (*offset >= segment.size()) {
    *offset -= segment.size();
    return;
  }
  const std::size_t begin = static_cast<std::size_t>(*offset);
  const std::size_t count = std::min(output->size(), segment.size() - begin);
  std::memcpy(output->data(), segment.data() + begin, count);
  *output = output->subspan(count);
  *offset = 0;
}

absl::Status Malformed(std::string_view message) {
  return absl::Status(absl::StatusCode::kInvalidArgument, message);
}

}  // namespace

absl::StatusOr<ReplicationCommandPayloadSource>
ReplicationCommandPayloadSource::Create(
    std::uint8_t db_id, std::span<const std::string_view> args) {
  if (db_id >= storage::kLogicalDatabaseCount) {
    return Malformed("replication command database is out of range");
  }
  if (args.empty() || args.size() > kMaxArgumentCount ||
      args.size() > std::numeric_limits<std::uint16_t>::max()) {
    return Malformed("replication command argument count is out of range");
  }

  ReplicationCommandPayloadSource source;
  source.header_.reserve(kFixedHeaderBytes +
                         args.size() * sizeof(std::uint32_t));
  source.header_.append(kMagic);
  source.header_.push_back(static_cast<char>(kVersion));
  source.header_.push_back(static_cast<char>(db_id));
  PutU16(&source.header_, static_cast<std::uint16_t>(args.size()));
  source.size_ = source.header_.size() + args.size() * sizeof(std::uint32_t);
  source.args_.reserve(args.size());
  for (std::string_view arg : args) {
    if (arg.size() > kMaxArgumentBytes ||
        arg.size() > std::numeric_limits<std::uint32_t>::max() ||
        source.size_ > std::numeric_limits<std::uint64_t>::max() - arg.size()) {
      return Malformed("replication command argument is too large");
    }
    PutU32(&source.header_, static_cast<std::uint32_t>(arg.size()));
    source.args_.push_back(arg);
    source.size_ += arg.size();
  }
  return source;
}

celer::Task<absl::Status> ReplicationCommandPayloadSource::Read(
    std::uint64_t offset, std::span<std::byte> output) {
  if (offset > size_ || output.size() > size_ - offset) {
    co_return absl::Status(absl::StatusCode::kOutOfRange,
                           "replication command source read is out of range");
  }
  if (output.empty()) co_return absl::OkStatus();
  std::span<std::byte> remaining = output;
  CopySegment(header_, &offset, &remaining);
  for (std::string_view arg : args_) {
    CopySegment(arg, &offset, &remaining);
    if (remaining.empty()) break;
  }
  if (!remaining.empty() || offset != 0) {
    co_return absl::Status(absl::StatusCode::kInternal,
                           "replication command source did not fill output");
  }
  co_return absl::OkStatus();
}

absl::StatusOr<ReplicatedCommand> DecodeReplicationCommand(
    std::string_view encoded) {
  if (encoded.size() < kFixedHeaderBytes ||
      encoded.substr(0, kMagic.size()) != kMagic) {
    return Malformed("invalid replication command magic");
  }
  if (static_cast<std::uint8_t>(encoded[4]) != kVersion) {
    return Malformed("unsupported replication command version");
  }
  ReplicatedCommand command;
  command.db_id_ = static_cast<std::uint8_t>(encoded[5]);
  if (command.db_id_ >= storage::kLogicalDatabaseCount) {
    return Malformed("replication command database is out of range");
  }
  std::size_t offset = 6;
  std::uint16_t argc = 0;
  if (!ReadU16(encoded, &offset, &argc) || argc == 0 ||
      argc > kMaxArgumentCount) {
    return Malformed("replication command argument count is out of range");
  }
  if (offset > encoded.size() ||
      static_cast<std::size_t>(argc) >
          (encoded.size() - offset) / sizeof(std::uint32_t)) {
    return Malformed("truncated replication command length table");
  }
  std::vector<std::uint32_t> lengths;
  lengths.reserve(argc);
  std::uint64_t payload_bytes = 0;
  for (std::uint16_t i = 0; i < argc; ++i) {
    std::uint32_t length = 0;
    if (!ReadU32(encoded, &offset, &length) || length > kMaxArgumentBytes ||
        payload_bytes > std::numeric_limits<std::uint64_t>::max() - length) {
      return Malformed("invalid replication command argument length");
    }
    lengths.push_back(length);
    payload_bytes += length;
  }
  if (payload_bytes != encoded.size() - offset) {
    return Malformed(payload_bytes > encoded.size() - offset
                         ? "truncated replication command payload"
                         : "trailing replication command bytes");
  }
  command.args_.reserve(argc);
  for (std::uint32_t length : lengths) {
    command.args_.emplace_back(encoded.substr(offset, length));
    offset += length;
  }
  return command;
}

void AppendReplicationExpirationEffect(
    std::vector<std::string>* args, std::uint8_t command_db_id,
    std::uint8_t effect_db_id, std::string_view key, bool exists,
    std::uint64_t expire_at_ms) {
  if (args == nullptr || args->empty() || !exists) return;
  if ((*args)[0] == kReplicatedExecCommand) {
    if (args->size() < 2) return;
    std::uint64_t count = 0;
    const std::string& encoded_count = (*args)[1];
    const char* begin = encoded_count.data();
    const char* end = begin + encoded_count.size();
    const auto parsed = std::from_chars(begin, end, count);
    if (parsed.ec != std::errc{} || parsed.ptr != end) {
      return;
    }
    (*args)[1] = std::to_string(count + 1);
  } else {
    std::vector<std::string> command = std::move(*args);
    args->clear();
    args->reserve(command.size() + 8);
    args->emplace_back(kReplicatedExecCommand);
    args->emplace_back("2");
    args->push_back(std::to_string(command_db_id));
    args->push_back(std::to_string(command.size()));
    args->insert(args->end(), std::make_move_iterator(command.begin()),
                 std::make_move_iterator(command.end()));
  }
  args->push_back(std::to_string(effect_db_id));
  if (expire_at_ms == 0) {
    args->emplace_back("2");
    args->emplace_back("PERSIST");
    args->emplace_back(key);
  } else {
    args->emplace_back("3");
    args->emplace_back("PEXPIREAT");
    args->emplace_back(key);
    args->push_back(std::to_string(expire_at_ms));
  }
}

}  // namespace keylane
