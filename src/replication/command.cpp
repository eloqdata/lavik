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

#include <algorithm>
#include <charconv>
#include <cstring>
#include <iterator>
#include <limits>
#include <new>
#include <optional>

#include "absl/strings/match.h"
#include "lavik/memory.h"
#include "lavik/replication_command.h"

namespace lavik {
namespace {

constexpr std::string_view kMagic = "LRC1";
constexpr std::uint8_t kVersion = 1;
constexpr std::size_t kFixedHeaderBytes = 8;
constexpr std::size_t kMaxArgumentCount = 1024;
constexpr std::size_t kMaxArgumentBytes =
    static_cast<std::size_t>(kMaxNativeReplicationEventBytes);
constexpr std::string_view kTransactionMagic = "LTX1";
constexpr std::size_t kTransactionFixedBytes = 16;
constexpr std::size_t kMaxTransactionBitmapBytes =
    (static_cast<std::size_t>(std::numeric_limits<std::uint16_t>::max()) + 1) /
    8;

void PutU16(std::string* output, std::uint16_t value) {
  output->push_back(static_cast<char>(value));
  output->push_back(static_cast<char>(value >> 8));
}

void PutU32(std::string* output, std::uint32_t value) {
  for (unsigned shift = 0; shift < 32; shift += 8) {
    output->push_back(static_cast<char>(value >> shift));
  }
}

void PutU64(std::string* output, std::uint64_t value) {
  for (unsigned shift = 0; shift < 64; shift += 8) {
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

bool ReadU64(std::string_view input, std::size_t* offset,
             std::uint64_t* value) {
  if (*offset > input.size() || input.size() - *offset < 8) return false;
  std::uint64_t decoded = 0;
  for (unsigned byte = 0; byte < 8; ++byte) {
    decoded |= static_cast<std::uint64_t>(
                   static_cast<std::uint8_t>(input[*offset + byte]))
               << (byte * 8);
  }
  *offset += 8;
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

absl::StatusOr<std::string> EncodeReplicationTransactionEnvelope(
    const ReplicationTransactionEnvelope& envelope) {
  if (envelope.id_ == 0 || envelope.participants_.empty() ||
      envelope.payload_flow_ > std::numeric_limits<std::uint16_t>::max()) {
    return Malformed("replication transaction identity is out of range");
  }
  unsigned maximum = 0;
  for (unsigned participant : envelope.participants_) {
    if (participant > std::numeric_limits<std::uint16_t>::max()) {
      return Malformed("replication transaction participant is out of range");
    }
    maximum = std::max(maximum, participant);
  }
  const std::size_t bitmap_bytes = static_cast<std::size_t>(maximum) / 8 + 1;
  std::string encoded;
  encoded.reserve(kTransactionFixedBytes + bitmap_bytes);
  encoded.append(kTransactionMagic);
  PutU64(&encoded, envelope.id_);
  PutU16(&encoded, static_cast<std::uint16_t>(envelope.payload_flow_));
  PutU16(&encoded, static_cast<std::uint16_t>(bitmap_bytes));
  encoded.resize(kTransactionFixedBytes + bitmap_bytes, '\0');
  for (unsigned participant : envelope.participants_) {
    char& byte = encoded[kTransactionFixedBytes + participant / 8];
    const auto mask = static_cast<std::uint8_t>(1U << (participant % 8));
    if ((static_cast<std::uint8_t>(byte) & mask) != 0) {
      return Malformed("replication transaction participant is duplicated");
    }
    byte = static_cast<char>(static_cast<std::uint8_t>(byte) | mask);
  }
  const unsigned payload = envelope.payload_flow_;
  if (payload > maximum ||
      (static_cast<std::uint8_t>(
           encoded[kTransactionFixedBytes + payload / 8]) &
       static_cast<std::uint8_t>(1U << (payload % 8))) == 0) {
    return Malformed(
        "replication transaction payload flow is not a participant");
  }
  return encoded;
}

absl::StatusOr<ReplicationTransactionEnvelope>
DecodeReplicationTransactionEnvelope(std::string_view encoded) {
  if (encoded.size() <= kTransactionFixedBytes ||
      encoded.substr(0, kTransactionMagic.size()) != kTransactionMagic) {
    return Malformed("invalid replication transaction envelope magic");
  }
  std::size_t offset = kTransactionMagic.size();
  ReplicationTransactionEnvelope envelope;
  std::uint16_t payload_flow = 0;
  std::uint16_t bitmap_bytes = 0;
  if (!ReadU64(encoded, &offset, &envelope.id_) || envelope.id_ == 0 ||
      !ReadU16(encoded, &offset, &payload_flow) ||
      !ReadU16(encoded, &offset, &bitmap_bytes) || bitmap_bytes == 0 ||
      bitmap_bytes > kMaxTransactionBitmapBytes ||
      encoded.size() != kTransactionFixedBytes + bitmap_bytes ||
      static_cast<std::uint8_t>(encoded.back()) == 0) {
    return Malformed("malformed replication transaction envelope");
  }
  envelope.payload_flow_ = payload_flow;
  if (static_cast<std::size_t>(payload_flow) >=
          static_cast<std::size_t>(bitmap_bytes) * 8 ||
      (static_cast<std::uint8_t>(
           encoded[kTransactionFixedBytes + payload_flow / 8]) &
       static_cast<std::uint8_t>(1U << (payload_flow % 8))) == 0) {
    return Malformed(
        "replication transaction payload flow is not a participant");
  }
  for (std::size_t byte = 0; byte < bitmap_bytes; ++byte) {
    const std::uint8_t bits =
        static_cast<std::uint8_t>(encoded[kTransactionFixedBytes + byte]);
    for (unsigned bit = 0; bit < 8; ++bit) {
      if ((bits & static_cast<std::uint8_t>(1U << bit)) != 0) {
        envelope.participants_.push_back(static_cast<unsigned>(byte * 8 + bit));
      }
    }
  }
  if (envelope.participants_.empty()) {
    return Malformed("replication transaction participant set is empty");
  }
  return envelope;
}

bool IsReplicationTransactionEnvelope(std::string_view encoded) noexcept {
  return encoded.size() >= kTransactionMagic.size() &&
         encoded.substr(0, kTransactionMagic.size()) == kTransactionMagic;
}

// This is the ownership boundary for the source's header and view table. A
// single catch keeps allocation failure out of callers without wrapping each
// vector or string operation separately.
absl::StatusOr<ReplicationCommandPayloadSource>
ReplicationCommandPayloadSource::Create(
    std::uint8_t db_id, std::span<const std::string_view> args) try {
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
  if (source.size_ > kMaxNativeReplicationEventBytes) {
    return absl::ResourceExhaustedError(
        "replication command exceeds the native event limit");
  }
  source.args_.reserve(args.size());
  for (std::string_view arg : args) {
    if (arg.size() > kMaxArgumentBytes ||
        arg.size() > std::numeric_limits<std::uint32_t>::max() ||
        source.size_ > std::numeric_limits<std::uint64_t>::max() - arg.size()) {
      return Malformed("replication command argument is too large");
    }
    if (source.size_ > kMaxNativeReplicationEventBytes - arg.size()) {
      return absl::ResourceExhaustedError(
          "replication command exceeds the native event limit");
    }
    PutU32(&source.header_, static_cast<std::uint32_t>(arg.size()));
    source.args_.push_back(arg);
    source.size_ += arg.size();
  }
  return source;
} catch (const std::length_error&) {
  return absl::ResourceExhaustedError("replication command is too large");
}

absl::StatusOr<ReplicationCommandPayloadSource>
ReplicationCommandPayloadSource::Create(std::uint8_t db_id,
                                        std::span<const std::string> args) try {
  std::vector<std::string_view> views;
  views.reserve(args.size());
  for (const std::string& arg : args) views.push_back(arg);
  return Create(db_id, views);
} catch (const std::length_error&) {
  return absl::ResourceExhaustedError("replication command is too large");
}

bycorf::Task<absl::Status> ReplicationCommandPayloadSource::Read(
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

// Argument views point into the disposable frame, so decoding must create all
// owned strings here. Treat the complete decode as one fallible allocation
// boundary instead of checking each argument copy independently.
absl::StatusOr<ReplicatedCommand> DecodeReplicationCommand(
    std::string_view encoded) try {
  if (encoded.size() > kMaxNativeReplicationEventBytes) {
    return absl::ResourceExhaustedError(
        "replication command exceeds the native event limit");
  }
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
    command.args_.emplace_back(encoded.data() + offset, length);
    offset += length;
  }
  return command;
} catch (const std::length_error&) {
  return absl::ResourceExhaustedError("replication command is too large");
}

bool IsPublishOnlyReplicationCommand(
    const ReplicatedCommand& command) noexcept {
  if (command.db_id_ >= storage::kLogicalDatabaseCount) return false;
  const std::span<const std::string> args(command.args_);
  if (args.empty()) return false;
  auto is_publish = [](std::span<const std::string> child) {
    return child.size() == 3 && absl::EqualsIgnoreCase(child[0], "PUBLISH");
  };
  if (args[0] != kReplicatedExecCommand) return is_publish(args);
  if (args.size() < 2) return false;
  auto parse_size = [](std::string_view text, std::uint64_t* value) {
    const auto parsed =
        std::from_chars(text.data(), text.data() + text.size(), *value);
    return parsed.ec == std::errc{} && parsed.ptr == text.data() + text.size();
  };
  std::uint64_t count = 0;
  // Each child needs its DB, argc, and all three PUBLISH arguments. Check
  // the complete envelope before granting the partition-reset exemption;
  // a valid leading publication must not hide a later storage mutation.
  if (!parse_size(args[1], &count) || count == 0 ||
      count > (args.size() - 2) / 5) {
    return false;
  }
  std::size_t offset = 2;
  for (std::uint64_t index = 0; index < count; ++index) {
    std::uint64_t db_id = 0;
    std::uint64_t argc = 0;
    if (!parse_size(args[offset], &db_id) ||
        db_id >= storage::kLogicalDatabaseCount ||
        !parse_size(args[offset + 1], &argc) || argc != 3 ||
        !is_publish(args.subspan(offset + 2, 3))) {
      return false;
    }
    offset += 5;
  }
  return offset == args.size();
}

void AppendReplicationExpirationEffect(std::vector<std::string>* args,
                                       std::uint8_t command_db_id,
                                       std::uint8_t effect_db_id,
                                       std::string_view key, bool exists,
                                       std::uint64_t expire_at_ms) {
  if (args == nullptr || args->empty() || !exists) return;
  // SET already clears the previous TTL, while SetLocked canonicalizes a
  // requested or retained TTL to an absolute PXAT argument before publishing
  // the command. Wrapping it in a replicated EXEC with a second PERSIST or
  // PEXPIREAT is redundant and makes the replica write an extra transactional
  // record plus commit for every ordinary SET.
  if ((*args)[0] == "SET") return;
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

}  // namespace lavik
