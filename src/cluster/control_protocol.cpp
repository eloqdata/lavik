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

#include "lavik/cluster/control_protocol.h"

#include <sys/random.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "lavik/status_macros.h"

namespace lavik::cluster::control {
namespace {

constexpr std::size_t kFrameCrcOffset = 24;
constexpr std::size_t kTransferChunkEnvelopeBytes = 16 + 8 + 4;
constexpr std::size_t kMaxTransferChunkBytes =
    kMaxFramePayloadBytes - kTransferChunkEnvelopeBytes;
constexpr std::uint16_t kDirectiveBodySchemaVersion = 1;
constexpr std::string_view kRebuildRequestMagic = "LVRR";

// Protocol-v1 heartbeat role tags are exhaustive. Unknown tags fail closed so
// adding a role requires an explicit codec update on both peers.
enum class HeartbeatRoleKind : std::uint8_t {
  kNone = 0,
  kAuthorityLeaseRequest = 1,
  kReplicaCandidate = 2,
};

enum class FailoverObservationKind : std::uint8_t {
  kSourcePaused = 1,
  kCandidatePrepared = 2,
  kCandidateRecoveryComplete = 4,
  kActionFailed = 3,
};

absl::StatusOr<std::uint64_t> TransferCap(TransferKind kind) {
  switch (kind) {
    case TransferKind::kFullDesiredState:
    case TransferKind::kNodeControlUpdate:
      return kMaxFullDesiredStateBytes;
    case TransferKind::kDirectivePayload:
      return kMaxDirectiveTransferBytes;
    case TransferKind::kDirectiveResult:
      return kMaxDirectiveResultTransferBytes;
  }
  return absl::InvalidArgumentError("unknown transfer kind");
}

absl::Status ProtocolError(std::string_view message) {
  return absl::InvalidArgumentError(message);
}

absl::Status ResourceLimit(std::string_view message) {
  return absl::ResourceExhaustedError(message);
}

class Writer {
 public:
  explicit Writer(bool retain_bytes = true) : retain_bytes_(retain_bytes) {}

  void U8(std::uint8_t value) {
    const char byte = static_cast<char>(value);
    Append(std::string_view(&byte, 1));
  }

  void U16(std::uint16_t value) {
    std::array<char, 2> bytes{};
    bytes[0] = static_cast<char>(value >> 8);
    bytes[1] = static_cast<char>(value);
    Append(std::string_view(bytes.data(), bytes.size()));
  }

  void U32(std::uint32_t value) {
    std::array<char, 4> bytes{};
    std::size_t offset = 0;
    for (int shift = 24; shift >= 0; shift -= 8) {
      bytes[offset++] = static_cast<char>(value >> shift);
    }
    Append(std::string_view(bytes.data(), bytes.size()));
  }

  void U64(std::uint64_t value) {
    std::array<char, 8> bytes{};
    std::size_t offset = 0;
    for (int shift = 56; shift >= 0; shift -= 8) {
      bytes[offset++] = static_cast<char>(value >> shift);
    }
    Append(std::string_view(bytes.data(), bytes.size()));
  }

  void Bool(bool value) { U8(value ? 1 : 0); }

  template <std::size_t N>
  void Fixed(const std::array<std::uint8_t, N>& value) {
    Append(std::string_view(reinterpret_cast<const char*>(value.data()),
                            value.size()));
  }

  void Raw(std::string_view value) { Append(value); }

  absl::Status String(std::string_view value, std::size_t cap,
                      std::string_view field) {
    if (value.size() > cap ||
        value.size() > std::numeric_limits<std::uint32_t>::max()) {
      return ResourceLimit(std::string(field) + " exceeds its protocol cap");
    }
    U32(static_cast<std::uint32_t>(value.size()));
    Raw(value);
    return absl::OkStatus();
  }

  std::string Take() { return std::move(bytes_); }
  std::size_t size() const noexcept { return size_; }

 private:
  void Append(std::string_view value) {
    size_ += value.size();
    if (retain_bytes_) bytes_.append(value);
  }

  const bool retain_bytes_;
  std::string bytes_;
  std::size_t size_ = 0;
};

class Reader {
 public:
  explicit Reader(std::string_view bytes) : bytes_(bytes) {}

  absl::StatusOr<std::string_view> Raw(std::size_t length) {
    if (length > remaining()) return ProtocolError("truncated message");
    const std::string_view value = bytes_.substr(offset_, length);
    offset_ += length;
    return value;
  }

  absl::StatusOr<std::uint8_t> U8() {
    auto raw = Raw(1);
    LAVIK_RETURN_IF_ERROR(raw);
    return static_cast<std::uint8_t>((*raw)[0]);
  }

  absl::StatusOr<std::uint16_t> U16() {
    auto raw = Raw(2);
    LAVIK_RETURN_IF_ERROR(raw);
    const auto* p = reinterpret_cast<const unsigned char*>(raw->data());
    return static_cast<std::uint16_t>((static_cast<std::uint16_t>(p[0]) << 8) |
                                      p[1]);
  }

  absl::StatusOr<std::uint32_t> U32() {
    auto raw = Raw(4);
    LAVIK_RETURN_IF_ERROR(raw);
    const auto* p = reinterpret_cast<const unsigned char*>(raw->data());
    std::uint32_t value = 0;
    for (unsigned i = 0; i < 4; ++i) value = (value << 8) | p[i];
    return value;
  }

  absl::StatusOr<std::uint64_t> U64() {
    auto raw = Raw(8);
    LAVIK_RETURN_IF_ERROR(raw);
    const auto* p = reinterpret_cast<const unsigned char*>(raw->data());
    std::uint64_t value = 0;
    for (unsigned i = 0; i < 8; ++i) value = (value << 8) | p[i];
    return value;
  }

  absl::StatusOr<bool> Bool() {
    auto tag = U8();
    LAVIK_RETURN_IF_ERROR(tag);
    if (*tag > 1) return ProtocolError("boolean tag must be 0 or 1");
    return *tag == 1;
  }

  template <std::size_t N>
  absl::StatusOr<std::array<std::uint8_t, N>> Fixed() {
    auto raw = Raw(N);
    LAVIK_RETURN_IF_ERROR(raw);
    std::array<std::uint8_t, N> value{};
    std::memcpy(value.data(), raw->data(), N);
    return value;
  }

  absl::StatusOr<std::string> String(std::size_t cap) {
    auto length = U32();
    LAVIK_RETURN_IF_ERROR(length);
    // Check the declared length before checking whether the body is present.
    // Over-limit input is never partially accepted as a truncation case.
    if (*length > cap) return ResourceLimit("message field exceeds its cap");
    auto raw = Raw(*length);
    LAVIK_RETURN_IF_ERROR(raw);
    return std::string(*raw);
  }

  absl::Status Finish() const {
    if (offset_ != bytes_.size())
      return ProtocolError("trailing message bytes");
    return absl::OkStatus();
  }

  std::size_t remaining() const noexcept { return bytes_.size() - offset_; }

 private:
  std::string_view bytes_;
  std::size_t offset_ = 0;
};

std::uint16_t ReadBe16(std::string_view bytes, std::size_t offset) {
  const auto* p = reinterpret_cast<const unsigned char*>(bytes.data() + offset);
  return static_cast<std::uint16_t>((static_cast<std::uint16_t>(p[0]) << 8) |
                                    p[1]);
}

std::uint32_t ReadBe32(std::string_view bytes, std::size_t offset) {
  const auto* p = reinterpret_cast<const unsigned char*>(bytes.data() + offset);
  std::uint32_t value = 0;
  for (unsigned i = 0; i < 4; ++i) value = (value << 8) | p[i];
  return value;
}

std::uint64_t ReadBe64(std::string_view bytes, std::size_t offset) {
  const auto* p = reinterpret_cast<const unsigned char*>(bytes.data() + offset);
  std::uint64_t value = 0;
  for (unsigned i = 0; i < 8; ++i) value = (value << 8) | p[i];
  return value;
}

void StoreBe32(std::string* bytes, std::size_t offset, std::uint32_t value) {
  for (unsigned i = 0; i < 4; ++i) {
    (*bytes)[offset + i] = static_cast<char>(value >> (24 - 8 * i));
  }
}

std::uint32_t Crc32c(std::string_view bytes) noexcept {
  // Reflected Castagnoli polynomial.  A local table keeps this shared codec
  // independent of either process's larger link graph.
  static const std::array<std::uint32_t, 256> table = [] {
    std::array<std::uint32_t, 256> values{};
    for (std::uint32_t i = 0; i < values.size(); ++i) {
      std::uint32_t crc = i;
      for (unsigned bit = 0; bit < 8; ++bit) {
        crc = (crc >> 1) ^ ((crc & 1) ? 0x82f63b78u : 0u);
      }
      values[i] = crc;
    }
    return values;
  }();
  std::uint32_t crc = std::numeric_limits<std::uint32_t>::max();
  for (const unsigned char byte : bytes) {
    crc = table[(crc ^ byte) & 0xff] ^ (crc >> 8);
  }
  return ~crc;
}

bool IsKnownMessageType(std::uint16_t raw) noexcept {
  return raw >= static_cast<std::uint16_t>(MessageType::kClientHello) &&
         raw <= static_cast<std::uint16_t>(MessageType::kBootstrapReply);
}

bool IsZeroHash(const WireHash256& hash) noexcept {
  return std::all_of(hash.begin(), hash.end(),
                     [](std::uint8_t byte) { return byte == 0; });
}

bool IsZeroId(const WireId128& id) noexcept {
  return std::all_of(id.begin(), id.end(),
                     [](std::uint8_t byte) { return byte == 0; });
}

absl::Status WriteSchemaHeader(Writer& writer, std::string_view magic) {
  if (magic.size() != 4) return ProtocolError("invalid schema magic");
  writer.Raw(magic);
  writer.U16(kDirectiveBodySchemaVersion);
  return absl::OkStatus();
}

absl::Status ReadSchemaHeader(Reader& reader, std::string_view magic) {
  auto encoded_magic = reader.Raw(4);
  LAVIK_RETURN_IF_ERROR(encoded_magic);
  if (*encoded_magic != magic)
    return ProtocolError("unknown directive body schema");
  auto version = reader.U16();
  LAVIK_RETURN_IF_ERROR(version);
  if (*version != kDirectiveBodySchemaVersion) {
    return ProtocolError("unknown directive body schema version");
  }
  return absl::OkStatus();
}

absl::Status FillRandom(void* output, std::size_t bytes) {
  auto* cursor = static_cast<unsigned char*>(output);
  while (bytes != 0) {
    const ssize_t result = ::getrandom(cursor, bytes, 0);
    if (result > 0) {
      cursor += result;
      bytes -= static_cast<std::size_t>(result);
      continue;
    }
    if (result < 0 && errno == EINTR) continue;
    const int error = result == 0 ? EIO : errno;
    return absl::InternalError(std::string("OS CSPRNG failed: ") +
                               std::strerror(error));
  }
  return absl::OkStatus();
}

absl::Status ValidateIdentity(std::string_view value, std::string_view field) {
  if (!IsCanonicalIdentity160(value)) {
    return ProtocolError(std::string(field) +
                         " must be 40 lowercase hexadecimal characters");
  }
  return absl::OkStatus();
}

absl::Status WriteIdentity(Writer& writer, std::string_view value,
                           std::string_view field) {
  LAVIK_RETURN_IF_ERROR(ValidateIdentity(value, field));
  writer.Raw(value);
  return absl::OkStatus();
}

absl::StatusOr<std::string> ReadIdentity(Reader& reader,
                                         std::string_view field) {
  auto raw = reader.Raw(40);
  LAVIK_RETURN_IF_ERROR(raw);
  LAVIK_RETURN_IF_ERROR(ValidateIdentity(*raw, field));
  return std::string(*raw);
}

void WriteProjectionBasis(Writer& writer, const WireProjectionBasis& basis) {
  writer.U64(basis.control_revision);
}

absl::StatusOr<WireProjectionBasis> ReadProjectionBasis(Reader& reader) {
  WireProjectionBasis basis;
  LAVIK_ASSIGN_OR_RETURN(basis.control_revision, reader.U64());
  return basis;
}

absl::Status WriteAuthorityAnchor(Writer& writer,
                                  const WireAuthorityAnchor& anchor) {
  LAVIK_RETURN_IF_ERROR(
      writer.String(anchor.group_id, kMaxIdentifierBytes, "group id"));
  writer.Fixed(anchor.assignment_id);
  writer.U64(anchor.group_term);
  return absl::OkStatus();
}

absl::StatusOr<WireAuthorityAnchor> ReadAuthorityAnchor(Reader& reader) {
  WireAuthorityAnchor anchor;
  auto group_id = reader.String(kMaxIdentifierBytes);
  LAVIK_RETURN_IF_ERROR(group_id);
  anchor.group_id = std::move(*group_id);
  auto assignment_id = reader.Fixed<16>();
  LAVIK_RETURN_IF_ERROR(assignment_id);
  anchor.assignment_id = *assignment_id;
  LAVIK_ASSIGN_OR_RETURN(anchor.group_term, reader.U64());
  return anchor;
}

void WriteDirectiveIdentity(Writer& writer,
                            const WireDirectiveIdentity& identity) {
  writer.Fixed(identity.operation_id);
  writer.Fixed(identity.directive_id);
  writer.Fixed(identity.attempt_id);
  writer.U64(identity.directive_revision);
}

absl::StatusOr<WireDirectiveIdentity> ReadDirectiveIdentity(Reader& reader) {
  WireDirectiveIdentity identity;
  LAVIK_ASSIGN_OR_RETURN(identity.operation_id, reader.Fixed<16>());
  LAVIK_ASSIGN_OR_RETURN(identity.directive_id, reader.Fixed<16>());
  LAVIK_ASSIGN_OR_RETURN(identity.attempt_id, reader.Fixed<16>());
  LAVIK_ASSIGN_OR_RETURN(identity.directive_revision, reader.U64());
  return identity;
}

absl::Status Finish(Reader& reader) { return reader.Finish(); }

}  // namespace

bool IsCanonicalIdentity160(std::string_view identity) noexcept {
  if (identity.size() != 40) return false;
  return std::all_of(identity.begin(), identity.end(), [](char digit) {
    return (digit >= '0' && digit <= '9') || (digit >= 'a' && digit <= 'f');
  });
}

absl::StatusOr<std::string> GenerateIdentity160() {
  std::array<std::uint8_t, 20> bytes{};
  LAVIK_RETURN_IF_ERROR(FillRandom(bytes.data(), bytes.size()));
  constexpr std::string_view kHex = "0123456789abcdef";
  std::string encoded(40, '\0');
  for (std::size_t i = 0; i < bytes.size(); ++i) {
    encoded[2 * i] = kHex[bytes[i] >> 4];
    encoded[2 * i + 1] = kHex[bytes[i] & 0x0f];
  }
  return encoded;
}

absl::StatusOr<WireId128> GenerateId128() {
  WireId128 id{};
  do {
    LAVIK_RETURN_IF_ERROR(FillRandom(id.data(), id.size()));
  } while (std::all_of(id.begin(), id.end(),
                       [](std::uint8_t byte) { return byte == 0; }));
  return id;
}

absl::StatusOr<std::string> EncodeRebuildRequest(
    const RebuildRequest& request) {
  if (request.source_flow_count == 0 ||
      request.source_flow_count > kMaxCandidateFlows) {
    return ProtocolError("invalid rebuild source flow count");
  }
  Writer writer;
  LAVIK_RETURN_IF_ERROR(WriteSchemaHeader(writer, kRebuildRequestMagic));
  writer.U32(request.source_flow_count);
  return std::move(writer).Take();
}

absl::StatusOr<RebuildRequest> DecodeRebuildRequest(std::string_view encoded) {
  Reader reader(encoded);
  LAVIK_RETURN_IF_ERROR(ReadSchemaHeader(reader, kRebuildRequestMagic));
  auto count = reader.U32();
  LAVIK_RETURN_IF_ERROR(count);
  if (*count == 0 || *count > kMaxCandidateFlows) {
    return ProtocolError("invalid rebuild source flow count");
  }
  LAVIK_RETURN_IF_ERROR(Finish(reader));
  return RebuildRequest{.source_flow_count = *count};
}

absl::StatusOr<FrameHeader> ParseFrameHeader(std::string_view encoded_header) {
  if (encoded_header.size() != kFrameHeaderBytes) {
    return ProtocolError("frame header must be exactly 28 bytes");
  }
  if (ReadBe32(encoded_header, 0) != kFrameMagic) {
    return ProtocolError("invalid frame magic");
  }
  if (ReadBe16(encoded_header, 4) != kProtocolVersion) {
    return ProtocolError("unsupported frame protocol version");
  }
  const std::uint16_t raw_type = ReadBe16(encoded_header, 6);
  if (!IsKnownMessageType(raw_type)) {
    return ProtocolError("unknown frame message type");
  }
  const std::uint16_t flags = ReadBe16(encoded_header, 8);
  if (flags != 0) return ProtocolError("frame flags are reserved");
  if (ReadBe16(encoded_header, 10) != 0) {
    return ProtocolError("non-zero reserved frame bits");
  }
  const std::uint32_t payload_length = ReadBe32(encoded_header, 12);
  if (payload_length > kMaxFramePayloadBytes) {
    return ResourceLimit("declared frame payload exceeds 16 KiB");
  }
  const std::uint64_t sequence = ReadBe64(encoded_header, 16);
  if (sequence == 0) return ProtocolError("frame sequence must start at one");
  return FrameHeader{.type = static_cast<MessageType>(raw_type),
                     .flags = flags,
                     .payload_length = payload_length,
                     .sequence = sequence,
                     .crc32c = ReadBe32(encoded_header, kFrameCrcOffset)};
}

absl::StatusOr<std::string> FrameEncoder::Encode(MessageType type,
                                                 std::string_view payload,
                                                 std::uint16_t flags) {
  if (!IsKnownMessageType(static_cast<std::uint16_t>(type))) {
    return ProtocolError("unknown frame message type");
  }
  if (flags != 0) return ProtocolError("frame flags are reserved");
  if (payload.size() > kMaxFramePayloadBytes) {
    return ResourceLimit("encoded frame exceeds 16 KiB");
  }
  if (next_sequence_ == 0) {
    return absl::FailedPreconditionError("frame sequence exhausted");
  }
  Writer writer;
  writer.U32(kFrameMagic);
  writer.U16(kProtocolVersion);
  writer.U16(static_cast<std::uint16_t>(type));
  writer.U16(flags);
  writer.U16(0);  // reserved
  writer.U32(static_cast<std::uint32_t>(payload.size()));
  writer.U64(next_sequence_);
  writer.U32(0);  // checksum is zero while CRC covers header + payload
  writer.Raw(payload);
  std::string encoded = writer.Take();
  StoreBe32(&encoded, kFrameCrcOffset, Crc32c(encoded));
  ++next_sequence_;
  return encoded;
}

absl::StatusOr<Frame> FrameDecoder::Decode(std::string_view encoded) {
  if (encoded.size() < kFrameHeaderBytes) {
    return ProtocolError("truncated frame header");
  }
  if (encoded.size() > kMaxFrameBytes) {
    return ResourceLimit("frame exceeds 16 KiB");
  }
  if (ReadBe32(encoded, 0) != kFrameMagic) {
    return ProtocolError("invalid frame magic");
  }
  if (ReadBe16(encoded, 4) != kProtocolVersion) {
    return ProtocolError("unsupported frame protocol version");
  }
  const std::uint16_t raw_type = ReadBe16(encoded, 6);
  if (!IsKnownMessageType(raw_type)) {
    return ProtocolError("unknown frame message type");
  }
  const std::uint16_t flags = ReadBe16(encoded, 8);
  if (flags != 0) return ProtocolError("frame flags are reserved");
  if (ReadBe16(encoded, 10) != 0) {
    return ProtocolError("non-zero reserved frame bits");
  }
  const std::uint32_t payload_length = ReadBe32(encoded, 12);
  if (payload_length > kMaxFramePayloadBytes) {
    return ResourceLimit("declared frame payload exceeds 16 KiB");
  }
  if (encoded.size() != kFrameHeaderBytes + payload_length) {
    return ProtocolError("frame length does not match its header");
  }
  const std::uint32_t expected_crc = ReadBe32(encoded, kFrameCrcOffset);
  std::string crc_input(encoded);
  StoreBe32(&crc_input, kFrameCrcOffset, 0);
  if (Crc32c(crc_input) != expected_crc) {
    return absl::DataLossError("frame CRC32C mismatch");
  }
  const std::uint64_t sequence = ReadBe64(encoded, 16);
  if (sequence != expected_sequence_) {
    return absl::FailedPreconditionError(
        "frame sequence is not strictly consecutive");
  }
  if (expected_sequence_ == std::numeric_limits<std::uint64_t>::max()) {
    expected_sequence_ = 0;
  } else {
    ++expected_sequence_;
  }
  return Frame{.type = static_cast<MessageType>(raw_type),
               .flags = flags,
               .sequence = sequence,
               .payload = std::string(encoded.substr(kFrameHeaderBytes))};
}

struct LargeObjectReassembler::Impl {
  struct Active {
    TransferStart start;
    std::uint64_t received = 0;
  };

  explicit Impl(LargeObjectSink& output) : sink(output) {}
  LargeObjectSink& sink;
  std::optional<Active> active;
};

LargeObjectReassembler::LargeObjectReassembler(LargeObjectSink& sink)
    : impl_(std::make_unique<Impl>(sink)) {}

LargeObjectReassembler::~LargeObjectReassembler() {
  if (impl_->active.has_value()) impl_->sink.Abort();
}

absl::Status LargeObjectReassembler::Accept(const TransferStart& start) {
  if (impl_->active.has_value()) {
    return absl::FailedPreconditionError(
        "only one object may be reassembled per direction");
  }
  const auto raw_kind = static_cast<std::uint16_t>(start.kind);
  if (raw_kind < 1 || raw_kind > 5) {
    return ProtocolError("unknown transfer kind");
  }
  auto cap = TransferCap(start.kind);
  LAVIK_RETURN_IF_ERROR(cap);
  if (start.total_length > *cap) {
    return ResourceLimit("large object exceeds its type-specific cap");
  }
  LAVIK_RETURN_IF_ERROR(impl_->sink.Begin(start));
  impl_->active.emplace(Impl::Active{.start = start, .received = 0});
  return absl::OkStatus();
}

absl::Status LargeObjectReassembler::Accept(const TransferChunk& chunk) {
  if (!impl_->active.has_value()) {
    return absl::FailedPreconditionError("transfer chunk has no active object");
  }
  Impl::Active& active = *impl_->active;
  if (chunk.object_id != active.start.object_id) {
    return absl::FailedPreconditionError("transfer chunk object id mismatch");
  }
  if (chunk.offset != active.received) {
    return absl::FailedPreconditionError("transfer chunk offset is not next");
  }
  if (chunk.bytes.empty()) return ProtocolError("empty transfer chunk");
  if (chunk.bytes.size() > kMaxTransferChunkBytes) {
    return ResourceLimit("transfer chunk cannot fit in one frame");
  }
  if (chunk.bytes.size() > active.start.total_length - active.received) {
    return ProtocolError("transfer chunk exceeds declared object length");
  }
  if (absl::Status status = impl_->sink.Write(chunk.offset, chunk.bytes);
      !status.ok()) {
    impl_->sink.Abort();
    impl_->active.reset();
    return status;
  }
  active.received += chunk.bytes.size();
  return absl::OkStatus();
}

absl::Status LargeObjectReassembler::Accept(const TransferEnd& end) {
  if (!impl_->active.has_value()) {
    return absl::FailedPreconditionError("transfer end has no active object");
  }
  Impl::Active& active = *impl_->active;
  if (end.object_id != active.start.object_id) {
    return absl::FailedPreconditionError("transfer end object id mismatch");
  }
  if (active.received != active.start.total_length) {
    return absl::FailedPreconditionError("transfer ended before all bytes");
  }
  if (absl::Status status = impl_->sink.Commit(); !status.ok()) {
    impl_->sink.Abort();
    impl_->active.reset();
    return status;
  }
  impl_->active.reset();
  return absl::OkStatus();
}

absl::Status LargeObjectReassembler::Accept(const TransferAbort& abort) {
  if (!impl_->active.has_value()) {
    return absl::FailedPreconditionError("transfer abort has no active object");
  }
  if (abort.object_id != impl_->active->start.object_id) {
    return absl::FailedPreconditionError("transfer abort object id mismatch");
  }
  impl_->sink.Abort();
  impl_->active.reset();
  return absl::OkStatus();
}

bool LargeObjectReassembler::active() const noexcept {
  return impl_->active.has_value();
}

absl::Status HeartbeatSequenceWindow::Observe(std::uint64_t sequence) {
  if (last_sequence_ == std::numeric_limits<std::uint64_t>::max() ||
      sequence != last_sequence_ + 1) {
    return absl::FailedPreconditionError(
        "heartbeat sequence duplicate, rollback or gap");
  }
  last_sequence_ = sequence;
  return absl::OkStatus();
}

void HeartbeatSequenceWindow::Reset() noexcept { last_sequence_ = 0; }

absl::Status LeaseChallengeTracker::Begin(WireId128 session_id,
                                          std::string data_boot_id,
                                          LeaseChallenge challenge) {
  LAVIK_RETURN_IF_ERROR(ValidateIdentity(data_boot_id, "data boot id"));
  if (challenge.group_id.empty() ||
      challenge.group_id.size() > kMaxIdentifierBytes) {
    return ProtocolError("lease challenge group id is invalid");
  }
  if ((pending_.has_value() && pending_->challenge.nonce == challenge.nonce) ||
      (last_nonce_.has_value() && *last_nonce_ == challenge.nonce)) {
    return absl::FailedPreconditionError(
        "a lease retry must use a new challenge nonce");
  }
  if (pending_.has_value()) last_nonce_ = pending_->challenge.nonce;
  pending_ = Pending{.session_id = session_id,
                     .data_boot_id = std::move(data_boot_id),
                     .challenge = std::move(challenge),
                     .lease_sent_at_ms = std::nullopt};
  return absl::OkStatus();
}

absl::Status LeaseChallengeTracker::MarkWritten(const WireId128& nonce,
                                                std::int64_t lease_now_ms) {
  if (!pending_.has_value() || pending_->challenge.nonce != nonce) {
    return absl::FailedPreconditionError("lease challenge is not pending");
  }
  if (pending_->lease_sent_at_ms.has_value()) {
    return absl::FailedPreconditionError(
        "lease challenge send time is already fixed");
  }
  pending_->lease_sent_at_ms = lease_now_ms;
  return absl::OkStatus();
}

absl::StatusOr<std::int64_t> LeaseChallengeTracker::AcceptGrant(
    const WireId128& session_id, const LeaseGranted& grant,
    std::int64_t lease_now_ms) {
  if (!pending_.has_value() || !pending_->lease_sent_at_ms.has_value()) {
    return absl::FailedPreconditionError(
        "lease grant has no written pending challenge");
  }
  const Pending& pending = *pending_;
  const LeaseChallenge& challenge = pending.challenge;
  if (session_id != pending.session_id || grant.nonce != challenge.nonce ||
      grant.data_boot_id != pending.data_boot_id ||
      grant.control_revision != challenge.control_revision ||
      grant.group_id != challenge.group_id ||
      grant.assignment_id != challenge.assignment_id ||
      grant.group_term != challenge.group_term) {
    return absl::FailedPreconditionError(
        "lease grant does not exactly match the pending challenge");
  }
  if (grant.granted_duration_ms == 0 ||
      *pending.lease_sent_at_ms >
          std::numeric_limits<std::int64_t>::max() -
              static_cast<std::int64_t>(grant.granted_duration_ms)) {
    last_nonce_ = pending.challenge.nonce;
    pending_.reset();
    return ProtocolError("lease grant duration is invalid");
  }
  const std::int64_t deadline =
      *pending.lease_sent_at_ms + grant.granted_duration_ms;
  last_nonce_ = pending.challenge.nonce;
  pending_.reset();  // exact grants are consumed even when already expired
  if (lease_now_ms >= deadline) {
    return absl::DeadlineExceededError(
        "lease grant arrived after its deadline");
  }
  return deadline;
}

void LeaseChallengeTracker::Cancel() noexcept {
  if (pending_.has_value()) last_nonce_ = pending_->challenge.nonce;
  pending_.reset();
}

namespace {

absl::Status WriteEndpoint(Writer& writer, const WireMetaEndpoint& endpoint) {
  writer.U32(endpoint.server_id);
  LAVIK_RETURN_IF_ERROR(
      writer.String(endpoint.host, kMaxIdentifierBytes, "endpoint host"));
  writer.U16(endpoint.port);
  writer.Bool(endpoint.principal.has_value());
  if (endpoint.principal.has_value()) {
    return writer.String(*endpoint.principal, kMaxIdentifierBytes,
                         "endpoint principal");
  }
  return absl::OkStatus();
}

absl::StatusOr<WireMetaEndpoint> ReadEndpoint(Reader& reader) {
  WireMetaEndpoint endpoint;
  LAVIK_ASSIGN_OR_RETURN(endpoint.server_id, reader.U32());
  LAVIK_ASSIGN_OR_RETURN(endpoint.host, reader.String(kMaxIdentifierBytes));
  LAVIK_ASSIGN_OR_RETURN(endpoint.port, reader.U16());
  auto has_principal = reader.Bool();
  LAVIK_RETURN_IF_ERROR(has_principal);
  if (*has_principal) {
    LAVIK_ASSIGN_OR_RETURN(endpoint.principal,
                           reader.String(kMaxIdentifierBytes));
  }
  return endpoint;
}

absl::Status WriteLeaseChallenge(Writer& writer,
                                 const LeaseChallenge& challenge) {
  writer.Fixed(challenge.nonce);
  writer.U64(challenge.control_revision);
  LAVIK_RETURN_IF_ERROR(writer.String(challenge.group_id, kMaxIdentifierBytes,
                                      "challenge group id"));
  writer.Fixed(challenge.assignment_id);
  writer.U64(challenge.group_term);
  return absl::OkStatus();
}

absl::StatusOr<LeaseChallenge> ReadLeaseChallenge(Reader& reader) {
  LeaseChallenge challenge;
  auto nonce = reader.Fixed<16>();
  LAVIK_RETURN_IF_ERROR(nonce);
  challenge.nonce = *nonce;
  auto control_revision = reader.U64();
  LAVIK_RETURN_IF_ERROR(control_revision);
  challenge.control_revision = *control_revision;
  auto group_id = reader.String(kMaxIdentifierBytes);
  LAVIK_RETURN_IF_ERROR(group_id);
  challenge.group_id = std::move(*group_id);
  auto assignment_id = reader.Fixed<16>();
  LAVIK_RETURN_IF_ERROR(assignment_id);
  challenge.assignment_id = *assignment_id;
  LAVIK_ASSIGN_OR_RETURN(challenge.group_term, reader.U64());
  return challenge;
}

absl::Status WriteHeartbeatFlowVector(Writer& writer,
                                      std::span<const std::uint64_t> next_lsns,
                                      std::string_view field) {
  if (next_lsns.empty() || next_lsns.size() > kMaxCandidateFlows) {
    return ResourceLimit(std::string(field) +
                         " count is outside its protocol cap");
  }
  writer.U16(static_cast<std::uint16_t>(next_lsns.size()));
  for (const std::uint64_t next_lsn : next_lsns) {
    if (next_lsn == 0) {
      return ProtocolError(std::string(field) + " contains a zero next LSN");
    }
    writer.U64(next_lsn);
  }
  return absl::OkStatus();
}

absl::StatusOr<std::vector<std::uint64_t>> ReadHeartbeatFlowVector(
    Reader& reader, std::string_view field) {
  auto count = reader.U16();
  LAVIK_RETURN_IF_ERROR(count);
  if (*count == 0 || *count > kMaxCandidateFlows) {
    return ResourceLimit(std::string(field) +
                         " count is outside its protocol cap");
  }
  std::vector<std::uint64_t> next_lsns;
  next_lsns.reserve(*count);
  for (std::uint16_t flow = 0; flow < *count; ++flow) {
    auto next_lsn = reader.U64();
    LAVIK_RETURN_IF_ERROR(next_lsn);
    if (*next_lsn == 0) {
      return ProtocolError(std::string(field) + " contains a zero next LSN");
    }
    next_lsns.push_back(*next_lsn);
  }
  return next_lsns;
}

absl::Status WriteFailoverObservation(Writer& writer,
                                      const FailoverObservation& observation) {
  if (const auto* paused = std::get_if<SourcePaused>(&observation)) {
    if (IsZeroId(paused->transition_id) ||
        IsZeroId(paused->source_assignment_id) ||
        paused->source_group_term == 0) {
      return ProtocolError("source-paused observation has an empty anchor");
    }
    writer.U8(
        static_cast<std::uint8_t>(FailoverObservationKind::kSourcePaused));
    writer.Fixed(paused->transition_id);
    LAVIK_RETURN_IF_ERROR(
        WriteIdentity(writer, paused->source_node_id, "paused source node id"));
    writer.Fixed(paused->source_assignment_id);
    LAVIK_RETURN_IF_ERROR(
        WriteIdentity(writer, paused->source_boot_id, "paused source boot id"));
    LAVIK_RETURN_IF_ERROR(WriteIdentity(writer, paused->source_history_id,
                                        "paused source history id"));
    writer.U64(paused->source_group_term);
    return WriteHeartbeatFlowVector(writer, paused->stable_next_lsns,
                                    "paused stable frontier");
  }

  if (const auto* prepared = std::get_if<CandidatePrepared>(&observation)) {
    if (IsZeroId(prepared->transition_id) || IsZeroId(prepared->action_id) ||
        IsZeroId(prepared->candidate_assignment_id) ||
        IsZeroId(prepared->prepared_context_id)) {
      return ProtocolError(
          "candidate-prepared observation has an empty anchor");
    }
    writer.U8(
        static_cast<std::uint8_t>(FailoverObservationKind::kCandidatePrepared));
    writer.Fixed(prepared->transition_id);
    writer.Fixed(prepared->action_id);
    LAVIK_RETURN_IF_ERROR(WriteIdentity(writer, prepared->candidate_node_id,
                                        "prepared candidate node id"));
    writer.Fixed(prepared->candidate_assignment_id);
    LAVIK_RETURN_IF_ERROR(WriteIdentity(writer, prepared->candidate_boot_id,
                                        "prepared candidate boot id"));
    writer.Fixed(prepared->prepared_context_id);
    return absl::OkStatus();
  }

  if (const auto* complete =
          std::get_if<CandidateRecoveryComplete>(&observation)) {
    if (IsZeroId(complete->transition_id) || IsZeroId(complete->action_id) ||
        IsZeroId(complete->candidate_assignment_id) ||
        (complete->recovery_deadline_unix_ms == 0 ||
         complete->recovery_deadline_unix_ms >
             static_cast<std::uint64_t>(
                 std::numeric_limits<std::int64_t>::max()) ||
         !IsRecoveryCompletionReason(complete->completion_reason))) {
      return ProtocolError(
          "candidate-complete observation has an empty anchor");
    }
    writer.U8(static_cast<std::uint8_t>(
        FailoverObservationKind::kCandidateRecoveryComplete));
    writer.Fixed(complete->transition_id);
    writer.Fixed(complete->action_id);
    LAVIK_RETURN_IF_ERROR(WriteIdentity(writer, complete->candidate_node_id,
                                        "complete candidate node id"));
    writer.Fixed(complete->candidate_assignment_id);
    LAVIK_RETURN_IF_ERROR(WriteIdentity(writer, complete->candidate_boot_id,
                                        "complete candidate boot id"));
    writer.U64(complete->recovery_deadline_unix_ms);
    LAVIK_RETURN_IF_ERROR(writer.String(complete->completion_reason,
                                        kMaxIdentifierBytes,
                                        "recovery completion reason"));
    return WriteHeartbeatFlowVector(writer, complete->applied_next_lsns,
                                    "recovery applied frontier");
  }

  const ActionFailed& failed = std::get<ActionFailed>(observation);
  if (IsZeroId(failed.transition_id) || IsZeroId(failed.action_id) ||
      IsZeroId(failed.candidate_assignment_id) ||
      failed.population_manifest_revision == 0 ||
      IsZeroHash(failed.population_manifest_digest) ||
      failed.partition_replication_epoch == 0 || failed.failure_class.empty() ||
      failed.failure_detail.empty()) {
    return ProtocolError("action-failed observation has an empty anchor");
  }
  writer.U8(static_cast<std::uint8_t>(FailoverObservationKind::kActionFailed));
  writer.Fixed(failed.transition_id);
  writer.Fixed(failed.action_id);
  LAVIK_RETURN_IF_ERROR(WriteIdentity(writer, failed.candidate_node_id,
                                      "failed candidate node id"));
  writer.Fixed(failed.candidate_assignment_id);
  LAVIK_RETURN_IF_ERROR(WriteIdentity(writer, failed.candidate_boot_id,
                                      "failed candidate boot id"));
  writer.U64(failed.population_manifest_revision);
  writer.Fixed(failed.population_manifest_digest);
  writer.U64(failed.partition_replication_epoch);
  LAVIK_RETURN_IF_ERROR(writer.String(failed.failure_class,
                                      kMaxFailoverFailureClassBytes,
                                      "failover failure class"));
  return writer.String(failed.failure_detail, kMaxFailoverFailureDetailBytes,
                       "failover failure detail");
}

absl::StatusOr<FailoverObservation> ReadFailoverObservation(Reader& reader) {
  auto kind = reader.U8();
  LAVIK_RETURN_IF_ERROR(kind);
  if (*kind ==
      static_cast<std::uint8_t>(FailoverObservationKind::kSourcePaused)) {
    SourcePaused paused;
    auto transition_id = reader.Fixed<16>();
    LAVIK_RETURN_IF_ERROR(transition_id);
    paused.transition_id = *transition_id;
    auto source_node_id = ReadIdentity(reader, "paused source node id");
    LAVIK_RETURN_IF_ERROR(source_node_id);
    paused.source_node_id = std::move(*source_node_id);
    LAVIK_ASSIGN_OR_RETURN(paused.source_assignment_id, reader.Fixed<16>());
    LAVIK_ASSIGN_OR_RETURN(paused.source_boot_id,
                           ReadIdentity(reader, "paused source boot id"));
    LAVIK_ASSIGN_OR_RETURN(paused.source_history_id,
                           ReadIdentity(reader, "paused source history id"));
    auto source_group_term = reader.U64();
    LAVIK_RETURN_IF_ERROR(source_group_term);
    paused.source_group_term = *source_group_term;
    LAVIK_ASSIGN_OR_RETURN(
        paused.stable_next_lsns,
        ReadHeartbeatFlowVector(reader, "paused stable frontier"));
    if (IsZeroId(paused.transition_id) ||
        IsZeroId(paused.source_assignment_id) ||
        paused.source_group_term == 0) {
      return ProtocolError("source-paused observation has an empty anchor");
    }
    return FailoverObservation{std::move(paused)};
  }

  if (*kind ==
      static_cast<std::uint8_t>(FailoverObservationKind::kCandidatePrepared)) {
    CandidatePrepared prepared;
    auto transition_id = reader.Fixed<16>();
    LAVIK_RETURN_IF_ERROR(transition_id);
    prepared.transition_id = *transition_id;
    auto action_id = reader.Fixed<16>();
    LAVIK_RETURN_IF_ERROR(action_id);
    prepared.action_id = *action_id;
    LAVIK_ASSIGN_OR_RETURN(prepared.candidate_node_id,
                           ReadIdentity(reader, "prepared candidate node id"));
    LAVIK_ASSIGN_OR_RETURN(prepared.candidate_assignment_id,
                           reader.Fixed<16>());
    LAVIK_ASSIGN_OR_RETURN(prepared.candidate_boot_id,
                           ReadIdentity(reader, "prepared candidate boot id"));
    LAVIK_ASSIGN_OR_RETURN(prepared.prepared_context_id, reader.Fixed<16>());
    if (IsZeroId(prepared.transition_id) || IsZeroId(prepared.action_id) ||
        IsZeroId(prepared.candidate_assignment_id) ||
        IsZeroId(prepared.prepared_context_id)) {
      return ProtocolError(
          "candidate-prepared observation has an empty anchor");
    }
    return FailoverObservation{std::move(prepared)};
  }

  if (*kind == static_cast<std::uint8_t>(
                   FailoverObservationKind::kCandidateRecoveryComplete)) {
    CandidateRecoveryComplete complete;
    auto transition_id = reader.Fixed<16>();
    LAVIK_RETURN_IF_ERROR(transition_id);
    complete.transition_id = *transition_id;
    auto action_id = reader.Fixed<16>();
    LAVIK_RETURN_IF_ERROR(action_id);
    complete.action_id = *action_id;
    LAVIK_ASSIGN_OR_RETURN(complete.candidate_node_id,
                           ReadIdentity(reader, "complete candidate node id"));
    LAVIK_ASSIGN_OR_RETURN(complete.candidate_assignment_id,
                           reader.Fixed<16>());
    LAVIK_ASSIGN_OR_RETURN(complete.candidate_boot_id,
                           ReadIdentity(reader, "complete candidate boot id"));
    LAVIK_ASSIGN_OR_RETURN(complete.recovery_deadline_unix_ms, reader.U64());
    LAVIK_ASSIGN_OR_RETURN(complete.completion_reason,
                           reader.String(kMaxIdentifierBytes));
    LAVIK_ASSIGN_OR_RETURN(
        complete.applied_next_lsns,
        ReadHeartbeatFlowVector(reader, "recovery applied frontier"));
    if (IsZeroId(complete.transition_id) || IsZeroId(complete.action_id) ||
        IsZeroId(complete.candidate_assignment_id) ||
        (complete.recovery_deadline_unix_ms == 0 ||
         complete.recovery_deadline_unix_ms >
             static_cast<std::uint64_t>(
                 std::numeric_limits<std::int64_t>::max()) ||
         !IsRecoveryCompletionReason(complete.completion_reason))) {
      return ProtocolError(
          "candidate-complete observation has an empty anchor");
    }
    return FailoverObservation{std::move(complete)};
  }

  if (*kind ==
      static_cast<std::uint8_t>(FailoverObservationKind::kActionFailed)) {
    ActionFailed failed;
    auto transition_id = reader.Fixed<16>();
    LAVIK_RETURN_IF_ERROR(transition_id);
    failed.transition_id = *transition_id;
    auto action_id = reader.Fixed<16>();
    LAVIK_RETURN_IF_ERROR(action_id);
    failed.action_id = *action_id;
    LAVIK_ASSIGN_OR_RETURN(failed.candidate_node_id,
                           ReadIdentity(reader, "failed candidate node id"));
    LAVIK_ASSIGN_OR_RETURN(failed.candidate_assignment_id, reader.Fixed<16>());
    LAVIK_ASSIGN_OR_RETURN(failed.candidate_boot_id,
                           ReadIdentity(reader, "failed candidate boot id"));
    LAVIK_ASSIGN_OR_RETURN(failed.population_manifest_revision, reader.U64());
    LAVIK_ASSIGN_OR_RETURN(failed.population_manifest_digest,
                           reader.Fixed<32>());
    LAVIK_ASSIGN_OR_RETURN(failed.partition_replication_epoch, reader.U64());
    auto failure_class = reader.String(kMaxFailoverFailureClassBytes);
    LAVIK_RETURN_IF_ERROR(failure_class);
    failed.failure_class = std::move(*failure_class);
    auto failure_detail = reader.String(kMaxFailoverFailureDetailBytes);
    LAVIK_RETURN_IF_ERROR(failure_detail);
    failed.failure_detail = std::move(*failure_detail);
    if (IsZeroId(failed.transition_id) || IsZeroId(failed.action_id) ||
        IsZeroId(failed.candidate_assignment_id) ||
        failed.population_manifest_revision == 0 ||
        IsZeroHash(failed.population_manifest_digest) ||
        failed.partition_replication_epoch == 0 ||
        failed.failure_class.empty() || failed.failure_detail.empty()) {
      return ProtocolError("action-failed observation has an empty anchor");
    }
    return FailoverObservation{std::move(failed)};
  }
  return ProtocolError("unknown failover observation kind");
}

absl::Status WriteLeaseGranted(Writer& writer, const LeaseGranted& grant) {
  writer.Fixed(grant.nonce);
  writer.U32(grant.leader_id);
  writer.U64(grant.raft_term);
  LAVIK_RETURN_IF_ERROR(
      WriteIdentity(writer, grant.data_boot_id, "data boot id"));
  writer.U64(grant.control_revision);
  LAVIK_RETURN_IF_ERROR(
      writer.String(grant.group_id, kMaxIdentifierBytes, "grant group id"));
  writer.Fixed(grant.assignment_id);
  writer.U64(grant.group_term);
  writer.U32(grant.granted_duration_ms);
  return absl::OkStatus();
}

absl::StatusOr<LeaseGranted> ReadLeaseGranted(Reader& reader) {
  LeaseGranted grant;
  auto nonce = reader.Fixed<16>();
  LAVIK_RETURN_IF_ERROR(nonce);
  grant.nonce = *nonce;
  auto leader_id = reader.U32();
  LAVIK_RETURN_IF_ERROR(leader_id);
  grant.leader_id = *leader_id;
  auto raft_term = reader.U64();
  LAVIK_RETURN_IF_ERROR(raft_term);
  grant.raft_term = *raft_term;
  LAVIK_ASSIGN_OR_RETURN(grant.data_boot_id,
                         ReadIdentity(reader, "data boot id"));
  auto control_revision = reader.U64();
  LAVIK_RETURN_IF_ERROR(control_revision);
  grant.control_revision = *control_revision;
  auto group_id = reader.String(kMaxIdentifierBytes);
  LAVIK_RETURN_IF_ERROR(group_id);
  grant.group_id = std::move(*group_id);
  auto assignment_id = reader.Fixed<16>();
  LAVIK_RETURN_IF_ERROR(assignment_id);
  grant.assignment_id = *assignment_id;
  LAVIK_ASSIGN_OR_RETURN(grant.group_term, reader.U64());
  LAVIK_ASSIGN_OR_RETURN(grant.granted_duration_ms, reader.U32());
  return grant;
}

std::uint8_t ModeTag(std::optional<ClientMode> mode) {
  if (!mode) return 0;
  return *mode == ClientMode::kSingle    ? 1
         : *mode == ClientMode::kCluster ? 2
                                         : 255;
}

absl::StatusOr<std::optional<ClientMode>> ReadMode(Reader& reader) {
  auto tag = reader.U8();
  LAVIK_RETURN_IF_ERROR(tag);
  if (*tag > 2) return ProtocolError("invalid client mode");
  if (*tag == 0) return std::optional<ClientMode>{};
  return std::optional<ClientMode>{static_cast<ClientMode>(*tag - 1)};
}

void WriteService(Writer& writer, const ServiceDeclaration& service) {
  writer.U8(ModeTag(service.client_mode));
  writer.Fixed(service.creation_id);
  writer.U64(service.genesis_commit_index);
}

absl::StatusOr<ServiceDeclaration> ReadService(Reader& reader) {
  auto mode = ReadMode(reader);
  LAVIK_RETURN_IF_ERROR(mode);
  auto id = reader.Fixed<16>();
  LAVIK_RETURN_IF_ERROR(id);
  auto index = reader.U64();
  LAVIK_RETURN_IF_ERROR(index);
  return ServiceDeclaration{*mode, *id, *index};
}

void WriteCapabilities(Writer& writer, const ClientServiceCapabilities& caps) {
  writer.U32(caps.supported_modes);
  writer.U32(caps.services);
  writer.U8(ModeTag(caps.installed_mode));
  writer.U32(caps.database_count);
}

absl::StatusOr<ClientServiceCapabilities> ReadCapabilities(Reader& reader) {
  auto modes = reader.U32();
  LAVIK_RETURN_IF_ERROR(modes);
  auto services = reader.U32();
  LAVIK_RETURN_IF_ERROR(services);
  auto mode = ReadMode(reader);
  LAVIK_RETURN_IF_ERROR(mode);
  auto databases = reader.U32();
  LAVIK_RETURN_IF_ERROR(databases);
  return ClientServiceCapabilities{*modes, *services, *mode, *databases};
}

absl::StatusOr<std::string> Encode(const BootstrapHello& hello) {
  if (hello.minimum_version == 0 ||
      hello.minimum_version > hello.maximum_version) {
    return ProtocolError("invalid BootstrapHello version range");
  }
  Writer writer;
  writer.U16(hello.minimum_version);
  writer.U16(hello.maximum_version);
  LAVIK_RETURN_IF_ERROR(
      WriteIdentity(writer, hello.node_id, "bootstrap node id"));
  WriteCapabilities(writer, hello.capabilities);
  return writer.Take();
}

absl::StatusOr<WireMessage> DecodeBootstrapHello(std::string_view bytes) {
  Reader reader(bytes);
  BootstrapHello hello;
  auto minimum = reader.U16();
  LAVIK_RETURN_IF_ERROR(minimum);
  auto maximum = reader.U16();
  LAVIK_RETURN_IF_ERROR(maximum);
  if (*minimum == 0 || *minimum > *maximum)
    return ProtocolError("invalid BootstrapHello version range");
  hello.minimum_version = *minimum;
  hello.maximum_version = *maximum;
  LAVIK_ASSIGN_OR_RETURN(hello.node_id,
                         ReadIdentity(reader, "bootstrap node id"));
  LAVIK_ASSIGN_OR_RETURN(hello.capabilities, ReadCapabilities(reader));
  LAVIK_RETURN_IF_ERROR(Finish(reader));
  return WireMessage{std::move(hello)};
}

absl::StatusOr<std::string> Encode(const ClientHello& hello) {
  if (hello.minimum_version == 0 ||
      hello.minimum_version > hello.maximum_version) {
    return ProtocolError("invalid ClientHello version range");
  }
  if (hello.replication_flow_count == 0 ||
      hello.replication_flow_count > kMaxCandidateFlows) {
    return ProtocolError("invalid ClientHello replication flow count");
  }
  Writer writer;
  writer.U16(hello.minimum_version);
  writer.U16(hello.maximum_version);
  LAVIK_RETURN_IF_ERROR(WriteIdentity(writer, hello.node_id, "node id"));
  LAVIK_RETURN_IF_ERROR(WriteIdentity(writer, hello.boot_id, "boot id"));
  LAVIK_RETURN_IF_ERROR(WriteIdentity(writer, hello.replication_history_id,
                                      "replication history id"));
  writer.U32(hello.replication_flow_count);
  WriteService(writer, hello.service);
  WriteCapabilities(writer, hello.capabilities);
  return std::move(writer).Take();
}

absl::StatusOr<WireMessage> DecodeClientHello(std::string_view bytes) {
  Reader reader(bytes);
  ClientHello hello;
  LAVIK_ASSIGN_OR_RETURN(hello.minimum_version, reader.U16());
  LAVIK_ASSIGN_OR_RETURN(hello.maximum_version, reader.U16());
  if (hello.minimum_version == 0 ||
      hello.minimum_version > hello.maximum_version) {
    return ProtocolError("invalid ClientHello version range");
  }
  auto node_id = ReadIdentity(reader, "node id");
  LAVIK_RETURN_IF_ERROR(node_id);
  hello.node_id = std::move(*node_id);
  auto boot_id = ReadIdentity(reader, "boot id");
  LAVIK_RETURN_IF_ERROR(boot_id);
  hello.boot_id = std::move(*boot_id);
  LAVIK_ASSIGN_OR_RETURN(hello.replication_history_id,
                         ReadIdentity(reader, "replication history id"));
  auto count = reader.U32();
  LAVIK_RETURN_IF_ERROR(count);
  if (*count == 0 || *count > kMaxCandidateFlows) {
    return ProtocolError("invalid ClientHello replication flow count");
  }
  hello.replication_flow_count = *count;
  auto service = ReadService(reader);
  LAVIK_RETURN_IF_ERROR(service);
  hello.service = *service;
  LAVIK_ASSIGN_OR_RETURN(hello.capabilities, ReadCapabilities(reader));
  LAVIK_RETURN_IF_ERROR(Finish(reader));
  return WireMessage{std::move(hello)};
}

absl::StatusOr<std::string> Encode(const ServerHello& hello) {
  const auto disposition = static_cast<std::uint8_t>(hello.disposition);
  if (disposition < 1 || disposition > 4) {
    return ProtocolError("unknown ServerHello disposition");
  }
  if (hello.directory.size() > kMaxDirectoryEntries) {
    return ResourceLimit("Meta directory exceeds its entry cap");
  }
  Writer writer;
  writer.U8(disposition);
  writer.U16(hello.negotiated_version);
  writer.U32(hello.meta_server_id);
  writer.U64(hello.raft_term);
  writer.Fixed(hello.session_id);
  writer.U64(hello.session_generation);
  writer.Bool(hello.leader_id.has_value());
  if (hello.leader_id.has_value()) writer.U32(*hello.leader_id);
  writer.U32(static_cast<std::uint32_t>(hello.directory.size()));
  for (const WireMetaEndpoint& endpoint : hello.directory) {
    LAVIK_RETURN_IF_ERROR(WriteEndpoint(writer, endpoint));
  }
  writer.U32(hello.observation_ttl_ms);
  writer.U32(hello.session_progress_timeout_ms);
  WriteService(writer, hello.service);
  LAVIK_RETURN_IF_ERROR(writer.String(hello.rejection_reason,
                                      kMaxIdentifierBytes, "hello rejection"));
  return std::move(writer).Take();
}

absl::StatusOr<WireMessage> DecodeServerHello(std::string_view bytes) {
  Reader reader(bytes);
  ServerHello hello;
  auto disposition = reader.U8();
  LAVIK_RETURN_IF_ERROR(disposition);
  if (*disposition < 1 || *disposition > 4) {
    return ProtocolError("unknown ServerHello disposition");
  }
  hello.disposition = static_cast<ServerHelloDisposition>(*disposition);
  LAVIK_ASSIGN_OR_RETURN(hello.negotiated_version, reader.U16());
  LAVIK_ASSIGN_OR_RETURN(hello.meta_server_id, reader.U32());
  auto raft_term = reader.U64();
  LAVIK_RETURN_IF_ERROR(raft_term);
  hello.raft_term = *raft_term;
  auto session_id = reader.Fixed<16>();
  LAVIK_RETURN_IF_ERROR(session_id);
  hello.session_id = *session_id;
  LAVIK_ASSIGN_OR_RETURN(hello.session_generation, reader.U64());
  auto has_leader = reader.Bool();
  LAVIK_RETURN_IF_ERROR(has_leader);
  if (*has_leader) {
    auto leader_id = reader.U32();
    LAVIK_RETURN_IF_ERROR(leader_id);
    hello.leader_id = *leader_id;
  }
  auto directory_count = reader.U32();
  LAVIK_RETURN_IF_ERROR(directory_count);
  if (*directory_count > kMaxDirectoryEntries) {
    return ResourceLimit("Meta directory exceeds its entry cap");
  }
  hello.directory.reserve(*directory_count);
  for (std::uint32_t i = 0; i < *directory_count; ++i) {
    auto endpoint = ReadEndpoint(reader);
    LAVIK_RETURN_IF_ERROR(endpoint);
    hello.directory.push_back(std::move(*endpoint));
  }
  LAVIK_ASSIGN_OR_RETURN(hello.observation_ttl_ms, reader.U32());
  LAVIK_ASSIGN_OR_RETURN(hello.session_progress_timeout_ms, reader.U32());
  auto service = ReadService(reader);
  LAVIK_RETURN_IF_ERROR(service);
  hello.service = *service;
  LAVIK_ASSIGN_OR_RETURN(hello.rejection_reason,
                         reader.String(kMaxIdentifierBytes));
  LAVIK_RETURN_IF_ERROR(Finish(reader));
  return WireMessage{std::move(hello)};
}

absl::Status ValidateBootstrapReply(const BootstrapReply& reply) {
  const auto disposition = static_cast<std::uint8_t>(reply.disposition);
  if (disposition < 1 || disposition > 4 ||
      reply.server.session_generation != 0 ||
      reply.server.session_id != WireId128{})
    return ProtocolError("invalid BootstrapReply");
  return absl::OkStatus();
}

absl::StatusOr<std::string> Encode(const BootstrapReply& reply) {
  LAVIK_RETURN_IF_ERROR(ValidateBootstrapReply(reply));
  auto server = Encode(reply.server);
  LAVIK_RETURN_IF_ERROR(server);
  Writer writer;
  writer.U8(static_cast<std::uint8_t>(reply.disposition));
  writer.Raw(*server);
  return writer.Take();
}

absl::StatusOr<WireMessage> DecodeBootstrapReply(std::string_view bytes) {
  Reader reader(bytes);
  auto disposition = reader.U8();
  LAVIK_RETURN_IF_ERROR(disposition);
  auto remaining = reader.Raw(reader.remaining());
  LAVIK_RETURN_IF_ERROR(remaining);
  auto server = DecodeServerHello(*remaining);
  LAVIK_RETURN_IF_ERROR(server);
  BootstrapReply reply{static_cast<BootstrapDisposition>(*disposition),
                       std::get<ServerHello>(std::move(*server))};
  LAVIK_RETURN_IF_ERROR(ValidateBootstrapReply(reply));
  return WireMessage{std::move(reply)};
}

absl::StatusOr<std::string> Encode(const TransferStart& start) {
  const auto kind = static_cast<std::uint16_t>(start.kind);
  if (kind < 1 || kind > 5) return ProtocolError("unknown transfer kind");
  auto cap = TransferCap(start.kind);
  LAVIK_RETURN_IF_ERROR(cap);
  if (start.total_length > *cap) {
    return ResourceLimit("large object exceeds its type-specific cap");
  }
  Writer writer;
  writer.U16(kind);
  writer.Fixed(start.object_id);
  writer.U64(start.total_length);
  return std::move(writer).Take();
}

absl::StatusOr<WireMessage> DecodeTransferStart(std::string_view bytes) {
  Reader reader(bytes);
  TransferStart start;
  auto kind = reader.U16();
  LAVIK_RETURN_IF_ERROR(kind);
  if (*kind < 1 || *kind > 5) return ProtocolError("unknown transfer kind");
  start.kind = static_cast<TransferKind>(*kind);
  auto object_id = reader.Fixed<16>();
  LAVIK_RETURN_IF_ERROR(object_id);
  start.object_id = *object_id;
  auto total_length = reader.U64();
  LAVIK_RETURN_IF_ERROR(total_length);
  start.total_length = *total_length;
  auto cap = TransferCap(start.kind);
  LAVIK_RETURN_IF_ERROR(cap);
  if (start.total_length > *cap) {
    return ResourceLimit("large object exceeds its type-specific cap");
  }
  LAVIK_RETURN_IF_ERROR(Finish(reader));
  return WireMessage{start};
}

absl::StatusOr<std::string> Encode(const TransferChunk& chunk) {
  if (chunk.bytes.size() > kMaxTransferChunkBytes) {
    return ResourceLimit("transfer chunk cannot fit in one frame");
  }
  Writer writer;
  writer.Fixed(chunk.object_id);
  writer.U64(chunk.offset);
  LAVIK_RETURN_IF_ERROR(
      writer.String(chunk.bytes, kMaxTransferChunkBytes, "transfer chunk"));
  return std::move(writer).Take();
}

absl::StatusOr<WireMessage> DecodeTransferChunk(std::string_view bytes) {
  Reader reader(bytes);
  TransferChunk chunk;
  auto object_id = reader.Fixed<16>();
  LAVIK_RETURN_IF_ERROR(object_id);
  chunk.object_id = *object_id;
  auto offset = reader.U64();
  LAVIK_RETURN_IF_ERROR(offset);
  chunk.offset = *offset;
  LAVIK_ASSIGN_OR_RETURN(chunk.bytes, reader.String(kMaxTransferChunkBytes));
  LAVIK_RETURN_IF_ERROR(Finish(reader));
  return WireMessage{std::move(chunk)};
}

template <typename T>
absl::StatusOr<std::string> EncodeObjectIdOnly(const T& message) {
  Writer writer;
  writer.Fixed(message.object_id);
  return std::move(writer).Take();
}

absl::StatusOr<std::string> Encode(const TransferEnd& end) {
  return EncodeObjectIdOnly(end);
}

absl::StatusOr<WireMessage> DecodeTransferEnd(std::string_view bytes) {
  Reader reader(bytes);
  auto object_id = reader.Fixed<16>();
  LAVIK_RETURN_IF_ERROR(object_id);
  LAVIK_RETURN_IF_ERROR(Finish(reader));
  return WireMessage{TransferEnd{.object_id = *object_id}};
}

absl::StatusOr<std::string> Encode(const TransferAbort& abort) {
  Writer writer;
  writer.Fixed(abort.object_id);
  writer.U16(static_cast<std::uint16_t>(abort.reason));
  return std::move(writer).Take();
}

absl::StatusOr<WireMessage> DecodeTransferAbort(std::string_view bytes) {
  Reader reader(bytes);
  TransferAbort abort;
  auto object_id = reader.Fixed<16>();
  LAVIK_RETURN_IF_ERROR(object_id);
  abort.object_id = *object_id;
  auto reason = reader.U16();
  LAVIK_RETURN_IF_ERROR(reason);
  abort.reason = static_cast<TransferAbortReason>(*reason);
  LAVIK_RETURN_IF_ERROR(Finish(reader));
  return WireMessage{abort};
}

absl::StatusOr<std::string> Encode(const FullStateApplied& applied) {
  Writer writer;
  writer.Fixed(applied.request_id);
  writer.U64(applied.control_revision);
  return std::move(writer).Take();
}

absl::StatusOr<WireMessage> DecodeFullStateApplied(std::string_view bytes) {
  Reader reader(bytes);
  FullStateApplied applied;
  LAVIK_ASSIGN_OR_RETURN(applied.request_id, reader.Fixed<16>());
  LAVIK_ASSIGN_OR_RETURN(applied.control_revision, reader.U64());
  LAVIK_RETURN_IF_ERROR(Finish(reader));
  return WireMessage{applied};
}

absl::StatusOr<std::string> Encode(const Heartbeat& heartbeat) {
  Writer writer;
  writer.Fixed(heartbeat.session_id);
  writer.U64(heartbeat.heartbeat_sequence);
  writer.Bool(heartbeat.health.storage_ready);
  writer.Bool(heartbeat.health.population_ready);
  writer.Bool(heartbeat.health.draining);
  writer.U32(heartbeat.health.active_groups);
  LAVIK_RETURN_IF_ERROR(writer.String(heartbeat.health.summary,
                                      kMaxOpaqueFieldBytes, "health summary"));
  if (std::holds_alternative<NoRoleInformation>(heartbeat.role_information)) {
    writer.U8(static_cast<std::uint8_t>(HeartbeatRoleKind::kNone));
  } else if (const auto* authority = std::get_if<AuthorityLeaseRequest>(
                 &heartbeat.role_information)) {
    writer.U8(
        static_cast<std::uint8_t>(HeartbeatRoleKind::kAuthorityLeaseRequest));
    LAVIK_RETURN_IF_ERROR(WriteLeaseChallenge(writer, authority->challenge));
  } else {
    writer.U8(static_cast<std::uint8_t>(HeartbeatRoleKind::kReplicaCandidate));
    const CandidateProgress& candidate =
        std::get<ReplicaCandidate>(heartbeat.role_information).progress;
    // Unknown-frontier recovery carries scope only. Reject hidden lineage
    // rather than silently dropping it during encoding.
    if (candidate.operator_recovery &&
        (candidate.recovered || candidate.source_group_term != 0 ||
         !candidate.source_node_id.empty() ||
         candidate.source_assignment_id != WireId128{} ||
         !candidate.source_boot_id.empty() ||
         !candidate.source_history_id.empty() ||
         !candidate.applied_next_lsns.empty())) {
      return ProtocolError("operator recovery cannot claim source progress");
    }
    writer.Bool(candidate.recovered);
    writer.Bool(candidate.operator_recovery);
    if (!candidate.operator_recovery &&
        (candidate.source_group_term == 0 ||
         candidate.source_group_term > candidate.group_term)) {
      return ProtocolError(
          "candidate source term must be nonzero and not exceed group term");
    }
    LAVIK_RETURN_IF_ERROR(writer.String(candidate.group_id, kMaxIdentifierBytes,
                                        "candidate group id"));
    writer.Fixed(candidate.assignment_id);
    writer.U64(candidate.group_term);
    writer.U64(candidate.source_group_term);
    writer.U64(candidate.manifest_revision);
    writer.Fixed(candidate.manifest_digest);
    writer.U64(candidate.partition_replication_epoch);
    if (!candidate.operator_recovery) {
      if (!IsCanonicalIdentity160(candidate.source_node_id) ||
          !IsCanonicalIdentity160(candidate.source_boot_id) ||
          !IsCanonicalIdentity160(candidate.source_history_id)) {
        return ProtocolError("candidate source lineage is not canonical");
      }
      LAVIK_RETURN_IF_ERROR(writer.String(candidate.source_node_id,
                                          kMaxIdentifierBytes,
                                          "candidate source node id"));
      writer.Fixed(candidate.source_assignment_id);
      LAVIK_RETURN_IF_ERROR(writer.String(candidate.source_boot_id,
                                          kMaxIdentifierBytes,
                                          "candidate source boot id"));
      LAVIK_RETURN_IF_ERROR(writer.String(candidate.source_history_id,
                                          kMaxIdentifierBytes,
                                          "candidate source history id"));
      LAVIK_RETURN_IF_ERROR(WriteHeartbeatFlowVector(
          writer, candidate.applied_next_lsns, "candidate flow vector"));
    }
  }
  writer.Bool(heartbeat.failover_observation.has_value());
  if (heartbeat.failover_observation.has_value()) {
    LAVIK_RETURN_IF_ERROR(
        WriteFailoverObservation(writer, *heartbeat.failover_observation));
  }
  if (writer.size() > kMaxFramePayloadBytes) {
    return ResourceLimit("heartbeat exceeds the single-frame payload cap");
  }
  return std::move(writer).Take();
}

absl::StatusOr<WireMessage> DecodeHeartbeat(std::string_view bytes) {
  Reader reader(bytes);
  Heartbeat heartbeat;
  auto session_id = reader.Fixed<16>();
  LAVIK_RETURN_IF_ERROR(session_id);
  heartbeat.session_id = *session_id;
  LAVIK_ASSIGN_OR_RETURN(heartbeat.heartbeat_sequence, reader.U64());
  auto storage_ready = reader.Bool();
  LAVIK_RETURN_IF_ERROR(storage_ready);
  heartbeat.health.storage_ready = *storage_ready;
  auto population_ready = reader.Bool();
  LAVIK_RETURN_IF_ERROR(population_ready);
  heartbeat.health.population_ready = *population_ready;
  auto draining = reader.Bool();
  LAVIK_RETURN_IF_ERROR(draining);
  heartbeat.health.draining = *draining;
  auto active_groups = reader.U32();
  LAVIK_RETURN_IF_ERROR(active_groups);
  heartbeat.health.active_groups = *active_groups;
  auto summary = reader.String(kMaxOpaqueFieldBytes);
  LAVIK_RETURN_IF_ERROR(summary);
  heartbeat.health.summary = std::move(*summary);
  auto role_kind = reader.U8();
  LAVIK_RETURN_IF_ERROR(role_kind);
  if (*role_kind ==
      static_cast<std::uint8_t>(HeartbeatRoleKind::kAuthorityLeaseRequest)) {
    auto challenge = ReadLeaseChallenge(reader);
    LAVIK_RETURN_IF_ERROR(challenge);
    heartbeat.role_information =
        AuthorityLeaseRequest{.challenge = std::move(*challenge)};
  } else if (*role_kind ==
             static_cast<std::uint8_t>(HeartbeatRoleKind::kReplicaCandidate)) {
    CandidateProgress candidate;
    auto recovered = reader.Bool();
    LAVIK_RETURN_IF_ERROR(recovered);
    candidate.recovered = *recovered;
    auto operator_recovery = reader.Bool();
    LAVIK_RETURN_IF_ERROR(operator_recovery);
    candidate.operator_recovery = *operator_recovery;
    if (candidate.operator_recovery && candidate.recovered)
      return ProtocolError("conflicting recovery kinds");
    auto group_id = reader.String(kMaxIdentifierBytes);
    LAVIK_RETURN_IF_ERROR(group_id);
    candidate.group_id = std::move(*group_id);
    auto assignment_id = reader.Fixed<16>();
    LAVIK_RETURN_IF_ERROR(assignment_id);
    candidate.assignment_id = *assignment_id;
    auto group_term = reader.U64();
    LAVIK_RETURN_IF_ERROR(group_term);
    candidate.group_term = *group_term;
    auto source_group_term = reader.U64();
    LAVIK_RETURN_IF_ERROR(source_group_term);
    candidate.source_group_term = *source_group_term;
    if (candidate.operator_recovery && candidate.source_group_term != 0) {
      return ProtocolError("operator recovery cannot claim a source term");
    }
    if (!candidate.operator_recovery &&
        (candidate.source_group_term == 0 ||
         candidate.source_group_term > candidate.group_term)) {
      return ProtocolError(
          "candidate source term must be nonzero and not exceed group term");
    }
    auto manifest_revision = reader.U64();
    LAVIK_RETURN_IF_ERROR(manifest_revision);
    candidate.manifest_revision = *manifest_revision;
    auto manifest_digest = reader.Fixed<32>();
    LAVIK_RETURN_IF_ERROR(manifest_digest);
    candidate.manifest_digest = *manifest_digest;
    auto partition_replication_epoch = reader.U64();
    LAVIK_RETURN_IF_ERROR(partition_replication_epoch);
    candidate.partition_replication_epoch = *partition_replication_epoch;
    if (!candidate.operator_recovery) {
      auto source_node_id = reader.String(kMaxIdentifierBytes);
      LAVIK_RETURN_IF_ERROR(source_node_id);
      auto source_assignment_id = reader.Fixed<16>();
      LAVIK_RETURN_IF_ERROR(source_assignment_id);
      candidate.source_assignment_id = *source_assignment_id;
      auto source_boot_id = reader.String(kMaxIdentifierBytes);
      LAVIK_RETURN_IF_ERROR(source_boot_id);
      auto source_history_id = reader.String(kMaxIdentifierBytes);
      LAVIK_RETURN_IF_ERROR(source_history_id);
      if (!IsCanonicalIdentity160(*source_node_id) ||
          !IsCanonicalIdentity160(*source_boot_id) ||
          !IsCanonicalIdentity160(*source_history_id)) {
        return ProtocolError("candidate source lineage is not canonical");
      }
      candidate.source_node_id = std::move(*source_node_id);
      candidate.source_boot_id = std::move(*source_boot_id);
      candidate.source_history_id = std::move(*source_history_id);
      LAVIK_ASSIGN_OR_RETURN(
          candidate.applied_next_lsns,
          ReadHeartbeatFlowVector(reader, "candidate flow vector"));
    }
    heartbeat.role_information =
        ReplicaCandidate{.progress = std::move(candidate)};
  } else if (*role_kind !=
             static_cast<std::uint8_t>(HeartbeatRoleKind::kNone)) {
    return ProtocolError("unknown heartbeat role-information kind");
  }
  auto has_failover_observation = reader.Bool();
  LAVIK_RETURN_IF_ERROR(has_failover_observation);
  if (*has_failover_observation) {
    LAVIK_ASSIGN_OR_RETURN(heartbeat.failover_observation,
                           ReadFailoverObservation(reader));
  }
  LAVIK_RETURN_IF_ERROR(Finish(reader));
  return WireMessage{std::move(heartbeat)};
}

absl::StatusOr<std::string> Encode(const HeartbeatAck& ack) {
  const auto observation = static_cast<std::uint8_t>(ack.observation_status);
  if (observation > 3) return ProtocolError("unknown observation status");
  Writer writer;
  writer.Fixed(ack.session_id);
  writer.U64(ack.heartbeat_sequence);
  writer.U8(observation);
  LAVIK_RETURN_IF_ERROR(writer.String(
      ack.observation_detail, kMaxOpaqueFieldBytes, "observation detail"));
  writer.U8(static_cast<std::uint8_t>(ack.lease_decision.index()));
  if (const auto* granted = std::get_if<LeaseGranted>(&ack.lease_decision)) {
    LAVIK_RETURN_IF_ERROR(WriteLeaseGranted(writer, *granted));
  } else if (const auto* denied =
                 std::get_if<LeaseDenied>(&ack.lease_decision)) {
    const auto reason = static_cast<std::uint16_t>(denied->reason);
    if (reason < 1 || reason > 6) {
      return ProtocolError("unknown lease denial reason");
    }
    writer.Fixed(denied->nonce);
    writer.U16(reason);
    writer.U64(denied->current_control_revision);
  } else if (const auto* stale =
                 std::get_if<LeaseStateOutOfDate>(&ack.lease_decision)) {
    writer.Fixed(stale->nonce);
    writer.U64(stale->current_control_revision);
  }
  return std::move(writer).Take();
}

absl::StatusOr<WireMessage> DecodeHeartbeatAck(std::string_view bytes) {
  Reader reader(bytes);
  HeartbeatAck ack;
  auto session_id = reader.Fixed<16>();
  LAVIK_RETURN_IF_ERROR(session_id);
  ack.session_id = *session_id;
  LAVIK_ASSIGN_OR_RETURN(ack.heartbeat_sequence, reader.U64());
  auto observation = reader.U8();
  LAVIK_RETURN_IF_ERROR(observation);
  if (*observation > 3) return ProtocolError("unknown observation status");
  ack.observation_status = static_cast<ObservationStatus>(*observation);
  LAVIK_ASSIGN_OR_RETURN(ack.observation_detail,
                         reader.String(kMaxOpaqueFieldBytes));
  auto decision = reader.U8();
  LAVIK_RETURN_IF_ERROR(decision);
  switch (*decision) {
    case 0:
      ack.lease_decision = NoChallenge{};
      break;
    case 1: {
      LAVIK_ASSIGN_OR_RETURN(ack.lease_decision, ReadLeaseGranted(reader));
      break;
    }
    case 2: {
      LeaseDenied denied;
      auto nonce = reader.Fixed<16>();
      LAVIK_RETURN_IF_ERROR(nonce);
      denied.nonce = *nonce;
      auto reason = reader.U16();
      LAVIK_RETURN_IF_ERROR(reason);
      if (*reason < 1 || *reason > 6) {
        return ProtocolError("unknown lease denial reason");
      }
      denied.reason = static_cast<LeaseDenialReason>(*reason);
      LAVIK_ASSIGN_OR_RETURN(denied.current_control_revision, reader.U64());
      ack.lease_decision = denied;
      break;
    }
    case 3: {
      LeaseStateOutOfDate stale;
      auto nonce = reader.Fixed<16>();
      LAVIK_RETURN_IF_ERROR(nonce);
      stale.nonce = *nonce;
      LAVIK_ASSIGN_OR_RETURN(stale.current_control_revision, reader.U64());
      ack.lease_decision = stale;
      break;
    }
    default:
      return ProtocolError("unknown heartbeat lease decision");
  }
  LAVIK_RETURN_IF_ERROR(Finish(reader));
  return WireMessage{std::move(ack)};
}

absl::StatusOr<std::string> Encode(const Fence& fence) {
  Writer writer;
  writer.Fixed(fence.session_id);
  LAVIK_RETURN_IF_ERROR(
      WriteIdentity(writer, fence.target_boot_id, "target boot id"));
  WriteProjectionBasis(writer, fence.basis);
  LAVIK_RETURN_IF_ERROR(WriteAuthorityAnchor(writer, fence.reject_through));
  return std::move(writer).Take();
}

absl::StatusOr<WireMessage> DecodeFence(std::string_view bytes) {
  Reader reader(bytes);
  Fence fence;
  auto session_id = reader.Fixed<16>();
  LAVIK_RETURN_IF_ERROR(session_id);
  fence.session_id = *session_id;
  LAVIK_ASSIGN_OR_RETURN(fence.target_boot_id,
                         ReadIdentity(reader, "target boot id"));
  auto basis = ReadProjectionBasis(reader);
  LAVIK_RETURN_IF_ERROR(basis);
  fence.basis = *basis;
  LAVIK_ASSIGN_OR_RETURN(fence.reject_through, ReadAuthorityAnchor(reader));
  LAVIK_RETURN_IF_ERROR(Finish(reader));
  return WireMessage{std::move(fence)};
}

absl::StatusOr<std::string> Encode(const FenceAck& ack) {
  Writer writer;
  writer.Fixed(ack.session_id);
  LAVIK_RETURN_IF_ERROR(
      WriteIdentity(writer, ack.target_boot_id, "target boot id"));
  LAVIK_RETURN_IF_ERROR(WriteAuthorityAnchor(writer, ack.reject_through));
  return std::move(writer).Take();
}

absl::StatusOr<WireMessage> DecodeFenceAck(std::string_view bytes) {
  Reader reader(bytes);
  FenceAck ack;
  auto session_id = reader.Fixed<16>();
  LAVIK_RETURN_IF_ERROR(session_id);
  ack.session_id = *session_id;
  LAVIK_ASSIGN_OR_RETURN(ack.target_boot_id,
                         ReadIdentity(reader, "target boot id"));
  LAVIK_ASSIGN_OR_RETURN(ack.reject_through, ReadAuthorityAnchor(reader));
  LAVIK_RETURN_IF_ERROR(Finish(reader));
  return WireMessage{std::move(ack)};
}

absl::StatusOr<std::string> Encode(const Directive& directive) {
  Writer writer;
  writer.Fixed(directive.session_id);
  WriteProjectionBasis(writer, directive.basis);
  LAVIK_RETURN_IF_ERROR(WriteAuthorityAnchor(writer, directive.authority));
  WriteDirectiveIdentity(writer, directive.identity);
  LAVIK_RETURN_IF_ERROR(WriteIdentity(writer, directive.recipient_node_id,
                                      "directive recipient node id"));
  LAVIK_RETURN_IF_ERROR(WriteIdentity(writer, directive.recipient_boot_id,
                                      "directive recipient boot id"));
  LAVIK_RETURN_IF_ERROR(WriteIdentity(writer, directive.target_node_id,
                                      "directive target node id"));
  LAVIK_RETURN_IF_ERROR(WriteIdentity(writer, directive.target_boot_id,
                                      "directive target boot id"));
  LAVIK_RETURN_IF_ERROR(WriteIdentity(writer, directive.source_node_id,
                                      "directive source node id"));
  writer.Fixed(directive.source_assignment_id);
  LAVIK_RETURN_IF_ERROR(WriteIdentity(writer, directive.source_boot_id,
                                      "directive source boot id"));
  LAVIK_RETURN_IF_ERROR(
      WriteIdentity(writer, directive.source_replication_history_id,
                    "directive source replication history id"));
  writer.U64(directive.manifest_revision);
  writer.Fixed(directive.manifest_digest);
  writer.U64(directive.partition_replication_epoch);
  const auto kind = static_cast<std::uint8_t>(directive.kind);
  if (kind < 1 || kind > static_cast<std::uint8_t>(
                             WireDirectiveKind::kInitializeEmptyPopulation)) {
    return ProtocolError("unknown directive kind");
  }
  writer.U8(kind);
  LAVIK_RETURN_IF_ERROR(writer.String(directive.payload, kMaxOpaqueFieldBytes,
                                      "directive payload"));

  return std::move(writer).Take();
}

absl::StatusOr<WireMessage> DecodeDirective(std::string_view bytes) {
  Reader reader(bytes);
  Directive directive;
  auto session_id = reader.Fixed<16>();
  LAVIK_RETURN_IF_ERROR(session_id);
  directive.session_id = *session_id;
  auto basis = ReadProjectionBasis(reader);
  LAVIK_RETURN_IF_ERROR(basis);
  directive.basis = *basis;
  auto authority = ReadAuthorityAnchor(reader);
  LAVIK_RETURN_IF_ERROR(authority);
  directive.authority = std::move(*authority);
  auto identity = ReadDirectiveIdentity(reader);
  LAVIK_RETURN_IF_ERROR(identity);
  directive.identity = *identity;
  auto recipient_node_id = ReadIdentity(reader, "directive recipient node id");
  LAVIK_RETURN_IF_ERROR(recipient_node_id);
  directive.recipient_node_id = std::move(*recipient_node_id);
  auto recipient_boot_id = ReadIdentity(reader, "directive recipient boot id");
  LAVIK_RETURN_IF_ERROR(recipient_boot_id);
  directive.recipient_boot_id = std::move(*recipient_boot_id);
  auto target_node_id = ReadIdentity(reader, "directive target node id");
  LAVIK_RETURN_IF_ERROR(target_node_id);
  directive.target_node_id = std::move(*target_node_id);
  auto target_boot_id = ReadIdentity(reader, "directive target boot id");
  LAVIK_RETURN_IF_ERROR(target_boot_id);
  directive.target_boot_id = std::move(*target_boot_id);
  auto source_node_id = ReadIdentity(reader, "directive source node id");
  LAVIK_RETURN_IF_ERROR(source_node_id);
  directive.source_node_id = std::move(*source_node_id);
  auto source_assignment_id = reader.Fixed<16>();
  LAVIK_RETURN_IF_ERROR(source_assignment_id);
  directive.source_assignment_id = *source_assignment_id;
  auto source_boot_id = ReadIdentity(reader, "directive source boot id");
  LAVIK_RETURN_IF_ERROR(source_boot_id);
  directive.source_boot_id = std::move(*source_boot_id);
  LAVIK_ASSIGN_OR_RETURN(
      directive.source_replication_history_id,
      ReadIdentity(reader, "directive source replication history id"));
  auto manifest_revision = reader.U64();
  LAVIK_RETURN_IF_ERROR(manifest_revision);
  directive.manifest_revision = *manifest_revision;
  auto manifest_digest = reader.Fixed<32>();
  LAVIK_RETURN_IF_ERROR(manifest_digest);
  directive.manifest_digest = *manifest_digest;
  auto partition_replication_epoch = reader.U64();
  LAVIK_RETURN_IF_ERROR(partition_replication_epoch);
  directive.partition_replication_epoch = *partition_replication_epoch;
  auto kind = reader.U8();
  LAVIK_RETURN_IF_ERROR(kind);
  if (*kind < 1 || *kind > static_cast<std::uint8_t>(
                               WireDirectiveKind::kInitializeEmptyPopulation)) {
    return ProtocolError("unknown directive kind");
  }
  directive.kind = static_cast<WireDirectiveKind>(*kind);
  auto payload = reader.String(kMaxOpaqueFieldBytes);
  LAVIK_RETURN_IF_ERROR(payload);
  directive.payload = std::move(*payload);

  LAVIK_RETURN_IF_ERROR(Finish(reader));
  return WireMessage{std::move(directive)};
}

absl::StatusOr<std::string> Encode(const DirectiveResponse& receipt) {
  Writer writer;
  writer.Fixed(receipt.session_id);
  LAVIK_RETURN_IF_ERROR(
      WriteIdentity(writer, receipt.recipient_boot_id, "recipient boot id"));
  WriteDirectiveIdentity(writer, receipt.identity);
  writer.Bool(receipt.started);
  return std::move(writer).Take();
}

absl::StatusOr<WireMessage> DecodeDirectiveResponse(std::string_view bytes) {
  Reader reader(bytes);
  DirectiveResponse receipt;
  auto session_id = reader.Fixed<16>();
  LAVIK_RETURN_IF_ERROR(session_id);
  receipt.session_id = *session_id;
  auto recipient_boot_id = ReadIdentity(reader, "recipient boot id");
  LAVIK_RETURN_IF_ERROR(recipient_boot_id);
  receipt.recipient_boot_id = std::move(*recipient_boot_id);
  auto identity = ReadDirectiveIdentity(reader);
  LAVIK_RETURN_IF_ERROR(identity);
  receipt.identity = *identity;
  auto started = reader.Bool();
  LAVIK_RETURN_IF_ERROR(started);
  receipt.started = *started;
  LAVIK_RETURN_IF_ERROR(Finish(reader));
  return WireMessage{std::move(receipt)};
}

absl::StatusOr<std::string> Encode(const DirectiveResult& result) {
  const auto status_tag = static_cast<std::uint8_t>(result.status);
  if (status_tag < 1 || status_tag > 3) {
    return ProtocolError("unknown directive result status");
  }
  Writer writer;
  writer.Fixed(result.session_id);
  LAVIK_RETURN_IF_ERROR(
      WriteIdentity(writer, result.recipient_boot_id, "recipient boot id"));
  writer.Fixed(result.assignment_id);
  WriteDirectiveIdentity(writer, result.identity);
  writer.U8(status_tag);
  LAVIK_RETURN_IF_ERROR(
      writer.String(result.result, kMaxOpaqueFieldBytes, "directive result"));
  return std::move(writer).Take();
}

absl::StatusOr<WireMessage> DecodeDirectiveResult(std::string_view bytes) {
  Reader reader(bytes);
  DirectiveResult result;
  auto session_id = reader.Fixed<16>();
  LAVIK_RETURN_IF_ERROR(session_id);
  result.session_id = *session_id;
  auto recipient_boot_id = ReadIdentity(reader, "recipient boot id");
  LAVIK_RETURN_IF_ERROR(recipient_boot_id);
  result.recipient_boot_id = std::move(*recipient_boot_id);
  auto assignment_id = reader.Fixed<16>();
  LAVIK_RETURN_IF_ERROR(assignment_id);
  result.assignment_id = *assignment_id;
  auto identity = ReadDirectiveIdentity(reader);
  LAVIK_RETURN_IF_ERROR(identity);
  result.identity = *identity;
  auto status_tag = reader.U8();
  LAVIK_RETURN_IF_ERROR(status_tag);
  if (*status_tag < 1 || *status_tag > 3) {
    return ProtocolError("unknown directive result status");
  }
  result.status = static_cast<DirectiveResultStatus>(*status_tag);
  LAVIK_ASSIGN_OR_RETURN(result.result, reader.String(kMaxOpaqueFieldBytes));
  LAVIK_RETURN_IF_ERROR(Finish(reader));
  return WireMessage{std::move(result)};
}

absl::StatusOr<std::string> Encode(const ResultCommitted& committed) {
  Writer writer;
  writer.Fixed(committed.session_id);
  LAVIK_RETURN_IF_ERROR(
      WriteIdentity(writer, committed.recipient_boot_id, "recipient boot id"));
  WriteDirectiveIdentity(writer, committed.identity);
  writer.U64(committed.committed_index);
  return std::move(writer).Take();
}

absl::StatusOr<WireMessage> DecodeResultCommitted(std::string_view bytes) {
  Reader reader(bytes);
  ResultCommitted committed;
  auto session_id = reader.Fixed<16>();
  LAVIK_RETURN_IF_ERROR(session_id);
  committed.session_id = *session_id;
  auto recipient_boot_id = ReadIdentity(reader, "recipient boot id");
  LAVIK_RETURN_IF_ERROR(recipient_boot_id);
  committed.recipient_boot_id = std::move(*recipient_boot_id);
  auto identity = ReadDirectiveIdentity(reader);
  LAVIK_RETURN_IF_ERROR(identity);
  committed.identity = *identity;
  auto committed_index = reader.U64();
  LAVIK_RETURN_IF_ERROR(committed_index);
  committed.committed_index = *committed_index;
  LAVIK_RETURN_IF_ERROR(Finish(reader));
  return WireMessage{std::move(committed)};
}

absl::StatusOr<std::string> Encode(const ResultNoLongerTracked& result) {
  Writer writer;
  writer.Fixed(result.session_id);
  LAVIK_RETURN_IF_ERROR(
      WriteIdentity(writer, result.recipient_boot_id, "recipient boot id"));
  WriteDirectiveIdentity(writer, result.identity);
  return std::move(writer).Take();
}

absl::StatusOr<WireMessage> DecodeResultNoLongerTracked(
    std::string_view bytes) {
  Reader reader(bytes);
  ResultNoLongerTracked result;
  auto session_id = reader.Fixed<16>();
  LAVIK_RETURN_IF_ERROR(session_id);
  result.session_id = *session_id;
  auto recipient_boot_id = ReadIdentity(reader, "recipient boot id");
  LAVIK_RETURN_IF_ERROR(recipient_boot_id);
  result.recipient_boot_id = std::move(*recipient_boot_id);
  auto identity = ReadDirectiveIdentity(reader);
  LAVIK_RETURN_IF_ERROR(identity);
  result.identity = *identity;
  LAVIK_RETURN_IF_ERROR(Finish(reader));
  return WireMessage{std::move(result)};
}

}  // namespace

absl::Status ValidateClientService(
    const ServiceDeclaration& declaration,
    const ClientServiceCapabilities& capabilities, bool require_installed) {
  if (!declaration.client_mode ||
      !IsValidClientMode(*declaration.client_mode) ||
      declaration.creation_id == WireId128{} ||
      declaration.genesis_commit_index == 0) {
    return absl::FailedPreconditionError(
        "committed client service declaration is missing");
  }
  const auto mode = *declaration.client_mode;
  const auto required_mode =
      mode == ClientMode::kSingle ? kSingleServiceMode : kClusterServiceMode;
  constexpr auto required_services =
      kDb0GroupAuthority | kReplicaPopulationRead;
  if ((capabilities.supported_modes & required_mode) == 0 ||
      (capabilities.services & required_services) != required_services) {
    return absl::FailedPreconditionError(
        "Data boot lacks required client mode or service capabilities");
  }
  if (require_installed &&
      (capabilities.installed_mode != declaration.client_mode ||
       capabilities.database_count !=
           (mode == ClientMode::kSingle ? 16u : 1u))) {
    return absl::FailedPreconditionError(
        "Data installed client mode or storage layout is incompatible");
  }
  return absl::OkStatus();
}

MessageType MessageTypeOf(const WireMessage& message) noexcept {
  return std::visit(
      [](const auto& value) -> MessageType {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, BootstrapHello>) {
          return MessageType::kBootstrapHello;
        } else if constexpr (std::is_same_v<T, BootstrapReply>) {
          return MessageType::kBootstrapReply;
        } else if constexpr (std::is_same_v<T, ClientHello>) {
          return MessageType::kClientHello;
        } else if constexpr (std::is_same_v<T, NodeControlUpdate>) {
          return MessageType::kNodeControlUpdate;
        } else if constexpr (std::is_same_v<T, ServerHello>) {
          return MessageType::kServerHello;
        } else if constexpr (std::is_same_v<T, TransferStart>) {
          return MessageType::kTransferStart;
        } else if constexpr (std::is_same_v<T, TransferChunk>) {
          return MessageType::kTransferChunk;
        } else if constexpr (std::is_same_v<T, TransferEnd>) {
          return MessageType::kTransferEnd;
        } else if constexpr (std::is_same_v<T, TransferAbort>) {
          return MessageType::kTransferAbort;
        } else if constexpr (std::is_same_v<T, FullStateApplied>) {
          return MessageType::kFullStateApplied;
        } else if constexpr (std::is_same_v<T, Heartbeat>) {
          return MessageType::kHeartbeat;
        } else if constexpr (std::is_same_v<T, HeartbeatAck>) {
          return MessageType::kHeartbeatAck;

        } else if constexpr (std::is_same_v<T, Fence>) {
          return MessageType::kFence;
        } else if constexpr (std::is_same_v<T, FenceAck>) {
          return MessageType::kFenceAck;
        } else if constexpr (std::is_same_v<T, Directive>) {
          return MessageType::kDirective;
        } else if constexpr (std::is_same_v<T, DirectiveResponse>) {
          return MessageType::kDirectiveResponse;
        } else if constexpr (std::is_same_v<T, DirectiveResult>) {
          return MessageType::kDirectiveResult;
        } else if constexpr (std::is_same_v<T, ResultCommitted>) {
          return MessageType::kResultCommitted;
        } else if constexpr (std::is_same_v<T, ResultNoLongerTracked>) {
          return MessageType::kResultNoLongerTracked;
        } else {
          static_assert(std::is_same_v<T, FullDesiredState>);
          return MessageType::kFullDesiredState;
        }
      },
      message);
}

absl::StatusOr<EncodedMessage> EncodeMessage(const WireMessage& message) {
  auto encoded = std::visit(
      [](const auto& value) -> absl::StatusOr<std::string> {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, FullDesiredState>) {
          return EncodeFullDesiredState(value);
        } else {
          if constexpr (std::is_same_v<T, NodeControlUpdate>)
            return EncodeNodeControlUpdate(value);
          else
            return Encode(value);
        }
      },
      message);
  LAVIK_RETURN_IF_ERROR(encoded);
  const MessageType type = MessageTypeOf(message);
  if (RequiresSingleFrame(type) && encoded->size() > kMaxFramePayloadBytes) {
    return ResourceLimit("non-fragmentable control message exceeds one frame");
  }
  return EncodedMessage{type, std::move(*encoded)};
}

absl::StatusOr<WireMessage> DecodeMessage(MessageType type,
                                          std::string_view payload) {
  if (RequiresSingleFrame(type) && payload.size() > kMaxFramePayloadBytes) {
    return ResourceLimit("non-fragmentable control message exceeds one frame");
  }
  switch (type) {
    case MessageType::kNodeControlUpdate: {
      auto update = DecodeNodeControlUpdate(payload);
      LAVIK_RETURN_IF_ERROR(update);
      return WireMessage{std::move(*update)};
    }
    case MessageType::kBootstrapHello:
      return DecodeBootstrapHello(payload);
    case MessageType::kBootstrapReply:
      return DecodeBootstrapReply(payload);
    case MessageType::kClientHello:
      return DecodeClientHello(payload);
    case MessageType::kServerHello:
      return DecodeServerHello(payload);
    case MessageType::kTransferStart:
      return DecodeTransferStart(payload);
    case MessageType::kTransferChunk:
      return DecodeTransferChunk(payload);
    case MessageType::kTransferEnd:
      return DecodeTransferEnd(payload);
    case MessageType::kTransferAbort:
      return DecodeTransferAbort(payload);
    case MessageType::kFullStateApplied:
      return DecodeFullStateApplied(payload);
    case MessageType::kHeartbeat:
      return DecodeHeartbeat(payload);
    case MessageType::kHeartbeatAck:
      return DecodeHeartbeatAck(payload);
    case MessageType::kFence:
      return DecodeFence(payload);
    case MessageType::kFenceAck:
      return DecodeFenceAck(payload);
    case MessageType::kDirective:
      return DecodeDirective(payload);
    case MessageType::kDirectiveResponse:
      return DecodeDirectiveResponse(payload);
    case MessageType::kDirectiveResult:
      return DecodeDirectiveResult(payload);
    case MessageType::kResultCommitted:
      return DecodeResultCommitted(payload);
    case MessageType::kResultNoLongerTracked:
      return DecodeResultNoLongerTracked(payload);
    case MessageType::kFullDesiredState: {
      auto desired = DecodeFullDesiredState(payload);
      LAVIK_RETURN_IF_ERROR(desired);
      return WireMessage(std::move(*desired));
    }
  }
  return ProtocolError("unknown message type");
}

bool RequiresSingleFrame(MessageType type) noexcept {
  switch (type) {
    case MessageType::kBootstrapHello:
    case MessageType::kBootstrapReply:
    case MessageType::kHeartbeat:
    case MessageType::kHeartbeatAck:  // contains the authority grant
    case MessageType::kFence:
    case MessageType::kFenceAck:
    case MessageType::kFullDesiredState:
      return true;
    default:
      return false;
  }
}

namespace {

absl::Status WriteCount(Writer& writer, std::size_t count, std::size_t cap,
                        std::string_view field) {
  if (count > cap || count > std::numeric_limits<std::uint32_t>::max()) {
    return ResourceLimit(std::string(field) + " exceeds its entry cap");
  }
  writer.U32(static_cast<std::uint32_t>(count));
  return absl::OkStatus();
}

absl::StatusOr<std::uint32_t> ReadCount(Reader& reader, std::size_t cap,
                                        std::string_view field) {
  auto count = reader.U32();
  LAVIK_RETURN_IF_ERROR(count);
  if (*count > cap) {
    return ResourceLimit(std::string(field) + " exceeds its entry cap");
  }
  return *count;
}

absl::Status WriteDataEndpoint(Writer& writer,
                               const WireDataEndpoint& endpoint) {
  LAVIK_RETURN_IF_ERROR(
      WriteIdentity(writer, endpoint.node_id, "data node id"));
  LAVIK_RETURN_IF_ERROR(
      writer.String(endpoint.host, kMaxIdentifierBytes, "data endpoint"));
  writer.U16(endpoint.port);
  writer.U16(endpoint.tls_port);
  return absl::OkStatus();
}

absl::StatusOr<WireDataEndpoint> ReadDataEndpoint(Reader& reader) {
  WireDataEndpoint endpoint;
  auto node_id = ReadIdentity(reader, "data node id");
  LAVIK_RETURN_IF_ERROR(node_id);
  endpoint.node_id = std::move(*node_id);
  auto host = reader.String(kMaxIdentifierBytes);
  LAVIK_RETURN_IF_ERROR(host);
  endpoint.host = std::move(*host);
  auto port = reader.U16();
  LAVIK_RETURN_IF_ERROR(port);
  endpoint.port = *port;
  auto tls_port = reader.U16();
  LAVIK_RETURN_IF_ERROR(tls_port);
  endpoint.tls_port = *tls_port;
  return endpoint;
}

absl::Status WriteDesiredMember(Writer& writer,
                                const WireDesiredMember& member) {
  LAVIK_RETURN_IF_ERROR(
      WriteIdentity(writer, member.node_id, "group member node id"));
  writer.Fixed(member.assignment_id);
  return absl::OkStatus();
}

absl::StatusOr<WireDesiredMember> ReadDesiredMember(Reader& reader) {
  WireDesiredMember member;
  auto node_id = ReadIdentity(reader, "group member node id");
  LAVIK_RETURN_IF_ERROR(node_id);
  member.node_id = std::move(*node_id);
  auto assignment_id = reader.Fixed<16>();
  LAVIK_RETURN_IF_ERROR(assignment_id);
  member.assignment_id = *assignment_id;
  return member;
}

absl::Status ValidateFailoverTransition(
    const WireFailoverTransition& transition, const WireDesiredGroup& group) {
  if (IsZeroId(transition.transition_id) || transition.revision == 0 ||
      transition.target_term == 0) {
    return ProtocolError("failover transition has an empty identity");
  }
  if (transition.mode != WireFailoverMode::kControlled &&
      transition.mode != WireFailoverMode::kUncontrolled) {
    return ProtocolError("unknown failover transition mode");
  }
  if (transition.recovery_deadline_unix_ms.has_value() &&
      (transition.mode != WireFailoverMode::kUncontrolled ||
       *transition.recovery_deadline_unix_ms == 0 ||
       *transition.recovery_deadline_unix_ms >
           static_cast<std::uint64_t>(
               std::numeric_limits<std::int64_t>::max()))) {
    return ProtocolError("recovery deadline is invalid");
  }
  if (transition.candidate_action.has_value()) {
    const WireFailoverCandidateAction& action = *transition.candidate_action;
    if (IsZeroId(action.action_id) ||
        !IsCanonicalIdentity160(action.candidate.node_id) ||
        IsZeroId(action.candidate.assignment_id) ||
        !IsCanonicalIdentity160(action.candidate.boot_id) ||
        (!action.operator_recovery &&
         (action.domain.source_group_term == 0 ||
          action.domain.source_group_term >= transition.target_term ||
          !IsCanonicalIdentity160(action.domain.source_node_id) ||
          IsZeroId(action.domain.source_assignment_id) ||
          !IsCanonicalIdentity160(action.domain.source_boot_id) ||
          !IsCanonicalIdentity160(action.domain.source_history_id)))) {
      return ProtocolError("failover candidate action is invalid");
    }
    if (!action.operator_recovery &&
        (action.domain.flow_count == 0 ||
         action.domain.flow_count > kMaxCandidateFlows)) {
      return ResourceLimit(
          "failover compatibility flow count is outside its protocol cap");
    }
    if (action.operator_recovery &&
        (transition.mode != WireFailoverMode::kUncontrolled ||
         action.domain != WireFailoverCompatibilityDomain{} ||
         (action.authorization.has_value() &&
          action.authorization->loss_if_cutover !=
              WireFailoverLoss::kUnknown))) {
      return ProtocolError(
          "operator recovery must have unknown loss and no source domain");
    }
    const auto candidate =
        std::find_if(group.members.begin(), group.members.end(),
                     [&action](const WireDesiredMember& member) {
                       return member.node_id == action.candidate.node_id;
                     });
    if (candidate == group.members.end() ||
        candidate->assignment_id != action.candidate.assignment_id) {
      return ProtocolError(
          "failover candidate is not the projected member incarnation");
    }
    if (action.authorization.has_value()) {
      const WireFailoverAuthorization& authorization = *action.authorization;
      if (authorization.authorized_revision == 0 ||
          authorization.authorized_revision > transition.revision ||
          (authorization.loss_if_cutover != WireFailoverLoss::kNone &&
           authorization.loss_if_cutover != WireFailoverLoss::kUnknown)) {
        return ProtocolError("failover authorization is invalid");
      }
    }
  }

  if (transition.mode == WireFailoverMode::kControlled) {
    if (!group.grant_active ||
        group.group_term == std::numeric_limits<std::uint64_t>::max() ||
        transition.target_term != group.group_term + 1 ||
        !group.owner_node_id.has_value() ||
        !group.owner_assignment_id.has_value() ||
        !transition.candidate_action.has_value()) {
      return ProtocolError(
          "controlled failover disagrees with current group authority");
    }
    const WireFailoverCandidateAction& action = *transition.candidate_action;
    if (action.domain.source_group_term != group.group_term ||
        action.domain.source_node_id != *group.owner_node_id ||
        action.domain.source_assignment_id != *group.owner_assignment_id ||
        action.candidate.node_id == *group.owner_node_id ||
        (action.authorization.has_value() &&
         action.authorization->loss_if_cutover != WireFailoverLoss::kNone)) {
      return ProtocolError("controlled failover lineage is inconsistent");
    }
  } else if (group.grant_active || transition.target_term != group.group_term) {
    return ProtocolError(
        "uncontrolled failover disagrees with fenced group authority");
  }
  return absl::OkStatus();
}

absl::Status WriteFailoverTransition(Writer& writer,
                                     const WireFailoverTransition& transition,
                                     const WireDesiredGroup& group) {
  LAVIK_RETURN_IF_ERROR(ValidateFailoverTransition(transition, group));
  writer.Fixed(transition.transition_id);
  writer.U64(transition.revision);
  writer.U8(static_cast<std::uint8_t>(transition.mode));
  writer.U64(transition.target_term);
  writer.Bool(transition.candidate_action.has_value());
  if (transition.candidate_action.has_value()) {
    const WireFailoverCandidateAction& action = *transition.candidate_action;
    writer.Fixed(action.action_id);
    LAVIK_RETURN_IF_ERROR(WriteIdentity(writer, action.candidate.node_id,
                                        "failover candidate node id"));
    writer.Fixed(action.candidate.assignment_id);
    LAVIK_RETURN_IF_ERROR(WriteIdentity(writer, action.candidate.boot_id,
                                        "failover candidate boot id"));
    writer.Bool(action.operator_recovery);
    if (!action.operator_recovery) {
      writer.U64(action.domain.source_group_term);
      LAVIK_RETURN_IF_ERROR(WriteIdentity(writer, action.domain.source_node_id,
                                          "failover source node id"));
      writer.Fixed(action.domain.source_assignment_id);
      LAVIK_RETURN_IF_ERROR(WriteIdentity(writer, action.domain.source_boot_id,
                                          "failover source boot id"));
      LAVIK_RETURN_IF_ERROR(WriteIdentity(writer,
                                          action.domain.source_history_id,
                                          "failover source history id"));
      writer.U32(action.domain.flow_count);
    }
    writer.Bool(action.authorization.has_value());
    if (action.authorization.has_value()) {
      writer.U64(action.authorization->authorized_revision);
      writer.U8(
          static_cast<std::uint8_t>(action.authorization->loss_if_cutover));
    }
  }
  writer.Bool(transition.recovery_deadline_unix_ms.has_value());
  if (transition.recovery_deadline_unix_ms.has_value())
    writer.U64(*transition.recovery_deadline_unix_ms);
  return absl::OkStatus();
}

absl::StatusOr<WireFailoverTransition> ReadFailoverTransition(
    Reader& reader, const WireDesiredGroup& group) {
  WireFailoverTransition transition;
  auto transition_id = reader.Fixed<16>();
  LAVIK_RETURN_IF_ERROR(transition_id);
  transition.transition_id = *transition_id;
  auto revision = reader.U64();
  LAVIK_RETURN_IF_ERROR(revision);
  transition.revision = *revision;
  auto mode = reader.U8();
  LAVIK_RETURN_IF_ERROR(mode);
  transition.mode = static_cast<WireFailoverMode>(*mode);
  auto target_term = reader.U64();
  LAVIK_RETURN_IF_ERROR(target_term);
  transition.target_term = *target_term;
  auto has_action = reader.Bool();
  LAVIK_RETURN_IF_ERROR(has_action);
  if (*has_action) {
    WireFailoverCandidateAction action;
    auto action_id = reader.Fixed<16>();
    LAVIK_RETURN_IF_ERROR(action_id);
    action.action_id = *action_id;
    LAVIK_ASSIGN_OR_RETURN(action.candidate.node_id,
                           ReadIdentity(reader, "failover candidate node id"));
    LAVIK_ASSIGN_OR_RETURN(action.candidate.assignment_id, reader.Fixed<16>());
    LAVIK_ASSIGN_OR_RETURN(action.candidate.boot_id,
                           ReadIdentity(reader, "failover candidate boot id"));
    auto operator_recovery = reader.Bool();
    LAVIK_RETURN_IF_ERROR(operator_recovery);
    action.operator_recovery = *operator_recovery;
    if (!action.operator_recovery) {
      auto source_group_term = reader.U64();
      LAVIK_RETURN_IF_ERROR(source_group_term);
      action.domain.source_group_term = *source_group_term;
      LAVIK_ASSIGN_OR_RETURN(action.domain.source_node_id,
                             ReadIdentity(reader, "failover source node id"));
      LAVIK_ASSIGN_OR_RETURN(action.domain.source_assignment_id,
                             reader.Fixed<16>());
      LAVIK_ASSIGN_OR_RETURN(action.domain.source_boot_id,
                             ReadIdentity(reader, "failover source boot id"));
      LAVIK_ASSIGN_OR_RETURN(
          action.domain.source_history_id,
          ReadIdentity(reader, "failover source history id"));
      auto flow_count = reader.U32();
      LAVIK_RETURN_IF_ERROR(flow_count);
      action.domain.flow_count = *flow_count;
    }
    auto has_authorization = reader.Bool();
    LAVIK_RETURN_IF_ERROR(has_authorization);
    if (*has_authorization) {
      WireFailoverAuthorization authorization;
      auto authorized_revision = reader.U64();
      LAVIK_RETURN_IF_ERROR(authorized_revision);
      authorization.authorized_revision = *authorized_revision;
      auto loss = reader.U8();
      LAVIK_RETURN_IF_ERROR(loss);
      authorization.loss_if_cutover = static_cast<WireFailoverLoss>(*loss);
      action.authorization = authorization;
    }
    transition.candidate_action = std::move(action);
  }
  auto has_deadline = reader.Bool();
  LAVIK_RETURN_IF_ERROR(has_deadline);
  if (*has_deadline) {
    LAVIK_ASSIGN_OR_RETURN(transition.recovery_deadline_unix_ms, reader.U64());
  }
  LAVIK_RETURN_IF_ERROR(ValidateFailoverTransition(transition, group));
  return transition;
}

absl::Status WriteDesiredGroup(Writer& writer, const WireDesiredGroup& group) {
  LAVIK_RETURN_IF_ERROR(
      writer.String(group.group_id, kMaxIdentifierBytes, "group id"));
  LAVIK_RETURN_IF_ERROR(WriteCount(writer, group.members.size(),
                                   kMaxProjectedNodes, "group members"));
  for (const WireDesiredMember& member : group.members) {
    LAVIK_RETURN_IF_ERROR(WriteDesiredMember(writer, member));
  }
  if (group.owner_node_id.has_value() !=
      group.owner_assignment_id.has_value()) {
    return ProtocolError("group owner node and assignment presence must match");
  }
  if (!group.owner_node_id.has_value() && group.grant_active) {
    return ProtocolError("grantless group cannot carry an active grant");
  }
  writer.Bool(group.owner_node_id.has_value());
  if (group.owner_node_id.has_value()) {
    LAVIK_RETURN_IF_ERROR(
        WriteIdentity(writer, *group.owner_node_id, "group owner node id"));
    writer.Fixed(*group.owner_assignment_id);
  }
  writer.U64(group.group_term);
  writer.Bool(group.grant_active);
  if (group.activation_action_id.has_value() &&
      (!group.grant_active || IsZeroId(*group.activation_action_id))) {
    return ProtocolError(
        "grant activation action requires a nonzero active grant");
  }
  writer.Bool(group.activation_action_id.has_value());
  if (group.activation_action_id.has_value()) {
    writer.Fixed(*group.activation_action_id);
  }
  LAVIK_RETURN_IF_ERROR(WriteCount(writer, group.slot_ranges.size(),
                                   kMaxManifestEntries, "group slot ranges"));
  for (const WireSlotRange& range : group.slot_ranges) {
    if (range.first > range.last || range.last >= 16384) {
      return ProtocolError("group slot range is invalid");
    }
    writer.U16(range.first);
    writer.U16(range.last);
  }
  writer.U64(group.manifest_revision);
  writer.Fixed(group.manifest_digest);
  writer.U64(group.partition_replication_epoch);
  writer.Bool(group.steady_replication_enabled);
  writer.Bool(group.failover_transition.has_value());
  if (group.failover_transition.has_value()) {
    return WriteFailoverTransition(writer, *group.failover_transition, group);
  }
  return absl::OkStatus();
}

absl::StatusOr<WireDesiredGroup> ReadDesiredGroup(Reader& reader) {
  WireDesiredGroup group;
  auto group_id = reader.String(kMaxIdentifierBytes);
  LAVIK_RETURN_IF_ERROR(group_id);
  group.group_id = std::move(*group_id);
  auto member_count = ReadCount(reader, kMaxProjectedNodes, "group members");
  LAVIK_RETURN_IF_ERROR(member_count);
  group.members.reserve(*member_count);
  for (std::uint32_t i = 0; i < *member_count; ++i) {
    auto member = ReadDesiredMember(reader);
    LAVIK_RETURN_IF_ERROR(member);
    group.members.push_back(std::move(*member));
  }
  auto has_owner = reader.Bool();
  LAVIK_RETURN_IF_ERROR(has_owner);
  if (*has_owner) {
    LAVIK_ASSIGN_OR_RETURN(group.owner_node_id,
                           ReadIdentity(reader, "group owner node id"));
    LAVIK_ASSIGN_OR_RETURN(group.owner_assignment_id, reader.Fixed<16>());
  }
  auto group_term = reader.U64();
  LAVIK_RETURN_IF_ERROR(group_term);
  group.group_term = *group_term;
  auto grant_active = reader.Bool();
  LAVIK_RETURN_IF_ERROR(grant_active);
  group.grant_active = *grant_active;
  if (!group.owner_node_id.has_value() && group.grant_active) {
    return ProtocolError("grantless group cannot carry an active grant");
  }
  auto has_activation_action = reader.Bool();
  LAVIK_RETURN_IF_ERROR(has_activation_action);
  if (*has_activation_action) {
    auto action_id = reader.Fixed<16>();
    LAVIK_RETURN_IF_ERROR(action_id);
    if (!group.grant_active || IsZeroId(*action_id)) {
      return ProtocolError(
          "grant activation action requires a nonzero active grant");
    }
    group.activation_action_id = *action_id;
  }
  auto range_count =
      ReadCount(reader, kMaxManifestEntries, "group slot ranges");
  LAVIK_RETURN_IF_ERROR(range_count);
  group.slot_ranges.reserve(*range_count);
  for (std::uint32_t i = 0; i < *range_count; ++i) {
    auto first = reader.U16();
    LAVIK_RETURN_IF_ERROR(first);
    auto last = reader.U16();
    LAVIK_RETURN_IF_ERROR(last);
    if (*first > *last || *last >= 16384) {
      return ProtocolError("group slot range is invalid");
    }
    group.slot_ranges.push_back({.first = *first, .last = *last});
  }
  auto manifest_revision = reader.U64();
  LAVIK_RETURN_IF_ERROR(manifest_revision);
  group.manifest_revision = *manifest_revision;
  auto manifest_digest = reader.Fixed<32>();
  LAVIK_RETURN_IF_ERROR(manifest_digest);
  group.manifest_digest = *manifest_digest;
  auto partition_replication_epoch = reader.U64();
  LAVIK_RETURN_IF_ERROR(partition_replication_epoch);
  group.partition_replication_epoch = *partition_replication_epoch;
  auto steady_replication_enabled = reader.Bool();
  LAVIK_RETURN_IF_ERROR(steady_replication_enabled);
  group.steady_replication_enabled = *steady_replication_enabled;
  auto has_failover_transition = reader.Bool();
  LAVIK_RETURN_IF_ERROR(has_failover_transition);
  if (*has_failover_transition) {
    LAVIK_ASSIGN_OR_RETURN(group.failover_transition,
                           ReadFailoverTransition(reader, group));
  }
  return group;
}

bool ManifestEntriesCanonical(
    const std::vector<WireManifestEntry>& entries) noexcept {
  for (std::size_t i = 1; i < entries.size(); ++i) {
    if (entries[i - 1].partition_id >= entries[i].partition_id) return false;
  }
  return true;
}

absl::Status WriteManifest(Writer& writer,
                           const WireManifestDocument& manifest) {
  if (!ManifestEntriesCanonical(manifest.entries)) {
    return ProtocolError(
        "manifest entries must be strictly sorted by partition id");
  }
  writer.U64(manifest.revision);
  writer.Fixed(manifest.digest);
  LAVIK_RETURN_IF_ERROR(WriteCount(writer, manifest.entries.size(),
                                   kMaxManifestEntries, "manifest entries"));
  for (const WireManifestEntry& entry : manifest.entries) {
    if (entry.partition_id >= 16384) {
      return ProtocolError("manifest partition id is out of range");
    }
    writer.U16(entry.partition_id);
    writer.U64(entry.logical_epoch);
  }
  return absl::OkStatus();
}

absl::StatusOr<WireManifestDocument> ReadManifest(Reader& reader) {
  WireManifestDocument manifest;
  auto revision = reader.U64();
  LAVIK_RETURN_IF_ERROR(revision);
  manifest.revision = *revision;
  auto digest = reader.Fixed<32>();
  LAVIK_RETURN_IF_ERROR(digest);
  manifest.digest = *digest;
  auto entry_count = ReadCount(reader, kMaxManifestEntries, "manifest entries");
  LAVIK_RETURN_IF_ERROR(entry_count);
  manifest.entries.reserve(*entry_count);
  for (std::uint32_t i = 0; i < *entry_count; ++i) {
    auto partition_id = reader.U16();
    LAVIK_RETURN_IF_ERROR(partition_id);
    if (*partition_id >= 16384) {
      return ProtocolError("manifest partition id is out of range");
    }
    auto logical_epoch = reader.U64();
    LAVIK_RETURN_IF_ERROR(logical_epoch);
    manifest.entries.push_back(
        {.partition_id = *partition_id, .logical_epoch = *logical_epoch});
  }
  if (!ManifestEntriesCanonical(manifest.entries)) {
    return ProtocolError(
        "manifest entries must be strictly sorted by partition id");
  }
  return manifest;
}

absl::Status WriteProjectedDirective(Writer& writer,
                                     const WireProjectedDirective& directive) {
  LAVIK_RETURN_IF_ERROR(WriteAuthorityAnchor(writer, directive.authority));
  WriteDirectiveIdentity(writer, directive.identity);
  LAVIK_RETURN_IF_ERROR(
      WriteIdentity(writer, directive.recipient_node_id, "recipient node id"));
  LAVIK_RETURN_IF_ERROR(
      WriteIdentity(writer, directive.recipient_boot_id, "recipient boot id"));
  LAVIK_RETURN_IF_ERROR(
      WriteIdentity(writer, directive.target_node_id, "target node id"));
  LAVIK_RETURN_IF_ERROR(
      WriteIdentity(writer, directive.target_boot_id, "target boot id"));
  LAVIK_RETURN_IF_ERROR(
      WriteIdentity(writer, directive.source_node_id, "source node id"));
  writer.Fixed(directive.source_assignment_id);
  LAVIK_RETURN_IF_ERROR(
      WriteIdentity(writer, directive.source_boot_id, "source boot id"));
  LAVIK_RETURN_IF_ERROR(WriteIdentity(writer,
                                      directive.source_replication_history_id,
                                      "source replication history id"));
  writer.U64(directive.manifest_revision);
  writer.Fixed(directive.manifest_digest);
  writer.U64(directive.partition_replication_epoch);
  const auto kind = static_cast<std::uint8_t>(directive.kind);
  if (kind < 1 || kind > static_cast<std::uint8_t>(
                             WireDirectiveKind::kInitializeEmptyPopulation)) {
    return ProtocolError("unknown directive kind");
  }
  writer.U8(kind);
  LAVIK_RETURN_IF_ERROR(writer.String(directive.payload, kMaxOpaqueFieldBytes,
                                      "directive payload"));

  return absl::OkStatus();
}

absl::StatusOr<WireProjectedDirective> ReadProjectedDirective(Reader& reader) {
  WireProjectedDirective directive;
  auto authority = ReadAuthorityAnchor(reader);
  LAVIK_RETURN_IF_ERROR(authority);
  directive.authority = std::move(*authority);
  auto identity = ReadDirectiveIdentity(reader);
  LAVIK_RETURN_IF_ERROR(identity);
  directive.identity = *identity;
  auto recipient_node_id = ReadIdentity(reader, "recipient node id");
  LAVIK_RETURN_IF_ERROR(recipient_node_id);
  directive.recipient_node_id = std::move(*recipient_node_id);
  auto recipient_boot_id = ReadIdentity(reader, "recipient boot id");
  LAVIK_RETURN_IF_ERROR(recipient_boot_id);
  directive.recipient_boot_id = std::move(*recipient_boot_id);
  auto target_node_id = ReadIdentity(reader, "target node id");
  LAVIK_RETURN_IF_ERROR(target_node_id);
  directive.target_node_id = std::move(*target_node_id);
  auto target_boot_id = ReadIdentity(reader, "target boot id");
  LAVIK_RETURN_IF_ERROR(target_boot_id);
  directive.target_boot_id = std::move(*target_boot_id);
  auto source_node_id = ReadIdentity(reader, "source node id");
  LAVIK_RETURN_IF_ERROR(source_node_id);
  directive.source_node_id = std::move(*source_node_id);
  auto source_assignment_id = reader.Fixed<16>();
  LAVIK_RETURN_IF_ERROR(source_assignment_id);
  directive.source_assignment_id = *source_assignment_id;
  auto source_boot_id = ReadIdentity(reader, "source boot id");
  LAVIK_RETURN_IF_ERROR(source_boot_id);
  directive.source_boot_id = std::move(*source_boot_id);
  LAVIK_ASSIGN_OR_RETURN(directive.source_replication_history_id,
                         ReadIdentity(reader, "source replication history id"));
  auto manifest_revision = reader.U64();
  LAVIK_RETURN_IF_ERROR(manifest_revision);
  directive.manifest_revision = *manifest_revision;
  auto manifest_digest = reader.Fixed<32>();
  LAVIK_RETURN_IF_ERROR(manifest_digest);
  directive.manifest_digest = *manifest_digest;
  auto partition_replication_epoch = reader.U64();
  LAVIK_RETURN_IF_ERROR(partition_replication_epoch);
  directive.partition_replication_epoch = *partition_replication_epoch;
  auto kind = reader.U8();
  LAVIK_RETURN_IF_ERROR(kind);
  if (*kind < 1 || *kind > static_cast<std::uint8_t>(
                               WireDirectiveKind::kInitializeEmptyPopulation)) {
    return ProtocolError("unknown directive kind");
  }
  directive.kind = static_cast<WireDirectiveKind>(*kind);
  auto payload = reader.String(kMaxOpaqueFieldBytes);
  LAVIK_RETURN_IF_ERROR(payload);
  directive.payload = std::move(*payload);

  return directive;
}

template <class T, class Write>
absl::Status WriteStateList(Writer& w, const std::vector<T>& values,
                            std::size_t cap, Write write) {
  LAVIK_RETURN_IF_ERROR(WriteCount(w, values.size(), cap, "state entries"));
  for (const auto& value : values) {
    LAVIK_RETURN_IF_ERROR(write(w, value));
  }
  return absl::OkStatus();
}

template <class T, class Read>
absl::StatusOr<std::vector<T>> ReadStateList(Reader& r, std::size_t cap,
                                             Read read) {
  auto count = ReadCount(r, cap, "state entries");
  LAVIK_RETURN_IF_ERROR(count);
  std::vector<T> values;
  values.reserve(*count);
  for (std::uint32_t i = 0; i < *count; ++i) {
    auto value = read(r);
    LAVIK_RETURN_IF_ERROR(value);
    values.push_back(std::move(*value));
  }
  return values;
}

absl::Status WriteRoute(Writer& w, const WireRoutingGroup& g) {
  LAVIK_RETURN_IF_ERROR(w.String(g.group_id, kMaxIdentifierBytes, "group"));
  w.U64(g.term);
  w.Bool(g.owner.has_value());
  if (g.owner) {
    LAVIK_RETURN_IF_ERROR(WriteIdentity(w, *g.owner, "owner"));
    w.Fixed(g.owner_assignment);
  }
  w.Bool(g.available);
  LAVIK_RETURN_IF_ERROR(WriteStateList(
      w, g.members, kMaxProjectedNodes, [](Writer& out, const std::string& id) {
        return WriteIdentity(out, id, "member");
      }));
  return WriteStateList(w, g.slots, kMaxManifestEntries,
                        [](Writer& out, const WireSlotRange& range) {
                          if (range.first > range.last || range.last >= 16384)
                            return ProtocolError("invalid route slots");
                          out.U16(range.first);
                          out.U16(range.last);
                          return absl::OkStatus();
                        });
}

absl::StatusOr<WireRoutingGroup> ReadRoute(Reader& r) {
  WireRoutingGroup g;
  auto id = r.String(kMaxIdentifierBytes);
  LAVIK_RETURN_IF_ERROR(id);
  g.group_id = std::move(*id);
  auto term = r.U64();
  LAVIK_RETURN_IF_ERROR(term);
  g.term = *term;
  auto owner = r.Bool();
  LAVIK_RETURN_IF_ERROR(owner);
  if (*owner) {
    LAVIK_ASSIGN_OR_RETURN(g.owner, ReadIdentity(r, "owner"));
    LAVIK_ASSIGN_OR_RETURN(g.owner_assignment, r.Fixed<16>());
  }
  auto available = r.Bool();
  LAVIK_RETURN_IF_ERROR(available);
  g.available = *available;
  auto members = ReadStateList<std::string>(
      r, kMaxProjectedNodes,
      [](Reader& in) { return ReadIdentity(in, "member"); });
  LAVIK_RETURN_IF_ERROR(members);
  g.members = std::move(*members);
  auto slots = ReadStateList<WireSlotRange>(
      r, kMaxManifestEntries, [](Reader& in) -> absl::StatusOr<WireSlotRange> {
        auto first = in.U16();
        LAVIK_RETURN_IF_ERROR(first);
        auto last = in.U16();
        LAVIK_RETURN_IF_ERROR(last);
        if (*first > *last || *last >= 16384)
          return ProtocolError("invalid route slots");
        return WireSlotRange{*first, *last};
      });
  LAVIK_RETURN_IF_ERROR(slots);
  g.slots = std::move(*slots);
  if (g.available && (!g.owner || g.term == 0 || IsZeroId(g.owner_assignment)))
    return ProtocolError("invalid available route");
  return g;
}

absl::Status WriteState(Writer& w, const RoutingState& v) {
  w.U64(v.revision);
  LAVIK_RETURN_IF_ERROR(
      WriteStateList(w, v.nodes, kMaxProjectedNodes, WriteDataEndpoint));
  LAVIK_RETURN_IF_ERROR(
      WriteStateList(w, v.groups, kMaxProjectedGroups, WriteRoute));
  return absl::OkStatus();
}
absl::StatusOr<RoutingState> ReadRoutingState(Reader& r) {
  RoutingState v;
  auto revision = r.U64();
  LAVIK_RETURN_IF_ERROR(revision);
  v.revision = std::move(*revision);
  auto nodes =
      ReadStateList<WireDataEndpoint>(r, kMaxProjectedNodes, ReadDataEndpoint);
  LAVIK_RETURN_IF_ERROR(nodes);
  v.nodes = std::move(*nodes);
  auto groups =
      ReadStateList<WireRoutingGroup>(r, kMaxProjectedGroups, ReadRoute);
  LAVIK_RETURN_IF_ERROR(groups);
  v.groups = std::move(*groups);
  return v;
}

absl::Status WriteState(Writer& w, const LocalGroupState& v) {
  w.U64(v.revision);
  w.U32(v.lease_duration_ms);
  LAVIK_RETURN_IF_ERROR(WriteStateList(w, v.groups, 1, WriteDesiredGroup));
  LAVIK_RETURN_IF_ERROR(WriteStateList(w, v.manifests, 1, WriteManifest));
  return absl::OkStatus();
}
absl::StatusOr<LocalGroupState> ReadLocalGroupState(Reader& r) {
  LocalGroupState v;
  auto revision = r.U64();
  LAVIK_RETURN_IF_ERROR(revision);
  v.revision = std::move(*revision);
  auto lease_duration_ms = r.U32();
  LAVIK_RETURN_IF_ERROR(lease_duration_ms);
  v.lease_duration_ms = std::move(*lease_duration_ms);
  auto groups = ReadStateList<WireDesiredGroup>(r, 1, ReadDesiredGroup);
  LAVIK_RETURN_IF_ERROR(groups);
  v.groups = std::move(*groups);
  auto manifests = ReadStateList<WireManifestDocument>(r, 1, ReadManifest);
  LAVIK_RETURN_IF_ERROR(manifests);
  v.manifests = std::move(*manifests);
  return v;
}

absl::Status WriteState(Writer& w, const MetaDirectoryState& v) {
  w.U64(v.revision);
  LAVIK_RETURN_IF_ERROR(
      WriteStateList(w, v.endpoints, kMaxDirectoryEntries, WriteEndpoint));
  return absl::OkStatus();
}
absl::StatusOr<MetaDirectoryState> ReadMetaDirectoryState(Reader& r) {
  MetaDirectoryState v;
  auto revision = r.U64();
  LAVIK_RETURN_IF_ERROR(revision);
  v.revision = std::move(*revision);
  auto endpoints =
      ReadStateList<WireMetaEndpoint>(r, kMaxDirectoryEntries, ReadEndpoint);
  LAVIK_RETURN_IF_ERROR(endpoints);
  v.endpoints = std::move(*endpoints);
  return v;
}

absl::Status WriteState(Writer& w, const TaskChanges& v) {
  w.U64(v.base_revision);
  w.U64(v.revision);
  LAVIK_RETURN_IF_ERROR(WriteStateList(w, v.upserts, kMaxProjectedDirectives,
                                       WriteProjectedDirective));
  LAVIK_RETURN_IF_ERROR(
      WriteStateList(w, v.removed, kMaxProjectedDirectives,
                     [](Writer& out, const WireDirectiveIdentity& id) {
                       WriteDirectiveIdentity(out, id);
                       return absl::OkStatus();
                     }));
  return absl::OkStatus();
}
absl::StatusOr<TaskChanges> ReadTaskChanges(Reader& r) {
  TaskChanges v;
  auto base_revision = r.U64();
  LAVIK_RETURN_IF_ERROR(base_revision);
  v.base_revision = std::move(*base_revision);
  auto revision = r.U64();
  LAVIK_RETURN_IF_ERROR(revision);
  v.revision = std::move(*revision);
  auto upserts = ReadStateList<WireProjectedDirective>(
      r, kMaxProjectedDirectives, ReadProjectedDirective);
  LAVIK_RETURN_IF_ERROR(upserts);
  v.upserts = std::move(*upserts);
  auto removed = ReadStateList<WireDirectiveIdentity>(
      r, kMaxProjectedDirectives, ReadDirectiveIdentity);
  LAVIK_RETURN_IF_ERROR(removed);
  v.removed = std::move(*removed);
  return v;
}

absl::Status WriteFullDesiredStateBody(Writer& writer,
                                       const FullDesiredState& state) {
  if (state.authority_lease_duration_ms == 0) {
    return ProtocolError("FullDesiredState lease duration is invalid");
  }
  writer.U16(kProtocolVersion);
  WriteService(writer, state.service);
  writer.U64(state.control_revision);
  writer.U64(state.topology_epoch);
  writer.U32(state.authority_lease_duration_ms);

  LAVIK_RETURN_IF_ERROR(WriteCount(writer, state.meta_directory.size(),
                                   kMaxDirectoryEntries, "Meta directory"));
  for (const WireMetaEndpoint& endpoint : state.meta_directory) {
    LAVIK_RETURN_IF_ERROR(WriteEndpoint(writer, endpoint));
  }

  LAVIK_RETURN_IF_ERROR(WriteCount(writer, state.nodes.size(),
                                   kMaxProjectedNodes, "projected nodes"));
  for (const WireDataEndpoint& endpoint : state.nodes) {
    LAVIK_RETURN_IF_ERROR(WriteDataEndpoint(writer, endpoint));
  }

  LAVIK_RETURN_IF_ERROR(WriteCount(writer, state.groups.size(),
                                   kMaxProjectedGroups, "projected groups"));
  for (const WireDesiredGroup& group : state.groups) {
    LAVIK_RETURN_IF_ERROR(WriteDesiredGroup(writer, group));
  }

  LAVIK_RETURN_IF_ERROR(WriteCount(writer, state.manifests.size(),
                                   kMaxProjectedGroups, "manifests"));
  for (const WireManifestDocument& manifest : state.manifests) {
    LAVIK_RETURN_IF_ERROR(WriteManifest(writer, manifest));
  }

  LAVIK_RETURN_IF_ERROR(WriteCount(writer, state.current_directives.size(),
                                   kMaxProjectedDirectives,
                                   "current directives"));
  for (const WireProjectedDirective& directive : state.current_directives) {
    LAVIK_RETURN_IF_ERROR(WriteProjectedDirective(writer, directive));
  }
  if (writer.size() > kMaxFullDesiredStateBytes) {
    return ResourceLimit("FullDesiredState exceeds 512 MiB");
  }
  return absl::OkStatus();
}

absl::Status ValidateFullDesiredStateProjectionBasis(
    const FullDesiredState& state) {
  if (state.authority_lease_duration_ms == 0) {
    return ProtocolError("FullDesiredState lease duration is invalid");
  }
  if (state.control_revision == 0) {
    return ProtocolError("bootstrap control revision is zero");
  }
  return absl::OkStatus();
}

}  // namespace

absl::Status ValidateFullDesiredState(const FullDesiredState& state) {
  LAVIK_RETURN_IF_ERROR(ValidateFullDesiredStateProjectionBasis(state));
  Writer counter(/*retain_bytes=*/false);
  return WriteFullDesiredStateBody(counter, state);
}

absl::StatusOr<std::string> EncodeFullDesiredState(
    const FullDesiredState& state) {
  LAVIK_RETURN_IF_ERROR(ValidateFullDesiredStateProjectionBasis(state));

  Writer writer;
  LAVIK_RETURN_IF_ERROR(WriteFullDesiredStateBody(writer, state));
  std::string encoded = std::move(writer).Take();
  return encoded;
}

absl::StatusOr<FullDesiredState> DecodeFullDesiredState(
    std::string_view encoded) {
  if (encoded.size() > kMaxFullDesiredStateBytes) {
    return ResourceLimit("FullDesiredState exceeds 512 MiB");
  }
  Reader reader(encoded);
  FullDesiredState state;
  auto version = reader.U16();
  LAVIK_RETURN_IF_ERROR(version);
  if (*version != kProtocolVersion) {
    return ProtocolError("unsupported FullDesiredState version");
  }
  auto service = ReadService(reader);
  LAVIK_RETURN_IF_ERROR(service);
  state.service = *service;
  LAVIK_ASSIGN_OR_RETURN(state.control_revision, reader.U64());
  auto topology_epoch = reader.U64();
  LAVIK_RETURN_IF_ERROR(topology_epoch);
  state.topology_epoch = *topology_epoch;
  auto authority_lease_duration_ms = reader.U32();
  LAVIK_RETURN_IF_ERROR(authority_lease_duration_ms);
  state.authority_lease_duration_ms = *authority_lease_duration_ms;

  auto directory_count =
      ReadCount(reader, kMaxDirectoryEntries, "Meta directory");
  LAVIK_RETURN_IF_ERROR(directory_count);
  state.meta_directory.reserve(*directory_count);
  for (std::uint32_t i = 0; i < *directory_count; ++i) {
    auto endpoint = ReadEndpoint(reader);
    LAVIK_RETURN_IF_ERROR(endpoint);
    state.meta_directory.push_back(std::move(*endpoint));
  }

  auto node_count = ReadCount(reader, kMaxProjectedNodes, "projected nodes");
  LAVIK_RETURN_IF_ERROR(node_count);
  state.nodes.reserve(*node_count);
  for (std::uint32_t i = 0; i < *node_count; ++i) {
    auto endpoint = ReadDataEndpoint(reader);
    LAVIK_RETURN_IF_ERROR(endpoint);
    state.nodes.push_back(std::move(*endpoint));
  }

  auto group_count = ReadCount(reader, kMaxProjectedGroups, "projected groups");
  LAVIK_RETURN_IF_ERROR(group_count);
  state.groups.reserve(*group_count);
  for (std::uint32_t i = 0; i < *group_count; ++i) {
    auto group = ReadDesiredGroup(reader);
    LAVIK_RETURN_IF_ERROR(group);
    state.groups.push_back(std::move(*group));
  }

  auto manifest_count = ReadCount(reader, kMaxProjectedGroups, "manifests");
  LAVIK_RETURN_IF_ERROR(manifest_count);
  state.manifests.reserve(*manifest_count);
  for (std::uint32_t i = 0; i < *manifest_count; ++i) {
    auto manifest = ReadManifest(reader);
    LAVIK_RETURN_IF_ERROR(manifest);
    state.manifests.push_back(std::move(*manifest));
  }

  auto directive_count =
      ReadCount(reader, kMaxProjectedDirectives, "current directives");
  LAVIK_RETURN_IF_ERROR(directive_count);
  state.current_directives.reserve(*directive_count);
  for (std::uint32_t i = 0; i < *directive_count; ++i) {
    auto directive = ReadProjectedDirective(reader);
    LAVIK_RETURN_IF_ERROR(directive);
    state.current_directives.push_back(std::move(*directive));
  }
  LAVIK_RETURN_IF_ERROR(Finish(reader));
  LAVIK_RETURN_IF_ERROR(ValidateFullDesiredStateProjectionBasis(state));
  return state;
}

absl::StatusOr<FullDesiredState> DecodeFullDesiredState(std::string&& encoded) {
  auto decoded = DecodeFullDesiredState(std::string_view(encoded));
  // A transferred FDS is already duplicated by its decoded owning strings.
  // Release the contiguous wire allocation before returning it to callers so
  // installation cannot retain both complete representations across awaits.
  std::string released;
  encoded.swap(released);
  return decoded;
}

bool SameDesiredState(const FullDesiredState& left,
                      const FullDesiredState& right) {
  if (left.service != right.service ||
      left.topology_epoch != right.topology_epoch ||
      left.authority_lease_duration_ms != right.authority_lease_duration_ms ||
      left.meta_directory != right.meta_directory ||
      left.nodes != right.nodes || left.groups != right.groups ||
      left.manifests != right.manifests ||
      left.current_directives.size() != right.current_directives.size()) {
    return false;
  }
  return left.current_directives == right.current_directives;
}

absl::StatusOr<std::string> EncodeNodeControlUpdate(
    const NodeControlUpdate& update) {
  Writer writer;
  WriteService(writer, update.service);
  writer.Fixed(update.request_id);
  writer.Bool(update.tasks.has_value());
  if (update.tasks) {
    LAVIK_RETURN_IF_ERROR(WriteState(writer, *update.tasks));
  }

  writer.Bool(update.routing.has_value());
  if (update.routing) {
    LAVIK_RETURN_IF_ERROR(WriteState(writer, *update.routing));
  }
  writer.Bool(update.local.has_value());
  if (update.local) {
    LAVIK_RETURN_IF_ERROR(WriteState(writer, *update.local));
  }
  writer.Bool(update.directory.has_value());
  if (update.directory) {
    LAVIK_RETURN_IF_ERROR(WriteState(writer, *update.directory));
  }
  if (writer.size() > kMaxFullDesiredStateBytes)
    return ResourceLimit("node update exceeds its cap");
  return std::move(writer).Take();
}

absl::StatusOr<NodeControlUpdate> DecodeNodeControlUpdate(
    std::string_view bytes) {
  if (bytes.size() > kMaxFullDesiredStateBytes)
    return ResourceLimit("node update exceeds its cap");
  Reader reader(bytes);
  NodeControlUpdate update;
  auto service = ReadService(reader);
  LAVIK_RETURN_IF_ERROR(service);
  update.service = *service;
  LAVIK_ASSIGN_OR_RETURN(update.request_id, reader.Fixed<16>());
  auto has_tasks = reader.Bool();
  LAVIK_RETURN_IF_ERROR(has_tasks);
  if (*has_tasks) {
    LAVIK_ASSIGN_OR_RETURN(update.tasks, ReadTaskChanges(reader));
  }
  auto has_routing = reader.Bool();
  LAVIK_RETURN_IF_ERROR(has_routing);
  if (*has_routing) {
    LAVIK_ASSIGN_OR_RETURN(update.routing, ReadRoutingState(reader));
  }
  auto has_local = reader.Bool();
  LAVIK_RETURN_IF_ERROR(has_local);
  if (*has_local) {
    LAVIK_ASSIGN_OR_RETURN(update.local, ReadLocalGroupState(reader));
  }
  auto has_directory = reader.Bool();
  LAVIK_RETURN_IF_ERROR(has_directory);
  if (*has_directory) {
    LAVIK_ASSIGN_OR_RETURN(update.directory, ReadMetaDirectoryState(reader));
  }
  LAVIK_RETURN_IF_ERROR(reader.Finish());
  return update;
}

NodeControlState SelectNodeControlState(const FullDesiredState& source,
                                        std::string_view node_id) {
  NodeControlState state;
  state.service = source.service;
  state.routing.revision = source.control_revision;
  state.routing.nodes = source.nodes;
  state.local.revision = source.control_revision;
  state.local.lease_duration_ms = source.authority_lease_duration_ms;
  state.directory = {1, source.meta_directory};
  state.tasks_revision = 1;
  for (const auto& group : source.groups) {
    WireRoutingGroup route{
        .group_id = group.group_id,
        .term = group.group_term,
        .owner = group.owner_node_id,
        .owner_assignment = group.owner_assignment_id.value_or(WireId128{}),
        .available = group.grant_active,
        .members = {},
        .slots = group.slot_ranges};
    bool local = false;
    for (const auto& member : group.members) {
      route.members.push_back(member.node_id);
      local |= member.node_id == node_id;
    }
    state.routing.groups.push_back(std::move(route));
    if (!local) continue;
    state.local.groups.push_back(group);
    for (const auto& manifest : source.manifests) {
      if (manifest.revision == group.manifest_revision &&
          manifest.digest == group.manifest_digest) {
        state.local.manifests.push_back(manifest);
      }
    }
  }
  for (const auto& task : source.current_directives) {
    if (task.recipient_node_id == node_id) state.tasks.push_back(task);
  }
  return state;
}

NodeControlUpdate DiffNodeControlState(const NodeControlState& previous,
                                       NodeControlState& next) {
  NodeControlUpdate update;
  update.service = next.service;
  const auto diff = [](const auto& old_value, auto& new_value, auto& change) {
    new_value.revision = old_value.revision;
    if (old_value != new_value) {
      ++new_value.revision;
      change = new_value;
    }
  };
  diff(previous.routing, next.routing, update.routing);
  diff(previous.local, next.local, update.local);
  // A local member's endpoint is execution input as well as discovery data.
  // Reconcile it under the local revision even when membership is unchanged.
  if (!update.local && previous.routing.nodes != next.routing.nodes) {
    bool endpoint_changed = false;
    for (const auto& group : next.local.groups) {
      for (const auto& member : group.members) {
        const auto endpoint = [&](const RoutingState& routes) {
          return std::find_if(
              routes.nodes.begin(), routes.nodes.end(),
              [&](const auto& node) { return node.node_id == member.node_id; });
        };
        const auto before = endpoint(previous.routing);
        const auto after = endpoint(next.routing);
        endpoint_changed |= before == previous.routing.nodes.end() ||
                            after == next.routing.nodes.end() ||
                            *before != *after;
      }
    }
    if (endpoint_changed) {
      ++next.local.revision;
      update.local = next.local;
    }
  }
  diff(previous.directory, next.directory, update.directory);
  next.tasks_revision = previous.tasks_revision;
  TaskChanges tasks{.base_revision = previous.tasks_revision,
                    .revision = previous.tasks_revision + 1,
                    .upserts = {},
                    .removed = {}};
  for (const auto& task : next.tasks) {
    if (std::find(previous.tasks.begin(), previous.tasks.end(), task) ==
        previous.tasks.end()) {
      tasks.upserts.push_back(task);
    }
  }
  for (const auto& task : previous.tasks) {
    if (std::none_of(next.tasks.begin(), next.tasks.end(),
                     [&](const auto& current) {
                       return current.identity == task.identity;
                     }))
      tasks.removed.push_back(task.identity);
  }
  if (!tasks.upserts.empty() || !tasks.removed.empty()) {
    next.tasks_revision = tasks.revision;
    update.tasks = std::move(tasks);
  }
  return update;
}

absl::Status ApplyNodeControlUpdate(NodeControlState& state,
                                    const NodeControlUpdate& update) {
  if (state.service != update.service) {
    return absl::FailedPreconditionError("cluster service declaration changed");
  }
  // Validate the entire request before publishing any module. A gap in the
  // task delta requires reconnect/bootstrap; complete objects can skip
  // revisions.
  const auto validate = [](const auto& current, const auto& change) {
    if (change &&
        (change->revision == 0 ||
         (change->revision == current.revision && *change != current))) {
      return absl::FailedPreconditionError(
          "conflicting control object revision");
    }
    return absl::OkStatus();
  };
  for (const auto& status : {validate(state.routing, update.routing),
                             validate(state.local, update.local),
                             validate(state.directory, update.directory)}) {
    LAVIK_RETURN_IF_ERROR(status);
  }
  std::optional<std::vector<WireProjectedDirective>> tasks;
  if (update.tasks && update.tasks->revision >= state.tasks_revision) {
    const auto& delta = *update.tasks;
    if (delta.revision == 0 || delta.revision <= delta.base_revision) {
      return absl::FailedPreconditionError("invalid task revision");
    }
    if (delta.revision == state.tasks_revision) {
      for (const auto& task : delta.upserts) {
        if (std::find(state.tasks.begin(), state.tasks.end(), task) ==
            state.tasks.end())
          return absl::FailedPreconditionError("conflicting task replay");
      }
      for (const auto& id : delta.removed) {
        if (std::any_of(state.tasks.begin(), state.tasks.end(),
                        [&](const auto& task) { return task.identity == id; }))
          return absl::FailedPreconditionError(
              "conflicting task removal replay");
      }
    } else {
      if (delta.base_revision != state.tasks_revision)
        return absl::FailedPreconditionError("task delta has a revision gap");
      tasks = state.tasks;
      for (const auto& id : delta.removed) {
        std::erase_if(*tasks,
                      [&](const auto& task) { return task.identity == id; });
      }
      for (const auto& task : delta.upserts) {
        auto existing = std::find_if(
            tasks->begin(), tasks->end(), [&](const auto& candidate) {
              return task.identity == candidate.identity;
            });
        if (existing != tasks->end()) {
          if (*existing != task)
            return absl::FailedPreconditionError(
                "task identity reused for different work");
        } else {
          tasks->push_back(task);
        }
      }
      if (tasks->size() > kMaxProjectedDirectives)
        return absl::ResourceExhaustedError("too many active tasks");
    }
  }
  const auto apply = [](auto& current, const auto& change) {
    if (change && change->revision > current.revision) current = *change;
  };
  apply(state.routing, update.routing);
  apply(state.local, update.local);
  apply(state.directory, update.directory);
  if (tasks) {
    state.tasks = std::move(*tasks);
    state.tasks_revision = update.tasks->revision;
  }
  return absl::OkStatus();
}

}  // namespace lavik::cluster::control
