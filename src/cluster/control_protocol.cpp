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
    if (!raw.ok()) return raw.status();
    return static_cast<std::uint8_t>((*raw)[0]);
  }

  absl::StatusOr<std::uint16_t> U16() {
    auto raw = Raw(2);
    if (!raw.ok()) return raw.status();
    const auto* p = reinterpret_cast<const unsigned char*>(raw->data());
    return static_cast<std::uint16_t>((static_cast<std::uint16_t>(p[0]) << 8) |
                                      p[1]);
  }

  absl::StatusOr<std::uint32_t> U32() {
    auto raw = Raw(4);
    if (!raw.ok()) return raw.status();
    const auto* p = reinterpret_cast<const unsigned char*>(raw->data());
    std::uint32_t value = 0;
    for (unsigned i = 0; i < 4; ++i) value = (value << 8) | p[i];
    return value;
  }

  absl::StatusOr<std::uint64_t> U64() {
    auto raw = Raw(8);
    if (!raw.ok()) return raw.status();
    const auto* p = reinterpret_cast<const unsigned char*>(raw->data());
    std::uint64_t value = 0;
    for (unsigned i = 0; i < 8; ++i) value = (value << 8) | p[i];
    return value;
  }

  absl::StatusOr<bool> Bool() {
    auto tag = U8();
    if (!tag.ok()) return tag.status();
    if (*tag > 1) return ProtocolError("boolean tag must be 0 or 1");
    return *tag == 1;
  }

  template <std::size_t N>
  absl::StatusOr<std::array<std::uint8_t, N>> Fixed() {
    auto raw = Raw(N);
    if (!raw.ok()) return raw.status();
    std::array<std::uint8_t, N> value{};
    std::memcpy(value.data(), raw->data(), N);
    return value;
  }

  absl::StatusOr<std::string> String(std::size_t cap) {
    auto length = U32();
    if (!length.ok()) return length.status();
    // Check the declared length before checking whether the body is present.
    // Over-limit input is never partially accepted as a truncation case.
    if (*length > cap) return ResourceLimit("message field exceeds its cap");
    auto raw = Raw(*length);
    if (!raw.ok()) return raw.status();
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
         raw <= static_cast<std::uint16_t>(MessageType::kNodeControlUpdate);
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
  if (!encoded_magic.ok()) return encoded_magic.status();
  if (*encoded_magic != magic)
    return ProtocolError("unknown directive body schema");
  auto version = reader.U16();
  if (!version.ok()) return version.status();
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
  if (absl::Status status = ValidateIdentity(value, field); !status.ok()) {
    return status;
  }
  writer.Raw(value);
  return absl::OkStatus();
}

absl::StatusOr<std::string> ReadIdentity(Reader& reader,
                                         std::string_view field) {
  auto raw = reader.Raw(40);
  if (!raw.ok()) return raw.status();
  if (absl::Status status = ValidateIdentity(*raw, field); !status.ok()) {
    return status;
  }
  return std::string(*raw);
}

void WriteProjectionBasis(Writer& writer, const WireProjectionBasis& basis) {
  writer.U64(basis.control_revision);
}

absl::StatusOr<WireProjectionBasis> ReadProjectionBasis(Reader& reader) {
  WireProjectionBasis basis;
  auto index = reader.U64();
  if (!index.ok()) return index.status();
  basis.control_revision = *index;
  return basis;
}

absl::Status WriteAuthorityAnchor(Writer& writer,
                                  const WireAuthorityAnchor& anchor) {
  if (absl::Status status =
          writer.String(anchor.group_id, kMaxIdentifierBytes, "group id");
      !status.ok()) {
    return status;
  }
  writer.Fixed(anchor.assignment_id);
  writer.U64(anchor.group_term);
  return absl::OkStatus();
}

absl::StatusOr<WireAuthorityAnchor> ReadAuthorityAnchor(Reader& reader) {
  WireAuthorityAnchor anchor;
  auto group_id = reader.String(kMaxIdentifierBytes);
  if (!group_id.ok()) return group_id.status();
  anchor.group_id = std::move(*group_id);
  auto assignment_id = reader.Fixed<16>();
  if (!assignment_id.ok()) return assignment_id.status();
  anchor.assignment_id = *assignment_id;
  auto term = reader.U64();
  if (!term.ok()) return term.status();
  anchor.group_term = *term;
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
  auto operation = reader.Fixed<16>();
  if (!operation.ok()) return operation.status();
  identity.operation_id = *operation;
  auto directive = reader.Fixed<16>();
  if (!directive.ok()) return directive.status();
  identity.directive_id = *directive;
  auto attempt = reader.Fixed<16>();
  if (!attempt.ok()) return attempt.status();
  identity.attempt_id = *attempt;
  auto revision = reader.U64();
  if (!revision.ok()) return revision.status();
  identity.directive_revision = *revision;
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
  if (absl::Status status = FillRandom(bytes.data(), bytes.size());
      !status.ok()) {
    return status;
  }
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
    if (absl::Status status = FillRandom(id.data(), id.size()); !status.ok()) {
      return status;
    }
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
  if (absl::Status status = WriteSchemaHeader(writer, kRebuildRequestMagic);
      !status.ok()) {
    return status;
  }
  writer.U32(request.source_flow_count);
  return std::move(writer).Take();
}

absl::StatusOr<RebuildRequest> DecodeRebuildRequest(std::string_view encoded) {
  Reader reader(encoded);
  if (absl::Status status = ReadSchemaHeader(reader, kRebuildRequestMagic);
      !status.ok()) {
    return status;
  }
  auto count = reader.U32();
  if (!count.ok()) return count.status();
  if (*count == 0 || *count > kMaxCandidateFlows) {
    return ProtocolError("invalid rebuild source flow count");
  }
  if (absl::Status status = Finish(reader); !status.ok()) return status;
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
  if (!cap.ok()) return cap.status();
  if (start.total_length > *cap) {
    return ResourceLimit("large object exceeds its type-specific cap");
  }
  if (absl::Status status = impl_->sink.Begin(start); !status.ok()) {
    return status;
  }
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
  if (absl::Status status = ValidateIdentity(data_boot_id, "data boot id");
      !status.ok()) {
    return status;
  }
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
  if (absl::Status status =
          writer.String(endpoint.host, kMaxIdentifierBytes, "endpoint host");
      !status.ok()) {
    return status;
  }
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
  auto server_id = reader.U32();
  if (!server_id.ok()) return server_id.status();
  endpoint.server_id = *server_id;
  auto host = reader.String(kMaxIdentifierBytes);
  if (!host.ok()) return host.status();
  endpoint.host = std::move(*host);
  auto port = reader.U16();
  if (!port.ok()) return port.status();
  endpoint.port = *port;
  auto has_principal = reader.Bool();
  if (!has_principal.ok()) return has_principal.status();
  if (*has_principal) {
    auto principal = reader.String(kMaxIdentifierBytes);
    if (!principal.ok()) return principal.status();
    endpoint.principal = std::move(*principal);
  }
  return endpoint;
}

absl::Status WriteLeaseChallenge(Writer& writer,
                                 const LeaseChallenge& challenge) {
  writer.Fixed(challenge.nonce);
  writer.U64(challenge.control_revision);
  if (absl::Status status = writer.String(
          challenge.group_id, kMaxIdentifierBytes, "challenge group id");
      !status.ok()) {
    return status;
  }
  writer.Fixed(challenge.assignment_id);
  writer.U64(challenge.group_term);
  return absl::OkStatus();
}

absl::StatusOr<LeaseChallenge> ReadLeaseChallenge(Reader& reader) {
  LeaseChallenge challenge;
  auto nonce = reader.Fixed<16>();
  if (!nonce.ok()) return nonce.status();
  challenge.nonce = *nonce;
  auto control_revision = reader.U64();
  if (!control_revision.ok()) return control_revision.status();
  challenge.control_revision = *control_revision;
  auto group_id = reader.String(kMaxIdentifierBytes);
  if (!group_id.ok()) return group_id.status();
  challenge.group_id = std::move(*group_id);
  auto assignment_id = reader.Fixed<16>();
  if (!assignment_id.ok()) return assignment_id.status();
  challenge.assignment_id = *assignment_id;
  auto term = reader.U64();
  if (!term.ok()) return term.status();
  challenge.group_term = *term;
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
  if (!count.ok()) return count.status();
  if (*count == 0 || *count > kMaxCandidateFlows) {
    return ResourceLimit(std::string(field) +
                         " count is outside its protocol cap");
  }
  std::vector<std::uint64_t> next_lsns;
  next_lsns.reserve(*count);
  for (std::uint16_t flow = 0; flow < *count; ++flow) {
    auto next_lsn = reader.U64();
    if (!next_lsn.ok()) return next_lsn.status();
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
    if (absl::Status status = WriteIdentity(writer, paused->source_node_id,
                                            "paused source node id");
        !status.ok()) {
      return status;
    }
    writer.Fixed(paused->source_assignment_id);
    if (absl::Status status = WriteIdentity(writer, paused->source_boot_id,
                                            "paused source boot id");
        !status.ok()) {
      return status;
    }
    if (absl::Status status = WriteIdentity(writer, paused->source_history_id,
                                            "paused source history id");
        !status.ok()) {
      return status;
    }
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
    if (absl::Status status = WriteIdentity(writer, prepared->candidate_node_id,
                                            "prepared candidate node id");
        !status.ok()) {
      return status;
    }
    writer.Fixed(prepared->candidate_assignment_id);
    if (absl::Status status = WriteIdentity(writer, prepared->candidate_boot_id,
                                            "prepared candidate boot id");
        !status.ok()) {
      return status;
    }
    writer.Fixed(prepared->prepared_context_id);
    return absl::OkStatus();
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
  if (absl::Status status = WriteIdentity(writer, failed.candidate_node_id,
                                          "failed candidate node id");
      !status.ok()) {
    return status;
  }
  writer.Fixed(failed.candidate_assignment_id);
  if (absl::Status status = WriteIdentity(writer, failed.candidate_boot_id,
                                          "failed candidate boot id");
      !status.ok()) {
    return status;
  }
  writer.U64(failed.population_manifest_revision);
  writer.Fixed(failed.population_manifest_digest);
  writer.U64(failed.partition_replication_epoch);
  if (absl::Status status =
          writer.String(failed.failure_class, kMaxFailoverFailureClassBytes,
                        "failover failure class");
      !status.ok()) {
    return status;
  }
  return writer.String(failed.failure_detail, kMaxFailoverFailureDetailBytes,
                       "failover failure detail");
}

absl::StatusOr<FailoverObservation> ReadFailoverObservation(Reader& reader) {
  auto kind = reader.U8();
  if (!kind.ok()) return kind.status();
  if (*kind ==
      static_cast<std::uint8_t>(FailoverObservationKind::kSourcePaused)) {
    SourcePaused paused;
    auto transition_id = reader.Fixed<16>();
    if (!transition_id.ok()) return transition_id.status();
    paused.transition_id = *transition_id;
    auto source_node_id = ReadIdentity(reader, "paused source node id");
    if (!source_node_id.ok()) return source_node_id.status();
    paused.source_node_id = std::move(*source_node_id);
    auto assignment_id = reader.Fixed<16>();
    if (!assignment_id.ok()) return assignment_id.status();
    paused.source_assignment_id = *assignment_id;
    auto boot_id = ReadIdentity(reader, "paused source boot id");
    if (!boot_id.ok()) return boot_id.status();
    paused.source_boot_id = std::move(*boot_id);
    auto history_id = ReadIdentity(reader, "paused source history id");
    if (!history_id.ok()) return history_id.status();
    paused.source_history_id = std::move(*history_id);
    auto source_group_term = reader.U64();
    if (!source_group_term.ok()) return source_group_term.status();
    paused.source_group_term = *source_group_term;
    auto frontier = ReadHeartbeatFlowVector(reader, "paused stable frontier");
    if (!frontier.ok()) return frontier.status();
    paused.stable_next_lsns = std::move(*frontier);
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
    if (!transition_id.ok()) return transition_id.status();
    prepared.transition_id = *transition_id;
    auto action_id = reader.Fixed<16>();
    if (!action_id.ok()) return action_id.status();
    prepared.action_id = *action_id;
    auto node_id = ReadIdentity(reader, "prepared candidate node id");
    if (!node_id.ok()) return node_id.status();
    prepared.candidate_node_id = std::move(*node_id);
    auto assignment_id = reader.Fixed<16>();
    if (!assignment_id.ok()) return assignment_id.status();
    prepared.candidate_assignment_id = *assignment_id;
    auto boot_id = ReadIdentity(reader, "prepared candidate boot id");
    if (!boot_id.ok()) return boot_id.status();
    prepared.candidate_boot_id = std::move(*boot_id);
    auto context_id = reader.Fixed<16>();
    if (!context_id.ok()) return context_id.status();
    prepared.prepared_context_id = *context_id;
    if (IsZeroId(prepared.transition_id) || IsZeroId(prepared.action_id) ||
        IsZeroId(prepared.candidate_assignment_id) ||
        IsZeroId(prepared.prepared_context_id)) {
      return ProtocolError(
          "candidate-prepared observation has an empty anchor");
    }
    return FailoverObservation{std::move(prepared)};
  }

  if (*kind ==
      static_cast<std::uint8_t>(FailoverObservationKind::kActionFailed)) {
    ActionFailed failed;
    auto transition_id = reader.Fixed<16>();
    if (!transition_id.ok()) return transition_id.status();
    failed.transition_id = *transition_id;
    auto action_id = reader.Fixed<16>();
    if (!action_id.ok()) return action_id.status();
    failed.action_id = *action_id;
    auto node_id = ReadIdentity(reader, "failed candidate node id");
    if (!node_id.ok()) return node_id.status();
    failed.candidate_node_id = std::move(*node_id);
    auto assignment_id = reader.Fixed<16>();
    if (!assignment_id.ok()) return assignment_id.status();
    failed.candidate_assignment_id = *assignment_id;
    auto boot_id = ReadIdentity(reader, "failed candidate boot id");
    if (!boot_id.ok()) return boot_id.status();
    failed.candidate_boot_id = std::move(*boot_id);
    auto manifest_revision = reader.U64();
    if (!manifest_revision.ok()) return manifest_revision.status();
    failed.population_manifest_revision = *manifest_revision;
    auto manifest_digest = reader.Fixed<32>();
    if (!manifest_digest.ok()) return manifest_digest.status();
    failed.population_manifest_digest = *manifest_digest;
    auto replication_epoch = reader.U64();
    if (!replication_epoch.ok()) return replication_epoch.status();
    failed.partition_replication_epoch = *replication_epoch;
    auto failure_class = reader.String(kMaxFailoverFailureClassBytes);
    if (!failure_class.ok()) return failure_class.status();
    failed.failure_class = std::move(*failure_class);
    auto failure_detail = reader.String(kMaxFailoverFailureDetailBytes);
    if (!failure_detail.ok()) return failure_detail.status();
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
  writer.U64(grant.leadership_generation);
  if (absl::Status status =
          WriteIdentity(writer, grant.data_boot_id, "data boot id");
      !status.ok()) {
    return status;
  }
  writer.U64(grant.control_revision);
  if (absl::Status status =
          writer.String(grant.group_id, kMaxIdentifierBytes, "grant group id");
      !status.ok()) {
    return status;
  }
  writer.Fixed(grant.assignment_id);
  writer.U64(grant.group_term);
  writer.U32(grant.granted_duration_ms);
  return absl::OkStatus();
}

absl::StatusOr<LeaseGranted> ReadLeaseGranted(Reader& reader) {
  LeaseGranted grant;
  auto nonce = reader.Fixed<16>();
  if (!nonce.ok()) return nonce.status();
  grant.nonce = *nonce;
  auto leader_id = reader.U32();
  if (!leader_id.ok()) return leader_id.status();
  grant.leader_id = *leader_id;
  auto raft_term = reader.U64();
  if (!raft_term.ok()) return raft_term.status();
  grant.raft_term = *raft_term;
  auto generation = reader.U64();
  if (!generation.ok()) return generation.status();
  grant.leadership_generation = *generation;
  auto boot_id = ReadIdentity(reader, "data boot id");
  if (!boot_id.ok()) return boot_id.status();
  grant.data_boot_id = std::move(*boot_id);
  auto control_revision = reader.U64();
  if (!control_revision.ok()) return control_revision.status();
  grant.control_revision = *control_revision;
  auto group_id = reader.String(kMaxIdentifierBytes);
  if (!group_id.ok()) return group_id.status();
  grant.group_id = std::move(*group_id);
  auto assignment_id = reader.Fixed<16>();
  if (!assignment_id.ok()) return assignment_id.status();
  grant.assignment_id = *assignment_id;
  auto term = reader.U64();
  if (!term.ok()) return term.status();
  grant.group_term = *term;
  auto duration = reader.U32();
  if (!duration.ok()) return duration.status();
  grant.granted_duration_ms = *duration;
  return grant;
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
  if (absl::Status status = WriteIdentity(writer, hello.node_id, "node id");
      !status.ok()) {
    return status;
  }
  if (absl::Status status = WriteIdentity(writer, hello.boot_id, "boot id");
      !status.ok()) {
    return status;
  }
  if (absl::Status status = WriteIdentity(writer, hello.replication_history_id,
                                          "replication history id");
      !status.ok()) {
    return status;
  }
  writer.U32(hello.replication_flow_count);
  return std::move(writer).Take();
}

absl::StatusOr<WireMessage> DecodeClientHello(std::string_view bytes) {
  Reader reader(bytes);
  ClientHello hello;
  auto minimum = reader.U16();
  if (!minimum.ok()) return minimum.status();
  hello.minimum_version = *minimum;
  auto maximum = reader.U16();
  if (!maximum.ok()) return maximum.status();
  hello.maximum_version = *maximum;
  if (hello.minimum_version == 0 ||
      hello.minimum_version > hello.maximum_version) {
    return ProtocolError("invalid ClientHello version range");
  }
  auto node_id = ReadIdentity(reader, "node id");
  if (!node_id.ok()) return node_id.status();
  hello.node_id = std::move(*node_id);
  auto boot_id = ReadIdentity(reader, "boot id");
  if (!boot_id.ok()) return boot_id.status();
  hello.boot_id = std::move(*boot_id);
  auto history_id = ReadIdentity(reader, "replication history id");
  if (!history_id.ok()) return history_id.status();
  hello.replication_history_id = std::move(*history_id);
  auto count = reader.U32();
  if (!count.ok()) return count.status();
  if (*count == 0 || *count > kMaxCandidateFlows) {
    return ProtocolError("invalid ClientHello replication flow count");
  }
  hello.replication_flow_count = *count;
  if (absl::Status status = Finish(reader); !status.ok()) return status;
  return WireMessage{std::move(hello)};
}

absl::StatusOr<std::string> Encode(const ServerHello& hello) {
  const auto disposition = static_cast<std::uint8_t>(hello.disposition);
  if (disposition < 1 || disposition > 3) {
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
    if (absl::Status status = WriteEndpoint(writer, endpoint); !status.ok()) {
      return status;
    }
  }
  writer.U32(hello.observation_ttl_ms);
  writer.U32(hello.session_progress_timeout_ms);
  return std::move(writer).Take();
}

absl::StatusOr<WireMessage> DecodeServerHello(std::string_view bytes) {
  Reader reader(bytes);
  ServerHello hello;
  auto disposition = reader.U8();
  if (!disposition.ok()) return disposition.status();
  if (*disposition < 1 || *disposition > 3) {
    return ProtocolError("unknown ServerHello disposition");
  }
  hello.disposition = static_cast<ServerHelloDisposition>(*disposition);
  auto version = reader.U16();
  if (!version.ok()) return version.status();
  hello.negotiated_version = *version;
  auto server_id = reader.U32();
  if (!server_id.ok()) return server_id.status();
  hello.meta_server_id = *server_id;
  auto raft_term = reader.U64();
  if (!raft_term.ok()) return raft_term.status();
  hello.raft_term = *raft_term;
  auto session_id = reader.Fixed<16>();
  if (!session_id.ok()) return session_id.status();
  hello.session_id = *session_id;
  auto generation = reader.U64();
  if (!generation.ok()) return generation.status();
  hello.session_generation = *generation;
  auto has_leader = reader.Bool();
  if (!has_leader.ok()) return has_leader.status();
  if (*has_leader) {
    auto leader_id = reader.U32();
    if (!leader_id.ok()) return leader_id.status();
    hello.leader_id = *leader_id;
  }
  auto directory_count = reader.U32();
  if (!directory_count.ok()) return directory_count.status();
  if (*directory_count > kMaxDirectoryEntries) {
    return ResourceLimit("Meta directory exceeds its entry cap");
  }
  hello.directory.reserve(*directory_count);
  for (std::uint32_t i = 0; i < *directory_count; ++i) {
    auto endpoint = ReadEndpoint(reader);
    if (!endpoint.ok()) return endpoint.status();
    hello.directory.push_back(std::move(*endpoint));
  }
  auto observation_ttl = reader.U32();
  if (!observation_ttl.ok()) return observation_ttl.status();
  hello.observation_ttl_ms = *observation_ttl;
  auto progress_timeout = reader.U32();
  if (!progress_timeout.ok()) return progress_timeout.status();
  hello.session_progress_timeout_ms = *progress_timeout;
  if (absl::Status status = Finish(reader); !status.ok()) return status;
  return WireMessage{std::move(hello)};
}

absl::StatusOr<std::string> Encode(const TransferStart& start) {
  const auto kind = static_cast<std::uint16_t>(start.kind);
  if (kind < 1 || kind > 5) return ProtocolError("unknown transfer kind");
  auto cap = TransferCap(start.kind);
  if (!cap.ok()) return cap.status();
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
  if (!kind.ok()) return kind.status();
  if (*kind < 1 || *kind > 5) return ProtocolError("unknown transfer kind");
  start.kind = static_cast<TransferKind>(*kind);
  auto object_id = reader.Fixed<16>();
  if (!object_id.ok()) return object_id.status();
  start.object_id = *object_id;
  auto total_length = reader.U64();
  if (!total_length.ok()) return total_length.status();
  start.total_length = *total_length;
  auto cap = TransferCap(start.kind);
  if (!cap.ok()) return cap.status();
  if (start.total_length > *cap) {
    return ResourceLimit("large object exceeds its type-specific cap");
  }
  if (absl::Status status = Finish(reader); !status.ok()) return status;
  return WireMessage{start};
}

absl::StatusOr<std::string> Encode(const TransferChunk& chunk) {
  if (chunk.bytes.size() > kMaxTransferChunkBytes) {
    return ResourceLimit("transfer chunk cannot fit in one frame");
  }
  Writer writer;
  writer.Fixed(chunk.object_id);
  writer.U64(chunk.offset);
  if (absl::Status status =
          writer.String(chunk.bytes, kMaxTransferChunkBytes, "transfer chunk");
      !status.ok()) {
    return status;
  }
  return std::move(writer).Take();
}

absl::StatusOr<WireMessage> DecodeTransferChunk(std::string_view bytes) {
  Reader reader(bytes);
  TransferChunk chunk;
  auto object_id = reader.Fixed<16>();
  if (!object_id.ok()) return object_id.status();
  chunk.object_id = *object_id;
  auto offset = reader.U64();
  if (!offset.ok()) return offset.status();
  chunk.offset = *offset;
  auto body = reader.String(kMaxTransferChunkBytes);
  if (!body.ok()) return body.status();
  chunk.bytes = std::move(*body);
  if (absl::Status status = Finish(reader); !status.ok()) return status;
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
  if (!object_id.ok()) return object_id.status();
  if (absl::Status status = Finish(reader); !status.ok()) return status;
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
  if (!object_id.ok()) return object_id.status();
  abort.object_id = *object_id;
  auto reason = reader.U16();
  if (!reason.ok()) return reason.status();
  abort.reason = static_cast<TransferAbortReason>(*reason);
  if (absl::Status status = Finish(reader); !status.ok()) return status;
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
  auto request = reader.Fixed<16>();
  if (!request.ok()) return request.status();
  applied.request_id = *request;
  auto index = reader.U64();
  if (!index.ok()) return index.status();
  applied.control_revision = *index;
  if (absl::Status status = Finish(reader); !status.ok()) return status;
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
  if (absl::Status status = writer.String(
          heartbeat.health.summary, kMaxOpaqueFieldBytes, "health summary");
      !status.ok()) {
    return status;
  }
  if (std::holds_alternative<NoRoleInformation>(heartbeat.role_information)) {
    writer.U8(static_cast<std::uint8_t>(HeartbeatRoleKind::kNone));
  } else if (const auto* authority = std::get_if<AuthorityLeaseRequest>(
                 &heartbeat.role_information)) {
    writer.U8(
        static_cast<std::uint8_t>(HeartbeatRoleKind::kAuthorityLeaseRequest));
    if (absl::Status status = WriteLeaseChallenge(writer, authority->challenge);
        !status.ok()) {
      return status;
    }
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
    if (absl::Status status = writer.String(
            candidate.group_id, kMaxIdentifierBytes, "candidate group id");
        !status.ok()) {
      return status;
    }
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
      if (absl::Status status =
              writer.String(candidate.source_node_id, kMaxIdentifierBytes,
                            "candidate source node id");
          !status.ok()) {
        return status;
      }
      writer.Fixed(candidate.source_assignment_id);
      if (absl::Status status =
              writer.String(candidate.source_boot_id, kMaxIdentifierBytes,
                            "candidate source boot id");
          !status.ok()) {
        return status;
      }
      if (absl::Status status =
              writer.String(candidate.source_history_id, kMaxIdentifierBytes,
                            "candidate source history id");
          !status.ok()) {
        return status;
      }
      if (absl::Status status = WriteHeartbeatFlowVector(
              writer, candidate.applied_next_lsns, "candidate flow vector");
          !status.ok()) {
        return status;
      }
    }
  }
  writer.Bool(heartbeat.failover_observation.has_value());
  if (heartbeat.failover_observation.has_value()) {
    if (absl::Status status =
            WriteFailoverObservation(writer, *heartbeat.failover_observation);
        !status.ok()) {
      return status;
    }
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
  if (!session_id.ok()) return session_id.status();
  heartbeat.session_id = *session_id;
  auto sequence = reader.U64();
  if (!sequence.ok()) return sequence.status();
  heartbeat.heartbeat_sequence = *sequence;
  auto storage_ready = reader.Bool();
  if (!storage_ready.ok()) return storage_ready.status();
  heartbeat.health.storage_ready = *storage_ready;
  auto population_ready = reader.Bool();
  if (!population_ready.ok()) return population_ready.status();
  heartbeat.health.population_ready = *population_ready;
  auto draining = reader.Bool();
  if (!draining.ok()) return draining.status();
  heartbeat.health.draining = *draining;
  auto active_groups = reader.U32();
  if (!active_groups.ok()) return active_groups.status();
  heartbeat.health.active_groups = *active_groups;
  auto summary = reader.String(kMaxOpaqueFieldBytes);
  if (!summary.ok()) return summary.status();
  heartbeat.health.summary = std::move(*summary);
  auto role_kind = reader.U8();
  if (!role_kind.ok()) return role_kind.status();
  if (*role_kind ==
      static_cast<std::uint8_t>(HeartbeatRoleKind::kAuthorityLeaseRequest)) {
    auto challenge = ReadLeaseChallenge(reader);
    if (!challenge.ok()) return challenge.status();
    heartbeat.role_information =
        AuthorityLeaseRequest{.challenge = std::move(*challenge)};
  } else if (*role_kind ==
             static_cast<std::uint8_t>(HeartbeatRoleKind::kReplicaCandidate)) {
    CandidateProgress candidate;
    auto recovered = reader.Bool();
    if (!recovered.ok()) return recovered.status();
    candidate.recovered = *recovered;
    auto operator_recovery = reader.Bool();
    if (!operator_recovery.ok()) return operator_recovery.status();
    candidate.operator_recovery = *operator_recovery;
    if (candidate.operator_recovery && candidate.recovered)
      return ProtocolError("conflicting recovery kinds");
    auto group_id = reader.String(kMaxIdentifierBytes);
    if (!group_id.ok()) return group_id.status();
    candidate.group_id = std::move(*group_id);
    auto assignment_id = reader.Fixed<16>();
    if (!assignment_id.ok()) return assignment_id.status();
    candidate.assignment_id = *assignment_id;
    auto group_term = reader.U64();
    if (!group_term.ok()) return group_term.status();
    candidate.group_term = *group_term;
    auto source_group_term = reader.U64();
    if (!source_group_term.ok()) return source_group_term.status();
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
    if (!manifest_revision.ok()) return manifest_revision.status();
    candidate.manifest_revision = *manifest_revision;
    auto manifest_digest = reader.Fixed<32>();
    if (!manifest_digest.ok()) return manifest_digest.status();
    candidate.manifest_digest = *manifest_digest;
    auto partition_replication_epoch = reader.U64();
    if (!partition_replication_epoch.ok()) {
      return partition_replication_epoch.status();
    }
    candidate.partition_replication_epoch = *partition_replication_epoch;
    if (!candidate.operator_recovery) {
      auto source_node_id = reader.String(kMaxIdentifierBytes);
      if (!source_node_id.ok()) return source_node_id.status();
      auto source_assignment_id = reader.Fixed<16>();
      if (!source_assignment_id.ok()) return source_assignment_id.status();
      candidate.source_assignment_id = *source_assignment_id;
      auto source_boot_id = reader.String(kMaxIdentifierBytes);
      if (!source_boot_id.ok()) return source_boot_id.status();
      auto source_history_id = reader.String(kMaxIdentifierBytes);
      if (!source_history_id.ok()) return source_history_id.status();
      if (!IsCanonicalIdentity160(*source_node_id) ||
          !IsCanonicalIdentity160(*source_boot_id) ||
          !IsCanonicalIdentity160(*source_history_id)) {
        return ProtocolError("candidate source lineage is not canonical");
      }
      candidate.source_node_id = std::move(*source_node_id);
      candidate.source_boot_id = std::move(*source_boot_id);
      candidate.source_history_id = std::move(*source_history_id);
      auto next_lsns = ReadHeartbeatFlowVector(reader, "candidate flow vector");
      if (!next_lsns.ok()) return next_lsns.status();
      candidate.applied_next_lsns = std::move(*next_lsns);
    }
    heartbeat.role_information =
        ReplicaCandidate{.progress = std::move(candidate)};
  } else if (*role_kind !=
             static_cast<std::uint8_t>(HeartbeatRoleKind::kNone)) {
    return ProtocolError("unknown heartbeat role-information kind");
  }
  auto has_failover_observation = reader.Bool();
  if (!has_failover_observation.ok()) {
    return has_failover_observation.status();
  }
  if (*has_failover_observation) {
    auto observation = ReadFailoverObservation(reader);
    if (!observation.ok()) return observation.status();
    heartbeat.failover_observation = std::move(*observation);
  }
  if (absl::Status status = Finish(reader); !status.ok()) return status;
  return WireMessage{std::move(heartbeat)};
}

absl::StatusOr<std::string> Encode(const HeartbeatAck& ack) {
  const auto observation = static_cast<std::uint8_t>(ack.observation_status);
  if (observation > 3) return ProtocolError("unknown observation status");
  Writer writer;
  writer.Fixed(ack.session_id);
  writer.U64(ack.heartbeat_sequence);
  writer.U8(observation);
  if (absl::Status status = writer.String(
          ack.observation_detail, kMaxOpaqueFieldBytes, "observation detail");
      !status.ok()) {
    return status;
  }
  writer.U8(static_cast<std::uint8_t>(ack.lease_decision.index()));
  if (const auto* granted = std::get_if<LeaseGranted>(&ack.lease_decision)) {
    if (absl::Status status = WriteLeaseGranted(writer, *granted);
        !status.ok()) {
      return status;
    }
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
  if (!session_id.ok()) return session_id.status();
  ack.session_id = *session_id;
  auto sequence = reader.U64();
  if (!sequence.ok()) return sequence.status();
  ack.heartbeat_sequence = *sequence;
  auto observation = reader.U8();
  if (!observation.ok()) return observation.status();
  if (*observation > 3) return ProtocolError("unknown observation status");
  ack.observation_status = static_cast<ObservationStatus>(*observation);
  auto detail = reader.String(kMaxOpaqueFieldBytes);
  if (!detail.ok()) return detail.status();
  ack.observation_detail = std::move(*detail);
  auto decision = reader.U8();
  if (!decision.ok()) return decision.status();
  switch (*decision) {
    case 0:
      ack.lease_decision = NoChallenge{};
      break;
    case 1: {
      auto granted = ReadLeaseGranted(reader);
      if (!granted.ok()) return granted.status();
      ack.lease_decision = std::move(*granted);
      break;
    }
    case 2: {
      LeaseDenied denied;
      auto nonce = reader.Fixed<16>();
      if (!nonce.ok()) return nonce.status();
      denied.nonce = *nonce;
      auto reason = reader.U16();
      if (!reason.ok()) return reason.status();
      if (*reason < 1 || *reason > 6) {
        return ProtocolError("unknown lease denial reason");
      }
      denied.reason = static_cast<LeaseDenialReason>(*reason);
      auto control_revision = reader.U64();
      if (!control_revision.ok()) return control_revision.status();
      denied.current_control_revision = *control_revision;
      ack.lease_decision = denied;
      break;
    }
    case 3: {
      LeaseStateOutOfDate stale;
      auto nonce = reader.Fixed<16>();
      if (!nonce.ok()) return nonce.status();
      stale.nonce = *nonce;
      auto control_revision = reader.U64();
      if (!control_revision.ok()) return control_revision.status();
      stale.current_control_revision = *control_revision;
      ack.lease_decision = stale;
      break;
    }
    default:
      return ProtocolError("unknown heartbeat lease decision");
  }
  if (absl::Status status = Finish(reader); !status.ok()) return status;
  return WireMessage{std::move(ack)};
}

absl::StatusOr<std::string> Encode(const Fence& fence) {
  Writer writer;
  writer.Fixed(fence.session_id);
  if (absl::Status status =
          WriteIdentity(writer, fence.target_boot_id, "target boot id");
      !status.ok()) {
    return status;
  }
  WriteProjectionBasis(writer, fence.basis);
  if (absl::Status status = WriteAuthorityAnchor(writer, fence.reject_through);
      !status.ok()) {
    return status;
  }
  return std::move(writer).Take();
}

absl::StatusOr<WireMessage> DecodeFence(std::string_view bytes) {
  Reader reader(bytes);
  Fence fence;
  auto session_id = reader.Fixed<16>();
  if (!session_id.ok()) return session_id.status();
  fence.session_id = *session_id;
  auto boot_id = ReadIdentity(reader, "target boot id");
  if (!boot_id.ok()) return boot_id.status();
  fence.target_boot_id = std::move(*boot_id);
  auto basis = ReadProjectionBasis(reader);
  if (!basis.ok()) return basis.status();
  fence.basis = *basis;
  auto anchor = ReadAuthorityAnchor(reader);
  if (!anchor.ok()) return anchor.status();
  fence.reject_through = std::move(*anchor);
  if (absl::Status status = Finish(reader); !status.ok()) return status;
  return WireMessage{std::move(fence)};
}

absl::StatusOr<std::string> Encode(const FenceAck& ack) {
  Writer writer;
  writer.Fixed(ack.session_id);
  if (absl::Status status =
          WriteIdentity(writer, ack.target_boot_id, "target boot id");
      !status.ok()) {
    return status;
  }
  if (absl::Status status = WriteAuthorityAnchor(writer, ack.reject_through);
      !status.ok()) {
    return status;
  }
  return std::move(writer).Take();
}

absl::StatusOr<WireMessage> DecodeFenceAck(std::string_view bytes) {
  Reader reader(bytes);
  FenceAck ack;
  auto session_id = reader.Fixed<16>();
  if (!session_id.ok()) return session_id.status();
  ack.session_id = *session_id;
  auto boot_id = ReadIdentity(reader, "target boot id");
  if (!boot_id.ok()) return boot_id.status();
  ack.target_boot_id = std::move(*boot_id);
  auto anchor = ReadAuthorityAnchor(reader);
  if (!anchor.ok()) return anchor.status();
  ack.reject_through = std::move(*anchor);
  if (absl::Status status = Finish(reader); !status.ok()) return status;
  return WireMessage{std::move(ack)};
}

absl::StatusOr<std::string> Encode(const Directive& directive) {
  Writer writer;
  writer.Fixed(directive.session_id);
  WriteProjectionBasis(writer, directive.basis);
  if (absl::Status status = WriteAuthorityAnchor(writer, directive.authority);
      !status.ok()) {
    return status;
  }
  WriteDirectiveIdentity(writer, directive.identity);
  if (absl::Status status = WriteIdentity(writer, directive.recipient_node_id,
                                          "directive recipient node id");
      !status.ok()) {
    return status;
  }
  if (absl::Status status = WriteIdentity(writer, directive.recipient_boot_id,
                                          "directive recipient boot id");
      !status.ok()) {
    return status;
  }
  if (absl::Status status = WriteIdentity(writer, directive.target_node_id,
                                          "directive target node id");
      !status.ok()) {
    return status;
  }
  if (absl::Status status = WriteIdentity(writer, directive.target_boot_id,
                                          "directive target boot id");
      !status.ok()) {
    return status;
  }
  if (absl::Status status = WriteIdentity(writer, directive.source_node_id,
                                          "directive source node id");
      !status.ok()) {
    return status;
  }
  writer.Fixed(directive.source_assignment_id);
  if (absl::Status status = WriteIdentity(writer, directive.source_boot_id,
                                          "directive source boot id");
      !status.ok()) {
    return status;
  }
  if (absl::Status status =
          WriteIdentity(writer, directive.source_replication_history_id,
                        "directive source replication history id");
      !status.ok()) {
    return status;
  }
  writer.U64(directive.manifest_revision);
  writer.Fixed(directive.manifest_digest);
  writer.U64(directive.partition_replication_epoch);
  const auto kind = static_cast<std::uint8_t>(directive.kind);
  if (kind < 1 || kind > static_cast<std::uint8_t>(
                             WireDirectiveKind::kInitializeEmptyPopulation)) {
    return ProtocolError("unknown directive kind");
  }
  writer.U8(kind);
  if (absl::Status status = writer.String(
          directive.payload, kMaxOpaqueFieldBytes, "directive payload");
      !status.ok()) {
    return status;
  }

  return std::move(writer).Take();
}

absl::StatusOr<WireMessage> DecodeDirective(std::string_view bytes) {
  Reader reader(bytes);
  Directive directive;
  auto session_id = reader.Fixed<16>();
  if (!session_id.ok()) return session_id.status();
  directive.session_id = *session_id;
  auto basis = ReadProjectionBasis(reader);
  if (!basis.ok()) return basis.status();
  directive.basis = *basis;
  auto authority = ReadAuthorityAnchor(reader);
  if (!authority.ok()) return authority.status();
  directive.authority = std::move(*authority);
  auto identity = ReadDirectiveIdentity(reader);
  if (!identity.ok()) return identity.status();
  directive.identity = *identity;
  auto recipient_node_id = ReadIdentity(reader, "directive recipient node id");
  if (!recipient_node_id.ok()) return recipient_node_id.status();
  directive.recipient_node_id = std::move(*recipient_node_id);
  auto recipient_boot_id = ReadIdentity(reader, "directive recipient boot id");
  if (!recipient_boot_id.ok()) return recipient_boot_id.status();
  directive.recipient_boot_id = std::move(*recipient_boot_id);
  auto target_node_id = ReadIdentity(reader, "directive target node id");
  if (!target_node_id.ok()) return target_node_id.status();
  directive.target_node_id = std::move(*target_node_id);
  auto target_boot_id = ReadIdentity(reader, "directive target boot id");
  if (!target_boot_id.ok()) return target_boot_id.status();
  directive.target_boot_id = std::move(*target_boot_id);
  auto source_node_id = ReadIdentity(reader, "directive source node id");
  if (!source_node_id.ok()) return source_node_id.status();
  directive.source_node_id = std::move(*source_node_id);
  auto source_assignment_id = reader.Fixed<16>();
  if (!source_assignment_id.ok()) return source_assignment_id.status();
  directive.source_assignment_id = *source_assignment_id;
  auto source_boot_id = ReadIdentity(reader, "directive source boot id");
  if (!source_boot_id.ok()) return source_boot_id.status();
  directive.source_boot_id = std::move(*source_boot_id);
  auto history_id =
      ReadIdentity(reader, "directive source replication history id");
  if (!history_id.ok()) return history_id.status();
  directive.source_replication_history_id = std::move(*history_id);
  auto manifest_revision = reader.U64();
  if (!manifest_revision.ok()) return manifest_revision.status();
  directive.manifest_revision = *manifest_revision;
  auto manifest_digest = reader.Fixed<32>();
  if (!manifest_digest.ok()) return manifest_digest.status();
  directive.manifest_digest = *manifest_digest;
  auto partition_replication_epoch = reader.U64();
  if (!partition_replication_epoch.ok()) {
    return partition_replication_epoch.status();
  }
  directive.partition_replication_epoch = *partition_replication_epoch;
  auto kind = reader.U8();
  if (!kind.ok()) return kind.status();
  if (*kind < 1 || *kind > static_cast<std::uint8_t>(
                               WireDirectiveKind::kInitializeEmptyPopulation)) {
    return ProtocolError("unknown directive kind");
  }
  directive.kind = static_cast<WireDirectiveKind>(*kind);
  auto payload = reader.String(kMaxOpaqueFieldBytes);
  if (!payload.ok()) return payload.status();
  directive.payload = std::move(*payload);

  if (absl::Status status = Finish(reader); !status.ok()) return status;
  return WireMessage{std::move(directive)};
}

absl::StatusOr<std::string> Encode(const DirectiveResponse& receipt) {
  Writer writer;
  writer.Fixed(receipt.session_id);
  if (absl::Status status =
          WriteIdentity(writer, receipt.recipient_boot_id, "recipient boot id");
      !status.ok()) {
    return status;
  }
  WriteDirectiveIdentity(writer, receipt.identity);
  writer.Bool(receipt.started);
  return std::move(writer).Take();
}

absl::StatusOr<WireMessage> DecodeDirectiveResponse(std::string_view bytes) {
  Reader reader(bytes);
  DirectiveResponse receipt;
  auto session_id = reader.Fixed<16>();
  if (!session_id.ok()) return session_id.status();
  receipt.session_id = *session_id;
  auto recipient_boot_id = ReadIdentity(reader, "recipient boot id");
  if (!recipient_boot_id.ok()) return recipient_boot_id.status();
  receipt.recipient_boot_id = std::move(*recipient_boot_id);
  auto identity = ReadDirectiveIdentity(reader);
  if (!identity.ok()) return identity.status();
  receipt.identity = *identity;
  auto started = reader.Bool();
  if (!started.ok()) return started.status();
  receipt.started = *started;
  if (absl::Status status = Finish(reader); !status.ok()) return status;
  return WireMessage{std::move(receipt)};
}

absl::StatusOr<std::string> Encode(const DirectiveResult& result) {
  const auto status_tag = static_cast<std::uint8_t>(result.status);
  if (status_tag < 1 || status_tag > 3) {
    return ProtocolError("unknown directive result status");
  }
  Writer writer;
  writer.Fixed(result.session_id);
  if (absl::Status status =
          WriteIdentity(writer, result.recipient_boot_id, "recipient boot id");
      !status.ok()) {
    return status;
  }
  writer.Fixed(result.assignment_id);
  WriteDirectiveIdentity(writer, result.identity);
  writer.U8(status_tag);
  if (absl::Status status = writer.String(result.result, kMaxOpaqueFieldBytes,
                                          "directive result");
      !status.ok()) {
    return status;
  }
  return std::move(writer).Take();
}

absl::StatusOr<WireMessage> DecodeDirectiveResult(std::string_view bytes) {
  Reader reader(bytes);
  DirectiveResult result;
  auto session_id = reader.Fixed<16>();
  if (!session_id.ok()) return session_id.status();
  result.session_id = *session_id;
  auto recipient_boot_id = ReadIdentity(reader, "recipient boot id");
  if (!recipient_boot_id.ok()) return recipient_boot_id.status();
  result.recipient_boot_id = std::move(*recipient_boot_id);
  auto assignment_id = reader.Fixed<16>();
  if (!assignment_id.ok()) return assignment_id.status();
  result.assignment_id = *assignment_id;
  auto identity = ReadDirectiveIdentity(reader);
  if (!identity.ok()) return identity.status();
  result.identity = *identity;
  auto status_tag = reader.U8();
  if (!status_tag.ok()) return status_tag.status();
  if (*status_tag < 1 || *status_tag > 3) {
    return ProtocolError("unknown directive result status");
  }
  result.status = static_cast<DirectiveResultStatus>(*status_tag);
  auto body = reader.String(kMaxOpaqueFieldBytes);
  if (!body.ok()) return body.status();
  result.result = std::move(*body);
  if (absl::Status status = Finish(reader); !status.ok()) return status;
  return WireMessage{std::move(result)};
}

absl::StatusOr<std::string> Encode(const ResultCommitted& committed) {
  Writer writer;
  writer.Fixed(committed.session_id);
  if (absl::Status status = WriteIdentity(writer, committed.recipient_boot_id,
                                          "recipient boot id");
      !status.ok()) {
    return status;
  }
  WriteDirectiveIdentity(writer, committed.identity);
  writer.U64(committed.committed_index);
  return std::move(writer).Take();
}

absl::StatusOr<WireMessage> DecodeResultCommitted(std::string_view bytes) {
  Reader reader(bytes);
  ResultCommitted committed;
  auto session_id = reader.Fixed<16>();
  if (!session_id.ok()) return session_id.status();
  committed.session_id = *session_id;
  auto recipient_boot_id = ReadIdentity(reader, "recipient boot id");
  if (!recipient_boot_id.ok()) return recipient_boot_id.status();
  committed.recipient_boot_id = std::move(*recipient_boot_id);
  auto identity = ReadDirectiveIdentity(reader);
  if (!identity.ok()) return identity.status();
  committed.identity = *identity;
  auto committed_index = reader.U64();
  if (!committed_index.ok()) return committed_index.status();
  committed.committed_index = *committed_index;
  if (absl::Status status = Finish(reader); !status.ok()) return status;
  return WireMessage{std::move(committed)};
}

absl::StatusOr<std::string> Encode(const ResultNoLongerTracked& result) {
  Writer writer;
  writer.Fixed(result.session_id);
  if (absl::Status status =
          WriteIdentity(writer, result.recipient_boot_id, "recipient boot id");
      !status.ok()) {
    return status;
  }
  WriteDirectiveIdentity(writer, result.identity);
  return std::move(writer).Take();
}

absl::StatusOr<WireMessage> DecodeResultNoLongerTracked(
    std::string_view bytes) {
  Reader reader(bytes);
  ResultNoLongerTracked result;
  auto session_id = reader.Fixed<16>();
  if (!session_id.ok()) return session_id.status();
  result.session_id = *session_id;
  auto recipient_boot_id = ReadIdentity(reader, "recipient boot id");
  if (!recipient_boot_id.ok()) return recipient_boot_id.status();
  result.recipient_boot_id = std::move(*recipient_boot_id);
  auto identity = ReadDirectiveIdentity(reader);
  if (!identity.ok()) return identity.status();
  result.identity = *identity;
  if (absl::Status status = Finish(reader); !status.ok()) return status;
  return WireMessage{std::move(result)};
}

}  // namespace

MessageType MessageTypeOf(const WireMessage& message) noexcept {
  return std::visit(
      [](const auto& value) -> MessageType {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, ClientHello>) {
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

absl::StatusOr<std::string> EncodeMessage(const WireMessage& message) {
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
  if (!encoded.ok()) return encoded.status();
  if (RequiresSingleFrame(MessageTypeOf(message)) &&
      encoded->size() > kMaxFramePayloadBytes) {
    return ResourceLimit("non-fragmentable control message exceeds one frame");
  }
  return encoded;
}

absl::StatusOr<WireMessage> DecodeMessage(MessageType type,
                                          std::string_view payload) {
  if (RequiresSingleFrame(type) && payload.size() > kMaxFramePayloadBytes) {
    return ResourceLimit("non-fragmentable control message exceeds one frame");
  }
  switch (type) {
    case MessageType::kNodeControlUpdate: {
      auto update = DecodeNodeControlUpdate(payload);
      if (!update.ok()) return update.status();
      return WireMessage{std::move(*update)};
    }
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
      if (!desired.ok()) return desired.status();
      return WireMessage(std::move(*desired));
    }
  }
  return ProtocolError("unknown message type");
}

bool RequiresSingleFrame(MessageType type) noexcept {
  switch (type) {
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
  if (!count.ok()) return count.status();
  if (*count > cap) {
    return ResourceLimit(std::string(field) + " exceeds its entry cap");
  }
  return *count;
}

absl::Status WriteDataEndpoint(Writer& writer,
                               const WireDataEndpoint& endpoint) {
  if (absl::Status status =
          WriteIdentity(writer, endpoint.node_id, "data node id");
      !status.ok()) {
    return status;
  }
  if (absl::Status status =
          writer.String(endpoint.host, kMaxIdentifierBytes, "data endpoint");
      !status.ok()) {
    return status;
  }
  writer.U16(endpoint.port);
  writer.U16(endpoint.tls_port);
  return absl::OkStatus();
}

absl::StatusOr<WireDataEndpoint> ReadDataEndpoint(Reader& reader) {
  WireDataEndpoint endpoint;
  auto node_id = ReadIdentity(reader, "data node id");
  if (!node_id.ok()) return node_id.status();
  endpoint.node_id = std::move(*node_id);
  auto host = reader.String(kMaxIdentifierBytes);
  if (!host.ok()) return host.status();
  endpoint.host = std::move(*host);
  auto port = reader.U16();
  if (!port.ok()) return port.status();
  endpoint.port = *port;
  auto tls_port = reader.U16();
  if (!tls_port.ok()) return tls_port.status();
  endpoint.tls_port = *tls_port;
  return endpoint;
}

absl::Status WriteDesiredMember(Writer& writer,
                                const WireDesiredMember& member) {
  if (absl::Status status =
          WriteIdentity(writer, member.node_id, "group member node id");
      !status.ok()) {
    return status;
  }
  writer.Fixed(member.assignment_id);
  return absl::OkStatus();
}

absl::StatusOr<WireDesiredMember> ReadDesiredMember(Reader& reader) {
  WireDesiredMember member;
  auto node_id = ReadIdentity(reader, "group member node id");
  if (!node_id.ok()) return node_id.status();
  member.node_id = std::move(*node_id);
  auto assignment_id = reader.Fixed<16>();
  if (!assignment_id.ok()) return assignment_id.status();
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
  if (absl::Status status = ValidateFailoverTransition(transition, group);
      !status.ok()) {
    return status;
  }
  writer.Fixed(transition.transition_id);
  writer.U64(transition.revision);
  writer.U8(static_cast<std::uint8_t>(transition.mode));
  writer.U64(transition.target_term);
  writer.Bool(transition.candidate_action.has_value());
  if (transition.candidate_action.has_value()) {
    const WireFailoverCandidateAction& action = *transition.candidate_action;
    writer.Fixed(action.action_id);
    if (absl::Status status = WriteIdentity(writer, action.candidate.node_id,
                                            "failover candidate node id");
        !status.ok()) {
      return status;
    }
    writer.Fixed(action.candidate.assignment_id);
    if (absl::Status status = WriteIdentity(writer, action.candidate.boot_id,
                                            "failover candidate boot id");
        !status.ok()) {
      return status;
    }
    writer.Bool(action.operator_recovery);
    if (!action.operator_recovery) {
      writer.U64(action.domain.source_group_term);
      if (absl::Status status = WriteIdentity(
              writer, action.domain.source_node_id, "failover source node id");
          !status.ok()) {
        return status;
      }
      writer.Fixed(action.domain.source_assignment_id);
      if (absl::Status status = WriteIdentity(
              writer, action.domain.source_boot_id, "failover source boot id");
          !status.ok()) {
        return status;
      }
      if (absl::Status status =
              WriteIdentity(writer, action.domain.source_history_id,
                            "failover source history id");
          !status.ok()) {
        return status;
      }
      writer.U32(action.domain.flow_count);
    }
    writer.Bool(action.authorization.has_value());
    if (action.authorization.has_value()) {
      writer.U64(action.authorization->authorized_revision);
      writer.U8(
          static_cast<std::uint8_t>(action.authorization->loss_if_cutover));
    }
  }
  return absl::OkStatus();
}

absl::StatusOr<WireFailoverTransition> ReadFailoverTransition(
    Reader& reader, const WireDesiredGroup& group) {
  WireFailoverTransition transition;
  auto transition_id = reader.Fixed<16>();
  if (!transition_id.ok()) return transition_id.status();
  transition.transition_id = *transition_id;
  auto revision = reader.U64();
  if (!revision.ok()) return revision.status();
  transition.revision = *revision;
  auto mode = reader.U8();
  if (!mode.ok()) return mode.status();
  transition.mode = static_cast<WireFailoverMode>(*mode);
  auto target_term = reader.U64();
  if (!target_term.ok()) return target_term.status();
  transition.target_term = *target_term;
  auto has_action = reader.Bool();
  if (!has_action.ok()) return has_action.status();
  if (*has_action) {
    WireFailoverCandidateAction action;
    auto action_id = reader.Fixed<16>();
    if (!action_id.ok()) return action_id.status();
    action.action_id = *action_id;
    auto candidate_node = ReadIdentity(reader, "failover candidate node id");
    if (!candidate_node.ok()) return candidate_node.status();
    action.candidate.node_id = std::move(*candidate_node);
    auto candidate_assignment = reader.Fixed<16>();
    if (!candidate_assignment.ok()) return candidate_assignment.status();
    action.candidate.assignment_id = *candidate_assignment;
    auto candidate_boot = ReadIdentity(reader, "failover candidate boot id");
    if (!candidate_boot.ok()) return candidate_boot.status();
    action.candidate.boot_id = std::move(*candidate_boot);
    auto operator_recovery = reader.Bool();
    if (!operator_recovery.ok()) return operator_recovery.status();
    action.operator_recovery = *operator_recovery;
    if (!action.operator_recovery) {
      auto source_group_term = reader.U64();
      if (!source_group_term.ok()) return source_group_term.status();
      action.domain.source_group_term = *source_group_term;
      auto source_node = ReadIdentity(reader, "failover source node id");
      if (!source_node.ok()) return source_node.status();
      action.domain.source_node_id = std::move(*source_node);
      auto source_assignment = reader.Fixed<16>();
      if (!source_assignment.ok()) return source_assignment.status();
      action.domain.source_assignment_id = *source_assignment;
      auto source_boot = ReadIdentity(reader, "failover source boot id");
      if (!source_boot.ok()) return source_boot.status();
      action.domain.source_boot_id = std::move(*source_boot);
      auto source_history = ReadIdentity(reader, "failover source history id");
      if (!source_history.ok()) return source_history.status();
      action.domain.source_history_id = std::move(*source_history);
      auto flow_count = reader.U32();
      if (!flow_count.ok()) return flow_count.status();
      action.domain.flow_count = *flow_count;
    }
    auto has_authorization = reader.Bool();
    if (!has_authorization.ok()) return has_authorization.status();
    if (*has_authorization) {
      WireFailoverAuthorization authorization;
      auto authorized_revision = reader.U64();
      if (!authorized_revision.ok()) return authorized_revision.status();
      authorization.authorized_revision = *authorized_revision;
      auto loss = reader.U8();
      if (!loss.ok()) return loss.status();
      authorization.loss_if_cutover = static_cast<WireFailoverLoss>(*loss);
      action.authorization = authorization;
    }
    transition.candidate_action = std::move(action);
  }
  if (absl::Status status = ValidateFailoverTransition(transition, group);
      !status.ok()) {
    return status;
  }
  return transition;
}

absl::Status WriteDesiredGroup(Writer& writer, const WireDesiredGroup& group) {
  if (absl::Status status =
          writer.String(group.group_id, kMaxIdentifierBytes, "group id");
      !status.ok()) {
    return status;
  }
  if (absl::Status status = WriteCount(writer, group.members.size(),
                                       kMaxProjectedNodes, "group members");
      !status.ok()) {
    return status;
  }
  for (const WireDesiredMember& member : group.members) {
    if (absl::Status status = WriteDesiredMember(writer, member);
        !status.ok()) {
      return status;
    }
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
    if (absl::Status status =
            WriteIdentity(writer, *group.owner_node_id, "group owner node id");
        !status.ok()) {
      return status;
    }
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
  if (absl::Status status =
          WriteCount(writer, group.slot_ranges.size(), kMaxManifestEntries,
                     "group slot ranges");
      !status.ok()) {
    return status;
  }
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
  if (!group_id.ok()) return group_id.status();
  group.group_id = std::move(*group_id);
  auto member_count = ReadCount(reader, kMaxProjectedNodes, "group members");
  if (!member_count.ok()) return member_count.status();
  group.members.reserve(*member_count);
  for (std::uint32_t i = 0; i < *member_count; ++i) {
    auto member = ReadDesiredMember(reader);
    if (!member.ok()) return member.status();
    group.members.push_back(std::move(*member));
  }
  auto has_owner = reader.Bool();
  if (!has_owner.ok()) return has_owner.status();
  if (*has_owner) {
    auto owner_id = ReadIdentity(reader, "group owner node id");
    if (!owner_id.ok()) return owner_id.status();
    group.owner_node_id = std::move(*owner_id);
    auto assignment_id = reader.Fixed<16>();
    if (!assignment_id.ok()) return assignment_id.status();
    group.owner_assignment_id = *assignment_id;
  }
  auto group_term = reader.U64();
  if (!group_term.ok()) return group_term.status();
  group.group_term = *group_term;
  auto grant_active = reader.Bool();
  if (!grant_active.ok()) return grant_active.status();
  group.grant_active = *grant_active;
  if (!group.owner_node_id.has_value() && group.grant_active) {
    return ProtocolError("grantless group cannot carry an active grant");
  }
  auto has_activation_action = reader.Bool();
  if (!has_activation_action.ok()) return has_activation_action.status();
  if (*has_activation_action) {
    auto action_id = reader.Fixed<16>();
    if (!action_id.ok()) return action_id.status();
    if (!group.grant_active || IsZeroId(*action_id)) {
      return ProtocolError(
          "grant activation action requires a nonzero active grant");
    }
    group.activation_action_id = *action_id;
  }
  auto range_count =
      ReadCount(reader, kMaxManifestEntries, "group slot ranges");
  if (!range_count.ok()) return range_count.status();
  group.slot_ranges.reserve(*range_count);
  for (std::uint32_t i = 0; i < *range_count; ++i) {
    auto first = reader.U16();
    if (!first.ok()) return first.status();
    auto last = reader.U16();
    if (!last.ok()) return last.status();
    if (*first > *last || *last >= 16384) {
      return ProtocolError("group slot range is invalid");
    }
    group.slot_ranges.push_back({.first = *first, .last = *last});
  }
  auto manifest_revision = reader.U64();
  if (!manifest_revision.ok()) return manifest_revision.status();
  group.manifest_revision = *manifest_revision;
  auto manifest_digest = reader.Fixed<32>();
  if (!manifest_digest.ok()) return manifest_digest.status();
  group.manifest_digest = *manifest_digest;
  auto partition_replication_epoch = reader.U64();
  if (!partition_replication_epoch.ok()) {
    return partition_replication_epoch.status();
  }
  group.partition_replication_epoch = *partition_replication_epoch;
  auto steady_replication_enabled = reader.Bool();
  if (!steady_replication_enabled.ok()) {
    return steady_replication_enabled.status();
  }
  group.steady_replication_enabled = *steady_replication_enabled;
  auto has_failover_transition = reader.Bool();
  if (!has_failover_transition.ok()) {
    return has_failover_transition.status();
  }
  if (*has_failover_transition) {
    auto transition = ReadFailoverTransition(reader, group);
    if (!transition.ok()) return transition.status();
    group.failover_transition = std::move(*transition);
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
  if (absl::Status status = WriteCount(writer, manifest.entries.size(),
                                       kMaxManifestEntries, "manifest entries");
      !status.ok()) {
    return status;
  }
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
  if (!revision.ok()) return revision.status();
  manifest.revision = *revision;
  auto digest = reader.Fixed<32>();
  if (!digest.ok()) return digest.status();
  manifest.digest = *digest;
  auto entry_count = ReadCount(reader, kMaxManifestEntries, "manifest entries");
  if (!entry_count.ok()) return entry_count.status();
  manifest.entries.reserve(*entry_count);
  for (std::uint32_t i = 0; i < *entry_count; ++i) {
    auto partition_id = reader.U16();
    if (!partition_id.ok()) return partition_id.status();
    if (*partition_id >= 16384) {
      return ProtocolError("manifest partition id is out of range");
    }
    auto logical_epoch = reader.U64();
    if (!logical_epoch.ok()) return logical_epoch.status();
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
  if (absl::Status status = WriteAuthorityAnchor(writer, directive.authority);
      !status.ok()) {
    return status;
  }
  WriteDirectiveIdentity(writer, directive.identity);
  if (absl::Status status = WriteIdentity(writer, directive.recipient_node_id,
                                          "recipient node id");
      !status.ok()) {
    return status;
  }
  if (absl::Status status = WriteIdentity(writer, directive.recipient_boot_id,
                                          "recipient boot id");
      !status.ok()) {
    return status;
  }
  if (absl::Status status =
          WriteIdentity(writer, directive.target_node_id, "target node id");
      !status.ok()) {
    return status;
  }
  if (absl::Status status =
          WriteIdentity(writer, directive.target_boot_id, "target boot id");
      !status.ok()) {
    return status;
  }
  if (absl::Status status =
          WriteIdentity(writer, directive.source_node_id, "source node id");
      !status.ok()) {
    return status;
  }
  writer.Fixed(directive.source_assignment_id);
  if (absl::Status status =
          WriteIdentity(writer, directive.source_boot_id, "source boot id");
      !status.ok()) {
    return status;
  }
  if (absl::Status status =
          WriteIdentity(writer, directive.source_replication_history_id,
                        "source replication history id");
      !status.ok()) {
    return status;
  }
  writer.U64(directive.manifest_revision);
  writer.Fixed(directive.manifest_digest);
  writer.U64(directive.partition_replication_epoch);
  const auto kind = static_cast<std::uint8_t>(directive.kind);
  if (kind < 1 || kind > static_cast<std::uint8_t>(
                             WireDirectiveKind::kInitializeEmptyPopulation)) {
    return ProtocolError("unknown directive kind");
  }
  writer.U8(kind);
  if (absl::Status status = writer.String(
          directive.payload, kMaxOpaqueFieldBytes, "directive payload");
      !status.ok()) {
    return status;
  }

  return absl::OkStatus();
}

absl::StatusOr<WireProjectedDirective> ReadProjectedDirective(Reader& reader) {
  WireProjectedDirective directive;
  auto authority = ReadAuthorityAnchor(reader);
  if (!authority.ok()) return authority.status();
  directive.authority = std::move(*authority);
  auto identity = ReadDirectiveIdentity(reader);
  if (!identity.ok()) return identity.status();
  directive.identity = *identity;
  auto recipient_node_id = ReadIdentity(reader, "recipient node id");
  if (!recipient_node_id.ok()) return recipient_node_id.status();
  directive.recipient_node_id = std::move(*recipient_node_id);
  auto recipient_boot_id = ReadIdentity(reader, "recipient boot id");
  if (!recipient_boot_id.ok()) return recipient_boot_id.status();
  directive.recipient_boot_id = std::move(*recipient_boot_id);
  auto target_node_id = ReadIdentity(reader, "target node id");
  if (!target_node_id.ok()) return target_node_id.status();
  directive.target_node_id = std::move(*target_node_id);
  auto target_boot_id = ReadIdentity(reader, "target boot id");
  if (!target_boot_id.ok()) return target_boot_id.status();
  directive.target_boot_id = std::move(*target_boot_id);
  auto source_node_id = ReadIdentity(reader, "source node id");
  if (!source_node_id.ok()) return source_node_id.status();
  directive.source_node_id = std::move(*source_node_id);
  auto source_assignment_id = reader.Fixed<16>();
  if (!source_assignment_id.ok()) return source_assignment_id.status();
  directive.source_assignment_id = *source_assignment_id;
  auto source_boot_id = ReadIdentity(reader, "source boot id");
  if (!source_boot_id.ok()) return source_boot_id.status();
  directive.source_boot_id = std::move(*source_boot_id);
  auto history_id = ReadIdentity(reader, "source replication history id");
  if (!history_id.ok()) return history_id.status();
  directive.source_replication_history_id = std::move(*history_id);
  auto manifest_revision = reader.U64();
  if (!manifest_revision.ok()) return manifest_revision.status();
  directive.manifest_revision = *manifest_revision;
  auto manifest_digest = reader.Fixed<32>();
  if (!manifest_digest.ok()) return manifest_digest.status();
  directive.manifest_digest = *manifest_digest;
  auto partition_replication_epoch = reader.U64();
  if (!partition_replication_epoch.ok()) {
    return partition_replication_epoch.status();
  }
  directive.partition_replication_epoch = *partition_replication_epoch;
  auto kind = reader.U8();
  if (!kind.ok()) return kind.status();
  if (*kind < 1 || *kind > static_cast<std::uint8_t>(
                               WireDirectiveKind::kInitializeEmptyPopulation)) {
    return ProtocolError("unknown directive kind");
  }
  directive.kind = static_cast<WireDirectiveKind>(*kind);
  auto payload = reader.String(kMaxOpaqueFieldBytes);
  if (!payload.ok()) return payload.status();
  directive.payload = std::move(*payload);

  return directive;
}

template <class T, class Write>
absl::Status WriteStateList(Writer& w, const std::vector<T>& values,
                            std::size_t cap, Write write) {
  if (auto status = WriteCount(w, values.size(), cap, "state entries");
      !status.ok())
    return status;
  for (const auto& value : values) {
    if (auto status = write(w, value); !status.ok()) return status;
  }
  return absl::OkStatus();
}

template <class T, class Read>
absl::StatusOr<std::vector<T>> ReadStateList(Reader& r, std::size_t cap,
                                             Read read) {
  auto count = ReadCount(r, cap, "state entries");
  if (!count.ok()) return count.status();
  std::vector<T> values;
  values.reserve(*count);
  for (std::uint32_t i = 0; i < *count; ++i) {
    auto value = read(r);
    if (!value.ok()) return value.status();
    values.push_back(std::move(*value));
  }
  return values;
}

absl::Status WriteRoute(Writer& w, const WireRoutingGroup& g) {
  if (auto st = w.String(g.group_id, kMaxIdentifierBytes, "group"); !st.ok())
    return st;
  w.U64(g.term);
  w.Bool(g.owner.has_value());
  if (g.owner) {
    if (auto st = WriteIdentity(w, *g.owner, "owner"); !st.ok()) return st;
    w.Fixed(g.owner_assignment);
  }
  w.Bool(g.available);
  if (auto st = WriteStateList(w, g.members, kMaxProjectedNodes,
                               [](Writer& out, const std::string& id) {
                                 return WriteIdentity(out, id, "member");
                               });
      !st.ok())
    return st;
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
  if (!id.ok()) return id.status();
  g.group_id = std::move(*id);
  auto term = r.U64();
  if (!term.ok()) return term.status();
  g.term = *term;
  auto owner = r.Bool();
  if (!owner.ok()) return owner.status();
  if (*owner) {
    auto id = ReadIdentity(r, "owner");
    if (!id.ok()) return id.status();
    g.owner = std::move(*id);
    auto assignment = r.Fixed<16>();
    if (!assignment.ok()) return assignment.status();
    g.owner_assignment = *assignment;
  }
  auto available = r.Bool();
  if (!available.ok()) return available.status();
  g.available = *available;
  auto members = ReadStateList<std::string>(
      r, kMaxProjectedNodes,
      [](Reader& in) { return ReadIdentity(in, "member"); });
  if (!members.ok()) return members.status();
  g.members = std::move(*members);
  auto slots = ReadStateList<WireSlotRange>(
      r, kMaxManifestEntries, [](Reader& in) -> absl::StatusOr<WireSlotRange> {
        auto first = in.U16();
        if (!first.ok()) return first.status();
        auto last = in.U16();
        if (!last.ok()) return last.status();
        if (*first > *last || *last >= 16384)
          return ProtocolError("invalid route slots");
        return WireSlotRange{*first, *last};
      });
  if (!slots.ok()) return slots.status();
  g.slots = std::move(*slots);
  if (g.available && (!g.owner || g.term == 0 || IsZeroId(g.owner_assignment)))
    return ProtocolError("invalid available route");
  return g;
}

absl::Status WriteState(Writer& w, const RoutingState& v) {
  w.U64(v.revision);
  if (auto st =
          WriteStateList(w, v.nodes, kMaxProjectedNodes, WriteDataEndpoint);
      !st.ok())
    return st;
  if (auto st = WriteStateList(w, v.groups, kMaxProjectedGroups, WriteRoute);
      !st.ok())
    return st;
  return absl::OkStatus();
}
absl::StatusOr<RoutingState> ReadRoutingState(Reader& r) {
  RoutingState v;
  auto revision = r.U64();
  if (!revision.ok()) return revision.status();
  v.revision = std::move(*revision);
  auto nodes =
      ReadStateList<WireDataEndpoint>(r, kMaxProjectedNodes, ReadDataEndpoint);
  if (!nodes.ok()) return nodes.status();
  v.nodes = std::move(*nodes);
  auto groups =
      ReadStateList<WireRoutingGroup>(r, kMaxProjectedGroups, ReadRoute);
  if (!groups.ok()) return groups.status();
  v.groups = std::move(*groups);
  return v;
}

absl::Status WriteState(Writer& w, const LocalGroupState& v) {
  w.U64(v.revision);
  w.U32(v.lease_duration_ms);
  if (auto st = WriteStateList(w, v.groups, 1, WriteDesiredGroup); !st.ok())
    return st;
  if (auto st = WriteStateList(w, v.manifests, 1, WriteManifest); !st.ok())
    return st;
  return absl::OkStatus();
}
absl::StatusOr<LocalGroupState> ReadLocalGroupState(Reader& r) {
  LocalGroupState v;
  auto revision = r.U64();
  if (!revision.ok()) return revision.status();
  v.revision = std::move(*revision);
  auto lease_duration_ms = r.U32();
  if (!lease_duration_ms.ok()) return lease_duration_ms.status();
  v.lease_duration_ms = std::move(*lease_duration_ms);
  auto groups = ReadStateList<WireDesiredGroup>(r, 1, ReadDesiredGroup);
  if (!groups.ok()) return groups.status();
  v.groups = std::move(*groups);
  auto manifests = ReadStateList<WireManifestDocument>(r, 1, ReadManifest);
  if (!manifests.ok()) return manifests.status();
  v.manifests = std::move(*manifests);
  return v;
}

absl::Status WriteState(Writer& w, const MetaDirectoryState& v) {
  w.U64(v.revision);
  if (auto st =
          WriteStateList(w, v.endpoints, kMaxDirectoryEntries, WriteEndpoint);
      !st.ok())
    return st;
  return absl::OkStatus();
}
absl::StatusOr<MetaDirectoryState> ReadMetaDirectoryState(Reader& r) {
  MetaDirectoryState v;
  auto revision = r.U64();
  if (!revision.ok()) return revision.status();
  v.revision = std::move(*revision);
  auto endpoints =
      ReadStateList<WireMetaEndpoint>(r, kMaxDirectoryEntries, ReadEndpoint);
  if (!endpoints.ok()) return endpoints.status();
  v.endpoints = std::move(*endpoints);
  return v;
}

absl::Status WriteState(Writer& w, const TaskChanges& v) {
  w.U64(v.base_revision);
  w.U64(v.revision);
  if (auto st = WriteStateList(w, v.upserts, kMaxProjectedDirectives,
                               WriteProjectedDirective);
      !st.ok())
    return st;
  if (auto st =
          WriteStateList(w, v.removed, kMaxProjectedDirectives,
                         [](Writer& out, const WireDirectiveIdentity& id) {
                           WriteDirectiveIdentity(out, id);
                           return absl::OkStatus();
                         });
      !st.ok())
    return st;
  return absl::OkStatus();
}
absl::StatusOr<TaskChanges> ReadTaskChanges(Reader& r) {
  TaskChanges v;
  auto base_revision = r.U64();
  if (!base_revision.ok()) return base_revision.status();
  v.base_revision = std::move(*base_revision);
  auto revision = r.U64();
  if (!revision.ok()) return revision.status();
  v.revision = std::move(*revision);
  auto upserts = ReadStateList<WireProjectedDirective>(
      r, kMaxProjectedDirectives, ReadProjectedDirective);
  if (!upserts.ok()) return upserts.status();
  v.upserts = std::move(*upserts);
  auto removed = ReadStateList<WireDirectiveIdentity>(
      r, kMaxProjectedDirectives, ReadDirectiveIdentity);
  if (!removed.ok()) return removed.status();
  v.removed = std::move(*removed);
  return v;
}

absl::Status WriteFullDesiredStateBody(Writer& writer,
                                       const FullDesiredState& state) {
  if (state.authority_lease_duration_ms == 0) {
    return ProtocolError("FullDesiredState lease duration is invalid");
  }
  writer.U16(kProtocolVersion);
  writer.U64(state.control_revision);
  writer.U64(state.topology_epoch);
  writer.U32(state.authority_lease_duration_ms);

  if (absl::Status status = WriteCount(writer, state.meta_directory.size(),
                                       kMaxDirectoryEntries, "Meta directory");
      !status.ok()) {
    return status;
  }
  for (const WireMetaEndpoint& endpoint : state.meta_directory) {
    if (absl::Status status = WriteEndpoint(writer, endpoint); !status.ok()) {
      return status;
    }
  }

  if (absl::Status status = WriteCount(writer, state.nodes.size(),
                                       kMaxProjectedNodes, "projected nodes");
      !status.ok()) {
    return status;
  }
  for (const WireDataEndpoint& endpoint : state.nodes) {
    if (absl::Status status = WriteDataEndpoint(writer, endpoint);
        !status.ok()) {
      return status;
    }
  }

  if (absl::Status status = WriteCount(writer, state.groups.size(),
                                       kMaxProjectedGroups, "projected groups");
      !status.ok()) {
    return status;
  }
  for (const WireDesiredGroup& group : state.groups) {
    if (absl::Status status = WriteDesiredGroup(writer, group); !status.ok()) {
      return status;
    }
  }

  if (absl::Status status = WriteCount(writer, state.manifests.size(),
                                       kMaxProjectedGroups, "manifests");
      !status.ok()) {
    return status;
  }
  for (const WireManifestDocument& manifest : state.manifests) {
    if (absl::Status status = WriteManifest(writer, manifest); !status.ok()) {
      return status;
    }
  }

  if (absl::Status status =
          WriteCount(writer, state.current_directives.size(),
                     kMaxProjectedDirectives, "current directives");
      !status.ok()) {
    return status;
  }
  for (const WireProjectedDirective& directive : state.current_directives) {
    if (absl::Status status = WriteProjectedDirective(writer, directive);
        !status.ok()) {
      return status;
    }
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
  if (auto status = ValidateFullDesiredStateProjectionBasis(state);
      !status.ok()) {
    return status;
  }
  Writer counter(/*retain_bytes=*/false);
  return WriteFullDesiredStateBody(counter, state);
}

absl::StatusOr<std::string> EncodeFullDesiredState(
    const FullDesiredState& state) {
  if (absl::Status status = ValidateFullDesiredStateProjectionBasis(state);
      !status.ok()) {
    return status;
  }

  Writer writer;
  if (absl::Status status = WriteFullDesiredStateBody(writer, state);
      !status.ok()) {
    return status;
  }
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
  if (!version.ok()) return version.status();
  if (*version != kProtocolVersion) {
    return ProtocolError("unsupported FullDesiredState version");
  }
  auto index = reader.U64();
  if (!index.ok()) return index.status();
  state.control_revision = *index;
  auto topology_epoch = reader.U64();
  if (!topology_epoch.ok()) return topology_epoch.status();
  state.topology_epoch = *topology_epoch;
  auto authority_lease_duration_ms = reader.U32();
  if (!authority_lease_duration_ms.ok()) {
    return authority_lease_duration_ms.status();
  }
  state.authority_lease_duration_ms = *authority_lease_duration_ms;

  auto directory_count =
      ReadCount(reader, kMaxDirectoryEntries, "Meta directory");
  if (!directory_count.ok()) return directory_count.status();
  state.meta_directory.reserve(*directory_count);
  for (std::uint32_t i = 0; i < *directory_count; ++i) {
    auto endpoint = ReadEndpoint(reader);
    if (!endpoint.ok()) return endpoint.status();
    state.meta_directory.push_back(std::move(*endpoint));
  }

  auto node_count = ReadCount(reader, kMaxProjectedNodes, "projected nodes");
  if (!node_count.ok()) return node_count.status();
  state.nodes.reserve(*node_count);
  for (std::uint32_t i = 0; i < *node_count; ++i) {
    auto endpoint = ReadDataEndpoint(reader);
    if (!endpoint.ok()) return endpoint.status();
    state.nodes.push_back(std::move(*endpoint));
  }

  auto group_count = ReadCount(reader, kMaxProjectedGroups, "projected groups");
  if (!group_count.ok()) return group_count.status();
  state.groups.reserve(*group_count);
  for (std::uint32_t i = 0; i < *group_count; ++i) {
    auto group = ReadDesiredGroup(reader);
    if (!group.ok()) return group.status();
    state.groups.push_back(std::move(*group));
  }

  auto manifest_count = ReadCount(reader, kMaxProjectedGroups, "manifests");
  if (!manifest_count.ok()) return manifest_count.status();
  state.manifests.reserve(*manifest_count);
  for (std::uint32_t i = 0; i < *manifest_count; ++i) {
    auto manifest = ReadManifest(reader);
    if (!manifest.ok()) return manifest.status();
    state.manifests.push_back(std::move(*manifest));
  }

  auto directive_count =
      ReadCount(reader, kMaxProjectedDirectives, "current directives");
  if (!directive_count.ok()) return directive_count.status();
  state.current_directives.reserve(*directive_count);
  for (std::uint32_t i = 0; i < *directive_count; ++i) {
    auto directive = ReadProjectedDirective(reader);
    if (!directive.ok()) return directive.status();
    state.current_directives.push_back(std::move(*directive));
  }
  if (absl::Status status = Finish(reader); !status.ok()) return status;
  if (absl::Status status = ValidateFullDesiredStateProjectionBasis(state);
      !status.ok()) {
    return status;
  }
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
  if (left.topology_epoch != right.topology_epoch ||
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
  writer.Fixed(update.request_id);
  writer.Bool(update.tasks.has_value());
  if (update.tasks) {
    if (auto st = WriteState(writer, *update.tasks); !st.ok()) return st;
  }

  writer.Bool(update.routing.has_value());
  if (update.routing) {
    if (auto st = WriteState(writer, *update.routing); !st.ok()) return st;
  }
  writer.Bool(update.local.has_value());
  if (update.local) {
    if (auto st = WriteState(writer, *update.local); !st.ok()) return st;
  }
  writer.Bool(update.directory.has_value());
  if (update.directory) {
    if (auto st = WriteState(writer, *update.directory); !st.ok()) return st;
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
  auto id = reader.Fixed<16>();
  if (!id.ok()) return id.status();
  update.request_id = *id;
  auto has_tasks = reader.Bool();
  if (!has_tasks.ok()) return has_tasks.status();
  if (*has_tasks) {
    auto value = ReadTaskChanges(reader);
    if (!value.ok()) return value.status();
    update.tasks = std::move(*value);
  }
  auto has_routing = reader.Bool();
  if (!has_routing.ok()) return has_routing.status();
  if (*has_routing) {
    auto value = ReadRoutingState(reader);
    if (!value.ok()) return value.status();
    update.routing = std::move(*value);
  }
  auto has_local = reader.Bool();
  if (!has_local.ok()) return has_local.status();
  if (*has_local) {
    auto value = ReadLocalGroupState(reader);
    if (!value.ok()) return value.status();
    update.local = std::move(*value);
  }
  auto has_directory = reader.Bool();
  if (!has_directory.ok()) return has_directory.status();
  if (*has_directory) {
    auto value = ReadMetaDirectoryState(reader);
    if (!value.ok()) return value.status();
    update.directory = std::move(*value);
  }
  if (auto st = reader.Finish(); !st.ok()) return st;
  return update;
}

NodeControlState SelectNodeControlState(const FullDesiredState& source,
                                        std::string_view node_id) {
  NodeControlState state;
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
    if (!status.ok()) return status;
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
