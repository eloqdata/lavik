#include "meta/meta_encoding.h"

namespace keylane::meta {

absl::Status MetaFailStopError(std::string_view message) {
  // kInvalidArgument maps to MetaFailureClass::kFailStop; see the header.
  return absl::Status(absl::StatusCode::kInvalidArgument, message);
}

absl::Status MetaDomainRejectError(std::string_view message) {
  // kFailedPrecondition maps to MetaFailureClass::kDomainReject.
  return absl::Status(absl::StatusCode::kFailedPrecondition, message);
}

MetaFailureClass MetaFailureClassOf(const absl::Status& status) {
  if (status.code() == absl::StatusCode::kFailedPrecondition) {
    return MetaFailureClass::kDomainReject;
  }
  // Every other failure this layer produces is a decode/fail-stop failure.
  return MetaFailureClass::kFailStop;
}

void MetaWriter::WriteU8(std::uint8_t v) {
  buffer_.push_back(static_cast<char>(v));
}

void MetaWriter::WriteU16(std::uint16_t v) {
  buffer_.push_back(static_cast<char>(v & 0xFF));
  buffer_.push_back(static_cast<char>((v >> 8) & 0xFF));
}

void MetaWriter::WriteU32(std::uint32_t v) {
  for (unsigned shift = 0; shift < 32; shift += 8) {
    buffer_.push_back(static_cast<char>((v >> shift) & 0xFF));
  }
}

void MetaWriter::WriteU64(std::uint64_t v) {
  for (unsigned shift = 0; shift < 64; shift += 8) {
    buffer_.push_back(static_cast<char>((v >> shift) & 0xFF));
  }
}

void MetaWriter::WriteRaw(std::string_view bytes) { buffer_.append(bytes); }

void MetaWriter::WriteString(std::string_view bytes) {
  WriteU32(static_cast<std::uint32_t>(bytes.size()));
  buffer_.append(bytes);
}

void MetaWriter::WriteCount(std::uint32_t count) { WriteU32(count); }

absl::StatusOr<std::uint8_t> MetaReader::ReadU8() {
  auto raw = ReadRaw(1);
  if (!raw.ok()) return raw.status();
  return static_cast<std::uint8_t>((*raw)[0]);
}

absl::StatusOr<std::uint16_t> MetaReader::ReadU16() {
  auto raw = ReadRaw(2);
  if (!raw.ok()) return raw.status();
  const auto* p = reinterpret_cast<const unsigned char*>(raw->data());
  return static_cast<std::uint16_t>(p[0] | (p[1] << 8));
}

absl::StatusOr<std::uint32_t> MetaReader::ReadU32() {
  auto raw = ReadRaw(4);
  if (!raw.ok()) return raw.status();
  const auto* p = reinterpret_cast<const unsigned char*>(raw->data());
  std::uint32_t v = 0;
  for (unsigned i = 0; i < 4; ++i) {
    v |= static_cast<std::uint32_t>(p[i]) << (8 * i);
  }
  return v;
}

absl::StatusOr<std::uint64_t> MetaReader::ReadU64() {
  auto raw = ReadRaw(8);
  if (!raw.ok()) return raw.status();
  const auto* p = reinterpret_cast<const unsigned char*>(raw->data());
  std::uint64_t v = 0;
  for (unsigned i = 0; i < 8; ++i) {
    v |= static_cast<std::uint64_t>(p[i]) << (8 * i);
  }
  return v;
}

absl::StatusOr<std::string_view> MetaReader::ReadRaw(std::size_t bytes) {
  if (bytes > remaining()) {
    return MetaFailStopError("truncated buffer: not enough bytes");
  }
  std::string_view out = data_.substr(pos_, bytes);
  pos_ += bytes;
  return out;
}

absl::StatusOr<std::string_view> MetaReader::ReadString(
    std::uint32_t max_bytes) {
  auto len = ReadU32();
  if (!len.ok()) return len.status();
  // Cap first: an over-cap prefix is a hard failure even when the body is
  // also truncated (plan §2: over-limit input fails safe, never truncates).
  if (*len > max_bytes) {
    return MetaFailStopError("length prefix exceeds the field cap");
  }
  return ReadRaw(*len);
}

absl::StatusOr<std::uint32_t> MetaReader::ReadCount(std::uint32_t max_count) {
  auto count = ReadU32();
  if (!count.ok()) return count.status();
  if (*count > max_count) {
    return MetaFailStopError("list count exceeds the field cap");
  }
  return count;
}

absl::Status MetaReader::Finish() const {
  if (pos_ != data_.size()) {
    return MetaFailStopError("trailing bytes after end of value");
  }
  return absl::OkStatus();
}

}  // namespace keylane::meta
