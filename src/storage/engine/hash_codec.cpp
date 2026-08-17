#include "impl.h"

namespace keylane::storage {

namespace {

struct HashValueHeader {
  std::uint64_t magic_ = kHashValueMagic;
  std::uint32_t version_ = kStorageFormatVersion;
  std::uint32_t header_bytes_ = sizeof(HashValueHeader);
  std::uint32_t element_count_ = 0;
  std::uint32_t reserved_ = 0;
  std::uint64_t encoded_bytes_ = 0;
};

static_assert(sizeof(HashValueHeader) == 32);

void AppendU32(std::string* output, std::uint32_t value) {
  output->push_back(static_cast<char>(value));
  output->push_back(static_cast<char>(value >> 8));
  output->push_back(static_cast<char>(value >> 16));
  output->push_back(static_cast<char>(value >> 24));
}

bool ReadU32(std::string_view input, std::size_t* offset,
             std::uint32_t* value) {
  if (*offset > input.size() || input.size() - *offset < sizeof(*value)) {
    return false;
  }
  const auto* bytes =
      reinterpret_cast<const unsigned char*>(input.data()) + *offset;
  *value = static_cast<std::uint32_t>(bytes[0]) |
           (static_cast<std::uint32_t>(bytes[1]) << 8) |
           (static_cast<std::uint32_t>(bytes[2]) << 16) |
           (static_cast<std::uint32_t>(bytes[3]) << 24);
  *offset += sizeof(*value);
  return true;
}

}  // namespace

absl::StatusOr<HashValue> DecodeHashValue(std::string_view payload) {
  if (payload.size() < sizeof(HashValueHeader) ||
      payload.size() > kMaxStringBytes) {
    return absl::InternalError("Hash value is truncated");
  }
  HashValueHeader header{};
  std::memcpy(&header, payload.data(), sizeof(header));
  if (header.magic_ != kHashValueMagic ||
      header.version_ != kStorageFormatVersion ||
      header.header_bytes_ != sizeof(HashValueHeader) ||
      header.element_count_ == 0 || header.reserved_ != 0 ||
      header.encoded_bytes_ != payload.size()) {
    return absl::InternalError("invalid Hash value header");
  }
  constexpr std::size_t kMinimumEntryBytes =
      sizeof(Digest) + 2 * sizeof(std::uint32_t);
  const std::size_t encoded_entries_bytes = payload.size() - sizeof(header);
  if (header.element_count_ > encoded_entries_bytes / kMinimumEntryBytes) {
    return absl::InternalError("Hash element count exceeds encoded payload");
  }

  HashValue value;
  value.entries_.reserve(header.element_count_);
  std::size_t offset = sizeof(header);
  for (std::uint32_t index = 0; index < header.element_count_; ++index) {
    if (offset > payload.size() || payload.size() - offset < sizeof(Digest)) {
      return absl::InternalError("Hash entry is truncated");
    }
    HashEntry entry;
    std::memcpy(&entry.digest_, payload.data() + offset, sizeof(Digest));
    offset += sizeof(Digest);
    std::uint32_t field_bytes = 0;
    std::uint32_t value_bytes = 0;
    if (!ReadU32(payload, &offset, &field_bytes) ||
        !ReadU32(payload, &offset, &value_bytes) || offset > payload.size() ||
        field_bytes > payload.size() - offset ||
        value_bytes > payload.size() - offset - field_bytes) {
      return absl::InternalError("Hash entry is truncated");
    }
    entry.field_.assign(payload.substr(offset, field_bytes));
    offset += field_bytes;
    entry.value_.assign(payload.substr(offset, value_bytes));
    offset += value_bytes;
    if (entry.digest_ != ComputeDigest(entry.field_)) {
      return absl::InternalError("Hash entry digest does not match field");
    }
    value.entries_.push_back(std::move(entry));
  }
  if (offset != payload.size()) {
    return absl::InternalError("Hash value has trailing bytes");
  }
  return value;
}

absl::StatusOr<std::string> EncodeHashValue(const HashValue& value) {
  if (value.entries_.empty() ||
      value.entries_.size() > std::numeric_limits<std::uint32_t>::max()) {
    return absl::OutOfRangeError("invalid Hash element count");
  }
  std::uint64_t bytes = sizeof(HashValueHeader);
  for (const HashEntry& entry : value.entries_) {
    if (entry.field_.size() > kMaxStringBytes ||
        entry.value_.size() > kMaxStringBytes ||
        bytes > kMaxStringBytes - sizeof(Digest) - 8 ||
        entry.field_.size() >
            kMaxStringBytes - bytes - sizeof(Digest) - 8 ||
        entry.value_.size() > kMaxStringBytes - bytes - sizeof(Digest) -
                                  8 - entry.field_.size()) {
      return absl::OutOfRangeError(
          "Hash field or value exceeds Redis-compatible limits");
    }
    bytes += sizeof(Digest) + 8 + entry.field_.size() + entry.value_.size();
  }

  const HashValueHeader header{
      .magic_ = kHashValueMagic,
      .version_ = kStorageFormatVersion,
      .header_bytes_ = sizeof(HashValueHeader),
      .element_count_ = static_cast<std::uint32_t>(value.entries_.size()),
      .reserved_ = 0,
      .encoded_bytes_ = bytes,
  };
  std::string output;
  output.reserve(static_cast<std::size_t>(bytes));
  output.append(reinterpret_cast<const char*>(&header), sizeof(header));
  for (const HashEntry& entry : value.entries_) {
    output.append(reinterpret_cast<const char*>(&entry.digest_),
                  sizeof(entry.digest_));
    AppendU32(&output, static_cast<std::uint32_t>(entry.field_.size()));
    AppendU32(&output, static_cast<std::uint32_t>(entry.value_.size()));
    output.append(entry.field_);
    output.append(entry.value_);
  }
  return output;
}

}  // namespace keylane::storage
