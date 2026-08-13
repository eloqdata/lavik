#include "keylane/storage/format.h"

#include <algorithm>
#include <array>
#include <bit>
#include <cassert>
#include <cstring>

#include "absl/crc/crc32c.h"

namespace keylane::storage {
namespace {

std::uint32_t LoadBigEndian(const std::uint8_t* input) noexcept {
  return (static_cast<std::uint32_t>(input[0]) << 24) |
         (static_cast<std::uint32_t>(input[1]) << 16) |
         (static_cast<std::uint32_t>(input[2]) << 8) |
         static_cast<std::uint32_t>(input[3]);
}

void StoreBigEndian(std::uint32_t value, std::uint8_t* output) noexcept {
  output[0] = static_cast<std::uint8_t>(value >> 24);
  output[1] = static_cast<std::uint8_t>(value >> 16);
  output[2] = static_cast<std::uint8_t>(value >> 8);
  output[3] = static_cast<std::uint8_t>(value);
}

void Sha1Compress(const std::uint8_t* block,
                  std::array<std::uint32_t, 5>* state) noexcept {
  std::array<std::uint32_t, 80> words{};
  for (std::size_t i = 0; i < 16; ++i) {
    words[i] = LoadBigEndian(block + i * 4);
  }
  for (std::size_t i = 16; i < words.size(); ++i) {
    words[i] = std::rotl(
        words[i - 3] ^ words[i - 8] ^ words[i - 14] ^ words[i - 16], 1);
  }

  std::uint32_t a = (*state)[0];
  std::uint32_t b = (*state)[1];
  std::uint32_t c = (*state)[2];
  std::uint32_t d = (*state)[3];
  std::uint32_t e = (*state)[4];
  for (std::size_t i = 0; i < 80; ++i) {
    std::uint32_t function = 0;
    std::uint32_t constant = 0;
    if (i < 20) {
      function = (b & c) | ((~b) & d);
      constant = 0x5a827999U;
    } else if (i < 40) {
      function = b ^ c ^ d;
      constant = 0x6ed9eba1U;
    } else if (i < 60) {
      function = (b & c) | (b & d) | (c & d);
      constant = 0x8f1bbcdcU;
    } else {
      function = b ^ c ^ d;
      constant = 0xca62c1d6U;
    }
    const std::uint32_t temp =
        std::rotl(a, 5) + function + e + constant + words[i];
    e = d;
    d = c;
    c = std::rotl(b, 30);
    b = a;
    a = temp;
  }
  (*state)[0] += a;
  (*state)[1] += b;
  (*state)[2] += c;
  (*state)[3] += d;
  (*state)[4] += e;
}

std::uint16_t RedisCrc16(std::string_view key) noexcept {
  std::uint16_t crc = 0;
  for (unsigned char byte : key) {
    crc ^= static_cast<std::uint16_t>(byte) << 8;
    for (unsigned bit = 0; bit < 8; ++bit) {
      crc = static_cast<std::uint16_t>(
          (crc & 0x8000U) != 0 ? (crc << 1) ^ 0x1021U : crc << 1);
    }
  }
  return crc;
}

std::string_view HashTag(std::string_view key) noexcept {
  const std::size_t open = key.find('{');
  if (open == std::string_view::npos) {
    return key;
  }
  const std::size_t close = key.find('}', open + 1);
  if (close == std::string_view::npos || close == open + 1) {
    return key;
  }
  return key.substr(open + 1, close - open - 1);
}

}  // namespace

std::size_t DigestHash::operator()(const Digest& digest) const noexcept {
  std::uint64_t first = 0;
  std::uint64_t second = 0;
  std::memcpy(&first, digest.bytes_.data(), sizeof(first));
  std::memcpy(&second, digest.bytes_.data() + sizeof(first), sizeof(second));
  first ^= second + 0x9e3779b97f4a7c15ULL + (first << 6) + (first >> 2);
  return static_cast<std::size_t>(first);
}

Digest ComputeDigest(std::string_view key) noexcept {
  std::array<std::uint32_t, 5> state{0x67452301U, 0xefcdab89U, 0x98badcfeU,
                                     0x10325476U, 0xc3d2e1f0U};
  const auto* input = reinterpret_cast<const std::uint8_t*>(key.data());
  std::size_t remaining = key.size();
  while (remaining >= 64) {
    Sha1Compress(input, &state);
    input += 64;
    remaining -= 64;
  }

  std::array<std::uint8_t, 128> tail{};
  if (remaining != 0) {
    std::memcpy(tail.data(), input, remaining);
  }
  tail[remaining] = 0x80;
  const std::size_t tail_bytes = remaining < 56 ? 64 : 128;
  const std::uint64_t bit_length = static_cast<std::uint64_t>(key.size()) * 8;
  for (unsigned i = 0; i < 8; ++i) {
    tail[tail_bytes - 1 - i] = static_cast<std::uint8_t>(bit_length >> (i * 8));
  }
  Sha1Compress(tail.data(), &state);
  if (tail_bytes == 128) {
    Sha1Compress(tail.data() + 64, &state);
  }

  Digest digest;
  for (std::size_t i = 0; i < state.size(); ++i) {
    StoreBigEndian(state[i], digest.bytes_.data() + i * 4);
  }
  return digest;
}

std::uint16_t RedisSlot(std::string_view key) noexcept {
  return static_cast<std::uint16_t>(RedisCrc16(HashTag(key)) & 0x3fffU);
}

std::uint32_t StorageShardForKey(std::string_view key) noexcept {
  return RedisSlot(key);
}

std::uint32_t Crc32c(std::span<const std::byte> bytes) noexcept {
  const absl::string_view input(reinterpret_cast<const char*>(bytes.data()),
                                bytes.size());
  return static_cast<std::uint32_t>(absl::ComputeCrc32c(input));
}

void EncodeDeviceLabel(
    const DeviceLabel& label,
    std::span<std::byte, kDirectIoAlignment> output) noexcept {
  std::fill(output.begin(), output.end(), std::byte{0});
  DeviceLabel encoded = label;
  encoded.checksum_ = 0;
  std::memcpy(output.data(), &encoded, sizeof(encoded));
  encoded.checksum_ = Crc32c(output);
  std::memcpy(output.data(), &encoded, sizeof(encoded));
}

bool DecodeDeviceLabel(std::span<const std::byte, kDirectIoAlignment> input,
                       DeviceLabel* label) noexcept {
  if (label == nullptr) {
    return false;
  }
  DeviceLabel decoded{};
  std::memcpy(&decoded, input.data(), sizeof(decoded));
  if (decoded.magic_ != kDeviceLabelMagic ||
      decoded.version_ != kStorageFormatVersion ||
      decoded.header_bytes_ != kDirectIoAlignment ||
      decoded.storage_set_id_ == 0 || decoded.device_id_ >= kDeviceIdLimit ||
      decoded.capacity_blocks_ < 2 ||
      decoded.capacity_blocks_ > kLocalBlockIdLimit ||
      decoded.device_count_ == 0 ||
      decoded.device_count_ > std::numeric_limits<std::uint16_t>::max() ||
      decoded.block_bytes_ != kStorageBlockBytes) {
    return false;
  }
  const std::uint32_t expected = decoded.checksum_;
  std::array<std::byte, kDirectIoAlignment> copy{};
  std::memcpy(copy.data(), input.data(), copy.size());
  decoded.checksum_ = 0;
  std::memcpy(copy.data(), &decoded, sizeof(decoded));
  if (Crc32c(copy) != expected) {
    return false;
  }
  *label = decoded;
  return true;
}

void EncodeMetadataPage(
    MetadataPageKind kind, std::uint32_t page_index, std::uint64_t generation,
    std::span<const std::byte> payload,
    std::span<std::byte, kDirectIoAlignment> output) noexcept {
  assert(payload.size() <= kMetadataPagePayloadBytes);
  std::fill(output.begin(), output.end(), std::byte{0});
  MetadataPageHeader header{
      .magic_ = kMetadataPageMagic,
      .version_ = kStorageFormatVersion,
      .kind_ = kind,
      .header_bytes_ = sizeof(MetadataPageHeader),
      .page_index_ = page_index,
      .payload_bytes_ = static_cast<std::uint32_t>(payload.size()),
      .generation_ = generation,
      .checksum_ = 0,
  };
  std::memcpy(output.data(), &header, sizeof(header));
  std::memcpy(output.data() + sizeof(header), payload.data(), payload.size());
  header.checksum_ = Crc32c(output);
  std::memcpy(output.data(), &header, sizeof(header));
}

bool DecodeMetadataPage(std::span<const std::byte, kDirectIoAlignment> input,
                        MetadataPageKind expected_kind,
                        std::uint32_t expected_page_index,
                        std::uint64_t* generation,
                        std::span<std::byte> payload) noexcept {
  if (generation == nullptr) {
    return false;
  }
  MetadataPageHeader header{};
  std::memcpy(&header, input.data(), sizeof(header));
  if (header.magic_ != kMetadataPageMagic ||
      header.version_ != kStorageFormatVersion ||
      header.kind_ != expected_kind ||
      header.header_bytes_ != sizeof(MetadataPageHeader) ||
      header.page_index_ != expected_page_index || header.generation_ == 0 ||
      header.payload_bytes_ > kMetadataPagePayloadBytes ||
      header.payload_bytes_ > payload.size()) {
    return false;
  }
  const std::uint32_t expected_checksum = header.checksum_;
  std::array<std::byte, kDirectIoAlignment> copy{};
  std::memcpy(copy.data(), input.data(), copy.size());
  header.checksum_ = 0;
  std::memcpy(copy.data(), &header, sizeof(header));
  if (Crc32c(copy) != expected_checksum) {
    return false;
  }
  std::fill(payload.begin(), payload.end(), std::byte{0});
  std::memcpy(payload.data(), input.data() + sizeof(MetadataPageHeader),
              header.payload_bytes_);
  *generation = header.generation_;
  return true;
}

void EncodeBlockHeader(
    const BlockHeader& header,
    std::span<std::byte, kBlockHeaderSlotBytes> output) noexcept {
  std::fill(output.begin(), output.end(), std::byte{0});
  BlockHeader encoded = header;
  encoded.checksum_ = 0;
  std::memcpy(output.data(), &encoded, sizeof(encoded));
  encoded.checksum_ = Crc32c(output);
  std::memcpy(output.data(), &encoded, sizeof(encoded));
}

bool DecodeBlockHeaderPages(std::span<const std::byte, kBlockHeaderBytes> input,
                            BlockHeader* header,
                            std::uint8_t* active_slot) noexcept {
  BlockHeader best{};
  std::uint8_t best_slot = 0;
  bool found = false;
  for (std::uint8_t slot = 0; slot < kBlockHeaderSlots; ++slot) {
    BlockHeader decoded{};
    if (!DecodeBlockHeader(std::span<const std::byte, kBlockHeaderSlotBytes>(
                               input.data() + slot * kBlockHeaderSlotBytes,
                               kBlockHeaderSlotBytes),
                           &decoded)) {
      continue;
    }
    if (!found || decoded.allocation_epoch_ > best.allocation_epoch_ ||
        (decoded.allocation_epoch_ == best.allocation_epoch_ &&
         decoded.header_sequence_ > best.header_sequence_)) {
      best = decoded;
      best_slot = slot;
      found = true;
    }
  }
  if (!found) {
    return false;
  }
  if (header != nullptr) {
    *header = best;
  }
  if (active_slot != nullptr) {
    *active_slot = best_slot;
  }
  return true;
}

bool DecodeBlockHeader(std::span<const std::byte, kBlockHeaderSlotBytes> input,
                       BlockHeader* header) noexcept {
  if (header == nullptr) {
    return false;
  }
  BlockHeader decoded{};
  std::memcpy(&decoded, input.data(), sizeof(decoded));
  if (decoded.magic_ != kBlockMagic || decoded.block_id_ == kInvalidBlockId ||
      LocalBlockId(decoded.block_id_) == 0 ||
      decoded.version_ != kStorageFormatVersion ||
      decoded.header_bytes_ != kBlockHeaderBytes ||
      decoded.block_bytes_ != kStorageBlockBytes ||
      decoded.layout_worker_count_ == 0 ||
      decoded.layout_worker_count_ > kLogicalStorageShards ||
      decoded.writer_id_ >= decoded.layout_worker_count_ ||
      decoded.committed_bytes_ < kBlockHeaderBytes ||
      decoded.committed_bytes_ > kStorageBlockBytes) {
    return false;
  }
  if ((decoded.kind_ != BlockKind::kRecords &&
       decoded.kind_ != BlockKind::kPayloadExtent) ||
      decoded.reserved_ != std::array<std::uint8_t, 3>{}) {
    return false;
  }
  if (decoded.kind_ == BlockKind::kRecords) {
    if (decoded.extent_index_ != 0 || decoded.extent_payload_bytes_ != 0 ||
        decoded.extent_payload_checksum_ != 0) {
      return false;
    }
  } else if (decoded.record_count_ != 0 || decoded.extent_payload_bytes_ == 0 ||
             decoded.extent_payload_bytes_ > kExtentPayloadBytes ||
             decoded.committed_bytes_ !=
                 kBlockHeaderBytes + decoded.extent_payload_bytes_) {
    return false;
  }
  const std::uint32_t expected = decoded.checksum_;
  std::array<std::byte, kBlockHeaderSlotBytes> copy{};
  std::memcpy(copy.data(), input.data(), copy.size());
  decoded.checksum_ = 0;
  std::memcpy(copy.data(), &decoded, sizeof(decoded));
  if (Crc32c(copy) != expected) {
    return false;
  }
  *header = decoded;
  return true;
}

bool EncodeRecordHeader(const RecordHeader& header, std::string_view key,
                        std::span<std::byte> output) noexcept {
  const std::size_t header_bytes =
      RecordHeaderBytes(key.size(), header.key_external_);
  if (header.magic_ != kRecordMagic ||
      header.version_ != kStorageFormatVersion || key.size() > MaxKeyBytes() ||
      key.size() != header.key_bytes_ ||
      (header.key_external_ && key.empty()) ||
      header.db_id_ >= kLogicalDatabaseCount ||
      static_cast<std::uint8_t>(header.value_type_) >
          static_cast<std::uint8_t>(ValueType::kStream) ||
      (header.kind_ == RecordKind::kValue &&
       header.value_type_ == ValueType::kNone) ||
      (header.kind_ == RecordKind::kTombstone &&
       (header.logical_size_ != 0 || header.expire_at_ms_ != 0 ||
        header.value_type_ != ValueType::kNone ||
        (!header.key_external_ &&
         (header.payload_bytes_ != 0 || header.external_)) ||
        (header.key_external_ && !header.external_ &&
         header.payload_bytes_ != header.key_bytes_))) ||
      (header.kind_ == RecordKind::kTxCommit &&
       (header.logical_size_ != 0 || header.payload_bytes_ != 0 ||
        header.external_ || header.expire_at_ms_ != 0 ||
        header.value_type_ != ValueType::kNone || header.txid_ == 0 ||
        header.key_bytes_ != 0 || header.key_external_)) ||
      (header.kind_ == RecordKind::kCollectionObject &&
       (header.value_type_ == ValueType::kNone || header.txid_ != 0 ||
        header.expire_at_ms_ != 0 || header.key_bytes_ != 0 ||
        header.key_external_)) ||
      header.header_bytes_ != header_bytes || output.size() != header_bytes) {
    return false;
  }
  std::fill(output.begin(), output.end(), std::byte{0});
  RecordHeader encoded = header;
  encoded.value_type_ =
      static_cast<ValueType>(static_cast<std::uint8_t>(encoded.value_type_) |
                             (encoded.external_ ? kExternalValueMask : 0));
  encoded.external_ = false;
  encoded.key_bytes_ = static_cast<std::uint32_t>(encoded.key_bytes_) |
                       (encoded.key_external_ ? kExternalKeyMask : 0);
  encoded.key_external_ = false;
  encoded.header_checksum_ = 0;
  std::memcpy(output.data(), &encoded, sizeof(encoded));
  if (!header.key_external_) {
    std::memcpy(output.data() + sizeof(encoded), key.data(), key.size());
  }
  encoded.header_checksum_ = Crc32c(output);
  std::memcpy(output.data(), &encoded, sizeof(encoded));
  return true;
}

bool DecodeRecordHeader(std::span<const std::byte> input, RecordHeader* header,
                        std::string_view* key) noexcept {
  if (header == nullptr || key == nullptr ||
      input.size() < sizeof(RecordHeader)) {
    return false;
  }
  RecordHeader decoded{};
  std::memcpy(&decoded, input.data(), sizeof(decoded));
  const std::uint8_t encoded_type =
      static_cast<std::uint8_t>(decoded.value_type_);
  decoded.external_ = (encoded_type & kExternalValueMask) != 0;
  decoded.value_type_ =
      static_cast<ValueType>(encoded_type & ~kExternalValueMask);
  const std::uint32_t encoded_key_bytes = decoded.key_bytes_;
  decoded.key_external_ = (encoded_key_bytes & kExternalKeyMask) != 0;
  decoded.key_bytes_ = encoded_key_bytes & ~kExternalKeyMask;
  if (decoded.magic_ != kRecordMagic ||
      decoded.version_ != kStorageFormatVersion ||
      (decoded.kind_ != RecordKind::kValue &&
       decoded.kind_ != RecordKind::kTombstone &&
       decoded.kind_ != RecordKind::kTxCommit &&
       decoded.kind_ != RecordKind::kCollectionObject) ||
      decoded.db_id_ >= kLogicalDatabaseCount ||
      static_cast<std::uint8_t>(decoded.value_type_) >
          static_cast<std::uint8_t>(ValueType::kStream) ||
      (decoded.kind_ == RecordKind::kValue &&
       decoded.value_type_ == ValueType::kNone) ||
      (decoded.kind_ == RecordKind::kValue &&
       decoded.value_type_ == ValueType::kString &&
       decoded.logical_size_ > kMaxStringBytes) ||
      decoded.replication_epoch_ == 0 || decoded.db_epoch_ == 0 ||
      decoded.key_bytes_ > MaxKeyBytes() ||
      decoded.header_bytes_ !=
          RecordHeaderBytes(decoded.key_bytes_, decoded.key_external_) ||
      decoded.header_bytes_ > input.size() ||
      decoded.total_disk_bytes_ !=
          AlignRecord(static_cast<std::size_t>(decoded.header_bytes_) +
                      decoded.payload_bytes_) ||
      decoded.total_disk_bytes_ > kStorageBlockBytes - kBlockHeaderBytes) {
    return false;
  }
  if (decoded.kind_ != RecordKind::kValue &&
      decoded.kind_ != RecordKind::kCollectionObject &&
      (decoded.logical_size_ != 0 || decoded.expire_at_ms_ != 0 ||
       decoded.value_type_ != ValueType::kNone)) {
    return false;
  }
  if (decoded.kind_ == RecordKind::kTombstone &&
      ((!decoded.key_external_ &&
        (decoded.payload_bytes_ != 0 || decoded.external_)) ||
       (decoded.key_external_ && !decoded.external_ &&
        decoded.payload_bytes_ != decoded.key_bytes_))) {
    return false;
  }
  if (decoded.kind_ == RecordKind::kTxCommit &&
      (decoded.txid_ == 0 || decoded.key_bytes_ != 0 ||
       decoded.key_external_)) {
    return false;
  }
  if (decoded.kind_ == RecordKind::kCollectionObject &&
      (decoded.value_type_ == ValueType::kNone || decoded.txid_ != 0 ||
       decoded.expire_at_ms_ != 0 || decoded.key_bytes_ != 0 ||
       decoded.key_external_)) {
    return false;
  }
  const std::uint32_t expected = decoded.header_checksum_;
  std::array<std::byte, kMaxRecordHeaderBytes> copy{};
  std::memcpy(copy.data(), input.data(), decoded.header_bytes_);
  RecordHeader checksum_header = decoded;
  checksum_header.value_type_ = static_cast<ValueType>(encoded_type);
  checksum_header.external_ = false;
  checksum_header.key_bytes_ = encoded_key_bytes;
  checksum_header.key_external_ = false;
  checksum_header.header_checksum_ = 0;
  std::memcpy(copy.data(), &checksum_header, sizeof(checksum_header));
  if (Crc32c(std::span<const std::byte>(copy.data(), decoded.header_bytes_)) !=
      expected) {
    return false;
  }
  *header = decoded;
  if (decoded.key_external_) {
    *key = {};
  } else {
    *key = std::string_view(
        reinterpret_cast<const char*>(input.data() + sizeof(RecordHeader)),
        decoded.key_bytes_);
  }
  return true;
}

}  // namespace keylane::storage
