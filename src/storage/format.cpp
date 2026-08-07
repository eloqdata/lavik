#include "keylane/storage/format.h"

#include "absl/crc/crc32c.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <bit>
#include <cstring>

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
    words[i] = std::rotl(words[i - 3] ^ words[i - 8] ^ words[i - 14] ^
                             words[i - 16],
                         1);
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
    const std::uint32_t temp = std::rotl(a, 5) + function + e + constant +
                               words[i];
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
  std::memcpy(&first, digest.bytes.data(), sizeof(first));
  std::memcpy(&second, digest.bytes.data() + sizeof(first), sizeof(second));
  first ^= second + 0x9e3779b97f4a7c15ULL + (first << 6) + (first >> 2);
  return static_cast<std::size_t>(first);
}

Digest ComputeDigest(std::string_view key) noexcept {
  std::array<std::uint32_t, 5> state{
      0x67452301U, 0xefcdab89U, 0x98badcfeU, 0x10325476U, 0xc3d2e1f0U};
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
    tail[tail_bytes - 1 - i] =
        static_cast<std::uint8_t>(bit_length >> (i * 8));
  }
  Sha1Compress(tail.data(), &state);
  if (tail_bytes == 128) {
    Sha1Compress(tail.data() + 64, &state);
  }

  Digest digest;
  for (std::size_t i = 0; i < state.size(); ++i) {
    StoreBigEndian(state[i], digest.bytes.data() + i * 4);
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
  const absl::string_view input(
      reinterpret_cast<const char*>(bytes.data()), bytes.size());
  return static_cast<std::uint32_t>(absl::ComputeCrc32c(input));
}

void EncodeDeviceLabel(
    const DeviceLabel& label,
    std::span<std::byte, kDirectIoAlignment> output) noexcept {
  std::fill(output.begin(), output.end(), std::byte{0});
  DeviceLabel encoded = label;
  encoded.checksum = 0;
  std::memcpy(output.data(), &encoded, sizeof(encoded));
  encoded.checksum = Crc32c(output);
  std::memcpy(output.data(), &encoded, sizeof(encoded));
}

bool DecodeDeviceLabel(
    std::span<const std::byte, kDirectIoAlignment> input,
    DeviceLabel* label) noexcept {
  if (label == nullptr) {
    return false;
  }
  DeviceLabel decoded{};
  std::memcpy(&decoded, input.data(), sizeof(decoded));
  if (decoded.magic != kDeviceLabelMagic ||
      decoded.version != kStorageFormatVersion ||
      decoded.header_bytes != kDirectIoAlignment ||
      decoded.storage_set_id == 0 || decoded.device_id >= kDeviceIdLimit ||
      decoded.capacity_blocks < 2 ||
      decoded.capacity_blocks > kLocalBlockIdLimit ||
      decoded.device_count == 0 ||
      decoded.device_count > std::numeric_limits<std::uint16_t>::max() ||
      decoded.block_bytes != kStorageBlockBytes) {
    return false;
  }
  const std::uint32_t expected = decoded.checksum;
  std::array<std::byte, kDirectIoAlignment> copy{};
  std::memcpy(copy.data(), input.data(), copy.size());
  decoded.checksum = 0;
  std::memcpy(copy.data(), &decoded, sizeof(decoded));
  if (Crc32c(copy) != expected) {
    return false;
  }
  *label = decoded;
  return true;
}

void EncodeMetadataPage(
    MetadataPageKind kind, std::uint32_t page_index,
    std::uint64_t generation, std::span<const std::byte> payload,
    std::span<std::byte, kDirectIoAlignment> output) noexcept {
  assert(payload.size() <= kMetadataPagePayloadBytes);
  std::fill(output.begin(), output.end(), std::byte{0});
  MetadataPageHeader header{
      .magic = kMetadataPageMagic,
      .version = kStorageFormatVersion,
      .kind = kind,
      .header_bytes = sizeof(MetadataPageHeader),
      .page_index = page_index,
      .payload_bytes = static_cast<std::uint32_t>(payload.size()),
      .generation = generation,
      .checksum = 0,
  };
  std::memcpy(output.data(), &header, sizeof(header));
  std::memcpy(output.data() + sizeof(header), payload.data(), payload.size());
  header.checksum = Crc32c(output);
  std::memcpy(output.data(), &header, sizeof(header));
}

bool DecodeMetadataPage(
    std::span<const std::byte, kDirectIoAlignment> input,
    MetadataPageKind expected_kind, std::uint32_t expected_page_index,
    std::uint64_t* generation, std::span<std::byte> payload) noexcept {
  if (generation == nullptr) {
    return false;
  }
  MetadataPageHeader header{};
  std::memcpy(&header, input.data(), sizeof(header));
  if (header.magic != kMetadataPageMagic ||
      header.version != kStorageFormatVersion ||
      header.kind != expected_kind ||
      header.header_bytes != sizeof(MetadataPageHeader) ||
      header.page_index != expected_page_index || header.generation == 0 ||
      header.payload_bytes > kMetadataPagePayloadBytes ||
      header.payload_bytes > payload.size()) {
    return false;
  }
  const std::uint32_t expected_checksum = header.checksum;
  std::array<std::byte, kDirectIoAlignment> copy{};
  std::memcpy(copy.data(), input.data(), copy.size());
  header.checksum = 0;
  std::memcpy(copy.data(), &header, sizeof(header));
  if (Crc32c(copy) != expected_checksum) {
    return false;
  }
  std::fill(payload.begin(), payload.end(), std::byte{0});
  std::memcpy(payload.data(),
              input.data() + sizeof(MetadataPageHeader),
              header.payload_bytes);
  *generation = header.generation;
  return true;
}

void EncodeBlockHeader(
    const BlockHeader& header,
    std::span<std::byte, kBlockHeaderSlotBytes> output) noexcept {
  std::fill(output.begin(), output.end(), std::byte{0});
  BlockHeader encoded = header;
  encoded.checksum = 0;
  std::memcpy(output.data(), &encoded, sizeof(encoded));
  encoded.checksum = Crc32c(output);
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
    if (!DecodeBlockHeader(
            std::span<const std::byte, kBlockHeaderSlotBytes>(
                input.data() + slot * kBlockHeaderSlotBytes,
                kBlockHeaderSlotBytes),
            &decoded)) {
      continue;
    }
    if (!found || decoded.allocation_epoch > best.allocation_epoch ||
        (decoded.allocation_epoch == best.allocation_epoch &&
         decoded.committed_bytes > best.committed_bytes)) {
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

bool DecodeBlockHeader(
    std::span<const std::byte, kBlockHeaderSlotBytes> input,
    BlockHeader* header) noexcept {
  if (header == nullptr) {
    return false;
  }
  BlockHeader decoded{};
  std::memcpy(&decoded, input.data(), sizeof(decoded));
  if (decoded.magic != kBlockMagic ||
      decoded.block_id == kInvalidBlockId ||
      LocalBlockId(decoded.block_id) == 0 ||
      decoded.version != kStorageFormatVersion ||
      decoded.header_bytes != kBlockHeaderBytes ||
      decoded.block_bytes != kStorageBlockBytes ||
      decoded.layout_worker_count == 0 ||
      decoded.layout_worker_count > kLogicalStorageShards ||
      decoded.writer_id >= decoded.layout_worker_count ||
      decoded.committed_bytes < kBlockHeaderBytes ||
      decoded.committed_bytes > kStorageBlockBytes) {
    return false;
  }
  if ((decoded.kind != BlockKind::kRecords &&
       decoded.kind != BlockKind::kValueExtent) ||
      decoded.reserved != std::array<std::uint8_t, 3>{}) {
    return false;
  }
  if (decoded.kind == BlockKind::kRecords) {
    if (decoded.extent_index != 0 || decoded.extent_payload_bytes != 0 ||
        decoded.extent_payload_checksum != 0) {
      return false;
    }
  } else if (decoded.record_count != 0 ||
             decoded.extent_payload_bytes == 0 ||
             decoded.extent_payload_bytes > kExtentPayloadBytes ||
             decoded.committed_bytes !=
                 kBlockHeaderBytes + decoded.extent_payload_bytes) {
    return false;
  }
  const std::uint32_t expected = decoded.checksum;
  std::array<std::byte, kBlockHeaderSlotBytes> copy{};
  std::memcpy(copy.data(), input.data(), copy.size());
  decoded.checksum = 0;
  std::memcpy(copy.data(), &decoded, sizeof(decoded));
  if (Crc32c(copy) != expected) {
    return false;
  }
  *header = decoded;
  return true;
}

bool EncodeRecordHeader(
    const RecordHeader& header, std::string_view key,
    std::span<std::byte> output) noexcept {
  const std::size_t header_bytes = RecordHeaderBytes(key.size());
  if (header.magic != kRecordMagic ||
      header.version != kStorageFormatVersion ||
      key.size() > MaxKeyBytes() || key.size() != header.key_bytes ||
      header.db_id >= kLogicalDatabaseCount ||
      static_cast<std::uint8_t>(header.value_type) >
          static_cast<std::uint8_t>(ValueType::kStream) ||
      (header.kind == RecordKind::kValue &&
       header.value_type == ValueType::kNone) ||
      (header.kind == RecordKind::kTombstone &&
       (header.logical_size != 0 || header.payload_bytes != 0 ||
        header.external || header.expire_at_ms != 0 ||
        header.value_type != ValueType::kNone)) ||
      header.header_bytes != header_bytes || output.size() != header_bytes) {
    return false;
  }
  std::fill(output.begin(), output.end(), std::byte{0});
  RecordHeader encoded = header;
  encoded.value_type = static_cast<ValueType>(
      static_cast<std::uint8_t>(encoded.value_type) |
      (encoded.external ? kExternalValueMask : 0));
  encoded.external = false;
  encoded.header_checksum = 0;
  std::memcpy(output.data(), &encoded, sizeof(encoded));
  std::memcpy(output.data() + sizeof(encoded), key.data(), key.size());
  encoded.header_checksum = Crc32c(output);
  std::memcpy(output.data(), &encoded, sizeof(encoded));
  return true;
}

bool DecodeRecordHeader(
    std::span<const std::byte> input,
    RecordHeader* header, std::string_view* key) noexcept {
  if (header == nullptr || key == nullptr ||
      input.size() < sizeof(RecordHeader)) {
    return false;
  }
  RecordHeader decoded{};
  std::memcpy(&decoded, input.data(), sizeof(decoded));
  const std::uint8_t encoded_type =
      static_cast<std::uint8_t>(decoded.value_type);
  decoded.external = (encoded_type & kExternalValueMask) != 0;
  decoded.value_type =
      static_cast<ValueType>(encoded_type & ~kExternalValueMask);
  if (decoded.magic != kRecordMagic ||
      decoded.version != kStorageFormatVersion ||
      (decoded.kind != RecordKind::kValue &&
       decoded.kind != RecordKind::kTombstone) ||
      decoded.db_id >= kLogicalDatabaseCount ||
      static_cast<std::uint8_t>(decoded.value_type) >
          static_cast<std::uint8_t>(ValueType::kStream) ||
      (decoded.kind == RecordKind::kValue &&
       decoded.value_type == ValueType::kNone) ||
      decoded.replication_epoch == 0 || decoded.db_epoch == 0 ||
      decoded.key_bytes > MaxKeyBytes() ||
      decoded.header_bytes != RecordHeaderBytes(decoded.key_bytes) ||
      decoded.header_bytes > input.size() ||
      decoded.total_disk_bytes != AlignRecord(
          static_cast<std::size_t>(decoded.header_bytes) +
          decoded.payload_bytes) ||
      decoded.total_disk_bytes > kStorageBlockBytes - kBlockHeaderBytes) {
    return false;
  }
  if (decoded.kind == RecordKind::kTombstone &&
      (decoded.logical_size != 0 || decoded.payload_bytes != 0 ||
       decoded.external || decoded.expire_at_ms != 0 ||
       decoded.value_type != ValueType::kNone)) {
    return false;
  }
  const std::uint32_t expected = decoded.header_checksum;
  std::array<std::byte, kMaxRecordHeaderBytes> copy{};
  std::memcpy(copy.data(), input.data(), decoded.header_bytes);
  RecordHeader checksum_header = decoded;
  checksum_header.value_type = static_cast<ValueType>(encoded_type);
  checksum_header.external = false;
  checksum_header.header_checksum = 0;
  std::memcpy(copy.data(), &checksum_header, sizeof(checksum_header));
  if (Crc32c(std::span<const std::byte>(copy.data(), decoded.header_bytes)) !=
      expected) {
    return false;
  }
  *header = decoded;
  *key = std::string_view(
      reinterpret_cast<const char*>(input.data() + sizeof(RecordHeader)),
      decoded.key_bytes);
  return true;
}

}  // namespace keylane::storage
