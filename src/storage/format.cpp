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

#include "lavik/storage/format.h"

#include <sys/random.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cassert>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <type_traits>

#include "absl/crc/crc32c.h"

namespace lavik::storage {
namespace {

std::uint64_t LoadLittleEndian(const std::uint8_t* input) noexcept {
  std::uint64_t value = 0;
  std::memcpy(&value, input, sizeof(value));
  if constexpr (std::endian::native == std::endian::big) {
    value = std::byteswap(value);
  }
  return value;
}

std::uint32_t LoadLittleEndian32(const std::byte* input) noexcept {
  return std::to_integer<std::uint8_t>(input[0]) |
         (static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(input[1]))
          << 8) |
         (static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(input[2]))
          << 16) |
         (static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(input[3]))
          << 24);
}

std::uint64_t LoadLittleEndian64(const std::byte* input) noexcept {
  std::uint64_t value = 0;
  for (unsigned byte = 0; byte < 8; ++byte) {
    value |=
        static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(input[byte]))
        << (byte * 8);
  }
  return value;
}

void StoreLittleEndian32(std::span<std::byte> output, std::size_t offset,
                         std::uint32_t value) noexcept {
  for (unsigned byte = 0; byte < 4; ++byte) {
    output[offset + byte] = static_cast<std::byte>(value >> (byte * 8));
  }
}

void StoreLittleEndian64(std::span<std::byte> output, std::size_t offset,
                         std::uint64_t value) noexcept {
  for (unsigned byte = 0; byte < 8; ++byte) {
    output[offset + byte] = static_cast<std::byte>(value >> (byte * 8));
  }
}

void SipRound(std::uint64_t* v0, std::uint64_t* v1, std::uint64_t* v2,
              std::uint64_t* v3) noexcept {
  *v0 += *v1;
  *v1 = std::rotl(*v1, 13);
  *v1 ^= *v0;
  *v0 = std::rotl(*v0, 32);
  *v2 += *v3;
  *v3 = std::rotl(*v3, 16);
  *v3 ^= *v2;
  *v0 += *v3;
  *v3 = std::rotl(*v3, 21);
  *v3 ^= *v0;
  *v2 += *v1;
  *v1 = std::rotl(*v1, 17);
  *v1 ^= *v2;
  *v2 = std::rotl(*v2, 32);
}

DigestSeed& MutableDigestSeed() noexcept {
  static DigestSeed seed = [] {
    DigestSeed generated{};
    std::size_t offset = 0;
    while (offset != generated.size()) {
      const ssize_t bytes =
          ::getrandom(generated.data() + offset, generated.size() - offset, 0);
      if (bytes > 0) {
        offset += static_cast<std::size_t>(bytes);
      } else if (bytes < 0 && errno == EINTR) {
        continue;
      } else {
        // A predictable fallback would make collision flooding possible. The
        // server is Linux-only and cannot safely run without an OS hash seed.
        std::abort();
      }
    }
    return generated;
  }();
  return seed;
}

std::uint64_t SipHash12(std::string_view input,
                        const std::array<std::uint8_t, 16>& seed) noexcept {
  // Runtime lookup uses Valkey's one compression and two finalization rounds.
  // The explicit-seed API also defines grouped-Hash version-1 routing: changing
  // the runtime hash must not silently change this persisted routing algorithm.
  const std::uint64_t k0 = LoadLittleEndian(seed.data());
  const std::uint64_t k1 = LoadLittleEndian(seed.data() + sizeof(k0));
  std::uint64_t v0 = 0x736f6d6570736575ULL ^ k0;
  std::uint64_t v1 = 0x646f72616e646f6dULL ^ k1;
  std::uint64_t v2 = 0x6c7967656e657261ULL ^ k0;
  std::uint64_t v3 = 0x7465646279746573ULL ^ k1;

  const auto* bytes = reinterpret_cast<const std::uint8_t*>(input.data());
  const std::size_t complete_bytes = input.size() & ~std::size_t{7};
  for (std::size_t offset = 0; offset != complete_bytes; offset += 8) {
    const std::uint64_t word = LoadLittleEndian(bytes + offset);
    v3 ^= word;
    SipRound(&v0, &v1, &v2, &v3);
    v0 ^= word;
  }

  std::uint64_t tail = static_cast<std::uint64_t>(input.size()) << 56;
  for (std::size_t index = complete_bytes; index != input.size(); ++index) {
    tail |= static_cast<std::uint64_t>(bytes[index])
            << (8 * (index - complete_bytes));
  }
  v3 ^= tail;
  SipRound(&v0, &v1, &v2, &v3);
  v0 ^= tail;
  v2 ^= 0xff;
  SipRound(&v0, &v1, &v2, &v3);
  SipRound(&v0, &v1, &v2, &v3);
  return v0 ^ v1 ^ v2 ^ v3;
}

constexpr std::array<std::uint16_t, 256> MakeRedisCrc16Table() noexcept {
  std::array<std::uint16_t, 256> table{};
  for (std::size_t byte = 0; byte < table.size(); ++byte) {
    std::uint16_t crc = static_cast<std::uint16_t>(byte << 8);
    for (unsigned bit = 0; bit < 8; ++bit) {
      crc = static_cast<std::uint16_t>(
          (crc & 0x8000U) != 0 ? (crc << 1) ^ 0x1021U : crc << 1);
    }
    table[byte] = crc;
  }
  return table;
}

constexpr auto kRedisCrc16Table = MakeRedisCrc16Table();

std::uint16_t RedisCrc16(std::string_view key) noexcept {
  std::uint16_t crc = 0;
  for (unsigned char byte : key) {
    const auto index = static_cast<std::uint8_t>((crc >> 8) ^ byte);
    crc = static_cast<std::uint16_t>((crc << 8) ^ kRedisCrc16Table[index]);
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

// Record fields use fixed byte offsets so the durable layout is
// independent of C++ padding and RecordHeader remains only a decoded view.
// All multi-byte fields retain the storage format's native-endian convention.
constexpr std::size_t kRecordMagicOffset = 0;
constexpr std::size_t kRecordKeyBytesOffset = 8;
constexpr std::size_t kRecordLogicalSizeOffset = 12;
constexpr std::size_t kRecordPayloadBytesOffset = 16;
constexpr std::size_t kRecordMetadataOffset = 20;
constexpr std::size_t kRecordReplicationEpochOffset = 24;
constexpr std::size_t kRecordDbEpochOffset = 32;
constexpr std::size_t kRecordMutationSequenceOffset = 40;
constexpr std::size_t kRecordLsnOffset = 48;
constexpr std::size_t kRecordAllocationEpochOffset = 56;
constexpr std::size_t kRecordPayloadChecksumOffset = 64;
constexpr std::size_t kRecordHeaderChecksumOffset = 68;
constexpr std::size_t kRecordOptionalOffset = kRecordHeaderBaseBytes;

constexpr unsigned kRecordKindShift = 0;
constexpr unsigned kRecordDbShift = 2;
constexpr unsigned kRecordTypeShift = 6;
constexpr unsigned kRecordExternalShift = 9;
constexpr unsigned kRecordKeyExternalShift = 10;
constexpr unsigned kRecordHasTxidShift = 11;
constexpr unsigned kRecordHasExpiryShift = 12;
constexpr unsigned kRecordGroupedShift = 13;
constexpr unsigned kRecordAuxiliaryGroupShift = 14;
constexpr std::uint16_t kRecordKindMask = 0x3;
constexpr std::uint16_t kRecordDbMask = 0xf;
constexpr std::uint16_t kRecordTypeMask = 0x7;
constexpr std::uint16_t kRecordReservedMask = 0x8000;

template <typename T>
void StoreRecordField(std::span<std::byte> output, std::size_t offset,
                      T value) noexcept {
  static_assert(std::is_integral_v<T>);
  assert(offset + sizeof(value) <= output.size());
  std::memcpy(output.data() + offset, &value, sizeof(value));
}

template <typename T>
T LoadRecordField(std::span<const std::byte> input,
                  std::size_t offset) noexcept {
  static_assert(std::is_integral_v<T>);
  assert(offset + sizeof(T) <= input.size());
  T value = 0;
  std::memcpy(&value, input.data() + offset, sizeof(value));
  return value;
}

template <std::size_t BufferBytes>
std::uint32_t RecordHeaderChecksumWithBuffer(
    std::span<const std::byte> input) noexcept {
  assert(input.size() <= BufferBytes);
  std::array<std::byte, BufferBytes> copy;
  std::memcpy(copy.data(), input.data(), input.size());
  StoreRecordField(std::span<std::byte>(copy.data(), input.size()),
                   kRecordHeaderChecksumOffset, std::uint32_t{0});
  return Crc32c(std::span<const std::byte>(copy.data(), input.size()));
}

// Keep the maximum-size scratch page out of the overwhelmingly common short-
// key decoder frame. Besides reserving 4 KiB, an inlined array makes hardened
// builds touch the extra page on every read for stack-clash protection.
[[gnu::noinline]] std::uint32_t LargeRecordHeaderChecksum(
    std::span<const std::byte> input) noexcept {
  return RecordHeaderChecksumWithBuffer<kMaxRecordHeaderBytes>(input);
}

std::uint32_t RecordHeaderChecksum(std::span<const std::byte> input) noexcept {
  constexpr std::size_t kCommonHeaderBytes = 256;
  if (input.size() <= kCommonHeaderBytes) [[likely]] {
    return RecordHeaderChecksumWithBuffer<kCommonHeaderBytes>(input);
  }
  return LargeRecordHeaderChecksum(input);
}

constexpr std::uint16_t RecordMetadata(const RecordHeader& header) noexcept {
  return static_cast<std::uint16_t>(
      (static_cast<std::uint16_t>(header.kind_) << kRecordKindShift) |
      (static_cast<std::uint16_t>(header.db_id_) << kRecordDbShift) |
      (static_cast<std::uint16_t>(header.value_type_) << kRecordTypeShift) |
      (static_cast<std::uint16_t>(header.external_) << kRecordExternalShift) |
      (static_cast<std::uint16_t>(header.key_external_)
       << kRecordKeyExternalShift) |
      (static_cast<std::uint16_t>(header.txid_ != 0) << kRecordHasTxidShift) |
      (static_cast<std::uint16_t>(header.expire_at_ms_ != 0)
       << kRecordHasExpiryShift) |
      (static_cast<std::uint16_t>(header.grouped_) << kRecordGroupedShift) |
      (static_cast<std::uint16_t>(header.auxiliary_group_)
       << kRecordAuxiliaryGroupShift));
}

bool ValidGroupedRecordHeader(const RecordHeader& header) noexcept {
  const bool hashed = header.value_type_ == ValueType::kHash ||
                      header.value_type_ == ValueType::kSet;
  const bool ordered = header.value_type_ == ValueType::kString ||
                       header.value_type_ == ValueType::kList ||
                       header.value_type_ == ValueType::kSortedSet ||
                       header.value_type_ == ValueType::kStream;
  if ((header.grouped_ || header.auxiliary_group_) &&
      (header.kind_ != RecordKind::kValue || (!hashed && !ordered) ||
       header.mutation_sequence_ == 0))
    return false;
  if (header.grouped_ && (header.auxiliary_group_ ||
                          (header.logical_size_ == 0 &&
                           header.value_type_ != ValueType::kStream) ||
                          (header.external_ && !header.key_external_)))
    return false;
  if (!header.auxiliary_group_) {
    return header.group_incarnation_ == 0 && header.group_prefix_ == 0 &&
           header.group_prefix_bits_ == 0 && !header.group_retired_ &&
           header.group_batch_txid_ == 0;
  }
  if (header.group_batch_txid_ != 0 && header.txid_ == 0) return false;
  if (header.group_retired_ && header.logical_size_ != 0) return false;
  if (header.group_incarnation_ == 0 || header.group_prefix_bits_ > 64 ||
      header.expire_at_ms_ != 0)
    return false;
  // Ordered page ids are opaque, monotonic identities, not hash ranges. A
  // zero-bit hash mask would reject every valid ordered page id. Sorted Set
  // member pages instead use canonical Hash prefixes, a disjoint namespace.
  if (ordered &&
      (header.value_type_ == ValueType::kString ||
       header.value_type_ == ValueType::kList ||
       header.value_type_ == ValueType::kStream ||
       (header.group_prefix_bits_ == 0 && header.group_prefix_ != 0)))
    return header.group_prefix_ != 0 && header.group_prefix_bits_ == 0;
  // Avoid a full-width shift for the root range and the deepest leaf.
  const auto mask = header.group_prefix_bits_ == 0
                        ? std::uint64_t{0}
                        : std::numeric_limits<std::uint64_t>::max()
                              << (64 - header.group_prefix_bits_);
  return (header.group_prefix_ & ~mask) == 0;
}

constexpr bool RecordMetadataBit(std::uint16_t metadata,
                                 unsigned shift) noexcept {
  return (metadata & (std::uint16_t{1} << shift)) != 0;
}

static_assert(kRecordHeaderChecksumOffset + sizeof(std::uint32_t) ==
              kRecordHeaderBaseBytes);

}  // namespace

std::size_t DigestHash::operator()(const Digest& digest) const noexcept {
  return static_cast<std::size_t>(digest.value_);
}

const DigestSeed& CurrentDigestSeed() noexcept { return MutableDigestSeed(); }

void RestoreDigestSeed(const DigestSeed& seed) noexcept {
  // Storage initialization calls this before recovery hashes a key and before
  // clients are admitted. Keeping the hot ComputeDigest path lock-free is
  // worth making that lifecycle constraint explicit instead of synchronizing
  // every command around a seed that is immutable during serving.
  MutableDigestSeed() = seed;
}

Digest ComputeDigest(std::string_view key) noexcept {
  return Digest{.value_ = SipHash12(key, CurrentDigestSeed())};
}

Digest ComputeDigest(std::string_view key, const DigestSeed& seed) noexcept {
  return Digest{.value_ = SipHash12(key, seed)};
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

std::uint64_t Crc64(std::span<const std::byte> bytes) noexcept {
  constexpr std::uint64_t kPolynomial = 0xad93d23594c935a9ULL;
  std::uint64_t crc = 0;
  for (const std::byte raw : bytes) {
    const std::uint8_t byte = std::to_integer<std::uint8_t>(raw);
    for (unsigned mask = 1; mask <= 0x80; mask <<= 1) {
      bool high = (crc & (std::uint64_t{1} << 63)) != 0;
      if ((byte & mask) != 0) high = !high;
      crc <<= 1;
      if (high) crc ^= kPolynomial;
    }
  }
  std::uint64_t reflected = crc & 1;
  for (unsigned bit = 1; bit < 64; ++bit) {
    crc >>= 1;
    reflected = (reflected << 1) | (crc & 1);
  }
  return reflected;
}

void EncodeSystemStateRoot(const SystemStateRoot& root,
                           std::span<std::byte> output) noexcept {
  assert(output.size() >= sizeof(SystemStateRoot));
  std::fill(output.begin(), output.end(), std::byte{0});
  StoreLittleEndian64(output, 0, kSystemStateRootMagic);
  StoreLittleEndian32(output, 8, kStorageFormatVersion);
  StoreLittleEndian32(output, 12, sizeof(SystemStateRoot));
  StoreLittleEndian64(output, 16, root.generation_);
  StoreLittleEndian64(output, 24, root.manifest_.block_id_);
  StoreLittleEndian64(output, 32, root.manifest_.allocation_epoch_);
  StoreLittleEndian32(output, 40, root.manifest_.payload_bytes_);
  StoreLittleEndian32(output, 44, root.manifest_.payload_checksum_);
  StoreLittleEndian64(output, 48, root.manifest_bytes_);
}

bool DecodeSystemStateRoot(std::span<const std::byte> input,
                           SystemStateRoot* root) noexcept {
  if (root == nullptr || input.size() < sizeof(SystemStateRoot) ||
      LoadLittleEndian64(input.data()) != kSystemStateRootMagic ||
      LoadLittleEndian32(input.data() + 8) != kStorageFormatVersion ||
      LoadLittleEndian32(input.data() + 12) != sizeof(SystemStateRoot)) {
    return false;
  }
  SystemStateRoot decoded;
  decoded.root_bytes_ = sizeof(SystemStateRoot);
  decoded.generation_ = LoadLittleEndian64(input.data() + 16);
  decoded.manifest_.block_id_ = LoadLittleEndian64(input.data() + 24);
  decoded.manifest_.allocation_epoch_ = LoadLittleEndian64(input.data() + 32);
  decoded.manifest_.payload_bytes_ = LoadLittleEndian32(input.data() + 40);
  decoded.manifest_.payload_checksum_ = LoadLittleEndian32(input.data() + 44);
  decoded.manifest_bytes_ = LoadLittleEndian64(input.data() + 48);
  if (decoded.generation_ == 0 ||
      decoded.manifest_.block_id_ == kInvalidBlockId ||
      decoded.manifest_.allocation_epoch_ == 0 ||
      decoded.manifest_.payload_bytes_ == 0 ||
      decoded.manifest_.payload_bytes_ > kExtentPayloadBytes ||
      decoded.manifest_bytes_ != decoded.manifest_.payload_bytes_) {
    return false;
  }
  *root = decoded;
  return true;
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
       decoded.kind_ != BlockKind::kPayloadExtent &&
       decoded.kind_ != BlockKind::kTransaction &&
       decoded.kind_ != BlockKind::kCheckpointIndex) ||
      decoded.reserved_ != std::array<std::uint8_t, 3>{}) {
    return false;
  }
  if (decoded.kind_ == BlockKind::kRecords ||
      decoded.kind_ == BlockKind::kTransaction) {
    if (decoded.extent_index_ != 0 || decoded.extent_payload_bytes_ != 0 ||
        decoded.extent_payload_checksum_ != 0 ||
        decoded.reserved_runtime_ != std::array<std::uint64_t, 3>{} ||
        (decoded.kind_ == BlockKind::kRecords && decoded.tx_generation_ != 0) ||
        (decoded.kind_ == BlockKind::kTransaction &&
         decoded.tx_generation_ == 0)) {
      return false;
    }
  } else if (decoded.kind_ == BlockKind::kPayloadExtent) {
    if (decoded.record_count_ != 0 || decoded.extent_payload_bytes_ == 0 ||
        decoded.extent_payload_bytes_ > kExtentPayloadBytes ||
        decoded.committed_bytes_ !=
            kBlockHeaderBytes + decoded.extent_payload_bytes_ ||
        decoded.reserved_runtime_ != std::array<std::uint64_t, 3>{} ||
        decoded.tx_generation_ != 0) {
      return false;
    }
  } else {
    if (decoded.tx_generation_ == 0 || decoded.extent_payload_bytes_ == 0 ||
        decoded.extent_payload_bytes_ > kExtentPayloadBytes ||
        decoded.committed_bytes_ !=
            kBlockHeaderBytes + decoded.extent_payload_bytes_ ||
        decoded.reserved_runtime_ != std::array<std::uint64_t, 3>{}) {
      return false;
    }
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
  const bool has_txid = header.txid_ != 0;
  const bool has_expiry = header.expire_at_ms_ != 0;
  const std::size_t fixed_header_bytes =
      RecordFixedHeaderBytes(has_txid, has_expiry, header.auxiliary_group_);
  const std::size_t header_bytes =
      RecordHeaderBytes(key.size(), header.key_external_, has_txid, has_expiry,
                        header.auxiliary_group_);
  const std::size_t total_disk_bytes =
      AlignRecord(header_bytes + header.payload_bytes_);
  if (!ValidGroupedRecordHeader(header) || !ValidRecordKeySize(key.size()) ||
      key.size() != header.key_bytes_ ||
      (header.key_external_ && key.empty()) ||
      header.db_id_ >= kLogicalDatabaseCount ||
      (header.kind_ != RecordKind::kValue &&
       header.kind_ != RecordKind::kTombstone &&
       header.kind_ != RecordKind::kTxCommit) ||
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
      header.replication_epoch_ == 0 || header.db_epoch_ == 0 ||
      header.header_bytes_ != header_bytes ||
      header.total_disk_bytes_ != total_disk_bytes ||
      total_disk_bytes > kStorageBlockBytes - kBlockHeaderBytes ||
      header_bytes > kMaxRecordHeaderBytes || output.size() != header_bytes) {
    return false;
  }
  StoreRecordField(output, kRecordMagicOffset, kRecordMagic);
  StoreRecordField(output, kRecordKeyBytesOffset, header.key_bytes_);
  StoreRecordField(output, kRecordLogicalSizeOffset, header.logical_size_);
  StoreRecordField(output, kRecordPayloadBytesOffset, header.payload_bytes_);
  StoreRecordField(output, kRecordMetadataOffset, RecordMetadata(header));
  // Packed metadata occupies two bytes in an aligned four-byte slot. Clear
  // the reserved half explicitly so the common encoder does not rewrite the
  // complete header before storing every field.
  StoreRecordField(output, kRecordMetadataOffset + sizeof(std::uint16_t),
                   std::uint16_t{0});
  StoreRecordField(output, kRecordReplicationEpochOffset,
                   header.replication_epoch_);
  StoreRecordField(output, kRecordDbEpochOffset, header.db_epoch_);
  StoreRecordField(output, kRecordMutationSequenceOffset,
                   header.mutation_sequence_);
  StoreRecordField(output, kRecordLsnOffset, header.lsn_);
  StoreRecordField(output, kRecordAllocationEpochOffset,
                   header.allocation_epoch_);
  StoreRecordField(output, kRecordPayloadChecksumOffset,
                   header.payload_checksum_);
  std::size_t optional_offset = kRecordOptionalOffset;
  if (has_txid) {
    StoreRecordField(output, optional_offset, header.txid_);
    optional_offset += kRecordHeaderOptionalBytes;
  }
  if (has_expiry) {
    StoreRecordField(output, optional_offset, header.expire_at_ms_);
    optional_offset += kRecordHeaderOptionalBytes;
  }
  if (header.auxiliary_group_) {
    StoreRecordField(output, optional_offset, header.group_incarnation_);
    StoreRecordField(output, optional_offset + 8, header.group_prefix_);
    StoreRecordField(
        output, optional_offset + 16,
        static_cast<std::uint64_t>(header.group_prefix_bits_) |
            (static_cast<std::uint64_t>(header.group_retired_) << 8));
    StoreRecordField(output, optional_offset + 24, header.group_batch_txid_);
    optional_offset += kRecordAuxiliaryGroupIdentityBytes;
  }
  assert(optional_offset == fixed_header_bytes);
  std::size_t encoded_bytes = fixed_header_bytes;
  if (!header.key_external_) {
    std::memcpy(output.data() + fixed_header_bytes, key.data(), key.size());
    encoded_bytes += key.size();
  }
  // Every durable field and inline key byte was overwritten above. Only the
  // alignment tail still needs deterministic zeroes for the header checksum.
  std::fill(output.begin() + encoded_bytes, output.end(), std::byte{0});
  StoreRecordField(output, kRecordHeaderChecksumOffset, std::uint32_t{0});
  StoreRecordField(output, kRecordHeaderChecksumOffset, Crc32c(output));
  return true;
}

bool DecodeRecordHeader(std::span<const std::byte> input, RecordHeader* header,
                        std::string_view* key) noexcept {
  if (header == nullptr || key == nullptr ||
      input.size() < kRecordHeaderBaseBytes) {
    return false;
  }
  if (LoadRecordField<std::uint64_t>(input, kRecordMagicOffset) !=
      kRecordMagic) {
    return false;
  }
  const std::uint16_t metadata =
      LoadRecordField<std::uint16_t>(input, kRecordMetadataOffset);
  if ((metadata & kRecordReservedMask) != 0) {
    return false;
  }
  const bool has_txid = RecordMetadataBit(metadata, kRecordHasTxidShift);
  const bool has_expiry = RecordMetadataBit(metadata, kRecordHasExpiryShift);
  const bool auxiliary_group =
      RecordMetadataBit(metadata, kRecordAuxiliaryGroupShift);
  const bool key_external =
      RecordMetadataBit(metadata, kRecordKeyExternalShift);
  const std::uint32_t key_bytes =
      LoadRecordField<std::uint32_t>(input, kRecordKeyBytesOffset);
  const std::uint32_t payload_bytes =
      LoadRecordField<std::uint32_t>(input, kRecordPayloadBytesOffset);
  const std::size_t fixed_header_bytes =
      RecordFixedHeaderBytes(has_txid, has_expiry, auxiliary_group);
  const std::size_t header_bytes = RecordHeaderBytes(
      key_bytes, key_external, has_txid, has_expiry, auxiliary_group);
  if (!ValidRecordKeySize(key_bytes) || header_bytes > kMaxRecordHeaderBytes ||
      header_bytes > input.size()) {
    return false;
  }
  const std::size_t total_disk_bytes =
      AlignRecord(header_bytes + payload_bytes);
  if (total_disk_bytes > kStorageBlockBytes - kBlockHeaderBytes) {
    return false;
  }

  RecordHeader decoded{
      .header_bytes_ = static_cast<std::uint16_t>(header_bytes),
      .kind_ = static_cast<RecordKind>((metadata >> kRecordKindShift) &
                                       kRecordKindMask),
      .db_id_ = static_cast<std::uint8_t>((metadata >> kRecordDbShift) &
                                          kRecordDbMask),
      .value_type_ = static_cast<ValueType>((metadata >> kRecordTypeShift) &
                                            kRecordTypeMask),
      .external_ = RecordMetadataBit(metadata, kRecordExternalShift),
      .key_external_ = key_external,
      .grouped_ = RecordMetadataBit(metadata, kRecordGroupedShift),
      .auxiliary_group_ = auxiliary_group,
      .key_bytes_ = key_bytes,
      .logical_size_ =
          LoadRecordField<std::uint32_t>(input, kRecordLogicalSizeOffset),
      .payload_bytes_ = payload_bytes,
      .total_disk_bytes_ = static_cast<std::uint32_t>(total_disk_bytes),
      .replication_epoch_ =
          LoadRecordField<std::uint64_t>(input, kRecordReplicationEpochOffset),
      .db_epoch_ = LoadRecordField<std::uint64_t>(input, kRecordDbEpochOffset),
      .mutation_sequence_ =
          LoadRecordField<std::uint64_t>(input, kRecordMutationSequenceOffset),
      .lsn_ = LoadRecordField<std::uint64_t>(input, kRecordLsnOffset),
      .allocation_epoch_ =
          LoadRecordField<std::uint64_t>(input, kRecordAllocationEpochOffset),
      .payload_checksum_ =
          LoadRecordField<std::uint32_t>(input, kRecordPayloadChecksumOffset),
      .header_checksum_ =
          LoadRecordField<std::uint32_t>(input, kRecordHeaderChecksumOffset),
  };
  std::size_t optional_offset = kRecordOptionalOffset;
  if (has_txid) {
    decoded.txid_ = LoadRecordField<std::uint64_t>(input, optional_offset);
    optional_offset += kRecordHeaderOptionalBytes;
  }
  if (has_expiry) {
    decoded.expire_at_ms_ =
        LoadRecordField<std::uint64_t>(input, optional_offset);
    optional_offset += kRecordHeaderOptionalBytes;
  }
  if (auxiliary_group) {
    decoded.group_incarnation_ =
        LoadRecordField<std::uint64_t>(input, optional_offset);
    decoded.group_prefix_ =
        LoadRecordField<std::uint64_t>(input, optional_offset + 8);
    const auto bits =
        LoadRecordField<std::uint64_t>(input, optional_offset + 16);
    // Bits 0..7 carry prefix length and bit 8 retires the routing identity.
    // All higher bits stay reserved; the final word names an optional nested
    // command decision independently of its enclosing transaction tag.
    if ((bits & ~std::uint64_t{0x1ff}) != 0 || (bits & 0xff) > 64) return false;
    decoded.group_prefix_bits_ = static_cast<std::uint8_t>(bits & 0xff);
    decoded.group_retired_ = (bits & 0x100) != 0;
    decoded.group_batch_txid_ =
        LoadRecordField<std::uint64_t>(input, optional_offset + 24);
    optional_offset += kRecordAuxiliaryGroupIdentityBytes;
  }
  assert(optional_offset == fixed_header_bytes);

  if (!ValidGroupedRecordHeader(decoded) || (has_txid && decoded.txid_ == 0) ||
      (has_expiry && decoded.expire_at_ms_ == 0) ||
      (decoded.kind_ != RecordKind::kValue &&
       decoded.kind_ != RecordKind::kTombstone &&
       decoded.kind_ != RecordKind::kTxCommit) ||
      decoded.db_id_ >= kLogicalDatabaseCount ||
      static_cast<std::uint8_t>(decoded.value_type_) >
          static_cast<std::uint8_t>(ValueType::kStream) ||
      (decoded.kind_ == RecordKind::kValue &&
       decoded.value_type_ == ValueType::kNone) ||
      (decoded.kind_ == RecordKind::kValue &&
       decoded.value_type_ == ValueType::kString &&
       decoded.logical_size_ > kMaxBitmapBytes) ||
      decoded.replication_epoch_ == 0 || decoded.db_epoch_ == 0 ||
      (decoded.key_external_ && decoded.key_bytes_ == 0)) {
    return false;
  }
  if (decoded.kind_ != RecordKind::kValue &&
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
  if (RecordHeaderChecksum(input.first(header_bytes)) !=
      decoded.header_checksum_) {
    return false;
  }
  *header = decoded;
  if (decoded.key_external_) {
    *key = {};
  } else {
    *key = std::string_view(
        reinterpret_cast<const char*>(input.data() + fixed_header_bytes),
        decoded.key_bytes_);
  }
  return true;
}

bool EncodeReplicationFrameHeader(
    const ReplicationFrameHeader& header,
    std::span<std::byte, sizeof(ReplicationFrameHeader)> output) noexcept {
  if (header.magic_ != kReplicationFrameMagic ||
      header.version_ != kStorageFormatVersion ||
      header.header_bytes_ != sizeof(ReplicationFrameHeader) ||
      header.lsn_ == 0 || header.partition_id_ >= kLogicalStorageShards ||
      header.reserved_ != 0 ||
      header.payload_bytes_ >
          kStorageBlockBytes - sizeof(ReplicationFrameHeader) ||
      header.total_disk_bytes_ !=
          AlignRecord(sizeof(ReplicationFrameHeader) + header.payload_bytes_)) {
    return false;
  }
  const std::uint8_t allowed_flags =
      ReplicationFrameFlag::kFirst | ReplicationFrameFlag::kLast;
  const bool first = (header.flags_ & static_cast<std::uint8_t>(
                                          ReplicationFrameFlag::kFirst)) != 0;
  if ((header.flags_ & ~allowed_flags) != 0 ||
      first != (header.fragment_index_ == 0)) {
    return false;
  }
  if (header.kind_ != ReplicationEventKind::kMutation &&
      header.kind_ != ReplicationEventKind::kTransaction &&
      header.kind_ != ReplicationEventKind::kControl &&
      header.kind_ != ReplicationEventKind::kEphemeral &&
      header.kind_ != ReplicationEventKind::kCatalogMutation) {
    return false;
  }
  ReplicationFrameHeader encoded = header;
  encoded.header_checksum_ = 0;
  std::memcpy(output.data(), &encoded, sizeof(encoded));
  encoded.header_checksum_ = Crc32c(output);
  std::memcpy(output.data(), &encoded, sizeof(encoded));
  return true;
}

bool DecodeReplicationFrameHeader(std::span<const std::byte> input,
                                  ReplicationFrameHeader* header) noexcept {
  if (header == nullptr || input.size() < sizeof(ReplicationFrameHeader)) {
    return false;
  }
  ReplicationFrameHeader decoded{};
  std::memcpy(&decoded, input.data(), sizeof(decoded));
  std::array<std::byte, sizeof(ReplicationFrameHeader)> encoded{};
  const std::uint32_t expected = decoded.header_checksum_;
  decoded.header_checksum_ = 0;
  std::memcpy(encoded.data(), &decoded, sizeof(decoded));
  if (Crc32c(encoded) != expected) {
    return false;
  }
  decoded.header_checksum_ = expected;
  std::array<std::byte, sizeof(ReplicationFrameHeader)> validated{};
  return EncodeReplicationFrameHeader(decoded, validated) &&
                 std::memcmp(validated.data(), input.data(),
                             validated.size()) == 0
             ? (*header = decoded, true)
             : false;
}

}  // namespace lavik::storage
