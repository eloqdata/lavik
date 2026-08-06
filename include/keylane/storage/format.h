#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <string_view>

namespace keylane::storage {

inline constexpr std::size_t kDirectIoAlignment = 4096;
inline constexpr std::size_t kBlockHeaderBytes = kDirectIoAlignment;
inline constexpr std::size_t kRecordAlignment = 8;
inline constexpr std::size_t kMaxRecordHeaderBytes = kDirectIoAlignment;
inline constexpr std::size_t kStorageBlockBytes = 8 * 1024 * 1024;
inline constexpr std::uint32_t kStorageFormatVersion = 1;
inline constexpr unsigned kLocalBlockIdBits = 27;
inline constexpr std::uint64_t kLocalBlockIdLimit =
    std::uint64_t{1} << kLocalBlockIdBits;
inline constexpr std::uint64_t kLocalBlockIdMask = kLocalBlockIdLimit - 1;
inline constexpr std::uint64_t kDeviceIdLimit =
    std::uint64_t{1} << (64 - kLocalBlockIdBits);
inline constexpr std::uint64_t kInvalidBlockId =
    std::numeric_limits<std::uint64_t>::max();
inline constexpr std::uint64_t kDeviceLabelMagic =
    0x314c42414c4c4bULL;  // KLLABL1
inline constexpr std::uint64_t kBlockMagic = 0x314b4c424c4f434bULL;   // KCOLBLK1
inline constexpr std::uint64_t kRecordMagic = 0x314b4c5245434f52ULL;  // ROCERLK1
inline constexpr std::uint64_t kMetadataMagic = 0x314154454d4c4bULL;  // KLMETA1
inline constexpr std::uint32_t kLogicalStorageShards = 16384;
inline constexpr std::uint8_t kLogicalDatabaseCount = 16;
inline constexpr std::uint64_t kDeviceLabelOffset = 0;
inline constexpr std::uint64_t kStorageMetadataOffset = kDirectIoAlignment;

constexpr std::uint64_t MakeBlockId(std::uint64_t device_id,
                                    std::uint32_t local_block_id) noexcept {
  return (device_id << kLocalBlockIdBits) | local_block_id;
}

constexpr std::uint64_t DeviceIdForBlock(std::uint64_t block_id) noexcept {
  return block_id >> kLocalBlockIdBits;
}

constexpr std::uint32_t LocalBlockId(std::uint64_t block_id) noexcept {
  return static_cast<std::uint32_t>(block_id & kLocalBlockIdMask);
}

constexpr std::uint64_t LocalBlockOffset(std::uint64_t block_id) noexcept {
  return static_cast<std::uint64_t>(LocalBlockId(block_id)) *
         kStorageBlockBytes;
}

struct Digest {
  std::array<std::uint8_t, 20> bytes{};

  bool operator==(const Digest&) const noexcept = default;
};

struct DigestHash {
  std::size_t operator()(const Digest& digest) const noexcept;
};

Digest ComputeDigest(std::string_view key) noexcept;
std::uint16_t RedisSlot(std::string_view key) noexcept;
std::uint32_t StorageShardForKey(std::string_view key) noexcept;

enum class RecordKind : std::uint8_t {
  kValue = 1,
  kTombstone = 2,
};

struct BlockHeader {
  std::uint64_t magic = kBlockMagic;
  std::uint64_t block_id = kInvalidBlockId;
  std::uint32_t version = kStorageFormatVersion;
  std::uint32_t header_bytes = kBlockHeaderBytes;
  std::uint32_t block_bytes = kStorageBlockBytes;
  std::uint32_t writer_id = 0;
  std::uint64_t allocation_epoch = 0;
  std::uint32_t committed_bytes = kBlockHeaderBytes;
  std::uint32_t record_count = 0;
  std::uint64_t max_lsn = 0;
  std::uint32_t checksum = 0;
  std::uint32_t layout_worker_count = 0;
};

// Every configured file or raw block device has an immutable identity. Local
// block zero is reserved for this label and mirrored storage metadata, so data
// blocks start at local block one on every device.
struct DeviceLabel {
  std::uint64_t magic = kDeviceLabelMagic;
  std::uint32_t version = kStorageFormatVersion;
  std::uint32_t header_bytes = kDirectIoAlignment;
  std::uint64_t storage_set_id = 0;
  std::uint64_t device_id = 0;
  std::uint64_t capacity_blocks = 0;
  std::uint32_t device_count = 0;
  std::uint32_t block_bytes = kStorageBlockBytes;
  std::uint32_t checksum = 0;
};

struct RecordHeader {
  std::uint64_t magic = kRecordMagic;
  std::uint32_t version = kStorageFormatVersion;
  std::uint16_t header_bytes = 0;
  RecordKind kind = RecordKind::kValue;
  std::uint8_t db_id = 0;
  Digest digest{};
  std::uint32_t key_bytes = 0;
  std::uint32_t value_bytes = 0;
  std::uint32_t value_disk_bytes = 0;
  std::uint32_t total_disk_bytes = 0;
  std::uint64_t generation = 0;
  std::uint64_t replication_epoch = 1;
  std::uint64_t db_epoch = 1;
  std::uint64_t mutation_sequence = 0;
  std::uint64_t relocation_sequence = 0;
  std::uint64_t lsn = 0;
  std::uint64_t allocation_epoch = 0;
  std::uint32_t payload_checksum = 0;
  std::uint32_t header_checksum = 0;
};

// This page is mirrored in local block zero after the device label. Data blocks
// start at local block one. A FLUSHDB first advances and persists the selected
// database epoch; old records can then be forgotten from memory without
// writing per-key tombstones.
struct StorageMetadata {
  std::uint64_t magic = kMetadataMagic;
  std::uint32_t version = kStorageFormatVersion;
  std::uint32_t header_bytes = kDirectIoAlignment;
  std::array<std::uint64_t, kLogicalDatabaseCount> db_epochs{};
  std::uint32_t checksum = 0;
};

static_assert(sizeof(BlockHeader) <= kBlockHeaderBytes);
static_assert(sizeof(DeviceLabel) <= kDirectIoAlignment);
static_assert(sizeof(RecordHeader) <= kMaxRecordHeaderBytes);
static_assert(sizeof(StorageMetadata) <= kDirectIoAlignment);

constexpr std::size_t AlignDirect(std::size_t size) noexcept {
  return (size + kDirectIoAlignment - 1) & ~(kDirectIoAlignment - 1);
}

constexpr std::size_t AlignRecord(std::size_t size) noexcept {
  return (size + kRecordAlignment - 1) & ~(kRecordAlignment - 1);
}

constexpr std::size_t RecordHeaderBytes(std::size_t key_bytes) noexcept {
  return AlignRecord(sizeof(RecordHeader) + key_bytes);
}

constexpr std::size_t MaxKeyBytes() noexcept {
  return kMaxRecordHeaderBytes - sizeof(RecordHeader);
}

std::uint32_t Crc32c(std::span<const std::byte> bytes) noexcept;

void EncodeDeviceLabel(
    const DeviceLabel& label,
    std::span<std::byte, kDirectIoAlignment> output) noexcept;
bool DecodeDeviceLabel(
    std::span<const std::byte, kDirectIoAlignment> input,
    DeviceLabel* label) noexcept;

void EncodeBlockHeader(const BlockHeader& header,
                       std::span<std::byte, kBlockHeaderBytes> output) noexcept;
bool DecodeBlockHeader(std::span<const std::byte, kBlockHeaderBytes> input,
                       BlockHeader* header) noexcept;

void EncodeStorageMetadata(
    const StorageMetadata& metadata,
    std::span<std::byte, kDirectIoAlignment> output) noexcept;
bool DecodeStorageMetadata(
    std::span<const std::byte, kDirectIoAlignment> input,
    StorageMetadata* metadata) noexcept;

bool EncodeRecordHeader(const RecordHeader& header, std::string_view key,
                        std::span<std::byte> output) noexcept;
bool DecodeRecordHeader(std::span<const std::byte> input,
                        RecordHeader* header, std::string_view* key) noexcept;

}  // namespace keylane::storage
