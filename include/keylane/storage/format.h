#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <string_view>

namespace keylane::storage {

inline constexpr std::size_t kDirectIoAlignment = 4096;
// Two header slot pages per block, written alternately so a torn header
// write can never destroy the last valid header. Records start after both.
inline constexpr std::size_t kBlockHeaderSlotBytes = kDirectIoAlignment;
inline constexpr std::size_t kBlockHeaderSlots = 2;
inline constexpr std::size_t kBlockHeaderBytes =
    kBlockHeaderSlotBytes * kBlockHeaderSlots;
inline constexpr std::size_t kRecordAlignment = 8;
inline constexpr std::size_t kMaxRecordHeaderBytes = kDirectIoAlignment;
inline constexpr std::size_t kStorageBlockBytes = 8 * 1024 * 1024;
inline constexpr std::uint32_t kStorageFormatVersion = 1;
inline constexpr unsigned kLocalBlockIdBits = 27;
inline constexpr std::uint64_t kLocalBlockIdLimit = std::uint64_t{1}
                                                    << kLocalBlockIdBits;
inline constexpr std::uint64_t kLocalBlockIdMask = kLocalBlockIdLimit - 1;
inline constexpr std::uint64_t kDeviceIdLimit = std::uint64_t{1}
                                                << (64 - kLocalBlockIdBits);
inline constexpr std::uint64_t kInvalidBlockId =
    std::numeric_limits<std::uint64_t>::max();
inline constexpr std::uint64_t kDeviceLabelMagic =
    0x314c42414c4c4bULL;                                             // KLLABL1
inline constexpr std::uint64_t kBlockMagic = 0x314b4c424c4f434bULL;  // KCOLBLK1
inline constexpr std::uint64_t kRecordMagic =
    0x314b4c5245434f52ULL;  // ROCERLK1
inline constexpr std::uint64_t kExtentManifestMagic =
    0x3154464e4d4c4bULL;  // KLMNFT1
inline constexpr std::uint64_t kMaxStringBytes = 512ULL * 1024 * 1024;
inline constexpr std::uint8_t kExternalValueMask = 0x80;
inline constexpr std::uint64_t kMetadataPageMagic =
    0x31475041544d4c4bULL;  // KLMETAP1
inline constexpr std::uint32_t kLogicalStorageShards = 16384;
inline constexpr std::uint8_t kLogicalDatabaseCount = 16;
inline constexpr std::uint64_t kDeviceLabelOffset = 0;

enum class MetadataPageKind : std::uint16_t {
  kEpochs = 1,
  kScanBitmap = 2,
};

struct MetadataPageHeader {
  std::uint64_t magic = kMetadataPageMagic;
  std::uint32_t version = kStorageFormatVersion;
  MetadataPageKind kind = MetadataPageKind::kEpochs;
  std::uint16_t header_bytes = 0;
  std::uint32_t page_index = 0;
  std::uint32_t payload_bytes = 0;
  std::uint64_t generation = 0;
  std::uint32_t checksum = 0;
  std::uint32_t reserved = 0;
};

inline constexpr std::size_t kMetadataPagePayloadBytes =
    kDirectIoAlignment - sizeof(MetadataPageHeader);
inline constexpr std::size_t kEpochValueCount =
    kLogicalDatabaseCount + kLogicalStorageShards;
inline constexpr std::size_t kEpochMetadataBytes =
    kEpochValueCount * sizeof(std::uint64_t);
inline constexpr std::size_t kEpochMetadataPageCount =
    (kEpochMetadataBytes + kMetadataPagePayloadBytes - 1) /
    kMetadataPagePayloadBytes;
inline constexpr std::uint64_t kEpochMetadataOffset = kDirectIoAlignment;
inline constexpr std::uint64_t kScanBitmapMetadataOffset =
    kEpochMetadataOffset + kEpochMetadataPageCount * 2 * kDirectIoAlignment;

constexpr std::size_t ScanBitmapBytes(std::uint64_t capacity_blocks) noexcept {
  return static_cast<std::size_t>((capacity_blocks + 7) / 8);
}

constexpr std::size_t ScanBitmapPageCount(
    std::uint64_t capacity_blocks) noexcept {
  return (ScanBitmapBytes(capacity_blocks) + kMetadataPagePayloadBytes - 1) /
         kMetadataPagePayloadBytes;
}

constexpr std::uint64_t FixedMetadataBytes(
    std::uint64_t capacity_blocks) noexcept {
  return kScanBitmapMetadataOffset +
         ScanBitmapPageCount(capacity_blocks) * 2 * kDirectIoAlignment;
}

constexpr std::uint32_t DataBlockBegin(std::uint64_t capacity_blocks) noexcept {
  return static_cast<std::uint32_t>(
      (FixedMetadataBytes(capacity_blocks) + kStorageBlockBytes - 1) /
      kStorageBlockBytes);
}

constexpr std::uint64_t MetadataPageSlotOffset(std::uint64_t base_offset,
                                               std::size_t page_index,
                                               unsigned slot) noexcept {
  return base_offset + (static_cast<std::uint64_t>(page_index) * 2 + slot) *
                           kDirectIoAlignment;
}

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
  // A multi-key transaction's commit decision: header-only, txid names the
  // committed transaction, keyless (key_bytes == 0). Never enters the index;
  // recovery keeps txid-tagged data records only when it finds this.
  kTxCommit = 3,
};

enum class BlockKind : std::uint8_t {
  kRecords = 1,
  kValueExtent = 2,
};

// Stable on-disk Redis value type identifiers. Only strings are implemented
// today; reserving the remaining top-level types keeps expiration and recovery
// metadata generic as their command implementations are added.
enum class ValueType : std::uint8_t {
  kNone = 0,
  kString = 1,
  kList = 2,
  kSet = 3,
  kSortedSet = 4,
  kHash = 5,
  kStream = 6,
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
  // Incremented on every header write for this allocation, and the slot that
  // write lands in is its parity. Slot resolution compares this rather than
  // committed_bytes, which can tie. A flush advances committed_bytes by at
  // least one page, so this cannot exceed kStorageBlockBytes / 4096.
  std::uint32_t header_sequence = 0;
  std::uint32_t checksum = 0;
  std::uint32_t layout_worker_count = 0;
  BlockKind kind = BlockKind::kRecords;
  std::array<std::uint8_t, 3> reserved{};
  std::uint32_t extent_index = 0;
  std::uint32_t extent_payload_bytes = 0;
  std::uint32_t extent_payload_checksum = 0;
};

// Every configured file or raw block device has an immutable identity. Fixed
// metadata follows this label, and data begins at DataBlockBegin(capacity).
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
  ValueType value_type = ValueType::kNone;
  // Transient decoded form. This byte is zero on disk; external is encoded in
  // the high bit of value_type.
  bool external = false;
  Digest digest{};
  std::uint32_t key_bytes = 0;
  // Redis-visible size: bytes for String and cardinality for collections.
  std::uint64_t logical_size = 0;
  // Physical payload following this header. External roots store a manifest.
  std::uint32_t payload_bytes = 0;
  std::uint32_t total_disk_bytes = 0;
  // Multi-key transaction id, or 0 for a standalone write. Recovery keeps a
  // tagged record only if it also finds the transaction's kTxCommit record;
  // untagged records are kept unconditionally. (This slot was `generation`,
  // the original newest-wins ordinal, orphaned when replication introduced
  // the (replication_epoch, mutation_sequence) order.)
  std::uint64_t txid = 0;
  std::uint64_t replication_epoch = 1;
  std::uint64_t db_epoch = 1;
  std::uint64_t mutation_sequence = 0;
  std::uint64_t relocation_sequence = 0;
  // Absolute Unix time in milliseconds. Zero means the value does not expire.
  std::uint64_t expire_at_ms = 0;
  std::uint64_t lsn = 0;
  std::uint64_t allocation_epoch = 0;
  std::uint32_t payload_checksum = 0;
  std::uint32_t header_checksum = 0;
};

struct ExtentManifestHeader {
  std::uint64_t magic = kExtentManifestMagic;
  std::uint32_t version = kStorageFormatVersion;
  std::uint32_t extent_count = 0;
};

struct ExtentRef {
  std::uint64_t block_id = kInvalidBlockId;
  std::uint64_t allocation_epoch = 0;
  std::uint32_t payload_bytes = 0;
  std::uint32_t payload_checksum = 0;
};

inline constexpr std::size_t kExtentPayloadBytes =
    kStorageBlockBytes - kBlockHeaderBytes;
inline constexpr std::size_t kMaxStringExtents =
    (kMaxStringBytes + kExtentPayloadBytes - 1) / kExtentPayloadBytes;

static_assert(sizeof(BlockHeader) <= kBlockHeaderBytes);
static_assert(sizeof(DeviceLabel) <= kDirectIoAlignment);
static_assert(sizeof(MetadataPageHeader) < kDirectIoAlignment);
static_assert(kMetadataPagePayloadBytes % sizeof(std::uint64_t) == 0);
static_assert(sizeof(RecordHeader) <= kMaxRecordHeaderBytes);
static_assert(sizeof(ExtentManifestHeader) == 16);
static_assert(sizeof(ExtentRef) == 24);

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
bool DecodeDeviceLabel(std::span<const std::byte, kDirectIoAlignment> input,
                       DeviceLabel* label) noexcept;

void EncodeMetadataPage(
    MetadataPageKind kind, std::uint32_t page_index, std::uint64_t generation,
    std::span<const std::byte> payload,
    std::span<std::byte, kDirectIoAlignment> output) noexcept;
bool DecodeMetadataPage(std::span<const std::byte, kDirectIoAlignment> input,
                        MetadataPageKind expected_kind,
                        std::uint32_t expected_page_index,
                        std::uint64_t* generation,
                        std::span<std::byte> payload) noexcept;

// Encode into / decode from a single header slot page.
void EncodeBlockHeader(
    const BlockHeader& header,
    std::span<std::byte, kBlockHeaderSlotBytes> output) noexcept;
bool DecodeBlockHeader(std::span<const std::byte, kBlockHeaderSlotBytes> input,
                       BlockHeader* header) noexcept;

// Decode the winning slot from a block's full header region: the valid slot
// with the larger (allocation_epoch, header_sequence). Returns the winning
// slot index in `active_slot`, or false when neither slot is valid.
bool DecodeBlockHeaderPages(std::span<const std::byte, kBlockHeaderBytes> input,
                            BlockHeader* header,
                            std::uint8_t* active_slot = nullptr) noexcept;

bool EncodeRecordHeader(const RecordHeader& header, std::string_view key,
                        std::span<std::byte> output) noexcept;
bool DecodeRecordHeader(std::span<const std::byte> input, RecordHeader* header,
                        std::string_view* key) noexcept;

}  // namespace keylane::storage
