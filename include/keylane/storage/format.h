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
// Version 2 makes transaction generations a physical block invariant. There
// is intentionally no reader for version-1 mixed records/TxCommit blocks.
inline constexpr std::uint32_t kStorageFormatVersion = 2;
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
inline constexpr std::uint64_t kReplicationFrameMagic =
    0x314c5045524c4bULL;  // KLREPL1
inline constexpr std::uint64_t kExtentManifestMagic =
    0x3154464e4d4c4bULL;                                              // KLMNFT1
inline constexpr std::uint64_t kHashValueMagic =
    0x3145554c4156484bULL;  // KHVALUE1
inline constexpr std::uint64_t kMaxStringBytes = 512ULL * 1024 * 1024;
// BITFIELD bounds the starting bit like Redis 7.2. A 64-bit field beginning
// at the final legal offset can extend through eight additional bytes.
inline constexpr std::uint64_t kMaxBitmapBytes = kMaxStringBytes + 8;
inline constexpr std::uint64_t kMaxRecordPayloadBytes = 2 * kMaxStringBytes;
inline constexpr std::uint8_t kExternalValueMask = 0x80;
inline constexpr std::uint32_t kExternalKeyMask = std::uint32_t{1} << 31;
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
  std::uint64_t magic_ = kMetadataPageMagic;
  std::uint32_t version_ = kStorageFormatVersion;
  MetadataPageKind kind_ = MetadataPageKind::kEpochs;
  std::uint16_t header_bytes_ = 0;
  std::uint32_t page_index_ = 0;
  std::uint32_t payload_bytes_ = 0;
  std::uint64_t generation_ = 0;
  std::uint32_t checksum_ = 0;
  std::uint32_t reserved_ = 0;
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
  std::array<std::uint8_t, 20> bytes_{};

  bool operator==(const Digest&) const noexcept = default;
};

inline std::uint64_t ScanCursorPrefix(const Digest& digest) noexcept {
  std::uint64_t prefix = 0;
  for (std::size_t i = 0; i < sizeof(prefix); ++i) {
    prefix = (prefix << 8) | digest.bytes_[i];
  }
  return prefix;
}

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
  kPayloadExtent = 2,
  // Runtime-only replication backlog. Recovery recognizes and reclaims these
  // blocks instead of treating them as primary data.
  kReplicationLog = 3,
  // Short-lived transaction generation: tagged keyed records and their
  // TxCommit decisions share this block class until the cleaner promotes the
  // committed winners to ordinary kRecords blocks with txid zero.
  kTransaction = 4,
};

enum class ReplicationEventKind : std::uint8_t {
  kMutation = 1,
  kTransaction = 2,
  kControl = 3,
};

enum class ReplicationFrameFlag : std::uint8_t {
  kFirst = 1U << 0,
  kLast = 1U << 1,
};

constexpr std::uint8_t operator|(ReplicationFrameFlag left,
                                 ReplicationFrameFlag right) noexcept {
  return static_cast<std::uint8_t>(left) | static_cast<std::uint8_t>(right);
}

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
  std::uint64_t magic_ = kBlockMagic;
  std::uint64_t block_id_ = kInvalidBlockId;
  std::uint32_t version_ = kStorageFormatVersion;
  std::uint32_t header_bytes_ = kBlockHeaderBytes;
  std::uint32_t block_bytes_ = kStorageBlockBytes;
  std::uint32_t writer_id_ = 0;
  std::uint64_t allocation_epoch_ = 0;
  std::uint32_t committed_bytes_ = kBlockHeaderBytes;
  std::uint32_t record_count_ = 0;
  std::uint64_t max_lsn_ = 0;
  // Incremented on every header write for this allocation, and the slot that
  // write lands in is its parity. Slot resolution compares this rather than
  // committed_bytes, which can tie. A flush advances committed_bytes by at
  // least one page, so this cannot exceed kStorageBlockBytes / 4096.
  std::uint32_t header_sequence_ = 0;
  std::uint32_t checksum_ = 0;
  std::uint32_t layout_worker_count_ = 0;
  BlockKind kind_ = BlockKind::kRecords;
  std::array<std::uint8_t, 3> reserved_{};
  std::uint32_t extent_index_ = 0;
  std::uint32_t extent_payload_bytes_ = 0;
  std::uint32_t extent_payload_checksum_ = 0;
  // Replication-log coordinates are deliberately separate from max_lsn_,
  // which is the physical primary-storage append order.
  std::uint64_t replication_log_epoch_ = 0;
  std::uint64_t first_replication_lsn_ = 0;
  std::uint64_t last_replication_lsn_ = 0;
  // Nonzero only for kTransaction.
  std::uint64_t tx_generation_ = 0;
};

// A logical replication event may span multiple frames and blocks. Every
// fragment shares one LSN. The receiver publishes the event only after seeing
// kLast, which keeps future large values atomic without retaining them whole
// in memory.
struct ReplicationFrameHeader {
  std::uint64_t magic_ = kReplicationFrameMagic;
  std::uint32_t version_ = kStorageFormatVersion;
  std::uint16_t header_bytes_ = 0;
  ReplicationEventKind kind_ = ReplicationEventKind::kMutation;
  std::uint8_t flags_ = 0;
  std::uint64_t lsn_ = 0;
  std::uint64_t partition_sequence_ = 0;
  std::uint32_t payload_bytes_ = 0;
  std::uint32_t total_disk_bytes_ = 0;
  std::uint32_t fragment_index_ = 0;
  std::uint16_t partition_id_ = 0;
  std::uint16_t reserved_ = 0;
  std::uint32_t payload_checksum_ = 0;
  std::uint32_t header_checksum_ = 0;
};

// Every configured file or raw block device has an immutable identity. Fixed
// metadata follows this label, and data begins at DataBlockBegin(capacity).
struct DeviceLabel {
  std::uint64_t magic_ = kDeviceLabelMagic;
  std::uint32_t version_ = kStorageFormatVersion;
  std::uint32_t header_bytes_ = kDirectIoAlignment;
  std::uint64_t storage_set_id_ = 0;
  std::uint64_t device_id_ = 0;
  std::uint64_t capacity_blocks_ = 0;
  std::uint32_t device_count_ = 0;
  std::uint32_t block_bytes_ = kStorageBlockBytes;
  std::uint32_t checksum_ = 0;
};

struct RecordHeader {
  std::uint64_t magic_ = kRecordMagic;
  std::uint32_t version_ = kStorageFormatVersion;
  std::uint16_t header_bytes_ = 0;
  RecordKind kind_ = RecordKind::kValue;
  std::uint8_t db_id_ = 0;
  ValueType value_type_ = ValueType::kNone;
  // Transient decoded form. This byte is zero on disk; the flags are encoded
  // in value_type_ and key_bytes_. Keeping them as bit fields preserves the
  // fixed on-disk header layout.
  bool external_ : 1 = false;
  bool key_external_ : 1 = false;
  Digest digest_{};
  std::uint32_t key_bytes_ = 0;
  // Redis-visible bytes/cardinality.
  std::uint32_t logical_size_ = 0;
  // Physical payload following this header. For an out-of-index key, an
  // inline payload is key || value; an external payload is one manifest for
  // the same logical concatenation. Small keys remain in the header and an
  // external payload then contains only the value.
  std::uint32_t payload_bytes_ = 0;
  std::uint32_t total_disk_bytes_ = 0;
  // Multi-key transaction id, or 0 for a standalone write. Recovery keeps a
  // tagged record only if it also finds the transaction's kTxCommit record;
  // untagged records are kept unconditionally. (This slot was `generation`,
  // the original newest-wins ordinal, orphaned when replication introduced
  // the (replication_epoch, mutation_sequence) order.)
  std::uint64_t txid_ = 0;
  std::uint64_t replication_epoch_ = 1;
  std::uint64_t db_epoch_ = 1;
  std::uint64_t mutation_sequence_ = 0;
  // Absolute Unix time in milliseconds. Zero means the value does not expire.
  std::uint64_t expire_at_ms_ = 0;
  std::uint64_t lsn_ = 0;
  std::uint64_t allocation_epoch_ = 0;
  std::uint32_t payload_checksum_ = 0;
  std::uint32_t header_checksum_ = 0;
};

struct ExtentManifestHeader {
  std::uint64_t magic_ = kExtentManifestMagic;
  std::uint32_t version_ = kStorageFormatVersion;
  std::uint32_t extent_count_ = 0;
};

struct ExtentRef {
  std::uint64_t block_id_ = kInvalidBlockId;
  std::uint64_t allocation_epoch_ = 0;
  std::uint32_t payload_bytes_ = 0;
  std::uint32_t payload_checksum_ = 0;
};


inline constexpr std::size_t kExtentPayloadBytes =
    kStorageBlockBytes - kBlockHeaderBytes;
inline constexpr std::size_t kMaxStringExtents =
    (kMaxRecordPayloadBytes + kExtentPayloadBytes - 1) / kExtentPayloadBytes;

static_assert(sizeof(BlockHeader) <= kBlockHeaderBytes);
static_assert(sizeof(DeviceLabel) <= kDirectIoAlignment);
static_assert(sizeof(MetadataPageHeader) < kDirectIoAlignment);
static_assert(kMetadataPagePayloadBytes % sizeof(std::uint64_t) == 0);
static_assert(sizeof(RecordHeader) <= kMaxRecordHeaderBytes);
static_assert(sizeof(RecordHeader) == 120);
static_assert(sizeof(ReplicationFrameHeader) == 56);
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

constexpr std::size_t MaxKeyBytes() noexcept { return kMaxStringBytes; }

constexpr std::size_t MaxInlineKeyBytes() noexcept {
  return kMaxRecordHeaderBytes - sizeof(RecordHeader);
}

inline constexpr std::size_t kDefaultInlineKeyBytes = MaxInlineKeyBytes();

constexpr std::size_t ExtentManifestBytes(std::size_t logical_bytes) noexcept {
  return sizeof(ExtentManifestHeader) +
         ((logical_bytes + kExtentPayloadBytes - 1) / kExtentPayloadBytes) *
             sizeof(ExtentRef);
}

constexpr std::size_t RecordHeaderBytes(std::size_t key_bytes,
                                        bool key_external) noexcept {
  return AlignRecord(sizeof(RecordHeader) + (key_external ? 0 : key_bytes));
}

static_assert(RecordHeaderBytes(kMaxStringBytes, true) ==
              AlignRecord(sizeof(RecordHeader)));

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

bool EncodeReplicationFrameHeader(
    const ReplicationFrameHeader& header,
    std::span<std::byte, sizeof(ReplicationFrameHeader)> output) noexcept;
bool DecodeReplicationFrameHeader(std::span<const std::byte> input,
                                  ReplicationFrameHeader* header) noexcept;

}  // namespace keylane::storage
