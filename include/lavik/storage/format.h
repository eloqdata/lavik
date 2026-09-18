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

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <string_view>

namespace lavik::storage {

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
// Pre-deployment format changes directly replace version 1. Compatibility
// with earlier development media is intentionally unsupported because its
// missing system-state lineage cannot be inferred safely. Lavik signatures
// also reject media bearing obsolete development signatures.
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
    0x314c42414c564cULL;                                             // LVLABL1
inline constexpr std::uint64_t kBlockMagic = 0x3130304b4c42564cULL;  // LVBLK001
inline constexpr std::uint64_t kRecordMagic =
    0x313030434552564cULL;  // LVREC001
inline constexpr std::uint64_t kReplicationFrameMagic =
    0x314c504552564cULL;  // LVREPL1
inline constexpr std::uint64_t kExtentManifestMagic =
    0x3154464e4d564cULL;  // LVMNFT1
inline constexpr std::uint64_t kHashValueMagic =
    0x3145554c4156484cULL;  // LHVALUE1
inline constexpr std::uint64_t kMaxStringBytes = 512ULL * 1024 * 1024;
// BITFIELD bounds the starting bit like Redis 7.2. A 64-bit field beginning
// at the final legal offset can extend through eight additional bytes.
inline constexpr std::uint64_t kMaxBitmapBytes = kMaxStringBytes + 8;
inline constexpr std::uint64_t kMaxRecordPayloadBytes = 2 * kMaxStringBytes;
inline constexpr std::uint64_t kMetadataPageMagic =
    0x31504154454d564cULL;  // LVMETAP1
inline constexpr std::uint64_t kSystemStateRootMagic =
    0x3152545353564cULL;  // LVSSTR1
inline constexpr std::uint32_t kLogicalStorageShards = 16384;
inline constexpr std::uint8_t kLogicalDatabaseCount = 16;
inline constexpr std::uint64_t kDeviceLabelOffset = 0;

enum class MetadataPageKind : std::uint16_t {
  kEpochs = 1,
  kScanBitmap = 2,
  kCheckpointBitmap = 3,
  kSystemState = 4,
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
    kLogicalDatabaseCount + kLogicalStorageShards + 4;
inline constexpr std::size_t kCheckpointGenerationIndex =
    kLogicalDatabaseCount + kLogicalStorageShards;
inline constexpr std::size_t kCheckpointConsumedGenerationIndex =
    kCheckpointGenerationIndex + 1;
inline constexpr std::size_t kCheckpointBlockCountIndex =
    kCheckpointGenerationIndex + 2;
inline constexpr std::size_t kCheckpointEntryCountIndex =
    kCheckpointGenerationIndex + 3;
inline constexpr std::size_t kEpochMetadataBytes =
    kEpochValueCount * sizeof(std::uint64_t);
inline constexpr std::size_t kEpochMetadataPageCount =
    (kEpochMetadataBytes + kMetadataPagePayloadBytes - 1) /
    kMetadataPagePayloadBytes;
static_assert(kEpochMetadataPageCount ==
                  ((kLogicalDatabaseCount + kLogicalStorageShards) *
                       sizeof(std::uint64_t) +
                   kMetadataPagePayloadBytes - 1) /
                      kMetadataPagePayloadBytes,
              "checkpoint root must fit existing fixed-metadata pages");
inline constexpr std::uint64_t kEpochMetadataOffset = kDirectIoAlignment;
// Keep the process-global recovery root at a capacity-independent address.
// Capacity-sized bitmap ranges may grow in future formats without relocating
// the bootstrap pointer needed before ordinary recovery begins.
inline constexpr std::uint64_t kSystemStateMetadataOffset =
    kEpochMetadataOffset + kEpochMetadataPageCount * 2 * kDirectIoAlignment;
inline constexpr std::uint64_t kScanBitmapMetadataOffset =
    kSystemStateMetadataOffset + 2 * kDirectIoAlignment;

constexpr std::size_t ScanBitmapBytes(std::uint64_t capacity_blocks) noexcept {
  return static_cast<std::size_t>((capacity_blocks + 7) / 8);
}

constexpr std::size_t ScanBitmapPageCount(
    std::uint64_t capacity_blocks) noexcept {
  return (ScanBitmapBytes(capacity_blocks) + kMetadataPagePayloadBytes - 1) /
         kMetadataPagePayloadBytes;
}

// Checkpoint discovery uses the same capacity-derived coverage as the
// allocation bitmap but a separate A/B page range.
inline constexpr std::uint64_t CheckpointBitmapMetadataOffset(
    std::uint64_t capacity_blocks) noexcept {
  return kScanBitmapMetadataOffset +
         ScanBitmapPageCount(capacity_blocks) * 2 * kDirectIoAlignment;
}

constexpr std::uint64_t FixedMetadataBytes(
    std::uint64_t capacity_blocks) noexcept {
  return CheckpointBitmapMetadataOffset(capacity_blocks) +
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
  std::uint64_t value_ = 0;

  bool operator==(const Digest&) const noexcept = default;
};

inline std::uint64_t ScanCursorPrefix(const Digest& digest) noexcept {
  return digest.value_;
}

struct DigestHash {
  std::size_t operator()(const Digest& digest) const noexcept;
};

using DigestSeed = std::array<std::uint8_t, 16>;

// Returns the process-wide SipHash seed. Storage recovery may replace it with
// the seed from a clean-shutdown checkpoint before any recovered key is
// hashed; callers must not mutate hashing state after request serving starts.
const DigestSeed& CurrentDigestSeed() noexcept;
void RestoreDigestSeed(const DigestSeed& seed) noexcept;

// Returns a SipHash-1-2 fingerprint under CurrentDigestSeed(). A digest may be
// persisted only together with that seed; ordinary records remain independent
// of runtime hashing and cold recovery can therefore choose a fresh seed.
Digest ComputeDigest(std::string_view key) noexcept;
// Explicit-seed SipHash-1-2 for durable routing. The caller persists the seed
// and routing algorithm version with the owning object; this overload never
// reads or changes the process-wide lookup seed.
Digest ComputeDigest(std::string_view key, const DigestSeed& seed) noexcept;
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
  // Value 3 was used by an unreleased runtime-backlog experiment. It remains
  // unassigned so the on-disk enum values do not move.
  // Short-lived transaction generation: tagged keyed records and their
  // TxCommit decisions share this block class until the cleaner promotes the
  // committed winners to ordinary kRecords blocks with txid zero.
  kTransaction = 4,
  // A shutdown checkpoint is published only after all of its index blocks and
  // the checkpoint bitmap naming them are durable. These blocks are recovery
  // accelerators, never authoritative user data.
  kCheckpointIndex = 5,
};

enum class ReplicationEventKind : std::uint8_t {
  kMutation = 1,
  kTransaction = 2,
  kControl = 3,
  // Runtime-only commands that have no durable keyspace after-image, such as
  // PUBLISH. They live in online/full-sync memory streams and vanish at
  // restart with the replication history.
  kEphemeral = 4,
  // A durable node-global Function catalog transition. The payload remains
  // the original Redis FUNCTION command; this kind only keeps it distinct
  // from runtime-only PUBLISH events in backlog policy and observability.
  kCatalogMutation = 5,
};

enum class ReplicationFrameFlag : std::uint8_t {
  kFirst = 1U << 0,
  kLast = 1U << 1,
};

constexpr std::uint8_t operator|(ReplicationFrameFlag left,
                                 ReplicationFrameFlag right) noexcept {
  return static_cast<std::uint8_t>(left) | static_cast<std::uint8_t>(right);
}

// Stable on-disk Redis value type identifiers shared by write, recovery, and
// expiration paths. Keep the numeric assignments stable across releases.
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
  std::array<std::uint64_t, 3> reserved_runtime_{};
  // Nonzero for kTransaction and kCheckpointIndex. For checkpoint blocks it
  // identifies the metadata generation that may make them live.
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
  // Derived during encoding and decoding; this does not occupy a durable
  // field. Keeping it in the decoded form avoids duplicating framing
  // arithmetic throughout storage callers.
  std::uint16_t header_bytes_ = 0;
  RecordKind kind_ = RecordKind::kValue;
  std::uint8_t db_id_ = 0;
  ValueType value_type_ = ValueType::kNone;
  bool external_ = false;
  bool key_external_ = false;
  // A root and its independently indexed groups preserve the parent Redis
  // collection type, but are not interchangeable compact values. Group
  // identity lives outside the payload so recovery and GC can identify an
  // extent-backed group without reading its potentially very large value.
  // Hash/Set and Sorted Set member groups use a high-bit hash prefix; List
  // and Sorted Set ordered pages use a nonzero stable page id in group_prefix_
  // with zero prefix bits. Root and auxiliary markers are mutually exclusive
  // and apply to all four collection types.
  bool grouped_ = false;
  bool auxiliary_group_ = false;
  // Routing retirement is authoritative header metadata. Recovery must not
  // read an obsolete group's already-reclaimed value extents merely to learn
  // whether this identity still owns a range. Only auxiliary records use it.
  bool group_retired_ = false;
  std::uint64_t group_incarnation_ = 0;
  std::uint64_t group_prefix_ = 0;
  std::uint8_t group_prefix_bits_ = 0;
  // An auxiliary command nested inside EXEC/Lua needs both its enclosing
  // transaction and its own batch decision. Zero denotes a single-decision
  // group or a GC-promoted unconditional record.
  std::uint64_t group_batch_txid_ = 0;
  std::uint32_t key_bytes_ = 0;
  // Redis-visible bytes/cardinality.
  std::uint32_t logical_size_ = 0;
  // Physical payload following this header. For an out-of-index key, an
  // inline payload is key || value; an external payload is one manifest for
  // the same logical concatenation. Small keys remain in the header and an
  // external payload then contains only the value.
  std::uint32_t payload_bytes_ = 0;
  // Derived from header_bytes_ and payload_bytes_; not stored durably.
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

  bool operator==(const ExtentRef&) const noexcept = default;
};

// The only mutable pointer in fixed system metadata. The referenced manifest
// is a normal COW payload extent containing the complete Function-catalog
// extent list and latest promotion state. Mirroring this compact root on every
// configured device makes a generation committed only when all devices expose
// the same valid value.
struct SystemStateRoot {
  std::uint64_t magic_ = kSystemStateRootMagic;
  std::uint32_t version_ = kStorageFormatVersion;
  std::uint32_t root_bytes_ = 0;
  std::uint64_t generation_ = 0;
  ExtentRef manifest_{};
  std::uint64_t manifest_bytes_ = 0;

  bool operator==(const SystemStateRoot&) const noexcept = default;
};

static_assert(sizeof(SystemStateRoot) <= kMetadataPagePayloadBytes);

inline constexpr std::size_t kExtentPayloadBytes =
    kStorageBlockBytes - kBlockHeaderBytes;
inline constexpr std::size_t kMaxStringExtents =
    (kMaxRecordPayloadBytes + kExtentPayloadBytes - 1) / kExtentPayloadBytes;

static_assert(sizeof(BlockHeader) <= kBlockHeaderBytes);
static_assert(sizeof(DeviceLabel) <= kDirectIoAlignment);
static_assert(sizeof(MetadataPageHeader) < kDirectIoAlignment);
static_assert(kMetadataPagePayloadBytes % sizeof(std::uint64_t) == 0);
static_assert(sizeof(ReplicationFrameHeader) == 56);
static_assert(sizeof(ExtentManifestHeader) == 16);
static_assert(sizeof(ExtentRef) == 24);

constexpr std::size_t AlignDirect(std::size_t size) noexcept {
  return (size + kDirectIoAlignment - 1) & ~(kDirectIoAlignment - 1);
}

constexpr std::size_t AlignRecord(std::size_t size) noexcept {
  return (size + kRecordAlignment - 1) & ~(kRecordAlignment - 1);
}

// The durable record prefix stores fields at explicit offsets rather than
// copying RecordHeader's C++ object representation. txid and expiration are
// sparse extensions, so the overwhelmingly common standalone non-expiring
// record pays only for the 72-byte base. An auxiliary group adds its
// incarnation and routing identity, covered by the header checksum.
inline constexpr std::size_t kRecordHeaderBaseBytes = 72;
inline constexpr std::size_t kRecordHeaderOptionalBytes = 8;
inline constexpr std::size_t kRecordAuxiliaryGroupIdentityBytes = 32;
inline constexpr std::size_t kMaxRecordFixedHeaderBytes =
    kRecordHeaderBaseBytes + 2 * kRecordHeaderOptionalBytes;
inline constexpr std::size_t kMaxAuxiliaryGroupFixedHeaderBytes =
    kMaxRecordFixedHeaderBytes + kRecordAuxiliaryGroupIdentityBytes;

constexpr std::size_t RecordFixedHeaderBytes(
    bool has_txid, bool has_expiry, bool auxiliary_group = false) noexcept {
  return kRecordHeaderBaseBytes + (has_txid ? kRecordHeaderOptionalBytes : 0) +
         (has_expiry ? kRecordHeaderOptionalBytes : 0) +
         (auxiliary_group ? kRecordAuxiliaryGroupIdentityBytes : 0);
}

constexpr std::size_t RecordHeaderBytes(std::size_t key_bytes,
                                        bool key_external = false,
                                        bool has_txid = false,
                                        bool has_expiry = false,
                                        bool auxiliary_group = false) noexcept {
  return AlignRecord(
      RecordFixedHeaderBytes(has_txid, has_expiry, auxiliary_group) +
      (key_external ? 0 : key_bytes));
}

constexpr std::size_t MaxKeyBytes() noexcept { return kMaxStringBytes; }
// The storage API and recovery decoder must enforce the same durable key
// boundary; protocol-specific argument limits are intentionally not enough.
constexpr bool ValidRecordKeySize(std::size_t bytes) noexcept {
  return bytes <= MaxKeyBytes();
}

constexpr std::size_t MaxInlineKeyBytes() noexcept {
  // The configured limit must remain valid for a transaction record carrying
  // an expiration, even though ordinary records could use the extension space
  // for a slightly larger inline key.
  return kMaxRecordHeaderBytes - kMaxRecordFixedHeaderBytes;
}

inline constexpr std::size_t kDefaultInlineKeyBytes = MaxInlineKeyBytes();

constexpr std::size_t ExtentManifestBytes(std::size_t logical_bytes) noexcept {
  return sizeof(ExtentManifestHeader) +
         ((logical_bytes + kExtentPayloadBytes - 1) / kExtentPayloadBytes) *
             sizeof(ExtentRef);
}

static_assert(RecordHeaderBytes(kMaxStringBytes, true) ==
              kRecordHeaderBaseBytes);

std::uint32_t Crc32c(std::span<const std::byte> bytes) noexcept;
// Redis-compatible CRC64 used for the Function dump durability token.
std::uint64_t Crc64(std::span<const std::byte> bytes) noexcept;

void EncodeSystemStateRoot(const SystemStateRoot& root,
                           std::span<std::byte> output) noexcept;
bool DecodeSystemStateRoot(std::span<const std::byte> input,
                           SystemStateRoot* root) noexcept;

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

}  // namespace lavik::storage
