#pragma once

#include <fcntl.h>
#include <linux/fs.h>

#include "keylane/storage/engine.h"
#ifdef BLOCK_SIZE
#undef BLOCK_SIZE
#endif
#include <sys/ioctl.h>
#include <sys/random.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <coroutine>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <limits>
#include <mutex>
#include <new>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "celer/io/storage.h"
#include "celer/runtime/concurrentqueue.h"
#include "celer/runtime/cross_core.h"
#include "celer/runtime/sync.h"
#include "celer/runtime/worker.h"
#include "keylane/storage/format.h"
#include "keylane/storage/scan_hash_map.h"
#include "keylane/tx/tx_shard.h"
#include "spdlog/spdlog.h"

namespace keylane::storage {

using celer::AsyncMutex;
using celer::AsyncNotification;
using celer::CoroutineBarrier;
using celer::FixedBuffer;
using celer::FixedFile;
using celer::Task;
using celer::UnlockGuard;
using celer::Worker;

using ExtentManifest = std::shared_ptr<const std::vector<ExtentRef>>;

struct RecordLocation {
  std::uint64_t block_id_ = 0;
  std::uint64_t replication_epoch_ = 1;
  std::uint64_t mutation_sequence_ = 0;
  std::uint64_t allocation_epoch_ = 0;
  std::uint64_t expire_at_ms_ = 0;
  std::uint64_t logical_size_ = 0;
  std::uint32_t record_offset_ = 0;
  std::uint32_t total_disk_bytes_ = 0;
  std::uint32_t payload_bytes_ = 0;
  std::uint32_t relocation_sequence_ = 0;
  // Owner in the current process topology. Unlike the persisted writer_id,
  // this must always be in [0, worker_count).
  std::uint16_t block_owner_ = 0;
  // Packed flags: one byte for all five.
  bool in_memory_ : 1 = false;
  bool external_ : 1 = false;
  bool key_external_ : 1 = false;
  // True while an older, still-unexpired value of this key may survive on
  // disk. Erasing this entry then would un-suppress that copy: recovery
  // picks the newest surviving record, so the key would resurrect with the
  // stale value. Propagates through every overwrite — tombstones included,
  // since a superseded tombstone leaves the disk like any dead record — and
  // is rebuilt exactly during recovery, which sees every surviving record.
  bool shielding_ : 1 = false;
  // Tomb-raider round state: set on candidates (tombstones, shielded values)
  // when a round begins, cleared when the sweep finds an older on-disk
  // record the entry still suppresses. Whatever survives the sweep
  // unclaimed proved nothing on disk needs it. False outside rounds, and
  // any overwrite resets it, exempting concurrently-touched keys.
  bool unclaimed_ : 1 = false;
  RecordKind kind_ = RecordKind::kValue;
  ValueType value_type_ = ValueType::kNone;

  bool SamePhysicalRecord(const RecordLocation& other) const noexcept {
    return block_id_ == other.block_id_ &&
           record_offset_ == other.record_offset_ &&
           allocation_epoch_ == other.allocation_epoch_;
  }
};

using RecordIndex = ScanHashMap<RecordLocation>;

static_assert(sizeof(RecordLocation) == 72);
static_assert(sizeof(RecordIndex::Entry) == 88);

inline bool IsNewer(const RecordLocation& candidate,
                    const RecordLocation& current) noexcept {
  if (candidate.replication_epoch_ != current.replication_epoch_) {
    return candidate.replication_epoch_ > current.replication_epoch_;
  }
  if (candidate.mutation_sequence_ != current.mutation_sequence_) {
    return candidate.mutation_sequence_ > current.mutation_sequence_;
  }
  if (candidate.relocation_sequence_ != current.relocation_sequence_) {
    return candidate.relocation_sequence_ > current.relocation_sequence_;
  }
  return false;
}

// Deterministic fault injection for crash-safety tests. Arming is naming the
// point in the KEYLANE_CRASH_POINT environment variable; execution reaching
// that point then kills the process on the spot — no flush, no destructors —
// as if power had been cut, with exit code 86 so the test harness can tell a
// fired crash point from an accidental death. Debug-only: NDEBUG builds
// compile the whole mechanism away, so crash-safety scenarios must run
// against a non-NDEBUG server binary.
#ifndef NDEBUG
inline void MaybeCrashAt(const char* point) noexcept {
  static const char* const armed = std::getenv("KEYLANE_CRASH_POINT");
  if (armed != nullptr && std::strcmp(armed, point) == 0) {
    std::_Exit(86);
  }
}
#define KEYLANE_MAYBE_CRASH_AT(point) ::keylane::storage::MaybeCrashAt(point)

// Deterministic write-fault injection for rollback tests: a tagged write of
// the key named in KEYLANE_FAIL_TX_WRITE fails instead of appending.
inline bool MaybeFailTxWrite(std::string_view key) noexcept {
  static const char* const armed = std::getenv("KEYLANE_FAIL_TX_WRITE");
  return armed != nullptr && key == armed;
}
#define KEYLANE_MAYBE_FAIL_TX_WRITE(key) \
  ::keylane::storage::MaybeFailTxWrite(key)
#else
#define KEYLANE_MAYBE_CRASH_AT(point) ((void)0)
#define KEYLANE_MAYBE_FAIL_TX_WRITE(key) false
#endif

inline std::uint64_t UnixTimeMillis() noexcept {
  const auto value = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
  return value > 0 ? static_cast<std::uint64_t>(value) : 0;
}

inline bool IsExpired(const RecordLocation& location,
                      std::uint64_t now_ms) noexcept {
  return location.kind_ == RecordKind::kValue && location.expire_at_ms_ != 0 &&
         location.expire_at_ms_ <= now_ms;
}

inline absl::StatusOr<std::shared_ptr<const std::vector<ExtentRef>>>
DecodeManifest(std::span<const std::byte> payload, std::uint64_t logical_size) {
  if (payload.size() < sizeof(ExtentManifestHeader)) {
    return absl::Status(absl::StatusCode::kInternal,
                        "external value manifest is truncated");
  }
  ExtentManifestHeader header{};
  std::memcpy(&header, payload.data(), sizeof(header));
  if (header.magic_ != kExtentManifestMagic ||
      header.version_ != kStorageFormatVersion || header.extent_count_ == 0 ||
      header.extent_count_ > kMaxStringExtents ||
      payload.size() !=
          sizeof(header) + static_cast<std::size_t>(header.extent_count_) *
                               sizeof(ExtentRef)) {
    return absl::Status(absl::StatusCode::kInternal,
                        "invalid external value manifest");
  }
  auto refs = std::make_shared<std::vector<ExtentRef>>(header.extent_count_);
  std::memcpy(refs->data(), payload.data() + sizeof(header),
              refs->size() * sizeof(ExtentRef));
  std::uint64_t total = 0;
  for (const ExtentRef& ref : *refs) {
    if (ref.block_id_ == kInvalidBlockId || ref.allocation_epoch_ == 0 ||
        ref.payload_bytes_ == 0 || ref.payload_bytes_ > kExtentPayloadBytes ||
        total > kMaxRecordPayloadBytes - ref.payload_bytes_) {
      return absl::Status(absl::StatusCode::kInternal,
                          "invalid extent reference");
    }
    total += ref.payload_bytes_;
  }
  if (total != logical_size || total > kMaxRecordPayloadBytes) {
    return absl::Status(absl::StatusCode::kInternal,
                        "extent manifest logical size mismatch");
  }
  return std::shared_ptr<const std::vector<ExtentRef>>(std::move(refs));
}

inline std::string EncodeManifest(std::span<const ExtentRef> refs) {
  ExtentManifestHeader header{
      .magic_ = kExtentManifestMagic,
      .version_ = kStorageFormatVersion,
      .extent_count_ = static_cast<std::uint32_t>(refs.size())};
  std::string output(sizeof(header) + refs.size_bytes(), '\0');
  std::memcpy(output.data(), &header, sizeof(header));
  std::memcpy(output.data() + sizeof(header), refs.data(), refs.size_bytes());
  return output;
}

struct ActiveBlock {
  std::uint64_t block_id_ = 0;
  std::uint32_t writer_id_ = 0;
  std::uint32_t layout_worker_count_ = 0;
  std::uint64_t allocation_epoch_ = 0;
  std::uint32_t committed_bytes_ = kBlockHeaderBytes;
  std::uint32_t record_count_ = 0;
  std::uint64_t max_lsn_ = 0;
  std::uint16_t write_buffer_id_ = 0;
  std::byte* heap_buffer_ = nullptr;
  std::size_t heap_buffer_size_ = 0;
  BlockKind kind_ = BlockKind::kRecords;
  std::uint32_t extent_index_ = 0;
  std::uint32_t extent_payload_checksum_ = 0;
};

// State that exists only while a block is held in memory behind a staging
// buffer: the buffer itself and the bookkeeping the flusher needs. At most a
// handful of blocks per worker are in that state at once, while every
// allocated block carries a BlockState, so this lives in a side table instead
// of costing all of them 38 bytes.
struct StagingSlot {
  std::uint16_t write_buffer_id_ = 0;
  std::byte* heap_data_ = nullptr;
  std::size_t heap_data_size_ = 0;
  // Bytes already written and fdatasynced. Direct-I/O aligned, so appends
  // never land in a durable page and a flush only writes the new tail.
  std::uint32_t durable_bytes_ = kBlockHeaderBytes;
  std::uint32_t record_count_ = 0;
  std::uint64_t max_lsn_ = 0;
  // Sequence stamped into the last header write. Its parity picks the slot,
  // so the header slot needs no field of its own.
  std::uint32_t header_sequence_ = 0;
  std::uint16_t next_free_ = 0;
};

inline constexpr std::uint16_t kUnownedBlock =
    std::numeric_limits<std::uint16_t>::max();

// These live in a dense per-device array, so aligning to 32 keeps every entry
// inside one cache line rather than letting some straddle two.
//
// The array is shared: any worker can address any entry. Only `owner` may be
// read by a worker that does not own the block, which is why it alone is
// atomic. Everything else is the owner's exclusive property, reached only
// after FindBlockState has confirmed ownership.
struct alignas(32) BlockState {
  std::uint64_t allocation_epoch_ = 0;
  std::uint32_t committed_bytes_ = 0;
  std::uint32_t live_bytes_ = 0;
  std::uint32_t pins_ = 0;
  // Written only by the owner as it claims or releases the block; read by
  // anyone that needs to know where to dispatch. kUnownedBlock means free.
  std::atomic<std::uint16_t> owner_{kUnownedBlock};
  std::uint16_t writer_id_ = 0;
  std::uint16_t layout_worker_count_ = 0;
  // Index into WorkerStore::staging_slots, or 0 when the block has no staging
  // buffer. Ids are 1-based so zero can mean "none".
  std::uint16_t staging_slot_ = 0;
  // Bitfields rather than bools: eight of these would otherwise cost a byte
  // each and push the struct past a cache line.
  bool allocated_ : 1 = false;
  bool defrag_queued_ : 1 = false;
  bool defragging_ : 1 = false;
  bool freeing_ : 1 = false;
  bool in_memory_ : 1 = false;
  bool flush_queued_ : 1 = false;
  bool flush_in_progress_ : 1 = false;
  bool release_pending_ : 1 = false;
  BlockKind kind_ = BlockKind::kRecords;

  // The atomic member makes this non-assignable, and clearing an entry has to
  // publish the new owner last so no one observes a half-reset block.
  void Reset(std::uint16_t new_owner) noexcept {
    allocation_epoch_ = 0;
    committed_bytes_ = 0;
    live_bytes_ = 0;
    pins_ = 0;
    writer_id_ = 0;
    layout_worker_count_ = 0;
    staging_slot_ = 0;
    allocated_ = false;
    defrag_queued_ = false;
    defragging_ = false;
    freeing_ = false;
    in_memory_ = false;
    flush_queued_ = false;
    flush_in_progress_ = false;
    release_pending_ = false;
    kind_ = BlockKind::kRecords;
    owner_.store(new_owner, std::memory_order_release);
  }
};

// Every allocated block carries one of these for its whole life, and cold
// paths scan them in bulk, so keep two per cache line. Anything that only
// matters while a block is staged in memory belongs in StagingSlot, and
// anything only an extent block needs belongs in the recovery-scoped map.
static_assert(sizeof(BlockState) == 32);
static_assert(alignof(BlockState) == 32);

struct ExtentIdentity {
  std::uint32_t extent_index_ = 0;
  std::uint32_t payload_checksum_ = 0;
};

struct RecoveryRecord {
  Digest digest_{};
  std::string key_;
  std::uint8_t db_id_ = 0;
  // Multi-key transaction tag. Tagged records are parked until every
  // worker's scan has contributed its kTxCommit sightings, then applied only
  // if their transaction committed.
  std::uint64_t txid_ = 0;
  RecordLocation location_{};
  ExtentManifest extents_;
};

struct RecoveryBlock {
  ActiveBlock block_{};
};

struct RecoveryBatch {
  std::vector<RecoveryRecord> records_;
  std::vector<RecoveryBlock> blocks_;
};

struct RecoveryLiveReference {
  std::uint64_t block_id_ = 0;
  std::uint64_t allocation_epoch_ = 0;
  std::uint32_t bytes_ = 0;
  bool extent_ = false;
  std::uint32_t extent_index_ = 0;
  std::uint32_t extent_payload_checksum_ = 0;
};

// A relocated record cannot make its source block reclaimable until the
// destination block header durably covers this boundary. Keeping the fence
// independent of the in-memory index also makes later overwrites harmless:
// once this version is durable, recovery always has at least this copy or a
// newer relocation to choose from.
struct RelocationDurabilityFence {
  std::uint64_t block_id_ = 0;
  std::uint64_t allocation_epoch_ = 0;
  std::uint16_t block_owner_ = 0;
  std::uint32_t committed_bytes_ = 0;
};

// The accounting handle for a superseded record: enough to subtract it from
// its block's live_bytes once its replacement no longer needs it as the
// durable copy.
struct RetiredRecord {
  std::uint64_t block_id_ = 0;
  std::uint64_t allocation_epoch_ = 0;
  std::uint32_t total_disk_bytes_ = 0;
  std::uint16_t block_owner_ = 0;
  ExtentManifest dependent_extents_;
  std::shared_ptr<const std::vector<ExtentManifest>> extra_dependent_extents_;
};

// The index state a defrag relocation observed when it validated its source
// record. WriteRecordLocked can release the store-state lock while it waits
// for a block allocation. A client write can replace this key, or FLUSHDB and
// replica reset can replace the whole index, during that gap. Re-checking both
// the physical record and the population epochs before the append prevents a
// stale relocation from resurrecting either one.
struct RelocationSource {
  std::uint64_t db_epoch_ = 0;
  std::uint64_t replication_epoch_ = 0;
  std::uint64_t index_generation_ = 0;
  std::uint64_t block_id_ = 0;
  std::uint64_t allocation_epoch_ = 0;
  std::uint32_t record_offset_ = 0;

  bool Matches(const RecordLocation& location) const noexcept {
    return location.block_id_ == block_id_ &&
           location.allocation_epoch_ == allocation_epoch_ &&
           location.record_offset_ == record_offset_;
  }
};

// Back-pointer from a block to the index entries staged in its write buffer, so
// the flush completion can flip them to on-disk reads without re-hashing every
// key. The entry can outlive the index that owns it: FLUSHDB detaches every
// partition index for one database while blocks are still in flight. Recording
// which database generation produced the entry lets the completion detect that
// and skip the entry instead of following a pointer into a freed population.
struct RecordIdentity {
  RecordIndex::Entry* entry_ = nullptr;
  std::shared_ptr<const std::vector<ExtentRef>> retired_extents_;
  // The version this record superseded. Retired only when this record's
  // flush completes: until the replacement is durable, the old copy is the
  // only durable version of the key, and subtracting it from live_bytes any
  // earlier lets the block reach zero and be durably freed — a crash before
  // the flush then loses a value that had already been made durable.
  std::optional<RetiredRecord> retired_record_;
  // A kTxCommit record additionally carries every retirement of its
  // transaction: the superseded versions may only leave their blocks'
  // accounting once the commit itself is durable, since without the commit
  // recovery drops the replacements and must still find the old copies.
  std::shared_ptr<std::vector<RetiredRecord>> tx_retirements_;
  std::uint64_t index_generation_ = 0;
  std::uint8_t db_id_ = 0;
};

// One journaled write of an in-flight multi-key transaction, enough to put
// the index back exactly as it was: entries are address-stable, the key
// locks are still held, and routed retirements never fired.
struct TxUndoEntry {
  RecordIndex::Entry* entry_ = nullptr;
  std::optional<RecordLocation> previous_;
  ExtentManifest previous_extents_;
  std::uint8_t db_id_ = 0;
};

struct ReplicaValueStage {
  std::uint8_t db_id_ = 0;
  std::uint64_t db_epoch_ = 0;
  std::uint64_t mutation_sequence_ = 0;
  std::uint64_t expire_at_ms_ = 0;
  std::uint64_t logical_size_ = 0;
  std::uint32_t next_chunk_ = 0;
  std::uint32_t chunk_count_ = 0;
  ValueType value_type_ = ValueType::kNone;
  std::string key_;
  std::string value_;
};

// One partition's worth of entries taken out of service by FLUSHDB. The entries
// are unreachable to readers the moment the index is detached, but the blocks
// they occupy still count them as live until the reclaimer subtracts them.
struct DetachedIndex {
  RecordIndex index_;
  std::uint8_t db_id_ = 0;
};

inline bool IsZero(std::span<const std::byte> bytes) noexcept {
  return std::all_of(bytes.begin(), bytes.end(),
                     [](std::byte value) { return value == std::byte{0}; });
}

// Header writes alternate between the two slots, so the sequence a write
// stamps also names the slot it lands in. The first write of an allocation
// carries sequence 1 and goes to slot 0.
constexpr std::uint8_t HeaderSlot(std::uint32_t header_sequence) noexcept {
  return static_cast<std::uint8_t>(1 - (header_sequence & 1));
}

// Every flush pads its tail out to a direct-I/O page and restarts the next
// record on the following page, so committed data can contain zero-filled
// holes. Returns where the next record starts, or nullopt when the bytes are
// neither a record header nor valid padding.
inline std::optional<std::uint32_t> NextRecordOffset(
    const std::byte* block, std::uint32_t record_offset,
    std::uint32_t committed_bytes) noexcept {
  std::uint64_t magic = 0;
  std::memcpy(&magic, block + record_offset, sizeof(magic));
  if (magic == kRecordMagic) {
    return record_offset;
  }
  const std::uint32_t next_page =
      static_cast<std::uint32_t>(AlignDirect(record_offset + 1));
  if (next_page > committed_bytes ||
      !IsZero(std::span<const std::byte>(block + record_offset,
                                         next_page - record_offset))) {
    return std::nullopt;
  }
  return next_page;
}

inline void AtomicMax(std::atomic<std::uint64_t>* target,
                      std::uint64_t value) noexcept {
  std::uint64_t current = target->load(std::memory_order_relaxed);
  while (current < value && !target->compare_exchange_weak(
                                current, value, std::memory_order_relaxed)) {
  }
}

inline absl::Status ReadExactlyAt(int fd, std::span<std::byte> output,
                                  std::uint64_t offset) {
  std::size_t done = 0;
  while (done < output.size()) {
    const ssize_t read = ::pread(fd, output.data() + done, output.size() - done,
                                 static_cast<off_t>(offset + done));
    if (read < 0 && errno == EINTR) {
      continue;
    }
    if (read <= 0) {
      return absl::Status(absl::StatusCode::kInternal,
                          read == 0 ? "short device-label read"
                                    : "device-label read failed: " +
                                          std::string(std::strerror(errno)));
    }
    done += static_cast<std::size_t>(read);
  }
  return absl::OkStatus();
}

inline absl::Status WriteExactlyAt(int fd, std::span<const std::byte> input,
                                   std::uint64_t offset) {
  std::size_t done = 0;
  while (done < input.size()) {
    const ssize_t written =
        ::pwrite(fd, input.data() + done, input.size() - done,
                 static_cast<off_t>(offset + done));
    if (written < 0 && errno == EINTR) {
      continue;
    }
    if (written <= 0) {
      return absl::Status(
          absl::StatusCode::kInternal,
          "device-label write failed: " + std::string(std::strerror(errno)));
    }
    done += static_cast<std::size_t>(written);
  }
  return absl::OkStatus();
}

inline absl::StatusOr<std::optional<DeviceLabel>> ReadDeviceLabel(
    const std::string& path) {
  const int fd = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
  if (fd < 0) {
    return absl::Status(
        absl::StatusCode::kInternal,
        "open device label failed: " + path + ": " + std::strerror(errno));
  }
  std::array<std::byte, kDirectIoAlignment> page{};
  absl::Status status = ReadExactlyAt(fd, page, kDeviceLabelOffset);
  const int close_error = ::close(fd);
  if (!status.ok()) {
    return status;
  }
  if (close_error != 0) {
    return absl::Status(absl::StatusCode::kInternal,
                        "close after device-label read failed: " + path);
  }
  if (IsZero(page)) {
    return std::optional<DeviceLabel>{};
  }
  DeviceLabel label{};
  if (!DecodeDeviceLabel(page, &label)) {
    return absl::Status(absl::StatusCode::kInternal,
                        "invalid or corrupt device label: " + path);
  }
  return std::optional<DeviceLabel>{label};
}

inline absl::Status WriteDeviceLabel(const std::string& path,
                                     const DeviceLabel& label) {
  const int fd = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
  if (fd < 0) {
    return absl::Status(absl::StatusCode::kInternal,
                        "open device label for write failed: " + path + ": " +
                            std::strerror(errno));
  }
  std::array<std::byte, kDirectIoAlignment> page{};
  EncodeDeviceLabel(label, page);
  absl::Status status = WriteExactlyAt(fd, page, kDeviceLabelOffset);
  if (status.ok() && ::fdatasync(fd) != 0) {
    status = absl::Status(
        absl::StatusCode::kInternal,
        "device-label fdatasync failed: " + path + ": " + std::strerror(errno));
  }
  const int close_error = ::close(fd);
  if (!status.ok()) {
    return status;
  }
  if (close_error != 0) {
    return absl::Status(absl::StatusCode::kInternal,
                        "close after device-label write failed: " + path);
  }
  return absl::OkStatus();
}

struct MetadataPageState {
  std::uint64_t generation_ = 0;
  std::uint8_t active_slot_ = 0;
};

struct LoadedMetadataPage {
  std::vector<std::byte> payload_;
  MetadataPageState state_{};
};

inline absl::StatusOr<LoadedMetadataPage> ReadMetadataPagePair(
    int fd, std::uint64_t base_offset, MetadataPageKind kind,
    std::uint32_t page_index, std::size_t payload_bytes) {
  LoadedMetadataPage selected;
  selected.payload_.resize(payload_bytes, std::byte{0});
  bool saw_nonzero = false;
  bool selected_valid = false;
  for (unsigned slot = 0; slot < 2; ++slot) {
    std::array<std::byte, kDirectIoAlignment> page{};
    absl::Status read = ReadExactlyAt(
        fd, page, MetadataPageSlotOffset(base_offset, page_index, slot));
    if (!read.ok()) {
      return read;
    }
    if (IsZero(page)) {
      continue;
    }
    saw_nonzero = true;
    std::vector<std::byte> payload(payload_bytes, std::byte{0});
    std::uint64_t generation = 0;
    if (!DecodeMetadataPage(page, kind, page_index, &generation, payload)) {
      continue;
    }
    if (!selected_valid || generation > selected.state_.generation_) {
      selected.payload_ = std::move(payload);
      selected.state_.generation_ = generation;
      selected.state_.active_slot_ = static_cast<std::uint8_t>(slot);
      selected_valid = true;
    }
  }
  if (saw_nonzero && !selected_valid) {
    return absl::Status(absl::StatusCode::kInternal,
                        "both fixed-metadata page slots are corrupt");
  }
  return selected;
}

inline absl::StatusOr<std::uint64_t> RandomStorageSetId() {
  std::uint64_t value = 0;
  while (value == 0) {
    const ssize_t bytes = ::getrandom(&value, sizeof(value), 0);
    if (bytes < 0 && errno == EINTR) {
      continue;
    }
    if (bytes != static_cast<ssize_t>(sizeof(value))) {
      return absl::Status(absl::StatusCode::kInternal,
                          "getrandom for storage-set id failed: " +
                              std::string(std::strerror(errno)));
    }
  }
  return value;
}

struct StorageDevice {
  std::string path_;
  std::uint64_t id_ = 0;
  std::uint64_t capacity_blocks_ = 0;
  std::uint32_t data_block_begin_ = 1;
  std::uint64_t data_block_count_ = 0;
  std::uint32_t file_index_ = 0;
  bool is_block_device_ = false;
};

inline constexpr std::size_t kCacheLineBytes = 64;

struct ReservedBlock {
  std::uint64_t block_id_ = 0;
  std::uint64_t allocation_epoch_ = 0;
};

struct alignas(kCacheLineBytes) RecoveryDeviceCursor {
  std::atomic<std::uint64_t> next_local_{1};
  std::atomic<std::uint64_t> next_allocation_epoch_{1};
};

static_assert(sizeof(RecoveryDeviceCursor) % kCacheLineBytes == 0);

struct DeviceAllocator {
  celer::WorkerId owner_ = 0;
  AsyncMutex mutex_;
  std::uint32_t data_block_begin_ = 1;
  std::uint64_t next_pristine_ = 1;
  std::uint64_t next_allocation_epoch_ = 1;
  std::vector<std::uint64_t> ready_blocks_;
  std::vector<std::uint64_t> cold_free_;
  std::vector<std::byte> scan_bitmap_;
  std::vector<MetadataPageState> bitmap_pages_;
  std::vector<MetadataPageState> epoch_pages_;
  std::vector<std::uint64_t> epoch_values_;
  std::vector<std::uint64_t> durable_epoch_values_;
  std::optional<absl::Status> failed_;
  bool refill_pending_ = false;
};

struct BlockDeviceInfo {
  std::size_t io_alignment_ = 0;
  std::uint64_t size_bytes_ = 0;
};

struct StoragePathInfo {
  bool is_block_device_ = false;
  std::size_t io_alignment_ = kDirectIoAlignment;
  std::uint64_t size_bytes_ = 0;
};

inline absl::StatusOr<BlockDeviceInfo> ProbeBlockDevice(
    const std::string& path) {
  const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    return absl::Status(absl::StatusCode::kInternal,
                        "open block device for probe failed: " + path + ": " +
                            std::strerror(errno));
  }

  int logical_block_bytes = 0;
  std::uint64_t size_bytes = 0;
  const int sector_error = ::ioctl(fd, BLKSSZGET, &logical_block_bytes);
  const int sector_errno = errno;
  const int size_error = ::ioctl(fd, BLKGETSIZE64, &size_bytes);
  const int size_errno = errno;
  const int close_error = ::close(fd);
  if (sector_error != 0) {
    return absl::Status(
        absl::StatusCode::kInternal,
        "BLKSSZGET failed: " + path + ": " + std::strerror(sector_errno));
  }
  if (size_error != 0) {
    return absl::Status(
        absl::StatusCode::kInternal,
        "BLKGETSIZE64 failed: " + path + ": " + std::strerror(size_errno));
  }
  if (close_error != 0) {
    return absl::Status(
        absl::StatusCode::kInternal,
        "close block device after alignment probe failed: " + path);
  }

  const auto alignment = static_cast<std::size_t>(logical_block_bytes);
  if (alignment == 0 || (alignment & (alignment - 1)) != 0) {
    return absl::Status(
        absl::StatusCode::kInternal,
        "block device logical sector size is not a power of two: " + path);
  }
  return BlockDeviceInfo{.io_alignment_ = alignment, .size_bytes_ = size_bytes};
}

inline absl::StatusOr<StoragePathInfo> ProbeStoragePath(
    const std::string& path) {
  struct stat file_info {};
  if (::stat(path.c_str(), &file_info) != 0) {
    return absl::Status(
        absl::StatusCode::kInternal,
        "stat storage path failed: " + path + ": " + std::strerror(errno));
  }
  if (S_ISBLK(file_info.st_mode)) {
    auto device = ProbeBlockDevice(path);
    if (!device.ok()) {
      return device.status();
    }
    return StoragePathInfo{
        .is_block_device_ = true,
        .io_alignment_ = device->io_alignment_,
        .size_bytes_ = device->size_bytes_,
    };
  }
  if (!S_ISREG(file_info.st_mode)) {
    return absl::Status(
        absl::StatusCode::kInvalidArgument,
        "storage path is neither a regular file nor a block device: " + path);
  }
  if (file_info.st_size < 0) {
    return absl::Status(absl::StatusCode::kOutOfRange,
                        "storage file reports a negative size: " + path);
  }
  return StoragePathInfo{
      .is_block_device_ = false,
      .io_alignment_ = kDirectIoAlignment,
      .size_bytes_ = static_cast<std::uint64_t>(file_info.st_size),
  };
}

inline Task<absl::StatusOr<std::size_t>> ReadStorageBuffer(
    Worker& worker, FixedFile file, FixedBuffer buffer, bool registered,
    std::uint64_t offset) {
  if (registered) {
    co_return co_await celer::ReadFixed(worker, file, buffer, offset);
  }
  co_return co_await celer::Read(
      worker, file, std::span<std::byte>(buffer.data_, buffer.size_), offset);
}

inline Task<absl::StatusOr<std::size_t>> WriteStorageBuffer(
    Worker& worker, FixedFile file, std::span<const std::byte> buffer,
    bool registered, FixedBuffer registered_buffer, std::uint64_t offset) {
  if (registered) {
    celer::FixedBuffer target = {
        .data_ = const_cast<std::byte*>(buffer.data()),
        .size_ = buffer.size(),
        .index_ = registered_buffer.index_,
    };
    if (target.index_ == 0 || target.data_ == nullptr ||
        target.size_ > registered_buffer.size_) {
      co_return absl::Status(absl::StatusCode::kInternal,
                             "invalid registered write buffer");
    }
    co_return co_await celer::WriteFixed(worker, file, target, offset);
  }
  co_return co_await celer::Write(worker, file, buffer, offset);
}

class StorageEngine::Impl {
 public:
  explicit Impl(StorageEngineOptions options) : options_(std::move(options)) {}

 private:
  std::size_t direct_io_alignment_ = kDirectIoAlignment;

 public:
  struct WorkerStore {
    struct PartitionStore {
      std::uint16_t id_ = 0;
      std::array<RecordIndex, kLogicalDatabaseCount> indexes_;
      std::array<std::size_t, kLogicalDatabaseCount> live_key_count_{};
      std::array<std::size_t, kLogicalDatabaseCount> expiring_key_count_{};
      std::uint64_t mutation_sequence_ = 0;
      std::uint64_t replication_epoch_ = 1;
      std::uint64_t delta_floor_ = 0;
      bool capture_deltas_ = false;
      bool delta_queued_ = false;
      std::deque<SnapshotRecord> deltas_;
      std::optional<ReplicaValueStage> replica_value_stage_;
    };

    struct ExpireCandidate {
      std::uint16_t partition_id_ = 0;
      std::uint8_t db_id_ = 0;
      Digest digest_{};
      std::uint64_t mutation_sequence_ = 0;
      std::uint64_t expire_at_ms_ = 0;
      std::string key_;
    };

    Worker* worker_ = nullptr;
    RegisteredBufferPool buffers_;
    std::vector<FixedFile> files_;
    std::vector<PartitionStore> partitions_;
    absl::flat_hash_map<std::uint64_t, std::vector<RecordIdentity>>
        staged_records_;
    // External manifests are exceptional and relatively large. Keeping them
    // here, keyed by the address-stable index entry, avoids a shared_ptr in
    // every ordinary key while preserving O(1) FLUSHDB detachment.
    absl::flat_hash_map<const RecordIndex::Entry*, ExtentManifest>
        external_manifests_;
    // Bumped every time FLUSHDB detaches this database's partition indexes.
    // Every index for one database is detached together and without suspending,
    // so one counter per database describes all of them.
    std::array<std::uint64_t, kLogicalDatabaseCount> index_generations_{};
    // Populations detached by FLUSHDB, still holding their entries. Draining
    // this is what actually frees them and settles the block accounting.
    std::deque<DetachedIndex> detached_indexes_;
    bool detached_reclaim_running_ = false;
    std::array<std::size_t, kLogicalDatabaseCount> live_key_count_{};
    std::optional<ActiveBlock> active_block_;
    // Recovery only. A recovered extent block's identity has to be checked
    // against the manifests that reference it, and the two arrive in separate
    // passes, so they meet here instead of in every BlockState. Cleared once
    // the live-reference pass has run.
    absl::flat_hash_map<std::uint64_t, ExtentIdentity> recovered_extents_;
    // Recovery-only exact identities for external-key entries. Runtime index
    // entries deliberately omit the full key, but recovery already had to
    // materialize it for routing, so retain it until every version is merged.
    absl::flat_hash_map<const RecordIndex::Entry*, std::string>
        recovery_external_keys_;
    // txid-tagged records parked by ApplyRecovery until the committed-txid set
    // is complete (after the recovery barrier).
    std::vector<RecoveryRecord> recovery_tx_records_;
    // Undo journals of in-flight multi-key writes on this shard, keyed by
    // txid; written and consumed under store_state_mutex.
    absl::flat_hash_map<std::uint64_t, std::vector<TxUndoEntry>> tx_undo_;
    // Relocation fences owed per source block. A salvage pass that fails
    // midway has already moved records whose copies are not yet durable; the
    // debt survives the pass here, and CleanBlockLocked settles every owed
    // fence before the block's bitmap bit may be durably cleared. Same-worker
    // access only.
    absl::flat_hash_map<std::uint64_t, std::vector<RelocationDurabilityFence>>
        pending_relocation_fences_;
    // A retired root record's shared key/value extents remain needed by
    // recovery until the whole records block is durably removed from the
    // allocation bitmap.
    absl::flat_hash_map<std::uint64_t, std::vector<ExtentManifest>>
        deferred_dependent_extent_reclaims_;
    // Index 0 is the "no staging buffer" sentinel. A deque keeps references
    // stable as the table grows, since heap fallback buffers are unbounded.
    std::deque<StagingSlot> staging_slots_{1};
    std::uint16_t free_staging_slot_ = 0;
    // Serializes this store's index, active append block, staging state, and
    // block accounting. Release it across block allocation and long I/O;
    // callers that do so must revalidate any state observed before the wait.
    AsyncMutex store_state_mutex_;
    std::deque<std::uint64_t> flush_queue_;
    std::deque<std::uint64_t> defrag_queue_;
    std::vector<std::size_t> home_devices_;
    std::vector<std::uint64_t> home_device_allocations_;
    bool flush_running_ = false;
    bool write_failed_ = false;
    bool defrag_running_ = false;
    bool defrag_waiting_ = false;
    std::size_t defrag_waiting_device_ = 0;
    std::size_t active_defrag_device_ = 0;
    std::size_t expiry_partition_cursor_ = 0;
    std::uint8_t expiry_db_cursor_ = 0;
    std::uint64_t expiry_scan_cursor_ = 0;
    // True for the whole of one expiration cycle, scan through last tombstone.
    // QuiesceExpiration waits on it, which covers every suspension inside the
    // cycle's deletes — including block-allocation waits that release
    // store_state_mutex mid-append.
    bool expiry_cycle_running_ = false;
    std::deque<ExpireCandidate> expired_candidates_;
  };

  absl::Status Prepare(unsigned worker_count);

  Task<absl::Status> InitializeWorker(Worker& worker);

  unsigned OwnerForKey(std::string_view key) const noexcept {
    return StorageShardForKey(key) % worker_count_;
  }

  Task<absl::StatusOr<DiskValue>> Get(std::uint8_t db_id, std::string_view key,
                                      ReadLatencyTrace* trace);

  // Caller holds this worker's key lock for `digest` (shared) and runs on
  // OwnerForKey(key). `digest` must equal ComputeDigest(key).
  Task<absl::StatusOr<DiskValue>> GetLocked(std::uint8_t db_id,
                                            std::string_view key,
                                            const Digest& digest,
                                            ReadLatencyTrace* trace);

  Task<absl::StatusOr<std::uint64_t>> StringLength(std::uint8_t db_id,
                                                   std::string_view key);

  // Caller holds the key lock (shared); see GetLocked.
  Task<absl::StatusOr<std::uint64_t>> StringLengthLocked(std::uint8_t db_id,
                                                         std::string_view key,
                                                         const Digest& digest);

  Task<absl::StatusOr<SetResult>> Set(std::uint8_t db_id, std::string_view key,
                                      std::string_view value,
                                      SetOptions options);

  // Caller holds the key lock (exclusive); takes store_state_mutex internally.
  Task<absl::StatusOr<SetResult>> SetLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      std::string_view value, SetOptions options, TxShardWrites* tx = nullptr);

  Task<ExpirationInfo> GetExpiration(std::uint8_t db_id, std::string_view key);

  // Caller holds the key lock (shared); see GetLocked.
  Task<ExpirationInfo> GetExpirationLocked(std::uint8_t db_id,
                                           std::string_view key,
                                           const Digest& digest);

  Task<absl::StatusOr<bool>> UpdateExpiration(std::uint8_t db_id,
                                              std::string_view key,
                                              std::uint64_t expire_at_ms,
                                              ExpirationCondition condition);

  // Caller holds the key lock (exclusive); takes store_state_mutex internally.
  Task<absl::StatusOr<bool>> UpdateExpirationLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      std::uint64_t expire_at_ms, ExpirationCondition condition,
      TxShardWrites* tx = nullptr);

  Task<absl::StatusOr<bool>> Delete(std::uint8_t db_id, std::string_view key);

  // Caller holds the key lock (exclusive); takes store_state_mutex internally.
  Task<absl::StatusOr<bool>> DeleteLocked(std::uint8_t db_id,
                                          std::string_view key,
                                          const Digest& digest,
                                          TxShardWrites* tx = nullptr);

  // Freezes the keyspace against expiration writes for stable-count scans
  // (KEYS): client writes are already excluded by the closed database gate;
  // this stops the active-expiry loop and drains any in-flight append by
  // bouncing off every worker's store-state mutex. Pauses nest — the database
  // gates are per-db, so KEYS on two databases can overlap — and every
  // successful QuiesceExpiration must be paired with exactly one
  // ResumeExpiration.
  Task<absl::Status> QuiesceExpiration();

  TombRaiderTotals TombRaiderStats() const noexcept {
    return TombRaiderTotals{
        .rounds_ = tomb_raider_rounds_.load(std::memory_order_relaxed),
        .reaped_ = tomb_raider_reaped_.load(std::memory_order_relaxed),
        .refreshed_ = tomb_raider_refreshed_.load(std::memory_order_relaxed),
    };
  }

  Task<StorageMetricsSnapshot> CollectMetrics() const;

  void ResumeExpiration() noexcept {
    expiration_pause_count_.fetch_sub(1, std::memory_order_acq_rel);
  }

  Task<bool> KeyLive(std::uint8_t db_id, std::string_view key,
                     const Digest& digest);

  Task<bool> Exists(std::uint8_t db_id, std::string_view key);

  // Caller holds the key lock (shared); see GetLocked.
  Task<bool> ExistsLocked(std::uint8_t db_id, std::string_view key,
                          const Digest& digest);

  Task<absl::StatusOr<std::int64_t>> Increment(std::uint8_t db_id,
                                               std::string_view key);

  // Caller holds the key lock (exclusive); takes store_state_mutex internally.
  Task<absl::StatusOr<std::int64_t>> IncrementLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      TxShardWrites* tx = nullptr);

  Task<absl::Status> CommitTxWrites(std::uint64_t txid,
                                    std::vector<TxShardWrites*> shards);

  void NoteTxCommitStarted() noexcept {
    active_tx_commits_.fetch_add(1, std::memory_order_acq_rel);
  }
  void NoteTxCommitFinished() noexcept {
    active_tx_commits_.fetch_sub(1, std::memory_order_acq_rel);
  }

  Task<absl::Status> RollbackTxLocal(std::uint64_t txid);

  Task<absl::Status> DiscardTxUndoLocal(std::uint64_t txid);

  unsigned worker_count() const noexcept { return worker_count_; }

  std::size_t LocalSize(std::uint8_t db_id) const noexcept {
    assert(db_id < kLogicalDatabaseCount);
    return CurrentStore().live_key_count_[db_id];
  }

  std::uint64_t DbEpoch(std::uint8_t db_id) const noexcept {
    assert(db_id < kLogicalDatabaseCount);
    return db_epochs_[db_id].load(std::memory_order_acquire);
  }

  Task<absl::Status> FlushDbDetach(std::uint8_t db_id);

  Task<absl::Status> FlushDbReclaim(bool wait) {
    co_return co_await ReclaimDetachedAllWorkers(wait);
  }

  // Persists the new epoch, then takes the database out of service on every
  // worker. Callers hold the database gate across this and can drop it as soon
  // as it returns: the keyspace is empty and durably so, and what remains is
  // reclamation that no reader can observe. Bounded by the partition count.
  //
  // The epoch has to reach the device before any index is detached. Crashing in
  // the other order leaves records on disk whose epoch still matches, and
  // recovery would resurrect the whole flushed database.
  Task<absl::Status> DetachDbEpoch(std::uint8_t db_id, std::uint64_t next);

  // Retires what DetachDbEpoch took out of service. Runs with the gate open and
  // ordinary traffic flowing. `wait` is the difference between FLUSHDB SYNC and
  // FLUSHDB ASYNC: either way a reclaimer runs, only the reply waits or not.
  Task<absl::Status> ReclaimDetachedAllWorkers(bool wait);

  Task<absl::Status> AdvanceDbEpoch(std::uint8_t db_id, std::uint64_t next);

  Task<absl::StatusOr<ScanBatch>> ScanPartition(
      std::uint16_t partition_id, std::uint8_t db_id, std::uint64_t cursor,
      std::size_t count, std::uint64_t now_ms,
      std::size_t max_bytes = SIZE_MAX);

  PartitionReplicationStart BeginPartitionReplication(
      std::uint16_t partition_id);

  Task<absl::StatusOr<PartitionSnapshotBatch>> SnapshotPartition(
      std::uint16_t partition_id, std::uint8_t db_id, std::uint64_t cursor,
      std::size_t count);

  PartitionDeltaBatch ReadPartitionDeltas(std::uint16_t partition_id,
                                          std::uint64_t after_sequence,
                                          std::size_t count);

  void AcknowledgePartitionDeltas(std::uint16_t partition_id,
                                  std::uint64_t through_sequence);

  bool TryTakeReplicationReady(std::uint16_t* partition_id) {
    return partition_id != nullptr &&
           replication_ready_.try_dequeue(*partition_id);
  }

  Task<absl::StatusOr<std::uint64_t>> ResetReplicaPartition(
      std::uint16_t partition_id,
      std::span<const std::uint64_t, kLogicalDatabaseCount> source_db_epochs);

  Task<absl::Status> ApplyReplicaRecords(
      std::uint16_t partition_id, std::uint64_t replication_epoch,
      std::span<const SnapshotRecord> records);

  absl::Status FlushForShutdown();

 private:
  static StagingSlot* StagingFor(WorkerStore& store, const BlockState& state);

  static std::uint16_t AcquireStagingSlot(WorkerStore& store);

  FixedBuffer StagingBufferFor(WorkerStore& store,
                               const BlockState& state) const;

  static void ReleaseStagingBuffer(WorkerStore& store, BlockState& state);

  struct LoadedValue {
    ReadBufferLease lease_;
    std::size_t value_offset_ = 0;
    std::size_t value_bytes_ = 0;

    std::span<const std::byte> value() const noexcept {
      const std::span<std::byte> buffer = lease_.bytes();
      if (value_offset_ > buffer.size() ||
          value_bytes_ > buffer.size() - value_offset_) {
        return {};
      }
      return buffer.subspan(value_offset_, value_bytes_);
    }
  };

  std::size_t DirectGetValueLimit() const noexcept;

  absl::StatusOr<DiskValue> EncodeDiskValue(LoadedValue loaded);

  void QueueExpiredCandidate(WorkerStore& store, std::uint16_t partition_id,
                             std::uint8_t db_id,
                             const RecordIndex::Entry& entry,
                             std::string_view known_key = {});

  WorkerStore& CurrentStore() { return *stores_[celer::ThisWorker().id_]; }

  const WorkerStore& CurrentStore() const {
    return *stores_[celer::ThisWorker().id_];
  }

  WorkerStore::PartitionStore& PartitionFor(WorkerStore& store,
                                            std::uint16_t partition_id) const {
    assert(partition_id < kLogicalStorageShards);
    assert(partition_id % worker_count_ == store.worker_->id());
    auto& partition = store.partitions_[partition_id / worker_count_];
    assert(partition.id_ == partition_id);
    return partition;
  }

  const WorkerStore::PartitionStore& PartitionFor(
      const WorkerStore& store, std::uint16_t partition_id) const {
    assert(partition_id < kLogicalStorageShards);
    assert(partition_id % worker_count_ == store.worker_->id());
    const auto& partition = store.partitions_[partition_id / worker_count_];
    assert(partition.id_ == partition_id);
    return partition;
  }

  WorkerStore::PartitionStore& PartitionForKey(WorkerStore& store,
                                               std::string_view key) const {
    return PartitionFor(store, RedisSlot(key));
  }

  const WorkerStore::PartitionStore& PartitionForKey(
      const WorkerStore& store, std::string_view key) const {
    return PartitionFor(store, RedisSlot(key));
  }

  // Absent means unallocated or owned by another worker. Ownership is settled
  // from the atomic first, so a foreign entry is never read past that field.
  BlockState* FindBlockState(WorkerStore& store,
                             std::uint64_t block_id) noexcept {
    BlockState& state = BlockStateAt(block_id);
    if (state.owner_.load(std::memory_order_acquire) != store.worker_->id()) {
      return nullptr;
    }
    return state.allocated_ ? &state : nullptr;
  }

  const BlockState* FindBlockState(const WorkerStore& store,
                                   std::uint64_t block_id) const noexcept {
    const BlockState& state = const_cast<Impl*>(this)->BlockStateAt(block_id);
    if (state.owner_.load(std::memory_order_acquire) != store.worker_->id()) {
      return nullptr;
    }
    return state.allocated_ ? &state : nullptr;
  }

  BlockState& CreateBlockState(WorkerStore& store, std::uint64_t block_id) {
    BlockState& state = BlockStateAt(block_id);
    state.Reset(static_cast<std::uint16_t>(store.worker_->id()));
    return state;
  }

  void DestroyBlockState(WorkerStore& store, std::uint64_t block_id) {
    (void)store;
    BlockStateAt(block_id).Reset(kUnownedBlock);
  }

  // Walks this worker's blocks. The array is shared, so ownership is filtered
  // from the atomic and no other worker's fields are touched.
  template <typename Fn>
  void ForEachOwnedBlock(WorkerStore& store, Fn&& fn) {
    const std::uint16_t me = static_cast<std::uint16_t>(store.worker_->id());
    for (std::size_t device_index = 0; device_index < devices_.size();
         ++device_index) {
      const StorageDevice& device = devices_[device_index];
      std::vector<BlockState>& states = device_block_states_[device_index];
      for (std::size_t slot = 0; slot < states.size(); ++slot) {
        BlockState& state = states[slot];
        if (state.owner_.load(std::memory_order_acquire) != me ||
            !state.allocated_) {
          continue;
        }
        fn(MakeBlockId(device.id_, static_cast<std::uint32_t>(
                                       slot + device.data_block_begin_)),
           state);
      }
    }
  }

  std::size_t DeviceIndexForBlock(std::uint64_t block_id) const noexcept;

  std::pair<std::uint32_t, std::uint64_t> FileOffset(
      std::uint64_t block_id) const noexcept {
    const StorageDevice& device = devices_[DeviceIndexForBlock(block_id)];
    assert(LocalBlockId(block_id) >= device.data_block_begin_);
    assert(LocalBlockId(block_id) < device.capacity_blocks_);
    return {device.file_index_, LocalBlockOffset(block_id)};
  }

  static bool BitmapBit(const DeviceAllocator& allocator,
                        std::uint32_t local_block) noexcept;

  static void SetBitmapBit(DeviceAllocator& allocator,
                           std::uint32_t local_block) noexcept;

  static void ClearBitmapBit(DeviceAllocator& allocator,
                             std::uint32_t local_block) noexcept;

  Task<absl::Status> PersistBitmapPages(std::size_t device_index,
                                        DeviceAllocator& allocator,
                                        std::vector<std::size_t> page_indexes);

  Task<absl::Status> InvalidateReactivatedBlockHeadersLocal(
      std::size_t device_index, std::span<const std::uint64_t> block_ids);

  void MaybeRefillDeviceInBackground(std::size_t device_index,
                                     DeviceAllocator& allocator);

  Task<absl::Status> RefillDeviceInBackground(std::size_t device_index);

  Task<absl::Status> RefillReadyBlocksLocal(std::size_t device_index,
                                            DeviceAllocator& allocator);

  Task<absl::StatusOr<ReservedBlock>> AllocateFromDeviceLocal(
      std::size_t device_index, bool for_defrag);

  Task<absl::StatusOr<ReservedBlock>> AllocateFromDevice(
      std::size_t device_index, bool for_defrag);

  Task<absl::Status> ReturnColdBlocksLocal(
      std::size_t device_index, std::vector<std::uint64_t> block_ids);

  Task<absl::Status> ReturnColdBlocks(std::vector<std::uint64_t> block_ids);

  Task<absl::Status> PersistEpochValueOnDeviceLocal(std::size_t device_index,
                                                    std::size_t value_index,
                                                    std::uint64_t epoch);

  Task<absl::Status> PersistEpochValue(std::size_t value_index,
                                       std::uint64_t epoch);

  // Takes the database out of service on this worker. Everything here is O(the
  // partition count) and runs without suspending, so the caller's FLUSHDB gate
  // stays closed for a bounded time no matter how many keys the database holds.
  // Retiring the detached entries is left to ReclaimDetachedIndexes.
  void DetachDbLocal(WorkerStore& store, std::uint8_t db_id);

  // Retires the entries FLUSHDB detached: subtracts what they contributed to
  // their blocks, then frees them. Readers can no longer reach any of it, so
  // this may run long after the command replied.
  //
  // Each block is credited once for the whole population rather than once per
  // record, which is the same total by construction and turns a per-record
  // cross-core hop into a per-block one. A block cannot be recycled underneath
  // an outstanding subtraction: whatever is still owed keeps its live_bytes
  // above zero, so it cannot reach the empty-block path until this settles.
  Task<absl::Status> ReclaimDetachedIndexes(WorkerStore& store);

  // At most one reclaimer per store, so the two ways in — a background one that
  // FLUSHDB ASYNC leaves behind, and a caller waiting for SYNC — never split a
  // population between them. Whichever runs picks up work queued after it
  // started, so an arriving FLUSHDB only has to make sure one is alive.
  void EnsureDetachedReclaim(WorkerStore& store);

  Task<absl::Status> RunDetachedReclaim(WorkerStore* store);

  // Waits for everything detached so far to be retired. The keyspace is already
  // empty either way; this is what makes FLUSHDB SYNC mean the memory came back
  // before the reply.
  Task<absl::Status> AwaitDetachedReclaim(WorkerStore& store);

  void Fail(const absl::Status& status);

  // Block states live in one dense array per device, indexed by local block
  // id. Each array is sized once at startup and never resized, so entries
  // never move and a BlockState* stays valid across suspension points.
  BlockState& BlockStateAt(std::uint64_t block_id) noexcept {
    const std::size_t device_index = DeviceIndexForBlock(block_id);
    const StorageDevice& device = devices_[device_index];
    const std::uint32_t local = LocalBlockId(block_id);
    assert(local >= device.data_block_begin_);
    assert(local < device.capacity_blocks_);
    return device_block_states_[device_index][local - device.data_block_begin_];
  }

  // Which worker owns a block, or kUnownedBlock if it is free. This is the one
  // field a non-owner may read, so it is the only one that is atomic.
  std::uint16_t BlockOwner(std::uint64_t block_id) const noexcept {
    const std::size_t device_index = DeviceIndexForBlock(block_id);
    const StorageDevice& device = devices_[device_index];
    const std::uint32_t local = LocalBlockId(block_id);
    if (device_block_states_.empty() || local < device.data_block_begin_ ||
        local >= device.capacity_blocks_) {
      return kUnownedBlock;
    }
    return device_block_states_[device_index][local - device.data_block_begin_]
        .owner_.load(std::memory_order_acquire);
  }

  std::uint16_t RecoveredBlockOwner(const BlockHeader& block,
                                    std::uint64_t block_id) const noexcept;

  // `allocated` marks blocks the scan bitmap said were in use — the only ones
  // that cost I/O. Free blocks are skipped without a read, so ETA and percent
  // are computed over allocated blocks; the capacity-wide sweep count only
  // detects completion.
  void ReportRecoveryProgress(std::uint64_t records, bool allocated);
  Task<absl::Status> ScanAssignedBlocks(
      WorkerStore& store, std::vector<RecoveryBatch>* batches,
      std::vector<std::uint64_t>* zero_blocks,
      absl::flat_hash_set<std::uint64_t>* committed_txids);

  void ApplyRecovery(unsigned target, RecoveryBatch batch);

  void ApplyRecoveredRecord(WorkerStore& store, const RecoveryRecord& record);

  static ExtentManifest ExtentsFor(const WorkerStore& store,
                                   const RecordIndex::Entry* entry) {
    if (entry == nullptr || !entry->value_.external_) {
      return {};
    }
    const auto found = store.external_manifests_.find(entry);
    return found == store.external_manifests_.end() ? ExtentManifest{}
                                                    : found->second;
  }

  static ExtentManifest DependentExtentsFor(const WorkerStore& store,
                                            const RecordIndex::Entry* entry) {
    if (entry == nullptr || !entry->value_.key_external_) [[likely]] {
      return {};
    }
    return entry->value_.external_ ? ExtentsFor(store, entry)
                                   : ExtentManifest{};
  }

  Task<absl::StatusOr<std::string>> LoadExternalKey(WorkerStore& store,
                                                    ExtentManifest extents,
                                                    std::size_t key_bytes);
  Task<absl::StatusOr<std::string>> LoadOutOfIndexKey(
      WorkerStore& store, const RecordLocation& location,
      ExtentManifest extents, std::size_t key_bytes);
  Task<absl::StatusOr<std::string>> LoadInlineRecordKeyLocal(
      WorkerStore& store, const RecordLocation& location,
      std::size_t key_bytes);
  Task<absl::StatusOr<std::string>> LoadExternalKeyForRecovery(
      WorkerStore& store, ExtentManifest extents, std::size_t key_bytes);

  Task<absl::StatusOr<bool>> VerifyExternalKey(WorkerStore& store,
                                               const RecordIndex::Entry& entry,
                                               std::string_view key);
  Task<absl::StatusOr<bool>> VerifyExternalKeyExtents(WorkerStore& store,
                                                      ExtentManifest extents,
                                                      std::string_view key);
  Task<absl::StatusOr<bool>> VerifyInlineRecordKey(
      WorkerStore& store, const RecordLocation& location, std::string_view key);
  Task<absl::StatusOr<bool>> VerifyInlineRecordKeyLocal(
      WorkerStore& store, const RecordLocation& location, std::string_view key);

  Task<absl::StatusOr<RecordIndex::Entry*>> FindVerifiedEntry(
      WorkerStore& store, RecordIndex& index, const Digest& digest,
      std::string_view key);

  Task<absl::StatusOr<LoadedValue>> LoadValue(
      WorkerStore& key_store, std::uint8_t db_id, std::string_view key,
      const Digest& digest, RecordLocation location, ExtentManifest extents,
      ReadLatencyTrace* trace = nullptr);

  // Reads one extent block's payload into `destination`. Runs on the worker
  // that owns that block, which is not necessarily the one holding the
  // manifest, so everything it needs is passed by value.
  Task<absl::Status> ReadExtentInto(WorkerStore& store, ExtentRef ref,
                                    std::uint32_t extent_index,
                                    std::byte* destination);

  Task<absl::StatusOr<LoadedValue>> LoadExternalValueLocal(
      WorkerStore& store, const RecordLocation& location,
      ExtentManifest extents, std::size_t key_bytes, ReadLatencyTrace* trace);

  Task<absl::StatusOr<LoadedValue>> LoadValueLocal(
      WorkerStore& store, std::uint8_t db_id, std::string_view key,
      const Digest& digest, RecordLocation location,
      ReadLatencyTrace* trace = nullptr);

  std::uint64_t ForegroundBlocksForDevice(
      std::size_t device_index) const noexcept {
    const std::uint64_t data_blocks = devices_[device_index].data_block_count_;
    const std::size_t reserve = DefragReserveForDevice(device_index);
    return data_blocks > reserve ? data_blocks - reserve : 0;
  }

  void ConfigureDefragReserves() {
    defrag_reserve_blocks_.assign(devices_.size(),
                                  kDefragReserveBlocksPerDevice);
  }

  void ConfigureWorkerDeviceAffinity();

  Task<absl::StatusOr<ReservedBlock>> AllocateBlock(WorkerStore& store,
                                                    bool for_defrag);

  std::size_t DefragReserveForDevice(std::size_t device_index) const noexcept {
    assert(device_index < defrag_reserve_blocks_.size());
    return defrag_reserve_blocks_[device_index];
  }

  absl::Status MarkRecordDeadLocal(unsigned owner, const RetiredRecord& record);

  Task<absl::Status> MarkRecordDead(const RetiredRecord& record);

  Task<absl::Status> MarkRetiredRecordsDead(WorkerStore* store,
                                            std::vector<RetiredRecord> records);

  static RetiredRecord RetiredRecordOf(const RecordLocation& location,
                                       ExtentManifest dependent_extents = {}) {
    return RetiredRecord{
        .block_id_ = location.block_id_,
        .allocation_epoch_ = location.allocation_epoch_,
        .total_disk_bytes_ = location.total_disk_bytes_,
        .block_owner_ = location.block_owner_,
        .dependent_extents_ = std::move(dependent_extents),
        .extra_dependent_extents_ = nullptr,
    };
  }

  Task<absl::StatusOr<std::shared_ptr<const std::vector<ExtentRef>>>>
  WriteExtentValueLocked(WorkerStore& store, std::string_view first,
                         std::string_view second = {});

  Task<absl::Status> AppendLocked(WorkerStore& store,
                                  WorkerStore::PartitionStore& partition,
                                  std::uint8_t db_id, std::string_view key,
                                  std::string_view value, RecordKind kind,
                                  ValueType value_type,
                                  std::uint64_t expire_at_ms,
                                  TxShardWrites* tx = nullptr);

  void AppendDelta(WorkerStore::PartitionStore& partition,
                   SnapshotRecord record);

  Task<absl::StatusOr<ReservedBlock>> AcquireWriteBlock(WorkerStore& store,
                                                        bool for_defrag,
                                                        bool unlock_writer);

  Task<absl::Status> ReturnReservedBlock(ReservedBlock block);

  Task<absl::Status> WriteRecordLocked(
      WorkerStore& store, std::uint8_t db_id, std::string_view key,
      std::string_view value, RecordKind kind, ValueType value_type,
      std::uint64_t expire_at_ms, const Digest& digest, std::uint64_t txid,
      std::uint64_t mutation_sequence, std::uint64_t relocation_sequence,
      bool for_defrag, bool unlock_writer_while_waiting = true,
      bool external = false, bool key_external = false,
      std::uint64_t logical_size = std::numeric_limits<std::uint64_t>::max(),
      std::shared_ptr<const std::vector<ExtentRef>> extents = nullptr,
      RecordLocation* written_location = nullptr,
      const RelocationSource* relocation = nullptr, TxShardWrites* tx = nullptr,
      std::shared_ptr<std::vector<RetiredRecord>> commit_retirements = nullptr);

  void SealActiveBlocks(WorkerStore& store);

  // Make the active block's tail durable without retiring it. The block stays
  // open for appends, so a slow writer no longer burns a whole 8 MiB block per
  // flush interval; it pays at most one padding page instead.
  void FlushActiveBlock(WorkerStore& store);

  void SealDeadActiveBlock(WorkerStore& store);

  Task<absl::Status> FlushWorkerForShutdown(WorkerStore* store);

  void CompleteShutdownFlush(const absl::Status& status);

  void AdvanceExpiryMap(WorkerStore& store);

  Task<absl::Status> ExpireCandidate(WorkerStore& store,
                                     WorkerStore::ExpireCandidate candidate);

  Task<absl::Status> ActiveExpiration(WorkerStore* store);

  // Tomb raider: a full-disk sweep that retires tombstones nothing on disk
  // needs any more, and re-validates stale shielding bits along the way.
  // One dangerous older record for a claimed key, seen anywhere on any
  // worker's blocks, exempts the entry for the round.
  struct TombClaim {
    std::uint64_t mutation_sequence_ = 0;
    std::uint64_t replication_epoch_ = 0;
    Digest digest_{};
    std::string key_;
    std::uint8_t db_id_ = 0;
  };

  Task<absl::Status> TombRaiderLoop(WorkerStore* store);

  Task<absl::Status> RunTombRaider();

  Task<absl::Status> TombMarkLocal(WorkerStore& store);

  Task<absl::Status> TombSweepLocal(WorkerStore& store);

  Task<absl::Status> TombClaimLocal(WorkerStore& store,
                                    std::vector<TombClaim> claims);

  Task<absl::Status> TombReapLocal(WorkerStore& store);

  Task<absl::Status> PeriodicFlush(WorkerStore* store);

  // Spawn an asynchronous extent reclaim, counted from before the spawn so
  // the block allocator's full-device check always sees it in flight.
  void SpawnExtentReclaim(
      WorkerStore& store,
      std::shared_ptr<const std::vector<ExtentRef>> extents);

  Task<absl::Status> ReclaimExtentsCounted(
      WorkerStore* store,
      std::shared_ptr<const std::vector<ExtentRef>> extents);

  // Retires one extent block. Runs on that block's owner, which after a
  // worker-count change is unrelated to the owner of the manifest that
  // referenced it. Reports whether the block became free.
  Task<absl::StatusOr<bool>> ReclaimExtentLocal(WorkerStore& store,
                                                ExtentRef ref);

  Task<absl::Status> ReclaimExtents(
      WorkerStore* store,
      std::shared_ptr<const std::vector<ExtentRef>> extents);

  void RequestFlush(WorkerStore& store, std::uint64_t block_id);

  Task<absl::Status> FlushPendingBlocks(WorkerStore* store);

  bool IsActiveBlock(const WorkerStore& store,
                     std::uint64_t block_id) const noexcept;

  bool IsDefragCandidate(const WorkerStore& store,
                         std::uint64_t block_id) const noexcept;

  void MaybeQueueDefrag(WorkerStore& store, std::uint64_t block_id);

  bool TryAcquireDefragPermit(std::size_t device_index);

  void ReleaseDefragPermit(std::size_t device_index);

  Task<absl::Status> StartQueuedDefrag(unsigned worker_id,
                                       std::size_t device_index);

  Task<absl::Status> WakeQueuedDefrags(std::size_t device_index);

  void RequestDefrag(WorkerStore& store);

  void FinishDefragPass(WorkerStore& store);

  Task<absl::Status> DefragOne(WorkerStore* store);

  Task<absl::StatusOr<std::optional<RelocationDurabilityFence>>>
  RelocateIfCurrent(unsigned key_owner, std::string_view key,
                    std::string_view value, const RecordHeader& record,
                    const RecordLocation& source_location);

  Task<absl::Status> AwaitRelocationDurableLocal(
      WorkerStore& store, const RelocationDurabilityFence& fence);

  Task<absl::Status> AwaitRelocationDurable(
      const RelocationDurabilityFence& fence);

  Task<absl::Status> CleanBlockLocked(WorkerStore& store,
                                      std::uint64_t block_id);

  // Rewrites every record in the block that is still current, so the block ends
  // up with no reachable data and the caller can free it. Clears `defragging`
  // on each failure path so the block stays eligible for a later pass.
  Task<absl::Status> SalvageBlockRecords(WorkerStore& store,
                                         std::uint64_t block_id,
                                         BlockState& source,
                                         std::uint32_t source_file_id,
                                         std::uint64_t source_block_offset);

  // Drains readers and hands the block back to the allocator. The caller must
  // have observed live_bytes == 0 under store_state_mutex and set `freeing`,
  // which stops LoadValueLocal from taking new pins.
  Task<absl::Status> ReleaseEmptyBlock(WorkerStore& store,
                                       std::uint64_t block_id,
                                       BlockState& source);

  StorageEngineOptions options_;
  unsigned worker_count_ = 0;
  std::uint64_t total_data_blocks_ = 0;
  std::vector<StorageDevice> devices_;
  // Which worker owns each block, by device and local block id. A record
  // carries its block's owner in its index entry, but an extent reference has
  // no such field, so this is how a worker holding a manifest finds the worker
  // to dispatch to. Written only by the owner as it allocates or frees a
  // block, read by anyone.
  // One dense array per device, indexed by local block id less the device's
  // data_block_begin. Shared across workers; each entry names its owner and
  // only that worker touches anything but the owner field.
  std::vector<std::vector<BlockState>> device_block_states_;
  std::vector<std::unique_ptr<DeviceAllocator>> device_allocators_;
  std::vector<std::size_t> defrag_reserve_blocks_;
  std::unique_ptr<std::atomic<unsigned>[]> active_defrags_by_device_;
  std::unique_ptr<moodycamel::ConcurrentQueue<std::uint16_t>[]>
      defrag_ready_by_device_;
  std::unique_ptr<RecoveryDeviceCursor[]> recovery_device_cursors_;
  std::vector<std::uint64_t> epoch_values_;
  std::atomic<bool> epoch_metadata_failed_{false};
  std::atomic<std::uint32_t> expiration_pause_count_{0};
  // Background tasks that settle accounting through cross-worker hops
  // (retired-record settlement, detached-index reclaim, a tomb raider
  // round). A frame parked on such a hop is registered with the remote
  // worker; tearing its own worker down under it lets the remote resume a
  // destroyed frame. The shutdown drain waits for this to reach zero, so
  // every one of these tasks must be short-lived or abort promptly once
  // shutdown_flush_requested_ is set.
  std::atomic<std::uint32_t> active_settlements_{0};
  std::atomic<bool> tomb_raider_running_{false};
  std::atomic<std::uint64_t> tomb_raider_rounds_{0};
  std::atomic<std::uint64_t> tomb_raider_reaped_{0};
  std::atomic<std::uint64_t> tomb_raider_refreshed_{0};
  std::vector<std::unique_ptr<WorkerStore>> stores_;
  std::unique_ptr<CoroutineBarrier> open_barrier_;
  std::unique_ptr<CoroutineBarrier> metadata_barrier_;
  std::unique_ptr<CoroutineBarrier> recovery_barrier_;
  std::unique_ptr<CoroutineBarrier> recovery_accounting_barrier_;
  std::unique_ptr<CoroutineBarrier> free_list_barrier_;
  std::atomic<std::uint64_t> recovery_scanned_blocks_{0};
  std::atomic<std::uint64_t> recovery_scanned_records_{0};
  std::atomic<std::uint64_t> recovery_max_txid_{0};
  // Commit chains spawned but not yet finished; graceful shutdown drains
  // them before the final flush so acknowledged multi-key writes do not
  // lose their commit records to the shutdown ordering.
  std::atomic<std::uint64_t> active_tx_commits_{0};
  // Committed transactions seen during the block scans; merged by each
  // worker before the recovery barrier, read only after it.
  std::mutex recovery_committed_mutex_;
  absl::flat_hash_set<std::uint64_t> recovery_committed_txids_;
  std::atomic<std::uint64_t> recovery_scanned_allocated_{0};
  // Blocks the loaded scan bitmaps mark as allocated, summed over all devices
  // in Prepare. This is the recovery scan's real workload.
  std::uint64_t recovery_allocated_blocks_ = 0;
  std::atomic<std::int64_t> recovery_next_log_ms_{0};
  std::atomic<bool> recovery_complete_logged_{false};
  std::int64_t recovery_started_ms_ = 0;
  static constexpr std::size_t kDefragReserveBlocksPerDevice = 8;
  static constexpr unsigned kDefragPermitsPerDevice = 8;
  std::atomic<std::uint64_t> next_lsn_{1};
  std::array<std::atomic<std::uint64_t>, kLogicalDatabaseCount> db_epochs_{};
  std::atomic<unsigned> active_defrags_{0};
  std::atomic<unsigned> pending_defrags_{0};
  std::atomic<unsigned> active_flushes_{0};
  // Extent reclaims in flight (from the moment they are spawned): the block
  // allocator must not report the device full while one may still free space.
  std::atomic<unsigned> active_extent_reclaims_{0};
  std::atomic<std::uint64_t> space_reclaim_generation_{0};
  std::atomic<bool> shutdown_flush_requested_{false};
  std::atomic<unsigned> shutdown_flush_completed_{0};
  std::atomic<bool> shutdown_flush_failed_{false};
  moodycamel::ConcurrentQueue<std::uint16_t> replication_ready_;
};

}  // namespace keylane::storage
