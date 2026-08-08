#pragma once

#include "keylane/storage/engine.h"

#include <fcntl.h>
#include <linux/fs.h>
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
#include <chrono>
#include <charconv>
#include <cerrno>
#include <coroutine>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <limits>
#include <new>
#include <mutex>
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
using celer::UnlockGuard;
using celer::FixedBuffer;
using celer::FixedFile;
using celer::Status;
using celer::StatusCode;
using celer::StatusOr;
using celer::Task;
using celer::Worker;

struct RecordLocation {
  std::uint64_t block_id = 0;
  std::uint64_t replication_epoch = 1;
  std::uint64_t mutation_sequence = 0;
  std::uint64_t allocation_epoch = 0;
  std::uint64_t expire_at_ms = 0;
  // Owner in the current process topology. Unlike the persisted writer_id,
  // this must always be in [0, worker_count).
  std::uint16_t block_owner = 0;
  std::uint32_t record_offset = 0;
  std::uint32_t total_disk_bytes = 0;
  std::uint64_t logical_size = 0;
  std::uint32_t payload_bytes = 0;
  std::uint32_t relocation_sequence = 0;
  bool in_memory = false;
  bool external = false;
  RecordKind kind = RecordKind::kValue;
  ValueType value_type = ValueType::kNone;
  std::shared_ptr<const std::vector<ExtentRef>> extents;

  bool SamePhysicalRecord(const RecordLocation& other) const noexcept {
    return block_id == other.block_id &&
           record_offset == other.record_offset &&
           allocation_epoch == other.allocation_epoch;
  }
};


using RecordIndex = ScanHashMap<RecordLocation>;

inline bool IsNewer(const RecordLocation& candidate,
             const RecordLocation& current) noexcept {
  if (candidate.replication_epoch != current.replication_epoch) {
    return candidate.replication_epoch > current.replication_epoch;
  }
  if (candidate.mutation_sequence != current.mutation_sequence) {
    return candidate.mutation_sequence > current.mutation_sequence;
  }
  if (candidate.relocation_sequence != current.relocation_sequence) {
    return candidate.relocation_sequence > current.relocation_sequence;
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
#else
#define KEYLANE_MAYBE_CRASH_AT(point) ((void)0)
#endif

inline std::uint64_t UnixTimeMillis() noexcept {
  const auto value = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
  return value > 0 ? static_cast<std::uint64_t>(value) : 0;
}

inline bool IsExpired(const RecordLocation& location,
               std::uint64_t now_ms) noexcept {
  return location.kind == RecordKind::kValue &&
         location.expire_at_ms != 0 && location.expire_at_ms <= now_ms;
}

inline StatusOr<std::shared_ptr<const std::vector<ExtentRef>>> DecodeManifest(
    std::span<const std::byte> payload, std::uint64_t logical_size) {
  if (payload.size() < sizeof(ExtentManifestHeader)) {
    return Status(StatusCode::kInternal, "external value manifest is truncated");
  }
  ExtentManifestHeader header{};
  std::memcpy(&header, payload.data(), sizeof(header));
  if (header.magic != kExtentManifestMagic ||
      header.version != kStorageFormatVersion || header.extent_count == 0 ||
      header.extent_count > kMaxStringExtents ||
      payload.size() != sizeof(header) +
                            static_cast<std::size_t>(header.extent_count) *
                                sizeof(ExtentRef)) {
    return Status(StatusCode::kInternal, "invalid external value manifest");
  }
  auto refs = std::make_shared<std::vector<ExtentRef>>(header.extent_count);
  std::memcpy(refs->data(), payload.data() + sizeof(header),
              refs->size() * sizeof(ExtentRef));
  std::uint64_t total = 0;
  for (const ExtentRef& ref : *refs) {
    if (ref.block_id == kInvalidBlockId || ref.allocation_epoch == 0 ||
        ref.payload_bytes == 0 || ref.payload_bytes > kExtentPayloadBytes ||
        total > kMaxStringBytes - ref.payload_bytes) {
      return Status(StatusCode::kInternal, "invalid extent reference");
    }
    total += ref.payload_bytes;
  }
  if (total != logical_size || total > kMaxStringBytes) {
    return Status(StatusCode::kInternal,
                  "extent manifest logical size mismatch");
  }
  return std::shared_ptr<const std::vector<ExtentRef>>(std::move(refs));
}

inline std::string EncodeManifest(std::span<const ExtentRef> refs) {
  ExtentManifestHeader header{.magic = kExtentManifestMagic,
                              .version = kStorageFormatVersion,
                              .extent_count =
                                  static_cast<std::uint32_t>(refs.size())};
  std::string output(sizeof(header) + refs.size_bytes(), '\0');
  std::memcpy(output.data(), &header, sizeof(header));
  std::memcpy(output.data() + sizeof(header), refs.data(), refs.size_bytes());
  return output;
}

struct ActiveBlock {
  std::uint64_t block_id = 0;
  std::uint32_t writer_id = 0;
  std::uint32_t layout_worker_count = 0;
  std::uint64_t allocation_epoch = 0;
  std::uint32_t committed_bytes = kBlockHeaderBytes;
  std::uint32_t record_count = 0;
  std::uint64_t max_lsn = 0;
  std::uint16_t write_buffer_id = 0;
  std::byte* heap_buffer = nullptr;
  std::size_t heap_buffer_size = 0;
  BlockKind kind = BlockKind::kRecords;
  std::uint32_t extent_index = 0;
  std::uint32_t extent_payload_checksum = 0;
};

// State that exists only while a block is held in memory behind a staging
// buffer: the buffer itself and the bookkeeping the flusher needs. At most a
// handful of blocks per worker are in that state at once, while every
// allocated block carries a BlockState, so this lives in a side table instead
// of costing all of them 38 bytes.
struct StagingSlot {
  std::uint16_t write_buffer_id = 0;
  std::byte* heap_data = nullptr;
  std::size_t heap_data_size = 0;
  // Bytes already written and fdatasynced. Direct-I/O aligned, so appends
  // never land in a durable page and a flush only writes the new tail.
  std::uint32_t durable_bytes = kBlockHeaderBytes;
  std::uint32_t record_count = 0;
  std::uint64_t max_lsn = 0;
  // Sequence stamped into the last header write. Its parity picks the slot,
  // so the header slot needs no field of its own.
  std::uint32_t header_sequence = 0;
  std::uint16_t next_free = 0;
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
  std::uint64_t allocation_epoch = 0;
  std::uint32_t committed_bytes = 0;
  std::uint32_t live_bytes = 0;
  std::uint32_t pins = 0;
  // Written only by the owner as it claims or releases the block; read by
  // anyone that needs to know where to dispatch. kUnownedBlock means free.
  std::atomic<std::uint16_t> owner{kUnownedBlock};
  std::uint16_t writer_id = 0;
  std::uint16_t layout_worker_count = 0;
  // Index into WorkerStore::staging_slots, or 0 when the block has no staging
  // buffer. Ids are 1-based so zero can mean "none".
  std::uint16_t staging_slot = 0;
  // Bitfields rather than bools: eight of these would otherwise cost a byte
  // each and push the struct past a cache line.
  bool allocated : 1 = false;
  bool defrag_queued : 1 = false;
  bool defragging : 1 = false;
  bool freeing : 1 = false;
  bool in_memory : 1 = false;
  bool flush_queued : 1 = false;
  bool flush_in_progress : 1 = false;
  bool release_pending : 1 = false;
  BlockKind kind = BlockKind::kRecords;

  // The atomic member makes this non-assignable, and clearing an entry has to
  // publish the new owner last so no one observes a half-reset block.
  void Reset(std::uint16_t new_owner) noexcept {
    allocation_epoch = 0;
    committed_bytes = 0;
    live_bytes = 0;
    pins = 0;
    writer_id = 0;
    layout_worker_count = 0;
    staging_slot = 0;
    allocated = false;
    defrag_queued = false;
    defragging = false;
    freeing = false;
    in_memory = false;
    flush_queued = false;
    flush_in_progress = false;
    release_pending = false;
    kind = BlockKind::kRecords;
    owner.store(new_owner, std::memory_order_release);
  }
};

// Every allocated block carries one of these for its whole life, and cold
// paths scan them in bulk, so keep two per cache line. Anything that only
// matters while a block is staged in memory belongs in StagingSlot, and
// anything only an extent block needs belongs in the recovery-scoped map.
static_assert(sizeof(BlockState) == 32);
static_assert(alignof(BlockState) == 32);

struct ExtentIdentity {
  std::uint32_t extent_index = 0;
  std::uint32_t payload_checksum = 0;
};

struct RecoveryRecord {
  Digest digest{};
  std::string key;
  std::uint8_t db_id = 0;
  RecordLocation location{};
};

struct RecoveryBlock {
  ActiveBlock block{};
};

struct RecoveryBatch {
  std::vector<RecoveryRecord> records;
  std::vector<RecoveryBlock> blocks;
};

struct RecoveryLiveReference {
  std::uint64_t block_id = 0;
  std::uint64_t allocation_epoch = 0;
  std::uint32_t bytes = 0;
  bool extent = false;
  std::uint32_t extent_index = 0;
  std::uint32_t extent_payload_checksum = 0;
};

// A relocated record cannot make its source block reclaimable until the
// destination block header durably covers this boundary. Keeping the fence
// independent of the in-memory index also makes later overwrites harmless:
// once this version is durable, recovery always has at least this copy or a
// newer relocation to choose from.
struct RelocationDurabilityFence {
  std::uint64_t block_id = 0;
  std::uint64_t allocation_epoch = 0;
  std::uint16_t block_owner = 0;
  std::uint32_t committed_bytes = 0;
};

// The accounting handle for a superseded record: enough to subtract it from
// its block's live_bytes once its replacement no longer needs it as the
// durable copy.
struct RetiredRecord {
  std::uint64_t block_id = 0;
  std::uint64_t allocation_epoch = 0;
  std::uint32_t total_disk_bytes = 0;
  std::uint16_t block_owner = 0;
};

// The index state a defrag relocation observed when it validated its source
// record. WriteRecordLocked can release writer_mutex while waiting for a
// standby block; if FLUSHDB detached the database or a replica reset rewrote
// the partition in that gap, the relocation would insert its (stale) copy
// into the successor index stamped with the successor's epochs — resurrecting
// a key the flush or reset just removed. Re-checking these before the append
// turns that into an aborted, retryable relocation instead.
struct RelocationSource {
  std::uint64_t db_epoch = 0;
  std::uint64_t replication_epoch = 0;
  std::uint64_t index_generation = 0;
};

// Back-pointer from a block to the index entries staged in its write buffer, so
// the flush completion can flip them to on-disk reads without re-hashing every
// key. The entry can outlive the index that owns it: FLUSHDB detaches every
// partition index for one database while blocks are still in flight. Recording
// which database generation produced the entry lets the completion detect that
// and skip the entry instead of following a pointer into a freed population.
struct RecordIdentity {
  RecordIndex::Entry* entry = nullptr;
  std::shared_ptr<const std::vector<ExtentRef>> retired_extents;
  // The version this record superseded. Retired only when this record's
  // flush completes: until the replacement is durable, the old copy is the
  // only durable version of the key, and subtracting it from live_bytes any
  // earlier lets the block reach zero and be durably freed — a crash before
  // the flush then loses a value that had already been made durable.
  std::optional<RetiredRecord> retired_record;
  std::uint64_t index_generation = 0;
  std::uint8_t db_id = 0;
};

struct ReplicaValueStage {
  std::uint8_t db_id = 0;
  std::uint64_t db_epoch = 0;
  std::uint64_t mutation_sequence = 0;
  std::uint64_t expire_at_ms = 0;
  std::uint64_t logical_size = 0;
  std::uint32_t next_chunk = 0;
  std::uint32_t chunk_count = 0;
  ValueType value_type = ValueType::kNone;
  std::string key;
  std::string value;
};

// One partition's worth of entries taken out of service by FLUSHDB. The entries
// are unreachable to readers the moment the index is detached, but the blocks
// they occupy still count them as live until the reclaimer subtracts them.
struct DetachedIndex {
  RecordIndex index;
  std::uint8_t db_id = 0;
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
  while (current < value &&
         !target->compare_exchange_weak(current, value,
                                        std::memory_order_relaxed)) {
  }
}

inline Status ReadExactlyAt(int fd, std::span<std::byte> output,
                     std::uint64_t offset) {
  std::size_t done = 0;
  while (done < output.size()) {
    const ssize_t read = ::pread(
        fd, output.data() + done, output.size() - done,
        static_cast<off_t>(offset + done));
    if (read < 0 && errno == EINTR) {
      continue;
    }
    if (read <= 0) {
      return Status(StatusCode::kInternal,
                    read == 0 ? "short device-label read"
                              : "device-label read failed: " +
                                    std::string(std::strerror(errno)));
    }
    done += static_cast<std::size_t>(read);
  }
  return Status::Ok();
}

inline Status WriteExactlyAt(int fd, std::span<const std::byte> input,
                      std::uint64_t offset) {
  std::size_t done = 0;
  while (done < input.size()) {
    const ssize_t written = ::pwrite(
        fd, input.data() + done, input.size() - done,
        static_cast<off_t>(offset + done));
    if (written < 0 && errno == EINTR) {
      continue;
    }
    if (written <= 0) {
      return Status(StatusCode::kInternal,
                    "device-label write failed: " +
                        std::string(std::strerror(errno)));
    }
    done += static_cast<std::size_t>(written);
  }
  return Status::Ok();
}

inline StatusOr<std::optional<DeviceLabel>> ReadDeviceLabel(
    const std::string& path) {
  const int fd = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
  if (fd < 0) {
    return Status(StatusCode::kInternal,
                  "open device label failed: " + path + ": " +
                      std::strerror(errno));
  }
  std::array<std::byte, kDirectIoAlignment> page{};
  Status status = ReadExactlyAt(fd, page, kDeviceLabelOffset);
  const int close_error = ::close(fd);
  if (!status.ok()) {
    return status;
  }
  if (close_error != 0) {
    return Status(StatusCode::kInternal,
                  "close after device-label read failed: " + path);
  }
  if (IsZero(page)) {
    return std::optional<DeviceLabel>{};
  }
  DeviceLabel label{};
  if (!DecodeDeviceLabel(page, &label)) {
    return Status(StatusCode::kInternal,
                  "invalid or corrupt device label: " + path);
  }
  return std::optional<DeviceLabel>{label};
}

inline Status WriteDeviceLabel(const std::string& path, const DeviceLabel& label) {
  const int fd = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
  if (fd < 0) {
    return Status(StatusCode::kInternal,
                  "open device label for write failed: " + path + ": " +
                      std::strerror(errno));
  }
  std::array<std::byte, kDirectIoAlignment> page{};
  EncodeDeviceLabel(label, page);
  Status status = WriteExactlyAt(fd, page, kDeviceLabelOffset);
  if (status.ok() && ::fdatasync(fd) != 0) {
    status = Status(StatusCode::kInternal,
                    "device-label fdatasync failed: " + path + ": " +
                        std::strerror(errno));
  }
  const int close_error = ::close(fd);
  if (!status.ok()) {
    return status;
  }
  if (close_error != 0) {
    return Status(StatusCode::kInternal,
                  "close after device-label write failed: " + path);
  }
  return Status::Ok();
}

struct MetadataPageState {
  std::uint64_t generation = 0;
  std::uint8_t active_slot = 0;
};

struct LoadedMetadataPage {
  std::vector<std::byte> payload;
  MetadataPageState state{};
};

inline StatusOr<LoadedMetadataPage> ReadMetadataPagePair(
    int fd, std::uint64_t base_offset, MetadataPageKind kind,
    std::uint32_t page_index, std::size_t payload_bytes) {
  LoadedMetadataPage selected;
  selected.payload.resize(payload_bytes, std::byte{0});
  bool saw_nonzero = false;
  bool selected_valid = false;
  for (unsigned slot = 0; slot < 2; ++slot) {
    std::array<std::byte, kDirectIoAlignment> page{};
    Status read = ReadExactlyAt(
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
    if (!selected_valid || generation > selected.state.generation) {
      selected.payload = std::move(payload);
      selected.state.generation = generation;
      selected.state.active_slot = static_cast<std::uint8_t>(slot);
      selected_valid = true;
    }
  }
  if (saw_nonzero && !selected_valid) {
    return Status(StatusCode::kInternal,
                  "both fixed-metadata page slots are corrupt");
  }
  return selected;
}

inline StatusOr<std::uint64_t> RandomStorageSetId() {
  std::uint64_t value = 0;
  while (value == 0) {
    const ssize_t bytes = ::getrandom(&value, sizeof(value), 0);
    if (bytes < 0 && errno == EINTR) {
      continue;
    }
    if (bytes != static_cast<ssize_t>(sizeof(value))) {
      return Status(StatusCode::kInternal,
                    "getrandom for storage-set id failed: " +
                        std::string(std::strerror(errno)));
    }
  }
  return value;
}

struct StorageDevice {
  std::string path;
  std::uint64_t id = 0;
  std::uint64_t capacity_blocks = 0;
  std::uint32_t data_block_begin = 1;
  std::uint64_t data_block_count = 0;
  std::uint32_t file_index = 0;
};

inline constexpr std::size_t kCacheLineBytes = 64;

struct ReservedBlock {
  std::uint64_t block_id = 0;
  std::uint64_t allocation_epoch = 0;
};

struct alignas(kCacheLineBytes) RecoveryDeviceCursor {
  std::atomic<std::uint64_t> next_local{1};
  std::atomic<std::uint64_t> next_allocation_epoch{1};
};

static_assert(sizeof(RecoveryDeviceCursor) % kCacheLineBytes == 0);

struct DeviceAllocator {
  celer::WorkerId owner = 0;
  AsyncMutex mutex;
  std::uint32_t data_block_begin = 1;
  std::uint64_t next_pristine = 1;
  std::uint64_t next_allocation_epoch = 1;
  std::vector<std::uint64_t> ready_blocks;
  std::vector<std::uint64_t> cold_free;
  std::vector<std::byte> scan_bitmap;
  std::vector<MetadataPageState> bitmap_pages;
  std::vector<MetadataPageState> epoch_pages;
  std::vector<std::uint64_t> epoch_values;
  std::vector<std::uint64_t> durable_epoch_values;
  std::optional<Status> failed;
};

struct BlockDeviceInfo {
  std::size_t io_alignment = 0;
  std::uint64_t size_bytes = 0;
};

struct StoragePathInfo {
  bool is_block_device = false;
  std::size_t io_alignment = kDirectIoAlignment;
  std::uint64_t size_bytes = 0;
};

inline StatusOr<BlockDeviceInfo> ProbeBlockDevice(const std::string& path) {
  const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    return Status(StatusCode::kInternal,
                  "open block device for probe failed: " + path +
                      ": " + std::strerror(errno));
  }

  int logical_block_bytes = 0;
  std::uint64_t size_bytes = 0;
  const int sector_error = ::ioctl(fd, BLKSSZGET, &logical_block_bytes);
  const int sector_errno = errno;
  const int size_error = ::ioctl(fd, BLKGETSIZE64, &size_bytes);
  const int size_errno = errno;
  const int close_error = ::close(fd);
  if (sector_error != 0) {
    return Status(StatusCode::kInternal,
                  "BLKSSZGET failed: " + path + ": " +
                      std::strerror(sector_errno));
  }
  if (size_error != 0) {
    return Status(StatusCode::kInternal,
                  "BLKGETSIZE64 failed: " + path + ": " +
                      std::strerror(size_errno));
  }
  if (close_error != 0) {
    return Status(StatusCode::kInternal,
                  "close block device after alignment probe failed: " + path);
  }

  const auto alignment = static_cast<std::size_t>(logical_block_bytes);
  if (alignment == 0 || (alignment & (alignment - 1)) != 0) {
    return Status(StatusCode::kInternal,
                  "block device logical sector size is not a power of two: " +
                      path);
  }
  return BlockDeviceInfo{.io_alignment = alignment, .size_bytes = size_bytes};
}

inline StatusOr<StoragePathInfo> ProbeStoragePath(const std::string& path) {
  struct stat file_info {};
  if (::stat(path.c_str(), &file_info) != 0) {
    return Status(StatusCode::kInternal,
                  "stat storage path failed: " + path + ": " +
                      std::strerror(errno));
  }
  if (S_ISBLK(file_info.st_mode)) {
    auto device = ProbeBlockDevice(path);
    if (!device.ok()) {
      return device.status();
    }
    return StoragePathInfo{
        .is_block_device = true,
        .io_alignment = device->io_alignment,
        .size_bytes = device->size_bytes,
    };
  }
  if (!S_ISREG(file_info.st_mode)) {
    return Status(StatusCode::kInvalidArgument,
                  "storage path is neither a regular file nor a block device: " +
                      path);
  }
  if (file_info.st_size < 0) {
    return Status(StatusCode::kOutOfRange,
                  "storage file reports a negative size: " + path);
  }
  return StoragePathInfo{
      .is_block_device = false,
      .io_alignment = kDirectIoAlignment,
      .size_bytes = static_cast<std::uint64_t>(file_info.st_size),
  };
}

inline Task<StatusOr<std::size_t>> ReadStorageBuffer(
    Worker& worker, FixedFile file, FixedBuffer buffer, bool registered,
    std::uint64_t offset) {
  if (registered) {
    co_return co_await celer::ReadFixed(worker, file, buffer, offset);
  }
  co_return co_await celer::Read(
      worker, file, std::span<std::byte>(buffer.data, buffer.size), offset);
}

inline Task<StatusOr<std::size_t>> WriteStorageBuffer(
    Worker& worker, FixedFile file,
    std::span<const std::byte> buffer, bool registered,
    FixedBuffer registered_buffer, std::uint64_t offset) {
  if (registered) {
    celer::FixedBuffer target = {
        .data = const_cast<std::byte*>(buffer.data()),
        .size = buffer.size(),
        .index = registered_buffer.index,
    };
    if (target.index == 0 || target.data == nullptr ||
        target.size > registered_buffer.size) {
      co_return Status(StatusCode::kInternal,
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
      std::uint16_t id = 0;
      std::array<RecordIndex, kLogicalDatabaseCount> indexes;
      std::array<std::size_t, kLogicalDatabaseCount> live_key_count{};
      std::array<std::size_t, kLogicalDatabaseCount> expiring_key_count{};
      std::uint64_t mutation_sequence = 0;
      std::uint64_t replication_epoch = 1;
      std::uint64_t delta_floor = 0;
      bool capture_deltas = false;
      bool delta_queued = false;
      std::deque<SnapshotRecord> deltas;
      std::optional<ReplicaValueStage> replica_value_stage;
    };

    struct ExpireCandidate {
      std::uint16_t partition_id = 0;
      std::uint8_t db_id = 0;
      Digest digest{};
      std::uint64_t mutation_sequence = 0;
      std::uint64_t expire_at_ms = 0;
      std::string key;
    };

    Worker* worker = nullptr;
    RegisteredBufferPool buffers;
    std::vector<FixedFile> files;
    std::vector<PartitionStore> partitions;
    absl::flat_hash_map<std::uint64_t, std::vector<RecordIdentity>>
        staged_records;
    // Bumped every time FLUSHDB detaches this database's partition indexes.
    // Every index for one database is detached together and without suspending,
    // so one counter per database describes all of them.
    std::array<std::uint64_t, kLogicalDatabaseCount> index_generations{};
    // Populations detached by FLUSHDB, still holding their entries. Draining
    // this is what actually frees them and settles the block accounting.
    std::deque<DetachedIndex> detached_indexes;
    bool detached_reclaim_running = false;
    std::array<std::size_t, kLogicalDatabaseCount> live_key_count{};
    std::optional<ActiveBlock> active_block;
    std::optional<ReservedBlock> standby_block;
    std::optional<Status> standby_error;
    AsyncNotification standby_ready;
    // Recovery only. A recovered extent block's identity has to be checked
    // against the manifests that reference it, and the two arrive in separate
    // passes, so they meet here instead of in every BlockState. Cleared once
    // the live-reference pass has run.
    absl::flat_hash_map<std::uint64_t, ExtentIdentity> recovered_extents;
    // Index 0 is the "no staging buffer" sentinel. A deque keeps references
    // stable as the table grows, since heap fallback buffers are unbounded.
    std::deque<StagingSlot> staging_slots{1};
    std::uint16_t free_staging_slot = 0;
    AsyncMutex writer_mutex;
    std::deque<std::uint64_t> flush_queue;
    std::deque<std::uint64_t> defrag_queue;
    std::vector<std::size_t> home_devices;
    std::vector<std::uint64_t> home_device_allocations;
    bool flush_running = false;
    bool write_failed = false;
    bool defrag_running = false;
    bool defrag_waiting = false;
    std::size_t defrag_waiting_device = 0;
    std::size_t active_defrag_device = 0;
    bool standby_request_pending = false;
    std::size_t expiry_partition_cursor = 0;
    std::uint8_t expiry_db_cursor = 0;
    std::uint64_t expiry_scan_cursor = 0;
    std::deque<ExpireCandidate> expired_candidates;
  };

  Status Prepare(unsigned worker_count);

  Task<Status> InitializeWorker(Worker& worker);

  unsigned OwnerForKey(std::string_view key) const noexcept {
    return StorageShardForKey(key) % worker_count_;
  }

  Task<StatusOr<DiskValue>> Get(std::uint8_t db_id, std::string_view key,
                                ReadLatencyTrace* trace);

  // Caller holds this worker's key lock for `digest` (shared) and runs on
  // OwnerForKey(key). `digest` must equal ComputeDigest(key).
  Task<StatusOr<DiskValue>> GetLocked(std::uint8_t db_id, std::string_view key,
                                      const Digest& digest,
                                      ReadLatencyTrace* trace);

  Task<StatusOr<std::uint64_t>> StringLength(std::uint8_t db_id,
                                             std::string_view key);

  // Caller holds the key lock (shared); see GetLocked.
  Task<StatusOr<std::uint64_t>> StringLengthLocked(std::uint8_t db_id,
                                                   std::string_view key,
                                                   const Digest& digest);

  Task<StatusOr<SetResult>> Set(std::uint8_t db_id, std::string_view key,
                                std::string_view value,
                                SetOptions options);

  // Caller holds the key lock (exclusive); takes writer_mutex internally.
  Task<StatusOr<SetResult>> SetLocked(std::uint8_t db_id, std::string_view key,
                                      const Digest& digest,
                                      std::string_view value,
                                      SetOptions options);

  Task<ExpirationInfo> GetExpiration(std::uint8_t db_id,
                                     std::string_view key);

  // Caller holds the key lock (shared); see GetLocked.
  Task<ExpirationInfo> GetExpirationLocked(std::uint8_t db_id,
                                           std::string_view key,
                                           const Digest& digest);

  Task<StatusOr<bool>> UpdateExpiration(
      std::uint8_t db_id, std::string_view key,
      std::uint64_t expire_at_ms, ExpirationCondition condition);

  // Caller holds the key lock (exclusive); takes writer_mutex internally.
  Task<StatusOr<bool>> UpdateExpirationLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      std::uint64_t expire_at_ms, ExpirationCondition condition);

  Task<StatusOr<bool>> Delete(std::uint8_t db_id, std::string_view key);

  // Caller holds the key lock (exclusive); takes writer_mutex internally.
  Task<StatusOr<bool>> DeleteLocked(std::uint8_t db_id, std::string_view key,
                                    const Digest& digest);

  // Freezes the keyspace against expiration writes for stable-count scans
  // (KEYS): client writes are already excluded by the closed database gate;
  // this stops the active-expiry loop and drains any in-flight append by
  // bouncing off every worker's writer mutex.
  Task<Status> QuiesceExpiration();

  void ResumeExpiration() noexcept {
    expiration_paused_.store(false, std::memory_order_release);
  }

  bool KeyLive(std::uint8_t db_id, std::string_view key,
               const Digest& digest) const;

  Task<bool> Exists(std::uint8_t db_id, std::string_view key);

  // Caller holds the key lock (shared); see GetLocked.
  Task<bool> ExistsLocked(std::uint8_t db_id, std::string_view key,
                          const Digest& digest);

  Task<StatusOr<std::int64_t>> Increment(std::uint8_t db_id,
                                         std::string_view key);

  // Caller holds the key lock (exclusive); takes writer_mutex internally.
  Task<StatusOr<std::int64_t>> IncrementLocked(std::uint8_t db_id,
                                               std::string_view key,
                                               const Digest& digest);

  unsigned worker_count() const noexcept { return worker_count_; }

  std::size_t LocalSize(std::uint8_t db_id) const noexcept {
    assert(db_id < kLogicalDatabaseCount);
    return CurrentStore().live_key_count[db_id];
  }

  std::uint64_t DbEpoch(std::uint8_t db_id) const noexcept {
    assert(db_id < kLogicalDatabaseCount);
    return db_epochs_[db_id].load(std::memory_order_acquire);
  }

  Task<Status> FlushDbDetach(std::uint8_t db_id);

  Task<Status> FlushDbReclaim(bool wait) {
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
  Task<Status> DetachDbEpoch(std::uint8_t db_id, std::uint64_t next);

  // Retires what DetachDbEpoch took out of service. Runs with the gate open and
  // ordinary traffic flowing. `wait` is the difference between FLUSHDB SYNC and
  // FLUSHDB ASYNC: either way a reclaimer runs, only the reply waits or not.
  Task<Status> ReclaimDetachedAllWorkers(bool wait);

  Task<Status> AdvanceDbEpoch(std::uint8_t db_id, std::uint64_t next);

  ScanBatch ScanPartition(std::uint16_t partition_id, std::uint8_t db_id,
                          std::uint64_t cursor, std::size_t count,
                          std::uint64_t now_ms) const;

  PartitionReplicationStart BeginPartitionReplication(
      std::uint16_t partition_id);

  Task<StatusOr<PartitionSnapshotBatch>> SnapshotPartition(
      std::uint16_t partition_id, std::uint8_t db_id,
      std::uint64_t cursor, std::size_t count);

  PartitionDeltaBatch ReadPartitionDeltas(
      std::uint16_t partition_id, std::uint64_t after_sequence,
      std::size_t count);

  void AcknowledgePartitionDeltas(std::uint16_t partition_id,
                                  std::uint64_t through_sequence);

  bool TryTakeReplicationReady(std::uint16_t* partition_id) {
    return partition_id != nullptr &&
           replication_ready_.try_dequeue(*partition_id);
  }

  Task<StatusOr<std::uint64_t>> ResetReplicaPartition(
      std::uint16_t partition_id,
      std::span<const std::uint64_t, kLogicalDatabaseCount> source_db_epochs);

  Task<Status> ApplyReplicaRecords(
      std::uint16_t partition_id, std::uint64_t replication_epoch,
      std::span<const SnapshotRecord> records);

  Status FlushForShutdown();

 private:
  static StagingSlot* StagingFor(WorkerStore& store, const BlockState& state);

  static std::uint16_t AcquireStagingSlot(WorkerStore& store);

  FixedBuffer StagingBufferFor(WorkerStore& store,
                               const BlockState& state) const;

  static void ReleaseStagingBuffer(WorkerStore& store, BlockState& state);

  struct LoadedValue {
    ReadBufferLease lease;
    std::size_t value_offset = 0;
    std::size_t value_bytes = 0;

    std::span<const std::byte> value() const noexcept {
      const std::span<std::byte> buffer = lease.bytes();
      if (value_offset > buffer.size() ||
          value_bytes > buffer.size() - value_offset) {
        return {};
      }
      return buffer.subspan(value_offset, value_bytes);
    }
  };

  std::size_t DirectGetValueLimit() const noexcept;

  StatusOr<DiskValue> EncodeDiskValue(LoadedValue loaded);

  void QueueExpiredCandidate(WorkerStore& store, std::uint16_t partition_id,
                             std::uint8_t db_id,
                             const RecordIndex::Entry& entry);

  WorkerStore& CurrentStore() {
    return *stores_[celer::ThisWorker().id];
  }

  const WorkerStore& CurrentStore() const {
    return *stores_[celer::ThisWorker().id];
  }

  WorkerStore::PartitionStore& PartitionFor(
      WorkerStore& store, std::uint16_t partition_id) const {
    assert(partition_id < kLogicalStorageShards);
    assert(partition_id % worker_count_ == store.worker->id());
    auto& partition = store.partitions[partition_id / worker_count_];
    assert(partition.id == partition_id);
    return partition;
  }

  const WorkerStore::PartitionStore& PartitionFor(
      const WorkerStore& store, std::uint16_t partition_id) const {
    assert(partition_id < kLogicalStorageShards);
    assert(partition_id % worker_count_ == store.worker->id());
    const auto& partition = store.partitions[partition_id / worker_count_];
    assert(partition.id == partition_id);
    return partition;
  }

  WorkerStore::PartitionStore& PartitionForKey(
      WorkerStore& store, std::string_view key) const {
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
    if (state.owner.load(std::memory_order_acquire) != store.worker->id()) {
      return nullptr;
    }
    return state.allocated ? &state : nullptr;
  }

  const BlockState* FindBlockState(
      const WorkerStore& store, std::uint64_t block_id) const noexcept {
    const BlockState& state = const_cast<Impl*>(this)->BlockStateAt(block_id);
    if (state.owner.load(std::memory_order_acquire) != store.worker->id()) {
      return nullptr;
    }
    return state.allocated ? &state : nullptr;
  }

  BlockState& CreateBlockState(WorkerStore& store, std::uint64_t block_id) {
    BlockState& state = BlockStateAt(block_id);
    state.Reset(static_cast<std::uint16_t>(store.worker->id()));
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
    const std::uint16_t me = static_cast<std::uint16_t>(store.worker->id());
    for (std::size_t device_index = 0; device_index < devices_.size();
         ++device_index) {
      const StorageDevice& device = devices_[device_index];
      std::vector<BlockState>& states = device_block_states_[device_index];
      for (std::size_t slot = 0; slot < states.size(); ++slot) {
        BlockState& state = states[slot];
        if (state.owner.load(std::memory_order_acquire) != me ||
            !state.allocated) {
          continue;
        }
        fn(MakeBlockId(device.id, static_cast<std::uint32_t>(
                                      slot + device.data_block_begin)),
           state);
      }
    }
  }

  std::size_t DeviceIndexForBlock(std::uint64_t block_id) const noexcept;

  std::pair<std::uint32_t, std::uint64_t> FileOffset(
      std::uint64_t block_id) const noexcept {
    const StorageDevice& device = devices_[DeviceIndexForBlock(block_id)];
    assert(LocalBlockId(block_id) >= device.data_block_begin);
    assert(LocalBlockId(block_id) < device.capacity_blocks);
    return {device.file_index, LocalBlockOffset(block_id)};
  }

  static bool BitmapBit(const DeviceAllocator& allocator,
                        std::uint32_t local_block) noexcept;

  static void SetBitmapBit(DeviceAllocator& allocator,
                           std::uint32_t local_block) noexcept;

  static void ClearBitmapBit(DeviceAllocator& allocator,
                             std::uint32_t local_block) noexcept;

  Task<Status> PersistBitmapPages(
      std::size_t device_index, DeviceAllocator& allocator,
      std::vector<std::size_t> page_indexes);

  Task<Status> InvalidateReactivatedBlockHeadersLocal(
      std::size_t device_index,
      std::span<const std::uint64_t> block_ids);

  Task<Status> RefillReadyBlocksLocal(std::size_t device_index,
                                      DeviceAllocator& allocator);

  Task<StatusOr<ReservedBlock>> AllocateFromDeviceLocal(
      std::size_t device_index, bool for_defrag);

  Task<StatusOr<ReservedBlock>> AllocateFromDevice(
      std::size_t device_index, bool for_defrag);

  Task<Status> ReturnColdBlocksLocal(
      std::size_t device_index, std::vector<std::uint64_t> block_ids);

  Task<Status> ReturnColdBlocks(std::vector<std::uint64_t> block_ids);

  Task<Status> PersistEpochValueOnDeviceLocal(std::size_t device_index,
                                              std::size_t value_index,
                                              std::uint64_t epoch);

  Task<Status> PersistEpochValue(std::size_t value_index,
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
  Task<Status> ReclaimDetachedIndexes(WorkerStore& store);

  // At most one reclaimer per store, so the two ways in — a background one that
  // FLUSHDB ASYNC leaves behind, and a caller waiting for SYNC — never split a
  // population between them. Whichever runs picks up work queued after it
  // started, so an arriving FLUSHDB only has to make sure one is alive.
  void EnsureDetachedReclaim(WorkerStore& store);

  Task<Status> RunDetachedReclaim(WorkerStore* store);

  // Waits for everything detached so far to be retired. The keyspace is already
  // empty either way; this is what makes FLUSHDB SYNC mean the memory came back
  // before the reply.
  Task<Status> AwaitDetachedReclaim(WorkerStore& store);

  void Fail(const Status& status);

  // Block states live in one dense array per device, indexed by local block
  // id. Each array is sized once at startup and never resized, so entries
  // never move and a BlockState* stays valid across suspension points.
  BlockState& BlockStateAt(std::uint64_t block_id) noexcept {
    const std::size_t device_index = DeviceIndexForBlock(block_id);
    const StorageDevice& device = devices_[device_index];
    const std::uint32_t local = LocalBlockId(block_id);
    assert(local >= device.data_block_begin);
    assert(local < device.capacity_blocks);
    return device_block_states_[device_index][local - device.data_block_begin];
  }

  // Which worker owns a block, or kUnownedBlock if it is free. This is the one
  // field a non-owner may read, so it is the only one that is atomic.
  std::uint16_t BlockOwner(std::uint64_t block_id) const noexcept {
    const std::size_t device_index = DeviceIndexForBlock(block_id);
    const StorageDevice& device = devices_[device_index];
    const std::uint32_t local = LocalBlockId(block_id);
    if (device_block_states_.empty() || local < device.data_block_begin ||
        local >= device.capacity_blocks) {
      return kUnownedBlock;
    }
    return device_block_states_[device_index][local - device.data_block_begin]
        .owner.load(std::memory_order_acquire);
  }

  std::uint16_t RecoveredBlockOwner(const BlockHeader& block,
                                    std::uint64_t block_id) const noexcept;


  // `allocated` marks blocks the scan bitmap said were in use — the only ones
  // that cost I/O. Free blocks are skipped without a read, so ETA and percent
  // are computed over allocated blocks; the capacity-wide sweep count only
  // detects completion.
  void ReportRecoveryProgress(std::uint64_t records, bool allocated);
  Task<Status> ScanAssignedBlocks(WorkerStore& store,
                                  std::vector<RecoveryBatch>* batches,
                                  std::vector<std::uint64_t>* zero_blocks);

  void ApplyRecovery(unsigned target, RecoveryBatch batch);

  Task<StatusOr<LoadedValue>> LoadValue(WorkerStore& key_store,
                                        std::uint8_t db_id,
                                        std::string_view key,
                                        const Digest& digest,
                                        RecordLocation location,
                                        ReadLatencyTrace* trace = nullptr);

  // Reads one extent block's payload into `destination`. Runs on the worker
  // that owns that block, which is not necessarily the one holding the
  // manifest, so everything it needs is passed by value.
  Task<Status> ReadExtentInto(WorkerStore& store, ExtentRef ref,
                              std::uint32_t extent_index,
                              std::byte* destination);

  Task<StatusOr<LoadedValue>> LoadExternalValueLocal(
      WorkerStore& store, const RecordLocation& location,
      ReadLatencyTrace* trace);

  Task<StatusOr<LoadedValue>> LoadValueLocal(
      WorkerStore& store, std::uint8_t db_id, std::string_view key,
      const Digest& digest, RecordLocation location,
      ReadLatencyTrace* trace = nullptr);

  std::uint64_t ForegroundBlocksForDevice(
      std::size_t device_index) const noexcept {
    const std::uint64_t data_blocks = devices_[device_index].data_block_count;
    const std::size_t reserve = DefragReserveForDevice(device_index);
    return data_blocks > reserve ? data_blocks - reserve : 0;
  }

  void ConfigureDefragReserves() {
    defrag_reserve_blocks_.assign(devices_.size(),
                                  kDefragReserveBlocksPerDevice);
  }

  void ConfigureWorkerDeviceAffinity();

  Task<StatusOr<ReservedBlock>> AllocateBlock(WorkerStore& store,
                                              bool for_defrag);

  std::size_t DefragReserveForDevice(std::size_t device_index) const noexcept {
    assert(device_index < defrag_reserve_blocks_.size());
    return defrag_reserve_blocks_[device_index];
  }

  Status MarkRecordDeadLocal(unsigned owner, const RetiredRecord& record);

  Task<Status> MarkRecordDead(const RetiredRecord& record);

  Task<Status> MarkRetiredRecordsDead(WorkerStore* store,
                                      std::vector<RetiredRecord> records);

  static RetiredRecord RetiredRecordOf(const RecordLocation& location) {
    return RetiredRecord{
        .block_id = location.block_id,
        .allocation_epoch = location.allocation_epoch,
        .total_disk_bytes = location.total_disk_bytes,
        .block_owner = location.block_owner,
    };
  }

  Task<StatusOr<ReservedBlock>> TakeStandaloneBlockLocked(
      WorkerStore& store);

  Task<StatusOr<std::shared_ptr<const std::vector<ExtentRef>>>>
  WriteExtentValueLocked(WorkerStore& store, std::string_view value);

  Task<Status> AppendLocked(WorkerStore& store,
                            WorkerStore::PartitionStore& partition,
                            std::uint8_t db_id,
                            std::string_view key, std::string_view value,
                            RecordKind kind, ValueType value_type,
                            std::uint64_t expire_at_ms);

  void AppendDelta(WorkerStore::PartitionStore& partition,
                   SnapshotRecord record);

  Task<Status> FetchStandbyBlock(WorkerStore* store, bool for_defrag);

  void RequestStandbyBlock(WorkerStore& store, bool for_defrag);

  void MaybePrefetchStandby(WorkerStore& store);

  Task<Status> WaitForStandbyWithWriterUnlocked(WorkerStore& store,
                                                bool for_defrag);

  Task<Status> WaitForStandbyWithWriterLocked(WorkerStore& store,
                                              bool for_defrag);

  Task<Status> WriteRecordLocked(WorkerStore& store, std::uint8_t db_id,
                                 std::string_view key, std::string_view value,
                                 RecordKind kind, ValueType value_type,
                                 std::uint64_t expire_at_ms,
                                 const Digest& digest,
                                 std::uint64_t generation,
                                 std::uint64_t mutation_sequence,
                                 std::uint64_t relocation_sequence,
                                 bool for_defrag,
                                 bool unlock_writer_while_waiting = true,
                                 bool external = false,
                                 std::uint64_t logical_size =
                                     std::numeric_limits<std::uint64_t>::max(),
                                 std::shared_ptr<const std::vector<ExtentRef>>
                                     extents = nullptr,
                                 RecordLocation* written_location = nullptr,
                                 const RelocationSource* relocation = nullptr);

  void SealActiveBlocks(WorkerStore& store);

  // Make the active block's tail durable without retiring it. The block stays
  // open for appends, so a slow writer no longer burns a whole 8 MiB block per
  // flush interval; it pays at most one padding page instead.
  void FlushActiveBlock(WorkerStore& store);

  void SealDeadActiveBlock(WorkerStore& store);

  Task<Status> FlushWorkerForShutdown(WorkerStore* store);

  void CompleteShutdownFlush(const Status& status);

  void AdvanceExpiryMap(WorkerStore& store);

  Task<Status> ExpireCandidate(WorkerStore& store,
                               WorkerStore::ExpireCandidate candidate);

  Task<Status> ActiveExpiration(WorkerStore* store);

  Task<Status> PeriodicFlush(WorkerStore* store);

  // Spawn an asynchronous extent reclaim, counted from before the spawn so
  // the block allocator's full-device check always sees it in flight.
  void SpawnExtentReclaim(WorkerStore& store,
                          std::shared_ptr<const std::vector<ExtentRef>> extents);

  Task<Status> ReclaimExtentsCounted(
      WorkerStore* store,
      std::shared_ptr<const std::vector<ExtentRef>> extents);

  // Retires one extent block. Runs on that block's owner, which after a
  // worker-count change is unrelated to the owner of the manifest that
  // referenced it. Reports whether the block became free.
  Task<StatusOr<bool>> ReclaimExtentLocal(WorkerStore& store, ExtentRef ref);

  Task<Status> ReclaimExtents(
      WorkerStore* store,
      std::shared_ptr<const std::vector<ExtentRef>> extents);

  void RequestFlush(WorkerStore& store, std::uint64_t block_id);

  Task<Status> FlushPendingBlocks(WorkerStore* store);

  bool IsActiveBlock(const WorkerStore& store,
                     std::uint64_t block_id) const noexcept;

  bool IsDefragCandidate(const WorkerStore& store,
                         std::uint64_t block_id) const noexcept;

  void MaybeQueueDefrag(WorkerStore& store, std::uint64_t block_id);

  bool TryAcquireDefragPermit(std::size_t device_index);

  void ReleaseDefragPermit(std::size_t device_index);

  Task<Status> StartQueuedDefrag(unsigned worker_id,
                                 std::size_t device_index);

  Task<Status> WakeQueuedDefrags(std::size_t device_index);

  void RequestDefrag(WorkerStore& store);

  void FinishDefragPass(WorkerStore& store);

  Task<Status> DefragOne(WorkerStore* store);

  Task<StatusOr<std::optional<RelocationDurabilityFence>>>
  RelocateIfCurrent(unsigned key_owner, std::string_view key,
                    std::string_view value, const RecordHeader& record,
                    const RecordLocation& source_location);

  Task<Status> AwaitRelocationDurableLocal(
      WorkerStore& store, const RelocationDurabilityFence& fence);

  Task<Status> AwaitRelocationDurable(
      const RelocationDurabilityFence& fence);

  Task<Status> CleanBlockLocked(WorkerStore& store,
                                std::uint64_t block_id);

  // Rewrites every record in the block that is still current, so the block ends
  // up with no reachable data and the caller can free it. Clears `defragging`
  // on each failure path so the block stays eligible for a later pass.
  Task<Status> SalvageBlockRecords(WorkerStore& store, std::uint64_t block_id,
                                   BlockState& source,
                                   std::uint32_t source_file_id,
                                   std::uint64_t source_block_offset);

  // Drains readers and hands the block back to the allocator. The caller must
  // have observed live_bytes == 0 under writer_mutex and set `freeing`, which
  // stops LoadValueLocal from taking new pins.
  Task<Status> ReleaseEmptyBlock(WorkerStore& store, std::uint64_t block_id,
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
  std::atomic<bool> expiration_paused_{false};
  std::vector<std::unique_ptr<WorkerStore>> stores_;
  std::unique_ptr<CoroutineBarrier> open_barrier_;
  std::unique_ptr<CoroutineBarrier> metadata_barrier_;
  std::unique_ptr<CoroutineBarrier> recovery_barrier_;
  std::unique_ptr<CoroutineBarrier> recovery_accounting_barrier_;
  std::unique_ptr<CoroutineBarrier> free_list_barrier_;
  std::atomic<std::uint64_t> recovery_scanned_blocks_{0};
  std::atomic<std::uint64_t> recovery_scanned_records_{0};
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
