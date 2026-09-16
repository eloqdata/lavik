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

#include <fcntl.h>
#include <linux/fs.h>

#include "lavik/storage/engine.h"
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
#include <map>
#include <mutex>
#include <new>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "../ring_buffer.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "bycorf/io/spdk_storage.h"
#include "bycorf/io/storage.h"
#include "bycorf/runtime/concurrentqueue.h"
#include "bycorf/runtime/cross_core.h"
#include "bycorf/runtime/sync.h"
#include "bycorf/runtime/worker.h"
#include "lavik/fault_injection.h"
#include "lavik/memory.h"
#include "lavik/storage/detail/compact_write.h"
#include "lavik/storage/detail/grouped_object_index.h"
#include "lavik/storage/detail/hash_codec.h"
#include "lavik/storage/detail/record_index.h"
#include "lavik/storage/detail/record_payload_cursor.h"
#include "lavik/storage/detail/replica_collection_stage.h"
#include "lavik/storage/format.h"
#include "lavik/storage/scan_hash_map.h"
#include "lavik/storage/tx_cleaner.h"
#include "lavik/tx/tx_shard.h"
#include "spdlog/spdlog.h"

namespace lavik::storage {

using bycorf::AsyncMutex;
using bycorf::AsyncNotification;
using bycorf::CoroutineBarrier;
using bycorf::FixedBuffer;
using bycorf::FixedFile;
using bycorf::Task;
using bycorf::Worker;

// Storage operations occasionally pin immutable physical state, release the
// worker-local metadata mutex across IO, then reacquire it for publication.
class UnlockGuard {
 public:
  UnlockGuard(AsyncMutex* mutex, Worker* worker)
      : mutex_(mutex), worker_(worker) {}
  UnlockGuard(const UnlockGuard&) = delete;
  UnlockGuard& operator=(const UnlockGuard&) = delete;
  ~UnlockGuard() { Unlock(); }

  void Unlock() {
    if (!owns_) return;
    mutex_->Unlock(*worker_);
    owns_ = false;
  }
  void Adopt() { owns_ = true; }

 private:
  AsyncMutex* mutex_;
  Worker* worker_;
  bool owns_ = true;
};

using ExtentManifest = std::shared_ptr<const std::vector<ExtentRef>>;

#if LAVIK_FAULTS_ENABLED
// Deterministic write-fault injection for rollback tests: a tagged write of
// the key named in LAVIK_FAIL_TX_WRITE fails instead of appending.
inline bool MaybeFailTxWrite(std::string_view key) noexcept {
  static const char* const armed = std::getenv("LAVIK_FAIL_TX_WRITE");
  if (armed == nullptr || key != armed) return false;
  static const char* const after_text =
      std::getenv("LAVIK_FAIL_TX_WRITE_AFTER");
  if (after_text == nullptr) return true;
  static const std::uint64_t fail_index = [] {
    std::uint64_t parsed = 0;
    const char* begin = after_text;
    const char* end = begin + std::strlen(begin);
    const auto result = std::from_chars(begin, end, parsed);
    return result.ec == std::errc{} && result.ptr == end ? parsed
                                                         : std::uint64_t{0};
  }();
  static std::atomic<std::uint64_t> matches{0};
  return matches.fetch_add(1, std::memory_order_relaxed) == fail_index;
}
#define LAVIK_MAYBE_FAIL_TX_WRITE(key) ::lavik::storage::MaybeFailTxWrite(key)
#else
#define LAVIK_MAYBE_FAIL_TX_WRITE(key) false
#endif

inline std::uint64_t UnixTimeMillis() noexcept {
  const auto value = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
  return value > 0 ? static_cast<std::uint64_t>(value) : 0;
}

inline bool IsExpired(const RecordLocation& location,
                      std::uint64_t now_ms) noexcept {
  return location.kind() == RecordKind::kValue && location.expire_at_ms_ != 0 &&
         location.expire_at_ms_ <= now_ms;
}

inline std::uint64_t ExpireAt(const RecordIndex::Entry& entry) noexcept {
  const std::uint64_t* expiry = entry.optional_extra();
  assert(entry.value_.has_expiry() == (expiry != nullptr));
  return expiry == nullptr ? 0 : *expiry;
}

inline bool IsExpired(const RecordIndex::Entry& entry,
                      std::uint64_t now_ms) noexcept {
  const std::uint64_t expire_at_ms = ExpireAt(entry);
  return entry.value_.kind() == RecordKind::kValue && expire_at_ms != 0 &&
         expire_at_ms <= now_ms;
}

// The compact index already says whether an expiry word exists. Keep the
// clock read behind that bit: non-expiring keys dominate foreground traffic,
// while expiring keys retain the exact same millisecond comparison.
[[gnu::always_inline]] inline bool IsExpiredNow(
    const RecordIndex::Entry& entry) noexcept {
  return entry.value_.has_expiry() && IsExpired(entry, UnixTimeMillis());
}

[[gnu::always_inline]] inline bool IsExpiredNow(
    const RecordLocation& location) noexcept {
  return location.has_expiry() && IsExpired(location, UnixTimeMillis());
}

inline absl::StatusOr<std::shared_ptr<const std::vector<ExtentRef>>>
DecodeManifest(std::span<const std::byte> payload, std::uint64_t logical_size,
               bool validate_logical_bytes = true) {
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
  if ((validate_logical_bytes && total != logical_size) ||
      total > kMaxRecordPayloadBytes) {
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
  std::uint64_t tx_generation_ = 0;
  std::uint32_t extent_index_ = 0;
  std::uint32_t extent_payload_checksum_ = 0;
};

// State that exists only while a block is held in memory behind a staging
// buffer: the buffer itself and the bookkeeping the flusher needs. At most a
// handful of blocks per worker are in that state at once, while every
// allocated block carries a BlockState, so this lives in a side table instead
// of charging staging-only state to every block.
struct StagingSlot {
  std::uint16_t write_buffer_id_ = 0;
  std::byte* heap_data_ = nullptr;
  std::size_t heap_data_size_ = 0;
  // Bytes already written and fdatasynced. Direct-I/O aligned, so appends
  // never land in a durable page and a flush only writes the new tail.
  std::uint32_t durable_bytes_ = kBlockHeaderBytes;
  // Current append boundary, including direct-I/O padding inserted by an
  // in-flight flush. Keeping it beside durable_bytes_ lets INFO aggregate
  // dirty staging bytes without walking every allocated block.
  std::uint32_t committed_bytes_ = kBlockHeaderBytes;
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
// The array is shared: any worker can address any entry. A key-index owner may
// read the immutable allocation epoch after acquiring `owner`; all other
// non-atomic fields are the physical owner's exclusive property, reached only
// after FindBlockState has confirmed ownership.
struct alignas(32) BlockState {
  // Reset initializes this before publishing owner_ with release semantics.
  // Live-byte accounting keeps the allocation live while an index entry
  // references it, so the epoch is immutable until no reader can materialize
  // such an entry and the block can be retired.
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

  // The atomic owner makes this non-assignable. Initialization and clearing
  // publish the new owner last so an acquire load never observes a
  // half-reset identity.
  void Reset(std::uint16_t new_owner,
             std::uint64_t new_allocation_epoch = 0) noexcept {
    allocation_epoch_ = new_allocation_epoch;
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

// Rebuild the self-contained runtime identity from a compact index entry. The
// acquire owner load observes the epoch initialized before publication; live-
// byte accounting prevents either field from changing while the entry is
// current.
inline RecordLocation MaterializePublishedIndexLocation(
    const RecordIndex::Entry& entry, const BlockState& state) noexcept {
  const std::uint16_t owner = state.owner_.load(std::memory_order_acquire);
  const std::uint64_t allocation_epoch = state.allocation_epoch_;
  assert(owner < kMaxMemoryWorkers);
  assert(allocation_epoch != 0);
  assert(RecordLocation::CanEncodeBlockIdentity(entry.value_.block_id(),
                                                allocation_epoch));
  return RecordIndexEntryPolicy::Load(entry.value_, entry.optional_extra(),
                                      allocation_epoch, owner);
}

struct ExtentIdentity {
  std::uint32_t extent_index_ = 0;
  std::uint32_t payload_checksum_ = 0;
};

using RecoveredGroupedRoot =
    std::variant<GroupedHashRoot, OrderedCollectionRoot>;

struct RecoveryRecord {
  Digest digest_{};
  std::string key_;
  std::uint8_t db_id_ = 0;
  // Multi-key transaction tag. Tagged records are parked until every
  // worker's scan has contributed its kTxCommit sightings, then applied only
  // if their transaction committed.
  std::uint64_t txid_ = 0;
  // Physical append order, used only to choose between defrag copies with
  // the same logical mutation sequence during recovery.
  std::uint64_t lsn_ = 0;
  // Recovery filters this durable generation before installing the location.
  // Every live entry then inherits PartitionStore::replication_epoch_.
  std::uint64_t replication_epoch_ = 1;
  RecordLocation location_{};
  ExtentManifest extents_;
  // Auxiliary records share their user key with the root, but must never
  // enter the top-level winner merge. Keep only checked routing metadata;
  // complete group values remain on disk throughout index reconstruction.
  std::optional<RecoveredHashGroup> auxiliary_group_;
  std::optional<RecoveredOrderedGroup> ordered_group_;
  std::optional<RecoveredGroupedRoot> grouped_root_;
  // Set only by complete graph reconstruction, then consumed by the bounded
  // physical-accounting pass. A prepared/orphan auxiliary never owns bytes.
  bool grouped_reachable_ = false;
  // A validated checkpoint entry already names the winner selected at clean
  // shutdown. Successful checkpoint recovery must not retain a second
  // pointer-to-LSN hash table for every key merely to protect that winner
  // from equal-sequence copies; checkpoint_active_ provides that protection.
  // If validation later falls back to a cold scan, InitializeWorker first
  // materializes the equivalent UINT64_MAX markers for the loaded prefix.
  bool checkpoint_snapshot_ = false;
};

// Non-owning counterpart used while a validated checkpoint buffer remains
// pinned. The caller owns both key bytes and the manifest for the duration of
// ApplyRecoveredRecord; the final index and manifest map take the copies they
// need before the I/O buffer can be reused.
struct RecoveryRecordView {
  Digest digest_{};
  std::string_view key_;
  std::uint8_t db_id_ = 0;
  std::uint64_t txid_ = 0;
  std::uint64_t lsn_ = 0;
  std::uint64_t replication_epoch_ = 1;
  RecordLocation location_{};
  const ExtentManifest* extents_ = nullptr;
  const RecoveredGroupedRoot* grouped_root_ = nullptr;
  bool checkpoint_snapshot_ = false;
};

struct RecoveryBlock {
  ActiveBlock block_{};
};

struct RecoveryBatch {
  std::vector<RecoveryRecord> records_;
  std::vector<RecoveryBlock> blocks_;
  // Commit records are not indexed. Charge them to their transaction block;
  // whole-generation retirement removes them after promotion is durable.
  struct CommitRecord {
    std::uint64_t block_id_ = 0;
    std::uint64_t txid_ = 0;
    std::uint32_t bytes_ = 0;
  };
  std::vector<CommitRecord> commit_records_;
};

struct RecoveryLiveReference {
  std::uint64_t block_id_ = 0;
  std::uint64_t allocation_epoch_ = 0;
  std::uint64_t txid_ = 0;
  // Total bytes charged for live references. Shared extents can contribute
  // the same physical payload more than once, so this may exceed the block's
  // committed payload size after checkpoint aggregation.
  std::uint32_t bytes_ = 0;
  // Checkpoint decoding happens before block headers establish runtime
  // ownership. Root records retain the owner serialized in their location;
  // extents leave this unowned and resolve it after the header scan.
  std::uint16_t expected_owner_ = kUnownedBlock;
  bool extent_ = false;
  // A checkpoint accounting entry is an already-aggregated absolute value.
  // Recovery must install it once, rather than add it like a cold-scan key
  // reference; this also turns duplicate table entries into a hard failure.
  bool replace_live_bytes_ = false;
  // The physical payload size used to validate one extent block. Keep it
  // separate from bytes_, which is reference-counted accounting.
  std::uint32_t extent_payload_bytes_ = 0;
  std::uint32_t extent_index_ = 0;
  std::uint32_t extent_payload_checksum_ = 0;
};

// Routing records and live-accounting references are held only until their
// target workers consume a batch. Divide one process-wide target across scan
// workers so recovery concurrency does not multiply temporary memory by the
// configured worker count. A single record may exceed its worker's target.
inline constexpr std::size_t kRecoveryProcessBatchTargetBytes =
    64 * 1024 * 1024;

constexpr std::size_t RecoveryWorkerBatchTargetBytes(
    unsigned worker_count) noexcept {
  return worker_count == 0
             ? kRecoveryProcessBatchTargetBytes
             : std::max<std::size_t>(
                   1, kRecoveryProcessBatchTargetBytes / worker_count);
}

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
  std::uint32_t record_offset_ = 0;
  bool tx_tagged_ = false;
  bool dependency_pinned_ = false;
  ExtentManifest dependent_extents_;
  ExtentManifest immediate_extents_;
  std::shared_ptr<const std::vector<ExtentManifest>> extra_dependent_extents_;
  // New grouped batches carry their retained allocation charge through the
  // undo, transaction receipt and final flush copies without growing the
  // ordinary per-staged-record identity.
  std::shared_ptr<const void> retained_owner_ = nullptr;
};

// Ordinary writes only need one dependent extent owner while waiting for the
// replacement record to flush. Keep this common staging representation apart
// from RetiredRecord: transaction commits and FLUSHDB need its additional
// ownership forms, but charging those three shared_ptr slots to every SET
// increases cache traffic on both the append and flush paths.
struct StagedRetiredRecord {
  ExtentManifest dependent_extents_;
  std::uint64_t block_id_ = 0;
  std::uint64_t allocation_epoch_ = 0;
  std::uint32_t total_disk_bytes_ = 0;
  std::uint16_t block_owner_ = 0;
  std::uint32_t record_offset_ = 0;
  bool tx_tagged_ = false;
  bool present_ = false;

  RetiredRecord Materialize() const {
    assert(present_);
    return RetiredRecord{
        .block_id_ = block_id_,
        .allocation_epoch_ = allocation_epoch_,
        .total_disk_bytes_ = total_disk_bytes_,
        .block_owner_ = block_owner_,
        .record_offset_ = record_offset_,
        .tx_tagged_ = tx_tagged_,
        .dependency_pinned_ = false,
        .dependent_extents_ = dependent_extents_,
        .immediate_extents_ = nullptr,
        .extra_dependent_extents_ = nullptr,
    };
  }
};

static_assert(sizeof(StagedRetiredRecord) == 48);

struct TxGenerationBlock {
  std::uint64_t block_id_ = 0;
  std::uint64_t allocation_epoch_ = 0;
  std::uint64_t generation_ = 0;
  std::uint64_t live_tagged_bytes_ = 0;
};

struct TxGenerationLocalState {
  std::vector<TxGenerationBlock> blocks_;
  std::vector<std::uint64_t> committed_txids_;
  std::uint64_t active_transactions_ = 0;
  std::uint64_t live_tagged_bytes_ = 0;
  std::uint64_t dependency_pins_ = 0;
  bool sealed_and_durable_ = true;
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

  bool Matches(const RecordLocationCore& location) const noexcept {
    return location.block_id() == block_id_ &&
           location.allocation_epoch() == allocation_epoch_ &&
           location.record_offset() == record_offset_;
  }
};

// Address of the index entry associated with a record staged in a block's write
// buffer, so flush completion can flip a still-current version to on-disk reads
// without re-hashing the key. The address may outlive either the entry or its
// owning index: representation changes free entries, while FLUSHDB detaches a
// whole database population. Generation and bucket-membership checks reject
// both cases before any Entry is dereferenced.
struct RecordIdentity {
  // Stored as address bits because a representation-changing overwrite may
  // end the Entry lifetime before this physical record's FLUSH completes.
  // The address is converted back to Entry* only when FindAddress proves a
  // live bucket slot still owns it.
  std::uintptr_t entry_address_ = 0;
  std::shared_ptr<const std::vector<ExtentRef>> retired_extents_;
  // The version this record superseded. Retired only when this record's
  // flush completes: until the replacement is durable, the old copy is the
  // only durable version of the key, and subtracting it from live_bytes any
  // earlier lets the block reach zero and be durably freed — a crash before
  // the flush then loses a value that had already been made durable.
  StagedRetiredRecord retired_record_;
  // A kTxCommit record additionally carries every retirement of its
  // transaction: the superseded versions may only leave their blocks'
  // accounting once the commit itself is durable, since without the commit
  // recovery drops the replacements and must still find the old copies.
  std::unique_ptr<std::vector<RetiredRecord>> tx_retirements_;
  std::uint64_t index_generation_ = 0;
  // The flush uses the cached bucket hash and owner partition to prove the
  // address is still a member of the live index. These fields occupy the
  // former tail padding, so validation does not enlarge per-staged-record
  // memory.
  std::uint32_t entry_hash_ = 0;
  std::uint16_t partition_id_ = 0;
  std::uint8_t db_id_ = 0;
};

static_assert(sizeof(RecordIdentity) == 96);

// One journaled write of an in-flight multi-key transaction, enough to put
// the index back exactly as it was. Multiple writes to one key share a stable
// handle so replacing its concrete Entry object never requires editing every
// earlier undo item.
struct TxUndoEntry {
  std::uint32_t entry_handle_ = 0;
  std::optional<RecordLocation> previous_;
  ExtentManifest previous_extents_;
  GroupedHashObject::Handle previous_grouped_ = nullptr;
  bool previous_dependency_pinned_ = false;
  std::shared_ptr<const std::vector<RetiredRecord>>
      previous_grouped_retirements_ = nullptr;
  // Prepared before root publication so an OOM-triggered abort never needs
  // fresh graph-retirement allocation merely to restore the previous view.
  std::shared_ptr<const std::vector<RetiredRecord>>
      applied_grouped_retirements_ = nullptr;
  std::uint8_t db_id_ = 0;
};

struct TxUndoLog {
  // Pull-based restore admits and reserves this before replacing any record,
  // so joining its one squashed entry back to a suspended prefix cannot OOM.
  void ReserveOneEntry() {
    entries_.reserve(entries_.size() + 1);
    current_entries_.reserve(current_entries_.size() + 1);
    handle_by_address_.reserve(current_entries_.size() + 1);
  }
  // Production callers hold the worker store mutex. Replace retargets the
  // stable slot before the old Entry is destroyed, so current_entries_ never
  // exposes a stale pointer to rollback even when the allocator later reuses
  // that address.
  bool CanTrack(const RecordIndex::Entry* entry) const noexcept {
    if (entry != nullptr &&
        handle_by_address_.contains(reinterpret_cast<std::uintptr_t>(entry))) {
      return true;
    }
    if (entry != nullptr &&
        std::find(current_entries_.begin(), current_entries_.end(), entry) !=
            current_entries_.end())
      return true;
    return current_entries_.size() < std::numeric_limits<std::uint32_t>::max();
  }

  // Null means the 32-bit handle namespace is full. Physical allocation
  // failure is deliberately not represented here and remains process-fatal.
  std::optional<std::uint32_t> Track(RecordIndex::Entry* entry) {
    assert(entry != nullptr);
    const std::uintptr_t address = reinterpret_cast<std::uintptr_t>(entry);
    if (auto found = handle_by_address_.find(address);
        found != handle_by_address_.end()) {
      return found->second;
    }
    // An isolated ingest can retarget a suspended prefix without allocating
    // while aborting. Its reverse-address cache is rebuilt only on this miss;
    // the stable handle is unchanged for every earlier undo record.
    if (auto found =
            std::find(current_entries_.begin(), current_entries_.end(), entry);
        found != current_entries_.end()) {
      const auto handle =
          static_cast<std::uint32_t>(found - current_entries_.begin());
      handle_by_address_.emplace(address, handle);
      return handle;
    }
    if (current_entries_.size() >= std::numeric_limits<std::uint32_t>::max()) {
      return std::nullopt;
    }
    const std::uint32_t handle =
        static_cast<std::uint32_t>(current_entries_.size());
    current_entries_.push_back(entry);
    [[maybe_unused]] const bool inserted =
        handle_by_address_.emplace(address, handle).second;
    assert(inserted);
    return handle;
  }

  RecordIndex::Entry* Current(std::uint32_t handle) const noexcept {
    assert(handle < current_entries_.size());
    return current_entries_[handle];
  }

  void Replace(RecordIndex::Entry* previous, RecordIndex::Entry* current) {
    assert(previous != nullptr);
    assert(current != nullptr);
    const std::uintptr_t previous_address =
        reinterpret_cast<std::uintptr_t>(previous);
    auto found = handle_by_address_.find(previous_address);
    if (found == handle_by_address_.end()) {
      for (auto& entry : current_entries_)
        if (entry == previous) entry = current;
      return;
    }
    const std::uint32_t handle = found->second;
    assert(handle < current_entries_.size());
    current_entries_[handle] = current;
    handle_by_address_.erase(found);
    [[maybe_unused]] const bool inserted =
        handle_by_address_
            .emplace(reinterpret_cast<std::uintptr_t>(current), handle)
            .second;
    assert(inserted);
  }

  // Prefix restoration runs on error paths where no new metadata may be
  // allocated. Drop only the address cache; Track's miss path recovers it.
  void NoAllocReplace(RecordIndex::Entry* previous,
                      RecordIndex::Entry* current) noexcept {
    assert(current != nullptr);
    for (auto& entry : current_entries_)
      if (entry == previous) entry = current;
    handle_by_address_.erase(reinterpret_cast<std::uintptr_t>(previous));
  }

  std::vector<TxUndoEntry> entries_;

 private:
  std::vector<RecordIndex::Entry*> current_entries_;
  absl::flat_hash_map<std::uintptr_t, std::uint32_t> handle_by_address_;
};

struct ReplicaCollectionStage;

struct ReplicaValueStage {
  std::uint8_t db_id_ = 0;
  std::uint64_t db_epoch_ = 0;
  std::uint64_t mutation_sequence_ = 0;
  std::uint64_t expire_at_ms_ = 0;
  // Redis logical bytes/cardinality and portable encoded byte size differ
  // for collection values, so framed replication tracks both.
  std::uint64_t logical_size_ = 0;
  std::uint64_t encoded_size_ = 0;
  std::uint32_t next_chunk_ = 0;
  std::uint32_t chunk_count_ = 0;
  ValueType value_type_ = ValueType::kNone;
  std::string key_;
  std::string value_;
  // Collection frames are decoded and persisted page by page. The separate
  // state owns their transaction/key hold and must be explicitly settled
  // before this stage is destroyed; ordinary String/Stream framing is
  // unchanged.
  std::shared_ptr<ReplicaCollectionStage> collection_;
  // The staged key/value survives multiple received frames, so its
  // conservative reservation becomes retained ownership until commit, abort,
  // or session teardown destroys this object.
  RetainedMemoryCharge memory_charge_;
};

// One partition's worth of entries taken out of service by FLUSHDB or replica
// reset. The entries are unreachable to readers the moment the index is
// detached, but the blocks they occupy still count them as live until the
// reclaimer subtracts them.
struct DetachedIndex {
  RecordIndex index_;
  std::uint8_t db_id_ = 0;
  // Sparse collection metadata follows the same detached population. Retain
  // the complete graph until its records/extents have been subtracted; an
  // ordinary String-only population allocates no grouped side index here.
  std::optional<GroupedObjectIndex> grouped_;
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

inline absl::Status ReadExactlyAt(const std::string& path,
                                  std::span<std::byte> output,
                                  std::uint64_t offset) {
  if (bycorf::IsSpdkStoragePath(path)) {
    return bycorf::ReadSpdkStorage(path, output, offset);
  }
  const int fd = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
  if (fd < 0) {
    return absl::Status(absl::StatusCode::kInternal,
                        "open storage path for read failed: " + path + ": " +
                            std::strerror(errno));
  }
  absl::Status status = ReadExactlyAt(fd, output, offset);
  const int close_error = ::close(fd);
  if (status.ok() && close_error != 0) {
    status = absl::Status(absl::StatusCode::kInternal,
                          "close storage path after read failed: " + path);
  }
  return status;
}

inline absl::Status WriteExactlyAt(const std::string& path,
                                   std::span<const std::byte> input,
                                   std::uint64_t offset, bool flush) {
  if (bycorf::IsSpdkStoragePath(path)) {
    return bycorf::WriteSpdkStorage(path, input, offset, flush);
  }
  const int fd = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
  if (fd < 0) {
    return absl::Status(absl::StatusCode::kInternal,
                        "open storage path for write failed: " + path + ": " +
                            std::strerror(errno));
  }
  absl::Status status = WriteExactlyAt(fd, input, offset);
  if (status.ok() && flush && ::fdatasync(fd) != 0) {
    status = absl::Status(
        absl::StatusCode::kInternal,
        "storage fdatasync failed: " + path + ": " + std::strerror(errno));
  }
  const int close_error = ::close(fd);
  if (status.ok() && close_error != 0) {
    status = absl::Status(absl::StatusCode::kInternal,
                          "close storage path after write failed: " + path);
  }
  return status;
}

inline absl::StatusOr<std::optional<DeviceLabel>> ReadDeviceLabel(
    const std::string& path) {
  std::array<std::byte, kDirectIoAlignment> page{};
  absl::Status status = ReadExactlyAt(path, page, kDeviceLabelOffset);
  if (!status.ok()) {
    return status;
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
  std::array<std::byte, kDirectIoAlignment> page{};
  EncodeDeviceLabel(label, page);
  return WriteExactlyAt(path, page, kDeviceLabelOffset, true);
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
    const std::string& path, std::uint64_t base_offset, MetadataPageKind kind,
    std::uint32_t page_index, std::size_t payload_bytes) {
  LoadedMetadataPage selected;
  selected.payload_.resize(payload_bytes, std::byte{0});
  bool saw_nonzero = false;
  bool selected_valid = false;
  for (unsigned slot = 0; slot < 2; ++slot) {
    std::array<std::byte, kDirectIoAlignment> page{};
    absl::Status read = ReadExactlyAt(
        path, page, MetadataPageSlotOffset(base_offset, page_index, slot));
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
  std::string controller_id_;
  std::uint64_t id_ = 0;
  std::uint64_t capacity_blocks_ = 0;
  unsigned io_queue_count_ = 0;
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

enum class AllocationPurpose : std::uint8_t {
  kForeground,
  kDefrag,
  // Shutdown recovery proof and checkpoint metadata may allocate after writes
  // have been frozen, but it never waits for reclamation or consumes the
  // reserve that guarantees defrag can make progress.
  kShutdownMetadata,
};

struct CheckpointRoot {
  std::uint64_t generation_ = 0;
  std::uint64_t consumed_generation_ = 0;
  std::uint64_t block_count_ = 0;
  std::uint64_t entry_count_ = 0;

  bool operator==(const CheckpointRoot&) const noexcept = default;
};

struct CheckpointShardResult {
  absl::Status status_ = absl::OkStatus();
  std::vector<std::uint64_t> blocks_;
  std::uint64_t entry_count_ = 0;
  std::uint64_t accounting_entry_count_ = 0;
};

struct CheckpointIndexCapacity {
  std::uint64_t entry_count_ = 0;
  std::uint16_t partition_id_ = 0;
  std::uint16_t shard_id_ = 0;
  std::uint8_t db_id_ = 0;
};

struct CheckpointBodyBlock {
  std::uint64_t block_id_ = 0;
  std::uint16_t shard_id_ = 0;
};

struct CheckpointLoadResult {
  absl::Status status_ = absl::OkStatus();
  std::vector<std::uint64_t> blocks_;
  std::uint64_t entry_count_ = 0;
  std::uint64_t accounting_entry_count_ = 0;
  std::vector<bool> saw_shards_;
  std::vector<bool> saw_accounting_shards_;
  std::vector<std::uint32_t> capacity_chunks_by_shard_;
  std::vector<CheckpointIndexCapacity> index_capacities_;
  std::optional<DigestSeed> digest_seed_;
  // Discovery records the durable shard because the physical bitmap stripe
  // reader need not be the index owner. The preparation barrier redistributes
  // these descriptors before any 8 MiB body is read.
  std::vector<CheckpointBodyBlock> discovered_body_blocks_;
  // After redistribution this contains only blocks owned by this worker, so
  // their decode and index installation remain owner-local.
  std::vector<std::uint64_t> body_blocks_;
  // The durable accounting chunks contain one entry per live physical block.
  // Keep the decoded table bounded by block count while the header scan
  // establishes runtime ownership. A failed checkpoint discards these
  // tentative values and cold recovery accounts winners from the index.
  absl::flat_hash_map<std::uint64_t, RecoveryLiveReference> live_by_block_;
};

struct alignas(kCacheLineBytes) RecoveryDeviceCursor {
  std::atomic<std::uint64_t> next_local_{1};
  std::atomic<std::uint64_t> next_allocation_epoch_{1};
};

static_assert(sizeof(RecoveryDeviceCursor) % kCacheLineBytes == 0);

struct DeviceAllocator {
  bycorf::WorkerId owner_ = 0;
  AsyncMutex mutex_;
  std::uint32_t data_block_begin_ = 1;
  std::uint64_t next_pristine_ = 1;
  std::uint64_t next_allocation_epoch_ = 1;
  std::vector<std::uint64_t> ready_blocks_;
  std::vector<std::uint64_t> cold_free_;
  std::vector<std::byte> scan_bitmap_;
  std::vector<MetadataPageState> bitmap_pages_;
  // This is a discovery index for one-shot shutdown checkpoint blocks. It is
  // independent of scan_bitmap_, which remains the allocation authority.
  std::vector<std::byte> checkpoint_bitmap_;
  std::vector<MetadataPageState> checkpoint_bitmap_pages_;
  bool checkpoint_bitmap_valid_ = true;
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
  std::string controller_id_;
  unsigned io_queue_count_ = 0;
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
  if (bycorf::IsSpdkStoragePath(path)) {
    auto device = bycorf::ProbeSpdkStorage(path);
    if (!device.ok()) {
      return device.status();
    }
    return StoragePathInfo{.is_block_device_ = true,
                           .io_alignment_ = device->io_alignment_,
                           .size_bytes_ = device->size_bytes_,
                           .controller_id_ = device->controller_id_,
                           .io_queue_count_ = device->io_queue_count_};
  }
  struct stat file_info{};
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
        .controller_id_ = {},
        .io_queue_count_ = 0,
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
      .controller_id_ = {},
      .io_queue_count_ = 0,
  };
}

inline bycorf::SizeIoAwaitable ReadStorageBuffer(Worker& worker, FixedFile file,
                                                 FixedBuffer buffer,
                                                 bool registered,
                                                 std::uint64_t offset) {
  if (registered) {
    return bycorf::ReadFixed(worker, file, buffer, offset);
  }
  return bycorf::Read(worker, file,
                      std::span<std::byte>(buffer.data_, buffer.size_), offset);
}

inline Task<absl::StatusOr<std::size_t>> WriteStorageBuffer(
    Worker& worker, FixedFile file, std::span<const std::byte> buffer,
    bool registered, FixedBuffer registered_buffer, std::uint64_t offset) {
  if (registered) {
    bycorf::FixedBuffer target = {
        .data_ = const_cast<std::byte*>(buffer.data()),
        .size_ = buffer.size(),
        .index_ = registered_buffer.index_,
    };
    if (target.index_ == 0 || target.data_ == nullptr ||
        target.size_ > registered_buffer.size_) {
      co_return absl::Status(absl::StatusCode::kInternal,
                             "invalid registered write buffer");
    }
    co_return co_await bycorf::WriteFixed(worker, file, target, offset);
  }
  co_return co_await bycorf::Write(worker, file, buffer, offset);
}

#if LAVIK_FAULTS_ENABLED
// Runs in the original write's key-lock lifetime, after store state is
// released. The whole call site is erased in Release, including the extra
// coroutine.
inline Task<absl::Status> PauseCompactWriteForTest(Worker& worker,
                                                   std::string_view key) {
  if (!LAVIK_FAULT_MATCHES("LAVIK_COMPACT_WRITE_PAUSE_KEY", key))
    co_return absl::OkStatus();
  const char* configured = std::getenv("LAVIK_COMPACT_WRITE_PAUSE_MS");
  if (configured == nullptr) co_return absl::OkStatus();
  std::uint64_t milliseconds = 0;
  const char* end = configured + std::strlen(configured);
  const auto parsed = std::from_chars(configured, end, milliseconds);
  if (parsed.ec != std::errc{} || parsed.ptr != end || milliseconds == 0 ||
      milliseconds > 60000)
    co_return absl::OkStatus();
  spdlog::info("compact collection write pause armed key={} milliseconds={}",
               key, milliseconds);
  auto status = co_await bycorf::SleepFor(
      worker, std::chrono::milliseconds(milliseconds));
  if (!status.ok()) co_return status;
  spdlog::info("compact collection write pause complete key={}", key);
  co_return absl::OkStatus();
}

// Select a preparation/extent boundary without delaying unrelated writes.
// Markers let concurrency tests observe the unlocked interval
// deterministically.
inline Task<absl::Status> PauseGroupedWriteForTest(Worker& worker,
                                                   std::string_view key,
                                                   std::string_view phase) {
  if (!LAVIK_FAULT_MATCHES("LAVIK_GROUPED_WRITE_PAUSE_KEY", key))
    co_return absl::OkStatus();
  const char* selected = std::getenv("LAVIK_GROUPED_WRITE_PAUSE_PHASE");
  if (selected == nullptr || phase != selected) co_return absl::OkStatus();
  spdlog::info("grouped write pause armed key={} phase={}", key, phase);
  const auto status =
      co_await bycorf::SleepFor(worker, std::chrono::seconds(3));
  if (!status.ok()) co_return status;
  spdlog::info("grouped write pause complete key={} phase={}", key, phase);
  co_return absl::OkStatus();
}
#endif

class StorageEngine::Impl {
 public:
  struct ExpirationAuthorityGrant {
    explicit ExpirationAuthorityGrant(
        std::chrono::nanoseconds deadline_since_boot)
        : deadline_since_boot_(deadline_since_boot) {}

    std::atomic<bool> active_{true};
    const std::chrono::nanoseconds deadline_since_boot_;
  };

#if LAVIK_FAULTS_ENABLED
  enum class ExpirationTestPoint : std::uint8_t {
    kBeforeDurableAppend,
    kBeforeDiskFullFallback,
  };
  using ExpirationTestHook =
      std::function<std::optional<absl::Status>(ExpirationTestPoint point)>;
#endif

  explicit Impl(StorageEngineOptions options) : options_(std::move(options)) {
    shutdown_checkpoint_enabled_.store(options_.shutdown_checkpoint_,
                                       std::memory_order_relaxed);
    replication_publish_queue_bytes_.store(
        options_.replication_publish_queue_bytes_, std::memory_order_relaxed);
    replication_backlog_backpressure_.store(
        options_.replication_backlog_backpressure_, std::memory_order_relaxed);
    expiration_authority_.store(options_.expiration_authority_,
                                std::memory_order_relaxed);
    if (options_.expiration_authority_) {
      active_expiration_authority_.store(
          std::make_shared<ExpirationAuthorityGrant>(
              std::chrono::nanoseconds::max()),
          std::memory_order_relaxed);
    }
    const TombRaiderMode mode =
        options_.expiration_authority_ && options_.tomb_raider_interval_ms_ != 0
            ? TombRaiderMode::kInterval
            : TombRaiderMode::kOff;
    tomb_raider_config_.mode_.store(mode, std::memory_order_relaxed);
    tomb_raider_config_.last_mode_.store(TombRaiderMode::kInterval,
                                         std::memory_order_relaxed);
    tomb_raider_config_.interval_ms_.store(options_.tomb_raider_interval_ms_,
                                           std::memory_order_relaxed);
    tomb_raider_config_.block_sleep_ms_.store(options_.tomb_raider_sleep_ms_,
                                              std::memory_order_relaxed);
    defrag_config_.max_active_per_device_.store(
        options_.defrag_max_active_per_device_, std::memory_order_relaxed);
    defrag_config_.block_sleep_ms_.store(options_.defrag_sleep_ms_,
                                         std::memory_order_relaxed);
    defrag_config_.record_sleep_us_.store(options_.defrag_record_sleep_us_,
                                          std::memory_order_relaxed);
    defrag_config_.paused_.store(options_.defrag_paused_,
                                 std::memory_order_relaxed);
    tx_cleaner_cooldown_ms_.store(options_.tx_cleaner_cooldown_ms_,
                                  std::memory_order_relaxed);
    const auto cleaner_now =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count();
    tx_cleaner_next_run_ms_.store(
        cleaner_now + options_.tx_cleaner_cooldown_ms_,
        std::memory_order_relaxed);
  }

 private:
  std::size_t direct_io_alignment_ = kDirectIoAlignment;

 public:
  struct FullSyncCollection;
  struct TxGenerationRuntime {
    std::atomic<std::uint64_t> active_transactions_{0};
    absl::flat_hash_set<std::uint64_t> committed_txids_;
    bool has_records_ = false;
  };

  struct WorkerStore {
    struct PendingTxCommit {
      std::uint64_t txid_ = 0;
      std::vector<TxShardWrites> writes_;
    };

    struct ReplicationSparseOffset {
      std::uint64_t lsn_ = 0;
      std::uint32_t fragment_index_ = 0;
      std::uint32_t byte_offset_ = 0;
    };

    struct RetainedByteDeleter {
      RetainedAllocationDomain domain_;

      void operator()(std::byte* pointer) noexcept {
        DeallocateRetainedBytes(domain_, pointer, alignof(std::max_align_t));
      }
    };

    struct ReplicationLogBlock {
      using ByteOwner = std::unique_ptr<std::byte, RetainedByteDeleter>;
      using SparseOffsets =
          std::vector<ReplicationSparseOffset,
                      RetainedAllocator<ReplicationSparseOffset>>;

      ReplicationLogBlock()
          : bytes_(nullptr, RetainedByteDeleter{RetainedAllocationDomain{}}),
            sparse_offsets_(RetainedAllocator<ReplicationSparseOffset>(
                RetainedAllocationDomain{})) {}

      explicit ReplicationLogBlock(RetainedAllocationDomain domain)
          : bytes_(nullptr, RetainedByteDeleter{domain}),
            sparse_offsets_(
                RetainedAllocator<ReplicationSparseOffset>(domain)) {}

      ByteOwner bytes_;
      std::uint64_t first_lsn_ = 0;
      std::uint64_t last_lsn_ = 0;
      std::uint32_t committed_bytes_ = 0;
      std::uint32_t frame_count_ = 0;
      bool sealed_ = false;
      SparseOffsets sparse_offsets_;
    };

    struct ReplicationLogRuntime {
      struct PublishFence {
        AsyncNotification ready_;
        absl::Status status_ =
            absl::UnknownError("replication publisher fence is pending");
        std::uint64_t next_lsn_ = 0;
        bool complete_ = false;
      };

      struct PendingCommand {
        std::uint64_t log_epoch_ = 0;
        std::size_t staging_bytes_ = 0;
        ReplicationCommandAppend append_;
        std::shared_ptr<PublishFence> fence_;
        std::shared_ptr<ReplicationTransaction> transaction_;
      };

      AsyncMutex mutex_;
      ReplicationLogState state_ = ReplicationLogState::kDisabled;
      std::uint64_t log_epoch_ = 0;
      std::uint64_t next_lsn_ = 1;
      std::size_t max_blocks_ = 0;
      std::deque<ReplicationLogBlock> blocks_;
      // The history quota counts published blocks only. This fully prepared
      // successor is admitted and accounted normally, but stays outside
      // blocks_ until rollover consumes it. Reserving both the 8 MiB payload
      // and maximum sparse index here preserves the all-or-nothing retained-
      // memory boundary while keeping both allocations off the append path.
      std::optional<ReplicationLogBlock> standby_block_;
      bool standby_refill_pending_ = false;
      std::uint64_t coverage_revocations_ = 0;
      // Only one publisher owns the log mutex, so this state is worker-local.
      // ACK cursor updates wake the notification without acquiring that mutex.
      bool capacity_backpressured_ = false;
      std::uint64_t capacity_waits_ = 0;
      AsyncNotification retention_advanced_;
      // The fixed staging charge covers both payloads and retained ring
      // capacity. The allocator must therefore neither admit nor account the
      // same backing allocation a second time.
      RingBuffer<PendingCommand, RetainedAllocator<PendingCommand>>
          publish_queue_{RetainedAllocator<PendingCommand>(
              RetainedAllocationDomain{.externally_admitted_ = true,
                                       .externally_accounted_ = true})};
      RetainedMemoryCharge publisher_staging_charge_;
      std::size_t publish_queue_bytes_ = 0;
      std::size_t publisher_admitted_bytes_ = 0;
      // Each outstanding publisher admission owns one future ring slot. This
      // prevents several suspended commands from relying on the same spare
      // capacity before any of them has enqueued its marker.
      std::size_t publisher_admitted_items_ = 0;
      bool publisher_running_ = false;
      AsyncNotification publisher_capacity_ready_;
      absl::flat_hash_map<std::uint64_t, std::uint64_t>
          retained_lsn_by_session_;

      // Backlog chunks are process-local and intentionally disappear on
      // restart together with the replication history id.
    };

    struct FullSyncCapture {
      enum class Phase : std::uint8_t {
        kCapturing,
        kTailing,
      };

      enum class KeyPhase : std::uint8_t {
        kBaselineInflight,
        kTailing,
        // The key returned from an override to coverage without returning its
        // full-key map credit. A later replacement can reuse that credit.
        kTailingWithOverrideCredit,
      };

      enum class DbPhase : std::uint8_t {
        kUnstarted,
        kScanning,
        kTailing,
      };

      std::uint64_t baseline_version_ = 0;
      std::map<std::uint64_t, SnapshotRecord> overrides_;
      // Database-local maps allow allocation-free string_view probes on the
      // write path. A combined "db byte + key" temporary would itself allocate
      // before the full-sync credit decision it is meant to protect.
      std::array<absl::flat_hash_map<std::string, std::uint64_t>,
                 kLogicalDatabaseCount>
          latest_by_key_;
      std::size_t replacement_credit_bytes_ = 0;
      // Logical coverage credit converted into live allocator bytes by this
      // partition. It is restored only after all capture containers are
      // cleared, allowing the session to reuse one largest-partition budget.
      std::size_t memory_credit_bytes_ = 0;
      bool arena_credit_consumed_ = false;
      ScanHashMap<KeyPhase> key_phases_;
      struct PendingSnapshotKey {
        std::string key_;
        ValueType value_type_ = ValueType::kNone;
        std::size_t value_bytes_ = 0;
      };
      std::deque<PendingSnapshotKey> pending_snapshot_keys_;
      std::uint64_t pending_snapshot_cursor_ = 0;
      struct PinnedValue {
        ExtentManifest extents_;
        std::shared_ptr<FullSyncCollection> collection_;
        std::size_t key_bytes_ = 0;
        std::uint64_t value_bytes_ = 0;
      };
      // Native grouped-source map capacity survives individual source ACKs.
      // Return its admission only when the capture releases the table itself.
      RetainedMemoryCharge pinned_values_charge_;
      absl::flat_hash_map<std::uint64_t, PinnedValue> pinned_values_;
      std::uint64_t next_pinned_value_id_ = 1;
      std::array<DbPhase, kLogicalDatabaseCount> db_phases_{};
      Phase phase_ = Phase::kCapturing;
    };

    struct FullSyncSessionState {
      struct PendingCommand {
        std::uint64_t id_ = 0;
        std::size_t staging_bytes_ = 0;
        std::shared_ptr<const ReplicationCommandAppend> command_;
        std::optional<SnapshotRecord> record_;
      };

      std::array<std::uint64_t, kLogicalDatabaseCount> db_epochs_{};
      std::size_t reserved_memory_bytes_ = 0;
      std::size_t available_memory_bytes_ = 0;
      RingBuffer<PendingCommand, RetainedAllocator<PendingCommand>>
          publish_queue_{RetainedAllocator<PendingCommand>(
              RetainedAllocationDomain{.externally_admitted_ = true,
                                       .externally_accounted_ = true})};
      // One fixed owner-local budget covers this session's FIFO, shared
      // command references, and retained ring high-water capacity.
      RetainedMemoryCharge publisher_staging_charge_;
      std::size_t publish_queue_bytes_ = 0;
      std::size_t publisher_admitted_bytes_ = 0;
      std::size_t publisher_admitted_items_ = 0;
      std::uint64_t next_publish_id_ = 1;
      // Exact keyed admissions that observed this (partition,DB) before scan
      // start. BeginPartitionDbReplication waits for them to finish before
      // changing UNSTARTED to SCANNING, so a write can neither be skipped nor
      // enqueue without having reserved queue credit.
      absl::flat_hash_map<std::uint32_t, std::uint32_t> unstarted_admissions_;
      AsyncNotification publisher_capacity_ready_;
      bool db_epoch_invalidated_ = false;
    };

    struct PartitionStore {
      struct RdbSnapshotValue {
        enum class Phase : std::uint8_t {
          kOldValue,
          kAbsent,
          kInflight,
          kDone,
        };

        RecordLocation location_{};
        ExtentManifest extents_;
        GroupedHashObject::Handle grouped_;
        struct BlockPin {
          std::uint64_t block_id_ = 0;
          std::uint64_t allocation_epoch_ = 0;
          bool extent_ = false;
        };
        struct BlockPins {
          RetainedMemoryCharge charge_;
          std::vector<BlockPin> blocks_;
        };
        // Captured while the grouped view is current, before any suspension.
        // Compact child entries omit allocation epochs; re-materializing an
        // old view after GC could accidentally pin a reused block incarnation.
        // The admitted list also makes release allocation-free at maxmemory.
        std::shared_ptr<const BlockPins> block_pins_;
        Phase phase_ = Phase::kAbsent;
        bool pins_held_ = false;
      };

      struct RdbSnapshotCapture {
        std::uint64_t session_id_ = 0;
        std::uint64_t cut_sequence_ = 0;
        std::uint64_t snapshot_time_ms_ = 0;
        ScanHashMap<RdbSnapshotValue> dirty_keys_;
        std::uint32_t capture_admissions_ = 0;
        bool accepting_ = true;
      };

      struct ReplicaSyncState {
        std::array<std::uint64_t, kLogicalDatabaseCount> source_db_epochs_{};
        std::array<std::uint64_t, kLogicalDatabaseCount> local_db_epochs_{};
        std::uint64_t session_id_ = 0;
        std::uint64_t replication_epoch_ = 0;
        std::optional<std::uint64_t> command_sequence_;
        bool tailing_ = false;
        // A malformed/failed collection stream cannot become a promotable
        // candidate even after its in-memory stage has been rolled back.
        bool stream_failed_ = false;
      };

      std::uint16_t id_ = 0;
      std::array<RecordIndex, kLogicalDatabaseCount> indexes_;
      // Sparse second-level metadata: ordinary String and compact collection
      // keys never consult this table. Its population follows the top-level
      // index through detach/reset; handles alone do not pin physical data.
      std::array<GroupedObjectIndex, kLogicalDatabaseCount> grouped_objects_;
      // A grouped view belongs to one partition/DB population. The worker's
      // broader index_generations_ invalidates ordinary suspended IO, but a
      // reset of an unrelated partition must not invalidate retained views
      // in this one. Advance only alongside this population's index detach.
      std::array<std::uint64_t, kLogicalDatabaseCount> grouped_generations_{};
      // Conservative full-sync coverage ownership for each index. The owner
      // worker updates this only when an index identity is created or erased;
      // value and TTL replacements leave it unchanged. Keeping the aggregate
      // beside the fixed index array lets session admission inspect a bounded
      // number of counters instead of synchronously scanning every key.
      std::array<std::uint64_t, kLogicalDatabaseCount>
          fullsync_coverage_bytes_{};
      std::array<std::size_t, kLogicalDatabaseCount> live_key_count_{};
      std::array<std::size_t, kLogicalDatabaseCount> expiring_key_count_{};
      std::uint64_t mutation_sequence_ = 0;
      std::uint64_t replication_epoch_ = 1;
      // Highest epoch reserved for an in-place replica rebuild. It may be
      // ahead after an aborted full sync and is never reused.
      std::uint64_t replica_candidate_epoch_ = 1;
      // Owner-local subscribers keyed by replication session. Writes inspect
      // this map through one [[unlikely]] branch and synchronously coalesce
      // the latest committed record for each (db,key).
      absl::flat_hash_map<std::uint64_t, FullSyncCapture> fullsync_subscribers_;
      std::optional<RdbSnapshotCapture> rdb_snapshot_;
      std::optional<ReplicaValueStage> replica_value_stage_;
      // Small protocol state only. Full sync destructively rebuilds indexes_
      // in place; no second data root is retained in the first version.
      std::unique_ptr<ReplicaSyncState> replica_sync_;
    };

    struct ExpireCandidate {
      std::uint16_t partition_id_ = 0;
      std::uint8_t db_id_ = 0;
      Digest digest_{};
      std::uint64_t mutation_sequence_ = 0;
      std::uint64_t expire_at_ms_ = 0;
      std::string key_;
      // Revoking or replacing the grant invalidates this exact queued work;
      // a later grant cannot authorize a candidate admitted by its predecessor.
      std::shared_ptr<ExpirationAuthorityGrant> expiration_authority_;
    };

    Worker* worker_ = nullptr;
    // Hot-path physical append ordinal. Recovery seeds every worker above the
    // greatest durable LSN; workers then advance disjoint striped sequences
    // by worker_count, so allocation is local and needs no atomic operation.
    std::uint64_t next_lsn_ = 0;
    RegisteredBufferPool buffers_;
    std::vector<FixedFile> files_;
    // Every primary index owned by this worker shares one handle namespace.
    // Declaring the arena before partitions and detached populations makes it
    // outlive every map during reverse-order WorkerStore destruction.
    std::shared_ptr<ScanHashMapEntryArena> record_index_entry_arena_;
    std::vector<PartitionStore> partitions_;
    // Present only while a shutdown checkpoint is being restored. Keeping the
    // exact counts owner-local permits a post-load equality check without
    // cross-thread reads of ScanHashMap state.
    std::vector<std::array<std::uint64_t, kLogicalDatabaseCount>>
        checkpoint_index_capacities_;
    struct RdbCollectionReadState {
      RetainedMemoryCharge charge_;
      PartitionStore* partition_ = nullptr;
      PartitionStore::RdbSnapshotValue* saved_ = nullptr;
      std::string key_;
      HashGroupMap<std::uint64_t>::const_iterator hash_cursor_;
      std::uint64_t token_ = 0;
      std::uint64_t cursor_ = 0;
      std::uint64_t emitted_ = 0;
      std::uint8_t db_id_ = 0;
      bool reading_ = false;
      bool done_ = false;
    };
    struct RdbSnapshotSession {
      std::uint64_t id_ = 0;
      std::uint64_t snapshot_time_ms_ = 0;
      bool invalidated_ = false;
      bool ending_ = false;
      std::uint32_t readers_ = 0;
      std::unique_ptr<RdbCollectionReadState> collection_;
    };
    std::optional<RdbSnapshotSession> rdb_snapshot_;
    // Serializes replica apply/reset work on this worker across sessions. A
    // disconnected session may still be suspended in storage IO; a new full
    // sync must not reset a partition until that stale apply has completed.
    AsyncMutex replica_apply_mutex_;
    // Owner-local full-sync lifetimes. FLUSHDB marks every active lifetime
    // invalid synchronously with the local epoch detach, including sessions
    // whose current partition has not yet installed a capture map.
    absl::flat_hash_map<std::uint64_t, FullSyncSessionState> fullsync_sessions_;
    // Stable worker-lifetime notification used by admission waiters. Session
    // objects themselves may be erased on disconnect, so waiters must never
    // suspend on a notification owned by one session.
    AsyncNotification fullsync_publisher_capacity_ready_;
    std::uint64_t fullsync_publisher_capacity_waits_ = 0;
    // FIFO admission prevents an oversized command from starving while later
    // small commands continuously refill the publisher queues.
    std::uint64_t replication_publisher_next_ticket_ = 1;
    std::uint64_t replication_publisher_serving_ticket_ = 1;
    AsyncNotification replication_publisher_admission_ready_;
    absl::flat_hash_map<std::uint64_t, std::vector<RecordIdentity>>
        staged_records_;
    // External manifests are exceptional and relatively large. Keeping them
    // here, keyed by the index entry (and migrated on a TTL type change),
    // avoids a shared_ptr in every ordinary key while preserving O(1) FLUSHDB
    // detachment.
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
    ReplicationLogRuntime replication_log_;
    std::optional<ActiveBlock> active_block_;
    // The ordinary stream prefetches only an ID; its 8 MiB staging buffer is
    // still acquired at rollover. The pending bit covers the complete task,
    // including returning a stale reservation, so shutdown can wait for the
    // detached coroutine before worker state is destroyed.
    std::optional<ReservedBlock> standby_block_;
    std::optional<std::uint64_t> standby_prefetch_for_block_;
    bool standby_prefetch_pending_ = false;
    // A missing ordinary append stream has exactly one allocator. Followers
    // drop store_state_mutex_ before waiting here, then revalidate the stream
    // after taking both locks. This prevents one rollover from fanning out
    // into many physical allocations without serializing ordinary appends.
    AsyncMutex active_block_allocation_mutex_;
    // Usually current and draining generations only. An old transaction may
    // finish after rotation, so append streams are keyed by generation.
    absl::flat_hash_map<std::uint64_t, std::optional<ActiveBlock>>
        active_tx_blocks_;
    // Transaction generations can allocate independently, but writers within
    // one generation single-flight rollover. The mutex objects are indirect
    // so active_tx_blocks_ rehash cannot invalidate a suspended waiter's gate;
    // retirement erases a gate only after that generation has no live lease.
    absl::flat_hash_map<std::uint64_t, std::unique_ptr<AsyncMutex>>
        active_tx_block_allocation_mutexes_;
    // Owner-local MPSC is unnecessary: commands and the drain coroutine both
    // run on this worker. Keeping receipts here replaces one detached
    // coroutine frame per transaction with one bounded-size batch runner.
    std::deque<PendingTxCommit> tx_commit_queue_;
    bool tx_commit_runner_ = false;
    AsyncNotification tx_commit_capacity_;
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
    // Recovery-only physical LSN of the candidate installed in each index
    // entry. Cleared after all versions have been merged.
    absl::flat_hash_map<const RecordIndex::Entry*, std::uint64_t>
        recovery_lsns_;
    // Recovery-only txid of the final winner in each entry. Physical blocks
    // can map to a different worker after a topology change, so the later
    // live-reference routing charges the transaction block owner.
    absl::flat_hash_map<const RecordIndex::Entry*, std::uint64_t>
        recovery_txids_;
    // txid-tagged records parked by ApplyRecovery until the committed-txid set
    // is complete (after the recovery barrier).
    std::vector<RecoveryRecord> recovery_tx_records_;
    // Root and auxiliary scans can arrive in any physical order. Resolve
    // their graph only after the shared transaction decision barrier and
    // top-level winner selection, before live-byte accounting frees orphans.
    std::vector<RecoveryRecord> recovery_hash_groups_;
    absl::flat_hash_map<const RecordIndex::Entry*, RecoveredGroupedRoot>
        recovery_grouped_roots_;
    // Undo journals of in-flight multi-key writes on this shard, keyed by
    // txid; written and consumed under store_state_mutex.
    absl::flat_hash_map<std::uint64_t, TxUndoLog> tx_undo_;
    // Relocation fences owed per source block. A salvage pass that fails
    // midway has already moved records whose copies are not yet durable; the
    // debt survives the pass here, and CleanBlockLocked settles every owed
    // fence before the block's bitmap bit may be durably cleared. Same-worker
    // access only.
    absl::flat_hash_map<std::uint64_t, std::vector<RelocationDurabilityFence>>
        pending_relocation_fences_;
    struct TxBlockRuntime {
      std::uint64_t allocation_epoch_ = 0;
      std::uint64_t generation_ = 0;
      std::uint64_t live_tagged_bytes_ = 0;
      std::uint32_t dependency_pins_ = 0;
    };
    // Sparse because only transaction blocks need generation/accounting
    // beyond the dense BlockState. Recovery rebuilds it from block headers.
    absl::flat_hash_map<std::uint64_t, TxBlockRuntime> tx_blocks_;
    // Generation metadata is worker-affine just like tx_blocks_. Foreground
    // transaction admission and commit registration touch only the current
    // worker's map; cleaner coordination reads it through owner tasks. The
    // shared runtime lets the last receipt decrement its atomic lease count on
    // any worker without touching the map that owns the entry.
    absl::flat_hash_map<std::uint64_t, std::shared_ptr<TxGenerationRuntime>>
        tx_generations_;
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
    // Independent of TTL candidates: tombstone reaping can leave a shrinking
    // index with no expiring keys and no subsequent foreground requests.
    std::size_t index_maintenance_partition_cursor_ = 0;
    std::uint8_t index_maintenance_db_cursor_ = 0;
    // True for the whole of one expiration cycle, scan through last tombstone.
    // QuiesceExpiration waits on it, which covers every suspension inside the
    // cycle's deletes — including block-allocation waits that release
    // store_state_mutex mid-append.
    bool expiry_cycle_running_ = false;
    std::deque<ExpireCandidate> expired_candidates_;
  };

  absl::Status Prepare(unsigned worker_count);

  Task<absl::Status> InitializeWorker(Worker& worker);
  void FinalizeWorker(unsigned worker_id) noexcept;
  bool AbandonWorkerStateForProcessExit() noexcept;

  unsigned OwnerForKey(std::string_view key) const noexcept {
    return StorageShardForKey(key) % worker_count_;
  }

  Task<absl::StatusOr<DiskValue>> Get(
      std::uint8_t db_id, std::string_view key, ReadLatencyTrace* trace,
      std::optional<std::uint16_t> routed_partition_id);

  // Caller holds this worker's key lock for `digest` (shared) and runs on
  // OwnerForKey(key). `digest` must equal ComputeDigest(key).
  Task<absl::StatusOr<DiskValue>> GetLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      ReadLatencyTrace* trace,
      std::optional<std::uint16_t> routed_partition_id = std::nullopt);

  // Shared implementation for ordinary and transaction-owned reads. Keeping
  // conditional lock acquisition in this coroutine avoids a nested Task frame
  // on every standalone GET while preserving the pre-locked public contract.
  Task<absl::StatusOr<DiskValue>> GetWithLockState(
      std::uint8_t db_id, std::string_view key, Digest digest,
      ReadLatencyTrace* trace, std::optional<std::uint16_t> routed_partition_id,
      bool acquire_key_lock, bool optimistic_read = false);

  Task<std::vector<BatchGetValue>> BatchGetLocked(
      std::uint8_t db_id, std::span<const BatchGetRequest> requests);

  Task<absl::StatusOr<std::uint64_t>> StringLength(std::uint8_t db_id,
                                                   std::string_view key);

  // Caller holds the key lock (shared); see GetLocked.
  Task<absl::StatusOr<std::uint64_t>> StringLengthLocked(std::uint8_t db_id,
                                                         std::string_view key,
                                                         const Digest& digest);

  Task<absl::StatusOr<SetResult>> Set(
      std::uint8_t db_id, std::string_view key, std::string_view value,
      SetOptions options, ReplicationCommandAppend* replication,
      SetLatencyTrace* trace, std::optional<std::uint16_t> routed_partition_id,
      const MutationPrecondition* mutation_precondition);

  // Caller holds the key lock (exclusive); takes store_state_mutex internally.
  Task<absl::StatusOr<SetResult>> SetLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      std::string_view value, SetOptions options, TxShardWrites* tx = nullptr,
      ReplicationCommandAppend* replication = nullptr,
      SetLatencyTrace* trace = nullptr,
      std::optional<std::uint16_t> routed_partition_id = std::nullopt,
      const MutationPrecondition* mutation_precondition = nullptr);

  Task<absl::StatusOr<std::uint64_t>> ListPush(
      std::uint8_t db_id, std::string_view key,
      std::span<const std::string_view> values,
      ReplicationCommandAppend* replication,
      const MutationPrecondition* mutation_precondition);

  // Caller holds the key lock (exclusive); takes store_state_mutex internally.
  Task<absl::StatusOr<std::uint64_t>> ListPushLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      std::span<const std::string_view> values, TxShardWrites* tx = nullptr,
      ReplicationCommandAppend* replication = nullptr,
      const MutationPrecondition* mutation_precondition = nullptr);

  Task<absl::StatusOr<ListResult>> ExecuteList(
      std::uint8_t db_id, std::string_view key, const ListOperation& operation,
      ReplicationCommandAppend* replication,
      const MutationPrecondition* mutation_precondition);

  Task<absl::StatusOr<ListResult>> ExecuteListLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      const ListOperation& operation, TxShardWrites* tx = nullptr,
      ReplicationCommandAppend* replication = nullptr,
      const MutationPrecondition* mutation_precondition = nullptr);

  Task<absl::StatusOr<HashResult>> ExecuteHash(
      std::uint8_t db_id, std::string_view key, const HashOperation& operation,
      ReplicationCommandAppend* replication,
      const MutationPrecondition* mutation_precondition);
  Task<absl::StatusOr<SortedSetResult>> ExecuteSortedSet(
      std::uint8_t db_id, std::string_view key,
      const SortedSetOperation& operation,
      ReplicationCommandAppend* replication,
      const MutationPrecondition* mutation_precondition);
  Task<absl::StatusOr<SortedSetResult>> ExecuteSortedSetLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      const SortedSetOperation& operation, TxShardWrites* tx = nullptr,
      ReplicationCommandAppend* replication = nullptr,
      const MutationPrecondition* mutation_precondition = nullptr);

  Task<absl::StatusOr<HashResult>> ExecuteHashLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      const HashOperation& operation, TxShardWrites* tx = nullptr,
      ReplicationCommandAppend* replication = nullptr,
      const MutationPrecondition* mutation_precondition = nullptr);

  Task<absl::StatusOr<HashResult>> ExecuteSet(
      std::uint8_t db_id, std::string_view key, const HashOperation& operation,
      ReplicationCommandAppend* replication,
      const MutationPrecondition* mutation_precondition);

  Task<absl::StatusOr<HashResult>> ExecuteSetLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      const HashOperation& operation, TxShardWrites* tx = nullptr,
      ReplicationCommandAppend* replication = nullptr,
      const MutationPrecondition* mutation_precondition = nullptr);

  Task<absl::Status> ExecuteCompact(
      std::uint8_t db_id, std::string_view key, ValueType value_type,
      bool read_only, const CompactValueCallback& callback,
      std::uint64_t now_ms, ReplicationCommandAppend* replication,
      const MutationPrecondition* mutation_precondition);
  Task<absl::Status> ExecuteCompactLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      ValueType value_type, bool read_only,
      const CompactValueCallback& callback, TxShardWrites* tx = nullptr,
      std::uint64_t now_ms = 0, ReplicationCommandAppend* replication = nullptr,
      bool prepare_unlocked = false,
      const MutationPrecondition* mutation_precondition = nullptr);

  Task<absl::StatusOr<HashResult>> ExecuteHashLikeLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      const HashOperation& operation, ValueType value_type,
      TxShardWrites* tx = nullptr,
      ReplicationCommandAppend* replication = nullptr,
      const MutationPrecondition* mutation_precondition = nullptr);

  // Pure private-page planning; no store lock is needed. The after-image owns
  // selected groups for point operations or all groups for full-image callers.
  // Only changed_groups are rewritten. Null previous builds a fresh graph.
  absl::StatusOr<HashGroupMutationPlan> PrepareGroupedHashMutation(
      const GroupedHashObject::Handle& previous, HashValue after_image,
      std::span<const HashGroupId> changed_groups, std::uint64_t field_count,
      std::uint64_t revision);
  // Caller retains exclusive key intent and store state. A prepared plan must
  // name the validated predecessor or a validated creation; commit assigns
  // its durable revision and stamps every page of a fresh incarnation.
  Task<absl::Status> CommitGroupedHashMutationLocked(
      WorkerStore& store, WorkerStore::PartitionStore& partition,
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      GroupedHashObject::Handle previous, HashValue after_image,
      std::vector<HashGroupId> changed_groups, std::uint64_t field_count,
      ValueType value_type, std::uint64_t expire_at_ms, TxShardWrites* tx,
      ReplicationCommandAppend* replication,
      const MutationPrecondition* mutation_precondition = nullptr,
      HashGroupMutationPlan* prepared = nullptr);

  // The plan contains complete changed ordered pages, not an append-only
  // mutation log. Null previous creates/promotes a collection; the adapter
  // assigns its fresh incarnation/revision before staging any auxiliary.
  struct SortedSetMemberMutation;
  Task<absl::Status> CommitGroupedOrderedMutationLocked(
      WorkerStore& store, WorkerStore::PartitionStore& partition,
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      GroupedHashObject::Handle previous, OrderedCollectionMutationPlan plan,
      std::uint64_t expire_at_ms, TxShardWrites* tx,
      ReplicationCommandAppend* replication = nullptr,
      const MutationPrecondition* mutation_precondition = nullptr,
      SortedSetMemberMutation* prepared_members = nullptr);
  struct SortedSetMemberMutation {
    MemoryReservation scratch_;
    MemoryReservation leaves_;
    HashGroupMutationPlan plan_;
  };
  // Derives member-index changes from complete ordered before/after pages so
  // every typed, callback and ingest writer shares the same atomic boundary.
  // Only touched prefix leaves are decoded; all retained scratch is admitted.
  // Unlocked callers yield between pages, keeping hot-buffer scans cooperative.
  Task<absl::StatusOr<SortedSetMemberMutation>> PrepareSortedSetMembers(
      WorkerStore& store, WorkerStore::PartitionStore& partition,
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      GroupedHashObject::Handle previous,
      const OrderedCollectionMutationPlan& ordered, bool unlocked = false);
  Task<absl::Status> UpdateGroupedExpirationLocked(
      WorkerStore& store, WorkerStore::PartitionStore& partition,
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      GroupedHashObject::Handle previous, std::uint64_t expire_at_ms,
      TxShardWrites* tx, ReplicationCommandAppend* replication,
      const MutationPrecondition* mutation_precondition = nullptr);
  // Publishes a new root revision over an already retained predecessor graph;
  // the caller separately cancels retirements for reused physical groups.
  Task<absl::Status> RestoreGroupedViewLocked(
      WorkerStore& store, WorkerStore::PartitionStore& partition,
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      GroupedHashObject::Handle previous, TxShardWrites* compensation,
      TxUndoLog* replacement_undo);
  // Page ownership and admission travel together from preparation through
  // publication. Declare charges first so pages die before credit is returned.
  struct PreparedOrderedMutation {
    MemoryReservation pages_;
    MemoryReservation inputs_;
    OrderedCollectionMutationPlan plan_;
  };
  // With an output, only prepare private pages; the caller owns store-lock
  // release/reacquisition, validation and commit (including successful no-ops).
  Task<absl::StatusOr<ListResult>> ExecuteGroupedListLocked(
      WorkerStore& store, WorkerStore::PartitionStore& partition,
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      const ListOperation& operation, GroupedHashObject::Handle previous,
      TxShardWrites* tx, ReplicationCommandAppend* replication,
      const MutationPrecondition* mutation_precondition = nullptr,
      PreparedOrderedMutation* prepared = nullptr);
  Task<absl::StatusOr<SortedSetResult>> ExecuteGroupedSortedSetLocked(
      WorkerStore& store, WorkerStore::PartitionStore& partition,
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      const SortedSetOperation& operation, GroupedHashObject::Handle previous,
      TxShardWrites* tx, ReplicationCommandAppend* replication,
      const MutationPrecondition* mutation_precondition = nullptr,
      PreparedOrderedMutation* prepared = nullptr);

  Task<ExpirationInfo> GetExpiration(std::uint8_t db_id, std::string_view key);

  // Caller holds the key lock (shared); see GetLocked.
  Task<absl::StatusOr<ExpirationInfo>> ReadKeyMetadata(std::uint8_t db_id,
                                                       std::string_view key);
  Task<absl::StatusOr<ExpirationInfo>> ReadKeyMetadataLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest);
  Task<absl::StatusOr<HashResult>> ExecuteGroupedHashRandomLocked(
      WorkerStore& store, WorkerStore::PartitionStore& partition,
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      const HashOperation& operation, GroupedHashObject::Handle object,
      ValueType value_type, TxShardWrites* tx,
      ReplicationCommandAppend* replication,
      const MutationPrecondition* mutation_precondition = nullptr);
  Task<ExpirationInfo> GetExpirationLocked(std::uint8_t db_id,
                                           std::string_view key,
                                           const Digest& digest);

  Task<absl::StatusOr<RawValue>> ReadRawValueLocked(std::uint8_t db_id,
                                                    std::string_view key,
                                                    const Digest& digest);
  Task<absl::StatusOr<RawValue>> ReadRawValue(std::uint8_t db_id,
                                              std::string_view key);
  Task<absl::StatusOr<TransferValue>> ReadValueForTransferLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest);
  Task<absl::Status> WriteValueForTransferLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      const TransferValue& value, TxShardWrites* tx = nullptr,
      ReplicationCommandAppend* replication = nullptr,
      const MutationPrecondition* mutation_precondition = nullptr);
  Task<absl::StatusOr<RestoreRawResult>> RestoreRawValue(
      std::uint8_t db_id, std::string_view key, const RawValue& value,
      bool replace, ReplicationCommandAppend* replication,
      const MutationPrecondition* mutation_precondition);
  Task<absl::StatusOr<RestoreRawResult>> RestoreCollectionValue(
      std::uint8_t db_id, std::string_view key, ValueType type,
      std::uint64_t expire_at_ms, bool replace,
      std::optional<std::uint64_t> expected_items, CollectionPageReader reader,
      ReplicationCommandAppend* replication,
      const MutationPrecondition* mutation_precondition = nullptr);
  Task<absl::StatusOr<RestoreRawResult>> RestoreCollectionValueLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      ValueType type, std::uint64_t expire_at_ms, bool replace,
      std::optional<std::uint64_t> expected_items, CollectionPageReader reader,
      TxShardWrites* tx, ReplicationCommandAppend* replication,
      const MutationPrecondition* mutation_precondition);
  Task<absl::StatusOr<RestoreRawResult>> RestoreRawValueLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      const RawValue& value, bool replace, TxShardWrites* tx,
      ReplicationCommandAppend* replication,
      const MutationPrecondition* mutation_precondition = nullptr);

  Task<absl::Status> WriteRawValueLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      const RawValue& value, TxShardWrites* tx = nullptr,
      ReplicationCommandAppend* replication = nullptr,
      const MutationPrecondition* mutation_precondition = nullptr);

  Task<absl::StatusOr<bool>> UpdateExpiration(
      std::uint8_t db_id, std::string_view key, std::uint64_t expire_at_ms,
      ExpirationCondition condition, ReplicationCommandAppend* replication,
      const MutationPrecondition* mutation_precondition);

  // Caller holds the key lock (exclusive); takes store_state_mutex internally.
  Task<absl::StatusOr<bool>> UpdateExpirationLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      std::uint64_t expire_at_ms, ExpirationCondition condition,
      TxShardWrites* tx = nullptr,
      ReplicationCommandAppend* replication = nullptr,
      const MutationPrecondition* mutation_precondition = nullptr);

  Task<absl::StatusOr<bool>> Delete(
      std::uint8_t db_id, std::string_view key,
      ReplicationCommandAppend* replication,
      const MutationPrecondition* mutation_precondition);

  // Caller holds the key lock (exclusive); takes store_state_mutex internally.
  Task<absl::StatusOr<bool>> DeleteLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      TxShardWrites* tx = nullptr,
      ReplicationCommandAppend* replication = nullptr,
      const MutationPrecondition* mutation_precondition = nullptr);

  // Freezes the keyspace against expiration writes for stable-count scans
  // (KEYS): client writes are already excluded by the closed database gate;
  // this stops the active-expiry loop and drains any in-flight append by
  // bouncing off every worker's store-state mutex. Pauses nest — the database
  // gates are per-db, so KEYS on two databases can overlap — and every
  // successful QuiesceExpiration must be paired with exactly one
  // ResumeExpiration.
  Task<absl::Status> QuiesceExpiration();

  TombRaiderTotals TombRaiderStats() const noexcept {
    TombRaiderTotals totals;
    std::uint64_t before = 0;
    std::uint64_t after = 0;
    do {
      before = tomb_raider_config_.generation_.load(std::memory_order_acquire);
      if ((before & 1) != 0) {
        continue;
      }
      totals.interval_ms_ =
          tomb_raider_config_.interval_ms_.load(std::memory_order_relaxed);
      totals.block_sleep_ms_ =
          tomb_raider_config_.block_sleep_ms_.load(std::memory_order_relaxed);
      totals.daily_second_ =
          tomb_raider_config_.daily_second_.load(std::memory_order_relaxed);
      totals.mode_ = tomb_raider_config_.mode_.load(std::memory_order_relaxed);
      std::atomic_thread_fence(std::memory_order_acquire);
      after = tomb_raider_config_.generation_.load(std::memory_order_acquire);
    } while (before != after || (after & 1) != 0);
    totals.rounds_ = tomb_raider_rounds_.load(std::memory_order_relaxed);
    totals.reaped_ = tomb_raider_reaped_.load(std::memory_order_relaxed);
    totals.refreshed_ = tomb_raider_refreshed_.load(std::memory_order_relaxed);
    totals.enabled_ = totals.mode_ != TombRaiderMode::kOff;
    totals.running_ = tomb_raider_running_.load(std::memory_order_relaxed);
    return totals;
  }

  Task<absl::Status> ConfigureTombRaider(TombRaiderConfigUpdate update);

  Task<absl::Status> QuiesceTombRaiderForReplica();

  DefragTotals DefragStats() const noexcept {
    return DefragTotals{
        .paused_ = defrag_config_.paused_.load(std::memory_order_acquire),
        .max_active_per_device_ = defrag_config_.max_active_per_device_.load(
            std::memory_order_acquire),
        .block_sleep_ms_ =
            defrag_config_.block_sleep_ms_.load(std::memory_order_acquire),
        .record_sleep_us_ =
            defrag_config_.record_sleep_us_.load(std::memory_order_acquire),
        .active_ = active_defrags_.load(std::memory_order_acquire),
        .pending_ = pending_defrags_.load(std::memory_order_acquire),
    };
  }

  Task<absl::Status> ConfigureDefrag(DefragConfigUpdate update);

  std::uint32_t ActiveExpirationConfigValue(
      ActiveExpirationConfigKey key) const noexcept;
  absl::Status ConfigureActiveExpiration(ActiveExpirationConfigKey key,
                                         std::uint64_t value);

  bool ShutdownCheckpointEnabled() const noexcept {
    return shutdown_checkpoint_enabled_.load(std::memory_order_acquire);
  }
  void ConfigureShutdownCheckpoint(bool enabled) noexcept {
    shutdown_checkpoint_enabled_.store(enabled, std::memory_order_release);
  }

  TxCleanerTotals TxCleanerStats() const noexcept {
    return TxCleanerTotals{
        .rounds_ = tx_cleaner_rounds_.load(std::memory_order_acquire),
        .failures_ = tx_cleaner_failures_.load(std::memory_order_acquire),
        .retired_generations_ =
            tx_cleaner_retired_generations_.load(std::memory_order_acquire),
        .retired_blocks_ =
            tx_cleaner_retired_blocks_.load(std::memory_order_acquire),
        .cooldown_ms_ = tx_cleaner_cooldown_ms_.load(std::memory_order_acquire),
        .running_ = tx_cleaner_running_.load(std::memory_order_acquire),
    };
  }
  std::uint32_t TxCleanerCooldownMs() const noexcept {
    return tx_cleaner_cooldown_ms_.load(std::memory_order_acquire);
  }
  absl::Status ConfigureTxCleanerCooldown(std::uint64_t cooldown_ms);
  void InitializeTxWrites(std::uint64_t txid, std::span<TxShardWrites> writes,
                          MutationPrecondition mutation_precondition);
  void RegisterRecoveredTxGeneration(WorkerStore& store,
                                     std::uint64_t generation);

  Task<StorageDurabilityStats> DurabilityStats() const;

  Task<StorageMetricsSnapshot> CollectMetrics() const;

  void ResumeExpiration() noexcept {
    expiration_pause_count_.fetch_sub(1, std::memory_order_acq_rel);
  }

  void SetExpirationAuthority(bool authority) noexcept;

  absl::Status SetExpirationAuthorityUntil(
      std::chrono::nanoseconds deadline_since_boot) noexcept;

  std::shared_ptr<ExpirationAuthorityGrant> CurrentExpirationAuthority()
      const noexcept;

  static bool ExpirationAuthorityIsValid(
      const ExpirationAuthorityGrant* authority) noexcept;

  static absl::Status ValidateExpirationAuthority(const void* context);

  static bool IsExpirationAuthorityCancellation(
      const absl::Status& status) noexcept;

  std::uint32_t ExpirationPauseCount() const noexcept {
    return expiration_pause_count_.load(std::memory_order_acquire);
  }

  Task<bool> KeyLive(std::uint8_t db_id, std::string_view key,
                     const Digest& digest);

  Task<bool> Exists(std::uint8_t db_id, std::string_view key);

  // Caller holds the key lock (shared); see GetLocked.
  Task<bool> ExistsLocked(std::uint8_t db_id, std::string_view key,
                          const Digest& digest);

  Task<absl::Status> CommitTxWrites(std::uint64_t txid,
                                    std::vector<TxShardWrites*> shards);

  static constexpr std::size_t kTxCommitQueueHighWatermark = 4096;

  [[nodiscard]] bool EnqueueTxCommit(std::uint64_t txid,
                                     std::vector<TxShardWrites> writes);
  Task<absl::Status> WaitForTxCommitCapacity();
  Task<absl::Status> DrainTxCommitQueue(WorkerStore* store);

  void PublishCommittedFullSyncEffects(TxShardWrites* shard);

  void NoteTxCommitStarted() noexcept {
    active_tx_commits_.fetch_add(1, std::memory_order_acq_rel);
  }
  void NoteTxCommitFinished() noexcept {
    active_tx_commits_.fetch_sub(1, std::memory_order_acq_rel);
  }

  TxCommitBatchTotals TxCommitBatchStats() const noexcept {
    return TxCommitBatchTotals{
        .batches_ = tx_commit_batches_.load(std::memory_order_acquire),
        .transactions_ =
            tx_commit_batch_transactions_.load(std::memory_order_acquire),
        .input_fences_ =
            tx_commit_input_fences_.load(std::memory_order_acquire),
        .merged_fences_ =
            tx_commit_merged_fences_.load(std::memory_order_acquire),
        .queue_depth_ = tx_commit_queue_depth_.load(std::memory_order_acquire),
        .queue_peak_ = tx_commit_queue_peak_.load(std::memory_order_acquire),
        .backpressure_waits_ =
            tx_commit_backpressure_waits_.load(std::memory_order_acquire),
        .queue_high_watermark_ = kTxCommitQueueHighWatermark,
    };
  }

  Task<absl::Status> RollbackTxLocal(std::uint64_t txid,
                                     TxShardWrites* compensation = nullptr,
                                     bool discard_uncommitted_absent = false,
                                     TxUndoLog* retained_prefix = nullptr,
                                     bool grouped_root_only = false);

  Task<absl::Status> DiscardTxUndoLocal(std::uint64_t txid);

  unsigned worker_count() const noexcept { return worker_count_; }

  std::size_t LocalSize(std::uint8_t db_id) const noexcept {
    assert(db_id < kLogicalDatabaseCount);
    return CurrentStore().live_key_count_[db_id];
  }

  Task<absl::StatusOr<std::optional<std::string>>> RandomKeyLocal(
      std::uint8_t db_id);

  // These helpers never suspend. Call with store state held, before borrowing
  // any index entry across a wait. Eligibility does not infer single-key
  // intent: the command adapter must opt in only where its lifetime contract is
  // known.
  bool CanPrepareCompactWriteUnlocked(
      const WorkerStore& store, const WorkerStore::PartitionStore& partition,
      const RecordIndex::Entry* entry, const RecordLocation& location,
      const TxShardWrites* tx) const noexcept {
    (void)store;
    return tx == nullptr && entry != nullptr && entry->key_complete() &&
           location.kind() == RecordKind::kValue && !location.grouped() &&
           !location.external() && !location.key_external() &&
           location.total_disk_bytes() < kGroupedHashPromotionBytes &&
           !partition.replica_sync_ &&
           !replica_loading_.load(std::memory_order_acquire);
  }

  // The command adapter must retain exclusive key intent and database
  // admission even when no live record exists. Multi-key/transaction and
  // candidate writers keep their separate lifetime contracts.
  bool CanPrepareCollectionCreateUnlocked(
      const WorkerStore::PartitionStore& partition,
      const TxShardWrites* tx) const noexcept {
    return tx == nullptr && !partition.replica_sync_ &&
           !replica_loading_.load(std::memory_order_acquire);
  }

  // Validate logical absence at the original command time, not the current
  // clock. An expired value or tombstone may be relocated/removed by GC, so
  // physical absence is not the invariant. The caller re-resolves the key
  // under store state, including full-key verification for external keys,
  // and passes only that freshly verified entry (or nullptr).
  absl::Status ValidateCollectionCreateSnapshot(
      const WorkerStore& store, const WorkerStore::PartitionStore& partition,
      std::uint8_t db_id, const RecordIndex::Entry* current,
      std::uint64_t now_ms, const CompactWriteSnapshot& snapshot,
      const MutationPrecondition* precondition) const {
    if (!CanPrepareCollectionCreateUnlocked(partition, nullptr) ||
        store.index_generations_[db_id] != snapshot.index_generation_ ||
        EffectiveRecordDbEpoch(partition, db_id) != snapshot.db_epoch_ ||
        partition.replication_epoch_ != snapshot.replication_epoch_)
      return absl::AbortedError(
          "collection population changed during creation");
    if (store.write_failed_ ||
        epoch_metadata_failed_.load(std::memory_order_acquire))
      return absl::FailedPreconditionError(
          "storage writer stopped during creation");
    if (current != nullptr && current->value_.kind() == RecordKind::kValue &&
        !IsExpired(*current, now_ms))
      return absl::AbortedError("collection key appeared during creation");
    return precondition != nullptr ? precondition->Validate()
                                   : absl::OkStatus();
  }

  CompactWriteSnapshot CaptureCompactWriteSnapshot(
      const WorkerStore& store, const WorkerStore::PartitionStore& partition,
      std::uint8_t db_id) const noexcept {
    return {.index_generation_ = store.index_generations_[db_id],
            .db_epoch_ = EffectiveRecordDbEpoch(partition, db_id),
            .replication_epoch_ = partition.replication_epoch_};
  }

  // The caller retains key intent and database admission. Candidate ingestion
  // has its own population lifetime and stays on its existing writer path.
  bool CanPrepareGroupedWriteUnlocked(
      const WorkerStore::PartitionStore& partition) const noexcept {
    return !partition.replica_sync_ &&
           !replica_loading_.load(std::memory_order_acquire);
  }

  // Call after reacquiring store state, including for no-ops and last-element
  // deletion. Retained directories own metadata, not physical allocations:
  // permit GC relocation but never publish a plan into another logical view.
  absl::Status ValidateGroupedWriteSnapshot(
      const WorkerStore& store, const WorkerStore::PartitionStore& partition,
      std::uint8_t db_id, std::string_view key,
      const GroupedHashObject::Handle& previous,
      const CompactWriteSnapshot& snapshot,
      const MutationPrecondition* precondition) const {
    if (!CanPrepareGroupedWriteUnlocked(partition) ||
        store.index_generations_[db_id] != snapshot.index_generation_ ||
        EffectiveRecordDbEpoch(partition, db_id) != snapshot.db_epoch_ ||
        partition.replication_epoch_ != snapshot.replication_epoch_)
      return absl::AbortedError(
          "grouped population changed during preparation");
    if (store.write_failed_ ||
        epoch_metadata_failed_.load(std::memory_order_acquire))
      return absl::FailedPreconditionError(
          "storage writer stopped during preparation");
    const auto current =
        partition.grouped_objects_[db_id].CurrentForMutation(key);
    if (!previous || !current)
      return absl::AbortedError(
          "grouped source disappeared during preparation");
    const auto& a = previous->version();
    const auto& b = current->version();
    if (a.db_epoch_ != b.db_epoch_ ||
        a.replication_epoch_ != b.replication_epoch_ ||
        a.index_generation_ != b.index_generation_ ||
        partition.grouped_generations_[db_id] != a.index_generation_ ||
        a.root_.mutation_sequence_ != b.root_.mutation_sequence_ ||
        a.root_.logical_size_ != b.root_.logical_size_ ||
        a.root_.expire_at_ms_ != b.root_.expire_at_ms_ ||
        a.root_.value_type() != b.root_.value_type() ||
        !previous->SameLogicalRoot(*current))
      return absl::AbortedError(
          "grouped logical version changed during preparation");
    auto status = current->ReadStatus();
    if (status.ok() && precondition != nullptr)
      status = precondition->Validate();
    return status;
  }

  // Reacquire store state before validating, even for successful no-ops. This
  // is not a general-purpose DB-epoch CAS: command database admission must also
  // cover AppendLocked's later allocation waits. No callback is retried here,
  // since its reply/canonical replication effect may already have been built.
  absl::Status ValidateCompactWriteSnapshot(
      const WorkerStore& store, const WorkerStore::PartitionStore& partition,
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      const RecordLocation& location,
      const CompactWriteSnapshot& snapshot) const {
    if (store.index_generations_[db_id] != snapshot.index_generation_ ||
        EffectiveRecordDbEpoch(partition, db_id) != snapshot.db_epoch_ ||
        partition.replication_epoch_ != snapshot.replication_epoch_ ||
        partition.replica_sync_ ||
        replica_loading_.load(std::memory_order_acquire))
      return absl::AbortedError(
          "compact collection population changed while preparing update");
    if (store.write_failed_ ||
        epoch_metadata_failed_.load(std::memory_order_acquire))
      return absl::FailedPreconditionError(
          "storage writer is stopped after an IO failure");
    const auto* current = partition.indexes_[db_id].Find(digest, key);
    if (current == nullptr || !current->key_complete() ||
        current->value_.kind() != RecordKind::kValue ||
        current->value_.value_type() != location.value_type() ||
        current->value_.grouped() ||
        current->value_.mutation_sequence_ != location.mutation_sequence_ ||
        current->value_.logical_size() != location.logical_size_ ||
        ExpireAt(*current) != location.expire_at_ms_)
      return absl::AbortedError(
          "compact collection logical version changed while preparing update");
    // Ignore GC/cleaner changes to block coordinates, staging flags and tx
    // tags. AppendLocked resolves its current physical predecessor after
    // allocation waits. Preserve command-time expiry rather than checking the
    // clock again.
    return absl::OkStatus();
  }

  std::uint64_t DbEpoch(std::uint8_t db_id) const noexcept {
    assert(db_id < kLogicalDatabaseCount);
    return db_epochs_[db_id].load(std::memory_order_acquire);
  }

  // A native replica builds its candidate partition under locally mapped
  // database epochs before those epochs become the globally served ones.
  // Physical records and the grouped side view must name that same population.
  std::uint64_t EffectiveRecordDbEpoch(
      const WorkerStore::PartitionStore& partition,
      std::uint8_t db_id) const noexcept {
    assert(db_id < kLogicalDatabaseCount);
    if (replica_loading_.load(std::memory_order_acquire) &&
        partition.replica_sync_ != nullptr) {
      return partition.replica_sync_->local_db_epochs_[db_id];
    }
    return DbEpoch(db_id);
  }

  Task<absl::Status> FlushDbDetach(std::uint8_t db_id);
  Task<absl::Status> FlushAllDetach();

  Task<absl::Status> FlushDbReclaim(bool wait) {
    return ReclaimDetachedAllWorkers(wait);
  }

  Task<absl::Status> PublishFlushDbReplication(std::uint8_t db_id,
                                               std::uint64_t db_epoch);
  Task<absl::Status> PublishFlushAllReplication(
      const std::array<std::uint64_t, kLogicalDatabaseCount>& db_epochs);

  Task<absl::Status> ApplyReplicatedFlushDb(std::uint8_t db_id,
                                            std::uint64_t source_db_epoch);
  Task<absl::Status> ApplyReplicatedFlushAll(
      const std::array<std::uint64_t, kLogicalDatabaseCount>& source_epochs);

  // Persists the new epoch, then takes the database out of service on every
  // worker. Callers hold the database gate across this and can drop it as soon
  // as it returns: the keyspace is empty and durably so, and what remains is
  // reclamation that no reader can observe. Bounded by the partition count.
  //
  // The epoch has to reach the device before any index is detached. Crashing in
  // the other order leaves records on disk whose epoch still matches, and
  // recovery would resurrect the whole flushed database.
  Task<absl::Status> DetachDbEpoch(std::uint8_t db_id, std::uint64_t next);
  Task<absl::Status> DetachDbEpochs(
      const std::array<std::uint64_t, kLogicalDatabaseCount>& next);

  // Retires what DetachDbEpoch took out of service. Runs with the gate open and
  // ordinary traffic flowing. `wait` is the difference between FLUSHDB SYNC and
  // FLUSHDB ASYNC: either way a reclaimer runs, only the reply waits or not.
  Task<absl::Status> ReclaimDetachedAllWorkers(bool wait);

  Task<absl::Status> AdvanceDbEpoch(std::uint8_t db_id, std::uint64_t next);

  struct ScanPartitionState {
    struct ExternalCandidate {
      std::uintptr_t entry_address_ = 0;
      ExtentManifest extents_;
      RecordLocation location_{};
      std::uint32_t hash_ = 0;
      std::uint32_t key_bytes_ = 0;
      std::size_t value_bytes_ = 0;
    };

    const RecordIndex* index_ = nullptr;
    std::uint64_t now_ms_ = 0;
    std::size_t count_ = 0;
    std::size_t max_bytes_ = 0;
    std::size_t max_iterations_ = 0;
    std::size_t iterations_ = 0;
    std::size_t bytes_ = 0;
    ScanBatch result_;
    std::vector<ExternalCandidate> external_;
  };

  ScanPartitionAwaitable ScanPartition(std::uint16_t partition_id,
                                       std::uint8_t db_id, std::uint64_t cursor,
                                       std::size_t count, std::uint64_t now_ms,
                                       std::size_t max_bytes = SIZE_MAX);

  bool ScanPartitionInline(ScanPartitionState* state);
  Task<absl::StatusOr<ScanBatch>> ResumeScanPartition(ScanPartitionState state);

  std::uint64_t FullSyncCoverageEntryBytes(
      std::size_t logical_key_bytes) const noexcept;
  void AddFullSyncCoverageEntry(WorkerStore::PartitionStore& partition,
                                std::uint8_t db_id,
                                std::size_t logical_key_bytes) noexcept;
  void RemoveFullSyncCoverageEntry(WorkerStore::PartitionStore& partition,
                                   std::uint8_t db_id,
                                   std::size_t logical_key_bytes) noexcept;

  absl::Status BeginRdbSnapshot(std::uint64_t session_id,
                                std::uint64_t snapshot_time_ms);
  Task<absl::StatusOr<RdbSnapshotBatch>> ReadRdbSnapshotBatch(
      std::uint64_t session_id, RdbSnapshotCursor cursor, std::size_t count,
      std::size_t max_bytes);
  Task<absl::Status> EndRdbSnapshot(std::uint64_t session_id);
  Task<absl::StatusOr<CollectionPage>> ReadRdbCollectionPage(
      std::uint64_t session_id, std::uint64_t token, std::uint64_t cursor);
  Task<absl::Status> FinishRdbCollection(std::uint64_t session_id,
                                         std::uint64_t token);

  absl::StatusOr<FullSyncSessionStart> BeginFullSyncSession(
      std::uint64_t session_id);
  bool FullSyncSessionValid(std::uint64_t session_id) const noexcept;
  void EndFullSyncSession(std::uint64_t session_id);

  absl::StatusOr<PartitionReplicationStart> BeginPartitionReplication(
      std::uint64_t session_id, std::uint16_t partition_id);

  absl::Status BeginPartitionDbReplication(std::uint64_t session_id,
                                           std::uint16_t partition_id,
                                           std::uint8_t db_id);

  void EndPartitionReplication(std::uint64_t session_id,
                               std::uint16_t partition_id);

  Task<absl::StatusOr<PartitionSnapshotBatch>> SnapshotPartition(
      std::uint64_t session_id, std::uint16_t partition_id, std::uint8_t db_id,
      std::uint64_t cursor, std::size_t count, std::size_t read_concurrency,
      std::size_t max_bytes);

  Task<absl::StatusOr<PartitionFullSyncBatch>> ReadPartitionFullSyncOverrides(
      std::uint64_t session_id, std::uint16_t partition_id, std::size_t count,
      std::size_t max_bytes);

  Task<absl::StatusOr<SnapshotRecord>> MaterializeFullSyncPublishRecord(
      std::uint64_t session_id, std::uint16_t partition_id,
      const SnapshotRecord& requested);
  Task<absl::StatusOr<std::string>> ReadFullSyncValueChunk(
      std::uint64_t session_id, std::uint16_t partition_id,
      std::uint64_t source_id, std::uint64_t offset, std::size_t max_bytes);
  void ReleaseFullSyncValue(std::uint64_t session_id,
                            std::uint16_t partition_id,
                            std::uint64_t source_id);

  Task<absl::Status> EnableReplicationLog(std::uint64_t log_epoch,
                                          std::size_t capacity_bytes);
  Task<absl::Status> SetReplicationLogCapacity(std::size_t capacity_bytes);
  Task<absl::Status> SetReplicationBacklogBackpressure(bool enabled);
  Task<absl::Status> SetReplicationPublishQueueCapacity(
      std::size_t capacity_bytes);
  Task<absl::StatusOr<std::uint64_t>> AppendReplicationLog(
      ReplicationLogAppend event);
  Task<absl::StatusOr<std::uint64_t>> FenceReplicationLog();
  Task<absl::StatusOr<ReplicationLogBatch>> ReadReplicationLog(
      ReplicationLogCursor next, std::size_t max_bytes, std::size_t max_frames);
  absl::Status RetainReplicationLog(std::uint64_t session_id,
                                    std::uint64_t keep_from_lsn);
  void ReleaseReplicationLogRetention(std::uint64_t session_id);
  Task<absl::Status> TrimReplicationLog(std::uint64_t keep_from_lsn);
  Task<absl::Status> DisableReplicationLog();
  ReplicationLogInfo LocalReplicationLogInfo() const;
  bool ReplicationLogActive() const noexcept;
  Task<absl::StatusOr<ReplicationPublisherAdmission>>
  AcquireReplicationPublisherAdmission(
      std::size_t logical_bytes,
      std::optional<ReplicationPublisherTarget> target);
  std::optional<ReplicationPublisherAdmission>
  TryAcquireFullSyncReplacementAdmission(std::size_t logical_bytes,
                                         ReplicationPublisherTarget target);
  void ReleaseReplicationPublisherAdmission(
      const ReplicationPublisherAdmission& admission,
      std::size_t logical_bytes);
  bool TryEnqueueReplicationCommand(ReplicationCommandAppend command);
  Task<absl::Status> PublishEphemeralReplicationCommand(
      std::uint16_t partition_id, std::vector<std::string> args,
      MutationPrecondition mutation_precondition);
  absl::StatusOr<PreparedReplicationCommandPublication>
  PrepareAdmittedReplicationCommand(
      const ReplicationPublisherAdmission& admission, ReplicationEventKind kind,
      std::uint16_t partition_id, std::vector<std::string> args,
      std::optional<std::vector<std::string>> fullsync_projection);
  absl::Status PublishPreparedReplicationCommand(
      const ReplicationPublisherAdmission& admission,
      PreparedReplicationCommandPublication publication);
  absl::Status PublishLateAdmittedReplicationCommand(
      const ReplicationPublisherAdmission& admission, ReplicationEventKind kind,
      std::uint16_t partition_id, std::vector<std::string> args,
      std::optional<std::vector<std::string>> fullsync_projection);
  bool TryEnqueueReplicationTransaction(
      std::shared_ptr<ReplicationTransaction> transaction);

  void AcknowledgePartitionFullSyncOverrides(
      std::uint64_t session_id, std::uint16_t partition_id,
      std::span<const SnapshotRecord> records);

  void AcknowledgePartitionSnapshotRecords(
      std::uint64_t session_id, std::uint16_t partition_id,
      std::span<const SnapshotRecord> records);

  absl::Status CompletePartitionReplication(std::uint64_t session_id,
                                            std::uint16_t partition_id);

  absl::Status CompletePartitionDbReplication(std::uint64_t session_id,
                                              std::uint16_t partition_id,
                                              std::uint8_t db_id);

  absl::StatusOr<std::vector<FullSyncPublishItem>> PeekFullSyncPublishItems(
      std::uint64_t session_id, std::size_t max_items);

  absl::StatusOr<FullSyncPublishQueueInfo> GetFullSyncPublishQueueInfo(
      std::uint64_t session_id) const;

  void AcknowledgeFullSyncPublishItem(std::uint64_t session_id,
                                      std::uint64_t item_id);

  Task<absl::StatusOr<std::uint64_t>> ResetReplicaPartition(
      std::uint16_t partition_id,
      std::span<const std::uint64_t, kLogicalDatabaseCount> source_db_epochs,
      std::uint64_t persisted_replication_epoch = 0,
      bool replica_lock_held = false);
  Task<absl::StatusOr<std::vector<ReplicaPartitionEpoch>>>
  ResetReplicaPartitions(std::uint64_t session_id,
                         std::span<const ReplicaPartitionReset> resets);
  Task<absl::Status> ResetPartitionsDetach(
      std::span<const std::uint16_t> partition_ids);
  Task<absl::Status> ResetPartitionsDetachLocal(
      std::span<const std::uint16_t> partition_ids);
  Task<absl::Status> HandoffReplicaPartition(std::uint64_t session_id,
                                             std::uint16_t partition_id,
                                             std::uint64_t replication_epoch);
  Task<absl::Status> BeginReplicaTailCommand(std::uint64_t session_id,
                                             std::uint16_t partition_id,
                                             std::uint64_t partition_sequence);
  Task<absl::Status> EndReplicaTailCommand(std::uint64_t session_id,
                                           std::uint16_t partition_id,
                                           std::uint64_t partition_sequence);

  Task<absl::Status> ApplyReplicaRecords(
      std::uint64_t session_id, std::uint16_t partition_id,
      std::uint64_t replication_epoch, std::span<const SnapshotRecord> records);
  // The outer apply mutex spans validation, page writes, and abort settlement.
  Task<absl::Status> ApplyReplicaRecordsLocked(
      std::uint64_t session_id, std::uint16_t partition_id,
      std::uint64_t replication_epoch, std::span<const SnapshotRecord> records);
  Task<absl::Status> BeginReplicaCollection(
      WorkerStore& store, WorkerStore::PartitionStore& partition,
      ReplicaValueStage& stage);
  Task<absl::Status> ConsumeReplicaCollection(
      WorkerStore& store, WorkerStore::PartitionStore& partition,
      ReplicaValueStage& stage, std::string_view input, bool finish);
  Task<absl::Status> WriteReplicaCollectionPage(
      WorkerStore& store, WorkerStore::PartitionStore& partition,
      ReplicaValueStage& stage, CollectionPage page);
  absl::Status SquashReplicaCollectionUndo(WorkerStore& store,
                                           ReplicaCollectionStage& state);
  Task<absl::Status> AbortReplicaValueStage(
      WorkerStore& store, WorkerStore::PartitionStore& partition);
  Task<absl::Status> PromoteReplicaRoot(std::uint64_t session_id);
  Task<absl::Status> AbortReplicaRoot(std::uint64_t session_id);
  void SetReplicaLoading(bool loading) noexcept {
    replica_loading_.store(loading, std::memory_order_release);
  }
  Task<absl::Status> DrainReplicaRootWritesLocal(WorkerStore& store);

  absl::Status FlushForShutdown();

  Task<absl::StatusOr<CatalogDurabilityToken>> CommitFunctionCatalog(
      std::string_view dump);
  absl::StatusOr<std::optional<RecoveredFunctionCatalog>>
  RecoverFunctionCatalog() const;
  Task<absl::Status> MakeDurable(const DurabilityFrontier& frontier,
                                 std::string_view opaque_accumulator);
  Task<absl::Status> CommitPromotionBase(PromotionBase base);
  absl::StatusOr<std::optional<PromotionBase>> RecoverPromotionBase() const;
  Task<absl::Status> CommitPopulationIdentity(std::string identity);
  Task<absl::StatusOr<std::optional<PopulationRecoveryRecord>>>
  ConsumePopulationRecovery();
  void StageCleanShutdownProof(std::string proof);
  Task<absl::Status> PublishCleanShutdownProof();
  absl::StatusOr<PopulationToken> RecoverPopulationToken() const;
  Task<absl::Status> BeginReplicaFullSync(std::uint64_t session_id);
  Task<absl::Status> CompleteReplicaFullSync(std::uint64_t session_id,
                                             PopulationToken population);
  bool ReplicaRecoveryFenced() const noexcept {
    return replica_recovery_fenced_.load(std::memory_order_acquire) ||
           request_serving_fenced_until_restart_.load(
               std::memory_order_acquire);
  }
  void FenceRequestServingUntilRestart() noexcept {
    request_serving_fenced_until_restart_.store(true,
                                                std::memory_order_release);
    replica_recovery_fenced_.store(true, std::memory_order_release);
    replica_loading_.store(true, std::memory_order_release);
    // Publish the monitor notification last. Its acquire load then proves the
    // immediate request/replication fence was already visible before worker
    // zero begins the asynchronous NodeControl barrier.
    runtime_failure_latched_.store(true, std::memory_order_release);
  }
  bool RuntimeFailureLatched() const noexcept {
    return runtime_failure_latched_.load(std::memory_order_acquire);
  }
  // Worker-local write_failed_ preserves existing shutdown/drain diagnostics;
  // the process-wide latch closes every request path and lets worker zero join
  // NodeControl cleanup. Both are irreversible for this process.
  void LatchRuntimeFailure(WorkerStore& store) noexcept {
    store.write_failed_ = true;
    LatchRuntimeFailure();
  }
  void LatchRuntimeFailure() noexcept { FenceRequestServingUntilRestart(); }

 private:
  friend class ExpirationAuthorityTestPeer;

  struct DurableSystemState {
    std::uint64_t generation_ = 0;
    CatalogDurabilityToken catalog_token_{};
    std::uint64_t catalog_bytes_ = 0;
    ExtentManifest catalog_extents_;
    ExtentManifest manifest_extents_;
    std::optional<PromotionBase> promotion_base_;
    std::string population_identity_;
    std::string clean_shutdown_proof_;
    PopulationToken population_token_{};
    std::uint64_t full_sync_session_id_ = 0;
    bool catalog_ready_ = false;
  };

  absl::Status LoadSystemState();
  Task<absl::Status> CommitSystemState(DurableSystemState next,
                                       std::string_view catalog_dump,
                                       bool replace_catalog,
                                       bool shutdown_metadata = false);
  Task<absl::Status> WriteSystemStateRootOnDeviceLocal(
      std::size_t device_index, const SystemStateRoot& root,
      std::uint8_t target_slot);
  static absl::StatusOr<std::string> EncodePromotionBase(
      const PromotionBase& base);
  static absl::StatusOr<PromotionBase> DecodePromotionBase(
      std::string_view encoded);
  static absl::StatusOr<std::string> EncodeSystemStateManifest(
      const DurableSystemState& state);
  static absl::StatusOr<DurableSystemState> DecodeSystemStateManifest(
      std::string_view encoded);

  struct SnapshotReadJoin;
  Task<absl::Status> ReadSnapshotRecord(
      WorkerStore& store, WorkerStore::PartitionStore& partition,
      RecordIndex& index, std::uint8_t db_id, const std::string* key,
      std::uint64_t session_id, std::uint64_t baseline_version,
      std::optional<SnapshotRecord>* output, SnapshotReadJoin* join);
  Task<absl::StatusOr<SnapshotRecord>> ReadFullSyncOverrideRecord(
      WorkerStore& store, WorkerStore::PartitionStore& partition,
      std::uint64_t session_id, const SnapshotRecord& requested);

  Task<absl::Status> EnsureReplicationLogActiveBlock(
      WorkerStore& store, std::uint64_t protected_lsn);
  absl::StatusOr<WorkerStore::ReplicationLogBlock>
  AllocateReplicationLogBlock();
  void EnsureReplicationLogStandby(WorkerStore& store);
  Task<absl::Status> RefillReplicationLogStandby(WorkerStore* store,
                                                 std::uint64_t log_epoch);
  Task<absl::Status> SealReplicationLogActiveBlock(WorkerStore& store);
  Task<absl::Status> ReclaimReplicationLogPrefix(WorkerStore& store,
                                                 std::uint64_t keep_from_lsn,
                                                 bool force_all = false);
  Task<absl::Status> DrainReplicationPublishQueue(WorkerStore* store);

  static StagingSlot* StagingFor(WorkerStore& store, const BlockState& state);

  static std::uint16_t AcquireStagingSlot(WorkerStore& store);

  FixedBuffer StagingBufferFor(WorkerStore& store,
                               const BlockState& state) const;

  static void ReleaseStagingBuffer(WorkerStore& store, BlockState& state);

  struct LoadedValue {
    ReadBufferLease lease_;
    std::size_t value_offset_ = 0;
    std::size_t value_bytes_ = 0;
    // The physical record tag is needed by defrag when it republishes a
    // value. It is deliberately not stored in RecordLocation: the tag is cold
    // metadata and widening every index entry would penalize all keys for a
    // relocation-only read.
    std::uint64_t txid_ = 0;

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

  WorkerStore& CurrentStore() { return *stores_[bycorf::ThisWorker().id_]; }

  const WorkerStore& CurrentStore() const {
    return *stores_[bycorf::ThisWorker().id_];
  }

  absl::StatusOr<std::uint64_t> AllocateLsn(WorkerStore& store) {
    const std::uint64_t stride = worker_count_;
    const std::uint64_t lsn = store.next_lsn_;
    if (lsn == 0) {
      return absl::Status(absl::StatusCode::kResourceExhausted,
                          "physical LSN space is exhausted");
    }
    store.next_lsn_ = lsn > std::numeric_limits<std::uint64_t>::max() - stride
                          ? 0
                          : lsn + stride;
    return lsn;
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

  BlockState& CreateBlockState(WorkerStore& store, std::uint64_t block_id,
                               std::uint64_t allocation_epoch) {
    BlockState& state = BlockStateAt(block_id);
    state.Reset(static_cast<std::uint16_t>(store.worker_->id()),
                allocation_epoch);
    return state;
  }

  void DestroyBlockState(WorkerStore& store, std::uint64_t block_id) {
    store.tx_blocks_.erase(block_id);
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
      std::size_t device_index, AllocationPurpose purpose);

  Task<absl::StatusOr<ReservedBlock>> AllocateFromDevice(
      std::size_t device_index, AllocationPurpose purpose);

  Task<absl::Status> ReturnColdBlocksLocal(
      std::size_t device_index, std::vector<std::uint64_t> block_ids);

  Task<absl::Status> ReturnColdBlocks(std::vector<std::uint64_t> block_ids);

  Task<absl::Status> PersistEpochValueOnDeviceLocal(std::size_t device_index,
                                                    std::size_t value_index,
                                                    std::uint64_t epoch);

  Task<absl::Status> PersistEpochValuesOnDeviceLocal(
      std::size_t device_index,
      std::span<const std::pair<std::size_t, std::uint64_t>> values);

  Task<absl::Status> PersistEpochValue(std::size_t value_index,
                                       std::uint64_t epoch);

  Task<absl::Status> PersistEpochValues(
      std::span<const std::pair<std::size_t, std::uint64_t>> values);

  Task<absl::Status> PersistCheckpointRootOnDeviceLocal(
      std::size_t device_index, const CheckpointRoot& root);

  Task<absl::Status> PersistCheckpointRoot(const CheckpointRoot& root);

  // Takes the database out of service on this worker. Everything here is O(the
  // partition count) and runs without suspending, so the caller's FLUSHDB gate
  // stays closed for a bounded time no matter how many keys the database holds.
  // Retiring the detached entries is left to ReclaimDetachedIndexes.
  void DetachDbLocal(WorkerStore& store, std::uint8_t db_id);

  // Retires entries detached by database flush or a replica population
  // transition: subtracts what they contributed to their blocks, then frees
  // them. Readers can no longer reach any of it, so asynchronous callers do
  // not need to wait for this work before observing the new keyspace.
  //
  // Each block is credited once for the whole population rather than once per
  // record, which is the same total by construction and turns a per-record
  // cross-core hop into a per-block one. A block cannot be recycled underneath
  // an outstanding subtraction: whatever is still owed keeps its live_bytes
  // above zero, so it cannot reach the empty-block path until this settles.
  Task<absl::Status> ReclaimDetachedIndexes(WorkerStore& store);

  void QueueDetachedIndex(WorkerStore& store, RecordIndex& index,
                          std::uint8_t db_id,
                          GroupedObjectIndex* grouped = nullptr);

  // At most one reclaimer runs per store, so background and synchronous callers
  // cannot split the detached-index FIFO between them. Whichever runs picks up
  // work queued after it started, so an arriving caller only has to make sure
  // one is alive.
  void EnsureDetachedReclaim(WorkerStore& store);

  Task<absl::Status> RunDetachedReclaim(WorkerStore* store);

  // Waits for everything detached so far to be retired. The keyspace is already
  // unreachable; synchronous flush and replica lifecycle callers use this to
  // ensure index memory and live-block accounting have settled before success.
  Task<absl::Status> AwaitDetachedReclaim(WorkerStore& store);

  void Fail(const absl::Status& status);

  // Block states live in one dense array per device, indexed by local block
  // id. Each array is sized once at startup and never resized, so entries
  // never move and a BlockState* stays valid across suspension points.
  struct BlockStateLookup {
    BlockState* states_ = nullptr;
    std::uint32_t local_begin_ = 0;
    std::uint32_t local_end_ = 0;
  };

  BlockState& BlockStateAt(std::uint64_t block_id) noexcept {
    const std::size_t device_index = DeviceIdForBlock(block_id);
    assert(device_index < block_state_lookup_.size());
    const BlockStateLookup& lookup = block_state_lookup_[device_index];
    const std::uint32_t local = LocalBlockId(block_id);
    assert(local >= lookup.local_begin_ && local < lookup.local_end_);
    return lookup.states_[local - lookup.local_begin_];
  }

  // Which worker owns a block, or kUnownedBlock if it is free. Along with the
  // immutable allocation epoch, this is safe for a key owner to read directly.
  std::uint16_t BlockOwner(std::uint64_t block_id) const noexcept {
    const std::size_t device_index = DeviceIdForBlock(block_id);
    const std::uint32_t local = LocalBlockId(block_id);
    if (device_index >= block_state_lookup_.size()) return kUnownedBlock;
    const BlockStateLookup& lookup = block_state_lookup_[device_index];
    if (local < lookup.local_begin_ || local >= lookup.local_end_) {
      return kUnownedBlock;
    }
    return lookup.states_[local - lookup.local_begin_].owner_.load(
        std::memory_order_acquire);
  }

  // Materialize the self-contained identity only while the entry is known to
  // be current. Publication charges its record to an allocated BlockState,
  // and retirement cannot reset that state until the index stops referencing
  // it. The returned value then owns the epoch snapshot and is safe to carry
  // across suspension even if a later relocation replaces the index entry.
  RecordLocation MaterializeIndexLocation(
      const RecordIndex::Entry& entry) const noexcept {
    const std::uint64_t block_id = entry.value_.block_id();
    const BlockState& state = const_cast<Impl*>(this)->BlockStateAt(block_id);
    RecordLocation location = MaterializePublishedIndexLocation(entry, state);
    assert(location.block_owner() < worker_count_);
    return location;
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
  Task<absl::Status> ApplyRecoveryBatches(WorkerStore& store,
                                          std::vector<RecoveryBatch>* batches);
  Task<absl::Status> ApplyRecoveryLiveReferenceBatches(
      WorkerStore& store,
      std::vector<std::vector<RecoveryLiveReference>>* batches);

  absl::Status ApplyRecovery(unsigned target, RecoveryBatch batch);

  absl::Status ApplyRecoveredRecord(WorkerStore& store,
                                    const RecoveryRecord& record);
  absl::Status ApplyRecoveredRecord(WorkerStore& store,
                                    WorkerStore::PartitionStore& partition,
                                    const RecoveryRecordView& record);
  Task<absl::Status> RecoverGroupedObjects(WorkerStore& store);
  Task<absl::StatusOr<GroupedHashObject::Handle>> RecoverOrderedObject(
      WorkerStore& store, const OrderedCollectionRoot& root,
      GroupedObjectVersion version, std::span<RecoveryRecord> candidates);
  // Validate only root-selected external groups. Obsolete value-only extents
  // can already be gone while their containing records block remains live.
  Task<absl::Status> ValidateRecoveredGroups(WorkerStore& store);

  static ExtentManifest ExtentsFor(const WorkerStore& store,
                                   const RecordIndex::Entry* entry) {
    if (entry == nullptr || !entry->value_.external()) {
      return {};
    }
    const auto found = store.external_manifests_.find(entry);
    return found == store.external_manifests_.end() ? ExtentManifest{}
                                                    : found->second;
  }

  static ExtentManifest DependentExtentsFor(const WorkerStore& store,
                                            const RecordIndex::Entry* entry) {
    if (entry == nullptr || !entry->value_.key_external()) [[likely]] {
      return {};
    }
    return entry->value_.external() ? ExtentsFor(store, entry)
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
  Task<absl::Status> ReadRecoveryExtentInto(
      WorkerStore& store, ExtentRef ref, std::uint32_t extent_index,
      std::span<std::byte> destination, std::size_t payload_offset = 0,
      OrderedGroupMetadataDecoder* ordered = nullptr);
  // Read a bounded slice while validating every extent in the manifest. Used
  // for group envelopes so an indivisible large field does not become a large
  // recovery allocation merely to reconstruct resident routing metadata.
  // Optional ordered decoding observes all bytes after offset during that same
  // pass, not just the returned slice. Its caller-owned state outlives the
  // task.
  Task<absl::StatusOr<std::string>> LoadRecoveryPayloadSlice(
      WorkerStore& store, ExtentManifest extents, std::size_t offset,
      std::size_t bytes, OrderedGroupMetadataDecoder* ordered = nullptr);

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
      WorkerStore& key_store, WorkerStore::PartitionStore& partition,
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      RecordLocation location, ExtentManifest extents,
      ReadLatencyTrace* trace = nullptr,
      GroupedHashObject::Handle grouped_snapshot = nullptr);

  // The optional snapshot view must already own physical pins. Ordinary
  // reads instead retry GC relocation against the same logical root version.
  Task<absl::StatusOr<LoadedHashGroup>> LoadHashGroupSnapshot(
      WorkerStore& store, WorkerStore::PartitionStore& partition,
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      GroupedHashObject::Handle object, HashGroupId id, bool pinned = false);
  Task<absl::StatusOr<HashValue>> LoadGroupedHashValue(
      WorkerStore& store, WorkerStore::PartitionStore& partition,
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      GroupedHashObject::Handle object, bool pinned = false);
  Task<absl::StatusOr<LoadedValue>> LoadGroupedValue(
      WorkerStore& store, WorkerStore::PartitionStore& partition,
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      RecordLocation location, GroupedHashObject::Handle snapshot);

  // Reads one extent block's payload into `destination`. Runs on the worker
  // that owns that block, which is not necessarily the one holding the
  // manifest, so everything it needs is passed by value.
  Task<absl::Status> ReadExtentInto(WorkerStore& store, ExtentRef ref,
                                    std::uint32_t extent_index,
                                    std::byte* destination);
  Task<absl::Status> ReadExtentSlice(WorkerStore& store, ExtentRef ref,
                                     std::uint32_t extent_index,
                                     std::size_t source_offset,
                                     std::span<std::byte> destination);

  Task<absl::StatusOr<LoadedValue>> LoadExternalValueLocal(
      WorkerStore& store, const RecordLocation& location,
      ExtentManifest extents, std::size_t key_bytes, ReadLatencyTrace* trace);

  Task<absl::StatusOr<LoadedValue>> LoadValueLocal(
      WorkerStore& store, std::uint8_t db_id, std::string_view key,
      RecordLocation location, std::uint64_t replication_epoch,
      ReadLatencyTrace* trace = nullptr,
      std::optional<std::uint64_t> expected_db_epoch = std::nullopt);

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

  absl::Status ConfigureWorkerDeviceAffinity();

  Task<absl::StatusOr<ReservedBlock>> AllocateBlock(WorkerStore& store,
                                                    AllocationPurpose purpose);

  std::size_t DefragReserveForDevice(std::size_t device_index) const noexcept {
    assert(device_index < defrag_reserve_blocks_.size());
    return defrag_reserve_blocks_[device_index];
  }

  absl::Status MarkRecordDeadLocal(unsigned owner, const RetiredRecord& record);

  Task<absl::Status> MarkRecordDead(const RetiredRecord& record);

  Task<absl::Status> MarkRetiredRecordsDead(WorkerStore* store,
                                            std::vector<RetiredRecord> records);

  // Captures child physical epochs synchronously while old/replacement views
  // are current or protected by pending transaction retirement. The root's
  // existing receipt is separate; unchanged groups and reused extents never
  // enter this child-only batch. The admitted shared charge follows copies
  // through undo/transaction/flush receipts via retained_owner_.
  absl::StatusOr<std::vector<RetiredRecord>> CollectGroupedRetirements(
      const GroupedHashObject::Handle& previous,
      const GroupedHashObject::Handle& replacement,
      std::optional<std::span<const HashGroupId>> touched = std::nullopt);
  Task<absl::Status> ClearGroupedUndoSlots(WorkerStore& store,
                                           const TxUndoLog& undo);
  struct GroupedRetirementPins {
    GroupedRetirementPins(Impl* engine, WorkerStore* store)
        : engine_(engine), store_(store) {}
    ~GroupedRetirementPins();
    GroupedRetirementPins(const GroupedRetirementPins&) = delete;
    GroupedRetirementPins& operator=(const GroupedRetirementPins&) = delete;
    void Sort();
    bool Contains(const RetiredRecord& record) const;
    bool Take(const RetiredRecord& record);
    Impl* engine_;
    WorkerStore* store_;
    RetainedMemoryCharge charge_;
    std::vector<RetiredRecord> pins_;
    std::size_t sorted_count_ = 0;
  };
  Task<absl::Status> PrepinGroupedRetirementsLocked(
      WorkerStore& store, const GroupedHashObject::Handle& previous,
      std::optional<std::span<const HashGroupId>> touched, bool include_root,
      std::unique_ptr<GroupedRetirementPins>* pins);
  Task<absl::Status> ReleaseGroupedRetirementPins(
      WorkerStore* store, std::vector<RetiredRecord> pins);

  static RetiredRecord RetiredRecordOf(const RecordLocation& location,
                                       ExtentManifest dependent_extents = {}) {
    return RetiredRecord{
        .block_id_ = location.block_id(),
        .allocation_epoch_ = location.allocation_epoch(),
        .total_disk_bytes_ = location.total_disk_bytes(),
        .block_owner_ = location.block_owner(),
        .record_offset_ = location.record_offset(),
        .tx_tagged_ = location.tx_tagged(),
        .dependency_pinned_ = false,
        .dependent_extents_ = std::move(dependent_extents),
        .immediate_extents_ = nullptr,
        .extra_dependent_extents_ = nullptr,
    };
  }

  static StagedRetiredRecord StagedRetiredRecordOf(
      const RecordLocation& location, ExtentManifest dependent_extents = {}) {
    return StagedRetiredRecord{
        .dependent_extents_ = std::move(dependent_extents),
        .block_id_ = location.block_id(),
        .allocation_epoch_ = location.allocation_epoch(),
        .total_disk_bytes_ = location.total_disk_bytes(),
        .block_owner_ = location.block_owner(),
        .record_offset_ = location.record_offset(),
        .tx_tagged_ = location.tx_tagged(),
        .present_ = true,
    };
  }

  void NoteTxRecordLocal(WorkerStore& store, std::uint64_t block_id,
                         std::uint64_t allocation_epoch,
                         std::uint64_t generation, std::uint64_t txid,
                         std::uint32_t bytes, bool commit);

  void DropTaggedRecordLocal(WorkerStore& store, std::uint64_t block_id,
                             std::uint64_t allocation_epoch,
                             std::uint32_t bytes) noexcept;

  bool PinTxDependencyLocal(WorkerStore& store,
                            const RecordLocation& location) noexcept;
  void UnpinTxDependencyLocal(WorkerStore& store, std::uint64_t block_id,
                              std::uint64_t allocation_epoch) noexcept;

  Task<absl::StatusOr<std::shared_ptr<const std::vector<ExtentRef>>>>
  WriteExtentValueLocked(WorkerStore& store, std::string_view first,
                         std::string_view second = {},
                         RecordPayloadCursor* cursor = nullptr,
                         std::string_view fault_key = {},
                         bool shutdown_metadata = false);

  struct GroupMutationWrite;
  Task<absl::Status> AppendLocked(
      WorkerStore& store, WorkerStore::PartitionStore& partition,
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      std::string_view value, RecordKind kind, ValueType value_type,
      std::uint64_t expire_at_ms, TxShardWrites* tx = nullptr,
      std::uint64_t logical_size = std::numeric_limits<std::uint64_t>::max(),
      std::unique_ptr<std::vector<RetiredRecord>> commit_retirements = nullptr,
      std::uint64_t* committed_sequence = nullptr,
      ReplicationCommandAppend* replication = nullptr,
      SetLatencyTrace* trace = nullptr, bool capture_fullsync = true,
      TxUndoLog* replacement_undo = nullptr,
      const MutationPrecondition* mutation_precondition = nullptr,
      GroupMutationWrite* grouped = nullptr);

  Task<absl::Status> CaptureRdbSnapshotBeforeWriteLocked(
      WorkerStore& store, WorkerStore::PartitionStore& partition,
      std::uint8_t db_id, std::string_view key, const Digest& digest);
  Task<absl::Status> PinRdbSnapshotValue(
      WorkerStore::PartitionStore::RdbSnapshotValue* value);
  // Capture compact child epochs before suspension while the view is current.
  // PinRdbSnapshotValue validates this admitted list; it never lazily creates
  // identities from a potentially stale retained metadata view.
  absl::Status PrepareGroupedSnapshotPins(
      WorkerStore::PartitionStore::RdbSnapshotValue* value);
  Task<absl::Status> ReleaseRdbSnapshotValue(
      WorkerStore::PartitionStore::RdbSnapshotValue* value);
  Task<absl::StatusOr<std::optional<storage::RdbSnapshotValue>>>
  MaterializeRdbSnapshotKey(WorkerStore& store,
                            WorkerStore::PartitionStore& partition,
                            std::uint64_t session_id, std::uint8_t db_id,
                            std::string key);

  void FullSyncOnCommit(
      WorkerStore& store, WorkerStore::PartitionStore& partition,
      const SnapshotRecord& record, const Digest& digest,
      std::shared_ptr<const ReplicationCommandAppend> command = nullptr);
  void FullSyncCaptureOnCommit(
      WorkerStore& store, std::uint64_t session_id,
      WorkerStore::FullSyncCapture& capture, const SnapshotRecord& record,
      const Digest& digest,
      std::shared_ptr<const ReplicationCommandAppend> command = nullptr,
      bool transaction_effect = false);
  bool TryEnqueueFullSyncCommand(
      WorkerStore& store, std::uint64_t session_id,
      std::shared_ptr<const ReplicationCommandAppend> command);
  bool TryEnqueueFullSyncRecord(WorkerStore& store, std::uint64_t session_id,
                                const SnapshotRecord& record);
  bool TryConsumeFullSyncCoverageCredit(WorkerStore& store,
                                        std::uint64_t session_id,
                                        WorkerStore::FullSyncCapture& capture,
                                        std::size_t key_bytes,
                                        bool allocates_arena_entry);
  bool TryConsumeFullSyncCredit(WorkerStore& store, std::uint64_t session_id,
                                WorkerStore::FullSyncCapture& capture,
                                std::size_t bytes);
  bool TryConsumeFullSyncArenaCredit(WorkerStore& store,
                                     std::uint64_t session_id,
                                     WorkerStore::FullSyncCapture& capture);
  void InvalidateFullSyncSession(WorkerStore& store, std::uint64_t session_id);
  void RestoreFullSyncCoverageCredit(WorkerStore& store,
                                     std::uint64_t session_id,
                                     WorkerStore::FullSyncCapture& capture);
  void ClearFullSyncCapture(WorkerStore& store, std::uint64_t session_id,
                            WorkerStore::FullSyncCapture& capture);
  Task<absl::Status> PinFullSyncExtents(ExtentManifest extents);
  Task<absl::Status> ReleaseFullSyncExtents(ExtentManifest extents);
  absl::Status PrepareFullSyncPinnedValueInsert(
      WorkerStore::FullSyncCapture& capture);
  Task<absl::StatusOr<std::uint64_t>> PinFullSyncCollection(
      WorkerStore& store, std::uint64_t session_id,
      WorkerStore::PartitionStore& partition, std::uint8_t db_id,
      std::string_view key, const Digest& digest, RecordLocation location,
      ExtentManifest root_extents, std::uint64_t* encoded_bytes);
  Task<absl::StatusOr<std::string>> ReadFullSyncCollectionChunk(
      std::shared_ptr<FullSyncCollection> collection, std::uint64_t offset,
      std::size_t max_bytes);
  // Register active_settlements_ before spawning this release. It cancels and
  // joins an in-flight reader before unpinning its exact captured graph.
  Task<absl::Status> ReleaseFullSyncCollection(
      std::shared_ptr<FullSyncCollection> collection);
  Task<absl::StatusOr<CollectionPage>> NextFullSyncCollectionPage(
      std::shared_ptr<FullSyncCollection> collection);
  Task<absl::StatusOr<std::uint64_t>> PinFullSyncValue(
      WorkerStore& store, std::uint64_t session_id,
      WorkerStore::PartitionStore& partition, RecordLocation location,
      ExtentManifest extents, std::size_t key_bytes);

  Task<absl::StatusOr<ReservedBlock>> AcquireWriteBlock(WorkerStore& store,
                                                        bool for_defrag,
                                                        bool unlock_writer);

  Task<absl::Status> ReturnReservedBlock(ReservedBlock block);

  // Called once after publishing each ordinary active block. It maintains one
  // reserved successor without adding an occupancy check to every append.
  void EnsureStandbyBlock(WorkerStore& store);

  Task<absl::Status> PrefetchStandbyBlock(WorkerStore* store,
                                          std::uint64_t source_block_id,
                                          std::uint64_t source_epoch);

  struct ExplicitWriteRoot {
    RecordIndex* index_ = nullptr;
    std::size_t* live_key_count_ = nullptr;
    std::size_t* store_live_key_count_ = nullptr;
    std::size_t* expiring_key_count_ = nullptr;
    std::uint64_t replication_epoch_ = 0;
    std::uint64_t db_epoch_ = 0;
    bool reject_older_sequence_ = false;
    // Snapshot records reject equal sequence duplicates. A serialized replay
    // command envelope may intentionally mutate one key repeatedly at the
    // same sequence, and uses physical publication LSN to order those writes.
    bool allow_equal_sequence_ = false;
  };

  // Auxiliary snapshots share the parent's durable key/epochs, but never
  // compete in its top-level RecordIndex or affect Redis key/expiry counts.
  // The caller owns their publication and retirement through the grouped
  // object view. All foreground auxiliaries require a transaction receipt.
  struct GroupRecordWrite {
    bool auxiliary_ = false;
    std::uint64_t incarnation_ = 0;
    HashGroupId id_{};
    bool retired_ = false;
    std::uint64_t batch_txid_ = 0;
    // Pre-admitted before any durable root write. The root and its side view
    // become visible in one non-suspending publication section.
    GroupedHashObject::PreparedHandle* prepared_root_ = nullptr;
    GroupedObjectIndex::Publication* publication_ = nullptr;
    // Invoked after the last possible storage/index wait but before staging
    // any root bytes. It rebuilds against current physical GC locations and
    // performs all remaining fallible metadata admission.
    std::function<absl::Status(const GroupedObjectVersion&)> prepare_root_;
    // Every complete page or split marker written by this mutation. Borrowed
    // for this call; only these prior ids need dependency pins/retirement.
    std::span<const HashGroupId> changed_groups_;
    // Foreground root after-image identity, known before the late builder.
    // A new incarnation replaces the complete prior graph, even when its
    // newly written ids happen to match a subset of the old directory.
    std::uint64_t root_incarnation_ = 0;
  };

  struct GroupMutationWrite {
    std::uint64_t sequence_ = 0;
    GroupRecordWrite* root_ = nullptr;
  };

  absl::StatusOr<std::shared_ptr<GroupedCommitDecision>> PrepareGroupedDecision(
      TxShardWrites& tx);
  // Retains the existing store mutex contract: releases it only while
  // awaiting a prior independent transaction's durable commit, then restores
  // ownership on every return. Caller revalidates key/population afterward.
  Task<absl::Status> AwaitGroupedDependencyLocked(
      WorkerStore& store, const GroupedHashObject::Handle& object,
      std::uint64_t successor_txid);

  // Writes one unpublished complete group snapshot. The receipt prevents its
  // transaction generation from retiring; the caller must either publish it
  // with the root's decision or reclaim it when that batch is abandoned.
  Task<absl::StatusOr<HashGroupLocation>> WriteHashGroupRecordLocked(
      WorkerStore& store, WorkerStore::PartitionStore& partition,
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      const HashGroupSnapshot& snapshot, std::uint64_t sequence,
      TxShardWrites& tx, ValueType value_type = ValueType::kHash,
      std::uint64_t batch_txid = 0);

  Task<absl::StatusOr<LoadedOrderedGroup>> LoadOrderedGroupSnapshot(
      WorkerStore& store, WorkerStore::PartitionStore& partition,
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      GroupedHashObject::Handle object, std::uint64_t id, bool pinned = false);
  Task<absl::StatusOr<std::vector<OrderedCollectionEntry>>>
  LoadGroupedOrderedValue(WorkerStore& store,
                          WorkerStore::PartitionStore& partition,
                          std::uint8_t db_id, std::string_view key,
                          const Digest& digest,
                          GroupedHashObject::Handle object,
                          bool pinned = false);
  Task<absl::StatusOr<HashGroupLocation>> WriteOrderedGroupRecordLocked(
      WorkerStore& store, WorkerStore::PartitionStore& partition,
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      const OrderedGroupSnapshot& snapshot, std::uint64_t revision,
      TxShardWrites& tx, std::uint64_t batch_txid = 0);

  Task<absl::Status> WriteRecordLocked(
      WorkerStore& store, std::uint8_t db_id, std::string_view key,
      std::string_view value, RecordKind kind, ValueType value_type,
      std::uint64_t expire_at_ms, const Digest& digest, std::uint64_t txid,
      std::uint64_t mutation_sequence, bool for_defrag,
      bool unlock_writer_while_waiting = true, bool external = false,
      bool key_external = false,
      std::uint64_t logical_size = std::numeric_limits<std::uint64_t>::max(),
      std::shared_ptr<const std::vector<ExtentRef>> extents = nullptr,
      RecordLocation* written_location = nullptr,
      const RelocationSource* relocation = nullptr, TxShardWrites* tx = nullptr,
      std::unique_ptr<std::vector<RetiredRecord>> commit_retirements = nullptr,
      SetLatencyTrace* trace = nullptr,
      const ExplicitWriteRoot* explicit_root = nullptr,
      TxUndoLog* replacement_undo = nullptr,
      WorkerStore::PartitionStore* known_partition = nullptr,
      const GroupRecordWrite* group = nullptr, bool mark_watched = false,
      const MutationPrecondition* mutation_precondition = nullptr);

  absl::StatusOr<RecordIndex::Entry*> ReplaceIndexLocation(
      WorkerStore& store, RecordIndex& index, RecordIndex::Entry* entry,
      const Digest& digest, const RecordLocation& location,
      TxUndoLog* tx_undo = nullptr);

  void SealActiveBlocks(WorkerStore& store);

  // Make the active block's tail durable without retiring it. The block stays
  // open for appends, so a slow writer no longer burns a whole 8 MiB block per
  // flush interval; it pays at most one padding page instead.
  void FlushActiveBlock(WorkerStore& store);

  void SealDeadActiveBlock(WorkerStore& store);

  Task<absl::Status> FlushWorkerForShutdown(WorkerStore* store);

  Task<absl::Status> BuildShutdownCheckpointShard(WorkerStore& store,
                                                  std::uint64_t generation);

  Task<absl::Status> PersistCheckpointBitmapOnDeviceLocal(
      std::size_t device_index, std::vector<std::uint64_t> block_ids);

  Task<absl::Status> PersistCheckpointBitmap(
      std::span<const std::uint64_t> block_ids);

  Task<absl::Status> PublishShutdownCheckpoint(std::uint64_t generation);

  Task<absl::Status> DiscoverCheckpoint(WorkerStore& store,
                                        CheckpointLoadResult* result);

  Task<absl::Status> PrepareCheckpointIndexes();

  absl::Status PreallocateCheckpointIndexes(WorkerStore& store);

  Task<absl::Status> LoadCheckpoint(WorkerStore& store,
                                    CheckpointLoadResult* result);

  absl::Status ValidateCheckpointIndexSizes(const WorkerStore& store) const;

  void CompleteShutdownFlush(const absl::Status& status);

  void AdvanceExpiryMap(WorkerStore& store);

  // Drops every invalid exact-capability prefix before replication admission.
  // These are cancelled attempts rather than delete work and consume none of
  // the cycle's mutation budget.
  std::size_t DiscardStaleExpirationCandidates(
      WorkerStore& store, std::size_t max_candidates) noexcept;

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

  Task<absl::Status> TombRaiderLoop(WorkerStore* store,
                                    std::uint64_t generation);

  absl::Status ApplyTombRaiderConfig(WorkerStore& coordinator,
                                     TombRaiderConfigUpdate update);

  Task<absl::Status> RunTombRaider();

  bool TombRaiderShouldForfeit() const noexcept {
    return shutdown_flush_requested_.load(std::memory_order_acquire) ||
           tomb_raider_forfeit_requested_.load(std::memory_order_acquire);
  }

  Task<absl::Status> TombMarkLocal(WorkerStore& store);

  Task<absl::Status> TombSweepLocal(WorkerStore& store);

  Task<absl::Status> TombClaimLocal(WorkerStore& store,
                                    std::vector<TombClaim> claims);

  Task<absl::Status> TombReapLocal(WorkerStore& store);

  Task<absl::Status> PeriodicFlush(WorkerStore* store);

  // Called before acquiring a standalone grouped transaction's generation
  // lease, with no store mutex held. A borrowed EXEC lease must never wait for
  // its own generation to become reclaimable. The estimate is only a pressure
  // signal, not a reservation or a second disk-capacity admission policy.
  Task<absl::Status> BeforeGroupedTransaction(WorkerStore& store,
                                              std::uint64_t append_bytes);
  Task<absl::Status> MaybeRunTxCleaner(bool force = false);
  Task<absl::Status> RunTxCleaner(bool shutdown_drain = false);

  // Shutdown has stopped new transaction admission and drained commit chains.
  // Force generation promotion regardless of the online cooldown so a
  // checkpoint never needs to encode transaction-tagged winners.
  Task<absl::Status> DrainTxCleanerForShutdown();
  Task<absl::StatusOr<TxGenerationLocalState>> InspectTxGenerationLocal(
      WorkerStore& store, std::uint64_t generation, bool seal);

  Task<std::vector<std::uint64_t>> ListTxGenerationsLocal(
      WorkerStore& store, std::uint64_t closed_before);

  Task<bool> TxGenerationHasRecordsLocal(WorkerStore& store,
                                         std::uint64_t generation);

  Task<absl::Status> ForgetTxGenerationLocal(WorkerStore& store,
                                             std::uint64_t generation);
  Task<absl::Status> PromoteTxGenerationLocal(
      WorkerStore& store, std::uint64_t generation,
      std::shared_ptr<const absl::flat_hash_set<std::uint64_t>> committed,
      bool shutdown_drain);
  Task<absl::Status> RetireTxGenerationLocal(WorkerStore& store,
                                             std::uint64_t generation);

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

  Task<absl::Status> DefragRecordCheckpoint(WorkerStore& store);

  Task<absl::StatusOr<std::optional<RelocationDurabilityFence>>>
  RelocateIfCurrent(unsigned key_owner, std::string_view key,
                    std::string_view value, const RecordHeader& record,
                    const RecordLocation& source_location,
                    bool clear_txid = false);

  Task<absl::Status> AwaitRelocationDurableLocal(
      WorkerStore& store, const RelocationDurabilityFence& fence);

  Task<absl::Status> AwaitRelocationDurable(
      const RelocationDurabilityFence& fence);

  Task<absl::Status> CleanBlockLocked(WorkerStore& store,
                                      std::uint64_t block_id);

  // Rewrites every record in the block that is still current, so the block ends
  // up with no reachable data and the caller can free it. Clears `defragging`
  // on each failure path so the block stays eligible for a later pass.
  Task<absl::Status> SalvageBlockRecords(
      WorkerStore& store, std::uint64_t block_id, BlockState& source,
      std::uint32_t source_file_id, std::uint64_t source_block_offset,
      std::shared_ptr<const absl::flat_hash_set<std::uint64_t>>
          committed_txids = nullptr);

  // Drains readers and hands the block back to the allocator. The caller must
  // have observed live_bytes == 0 under store_state_mutex and set `freeing`,
  // which stops LoadValueLocal from taking new pins.
  Task<absl::Status> ReleaseEmptyBlock(WorkerStore& store,
                                       std::uint64_t block_id,
                                       BlockState& source);

  StorageEngineOptions options_;
  // CONFIG SET updates the next-shutdown policy without visiting workers.
  // FlushForShutdown snapshots it before publishing shutdown_flush_requested_,
  // so every worker enters the same barrier path for that shutdown.
  std::atomic<bool> shutdown_checkpoint_enabled_{false};
  bool shutdown_checkpoint_for_flush_ = false;
  // One runtime setting shared by all workers. Queue occupancy and waiters
  // remain worker-local; CONFIG SET stores this atomically and then visits each
  // worker only to wake publishers that may now fit under a larger limit.
  std::atomic<std::size_t> replication_publish_queue_bytes_{0};
  // The append path samples this only at an 8 MiB block rollover. CONFIG
  // visits each worker after changing it so a disabled policy wakes sleepers.
  std::atomic<bool> replication_backlog_backpressure_{true};
  unsigned worker_count_ = 0;
  std::uint64_t total_data_blocks_ = 0;
  std::vector<StorageDevice> devices_;
  // Populated only for SPDK: workers owning a qpair for each controller.
  std::vector<std::vector<std::uint16_t>> device_owners_;
  // Which worker owns each block, by device and local block id. A record
  // carries its block's owner in its index entry, but an extent reference has
  // no such field, so this is how a worker holding a manifest finds the worker
  // to dispatch to. Written only by the owner as it allocates or frees a
  // block, read by anyone.
  // One dense array per device, indexed by local block id less the device's
  // data_block_begin. Shared across workers; each entry names its owner and
  // only that worker touches anything but the owner field.
  std::vector<std::vector<BlockState>> device_block_states_;
  // Combines the immutable begin and data pointer used by every point lookup.
  // The owning vectors above are never resized after initialization, so these
  // pointers stay valid for the engine lifetime.
  std::vector<BlockStateLookup> block_state_lookup_;
  std::vector<std::unique_ptr<DeviceAllocator>> device_allocators_;
  std::vector<std::size_t> defrag_reserve_blocks_;
  std::unique_ptr<std::atomic<unsigned>[]> active_defrags_by_device_;
  std::unique_ptr<moodycamel::ConcurrentQueue<std::uint16_t>[]>
      defrag_ready_by_device_;
  std::unique_ptr<RecoveryDeviceCursor[]> recovery_device_cursors_;
  std::vector<std::uint64_t> epoch_values_;
  CheckpointRoot checkpoint_root_{};
  bool checkpoint_root_coherent_ = true;
  std::atomic<bool> checkpoint_active_{false};
  std::vector<CheckpointShardResult> checkpoint_shards_;
  std::vector<CheckpointLoadResult> checkpoint_load_results_;
  std::uint64_t checkpoint_loaded_block_count_ = 0;
  absl::Status checkpoint_tx_cleanup_status_ = absl::OkStatus();
  absl::Status checkpoint_publish_status_ = absl::OkStatus();
  // A successful root publication makes the clean-shutdown snapshot the next
  // process's recovery authority. Only then may the server deliberately leave
  // worker memory to kernel process teardown instead of walking a billion-key
  // index during an otherwise complete shutdown.
  std::atomic<bool> shutdown_checkpoint_published_{false};
  std::atomic<bool> abandon_worker_state_on_finalize_{false};
  std::atomic<bool> epoch_metadata_failed_{false};
  std::atomic<bool> runtime_failure_latched_{false};
  // Cold branch on every logical write. It is set only while this node is
  // destructively rebuilding its single data root; foreground commands are
  // rejected above the engine, while tail commands stamp the pending epochs.
  std::atomic<bool> replica_loading_{false};
  std::atomic<bool> replica_recovery_fenced_{false};
  // Unlike the durable full-sync fence, no in-process transition may clear
  // this flag; a new process must recover the authoritative system state and
  // create a fresh replication history first.
  std::atomic<bool> request_serving_fenced_until_restart_{false};
  // All catalog and promotion updates serialize through worker zero and this
  // mutex. The manifest is always rewritten from the current state so one
  // field update cannot erase the other.
  AsyncMutex system_state_mutex_;
  DurableSystemState system_state_;
  std::string staged_clean_shutdown_proof_;
  std::optional<std::string> recovered_catalog_dump_;
  std::optional<absl::Status> system_state_failure_;
  std::atomic<bool> system_state_root_failure_injected_{false};
  // Tomb Raider retains the pre-existing coarse authority switch. Finite
  // capabilities govern active expiration only and deliberately do not alter
  // Tomb Raider admission, scheduling, or an in-flight cleanup round.
  std::atomic<bool> expiration_authority_{true};
  std::atomic<std::shared_ptr<ExpirationAuthorityGrant>>
      active_expiration_authority_;
#if LAVIK_FAULTS_ENABLED
  // Unit tests use this synchronous hook to revoke an exact grant after the
  // early check without relying on scheduler timing. It is absent from
  // production builds and never supplies production behavior.
  ExpirationTestHook expiration_test_hook_;
#endif
  std::atomic<std::uint32_t> expiration_pause_count_{0};
  // Independent process-wide knobs; no worker cursor or candidate queue is
  // mutated by CONFIG. A cycle keeps its sampled budgets across suspensions.
  std::atomic<std::uint32_t> active_expiration_interval_ms_{10};
  std::atomic<std::uint32_t> active_expiration_map_steps_per_cycle_{256};
  std::atomic<std::uint32_t> active_expiration_deletes_per_cycle_{64};
  std::atomic<std::uint32_t>
      active_expiration_index_maintenance_steps_per_cycle_{256};
  // Background tasks that settle accounting through cross-worker hops
  // (retired-record settlement, detached-index reclaim, a tomb raider
  // round). A frame parked on such a hop is registered with the remote
  // worker; tearing its own worker down under it lets the remote resume a
  // destroyed frame. The shutdown drain waits for this to reach zero, so
  // every one of these tasks must be short-lived or abort promptly once
  // shutdown_flush_requested_ is set.
  std::atomic<std::uint32_t> active_settlements_{0};
  struct alignas(64) DefragRuntimeConfig {
    std::atomic<bool> paused_{false};
    std::atomic<unsigned> max_active_per_device_{1};
    std::atomic<std::uint32_t> block_sleep_ms_{0};
    std::atomic<std::uint32_t> record_sleep_us_{0};
  } defrag_config_;
  struct alignas(64) TombRaiderRuntimeConfig {
    // Even values are stable scheduler generations; odd means a worker-0
    // configuration update is publishing new fields.
    std::atomic<std::uint64_t> generation_{2};
    std::atomic<std::uint64_t> interval_ms_{0};
    std::atomic<std::uint32_t> block_sleep_ms_{0};
    std::atomic<std::uint32_t> daily_second_{0};
    std::atomic<TombRaiderMode> mode_{TombRaiderMode::kOff};
    std::atomic<TombRaiderMode> last_mode_{TombRaiderMode::kInterval};
  } tomb_raider_config_;
  static_assert(sizeof(TombRaiderRuntimeConfig) == 64);
  AsyncNotification tomb_raider_round_finished_;
  std::atomic<bool> tomb_raider_running_{false};
  // Cross-worker phase checkpoints observe this flag. The worker-0 quiesce
  // coordinator keeps it set until the current round has published running
  // false; ordinary TOMBRAIDER OFF never touches it.
  std::atomic<bool> tomb_raider_forfeit_requested_{false};
  // Worker-0-only serialization prevents an enabling CONFIG update from
  // reopening admission while replica quiesce is suspended waiting on a
  // remote phase.
  bool tomb_raider_quiescing_ = false;
  std::atomic<std::uint64_t> tomb_raider_rounds_{0};
  std::atomic<std::uint64_t> tomb_raider_reaped_{0};
  std::atomic<std::uint64_t> tomb_raider_refreshed_{0};
  std::vector<std::unique_ptr<WorkerStore>> stores_;
  std::unique_ptr<CoroutineBarrier> open_barrier_;
  std::unique_ptr<CoroutineBarrier> checkpoint_consumed_barrier_;
  std::unique_ptr<CoroutineBarrier> checkpoint_capacity_loaded_barrier_;
  std::unique_ptr<CoroutineBarrier> checkpoint_capacity_ready_barrier_;
  std::unique_ptr<CoroutineBarrier> checkpoint_indexes_preallocated_barrier_;
  std::unique_ptr<CoroutineBarrier> checkpoint_indexes_ready_barrier_;
  std::unique_ptr<CoroutineBarrier> checkpoint_loaded_barrier_;
  std::unique_ptr<CoroutineBarrier> checkpoint_index_validated_barrier_;
  std::unique_ptr<CoroutineBarrier> metadata_barrier_;
  std::unique_ptr<CoroutineBarrier> checkpoint_retired_barrier_;
  std::unique_ptr<CoroutineBarrier> recovery_barrier_;
  std::unique_ptr<CoroutineBarrier> recovery_accounting_barrier_;
  std::unique_ptr<CoroutineBarrier> free_list_barrier_;
  std::unique_ptr<CoroutineBarrier> orphan_extent_barrier_;
  std::unique_ptr<CoroutineBarrier> shutdown_recovery_ready_barrier_;
  std::unique_ptr<CoroutineBarrier> shutdown_recovery_published_barrier_;
  std::unique_ptr<CoroutineBarrier> shutdown_checkpoint_ready_barrier_;
  std::unique_ptr<CoroutineBarrier> shutdown_checkpoint_tx_cleaned_barrier_;
  std::unique_ptr<CoroutineBarrier> shutdown_checkpoint_refrozen_barrier_;
  std::unique_ptr<CoroutineBarrier> shutdown_checkpoint_built_barrier_;
  std::unique_ptr<CoroutineBarrier> shutdown_checkpoint_published_barrier_;
  std::atomic<std::uint64_t> recovery_scanned_blocks_{0};
  std::atomic<std::uint64_t> recovery_scanned_records_{0};
  std::atomic<std::uint64_t> recovery_max_txid_{0};
  // Cold-path reduction over every scanned record/block. After the recovery
  // barrier it seeds each worker's non-atomic striped LSN sequence.
  std::atomic<std::uint64_t> recovery_max_lsn_{0};
  // Commit chains spawned but not yet finished; graceful shutdown drains
  // them before the final flush so acknowledged multi-key writes do not
  // lose their commit records to the shutdown ordering.
  std::atomic<std::uint64_t> active_tx_commits_{0};
  std::atomic<std::uint64_t> tx_commit_batches_{0};
  std::atomic<std::uint64_t> tx_commit_batch_transactions_{0};
  std::atomic<std::uint64_t> tx_commit_input_fences_{0};
  std::atomic<std::uint64_t> tx_commit_merged_fences_{0};
  std::atomic<std::uint64_t> tx_commit_queue_depth_{0};
  std::atomic<std::uint64_t> tx_commit_queue_peak_{0};
  std::atomic<std::uint64_t> tx_commit_backpressure_waits_{0};
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
  // A structurally invalid checkpoint can leave a validated prefix installed
  // before ordinary recovery takes over. Workers use this flag to materialize
  // winner-order markers for that prefix before scanning record bodies.
  std::atomic<bool> checkpoint_load_fell_back_{false};
  std::int64_t recovery_started_ms_ = 0;
  static constexpr std::size_t kDefragReserveBlocksPerDevice = 8;
  std::array<std::atomic<std::uint64_t>, kLogicalDatabaseCount> db_epochs_{};
  // Runtime translation installed with a promoted replica root. Source DB
  // epochs can be lower than this node's pre-existing local epoch.
  std::array<std::atomic<std::uint64_t>, kLogicalDatabaseCount>
      replica_source_db_epochs_{};
  // Allocates one identity for each cross-flow DB control barrier. It is
  // scoped to the in-memory replication history; a restart changes history
  // id, so it does not need persistence.
  std::atomic<std::uint64_t> next_replication_control_id_{1};
  std::atomic<std::uint64_t> next_replication_ephemeral_id_{1};
  // Replica control rendezvous collapses all source-flow copies before
  // entering storage. Per-DB async gates serialize the resulting epoch
  // installation against another control apply; they are cold-path and not a
  // global mutex.
  std::array<AsyncMutex, kLogicalDatabaseCount> replica_db_epoch_mutexes_;
  std::atomic<unsigned> active_defrags_{0};
  std::atomic<unsigned> pending_defrags_{0};
  std::atomic<unsigned> active_flushes_{0};
  // Extent reclaims in flight (from the moment they are spawned): the block
  // allocator must not report the device full while one may still free space.
  std::atomic<unsigned> active_extent_reclaims_{0};
  std::atomic<std::uint64_t> space_reclaim_generation_{0};
  std::atomic<std::uint64_t> current_tx_generation_{1};
  std::atomic<std::uint32_t> tx_cleaner_cooldown_ms_{60'000};
  std::atomic<std::int64_t> tx_cleaner_next_run_ms_{0};
  std::atomic<bool> tx_cleaner_dirty_{true};
  std::atomic<bool> tx_cleaner_running_{false};
  std::atomic<std::uint64_t> tx_cleaner_rounds_{0};
  std::atomic<std::uint64_t> tx_cleaner_failures_{0};
  std::atomic<std::uint64_t> tx_cleaner_retired_generations_{0};
  std::atomic<std::uint64_t> tx_cleaner_retired_blocks_{0};
  std::atomic<bool> shutdown_flush_requested_{false};
  std::atomic<unsigned> shutdown_flush_completed_{0};
  std::atomic<bool> shutdown_flush_failed_{false};
};

}  // namespace lavik::storage
