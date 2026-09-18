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

#include "impl.h"

namespace lavik::storage {
namespace {

struct SnapshotReadGuard {
  explicit SnapshotReadGuard(std::uint32_t* readers) : readers_(readers) {
    ++*readers_;
  }
  ~SnapshotReadGuard() { --*readers_; }
  std::uint32_t* readers_;
};

absl::StatusOr<std::uint64_t> AllocateCollectionToken() {
  // Process-wide identity also rejects an accidental token handoff to the
  // wrong worker. Exhaustion fails closed instead of reusing a live token.
  static std::atomic<std::uint64_t> next{1};
  auto value = next.load(std::memory_order_relaxed);
  while (value != UINT64_MAX) {
    if (next.compare_exchange_weak(value, value + 1, std::memory_order_relaxed))
      return value;
  }
  return absl::ResourceExhaustedError("RDB collection token space exhausted");
}

std::string SnapshotMapKey(std::uint8_t db_id, std::string_view key) {
  std::string result;
  result.reserve(key.size() + 1);
  result.push_back(static_cast<char>(db_id));
  result.append(key);
  return result;
}

template <typename Map>
std::optional<MemoryReservation> ReserveSnapshotMapInsert(
    Map& map, const Digest& digest, std::string_view key) noexcept {
  if (!map.CanAllocateEntry(key, /*key_complete=*/true,
                            /*has_extra=*/false)) {
    return std::nullopt;
  }
  const std::size_t required = map.RequiredAllocationBytes(
      digest, key, /*key_complete=*/true, /*has_extra=*/false,
      /*inserting=*/true);
  if (required == std::numeric_limits<std::size_t>::max()) {
    return std::nullopt;
  }
  return TryReserveMemory(required);
}

// The caller captures this list synchronously while the side view is current.
// Holding its shared metadata after suspension does not preserve a compact
// child's omitted physical allocation epoch. A successful pin later verifies
// precisely these identities, and unpin never needs a fresh allocation.
template <typename SnapshotValue, typename Materialize>
absl::Status PrepareSnapshotBlockPins(SnapshotValue* value,
                                      Materialize materialize) {
  using BlockPin = typename SnapshotValue::BlockPin;
  using BlockPins = typename SnapshotValue::BlockPins;
  std::size_t count = 1;
  bool overflow = false;
  auto add_count = [&](std::size_t added) {
    if (added > std::numeric_limits<std::size_t>::max() - count)
      overflow = true;
    else
      count += added;
  };
  if (value->extents_ != nullptr) add_count(value->extents_->size());
  if (value->grouped_ != nullptr) {
    value->grouped_->ForEachRecord(
        [&](HashGroupId, const RecordIndex::Entry&,
            const std::shared_ptr<const std::vector<ExtentRef>>& extents,
            bool) {
          add_count(1);
          if (extents != nullptr) add_count(extents->size());
        });
  }
  constexpr std::size_t overhead = sizeof(BlockPins) + 4 * sizeof(void*);
  if (overflow || count > (std::numeric_limits<std::size_t>::max() - overhead) /
                              sizeof(BlockPin)) {
    return absl::ResourceExhaustedError("RDB snapshot pin list is too large");
  }
  auto reservation = TryReserveMemory(overhead + count * sizeof(BlockPin));
  if (!reservation.has_value()) {
    RecordMemoryRejection();
    return absl::ResourceExhaustedError(
        "OOM RDB snapshot pin list admission failed");
  }
  auto pins = std::make_shared<BlockPins>();
  pins->blocks_.reserve(count);
  auto add_record = [&](const RecordLocation& location) {
    pins->blocks_.push_back(
        BlockPin{location.block_id(), location.allocation_epoch(), false});
  };
  auto add_extents = [&](const ExtentManifest& extents) {
    if (extents == nullptr) return;
    for (const auto& extent : *extents)
      pins->blocks_.push_back(
          BlockPin{extent.block_id_, extent.allocation_epoch_, true});
  };
  add_record(value->location_);
  add_extents(value->extents_);
  if (value->grouped_ != nullptr) {
    value->grouped_->ForEachRecord(
        [&](HashGroupId, const RecordIndex::Entry& entry,
            const std::shared_ptr<const std::vector<ExtentRef>>& extents,
            bool) {
          add_record(materialize(entry));
          add_extents(extents);
        });
  }
  auto& blocks = pins->blocks_;
  std::sort(blocks.begin(), blocks.end(),
            [](const BlockPin& a, const BlockPin& b) {
              if (a.block_id_ != b.block_id_) return a.block_id_ < b.block_id_;
              return a.allocation_epoch_ < b.allocation_epoch_;
            });
  for (std::size_t i = 1; i < blocks.size(); ++i) {
    if (blocks[i - 1].block_id_ == blocks[i].block_id_ &&
        (blocks[i - 1].allocation_epoch_ != blocks[i].allocation_epoch_ ||
         blocks[i - 1].extent_ != blocks[i].extent_)) {
      return absl::DataLossError(
          "RDB snapshot graph has conflicting block identities");
    }
  }
  blocks.erase(std::unique(blocks.begin(), blocks.end(),
                           [](const BlockPin& a, const BlockPin& b) {
                             return a.block_id_ == b.block_id_ &&
                                    a.allocation_epoch_ == b.allocation_epoch_;
                           }),
               blocks.end());
  pins->charge_.Adopt(&*reservation,
                      overhead + blocks.capacity() * sizeof(BlockPin));
  value->block_pins_ = std::move(pins);
  return absl::OkStatus();
}

}  // namespace

absl::Status StorageEngine::Impl::PrepareGroupedSnapshotPins(
    WorkerStore::PartitionStore::RdbSnapshotValue* value) {
  return PrepareSnapshotBlockPins(value,
                                  [this](const RecordIndex::Entry& entry) {
                                    return MaterializeIndexLocation(entry);
                                  });
}

absl::Status StorageEngine::Impl::BeginRdbSnapshot(
    std::uint64_t session_id, std::uint64_t snapshot_time_ms) {
  if (session_id == 0 || snapshot_time_ms == 0) {
    return absl::InvalidArgumentError("invalid RDB snapshot cut");
  }
  WorkerStore& store = CurrentStore();
  if (store.rdb_snapshot_.has_value()) {
    return absl::FailedPreconditionError(
        "an RDB snapshot is already active on this worker");
  }
  store.rdb_snapshot_.emplace(WorkerStore::RdbSnapshotSession{
      .id_ = session_id,
      .snapshot_time_ms_ = snapshot_time_ms,
      .invalidated_ = false,
      .ending_ = false,
      .readers_ = 0,
      .collection_ = nullptr,
  });
  // Dirty keys from every partition share one worker-local arena. Its
  // allocation domain relies on the explicit per-insert reservation below;
  // a successful reservation makes all subsequent physical growth a
  // fail-fast allocator boundary rather than an exception-based admission
  // decision.
  auto dirty_key_arena = std::make_shared<ScanHashMapEntryArena>(
      ScanHashMapEntryArena::kMaximumPageId,
      /*externally_admitted=*/true);
  for (auto& partition : store.partitions_) {
    auto& capture = partition.rdb_snapshot_.emplace(
        WorkerStore::PartitionStore::RdbSnapshotCapture{
            .session_id_ = session_id,
            .cut_sequence_ = partition.mutation_sequence_,
            .snapshot_time_ms_ = snapshot_time_ms,
            .dirty_keys_ = {},
        });
    capture.dirty_keys_.SetEntryArena(dirty_key_arena);
  }
  return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::PinRdbSnapshotValue(
    WorkerStore::PartitionStore::RdbSnapshotValue* value) {
  if (value == nullptr || value->pins_held_) {
    co_return value == nullptr
        ? absl::InvalidArgumentError("missing RDB snapshot value")
        : absl::OkStatus();
  }

  using BlockPin = WorkerStore::PartitionStore::RdbSnapshotValue::BlockPin;
  if (value->block_pins_ == nullptr)
    co_return absl::InternalError(
        "RDB snapshot has no captured physical pin list");
  const auto& blocks = value->block_pins_->blocks_;

  // Keep same-worker and cross-worker suspensions in separate statements.
  // GCC 13 can reuse the wrong coroutine-frame slot for co_await in both ?:
  // arms.
  auto pin_one = [this](BlockPin pin) -> Task<absl::Status> {
    const unsigned owner = BlockOwner(pin.block_id_);
    if (owner >= worker_count_) {
      co_return absl::AbortedError("RDB snapshot block has no owner");
    }
    auto on_owner = [this, owner, pin]() -> Task<absl::Status> {
      WorkerStore& block_store = *stores_[owner];
      co_await block_store.store_state_mutex_.Lock();
      UnlockGuard unlock(&block_store.store_state_mutex_, block_store.worker_);
      BlockState* state = FindBlockState(block_store, pin.block_id_);
      if (state == nullptr || !state->allocated_ || state->freeing_ ||
          state->defragging_ ||
          state->allocation_epoch_ != pin.allocation_epoch_ ||
          (pin.extent_ && state->kind_ != BlockKind::kPayloadExtent) ||
          (!pin.extent_ && state->kind_ == BlockKind::kPayloadExtent) ||
          state->pins_ == std::numeric_limits<std::uint32_t>::max()) {
        co_return absl::AbortedError("stale RDB snapshot block");
      }
      ++state->pins_;
      co_return absl::OkStatus();
    };
    absl::Status status;
    if (owner == bycorf::ThisWorker().id_) {
      status = co_await on_owner();
    } else {
      status = co_await bycorf::SubmitTaskTo(owner, on_owner);
    }
    co_return status;
  };

  auto release_one = [this](BlockPin pin) -> Task<absl::Status> {
    const unsigned owner = BlockOwner(pin.block_id_);
    if (owner >= worker_count_) co_return absl::OkStatus();
    auto on_owner = [this, owner, pin]() -> Task<absl::Status> {
      WorkerStore& block_store = *stores_[owner];
      co_await block_store.store_state_mutex_.Lock();
      UnlockGuard unlock(&block_store.store_state_mutex_, block_store.worker_);
      BlockState* state = FindBlockState(block_store, pin.block_id_);
      if (state != nullptr && state->allocated_ &&
          state->allocation_epoch_ == pin.allocation_epoch_ &&
          state->pins_ != 0) {
        --state->pins_;
        if (state->pins_ == 0 && state->release_pending_) {
          ReleaseStagingBuffer(block_store, *state);
        }
      }
      co_return absl::OkStatus();
    };
    absl::Status status;
    if (owner == bycorf::ThisWorker().id_) {
      status = co_await on_owner();
    } else {
      status = co_await bycorf::SubmitTaskTo(owner, on_owner);
    }
    co_return status;
  };

  std::size_t pinned = 0;
  for (; pinned < blocks.size(); ++pinned) {
    absl::Status status = co_await pin_one(blocks[pinned]);
    if (!status.ok()) {
      while (pinned != 0) {
        (void)co_await release_one(blocks[--pinned]);
      }
      co_return status;
    }
  }
  value->pins_held_ = true;
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::ReleaseRdbSnapshotValue(
    WorkerStore::PartitionStore::RdbSnapshotValue* value) {
  if (value == nullptr || !value->pins_held_) co_return absl::OkStatus();
  using BlockPin = WorkerStore::PartitionStore::RdbSnapshotValue::BlockPin;
  // Retain one shared reference until all owner hops finish. The snapshot map
  // may release its copy afterwards without changing this exact pin set.
  const auto pins = value->block_pins_;
  if (pins == nullptr)
    co_return absl::InternalError("pinned RDB snapshot lost its pin list");
  value->pins_held_ = false;
  for (const BlockPin pin : pins->blocks_) {
    const unsigned owner = BlockOwner(pin.block_id_);
    if (owner >= worker_count_) continue;
    auto on_owner = [this, owner, pin]() -> Task<absl::Status> {
      WorkerStore& block_store = *stores_[owner];
      co_await block_store.store_state_mutex_.Lock();
      UnlockGuard unlock(&block_store.store_state_mutex_, block_store.worker_);
      BlockState* state = FindBlockState(block_store, pin.block_id_);
      if (state != nullptr && state->allocated_ &&
          state->allocation_epoch_ == pin.allocation_epoch_ &&
          state->pins_ != 0) {
        --state->pins_;
        if (state->pins_ == 0 && state->release_pending_) {
          ReleaseStagingBuffer(block_store, *state);
        }
      }
      co_return absl::OkStatus();
    };
    // Preserve the GCC 13 coroutine-frame invariant from the pin path above.
    absl::Status released;
    if (owner == bycorf::ThisWorker().id_) {
      released = co_await on_owner();
    } else {
      released = co_await bycorf::SubmitTaskTo(owner, on_owner);
    }
    if (!released.ok()) co_return released;
  }
  value->block_pins_.reset();
  value->grouped_.reset();
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::CaptureRdbSnapshotBeforeWriteLocked(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest) {
  using SnapshotValue = WorkerStore::PartitionStore::RdbSnapshotValue;
  using Phase = SnapshotValue::Phase;
  auto* capture = partition.rdb_snapshot_ ? &*partition.rdb_snapshot_ : nullptr;
  if (capture == nullptr || !capture->accepting_ ||
      store.rdb_snapshot_ == std::nullopt ||
      store.rdb_snapshot_->id_ != capture->session_id_ ||
      store.rdb_snapshot_->invalidated_) {
    co_return absl::OkStatus();
  }
  const std::string map_key = SnapshotMapKey(db_id, key);
  const Digest map_digest = ComputeDigest(map_key);
  if (capture->dirty_keys_.Find(map_digest, map_key) != nullptr) {
    co_return absl::OkStatus();
  }

  const std::uint64_t session_id = capture->session_id_;
  auto* const admitted_capture = capture;
  ++capture->capture_admissions_;
  auto& index = partition.indexes_[db_id];
  while (true) {
    auto resolved = co_await FindVerifiedEntry(store, index, digest, key);
    if (!resolved.ok()) {
      store.rdb_snapshot_->invalidated_ = true;
      --capture->capture_admissions_;
      co_return absl::OkStatus();
    }
    RecordIndex::Entry* current = *resolved;
    if (current == nullptr || current->value_.kind() != RecordKind::kValue ||
        current->value_.mutation_sequence_ > capture->cut_sequence_ ||
        IsExpired(*current, capture->snapshot_time_ms_)) {
      auto reservation =
          ReserveSnapshotMapInsert(capture->dirty_keys_, map_digest, map_key);
      if (!reservation.has_value()) {
        // The write has already entered snapshot capture and must remain
        // available to make progress near maxmemory. Losing the ABSENT marker
        // makes this cut unusable, so invalidate only the RDB session and let
        // the foreground mutation continue.
        RecordMemoryRejection();
        if (store.rdb_snapshot_ && store.rdb_snapshot_->id_ == session_id) {
          store.rdb_snapshot_->invalidated_ = true;
        }
      } else {
        if (capture->dirty_keys_.InsertNew(map_digest, map_key,
                                           SnapshotValue{
                                               .location_ = {},
                                               .extents_ = nullptr,
                                               .grouped_ = nullptr,
                                               .block_pins_ = nullptr,
                                               .phase_ = Phase::kAbsent,
                                           }) == nullptr) {
          store.rdb_snapshot_->invalidated_ = true;
        }
        reservation->Release();
      }
      --capture->capture_admissions_;
      co_return absl::OkStatus();
    }

    SnapshotValue old{
        .location_ = MaterializeIndexLocation(*current),
        .extents_ = ExtentsFor(store, current),
        .grouped_ = nullptr,
        .block_pins_ = nullptr,
        .phase_ = Phase::kOldValue,
    };
    if (old.location_.grouped()) {
      auto grouped = partition.grouped_objects_[db_id].Lookup(
          key, GroupedObjectVersion{
                   .root_ = old.location_,
                   .db_epoch_ = DbEpoch(db_id),
                   .replication_epoch_ = partition.replication_epoch_,
                   .index_generation_ = partition.grouped_generations_[db_id],
               });
      if (!grouped.ok()) {
        store.rdb_snapshot_->invalidated_ = true;
        --capture->capture_admissions_;
        co_return absl::OkStatus();
      }
      old.grouped_ = std::move(*grouped);
    }
    auto prepared =
        PrepareSnapshotBlockPins(&old, [this](const RecordIndex::Entry& entry) {
          return MaterializeIndexLocation(entry);
        });
    if (!prepared.ok()) {
      store.rdb_snapshot_->invalidated_ = true;
      --capture->capture_admissions_;
      co_return absl::OkStatus();
    }
    store.store_state_mutex_.Unlock(*store.worker_);
    LAVIK_FAULT_INJECT(
        // Deterministic regression hook for an append-stream rollover while
        // this writer has released the store lock to pin the pre-cut value.
        static std::atomic<bool> pause_claimed = false;
        const char* pause_text = std::getenv("LAVIK_RDB_CAPTURE_PAUSE_MS");
        bool expected_pause = false;
        if (pause_text != nullptr &&
            pause_claimed.compare_exchange_strong(expected_pause, true)) {
          char* end = nullptr;
          const unsigned long pause_ms = std::strtoul(pause_text, &end, 10);
          if (end != pause_text && *end == '\0' && pause_ms != 0) {
            (void)co_await bycorf::SleepFor(
                *store.worker_, std::chrono::milliseconds(pause_ms));
          }
        });
    absl::Status pinned = co_await PinRdbSnapshotValue(&old);
    co_await store.store_state_mutex_.Lock();

    // Partition finalization waits for every admitted writer, so this capture
    // cannot be replaced while the mutex is released for cross-worker pins.
    assert(partition.rdb_snapshot_.has_value());
    assert(&*partition.rdb_snapshot_ == admitted_capture);
    assert(admitted_capture->session_id_ == session_id);
    capture = admitted_capture;
    if (!pinned.ok()) {
      if (absl::IsAborted(pinned)) {
        // Defrag may have claimed or relocated the physical record while the
        // store mutex was released for cross-worker pinning. The key lock
        // still protects the logical value, so resolve its new location and
        // retry instead of invalidating the whole snapshot.
        continue;
      }
      if (store.rdb_snapshot_ && store.rdb_snapshot_->id_ == session_id) {
        store.rdb_snapshot_->invalidated_ = true;
      }
      assert(capture->capture_admissions_ != 0);
      --capture->capture_admissions_;
      co_return absl::OkStatus();
    }
    if (!capture->accepting_ || !store.rdb_snapshot_ ||
        store.rdb_snapshot_->invalidated_ ||
        capture->dirty_keys_.Find(map_digest, map_key) != nullptr) {
      store.store_state_mutex_.Unlock(*store.worker_);
      (void)co_await ReleaseRdbSnapshotValue(&old);
      co_await store.store_state_mutex_.Lock();
      assert(capture->capture_admissions_ != 0);
      --capture->capture_admissions_;
      co_return absl::OkStatus();
    }

    resolved = co_await FindVerifiedEntry(store, index, digest, key);
    if (!resolved.ok()) {
      store.rdb_snapshot_->invalidated_ = true;
      store.store_state_mutex_.Unlock(*store.worker_);
      (void)co_await ReleaseRdbSnapshotValue(&old);
      co_await store.store_state_mutex_.Lock();
      assert(capture->capture_admissions_ != 0);
      --capture->capture_admissions_;
      co_return absl::OkStatus();
    }
    current = *resolved;
    if (current != nullptr &&
        MaterializeIndexLocation(*current).SamePhysicalRecord(old.location_)) {
      auto reservation =
          ReserveSnapshotMapInsert(capture->dirty_keys_, map_digest, map_key);
      if (!reservation.has_value()) {
        // No map entry took ownership of the pin. Release it before settling
        // capture_admissions_; EndRdbSnapshot waits on that count before it
        // can destroy the partition capture.
        RecordMemoryRejection();
        if (store.rdb_snapshot_ && store.rdb_snapshot_->id_ == session_id) {
          store.rdb_snapshot_->invalidated_ = true;
        }
        store.store_state_mutex_.Unlock(*store.worker_);
        (void)co_await ReleaseRdbSnapshotValue(&old);
        co_await store.store_state_mutex_.Lock();
      } else {
        if (capture->dirty_keys_.InsertNew(map_digest, map_key, old) ==
            nullptr) {
          store.rdb_snapshot_->invalidated_ = true;
          store.store_state_mutex_.Unlock(*store.worker_);
          (void)co_await ReleaseRdbSnapshotValue(&old);
          co_await store.store_state_mutex_.Lock();
        }
        reservation->Release();
      }
      assert(capture->capture_admissions_ != 0);
      --capture->capture_admissions_;
      co_return absl::OkStatus();
    }

    // A defrag relocation can race the cross-worker pins. The logical writer
    // still holds the key lock, so release the stale physical version and try
    // the relocated record without losing the cut value.
    store.store_state_mutex_.Unlock(*store.worker_);
    (void)co_await ReleaseRdbSnapshotValue(&old);
    co_await store.store_state_mutex_.Lock();
  }
}

Task<absl::StatusOr<std::optional<RdbSnapshotValue>>>
StorageEngine::Impl::MaterializeRdbSnapshotKey(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint64_t session_id, std::uint8_t db_id, std::string key) {
  using SavedValue = WorkerStore::PartitionStore::RdbSnapshotValue;
  using Phase = SavedValue::Phase;
  const Digest digest = ComputeDigest(key);
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest), tx::LockMode::kShared);
  auto* capture = partition.rdb_snapshot_ ? &*partition.rdb_snapshot_ : nullptr;
  if (capture == nullptr || capture->session_id_ != session_id ||
      !store.rdb_snapshot_ || store.rdb_snapshot_->id_ != session_id ||
      store.rdb_snapshot_->invalidated_) {
    co_return absl::FailedPreconditionError("RDB snapshot is not active");
  }

  const std::string map_key = SnapshotMapKey(db_id, key);
  const Digest map_digest = ComputeDigest(map_key);
  auto* saved = capture->dirty_keys_.Find(map_digest, map_key);
  if (saved != nullptr && (saved->value_.phase_ == Phase::kAbsent ||
                           saved->value_.phase_ == Phase::kDone ||
                           saved->value_.phase_ == Phase::kInflight)) {
    if (saved->value_.phase_ == Phase::kAbsent) {
      saved->value_.phase_ = Phase::kDone;
    }
    co_return std::optional<RdbSnapshotValue>{};
  }

  if (saved == nullptr) {
    auto& index = partition.indexes_[db_id];
    while (true) {
      auto resolved = co_await FindVerifiedEntry(store, index, digest, key);
      if (!resolved.ok()) {
        store.rdb_snapshot_->invalidated_ = true;
        co_return resolved.status();
      }
      RecordIndex::Entry* current = *resolved;
      if (current == nullptr || current->value_.kind() != RecordKind::kValue ||
          IsExpired(*current, capture->snapshot_time_ms_)) {
        auto reservation =
            ReserveSnapshotMapInsert(capture->dirty_keys_, map_digest, map_key);
        if (!reservation.has_value()) {
          RecordMemoryRejection();
          store.rdb_snapshot_->invalidated_ = true;
          co_return absl::ResourceExhaustedError(
              "RDB snapshot dirty-key allocation failed");
        }
        if (capture->dirty_keys_.InsertNew(map_digest, map_key,
                                           SavedValue{
                                               .location_ = {},
                                               .extents_ = nullptr,
                                               .grouped_ = nullptr,
                                               .block_pins_ = nullptr,
                                               .phase_ = Phase::kDone,
                                           }) == nullptr) {
          store.rdb_snapshot_->invalidated_ = true;
          co_return absl::ResourceExhaustedError(
              "RDB snapshot dirty-key capacity exhausted");
        }
        reservation->Release();
        co_return std::optional<RdbSnapshotValue>{};
      }
      if (current->value_.mutation_sequence_ > capture->cut_sequence_) {
        store.rdb_snapshot_->invalidated_ = true;
        co_return absl::InternalError(
            "post-cut RDB key has no ABSENT/old-value capture");
      }
      SavedValue candidate{
          .location_ = MaterializeIndexLocation(*current),
          .extents_ = ExtentsFor(store, current),
          .grouped_ = nullptr,
          .block_pins_ = nullptr,
          .phase_ = Phase::kInflight,
      };
      if (candidate.location_.grouped()) {
        auto grouped = partition.grouped_objects_[db_id].Lookup(
            key, GroupedObjectVersion{
                     .root_ = candidate.location_,
                     .db_epoch_ = DbEpoch(db_id),
                     .replication_epoch_ = partition.replication_epoch_,
                     .index_generation_ = partition.grouped_generations_[db_id],
                 });
        if (!grouped.ok()) {
          store.rdb_snapshot_->invalidated_ = true;
          co_return grouped.status();
        }
        candidate.grouped_ = std::move(*grouped);
      }
      auto prepared = PrepareSnapshotBlockPins(
          &candidate, [this](const RecordIndex::Entry& entry) {
            return MaterializeIndexLocation(entry);
          });
      if (!prepared.ok()) {
        store.rdb_snapshot_->invalidated_ = true;
        co_return prepared;
      }
      auto reservation =
          ReserveSnapshotMapInsert(capture->dirty_keys_, map_digest, map_key);
      if (!reservation.has_value()) {
        RecordMemoryRejection();
        store.rdb_snapshot_->invalidated_ = true;
        co_return absl::ResourceExhaustedError(
            "RDB snapshot dirty-key allocation failed");
      }
      saved = capture->dirty_keys_.InsertNew(map_digest, map_key, candidate);
      if (saved == nullptr) {
        store.rdb_snapshot_->invalidated_ = true;
        co_return absl::ResourceExhaustedError(
            "RDB snapshot dirty-key capacity exhausted");
      }
      reservation->Release();
      absl::Status pinned = co_await PinRdbSnapshotValue(&saved->value_);
      if (!pinned.ok()) {
        capture->dirty_keys_.Erase(saved);
        // Only a concurrent physical relocation is retryable. Retrying a
        // persistent invalid graph or allocation failure would spin forever.
        if (!absl::IsAborted(pinned)) {
          store.rdb_snapshot_->invalidated_ = true;
          co_return pinned;
        }
        continue;
      }
      resolved = co_await FindVerifiedEntry(store, index, digest, key);
      if (!resolved.ok()) {
        store.rdb_snapshot_->invalidated_ = true;
        (void)co_await ReleaseRdbSnapshotValue(&saved->value_);
        co_return resolved.status();
      }
      current = *resolved;
      if (current != nullptr &&
          MaterializeIndexLocation(*current).SamePhysicalRecord(
              saved->value_.location_)) {
        break;
      }
      (void)co_await ReleaseRdbSnapshotValue(&saved->value_);
      capture->dirty_keys_.Erase(saved);
      saved = nullptr;
    }
  } else {
    assert(saved->value_.phase_ == Phase::kOldValue);
    assert(saved->value_.pins_held_);
    saved->value_.phase_ = Phase::kInflight;
  }

  const SavedValue physical = saved->value_;
  if (physical.grouped_ != nullptr) {
    // The dirty-map entry remains Inflight until Finish/End. Its immutable
    // view and exact graph pins, not the current key index, own every page
    // read while a filesystem queue may suspend the producer.
    using Stream = WorkerStore::RdbCollectionReadState;
    auto reservation = TryReserveMemory(sizeof(Stream) + key.size() + 64);
    auto token = AllocateCollectionToken();
    if (!reservation || !token.ok()) {
      store.rdb_snapshot_->invalidated_ = true;
      (void)co_await ReleaseRdbSnapshotValue(&saved->value_);
      if (!token.ok()) co_return token.status();
      RecordMemoryRejection();
      co_return absl::ResourceExhaustedError(
          "OOM RDB collection stream admission failed");
    }
    auto stream = std::make_unique<Stream>();
    stream->partition_ = &partition;
    stream->saved_ = &saved->value_;
    stream->key_ = key;
    stream->token_ = *token;
    stream->db_id_ = db_id;
    if (!physical.grouped_->is_ordered())
      stream->hash_cursor_ = physical.grouped_->directory().groups().begin();
    stream->charge_.Adopt(&*reservation,
                          sizeof(Stream) + stream->key_.capacity() + 1);
    store.rdb_snapshot_->collection_ = std::move(stream);
    co_return std::optional<RdbSnapshotValue>(RdbSnapshotValue{
        .db_id_ = db_id,
        .key_ = std::move(key),
        .value_ =
            RawValue{
                .encoded_ = {},
                .logical_size_ = physical.location_.logical_size_,
                .expire_at_ms_ = physical.location_.expire_at_ms_,
                .value_type_ = physical.location_.value_type(),
            },
        .collection_token_ = *token,
    });
  }
  auto loaded = co_await LoadValue(store, partition, db_id, key, digest,
                                   physical.location_, physical.extents_,
                                   nullptr, physical.grouped_);
  if (!loaded.ok()) {
    store.rdb_snapshot_->invalidated_ = true;
    (void)co_await ReleaseRdbSnapshotValue(&saved->value_);
    co_return loaded.status();
  }
  const std::span<const std::byte> bytes = loaded->value();
  RawValue raw{
      .encoded_ = std::string(reinterpret_cast<const char*>(bytes.data()),
                              bytes.size()),
      .logical_size_ = physical.location_.logical_size_,
      .expire_at_ms_ = physical.location_.expire_at_ms_,
      .value_type_ = physical.location_.value_type(),
  };
  absl::Status released = co_await ReleaseRdbSnapshotValue(&saved->value_);
  if (!released.ok()) {
    store.rdb_snapshot_->invalidated_ = true;
    co_return released;
  }
  saved->value_.phase_ = Phase::kDone;
  co_return std::optional<RdbSnapshotValue>(RdbSnapshotValue{
      .db_id_ = db_id,
      .key_ = std::move(key),
      .value_ = std::move(raw),
  });
}

Task<absl::StatusOr<RdbSnapshotBatch>>
StorageEngine::Impl::ReadRdbSnapshotBatch(std::uint64_t session_id,
                                          RdbSnapshotCursor cursor,
                                          std::size_t count,
                                          std::size_t max_bytes) {
  if (session_id == 0 || count == 0 || max_bytes == 0) {
    co_return absl::InvalidArgumentError("invalid RDB snapshot batch request");
  }
  WorkerStore& store = CurrentStore();
  try {
    LAVIK_FAULT_INJECT(
        // Deterministic snapshot tests mutate or detach a grouped key after the
        // global cut but before its first physical read. Pause no captured
        // pointer: the normal session validation below must still reject
        // cancellation while this coroutine was suspended. Production builds
        // omit the environment hook.
        if (cursor.partition_index_ == 0 && cursor.db_id_ == 0 &&
            cursor.index_cursor_ == 0 && cursor.dirty_cursor_ == 0 &&
            !cursor.finalizing_) {
          if (const char* configured = std::getenv("LAVIK_RDB_SCAN_PAUSE_MS");
              configured != nullptr) {
            std::uint32_t delay_ms = 0;
            const char* end = configured + std::strlen(configured);
            const auto parsed = std::from_chars(configured, end, delay_ms);
            if (parsed.ec == std::errc{} && parsed.ptr == end &&
                delay_ms != 0 && delay_ms <= 10000) {
              const absl::Status delayed = co_await bycorf::SleepFor(
                  *store.worker_, std::chrono::milliseconds(delay_ms));
              if (!delayed.ok()) co_return delayed;
            }
          }
        });
    if (!store.rdb_snapshot_ || store.rdb_snapshot_->id_ != session_id ||
        store.rdb_snapshot_->invalidated_) {
      co_return absl::FailedPreconditionError(
          "RDB snapshot was invalidated or ended");
    }
    if (store.rdb_snapshot_->collection_ || store.rdb_snapshot_->readers_ != 0)
      co_return absl::FailedPreconditionError(
          "finish the outstanding RDB stream before scanning");
    SnapshotReadGuard read_guard(&store.rdb_snapshot_->readers_);
    // A batch is allowed to contain fewer values than requested. Keeping one
    // outstanding key avoids retaining multiple graphs and makes cancellation
    // and file-entry output ownership bounded independently of key count.
    count = 1;

    RdbSnapshotBatch result{
        .cursor_ = cursor,
        .values_ = {},
    };
    std::size_t logical_bytes = 0;
    std::size_t partitions_visited = 0;
    constexpr std::size_t kMaxEmptyPartitionsPerBatch = 64;
    while (result.values_.size() < count && logical_bytes < max_bytes &&
           result.cursor_.partition_index_ < store.partitions_.size()) {
      auto& partition = store.partitions_[result.cursor_.partition_index_];
      auto* capture =
          partition.rdb_snapshot_ ? &*partition.rdb_snapshot_ : nullptr;
      if (capture == nullptr || capture->session_id_ != session_id) {
        co_return absl::FailedPreconditionError(
            "RDB partition snapshot is not active");
      }

      if (!result.cursor_.finalizing_) {
        if (result.cursor_.db_id_ < options_.database_count_) {
          const std::size_t remaining = count - result.values_.size();
          const auto scan_start = result.cursor_.index_cursor_;
          auto scanned = co_await ScanPartition(
              partition.id_, result.cursor_.db_id_,
              result.cursor_.index_cursor_, std::max<std::size_t>(remaining, 1),
              capture->snapshot_time_ms_, max_bytes - logical_bytes);
          if (!scanned.ok()) co_return scanned.status();
          result.cursor_.index_cursor_ = scanned->cursor_;
          for (std::string& key : scanned->keys_) {
            auto value = co_await MaterializeRdbSnapshotKey(
                store, partition, session_id, result.cursor_.db_id_,
                std::move(key));
            if (!value.ok()) co_return value.status();
            if (value->has_value()) {
              logical_bytes += (**value).key_.size();
              logical_bytes += (**value).value_.encoded_.size();
              result.values_.push_back(std::move(**value));
              // RecordIndex::Scan visits a whole bucket and may exceed count.
              // Resume that bucket on the next batch; the dirty-map Done or
              // Inflight markers skip already emitted keys. Advancing past it
              // here would silently lose its other keys, while materializing
              // them all would violate the one-stream-per-worker contract.
              result.cursor_.index_cursor_ = scan_start;
              break;
            }
          }
          if (!result.values_.empty()) break;
          if (result.cursor_.index_cursor_ == 0) {
            ++result.cursor_.db_id_;
          }
          continue;
        }
        capture->accepting_ = false;
        result.cursor_.finalizing_ = true;
        result.cursor_.dirty_cursor_ = 0;
      }

      if (capture->capture_admissions_ != 0) {
        absl::Status yielded = co_await bycorf::SleepFor(
            *store.worker_, std::chrono::milliseconds(1));
        if (!yielded.ok()) co_return yielded;
        continue;
      }

      struct DirtyKey {
        std::uint8_t db_id_ = 0;
        std::string key_;
      };
      std::vector<DirtyKey> old_keys;
      const std::size_t remaining = count - result.values_.size();
      result.cursor_.dirty_cursor_ = capture->dirty_keys_.Scan(
          result.cursor_.dirty_cursor_, [&](auto& entry) {
            if (entry.value_.phase_ ==
                WorkerStore::PartitionStore::RdbSnapshotValue::Phase::kAbsent) {
              entry.value_.phase_ =
                  WorkerStore::PartitionStore::RdbSnapshotValue::Phase::kDone;
            } else if (entry.value_.phase_ ==
                           WorkerStore::PartitionStore::RdbSnapshotValue::
                               Phase::kOldValue &&
                       old_keys.size() < remaining) {
              const std::string_view composite = entry.key();
              if (!composite.empty()) {
                old_keys.push_back(DirtyKey{
                    .db_id_ = static_cast<std::uint8_t>(composite.front()),
                    .key_ = std::string(composite.substr(1)),
                });
              }
            }
          });
      for (DirtyKey& key : old_keys) {
        auto value = co_await MaterializeRdbSnapshotKey(
            store, partition, session_id, key.db_id_, std::move(key.key_));
        if (!value.ok()) co_return value.status();
        if (value->has_value()) {
          logical_bytes += (**value).key_.size();
          logical_bytes += (**value).value_.encoded_.size();
          result.values_.push_back(std::move(**value));
        }
      }
      if (!result.values_.empty()) break;

      if (result.cursor_.dirty_cursor_ == 0) {
        bool old_remains = false;
        capture->dirty_keys_.ForEach([&](const auto& entry) {
          old_remains |=
              entry.value_.phase_ ==
              WorkerStore::PartitionStore::RdbSnapshotValue::Phase::kOldValue;
        });
        if (old_remains) continue;
        capture->dirty_keys_.Clear();
        partition.rdb_snapshot_.reset();
        ++result.cursor_.partition_index_;
        result.cursor_.db_id_ = 0;
        result.cursor_.index_cursor_ = 0;
        result.cursor_.dirty_cursor_ = 0;
        result.cursor_.finalizing_ = false;
        if (++partitions_visited == kMaxEmptyPartitionsPerBatch) break;
      }
    }
    result.done_ = result.cursor_.partition_index_ == store.partitions_.size();
    co_return result;
  } catch (const std::bad_alloc&) {
    if (store.rdb_snapshot_ && store.rdb_snapshot_->id_ == session_id)
      store.rdb_snapshot_->invalidated_ = true;
    RecordMemoryRejection();
    co_return absl::ResourceExhaustedError("OOM RDB snapshot batch");
  }
}

Task<absl::Status> StorageEngine::Impl::EndRdbSnapshot(
    std::uint64_t session_id) {
  WorkerStore& store = CurrentStore();
  while (store.rdb_snapshot_ && store.rdb_snapshot_->id_ == session_id &&
         store.rdb_snapshot_->ending_) {
    auto status =
        co_await bycorf::SleepFor(*store.worker_, std::chrono::milliseconds(1));
    if (!status.ok()) co_return status;
  }
  if (!store.rdb_snapshot_ || store.rdb_snapshot_->id_ != session_id) {
    co_return absl::OkStatus();
  }
  store.rdb_snapshot_->ending_ = true;
  struct EndGuard {
    WorkerStore* store;
    std::uint64_t session;
    ~EndGuard() {
      if (store->rdb_snapshot_ && store->rdb_snapshot_->id_ == session)
        store->rdb_snapshot_->ending_ = false;
    }
  } end_guard{&store, session_id};
  store.rdb_snapshot_->invalidated_ = true;
  while (store.rdb_snapshot_->readers_ != 0) {
    auto status =
        co_await bycorf::SleepFor(*store.worker_, std::chrono::milliseconds(1));
    if (!status.ok()) co_return status;
  }
  // No page cursor can reference the dirty-map entries after this point.
  store.rdb_snapshot_->collection_.reset();
  for (auto& partition : store.partitions_) {
    if (!partition.rdb_snapshot_ ||
        partition.rdb_snapshot_->session_id_ != session_id) {
      continue;
    }
    partition.rdb_snapshot_->accepting_ = false;
    while (partition.rdb_snapshot_->capture_admissions_ != 0) {
      absl::Status yielded = co_await bycorf::SleepFor(
          *store.worker_, std::chrono::milliseconds(1));
      if (!yielded.ok()) co_return yielded;
    }
    // Cancellation must also succeed when a previous admission failed. Walk
    // and release one pinned value at a time instead of allocating a vector
    // proportional to every dirty key. No capture or reader can mutate this
    // map now; its arena entry remains stable across the owner-hop release.
    auto& dirty = partition.rdb_snapshot_->dirty_keys_;
    std::uint64_t cursor = 0;
    WorkerStore::PartitionStore::RdbSnapshotValue* pinned = nullptr;
    do {
      const auto start = cursor;
      pinned = nullptr;
      cursor = dirty.Scan(cursor, [&](auto& entry) {
        if (pinned == nullptr && entry.value_.pins_held_)
          pinned = &entry.value_;
      });
      if (pinned != nullptr) {
        auto released = co_await ReleaseRdbSnapshotValue(pinned);
        if (!released.ok()) co_return released;
        cursor = start;  // Drain any other pins in the same scanned bucket.
      }
    } while (cursor != 0 || pinned != nullptr);
    dirty.Clear();
    partition.rdb_snapshot_.reset();
  }
  store.rdb_snapshot_.reset();
  co_return absl::OkStatus();
}

Task<absl::StatusOr<CollectionPage>> StorageEngine::Impl::ReadRdbCollectionPage(
    std::uint64_t session_id, std::uint64_t token, std::uint64_t cursor) {
  auto& store = CurrentStore();
  try {
    if (!store.rdb_snapshot_ || store.rdb_snapshot_->id_ != session_id ||
        store.rdb_snapshot_->invalidated_ || !store.rdb_snapshot_->collection_)
      co_return absl::FailedPreconditionError(
          "RDB collection stream is not active");
    auto& stream = *store.rdb_snapshot_->collection_;
    if (token == 0 || token != stream.token_ || cursor != stream.cursor_ ||
        stream.reading_ || stream.done_)
      co_return absl::InvalidArgumentError(
          "invalid RDB collection token or cursor");
    SnapshotReadGuard read_guard(&store.rdb_snapshot_->readers_);
    stream.reading_ = true;
    struct ReadingGuard {
      bool* reading;
      ~ReadingGuard() { *reading = false; }
    } reading_guard{&stream.reading_};
    LAVIK_FAULT_INJECT(if (const char* configured =
                               std::getenv("LAVIK_FAIL_RDB_COLLECTION_PAGE");
                           configured != nullptr) {
      std::uint64_t failed_cursor = 0;
      const char* end = configured + std::strlen(configured);
      const bool allocation =
          std::string_view(configured).starts_with("alloc:");
      const char* number = configured + (allocation ? 6 : 0);
      const auto parsed = std::from_chars(number, end, failed_cursor);
      if (parsed.ec == std::errc{} && parsed.ptr == end &&
          cursor == failed_cursor) {
        if (allocation) throw std::bad_alloc();
        // A later-page fault leaves an incomplete key in the temporary output.
        // The job must cancel its token and never publish that file over a
        // previously successful dump. Production builds omit this test hook.
        store.rdb_snapshot_->invalidated_ = true;
        co_return absl::InternalError(
            "injected RDB collection page read failure");
      }
    });
    const auto object = stream.saved_->grouped_;
    HashGroupId group_id;
    if (object->is_ordered()) {
      const auto& groups = object->ordered_directory().groups();
      if (cursor >= groups.size())
        co_return absl::DataLossError("RDB ordered cursor exceeds directory");
      group_id = {groups[cursor].id_, 0};
    } else {
      if (stream.hash_cursor_ == object->directory().groups().end())
        co_return absl::DataLossError("RDB Hash cursor exceeds directory");
      group_id = stream.hash_cursor_->second.id_;
    }
    const auto* group_entry = object->FindGroup(group_id);
    if (group_entry == nullptr)
      co_return absl::DataLossError("RDB page has no physical record");
    const auto location = MaterializeIndexLocation(*group_entry);
    std::size_t payload_bytes = location.total_disk_bytes();
    if (location.external()) {
      const auto extents = object->ExtentsFor(group_id);
      if (extents == nullptr)
        co_return absl::DataLossError("RDB page has no extent manifest");
      payload_bytes = 0;
      for (const auto& extent : *extents) {
        if (extent.payload_bytes_ > kMaxRecordPayloadBytes - payload_bytes)
          co_return absl::DataLossError("RDB page extent payload overflow");
        payload_bytes += extent.payload_bytes_;
      }
    }
    // A decoded page survives asynchronous output backpressure. Reserve its
    // retention before any page IO/decode; the loaders validate the checked
    // envelope count before allocating entry vectors. This conservatively
    // covers both decoder/DTO vector headers and small-string capacity.
    const auto maximum = std::numeric_limits<std::size_t>::max();
    constexpr std::size_t per_entry = 256;
    constexpr std::size_t overhead = 4096;
    if (payload_bytes > maximum - overhead ||
        location.logical_size_ >
            (maximum - payload_bytes - overhead) / per_entry)
      co_return absl::ResourceExhaustedError(
          "RDB page admission size overflow");
    auto reservation = TryReserveMemory(payload_bytes + overhead +
                                        location.logical_size_ * per_entry);
    if (!reservation) {
      RecordMemoryRejection();
      store.rdb_snapshot_->invalidated_ = true;
      co_return absl::ResourceExhaustedError(
          "OOM RDB collection page retention rejected");
    }
    CollectionPage page;
    page.value_type_ = stream.saved_->location_.value_type();
    const Digest digest = ComputeDigest(stream.key_);
    if (object->is_ordered()) {
      const auto& groups = object->ordered_directory().groups();
      if (cursor >= groups.size())
        co_return absl::DataLossError("RDB ordered cursor exceeds directory");
      auto loaded = co_await LoadOrderedGroupSnapshot(
          store, *stream.partition_, stream.db_id_, stream.key_, digest, object,
          groups[cursor].id_, true);
      if (!loaded.ok()) {
        store.rdb_snapshot_->invalidated_ = true;
        co_return loaded.status();
      }
      if (page.value_type_ == ValueType::kList) {
        page.elements_.reserve(loaded->snapshot_.entries_.size());
        for (auto& entry : loaded->snapshot_.entries_)
          page.elements_.push_back(std::move(entry.value_));
      } else {
        page.scored_members_.reserve(loaded->snapshot_.entries_.size());
        for (auto& entry : loaded->snapshot_.entries_)
          page.scored_members_.push_back(
              {std::move(entry.value_), entry.score_});
      }
      page.done_ = cursor + 1 == groups.size();
    } else {
      const auto& groups = object->directory().groups();
      if (stream.hash_cursor_ == groups.end())
        co_return absl::DataLossError("RDB Hash cursor exceeds directory");
      const auto id = stream.hash_cursor_->second.id_;
      auto loaded = co_await LoadHashGroupSnapshot(store, *stream.partition_,
                                                   stream.db_id_, stream.key_,
                                                   digest, object, id, true);
      if (!loaded.ok()) {
        store.rdb_snapshot_->invalidated_ = true;
        co_return loaded.status();
      }
      if (page.value_type_ == ValueType::kHash) {
        page.fields_.reserve(loaded->snapshot_.value_.entries_.size());
        for (auto& field : loaded->snapshot_.value_.entries_)
          page.fields_.push_back(
              {std::move(field.field_), std::move(field.value_)});
      } else {
        page.elements_.reserve(loaded->snapshot_.value_.entries_.size());
        for (auto& field : loaded->snapshot_.value_.entries_) {
          if (!field.value_.empty()) {
            store.rdb_snapshot_->invalidated_ = true;
            co_return absl::DataLossError("RDB Set page contains a Hash value");
          }
          page.elements_.push_back(std::move(field.field_));
        }
      }
      ++stream.hash_cursor_;
      page.done_ = stream.hash_cursor_ == groups.end();
    }
    if (store.rdb_snapshot_->invalidated_)
      co_return absl::CancelledError(
          "RDB collection stream was cancelled during read");
    const auto expected = stream.saved_->location_.logical_size_;
    if (stream.emitted_ > expected ||
        page.size() > expected - stream.emitted_ ||
        (page.done_ && page.size() != expected - stream.emitted_)) {
      store.rdb_snapshot_->invalidated_ = true;
      co_return absl::DataLossError("RDB collection aggregate count mismatch");
    }
    stream.emitted_ += page.size();
    page.next_cursor_ = ++stream.cursor_;
    stream.done_ = page.done_;
    const auto retained_bytes = page.RetainedBytes();
    if (retained_bytes > reservation->bytes()) {
      store.rdb_snapshot_->invalidated_ = true;
      co_return absl::DataLossError(
          "RDB decoded page exceeds its admitted envelope");
    }
    page.retained_charge_.Adopt(&*reservation, retained_bytes);
    LAVIK_FAULT_INJECT(if (LAVIK_FAULT_MATCHES("LAVIK_RDB_CANCEL_ADMITTED_PAGE",
                                               stream.key_)) {
      static std::atomic_flag once;
      if (!once.test_and_set(std::memory_order_relaxed)) {
        // Exercise a real concurrent End, not merely FLUSHDB's invalidation.
        // The independent coroutine must wait for this reader, and its exact
        // pin checks must remain a registered settlement during shutdown.
        using BlockPins =
            WorkerStore::PartitionStore::RdbSnapshotValue::BlockPins;
        const auto pins = stream.saved_->block_pins_;
        const auto before =
            GetWorkerMemoryStats(store.worker_->id()).retained_bytes_;
        auto cancel = [](Impl* engine, WorkerStore* owner,
                         std::uint64_t session,
                         std::shared_ptr<const BlockPins> exact_pins,
                         std::uint64_t retained_before,
                         std::size_t page_bytes) -> Task<absl::Status> {
          struct Settlement {
            std::atomic<std::uint32_t>* count;
            ~Settlement() { count->fetch_sub(1, std::memory_order_acq_rel); }
          } settlement{&engine->active_settlements_};
          auto ended = co_await engine->EndRdbSnapshot(session);
          if (!ended.ok()) co_return ended;
          ended = co_await engine->EndRdbSnapshot(session);
          if (!ended.ok()) co_return ended;
          std::size_t remaining = 0;
          for (const auto pin : exact_pins->blocks_) {
            const unsigned physical_owner = engine->BlockOwner(pin.block_id_);
            auto count = [engine, physical_owner,
                          pin]() -> Task<std::uint32_t> {
              auto& physical = *engine->stores_[physical_owner];
              co_await physical.store_state_mutex_.Lock();
              UnlockGuard unlock(&physical.store_state_mutex_,
                                 physical.worker_);
              const auto* block =
                  engine->FindBlockState(physical, pin.block_id_);
              co_return block != nullptr && block->allocated_ &&
                      block->allocation_epoch_ == pin.allocation_epoch_
                  ? block->pins_
                  : 0;
            };
            // if/else, not ?:, to keep the two co_awaits in separate full
            // expressions. GCC 13 can reuse the wrong coroutine-frame slot
            // when both arms of ?: contain co_await.
            if (physical_owner == owner->worker_->id()) {
              remaining += co_await count();
            } else {
              remaining += co_await bycorf::SubmitTaskTo(physical_owner, count);
            }
          }
          const auto after =
              GetWorkerMemoryStats(owner->worker_->id()).retained_bytes_;
          if (remaining != 0 || retained_before < page_bytes ||
              after > retained_before - page_bytes) {
            spdlog::error(
                "RDB page cancellation leaked state: pins={} before={} "
                "after={} page={}",
                remaining, retained_before, after, page_bytes);
            co_return absl::InternalError("RDB page cancellation leaked state");
          }
          spdlog::info(
              "RDB page cancellation completed: pins={} remaining=0 "
              "returned-page-bytes={}",
              exact_pins->blocks_.size(), page_bytes);
          co_return absl::OkStatus();
        };
        active_settlements_.fetch_add(1, std::memory_order_acq_rel);
        store.worker_->Spawn(
            cancel(this, &store, session_id, pins, before, retained_bytes));
        auto paused = co_await bycorf::SleepFor(*store.worker_,
                                                std::chrono::milliseconds(50));
        if (!paused.ok()) co_return paused;
        if (!store.rdb_snapshot_ || !store.rdb_snapshot_->ending_ ||
            store.rdb_snapshot_->readers_ == 0) {
          co_return absl::InternalError(
              "RDB End did not wait for its page reader");
        }
        spdlog::info(
            "RDB page cancellation waiting: readers={} retained-page={}",
            store.rdb_snapshot_->readers_, retained_bytes);
        co_return absl::CancelledError(
            "RDB page cancelled while holding reader");
      }
    });
    co_return std::move(page);
  } catch (const std::bad_alloc&) {
    // Guards have dropped the active reader before End can settle the token;
    // invalidate it so a caller cannot retry a partially advanced cursor.
    if (store.rdb_snapshot_ && store.rdb_snapshot_->id_ == session_id)
      store.rdb_snapshot_->invalidated_ = true;
    RecordMemoryRejection();
    co_return absl::ResourceExhaustedError(
        "OOM RDB collection page allocation");
  }
}

Task<absl::Status> StorageEngine::Impl::FinishRdbCollection(
    std::uint64_t session_id, std::uint64_t token) {
  auto& store = CurrentStore();
  if (!store.rdb_snapshot_ || store.rdb_snapshot_->id_ != session_id ||
      store.rdb_snapshot_->invalidated_ || !store.rdb_snapshot_->collection_)
    co_return absl::FailedPreconditionError(
        "RDB collection stream is not active");
  auto& stream = *store.rdb_snapshot_->collection_;
  if (token == 0 || token != stream.token_ || stream.reading_ || !stream.done_)
    co_return absl::InvalidArgumentError(
        "RDB collection is incomplete or busy");
  SnapshotReadGuard read_guard(&store.rdb_snapshot_->readers_);
  stream.reading_ = true;
  auto released = co_await ReleaseRdbSnapshotValue(stream.saved_);
  if (!released.ok()) {
    stream.reading_ = false;
    store.rdb_snapshot_->invalidated_ = true;
    co_return released;
  }
  stream.saved_->phase_ =
      WorkerStore::PartitionStore::RdbSnapshotValue::Phase::kDone;
  store.rdb_snapshot_->collection_.reset();
  co_return absl::OkStatus();
}

}  // namespace lavik::storage
