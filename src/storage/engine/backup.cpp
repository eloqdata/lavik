#include "impl.h"

namespace keylane::storage {
namespace {

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

}  // namespace

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

  struct BlockPin {
    std::uint64_t block_id_ = 0;
    std::uint64_t allocation_epoch_ = 0;
    bool extent_ = false;
  };
  std::vector<BlockPin> blocks;
  blocks.reserve(1 +
                 (value->extents_ == nullptr ? 0 : value->extents_->size()));
  blocks.push_back(BlockPin{value->location_.block_id(),
                            value->location_.allocation_epoch(), false});
  if (value->extents_ != nullptr) {
    for (const ExtentRef& ref : *value->extents_) {
      blocks.push_back(BlockPin{ref.block_id_, ref.allocation_epoch_, true});
    }
  }

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
    co_return owner == celer::ThisWorker().id_
        ? co_await on_owner()
        : co_await celer::SubmitTaskTo(owner, on_owner);
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
    co_return owner == celer::ThisWorker().id_
        ? co_await on_owner()
        : co_await celer::SubmitTaskTo(owner, on_owner);
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
  struct BlockPin {
    std::uint64_t block_id_ = 0;
    std::uint64_t allocation_epoch_ = 0;
  };
  std::vector<BlockPin> blocks;
  blocks.reserve(1 +
                 (value->extents_ == nullptr ? 0 : value->extents_->size()));
  blocks.push_back(BlockPin{value->location_.block_id(),
                            value->location_.allocation_epoch()});
  if (value->extents_ != nullptr) {
    for (const ExtentRef& ref : *value->extents_) {
      blocks.push_back(BlockPin{ref.block_id_, ref.allocation_epoch_});
    }
  }
  value->pins_held_ = false;
  for (const BlockPin pin : blocks) {
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
    absl::Status released = owner == celer::ThisWorker().id_
                                ? co_await on_owner()
                                : co_await celer::SubmitTaskTo(owner, on_owner);
    if (!released.ok()) co_return released;
  }
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
        capture->dirty_keys_.InsertNew(map_digest, map_key,
                                       SnapshotValue{
                                           .location_ = {},
                                           .extents_ = nullptr,
                                           .phase_ = Phase::kAbsent,
                                       });
        reservation->Release();
      }
      --capture->capture_admissions_;
      co_return absl::OkStatus();
    }

    SnapshotValue old{
        .location_ = MaterializeIndexLocation(*current),
        .extents_ = ExtentsFor(store, current),
        .phase_ = Phase::kOldValue,
    };
    store.store_state_mutex_.Unlock(*store.worker_);
#ifndef NDEBUG
    // Deterministic regression hook for an append-stream rollover while this
    // writer has released the store lock to pin the pre-cut value.
    static std::atomic<bool> pause_claimed = false;
    const char* pause_text = std::getenv("KEYLANE_RDB_CAPTURE_PAUSE_MS");
    bool expected_pause = false;
    if (pause_text != nullptr &&
        pause_claimed.compare_exchange_strong(expected_pause, true)) {
      char* end = nullptr;
      const unsigned long pause_ms = std::strtoul(pause_text, &end, 10);
      if (end != pause_text && *end == '\0' && pause_ms != 0) {
        (void)co_await celer::SleepFor(*store.worker_,
                                       std::chrono::milliseconds(pause_ms));
      }
    }
#endif
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
        capture->dirty_keys_.InsertNew(map_digest, map_key, old);
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
        capture->dirty_keys_.InsertNew(map_digest, map_key,
                                       SavedValue{
                                           .location_ = {},
                                           .extents_ = nullptr,
                                           .phase_ = Phase::kDone,
                                       });
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
          .phase_ = Phase::kInflight,
      };
      auto reservation =
          ReserveSnapshotMapInsert(capture->dirty_keys_, map_digest, map_key);
      if (!reservation.has_value()) {
        RecordMemoryRejection();
        store.rdb_snapshot_->invalidated_ = true;
        co_return absl::ResourceExhaustedError(
            "RDB snapshot dirty-key allocation failed");
      }
      saved = capture->dirty_keys_.InsertNew(map_digest, map_key, candidate);
      reservation->Release();
      absl::Status pinned = co_await PinRdbSnapshotValue(&saved->value_);
      if (!pinned.ok()) {
        capture->dirty_keys_.Erase(saved);
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
  auto loaded = co_await LoadValue(store, partition, db_id, key, digest,
                                   physical.location_, physical.extents_);
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
  if (!store.rdb_snapshot_ || store.rdb_snapshot_->id_ != session_id ||
      store.rdb_snapshot_->invalidated_) {
    co_return absl::FailedPreconditionError(
        "RDB snapshot was invalidated or ended");
  }

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
      if (result.cursor_.db_id_ < kLogicalDatabaseCount) {
        const std::size_t remaining = count - result.values_.size();
        auto scanned = co_await ScanPartition(
            partition.id_, result.cursor_.db_id_, result.cursor_.index_cursor_,
            std::max<std::size_t>(remaining, 1), capture->snapshot_time_ms_,
            max_bytes - logical_bytes);
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
          }
        }
        if (result.cursor_.index_cursor_ == 0) {
          ++result.cursor_.db_id_;
        }
        if (!result.values_.empty()) break;
        continue;
      }
      capture->accepting_ = false;
      result.cursor_.finalizing_ = true;
      result.cursor_.dirty_cursor_ = 0;
    }

    if (capture->capture_admissions_ != 0) {
      absl::Status yielded = co_await celer::SleepFor(
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
                         WorkerStore::PartitionStore::RdbSnapshotValue::Phase::
                             kOldValue &&
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
}

Task<absl::Status> StorageEngine::Impl::EndRdbSnapshot(
    std::uint64_t session_id) {
  WorkerStore& store = CurrentStore();
  if (!store.rdb_snapshot_ || store.rdb_snapshot_->id_ != session_id) {
    co_return absl::OkStatus();
  }
  store.rdb_snapshot_->invalidated_ = true;
  for (auto& partition : store.partitions_) {
    if (!partition.rdb_snapshot_ ||
        partition.rdb_snapshot_->session_id_ != session_id) {
      continue;
    }
    partition.rdb_snapshot_->accepting_ = false;
    while (partition.rdb_snapshot_->capture_admissions_ != 0) {
      absl::Status yielded = co_await celer::SleepFor(
          *store.worker_, std::chrono::milliseconds(1));
      if (!yielded.ok()) co_return yielded;
    }
    std::vector<WorkerStore::PartitionStore::RdbSnapshotValue> pinned;
    partition.rdb_snapshot_->dirty_keys_.ForEach([&](const auto& entry) {
      if (entry.value_.pins_held_) pinned.push_back(entry.value_);
    });
    partition.rdb_snapshot_->dirty_keys_.Clear();
    partition.rdb_snapshot_.reset();
    for (auto& value : pinned) {
      absl::Status released = co_await ReleaseRdbSnapshotValue(&value);
      if (!released.ok()) co_return released;
    }
  }
  store.rdb_snapshot_.reset();
  co_return absl::OkStatus();
}

}  // namespace keylane::storage
