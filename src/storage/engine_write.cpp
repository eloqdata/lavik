#include "engine_impl.h"

namespace keylane::storage {

Task<StatusOr<SetResult>> StorageEngine::Impl::Set(std::uint8_t db_id,
                                                   std::string_view key,
                                                   std::string_view value,
                                                   SetOptions options) {
  assert(db_id < kLogicalDatabaseCount);
  const Digest digest = ComputeDigest(key);
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest), tx::LockMode::kExclusive);
  co_return co_await SetLocked(db_id, key, digest, value, options);
}

Task<StatusOr<SetResult>> StorageEngine::Impl::SetLocked(std::uint8_t db_id,
                                                         std::string_view key,
                                                         const Digest& digest,
                                                         std::string_view value,
                                                         SetOptions options) {
  assert(db_id < kLogicalDatabaseCount);
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionForKey(store, key);
  co_await store.writer_mutex.Lock();
  UnlockGuard unlock(&store.writer_mutex, store.worker);

  auto& index = partition.indexes[db_id];
  auto* found = index.Find(digest, key);
  const std::uint64_t now_ms = UnixTimeMillis();
  const bool exists = found != nullptr &&
                      found->value.kind == RecordKind::kValue &&
                      !IsExpired(found->value, now_ms);
  SetResult result;
  if (options.return_old_value && exists) {
    if (found->value.value_type != ValueType::kString) {
      co_return Status(StatusCode::kInvalidArgument,
                       "WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    auto loaded = co_await LoadValue(store, db_id, key, digest, found->value);
    if (!loaded.ok()) {
      co_return loaded.status();
    }
    auto encoded = EncodeDiskValue(std::move(*loaded));
    if (!encoded.ok()) {
      co_return encoded.status();
    }
    result.old_value.emplace(std::move(*encoded));
  }

  const bool condition_met =
      options.condition == SetCondition::kNone ||
      (options.condition == SetCondition::kIfAbsent && !exists) ||
      (options.condition == SetCondition::kIfPresent && exists);
  if (!condition_met) {
    co_return result;
  }

  const std::uint64_t expire_at_ms =
      options.keep_ttl && exists ? found->value.expire_at_ms
                                 : options.expire_at_ms;
  Status status = co_await AppendLocked(
      store, partition, db_id, key, value, RecordKind::kValue,
      ValueType::kString, expire_at_ms);
  if (!status.ok()) {
    co_return status;
  }
  result.applied = true;
  co_return result;
}

Task<StatusOr<bool>> StorageEngine::Impl::UpdateExpiration(
    std::uint8_t db_id, std::string_view key, std::uint64_t expire_at_ms,
    ExpirationCondition condition) {
  assert(db_id < kLogicalDatabaseCount);
  const Digest digest = ComputeDigest(key);
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest), tx::LockMode::kExclusive);
  co_return co_await UpdateExpirationLocked(db_id, key, digest, expire_at_ms,
                                            condition);
}

Task<StatusOr<bool>> StorageEngine::Impl::UpdateExpirationLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    std::uint64_t expire_at_ms, ExpirationCondition condition) {
  assert(db_id < kLogicalDatabaseCount);
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionForKey(store, key);
  co_await store.writer_mutex.Lock();
  UnlockGuard unlock(&store.writer_mutex, store.worker);

  auto& index = partition.indexes[db_id];
  auto* found = index.Find(digest, key);
  const std::uint64_t now_ms = UnixTimeMillis();
  if (found == nullptr || found->value.kind != RecordKind::kValue ||
      IsExpired(found->value, now_ms)) {
    co_return false;
  }
  const std::uint64_t current = found->value.expire_at_ms;
  bool condition_met = true;
  switch (condition) {
    case ExpirationCondition::kNone:
      break;
    case ExpirationCondition::kIfNoExpiration:
      condition_met = current == 0;
      break;
    case ExpirationCondition::kIfHasExpiration:
      condition_met = current != 0;
      break;
    case ExpirationCondition::kIfGreater:
      condition_met = current != 0 && expire_at_ms > current;
      break;
    case ExpirationCondition::kIfLess:
      condition_met = current == 0 || expire_at_ms < current;
      break;
  }
  if (!condition_met) {
    co_return false;
  }

  if (expire_at_ms != 0 && expire_at_ms <= now_ms) {
    Status status = co_await AppendLocked(
        store, partition, db_id, key, {}, RecordKind::kTombstone,
        ValueType::kNone, 0);
    if (!status.ok()) {
      co_return status;
    }
    co_return true;
  }

  const RecordLocation previous = found->value;
  auto loaded = co_await LoadValue(store, db_id, key, digest, previous);
  if (!loaded.ok()) {
    co_return loaded.status();
  }
  FixedBuffer value_buffer = loaded->lease.io_buffer();
  std::string_view value(reinterpret_cast<const char*>(value_buffer.data),
                         loaded->value_bytes);
  Status status = co_await AppendLocked(
      store, partition, db_id, key, value, RecordKind::kValue,
      previous.value_type, expire_at_ms);
  if (!status.ok()) {
    co_return status;
  }
  co_return true;
}

Task<StatusOr<bool>> StorageEngine::Impl::Delete(std::uint8_t db_id,
                                                 std::string_view key) {
  assert(db_id < kLogicalDatabaseCount);
  const Digest digest = ComputeDigest(key);
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest), tx::LockMode::kExclusive);
  co_return co_await DeleteLocked(db_id, key, digest);
}

Task<StatusOr<bool>> StorageEngine::Impl::DeleteLocked(std::uint8_t db_id,
                                                       std::string_view key,
                                                       const Digest& digest) {
  assert(db_id < kLogicalDatabaseCount);
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionForKey(store, key);
  co_await store.writer_mutex.Lock();
  UnlockGuard unlock(&store.writer_mutex, store.worker);

  auto& index = partition.indexes[db_id];
  auto* found = index.Find(digest, key);
  if (found == nullptr || found->value.kind == RecordKind::kTombstone) {
    co_return false;
  }
  const bool expired = IsExpired(found->value, UnixTimeMillis());
  Status status =
      co_await AppendLocked(store, partition, db_id, key, {},
                            RecordKind::kTombstone, ValueType::kNone, 0);
  if (!status.ok()) {
    co_return status;
  }
  co_return !expired;
}

Task<StatusOr<std::int64_t>> StorageEngine::Impl::Increment(std::uint8_t db_id,
                                                            std::string_view key) {
  assert(db_id < kLogicalDatabaseCount);
  const Digest digest = ComputeDigest(key);
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest), tx::LockMode::kExclusive);
  co_return co_await IncrementLocked(db_id, key, digest);
}

Task<StatusOr<std::int64_t>> StorageEngine::Impl::IncrementLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest) {
  assert(db_id < kLogicalDatabaseCount);
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionForKey(store, key);
  co_await store.writer_mutex.Lock();
  UnlockGuard unlock(&store.writer_mutex, store.worker);

  std::int64_t value = 0;
  std::uint64_t expire_at_ms = 0;
  auto& index = partition.indexes[db_id];
  auto* found = index.Find(digest, key);
  const bool exists = found != nullptr &&
                      found->value.kind == RecordKind::kValue &&
                      !IsExpired(found->value, UnixTimeMillis());
  if (exists) {
    if (found->value.value_type != ValueType::kString) {
      co_return Status(StatusCode::kInvalidArgument,
                       "WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    expire_at_ms = found->value.expire_at_ms;
    auto loaded =
        co_await LoadValue(store, db_id, key, digest, found->value);
    if (!loaded.ok()) {
      co_return loaded.status();
    }
    const std::span<const std::byte> value_bytes = loaded->value();
    std::string_view text(reinterpret_cast<const char*>(value_bytes.data()),
                          value_bytes.size());
    auto [end, error] = std::from_chars(text.data(), text.data() + text.size(),
                                        value);
    if (error != std::errc{} || end != text.data() + text.size() ||
        value == std::numeric_limits<std::int64_t>::max()) {
      co_return Status(StatusCode::kInvalidArgument,
                       "value is not an integer or out of range");
    }
  }
  ++value;
  const std::string encoded = std::to_string(value);
  Status status =
      co_await AppendLocked(store, partition, db_id, key, encoded,
                            RecordKind::kValue, ValueType::kString,
                            expire_at_ms);
  if (!status.ok()) {
    co_return status;
  }
  co_return value;
}

StagingSlot* StorageEngine::Impl::StagingFor(WorkerStore& store,
                                             const BlockState& state) {
  return state.staging_slot == 0
             ? nullptr
             : &store.staging_slots[state.staging_slot];
}

std::uint16_t StorageEngine::Impl::AcquireStagingSlot(WorkerStore& store) {
  if (store.free_staging_slot != 0) {
    const std::uint16_t id = store.free_staging_slot;
    store.free_staging_slot = store.staging_slots[id].next_free;
    store.staging_slots[id] = StagingSlot{};
    return id;
  }
  store.staging_slots.emplace_back();
  return static_cast<std::uint16_t>(store.staging_slots.size() - 1);
}

FixedBuffer StorageEngine::Impl::StagingBufferFor(WorkerStore& store,
                                                  const BlockState& state) const {
  const StagingSlot* slot = StagingFor(store, state);
  if (slot == nullptr) {
    return FixedBuffer{};
  }
  if (slot->write_buffer_id != 0) {
    return store.buffers.write_buffer(slot->write_buffer_id);
  }
  return FixedBuffer{.data = slot->heap_data,
                    .size = slot->heap_data_size,
                    .index = 0};
}

void StorageEngine::Impl::ReleaseStagingBuffer(WorkerStore& store,
                                               BlockState& state) {
  if (StagingSlot* slot = StagingFor(store, state); slot != nullptr) {
    if (slot->write_buffer_id != 0) {
      store.buffers.ReleaseWriteBuffer(slot->write_buffer_id);
    } else if (slot->heap_data != nullptr) {
      store.buffers.ReleaseHeapWriteBuffer(slot->heap_data);
    }
    *slot = StagingSlot{};
    slot->next_free = store.free_staging_slot;
    store.free_staging_slot = state.staging_slot;
    state.staging_slot = 0;
  }
  state.release_pending = false;
  state.in_memory = false;
}

Status StorageEngine::Impl::MarkRecordDeadLocal(unsigned owner,
                                                const RecordLocation& location) {
  WorkerStore& store = *stores_[owner];
  BlockState* state = FindBlockState(store, location.block_id);
  if (state == nullptr || !state->allocated ||
      state->allocation_epoch != location.allocation_epoch) {
    return Status(StatusCode::kInternal,
                  "stale block owner while invalidating record");
  }
  // live_bytes is the byte sum over exactly the index entries naming this
  // block at this allocation epoch, so the entry being retired here is one of
  // the summands and the subtraction cannot underflow. Saturating instead of
  // reporting would drive live_bytes to zero while records are still
  // reachable, which CleanBlockLocked now reads as "nothing to salvage" and
  // frees without inspecting the block. Fail loudly rather than lose data.
  if (state->live_bytes < location.total_disk_bytes) {
    return Status(StatusCode::kInternal,
                  "block live-byte accounting underflow");
  }
  state->live_bytes -= location.total_disk_bytes;
  MaybeQueueDefrag(store, location.block_id);
  return Status::Ok();
}

Task<Status> StorageEngine::Impl::MarkRecordDead(const RecordLocation& location) {
  assert(location.block_owner < worker_count_);
  const unsigned owner = location.block_owner;
  if (owner == celer::ThisWorker().id) {
    co_return MarkRecordDeadLocal(owner, location);
  }
  co_return co_await celer::SubmitTo(
      owner, [this, owner, location] {
        return MarkRecordDeadLocal(owner, location);
      });
}

Task<StatusOr<ReservedBlock>> StorageEngine::Impl::TakeStandaloneBlockLocked(
    WorkerStore& store) {
  while (!store.standby_block.has_value()) {
    Status waited = co_await WaitForStandbyWithWriterUnlocked(store, false);
    if (!waited.ok()) {
      co_return waited;
    }
  }
  const ReservedBlock block = *store.standby_block;
  store.standby_block.reset();
  RequestStandbyBlock(store, false);
  co_return block;
}

Task<StatusOr<std::shared_ptr<const std::vector<ExtentRef>>>>
StorageEngine::Impl::WriteExtentValueLocked(WorkerStore& store,
                                            std::string_view value) {
  if (value.empty() || value.size() > kMaxStringBytes) {
    co_return Status(StatusCode::kOutOfRange,
                     "String exceeds the 512 MiB limit");
  }
  auto refs = std::make_shared<std::vector<ExtentRef>>();
  refs->reserve((value.size() + kExtentPayloadBytes - 1) /
                kExtentPayloadBytes);
  auto reclaim_allocated = [&]() {
    if (!refs->empty()) {
      SpawnExtentReclaim(
          store, std::shared_ptr<const std::vector<ExtentRef>>(refs));
    }
  };
  std::size_t value_offset = 0;
  std::uint32_t extent_index = 0;
  while (value_offset < value.size()) {
    auto reserved = co_await TakeStandaloneBlockLocked(store);
    if (!reserved.ok()) {
      reclaim_allocated();
      co_return reserved.status();
    }
    const std::size_t payload_bytes =
        std::min(kExtentPayloadBytes, value.size() - value_offset);
    const auto payload = std::span<const std::byte>(
        reinterpret_cast<const std::byte*>(value.data() + value_offset),
        payload_bytes);
    const std::uint32_t payload_checksum = Crc32c(payload);
    BlockState& state = CreateBlockState(store, reserved->block_id);
    state.writer_id = store.worker->id();
    state.layout_worker_count = worker_count_;
    state.allocation_epoch = reserved->allocation_epoch;
    state.committed_bytes = static_cast<std::uint32_t>(
        kBlockHeaderBytes + payload_bytes);
    // An extent block is written whole right here and never enters the flush
    // queue, so it needs no staging slot.
    state.live_bytes = static_cast<std::uint32_t>(payload_bytes);
    state.allocated = true;
    state.kind = BlockKind::kValueExtent;
    refs->push_back(ExtentRef{
        .block_id = reserved->block_id,
        .allocation_epoch = reserved->allocation_epoch,
        .payload_bytes = static_cast<std::uint32_t>(payload_bytes),
        .payload_checksum = payload_checksum,
    });
    std::uint16_t write_buffer_id = 0;
    std::byte* heap_buffer = nullptr;
    if (!store.buffers.TryAcquireWriteBuffer(&write_buffer_id) &&
        !store.buffers.TryAcquireHeapWriteBuffer(&heap_buffer)) {
      reclaim_allocated();
      co_return Status(StatusCode::kResourceExhausted,
                       "no extent write buffer is available");
    }
    auto release_buffer = [&]() {
      if (write_buffer_id != 0) {
        store.buffers.ReleaseWriteBuffer(write_buffer_id);
      } else {
        store.buffers.ReleaseHeapWriteBuffer(heap_buffer);
      }
    };
    FixedBuffer staging =
        write_buffer_id != 0
            ? store.buffers.write_buffer(write_buffer_id)
            : FixedBuffer{.data = heap_buffer,
                          .size = options_.buffers.write_buffer_bytes,
                          .index = 0};
    if (staging.data == nullptr || staging.size < kStorageBlockBytes) {
      release_buffer();
      reclaim_allocated();
      co_return Status(StatusCode::kInternal,
                       "extent staging buffer is smaller than a block");
    }
    std::fill_n(staging.data, kStorageBlockBytes, std::byte{0});
    BlockHeader header{
        .magic = kBlockMagic,
        .block_id = reserved->block_id,
        .version = kStorageFormatVersion,
        .header_bytes = kBlockHeaderBytes,
        .block_bytes = kStorageBlockBytes,
        .writer_id = store.worker->id(),
        .allocation_epoch = reserved->allocation_epoch,
        .committed_bytes = static_cast<std::uint32_t>(
            kBlockHeaderBytes + payload_bytes),
        .record_count = 0,
        .max_lsn = next_lsn_.fetch_add(1, std::memory_order_relaxed),
        .header_sequence = 1,
        .checksum = 0,
        .layout_worker_count = worker_count_,
        .kind = BlockKind::kValueExtent,
        .reserved = {},
        .extent_index = extent_index,
        .extent_payload_bytes = static_cast<std::uint32_t>(payload_bytes),
        .extent_payload_checksum = payload_checksum,
    };
    EncodeBlockHeader(
        header, std::span<std::byte, kBlockHeaderSlotBytes>(
                    staging.data, kBlockHeaderSlotBytes));
    std::memset(staging.data + kBlockHeaderSlotBytes, 0,
                kBlockHeaderBytes - kBlockHeaderSlotBytes);
    std::memcpy(staging.data + kBlockHeaderBytes, payload.data(),
                payload.size());
    const auto [file_id, block_offset] = FileOffset(reserved->block_id);
    // Start at the second header slot, which staging left zero. An extent
    // block only ever writes slot 0, so this durably clears whatever header
    // the block carried in a previous life before the new one commits.
    const std::size_t write_begin = kBlockHeaderSlotBytes;
    const std::size_t write_bytes =
        kBlockHeaderBytes + AlignDirect(payload_bytes);
    bool write_ok = true;
    Status write_status = Status::Ok();
    for (std::size_t offset = write_begin; offset < write_bytes;) {
      const std::size_t chunk =
          std::min(options_.flush_size_bytes, write_bytes - offset);
      auto written = co_await WriteStorageBuffer(
          *store.worker, store.files[file_id],
          std::span<const std::byte>(staging.data + offset, chunk),
          write_buffer_id != 0, staging, block_offset + offset);
      if (!written.ok() || *written != chunk) {
        write_ok = false;
        write_status = written.ok()
                           ? Status(StatusCode::kInternal,
                                    "short extent block write")
                           : written.status();
        break;
      }
      offset += chunk;
    }
    if (write_ok) {
      write_status =
          co_await celer::Fdatasync(*store.worker, store.files[file_id]);
    }
    if (write_status.ok()) {
      auto written = co_await WriteStorageBuffer(
          *store.worker, store.files[file_id],
          std::span<const std::byte>(staging.data, kBlockHeaderSlotBytes),
          write_buffer_id != 0, staging, block_offset);
      if (!written.ok() || *written != kBlockHeaderSlotBytes) {
        write_status = written.ok()
                           ? Status(StatusCode::kInternal,
                                    "short extent header write")
                           : written.status();
      }
    }
    if (write_status.ok()) {
      write_status =
          co_await celer::Fdatasync(*store.worker, store.files[file_id]);
    }
    release_buffer();
    if (!write_status.ok()) {
      store.write_failed = true;
      reclaim_allocated();
      co_return write_status;
    }
    value_offset += payload_bytes;
    ++extent_index;
  }
  co_return std::shared_ptr<const std::vector<ExtentRef>>(std::move(refs));
}

Task<Status> StorageEngine::Impl::AppendLocked(WorkerStore& store,
                                               WorkerStore::PartitionStore& partition,
                                               std::uint8_t db_id,
                                               std::string_view key,
                                               std::string_view value,
                                               RecordKind kind,
                                               ValueType value_type,
                                               std::uint64_t expire_at_ms) {
  const Digest digest = ComputeDigest(key);
  // Every real keyspace modification funnels through here (client writes,
  // deletes, expiration rewrites, active expiry): invalidate watchers.
  tx::CurrentTxShard().MarkWatched(db_id, tx::FingerprintOf(digest));
  const std::uint64_t mutation_sequence = ++partition.mutation_sequence;
  Status status = Status::Ok();
  const std::size_t inline_bytes =
      AlignRecord(RecordHeaderBytes(key.size()) + value.size());
  if (kind == RecordKind::kValue && value_type == ValueType::kString &&
      inline_bytes > kStorageBlockBytes - kBlockHeaderBytes) {
    auto extents = co_await WriteExtentValueLocked(store, value);
    if (!extents.ok()) {
      co_return extents.status();
    }
    const std::string manifest = EncodeManifest(**extents);
    status = co_await WriteRecordLocked(
        store, db_id, key, manifest, kind, value_type, expire_at_ms, digest,
        mutation_sequence, mutation_sequence, 0, false, true, true,
        value.size(), *extents);
    if (!status.ok()) {
      store.worker->Spawn(ReclaimExtents(&store, *extents));
    }
  } else {
    status = co_await WriteRecordLocked(
        store, db_id, key, value, kind, value_type, expire_at_ms, digest,
        mutation_sequence, mutation_sequence, 0, false);
  }
  if (status.ok() && partition.capture_deltas) {
    AppendDelta(partition, SnapshotRecord{
                               .kind = kind == RecordKind::kValue
                                           ? SnapshotRecord::Kind::kValue
                                           : SnapshotRecord::Kind::kDelete,
                               .db_id = db_id,
                               .db_epoch = DbEpoch(db_id),
                               .mutation_sequence = mutation_sequence,
                               .expire_at_ms = expire_at_ms,
                               .value_type = value_type,
                               .key = std::string(key),
                               .value = std::string(value),
                           });
  }
  co_return status;
}

void StorageEngine::Impl::AppendDelta(WorkerStore::PartitionStore& partition,
                                      SnapshotRecord record) {
  constexpr std::size_t kMaxRetainedMutations = 65536;
  partition.deltas.push_back(std::move(record));
  if (!partition.delta_queued) {
    partition.delta_queued = true;
    (void)replication_ready_.enqueue(partition.id);
  }
  while (partition.deltas.size() > kMaxRetainedMutations) {
    partition.delta_floor = std::max(
        partition.delta_floor,
        partition.deltas.front().mutation_sequence);
    partition.deltas.pop_front();
  }
}

Task<Status> StorageEngine::Impl::FetchStandbyBlock(WorkerStore* store,
                                                    bool for_defrag) {
  auto allocated = co_await AllocateBlock(*store, for_defrag);
  store->standby_request_pending = false;
  if (allocated.ok()) {
    store->standby_block = *allocated;
    store->standby_error.reset();
  } else {
    store->standby_error = allocated.status();
  }
  store->standby_ready.NotifyAll(*store->worker);
  co_return allocated.ok() ? Status::Ok() : allocated.status();
}

void StorageEngine::Impl::RequestStandbyBlock(WorkerStore& store,
                                              bool for_defrag) {
  if (store.standby_block.has_value() || store.standby_request_pending ||
      store.write_failed ||
      shutdown_flush_requested_.load(std::memory_order_acquire)) {
    return;
  }
  store.standby_error.reset();
  store.standby_request_pending = true;
  if (for_defrag) {
    store.worker->SpawnBackground(FetchStandbyBlock(&store, true));
  } else {
    store.worker->Spawn(FetchStandbyBlock(&store, false));
  }
}

void StorageEngine::Impl::MaybePrefetchStandby(WorkerStore& store) {
  if (!store.active_block.has_value()) {
    return;
  }
  constexpr std::uint64_t kPrefetchNumerator = 3;
  constexpr std::uint64_t kPrefetchDenominator = 4;
  const std::uint64_t usable = kStorageBlockBytes - kBlockHeaderBytes;
  const std::uint64_t used =
      store.active_block->committed_bytes - kBlockHeaderBytes;
  if (used * kPrefetchDenominator >= usable * kPrefetchNumerator) {
    RequestStandbyBlock(store, false);
  }
}

Task<Status> StorageEngine::Impl::WaitForStandbyWithWriterUnlocked(
    WorkerStore& store, bool for_defrag) {
  RequestStandbyBlock(store, for_defrag);
  store.writer_mutex.Unlock(*store.worker);
  co_await store.standby_ready.Wait();
  co_await store.writer_mutex.Lock();
  if (store.standby_error.has_value()) {
    Status status = *store.standby_error;
    store.standby_error.reset();
    co_return status;
  }
  co_return Status::Ok();
}

Task<Status> StorageEngine::Impl::WaitForStandbyWithWriterLocked(
    WorkerStore& store, bool for_defrag) {
  RequestStandbyBlock(store, for_defrag);
  co_await store.standby_ready.Wait();
  if (store.standby_error.has_value()) {
    Status status = *store.standby_error;
    store.standby_error.reset();
    co_return status;
  }
  co_return Status::Ok();
}

Task<Status> StorageEngine::Impl::WriteRecordLocked(
    WorkerStore& store, std::uint8_t db_id, std::string_view key,
    std::string_view value, RecordKind kind, ValueType value_type,
    std::uint64_t expire_at_ms, const Digest& digest, std::uint64_t generation,
    std::uint64_t mutation_sequence, std::uint64_t relocation_sequence,
    bool for_defrag, bool unlock_writer_while_waiting, bool external,
    std::uint64_t logical_size,
    std::shared_ptr<const std::vector<ExtentRef>> extents) {
  if (store.write_failed ||
      epoch_metadata_failed_.load(std::memory_order_acquire)) {
    co_return Status(StatusCode::kFailedPrecondition,
                     "storage writer is stopped after an IO failure");
  }
  if (logical_size == std::numeric_limits<std::uint64_t>::max()) {
    logical_size = value.size();
  }
  if ((kind == RecordKind::kValue && value_type == ValueType::kNone) ||
      (kind == RecordKind::kTombstone &&
       (value_type != ValueType::kNone || expire_at_ms != 0 ||
        !value.empty() || logical_size != 0 || external)) ||
      (external &&
       (kind != RecordKind::kValue || value_type != ValueType::kString ||
        extents == nullptr || extents->empty()))) {
    co_return Status(StatusCode::kInvalidArgument,
                     "invalid value type or expiration metadata");
  }
  if (key.size() > MaxKeyBytes()) {
    co_return Status(StatusCode::kOutOfRange,
                     "key is too large for the on-disk record header");
  }
  const std::size_t record_header_bytes = RecordHeaderBytes(key.size());
  const std::size_t payload_bytes = value.size();
  const std::size_t total_disk_bytes = AlignRecord(
      record_header_bytes + payload_bytes);
  if (total_disk_bytes > kStorageBlockBytes - kBlockHeaderBytes ||
      total_disk_bytes > options_.buffers.write_buffer_bytes) {
    co_return Status(StatusCode::kOutOfRange,
                     "record payload does not fit an inline block");
  }

  // Logical partitions route keys, but physical append streams are per
  // worker. This keeps foreground writes local and bounds active 8 MiB
  // buffers by worker count rather than logical partition count.
  const celer::WorkerId writer_id = store.worker->id();
  auto& partition = PartitionForKey(store, key);
  auto& index = partition.indexes[db_id];
  const std::uint64_t lsn =
      next_lsn_.fetch_add(1, std::memory_order_relaxed);

  auto& active = store.active_block;
  if (!active.has_value() ||
      active->committed_bytes + total_disk_bytes > kStorageBlockBytes) {
    if (active.has_value()) {
      RequestFlush(store, active->block_id);
      active.reset();
    }
    while (!store.standby_block.has_value()) {
      Status standby = Status::Ok();
      if (unlock_writer_while_waiting) {
        standby =
            co_await WaitForStandbyWithWriterUnlocked(store, for_defrag);
      } else {
        standby = co_await WaitForStandbyWithWriterLocked(store,
                                                          for_defrag);
      }
      if (!standby.ok()) {
        co_return standby;
      }
      // Another writer may have installed an active block while this
      // coroutine had writer_mutex released. Reuse it instead of consuming
      // a second standby and overwriting that active block.
      if (active.has_value() &&
          active->committed_bytes + total_disk_bytes <=
              kStorageBlockBytes) {
        break;
      }
    }
    if (!active.has_value() ||
        active->committed_bytes + total_disk_bytes > kStorageBlockBytes) {
      if (active.has_value()) {
        RequestFlush(store, active->block_id);
        active.reset();
      }
      std::uint16_t write_buffer_id = 0;
      std::byte* heap_buffer = nullptr;
      if (!store.buffers.TryAcquireWriteBuffer(&write_buffer_id)) {
        if (!store.buffers.TryAcquireHeapWriteBuffer(&heap_buffer)) {
          co_return Status(StatusCode::kResourceExhausted,
                           "no registered or fallback write buffers");
        }
      }
      FixedBuffer staging_buffer =
          write_buffer_id != 0
              ? store.buffers.write_buffer(write_buffer_id)
              : FixedBuffer{.data = heap_buffer,
                            .size = options_.buffers.write_buffer_bytes,
                            .index = 0};
      if (staging_buffer.data == nullptr || staging_buffer.size == 0) {
        if (write_buffer_id != 0) {
          store.buffers.ReleaseWriteBuffer(write_buffer_id);
        } else {
          store.buffers.ReleaseHeapWriteBuffer(heap_buffer);
        }
        co_return Status(StatusCode::kInternal,
                         "active write staging allocation is invalid");
      }
      const ReservedBlock allocated = *store.standby_block;
      store.standby_block.reset();
      const std::uint64_t block_id = allocated.block_id;
      std::fill_n(staging_buffer.data, staging_buffer.size, std::byte{0});
      active = ActiveBlock{
          .block_id = block_id,
          .writer_id = writer_id,
          .layout_worker_count = worker_count_,
          .allocation_epoch = allocated.allocation_epoch,
          .committed_bytes = kBlockHeaderBytes,
          .record_count = 0,
          .max_lsn = 0,
          .write_buffer_id = write_buffer_id,
          .heap_buffer = heap_buffer,
          .heap_buffer_size = options_.buffers.write_buffer_bytes,
      };
      BlockState& state = CreateBlockState(store, block_id);
      state.writer_id = writer_id;
      state.layout_worker_count = worker_count_;
      state.allocation_epoch = active->allocation_epoch;
      state.committed_bytes = kBlockHeaderBytes;
      state.live_bytes = 0;
      state.pins = 0;
      state.allocated = true;
      state.defragging = false;
      state.in_memory = true;
      state.flush_queued = false;
      state.flush_in_progress = false;
      state.staging_slot = AcquireStagingSlot(store);
      StagingSlot& staging_state = store.staging_slots[state.staging_slot];
      staging_state.write_buffer_id = write_buffer_id;
      staging_state.heap_data = heap_buffer;
      staging_state.heap_data_size = options_.buffers.write_buffer_bytes;
      store.staged_records.erase(block_id);

      // The header region stays zero in staging until a flush encodes it
      // into the slot it is about to write. Encoding it here, or on every
      // append, would race the flush that is reading the same page.
    }
  }

  // The writer mutex may have been released while waiting for a standby.
  // FLUSHDB or partition reset can replace the index state during that gap,
  // so capture the previous location only after the append stream is locked
  // again and an active block is available.
  auto* previous_entry = index.Find(digest, key);
  const std::optional<RecordLocation> previous =
      previous_entry == nullptr
          ? std::nullopt
          : std::optional<RecordLocation>(previous_entry->value);
  ActiveBlock updated = *active;
  const std::uint32_t record_offset = updated.committed_bytes;
  updated.committed_bytes += static_cast<std::uint32_t>(total_disk_bytes);
  ++updated.record_count;
  updated.max_lsn = std::max(updated.max_lsn, lsn);

  BlockState* state_ptr = FindBlockState(store, updated.block_id);
  if (state_ptr == nullptr) {
    co_return Status(StatusCode::kInternal,
                     "active block has no owner state");
  }
  BlockState& state = *state_ptr;
  StagingSlot* staging_state = StagingFor(store, state);
  if (staging_state == nullptr) {
    co_return Status(StatusCode::kInternal,
                     "active block has no staging slot");
  }
  FixedBuffer staging = updated.write_buffer_id != 0
                            ? store.buffers.write_buffer(updated.write_buffer_id)
                            : FixedBuffer{.data = updated.heap_buffer,
                                         .size = updated.heap_buffer_size,
                                         .index = 0};
  if (staging.data == nullptr ||
      record_offset + total_disk_bytes > staging.size) {
    co_return Status(StatusCode::kInternal, "invalid active staging block");
  }
  std::fill_n(staging.data + record_offset, total_disk_bytes, std::byte{0});
  RecordHeader record{
      .magic = kRecordMagic,
      .version = kStorageFormatVersion,
      .header_bytes = static_cast<std::uint16_t>(record_header_bytes),
      .kind = kind,
      .db_id = db_id,
      .value_type = value_type,
      .external = external,
      .digest = digest,
      .key_bytes = static_cast<std::uint32_t>(key.size()),
      .logical_size = logical_size,
      .payload_bytes = static_cast<std::uint32_t>(payload_bytes),
      .total_disk_bytes = static_cast<std::uint32_t>(total_disk_bytes),
      .generation = generation,
      .replication_epoch = partition.replication_epoch,
      .db_epoch = DbEpoch(db_id),
      .mutation_sequence = mutation_sequence,
      .relocation_sequence = relocation_sequence,
      .expire_at_ms = expire_at_ms,
      .lsn = lsn,
      .allocation_epoch = updated.allocation_epoch,
      .payload_checksum = Crc32c(std::span<const std::byte>(
          reinterpret_cast<const std::byte*>(value.data()), value.size())),
      .header_checksum = 0,
  };
  std::span<std::byte> record_output(staging.data + record_offset,
                                    record_header_bytes);
  if (!EncodeRecordHeader(record, key, record_output)) {
    co_return Status(StatusCode::kInternal, "record header encoding failed");
  }
  if (!value.empty()) {
    std::memcpy(staging.data + record_offset + record_header_bytes,
                value.data(), value.size());
  }

  // The staging slot already holds this block's buffer; it is fixed for the
  // life of the allocation, so only the flush counters need syncing below.
  if (updated.committed_bytes == kStorageBlockBytes) {
    state.in_memory = true;
    RequestFlush(store, updated.block_id);
    active.reset();
  } else {
    *active = updated;
  }

  const RecordLocation location{
      .block_id = updated.block_id,
      .replication_epoch = partition.replication_epoch,
      .mutation_sequence = mutation_sequence,
      .allocation_epoch = updated.allocation_epoch,
      .expire_at_ms = expire_at_ms,
      .block_owner = writer_id,
      .record_offset = record_offset,
      .total_disk_bytes = static_cast<std::uint32_t>(total_disk_bytes),
      .logical_size = logical_size,
      .payload_bytes = static_cast<std::uint32_t>(payload_bytes),
      .relocation_sequence = static_cast<std::uint32_t>(
          relocation_sequence),
      .in_memory = true,
      .external = external,
      .kind = kind,
      .value_type = value_type,
      .extents = std::move(extents),
  };
  const bool was_live =
      previous.has_value() && previous->kind == RecordKind::kValue;
  const bool is_live = kind == RecordKind::kValue;
  const bool was_expiring =
      was_live && previous->expire_at_ms != 0;
  const bool is_expiring = is_live && expire_at_ms != 0;
  auto inserted = index.InsertOrAssign(digest, key, location);
  store.staged_records[updated.block_id].push_back(RecordIdentity{
      .entry = inserted.entry,
      .retired_extents =
          !for_defrag && previous.has_value() && previous->external
              ? previous->extents
              : nullptr,
      .index_generation = store.index_generations[db_id],
      .db_id = db_id,
  });
  if (was_live != is_live) {
    if (is_live) {
      ++partition.live_key_count[db_id];
      ++store.live_key_count[db_id];
    } else {
      --partition.live_key_count[db_id];
      --store.live_key_count[db_id];
    }
  }
  if (was_expiring != is_expiring) {
    if (is_expiring) {
      ++partition.expiring_key_count[db_id];
    } else {
      --partition.expiring_key_count[db_id];
    }
  }
  state.committed_bytes = updated.committed_bytes;
  state.in_memory = true;
  staging_state->record_count = updated.record_count;
  staging_state->max_lsn = updated.max_lsn;
  state.live_bytes += location.total_disk_bytes;
  state.flush_queued = updated.committed_bytes == kStorageBlockBytes;
  if (previous.has_value()) {
    Status dead = co_await MarkRecordDead(*previous);
    if (!dead.ok()) {
      store.write_failed = true;
      co_return dead;
    }
  }
  if (!for_defrag && previous.has_value() && previous->external) {
    RequestFlush(store, updated.block_id);
    if (store.active_block.has_value() &&
        store.active_block->block_id == updated.block_id) {
      store.active_block.reset();
    }
  }
  MaybePrefetchStandby(store);
  co_return Status::Ok();
}

void StorageEngine::Impl::SealActiveBlocks(WorkerStore& store) {
  if (store.active_block.has_value() &&
      store.active_block->committed_bytes > kBlockHeaderBytes) {
    RequestFlush(store, store.active_block->block_id);
    store.active_block.reset();
  }
}

void StorageEngine::Impl::FlushActiveBlock(WorkerStore& store) {
  if (store.active_block.has_value() &&
      store.active_block->committed_bytes > kBlockHeaderBytes) {
    RequestFlush(store, store.active_block->block_id);
  }
}

void StorageEngine::Impl::SealDeadActiveBlock(WorkerStore& store) {
  if (!store.active_block.has_value()) {
    return;
  }
  BlockState* state = FindBlockState(store, store.active_block->block_id);
  if (state == nullptr || state->live_bytes != 0) {
    return;
  }
  RequestFlush(store, store.active_block->block_id);
  store.active_block.reset();
}

}  // namespace keylane::storage
