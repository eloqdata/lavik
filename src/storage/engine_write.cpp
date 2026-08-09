#include "engine_impl.h"

namespace keylane::storage {

Task<absl::StatusOr<SetResult>> StorageEngine::Impl::Set(std::uint8_t db_id,
                                                   std::string_view key,
                                                   std::string_view value,
                                                   SetOptions options) {
  assert(db_id < kLogicalDatabaseCount);
  const Digest digest = ComputeDigest(key);
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest), tx::LockMode::kExclusive);
  co_return co_await SetLocked(db_id, key, digest, value, options);
}

Task<absl::StatusOr<SetResult>> StorageEngine::Impl::SetLocked(std::uint8_t db_id,
                                                         std::string_view key,
                                                         const Digest& digest,
                                                         std::string_view value,
                                                         SetOptions options,
                                                         TxShardWrites* tx) {
  assert(db_id < kLogicalDatabaseCount);
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionForKey(store, key);
  co_await store.store_state_mutex.Lock();
  UnlockGuard unlock(&store.store_state_mutex, store.worker);

  auto& index = partition.indexes[db_id];
  auto* found = index.Find(digest, key);
  const std::uint64_t now_ms = UnixTimeMillis();
  const bool exists = found != nullptr &&
                      found->value.kind == RecordKind::kValue &&
                      !IsExpired(found->value, now_ms);
  SetResult result;
  if (options.return_old_value && exists) {
    if (found->value.value_type != ValueType::kString) {
      co_return absl::Status(absl::StatusCode::kInvalidArgument,
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
  absl::Status status = co_await AppendLocked(
      store, partition, db_id, key, value, RecordKind::kValue,
      ValueType::kString, expire_at_ms, tx);
  if (!status.ok()) {
    co_return status;
  }
  result.applied = true;
  co_return result;
}

Task<absl::StatusOr<bool>> StorageEngine::Impl::UpdateExpiration(
    std::uint8_t db_id, std::string_view key, std::uint64_t expire_at_ms,
    ExpirationCondition condition) {
  assert(db_id < kLogicalDatabaseCount);
  const Digest digest = ComputeDigest(key);
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest), tx::LockMode::kExclusive);
  co_return co_await UpdateExpirationLocked(db_id, key, digest, expire_at_ms,
                                            condition);
}

Task<absl::StatusOr<bool>> StorageEngine::Impl::UpdateExpirationLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    std::uint64_t expire_at_ms, ExpirationCondition condition,
    TxShardWrites* tx) {
  assert(db_id < kLogicalDatabaseCount);
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionForKey(store, key);
  co_await store.store_state_mutex.Lock();
  UnlockGuard unlock(&store.store_state_mutex, store.worker);

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
    absl::Status status = co_await AppendLocked(
        store, partition, db_id, key, {}, RecordKind::kTombstone,
        ValueType::kNone, 0, tx);
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
  const std::span<const std::byte> value_bytes = loaded->value();
  std::string_view value(reinterpret_cast<const char*>(value_bytes.data()),
                         value_bytes.size());
  absl::Status status = co_await AppendLocked(
      store, partition, db_id, key, value, RecordKind::kValue,
      previous.value_type, expire_at_ms, tx);
  if (!status.ok()) {
    co_return status;
  }
  co_return true;
}

Task<absl::StatusOr<bool>> StorageEngine::Impl::Delete(std::uint8_t db_id,
                                                 std::string_view key) {
  assert(db_id < kLogicalDatabaseCount);
  const Digest digest = ComputeDigest(key);
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest), tx::LockMode::kExclusive);
  co_return co_await DeleteLocked(db_id, key, digest);
}

Task<absl::StatusOr<bool>> StorageEngine::Impl::DeleteLocked(std::uint8_t db_id,
                                                       std::string_view key,
                                                       const Digest& digest,
                                                       TxShardWrites* tx) {
  assert(db_id < kLogicalDatabaseCount);
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionForKey(store, key);
  co_await store.store_state_mutex.Lock();
  UnlockGuard unlock(&store.store_state_mutex, store.worker);

  auto& index = partition.indexes[db_id];
  auto* found = index.Find(digest, key);
  if (found == nullptr || found->value.kind == RecordKind::kTombstone) {
    co_return false;
  }
  const bool expired = IsExpired(found->value, UnixTimeMillis());
  absl::Status status =
      co_await AppendLocked(store, partition, db_id, key, {},
                            RecordKind::kTombstone, ValueType::kNone, 0, tx);
  if (!status.ok()) {
    co_return status;
  }
  co_return !expired;
}

Task<absl::StatusOr<std::int64_t>> StorageEngine::Impl::Increment(std::uint8_t db_id,
                                                            std::string_view key) {
  assert(db_id < kLogicalDatabaseCount);
  const Digest digest = ComputeDigest(key);
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest), tx::LockMode::kExclusive);
  co_return co_await IncrementLocked(db_id, key, digest);
}

Task<absl::StatusOr<std::int64_t>> StorageEngine::Impl::IncrementLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    TxShardWrites* tx) {
  assert(db_id < kLogicalDatabaseCount);
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionForKey(store, key);
  co_await store.store_state_mutex.Lock();
  UnlockGuard unlock(&store.store_state_mutex, store.worker);

  std::int64_t value = 0;
  std::uint64_t expire_at_ms = 0;
  auto& index = partition.indexes[db_id];
  auto* found = index.Find(digest, key);
  const bool exists = found != nullptr &&
                      found->value.kind == RecordKind::kValue &&
                      !IsExpired(found->value, UnixTimeMillis());
  if (exists) {
    if (found->value.value_type != ValueType::kString) {
      co_return absl::Status(absl::StatusCode::kInvalidArgument,
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
      co_return absl::Status(absl::StatusCode::kInvalidArgument,
                       "value is not an integer or out of range");
    }
  }
  ++value;
  const std::string encoded = std::to_string(value);
  absl::Status status =
      co_await AppendLocked(store, partition, db_id, key, encoded,
                            RecordKind::kValue, ValueType::kString,
                            expire_at_ms, tx);
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

absl::Status StorageEngine::Impl::MarkRecordDeadLocal(unsigned owner,
                                                const RetiredRecord& record) {
  WorkerStore& store = *stores_[owner];
  BlockState* state = FindBlockState(store, record.block_id);
  if (state == nullptr || !state->allocated ||
      state->allocation_epoch != record.allocation_epoch) {
    return absl::Status(absl::StatusCode::kInternal,
                  "stale block owner while invalidating record");
  }
  // live_bytes is the byte sum over exactly the index entries naming this
  // block at this allocation epoch, so the entry being retired here is one of
  // the summands and the subtraction cannot underflow. Saturating instead of
  // reporting would drive live_bytes to zero while records are still
  // reachable, which CleanBlockLocked now reads as "nothing to salvage" and
  // frees without inspecting the block. Fail loudly rather than lose data.
  if (state->live_bytes < record.total_disk_bytes) {
    return absl::Status(absl::StatusCode::kInternal,
                  "block live-byte accounting underflow");
  }
  state->live_bytes -= record.total_disk_bytes;
  MaybeQueueDefrag(store, record.block_id);
  return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::MarkRecordDead(const RetiredRecord& record) {
  assert(record.block_owner < worker_count_);
  const unsigned owner = record.block_owner;
  if (owner == celer::ThisWorker().id) {
    co_return MarkRecordDeadLocal(owner, record);
  }
  co_return co_await celer::SubmitTo(
      owner, [this, owner, record] {
        return MarkRecordDeadLocal(owner, record);
      });
}

Task<absl::Status> StorageEngine::Impl::CommitTxWrites(
    std::uint64_t txid, std::vector<TxShardWrites*> shards) {
  // The commit record must land strictly after every tagged data record is
  // durable: recovery treats "commit without data" as impossible, and
  // "data without commit" as an aborted transaction.
  auto retirements = std::make_shared<std::vector<RetiredRecord>>();
  for (TxShardWrites* shard : shards) {
    if (shard == nullptr) {
      continue;
    }
    for (const TxShardWrites::Fence& fence : shard->fences) {
      absl::Status durable = co_await AwaitRelocationDurable(RelocationDurabilityFence{
          .block_id = fence.block_id,
          .allocation_epoch = fence.allocation_epoch,
          .block_owner = fence.block_owner,
          .committed_bytes = fence.committed_bytes,
      });
      if (!durable.ok()) {
        // No commit: recovery aborts the transaction. The routed
        // retirements never fire, so the superseded copies stay accounted —
        // a leak on an already fail-stopped path, never a loss.
        co_return durable;
      }
    }
    for (const TxShardWrites::Retired& retired : shard->retirements) {
      retirements->push_back(RetiredRecord{
          .block_id = retired.block_id,
          .allocation_epoch = retired.allocation_epoch,
          .total_disk_bytes = retired.total_disk_bytes,
          .block_owner = retired.block_owner,
      });
    }
  }
  // Every tagged record is durable; the transaction's fate now rests solely
  // on the commit record. Crash-safety tests arm this point to prove the
  // all-or-nothing promise: dying here must abort the whole transaction.
  KEYLANE_MAYBE_CRASH_AT("tx-commit-append");
  WorkerStore& store = CurrentStore();
  co_await store.store_state_mutex.Lock();
  UnlockGuard unlock(&store.store_state_mutex, store.worker);
  RecordLocation commit_location;
  absl::Status written = co_await WriteRecordLocked(
      store, 0, {}, {}, RecordKind::kTxCommit, ValueType::kNone, 0,
      ComputeDigest({}), txid, 0, 0, false, true, false,
      std::numeric_limits<std::uint64_t>::max(), nullptr, &commit_location,
      nullptr, nullptr, std::move(retirements));
  if (!written.ok()) {
    co_return written;
  }
  // Nudge the commit's own block so the decision becomes durable promptly
  // instead of waiting out the periodic flush: until it lands, a crash
  // drops the whole (acknowledged but never durability-promised)
  // transaction.
  RequestFlush(store, commit_location.block_id);
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::RollbackTxLocal(std::uint64_t txid) {
  WorkerStore& store = CurrentStore();
  co_await store.store_state_mutex.Lock();
  UnlockGuard unlock(&store.store_state_mutex, store.worker);
  auto found = store.tx_undo.find(txid);
  if (found == store.tx_undo.end()) {
    co_return absl::OkStatus();
  }
  std::vector<TxUndoEntry> undo = std::move(found->second);
  store.tx_undo.erase(found);
  // Reverse order: a key written twice in one transaction unwinds through
  // its intermediate version back to the original.
  for (auto it = undo.rbegin(); it != undo.rend(); ++it) {
    TxUndoEntry& entry = *it;
    const RecordLocation applied = entry.entry->value;
    auto& partition = PartitionForKey(store, entry.entry->key);
    if (!entry.previous.has_value()) {
      // The key did not exist: a normal tombstone append restores absence
      // with every side effect handled (accounting, watchers, and the
      // replica delta that supersedes the aborted value).
      absl::Status tombstone = co_await AppendLocked(
          store, partition, entry.db_id, entry.entry->key, {},
          RecordKind::kTombstone, ValueType::kNone, 0);
      if (!tombstone.ok()) {
        store.write_failed = true;
        co_return tombstone;
      }
      continue;
    }
    // Mirror the append-time counter math in reverse.
    const bool applied_live = applied.kind == RecordKind::kValue;
    const bool restored_live = entry.previous->kind == RecordKind::kValue;
    if (applied_live != restored_live) {
      if (restored_live) {
        ++partition.live_key_count[entry.db_id];
        ++store.live_key_count[entry.db_id];
      } else {
        --partition.live_key_count[entry.db_id];
        --store.live_key_count[entry.db_id];
      }
    }
    const bool applied_expiring = applied_live && applied.expire_at_ms != 0;
    const bool restored_expiring =
        restored_live && entry.previous->expire_at_ms != 0;
    if (applied_expiring != restored_expiring) {
      if (restored_expiring) {
        ++partition.expiring_key_count[entry.db_id];
      } else {
        --partition.expiring_key_count[entry.db_id];
      }
    }
    entry.entry->value = *entry.previous;
    absl::Status dead = MarkRecordDeadLocal(store.worker->id(),
                                      RetiredRecordOf(applied));
    if (!dead.ok()) {
      store.write_failed = true;
      co_return dead;
    }
    if (partition.capture_deltas) {
      // The aborted value may already have shipped; there is no delta that
      // can express "go back", so force the replica to re-copy the
      // partition.
      partition.deltas.clear();
      partition.delta_floor = partition.mutation_sequence;
    }
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::DiscardTxUndoLocal(std::uint64_t txid) {
  WorkerStore& store = CurrentStore();
  co_await store.store_state_mutex.Lock();
  UnlockGuard unlock(&store.store_state_mutex, store.worker);
  store.tx_undo.erase(txid);
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::MarkRetiredRecordsDead(
    WorkerStore* store, std::vector<RetiredRecord> records) {
  struct SettlementGuard {
    std::atomic<std::uint32_t>* active;
    ~SettlementGuard() { active->fetch_sub(1, std::memory_order_acq_rel); }
  } settlement{&active_settlements_};
  for (const RetiredRecord& record : records) {
    absl::Status dead = co_await MarkRecordDead(record);
    if (!dead.ok()) {
      // The inline path fails the client write on an accounting error; here
      // there is no client left to tell, so fail-stop the writer the same way
      // a flush IO error does.
      spdlog::error("retiring superseded record failed: {}", dead.message());
      store->write_failed = true;
      co_return dead;
    }
  }
  co_return absl::OkStatus();
}

// Allocates a block for this writer inline. `unlock_writer` releases the
// store-state mutex across the allocation so appends behind this one keep
// flowing; the caller must revalidate whatever it read before the call. Every
// refusal surfaces as an error to exactly this caller — waiters queue on
// mutexes end to end, so there is no notification to miss.
Task<absl::StatusOr<ReservedBlock>> StorageEngine::Impl::AcquireWriteBlock(
    WorkerStore& store, bool for_defrag, bool unlock_writer) {
  if (unlock_writer) {
    store.store_state_mutex.Unlock(*store.worker);
  }
  absl::StatusOr<ReservedBlock> allocated{
      absl::Status(absl::StatusCode::kUnavailable, "storage is shutting down")};
  // A writer racing shutdown must not park behind an allocation the shutdown
  // flush is waiting out; a dropped commit chain is simply discarded at
  // recovery (never half-kept). Defrag keeps allocating from its reserve.
  if (for_defrag ||
      !shutdown_flush_requested_.load(std::memory_order_acquire)) {
    allocated = co_await AllocateBlock(store, for_defrag);
  }
  if (unlock_writer) {
    co_await store.store_state_mutex.Lock();
  }
  if (allocated.ok() && store.write_failed) {
    // The writer fail-stopped while the allocation waited; report that
    // instead of appending into a stream that will never flush.
    co_await ReturnReservedBlock(*allocated);
    co_return absl::Status(absl::StatusCode::kFailedPrecondition,
                     "storage writer is stopped after an IO failure");
  }
  co_return allocated;
}

// Hands a reserved-but-unwritten block back to its device's ready pool. The
// allocation bit is already durably set, which is exactly the state pool
// entries are in; the next consumer stamps a fresh allocation epoch.
Task<absl::Status> StorageEngine::Impl::ReturnReservedBlock(ReservedBlock block) {
  const std::size_t device_index = DeviceIndexForBlock(block.block_id);
  co_return co_await celer::SubmitTaskTo(
      device_allocators_[device_index]->owner,
      [this, device_index, block]() -> Task<absl::Status> {
        DeviceAllocator& allocator = *device_allocators_[device_index];
        co_await allocator.mutex.Lock();
        UnlockGuard unlock(&allocator.mutex,
                           stores_[allocator.owner]->worker);
        allocator.ready_blocks.push_back(block.block_id);
        co_return absl::OkStatus();
      });
}

Task<absl::StatusOr<std::shared_ptr<const std::vector<ExtentRef>>>>
StorageEngine::Impl::WriteExtentValueLocked(WorkerStore& store,
                                            std::string_view value) {
  if (value.empty() || value.size() > kMaxStringBytes) {
    co_return absl::Status(absl::StatusCode::kOutOfRange,
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
    auto reserved =
        co_await AcquireWriteBlock(store, false, /*unlock_writer=*/true);
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
      co_return absl::Status(absl::StatusCode::kResourceExhausted,
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
      co_return absl::Status(absl::StatusCode::kInternal,
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
    absl::Status write_status = absl::OkStatus();
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
                           ? absl::Status(absl::StatusCode::kInternal,
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
                           ? absl::Status(absl::StatusCode::kInternal,
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

Task<absl::Status> StorageEngine::Impl::AppendLocked(WorkerStore& store,
                                               WorkerStore::PartitionStore& partition,
                                               std::uint8_t db_id,
                                               std::string_view key,
                                               std::string_view value,
                                               RecordKind kind,
                                               ValueType value_type,
                                               std::uint64_t expire_at_ms,
                                               TxShardWrites* tx) {
  const Digest digest = ComputeDigest(key);
  // Every real keyspace modification funnels through here (client writes,
  // deletes, expiration rewrites, active expiry): invalidate watchers.
  tx::CurrentTxShard().MarkWatched(db_id, tx::FingerprintOf(digest));
  const std::uint64_t mutation_sequence = ++partition.mutation_sequence;
  absl::Status status = absl::OkStatus();
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
        /*txid=*/0, mutation_sequence, 0, false, true, true,
        value.size(), *extents, nullptr, nullptr, tx);
    if (!status.ok()) {
      store.worker->Spawn(ReclaimExtents(&store, *extents));
    }
  } else {
    status = co_await WriteRecordLocked(
        store, db_id, key, value, kind, value_type, expire_at_ms, digest,
        /*txid=*/0, mutation_sequence, 0, false, true, false,
        std::numeric_limits<std::uint64_t>::max(), nullptr, nullptr, nullptr,
        tx);
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

Task<absl::Status> StorageEngine::Impl::WriteRecordLocked(
    WorkerStore& store, std::uint8_t db_id, std::string_view key,
    std::string_view value, RecordKind kind, ValueType value_type,
    std::uint64_t expire_at_ms, const Digest& digest, std::uint64_t txid,
    std::uint64_t mutation_sequence, std::uint64_t relocation_sequence,
    bool for_defrag, bool unlock_writer_while_waiting, bool external,
    std::uint64_t logical_size,
    std::shared_ptr<const std::vector<ExtentRef>> extents,
    RecordLocation* written_location, const RelocationSource* relocation,
    TxShardWrites* tx,
    std::shared_ptr<std::vector<RetiredRecord>> commit_retirements) {
  if (store.write_failed ||
      epoch_metadata_failed_.load(std::memory_order_acquire)) {
    co_return absl::Status(absl::StatusCode::kFailedPrecondition,
                     "storage writer is stopped after an IO failure");
  }
  if (logical_size == std::numeric_limits<std::uint64_t>::max()) {
    logical_size = value.size();
  }
  if (tx != nullptr) {
    assert(tx->txid != 0);
    txid = tx->txid;
    if (KEYLANE_MAYBE_FAIL_TX_WRITE(key)) {
      co_return absl::Status(absl::StatusCode::kInternal,
                       "injected transaction write fault");
    }
  }
  if ((kind == RecordKind::kValue && value_type == ValueType::kNone) ||
      (kind == RecordKind::kTombstone &&
       (value_type != ValueType::kNone || expire_at_ms != 0 ||
        !value.empty() || logical_size != 0 || external)) ||
      (external &&
       (kind != RecordKind::kValue || value_type != ValueType::kString ||
        extents == nullptr || extents->empty()))) {
    co_return absl::Status(absl::StatusCode::kInvalidArgument,
                     "invalid value type or expiration metadata");
  }
  if (key.size() > MaxKeyBytes()) {
    co_return absl::Status(absl::StatusCode::kOutOfRange,
                     "key is too large for the on-disk record header");
  }
  const std::size_t record_header_bytes = RecordHeaderBytes(key.size());
  const std::size_t payload_bytes = value.size();
  const std::size_t total_disk_bytes = AlignRecord(
      record_header_bytes + payload_bytes);
  if (total_disk_bytes > kStorageBlockBytes - kBlockHeaderBytes ||
      total_disk_bytes > options_.buffers.write_buffer_bytes) {
    co_return absl::Status(absl::StatusCode::kOutOfRange,
                     "record payload does not fit an inline block");
  }

  // Logical partitions route keys, but physical append streams are per
  // worker. This keeps foreground writes local and bounds active 8 MiB
  // buffers by worker count rather than logical partition count.
  const celer::WorkerId writer_id = store.worker->id();
  // Commit records are keyless and belong to no partition: they append
  // wherever their coordinator runs, and recovery reads them independently
  // of any partition's epochs.
  WorkerStore::PartitionStore* partition_ptr =
      kind == RecordKind::kTxCommit ? nullptr : &PartitionForKey(store, key);
  RecordIndex* index_ptr =
      partition_ptr == nullptr ? nullptr : &partition_ptr->indexes[db_id];
  const std::uint64_t lsn =
      next_lsn_.fetch_add(1, std::memory_order_relaxed);

  auto& active = store.active_block;
  while (!active.has_value() ||
         active->committed_bytes + total_disk_bytes > kStorageBlockBytes) {
    if (active.has_value()) {
      RequestFlush(store, active->block_id);
      active.reset();
    }
    auto allocated = co_await AcquireWriteBlock(store, for_defrag,
                                                unlock_writer_while_waiting);
    if (!allocated.ok()) {
      co_return allocated.status();
    }
    // Another writer may have installed an active block while this coroutine
    // had store_state_mutex released. Keep that one and hand the spare back to
    // the pool instead of overwriting it; the loop re-checks the fit.
    if (active.has_value()) {
      co_await ReturnReservedBlock(*allocated);
      continue;
    }
    {
      std::uint16_t write_buffer_id = 0;
      std::byte* heap_buffer = nullptr;
      if (!store.buffers.TryAcquireWriteBuffer(&write_buffer_id)) {
        if (!store.buffers.TryAcquireHeapWriteBuffer(&heap_buffer)) {
          co_await ReturnReservedBlock(*allocated);
          co_return absl::Status(absl::StatusCode::kResourceExhausted,
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
        co_await ReturnReservedBlock(*allocated);
        co_return absl::Status(absl::StatusCode::kInternal,
                         "active write staging allocation is invalid");
      }
      const std::uint64_t block_id = allocated->block_id;
      std::fill_n(staging_buffer.data, staging_buffer.size, std::byte{0});
      active = ActiveBlock{
          .block_id = block_id,
          .writer_id = writer_id,
          .layout_worker_count = worker_count_,
          .allocation_epoch = allocated->allocation_epoch,
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

  // Block allocation may have released the store-state lock. A client write
  // can replace this key, or FLUSHDB/replica reset can replace its index,
  // during that gap. Capture and validate the current physical record only
  // after the append stream is locked again and an active block is available.
  auto* previous_entry =
      index_ptr == nullptr ? nullptr : index_ptr->Find(digest, key);
  if (relocation != nullptr && partition_ptr != nullptr &&
      (DbEpoch(db_id) != relocation->db_epoch ||
       partition_ptr->replication_epoch != relocation->replication_epoch ||
       store.index_generations[db_id] != relocation->index_generation ||
       previous_entry == nullptr ||
       !relocation->Matches(previous_entry->value))) {
    co_return absl::Status(absl::StatusCode::kAborted,
                     "relocation source changed while waiting");
  }
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
    co_return absl::Status(absl::StatusCode::kInternal,
                     "active block has no owner state");
  }
  BlockState& state = *state_ptr;
  StagingSlot* staging_state = StagingFor(store, state);
  if (staging_state == nullptr) {
    co_return absl::Status(absl::StatusCode::kInternal,
                     "active block has no staging slot");
  }
  FixedBuffer staging = updated.write_buffer_id != 0
                            ? store.buffers.write_buffer(updated.write_buffer_id)
                            : FixedBuffer{.data = updated.heap_buffer,
                                         .size = updated.heap_buffer_size,
                                         .index = 0};
  if (staging.data == nullptr ||
      record_offset + total_disk_bytes > staging.size) {
    co_return absl::Status(absl::StatusCode::kInternal, "invalid active staging block");
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
      .txid = txid,
      .replication_epoch =
          partition_ptr == nullptr ? 1 : partition_ptr->replication_epoch,
      // A relocation stamps the epoch its source was validated under, not a
      // fresh read: worker 0 publishes a FLUSHDB epoch concurrently, and a
      // fresh read here could adopt it mid-append — turning a record
      // recovery must drop into one it must keep.
      .db_epoch = relocation != nullptr ? relocation->db_epoch
                                        : DbEpoch(db_id),
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
    co_return absl::Status(absl::StatusCode::kInternal, "record header encoding failed");
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
      .replication_epoch =
          partition_ptr == nullptr ? 1 : partition_ptr->replication_epoch,
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
      // A relocation rewrites the same logical version, so it carries the
      // bit unchanged. A real overwrite shields what its predecessor was
      // shielding, plus the buried value itself — but only if that value
      // could outlive this record's own erasure deadline: a predecessor
      // whose expiry falls before it would already be self-suppressed by
      // its timestamp whenever this entry may be dropped. Records with no
      // deadline of their own (tombstones, TTL-less values) must judge
      // against now instead, since their successors' deadlines are unknown.
      .shielding =
          previous.has_value() &&
          (relocation != nullptr
               ? previous->shielding
               : (previous->shielding ||
                  (previous->kind == RecordKind::kValue &&
                   (previous->expire_at_ms == 0 ||
                    previous->expire_at_ms >
                        std::max(expire_at_ms, UnixTimeMillis()))))),
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
  RecordIndex::Entry* inserted_entry = nullptr;
  if (index_ptr != nullptr) {
    inserted_entry = index_ptr->InsertOrAssign(digest, key, location).entry;
  }
  const bool route_to_commit = tx != nullptr && previous.has_value();
  store.staged_records[updated.block_id].push_back(RecordIdentity{
      .entry = inserted_entry,
      .retired_extents =
          !for_defrag && previous.has_value() && previous->external
              ? previous->extents
              : nullptr,
      .retired_record = !for_defrag && !route_to_commit && previous.has_value()
                            ? std::optional<RetiredRecord>(
                                  RetiredRecordOf(*previous))
                            : std::nullopt,
      .tx_retirements = std::move(commit_retirements),
      .index_generation = store.index_generations[db_id],
      .db_id = db_id,
  });
  if (tx != nullptr && tx->collect_undo && inserted_entry != nullptr) {
    store.tx_undo[txid].push_back(TxUndoEntry{
        .entry = inserted_entry,
        .previous = previous,
        .db_id = db_id,
    });
  }
  if (route_to_commit) {
    // The superseded version may only leave its block's accounting once the
    // commit record is durable — recovery drops uncommitted replacements and
    // must still find the old copy — so its retirement travels with the
    // transaction instead of this record's flush.
    tx->retirements.push_back(TxShardWrites::Retired{
        .block_id = previous->block_id,
        .allocation_epoch = previous->allocation_epoch,
        .total_disk_bytes = previous->total_disk_bytes,
        .block_owner = previous->block_owner,
    });
  }
  if (tx != nullptr) {
    const std::uint32_t staged_end = static_cast<std::uint32_t>(
        record_offset + total_disk_bytes);
    bool merged = false;
    for (TxShardWrites::Fence& fence : tx->fences) {
      if (fence.block_id == updated.block_id &&
          fence.allocation_epoch == updated.allocation_epoch) {
        fence.committed_bytes = std::max(fence.committed_bytes, staged_end);
        merged = true;
        break;
      }
    }
    if (!merged) {
      tx->fences.push_back(TxShardWrites::Fence{
          .block_id = updated.block_id,
          .allocation_epoch = updated.allocation_epoch,
          .committed_bytes = staged_end,
          .block_owner = writer_id,
      });
    }
  }
  if (was_live != is_live) {
    if (is_live) {
      ++partition_ptr->live_key_count[db_id];
      ++store.live_key_count[db_id];
    } else {
      --partition_ptr->live_key_count[db_id];
      --store.live_key_count[db_id];
    }
  }
  if (was_expiring != is_expiring) {
    if (is_expiring) {
      ++partition_ptr->expiring_key_count[db_id];
    } else {
      --partition_ptr->expiring_key_count[db_id];
    }
  }
  state.committed_bytes = updated.committed_bytes;
  state.in_memory = true;
  staging_state->record_count = updated.record_count;
  staging_state->max_lsn = updated.max_lsn;
  state.live_bytes += location.total_disk_bytes;
  state.flush_queued = updated.committed_bytes == kStorageBlockBytes;
  // A superseded record stays in its block's live_bytes until this record's
  // flush completes (the RecordIdentity above carries it there): the old copy
  // is the key's only durable version until then, and retiring it now lets
  // its block reach zero and be durably freed ahead of the replacement — a
  // crash in that window destroys data that had already been made durable.
  // Defrag relocations keep the inline retirement: their source blocks are
  // protected by RelocationDurabilityFence, and the defrag pass needs the
  // decrement to observe the block emptying within the same pass.
  if (for_defrag && previous.has_value()) {
    absl::Status dead = co_await MarkRecordDead(RetiredRecordOf(*previous));
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
  if (written_location != nullptr) {
    *written_location = location;
  }
  co_return absl::OkStatus();
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
