#include "absl/strings/str_cat.h"
#include "impl.h"

namespace keylane::storage {

namespace {

ExtentManifest ExtentsNotReferencedBy(ExtentManifest previous,
                                      ExtentManifest replacement) {
  if (previous == nullptr || previous->empty()) return {};
  if (replacement == nullptr || replacement->empty()) return previous;
  auto retired = std::make_shared<std::vector<ExtentRef>>();
  for (const ExtentRef& old : *previous) {
    const bool reused = std::any_of(
        replacement->begin(), replacement->end(), [&](const ExtentRef& next) {
          return old.block_id_ == next.block_id_ &&
                 old.allocation_epoch_ == next.allocation_epoch_;
        });
    if (!reused) retired->push_back(old);
  }
  if (retired->empty()) return {};
  return std::shared_ptr<const std::vector<ExtentRef>>(std::move(retired));
}

}  // namespace

Task<absl::StatusOr<SetResult>> StorageEngine::Impl::Set(
    std::uint8_t db_id, std::string_view key, std::string_view value,
    SetOptions options, ReplicationCommandAppend* replication,
    SetLatencyTrace* trace) {
  assert(db_id < kLogicalDatabaseCount);
  const Digest digest = ComputeDigest(key);
  if (trace != nullptr) trace->key_lock_start_ns_ = SetTraceNowNanos();
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest), tx::LockMode::kExclusive);
  if (trace != nullptr) trace->key_lock_acquired_ns_ = SetTraceNowNanos();
  co_return co_await SetLocked(db_id, key, digest, value, options, nullptr,
                               replication, trace);
}

Task<absl::StatusOr<SetResult>> StorageEngine::Impl::SetLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    std::string_view value, SetOptions options, TxShardWrites* tx,
    ReplicationCommandAppend* replication, SetLatencyTrace* trace) {
  assert(db_id < kLogicalDatabaseCount);
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionForKey(store, key);
  if (trace != nullptr) trace->store_lock_start_ns_ = SetTraceNowNanos();
  co_await store.store_state_mutex_.Lock();
  if (trace != nullptr) trace->store_lock_acquired_ns_ = SetTraceNowNanos();
  UnlockGuard unlock(&store.store_state_mutex_, store.worker_);

  auto& index = partition.indexes_[db_id];
  auto* found = index.Find(digest, key);
  if (found != nullptr && !found->key_complete()) [[unlikely]] {
    auto resolved = co_await FindVerifiedEntry(store, index, digest, key);
    if (!resolved.ok()) {
      co_return resolved.status();
    }
    found = *resolved;
  }
  const std::uint64_t now_ms = UnixTimeMillis();
  const bool exists = found != nullptr &&
                      found->value_.kind_ == RecordKind::kValue &&
                      !IsExpired(found->value_, now_ms);
  if (trace != nullptr) trace->lookup_done_ns_ = SetTraceNowNanos();
  SetResult result;
  if (options.return_old_value_ && exists) {
    if (found->value_.value_type_ != ValueType::kString) {
      co_return absl::Status(
          absl::StatusCode::kInvalidArgument,
          "WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    auto loaded = co_await LoadValue(store, partition, db_id, key, digest,
                                     found->value_, ExtentsFor(store, found));
    if (!loaded.ok()) {
      co_return loaded.status();
    }
    auto encoded = EncodeDiskValue(std::move(*loaded));
    if (!encoded.ok()) {
      co_return encoded.status();
    }
    result.old_value_.emplace(std::move(*encoded));
  }

  const bool condition_met =
      options.condition_ == SetCondition::kNone ||
      (options.condition_ == SetCondition::kIfAbsent && !exists) ||
      (options.condition_ == SetCondition::kIfPresent && exists);
  if (!condition_met) {
    co_return result;
  }

  const std::uint64_t expire_at_ms = options.keep_ttl_ && exists
                                         ? found->value_.expire_at_ms_
                                         : options.expire_at_ms_;
  if (replication != nullptr && expire_at_ms != 0) {
    replication->args_.emplace_back("PXAT");
    replication->args_.emplace_back(std::to_string(expire_at_ms));
  }
  if (trace != nullptr) trace->append_start_ns_ = SetTraceNowNanos();
  absl::Status status = co_await AppendLocked(
      store, partition, db_id, key, value, RecordKind::kValue,
      ValueType::kString, expire_at_ms, tx,
      std::numeric_limits<std::uint64_t>::max(), nullptr, nullptr, replication,
      trace);
  if (trace != nullptr) trace->append_done_ns_ = SetTraceNowNanos();
  if (!status.ok()) co_return status;
  result.applied_ = true;
  if (trace != nullptr) trace->replication_done_ns_ = SetTraceNowNanos();
  co_return result;
}

Task<absl::StatusOr<bool>> StorageEngine::Impl::UpdateExpiration(
    std::uint8_t db_id, std::string_view key, std::uint64_t expire_at_ms,
    ExpirationCondition condition, ReplicationCommandAppend* replication) {
  assert(db_id < kLogicalDatabaseCount);
  const Digest digest = ComputeDigest(key);
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest), tx::LockMode::kExclusive);
  co_return co_await UpdateExpirationLocked(db_id, key, digest, expire_at_ms,
                                            condition, nullptr, replication);
}

Task<absl::StatusOr<bool>> StorageEngine::Impl::UpdateExpirationLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    std::uint64_t expire_at_ms, ExpirationCondition condition,
    TxShardWrites* tx, ReplicationCommandAppend* replication) {
  assert(db_id < kLogicalDatabaseCount);
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionForKey(store, key);
  co_await store.store_state_mutex_.Lock();
  UnlockGuard unlock(&store.store_state_mutex_, store.worker_);

  auto& index = partition.indexes_[db_id];
  auto* found = index.Find(digest, key);
  if (found != nullptr && !found->key_complete()) [[unlikely]] {
    auto resolved = co_await FindVerifiedEntry(store, index, digest, key);
    if (!resolved.ok()) {
      co_return resolved.status();
    }
    found = *resolved;
  }
  const std::uint64_t now_ms = UnixTimeMillis();
  if (found == nullptr || found->value_.kind_ != RecordKind::kValue ||
      IsExpired(found->value_, now_ms)) {
    co_return false;
  }
  const std::uint64_t current = found->value_.expire_at_ms_;
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
        ValueType::kNone, 0, tx, 0, nullptr, nullptr, replication);
    if (!status.ok()) co_return status;
    co_return true;
  }

  const RecordLocation previous = found->value_;
  auto loaded = co_await LoadValue(store, partition, db_id, key, digest,
                                   previous, ExtentsFor(store, found));
  if (!loaded.ok()) {
    co_return loaded.status();
  }
  const std::span<const std::byte> value_bytes = loaded->value();
  std::string_view value(reinterpret_cast<const char*>(value_bytes.data()),
                         value_bytes.size());
  absl::Status status = co_await AppendLocked(
      store, partition, db_id, key, value, RecordKind::kValue,
      previous.value_type_, expire_at_ms, tx, previous.logical_size_, nullptr,
      nullptr, replication);
  if (!status.ok()) {
    co_return status;
  }
  co_return true;
}

Task<absl::StatusOr<bool>> StorageEngine::Impl::Delete(
    std::uint8_t db_id, std::string_view key,
    ReplicationCommandAppend* replication) {
  assert(db_id < kLogicalDatabaseCount);
  const Digest digest = ComputeDigest(key);
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest), tx::LockMode::kExclusive);
  co_return co_await DeleteLocked(db_id, key, digest, nullptr, replication);
}

Task<absl::StatusOr<bool>> StorageEngine::Impl::DeleteLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    TxShardWrites* tx, ReplicationCommandAppend* replication) {
  assert(db_id < kLogicalDatabaseCount);
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionForKey(store, key);
  co_await store.store_state_mutex_.Lock();
  UnlockGuard unlock(&store.store_state_mutex_, store.worker_);

  auto& index = partition.indexes_[db_id];
  auto* found = index.Find(digest, key);
  if (found != nullptr && !found->key_complete()) [[unlikely]] {
    auto resolved = co_await FindVerifiedEntry(store, index, digest, key);
    if (!resolved.ok()) {
      co_return resolved.status();
    }
    found = *resolved;
  }
  if (found == nullptr || found->value_.kind_ == RecordKind::kTombstone) {
    co_return false;
  }
  const bool expired = IsExpired(found->value_, UnixTimeMillis());
  absl::Status status = co_await AppendLocked(
      store, partition, db_id, key, {}, RecordKind::kTombstone,
      ValueType::kNone, 0, tx, 0, nullptr, nullptr, replication);
  if (!status.ok()) co_return status;
  co_return !expired;
}

Task<absl::Status> StorageEngine::Impl::WriteRawValueLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const RawValue& value, TxShardWrites* tx,
    ReplicationCommandAppend* replication) {
  assert(db_id < kLogicalDatabaseCount);
  if (digest != ComputeDigest(key)) {
    co_return absl::InvalidArgumentError("raw value digest mismatch");
  }
  if (value.value_type_ == ValueType::kNone) {
    co_return absl::InvalidArgumentError("raw value has no type");
  }
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionForKey(store, key);
  co_await store.store_state_mutex_.Lock();
  UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
  co_return co_await AppendLocked(store, partition, db_id, key, value.encoded_,
                                  RecordKind::kValue, value.value_type_,
                                  value.expire_at_ms_, tx, value.logical_size_,
                                  nullptr, nullptr, replication);
}

StagingSlot* StorageEngine::Impl::StagingFor(WorkerStore& store,
                                             const BlockState& state) {
  return state.staging_slot_ == 0 ? nullptr
                                  : &store.staging_slots_[state.staging_slot_];
}

std::uint16_t StorageEngine::Impl::AcquireStagingSlot(WorkerStore& store) {
  if (store.free_staging_slot_ != 0) {
    const std::uint16_t id = store.free_staging_slot_;
    store.free_staging_slot_ = store.staging_slots_[id].next_free_;
    store.staging_slots_[id] = StagingSlot{};
    return id;
  }
  store.staging_slots_.emplace_back();
  return static_cast<std::uint16_t>(store.staging_slots_.size() - 1);
}

FixedBuffer StorageEngine::Impl::StagingBufferFor(
    WorkerStore& store, const BlockState& state) const {
  const StagingSlot* slot = StagingFor(store, state);
  if (slot == nullptr) {
    return FixedBuffer{};
  }
  if (slot->write_buffer_id_ != 0) {
    return store.buffers_.write_buffer(slot->write_buffer_id_);
  }
  return FixedBuffer{
      .data_ = slot->heap_data_, .size_ = slot->heap_data_size_, .index_ = 0};
}

void StorageEngine::Impl::ReleaseStagingBuffer(WorkerStore& store,
                                               BlockState& state) {
  if (StagingSlot* slot = StagingFor(store, state); slot != nullptr) {
    if (slot->write_buffer_id_ != 0) {
      store.buffers_.ReleaseWriteBuffer(slot->write_buffer_id_);
    } else if (slot->heap_data_ != nullptr) {
      store.buffers_.ReleaseHeapWriteBuffer(slot->heap_data_);
    }
    *slot = StagingSlot{};
    slot->next_free_ = store.free_staging_slot_;
    store.free_staging_slot_ = state.staging_slot_;
    state.staging_slot_ = 0;
  }
  state.release_pending_ = false;
  state.in_memory_ = false;
}

absl::Status StorageEngine::Impl::MarkRecordDeadLocal(
    unsigned owner, const RetiredRecord& record) {
  WorkerStore& store = *stores_[owner];
  BlockState* state = FindBlockState(store, record.block_id_);
  if (state == nullptr || !state->allocated_ ||
      state->allocation_epoch_ != record.allocation_epoch_) {
    return absl::Status(absl::StatusCode::kInternal,
                        "stale block owner while invalidating record");
  }
  // live_bytes is the byte sum over exactly the index entries naming this
  // block at this allocation epoch, so the entry being retired here is one of
  // the summands and the subtraction cannot underflow. Saturating instead of
  // reporting would drive live_bytes to zero while records are still
  // reachable, which CleanBlockLocked now reads as "nothing to salvage" and
  // frees without inspecting the block. Fail loudly rather than lose data.
  if (state->live_bytes_ < record.total_disk_bytes_) {
    return absl::Status(
        absl::StatusCode::kInternal,
        absl::StrCat(
            "block live-byte accounting underflow: block=", record.block_id_,
            " live=", state->live_bytes_, " retire=", record.total_disk_bytes_,
            " epoch=", record.allocation_epoch_));
  }
  if (record.dependent_extents_ != nullptr) [[unlikely]] {
    store.deferred_dependent_extent_reclaims_[record.block_id_].push_back(
        record.dependent_extents_);
  }
  if (record.extra_dependent_extents_ != nullptr) [[unlikely]] {
    auto& deferred =
        store.deferred_dependent_extent_reclaims_[record.block_id_];
    deferred.insert(deferred.end(), record.extra_dependent_extents_->begin(),
                    record.extra_dependent_extents_->end());
  }
  if (record.immediate_extents_ != nullptr) [[unlikely]] {
    SpawnExtentReclaim(store, record.immediate_extents_);
  }
  state->live_bytes_ -= record.total_disk_bytes_;
  MaybeQueueDefrag(store, record.block_id_);
  return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::MarkRecordDead(
    const RetiredRecord& record) {
  assert(record.block_owner_ < worker_count_);
  const unsigned owner = record.block_owner_;
  if (owner == celer::ThisWorker().id_) {
    co_return MarkRecordDeadLocal(owner, record);
  }
  co_return co_await celer::SubmitTo(owner, [this, owner, record] {
    return MarkRecordDeadLocal(owner, record);
  });
}

Task<absl::Status> StorageEngine::Impl::CommitTxWrites(
    std::uint64_t txid, std::vector<TxShardWrites*> shards) {
  // The commit record must land strictly after every tagged data record is
  // durable: recovery treats "commit without data" as impossible, and
  // "data without commit" as an aborted transaction.
  auto retirements = std::make_unique<std::vector<RetiredRecord>>();
  for (TxShardWrites* shard : shards) {
    if (shard == nullptr) {
      continue;
    }
    for (const TxShardWrites::Fence& fence : shard->fences_) {
      absl::Status durable =
          co_await AwaitRelocationDurable(RelocationDurabilityFence{
              .block_id_ = fence.block_id_,
              .allocation_epoch_ = fence.allocation_epoch_,
              .block_owner_ = fence.block_owner_,
              .committed_bytes_ = fence.committed_bytes_,
          });
      if (!durable.ok()) {
        // No commit: recovery aborts the transaction. The routed
        // retirements never fire, so the superseded copies stay accounted —
        // a leak on an already fail-stopped path, never a loss.
        co_return durable;
      }
    }
    for (const TxShardWrites::Retired& retired : shard->retirements_) {
      retirements->push_back(RetiredRecord{
          .block_id_ = retired.block_id_,
          .allocation_epoch_ = retired.allocation_epoch_,
          .total_disk_bytes_ = retired.total_disk_bytes_,
          .block_owner_ = retired.block_owner_,
          .record_offset_ = retired.record_offset_,
          .dependent_extents_ = retired.dependent_extents_,
          .immediate_extents_ = retired.immediate_extents_,
          .extra_dependent_extents_ = nullptr,
      });
    }
  }
  // Every tagged record is durable; the transaction's fate now rests solely
  // on the commit record. Crash-safety tests arm this point to prove the
  // all-or-nothing promise: dying here must abort the whole transaction.
  KEYLANE_MAYBE_CRASH_AT("tx-commit-append");
  WorkerStore& store = CurrentStore();
  co_await store.store_state_mutex_.Lock();
  UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
  RecordLocation commit_location;
  absl::Status written = co_await WriteRecordLocked(
      store, 0, {}, {}, RecordKind::kTxCommit, ValueType::kNone, 0,
      ComputeDigest({}), txid, 0, false, true, false, false,
      std::numeric_limits<std::uint64_t>::max(), nullptr, &commit_location,
      nullptr, nullptr, std::move(retirements));
  if (!written.ok()) {
    co_return written;
  }
  // Nudge the commit's own block so the decision becomes durable promptly
  // instead of waiting out the periodic flush: until it lands, a crash
  // drops the whole (acknowledged but never durability-promised)
  // transaction.
  RequestFlush(store, commit_location.block_id_);
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::RollbackTxLocal(
    std::uint64_t txid, TxShardWrites* compensation) {
  WorkerStore& store = CurrentStore();
  co_await store.store_state_mutex_.Lock();
  UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
  auto found = store.tx_undo_.find(txid);
  if (found == store.tx_undo_.end()) {
    co_return absl::OkStatus();
  }
  std::vector<TxUndoEntry> undo = std::move(found->second);
  store.tx_undo_.erase(found);
  // Reverse order: a key written twice in one transaction unwinds through
  // its intermediate version back to the original.
  for (auto it = undo.rbegin(); it != undo.rend(); ++it) {
    TxUndoEntry& entry = *it;
    const RecordLocation applied = entry.entry_->value_;
    std::string loaded_key;
    if (!entry.entry_->key_complete()) [[unlikely]] {
      auto key = co_await LoadOutOfIndexKey(store, entry.entry_->value_,
                                            ExtentsFor(store, entry.entry_),
                                            entry.entry_->logical_key_size());
      if (!key.ok()) {
        store.write_failed_ = true;
        co_return key.status();
      }
      loaded_key = std::move(*key);
    }
    const std::string_view undo_key = entry.entry_->key_complete()
                                          ? entry.entry_->key()
                                          : std::string_view(loaded_key);
    auto& partition = PartitionForKey(store, undo_key);
    if (compensation != nullptr) {
      // Do not merely rewind the in-memory index: EXEC will later commit this
      // txid, so recovery would accept the failed half-write again. Append a
      // later record in the same transaction that represents the restored
      // state. Its normal retirement receipts also make every intermediate
      // record safe to reclaim after the outer commit becomes durable.
      std::string_view restored_payload;
      std::optional<LoadedValue> restored;
      if (entry.previous_.has_value() &&
          entry.previous_->kind_ == RecordKind::kValue) {
        auto loaded = co_await LoadValue(
            store, partition, entry.db_id_, undo_key,
            ComputeDigest(undo_key), *entry.previous_, entry.previous_extents_);
        if (!loaded.ok()) {
          store.write_failed_ = true;
          co_return loaded.status();
        }
        restored.emplace(std::move(*loaded));
        const auto bytes = restored->value();
        restored_payload = std::string_view(
            reinterpret_cast<const char*>(bytes.data()), bytes.size());
      }
      const RecordKind restored_kind =
          entry.previous_.has_value() ? entry.previous_->kind_
                                      : RecordKind::kTombstone;
      const ValueType restored_type =
          restored_kind == RecordKind::kValue
              ? entry.previous_->value_type_
              : ValueType::kNone;
      const std::uint64_t restored_expiry =
          restored_kind == RecordKind::kValue
              ? entry.previous_->expire_at_ms_
              : 0;
      const std::uint64_t restored_size =
          restored_kind == RecordKind::kValue
              ? entry.previous_->logical_size_
              : 0;
      absl::Status appended = co_await AppendLocked(
          store, partition, entry.db_id_, undo_key, restored_payload,
          restored_kind, restored_type, restored_expiry, compensation,
          restored_size);
      if (!appended.ok()) {
        store.write_failed_ = true;
        co_return appended;
      }
      continue;
    }
    if (!entry.previous_.has_value()) {
      // The key did not exist: a normal tombstone append restores absence
      // with every side effect handled (accounting, watchers, and the
      // replica delta that supersedes the aborted value).
      absl::Status tombstone =
          co_await AppendLocked(store, partition, entry.db_id_, undo_key, {},
                                RecordKind::kTombstone, ValueType::kNone, 0);
      if (!tombstone.ok()) {
        store.write_failed_ = true;
        co_return tombstone;
      }
      continue;
    }
    // Mirror the append-time counter math in reverse.
    const ExtentManifest applied_extents = ExtentsFor(store, entry.entry_);
    const ExtentManifest applied_dependent_extents =
        DependentExtentsFor(store, entry.entry_);
    const bool applied_live = applied.kind_ == RecordKind::kValue;
    const bool restored_live = entry.previous_->kind_ == RecordKind::kValue;
    if (applied_live != restored_live) {
      if (restored_live) {
        ++partition.live_key_count_[entry.db_id_];
        ++store.live_key_count_[entry.db_id_];
      } else {
        --partition.live_key_count_[entry.db_id_];
        --store.live_key_count_[entry.db_id_];
      }
    }
    const bool applied_expiring = applied_live && applied.expire_at_ms_ != 0;
    const bool restored_expiring =
        restored_live && entry.previous_->expire_at_ms_ != 0;
    if (applied_expiring != restored_expiring) {
      if (restored_expiring) {
        ++partition.expiring_key_count_[entry.db_id_];
      } else {
        --partition.expiring_key_count_[entry.db_id_];
      }
    }
    entry.entry_->value_ = *entry.previous_;
    if (entry.previous_->external_) {
      store.external_manifests_.insert_or_assign(entry.entry_,
                                                 entry.previous_extents_);
    } else {
      store.external_manifests_.erase(entry.entry_);
    }
    if (applied.external_ && !applied.key_external_ &&
        applied_extents != nullptr) {
      SpawnExtentReclaim(store, ExtentsNotReferencedBy(
                                    applied_extents, entry.previous_extents_));
    }
    absl::Status dead = MarkRecordDeadLocal(
        store.worker_->id(),
        RetiredRecordOf(applied, applied_dependent_extents));
    if (!dead.ok()) {
      store.write_failed_ = true;
      co_return dead;
    }
    if (partition.capture_deltas_) {
      // The aborted value may already have shipped; there is no delta that
      // can express "go back", so force the replica to re-copy the
      // partition.
      assert(store.replication_delta_bytes_ >= partition.delta_bytes_);
      store.replication_delta_bytes_ -= partition.delta_bytes_;
      partition.deltas_.clear();
      partition.delta_bytes_ = 0;
      partition.delta_floor_ = partition.mutation_sequence_;
      partition.delta_overflow_ = true;
    }
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::DiscardTxUndoLocal(std::uint64_t txid) {
  WorkerStore& store = CurrentStore();
  co_await store.store_state_mutex_.Lock();
  UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
  store.tx_undo_.erase(txid);
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::MarkRetiredRecordsDead(
    WorkerStore* store, std::vector<RetiredRecord> records) {
  struct SettlementGuard {
    std::atomic<std::uint32_t>* active_;
    ~SettlementGuard() { active_->fetch_sub(1, std::memory_order_acq_rel); }
  } settlement{&active_settlements_};
  for (const RetiredRecord& record : records) {
    absl::Status dead = co_await MarkRecordDead(record);
    if (!dead.ok()) {
      // The inline path fails the client write on an accounting error; here
      // there is no client left to tell, so fail-stop the writer the same way
      // a flush IO error does.
      spdlog::error("retiring superseded record failed: {}", dead.message());
      store->write_failed_ = true;
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
    store.store_state_mutex_.Unlock(*store.worker_);
  }
  absl::StatusOr<ReservedBlock> allocated{
      absl::Status(absl::StatusCode::kUnavailable, "storage is shutting down")};
  // A writer racing shutdown must not park behind an allocation the shutdown
  // flush is waiting out; a dropped commit chain is simply discarded at
  // recovery (never half-kept). Defrag keeps allocating from its reserve.
  if (for_defrag ||
      !shutdown_flush_requested_.load(std::memory_order_acquire)) {
    allocated = co_await AllocateBlock(
        store, for_defrag ? AllocationPurpose::kDefrag
                          : AllocationPurpose::kForeground);
  }
  if (unlock_writer) {
    co_await store.store_state_mutex_.Lock();
  }
  if (allocated.ok() && store.write_failed_) {
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
Task<absl::Status> StorageEngine::Impl::ReturnReservedBlock(
    ReservedBlock block) {
  const std::size_t device_index = DeviceIndexForBlock(block.block_id_);
  co_return co_await celer::SubmitTaskTo(
      device_allocators_[device_index]->owner_,
      [this, device_index, block]() -> Task<absl::Status> {
        DeviceAllocator& allocator = *device_allocators_[device_index];
        co_await allocator.mutex_.Lock();
        UnlockGuard unlock(&allocator.mutex_,
                           stores_[allocator.owner_]->worker_);
        allocator.ready_blocks_.push_back(block.block_id_);
        co_return absl::OkStatus();
      });
}

Task<absl::StatusOr<std::shared_ptr<const std::vector<ExtentRef>>>>
StorageEngine::Impl::WriteExtentValueLocked(WorkerStore& store,
                                            std::string_view first,
                                            std::string_view second) {
  const std::uint64_t logical_bytes =
      static_cast<std::uint64_t>(first.size()) + second.size();
  if (logical_bytes == 0 || logical_bytes > kMaxRecordPayloadBytes) {
    co_return absl::Status(absl::StatusCode::kOutOfRange,
                           "record payload exceeds the 1 GiB limit");
  }
  auto refs = std::make_shared<std::vector<ExtentRef>>();
  refs->reserve((logical_bytes + kExtentPayloadBytes - 1) /
                kExtentPayloadBytes);
  auto reclaim_allocated = [&]() {
    if (!refs->empty()) {
      SpawnExtentReclaim(store,
                         std::shared_ptr<const std::vector<ExtentRef>>(refs));
    }
  };
  std::uint64_t payload_offset = 0;
  std::uint32_t extent_index = 0;
  while (payload_offset < logical_bytes) {
    auto reserved =
        co_await AcquireWriteBlock(store, false, /*unlock_writer=*/true);
    if (!reserved.ok()) {
      reclaim_allocated();
      co_return reserved.status();
    }
    const std::size_t payload_bytes =
        static_cast<std::size_t>(std::min<std::uint64_t>(
            kExtentPayloadBytes, logical_bytes - payload_offset));
    BlockState& state = CreateBlockState(store, reserved->block_id_);
    state.writer_id_ = store.worker_->id();
    state.layout_worker_count_ = worker_count_;
    state.allocation_epoch_ = reserved->allocation_epoch_;
    state.committed_bytes_ =
        static_cast<std::uint32_t>(kBlockHeaderBytes + payload_bytes);
    // An extent block is written whole right here and never enters the flush
    // queue, so it needs no staging slot.
    state.live_bytes_ = static_cast<std::uint32_t>(payload_bytes);
    state.allocated_ = true;
    state.kind_ = BlockKind::kPayloadExtent;
    refs->push_back(ExtentRef{
        .block_id_ = reserved->block_id_,
        .allocation_epoch_ = reserved->allocation_epoch_,
        .payload_bytes_ = static_cast<std::uint32_t>(payload_bytes),
        .payload_checksum_ = 0,
    });
    std::uint16_t write_buffer_id = 0;
    std::byte* heap_buffer = nullptr;
    if (!store.buffers_.TryAcquireWriteBuffer(&write_buffer_id) &&
        !store.buffers_.TryAcquireHeapWriteBuffer(&heap_buffer)) {
      reclaim_allocated();
      co_return absl::Status(absl::StatusCode::kResourceExhausted,
                             "no extent write buffer is available");
    }
    auto release_buffer = [&]() {
      if (write_buffer_id != 0) {
        store.buffers_.ReleaseWriteBuffer(write_buffer_id);
      } else {
        store.buffers_.ReleaseHeapWriteBuffer(heap_buffer);
      }
    };
    FixedBuffer staging =
        write_buffer_id != 0
            ? store.buffers_.write_buffer(write_buffer_id)
            : FixedBuffer{.data_ = heap_buffer,
                          .size_ = options_.buffers_.write_buffer_bytes_,
                          .index_ = 0};
    if (staging.data_ == nullptr || staging.size_ < kStorageBlockBytes) {
      release_buffer();
      reclaim_allocated();
      co_return absl::Status(absl::StatusCode::kInternal,
                             "extent staging buffer is smaller than a block");
    }
    std::fill_n(staging.data_, kStorageBlockBytes, std::byte{0});
    std::size_t copied = 0;
    while (copied < payload_bytes) {
      const std::uint64_t logical_offset = payload_offset + copied;
      const std::string_view source =
          logical_offset < first.size() ? first : second;
      const std::size_t source_offset =
          logical_offset < first.size()
              ? static_cast<std::size_t>(logical_offset)
              : static_cast<std::size_t>(logical_offset - first.size());
      const std::size_t chunk =
          std::min(payload_bytes - copied, source.size() - source_offset);
      std::memcpy(staging.data_ + kBlockHeaderBytes + copied,
                  source.data() + source_offset, chunk);
      copied += chunk;
    }
    const auto payload = std::span<const std::byte>(
        staging.data_ + kBlockHeaderBytes, payload_bytes);
    const std::uint32_t payload_checksum = Crc32c(payload);
    refs->back() = ExtentRef{
        .block_id_ = reserved->block_id_,
        .allocation_epoch_ = reserved->allocation_epoch_,
        .payload_bytes_ = static_cast<std::uint32_t>(payload_bytes),
        .payload_checksum_ = payload_checksum,
    };
    auto allocated_lsn = AllocateLsn(store);
    if (!allocated_lsn.ok()) {
      release_buffer();
      reclaim_allocated();
      co_return allocated_lsn.status();
    }
    BlockHeader header{
        .magic_ = kBlockMagic,
        .block_id_ = reserved->block_id_,
        .version_ = kStorageFormatVersion,
        .header_bytes_ = kBlockHeaderBytes,
        .block_bytes_ = kStorageBlockBytes,
        .writer_id_ = store.worker_->id(),
        .allocation_epoch_ = reserved->allocation_epoch_,
        .committed_bytes_ =
            static_cast<std::uint32_t>(kBlockHeaderBytes + payload_bytes),
        .record_count_ = 0,
        .max_lsn_ = *allocated_lsn,
        .header_sequence_ = 1,
        .checksum_ = 0,
        .layout_worker_count_ = worker_count_,
        .kind_ = BlockKind::kPayloadExtent,
        .reserved_ = {},
        .extent_index_ = extent_index,
        .extent_payload_bytes_ = static_cast<std::uint32_t>(payload_bytes),
        .extent_payload_checksum_ = payload_checksum,
    };
    EncodeBlockHeader(header, std::span<std::byte, kBlockHeaderSlotBytes>(
                                  staging.data_, kBlockHeaderSlotBytes));
    std::memset(staging.data_ + kBlockHeaderSlotBytes, 0,
                kBlockHeaderBytes - kBlockHeaderSlotBytes);
    const auto [file_id, block_offset] = FileOffset(reserved->block_id_);
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
          std::min(options_.flush_size_bytes_, write_bytes - offset);
      auto written = co_await WriteStorageBuffer(
          *store.worker_, store.files_[file_id],
          std::span<const std::byte>(staging.data_ + offset, chunk),
          write_buffer_id != 0 && store.buffers_.buffers_registered(), staging,
          block_offset + offset);
      if (!written.ok() || *written != chunk) {
        write_ok = false;
        write_status = written.ok() ? absl::Status(absl::StatusCode::kInternal,
                                                   "short extent block write")
                                    : written.status();
        break;
      }
      offset += chunk;
    }
    if (write_ok) {
      write_status =
          co_await celer::Fdatasync(*store.worker_, store.files_[file_id]);
    }
    if (write_status.ok()) {
      auto written = co_await WriteStorageBuffer(
          *store.worker_, store.files_[file_id],
          std::span<const std::byte>(staging.data_, kBlockHeaderSlotBytes),
          write_buffer_id != 0 && store.buffers_.buffers_registered(), staging,
          block_offset);
      if (!written.ok() || *written != kBlockHeaderSlotBytes) {
        write_status = written.ok() ? absl::Status(absl::StatusCode::kInternal,
                                                   "short extent header write")
                                    : written.status();
      }
    }
    if (write_status.ok()) {
      write_status =
          co_await celer::Fdatasync(*store.worker_, store.files_[file_id]);
    }
    release_buffer();
    if (!write_status.ok()) {
      store.write_failed_ = true;
      reclaim_allocated();
      co_return write_status;
    }
    payload_offset += payload_bytes;
    ++extent_index;
  }
  co_return std::shared_ptr<const std::vector<ExtentRef>>(std::move(refs));
}

Task<absl::Status> StorageEngine::Impl::AppendLocked(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, std::string_view value,
    RecordKind kind, ValueType value_type, std::uint64_t expire_at_ms,
    TxShardWrites* tx, std::uint64_t logical_size,
    std::unique_ptr<std::vector<RetiredRecord>> commit_retirements,
    std::uint64_t* committed_sequence,
    ReplicationCommandAppend* replication, SetLatencyTrace* trace) {
  if (logical_size == std::numeric_limits<std::uint64_t>::max()) {
    logical_size = value.size();
  }
  const Digest digest = ComputeDigest(key);
  // Every real keyspace modification funnels through here (client writes,
  // deletes, expiration rewrites, active expiry): invalidate watchers.
  tx::CurrentTxShard().MarkWatched(db_id, tx::FingerprintOf(digest));
  const std::uint64_t mutation_sequence = ++partition.mutation_sequence_;
  absl::Status status = absl::OkStatus();
  const bool key_external = key.size() > options_.inline_key_max_bytes_;
  const std::uint64_t logical_payload_bytes =
      static_cast<std::uint64_t>(value.size()) +
      (key_external ? key.size() : 0);
  const std::size_t inline_bytes =
      AlignRecord(RecordHeaderBytes(key.size(), key_external) +
                  static_cast<std::size_t>(logical_payload_bytes));
  if (inline_bytes > kStorageBlockBytes - kBlockHeaderBytes) [[unlikely]] {
    auto extents = co_await WriteExtentValueLocked(
        store, key_external ? key : std::string_view{}, value);
    if (!extents.ok()) {
      co_return extents.status();
    }
    const std::string manifest = EncodeManifest(**extents);
    status = co_await WriteRecordLocked(
        store, db_id, key, manifest, kind, value_type, expire_at_ms, digest,
        /*txid=*/0, mutation_sequence, false, true, true, key_external,
        logical_size, *extents, nullptr, nullptr, tx,
        std::move(commit_retirements), trace);
    if (!status.ok()) {
      store.worker_->Spawn(ReclaimExtents(&store, *extents));
    }
  } else {
    status = co_await WriteRecordLocked(
        store, db_id, key, value, kind, value_type, expire_at_ms, digest,
        /*txid=*/0, mutation_sequence, false, true, false, key_external,
        logical_size, nullptr, nullptr, nullptr, tx,
        std::move(commit_retirements), trace);
  }
  if (status.ok() && committed_sequence != nullptr) {
    *committed_sequence = mutation_sequence;
  }
  if (status.ok() && replication != nullptr) {
    replication->db_id_ = db_id;
    replication->partition_id_ = partition.id_;
    replication->partition_sequence_ = mutation_sequence;
    (void)TryEnqueueReplicationCommand(std::move(*replication));
  }
  if (status.ok() && partition.capture_deltas_) {
    std::string replicated_value(value);
    AppendDelta(store, partition, SnapshotRecord{
                               .kind_ = kind == RecordKind::kValue
                                            ? SnapshotRecord::Kind::kValue
                                            : SnapshotRecord::Kind::kDelete,
                               .db_id_ = db_id,
                               .db_epoch_ = DbEpoch(db_id),
                               .mutation_sequence_ = mutation_sequence,
                               .expire_at_ms_ = expire_at_ms,
                               .value_type_ = value_type,
                               .logical_size_ = logical_size,
                               .key_ = std::string(key),
                               .value_ = std::move(replicated_value),
                           });
  }
  co_return status;
}

void StorageEngine::Impl::AppendDelta(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    SnapshotRecord record) {
  constexpr std::size_t kMaxRetainedMutations = 65536;
  constexpr std::size_t kMaxRetainedBytesPerWorker = 64U * 1024U * 1024U;
  const std::size_t record_bytes =
      sizeof(SnapshotRecord) + record.key_.size() + record.value_.size();
  if (partition.delta_overflow_) return;
  if (record_bytes > kMaxRetainedBytesPerWorker ||
      store.replication_delta_bytes_ >
          kMaxRetainedBytesPerWorker - record_bytes) {
    // This compatibility queue is intentionally lossy at its hard bound. A
    // flow observing any affected partition must restart its full sync; the
    // primary write remains independent of replica speed.
    for (auto& candidate : store.partitions_) {
      if (!candidate.capture_deltas_) continue;
      candidate.delta_floor_ = candidate.mutation_sequence_;
      candidate.delta_bytes_ = 0;
      candidate.deltas_.clear();
      candidate.delta_overflow_ = true;
    }
    store.replication_delta_bytes_ = 0;
    return;
  }
  partition.delta_bytes_ += record_bytes;
  store.replication_delta_bytes_ += record_bytes;
  partition.deltas_.push_back(std::move(record));
  if (!partition.delta_queued_) {
    partition.delta_queued_ = true;
    (void)replication_ready_.enqueue(partition.id_);
  }
  while (partition.deltas_.size() > kMaxRetainedMutations) {
    partition.delta_floor_ = std::max(
        partition.delta_floor_, partition.deltas_.front().mutation_sequence_);
    const SnapshotRecord& removed = partition.deltas_.front();
    const std::size_t removed_bytes =
        sizeof(SnapshotRecord) + removed.key_.size() + removed.value_.size();
    assert(partition.delta_bytes_ >= removed_bytes);
    assert(store.replication_delta_bytes_ >= removed_bytes);
    partition.delta_bytes_ -= removed_bytes;
    store.replication_delta_bytes_ -= removed_bytes;
    partition.deltas_.pop_front();
  }
}

Task<absl::Status> StorageEngine::Impl::WriteRecordLocked(
    WorkerStore& store, std::uint8_t db_id, std::string_view key,
    std::string_view value, RecordKind kind, ValueType value_type,
    std::uint64_t expire_at_ms, const Digest& digest, std::uint64_t txid,
    std::uint64_t mutation_sequence, bool for_defrag,
    bool unlock_writer_while_waiting, bool external, bool key_external,
    std::uint64_t logical_size,
    std::shared_ptr<const std::vector<ExtentRef>> extents,
    RecordLocation* written_location, const RelocationSource* relocation,
    TxShardWrites* tx,
    std::unique_ptr<std::vector<RetiredRecord>> commit_retirements,
    SetLatencyTrace* trace) {
  if (store.write_failed_ ||
      epoch_metadata_failed_.load(std::memory_order_acquire)) {
    co_return absl::Status(absl::StatusCode::kFailedPrecondition,
                           "storage writer is stopped after an IO failure");
  }
  if (logical_size == std::numeric_limits<std::uint64_t>::max()) {
    logical_size = value.size();
  }
  if (tx != nullptr) {
    assert(tx->txid_ != 0);
    txid = tx->txid_;
    if (KEYLANE_MAYBE_FAIL_TX_WRITE(key)) {
      co_return absl::Status(absl::StatusCode::kInternal,
                             "injected transaction write fault");
    }
  }
  if ((kind == RecordKind::kValue && value_type == ValueType::kNone) ||
      (kind == RecordKind::kTombstone &&
       (value_type != ValueType::kNone || expire_at_ms != 0 ||
        logical_size != 0 || (!external && !value.empty()))) ||
      (external && (extents == nullptr || extents->empty() ||
                    (kind == RecordKind::kTombstone && !key_external))) ||
      (key_external && key.empty())) {
    co_return absl::Status(absl::StatusCode::kInvalidArgument,
                           "invalid value type or expiration metadata");
  }
  if (key.size() > MaxKeyBytes()) {
    co_return absl::Status(absl::StatusCode::kOutOfRange,
                           "key exceeds the Redis-compatible 512 MiB limit");
  }
  const bool invalid_logical_size =
      (value_type == ValueType::kString && logical_size > kMaxBitmapBytes) ||
      logical_size > std::numeric_limits<std::uint32_t>::max();
  const std::uint64_t key_prefix = key_external ? key.size() : 0;
  if (invalid_logical_size || value.size() > kMaxRecordPayloadBytes ||
      key_prefix > kMaxRecordPayloadBytes - value.size()) {
    co_return absl::Status(absl::StatusCode::kOutOfRange,
                           "record key and value exceed storage limits");
  }
  if (external) {
    std::uint64_t extent_bytes = 0;
    for (const ExtentRef& ref : *extents) {
      if (ref.payload_bytes_ == 0 ||
          ref.payload_bytes_ > kMaxRecordPayloadBytes - extent_bytes) {
        co_return absl::Status(absl::StatusCode::kInvalidArgument,
                               "invalid external payload manifest");
      }
      extent_bytes += ref.payload_bytes_;
    }
    const bool exact_extent_bytes =
        kind != RecordKind::kValue || value_type == ValueType::kString;
    if (extent_bytes < key_prefix ||
        (exact_extent_bytes && extent_bytes != key_prefix + logical_size)) {
      co_return absl::Status(absl::StatusCode::kInvalidArgument,
                             "external payload length mismatch");
    }
  } else if (kind == RecordKind::kValue && value_type == ValueType::kString &&
             value.size() != logical_size) {
    co_return absl::Status(absl::StatusCode::kInvalidArgument,
                           "inline string length mismatch");
  }
  const std::size_t record_header_bytes =
      RecordHeaderBytes(key.size(), key_external);
  const std::size_t payload_bytes =
      value.size() + (key_external && !external ? key.size() : 0);
  const std::size_t total_disk_bytes =
      AlignRecord(record_header_bytes + payload_bytes);
  if (total_disk_bytes > kStorageBlockBytes - kBlockHeaderBytes ||
      total_disk_bytes > options_.buffers_.write_buffer_bytes_) {
    co_return absl::Status(absl::StatusCode::kOutOfRange,
                           "record payload does not fit an inline block");
  }

  // Logical partitions route keys, but physical append streams are per
  // worker. This keeps foreground writes local and bounds active 8 MiB
  // buffers by worker count rather than logical partition count.
  const celer::WorkerId writer_id = store.worker_->id();
  // Commit records are keyless and belong to no partition: they append
  // wherever their coordinator runs, and recovery reads them independently
  // of any partition's epochs.
  WorkerStore::PartitionStore* partition_ptr =
      kind == RecordKind::kTxCommit ? nullptr : &PartitionForKey(store, key);
  RecordIndex* index_ptr =
      partition_ptr == nullptr ? nullptr : &partition_ptr->indexes_[db_id];
  auto allocated_lsn = AllocateLsn(store);
  if (!allocated_lsn.ok()) {
    co_return allocated_lsn.status();
  }
  const std::uint64_t lsn = *allocated_lsn;

  auto& active = store.active_block_;
  if (trace != nullptr) trace->block_wait_start_ns_ = SetTraceNowNanos();
  while (!active.has_value() ||
         active->committed_bytes_ + total_disk_bytes > kStorageBlockBytes) {
    if (trace != nullptr) trace->allocated_block_ = true;
    if (active.has_value()) {
      RequestFlush(store, active->block_id_);
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
      if (!store.buffers_.TryAcquireWriteBuffer(&write_buffer_id)) {
        if (!store.buffers_.TryAcquireHeapWriteBuffer(&heap_buffer)) {
          co_await ReturnReservedBlock(*allocated);
          co_return absl::Status(absl::StatusCode::kResourceExhausted,
                                 "no registered or fallback write buffers");
        }
      }
      FixedBuffer staging_buffer =
          write_buffer_id != 0
              ? store.buffers_.write_buffer(write_buffer_id)
              : FixedBuffer{.data_ = heap_buffer,
                            .size_ = options_.buffers_.write_buffer_bytes_,
                            .index_ = 0};
      if (staging_buffer.data_ == nullptr || staging_buffer.size_ == 0) {
        if (write_buffer_id != 0) {
          store.buffers_.ReleaseWriteBuffer(write_buffer_id);
        } else {
          store.buffers_.ReleaseHeapWriteBuffer(heap_buffer);
        }
        co_await ReturnReservedBlock(*allocated);
        co_return absl::Status(absl::StatusCode::kInternal,
                               "active write staging allocation is invalid");
      }
      const std::uint64_t block_id = allocated->block_id_;
      std::fill_n(staging_buffer.data_, staging_buffer.size_, std::byte{0});
      active = ActiveBlock{
          .block_id_ = block_id,
          .writer_id_ = writer_id,
          .layout_worker_count_ = worker_count_,
          .allocation_epoch_ = allocated->allocation_epoch_,
          .committed_bytes_ = kBlockHeaderBytes,
          .record_count_ = 0,
          .max_lsn_ = 0,
          .write_buffer_id_ = write_buffer_id,
          .heap_buffer_ = heap_buffer,
          .heap_buffer_size_ = options_.buffers_.write_buffer_bytes_,
      };
      BlockState& state = CreateBlockState(store, block_id);
      state.writer_id_ = writer_id;
      state.layout_worker_count_ = worker_count_;
      state.allocation_epoch_ = active->allocation_epoch_;
      state.committed_bytes_ = kBlockHeaderBytes;
      state.live_bytes_ = 0;
      state.pins_ = 0;
      state.allocated_ = true;
      state.defragging_ = false;
      state.in_memory_ = true;
      state.flush_queued_ = false;
      state.flush_in_progress_ = false;
      state.staging_slot_ = AcquireStagingSlot(store);
      StagingSlot& staging_state = store.staging_slots_[state.staging_slot_];
      staging_state.write_buffer_id_ = write_buffer_id;
      staging_state.heap_data_ = heap_buffer;
      staging_state.heap_data_size_ = options_.buffers_.write_buffer_bytes_;
      staging_state.committed_bytes_ = kBlockHeaderBytes;
      store.staged_records_.erase(block_id);

      // The header region stays zero in staging until a flush encodes it
      // into the slot it is about to write. Encoding it here, or on every
      // append, would race the flush that is reading the same page.
    }
  }
  if (trace != nullptr) trace->block_ready_ns_ = SetTraceNowNanos();

  // Block allocation may have released the store-state lock. A client write
  // can replace this key, or FLUSHDB/replica reset can replace its index,
  // during that gap. Capture and validate the current physical record only
  // after the append stream is locked again and an active block is available.
  RecordIndex::Entry* previous_entry = nullptr;
  if (index_ptr != nullptr) {
    previous_entry = index_ptr->Find(digest, key);
    if (previous_entry != nullptr && !previous_entry->key_complete())
        [[unlikely]] {
      auto resolved =
          co_await FindVerifiedEntry(store, *index_ptr, digest, key);
      if (!resolved.ok()) {
        co_return resolved.status();
      }
      previous_entry = *resolved;
    }
  }
  if (relocation != nullptr && partition_ptr != nullptr &&
      (DbEpoch(db_id) != relocation->db_epoch_ ||
       partition_ptr->replication_epoch_ != relocation->replication_epoch_ ||
       store.index_generations_[db_id] != relocation->index_generation_ ||
       previous_entry == nullptr ||
       !relocation->Matches(previous_entry->value_))) {
    co_return absl::Status(absl::StatusCode::kAborted,
                           "relocation source changed while waiting");
  }
  const std::optional<RecordLocation> previous =
      previous_entry == nullptr
          ? std::nullopt
          : std::optional<RecordLocation>(previous_entry->value_);
  const ExtentManifest previous_extents = ExtentsFor(store, previous_entry);
  const ExtentManifest retired_value_extents = ExtentsNotReferencedBy(
      previous_extents, external && !key_external ? extents : nullptr);
  const ExtentManifest previous_dependent_extents =
      DependentExtentsFor(store, previous_entry);
  ActiveBlock updated = *active;
  const std::uint32_t record_offset = updated.committed_bytes_;
  updated.committed_bytes_ += static_cast<std::uint32_t>(total_disk_bytes);
  ++updated.record_count_;
  updated.max_lsn_ = std::max(updated.max_lsn_, lsn);

  BlockState* state_ptr = FindBlockState(store, updated.block_id_);
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
  FixedBuffer staging =
      updated.write_buffer_id_ != 0
          ? store.buffers_.write_buffer(updated.write_buffer_id_)
          : FixedBuffer{.data_ = updated.heap_buffer_,
                        .size_ = updated.heap_buffer_size_,
                        .index_ = 0};
  if (staging.data_ == nullptr ||
      record_offset + total_disk_bytes > staging.size_) {
    co_return absl::Status(absl::StatusCode::kInternal,
                           "invalid active staging block");
  }
  std::fill_n(staging.data_ + record_offset, total_disk_bytes, std::byte{0});
  RecordHeader record{
      .magic_ = kRecordMagic,
      .version_ = kStorageFormatVersion,
      .header_bytes_ = static_cast<std::uint16_t>(record_header_bytes),
      .kind_ = kind,
      .db_id_ = db_id,
      .value_type_ = value_type,
      .external_ = external,
      .key_external_ = key_external,
      .digest_ = digest,
      .key_bytes_ = static_cast<std::uint32_t>(key.size()),
      .logical_size_ = static_cast<std::uint32_t>(logical_size),
      .payload_bytes_ = static_cast<std::uint32_t>(payload_bytes),
      .total_disk_bytes_ = static_cast<std::uint32_t>(total_disk_bytes),
      .txid_ = txid,
      .replication_epoch_ =
          partition_ptr == nullptr ? 1 : partition_ptr->replication_epoch_,
      // A relocation stamps the epoch its source was validated under, not a
      // fresh read: worker 0 publishes a FLUSHDB epoch concurrently, and a
      // fresh read here could adopt it mid-append — turning a record
      // recovery must drop into one it must keep.
      .db_epoch_ =
          relocation != nullptr ? relocation->db_epoch_ : DbEpoch(db_id),
      .mutation_sequence_ = mutation_sequence,
      .expire_at_ms_ = expire_at_ms,
      .lsn_ = lsn,
      .allocation_epoch_ = updated.allocation_epoch_,
      .payload_checksum_ = 0,
      .header_checksum_ = 0,
  };
  std::span<std::byte> record_output(staging.data_ + record_offset,
                                     record_header_bytes);
  std::byte* payload_output =
      staging.data_ + record_offset + record_header_bytes;
  if (key_external && !external) [[unlikely]] {
    std::memcpy(payload_output, key.data(), key.size());
    payload_output += key.size();
  }
  if (!value.empty()) {
    std::memcpy(payload_output, value.data(), value.size());
  }
  record.payload_checksum_ = Crc32c(std::span<const std::byte>(
      staging.data_ + record_offset + record_header_bytes, payload_bytes));
  if (!EncodeRecordHeader(record, key, record_output)) {
    co_return absl::Status(absl::StatusCode::kInternal,
                           "record checksum encoding failed");
  }
  if (trace != nullptr) trace->encode_done_ns_ = SetTraceNowNanos();

  // The staging slot already holds this block's buffer; it is fixed for the
  // life of the allocation, so only the flush counters need syncing below.
  if (updated.committed_bytes_ == kStorageBlockBytes) {
    state.in_memory_ = true;
    RequestFlush(store, updated.block_id_);
    active.reset();
  } else {
    *active = updated;
  }

  const RecordLocation location{
      .block_id_ = updated.block_id_,
      .mutation_sequence_ = mutation_sequence,
      .allocation_epoch_ = updated.allocation_epoch_,
      .expire_at_ms_ = expire_at_ms,
      .logical_size_ = static_cast<std::uint32_t>(logical_size),
      .record_offset_ = record_offset,
      .total_disk_bytes_ = static_cast<std::uint32_t>(total_disk_bytes),
      .block_owner_ = writer_id,
      .in_memory_ = true,
      .external_ = external,
      .key_external_ = key_external,
      // A relocation rewrites the same logical version, so it carries the
      // bit unchanged. A real overwrite shields what its predecessor was
      // shielding, plus the buried value itself — but only if that value
      // could outlive this record's own erasure deadline: a predecessor
      // whose expiry falls before it would already be self-suppressed by
      // its timestamp whenever this entry may be dropped. Records with no
      // deadline of their own (tombstones, TTL-less values) must judge
      // against now instead, since their successors' deadlines are unknown.
      .shielding_ = previous.has_value() &&
                    (relocation != nullptr
                         ? previous->shielding_
                         : (previous->shielding_ ||
                            (previous->kind_ == RecordKind::kValue &&
                             (previous->expire_at_ms_ == 0 ||
                              previous->expire_at_ms_ >
                                  std::max(expire_at_ms, UnixTimeMillis()))))),
      .kind_ = kind,
      .value_type_ = value_type,
  };
  const bool was_live =
      previous.has_value() && previous->kind_ == RecordKind::kValue;
  const bool is_live = kind == RecordKind::kValue;
  const bool was_expiring = was_live && previous->expire_at_ms_ != 0;
  const bool is_expiring = is_live && expire_at_ms != 0;
  RecordIndex::Entry* inserted_entry = nullptr;
  if (index_ptr != nullptr) {
    if (previous_entry != nullptr) {
      previous_entry->value_ = location;
      inserted_entry = previous_entry;
    } else {
      inserted_entry =
          index_ptr->InsertNew(digest, key, location, !key_external);
    }
    if (external) {
      store.external_manifests_.insert_or_assign(inserted_entry, extents);
    } else {
      store.external_manifests_.erase(inserted_entry);
    }
  }
  const bool route_to_commit = tx != nullptr && previous.has_value();
  const bool defer_defrag_retirement =
      for_defrag && commit_retirements != nullptr;
  store.staged_records_[updated.block_id_].push_back(RecordIdentity{
      .entry_ = inserted_entry,
      .retired_extents_ = (!for_defrag || defer_defrag_retirement) &&
                                  !route_to_commit && previous.has_value() &&
                                  previous->external_ &&
                                  !previous->key_external_
                              ? retired_value_extents
                              : nullptr,
      .retired_record_ = (!for_defrag || defer_defrag_retirement) &&
                                 !route_to_commit && previous.has_value()
                             ? std::optional<RetiredRecord>(RetiredRecordOf(
                                   *previous, previous_dependent_extents))
                             : std::nullopt,
      .tx_retirements_ = std::move(commit_retirements),
      .index_generation_ = store.index_generations_[db_id],
      .db_id_ = db_id,
  });
  if (tx != nullptr && tx->collect_undo_ && inserted_entry != nullptr) {
    store.tx_undo_[txid].push_back(TxUndoEntry{
        .entry_ = inserted_entry,
        .previous_ = previous,
        .previous_extents_ = previous_extents,
        .db_id_ = db_id,
    });
  }
  if (route_to_commit) {
    // The superseded version may only leave its block's accounting once the
    // commit record is durable — recovery drops uncommitted replacements and
    // must still find the old copy — so its retirement travels with the
    // transaction instead of this record's flush.
    tx->retirements_.push_back(TxShardWrites::Retired{
        .block_id_ = previous->block_id_,
        .allocation_epoch_ = previous->allocation_epoch_,
        .total_disk_bytes_ = previous->total_disk_bytes_,
        .block_owner_ = previous->block_owner_,
        .record_offset_ = previous->record_offset_,
        .dependent_extents_ =
            previous->key_external_ ? previous_dependent_extents : nullptr,
        .immediate_extents_ =
            previous->key_external_ ? nullptr : retired_value_extents,
    });
  }
  if (tx != nullptr) {
    const std::uint32_t staged_end =
        static_cast<std::uint32_t>(record_offset + total_disk_bytes);
    bool merged = false;
    for (TxShardWrites::Fence& fence : tx->fences_) {
      if (fence.block_id_ == updated.block_id_ &&
          fence.allocation_epoch_ == updated.allocation_epoch_) {
        fence.committed_bytes_ = std::max(fence.committed_bytes_, staged_end);
        merged = true;
        break;
      }
    }
    if (!merged) {
      tx->fences_.push_back(TxShardWrites::Fence{
          .block_id_ = updated.block_id_,
          .allocation_epoch_ = updated.allocation_epoch_,
          .committed_bytes_ = staged_end,
          .block_owner_ = writer_id,
      });
    }
  }
  if (was_live != is_live) {
    if (is_live) {
      ++partition_ptr->live_key_count_[db_id];
      ++store.live_key_count_[db_id];
    } else {
      --partition_ptr->live_key_count_[db_id];
      --store.live_key_count_[db_id];
    }
  }
  if (was_expiring != is_expiring) {
    if (is_expiring) {
      ++partition_ptr->expiring_key_count_[db_id];
    } else {
      --partition_ptr->expiring_key_count_[db_id];
    }
  }
  state.committed_bytes_ = updated.committed_bytes_;
  state.in_memory_ = true;
  staging_state->committed_bytes_ = updated.committed_bytes_;
  staging_state->record_count_ = updated.record_count_;
  staging_state->max_lsn_ = updated.max_lsn_;
  state.live_bytes_ += location.total_disk_bytes_;
  state.flush_queued_ = updated.committed_bytes_ == kStorageBlockBytes;
  // A superseded record stays in its block's live_bytes until this record's
  // flush completes (the RecordIdentity above carries it there): the old copy
  // is the key's only durable version until then, and retiring it now lets
  // its block reach zero and be durably freed ahead of the replacement — a
  // crash in that window destroys data that had already been made durable.
  // Defrag relocations keep the inline retirement: their source blocks are
  // protected by RelocationDurabilityFence, and the defrag pass needs the
  // decrement to observe the block emptying within the same pass.
  if (for_defrag && !defer_defrag_retirement && previous.has_value()) {
    absl::Status dead = co_await MarkRecordDead(RetiredRecordOf(*previous));
    if (!dead.ok()) {
      store.write_failed_ = true;
      co_return dead;
    }
  }
  if (!for_defrag && previous.has_value() && previous->external_) {
    RequestFlush(store, updated.block_id_);
    if (store.active_block_.has_value() &&
        store.active_block_->block_id_ == updated.block_id_) {
      store.active_block_.reset();
    }
  }
  if (written_location != nullptr) {
    *written_location = location;
  }
  if (trace != nullptr) trace->index_done_ns_ = SetTraceNowNanos();
  co_return absl::OkStatus();
}

void StorageEngine::Impl::SealActiveBlocks(WorkerStore& store) {
  if (store.active_block_.has_value() &&
      store.active_block_->committed_bytes_ > kBlockHeaderBytes) {
    RequestFlush(store, store.active_block_->block_id_);
    store.active_block_.reset();
  }
}

void StorageEngine::Impl::FlushActiveBlock(WorkerStore& store) {
  if (store.active_block_.has_value() &&
      store.active_block_->committed_bytes_ > kBlockHeaderBytes) {
    RequestFlush(store, store.active_block_->block_id_);
  }
}

void StorageEngine::Impl::SealDeadActiveBlock(WorkerStore& store) {
  if (!store.active_block_.has_value()) {
    return;
  }
  BlockState* state = FindBlockState(store, store.active_block_->block_id_);
  if (state == nullptr || state->live_bytes_ != 0) {
    return;
  }
  RequestFlush(store, store.active_block_->block_id_);
  store.active_block_.reset();
}

}  // namespace keylane::storage
