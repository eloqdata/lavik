#include "absl/strings/str_cat.h"
#include "impl.h"
#include "keylane/metrics.h"
#include "keylane/replication_command.h"

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
                      found->value_.kind() == RecordKind::kValue &&
                      !IsExpired(found->value_, now_ms);
  if (trace != nullptr) trace->lookup_done_ns_ = SetTraceNowNanos();
  SetResult result;
  if (options.return_old_value_ && exists) {
    if (found->value_.value_type() != ValueType::kString) {
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
      store, partition, db_id, key, digest, value, RecordKind::kValue,
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
  if (found == nullptr || found->value_.kind() != RecordKind::kValue ||
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
        store, partition, db_id, key, digest, {}, RecordKind::kTombstone,
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
      store, partition, db_id, key, digest, value, RecordKind::kValue,
      previous.value_type(), expire_at_ms, tx, previous.logical_size_, nullptr,
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
  if (found == nullptr || found->value_.kind() == RecordKind::kTombstone) {
    co_return false;
  }
  const bool expired = IsExpired(found->value_, UnixTimeMillis());
  absl::Status status = co_await AppendLocked(
      store, partition, db_id, key, digest, {}, RecordKind::kTombstone,
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
  co_return co_await AppendLocked(
      store, partition, db_id, key, digest, value.encoded_, RecordKind::kValue,
      value.value_type_, value.expire_at_ms_, tx, value.logical_size_, nullptr,
      nullptr, replication);
}

Task<absl::StatusOr<RestoreRawResult>> StorageEngine::Impl::RestoreRawValue(
    std::uint8_t db_id, std::string_view key, const RawValue& value,
    bool replace, ReplicationCommandAppend* replication) {
  assert(db_id < kLogicalDatabaseCount);
  const Digest digest = ComputeDigest(key);
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest), tx::LockMode::kExclusive);
  co_return co_await RestoreRawValueLocked(db_id, key, digest, value, replace,
                                           nullptr, replication);
}

Task<absl::StatusOr<RestoreRawResult>>
StorageEngine::Impl::RestoreRawValueLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const RawValue& value, bool replace, TxShardWrites* tx,
    ReplicationCommandAppend* replication) {
  const bool exists = co_await ExistsLocked(db_id, key, digest);
  if (exists && !replace) co_return RestoreRawResult{.busy_ = true};
  if (value.expire_at_ms_ != 0 && value.expire_at_ms_ <= UnixTimeMillis()) {
    if (!exists) co_return RestoreRawResult{};
    auto deleted = co_await DeleteLocked(db_id, key, digest, tx, replication);
    if (!deleted.ok()) co_return deleted.status();
    co_return RestoreRawResult{.changed_ = *deleted, .deleted_ = *deleted};
  }
  absl::Status written =
      co_await WriteRawValueLocked(db_id, key, digest, value, tx, replication);
  if (!written.ok()) co_return written;
  co_return RestoreRawResult{.changed_ = true};
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
  if (record.tx_tagged_) {
    DropTaggedRecordLocal(store, record.block_id_, record.allocation_epoch_,
                          record.total_disk_bytes_);
  }
  if (record.dependency_pinned_) {
    UnpinTxDependencyLocal(store, record.block_id_, record.allocation_epoch_);
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
  TxShardWrites* generation_receipt = nullptr;
  for (TxShardWrites* shard : shards) {
    if (shard == nullptr) {
      continue;
    }
    if (generation_receipt == nullptr) {
      generation_receipt = shard;
    } else if (shard->generation_ != generation_receipt->generation_) {
      co_return absl::InvalidArgumentError(
          "transaction shards span multiple generations");
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
          .tx_tagged_ = retired.tx_tagged_,
          .dependency_pinned_ = retired.dependency_pinned_,
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
      nullptr, generation_receipt, std::move(retirements));
  if (!written.ok()) {
    co_return written;
  }
  std::uint64_t dataset_changes = 0;
  for (const TxShardWrites* shard : shards) {
    if (shard != nullptr) dataset_changes += shard->dataset_changes_;
  }
  RecordDatasetChanges(dataset_changes);
  // Nudge the commit's own block so the decision becomes durable promptly
  // instead of waiting out the periodic flush: until it lands, a crash
  // drops the whole (acknowledged but never durability-promised)
  // transaction.
  RequestFlush(store, commit_location.block_id());
  co_return absl::OkStatus();
}

namespace {

constexpr std::size_t kTxCommitBatchSize = 256;

void MergeTxCommitFence(std::vector<RelocationDurabilityFence>* merged,
                        const TxShardWrites::Fence& fence) {
  for (RelocationDurabilityFence& existing : *merged) {
    if (existing.block_id_ == fence.block_id_ &&
        existing.allocation_epoch_ == fence.allocation_epoch_ &&
        existing.block_owner_ == fence.block_owner_) {
      existing.committed_bytes_ =
          std::max(existing.committed_bytes_, fence.committed_bytes_);
      return;
    }
  }
  merged->push_back(RelocationDurabilityFence{
      .block_id_ = fence.block_id_,
      .allocation_epoch_ = fence.allocation_epoch_,
      .block_owner_ = fence.block_owner_,
      .committed_bytes_ = fence.committed_bytes_,
  });
}

}  // namespace

bool StorageEngine::Impl::EnqueueTxCommit(std::uint64_t txid,
                                          std::vector<TxShardWrites> writes) {
  assert(txid != 0);
#ifndef NDEBUG
  for (const TxShardWrites& shard : writes) {
    // Command-local undo must be settled before ownership transfers to the
    // background coordinator. Otherwise an append could race rollback.
    assert(!shard.collect_undo_);
  }
#endif
  WorkerStore& store = CurrentStore();
  NoteTxCommitStarted();
  store.tx_commit_queue_.push_back(WorkerStore::PendingTxCommit{
      .txid_ = txid,
      .writes_ = std::move(writes),
  });
  const std::uint64_t depth =
      tx_commit_queue_depth_.fetch_add(1, std::memory_order_acq_rel) + 1;
  std::uint64_t peak = tx_commit_queue_peak_.load(std::memory_order_relaxed);
  while (depth > peak && !tx_commit_queue_peak_.compare_exchange_weak(
                             peak, depth, std::memory_order_release,
                             std::memory_order_relaxed)) {
  }
  if (!store.tx_commit_runner_) {
    store.tx_commit_runner_ = true;
    store.worker_->Spawn(DrainTxCommitQueue(&store));
  }
  if (store.tx_commit_queue_.size() < kTxCommitQueueHighWatermark) {
    return true;
  }
  tx_commit_backpressure_waits_.fetch_add(1, std::memory_order_relaxed);
  return false;
}

Task<absl::Status> StorageEngine::Impl::WaitForTxCommitCapacity() {
  WorkerStore& store = CurrentStore();
  while (store.tx_commit_queue_.size() >= kTxCommitQueueHighWatermark) {
    co_await store.tx_commit_capacity_.Wait();
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::DrainTxCommitQueue(WorkerStore* store) {
  while (!store->tx_commit_queue_.empty()) {
    std::vector<WorkerStore::PendingTxCommit> batch;
    const std::size_t count =
        std::min(kTxCommitBatchSize, store->tx_commit_queue_.size());
    batch.reserve(count);
    for (std::size_t index = 0; index < count; ++index) {
      batch.push_back(std::move(store->tx_commit_queue_.front()));
      store->tx_commit_queue_.pop_front();
    }
    tx_commit_queue_depth_.fetch_sub(count, std::memory_order_acq_rel);
    if (store->tx_commit_queue_.size() < kTxCommitQueueHighWatermark) {
      store->tx_commit_capacity_.NotifyAll(*store->worker_);
    }
    tx_commit_batches_.fetch_add(1, std::memory_order_relaxed);
    tx_commit_batch_transactions_.fetch_add(count, std::memory_order_relaxed);

    // Transactions sharing a participant's active transaction block also
    // share a durability frontier. Trigger every unique frontier before
    // awaiting any one of them, so all owner flushes make progress in
    // parallel without one detached waiter coroutine per transaction.
    std::vector<RelocationDurabilityFence> fences;
    std::uint64_t input_fences = 0;
    for (const WorkerStore::PendingTxCommit& pending : batch) {
      for (const TxShardWrites& shard : pending.writes_) {
        input_fences += shard.fences_.size();
        for (const TxShardWrites::Fence& fence : shard.fences_) {
          MergeTxCommitFence(&fences, fence);
        }
      }
    }
    tx_commit_input_fences_.fetch_add(input_fences, std::memory_order_relaxed);
    tx_commit_merged_fences_.fetch_add(fences.size(),
                                       std::memory_order_relaxed);

    absl::Status batch_status = absl::OkStatus();
    // Preserve the original one-transaction path exactly: opportunistic
    // batching must not add a dispatch round trip to an idle connection's
    // durability latency. With backlog, pre-arm every unique block so their
    // flushes overlap; each transaction below still awaits only its own
    // fences, never the slowest unrelated fence in the batch.
    if (batch.size() > 1) {
      for (const RelocationDurabilityFence& fence : fences) {
        if (fence.block_owner_ >= worker_count_) {
          batch_status = absl::InternalError(
              "transaction durability fence has an invalid block owner");
          break;
        }
        auto request = [this, fence]() -> Task<absl::Status> {
          WorkerStore& owner = *stores_[fence.block_owner_];
          co_await owner.store_state_mutex_.Lock();
          UnlockGuard unlock(&owner.store_state_mutex_, owner.worker_);
          BlockState* state = FindBlockState(owner, fence.block_id_);
          if (state != nullptr && state->allocated_ &&
              state->allocation_epoch_ == fence.allocation_epoch_) {
            RequestFlush(owner, fence.block_id_);
          }
          co_return owner.write_failed_
              ? absl::InternalError(
                    "storage write failed while starting "
                    "transaction batch flush")
              : absl::OkStatus();
        };
        absl::Status requested =
            fence.block_owner_ == celer::ThisWorker().id_
                ? co_await request()
                : co_await celer::SubmitTaskTo(fence.block_owner_,
                                               std::move(request));
        if (!requested.ok()) {
          batch_status = std::move(requested);
          break;
        }
      }
    }

    for (WorkerStore::PendingTxCommit& pending : batch) {
      if (batch_status.ok()) {
        std::vector<TxShardWrites*> shards;
        for (TxShardWrites& shard : pending.writes_) {
          if (!shard.fences_.empty() || !shard.retirements_.empty()) {
            shards.push_back(&shard);
          }
        }
        if (!shards.empty()) {
          absl::Status committed =
              co_await CommitTxWrites(pending.txid_, std::move(shards));
          if (!committed.ok()) {
            spdlog::warn("transaction {} commit append failed: {}",
                         pending.txid_, committed.message());
          }
        }
      } else {
        spdlog::warn("transaction {} batch durability failed: {}",
                     pending.txid_, batch_status.message());
      }
      NoteTxCommitFinished();
    }
  }
  store->tx_commit_runner_ = false;
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
    const Digest undo_digest = ComputeDigest(undo_key);
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
          entry.previous_->kind() == RecordKind::kValue) {
        auto loaded = co_await LoadValue(
            store, partition, entry.db_id_, undo_key, undo_digest,
            *entry.previous_, entry.previous_extents_);
        if (!loaded.ok()) {
          store.write_failed_ = true;
          co_return loaded.status();
        }
        restored.emplace(std::move(*loaded));
        const auto bytes = restored->value();
        restored_payload = std::string_view(
            reinterpret_cast<const char*>(bytes.data()), bytes.size());
      }
      const RecordKind restored_kind = entry.previous_.has_value()
                                           ? entry.previous_->kind()
                                           : RecordKind::kTombstone;
      const ValueType restored_type = restored_kind == RecordKind::kValue
                                          ? entry.previous_->value_type()
                                          : ValueType::kNone;
      const std::uint64_t restored_expiry = restored_kind == RecordKind::kValue
                                                ? entry.previous_->expire_at_ms_
                                                : 0;
      const std::uint64_t restored_size = restored_kind == RecordKind::kValue
                                              ? entry.previous_->logical_size_
                                              : 0;
      absl::Status appended = co_await AppendLocked(
          store, partition, entry.db_id_, undo_key, undo_digest,
          restored_payload, restored_kind, restored_type, restored_expiry,
          compensation, restored_size);
      if (!appended.ok()) {
        store.write_failed_ = true;
        co_return appended;
      }
      continue;
    }
    if (!entry.previous_.has_value()) {
      // The key did not exist: append a tombstone to restore runtime and
      // recovery state. The aborted transaction was never published to a
      // full-sync session, so this internal rollback must not publish either.
      absl::Status tombstone = co_await AppendLocked(
          store, partition, entry.db_id_, undo_key, undo_digest, {},
          RecordKind::kTombstone, ValueType::kNone, 0,
          /*tx=*/nullptr, /*logical_size=*/0,
          /*commit_retirements=*/nullptr, /*committed_sequence=*/nullptr,
          /*replication=*/nullptr, /*trace=*/nullptr,
          /*capture_fullsync=*/false);
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
    const bool applied_live = applied.kind() == RecordKind::kValue;
    const bool restored_live = entry.previous_->kind() == RecordKind::kValue;
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
    if (entry.previous_->external()) {
      store.external_manifests_.insert_or_assign(entry.entry_,
                                                 entry.previous_extents_);
    } else {
      store.external_manifests_.erase(entry.entry_);
    }
    if (applied.external() && !applied.key_external() &&
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
    UnpinTxDependencyLocal(store, entry.previous_->block_id(),
                           entry.previous_->allocation_epoch());
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
  // Keep one settlement coroutine per flush, not one child coroutine per
  // retired record. Local records settle synchronously after the flush lock
  // has been released; the exceptional remote records use SubmitTo directly
  // and retain their original order.
  for (const RetiredRecord& record : records) {
    assert(record.block_owner_ < worker_count_);
    const unsigned owner = record.block_owner_;
    absl::Status dead;
    if (owner == celer::ThisWorker().id_) {
      dead = MarkRecordDeadLocal(owner, record);
    } else {
      dead = co_await celer::SubmitTo(owner, [this, owner, record] {
        return MarkRecordDeadLocal(owner, record);
      });
    }
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
#ifndef NDEBUG
  // Deterministically expose the active_tx_blocks_ rehash window: another
  // transaction generation may install its append stream while this writer
  // owns no store-state lock. Only the first foreground allocation pauses.
  static std::atomic<bool> tx_active_pause_claimed = false;
  const char* tx_active_pause_text =
      std::getenv("KEYLANE_TX_ACTIVE_BLOCK_PAUSE_MS");
  bool expected_tx_active_pause = false;
  if (!for_defrag && unlock_writer && tx_active_pause_text != nullptr &&
      tx_active_pause_claimed.compare_exchange_strong(
          expected_tx_active_pause, true, std::memory_order_acq_rel)) {
    char* end = nullptr;
    const unsigned long pause_ms = std::strtoul(tx_active_pause_text, &end, 10);
    if (end != tx_active_pause_text && *end == '\0' && pause_ms != 0) {
      // Test-only observability: e2e fixtures poll the server log for this
      // marker to confirm the pause is actually in effect instead of guessing
      // with sleeps.
      spdlog::warn(
          "KEYLANE_TX_ACTIVE_BLOCK_PAUSE_MS pausing foreground allocation "
          "for {} ms",
          pause_ms);
      absl::Status paused = co_await celer::SleepFor(
          *store.worker_, std::chrono::milliseconds(pause_ms));
      if (!paused.ok()) {
        co_await store.store_state_mutex_.Lock();
        co_return paused;
      }
    }
  }
#endif
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
    while (!store.buffers_.TryAcquireWriteBuffer(&write_buffer_id)) {
      // Extent construction is part of a foreground write. Do not let it
      // bypass the configured storage pool with an unbounded 8 MiB heap
      // allocation. The caller holds store_state_mutex_; release it so the
      // flush completion that returns a buffer can make progress.
      store.store_state_mutex_.Unlock(*store.worker_);
      co_await store.buffers_.WaitForWriteBuffer();
      co_await store.store_state_mutex_.Lock();
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
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    std::string_view value, RecordKind kind, ValueType value_type,
    std::uint64_t expire_at_ms, TxShardWrites* tx, std::uint64_t logical_size,
    std::unique_ptr<std::vector<RetiredRecord>> commit_retirements,
    std::uint64_t* committed_sequence, ReplicationCommandAppend* replication,
    SetLatencyTrace* trace, bool capture_fullsync) {
  if (logical_size == std::numeric_limits<std::uint64_t>::max()) {
    logical_size = value.size();
  }
  // Every real keyspace modification funnels through here (client writes,
  // deletes, expiration rewrites, active expiry): invalidate watchers.
  tx::CurrentTxShard().MarkWatched(db_id, tx::FingerprintOf(digest));
  std::optional<ExplicitWriteRoot> replica_write_root;
  std::optional<std::uint64_t> replica_mutation_sequence;
  if (replica_loading_.load(std::memory_order_acquire)) [[unlikely]] {
    auto* sync = partition.replica_sync_.get();
    if (sync == nullptr || !sync->command_sequence_.has_value()) {
      co_return absl::FailedPreconditionError(
          "replica command arrived outside its apply context");
    }
    replica_mutation_sequence = *sync->command_sequence_;
    replica_write_root.emplace(ExplicitWriteRoot{
        .index_ = &partition.indexes_[db_id],
        .live_key_count_ = &partition.live_key_count_[db_id],
        .store_live_key_count_ = &store.live_key_count_[db_id],
        .expiring_key_count_ = &partition.expiring_key_count_[db_id],
        .replication_epoch_ = sync->replication_epoch_,
        .db_epoch_ = sync->local_db_epochs_[db_id],
        .reject_older_sequence_ = true,
    });
  }
  const std::uint64_t mutation_sequence = replica_mutation_sequence.has_value()
                                              ? *replica_mutation_sequence
                                              : ++partition.mutation_sequence_;
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
        std::move(commit_retirements), trace,
        replica_write_root.has_value() ? &*replica_write_root : nullptr);
    if (!status.ok()) {
      store.worker_->Spawn(ReclaimExtents(&store, *extents));
    }
  } else {
    status = co_await WriteRecordLocked(
        store, db_id, key, value, kind, value_type, expire_at_ms, digest,
        /*txid=*/0, mutation_sequence, false, true, false, key_external,
        logical_size, nullptr, nullptr, nullptr, tx,
        std::move(commit_retirements), trace,
        replica_write_root.has_value() ? &*replica_write_root : nullptr);
  }
  if (status.ok() && committed_sequence != nullptr) {
    *committed_sequence = mutation_sequence;
  }
  if (status.ok() && replica_mutation_sequence.has_value()) {
    partition.mutation_sequence_ =
        std::max(partition.mutation_sequence_, mutation_sequence);
  }
  if (status.ok() && tx != nullptr) {
    ++tx->dataset_changes_;
    tx->expiration_effects_.push_back(TxShardWrites::ExpirationEffect{
        .key_ = std::string(key),
        .expire_at_ms_ = kind == RecordKind::kValue ? expire_at_ms : 0,
        .db_id_ = db_id,
        .exists_ = kind == RecordKind::kValue,
    });
  }
  if (status.ok() && tx == nullptr) {
    RecordDatasetChanges();
  }
  std::shared_ptr<const ReplicationCommandAppend> fullsync_command;
  if (status.ok() && replication != nullptr) {
    AppendReplicationExpirationEffect(
        &replication->args_, db_id, db_id, key, kind == RecordKind::kValue,
        kind == RecordKind::kValue ? expire_at_ms : 0);
    replication->db_id_ = db_id;
    replication->partition_id_ = partition.id_;
    replication->partition_sequence_ = mutation_sequence;
    if (!partition.fullsync_subscribers_.empty()) [[unlikely]] {
      fullsync_command =
          std::make_shared<const ReplicationCommandAppend>(*replication);
    }
    (void)TryEnqueueReplicationCommand(std::move(*replication));
  }
  if (status.ok() && capture_fullsync &&
      (tx != nullptr || !partition.fullsync_subscribers_.empty()))
      [[unlikely]] {
    auto make_effect = [&] {
      return SnapshotRecord{
          .kind_ = kind == RecordKind::kValue ? SnapshotRecord::Kind::kValue
                                              : SnapshotRecord::Kind::kDelete,
          .db_id_ = db_id,
          .db_epoch_ = DbEpoch(db_id),
          .mutation_sequence_ = mutation_sequence,
          .expire_at_ms_ = expire_at_ms,
          .value_type_ = value_type,
          .logical_size_ = logical_size,
          .key_ = std::string(key),
          .value_ = {},
      };
    };
    if (tx != nullptr) {
      std::vector<std::uint64_t> session_ids;
      session_ids.reserve(partition.fullsync_subscribers_.size());
      for (const auto& [session_id, capture] :
           partition.fullsync_subscribers_) {
        // Transaction events are not projected before FULLSYNC_CUT. Their
        // participant-local after-images remain captured through the final
        // transaction fence, including for partitions already tailing.
        (void)capture;
        session_ids.push_back(session_id);
      }
      if (!session_ids.empty()) {
        tx->fullsync_effects_.push_back(TxShardWrites::FullSyncEffect{
            .partition_id_ = partition.id_,
            .record_ = make_effect(),
            .session_ids_ = std::move(session_ids),
        });
      }
    } else {
      SnapshotRecord effect = make_effect();
      FullSyncOnCommit(store, partition, effect, digest,
                       std::move(fullsync_command));
    }
  }
  co_return status;
}

std::string StorageEngine::Impl::FullSyncOverrideKey(std::uint8_t db_id,
                                                     std::string_view key) {
  std::string result;
  result.reserve(key.size() + 1);
  result.push_back(static_cast<char>(db_id));
  result.append(key);
  return result;
}

void StorageEngine::Impl::ClearFullSyncCapture(
    WorkerStore& store, WorkerStore::FullSyncCapture& capture) {
  for (auto& [_, pinned] : capture.pinned_values_) {
    store.worker_->Spawn(ReleaseFullSyncExtents(std::move(pinned.extents_)));
  }
  capture.pinned_values_.clear();
  capture.overrides_.clear();
  capture.latest_by_key_.clear();
  capture.replacement_credit_bytes_ = 0;
  capture.key_phases_.Clear();
  capture.pending_snapshot_keys_.clear();
  capture.pending_snapshot_cursor_ = 0;
}

void StorageEngine::Impl::FullSyncOnCommit(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    const SnapshotRecord& record, const Digest& digest,
    std::shared_ptr<const ReplicationCommandAppend> command) {
  for (auto& [session_id, capture] : partition.fullsync_subscribers_) {
    FullSyncCaptureOnCommit(store, session_id, capture, record, digest,
                            command);
  }
}

void StorageEngine::Impl::FullSyncCaptureOnCommit(
    WorkerStore& store, std::uint64_t session_id,
    WorkerStore::FullSyncCapture& capture, const SnapshotRecord& record,
    const Digest& digest,
    std::shared_ptr<const ReplicationCommandAppend> command,
    bool transaction_effect) {
  const auto db_phase = capture.db_phases_[record.db_id_];
  if (db_phase == WorkerStore::FullSyncCapture::DbPhase::kUnstarted) {
    return;
  }
  auto* phase = capture.key_phases_.Find(digest, record.key_);
  const bool has_ordered_base =
      db_phase == WorkerStore::FullSyncCapture::DbPhase::kTailing ||
      phase != nullptr;
  if (has_ordered_base && (command != nullptr || transaction_effect)) {
    if (command != nullptr) {
      (void)TryEnqueueFullSyncCommand(store, session_id, std::move(command));
    } else {
      // Transaction participants have no independently replayable command.
      // Put their after-image identity in the same FIFO so a later ordinary
      // command can never overtake it.
      (void)TryEnqueueFullSyncRecord(store, session_id, record);
    }
    return;
  }
  // An unseen key, or a committed transaction participant for which there is
  // no independently replayable command, is represented by its latest
  // after-image. A later scanner observation skips this key; an older ACK can
  // never erase the newer sequence below.
  if (phase != nullptr) {
    capture.key_phases_.Erase(phase);
  }
  const std::string key = FullSyncOverrideKey(record.db_id_, record.key_);
  const bool replacing = capture.latest_by_key_.contains(key);
  if (auto found = capture.latest_by_key_.find(key);
      found != capture.latest_by_key_.end()) {
    auto previous = capture.overrides_.find(found->second);
    assert(previous != capture.overrides_.end());
    capture.overrides_.erase(previous);
    capture.latest_by_key_.erase(found);
  }
  if (!replacing) {
    auto session = store.fullsync_sessions_.find(session_id);
    std::size_t credit = kFullSyncReplacementMetadataBytes;
    if (record.key_.size() >
        (std::numeric_limits<std::size_t>::max() - credit) / 2) {
      if (session != store.fullsync_sessions_.end()) {
        session->second.db_epoch_invalidated_ = true;
      }
      store.fullsync_publisher_capacity_ready_.NotifyAll(*store.worker_);
      return;
    }
    credit += record.key_.size() * 2;
    if (session == store.fullsync_sessions_.end() ||
        credit > std::numeric_limits<std::size_t>::max() -
                     session->second.publish_queue_bytes_ ||
        credit > std::numeric_limits<std::size_t>::max() -
                     capture.replacement_credit_bytes_) {
      if (session != store.fullsync_sessions_.end()) {
        session->second.db_epoch_invalidated_ = true;
      }
      store.fullsync_publisher_capacity_ready_.NotifyAll(*store.worker_);
      return;
    }
    session->second.publish_queue_bytes_ += credit;
    capture.replacement_credit_bytes_ += credit;
  }
  capture.overrides_.emplace(record.mutation_sequence_, record);
  capture.latest_by_key_.emplace(std::move(key), record.mutation_sequence_);
}

bool StorageEngine::Impl::TryEnqueueFullSyncCommand(
    WorkerStore& store, std::uint64_t session_id,
    std::shared_ptr<const ReplicationCommandAppend> command) {
  auto session = store.fullsync_sessions_.find(session_id);
  if (session == store.fullsync_sessions_.end() ||
      session->second.db_epoch_invalidated_ || command == nullptr ||
      command->args_.empty()) {
    return false;
  }
  std::size_t logical_bytes = sizeof(command->kind_) + sizeof(command->db_id_) +
                              sizeof(command->partition_id_) +
                              sizeof(command->partition_sequence_);
  for (const std::string& arg : command->args_) {
    if (arg.size() > std::numeric_limits<std::size_t>::max() - logical_bytes ||
        sizeof(std::uint32_t) > std::numeric_limits<std::size_t>::max() -
                                    logical_bytes - arg.size()) {
      session->second.db_epoch_invalidated_ = true;
      store.fullsync_publisher_capacity_ready_.NotifyAll(*store.worker_);
      return false;
    }
    logical_bytes += sizeof(std::uint32_t) + arg.size();
  }
  auto& state = session->second;
  if (state.next_publish_id_ == 0) {
    state.db_epoch_invalidated_ = true;
    store.fullsync_publisher_capacity_ready_.NotifyAll(*store.worker_);
    return false;
  }
  if (logical_bytes >
      std::numeric_limits<std::size_t>::max() - state.publish_queue_bytes_) {
    state.db_epoch_invalidated_ = true;
    store.fullsync_publisher_capacity_ready_.NotifyAll(*store.worker_);
    return false;
  }
  const std::uint64_t id = state.next_publish_id_++;
  state.publish_queue_bytes_ += logical_bytes;
  state.publish_queue_.push_back(
      WorkerStore::FullSyncSessionState::PendingCommand{
          .id_ = id,
          .logical_bytes_ = logical_bytes,
          .command_ = std::move(command),
          .record_ = std::nullopt,
      });
  return true;
}

bool StorageEngine::Impl::TryEnqueueFullSyncRecord(
    WorkerStore& store, std::uint64_t session_id,
    const SnapshotRecord& record) {
  auto session = store.fullsync_sessions_.find(session_id);
  if (session == store.fullsync_sessions_.end() ||
      session->second.db_epoch_invalidated_) {
    return false;
  }
  auto& state = session->second;
  std::size_t logical_bytes = kFullSyncReplacementMetadataBytes;
  if (record.key_.size() >
          (std::numeric_limits<std::size_t>::max() - logical_bytes) / 2 ||
      state.next_publish_id_ == 0) {
    state.db_epoch_invalidated_ = true;
    store.fullsync_publisher_capacity_ready_.NotifyAll(*store.worker_);
    return false;
  }
  logical_bytes += record.key_.size() * 2;
  if (logical_bytes >
      std::numeric_limits<std::size_t>::max() - state.publish_queue_bytes_) {
    state.db_epoch_invalidated_ = true;
    store.fullsync_publisher_capacity_ready_.NotifyAll(*store.worker_);
    return false;
  }
  const std::uint64_t id = state.next_publish_id_++;
  state.publish_queue_bytes_ += logical_bytes;
  state.publish_queue_.push_back(
      WorkerStore::FullSyncSessionState::PendingCommand{
          .id_ = id,
          .logical_bytes_ = logical_bytes,
          .command_ = nullptr,
          .record_ = record,
      });
  return true;
}

void StorageEngine::Impl::PublishCommittedFullSyncEffects(
    TxShardWrites* shard) {
  if (shard == nullptr) return;
  WorkerStore& store = CurrentStore();
  for (const TxShardWrites::FullSyncEffect& effect : shard->fullsync_effects_) {
    auto& partition = PartitionFor(store, effect.partition_id_);
    for (std::uint64_t session_id : effect.session_ids_) {
      auto capture = partition.fullsync_subscribers_.find(session_id);
      if (capture == partition.fullsync_subscribers_.end()) continue;
      FullSyncCaptureOnCommit(
          store, session_id, capture->second, effect.record_,
          ComputeDigest(effect.record_.key_), nullptr, true);
    }
  }
  shard->fullsync_effects_.clear();
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
    SetLatencyTrace* trace, const ExplicitWriteRoot* explicit_root) {
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
  if ((txid != 0 && (tx == nullptr || for_defrag)) ||
      (kind == RecordKind::kTxCommit && txid == 0)) {
    co_return absl::InvalidArgumentError(
        "tagged records require a live transaction generation");
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
      explicit_root != nullptr
          ? explicit_root->index_
          : (partition_ptr == nullptr ? nullptr
                                      : &partition_ptr->indexes_[db_id]);
  auto allocated_lsn = AllocateLsn(store);
  if (!allocated_lsn.ok()) {
    co_return allocated_lsn.status();
  }
  const std::uint64_t lsn = *allocated_lsn;

  const bool transaction_append = txid != 0;
  const std::uint64_t tx_generation =
      transaction_append && tx != nullptr ? tx->generation_ : 0;
  if (transaction_append && tx_generation == 0) {
    co_return absl::InvalidArgumentError(
        "transaction record has no generation lease");
  }
  const BlockKind append_block_kind =
      transaction_append ? BlockKind::kTransaction : BlockKind::kRecords;
  // Never keep a flat_hash_map value reference across an await that can
  // release store_state_mutex_. Another transaction may install a different
  // generation and rehash active_tx_blocks_ while block allocation is in
  // flight. Re-resolve the entry on every use; active_block_ itself is stable.
  auto active_stream = [&]() -> std::optional<ActiveBlock>& {
    return transaction_append ? store.active_tx_blocks_[tx_generation]
                              : store.active_block_;
  };
acquire_active_stream:
  if (trace != nullptr) trace->block_wait_start_ns_ = SetTraceNowNanos();
  while (!active_stream().has_value() ||
         active_stream()->committed_bytes_ + total_disk_bytes >
             kStorageBlockBytes) {
    if (trace != nullptr) trace->allocated_block_ = true;
    if (active_stream().has_value()) {
      RequestFlush(store, active_stream()->block_id_);
      active_stream().reset();
    }
    auto allocated = co_await AcquireWriteBlock(store, for_defrag,
                                                unlock_writer_while_waiting);
    if (!allocated.ok()) {
      co_return allocated.status();
    }
    // Another writer may have installed an active block while this coroutine
    // had store_state_mutex released. Keep that one and hand the spare back to
    // the pool instead of overwriting it; the loop re-checks the fit.
    if (active_stream().has_value()) {
      co_await ReturnReservedBlock(*allocated);
      continue;
    }
    {
      std::uint16_t write_buffer_id = 0;
      std::byte* heap_buffer = nullptr;
      if (!store.buffers_.TryAcquireWriteBuffer(&write_buffer_id)) {
        if (for_defrag || !unlock_writer_while_waiting) {
          // Maintenance paths that deliberately keep store_state_mutex_
          // across an atomic rewrite cannot wait for a flush that needs the
          // same lock. Their concurrency is separately bounded.
          if (!store.buffers_.TryAcquireHeapWriteBuffer(&heap_buffer)) {
            co_await ReturnReservedBlock(*allocated);
            co_return absl::Status(absl::StatusCode::kResourceExhausted,
                                   "no storage write buffer is available");
          }
        } else {
          do {
            store.store_state_mutex_.Unlock(*store.worker_);
            co_await store.buffers_.WaitForWriteBuffer();
            co_await store.store_state_mutex_.Lock();
          } while (!store.buffers_.TryAcquireWriteBuffer(&write_buffer_id));

          // Another writer may have installed this append stream while this
          // coroutine was waiting without the store lock. It owns the stream;
          // return both resources and let the outer loop append to it.
          if (active_stream().has_value()) {
            store.buffers_.ReleaseWriteBuffer(write_buffer_id);
            co_await ReturnReservedBlock(*allocated);
            continue;
          }
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
      active_stream() = ActiveBlock{
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
          .kind_ = append_block_kind,
          .tx_generation_ = tx_generation,
      };
      BlockState& state = CreateBlockState(store, block_id);
      state.writer_id_ = writer_id;
      state.layout_worker_count_ = worker_count_;
      state.allocation_epoch_ = active_stream()->allocation_epoch_;
      state.committed_bytes_ = kBlockHeaderBytes;
      state.live_bytes_ = 0;
      state.pins_ = 0;
      state.allocated_ = true;
      state.defragging_ = false;
      state.in_memory_ = true;
      state.flush_queued_ = false;
      state.flush_in_progress_ = false;
      state.kind_ = append_block_kind;
      if (transaction_append) {
        store.tx_blocks_.insert_or_assign(
            block_id, WorkerStore::TxBlockRuntime{
                          .allocation_epoch_ = allocated->allocation_epoch_,
                          .generation_ = tx_generation,
                      });
      }
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
  if (explicit_root != nullptr && explicit_root->reject_older_sequence_ &&
      previous_entry != nullptr &&
      previous_entry->value_.mutation_sequence_ >= mutation_sequence) {
    co_return absl::OkStatus();
  }
  if (!for_defrag && partition_ptr != nullptr &&
      partition_ptr->rdb_snapshot_.has_value()) [[unlikely]] {
    // The capture stores only physical metadata and pins. It may release the
    // store mutex while pinning a block owned by another worker, so resolve
    // the current entry again before the ordinary overwrite bookkeeping.
    (void)co_await CaptureRdbSnapshotBeforeWriteLocked(store, *partition_ptr,
                                                       db_id, key, digest);
    previous_entry =
        index_ptr != nullptr ? index_ptr->Find(digest, key) : nullptr;
    if (previous_entry != nullptr && !previous_entry->key_complete())
        [[unlikely]] {
      auto resolved =
          co_await FindVerifiedEntry(store, *index_ptr, digest, key);
      if (!resolved.ok()) co_return resolved.status();
      previous_entry = *resolved;
    }
    if (explicit_root != nullptr && explicit_root->reject_older_sequence_ &&
        previous_entry != nullptr &&
        previous_entry->value_.mutation_sequence_ >= mutation_sequence) {
      co_return absl::OkStatus();
    }
  }
  // FindVerifiedEntry and the RDB old-value capture may release the store
  // mutex. Another writer can fill and seal this worker's append stream while
  // this coroutine is suspended. Re-enter allocation before dereferencing the
  // optional or appending to a replacement block that no longer has room.
  if (!active_stream().has_value() ||
      active_stream()->committed_bytes_ + total_disk_bytes >
          kStorageBlockBytes) {
    goto acquire_active_stream;
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
  ActiveBlock updated = *active_stream();
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
          explicit_root != nullptr
              ? explicit_root->replication_epoch_
              : (partition_ptr == nullptr ? 1
                                          : partition_ptr->replication_epoch_),
      // A relocation stamps the epoch its source was validated under, not a
      // fresh read: worker 0 publishes a FLUSHDB epoch concurrently, and a
      // fresh read here could adopt it mid-append — turning a record
      // recovery must drop into one it must keep.
      .db_epoch_ = explicit_root != nullptr
                       ? explicit_root->db_epoch_
                       : (relocation != nullptr ? relocation->db_epoch_
                                                : DbEpoch(db_id)),
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
    active_stream().reset();
  } else {
    active_stream() = updated;
  }

  const RecordLocation location(
      updated.block_id_, mutation_sequence, updated.allocation_epoch_,
      expire_at_ms, static_cast<std::uint32_t>(logical_size),
      // A relocation rewrites the same logical version, so it carries the
      // bit unchanged. A real overwrite shields what its predecessor was
      // shielding, plus the buried value itself — but only if that value
      // could outlive this record's own erasure deadline: a predecessor
      // whose expiry falls before it would already be self-suppressed by
      // its timestamp whenever this entry may be dropped. Records with no
      // deadline of their own (tombstones, TTL-less values) must judge
      // against now instead, since their successors' deadlines are unknown.
      RecordLocation::PackedMetadata::Encode(
          record_offset, static_cast<std::uint32_t>(total_disk_bytes),
          writer_id, true, external, key_external,
          previous.has_value() &&
              (relocation != nullptr
                   ? previous->shielding()
                   : (previous->shielding() ||
                      (previous->kind() == RecordKind::kValue &&
                       (previous->expire_at_ms_ == 0 ||
                        previous->expire_at_ms_ >
                            std::max(expire_at_ms, UnixTimeMillis()))))),
          false, txid != 0 && kind != RecordKind::kTxCommit,
          // Commit records never enter the key index. Their temporary
          // RecordLocation is used only to request the destination block's
          // flush, so encode the packed, index-only type state as its empty
          // default rather than spending one of the five reserve bits.
          kind == RecordKind::kTxCommit ? RecordKind::kValue : kind,
          value_type));
  if (transaction_append) {
    NoteTxRecordLocal(store, updated.block_id_, updated.allocation_epoch_,
                      tx_generation, txid, location.total_disk_bytes(),
                      kind == RecordKind::kTxCommit);
  }
  const bool was_live =
      previous.has_value() && previous->kind() == RecordKind::kValue;
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
  const bool dependency_pinned =
      route_to_commit && PinTxDependencyLocal(store, *previous);
  const bool defer_defrag_retirement =
      for_defrag && commit_retirements != nullptr;
  store.staged_records_[updated.block_id_].push_back(RecordIdentity{
      .entry_ = inserted_entry,
      .retired_extents_ = (!for_defrag || defer_defrag_retirement) &&
                                  !route_to_commit && previous.has_value() &&
                                  previous->external() &&
                                  !previous->key_external()
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
        .block_id_ = previous->block_id(),
        .allocation_epoch_ = previous->allocation_epoch(),
        .total_disk_bytes_ = previous->total_disk_bytes(),
        .block_owner_ = previous->block_owner(),
        .record_offset_ = previous->record_offset(),
        .tx_tagged_ = previous->tx_tagged(),
        .dependency_pinned_ = dependency_pinned,
        .dependent_extents_ =
            previous->key_external() ? previous_dependent_extents : nullptr,
        .immediate_extents_ =
            previous->key_external() ? nullptr : retired_value_extents,
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
      if (explicit_root != nullptr) {
        ++*explicit_root->live_key_count_;
        if (explicit_root->store_live_key_count_ != nullptr) {
          ++*explicit_root->store_live_key_count_;
        }
      } else {
        ++partition_ptr->live_key_count_[db_id];
        ++store.live_key_count_[db_id];
      }
    } else {
      if (explicit_root != nullptr) {
        --*explicit_root->live_key_count_;
        if (explicit_root->store_live_key_count_ != nullptr) {
          --*explicit_root->store_live_key_count_;
        }
      } else {
        --partition_ptr->live_key_count_[db_id];
        --store.live_key_count_[db_id];
      }
    }
  }
  if (was_expiring != is_expiring) {
    if (is_expiring) {
      if (explicit_root != nullptr) {
        ++*explicit_root->expiring_key_count_;
      } else {
        ++partition_ptr->expiring_key_count_[db_id];
      }
    } else {
      if (explicit_root != nullptr) {
        --*explicit_root->expiring_key_count_;
      } else {
        --partition_ptr->expiring_key_count_[db_id];
      }
    }
  }
  state.committed_bytes_ = updated.committed_bytes_;
  state.in_memory_ = true;
  staging_state->committed_bytes_ = updated.committed_bytes_;
  staging_state->record_count_ = updated.record_count_;
  staging_state->max_lsn_ = updated.max_lsn_;
  state.live_bytes_ += location.total_disk_bytes();
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
  if (!for_defrag && previous.has_value() && previous->external()) {
    RequestFlush(store, updated.block_id_);
    if (active_stream().has_value() &&
        active_stream()->block_id_ == updated.block_id_) {
      active_stream().reset();
    }
  }
  if (written_location != nullptr) {
    *written_location = location;
  }
  if (trace != nullptr) trace->index_done_ns_ = SetTraceNowNanos();
  co_return absl::OkStatus();
}

void StorageEngine::Impl::SealActiveBlocks(WorkerStore& store) {
  auto seal = [&](std::optional<ActiveBlock>& active) {
    if (!active.has_value() || active->committed_bytes_ <= kBlockHeaderBytes) {
      return;
    }
    RequestFlush(store, active->block_id_);
    active.reset();
  };
  seal(store.active_block_);
  for (auto& [generation, active] : store.active_tx_blocks_) {
    (void)generation;
    seal(active);
  }
}

void StorageEngine::Impl::FlushActiveBlock(WorkerStore& store) {
  auto flush = [&](const std::optional<ActiveBlock>& active) {
    if (active.has_value() && active->committed_bytes_ > kBlockHeaderBytes) {
      RequestFlush(store, active->block_id_);
    }
  };
  flush(store.active_block_);
  for (const auto& [generation, active] : store.active_tx_blocks_) {
    (void)generation;
    flush(active);
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
