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

#include <exception>
#include <new>

#include "absl/strings/str_cat.h"
#include "impl.h"
#include "lavik/memory.h"
#include "lavik/metrics.h"
#include "lavik/replication_command.h"

namespace lavik::storage {

absl::StatusOr<RecordIndex::Entry*> StorageEngine::Impl::ReplaceIndexLocation(
    WorkerStore& store, RecordIndex& index, RecordIndex::Entry* entry,
    const Digest& digest, const RecordLocation& location, TxUndoLog* tx_undo) {
  RecordIndex::Entry* replaced = nullptr;
  RecordIndex::Entry* current =
      index.ReplaceValue(entry, location, digest, &replaced);
  if (current == nullptr) {
    return absl::ResourceExhaustedError(
        "record index entry capacity exhausted");
  }
  if (replaced == nullptr) {
    return current;
  }

  // Staged records deliberately keep only the old address bits. Flush proves
  // bucket membership before recovering a live pointer, so changing
  // representation is O(1) in the number of pending records. Only the
  // transaction that owns this key can retain a dereferenceable rollback
  // handle: its exclusive key lock prevents any other transaction or
  // foreground writer from observing the transition. Updating the handle's
  // one current-pointer slot avoids scanning every prior write in a large
  // transaction when many keys change TTL representation.
  if (tx_undo != nullptr) {
    tx_undo->Replace(replaced, current);
  }

  auto move_pointer_key = [replaced, current](auto& values) {
    auto found = values.find(replaced);
    if (found == values.end()) {
      return;
    }
    auto value = std::move(found->second);
    values.erase(found);
    values.insert_or_assign(current, std::move(value));
  };
  move_pointer_key(store.external_manifests_);
  move_pointer_key(store.recovery_external_keys_);
  move_pointer_key(store.recovery_lsns_);
  move_pointer_key(store.recovery_txids_);
  move_pointer_key(store.recovery_grouped_roots_);

  index.DestroyDetached(replaced);
  return current;
}

namespace {

template <typename Queue>
bool TryPreparePostMutationQueueSlot(Queue* queue,
                                     std::size_t admitted_items) noexcept {
  // One command can yield several full-sync after-images. The ordinary
  // admission owns one slot; if later effects exhaust it, grow inside the
  // session's fixed staging budget. Failure invalidates only the
  // lower-priority full-sync attempt at the caller.
  const std::size_t additional_slots = std::max<std::size_t>(admitted_items, 1);
  if (queue->size() >
      std::numeric_limits<std::size_t>::max() - additional_slots) {
    return false;
  }
  const std::size_t minimum_capacity = queue->size() + additional_slots;
  if (minimum_capacity <= queue->capacity()) return true;
  try {
    queue->PrepareCapacity(minimum_capacity);
    return true;
  } catch (const std::length_error&) {
    return false;
  }
}

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
    SetLatencyTrace* trace, std::optional<std::uint16_t> routed_partition_id,
    const MutationPrecondition* mutation_precondition) {
  assert(db_id < kLogicalDatabaseCount);
  const Digest digest = ComputeDigest(key);
  if (trace != nullptr) trace->key_lock_start_ns_ = SetTraceNowNanos();
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest), tx::LockMode::kExclusive);
  if (trace != nullptr) trace->key_lock_acquired_ns_ = SetTraceNowNanos();
  co_return co_await SetLocked(db_id, key, digest, value, options, nullptr,
                               replication, trace, routed_partition_id,
                               mutation_precondition);
}

Task<absl::StatusOr<SetResult>> StorageEngine::Impl::SetLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    std::string_view value, SetOptions options, TxShardWrites* tx,
    ReplicationCommandAppend* replication, SetLatencyTrace* trace,
    std::optional<std::uint16_t> routed_partition_id,
    const MutationPrecondition* mutation_precondition) {
  assert(db_id < kLogicalDatabaseCount);
  WorkerStore& store = CurrentStore();
  // The route hint is produced from this exact key immediately before the
  // cross-core handoff. Debug builds recheck that contract; optimized builds
  // avoid another Redis CRC16 calculation on every source SET.
  assert(!routed_partition_id.has_value() ||
         *routed_partition_id == RedisSlot(key));
  auto& partition = routed_partition_id.has_value()
                        ? PartitionFor(store, *routed_partition_id)
                        : PartitionForKey(store, key);
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
  bool exists = found != nullptr && found->value_.kind() == RecordKind::kValue;
  // Expiry metadata is out-of-line and uncommon in the no-TTL workload. Do
  // not read wall time for the ordinary overwrite path; it is irrelevant when
  // the index entry cannot expire.
  if (exists && found->value_.has_expiry() && IsExpiredNow(*found)) {
    exists = false;
  }
  if (trace != nullptr) trace->lookup_done_ns_ = SetTraceNowNanos();
  SetResult result;
  if (options.return_old_value_ && exists) {
    if (found->value_.value_type() != ValueType::kString) {
      co_return absl::Status(
          absl::StatusCode::kInvalidArgument,
          "WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    auto loaded = co_await LoadValue(store, partition, db_id, key, digest,
                                     MaterializeIndexLocation(*found),
                                     ExtentsFor(store, found));
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

  const std::uint64_t expire_at_ms =
      options.keep_ttl_ && exists ? ExpireAt(*found) : options.expire_at_ms_;
  if (replication != nullptr && expire_at_ms != 0) {
    replication->args_.emplace_back("PXAT");
    replication->args_.emplace_back(std::to_string(expire_at_ms));
  }
  if (trace != nullptr) trace->append_start_ns_ = SetTraceNowNanos();
  absl::Status status = co_await AppendLocked(
      store, partition, db_id, key, digest, value, RecordKind::kValue,
      ValueType::kString, expire_at_ms, tx,
      std::numeric_limits<std::uint64_t>::max(), nullptr, nullptr, replication,
      trace, true, nullptr, mutation_precondition);
  if (trace != nullptr) trace->append_done_ns_ = SetTraceNowNanos();
  if (!status.ok()) co_return status;
  result.applied_ = true;
  if (trace != nullptr) trace->replication_done_ns_ = SetTraceNowNanos();
  co_return result;
}

Task<absl::StatusOr<bool>> StorageEngine::Impl::UpdateExpiration(
    std::uint8_t db_id, std::string_view key, std::uint64_t expire_at_ms,
    ExpirationCondition condition, ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  assert(db_id < kLogicalDatabaseCount);
  const Digest digest = ComputeDigest(key);
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest), tx::LockMode::kExclusive);
  co_return co_await UpdateExpirationLocked(db_id, key, digest, expire_at_ms,
                                            condition, nullptr, replication,
                                            mutation_precondition);
}

Task<absl::StatusOr<bool>> StorageEngine::Impl::UpdateExpirationLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    std::uint64_t expire_at_ms, ExpirationCondition condition,
    TxShardWrites* tx, ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
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
      IsExpired(*found, now_ms)) {
    co_return false;
  }
  const std::uint64_t current = ExpireAt(*found);
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
        ValueType::kNone, 0, tx, 0, nullptr, nullptr, replication, nullptr,
        true, nullptr, mutation_precondition);
    if (!status.ok()) co_return status;
    co_return true;
  }

  const RecordLocation previous = MaterializeIndexLocation(*found);
  if (previous.grouped()) {
    auto view = partition.grouped_objects_[db_id].Lookup(
        key, GroupedObjectVersion{
                 .root_ = previous,
                 .db_epoch_ = EffectiveRecordDbEpoch(partition, db_id),
                 .replication_epoch_ = partition.replication_epoch_,
                 .index_generation_ = partition.grouped_generations_[db_id]});
    if (!view.ok()) co_return view.status();
    const auto updated = co_await UpdateGroupedExpirationLocked(
        store, partition, db_id, key, digest, *view, expire_at_ms, tx,
        replication, mutation_precondition);
    if (!updated.ok()) co_return updated;
    co_return true;
  }
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
      nullptr, replication, nullptr, true, nullptr, mutation_precondition);
  if (!status.ok()) {
    co_return status;
  }
  co_return true;
}

Task<absl::StatusOr<bool>> StorageEngine::Impl::Delete(
    std::uint8_t db_id, std::string_view key,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  assert(db_id < kLogicalDatabaseCount);
  const Digest digest = ComputeDigest(key);
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest), tx::LockMode::kExclusive);
  co_return co_await DeleteLocked(db_id, key, digest, nullptr, replication,
                                  mutation_precondition);
}

Task<absl::StatusOr<bool>> StorageEngine::Impl::DeleteLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    TxShardWrites* tx, ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
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
  const bool expired = IsExpiredNow(*found);
  absl::Status status = co_await AppendLocked(
      store, partition, db_id, key, digest, {}, RecordKind::kTombstone,
      ValueType::kNone, 0, tx, 0, nullptr, nullptr, replication, nullptr, true,
      nullptr, mutation_precondition);
  if (!status.ok()) co_return status;
  co_return !expired;
}

Task<absl::Status> StorageEngine::Impl::WriteRawValueLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const RawValue& value, TxShardWrites* tx,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
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
      nullptr, replication, nullptr, true, nullptr, mutation_precondition);
}

Task<absl::StatusOr<RestoreRawResult>> StorageEngine::Impl::RestoreRawValue(
    std::uint8_t db_id, std::string_view key, const RawValue& value,
    bool replace, ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  assert(db_id < kLogicalDatabaseCount);
  const Digest digest = ComputeDigest(key);
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest), tx::LockMode::kExclusive);
  co_return co_await RestoreRawValueLocked(db_id, key, digest, value, replace,
                                           nullptr, replication,
                                           mutation_precondition);
}

Task<absl::StatusOr<RestoreRawResult>>
StorageEngine::Impl::RestoreRawValueLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const RawValue& value, bool replace, TxShardWrites* tx,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  const bool exists = co_await ExistsLocked(db_id, key, digest);
  if (exists && !replace) co_return RestoreRawResult{.busy_ = true};
  if (value.expire_at_ms_ != 0 && value.expire_at_ms_ <= UnixTimeMillis()) {
    if (!exists) co_return RestoreRawResult{};
    auto deleted = co_await DeleteLocked(db_id, key, digest, tx, replication,
                                         mutation_precondition);
    if (!deleted.ok()) co_return deleted.status();
    co_return RestoreRawResult{.changed_ = *deleted, .deleted_ = *deleted};
  }
  absl::Status written = co_await WriteRawValueLocked(
      db_id, key, digest, value, tx, replication, mutation_precondition);
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
  if (owner == bycorf::ThisWorker().id_) {
    co_return MarkRecordDeadLocal(owner, record);
  }
  co_return co_await bycorf::SubmitTo(owner, [this, owner, record] {
    return MarkRecordDeadLocal(owner, record);
  });
}

Task<absl::Status> StorageEngine::Impl::CommitTxWrites(
    std::uint64_t txid, std::vector<TxShardWrites*> shards) {
  struct FailUncommittedDependencies {
    const std::vector<TxShardWrites*>& shards_;
    bool completed_ = false;
    ~FailUncommittedDependencies() {
      if (completed_) return;
      for (auto* shard : shards_) {
        if (shard != nullptr && shard->grouped_decision_ != nullptr) {
          shard->grouped_decision_->FailPending();
        }
      }
    }
  } dependency_guard{shards};
  // The commit record must land strictly after every tagged data record is
  // durable: recovery treats "commit without data" as impossible, and
  // "data without commit" as an aborted transaction.
  auto retirements = std::make_unique<std::vector<RetiredRecord>>();
  TxShardWrites* generation_receipt = nullptr;
  for (TxShardWrites* shard : shards) {
    if (shard == nullptr) {
      continue;
    }
    if (shard->grouped_decision_ != nullptr &&
        shard->grouped_decision_->state_.load(std::memory_order_acquire) ==
            GroupedCommitDecision::State::kFailed) {
      co_return absl::FailedPreconditionError(
          "grouped transaction was abandoned");
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
          .retained_owner_ = retired.retained_owner_,
      });
    }
  }
  // Every tagged record is durable; the transaction's fate now rests solely
  // on the commit record. Crash-safety tests arm this point to prove the
  // all-or-nothing promise: dying here must abort the whole transaction.
  LAVIK_MAYBE_CRASH_AT("tx-commit-append");
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
  const bool grouped =
      std::any_of(shards.begin(), shards.end(), [](auto* shard) {
        return shard != nullptr && shard->grouped_decision_ != nullptr;
      });
  if (grouped) {
    unlock.Unlock();
    auto durable = co_await AwaitRelocationDurable(RelocationDurabilityFence{
        .block_id_ = commit_location.block_id(),
        .allocation_epoch_ = commit_location.allocation_epoch(),
        .block_owner_ = commit_location.block_owner(),
        .committed_bytes_ =
            static_cast<std::uint32_t>(commit_location.record_offset() +
                                       commit_location.total_disk_bytes()),
    });
    if (!durable.ok()) co_return durable;
    for (auto* shard : shards) {
      if (shard != nullptr && shard->grouped_decision_ != nullptr) {
        shard->grouped_decision_->state_.store(
            GroupedCommitDecision::State::kDurable, std::memory_order_release);
      }
    }
  }
  dependency_guard.completed_ = true;
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
  store.tx_commit_queue_.push_back(WorkerStore::PendingTxCommit{
      .txid_ = txid,
      .writes_ = std::move(writes),
  });
  // A deque allocation may fail. Count a commit only after the queue owns
  // it, otherwise shutdown would wait forever for a nonexistent pending item.
  NoteTxCommitStarted();
  const std::uint64_t depth =
      tx_commit_queue_depth_.fetch_add(1, std::memory_order_acq_rel) + 1;
  std::uint64_t peak = tx_commit_queue_peak_.load(std::memory_order_relaxed);
  while (depth > peak && !tx_commit_queue_peak_.compare_exchange_weak(
                             peak, depth, std::memory_order_release,
                             std::memory_order_relaxed)) {
  }
  if (!store.tx_commit_runner_) {
    try {
      // Allocate the coroutine before claiming its runner slot. A failed
      // launch must release this queue/count ownership as well as poisoning
      // the grouped decision in the foreground handoff guard.
      auto runner = DrainTxCommitQueue(&store);
      store.tx_commit_runner_ = true;
      store.worker_->Spawn(std::move(runner));
    } catch (const std::bad_alloc&) {
      store.tx_commit_runner_ = false;
      for (auto& shard : store.tx_commit_queue_.back().writes_)
        if (shard.grouped_decision_) shard.grouped_decision_->FailPending();
      store.tx_commit_queue_.pop_back();
      tx_commit_queue_depth_.fetch_sub(1, std::memory_order_acq_rel);
      NoteTxCommitFinished();
      throw;
    }
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
          co_return owner.write_failed_ || RuntimeFailureLatched()
              ? absl::InternalError(
                    "storage write failed while starting "
                    "transaction batch flush")
              : absl::OkStatus();
        };
        // Keep the two suspension paths as statements: GCC 13 can alias their
        // coroutine-frame slots when both are operands of one conditional.
        absl::Status requested;
        if (fence.block_owner_ == bycorf::ThisWorker().id_) {
          requested = co_await request();
        } else {
          requested = co_await bycorf::SubmitTaskTo(fence.block_owner_,
                                                    std::move(request));
        }
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
        for (auto& shard : pending.writes_) {
          if (shard.grouped_decision_ != nullptr) {
            shard.grouped_decision_->FailPending();
          }
        }
      }
      NoteTxCommitFinished();
    }
  }
  store->tx_commit_runner_ = false;
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::RollbackTxLocal(
    std::uint64_t txid, TxShardWrites* compensation,
    bool discard_uncommitted_absent, TxUndoLog* retained_prefix,
    bool grouped_root_only) {
  WorkerStore& store = CurrentStore();
  co_await store.store_state_mutex_.Lock();
  UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
  auto found = store.tx_undo_.find(txid);
  if (found == store.tx_undo_.end()) {
    co_return absl::OkStatus();
  }
  TxUndoLog undo = std::move(found->second);
  // A pull-based restore isolates only its suffix. Return the retained prefix
  // to this already allocated map slot before any awaited rollback work;
  // inserting a new journal on an OOM abort path would itself require memory.
  if (retained_prefix != nullptr)
    found->second = std::move(*retained_prefix);
  else
    store.tx_undo_.erase(found);
  assert(!discard_uncommitted_absent || compensation == nullptr);
  // Compensation must restore the pre-command state even when the authority
  // precondition that admitted the failed transaction is now stale.
  const MutationPrecondition bypass_mutation_precondition;
  // Reverse order: a key written twice in one transaction unwinds through
  // its intermediate version back to the original.
  for (auto it = undo.entries_.rbegin(); it != undo.entries_.rend(); ++it) {
    TxUndoEntry& entry = *it;
    RecordIndex::Entry* current = undo.Current(entry.entry_handle_);
    const RecordLocation applied = MaterializeIndexLocation(*current);
    std::string loaded_key;
    if (!current->key_complete()) [[unlikely]] {
      auto key = co_await LoadOutOfIndexKey(
          store, MaterializeIndexLocation(*current), ExtentsFor(store, current),
          current->logical_key_size());
      if (!key.ok()) {
        LatchRuntimeFailure(store);
        co_return key.status();
      }
      loaded_key = std::move(*key);
    }
    const std::string_view undo_key =
        current->key_complete() ? current->key() : std::string_view(loaded_key);
    const Digest undo_digest = ComputeDigest(undo_key);
    auto& partition = PartitionForKey(store, undo_key);
    GroupedHashObject::Handle applied_grouped;
    if (applied.grouped()) {
      auto view = partition.grouped_objects_[entry.db_id_].Lookup(
          undo_key,
          GroupedObjectVersion{
              .root_ = applied,
              .db_epoch_ = EffectiveRecordDbEpoch(partition, entry.db_id_),
              .replication_epoch_ = partition.replication_epoch_,
              .index_generation_ = partition.grouped_generations_[entry.db_id_],
          },
          /*allow_failed=*/true);
      if (!view.ok()) {
        store.write_failed_ = true;
        co_return view.status();
      }
      applied_grouped = std::move(*view);
    }
    if (compensation != nullptr) {
      if (grouped_root_only && entry.previous_grouped_ != nullptr) {
        // The isolated restore driver cancels old-graph physical retirements
        // separately. Never materialize a potentially multi-GiB predecessor.
        const auto restored = co_await RestoreGroupedViewLocked(
            store, partition, entry.db_id_, undo_key, undo_digest,
            entry.previous_grouped_, compensation, &undo);
        if (!restored.ok()) {
          store.write_failed_ = true;
          co_return restored;
        }
        continue;
      }
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
            *entry.previous_, entry.previous_extents_, nullptr,
            entry.previous_grouped_);
        if (!loaded.ok()) {
          LatchRuntimeFailure(store);
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
          compensation, restored_size,
          /*commit_retirements=*/nullptr,
          /*committed_sequence=*/nullptr,
          /*replication=*/nullptr,
          /*trace=*/nullptr,
          /*capture_fullsync=*/true, &undo, &bypass_mutation_precondition);
      if (!appended.ok()) {
        LatchRuntimeFailure(store);
        co_return appended;
      }
      continue;
    }
    if (!entry.previous_.has_value()) {
      if (discard_uncommitted_absent) {
        // The target stream holds this key and will never commit its outer
        // decision. Keep its handle alive until every reverse-undo and slot
        // lookup has finished, then restore absence without writing to a
        // possibly fail-stopped device. No ordinary transaction uses this path.
        continue;
      }
      // The key did not exist: append a tombstone to restore runtime and
      // recovery state. The aborted transaction was never published to a
      // full-sync session, so this internal rollback must not publish either.
      absl::Status tombstone = co_await AppendLocked(
          store, partition, entry.db_id_, undo_key, undo_digest, {},
          RecordKind::kTombstone, ValueType::kNone, 0,
          /*tx=*/nullptr, /*logical_size=*/0,
          /*commit_retirements=*/nullptr, /*committed_sequence=*/nullptr,
          /*replication=*/nullptr, /*trace=*/nullptr,
          /*capture_fullsync=*/false, &undo);
      if (!tombstone.ok()) {
        LatchRuntimeFailure(store);
        co_return tombstone;
      }
      continue;
    }
    std::optional<GroupedObjectIndex::Publication> restored_group_slot;
    if (entry.previous_grouped_ != nullptr) {
      auto reserved = partition.grouped_objects_[entry.db_id_].PreparePublish(
          undo_key, applied_grouped);
      if (!reserved.ok()) {
        store.write_failed_ = true;
        co_return reserved.status();
      }
      restored_group_slot.emplace(std::move(*reserved));
    }
    // Mirror the append-time counter math in reverse.
    const ExtentManifest applied_extents = ExtentsFor(store, current);
    const ExtentManifest applied_dependent_extents =
        DependentExtentsFor(store, current);
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
    auto restored =
        ReplaceIndexLocation(store, partition.indexes_[entry.db_id_], current,
                             undo_digest, *entry.previous_, &undo);
    if (!restored.ok()) {
      LatchRuntimeFailure(store);
      co_return restored.status();
    }
    current = *restored;
    if (restored_group_slot.has_value()) {
      auto published = restored_group_slot->Commit(entry.previous_grouped_);
      if (!published.ok()) {
        store.write_failed_ = true;
        co_return published;
      }
    } else if (applied_grouped != nullptr) {
      // A still-earlier undo entry can need this same slot. Keep it until
      // the entire reverse journal has settled, not just this one rewind.
      auto cleared = partition.grouped_objects_[entry.db_id_].ClearKeepingSlot(
          undo_key, applied_grouped);
      if (!cleared.ok()) {
        store.write_failed_ = true;
        co_return cleared;
      }
    }
    if (entry.previous_->external()) {
      store.external_manifests_.insert_or_assign(current,
                                                 entry.previous_extents_);
    } else {
      store.external_manifests_.erase(current);
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
      LatchRuntimeFailure(store);
      co_return dead;
    }
    // Child epochs were captured before restoring/publishing either view.
    // Only new physical children leave accounting; old shared groups remain
    // authoritative and must never be retired by an aborted replacement.
    if (entry.applied_grouped_retirements_ != nullptr) {
      for (const auto& group : *entry.applied_grouped_retirements_) {
        if (group.block_owner_ == store.worker_->id()) {
          dead = MarkRecordDeadLocal(store.worker_->id(), group);
        } else {
          unlock.Unlock();
          dead = co_await MarkRecordDead(group);
          co_await store.store_state_mutex_.Lock();
          unlock.Adopt();
        }
        if (!dead.ok()) {
          store.write_failed_ = true;
          co_return dead;
        }
      }
    }
    if (entry.previous_grouped_retirements_ != nullptr) {
      for (const auto& group : *entry.previous_grouped_retirements_) {
        if (!group.dependency_pinned_) continue;
        if (group.block_owner_ == store.worker_->id()) {
          UnpinTxDependencyLocal(store, group.block_id_,
                                 group.allocation_epoch_);
        } else {
          unlock.Unlock();
          (void)co_await bycorf::SubmitTo(group.block_owner_, [this, group] {
            UnpinTxDependencyLocal(*stores_[group.block_owner_],
                                   group.block_id_, group.allocation_epoch_);
            return true;
          });
          co_await store.store_state_mutex_.Lock();
          unlock.Adopt();
        }
      }
    }
    if (entry.previous_dependency_pinned_) {
      const auto previous = *entry.previous_;
      if (previous.block_owner() == store.worker_->id()) {
        UnpinTxDependencyLocal(store, previous.block_id(),
                               previous.allocation_epoch());
      } else {
        // The exact root pin was acquired on its physical owner before
        // publication. Abort must release that same receipt, not inspect the
        // logical key owner's unrelated transaction-block table.
        unlock.Unlock();
        (void)co_await bycorf::SubmitTo(
            previous.block_owner(), [this, previous] {
              UnpinTxDependencyLocal(*stores_[previous.block_owner()],
                                     previous.block_id(),
                                     previous.allocation_epoch());
              return true;
            });
        co_await store.store_state_mutex_.Lock();
        unlock.Adopt();
      }
    }
  }
  auto cleared = co_await ClearGroupedUndoSlots(store, undo);
  if (!cleared.ok()) {
    store.write_failed_ = true;
    co_return cleared;
  }
  for (const auto& candidate : undo.entries_) {
    if (!discard_uncommitted_absent || candidate.previous_.has_value())
      continue;
    const auto* entry = &candidate;
    auto* current = undo.Current(entry->entry_handle_);
    const auto applied = MaterializeIndexLocation(*current);
    std::string external_key;
    if (!current->key_complete()) {
      auto loaded =
          co_await LoadOutOfIndexKey(store, applied, ExtentsFor(store, current),
                                     current->logical_key_size());
      if (!loaded.ok()) co_return loaded.status();
      external_key = std::move(*loaded);
    }
    const auto key = current->key_complete() ? current->key()
                                             : std::string_view(external_key);
    auto& partition = PartitionForKey(store, key);
    auto root_retirement = RetiredRecordOf(
        applied,
        applied.key_external() ? DependentExtentsFor(store, current) : nullptr);
    if (applied.external() && !applied.key_external())
      root_retirement.immediate_extents_ = ExtentsFor(store, current);
    auto grouped =
        partition.grouped_objects_[entry->db_id_].CurrentForMutation(key);
    if (grouped) {
      const auto removed =
          partition.grouped_objects_[entry->db_id_].Erase(key, grouped);
      if (!removed.ok()) co_return removed;
    }
    if (applied.kind() == RecordKind::kValue) {
      --partition.live_key_count_[entry->db_id_];
      --store.live_key_count_[entry->db_id_];
      if (applied.expire_at_ms_ != 0)
        --partition.expiring_key_count_[entry->db_id_];
    }
    const auto key_bytes = current->logical_key_size();
    store.external_manifests_.erase(current);
    if (!partition.indexes_[entry->db_id_].Erase(current))
      co_return absl::InternalError("replica absence undo lost its entry");
    RemoveFullSyncCoverageEntry(partition, entry->db_id_, key_bytes);
    unlock.Unlock();
    auto dead = co_await MarkRecordDead(root_retirement);
    if (dead.ok() && entry->applied_grouped_retirements_) {
      for (const auto& child : *entry->applied_grouped_retirements_) {
        dead = co_await MarkRecordDead(child);
        if (!dead.ok()) break;
      }
    }
    if (dead.ok() && entry->previous_grouped_retirements_) {
      // A squashed stream keeps intermediate-root/child pins here even when
      // the original predecessor was absent. They are release-only receipts.
      for (const auto& pin : *entry->previous_grouped_retirements_) {
        if (!pin.dependency_pinned_) continue;
        auto release = [this, pin]() -> Task<absl::Status> {
          auto& owner = *stores_[pin.block_owner_];
          co_await owner.store_state_mutex_.Lock();
          UnlockGuard pin_unlock(&owner.store_state_mutex_, owner.worker_);
          UnpinTxDependencyLocal(owner, pin.block_id_, pin.allocation_epoch_);
          co_return absl::OkStatus();
        };
        if (pin.block_owner_ == store.worker_->id())
          dead = co_await release();
        else
          dead = co_await bycorf::SubmitTaskTo(pin.block_owner_, release);
        if (!dead.ok()) break;
      }
    }
    co_await store.store_state_mutex_.Lock();
    unlock.Adopt();
    if (!dead.ok()) co_return dead;
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::DiscardTxUndoLocal(std::uint64_t txid) {
  WorkerStore& store = CurrentStore();
  co_await store.store_state_mutex_.Lock();
  UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
  if (auto found = store.tx_undo_.find(txid); found != store.tx_undo_.end()) {
    // Resolving an external key may suspend. Keep neither an unordered-map
    // iterator nor a reference to its element across that suspension: another
    // transaction can grow the map while this coroutine is waiting for IO.
    TxUndoLog undo = std::move(found->second);
    store.tx_undo_.erase(found);
    auto cleared = co_await ClearGroupedUndoSlots(store, undo);
    if (!cleared.ok()) {
      store.write_failed_ = true;
      co_return cleared;
    }
  }
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
    if (owner == bycorf::ThisWorker().id_) {
      dead = MarkRecordDeadLocal(owner, record);
    } else {
      dead = co_await bycorf::SubmitTo(owner, [this, owner, record] {
        return MarkRecordDeadLocal(owner, record);
      });
    }
    if (!dead.ok()) {
      // The inline path fails the client write on an accounting error; here
      // there is no client left to tell, so fail-stop the writer the same way
      // a flush IO error does.
      spdlog::error("retiring superseded record failed: {}", dead.message());
      LatchRuntimeFailure(*store);
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
  LAVIK_FAULT_INJECT(
      // Deterministically hold the elected foreground allocator after it
      // releases store_state_mutex_. Tests use this to prove that a peer for
      // the same stream waits on the allocation gate instead of allocating a
      // spare block. Only the first foreground allocation pauses.
      static std::atomic<bool> tx_active_pause_claimed = false;
      const char* tx_active_pause_text =
          std::getenv("LAVIK_TX_ACTIVE_BLOCK_PAUSE_MS");
      bool expected_tx_active_pause = false;
      if (!for_defrag && unlock_writer && tx_active_pause_text != nullptr &&
          tx_active_pause_claimed.compare_exchange_strong(
              expected_tx_active_pause, true, std::memory_order_acq_rel)) {
        char* end = nullptr;
        const unsigned long pause_ms =
            std::strtoul(tx_active_pause_text, &end, 10);
        if (end != tx_active_pause_text && *end == '\0' && pause_ms != 0) {
          // Test-only observability: e2e fixtures poll the server log for this
          // marker to confirm the pause is actually in effect instead of
          // guessing with sleeps.
          spdlog::warn(
              "LAVIK_TX_ACTIVE_BLOCK_PAUSE_MS pausing foreground allocation "
              "for {} ms",
              pause_ms);
          absl::Status paused = co_await bycorf::SleepFor(
              *store.worker_, std::chrono::milliseconds(pause_ms));
          if (!paused.ok()) {
            co_await store.store_state_mutex_.Lock();
            co_return paused;
          }
        }
      });
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
  if (allocated.ok() && (store.write_failed_ || RuntimeFailureLatched())) {
    // The writer fail-stopped while the allocation waited; report that
    // instead of appending into a stream that will never flush.
    if (unlock_writer) {
      store.store_state_mutex_.Unlock(*store.worker_);
    }
    (void)co_await ReturnReservedBlock(*allocated);
    if (unlock_writer) {
      co_await store.store_state_mutex_.Lock();
    }
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
  co_return co_await bycorf::SubmitTaskTo(
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

void StorageEngine::Impl::EnsureStandbyBlock(WorkerStore& store) {
  if (!store.active_block_.has_value() || store.standby_block_.has_value() ||
      store.standby_prefetch_pending_ ||
      store.standby_prefetch_for_block_ == store.active_block_->block_id_ ||
      store.write_failed_ || RuntimeFailureLatched() ||
      shutdown_flush_requested_.load(std::memory_order_acquire)) {
    return;
  }
  const std::uint64_t source_block_id = store.active_block_->block_id_;
  const std::uint64_t source_epoch = store.active_block_->allocation_epoch_;
  store.standby_prefetch_for_block_ = source_block_id;
  store.standby_prefetch_pending_ = true;
  store.worker_->Spawn(
      PrefetchStandbyBlock(&store, source_block_id, source_epoch));
}

Task<absl::Status> StorageEngine::Impl::PrefetchStandbyBlock(
    WorkerStore* store, std::uint64_t source_block_id,
    std::uint64_t source_epoch) {
  // The same stateful gate used by rollover is the completion handshake: a
  // writer that reaches the end of the active block waits behind this task,
  // then observes either the published standby or a normal inline retry.
  // There is no edge-triggered notification that can fire before it waits.
  co_await store->active_block_allocation_mutex_.Lock();
  UnlockGuard allocation_unlock(&store->active_block_allocation_mutex_,
                                store->worker_);

  co_await store->store_state_mutex_.Lock();
  const bool should_allocate =
      store->standby_prefetch_pending_ &&
      store->standby_prefetch_for_block_ == source_block_id &&
      !store->standby_block_.has_value() && store->active_block_.has_value() &&
      store->active_block_->block_id_ == source_block_id &&
      store->active_block_->allocation_epoch_ == source_epoch &&
      !store->write_failed_ && !RuntimeFailureLatched() &&
      !shutdown_flush_requested_.load(std::memory_order_acquire);
  store->store_state_mutex_.Unlock(*store->worker_);

  absl::StatusOr<ReservedBlock> allocated{
      absl::CancelledError("standby prefetch is no longer needed")};
  if (should_allocate) {
    absl::Status pause_status = absl::OkStatus();
    LAVIK_FAULT_INJECT(
        static std::atomic<bool> standby_pause_claimed = false;
        const char* standby_pause_text =
            std::getenv("LAVIK_STANDBY_PREFETCH_PAUSE_MS");
        bool expected_standby_pause = false;
        if (standby_pause_text != nullptr &&
            standby_pause_claimed.compare_exchange_strong(
                expected_standby_pause, true, std::memory_order_acq_rel)) {
          char* end = nullptr;
          const unsigned long pause_ms =
              std::strtoul(standby_pause_text, &end, 10);
          if (end != standby_pause_text && *end == '\0' && pause_ms != 0) {
            spdlog::warn(
                "LAVIK_STANDBY_PREFETCH_PAUSE_MS pausing standby prefetch "
                "for {} ms",
                pause_ms);
            pause_status = co_await bycorf::SleepFor(
                *store->worker_, std::chrono::milliseconds(pause_ms));
          }
        });
    if (pause_status.ok()) {
      allocated =
          co_await AllocateBlock(*store, AllocationPurpose::kForeground);
    } else {
      allocated = pause_status;
    }
  }

  co_await store->store_state_mutex_.Lock();
  const bool publish =
      allocated.ok() && store->standby_prefetch_pending_ &&
      store->standby_prefetch_for_block_ == source_block_id &&
      !store->standby_block_.has_value() && store->active_block_.has_value() &&
      store->active_block_->block_id_ == source_block_id &&
      store->active_block_->allocation_epoch_ == source_epoch &&
      !store->write_failed_ && !RuntimeFailureLatched() &&
      !shutdown_flush_requested_.load(std::memory_order_acquire);
  if (publish) {
    store->standby_block_ = *allocated;
    store->standby_prefetch_pending_ = false;
    store->store_state_mutex_.Unlock(*store->worker_);
    co_return absl::OkStatus();
  }
  store->store_state_mutex_.Unlock(*store->worker_);

  // Keep pending true until a stale reservation is back in the device pool.
  // Shutdown polls this bit before destroying WorkerStore and allocator state.
  if (allocated.ok()) {
    (void)co_await ReturnReservedBlock(*allocated);
  }
  co_await store->store_state_mutex_.Lock();
  store->standby_prefetch_pending_ = false;
  store->store_state_mutex_.Unlock(*store->worker_);
  co_return absl::OkStatus();
}

Task<absl::StatusOr<std::shared_ptr<const std::vector<ExtentRef>>>>
StorageEngine::Impl::WriteExtentValueLocked(
    WorkerStore& store, std::string_view first, std::string_view second,
    RecordPayloadCursor* cursor, [[maybe_unused]] std::string_view fault_key,
    bool shutdown_metadata) {
  if (cursor != nullptr && (!first.empty() || !second.empty())) {
    co_return absl::InvalidArgumentError(
        "extent writer requires either spans or a payload cursor");
  }
  const std::uint64_t logical_bytes =
      cursor != nullptr
          ? cursor->encoded_bytes()
          : static_cast<std::uint64_t>(first.size()) + second.size();
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
    absl::StatusOr<ReservedBlock> reserved =
        absl::UnknownError("allocation not dispatched");
    if (shutdown_metadata) {
      // Only the final system-state certificate uses this bounded allocation
      // after foreground admission closes. It cannot reopen a record stream,
      // wait for defrag, or start allocator prefetch behind the shutdown drain.
      store.store_state_mutex_.Unlock(*store.worker_);
      reserved =
          co_await AllocateBlock(store, AllocationPurpose::kShutdownMetadata);
      co_await store.store_state_mutex_.Lock();
    } else {
      reserved =
          co_await AcquireWriteBlock(store, false, /*unlock_writer=*/true);
    }
    if (!reserved.ok()) {
      reclaim_allocated();
      co_return reserved.status();
    }
    const std::size_t payload_bytes =
        static_cast<std::size_t>(std::min<std::uint64_t>(
            kExtentPayloadBytes, logical_bytes - payload_offset));
    BlockState& state = CreateBlockState(store, reserved->block_id_,
                                         reserved->allocation_epoch_);
    state.writer_id_ = store.worker_->id();
    state.layout_worker_count_ = worker_count_;
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
    auto allocated_lsn = AllocateLsn(store);
    if (!allocated_lsn.ok()) {
      release_buffer();
      reclaim_allocated();
      co_return allocated_lsn.status();
    }
    // This extent is not reachable from any root yet. Its live-byte charge
    // and explicit pin keep allocation identity stable while this coroutine
    // exclusively owns the buffer/cursor. Only private bytes and device IO
    // are touched outside store state; body-before-header durability is intact.
    ++state.pins_;
    auto write_extent = [&]() -> Task<absl::Status> {
      LAVIK_FAULT_INJECT(if (extent_index == 0) {
        const auto paused = co_await PauseGroupedWriteForTest(
            *store.worker_, fault_key, "extent");
        if (!paused.ok()) co_return paused;
      });
      std::fill_n(staging.data_, kStorageBlockBytes, std::byte{0});
      std::size_t copied = 0;
      if (cursor != nullptr) {
        auto copied_status = cursor->Read(std::span<std::byte>(
            staging.data_ + kBlockHeaderBytes, payload_bytes));
        if (!copied_status.ok()) {
          co_return copied_status;
        }
        copied = payload_bytes;
      }
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
            write_buffer_id != 0 && store.buffers_.buffers_registered(),
            staging, block_offset + offset);
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
            co_await bycorf::Fdatasync(*store.worker_, store.files_[file_id]);
      }
      if (write_status.ok()) {
        auto written = co_await WriteStorageBuffer(
            *store.worker_, store.files_[file_id],
            std::span<const std::byte>(staging.data_, kBlockHeaderSlotBytes),
            write_buffer_id != 0 && store.buffers_.buffers_registered(),
            staging, block_offset);
        if (!written.ok() || *written != kBlockHeaderSlotBytes) {
          write_status = written.ok()
                             ? absl::Status(absl::StatusCode::kInternal,
                                            "short extent header write")
                             : written.status();
        }
      }
      if (write_status.ok()) {
        write_status =
            co_await bycorf::Fdatasync(*store.worker_, store.files_[file_id]);
      }
      co_return write_status;
    };
    store.store_state_mutex_.Unlock(*store.worker_);
    absl::Status extent_status;
    std::exception_ptr exception;
    try {
      extent_status = co_await write_extent();
    } catch (...) {
      exception = std::current_exception();
    }
    // Restore the caller's lock invariant on every exit, including failure to
    // allocate the IO coroutine frame. No destructor attempts an async lock.
    co_await store.store_state_mutex_.Lock();
    assert(state.allocation_epoch_ == reserved->allocation_epoch_ &&
           state.pins_ != 0);
    --state.pins_;
    release_buffer();
    if (exception) {
      reclaim_allocated();
      std::rethrow_exception(exception);
    }
    if (!extent_status.ok()) {
      LatchRuntimeFailure(store);
      reclaim_allocated();
      co_return extent_status;
    }
    if (store.write_failed_) {
      reclaim_allocated();
      co_return absl::FailedPreconditionError(
          "storage writer stopped during extent IO");
    }
    // The first extent has a durable header and allocation bit, but the
    // remaining payload and keyed manifest have not been published. Recovery
    // must reclaim this orphan without losing the previous complete value.
    if (payload_offset == 0 && payload_bytes < logical_bytes) {
      LAVIK_MAYBE_CRASH_AT("extent-first-part-durable");
    }
    payload_offset += payload_bytes;
    ++extent_index;
  }
  if (cursor != nullptr) {
    auto finished = cursor->Finish();
    if (!finished.ok()) {
      reclaim_allocated();
      co_return finished;
    }
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
    SetLatencyTrace* trace, bool capture_fullsync, TxUndoLog* replacement_undo,
    const MutationPrecondition* mutation_precondition,
    GroupMutationWrite* grouped) {
  if (RuntimeFailureLatched()) {
    co_return absl::FailedPreconditionError(
        "storage writer is stopped after an IO failure");
  }
  if (logical_size == std::numeric_limits<std::uint64_t>::max()) {
    logical_size = value.size();
  }
  if (!ValidRecordKeySize(key.size())) {
    co_return absl::OutOfRangeError("record key exceeds storage limit");
  }
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
        .allow_equal_sequence_ = true,
    });
  }
  if (grouped != nullptr &&
      (grouped->sequence_ == 0 || grouped->root_ == nullptr ||
       (replica_mutation_sequence.has_value() &&
        *replica_mutation_sequence != grouped->sequence_))) {
    co_return absl::InvalidArgumentError("invalid grouped mutation sequence");
  }
  const std::uint64_t mutation_sequence =
      grouped != nullptr                      ? grouped->sequence_
      : replica_mutation_sequence.has_value() ? *replica_mutation_sequence
                                              : ++partition.mutation_sequence_;
  std::shared_ptr<const ReplicationCommandAppend> fullsync_command;
  if (replication != nullptr) {
    AppendReplicationExpirationEffect(
        &replication->args_, db_id, db_id, key, kind == RecordKind::kValue,
        kind == RecordKind::kValue ? expire_at_ms : 0);
    replication->db_id_ = db_id;
    replication->partition_id_ = partition.id_;
    replication->partition_sequence_ = mutation_sequence;
    if (!partition.fullsync_subscribers_.empty()) [[unlikely]] {
      // Build the subscriber-owned copy before writing. Active sessions hold
      // fixed staging budgets; a physical allocation failure is fatal rather
      // than converted into a recoverable command error.
      fullsync_command =
          std::make_shared<ReplicationCommandAppend>(*replication);
    }
  }
  absl::Status status = absl::OkStatus();
  const bool key_external = key.size() > options_.inline_key_max_bytes_;
  const std::uint64_t logical_payload_bytes =
      static_cast<std::uint64_t>(value.size()) +
      (key_external ? key.size() : 0);
  const std::size_t inline_bytes =
      AlignRecord(RecordHeaderBytes(key.size(), key_external, tx != nullptr,
                                    expire_at_ms != 0) +
                  static_cast<std::size_t>(logical_payload_bytes));
  if (inline_bytes > kStorageBlockBytes - kBlockHeaderBytes) [[unlikely]] {
    auto extents = co_await WriteExtentValueLocked(
        store, key_external ? key : std::string_view{}, value);
    if (!extents.ok()) {
      co_return extents.status();
    }
    if (value_type == ValueType::kHash) {
      LAVIK_MAYBE_CRASH_AT("hash-extents-durable-before-root");
    }
    const std::string manifest = EncodeManifest(**extents);
    status = co_await WriteRecordLocked(
        store, db_id, key, manifest, kind, value_type, expire_at_ms, digest,
        /*txid=*/0, mutation_sequence, false, true, true, key_external,
        logical_size, *extents, nullptr, nullptr, tx,
        std::move(commit_retirements), trace,
        replica_write_root.has_value() ? &*replica_write_root : nullptr,
        replacement_undo, &partition,
        grouped != nullptr ? grouped->root_ : nullptr,
        /*mark_watched=*/true, mutation_precondition);
    if (!status.ok()) {
      store.worker_->Spawn(ReclaimExtents(&store, *extents));
    }
  } else {
    status = co_await WriteRecordLocked(
        store, db_id, key, value, kind, value_type, expire_at_ms, digest,
        /*txid=*/0, mutation_sequence, false, true, false, key_external,
        logical_size, nullptr, nullptr, nullptr, tx,
        std::move(commit_retirements), trace,
        replica_write_root.has_value() ? &*replica_write_root : nullptr,
        replacement_undo, &partition,
        grouped != nullptr ? grouped->root_ : nullptr,
        /*mark_watched=*/true, mutation_precondition);
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
  if (status.ok() && replication != nullptr) {
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

void StorageEngine::Impl::InvalidateFullSyncSession(WorkerStore& store,
                                                    std::uint64_t session_id) {
  auto session = store.fullsync_sessions_.find(session_id);
  if (session != store.fullsync_sessions_.end()) {
    session->second.db_epoch_invalidated_ = true;
  }
  store.fullsync_publisher_capacity_ready_.NotifyAll(*store.worker_);
}

bool StorageEngine::Impl::TryConsumeFullSyncCoverageCredit(
    WorkerStore& store, std::uint64_t session_id,
    WorkerStore::FullSyncCapture& capture, std::size_t key_bytes,
    bool allocates_arena_entry) {
  constexpr std::size_t kArenaFixedBytes =
      ScanHashMapEntryArena::kSmallSpanAdmissionBytes;
  std::size_t bytes = kFullSyncReplacementMetadataBytes;
  if (key_bytes > (std::numeric_limits<std::size_t>::max() - bytes) / 2) {
    RecordMemoryRejection();
    InvalidateFullSyncSession(store, session_id);
    return false;
  }
  bytes += key_bytes * 2;
  // The first identity also pays for the arena span, direct bucket, and
  // container control allocations. The caller performs its first arena
  // insertion synchronously after this conversion, so foreground admission
  // cannot consume the gap between logical and allocator accounting.
  const bool consume_arena_credit =
      allocates_arena_entry && !capture.arena_credit_consumed_;
  if (consume_arena_credit) {
    if (bytes > std::numeric_limits<std::size_t>::max() - kArenaFixedBytes) {
      RecordMemoryRejection();
      InvalidateFullSyncSession(store, session_id);
      return false;
    }
    bytes += kArenaFixedBytes;
  } else if (!capture.arena_credit_consumed_) {
    // Override-only identities may use per-key credit, but they cannot spend
    // the physical-span slice that a later scanner/ACK needs to allocate the
    // arena. The separate control allowance remains available for a bounded
    // number of post-fence identities in an otherwise empty DB.
    auto session = store.fullsync_sessions_.find(session_id);
    if (session == store.fullsync_sessions_.end() ||
        session->second.db_epoch_invalidated_ ||
        session->second.available_memory_bytes_ < kArenaFixedBytes ||
        bytes > session->second.available_memory_bytes_ - kArenaFixedBytes) {
      RecordMemoryRejection();
      InvalidateFullSyncSession(store, session_id);
      return false;
    }
  }
  if (!TryConsumeFullSyncCredit(store, session_id, capture, bytes)) {
    return false;
  }
  if (consume_arena_credit) {
    capture.key_phases_.SetEntryArena(std::make_shared<ScanHashMapEntryArena>(
        ScanHashMapEntryArena::kMaximumPageId,
        /*externally_admitted=*/true,
        /*externally_accounted=*/true));
    capture.arena_credit_consumed_ = true;
  }
  return true;
}

bool StorageEngine::Impl::TryConsumeFullSyncCredit(
    WorkerStore& store, std::uint64_t session_id,
    WorkerStore::FullSyncCapture& capture, std::size_t bytes) {
  auto session = store.fullsync_sessions_.find(session_id);
  if (session == store.fullsync_sessions_.end() ||
      session->second.db_epoch_invalidated_ ||
      bytes > session->second.available_memory_bytes_ ||
      bytes > std::numeric_limits<std::size_t>::max() -
                  capture.memory_credit_bytes_) {
    RecordMemoryRejection();
    InvalidateFullSyncSession(store, session_id);
    return false;
  }
  ConsumeFullSyncMemory(bytes);
  session->second.available_memory_bytes_ -= bytes;
  capture.memory_credit_bytes_ += bytes;
  return true;
}

bool StorageEngine::Impl::TryConsumeFullSyncArenaCredit(
    WorkerStore& store, std::uint64_t session_id,
    WorkerStore::FullSyncCapture& capture) {
  if (capture.arena_credit_consumed_) return true;
  constexpr std::size_t kArenaFixedBytes =
      ScanHashMapEntryArena::kSmallSpanAdmissionBytes;
  if (!TryConsumeFullSyncCredit(store, session_id, capture, kArenaFixedBytes)) {
    return false;
  }
  capture.key_phases_.SetEntryArena(std::make_shared<ScanHashMapEntryArena>(
      ScanHashMapEntryArena::kMaximumPageId,
      /*externally_admitted=*/true,
      /*externally_accounted=*/true));
  capture.arena_credit_consumed_ = true;
  return true;
}

void StorageEngine::Impl::RestoreFullSyncCoverageCredit(
    WorkerStore& store, std::uint64_t session_id,
    WorkerStore::FullSyncCapture& capture) {
  if (capture.memory_credit_bytes_ == 0) return;
  auto session = store.fullsync_sessions_.find(session_id);
  assert(session != store.fullsync_sessions_.end());
  assert(session->second.available_memory_bytes_ <=
         session->second.reserved_memory_bytes_);
  assert(capture.memory_credit_bytes_ <=
         session->second.reserved_memory_bytes_ -
             session->second.available_memory_bytes_);
  RestoreFullSyncMemory(capture.memory_credit_bytes_);
  session->second.available_memory_bytes_ += capture.memory_credit_bytes_;
  capture.memory_credit_bytes_ = 0;
  capture.arena_credit_consumed_ = false;
}

void StorageEngine::Impl::ClearFullSyncCapture(
    WorkerStore& store, std::uint64_t session_id,
    WorkerStore::FullSyncCapture& capture) {
  for (auto& [_, pinned] : capture.pinned_values_) {
    if (pinned.collection_ != nullptr) {
      active_settlements_.fetch_add(1, std::memory_order_acq_rel);
      store.worker_->Spawn(
          ReleaseFullSyncCollection(std::move(pinned.collection_)));
    } else {
      store.worker_->Spawn(ReleaseFullSyncExtents(std::move(pinned.extents_)));
    }
  }
  capture.pinned_values_.clear();
  capture.pinned_values_.rehash(0);
  capture.pinned_values_charge_.Reset();
  capture.overrides_.clear();
  for (auto& latest : capture.latest_by_key_) {
    latest.clear();
    latest.rehash(0);
  }
  capture.replacement_credit_bytes_ = 0;
  capture.key_phases_.Clear();
  // Clear() releases entries and buckets, but the map still owns its arena
  // directory. Destroy that arena before restoring the logical credit; doing
  // so preserves the invariant that every consumed byte is either live in an
  // allocator-owned capture structure or available in the session reserve.
  capture.key_phases_ = ScanHashMap<WorkerStore::FullSyncCapture::KeyPhase>{};
  decltype(capture.pending_snapshot_keys_){}.swap(
      capture.pending_snapshot_keys_);
  capture.pending_snapshot_cursor_ = 0;

  // Container destruction publishes allocator frees synchronously. Restore
  // the logical reservation afterwards, so the next partition can consume the
  // same credit without ever hiding live coverage bytes from admission.
  RestoreFullSyncCoverageCredit(store, session_id, capture);
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
  const auto active_session = store.fullsync_sessions_.find(session_id);
  if (active_session == store.fullsync_sessions_.end() ||
      active_session->second.db_epoch_invalidated_) {
    return;
  }
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
  auto& latest_by_key = capture.latest_by_key_[record.db_id_];
  auto found = latest_by_key.find(record.key_);
  const bool replacing = found != latest_by_key.end();
  const bool transfers_coverage_credit = phase != nullptr;
  if (!replacing && !transfers_coverage_credit &&
      !TryConsumeFullSyncCoverageCredit(store, session_id, capture,
                                        record.key_.size(),
                                        /*allocates_arena_entry=*/false)) {
    // The durable foreground mutation remains valid. Full sync is the
    // lower-priority consumer, so invalidate only that session before any
    // unbudgeted override container allocation can occur.
    return;
  }
  if (phase != nullptr &&
      phase->value_ !=
          WorkerStore::FullSyncCapture::KeyPhase::kTailingWithOverrideCredit &&
      record.key_.size() > options_.inline_key_max_bytes_) {
    assert(record.key_.size() >= sizeof(Digest));
    const std::size_t additional_key_bytes =
        (record.key_.size() - sizeof(Digest)) * 2;
    if (!TryConsumeFullSyncCredit(store, session_id, capture,
                                  additional_key_bytes)) {
      return;
    }
  }
  if (phase != nullptr) {
    capture.key_phases_.Erase(phase);
  }
  if (found != latest_by_key.end()) {
    auto previous = capture.overrides_.find(found->second);
    assert(previous != capture.overrides_.end());
    capture.overrides_.erase(previous);
    latest_by_key.erase(found);
  }
  if (!replacing) {
    auto session = store.fullsync_sessions_.find(session_id);
    std::size_t credit = kFullSyncReplacementMetadataBytes;
    if (record.key_.size() >
        (std::numeric_limits<std::size_t>::max() - credit) / 2) {
      InvalidateFullSyncSession(store, session_id);
      return;
    }
    credit += record.key_.size() * 2;
    if (session == store.fullsync_sessions_.end() ||
        credit > std::numeric_limits<std::size_t>::max() -
                     session->second.publish_queue_bytes_ ||
        credit > std::numeric_limits<std::size_t>::max() -
                     capture.replacement_credit_bytes_) {
      InvalidateFullSyncSession(store, session_id);
      return;
    }
    session->second.publish_queue_bytes_ += credit;
    capture.replacement_credit_bytes_ += credit;
  }
  auto [override, inserted] =
      capture.overrides_.emplace(record.mutation_sequence_, record);
  if (!inserted) {
    InvalidateFullSyncSession(store, session_id);
    return;
  }
  auto [latest, latest_inserted] = latest_by_key.emplace(
      std::string(record.key_), record.mutation_sequence_);
  (void)latest;
  if (!latest_inserted) {
    capture.overrides_.erase(override);
    InvalidateFullSyncSession(store, session_id);
  }
}

bool StorageEngine::Impl::TryEnqueueFullSyncCommand(
    WorkerStore& store, std::uint64_t session_id,
    std::shared_ptr<const ReplicationCommandAppend> command) {
  static_assert(sizeof(WorkerStore::FullSyncSessionState::PendingCommand) <=
                kReplicationPublisherItemMetadataBytes);
  auto session = store.fullsync_sessions_.find(session_id);
  if (session == store.fullsync_sessions_.end() ||
      session->second.db_epoch_invalidated_ || command == nullptr ||
      command->args_.empty()) {
    return false;
  }
  const auto staging_bytes = ReplicationCommandStagingBytes(command->args_);
  if (!staging_bytes.has_value()) {
    session->second.db_epoch_invalidated_ = true;
    store.fullsync_publisher_capacity_ready_.NotifyAll(*store.worker_);
    return false;
  }
  const std::size_t logical_bytes = *staging_bytes;
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
  if (!TryPreparePostMutationQueueSlot(&state.publish_queue_,
                                       state.publisher_admitted_items_)) {
    state.publish_queue_bytes_ -= logical_bytes;
    state.db_epoch_invalidated_ = true;
    RecordMemoryRejection();
    store.fullsync_publisher_capacity_ready_.NotifyAll(*store.worker_);
    return false;
  }
  state.publish_queue_.push_back_prepared(
      WorkerStore::FullSyncSessionState::PendingCommand{
          .id_ = id,
          .staging_bytes_ = logical_bytes,
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
  if (!TryPreparePostMutationQueueSlot(&state.publish_queue_,
                                       state.publisher_admitted_items_)) {
    state.publish_queue_bytes_ -= logical_bytes;
    state.db_epoch_invalidated_ = true;
    RecordMemoryRejection();
    store.fullsync_publisher_capacity_ready_.NotifyAll(*store.worker_);
    return false;
  }
  WorkerStore::FullSyncSessionState::PendingCommand pending{
      .id_ = id,
      .staging_bytes_ = logical_bytes,
      .command_ = nullptr,
      .record_ = record,
  };
  state.publish_queue_.push_back_prepared(std::move(pending));
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
    SetLatencyTrace* trace, const ExplicitWriteRoot* explicit_root,
    TxUndoLog* replacement_undo, WorkerStore::PartitionStore* known_partition,
    const GroupRecordWrite* group, bool mark_watched,
    const MutationPrecondition* mutation_precondition) {
  if (store.write_failed_ || RuntimeFailureLatched() ||
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
    if (LAVIK_MAYBE_FAIL_TX_WRITE(key)) {
      co_return absl::Status(absl::StatusCode::kInternal,
                             "injected transaction write fault");
    }
  }
  const bool auxiliary = group != nullptr && group->auxiliary_;
  const bool grouped_root = group != nullptr && !group->auxiliary_;
  if (group != nullptr &&
      (kind != RecordKind::kValue ||
       (value_type != ValueType::kHash && value_type != ValueType::kSet &&
        value_type != ValueType::kList &&
        value_type != ValueType::kSortedSet) ||
       mutation_sequence == 0 ||
       (auxiliary &&
        (group->incarnation_ == 0 ||
         ((value_type == ValueType::kList ||
           (value_type == ValueType::kSortedSet && IsOrderedPageId(group->id_)))
              ? (group->id_.prefix_ == 0 || group->id_.bits_ != 0)
              : !group->id_.valid()) ||
         expire_at_ms != 0 || explicit_root != nullptr ||
         (group->retired_ && logical_size != 0) ||
         (group->batch_txid_ != 0 && txid == 0) ||
         (tx == nullptr && !for_defrag))) ||
       (grouped_root &&
        (logical_size == 0 || group->incarnation_ != 0 ||
         group->id_ != HashGroupId{} || group->retired_ ||
         group->batch_txid_ != 0 || group->prepared_root_ == nullptr ||
         group->publication_ == nullptr)))) {
    co_return absl::InvalidArgumentError("invalid grouped record write");
  }
  struct FailIncompleteGroupedRoot {
    WorkerStore& store_;
    TxShardWrites* tx_;
    bool armed_ = false;
    bool completed_ = false;
    ~FailIncompleteGroupedRoot() {
      if (!armed_ || completed_) return;
      // A staged root carries the outer transaction's publication decision.
      // No later command may commit that decision after this command failed,
      // including when its coordinator lives on another healthy worker.
      store_.write_failed_ = true;
      if (tx_ != nullptr && tx_->grouped_decision_ != nullptr) {
        tx_->grouped_decision_->FailPending();
      }
    }
  } grouped_root_guard{store, tx};
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
  const bool invalid_logical_size =
      (value_type == ValueType::kString && logical_size > kMaxBitmapBytes) ||
      logical_size > std::numeric_limits<std::uint32_t>::max();
  const std::uint64_t key_prefix = key_external ? key.size() : 0;
  if (!ValidRecordKeySize(key.size()) || invalid_logical_size ||
      value.size() > kMaxRecordPayloadBytes ||
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
  const std::size_t record_header_bytes = RecordHeaderBytes(
      key.size(), key_external, txid != 0, expire_at_ms != 0, auxiliary);
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
  const bycorf::WorkerId writer_id = store.worker_->id();
  // Commit records are keyless and belong to no partition: they append
  // wherever their coordinator runs, and recovery reads them independently
  // of any partition's epochs.
  WorkerStore::PartitionStore* partition_ptr =
      kind == RecordKind::kTxCommit
          ? nullptr
          : (known_partition != nullptr ? known_partition
                                        : &PartitionForKey(store, key));
  assert(known_partition == nullptr || known_partition->id_ == RedisSlot(key));
  RecordIndex* index_ptr =
      auxiliary ? nullptr
      : explicit_root != nullptr
          ? explicit_root->index_
          : (partition_ptr == nullptr ? nullptr
                                      : &partition_ptr->indexes_[db_id]);
  const bool transaction_append = txid != 0;
  const std::uint64_t tx_generation =
      transaction_append && tx != nullptr ? tx->generation_ : 0;
  if (transaction_append && tx_generation == 0) {
    co_return absl::InvalidArgumentError(
        "transaction record has no generation lease");
  }
  const BlockKind append_block_kind =
      transaction_append ? BlockKind::kTransaction : BlockKind::kRecords;
  LAVIK_FAULT_INJECT(
      if (!for_defrag && !key.empty() &&
          LAVIK_FAULT_MATCHES("LAVIK_RECORD_WRITE_PAUSE_KEY", key)) {
        // A deterministic publication-order race: let GC publish the previous
        // value while this foreground append has not acquired its final stream.
        spdlog::info("record write publication pause armed");
        store.store_state_mutex_.Unlock(*store.worker_);
        auto paused = co_await bycorf::SleepFor(
            *store.worker_, std::chrono::milliseconds(1000));
        co_await store.store_state_mutex_.Lock();
        if (!paused.ok()) co_return paused;
      });
  // Never keep a flat_hash_map value reference across an await that can
  // release store_state_mutex_. Another transaction may install a different
  // generation and rehash active_tx_blocks_ while block allocation is in
  // flight. Re-resolve the entry on every use; active_block_ itself is stable.
  auto active_stream = [&]() -> std::optional<ActiveBlock>& {
    return transaction_append ? store.active_tx_blocks_[tx_generation]
                              : store.active_block_;
  };
  AsyncMutex* allocation_mutex = &store.active_block_allocation_mutex_;
  if (transaction_append) {
    auto& gate = store.active_tx_block_allocation_mutexes_[tx_generation];
    if (gate == nullptr) gate = std::make_unique<AsyncMutex>();
    allocation_mutex = gate.get();
  }
  // Return paths normally run on the allocator owner. Never make unrelated
  // appends wait on that cross-core hop: the per-stream allocation gate keeps
  // other allocators out while store_state_mutex_ is released, and every
  // caller revalidates the active stream after this helper resumes.
  auto return_reserved = [&](ReservedBlock block) -> Task<absl::Status> {
    if (unlock_writer_while_waiting) {
      store.store_state_mutex_.Unlock(*store.worker_);
    }
    absl::Status returned = co_await ReturnReservedBlock(block);
    if (unlock_writer_while_waiting) {
      co_await store.store_state_mutex_.Lock();
    }
    co_return returned;
  };
  std::unique_ptr<GroupedRetirementPins> grouped_dependency_pins;
acquire_active_stream:
  if (trace != nullptr) trace->block_wait_start_ns_ = SetTraceNowNanos();
  while (!active_stream().has_value() ||
         active_stream()->committed_bytes_ + total_disk_bytes >
             kStorageBlockBytes) {
    // Waiting for a physical block must not hold store_state_mutex_: the
    // allocator, flush completion, and the elected writer may all need this
    // worker's state before the new stream can be published. The gate is per
    // append stream, so unrelated transaction generations remain concurrent.
    std::optional<UnlockGuard> allocation_unlock;
    if (unlock_writer_while_waiting) {
      store.store_state_mutex_.Unlock(*store.worker_);
      co_await allocation_mutex->Lock();
      allocation_unlock.emplace(allocation_mutex, store.worker_);
      co_await store.store_state_mutex_.Lock();

      // The elected allocator may have installed a stream before this waiter
      // reached the front. Reuse it instead of allocating a spare block.
      if (active_stream().has_value() &&
          active_stream()->committed_bytes_ + total_disk_bytes <=
              kStorageBlockBytes) {
        continue;
      }
    }
    if (active_stream().has_value()) {
      RequestFlush(store, active_stream()->block_id_);
      active_stream().reset();
    }
    absl::StatusOr<ReservedBlock> allocated{
        absl::UnavailableError("no standby block is available")};
    if (!transaction_append && store.standby_block_.has_value()) {
      allocated = *store.standby_block_;
      store.standby_block_.reset();
      if (trace != nullptr) trace->standby_block_ = true;
    } else {
      if (trace != nullptr) trace->allocated_block_ = true;
      allocated = co_await AcquireWriteBlock(store, for_defrag,
                                             unlock_writer_while_waiting);
    }
    if (!allocated.ok()) {
      co_return allocated.status();
    }
    // Another writer may have installed an active block while this coroutine
    // had store_state_mutex released. This is now limited to maintenance paths
    // that deliberately retain store_state_mutex across an atomic rewrite;
    // ordinary writers for this stream are held behind allocation_mutex.
    if (active_stream().has_value()) {
      absl::Status returned = co_await return_reserved(*allocated);
      if (!returned.ok()) co_return returned;
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
            absl::Status returned = co_await return_reserved(*allocated);
            if (!returned.ok()) co_return returned;
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
            absl::Status returned = co_await return_reserved(*allocated);
            if (!returned.ok()) co_return returned;
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
        absl::Status returned = co_await return_reserved(*allocated);
        if (!returned.ok()) co_return returned;
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
      if (!transaction_append) {
        store.standby_prefetch_for_block_.reset();
      }
      BlockState& state =
          CreateBlockState(store, block_id, active_stream()->allocation_epoch_);
      state.writer_id_ = writer_id;
      state.layout_worker_count_ = worker_count_;
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
      if (!transaction_append) {
        // Replenish immediately after installation. All later records use the
        // active block without testing an occupancy threshold; rollover either
        // consumes this successor or waits behind its stateful allocation
        // gate.
        EnsureStandbyBlock(store);
      }
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
  if (!auxiliary && relocation != nullptr && partition_ptr != nullptr &&
      (EffectiveRecordDbEpoch(*partition_ptr, db_id) != relocation->db_epoch_ ||
       partition_ptr->replication_epoch_ != relocation->replication_epoch_ ||
       store.index_generations_[db_id] != relocation->index_generation_ ||
       previous_entry == nullptr ||
       !relocation->Matches(MaterializeIndexLocation(*previous_entry)))) {
    co_return absl::Status(absl::StatusCode::kAborted,
                           "relocation source changed while waiting");
  }
  if (explicit_root != nullptr && explicit_root->reject_older_sequence_ &&
      previous_entry != nullptr &&
      (previous_entry->value_.mutation_sequence_ > mutation_sequence ||
       (!explicit_root->allow_equal_sequence_ &&
        previous_entry->value_.mutation_sequence_ == mutation_sequence))) {
    co_return absl::OkStatus();
  }
  if (!auxiliary && !for_defrag && partition_ptr != nullptr &&
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
        (previous_entry->value_.mutation_sequence_ > mutation_sequence ||
         (!explicit_root->allow_equal_sequence_ &&
          previous_entry->value_.mutation_sequence_ == mutation_sequence))) {
      co_return absl::OkStatus();
    }
  }
  if (!for_defrag && previous_entry != nullptr &&
      previous_entry->value_.grouped()) {
    auto old_view = partition_ptr->grouped_objects_[db_id].Lookup(
        key,
        GroupedObjectVersion{
            .root_ = MaterializeIndexLocation(*previous_entry),
            .db_epoch_ = EffectiveRecordDbEpoch(*partition_ptr, db_id),
            .replication_epoch_ = partition_ptr->replication_epoch_,
            .index_generation_ = partition_ptr->grouped_generations_[db_id],
        },
        /*allow_failed=*/replacement_undo != nullptr);
    if (!old_view.ok()) co_return old_view.status();
    auto pinned = co_await PrepinGroupedRetirementsLocked(
        store, *old_view,
        grouped_root && group->root_incarnation_ == (*old_view)->incarnation()
            ? std::optional(group->changed_groups_)
            : std::nullopt,
        tx != nullptr, &grouped_dependency_pins);
    if (!pinned.ok()) {
      if (absl::IsAborted(pinned)) goto acquire_active_stream;
      co_return pinned;
    }
    LAVIK_FAULT_INJECT(if (LAVIK_FAULT_MATCHES("LAVIK_GROUP_ROOT_PIN_PAUSE_KEY",
                                               key) &&
                           tx != nullptr) {
      static std::atomic<bool> root_pin_pause_claimed{false};
      if (!root_pin_pause_claimed.exchange(true, std::memory_order_relaxed)) {
        const RecordLocation root = (*old_view)->version().root_;
        const bool retained =
            grouped_dependency_pins != nullptr &&
            grouped_dependency_pins->Contains(RetiredRecordOf(root));
        // Unlike a changed-page pin in the same transaction block, this must
        // also hold for a metadata-only root update with zero touched pages.
        if (!root.tx_tagged() || !retained)
          co_return absl::InternalError("test root dependency was not pinned");
        spdlog::info(
            "group root dependency pause owner={} key-owner={} pinned=1",
            root.block_owner(), store.worker_->id());
        store.store_state_mutex_.Unlock(*store.worker_);
        auto paused = co_await bycorf::SleepFor(
            *store.worker_, std::chrono::milliseconds(1000));
        co_await store.store_state_mutex_.Lock();
        if (!paused.ok()) co_return paused;
      }
    });
    auto resolved = co_await FindVerifiedEntry(store, *index_ptr, digest, key);
    if (!resolved.ok()) co_return resolved.status();
    previous_entry = *resolved;
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
  const bool has_index_extra = expire_at_ms != 0;
  const bool needs_index_allocation =
      index_ptr != nullptr && (previous_entry == nullptr ||
                               previous_entry->has_extra() != has_index_extra);
  if (needs_index_allocation &&
      !index_ptr->CanAllocateEntry(key, !key_external, has_index_extra)) {
    // The handle's 21-bit page ID is a hard per-worker capacity boundary.
    // Check it after every suspension and before mutating the staging buffer,
    // so exhaustion is reported without leaving a durable record whose index
    // entry cannot be published. Page IDs are never allowed to wrap.
    co_return absl::ResourceExhaustedError(
        "record index entry page capacity exhausted");
  }
  if (!auxiliary && tx != nullptr && tx->collect_undo_) {
    const auto undo = store.tx_undo_.find(txid);
    if (undo != store.tx_undo_.end() &&
        !undo->second.CanTrack(previous_entry)) {
      // The journal must reject its deterministic handle limit before the
      // durable staging buffer changes; allocator failure while growing the
      // admitted journal is a process-fatal physical OOM instead.
      co_return absl::ResourceExhaustedError(
          "transaction undo handle capacity exhausted");
    }
  }
  std::optional<MemoryReservation> index_memory_reservation;
  if (needs_index_allocation) {
    const std::size_t allocation_bytes = index_ptr->RequiredAllocationBytes(
        digest, key, !key_external, has_index_extra, previous_entry == nullptr);
    if (allocation_bytes != 0) {
      index_memory_reservation = TryReserveMemory(allocation_bytes);
      if (!index_memory_reservation.has_value()) {
        RecordMemoryRejection();
        co_return absl::ResourceExhaustedError(
            "OOM record index allocation exceeds this worker's maxmemory "
            "share");
      }
    }
  }
  const std::optional<RecordLocation> previous =
      previous_entry == nullptr
          ? std::nullopt
          : std::optional<RecordLocation>(
                MaterializeIndexLocation(*previous_entry));
  const ExtentManifest previous_extents = ExtentsFor(store, previous_entry);
  const ExtentManifest retired_value_extents = ExtentsNotReferencedBy(
      previous_extents, external && !key_external ? extents : nullptr);
  const ExtentManifest previous_dependent_extents =
      DependentExtentsFor(store, previous_entry);
  ActiveBlock updated = *active_stream();
  const std::uint32_t record_offset = updated.committed_bytes_;
  updated.committed_bytes_ += static_cast<std::uint32_t>(total_disk_bytes);
  ++updated.record_count_;

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
  if (grouped_root && group->prepare_root_) {
    const RecordLocation provisional(
        updated.block_id_, mutation_sequence, updated.allocation_epoch_,
        expire_at_ms, static_cast<std::uint32_t>(logical_size),
        RecordLocation::PackedMetadata::Encode(
            record_offset, static_cast<std::uint32_t>(total_disk_bytes),
            writer_id, true, external, key_external, false, false, txid != 0,
            kind, value_type, expire_at_ms != 0, true));
    const auto prepared = group->prepare_root_(GroupedObjectVersion{
        .root_ = provisional,
        .db_epoch_ = explicit_root != nullptr
                         ? explicit_root->db_epoch_
                         : EffectiveRecordDbEpoch(*partition_ptr, db_id),
        .replication_epoch_ = explicit_root != nullptr
                                  ? explicit_root->replication_epoch_
                                  : partition_ptr->replication_epoch_,
        .index_generation_ = partition_ptr->grouped_generations_[db_id],
    });
    if (!prepared.ok()) co_return prepared;
  }
  GroupedHashObject::Handle previous_grouped;
  std::shared_ptr<std::vector<RetiredRecord>> grouped_retirements;
  std::shared_ptr<std::vector<RetiredRecord>> grouped_abort_retirements;
  GroupedHashObject::Handle replacement_grouped =
      grouped_root ? GroupedHashObject::Handle(*group->prepared_root_)
                   : nullptr;
  auto touched_groups =
      grouped_root ? std::optional(group->changed_groups_) : std::nullopt;
  if (!for_defrag && previous && previous->grouped()) {
    auto old_view = partition_ptr->grouped_objects_[db_id].Lookup(
        key,
        GroupedObjectVersion{
            .root_ = *previous,
            .db_epoch_ = EffectiveRecordDbEpoch(*partition_ptr, db_id),
            .replication_epoch_ = partition_ptr->replication_epoch_,
            .index_generation_ = partition_ptr->grouped_generations_[db_id],
        },
        /*allow_failed=*/replacement_undo != nullptr);
    if (!old_view.ok()) co_return old_view.status();
    previous_grouped = std::move(*old_view);
    if (tx != nullptr && previous->tx_tagged() &&
        (grouped_dependency_pins == nullptr ||
         !grouped_dependency_pins->Contains(RetiredRecordOf(*previous)))) {
      // The physical root can move independently of every unchanged child,
      // including during a TTL-only write. Re-enter the pre-stage owner-hop
      // protocol instead of borrowing the new block's transaction identity.
      goto acquire_active_stream;
    }
    if (tx != nullptr) {
      // Replacing a grouped graph also needs a shared failure decision: the
      // top-level root may be compact, but its old graph is still atomic.
      auto decision = PrepareGroupedDecision(*tx);
      if (!decision.ok()) co_return decision.status();
    }
    if (replacement_grouped != nullptr &&
        previous_grouped->incarnation() != replacement_grouped->incarnation())
      touched_groups.reset();
    auto retired = CollectGroupedRetirements(
        previous_grouped, replacement_grouped, touched_groups);
    if (!retired.ok()) co_return retired.status();
    for (const auto& child : *retired) {
      if (child.tx_tagged_ && (grouped_dependency_pins == nullptr ||
                               !grouped_dependency_pins->Contains(child))) {
        // A physical-only relocation won during pinning/index resolution.
        // Nothing is staged yet; re-pin the new identity before re-entering
        // this non-suspending root publication section.
        goto acquire_active_stream;
      }
    }
    if (!retired->empty())
      grouped_retirements =
          std::make_shared<std::vector<RetiredRecord>>(std::move(*retired));
  }
  if (!for_defrag && tx != nullptr && tx->collect_undo_ &&
      replacement_grouped != nullptr) {
    auto discarded = CollectGroupedRetirements(
        replacement_grouped, previous_grouped, touched_groups);
    if (!discarded.ok()) co_return discarded.status();
    if (!discarded->empty())
      grouped_abort_retirements =
          std::make_shared<std::vector<RetiredRecord>>(std::move(*discarded));
  }
  if (grouped_retirements != nullptr) {
    // Capacity is admitted before the first staged byte. All later routing
    // consists only of moves/copies into these preallocated receipt vectors.
    if (tx != nullptr) {
      tx->retirements_.reserve(tx->retirements_.size() +
                               grouped_retirements->size() + 1);
    } else {
      if (commit_retirements == nullptr)
        commit_retirements = std::make_unique<std::vector<RetiredRecord>>();
      commit_retirements->reserve(commit_retirements->size() +
                                  grouped_retirements->size());
    }
  }
  // FinalizeRoot requires exclusive ownership of its unpublished builder.
  // These temporary const aliases were used only for fallible preparation.
  replacement_grouped.reset();
  // Assign the physical tie-breaker only after the final suspension/retry.
  // In particular, GC may publish the old value while a foreground replay
  // write waits for allocation. Reserving this LSN before that wait would let
  // the old GC copy outrank the later publication at the same command seq.
  // From here through index/side publication the owner never yields.
  auto allocated_lsn = AllocateLsn(store);
  if (!allocated_lsn.ok()) co_return allocated_lsn.status();
  const std::uint64_t lsn = *allocated_lsn;
  updated.max_lsn_ = std::max(updated.max_lsn_, lsn);
  LAVIK_FAULT_INJECT(
      if (!auxiliary &&
          LAVIK_FAULT_MATCHES("LAVIK_RECORD_WRITE_PAUSE_KEY", key)) {
        spdlog::info("record publication test type={} lsn={}",
                     static_cast<unsigned>(value_type), lsn);
      });
  // This is the last no-await cut before the staging buffer and key index can
  // change. An explicit empty precondition is meaningful: rollback uses it to
  // bypass the stale admission inherited from its TxShardWrites receipt.
  const MutationPrecondition* effective_precondition = mutation_precondition;
  if (effective_precondition == nullptr && tx != nullptr) {
    effective_precondition = &tx->mutation_precondition_;
  }
  if (index_ptr != nullptr && effective_precondition != nullptr &&
      static_cast<bool>(*effective_precondition)) {
    absl::Status admissible = effective_precondition->Validate();
    if (!admissible.ok()) co_return admissible;
  }
  // WATCH invalidation belongs to the same linearization cut as publication:
  // rejected authority checks must not invalidate it, while an observer must
  // never see the new index value before the watch fingerprint changes.
  if (index_ptr != nullptr && mark_watched) {
    tx::CurrentTxShard().MarkWatched(db_id, tx::FingerprintOf(digest));
  }
  // The encoder overwrites the complete header and the copies below overwrite
  // the complete payload. Preserve deterministic on-disk padding without
  // clearing those bytes twice on every append.
  const std::size_t encoded_record_bytes = record_header_bytes + payload_bytes;
  grouped_root_guard.armed_ = grouped_root || previous_grouped != nullptr;
  std::fill_n(staging.data_ + record_offset + encoded_record_bytes,
              total_disk_bytes - encoded_record_bytes, std::byte{0});
  RecordHeader record{
      .header_bytes_ = static_cast<std::uint16_t>(record_header_bytes),
      .kind_ = kind,
      .db_id_ = db_id,
      .value_type_ = value_type,
      .external_ = external,
      .key_external_ = key_external,
      .grouped_ = grouped_root,
      .auxiliary_group_ = auxiliary,
      .group_retired_ = auxiliary && group->retired_,
      .group_incarnation_ = auxiliary ? group->incarnation_ : 0,
      .group_prefix_ = auxiliary ? group->id_.prefix_ : 0,
      .group_prefix_bits_ = auxiliary ? group->id_.bits_ : std::uint8_t{0},
      .group_batch_txid_ = auxiliary ? group->batch_txid_ : 0,
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
      .db_epoch_ =
          explicit_root != nullptr
              ? explicit_root->db_epoch_
              : (relocation != nullptr
                     ? relocation->db_epoch_
                     : (partition_ptr != nullptr
                            ? EffectiveRecordDbEpoch(*partition_ptr, db_id)
                            : DbEpoch(db_id))),
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
          // default rather than spending one of the remaining reserve
          // bits.
          kind == RecordKind::kTxCommit ? RecordKind::kValue : kind, value_type,
          expire_at_ms != 0, grouped_root));
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
  if (grouped_root) {
    GroupedObjectVersion version{
        .root_ = location,
        .db_epoch_ = record.db_epoch_,
        .replication_epoch_ = record.replication_epoch_,
        .index_generation_ = partition_ptr->grouped_generations_[db_id],
        .decision_ = tx != nullptr ? tx->grouped_decision_ : nullptr,
    };
    absl::Status finalized;
    if (for_defrag) {
      auto current = partition_ptr->grouped_objects_[db_id].Lookup(
          key,
          GroupedObjectVersion{
              .root_ = *previous,
              .db_epoch_ = record.db_epoch_,
              .replication_epoch_ = record.replication_epoch_,
              .index_generation_ = partition_ptr->grouped_generations_[db_id],
          });
      if (current.ok()) version.decision_ = (*current)->version().decision_;
      finalized = current.ok() ? GroupedHashObject::FinalizeRootRelocation(
                                     *group->prepared_root_, *current, version)
                               : current.status();
      if (finalized.ok()) {
        finalized = group->publication_->RefreshExpected(*current);
      }
    } else {
      finalized =
          GroupedHashObject::FinalizeRoot(*group->prepared_root_, version);
    }
    if (!finalized.ok()) {
      // This can only be an internal contract violation after the validated
      // builder was admitted. Never acknowledge a root without its view.
      store.write_failed_ = true;
      co_return finalized;
    }
  }
  RecordIndex::Entry* inserted_entry = nullptr;
  if (index_ptr != nullptr) {
    if (previous_entry != nullptr) {
      TxUndoLog* current_tx_undo = replacement_undo;
      if (current_tx_undo == nullptr && tx != nullptr && tx->collect_undo_) {
        if (auto found = store.tx_undo_.find(txid);
            found != store.tx_undo_.end()) {
          current_tx_undo = &found->second;
        }
      }
      auto replaced = ReplaceIndexLocation(store, *index_ptr, previous_entry,
                                           digest, location, current_tx_undo);
      if (!replaced.ok()) {
        LatchRuntimeFailure(store);
        co_return replaced.status();
      }
      inserted_entry = *replaced;
    } else {
      inserted_entry =
          index_ptr->InsertNew(digest, key, location, !key_external);
      if (inserted_entry == nullptr) {
        LatchRuntimeFailure(store);
        co_return absl::ResourceExhaustedError(
            "record index entry capacity exhausted");
      }
      assert(partition_ptr != nullptr);
      AddFullSyncCoverageEntry(*partition_ptr, db_id, key.size());
    }
    if (external) {
      store.external_manifests_.insert_or_assign(inserted_entry, extents);
    } else {
      store.external_manifests_.erase(inserted_entry);
    }
  }
  if (grouped_root) {
    auto published = group->publication_->Commit(*group->prepared_root_);
    if (!published.ok()) {
      store.write_failed_ = true;
      co_return published;
    }
  } else if (previous_grouped != nullptr) {
    const bool retain_undo_slot =
        (tx != nullptr && tx->collect_undo_) || replacement_undo != nullptr;
    auto removed =
        retain_undo_slot
            ? partition_ptr->grouped_objects_[db_id].ClearKeepingSlot(
                  key, previous_grouped)
            : partition_ptr->grouped_objects_[db_id].Erase(key,
                                                           previous_grouped);
    if (!removed.ok()) {
      store.write_failed_ = true;
      co_return removed;
    }
  }
  const bool route_to_commit = tx != nullptr && previous.has_value();
  const bool dependency_pinned =
      route_to_commit &&
      (previous_grouped != nullptr
           ? (grouped_dependency_pins != nullptr &&
              grouped_dependency_pins->Take(RetiredRecordOf(*previous)))
           : PinTxDependencyLocal(store, *previous));
  const bool defer_defrag_retirement =
      for_defrag && commit_retirements != nullptr;
  if (grouped_retirements != nullptr) {
    for (auto& child : *grouped_retirements) {
      child.dependency_pinned_ = grouped_dependency_pins != nullptr &&
                                 grouped_dependency_pins->Take(child);
      if (tx != nullptr) {
        tx->retirements_.push_back(TxShardWrites::Retired{
            .block_id_ = child.block_id_,
            .allocation_epoch_ = child.allocation_epoch_,
            .total_disk_bytes_ = child.total_disk_bytes_,
            .block_owner_ = child.block_owner_,
            .record_offset_ = child.record_offset_,
            .tx_tagged_ = child.tx_tagged_,
            .dependency_pinned_ = child.dependency_pinned_,
            .dependent_extents_ = child.dependent_extents_,
            .immediate_extents_ = child.immediate_extents_,
            .retained_owner_ = child.retained_owner_,
        });
      } else {
        commit_retirements->push_back(child);
      }
    }
  }
  store.staged_records_[updated.block_id_].push_back(RecordIdentity{
      .entry_address_ = reinterpret_cast<std::uintptr_t>(inserted_entry),
      .retired_extents_ = (!for_defrag || defer_defrag_retirement) &&
                                  !route_to_commit && previous.has_value() &&
                                  previous->external() &&
                                  !previous->key_external()
                              ? retired_value_extents
                              : nullptr,
      .retired_record_ =
          (!for_defrag || defer_defrag_retirement) && !route_to_commit &&
                  previous.has_value()
              ? StagedRetiredRecordOf(*previous, previous_dependent_extents)
              : StagedRetiredRecord{},
      .tx_retirements_ = std::move(commit_retirements),
      .index_generation_ = store.index_generations_[db_id],
      .entry_hash_ =
          inserted_entry == nullptr ? 0 : RecordIndex::AddressHash(digest),
      .partition_id_ =
          partition_ptr == nullptr ? std::uint16_t{0} : partition_ptr->id_,
      .db_id_ = db_id,
  });
  if (tx != nullptr && tx->collect_undo_ && inserted_entry != nullptr) {
    TxUndoLog& undo = store.tx_undo_[txid];
    const std::optional<std::uint32_t> entry_handle =
        undo.Track(inserted_entry);
    if (!entry_handle.has_value()) {
      co_return absl::ResourceExhaustedError(
          "transaction undo handle capacity exhausted");
    }
    undo.entries_.push_back(TxUndoEntry{
        .entry_handle_ = *entry_handle,
        .previous_ = previous,
        .previous_extents_ = previous_extents,
        .previous_grouped_ = previous_grouped,
        .previous_dependency_pinned_ = dependency_pinned,
        .previous_grouped_retirements_ = grouped_retirements,
        .applied_grouped_retirements_ = grouped_abort_retirements,
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
  if (!auxiliary && was_live != is_live) {
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
  if (!auxiliary && was_expiring != is_expiring) {
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
      LatchRuntimeFailure(store);
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
  grouped_root_guard.completed_ = true;
  co_return absl::OkStatus();
}

void StorageEngine::Impl::SealActiveBlocks(WorkerStore& store) {
  auto seal = [&](std::optional<ActiveBlock>& active) {
    if (!active.has_value()) return;
    // An allocation can install an empty stream and then lose revalidation
    // before its first append. It still has to be detached at a freeze
    // boundary: leaving it active lets a stale replication/apply coroutine
    // reuse the allocation after shutdown has closed AcquireWriteBlock.
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

}  // namespace lavik::storage
