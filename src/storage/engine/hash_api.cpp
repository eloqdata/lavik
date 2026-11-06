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
#include "lavik/error_detail.h"

namespace lavik::storage {

namespace {

bool IsHashLikeWrite(const HashOperation& operation) {
  return operation.kind_ == HashOperationKind::kSet ||
         operation.kind_ == HashOperationKind::kReplaceOnly ||
         operation.kind_ == HashOperationKind::kSetIfAbsent ||
         operation.kind_ == HashOperationKind::kDelete ||
         operation.kind_ == HashOperationKind::kPopRandom ||
         operation.kind_ == HashOperationKind::kIncrementInteger ||
         operation.kind_ == HashOperationKind::kIncrementFloat;
}

bool HasGroupLockPlan(HashOperationKind kind) {
  switch (kind) {
    case HashOperationKind::kSet:
    case HashOperationKind::kSetIfAbsent:
    case HashOperationKind::kDelete:
    case HashOperationKind::kIncrementInteger:
    case HashOperationKind::kIncrementFloat:
    case HashOperationKind::kGet:
    case HashOperationKind::kGetMany:
    case HashOperationKind::kExists:
    case HashOperationKind::kStringLength:
    case HashOperationKind::kLength:
    case HashOperationKind::kScan:
    case HashOperationKind::kRandomFields:
    case HashOperationKind::kPopRandom:
      return true;
    default:
      return false;
  }
}

}  // namespace

Task<absl::StatusOr<HashResult>> StorageEngine::Impl::ExecuteHash(
    std::uint8_t db_id, std::string_view key, const HashOperation& operation,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  return ExecuteHashLike(db_id, key, operation, ValueType::kHash, replication,
                         mutation_precondition);
}

Task<absl::StatusOr<HashResult>> StorageEngine::Impl::ExecuteHashLike(
    std::uint8_t db_id, std::string_view key, const HashOperation& operation,
    ValueType value_type, ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  assert(db_id < kLogicalDatabaseCount);
  const Digest digest = ComputeDigest(key);
  const bool write = IsHashLikeWrite(operation);
  bool concurrent = false;
  if (HasGroupLockPlan(operation.kind_)) {
    // Only the owner reads this hint, without suspension or retained pointers.
    // Admission drains direct readers; the callee rechecks after the key wait.
    auto& partition = PartitionForKey(CurrentStore(), key);
    const auto* found = partition.indexes_[db_id].Find(digest, key);
    concurrent = CanPrepareGroupedWriteUnlocked(partition) && found &&
                 found->key_complete() && found->value_.grouped() &&
                 found->value_.value_type() == value_type;
  }
  TraceSpanScope key_wait(operation.diagnostics_, TraceStage::kKeyLock);
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest),
      concurrent ? (write ? tx::LockMode::kIntentExclusive
                          : tx::LockMode::kIntentShared)
                 : (write ? tx::LockMode::kExclusive : tx::LockMode::kShared),
      lavik::storage::RedisSlot(key));
  key_wait.End();
  if (concurrent) {
    // The speculative writer can canonicalize increments/pops and append TTL
    // effects before its final batch admission requests key X. Preserve the
    // original command: after IX is released, X may observe another incarnation
    // with different contents and expiry. Only replicated intent writers need
    // this copy; ordinary compact operations retain their existing fast path.
    std::optional<ReplicationCommandAppend> retry_replication;
    if (write && replication) {
      retry_replication.emplace(*replication);
      // The snapshot owns distinct argument buffers, not the original charge.
      retry_replication->retained_owner_.reset();
      AccountReplicationCommand(&*retry_replication);
    }
    auto result = co_await TryConcurrentHashLike(db_id, key, digest, operation,
                                                 value_type, replication,
                                                 mutation_precondition);
    if (!result.ok()) co_return result.status();
    if (*result) co_return std::move(**result);
    if (retry_replication) {
      // Discard every attempt-derived effect and publication coordinate. The
      // retry accounts its newly canonicalized buffers at append admission.
      retry_replication->retained_owner_.reset();
      *replication = std::move(*retry_replication);
    }
    // The nested task has released every group. Upgrading an intent while
    // retaining groups could deadlock another intent holder waiting for them.
    key_lock.Reset();
    TraceSpanScope fallback(operation.diagnostics_, TraceStage::kKeyLock);
    key_lock = co_await tx::CurrentTxShard().AcquireKey(
        db_id, tx::FingerprintOf(digest),
        write ? tx::LockMode::kExclusive : tx::LockMode::kShared,
        lavik::storage::RedisSlot(key));
  }
  co_return co_await ExecuteHashLikeLocked(db_id, key, digest, operation,
                                           value_type, nullptr, replication,
                                           mutation_precondition);
}

Task<absl::StatusOr<std::optional<HashResult>>>
StorageEngine::Impl::TryConcurrentHashLike(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const HashOperation& operation, ValueType value_type,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  auto& store = CurrentStore();
  auto& partition = PartitionForKey(store, key);
  co_await store.store_state_mutex_.Lock();
  UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
  const auto* found = partition.indexes_[db_id].Find(digest, key);
  const auto now = operation.now_ms_ ? operation.now_ms_ : UnixTimeMillis();
  if (!CanPrepareGroupedWriteUnlocked(partition) || !found ||
      !found->key_complete() || !found->value_.grouped() ||
      found->value_.value_type() != value_type || IsExpired(*found, now))
    co_return std::nullopt;
  auto view = partition.grouped_objects_[db_id].Lookup(
      key, GroupedObjectVersion{
               .root_ = MaterializeIndexLocation(*found),
               .db_epoch_ = EffectiveRecordDbEpoch(partition, db_id),
               .replication_epoch_ = partition.replication_epoch_,
               .index_generation_ = partition.grouped_generations_[db_id]});
  if (!view.ok()) co_return view.status();
  auto object = std::move(*view);
  if (!object || object->is_ordered()) co_return std::nullopt;
  if (operation.kind_ == HashOperationKind::kLength) {
    // No payload can affect this projection after the atomic root snapshot.
    const auto readable = object->ReadStatus();
    if (!readable.ok()) co_return readable;
    HashResult result;
    result.key_exists_ = true;
    result.length_ = object->directory().root().field_count_;
    co_return std::optional(std::move(result));
  }
  if (operation.kind_ == HashOperationKind::kRandomFields ||
      operation.kind_ == HashOperationKind::kPopRandom) {
    // Sampling determines its leaves from cardinalities, so its adapter owns
    // selection, group acquisition and post-wait rank validation together.
    unlock.Unlock();
    bool retry = false;
    auto result = co_await ExecuteHashLikeLocked(
        db_id, key, digest, operation, value_type, nullptr, replication,
        mutation_precondition, &retry);
    if (retry) co_return std::nullopt;
    if (!result.ok()) co_return result.status();
    co_return std::optional(std::move(*result));
  }
  const bool scan = operation.kind_ == HashOperationKind::kScan;
  const auto count = scan ? std::size_t{1} : operation.fields_.size();
  if (count > SIZE_MAX / sizeof(GroupedRecordId))
    co_return MemoryLimitError("OOM Hash group lock plan overflow");
  auto admission = TryReserveMemory(count * sizeof(GroupedRecordId));
  if (!admission) co_return MemoryLimitError("OOM Hash group lock plan");
  std::vector<GroupedRecordId> groups;
  groups.reserve(count);
  if (groups.capacity() > admission->bytes() / sizeof(GroupedRecordId))
    co_return MemoryLimitError("OOM Hash group lock plan");
  if (scan) {
    const auto* route = object->directory().groups().Floor(operation.cursor_);
    if (!route) co_return absl::DataLossError("Hash scan has no route");
    groups.push_back(route->id_);
  } else {
    for (const auto field : operation.fields_) {
      const auto* route = object->directory().Find(field);
      if (!route) co_return absl::DataLossError("Hash field has no route");
      groups.push_back(route->id_);
    }
  }
  std::sort(groups.begin(), groups.end());
  groups.erase(std::unique(groups.begin(), groups.end()), groups.end());
  if (!store.group_locks_)
    store.group_locks_ = std::make_unique<GroupedLockTable>(*store.worker_);
  auto holds = store.group_locks_->MakeLockSet(
      {db_id, tx::FingerprintOf(digest), object->incarnation()});
  unlock.Unlock();
  const bool write = IsHashLikeWrite(operation);
  for (const auto id : groups) {
    co_await holds.Acquire(
        {GroupedLockIndex::kMember, id},
        write ? GroupedLockMode::kExclusive : GroupedLockMode::kShared);
  }
  // All leaves must be held before the adapter captures its common read view.
  // IS/IX excludes topology edits, but population replacement still needs the
  // same epoch/incarnation checks as other grouped readers.
  const auto current =
      partition.grouped_objects_[db_id].CurrentForMutation(key);
  if (!current || current->is_ordered() ||
      current->incarnation() != object->incarnation() ||
      current->version().db_epoch_ != object->version().db_epoch_ ||
      current->version().replication_epoch_ !=
          object->version().replication_epoch_ ||
      current->version().index_generation_ !=
          object->version().index_generation_)
    co_return std::nullopt;
  const auto held = [&](const auto* route) {
    return route &&
           std::binary_search(groups.begin(), groups.end(), route->id_);
  };
  if (scan) {
    if (!held(current->directory().groups().Floor(operation.cursor_)))
      co_return std::nullopt;
  } else {
    for (const auto field : operation.fields_)
      if (!held(current->directory().Find(field))) co_return std::nullopt;
  }
  bool retry = false;
  auto result = co_await ExecuteHashLikeLocked(db_id, key, digest, operation,
                                               value_type, nullptr, replication,
                                               mutation_precondition, &retry);
  if (retry) co_return std::nullopt;
  if (!result.ok()) co_return result.status();
  co_return std::optional(std::move(*result));
}

Task<absl::StatusOr<HashResult>> StorageEngine::Impl::ExecuteSet(
    std::uint8_t db_id, std::string_view key, const HashOperation& operation,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  return ExecuteHashLike(db_id, key, operation, ValueType::kSet, replication,
                         mutation_precondition);
}

Task<absl::StatusOr<HashResult>> StorageEngine::Impl::ExecuteSetLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const HashOperation& operation, TxShardWrites* tx,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  return ExecuteHashLikeLocked(db_id, key, digest, operation, ValueType::kSet,
                               tx, replication, mutation_precondition);
}

}  // namespace lavik::storage
