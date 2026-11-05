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

Task<absl::StatusOr<SortedSetResult>> StorageEngine::ExecuteSortedSet(
    std::uint8_t db_id, std::string_view key,
    const SortedSetOperation& operation, ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  return impl_->ExecuteSortedSet(db_id, key, operation, replication,
                                 mutation_precondition);
}

Task<absl::StatusOr<SortedSetResult>> StorageEngine::ExecuteSortedSetLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const SortedSetOperation& operation, TxShardWrites* tx,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  return impl_->ExecuteSortedSetLocked(db_id, key, digest, operation, tx,
                                       replication, mutation_precondition);
}

/// Executes a standalone operation on its owner worker and owns key admission.
/// Eligible grouped operations try IS/IX using a revalidated state hint. A
/// declined plan has released its groups before this caller drops the intent
/// and acquires whole-key S/X once; operation errors propagate without retry.
/// All borrowed inputs must outlive the coroutine, including its lock/IO waits.
Task<absl::StatusOr<SortedSetResult>> StorageEngine::Impl::ExecuteSortedSet(
    std::uint8_t db_id, std::string_view key,
    const SortedSetOperation& operation, ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  assert(db_id < kLogicalDatabaseCount);
  const Digest digest = ComputeDigest(key);
  const bool read_only =
      operation.kind_ != SortedSetOperationKind::kAdd &&
      operation.kind_ != SortedSetOperationKind::kRemove &&
      operation.kind_ != SortedSetOperationKind::kPop &&
      operation.kind_ != SortedSetOperationKind::kRemoveRange;
  // This lookup never suspends and touches only this worker's index. It is a
  // lock-mode hint, not a retained pointer or a validated read. Key admission
  // closes/drains remote direct readers before IX is granted; the callee
  // checks the current representation again after every acquisition wait.
  const bool point = operation.prepare_unlocked_ &&
                     ((operation.kind_ == SortedSetOperationKind::kAdd &&
                       operation.entries_.size() == 1) ||
                      (operation.kind_ == SortedSetOperationKind::kRemove &&
                       operation.members_.size() == 1));
  const bool partial_read =
      operation.kind_ == SortedSetOperationKind::kScores ||
      operation.kind_ == SortedSetOperationKind::kLength ||
      operation.kind_ == SortedSetOperationKind::kRange ||
      operation.kind_ == SortedSetOperationKind::kCount ||
      operation.kind_ == SortedSetOperationKind::kRank ||
      operation.kind_ == SortedSetOperationKind::kRandom ||
      operation.kind_ == SortedSetOperationKind::kScan;
  bool concurrent = false;
  if (point || partial_read) {
    auto& store = CurrentStore();
    auto& partition = PartitionForKey(store, key);
    const auto* found = partition.indexes_[db_id].Find(digest, key);
    concurrent = CanPrepareGroupedWriteUnlocked(partition) && found &&
                 found->key_complete() && found->value_.grouped() &&
                 found->value_.value_type() == ValueType::kSortedSet;
  }
  TraceSpanScope key_wait(operation.diagnostics_, TraceStage::kKeyLock);
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest),
      concurrent && read_only ? tx::LockMode::kIntentShared
      : read_only             ? tx::LockMode::kShared
      : concurrent            ? tx::LockMode::kIntentExclusive
                              : tx::LockMode::kExclusive,
      lavik::storage::RedisSlot(key));
  key_wait.End();
  if (concurrent) {
    auto result = co_await [&]() {
      if (read_only)
        return TryConcurrentSortedSetRead(db_id, key, digest, operation);
      return TryConcurrentSortedSetPoint(db_id, key, digest, operation,
                                         replication, mutation_precondition);
    }();
    if (!result.ok()) co_return result.status();
    if (*result) co_return std::move(**result);
    // The nested task has destroyed every group hold. Never strengthen an
    // intent in place: another intent holder can be waiting for those groups.
    key_lock.Reset();
    TraceSpanScope fallback_wait(operation.diagnostics_, TraceStage::kKeyLock);
    key_lock = co_await tx::CurrentTxShard().AcquireKey(
        db_id, tx::FingerprintOf(digest),
        read_only ? tx::LockMode::kShared : tx::LockMode::kExclusive,
        lavik::storage::RedisSlot(key));
  }
  co_return co_await ExecuteSortedSetLocked(db_id, key, digest, operation,
                                            nullptr, replication,
                                            mutation_precondition);
}

}  // namespace lavik::storage
