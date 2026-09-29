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
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest),
      read_only ? tx::LockMode::kShared : tx::LockMode::kExclusive);
  co_return co_await ExecuteSortedSetLocked(db_id, key, digest, operation,
                                            nullptr, replication,
                                            mutation_precondition);
}

}  // namespace lavik::storage
