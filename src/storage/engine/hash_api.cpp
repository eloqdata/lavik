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

bool IsHashLikeWrite(const HashOperation& operation) {
  return operation.kind_ == HashOperationKind::kSet ||
         operation.kind_ == HashOperationKind::kReplaceOnly ||
         operation.kind_ == HashOperationKind::kSetIfAbsent ||
         operation.kind_ == HashOperationKind::kDelete ||
         operation.kind_ == HashOperationKind::kPopRandom ||
         operation.kind_ == HashOperationKind::kIncrementInteger ||
         operation.kind_ == HashOperationKind::kIncrementFloat;
}

}  // namespace

Task<absl::StatusOr<HashResult>> StorageEngine::Impl::ExecuteHash(
    std::uint8_t db_id, std::string_view key, const HashOperation& operation,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  assert(db_id < kLogicalDatabaseCount);
  const Digest digest = ComputeDigest(key);
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest),
      IsHashLikeWrite(operation) ? tx::LockMode::kExclusive
                                 : tx::LockMode::kShared,
      lavik::storage::RedisSlot(key));
  co_return co_await ExecuteHashLocked(db_id, key, digest, operation, nullptr,
                                       replication, mutation_precondition);
}

Task<absl::StatusOr<HashResult>> StorageEngine::Impl::ExecuteSet(
    std::uint8_t db_id, std::string_view key, const HashOperation& operation,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  assert(db_id < kLogicalDatabaseCount);
  const Digest digest = ComputeDigest(key);
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest),
      IsHashLikeWrite(operation) ? tx::LockMode::kExclusive
                                 : tx::LockMode::kShared,
      lavik::storage::RedisSlot(key));
  co_return co_await ExecuteSetLocked(db_id, key, digest, operation, nullptr,
                                      replication, mutation_precondition);
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
