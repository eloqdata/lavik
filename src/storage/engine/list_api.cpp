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
#include "lavik/status_macros.h"

namespace lavik::storage {

Task<absl::StatusOr<std::uint64_t>> StorageEngine::Impl::ListPush(
    std::uint8_t db_id, std::string_view key,
    std::span<const std::string_view> values,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  assert(db_id < kLogicalDatabaseCount);
  const Digest digest = ComputeDigest(key);
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest), tx::LockMode::kExclusive);
  co_return co_await ListPushLocked(db_id, key, digest, values, nullptr,
                                    replication, mutation_precondition);
}

Task<absl::StatusOr<std::uint64_t>> StorageEngine::Impl::ListPushLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    std::span<const std::string_view> values, TxShardWrites* tx,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  if (values.empty()) {
    co_return absl::InvalidArgumentError("LPUSH requires at least one element");
  }
  ListOperation operation;
  operation.kind_ = ListOperationKind::kPushLeft;
  operation.values_.assign(values.begin(), values.end());
  auto result = co_await ExecuteListLocked(db_id, key, digest, operation, tx,
                                           replication, mutation_precondition);
  LAVIK_CO_RETURN_IF_ERROR(result.status());
  co_return result->length_;
}

Task<absl::StatusOr<ListResult>> StorageEngine::Impl::ExecuteList(
    std::uint8_t db_id, std::string_view key, const ListOperation& operation,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  assert(db_id < kLogicalDatabaseCount);
  const Digest digest = ComputeDigest(key);
  const bool read_only = operation.kind_ == ListOperationKind::kLength ||
                         operation.kind_ == ListOperationKind::kIndex ||
                         operation.kind_ == ListOperationKind::kRange ||
                         operation.kind_ == ListOperationKind::kPosition;
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest),
      read_only ? tx::LockMode::kShared : tx::LockMode::kExclusive);
  co_return co_await ExecuteListLocked(db_id, key, digest, operation, nullptr,
                                       replication, mutation_precondition);
}

}  // namespace lavik::storage
