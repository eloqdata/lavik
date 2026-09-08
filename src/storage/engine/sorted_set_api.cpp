#include "impl.h"

namespace keylane::storage {

Task<absl::StatusOr<SortedSetResult>> StorageEngine::ExecuteSortedSet(
    std::uint8_t db_id, std::string_view key,
    const SortedSetOperation& operation,
    ReplicationCommandAppend* replication) {
  return impl_->ExecuteSortedSet(db_id, key, operation, replication);
}

Task<absl::StatusOr<SortedSetResult>> StorageEngine::ExecuteSortedSetLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const SortedSetOperation& operation, TxShardWrites* tx,
    ReplicationCommandAppend* replication) {
  return impl_->ExecuteSortedSetLocked(db_id, key, digest, operation, tx,
                                       replication);
}

Task<absl::StatusOr<SortedSetResult>> StorageEngine::Impl::ExecuteSortedSet(
    std::uint8_t db_id, std::string_view key,
    const SortedSetOperation& operation,
    ReplicationCommandAppend* replication) {
  assert(db_id < kLogicalDatabaseCount);
  const Digest digest = ComputeDigest(key);
  const bool read_only = operation.kind_ != SortedSetOperationKind::kAdd &&
                         operation.kind_ != SortedSetOperationKind::kRemove &&
                         operation.kind_ != SortedSetOperationKind::kPop;
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest),
      read_only ? tx::LockMode::kShared : tx::LockMode::kExclusive);
  co_return co_await ExecuteSortedSetLocked(db_id, key, digest, operation,
                                            nullptr, replication);
}

}  // namespace keylane::storage
