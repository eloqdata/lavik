#include "impl.h"

namespace keylane::storage {

namespace {

bool IsHashLikeWrite(const HashOperation& operation) {
  return operation.kind_ == HashOperationKind::kSet ||
         operation.kind_ == HashOperationKind::kSetIfAbsent ||
         operation.kind_ == HashOperationKind::kDelete ||
         operation.kind_ == HashOperationKind::kPopRandom ||
         operation.kind_ == HashOperationKind::kIncrementInteger ||
         operation.kind_ == HashOperationKind::kIncrementFloat;
}

}  // namespace

Task<absl::StatusOr<HashResult>> StorageEngine::Impl::ExecuteHash(
    std::uint8_t db_id, std::string_view key, const HashOperation& operation) {
  assert(db_id < kLogicalDatabaseCount);
  const Digest digest = ComputeDigest(key);
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest),
      IsHashLikeWrite(operation) ? tx::LockMode::kExclusive
                                 : tx::LockMode::kShared);
  co_return co_await ExecuteHashLocked(db_id, key, digest, operation);
}

Task<absl::StatusOr<HashResult>> StorageEngine::Impl::ExecuteSet(
    std::uint8_t db_id, std::string_view key, const HashOperation& operation) {
  assert(db_id < kLogicalDatabaseCount);
  const Digest digest = ComputeDigest(key);
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest),
      IsHashLikeWrite(operation) ? tx::LockMode::kExclusive
                                 : tx::LockMode::kShared);
  co_return co_await ExecuteSetLocked(db_id, key, digest, operation);
}

Task<absl::StatusOr<HashResult>> StorageEngine::Impl::ExecuteSetLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const HashOperation& operation, TxShardWrites* tx) {
  co_return co_await ExecuteHashLikeLocked(db_id, key, digest, operation,
                                           ValueType::kSet, tx);
}

}  // namespace keylane::storage
