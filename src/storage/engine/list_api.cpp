#include "impl.h"

namespace keylane::storage {

Task<absl::StatusOr<std::uint64_t>> StorageEngine::Impl::ListPush(
    std::uint8_t db_id, std::string_view key,
    std::span<const std::string_view> values) {
  assert(db_id < kLogicalDatabaseCount);
  const Digest digest = ComputeDigest(key);
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest), tx::LockMode::kExclusive);
  co_return co_await ListPushLocked(db_id, key, digest, values);
}

Task<absl::StatusOr<std::uint64_t>> StorageEngine::Impl::ListPushLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    std::span<const std::string_view> values, TxShardWrites* tx) {
  if (values.empty()) {
    co_return absl::InvalidArgumentError(
        "LPUSH requires at least one element");
  }
  ListOperation operation;
  operation.kind_ = ListOperationKind::kPushLeft;
  operation.values_.assign(values.begin(), values.end());
  auto result = co_await ExecuteListLocked(db_id, key, digest, operation, tx);
  if (!result.ok()) co_return result.status();
  co_return result->length_;
}

Task<absl::StatusOr<ListResult>> StorageEngine::Impl::ExecuteList(
    std::uint8_t db_id, std::string_view key,
    const ListOperation& operation) {
  assert(db_id < kLogicalDatabaseCount);
  const Digest digest = ComputeDigest(key);
  const bool read_only = operation.kind_ == ListOperationKind::kLength ||
                         operation.kind_ == ListOperationKind::kIndex ||
                         operation.kind_ == ListOperationKind::kRange ||
                         operation.kind_ == ListOperationKind::kPosition;
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest),
      read_only ? tx::LockMode::kShared : tx::LockMode::kExclusive);
  co_return co_await ExecuteListLocked(db_id, key, digest, operation);
}

}  // namespace keylane::storage
