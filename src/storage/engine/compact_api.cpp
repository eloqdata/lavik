#include "impl.h"

namespace keylane::storage {

Task<absl::Status> StorageEngine::Impl::ExecuteCompact(
    std::uint8_t db_id, std::string_view key, ValueType value_type,
    bool read_only, const CompactValueCallback& callback,
    std::uint64_t now_ms) {
  assert(db_id < kLogicalDatabaseCount);
  const Digest digest = ComputeDigest(key);
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest),
      read_only ? tx::LockMode::kShared : tx::LockMode::kExclusive);
  co_return co_await ExecuteCompactLocked(db_id, key, digest, value_type,
                                          read_only, callback, nullptr, now_ms);
}

Task<absl::Status> StorageEngine::Impl::ExecuteCompactLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    ValueType value_type, bool read_only, const CompactValueCallback& callback,
    TxShardWrites* tx, std::uint64_t now_ms) {
  assert(db_id < kLogicalDatabaseCount);
  if (value_type != ValueType::kString && value_type != ValueType::kSortedSet &&
      value_type != ValueType::kStream) {
    co_return absl::InvalidArgumentError("unsupported compact value type");
  }

  WorkerStore& store = CurrentStore();
  auto& partition = PartitionForKey(store, key);
  co_await store.store_state_mutex_.Lock();
  UnlockGuard unlock(&store.store_state_mutex_, store.worker_);

  auto& index = partition.indexes_[db_id];
  auto* found = index.Find(digest, key);
  if (found != nullptr && !found->key_complete()) [[unlikely]] {
    auto resolved = co_await FindVerifiedEntry(store, index, digest, key);
    if (!resolved.ok()) co_return resolved.status();
    found = *resolved;
  }

  const bool stored_value =
      found != nullptr && found->value_.kind_ == RecordKind::kValue;
  if (now_ms == 0) now_ms = UnixTimeMillis();
  const bool exists = stored_value && !IsExpired(found->value_, now_ms);
  if (exists && found->value_.value_type_ != value_type) {
    co_return absl::InvalidArgumentError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  }
  const std::uint64_t observed_index_generation =
      store.index_generations_[db_id];
  const std::uint64_t observed_db_epoch = DbEpoch(db_id);
  const std::uint64_t observed_replication_epoch = partition.replication_epoch_;
  auto read_epoch_changed = [&]() {
    return read_only &&
           (store.index_generations_[db_id] != observed_index_generation ||
            DbEpoch(db_id) != observed_db_epoch ||
            partition.replication_epoch_ != observed_replication_epoch);
  };
  const RecordLocation location = exists ? found->value_ : RecordLocation{};
  const ExtentManifest extents =
      exists ? ExtentsFor(store, found) : ExtentManifest{};
  std::optional<LoadedValue> loaded;
  std::optional<CompactValueView> view;
  if (read_only) unlock.Unlock();
  if (exists) {
    auto value = co_await LoadValue(store, partition, db_id, key, digest,
                                    location, extents);
    if (!value.ok()) {
      if (!read_epoch_changed()) co_return value.status();
    } else {
      loaded.emplace(std::move(*value));
      const auto bytes = loaded->value();
      view = CompactValueView{
          .encoded_ = std::string_view(
              reinterpret_cast<const char*>(bytes.data()), bytes.size()),
          .logical_size_ = location.logical_size_,
          .expire_at_ms_ = location.expire_at_ms_,
      };
    }
  }

  auto update = callback(view);
  if (!update.ok()) co_return update.status();
  if (read_only) {
    if (update->changed_) {
      co_return absl::InternalError("read-only compact operation mutated");
    }
    co_return absl::OkStatus();
  }
  if (!update->changed_) co_return absl::OkStatus();
  if (update->erase_ && !exists) co_return absl::OkStatus();
  if (update->reuse_encoded_ &&
      (!exists || update->erase_ || !update->encoded_.empty())) {
    co_return absl::InternalError("invalid compact payload reuse");
  }
  if (!update->erase_ && update->logical_size_ == 0 &&
      value_type == ValueType::kSortedSet) {
    co_return absl::InvalidArgumentError(
        "nonempty compact payload has zero cardinality");
  }

  const RecordKind kind =
      update->erase_ ? RecordKind::kTombstone : RecordKind::kValue;
  const ValueType published_type =
      update->erase_ ? ValueType::kNone : value_type;
  const std::uint64_t expire_at_ms =
      update->erase_
          ? 0
          : update->expire_at_ms_.value_or(exists ? location.expire_at_ms_ : 0);
  const std::string_view encoded =
      update->reuse_encoded_ ? view->encoded_ : std::string_view(update->encoded_);
  const std::uint64_t logical_size =
      update->reuse_encoded_ ? location.logical_size_ : update->logical_size_;
  absl::Status status = co_await AppendLocked(
      store, partition, db_id, key,
      update->erase_ ? std::string_view{} : encoded,
      kind, published_type, expire_at_ms, tx,
      update->erase_ ? 0 : logical_size);
  co_return status;
}

}  // namespace keylane::storage
