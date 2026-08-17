#include "impl.h"

namespace keylane::storage {

Task<absl::StatusOr<ScanBatch>> StorageEngine::Impl::ScanPartition(
    std::uint16_t partition_id, std::uint8_t db_id, std::uint64_t cursor,
    std::size_t count, std::uint64_t now_ms, std::size_t max_bytes) {
  assert(db_id < kLogicalDatabaseCount);
  assert(count > 0);
  const auto& index =
      PartitionFor(CurrentStore(), partition_id).indexes_[db_id];
  ScanBatch result;
  result.cursor_ = cursor;
  if (now_ms == 0) {
    now_ms = UnixTimeMillis();
  }
  const std::size_t max_iterations =
      count > std::numeric_limits<std::size_t>::max() / 10
          ? std::numeric_limits<std::size_t>::max()
          : count * 10;
  std::size_t iterations = 0;
  std::size_t bytes = 0;
  do {
    struct ExternalCandidate {
      const RecordIndex::Entry* entry_ = nullptr;
      ExtentManifest extents_;
      RecordLocation location_{};
      std::uint64_t hash_ = 0;
      std::uint32_t key_bytes_ = 0;
    };
    std::vector<ExternalCandidate> external;
    result.cursor_ =
        index.Scan(result.cursor_, [&](const RecordIndex::Entry& entry) {
          if (entry.value_.kind_ == RecordKind::kValue &&
              !IsExpired(entry.value_, now_ms)) {
            if (entry.key_complete()) [[likely]] {
              bytes += entry.key().size();
              result.keys_.emplace_back(entry.key());
              result.value_types_.push_back(entry.value_.value_type_);
            } else [[unlikely]] {
              external.push_back(ExternalCandidate{
                  .entry_ = &entry,
                  .extents_ = ExtentsFor(CurrentStore(), &entry),
                  .location_ = entry.value_,
                  .hash_ = entry.hash_,
                  .key_bytes_ = entry.logical_key_size(),
              });
            }
          }
        });
    for (const ExternalCandidate& candidate : external) {
      auto key =
          co_await LoadOutOfIndexKey(CurrentStore(), candidate.location_,
                                     candidate.extents_, candidate.key_bytes_);
      if (!key.ok()) {
        co_return key.status();
      }
      if (!index.Contains(candidate.entry_, candidate.hash_)) {
        continue;
      }
      const RecordIndex::Entry* current = candidate.entry_;
      if (current->value_.SamePhysicalRecord(candidate.location_) &&
          current->value_.kind_ == RecordKind::kValue &&
          !IsExpired(current->value_, now_ms)) {
        bytes += key->size();
        result.keys_.push_back(std::move(*key));
        result.value_types_.push_back(current->value_.value_type_);
      }
    }
    ++iterations;
  } while (result.cursor_ != 0 && result.keys_.size() < count &&
           bytes < max_bytes && iterations < max_iterations);
  co_return result;
}

PartitionReplicationStart StorageEngine::Impl::BeginPartitionReplication(
    std::uint16_t partition_id) {
  auto& partition = PartitionFor(CurrentStore(), partition_id);
  partition.capture_deltas_ = true;
  partition.deltas_.clear();
  partition.delta_floor_ = partition.mutation_sequence_;
  partition.delta_queued_ = false;
  PartitionReplicationStart result;
  result.snapshot_sequence_ = partition.mutation_sequence_;
  for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
    result.db_epochs_[db_id] = DbEpoch(db_id);
    if (partition.live_key_count_[db_id] != 0) {
      result.nonempty_db_mask_ |= static_cast<std::uint16_t>(1U << db_id);
    }
  }
  return result;
}

Task<absl::StatusOr<PartitionSnapshotBatch>>
StorageEngine::Impl::SnapshotPartition(std::uint16_t partition_id,
                                       std::uint8_t db_id, std::uint64_t cursor,
                                       std::size_t count) {
  if (db_id >= kLogicalDatabaseCount || count == 0) {
    co_return absl::Status(absl::StatusCode::kInvalidArgument,
                           "invalid partition snapshot request");
  }
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionFor(store, partition_id);
  auto& index = partition.indexes_[db_id];
  const std::uint64_t now_ms = UnixTimeMillis();
  auto scanned = co_await ScanPartition(partition_id, db_id, cursor, count,
                                        now_ms, SIZE_MAX);
  if (!scanned.ok()) {
    co_return scanned.status();
  }
  std::vector<std::string> keys = std::move(scanned->keys_);
  const std::uint64_t next = scanned->cursor_;

  PartitionSnapshotBatch batch;
  batch.cursor_ = next;
  batch.records_.reserve(keys.size());
  for (const std::string& key : keys) {
    const Digest digest = ComputeDigest(key);
    auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
        db_id, tx::FingerprintOf(digest), tx::LockMode::kShared);
    auto resolved = co_await FindVerifiedEntry(store, index, digest, key);
    if (!resolved.ok()) {
      co_return resolved.status();
    }
    auto* current = *resolved;
    if (current == nullptr || current->value_.kind_ != RecordKind::kValue) {
      continue;
    }
    const RecordLocation location = current->value_;
    if (IsExpired(location, UnixTimeMillis())) {
      QueueExpiredCandidate(store, partition.id_, db_id, *current, key);
      continue;
    }
    auto loaded = co_await LoadValue(store, partition, db_id, key, digest,
                                     location, ExtentsFor(store, current));
    if (!loaded.ok()) {
      if (loaded.status().code() == absl::StatusCode::kNotFound) {
        continue;
      }
      co_return loaded.status();
    }
    const std::span<const std::byte> value = loaded->value();
    batch.records_.push_back(SnapshotRecord{
        .kind_ = SnapshotRecord::Kind::kValue,
        .db_id_ = db_id,
        .db_epoch_ = DbEpoch(db_id),
        .mutation_sequence_ = location.mutation_sequence_,
        .expire_at_ms_ = location.expire_at_ms_,
        .value_type_ = location.value_type_,
        .logical_size_ = location.logical_size_,
        .key_ = key,
        .value_ = std::string(reinterpret_cast<const char*>(value.data()),
                              value.size()),
    });
  }
  co_return batch;
}

PartitionDeltaBatch StorageEngine::Impl::ReadPartitionDeltas(
    std::uint16_t partition_id, std::uint64_t after_sequence,
    std::size_t count) {
  auto& partition = PartitionFor(CurrentStore(), partition_id);
  partition.delta_queued_ = false;
  PartitionDeltaBatch batch;
  batch.watermark_ = partition.mutation_sequence_;
  batch.overflow_ = after_sequence < partition.delta_floor_;
  if (batch.overflow_ || count == 0) {
    return batch;
  }
  batch.records_.reserve(std::min(count, partition.deltas_.size()));
  for (const SnapshotRecord& mutation : partition.deltas_) {
    if (mutation.mutation_sequence_ > after_sequence) {
      batch.records_.push_back(mutation);
      if (batch.records_.size() == count) {
        break;
      }
    }
  }
  return batch;
}

void StorageEngine::Impl::AcknowledgePartitionDeltas(
    std::uint16_t partition_id, std::uint64_t through_sequence) {
  auto& partition = PartitionFor(CurrentStore(), partition_id);
  while (!partition.deltas_.empty() &&
         partition.deltas_.front().mutation_sequence_ <= through_sequence) {
    partition.delta_floor_ = std::max(
        partition.delta_floor_, partition.deltas_.front().mutation_sequence_);
    partition.deltas_.pop_front();
  }
  if (!partition.deltas_.empty() && !partition.delta_queued_) {
    partition.delta_queued_ = true;
    (void)replication_ready_.enqueue(partition.id_);
  }
}

Task<absl::StatusOr<std::uint64_t>> StorageEngine::Impl::ResetReplicaPartition(
    std::uint16_t partition_id,
    std::span<const std::uint64_t, kLogicalDatabaseCount> source_db_epochs) {
  for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
    absl::Status advanced =
        co_await AdvanceDbEpoch(db_id, source_db_epochs[db_id]);
    if (!advanced.ok()) {
      co_return advanced;
    }
  }

  WorkerStore& store = CurrentStore();
  auto& partition = PartitionFor(store, partition_id);
  if (partition.replication_epoch_ ==
      std::numeric_limits<std::uint64_t>::max()) {
    co_return absl::Status(absl::StatusCode::kOutOfRange,
                           "partition replication epoch exhausted");
  }
  // Stop this worker's append stream before making the new epoch durable.
  // Otherwise a concurrent command could append an old-epoch record after
  // the metadata commit and receive OK even though restart must discard it.
  co_await store.store_state_mutex_.Lock();
  UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
  const std::uint64_t next_epoch = partition.replication_epoch_ + 1;
  absl::Status persisted = co_await PersistEpochValue(
      kLogicalDatabaseCount + partition_id, next_epoch);
  if (!persisted.ok()) {
    co_return persisted;
  }

  struct OldKey {
    std::uint8_t db_id_ = 0;
    std::string key_;
  };
  std::vector<OldKey> old_keys;
  for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
    std::vector<const RecordIndex::Entry*> external_entries;
    partition.indexes_[db_id].ForEach([&](const RecordIndex::Entry& entry) {
      if (entry.key_complete()) [[likely]] {
        old_keys.push_back(
            OldKey{.db_id_ = db_id, .key_ = std::string(entry.key())});
      } else [[unlikely]] {
        external_entries.push_back(&entry);
      }
    });
    for (const RecordIndex::Entry* entry : external_entries) {
      auto key = co_await LoadOutOfIndexKey(store, entry->value_,
                                            ExtentsFor(store, entry),
                                            entry->logical_key_size());
      if (!key.ok()) {
        co_return key.status();
      }
      old_keys.push_back(
          OldKey{.db_id_ = db_id, .key_ = std::move(*key)});
    }
  }
  partition.replication_epoch_ = next_epoch;
  partition.mutation_sequence_ = 0;
  partition.capture_deltas_ = false;
  partition.deltas_.clear();
  partition.replica_value_stage_.reset();
  partition.delta_floor_ = 0;
  partition.delta_queued_ = false;
  for (const OldKey& old : old_keys) {
    const std::uint8_t db_id = old.db_id_;
    const std::string& key = old.key_;
    const Digest digest = ComputeDigest(key);
    const bool key_external = key.size() > options_.inline_key_max_bytes_;
    const bool external =
        AlignRecord(RecordHeaderBytes(key.size(), key_external) +
                    (key_external ? key.size() : 0)) >
        kStorageBlockBytes - kBlockHeaderBytes;
    ExtentManifest extents;
    std::string manifest;
    if (external) [[unlikely]] {
      auto written = co_await WriteExtentValueLocked(store, key);
      if (!written.ok()) {
        co_return written.status();
      }
      extents = std::move(*written);
      manifest = EncodeManifest(*extents);
    }
    absl::Status tombstone = co_await WriteRecordLocked(
        store, db_id, key, manifest, RecordKind::kTombstone, ValueType::kNone,
        0, digest, 0, 0, false, false, external, key_external, 0, extents,
        nullptr, nullptr, nullptr);
    if (!tombstone.ok()) {
      if (extents != nullptr) [[unlikely]] {
        SpawnExtentReclaim(store, extents);
      }
      co_return tombstone;
    }
  }
  co_return next_epoch;
}

// TODO(replication): the epoch is validated only here at entry, but the loop
// below suspends repeatedly (key lock, store_state_mutex, extent IO, and
// AdvanceDbEpoch's reclaim wait in the kFlushDb branch), and a handler whose
// connection died is not cancelled. A reconnecting session's
// ResetReplicaPartition can run inside such a gap; the stale handler then
// resumes and keeps writing its old batch, stamped with the post-reset
// replication_epoch (WriteRecordLocked reads it at write time), overriding
// the reset tombstones — the replica keeps a key the primary deleted, and no
// future delta ever corrects it. Design pending: re-validate the epoch after
// every suspension point (including WriteRecordLocked's allocation wait,
// via the defrag-style expected-version handoff), or serialize per-partition
// application across sessions.
//
// TODO(replication): ResetReplicaPartition above holds store_state_mutex across
// its whole tombstone loop (unlock_writer_while_waiting=false), so on a full
// device its inline block allocation waits for reclaim progress while the
// flush that would free space is itself waiting for this store_state_mutex — a
// three-way stall that never resolves. When the epoch redesign lands, the
// loop should release the mutex around allocation waits and revalidate
// (db_epoch, replication_epoch, index_generation) afterwards, the same
// expected-version handoff defrag relocation uses.
//
// TODO(replication): this path applies records without capturing deltas, and
// CatchUp advances the acknowledged watermark on empty batches, so a node
// that is both a replica and a source (A -> B -> C) silently forwards nothing
// after the snapshot baseline: C reports in-sync while diverging forever.
// Until cascading is designed, the option parser should reject running with
// --replication-port and --replicate-to at the same time.
//
// TODO(replication): this path also bypasses the command layer's database
// gates (file-static in command.cpp), which KEYS and FLUSHDB close to get an
// exclusive, still keyspace. On a replica, apply traffic keeps mutating the
// index between KEYS's counting and emitting passes — the announced *N can
// disagree with the emitted element count, desynchronizing that client's
// RESP stream — and FLUSHDB's drain-then-detach exclusivity assumption does
// not hold either. The redesign should either route apply through the gates
// or pause application while a gated operation is in flight.
Task<absl::Status> StorageEngine::Impl::ApplyReplicaRecords(
    std::uint16_t partition_id, std::uint64_t replication_epoch,
    std::span<const SnapshotRecord> records) {
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionFor(store, partition_id);
  if (replication_epoch != partition.replication_epoch_) {
    co_return absl::Status(absl::StatusCode::kFailedPrecondition,
                           "stale partition replication epoch");
  }
  for (const SnapshotRecord& record : records) {
    std::optional<SnapshotRecord> materialized;
    const SnapshotRecord* effective = &record;
    if (record.db_id_ >= kLogicalDatabaseCount ||
        RedisSlot(record.key_) != partition_id) {
      if (record.kind_ != SnapshotRecord::Kind::kFlushDb) {
        co_return absl::Status(absl::StatusCode::kInvalidArgument,
                               "replica record belongs to another partition");
      }
    }
    if (record.kind_ == SnapshotRecord::Kind::kFlushDb) {
      absl::Status advanced =
          co_await AdvanceDbEpoch(record.db_id_, record.db_epoch_);
      if (!advanced.ok()) {
        co_return advanced;
      }
      partition.mutation_sequence_ =
          std::max(partition.mutation_sequence_, record.mutation_sequence_);
      continue;
    }
    if (record.db_epoch_ != DbEpoch(record.db_id_)) {
      if (record.db_epoch_ < DbEpoch(record.db_id_)) {
        continue;
      }
      co_return absl::Status(absl::StatusCode::kFailedPrecondition,
                             "replica record database epoch is not installed");
    }

    if (record.kind_ == SnapshotRecord::Kind::kValueBegin) {
      std::uint64_t value_logical_size = 0;
      if (record.value_.size() == sizeof(value_logical_size)) {
        for (std::size_t byte = 0; byte < sizeof(value_logical_size); ++byte) {
          value_logical_size |=
              static_cast<std::uint64_t>(
                  static_cast<unsigned char>(record.value_[byte]))
              << (byte * 8);
        }
      }
      const bool nonempty_collection =
          record.value_type_ == ValueType::kList ||
          record.value_type_ == ValueType::kHash ||
          record.value_type_ == ValueType::kSet ||
          record.value_type_ == ValueType::kSortedSet;
      if (partition.replica_value_stage_.has_value() ||
          (record.value_type_ != ValueType::kString &&
           record.value_type_ != ValueType::kList &&
           record.value_type_ != ValueType::kHash &&
           record.value_type_ != ValueType::kSet &&
           record.value_type_ != ValueType::kSortedSet &&
          record.value_type_ != ValueType::kStream) ||
          record.value_.size() != sizeof(value_logical_size) ||
          (nonempty_collection && value_logical_size == 0) ||
          (record.value_type_ == ValueType::kString &&
           value_logical_size != record.logical_size_) ||
          value_logical_size > std::numeric_limits<std::uint32_t>::max() ||
          record.logical_size_ == 0 ||
          record.logical_size_ > kMaxBitmapBytes ||
          record.chunk_count_ == 0 ||
          record.chunk_count_ !=
              (record.logical_size_ + kExtentPayloadBytes - 1) /
                  kExtentPayloadBytes) {
        co_return absl::Status(absl::StatusCode::kInvalidArgument,
                               "invalid replicated large value begin frame");
      }
      partition.replica_value_stage_ = ReplicaValueStage{
          .db_id_ = record.db_id_,
          .db_epoch_ = record.db_epoch_,
          .mutation_sequence_ = record.mutation_sequence_,
          .expire_at_ms_ = record.expire_at_ms_,
          .logical_size_ = value_logical_size,
          .encoded_size_ = record.logical_size_,
          .next_chunk_ = 0,
          .chunk_count_ = record.chunk_count_,
          .value_type_ = record.value_type_,
          .key_ = record.key_,
          .value_ = {},
      };
      partition.replica_value_stage_->value_.reserve(
          static_cast<std::size_t>(record.logical_size_));
      continue;
    }
    if (record.kind_ == SnapshotRecord::Kind::kValueChunk) {
      auto& stage = partition.replica_value_stage_;
      if (!stage.has_value() || stage->db_id_ != record.db_id_ ||
          stage->db_epoch_ != record.db_epoch_ ||
          stage->mutation_sequence_ != record.mutation_sequence_ ||
          stage->key_ != record.key_ ||
          stage->next_chunk_ != record.chunk_index_ ||
          stage->chunk_count_ != record.chunk_count_ || record.value_.empty() ||
          record.value_.size() > kExtentPayloadBytes ||
          record.value_.size() > stage->encoded_size_ ||
          stage->value_.size() > stage->encoded_size_ - record.value_.size()) {
        co_return absl::Status(absl::StatusCode::kInvalidArgument,
                               "invalid replicated large value chunk frame");
      }
      stage->value_.append(record.value_);
      ++stage->next_chunk_;
      continue;
    }
    if (record.kind_ == SnapshotRecord::Kind::kValueCommit) {
      auto& stage = partition.replica_value_stage_;
      if (!stage.has_value() || stage->db_id_ != record.db_id_ ||
          stage->db_epoch_ != record.db_epoch_ ||
          stage->mutation_sequence_ != record.mutation_sequence_ ||
          stage->key_ != record.key_ || !record.value_.empty() ||
          stage->next_chunk_ != stage->chunk_count_ ||
          record.chunk_index_ != stage->chunk_count_ ||
          stage->value_.size() != stage->encoded_size_) {
        co_return absl::Status(absl::StatusCode::kInvalidArgument,
                               "invalid replicated large value commit frame");
      }
      materialized.emplace(SnapshotRecord{
          .kind_ = SnapshotRecord::Kind::kValue,
          .db_id_ = stage->db_id_,
          .db_epoch_ = stage->db_epoch_,
          .mutation_sequence_ = stage->mutation_sequence_,
          .expire_at_ms_ = stage->expire_at_ms_,
          .value_type_ = stage->value_type_,
          .logical_size_ = stage->logical_size_,
          .key_ = std::move(stage->key_),
          .value_ = std::move(stage->value_),
      });
      stage.reset();
      effective = &*materialized;
    } else if (partition.replica_value_stage_.has_value()) {
      co_return absl::Status(
          absl::StatusCode::kInvalidArgument,
          "replicated large value frame sequence interrupted");
    }

    const SnapshotRecord& applied = *effective;
    const Digest digest = ComputeDigest(applied.key_);
    auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
        applied.db_id_, tx::FingerprintOf(digest), tx::LockMode::kExclusive);
    // Replicated modifications invalidate local watchers too.
    tx::CurrentTxShard().MarkWatched(applied.db_id_, tx::FingerprintOf(digest));
    co_await store.store_state_mutex_.Lock();
    UnlockGuard write_unlock(&store.store_state_mutex_, store.worker_);
    if (replication_epoch != partition.replication_epoch_) [[unlikely]] {
      co_return absl::Status(absl::StatusCode::kFailedPrecondition,
                             "stale partition replication epoch");
    }
    auto& index = partition.indexes_[applied.db_id_];
    auto resolved =
        co_await FindVerifiedEntry(store, index, digest, applied.key_);
    if (!resolved.ok()) {
      co_return resolved.status();
    }
    auto* current = *resolved;
    if (current != nullptr &&
        current->value_.mutation_sequence_ >= applied.mutation_sequence_) {
      continue;
    }
    const RecordKind kind = applied.kind_ == SnapshotRecord::Kind::kValue
                                ? RecordKind::kValue
                                : RecordKind::kTombstone;
    const ValueType value_type =
        kind == RecordKind::kValue ? applied.value_type_ : ValueType::kNone;
    if (kind == RecordKind::kValue && value_type == ValueType::kNone) {
      co_return absl::Status(absl::StatusCode::kInvalidArgument,
                             "replicated value has no Redis type");
    }
    const bool nonempty_collection =
        value_type == ValueType::kList || value_type == ValueType::kHash ||
        value_type == ValueType::kSet ||
        value_type == ValueType::kSortedSet;
    if (kind == RecordKind::kValue &&
        ((nonempty_collection && applied.logical_size_ == 0) ||
         (value_type == ValueType::kString &&
          applied.logical_size_ != applied.value_.size()))) {
      co_return absl::InvalidArgumentError(
          "replicated value has inconsistent logical size");
    }
    if (kind == RecordKind::kValue &&
        applied.logical_size_ > std::numeric_limits<std::uint32_t>::max()) {
      co_return absl::InvalidArgumentError(
          "replicated logical size exceeds record metadata");
    }
    absl::Status written;
    const bool key_external =
        applied.key_.size() > options_.inline_key_max_bytes_;
    const std::uint64_t logical_payload_bytes =
        static_cast<std::uint64_t>(applied.value_.size()) +
        (key_external ? applied.key_.size() : 0);
    const std::size_t inline_bytes =
        AlignRecord(RecordHeaderBytes(applied.key_.size(), key_external) +
                    static_cast<std::size_t>(logical_payload_bytes));
    if (inline_bytes > kStorageBlockBytes - kBlockHeaderBytes) [[unlikely]] {
      auto extents = co_await WriteExtentValueLocked(
          store,
          key_external ? std::string_view(applied.key_) : std::string_view{},
          applied.value_);
      if (!extents.ok()) {
        co_return extents.status();
      }
      const std::string manifest = EncodeManifest(**extents);
      written = co_await WriteRecordLocked(
          store, applied.db_id_, applied.key_, manifest, kind, value_type,
          applied.expire_at_ms_, digest, /*txid=*/0, applied.mutation_sequence_,
          false, true, true, key_external, applied.logical_size_, *extents);
      if (!written.ok()) {
        SpawnExtentReclaim(store, *extents);
      }
    } else {
      written = co_await WriteRecordLocked(
          store, applied.db_id_, applied.key_, applied.value_, kind, value_type,
          kind == RecordKind::kValue ? applied.expire_at_ms_ : 0, digest,
          /*txid=*/0, applied.mutation_sequence_, false, true, false,
          key_external, applied.logical_size_, nullptr);
    }
    if (!written.ok()) {
      co_return written;
    }
    partition.mutation_sequence_ =
        std::max(partition.mutation_sequence_, applied.mutation_sequence_);
  }
  co_return absl::OkStatus();
}

}  // namespace keylane::storage
