#include "engine_impl.h"

namespace keylane::storage {

ScanBatch StorageEngine::Impl::ScanPartition(std::uint16_t partition_id,
                                             std::uint8_t db_id,
                                             std::uint64_t cursor,
                                             std::size_t count,
                                             std::uint64_t now_ms) const {
  assert(db_id < kLogicalDatabaseCount);
  assert(count > 0);
  const auto& index =
      PartitionFor(CurrentStore(), partition_id).indexes[db_id];
  ScanBatch result;
  result.cursor = cursor;
  if (now_ms == 0) {
    now_ms = UnixTimeMillis();
  }
  const std::size_t max_iterations =
      count > std::numeric_limits<std::size_t>::max() / 10
          ? std::numeric_limits<std::size_t>::max()
          : count * 10;
  std::size_t iterations = 0;
  do {
    result.cursor = index.Scan(
        result.cursor, [&](const RecordIndex::Entry& entry) {
          if (entry.value.kind == RecordKind::kValue &&
              !IsExpired(entry.value, now_ms)) {
            result.keys.push_back(entry.key);
          }
        });
    ++iterations;
  } while (result.cursor != 0 && result.keys.size() < count &&
           iterations < max_iterations);
  return result;
}

PartitionReplicationStart StorageEngine::Impl::BeginPartitionReplication(
    std::uint16_t partition_id) {
  auto& partition = PartitionFor(CurrentStore(), partition_id);
  partition.capture_deltas = true;
  partition.deltas.clear();
  partition.delta_floor = partition.mutation_sequence;
  partition.delta_queued = false;
  PartitionReplicationStart result;
  result.snapshot_sequence = partition.mutation_sequence;
  for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
    result.db_epochs[db_id] = DbEpoch(db_id);
    if (partition.live_key_count[db_id] != 0) {
      result.nonempty_db_mask |= static_cast<std::uint16_t>(1U << db_id);
    }
  }
  return result;
}

Task<StatusOr<PartitionSnapshotBatch>> StorageEngine::Impl::SnapshotPartition(
    std::uint16_t partition_id, std::uint8_t db_id, std::uint64_t cursor,
    std::size_t count) {
  if (db_id >= kLogicalDatabaseCount || count == 0) {
    co_return Status(StatusCode::kInvalidArgument,
                     "invalid partition snapshot request");
  }
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionFor(store, partition_id);
  auto& index = partition.indexes[db_id];
  std::vector<std::string> keys;
  const std::uint64_t now_ms = UnixTimeMillis();
  std::uint64_t next = cursor;
  const std::size_t max_iterations =
      count > std::numeric_limits<std::size_t>::max() / 10
          ? std::numeric_limits<std::size_t>::max()
          : count * 10;
  std::size_t iterations = 0;
  do {
    next = index.Scan(next, [&](const RecordIndex::Entry& entry) {
      if (entry.value.kind == RecordKind::kValue &&
          !IsExpired(entry.value, now_ms)) {
        keys.push_back(entry.key);
      }
    });
    ++iterations;
  } while (next != 0 && keys.size() < count &&
           iterations < max_iterations);

  PartitionSnapshotBatch batch;
  batch.cursor = next;
  batch.records.reserve(keys.size());
  for (const std::string& key : keys) {
    const Digest digest = ComputeDigest(key);
    auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
        db_id, tx::FingerprintOf(digest), tx::LockMode::kShared);
    auto* current = index.Find(digest, key);
    if (current == nullptr || current->value.kind != RecordKind::kValue) {
      continue;
    }
    const RecordLocation location = current->value;
    if (IsExpired(location, UnixTimeMillis())) {
      QueueExpiredCandidate(store, partition.id, db_id, *current);
      continue;
    }
    auto loaded = co_await LoadValue(store, db_id, key, digest, location);
    if (!loaded.ok()) {
      if (loaded.status().code() == StatusCode::kNotFound) {
        continue;
      }
      co_return loaded.status();
    }
    const std::span<const std::byte> value = loaded->value();
    batch.records.push_back(SnapshotRecord{
        .kind = SnapshotRecord::Kind::kValue,
        .db_id = db_id,
        .db_epoch = DbEpoch(db_id),
        .mutation_sequence = location.mutation_sequence,
        .expire_at_ms = location.expire_at_ms,
        .value_type = location.value_type,
        .key = key,
        .value = std::string(reinterpret_cast<const char*>(value.data()),
                             value.size()),
    });
  }
  co_return batch;
}

PartitionDeltaBatch StorageEngine::Impl::ReadPartitionDeltas(
    std::uint16_t partition_id, std::uint64_t after_sequence, std::size_t count) {
  auto& partition = PartitionFor(CurrentStore(), partition_id);
  partition.delta_queued = false;
  PartitionDeltaBatch batch;
  batch.watermark = partition.mutation_sequence;
  batch.overflow = after_sequence < partition.delta_floor;
  if (batch.overflow || count == 0) {
    return batch;
  }
  batch.records.reserve(std::min(count, partition.deltas.size()));
  for (const SnapshotRecord& mutation : partition.deltas) {
    if (mutation.mutation_sequence > after_sequence) {
      batch.records.push_back(mutation);
      if (batch.records.size() == count) {
        break;
      }
    }
  }
  return batch;
}

void StorageEngine::Impl::AcknowledgePartitionDeltas(std::uint16_t partition_id,
                                                     std::uint64_t through_sequence) {
  auto& partition = PartitionFor(CurrentStore(), partition_id);
  while (!partition.deltas.empty() &&
         partition.deltas.front().mutation_sequence <= through_sequence) {
    partition.delta_floor = std::max(
        partition.delta_floor,
        partition.deltas.front().mutation_sequence);
    partition.deltas.pop_front();
  }
  if (!partition.deltas.empty() && !partition.delta_queued) {
    partition.delta_queued = true;
    (void)replication_ready_.enqueue(partition.id);
  }
}

Task<StatusOr<std::uint64_t>> StorageEngine::Impl::ResetReplicaPartition(
    std::uint16_t partition_id,
    std::span<const std::uint64_t, kLogicalDatabaseCount> source_db_epochs) {
  for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
    Status advanced = co_await AdvanceDbEpoch(db_id, source_db_epochs[db_id]);
    if (!advanced.ok()) {
      co_return advanced;
    }
  }

  WorkerStore& store = CurrentStore();
  auto& partition = PartitionFor(store, partition_id);
  if (partition.replication_epoch ==
      std::numeric_limits<std::uint64_t>::max()) {
    co_return Status(StatusCode::kOutOfRange,
                     "partition replication epoch exhausted");
  }
  // Stop this worker's append stream before making the new epoch durable.
  // Otherwise a concurrent command could append an old-epoch record after
  // the metadata commit and receive OK even though restart must discard it.
  co_await store.writer_mutex.Lock();
  UnlockGuard unlock(&store.writer_mutex, store.worker);
  const std::uint64_t next_epoch = partition.replication_epoch + 1;
  Status persisted = co_await PersistEpochValue(
      kLogicalDatabaseCount + partition_id, next_epoch);
  if (!persisted.ok()) {
    co_return persisted;
  }

  std::vector<std::pair<std::uint8_t, std::string>> old_keys;
  for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
    partition.indexes[db_id].ForEach(
        [&](const RecordIndex::Entry& entry) {
          old_keys.emplace_back(db_id, entry.key);
        });
  }
  partition.replication_epoch = next_epoch;
  partition.mutation_sequence = 0;
  partition.capture_deltas = false;
  partition.deltas.clear();
  partition.replica_value_stage.reset();
  partition.delta_floor = 0;
  partition.delta_queued = false;
  for (const auto& [db_id, key] : old_keys) {
    const Digest digest = ComputeDigest(key);
    Status tombstone = co_await WriteRecordLocked(
        store, db_id, key, {}, RecordKind::kTombstone, ValueType::kNone,
        0, digest, 0, 0, 0, false, false);
    if (!tombstone.ok()) {
      co_return tombstone;
    }
  }
  co_return next_epoch;
}

Task<Status> StorageEngine::Impl::ApplyReplicaRecords(
    std::uint16_t partition_id, std::uint64_t replication_epoch,
    std::span<const SnapshotRecord> records) {
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionFor(store, partition_id);
  if (replication_epoch != partition.replication_epoch) {
    co_return Status(StatusCode::kFailedPrecondition,
                     "stale partition replication epoch");
  }
  for (const SnapshotRecord& record : records) {
    std::optional<SnapshotRecord> materialized;
    const SnapshotRecord* effective = &record;
    if (record.db_id >= kLogicalDatabaseCount ||
        RedisSlot(record.key) != partition_id) {
      if (record.kind != SnapshotRecord::Kind::kFlushDb) {
        co_return Status(StatusCode::kInvalidArgument,
                         "replica record belongs to another partition");
      }
    }
    if (record.kind == SnapshotRecord::Kind::kFlushDb) {
      Status advanced = co_await AdvanceDbEpoch(record.db_id,
                                                record.db_epoch);
      if (!advanced.ok()) {
        co_return advanced;
      }
      partition.mutation_sequence = std::max(
          partition.mutation_sequence, record.mutation_sequence);
      continue;
    }
    if (record.db_epoch != DbEpoch(record.db_id)) {
      if (record.db_epoch < DbEpoch(record.db_id)) {
        continue;
      }
      co_return Status(StatusCode::kFailedPrecondition,
                       "replica record database epoch is not installed");
    }

    if (record.kind == SnapshotRecord::Kind::kValueBegin) {
      if (partition.replica_value_stage.has_value() ||
          record.value_type != ValueType::kString || !record.value.empty() ||
          record.logical_size == 0 ||
          record.logical_size > kMaxStringBytes ||
          record.chunk_count == 0 ||
          record.chunk_count !=
              (record.logical_size + kExtentPayloadBytes - 1) /
                  kExtentPayloadBytes) {
        co_return Status(StatusCode::kInvalidArgument,
                         "invalid replicated large value begin frame");
      }
      partition.replica_value_stage = ReplicaValueStage{
          .db_id = record.db_id,
          .db_epoch = record.db_epoch,
          .mutation_sequence = record.mutation_sequence,
          .expire_at_ms = record.expire_at_ms,
          .logical_size = record.logical_size,
          .next_chunk = 0,
          .chunk_count = record.chunk_count,
          .value_type = record.value_type,
          .key = record.key,
          .value = {},
      };
      partition.replica_value_stage->value.reserve(
          static_cast<std::size_t>(record.logical_size));
      continue;
    }
    if (record.kind == SnapshotRecord::Kind::kValueChunk) {
      auto& stage = partition.replica_value_stage;
      if (!stage.has_value() || stage->db_id != record.db_id ||
          stage->db_epoch != record.db_epoch ||
          stage->mutation_sequence != record.mutation_sequence ||
          stage->key != record.key ||
          stage->next_chunk != record.chunk_index ||
          stage->chunk_count != record.chunk_count ||
          record.value.empty() ||
          record.value.size() > kExtentPayloadBytes ||
          record.value.size() > stage->logical_size ||
          stage->value.size() > stage->logical_size - record.value.size()) {
        co_return Status(StatusCode::kInvalidArgument,
                         "invalid replicated large value chunk frame");
      }
      stage->value.append(record.value);
      ++stage->next_chunk;
      continue;
    }
    if (record.kind == SnapshotRecord::Kind::kValueCommit) {
      auto& stage = partition.replica_value_stage;
      if (!stage.has_value() || stage->db_id != record.db_id ||
          stage->db_epoch != record.db_epoch ||
          stage->mutation_sequence != record.mutation_sequence ||
          stage->key != record.key || !record.value.empty() ||
          stage->next_chunk != stage->chunk_count ||
          record.chunk_index != stage->chunk_count ||
          stage->value.size() != stage->logical_size) {
        co_return Status(StatusCode::kInvalidArgument,
                         "invalid replicated large value commit frame");
      }
      materialized.emplace(SnapshotRecord{
          .kind = SnapshotRecord::Kind::kValue,
          .db_id = stage->db_id,
          .db_epoch = stage->db_epoch,
          .mutation_sequence = stage->mutation_sequence,
          .expire_at_ms = stage->expire_at_ms,
          .value_type = stage->value_type,
          .logical_size = stage->logical_size,
          .key = std::move(stage->key),
          .value = std::move(stage->value),
      });
      stage.reset();
      effective = &*materialized;
    } else if (partition.replica_value_stage.has_value()) {
      co_return Status(StatusCode::kInvalidArgument,
                       "replicated large value frame sequence interrupted");
    }

    const SnapshotRecord& applied = *effective;
    const Digest digest = ComputeDigest(applied.key);
    auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
        applied.db_id, tx::FingerprintOf(digest), tx::LockMode::kExclusive);
    // Replicated modifications invalidate local watchers too.
    tx::CurrentTxShard().MarkWatched(applied.db_id,
                                     tx::FingerprintOf(digest));
    co_await store.writer_mutex.Lock();
    UnlockGuard write_unlock(&store.writer_mutex, store.worker);
    auto& index = partition.indexes[applied.db_id];
    auto* current = index.Find(digest, applied.key);
    if (current != nullptr &&
        current->value.replication_epoch == replication_epoch &&
        current->value.mutation_sequence >= applied.mutation_sequence) {
      continue;
    }
    const RecordKind kind = applied.kind == SnapshotRecord::Kind::kValue
                                ? RecordKind::kValue
                                : RecordKind::kTombstone;
    const ValueType value_type = kind == RecordKind::kValue
                                     ? applied.value_type
                                     : ValueType::kNone;
    if (kind == RecordKind::kValue && value_type == ValueType::kNone) {
      co_return Status(StatusCode::kInvalidArgument,
                       "replicated value has no Redis type");
    }
    Status written;
    const std::size_t inline_bytes = AlignRecord(
        RecordHeaderBytes(applied.key.size()) + applied.value.size());
    if (kind == RecordKind::kValue && value_type == ValueType::kString &&
        inline_bytes > kStorageBlockBytes - kBlockHeaderBytes) {
      auto extents = co_await WriteExtentValueLocked(store, applied.value);
      if (!extents.ok()) {
        co_return extents.status();
      }
      const std::string manifest = EncodeManifest(**extents);
      written = co_await WriteRecordLocked(
          store, applied.db_id, applied.key, manifest, kind, value_type,
          applied.expire_at_ms, digest, applied.mutation_sequence,
          applied.mutation_sequence, 0, false, true, true,
          applied.value.size(), *extents);
      if (!written.ok()) {
        SpawnExtentReclaim(store, *extents);
      }
    } else {
      written = co_await WriteRecordLocked(
          store, applied.db_id, applied.key, applied.value, kind, value_type,
          kind == RecordKind::kValue ? applied.expire_at_ms : 0, digest,
          applied.mutation_sequence, applied.mutation_sequence, 0, false);
    }
    if (!written.ok()) {
      co_return written;
    }
    partition.mutation_sequence = std::max(
        partition.mutation_sequence, applied.mutation_sequence);
  }
  co_return Status::Ok();
}

}  // namespace keylane::storage
