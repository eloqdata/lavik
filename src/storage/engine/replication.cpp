#include "impl.h"

namespace keylane::storage {

struct StorageEngine::Impl::SnapshotReadJoin {
  std::size_t pending_ = 0;
  std::coroutine_handle<> waiter_;
  absl::Status error_;

  void Complete(absl::Status status) {
    if (!status.ok() && error_.ok()) {
      error_ = std::move(status);
    }
    assert(pending_ != 0);
    if (--pending_ == 0 && waiter_) {
      const auto waiter = std::exchange(waiter_, {});
      celer::ThisWorker().self_->Enqueue(waiter);
    }
  }

  auto Join() {
    struct Awaiter {
      SnapshotReadJoin* join_;
      bool await_ready() const noexcept { return join_->pending_ == 0; }
      void await_suspend(std::coroutine_handle<> waiter) const noexcept {
        join_->waiter_ = waiter;
      }
      void await_resume() const noexcept {}
    };
    return Awaiter{this};
  }
};

Task<absl::Status> StorageEngine::Impl::ReadSnapshotRecord(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    RecordIndex& index, std::uint8_t db_id, const std::string* key,
    std::uint64_t session_id, std::uint64_t baseline_version,
    std::optional<SnapshotRecord>* output, SnapshotReadJoin* join) {
  absl::Status status;
  {
    const Digest digest = ComputeDigest(*key);
    auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
        db_id, tx::FingerprintOf(digest), tx::LockMode::kShared);
    const std::string& coverage_key = *key;
    const std::string override_key = FullSyncOverrideKey(db_id, *key);
    auto capture = partition.fullsync_subscribers_.find(session_id);
    if (capture == partition.fullsync_subscribers_.end() ||
        capture->second.phase_ !=
            WorkerStore::FullSyncCapture::Phase::kCapturing ||
        capture->second.db_phases_[db_id] !=
            WorkerStore::FullSyncCapture::DbPhase::kScanning ||
        capture->second.latest_by_key_.contains(override_key) ||
        capture->second.key_phases_.Find(digest, coverage_key) != nullptr) {
      key_lock.Reset();
      join->Complete(absl::OkStatus());
      co_return absl::OkStatus();
    }
    auto resolved = co_await FindVerifiedEntry(store, index, digest, *key);
    if (!resolved.ok()) {
      status = resolved.status();
    } else {
      auto* current = *resolved;
      if (current != nullptr && current->value_.kind_ == RecordKind::kValue) {
        const RecordLocation location = current->value_;
        if (IsExpired(location, UnixTimeMillis())) {
          QueueExpiredCandidate(store, partition.id_, db_id, *current, *key);
        } else {
          auto loaded =
              co_await LoadValue(store, partition, db_id, *key, digest,
                                 location, ExtentsFor(store, current));
          if (!loaded.ok()) {
            if (loaded.status().code() != absl::StatusCode::kNotFound) {
              status = loaded.status();
            }
          } else {
            const std::span<const std::byte> value = loaded->value();
            output->emplace(SnapshotRecord{
                .kind_ = SnapshotRecord::Kind::kValue,
                .db_id_ = db_id,
                .db_epoch_ = DbEpoch(db_id),
                .mutation_sequence_ = baseline_version,
                .expire_at_ms_ = location.expire_at_ms_,
                .value_type_ = location.value_type_,
                .logical_size_ = location.logical_size_,
                .key_digest_ = digest,
                .key_ = *key,
                .value_ = std::string(
                    reinterpret_cast<const char*>(value.data()), value.size()),
            });
          }
        }
      }
    }
    if (status.ok() && output->has_value()) {
      capture = partition.fullsync_subscribers_.find(session_id);
      if (capture == partition.fullsync_subscribers_.end() ||
          capture->second.phase_ !=
              WorkerStore::FullSyncCapture::Phase::kCapturing) {
        output->reset();
      } else {
        capture->second.key_phases_.InsertOrAssign(
            digest, coverage_key,
            WorkerStore::FullSyncCapture::KeyPhase::kBaselineInflight);
      }
    }
    // Release the shared key hold before waking the parent so foreground
    // writes never wait for frame cleanup.
    key_lock.Reset();
  }
  join->Complete(std::move(status));
  co_return absl::OkStatus();
}

Task<absl::StatusOr<SnapshotRecord>>
StorageEngine::Impl::ReadFullSyncOverrideRecord(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    const SnapshotRecord& requested) {
  const Digest digest = ComputeDigest(requested.key_);
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      requested.db_id_, tx::FingerprintOf(digest), tx::LockMode::kShared);
  auto& index = partition.indexes_[requested.db_id_];
  auto resolved =
      co_await FindVerifiedEntry(store, index, digest, requested.key_);
  if (!resolved.ok()) co_return resolved.status();

  const RecordIndex::Entry* current = *resolved;
  if (current == nullptr || current->value_.kind_ != RecordKind::kValue ||
      IsExpired(current->value_, UnixTimeMillis())) {
    const std::uint64_t sequence =
        current == nullptr ? requested.mutation_sequence_
                           : std::max(requested.mutation_sequence_,
                                      current->value_.mutation_sequence_);
    key_lock.Reset();
    co_return SnapshotRecord{
        .kind_ = SnapshotRecord::Kind::kDelete,
        .db_id_ = requested.db_id_,
        .db_epoch_ = DbEpoch(requested.db_id_),
        .mutation_sequence_ = sequence,
        .key_ = requested.key_,
        .value_ = {},
    };
  }

  const RecordLocation location = current->value_;
  auto loaded =
      co_await LoadValue(store, partition, requested.db_id_, requested.key_,
                         digest, location, ExtentsFor(store, current));
  if (!loaded.ok()) co_return loaded.status();
  const std::span<const std::byte> value = loaded->value();
  SnapshotRecord result{
      .kind_ = SnapshotRecord::Kind::kValue,
      .db_id_ = requested.db_id_,
      .db_epoch_ = DbEpoch(requested.db_id_),
      .mutation_sequence_ =
          std::max(requested.mutation_sequence_, location.mutation_sequence_),
      .expire_at_ms_ = location.expire_at_ms_,
      .value_type_ = location.value_type_,
      .logical_size_ = location.logical_size_,
      .key_ = requested.key_,
      .value_ = std::string(reinterpret_cast<const char*>(value.data()),
                            value.size()),
  };
  key_lock.Reset();
  co_return result;
}

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

absl::StatusOr<FullSyncSessionStart> StorageEngine::Impl::BeginFullSyncSession(
    std::uint64_t session_id) {
  WorkerStore& store = CurrentStore();
  auto [session, inserted] = store.fullsync_sessions_.try_emplace(session_id);
  if (!inserted) {
    if (session->second.db_epoch_invalidated_) {
      return absl::FailedPreconditionError(
          "full-sync session was invalidated by a database epoch advance");
    }
    return FullSyncSessionStart{.db_epochs_ = session->second.db_epochs_};
  }
  for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
    session->second.db_epochs_[db_id] = DbEpoch(db_id);
  }
  // One worker scans one partition at a time. Reserve reusable headroom for
  // the largest owner-local coverage map before exposing the session. Actual
  // DB maps and read pins are released at each handoff; this logical
  // reservation stays until the session ends so foreground growth cannot
  // make a later partition fail halfway through the same full sync.
  // A dirty key may occupy a std::map replacement node plus one ScanHashMap
  // coverage entry. Both are metadata-only; value bytes are deliberately
  // excluded. Keep a conservative fixed allowance for bucket/node/control
  // overhead and both possible logical key copies. This is reservation
  // accounting, not an on-disk format constant.
  constexpr std::size_t kCoverageMetadataBytesPerKey = 320;
  constexpr std::size_t kCoverageFixedBytes = 64 * 1024;
  std::size_t largest_partition_bytes = 0;
  for (const auto& partition : store.partitions_) {
    std::size_t bytes = kCoverageFixedBytes;
    bool overflow = false;
    for (const auto& index : partition.indexes_) {
      std::uint64_t cursor = 0;
      do {
        cursor = index.Scan(cursor, [&](const RecordIndex::Entry& entry) {
          if (overflow) return;
          const std::size_t key_bytes = entry.logical_key_size();
          if (key_bytes > (std::numeric_limits<std::size_t>::max() -
                           kCoverageMetadataBytesPerKey) /
                              2 ||
              kCoverageMetadataBytesPerKey + key_bytes * 2 >
                  std::numeric_limits<std::size_t>::max() - bytes) {
            overflow = true;
            return;
          }
          bytes += kCoverageMetadataBytesPerKey + key_bytes * 2;
        });
      } while (cursor != 0 && !overflow);
    }
    if (overflow) {
      store.fullsync_sessions_.erase(session);
      return absl::ResourceExhaustedError(
          "full-sync coverage reservation overflow");
    }
    largest_partition_bytes = std::max(largest_partition_bytes, bytes);
  }
  const std::size_t reserve =
      std::max(kCoverageFixedBytes, largest_partition_bytes);
  if (!TryReserveFullSyncMemory(reserve)) {
    store.fullsync_sessions_.erase(session);
    return absl::ResourceExhaustedError(
        "insufficient maxmemory headroom for full-sync coverage");
  }
  session->second.reserved_memory_bytes_ = reserve;
  return FullSyncSessionStart{.db_epochs_ = session->second.db_epochs_};
}

bool StorageEngine::Impl::FullSyncSessionValid(
    std::uint64_t session_id) const noexcept {
  const WorkerStore& store = CurrentStore();
  const auto session = store.fullsync_sessions_.find(session_id);
  return session != store.fullsync_sessions_.end() &&
         !session->second.db_epoch_invalidated_;
}

void StorageEngine::Impl::EndFullSyncSession(std::uint64_t session_id) {
  WorkerStore& store = CurrentStore();
  for (auto& partition : store.partitions_) {
    auto capture = partition.fullsync_subscribers_.find(session_id);
    if (capture == partition.fullsync_subscribers_.end()) continue;
    ClearFullSyncCapture(store, capture->second);
    partition.fullsync_subscribers_.erase(capture);
  }
  auto session = store.fullsync_sessions_.find(session_id);
  if (session != store.fullsync_sessions_.end()) {
    session->second.publish_queue_.clear();
    session->second.publish_queue_bytes_ = 0;
    session->second.publisher_admitted_bytes_ = 0;
    ReleaseFullSyncMemory(session->second.reserved_memory_bytes_);
    store.fullsync_sessions_.erase(session);
  }
  store.fullsync_publisher_capacity_ready_.NotifyAll(*store.worker_);
}

absl::StatusOr<PartitionReplicationStart>
StorageEngine::Impl::BeginPartitionReplication(std::uint64_t session_id,
                                               std::uint16_t partition_id) {
  WorkerStore& store = CurrentStore();
  const auto session = store.fullsync_sessions_.find(session_id);
  if (session == store.fullsync_sessions_.end()) {
    return absl::FailedPreconditionError(
        "full-sync session has not been started on this worker");
  }
  if (session->second.db_epoch_invalidated_) {
    return absl::FailedPreconditionError(
        "full-sync session was invalidated by a database epoch advance");
  }
  auto& partition = PartitionFor(store, partition_id);
  auto [capture, inserted] =
      partition.fullsync_subscribers_.try_emplace(session_id);
  if (!inserted) {
    ClearFullSyncCapture(store, capture->second);
  }
  capture->second.baseline_version_ = partition.mutation_sequence_;
  capture->second.db_phases_.fill(
      WorkerStore::FullSyncCapture::DbPhase::kUnstarted);
  capture->second.phase_ = WorkerStore::FullSyncCapture::Phase::kCapturing;
  PartitionReplicationStart result;
  result.baseline_version_ = capture->second.baseline_version_;
  for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
    result.db_epochs_[db_id] = session->second.db_epochs_[db_id];
    if (partition.live_key_count_[db_id] != 0) {
      result.nonempty_db_mask_ |= static_cast<std::uint16_t>(1U << db_id);
    }
  }
  return result;
}

absl::Status StorageEngine::Impl::BeginPartitionDbReplication(
    std::uint64_t session_id, std::uint16_t partition_id, std::uint8_t db_id) {
  if (db_id >= kLogicalDatabaseCount) {
    return absl::InvalidArgumentError("invalid full-sync database");
  }
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionFor(store, partition_id);
  const auto session = store.fullsync_sessions_.find(session_id);
  if (session == store.fullsync_sessions_.end() ||
      session->second.db_epoch_invalidated_) {
    return absl::FailedPreconditionError("full-sync session is not active");
  }
  auto capture = partition.fullsync_subscribers_.find(session_id);
  if (capture == partition.fullsync_subscribers_.end() ||
      capture->second.phase_ !=
          WorkerStore::FullSyncCapture::Phase::kCapturing) {
    return absl::FailedPreconditionError(
        "full-sync partition capture is not active");
  }
  for (std::uint8_t other = 0; other < kLogicalDatabaseCount; ++other) {
    if (other != db_id &&
        capture->second.db_phases_[other] ==
            WorkerStore::FullSyncCapture::DbPhase::kScanning) {
      return absl::FailedPreconditionError(
          "another database is already scanning in this partition");
    }
  }
  if (capture->second.db_phases_[db_id] !=
      WorkerStore::FullSyncCapture::DbPhase::kUnstarted) {
    return absl::FailedPreconditionError(
        "full-sync database has already started");
  }
  const std::uint32_t target_id =
      (static_cast<std::uint32_t>(partition_id) << 8) | db_id;
  if (session->second.unstarted_admissions_.contains(target_id)) {
    return absl::UnavailableError(
        "full-sync database has an admitted UNSTARTED write");
  }
  capture->second.key_phases_.Clear();
  capture->second.db_phases_[db_id] =
      WorkerStore::FullSyncCapture::DbPhase::kScanning;
  return absl::OkStatus();
}

void StorageEngine::Impl::EndPartitionReplication(std::uint64_t session_id,
                                                  std::uint16_t partition_id) {
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionFor(store, partition_id);
  auto capture = partition.fullsync_subscribers_.find(session_id);
  if (capture == partition.fullsync_subscribers_.end()) return;
  ClearFullSyncCapture(store, capture->second);
  partition.fullsync_subscribers_.erase(capture);
}

Task<absl::StatusOr<PartitionSnapshotBatch>>
StorageEngine::Impl::SnapshotPartition(std::uint64_t session_id,
                                       std::uint16_t partition_id,
                                       std::uint8_t db_id, std::uint64_t cursor,
                                       std::size_t count,
                                       std::size_t read_concurrency) {
  if (db_id >= kLogicalDatabaseCount || count == 0 || read_concurrency == 0) {
    co_return absl::Status(absl::StatusCode::kInvalidArgument,
                           "invalid partition snapshot request");
  }
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionFor(store, partition_id);
  const auto session = store.fullsync_sessions_.find(session_id);
  const auto capture = partition.fullsync_subscribers_.find(session_id);
  if (session == store.fullsync_sessions_.end() ||
      session->second.db_epoch_invalidated_ ||
      capture == partition.fullsync_subscribers_.end() ||
      capture->second.phase_ !=
          WorkerStore::FullSyncCapture::Phase::kCapturing ||
      capture->second.db_phases_[db_id] !=
          WorkerStore::FullSyncCapture::DbPhase::kScanning) {
    co_return absl::Status(absl::StatusCode::kFailedPrecondition,
                           "full-sync partition capture is not active");
  }
  const std::uint64_t baseline_version = capture->second.baseline_version_;
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
  std::vector<std::optional<SnapshotRecord>> records(keys.size());
  for (std::size_t first = 0; first < keys.size();) {
    const std::size_t last =
        first + std::min(read_concurrency, keys.size() - first);
    SnapshotReadJoin join;
    join.pending_ = last - first;
    for (std::size_t i = first; i < last; ++i) {
      // Snapshot work is an online replication task, so it remains on the
      // foreground queue. The bounded wave yields naturally between groups.
      store.worker_->Spawn(
          ReadSnapshotRecord(store, partition, index, db_id, &keys[i],
                             session_id, baseline_version, &records[i], &join));
    }
    co_await join.Join();
    if (!join.error_.ok()) {
      co_return join.error_;
    }
    first = last;
  }
  for (std::optional<SnapshotRecord>& record : records) {
    if (record.has_value()) {
      batch.records_.push_back(std::move(*record));
    }
  }
  co_return batch;
}

Task<absl::StatusOr<PartitionFullSyncBatch>>
StorageEngine::Impl::ReadPartitionFullSyncOverrides(std::uint64_t session_id,
                                                    std::uint16_t partition_id,
                                                    std::size_t count) {
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionFor(store, partition_id);
  const auto session = store.fullsync_sessions_.find(session_id);
  auto capture = partition.fullsync_subscribers_.find(session_id);
  if (session == store.fullsync_sessions_.end() ||
      session->second.db_epoch_invalidated_ ||
      capture == partition.fullsync_subscribers_.end()) {
    co_return absl::FailedPreconditionError(
        "full-sync partition capture is not active");
  }
  PartitionFullSyncBatch batch;
  if (count == 0) co_return batch;
  std::vector<SnapshotRecord> requested;
  requested.reserve(std::min(count, capture->second.overrides_.size()));
  for (const auto& [sequence, record] : capture->second.overrides_) {
    (void)sequence;
    requested.push_back(record);
    if (requested.size() == count) break;
  }
  batch.records_.reserve(requested.size());
  for (const SnapshotRecord& record : requested) {
    auto loaded = co_await ReadFullSyncOverrideRecord(store, partition, record);
    if (!loaded.ok()) co_return loaded.status();
    batch.records_.push_back(std::move(*loaded));
  }
  co_return batch;
}

void StorageEngine::Impl::AcknowledgePartitionFullSyncOverrides(
    std::uint64_t session_id, std::uint16_t partition_id,
    std::span<const SnapshotRecord> records) {
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionFor(store, partition_id);
  const auto session = store.fullsync_sessions_.find(session_id);
  auto capture = partition.fullsync_subscribers_.find(session_id);
  if (session == store.fullsync_sessions_.end() ||
      session->second.db_epoch_invalidated_ ||
      capture == partition.fullsync_subscribers_.end()) {
    return;
  }
  for (const SnapshotRecord& acknowledged : records) {
    const std::string key =
        FullSyncOverrideKey(acknowledged.db_id_, acknowledged.key_);
    auto latest = capture->second.latest_by_key_.find(key);
    if (latest == capture->second.latest_by_key_.end() ||
        latest->second > acknowledged.mutation_sequence_) {
      continue;
    }
    auto current = capture->second.overrides_.find(latest->second);
    if (current == capture->second.overrides_.end()) continue;
    capture->second.latest_by_key_.erase(latest);
    capture->second.overrides_.erase(current);
    if (capture->second.phase_ ==
            WorkerStore::FullSyncCapture::Phase::kCapturing &&
        capture->second.db_phases_[acknowledged.db_id_] ==
            WorkerStore::FullSyncCapture::DbPhase::kScanning) {
      capture->second.key_phases_.InsertOrAssign(
          ComputeDigest(acknowledged.key_), acknowledged.key_,
          WorkerStore::FullSyncCapture::KeyPhase::kTailing);
    }
  }
  if (capture->second.overrides_.empty()) {
    // The session keeps only its logical max-partition reservation. Release
    // this partition's actual hash allocation as soon as every materialized
    // replacement is acknowledged; later writes may allocate from the same
    // reserved headroom without pinning a completed scan window.
    capture->second.latest_by_key_.clear();
    capture->second.latest_by_key_.rehash(0);
  }
}

void StorageEngine::Impl::AcknowledgePartitionSnapshotRecords(
    std::uint64_t session_id, std::uint16_t partition_id,
    std::span<const SnapshotRecord> records) {
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionFor(store, partition_id);
  auto capture = partition.fullsync_subscribers_.find(session_id);
  if (capture == partition.fullsync_subscribers_.end() ||
      capture->second.phase_ !=
          WorkerStore::FullSyncCapture::Phase::kCapturing) {
    return;
  }
  for (const SnapshotRecord& record : records) {
    const std::string& key = record.key_;
    auto* phase = capture->second.key_phases_.Find(record.key_digest_, key);
    if (phase == nullptr ||
        phase->value_ !=
            WorkerStore::FullSyncCapture::KeyPhase::kBaselineInflight) {
      continue;
    }
    // A transaction participant may have fallen back to a newer replacement
    // while this baseline was in flight. In that case the baseline ACK must
    // not make the key command-eligible yet.
    if (capture->second.latest_by_key_.contains(
            FullSyncOverrideKey(record.db_id_, record.key_))) {
      capture->second.key_phases_.Erase(phase);
    } else {
      phase->value_ = WorkerStore::FullSyncCapture::KeyPhase::kTailing;
    }
  }
}

absl::Status StorageEngine::Impl::CompletePartitionReplication(
    std::uint64_t session_id, std::uint16_t partition_id) {
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionFor(store, partition_id);
  auto capture = partition.fullsync_subscribers_.find(session_id);
  if (capture == partition.fullsync_subscribers_.end()) {
    return absl::FailedPreconditionError(
        "full-sync partition capture is not active");
  }
  if (!capture->second.overrides_.empty()) {
    return absl::UnavailableError(
        "full-sync partition still has pending replacements");
  }
  if (std::any_of(capture->second.db_phases_.begin(),
                  capture->second.db_phases_.end(), [](auto phase) {
                    return phase !=
                           WorkerStore::FullSyncCapture::DbPhase::kTailing;
                  })) {
    return absl::FailedPreconditionError(
        "full-sync partition still has an incomplete database");
  }
  capture->second.phase_ = WorkerStore::FullSyncCapture::Phase::kTailing;
  capture->second.latest_by_key_.clear();
  capture->second.latest_by_key_.rehash(0);
  capture->second.key_phases_.Clear();
  return absl::OkStatus();
}

absl::Status StorageEngine::Impl::CompletePartitionDbReplication(
    std::uint64_t session_id, std::uint16_t partition_id, std::uint8_t db_id) {
  if (db_id >= kLogicalDatabaseCount) {
    return absl::InvalidArgumentError("invalid full-sync database");
  }
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionFor(store, partition_id);
  auto capture = partition.fullsync_subscribers_.find(session_id);
  if (capture == partition.fullsync_subscribers_.end() ||
      capture->second.db_phases_[db_id] !=
          WorkerStore::FullSyncCapture::DbPhase::kScanning) {
    return absl::FailedPreconditionError(
        "full-sync database scan is not active");
  }
  const bool pending = std::any_of(
      capture->second.overrides_.begin(), capture->second.overrides_.end(),
      [db_id](const auto& entry) { return entry.second.db_id_ == db_id; });
  if (pending) {
    return absl::UnavailableError(
        "full-sync database still has pending replacements");
  }
  capture->second.db_phases_[db_id] =
      WorkerStore::FullSyncCapture::DbPhase::kTailing;
  capture->second.key_phases_.Clear();
  return absl::OkStatus();
}

absl::StatusOr<std::vector<FullSyncPublishItem>>
StorageEngine::Impl::PeekFullSyncPublishItems(std::uint64_t session_id,
                                              std::size_t max_items) {
  WorkerStore& store = CurrentStore();
  auto session = store.fullsync_sessions_.find(session_id);
  if (session == store.fullsync_sessions_.end() ||
      session->second.db_epoch_invalidated_) {
    return absl::FailedPreconditionError(
        "full-sync publish session is not active");
  }
  if (max_items == 0) {
    return absl::InvalidArgumentError(
        "full-sync publish batch size must be nonzero");
  }
  std::vector<FullSyncPublishItem> result;
  result.reserve(std::min(max_items, session->second.publish_queue_.size()));
  const std::size_t count =
      std::min(max_items, session->second.publish_queue_.size());
  for (std::size_t index = 0; index < count; ++index) {
    const auto& pending = session->second.publish_queue_[index];
    result.push_back(
        FullSyncPublishItem{.id_ = pending.id_, .command_ = pending.command_});
  }
  return result;
}

absl::StatusOr<FullSyncPublishQueueInfo>
StorageEngine::Impl::GetFullSyncPublishQueueInfo(
    std::uint64_t session_id) const {
  const WorkerStore& store = CurrentStore();
  const auto session = store.fullsync_sessions_.find(session_id);
  if (session == store.fullsync_sessions_.end() ||
      session->second.db_epoch_invalidated_) {
    return absl::FailedPreconditionError(
        "full-sync publish session is not active");
  }
  return FullSyncPublishQueueInfo{
      .queued_bytes_ = session->second.publish_queue_bytes_,
      .admitted_bytes_ = session->second.publisher_admitted_bytes_,
      .capacity_bytes_ =
          replication_publish_queue_bytes_.load(std::memory_order_acquire),
  };
}

void StorageEngine::Impl::AcknowledgeFullSyncPublishItem(
    std::uint64_t session_id, std::uint64_t item_id) {
  WorkerStore& store = CurrentStore();
  auto session = store.fullsync_sessions_.find(session_id);
  if (session == store.fullsync_sessions_.end() ||
      session->second.publish_queue_.empty() ||
      session->second.publish_queue_.front().id_ != item_id) {
    return;
  }
  const std::size_t bytes =
      session->second.publish_queue_.front().logical_bytes_;
  session->second.publish_queue_.pop_front();
  session->second.publish_queue_bytes_ -=
      std::min(session->second.publish_queue_bytes_, bytes);
  store.fullsync_publisher_capacity_ready_.NotifyAll(*store.worker_);
}

Task<absl::StatusOr<std::uint64_t>> StorageEngine::Impl::ResetReplicaPartition(
    std::uint16_t partition_id,
    std::span<const std::uint64_t, kLogicalDatabaseCount> source_db_epochs,
    std::uint64_t persisted_replication_epoch, bool replica_lock_held) {
  WorkerStore& store = CurrentStore();
  std::unique_ptr<UnlockGuard> replica_unlock;
  if (!replica_lock_held) {
    co_await store.replica_apply_mutex_.Lock();
    replica_unlock = std::make_unique<UnlockGuard>(&store.replica_apply_mutex_,
                                                   store.worker_);
  }
  if (persisted_replication_epoch == 0) {
    for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
      absl::Status advanced =
          co_await AdvanceDbEpoch(db_id, source_db_epochs[db_id]);
      if (!advanced.ok()) {
        co_return advanced;
      }
    }
  }

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
  if (persisted_replication_epoch != 0 &&
      persisted_replication_epoch != next_epoch) {
    co_return absl::Status(absl::StatusCode::kAborted,
                           "replica reset superseded by another session");
  }
  if (persisted_replication_epoch == 0) {
    absl::Status persisted = co_await PersistEpochValue(
        kLogicalDatabaseCount + partition_id, next_epoch);
    if (!persisted.ok()) {
      co_return persisted;
    }
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
      old_keys.push_back(OldKey{.db_id_ = db_id, .key_ = std::move(*key)});
    }
  }
  partition.replication_epoch_ = next_epoch;
  partition.mutation_sequence_ = 0;
  for (auto& [session_id, capture] : partition.fullsync_subscribers_) {
    (void)session_id;
    ClearFullSyncCapture(store, capture);
  }
  partition.fullsync_subscribers_.clear();
  partition.replica_value_stage_.reset();
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

Task<absl::StatusOr<std::vector<ReplicaPartitionEpoch>>>
StorageEngine::Impl::ResetReplicaPartitions(
    std::uint64_t session_id, std::span<const ReplicaPartitionReset> resets) {
  if (session_id == 0 || resets.empty()) {
    co_return absl::Status(absl::StatusCode::kInvalidArgument,
                           "replica reset batch is empty");
  }
  WorkerStore& store = CurrentStore();
  co_await store.replica_apply_mutex_.Lock();
  UnlockGuard replica_unlock(&store.replica_apply_mutex_, store.worker_);
  std::array<bool, kLogicalStorageShards> seen{};
  std::array<std::uint64_t, kLogicalDatabaseCount> local_db_epochs{};
  for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
    const std::uint64_t current = DbEpoch(db_id);
    if (current == std::numeric_limits<std::uint64_t>::max()) {
      co_return absl::OutOfRangeError("database epoch exhausted");
    }
    local_db_epochs[db_id] = current + 1;
  }
  std::vector<std::pair<std::size_t, std::uint64_t>> epoch_updates;
  epoch_updates.reserve(resets.size());
  for (const ReplicaPartitionReset& reset : resets) {
    if (reset.partition_id_ >= kLogicalStorageShards ||
        reset.partition_id_ % worker_count_ != celer::ThisWorker().id_ ||
        seen[reset.partition_id_]) {
      co_return absl::Status(absl::StatusCode::kInvalidArgument,
                             "invalid replica reset batch partition");
    }
    seen[reset.partition_id_] = true;
    for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
      if (reset.db_epochs_[db_id] == 0) {
        co_return absl::Status(absl::StatusCode::kInvalidArgument,
                               "invalid replica reset batch database epoch");
      }
    }
    const auto& partition = PartitionFor(CurrentStore(), reset.partition_id_);
    if (partition.replica_sync_ != nullptr) {
      co_return absl::FailedPreconditionError(
          "replica partition is already being synchronized");
    }
    if (partition.replica_candidate_epoch_ ==
        std::numeric_limits<std::uint64_t>::max()) {
      co_return absl::Status(absl::StatusCode::kOutOfRange,
                             "partition replication epoch exhausted");
    }
    epoch_updates.emplace_back(kLogicalDatabaseCount + reset.partition_id_,
                               partition.replica_candidate_epoch_ + 1);
  }
  // Reserving the candidate epochs before detaching the old population makes
  // recovery discard both old records and an interrupted partial rebuild.
  // A replica always restarts in LOADING and begins a fresh full sync.
  absl::Status persisted = co_await PersistEpochValues(epoch_updates);
  if (!persisted.ok()) co_return persisted;

  co_await store.store_state_mutex_.Lock();
  UnlockGuard write_unlock(&store.store_state_mutex_, store.worker_);
  std::vector<ReplicaPartitionEpoch> result;
  result.reserve(resets.size());
  for (std::size_t index = 0; index < resets.size(); ++index) {
    const ReplicaPartitionReset& reset = resets[index];
    auto& partition = PartitionFor(store, reset.partition_id_);
    if (partition.replica_sync_ != nullptr) {
      co_return absl::FailedPreconditionError(
          "replica partition is already being synchronized");
    }
    auto sync =
        std::make_unique<WorkerStore::PartitionStore::ReplicaSyncState>();
    sync->session_id_ = session_id;
    sync->replication_epoch_ = epoch_updates[index].second;
    sync->source_db_epochs_ = reset.db_epochs_;
    for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
      sync->local_db_epochs_[db_id] = local_db_epochs[db_id];
      if (partition.indexes_[db_id].has_allocated_storage()) {
        store.detached_indexes_.push_back(DetachedIndex{
            .index_ = partition.indexes_[db_id].Detach(),
            .db_id_ = db_id,
        });
      }
      if (store.live_key_count_[db_id] < partition.live_key_count_[db_id])
          [[unlikely]] {
        co_return absl::InternalError(
            "replica reset found inconsistent live-key accounting");
      }
      store.live_key_count_[db_id] -= partition.live_key_count_[db_id];
      partition.live_key_count_[db_id] = 0;
      partition.expiring_key_count_[db_id] = 0;
    }
    partition.replica_candidate_epoch_ = epoch_updates[index].second;
    partition.replication_epoch_ = epoch_updates[index].second;
    partition.mutation_sequence_ = 0;
    partition.replica_value_stage_.reset();
    partition.replica_sync_ = std::move(sync);
    result.push_back(ReplicaPartitionEpoch{
        .partition_id_ = reset.partition_id_,
        .replication_epoch_ = epoch_updates[index].second,
    });
  }
  for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
    ++store.index_generations_[db_id];
    tx::CurrentTxShard().MarkAllWatched(db_id);
  }
  replica_loading_.store(true, std::memory_order_release);
  EnsureDetachedReclaim(store);
  co_return result;
}

// TODO(replication): ResetReplicaPartition above holds store_state_mutex across
// its whole tombstone loop (unlock_writer_while_waiting=false), so on a full
// device its inline block allocation waits for reclaim progress while the
// flush that would free space is itself waiting for this store_state_mutex — a
// three-way stall that never resolves. When the epoch redesign lands, the
// loop should release the mutex around allocation waits and revalidate
// (db_epoch, replication_epoch, index_generation) afterwards, the same
// expected-version handoff defrag relocation uses.
//
// TODO(replication): this path applies records without invoking source-side
// full-sync subscribers, so a node that is both a replica and a source
// (A -> B -> C) silently forwards nothing after the snapshot baseline.
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
Task<absl::Status> StorageEngine::Impl::HandoffReplicaPartition(
    std::uint64_t session_id, std::uint16_t partition_id,
    std::uint64_t replication_epoch) {
  WorkerStore& store = CurrentStore();
  if (partition_id >= kLogicalStorageShards ||
      partition_id % worker_count_ != celer::ThisWorker().id_) {
    co_return absl::InvalidArgumentError(
        "replica handoff partition belongs to another worker");
  }
  co_await store.replica_apply_mutex_.Lock();
  UnlockGuard replica_unlock(&store.replica_apply_mutex_, store.worker_);
  co_await store.store_state_mutex_.Lock();
  UnlockGuard write_unlock(&store.store_state_mutex_, store.worker_);
  auto& partition = PartitionFor(store, partition_id);
  auto* sync = partition.replica_sync_.get();
  if (sync == nullptr || sync->session_id_ != session_id ||
      sync->replication_epoch_ != replication_epoch) {
    co_return absl::FailedPreconditionError("stale replica partition handoff");
  }
  if (partition.replica_value_stage_.has_value()) {
    co_return absl::FailedPreconditionError(
        "replica partition handoff interrupted a large value");
  }
  sync->tailing_ = true;
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::BeginReplicaTailCommand(
    std::uint64_t session_id, std::uint16_t partition_id,
    std::uint64_t partition_sequence) {
  if (partition_sequence == 0 || partition_id >= kLogicalStorageShards ||
      partition_id % worker_count_ != celer::ThisWorker().id_) {
    co_return absl::InvalidArgumentError(
        "invalid replica tail command identity");
  }
  WorkerStore& store = CurrentStore();
  co_await store.store_state_mutex_.Lock();
  UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
  auto& partition = PartitionFor(store, partition_id);
  auto* sync = partition.replica_sync_.get();
  if (sync == nullptr || sync->session_id_ != session_id ||
      sync->command_sequence_.has_value()) {
    co_return absl::FailedPreconditionError(
        "replica command is outside its apply window");
  }
  sync->command_sequence_ = partition_sequence;
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::EndReplicaTailCommand(
    std::uint64_t session_id, std::uint16_t partition_id,
    std::uint64_t partition_sequence) {
  WorkerStore& store = CurrentStore();
  co_await store.store_state_mutex_.Lock();
  UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
  auto& partition = PartitionFor(store, partition_id);
  auto* sync = partition.replica_sync_.get();
  if (sync == nullptr || sync->session_id_ != session_id ||
      sync->command_sequence_ != partition_sequence) {
    co_return absl::FailedPreconditionError(
        "replica tail command context changed during apply");
  }
  sync->command_sequence_.reset();
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::ApplyReplicaRecords(
    std::uint64_t session_id, std::uint16_t partition_id,
    std::uint64_t replication_epoch, std::span<const SnapshotRecord> records) {
  WorkerStore& store = CurrentStore();
  co_await store.replica_apply_mutex_.Lock();
  UnlockGuard replica_unlock(&store.replica_apply_mutex_, store.worker_);
  auto& partition = PartitionFor(store, partition_id);
  auto* sync = partition.replica_sync_.get();
  if (sync == nullptr || sync->session_id_ != session_id ||
      replication_epoch != sync->replication_epoch_) {
    co_return absl::Status(absl::StatusCode::kFailedPrecondition,
                           "stale partition replication epoch");
  }
  for (const SnapshotRecord& record : records) {
    std::optional<SnapshotRecord> materialized;
    const SnapshotRecord* effective = &record;
    if (record.db_id_ >= kLogicalDatabaseCount ||
        RedisSlot(record.key_) != partition_id) {
      co_return absl::Status(absl::StatusCode::kInvalidArgument,
                             "replica record belongs to another partition");
    }
    if (record.db_epoch_ != sync->source_db_epochs_[record.db_id_]) {
      co_return absl::Status(absl::StatusCode::kFailedPrecondition,
                             "replica record database epoch changed");
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
          record.logical_size_ == 0 || record.logical_size_ > kMaxBitmapBytes ||
          record.chunk_count_ == 0 ||
          record.chunk_count_ !=
              (record.logical_size_ + kReplicationTransferBytes - 1) /
                  kReplicationTransferBytes) {
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
          record.value_.size() > kReplicationTransferBytes ||
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
    co_await store.store_state_mutex_.Lock();
    UnlockGuard write_unlock(&store.store_state_mutex_, store.worker_);
    sync = partition.replica_sync_.get();
    if (sync == nullptr || sync->session_id_ != session_id ||
        replication_epoch != sync->replication_epoch_) [[unlikely]] {
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
        value_type == ValueType::kSet || value_type == ValueType::kSortedSet;
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
    const ExplicitWriteRoot write_root{
        .index_ = &index,
        .live_key_count_ = &partition.live_key_count_[applied.db_id_],
        .store_live_key_count_ = &store.live_key_count_[applied.db_id_],
        .expiring_key_count_ = &partition.expiring_key_count_[applied.db_id_],
        .replication_epoch_ = sync->replication_epoch_,
        .db_epoch_ = sync->local_db_epochs_[applied.db_id_],
    };
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
          false, true, true, key_external, applied.logical_size_, *extents,
          nullptr, nullptr, nullptr, nullptr, nullptr, &write_root);
      if (!written.ok()) {
        SpawnExtentReclaim(store, *extents);
      }
    } else {
      written = co_await WriteRecordLocked(
          store, applied.db_id_, applied.key_, applied.value_, kind, value_type,
          kind == RecordKind::kValue ? applied.expire_at_ms_ : 0, digest,
          /*txid=*/0, applied.mutation_sequence_, false, true, false,
          key_external, applied.logical_size_, nullptr, nullptr, nullptr,
          nullptr, nullptr, nullptr, &write_root);
    }
    if (!written.ok()) {
      co_return written;
    }
    partition.mutation_sequence_ =
        std::max(partition.mutation_sequence_, applied.mutation_sequence_);
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::DrainReplicaRootWritesLocal(
    WorkerStore& store) {
  co_await store.replica_apply_mutex_.Lock();
  UnlockGuard replica_unlock(&store.replica_apply_mutex_, store.worker_);
  {
    co_await store.store_state_mutex_.Lock();
    UnlockGuard write_unlock(&store.store_state_mutex_, store.worker_);
    FlushActiveBlock(store);
  }
  while (true) {
    bool done = false;
    bool failed = false;
    {
      co_await store.store_state_mutex_.Lock();
      UnlockGuard write_unlock(&store.store_state_mutex_, store.worker_);
      done = !store.flush_running_ && store.flush_queue_.empty();
      failed = store.write_failed_;
    }
    if (failed) {
      co_return absl::InternalError(
          "storage write failed while draining replica root");
    }
    if (done) co_return absl::OkStatus();
    absl::Status waited =
        co_await celer::SleepFor(*store.worker_, std::chrono::milliseconds(1));
    if (!waited.ok()) co_return waited;
  }
}

Task<absl::Status> StorageEngine::Impl::PromoteReplicaRoot(
    std::uint64_t session_id) {
  if (session_id == 0) {
    co_return absl::InvalidArgumentError("invalid replica root session");
  }
  if (celer::ThisWorker().id_ != 0) {
    co_return co_await celer::SubmitTaskTo(
        0, [this, session_id]() { return PromoteReplicaRoot(session_id); });
  }

  const auto& first_partition = PartitionFor(*stores_[0], 0);
  if (first_partition.replica_sync_ == nullptr ||
      first_partition.replica_sync_->session_id_ != session_id) {
    co_return absl::FailedPreconditionError("replica sync state is empty");
  }
  const std::array<std::uint64_t, kLogicalDatabaseCount> local_db_epochs =
      first_partition.replica_sync_->local_db_epochs_;
  const std::array<std::uint64_t, kLogicalDatabaseCount> source_db_epochs =
      first_partition.replica_sync_->source_db_epochs_;
  for (unsigned target = 0; target < worker_count_; ++target) {
    auto validate = [this, target, session_id, local_db_epochs,
                     source_db_epochs]() -> absl::Status {
      WorkerStore& store = *stores_[target];
      for (const auto& partition : store.partitions_) {
        if (partition.replica_sync_ == nullptr ||
            partition.replica_sync_->session_id_ != session_id ||
            !partition.replica_sync_->tailing_ ||
            partition.replica_value_stage_.has_value()) {
          return absl::FailedPreconditionError(
              "replica synchronization is incomplete");
        }
        if (local_db_epochs != partition.replica_sync_->local_db_epochs_ ||
            source_db_epochs != partition.replica_sync_->source_db_epochs_) {
          return absl::FailedPreconditionError(
              "replica synchronization database epochs disagree");
        }
      }
      return absl::OkStatus();
    };
    absl::Status valid =
        target == 0 ? validate() : co_await celer::SubmitTo(target, validate);
    if (!valid.ok()) co_return valid;
  }
  for (unsigned target = 0; target < worker_count_; ++target) {
    auto drain = [this, target]() {
      return DrainReplicaRootWritesLocal(*stores_[target]);
    };
    absl::Status drained = target == 0
                               ? co_await drain()
                               : co_await celer::SubmitTaskTo(target, drain);
    if (!drained.ok()) co_return drained;
  }

  std::vector<std::pair<std::size_t, std::uint64_t>> epoch_updates;
  epoch_updates.reserve(kLogicalDatabaseCount);
  for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
    epoch_updates.emplace_back(db_id, local_db_epochs[db_id]);
  }
  absl::Status persisted = co_await PersistEpochValues(epoch_updates);
  if (!persisted.ok()) co_return persisted;
  for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
    db_epochs_[db_id].store(local_db_epochs[db_id], std::memory_order_release);
    replica_source_db_epochs_[db_id].store(source_db_epochs[db_id],
                                           std::memory_order_release);
  }

  for (unsigned target = 0; target < worker_count_; ++target) {
    auto publish = [this, target, session_id]() -> Task<absl::Status> {
      WorkerStore& store = *stores_[target];
      co_await store.replica_apply_mutex_.Lock();
      UnlockGuard replica_unlock(&store.replica_apply_mutex_, store.worker_);
      co_await store.store_state_mutex_.Lock();
      UnlockGuard write_unlock(&store.store_state_mutex_, store.worker_);
      for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
        ++store.index_generations_[db_id];
        tx::CurrentTxShard().MarkAllWatched(db_id);
      }
      for (auto& partition : store.partitions_) {
        auto* sync = partition.replica_sync_.get();
        if (sync == nullptr || sync->session_id_ != session_id ||
            !sync->tailing_) {
          co_return absl::FailedPreconditionError(
              "replica synchronization changed during promotion");
        }
        partition.replica_value_stage_.reset();
        partition.replica_sync_.reset();
      }
      co_return absl::OkStatus();
    };
    absl::Status published =
        target == 0 ? co_await publish()
                    : co_await celer::SubmitTaskTo(target, publish);
    if (!published.ok()) co_return published;
  }
  replica_loading_.store(false, std::memory_order_release);
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::AbortReplicaRoot(
    std::uint64_t session_id) {
  if (session_id == 0) co_return absl::OkStatus();
  if (celer::ThisWorker().id_ != 0) {
    co_return co_await celer::SubmitTaskTo(
        0, [this, session_id]() { return AbortReplicaRoot(session_id); });
  }
  for (unsigned target = 0; target < worker_count_; ++target) {
    auto drain = [this, target]() {
      return DrainReplicaRootWritesLocal(*stores_[target]);
    };
    absl::Status drained = target == 0
                               ? co_await drain()
                               : co_await celer::SubmitTaskTo(target, drain);
    if (!drained.ok()) co_return drained;
  }
  for (unsigned target = 0; target < worker_count_; ++target) {
    auto discard = [this, target, session_id]() -> Task<absl::Status> {
      WorkerStore& store = *stores_[target];
      co_await store.replica_apply_mutex_.Lock();
      UnlockGuard replica_unlock(&store.replica_apply_mutex_, store.worker_);
      co_await store.store_state_mutex_.Lock();
      UnlockGuard write_unlock(&store.store_state_mutex_, store.worker_);
      bool discarded_any = false;
      for (auto& partition : store.partitions_) {
        auto* sync = partition.replica_sync_.get();
        if (sync == nullptr || sync->session_id_ != session_id) continue;
        discarded_any = true;
        for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
          if (partition.indexes_[db_id].has_allocated_storage()) {
            store.detached_indexes_.push_back(DetachedIndex{
                .index_ = partition.indexes_[db_id].Detach(),
                .db_id_ = db_id,
            });
          }
          if (store.live_key_count_[db_id] < partition.live_key_count_[db_id])
              [[unlikely]] {
            co_return absl::InternalError(
                "replica abort found inconsistent live-key accounting");
          }
          store.live_key_count_[db_id] -= partition.live_key_count_[db_id];
          partition.live_key_count_[db_id] = 0;
          partition.expiring_key_count_[db_id] = 0;
        }
        partition.mutation_sequence_ = 0;
        partition.replica_value_stage_.reset();
        partition.replica_sync_.reset();
      }
      if (discarded_any) {
        for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
          ++store.index_generations_[db_id];
          tx::CurrentTxShard().MarkAllWatched(db_id);
        }
      }
      EnsureDetachedReclaim(store);
      co_return absl::OkStatus();
    };
    absl::Status discarded =
        target == 0 ? co_await discard()
                    : co_await celer::SubmitTaskTo(target, discard);
    if (!discarded.ok()) co_return discarded;
  }
  co_return absl::OkStatus();
}

}  // namespace keylane::storage
