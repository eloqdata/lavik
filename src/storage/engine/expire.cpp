#include "impl.h"

namespace keylane::storage {

Task<absl::Status> StorageEngine::Impl::QuiesceExpiration() {
  expiration_pause_count_.fetch_add(1, std::memory_order_acq_rel);
  // Drain the in-flight expiration cycle on every worker. The flag spans a
  // whole cycle, so once it drops every tombstone of that cycle has landed —
  // no matter where the cycle suspended along the way. A cycle raises the
  // flag before it checks the pause count, so it either sees the increment
  // above and abstains, or is seen here and waited out.
  for (unsigned target = 0; target < worker_count_; ++target) {
    absl::Status drained = co_await celer::SubmitTaskTo(
        target, [this, target]() -> Task<absl::Status> {
          WorkerStore& store = *stores_[target];
          while (store.expiry_cycle_running_) {
            absl::Status waited = co_await celer::SleepFor(
                *store.worker_, std::chrono::milliseconds(1));
            if (!waited.ok()) {
              co_return waited;
            }
          }
          co_return absl::OkStatus();
        });
    if (!drained.ok()) {
      ResumeExpiration();
      co_return drained;
    }
  }
  co_return absl::OkStatus();
}

void StorageEngine::Impl::QueueExpiredCandidate(WorkerStore& store,
                                                std::uint16_t partition_id,
                                                std::uint8_t db_id,
                                                const RecordIndex::Entry& entry,
                                                std::string_view known_key) {
  constexpr std::size_t kMaxQueuedExpiredCandidates = 4096;
  if (!expiration_authority_.load(std::memory_order_acquire) ||
      store.expired_candidates_.size() >= kMaxQueuedExpiredCandidates ||
      entry.value_.kind() != RecordKind::kValue || ExpireAt(entry) == 0) {
    return;
  }
  const std::string_view key = entry.key_complete() ? entry.key() : known_key;
  if (key.empty() && entry.logical_key_size() != 0) {
    return;
  }
  store.expired_candidates_.push_back(WorkerStore::ExpireCandidate{
      .partition_id_ = partition_id,
      .db_id_ = db_id,
      .digest_ = entry.key_complete() ? ComputeDigest(key)
                                      : entry.external_key_digest(),
      .mutation_sequence_ = entry.value_.mutation_sequence_,
      .expire_at_ms_ = ExpireAt(entry),
      .key_ = std::string(key),
  });
}

void StorageEngine::Impl::AdvanceExpiryMap(WorkerStore& store) {
  store.expiry_scan_cursor_ = 0;
  ++store.expiry_db_cursor_;
  if (store.expiry_db_cursor_ == kLogicalDatabaseCount) {
    store.expiry_db_cursor_ = 0;
    ++store.expiry_partition_cursor_;
    if (store.expiry_partition_cursor_ == store.partitions_.size()) {
      store.expiry_partition_cursor_ = 0;
    }
  }
}

Task<absl::Status> StorageEngine::Impl::ExpireCandidate(
    WorkerStore& store, WorkerStore::ExpireCandidate candidate) {
  if (candidate.partition_id_ >= kLogicalStorageShards ||
      candidate.db_id_ >= kLogicalDatabaseCount ||
      expiration_pause_count_.load(std::memory_order_acquire) != 0) {
    co_return absl::OkStatus();
  }
  auto& partition = PartitionFor(store, candidate.partition_id_);
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      candidate.db_id_, tx::FingerprintOf(candidate.digest_),
      tx::LockMode::kExclusive);
  co_await store.store_state_mutex_.Lock();
  UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
  auto resolved =
      co_await FindVerifiedEntry(store, partition.indexes_[candidate.db_id_],
                                 candidate.digest_, candidate.key_);
  if (!resolved.ok()) {
    co_return resolved.status();
  }
  auto* current = *resolved;
  const auto matches_candidate = [&candidate](const RecordIndex::Entry* entry) {
    return entry != nullptr && entry->value_.kind() == RecordKind::kValue &&
           entry->value_.mutation_sequence_ == candidate.mutation_sequence_ &&
           ExpireAt(*entry) == candidate.expire_at_ms_ && IsExpiredNow(*entry);
  };
  if (!matches_candidate(current)) {
    co_return absl::OkStatus();
  }
  // Prefer a durable delete so a later wall-clock rollback cannot expose the
  // expired value again. If the device has no foreground space left, an
  // unshielded record is nevertheless safe to retire in memory: there is no
  // older live version for this record to hide, and recovery still observes
  // its own expiration timestamp. This is the full-disk escape valve that
  // lets expiration free blocks which can then accept durable tombstones.
  absl::Status durable = co_await AppendLocked(
      store, partition, candidate.db_id_, candidate.key_, candidate.digest_, {},
      RecordKind::kTombstone, ValueType::kNone, 0, nullptr, 0);
  if (durable.ok() || durable.code() != absl::StatusCode::kResourceExhausted) {
    co_return durable;
  }

  // Append may release store_state_mutex_ while acquiring space or pinning
  // transaction dependencies. The key hold excludes logical writes, but GC
  // may replace the index entry and the group's physical coordinates.
  resolved =
      co_await FindVerifiedEntry(store, partition.indexes_[candidate.db_id_],
                                 candidate.digest_, candidate.key_);
  if (!resolved.ok()) co_return resolved.status();
  current = *resolved;
  if (!matches_candidate(current)) co_return absl::OkStatus();
  if (current->value_.shielding()) co_return durable;

  const RecordLocation dropped = MaterializeIndexLocation(*current);
  const ExtentManifest dropped_extents = ExtentsFor(store, current);
  const ExtentManifest dropped_dependent_extents =
      DependentExtentsFor(store, current);
  GroupedHashObject::Handle grouped;
  std::vector<RetiredRecord> grouped_retirements;
  if (dropped.grouped()) {
    try {
      auto view = partition.grouped_objects_[candidate.db_id_].Lookup(
          candidate.key_,
          GroupedObjectVersion{
              .root_ = dropped,
              .db_epoch_ = EffectiveRecordDbEpoch(partition, candidate.db_id_),
              .replication_epoch_ = partition.replication_epoch_,
              .index_generation_ =
                  partition.grouped_generations_[candidate.db_id_]});
      if (!view.ok()) co_return view.status();
      if (*view == nullptr)
        co_return absl::DataLossError("missing expired grouped view");
      grouped = std::move(*view);
      // Prepare the complete graph before detaching either index, without
      // allocating disk space or decoding values. This includes split-parent
      // retirement records and preserves external parent-key extent debt.
      // Admission failure leaves the expired key indexed for a later retry.
      auto retired = CollectGroupedRetirements(grouped, nullptr);
      if (!retired.ok()) co_return retired.status();
      grouped_retirements = std::move(*retired);
    } catch (const std::bad_alloc&) {
      co_return absl::ResourceExhaustedError(
          "OOM preparing expired grouped retirements");
    }
  }

  tx::CurrentTxShard().MarkWatched(candidate.db_id_,
                                   tx::FingerprintOf(candidate.digest_));
  const std::uint64_t sequence = ++partition.mutation_sequence_;
  if (!partition.fullsync_subscribers_.empty()) [[unlikely]] {
    FullSyncOnCommit(store, partition,
                     SnapshotRecord{
                         .kind_ = SnapshotRecord::Kind::kDelete,
                         .db_id_ = candidate.db_id_,
                         .db_epoch_ = DbEpoch(candidate.db_id_),
                         .mutation_sequence_ = sequence,
                         .expire_at_ms_ = 0,
                         .value_type_ = ValueType::kNone,
                         .key_ = candidate.key_,
                         .value_ = {},
                     },
                     candidate.digest_);
  }
  // No suspension separates physical capture, side-view removal and root
  // removal. Once detached, GC cannot relocate these records; their live-byte
  // charges keep their blocks allocated until settlement on each owner.
  if (grouped != nullptr) {
    const auto removed = partition.grouped_objects_[candidate.db_id_].Erase(
        candidate.key_, grouped);
    if (!removed.ok()) {
      store.write_failed_ = true;
      co_return removed;
    }
  }
  --partition.live_key_count_[candidate.db_id_];
  --store.live_key_count_[candidate.db_id_];
  --partition.expiring_key_count_[candidate.db_id_];
  store.external_manifests_.erase(current);
  const std::size_t logical_key_bytes = current->logical_key_size();
  const bool erased = partition.indexes_[candidate.db_id_].Erase(current);
  assert(erased);
  if (erased) {
    RemoveFullSyncCoverageEntry(partition, candidate.db_id_, logical_key_bytes);
  }
  if (dropped.external() && !dropped.key_external()) {
    SpawnExtentReclaim(store, dropped_extents);
  }
  absl::Status dead = co_await MarkRecordDead(
      RetiredRecordOf(dropped, dropped_dependent_extents));
  for (const auto& record : grouped_retirements) {
    if (!dead.ok()) break;
    dead = co_await MarkRecordDead(record);
  }
  if (!dead.ok()) store.write_failed_ = true;
  co_return dead;
}

Task<absl::Status> StorageEngine::Impl::ActiveExpiration(WorkerStore* store) {
  constexpr auto kInterval = std::chrono::milliseconds(10);
  constexpr std::size_t kMapStepsPerCycle = 256;
  constexpr std::size_t kDeletesPerCycle = 64;
  while (!store->worker_->stop_requested()) {
    absl::Status waited = co_await celer::SleepFor(*store->worker_, kInterval);
    if (!waited.ok()) {
      co_return waited;
    }
    // Raise the flag before checking the pause count — with no suspension
    // between the two, a cycle QuiesceExpiration's increment misses is
    // already visible to its drain. The guard drops the flag on every exit
    // from the cycle: abstain, shutdown, delete error, or completion.
    store->expiry_cycle_running_ = true;
    struct CycleGuard {
      bool* running_;
      ~CycleGuard() { *running_ = false; }
    } cycle_guard{&store->expiry_cycle_running_};
#if KEYLANE_FAULTS_ENABLED
    // Deterministic coverage for lazy-expiration replacement. Production
    // builds without explicit test faults never expose an expiration switch.
    if (std::getenv("KEYLANE_DISABLE_ACTIVE_EXPIRATION") != nullptr) continue;
#endif
    if (expiration_pause_count_.load(std::memory_order_acquire) != 0) {
      continue;  // a stable-keyspace scan (KEYS) is in flight
    }
    if (!expiration_authority_.load(std::memory_order_acquire)) continue;
    if (store->worker_->stop_requested() ||
        shutdown_flush_requested_.load(std::memory_order_acquire)) {
      break;
    }

    const std::uint64_t now_ms = UnixTimeMillis();
    for (std::size_t step = 0;
         step < kMapStepsPerCycle && !store->partitions_.empty(); ++step) {
      auto& partition = store->partitions_[store->expiry_partition_cursor_];
      const std::uint8_t db_id = store->expiry_db_cursor_;
      if (partition.expiring_key_count_[db_id] == 0) {
        AdvanceExpiryMap(*store);
      } else {
        auto& index = partition.indexes_[db_id];
        struct ExternalExpired {
          std::uintptr_t entry_address_ = 0;
          ExtentManifest extents_;
          RecordLocation location_{};
          std::uint32_t hash_ = 0;
          std::uint32_t key_bytes_ = 0;
        };
        std::vector<ExternalExpired> external_expired;
        store->expiry_scan_cursor_ = index.Scan(
            store->expiry_scan_cursor_, [&](RecordIndex::Entry& entry) {
              if (IsExpired(entry, now_ms)) {
                if (entry.key_complete()) [[likely]] {
                  QueueExpiredCandidate(*store, partition.id_, db_id, entry);
                } else {
                  external_expired.push_back(ExternalExpired{
                      .entry_address_ =
                          reinterpret_cast<std::uintptr_t>(&entry),
                      .extents_ = ExtentsFor(*store, &entry),
                      .location_ = MaterializeIndexLocation(entry),
                      .hash_ =
                          RecordIndex::AddressHash(entry.external_key_digest()),
                      .key_bytes_ = entry.logical_key_size(),
                  });
                }
              }
            });
        for (const ExternalExpired& candidate : external_expired) {
          auto key = co_await LoadOutOfIndexKey(*store, candidate.location_,
                                                candidate.extents_,
                                                candidate.key_bytes_);
          if (!key.ok()) {
            co_return key.status();
          }
          RecordIndex::Entry* current =
              index.FindAddress(candidate.entry_address_, candidate.hash_);
          if (current == nullptr) continue;
          if (MaterializeIndexLocation(*current).SamePhysicalRecord(
                  candidate.location_) &&
              IsExpired(*current, now_ms)) {
            QueueExpiredCandidate(*store, partition.id_, db_id, *current, *key);
          }
        }
        if (store->expiry_scan_cursor_ == 0) {
          AdvanceExpiryMap(*store);
        }
      }
      co_await celer::Yield(*store->worker_);
    }

    std::size_t deleted = 0;
    bool warned_failure = false;
    while (deleted < kDeletesPerCycle && !store->expired_candidates_.empty()) {
      const WorkerStore::ExpireCandidate& front =
          store->expired_candidates_.front();
      std::size_t replacement_bytes = kFullSyncReplacementMetadataBytes;
      if (front.key_.size() >
          (std::numeric_limits<std::size_t>::max() - replacement_bytes) / 2) {
        co_return absl::ResourceExhaustedError(
            "active-expiration replacement identity is too large");
      }
      replacement_bytes += front.key_.size() * 2;
      auto admission = TryAcquireFullSyncReplacementAdmission(
          replacement_bytes,
          ReplicationPublisherTarget{.partition_id_ = front.partition_id_,
                                     .db_id_ = front.db_id_});
      if (!admission.has_value()) {
        // Expiration is maintenance and expired records are already logically
        // invisible. Leave the candidate queued and retry next cycle rather
        // than waiting behind a slow full-sync replica or foreground writer.
        break;
      }
      WorkerStore::ExpireCandidate candidate =
          std::move(store->expired_candidates_.front());
      store->expired_candidates_.pop_front();
      absl::Status expired =
          co_await ExpireCandidate(*store, std::move(candidate));
      ReleaseReplicationPublisherAdmission(*admission, replacement_bytes);
      if (!expired.ok()) {
        // Do not terminate this worker's lifetime expiration coroutine. The
        // key remains indexed as expired and a later map pass will enqueue it
        // again, while the warning keeps persistent write failures visible.
        if (!warned_failure) {
          spdlog::warn("worker[{}] active expiration failed: {}",
                       store->worker_->id(), expired.ToString());
          warned_failure = true;
        }
        ++deleted;
        co_await celer::Yield(*store->worker_);
        continue;
      }
      ++deleted;
      co_await celer::Yield(*store->worker_);
    }
  }
  co_return absl::OkStatus();
}

}  // namespace keylane::storage
