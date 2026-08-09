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

void StorageEngine::Impl::QueueExpiredCandidate(
    WorkerStore& store, std::uint16_t partition_id, std::uint8_t db_id,
    const RecordIndex::Entry& entry) {
  constexpr std::size_t kMaxQueuedExpiredCandidates = 4096;
  if (!options_.expiration_authority_ ||
      store.expired_candidates_.size() >= kMaxQueuedExpiredCandidates ||
      entry.value_.kind_ != RecordKind::kValue ||
      entry.value_.expire_at_ms_ == 0) {
    return;
  }
  store.expired_candidates_.push_back(WorkerStore::ExpireCandidate{
      .partition_id_ = partition_id,
      .db_id_ = db_id,
      .digest_ = entry.digest_,
      .mutation_sequence_ = entry.value_.mutation_sequence_,
      .expire_at_ms_ = entry.value_.expire_at_ms_,
      .key_ = entry.key_,
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
  auto* current = partition.indexes_[candidate.db_id_].Find(candidate.digest_,
                                                            candidate.key_);
  if (current == nullptr || current->value_.kind_ != RecordKind::kValue ||
      current->value_.mutation_sequence_ != candidate.mutation_sequence_ ||
      current->value_.expire_at_ms_ != candidate.expire_at_ms_ ||
      !IsExpired(current->value_, UnixTimeMillis())) {
    co_return absl::OkStatus();
  }
  if (current->value_.shielding_) {
    // An older, still-unexpired value of this key may survive on disk;
    // without a durable tombstone above it, recovery would resurrect it
    // once this record's block is reclaimed. Keep the tombstone path for
    // exactly this case.
    co_return co_await AppendLocked(store, partition, candidate.db_id_,
                                    candidate.key_, {}, RecordKind::kTombstone,
                                    ValueType::kNone, 0);
  }
  // Memory-only expiration. Every older on-disk version of this key is
  // expired or gone, and the record carries its own expire_at_ms, so
  // recovery and replicas already treat it as absent — nothing needs to be
  // written. Mirror everything the tombstone append would have done:
  // invalidate watchers, ship a delete to any capturing replica stream,
  // settle the block accounting, and drop the entry itself.
  tx::CurrentTxShard().MarkWatched(candidate.db_id_,
                                   tx::FingerprintOf(candidate.digest_));
  const RecordLocation dropped = current->value_;
  const std::uint64_t sequence = ++partition.mutation_sequence_;
  if (partition.capture_deltas_) {
    AppendDelta(partition, SnapshotRecord{
                               .kind_ = SnapshotRecord::Kind::kDelete,
                               .db_id_ = candidate.db_id_,
                               .db_epoch_ = DbEpoch(candidate.db_id_),
                               .mutation_sequence_ = sequence,
                               .expire_at_ms_ = 0,
                               .value_type_ = ValueType::kNone,
                               .key_ = candidate.key_,
                               .value_ = {},
                           });
  }
  // The flush completion dereferences staged entries by pointer before it
  // can match them; detach every identity naming this one before it is
  // freed. Their retirements still settle — only the marking becomes moot.
  for (auto& [block_id, identities] : store.staged_records_) {
    for (RecordIdentity& identity : identities) {
      if (identity.entry_ == current) {
        identity.entry_ = nullptr;
      }
    }
  }
  --partition.live_key_count_[candidate.db_id_];
  --store.live_key_count_[candidate.db_id_];
  --partition.expiring_key_count_[candidate.db_id_];
  partition.indexes_[candidate.db_id_].Erase(candidate.digest_, candidate.key_);
  if (dropped.external_) {
    SpawnExtentReclaim(store, dropped.extents_);
  }
  absl::Status dead = co_await MarkRecordDead(RetiredRecordOf(dropped));
  if (!dead.ok()) {
    store.write_failed_ = true;
  }
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
    if (expiration_pause_count_.load(std::memory_order_acquire) != 0) {
      continue;  // a stable-keyspace scan (KEYS) is in flight
    }
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
        store->expiry_scan_cursor_ = index.Scan(
            store->expiry_scan_cursor_, [&](const RecordIndex::Entry& entry) {
              if (IsExpired(entry.value_, now_ms)) {
                QueueExpiredCandidate(*store, partition.id_, db_id, entry);
              }
            });
        if (store->expiry_scan_cursor_ == 0) {
          AdvanceExpiryMap(*store);
        }
      }
      co_await celer::Yield(*store->worker_);
    }

    std::size_t deleted = 0;
    while (deleted < kDeletesPerCycle && !store->expired_candidates_.empty()) {
      WorkerStore::ExpireCandidate candidate =
          std::move(store->expired_candidates_.front());
      store->expired_candidates_.pop_front();
      absl::Status expired =
          co_await ExpireCandidate(*store, std::move(candidate));
      if (!expired.ok()) {
        co_return expired;
      }
      ++deleted;
      co_await celer::Yield(*store->worker_);
    }
  }
  co_return absl::OkStatus();
}

}  // namespace keylane::storage
