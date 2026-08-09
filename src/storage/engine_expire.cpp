#include "engine_impl.h"

namespace keylane::storage {

Task<Status> StorageEngine::Impl::QuiesceExpiration() {
  expiration_pause_count_.fetch_add(1, std::memory_order_acq_rel);
  // Drain the in-flight expiration cycle on every worker. The flag spans a
  // whole cycle, so once it drops every tombstone of that cycle has landed —
  // no matter where the cycle suspended along the way. A cycle raises the
  // flag before it checks the pause count, so it either sees the increment
  // above and abstains, or is seen here and waited out.
  for (unsigned target = 0; target < worker_count_; ++target) {
    Status drained = co_await celer::SubmitTaskTo(
        target, [this, target]() -> Task<Status> {
          WorkerStore& store = *stores_[target];
          while (store.expiry_cycle_running) {
            Status waited = co_await celer::SleepFor(
                *store.worker, std::chrono::milliseconds(1));
            if (!waited.ok()) {
              co_return waited;
            }
          }
          co_return Status::Ok();
        });
    if (!drained.ok()) {
      ResumeExpiration();
      co_return drained;
    }
  }
  co_return Status::Ok();
}

void StorageEngine::Impl::QueueExpiredCandidate(WorkerStore& store,
                                                std::uint16_t partition_id,
                                                std::uint8_t db_id,
                                                const RecordIndex::Entry& entry) {
  constexpr std::size_t kMaxQueuedExpiredCandidates = 4096;
  if (!options_.expiration_authority ||
      store.expired_candidates.size() >= kMaxQueuedExpiredCandidates ||
      entry.value.kind != RecordKind::kValue ||
      entry.value.expire_at_ms == 0) {
    return;
  }
  store.expired_candidates.push_back(WorkerStore::ExpireCandidate{
      .partition_id = partition_id,
      .db_id = db_id,
      .digest = entry.digest,
      .mutation_sequence = entry.value.mutation_sequence,
      .expire_at_ms = entry.value.expire_at_ms,
      .key = entry.key,
  });
}

void StorageEngine::Impl::AdvanceExpiryMap(WorkerStore& store) {
  store.expiry_scan_cursor = 0;
  ++store.expiry_db_cursor;
  if (store.expiry_db_cursor == kLogicalDatabaseCount) {
    store.expiry_db_cursor = 0;
    ++store.expiry_partition_cursor;
    if (store.expiry_partition_cursor == store.partitions.size()) {
      store.expiry_partition_cursor = 0;
    }
  }
}

Task<Status> StorageEngine::Impl::ExpireCandidate(
    WorkerStore& store, WorkerStore::ExpireCandidate candidate) {
  if (candidate.partition_id >= kLogicalStorageShards ||
      candidate.db_id >= kLogicalDatabaseCount ||
      expiration_pause_count_.load(std::memory_order_acquire) != 0) {
    co_return Status::Ok();
  }
  auto& partition = PartitionFor(store, candidate.partition_id);
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      candidate.db_id, tx::FingerprintOf(candidate.digest),
      tx::LockMode::kExclusive);
  co_await store.writer_mutex.Lock();
  UnlockGuard unlock(&store.writer_mutex, store.worker);
  auto* current = partition.indexes[candidate.db_id].Find(
      candidate.digest, candidate.key);
  if (current == nullptr || current->value.kind != RecordKind::kValue ||
      current->value.mutation_sequence != candidate.mutation_sequence ||
      current->value.expire_at_ms != candidate.expire_at_ms ||
      !IsExpired(current->value, UnixTimeMillis())) {
    co_return Status::Ok();
  }
  if (current->value.shielding) {
    // An older, still-unexpired value of this key may survive on disk;
    // without a durable tombstone above it, recovery would resurrect it
    // once this record's block is reclaimed. Keep the tombstone path for
    // exactly this case.
    co_return co_await AppendLocked(
        store, partition, candidate.db_id, candidate.key, {},
        RecordKind::kTombstone, ValueType::kNone, 0);
  }
  // Memory-only expiration. Every older on-disk version of this key is
  // expired or gone, and the record carries its own expire_at_ms, so
  // recovery and replicas already treat it as absent — nothing needs to be
  // written. Mirror everything the tombstone append would have done:
  // invalidate watchers, ship a delete to any capturing replica stream,
  // settle the block accounting, and drop the entry itself.
  tx::CurrentTxShard().MarkWatched(candidate.db_id,
                                   tx::FingerprintOf(candidate.digest));
  const RecordLocation dropped = current->value;
  const std::uint64_t sequence = ++partition.mutation_sequence;
  if (partition.capture_deltas) {
    AppendDelta(partition, SnapshotRecord{
                               .kind = SnapshotRecord::Kind::kDelete,
                               .db_id = candidate.db_id,
                               .db_epoch = DbEpoch(candidate.db_id),
                               .mutation_sequence = sequence,
                               .expire_at_ms = 0,
                               .value_type = ValueType::kNone,
                               .key = candidate.key,
                               .value = {},
                           });
  }
  // The flush completion dereferences staged entries by pointer before it
  // can match them; detach every identity naming this one before it is
  // freed. Their retirements still settle — only the marking becomes moot.
  for (auto& [block_id, identities] : store.staged_records) {
    for (RecordIdentity& identity : identities) {
      if (identity.entry == current) {
        identity.entry = nullptr;
      }
    }
  }
  --partition.live_key_count[candidate.db_id];
  --store.live_key_count[candidate.db_id];
  --partition.expiring_key_count[candidate.db_id];
  partition.indexes[candidate.db_id].Erase(candidate.digest, candidate.key);
  if (dropped.external) {
    SpawnExtentReclaim(store, dropped.extents);
  }
  Status dead = co_await MarkRecordDead(RetiredRecordOf(dropped));
  if (!dead.ok()) {
    store.write_failed = true;
  }
  co_return dead;
}

Task<Status> StorageEngine::Impl::ActiveExpiration(WorkerStore* store) {
  constexpr auto kInterval = std::chrono::milliseconds(10);
  constexpr std::size_t kMapStepsPerCycle = 256;
  constexpr std::size_t kDeletesPerCycle = 64;
  while (!store->worker->stop_requested()) {
    Status waited = co_await celer::SleepFor(*store->worker, kInterval);
    if (!waited.ok()) {
      co_return waited;
    }
    // Raise the flag before checking the pause count — with no suspension
    // between the two, a cycle QuiesceExpiration's increment misses is
    // already visible to its drain. The guard drops the flag on every exit
    // from the cycle: abstain, shutdown, delete error, or completion.
    store->expiry_cycle_running = true;
    struct CycleGuard {
      bool* running;
      ~CycleGuard() { *running = false; }
    } cycle_guard{&store->expiry_cycle_running};
    if (expiration_pause_count_.load(std::memory_order_acquire) != 0) {
      continue;  // a stable-keyspace scan (KEYS) is in flight
    }
    if (store->worker->stop_requested() ||
        shutdown_flush_requested_.load(std::memory_order_acquire)) {
      break;
    }

    const std::uint64_t now_ms = UnixTimeMillis();
    for (std::size_t step = 0;
         step < kMapStepsPerCycle && !store->partitions.empty(); ++step) {
      auto& partition =
          store->partitions[store->expiry_partition_cursor];
      const std::uint8_t db_id = store->expiry_db_cursor;
      if (partition.expiring_key_count[db_id] == 0) {
        AdvanceExpiryMap(*store);
      } else {
        auto& index = partition.indexes[db_id];
        store->expiry_scan_cursor = index.Scan(
            store->expiry_scan_cursor,
            [&](const RecordIndex::Entry& entry) {
              if (IsExpired(entry.value, now_ms)) {
                QueueExpiredCandidate(*store, partition.id, db_id, entry);
              }
            });
        if (store->expiry_scan_cursor == 0) {
          AdvanceExpiryMap(*store);
        }
      }
      co_await celer::Yield(*store->worker);
    }

    std::size_t deleted = 0;
    while (deleted < kDeletesPerCycle &&
           !store->expired_candidates.empty()) {
      WorkerStore::ExpireCandidate candidate =
          std::move(store->expired_candidates.front());
      store->expired_candidates.pop_front();
      Status expired = co_await ExpireCandidate(*store,
                                                std::move(candidate));
      if (!expired.ok()) {
        co_return expired;
      }
      ++deleted;
      co_await celer::Yield(*store->worker);
    }
  }
  co_return Status::Ok();
}

}  // namespace keylane::storage
