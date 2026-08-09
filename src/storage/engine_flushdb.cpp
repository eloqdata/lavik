#include "engine_impl.h"

namespace keylane::storage {

Task<absl::Status> StorageEngine::Impl::FlushDbDetach(std::uint8_t db_id) {
  assert(db_id < kLogicalDatabaseCount);
  if (celer::ThisWorker().id != 0) {
    co_return co_await celer::SubmitTaskTo(
        0, [this, db_id]() -> Task<absl::Status> {
          co_return co_await FlushDbDetach(db_id);
        });
  }

  const std::uint64_t current = DbEpoch(db_id);
  if (current == std::numeric_limits<std::uint64_t>::max()) {
    co_return absl::Status(absl::StatusCode::kOutOfRange, "database epoch exhausted");
  }
  co_return co_await DetachDbEpoch(db_id, current + 1);
}

Task<absl::Status> StorageEngine::Impl::DetachDbEpoch(std::uint8_t db_id,
                                                std::uint64_t next) {
  if (celer::ThisWorker().id != 0) {
    co_return co_await celer::SubmitTaskTo(
        0, [this, db_id, next]() -> Task<absl::Status> {
          co_return co_await DetachDbEpoch(db_id, next);
        });
  }
  const std::uint64_t current = DbEpoch(db_id);
  if (next < current) {
    co_return absl::Status(absl::StatusCode::kFailedPrecondition,
                     "replica database epoch is ahead of primary");
  }
  if (next == current) {
    co_return absl::OkStatus();
  }
  absl::Status status = co_await PersistEpochValue(db_id, next);
  if (!status.ok()) {
    co_return status;
  }
  db_epochs_[db_id].store(next, std::memory_order_release);

  for (unsigned target = 0; target < worker_count_; ++target) {
    auto detach = [this, target, db_id]() -> Task<absl::Status> {
      WorkerStore& store = *stores_[target];
      co_await store.store_state_mutex.Lock();
      UnlockGuard unlock(&store.store_state_mutex, store.worker);
      DetachDbLocal(store, db_id);
      co_return absl::OkStatus();
    };
    // if/else, not ?:, to keep the two co_awaits in separate full
    // expressions (GCC coroutine frame-slot aliasing).
    absl::Status detached;
    if (target == 0) {
      detached = co_await detach();
    } else {
      detached = co_await celer::SubmitTaskTo(target, detach);
    }
    if (!detached.ok()) {
      co_return detached;
    }
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::ReclaimDetachedAllWorkers(bool wait) {
  if (celer::ThisWorker().id != 0) {
    co_return co_await celer::SubmitTaskTo(
        0, [this, wait]() -> Task<absl::Status> {
          co_return co_await ReclaimDetachedAllWorkers(wait);
        });
  }
  for (unsigned target = 0; target < worker_count_; ++target) {
    auto reclaim = [this, target, wait]() -> Task<absl::Status> {
      WorkerStore& store = *stores_[target];
      if (!wait) {
        EnsureDetachedReclaim(store);
        co_return absl::OkStatus();
      }
      co_return co_await AwaitDetachedReclaim(store);
    };
    absl::Status reclaimed;
    if (target == 0) {
      reclaimed = co_await reclaim();
    } else {
      reclaimed = co_await celer::SubmitTaskTo(target, reclaim);
    }
    if (!reclaimed.ok()) {
      co_return reclaimed;
    }
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::AdvanceDbEpoch(std::uint8_t db_id,
                                                 std::uint64_t next) {
  absl::Status detached = co_await DetachDbEpoch(db_id, next);
  if (!detached.ok()) {
    co_return detached;
  }
  co_return co_await ReclaimDetachedAllWorkers(/*wait=*/true);
}

void StorageEngine::Impl::DetachDbLocal(WorkerStore& store, std::uint8_t db_id) {
  // FLUSHDB invalidates every watcher of this database, including watches
  // on keys that never existed (Redis semantics).
  tx::CurrentTxShard().MarkAllWatched(db_id);
  ++store.index_generations[db_id];
  for (auto& partition : store.partitions) {
    store.detached_indexes.push_back(DetachedIndex{
        .index = partition.indexes[db_id].Detach(),
        .db_id = db_id,
    });
    partition.live_key_count[db_id] = 0;
    partition.expiring_key_count[db_id] = 0;
    const std::uint64_t sequence = ++partition.mutation_sequence;
    if (partition.capture_deltas) {
      AppendDelta(partition, SnapshotRecord{
                                 .kind = SnapshotRecord::Kind::kFlushDb,
                                 .db_id = db_id,
                                 .db_epoch = DbEpoch(db_id),
                                 .mutation_sequence = sequence,
                                 .key = {},
                                 .value = {},
                             });
    }
  }
  store.live_key_count[db_id] = 0;
}

Task<absl::Status> StorageEngine::Impl::ReclaimDetachedIndexes(WorkerStore& store) {
  while (!store.detached_indexes.empty()) {
    DetachedIndex detached = std::move(store.detached_indexes.front());
    store.detached_indexes.pop_front();

    struct BlockDelta {
      std::uint64_t bytes = 0;
      std::uint16_t block_owner = 0;
    };
    // Keyed by allocation epoch as well as block id: entries naming the same
    // block at different epochs are an accounting violation rather than
    // something to sum, so they stay separate and MarkRecordDeadLocal's epoch
    // check rejects the stale one instead of the total silently absorbing it.
    // Bounded by the number of blocks the population touched, not by the
    // number of records in it.
    absl::flat_hash_map<std::pair<std::uint64_t, std::uint64_t>, BlockDelta>
        dead_by_block;
    // An external value's manifest is what the block accounting above sees;
    // the extent blocks holding its payload are owned by the manifest and
    // released as a unit, so they are collected per record rather than
    // folded into the per-block totals.
    std::vector<std::shared_ptr<const std::vector<ExtentRef>>> dead_extents;
    detached.index.ForEach([&](const RecordIndex::Entry& entry) {
      BlockDelta& delta = dead_by_block[std::pair(
          entry.value.block_id, entry.value.allocation_epoch)];
      delta.block_owner = entry.value.block_owner;
      delta.bytes += entry.value.total_disk_bytes;
      if (entry.value.external && entry.value.extents != nullptr) {
        dead_extents.push_back(entry.value.extents);
      }
    });

    for (const auto& extents : dead_extents) {
      SpawnExtentReclaim(store, extents);
    }

    for (const auto& [block, delta] : dead_by_block) {
      // A block holds at most kStorageBlockBytes, so the sum still fits the
      // per-record width.
      assert(delta.bytes <= kStorageBlockBytes);
      RetiredRecord aggregate{
          .block_id = block.first,
          .allocation_epoch = block.second,
          .total_disk_bytes = static_cast<std::uint32_t>(delta.bytes),
          .block_owner = delta.block_owner,
      };
      absl::Status dead = co_await MarkRecordDead(aggregate);
      if (!dead.ok()) {
        store.write_failed = true;
        co_return dead;
      }
    }

    // Freeing the entries is the expensive part of this loop, and it happens
    // as `detached` goes out of scope. Yield so online work is polled between
    // populations.
    co_await celer::Yield(*store.worker);
  }

  co_await store.store_state_mutex.Lock();
  UnlockGuard unlock(&store.store_state_mutex, store.worker);
  SealDeadActiveBlock(store);
  co_return absl::OkStatus();
}

void StorageEngine::Impl::EnsureDetachedReclaim(WorkerStore& store) {
  if (store.detached_reclaim_running || store.detached_indexes.empty()) {
    return;
  }
  store.detached_reclaim_running = true;
  active_settlements_.fetch_add(1, std::memory_order_acq_rel);
  store.worker->SpawnBackground(RunDetachedReclaim(&store));
}

Task<absl::Status> StorageEngine::Impl::RunDetachedReclaim(WorkerStore* store) {
  struct SettlementGuard {
    std::atomic<std::uint32_t>* active;
    ~SettlementGuard() { active->fetch_sub(1, std::memory_order_acq_rel); }
  } settlement{&active_settlements_};
  absl::Status status = co_await ReclaimDetachedIndexes(*store);
  store->detached_reclaim_running = false;
  if (!status.ok()) {
    spdlog::error("worker[{}] detached index reclaim failed: {}",
                  store->worker->id(), status.message());
  }
  co_return status;
}

Task<absl::Status> StorageEngine::Impl::AwaitDetachedReclaim(WorkerStore& store) {
  EnsureDetachedReclaim(store);
  while (store.detached_reclaim_running || !store.detached_indexes.empty()) {
    // A reclaimer that died mid-stream never empties the queue and nothing
    // restarts it; fall through to the fail-stop report instead of
    // spinning forever.
    if (!store.detached_reclaim_running && store.write_failed) {
      break;
    }
    absl::Status waited = co_await celer::SleepFor(
        *store.worker, std::chrono::milliseconds(1));
    if (!waited.ok()) {
      co_return waited;
    }
  }
  if (store.write_failed) {
    co_return absl::Status(absl::StatusCode::kInternal,
                     "storage writer stopped while reclaiming flushed keys");
  }
  co_return absl::OkStatus();
}

}  // namespace keylane::storage
