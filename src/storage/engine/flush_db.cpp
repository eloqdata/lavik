#include "impl.h"

namespace keylane::storage {

Task<absl::Status> StorageEngine::Impl::FlushDbDetach(std::uint8_t db_id) {
  assert(db_id < kLogicalDatabaseCount);
  if (celer::ThisWorker().id_ != 0) {
    co_return co_await celer::SubmitTaskTo(
        0, [this, db_id]() -> Task<absl::Status> {
          co_return co_await FlushDbDetach(db_id);
        });
  }

  const std::uint64_t current = DbEpoch(db_id);
  if (current == std::numeric_limits<std::uint64_t>::max()) {
    co_return absl::Status(absl::StatusCode::kOutOfRange,
                           "database epoch exhausted");
  }
  co_return co_await DetachDbEpoch(db_id, current + 1);
}

Task<absl::Status> StorageEngine::Impl::DetachDbEpoch(std::uint8_t db_id,
                                                      std::uint64_t next) {
  if (celer::ThisWorker().id_ != 0) {
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
      co_await store.store_state_mutex_.Lock();
      UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
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
  if (celer::ThisWorker().id_ != 0) {
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

void StorageEngine::Impl::DetachDbLocal(WorkerStore& store,
                                        std::uint8_t db_id) {
  // FLUSHDB invalidates every watcher of this database, including watches
  // on keys that never existed (Redis semantics).
  tx::CurrentTxShard().MarkAllWatched(db_id);
  ++store.index_generations_[db_id];
  for (auto& partition : store.partitions_) {
    auto& index = partition.indexes_[db_id];
    if (index.has_allocated_storage()) {
      store.detached_indexes_.push_back(DetachedIndex{
          .index_ = index.Detach(),
          .db_id_ = db_id,
      });
    }
    partition.live_key_count_[db_id] = 0;
    partition.expiring_key_count_[db_id] = 0;
    const std::uint64_t sequence = ++partition.mutation_sequence_;
    if (partition.capture_deltas_) {
      AppendDelta(partition, SnapshotRecord{
                                 .kind_ = SnapshotRecord::Kind::kFlushDb,
                                 .db_id_ = db_id,
                                 .db_epoch_ = DbEpoch(db_id),
                                 .mutation_sequence_ = sequence,
                                 .key_ = {},
                                 .value_ = {},
                             });
    }
  }
  store.live_key_count_[db_id] = 0;
}

Task<absl::Status> StorageEngine::Impl::ReclaimDetachedIndexes(
    WorkerStore& store) {
  while (!store.detached_indexes_.empty()) {
    DetachedIndex detached = std::move(store.detached_indexes_.front());
    store.detached_indexes_.pop_front();

    struct BlockDelta {
      std::uint64_t bytes_ = 0;
      std::uint16_t block_owner_ = 0;
      std::vector<ExtentManifest> dependent_extents_;
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
    detached.index_.ForEach([&](const RecordIndex::Entry& entry) {
      BlockDelta& delta = dead_by_block[std::pair(
          entry.value_.block_id_, entry.value_.allocation_epoch_)];
      delta.block_owner_ = entry.value_.block_owner_;
      delta.bytes_ += entry.value_.total_disk_bytes_;
      if (entry.value_.external_) {
        auto manifest = store.external_manifests_.find(&entry);
        if (manifest != store.external_manifests_.end()) {
          if (entry.value_.key_external_) [[unlikely]] {
            delta.dependent_extents_.push_back(std::move(manifest->second));
          } else {
            dead_extents.push_back(std::move(manifest->second));
          }
          store.external_manifests_.erase(manifest);
        }
      }
    });

    for (const auto& extents : dead_extents) {
      SpawnExtentReclaim(store, extents);
    }

    for (auto& [block, delta] : dead_by_block) {
      // A block holds at most kStorageBlockBytes, so the sum still fits the
      // per-record width.
      assert(delta.bytes_ <= kStorageBlockBytes);
      RetiredRecord aggregate{
          .block_id_ = block.first,
          .allocation_epoch_ = block.second,
          .total_disk_bytes_ = static_cast<std::uint32_t>(delta.bytes_),
          .block_owner_ = delta.block_owner_,
          .dependent_extents_ = nullptr,
          .extra_dependent_extents_ =
              delta.dependent_extents_.empty()
                  ? nullptr
                  : std::make_shared<const std::vector<ExtentManifest>>(
                        std::move(delta.dependent_extents_)),
      };
      absl::Status dead = co_await MarkRecordDead(aggregate);
      if (!dead.ok()) {
        store.write_failed_ = true;
        co_return dead;
      }
    }

    // Freeing the entries is the expensive part of this loop, and it happens
    // as `detached` goes out of scope. Yield so online work is polled between
    // populations.
    co_await celer::Yield(*store.worker_);
  }

  co_await store.store_state_mutex_.Lock();
  UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
  SealDeadActiveBlock(store);
  co_return absl::OkStatus();
}

void StorageEngine::Impl::EnsureDetachedReclaim(WorkerStore& store) {
  if (store.detached_reclaim_running_ || store.detached_indexes_.empty()) {
    return;
  }
  store.detached_reclaim_running_ = true;
  active_settlements_.fetch_add(1, std::memory_order_acq_rel);
  store.worker_->SpawnBackground(RunDetachedReclaim(&store));
}

Task<absl::Status> StorageEngine::Impl::RunDetachedReclaim(WorkerStore* store) {
  struct SettlementGuard {
    std::atomic<std::uint32_t>* active_;
    ~SettlementGuard() { active_->fetch_sub(1, std::memory_order_acq_rel); }
  } settlement{&active_settlements_};
  absl::Status status = co_await ReclaimDetachedIndexes(*store);
  store->detached_reclaim_running_ = false;
  if (!status.ok()) {
    spdlog::error("worker[{}] detached index reclaim failed: {}",
                  store->worker_->id(), status.message());
  }
  co_return status;
}

Task<absl::Status> StorageEngine::Impl::AwaitDetachedReclaim(
    WorkerStore& store) {
  EnsureDetachedReclaim(store);
  while (store.detached_reclaim_running_ || !store.detached_indexes_.empty()) {
    // A reclaimer that died mid-stream never empties the queue and nothing
    // restarts it; fall through to the fail-stop report instead of
    // spinning forever.
    if (!store.detached_reclaim_running_ && store.write_failed_) {
      break;
    }
    absl::Status waited =
        co_await celer::SleepFor(*store.worker_, std::chrono::milliseconds(1));
    if (!waited.ok()) {
      co_return waited;
    }
  }
  if (store.write_failed_) {
    co_return absl::Status(
        absl::StatusCode::kInternal,
        "storage writer stopped while reclaiming flushed keys");
  }
  co_return absl::OkStatus();
}

}  // namespace keylane::storage
