/*
 * Copyright (C) 2026 EloqData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "impl.h"

namespace lavik::storage {

Task<absl::Status> StorageEngine::Impl::FlushDbDetach(std::uint8_t db_id) {
  assert(db_id < kLogicalDatabaseCount);
  if (bycorf::ThisWorker().id_ != 0) {
    co_return co_await bycorf::SubmitTaskTo(
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

Task<absl::Status> StorageEngine::Impl::FlushAllDetach() {
  if (bycorf::ThisWorker().id_ != 0) {
    co_return co_await bycorf::SubmitTaskTo(0, [this]() -> Task<absl::Status> {
      co_return co_await FlushAllDetach();
    });
  }
  std::array<std::uint64_t, kLogicalDatabaseCount> next{};
  for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
    const std::uint64_t current = DbEpoch(db_id);
    if (current == std::numeric_limits<std::uint64_t>::max()) {
      co_return absl::OutOfRangeError("database epoch exhausted");
    }
    next[db_id] = current + 1;
  }
  co_return co_await DetachDbEpochs(next);
}

Task<absl::Status> StorageEngine::Impl::ApplyReplicatedFlushDb(
    std::uint8_t db_id, std::uint64_t source_db_epoch) {
  if (db_id >= kLogicalDatabaseCount || source_db_epoch == 0) {
    co_return absl::InvalidArgumentError("invalid replicated database epoch");
  }
  if (bycorf::ThisWorker().id_ != 0) {
    co_return co_await bycorf::SubmitTaskTo(
        0, [this, db_id, source_db_epoch]() {
          return ApplyReplicatedFlushDb(db_id, source_db_epoch);
        });
  }
  AsyncMutex& mutex = replica_db_epoch_mutexes_[db_id];
  co_await mutex.Lock();
  UnlockGuard unlock(&mutex, bycorf::ThisWorker().self_);
  const std::uint64_t installed_source =
      replica_source_db_epochs_[db_id].load(std::memory_order_acquire);
  // Before a foreign root is promoted there is no source/local translation.
  // Keep the original identity semantics used by direct replay and recovery.
  if (installed_source == 0) {
    co_return co_await AdvanceDbEpoch(db_id, source_db_epoch);
  }
  if (source_db_epoch < installed_source) {
    co_return absl::FailedPreconditionError(
        "replicated database epoch is outside the installed source root");
  }
  if (source_db_epoch == installed_source) {
    co_return absl::OkStatus();
  }
  const std::uint64_t delta = source_db_epoch - installed_source;
  const std::uint64_t local = DbEpoch(db_id);
  if (delta > std::numeric_limits<std::uint64_t>::max() - local) {
    co_return absl::OutOfRangeError("database epoch exhausted");
  }
  absl::Status advanced = co_await AdvanceDbEpoch(db_id, local + delta);
  if (!advanced.ok()) co_return advanced;
  replica_source_db_epochs_[db_id].store(source_db_epoch,
                                         std::memory_order_release);
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::ApplyReplicatedFlushAll(
    const std::array<std::uint64_t, kLogicalDatabaseCount>& source_epochs) {
  if (std::any_of(source_epochs.begin(), source_epochs.end(),
                  [](std::uint64_t epoch) { return epoch == 0; })) {
    co_return absl::InvalidArgumentError(
        "invalid replicated FLUSHALL database epochs");
  }
  if (bycorf::ThisWorker().id_ != 0) {
    co_return co_await bycorf::SubmitTaskTo(0, [this, source_epochs]() {
      return ApplyReplicatedFlushAll(source_epochs);
    });
  }
  std::array<std::uint64_t, kLogicalDatabaseCount> next{};
  for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
    const std::uint64_t installed_source =
        replica_source_db_epochs_[db_id].load(std::memory_order_acquire);
    if (installed_source == 0) {
      next[db_id] = source_epochs[db_id];
      continue;
    }
    if (source_epochs[db_id] < installed_source) {
      co_return absl::FailedPreconditionError(
          "replicated FLUSHALL epoch is outside the installed source root");
    }
    const std::uint64_t delta = source_epochs[db_id] - installed_source;
    const std::uint64_t local = DbEpoch(db_id);
    if (delta > std::numeric_limits<std::uint64_t>::max() - local) {
      co_return absl::OutOfRangeError("database epoch exhausted");
    }
    next[db_id] = local + delta;
  }
  // The caller holds every command DB gate. The 16 DB epochs occupy one
  // metadata page, so persist them as one vector before detaching any index.
  absl::Status detached = co_await DetachDbEpochs(next);
  if (!detached.ok()) co_return detached;
  for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
    replica_source_db_epochs_[db_id].store(source_epochs[db_id],
                                           std::memory_order_release);
  }
  co_return co_await ReclaimDetachedAllWorkers(/*wait=*/true);
}

Task<absl::Status> StorageEngine::Impl::DetachDbEpochs(
    const std::array<std::uint64_t, kLogicalDatabaseCount>& next) {
  if (bycorf::ThisWorker().id_ != 0) {
    co_return co_await bycorf::SubmitTaskTo(
        0, [this, next]() -> Task<absl::Status> {
          co_return co_await DetachDbEpochs(next);
        });
  }
  std::array<bool, kLogicalDatabaseCount> changed{};
  std::vector<std::pair<std::size_t, std::uint64_t>> updates;
  updates.reserve(kLogicalDatabaseCount);
  for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
    const std::uint64_t current = DbEpoch(db_id);
    if (next[db_id] == 0 || next[db_id] < current) {
      co_return absl::FailedPreconditionError(
          "replica database epoch vector is behind the local root");
    }
    if (next[db_id] == current) continue;
    changed[db_id] = true;
    updates.emplace_back(db_id, next[db_id]);
  }
  if (updates.empty()) co_return absl::OkStatus();

  absl::Status persisted = co_await PersistEpochValues(updates);
  if (!persisted.ok()) co_return persisted;
  for (const auto& [index, epoch] : updates) {
    db_epochs_[index].store(epoch, std::memory_order_release);
  }

  for (unsigned target = 0; target < worker_count_; ++target) {
    auto detach = [this, target, changed]() -> Task<absl::Status> {
      WorkerStore& store = *stores_[target];
      co_await store.store_state_mutex_.Lock();
      UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
      for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
        if (changed[db_id]) DetachDbLocal(store, db_id);
      }
      co_return absl::OkStatus();
    };
    absl::Status detached;
    if (target == 0) {
      detached = co_await detach();
    } else {
      detached = co_await bycorf::SubmitTaskTo(target, detach);
    }
    if (!detached.ok()) co_return detached;
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::DetachDbEpoch(std::uint8_t db_id,
                                                      std::uint64_t next) {
  if (bycorf::ThisWorker().id_ != 0) {
    co_return co_await bycorf::SubmitTaskTo(
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
      detached = co_await bycorf::SubmitTaskTo(target, detach);
    }
    if (!detached.ok()) {
      co_return detached;
    }
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::ReclaimDetachedAllWorkers(bool wait) {
  if (bycorf::ThisWorker().id_ != 0) {
    co_return co_await bycorf::SubmitTaskTo(
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
      reclaimed = co_await bycorf::SubmitTaskTo(target, reclaim);
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
  // Epoch metadata covers all 16 DBs even when runtime indexes cover DB0 only.
  if (db_id >= options_.database_count_) return;
  // FLUSHDB invalidates every watcher of this database, including watches
  // on keys that never existed (Redis semantics).
  tx::CurrentTxShard().MarkAllWatched(db_id);
  // The first full-sync implementation treats a DB epoch change as a session
  // boundary. A partially built root may already contain keys from this DB,
  // including partitions whose capture has not started, so a per-partition
  // replacement cannot make the in-place rebuild correct. Invalidate every
  // local session synchronously with detach; its next snapshot/override/DB
  // handoff operation aborts the whole full sync.
  for (auto& [session_id, session] : store.fullsync_sessions_) {
    (void)session_id;
    session.db_epoch_invalidated_ = true;
    session.publish_queue_.clear();
    session.publish_queue_bytes_ = 0;
    session.publisher_admitted_bytes_ = 0;
    session.publisher_admitted_items_ = 0;
  }
  // A DB epoch is part of every physical record validation. Keep FLUSHDB
  // online and bounded by invalidating an active RDB job; its coordinator
  // removes the temporary file and releases all snapshot pins.
  if (store.rdb_snapshot_.has_value()) [[unlikely]] {
    store.rdb_snapshot_->invalidated_ = true;
    for (auto& partition : store.partitions_) {
      if (partition.rdb_snapshot_.has_value()) {
        partition.rdb_snapshot_->accepting_ = false;
      }
    }
  }
  store.fullsync_publisher_capacity_ready_.NotifyAll(*store.worker_);
  ++store.index_generations_[db_id];
  for (auto& partition : store.partitions_) {
    auto& index = partition.indexes_[db_id];
    ++partition.grouped_generations_[db_id];
    QueueDetachedIndex(store, index, db_id, &partition.grouped_objects_[db_id]);
    partition.fullsync_coverage_bytes_[db_id] = 0;
    partition.live_key_count_[db_id] = 0;
    partition.expiring_key_count_[db_id] = 0;
    ++partition.mutation_sequence_;
    if (!partition.fullsync_subscribers_.empty()) [[unlikely]] {
      for (auto& [session_id, capture] : partition.fullsync_subscribers_) {
        ClearFullSyncCapture(store, session_id, capture);
      }
    }
  }
  store.live_key_count_[db_id] = 0;
}

void StorageEngine::Impl::QueueDetachedIndex(WorkerStore& store,
                                             RecordIndex& index,
                                             std::uint8_t db_id,
                                             GroupedObjectIndex* grouped) {
  const bool has_groups = grouped != nullptr && !grouped->empty();
  if (!index.has_allocated_storage() && !has_groups) return;
  store.detached_indexes_.push_back(DetachedIndex{
      .index_ = index.Detach(),
      .db_id_ = db_id,
      .grouped_ = has_groups
                      ? std::optional<GroupedObjectIndex>(grouped->Detach())
                      : std::nullopt,
  });
}

Task<absl::Status> StorageEngine::Impl::ReclaimDetachedIndexes(
    WorkerStore& store) {
  while (!store.detached_indexes_.empty()) {
    DetachedIndex detached = std::move(store.detached_indexes_.front());
    store.detached_indexes_.pop_front();

    struct BlockDelta {
      std::uint64_t bytes_ = 0;
      std::uint64_t tagged_bytes_ = 0;
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
    const auto accumulate_record = [&](const RecordIndex::Entry& entry,
                                       const ExtentManifest& manifest) {
      const RecordLocation location = MaterializeIndexLocation(entry);
      BlockDelta& delta = dead_by_block[std::pair(location.block_id(),
                                                  location.allocation_epoch())];
      delta.block_owner_ = location.block_owner();
      delta.bytes_ += entry.value_.total_disk_bytes();
      if (entry.value_.tx_tagged()) {
        delta.tagged_bytes_ += entry.value_.total_disk_bytes();
      }
      if (manifest) {
        // External parent-key bytes are needed to decode surviving source
        // records during a cold scan, even after their logical population is
        // detached. Keep their entire manifest dependent on source retirement.
        // Value-only children can enter the ordinary pinned extent reclaim.
        if (entry.value_.key_external()) [[unlikely]] {
          delta.dependent_extents_.push_back(manifest);
        } else {
          dead_extents.push_back(manifest);
        }
      }
    };
    detached.index_.ForEach([&](const RecordIndex::Entry& entry) {
      if (!entry.value_.external()) {
        accumulate_record(entry, nullptr);
        return;
      }
      auto manifest = store.external_manifests_.find(&entry);
      accumulate_record(entry, manifest == store.external_manifests_.end()
                                   ? nullptr
                                   : manifest->second);
      if (manifest != store.external_manifests_.end()) {
        store.external_manifests_.erase(manifest);
      }
    });
    absl::Status grouped_status = absl::OkStatus();
    if (detached.grouped_) {
      detached.grouped_->ForEach([&](std::string_view, const auto& object) {
        if (!object) {
          // Undo may retain an admitted empty slot after grouped->compact
          // replacement. The root and its journal own all physical records;
          // a null reservation contributes metadata capacity, not a graph.
          return;
        }
        object->ForEachRecord([&](HashGroupId, const RecordIndex::Entry& entry,
                                  const ExtentManifest& manifest, bool) {
          if (entry.value_.external() != static_cast<bool>(manifest)) {
            grouped_status = absl::InternalError(
                "detached group lost its external extent manifest");
            return;
          }
          // Retired parent markers are physical live records too. Omitting
          // them leaks both block live bytes and transaction-generation tags.
          accumulate_record(entry, manifest);
        });
      });
    }
    if (!grouped_status.ok()) {
      store.write_failed_ = true;
      co_return grouped_status;
    }

    for (const auto& extents : dead_extents) {
      SpawnExtentReclaim(store, extents);
    }

    for (auto& [block, delta] : dead_by_block) {
      // A block holds at most kStorageBlockBytes, so the sum still fits the
      // per-record width.
      assert(delta.bytes_ <= kStorageBlockBytes);
      // The storage format keeps tagged records in transaction blocks and
      // untagged records in ordinary blocks. Preserve that distinction while
      // batching detached entries: transaction-generation accounting must
      // lose the same bytes as the block's ordinary live-byte accounting.
      if (delta.tagged_bytes_ != 0 && delta.tagged_bytes_ != delta.bytes_)
          [[unlikely]] {
        LatchRuntimeFailure(store);
        co_return absl::InternalError(
            "detached index mixed tagged and untagged records in one block");
      }
      RetiredRecord aggregate{
          .block_id_ = block.first,
          .allocation_epoch_ = block.second,
          .total_disk_bytes_ = static_cast<std::uint32_t>(delta.bytes_),
          .block_owner_ = delta.block_owner_,
          .record_offset_ = 0,
          .tx_tagged_ = delta.tagged_bytes_ != 0,
          .dependent_extents_ = nullptr,
          .immediate_extents_ = nullptr,
          .extra_dependent_extents_ =
              delta.dependent_extents_.empty()
                  ? nullptr
                  : std::make_shared<const std::vector<ExtentManifest>>(
                        std::move(delta.dependent_extents_)),
      };
      absl::Status dead = co_await MarkRecordDead(aggregate);
      if (!dead.ok()) {
        LatchRuntimeFailure(store);
        co_return dead;
      }
    }

    // Freeing the entries is the expensive part of this loop, and it happens
    // as `detached` goes out of scope. Yield so online work is polled between
    // populations.
    co_await bycorf::Yield(*store.worker_);
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
  } else {
    // An enqueue can observe the old runner while it is in its final
    // SealDeadActiveBlock await. Recheck after publishing false so that work
    // cannot be stranded without a runner in that handoff window.
    EnsureDetachedReclaim(*store);
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
    if (!store.detached_reclaim_running_ &&
        (store.write_failed_ || RuntimeFailureLatched())) {
      break;
    }
    absl::Status waited =
        co_await bycorf::SleepFor(*store.worker_, std::chrono::milliseconds(1));
    if (!waited.ok()) {
      co_return waited;
    }
  }
  if (store.write_failed_ || RuntimeFailureLatched()) {
    co_return absl::Status(
        absl::StatusCode::kInternal,
        "storage writer stopped while reclaiming detached indexes");
  }
  co_return absl::OkStatus();
}

}  // namespace lavik::storage
