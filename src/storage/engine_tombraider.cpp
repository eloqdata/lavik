#include "engine_impl.h"

namespace keylane::storage {

// A tombstone (and the index entry pinning it) is dead weight once no older
// record of its key survives on disk: recovery would conclude "absent" with
// or without it. The raider proves that by scanning every allocated records
// block once — the only authority on what actually survives — in three
// phases: mark every candidate entry unclaimed, sweep all blocks clearing
// the bit for entries a surviving dangerous record still needs, then reap
// what stayed unclaimed. Everything is memory-state; a crash mid-round just
// forfeits the round, and recovery rebuilds both tombstone entries and
// shielding bits exactly from the surviving records.

Task<absl::Status> StorageEngine::Impl::TombRaiderLoop(WorkerStore* store) {
  const auto interval =
      std::chrono::milliseconds(options_.tomb_raider_interval_ms);
  while (!store->worker->stop_requested()) {
    absl::Status waited = co_await celer::SleepFor(*store->worker, interval);
    if (!waited.ok()) {
      co_return waited;
    }
    if (store->worker->stop_requested() ||
        shutdown_flush_requested_.load(std::memory_order_acquire)) {
      break;
    }
    absl::Status round = co_await RunTombRaider();
    if (!round.ok()) {
      spdlog::error("tomb raider round failed: {}", round.message());
      co_return round;
    }
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::RunTombRaider() {
  bool expected = false;
  if (!tomb_raider_running_.compare_exchange_strong(
          expected, true, std::memory_order_acq_rel)) {
    co_return absl::OkStatus();
  }
  // The round counts as a settlement: its frames park on cross-worker hops,
  // so the shutdown drain must not finish under it. In exchange, every
  // phase aborts at its next block/batch boundary once a shutdown flush is
  // requested — a forfeited round costs nothing, the next run redoes it.
  active_settlements_.fetch_add(1, std::memory_order_acq_rel);
  struct RoundGuard {
    std::atomic<bool>* running;
    std::atomic<std::uint32_t>* settlements;
    ~RoundGuard() {
      settlements->fetch_sub(1, std::memory_order_acq_rel);
      running->store(false, std::memory_order_release);
    }
  } round_guard{&tomb_raider_running_, &active_settlements_};

  // The reap must not start until every worker's sweep has finished: the
  // record that still needs a candidate may sit in the last unswept block.
  for (unsigned target = 0; target < worker_count_; ++target) {
    absl::Status marked = co_await celer::SubmitTaskTo(
        target, [this, target]() -> Task<absl::Status> {
          co_return co_await TombMarkLocal(*stores_[target]);
        });
    if (!marked.ok()) {
      co_return marked;
    }
  }
  for (unsigned target = 0; target < worker_count_; ++target) {
    absl::Status swept = co_await celer::SubmitTaskTo(
        target, [this, target]() -> Task<absl::Status> {
          co_return co_await TombSweepLocal(*stores_[target]);
        });
    if (!swept.ok()) {
      co_return swept;
    }
  }
  for (unsigned target = 0; target < worker_count_; ++target) {
    absl::Status reaped = co_await celer::SubmitTaskTo(
        target, [this, target]() -> Task<absl::Status> {
          co_return co_await TombReapLocal(*stores_[target]);
        });
    if (!reaped.ok()) {
      co_return reaped;
    }
  }
  tomb_raider_rounds_.fetch_add(1, std::memory_order_relaxed);
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::TombMarkLocal(WorkerStore& store) {
  std::size_t steps = 0;
  for (auto& partition : store.partitions) {
    for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
      auto& index = partition.indexes[db_id];
      if (index.empty()) {
        continue;
      }
      std::uint64_t cursor = 0;
      do {
        if (shutdown_flush_requested_.load(std::memory_order_acquire)) {
          co_return absl::OkStatus();  // forfeit the round
        }
        cursor = index.Scan(cursor, [](RecordIndex::Entry& entry) {
          if (entry.value.kind == RecordKind::kTombstone ||
              (entry.value.kind == RecordKind::kValue &&
               entry.value.shielding)) {
            entry.value.unclaimed = true;
          }
        });
        if (++steps % 256 == 0) {
          co_await celer::Yield(*store.worker);
        }
      } while (cursor != 0);
    }
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::TombClaimLocal(
    WorkerStore& store, std::vector<TombClaim> claims) {
  std::size_t handled = 0;
  for (const TombClaim& claim : claims) {
    auto& partition = PartitionForKey(store, claim.key);
    if (partition.replication_epoch == claim.replication_epoch) {
      auto* entry =
          partition.indexes[claim.db_id].Find(claim.digest, claim.key);
      if (entry != nullptr && entry->value.unclaimed &&
          claim.mutation_sequence < entry->value.mutation_sequence) {
        entry->value.unclaimed = false;
      }
    }
    if (++handled % 256 == 0) {
      co_await celer::Yield(*store.worker);
    }
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::TombSweepLocal(WorkerStore& store) {
  struct SweepBuffer {
    RegisteredBufferPool* pool = nullptr;
    std::uint16_t buffer_id = 0;
    std::byte* heap_data = nullptr;
    FixedBuffer buffer{};

    ~SweepBuffer() {
      if (buffer_id != 0) {
        pool->ReleaseWriteBuffer(buffer_id);
      } else if (heap_data != nullptr) {
        pool->ReleaseHeapWriteBuffer(heap_data);
      }
    }

    bool registered() const noexcept { return buffer_id != 0; }
  } sweep{.pool = &store.buffers};
  if (store.buffers.TryAcquireWriteBuffer(&sweep.buffer_id)) {
    sweep.buffer = store.buffers.write_buffer(sweep.buffer_id);
  } else if (store.buffers.TryAcquireHeapWriteBuffer(&sweep.heap_data)) {
    sweep.buffer = FixedBuffer{
        .data = sweep.heap_data,
        .size = options_.buffers.write_buffer_bytes,
        .index = 0,
    };
  } else {
    co_return absl::Status(absl::StatusCode::kResourceExhausted,
                           "failed to allocate a tomb raider sweep buffer");
  }
  if (sweep.buffer.size < kStorageBlockBytes) {
    co_return absl::Status(absl::StatusCode::kResourceExhausted,
                           "tomb raider sweep buffer is smaller than a block");
  }
  sweep.buffer.size = kStorageBlockBytes;

  struct BlockSnapshot {
    std::uint64_t block_id = 0;
    std::uint64_t allocation_epoch = 0;
  };
  // Blocks allocated after this snapshot cannot matter: new appends carry
  // higher sequences, and relocations only move index-current records — for
  // a key whose entry is a candidate, every older record is dead to the
  // index and is dropped by salvage, never moved.
  std::vector<BlockSnapshot> blocks;
  ForEachOwnedBlock(store, [&](std::uint64_t block_id, BlockState& state) {
    if (state.kind == BlockKind::kRecords &&
        state.committed_bytes > kBlockHeaderBytes) {
      blocks.push_back(BlockSnapshot{
          .block_id = block_id,
          .allocation_epoch = state.allocation_epoch,
      });
    }
  });

  std::vector<std::vector<TombClaim>> pending(worker_count_);
  auto flush_claims = [&](unsigned owner) -> Task<absl::Status> {
    std::vector<TombClaim> batch = std::move(pending[owner]);
    pending[owner].clear();
    if (batch.empty()) {
      co_return absl::OkStatus();
    }
    if (owner == store.worker->id()) {
      co_return co_await TombClaimLocal(store, std::move(batch));
    }
    co_return co_await celer::SubmitTaskTo(
        owner,
        [this, owner,
         batch = std::move(batch)]() mutable -> Task<absl::Status> {
          co_return co_await TombClaimLocal(*stores_[owner], std::move(batch));
        });
  };

  for (const BlockSnapshot& snapshot : blocks) {
    if (shutdown_flush_requested_.load(std::memory_order_acquire)) {
      co_return absl::OkStatus();  // forfeit the round
    }
    BlockState* state = FindBlockState(store, snapshot.block_id);
    if (state == nullptr || !state->allocated ||
        state->allocation_epoch != snapshot.allocation_epoch ||
        state->kind != BlockKind::kRecords) {
      continue;  // freed or reused: its old records left the disk with it
    }
    const std::uint32_t committed = state->committed_bytes;
    if (committed <= kBlockHeaderBytes) {
      continue;
    }
    StagingSlot* slot = StagingFor(store, *state);
    if (slot != nullptr) {
      // Unflushed records exist only in the staging buffer; missing them
      // would let a still-dangerous key reap its tombstone. The copy runs
      // without suspending, so the captured prefix is consistent and the
      // slot cannot be released underneath it.
      FixedBuffer staged =
          slot->write_buffer_id != 0
              ? store.buffers.write_buffer(slot->write_buffer_id)
              : FixedBuffer{.data = slot->heap_data,
                            .size = slot->heap_data_size,
                            .index = 0};
      if (staged.data == nullptr || staged.size < committed) {
        continue;
      }
      std::memcpy(sweep.buffer.data, staged.data, committed);
    } else {
      // Slotless blocks are fully flushed and never appended to again, so
      // the on-disk image below `committed` is final.
      const auto [file_id, block_offset] = FileOffset(snapshot.block_id);
      auto read = co_await ReadStorageBuffer(*store.worker,
                                             store.files[file_id], sweep.buffer,
                                             sweep.registered(), block_offset);
      if (!read.ok()) {
        co_return read.status();
      }
      if (*read != kStorageBlockBytes) {
        co_return absl::Status(absl::StatusCode::kInternal,
                               "short block read during tomb raider sweep");
      }
      BlockState* current = FindBlockState(store, snapshot.block_id);
      if (current == nullptr || !current->allocated ||
          current->allocation_epoch != snapshot.allocation_epoch) {
        continue;  // reclaimed while the read was in flight
      }
    }

    const std::uint64_t now_ms = UnixTimeMillis();
    std::uint32_t record_offset = kBlockHeaderBytes;
    std::size_t decoded = 0;
    while (record_offset < committed) {
      const std::optional<std::uint32_t> next =
          NextRecordOffset(sweep.buffer.data, record_offset, committed);
      if (!next.has_value()) {
        break;  // torn or foreign bytes: nothing decodable remains
      }
      if (*next != record_offset) {
        record_offset = *next;
        continue;
      }
      RecordHeader record{};
      std::string_view disk_key;
      std::span<const std::byte> record_bytes(sweep.buffer.data + record_offset,
                                              committed - record_offset);
      if (!DecodeRecordHeader(record_bytes, &record, &disk_key) ||
          record.allocation_epoch != snapshot.allocation_epoch ||
          record.total_disk_bytes == 0 ||
          record_offset + record.total_disk_bytes > committed) {
        break;
      }
      record_offset += record.total_disk_bytes;
      // Only a value that could still be alive claims its key's candidate:
      // expired values are suppressed by their own timestamp, tombstones
      // and commit records suppress nothing worth keeping a marker for,
      // and records from a flushed database epoch are already condemned.
      if (record.kind == RecordKind::kValue &&
          record.db_epoch == DbEpoch(record.db_id) &&
          (record.expire_at_ms == 0 || record.expire_at_ms > now_ms)) {
        const unsigned key_owner = OwnerForKey(disk_key);
        pending[key_owner].push_back(TombClaim{
            .mutation_sequence = record.mutation_sequence,
            .replication_epoch = record.replication_epoch,
            .digest = record.digest,
            .key = std::string(disk_key),
            .db_id = record.db_id,
        });
        if (pending[key_owner].size() >= 512) {
          absl::Status flushed = co_await flush_claims(key_owner);
          if (!flushed.ok()) {
            co_return flushed;
          }
        }
      }
      if (++decoded % 256 == 0) {
        co_await celer::Yield(*store.worker);
      }
    }
    // Throttle: one block per sleep bounds the sweep's disk-bandwidth and
    // CPU share, so a full-disk round never crowds out online traffic.
    if (options_.tomb_raider_sleep_ms != 0) {
      absl::Status slept = co_await celer::SleepFor(
          *store.worker,
          std::chrono::milliseconds(options_.tomb_raider_sleep_ms));
      if (!slept.ok()) {
        co_return slept;
      }
    }
  }
  for (unsigned owner = 0; owner < worker_count_; ++owner) {
    absl::Status flushed = co_await flush_claims(owner);
    if (!flushed.ok()) {
      co_return flushed;
    }
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::TombReapLocal(WorkerStore& store) {
  struct Candidate {
    Digest digest{};
    std::string key;
    std::uint8_t db_id = 0;
  };
  std::vector<Candidate> tombs;
  std::uint64_t refreshed = 0;
  std::size_t steps = 0;
  for (auto& partition : store.partitions) {
    for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
      auto& index = partition.indexes[db_id];
      if (index.empty()) {
        continue;
      }
      std::uint64_t cursor = 0;
      do {
        cursor = index.Scan(cursor, [&](RecordIndex::Entry& entry) {
          if (!entry.value.unclaimed) {
            return;
          }
          if (entry.value.kind == RecordKind::kValue) {
            // The sweep found nothing this value was shielding: the sticky
            // bit outlived whatever it once protected. Its next expiry can
            // take the in-memory path.
            entry.value.unclaimed = false;
            if (entry.value.shielding) {
              entry.value.shielding = false;
              ++refreshed;
            }
          } else if (entry.value.kind == RecordKind::kTombstone) {
            tombs.push_back(Candidate{
                .digest = entry.digest,
                .key = entry.key,
                .db_id = db_id,
            });
          }
        });
        if (++steps % 256 == 0) {
          co_await celer::Yield(*store.worker);
        }
      } while (cursor != 0);
    }
  }

  std::uint64_t reaped = 0;
  for (const Candidate& candidate : tombs) {
    if (shutdown_flush_requested_.load(std::memory_order_acquire)) {
      break;  // forfeit the rest; totals below still publish
    }
    auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
        candidate.db_id, tx::FingerprintOf(candidate.digest),
        tx::LockMode::kExclusive);
    co_await store.store_state_mutex.Lock();
    UnlockGuard unlock(&store.store_state_mutex, store.worker);
    auto& partition = PartitionForKey(store, candidate.key);
    auto* entry = partition.indexes[candidate.db_id].Find(candidate.digest,
                                                          candidate.key);
    if (entry == nullptr || entry->value.kind != RecordKind::kTombstone ||
        !entry->value.unclaimed) {
      continue;  // rewritten or claimed since collection
    }
    const RecordLocation dropped = entry->value;
    // No watcher or replica cares: erasing a tombstone changes nothing a
    // reader can observe. Only the flush completion's staged identities
    // dereference the entry by pointer, so detach them before it is freed.
    for (auto& [block_id, identities] : store.staged_records) {
      for (RecordIdentity& identity : identities) {
        if (identity.entry == entry) {
          identity.entry = nullptr;
        }
      }
    }
    partition.indexes[candidate.db_id].Erase(candidate.digest, candidate.key);
    absl::Status dead = co_await MarkRecordDead(RetiredRecordOf(dropped));
    if (!dead.ok()) {
      store.write_failed = true;
      co_return dead;
    }
    ++reaped;
    co_await celer::Yield(*store.worker);
  }
  if (reaped != 0) {
    tomb_raider_reaped_.fetch_add(reaped, std::memory_order_relaxed);
  }
  if (refreshed != 0) {
    tomb_raider_refreshed_.fetch_add(refreshed, std::memory_order_relaxed);
  }
  co_return absl::OkStatus();
}

}  // namespace keylane::storage
