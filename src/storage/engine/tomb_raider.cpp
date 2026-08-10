#include "impl.h"

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
      std::chrono::milliseconds(options_.tomb_raider_interval_ms_);
  while (!store->worker_->stop_requested()) {
    absl::Status waited = co_await celer::SleepFor(*store->worker_, interval);
    if (!waited.ok()) {
      co_return waited;
    }
    if (store->worker_->stop_requested() ||
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
    std::atomic<bool>* running_;
    std::atomic<std::uint32_t>* settlements_;
    ~RoundGuard() {
      settlements_->fetch_sub(1, std::memory_order_acq_rel);
      running_->store(false, std::memory_order_release);
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
  for (auto& partition : store.partitions_) {
    for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
      auto& index = partition.indexes_[db_id];
      if (index.empty()) {
        continue;
      }
      std::uint64_t cursor = 0;
      do {
        if (shutdown_flush_requested_.load(std::memory_order_acquire)) {
          co_return absl::OkStatus();  // forfeit the round
        }
        cursor = index.Scan(cursor, [](RecordIndex::Entry& entry) {
          if (entry.value_.kind_ == RecordKind::kTombstone ||
              (entry.value_.kind_ == RecordKind::kValue &&
               entry.value_.shielding_)) {
            entry.value_.unclaimed_ = true;
          }
        });
        if (++steps % 256 == 0) {
          co_await celer::Yield(*store.worker_);
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
    auto& partition = PartitionForKey(store, claim.key_);
    if (partition.replication_epoch_ == claim.replication_epoch_) {
      auto* entry =
          partition.indexes_[claim.db_id_].Find(claim.digest_, claim.key_);
      if (entry != nullptr && entry->value_.unclaimed_ &&
          claim.mutation_sequence_ < entry->value_.mutation_sequence_) {
        entry->value_.unclaimed_ = false;
      }
    }
    if (++handled % 256 == 0) {
      co_await celer::Yield(*store.worker_);
    }
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::TombSweepLocal(WorkerStore& store) {
  struct SweepBuffer {
    RegisteredBufferPool* pool_ = nullptr;
    std::uint16_t buffer_id_ = 0;
    std::byte* heap_data_ = nullptr;
    FixedBuffer buffer_{};

    ~SweepBuffer() {
      if (buffer_id_ != 0) {
        pool_->ReleaseWriteBuffer(buffer_id_);
      } else if (heap_data_ != nullptr) {
        pool_->ReleaseHeapWriteBuffer(heap_data_);
      }
    }

    bool registered() const noexcept { return buffer_id_ != 0; }
  } sweep{.pool_ = &store.buffers_};
  if (store.buffers_.TryAcquireWriteBuffer(&sweep.buffer_id_)) {
    sweep.buffer_ = store.buffers_.write_buffer(sweep.buffer_id_);
  } else if (store.buffers_.TryAcquireHeapWriteBuffer(&sweep.heap_data_)) {
    sweep.buffer_ = FixedBuffer{
        .data_ = sweep.heap_data_,
        .size_ = options_.buffers_.write_buffer_bytes_,
        .index_ = 0,
    };
  } else {
    co_return absl::Status(absl::StatusCode::kResourceExhausted,
                           "failed to allocate a tomb raider sweep buffer");
  }
  if (sweep.buffer_.size_ < kStorageBlockBytes) {
    co_return absl::Status(absl::StatusCode::kResourceExhausted,
                           "tomb raider sweep buffer is smaller than a block");
  }
  sweep.buffer_.size_ = kStorageBlockBytes;

  struct BlockSnapshot {
    std::uint64_t block_id_ = 0;
    std::uint64_t allocation_epoch_ = 0;
  };
  // Blocks allocated after this snapshot cannot matter: new appends carry
  // higher sequences, and relocations only move index-current records — for
  // a key whose entry is a candidate, every older record is dead to the
  // index and is dropped by salvage, never moved.
  std::vector<BlockSnapshot> blocks;
  ForEachOwnedBlock(store, [&](std::uint64_t block_id, BlockState& state) {
    if (state.kind_ == BlockKind::kRecords &&
        state.committed_bytes_ > kBlockHeaderBytes) {
      blocks.push_back(BlockSnapshot{
          .block_id_ = block_id,
          .allocation_epoch_ = state.allocation_epoch_,
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
    if (owner == store.worker_->id()) {
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
    BlockState* state = FindBlockState(store, snapshot.block_id_);
    if (state == nullptr || !state->allocated_ ||
        state->allocation_epoch_ != snapshot.allocation_epoch_ ||
        state->kind_ != BlockKind::kRecords) {
      continue;  // freed or reused: its old records left the disk with it
    }
    const std::uint32_t committed = state->committed_bytes_;
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
          slot->write_buffer_id_ != 0
              ? store.buffers_.write_buffer(slot->write_buffer_id_)
              : FixedBuffer{.data_ = slot->heap_data_,
                            .size_ = slot->heap_data_size_,
                            .index_ = 0};
      if (staged.data_ == nullptr || staged.size_ < committed) {
        continue;
      }
      std::memcpy(sweep.buffer_.data_, staged.data_, committed);
    } else {
      // Slotless blocks are fully flushed and never appended to again, so
      // the on-disk image below `committed` is final.
      const auto [file_id, block_offset] = FileOffset(snapshot.block_id_);
      auto read = co_await ReadStorageBuffer(
          *store.worker_, store.files_[file_id], sweep.buffer_,
          sweep.registered(), block_offset);
      if (!read.ok()) {
        co_return read.status();
      }
      if (*read != kStorageBlockBytes) {
        co_return absl::Status(absl::StatusCode::kInternal,
                               "short block read during tomb raider sweep");
      }
      BlockState* current = FindBlockState(store, snapshot.block_id_);
      if (current == nullptr || !current->allocated_ ||
          current->allocation_epoch_ != snapshot.allocation_epoch_) {
        continue;  // reclaimed while the read was in flight
      }
    }

    const std::uint64_t now_ms = UnixTimeMillis();
    std::uint32_t record_offset = kBlockHeaderBytes;
    std::size_t decoded = 0;
    while (record_offset < committed) {
      const std::optional<std::uint32_t> next =
          NextRecordOffset(sweep.buffer_.data_, record_offset, committed);
      if (!next.has_value()) {
        break;  // torn or foreign bytes: nothing decodable remains
      }
      if (*next != record_offset) {
        record_offset = *next;
        continue;
      }
      RecordHeader record{};
      std::string_view disk_key;
      std::span<const std::byte> record_bytes(
          sweep.buffer_.data_ + record_offset, committed - record_offset);
      if (!DecodeRecordHeader(record_bytes, &record, &disk_key) ||
          record.allocation_epoch_ != snapshot.allocation_epoch_ ||
          record.total_disk_bytes_ == 0 ||
          record_offset + record.total_disk_bytes_ > committed) {
        break;
      }
      record_offset += record.total_disk_bytes_;
      // Only a value that could still be alive claims its key's candidate:
      // expired values are suppressed by their own timestamp, tombstones
      // and commit records suppress nothing worth keeping a marker for,
      // and records from a flushed database epoch are already condemned.
      if (record.kind_ == RecordKind::kValue &&
          record.db_epoch_ == DbEpoch(record.db_id_) &&
          (record.expire_at_ms_ == 0 || record.expire_at_ms_ > now_ms)) {
        const unsigned key_owner = OwnerForKey(disk_key);
        pending[key_owner].push_back(TombClaim{
            .mutation_sequence_ = record.mutation_sequence_,
            .replication_epoch_ = record.replication_epoch_,
            .digest_ = record.digest_,
            .key_ = std::string(disk_key),
            .db_id_ = record.db_id_,
        });
        if (pending[key_owner].size() >= 512) {
          absl::Status flushed = co_await flush_claims(key_owner);
          if (!flushed.ok()) {
            co_return flushed;
          }
        }
      }
      if (++decoded % 256 == 0) {
        co_await celer::Yield(*store.worker_);
      }
    }
    // Throttle: one block per sleep bounds the sweep's disk-bandwidth and
    // CPU share, so a full-disk round never crowds out online traffic.
    if (options_.tomb_raider_sleep_ms_ != 0) {
      absl::Status slept = co_await celer::SleepFor(
          *store.worker_,
          std::chrono::milliseconds(options_.tomb_raider_sleep_ms_));
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
    Digest digest_{};
    std::string key_;
    std::uint8_t db_id_ = 0;
  };
  std::vector<Candidate> tombs;
  std::uint64_t refreshed = 0;
  std::size_t steps = 0;
  for (auto& partition : store.partitions_) {
    for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
      auto& index = partition.indexes_[db_id];
      if (index.empty()) {
        continue;
      }
      std::uint64_t cursor = 0;
      do {
        cursor = index.Scan(cursor, [&](RecordIndex::Entry& entry) {
          if (!entry.value_.unclaimed_) {
            return;
          }
          if (entry.value_.kind_ == RecordKind::kValue) {
            // The sweep found nothing this value was shielding: the sticky
            // bit outlived whatever it once protected. Its next expiry can
            // take the in-memory path.
            entry.value_.unclaimed_ = false;
            if (entry.value_.shielding_) {
              entry.value_.shielding_ = false;
              ++refreshed;
            }
          } else if (entry.value_.kind_ == RecordKind::kTombstone) {
            tombs.push_back(Candidate{
                .digest_ = ComputeDigest(entry.key()),
                .key_ = std::string(entry.key()),
                .db_id_ = db_id,
            });
          }
        });
        if (++steps % 256 == 0) {
          co_await celer::Yield(*store.worker_);
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
        candidate.db_id_, tx::FingerprintOf(candidate.digest_),
        tx::LockMode::kExclusive);
    co_await store.store_state_mutex_.Lock();
    UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
    auto& partition = PartitionForKey(store, candidate.key_);
    auto* entry = partition.indexes_[candidate.db_id_].Find(candidate.digest_,
                                                            candidate.key_);
    if (entry == nullptr || entry->value_.kind_ != RecordKind::kTombstone ||
        !entry->value_.unclaimed_) {
      continue;  // rewritten or claimed since collection
    }
    const RecordLocation dropped = entry->value_;
    // No watcher or replica cares: erasing a tombstone changes nothing a
    // reader can observe. Only the flush completion's staged identities
    // dereference the entry by pointer, so detach them before it is freed.
    for (auto& [block_id, identities] : store.staged_records_) {
      for (RecordIdentity& identity : identities) {
        if (identity.entry_ == entry) {
          identity.entry_ = nullptr;
        }
      }
    }
    store.external_manifests_.erase(entry);
    partition.indexes_[candidate.db_id_].Erase(candidate.digest_,
                                               candidate.key_);
    absl::Status dead = co_await MarkRecordDead(RetiredRecordOf(dropped));
    if (!dead.ok()) {
      store.write_failed_ = true;
      co_return dead;
    }
    ++reaped;
    co_await celer::Yield(*store.worker_);
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
