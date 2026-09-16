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

Task<absl::Status> StorageEngine::Impl::PeriodicFlush(WorkerStore* store) {
  const auto interval = std::chrono::milliseconds(options_.flush_max_ms_);
  while (!store->worker_->stop_requested()) {
    absl::Status status = co_await bycorf::SleepFor(*store->worker_, interval);
    if (!status.ok()) {
      CompleteShutdownFlush(status);
      co_return status;
    }
    if (store->worker_->stop_requested()) {
      break;
    }

    if (shutdown_flush_requested_.load(std::memory_order_acquire)) {
      status = co_await FlushWorkerForShutdown(store);
      if (!status.ok())
        shutdown_flush_failed_.store(true, std::memory_order_release);
      // This all-worker boundary is independent of the optional index cache.
      // Once the proof commits, only semantics-preserving checkpoint work may
      // run; no accepted client, replication, expiry or transaction work
      // remains.
      absl::Status recovery_barrier =
          co_await shutdown_recovery_ready_barrier_->Wait(*store->worker_);
      if (recovery_barrier.ok() && store->worker_->id() == 0 && status.ok()) {
        status = co_await PublishCleanShutdownProof();
        if (!status.ok())
          shutdown_flush_failed_.store(true, std::memory_order_release);
      }
      if (recovery_barrier.ok()) {
        recovery_barrier = co_await shutdown_recovery_published_barrier_->Wait(
            *store->worker_);
      }
      if (!recovery_barrier.ok() && status.ok()) status = recovery_barrier;
      if (shutdown_checkpoint_for_flush_) {
        // Stop ordinary append activity before worker 0 promotes every
        // committed transaction-tagged winner. All workers must observe that
        // result before freezing their index shard; the remaining barriers
        // keep shard construction and bitmap/root publication ordered.
        absl::Status barrier =
            co_await shutdown_checkpoint_ready_barrier_->Wait(*store->worker_);
        if (barrier.ok()) {
          if (store->worker_->id() == 0) {
            checkpoint_tx_cleanup_status_ =
                co_await DrainTxCleanerForShutdown();
          }
          barrier = co_await shutdown_checkpoint_tx_cleaned_barrier_->Wait(
              *store->worker_);
        }
        if (barrier.ok()) {
          // The transaction cleaner can promote tagged winners by appending
          // ordinary records and can therefore reopen an append stream after
          // the first freeze. Seal and drain that tail on every owner before
          // any shard starts traversing its supposedly stable index.
          absl::Status refrozen = co_await FlushWorkerForShutdown(store);
          if (status.ok() && !refrozen.ok()) status = std::move(refrozen);
          barrier = co_await shutdown_checkpoint_refrozen_barrier_->Wait(
              *store->worker_);
        }
        if (barrier.ok()) {
          const std::uint64_t generation = checkpoint_root_.generation_ + 1;
          CheckpointShardResult& shard =
              checkpoint_shards_[store->worker_->id()];
          bool frozen = false;
          co_await store->store_state_mutex_.Lock();
          {
            UnlockGuard guard(&store->store_state_mutex_, store->worker_);
            frozen = !store->expiry_cycle_running_ && !store->flush_running_ &&
                     store->flush_queue_.empty() && !RuntimeFailureLatched();
          }
          if (!frozen) {
            shard.status_ = absl::FailedPreconditionError(
                "storage index was not frozen before shutdown checkpoint");
          } else if (!status.ok()) {
            shard.status_ = status;
          } else if (!checkpoint_tx_cleanup_status_.ok()) {
            shard.status_ = checkpoint_tx_cleanup_status_;
          } else if (generation == 0) {
            shard.status_ = absl::ResourceExhaustedError(
                "checkpoint generation is exhausted");
          } else {
            shard.status_ =
                co_await BuildShutdownCheckpointShard(*store, generation);
          }
          barrier = co_await shutdown_checkpoint_built_barrier_->Wait(
              *store->worker_);
          if (barrier.ok() && store->worker_->id() == 0) {
            checkpoint_publish_status_ =
                co_await PublishShutdownCheckpoint(generation);
          }
          if (barrier.ok()) {
            barrier = co_await shutdown_checkpoint_published_barrier_->Wait(
                *store->worker_);
          }
          if (barrier.ok() && !checkpoint_publish_status_.ok() &&
              store->worker_->id() == 0) {
            spdlog::warn("shutdown checkpoint was not published: {}",
                         checkpoint_publish_status_.message());
          } else if (barrier.ok() && store->worker_->id() == 0) {
            shutdown_checkpoint_published_.store(true,
                                                 std::memory_order_release);
            std::uint64_t entries = 0;
            std::uint64_t accounting_entries = 0;
            std::size_t blocks = 0;
            for (const CheckpointShardResult& completed : checkpoint_shards_) {
              entries += completed.entry_count_;
              accounting_entries += completed.accounting_entry_count_;
              blocks += completed.blocks_.size();
            }
            spdlog::info(
                "published shutdown checkpoint generation={} entries={} "
                "accounting-entries={} blocks={}",
                generation, entries, accounting_entries, blocks);
          }
        }
        if (!barrier.ok() && status.ok()) status = barrier;
      }
      CompleteShutdownFlush(status);
      co_return status;
    }

    {
      co_await store->store_state_mutex_.Lock();
      UnlockGuard guard(&store->store_state_mutex_, store->worker_);
      FlushActiveBlock(*store);
    }
    status = co_await MaybeRunTxCleaner();
    if (!status.ok()) {
      // Cleaner races (pins, foreground replacement) and allocation pressure
      // are retryable background-maintenance failures. They must never stop
      // this worker's periodic flush loop or masquerade as completion of a
      // shutdown drain. MaybeRunTxCleaner has already re-armed dirty state;
      // record the failure and retry after the configured cooldown.
      spdlog::warn("worker[{}] transaction cleaner round failed; retrying: {}",
                   store->worker_->id(), status.message());
    }
  }
  co_return absl::OkStatus();
}

void StorageEngine::Impl::RequestFlush(WorkerStore& store,
                                       std::uint64_t block_id) {
  BlockState* state = FindBlockState(store, block_id);
  if (state == nullptr || !state->allocated_ || !state->in_memory_ ||
      state->staging_slot_ == 0) {
    return;
  }
  if (state->flush_queued_ || state->flush_in_progress_) {
    return;
  }
  state->flush_queued_ = true;
  store.flush_queue_.push_back(block_id);
  if (store.flush_running_) {
    return;
  }
  store.flush_running_ = true;
  active_flushes_.fetch_add(1, std::memory_order_acq_rel);
  store.worker_->Spawn(FlushPendingBlocks(&store));
}

Task<absl::Status> StorageEngine::Impl::FlushPendingBlocks(WorkerStore* store) {
  struct FlushRunGuard {
    Impl* engine_ = nullptr;
    ~FlushRunGuard() {
      engine_->space_reclaim_generation_.fetch_add(1,
                                                   std::memory_order_release);
      engine_->active_flushes_.fetch_sub(1, std::memory_order_acq_rel);
    }
  } flush_run_guard{this};

  struct PendingFlush {
    std::uint64_t block_id_ = 0;
    std::uint32_t committed_bytes_ = 0;
    std::uint32_t durable_bytes_ = 0;
    std::uint64_t allocation_epoch_ = 0;
    std::uint16_t write_buffer_id_ = 0;
    std::byte* heap_data_ = nullptr;
    std::size_t heap_data_size_ = 0;
    std::uint8_t slot_ = 0;
    std::vector<RecordIdentity> staged_records_;
  };

  while (true) {
    std::optional<PendingFlush> pending;
    // On any failure below, the staging buffer deliberately stays with its
    // slot: the block's staging_slot and in_memory still reference it, so
    // handing it back to the pool would let another writer reacquire memory
    // that staged readers are still following (and the heap path would
    // accept the same pointer twice). write_failed fail-stops the writer, so
    // the buffer simply remains owned by the slot — staged reads keep
    // working — until shutdown.

    {
      co_await store->store_state_mutex_.Lock();
      UnlockGuard guard(&store->store_state_mutex_, store->worker_);

      if (store->flush_queue_.empty()) {
        store->flush_running_ = false;
        co_return absl::OkStatus();
      }

      const std::uint64_t block_id = store->flush_queue_.front();
      store->flush_queue_.pop_front();
      BlockState* state = FindBlockState(*store, block_id);
      if (state == nullptr) {
        continue;
      }
      if (!state->allocated_ || !state->in_memory_ ||
          state->staging_slot_ == 0 || state->flush_in_progress_) {
        state->flush_queued_ = false;
        continue;
      }
      // Readers do not hold this flush up. It writes only the padding above
      // committed_bytes and a header slot, neither of which a record read
      // touches; it never frees the staging buffer, which the completion
      // path below defers behind release_pending flags; it never erases the
      // BlockState, which is what pins actually keep alive. Waiting for
      // pins here starved the flush instead: a block under steady read
      // traffic never shows a zero pin count, so its tail stayed dirty
      // indefinitely and shutdown could not drain the queue.
      StagingSlot& staging_state = store->staging_slots_[state->staging_slot_];
      const FixedBuffer buffer = StagingBufferFor(*store, *state);
      // Pad the tail out to a direct-I/O page and move the append cursor
      // past it. Every data page is then written exactly once, so a torn
      // write can never damage a record that is already durable.
      const std::uint32_t padded =
          static_cast<std::uint32_t>(AlignDirect(state->committed_bytes_));
      if (buffer.data_ == nullptr || buffer.size_ < padded ||
          staging_state.durable_bytes_ > state->committed_bytes_) {
        state->flush_queued_ = false;
        LatchRuntimeFailure(*store);
        store->flush_running_ = false;
        co_return absl::Status(absl::StatusCode::kInternal,
                               "invalid pending flush staging buffer");
      }
      if (padded == staging_state.durable_bytes_) {
        // Nothing new since the last flush. Rewriting the header would only
        // burn a slot and two fdatasyncs.
        state->flush_queued_ = false;
        if (!IsActiveBlock(*store, block_id)) {
          ReleaseStagingBuffer(*store, *state);
          MaybeQueueDefrag(*store, block_id);
        }
        continue;
      }
      std::fill_n(buffer.data_ + state->committed_bytes_,
                  padded - state->committed_bytes_, std::byte{0});
      state->committed_bytes_ = padded;
      staging_state.committed_bytes_ = padded;
      if (store->active_block_.has_value() &&
          store->active_block_->block_id_ == block_id) {
        store->active_block_->committed_bytes_ = padded;
      } else {
        for (auto& [generation, active] : store->active_tx_blocks_) {
          (void)generation;
          if (active.has_value() && active->block_id_ == block_id) {
            active->committed_bytes_ = padded;
            break;
          }
        }
      }
      ++staging_state.header_sequence_;
      const std::uint8_t slot = HeaderSlot(staging_state.header_sequence_);

      const BlockHeader header{
          .magic_ = kBlockMagic,
          .block_id_ = block_id,
          .version_ = kStorageFormatVersion,
          .header_bytes_ = kBlockHeaderBytes,
          .block_bytes_ = kStorageBlockBytes,
          .writer_id_ = state->writer_id_,
          .allocation_epoch_ = state->allocation_epoch_,
          .committed_bytes_ = padded,
          .record_count_ = staging_state.record_count_,
          .max_lsn_ = staging_state.max_lsn_,
          .header_sequence_ = staging_state.header_sequence_,
          .checksum_ = 0,
          .layout_worker_count_ = state->layout_worker_count_,
          .kind_ = state->kind_,
          .tx_generation_ = state->kind_ == BlockKind::kTransaction
                                ? store->tx_blocks_.at(block_id).generation_
                                : 0,
      };
      EncodeBlockHeader(header, std::span<std::byte, kBlockHeaderSlotBytes>(
                                    buffer.data_ + slot * kBlockHeaderSlotBytes,
                                    kBlockHeaderSlotBytes));

      pending.emplace(PendingFlush{
          .block_id_ = block_id,
          .committed_bytes_ = padded,
          .durable_bytes_ = staging_state.durable_bytes_,
          .allocation_epoch_ = state->allocation_epoch_,
          .write_buffer_id_ = staging_state.write_buffer_id_,
          .heap_data_ = staging_state.heap_data_,
          .heap_data_size_ = staging_state.heap_data_size_,
          .slot_ = slot,
          .staged_records_ = {},
      });
      if (auto found = store->staged_records_.find(block_id);
          found != store->staged_records_.end()) {
        pending->staged_records_ = std::move(found->second);
        store->staged_records_.erase(found);
      }
      state->flush_queued_ = false;
      state->flush_in_progress_ = true;
    }

    const auto [file_id, block_offset] = FileOffset(pending->block_id_);
    FixedBuffer staging =
        pending->write_buffer_id_ != 0
            ? store->buffers_.write_buffer(pending->write_buffer_id_)
            : FixedBuffer{.data_ = pending->heap_data_,
                          .size_ = pending->heap_data_size_,
                          .index_ = 0};
    LAVIK_FAULT_INJECT(
        // Deterministic regression hook for the dirty-tail ordering window: let
        // a command append beyond this immutable flush snapshot and roll to a
        // later block before the snapshot completes. Only the first flush in
        // the process pauses; ordinary release builds contain no hook.
        static std::atomic<bool> pause_claimed = false;
        const char* pause_text = std::getenv("LAVIK_FLUSH_SNAPSHOT_PAUSE_MS");
        bool expected_pause = false;
        if (pause_text != nullptr &&
            pause_claimed.compare_exchange_strong(expected_pause, true)) {
          char* end = nullptr;
          const unsigned long pause_ms = std::strtoul(pause_text, &end, 10);
          if (end != pause_text && *end == '\0' && pause_ms != 0) {
            absl::Status paused = co_await bycorf::SleepFor(
                *store->worker_, std::chrono::milliseconds(pause_ms));
            if (!paused.ok()) co_return paused;
          }
        });
    // The first flush of a block starts at the unused header slot, which is
    // still zero in staging. That makes the slot durably zero before the
    // first header lands in the other one, so a torn first header cannot
    // leave a stale header from this block's previous life as the winner.
    const std::size_t write_begin =
        pending->durable_bytes_ == kBlockHeaderBytes
            ? kBlockHeaderSlotBytes * (1 - pending->slot_)
            : pending->durable_bytes_;
    const std::size_t write_bytes = pending->committed_bytes_;
    if (staging.data_ == nullptr || staging.size_ < write_bytes) {
      co_await store->store_state_mutex_.Lock();
      UnlockGuard guard(&store->store_state_mutex_, store->worker_);
      BlockState* state = FindBlockState(*store, pending->block_id_);
      if (state != nullptr) {
        state->flush_in_progress_ = false;
        state->flush_queued_ = false;
      }
      LatchRuntimeFailure(*store);
      store->flush_running_ = false;
      co_return absl::Status(absl::StatusCode::kInternal,
                             "invalid pending flush staging buffer");
    }

    for (std::size_t write_offset = write_begin; write_offset < write_bytes;) {
      const std::size_t chunk_bytes =
          std::min(options_.flush_size_bytes_, write_bytes - write_offset);
      auto written = co_await WriteStorageBuffer(
          *store->worker_, store->files_[file_id],
          std::span<const std::byte>(staging.data_ + write_offset, chunk_bytes),
          pending->write_buffer_id_ != 0 &&
              store->buffers_.buffers_registered(),
          staging, block_offset + write_offset);
      if (!written.ok() || *written != chunk_bytes) {
        co_await store->store_state_mutex_.Lock();
        UnlockGuard guard(&store->store_state_mutex_, store->worker_);
        BlockState* state = FindBlockState(*store, pending->block_id_);
        if (state != nullptr) {
          state->flush_in_progress_ = false;
          state->flush_queued_ = false;
        }
        LatchRuntimeFailure(*store);
        store->flush_running_ = false;
        if (!written.ok()) {
          co_return written.status();
        }
        co_return absl::Status(absl::StatusCode::kInternal,
                               "short block flush write");
      }
      write_offset += chunk_bytes;
    }
    // The header is the block's commit record, so it must land strictly
    // after the data it describes is durable. Otherwise a crash between the
    // two can leave a header advertising records that were never written.
    auto fail_flush = [&](absl::Status status) -> Task<absl::Status> {
      co_await store->store_state_mutex_.Lock();
      UnlockGuard guard(&store->store_state_mutex_, store->worker_);
      BlockState* state = FindBlockState(*store, pending->block_id_);
      if (state != nullptr) {
        state->flush_in_progress_ = false;
        state->flush_queued_ = false;
      }
      LatchRuntimeFailure(*store);
      store->flush_running_ = false;
      co_return status;
    };

    auto synced =
        co_await bycorf::Fdatasync(*store->worker_, store->files_[file_id]);
    if (!synced.ok()) {
      co_return co_await fail_flush(synced);
    }

    const std::uint64_t slot_offset =
        block_offset + pending->slot_ * kBlockHeaderSlotBytes;
    auto header_written = co_await WriteStorageBuffer(
        *store->worker_, store->files_[file_id],
        std::span<const std::byte>(
            staging.data_ + pending->slot_ * kBlockHeaderSlotBytes,
            kBlockHeaderSlotBytes),
        pending->write_buffer_id_ != 0 && store->buffers_.buffers_registered(),
        staging, slot_offset);
    if (!header_written.ok() || *header_written != kBlockHeaderSlotBytes) {
      absl::Status header_status =
          header_written.ok() ? absl::Status(absl::StatusCode::kInternal,
                                             "short block header write")
                              : header_written.status();
      co_return co_await fail_flush(std::move(header_status));
    }
    synced =
        co_await bycorf::Fdatasync(*store->worker_, store->files_[file_id]);
    if (!synced.ok()) {
      co_return co_await fail_flush(synced);
    }

    co_await store->store_state_mutex_.Lock();
    UnlockGuard write_guard(&store->store_state_mutex_, store->worker_);
    BlockState* state = FindBlockState(*store, pending->block_id_);
    if (state == nullptr) {
      store->flush_running_ = false;
      co_return absl::OkStatus();
    }
    if (!state->allocated_ || state->flush_in_progress_ == false ||
        state->allocation_epoch_ != pending->allocation_epoch_) {
      state->flush_in_progress_ = false;
      state->flush_queued_ = false;
      store->flush_running_ = false;
      co_return absl::OkStatus();
    }

    // Every staged record in this snapshot is durable now (data pages and
    // header both fdatasync'd above), so the versions they superseded are no
    // longer anyone's durable copy and can leave their blocks' accounting.
    std::vector<RetiredRecord> retired_records;
    for (const RecordIdentity& identity : pending->staged_records_) {
      if (identity.retired_extents_ != nullptr) {
        SpawnExtentReclaim(*store, identity.retired_extents_);
      }
      if (identity.retired_record_.present_) {
        retired_records.push_back(identity.retired_record_.Materialize());
      }
      if (identity.tx_retirements_ != nullptr) {
        // The publication boundary is durable: either a transaction's commit
        // record or an ordinary root replacing a grouped graph. Its retained
        // child receipts can now leave physical accounting with that boundary.
        retired_records.insert(retired_records.end(),
                               identity.tx_retirements_->begin(),
                               identity.tx_retirements_->end());
      }
      if (identity.entry_address_ == 0) {
        continue;
      }
      // FLUSHDB detached the population this entry belongs to. The entry is
      // either already freed or waiting to be, and nothing reaches it either
      // way, so it must not be dereferenced.
      if (identity.index_generation_ !=
          store->index_generations_[identity.db_id_]) {
        continue;
      }
      // A TTL transition can replace and free the concrete Entry while this
      // physical record is staged. FindAddress compares integer addresses
      // with bucket slots; only a still-live member may be inspected below.
      // Address reuse is harmless because the physical boundary check then
      // applies to the replacement entry's own location.
      RecordIndex& index = PartitionFor(*store, identity.partition_id_)
                               .indexes_[identity.db_id_];
      RecordIndex::Entry* current_entry =
          index.FindAddress(identity.entry_address_, identity.entry_hash_);
      if (current_entry == nullptr) {
        continue;
      }
      RecordIndexValue& current = current_entry->value_;
      // The entry may no longer hold the version this identity was staged
      // for. Matching on block and epoch alone was enough when a block
      // flushed once: an overwrite necessarily landed in a different block.
      // With block reuse an overwrite racing this flush lands in the same
      // block above the snapshot boundary, and marking it flushed would
      // send readers to disk pages that are still zero. Offsets within one
      // allocation only grow, so the boundary check identifies stale
      // versions exactly.
      if (current.block_id() == pending->block_id_ &&
          current.record_offset() + current.total_disk_bytes() <=
              pending->committed_bytes_) {
        current.set_in_memory(false);
      }
    }

    if (!retired_records.empty()) {
      // Counted before the spawn so the shutdown drain can never observe
      // zero between this completion and the task's first slice.
      active_settlements_.fetch_add(1, std::memory_order_acq_rel);
      store->worker_->Spawn(
          MarkRetiredRecordsDead(store, std::move(retired_records)));
    }

    if (StagingSlot* slot = StagingFor(*store, *state); slot != nullptr) {
      slot->durable_bytes_ = pending->committed_bytes_;
    }
    state->flush_in_progress_ = false;
    state->flush_queued_ = false;

    // RequestFlush coalesces requests while an earlier snapshot is in
    // flight. If rollover or shutdown sealed the block during that write,
    // records may have been appended above the committed boundary before
    // active_block was cleared. Queue that tail now, before releasing the
    // staging buffer; otherwise shutdown can observe an empty queue and
    // report success while acknowledged records remain only in memory.
    if (!IsActiveBlock(*store, pending->block_id_) &&
        state->committed_bytes_ > pending->committed_bytes_) {
      // This tail contains records appended while the older snapshot was in
      // flight. It necessarily precedes every block already queued by that
      // append stream, so reinsert it at the front. Pushing it to the back can
      // make a later root durable before the COW child records it references.
      state->flush_queued_ = true;
      store->flush_queue_.push_front(pending->block_id_);
      continue;
    }

    // A block that is still the append stream's active block keeps its
    // staging buffer and stays in memory: the periodic flush only makes the
    // tail durable, it no longer retires the block. Sealing is what frees
    // the buffer, and sealing already cleared active_block by this point.
    if (IsActiveBlock(*store, pending->block_id_)) {
      continue;
    }

    state->in_memory_ = false;
    if (state->kind_ == BlockKind::kTransaction) {
      tx_cleaner_dirty_.store(true, std::memory_order_release);
    }
    if (state->pins_ > 0) {
      state->release_pending_ = true;
      continue;
    }
    ReleaseStagingBuffer(*store, *state);
    MaybeQueueDefrag(*store, pending->block_id_);
  }
}

bool StorageEngine::Impl::IsActiveBlock(const WorkerStore& store,
                                        std::uint64_t block_id) const noexcept {
  if (store.active_block_.has_value() &&
      store.active_block_->block_id_ == block_id) {
    return true;
  }
  for (const auto& [generation, active] : store.active_tx_blocks_) {
    (void)generation;
    if (active.has_value() && active->block_id_ == block_id) return true;
  }
  return false;
}

}  // namespace lavik::storage
