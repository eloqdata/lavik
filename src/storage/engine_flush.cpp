#include "engine_impl.h"

namespace keylane::storage {

Task<Status> StorageEngine::Impl::PeriodicFlush(WorkerStore* store) {
  const auto interval =
      std::chrono::milliseconds(options_.flush_max_ms);
  while (!store->worker->stop_requested()) {
    Status status = co_await celer::SleepFor(*store->worker, interval);
    if (!status.ok()) {
      CompleteShutdownFlush(status);
      co_return status;
    }
    if (store->worker->stop_requested()) {
      break;
    }

    if (shutdown_flush_requested_.load(std::memory_order_acquire)) {
      status = co_await FlushWorkerForShutdown(store);
      CompleteShutdownFlush(status);
      co_return status;
    }

    co_await store->writer_mutex.Lock();
    UnlockGuard guard(&store->writer_mutex, store->worker);
    FlushActiveBlock(*store);
  }
  co_return Status::Ok();
}

void StorageEngine::Impl::RequestFlush(WorkerStore& store,
                                       std::uint64_t block_id) {
  BlockState* state = FindBlockState(store, block_id);
  if (state == nullptr || !state->allocated || !state->in_memory ||
      state->staging_slot == 0) {
    return;
  }
  if (state->flush_queued || state->flush_in_progress) {
    return;
  }
  state->flush_queued = true;
  store.flush_queue.push_back(block_id);
  if (store.flush_running) {
    return;
  }
  store.flush_running = true;
  active_flushes_.fetch_add(1, std::memory_order_acq_rel);
  store.worker->Spawn(FlushPendingBlocks(&store));
}

Task<Status> StorageEngine::Impl::FlushPendingBlocks(WorkerStore* store) {
  struct FlushRunGuard {
    Impl* engine = nullptr;
    ~FlushRunGuard() {
      engine->space_reclaim_generation_.fetch_add(
          1, std::memory_order_release);
      engine->active_flushes_.fetch_sub(1, std::memory_order_acq_rel);
    }
  } flush_run_guard{this};

  struct PendingFlush {
    std::uint64_t block_id = 0;
    std::uint32_t committed_bytes = 0;
    std::uint32_t durable_bytes = 0;
    std::uint64_t allocation_epoch = 0;
    std::uint16_t write_buffer_id = 0;
    std::byte* heap_data = nullptr;
    std::size_t heap_data_size = 0;
    std::uint8_t slot = 0;
    std::vector<RecordIdentity> staged_records;
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
      co_await store->writer_mutex.Lock();
      UnlockGuard guard(&store->writer_mutex, store->worker);

      if (store->flush_queue.empty()) {
        store->flush_running = false;
        co_return Status::Ok();
      }

      const std::uint64_t block_id = store->flush_queue.front();
      store->flush_queue.pop_front();
      BlockState* state = FindBlockState(*store, block_id);
      if (state == nullptr) {
        continue;
      }
      if (!state->allocated || !state->in_memory ||
          state->staging_slot == 0 || state->flush_in_progress) {
        state->flush_queued = false;
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
      StagingSlot& staging_state = store->staging_slots[state->staging_slot];
      const FixedBuffer buffer = StagingBufferFor(*store, *state);
      // Pad the tail out to a direct-I/O page and move the append cursor
      // past it. Every data page is then written exactly once, so a torn
      // write can never damage a record that is already durable.
      const std::uint32_t padded =
          static_cast<std::uint32_t>(AlignDirect(state->committed_bytes));
      if (buffer.data == nullptr || buffer.size < padded ||
          staging_state.durable_bytes > state->committed_bytes) {
        state->flush_queued = false;
        store->write_failed = true;
        store->flush_running = false;
        co_return Status(StatusCode::kInternal,
                         "invalid pending flush staging buffer");
      }
      if (padded == staging_state.durable_bytes) {
        // Nothing new since the last flush. Rewriting the header would only
        // burn a slot and two fdatasyncs.
        state->flush_queued = false;
        if (!IsActiveBlock(*store, block_id)) {
          ReleaseStagingBuffer(*store, *state);
          MaybeQueueDefrag(*store, block_id);
        }
        continue;
      }
      std::fill_n(buffer.data + state->committed_bytes,
                  padded - state->committed_bytes, std::byte{0});
      state->committed_bytes = padded;
      if (store->active_block.has_value() &&
          store->active_block->block_id == block_id) {
        store->active_block->committed_bytes = padded;
      }
      ++staging_state.header_sequence;
      const std::uint8_t slot = HeaderSlot(staging_state.header_sequence);

      const BlockHeader header{
          .magic = kBlockMagic,
          .block_id = block_id,
          .version = kStorageFormatVersion,
          .header_bytes = kBlockHeaderBytes,
          .block_bytes = kStorageBlockBytes,
          .writer_id = state->writer_id,
          .allocation_epoch = state->allocation_epoch,
          .committed_bytes = padded,
          .record_count = staging_state.record_count,
          .max_lsn = staging_state.max_lsn,
          .header_sequence = staging_state.header_sequence,
          .checksum = 0,
          .layout_worker_count = state->layout_worker_count,
      };
      EncodeBlockHeader(
          header, std::span<std::byte, kBlockHeaderSlotBytes>(
                      buffer.data + slot * kBlockHeaderSlotBytes,
                      kBlockHeaderSlotBytes));

      pending.emplace(PendingFlush{
          .block_id = block_id,
          .committed_bytes = padded,
          .durable_bytes = staging_state.durable_bytes,
          .allocation_epoch = state->allocation_epoch,
          .write_buffer_id = staging_state.write_buffer_id,
          .heap_data = staging_state.heap_data,
          .heap_data_size = staging_state.heap_data_size,
          .slot = slot,
          .staged_records = {},
      });
      if (auto found = store->staged_records.find(block_id);
          found != store->staged_records.end()) {
        pending->staged_records = std::move(found->second);
        store->staged_records.erase(found);
      }
      state->flush_queued = false;
      state->flush_in_progress = true;
    }

    const auto [file_id, block_offset] =
        FileOffset(pending->block_id);
    FixedBuffer staging = pending->write_buffer_id != 0
                             ? store->buffers.write_buffer(
                                   pending->write_buffer_id)
                             : FixedBuffer{.data = pending->heap_data,
                                          .size = pending->heap_data_size,
                                          .index = 0};
    // The first flush of a block starts at the unused header slot, which is
    // still zero in staging. That makes the slot durably zero before the
    // first header lands in the other one, so a torn first header cannot
    // leave a stale header from this block's previous life as the winner.
    const std::size_t write_begin =
        pending->durable_bytes == kBlockHeaderBytes
            ? kBlockHeaderSlotBytes * (1 - pending->slot)
            : pending->durable_bytes;
    const std::size_t write_bytes = pending->committed_bytes;
    if (staging.data == nullptr || staging.size < write_bytes) {
      co_await store->writer_mutex.Lock();
      UnlockGuard guard(&store->writer_mutex, store->worker);
      BlockState* state = FindBlockState(*store, pending->block_id);
      if (state != nullptr) {
        state->flush_in_progress = false;
        state->flush_queued = false;
      }
      store->write_failed = true;
      store->flush_running = false;
      co_return Status(StatusCode::kInternal,
                       "invalid pending flush staging buffer");
    }

    for (std::size_t write_offset = write_begin;
         write_offset < write_bytes;) {
      const std::size_t chunk_bytes =
          std::min(options_.flush_size_bytes, write_bytes - write_offset);
      auto written = co_await WriteStorageBuffer(
          *store->worker, store->files[file_id],
          std::span<const std::byte>(staging.data + write_offset, chunk_bytes),
          pending->write_buffer_id != 0, staging,
          block_offset + write_offset);
      if (!written.ok() || *written != chunk_bytes) {
        co_await store->writer_mutex.Lock();
        UnlockGuard guard(&store->writer_mutex, store->worker);
        BlockState* state = FindBlockState(*store, pending->block_id);
        if (state != nullptr) {
          state->flush_in_progress = false;
          state->flush_queued = false;
        }
        store->write_failed = true;
        store->flush_running = false;
        if (!written.ok()) {
          co_return written.status();
        }
        co_return Status(StatusCode::kInternal,
                         "short block flush write");
      }
      write_offset += chunk_bytes;
    }
    // The header is the block's commit record, so it must land strictly
    // after the data it describes is durable. Otherwise a crash between the
    // two can leave a header advertising records that were never written.
    auto fail_flush = [&](Status status) -> Task<Status> {
      co_await store->writer_mutex.Lock();
      UnlockGuard guard(&store->writer_mutex, store->worker);
      BlockState* state = FindBlockState(*store, pending->block_id);
      if (state != nullptr) {
        state->flush_in_progress = false;
        state->flush_queued = false;
      }
      store->write_failed = true;
      store->flush_running = false;
      co_return status;
    };

    auto synced = co_await celer::Fdatasync(*store->worker,
                                            store->files[file_id]);
    if (!synced.ok()) {
      co_return co_await fail_flush(synced);
    }

    const std::uint64_t slot_offset =
        block_offset + pending->slot * kBlockHeaderSlotBytes;
    auto header_written = co_await WriteStorageBuffer(
        *store->worker, store->files[file_id],
        std::span<const std::byte>(
            staging.data + pending->slot * kBlockHeaderSlotBytes,
            kBlockHeaderSlotBytes),
        pending->write_buffer_id != 0, staging, slot_offset);
    if (!header_written.ok() || *header_written != kBlockHeaderSlotBytes) {
      co_return co_await fail_flush(
          header_written.ok()
              ? Status(StatusCode::kInternal, "short block header write")
              : header_written.status());
    }
    synced = co_await celer::Fdatasync(*store->worker, store->files[file_id]);
    if (!synced.ok()) {
      co_return co_await fail_flush(synced);
    }

    co_await store->writer_mutex.Lock();
    UnlockGuard write_guard(&store->writer_mutex, store->worker);
    BlockState* state = FindBlockState(*store, pending->block_id);
    if (state == nullptr) {
      store->flush_running = false;
      co_return Status::Ok();
    }
    if (!state->allocated || state->flush_in_progress == false ||
        state->allocation_epoch != pending->allocation_epoch) {
      state->flush_in_progress = false;
      state->flush_queued = false;
      store->flush_running = false;
      co_return Status::Ok();
    }

    // Every staged record in this snapshot is durable now (data pages and
    // header both fdatasync'd above), so the versions they superseded are no
    // longer anyone's durable copy and can leave their blocks' accounting.
    std::vector<RetiredRecord> retired_records;
    for (const RecordIdentity& identity : pending->staged_records) {
      if (identity.retired_extents != nullptr) {
        SpawnExtentReclaim(*store, identity.retired_extents);
      }
      if (identity.retired_record.has_value()) {
        retired_records.push_back(*identity.retired_record);
      }
      if (identity.entry == nullptr) {
        continue;
      }
      // FLUSHDB detached the population this entry belongs to. The entry is
      // either already freed or waiting to be, and nothing reaches it either
      // way, so it must not be dereferenced.
      if (identity.index_generation !=
          store->index_generations[identity.db_id]) {
        continue;
      }
      RecordLocation& current = identity.entry->value;
      // The entry may no longer hold the version this identity was staged
      // for. Matching on block and epoch alone was enough when a block
      // flushed once: an overwrite necessarily landed in a different block.
      // With block reuse an overwrite racing this flush lands in the same
      // block above the snapshot boundary, and marking it flushed would
      // send readers to disk pages that are still zero. Offsets within one
      // allocation only grow, so the boundary check identifies stale
      // versions exactly.
      // The entry may no longer hold the version this identity was staged
      // for. Matching on block and epoch alone was enough when a block
      // flushed once: an overwrite necessarily landed in a different block.
      // With block reuse an overwrite racing this flush lands in the same
      // block above the snapshot boundary, and marking it flushed would
      // send readers to disk pages that are still zero. Offsets within one
      // allocation only grow, so the boundary check identifies stale
      // versions exactly.
      if (current.block_id == pending->block_id &&
          current.allocation_epoch == pending->allocation_epoch &&
          current.record_offset + current.total_disk_bytes <=
              pending->committed_bytes) {
        current.in_memory = false;
      }
    }

    if (!retired_records.empty()) {
      store->worker->Spawn(
          MarkRetiredRecordsDead(store, std::move(retired_records)));
    }

    if (StagingSlot* slot = StagingFor(*store, *state); slot != nullptr) {
      slot->durable_bytes = pending->committed_bytes;
    }
    state->flush_in_progress = false;
    state->flush_queued = false;

    // RequestFlush coalesces requests while an earlier snapshot is in
    // flight. If rollover or shutdown sealed the block during that write,
    // records may have been appended above the committed boundary before
    // active_block was cleared. Queue that tail now, before releasing the
    // staging buffer; otherwise shutdown can observe an empty queue and
    // report success while acknowledged records remain only in memory.
    if (!IsActiveBlock(*store, pending->block_id) &&
        state->committed_bytes > pending->committed_bytes) {
      RequestFlush(*store, pending->block_id);
      continue;
    }

    // A block that is still the append stream's active block keeps its
    // staging buffer and stays in memory: the periodic flush only makes the
    // tail durable, it no longer retires the block. Sealing is what frees
    // the buffer, and sealing already cleared active_block by this point.
    if (IsActiveBlock(*store, pending->block_id)) {
      continue;
    }

    state->in_memory = false;
    if (state->pins > 0) {
      state->release_pending = true;
      continue;
    }
    ReleaseStagingBuffer(*store, *state);
    MaybeQueueDefrag(*store, pending->block_id);
  }
}

bool StorageEngine::Impl::IsActiveBlock(const WorkerStore& store,
                                        std::uint64_t block_id) const noexcept {
  return store.active_block.has_value() &&
         store.active_block->block_id == block_id;
}

}  // namespace keylane::storage
