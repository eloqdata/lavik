#include "engine_impl.h"

namespace keylane::storage {

void StorageEngine::Impl::SpawnExtentReclaim(
    WorkerStore& store, std::shared_ptr<const std::vector<ExtentRef>> extents) {
  active_extent_reclaims_.fetch_add(1, std::memory_order_acq_rel);
  store.worker->Spawn(ReclaimExtentsCounted(&store, std::move(extents)));
}

Task<Status> StorageEngine::Impl::ReclaimExtentsCounted(
    WorkerStore* store, std::shared_ptr<const std::vector<ExtentRef>> extents) {
  struct ReclaimGuard {
    Impl* engine = nullptr;
    ~ReclaimGuard() {
      engine->active_extent_reclaims_.fetch_sub(1,
                                                std::memory_order_acq_rel);
    }
  } guard{this};
  co_return co_await ReclaimExtents(store, std::move(extents));
}

Task<StatusOr<bool>> StorageEngine::Impl::ReclaimExtentLocal(
    WorkerStore& store, ExtentRef ref) {
  while (true) {
    co_await store.writer_mutex.Lock();
    BlockState* state = FindBlockState(store, ref.block_id);
    if (state == nullptr || !state->allocated ||
        state->allocation_epoch != ref.allocation_epoch) {
      store.writer_mutex.Unlock(*store.worker);
      co_return false;
    }
    if (state->kind != BlockKind::kValueExtent) {
      store.writer_mutex.Unlock(*store.worker);
      co_return Status(StatusCode::kInternal,
                       "extent reclaim found a record block");
    }
    state->live_bytes = 0;
    if (state->pins != 0 || state->freeing) {
      store.writer_mutex.Unlock(*store.worker);
      Status waited = co_await celer::SleepFor(
          *store.worker, std::chrono::milliseconds(1));
      if (!waited.ok()) {
        co_return waited;
      }
      continue;
    }
    state->freeing = true;
    store.writer_mutex.Unlock(*store.worker);

    co_await store.writer_mutex.Lock();
    BlockState* current = FindBlockState(store, ref.block_id);
    bool freed = false;
    if (current != nullptr &&
        current->allocation_epoch == ref.allocation_epoch) {
      DestroyBlockState(store, ref.block_id);
      freed = true;
    }
    store.writer_mutex.Unlock(*store.worker);
    co_return freed;
  }
}

Task<Status> StorageEngine::Impl::ReclaimExtents(
    WorkerStore* store, std::shared_ptr<const std::vector<ExtentRef>> extents) {
  if (extents == nullptr) {
    co_return Status::Ok();
  }
  std::vector<std::uint64_t> released;
  released.reserve(extents->size());
  for (const ExtentRef& ref : *extents) {
    // Retiring an extent means touching its BlockState, which only its owner
    // may do. Looking it up locally instead used to find nothing and skip in
    // silence, leaking every extent block that had drifted to another owner;
    // nothing else reclaims them, since extent blocks are not defrag
    // candidates.
    const std::uint16_t owner = BlockOwner(ref.block_id);
    if (owner >= worker_count_) {
      continue;
    }
    // if/else, not ?:, to keep the two co_awaits in separate full
    // expressions (GCC coroutine frame-slot aliasing).
    StatusOr<bool> freed = false;
    if (owner == store->worker->id()) {
      freed = co_await ReclaimExtentLocal(*store, ref);
    } else {
      freed = co_await celer::SubmitTaskTo(
          owner, [this, owner, ref]() -> Task<StatusOr<bool>> {
            co_return co_await ReclaimExtentLocal(*stores_[owner], ref);
          });
    }
    if (!freed.ok()) {
      co_return freed.status();
    }
    if (*freed) {
      released.push_back(ref.block_id);
    }
  }
  Status returned = co_await ReturnColdBlocks(std::move(released));
  if (!returned.ok()) {
    co_return returned;
  }
  space_reclaim_generation_.fetch_add(1, std::memory_order_release);
  co_return Status::Ok();
}

bool StorageEngine::Impl::IsDefragCandidate(const WorkerStore& store,
                                            std::uint64_t block_id) const noexcept {
  const BlockState* state = FindBlockState(store, block_id);
  if (state == nullptr || !state->allocated || state->defrag_queued ||
      state->defragging || state->pins != 0 || state->in_memory ||
      state->kind != BlockKind::kRecords ||
      state->flush_queued || state->flush_in_progress ||
      IsActiveBlock(store, block_id) ||
      state->committed_bytes <= kBlockHeaderBytes) {
    return false;
  }
  const std::uint64_t used = state->committed_bytes - kBlockHeaderBytes;
  const std::uint64_t live_ratio =
      used == 0
          ? 0
          : (static_cast<std::uint64_t>(state->live_bytes) * 1000) / used;
  return live_ratio <= 500;
}

void StorageEngine::Impl::MaybeQueueDefrag(WorkerStore& store,
                                           std::uint64_t block_id) {
  BlockState* state = FindBlockState(store, block_id);
  if (state == nullptr || !IsDefragCandidate(store, block_id)) {
    return;
  }
  state->defrag_queued = true;
  store.defrag_queue.push_back(block_id);
  RequestDefrag(store);
}

bool StorageEngine::Impl::TryAcquireDefragPermit(std::size_t device_index) {
  std::atomic<unsigned>& device_active =
      active_defrags_by_device_[device_index];
  unsigned active = device_active.load(std::memory_order_acquire);
  while (active < kDefragPermitsPerDevice) {
    if (device_active.compare_exchange_weak(
            active, active + 1, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      active_defrags_.fetch_add(1, std::memory_order_acq_rel);
      return true;
    }
  }
  return false;
}

void StorageEngine::Impl::ReleaseDefragPermit(std::size_t device_index) {
  active_defrags_by_device_[device_index].fetch_sub(
      1, std::memory_order_acq_rel);
  active_defrags_.fetch_sub(1, std::memory_order_acq_rel);
}

Task<Status> StorageEngine::Impl::StartQueuedDefrag(unsigned worker_id,
                                                    std::size_t device_index) {
  WorkerStore& store = *stores_[worker_id];
  if (!store.defrag_waiting ||
      store.defrag_waiting_device != device_index) {
    ReleaseDefragPermit(device_index);
    co_return Status::Ok();
  }
  store.defrag_waiting = false;
  if (shutdown_flush_requested_.load(std::memory_order_acquire) ||
      store.defrag_running || store.defrag_queue.empty() ||
      DeviceIndexForBlock(store.defrag_queue.front()) != device_index) {
    ReleaseDefragPermit(device_index);
    RequestDefrag(store);
    co_return Status::Ok();
  }
  store.defrag_running = true;
  store.active_defrag_device = device_index;
  store.worker->SpawnBackground(DefragOne(&store));
  co_return Status::Ok();
}

Task<Status> StorageEngine::Impl::WakeQueuedDefrags(std::size_t device_index) {
  while (TryAcquireDefragPermit(device_index)) {
    std::uint16_t worker_id = 0;
    if (!defrag_ready_by_device_[device_index].try_dequeue(worker_id)) {
      ReleaseDefragPermit(device_index);
      co_return Status::Ok();
    }
    pending_defrags_.fetch_sub(1, std::memory_order_acq_rel);
    Status started;
    if (worker_id == celer::ThisWorker().id) {
      started = co_await StartQueuedDefrag(worker_id, device_index);
    } else {
      started = co_await celer::SubmitTaskTo(
          worker_id, [this, worker_id, device_index]() -> Task<Status> {
            co_return co_await StartQueuedDefrag(worker_id, device_index);
          });
    }
    if (!started.ok()) {
      ReleaseDefragPermit(device_index);
      co_return started;
    }
  }
  co_return Status::Ok();
}

void StorageEngine::Impl::RequestDefrag(WorkerStore& store) {
  if (shutdown_flush_requested_.load(std::memory_order_acquire) ||
      store.defrag_running || store.defrag_waiting ||
      store.defrag_queue.empty()) {
    return;
  }
  const std::size_t device_index =
      DeviceIndexForBlock(store.defrag_queue.front());
  store.defrag_waiting = true;
  store.defrag_waiting_device = device_index;
  pending_defrags_.fetch_add(1, std::memory_order_acq_rel);
  if (!defrag_ready_by_device_[device_index].enqueue(store.worker->id())) {
    pending_defrags_.fetch_sub(1, std::memory_order_acq_rel);
    store.defrag_waiting = false;
    spdlog::error(
        "worker[{}] failed to enqueue defrag request for device {}",
        store.worker->id(), devices_[device_index].id);
    return;
  }
  store.worker->SpawnBackground(WakeQueuedDefrags(device_index));
}

void StorageEngine::Impl::FinishDefragPass(WorkerStore& store) {
  const std::size_t completed_device = store.active_defrag_device;
  store.defrag_running = false;
  // Queue this worker's next candidate before releasing the active permit,
  // so foreground allocation never observes a false no-reclaim gap.
  RequestDefrag(store);
  space_reclaim_generation_.fetch_add(1, std::memory_order_release);
  ReleaseDefragPermit(completed_device);
  store.worker->SpawnBackground(WakeQueuedDefrags(completed_device));
}

Task<Status> StorageEngine::Impl::DefragOne(WorkerStore* store) {
  if (store->defrag_queue.empty()) {
    FinishDefragPass(*store);
    co_return Status::Ok();
  }
  const std::uint64_t candidate = store->defrag_queue.front();
  store->defrag_queue.pop_front();
  BlockState* candidate_state = FindBlockState(*store, candidate);
  if (candidate_state == nullptr) {
    FinishDefragPass(*store);
    co_return Status::Ok();
  }
  candidate_state->defrag_queued = false;

  Status status = co_await CleanBlockLocked(*store, candidate);
  if (!status.ok()) {
    spdlog::error("worker[{}] defrag block {} failed: {}",
                  store->worker->id(), candidate, status.message());
  }
  if (status.code() == StatusCode::kResourceExhausted) {
    MaybeQueueDefrag(*store, candidate);
  }
  FinishDefragPass(*store);
  co_return status;
}

Task<StatusOr<std::optional<RelocationDurabilityFence>>>
StorageEngine::Impl::RelocateIfCurrent(
    unsigned key_owner, std::string_view key, std::string_view value,
    const RecordHeader& record, const RecordLocation& source_location) {
  WorkerStore& key_store = *stores_[key_owner];
  // Always dispatched to key_owner's thread (see the defrag loop), so the
  // current worker's TxShard is the right lock authority.
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      record.db_id, tx::FingerprintOf(record.digest),
      tx::LockMode::kExclusive);
  co_await key_store.writer_mutex.Lock();
  UnlockGuard write_unlock(&key_store.writer_mutex, key_store.worker);

  auto& partition = PartitionForKey(key_store, key);
  auto& index = partition.indexes[record.db_id];
  auto* current = index.Find(record.digest, key);
  if (current == nullptr ||
      !current->value.SamePhysicalRecord(source_location)) {
    co_return std::optional<RelocationDurabilityFence>{};
  }

  RecordLocation relocated;
  Status written = co_await WriteRecordLocked(
      key_store, record.db_id, key, value, record.kind, record.value_type,
      record.expire_at_ms, record.digest, record.generation,
      record.mutation_sequence,
      record.relocation_sequence + 1, true, true, record.external,
      record.logical_size, source_location.extents, &relocated);
  if (!written.ok()) {
    co_return written;
  }
  co_return std::optional<RelocationDurabilityFence>(
      RelocationDurabilityFence{
          .block_id = relocated.block_id,
          .allocation_epoch = relocated.allocation_epoch,
          .block_owner = relocated.block_owner,
          .committed_bytes = static_cast<std::uint32_t>(
              relocated.record_offset + relocated.total_disk_bytes),
      });
}

Task<Status> StorageEngine::Impl::AwaitRelocationDurableLocal(
    WorkerStore& store, const RelocationDurabilityFence& fence) {
  while (true) {
    co_await store.writer_mutex.Lock();
    bool durable = false;
    bool failed = false;
    {
      UnlockGuard unlock(&store.writer_mutex, store.worker);
      BlockState* state = FindBlockState(store, fence.block_id);
      if (state == nullptr || !state->allocated ||
          state->allocation_epoch != fence.allocation_epoch) {
        // The only paths that destroy or reuse a committed block first make
        // every record they retire durable elsewhere (or durably invalidate
        // the whole DB/partition). That guarantee is transitive across a
        // chain of defrag relocations.
        durable = true;
      } else if (StagingSlot* staging = StagingFor(store, *state);
                 staging == nullptr) {
        // A records block loses its staging buffer only after its committed
        // header is durable.
        durable = !state->in_memory && !state->flush_in_progress;
      } else if (staging->durable_bytes >= fence.committed_bytes) {
        durable = true;
      } else {
        RequestFlush(store, fence.block_id);
      }
      failed = store.write_failed;
    }
    if (failed) {
      co_return Status(StatusCode::kInternal,
                       "storage write failed while flushing defrag relocation");
    }
    if (durable) {
      co_return Status::Ok();
    }
    Status waited = co_await celer::SleepFor(
        *store.worker, std::chrono::milliseconds(1));
    if (!waited.ok()) {
      co_return waited;
    }
  }
}

Task<Status> StorageEngine::Impl::AwaitRelocationDurable(
    const RelocationDurabilityFence& fence) {
  if (fence.block_owner >= worker_count_) {
    co_return Status(StatusCode::kInternal,
                     "defrag relocation has an invalid block owner");
  }
  WorkerStore& owner = *stores_[fence.block_owner];
  if (fence.block_owner == celer::ThisWorker().id) {
    co_return co_await AwaitRelocationDurableLocal(owner, fence);
  }
  co_return co_await celer::SubmitTaskTo(
      fence.block_owner,
      [this, fence]() -> Task<Status> {
        co_return co_await AwaitRelocationDurableLocal(
            *stores_[fence.block_owner], fence);
      });
}

Task<Status> StorageEngine::Impl::CleanBlockLocked(WorkerStore& store,
                                                   std::uint64_t block_id) {
  BlockState* source_ptr = FindBlockState(store, block_id);
  if (source_ptr == nullptr) {
    co_return Status::Ok();
  }
  BlockState& source = *source_ptr;
  if (!source.allocated || source.in_memory || source.pins != 0 ||
      source.flush_queued || source.flush_in_progress ||
      IsActiveBlock(store, block_id)) {
    co_return Status::Ok();
  }
  source.defragging = true;
  const auto [source_file_id, source_block_offset] = FileOffset(block_id);

  // live_bytes counts exactly the index entries naming this block, so zero
  // means nothing here is reachable and the salvage pass is a provable no-op:
  // every record it decoded would find its key either absent from the index
  // or pointing at a different physical record, so RelocateIfCurrent would
  // rewrite nothing and the live_bytes re-check below would still see zero.
  // Skipping reaches the same free path under the same precondition, without
  // reading 8 MiB and CRC-checking every record in it. FLUSHDB empties whole
  // blocks at once, which is where this dominates.
  if (source.live_bytes != 0) {
    Status salvaged = co_await SalvageBlockRecords(
        store, block_id, source, source_file_id, source_block_offset);
    if (!salvaged.ok()) {
      co_return salvaged;
    }
  }

  {
    co_await store.writer_mutex.Lock();
    UnlockGuard write_unlock(&store.writer_mutex, store.worker);
    if (source.live_bytes != 0) {
      source.defragging = false;
      co_return Status::Ok();
    }
    source.freeing = true;
  }
  co_return co_await ReleaseEmptyBlock(store, block_id, source);
}

Task<Status> StorageEngine::Impl::SalvageBlockRecords(WorkerStore& store,
                                                      std::uint64_t block_id,
                                                      BlockState& source,
                                                      std::uint32_t source_file_id,
                                                      std::uint64_t source_block_offset) {
  struct DefragBuffer {
    RegisteredBufferPool* pool = nullptr;
    std::uint16_t buffer_id = 0;
    std::byte* heap_data = nullptr;
    FixedBuffer buffer{};

    ~DefragBuffer() {
      if (buffer_id != 0) {
        pool->ReleaseWriteBuffer(buffer_id);
      } else if (heap_data != nullptr) {
        pool->ReleaseHeapWriteBuffer(heap_data);
      }
    }

    bool registered() const noexcept { return buffer_id != 0; }
  } block_data{.pool = &store.buffers};
  if (store.buffers.TryAcquireWriteBuffer(&block_data.buffer_id)) {
    block_data.buffer = store.buffers.write_buffer(block_data.buffer_id);
  } else if (store.buffers.TryAcquireHeapWriteBuffer(&block_data.heap_data)) {
    block_data.buffer = FixedBuffer{
        .data = block_data.heap_data,
        .size = options_.buffers.write_buffer_bytes,
        .index = 0,
    };
  } else {
    source.defragging = false;
    co_return Status(StatusCode::kResourceExhausted,
                     "failed to allocate defrag block buffer");
  }
  if (block_data.buffer.size < kStorageBlockBytes) {
    source.defragging = false;
    co_return Status(StatusCode::kResourceExhausted,
                     "defrag block buffer is smaller than a storage block");
  }
  block_data.buffer.size = kStorageBlockBytes;
  auto read = co_await ReadStorageBuffer(
      *store.worker, store.files[source_file_id], block_data.buffer,
      block_data.registered(), source_block_offset);
  if (!read.ok() || *read != kStorageBlockBytes) {
    source.defragging = false;
    co_return read.ok()
                  ? Status(StatusCode::kInternal,
                           "short block read during defrag")
                  : read.status();
  }

  std::vector<RelocationDurabilityFence> durability_fences;
  std::uint32_t record_offset = kBlockHeaderBytes;
  while (record_offset < source.committed_bytes) {
    const std::optional<std::uint32_t> next = NextRecordOffset(
        block_data.buffer.data, record_offset, source.committed_bytes);
    if (!next.has_value()) {
      source.defragging = false;
      co_return Status(StatusCode::kInternal,
                       "corrupt committed record during defrag");
    }
    if (*next != record_offset) {
      record_offset = *next;
      continue;
    }
    RecordHeader record{};
    std::string_view disk_key;
    std::span<const std::byte> record_bytes(
        block_data.buffer.data + record_offset,
        source.committed_bytes - record_offset);
    if (!DecodeRecordHeader(record_bytes, &record, &disk_key) ||
        record.allocation_epoch != source.allocation_epoch ||
        record_offset + record.total_disk_bytes > source.committed_bytes) {
      source.defragging = false;
      co_return Status(StatusCode::kInternal,
                       "corrupt committed record during defrag");
    }

    RecordLocation source_location{
        .block_id = block_id,
        .replication_epoch = record.replication_epoch,
        .mutation_sequence = record.mutation_sequence,
        .allocation_epoch = record.allocation_epoch,
        .expire_at_ms = record.expire_at_ms,
        .block_owner = store.worker->id(),
        .record_offset = record_offset,
        .total_disk_bytes = record.total_disk_bytes,
        .logical_size = record.logical_size,
        .payload_bytes = record.payload_bytes,
        .relocation_sequence = static_cast<std::uint32_t>(
            record.relocation_sequence),
        .external = record.external,
        .kind = record.kind,
        .value_type = record.value_type,
        .extents = {},
    };
    const std::byte* value_data =
        block_data.buffer.data + record_offset + record.header_bytes;
    if (Crc32c(std::span<const std::byte>(value_data,
                                         record.payload_bytes)) !=
        record.payload_checksum) {
      source.defragging = false;
      co_return Status(StatusCode::kInternal,
                       "value checksum mismatch during defrag");
    }
    if (record.external) {
      auto decoded = DecodeManifest(
          std::span<const std::byte>(value_data, record.payload_bytes),
          record.logical_size);
      if (!decoded.ok()) {
        source.defragging = false;
        co_return decoded.status();
      }
      source_location.extents = std::move(*decoded);
    }

    const unsigned key_owner = OwnerForKey(disk_key);
    const std::string key(disk_key);
    const std::string value(reinterpret_cast<const char*>(value_data),
                            record.payload_bytes);
    StatusOr<std::optional<RelocationDurabilityFence>> relocated(
        std::optional<RelocationDurabilityFence>{});
    if (key_owner == store.worker->id()) {
      relocated = co_await RelocateIfCurrent(
          key_owner, key, value, record, source_location);
    } else {
      relocated = co_await celer::SubmitTaskTo(
          key_owner,
          [this, key_owner, key, value, record,
           source_location]() mutable
              -> Task<StatusOr<std::optional<
                  RelocationDurabilityFence>>> {
            co_return co_await RelocateIfCurrent(
                key_owner, key, value, record, source_location);
          });
    }
    if (!relocated.ok()) {
      source.defragging = false;
      co_return relocated.status();
    }
    if (relocated->has_value()) {
      const RelocationDurabilityFence& fence = **relocated;
      auto existing = std::find_if(
          durability_fences.begin(), durability_fences.end(),
          [&](const RelocationDurabilityFence& candidate) {
            return candidate.block_owner == fence.block_owner &&
                   candidate.block_id == fence.block_id &&
                   candidate.allocation_epoch == fence.allocation_epoch;
          });
      if (existing == durability_fences.end()) {
        durability_fences.push_back(fence);
      } else {
        existing->committed_bytes =
            std::max(existing->committed_bytes, fence.committed_bytes);
      }
    }
    record_offset += record.total_disk_bytes;
    // Cheap while the 50 us background slice has budget remaining; once it
    // expires this defers the cleaner to the next scheduler round so online
    // I/O and cross-core work are polled first.
    co_await celer::Yield(*store.worker);
  }
  // Do not clear the source block's allocation bitmap bit until every newly
  // written destination record is covered by a durable destination header.
  // If the process dies while waiting, recovery still scans the source; if it
  // dies afterwards, recovery skips it or selects the higher-sequence
  // relocation if the bitmap update had not become durable yet.
  for (const RelocationDurabilityFence& fence : durability_fences) {
    Status durable = co_await AwaitRelocationDurable(fence);
    if (!durable.ok()) {
      source.defragging = false;
      co_return durable;
    }
  }
  co_return Status::Ok();
}

Task<Status> StorageEngine::Impl::ReleaseEmptyBlock(WorkerStore& store,
                                                    std::uint64_t block_id,
                                                    BlockState& source) {
  while (source.pins != 0) {
    Status waited = co_await celer::SleepFor(
        *store.worker, std::chrono::milliseconds(1));
    if (!waited.ok()) {
      source.freeing = false;
      source.defragging = false;
      co_return waited;
    }
  }

  // The durable allocation bitmap is authoritative during recovery. Retire
  // the runtime state before publishing the block to cold_free so another
  // worker cannot allocate it while its old owner still names it. A bitmap
  // write failure fail-stops the allocator, so this block cannot be reused
  // in the ambiguous state.
  DestroyBlockState(store, block_id);
  Status returned = co_await ReturnColdBlocks({block_id});
  if (returned.ok()) {
    // The source's cleared allocation bit is now durable while its stale
    // records are still on disk — the exact window the relocation durability
    // fence exists to protect. Crash-safety tests arm this point.
    KEYLANE_MAYBE_CRASH_AT("defrag-source-retired");
  }
  co_return returned;
}

}  // namespace keylane::storage
