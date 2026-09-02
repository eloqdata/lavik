#include "impl.h"
#include "keylane/metrics.h"

namespace keylane::storage {

void StorageEngine::Impl::SpawnExtentReclaim(
    WorkerStore& store, std::shared_ptr<const std::vector<ExtentRef>> extents) {
  active_extent_reclaims_.fetch_add(1, std::memory_order_acq_rel);
  store.worker_->Spawn(ReclaimExtentsCounted(&store, std::move(extents)));
}

Task<absl::Status> StorageEngine::Impl::ReclaimExtentsCounted(
    WorkerStore* store, std::shared_ptr<const std::vector<ExtentRef>> extents) {
  struct ReclaimGuard {
    Impl* engine_ = nullptr;
    ~ReclaimGuard() {
      engine_->active_extent_reclaims_.fetch_sub(1, std::memory_order_acq_rel);
    }
  } guard{this};
  absl::Status status = co_await ReclaimExtents(store, std::move(extents));
  if (!status.ok()) {
    // Most callers detach this task and have nobody to receive its result; the
    // failure must therefore fail-stop storage rather than masquerade as
    // reusable capacity.
    store->write_failed_ = true;
    spdlog::error("worker[{}] extent reclaim failed: {}", store->worker_->id(),
                  status.message());
  }
  co_return status;
}

Task<absl::StatusOr<bool>> StorageEngine::Impl::ReclaimExtentLocal(
    WorkerStore& store, ExtentRef ref) {
  while (true) {
    co_await store.store_state_mutex_.Lock();
    BlockState* state = FindBlockState(store, ref.block_id_);
    if (state == nullptr || !state->allocated_ ||
        state->allocation_epoch_ != ref.allocation_epoch_) {
      store.store_state_mutex_.Unlock(*store.worker_);
      co_return false;
    }
    if (state->kind_ != BlockKind::kPayloadExtent) {
      store.store_state_mutex_.Unlock(*store.worker_);
      co_return absl::Status(absl::StatusCode::kInternal,
                             "extent reclaim found a record block");
    }
    state->live_bytes_ = 0;
    if (state->pins_ != 0 || state->freeing_) {
      store.store_state_mutex_.Unlock(*store.worker_);
      absl::Status waited = co_await celer::SleepFor(
          *store.worker_, std::chrono::milliseconds(1));
      if (!waited.ok()) {
        co_return waited;
      }
      continue;
    }
    state->freeing_ = true;
    store.store_state_mutex_.Unlock(*store.worker_);

    co_await store.store_state_mutex_.Lock();
    BlockState* current = FindBlockState(store, ref.block_id_);
    bool freed = false;
    if (current != nullptr &&
        current->allocation_epoch_ == ref.allocation_epoch_) {
      DestroyBlockState(store, ref.block_id_);
      freed = true;
    }
    store.store_state_mutex_.Unlock(*store.worker_);
    co_return freed;
  }
}

Task<absl::Status> StorageEngine::Impl::ReclaimExtents(
    WorkerStore* store, std::shared_ptr<const std::vector<ExtentRef>> extents) {
  if (extents == nullptr) {
    co_return absl::OkStatus();
  }
  std::vector<std::uint64_t> released;
  released.reserve(extents->size());
  for (const ExtentRef& ref : *extents) {
    // Retiring an extent means touching its BlockState, which only its owner
    // may do. Looking it up locally instead used to find nothing and skip in
    // silence, leaking every extent block that had drifted to another owner;
    // nothing else reclaims them, since extent blocks are not defrag
    // candidates.
    const std::uint16_t owner = BlockOwner(ref.block_id_);
    if (owner >= worker_count_) {
      continue;
    }
    // if/else, not ?:, to keep the two co_awaits in separate full
    // expressions (GCC coroutine frame-slot aliasing).
    absl::StatusOr<bool> freed = false;
    if (owner == store->worker_->id()) {
      freed = co_await ReclaimExtentLocal(*store, ref);
    } else {
      freed = co_await celer::SubmitTaskTo(
          owner, [this, owner, ref]() -> Task<absl::StatusOr<bool>> {
            co_return co_await ReclaimExtentLocal(*stores_[owner], ref);
          });
    }
    if (!freed.ok()) {
      co_return freed.status();
    }
    if (*freed) {
      released.push_back(ref.block_id_);
    }
  }
  absl::Status returned = co_await ReturnColdBlocks(std::move(released));
  if (!returned.ok()) {
    co_return returned;
  }
  space_reclaim_generation_.fetch_add(1, std::memory_order_release);
  co_return absl::OkStatus();
}

bool StorageEngine::Impl::IsDefragCandidate(
    const WorkerStore& store, std::uint64_t block_id) const noexcept {
  const BlockState* state = FindBlockState(store, block_id);
  if (state == nullptr || !state->allocated_ || state->defrag_queued_ ||
      state->defragging_ || state->pins_ != 0 || state->in_memory_ ||
      state->kind_ != BlockKind::kRecords || state->flush_queued_ ||
      state->flush_in_progress_ || IsActiveBlock(store, block_id) ||
      state->committed_bytes_ <= kBlockHeaderBytes) {
    return false;
  }
  const std::uint64_t used = state->committed_bytes_ - kBlockHeaderBytes;
  const std::uint64_t live_ratio =
      used == 0
          ? 0
          : (static_cast<std::uint64_t>(state->live_bytes_) * 1000) / used;
  return live_ratio <= 500;
}

void StorageEngine::Impl::MaybeQueueDefrag(WorkerStore& store,
                                           std::uint64_t block_id) {
  BlockState* state = FindBlockState(store, block_id);
  if (state == nullptr || !IsDefragCandidate(store, block_id)) {
    return;
  }
  state->defrag_queued_ = true;
  store.defrag_queue_.push_back(block_id);
  RequestDefrag(store);
}

Task<absl::Status> StorageEngine::Impl::ConfigureDefrag(
    DefragConfigUpdate update) {
  co_return co_await celer::SubmitTo(0, [this, update] {
    switch (update.action_) {
      case DefragConfigAction::kPause:
        defrag_config_.paused_.store(true, std::memory_order_release);
        return absl::OkStatus();
      case DefragConfigAction::kResume:
        if (defrag_config_.paused_.exchange(false, std::memory_order_acq_rel)) {
          for (std::size_t device_index = 0; device_index < devices_.size();
               ++device_index) {
            stores_[0]->worker_->SpawnBackground(
                WakeQueuedDefrags(device_index));
          }
        }
        return absl::OkStatus();
      case DefragConfigAction::kMaxActivePerDevice: {
        if (update.value_ == 0 ||
            update.value_ > kDefragReserveBlocksPerDevice) {
          return absl::Status(
              absl::StatusCode::kInvalidArgument,
              "defrag concurrency must be between 1 and the per-device "
              "reserve");
        }
        const unsigned requested = static_cast<unsigned>(update.value_);
        const unsigned previous =
            defrag_config_.max_active_per_device_.exchange(
                requested, std::memory_order_acq_rel);
        if (requested > previous) {
          for (std::size_t device_index = 0; device_index < devices_.size();
               ++device_index) {
            stores_[0]->worker_->SpawnBackground(
                WakeQueuedDefrags(device_index));
          }
        }
        return absl::OkStatus();
      }
      case DefragConfigAction::kBlockSleep:
        if (update.value_ > std::numeric_limits<std::uint32_t>::max()) {
          return absl::Status(absl::StatusCode::kInvalidArgument,
                              "invalid defrag block sleep");
        }
        defrag_config_.block_sleep_ms_.store(
            static_cast<std::uint32_t>(update.value_),
            std::memory_order_release);
        return absl::OkStatus();
      case DefragConfigAction::kRecordSleep:
        if (update.value_ > std::numeric_limits<std::uint32_t>::max()) {
          return absl::Status(absl::StatusCode::kInvalidArgument,
                              "invalid defrag record sleep");
        }
        defrag_config_.record_sleep_us_.store(
            static_cast<std::uint32_t>(update.value_),
            std::memory_order_release);
        return absl::OkStatus();
    }
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "invalid defrag configuration action");
  });
}

bool StorageEngine::Impl::TryAcquireDefragPermit(std::size_t device_index) {
  if (defrag_config_.paused_.load(std::memory_order_acquire)) {
    return false;
  }
  std::atomic<unsigned>& device_active =
      active_defrags_by_device_[device_index];
  unsigned active = device_active.load(std::memory_order_acquire);
  while (active < defrag_config_.max_active_per_device_.load(
                      std::memory_order_acquire)) {
    if (device_active.compare_exchange_weak(active, active + 1,
                                            std::memory_order_acq_rel,
                                            std::memory_order_acquire)) {
      active_defrags_.fetch_add(1, std::memory_order_acq_rel);
      return true;
    }
  }
  return false;
}

void StorageEngine::Impl::ReleaseDefragPermit(std::size_t device_index) {
  active_defrags_by_device_[device_index].fetch_sub(1,
                                                    std::memory_order_acq_rel);
  active_defrags_.fetch_sub(1, std::memory_order_acq_rel);
}

Task<absl::Status> StorageEngine::Impl::StartQueuedDefrag(
    unsigned worker_id, std::size_t device_index) {
  WorkerStore& store = *stores_[worker_id];
  if (!store.defrag_waiting_ || store.defrag_waiting_device_ != device_index) {
    ReleaseDefragPermit(device_index);
    co_return absl::OkStatus();
  }
  store.defrag_waiting_ = false;
  SetDefragPending(false);
  if (shutdown_flush_requested_.load(std::memory_order_acquire) ||
      store.defrag_running_ || store.defrag_queue_.empty() ||
      DeviceIndexForBlock(store.defrag_queue_.front()) != device_index) {
    ReleaseDefragPermit(device_index);
    RequestDefrag(store);
    co_return absl::OkStatus();
  }
  store.defrag_running_ = true;
  SetDefragActive(true);
  store.active_defrag_device_ = device_index;
  store.worker_->SpawnBackground(DefragOne(&store));
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::WakeQueuedDefrags(
    std::size_t device_index) {
  while (TryAcquireDefragPermit(device_index)) {
    std::uint16_t worker_id = 0;
    if (!defrag_ready_by_device_[device_index].try_dequeue(worker_id)) {
      ReleaseDefragPermit(device_index);
      co_return absl::OkStatus();
    }
    pending_defrags_.fetch_sub(1, std::memory_order_acq_rel);
    absl::Status started;
    if (worker_id == celer::ThisWorker().id_) {
      started = co_await StartQueuedDefrag(worker_id, device_index);
    } else {
      started = co_await celer::SubmitTaskTo(
          worker_id, [this, worker_id, device_index]() -> Task<absl::Status> {
            co_return co_await StartQueuedDefrag(worker_id, device_index);
          });
    }
    if (!started.ok()) {
      ReleaseDefragPermit(device_index);
      co_return started;
    }
  }
  co_return absl::OkStatus();
}

void StorageEngine::Impl::RequestDefrag(WorkerStore& store) {
  if (shutdown_flush_requested_.load(std::memory_order_acquire) ||
      store.defrag_running_ || store.defrag_waiting_ ||
      store.defrag_queue_.empty()) {
    return;
  }
  const std::size_t device_index =
      DeviceIndexForBlock(store.defrag_queue_.front());
  store.defrag_waiting_ = true;
  SetDefragPending(true);
  store.defrag_waiting_device_ = device_index;
  pending_defrags_.fetch_add(1, std::memory_order_acq_rel);
  if (!defrag_ready_by_device_[device_index].enqueue(store.worker_->id())) {
    pending_defrags_.fetch_sub(1, std::memory_order_acq_rel);
    store.defrag_waiting_ = false;
    SetDefragPending(false);
    spdlog::error("worker[{}] failed to enqueue defrag request for device {}",
                  store.worker_->id(), devices_[device_index].id_);
    return;
  }
  store.worker_->SpawnBackground(WakeQueuedDefrags(device_index));
}

void StorageEngine::Impl::FinishDefragPass(WorkerStore& store) {
  const std::size_t completed_device = store.active_defrag_device_;
  store.defrag_running_ = false;
  SetDefragActive(false);
  // Queue this worker's next candidate before releasing the active permit,
  // so foreground allocation never observes a false no-reclaim gap.
  RequestDefrag(store);
  space_reclaim_generation_.fetch_add(1, std::memory_order_release);
  ReleaseDefragPermit(completed_device);
  store.worker_->SpawnBackground(WakeQueuedDefrags(completed_device));
}

Task<absl::Status> StorageEngine::Impl::DefragOne(WorkerStore* store) {
  if (store->defrag_queue_.empty()) {
    FinishDefragPass(*store);
    co_return absl::OkStatus();
  }
  const std::uint64_t candidate = store->defrag_queue_.front();
  store->defrag_queue_.pop_front();
  BlockState* candidate_state = FindBlockState(*store, candidate);
  if (candidate_state == nullptr) {
    FinishDefragPass(*store);
    co_return absl::OkStatus();
  }
  candidate_state->defrag_queued_ = false;

  absl::Status status = co_await CleanBlockLocked(*store, candidate);
  if (status.ok()) {
    RecordDefragMetric(DefragMetricResult::kSuccess);
  } else if (status.code() == absl::StatusCode::kResourceExhausted) {
    RecordDefragMetric(DefragMetricResult::kResourceExhausted);
  } else {
    RecordDefragMetric(DefragMetricResult::kFailure);
  }
  if (!status.ok()) {
    spdlog::error("worker[{}] defrag block {} failed: {}", store->worker_->id(),
                  candidate, status.message());
  }
  if (status.code() == absl::StatusCode::kResourceExhausted) {
    MaybeQueueDefrag(*store, candidate);
  }
  const std::uint32_t block_sleep_ms =
      defrag_config_.block_sleep_ms_.load(std::memory_order_acquire);
  if (block_sleep_ms != 0 &&
      !shutdown_flush_requested_.load(std::memory_order_acquire)) {
    // Hold the device permit during an asynchronous cooldown. This paces the
    // device rather than merely delaying this worker while another worker
    // immediately takes its place. SleepFor suspends only this background
    // coroutine; it never blocks the worker thread.
    (void)co_await celer::SleepFor(*store->worker_,
                                   std::chrono::milliseconds(block_sleep_ms));
  }
  FinishDefragPass(*store);
  co_return status;
}

Task<absl::Status> StorageEngine::Impl::DefragRecordCheckpoint(
    WorkerStore& store) {
  const std::uint32_t sleep_us =
      defrag_config_.record_sleep_us_.load(std::memory_order_acquire);
  if (sleep_us != 0) {
    co_return co_await celer::SleepFor(*store.worker_,
                                       std::chrono::microseconds(sleep_us));
  }
  co_await celer::Yield(*store.worker_);
  co_return absl::OkStatus();
}

Task<absl::StatusOr<std::optional<RelocationDurabilityFence>>>
StorageEngine::Impl::RelocateIfCurrent(unsigned key_owner, std::string_view key,
                                       std::string_view value,
                                       const RecordHeader& record,
                                       const RecordLocation& source_location,
                                       bool clear_txid) {
  WorkerStore& key_store = *stores_[key_owner];
  co_await key_store.store_state_mutex_.Lock();
  UnlockGuard write_unlock(&key_store.store_state_mutex_, key_store.worker_);

  auto& partition = PartitionForKey(key_store, key);
  auto& index = partition.indexes_[record.db_id_];
  const Digest digest = ComputeDigest(key);
  RecordIndex::Entry* current = nullptr;
  for (RecordIndex::Entry* candidate : index.FindCandidates(digest, key)) {
    if (MaterializeIndexLocation(*candidate)
            .SamePhysicalRecord(source_location)) {
      current = candidate;
      break;
    }
  }
  if (current == nullptr) {
    co_return std::optional<RelocationDurabilityFence>{};
  }
  // A source from a flushed database epoch is already condemned: FLUSHDB has
  // published the new epoch and this worker's detach just has not run yet.
  // There is no value in copying it; relocation would retain the source epoch
  // for crash safety and detached-index reclaim will settle its accounting.
  if (DbEpoch(record.db_id_) != record.db_epoch_) {
    co_return std::optional<RelocationDurabilityFence>{};
  }
  const RelocationSource source{
      .db_epoch_ = record.db_epoch_,
      .replication_epoch_ = partition.replication_epoch_,
      .index_generation_ = key_store.index_generations_[record.db_id_],
      .block_id_ = source_location.block_id(),
      .allocation_epoch_ = source_location.allocation_epoch(),
      .record_offset_ = source_location.record_offset(),
  };
  RecordLocation relocated;
  absl::Status written = co_await WriteRecordLocked(
      key_store, record.db_id_, key, value, record.kind_, record.value_type_,
      record.expire_at_ms_, digest, clear_txid ? 0 : record.txid_,
      record.mutation_sequence_, true, true, record.external_,
      record.key_external_, record.logical_size_,
      ExtentsFor(key_store, current), &relocated, &source);
  if (written.code() == absl::StatusCode::kAborted) {
    // A client write replaced this key, or FLUSHDB/replica reset replaced the
    // index, while relocation waited for a block. Nothing was written; the
    // source stays uncleaned this pass rather than resurrecting stale state.
    co_return std::optional<RelocationDurabilityFence>{};
  }
  if (!written.ok()) {
    co_return written;
  }
  co_return std::optional<RelocationDurabilityFence>(RelocationDurabilityFence{
      .block_id_ = relocated.block_id(),
      .allocation_epoch_ = relocated.allocation_epoch(),
      .block_owner_ = relocated.block_owner(),
      .committed_bytes_ = static_cast<std::uint32_t>(
          relocated.record_offset() + relocated.total_disk_bytes()),
  });
}

Task<absl::Status> StorageEngine::Impl::AwaitRelocationDurableLocal(
    WorkerStore& store, const RelocationDurabilityFence& fence) {
  while (true) {
    co_await store.store_state_mutex_.Lock();
    bool durable = false;
    bool failed = false;
    {
      UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
      BlockState* state = FindBlockState(store, fence.block_id_);
      if (state == nullptr || !state->allocated_ ||
          state->allocation_epoch_ != fence.allocation_epoch_) {
        // The only paths that destroy or reuse a committed block first make
        // every record they retire durable elsewhere (or durably invalidate
        // the whole DB/partition). That guarantee is transitive across a
        // chain of defrag relocations.
        durable = true;
      } else if (StagingSlot* staging = StagingFor(store, *state);
                 staging == nullptr) {
        // A records block loses its staging buffer only after its committed
        // header is durable.
        durable = !state->in_memory_ && !state->flush_in_progress_;
      } else if (staging->durable_bytes_ >= fence.committed_bytes_) {
        durable = true;
      } else {
        RequestFlush(store, fence.block_id_);
      }
      failed = store.write_failed_;
    }
    if (failed) {
      co_return absl::Status(
          absl::StatusCode::kInternal,
          "storage write failed while flushing defrag relocation");
    }
    if (durable) {
      co_return absl::OkStatus();
    }
    absl::Status waited =
        co_await celer::SleepFor(*store.worker_, std::chrono::milliseconds(1));
    if (!waited.ok()) {
      co_return waited;
    }
  }
}

Task<absl::Status> StorageEngine::Impl::AwaitRelocationDurable(
    const RelocationDurabilityFence& fence) {
  if (fence.block_owner_ >= worker_count_) {
    co_return absl::Status(absl::StatusCode::kInternal,
                           "defrag relocation has an invalid block owner");
  }
  WorkerStore& owner = *stores_[fence.block_owner_];
  if (fence.block_owner_ == celer::ThisWorker().id_) {
    co_return co_await AwaitRelocationDurableLocal(owner, fence);
  }
  co_return co_await celer::SubmitTaskTo(
      fence.block_owner_, [this, fence]() -> Task<absl::Status> {
        co_return co_await AwaitRelocationDurableLocal(
            *stores_[fence.block_owner_], fence);
      });
}

namespace {

// Deduplicated by destination (block, epoch, owner), keeping the highest
// staged boundary — awaiting that covers every lower one.
void MergeRelocationFences(std::vector<RelocationDurabilityFence>* into,
                           const std::vector<RelocationDurabilityFence>& from) {
  for (const RelocationDurabilityFence& fence : from) {
    bool merged = false;
    for (RelocationDurabilityFence& existing : *into) {
      if (existing.block_id_ == fence.block_id_ &&
          existing.allocation_epoch_ == fence.allocation_epoch_ &&
          existing.block_owner_ == fence.block_owner_) {
        existing.committed_bytes_ =
            std::max(existing.committed_bytes_, fence.committed_bytes_);
        merged = true;
        break;
      }
    }
    if (!merged) {
      into->push_back(fence);
    }
  }
}

}  // namespace

Task<absl::Status> StorageEngine::Impl::CleanBlockLocked(
    WorkerStore& store, std::uint64_t block_id) {
  BlockState* source_ptr = FindBlockState(store, block_id);
  if (source_ptr == nullptr) {
    co_return absl::OkStatus();
  }
  BlockState& source = *source_ptr;
  if (!source.allocated_ || source.defragging_ || source.in_memory_ ||
      source.pins_ != 0 || source.flush_queued_ || source.flush_in_progress_ ||
      IsActiveBlock(store, block_id)) {
    co_return absl::OkStatus();
  }
  source.defragging_ = true;
  const auto [source_file_id, source_block_offset] = FileOffset(block_id);

  // live_bytes counts exactly the index entries naming this block, so zero
  // means nothing here is reachable and the salvage pass is a provable no-op:
  // every record it decoded would find its key either absent from the index
  // or pointing at a different physical record, so RelocateIfCurrent would
  // rewrite nothing and the live_bytes re-check below would still see zero.
  // Skipping reaches the same free path under the same precondition, without
  // reading 8 MiB and CRC-checking every record in it. FLUSHDB empties whole
  // blocks at once, which is where this dominates.
  if (source.live_bytes_ != 0) {
    absl::Status salvaged = co_await SalvageBlockRecords(
        store, block_id, source, source_file_id, source_block_offset);
    if (!salvaged.ok()) {
      co_return salvaged;
    }
  }

  // Do not clear the source block's allocation bitmap bit until every
  // relocation ever made out of it — this pass's and any failed earlier
  // pass's — is covered by a durable destination header. If the process dies
  // while waiting, recovery still scans the source; if it dies afterwards,
  // recovery skips it or selects the higher-sequence relocation.
  if (auto owed = store.pending_relocation_fences_.find(block_id);
      owed != store.pending_relocation_fences_.end()) {
    std::vector<RelocationDurabilityFence> fences = std::move(owed->second);
    store.pending_relocation_fences_.erase(owed);
    for (std::size_t i = 0; i < fences.size(); ++i) {
      absl::Status durable = co_await AwaitRelocationDurable(fences[i]);
      if (!durable.ok()) {
        // Re-stash the unconfirmed remainder for the next pass.
        std::vector<RelocationDurabilityFence> remainder(
            fences.begin() + static_cast<std::ptrdiff_t>(i), fences.end());
        MergeRelocationFences(&store.pending_relocation_fences_[block_id],
                              remainder);
        source.defragging_ = false;
        co_return durable;
      }
    }
  }

  {
    co_await store.store_state_mutex_.Lock();
    UnlockGuard write_unlock(&store.store_state_mutex_, store.worker_);
    if (source.live_bytes_ != 0) {
      source.defragging_ = false;
      co_return absl::OkStatus();
    }
    source.freeing_ = true;
  }
  co_return co_await ReleaseEmptyBlock(store, block_id, source);
}

Task<absl::Status> StorageEngine::Impl::SalvageBlockRecords(
    WorkerStore& store, std::uint64_t block_id, BlockState& source,
    std::uint32_t source_file_id, std::uint64_t source_block_offset,
    std::shared_ptr<const absl::flat_hash_set<std::uint64_t>> committed_txids) {
  struct DefragBuffer {
    RegisteredBufferPool* pool_ = nullptr;
    std::uint16_t buffer_id_ = 0;
    std::byte* heap_data_ = nullptr;
    FixedBuffer buffer_{};

    ~DefragBuffer() {
      if (buffer_id_ != 0) {
        pool_->ReleaseWriteBuffer(buffer_id_);
      } else if (heap_data_ != nullptr) {
        pool_->ReleaseHeapWriteBuffer(heap_data_);
      }
    }

    bool registered() const noexcept {
      return buffer_id_ != 0 && pool_->buffers_registered();
    }
  } block_data{.pool_ = &store.buffers_};
  if (store.buffers_.TryAcquireWriteBuffer(&block_data.buffer_id_)) {
    block_data.buffer_ = store.buffers_.write_buffer(block_data.buffer_id_);
  } else if (store.buffers_.TryAcquireHeapWriteBuffer(&block_data.heap_data_)) {
    block_data.buffer_ = FixedBuffer{
        .data_ = block_data.heap_data_,
        .size_ = options_.buffers_.write_buffer_bytes_,
        .index_ = 0,
    };
  } else {
    source.defragging_ = false;
    co_return absl::Status(absl::StatusCode::kResourceExhausted,
                           "failed to allocate defrag block buffer");
  }
  if (block_data.buffer_.size_ < kStorageBlockBytes) {
    source.defragging_ = false;
    co_return absl::Status(
        absl::StatusCode::kResourceExhausted,
        "defrag block buffer is smaller than a storage block");
  }
  block_data.buffer_.size_ = kStorageBlockBytes;
  auto read = co_await ReadStorageBuffer(
      *store.worker_, store.files_[source_file_id], block_data.buffer_,
      block_data.registered(), source_block_offset);
  if (!read.ok() || *read != kStorageBlockBytes) {
    source.defragging_ = false;
    co_return read.ok() ? absl::Status(absl::StatusCode::kInternal,
                                       "short block read during defrag")
                        : read.status();
  }

  // Every fence this pass produces is deposited into the store's per-block
  // debt on every exit path (the destructor runs on error returns too): a
  // pass that fails midway has already moved records, and forgetting their
  // fences let a later pass durably free the source before those copies
  // were flushed — records that were durable before defrag died with it.
  std::vector<RelocationDurabilityFence> durability_fences;
  struct FenceDebt {
    WorkerStore* store_;
    std::uint64_t block_id_;
    std::vector<RelocationDurabilityFence>* fences_;
    ~FenceDebt() {
      if (!fences_->empty()) {
        MergeRelocationFences(&(*store_).pending_relocation_fences_[block_id_],
                              *fences_);
      }
    }
  } fence_debt{&store, block_id, &durability_fences};
  std::uint32_t record_offset = kBlockHeaderBytes;
  while (record_offset < source.committed_bytes_) {
    // live_bytes is updated on this worker between salvage resumptions. Zero
    // proves that no index entry names any record in the source block.
    if (source.live_bytes_ == 0) {
      break;
    }
    const std::optional<std::uint32_t> next = NextRecordOffset(
        block_data.buffer_.data_, record_offset, source.committed_bytes_);
    if (!next.has_value()) {
      source.defragging_ = false;
      co_return absl::Status(absl::StatusCode::kInternal,
                             "corrupt committed record during defrag");
    }
    if (*next != record_offset) {
      record_offset = *next;
      continue;
    }
    RecordHeader record{};
    std::string_view disk_key;
    std::span<const std::byte> record_bytes(
        block_data.buffer_.data_ + record_offset,
        source.committed_bytes_ - record_offset);
    if (!DecodeRecordHeader(record_bytes, &record, &disk_key) ||
        record.allocation_epoch_ != source.allocation_epoch_ ||
        record_offset + record.total_disk_bytes_ > source.committed_bytes_) {
      source.defragging_ = false;
      co_return absl::Status(absl::StatusCode::kInternal,
                             "corrupt committed record during defrag");
    }
    // A record from a flushed database epoch is unreachable. Its header is
    // sufficient to advance safely; do not checksum a large payload or read
    // a shared extent chain that can no longer affect recovery.
    if (record.kind_ != RecordKind::kTxCommit &&
        DbEpoch(record.db_id_) != record.db_epoch_) {
      record_offset += record.total_disk_bytes_;
      absl::Status paced = co_await DefragRecordCheckpoint(store);
      if (!paced.ok()) {
        source.defragging_ = false;
        co_return paced;
      }
      continue;
    }
    const std::byte* payload_data =
        block_data.buffer_.data_ + record_offset + record.header_bytes_;
    const auto payload =
        std::span<const std::byte>(payload_data, record.payload_bytes_);
    if (Crc32c(payload) != record.payload_checksum_) {
      source.defragging_ = false;
      co_return absl::Status(absl::StatusCode::kInternal,
                             "payload checksum mismatch during defrag");
    }
    if (record.kind_ == RecordKind::kTxCommit) {
      if (committed_txids == nullptr) {
        source.defragging_ = false;
        co_return absl::InternalError(
            "ordinary records block contains a TxCommit");
      }
      // Commit decisions are not key-index locations, so they have no packed
      // runtime type state to construct. Keep them in place until every
      // tagged winner in the generation has a durable untagged copy.
      record_offset += record.total_disk_bytes_;
      continue;
    }
    std::string loaded_key;
    if (record.key_external_) [[unlikely]] {
      if (record.external_) {
        auto decoded =
            DecodeManifest(payload,
                           static_cast<std::uint64_t>(record.key_bytes_) +
                               record.logical_size_,
                           record.kind_ != RecordKind::kValue ||
                               record.value_type_ == ValueType::kString);
        if (!decoded.ok()) {
          source.defragging_ = false;
          co_return decoded.status();
        }
        auto external_key =
            co_await LoadExternalKey(store, *decoded, record.key_bytes_);
        if (!external_key.ok()) {
          source.defragging_ = false;
          co_return external_key.status();
        }
        loaded_key = std::move(*external_key);
        disk_key = loaded_key;
      } else {
        if (record.payload_bytes_ < record.key_bytes_) {
          source.defragging_ = false;
          co_return absl::Status(absl::StatusCode::kInternal,
                                 "inline external key is truncated");
        }
        disk_key = std::string_view(reinterpret_cast<const char*>(payload_data),
                                    record.key_bytes_);
      }
    }

    RecordLocation source_location(
        block_id, record.mutation_sequence_, record.allocation_epoch_,
        record.expire_at_ms_, static_cast<std::uint32_t>(record.logical_size_),
        RecordLocation::PackedMetadata::Encode(
            record_offset, record.total_disk_bytes_, store.worker_->id(), false,
            record.external_, record.key_external_, false, false,
            record.txid_ != 0 && record.kind_ != RecordKind::kTxCommit,
            record.kind_, record.value_type_));
    if (record.external_) {
      const std::uint64_t extent_bytes =
          record.logical_size_ + (record.key_external_ ? record.key_bytes_ : 0);
      auto decoded =
          DecodeManifest(payload, extent_bytes,
                         record.kind_ != RecordKind::kValue ||
                             record.value_type_ == ValueType::kString);
      if (!decoded.ok()) {
        source.defragging_ = false;
        co_return decoded.status();
      }
    }

    if (committed_txids != nullptr &&
        !committed_txids->contains(record.txid_)) {
      // No durable decision: never turn this record into an unconditional
      // txid-zero recovery winner. Rollback/accounting will make it dead; if
      // it is still charged, the generation remains unreclaimable.
      record_offset += record.total_disk_bytes_;
      continue;
    }
    const unsigned key_owner = OwnerForKey(disk_key);
    const std::string key(disk_key);
    const std::size_t key_prefix =
        record.key_external_ && !record.external_ ? record.key_bytes_ : 0;
    const std::string value(
        reinterpret_cast<const char*>(payload_data + key_prefix),
        record.payload_bytes_ - key_prefix);
    absl::StatusOr<std::optional<RelocationDurabilityFence>> relocated(
        std::optional<RelocationDurabilityFence>{});
    if (key_owner == store.worker_->id()) {
      relocated = co_await RelocateIfCurrent(key_owner, key, value, record,
                                             source_location,
                                             committed_txids != nullptr);
    } else {
      relocated = co_await celer::SubmitTaskTo(
          key_owner,
          [this, key_owner, key, value, record, source_location,
           promote = committed_txids != nullptr]() mutable
          -> Task<absl::StatusOr<std::optional<RelocationDurabilityFence>>> {
            co_return co_await RelocateIfCurrent(key_owner, key, value, record,
                                                 source_location, promote);
          });
    }
    if (!relocated.ok()) {
      source.defragging_ = false;
      co_return relocated.status();
    }
    if (relocated->has_value()) {
      const RelocationDurabilityFence& fence = **relocated;
      auto existing = std::find_if(
          durability_fences.begin(), durability_fences.end(),
          [&](const RelocationDurabilityFence& candidate) {
            return candidate.block_owner_ == fence.block_owner_ &&
                   candidate.block_id_ == fence.block_id_ &&
                   candidate.allocation_epoch_ == fence.allocation_epoch_;
          });
      if (existing == durability_fences.end()) {
        durability_fences.push_back(fence);
      } else {
        existing->committed_bytes_ =
            std::max(existing->committed_bytes_, fence.committed_bytes_);
      }
    }
    record_offset += record.total_disk_bytes_;
    // With record_sleep_us=0 this is the cooperative background-budget
    // checkpoint. A positive value forces an asynchronous pause after every
    // record, smoothing one block's CPU, cross-core, and device-I/O burst.
    absl::Status paced = co_await DefragRecordCheckpoint(store);
    if (!paced.ok()) {
      source.defragging_ = false;
      co_return paced;
    }
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::ReleaseEmptyBlock(
    WorkerStore& store, std::uint64_t block_id, BlockState& source) {
  while (source.pins_ != 0) {
    absl::Status waited =
        co_await celer::SleepFor(*store.worker_, std::chrono::milliseconds(1));
    if (!waited.ok()) {
      source.freeing_ = false;
      source.defragging_ = false;
      co_return waited;
    }
  }

  // The durable allocation bitmap is authoritative during recovery. Retire
  // the runtime state before publishing the block to cold_free so another
  // worker cannot allocate it while its old owner still names it. A bitmap
  // write failure fail-stops the allocator, so this block cannot be reused
  // in the ambiguous state.
  DestroyBlockState(store, block_id);
  absl::Status returned = co_await ReturnColdBlocks({block_id});
  if (returned.ok()) {
    // The source's cleared allocation bit is now durable while its stale
    // records are still on disk — the exact window the relocation durability
    // fence exists to protect. Crash-safety tests arm this point.
    KEYLANE_MAYBE_CRASH_AT("defrag-source-retired");
    auto deferred = store.deferred_dependent_extent_reclaims_.find(block_id);
    if (deferred != store.deferred_dependent_extent_reclaims_.end()) {
      std::vector<ExtentManifest> manifests = std::move(deferred->second);
      store.deferred_dependent_extent_reclaims_.erase(deferred);
      for (const ExtentManifest& manifest : manifests) {
        SpawnExtentReclaim(store, manifest);
      }
    }
  }
  co_return returned;
}

}  // namespace keylane::storage
