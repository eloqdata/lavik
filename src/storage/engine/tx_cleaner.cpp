#include <algorithm>
#include <limits>

#include "impl.h"

namespace keylane::storage {

namespace {

std::int64_t MonotonicMillis() noexcept {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

}  // namespace

void StorageEngine::Impl::InitializeTxWrites(std::uint64_t txid,
                                             std::span<TxShardWrites> writes) {
  if (writes.empty()) return;

  const std::uint64_t generation =
      current_tx_generation_.load(std::memory_order_seq_cst);
  WorkerStore& store = CurrentStore();
  auto& slot = store.tx_generations_[generation];
  if (slot == nullptr) slot = std::make_shared<TxGenerationRuntime>();
  const std::shared_ptr<TxGenerationRuntime> state = slot;

  auto lease =
      std::shared_ptr<void>(new std::uint8_t{0}, [this, state](void* token) {
        delete static_cast<std::uint8_t*>(token);
        const std::uint64_t previous =
            state->active_transactions_.fetch_sub(1, std::memory_order_acq_rel);
        assert(previous != 0);
        (void)previous;
        tx_cleaner_dirty_.store(true, std::memory_order_release);
      });
  state->active_transactions_.fetch_add(1, std::memory_order_release);
  for (TxShardWrites& shard : writes) {
    shard.txid_ = txid;
    shard.generation_ = generation;
    shard.generation_lease_ = lease;
  }
}

void StorageEngine::Impl::RegisterRecoveredTxGeneration(
    WorkerStore& store, std::uint64_t generation) {
  assert(generation != 0);
  auto& slot = store.tx_generations_[generation];
  if (slot == nullptr) slot = std::make_shared<TxGenerationRuntime>();
  slot->has_records_ = true;
  std::uint64_t next = current_tx_generation_.load(std::memory_order_relaxed);
  while (next <= generation &&
         !current_tx_generation_.compare_exchange_weak(
             next, generation + 1, std::memory_order_release,
             std::memory_order_relaxed)) {
  }
  tx_cleaner_dirty_.store(true, std::memory_order_release);
}

void StorageEngine::Impl::NoteTxRecordLocal(WorkerStore& store,
                                            std::uint64_t block_id,
                                            std::uint64_t allocation_epoch,
                                            std::uint64_t generation,
                                            std::uint64_t txid,
                                            std::uint32_t bytes, bool commit) {
  assert(generation != 0 && txid != 0 && bytes != 0);
  WorkerStore::TxBlockRuntime& block = store.tx_blocks_[block_id];
  if (block.allocation_epoch_ != allocation_epoch ||
      block.generation_ != generation) {
    block = WorkerStore::TxBlockRuntime{
        .allocation_epoch_ = allocation_epoch,
        .generation_ = generation,
    };
  }
  if (!commit) {
    block.live_tagged_bytes_ += bytes;
  }
  auto& generation_state = store.tx_generations_[generation];
  if (generation_state == nullptr) {
    generation_state = std::make_shared<TxGenerationRuntime>();
  }
  generation_state->has_records_ = true;
  if (commit) generation_state->committed_txids_.insert(txid);
  tx_cleaner_dirty_.store(true, std::memory_order_release);
}

void StorageEngine::Impl::DropTaggedRecordLocal(WorkerStore& store,
                                                std::uint64_t block_id,
                                                std::uint64_t allocation_epoch,
                                                std::uint32_t bytes) noexcept {
  const auto found = store.tx_blocks_.find(block_id);
  if (found == store.tx_blocks_.end() ||
      found->second.allocation_epoch_ != allocation_epoch) {
    return;  // A stale/duplicate retirement no longer owns this allocation.
  }
  assert(found->second.live_tagged_bytes_ >= bytes);
  found->second.live_tagged_bytes_ -= bytes;
  tx_cleaner_dirty_.store(true, std::memory_order_release);
}

bool StorageEngine::Impl::PinTxDependencyLocal(
    WorkerStore& store, const RecordLocation& location) noexcept {
  const auto found = store.tx_blocks_.find(location.block_id());
  if (found == store.tx_blocks_.end() ||
      found->second.allocation_epoch_ != location.allocation_epoch()) {
    return false;
  }
  ++found->second.dependency_pins_;
  tx_cleaner_dirty_.store(true, std::memory_order_release);
  return true;
}

void StorageEngine::Impl::UnpinTxDependencyLocal(
    WorkerStore& store, std::uint64_t block_id,
    std::uint64_t allocation_epoch) noexcept {
  const auto found = store.tx_blocks_.find(block_id);
  if (found == store.tx_blocks_.end() ||
      found->second.allocation_epoch_ != allocation_epoch) {
    return;
  }
  assert(found->second.dependency_pins_ != 0);
  --found->second.dependency_pins_;
  tx_cleaner_dirty_.store(true, std::memory_order_release);
}

absl::Status StorageEngine::Impl::ConfigureTxCleanerCooldown(
    std::uint64_t cooldown_ms) {
  if (cooldown_ms > std::numeric_limits<std::uint32_t>::max()) {
    return absl::InvalidArgumentError("cooldown is out of range");
  }
  tx_cleaner_cooldown_ms_.store(static_cast<std::uint32_t>(cooldown_ms),
                                std::memory_order_release);
  tx_cleaner_next_run_ms_.store(0, std::memory_order_release);
  tx_cleaner_dirty_.store(true, std::memory_order_release);
  return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::MaybeRunTxCleaner() {
  const std::uint32_t cooldown =
      tx_cleaner_cooldown_ms_.load(std::memory_order_acquire);
  if (cooldown == 0 || !tx_cleaner_dirty_.load(std::memory_order_acquire)) {
    co_return absl::OkStatus();
  }
  const std::int64_t now = MonotonicMillis();
  if (now < tx_cleaner_next_run_ms_.load(std::memory_order_acquire)) {
    co_return absl::OkStatus();
  }
  bool expected = false;
  if (!tx_cleaner_running_.compare_exchange_strong(expected, true,
                                                   std::memory_order_acq_rel,
                                                   std::memory_order_acquire)) {
    co_return absl::OkStatus();
  }
  struct RunningGuard {
    std::atomic<bool>* running_;
    ~RunningGuard() { running_->store(false, std::memory_order_release); }
  } guard{&tx_cleaner_running_};

  tx_cleaner_dirty_.store(false, std::memory_order_release);
  tx_cleaner_next_run_ms_.store(now + static_cast<std::int64_t>(cooldown),
                                std::memory_order_release);
  tx_cleaner_rounds_.fetch_add(1, std::memory_order_relaxed);
  absl::Status status = absl::OkStatus();
#ifndef NDEBUG
  static std::atomic<bool> cleaner_failure_claimed = false;
  bool expected_failure = false;
  if (std::getenv("KEYLANE_FAIL_TX_CLEANER_ONCE") != nullptr &&
      cleaner_failure_claimed.compare_exchange_strong(
          expected_failure, true, std::memory_order_acq_rel)) {
    status = absl::FailedPreconditionError(
        "injected retryable transaction cleaner failure");
  } else {
    status = co_await RunTxCleaner();
  }
#else
  status = co_await RunTxCleaner();
#endif
  if (!status.ok()) {
    tx_cleaner_failures_.fetch_add(1, std::memory_order_relaxed);
    tx_cleaner_dirty_.store(true, std::memory_order_release);
  }
  co_return status;
}

Task<absl::StatusOr<TxGenerationLocalState>>
StorageEngine::Impl::InspectTxGenerationLocal(WorkerStore& store,
                                              std::uint64_t generation,
                                              bool seal) {
  std::optional<RelocationDurabilityFence> fence;
  {
    co_await store.store_state_mutex_.Lock();
    UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
    auto active = store.active_tx_blocks_.find(generation);
    if (seal && active != store.active_tx_blocks_.end() &&
        active->second.has_value()) {
      const ActiveBlock block = *active->second;
      RequestFlush(store, block.block_id_);
      active->second.reset();
      fence = RelocationDurabilityFence{
          .block_id_ = block.block_id_,
          .allocation_epoch_ = block.allocation_epoch_,
          .block_owner_ = static_cast<std::uint16_t>(store.worker_->id()),
          .committed_bytes_ = block.committed_bytes_,
      };
    }
  }
  if (fence.has_value()) {
    absl::Status durable = co_await AwaitRelocationDurableLocal(store, *fence);
    if (!durable.ok()) co_return durable;
  }

  co_await store.store_state_mutex_.Lock();
  UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
  TxGenerationLocalState result;
  if (const auto runtime = store.tx_generations_.find(generation);
      runtime != store.tx_generations_.end()) {
    result.active_transactions_ =
        runtime->second->active_transactions_.load(std::memory_order_acquire);
    result.committed_txids_.assign(runtime->second->committed_txids_.begin(),
                                   runtime->second->committed_txids_.end());
  }
  for (const auto& [block_id, tx_block] : store.tx_blocks_) {
    if (tx_block.generation_ != generation) continue;
    const BlockState* state = FindBlockState(store, block_id);
    const bool durable =
        state != nullptr && state->allocated_ &&
        state->allocation_epoch_ == tx_block.allocation_epoch_ &&
        state->kind_ == BlockKind::kTransaction &&
        !IsActiveBlock(store, block_id) && !state->in_memory_ &&
        !state->flush_queued_ && !state->flush_in_progress_ && !state->freeing_;
    result.sealed_and_durable_ &= durable;
    result.live_tagged_bytes_ += tx_block.live_tagged_bytes_;
    result.dependency_pins_ += tx_block.dependency_pins_;
    result.blocks_.push_back(TxGenerationBlock{
        .block_id_ = block_id,
        .allocation_epoch_ = tx_block.allocation_epoch_,
        .generation_ = generation,
        .live_tagged_bytes_ = tx_block.live_tagged_bytes_,
    });
  }
  co_return result;
}

Task<std::vector<std::uint64_t>> StorageEngine::Impl::ListTxGenerationsLocal(
    WorkerStore& store, std::uint64_t closed_before) {
  co_await store.store_state_mutex_.Lock();
  UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
  std::vector<std::uint64_t> generations;
  generations.reserve(store.tx_generations_.size());
  for (const auto& entry : store.tx_generations_) {
    if (entry.first < closed_before) generations.push_back(entry.first);
  }
  co_return generations;
}

Task<bool> StorageEngine::Impl::TxGenerationHasRecordsLocal(
    WorkerStore& store, std::uint64_t generation) {
  co_await store.store_state_mutex_.Lock();
  UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
  const auto runtime = store.tx_generations_.find(generation);
  co_return runtime != store.tx_generations_.end() &&
      runtime->second->has_records_;
}

Task<absl::Status> StorageEngine::Impl::ForgetTxGenerationLocal(
    WorkerStore& store, std::uint64_t generation) {
  co_await store.store_state_mutex_.Lock();
  UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
  const auto runtime = store.tx_generations_.find(generation);
  if (runtime != store.tx_generations_.end() &&
      runtime->second->active_transactions_.load(std::memory_order_acquire) !=
          0) {
    co_return absl::FailedPreconditionError(
        "transaction generation regained an active lease");
  }
  for (const auto& [block_id, tx_block] : store.tx_blocks_) {
    if (tx_block.generation_ == generation) {
      co_return absl::FailedPreconditionError(
          "transaction generation still owns blocks");
    }
  }
  store.tx_generations_.erase(generation);
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::PromoteTxGenerationLocal(
    WorkerStore& store, std::uint64_t generation,
    std::shared_ptr<const absl::flat_hash_set<std::uint64_t>> committed) {
  auto inspected = co_await InspectTxGenerationLocal(store, generation, false);
  if (!inspected.ok()) co_return inspected.status();
  for (const TxGenerationBlock& block : inspected->blocks_) {
    if (block.live_tagged_bytes_ == 0) continue;

    co_await store.store_state_mutex_.Lock();
    BlockState* source = FindBlockState(store, block.block_id_);
    const auto tx_block = store.tx_blocks_.find(block.block_id_);
    if (source == nullptr || tx_block == store.tx_blocks_.end() ||
        source->allocation_epoch_ != block.allocation_epoch_ ||
        tx_block->second.generation_ != generation || source->in_memory_ ||
        source->flush_queued_ || source->flush_in_progress_ ||
        source->defragging_ || source->freeing_ || source->pins_ != 0) {
      store.store_state_mutex_.Unlock(*store.worker_);
      continue;
    }
    source->defragging_ = true;
    const auto [file_id, offset] = FileOffset(block.block_id_);
    store.store_state_mutex_.Unlock(*store.worker_);

    absl::Status promoted = co_await SalvageBlockRecords(
        store, block.block_id_, *source, file_id, offset, committed);

    std::vector<RelocationDurabilityFence> fences;
    co_await store.store_state_mutex_.Lock();
    if (auto owed = store.pending_relocation_fences_.find(block.block_id_);
        owed != store.pending_relocation_fences_.end()) {
      fences = std::move(owed->second);
      store.pending_relocation_fences_.erase(owed);
    }
    BlockState* current = FindBlockState(store, block.block_id_);
    if (current != nullptr &&
        current->allocation_epoch_ == block.allocation_epoch_) {
      current->defragging_ = false;
    }
    store.store_state_mutex_.Unlock(*store.worker_);
    if (!promoted.ok()) co_return promoted;
    for (const RelocationDurabilityFence& destination : fences) {
      absl::Status durable = co_await AwaitRelocationDurable(destination);
      if (!durable.ok()) co_return durable;
    }
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::RetireTxGenerationLocal(
    WorkerStore& store, std::uint64_t generation) {
  std::vector<std::uint64_t> released;
  {
    co_await store.store_state_mutex_.Lock();
    UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
    for (const auto& [block_id, tx_block] : store.tx_blocks_) {
      if (tx_block.generation_ != generation) continue;
      const BlockState* state = FindBlockState(store, block_id);
      if (state == nullptr ||
          state->allocation_epoch_ != tx_block.allocation_epoch_ ||
          state->kind_ != BlockKind::kTransaction || state->in_memory_ ||
          state->flush_queued_ || state->flush_in_progress_ ||
          state->defragging_ || state->freeing_ || state->pins_ != 0 ||
          tx_block.live_tagged_bytes_ != 0 || tx_block.dependency_pins_ != 0 ||
          IsActiveBlock(store, block_id)) {
        co_return absl::FailedPreconditionError(
            "transaction generation changed before retirement");
      }
      released.push_back(block_id);
    }
    for (std::uint64_t block_id : released) {
      BlockState* state = FindBlockState(store, block_id);
      assert(state != nullptr);
      state->freeing_ = true;
      DestroyBlockState(store, block_id);
    }
    store.active_tx_blocks_.erase(generation);
    // Retirement requires active_transactions_ == 0, so no writer can still
    // be queued on or own this generation's allocation gate.
    store.active_tx_block_allocation_mutexes_.erase(generation);
  }
  if (released.empty()) co_return absl::OkStatus();
  const std::vector<std::uint64_t> retired_blocks = released;
  const std::size_t released_count = released.size();
  absl::Status returned = co_await ReturnColdBlocks(std::move(released));
  if (returned.ok()) {
    // External-key extents remain recovery dependencies until the transaction
    // block's allocation bit is durably clear. Ordinary record blocks release
    // the same debt in ReleaseEmptyBlock; transaction generations retire by a
    // separate path and must perform the matching handoff here.
    for (const std::uint64_t block_id : retired_blocks) {
      auto deferred = store.deferred_dependent_extent_reclaims_.find(block_id);
      if (deferred == store.deferred_dependent_extent_reclaims_.end()) {
        continue;
      }
      std::vector<ExtentManifest> manifests = std::move(deferred->second);
      store.deferred_dependent_extent_reclaims_.erase(deferred);
      for (const ExtentManifest& manifest : manifests) {
        SpawnExtentReclaim(store, manifest);
      }
    }
    tx_cleaner_retired_blocks_.fetch_add(released_count,
                                         std::memory_order_relaxed);
    space_reclaim_generation_.fetch_add(1, std::memory_order_release);
  }
  co_return returned;
}

Task<absl::Status> StorageEngine::Impl::RunTxCleaner() {
  // Close the current generation only after it has actually received a
  // record. Empty current generations are left in place, so repeated retries
  // of an older blocked generation do not manufacture unbounded empty ones.
  const unsigned coordinator = celer::ThisWorker().id_;
  std::uint64_t current =
      current_tx_generation_.load(std::memory_order_seq_cst);
  bool current_has_records = false;
  for (unsigned owner = 0; owner < worker_count_; ++owner) {
    bool local_has_records = false;
    if (owner == coordinator) {
      local_has_records =
          co_await TxGenerationHasRecordsLocal(*stores_[owner], current);
    } else {
      local_has_records =
          co_await celer::SubmitTaskTo(owner, [this, owner, current]() {
            return TxGenerationHasRecordsLocal(*stores_[owner], current);
          });
    }
    current_has_records |= local_has_records;
  }
  if (current_has_records) {
    if (current == std::numeric_limits<std::uint64_t>::max()) {
      co_return absl::OutOfRangeError("transaction generation exhausted");
    }
    if (!current_tx_generation_.compare_exchange_strong(
            current, current + 1, std::memory_order_seq_cst,
            std::memory_order_seq_cst)) {
      // Normal runtime has one elected cleaner and no other generation
      // rotator. Recovery may only advance the id before serving traffic, so a
      // concurrent change here is an invariant violation rather than a state
      // to guess through.
      co_return absl::AbortedError(
          "transaction generation changed during cleaner rotation");
    }
  }

  const std::uint64_t closed_before =
      current_tx_generation_.load(std::memory_order_acquire);
  std::vector<std::uint64_t> generations;
  for (unsigned owner = 0; owner < worker_count_; ++owner) {
    std::vector<std::uint64_t> local;
    if (owner == coordinator) {
      local = co_await ListTxGenerationsLocal(*stores_[owner], closed_before);
    } else {
      local =
          co_await celer::SubmitTaskTo(owner, [this, owner, closed_before]() {
            return ListTxGenerationsLocal(*stores_[owner], closed_before);
          });
    }
    generations.insert(generations.end(), local.begin(), local.end());
  }
  std::sort(generations.begin(), generations.end());
  generations.erase(std::unique(generations.begin(), generations.end()),
                    generations.end());

  for (std::uint64_t generation : generations) {
    auto committed = std::make_shared<absl::flat_hash_set<std::uint64_t>>();
    std::uint64_t active_transactions = 0;
    for (unsigned owner = 0; owner < worker_count_; ++owner) {
      absl::StatusOr<TxGenerationLocalState> local;
      if (owner == coordinator) {
        local = co_await InspectTxGenerationLocal(*stores_[owner], generation,
                                                  false);
      } else {
        local = co_await celer::SubmitTaskTo(owner, [this, owner,
                                                     generation]() {
          return InspectTxGenerationLocal(*stores_[owner], generation, false);
        });
      }
      if (!local.ok()) co_return local.status();
      active_transactions += local->active_transactions_;
      committed->insert(local->committed_txids_.begin(),
                        local->committed_txids_.end());
    }
    if (active_transactions != 0) {
      tx_cleaner_dirty_.store(true, std::memory_order_release);
      continue;
    }

    internal::TxGenerationReadiness readiness{
        .sealed_and_durable_ = true,
    };
    for (unsigned owner = 0; owner < worker_count_; ++owner) {
      absl::StatusOr<TxGenerationLocalState> local;
      if (owner == coordinator) {
        local = co_await InspectTxGenerationLocal(*stores_[owner], generation,
                                                  true);
      } else {
        local = co_await celer::SubmitTaskTo(owner, [this, owner,
                                                     generation]() {
          return InspectTxGenerationLocal(*stores_[owner], generation, true);
        });
      }
      if (!local.ok()) co_return local.status();
      readiness.active_transactions_ += local->active_transactions_;
      readiness.live_tagged_bytes_ += local->live_tagged_bytes_;
      readiness.dependency_pins_ += local->dependency_pins_;
      readiness.sealed_and_durable_ &= local->sealed_and_durable_;
      committed->insert(local->committed_txids_.begin(),
                        local->committed_txids_.end());
    }
    if (readiness.active_transactions_ != 0 || !readiness.sealed_and_durable_) {
      tx_cleaner_dirty_.store(true, std::memory_order_release);
      continue;
    }

    auto frozen_committed =
        std::shared_ptr<const absl::flat_hash_set<std::uint64_t>>(
            std::move(committed));
    for (unsigned owner = 0; owner < worker_count_; ++owner) {
      absl::Status promoted;
      if (owner == coordinator) {
        promoted = co_await PromoteTxGenerationLocal(
            *stores_[owner], generation, frozen_committed);
      } else {
        promoted = co_await celer::SubmitTaskTo(
            owner, [this, owner, generation, frozen_committed]() {
              return PromoteTxGenerationLocal(*stores_[owner], generation,
                                              frozen_committed);
            });
      }
      if (!promoted.ok()) co_return promoted;
    }

    readiness.active_transactions_ = 0;
    readiness.live_tagged_bytes_ = 0;
    readiness.dependency_pins_ = 0;
    readiness.sealed_and_durable_ = true;
    for (unsigned owner = 0; owner < worker_count_; ++owner) {
      absl::StatusOr<TxGenerationLocalState> local;
      if (owner == coordinator) {
        local = co_await InspectTxGenerationLocal(*stores_[owner], generation,
                                                  false);
      } else {
        local = co_await celer::SubmitTaskTo(owner, [this, owner,
                                                     generation]() {
          return InspectTxGenerationLocal(*stores_[owner], generation, false);
        });
      }
      if (!local.ok()) co_return local.status();
      readiness.active_transactions_ += local->active_transactions_;
      readiness.live_tagged_bytes_ += local->live_tagged_bytes_;
      readiness.dependency_pins_ += local->dependency_pins_;
      readiness.sealed_and_durable_ &= local->sealed_and_durable_;
    }
    if (!internal::CanReclaimTxGeneration(readiness)) {
      tx_cleaner_dirty_.store(true, std::memory_order_release);
      continue;
    }

    for (unsigned owner = 0; owner < worker_count_; ++owner) {
      absl::Status retired;
      if (owner == coordinator) {
        retired = co_await RetireTxGenerationLocal(*stores_[owner], generation);
      } else {
        retired =
            co_await celer::SubmitTaskTo(owner, [this, owner, generation]() {
              return RetireTxGenerationLocal(*stores_[owner], generation);
            });
      }
      if (!retired.ok()) co_return retired;
    }
    for (unsigned owner = 0; owner < worker_count_; ++owner) {
      absl::Status forgotten;
      if (owner == coordinator) {
        forgotten =
            co_await ForgetTxGenerationLocal(*stores_[owner], generation);
      } else {
        forgotten =
            co_await celer::SubmitTaskTo(owner, [this, owner, generation]() {
              return ForgetTxGenerationLocal(*stores_[owner], generation);
            });
      }
      if (!forgotten.ok()) co_return forgotten;
    }
    tx_cleaner_retired_generations_.fetch_add(1, std::memory_order_relaxed);
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::DrainTxCleanerForShutdown() {
  if (active_tx_commits_.load(std::memory_order_acquire) != 0) {
    co_return absl::FailedPreconditionError(
        "transaction commits are still active at shutdown");
  }

  // Every worker has completed its normal shutdown flush and reached the
  // checkpoint-ready barrier, so no online cleaner can still be running.
  // Keep the ownership check nonetheless: checkpoint publication is optional,
  // and an invariant violation must degrade to cold recovery instead of racing
  // two generation coordinators.
  bool expected = false;
  if (!tx_cleaner_running_.compare_exchange_strong(
          expected, true, std::memory_order_acq_rel,
          std::memory_order_acquire)) {
    co_return absl::FailedPreconditionError(
        "transaction cleaner is still running at shutdown");
  }
  struct RunningGuard {
    std::atomic<bool>* running_;
    ~RunningGuard() { running_->store(false, std::memory_order_release); }
  } guard{&tx_cleaner_running_};

  // Promotion and retirement can make another pass necessary (for example,
  // after relocation releases the last dependency pin). Bound only the number
  // of fixed-point passes, not their I/O duration: returning an error leaves
  // the durable transaction records intact for normal cold recovery.
  constexpr unsigned kMaxShutdownCleanerRounds = 8;
  for (unsigned round = 0; round < kMaxShutdownCleanerRounds; ++round) {
    tx_cleaner_dirty_.store(false, std::memory_order_release);
    tx_cleaner_rounds_.fetch_add(1, std::memory_order_relaxed);
    absl::Status status = co_await RunTxCleaner();
    if (!status.ok()) {
      tx_cleaner_failures_.fetch_add(1, std::memory_order_relaxed);
      tx_cleaner_dirty_.store(true, std::memory_order_release);
      co_return status;
    }
    if (!tx_cleaner_dirty_.load(std::memory_order_acquire)) {
      co_return absl::OkStatus();
    }
  }

  co_return absl::FailedPreconditionError(
      "transaction generations did not quiesce during shutdown");
}

}  // namespace keylane::storage
