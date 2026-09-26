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

#include <algorithm>
#include <coroutine>
#include <exception>
#include <limits>
#include <utility>
#include <vector>

#include "impl.h"

namespace lavik::storage {

namespace {

std::int64_t MonotonicMillis() noexcept {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

// Promotion only mutates the source owner's store. Each submitted task returns
// to the coordinator before completing this join, so the borrowed generation
// snapshot and commit-decision map remain alive even when one owner fails.
struct CleanerOwnerJoin {
  std::size_t pending_ = 0;
  std::coroutine_handle<> waiter_{};
  absl::Status error_;
  std::exception_ptr exception_;

  void Complete(absl::Status status) {
    if (!status.ok() && error_.ok()) error_ = std::move(status);
    Arrive();
  }

  void CompleteException(std::exception_ptr error) {
    if (!exception_) exception_ = std::move(error);
    Arrive();
  }

  void Arrive() {
    assert(pending_ != 0);
    if (--pending_ == 0 && waiter_) {
      const auto waiter = std::exchange(waiter_, {});
      bycorf::ThisWorker().self_->Enqueue(waiter);
    }
  }

  auto Join() {
    struct Awaiter {
      CleanerOwnerJoin* join_;
      bool await_ready() const noexcept { return join_->pending_ == 0; }
      void await_suspend(std::coroutine_handle<> waiter) const noexcept {
        join_->waiter_ = waiter;
      }
      void await_resume() const noexcept {}
    };
    return Awaiter{this};
  }
};

template <typename Step>
Task<absl::Status> RunCleanerOwnerStep(unsigned owner, Step step,
                                       CleanerOwnerJoin* join) {
  absl::Status status;
  try {
    if (owner == bycorf::ThisWorker().id_) {
      status = co_await step();
    } else {
      status = co_await bycorf::SubmitTaskTo(owner, std::move(step));
    }
  } catch (...) {
    // A detached task must still settle the join before the coordinator can
    // release the generation snapshot borrowed by all other owners.
    join->CompleteException(std::current_exception());
    co_return absl::OkStatus();
  }
  join->Complete(std::move(status));
  co_return absl::OkStatus();
}

template <typename StepAt>
Task<absl::Status> ForEachCleanerOwner(unsigned count, bool parallel,
                                       StepAt step_at) {
  if (count == 0) co_return absl::OkStatus();
  if (!parallel || count == 1) {
    // Shutdown's checkpoint drain runs while workers are stopping. It must
    // use awaited tasks because Spawn may discard new detached work then.
    for (unsigned index = 0; index < count; ++index) {
      auto [owner, step] = step_at(index);
      absl::Status status;
      if (owner == bycorf::ThisWorker().id_) {
        status = co_await step();
      } else {
        status = co_await bycorf::SubmitTaskTo(owner, std::move(step));
      }
      if (!status.ok()) co_return status;
    }
    co_return absl::OkStatus();
  }
  CleanerOwnerJoin join;
  std::vector<Task<absl::Status>> tasks;
  tasks.reserve(count);
  // Construct every coroutine before the first launch. A frame-allocation
  // failure must not leave already-running owners borrowing a dead join.
  for (unsigned index = 0; index < count; ++index) {
    auto [owner, step] = step_at(index);
    tasks.push_back(RunCleanerOwnerStep(owner, std::move(step), &join));
  }
  join.pending_ = count;
  for (auto& task : tasks) bycorf::ThisWorker().self_->Spawn(std::move(task));
  co_await join.Join();
  if (join.exception_) std::rethrow_exception(join.exception_);
  co_return std::move(join.error_);
}

}  // namespace

void StorageEngine::Impl::InitializeTxWrites(
    std::uint64_t txid, std::span<TxShardWrites> writes,
    MutationPrecondition precondition) {
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
    shard.mutation_precondition_ = precondition;
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

void StorageEngine::Impl::NoteTxRecordLocal(
    WorkerStore& store, std::uint64_t block_id, std::uint64_t allocation_epoch,
    std::uint64_t generation, std::uint64_t txid, std::uint32_t bytes,
    bool commit, const TxShardWrites* receipt, std::uint32_t record_end,
    std::uint64_t dependency_txid) {
  assert(generation != 0 && txid != 0 && bytes != 0);
  WorkerStore::TxBlockRuntime& block = store.tx_blocks_[block_id];
  if (block.allocation_epoch_ != allocation_epoch ||
      block.generation_ != generation) {
    block = WorkerStore::TxBlockRuntime{
        .allocation_epoch_ = allocation_epoch,
        .generation_ = generation,
        .txids_ = {},
        .commit_txids_ = {},
    };
  }
  if (!commit) {
    block.live_tagged_bytes_ += bytes;
  } else {
    block.commit_txids_.insert(txid);
  }
  block.txids_.insert_or_assign(
      txid, receipt != nullptr ? std::weak_ptr<void>(receipt->generation_lease_)
                               : std::weak_ptr<void>{});
  if (dependency_txid != 0)
    block.txids_.insert_or_assign(
        dependency_txid, receipt != nullptr
                             ? std::weak_ptr<void>(receipt->generation_lease_)
                             : std::weak_ptr<void>{});
  block.last_append_ms_ = MonotonicMillis();
  auto& generation_state = store.tx_generations_[generation];
  if (generation_state == nullptr) {
    generation_state = std::make_shared<TxGenerationRuntime>();
  }
  generation_state->has_records_ = true;
  if (commit) {
    generation_state->committed_txids_.insert(txid);
    if (record_end != 0) {
      generation_state->commit_fences_.insert_or_assign(
          txid,
          RelocationDurabilityFence{
              .block_id_ = block_id,
              .allocation_epoch_ = allocation_epoch,
              .block_owner_ = static_cast<std::uint16_t>(store.worker_->id()),
              .committed_bytes_ = record_end,
          });
    }
  }
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

absl::Status StorageEngine::Impl::ConfigureTxBacklogLimit(std::uint64_t bytes) {
  if (bytes < kStorageBlockBytes) {
    return absl::InvalidArgumentError(
        "transaction backlog limit must be at least 8 MiB");
  }
  tx_backlog_limit_bytes_.store(bytes, std::memory_order_release);
  return absl::OkStatus();
}

void StorageEngine::Impl::NoteTxBlockSealedLocal(WorkerStore& store,
                                                 std::uint64_t block_id) {
  const auto found = store.tx_blocks_.find(block_id);
  if (found == store.tx_blocks_.end() || found->second.counted_backlog_) return;
  const BlockState* state = FindBlockState(store, block_id);
  if (state == nullptr ||
      state->allocation_epoch_ != found->second.allocation_epoch_ ||
      state->committed_bytes_ < kBlockHeaderBytes)
    return;
  // Charge the occupied Tx-record bytes, not the entire 8 MiB allocation.
  // Sparse blocks sealed after idleness therefore consume only their actual
  // backlog budget, while their physical allocation remains tracked by the
  // block allocator.
  found->second.backlog_bytes_ = state->committed_bytes_ - kBlockHeaderBytes;
  found->second.counted_backlog_ = true;
  auto& generation = store.tx_generations_[found->second.generation_];
  if (generation == nullptr)
    generation = std::make_shared<TxGenerationRuntime>();
  generation->has_records_ = true;
  store.tx_backlog_bytes_.fetch_add(found->second.backlog_bytes_,
                                    std::memory_order_release);
  tx_cleaner_dirty_.store(true, std::memory_order_release);
}

bool StorageEngine::Impl::TxBacklogAtLimit() const noexcept {
  if (tx_cleaner_cooldown_ms_.load(std::memory_order_acquire) == 0)
    return false;
  const std::uint64_t limit =
      tx_backlog_limit_bytes_.load(std::memory_order_acquire);
  for (const auto& store : stores_)
    if (store->tx_backlog_bytes_.load(std::memory_order_acquire) > limit)
      return true;
  return false;
}

Task<absl::Status> StorageEngine::Impl::WaitForTxBacklog() {
  // This is an admission gate, never a per-append limit. The caller has no
  // generation lease; a transaction admitted below the threshold can finish
  // even if its own blocks take the worker far above it.
  bool counted_wait = false;
  for (;;) {
    if (shutdown_flush_requested_.load(std::memory_order_acquire))
      co_return absl::UnavailableError("storage is shutting down");
    if (!TxBacklogAtLimit()) co_return absl::OkStatus();
    if (!counted_wait) {
      tx_backlog_waits_.fetch_add(1, std::memory_order_relaxed);
      counted_wait = true;
    }
    const std::int64_t now = MonotonicMillis();
    std::int64_t next = tx_backlog_retry_ms_.load(std::memory_order_acquire);
    if (now >= next && tx_backlog_retry_ms_.compare_exchange_strong(
                           next, now + 50, std::memory_order_acq_rel,
                           std::memory_order_acquire)) {
      const absl::Status cleaned = co_await MaybeRunTxCleaner(true);
      if (!cleaned.ok() && !absl::IsFailedPrecondition(cleaned) &&
          !absl::IsAborted(cleaned) && !absl::IsResourceExhausted(cleaned))
        co_return cleaned;
    }
    const absl::Status waited = co_await bycorf::SleepFor(
        *bycorf::ThisWorker().self_, std::chrono::milliseconds(2));
    if (!waited.ok()) co_return waited;
  }
}

void StorageEngine::Impl::SealIdleTxBlocksLocal(WorkerStore& store) {
  if (tx_cleaner_cooldown_ms_.load(std::memory_order_acquire) == 0) return;
  constexpr std::int64_t kIdleSealMs = 60'000;
  const std::int64_t now = MonotonicMillis();
  for (auto& [generation, active] : store.active_tx_blocks_) {
    (void)generation;
    if (!active.has_value()) continue;
    const auto found = store.tx_blocks_.find(active->block_id_);
    if (found == store.tx_blocks_.end() ||
        found->second.allocation_epoch_ != active->allocation_epoch_ ||
        found->second.last_append_ms_ == 0 ||
        now - found->second.last_append_ms_ < kIdleSealMs)
      continue;
    RequestFlush(store, active->block_id_);
    NoteTxBlockSealedLocal(store, active->block_id_);
    active.reset();
    // The timer closes only this physical stream. A live transaction keeps
    // its generation lease and may append a commit in a successor block.
    tx_cleaner_dirty_.store(true, std::memory_order_release);
    tx_cleaner_next_run_ms_.store(0, std::memory_order_release);
  }
}

Task<absl::Status> StorageEngine::Impl::BeforeGroupedTransaction(
    WorkerStore& store, std::uint64_t append_bytes) {
  // Ordinary grouped snapshots can fill transaction blocks long before the
  // periodic cooldown expires. Cleaning after this command has acquired the
  // same generation would leave its own lease preventing reclamation.
  if (tx_cleaner_cooldown_ms_.load(std::memory_order_acquire) == 0)
    co_return absl::OkStatus();  // Preserve explicit maintenance disabling.
  constexpr std::uint64_t payload = kStorageBlockBytes - kBlockHeaderBytes;
  const auto needed = append_bytes / payload + (append_bytes % payload != 0);
  const auto deadline = MonotonicMillis() + 1000;
  unsigned rounds = 0;
  try {
    for (;;) {
      if (shutdown_flush_requested_.load(std::memory_order_acquire))
        co_return absl::UnavailableError("storage is shutting down");
      // Observe only on this stream's owner, without suspension. Small
      // successors can reuse the current generation's staging capacity even
      // when every free foreground block is occupied. Forcing a rotation in
      // that case would discard usable space and require a fresh tx block:
      // a snapshot may retain the old extents until it obtains the key intent
      // this very writer holds. This is not append admission; WriteRecord
      // still validates the stream and remaining bytes after its own waits.
      const auto generation =
          current_tx_generation_.load(std::memory_order_acquire);
      const auto active = store.active_tx_blocks_.find(generation);
      if (active != store.active_tx_blocks_.end() && active->second) {
        const auto& stream = *active->second;
        const auto* state = FindBlockState(store, stream.block_id_);
        if (state != nullptr && state->allocated_ && state->in_memory_ &&
            !state->freeing_ && !state->release_pending_ &&
            state->allocation_epoch_ == stream.allocation_epoch_ &&
            state->kind_ == BlockKind::kTransaction) {
          const auto used =
              std::max(stream.committed_bytes_, state->committed_bytes_);
          // An in-flight flush owns only its captured prefix, so still-open
          // staging bytes remain reusable; the writer rechecks after waiting.
          if (used <= kStorageBlockBytes &&
              append_bytes <= kStorageBlockBytes - used)
            co_return absl::OkStatus();
        }
      }
      std::uint64_t free = 0;
      std::uint64_t capacity = 0;
      for (std::size_t index = 0; index < devices_.size(); ++index) {
        if (bycorf::SpdkStorageEnabled()) {
          if (std::find(store.home_devices_.begin(), store.home_devices_.end(),
                        index) == store.home_devices_.end())
            continue;
        }
        const auto available = co_await bycorf::SubmitTo(
            device_allocators_[index]->owner_, [this, index] {
              const auto& allocator = *device_allocators_[index];
              const auto& device = devices_[index];
              const auto pristine =
                  allocator.next_pristine_ < device.capacity_blocks_
                      ? device.capacity_blocks_ - allocator.next_pristine_
                      : 0;
              return pristine + allocator.ready_blocks_.size() +
                     allocator.cold_free_.size();
            });
        const auto reserve = DefragReserveForDevice(index);
        free += available > reserve ? available - reserve : 0;
        capacity += ForegroundBlocksForDevice(index);
      }
      // Include staging/fragmentation headroom, but do not reject a write
      // based on this approximate snapshot. The allocator remains
      // authoritative.
      if (free > std::max(needed + 2, capacity / 8)) co_return absl::OkStatus();
      if (MonotonicMillis() >= deadline || rounds == 4)
        co_return absl::OkStatus();
      if (!tx_cleaner_running_.load(std::memory_order_acquire)) {
        const auto cleaned = co_await MaybeRunTxCleaner(true);
        if (!cleaned.ok()) {
          // Maintenance admission/pin races are not evidence that the user's
          // append cannot fit. The elected coordinator has rearmed dirty
          // state; let the ordinary allocator make the final capacity choice.
          // Corruption and I/O errors must not become a successful write.
          if (!store.write_failed_ &&
              !epoch_metadata_failed_.load(std::memory_order_acquire) &&
              (absl::IsResourceExhausted(cleaned) || absl::IsAborted(cleaned) ||
               absl::IsFailedPrecondition(cleaned)))
            co_return absl::OkStatus();
          co_return cleaned;
        }
        ++rounds;
      }
      const auto waited = co_await bycorf::SleepFor(
          *store.worker_, std::chrono::milliseconds(1));
      if (!waited.ok()) co_return waited;
    }
  } catch (const std::bad_alloc&) {
    RecordMemoryRejection();
    co_return absl::ResourceExhaustedError(
        "OOM grouped space-pressure cleanup");
  }
}

Task<absl::Status> StorageEngine::Impl::MaybeRunTxCleaner(bool force) {
  const std::uint32_t cooldown =
      tx_cleaner_cooldown_ms_.load(std::memory_order_acquire);
  if (cooldown == 0 ||
      (!force && !tx_cleaner_dirty_.load(std::memory_order_acquire))) {
    co_return absl::OkStatus();
  }
  const std::int64_t now = MonotonicMillis();
  if (!force && now < tx_cleaner_next_run_ms_.load(std::memory_order_acquire)) {
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
  LAVIK_FAULT_INJECT(
      static std::atomic<bool> cleaner_failure_claimed = false;
      bool expected_failure = false;
      if (std::getenv("LAVIK_FAIL_TX_CLEANER_ONCE") != nullptr &&
          cleaner_failure_claimed.compare_exchange_strong(
              expected_failure, true, std::memory_order_acq_rel)) {
        status = absl::FailedPreconditionError(
            "injected retryable transaction cleaner failure");
      });
  if (status.ok()) status = co_await RunTxCleaner();
  if (!status.ok()) {
    if (!(absl::IsCancelled(status) &&
          shutdown_flush_requested_.load(std::memory_order_acquire)))
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
      NoteTxBlockSealedLocal(store, block.block_id_);
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
    result.commit_fences_.assign(runtime->second->commit_fences_.begin(),
                                 runtime->second->commit_fences_.end());
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
    TxGenerationBlock block{
        .block_id_ = block_id,
        .allocation_epoch_ = tx_block.allocation_epoch_,
        .generation_ = generation,
        .live_tagged_bytes_ = tx_block.live_tagged_bytes_,
        .txids_ = {},
        .commit_txids_ = {},
        .sealed_and_durable_ = durable,
    };
    block.txids_.reserve(tx_block.txids_.size());
    for (const auto& [txid, lease] : tx_block.txids_) {
      block.txids_.push_back(txid);
      block.active_transaction_ |= !lease.expired();
    }
    block.commit_txids_.assign(tx_block.commit_txids_.begin(),
                               tx_block.commit_txids_.end());
    result.blocks_.push_back(std::move(block));
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
    std::shared_ptr<const absl::flat_hash_set<std::uint64_t>> committed,
    bool shutdown_drain, std::optional<std::uint64_t> only_block) {
  auto inspected = co_await InspectTxGenerationLocal(store, generation, false);
  if (!inspected.ok()) co_return inspected.status();
  for (const TxGenerationBlock& block : inspected->blocks_) {
    if (only_block.has_value() && block.block_id_ != *only_block) continue;
    // Previous block relocations have reached their durability fences. Keep
    // all generation decisions/source allocations intact when an online round
    // yields to shutdown; the explicit checkpoint drain may finish the round.
    if (!shutdown_drain &&
        shutdown_flush_requested_.load(std::memory_order_acquire))
      co_return absl::CancelledError(
          "online transaction cleaner yielding to shutdown");
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

Task<absl::Status> StorageEngine::Impl::RetireTxBlockLocal(
    WorkerStore& store, const TxGenerationBlock& block) {
  std::uint32_t retired_backlog_bytes = 0;
  {
    co_await store.store_state_mutex_.Lock();
    UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
    const auto found = store.tx_blocks_.find(block.block_id_);
    BlockState* state = FindBlockState(store, block.block_id_);
    if (found == store.tx_blocks_.end() || state == nullptr ||
        found->second.allocation_epoch_ != block.allocation_epoch_ ||
        found->second.generation_ != block.generation_ ||
        state->allocation_epoch_ != block.allocation_epoch_ ||
        state->kind_ != BlockKind::kTransaction || state->in_memory_ ||
        state->flush_queued_ || state->flush_in_progress_ ||
        state->defragging_ || state->freeing_ || state->pins_ != 0 ||
        found->second.live_tagged_bytes_ != 0 ||
        found->second.dependency_pins_ != 0 ||
        !found->second.counted_backlog_ ||
        IsActiveBlock(store, block.block_id_)) {
      co_return absl::FailedPreconditionError(
          "transaction block changed before retirement");
    }
    retired_backlog_bytes = found->second.backlog_bytes_;
    state->freeing_ = true;
    DestroyBlockState(store, block.block_id_);
  }
  absl::Status returned =
      co_await ReturnColdBlocks(std::vector<std::uint64_t>{block.block_id_});
  if (returned.ok()) {
    store.tx_backlog_bytes_.fetch_sub(retired_backlog_bytes,
                                      std::memory_order_release);
    // UUID and extent dependencies remain live until the source allocation
    // bit is durably cleared, even after its tagged winners have moved.
    store.indirect_key_references_.erase(
        {block.block_id_, block.allocation_epoch_});
    auto deferred =
        store.deferred_dependent_extent_reclaims_.find(block.block_id_);
    if (deferred != store.deferred_dependent_extent_reclaims_.end()) {
      std::vector<ExtentManifest> manifests = std::move(deferred->second);
      store.deferred_dependent_extent_reclaims_.erase(deferred);
      for (const ExtentManifest& manifest : manifests)
        SpawnExtentReclaim(store, manifest);
    }
    tx_cleaner_retired_blocks_.fetch_add(1, std::memory_order_relaxed);
    space_reclaim_generation_.fetch_add(1, std::memory_order_release);
  }
  co_return returned;
}

Task<absl::Status> StorageEngine::Impl::RetireTxGenerationLocal(
    WorkerStore& store, std::uint64_t generation) {
  std::vector<std::uint64_t> released;
  std::uint64_t retired_backlog_bytes = 0;
  absl::flat_hash_map<std::uint64_t, std::uint64_t> retired_epochs;
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
          !tx_block.counted_backlog_ || IsActiveBlock(store, block_id)) {
        co_return absl::FailedPreconditionError(
            "transaction generation changed before retirement");
      }
      released.push_back(block_id);
      retired_backlog_bytes += tx_block.backlog_bytes_;
      retired_epochs.emplace(block_id, state->allocation_epoch_);
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
    store.tx_backlog_bytes_.fetch_sub(retired_backlog_bytes,
                                      std::memory_order_release);
    // UUIDs and deferred manifests remain recovery dependencies until the
    // transaction block's allocation bit is durably clear. Ordinary record
    // blocks release the same debt in ReleaseEmptyBlock; transaction
    // generations retire by a separate path and must perform the matching
    // handoff here.
    for (const std::uint64_t block_id : retired_blocks) {
      store.indirect_key_references_.erase(
          {block_id, retired_epochs.at(block_id)});
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

Task<absl::Status> StorageEngine::Impl::RunTxCleaner(bool shutdown_drain) {
  if (!shutdown_drain &&
      shutdown_flush_requested_.load(std::memory_order_acquire))
    co_return absl::CancelledError(
        "online transaction cleaner yielding to shutdown");
  // Close the current generation only after it has actually received a
  // record. Empty current generations are left in place, so repeated retries
  // of an older blocked generation do not manufacture unbounded empty ones.
  const unsigned coordinator = bycorf::ThisWorker().id_;
  std::uint64_t current =
      current_tx_generation_.load(std::memory_order_seq_cst);
  bool current_has_records = false;
  bool draining_leases = false;
  for (unsigned owner = 0; owner < worker_count_; ++owner) {
    auto inspect = [this, owner, current]() -> Task<std::pair<bool, bool>> {
      auto& store = *stores_[owner];
      co_await store.store_state_mutex_.Lock();
      UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
      bool has_records = false;
      bool leased_closed = false;
      for (const auto& [generation, state] : store.tx_generations_) {
        if (generation == current) has_records = state->has_records_;
        if (generation < current &&
            state->active_transactions_.load(std::memory_order_acquire) != 0)
          leased_closed = true;
      }
      co_return std::pair{has_records, leased_closed};
    };
    std::pair<bool, bool> local;
    if (owner == coordinator) {
      local = co_await inspect();
    } else {
      local = co_await bycorf::SubmitTaskTo(owner, inspect);
    }
    current_has_records |= local.first;
    draining_leases |= local.second;
  }
  // A short cooldown must not manufacture generations faster than accepted
  // transactions can commit. Each leased generation needs its own append
  // streams; enough of them can consume every foreground block and strand
  // their own commit decisions. Keep at most one closed generation with live
  // leases, while continuing to clean older settled/pinned generations below.
  // Pins alone do not inhibit rotation: an old snapshot must not stop newer
  // generations from being reclaimed. No new lease can join a closed id.
  if (current_has_records && !draining_leases) {
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
          co_await bycorf::SubmitTaskTo(owner, [this, owner, closed_before]() {
            return ListTxGenerationsLocal(*stores_[owner], closed_before);
          });
    }
    generations.insert(generations.end(), local.begin(), local.end());
  }
  std::sort(generations.begin(), generations.end());
  generations.erase(std::unique(generations.begin(), generations.end()),
                    generations.end());
  // A full or idle-sealed block in the current generation can settle even
  // while another transaction keeps that generation open.
  if (current_has_records && closed_before == current &&
      std::find(generations.begin(), generations.end(), closed_before) ==
          generations.end())
    generations.push_back(closed_before);

  for (std::uint64_t generation : generations) {
    if (!shutdown_drain &&
        shutdown_flush_requested_.load(std::memory_order_acquire))
      co_return absl::CancelledError(
          "online transaction cleaner yielding to shutdown");
    auto committed = std::make_shared<absl::flat_hash_set<std::uint64_t>>();
    absl::flat_hash_map<std::uint64_t, std::vector<RelocationDurabilityFence>>
        commit_fences;
    std::vector<TxGenerationLocalState> owner_states;
    owner_states.reserve(worker_count_);
    std::uint64_t active_transactions = 0;
    for (unsigned owner = 0; owner < worker_count_; ++owner) {
      absl::StatusOr<TxGenerationLocalState> local;
      if (owner == coordinator) {
        local = co_await InspectTxGenerationLocal(*stores_[owner], generation,
                                                  false);
      } else {
        local = co_await bycorf::SubmitTaskTo(owner, [this, owner,
                                                      generation]() {
          return InspectTxGenerationLocal(*stores_[owner], generation, false);
        });
      }
      if (!local.ok()) co_return local.status();
      active_transactions += local->active_transactions_;
      committed->insert(local->committed_txids_.begin(),
                        local->committed_txids_.end());
      for (const auto& [txid, fence] : local->commit_fences_)
        commit_fences[txid].push_back(fence);
      owner_states.push_back(std::move(*local));
    }
    // A decision block remains allocated until every physical tagged block
    // for its txid is gone. Otherwise recovery could lose the only commit
    // evidence while another worker still has that transaction's data.
    absl::flat_hash_map<std::uint64_t, std::size_t> tx_block_count;
    for (const auto& state : owner_states)
      for (const TxGenerationBlock& block : state.blocks_)
        for (std::uint64_t txid : block.txids_) ++tx_block_count[txid];

    // Commit fences are checked before each owner's promotion. Owners may
    // relocate concurrently because they have disjoint source streams and
    // worker-local index state. Keep retirement below this join: a decision
    // block cannot disappear while another owner's promoted copy is pending.
    const absl::Status promoted = co_await ForEachCleanerOwner(
        worker_count_, !shutdown_drain, [&](unsigned owner) {
          return std::pair{
              owner,
              [this, owner, generation, committed, shutdown_drain,
               &owner_states, &commit_fences]() -> Task<absl::Status> {
                for (const TxGenerationBlock& block :
                     owner_states[owner].blocks_) {
                  if (!block.sealed_and_durable_ || block.active_transaction_)
                    continue;
                  for (std::uint64_t txid : block.txids_) {
                    if (!committed->contains(txid)) continue;
                    if (const auto fence = commit_fences.find(txid);
                        fence != commit_fences.end()) {
                      for (const RelocationDurabilityFence& decision :
                           fence->second) {
                        const absl::Status durable =
                            co_await AwaitRelocationDurable(decision);
                        if (!durable.ok()) co_return durable;
                      }
                    }
                    // Recovered commit records have no runtime fence: their
                    // recovery scan already validated the durable record.
                  }
                  const absl::Status result = co_await PromoteTxGenerationLocal(
                      *stores_[owner], generation, committed, shutdown_drain,
                      block.block_id_);
                  if (!result.ok()) co_return result;
                }
                co_return absl::OkStatus();
              }};
        });
    if (!promoted.ok()) co_return promoted;
    for (unsigned owner = 0; owner < worker_count_; ++owner) {
      for (const TxGenerationBlock& block : owner_states[owner].blocks_) {
        if (!block.sealed_and_durable_ || block.active_transaction_) continue;
        bool has_decision_dependencies = false;
        for (std::uint64_t txid : block.commit_txids_)
          has_decision_dependencies |= tx_block_count[txid] > 1;
        if (has_decision_dependencies) continue;

        absl::Status retired;
        if (owner == coordinator) {
          retired = co_await RetireTxBlockLocal(*stores_[owner], block);
        } else {
          retired =
              co_await bycorf::SubmitTaskTo(owner, [this, owner, block]() {
                return RetireTxBlockLocal(*stores_[owner], block);
              });
        }
        if (absl::IsFailedPrecondition(retired)) {
          tx_cleaner_dirty_.store(true, std::memory_order_release);
          continue;  // Pins or a concurrent relocation still own this block.
        }
        if (!retired.ok()) co_return retired;
        for (std::uint64_t txid : block.txids_) {
          assert(tx_block_count[txid] != 0);
          --tx_block_count[txid];
        }
        tx_cleaner_dirty_.store(true, std::memory_order_release);
      }
    }

    if (generation >= closed_before) continue;
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
        local = co_await bycorf::SubmitTaskTo(owner, [this, owner,
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
    const absl::Status generation_promoted = co_await ForEachCleanerOwner(
        worker_count_, !shutdown_drain, [&](unsigned owner) {
          return std::pair{owner,
                           [this, owner, generation, frozen_committed,
                            shutdown_drain]() -> Task<absl::Status> {
                             co_return co_await PromoteTxGenerationLocal(
                                 *stores_[owner], generation, frozen_committed,
                                 shutdown_drain);
                           }};
        });
    if (!generation_promoted.ok()) co_return generation_promoted;

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
        local = co_await bycorf::SubmitTaskTo(owner, [this, owner,
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
            co_await bycorf::SubmitTaskTo(owner, [this, owner, generation]() {
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
            co_await bycorf::SubmitTaskTo(owner, [this, owner, generation]() {
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
  if (!tx_cleaner_running_.compare_exchange_strong(expected, true,
                                                   std::memory_order_acq_rel,
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
    absl::Status status = co_await RunTxCleaner(true);
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

}  // namespace lavik::storage
