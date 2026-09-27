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
// to the coordinator before completing this join, so the borrowed block
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
    // release the block snapshot borrowed by all other owners.
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
  auto lease = std::shared_ptr<void>(new std::uint8_t{0}, [this](void* token) {
    delete static_cast<std::uint8_t*>(token);
    tx_cleaner_dirty_.store(true, std::memory_order_release);
  });
  for (TxShardWrites& shard : writes) {
    shard.txid_ = txid;
    shard.transaction_lease_ = lease;
    shard.mutation_precondition_ = precondition;
  }
}

void StorageEngine::Impl::NoteTxRecordLocal(
    WorkerStore& store, std::uint64_t block_id, std::uint64_t allocation_epoch,
    std::uint64_t txid, std::uint32_t bytes, bool commit,
    const TxShardWrites* receipt, std::uint32_t record_end,
    std::uint64_t dependency_txid) {
  assert(txid != 0 && bytes != 0);
  WorkerStore::TxBlockRuntime& block = store.tx_blocks_[block_id];
  if (block.allocation_epoch_ != allocation_epoch) {
    block = WorkerStore::TxBlockRuntime{
        .allocation_epoch_ = allocation_epoch,
        .txids_ = {},
        .commit_ends_ = {},
    };
  }
  if (!commit) {
    block.live_tagged_bytes_ += bytes;
  } else {
    block.commit_ends_.insert_or_assign(txid, record_end);
  }
  block.txids_.insert_or_assign(
      txid, receipt != nullptr
                ? std::weak_ptr<void>(receipt->transaction_lease_)
                : std::weak_ptr<void>{});
  if (dependency_txid != 0)
    block.txids_.insert_or_assign(
        dependency_txid, receipt != nullptr
                             ? std::weak_ptr<void>(receipt->transaction_lease_)
                             : std::weak_ptr<void>{});
  block.last_append_ms_ = MonotonicMillis();
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
  // transaction lease; a transaction admitted below the threshold can finish
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
  auto& active = store.active_tx_block_;
  if (!active) return;
  const auto found = store.tx_blocks_.find(active->block_id_);
  if (found == store.tx_blocks_.end() ||
      found->second.allocation_epoch_ != active->allocation_epoch_ ||
      found->second.last_append_ms_ == 0 ||
      now - found->second.last_append_ms_ < kIdleSealMs)
    return;
  RequestFlush(store, active->block_id_);
  NoteTxBlockSealedLocal(store, active->block_id_);
  active.reset();
  // The timer closes only this physical stream. A live transaction keeps
  // its lease and may append a commit in the successor block.
  tx_cleaner_dirty_.store(true, std::memory_order_release);
  tx_cleaner_next_run_ms_.store(0, std::memory_order_release);
}

Task<absl::Status> StorageEngine::Impl::BeforeGroupedTransaction(
    WorkerStore& store, std::uint64_t append_bytes) {
  // Ordinary grouped snapshots can fill transaction blocks long before the
  // periodic cooldown expires. Cleaning after this command has acquired its
  // lease would leave that transaction preventing reclamation.
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
      // successors can reuse the current Tx stream's staging capacity even
      // when every free foreground block is occupied. Forcing a rotation in
      // that case would discard usable space and require a fresh tx block:
      // a snapshot may retain the old extents until it obtains the key intent
      // this very writer holds. This is not append admission; WriteRecord
      // still validates the stream and remaining bytes after its own waits.
      if (store.active_tx_block_) {
        const auto& stream = *store.active_tx_block_;
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

Task<absl::StatusOr<TxCleanerLocalState>>
StorageEngine::Impl::InspectTxBlocksLocal(WorkerStore& store, bool seal) {
  std::optional<RelocationDurabilityFence> fence;
  {
    co_await store.store_state_mutex_.Lock();
    UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
    bool active_writers = false;
    if (store.active_tx_block_) {
      const auto found =
          store.tx_blocks_.find(store.active_tx_block_->block_id_);
      if (found != store.tx_blocks_.end())
        for (const auto& [txid, lease] : found->second.txids_) {
          (void)txid;
          active_writers |= !lease.expired();
        }
    }
    // A settled tail must be sealed or a sparse transaction would wait for
    // the one-minute idle timer before it could be promoted. Keep a live
    // writer's stream open across cleaner rounds.
    if (seal && store.active_tx_block_ && !active_writers) {
      const ActiveBlock block = *store.active_tx_block_;
      RequestFlush(store, block.block_id_);
      NoteTxBlockSealedLocal(store, block.block_id_);
      store.active_tx_block_.reset();
      fence = RelocationDurabilityFence{
          .block_id_ = block.block_id_,
          .allocation_epoch_ = block.allocation_epoch_,
          .block_owner_ = static_cast<std::uint16_t>(store.worker_->id()),
          .committed_bytes_ = block.committed_bytes_,
      };
    }
  }
  if (fence) {
    const absl::Status durable =
        co_await AwaitRelocationDurableLocal(store, *fence);
    if (!durable.ok()) co_return durable;
  }

  co_await store.store_state_mutex_.Lock();
  UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
  TxCleanerLocalState result;
  result.blocks_.reserve(store.tx_blocks_.size());
  for (const auto& [block_id, tx_block] : store.tx_blocks_) {
    const BlockState* state = FindBlockState(store, block_id);
    const bool durable =
        state != nullptr && state->allocated_ &&
        state->allocation_epoch_ == tx_block.allocation_epoch_ &&
        state->kind_ == BlockKind::kTransaction &&
        !IsActiveBlock(store, block_id) && !state->in_memory_ &&
        !state->flush_queued_ && !state->flush_in_progress_ && !state->freeing_;
    TxCleanerBlock block{
        .block_id_ = block_id,
        .allocation_epoch_ = tx_block.allocation_epoch_,
        .live_tagged_bytes_ = tx_block.live_tagged_bytes_,
        .dependency_pins_ = tx_block.dependency_pins_,
        .txids_ = {},
        .commit_decisions_ = {},
        .sealed_and_durable_ = durable,
        .pending_relocation_ =
            store.pending_relocation_fences_.contains(block_id),
    };
    block.txids_.reserve(tx_block.txids_.size());
    for (const auto& [txid, lease] : tx_block.txids_) {
      block.txids_.push_back(txid);
      block.active_transaction_ |= !lease.expired();
    }
    block.commit_decisions_.assign(tx_block.commit_ends_.begin(),
                                   tx_block.commit_ends_.end());
    result.blocks_.push_back(std::move(block));
  }
  co_return result;
}

Task<absl::Status> StorageEngine::Impl::PromoteTxBlockLocal(
    WorkerStore& store, const TxCleanerBlock& block,
    std::shared_ptr<const absl::flat_hash_set<std::uint64_t>> committed,
    bool shutdown_drain) {
  if (!shutdown_drain &&
      shutdown_flush_requested_.load(std::memory_order_acquire))
    co_return absl::CancelledError(
        "online transaction cleaner yielding to shutdown");
  co_await store.store_state_mutex_.Lock();
  BlockState* source = FindBlockState(store, block.block_id_);
  const auto tx_block = store.tx_blocks_.find(block.block_id_);
  bool active_transaction = false;
  if (tx_block != store.tx_blocks_.end())
    for (const auto& [txid, lease] : tx_block->second.txids_) {
      (void)txid;
      active_transaction |= !lease.expired();
    }
  if (source == nullptr || tx_block == store.tx_blocks_.end() ||
      source->allocation_epoch_ != block.allocation_epoch_ ||
      tx_block->second.allocation_epoch_ != block.allocation_epoch_ ||
      IsActiveBlock(store, block.block_id_) || active_transaction ||
      source->in_memory_ || source->flush_queued_ ||
      source->flush_in_progress_ || source->defragging_ || source->freeing_ ||
      source->pins_ != 0) {
    store.store_state_mutex_.Unlock(*store.worker_);
    co_return absl::FailedPreconditionError(
        "transaction block changed before promotion");
  }
  const bool has_live_records = tx_block->second.live_tagged_bytes_ != 0;
  source->defragging_ = true;
  const auto [file_id, offset] = FileOffset(block.block_id_);
  store.store_state_mutex_.Unlock(*store.worker_);

  absl::Status promoted = absl::OkStatus();
  if (has_live_records)
    promoted = co_await SalvageBlockRecords(store, block.block_id_, *source,
                                            file_id, offset, committed);
  std::vector<RelocationDurabilityFence> fences;
  co_await store.store_state_mutex_.Lock();
  if (const auto owed = store.pending_relocation_fences_.find(block.block_id_);
      owed != store.pending_relocation_fences_.end())
    fences = owed->second;
  if (BlockState* current = FindBlockState(store, block.block_id_);
      current != nullptr &&
      current->allocation_epoch_ == block.allocation_epoch_)
    current->defragging_ = false;
  store.store_state_mutex_.Unlock(*store.worker_);
  if (!promoted.ok()) co_return promoted;
  for (const RelocationDurabilityFence& destination : fences) {
    const absl::Status durable = co_await AwaitRelocationDurable(destination);
    if (!durable.ok()) co_return durable;
  }
  // Keep failed fence obligations on the source block. A later pass must not
  // retire it just because relocation already removed its live tagged bytes.
  co_await store.store_state_mutex_.Lock();
  store.pending_relocation_fences_.erase(block.block_id_);
  store.store_state_mutex_.Unlock(*store.worker_);
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::RetireTxBlockLocal(
    WorkerStore& store, const TxCleanerBlock& block) {
  std::uint32_t retired_backlog_bytes = 0;
  {
    co_await store.store_state_mutex_.Lock();
    UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
    const auto found = store.tx_blocks_.find(block.block_id_);
    BlockState* state = FindBlockState(store, block.block_id_);
    bool active_transaction = false;
    if (found != store.tx_blocks_.end())
      for (const auto& [txid, lease] : found->second.txids_) {
        (void)txid;
        active_transaction |= !lease.expired();
      }
    if (found == store.tx_blocks_.end() || state == nullptr ||
        found->second.allocation_epoch_ != block.allocation_epoch_ ||
        state->allocation_epoch_ != block.allocation_epoch_ ||
        state->kind_ != BlockKind::kTransaction || state->in_memory_ ||
        state->flush_queued_ || state->flush_in_progress_ ||
        state->defragging_ || state->freeing_ || state->pins_ != 0 ||
        found->second.live_tagged_bytes_ != 0 ||
        found->second.dependency_pins_ != 0 || active_transaction ||
        !found->second.counted_backlog_ ||
        store.pending_relocation_fences_.contains(block.block_id_) ||
        IsActiveBlock(store, block.block_id_))
      co_return absl::FailedPreconditionError(
          "transaction block changed before retirement");
    retired_backlog_bytes = found->second.backlog_bytes_;
    state->freeing_ = true;
    DestroyBlockState(store, block.block_id_);
  }
  const absl::Status returned =
      co_await ReturnColdBlocks(std::vector<std::uint64_t>{block.block_id_});
  co_await store.store_state_mutex_.Lock();
  UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
  // DestroyBlockState already removed the runtime Tx entry. An allocator
  // failure fail-stops reuse of this block, so its backlog charge must still
  // be removed even though dependent references remain until cold retirement.
  store.tx_backlog_bytes_.fetch_sub(retired_backlog_bytes,
                                    std::memory_order_release);
  if (returned.ok()) {
    // UUID and extent dependencies stay live until the source allocation bit
    // is durably clear, even after the tagged winners have moved.
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

Task<absl::Status> StorageEngine::Impl::RunTxCleaner(bool shutdown_drain) {
  if (!shutdown_drain &&
      shutdown_flush_requested_.load(std::memory_order_acquire))
    co_return absl::CancelledError(
        "online transaction cleaner yielding to shutdown");
  const unsigned coordinator = bycorf::ThisWorker().id_;
  auto inspect_owner = [this, coordinator](unsigned owner, bool seal)
      -> Task<absl::StatusOr<TxCleanerLocalState>> {
    if (owner == coordinator)
      co_return co_await InspectTxBlocksLocal(*stores_[owner], seal);
    co_return co_await bycorf::SubmitTaskTo(owner, [this, owner, seal]() {
      return InspectTxBlocksLocal(*stores_[owner], seal);
    });
  };

  // First observe settled leases. A later snapshot then sees every block and
  // decision for those transactions: no participant can append after its last
  // receipt is destroyed. This ordering avoids a cross-worker scan racing a
  // commit append or the last source-block append.
  absl::flat_hash_set<std::uint64_t> settled_txids;
  for (unsigned owner = 0; owner < worker_count_; ++owner) {
    auto local = co_await inspect_owner(owner, true);
    if (!local.ok()) co_return local.status();
    for (const TxCleanerBlock& block : local->blocks_)
      if (!block.active_transaction_)
        settled_txids.insert(block.txids_.begin(), block.txids_.end());
  }

  std::vector<TxCleanerLocalState> owner_states;
  owner_states.reserve(worker_count_);
  auto committed = std::make_shared<absl::flat_hash_set<std::uint64_t>>();
  absl::flat_hash_map<std::uint64_t, std::vector<RelocationDurabilityFence>>
      commit_fences;
  for (unsigned owner = 0; owner < worker_count_; ++owner) {
    auto local = co_await inspect_owner(owner, false);
    if (!local.ok()) co_return local.status();
    for (const TxCleanerBlock& block : local->blocks_)
      for (const auto& [txid, record_end] : block.commit_decisions_) {
        committed->insert(txid);
        if (record_end != 0)
          commit_fences[txid].push_back(RelocationDurabilityFence{
              .block_id_ = block.block_id_,
              .allocation_epoch_ = block.allocation_epoch_,
              .block_owner_ = static_cast<std::uint16_t>(owner),
              .committed_bytes_ = record_end,
          });
      }
    owner_states.push_back(std::move(*local));
  }
  auto eligible = [&settled_txids](const TxCleanerBlock& block) {
    if (!block.sealed_and_durable_ || block.active_transaction_) return false;
    return std::all_of(block.txids_.begin(), block.txids_.end(),
                       [&settled_txids](std::uint64_t txid) {
                         return settled_txids.contains(txid);
                       });
  };

  // Promote owners concurrently, but keep decision and source blocks until
  // every owner has finished its destination durability waits.
  const absl::Status promoted = co_await ForEachCleanerOwner(
      worker_count_, !shutdown_drain, [&](unsigned owner) {
        return std::pair{
            owner,
            [this, owner, committed, shutdown_drain, &owner_states,
             &commit_fences, &eligible]() -> Task<absl::Status> {
              for (const TxCleanerBlock& block : owner_states[owner].blocks_) {
                if (!eligible(block)) continue;
                for (std::uint64_t txid : block.txids_) {
                  if (!committed->contains(txid)) continue;
                  if (const auto found = commit_fences.find(txid);
                      found != commit_fences.end()) {
                    for (const RelocationDurabilityFence& decision :
                         found->second) {
                      const absl::Status durable =
                          co_await AwaitRelocationDurable(decision);
                      if (!durable.ok()) co_return durable;
                    }
                  }
                  // Recovered commit records were already validated on disk.
                }
                const absl::Status result = co_await PromoteTxBlockLocal(
                    *stores_[owner], block, committed, shutdown_drain);
                if (absl::IsFailedPrecondition(result)) {
                  tx_cleaner_dirty_.store(true, std::memory_order_release);
                  continue;
                }
                if (!result.ok()) co_return result;
              }
              co_return absl::OkStatus();
            }};
      });
  if (!promoted.ok()) co_return promoted;

  // Recheck after promotion and destination durability. A decision may be
  // discarded once every Tx block naming that transaction has no live tagged
  // winner, pin protecting a possible undo predecessor, or unpaid relocation
  // fence. The old source blocks may still be allocated: recovery discards
  // their tagged copies without a decision
  // and reads the durable ordinary replacements. This also breaks cycles
  // when two blocks each carry the other's commit record.
  absl::flat_hash_set<std::uint64_t> decisions_needed;
  for (unsigned owner = 0; owner < worker_count_; ++owner) {
    auto current = co_await inspect_owner(owner, false);
    if (!current.ok()) co_return current.status();
    for (const TxCleanerBlock& block : current->blocks_)
      if (block.live_tagged_bytes_ != 0 || block.dependency_pins_ != 0 ||
          block.pending_relocation_)
        decisions_needed.insert(block.txids_.begin(), block.txids_.end());
  }
  for (unsigned owner = 0; owner < worker_count_; ++owner) {
    for (const TxCleanerBlock& block : owner_states[owner].blocks_) {
      if (!eligible(block)) continue;
      bool has_decision_dependencies = false;
      for (const auto& [txid, record_end] : block.commit_decisions_) {
        (void)record_end;
        has_decision_dependencies |= decisions_needed.contains(txid);
      }
      if (has_decision_dependencies) continue;
      absl::Status retired;
      if (owner == coordinator) {
        retired = co_await RetireTxBlockLocal(*stores_[owner], block);
      } else {
        retired = co_await bycorf::SubmitTaskTo(owner, [this, owner, block]() {
          return RetireTxBlockLocal(*stores_[owner], block);
        });
      }
      if (absl::IsFailedPrecondition(retired)) {
        tx_cleaner_dirty_.store(true, std::memory_order_release);
        continue;
      }
      if (!retired.ok()) co_return retired;
      tx_cleaner_dirty_.store(true, std::memory_order_release);
    }
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
  // two cleaner coordinators.
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
      for (unsigned owner = 0; owner < worker_count_; ++owner) {
        absl::StatusOr<TxCleanerLocalState> state;
        if (owner == bycorf::ThisWorker().id_) {
          state = co_await InspectTxBlocksLocal(*stores_[owner], false);
        } else {
          state = co_await bycorf::SubmitTaskTo(owner, [this, owner]() {
            return InspectTxBlocksLocal(*stores_[owner], false);
          });
        }
        if (!state.ok()) co_return state.status();
        if (!state->blocks_.empty())
          co_return absl::FailedPreconditionError(
              "transaction blocks remain at shutdown checkpoint");
      }
      co_return absl::OkStatus();
    }
  }

  co_return absl::FailedPreconditionError(
      "transaction blocks did not quiesce during shutdown");
}

}  // namespace lavik::storage
