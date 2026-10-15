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
  // All completions return to the coordinator. Keep this state and the
  // borrowed block snapshots alive until every owner finishes, even on error.
  unsigned pending = count;
  bycorf::AsyncNotification finished;
  absl::Status error;
  std::exception_ptr exception;
  auto run_step = [&](unsigned owner, auto step) -> Task<absl::Status> {
    try {
      absl::Status status;
      if (owner == bycorf::ThisWorker().id_) {
        status = co_await step();
      } else {
        status = co_await bycorf::SubmitTaskTo(owner, std::move(step));
      }
      if (!status.ok() && error.ok()) error = std::move(status);
    } catch (...) {
      if (!exception) exception = std::current_exception();
    }
    assert(pending != 0);
    if (--pending == 0) finished.NotifyAll(*bycorf::ThisWorker().self_);
    co_return absl::OkStatus();
  };
  std::vector<Task<absl::Status>> tasks;
  tasks.reserve(count);
  // Construct every coroutine before the first launch. A frame-allocation
  // failure must not leave running owners borrowing destroyed local state.
  for (unsigned index = 0; index < count; ++index) {
    auto [owner, step] = step_at(index);
    tasks.push_back(run_step(owner, std::move(step)));
  }
  for (auto& task : tasks) bycorf::ThisWorker().self_->Spawn(std::move(task));
  // Notifications are not latched. Check the count on the coordinator, where
  // completions also run, so none can arrive between this check and Wait().
  while (pending != 0) co_await finished.Wait();
  if (exception) std::rethrow_exception(exception);
  co_return error;
}

}  // namespace

void StorageEngine::Impl::InitializeTxWrites(
    std::uint64_t txid, std::span<TxShardWrites> writes,
    MutationPrecondition precondition) {
  if (writes.empty()) return;
  auto lease = std::shared_ptr<void>(new std::uint8_t{0}, [this](void* token) {
    delete static_cast<std::uint8_t*>(token);
    tx_cleaner_dirty_.store(true, std::memory_order_release);
    RequestTxCleaner(true);
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
  block.NotePromotionProgress();
  block.txids_.Record(txid,
                      receipt != nullptr
                          ? std::weak_ptr<void>(receipt->transaction_lease_)
                          : std::weak_ptr<void>{});
  if (dependency_txid != 0)
    block.txids_.Record(dependency_txid,
                        receipt != nullptr
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
  found->second.NotePromotionProgress();
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
  found->second.NotePromotionProgress();
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
  found->second.NotePromotionProgress();
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
  RequestTxCleaner(false);
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
  // Report occupied Tx-record bytes rather than full block capacity, so an
  // idle, sparsely filled block does not inflate the backlog metric. Physical
  // space is accounted for separately by the block allocator.
  found->second.backlog_bytes_ = state->committed_bytes_ - kBlockHeaderBytes;
  found->second.counted_backlog_ = true;
  store.tx_backlog_bytes_.fetch_add(found->second.backlog_bytes_,
                                    std::memory_order_release);
  tx_cleaner_dirty_.store(true, std::memory_order_release);
  RequestTxCleaner(true);
}

bool StorageEngine::Impl::HasSealedTxBacklog() const noexcept {
  // This is work discovery, not write admission: even one sealed record can
  // become reclaimable, and allocated capacity is enforced by the allocator.
  for (const auto& store : stores_)
    if (store->tx_backlog_bytes_.load(std::memory_order_acquire) != 0)
      return true;
  return false;
}

void StorageEngine::Impl::RequestTxCleaner(bool force) {
  // Lease completion and grouped writes share one runner. Avoid touching
  // every owner's backlog on those hot paths while it already owns the work.
  if (tx_cleaner_scheduled_.load(std::memory_order_acquire)) return;
  const auto& current = bycorf::ThisWorker();
  // Lease destruction and configuration can originate outside the runtime.
  // PeriodicFlush starts any outstanding work for those callers. Recovery
  // also rebuilds sealed-block metadata; do not clean it before this worker
  // has finished initialization and enabled online access.
  if (current.self_ == nullptr || current.cross_core_ == nullptr ||
      current.self_->stop_requested() || current.id_ >= stores_.size() ||
      !stores_[current.id_] ||
      !stores_[current.id_]->remote_reads_ready_.load(
          std::memory_order_acquire) ||
      shutdown_flush_requested_.load(std::memory_order_acquire) ||
      RuntimeFailureLatched() ||
      tx_cleaner_cooldown_ms_.load(std::memory_order_acquire) == 0)
    return;
  if (force) {
    if (!HasSealedTxBacklog()) return;
  } else if (tx_cleaner_running_.load(std::memory_order_acquire) ||
             !tx_cleaner_dirty_.load(std::memory_order_acquire) ||
             MonotonicMillis() <
                 tx_cleaner_next_run_ms_.load(std::memory_order_acquire)) {
    return;
  }
  bool expected = false;
  if (!tx_cleaner_scheduled_.compare_exchange_strong(
          expected, true, std::memory_order_acq_rel, std::memory_order_acquire))
    return;
  TxCleanerRequest request(this, [](Impl* engine) {
    engine->tx_cleaner_scheduled_.store(false, std::memory_order_release);
  });
  // The frame owns request before publication, including allocation failure
  // and Spawn's stop-time discard path. A queued task is visible to shutdown.
  auto task = RunRequestedTxCleaner(std::move(request), force);
  current.self_->SpawnBackground(std::move(task));
}

Task<absl::Status> StorageEngine::Impl::RunRequestedTxCleaner(
    TxCleanerRequest request, bool force) {
  (void)request;
  auto* worker = bycorf::ThisWorker().self_;
  for (;;) {
    if (shutdown_flush_requested_.load(std::memory_order_acquire) ||
        worker->stop_requested() || RuntimeFailureLatched() ||
        tx_cleaner_cooldown_ms_.load(std::memory_order_acquire) == 0 ||
        (force && !HasSealedTxBacklog()))
      co_return absl::OkStatus();
    const auto retired_before =
        tx_cleaner_retired_blocks_.load(std::memory_order_relaxed);
    const auto status = co_await MaybeRunTxCleaner(force);
    if (!status.ok() &&
        !(absl::IsCancelled(status) &&
          shutdown_flush_requested_.load(std::memory_order_acquire))) {
      // The next grouped write observes the published error code. Keep full
      // diagnostic detail here even when no client is currently writing.
      spdlog::warn("transaction cleaner round failed; retrying: {}",
                   status.message());
    }
    if (shutdown_flush_requested_.load(std::memory_order_acquire) ||
        worker->stop_requested() || RuntimeFailureLatched() ||
        tx_cleaner_cooldown_ms_.load(std::memory_order_acquire) == 0 ||
        !HasSealedTxBacklog())
      co_return status;
    if (status.ok() && tx_cleaner_retired_blocks_.load(
                           std::memory_order_relaxed) != retired_before) {
      // Retirement can remove decision dependencies for another pass. Keep
      // draining without imposing an idle cooldown, but let ready work run.
      co_await bycorf::Yield(*worker);
    } else {
      // Leases, read pins and failed promotion can temporarily prevent any
      // retirement. One bounded background retry serves the whole engine;
      // neither a waiting request nor another append is needed for progress.
      const auto waited =
          co_await bycorf::SleepFor(*worker, std::chrono::milliseconds(50));
      if (!waited.ok()) co_return waited;
    }
    force = true;
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

absl::Status StorageEngine::Impl::BeforeGroupedTransaction(WorkerStore& store) {
  if (shutdown_flush_requested_.load(std::memory_order_acquire) ||
      store.worker_->stop_requested())
    return absl::UnavailableError("storage is shutting down");
  if (store.write_failed_ || RuntimeFailureLatched())
    return absl::InternalError("storage writer has failed");
  if (tx_cleaner_cooldown_ms_.load(std::memory_order_acquire) == 0)
    return absl::OkStatus();
  // Holding a key intent must not make unrelated cleanup part of this write.
  // A free physical block and staging buffer suffice to append a successor;
  // only their actual allocation paths may apply capacity backpressure.
  RequestTxCleaner(true);
  const auto error = tx_cleaner_error_.load(std::memory_order_acquire);
  if (error != absl::StatusCode::kOk)
    return absl::Status(error,
                        "transaction cleaner failed before grouped write");
  return absl::OkStatus();
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
  if (status.ok()) {
#if LAVIK_FAULTS_ENABLED
    if (tx_cleaner_round_for_test_)
      status = tx_cleaner_round_for_test_();
    else
#endif
      status = co_await RunTxCleaner();
  }
  if (!status.ok()) {
    if (!(absl::IsCancelled(status) &&
          shutdown_flush_requested_.load(std::memory_order_acquire)))
      tx_cleaner_failures_.fetch_add(1, std::memory_order_relaxed);
    tx_cleaner_dirty_.store(true, std::memory_order_release);
  }
  const bool retryable = absl::IsFailedPrecondition(status) ||
                         absl::IsAborted(status) ||
                         absl::IsResourceExhausted(status);
  const auto cleaner_error =
      status.ok() || retryable ? absl::StatusCode::kOk : status.code();
  // Publish only after an actual round: a concurrent caller that lost the
  // running CAS must not erase another round's error. A later successful or
  // retryable round can clear this result without a permanent engine fence.
  tx_cleaner_error_.store(cleaner_error, std::memory_order_release);
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
        active_writers = found->second.txids_.HasLiveWriter();
    }
    // A cleaner round must not discard usable space in the current append
    // block. Rollovers seal it normally; this path handles a nearly full tail
    // whose writers have settled but cannot allocate a successor yet.
    if (seal && store.active_tx_block_ && !active_writers &&
        store.active_tx_block_->committed_bytes_ >=
            kStorageBlockBytes - 2 * kDirectIoAlignment) {
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
  result.reserve(store.tx_blocks_.size());
  for (auto& [block_id, tx_block] : store.tx_blocks_) {
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
    block.active_transaction_ = tx_block.txids_.HasLiveWriter();
    block.txids_.reserve(tx_block.txids_.entries().size());
    for (const auto& [txid, lease] : tx_block.txids_.entries()) {
      (void)lease;
      block.txids_.push_back(txid);
    }
    block.commit_decisions_.assign(tx_block.commit_ends_.begin(),
                                   tx_block.commit_ends_.end());
    result.push_back(std::move(block));
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
    active_transaction = tx_block->second.txids_.HasLiveWriter();
  if (source == nullptr || tx_block == store.tx_blocks_.end() ||
      source->allocation_epoch_ != block.allocation_epoch_ ||
      tx_block->second.allocation_epoch_ != block.allocation_epoch_ ||
      IsActiveBlock(store, block.block_id_) || active_transaction ||
      source->in_memory_ || source->flush_queued_ ||
      source->flush_in_progress_ || source->defragging_ || source->freeing_ ||
      source->pin_count() != 0) {
    store.store_state_mutex_.Unlock(*store.worker_);
    co_return absl::FailedPreconditionError(
        "transaction block changed before promotion");
  }
  source->CloseRemotePins();
  if (source->pin_count() != 0) {
    source->OpenRemotePins();
    store.store_state_mutex_.Unlock(*store.worker_);
    co_return absl::FailedPreconditionError(
        "transaction block has remote readers");
  }
  struct RemoteAdmission {
    BlockState& state_;
    ~RemoteAdmission() { state_.OpenRemotePins(); }
  } remote_admission{*source};
  const bool has_live_records = tx_block->second.live_tagged_bytes_ != 0;
  const auto promotion_generation = tx_block->second.promotion_generation_;
  const auto rewind_generation =
      tx_promotion_rewind_generation_.load(std::memory_order_acquire);
  // Eligibility has already established settled leases and a complete commit
  // decision snapshot. No later append/decision can make an omitted record
  // movable; only accounting/dependency progress or inverse publication can.
  const bool scan_needed =
      has_live_records &&
      (tx_block->second.scanned_promotion_generation_ != promotion_generation ||
       tx_block->second.scanned_rewind_generation_ != rewind_generation);
  source->defragging_ = true;
  const auto [file_id, offset] = FileOffset(block.block_id_);
  store.store_state_mutex_.Unlock(*store.worker_);

  absl::Status promoted = absl::OkStatus();
  if (scan_needed) {
#if LAVIK_FAULTS_ENABLED
    if (tx_promotion_scan_for_test_)
      promoted = co_await tx_promotion_scan_for_test_();
    else
#endif
      promoted =
          co_await SalvageBlockRecords(store, block.block_id_, *source, file_id,
                                       offset, committed, !shutdown_drain);
  }
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
  if (scan_needed) {
    const auto current = store.tx_blocks_.find(block.block_id_);
    if (current != store.tx_blocks_.end() &&
        current->second.allocation_epoch_ == block.allocation_epoch_ &&
        current->second.promotion_generation_ == promotion_generation &&
        tx_promotion_rewind_generation_.load(std::memory_order_acquire) ==
            rewind_generation) {
      // A successful full scan has processed every currently movable record.
      // Remaining charged bytes can belong to superseded versions awaiting
      // their successors' durable retirement. Another block's progress must
      // not reread these bytes. Changes during either the scan or fence waits
      // invalidate this proof, including our own relocation accounting; never
      // stamp the newer generation as though it had already been scanned.
      current->second.scanned_promotion_generation_ = promotion_generation;
      current->second.scanned_rewind_generation_ = rewind_generation;
    }
  }
  store.store_state_mutex_.Unlock(*store.worker_);
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::RetireTxBlockLocal(
    WorkerStore& store, const TxCleanerBlock& block) {
  assert(block.sealed_and_durable_ && !block.active_transaction_);
  std::uint32_t retired_backlog_bytes = 0;
  {
    co_await store.store_state_mutex_.Lock();
    UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
    const auto found = store.tx_blocks_.find(block.block_id_);
    BlockState* state = FindBlockState(store, block.block_id_);
    if (found == store.tx_blocks_.end() || state == nullptr ||
        found->second.allocation_epoch_ != block.allocation_epoch_ ||
        state->allocation_epoch_ != block.allocation_epoch_)
      co_return absl::FailedPreconditionError(
          "transaction block changed before retirement");

    // eligible() selected a sealed, durable block whose leases had ended.
    // It cannot reopen or gain writers, and an expired lease cannot revive.
    // Only this cleaner promotes/retires Tx blocks; all promotions have joined.
    assert(state->kind_ == BlockKind::kTransaction);
    assert(found->second.counted_backlog_);
    assert(!state->defragging_ && !state->freeing_);

    // Reads and undo dependencies can still acquire references after
    // inspection.
    if (state->pin_count() != 0 || found->second.live_tagged_bytes_ != 0 ||
        found->second.dependency_pins_ != 0)
      co_return absl::FailedPreconditionError(
          "transaction block is still referenced");
    // A failed promotion may have moved winners without making them durable.
    if (store.pending_relocation_fences_.contains(block.block_id_))
      co_return absl::FailedPreconditionError(
          "transaction block has pending relocation durability");

    retired_backlog_bytes = found->second.backlog_bytes_;
    // No suspension between the final checks and retiring the runtime block.
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
    for (const TxCleanerBlock& block : *local)
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
    for (const TxCleanerBlock& block : *local)
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
              for (const TxCleanerBlock& block : owner_states[owner]) {
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
    for (const TxCleanerBlock& block : *current)
      if (block.live_tagged_bytes_ != 0 || block.dependency_pins_ != 0 ||
          block.pending_relocation_)
        decisions_needed.insert(block.txids_.begin(), block.txids_.end());
  }
  for (unsigned owner = 0; owner < worker_count_; ++owner) {
    for (const TxCleanerBlock& block : owner_states[owner]) {
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
        if (!state->empty())
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
