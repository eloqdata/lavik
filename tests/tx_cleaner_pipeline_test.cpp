/*
 * Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <sys/eventfd.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <limits>
#include <memory>
#include <span>
#include <utility>
#include <vector>

#include "../src/storage/engine/impl.h"
#include "gtest/gtest.h"
#include "lavik/metrics.h"

namespace lavik::storage {

#if LAVIK_FAULTS_ENABLED

// These tests substitute either a physical cleaner round or its promotion
// scan. Real workers exercise scheduling and the promotion state machine,
// including dependency invalidation and destination durability waits. Physical
// retirement and persistence also have multikey and low-capacity E2E coverage.
class TxCleanerPipelineTestPeer : public testing::Test {
 protected:
  static constexpr unsigned kWorkers = 2;
  static constexpr std::uint64_t kPayloadBytes =
      kStorageBlockBytes - kBlockHeaderBytes;

  void SetUp() override {
    saved_context_ = std::move(bycorf::MutableThisWorker());
    auto& engine = *storage_.impl_;
    engine.worker_count_ = kWorkers;
    for (unsigned owner = 0; owner < kWorkers; ++owner) {
      wake_fds_[owner] = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
      ASSERT_GE(wake_fds_[owner], 0);
      cross_core_.mailbox(owner).wake_fd_ = wake_fds_[owner];
      workers_[owner].BindCrossCore(owner, &cross_core_);
      On(owner);
      const auto initialized = workers_[owner].Init({.recv_buffer_count_ = 0});
      ASSERT_TRUE(initialized.ok()) << initialized;
      initialized_[owner] = true;
      auto store = std::make_unique<StorageEngine::Impl::WorkerStore>();
      store->engine_ = &engine;
      store->worker_ = &workers_[owner];
      store->remote_reads_ready_.store(true);
      engine.stores_.push_back(std::move(store));
    }
    On(0);
    engine.tx_cleaner_round_for_test_ = [this]() -> Task<absl::Status> {
      ++rounds_;
      round_owner_ = bycorf::ThisWorker().id_;
      if (pause_round_) co_await round_continue_.Wait();
      co_return round_ ? round_() : absl::OkStatus();
    };
    engine.tx_cleaner_retry_delay_for_test_ = [this](std::uint64_t delay_ms) {
      retry_delays_.push_back(delay_ms);
    };
  }

  void TearDown() override {
    if (storage_.impl_ && storage_.impl_->stores_.size() == kWorkers) {
      On(0);
      storage_.ConfigureTxCleanerCooldown(0).IgnoreError();
      receipts_.clear();
      // A failed assertion must not leave the real runner borrowing a test's
      // stack lambda, nor destroy an allocator still registered for a wake.
      round_ = {};
      ResumeRound();
      // Leave the round hook alive until a queued or timer-suspended driver
      // exits, including after an assertion fails midway through the test.
      EXPECT_TRUE(
          PumpUntil([&] { return DriverIdle() && CapacityTasksDone(); }));
      for (auto& task : capacity_tasks_) task.reset();
      capacity_probe_.reset();
      storage_.impl_->tx_cleaner_round_for_test_ = {};
      storage_.impl_->tx_cleaner_retry_delay_for_test_ = {};
      for (unsigned round = 0; round < 2; ++round) Pump();
    }
    for (unsigned owner = 0; owner < kWorkers; ++owner) {
      if (!initialized_[owner]) continue;
      On(owner);
      workers_[owner].Shutdown();
    }
    storage_.impl_.reset();
    for (const int fd : wake_fds_)
      if (fd >= 0) close(fd);
    bycorf::MutableThisWorker() = std::move(saved_context_);
  }

  void On(unsigned owner) {
    bycorf::SetThisWorker(owner, &cross_core_, &workers_[owner]);
  }

  StorageEngine::Impl::WorkerStore& Store(unsigned owner) {
    return *storage_.impl_->stores_[owner];
  }

  void AddLiveBacklog(std::uint64_t bytes = 3 * kPayloadBytes) {
    On(1);
    auto& receipt = receipts_.emplace_back();
    storage_.InitializeTxWrites(receipts_.size(), std::span(&receipt, 1));
    auto& block = Store(1).tx_blocks_[1];
    block.allocation_epoch_ = 1;
    block.counted_backlog_ = true;
    block.backlog_bytes_ = bytes;
    block.txids_.Record(receipt.txid_, receipt.transaction_lease_);
    Store(1).tx_backlog_bytes_.store(bytes);
  }

  absl::Status PrepareWrite(unsigned owner = 0) {
    On(owner);
    return storage_.impl_->BeforeGroupedTransaction(Store(owner));
  }

  void RequestCleaning(unsigned owner = 0) {
    On(owner);
    storage_.impl_->RequestTxCleaner(true);
  }

  void CompleteBacklog() { Store(1).tx_backlog_bytes_.store(0); }

  void RecordRoundRetirement() {
    storage_.impl_->tx_cleaner_retired_blocks_.fetch_add(1);
  }

  void BeginShutdown() {
    storage_.impl_->shutdown_flush_requested_.store(true);
    // The fixture has no PeriodicFlush; deliver its shutdown recheck here.
    storage_.impl_->NotifyTxCleanerCapacityWaiters();
  }

  bool DriverIdle() const {
    return !storage_.impl_->tx_cleaner_scheduled_.load() &&
           !storage_.impl_->tx_cleaner_running_.load();
  }

  void PrepareFullDevice() {
    auto& engine = *storage_.impl_;
    engine.devices_.push_back(
        StorageDevice{.capacity_blocks_ = 16, .data_block_count_ = 15});
    engine.ConfigureDefragReserves();
    auto allocator = std::make_unique<DeviceAllocator>();
    allocator->owner_ = 0;
    allocator->next_pristine_ = 16;
    // All remaining blocks belong to the existing maintenance reserve. No
    // bitmap I/O is needed: there are no pristine or cold blocks to activate.
    for (std::uint64_t id = 1; id <= 8; ++id)
      allocator->ready_blocks_.push_back(id);
    engine.device_allocators_.push_back(std::move(allocator));
    for (unsigned owner = 0; owner < kWorkers; ++owner) {
      Store(owner).home_devices_.push_back(0);
      Store(owner).home_device_allocations_.push_back(0);
    }
  }

  void ResumeRound(bool pause_successor = false) {
    On(round_owner_);
    pause_round_ = pause_successor;
    round_continue_.NotifyAll(workers_[round_owner_]);
  }

  template <typename T>
  void StartTask(Task<T>& task, unsigned owner) {
    On(owner);
    std::move(task)
        .operator co_await()
        .await_suspend(std::noop_coroutine())
        .resume();
  }

  Task<absl::StatusOr<ReservedBlock>> AllocateForeground(unsigned owner,
                                                         bool unlock_writer) {
    auto& store = Store(owner);
    // Match a real rollover: retain the stream gate, but allow maintenance
    // to take store state while AcquireWriteBlock waits for capacity.
    co_await store.active_block_allocation_mutex_.Lock();
    UnlockGuard gate(&store.active_block_allocation_mutex_, store.worker_);
    co_await store.store_state_mutex_.Lock();
    UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
    auto result =
        co_await storage_.impl_->AcquireWriteBlock(store, false, unlock_writer);
    // Probe without enqueuing a second lock waiter. If restoration regresses,
    // take the missing lock so the guard can still clean up safely.
    auto probe = store.store_state_mutex_.Lock();
    capacity_lock_restored_[owner] = !probe.await_ready();
    co_return result;
  }

  void StartCapacity(unsigned owner, bool unlock_writer = true) {
    capacity_tasks_[owner].emplace(AllocateForeground(owner, unlock_writer));
    StartTask(*capacity_tasks_[owner], owner);
  }

  bool CapacityDone(unsigned owner) const {
    return capacity_tasks_[owner] && capacity_tasks_[owner]->done();
  }

  bool CapacityTasksDone() const {
    for (const auto& task : capacity_tasks_)
      if (task && !task->done()) return false;
    return !capacity_probe_ || capacity_probe_->done();
  }

  absl::StatusOr<ReservedBlock> CapacityResult(unsigned owner) {
    return std::move(*capacity_tasks_[owner]).TakeResult();
  }

  bool StoreLockAvailable(unsigned owner) {
    On(owner);
    auto lock = Store(owner).store_state_mutex_.Lock();
    if (!lock.await_ready()) return false;
    Store(owner).store_state_mutex_.Unlock(workers_[owner]);
    return true;
  }

  unsigned CapacityWaiters(unsigned owner) {
    return Store(owner).tx_cleaner_capacity_waiters_.load();
  }

  bool CleanerRoundRunning() const {
    return storage_.impl_->tx_cleaner_running_.load();
  }

  std::uint64_t CompletedCleanerRounds() const {
    return storage_.impl_->tx_cleaner_completed_rounds_.load();
  }

  std::uint64_t ReclaimGeneration() const {
    return storage_.impl_->space_reclaim_generation_.load();
  }

  void StartCapacityProbe(unsigned owner, std::uint64_t completed,
                          std::uint64_t space) {
    capacity_probe_.emplace(storage_.impl_->AwaitRunningTxCleanerForCapacity(
        Store(owner), completed, space));
    StartTask(*capacity_probe_, owner);
  }

  void ReleaseCapacityBlock() {
    On(0);
    // Supply one durably reclaimed block on its allocator owner. The real
    // allocator, admission limits and capacity-wait protocol remain intact.
    storage_.impl_->device_allocators_[0]->ready_blocks_.push_back(9);
    storage_.impl_->space_reclaim_generation_.fetch_add(1);
    storage_.impl_->NotifyTxCleanerCapacityWaiters();
  }

  void PreparePromotion() {
    On(0);
    auto& engine = *storage_.impl_;
    engine.devices_.push_back(StorageDevice{.capacity_blocks_ = 3});
    engine.block_state_lookup_.push_back({promotion_blocks_.data(), 1, 3});
    ResetPromotionSource(7);
    engine.tx_promotion_scan_for_test_ = [this] { return ScanPromotion(); };
  }

  void ResetPromotionSource(std::uint64_t epoch) {
    On(0);
    auto& source = promotion_blocks_[0];
    source.Reset(0, epoch);
    source.allocated_ = true;
    source.kind_ = BlockKind::kTransaction;
    source.committed_bytes_ = kBlockHeaderBytes + 1024;
    source.live_bytes_ = 1024;
    Store(0).tx_blocks_.erase(1);
    auto& runtime = Store(0).tx_blocks_[1];
    runtime.allocation_epoch_ = epoch;
    runtime.live_tagged_bytes_ = 1024;
    runtime.counted_backlog_ = true;
    runtime.backlog_bytes_ = 1024;
  }

  Task<absl::Status> ScanPromotion() {
    ++promotion_scans_;
    if (pause_promotion_scan_) co_await promotion_continue_.Wait();
    co_return promotion_scan_result_;
  }

  void ResumePromotionScan() {
    On(0);
    pause_promotion_scan_ = false;
    promotion_continue_.NotifyAll(workers_[0]);
  }

  Task<absl::Status> Promotion(std::uint64_t epoch = 7) {
    // Keep the inspected snapshot alive across the real promotion's waits.
    const TxCleanerBlock inspected{
        .block_id_ = 1,
        .allocation_epoch_ = epoch,
        .live_tagged_bytes_ = 1024,
        .sealed_and_durable_ = true,
    };
    co_return co_await storage_.impl_->PromoteTxBlockLocal(
        Store(0), inspected,
        std::make_shared<const absl::flat_hash_set<std::uint64_t>>(), false);
  }

  void StartPromotion(Task<absl::Status>& task) {
    On(0);
    std::move(task)
        .operator co_await()
        .await_suspend(std::noop_coroutine())
        .resume();
  }

  absl::Status FinishPromotion(Task<absl::Status>& task) {
    if (!PumpUntil([&] { return task.done(); }))
      return absl::DeadlineExceededError("promotion fixture did not finish");
    return std::move(task).TakeResult();
  }

  absl::Status Promote(std::uint64_t epoch = 7) {
    auto task = Promotion(epoch);
    StartPromotion(task);
    return FinishPromotion(task);
  }

  bool PinPromotionDependency() {
    On(0);
    const RecordLocation source(
        1, 1, promotion_blocks_[0].allocation_epoch_, 0, 128,
        RecordLocation::PackedMetadata::Encode(
            kBlockHeaderBytes, 128, 0, false, false, false, false, false, true,
            RecordKind::kValue, ValueType::kString));
    return storage_.impl_->PinTxDependencyLocal(Store(0), source);
  }

  void UnpinPromotionDependency() {
    On(0);
    storage_.impl_->UnpinTxDependencyLocal(
        Store(0), 1, promotion_blocks_[0].allocation_epoch_);
  }

  void RetirePromotionRecord() {
    On(0);
    storage_.impl_->DropTaggedRecordLocal(
        Store(0), 1, promotion_blocks_[0].allocation_epoch_, 128);
  }

  void RewindPromotionIndex() {
    // Rollback can restore a source record from another key owner.
    On(1);
    storage_.impl_->InvalidateTxPromotionAfterRewind();
  }

  void OwePromotionFence() {
    On(0);
    auto& destination = promotion_blocks_[1];
    destination.Reset(0, 11);
    destination.allocated_ = true;
    destination.in_memory_ = true;
    // Model an already-running flush so the real fence waits for its notify
    // without dispatching fixture buffers to a nonexistent device.
    destination.flush_in_progress_ = true;
    destination.staging_slot_ = 1;
    Store(0).staging_slots_.resize(2);
    Store(0).staging_slots_[1].durable_bytes_ = kBlockHeaderBytes;
    Store(0).pending_relocation_fences_[1] = {
        {.block_id_ = 2,
         .allocation_epoch_ = 11,
         .block_owner_ = 0,
         .committed_bytes_ = kBlockHeaderBytes + 4096}};
  }

  bool HasPromotionFence() {
    On(0);
    return Store(0).pending_relocation_fences_.contains(1);
  }

  void CompletePromotionFence() {
    On(0);
    Store(0).staging_slots_[1].durable_bytes_ = kBlockHeaderBytes + 4096;
    promotion_blocks_[1].flush_in_progress_ = false;
    Store(0).durability_progress_.NotifyAll(workers_[0]);
  }

  void FailPromotionFence(bool failed) {
    On(0);
    Store(0).write_failed_ = failed;
    Store(0).durability_progress_.NotifyAll(workers_[0]);
  }

  void Pump() {
    for (unsigned owner = 0; owner < kWorkers; ++owner) {
      On(owner);
      workers_[owner].RunOnce(false);
    }
  }

  StorageDiagnosticsSnapshot Diagnostics() {
    auto task = storage_.CollectDiagnostics();
    StartTask(task, 0);
    if (!PumpUntil([&] { return task.done(); })) {
      ADD_FAILURE() << "diagnostic snapshot did not finish";
      return {};
    }
    return std::move(task).TakeResult();
  }

  void SealDiagnosticBlock() {
    On(0);
    storage_.impl_->NoteTxBlockSealedLocal(Store(0), 1);
  }

  void SetPendingCounts(unsigned flushes, std::uint64_t commits) {
    storage_.impl_->active_flushes_.store(flushes);
    storage_.impl_->active_tx_commits_.store(commits);
  }

  bool EnqueueDiagnosticCommit(std::uint64_t txid,
                               std::vector<TxShardWrites> writes = {}) {
    On(0);
    return storage_.impl_->EnqueueTxCommit(txid, std::move(writes));
  }

  template <typename Predicate>
  bool PumpUntil(Predicate ready) {
    // A deadlock watchdog, not a latency or throughput assertion. The normal
    // cooldown is 60 s; autonomous retries must complete without that timer.
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!ready() && std::chrono::steady_clock::now() < deadline) Pump();
    return ready();
  }

  bycorf::CurrentWorker saved_context_;
  bycorf::CrossCore cross_core_{kWorkers};
  std::array<bycorf::Worker, kWorkers> workers_;
  std::array<int, kWorkers> wake_fds_{-1, -1};
  std::array<bool, kWorkers> initialized_{};
  std::array<BlockState, 2> promotion_blocks_;
  StorageEngine storage_{StorageEngineOptions{}};
  std::deque<TxShardWrites> receipts_;
  std::function<absl::Status()> round_;
  std::vector<std::uint64_t> retry_delays_;
  unsigned rounds_ = 0;
  unsigned round_owner_ = 0;
  bool pause_round_ = false;
  AsyncNotification round_continue_;
  std::array<std::optional<Task<absl::StatusOr<ReservedBlock>>>, kWorkers>
      capacity_tasks_;
  std::optional<Task<bool>> capacity_probe_;
  std::array<bool, kWorkers> capacity_lock_restored_{};
  unsigned promotion_scans_ = 0;
  bool pause_promotion_scan_ = false;
  absl::Status promotion_scan_result_;
  AsyncNotification promotion_continue_;
};

TEST_F(TxCleanerPipelineTestPeer, DiagnosticsExcludeActiveBlocksAndSumOwners) {
  ASSERT_TRUE(storage_.ConfigureTxCleanerCooldown(0).ok());
  PreparePromotion();
  auto& runtime = Store(0).tx_blocks_.at(1);
  runtime.counted_backlog_ = false;
  runtime.backlog_bytes_ = 0;
  // A live active Tx block has records and metadata but is not backlog yet.
  EXPECT_EQ(Diagnostics().tx_cleaner_.tx_backlog_bytes_total_, 0);
  On(0);
  SealDiagnosticBlock();
  SealDiagnosticBlock();  // Sealing is idempotent.
  On(1);
  Store(1).tx_backlog_bytes_.store(2048);
  auto snapshot = Diagnostics();
  EXPECT_EQ(snapshot.tx_cleaner_.tx_backlog_bytes_total_, 3072);
  EXPECT_EQ(snapshot.tx_cleaner_.tx_backlog_bytes_max_, 2048);
  On(1);
  Store(1).tx_backlog_bytes_.store(0);
  snapshot = Diagnostics();
  EXPECT_EQ(snapshot.tx_cleaner_.tx_backlog_bytes_total_, 1024);
  EXPECT_EQ(snapshot.tx_cleaner_.tx_backlog_bytes_max_, 1024);
}

TEST_F(TxCleanerPipelineTestPeer, DiagnosticsKeepConservativePendingSamples) {
  for (unsigned owner = 0; owner < kWorkers; ++owner) {
    On(owner);
    auto& slots = Store(owner).staging_slots_;
    slots.resize(3);
    slots[1].write_buffer_id_ = 1;
    slots[1].committed_bytes_ = kBlockHeaderBytes + 100 * (owner + 1);
    // A released buffer's stale descriptor must not contribute dirty bytes.
    slots[2].committed_bytes_ = kBlockHeaderBytes + 9000;
  }
  SetPendingCounts(2, 7);
  auto task = storage_.CollectDiagnostics();
  StartTask(task, 0);
  ASSERT_FALSE(task.done());  // Paused at the remote worker snapshot.
  SetPendingCounts(0, 0);
  ASSERT_TRUE(PumpUntil([&] { return task.done(); }));
  auto snapshot = std::move(task).TakeResult();
  EXPECT_EQ(snapshot.durability_.dirty_staging_bytes_, 300);
  EXPECT_EQ(snapshot.durability_.flushes_pending_, 2);
  EXPECT_EQ(snapshot.durability_.tx_commits_pending_, 7);
  EXPECT_TRUE(snapshot.durability_.pending());

  task = storage_.CollectDiagnostics();
  StartTask(task, 0);
  SetPendingCounts(0, 3);
  ASSERT_TRUE(PumpUntil([&] { return task.done(); }));
  EXPECT_EQ(std::move(task).TakeResult().durability_.tx_commits_pending_, 3);
  SetPendingCounts(0, 0);
  for (unsigned owner = 0; owner < kWorkers; ++owner) {
    On(owner);
    auto& slot = Store(owner).staging_slots_[1];
    slot.durable_bytes_ = slot.committed_bytes_;
  }
  EXPECT_FALSE(Diagnostics().durability_.pending());
}

TEST_F(TxCleanerPipelineTestPeer, DiagnosticsReportNestedExpirationPauses) {
  for (unsigned depth = 1; depth <= 2; ++depth) {
    auto pause = storage_.QuiesceExpiration();
    StartTask(pause, 0);
    ASSERT_TRUE(PumpUntil([&] { return pause.done(); }));
    ASSERT_TRUE(std::move(pause).TakeResult().ok());
    EXPECT_EQ(Diagnostics().expiration_pause_count_, depth);
  }
  storage_.ResumeExpiration();
  EXPECT_EQ(Diagnostics().expiration_pause_count_, 1);
  storage_.ResumeExpiration();
  EXPECT_EQ(Diagnostics().expiration_pause_count_, 0);
}

TEST_F(TxCleanerPipelineTestPeer,
       ConcurrentMetricsKeepOwnerSnapshotsAndPending) {
  for (unsigned owner = 0; owner < kWorkers; ++owner) {
    On(owner);
    auto& store = Store(owner);
    // Model a large historical staging high-water mark after buffers drain.
    store.staging_slots_.resize(4096);
    for (auto& slot : store.staging_slots_) {
      slot.committed_bytes_ = kBlockHeaderBytes + 9000;
    }
    auto& dirty = store.staging_slots_[1];
    dirty.write_buffer_id_ = 1;
    dirty.committed_bytes_ = kBlockHeaderBytes + 100 * (owner + 1);
    store.replication_log_.next_lsn_ = 20 + owner;
    store.replication_log_.coverage_revocations_ = 3 + owner;
  }
  SetPendingCounts(2, 7);
  auto metrics = storage_.CollectMetrics();
  StartTask(metrics, 0);
  auto diagnostics = storage_.CollectDiagnostics();
  StartTask(diagnostics, 0);
  ASSERT_FALSE(metrics.done());
  ASSERT_FALSE(diagnostics.done());
  // Both collectors must retain their initial pending sample even if the
  // final commit/flush finishes while their remote snapshots are queued.
  SetPendingCounts(0, 0);
  ASSERT_TRUE(PumpUntil([&] { return metrics.done() && diagnostics.done(); }));
  const auto full = std::move(metrics).TakeResult();
  const auto info = std::move(diagnostics).TakeResult();
  EXPECT_EQ(full.diagnostics_.durability_.dirty_staging_bytes_, 300);
  EXPECT_EQ(full.diagnostics_.durability_.dirty_staging_bytes_,
            info.durability_.dirty_staging_bytes_);
  EXPECT_EQ(full.diagnostics_.durability_.flushes_pending_, 2);
  EXPECT_EQ(full.diagnostics_.durability_.tx_commits_pending_, 7);
  ASSERT_EQ(full.replication_logs_.size(), kWorkers);
  for (unsigned owner = 0; owner < kWorkers; ++owner) {
    EXPECT_EQ(full.replication_logs_[owner].worker_id_, owner);
    EXPECT_EQ(full.replication_logs_[owner].tail_lsn_, 19 + owner);
    EXPECT_EQ(full.replication_logs_[owner].coverage_revocations_, 3 + owner);
  }

  metrics = storage_.CollectMetrics();
  StartTask(metrics, 0);
  SetPendingCounts(1, 3);
  ASSERT_TRUE(PumpUntil([&] { return metrics.done(); }));
  const auto subsequent = std::move(metrics).TakeResult();
  EXPECT_EQ(subsequent.diagnostics_.durability_.flushes_pending_, 1);
  EXPECT_EQ(subsequent.diagnostics_.durability_.tx_commits_pending_, 3);
  SetPendingCounts(0, 0);
}

TEST_F(TxCleanerPipelineTestPeer, DiagnosticsRetainQueuePeakAfterBackpressure) {
  const auto watermark = storage_.TxCommitBatchStats().queue_high_watermark_;
  ASSERT_GT(watermark, 0);
  // Do not pump the queued runner until enqueues have reached the watermark.
  // Empty receipts exercise accounting and drain without physical I/O.
  for (std::uint64_t txid = 1; txid <= watermark + 1; ++txid) {
    EXPECT_EQ(EnqueueDiagnosticCommit(txid), txid < watermark);
  }
  const auto queued = storage_.TxCommitBatchStats();
  EXPECT_EQ(queued.queue_depth_, watermark + 1);
  EXPECT_EQ(queued.queue_peak_, watermark + 1);
  EXPECT_EQ(queued.backpressure_waits_, 2);
  ASSERT_TRUE(PumpUntil(
      [&] { return storage_.TxCommitBatchStats().queue_depth_ == 0; }));
  const auto drained = Diagnostics();
  EXPECT_EQ(drained.tx_commit_batches_.transactions_, watermark + 1);
  EXPECT_GT(drained.tx_commit_batches_.batches_, 0);
  EXPECT_EQ(drained.tx_commit_batches_.queue_depth_, 0);
  EXPECT_EQ(drained.tx_commit_batches_.queue_peak_, watermark + 1);
  EXPECT_EQ(drained.tx_commit_batches_.backpressure_waits_, 2);
  EXPECT_EQ(drained.durability_.tx_commits_pending_, 0);
}

TEST_F(TxCleanerPipelineTestPeer, DiagnosticsCountBatchFencesBeforeFailure) {
  for (std::uint64_t txid = 1; txid <= 3; ++txid) {
    std::vector<TxShardWrites> writes(1);
    // Repeated frontiers merge within the batch. An invalid owner then fails
    // durability before I/O, proving these are processing, not success counts.
    for (unsigned fence = 0; fence < 2; ++fence) {
      writes[0].fences_.push_back(
          {.block_id_ = 1,
           .allocation_epoch_ = 1,
           .committed_bytes_ = static_cast<std::uint32_t>(100 + txid),
           .block_owner_ = kWorkers});
    }
    ASSERT_TRUE(EnqueueDiagnosticCommit(txid, std::move(writes)));
  }
  ASSERT_TRUE(PumpUntil(
      [&] { return storage_.TxCommitBatchStats().queue_depth_ == 0; }));
  const auto snapshot = Diagnostics();
  EXPECT_EQ(snapshot.tx_commit_batches_.batches_, 1);
  EXPECT_EQ(snapshot.tx_commit_batches_.transactions_, 3);
  EXPECT_EQ(snapshot.tx_commit_batches_.input_fences_, 6);
  EXPECT_EQ(snapshot.tx_commit_batches_.merged_fences_, 1);
  EXPECT_EQ(snapshot.durability_.tx_commits_pending_, 0);
}

TEST_F(TxCleanerPipelineTestPeer, DiagnosticsObserveCleanerRunningAndFailure) {
  AddLiveBacklog();
  pause_round_ = true;
  round_ = [] { return absl::FailedPreconditionError("test cleaner failure"); };
  RequestCleaning();
  ASSERT_TRUE(PumpUntil([&] { return rounds_ == 1; }));
  const auto running = Diagnostics();
  EXPECT_TRUE(running.tx_cleaner_.running_);
  EXPECT_EQ(running.tx_cleaner_.rounds_, 1);
  EXPECT_EQ(running.tx_cleaner_.failures_, 0);
  // Disable retries before releasing the failed round, making the final
  // sample stable without changing how an already-running round finishes.
  ASSERT_TRUE(storage_.ConfigureTxCleanerCooldown(0).ok());
  ResumeRound();
  ASSERT_TRUE(PumpUntil([&] { return DriverIdle(); }));
  const auto failed = Diagnostics();
  EXPECT_FALSE(failed.tx_cleaner_.running_);
  EXPECT_EQ(failed.tx_cleaner_.rounds_, 1);
  EXPECT_EQ(failed.tx_cleaner_.failures_, 1);
  EXPECT_EQ(failed.tx_cleaner_.retired_blocks_, 0);
}

TEST_F(TxCleanerPipelineTestPeer,
       UnreadyExporterDoesNotReadStorageDiagnostics) {
  std::string output;
  auto task = RenderPrometheusMetrics(storage_, false, &output);
  StartTask(task, 0);
  ASSERT_TRUE(task.done());
  ASSERT_TRUE(std::move(task).TakeResult().ok());
  EXPECT_NE(output.find("lavik_server_ready 0\n"), std::string::npos);
  EXPECT_EQ(output.find("lavik_storage_"), std::string::npos);
}

TEST_F(TxCleanerPipelineTestPeer, GroupedWriteDoesNotWaitForLiveBacklog) {
  AddLiveBacklog();
  round_ = [this] {
    CompleteBacklog();
    return absl::OkStatus();
  };

  EXPECT_TRUE(PrepareWrite(0).ok());
  EXPECT_TRUE(PrepareWrite(1).ok());
  EXPECT_EQ(rounds_, 0);
  EXPECT_FALSE(DriverIdle());
  EXPECT_NE(receipts_[0].transaction_lease_, nullptr);

  ASSERT_TRUE(PumpUntil([&] { return DriverIdle(); }));
  EXPECT_EQ(rounds_, 1);  // Both owners requested the same background driver.
  EXPECT_NE(receipts_[0].transaction_lease_, nullptr);
}

TEST_F(TxCleanerPipelineTestPeer, LastLeaseStartsSmallBacklogWithoutNewWrite) {
  AddLiveBacklog(1);
  round_ = [this] {
    CompleteBacklog();
    return absl::OkStatus();
  };
  EXPECT_TRUE(DriverIdle());

  On(1);
  receipts_[0].transaction_lease_.reset();

  ASSERT_TRUE(PumpUntil([&] { return rounds_ != 0 && DriverIdle(); }));
  EXPECT_EQ(rounds_, 1);
}

TEST_F(TxCleanerPipelineTestPeer, ProgressContinuesWithoutAnotherWrite) {
  AddLiveBacklog();
  round_ = [this] {
    // Supply the result of a successful physical round. The test verifies
    // scheduling the successor, not the physical retirement algorithm.
    RecordRoundRetirement();
    if (rounds_ == 2) CompleteBacklog();
    return absl::OkStatus();
  };

  RequestCleaning();

  ASSERT_TRUE(PumpUntil([&] { return DriverIdle(); }));
  EXPECT_EQ(rounds_, 2);
  EXPECT_EQ(storage_.TxCleanerCooldownMs(), 60'000);
}

TEST_F(TxCleanerPipelineTestPeer, NoProgressRetriesWithoutForegroundWaiter) {
  AddLiveBacklog();
  round_ = [this] {
    if (rounds_ == 2) CompleteBacklog();
    return absl::OkStatus();
  };

  RequestCleaning();

  ASSERT_TRUE(PumpUntil([&] { return DriverIdle(); }));
  EXPECT_EQ(rounds_, 2);
  EXPECT_EQ(storage_.TxCleanerCooldownMs(), 60'000);
}

TEST_F(TxCleanerPipelineTestPeer,
       RetryableFailureRetriesWithoutExternalProgress) {
  AddLiveBacklog();
  round_ = [this] {
    if (rounds_ == 1)
      return absl::ResourceExhaustedError("retryable cleaner fixture");
    CompleteBacklog();
    return absl::OkStatus();
  };
  RequestCleaning();
  ASSERT_TRUE(PumpUntil([&] { return rounds_ >= 1; }));
  EXPECT_TRUE(PrepareWrite(1).ok());

  ASSERT_TRUE(PumpUntil([&] { return DriverIdle(); }));

  EXPECT_EQ(rounds_, 2);
  EXPECT_EQ(storage_.TxCleanerCooldownMs(), 60'000);
}

TEST_F(TxCleanerPipelineTestPeer,
       SeriousFailureReachesWriteAndSuccessfulRetryClearsIt) {
  AddLiveBacklog();
  bool recover = false;
  round_ = [this, &recover] {
    if (!recover) return absl::DataLossError("cleaner corruption fixture");
    CompleteBacklog();
    return absl::OkStatus();
  };
  RequestCleaning();
  ASSERT_TRUE(PumpUntil([&] { return rounds_ >= 1; }));

  EXPECT_TRUE(absl::IsDataLoss(PrepareWrite(1)));
  EXPECT_FALSE(storage_.RuntimeFailureLatched());
  recover = true;
  ASSERT_TRUE(PumpUntil([&] { return DriverIdle(); }));
  EXPECT_GE(rounds_, 2);
  EXPECT_TRUE(PrepareWrite(1).ok());
}

TEST_F(TxCleanerPipelineTestPeer,
       RepeatedFailuresBackOffAndSuccessRestoresFastProgressChecks) {
  AddLiveBacklog();
  round_ = [this] {
    if (rounds_ <= 3)
      return absl::DataLossError("persistent cleaner corruption fixture");
    if (rounds_ == 5)
      return absl::ResourceExhaustedError("transient cleaner fixture");
    if (rounds_ == 6) CompleteBacklog();
    return absl::OkStatus();
  };
  ASSERT_TRUE(storage_.ConfigureTxCleanerCooldown(250).ok());
  RequestCleaning();

  ASSERT_TRUE(PumpUntil([&] { return rounds_ == 3; }));
  EXPECT_TRUE(absl::IsDataLoss(PrepareWrite(1)));
  EXPECT_FALSE(storage_.RuntimeFailureLatched());
  ASSERT_TRUE(PumpUntil([&] { return rounds_ == 4; }));
  EXPECT_TRUE(PrepareWrite(1).ok());
  ASSERT_TRUE(PumpUntil([&] { return DriverIdle(); }));

  // The real runner executed every timer and round. Observe its scheduling
  // decisions, not elapsed wall time, so loaded CI workers cannot turn this
  // into a flaky latency assertion. Success without retirement resets the
  // failure streak and returns leases/read pins to their normal fast retry.
  EXPECT_EQ(retry_delays_,
            (std::vector<std::uint64_t>{100, 200, 250, 50, 100}));
  EXPECT_EQ(rounds_, 6);
  EXPECT_TRUE(PrepareWrite(1).ok());
}

TEST_F(TxCleanerPipelineTestPeer,
       FailureBackoffTracksChangedCooldownWithoutRestartingDriver) {
  AddLiveBacklog();
  round_ = [this] {
    if (rounds_ < 4)
      return absl::DataLossError("persistent cleaner corruption fixture");
    CompleteBacklog();
    return absl::OkStatus();
  };
  ASSERT_TRUE(storage_.ConfigureTxCleanerCooldown(150).ok());
  RequestCleaning();
  ASSERT_TRUE(PumpUntil([&] { return rounds_ == 2; }));
  EXPECT_EQ(retry_delays_, (std::vector<std::uint64_t>{100, 150}));

  // Raising the cap keeps the existing runner and its failure history. Even
  // the largest legal cooldown is accepted without overflowing timer math.
  ASSERT_TRUE(
      storage_
          .ConfigureTxCleanerCooldown(std::numeric_limits<std::uint32_t>::max())
          .ok());
  ASSERT_TRUE(PumpUntil([&] { return rounds_ == 3; }));
  EXPECT_TRUE(absl::IsDataLoss(PrepareWrite(1)));
  EXPECT_EQ(retry_delays_, (std::vector<std::uint64_t>{100, 150, 300}));

  // A lower setting applies to the already pending error wait. It does not
  // require another append, a new driver, or expiration of the old cap.
  ASSERT_TRUE(storage_.ConfigureTxCleanerCooldown(20).ok());
  ASSERT_TRUE(PumpUntil([&] { return DriverIdle(); }));
  EXPECT_EQ(retry_delays_, (std::vector<std::uint64_t>{100, 150, 300, 20}));
  EXPECT_EQ(rounds_, 4);
  EXPECT_TRUE(PrepareWrite(1).ok());
}

TEST_F(TxCleanerPipelineTestPeer, DisablingMaintenanceDrainsRetryDriver) {
  AddLiveBacklog();
  RequestCleaning();
  ASSERT_TRUE(PumpUntil([&] { return rounds_ >= 1; }));
  const auto completed_rounds = rounds_;

  On(1);
  ASSERT_TRUE(storage_.ConfigureTxCleanerCooldown(0).ok());

  ASSERT_TRUE(PumpUntil([&] { return DriverIdle(); }));
  EXPECT_EQ(rounds_, completed_rounds);
  EXPECT_TRUE(PrepareWrite(1).ok());
  EXPECT_TRUE(DriverIdle());
  EXPECT_NE(receipts_[0].transaction_lease_, nullptr);
}

TEST_F(TxCleanerPipelineTestPeer, DisablingMaintenanceDrainsFailureBackoff) {
  AddLiveBacklog();
  round_ = [] { return absl::DataLossError("persistent cleaner fixture"); };
  RequestCleaning();
  ASSERT_TRUE(PumpUntil([&] { return rounds_ == 3; }));
  ASSERT_EQ(retry_delays_, (std::vector<std::uint64_t>{100, 200, 400}));
  EXPECT_TRUE(absl::IsDataLoss(PrepareWrite(1)));

  ASSERT_TRUE(storage_.ConfigureTxCleanerCooldown(0).ok());

  ASSERT_TRUE(PumpUntil([&] { return DriverIdle(); }));
  EXPECT_EQ(rounds_, 3);
  EXPECT_TRUE(PrepareWrite(1).ok());
}

TEST_F(TxCleanerPipelineTestPeer, DisabledCleanerDoesNotFenceWritesOnOldError) {
  AddLiveBacklog();
  round_ = [] { return absl::DataLossError("cleaner corruption fixture"); };
  RequestCleaning();
  ASSERT_TRUE(PumpUntil([&] { return rounds_ >= 1; }));
  EXPECT_TRUE(absl::IsDataLoss(PrepareWrite(1)));

  ASSERT_TRUE(storage_.ConfigureTxCleanerCooldown(0).ok());

  ASSERT_TRUE(PumpUntil([&] { return DriverIdle(); }));
  EXPECT_TRUE(PrepareWrite(1).ok());
  storage_.FenceRequestServingUntilRestart();
  EXPECT_FALSE(PrepareWrite(1).ok());
}

TEST_F(TxCleanerPipelineTestPeer, ShutdownDrainsRetryAndRejectsNewWrite) {
  AddLiveBacklog();
  RequestCleaning();
  ASSERT_TRUE(PumpUntil([&] { return rounds_ >= 1; }));
  const auto completed_rounds = rounds_;

  BeginShutdown();

  EXPECT_TRUE(absl::IsUnavailable(PrepareWrite(1)));
  ASSERT_TRUE(PumpUntil([&] { return DriverIdle(); }));
  EXPECT_EQ(rounds_, completed_rounds);
}

TEST_F(TxCleanerPipelineTestPeer, ShutdownDrainsFailureBackoff) {
  AddLiveBacklog();
  round_ = [] { return absl::DataLossError("persistent cleaner fixture"); };
  RequestCleaning();
  ASSERT_TRUE(PumpUntil([&] { return rounds_ == 3; }));
  ASSERT_EQ(retry_delays_, (std::vector<std::uint64_t>{100, 200, 400}));

  BeginShutdown();

  EXPECT_TRUE(absl::IsUnavailable(PrepareWrite(1)));
  ASSERT_TRUE(PumpUntil([&] { return DriverIdle(); }));
  EXPECT_EQ(rounds_, 3);
}

TEST_F(TxCleanerPipelineTestPeer,
       WorkerStopDiscardsQueuedCleanerAndRejectsWrite) {
  AddLiveBacklog();
  RequestCleaning();
  ASSERT_FALSE(DriverIdle());

  workers_[0].RequestStop();

  EXPECT_TRUE(absl::IsUnavailable(PrepareWrite(0)));
  ASSERT_TRUE(PumpUntil([&] { return DriverIdle(); }));
  EXPECT_EQ(rounds_, 0);
}

TEST_F(TxCleanerPipelineTestPeer, StorageFailureStopsRetryAndRejectsWrite) {
  AddLiveBacklog();
  RequestCleaning();
  ASSERT_TRUE(PumpUntil([&] { return rounds_ >= 1; }));
  const auto completed_rounds = rounds_;

  On(1);
  storage_.FenceRequestServingUntilRestart();

  EXPECT_FALSE(PrepareWrite(1).ok());
  ASSERT_TRUE(PumpUntil([&] { return DriverIdle(); }));
  EXPECT_EQ(rounds_, completed_rounds);
}

TEST_F(TxCleanerPipelineTestPeer, FullDoesNotWaitForMerelyScheduledCleaner) {
  PrepareFullDevice();
  AddLiveBacklog();
  RequestCleaning();
  ASSERT_FALSE(DriverIdle());
  ASSERT_EQ(rounds_, 0);

  StartCapacity(0);

  // The owner-local allocator can prove FULL synchronously. A scheduled
  // runner that has not started an actual round must not delay that result.
  ASSERT_TRUE(CapacityDone(0));
  EXPECT_TRUE(absl::IsResourceExhausted(CapacityResult(0).status()));
  EXPECT_TRUE(capacity_lock_restored_[0]);
  EXPECT_EQ(rounds_, 0);
  EXPECT_EQ(CapacityWaiters(0), 0);
}

TEST_F(TxCleanerPipelineTestPeer,
       WriterThatRetainsStoreStateDoesNotWaitForCleanerCapacity) {
  PrepareFullDevice();
  AddLiveBacklog();
  pause_round_ = true;
  RequestCleaning(0);
  ASSERT_TRUE(PumpUntil([&] { return rounds_ == 1; }));

  StartCapacity(0, false);

  // Replica reset and similar atomic writers retain store state. Joining a
  // cleaner that needs that same lock would prevent the round from ending.
  ASSERT_TRUE(CapacityDone(0));
  EXPECT_TRUE(absl::IsResourceExhausted(CapacityResult(0).status()));
  EXPECT_TRUE(capacity_lock_restored_[0]);
  EXPECT_EQ(CapacityWaiters(0), 0);
  EXPECT_TRUE(CleanerRoundRunning());
}

TEST_F(TxCleanerPipelineTestPeer,
       FirstReclaimedBlockWakesAllocatorBeforeCleanerRoundCompletes) {
  PrepareFullDevice();
  AddLiveBacklog();
  pause_round_ = true;
  RequestCleaning(0);
  ASSERT_TRUE(PumpUntil([&] { return rounds_ == 1; }));
  StartCapacity(1);
  ASSERT_TRUE(PumpUntil([&] { return CapacityWaiters(1) == 1; }));
  EXPECT_TRUE(StoreLockAvailable(1));

  ReleaseCapacityBlock();

  ASSERT_TRUE(PumpUntil([&] { return CapacityDone(1); }));
  const auto allocated = CapacityResult(1);
  ASSERT_TRUE(allocated.ok()) << allocated.status();
  EXPECT_EQ(allocated->block_id_, 9);
  EXPECT_TRUE(capacity_lock_restored_[1]);
  EXPECT_EQ(CapacityWaiters(1), 0);
  EXPECT_TRUE(CleanerRoundRunning());
  EXPECT_EQ(CompletedCleanerRounds(), 0);
}

TEST_F(TxCleanerPipelineTestPeer,
       CompetingAllocatorsKeepWaitingOnlyWithinTheObservedRound) {
  PrepareFullDevice();
  AddLiveBacklog();
  pause_round_ = true;
  RequestCleaning(0);
  ASSERT_TRUE(PumpUntil([&] { return rounds_ == 1; }));
  StartCapacity(0);
  StartCapacity(1);
  ASSERT_TRUE(PumpUntil(
      [&] { return CapacityWaiters(0) == 1 && CapacityWaiters(1) == 1; }));

  ReleaseCapacityBlock();

  ASSERT_TRUE(PumpUntil([&] {
    return CapacityDone(0) != CapacityDone(1) &&
           CapacityWaiters(0) + CapacityWaiters(1) == 1;
  }));
  const unsigned winner = CapacityDone(0) ? 0 : 1;
  const unsigned loser = 1 - winner;
  ASSERT_TRUE(CapacityResult(winner).ok());
  EXPECT_TRUE(StoreLockAvailable(loser));
  // The other worker lost this reclaimed block. Completing the round without
  // more space must return FULL, even if its live lease keeps the background
  // driver scheduled and a successor round starts before it resumes.
  ResumeRound(true);
  ASSERT_TRUE(PumpUntil([&] { return CapacityDone(loser); }));
  EXPECT_TRUE(absl::IsResourceExhausted(CapacityResult(loser).status()));
  EXPECT_TRUE(capacity_lock_restored_[0]);
  EXPECT_TRUE(capacity_lock_restored_[1]);
  EXPECT_EQ(CapacityWaiters(0) + CapacityWaiters(1), 0);
  EXPECT_NE(receipts_[0].transaction_lease_, nullptr);
}

TEST_F(TxCleanerPipelineTestPeer,
       NoProgressRoundReturnsFullWithoutWaitingForOwnLease) {
  PrepareFullDevice();
  AddLiveBacklog();
  pause_round_ = true;
  RequestCleaning(0);
  ASSERT_TRUE(PumpUntil([&] { return rounds_ == 1; }));
  StartCapacity(1);
  ASSERT_TRUE(PumpUntil([&] { return CapacityWaiters(1) == 1; }));

  ResumeRound(true);

  ASSERT_TRUE(PumpUntil([&] { return CapacityDone(1); }));
  EXPECT_TRUE(absl::IsResourceExhausted(CapacityResult(1).status()));
  EXPECT_TRUE(capacity_lock_restored_[1]);
  EXPECT_NE(receipts_[0].transaction_lease_, nullptr);
  EXPECT_EQ(CapacityWaiters(1), 0);
}

TEST_F(TxCleanerPipelineTestPeer,
       FailedCapacityRoundReturnsFullAndPreservesCleanerError) {
  PrepareFullDevice();
  AddLiveBacklog();
  pause_round_ = true;
  round_ = [] { return absl::DataLossError("capacity cleaner failure"); };
  RequestCleaning(0);
  ASSERT_TRUE(PumpUntil([&] { return rounds_ == 1; }));
  StartCapacity(1);
  ASSERT_TRUE(PumpUntil([&] { return CapacityWaiters(1) == 1; }));

  ResumeRound(true);

  ASSERT_TRUE(PumpUntil([&] { return CapacityDone(1); }));
  EXPECT_TRUE(absl::IsResourceExhausted(CapacityResult(1).status()));
  EXPECT_TRUE(absl::IsDataLoss(PrepareWrite(1)));
  EXPECT_TRUE(capacity_lock_restored_[1]);
  EXPECT_EQ(CapacityWaiters(1), 0);
}

TEST_F(TxCleanerPipelineTestPeer,
       CapacityRecheckDoesNotJoinSuccessorAfterMissedRoundCompletion) {
  AddLiveBacklog();
  pause_round_ = true;
  round_ = [this] {
    CompleteBacklog();
    return absl::OkStatus();
  };
  RequestCleaning(0);
  ASSERT_TRUE(PumpUntil([&] { return rounds_ == 1; }));
  const auto completed = CompletedCleanerRounds();
  const auto space = ReclaimGeneration();
  ResumeRound();
  ASSERT_TRUE(PumpUntil([&] { return DriverIdle(); }));
  AddLiveBacklog();
  pause_round_ = true;
  RequestCleaning(0);
  ASSERT_TRUE(PumpUntil([&] { return rounds_ == 2; }));

  // Model completion and restart between the failed allocation and waiter
  // registration. running=true now belongs to a different round.
  StartCapacityProbe(1, completed, space);

  ASSERT_TRUE(capacity_probe_->done());
  EXPECT_TRUE(std::move(*capacity_probe_).TakeResult());
  EXPECT_EQ(CapacityWaiters(1), 0);
  EXPECT_TRUE(CleanerRoundRunning());
}

TEST_F(TxCleanerPipelineTestPeer, DisablingCleanerWakesCapacityWaiter) {
  PrepareFullDevice();
  AddLiveBacklog();
  pause_round_ = true;
  RequestCleaning(0);
  ASSERT_TRUE(PumpUntil([&] { return rounds_ == 1; }));
  StartCapacity(1);
  ASSERT_TRUE(PumpUntil([&] { return CapacityWaiters(1) == 1; }));

  On(0);
  ASSERT_TRUE(storage_.ConfigureTxCleanerCooldown(0).ok());

  ASSERT_TRUE(PumpUntil([&] { return CapacityDone(1); }));
  EXPECT_TRUE(absl::IsResourceExhausted(CapacityResult(1).status()));
  EXPECT_TRUE(capacity_lock_restored_[1]);
  EXPECT_TRUE(CleanerRoundRunning());
  EXPECT_EQ(CapacityWaiters(1), 0);
}

TEST_F(TxCleanerPipelineTestPeer, ShutdownWakesCapacityWaiter) {
  PrepareFullDevice();
  AddLiveBacklog();
  pause_round_ = true;
  RequestCleaning(0);
  ASSERT_TRUE(PumpUntil([&] { return rounds_ == 1; }));
  StartCapacity(1);
  ASSERT_TRUE(PumpUntil([&] { return CapacityWaiters(1) == 1; }));

  On(0);
  BeginShutdown();

  ASSERT_TRUE(PumpUntil([&] { return CapacityDone(1); }));
  EXPECT_TRUE(absl::IsUnavailable(CapacityResult(1).status()));
  EXPECT_TRUE(capacity_lock_restored_[1]);
  EXPECT_EQ(CapacityWaiters(1), 0);
}

TEST_F(TxCleanerPipelineTestPeer, StorageFailureWakesCapacityWaiter) {
  PrepareFullDevice();
  AddLiveBacklog();
  pause_round_ = true;
  RequestCleaning(0);
  ASSERT_TRUE(PumpUntil([&] { return rounds_ == 1; }));
  StartCapacity(1);
  ASSERT_TRUE(PumpUntil([&] { return CapacityWaiters(1) == 1; }));

  On(0);
  storage_.FenceRequestServingUntilRestart();

  ASSERT_TRUE(PumpUntil([&] { return CapacityDone(1); }));
  EXPECT_TRUE(absl::IsFailedPrecondition(CapacityResult(1).status()));
  EXPECT_TRUE(capacity_lock_restored_[1]);
  EXPECT_EQ(CapacityWaiters(1), 0);
}

TEST_F(TxCleanerPipelineTestPeer, UnchangedPromotionDoesNotRereadSource) {
  PreparePromotion();

  EXPECT_EQ(Promote(), absl::OkStatus());
  EXPECT_EQ(Promote(), absl::OkStatus());
  EXPECT_EQ(Promote(), absl::OkStatus());

  EXPECT_EQ(promotion_scans_, 1);
}

TEST_F(TxCleanerPipelineTestPeer,
       DependencyPinnedBlockStillScansMovableRecordsOnce) {
  PreparePromotion();
  ASSERT_TRUE(PinPromotionDependency());

  // A pin can protect only some records in a block. It must not prevent the
  // first scan from finding unrelated current records that can be relocated.
  EXPECT_EQ(Promote(), absl::OkStatus());
  EXPECT_EQ(Promote(), absl::OkStatus());
  EXPECT_EQ(promotion_scans_, 1);

  UnpinPromotionDependency();
  EXPECT_EQ(Promote(), absl::OkStatus());
  EXPECT_EQ(promotion_scans_, 2);
}

TEST_F(TxCleanerPipelineTestPeer,
       RecordRetirementAndDependencyChangesInvalidatePromotion) {
  PreparePromotion();
  EXPECT_EQ(Promote(), absl::OkStatus());

  ASSERT_TRUE(PinPromotionDependency());
  EXPECT_EQ(Promote(), absl::OkStatus());
  EXPECT_EQ(promotion_scans_, 2);
  UnpinPromotionDependency();
  EXPECT_EQ(Promote(), absl::OkStatus());
  EXPECT_EQ(promotion_scans_, 3);
  RetirePromotionRecord();
  EXPECT_EQ(Promote(), absl::OkStatus());
  EXPECT_EQ(promotion_scans_, 4);

  // Equal before/after pin counts are insufficient: a dependency may have
  // changed which records are movable between two cleaner rounds.
  ASSERT_TRUE(PinPromotionDependency());
  UnpinPromotionDependency();
  EXPECT_EQ(Promote(), absl::OkStatus());
  EXPECT_EQ(Promote(), absl::OkStatus());
  EXPECT_EQ(promotion_scans_, 5);
}

TEST_F(TxCleanerPipelineTestPeer, DependencyChangeDuringScanRequiresRescan) {
  PreparePromotion();
  pause_promotion_scan_ = true;
  auto task = Promotion();
  StartPromotion(task);
  EXPECT_FALSE(task.done());
  EXPECT_EQ(promotion_scans_, 1);

  EXPECT_TRUE(PinPromotionDependency());
  UnpinPromotionDependency();
  ResumePromotionScan();
  EXPECT_EQ(FinishPromotion(task), absl::OkStatus());

  EXPECT_EQ(Promote(), absl::OkStatus());
  EXPECT_EQ(Promote(), absl::OkStatus());
  EXPECT_EQ(promotion_scans_, 2);
}

TEST_F(TxCleanerPipelineTestPeer, RollbackDuringScanRequiresRescan) {
  PreparePromotion();
  pause_promotion_scan_ = true;
  auto task = Promotion();
  StartPromotion(task);
  EXPECT_FALSE(task.done());

  RewindPromotionIndex();
  ResumePromotionScan();
  EXPECT_EQ(FinishPromotion(task), absl::OkStatus());

  EXPECT_EQ(Promote(), absl::OkStatus());
  EXPECT_EQ(Promote(), absl::OkStatus());
  EXPECT_EQ(promotion_scans_, 2);
}

TEST_F(TxCleanerPipelineTestPeer, FailedOrCancelledScanIsRetried) {
  PreparePromotion();
  promotion_scan_result_ = absl::DataLossError("promotion fixture failure");
  EXPECT_TRUE(absl::IsDataLoss(Promote()));
  promotion_scan_result_ = absl::CancelledError("promotion fixture shutdown");
  EXPECT_TRUE(absl::IsCancelled(Promote()));
  promotion_scan_result_ = absl::OkStatus();

  EXPECT_EQ(Promote(), absl::OkStatus());
  EXPECT_EQ(Promote(), absl::OkStatus());
  EXPECT_EQ(promotion_scans_, 3);
}

TEST_F(TxCleanerPipelineTestPeer, RollbackInvalidatesCompletedPromotion) {
  PreparePromotion();
  EXPECT_EQ(Promote(), absl::OkStatus());

  RewindPromotionIndex();

  EXPECT_EQ(Promote(), absl::OkStatus());
  EXPECT_EQ(Promote(), absl::OkStatus());
  EXPECT_EQ(promotion_scans_, 2);
}

TEST_F(TxCleanerPipelineTestPeer, ReusedAllocationRequiresFreshPromotion) {
  PreparePromotion();
  EXPECT_EQ(Promote(), absl::OkStatus());

  ResetPromotionSource(8);

  EXPECT_TRUE(absl::IsFailedPrecondition(Promote(7)));
  EXPECT_EQ(promotion_scans_, 1);
  EXPECT_EQ(Promote(8), absl::OkStatus());
  EXPECT_EQ(Promote(8), absl::OkStatus());
  EXPECT_EQ(promotion_scans_, 2);
}

TEST_F(TxCleanerPipelineTestPeer,
       CompletedScanStillWaitsForPendingRelocationDurability) {
  PreparePromotion();
  EXPECT_EQ(Promote(), absl::OkStatus());
  OwePromotionFence();

  auto task = Promotion();
  StartPromotion(task);
  EXPECT_FALSE(task.done());
  EXPECT_EQ(promotion_scans_, 1);
  EXPECT_TRUE(HasPromotionFence());

  CompletePromotionFence();
  EXPECT_EQ(FinishPromotion(task), absl::OkStatus());
  EXPECT_FALSE(HasPromotionFence());
  EXPECT_EQ(promotion_scans_, 1);
}

TEST_F(TxCleanerPipelineTestPeer,
       FailedRelocationFenceKeepsDebtAndPreventsScanProof) {
  PreparePromotion();
  OwePromotionFence();
  auto failed = Promotion();
  StartPromotion(failed);
  EXPECT_FALSE(failed.done());
  EXPECT_EQ(promotion_scans_, 1);

  FailPromotionFence(true);
  EXPECT_TRUE(absl::IsInternal(FinishPromotion(failed)));
  EXPECT_TRUE(HasPromotionFence());
  FailPromotionFence(false);

  auto retry = Promotion();
  StartPromotion(retry);
  EXPECT_FALSE(retry.done());
  EXPECT_EQ(promotion_scans_, 2);
  CompletePromotionFence();
  EXPECT_EQ(FinishPromotion(retry), absl::OkStatus());
  EXPECT_FALSE(HasPromotionFence());
  EXPECT_EQ(Promote(), absl::OkStatus());
  EXPECT_EQ(promotion_scans_, 2);
}

#else

TEST(TxCleanerPipelineTestPeer, RequiresFaultHooks) {
  GTEST_SKIP() << "Cleaner scheduling tests require Debug or test fault hooks";
}

#endif

}  // namespace lavik::storage
