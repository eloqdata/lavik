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
    engine.tx_cleaner_round_for_test_ = [this] {
      ++rounds_;
      return round_ ? round_() : absl::OkStatus();
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
      // Leave the round hook alive until a queued or timer-suspended driver
      // exits, including after an assertion fails midway through the test.
      EXPECT_TRUE(PumpUntil([&] { return DriverIdle(); }));
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
  }

  bool DriverIdle() const {
    return !storage_.impl_->tx_cleaner_scheduled_.load() &&
           !storage_.impl_->tx_cleaner_running_.load();
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
  unsigned promotion_scans_ = 0;
  bool pause_promotion_scan_ = false;
  absl::Status promotion_scan_result_;
  AsyncNotification promotion_continue_;
};

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
