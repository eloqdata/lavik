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
#include <memory>
#include <span>
#include <utility>

#include "../src/storage/engine/impl.h"
#include "gtest/gtest.h"

namespace lavik::storage {

#if LAVIK_FAULTS_ENABLED

// These tests substitute the physical cleaner round, not its scheduler or
// foreground preparation. Real workers exercise task ownership, cross-worker
// coalescing, timer retries and result publication. Physical retirement and
// persistence are covered by multikey and low-capacity storage E2E tests.
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
  StorageEngine storage_{StorageEngineOptions{}};
  std::deque<TxShardWrites> receipts_;
  std::function<absl::Status()> round_;
  unsigned rounds_ = 0;
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

#else

TEST(TxCleanerPipelineTestPeer, RequiresFaultHooks) {
  GTEST_SKIP() << "Cleaner scheduling tests require Debug or test fault hooks";
}

#endif

}  // namespace lavik::storage
