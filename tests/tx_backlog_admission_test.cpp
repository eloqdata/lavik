/*
 * Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>
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

// Only the sealed-block observations are supplied by the fixture. Admission,
// lease destruction, configuration changes and cross-worker notification use
// their production paths. Explicitly pumping both real workers makes the
// probe/notification race deterministic without storage I/O or timing ratios.
class TxBacklogAdmissionTestPeer : public testing::Test {
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
      engine.stores_.push_back(std::move(store));
    }
    On(0);
    // This test supplies no physical blocks for the cleaner to inspect. Keep
    // admission enabled while an existing-driver marker suppresses physical
    // maintenance. Driver tests replace the marker with their controlled round.
    ASSERT_TRUE(storage_.ConfigureTxCleanerCooldown(60'000).ok());
    engine.tx_cleaner_scheduled_.store(true);
    engine.tx_backlog_before_wait_for_test_ = [this] {
      ++before_wait_[bycorf::ThisWorker().id_];
      if (before_wait_action_) {
        auto action = std::move(before_wait_action_);
        action();
      }
    };
  }

  void TearDown() override {
    if (storage_.impl_ && storage_.impl_->stores_.size() == kWorkers) {
      On(0);
      storage_.impl_->tx_backlog_before_wait_for_test_ = {};
      storage_.impl_->tx_cleaner_round_for_test_ = {};
      receipts_.clear();
      storage_.ConfigureTxCleanerCooldown(0).IgnoreError();
      if (!real_cleaner_driver_)
        storage_.impl_->tx_cleaner_scheduled_.store(false);
      // A failed assertion must not destroy a frame still registered in a
      // notification or referenced by an outstanding cross-worker probe.
      for (unsigned owner = 0; owner < kWorkers; ++owner) {
        On(owner);
        Store(owner).tx_backlog_progress_.NotifyAll(workers_[owner]);
      }
      EXPECT_TRUE(PumpUntil([&] {
        return !storage_.impl_->tx_cleaner_scheduled_.load() &&
               !storage_.impl_->tx_cleaner_running_.load() &&
               std::all_of(waits_.begin(), waits_.end(),
                           [](const auto& wait) { return wait.done(); });
      }));
      for (unsigned round = 0; round < 2; ++round) Pump();
      waits_.clear();
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

  void AddWriter(unsigned owner = 1) {
    On(owner);
    auto& receipt = receipts_.emplace_back();
    storage_.InitializeTxWrites(receipts_.size(), std::span(&receipt, 1));
    auto& store = Store(owner);
    for (std::uint64_t block_id = 1; block_id <= 3; ++block_id) {
      auto& block = store.tx_blocks_[block_id];
      block.allocation_epoch_ = 1;
      block.counted_backlog_ = true;
      block.backlog_bytes_ = kPayloadBytes;
      block.txids_.Record(receipt.txid_, receipt.transaction_lease_);
    }
    store.tx_backlog_bytes_.store(3 * kPayloadBytes);
    store.tx_backlog_may_have_writers_.store(true);
    ASSERT_TRUE(storage_.TxBacklogAtLimit());
  }

  auto& StartWait(unsigned owner) {
    On(owner);
    auto& wait = waits_.emplace_back(storage_.WaitForTxBacklog());
    auto handle = std::move(wait).operator co_await().await_suspend(
        std::noop_coroutine());
    handle.resume();
    return wait;
  }

  void PumpOwner(unsigned owner) {
    On(owner);
    workers_[owner].RunOnce(false);
  }

  void Pump() {
    for (unsigned owner = 0; owner < kWorkers; ++owner) PumpOwner(owner);
  }

  template <typename Predicate>
  bool PumpUntil(Predicate ready) {
    // This is a deadlock watchdog, not a latency or throughput assertion.
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!ready() && std::chrono::steady_clock::now() < deadline) Pump();
    return ready();
  }

  void ReleaseWriter(unsigned index, unsigned releasing_owner = 1) {
    On(releasing_owner);
    receipts_[index].transaction_lease_.reset();
  }

  void RetireObservedBlock(std::uint64_t block_id) {
    On(1);
    auto& store = Store(1);
    const auto block = store.tx_blocks_.find(block_id);
    ASSERT_NE(block, store.tx_blocks_.end());
    store.tx_backlog_bytes_.fetch_sub(block->second.backlog_bytes_);
    store.tx_blocks_.erase(block);
    // Simulate the metadata transition after physical retirement. The real
    // admission loop must re-evaluate its threshold, not grant a free-block
    // token to whichever waiter happened to receive this progress event.
    storage_.impl_->NotifyTxBacklogProgress();
  }

  std::uint64_t Generation() const {
    return storage_.impl_->tx_backlog_progress_generation_.load();
  }

  void BeginShutdown() {
    storage_.impl_->shutdown_flush_requested_.store(true);
    storage_.impl_->NotifyTxBacklogProgress();
  }

  void ControlCleanerRounds(std::function<absl::Status()> round) {
    storage_.impl_->tx_cleaner_round_for_test_ = std::move(round);
    real_cleaner_driver_ = true;
    storage_.impl_->tx_cleaner_scheduled_.store(false);
    storage_.impl_->tx_backlog_retry_ms_.store(0);
  }

  bycorf::CurrentWorker saved_context_;
  bycorf::CrossCore cross_core_{kWorkers};
  std::array<bycorf::Worker, kWorkers> workers_;
  std::array<int, kWorkers> wake_fds_{-1, -1};
  std::array<bool, kWorkers> initialized_{};
  StorageEngine storage_{StorageEngineOptions{}};
  std::deque<TxShardWrites> receipts_;
  std::deque<bycorf::Task<absl::Status>> waits_;
  std::array<unsigned, kWorkers> before_wait_{};
  std::function<void()> before_wait_action_;
  bool real_cleaner_driver_ = false;
};

TEST_F(TxBacklogAdmissionTestPeer, LastLeaseWakesAllLocalAndRemoteWaiters) {
  AddWriter();
  auto& remote = StartWait(0);
  auto& local = StartWait(1);
  ASSERT_TRUE(PumpUntil([&] { return before_wait_[0] && before_wait_[1]; }));
  ASSERT_FALSE(remote.done());
  ASSERT_FALSE(local.done());

  ReleaseWriter(0);

  ASSERT_TRUE(PumpUntil([&] { return remote.done() && local.done(); }));
  EXPECT_TRUE(std::move(remote).TakeResult().ok());
  EXPECT_TRUE(std::move(local).TakeResult().ok());
  EXPECT_EQ(Store(0).tx_backlog_waiters_.load(), 0);
  EXPECT_EQ(Store(1).tx_backlog_waiters_.load(), 0);
}

TEST_F(TxBacklogAdmissionTestPeer, AnotherLiveLeaseKeepsAdmissionBlocked) {
  AddWriter();
  AddWriter();
  auto& wait = StartWait(0);
  ASSERT_TRUE(PumpUntil([&] { return before_wait_[0] != 0; }));
  const auto probed = before_wait_[0];

  ReleaseWriter(0);

  ASSERT_TRUE(PumpUntil([&] { return before_wait_[0] > probed; }));
  EXPECT_FALSE(wait.done());
  EXPECT_TRUE(storage_.TxBacklogAtLimit());
  ReleaseWriter(1);
  ASSERT_TRUE(PumpUntil([&] { return wait.done(); }));
  EXPECT_TRUE(std::move(wait).TakeResult().ok());
}

TEST_F(TxBacklogAdmissionTestPeer,
       ReclaimedBlockWakesButDoesNotBypassThreshold) {
  AddWriter();
  auto& wait = StartWait(0);
  ASSERT_TRUE(PumpUntil([&] { return before_wait_[0] != 0; }));
  const auto probed = before_wait_[0];

  RetireObservedBlock(1);

  ASSERT_TRUE(PumpUntil([&] { return before_wait_[0] > probed; }));
  EXPECT_FALSE(wait.done());
  EXPECT_TRUE(storage_.TxBacklogAtLimit());
  RetireObservedBlock(2);
  ASSERT_TRUE(PumpUntil([&] { return wait.done(); }));
  EXPECT_TRUE(std::move(wait).TakeResult().ok());
  EXPECT_NE(receipts_[0].transaction_lease_, nullptr);
}

TEST_F(TxBacklogAdmissionTestPeer, ProgressDeliveredBeforeParkingIsNotLost) {
  AddWriter();
  auto& wait = StartWait(0);
  ASSERT_EQ(before_wait_[0], 0);
  ASSERT_EQ(Store(0).tx_backlog_waiters_.load(), 1);
  // Complete only the remote positive probe. Its reply is queued, but the
  // origin has not resumed from it yet.
  PumpOwner(1);
  ASSERT_TRUE(Store(1).tx_backlog_writer_block_.has_value());
  ASSERT_EQ(before_wait_[0], 0);
  ASSERT_FALSE(wait.done());
  ReleaseWriter(0);
  const auto released_generation = Generation();
  bool notification_preceded_park = false;
  before_wait_action_ = [&] {
    notification_preceded_park =
        Store(0).tx_backlog_wake_generation_ == released_generation;
  };

  // Cross-core draining delivers the notification before running the queued
  // reply continuation. That notification finds no parked coroutine: only
  // the generation recheck can prevent a wait for an event already delivered.
  ASSERT_TRUE(PumpUntil([&] { return wait.done(); }));
  EXPECT_TRUE(notification_preceded_park);
  EXPECT_TRUE(std::move(wait).TakeResult().ok());
}

TEST_F(TxBacklogAdmissionTestPeer, DisablingCleanerWakesBlockedAdmissions) {
  AddWriter();
  auto& wait = StartWait(0);
  ASSERT_TRUE(PumpUntil([&] { return before_wait_[0] != 0; }));

  On(1);
  ASSERT_TRUE(storage_.ConfigureTxCleanerCooldown(0).ok());

  ASSERT_TRUE(PumpUntil([&] { return wait.done(); }));
  EXPECT_TRUE(std::move(wait).TakeResult().ok());
  EXPECT_NE(receipts_[0].transaction_lease_, nullptr);
}

TEST_F(TxBacklogAdmissionTestPeer, ShutdownWakesWithUnavailable) {
  AddWriter();
  auto& wait = StartWait(0);
  ASSERT_TRUE(PumpUntil([&] { return before_wait_[0] != 0; }));

  On(1);
  BeginShutdown();

  ASSERT_TRUE(PumpUntil([&] { return wait.done(); }));
  EXPECT_TRUE(absl::IsUnavailable(std::move(wait).TakeResult()));
}

TEST_F(TxBacklogAdmissionTestPeer, StorageFailureWakesWithoutAdmittingWrite) {
  AddWriter();
  auto& wait = StartWait(0);
  ASSERT_TRUE(PumpUntil([&] { return before_wait_[0] != 0; }));

  On(1);
  storage_.FenceRequestServingUntilRestart();

  ASSERT_TRUE(PumpUntil([&] { return wait.done(); }));
  EXPECT_FALSE(std::move(wait).TakeResult().ok());
  EXPECT_TRUE(storage_.TxBacklogAtLimit());
  EXPECT_NE(receipts_[0].transaction_lease_, nullptr);
}

TEST_F(TxBacklogAdmissionTestPeer, WorkerStopDuringPositiveProbeDoesNotPark) {
  AddWriter();
  auto& wait = StartWait(0);
  PumpOwner(1);
  ASSERT_TRUE(Store(1).tx_backlog_writer_block_.has_value());
  ASSERT_EQ(before_wait_[0], 0);

  // The reply is already queued, but there has never been a parked backlog
  // continuation to wake. Resuming from the probe must observe worker stop.
  workers_[0].RequestStop();

  ASSERT_TRUE(PumpUntil([&] { return wait.done(); }));
  EXPECT_TRUE(absl::IsUnavailable(std::move(wait).TakeResult()));
}

TEST_F(TxBacklogAdmissionTestPeer, WorkerStopDuringSettledProbeDoesNotAdmit) {
  AddWriter();
  auto& wait = StartWait(0);
  ReleaseWriter(0);
  PumpOwner(1);
  ASSERT_FALSE(Store(1).tx_backlog_may_have_writers_.load());
  ASSERT_EQ(before_wait_[0], 0);

  // Even an asynchronous probe reporting no remaining writers cannot admit
  // a request after its worker has begun stopping.
  workers_[0].RequestStop();

  ASSERT_TRUE(PumpUntil([&] { return wait.done(); }));
  EXPECT_TRUE(absl::IsUnavailable(std::move(wait).TakeResult()));
}

TEST_F(TxBacklogAdmissionTestPeer,
       RetryableCleanerFailureRetriesWithoutExternalProgress) {
  AddWriter();
  unsigned rounds = 0;
  ControlCleanerRounds([&] {
    if (++rounds == 1)
      return absl::ResourceExhaustedError("retryable cleaner fixture");
    // Only the driver's second attempt supplies progress. No timer in the
    // fixture or foreground command releases the lease to provoke a retry.
    receipts_[0].transaction_lease_.reset();
    return absl::OkStatus();
  });
  auto& remote = StartWait(0);
  auto& local = StartWait(1);

  ASSERT_TRUE(PumpUntil([&] { return remote.done() && local.done(); }));

  EXPECT_EQ(rounds, 2);
  EXPECT_EQ(storage_.TxCleanerCooldownMs(), 60'000);
  EXPECT_TRUE(std::move(remote).TakeResult().ok());
  EXPECT_TRUE(std::move(local).TakeResult().ok());
}

TEST_F(TxBacklogAdmissionTestPeer, FatalCleanerFailureWakesBlockedAdmissions) {
  AddWriter();
  unsigned rounds = 0;
  ControlCleanerRounds([&] {
    ++rounds;
    return absl::DataLossError("fatal cleaner fixture");
  });
  auto& remote = StartWait(0);
  auto& local = StartWait(1);

  ASSERT_TRUE(PumpUntil([&] { return remote.done() && local.done(); }));

  EXPECT_EQ(rounds, 1);
  EXPECT_TRUE(absl::IsDataLoss(std::move(remote).TakeResult()));
  EXPECT_TRUE(absl::IsDataLoss(std::move(local).TakeResult()));
  EXPECT_NE(receipts_[0].transaction_lease_, nullptr);
  // This is error propagation from the cleaner, not a notification caused by
  // storage failure or by the pressure predicate becoming false.
  EXPECT_FALSE(storage_.RuntimeFailureLatched());
  EXPECT_TRUE(storage_.TxBacklogAtLimit());
}

#else

TEST(TxBacklogAdmissionTestPeer, RequiresFaultHooks) {
  GTEST_SKIP() << "Admission race tests require Debug or test fault hooks";
}

#endif

}  // namespace lavik::storage
