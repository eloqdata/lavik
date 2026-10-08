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

#include "lavik/memory.h"

#include <gtest/gtest.h>

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "../src/storage/engine/recovery_allocator.h"

TEST(MemoryTest, RetainedAdmissionLeavesTenPercentOutsideRetainedState) {
  constexpr std::uint64_t kInitialLimit = 1024ULL * 1024 * 1024;
  constexpr std::uint64_t kSteadyGrowth = 16ULL * 1024 * 1024;
  ASSERT_TRUE(lavik::InitMemoryLimit(kInitialLimit, 1).ok());
  lavik::BindMemoryAccountingShard(0);
  lavik::RefreshMemoryStats();
  const std::uint64_t used = lavik::GetMemoryStats().used_bytes_;
  const std::uint64_t target_steady = used + kSteadyGrowth;

  // Solve max - floor(max / 10) >= target_steady without exposing the policy
  // calculation as mutable runtime state.
  std::uint64_t maximum = (target_steady * 10 + 8) / 9;
  while (maximum - maximum / 10 < target_steady) ++maximum;
  ASSERT_TRUE(lavik::InitMemoryLimit(maximum, 1).ok());
  const std::uint64_t steady = maximum - maximum / 10;
  const std::size_t steady_remaining = static_cast<std::size_t>(steady - used);

  EXPECT_FALSE(lavik::WouldExceedMemoryLimit(steady_remaining));
  EXPECT_TRUE(lavik::WouldExceedMemoryLimit(steady_remaining + 1));
  auto retained = lavik::TryReserveMemory(steady_remaining);
  ASSERT_TRUE(retained.has_value());
  EXPECT_TRUE(lavik::WouldExceedMemoryLimit(1));
  EXPECT_FALSE(lavik::TryReserveFullSyncMemory(1));

  // Client buffers have an independent abuse-control quota. At its default 5%,
  // a full client quota still leaves 5% outside both admitted classes.
  {
    lavik::ClientBufferReservation client;
    EXPECT_TRUE(client.TryAcquire(1));
  }

  retained.reset();
  lavik::BindMemoryAccountingShard(lavik::kMaxMemoryWorkers);
}

TEST(MemoryTest, ClientBuffersHaveAWorkerLocalFivePercentLimit) {
  constexpr std::uint64_t kMaximum = 1024ULL * 1024 * 1024;
  constexpr std::size_t kClientCapacity = kMaximum / 20;
  ASSERT_TRUE(lavik::InitMemoryLimit(kMaximum, 1).ok());
  lavik::BindMemoryAccountingShard(0);

  {
    lavik::ClientBufferReservation reservation;
    EXPECT_TRUE(reservation.TryAcquire(kClientCapacity));
    EXPECT_FALSE(reservation.TryAcquire(1));
    EXPECT_EQ(reservation.bytes(), kClientCapacity);
    EXPECT_EQ(lavik::GetMemoryStats().client_buffered_bytes_, kClientCapacity);

    reservation.Release(kClientCapacity - 1);
    EXPECT_TRUE(reservation.TryAcquire(kClientCapacity - 1));
  }

  EXPECT_EQ(lavik::GetMemoryStats().client_buffered_bytes_, 0);
  lavik::BindMemoryAccountingShard(lavik::kMaxMemoryWorkers);
}

TEST(MemoryTest, TinyLimitsRetainAProtocolRecoveryAllowance) {
  ASSERT_TRUE(lavik::InitMemoryLimit(1, 1).ok());
  lavik::BindMemoryAccountingShard(0);

  {
    lavik::ClientBufferReservation reservation;
    EXPECT_TRUE(reservation.TryAcquire(128 * 1024));
    EXPECT_FALSE(reservation.TryAcquire(1));
  }

  lavik::BindMemoryAccountingShard(lavik::kMaxMemoryWorkers);
}

TEST(MemoryTest, FullSyncCreditMovesBetweenReservationAndLiveAllocation) {
  constexpr std::uint64_t kMaximum = 1024ULL * 1024 * 1024;
  constexpr std::size_t kReserved = 4 * 1024 * 1024;
  constexpr std::size_t kConsumed = 1024 * 1024;
  ASSERT_TRUE(lavik::InitMemoryLimit(kMaximum, 1).ok());
  lavik::BindMemoryAccountingShard(0);
  const std::uint64_t before = lavik::GetMemoryStats().fullsync_reserved_bytes_;

  ASSERT_TRUE(lavik::TryReserveFullSyncMemory(kReserved));
  EXPECT_EQ(lavik::GetMemoryStats().fullsync_reserved_bytes_,
            before + kReserved);

  lavik::ConsumeFullSyncMemory(kConsumed);
  EXPECT_EQ(lavik::GetMemoryStats().fullsync_reserved_bytes_,
            before + kReserved - kConsumed);

  lavik::RestoreFullSyncMemory(kConsumed);
  EXPECT_EQ(lavik::GetMemoryStats().fullsync_reserved_bytes_,
            before + kReserved);

  lavik::ReleaseFullSyncMemory(kReserved);
  EXPECT_EQ(lavik::GetMemoryStats().fullsync_reserved_bytes_, before);
  lavik::BindMemoryAccountingShard(lavik::kMaxMemoryWorkers);
}

TEST(MemoryTest, ClientBufferLimitAcceptsPercentAbsoluteAndDisabledModes) {
  constexpr std::uint64_t kMaximum = 1024ULL * 1024 * 1024;
  lavik::BindMemoryAccountingShard(lavik::kMaxMemoryWorkers);

  ASSERT_TRUE(lavik::InitMemoryLimit(
                  kMaximum, 1,
                  lavik::ClientBufferLimit{.value_ = 1, .percentage_ = true})
                  .ok());
  lavik::BindMemoryAccountingShard(0);
  {
    lavik::ClientBufferReservation reservation;
    const std::size_t capacity = kMaximum / 100;
    EXPECT_TRUE(reservation.TryAcquire(capacity));
    EXPECT_FALSE(reservation.TryAcquire(1));
    EXPECT_EQ(lavik::GetMemoryStats().client_buffer_limit_bytes_, capacity);
  }

  lavik::BindMemoryAccountingShard(lavik::kMaxMemoryWorkers);
  ASSERT_TRUE(
      lavik::InitMemoryLimit(
          kMaximum, 1,
          lavik::ClientBufferLimit{.value_ = 256 * 1024, .percentage_ = false})
          .ok());
  lavik::BindMemoryAccountingShard(0);
  {
    lavik::ClientBufferReservation reservation;
    EXPECT_TRUE(reservation.TryAcquire(256 * 1024));
    EXPECT_FALSE(reservation.TryAcquire(1));
  }

  lavik::BindMemoryAccountingShard(lavik::kMaxMemoryWorkers);
  ASSERT_TRUE(lavik::InitMemoryLimit(
                  kMaximum, 1,
                  lavik::ClientBufferLimit{.value_ = 0, .percentage_ = false})
                  .ok());
  lavik::BindMemoryAccountingShard(0);
  {
    lavik::ClientBufferReservation reservation;
    EXPECT_TRUE(reservation.TryAcquire(kMaximum));
    EXPECT_EQ(lavik::GetMemoryStats().client_buffer_limit_bytes_, 0);
  }
  lavik::BindMemoryAccountingShard(lavik::kMaxMemoryWorkers);
}

TEST(MemoryTest, CrossWorkerRetainedReleaseReturnsBytesToExplicitOrigin) {
  ASSERT_TRUE(lavik::InitMemoryLimit(1024ULL * 1024 * 1024, 2).ok());

  std::mutex mutex;
  std::condition_variable ready;
  constexpr std::size_t kRetainedBytes = 128;
  bool allocated = false;
  bool released = false;
  std::int64_t owner_before = 0;
  std::int64_t owner_after_allocate = 0;
  std::int64_t owner_after_release = 0;
  std::int64_t releaser_before = 0;
  std::int64_t releaser_after = 0;

  std::thread owner([&] {
    lavik::BindMemoryAccountingShard(0);
    const unsigned owner_shard = lavik::CurrentMemoryAccountingShard();
    owner_before = lavik::WorkerMemoryAccountingBytes(0);
    lavik::AccountRetainedMemory(owner_shard, kRetainedBytes);
    {
      std::lock_guard lock(mutex);
      owner_after_allocate = lavik::WorkerMemoryAccountingBytes(0);
      allocated = true;
    }
    ready.notify_all();
    std::unique_lock lock(mutex);
    ready.wait(lock, [&] { return released; });
    owner_after_release = lavik::WorkerMemoryAccountingBytes(0);
  });

  std::thread releaser([&] {
    lavik::BindMemoryAccountingShard(1);
    {
      std::unique_lock lock(mutex);
      ready.wait(lock, [&] { return allocated; });
    }
    releaser_before = lavik::WorkerMemoryAccountingBytes(1);
    // The retained owner carries worker 0's slot; the freeing thread does not
    // infer ownership from its current worker or the allocation address.
    lavik::ReleaseRetainedMemory(/*owner_shard=*/1, kRetainedBytes);
    releaser_after = lavik::WorkerMemoryAccountingBytes(1);
    {
      std::lock_guard lock(mutex);
      released = true;
    }
    ready.notify_all();
  });

  owner.join();
  releaser.join();
  EXPECT_EQ(owner_after_allocate - owner_before,
            static_cast<std::int64_t>(kRetainedBytes));
  EXPECT_EQ(owner_after_release, owner_before);
  EXPECT_EQ(releaser_after, releaser_before);
}

TEST(MemoryTest, WorkerSnapshotsMatchAdmissionShares) {
  constexpr std::uint64_t kMaximum = 1024ULL * 1024 * 1024;
  ASSERT_TRUE(lavik::InitMemoryLimit(kMaximum, 2).ok());
  ASSERT_EQ(lavik::MemoryAccountingWorkerCount(), 2u);

  const lavik::WorkerMemoryStats before0 = lavik::GetWorkerMemoryStats(0);
  const lavik::WorkerMemoryStats before1 = lavik::GetWorkerMemoryStats(1);
  EXPECT_EQ(before0.retained_limit_bytes_ + before1.retained_limit_bytes_,
            kMaximum - kMaximum / 10);

  // Slot zero is distributed deterministically because it participates in
  // every worker's admission decision even though it has no worker owner.
  constexpr std::size_t kFallbackBytes = 4;
  constexpr std::size_t kOwnedBytes = 5;
  lavik::AccountRetainedMemory(/*owner_shard=*/0, kFallbackBytes);
  lavik::AccountRetainedMemory(/*owner_shard=*/1, kOwnedBytes);
  const lavik::WorkerMemoryStats after0 = lavik::GetWorkerMemoryStats(0);
  const lavik::WorkerMemoryStats after1 = lavik::GetWorkerMemoryStats(1);
  EXPECT_EQ(after0.retained_bytes_ - before0.retained_bytes_, 7u);
  EXPECT_EQ(after1.retained_bytes_ - before1.retained_bytes_, 2u);

  lavik::ReleaseRetainedMemory(/*owner_shard=*/1, kOwnedBytes);
  lavik::ReleaseRetainedMemory(/*owner_shard=*/0, kFallbackBytes);
  EXPECT_EQ(lavik::GetWorkerMemoryStats(0).retained_bytes_,
            before0.retained_bytes_);
  EXPECT_EQ(lavik::GetWorkerMemoryStats(1).retained_bytes_,
            before1.retained_bytes_);
  EXPECT_EQ(lavik::GetWorkerMemoryStats(2).retained_limit_bytes_, 0u);
}

TEST(MemoryTest, RetainedChargeTransfersOwnershipWithoutGlobalNewHooks) {
  ASSERT_TRUE(lavik::InitMemoryLimit(1024ULL * 1024 * 1024, 1).ok());
  lavik::BindMemoryAccountingShard(0);
  const std::int64_t before = lavik::WorkerMemoryAccountingBytes(0);

  {
    // Ordinary temporary C++ allocations are deliberately outside retained
    // accounting even though the final server executable routes them through
    // mimalloc's official global override.
    std::string temporary(1024 * 1024, 'x');
    EXPECT_EQ(lavik::WorkerMemoryAccountingBytes(0), before);

    auto reservation = lavik::TryReserveMemory(4096);
    ASSERT_TRUE(reservation.has_value());
    lavik::RetainedMemoryCharge charge;
    charge.Adopt(&*reservation, 4096);
    EXPECT_EQ(lavik::WorkerMemoryAccountingBytes(0), before + 4096);

    lavik::RetainedMemoryCharge moved(std::move(charge));
    EXPECT_EQ(charge.bytes(), 0);
    EXPECT_EQ(moved.bytes(), 4096);
  }

  EXPECT_EQ(lavik::WorkerMemoryAccountingBytes(0), before);
  lavik::BindMemoryAccountingShard(lavik::kMaxMemoryWorkers);
}

TEST(MemoryTest, SharedReservationIsDischargedBeforeCrossWorkerDestruction) {
  ASSERT_TRUE(lavik::InitMemoryLimit(1024ULL * 1024 * 1024, 2).ok());
  lavik::BindMemoryAccountingShard(0);
  const std::uint64_t before = lavik::GetMemoryStats().admission_pending_bytes_;

  auto reservation = lavik::TryReserveMemory(4096);
  ASSERT_TRUE(reservation.has_value());
  auto shared =
      std::make_shared<lavik::MemoryReservation>(std::move(*reservation));
  EXPECT_EQ(lavik::GetMemoryStats().admission_pending_bytes_, before + 4096);

  // Publisher release executes on the allocation owner. The public fan-out
  // envelope may then carry an inert shared_ptr back to another worker.
  shared->Release();
  EXPECT_EQ(lavik::GetMemoryStats().admission_pending_bytes_, before);
  std::thread coordinator([token = std::move(shared)]() mutable {
    lavik::BindMemoryAccountingShard(1);
    token.reset();
  });
  coordinator.join();

  EXPECT_EQ(lavik::GetMemoryStats().admission_pending_bytes_, before);
  lavik::BindMemoryAccountingShard(lavik::kMaxMemoryWorkers);
}

TEST(MemoryTest, RecoveryUsesFullShareAndMustFitSteadyBudgetBeforeServing) {
  constexpr std::uint64_t kMaximum = 1024ULL * 1024 * 1024;
  ASSERT_TRUE(lavik::InitMemoryLimit(kMaximum, 1).ok());
  lavik::BindMemoryAccountingShard(0);
  lavik::RefreshMemoryStats();
  const auto baseline = lavik::GetMemoryStats().used_bytes_;
  const auto steady = kMaximum - kMaximum / 10;
  ASSERT_LT(baseline, steady);
  EXPECT_FALSE(lavik::TryReserveMemory(steady - baseline + 1));
  {
    lavik::RecoveryMemoryBudget recovery;
    EXPECT_EQ(lavik::GetWorkerMemoryStats(0).retained_limit_bytes_, kMaximum);
    auto reservation = lavik::TryReserveMemory(kMaximum - baseline);
    ASSERT_TRUE(reservation);
    EXPECT_FALSE(lavik::WouldExceedMemoryLimit(0));
    EXPECT_FALSE(lavik::TryReserveMemory(1));
    // Pending construction credit counts too; a barrier must not promote an
    // in-flight allocation by switching to a smaller policy underneath it.
    EXPECT_TRUE(absl::IsResourceExhausted(recovery.Finish()));
    lavik::RetainedMemoryCharge charge;
    charge.Adopt(&*reservation, kMaximum - baseline);
    EXPECT_TRUE(absl::IsResourceExhausted(recovery.Finish()));
    charge.Reset();
    ASSERT_TRUE(recovery.Finish().ok());
    EXPECT_EQ(lavik::GetWorkerMemoryStats(0).retained_limit_bytes_, steady);
    EXPECT_FALSE(lavik::TryReserveMemory(steady - baseline + 1));
  }
  lavik::BindMemoryAccountingShard(lavik::kMaxMemoryWorkers);
}

TEST(MemoryTest, RecoveryScratchConsumesBudgetAndCreditsItsOriginAfterMove) {
  constexpr std::uint64_t kMaximum = 1024ULL * 1024 * 1024;
  ASSERT_TRUE(lavik::InitMemoryLimit(kMaximum, 2).ok());
  lavik::BindMemoryAccountingShard(0);
  const auto baseline = lavik::GetWorkerMemoryStats(0).retained_bytes_;
  {
    lavik::RecoveryMemoryBudget recovery;
    using Scratch =
        std::vector<std::byte, lavik::storage::RecoveryAllocator<std::byte>>;
    Scratch batch;
    batch.resize(1024 * 1024);
    const auto used = lavik::GetWorkerMemoryStats(0).retained_bytes_;
    EXPECT_GE(used - baseline, batch.capacity());
    auto remaining = lavik::TryReserveMemory(kMaximum / 2 - used);
    ASSERT_TRUE(remaining);
    EXPECT_FALSE(lavik::TryReserveMemory(1));
    remaining.reset();
    std::thread target([batch = std::move(batch)]() mutable {
      lavik::BindMemoryAccountingShard(1);
      // Move assignment propagates the origin's allocator as well as its
      // storage. Destruction on this worker must not debit worker 1.
      Scratch received;
      received = std::move(batch);
    });
    target.join();
    EXPECT_EQ(lavik::GetWorkerMemoryStats(0).retained_bytes_, baseline);
    // Global usage still fits 90%, but worker 0 cannot borrow worker 1's
    // online headroom. Check the per-worker transition independently.
    auto full_share = lavik::TryReserveMemory(kMaximum / 2 - baseline);
    ASSERT_TRUE(full_share);
    auto transition = recovery.Finish();
    EXPECT_TRUE(absl::IsResourceExhausted(transition));
    EXPECT_NE(transition.message().find("worker=0"), std::string::npos);
    full_share.reset();
    // Abandon startup: the owner's destructor must restore the normal limit.
  }
  EXPECT_EQ(lavik::GetWorkerMemoryStats(0).retained_limit_bytes_,
            (kMaximum - kMaximum / 10) / 2);
  lavik::BindMemoryAccountingShard(lavik::kMaxMemoryWorkers);
}
