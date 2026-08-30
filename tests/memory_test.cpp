#include "keylane/memory.h"

#include <gtest/gtest.h>

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

TEST(MemoryTest, RetainedAdmissionLeavesTenPercentOutsideRetainedState) {
  constexpr std::uint64_t kInitialLimit = 1024ULL * 1024 * 1024;
  constexpr std::uint64_t kSteadyGrowth = 16ULL * 1024 * 1024;
  ASSERT_TRUE(keylane::InitMemoryLimit(kInitialLimit, 1).ok());
  keylane::BindMemoryAccountingShard(0);
  keylane::RefreshMemoryStats();
  const std::uint64_t used = keylane::GetMemoryStats().used_bytes_;
  const std::uint64_t target_steady = used + kSteadyGrowth;

  // Solve max - floor(max / 10) >= target_steady without exposing the policy
  // calculation as mutable runtime state.
  std::uint64_t maximum = (target_steady * 10 + 8) / 9;
  while (maximum - maximum / 10 < target_steady) ++maximum;
  ASSERT_TRUE(keylane::InitMemoryLimit(maximum, 1).ok());
  const std::uint64_t steady = maximum - maximum / 10;
  const std::size_t steady_remaining = static_cast<std::size_t>(steady - used);

  EXPECT_FALSE(keylane::WouldExceedMemoryLimit(steady_remaining));
  EXPECT_TRUE(keylane::WouldExceedMemoryLimit(steady_remaining + 1));
  auto retained = keylane::TryReserveMemory(steady_remaining);
  ASSERT_TRUE(retained.has_value());
  EXPECT_TRUE(keylane::WouldExceedMemoryLimit(1));
  EXPECT_FALSE(keylane::TryReserveFullSyncMemory(1));

  // Client buffers have an independent abuse-control quota. At its default 5%,
  // a full client quota still leaves 5% outside both admitted classes.
  {
    keylane::ClientBufferReservation client;
    EXPECT_TRUE(client.TryAcquire(1));
  }

  retained.reset();
  keylane::BindMemoryAccountingShard(keylane::kMaxMemoryWorkers);
}

TEST(MemoryTest, ClientBuffersHaveAWorkerLocalFivePercentLimit) {
  constexpr std::uint64_t kMaximum = 1024ULL * 1024 * 1024;
  constexpr std::size_t kClientCapacity = kMaximum / 20;
  ASSERT_TRUE(keylane::InitMemoryLimit(kMaximum, 1).ok());
  keylane::BindMemoryAccountingShard(0);

  {
    keylane::ClientBufferReservation reservation;
    EXPECT_TRUE(reservation.TryAcquire(kClientCapacity));
    EXPECT_FALSE(reservation.TryAcquire(1));
    EXPECT_EQ(reservation.bytes(), kClientCapacity);
    EXPECT_EQ(keylane::GetMemoryStats().client_buffered_bytes_,
              kClientCapacity);

    reservation.Release(kClientCapacity - 1);
    EXPECT_TRUE(reservation.TryAcquire(kClientCapacity - 1));
  }

  EXPECT_EQ(keylane::GetMemoryStats().client_buffered_bytes_, 0);
  keylane::BindMemoryAccountingShard(keylane::kMaxMemoryWorkers);
}

TEST(MemoryTest, TinyLimitsRetainAProtocolRecoveryAllowance) {
  ASSERT_TRUE(keylane::InitMemoryLimit(1, 1).ok());
  keylane::BindMemoryAccountingShard(0);

  {
    keylane::ClientBufferReservation reservation;
    EXPECT_TRUE(reservation.TryAcquire(128 * 1024));
    EXPECT_FALSE(reservation.TryAcquire(1));
  }

  keylane::BindMemoryAccountingShard(keylane::kMaxMemoryWorkers);
}

TEST(MemoryTest, FullSyncCreditMovesBetweenReservationAndLiveAllocation) {
  constexpr std::uint64_t kMaximum = 1024ULL * 1024 * 1024;
  constexpr std::size_t kReserved = 4 * 1024 * 1024;
  constexpr std::size_t kConsumed = 1024 * 1024;
  ASSERT_TRUE(keylane::InitMemoryLimit(kMaximum, 1).ok());
  keylane::BindMemoryAccountingShard(0);
  const std::uint64_t before =
      keylane::GetMemoryStats().fullsync_reserved_bytes_;

  ASSERT_TRUE(keylane::TryReserveFullSyncMemory(kReserved));
  EXPECT_EQ(keylane::GetMemoryStats().fullsync_reserved_bytes_,
            before + kReserved);

  keylane::ConsumeFullSyncMemory(kConsumed);
  EXPECT_EQ(keylane::GetMemoryStats().fullsync_reserved_bytes_,
            before + kReserved - kConsumed);

  keylane::RestoreFullSyncMemory(kConsumed);
  EXPECT_EQ(keylane::GetMemoryStats().fullsync_reserved_bytes_,
            before + kReserved);

  keylane::ReleaseFullSyncMemory(kReserved);
  EXPECT_EQ(keylane::GetMemoryStats().fullsync_reserved_bytes_, before);
  keylane::BindMemoryAccountingShard(keylane::kMaxMemoryWorkers);
}

TEST(MemoryTest, ClientBufferLimitAcceptsPercentAbsoluteAndDisabledModes) {
  constexpr std::uint64_t kMaximum = 1024ULL * 1024 * 1024;
  keylane::BindMemoryAccountingShard(keylane::kMaxMemoryWorkers);

  ASSERT_TRUE(keylane::InitMemoryLimit(
                  kMaximum, 1,
                  keylane::ClientBufferLimit{.value_ = 1, .percentage_ = true})
                  .ok());
  keylane::BindMemoryAccountingShard(0);
  {
    keylane::ClientBufferReservation reservation;
    const std::size_t capacity = kMaximum / 100;
    EXPECT_TRUE(reservation.TryAcquire(capacity));
    EXPECT_FALSE(reservation.TryAcquire(1));
    EXPECT_EQ(keylane::GetMemoryStats().client_buffer_limit_bytes_, capacity);
  }

  keylane::BindMemoryAccountingShard(keylane::kMaxMemoryWorkers);
  ASSERT_TRUE(
      keylane::InitMemoryLimit(kMaximum, 1,
                               keylane::ClientBufferLimit{.value_ = 256 * 1024,
                                                          .percentage_ = false})
          .ok());
  keylane::BindMemoryAccountingShard(0);
  {
    keylane::ClientBufferReservation reservation;
    EXPECT_TRUE(reservation.TryAcquire(256 * 1024));
    EXPECT_FALSE(reservation.TryAcquire(1));
  }

  keylane::BindMemoryAccountingShard(keylane::kMaxMemoryWorkers);
  ASSERT_TRUE(keylane::InitMemoryLimit(
                  kMaximum, 1,
                  keylane::ClientBufferLimit{.value_ = 0, .percentage_ = false})
                  .ok());
  keylane::BindMemoryAccountingShard(0);
  {
    keylane::ClientBufferReservation reservation;
    EXPECT_TRUE(reservation.TryAcquire(kMaximum));
    EXPECT_EQ(keylane::GetMemoryStats().client_buffer_limit_bytes_, 0);
  }
  keylane::BindMemoryAccountingShard(keylane::kMaxMemoryWorkers);
}

TEST(MemoryTest, CrossWorkerRetainedReleaseReturnsBytesToExplicitOrigin) {
  ASSERT_TRUE(keylane::InitMemoryLimit(1024ULL * 1024 * 1024, 2).ok());

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
    keylane::BindMemoryAccountingShard(0);
    const unsigned owner_shard = keylane::CurrentMemoryAccountingShard();
    owner_before = keylane::WorkerMemoryAccountingBytes(0);
    keylane::AccountRetainedMemory(owner_shard, kRetainedBytes);
    {
      std::lock_guard lock(mutex);
      owner_after_allocate = keylane::WorkerMemoryAccountingBytes(0);
      allocated = true;
    }
    ready.notify_all();
    std::unique_lock lock(mutex);
    ready.wait(lock, [&] { return released; });
    owner_after_release = keylane::WorkerMemoryAccountingBytes(0);
  });

  std::thread releaser([&] {
    keylane::BindMemoryAccountingShard(1);
    {
      std::unique_lock lock(mutex);
      ready.wait(lock, [&] { return allocated; });
    }
    releaser_before = keylane::WorkerMemoryAccountingBytes(1);
    // The retained owner carries worker 0's slot; the freeing thread does not
    // infer ownership from its current worker or the allocation address.
    keylane::ReleaseRetainedMemory(/*owner_shard=*/1, kRetainedBytes);
    releaser_after = keylane::WorkerMemoryAccountingBytes(1);
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

TEST(MemoryTest, RetainedChargeTransfersOwnershipWithoutGlobalNewHooks) {
  ASSERT_TRUE(keylane::InitMemoryLimit(1024ULL * 1024 * 1024, 1).ok());
  keylane::BindMemoryAccountingShard(0);
  const std::int64_t before = keylane::WorkerMemoryAccountingBytes(0);

  {
    // Ordinary temporary C++ allocations are deliberately outside retained
    // accounting even though the final server executable routes them through
    // mimalloc's official global override.
    std::string temporary(1024 * 1024, 'x');
    EXPECT_EQ(keylane::WorkerMemoryAccountingBytes(0), before);

    auto reservation = keylane::TryReserveMemory(4096);
    ASSERT_TRUE(reservation.has_value());
    keylane::RetainedMemoryCharge charge;
    charge.Adopt(&*reservation, 4096);
    EXPECT_EQ(keylane::WorkerMemoryAccountingBytes(0), before + 4096);

    keylane::RetainedMemoryCharge moved(std::move(charge));
    EXPECT_EQ(charge.bytes(), 0);
    EXPECT_EQ(moved.bytes(), 4096);
  }

  EXPECT_EQ(keylane::WorkerMemoryAccountingBytes(0), before);
  keylane::BindMemoryAccountingShard(keylane::kMaxMemoryWorkers);
}

TEST(MemoryTest, SharedReservationIsDischargedBeforeCrossWorkerDestruction) {
  ASSERT_TRUE(keylane::InitMemoryLimit(1024ULL * 1024 * 1024, 2).ok());
  keylane::BindMemoryAccountingShard(0);
  const std::uint64_t before =
      keylane::GetMemoryStats().admission_pending_bytes_;

  auto reservation = keylane::TryReserveMemory(4096);
  ASSERT_TRUE(reservation.has_value());
  auto shared =
      std::make_shared<keylane::MemoryReservation>(std::move(*reservation));
  EXPECT_EQ(keylane::GetMemoryStats().admission_pending_bytes_, before + 4096);

  // Publisher release executes on the allocation owner. The public fan-out
  // envelope may then carry an inert shared_ptr back to another worker.
  shared->Release();
  EXPECT_EQ(keylane::GetMemoryStats().admission_pending_bytes_, before);
  std::thread coordinator([token = std::move(shared)]() mutable {
    keylane::BindMemoryAccountingShard(1);
    token.reset();
  });
  coordinator.join();

  EXPECT_EQ(keylane::GetMemoryStats().admission_pending_bytes_, before);
  keylane::BindMemoryAccountingShard(keylane::kMaxMemoryWorkers);
}
