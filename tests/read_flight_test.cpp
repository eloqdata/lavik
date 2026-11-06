/*
 * Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0
 */
#include "lavik/storage/read_flight.h"

#include <sys/eventfd.h>
#include <unistd.h>

#include <array>
#include <functional>
#include <limits>

#include "absl/cleanup/cleanup.h"
#include "bycorf/runtime/worker.h"
#include "gtest/gtest.h"

namespace lavik::storage {
namespace {
struct Waiter : ReadFlightWaiter {
  ReadResultBuffer buffer;
  int result = 0;
  unsigned calls = 0;
  std::function<void()> on_receive;
  void ReceiveRead(bycorf::Worker&, ReadResultBuffer value, int bytes,
                   unsigned) override {
    buffer = std::move(value);
    result = bytes;
    ++calls;
    if (on_receive) on_receive();
  }
};
struct Pins {
  unsigned normal = 0, quiesced = 0;
  static void Release(void* context, std::uint64_t, unsigned,
                      bool quiet) noexcept {
    auto& self = *static_cast<Pins*>(context);
    if (quiet)
      ++self.quiesced;
    else
      ++self.normal;
  }
};
class ReadFlightTest : public testing::Test {
 protected:
  void SetUp() override {
    ASSERT_TRUE(InitMemoryLimit(1024 * kMiB, 2).ok());
    saved_ = std::move(bycorf::MutableThisWorker());
    wake_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    ASSERT_GE(wake_, 0);
    cross_.mailbox(0).wake_fd_ = wake_;
    worker_.BindCrossCore(0, &cross_);
    bycorf::SetThisWorker(0, &cross_, &worker_);
    ASSERT_TRUE(worker_.Init({.recv_buffer_count_ = 0}).ok());
    RegisteredBufferPoolOptions options;
    options.storage_write_buffer_count_ = 2;
    options.registered_bytes_ = 20 * kMiB;
    ASSERT_TRUE(pool_.Init(worker_, options).ok());
  }
  void TearDown() override {
    worker_.Shutdown();
    pool_.FinalizeReadSharing();
    close(wake_);
    bycorf::MutableThisWorker() = std::move(saved_);
  }
  auto Registry(std::size_t capacity = 2) {
    return ReadFlightRegistry::TryCreate(worker_, pool_, &pins_, Pins::Release,
                                         capacity);
  }
  ReadBufferLease Buffer() {
    auto acquired = pool_.TryAcquireReadBuffer(4096);
    EXPECT_TRUE(acquired.ok());
    return acquired.ok() ? std::move(*acquired) : ReadBufferLease{};
  }
  bycorf::CurrentWorker saved_;
  bycorf::CrossCore cross_{2};
  bycorf::Worker worker_;
  RegisteredBufferPool pool_;
  Pins pins_;
  int wake_ = -1;
};

TEST_F(ReadFlightTest, HashCollisionAndEveryIdentityFenceStayIndependent) {
  const ReadFlightKey key{.engine = 1,
                          .block = 2,
                          .allocation_epoch = 3,
                          .mutation_sequence = 4,
                          .aligned_offset = 4096,
                          .read_bytes = 4096,
                          .db_epoch = 5,
                          .replication_epoch = 6,
                          .population_generation = 8,
                          .file = 1,
                          .record_offset = 12,
                          .record_bytes = 128,
                          .block_owner = 1,
                          .db = 1};
  for (unsigned field = 0; field != 14; ++field) {
    auto registry = Registry();
    ASSERT_TRUE(registry);
    auto other = key;
    switch (field) {
      case 0:
        ++other.engine;
        break;
      case 1:
        ++other.block;
        break;
      case 2:
        ++other.allocation_epoch;
        break;
      case 3:
        ++other.mutation_sequence;
        break;
      case 4:
        ++other.aligned_offset;
        break;
      case 5:
        ++other.read_bytes;
        break;
      case 6:
        ++other.db_epoch;
        break;
      case 7:
        ++other.replication_epoch;
        break;
      case 8:
        ++other.population_generation;
        break;
      case 9:
        ++other.file;
        break;
      case 10:
        ++other.record_offset;
        break;
      case 11:
        ++other.record_bytes;
        break;
      case 12:
        ++other.block_owner;
        break;
      case 13:
        ++other.db;
        break;
    }
    Waiter first, second;
    auto a = Buffer(), b = Buffer();
    bool pin_a = true, pin_b = true;
    auto leader = registry->Start(key, 9, a, pin_a, first);
    ASSERT_EQ(leader.outcome, ReadFlightRegistry::Outcome::kStarted);
    EXPECT_FALSE(registry->TryJoin(other, 9, second));
    auto separate = registry->Start(other, 9, b, pin_b, second);
    ASSERT_EQ(separate.outcome, ReadFlightRegistry::Outcome::kStarted);
    EXPECT_NE(leader.tag, separate.tag);
    leader.tag->Complete(worker_, 4096, 0);
    separate.tag->Complete(worker_, 4096, 0);
    EXPECT_EQ(first.calls, 1);
    EXPECT_EQ(second.calls, 1);
    EXPECT_FALSE(first.buffer.shared());
    EXPECT_FALSE(second.buffer.shared());
  }
}

TEST_F(ReadFlightTest, BoundedWaitersAndFullTableBypassWithoutTakingOwnership) {
  auto registry = Registry(1);
  ASSERT_TRUE(registry);
  std::array<Waiter, 33> waiters;
  auto buffer = Buffer();
  bool pin = true;
  const ReadFlightKey key{.block = 1};
  auto producer = registry->Start(key, 42, buffer, pin, waiters[0]);
  ASSERT_NE(producer.tag, nullptr);
  for (unsigned i = 1; i != 32; ++i)
    ASSERT_TRUE(registry->TryJoin(key, 42, waiters[i]));
  EXPECT_FALSE(registry->TryJoin(key, 42, waiters[32]));
  auto bypass_buffer = Buffer();
  bool bypass_pin = true;
  EXPECT_EQ(registry
                ->Start(ReadFlightKey{.block = 2}, 42, bypass_buffer,
                        bypass_pin, waiters[32])
                .outcome,
            ReadFlightRegistry::Outcome::kBypass);
  EXPECT_TRUE(bypass_pin);
  EXPECT_TRUE(bypass_buffer.valid());
  producer.tag->Complete(worker_, 4096, 0);
  for (unsigned i = 0; i != 32; ++i) {
    EXPECT_EQ(waiters[i].calls, 1);
    EXPECT_TRUE(waiters[i].buffer.shared());
    EXPECT_EQ(waiters[i].buffer.bytes().data(),
              waiters[0].buffer.bytes().data());
  }
  EXPECT_EQ(waiters[32].calls, 0);
  EXPECT_EQ(pins_.normal, 1);
}

TEST_F(ReadFlightTest,
       SharedRetentionBudgetRejectsFollowerWithoutAffectingProducer) {
  auto registry = Registry();
  ASSERT_TRUE(registry);
  auto quota = pool_.TryReserveSharedRead(pool_.read_capacity_bytes() / 4);
  ASSERT_TRUE(quota.valid());
  Waiter first, second;
  auto buffer = Buffer();
  bool pin = true;
  auto producer = registry->Start({}, 42, buffer, pin, first);
  ASSERT_NE(producer.tag, nullptr);
  EXPECT_FALSE(registry->TryJoin({}, 42, second));
  EXPECT_EQ(registry->stats().shared_budget, 1);
  producer.tag->Complete(worker_, 4096, 0);
  EXPECT_EQ(first.calls, 1);
  EXPECT_FALSE(first.buffer.shared());
  EXPECT_EQ(second.calls, 0);
}

TEST_F(ReadFlightTest, CompleteRetiresBeforeReentryAndChurnAllocatesNothing) {
  auto registry = Registry(1);
  ASSERT_TRUE(registry);
  const auto allocations = registry->map_allocations();
  bycorf::IoCompletion* previous = nullptr;
  for (unsigned i = 0; i != 10000; ++i) {
    Waiter first, nested;
    auto buffer = Buffer();
    bool pin = true;
    auto producer = registry->Start({}, 42, buffer, pin, first);
    ASSERT_NE(producer.tag, nullptr);
    if (previous) EXPECT_EQ(producer.tag, previous);
    previous = producer.tag;
    first.on_receive = [&] {
      EXPECT_EQ(registry->size(), 0);
      auto nested_buffer = Buffer();
      bool nested_pin = true;
      auto next = registry->Start({}, 42, nested_buffer, nested_pin, nested);
      ASSERT_NE(next.tag, nullptr);
      EXPECT_EQ(next.tag, producer.tag);
      next.tag->Complete(worker_, -EIO, 0);
    };
    producer.tag->Complete(worker_, 4096, 0);
    EXPECT_EQ(nested.calls, 1);
    EXPECT_EQ(nested.result, -EIO);
  }
  EXPECT_EQ(registry->map_allocations(), allocations);
  EXPECT_EQ(registry->size(), 0);
}

TEST_F(ReadFlightTest, SameIdentityOnDifferentWorkersHasIndependentProducer) {
  auto registry = Registry();
  ASSERT_TRUE(registry);
  Waiter first;
  auto buffer = Buffer();
  bool pin = true;
  auto producer = registry->Start({}, 42, buffer, pin, first);
  ASSERT_NE(producer.tag, nullptr);
  {
    bycorf::Worker second_worker;
    second_worker.BindCrossCore(1, &cross_);
    bycorf::SetThisWorker(1, &cross_, &second_worker);
    absl::Cleanup restore = [&] {
      bycorf::SetThisWorker(0, &cross_, &worker_);
    };
    RegisteredBufferPool second_pool;
    absl::Cleanup shutdown = [&] { second_worker.Shutdown(); };
    ASSERT_TRUE(second_worker.Init({.recv_buffer_count_ = 0}).ok());
    RegisteredBufferPoolOptions options;
    options.storage_write_buffer_count_ = 2;
    options.registered_bytes_ = 20 * kMiB;
    ASSERT_TRUE(second_pool.Init(second_worker, options).ok());
    auto second_registry = ReadFlightRegistry::TryCreate(
        second_worker, second_pool, &pins_, Pins::Release, 2);
    ASSERT_TRUE(second_registry);
    Waiter second;
    EXPECT_FALSE(second_registry->TryJoin({}, 42, second));
    auto acquired = second_pool.TryAcquireReadBuffer(4096);
    ASSERT_TRUE(acquired.ok());
    bool second_pin = true;
    auto separate =
        second_registry->Start({}, 42, *acquired, second_pin, second);
    ASSERT_NE(separate.tag, nullptr);
    EXPECT_NE(separate.tag, producer.tag);
    separate.tag->Complete(second_worker, 4096, 0);
    bycorf::SetThisWorker(0, &cross_, &worker_);
    producer.tag->Complete(worker_, 4096, 0);
    EXPECT_NE(first.buffer.bytes().data(), second.buffer.bytes().data());
    EXPECT_FALSE(first.buffer.shared());
    EXPECT_FALSE(second.buffer.shared());
    EXPECT_EQ(registry->stats().leaders, 1);
    EXPECT_EQ(second_registry->stats().leaders, 1);
    bycorf::SetThisWorker(1, &cross_, &second_worker);
  }
  EXPECT_EQ(pins_.normal, 2);
}

TEST_F(ReadFlightTest, CancellationUnlinksMiddleHeadAndProducerIndependently) {
  auto registry = Registry();
  ASSERT_TRUE(registry);
  std::array<std::unique_ptr<Waiter>, 6> waiters;
  for (auto& waiter : waiters) waiter = std::make_unique<Waiter>();
  auto buffer = Buffer();
  bool pin = true;
  auto producer = registry->Start({}, 42, buffer, pin, *waiters[0]);
  ASSERT_NE(producer.tag, nullptr);
  for (unsigned i = 1; i < waiters.size(); ++i)
    ASSERT_TRUE(registry->TryJoin({}, 42, *waiters[i]));
  waiters[3].reset();
  waiters[5].reset();
  waiters[0].reset();
  producer.tag->Complete(worker_, 4096, 0);
  for (const auto& waiter : waiters) {
    if (!waiter) continue;
    EXPECT_EQ(waiter->calls, 1);
    EXPECT_TRUE(waiter->buffer.shared());
  }
  EXPECT_EQ(registry->stats().cancellations, 3);
  EXPECT_EQ(pins_.normal, 1);
}

TEST_F(ReadFlightTest, InsertionCounterWrapCannotReviveAnOldMiss) {
  auto registry = Registry();
  ASSERT_TRUE(registry);
  Waiter delayed, leader;
  EXPECT_FALSE(registry->TryJoin({}, 42, delayed));
  // Simulate the theoretical lifetime wrap without changing insertion logic.
  const_cast<ReadFlightStats&>(registry->stats()).leaders =
      std::numeric_limits<std::uint64_t>::max();
  auto buffer = Buffer();
  bool pin = true;
  auto producer = registry->Start({}, 42, buffer, pin, leader);
  ASSERT_NE(producer.tag, nullptr);
  EXPECT_EQ(registry->stats().leaders, 0);
  auto redundant = Buffer();
  bool redundant_pin = true;
  auto joined = registry->Start({}, 42, redundant, redundant_pin, delayed);
  ASSERT_EQ(joined.outcome, ReadFlightRegistry::Outcome::kJoined);
  redundant.Reset();
  producer.tag->Complete(worker_, 4096, 0);
  EXPECT_EQ(leader.calls, 1);
  EXPECT_EQ(delayed.calls, 1);
}

TEST_F(ReadFlightTest, OptionalMetadataAdmissionFailureBypasses) {
  ASSERT_TRUE(InitMemoryLimit(1, 1).ok());
  EXPECT_FALSE(Registry());
  ASSERT_TRUE(InitMemoryLimit(1024 * kMiB, 2).ok());
  EXPECT_TRUE(Registry());
}
}  // namespace
}  // namespace lavik::storage
