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

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <coroutine>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "bycorf/runtime/worker.h"
#include "lavik/tx/intent_lock.h"
#include "lavik/tx/tx_queue.h"
#include "lavik/tx/tx_shard.h"

namespace {

using lavik::tx::KeyRef;
using lavik::tx::LockFp;
using lavik::tx::LockMode;
using lavik::tx::LockTable;
using lavik::tx::TxQueue;
using lavik::tx::TxShard;
using lavik::tx::TxWaiter;

#define EXPECT_CHECK(condition, message) EXPECT_TRUE(condition) << message

// Minimal eagerly-started coroutine; the frame self-destroys at completion.
struct TestTask {
  struct promise_type {
    TestTask get_return_object() { return {}; }
    std::suspend_never initial_suspend() { return {}; }
    std::suspend_never final_suspend() noexcept { return {}; }
    void return_void() {}
    void unhandled_exception() { std::terminate(); }
  };
};

// Awaitable the test fires manually — stands in for a disk I/O suspension.
struct ManualEvent {
  std::coroutine_handle<> handle_;
  bool ready_ = false;

  auto operator co_await() {
    struct Awaiter {
      ManualEvent* event_;
      bool await_ready() const { return event_->ready_; }
      void await_suspend(std::coroutine_handle<> h) { event_->handle_ = h; }
      void await_resume() const {}
    };
    return Awaiter{this};
  }

  void Fire() {
    ready_ = true;
    if (handle_) {
      auto h = handle_;
      handle_ = {};
      h.resume();
    }
  }
};

TEST(TxLockTest, IntentAndHoldCompatibility) {
  LockTable table;
  const LockFp fp = 42;

  // Shared/shared grants; exclusive conflicts both ways.
  EXPECT_CHECK(table.AcquireIntent(fp, LockMode::kShared), "S grant on free");
  EXPECT_CHECK(table.AcquireIntent(fp, LockMode::kShared), "S/S grant");
  EXPECT_CHECK(!table.AcquireIntent(fp, LockMode::kExclusive),
               "X blocked by S");
  table.ReleaseIntent(fp, LockMode::kExclusive);
  table.ReleaseIntent(fp, LockMode::kShared);
  table.ReleaseIntent(fp, LockMode::kShared);
  EXPECT_CHECK(table.size() == 0, "entry erased when all counters zero");

  EXPECT_CHECK(table.AcquireIntent(fp, LockMode::kExclusive),
               "X grant on free");
  EXPECT_CHECK(!table.AcquireIntent(fp, LockMode::kShared), "S blocked by X");
  EXPECT_CHECK(!table.AcquireIntent(fp, LockMode::kExclusive),
               "X blocked by X");
  table.ReleaseIntent(fp, LockMode::kExclusive);
  table.ReleaseIntent(fp, LockMode::kShared);
  table.ReleaseIntent(fp, LockMode::kExclusive);
  EXPECT_CHECK(table.size() == 0, "entry erased again");

  // Holds: a sleeping shared holder blocks an exclusive hold but not the
  // intent bookkeeping.
  EXPECT_CHECK(table.AcquireIntent(fp, LockMode::kShared), "runner intent");
  table.AcquireHold(fp, LockMode::kShared);
  EXPECT_CHECK(table.CanHold(fp, LockMode::kShared),
               "S hold compatible with S hold");
  EXPECT_CHECK(!table.CanHold(fp, LockMode::kExclusive),
               "X hold blocked by S hold");
  table.ReleaseHold(fp, LockMode::kShared);
  EXPECT_CHECK(table.CanHold(fp, LockMode::kExclusive),
               "X hold free after drain");
  table.ReleaseIntent(fp, LockMode::kShared);
  EXPECT_CHECK(table.size() == 0, "held entry erased after full release");
}

TEST(TxLockTest, AllModesHonorCompatibilityOnFastAndQueuedPaths) {
  constexpr std::array modes{LockMode::kShared, LockMode::kExclusive,
                             LockMode::kIntentExclusive,
                             LockMode::kIntentShared};
  constexpr bool compatible[4][4] = {{true, false, false, true},
                                     {false, false, false, false},
                                     {false, false, true, true},
                                     {true, false, true, true}};
  for (std::size_t first = 0; first < modes.size(); ++first) {
    for (std::size_t second = 0; second < modes.size(); ++second) {
      for (bool fast : {false, true}) {
        SCOPED_TRACE(::testing::Message()
                     << first << "," << second << ", fast=" << fast);
        LockTable table;
        constexpr LockFp fp = 42;
        ASSERT_TRUE(table.AcquireIntentAndHoldIfGranted(fp, modes[first]));
        const bool granted =
            fast ? table.AcquireIntentAndHoldIfGranted(fp, modes[second])
                 : table.AcquireIntent(fp, modes[second]);
        EXPECT_EQ(granted, compatible[first][second]);
        EXPECT_EQ(table.CanHold(fp, modes[second]), compatible[first][second]);
        if (granted) {
          if (!fast) table.AcquireHold(fp, modes[second]);
          table.ReleaseHoldAndIntent(fp, modes[second]);
        } else {
          // Once the predecessor drains, the queued mode must be granted
          // based on holds, not blocked by its own recorded intent.
          table.ReleaseHoldAndIntent(fp, modes[first]);
          ASSERT_TRUE(table.CanHold(fp, modes[second]));
          table.AcquireHold(fp, modes[second]);
          table.ReleaseHold(fp, modes[second]);
          table.ReleaseIntent(fp, modes[second]);
          EXPECT_EQ(table.size(), 0);
          continue;
        }
        table.ReleaseHoldAndIntent(fp, modes[first]);
        EXPECT_EQ(table.size(), 0);
      }
    }
  }
}

TEST(TxLockTest, DuplicateModesPreserveAllAccessRequirements) {
  constexpr std::array modes{LockMode::kShared, LockMode::kExclusive,
                             LockMode::kIntentExclusive,
                             LockMode::kIntentShared};
  constexpr LockMode expected[4][4] = {
      {LockMode::kShared, LockMode::kExclusive, LockMode::kExclusive,
       LockMode::kShared},
      {LockMode::kExclusive, LockMode::kExclusive, LockMode::kExclusive,
       LockMode::kExclusive},
      {LockMode::kExclusive, LockMode::kExclusive, LockMode::kIntentExclusive,
       LockMode::kIntentExclusive},
      {LockMode::kShared, LockMode::kExclusive, LockMode::kIntentExclusive,
       LockMode::kIntentShared}};
  for (std::size_t first = 0; first < modes.size(); ++first) {
    for (std::size_t second = 0; second < modes.size(); ++second) {
      EXPECT_EQ(lavik::tx::MergeLockModes(modes[first], modes[second]),
                expected[first][second]);
      for (const auto third : modes) {
        // Deduplication must not depend on argument order or grouping.
        EXPECT_EQ(
            lavik::tx::MergeLockModes(expected[first][second], third),
            lavik::tx::MergeLockModes(
                modes[first], lavik::tx::MergeLockModes(modes[second], third)));
      }
    }
  }
}

TEST(TxLockTest, IntentSharedHoldsSurviveTheLastSharedOrPartialWriter) {
  for (const auto other : {LockMode::kShared, LockMode::kIntentExclusive}) {
    for (bool release_is_first : {false, true}) {
      SCOPED_TRACE(::testing::Message()
                   << static_cast<int>(other)
                   << ", release IS first=" << release_is_first);
      LockTable table;
      ASSERT_TRUE(
          table.AcquireIntentAndHoldIfGranted(1, LockMode::kIntentShared));
      ASSERT_TRUE(table.AcquireIntentAndHoldIfGranted(1, other));
      ASSERT_TRUE(
          table.AcquireIntentAndHoldIfGranted(1, LockMode::kIntentShared));
      const auto opposite = other == LockMode::kShared
                                ? LockMode::kIntentExclusive
                                : LockMode::kShared;
      EXPECT_FALSE(table.CanHold(1, opposite));
      table.ReleaseHoldAndIntent(1, LockMode::kIntentShared);
      EXPECT_FALSE(table.CanHold(1, LockMode::kExclusive));
      const auto first = release_is_first ? LockMode::kIntentShared : other;
      const auto last = release_is_first ? other : LockMode::kIntentShared;
      table.ReleaseHoldAndIntent(1, first);
      EXPECT_FALSE(table.CanHold(1, LockMode::kExclusive));
      EXPECT_EQ(table.size(), 1);
      EXPECT_TRUE(table.CanHold(1, last));
      if (!release_is_first) {
        EXPECT_TRUE(table.CanHold(1, LockMode::kShared));
        EXPECT_TRUE(table.CanHold(1, LockMode::kIntentExclusive));
        ASSERT_TRUE(table.AcquireIntentAndHoldIfGranted(1, opposite));
        EXPECT_FALSE(table.CanHold(1, other));
        table.ReleaseHoldAndIntent(1, opposite);
      }
      table.ReleaseHoldAndIntent(1, last);
      EXPECT_TRUE(table.CanHold(1, LockMode::kExclusive));
      EXPECT_EQ(table.size(), 0);
    }
  }
}

TEST(TxLockTest, QueueOrdersByTransactionId) {
  TxQueue queue;
  TxWaiter a{.txid_ = 5};
  TxWaiter b{.txid_ = 3};
  TxWaiter c{.txid_ = 7};
  queue.Insert(&a);
  queue.Insert(&b);
  queue.Insert(&c);
  EXPECT_CHECK(queue.TailTxid() == 7, "tail is max txid");
  EXPECT_CHECK(queue.Front() == &b, "front is min txid");
  queue.Remove(&a);  // mid-queue tombstone
  queue.PopFront();  // b
  EXPECT_CHECK(queue.Front() == &c, "tombstone skipped");
  queue.PopFront();
  EXPECT_CHECK(queue.Empty(), "queue drained");
}

TestTask RunWithIo(TxShard& shard, LockFp fp, LockMode mode, ManualEvent& io,
                   std::vector<std::string>& order, const char* name) {
  auto guard = co_await shard.AcquireKey(0, fp, mode);
  order.push_back(std::string(name) + ":start");
  co_await io;  // "disk read" while holding the key
  order.push_back(std::string(name) + ":done");
}

TestTask RunImmediate(TxShard& shard, LockFp fp, LockMode mode,
                      std::vector<std::string>& order, const char* name) {
  auto guard = co_await shard.AcquireKey(0, fp, mode);
  order.push_back(std::string(name) + ":start");
  order.push_back(std::string(name) + ":done");
}

TestTask RunKeysWithIo(TxShard& shard, std::span<const KeyRef> keys,
                       ManualEvent& io, std::vector<std::string>& order) {
  auto guard = co_await shard.AcquireKeys(keys);
  order.push_back("keys:start");
  co_await io;
  order.push_back("keys:done");
}

TEST(TxLockTest, PartialWriteKeySetWaitsWithoutTakingPartialHolds) {
  TxShard shard;
  std::atomic<std::uint64_t> counter{1};
  shard.BindTxidCounter(&counter);
  std::vector<std::string> order;
  ManualEvent io_blocker, io_keys;
  RunWithIo(shard, 1, LockMode::kExclusive, io_blocker, order, "blocker");
  const std::array keys{KeyRef{.fp_ = 1, .mode_ = LockMode::kIntentExclusive},
                        KeyRef{.fp_ = 2, .mode_ = LockMode::kIntentExclusive}};
  RunKeysWithIo(shard, keys, io_keys, order);
  EXPECT_EQ(order, (std::vector<std::string>{"blocker:start"}));
  EXPECT_TRUE(shard.locks(0).CanHold(2, LockMode::kExclusive));
  // The pending key-set intent must still stop a later conflicting writer.
  RunImmediate(shard, 2, LockMode::kExclusive, order, "later");
  EXPECT_EQ(order.size(), 1);
  io_blocker.Fire();
  EXPECT_EQ(order.back(), "keys:start");
  EXPECT_FALSE(shard.locks(0).CanHold(1, LockMode::kShared));
  EXPECT_FALSE(shard.locks(0).CanHold(2, LockMode::kShared));
  io_keys.Fire();
  EXPECT_EQ(order, (std::vector<std::string>{"blocker:start", "blocker:done",
                                             "keys:start", "keys:done",
                                             "later:start", "later:done"}));
  EXPECT_EQ(shard.locks(0).size(), 0);
}

TEST(TxLockTest, PartialWritersOverlapIoButCannotBargePastReadersOrWriters) {
  for (const auto blocker : {LockMode::kShared, LockMode::kExclusive}) {
    SCOPED_TRACE(static_cast<int>(blocker));
    TxShard shard;
    std::atomic<std::uint64_t> counter{1};
    shard.BindTxidCounter(&counter);
    std::vector<std::string> order;
    ManualEvent io_a, io_b, io_blocker;
    RunWithIo(shard, 1, LockMode::kIntentExclusive, io_a, order, "A");
    RunWithIo(shard, 1, LockMode::kIntentExclusive, io_b, order, "B");
    EXPECT_EQ(order, (std::vector<std::string>{"A:start", "B:start"}));
    EXPECT_EQ(counter.load(), 1);  // Compatible fast paths allocate no txid.

    RunWithIo(shard, 1, blocker, io_blocker, order, "barrier");
    RunImmediate(shard, 1, LockMode::kIntentExclusive, order, "C");
    EXPECT_EQ(order.size(), 2);
    RunImmediate(shard, 2, LockMode::kExclusive, order, "unrelated");
    EXPECT_EQ(order.back(), "unrelated:done");

    io_a.Fire();
    EXPECT_EQ(order.back(), "A:done");
    io_b.Fire();
    EXPECT_EQ(order.back(), "barrier:start");
    io_blocker.Fire();
    EXPECT_EQ(order, (std::vector<std::string>{
                         "A:start", "B:start", "unrelated:start",
                         "unrelated:done", "A:done", "B:done", "barrier:start",
                         "barrier:done", "C:start", "C:done"}));
    EXPECT_EQ(shard.locks(0).size(), 0);
    EXPECT_EQ(shard.queued_runs(), 2);
  }
}

TEST(TxLockTest, CancelledBarrierWakesCompatibleSuccessorBeforeHolderFinishes) {
  constexpr std::array modes{LockMode::kShared, LockMode::kExclusive,
                             LockMode::kIntentExclusive};
  for (const auto holder : {LockMode::kShared, LockMode::kIntentExclusive,
                            LockMode::kIntentShared}) {
    for (const auto barrier : modes) {
      if (holder == barrier || (holder == LockMode::kIntentShared &&
                                barrier != LockMode::kExclusive))
        continue;
      SCOPED_TRACE(::testing::Message() << static_cast<int>(holder) << ","
                                        << static_cast<int>(barrier));
      TxShard shard;
      std::atomic<std::uint64_t> counter{1};
      shard.BindTxidCounter(&counter);
      std::vector<std::string> order;
      ManualEvent io;
      RunWithIo(shard, 1, holder, io, order, "holder");
      {
        auto cancelled = shard.AcquireKey(0, 1, barrier);
        ASSERT_FALSE(cancelled.await_ready());
        cancelled.await_suspend(std::noop_coroutine());
        RunImmediate(shard, 1, holder, order, "successor");
        EXPECT_EQ(order, (std::vector<std::string>{"holder:start"}));
      }
      EXPECT_EQ(order,
                (std::vector<std::string>{"holder:start", "successor:start",
                                          "successor:done"}));
      io.Fire();
      EXPECT_EQ(shard.locks(0).size(), 0);
      EXPECT_TRUE(shard.queue().Empty());
    }
  }
}

TEST(TxLockTest, IntentSharedOverlapsQueuedNonExclusiveModesButNotQueuedX) {
  for (const auto holder : {LockMode::kShared, LockMode::kIntentExclusive}) {
    SCOPED_TRACE(static_cast<int>(holder));
    const auto waiter = holder == LockMode::kShared ? LockMode::kIntentExclusive
                                                    : LockMode::kShared;
    TxShard shard;
    std::atomic<std::uint64_t> counter{1};
    shard.BindTxidCounter(&counter);
    std::vector<std::string> order;
    ManualEvent io_holder, io_waiter, io_is;
    RunWithIo(shard, 1, holder, io_holder, order, "holder");
    RunWithIo(shard, 1, waiter, io_waiter, order, "waiter");
    RunWithIo(shard, 1, LockMode::kIntentShared, io_is, order, "IS");
    EXPECT_EQ(order, (std::vector<std::string>{"holder:start", "IS:start"}));
    EXPECT_EQ(counter.load(), 2);  // IS is compatible with both modes.

    RunImmediate(shard, 1, LockMode::kExclusive, order, "X");
    RunImmediate(shard, 1, LockMode::kIntentShared, order, "later IS");
    EXPECT_EQ(order.size(), 2);
    io_holder.Fire();
    EXPECT_EQ(order.back(), "waiter:start");
    io_waiter.Fire();
    EXPECT_EQ(order.back(), "waiter:done");
    EXPECT_FALSE(shard.locks(0).CanHold(1, LockMode::kExclusive));
    io_is.Fire();
    EXPECT_EQ(order, (std::vector<std::string>{
                         "holder:start", "IS:start", "holder:done",
                         "waiter:start", "waiter:done", "IS:done", "X:start",
                         "X:done", "later IS:start", "later IS:done"}));
    EXPECT_EQ(shard.locks(0).size(), 0);
    EXPECT_TRUE(shard.queue().Empty());
  }
}

TEST(TxLockTest, QueuedCompatibleHoldersReceiveHoldsBeforeWorkerResumption) {
  constexpr std::array modes{LockMode::kShared, LockMode::kIntentExclusive,
                             LockMode::kIntentShared};
  for (const auto first_mode : modes) {
    for (const auto second_mode : modes) {
      if (first_mode != second_mode && first_mode != LockMode::kIntentShared &&
          second_mode != LockMode::kIntentShared)
        continue;
      SCOPED_TRACE(::testing::Message() << static_cast<int>(first_mode) << ","
                                        << static_cast<int>(second_mode));
      // Use a bound worker without starting its loop: queued noop continuations
      // let us inspect grants independently of when request bodies resume.
      bycorf::Worker worker;
      TxShard shard;
      shard.Bind(worker);
      std::atomic<std::uint64_t> counter{1};
      shard.BindTxidCounter(&counter);
      auto initial = shard.AcquireKey(0, 1, LockMode::kExclusive);
      ASSERT_TRUE(initial.await_ready());
      auto initial_guard = initial.await_resume();
      auto first = shard.AcquireKey(0, 1, first_mode);
      ASSERT_FALSE(first.await_ready());
      first.await_suspend(std::noop_coroutine());
      std::optional<TxShard::Awaiter> second;
      second.emplace(&shard, KeyRef{.fp_ = 1, .mode_ = second_mode});
      ASSERT_FALSE(second->await_ready());
      second->await_suspend(std::noop_coroutine());
      auto exclusive = shard.AcquireKey(0, 1, LockMode::kExclusive);
      ASSERT_FALSE(exclusive.await_ready());
      exclusive.await_suspend(std::noop_coroutine());

      initial_guard.Reset();
      EXPECT_EQ(shard.queued_runs(), 2);
      ASSERT_NE(shard.queue().Front(), nullptr);
      EXPECT_EQ(shard.queue().Front()->txid_, 3);
      auto first_guard = first.await_resume();
      first_guard.Reset();
      EXPECT_FALSE(shard.locks(0).CanHold(1, LockMode::kExclusive));
      EXPECT_EQ(shard.queued_runs(), 2);
      // Cancellation after grant, before await_resume, releases the second hold
      // and permits the queued X without leaving a pointer to the awaiter.
      second.reset();
      EXPECT_EQ(shard.queued_runs(), 3);
      EXPECT_TRUE(shard.queue().Empty());
      auto exclusive_guard = exclusive.await_resume();
      exclusive_guard.Reset();
      EXPECT_EQ(shard.locks(0).size(), 0);
    }
  }
}

TEST(TxLockTest, MixedIntentHoldersRetainReaderClosuresUntilEveryHoldDrains) {
  struct Admission {
    unsigned closures = 0;
    unsigned readers = 1;
  } state;
  TxShard shard;
  std::atomic<std::uint64_t> counter{1};
  shard.BindTxidCounter(&counter);
  shard.BindReadAdmission({&state,
                           [](void* ctx, const KeyRef&) {
                             ++static_cast<Admission*>(ctx)->closures;
                           },
                           [](void* ctx, const KeyRef&) {
                             --static_cast<Admission*>(ctx)->closures;
                           },
                           [](void* ctx, const KeyRef&) {
                             return static_cast<Admission*>(ctx)->readers == 0;
                           }});
  std::vector<std::string> order;
  ManualEvent io_a, io_b;
  RunWithIo(shard, 1, LockMode::kIntentShared, io_a, order, "A");
  RunWithIo(shard, 1, LockMode::kIntentExclusive, io_b, order, "B");
  RunImmediate(shard, 1, LockMode::kShared, order, "reader");
  EXPECT_TRUE(order.empty());
  EXPECT_EQ(state.closures, 3);
  for (const auto mode :
       {LockMode::kIntentExclusive, LockMode::kIntentShared}) {
    auto cancelled = shard.AcquireKey(0, 1, mode);
    ASSERT_FALSE(cancelled.await_ready());
    cancelled.await_suspend(std::noop_coroutine());
    EXPECT_EQ(state.closures, 4);
  }
  EXPECT_EQ(state.closures, 3);
  EXPECT_TRUE(order.empty());
  state.readers = 0;
  shard.ReadersDrained();
  EXPECT_EQ(order, (std::vector<std::string>{"A:start", "B:start"}));
  io_a.Fire();
  EXPECT_EQ(state.closures, 2);
  EXPECT_EQ(order.back(), "A:done");
  EXPECT_FALSE(shard.CanReadOptimistically(0));
  io_b.Fire();
  EXPECT_EQ(order,
            (std::vector<std::string>{"A:start", "B:start", "A:done", "B:done",
                                      "reader:start", "reader:done"}));
  EXPECT_EQ(state.closures, 0);
  EXPECT_EQ(shard.locks(0).size(), 0);
  EXPECT_TRUE(shard.CanReadOptimistically(0));
}

TEST(TxLockTest, ReaderDrainResumesQueueAndRetainsAllIntentClosures) {
  struct Admission {
    std::array<unsigned, 32> intents{};
    std::array<unsigned, 32> readers{};
  } state;
  TxShard shard;
  std::atomic<std::uint64_t> counter{1};
  shard.BindTxidCounter(&counter);
  shard.BindReadAdmission(
      {&state,
       [](void* ctx, const KeyRef& key) {
         ++static_cast<Admission*>(ctx)->intents[key.fp_ & 31];
       },
       [](void* ctx, const KeyRef& key) {
         --static_cast<Admission*>(ctx)->intents[key.fp_ & 31];
       },
       [](void* ctx, const KeyRef& key) {
         return static_cast<Admission*>(ctx)->readers[key.fp_ & 31] == 0;
       }});
  state.readers[0] = 2;
  std::vector<std::string> order;
  ManualEvent io;
  RunWithIo(shard, 0, LockMode::kExclusive, io, order, "writer");
  RunImmediate(shard, 0, LockMode::kShared, order, "reader");
  EXPECT_TRUE(order.empty());
  EXPECT_EQ(state.intents[0], 2);
  RunImmediate(shard, 1, LockMode::kExclusive, order, "unrelated");
  EXPECT_EQ(order.size(), 2);
  --state.readers[0];
  shard.ReadersDrained();
  EXPECT_EQ(order.size(), 2);
  --state.readers[0];
  shard.ReadersDrained();
  ASSERT_EQ(order.size(), 3);
  EXPECT_EQ(order.back(), "writer:start");
  EXPECT_EQ(state.intents[0], 2);
  io.Fire();
  EXPECT_EQ(order.back(), "reader:done");
  EXPECT_EQ(state.intents[0], 0);
  EXPECT_TRUE(shard.CanReadOptimistically(0));
  state.readers[0] = 1;
  {
    auto cancelled = shard.AcquireKey(0, 0, LockMode::kExclusive);
    ASSERT_FALSE(cancelled.await_ready());
    cancelled.await_suspend(std::noop_coroutine());
    EXPECT_EQ(state.intents[0], 1);
  }
  EXPECT_EQ(state.intents[0], 0);
  EXPECT_TRUE(shard.CanReadOptimistically(0));
  state.readers[0] = 0;
  shard.ReadersDrained();  // No pointer into the destroyed awaiter remains.
}

TEST(TxLockTest, QueuesConflictsWithoutBlockingOtherKeys) {
  TxShard shard;  // unbound: Poll resumes inline
  std::atomic<std::uint64_t> counter{1};
  shard.BindTxidCounter(&counter);
  std::vector<std::string> order;
  const LockFp k1 = 100;
  const LockFp k2 = 200;
  EXPECT_TRUE(shard.CanReadOptimistically(0));

  // t0: fast-path reader takes k1 and falls asleep on "disk".
  ManualEvent io_a;
  RunWithIo(shard, k1, LockMode::kShared, io_a, order, "A");
  EXPECT_CHECK(order == std::vector<std::string>{"A:start"},
               "A started, sleeping");
  EXPECT_CHECK(counter.load() == 1,
               "fast path must not touch the txid counter");
  EXPECT_FALSE(shard.CanReadOptimistically(0));

  // t1: writer on k1 conflicts with the sleeping holder -> queues.
  RunImmediate(shard, k1, LockMode::kExclusive, order, "B");
  EXPECT_CHECK(order.size() == 1, "B must wait for the sleeping holder");
  EXPECT_CHECK(counter.load() == 2, "contended acquisition draws a txid");
  EXPECT_FALSE(shard.CanReadOptimistically(0));

  // t2: second reader on k1 blocked by B's exclusive intent (no barging).
  RunImmediate(shard, k1, LockMode::kShared, order, "C");
  EXPECT_CHECK(order.size() == 1, "C must queue behind B");

  // Non-conflicting traffic overlaps the sleeping holder's I/O.
  RunImmediate(shard, k2, LockMode::kExclusive, order, "D");
  EXPECT_CHECK(
      order.size() == 3 && order[1] == "D:start" && order[2] == "D:done",
      "D runs immediately on an uncontended key");
  EXPECT_CHECK(shard.fastpath_runs() == 2, "A and D took the fast path");

  // t3: the sleeping holder finishes; queue drains in txid order.
  io_a.Fire();
  const std::vector<std::string> expected{"A:start", "D:start", "D:done",
                                          "A:done",  "B:start", "B:done",
                                          "C:start", "C:done"};
  EXPECT_CHECK(order == expected,
               "queue drained in order after holder release");
  EXPECT_CHECK(shard.queued_runs() == 2, "B and C ran from the queue");
  EXPECT_CHECK(shard.committed_txid() == 2,
               "committed_txid advanced to C's txid");
  EXPECT_CHECK(shard.locks(0).size() == 0, "lock table empty at quiescence");
  EXPECT_TRUE(shard.CanReadOptimistically(0));
}

}  // namespace
