#include <atomic>
#include <coroutine>
#include <cstdint>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "keylane/tx/intent_lock.h"
#include "keylane/tx/tx_queue.h"
#include "keylane/tx/tx_shard.h"

namespace {

using keylane::tx::KeyRef;
using keylane::tx::LockFp;
using keylane::tx::LockMode;
using keylane::tx::LockTable;
using keylane::tx::TxQueue;
using keylane::tx::TxShard;
using keylane::tx::TxWaiter;

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
  std::coroutine_handle<> handle;
  bool ready = false;

  auto operator co_await() {
    struct Awaiter {
      ManualEvent* event;
      bool await_ready() const { return event->ready; }
      void await_suspend(std::coroutine_handle<> h) { event->handle = h; }
      void await_resume() const {}
    };
    return Awaiter{this};
  }

  void Fire() {
    ready = true;
    if (handle) {
      auto h = handle;
      handle = {};
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
  EXPECT_CHECK(!table.AcquireIntent(fp, LockMode::kExclusive), "X blocked by S");
  table.ReleaseIntent(fp, LockMode::kExclusive);
  table.ReleaseIntent(fp, LockMode::kShared);
  table.ReleaseIntent(fp, LockMode::kShared);
  EXPECT_CHECK(table.size() == 0, "entry erased when all counters zero");

  EXPECT_CHECK(table.AcquireIntent(fp, LockMode::kExclusive), "X grant on free");
  EXPECT_CHECK(!table.AcquireIntent(fp, LockMode::kShared), "S blocked by X");
  EXPECT_CHECK(!table.AcquireIntent(fp, LockMode::kExclusive), "X blocked by X");
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

TEST(TxLockTest, QueueOrdersByTransactionId) {
  TxQueue queue;
  TxWaiter a{.txid = 5};
  TxWaiter b{.txid = 3};
  TxWaiter c{.txid = 7};
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

TEST(TxLockTest, QueuesConflictsWithoutBlockingOtherKeys) {
  TxShard shard;  // unbound: Poll resumes inline
  std::atomic<std::uint64_t> counter{1};
  shard.BindTxidCounter(&counter);
  std::vector<std::string> order;
  const LockFp k1 = 100;
  const LockFp k2 = 200;

  // t0: fast-path reader takes k1 and falls asleep on "disk".
  ManualEvent io_a;
  RunWithIo(shard, k1, LockMode::kShared, io_a, order, "A");
  EXPECT_CHECK(order == std::vector<std::string>{"A:start"},
               "A started, sleeping");
  EXPECT_CHECK(counter.load() == 1, "fast path must not touch the txid counter");

  // t1: writer on k1 conflicts with the sleeping holder -> queues.
  RunImmediate(shard, k1, LockMode::kExclusive, order, "B");
  EXPECT_CHECK(order.size() == 1, "B must wait for the sleeping holder");
  EXPECT_CHECK(counter.load() == 2, "contended acquisition draws a txid");

  // t2: second reader on k1 blocked by B's exclusive intent (no barging).
  RunImmediate(shard, k1, LockMode::kShared, order, "C");
  EXPECT_CHECK(order.size() == 1, "C must queue behind B");

  // Non-conflicting traffic overlaps the sleeping holder's I/O.
  RunImmediate(shard, k2, LockMode::kExclusive, order, "D");
  EXPECT_CHECK(order.size() == 3 && order[1] == "D:start" && order[2] == "D:done",
               "D runs immediately on an uncontended key");
  EXPECT_CHECK(shard.fastpath_runs() == 2, "A and D took the fast path");

  // t3: the sleeping holder finishes; queue drains in txid order.
  io_a.Fire();
  const std::vector<std::string> expected{
      "A:start", "D:start", "D:done", "A:done",
      "B:start", "B:done",  "C:start", "C:done"};
  EXPECT_CHECK(order == expected, "queue drained in order after holder release");
  EXPECT_CHECK(shard.queued_runs() == 2, "B and C ran from the queue");
  EXPECT_CHECK(shard.committed_txid() == 2,
               "committed_txid advanced to C's txid");
  EXPECT_CHECK(shard.locks(0).size() == 0, "lock table empty at quiescence");
}

}  // namespace
