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

#include "lavik/storage/detail/grouped/lock.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <coroutine>
#include <deque>
#include <exception>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "lavik/tx/tx_shard.h"

namespace {

using lavik::storage::GroupedLockId;
using lavik::storage::GroupedLockIndex;
using lavik::storage::GroupedLockMode;
using lavik::storage::GroupedLockObject;
using lavik::storage::GroupedLockTable;
constexpr auto kS = GroupedLockMode::kShared;
constexpr auto kX = GroupedLockMode::kExclusive;
constexpr GroupedLockObject kObject{.fp_ = 41, .incarnation_ = 7};

constexpr GroupedLockId Page(std::uint64_t id) {
  return {GroupedLockIndex::kOrdered, {id, 0}};
}

struct ResumeQueue {
  std::deque<std::coroutine_handle<>> ready_;

  GroupedLockTable::ResumeQueue queue() {
    return {this, [](void* context, std::coroutine_handle<> handle) {
              static_cast<ResumeQueue*>(context)->ready_.push_back(handle);
            }};
  }
  void RunOne() {
    ASSERT_FALSE(ready_.empty());
    auto handle = ready_.front();
    ready_.pop_front();
    handle.resume();
  }
  void RunAll() {
    while (!ready_.empty()) RunOne();
  }
};

// Frame destruction exercises real queued cancellation. Tests only destroy a
// suspended frame before grant, or after its scheduled resumption has run.
class Task {
 public:
  struct promise_type {
    Task get_return_object() {
      return Task(std::coroutine_handle<promise_type>::from_promise(*this));
    }
    std::suspend_never initial_suspend() noexcept { return {}; }
    std::suspend_always final_suspend() noexcept { return {}; }
    void return_void() {}
    void unhandled_exception() { std::terminate(); }
  };
  explicit Task(std::coroutine_handle<promise_type> handle) : handle_(handle) {}
  Task(const Task&) = delete;
  Task& operator=(const Task&) = delete;
  ~Task() { Cancel(); }
  void Cancel() {
    if (handle_) std::exchange(handle_, {}).destroy();
  }

 private:
  std::coroutine_handle<promise_type> handle_;
};

struct IoWait {
  std::coroutine_handle<> handle_;
  bool await_ready() const noexcept { return false; }
  void await_suspend(std::coroutine_handle<> handle) noexcept {
    handle_ = handle;
  }
  void await_resume() const noexcept {}
  void Complete() { std::exchange(handle_, {}).resume(); }
};

void AcquireNow(GroupedLockTable::LockSet& locks, GroupedLockId id,
                GroupedLockMode mode = kX) {
  auto acquire = locks.Acquire(id, mode);
  ASSERT_TRUE(acquire.await_ready());
  acquire.await_resume();
}

Task RunWithGroups(GroupedLockTable& table, std::vector<GroupedLockId> ids,
                   GroupedLockMode mode, std::vector<std::string>& events,
                   std::string name, IoWait* io = nullptr) {
  auto locks = table.MakeLockSet(kObject);
  for (auto id : ids) co_await locks.Acquire(id, mode);
  events.push_back(name + ":start");
  if (io) co_await *io;
  events.push_back(name + ":done");
}

TEST(GroupedLockTest, SameGroupCompatibilityAndUncontendedCleanup) {
  for (auto first_mode : {kS, kX}) {
    for (auto second_mode : {kS, kX}) {
      SCOPED_TRACE(::testing::Message() << static_cast<int>(first_mode) << ","
                                        << static_cast<int>(second_mode));
      ResumeQueue queue;
      GroupedLockTable table(queue.queue());
      auto first = table.MakeLockSet(kObject);
      auto second = table.MakeLockSet(kObject);
      AcquireNow(first, Page(1), first_mode);
      auto acquire = second.Acquire(Page(1), second_mode);
      const bool compatible = first_mode == kS && second_mode == kS;
      EXPECT_EQ(acquire.await_ready(), compatible);
      if (!compatible) acquire.await_suspend(std::noop_coroutine());
      EXPECT_EQ(table.size(), 1);
      first.Reset();
      EXPECT_EQ(queue.ready_.size(), compatible ? 0 : 1);
      // The grant is installed even before the worker resumes this waiter.
      EXPECT_EQ(second.size(), 1);
      queue.RunAll();
      acquire.await_resume();
      second.Reset();
      EXPECT_EQ(table.size(), 0);
    }
  }
}

TEST(GroupedLockTest, DatabaseKeyIncarnationAndGraphScopeIdentities) {
  ResumeQueue queue;
  GroupedLockTable table(queue.queue());
  auto base = table.MakeLockSet(kObject);
  auto database = table.MakeLockSet({1, kObject.fp_, kObject.incarnation_});
  auto key = table.MakeLockSet({0, kObject.fp_ + 1, kObject.incarnation_});
  auto incarnation =
      table.MakeLockSet({0, kObject.fp_, kObject.incarnation_ + 1});
  AcquireNow(base, {GroupedLockIndex::kMember, {0, 0}});
  AcquireNow(base, {GroupedLockIndex::kOrdered, {0, 0}});
  AcquireNow(database, {GroupedLockIndex::kMember, {0, 0}});
  AcquireNow(key, {GroupedLockIndex::kMember, {0, 0}});
  AcquireNow(incarnation, {GroupedLockIndex::kMember, {0, 0}});
  EXPECT_EQ(table.size(), 5);
  EXPECT_TRUE(queue.ready_.empty());
}

TEST(GroupedLockTest, OrderedAcquisitionsCanRevisitSufficientHolds) {
  ResumeQueue queue;
  GroupedLockTable table(queue.queue());
  auto locks = table.MakeLockSet(kObject);
  const GroupedLockId member{GroupedLockIndex::kMember, {8, 1}};
  const GroupedLockId later_member{GroupedLockIndex::kMember, {8, 2}};
  const GroupedLockId last_member{
      GroupedLockIndex::kMember,
      {std::numeric_limits<std::uint64_t>::max(), 64}};
  AcquireNow(locks, member, kS);
  AcquireNow(locks, later_member);
  AcquireNow(locks, last_member);
  AcquireNow(locks, Page(1));
  // Idempotent requests may revisit an earlier hold; X also satisfies S.
  AcquireNow(locks, member, kS);
  AcquireNow(locks, later_member, kS);
  EXPECT_EQ(locks.size(), 4);
  locks.Reset();
  EXPECT_EQ(table.size(), 0);
  AcquireNow(locks, member, kX);
}

#ifndef NDEBUG
TEST(GroupedLockDeathTest, InvalidRequestsViolateAcquisitionContract) {
  ResumeQueue queue;
  GroupedLockTable table(queue.queue());
  auto locks = table.MakeLockSet(kObject);
  EXPECT_DEATH(
      {
        auto acquire = locks.Acquire(Page(1), static_cast<GroupedLockMode>(2));
        acquire.await_ready();
      },
      "invalid group lock mode");
  EXPECT_DEATH(
      {
        auto acquire =
            locks.Acquire({static_cast<GroupedLockIndex>(2), {1, 0}}, kX);
        acquire.await_ready();
      },
      "invalid group lock index");
  AcquireNow(locks, Page(2), kS);
  EXPECT_DEATH(
      {
        auto acquire = locks.Acquire(Page(1), kX);
        acquire.await_ready();
      },
      "group locks must be acquired in increasing order");
  EXPECT_DEATH(
      {
        auto acquire = locks.Acquire(Page(2), kX);
        acquire.await_ready();
      },
      "group lock upgrade requires releasing and replanning");
  auto owner = table.MakeLockSet(kObject);
  AcquireNow(owner, Page(3));
  auto pending = locks.Acquire(Page(3), kX);
  ASSERT_FALSE(pending.await_ready());
  pending.await_suspend(std::noop_coroutine());
  EXPECT_DEATH(
      {
        auto acquire = locks.Acquire(Page(4), kX);
        acquire.await_ready();
      },
      "a group acquisition is already pending");
}
#endif

TEST(GroupedLockTest, RetainsEarlierGroupsWhileOtherGroupsKeepProgressing) {
  ResumeQueue queue;
  GroupedLockTable table(queue.queue());
  std::vector<std::string> events;
  IoWait io;
  auto first = table.MakeLockSet(kObject);
  AcquireNow(first, Page(2));
  auto waiting =
      RunWithGroups(table, {Page(1), Page(2)}, kX, events, "waiting", &io);
  auto behind = RunWithGroups(table, {Page(1)}, kX, events, "behind");
  EXPECT_TRUE(events.empty());
  // A global waiter queue would hide this acquisition behind "waiting" even
  // though it is needed by the current group-2 holder to finish its operation.
  AcquireNow(first, Page(3));
  auto independent = RunWithGroups(table, {Page(4)}, kX, events, "independent");
  EXPECT_EQ(events, (std::vector<std::string>{"independent:start",
                                              "independent:done"}));
  first.Reset();
  EXPECT_EQ(queue.ready_.size(), 1);
  queue.RunAll();
  EXPECT_EQ(events.back(), "waiting:start");
  EXPECT_EQ(table.size(), 2);
  io.Complete();
  queue.RunAll();
  EXPECT_EQ(events,
            (std::vector<std::string>{"independent:start", "independent:done",
                                      "waiting:start", "waiting:done",
                                      "behind:start", "behind:done"}));
  EXPECT_EQ(table.size(), 0);
}

TEST(GroupedLockTest,
     QueuedWriterFencesReadersAndReceivesOwnershipBeforeResume) {
  ResumeQueue queue;
  GroupedLockTable table(queue.queue());
  std::vector<std::string> events;
  IoWait writer_io;
  auto reader = table.MakeLockSet(kObject);
  AcquireNow(reader, Page(1), kS);
  auto writer =
      RunWithGroups(table, {Page(1)}, kX, events, "writer", &writer_io);
  auto waiting_reader = RunWithGroups(table, {Page(1)}, kS, events, "reader");
  EXPECT_TRUE(events.empty());
  reader.Reset();
  ASSERT_EQ(queue.ready_.size(), 1);
  auto late_reader = RunWithGroups(table, {Page(1)}, kS, events, "late reader");
  auto late_writer = RunWithGroups(table, {Page(1)}, kX, events, "late writer");
  EXPECT_TRUE(events.empty());
  queue.RunOne();
  EXPECT_EQ(events, (std::vector<std::string>{"writer:start"}));
  writer_io.Complete();
  // Both readers are granted before either resumes. The next writer waits
  // for both holds, including the reader still sitting in the resume queue.
  ASSERT_EQ(queue.ready_.size(), 2);
  queue.RunOne();
  EXPECT_EQ(queue.ready_.size(), 1);
  EXPECT_EQ(events.back(), "reader:done");
  queue.RunOne();
  EXPECT_EQ(queue.ready_.size(), 1);
  EXPECT_EQ(events.back(), "late reader:done");
  queue.RunOne();
  EXPECT_EQ(events, (std::vector<std::string>{
                        "writer:start", "writer:done", "reader:start",
                        "reader:done", "late reader:start", "late reader:done",
                        "late writer:start", "late writer:done"}));
  EXPECT_EQ(table.size(), 0);
}

TEST(GroupedLockTest, CancellingWriterWakesReadersWhileAnEarlierReaderHolds) {
  ResumeQueue queue;
  GroupedLockTable table(queue.queue());
  std::vector<std::string> events;
  auto first = table.MakeLockSet(kObject);
  AcquireNow(first, Page(1), kS);
  auto writer = RunWithGroups(table, {Page(1)}, kX, events, "writer");
  auto reader = RunWithGroups(table, {Page(1)}, kS, events, "reader");
  writer.Cancel();
  ASSERT_EQ(queue.ready_.size(), 1);
  queue.RunAll();
  EXPECT_EQ(events, (std::vector<std::string>{"reader:start", "reader:done"}));
  EXPECT_EQ(first.size(), 1);
  EXPECT_EQ(table.size(), 1);
}

TEST(GroupedLockTest, CancellingMiddleAndTailKeepsRemainingFifoOrder) {
  ResumeQueue queue;
  GroupedLockTable table(queue.queue());
  std::vector<std::string> events;
  auto owner = table.MakeLockSet(kObject);
  AcquireNow(owner, Page(1));
  auto first = RunWithGroups(table, {Page(1)}, kX, events, "first");
  auto middle = RunWithGroups(table, {Page(1)}, kX, events, "middle");
  auto third = RunWithGroups(table, {Page(1)}, kX, events, "third");
  auto tail = RunWithGroups(table, {Page(1)}, kX, events, "tail");
  middle.Cancel();
  tail.Cancel();
  owner.Reset();
  queue.RunAll();
  EXPECT_EQ(events, (std::vector<std::string>{"first:start", "first:done",
                                              "third:start", "third:done"}));
  EXPECT_EQ(table.size(), 0);
}

TEST(GroupedLockTest, DestroyingQueuedFrameReleasesEarlierGroups) {
  ResumeQueue queue;
  GroupedLockTable table(queue.queue());
  std::vector<std::string> events;
  auto owner = table.MakeLockSet(kObject);
  AcquireNow(owner, Page(2));
  auto cancelled =
      RunWithGroups(table, {Page(1), Page(2)}, kX, events, "cancelled");
  auto successor = RunWithGroups(table, {Page(1)}, kX, events, "successor");
  cancelled.Cancel();
  queue.RunAll();
  EXPECT_EQ(events,
            (std::vector<std::string>{"successor:start", "successor:done"}));
  EXPECT_EQ(table.size(), 1);
  owner.Reset();
  EXPECT_TRUE(queue.ready_.empty());
  EXPECT_EQ(table.size(), 0);
}

TEST(GroupedLockTest, CancellingPendingAcquisitionRetainsEarlierHolds) {
  ResumeQueue queue;
  GroupedLockTable table(queue.queue());
  auto owner = table.MakeLockSet(kObject);
  auto waiting = table.MakeLockSet(kObject);
  AcquireNow(owner, Page(2));
  AcquireNow(waiting, Page(1));
  {
    auto pending = waiting.Acquire(Page(2), kX);
    ASSERT_FALSE(pending.await_ready());
    pending.await_suspend(std::noop_coroutine());
  }
  EXPECT_EQ(waiting.size(), 1);
  AcquireNow(waiting, Page(3));
  owner.Reset();
  EXPECT_TRUE(queue.ready_.empty());
  waiting.Reset();
  EXPECT_EQ(table.size(), 0);
}

TEST(GroupedLockTest, AbandonedFastAcquisitionReleasesOnlyItsOwnHold) {
  ResumeQueue queue;
  GroupedLockTable table(queue.queue());
  auto locks = table.MakeLockSet(kObject);
  AcquireNow(locks, Page(1));
  {
    auto abandoned = locks.Acquire(Page(2), kX);
    ASSERT_TRUE(abandoned.await_ready());
  }
  EXPECT_EQ(locks.size(), 1);
  EXPECT_EQ(table.size(), 1);
  AcquireNow(locks, Page(2));
}

TEST(GroupedLockTest, GrowingTablesAndLockSetsPreserveHoldersAndWaiters) {
  ResumeQueue queue;
  GroupedLockTable table(queue.queue());
  std::vector<std::string> events;
  IoWait io;
  auto blocker = table.MakeLockSet(kObject);
  AcquireNow(blocker, Page(17));
  std::vector<GroupedLockId> ids;
  for (unsigned i = 1; i <= 17; ++i) ids.push_back(Page(i));
  auto waiting = RunWithGroups(table, ids, kX, events, "waiting", &io);
  std::vector<std::unique_ptr<GroupedLockTable::LockSet>> others;
  for (unsigned i = 18; i < 146; ++i) {
    auto& locks = others.emplace_back(
        new GroupedLockTable::LockSet(table.MakeLockSet(kObject)));
    AcquireNow(*locks, Page(i));
  }
  EXPECT_EQ(table.size(), 145);
  blocker.Reset();
  queue.RunAll();
  EXPECT_EQ(events, (std::vector<std::string>{"waiting:start"}));
  others.clear();
  EXPECT_EQ(table.size(), 17);
  io.Complete();
  EXPECT_EQ(table.size(), 0);
}

TEST(GroupedLockTest, WholeKeyWaiterCannotHideAGroupNeededByAnIxHolder) {
  using lavik::tx::LockMode;
  lavik::tx::TxShard shard;
  std::atomic<std::uint64_t> next_txid{1};
  shard.BindTxidCounter(&next_txid);
  auto first_key = shard.AcquireKey(0, kObject.fp_, LockMode::kIntentExclusive);
  ASSERT_TRUE(first_key.await_ready());
  auto first_guard = first_key.await_resume();
  auto second_key =
      shard.AcquireKey(0, kObject.fp_, LockMode::kIntentExclusive);
  ASSERT_TRUE(second_key.await_ready());
  auto second_guard = second_key.await_resume();
  ResumeQueue queue;
  GroupedLockTable table(queue.queue());
  auto first = table.MakeLockSet(kObject);
  auto second = table.MakeLockSet(kObject);
  AcquireNow(first, Page(2));
  AcquireNow(second, Page(1));
  auto waiting = second.Acquire(Page(2), kX);
  ASSERT_FALSE(waiting.await_ready());
  waiting.await_suspend(std::noop_coroutine());
  auto whole_key = shard.AcquireKey(0, kObject.fp_, LockMode::kExclusive);
  ASSERT_FALSE(whole_key.await_ready());
  whole_key.await_suspend(std::noop_coroutine());
  AcquireNow(first, Page(3));
  EXPECT_EQ(next_txid.load(),
            2);  // Only the key-level X waiter allocated an id.
  first.Reset();
  first_guard.Reset();
  EXPECT_EQ(shard.queue().size(), 1);
  queue.RunAll();
  waiting.await_resume();
  second.Reset();
  second_guard.Reset();
  EXPECT_TRUE(shard.queue().Empty());
  auto whole_guard = whole_key.await_resume();
  whole_guard.Reset();
  EXPECT_EQ(table.size(), 0);
  EXPECT_EQ(shard.locks(0).size(), 0);
}

}  // namespace
