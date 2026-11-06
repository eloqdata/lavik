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

#include <algorithm>
#include <cassert>
#include <limits>
#include <utility>

#include "bycorf/runtime/worker.h"

namespace lavik::storage {

GroupedLockTable::GroupedLockTable(bycorf::Worker& worker)
    : GroupedLockTable(ResumeQueue{
          &worker, [](void* context, std::coroutine_handle<> handle) noexcept {
            static_cast<bycorf::Worker*>(context)->Enqueue(handle);
          }}) {}

GroupedLockTable::GroupedLockTable(ResumeQueue queue) : queue_(queue) {
  assert(queue_.enqueue_ != nullptr);
}

GroupedLockTable::~GroupedLockTable() { assert(entries_.empty()); }

GroupedLockTable::LockSet GroupedLockTable::MakeLockSet(
    GroupedLockObject object) {
  assert(object.db_id_ < kLogicalDatabaseCount);
  assert(object.incarnation_ != 0);
  return LockSet(*this, object);
}

GroupedLockTable::LockSet::~LockSet() { Reset(); }

GroupedLockTable::Awaiter GroupedLockTable::LockSet::Acquire(
    GroupedLockId id, GroupedLockMode mode) {
  return Awaiter(*this, id, mode);
}

void GroupedLockTable::LockSet::Reset() noexcept {
  assert(pending_ == nullptr);
  while (!held_.empty()) {
    const Held held = held_.back();
    held_.pop_back();
    table_.Release(object_, held);
  }
}

bool GroupedLockTable::Entry::CanHold(GroupedLockMode mode) const noexcept {
  return !writer_ && (mode == GroupedLockMode::kShared || readers_ == 0);
}

bool GroupedLockTable::Awaiter::await_ready() {
  assert(!started_);
  started_ = true;
  assert(set_.pending_ == nullptr &&
         "a group acquisition is already pending for this lock set");
  assert((mode_ == GroupedLockMode::kShared ||
          mode_ == GroupedLockMode::kExclusive) &&
         "invalid group lock mode");
  assert((id_.index_ == GroupedLockIndex::kMember ||
          id_.index_ == GroupedLockIndex::kOrdered) &&
         "invalid group lock index");
  if (!set_.held_.empty() && id_ <= set_.held_.back().entry_->id_) {
#ifndef NDEBUG
    // A valid request can only revisit an already-held identity here. Keep
    // the diagnostic lookup out of Release's idempotent acquisition path.
    const auto it = std::lower_bound(
        set_.held_.begin(), set_.held_.end(), id_,
        [](const LockSet::Held& held, const GroupedLockId& id) {
          return held.entry_->id_ < id;
        });
    assert(it != set_.held_.end() && it->entry_->id_ == id_ &&
           "group locks must be acquired in increasing order");
    assert((it->mode_ == mode_ || it->mode_ == GroupedLockMode::kExclusive) &&
           "group lock upgrade requires releasing and replanning the lock set");
#endif
    return true;
  }

  // Prepare lock-set storage before recording the request so transferring
  // ownership cannot allocate. Resumption uses the queue's non-throwing
  // enqueue contract, just as the worker's other lock-release paths do.
  if (set_.held_.size() == set_.held_.capacity()) {
    set_.held_.reserve(set_.held_.size() * 2);
  }
  auto [it, inserted] =
      set_.table_.entries_.try_emplace(Key{set_.object_, id_}, id_);
  entry_ = &it->second;
  set_.pending_ = this;
  // Even a compatible new reader waits behind an earlier queued writer.
  if (entry_->head_ == nullptr && entry_->CanHold(mode_)) {
    set_.table_.Grant(*this);
    return true;
  }
  return false;
}

void GroupedLockTable::Awaiter::await_suspend(
    std::coroutine_handle<> handle) noexcept {
  assert(entry_ != nullptr && !granted_ && !queued_);
  resume_ = handle;
  previous_ = entry_->tail_;
  if (previous_ != nullptr) {
    previous_->next_ = this;
  } else {
    entry_->head_ = this;
  }
  entry_->tail_ = this;
  queued_ = true;
}

void GroupedLockTable::Awaiter::await_resume() noexcept {
  if (entry_ != nullptr) {
    assert(granted_ && set_.pending_ == this);
    set_.pending_ = nullptr;
    entry_ = nullptr;
  }
}

GroupedLockTable::Awaiter::~Awaiter() {
  if (entry_ == nullptr) return;
  assert(set_.pending_ == this);
  // Worker ready queues hold raw coroutine handles, so post-grant cancellation
  // must run after resumption instead of destroying an enqueued frame.
  assert(!scheduled_);
  set_.pending_ = nullptr;
  if (granted_) {
    const auto held = set_.held_.back();
    assert(held.entry_ == entry_);
    set_.held_.pop_back();
    set_.table_.Release(set_.object_, held);
  } else if (queued_) {
    set_.table_.Unlink(*this);
    set_.table_.Drain(*entry_);
    set_.table_.EraseIfFree(set_.object_, *entry_);
  }
}

void GroupedLockTable::Grant(Awaiter& waiter) noexcept {
  Entry& entry = *waiter.entry_;
  assert(entry.CanHold(waiter.mode_));
  assert(waiter.set_.held_.size() < waiter.set_.held_.capacity());
  waiter.set_.held_.push_back({&entry, waiter.mode_});
  if (waiter.mode_ == GroupedLockMode::kShared) {
    assert(entry.readers_ < std::numeric_limits<std::uint32_t>::max());
    ++entry.readers_;
  } else {
    entry.writer_ = true;
  }
  waiter.granted_ = true;
}

void GroupedLockTable::Unlink(Awaiter& waiter) noexcept {
  assert(waiter.queued_);
  Entry& entry = *waiter.entry_;
  if (waiter.previous_ != nullptr) {
    waiter.previous_->next_ = waiter.next_;
  } else {
    entry.head_ = waiter.next_;
  }
  if (waiter.next_ != nullptr) {
    waiter.next_->previous_ = waiter.previous_;
  } else {
    entry.tail_ = waiter.previous_;
  }
  waiter.previous_ = nullptr;
  waiter.next_ = nullptr;
  waiter.queued_ = false;
}

void GroupedLockTable::Drain(Entry& entry) noexcept {
  while (entry.head_ != nullptr && entry.CanHold(entry.head_->mode_)) {
    Awaiter& next = *entry.head_;
    Unlink(next);
    // Transfer ownership before enqueueing, so new arrivals cannot steal the
    // lock while the granted coroutine is still waiting in the worker queue.
    Grant(next);
    next.scheduled_ = true;
    queue_.enqueue_(queue_.context_, next.resume_);
  }
}

void GroupedLockTable::EraseIfFree(GroupedLockObject object,
                                   Entry& entry) noexcept {
  if (!entry.writer_ && entry.readers_ == 0 && entry.head_ == nullptr) {
    entries_.erase(Key{object, entry.id_});
  }
}

void GroupedLockTable::Release(GroupedLockObject object,
                               LockSet::Held held) noexcept {
  Entry& entry = *held.entry_;
  if (held.mode_ == GroupedLockMode::kShared) {
    assert(entry.readers_ != 0);
    --entry.readers_;
  } else {
    assert(entry.writer_);
    entry.writer_ = false;
  }
  Drain(entry);
  EraseIfFree(object, entry);
}

}  // namespace lavik::storage
