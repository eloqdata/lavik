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

#pragma once

#include <compare>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <utility>

#include "absl/container/inlined_vector.h"
#include "absl/container/node_hash_map.h"
#include "lavik/storage/detail/grouped/hash.h"
#include "lavik/tx/fingerprint.h"

namespace bycorf {
class Worker;
}

namespace lavik::storage {

// Member routing is resolved before score-ordered routing. Allocation order of
// GroupedRecordId is irrelevant: its existing total order breaks ties within
// each graph. A page's score/hash boundary is not part of its lock identity.
enum class GroupedLockIndex : std::uint8_t { kMember, kOrdered };
enum class GroupedLockMode : std::uint8_t { kShared, kExclusive };

struct GroupedLockId {
  GroupedLockIndex index_ = GroupedLockIndex::kMember;
  GroupedRecordId record_{};

  auto operator<=>(const GroupedLockId&) const noexcept = default;
};

// Parent key identity is scoped by logical DB and grouped incarnation. A
// fingerprint collision can introduce false contention, never false sharing
// of the protected payload. Database admission fences population replacement.
struct GroupedLockObject {
  std::uint8_t db_id_ = 0;
  tx::LockFp fp_ = 0;
  std::uint64_t incarnation_ = 0;

  bool operator==(const GroupedLockObject&) const noexcept = default;
};

// Worker-local sub-key arbitration, independent of TxShard's ordered key queue.
// Waiting retains earlier group holds and the caller's parent key hold. Each
// group has its own FIFO so a blocked request cannot hide an unrelated group
// needed by a current holder. No worker thread blocks and no txid is allocated.
//
// All calls, including destruction, must run on the same owner. The table and
// its resume queue must outlive every lock set and awaiter.
class GroupedLockTable {
  struct Entry;

 public:
  class Awaiter;

  // RAII ownership of monotonically acquired groups in one object. The caller
  // must hold the parent key in IS (read-only), IX (may write), S (read-only),
  // or X, and retain database admission until after this set is destroyed.
  // All group acquisitions for that parent-key operation must use this set;
  // independent sets cannot enforce a common acquisition order.
  // Do not acquire another parent key or upgrade it while holding groups.
  // Neither the set nor an active awaiter may move across workers.
  class LockSet {
   public:
    LockSet(const LockSet&) = delete;
    LockSet& operator=(const LockSet&) = delete;
    ~LockSet();

    // Acquires one group, retaining all earlier groups across the wait and IO.
    // New identities must increase. An already-held identity is a no-op when
    // its mode suffices. Descending acquisitions and S-to-X upgrades violate
    // the caller's lock-order contract and are asserted in Debug. Replanning
    // or whole-key fallback must release the set first.
    // Only one acquisition may be outstanding per set. After a wait, callers
    // must revalidate routing before accessing the group they intended to use.
    [[nodiscard]] Awaiter Acquire(GroupedLockId id, GroupedLockMode mode);

    // Releases every acquired group. No acquisition may be outstanding. The
    // empty set can be reused, starting a new increasing acquisition sequence.
    void Reset() noexcept;
    std::size_t size() const noexcept { return held_.size(); }

   private:
    friend class GroupedLockTable;
    friend class Awaiter;
    struct Held {
      Entry* entry_;
      GroupedLockMode mode_;
    };
    LockSet(GroupedLockTable& table, GroupedLockObject object)
        : table_(table), object_(object) {}

    GroupedLockTable& table_;
    GroupedLockObject object_;
    absl::InlinedVector<Held, 4> held_;
    Awaiter* pending_ = nullptr;
  };

  // A queued, not-yet-granted acquisition can be cancelled by destroying its
  // awaiter on the owner; earlier holds remain in the LockSet. Once its resume
  // has been enqueued, the coroutine must stay alive through await_resume,
  // matching Worker::Enqueue's frame-lifetime contract. Cancellation then runs
  // in the resumed caller and releases the set normally.
  class Awaiter {
   public:
    Awaiter(const Awaiter&) = delete;
    Awaiter& operator=(const Awaiter&) = delete;
    ~Awaiter();

    bool await_ready();
    void await_suspend(std::coroutine_handle<> handle) noexcept;
    void await_resume() noexcept;

   private:
    friend class LockSet;
    friend class GroupedLockTable;
    Awaiter(LockSet& set, GroupedLockId id, GroupedLockMode mode)
        : set_(set), id_(id), mode_(mode) {}

    LockSet& set_;
    GroupedLockId id_;
    GroupedLockMode mode_;
    Entry* entry_ = nullptr;
    Awaiter* previous_ = nullptr;
    Awaiter* next_ = nullptr;
    std::coroutine_handle<> resume_{};
    bool started_ = false;
    bool queued_ = false;
    bool granted_ = false;
    bool scheduled_ = false;
  };

  // enqueue_ must not throw or resume inline; it schedules on this owner.
  // Deferring execution lets a release install a complete compatible prefix
  // before any reader runs.
  // The alternate constructor also supports deterministic schedulers in tests.
  struct ResumeQueue {
    void* context_;
    void (*enqueue_)(void*, std::coroutine_handle<>);
  };
  explicit GroupedLockTable(bycorf::Worker& worker);
  explicit GroupedLockTable(ResumeQueue queue);
  ~GroupedLockTable();
  GroupedLockTable(const GroupedLockTable&) = delete;
  GroupedLockTable& operator=(const GroupedLockTable&) = delete;

  // Returns an empty set; the caller already owns its parent-key protection.
  // The set must stay at a stable address while any acquisition is pending.
  [[nodiscard]] LockSet MakeLockSet(GroupedLockObject object);
  std::size_t size() const noexcept { return entries_.size(); }

 private:
  struct Key {
    GroupedLockObject object_;
    GroupedLockId id_;
    bool operator==(const Key&) const noexcept = default;

    template <typename H>
    friend H AbslHashValue(H h, const Key& key) {
      return H::combine(std::move(h), key.object_.db_id_, key.object_.fp_,
                        key.object_.incarnation_, key.id_.index_,
                        key.id_.record_.prefix_, key.id_.record_.bits_);
    }
  };
  struct Entry {
    explicit Entry(GroupedLockId id) : id_(id) {}
    GroupedLockId id_;
    std::uint32_t readers_ = 0;
    bool writer_ = false;
    Awaiter* head_ = nullptr;
    Awaiter* tail_ = nullptr;

    bool CanHold(GroupedLockMode mode) const noexcept;
  };

  void Grant(Awaiter& waiter) noexcept;
  void Unlink(Awaiter& waiter) noexcept;
  void Drain(Entry& entry) noexcept;
  void EraseIfFree(GroupedLockObject object, Entry& entry) noexcept;
  void Release(GroupedLockObject object, LockSet::Held held) noexcept;

  ResumeQueue queue_;
  // Node addresses survive growth while requests wait or hold other groups.
  // Entries exist only for held or pending groups, not every resident page.
  absl::node_hash_map<Key, Entry> entries_;
};

}  // namespace lavik::storage
