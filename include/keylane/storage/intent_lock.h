#pragma once

#include <cassert>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "celer/runtime/worker.h"
#include "keylane/storage/format.h"

namespace keylane::storage {

enum class IntentLockMode : std::uint8_t {
  kShared,
  kExclusive,
};

// Per-worker coroutine intent locks. All methods run on the owning worker.
// Awaiters retain only a digest; no flat_hash_map iterator, reference, or value
// pointer escapes an uninterrupted method call, so rehashing is harmless.
class IntentLockTable {
 public:
  class Awaiter;

  class Guard {
   public:
    Guard() = default;
    Guard(IntentLockTable* table, Digest digest, IntentLockMode mode) noexcept
        : table_(table), digest_(digest), mode_(mode) {}

    Guard(const Guard&) = delete;
    Guard& operator=(const Guard&) = delete;

    Guard(Guard&& other) noexcept
        : table_(std::exchange(other.table_, nullptr)),
          digest_(other.digest_),
          mode_(other.mode_) {}

    Guard& operator=(Guard&& other) noexcept {
      if (this != &other) {
        Reset();
        table_ = std::exchange(other.table_, nullptr);
        digest_ = other.digest_;
        mode_ = other.mode_;
      }
      return *this;
    }

    ~Guard() { Reset(); }

    void Reset() noexcept {
      if (table_ != nullptr) {
        IntentLockTable* table = std::exchange(table_, nullptr);
        table->Release(digest_, mode_);
      }
    }

   private:
    IntentLockTable* table_ = nullptr;
    Digest digest_{};
    IntentLockMode mode_ = IntentLockMode::kShared;
  };

  class Awaiter {
   public:
    Awaiter(IntentLockTable* table, Digest digest, IntentLockMode mode) noexcept
        : table_(table), digest_(digest), mode_(mode) {}

    bool await_ready() noexcept {
      granted_ = table_->TryAcquire(digest_, mode_);
      return granted_;
    }

    void await_suspend(std::coroutine_handle<> awaiting) {
      awaiting_ = awaiting;
      table_->Queue(this);
    }

    Guard await_resume() noexcept {
      assert(granted_);
      return Guard(table_, digest_, mode_);
    }

   private:
    friend class IntentLockTable;

    IntentLockTable* table_;
    Digest digest_;
    IntentLockMode mode_;
    std::coroutine_handle<> awaiting_{};
    bool granted_ = false;
  };

  IntentLockTable() = default;
  IntentLockTable(const IntentLockTable&) = delete;
  IntentLockTable& operator=(const IntentLockTable&) = delete;

  void Bind(celer::Worker& worker) noexcept { worker_ = &worker; }

  Awaiter Acquire(Digest digest, IntentLockMode mode) noexcept {
    return Awaiter(this, digest, mode);
  }

  std::size_t size() const noexcept { return locks_.size(); }

 private:
  struct LockState {
    unsigned active_shared = 0;
    bool active_exclusive = false;
    std::deque<Awaiter*> waiters;
  };

  bool TryAcquire(const Digest& digest, IntentLockMode mode) {
    auto [it, inserted] = locks_.try_emplace(digest);
    (void)inserted;
    LockState& state = it->second;
    if (!state.waiters.empty()) {
      return false;
    }
    if (mode == IntentLockMode::kShared) {
      if (state.active_exclusive) {
        return false;
      }
      ++state.active_shared;
      return true;
    }
    if (state.active_exclusive || state.active_shared != 0) {
      return false;
    }
    state.active_exclusive = true;
    return true;
  }

  void Queue(Awaiter* waiter) {
    auto [it, inserted] = locks_.try_emplace(waiter->digest_);
    (void)inserted;
    it->second.waiters.push_back(waiter);
  }

  void Release(const Digest& digest, IntentLockMode mode) noexcept {
    auto found = locks_.find(digest);
    assert(found != locks_.end());
    LockState& state = found->second;
    if (mode == IntentLockMode::kShared) {
      assert(state.active_shared != 0);
      --state.active_shared;
    } else {
      assert(state.active_exclusive);
      state.active_exclusive = false;
    }

    if (!state.active_exclusive && state.active_shared == 0 &&
        !state.waiters.empty()) {
      if (state.waiters.front()->mode_ == IntentLockMode::kExclusive) {
        Awaiter* waiter = state.waiters.front();
        state.waiters.pop_front();
        state.active_exclusive = true;
        waiter->granted_ = true;
        worker_->Enqueue(waiter->awaiting_);
      } else {
        while (!state.waiters.empty() &&
               state.waiters.front()->mode_ == IntentLockMode::kShared) {
          Awaiter* waiter = state.waiters.front();
          state.waiters.pop_front();
          ++state.active_shared;
          waiter->granted_ = true;
          worker_->Enqueue(waiter->awaiting_);
        }
      }
    }

    if (!state.active_exclusive && state.active_shared == 0 &&
        state.waiters.empty()) {
      locks_.erase(found);
    }

  }

  celer::Worker* worker_ = nullptr;
  absl::flat_hash_map<Digest, LockState, DigestHash> locks_;
};

}  // namespace keylane::storage
