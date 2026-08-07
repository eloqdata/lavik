#pragma once

#include <cassert>
#include <coroutine>
#include <cstdint>
#include <deque>
#include <span>

#include "keylane/tx/fingerprint.h"

namespace keylane::tx {

class Transaction;

// One queued acquisition waiting for its turn on a shard.
//
// Two flavors share the queue so ordering is uniform:
//  - plain waiters (tx == nullptr): live in the awaiter inside the waiting
//    coroutine's frame; Poll removes them from the queue before resuming, and
//    the resumed coroutine owns holds via its Guard.
//  - transaction entries (tx != nullptr): live in the Transaction's per-shard
//    data; they stay queued (holding their position) until the transaction
//    concludes, running one armed hop at a time.
struct TxWaiter {
  std::uint64_t txid = 0;
  std::span<const KeyRef> keys;
  std::coroutine_handle<> resume;
  Transaction* tx = nullptr;
  std::uint16_t shard_slot = 0;
  std::uint8_t db_id = 0;
  bool armed = false;
  bool running = false;
  bool holds_acquired = false;
};

// Per-shard transaction queue, sorted ascending by txid. Single-threaded.
// Mid-queue removal leaves a nullptr tombstone; Front() skips them.
class TxQueue {
 public:
  void Insert(TxWaiter* waiter) {
    assert(waiter != nullptr && waiter->txid != 0);
    // Near-monotonic arrival: scan from the tail. Single-shard lazy txid
    // allocation always lands at the tail (the id was drawn after every id
    // already queued); multi-shard scheduling may insert earlier.
    auto it = queue_.end();
    while (it != queue_.begin()) {
      auto prev = it;
      --prev;
      if (*prev != nullptr && (*prev)->txid < waiter->txid) {
        break;
      }
      it = prev;
    }
    queue_.insert(it, waiter);
  }

  // First live entry, dropping leading tombstones; nullptr when empty.
  TxWaiter* Front() {
    while (!queue_.empty() && queue_.front() == nullptr) {
      queue_.pop_front();
    }
    return queue_.empty() ? nullptr : queue_.front();
  }

  void PopFront() {
    assert(!queue_.empty() && queue_.front() != nullptr);
    queue_.pop_front();
  }

  // Tombstone `waiter` wherever it sits (used by schedule cancellation).
  void Remove(TxWaiter* waiter) {
    for (auto& slot : queue_) {
      if (slot == waiter) {
        slot = nullptr;
        return;
      }
    }
    assert(false && "waiter not found in queue");
  }

  bool Empty() {
    return Front() == nullptr;
  }

  // txid of the last live entry; 0 when empty. Used by the reordering rule.
  std::uint64_t TailTxid() const {
    for (auto it = queue_.rbegin(); it != queue_.rend(); ++it) {
      if (*it != nullptr) {
        return (*it)->txid;
      }
    }
    return 0;
  }

  std::size_t size() const noexcept { return queue_.size(); }

 private:
  std::deque<TxWaiter*> queue_;
};

}  // namespace keylane::tx
