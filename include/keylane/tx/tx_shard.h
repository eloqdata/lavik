#pragma once

#include <array>
#include <atomic>
#include <cassert>
#include <coroutine>
#include <cstdint>
#include <memory>
#include <span>
#include <utility>

#include "absl/container/inlined_vector.h"
#include "keylane/storage/format.h"
#include "keylane/tx/intent_lock.h"
#include "keylane/tx/tx_queue.h"

namespace celer {
class Worker;
}  // namespace celer

namespace keylane::tx {

// Per-worker transaction scheduling context: the VLL lock tables (one per
// logical DB), the txid-ordered queue, and the poll loop that starts the
// queue head once it is hold-compatible. All state is single-threaded on the
// owning worker; the only shared word in the system is TxRuntime::next_txid,
// touched exclusively by contended or multi-shard acquisitions.
class TxShard {
 public:
  class Awaiter;

  // RAII over an acquired key set: releases holds and intents and re-polls
  // the queue on destruction. Owns its copy of the key refs.
  class Guard {
   public:
    Guard() = default;
    Guard(TxShard* shard, std::uint8_t db_id, std::span<const KeyRef> keys)
        : shard_(shard), db_id_(db_id), keys_(keys.begin(), keys.end()) {}

    Guard(const Guard&) = delete;
    Guard& operator=(const Guard&) = delete;

    Guard(Guard&& other) noexcept
        : shard_(std::exchange(other.shard_, nullptr)),
          db_id_(other.db_id_),
          keys_(std::move(other.keys_)) {}

    Guard& operator=(Guard&& other) noexcept {
      if (this != &other) {
        Reset();
        shard_ = std::exchange(other.shard_, nullptr);
        db_id_ = other.db_id_;
        keys_ = std::move(other.keys_);
      }
      return *this;
    }

    ~Guard() { Reset(); }

    void Reset() noexcept {
      if (shard_ != nullptr) {
        TxShard* shard = std::exchange(shard_, nullptr);
        shard->Release(db_id_, keys_);
      }
    }

   private:
    TxShard* shard_ = nullptr;
    std::uint8_t db_id_ = 0;
    absl::InlinedVector<KeyRef, 2> keys_;
  };

  // Awaitable acquisition of a key set on this shard. Fast path: all intents
  // granted at record time -> acquire holds and continue without suspending,
  // never touching the global txid counter. Contended path: keep the recorded
  // intents (they block later barging), draw a txid, enqueue, suspend until
  // Poll starts us as the hold-compatible head.
  class Awaiter {
   public:
    Awaiter(TxShard* shard, std::uint8_t db_id, std::span<const KeyRef> keys)
        : shard_(shard), db_id_(db_id), keys_(keys) {}

    // Single-key form; the ref is stored inline so callers can pass
    // temporaries.
    Awaiter(TxShard* shard, std::uint8_t db_id, KeyRef key)
        : shard_(shard), db_id_(db_id), inline_key_(key),
          keys_(&inline_key_, 1) {}

    Awaiter(const Awaiter&) = delete;
    Awaiter& operator=(const Awaiter&) = delete;

    bool await_ready() {
      granted_ = shard_->TryFastPath(db_id_, keys_);
      return granted_;
    }

    void await_suspend(std::coroutine_handle<> handle) {
      waiter_.txid = shard_->AllocateTxid();
      waiter_.db_id = db_id_;
      waiter_.keys = keys_;
      waiter_.resume = handle;
      shard_->Enqueue(&waiter_);
    }

    Guard await_resume() noexcept {
      return Guard(shard_, db_id_, keys_);
    }

   private:
    TxShard* shard_;
    std::uint8_t db_id_;
    KeyRef inline_key_{};
    std::span<const KeyRef> keys_;
    TxWaiter waiter_;
    bool granted_ = false;
  };

  TxShard() = default;
  TxShard(const TxShard&) = delete;
  TxShard& operator=(const TxShard&) = delete;

  // Bind to the owning worker; queued resumptions go through
  // worker.Enqueue. Unbound (tests only) resumes inline from Poll.
  void Bind(celer::Worker& worker) noexcept { worker_ = &worker; }
  void BindTxidCounter(std::atomic<std::uint64_t>* counter) noexcept {
    next_txid_ = counter;
  }

  // The caller must keep the KeyRef storage alive across the co_await; the
  // set must be duplicate-free.
  Awaiter AcquireKeys(std::uint8_t db_id, std::span<const KeyRef> keys) {
    assert(db_id < storage::kLogicalDatabaseCount);
    return Awaiter(this, db_id, keys);
  }
  Awaiter AcquireKey(std::uint8_t db_id, LockFp fp, LockMode mode) {
    assert(db_id < storage::kLogicalDatabaseCount);
    return Awaiter(this, db_id, KeyRef{fp, mode});
  }

  // Starts the queue head while it is hold-compatible. Called after every
  // release and by the queue machinery; safe to call at any time.
  void Poll();

  LockTable& locks(std::uint8_t db_id) { return locks_[db_id]; }
  std::uint64_t committed_txid() const noexcept { return committed_txid_; }
  std::uint64_t fastpath_runs() const noexcept { return fastpath_runs_; }
  std::uint64_t queued_runs() const noexcept { return queued_runs_; }

 private:
  friend class Awaiter;

  bool TryFastPath(std::uint8_t db_id, std::span<const KeyRef> keys) {
    LockTable& table = locks_[db_id];
    // Intents are recorded even when not granted: they block later barging
    // while this acquisition waits in the queue.
    if (!table.AcquireIntents(keys)) {
      return false;
    }
    // Granted => sole/compatible intent owner => (holds ⊆ intents) no
    // conflicting hold can exist.
    table.AcquireHolds(keys);
    ++fastpath_runs_;
    return true;
  }

  std::uint64_t AllocateTxid() {
    assert(next_txid_ != nullptr);
    return next_txid_->fetch_add(1, std::memory_order_relaxed);
  }

  void Enqueue(TxWaiter* waiter) { queue_.Insert(waiter); }

  void Release(std::uint8_t db_id, std::span<const KeyRef> keys) {
    LockTable& table = locks_[db_id];
    table.ReleaseHolds(keys);
    table.ReleaseIntents(keys);
    Poll();
  }

  std::array<LockTable, storage::kLogicalDatabaseCount> locks_;
  TxQueue queue_;
  std::uint64_t committed_txid_ = 0;
  bool polling_ = false;
  celer::Worker* worker_ = nullptr;
  std::atomic<std::uint64_t>* next_txid_ = nullptr;
  std::uint64_t fastpath_runs_ = 0;
  std::uint64_t queued_runs_ = 0;
};

// Process-wide transaction runtime: one TxShard per worker plus the global
// txid counter — the only cross-thread atomic, off the uncontended path.
class TxRuntime {
 public:
  static void Create(unsigned worker_count);
  static TxRuntime* Get() noexcept;

  TxShard& shard(unsigned worker_id) {
    assert(worker_id < shards_.size());
    return *shards_[worker_id];
  }
  unsigned shard_count() const noexcept {
    return static_cast<unsigned>(shards_.size());
  }

  std::atomic<std::uint64_t> next_txid{1};

 private:
  std::vector<std::unique_ptr<TxShard>> shards_;
};

// The TxShard owned by the calling worker thread.
TxShard& CurrentTxShard();

}  // namespace keylane::tx
