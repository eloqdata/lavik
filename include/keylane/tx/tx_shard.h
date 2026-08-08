#pragma once

#include <array>
#include <atomic>
#include <cassert>
#include <coroutine>
#include <cstdint>
#include <memory>
#include <span>
#include <utility>

#include "absl/container/flat_hash_map.h"
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
    Guard(TxShard* shard, std::span<const KeyRef> keys)
        : shard_(shard), keys_(keys.begin(), keys.end()) {}

    Guard(const Guard&) = delete;
    Guard& operator=(const Guard&) = delete;

    Guard(Guard&& other) noexcept
        : shard_(std::exchange(other.shard_, nullptr)),
          keys_(std::move(other.keys_)) {}

    Guard& operator=(Guard&& other) noexcept {
      if (this != &other) {
        Reset();
        shard_ = std::exchange(other.shard_, nullptr);
        keys_ = std::move(other.keys_);
      }
      return *this;
    }

    ~Guard() { Reset(); }

    void Reset() noexcept {
      if (shard_ != nullptr) {
        TxShard* shard = std::exchange(shard_, nullptr);
        shard->Release(keys_);
      }
    }

   private:
    TxShard* shard_ = nullptr;
    absl::InlinedVector<KeyRef, 2> keys_;
  };

  // Awaitable acquisition of a key set on this shard. Fast path: all intents
  // granted at record time -> acquire holds and continue without suspending,
  // never touching the global txid counter. Contended path: keep the recorded
  // intents (they block later barging), draw a txid, enqueue, suspend until
  // Poll starts us as the hold-compatible head.
  class Awaiter {
   public:
    Awaiter(TxShard* shard, std::span<const KeyRef> keys)
        : shard_(shard), keys_(keys) {}

    // Single-key form; the ref is stored inline so callers can pass
    // temporaries.
    Awaiter(TxShard* shard, KeyRef key)
        : shard_(shard), inline_key_(key), keys_(&inline_key_, 1) {}

    Awaiter(const Awaiter&) = delete;
    Awaiter& operator=(const Awaiter&) = delete;

    bool await_ready() {
      granted_ = shard_->TryFastPath(keys_);
      return granted_;
    }

    void await_suspend(std::coroutine_handle<> handle) {
      waiter_.txid = shard_->AllocateTxid();
      waiter_.keys = keys_;
      waiter_.resume = handle;
      shard_->Enqueue(&waiter_);
    }

    Guard await_resume() noexcept { return Guard(shard_, keys_); }

   private:
    TxShard* shard_;
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
  // set must be duplicate-free per (db, fp).
  Awaiter AcquireKeys(std::span<const KeyRef> keys) {
    return Awaiter(this, keys);
  }
  Awaiter AcquireKey(std::uint8_t db_id, LockFp fp, LockMode mode) {
    assert(db_id < storage::kLogicalDatabaseCount);
    return Awaiter(this, KeyRef{fp, mode, db_id});
  }

  // Starts the queue head while it is hold-compatible. Called after every
  // release and by the queue machinery; safe to call at any time.
  void Poll();

  // Shard-local WATCH registrations (push model): every real keyspace
  // modification marks the watchers of that fingerprint; EXEC checks its own
  // connection's entries after taking its locks. Marks are sticky until the
  // entry is removed (UNWATCH / DISCARD / EXEC / connection close). The
  // liveness snapshot lives with the connection, not here: this entry is
  // per (db, fingerprint), and two of a connection's watched keys may share
  // a fingerprint — one snapshot slot would make the second key's passive
  // expiration invisible.
  void Watch(std::uint8_t db_id, LockFp fp, std::uint64_t conn_id) {
    auto& entries = watches_[db_id][fp];
    for (const WatchEntry& entry : entries) {
      if (entry.conn_id == conn_id) {
        return;  // already registered; the existing marks stay
      }
    }
    entries.push_back(WatchEntry{conn_id, false});
  }

  void MarkWatched(std::uint8_t db_id, LockFp fp) {
    auto& table = watches_[db_id];
    if (table.empty()) {
      return;
    }
    auto it = table.find(fp);
    if (it == table.end()) {
      return;
    }
    for (WatchEntry& entry : it->second) {
      entry.dirty = true;
    }
  }

  void MarkAllWatched(std::uint8_t db_id) {
    for (auto& [fp, entries] : watches_[db_id]) {
      for (WatchEntry& entry : entries) {
        entry.dirty = true;
      }
    }
  }

  // True when the connection's registration exists and no write has marked
  // it. The caller pairs this with its own liveness comparison (passive
  // expiration invalidates like a write, mirroring Redis).
  bool WatchClean(std::uint8_t db_id, LockFp fp,
                  std::uint64_t conn_id) const {
    auto it = watches_[db_id].find(fp);
    if (it == watches_[db_id].end()) {
      return false;
    }
    for (const WatchEntry& entry : it->second) {
      if (entry.conn_id == conn_id) {
        return !entry.dirty;
      }
    }
    return false;
  }

  void Unwatch(std::uint8_t db_id, LockFp fp, std::uint64_t conn_id) {
    auto& table = watches_[db_id];
    auto it = table.find(fp);
    if (it == table.end()) {
      return;
    }
    auto& entries = it->second;
    for (std::size_t i = 0; i < entries.size(); ++i) {
      if (entries[i].conn_id == conn_id) {
        entries[i] = entries.back();
        entries.pop_back();
        break;
      }
    }
    if (entries.empty()) {
      table.erase(it);
    }
  }

  // Key-set lock operations dispatch each ref to its database's table, so a
  // single set (and therefore a single transaction) may span databases.
  bool AcquireIntents(std::span<const KeyRef> keys) {
    bool granted = true;
    for (const KeyRef& key : keys) {
      granted &= locks_[key.db].AcquireIntent(key.fp, key.mode);
    }
    return granted;
  }
  void ReleaseIntents(std::span<const KeyRef> keys) {
    for (const KeyRef& key : keys) {
      locks_[key.db].ReleaseIntent(key.fp, key.mode);
    }
  }
  bool CanHoldAll(std::span<const KeyRef> keys) const {
    for (const KeyRef& key : keys) {
      if (!locks_[key.db].CanHold(key.fp, key.mode)) {
        return false;
      }
    }
    return true;
  }
  void AcquireHolds(std::span<const KeyRef> keys) {
    for (const KeyRef& key : keys) {
      locks_[key.db].AcquireHold(key.fp, key.mode);
    }
  }
  void ReleaseHolds(std::span<const KeyRef> keys) {
    for (const KeyRef& key : keys) {
      locks_[key.db].ReleaseHold(key.fp, key.mode);
    }
  }

  LockTable& locks(std::uint8_t db_id) { return locks_[db_id]; }
  TxQueue& queue() noexcept { return queue_; }
  celer::Worker* worker() const noexcept { return worker_; }
  std::uint64_t committed_txid() const noexcept { return committed_txid_; }
  void PublishCommitted(std::uint64_t txid) noexcept {
    committed_txid_ = std::max(committed_txid_, txid);
  }
  std::uint64_t fastpath_runs() const noexcept { return fastpath_runs_; }
  std::uint64_t queued_runs() const noexcept { return queued_runs_; }

 private:
  friend class Awaiter;

  bool TryFastPath(std::span<const KeyRef> keys) {
    // Intents are recorded even when not granted: they block later barging
    // while this acquisition waits in the queue.
    if (!AcquireIntents(keys)) {
      return false;
    }
    // Granted => sole/compatible intent owner => (holds ⊆ intents) no
    // conflicting hold can exist.
    AcquireHolds(keys);
    ++fastpath_runs_;
    return true;
  }

  std::uint64_t AllocateTxid() {
    assert(next_txid_ != nullptr);
    return next_txid_->fetch_add(1, std::memory_order_relaxed);
  }

  void Enqueue(TxWaiter* waiter) { queue_.Insert(waiter); }

  void Release(std::span<const KeyRef> keys) {
    ReleaseHolds(keys);
    ReleaseIntents(keys);
    Poll();
  }

  struct WatchEntry {
    std::uint64_t conn_id = 0;
    bool dirty = false;
  };

  std::array<LockTable, storage::kLogicalDatabaseCount> locks_;
  std::array<absl::flat_hash_map<LockFp, absl::InlinedVector<WatchEntry, 1>,
                                 LockFpIdentityHash>,
             storage::kLogicalDatabaseCount>
      watches_;
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
  // Multi-shard schedule rounds that failed the reorder rule and retried
  // with a fresh txid.
  std::atomic<std::uint64_t> schedule_retries{0};

 private:
  std::vector<std::unique_ptr<TxShard>> shards_;
};

// The TxShard owned by the calling worker thread.
TxShard& CurrentTxShard();

}  // namespace keylane::tx
