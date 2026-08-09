#pragma once

#include <atomic>
#include <cassert>
#include <coroutine>
#include <cstdint>
#include <span>

#include "absl/container/inlined_vector.h"
#include "absl/status/statusor.h"
#include "celer/runtime/cross_core.h"
#include "celer/runtime/task.h"
#include "keylane/storage/format.h"
#include "keylane/tx/fingerprint.h"
#include "keylane/tx/tx_queue.h"

namespace keylane::tx {

class TxShard;

// One key of a transaction: the full digest for engine access, the
// fingerprint, database, and mode for locking, and the argument index it
// came from so shard callbacks can find paired values and reply slots.
struct TxKey {
  storage::Digest digest_;
  LockFp fp_ = 0;
  std::uint32_t arg_index_ = 0;
  LockMode mode_ = LockMode::kShared;
  std::uint8_t db_ = 0;
};

// The per-shard view handed to a shard callback: this shard's keys in
// argument order, duplicates included (locking is deduplicated separately).
struct ShardSlice {
  std::span<const TxKey> keys_;
};

// Shard callbacks are plain function pointers with a caller-owned context so
// the hot path never heap-allocates a closure. They run on the owning shard
// with all of the transaction's holds acquired and may suspend on disk I/O.
using ShardCallback = celer::Task<absl::Status> (*)(void* ctx,
                                                    const ShardSlice& slice);

// A multi-key transaction, embedded in the coordinator coroutine's frame.
//
// Lifecycle: Begin -> AddKey... -> Seal -> [Schedule ->] Execute(release).
// Single-shard transactions skip Schedule entirely: Execute hops to the
// owner and takes the fast-path key-set guard there, never touching the
// global txid counter. Multi-shard transactions draw a txid, run a schedule
// round on every shard (recording lock intents and taking a txid-ordered
// queue position; the reorder rule may fail the round, which cancels and
// retries with a fresh txid), then execute hops: each hop arms every shard
// and waits on a barrier until every shard ran its slice.
//
// Lifetime rule: the coordinator awaits a barrier after every round it
// starts, and a shard's barrier decrement is its last access to this object,
// so the frame-embedded transaction cannot dangle.
class Transaction {
 public:
  Transaction() = default;
  Transaction(const Transaction&) = delete;
  Transaction& operator=(const Transaction&) = delete;

  // Owner is the key's home worker (StorageEngine::OwnerForKey).
  void AddKey(unsigned owner, std::uint8_t db, const storage::Digest& digest,
              std::uint32_t arg_index, LockMode mode);

  // Groups keys by shard and builds the deduplicated lock sets. No keys may
  // be added afterwards (spans into the internal vectors are handed out).
  void Seal();

  bool single_shard() const { return shards_.size() == 1; }
  std::size_t shard_count() const { return shards_.size(); }

  // Multi-shard only; no-op for single-shard transactions.
  celer::Task<absl::Status> Schedule();

  // Runs `cb` on every shard's slice. `release` drops all locks and queue
  // positions once the hop completes. Single-shard transactions currently
  // require release == true (multi-hop lands with MULTI/EXEC).
  celer::Task<absl::Status> Execute(ShardCallback cb, void* ctx, bool release);

  // Final no-op hop that releases every shard's locks and queue position.
  celer::Task<absl::Status> Release();

  bool releasing() const { return releasing_; }

  // Shard-side entry points (shard thread only).
  celer::Task<absl::Status> InvokeCallback(std::uint16_t shard_slot);
  void CompleteShardRound();
  void SetShardStatus(std::uint16_t shard_slot, absl::Status status);

 private:
  enum class Phase : std::uint8_t { kSchedule, kCancel, kArm };

  struct ShardData;

  struct ShardMsg : celer::RemoteWork {
    ShardData* sd_ = nullptr;
  };

  struct ShardData {
    ShardMsg msg_;
    Transaction* tx_ = nullptr;
    TxWaiter node_;
    absl::Status status_;
    std::uint16_t shard_id_ = 0;
    std::uint16_t key_begin_ = 0;
    std::uint16_t key_count_ = 0;
    std::uint16_t lock_begin_ = 0;
    std::uint16_t lock_count_ = 0;
    Phase phase_ = Phase::kSchedule;
    bool schedule_failed_ = false;
    bool granted_ = false;
  };

  struct RoundAwaiter {
    Transaction* tx_;
    Phase phase_;

    bool await_ready() const noexcept { return tx_->RoundTargets(phase_) == 0; }
    void await_suspend(std::coroutine_handle<> handle);
    void await_resume() const noexcept {}
  };

  friend struct RoundAwaiter;
  friend void StartTransactionHop(TxShard& shard, TxWaiter* node);

  std::uint32_t RoundTargets(Phase phase) const;
  bool InRound(const ShardData& sd, Phase phase) const;
  static void ShardPhaseEntry(celer::RemoteWork* base);
  static void RunShardPhase(ShardData* sd);
  static void ScheduleInShard(ShardData* sd);
  static void CancelInShard(ShardData* sd);
  static void ArmInShard(ShardData* sd);
  ShardSlice Slice(const ShardData& sd) const;
  celer::Task<absl::Status> ExecuteSingleShard();

  bool releasing_ = false;
  bool scheduled_ = false;
  std::uint64_t txid_ = 0;
  ShardCallback cb_ = nullptr;
  void* cb_ctx_ = nullptr;
  absl::InlinedVector<TxKey, 4> keys_;
  absl::InlinedVector<std::uint16_t, 4> owners_;
  absl::InlinedVector<KeyRef, 4> lock_refs_;
  absl::InlinedVector<ShardData, 2> shards_;
  std::uint64_t schedule_retries_ = 0;

  // The only cross-thread words on the hop path.
  std::atomic<std::uint32_t> barrier_{0};
  std::coroutine_handle<> coord_handle_;
  celer::WorkerId coord_worker_ = 0;
};

// Called by TxShard::Poll when an armed transaction entry reaches the head
// of the queue with its holds acquired. Defined in transaction.cpp.
void StartTransactionHop(TxShard& shard, TxWaiter* node);

}  // namespace keylane::tx
