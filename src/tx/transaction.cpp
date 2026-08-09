#include "keylane/tx/transaction.h"

#include <algorithm>

#include "celer/runtime/worker.h"
#include "keylane/tx/tx_shard.h"

namespace keylane::tx {

using celer::Task;

void Transaction::AddKey(unsigned owner, std::uint8_t db,
                         const storage::Digest& digest, std::uint32_t arg_index,
                         LockMode mode) {
  assert(shards_.empty() && "AddKey after Seal");
  keys_.push_back(TxKey{
      .digest = digest,
      .fp = FingerprintOf(digest),
      .arg_index = arg_index,
      .mode = mode,
      .db = db,
  });
  owners_.push_back(static_cast<std::uint16_t>(owner));
}

void Transaction::Seal() {
  assert(!keys_.empty());
  // Group keys by owner, preserving argument order within each shard.
  absl::InlinedVector<std::uint16_t, 4> distinct;
  for (std::uint16_t owner : owners_) {
    if (std::find(distinct.begin(), distinct.end(), owner) == distinct.end()) {
      distinct.push_back(owner);
    }
  }
  absl::InlinedVector<TxKey, 4> grouped;
  grouped.reserve(keys_.size());
  shards_.resize(distinct.size());
  for (std::size_t s = 0; s < distinct.size(); ++s) {
    ShardData& sd = shards_[s];
    sd.tx = this;
    sd.msg.sd = &sd;
    sd.msg.run_fn = &Transaction::ShardPhaseEntry;
    sd.shard_id = distinct[s];
    sd.key_begin = static_cast<std::uint16_t>(grouped.size());
    for (std::size_t i = 0; i < keys_.size(); ++i) {
      if (owners_[i] == distinct[s]) {
        grouped.push_back(keys_[i]);
      }
    }
    sd.key_count = static_cast<std::uint16_t>(grouped.size() - sd.key_begin);
    // Deduplicate the lock set: one ref per (db, fingerprint), exclusive if
    // any occurrence writes.
    sd.lock_begin = static_cast<std::uint16_t>(lock_refs_.size());
    for (std::size_t i = sd.key_begin; i < grouped.size(); ++i) {
      const TxKey& key = grouped[i];
      bool merged = false;
      for (std::size_t j = sd.lock_begin; j < lock_refs_.size(); ++j) {
        if (lock_refs_[j].fp == key.fp && lock_refs_[j].db == key.db) {
          if (key.mode == LockMode::kExclusive) {
            lock_refs_[j].mode = LockMode::kExclusive;
          }
          merged = true;
          break;
        }
      }
      if (!merged) {
        lock_refs_.push_back(KeyRef{key.fp, key.mode, key.db});
      }
    }
    sd.lock_count =
        static_cast<std::uint16_t>(lock_refs_.size() - sd.lock_begin);
  }
  keys_ = std::move(grouped);
  // Vectors are final now; hand out the stable spans.
  for (std::size_t s = 0; s < shards_.size(); ++s) {
    ShardData& sd = shards_[s];
    sd.node.tx = this;
    sd.node.shard_slot = static_cast<std::uint16_t>(s);
    sd.node.keys = std::span<const KeyRef>(lock_refs_.data() + sd.lock_begin,
                                           sd.lock_count);
  }
}

ShardSlice Transaction::Slice(const ShardData& sd) const {
  return ShardSlice{
      .keys = std::span<const TxKey>(keys_.data() + sd.key_begin, sd.key_count),
  };
}

std::uint32_t Transaction::RoundTargets(Phase phase) const {
  std::uint32_t targets = 0;
  for (const ShardData& sd : shards_) {
    targets += InRound(sd, phase) ? 1 : 0;
  }
  return targets;
}

bool Transaction::InRound(const ShardData& sd, Phase phase) const {
  // Cancel rounds only visit shards whose schedule succeeded.
  return phase != Phase::kCancel || !sd.schedule_failed;
}

void Transaction::RoundAwaiter::await_suspend(std::coroutine_handle<> handle) {
  tx->coord_handle_ = handle;
  tx->coord_worker_ = celer::ThisWorker().id;
  tx->barrier_.store(tx->RoundTargets(phase), std::memory_order_release);
  for (ShardData& sd : tx->shards_) {
    if (!tx->InRound(sd, phase)) {
      continue;
    }
    sd.phase = phase;
    if (sd.shard_id == celer::ThisWorker().id) {
      RunShardPhase(&sd);
    } else {
      celer::PostRequest(celer::ThisWorker().cross_core, sd.shard_id, &sd.msg);
    }
  }
}

void Transaction::ShardPhaseEntry(celer::RemoteWork* base) {
  auto* msg = static_cast<ShardMsg*>(base);
  // Rounds complete through the transaction barrier, never through the
  // cross-core reply leg.
  msg->reply_deferred = true;
  RunShardPhase(msg->sd);
}

void Transaction::RunShardPhase(ShardData* sd) {
  switch (sd->phase) {
    case Phase::kSchedule:
      ScheduleInShard(sd);
      sd->tx->CompleteShardRound();
      return;
    case Phase::kCancel:
      CancelInShard(sd);
      sd->tx->CompleteShardRound();
      return;
    case Phase::kArm:
      // The barrier is decremented when the hop callback finishes.
      ArmInShard(sd);
      return;
  }
}

void Transaction::ScheduleInShard(ShardData* sd) {
  Transaction* tx = sd->tx;
  TxShard& shard = CurrentTxShard();
  sd->schedule_failed = false;
  // Stale txid: a later transaction already committed on this shard, so this
  // position in the serial order is in the past.
  if (tx->txid_ <= shard.committed_txid()) {
    sd->schedule_failed = true;
    return;
  }
  sd->node.txid = tx->txid_;
  sd->granted = shard.AcquireIntents(sd->node.keys);
  const std::uint64_t tail = shard.queue().TailTxid();
  // Reorder rule: inserting before the tail while conflicting is unsound —
  // a later transaction may already have run out of order assuming nothing
  // precedes it. Fail the schedule; the coordinator retries with a fresh,
  // larger txid.
  if (!sd->granted && tail != 0 && tx->txid_ < tail) {
    shard.ReleaseIntents(sd->node.keys);
    sd->schedule_failed = true;
    return;
  }
  shard.queue().Insert(&sd->node);
}

void Transaction::CancelInShard(ShardData* sd) {
  TxShard& shard = CurrentTxShard();
  shard.ReleaseIntents(sd->node.keys);
  shard.queue().Remove(&sd->node);
  sd->granted = false;
  shard.Poll();
}

void Transaction::ArmInShard(ShardData* sd) {
  sd->node.armed = true;
  CurrentTxShard().Poll();
}

Task<absl::Status> Transaction::InvokeCallback(std::uint16_t shard_slot) {
  co_return co_await cb_(cb_ctx_, Slice(shards_[shard_slot]));
}

void Transaction::SetShardStatus(std::uint16_t shard_slot,
                                 absl::Status status) {
  shards_[shard_slot].status = std::move(status);
}

void Transaction::CompleteShardRound() {
  if (barrier_.fetch_sub(1, std::memory_order_acq_rel) != 1) {
    return;
  }
  if (celer::ThisWorker().id == coord_worker_) {
    celer::ThisWorker().self->Enqueue(coord_handle_);
    return;
  }
  celer::PostNotification(
      celer::ThisWorker().cross_core, coord_worker_,
      celer::RemoteNotification{
          .context = this,
          .value = 0,
          .run_fn =
              [](void* context, std::uint64_t) noexcept {
                celer::ThisWorker().self->Enqueue(
                    static_cast<Transaction*>(context)->coord_handle_);
              },
      });
}

Task<absl::Status> Transaction::Schedule() {
  assert(!shards_.empty() && "Seal before Schedule");
  if (single_shard()) {
    co_return absl::OkStatus();
  }
  for (;;) {
    txid_ = TxRuntime::Get()->next_txid.fetch_add(1, std::memory_order_relaxed);
    co_await RoundAwaiter{this, Phase::kSchedule};
    bool failed = false;
    for (const ShardData& sd : shards_) {
      failed |= sd.schedule_failed;
    }
    if (!failed) {
      scheduled_ = true;
      co_return absl::OkStatus();
    }
    co_await RoundAwaiter{this, Phase::kCancel};
    ++schedule_retries_;
    TxRuntime::Get()->schedule_retries.fetch_add(1, std::memory_order_relaxed);
  }
}

Task<absl::Status> Transaction::ExecuteSingleShard() {
  // The whole key set lives on one shard: hop there and take the fast-path
  // key-set guard. No txid, no queue entry, no barrier beyond the hop.
  const unsigned owner = shards_[0].shard_id;
  co_return co_await celer::SubmitTaskTo(owner, [this]() -> Task<absl::Status> {
    ShardData& sd = shards_[0];
    auto guard = co_await CurrentTxShard().AcquireKeys(sd.node.keys);
    co_return co_await cb_(cb_ctx_, Slice(sd));
  });
}

Task<absl::Status> Transaction::Execute(ShardCallback cb, void* ctx,
                                        bool release) {
  assert(!shards_.empty() && "Seal before Execute");
  cb_ = cb;
  cb_ctx_ = ctx;
  releasing_ = release;
  if (single_shard()) {
    assert(release && "single-shard multi-hop lands with MULTI/EXEC");
    co_return co_await ExecuteSingleShard();
  }
  assert(scheduled_ && "Schedule before Execute");
  for (ShardData& sd : shards_) {
    sd.status = absl::OkStatus();
  }
  co_await RoundAwaiter{this, Phase::kArm};
  for (ShardData& sd : shards_) {
    if (!sd.status.ok()) {
      co_return sd.status;
    }
  }
  co_return absl::OkStatus();
}

namespace {

Task<absl::Status> NoopShardCallback(void*, const ShardSlice&) {
  co_return absl::OkStatus();
}

}  // namespace

Task<absl::Status> Transaction::Release() {
  co_return co_await Execute(&NoopShardCallback, nullptr, true);
}

namespace {

Task<absl::Status> RunShardHop(TxShard* shard, TxWaiter* node) {
  Transaction* tx = node->tx;
  absl::Status status = co_await tx->InvokeCallback(node->shard_slot);
  tx->SetShardStatus(node->shard_slot, std::move(status));
  // Non-suspending epilogue on the shard thread.
  node->running = false;
  if (tx->releasing()) {
    shard->ReleaseHolds(node->keys);
    shard->ReleaseIntents(node->keys);
    node->holds_acquired = false;
    shard->queue().Remove(node);
    shard->Poll();
  }
  // Barrier decrement is the last access to the transaction.
  tx->CompleteShardRound();
  co_return absl::OkStatus();
}

}  // namespace

void StartTransactionHop(TxShard& shard, TxWaiter* node) {
  assert(shard.worker() != nullptr);
  shard.worker()->Spawn(RunShardHop(&shard, node));
}

}  // namespace keylane::tx
