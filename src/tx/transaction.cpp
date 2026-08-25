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
      .digest_ = digest,
      .fp_ = FingerprintOf(digest),
      .arg_index_ = arg_index,
      .mode_ = mode,
      .db_ = db,
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
    sd.tx_ = this;
    sd.msg_.sd_ = &sd;
    sd.msg_.run_fn_ = &Transaction::ShardPhaseEntry;
    sd.shard_id_ = distinct[s];
    sd.key_begin_ = static_cast<std::uint16_t>(grouped.size());
    for (std::size_t i = 0; i < keys_.size(); ++i) {
      if (owners_[i] == distinct[s]) {
        grouped.push_back(keys_[i]);
      }
    }
    sd.key_count_ = static_cast<std::uint16_t>(grouped.size() - sd.key_begin_);
    // Deduplicate the lock set: one ref per (db, fingerprint), exclusive if
    // any occurrence writes.
    sd.lock_begin_ = static_cast<std::uint16_t>(lock_refs_.size());
    for (std::size_t i = sd.key_begin_; i < grouped.size(); ++i) {
      const TxKey& key = grouped[i];
      bool merged = false;
      for (std::size_t j = sd.lock_begin_; j < lock_refs_.size(); ++j) {
        if (lock_refs_[j].fp_ == key.fp_ && lock_refs_[j].db_ == key.db_) {
          if (key.mode_ == LockMode::kExclusive) {
            lock_refs_[j].mode_ = LockMode::kExclusive;
          }
          merged = true;
          break;
        }
      }
      if (!merged) {
        lock_refs_.push_back(KeyRef{key.fp_, key.mode_, key.db_});
      }
    }
    sd.lock_count_ =
        static_cast<std::uint16_t>(lock_refs_.size() - sd.lock_begin_);
  }
  keys_ = std::move(grouped);
  // Vectors are final now; hand out the stable spans.
  for (std::size_t s = 0; s < shards_.size(); ++s) {
    ShardData& sd = shards_[s];
    sd.node_.tx_ = this;
    sd.node_.shard_slot_ = static_cast<std::uint16_t>(s);
    sd.node_.keys_ = std::span<const KeyRef>(lock_refs_.data() + sd.lock_begin_,
                                             sd.lock_count_);
  }
}

ShardSlice Transaction::Slice(const ShardData& sd) const {
  return ShardSlice{
      .keys_ =
          std::span<const TxKey>(keys_.data() + sd.key_begin_, sd.key_count_),
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
  return phase != Phase::kCancel || !sd.schedule_failed_;
}

void Transaction::RoundAwaiter::await_suspend(std::coroutine_handle<> handle) {
  tx_->coord_handle_ = handle;
  tx_->coord_worker_ = celer::ThisWorker().id_;
  tx_->barrier_.store(tx_->RoundTargets(phase_), std::memory_order_release);
  for (ShardData& sd : tx_->shards_) {
    if (!tx_->InRound(sd, phase_)) {
      continue;
    }
    sd.phase_ = phase_;
    if (sd.shard_id_ == celer::ThisWorker().id_) {
      RunShardPhase(&sd);
    } else {
      celer::PostRequest(celer::ThisWorker().cross_core_, sd.shard_id_,
                         &sd.msg_);
    }
  }
}

void Transaction::ShardPhaseEntry(celer::RemoteWork* base) {
  auto* msg = static_cast<ShardMsg*>(base);
  // Rounds complete through the transaction barrier, never through the
  // cross-core reply leg.
  msg->reply_deferred_ = true;
  RunShardPhase(msg->sd_);
}

void Transaction::RunShardPhase(ShardData* sd) {
  switch (sd->phase_) {
    case Phase::kSchedule:
      ScheduleInShard(sd);
      sd->tx_->CompleteShardRound();
      return;
    case Phase::kCancel:
      CancelInShard(sd);
      sd->tx_->CompleteShardRound();
      return;
    case Phase::kArm:
      // The barrier is decremented when the hop callback finishes.
      ArmInShard(sd);
      return;
  }
}

void Transaction::ScheduleInShard(ShardData* sd) {
  Transaction* tx = sd->tx_;
  TxShard& shard = CurrentTxShard();
  sd->schedule_failed_ = false;
  // Stale txid: a later transaction already committed on this shard, so this
  // position in the serial order is in the past.
  if (tx->txid_ <= shard.committed_txid()) {
    sd->schedule_failed_ = true;
    return;
  }
  sd->node_.txid_ = tx->txid_;
  sd->granted_ = shard.AcquireIntents(sd->node_.keys_);
  // A fully granted intent set has no conflict with any transaction already
  // registered on this shard. Preserve that fact for every hop: future
  // conflicting schedulers see these intents and cannot take the bypass.
  const std::uint64_t tail = shard.queue().TailTxid();
  // Reorder rule: inserting before the tail while conflicting is unsound —
  // a later transaction may already have run out of order assuming nothing
  // precedes it. Fail the schedule; the coordinator retries with a fresh,
  // larger txid.
  if (!sd->granted_ && tail != 0 && tx->txid_ < tail) {
    shard.ReleaseIntents(sd->node_.keys_);
    sd->schedule_failed_ = true;
    return;
  }
  shard.queue().Insert(&sd->node_);
}

void Transaction::CancelInShard(ShardData* sd) {
  TxShard& shard = CurrentTxShard();
  shard.ReleaseIntents(sd->node_.keys_);
  shard.queue().Remove(&sd->node_);
  sd->granted_ = false;
  shard.Poll();
}

void Transaction::ArmInShard(ShardData* sd) {
  CurrentTxShard().ArmTransaction(&sd->node_, sd->granted_);
}

Task<absl::Status> Transaction::InvokeCallback(std::uint16_t shard_slot) {
  ShardData& shard = shards_[shard_slot];
  if (!shard.entry_hook_invoked_ && entry_hook_ != nullptr) {
    shard.entry_hook_invoked_ = true;
    entry_hook_(entry_hook_ctx_, shard.shard_id_);
  }
  co_return co_await cb_(cb_ctx_, Slice(shard));
}

std::vector<unsigned> Transaction::shard_ids() const {
  std::vector<unsigned> result;
  result.reserve(shards_.size());
  for (const ShardData& shard : shards_) result.push_back(shard.shard_id_);
  return result;
}

void Transaction::SetShardStatus(std::uint16_t shard_slot,
                                 absl::Status status) {
  shards_[shard_slot].status_ = std::move(status);
}

void Transaction::CompleteShardRound() {
  if (barrier_.fetch_sub(1, std::memory_order_acq_rel) != 1) {
    return;
  }
  if (celer::ThisWorker().id_ == coord_worker_) {
    celer::ThisWorker().self_->Enqueue(coord_handle_);
    return;
  }
  celer::PostNotification(
      celer::ThisWorker().cross_core_, coord_worker_,
      celer::RemoteNotification{
          .context_ = this,
          .value_ = 0,
          .run_fn_ =
              [](void* context, std::uint64_t) noexcept {
                celer::ThisWorker().self_->Enqueue(
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
    txid_ =
        TxRuntime::Get()->next_txid_.fetch_add(1, std::memory_order_relaxed);
    co_await RoundAwaiter{this, Phase::kSchedule};
    bool failed = false;
    for (const ShardData& sd : shards_) {
      failed |= sd.schedule_failed_;
    }
    if (!failed) {
      scheduled_ = true;
      co_return absl::OkStatus();
    }
    co_await RoundAwaiter{this, Phase::kCancel};
    ++schedule_retries_;
    TxRuntime::Get()->schedule_retries_.fetch_add(1, std::memory_order_relaxed);
  }
}

Task<absl::Status> Transaction::ExecuteSingleShard(bool release) {
  // The whole key set lives on one shard. The first hop takes the fast-path
  // key-set guard; a non-releasing hop leaves it resident on that owner for
  // the next callback. The final hop resets it there. No txid or queue entry.
  const unsigned owner = shards_[0].shard_id_;
  co_return co_await celer::SubmitTaskTo(
      owner, [this, release]() -> Task<absl::Status> {
        ShardData& sd = shards_[0];
        if (!single_shard_guard_.has_value()) {
          single_shard_guard_.emplace(
              co_await CurrentTxShard().AcquireKeys(sd.node_.keys_));
        }
        if (!sd.entry_hook_invoked_ && entry_hook_ != nullptr) {
          sd.entry_hook_invoked_ = true;
          entry_hook_(entry_hook_ctx_, sd.shard_id_);
        }
        absl::Status status = co_await cb_(cb_ctx_, Slice(sd));
        if (release) single_shard_guard_.reset();
        co_return status;
      });
}

Task<absl::Status> Transaction::Execute(ShardCallback cb, void* ctx,
                                        bool release) {
  assert(!shards_.empty() && "Seal before Execute");
  cb_ = cb;
  cb_ctx_ = ctx;
  releasing_ = release;
  if (single_shard()) {
    co_return co_await ExecuteSingleShard(release);
  }
  assert(scheduled_ && "Schedule before Execute");
  for (ShardData& sd : shards_) {
    sd.status_ = absl::OkStatus();
  }
  co_await RoundAwaiter{this, Phase::kArm};
  for (ShardData& sd : shards_) {
    if (!sd.status_.ok()) {
      co_return sd.status_;
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
  return Execute(&NoopShardCallback, nullptr, true);
}

namespace {

Task<absl::Status> RunShardHop(TxShard* shard, TxWaiter* node) {
  Transaction* tx = node->tx_;
  absl::Status status = co_await tx->InvokeCallback(node->shard_slot_);
  tx->SetShardStatus(node->shard_slot_, std::move(status));
  // Non-suspending epilogue on the shard thread.
  node->running_ = false;
  if (tx->releasing()) {
    shard->ReleaseHolds(node->keys_);
    shard->ReleaseIntents(node->keys_);
    node->holds_acquired_ = false;
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
