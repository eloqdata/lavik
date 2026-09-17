#include "keylane/tx/tx_shard.h"

#include <algorithm>

#include "bycorf/runtime/worker.h"
#include "keylane/tx/transaction.h"

namespace keylane::tx {

namespace {

TxRuntime* g_runtime = nullptr;

}  // namespace

void TxShard::EnqueueBypassReady(TxWaiter* waiter) {
  assert(waiter != nullptr && waiter->tx_ != nullptr);
  assert(waiter->armed_ && !waiter->running_);
  bypass_ready_.push_back(waiter);
}

TxWaiter* TxShard::PopBypassReady() noexcept {
  if (bypass_ready_.empty()) return nullptr;
  TxWaiter* waiter = bypass_ready_.front();
  bypass_ready_.pop_front();
  return waiter;
}

void TxShard::ArmTransaction(TxWaiter* waiter, bool bypass_ordered_queue) {
  assert(waiter != nullptr && waiter->tx_ != nullptr);
  assert(!waiter->armed_ && !waiter->running_);
  waiter->armed_ = true;
  if (bypass_ordered_queue) EnqueueBypassReady(waiter);
  Poll();
}

void TxShard::Poll() {
  if (polling_) {
    return;
  }
  polling_ = true;

  // Every bypass-ready entry also retains its ordered queue position. Keep
  // the uncontended plain/single-shard path identical to the old scheduler:
  // one empty ordered-queue check, then return without touching the
  // multi-shard-only ready queue.
  if (queue_.Front() == nullptr) {
    polling_ = false;
    return;
  }

  // A granted multi-shard entry is independent of everything that was
  // registered before it on this shard. Dispatch all such armed hops first.
  // Their intents prevent later conflicting entries from taking this path,
  // and CanHoldAll protects against a callback that is still suspended with
  // an actual hold.
  while (TxWaiter* ready = PopBypassReady()) {
    assert(ready->tx_ != nullptr);
    if (!ready->armed_ || ready->running_) continue;
    if (!ready->holds_acquired_ && !CanHoldAll(ready->keys_)) {
      // This should only be transient (for example a callback already
      // executing when the intent was recorded). Leave it armed; the holder's
      // release will poll again.
      EnqueueBypassReady(ready);
      break;
    }
    committed_txid_ = std::max(committed_txid_, ready->txid_);
    if (!ready->holds_acquired_) {
      AcquireHolds(ready->keys_);
      ready->holds_acquired_ = true;
    }
    ready->armed_ = false;
    ready->running_ = true;
    ++queued_runs_;
    StartTransactionHop(*this, ready);
  }

  while (true) {
    TxWaiter* head = queue_.Front();
    if (head == nullptr) {
      break;
    }
    if (head->tx_ != nullptr) {
      // Transaction entry: stays queued (holding its position) until the
      // transaction's release hop; runs one armed hop at a time. Holds are
      // acquired once and retained across hops.
      if (head->running_ || !head->armed_) {
        break;
      }
      if (!head->holds_acquired_ && !CanHoldAll(head->keys_)) {
        break;
      }
      committed_txid_ = std::max(committed_txid_, head->txid_);
      if (!head->holds_acquired_) {
        AcquireHolds(head->keys_);
        head->holds_acquired_ = true;
      }
      head->armed_ = false;
      head->running_ = true;
      ++queued_runs_;
      StartTransactionHop(*this, head);
      break;
    }
    if (!CanHoldAll(head->keys_)) {
      // A suspended runner still holds a conflicting key; its release will
      // re-poll. The head's recorded intents guarantee no new conflicting
      // holder can appear, so the wait set only drains.
      break;
    }
    // Publish before the head can run (its callback may suspend at any
    // point after resumption).
    committed_txid_ = std::max(committed_txid_, head->txid_);
    AcquireHolds(head->keys_);
    ++queued_runs_;
    // Remove before resuming: once resumed, the waiter (living in the
    // suspended coroutine's frame) is no longer referenced by the queue.
    std::coroutine_handle<> resume = head->resume_;
    queue_.PopFront();
    if (worker_ != nullptr) {
      worker_->Enqueue(resume);
      // The resumed head releases through its Guard, which re-polls; later
      // entries wait for its holds anyway.
      break;
    }
    // Unbound (test) mode: run inline. The guard's release re-enters Poll and
    // bails on polling_, so loop again for the next head.
    resume.resume();
  }
  polling_ = false;
}

void TxRuntime::Create(unsigned worker_count) {
  assert(g_runtime == nullptr);
  auto* runtime = new TxRuntime();
  runtime->shards_.reserve(worker_count);
  for (unsigned i = 0; i < worker_count; ++i) {
    auto shard = std::make_unique<TxShard>();
    shard->BindTxidCounter(&runtime->next_txid_);
    runtime->shards_.push_back(std::move(shard));
  }
  g_runtime = runtime;
}

TxRuntime* TxRuntime::Get() noexcept { return g_runtime; }

TxShard& CurrentTxShard() {
  return TxRuntime::Get()->shard(bycorf::ThisWorker().id_);
}

}  // namespace keylane::tx
