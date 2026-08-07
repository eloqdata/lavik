#include "keylane/tx/tx_shard.h"

#include <algorithm>

#include "celer/runtime/worker.h"
#include "keylane/tx/transaction.h"

namespace keylane::tx {

namespace {

TxRuntime* g_runtime = nullptr;

}  // namespace

void TxShard::Poll() {
  if (polling_) {
    return;
  }
  polling_ = true;
  while (true) {
    TxWaiter* head = queue_.Front();
    if (head == nullptr) {
      break;
    }
    if (head->tx != nullptr) {
      // Transaction entry: stays queued (holding its position) until the
      // transaction concludes; runs one armed hop at a time. Holds are
      // acquired once and retained across hops.
      if (head->running || !head->armed) {
        break;
      }
      if (!head->holds_acquired && !CanHoldAll(head->keys)) {
        break;
      }
      committed_txid_ = std::max(committed_txid_, head->txid);
      if (!head->holds_acquired) {
        AcquireHolds(head->keys);
        head->holds_acquired = true;
      }
      head->armed = false;
      head->running = true;
      ++queued_runs_;
      StartTransactionHop(*this, head);
      break;
    }
    if (!CanHoldAll(head->keys)) {
      // A suspended runner still holds a conflicting key; its release will
      // re-poll. The head's recorded intents guarantee no new conflicting
      // holder can appear, so the wait set only drains.
      break;
    }
    // Publish before the head can run (its callback may suspend at any
    // point after resumption).
    committed_txid_ = std::max(committed_txid_, head->txid);
    AcquireHolds(head->keys);
    ++queued_runs_;
    // Remove before resuming: once resumed, the waiter (living in the
    // suspended coroutine's frame) is no longer referenced by the queue.
    std::coroutine_handle<> resume = head->resume;
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
    shard->BindTxidCounter(&runtime->next_txid);
    runtime->shards_.push_back(std::move(shard));
  }
  g_runtime = runtime;
}

TxRuntime* TxRuntime::Get() noexcept { return g_runtime; }

TxShard& CurrentTxShard() {
  return TxRuntime::Get()->shard(celer::ThisWorker().id);
}

}  // namespace keylane::tx
