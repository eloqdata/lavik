#pragma once

// Cluster owner-side authority re-check plumbing shared by command.cpp and the
// per-type multi-key executors (invariant 1 choke point 2).
//
// The dispatch gate captures the ServingState a request was admitted against
// on CommandRequest (cluster_admitted_state_ / ClusterSlots()). A topology
// reload (SIGHUP) can fence that admission while a command suspends on
// scheduling or I/O, so every mutation path re-checks the captured per-group
// authority tokens against the current cache right before writing. The
// tx::Transaction hook covers shard-callback executors; executors whose
// mutation runs in a bare SubmitTaskTo hop use the one-shot re-check.
//
// Every entry point is a no-op in standalone mode, for replication replay, and
// for requests without a captured admission, so non-cluster behavior is
// unchanged. Definitions live in src/redis/command.cpp.

#include <atomic>
#include <cstdint>
#include <memory>
#include <span>

#include "absl/status/status.h"
#include "keylane/cluster/authority.h"
#include "keylane/cluster/topology.h"
#include "keylane/command.h"
#include "keylane/tx/transaction.h"

namespace keylane {

// Context carried by a transaction's shard validator. It lives on the
// coordinator coroutine frame that owns the transaction (the barrier rule
// guarantees the frame outlives every hop); tripped_ is written from shard
// threads and lets the command layer tell a fence abort apart from an
// ordinary storage failure.
struct ClusterShardValidatorContext {
  std::shared_ptr<const cluster::ServingState> admitted_;
  std::span<const std::uint16_t> slots_;
  std::atomic<bool> tripped_{false};
};

// tx::ShardValidator implementation: compares the captured per-group authority
// tokens against the current cache. Runs on the owner shard before the shard
// callback; must not suspend.
absl::Status ValidateClusterShardAuthority(void* ctx, unsigned shard);

// Installs the per-shard re-check on a write transaction when the request was
// cluster-admitted with captured state. No-op otherwise; such transactions
// never branch on the hook at all.
void InstallClusterShardValidator(tx::Transaction& transaction,
                                  const CommandRequest& request,
                                  ClusterShardValidatorContext& context);

// Wire answer for a validator-aborted write transaction. A single-shard
// transaction provably ran nothing when the validator tripped (the hook fires
// before the shard callback), so a fresh re-admission answers honestly. A
// multi-shard transaction may have mutated a shard whose validator raced the
// fence; the outcome is then undeterminable and the connection closes.
CommandReply ClusterValidatorFailureReply(
    const tx::Transaction& transaction,
    const ClusterShardValidatorContext& context, bool connection_tls,
    ReplyBuilder& reply_builder);

// One-shot re-check for mutation paths that do not run inside a transaction
// callback (single-hop SubmitTaskTo executors such as the list single-shard
// fast path or SORT STORE's destination replace). Returns OkStatus when the
// captured admission still holds.
absl::Status RecheckClusterRequestAuthority(const CommandRequest& request);

// Re-admits `slots` against the current cache and produces the standard wire
// answer for a write that provably never executed. A serve decision here
// means the authority flipped back between the failing check and this call;
// the execution outcome is then genuinely undeterminable, so the connection
// closes instead of inventing an answer.
CommandReply ClusterAuthorityChangedReply(std::span<const std::uint16_t> slots,
                                          bool connection_tls,
                                          ReplyBuilder& reply_builder);

// The status the transaction validator and the bare-hop re-checks use to
// report a fenced admission; distinct enough that callers never confuse it
// with a storage failure.
absl::Status ClusterAuthorityChangedStatus();
bool IsClusterAuthorityChanged(const absl::Status& status);

}  // namespace keylane
