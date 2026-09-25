/*
 * Copyright (C) 2026 EloqData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

// Cluster owner-side authority re-check plumbing shared by command.cpp and the
// per-type multi-key executors (invariant 1 choke point 2).
//
// The dispatch gate captures AuthorityGuard's complete admission proof on
// CommandRequest (cluster_authority_admission_ / ClusterSlots()). A topology,
// session, lease, or fence change can invalidate that proof while a command
// suspends. Early one-shot and tx::Transaction checks reject stale work before
// expensive handlers run; a storage-neutral MutationPrecondition carries the
// same proof across later suspensions and checks again at logical publication.
//
// Every entry point is a no-op in standalone mode, for replication replay, and
// for requests without a captured admission, so non-cluster behavior is
// unchanged. Definitions live in src/redis/command.cpp.

#include <atomic>
#include <cstdint>
#include <memory>
#include <span>

#include "absl/status/status.h"
#include "lavik/cluster/authority.h"
#include "lavik/cluster/topology.h"
#include "lavik/command.h"
#include "lavik/tx/transaction.h"

namespace lavik {

namespace storage {
class MutationPrecondition;
}  // namespace storage

// Context carried by a transaction's shard validator. It lives on the
// coordinator coroutine frame that owns the transaction (the barrier rule
// guarantees the frame outlives every hop); tripped_ is written from shard
// threads and lets the command layer tell a fence abort apart from an
// ordinary storage failure.
struct ClusterShardValidatorContext {
  std::shared_ptr<const cluster::AuthorityAdmission> admission_;
  std::atomic<bool> tripped_{false};
};

// tx::ShardValidator implementation: re-checks the captured topology and
// finite-lease proof. Runs on the owner shard before the shard callback; must
// not suspend.
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

// Builds the storage-neutral final check carried to keyspace or catalog
// publication seam. The returned object owns the admission proof across
// suspension and is empty for standalone, read, and replication-replay work.
storage::MutationPrecondition ClusterMutationPrecondition(
    const CommandRequest& request);

// Maps a non-serving admission decision to its wire reply. Returns true when
// the decision produced a terminal reply; false when it admits local
// execution (kServe/kServeStaleRead). kCloseConnection yields an empty reply
// with the close flag: the outcome is undeterminable, so nothing is written.
bool EmitClusterDecision(const cluster::Decision& decision, bool connection_tls,
                         ReplyBuilder& reply_builder, CommandReply* reply);

// Replaces an inner handler's reply after a final storage re-check failed.
// No prior mutation gets a fresh redirect; any prior mutation makes the
// aggregate outcome indeterminate and closes the connection.
CommandReply FinalizeClusterMutationReply(const CommandRequest& request,
                                          ReplyBuilder& reply_builder,
                                          CommandReply reply);

// Re-admits `slots` against the current cache and produces the standard wire
// answer for a write that provably never executed. A serve decision here
// means the authority flipped back between the failing check and this call;
// the execution outcome is then genuinely undeterminable, so the connection
// closes instead of inventing an answer.
CommandReply ClusterAuthorityChangedReply(std::span<const std::uint16_t> slots,
                                          bool connection_tls,
                                          ReplyBuilder& reply_builder,
                                          bool local_group = false);

// The status the transaction validator and the bare-hop re-checks use to
// report a fenced admission; distinct enough that callers never confuse it
// with a storage failure.
absl::Status ClusterAuthorityChangedStatus();
bool IsClusterAuthorityChanged(const absl::Status& status);

}  // namespace lavik
