#pragma once

// MetaStateApply is the metadata control plane's apply dispatcher and the
// pure-function core of ApplyCommitted. MetaStateMachine calls it from
// commit() on every node with the committed command, its raft log index, and
// the ActorContext fields the trusted entry injected at Propose time.
//
// Contract:
//   - DETERMINISTIC PURE FUNCTION of (committed state, command, log index,
//     injected actor fields). It never reads a clock (readable_time is
//     caller-injected text copied verbatim into the audit record), never
//     touches observation state, never does IO, and takes no locks
//     (concurrency control lives in the state machine above).
//   - NEVER FAIL-STOPS on domain input. A cleanly decoded command that
//     violates a domain or cross-store rule is REJECTED: the log index is
//     consumed, an audit record is written, committed state is unchanged.
//     Fail-stop (spdlog::critical + abort inside the stores on broken
//     index/capacity correspondence) remains the stores' wiring-bug semantic;
//     correct wiring — log indexes strictly increasing from 1, with the
//     coordinator reserving audit-window capacity — never reaches it.
//   - AUDIT: every privileged command appends exactly one audit
//     record keyed by its raft log index, accepted or rejected, carrying the
//     injected actor principal, a deterministic command summary, the verdict,
//     and the injected readable time. Replay of the same log index reproduces
//     the identical record and the audit store's Append is then an idempotent
//     no-op, so replay never grows the window.
//   - REPLAY IDEMPOTENCY: re-applying the same (log_index, command) pair
//     yields the same verdict and the same state. The stores implement the
//     "post-effect already present with identical content -> idempotent
//     accept" rule; the cross-store checks below are phrased so a command's
//     own post-effect never flips their outcome (the checks either read state
//     the command cannot move, or are skipped once the effect is in place).
//
// Cross-store invariants enforced HERE (the stores expose fact queries; this
// layer is the only place that sees all six stores):
//   1. principal vs grant: the target node of AssignNodeToGroup,
//      GrantAuthority, and ActivateAuthority must be a registered, non-retired
//      node (identity store).
//   2. RetirePolicy: rejected while either an active grant or a non-terminal
//      operation's structured policy-reference list names the version.
//      Operation intents remain opaque; callers must put safety-relevant
//      policy dependencies in that committed list.
//   3. ActivateAuthority atomicity: grant-store ValidateActivate plus every
//      topology-side check (topology_epoch exactly current+1, new owner is a
//      group member and registered active, grant policy version active) all
//      run BEFORE any write; only then the grant half (ApplyGrantPart) and
//      the topology half (owner, authority_version, topology_epoch,
//      config_epoch) are written in order. Any rejection leaves both halves
//      untouched.
//   4. one-node-one-group cross-store half: an AssignNodeToGroup that would
//      move a node into a group it is not currently a member of requires the
//      node to hold no current membership and no active grant. "Grant owner
//      => member of the group" is maintained by the ActivateAuthority member
//      check (3) and by rejecting RemoveNodeFromGroup of a grant owner, so
//      the grant fact is read through the node's current group. Operation
//      intents remain opaque and do not create implicit node obligations.
//   5. The policy version referenced by GrantAuthority/ActivateAuthority must
//      be committed and non-retired (policy store IsVersionActive).
//   6. Remaining cross-domain facts: group existence and membership CAS live
//      in the stores; RetireNode is additionally rejected while the node
//      still holds group membership (which, by the invariant in (4), also
//      covers an active grant).
//
// MetaStores is the committed aggregate that snapshots serialize as one
// versioned envelope: per-store length-prefixed versioned blobs in a fixed
// order plus the committed active_write_schema. Deserialize is
// strict; every failure is MetaFailureClass::kFailStop — the same bytes fail
// identically on every node.

#include <cstdint>
#include <string>
#include <string_view>

#include "absl/status/statusor.h"
#include "meta/meta_audit_store.h"
#include "meta/meta_commands.h"
#include "meta/meta_grant_store.h"
#include "meta/meta_identity_store.h"
#include "meta/meta_operation_store.h"
#include "meta/meta_policy_store.h"
#include "meta/meta_topology_store.h"

namespace keylane::meta {

// The six committed stores plus the committed active_write_schema — the only
// committed state without a store of its own. SetSchemaVersion rewrites
// it as an absolute value. Store constructor knobs (audit window capacity,
// group/operation caps) are v1 deployment constants: snapshots do not carry
// them and Deserialize restores defaults.
struct MetaStores {
  MetaIdentityStore identity_;
  MetaTopologyStore topology_;
  MetaPolicyStore policy_;
  MetaGrantStore grant_;
  MetaOperationStore operation_;
  MetaAuditStore audit_;
  std::uint16_t active_write_schema_ = kMetaSchemaVersionV1;

  // One versioned envelope for snapshots: u16 schema_version, then a u32
  // length prefix + the store's own versioned blob per store in member order,
  // then u16 active_write_schema. Fails with MetaFailureClass::kDomainReject
  // when the total exceeds kMaxMetaSnapshotBytes; create_snapshot must fail
  // and alert, never silently truncate.
  absl::StatusOr<std::string> Serialize() const;
  // Strict decode of the Serialize envelope; every failure is fail-stop,
  // including an active_write_schema this binary cannot write.
  static absl::StatusOr<MetaStores> Deserialize(std::string_view bytes);
};

// The outcome of applying one committed command. verdict_ reuses the audit
// schema's enum so the apply result and the persisted audit verdict can never
// drift apart; a kRejected verdict is always the kDomainReject class
// (index consumed, audit written, state unchanged).
struct MetaApplyResult {
  MetaAuditVerdict verdict_ = MetaAuditVerdict::kRejected;
  std::string detail_;           // rejection reason; empty on accept
  std::uint64_t log_index_ = 0;  // echo of the applied raft log index
  // Which command was dispatched; always set by ApplyCommitted.
  MetaCommandTag command_tag_ = MetaCommandTag::kRegisterNode;
  bool operator==(const MetaApplyResult&) const = default;
};

// Applies one committed command to the aggregate state. See the file header
// for the full contract. `actor_principal`/`readable_time` are the trusted
// entry's injected ActorContext fields, carried by the raft-log command
// encoding as ordinary bounded strings (unforgeability is the entry layer's
// property); apply only copies them into the audit record
// and, for SubmitOperation, into the journal record's persisted submitter
// context.
//
// log_index is the command's raft log index (>= 1; raft indexes start at 1).
// It keys the audit record and is the operation_seq of SubmitOperation. A
// caller that passes 0 has lost the index correspondence; the command is then
// rejected without dispatch or audit write (the audit store would fail-stop
// on it), deterministically on every node. The same guard applies to actor
// fields exceeding the audit record caps: rejected before dispatch so state
// stays unchanged and identical everywhere.
MetaApplyResult ApplyCommitted(MetaStores& stores, std::uint64_t log_index,
                               const MetaCommand& command,
                               std::string_view actor_principal,
                               std::string_view readable_time);

}  // namespace keylane::meta
