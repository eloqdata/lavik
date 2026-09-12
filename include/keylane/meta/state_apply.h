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
// layer is the only place that sees all eight stores):
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
//   7. TransitionOperationPhase is applied to a candidate operation store and
//      the exact FullDesiredState for every old/new directive recipient is
//      projected and encoded before commit. This aggregate check prevents
//      individually valid operations from making a node's projection exceed
//      protocol count, field, or total-object limits; rejection leaves the
//      operation store unchanged.
//   8. Live directives remain valid after later committed mutations. After
//      every accepted command, apply rechecks their exact source/target
//      assignments, active term/authority/grant revision, and population
//      identity, removes stale attempts, and bumps each affected operation's
//      CAS revision once. CommitDirectiveResult repeats the same check before
//      first commit; snapshot recovery and projection reject any stale entry
//      that bypassed this invariant.
//   9. SetSlotMap first constructs the complete candidate topology and rejects
//      any slot-ownership or config-epoch change that affects a group with an
//      active grant. Source and destination groups must be fenced before the
//      cut, so no lease issued for the old projection can span a slot move.
//  10. Creation and Meta-membership workflows have one shared durable
//      reservation. A new cluster-create workflow reserves pristine topology
//      before its first mutation and excludes another active creation id.
//      Exact-id replay resolves before this guard, even after topology is
//      populated.
//  11. A live controlled failover owns its group's recovery and authority
//      mutations. Apply revalidates the typed operation/recovery relation on
//      every replica; in particular, an operation receipt and the independently
//      versioned recovery record cannot race to discard a committed exact
//      frozen-source result.
//
// MetaStores is the committed aggregate that snapshots serialize as one
// versioned envelope: per-store length-prefixed versioned blobs in a fixed
// order. Deserialize is strict and revalidates the cross-store group set,
// authority anchors, active identities/policies, and retained manifest
// documents before exposing any decoded store; every failure is
// MetaFailureClass::kFailStop — the same bytes fail
// identically on every node.

#include <cstdint>
#include <string>
#include <string_view>

#include "absl/status/statusor.h"
#include "keylane/meta/audit_store.h"
#include "keylane/meta/commands.h"
#include "keylane/meta/failover_recovery_store.h"
#include "keylane/meta/grant_store.h"
#include "keylane/meta/identity_store.h"
#include "keylane/meta/operation_store.h"
#include "keylane/meta/policy_store.h"
#include "keylane/meta/population_manifest_store.h"
#include "keylane/meta/topology_store.h"

namespace keylane::meta {

// The eight committed stores. Store constructor knobs (audit window capacity,
// group/operation caps) are deployment constants: snapshots do not carry
// them and Deserialize restores defaults.
struct MetaStores {
  MetaIdentityStore identity_;
  MetaTopologyStore topology_;
  MetaPolicyStore policy_;
  MetaGrantStore grant_;
  MetaOperationStore operation_;
  MetaPopulationManifestStore population_manifest_;
  MetaFailoverRecoveryStore failover_recovery_;
  MetaAuditStore audit_;

  // One versioned envelope for snapshots: u16 schema_version, then a u32
  // length prefix + the store's own versioned blob per store in member order.
  // Fails with MetaFailureClass::kDomainReject
  // when the total exceeds kMaxMetaSnapshotBytes; create_snapshot must fail
  // and alert, never silently truncate.
  absl::StatusOr<std::string> Serialize() const;
  // Strict decode of the Serialize envelope; every failure is fail-stop.
  static absl::StatusOr<MetaStores> Deserialize(std::string_view bytes);
};

// Validates one durable directive against the exact currently committed
// source/target memberships, active authority, and population identity. Boot
// incarnations are durable intent anchors, but the committed identity registry
// has no boot lifecycle against which to validate them; authenticated sessions
// and observations supply that independent check. Transition apply, result
// commit, snapshot recovery, and wire projection share this predicate so none
// can accept or expose a directive after its committed anchor has gone stale.
absl::Status ValidateCommittedDirectiveAnchor(
    const MetaStores& stores, const MetaDirectiveSpec& directive);

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

namespace detail {

// Pure budget seam used by SetFailoverRecovery apply and its boundary tests.
// `serialized_size` already includes the candidate recovery effect. An exact
// same-index effect also proves that the corresponding audit record is part
// of the current aggregate, so replay has no additional snapshot growth.
bool FailoverRecoveryFitsSnapshotBudget(std::uint64_t serialized_size,
                                        bool exact_same_index_effect,
                                        std::uint64_t snapshot_byte_limit);

}  // namespace detail

// Process-local NuRaft completion payload. It is not part of the durable WAL
// or snapshot format; carrying the apply verdict in cmd_result avoids racing
// a later audit rotation/prune when the proposer resumes.
std::string EncodeMetaApplyResult(const MetaApplyResult& result);
absl::StatusOr<MetaApplyResult> DecodeMetaApplyResult(std::string_view bytes);

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
