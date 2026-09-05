#pragma once

// MetaCommands: the committed command schema of the issue #19 metadata
// control plane (plan docs/plans/issue-19-metadata-raft-implementation.md
// §2 "Committed 命令集 v1").
//
// Wire envelope (see meta_encoding.h for primitives):
//   schema_version u16 | command_tag u16 | request_id 16B |
//   actor_principal str | readable_time str | command body
// Bodies are sequences of fixed-width LE integers, length-prefixed capped
// strings/bytes, and capped lists. Every field has a hard cap (§2: state is
// size-bounded; over-cap input fails, never silently truncates).
//
// Cross-cutting schema rules (§2):
//   - Every command carries an opaque 16-byte request_id for audit
//     correlation. Trusted entries generate one when their external protocol
//     does not provide it; byte ordering and ULID semantics are deliberately
//     not part of the durable contract.
//   - Every command carries an ActorContext, injected by the trusted entry
//     via Propose(command, AuthenticatedPrincipal); apply only copies it into
//     the audit record keyed by raft log index. On the raft-log wire the two
//     actor fields are ordinary bounded strings — they MUST be encoded,
//     otherwise a follower's apply would lose the audit identity. The
//     unforgeability guarantee is a property of the ENTRY layer (only the
//     trusted ctl/coordinator entries can construct commands): the plan's
//     "外部 codec 不接受 actor 字段" defense is implemented at the ctl
//     text-protocol entry, not in this internal encoding.
//   - Mutations of mutable records carry expected_revision (CAS; conflict is
//     a domain rejection, not a decode failure).
//   - All changes are absolute values written by the proposer; there are no
//     "+1"-style relative mutations.
//   - Unknown schema versions and unknown command tags are decode failures
//     (fail-stop class); domain validation belongs to the apply layer.
//
// SetSchemaVersion is part of the permanently frozen v1 layout subset: it is
// always encoded with schema_version = kMetaSchemaVersionV1 so that any
// binary within the readable window can decode it (§2 升级契约). Its frozen
// layout is tag + request_id + actor_principal + readable_time +
// new_active_write_schema + attestation — the actor fields are part of the
// frozen subset because it is a privileged command whose audit record must
// identify the proposing entry; two bounded strings do not weaken the
// "decodable by any v2+ binary" promise. This layout NEVER changes.
//
// The checked-in v1 fixture freezes the oldest-readable SetSchemaVersion
// layout used during upgrades. The remaining issue-19 layouts become durable
// contracts at release; later evolution uses a newer schema version or an
// append-only command tag whose minimum write schema is explicit.

#include <array>
#include <cstdint>
#include <string>
#include <variant>
#include <vector>

#include "absl/status/statusor.h"
#include "meta/meta_encoding.h"

namespace keylane::meta {

// ---------------------------------------------------------------------------
// Shared scalar types and field caps.
// ---------------------------------------------------------------------------

// Opaque proposal-correlation id (16 bytes, fixed).
using MetaRequestId = std::array<std::uint8_t, 16>;

// operation_id: client-provided stable UUID; the operation's permanent
// idempotency key (§2 Operation 标识).
using MetaOperationId = std::array<std::uint8_t, 16>;

// Content-addressing hashes (policy content, intent, evidence): SHA-256.
using MetaHash256 = std::array<std::uint8_t, 32>;

// boot_incarnation: opaque fixed-length value; never ordered by magnitude
// (§4), compared only for equality.
inline constexpr std::size_t kMetaBootIncarnationBytes = 16;
using MetaBootIncarnation = std::array<std::uint8_t, kMetaBootIncarnationBytes>;

// Injected by the trusted entry at Propose time (§2). principal_ is the
// canonical SAN principal (§6); readable_time_ is the human-readable propose
// timestamp the trusted entry writes and apply only copies into the audit
// record. Both ride the raft-log encoding as ordinary bounded strings (see
// the file header for why unforgeability is an entry-layer property).
struct ActorContext {
  std::string principal_;
  std::string readable_time_;
  bool operator==(const ActorContext&) const = default;
};

// node_id: 40 lowercase hex chars, matching the data-plane topology
// convention (include/keylane/cluster/topology.h NodeDescriptor::node_id_).
inline constexpr std::uint32_t kMetaNodeIdBytes = 40;
// group_id: opaque to the data plane (topology.h GroupView::group_id_);
// Meta-scoped ids ride the same seam. Bounded as a string.
inline constexpr std::uint32_t kMaxMetaGroupIdBytes = 64;
inline constexpr std::uint32_t kMaxMetaPrincipalBytes = 256;
// Wire cap for ActorContext::readable_time_. Must stay equal to (or tighter
// than) the audit store's persistence cap kMaxMetaAuditReadableTimeBytes
// (meta_audit_store.h), so anything that decodes is persistable.
inline constexpr std::uint32_t kMaxMetaActorReadableTimeBytes = 128;
inline constexpr std::uint32_t kMaxMetaEndpointsPerNode = 8;
inline constexpr std::uint32_t kMaxMetaEndpointBytes = 256;
inline constexpr std::uint32_t kMaxMetaPolicyIdBytes = 128;
inline constexpr std::uint32_t kMaxMetaPolicyReferencesPerOperation = 16;
inline constexpr std::uint32_t kMaxMetaOperationKindBytes = 64;
inline constexpr std::uint32_t kMaxMetaEvidenceSummariesPerCommand = 64;
// Slot ranges per SetSlotMap; bounded by the Redis Cluster slot count
// (16384, topology.h kSlotCount).
inline constexpr std::uint32_t kMaxMetaSlotRangeCount = 16384;
// Valid slot ids are [0, kMetaSlotCount).
inline constexpr std::uint32_t kMetaSlotCount = 16384;
inline constexpr std::uint32_t kMaxMetaAbortReasonBytes = 1024;
inline constexpr std::uint32_t kMaxMetaAttestationBytes = 4096;

// Mirrors the data-plane primary/replica distinction (topology.h
// NodeDescriptor::is_primary_).
enum class MetaNodeRole : std::uint8_t { kPrimary = 1, kReplica = 2 };

// Wire tag per command. Tags are append-only and never reused.
enum class MetaCommandTag : std::uint16_t {
  kRegisterNode = 1,
  kUpdateNode = 2,
  kRetireNode = 3,
  kCreateGroup = 4,
  kAssignNodeToGroup = 5,
  kRemoveNodeFromGroup = 6,
  kSetSlotMap = 7,
  kBeginGroupTerm = 8,
  kGrantAuthority = 9,
  kActivateAuthority = 10,
  kRevokeGrant = 11,
  kFenceGroup = 12,
  kPutPolicy = 13,
  kRetirePolicy = 14,
  kSubmitOperation = 15,
  kTransitionOperationPhase = 16,
  kCompleteOperation = 17,
  kAbortOperation = 18,
  kArchiveOperations = 19,
  kSetSchemaVersion = 20,
  kPruneAudit = 21,
  kPruneOperationArchive = 22,
  kBindMetaMember = 23,
  kRetireMetaMember = 24,
  // Added in schema v2; v1 writers must never emit this tag.
  kSetGroupReplicationState = 25,
};

// ---------------------------------------------------------------------------
// identity/enrollment (§2). RegisterNode binds the certificate principal;
// UpdateNode must NOT modify it (no principal field — rotation unimplemented,
// §6).
// ---------------------------------------------------------------------------

struct RegisterNode {
  MetaRequestId request_id_{};
  ActorContext actor_;  // trusted-entry injected; encoded on the raft wire
  std::string node_id_;
  std::string principal_;  // canonical SAN principal, globally 1:1 (§6)
  std::vector<std::string> endpoints_;
  std::uint64_t capability_mask_ = 0;
  MetaNodeRole role_ = MetaNodeRole::kPrimary;
  bool operator==(const RegisterNode&) const = default;
};

// Mutable-record mutation: expected_revision is the CAS token (§2). There is
// deliberately no principal_ field: UpdateNode must not modify the principal
// binding (§6).
struct UpdateNode {
  MetaRequestId request_id_{};
  ActorContext actor_;
  std::string node_id_;
  std::uint64_t expected_revision_ = 0;
  std::vector<std::string> endpoints_;
  std::uint64_t capability_mask_ = 0;
  std::uint64_t new_topology_epoch_ = 0;  // endpoint visibility, absolute
  bool operator==(const UpdateNode&) const = default;
};

struct RetireNode {
  MetaRequestId request_id_{};
  ActorContext actor_;
  std::string node_id_;
  std::uint64_t expected_revision_ = 0;
  bool operator==(const RetireNode&) const = default;
};

// ---------------------------------------------------------------------------
// topology (§2). Every topology-visible change carries the new topology_epoch
// as an absolute value; owner-visible changes additionally carry the affected
// groups' new config_epoch values.
// ---------------------------------------------------------------------------

struct CreateGroup {
  MetaRequestId request_id_{};
  ActorContext actor_;
  std::string group_id_;
  std::uint64_t new_topology_epoch_ = 0;  // absolute (§2)
  bool operator==(const CreateGroup&) const = default;
};

// One-node-one-group is enforced by apply (§2); the command asserts the
// absolute target membership and the group record CAS token.
struct AssignNodeToGroup {
  MetaRequestId request_id_{};
  ActorContext actor_;
  std::string group_id_;
  std::string node_id_;
  MetaNodeRole role_ = MetaNodeRole::kPrimary;
  std::uint64_t expected_revision_ = 0;
  std::uint64_t new_topology_epoch_ = 0;  // absolute (§2)
  bool operator==(const AssignNodeToGroup&) const = default;
};

struct RemoveNodeFromGroup {
  MetaRequestId request_id_{};
  ActorContext actor_;
  std::string group_id_;
  std::string node_id_;
  std::uint64_t expected_revision_ = 0;
  std::uint64_t new_topology_epoch_ = 0;  // absolute (§2)
  bool operator==(const RemoveNodeFromGroup&) const = default;
};

// One slot range (both ends inclusive, [0, 16384)) and its owning group.
struct MetaSlotAssignment {
  std::uint16_t first_slot_ = 0;
  std::uint16_t last_slot_ = 0;
  std::string group_id_;
  bool operator==(const MetaSlotAssignment&) const = default;
};

// New config_epoch (absolute) for one affected group.
struct MetaGroupConfigEpoch {
  std::string group_id_;
  std::uint64_t config_epoch_ = 0;
  bool operator==(const MetaGroupConfigEpoch&) const = default;
};

struct SetSlotMap {
  MetaRequestId request_id_{};
  ActorContext actor_;
  std::vector<MetaSlotAssignment> ranges_;
  std::uint64_t new_topology_epoch_ = 0;  // absolute (§2)
  std::vector<MetaGroupConfigEpoch> config_epochs_;
  bool operator==(const SetSlotMap&) const = default;
};

// Replicated evolution of the GroupRecord fields used to validate
// observation/evidence freshness. Both fields are absolute and CAS-guarded;
// at least one must advance by exactly one. The topology epoch couples the
// newly visible replication state to every other topology consumer.
struct SetGroupReplicationState {
  MetaRequestId request_id_{};
  ActorContext actor_;
  std::string group_id_;
  std::uint64_t expected_population_manifest_id_ = 0;
  std::uint64_t new_population_manifest_id_ = 0;
  std::uint64_t expected_partition_replication_epoch_ = 0;
  std::uint64_t new_partition_replication_epoch_ = 0;
  std::uint64_t new_topology_epoch_ = 0;
  bool operator==(const SetGroupReplicationState&) const = default;
};

// ---------------------------------------------------------------------------
// GroupRecord (§2): per-group committed record. replication_history_id is
// deliberately absent: it is a data-plane boot-scoped identity (#14) and does
// not enter the committed record. The versioned record codec is defined here
// (u16 schema_version envelope, same convention as commands).
// ---------------------------------------------------------------------------

struct MetaGroupRecord {
  std::string owner_;  // node_id
  std::uint64_t group_term_ = 0;
  std::uint64_t authority_version_ = 0;
  std::uint64_t population_manifest_id_ = 0;
  std::uint64_t partition_replication_epoch_ = 0;
  bool operator==(const MetaGroupRecord&) const = default;
};

absl::StatusOr<std::string> EncodeMetaGroupRecord(
    const MetaGroupRecord& record);
absl::StatusOr<MetaGroupRecord> DecodeMetaGroupRecord(std::string_view bytes);

// ---------------------------------------------------------------------------
// term/grant (§2: term 只升一次,激活不再动 term). BeginGroupTerm(T) raises
// group_term and enters the no-grant/fenced state; GrantAuthority renews the
// same owner's lease without moving owner/term; ActivateAuthority is the
// failover/migration atomic commit point — it validates expected_term and
// atomically sets owner + grant + authority_version + epochs, and
// deliberately carries NO new term.
// ---------------------------------------------------------------------------

// Lease parameters plus the committed policy-version reference every grant
// must carry (§2: grant 引用的 policy 版本必须已 committed).
struct MetaGrantSpec {
  std::uint64_t lease_duration_ms_ = 0;
  std::string policy_id_;
  std::uint64_t policy_version_ = 0;
  bool operator==(const MetaGrantSpec&) const = default;
};

struct BeginGroupTerm {
  MetaRequestId request_id_{};
  ActorContext actor_;
  std::string group_id_;
  std::uint64_t expected_term_ = 0;  // T-1
  std::uint64_t new_term_ = 0;       // T
  bool operator==(const BeginGroupTerm&) const = default;
};

struct GrantAuthority {
  MetaRequestId request_id_{};
  ActorContext actor_;
  std::string group_id_;
  std::string node_id_;  // current owner; apply verifies it is unchanged
  std::uint64_t term_ = 0;
  std::uint64_t authority_version_ = 0;
  MetaGrantSpec grant_;
  bool operator==(const GrantAuthority&) const = default;
};

struct ActivateAuthority {
  MetaRequestId request_id_{};
  ActorContext actor_;
  std::string group_id_;
  std::uint64_t expected_term_ = 0;  // CAS on the current term; no new term
  std::string new_owner_;            // node_id
  MetaGrantSpec grant_;
  std::uint64_t new_authority_version_ = 0;
  std::uint64_t new_topology_epoch_ = 0;
  std::uint64_t new_config_epoch_ = 0;
  bool operator==(const ActivateAuthority&) const = default;
};

struct RevokeGrant {
  MetaRequestId request_id_{};
  ActorContext actor_;
  std::string group_id_;
  std::uint64_t expected_term_ = 0;
  bool operator==(const RevokeGrant&) const = default;
};

struct FenceGroup {
  MetaRequestId request_id_{};
  ActorContext actor_;
  std::string group_id_;
  std::uint64_t expected_term_ = 0;
  bool operator==(const FenceGroup&) const = default;
};

// ---------------------------------------------------------------------------
// policy (§2 PolicyStore): versioned documents, content-hash addressed.
// RetirePolicy must be rejected by apply while a version is still referenced
// by an active grant or a non-terminal operation — the schema carries just
// the (policy_id, version) pair.
// ---------------------------------------------------------------------------

struct PutPolicy {
  MetaRequestId request_id_{};
  ActorContext actor_;
  std::string policy_id_;
  std::uint64_t version_ = 0;
  std::string content_;  // bounded by kMaxMetaPayloadBytes
  MetaHash256 content_hash_{};
  bool operator==(const PutPolicy&) const = default;
};

struct RetirePolicy {
  MetaRequestId request_id_{};
  ActorContext actor_;
  std::string policy_id_;
  std::uint64_t version_ = 0;
  bool operator==(const RetirePolicy&) const = default;
};

// ---------------------------------------------------------------------------
// operation journal (§2 通用生命周期): Submitted -> Running -> Completed |
// Aborted. kind-specific phase-graph legality is checked by coordinator-
// registered ValidateProposal plugins (leader-local), never by apply; the
// schema therefore treats kind and phase blobs as opaque bounded values.
// ---------------------------------------------------------------------------

// Immutable evidence summary baked into a command by the leader after
// ValidateProposal (§2 Leader-local 校验与 apply 校验的拆分). The journal and
// audit trail persist these summaries, never observation references.
// boot_incarnation_ is opaque and never ordered by magnitude (§4).
struct MetaEvidenceSummary {
  std::string node_id_;
  MetaBootIncarnation boot_incarnation_{};
  std::uint64_t group_term_ = 0;
  std::uint64_t population_manifest_id_ = 0;
  std::uint64_t replication_history_id_ = 0;
  MetaOperationId operation_id_{};
  MetaHash256 kind_hash_{};  // kind-specific evidence hash
  bool operator==(const MetaEvidenceSummary&) const = default;
};

// A committed policy dependency of a non-terminal operation. Keeping the
// reference structured (rather than interpreting the opaque intent) lets
// apply deterministically prevent retirement while work is live.
struct MetaPolicyReference {
  std::string policy_id_;
  std::uint64_t version_ = 0;
  bool operator==(const MetaPolicyReference&) const = default;
};

struct SubmitOperation {
  MetaRequestId request_id_{};
  ActorContext actor_;
  MetaOperationId operation_id_{};  // client-provided stable UUID
  std::string kind_;
  std::string intent_;  // bounded by kMaxMetaPayloadBytes
  MetaHash256 intent_hash_{};
  // Zero means no replication-history binding. Evidence carrying a history
  // id is accepted only when it matches this committed anchor.
  std::uint64_t replication_history_id_ = 0;
  std::vector<MetaPolicyReference> policy_references_;
  bool operator==(const SubmitOperation&) const = default;
};

struct TransitionOperationPhase {
  MetaRequestId request_id_{};
  ActorContext actor_;
  MetaOperationId operation_id_{};
  std::uint64_t expected_revision_ = 0;
  std::string kind_phase_blob_;  // bounded by kMaxMetaPayloadBytes
  std::vector<MetaEvidenceSummary> evidence_;
  bool operator==(const TransitionOperationPhase&) const = default;
};

struct CompleteOperation {
  MetaRequestId request_id_{};
  ActorContext actor_;
  MetaOperationId operation_id_{};
  std::uint64_t expected_revision_ = 0;
  std::string result_;  // terminal result, bounded by kMaxMetaPayloadBytes
  bool data_loss_possible_ = false;
  bool operator==(const CompleteOperation&) const = default;
};

struct AbortOperation {
  MetaRequestId request_id_{};
  ActorContext actor_;
  MetaOperationId operation_id_{};
  std::uint64_t expected_revision_ = 0;
  std::string reason_;
  bool operator==(const AbortOperation&) const = default;
};

// Non-contiguous archival of terminal operations (§2 归档去卡死).
// operation_seq values are raft log indexes of the corresponding
// SubmitOperation commands (§2 seq 颁发), used here purely as references.
struct ArchiveOperations {
  MetaRequestId request_id_{};
  ActorContext actor_;
  std::vector<std::uint64_t> operation_seqs_;
  bool operator==(const ArchiveOperations&) const = default;
};

// ---------------------------------------------------------------------------
// upgrade (§2/§3 升级契约). SetSchemaVersion switches the committed
// active_write_schema. Its encoding is part of the PERMANENTLY FROZEN v1
// layout subset: it is always written with schema_version =
// kMetaSchemaVersionV1 so every binary within the readable window can decode
// it (§2 "自身以最旧可读格式编码"). Fields may never be reordered, removed,
// or re-typed; extensions go through new command tags. The frozen layout —
// tag + request_id + actor_principal + readable_time +
// new_active_write_schema + attestation — carries the actor like every other
// privileged command, so its audit record identifies the proposing entry.
// ---------------------------------------------------------------------------

struct SetSchemaVersion {
  MetaRequestId request_id_{};
  ActorContext actor_;  // encoded, and part of the frozen layout
  std::uint16_t new_active_write_schema_ = 0;
  std::string attestation_;  // operator attestation (ticket, reason)
  bool operator==(const SetSchemaVersion&) const = default;
};

// Replicated acknowledgement that an audit prefix has been durably exported.
// The apply layer prunes through this index before appending this command's own
// audit record, so the prune remains visible and replay-idempotent.
struct PruneAudit {
  MetaRequestId request_id_{};
  ActorContext actor_;
  std::uint64_t through_log_index_ = 0;
  bool operator==(const PruneAudit&) const = default;
};

// Removes externally archived operation tombstones after their documented
// retry-retention window. Unknown/already-pruned seqs are idempotent no-ops.
struct PruneOperationArchive {
  MetaRequestId request_id_{};
  ActorContext actor_;
  std::vector<std::uint64_t> operation_seqs_;
  bool operator==(const PruneOperationArchive&) const = default;
};

// First stage of a dynamic Meta membership add: the canonical certificate
// identity and schema capability are committed and audited before add_srv.
struct BindMetaMember {
  MetaRequestId request_id_{};
  ActorContext actor_;
  std::uint32_t server_id_ = 0;
  std::string principal_;
  std::uint16_t min_schema_ = 0;
  std::uint16_t max_schema_ = 0;
  bool operator==(const BindMetaMember&) const = default;
};

// Second stage of a dynamic Meta membership removal: remove_srv commits
// first, then this command retires (but preserves) the identity tombstone.
struct RetireMetaMember {
  MetaRequestId request_id_{};
  ActorContext actor_;
  std::uint32_t server_id_ = 0;
  bool operator==(const RetireMetaMember&) const = default;
};

using MetaCommand =
    std::variant<RegisterNode, UpdateNode, RetireNode, CreateGroup,
                 AssignNodeToGroup, RemoveNodeFromGroup, SetSlotMap,
                 BeginGroupTerm, GrantAuthority, ActivateAuthority, RevokeGrant,
                 FenceGroup, PutPolicy, RetirePolicy, SubmitOperation,
                 TransitionOperationPhase, CompleteOperation, AbortOperation,
                 ArchiveOperations, SetSchemaVersion, PruneAudit,
                 PruneOperationArchive, BindMetaMember, RetireMetaMember,
                 SetGroupReplicationState>;

// Encode produces the full envelope. Fails (kDomainReject class) when a field
// exceeds its cap or the total exceeds kMaxMetaCommandBytes; the encoding is
// never silently truncated.
absl::StatusOr<std::string> EncodeMetaCommand(
    const MetaCommand& command,
    std::uint16_t write_schema = kMetaCurrentSchemaVersion);

// Decode is strict: unknown version/tag, truncation, over-cap fields, and
// trailing bytes all fail with MetaFailureClass::kFailStop.
absl::StatusOr<MetaCommand> DecodeMetaCommand(std::string_view bytes);

}  // namespace keylane::meta
