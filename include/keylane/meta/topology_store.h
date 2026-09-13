#pragma once

// MetaTopologyStore is the metadata control plane's committed topology store.
// It holds the single logical Data cluster's lifecycle, the group table
// (group_id -> GroupState), the retained last-assignment identity per node,
// the 16384-entry slot map, and the cluster-wide topology_epoch.
//
// Invariants:
//   - One-node-one-group: a node_id is a member of at most one group.
//     AssignNodeToGroup to the same group with the same role replays as an
//     idempotent accept only when assignment_id and the command's
//     expected/current membership revisions also identify that exact applied
//     transition; a different identity, role, or group is a domain rejection
//     (membership change requires an explicit RemoveNodeFromGroup first).
//     Whether the node's old authority/obligations were cleared is a
//     CROSS-STORE question: this store only exposes the facts
//     (FindGroupOfNode, FindGroup) and lets the apply dispatcher enforce.
//   - revision_ is the membership CAS token of a group: 1 at creation,
//     expected_revision+1 after each applied membership change. It does not
//     move for record-field or slot-map changes.
//   - topology_epoch is strictly monotonic and gap-free: every command that
//     carries new_topology_epoch (group lifecycle/membership, endpoint,
//     replication state, slot map, and ActivateAuthority through the
//     granular primitives) must carry exactly current + 1.
//   - Cluster lifecycle has an independent revision. Only Uninitialized may
//     enter Creating; Created and ProvisioningFailed are terminal. These
//     transitions do not advance topology_epoch. The root operation id and
//     Genesis commit index remain after operation archive/prune, so duplicate
//     creation rejection never depends on operation retention.
//   - Slot map is absolute: SetSlotMap replaces the whole map; ranges must be
//     in bounds [0, kMetaSlotCount) and pairwise non-overlapping, and every
//     referenced group (ranges and config_epochs) must exist. Partial
//     coverage is legal (unassigned slots have no owner); an empty range list
//     clears the map. config_epoch values are absolute assignments, no
//     ordering enforced here. Whether a changed slot owner or config epoch is
//     covered by an active grant is a cross-store fact: MetaStateApply rejects
//     that transition until every affected group is fenced.
//   - MetaGroupRecord fields (owner, group_term, authority_version,
//     population manifest revision/digest, partition_replication_epoch) and
//     per-group config_epoch change ONLY through the granular primitives below.
//     The term/grant semantics and the atomicity of owner switches (failover /
//     ActivateAuthority) span the grant store and are orchestrated by the
//     apply dispatcher. The primitives therefore validate group existence and
//     absolute-value/idempotency only; ordering rules (term raised once via
//     BeginGroupTerm, authority_version bumps, ...) live in the grant layer.
//   - Membership does not cascade: removing the node named by record.owner_
//     from the member table leaves owner_ untouched. The apply dispatcher
//     reads the fact and decides.
//   - State is size-bounded: kMaxMetaGroups groups, at most
//     kMaxMetaNodes members per group; over-cap applies are rejected, never
//     silently truncated.
//
// Replay idempotency: re-applying a command
// whose exact post-effect is already present is an idempotent accept
// (no-op); conflicting content is a domain rejection. The granular
// primitives follow the same rule: setting a field to the value it already
// holds is a no-op accept.
//
// Failure classes: domain rejections return MetaDomainRejectError
// (kDomainReject); deserialization failures are fail-stop (kFailStop).
//
// Scope: pure in-memory function of command + committed state — no IO, no
// locks, never reads the local clock, never touches observation state. In
// particular this store does NOT know whether a node_id is registered:
// registration is the identity store's fact, cross-checked by the dispatcher.
//
// Serialization: u16 schema_version envelope; lifecycle, then groups sorted
// by group_id, members sorted by node_id, retained last-assignment index sorted
// by node_id, and the slot map as sorted runs. Byte output is deterministic so
// equal states serialize to equal bytes. Pre-release stores use no migration;
// an older development data directory must be rebuilt.

#include <array>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "keylane/meta/commands.h"

namespace keylane::meta {

inline constexpr std::uint16_t kMetaTopologyStoreFormatVersion = 2;
inline constexpr std::uint32_t kMaxMetaClusterFailureSummaryBytes = 512;

// Durable lifecycle of the one logical Data cluster owned by a Meta Raft
// cluster. Runtime readiness is intentionally not represented here.
enum class MetaClusterLifecycle : std::uint8_t {
  kUninitialized = 0,
  kCreating = 1,
  kCreated = 2,
  kProvisioningFailed = 3,
};

enum class MetaClusterTerminalOutcome : std::uint8_t {
  kNone = 0,
  kCreated = 1,
  kProvisioningFailed = 2,
};

struct MetaClusterLifecycleState {
  MetaClusterLifecycle state_ = MetaClusterLifecycle::kUninitialized;
  std::uint64_t revision_ = 0;
  MetaOperationId root_operation_id_{};
  std::uint64_t genesis_commit_index_ = 0;
  MetaClusterTerminalOutcome terminal_outcome_ =
      MetaClusterTerminalOutcome::kNone;
  std::string failure_summary_;
  bool operator==(const MetaClusterLifecycleState&) const = default;
};

// One member of a group. Query results are sorted by node_id.
struct MetaGroupMember {
  std::string node_id_;
  MetaAssignmentId assignment_id_{};
  MetaNodeRole role_ = MetaNodeRole::kPrimary;
  bool operator==(const MetaGroupMember&) const = default;
};

// Read view of one group: the committed GroupRecord plus the topology
// store's own bookkeeping (config_epoch, membership CAS revision, members).
struct MetaTopologyGroupView {
  std::string group_id_;
  MetaGroupRecord record_;
  std::uint64_t config_epoch_ = 0;
  std::uint64_t revision_ = 0;  // 1 at creation, +1 per membership change
  std::vector<MetaGroupMember> members_;
  bool operator==(const MetaTopologyGroupView&) const = default;
};

class MetaTopologyStore {
 public:
  // Cluster creation has its own revision and never advances topology_epoch.
  // Exact calls replay as no-ops; Created and ProvisioningFailed are terminal.
  absl::Status BeginClusterCreate(const MetaOperationId& root_operation_id,
                                  std::uint64_t genesis_commit_index);
  absl::Status CompleteClusterCreate(
      const MetaOperationId& root_operation_id);
  absl::Status FailClusterCreate(const MetaOperationId& root_operation_id,
                                 std::string failure_summary);
  const MetaClusterLifecycleState& ClusterLifecycle() const {
    return cluster_lifecycle_;
  }

  // Domain-validated apply of the topology commands. Each returns
  // absl::OkStatus() on apply or idempotent accept, and a kDomainReject
  // status otherwise; state is unchanged on rejection.
  absl::Status Apply(const CreateGroup& cmd);
  absl::Status Apply(const AssignNodeToGroup& cmd);
  absl::Status Apply(const RemoveNodeFromGroup& cmd);
  absl::Status Apply(const SetSlotMap& cmd);
  absl::Status Apply(const SetGroupReplicationState& cmd);

  // Granular primitives for the apply dispatcher (e.g. orchestrating
  // ActivateAuthority atomically with the grant store). Each validates that
  // the group exists; setting the value the field already holds is an
  // idempotent no-op accept. SetTopologyEpoch requires exactly current+1 (or
  // current, idempotently). Any cross-store rule (grant consistency, term
  // semantics, epoch coupling) is enforced by the caller, not here.
  absl::Status SetOwner(const std::string& group_id,
                        const std::string& new_owner);
  absl::Status SetGroupTerm(const std::string& group_id, std::uint64_t term);
  absl::Status SetAuthorityVersion(const std::string& group_id,
                                   std::uint64_t authority_version);
  absl::Status SetPopulationManifest(const std::string& group_id,
                                     std::uint64_t manifest_revision,
                                     const MetaHash256& manifest_digest);
  bool PopulationManifestInUse(const MetaHash256& manifest_digest) const;
  absl::Status SetPartitionReplicationEpoch(const std::string& group_id,
                                            std::uint64_t epoch);
  absl::Status SetGroupConfigEpoch(const std::string& group_id,
                                   std::uint64_t config_epoch);
  absl::Status SetTopologyEpoch(std::uint64_t new_topology_epoch);
  // Read-only preflight for cross-store transactions such as UpdateNode.
  // Accepts current (replay) or current+1 (fresh apply).
  absl::Status ValidateTopologyEpoch(std::uint64_t new_topology_epoch) const;

  // Fact queries.
  std::uint64_t TopologyEpoch() const { return topology_epoch_; }
  std::optional<MetaTopologyGroupView> FindGroup(
      const std::string& group_id) const;
  // The group node_id is a member of, if any (one-node-one-group).
  std::optional<std::string> FindGroupOfNode(const std::string& node_id) const;
  // Owning group of a slot; nullopt when unassigned or slot out of range.
  std::optional<std::string> SlotOwner(std::uint32_t slot) const;
  bool GroupExists(const std::string& group_id) const;
  std::vector<MetaTopologyGroupView> Groups() const;
  std::size_t GroupCount() const { return groups_.size(); }

  // Snapshot support: u16 schema_version envelope, deterministic bytes.
  // Serialize cannot fail (state is bounded and codec-valid by
  // construction). Deserialize is strict and every failure is fail-stop,
  // including invariant violations inside the bytes (node in two groups,
  // slot run out of bounds/overlapping/referencing an unknown group).
  std::string Serialize() const;
  static absl::StatusOr<MetaTopologyStore> Deserialize(std::string_view bytes);

 private:
  struct GroupState {
    MetaGroupRecord record_;
    std::uint64_t config_epoch_ = 0;
    std::uint64_t revision_ = 0;
    struct MemberState {
      MetaAssignmentId assignment_id_{};
      MetaNodeRole role_ = MetaNodeRole::kPrimary;
      bool operator==(const MemberState&) const = default;
    };
    std::map<std::string, MemberState> members_;  // node_id -> state, sorted
  };

  std::map<std::string, GroupState> groups_;  // by group_id, sorted
  // node_id -> group_id reverse index enforcing one-node-one-group.
  std::map<std::string, std::string> group_of_node_;
  // Retained after removal to reject direct replay of the most recent
  // membership identity across snapshot/restart. Assignment ids are globally
  // unique by proposer contract; this bounded index is not an unbounded
  // history of every prior incarnation.
  std::map<std::string, MetaAssignmentId> last_assignment_by_node_;
  // Slot -> owning group_id; empty string = unassigned.
  std::array<std::string, kMetaSlotCount> slots_;
  std::uint64_t topology_epoch_ = 0;
  MetaClusterLifecycleState cluster_lifecycle_;
};

}  // namespace keylane::meta
