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
//     move for record-field, failover-transition, or slot-map changes. An
//     active failover transition has its own Raft-index revision and is
//     replaced only through an exact transition-id/revision reference.
//   - topology_epoch is strictly monotonic and gap-free: every command that
//     carries new_topology_epoch (group lifecycle/membership, endpoint,
//     replication state, slot map, authority activation, and failover cutover
//     through the granular primitives) must carry exactly current + 1.
//   - Cluster lifecycle's public revision is derived from its state. Only
//     Uninitialized may enter Creating; Created and ProvisioningFailed are
//     terminal. These transitions do not advance topology_epoch. The root
//     operation id and
//     Genesis commit index remain after operation archive/prune, so duplicate
//     creation rejection never depends on operation retention.
//   - Slot map is absolute: SetSlotMap replaces the whole map; ranges must be
//     in bounds [0, kMetaSlotCount) and pairwise non-overlapping, and every
//     referenced group must exist. Partial coverage is legal (unassigned
//     slots have no owner); an empty range list clears the map. Whether a
//     changed slot owner is covered by active Group authority is checked by
//     MetaStateApply, which rejects the transition until every affected group
//     is fenced.
//   - Each Group owns one term and owner plus its authority-active state.
//     BeginGroupTerm fences and advances the term; ActivateAuthority changes
//     owner and installs authority together. Aggregate epoch, membership and
//     identity checks remain in ApplyCommitted.
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
// Serialization: u16 schema_version envelope; lifecycle, then groups (including
// optional failover transitions) sorted by group_id, members sorted by node_id,
// retained last-assignment index sorted by node_id, and the slot map as sorted
// runs. Byte output is deterministic so equal states serialize to equal bytes.
// Pre-release stores use no migration; an older development data directory
// must be rebuilt.

#include <array>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "lavik/client_mode.h"
#include "lavik/meta/commands.h"
#include "lavik/meta/encoding.h"

namespace lavik::meta {

inline constexpr std::uint16_t kMetaTopologyStoreFormatVersion = 1;
inline constexpr std::uint32_t kMaxMetaClusterFailureSummaryBytes = 512;

// Durable lifecycle of the one logical Data cluster owned by a Meta Raft
// cluster. Runtime readiness is intentionally not represented here.
enum class MetaClusterLifecycle : std::uint8_t {
  kUninitialized = 0,
  kCreating = 1,
  kCreated = 2,
  kProvisioningFailed = 3,
};

struct MetaClusterLifecycleState {
  MetaClusterLifecycle state_ = MetaClusterLifecycle::kUninitialized;
  // Absent only before Genesis. This declaration is immutable, independent
  // of readiness and retained after the creation operation is archived.
  std::optional<ClientMode> client_mode_;
  MetaOperationId root_operation_id_{};
  std::uint64_t genesis_commit_index_ = 0;
  std::string failure_summary_;
  // The one-shot lifecycle has no independent revision: initial, creating,
  // and either terminal state are generations 0, 1, and 2 respectively.
  std::uint64_t Revision() const {
    return state_ == MetaClusterLifecycle::kUninitialized ? 0
           : state_ == MetaClusterLifecycle::kCreating    ? 1
                                                          : 2;
  }
  bool operator==(const MetaClusterLifecycleState&) const = default;
};

// One member of a group. Query results are sorted by node_id.
struct MetaGroupMember {
  std::string node_id_;
  MetaAssignmentId assignment_id_{};
  MetaNodeRole role_ = MetaNodeRole::kPrimary;
  bool operator==(const MetaGroupMember&) const = default;
};

// Read view of one group: the committed GroupRecord, any active failover
// transition, and the topology store's membership CAS revision and members.
// Members are sorted by node_id_.
struct MetaTopologyGroupView {
  std::string group_id_;
  MetaGroupRecord record_;
  std::optional<MetaFailoverTransition> failover_transition_;
  std::uint64_t revision_ = 0;  // 1 at creation, +1 per membership change
  std::vector<MetaGroupMember> members_;
  bool operator==(const MetaTopologyGroupView&) const = default;
};

// Derived authority query views; neither is separately persisted.
struct MetaActiveAuthorityView {
  std::string owner_;  // node_id
  // Set only by failover cutover. Data activation must match this committed
  // action to the boot-local prepared context; ordinary authority activation
  // clears it.
  std::optional<MetaFailoverActionId> activation_action_id_;
  bool operator==(const MetaActiveAuthorityView&) const = default;
};

// Read-only view of one group's term/grant state (fact query result).
struct MetaGroupAuthorityView {
  std::uint64_t group_term_ = 0;
  std::optional<MetaActiveAuthorityView> grant_;  // absent == fenced
  bool operator==(const MetaGroupAuthorityView&) const = default;
};

class MetaTopologyStore {
 public:
  // Cluster creation never advances topology_epoch.
  // Exact calls replay as no-ops; Created and ProvisioningFailed are terminal.
  absl::Status BeginClusterCreate(const MetaOperationId& root_operation_id,
                                  std::uint64_t genesis_commit_index,
                                  ClientMode client_mode);
  absl::Status CompleteClusterCreate(const MetaOperationId& root_operation_id);
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

  // Granular population/epoch updates participate in ApplyCommitted's
  // cross-record validation. SetTopologyEpoch accepts current or current + 1.
  absl::Status SetPopulationManifest(const std::string& group_id,
                                     std::uint64_t manifest_revision,
                                     const MetaHash256& manifest_digest);
  bool PopulationManifestInUse(const MetaHash256& manifest_digest) const;
  absl::Status SetPartitionReplicationEpoch(const std::string& group_id,
                                            std::uint64_t epoch);
  // Installs the only active transition for a group. committed_index becomes
  // the transition revision regardless of the caller's input value. An exact
  // replay at that index is a no-op; another active transition conflicts.
  // Membership and cross-store facts are intentionally caller-owned.
  absl::Status InstallFailoverTransition(
      const std::string& group_id, const MetaFailoverTransition& transition,
      std::uint64_t committed_index);
  // Replaces the transition named by the exact id/revision reference and
  // records committed_index as its strictly newer revision. Replacement
  // cannot change transition identity. The complete post-state is checked
  // before the current-revision CAS so an exact Raft replay is a no-op.
  absl::Status ReplaceFailoverTransition(
      const std::string& group_id,
      const MetaFailoverTransitionRef& expected_transition,
      const MetaFailoverTransition& replacement, std::uint64_t committed_index);
  // Clears only the exact transition id/revision. An already-empty slot is
  // accepted because this primitive is idempotent. Compound commands must
  // still prove their other post-state in the aggregate layer: absence alone
  // cannot distinguish replay of an old clear from a later cleared state.
  absl::Status ClearFailoverTransition(
      const std::string& group_id,
      const MetaFailoverTransitionRef& expected_transition);
  absl::Status SetTopologyEpoch(std::uint64_t new_topology_epoch);
  // Read-only preflight for cross-store transactions such as UpdateNode.
  // Accepts current (replay) or current+1 (fresh apply).
  absl::Status ValidateTopologyEpoch(std::uint64_t new_topology_epoch) const;

  // Group authority shares record_.group_term_/owner_. Advancing term fences
  // while retaining the last owner for replication; each term activates once.
  absl::Status BeginGroupTerm(const lavik::meta::BeginGroupTerm& command);
  absl::Status ValidateActivate(const lavik::meta::ActivateAuthority& command,
                                std::optional<MetaFailoverActionId>
                                    activation_action_id = std::nullopt) const;
  // Validates and atomically installs owner and active authority in this Group.
  // Aggregate membership/identity/epoch checks remain with ApplyCommitted.
  absl::Status ActivateAuthority(
      const lavik::meta::ActivateAuthority& command,
      std::optional<MetaFailoverActionId> activation_action_id = std::nullopt);
  std::optional<MetaGroupAuthorityView> AuthorityFor(
      std::string_view group_id) const;
  std::optional<std::uint64_t> CurrentGroupTerm(
      std::string_view group_id) const;

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
  // Exact durable size without allocating or copying snapshot bytes.
  std::uint64_t SerializedSize() const;
  static absl::StatusOr<MetaTopologyStore> Deserialize(std::string_view bytes);

 private:
  friend class MetaTopologyTestAccess;
  void WriteSnapshot(MetaWriter& writer) const;
  friend class MetaApplyRollback;

  struct GroupState {
    MetaGroupRecord record_;
    bool authority_active_ = false;
    std::optional<MetaFailoverActionId> activation_action_id_;
    std::optional<MetaFailoverTransition> failover_transition_;
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

}  // namespace lavik::meta
