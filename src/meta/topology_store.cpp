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

#include "lavik/meta/topology_store.h"

#include <algorithm>
#include <set>

#include "absl/strings/str_cat.h"
#include "lavik/status_macros.h"

namespace lavik::meta {
namespace {

// Field-cap re-validation at the store boundary (commands normally arrive
// via the strict decoder; the store keeps its invariants self-contained).
absl::Status CheckGroupId(const std::string& group_id) {
  if (group_id.empty() || group_id.size() > kMaxMetaGroupIdBytes) {
    return MetaDomainRejectError("group_id empty or over cap");
  }
  return absl::OkStatus();
}

absl::Status CheckNodeId(const std::string& node_id) {
  if (node_id.empty() || node_id.size() > kMetaNodeIdBytes) {
    return MetaDomainRejectError("node_id empty or over cap");
  }
  return absl::OkStatus();
}

bool IsZero(const MetaAssignmentId& id) {
  return std::all_of(id.begin(), id.end(),
                     [](std::uint8_t byte) { return byte == 0; });
}

bool IsZero(const MetaHash256& hash) {
  return std::all_of(hash.begin(), hash.end(),
                     [](std::uint8_t byte) { return byte == 0; });
}

absl::Status CheckClusterRoot(const MetaOperationId& root_operation_id,
                              std::uint64_t genesis_commit_index) {
  if (IsZero(root_operation_id)) {
    return MetaDomainRejectError("cluster create root operation id is zero");
  }
  if (genesis_commit_index == 0) {
    return MetaDomainRejectError("cluster Genesis commit index is zero");
  }
  return absl::OkStatus();
}

absl::Status ValidateClusterLifecycle(
    const MetaClusterLifecycleState& lifecycle) {
  if ((lifecycle.state_ == MetaClusterLifecycle::kUninitialized) !=
          !lifecycle.client_mode_.has_value() ||
      (lifecycle.client_mode_ && !IsValidClientMode(*lifecycle.client_mode_))) {
    return MetaFailStopError("invalid cluster client mode in snapshot");
  }
  const bool root_is_zero = IsZero(lifecycle.root_operation_id_);
  switch (lifecycle.state_) {
    case MetaClusterLifecycle::kUninitialized:
      if (root_is_zero && lifecycle.genesis_commit_index_ == 0 &&
          lifecycle.failure_summary_.empty()) {
        return absl::OkStatus();
      }
      break;
    case MetaClusterLifecycle::kCreating:
      if (!root_is_zero && lifecycle.genesis_commit_index_ != 0 &&
          lifecycle.failure_summary_.empty()) {
        return absl::OkStatus();
      }
      break;
    case MetaClusterLifecycle::kCreated:
      if (!root_is_zero && lifecycle.genesis_commit_index_ != 0 &&
          lifecycle.failure_summary_.empty()) {
        return absl::OkStatus();
      }
      break;
    case MetaClusterLifecycle::kProvisioningFailed:
      if (!root_is_zero && lifecycle.genesis_commit_index_ != 0 &&
          !lifecycle.failure_summary_.empty() &&
          lifecycle.failure_summary_.size() <=
              kMaxMetaClusterFailureSummaryBytes &&
          std::all_of(lifecycle.failure_summary_.begin(),
                      lifecycle.failure_summary_.end(), [](char ch) {
                        const auto byte = static_cast<unsigned char>(ch);
                        return byte >= 0x20 && byte <= 0x7e;
                      })) {
        return absl::OkStatus();
      }
      break;
  }
  return MetaFailStopError("invalid cluster lifecycle in snapshot");
}

// The epoch rule: absolute values, strictly monotonic and gap-free —
// the command must carry exactly current+1. Saturating at u64 max is a
// rejection, never a wrap.
absl::Status CheckNextTopologyEpoch(std::uint64_t current,
                                    std::uint64_t new_epoch) {
  if (current == UINT64_MAX || new_epoch != current + 1) {
    return MetaDomainRejectError(absl::StrCat(
        "new_topology_epoch must be exactly current+1 (", current, ")"));
  }
  return absl::OkStatus();
}

}  // namespace

absl::Status MetaTopologyStore::BeginClusterCreate(
    const MetaOperationId& root_operation_id,
    std::uint64_t genesis_commit_index, ClientMode client_mode) {
  if (!IsValidClientMode(client_mode)) {
    return MetaDomainRejectError("invalid cluster client mode");
  }
  LAVIK_RETURN_IF_ERROR(
      CheckClusterRoot(root_operation_id, genesis_commit_index));
  if (cluster_lifecycle_.state_ != MetaClusterLifecycle::kUninitialized &&
      cluster_lifecycle_.root_operation_id_ == root_operation_id &&
      cluster_lifecycle_.genesis_commit_index_ == genesis_commit_index &&
      cluster_lifecycle_.client_mode_ == client_mode &&
      ValidateClusterLifecycle(cluster_lifecycle_).ok()) {
    return absl::OkStatus();
  }
  if (cluster_lifecycle_.state_ != MetaClusterLifecycle::kUninitialized) {
    return MetaDomainRejectError("cluster has already accepted creation");
  }
  cluster_lifecycle_.state_ = MetaClusterLifecycle::kCreating;
  cluster_lifecycle_.client_mode_ = client_mode;

  cluster_lifecycle_.root_operation_id_ = root_operation_id;
  cluster_lifecycle_.genesis_commit_index_ = genesis_commit_index;
  return absl::OkStatus();
}

absl::Status MetaTopologyStore::CompleteClusterCreate(
    const MetaOperationId& root_operation_id) {
  if (cluster_lifecycle_.state_ == MetaClusterLifecycle::kCreated &&
      cluster_lifecycle_.root_operation_id_ == root_operation_id &&
      cluster_lifecycle_.failure_summary_.empty()) {
    return absl::OkStatus();
  }
  if (cluster_lifecycle_.state_ != MetaClusterLifecycle::kCreating ||
      cluster_lifecycle_.root_operation_id_ != root_operation_id) {
    return MetaDomainRejectError(
        "cluster create completion does not match the active root");
  }
  if (cluster_lifecycle_.client_mode_ == ClientMode::kSingle &&
      (groups_.size() != 1 ||
       std::any_of(slots_.begin(), slots_.end(),
                   [](const std::string& owner) { return owner.empty(); }))) {
    return MetaDomainRejectError(
        "Single creation requires one fully covered Group");
  }
  cluster_lifecycle_.state_ = MetaClusterLifecycle::kCreated;

  cluster_lifecycle_.failure_summary_.clear();
  return absl::OkStatus();
}

absl::Status MetaTopologyStore::FailClusterCreate(
    const MetaOperationId& root_operation_id, std::string failure_summary) {
  if (failure_summary.empty() ||
      failure_summary.size() > kMaxMetaClusterFailureSummaryBytes ||
      std::any_of(failure_summary.begin(), failure_summary.end(), [](char ch) {
        const auto byte = static_cast<unsigned char>(ch);
        return byte < 0x20 || byte > 0x7e;
      })) {
    return MetaDomainRejectError(
        "cluster failure summary is empty, unsafe, or over cap");
  }
  if (cluster_lifecycle_.state_ == MetaClusterLifecycle::kProvisioningFailed &&
      cluster_lifecycle_.root_operation_id_ == root_operation_id &&
      cluster_lifecycle_.failure_summary_ == failure_summary) {
    return absl::OkStatus();
  }
  if (cluster_lifecycle_.state_ != MetaClusterLifecycle::kCreating ||
      cluster_lifecycle_.root_operation_id_ != root_operation_id) {
    return MetaDomainRejectError(
        "cluster create failure does not match the active root");
  }
  cluster_lifecycle_.state_ = MetaClusterLifecycle::kProvisioningFailed;

  cluster_lifecycle_.failure_summary_ = std::move(failure_summary);
  return absl::OkStatus();
}

absl::Status MetaTopologyStore::Apply(const CreateGroup& cmd) {
  LAVIK_RETURN_IF_ERROR(CheckGroupId(cmd.group_id_));
  if (const auto it = groups_.find(cmd.group_id_); it != groups_.end()) {
    // Replay: the group exists exactly as created (never mutated) and the
    // topology epoch already carries this command's value -> idempotent
    // accept. Anything else under this group_id is a conflict.
    const GroupState& group = it->second;
    const bool pristine = group.revision_ == 1 && group.members_.empty() &&
                          group.record_ == MetaGroupRecord{};
    if (pristine && topology_epoch_ == cmd.new_topology_epoch_) {
      return absl::OkStatus();
    }
    return MetaDomainRejectError(
        absl::StrCat("group ", cmd.group_id_, " already exists"));
  }
  LAVIK_RETURN_IF_ERROR(
      CheckNextTopologyEpoch(topology_epoch_, cmd.new_topology_epoch_));
  if (groups_.size() >= kMaxMetaGroups) {
    return MetaDomainRejectError("group cap reached");
  }
  if (cluster_lifecycle_.client_mode_ == ClientMode::kSingle &&
      !groups_.empty()) {
    return MetaDomainRejectError("Single requires exactly one Group");
  }
  GroupState group;
  group.revision_ = 1;
  groups_.emplace(cmd.group_id_, std::move(group));
  topology_epoch_ = cmd.new_topology_epoch_;
  return absl::OkStatus();
}

absl::Status MetaTopologyStore::Apply(const AssignNodeToGroup& cmd) {
  LAVIK_RETURN_IF_ERROR(CheckGroupId(cmd.group_id_));
  LAVIK_RETURN_IF_ERROR(CheckNodeId(cmd.node_id_));
  if (IsZero(cmd.assignment_id_)) {
    return MetaDomainRejectError("assignment_id must not be zero");
  }
  const auto it = groups_.find(cmd.group_id_);
  if (it == groups_.end()) {
    return MetaDomainRejectError(absl::StrCat("unknown group ", cmd.group_id_));
  }
  GroupState& group = it->second;
  const auto member = group.members_.find(cmd.node_id_);
  // Replay: the member already sits in this group with the same role and the
  // record at the revision this command produces -> idempotent accept.
  if (member != group.members_.end() &&
      member->second.assignment_id_ == cmd.assignment_id_ &&
      member->second.role_ == cmd.role_ &&
      group.revision_ == cmd.expected_revision_ + 1 &&
      topology_epoch_ == cmd.new_topology_epoch_) {
    return absl::OkStatus();
  }
  if (group.revision_ != cmd.expected_revision_) {
    return MetaDomainRejectError(
        absl::StrCat("expected_revision CAS conflict on ", cmd.group_id_));
  }
  // One-node-one-group. Cross-store facts (registration, old
  // authority/obligations) are exposed to the apply dispatcher, not checked
  // here.
  if (const auto prior = group_of_node_.find(cmd.node_id_);
      prior != group_of_node_.end()) {
    if (prior->second == cmd.group_id_) {
      return MetaDomainRejectError(absl::StrCat(
          "node already a member of ", cmd.group_id_, " with another role"));
    }
    return MetaDomainRejectError(absl::StrCat(
        "node already a member of ", prior->second, " (one-node-one-group)"));
  }
  if (const auto prior = last_assignment_by_node_.find(cmd.node_id_);
      prior != last_assignment_by_node_.end() &&
      prior->second == cmd.assignment_id_) {
    return MetaDomainRejectError("remove/re-add must use a new assignment_id");
  }
  if (group.members_.size() >= kMaxMetaNodes) {
    return MetaDomainRejectError("group member cap reached");
  }
  LAVIK_RETURN_IF_ERROR(
      CheckNextTopologyEpoch(topology_epoch_, cmd.new_topology_epoch_));
  group.members_.emplace(
      cmd.node_id_, GroupState::MemberState{cmd.assignment_id_, cmd.role_});
  group_of_node_.emplace(cmd.node_id_, cmd.group_id_);
  last_assignment_by_node_[cmd.node_id_] = cmd.assignment_id_;
  group.revision_ = cmd.expected_revision_ + 1;
  topology_epoch_ = cmd.new_topology_epoch_;
  return absl::OkStatus();
}

absl::Status MetaTopologyStore::Apply(const RemoveNodeFromGroup& cmd) {
  LAVIK_RETURN_IF_ERROR(CheckGroupId(cmd.group_id_));
  LAVIK_RETURN_IF_ERROR(CheckNodeId(cmd.node_id_));
  const auto it = groups_.find(cmd.group_id_);
  if (it == groups_.end()) {
    return MetaDomainRejectError(absl::StrCat("unknown group ", cmd.group_id_));
  }
  GroupState& group = it->second;
  const auto member = group.members_.find(cmd.node_id_);
  // Replay: the member is already gone and the record sits at the revision
  // this command produces -> idempotent accept.
  if (member == group.members_.end() &&
      group.revision_ == cmd.expected_revision_ + 1 &&
      topology_epoch_ == cmd.new_topology_epoch_) {
    return absl::OkStatus();
  }
  if (group.revision_ != cmd.expected_revision_) {
    return MetaDomainRejectError(
        absl::StrCat("expected_revision CAS conflict on ", cmd.group_id_));
  }
  if (member == group.members_.end()) {
    return MetaDomainRejectError(
        absl::StrCat("node not a member of ", cmd.group_id_));
  }
  LAVIK_RETURN_IF_ERROR(
      CheckNextTopologyEpoch(topology_epoch_, cmd.new_topology_epoch_));
  // No cascade: if the removed node is the record owner, owner_ is left
  // untouched; the apply dispatcher reads the fact and decides.
  group.members_.erase(member);
  group_of_node_.erase(cmd.node_id_);
  group.revision_ = cmd.expected_revision_ + 1;
  topology_epoch_ = cmd.new_topology_epoch_;
  return absl::OkStatus();
}

absl::Status MetaTopologyStore::Apply(const SetSlotMap& cmd) {
  if (cmd.ranges_.size() > kMaxMetaSlotRangeCount) {
    return MetaDomainRejectError("slot map field count over cap");
  }
  // Structural re-validation (the codec enforces it on the wire; the store
  // keeps its own invariants): in-bounds ranges, non-overlapping, and every
  // referenced group known.
  for (const MetaSlotAssignment& range : cmd.ranges_) {
    if (range.first_slot_ > range.last_slot_ ||
        range.last_slot_ >= kMetaSlotCount) {
      return MetaDomainRejectError("slot range out of bounds");
    }
    LAVIK_RETURN_IF_ERROR(CheckGroupId(range.group_id_));
  }
  {
    std::vector<MetaSlotAssignment> sorted = cmd.ranges_;
    std::sort(sorted.begin(), sorted.end(),
              [](const MetaSlotAssignment& a, const MetaSlotAssignment& b) {
                return a.first_slot_ < b.first_slot_;
              });
    if (cluster_lifecycle_.client_mode_ == ClientMode::kSingle) {
      std::uint32_t next = 0;
      for (const auto& range : sorted) {
        if (range.first_slot_ != next || groups_.size() != 1 ||
            range.group_id_ != groups_.begin()->first) {
          return MetaDomainRejectError(
              "Single requires one fully covered Group");
        }
        next = static_cast<std::uint32_t>(range.last_slot_) + 1;
      }
      if (next != kMetaSlotCount) {
        return MetaDomainRejectError("Single requires complete slot coverage");
      }
    }
    for (std::size_t i = 1; i < sorted.size(); ++i) {
      if (sorted[i].first_slot_ <= sorted[i - 1].last_slot_) {
        return MetaDomainRejectError("overlapping slot ranges");
      }
    }
  }
  // Replay: slot map and topology epoch already carry the command's effect.
  if (topology_epoch_ == cmd.new_topology_epoch_) {
    std::array<std::string, kMetaSlotCount> target;
    for (const MetaSlotAssignment& range : cmd.ranges_) {
      for (std::uint32_t slot = range.first_slot_; slot <= range.last_slot_;
           ++slot) {
        target[slot] = range.group_id_;
      }
    }
    if (target == slots_) return absl::OkStatus();
  }

  LAVIK_RETURN_IF_ERROR(
      CheckNextTopologyEpoch(topology_epoch_, cmd.new_topology_epoch_));
  for (const MetaSlotAssignment& range : cmd.ranges_) {
    if (!groups_.contains(range.group_id_)) {
      return MetaDomainRejectError(absl::StrCat(
          "slot range references unknown group ", range.group_id_));
    }
  }

  // Absolute replacement: the whole map is rewritten from the ranges.
  slots_.fill(std::string());
  for (const MetaSlotAssignment& range : cmd.ranges_) {
    for (std::uint32_t slot = range.first_slot_; slot <= range.last_slot_;
         ++slot) {
      slots_[slot] = range.group_id_;
    }
  }
  topology_epoch_ = cmd.new_topology_epoch_;
  return absl::OkStatus();
}

absl::Status MetaTopologyStore::Apply(const SetGroupReplicationState& cmd) {
  LAVIK_RETURN_IF_ERROR(CheckGroupId(cmd.group_id_));
  const auto it = groups_.find(cmd.group_id_);
  if (it == groups_.end()) {
    return MetaDomainRejectError(absl::StrCat("unknown group ", cmd.group_id_));
  }
  GroupState& group = it->second;
  const MetaGroupRecord& record = group.record_;
  if (record.population_manifest_revision_ ==
          cmd.new_population_manifest_revision_ &&
      record.population_manifest_digest_ ==
          cmd.new_population_manifest_digest_ &&
      record.partition_replication_epoch_ ==
          cmd.new_partition_replication_epoch_ &&
      topology_epoch_ == cmd.new_topology_epoch_) {
    return absl::OkStatus();
  }
  if (record.population_manifest_revision_ !=
          cmd.expected_population_manifest_revision_ ||
      record.population_manifest_digest_ !=
          cmd.expected_population_manifest_digest_ ||
      record.partition_replication_epoch_ !=
          cmd.expected_partition_replication_epoch_) {
    return MetaDomainRejectError("group replication-state CAS conflict");
  }
  const auto advances_by_at_most_one = [](std::uint64_t expected,
                                          std::uint64_t next) {
    return next == expected || (expected != UINT64_MAX && next == expected + 1);
  };
  if (!advances_by_at_most_one(cmd.expected_population_manifest_revision_,
                               cmd.new_population_manifest_revision_) ||
      !advances_by_at_most_one(cmd.expected_partition_replication_epoch_,
                               cmd.new_partition_replication_epoch_)) {
    return MetaDomainRejectError(
        "group replication fields must stay unchanged or advance by one");
  }
  if ((cmd.expected_population_manifest_revision_ == 0) !=
          IsZero(cmd.expected_population_manifest_digest_) ||
      (cmd.new_population_manifest_revision_ == 0) !=
          IsZero(cmd.new_population_manifest_digest_)) {
    return MetaDomainRejectError(
        "manifest revision zero must have the zero digest and vice versa");
  }
  if (cmd.new_population_manifest_revision_ ==
          cmd.expected_population_manifest_revision_ &&
      cmd.new_population_manifest_digest_ !=
          cmd.expected_population_manifest_digest_) {
    return MetaDomainRejectError(
        "manifest digest cannot change without a revision advance");
  }
  if (cmd.new_population_manifest_revision_ ==
          cmd.expected_population_manifest_revision_ &&
      cmd.new_partition_replication_epoch_ ==
          cmd.expected_partition_replication_epoch_) {
    return MetaDomainRejectError("group replication update has no effect");
  }
  LAVIK_RETURN_IF_ERROR(
      CheckNextTopologyEpoch(topology_epoch_, cmd.new_topology_epoch_));
  group.record_.population_manifest_revision_ =
      cmd.new_population_manifest_revision_;
  group.record_.population_manifest_digest_ =
      cmd.new_population_manifest_digest_;
  group.record_.partition_replication_epoch_ =
      cmd.new_partition_replication_epoch_;
  topology_epoch_ = cmd.new_topology_epoch_;
  return absl::OkStatus();
}

absl::Status MetaTopologyStore::SetPopulationManifest(
    const std::string& group_id, std::uint64_t manifest_revision,
    const MetaHash256& manifest_digest) {
  const auto it = groups_.find(group_id);
  if (it == groups_.end()) {
    return MetaDomainRejectError(absl::StrCat("unknown group ", group_id));
  }
  it->second.record_.population_manifest_revision_ = manifest_revision;
  it->second.record_.population_manifest_digest_ = manifest_digest;
  return absl::OkStatus();
}

bool MetaTopologyStore::PopulationManifestInUse(
    const MetaHash256& manifest_digest) const {
  return std::any_of(
      groups_.begin(), groups_.end(), [&manifest_digest](const auto& item) {
        const MetaGroupRecord& record = item.second.record_;
        return record.population_manifest_revision_ != 0 &&
               record.population_manifest_digest_ == manifest_digest;
      });
}

absl::Status MetaTopologyStore::SetPartitionReplicationEpoch(
    const std::string& group_id, std::uint64_t epoch) {
  const auto it = groups_.find(group_id);
  if (it == groups_.end()) {
    return MetaDomainRejectError(absl::StrCat("unknown group ", group_id));
  }
  it->second.record_.partition_replication_epoch_ = epoch;
  return absl::OkStatus();
}

absl::Status MetaTopologyStore::InstallFailoverTransition(
    const std::string& group_id, const MetaFailoverTransition& transition,
    std::uint64_t committed_index) {
  const auto it = groups_.find(group_id);
  if (it == groups_.end()) {
    return MetaDomainRejectError(absl::StrCat("unknown group ", group_id));
  }
  if (committed_index == 0) {
    return MetaDomainRejectError("failover transition revision is zero");
  }

  MetaFailoverTransition installed = transition;
  installed.revision_ = committed_index;
  LAVIK_RETURN_IF_ERROR(ValidateMetaFailoverTransition(installed));

  GroupState& group = it->second;
  if (group.failover_transition_ == installed) {
    return absl::OkStatus();
  }
  if (group.failover_transition_.has_value()) {
    return MetaDomainRejectError("group already has a failover transition");
  }
  group.failover_transition_ = std::move(installed);
  return absl::OkStatus();
}

absl::Status MetaTopologyStore::ReplaceFailoverTransition(
    const std::string& group_id,
    const MetaFailoverTransitionRef& expected_transition,
    const MetaFailoverTransition& replacement, std::uint64_t committed_index) {
  const auto it = groups_.find(group_id);
  if (it == groups_.end()) {
    return MetaDomainRejectError(absl::StrCat("unknown group ", group_id));
  }
  if (IsZero(expected_transition.transition_id_) ||
      expected_transition.revision_ == 0) {
    return MetaDomainRejectError("invalid failover transition reference");
  }
  if (committed_index == 0) {
    return MetaDomainRejectError("failover transition revision is zero");
  }

  MetaFailoverTransition installed = replacement;
  installed.revision_ = committed_index;
  if (installed.transition_id_ != expected_transition.transition_id_) {
    return MetaDomainRejectError(
        "failover replacement cannot change transition identity");
  }
  LAVIK_RETURN_IF_ERROR(ValidateMetaFailoverTransition(installed));

  GroupState& group = it->second;
  if (group.failover_transition_ == installed) {
    return absl::OkStatus();
  }
  if (!group.failover_transition_.has_value()) {
    return MetaDomainRejectError("group has no active failover transition");
  }
  const MetaFailoverTransition& current = *group.failover_transition_;
  if (current.transition_id_ != expected_transition.transition_id_ ||
      current.revision_ != expected_transition.revision_) {
    return MetaDomainRejectError("stale failover transition reference");
  }
  if (committed_index <= current.revision_) {
    return MetaDomainRejectError(
        "failover transition revision must strictly increase");
  }
  if (current.recovery_deadline_unix_ms_.has_value() &&
      installed.recovery_deadline_unix_ms_ !=
          current.recovery_deadline_unix_ms_) {
    return MetaDomainRejectError("failover recovery deadline is immutable");
  }
  group.failover_transition_ = std::move(installed);
  return absl::OkStatus();
}

absl::Status MetaTopologyStore::ClearFailoverTransition(
    const std::string& group_id,
    const MetaFailoverTransitionRef& expected_transition) {
  const auto it = groups_.find(group_id);
  if (it == groups_.end()) {
    return MetaDomainRejectError(absl::StrCat("unknown group ", group_id));
  }
  if (IsZero(expected_transition.transition_id_) ||
      expected_transition.revision_ == 0) {
    return MetaDomainRejectError("invalid failover transition reference");
  }

  GroupState& group = it->second;
  if (!group.failover_transition_.has_value()) {
    return absl::OkStatus();
  }
  if (group.failover_transition_->transition_id_ !=
          expected_transition.transition_id_ ||
      group.failover_transition_->revision_ != expected_transition.revision_) {
    return MetaDomainRejectError("stale failover transition reference");
  }
  group.failover_transition_.reset();
  return absl::OkStatus();
}

absl::Status MetaTopologyStore::SetTopologyEpoch(
    std::uint64_t new_topology_epoch) {
  // Same value already held: idempotent no-op accept.
  if (new_topology_epoch == topology_epoch_) return absl::OkStatus();
  LAVIK_RETURN_IF_ERROR(
      CheckNextTopologyEpoch(topology_epoch_, new_topology_epoch));
  topology_epoch_ = new_topology_epoch;
  return absl::OkStatus();
}

absl::Status MetaTopologyStore::BeginGroupTerm(
    const lavik::meta::BeginGroupTerm& command) {
  const auto it = groups_.find(command.group_id_);
  if (it == groups_.end()) return MetaDomainRejectError("unknown group");
  GroupState& group = it->second;
  if (command.expected_term_ == std::numeric_limits<std::uint64_t>::max() ||
      command.new_term_ != command.expected_term_ + 1) {
    return MetaDomainRejectError("new term must be exactly expected term + 1");
  }
  if (group.record_.group_term_ != command.expected_term_) {
    if (group.record_.group_term_ == command.new_term_ &&
        !group.authority_active_)
      return absl::OkStatus();
    return MetaDomainRejectError("expected term does not match current term");
  }
  group.record_.group_term_ = command.new_term_;
  group.authority_active_ = false;
  group.activation_action_id_.reset();
  return absl::OkStatus();
}

absl::Status MetaTopologyStore::ValidateActivate(
    const lavik::meta::ActivateAuthority& command,
    std::optional<MetaFailoverActionId> action) const {
  if (action.has_value() && IsZero(*action))
    return MetaDomainRejectError("failover activation action id is zero");
  const auto it = groups_.find(command.group_id_);
  if (it == groups_.end()) return MetaDomainRejectError("unknown group");
  const GroupState& group = it->second;
  if (command.expected_term_ == 0 ||
      group.record_.group_term_ != command.expected_term_)
    return MetaDomainRejectError("authority requires the current nonzero term");
  if (command.new_owner_.empty() ||
      command.new_owner_.size() > kMetaNodeIdBytes)
    return MetaDomainRejectError("invalid authority owner");
  if (group.authority_active_ && (group.record_.owner_ != command.new_owner_ ||
                                  group.activation_action_id_ != action))
    return MetaDomainRejectError("group term already has an active grant");
  return absl::OkStatus();
}

absl::Status MetaTopologyStore::ActivateAuthority(
    const lavik::meta::ActivateAuthority& command,
    std::optional<MetaFailoverActionId> action) {
  LAVIK_RETURN_IF_ERROR(ValidateActivate(command, action));
  GroupState& group = groups_.at(command.group_id_);
  group.record_.owner_ = command.new_owner_;
  group.authority_active_ = true;
  group.activation_action_id_ = std::move(action);
  return absl::OkStatus();
}

std::optional<MetaGroupAuthorityView> MetaTopologyStore::AuthorityFor(
    std::string_view group_id) const {
  const auto it = groups_.find(std::string(group_id));
  if (it == groups_.end()) return std::nullopt;
  const GroupState& group = it->second;
  MetaGroupAuthorityView view{.group_term_ = group.record_.group_term_,
                              .grant_ = std::nullopt};
  if (group.authority_active_)
    view.grant_ = MetaActiveAuthorityView{group.record_.owner_,
                                          group.activation_action_id_};
  return view;
}

std::optional<std::uint64_t> MetaTopologyStore::CurrentGroupTerm(
    std::string_view group_id) const {
  const auto it = groups_.find(std::string(group_id));
  if (it == groups_.end()) return std::nullopt;
  return it->second.record_.group_term_;
}

absl::Status MetaTopologyStore::ValidateTopologyEpoch(
    std::uint64_t new_topology_epoch) const {
  if (new_topology_epoch == topology_epoch_) return absl::OkStatus();
  return CheckNextTopologyEpoch(topology_epoch_, new_topology_epoch);
}

std::optional<MetaTopologyGroupView> MetaTopologyStore::FindGroup(
    const std::string& group_id) const {
  const auto it = groups_.find(group_id);
  if (it == groups_.end()) return std::nullopt;
  const GroupState& group = it->second;
  MetaTopologyGroupView view;
  view.group_id_ = group_id;
  view.record_ = group.record_;
  view.failover_transition_ = group.failover_transition_;
  view.revision_ = group.revision_;
  view.members_.reserve(group.members_.size());
  for (const auto& [node_id, member] : group.members_) {
    view.members_.push_back(
        MetaGroupMember{node_id, member.assignment_id_, member.role_});
  }
  return view;
}

std::optional<std::string> MetaTopologyStore::FindGroupOfNode(
    const std::string& node_id) const {
  const auto it = group_of_node_.find(node_id);
  if (it == group_of_node_.end()) return std::nullopt;
  return it->second;
}

std::optional<std::string> MetaTopologyStore::SlotOwner(
    std::uint32_t slot) const {
  if (slot >= kMetaSlotCount) return std::nullopt;
  if (slots_[slot].empty()) return std::nullopt;
  return slots_[slot];
}

bool MetaTopologyStore::GroupExists(const std::string& group_id) const {
  return groups_.contains(group_id);
}

std::vector<std::string> MetaTopologyStore::SlotOwners() const {
  return {slots_.begin(), slots_.end()};
}

std::vector<MetaSlotAssignment> MetaTopologyStore::SlotRanges() const {
  std::vector<MetaSlotAssignment> ranges;
  for (std::uint32_t first = 0; first < kMetaSlotCount;) {
    std::uint32_t last = first;
    while (last + 1 < kMetaSlotCount && slots_[last + 1] == slots_[first])
      ++last;
    if (!slots_[first].empty())
      ranges.push_back({static_cast<std::uint16_t>(first),
                        static_cast<std::uint16_t>(last), slots_[first]});
    first = last + 1;
  }
  return ranges;
}

std::vector<MetaObservationGroupFacts> MetaTopologyStore::ObservationFacts()
    const {
  std::vector<MetaObservationGroupFacts> result;
  result.reserve(groups_.size());
  for (const auto& [group_id, state] : groups_) {
    auto& group = result.emplace_back();
    group.group_id_ = group_id;
    group.record_ = state.record_;
    group.authority_active_ = state.authority_active_;
    group.failover_transition_ = state.failover_transition_;
    group.members_.reserve(state.members_.size());
    for (const auto& [node_id, member] : state.members_) {
      group.members_.push_back({node_id, member.assignment_id_});
    }
  }
  return result;
}

std::vector<MetaFailoverWork> MetaTopologyStore::FailoverWork() const {
  std::vector<MetaFailoverWork> result;
  for (const auto& [id, state] : groups_) {
    if (!state.failover_transition_) continue;
    const auto& transition = *state.failover_transition_;
    auto& work = result.emplace_back();
    work.group_id_ = id;
    work.transition_ = {transition.transition_id_, transition.revision_};
    work.mode_ = transition.mode_;
    work.recovery_started_ = transition.recovery_deadline_unix_ms_.has_value();
    if (transition.controlled_)
      work.operation_id_ = transition.controlled_->operation_id_;
    if (transition.candidate_action_) {
      work.action_id_ = transition.candidate_action_->action_id_;
      if (transition.candidate_action_->authorization_)
        work.authorized_loss_ =
            transition.candidate_action_->authorization_->loss_if_cutover_;
    }
  }
  return result;
}

std::vector<MetaAutomaticGroupFacts>
MetaTopologyStore::AutomaticDetectionFacts() const {
  std::vector<MetaAutomaticGroupFacts> result;
  result.reserve(groups_.size());
  for (const auto& [id, state] : groups_) {
    auto& facts = result.emplace_back();
    facts.group_id_ = id;
    facts.record_ = state.record_;
    facts.revision_ = state.revision_;
    const auto owner = state.members_.find(state.record_.owner_);
    if (owner != state.members_.end())
      facts.owner_assignment_ = owner->second.assignment_id_;
    auto authority = AuthorityFor(id);
    facts.authority_ = std::move(*authority);
    if (state.failover_transition_)
      facts.transition_ = {state.failover_transition_->transition_id_,
                           state.failover_transition_->revision_};
  }
  return result;
}

bool MetaTopologyStore::HasControlledOperation(
    const MetaOperationId& id) const {
  return std::any_of(groups_.begin(), groups_.end(), [&](const auto& entry) {
    const auto& transition = entry.second.failover_transition_;
    return transition && transition->mode_ == MetaFailoverMode::kControlled &&
           transition->controlled_ &&
           transition->controlled_->operation_id_ == id;
  });
}

std::vector<MetaTopologyGroupView> MetaTopologyStore::Groups() const {
  std::vector<MetaTopologyGroupView> result;
  result.reserve(groups_.size());
  for (const auto& [group_id, state] : groups_) {
    (void)state;
    result.push_back(*FindGroup(group_id));
  }
  return result;
}

// Envelope: schema_version u16 | cluster lifecycle | topology_epoch u64 |
// group count u32 | sorted group records | retained-assignment count u32 |
// sorted (node_id, assignment_id) entries | slot run count u32 | sorted runs.
// The lifecycle is intentionally in this store but independent of
// topology_epoch: accepting Genesis is not itself a topology mutation.
void MetaTopologyStore::WriteSnapshot(MetaWriter& w) const {
  w.WriteU16(kMetaTopologyStoreFormatVersion);
  w.WriteU8(static_cast<std::uint8_t>(cluster_lifecycle_.state_));
  w.WriteU8(cluster_lifecycle_.client_mode_
                ? static_cast<std::uint8_t>(*cluster_lifecycle_.client_mode_) +
                      1
                : 0);

  WriteFixedArray(w, cluster_lifecycle_.root_operation_id_);
  w.WriteU64(cluster_lifecycle_.genesis_commit_index_);

  w.WriteString(cluster_lifecycle_.failure_summary_);
  w.WriteU64(topology_epoch_);
  w.WriteCount(static_cast<std::uint32_t>(groups_.size()));
  for (const auto& [group_id, group] : groups_) {
    w.WriteString(group_id);
    w.WriteString(group.record_.owner_);
    w.WriteU64(group.record_.group_term_);
    w.WriteBool(group.authority_active_);
    w.WriteOptional(group.activation_action_id_,
                    [](MetaWriter& nested, const MetaFailoverActionId& id) {
                      WriteFixedArray(nested, id);
                    });
    w.WriteU64(group.record_.population_manifest_revision_);
    WriteFixedArray(w, group.record_.population_manifest_digest_);
    w.WriteU64(group.record_.partition_replication_epoch_);
    w.WriteU64(group.revision_);
    w.WriteOptional(
        group.failover_transition_,
        [](MetaWriter& nested, const MetaFailoverTransition& transition) {
          // Store mutation and snapshot restore both validate this value, so
          // the infallible topology serializer cannot encounter a codec
          // domain error here.
          (void)WriteMetaFailoverTransition(nested, transition);
        });
    w.WriteCount(static_cast<std::uint32_t>(group.members_.size()));
    for (const auto& [node_id, member] : group.members_) {
      w.WriteString(node_id);
      WriteFixedArray(w, member.assignment_id_);
      w.WriteU8(static_cast<std::uint8_t>(member.role_));
    }
  }
  w.WriteCount(static_cast<std::uint32_t>(last_assignment_by_node_.size()));
  for (const auto& [node_id, assignment_id] : last_assignment_by_node_) {
    w.WriteString(node_id);
    WriteFixedArray(w, assignment_id);
  }
  // The slot map as maximal runs of consecutive slots owned by one group.
  // Two passes over the fixed 16384-entry array: count, then emit.
  std::uint32_t run_count = 0;
  for (std::uint32_t slot = 0; slot < kMetaSlotCount; ++slot) {
    if (!slots_[slot].empty() &&
        (slot == 0 || slots_[slot - 1] != slots_[slot])) {
      ++run_count;
    }
  }
  w.WriteCount(run_count);
  for (std::uint32_t slot = 0; slot < kMetaSlotCount; ++slot) {
    if (slots_[slot].empty()) continue;
    if (slot > 0 && slots_[slot - 1] == slots_[slot]) continue;
    std::uint32_t last = slot;
    while (last + 1 < kMetaSlotCount && slots_[last + 1] == slots_[slot]) {
      ++last;
    }
    w.WriteU16(static_cast<std::uint16_t>(slot));
    w.WriteU16(static_cast<std::uint16_t>(last));
    w.WriteString(slots_[slot]);
  }
}

std::string MetaTopologyStore::Serialize() const {
  MetaWriter writer;
  WriteSnapshot(writer);
  return writer.TakeBuffer();
}

std::uint64_t MetaTopologyStore::SerializedSize() const {
  MetaWriter counter(false);
  WriteSnapshot(counter);
  return counter.size();
}

absl::StatusOr<MetaTopologyStore> MetaTopologyStore::Deserialize(
    std::string_view bytes) {
  MetaReader r(bytes);
  auto version = r.ReadU16();
  LAVIK_RETURN_IF_ERROR(version);
  if (*version != kMetaTopologyStoreFormatVersion) {
    return MetaFailStopError("unknown schema_version");
  }
  auto lifecycle_state = r.ReadU8();
  LAVIK_RETURN_IF_ERROR(lifecycle_state);
  auto client_mode = r.ReadU8();
  LAVIK_RETURN_IF_ERROR(client_mode);
  if (*client_mode > 2) return MetaFailStopError("unknown cluster client mode");
  auto root_operation_id = ReadFixedArray<16>(r);
  LAVIK_RETURN_IF_ERROR(root_operation_id);
  auto genesis_commit_index = r.ReadU64();
  LAVIK_RETURN_IF_ERROR(genesis_commit_index);
  auto failure_summary = r.ReadString(kMaxMetaClusterFailureSummaryBytes);
  LAVIK_RETURN_IF_ERROR(failure_summary);
  if (*lifecycle_state >
      static_cast<std::uint8_t>(MetaClusterLifecycle::kProvisioningFailed)) {
    return MetaFailStopError("unknown cluster lifecycle tag");
  }
  auto topology_epoch = r.ReadU64();
  LAVIK_RETURN_IF_ERROR(topology_epoch);
  auto group_count = r.ReadCount(kMaxMetaGroups);
  LAVIK_RETURN_IF_ERROR(group_count);

  MetaTopologyStore store;
  store.cluster_lifecycle_.state_ =
      static_cast<MetaClusterLifecycle>(*lifecycle_state);
  if (*client_mode != 0) {
    store.cluster_lifecycle_.client_mode_ =
        static_cast<ClientMode>(*client_mode - 1);
  }

  store.cluster_lifecycle_.root_operation_id_ = *root_operation_id;
  store.cluster_lifecycle_.genesis_commit_index_ = *genesis_commit_index;
  store.cluster_lifecycle_.failure_summary_ = std::move(*failure_summary);
  LAVIK_RETURN_IF_ERROR(ValidateClusterLifecycle(store.cluster_lifecycle_));
  store.topology_epoch_ = *topology_epoch;
  for (std::uint32_t i = 0; i < *group_count; ++i) {
    auto group_id = r.ReadString(kMaxMetaGroupIdBytes);
    LAVIK_RETURN_IF_ERROR(group_id);
    auto owner = r.ReadString(kMetaNodeIdBytes);
    LAVIK_RETURN_IF_ERROR(owner);
    auto group_term = r.ReadU64();
    LAVIK_RETURN_IF_ERROR(group_term);
    auto active = r.ReadBool("invalid authority state");
    LAVIK_RETURN_IF_ERROR(active);
    auto action = r.ReadOptional<MetaFailoverActionId>(
        [](MetaReader& nested) { return ReadFixedArray<16>(nested); });
    LAVIK_RETURN_IF_ERROR(action);
    if ((*active && (*group_term == 0 || owner->empty())) ||
        (action->has_value() && (!*active || IsZero(**action)))) {
      return MetaFailStopError("invalid group authority");
    }
    auto manifest_revision = r.ReadU64();
    LAVIK_RETURN_IF_ERROR(manifest_revision);
    auto manifest_digest = ReadFixedArray<32>(r);
    LAVIK_RETURN_IF_ERROR(manifest_digest);
    auto partition_epoch = r.ReadU64();
    LAVIK_RETURN_IF_ERROR(partition_epoch);
    auto revision = r.ReadU64();
    LAVIK_RETURN_IF_ERROR(revision);
    auto failover_transition = r.ReadOptional<MetaFailoverTransition>(
        [](MetaReader& nested) { return ReadMetaFailoverTransition(nested); });
    LAVIK_RETURN_IF_ERROR(failover_transition);
    auto members = r.ReadList<MetaGroupMember>(
        kMaxMetaNodes, [](MetaReader& rr) -> absl::StatusOr<MetaGroupMember> {
          auto node_id = rr.ReadString(kMetaNodeIdBytes);
          LAVIK_RETURN_IF_ERROR(node_id);
          auto assignment_id = ReadFixedArray<16>(rr);
          LAVIK_RETURN_IF_ERROR(assignment_id);
          auto role = rr.ReadU8();
          LAVIK_RETURN_IF_ERROR(role);
          if (*role != static_cast<std::uint8_t>(MetaNodeRole::kPrimary) &&
              *role != static_cast<std::uint8_t>(MetaNodeRole::kReplica)) {
            return MetaFailStopError("unknown node role");
          }
          return MetaGroupMember{std::string(*node_id), *assignment_id,
                                 static_cast<MetaNodeRole>(*role)};
        });
    LAVIK_RETURN_IF_ERROR(members);

    // Invariant enforcement (fail-stop): a corrupt snapshot fails
    // identically on every node.
    if (group_id->empty()) {
      return MetaFailStopError("empty group_id in snapshot");
    }
    if (*revision == 0) {
      return MetaFailStopError("revision 0 in snapshot");
    }
    if (store.groups_.contains(std::string(*group_id))) {
      return MetaFailStopError("duplicate group_id in snapshot");
    }
    GroupState group;
    group.authority_active_ = *active;
    group.activation_action_id_ = std::move(*action);
    group.record_.owner_ = std::string(*owner);
    group.record_.group_term_ = *group_term;
    group.record_.population_manifest_revision_ = *manifest_revision;
    group.record_.population_manifest_digest_ = *manifest_digest;
    group.record_.partition_replication_epoch_ = *partition_epoch;
    group.revision_ = *revision;
    group.failover_transition_ = std::move(*failover_transition);
    if ((group.record_.population_manifest_revision_ == 0) !=
        IsZero(group.record_.population_manifest_digest_)) {
      return MetaFailStopError(
          "manifest revision/digest invariant violated in snapshot");
    }
    for (const MetaGroupMember& member : *members) {
      if (member.node_id_.empty() || IsZero(member.assignment_id_)) {
        return MetaFailStopError(
            "empty member node_id or zero assignment_id in snapshot");
      }
      if (!group.members_
               .emplace(
                   member.node_id_,
                   GroupState::MemberState{member.assignment_id_, member.role_})
               .second) {
        return MetaFailStopError("duplicate member in snapshot");
      }
      // One-node-one-group must hold in the decoded state too.
      if (!store.group_of_node_.emplace(member.node_id_, std::string(*group_id))
               .second) {
        return MetaFailStopError("node in two groups in snapshot");
      }
    }
    store.groups_.emplace(std::string(*group_id), std::move(group));
  }

  auto assignment_history =
      r.ReadList<std::pair<std::string, MetaAssignmentId>>(
          kMaxMetaNodes,
          [](MetaReader& rr)
              -> absl::StatusOr<std::pair<std::string, MetaAssignmentId>> {
            auto node_id = rr.ReadString(kMetaNodeIdBytes);
            LAVIK_RETURN_IF_ERROR(node_id);
            auto assignment_id = ReadFixedArray<16>(rr);
            LAVIK_RETURN_IF_ERROR(assignment_id);
            return std::pair<std::string, MetaAssignmentId>{
                std::string(*node_id), *assignment_id};
          });
  LAVIK_RETURN_IF_ERROR(assignment_history);
  for (const auto& [node_id, assignment_id] : *assignment_history) {
    if (node_id.empty() || IsZero(assignment_id) ||
        !store.last_assignment_by_node_.emplace(node_id, assignment_id)
             .second) {
      return MetaFailStopError("invalid assignment history in snapshot");
    }
  }
  for (const auto& [node_id, group_id] : store.group_of_node_) {
    const auto history = store.last_assignment_by_node_.find(node_id);
    const auto group = store.groups_.find(group_id);
    const auto member = group->second.members_.find(node_id);
    if (history == store.last_assignment_by_node_.end() ||
        history->second != member->second.assignment_id_) {
      return MetaFailStopError(
          "active membership does not match assignment history");
    }
  }

  auto runs = r.ReadList<MetaSlotAssignment>(
      kMetaSlotCount, [](MetaReader& rr) -> absl::StatusOr<MetaSlotAssignment> {
        auto first = rr.ReadU16();
        LAVIK_RETURN_IF_ERROR(first);
        auto last = rr.ReadU16();
        LAVIK_RETURN_IF_ERROR(last);
        auto group_id = rr.ReadString(kMaxMetaGroupIdBytes);
        LAVIK_RETURN_IF_ERROR(group_id);
        if (*first > *last || *last >= kMetaSlotCount) {
          return MetaFailStopError("slot run out of bounds");
        }
        return MetaSlotAssignment{static_cast<std::uint16_t>(*first),
                                  static_cast<std::uint16_t>(*last),
                                  std::string(*group_id)};
      });
  LAVIK_RETURN_IF_ERROR(runs);
  std::uint32_t previous_last = 0;
  for (std::size_t i = 0; i < runs->size(); ++i) {
    const MetaSlotAssignment& run = (*runs)[i];
    // Runs must be strictly ascending; this also rejects overlaps.
    if (i > 0 && run.first_slot_ <= previous_last) {
      return MetaFailStopError("overlapping or unsorted slot runs");
    }
    previous_last = run.last_slot_;
    if (!store.groups_.contains(run.group_id_)) {
      return MetaFailStopError("slot run references unknown group");
    }
    for (std::uint32_t slot = run.first_slot_; slot <= run.last_slot_; ++slot) {
      store.slots_[slot] = run.group_id_;
    }
  }
  LAVIK_RETURN_IF_ERROR(r.Finish());
  if (store.cluster_lifecycle_.client_mode_ == ClientMode::kSingle &&
      (store.groups_.size() > 1 ||
       ((!runs->empty() ||
         store.cluster_lifecycle_.state_ == MetaClusterLifecycle::kCreated) &&
        (store.groups_.size() != 1 ||
         std::any_of(
             store.slots_.begin(), store.slots_.end(),
             [](const std::string& owner) { return owner.empty(); }))))) {
    return MetaFailStopError("invalid Single topology in snapshot");
  }
  return store;
}

}  // namespace lavik::meta
