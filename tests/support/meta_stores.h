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

#include <algorithm>
#include <limits>

#include "lavik/meta/observation_store.h"
#include "lavik/meta/state_machine.h"

namespace lavik::meta {

// Full-state test oracle. Production readers use purpose-specific captures.
class FullStoresTestView {
 public:
  FullStoresTestView(MetaStores stores, std::uint64_t applied_index)
      : stores_(std::move(stores)), applied_index_(applied_index) {}

  const MetaStores& stores() const { return stores_; }
  // Highest raft log index whose effects the stores reflect.
  std::uint64_t applied_index() const { return applied_index_; }

  const MetaIdentityStore& identity() const { return stores_.identity_; }
  const MetaTopologyStore& topology() const { return stores_.topology_; }
  const MetaPolicyStore& policy() const { return stores_.policy_; }
  const MetaOperationStore& operation() const { return stores_.operation_; }
  const MetaPopulationManifestStore& population_manifest() const {
    return stores_.population_manifest_;
  }
  const MetaAuditStore& audit() const { return stores_.audit_; }

 private:
  MetaStores stores_;
  std::uint64_t applied_index_;
};

// Legacy facts oracle for differential tests; the fixture stores must outlive
// it.
class StoredFactsTestAdapter : public MetaCommittedFacts {
 public:
  explicit StoredFactsTestAdapter(const MetaStores& stores) : stores_(stores) {}

  bool IsActiveNode(std::string_view node_id) const override;
  uint64_t CurrentGroupTerm(std::string_view group_id) const override;
  uint64_t CurrentPopulationManifestRevision(
      std::string_view group_id) const override;
  MetaHash256 CurrentPopulationManifestDigest(
      std::string_view group_id) const override;
  uint64_t CurrentPartitionReplicationEpoch(
      std::string_view group_id) const override;
  bool AssignmentMatches(std::string_view group_id, std::string_view node_id,
                         const MetaAssignmentId& assignment_id) const override;
  bool IsOwnerAssignment(std::string_view group_id, std::string_view node_id,
                         const MetaAssignmentId& assignment_id) const override;
  bool MayReportFencedOwnerCandidate(
      const MetaCandidateProgressObs& candidate) const override;
  bool IsCurrentFailoverCandidate(
      std::string_view node_id,
      const MetaBootIncarnation& boot_id) const override;
  std::optional<FailoverTransitionView> FailoverTransitionById(
      const MetaFailoverTransitionId& transition_id) const override;

 private:
  const MetaStores& stores_;
};

inline bool StoredFactsTestAdapter::IsActiveNode(
    std::string_view node_id) const {
  return stores_.identity_.IsActiveNode(std::string(node_id));
}

inline uint64_t StoredFactsTestAdapter::CurrentGroupTerm(
    std::string_view group_id) const {
  // Conservative "unknown" per the MetaCommittedFacts contract: 0.
  return stores_.topology_.CurrentGroupTerm(group_id).value_or(0);
}

inline uint64_t StoredFactsTestAdapter::CurrentPopulationManifestRevision(
    std::string_view group_id) const {
  const auto group = stores_.topology_.FindGroup(std::string(group_id));
  return group.has_value() ? group->record_.population_manifest_revision_ : 0;
}

inline MetaHash256 StoredFactsTestAdapter::CurrentPopulationManifestDigest(
    std::string_view group_id) const {
  const auto group = stores_.topology_.FindGroup(std::string(group_id));
  return group.has_value() ? group->record_.population_manifest_digest_
                           : MetaHash256{};
}

inline uint64_t StoredFactsTestAdapter::CurrentPartitionReplicationEpoch(
    std::string_view group_id) const {
  const auto group = stores_.topology_.FindGroup(std::string(group_id));
  return group.has_value() ? group->record_.partition_replication_epoch_ : 0;
}

inline bool StoredFactsTestAdapter::AssignmentMatches(
    std::string_view group_id, std::string_view node_id,
    const MetaAssignmentId& assignment_id) const {
  const auto group = stores_.topology_.FindGroup(std::string(group_id));
  return group.has_value() &&
         std::any_of(group->members_.begin(), group->members_.end(),
                     [&](const MetaGroupMember& member) {
                       return member.node_id_ == node_id &&
                              member.assignment_id_ == assignment_id;
                     });
}

inline bool StoredFactsTestAdapter::IsOwnerAssignment(
    std::string_view group_id, std::string_view node_id,
    const MetaAssignmentId& assignment_id) const {
  const auto group = stores_.topology_.FindGroup(std::string(group_id));
  return group.has_value() && group->record_.owner_ == node_id &&
         std::any_of(group->members_.begin(), group->members_.end(),
                     [&](const MetaGroupMember& member) {
                       return member.node_id_ == node_id &&
                              member.assignment_id_ == assignment_id;
                     });
}

inline bool StoredFactsTestAdapter::MayReportFencedOwnerCandidate(
    const MetaCandidateProgressObs& candidate) const {
  const auto group =
      stores_.topology_.FindGroup(std::string(candidate.group_id_));
  const auto grant = stores_.topology_.AuthorityFor(candidate.group_id_);
  if (!group.has_value() || !grant.has_value() ||
      !group->failover_transition_.has_value() ||
      group->failover_transition_->mode_ != MetaFailoverMode::kUncontrolled ||
      group->failover_transition_->target_term_ != group->record_.group_term_ ||
      candidate.group_term_ != group->record_.group_term_ ||
      candidate.source_group_term_ ==
          std::numeric_limits<std::uint64_t>::max() ||
      candidate.source_group_term_ + 1 != candidate.group_term_ ||
      group->record_.owner_ != candidate.node_id_ ||
      grant->group_term_ != group->record_.group_term_ ||
      grant->grant_.has_value()) {
    return false;
  }
  return std::ranges::any_of(
      group->members_, [&](const MetaGroupMember& member) {
        return member.node_id_ == candidate.node_id_ &&
               member.assignment_id_ == candidate.assignment_id_;
      });
}

inline bool StoredFactsTestAdapter::IsCurrentFailoverCandidate(
    std::string_view node_id, const MetaBootIncarnation& boot_id) const {
  return std::ranges::any_of(
      stores_.topology_.Groups(), [&](const MetaTopologyGroupView& group) {
        return group.failover_transition_.has_value() &&
               group.failover_transition_->candidate_action_.has_value() &&
               group.failover_transition_->candidate_action_->candidate_
                       .node_id_ == node_id &&
               group.failover_transition_->candidate_action_->candidate_
                       .boot_id_ == boot_id;
      });
}

inline std::optional<MetaCommittedFacts::FailoverTransitionView>
StoredFactsTestAdapter::FailoverTransitionById(
    const MetaFailoverTransitionId& transition_id) const {
  for (const MetaTopologyGroupView& group : stores_.topology_.Groups()) {
    if (group.failover_transition_.has_value() &&
        group.failover_transition_->transition_id_ == transition_id) {
      return FailoverTransitionView{group.group_id_,
                                    *group.failover_transition_};
    }
  }
  return std::nullopt;
}

inline FullStoresTestView CaptureFullViewForTest(
    const MetaStateMachine& machine) {
  auto captured = machine.CaptureRecoveryStores();
  return FullStoresTestView(std::move(captured.stores_),
                            captured.cursor_.applied_index());
}

}  // namespace lavik::meta
