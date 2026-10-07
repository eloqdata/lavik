/* Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0 */

#include "lavik/meta/observation_facts_view.h"

#include <algorithm>
#include <limits>
#include <tuple>
#include <utility>

namespace lavik::meta {

MetaObservationFactsView::MetaObservationFactsView(
    std::vector<std::string> active_nodes,
    std::vector<MetaObservationGroupFacts> groups, MetaCommittedCursor cursor)
    : active_nodes_(std::move(active_nodes)),
      groups_(std::move(groups)),
      cursor_(cursor) {
  std::size_t transition_count = 0;
  std::size_t candidate_count = 0;
  for (const auto& group : groups_) {
    if (!group.failover_transition_.has_value()) continue;
    ++transition_count;
    candidate_count +=
        group.failover_transition_->candidate_action_.has_value();
  }
  transition_index_.reserve(transition_count);
  candidate_index_.reserve(candidate_count);
  for (std::size_t index = 0; index < groups_.size(); ++index) {
    const auto& transition = groups_[index].failover_transition_;
    if (!transition.has_value()) continue;
    transition_index_.push_back(index);
    if (transition->candidate_action_.has_value()) {
      candidate_index_.push_back(index);
    }
  }
  std::sort(
      transition_index_.begin(), transition_index_.end(),
      [&](std::size_t left, std::size_t right) {
        // Preserve the first-Group lookup even if a test fixture
        // supplies duplicate transition identities across Groups.
        return std::tie(groups_[left].failover_transition_->transition_id_,
                        left) <
               std::tie(groups_[right].failover_transition_->transition_id_,
                        right);
      });
  std::sort(
      candidate_index_.begin(), candidate_index_.end(),
      [&](std::size_t left, std::size_t right) {
        const auto& a =
            groups_[left].failover_transition_->candidate_action_->candidate_;
        const auto& b =
            groups_[right].failover_transition_->candidate_action_->candidate_;
        return std::tie(a.node_id_, a.boot_id_) <
               std::tie(b.node_id_, b.boot_id_);
      });
}

const MetaObservationGroupFacts* MetaObservationFactsView::FindGroup(
    std::string_view group_id) const {
  const auto found =
      std::lower_bound(groups_.begin(), groups_.end(), group_id,
                       [](const auto& group, std::string_view id) {
                         return group.group_id_ < id;
                       });
  return found != groups_.end() && found->group_id_ == group_id ? &*found
                                                                : nullptr;
}

std::optional<MetaAssignmentId> MetaObservationFactsView::AssignmentFor(
    std::string_view group_id, std::string_view node_id) const {
  const auto* group = FindGroup(group_id);
  if (!group) return std::nullopt;
  const auto member = std::lower_bound(
      group->members_.begin(), group->members_.end(), node_id,
      [](const auto& value, auto id) { return value.node_id_ < id; });
  if (member == group->members_.end() || member->node_id_ != node_id)
    return std::nullopt;
  return member->assignment_id_;
}

bool MetaObservationFactsView::IsActiveNode(std::string_view node_id) const {
  return std::binary_search(active_nodes_.begin(), active_nodes_.end(),
                            node_id);
}

std::uint64_t MetaObservationFactsView::CurrentGroupTerm(
    std::string_view group_id) const {
  const auto* group = FindGroup(group_id);
  return group != nullptr ? group->record_.group_term_ : 0;
}

std::uint64_t MetaObservationFactsView::CurrentPopulationManifestRevision(
    std::string_view group_id) const {
  const auto* group = FindGroup(group_id);
  return group != nullptr ? group->record_.population_manifest_revision_ : 0;
}

MetaHash256 MetaObservationFactsView::CurrentPopulationManifestDigest(
    std::string_view group_id) const {
  const auto* group = FindGroup(group_id);
  return group != nullptr ? group->record_.population_manifest_digest_
                          : MetaHash256{};
}

std::uint64_t MetaObservationFactsView::CurrentPartitionReplicationEpoch(
    std::string_view group_id) const {
  const auto* group = FindGroup(group_id);
  return group != nullptr ? group->record_.partition_replication_epoch_ : 0;
}

bool MetaObservationFactsView::AssignmentMatches(
    const MetaObservationGroupFacts& group, std::string_view node_id,
    const MetaAssignmentId& assignment_id) {
  const auto member =
      std::lower_bound(group.members_.begin(), group.members_.end(), node_id,
                       [](const auto& value, std::string_view id) {
                         return value.node_id_ < id;
                       });
  return member != group.members_.end() && member->node_id_ == node_id &&
         member->assignment_id_ == assignment_id;
}

bool MetaObservationFactsView::AssignmentMatches(
    std::string_view group_id, std::string_view node_id,
    const MetaAssignmentId& assignment_id) const {
  const auto* group = FindGroup(group_id);
  return group != nullptr && AssignmentMatches(*group, node_id, assignment_id);
}

bool MetaObservationFactsView::IsOwnerAssignment(
    std::string_view group_id, std::string_view node_id,
    const MetaAssignmentId& assignment_id) const {
  const auto* group = FindGroup(group_id);
  return group != nullptr && group->record_.owner_ == node_id &&
         AssignmentMatches(*group, node_id, assignment_id);
}

bool MetaObservationFactsView::MayReportFencedOwnerCandidate(
    const MetaCandidateProgressObs& candidate) const {
  const auto* group = FindGroup(candidate.group_id_);
  if (group == nullptr || group->authority_active_ ||
      !group->failover_transition_.has_value() ||
      group->failover_transition_->mode_ != MetaFailoverMode::kUncontrolled ||
      group->failover_transition_->target_term_ != group->record_.group_term_ ||
      candidate.group_term_ != group->record_.group_term_ ||
      candidate.source_group_term_ ==
          std::numeric_limits<std::uint64_t>::max() ||
      candidate.source_group_term_ + 1 != candidate.group_term_ ||
      group->record_.owner_ != candidate.node_id_) {
    return false;
  }
  // Authority and record share the same GroupState term; capture stores their
  // common term once. Only exact current assignments retain historical-owner
  // eligibility while fenced under an uncontrolled transition.
  return AssignmentMatches(*group, candidate.node_id_,
                           candidate.assignment_id_);
}

bool MetaObservationFactsView::IsCurrentFailoverCandidate(
    std::string_view node_id, const MetaBootIncarnation& boot_id) const {
  const auto key = std::pair(node_id, boot_id);
  const auto found = std::lower_bound(
      candidate_index_.begin(), candidate_index_.end(), key,
      [&](std::size_t index, const auto& candidate_key) {
        const auto& candidate =
            groups_[index].failover_transition_->candidate_action_->candidate_;
        return std::pair(std::string_view(candidate.node_id_),
                         candidate.boot_id_) < candidate_key;
      });
  if (found == candidate_index_.end()) return false;
  const auto& candidate =
      groups_[*found].failover_transition_->candidate_action_->candidate_;
  return candidate.node_id_ == node_id && candidate.boot_id_ == boot_id;
}

std::optional<MetaCommittedFacts::FailoverTransitionView>
MetaObservationFactsView::FailoverTransitionById(
    const MetaFailoverTransitionId& transition_id) const {
  const auto found = std::lower_bound(
      transition_index_.begin(), transition_index_.end(), transition_id,
      [&](std::size_t index, const auto& id) {
        return groups_[index].failover_transition_->transition_id_ < id;
      });
  if (found == transition_index_.end()) return std::nullopt;
  const auto& group = groups_[*found];
  if (group.failover_transition_->transition_id_ != transition_id) {
    return std::nullopt;
  }
  return FailoverTransitionView{group.group_id_, *group.failover_transition_};
}

}  // namespace lavik::meta
