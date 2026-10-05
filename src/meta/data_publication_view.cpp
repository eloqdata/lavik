/* Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0 */
#include "lavik/meta/data_publication_view.h"

#include <algorithm>
#include <limits>
#include <set>
#include <utility>

#include "lavik/meta/state_apply.h"

namespace lavik::meta {
namespace {
template <typename Records, typename Key, typename Member>
const typename Records::value_type* Find(const Records& records, const Key& key,
                                         Member member) {
  auto it = std::lower_bound(records.begin(), records.end(), key,
                             [member](const auto& record, const auto& id) {
                               return record.*member < id;
                             });
  return it != records.end() && (*it).*member == key ? &*it : nullptr;
}
}  // namespace

MetaDataPublicationView::CaptureData MetaDataPublicationView::Extract(
    const MetaStores& stores, MetaCommittedCursor cursor) {
  CaptureData capture;
  auto& data = capture.selected_;
  data.cursor_ = cursor;
  data.lifecycle_ = stores.topology_.ClusterLifecycle();
  data.topology_epoch_ = stores.topology_.TopologyEpoch();
  data.nodes_ = stores.identity_.Nodes();
  data.meta_members_ = stores.identity_.MetaMembers();
  data.groups_ = stores.topology_.Groups();
  data.slots_ = stores.topology_.SlotRanges();
  data.authorities_.reserve(data.groups_.size());
  std::set<MetaHash256> manifests;
  for (const auto& group : data.groups_) {
    data.authorities_.push_back(
        *stores.topology_.AuthorityFor(group.group_id_));
    if (group.record_.population_manifest_revision_ != 0)
      manifests.insert(group.record_.population_manifest_digest_);
  }
  // Inspect borrowed headers/directives before copying. Capacity must grow
  // with selected records, never the size of the unrelated live journal.
  for (const auto& operation : stores.operation_.LiveOperationsView()) {
    const bool creation_root =
        data.lifecycle_.state_ == MetaClusterLifecycle::kCreating &&
        operation.operation_id_ == data.lifecycle_.root_operation_id_;
    if (operation.current_directives_.empty() && !creation_root) continue;
    data.operations_.push_back(operation);
    // Whole records preserve cross-recipient AuthorizeSource and its exact
    // receipt revision; the rebuild's revision is deliberately different.
    for (const auto& directive : operation.current_directives_)
      if (directive.spec_.population_manifest_revision_ != 0)
        manifests.insert(directive.spec_.population_manifest_digest_);
  }
  for (const auto& digest : manifests) {
    auto document = stores.population_manifest_.Find(digest);
    if (document) data.manifests_.emplace_back(digest, std::move(*document));
  }
  capture.lease_policy_ =
      stores.policy_.CurrentVersion(std::string(kAuthorityLeasePolicyId));
  return capture;
}

MetaDataPublicationView::MetaDataPublicationView(CaptureData capture)
    : data_(std::move(capture.selected_)) {
  if (capture.lease_policy_) {
    auto decoded = DecodeAuthorityLeasePolicy(capture.lease_policy_->content_);
    if (decoded.ok()) {
      decoded->version_ = capture.lease_policy_->version_;
      authority_lease_ = *decoded;
    }
  }
}

MetaDataPublicationView MetaDataPublicationView::FromStores(
    const MetaStores& stores, MetaCommittedCursor cursor) {
  return MetaDataPublicationView(Extract(stores, cursor));
}

const MetaNodeRecord* MetaDataPublicationView::FindNode(
    std::string_view id) const {
  return Find(data_.nodes_, id, &MetaNodeRecord::node_id_);
}
const MetaMemberRecord* MetaDataPublicationView::FindMetaMember(
    std::uint32_t id) const {
  return Find(data_.meta_members_, id, &MetaMemberRecord::server_id_);
}
const MetaTopologyGroupView* MetaDataPublicationView::FindGroup(
    std::string_view id) const {
  return Find(data_.groups_, id, &MetaTopologyGroupView::group_id_);
}
const MetaGroupAuthorityView* MetaDataPublicationView::AuthorityFor(
    std::string_view id) const {
  const auto* group = FindGroup(id);
  return group ? &data_.authorities_[group - data_.groups_.data()] : nullptr;
}
const MetaOperationRecord* MetaDataPublicationView::FindOperation(
    const MetaOperationId& id) const {
  return Find(data_.operations_, id, &MetaOperationRecord::operation_id_);
}
const MetaPopulationManifestDocument* MetaDataPublicationView::FindManifest(
    const MetaHash256& digest) const {
  auto it =
      std::lower_bound(data_.manifests_.begin(), data_.manifests_.end(), digest,
                       [](const auto& entry, const MetaHash256& key) {
                         return entry.first < key;
                       });
  return it != data_.manifests_.end() && it->first == digest ? &it->second
                                                             : nullptr;
}
absl::Status MetaDataPublicationView::ValidateDirectiveAnchor(
    const MetaDirectiveSpec& directive) const {
  const bool active =
      IsActiveNode(directive.recipient_node_id_) &&
      IsActiveNode(directive.target_node_id_) &&
      (directive.kind_ == kMetaDirectiveInitializeEmptyPopulation ||
       IsActiveNode(directive.source_node_id_));
  return ValidateCommittedDirectiveAnchor(directive, active,
                                          FindGroup(directive.group_id_),
                                          AuthorityFor(directive.group_id_));
}

bool MetaDataPublicationView::IsActiveNode(std::string_view node_id) const {
  const auto* node = FindNode(node_id);
  return node != nullptr && !node->retired_;
}

uint64_t MetaDataPublicationView::CurrentGroupTerm(
    std::string_view group_id) const {
  // Conservative "unknown" per the MetaCommittedFacts contract: 0.
  const auto* group = FindGroup(group_id);
  return group ? group->record_.group_term_ : 0;
}

uint64_t MetaDataPublicationView::CurrentPopulationManifestRevision(
    std::string_view group_id) const {
  const auto group = FindGroup(group_id);
  return group != nullptr ? group->record_.population_manifest_revision_ : 0;
}

MetaHash256 MetaDataPublicationView::CurrentPopulationManifestDigest(
    std::string_view group_id) const {
  const auto group = FindGroup(group_id);
  return group != nullptr ? group->record_.population_manifest_digest_
                          : MetaHash256{};
}

uint64_t MetaDataPublicationView::CurrentPartitionReplicationEpoch(
    std::string_view group_id) const {
  const auto group = FindGroup(group_id);
  return group != nullptr ? group->record_.partition_replication_epoch_ : 0;
}

bool MetaDataPublicationView::AssignmentMatches(
    std::string_view group_id, std::string_view node_id,
    const MetaAssignmentId& assignment_id) const {
  const auto group = FindGroup(group_id);
  return group != nullptr &&
         std::any_of(group->members_.begin(), group->members_.end(),
                     [&](const MetaGroupMember& member) {
                       return member.node_id_ == node_id &&
                              member.assignment_id_ == assignment_id;
                     });
}

bool MetaDataPublicationView::IsOwnerAssignment(
    std::string_view group_id, std::string_view node_id,
    const MetaAssignmentId& assignment_id) const {
  const auto group = FindGroup(group_id);
  return group != nullptr && group->record_.owner_ == node_id &&
         std::any_of(group->members_.begin(), group->members_.end(),
                     [&](const MetaGroupMember& member) {
                       return member.node_id_ == node_id &&
                              member.assignment_id_ == assignment_id;
                     });
}

bool MetaDataPublicationView::MayReportFencedOwnerCandidate(
    const MetaCandidateProgressObs& candidate) const {
  const auto group = FindGroup(candidate.group_id_);
  const auto grant = AuthorityFor(candidate.group_id_);
  if (group == nullptr || grant == nullptr ||
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

bool MetaDataPublicationView::IsCurrentFailoverCandidate(
    std::string_view node_id, const MetaBootIncarnation& boot_id) const {
  return std::ranges::any_of(groups(), [&](const MetaTopologyGroupView& group) {
    return group.failover_transition_.has_value() &&
           group.failover_transition_->candidate_action_.has_value() &&
           group.failover_transition_->candidate_action_->candidate_.node_id_ ==
               node_id &&
           group.failover_transition_->candidate_action_->candidate_.boot_id_ ==
               boot_id;
  });
}

std::optional<MetaCommittedFacts::FailoverTransitionView>
MetaDataPublicationView::FailoverTransitionById(
    const MetaFailoverTransitionId& transition_id) const {
  for (const MetaTopologyGroupView& group : groups()) {
    if (group.failover_transition_.has_value() &&
        group.failover_transition_->transition_id_ == transition_id) {
      return FailoverTransitionView{group.group_id_,
                                    *group.failover_transition_};
    }
  }
  return std::nullopt;
}

}  // namespace lavik::meta
