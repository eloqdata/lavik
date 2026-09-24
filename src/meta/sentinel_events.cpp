/* Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0 */
#include <algorithm>
#include <set>

#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "lavik/meta/sentinel_discovery.h"

namespace lavik::meta {
namespace {
const MetaDataControlRuntimeNode* RuntimeNode(const MetaDiscoveryCut& cut,
                                              const std::string& id) {
  const auto it =
      std::find_if(cut.runtime_.nodes_.begin(), cut.runtime_.nodes_.end(),
                   [&](const auto& n) { return n.node_id_ == id; });
  return it == cut.runtime_.nodes_.end() ? nullptr : &*it;
}
bool Fresh(const MetaDiscoveryCut& cut,
           const MetaDataControlRuntimeNode& node) {
  return node.leader_term_ == cut.runtime_.leader_term_ &&
         node.health_.has_value() && node.health_->population_ready &&
         node.health_->storage_ready && !node.health_->draining &&
         node.health_received_unix_ms_ <= cut.now_unix_ms_ &&
         cut.now_unix_ms_ - node.health_received_unix_ms_ <=
             cut.observation_ttl_ms_;
}
std::string Address(const NumericEndpoint& endpoint) {
  return absl::StrCat(endpoint.host_, " ", endpoint.port_);
}
}  // namespace

bool ReplicaReconfigurationComplete(const MetaDiscoveryCut& cut,
                                    const MetaCommittedStatusGroup& group,
                                    const MetaGroupMember& member) {
  const auto primary = PublishablePrimary(cut, group);
  if (!primary || primary->owner_node_id_ == member.node_id_) return false;
  const auto* replica = RuntimeNode(cut, member.node_id_);
  const auto* owner = RuntimeNode(cut, primary->owner_node_id_);
  if (!replica || !owner || !Fresh(cut, *replica) || !Fresh(cut, *owner) ||
      !replica->replica_progress_)
    return false;
  const auto& proof = *replica->replica_progress_;
  const auto source = std::find_if(
      group.topology_.members_.begin(), group.topology_.members_.end(),
      [&](const auto& m) { return m.node_id_ == primary->owner_node_id_; });
  const auto& history = owner->replication_history_id_;
  const auto history_hex = absl::BytesToHexString(std::string_view(
      reinterpret_cast<const char*>(history.data()), history.size()));
  return !proof.recovered && !proof.operator_recovery &&
         proof.group_id == group.topology_.group_id_ &&
         proof.group_term == primary->group_term_ &&
         proof.assignment_id == member.assignment_id_ &&
         proof.manifest_revision ==
             group.topology_.record_.population_manifest_revision_ &&
         proof.manifest_digest ==
             group.topology_.record_.population_manifest_digest_ &&
         proof.partition_replication_epoch ==
             group.topology_.record_.partition_replication_epoch_ &&
         proof.source_group_term == primary->group_term_ &&
         proof.source_node_id == primary->owner_node_id_ &&
         source != group.topology_.members_.end() &&
         proof.source_assignment_id == source->assignment_id_ &&
         proof.source_boot_id == owner->boot_id_ &&
         proof.source_history_id == history_hex;
}

void MetaDiscoveryEvents::Reset() {
  primary_.reset();
  term_ = 0;
  replicas_.clear();
}

std::vector<MetaDiscoveryEvent> MetaDiscoveryEvents::Observe(
    const MetaDiscoveryCut& cut) {
  std::vector<MetaDiscoveryEvent> events;
  const auto* group = DiscoveryServiceGroup(cut);
  if (!group) {
    Reset();
    return events;
  }
  const auto primary = PublishablePrimary(cut, *group);
  // A fence retains the last address; it cannot manufacture an empty endpoint.
  if (!primary) return events;
  if (primary_ && primary_->group_id_ != primary->group_id_) Reset();
  const bool target_changed =
      primary_ && (primary_->owner_node_id_ != primary->owner_node_id_ ||
                   term_ != primary->group_term_);
  if (primary_ && Address(primary_->endpoint_) != Address(primary->endpoint_)) {
    events.push_back(
        {"+switch-master",
         absl::StrCat(primary->group_id_, " ", Address(primary_->endpoint_),
                      " ", Address(primary->endpoint_))});
  }
  if (target_changed) replicas_.clear();
  primary_ = primary;
  term_ = primary->group_term_;
  std::set<std::string> retained;
  for (const auto& member : group->topology_.members_) {
    if (member.node_id_ == primary->owner_node_id_) continue;
    retained.insert(member.node_id_);
    const auto* node = RuntimeNode(cut, member.node_id_);
    if (!node || !Fresh(cut, *node)) continue;
    auto [it, inserted] = replicas_.try_emplace(member.node_id_);
    auto& state = it->second;
    if (state.boot_ != node->boot_id_ ||
        state.assignment_ != member.assignment_id_) {
      state = ReplicaState{.boot_ = node->boot_id_,
                           .assignment_ = member.assignment_id_};
      inserted = true;
    }
    const bool complete = ReplicaReconfigurationComplete(cut, *group, member);
    // First seeing an already-complete replica after Meta replacement is a
    // baseline. An actual observed older lineage (or a witnessed target change)
    // is required before reporting a subsequent completion.
    if (inserted && complete) {
      state.completed_ = true;
      continue;
    }
    if (!complete) {
      state.pending_ |= target_changed || node->replica_progress_.has_value();
      continue;
    }
    if (state.pending_ && !state.completed_) {
      const auto list = ListReplicas(cut, *group, true);
      const auto replica = std::find_if(
          list.begin(), list.end(),
          [&](const auto& r) { return r.node_id_ == member.node_id_; });
      if (replica != list.end() && !replica->s_down_) {
        events.push_back(
            {"+replica-reconf-done",
             absl::StrCat("slave ", FormatNumericEndpoint(replica->endpoint_),
                          " ", Address(replica->endpoint_), " @ ",
                          primary->group_id_, " ",
                          Address(primary->endpoint_))});
        state.completed_ = true;
      }
    }
  }
  std::erase_if(replicas_, [&](const auto& entry) {
    return !retained.contains(entry.first);
  });
  return events;
}
}  // namespace lavik::meta
