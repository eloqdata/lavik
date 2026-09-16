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

#include "lavik/cluster/meta_control.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "lavik/cluster/control_protocol.h"
#include "lavik/cluster/control_types.h"
#include "lavik/cluster/topology.h"
#include "lavik/numeric_endpoint.h"
#include "lavik/replication_group.h"

namespace lavik::cluster {
namespace {

bool IsZero(const control::WireId128& value) {
  return std::all_of(value.begin(), value.end(),
                     [](std::uint8_t byte) { return byte == 0; });
}

bool IsZero(const control::WireHash256& value) {
  return std::all_of(value.begin(), value.end(),
                     [](std::uint8_t byte) { return byte == 0; });
}

absl::Status Invalid(std::string message) {
  return absl::InvalidArgumentError(
      absl::StrCat("invalid Meta full desired state: ", message));
}

bool IsCanonicalNumericHost(std::string_view host) {
  const std::string encoded = host.find(':') == std::string_view::npos
                                  ? absl::StrCat(host, ":1")
                                  : absl::StrCat("[", host, "]:1");
  const auto parsed = lavik::ParseNumericEndpoint(encoded);
  return parsed.has_value() && parsed->host_ == host;
}

PreparedFailoverTransition ToPreparedFailoverTransition(
    const control::WireFailoverTransition& source) {
  PreparedFailoverTransition prepared{
      .transition_id_ = FailoverTransitionId::FromBytes(source.transition_id),
      .revision_ = source.revision,
      .mode_ = source.mode == control::WireFailoverMode::kControlled
                   ? PreparedFailoverMode::kControlled
                   : PreparedFailoverMode::kUncontrolled,
      .target_term_ = source.target_term,
      .candidate_action_ = std::nullopt,
  };
  if (!source.candidate_action.has_value()) return prepared;
  const control::WireFailoverCandidateAction& action = *source.candidate_action;
  prepared.candidate_action_ = PreparedFailoverAction{
      .action_id_ = FailoverActionId::FromBytes(action.action_id),
      .candidate_ =
          {
              .node_id_ = *NodeId::Parse(action.candidate.node_id),
              .assignment_id_ =
                  AssignmentId::FromBytes(action.candidate.assignment_id),
              .boot_id_ = *NodeId::Parse(action.candidate.boot_id),
          },
      .domain_ =
          {
              .source_group_term_ = action.domain.source_group_term,
              .source_node_id_ =
                  action.operator_recovery
                      ? NodeId{}
                      : *NodeId::Parse(action.domain.source_node_id),
              .source_assignment_id_ =
                  AssignmentId::FromBytes(action.domain.source_assignment_id),
              .source_boot_id_ =
                  action.operator_recovery
                      ? NodeId{}
                      : *NodeId::Parse(action.domain.source_boot_id),
              .source_history_id_ =
                  action.operator_recovery
                      ? NodeId{}
                      : *NodeId::Parse(action.domain.source_history_id),
              .flow_count_ = action.domain.flow_count,
          },
      .authorization_ = std::nullopt,
      .operator_recovery_ = action.operator_recovery,
  };
  if (action.authorization.has_value()) {
    prepared.candidate_action_->authorization_ = PreparedFailoverAuthorization{
        .authorized_revision_ = action.authorization->authorized_revision,
        .loss_if_cutover_ = action.authorization->loss_if_cutover ==
                                    control::WireFailoverLoss::kNone
                                ? PreparedFailoverLoss::kNone
                                : PreparedFailoverLoss::kUnknown,
    };
  }
  return prepared;
}

}  // namespace

absl::StatusOr<PreparedFullState> PrepareMetaFullState(
    const control::FullDesiredState& desired, std::string_view local_node_id,
    std::size_t request_worker_count) {
  if (!control::IsCanonicalIdentity160(local_node_id)) {
    return Invalid("local node id is not canonical");
  }
  if (request_worker_count == 0) {
    return Invalid("request worker count is zero");
  }
  if (desired.control_revision == 0) {
    return Invalid("local control revision is zero");
  }
  if (auto status = control::ValidateFullDesiredState(desired); !status.ok()) {
    return Invalid(std::string(status.message()));
  }

  std::map<std::pair<std::uint64_t, Sha256Digest>,
           std::vector<PopulationManifestEntry>>
      manifests;
  for (const control::WireManifestDocument& document : desired.manifests) {
    std::vector<PopulationManifestEntry> entries;
    entries.reserve(document.entries.size());
    for (const control::WireManifestEntry& entry : document.entries) {
      entries.push_back({entry.partition_id, entry.logical_epoch});
    }
    auto manifest = PopulationManifest::Create(entries);
    if (!manifest.ok() || document.revision == 0 ||
        manifest->id().bytes_ != document.digest ||
        !manifests
             .emplace(std::make_pair(document.revision, document.digest),
                      std::move(entries))
             .second) {
      return Invalid("manifest document is non-canonical or duplicated");
    }
  }

  ServingStateBuilder builder;
  builder.SetInFlightStripeCount(request_worker_count);
  std::map<std::string, NodeIndex> node_indices;
  std::vector<NodeDescriptor> nodes;
  nodes.reserve(desired.nodes.size());
  for (const control::WireDataEndpoint& endpoint : desired.nodes) {
    const std::optional<NodeId> id = NodeId::Parse(endpoint.node_id);
    if (!id.has_value()) return Invalid("data node id is not canonical");
    if (!IsCanonicalNumericHost(endpoint.host) ||
        (endpoint.port == 0 && endpoint.tls_port == 0)) {
      return Invalid(absl::StrCat("node ", endpoint.node_id,
                                  " has no canonical numeric client endpoint"));
    }
    const NodeIndex index = static_cast<NodeIndex>(nodes.size());
    if (!node_indices.emplace(endpoint.node_id, index).second) {
      return Invalid(absl::StrCat("duplicate node ", endpoint.node_id));
    }
    NodeDescriptor node;
    node.node_id_ = *id;
    node.port_ = endpoint.port;
    node.tls_port_ = endpoint.tls_port;
    node.SetHost(endpoint.host);
    nodes.push_back(std::move(node));
  }
  const auto self = node_indices.find(std::string(local_node_id));
  if (self == node_indices.end()) {
    return Invalid("the local active node is absent from its projection");
  }

  // Resolve the primary pointer of every node before handing the table to the
  // builder. One-node-one-group is a committed Meta invariant, but this
  // untrusted boundary rechecks it instead of relying on the sender.
  std::set<std::string> assigned_nodes;
  for (const control::WireDesiredGroup& group : desired.groups) {
    builder.IncludeGroupTerm(group.group_term);
    if (group.group_id.empty()) return Invalid("group id is empty");
    if ((group.manifest_revision == 0) != IsZero(group.manifest_digest)) {
      return Invalid(absl::StrCat("group ", group.group_id,
                                  " has a partial manifest identity"));
    }
    if (group.manifest_revision != 0 &&
        !manifests.contains({group.manifest_revision, group.manifest_digest})) {
      return Invalid(absl::StrCat("group ", group.group_id,
                                  " references an absent manifest"));
    }
    if (group.owner_node_id.has_value() !=
        group.owner_assignment_id.has_value()) {
      return Invalid(absl::StrCat("group ", group.group_id,
                                  " has a partial owner identity"));
    }
    if (group.grant_active && !group.owner_node_id.has_value()) {
      return Invalid(absl::StrCat("group ", group.group_id,
                                  " active grant has no serving owner"));
    }
    std::optional<NodeIndex> owner_index;
    if (group.owner_node_id.has_value()) {
      if (group.group_term == 0) {
        return Invalid(absl::StrCat("group ", group.group_id,
                                    " has an incomplete owner authority"));
      }
      const auto owner = node_indices.find(*group.owner_node_id);
      if (owner == node_indices.end()) {
        return Invalid(
            absl::StrCat("group ", group.group_id, " names an unknown owner"));
      }
      owner_index = owner->second;
    }
    bool owner_member = false;
    for (const control::WireDesiredMember& member : group.members) {
      const auto node = node_indices.find(member.node_id);
      if (node == node_indices.end()) {
        return Invalid(
            absl::StrCat("group ", group.group_id, " names an unknown member"));
      }
      if (IsZero(member.assignment_id)) {
        return Invalid(absl::StrCat("group ", group.group_id,
                                    " has an empty member assignment"));
      }
      if (!assigned_nodes.insert(member.node_id).second) {
        return Invalid(absl::StrCat("node ", member.node_id,
                                    " is assigned more than once"));
      }
      if (group.owner_node_id.has_value() &&
          member.node_id == *group.owner_node_id) {
        owner_member = true;
        if (member.assignment_id != *group.owner_assignment_id) {
          return Invalid(absl::StrCat("group ", group.group_id,
                                      " owner assignment does not match"));
        }
      } else if (group.grant_active && owner_index.has_value()) {
        nodes[node->second].primary_node_index_ = *owner_index;
      }
      nodes[node->second].group_term_ = group.group_term;
    }
    if (group.owner_node_id.has_value() &&
        (!owner_member || IsZero(*group.owner_assignment_id))) {
      return Invalid(absl::StrCat("group ", group.group_id,
                                  " has no valid owner assignment"));
    }
  }

  builder.SetTopologyEpoch(desired.topology_epoch)
      .SetSelfNodeIndex(self->second);
  for (NodeDescriptor& node : nodes) builder.AddNode(std::move(node));

  std::vector<DesiredClusterControl> desired_cluster_controls;
  desired_cluster_controls.reserve(desired.groups.size());
  for (const control::WireDesiredGroup& source : desired.groups) {
    PreparedGroupControlIdentity control_group{
        .group_id_ = source.group_id,
        .group_term_ = source.group_term,
        .manifest_revision_ = source.manifest_revision,
        .manifest_digest_ = source.manifest_digest,
        .partition_replication_epoch_ = source.partition_replication_epoch,
        .members_ = {},
    };
    control_group.members_.reserve(source.members.size());
    for (const control::WireDesiredMember& member : source.members) {
      control_group.members_.push_back(PreparedMemberAssignment{
          .node_id_ = *NodeId::Parse(member.node_id),
          .assignment_id_ = AssignmentId::FromBytes(member.assignment_id),
      });
    }
    DesiredClusterControl desired_control{
        .identity_ = std::move(control_group),
        .owner_ = std::nullopt,
        .grant_active_ = source.grant_active,
        .activation_action_id_ = std::nullopt,
        .failover_transition_ = std::nullopt,
        .owner_endpoint_ = std::nullopt,
        .manifest_entries_ = {},
        .steady_replication_enabled_ = source.steady_replication_enabled,
        .population_transition_expected_ = false,
    };
    if (source.owner_node_id.has_value()) {
      desired_control.owner_ = PreparedMemberAssignment{
          .node_id_ = *NodeId::Parse(*source.owner_node_id),
          .assignment_id_ =
              AssignmentId::FromBytes(*source.owner_assignment_id),
      };
      const control::WireDataEndpoint& endpoint =
          desired.nodes[node_indices.at(*source.owner_node_id)];
      desired_control.owner_endpoint_ = PreparedReplicationEndpoint{
          .node_id_ = desired_control.owner_->node_id_,
          .host_ = endpoint.host,
          .port_ = endpoint.port,
          .tls_port_ = endpoint.tls_port,
      };
    }
    if (source.manifest_revision != 0) {
      desired_control.manifest_entries_ =
          manifests.at({source.manifest_revision, source.manifest_digest});
    }
    if (source.activation_action_id.has_value()) {
      desired_control.activation_action_id_ =
          FailoverActionId::FromBytes(*source.activation_action_id);
    }
    if (source.failover_transition.has_value()) {
      desired_control.failover_transition_ =
          ToPreparedFailoverTransition(*source.failover_transition);
    }
    desired_cluster_controls.push_back(std::move(desired_control));

    // Durable owner intent remains available for heartbeat role
    // classification while fenced. Its slots remain unbound until Meta
    // commits a fresh activation.
    if (!source.grant_active) continue;
    const NodeIndex primary = node_indices.at(*source.owner_node_id);
    GroupView group;
    group.group_id_ = source.group_id;
    group.primary_node_index_ = primary;
    group.assignment_id_ = AssignmentId::FromBytes(*source.owner_assignment_id);
    group.granted_ = source.grant_active;
    const bool local_member =
        std::any_of(source.members.begin(), source.members.end(),
                    [&](const control::WireDesiredMember& member) {
                      return member.node_id == local_node_id;
                    });
    // A local population proof is process-memory state and must be supplied by
    // ReplicationManager after every boot. Remote groups stay routing-ready so
    // this node can redirect their slots without claiming to serve them.
    group.population_ready_ = source.grant_active && !local_member;
    group.storage_ready_ = false;
    group.mutations_paused_ = source.owner_node_id == local_node_id &&
                              source.failover_transition.has_value() &&
                              source.failover_transition->mode ==
                                  control::WireFailoverMode::kControlled;
    group.group_term_ = source.group_term;
    group.manifest_revision_ = source.manifest_revision;
    for (const control::WireDesiredMember& member : source.members) {
      if (member.node_id != *source.owner_node_id) {
        group.replica_node_indices_.push_back(node_indices.at(member.node_id));
      }
    }
    for (const control::WireSlotRange& range : source.slot_ranges) {
      group.slot_ranges_.push_back(SlotRange{range.first, range.last});
    }
    builder.AddGroup(std::move(group));
  }

  auto state = builder.Build();
  if (!state.ok()) return Invalid(std::string(state.status().message()));
  return PreparedFullState{
      .serving_state_ = std::move(*state),
      .authority_lease_duration_ms_ = desired.authority_lease_duration_ms,
      .desired_cluster_controls_ = std::move(desired_cluster_controls),
  };
}

absl::StatusOr<PreparedFullState> PrepareNodeControlState(
    const control::NodeControlState& state, std::string_view local_node_id,
    std::size_t request_worker_count) {
  if (state.local.groups.size() > 1 || state.local.manifests.size() > 1) {
    return Invalid("Data supports one local Group population");
  }
  // Reuse local population/authority validation without retaining remote
  // assignments, manifests or failover workflows in the Data control model.
  control::FullDesiredState local{
      .control_revision = state.local.revision,
      .topology_epoch = state.routing.revision,
      .authority_lease_duration_ms = state.local.lease_duration_ms,
      .meta_directory = state.directory.endpoints,
      .nodes = state.routing.nodes,
      .groups = state.local.groups,
      .manifests = state.local.manifests,
      .current_directives = state.tasks,
  };
  for (const auto& group : local.groups) {
    if (std::none_of(group.members.begin(), group.members.end(),
                     [&](const auto& member) {
                       return member.node_id == local_node_id;
                     }))
      return Invalid("local control names a remote Group");
    const auto route =
        std::find_if(state.routing.groups.begin(), state.routing.groups.end(),
                     [&](const auto& candidate) {
                       return candidate.group_id == group.group_id;
                     });
    if (route == state.routing.groups.end() ||
        route->term != group.group_term ||
        route->owner != group.owner_node_id ||
        route->available != group.grant_active ||
        route->owner_assignment !=
            group.owner_assignment_id.value_or(control::WireId128{}) ||
        route->slots != group.slot_ranges ||
        route->members.size() != group.members.size())
      return Invalid("local control and routing disagree");
    for (std::size_t i = 0; i < route->members.size(); ++i) {
      if (route->members[i] != group.members[i].node_id)
        return Invalid("local routing membership disagrees");
    }
  }
  auto prepared =
      PrepareMetaFullState(local, local_node_id, request_worker_count);
  if (!prepared.ok()) return prepared.status();
  ServingStateBuilder builder;
  builder.SetInFlightStripeCount(request_worker_count);
  builder.SetTopologyEpoch(state.routing.revision);
  builder.SetSelfNodeIndex(prepared->serving_state_->SelfNodeIndex());
  auto nodes = prepared->serving_state_->Nodes();
  std::vector<NodeDescriptor> descriptors(nodes.begin(), nodes.end());
  std::map<std::string, NodeIndex> indices;
  for (std::size_t i = 0; i < descriptors.size(); ++i)
    indices.emplace(descriptors[i].node_id_.ToHexString(),
                    static_cast<NodeIndex>(i));
  std::set<std::string> members;
  std::set<std::string> groups;
  for (const auto& route : state.routing.groups) {
    if (route.group_id.empty() || !groups.insert(route.group_id).second)
      return Invalid("routing Group is empty or duplicated");
    builder.IncludeGroupTerm(route.term);
    const auto owner = route.owner ? indices.find(*route.owner) : indices.end();
    if (route.owner && (owner == indices.end() || route.term == 0 ||
                        IsZero(route.owner_assignment)))
      return Invalid("routing owner is invalid");
    if (route.available && !route.owner)
      return Invalid("available route has no owner");
    bool owner_member = false;
    for (const auto& member : route.members) {
      const auto found = indices.find(member);
      if (found == indices.end() || !members.insert(member).second)
        return Invalid("routing member is unknown or duplicated");
      owner_member |= route.owner == member;
      auto& node = descriptors[found->second];
      node.group_term_ = route.term;
      node.primary_node_index_ = route.available && route.owner != member
                                     ? owner->second
                                     : kNoNodeIndex;
    }
    if (route.owner && !owner_member)
      return Invalid("routing owner is not a member");
    const bool is_local = std::find(route.members.begin(), route.members.end(),
                                    local_node_id) != route.members.end();
    if (is_local && (local.groups.empty() ||
                     local.groups.front().group_id != route.group_id))
      return Invalid("routing assigns local node without local control");
    if (!route.available) continue;
    if (is_local) {
      for (const auto& group : prepared->serving_state_->Groups())
        builder.AddGroup(group);
      continue;
    }
    GroupView group;
    group.group_id_ = route.group_id;
    group.primary_node_index_ = owner->second;
    group.assignment_id_ = AssignmentId::FromBytes(route.owner_assignment);
    group.granted_ = true;
    group.population_ready_ = true;
    group.group_term_ = route.term;
    for (const auto& member : route.members)
      if (member != *route.owner)
        group.replica_node_indices_.push_back(indices.at(member));
    for (const auto& slot : route.slots)
      group.slot_ranges_.push_back({slot.first, slot.last});
    builder.AddGroup(std::move(group));
  }
  for (auto& node : descriptors) builder.AddNode(std::move(node));
  auto serving = builder.Build();
  if (!serving.ok()) return serving.status();
  prepared->serving_state_ = std::move(*serving);
  return prepared;
}

}  // namespace lavik::cluster
