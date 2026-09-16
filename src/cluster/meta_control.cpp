#include "keylane/cluster/meta_control.h"

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
#include "keylane/cluster/control_protocol.h"
#include "keylane/cluster/control_types.h"
#include "keylane/cluster/topology.h"
#include "keylane/numeric_endpoint.h"
#include "keylane/replication_group.h"

namespace keylane::cluster {
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
  const auto parsed = keylane::ParseNumericEndpoint(encoded);
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
              .source_node_id_ = *NodeId::Parse(action.domain.source_node_id),
              .source_assignment_id_ =
                  AssignmentId::FromBytes(action.domain.source_assignment_id),
              .source_boot_id_ = *NodeId::Parse(action.domain.source_boot_id),
              .source_history_id_ =
                  *NodeId::Parse(action.domain.source_history_id),
              .flow_count_ = action.domain.flow_count,
          },
      .authorization_ = std::nullopt,
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
  if (desired.source_meta_applied_index == 0) {
    return Invalid("source Meta applied index is zero");
  }
  if (IsZero(desired.projection_hash) || IsZero(desired.object_hash)) {
    return Invalid("projection or object hash is empty");
  }
  auto semantic_hash = control::ComputeProjectionHash(desired);
  if (!semantic_hash.ok()) {
    return Invalid(std::string(semantic_hash.status().message()));
  }
  if (*semantic_hash != desired.projection_hash) {
    return Invalid("projection hash does not match the semantic content");
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
      if (group.group_term == 0 || group.authority_version == 0 ||
          group.grant_revision == 0 || group.config_epoch == 0) {
        return Invalid(absl::StrCat("group ", group.group_id,
                                    " has an incomplete owner authority"));
      }
      const auto owner = node_indices.find(*group.owner_node_id);
      if (owner == node_indices.end()) {
        return Invalid(
            absl::StrCat("group ", group.group_id, " names an unknown owner"));
      }
      owner_index = owner->second;
    } else if ((group.authority_version == 0) != (group.grant_revision == 0) ||
               (group.authority_version != 0 && group.group_term == 0)) {
      // Ownerless groups cover both pre-activation and fenced states. The
      // latter retains its last authority anchors for directive/lease
      // fencing, but it never exposes that history as a serving owner.
      return Invalid(absl::StrCat("ownerless group ", group.group_id,
                                  " has partial historical authority"));
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
      nodes[node->second].config_epoch_ = group.config_epoch;
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

  std::vector<PreparedGroupControlIdentity> control_groups;
  control_groups.reserve(desired.groups.size());
  std::vector<DesiredClusterControl> desired_cluster_controls;
  desired_cluster_controls.reserve(desired.groups.size());
  for (const control::WireDesiredGroup& source : desired.groups) {
    PreparedGroupControlIdentity control_group{
        .group_id_ = source.group_id,
        .group_term_ = source.group_term,
        .authority_version_ = source.authority_version,
        .grant_revision_ = source.grant_revision,
        .config_epoch_ = source.config_epoch,
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
        .identity_ = control_group,
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
    control_groups.push_back(std::move(control_group));

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
    group.authority_version_ = source.authority_version;
    group.grant_revision_ = source.grant_revision;
    group.manifest_revision_ = source.manifest_revision;
    group.config_epoch_ = source.config_epoch;
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
      .object_hash_ = desired.object_hash,
      .control_groups_ = std::move(control_groups),
      .desired_cluster_controls_ = std::move(desired_cluster_controls),
  };
}

}  // namespace keylane::cluster
