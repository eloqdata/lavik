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

#include "lavik/meta/control_projector.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "lavik/cluster/control_protocol.h"
#include "lavik/meta/commands.h"
#include "lavik/meta/policy_store.h"
#include "lavik/meta/population_manifest_store.h"
#include "lavik/meta/state_apply.h"
#include "lavik/numeric_endpoint.h"

namespace lavik::meta {
namespace {

namespace control = lavik::cluster::control;

struct HostPort {
  std::string host;
  std::uint16_t port = 0;
};

enum class DataEndpointKind { kLegacy, kTcp, kTls };

struct TaggedDataEndpoint {
  DataEndpointKind kind = DataEndpointKind::kLegacy;
  HostPort endpoint;
};

template <std::size_t N>
bool IsZero(const std::array<std::uint8_t, N>& value) {
  return std::all_of(value.begin(), value.end(),
                     [](std::uint8_t byte) { return byte == 0; });
}

absl::Status Invalid(std::string message) {
  return absl::InvalidArgumentError(
      absl::StrCat("cannot project Meta committed state: ", message));
}

absl::Status Inconsistent(std::string message) {
  return absl::FailedPreconditionError(absl::StrCat(
      "cannot project inconsistent Meta committed state: ", message));
}

absl::StatusOr<HostPort> ParseNumericHostPort(std::string_view encoded,
                                              std::string_view field) {
  auto endpoint = lavik::ParseNumericEndpoint(encoded);
  if (!endpoint.has_value()) {
    return Invalid(absl::StrCat(
        field, " must be numeric IPv4:port or bracketed [IPv6]:port"));
  }
  return HostPort{std::move(endpoint->host_), endpoint->port_};
}

absl::StatusOr<TaggedDataEndpoint> ParseDataEndpoint(std::string_view encoded) {
  TaggedDataEndpoint result;
  constexpr std::string_view kTcpPrefix = "tcp://";
  constexpr std::string_view kTlsPrefix = "tls://";
  if (encoded.starts_with(kTcpPrefix)) {
    result.kind = DataEndpointKind::kTcp;
    encoded.remove_prefix(kTcpPrefix.size());
  } else if (encoded.starts_with(kTlsPrefix)) {
    result.kind = DataEndpointKind::kTls;
    encoded.remove_prefix(kTlsPrefix.size());
  }
  auto endpoint = ParseNumericHostPort(encoded, "data endpoint");
  if (!endpoint.ok()) return endpoint.status();
  result.endpoint = std::move(*endpoint);
  return result;
}

absl::StatusOr<control::WireDataEndpoint> ProjectDataEndpoint(
    const MetaNodeRecord& node) {
  if (!control::IsCanonicalIdentity160(node.node_id_)) {
    return Invalid(
        absl::StrCat("data node ", node.node_id_, " has a non-canonical id"));
  }
  if (node.endpoints_.empty() || node.endpoints_.size() > 2) {
    return Invalid(absl::StrCat("active data node ", node.node_id_,
                                " must have one or two endpoints"));
  }

  std::vector<TaggedDataEndpoint> parsed;
  parsed.reserve(node.endpoints_.size());
  bool has_explicit = false;
  bool has_legacy = false;
  for (const std::string& encoded : node.endpoints_) {
    auto endpoint = ParseDataEndpoint(encoded);
    if (!endpoint.ok()) return endpoint.status();
    has_explicit |= endpoint->kind != DataEndpointKind::kLegacy;
    has_legacy |= endpoint->kind == DataEndpointKind::kLegacy;
    parsed.push_back(std::move(*endpoint));
  }
  if (has_explicit && has_legacy) {
    return Invalid(absl::StrCat("active data node ", node.node_id_,
                                " mixes explicit and legacy endpoints"));
  }

  control::WireDataEndpoint result;
  result.node_id = node.node_id_;
  result.host = parsed.front().endpoint.host;
  for (const TaggedDataEndpoint& endpoint : parsed) {
    if (endpoint.endpoint.host != result.host) {
      return Invalid(absl::StrCat("active data node ", node.node_id_,
                                  " endpoints use different hosts"));
    }
  }

  if (!has_explicit) {
    result.port = parsed[0].endpoint.port;
    if (parsed.size() == 2) result.tls_port = parsed[1].endpoint.port;
    return result;
  }
  for (const TaggedDataEndpoint& endpoint : parsed) {
    std::uint16_t* destination = endpoint.kind == DataEndpointKind::kTcp
                                     ? &result.port
                                     : &result.tls_port;
    if (*destination != 0) {
      return Invalid(absl::StrCat("active data node ", node.node_id_,
                                  " repeats an endpoint transport"));
    }
    *destination = endpoint.endpoint.port;
  }
  return result;
}

template <std::size_t N>
std::string Hex(const std::array<std::uint8_t, N>& bytes) {
  constexpr std::string_view kHex = "0123456789abcdef";
  std::string result(bytes.size() * 2, '\0');
  for (std::size_t i = 0; i < bytes.size(); ++i) {
    result[i * 2] = kHex[bytes[i] >> 4];
    result[i * 2 + 1] = kHex[bytes[i] & 0x0f];
  }
  return result;
}

absl::StatusOr<control::WireFailoverTransition> ProjectFailoverTransition(
    const MetaFailoverTransition& source) {
  if (absl::Status status = ValidateMetaFailoverTransition(source);
      !status.ok()) {
    return Inconsistent(absl::StrCat("invalid committed failover transition: ",
                                     status.message()));
  }

  control::WireFailoverTransition projected;
  projected.transition_id = source.transition_id_;
  projected.revision = source.revision_;
  switch (source.mode_) {
    case MetaFailoverMode::kControlled:
      projected.mode = control::WireFailoverMode::kControlled;
      break;
    case MetaFailoverMode::kUncontrolled:
      projected.mode = control::WireFailoverMode::kUncontrolled;
      break;
  }
  projected.target_term = source.target_term_;
  projected.recovery_deadline_unix_ms = source.recovery_deadline_unix_ms_;
  if (source.candidate_action_.has_value()) {
    const MetaFailoverCandidateAction& action = *source.candidate_action_;
    control::WireFailoverCandidateAction projected_action;
    projected_action.action_id = action.action_id_;
    projected_action.candidate = {
        .node_id = action.candidate_.node_id_,
        .assignment_id = action.candidate_.assignment_id_,
        .boot_id = Hex(action.candidate_.boot_id_),
    };
    projected_action.operator_recovery = action.operator_recovery_;
    projected_action.domain = {
        .source_group_term = action.domain_.source_group_term_,
        .source_node_id = action.domain_.source_node_id_,
        .source_assignment_id = action.domain_.source_assignment_id_,
        .source_boot_id = Hex(action.domain_.source_boot_id_),
        .source_history_id = Hex(action.domain_.source_history_id_),
        .flow_count = action.domain_.flow_count_,
    };
    if (action.operator_recovery_) projected_action.domain = {};
    if (action.authorization_.has_value()) {
      control::WireFailoverLoss loss;
      switch (action.authorization_->loss_if_cutover_) {
        case MetaFailoverLoss::kNone:
          loss = control::WireFailoverLoss::kNone;
          break;
        case MetaFailoverLoss::kUnknown:
          loss = control::WireFailoverLoss::kUnknown;
          break;
      }
      projected_action.authorization = control::WireFailoverAuthorization{
          .authorized_revision = action.authorization_->authorized_revision_,
          .loss_if_cutover = loss,
      };
    }
    projected.candidate_action = std::move(projected_action);
  }
  return projected;
}

absl::StatusOr<control::WireDirectiveKind> ProjectDirectiveKind(
    const MetaDirectiveSpec& directive) {
  if ((directive.kind_ == kMetaDirectiveRebuild ||
       directive.kind_ == kMetaDirectiveAuthorizeSource) &&
      !control::DecodeRebuildRequest(directive.payload_).ok()) {
    return Invalid("rebuild/source authorization has an invalid typed body");
  }
  if (directive.kind_ == kMetaDirectiveRebuild) {
    return control::WireDirectiveKind::kRebuild;
  }
  if (directive.kind_ == kMetaDirectiveInitializeEmptyPopulation) {
    return control::WireDirectiveKind::kInitializeEmptyPopulation;
  }
  if (directive.kind_ == kMetaDirectiveAuthorizeSource) {
    return control::WireDirectiveKind::kAuthorizeSource;
  }
  if (directive.kind_ == kMetaDirectiveRevokeSources) {
    return control::WireDirectiveKind::kRevokeSources;
  }
  return Invalid(absl::StrCat("unknown directive kind ", directive.kind_));
}

using ManifestReference = std::pair<std::uint64_t, MetaHash256>;

absl::Status AddManifestReference(std::uint64_t revision,
                                  const MetaHash256& digest,
                                  std::set<ManifestReference>* references) {
  if ((revision == 0) != IsZero(digest)) {
    return Inconsistent("manifest revision and digest presence disagree");
  }
  if (revision != 0) references->emplace(revision, digest);
  return absl::OkStatus();
}

absl::StatusOr<control::WireProjectedDirective> ProjectDirective(
    const MetaOperationRecord& operation, const MetaCurrentDirective& current) {
  const MetaDirectiveSpec& source = current.spec_;
  const bool initializes_empty =
      source.kind_ == kMetaDirectiveInitializeEmptyPopulation;
  if (IsZero(operation.operation_id_) || IsZero(source.directive_id_) ||
      IsZero(source.attempt_id_) || IsZero(source.assignment_id_) ||
      IsZero(source.target_boot_id_) ||
      (!initializes_empty && (IsZero(source.source_assignment_id_) ||
                              IsZero(source.source_boot_id_) ||
                              IsZero(source.source_replication_history_id_))) ||
      current.directive_revision_ == 0) {
    return Inconsistent("current directive contains an empty identity");
  }
  if (!control::IsCanonicalIdentity160(source.recipient_node_id_) ||
      !control::IsCanonicalIdentity160(source.target_node_id_) ||
      !control::IsCanonicalIdentity160(source.source_node_id_)) {
    return Inconsistent("current directive contains a non-canonical node id");
  }
  auto kind = ProjectDirectiveKind(source);
  if (!kind.ok()) return kind.status();
  const bool executes_on_target =
      *kind == control::WireDirectiveKind::kRebuild ||
      *kind == control::WireDirectiveKind::kInitializeEmptyPopulation;
  const std::string& expected_recipient =
      executes_on_target ? source.target_node_id_ : source.source_node_id_;
  if (source.recipient_node_id_ != expected_recipient) {
    return Inconsistent(
        "current directive recipient does not match its execution kind");
  }

  control::WireProjectedDirective result;
  result.authority.group_id = source.group_id_;
  result.authority.assignment_id = source.assignment_id_;
  result.authority.group_term = source.group_term_;
  result.identity.operation_id = operation.operation_id_;
  result.identity.directive_id = source.directive_id_;
  result.identity.attempt_id = source.attempt_id_;
  result.identity.directive_revision = current.directive_revision_;
  result.recipient_node_id = source.recipient_node_id_;
  result.recipient_boot_id =
      Hex(executes_on_target ? source.target_boot_id_ : source.source_boot_id_);
  result.target_node_id = source.target_node_id_;
  result.target_boot_id = Hex(source.target_boot_id_);
  result.source_node_id = source.source_node_id_;
  result.source_assignment_id = source.source_assignment_id_;
  result.source_boot_id = Hex(source.source_boot_id_);
  result.source_replication_history_id =
      Hex(source.source_replication_history_id_);
  result.manifest_revision = source.population_manifest_revision_;
  result.manifest_digest = source.population_manifest_digest_;
  result.partition_replication_epoch = source.partition_replication_epoch_;
  result.kind = *kind;
  result.payload = source.payload_;

  return result;
}

bool SameClusterCreateRebuildScope(const MetaDirectiveSpec& authorize,
                                   const MetaDirectiveSpec& rebuild) {
  return authorize.target_node_id_ == rebuild.target_node_id_ &&
         authorize.target_boot_id_ == rebuild.target_boot_id_ &&
         authorize.assignment_id_ == rebuild.assignment_id_ &&
         authorize.source_node_id_ == rebuild.source_node_id_ &&
         authorize.source_assignment_id_ == rebuild.source_assignment_id_ &&
         authorize.source_boot_id_ == rebuild.source_boot_id_ &&
         authorize.source_replication_history_id_ ==
             rebuild.source_replication_history_id_ &&
         authorize.group_id_ == rebuild.group_id_ &&
         authorize.group_term_ == rebuild.group_term_ &&
         authorize.population_manifest_revision_ ==
             rebuild.population_manifest_revision_ &&
         authorize.population_manifest_digest_ ==
             rebuild.population_manifest_digest_ &&
         authorize.partition_replication_epoch_ ==
             rebuild.partition_replication_epoch_ &&
         authorize.payload_ == rebuild.payload_;
}

// Cluster creation retains the acknowledged source authorization when it
// commits target rebuilds in a later phase. Their directive revisions are
// therefore deliberately different; scope equality plus the authorization's
// own exact terminal-receipt key proves that the target may dial.
bool ClusterCreateDirectiveReady(const MetaOperationRecord& operation,
                                 const MetaCurrentDirective& current) {
  if (operation.kind_ != kMetaClusterCreateV1GroupOperationKind ||
      current.spec_.kind_ != kMetaDirectiveRebuild) {
    return true;
  }
  const auto authorize = std::find_if(
      operation.current_directives_.begin(),
      operation.current_directives_.end(),
      [&](const MetaCurrentDirective& candidate) {
        return candidate.spec_.kind_ == kMetaDirectiveAuthorizeSource &&
               SameClusterCreateRebuildScope(candidate.spec_, current.spec_);
      });
  if (authorize == operation.current_directives_.end()) return false;
  return std::any_of(
      operation.terminal_receipts_.begin(), operation.terminal_receipts_.end(),
      [&](const MetaTerminalReceipt& receipt) {
        return receipt.key_.operation_id_ == operation.operation_id_ &&
               receipt.key_.directive_id_ == authorize->spec_.directive_id_ &&
               receipt.key_.attempt_id_ == authorize->spec_.attempt_id_ &&
               receipt.key_.directive_revision_ ==
                   authorize->directive_revision_ &&
               receipt.status_ == MetaDirectiveResultStatus::kSucceeded;
      });
}

}  // namespace

namespace {

absl::StatusOr<control::FullDesiredState> ProjectNodeState(
    const MetaStores& stores, std::uint64_t applied_index,
    std::string_view node_id) {
  if (!control::IsCanonicalIdentity160(node_id)) {
    return Invalid("requested data node id is not canonical");
  }
  if (!stores.identity_.IsActiveNode(std::string(node_id))) {
    return absl::NotFoundError("requested data node is not active");
  }
  if (applied_index == 0) {
    return Inconsistent("committed view has applied index zero");
  }
  control::FullDesiredState state;
  state.control_revision = applied_index;
  state.topology_epoch = stores.topology_.TopologyEpoch();
  const std::optional<MetaAuthorityLeasePolicy> authority_lease =
      stores.policy_.CurrentAuthorityLease();
  if (!authority_lease.has_value()) {
    return Inconsistent("current Authority Lease Policy is missing");
  }
  if (authority_lease->duration_ms_ == 0 ||
      authority_lease->duration_ms_ >
          std::numeric_limits<std::uint32_t>::max()) {
    return Inconsistent(
        "current Authority Lease Policy duration is not wire-representable");
  }
  state.authority_lease_duration_ms =
      static_cast<std::uint32_t>(authority_lease->duration_ms_);

  for (const MetaMemberRecord& member : stores.identity_.MetaMembers()) {
    if (member.retired_) continue;
    auto endpoint = ParseNumericHostPort(member.data_control_endpoint_,
                                         "Meta data-control endpoint");
    if (!endpoint.ok()) return endpoint.status();
    state.meta_directory.push_back(
        control::WireMetaEndpoint{member.server_id_, std::move(endpoint->host),
                                  endpoint->port, member.principal_});
  }

  std::set<std::string> active_nodes;
  for (const MetaNodeRecord& node : stores.identity_.Nodes()) {
    if (node.retired_) continue;
    auto endpoint = ProjectDataEndpoint(node);
    if (!endpoint.ok()) return endpoint.status();
    if (!active_nodes.insert(node.node_id_).second) {
      return Inconsistent(
          absl::StrCat("duplicate active node ", node.node_id_));
    }
    state.nodes.push_back(std::move(*endpoint));
  }

  std::set<ManifestReference> manifest_references;
  std::map<std::string, std::size_t> group_indices;
  for (const MetaTopologyGroupView& source : stores.topology_.Groups()) {
    const auto grant = stores.topology_.AuthorityFor(source.group_id_);
    if (!grant.has_value()) {
      return Inconsistent(
          absl::StrCat("group ", source.group_id_, " has no grant state"));
    }
    if (source.record_.group_term_ != grant->group_term_) {
      return Inconsistent(absl::StrCat("group ", source.group_id_,
                                       " topology/grant anchors disagree"));
    }

    control::WireDesiredGroup projected;
    projected.group_id = source.group_id_;
    projected.group_term = source.record_.group_term_;
    projected.manifest_revision = source.record_.population_manifest_revision_;
    projected.manifest_digest = source.record_.population_manifest_digest_;
    projected.partition_replication_epoch =
        source.record_.partition_replication_epoch_;
    projected.steady_replication_enabled =
        stores.topology_.ClusterLifecycle().state_ ==
        MetaClusterLifecycle::kCreated;

    for (const MetaGroupMember& member : source.members_) {
      if (!active_nodes.contains(member.node_id_)) {
        return Inconsistent(absl::StrCat("group ", source.group_id_,
                                         " names an inactive member"));
      }
      // Membership has no redundant role byte. Protocol v1 projects owner
      // intent separately below and grant_active independently authorizes
      // serving.
      projected.members.push_back({member.node_id_, member.assignment_id_});
    }

    // Project durable owner intent even while fenced. Data needs this fact to
    // choose the owner/no-role branch instead of misreporting a Ready former
    // authority as a replica candidate. grant_active remains the independent
    // serving-authority bit.
    auto owner = source.members_.end();
    if (!source.record_.owner_.empty()) {
      owner = std::find_if(source.members_.begin(), source.members_.end(),
                           [&source](const MetaGroupMember& member) {
                             return member.node_id_ == source.record_.owner_;
                           });
      // Removing a fenced owner may leave its id as durable history. It no
      // longer classifies any live member and is intentionally omitted.
      if (owner != source.members_.end() && IsZero(owner->assignment_id_)) {
        return Inconsistent(absl::StrCat("group ", source.group_id_,
                                         " owner has no live assignment"));
      }
      if (owner != source.members_.end()) {
        projected.owner_node_id = source.record_.owner_;
        projected.owner_assignment_id = owner->assignment_id_;
      }
    }

    if (grant->grant_.has_value()) {
      const MetaActiveAuthorityView& active = *grant->grant_;
      if (source.record_.owner_.empty() ||
          active.owner_ != source.record_.owner_) {
        return Inconsistent(absl::StrCat("group ", source.group_id_,
                                         " active grant is inconsistent"));
      }
      if (source.record_.group_term_ == 0) {
        return Inconsistent(
            absl::StrCat("group ", source.group_id_,
                         " active grant has incomplete serving authority"));
      }
      if (owner == source.members_.end()) {
        return Inconsistent(absl::StrCat("group ", source.group_id_,
                                         " active grant has no owner"));
      }
      projected.grant_active = true;
      projected.activation_action_id = active.activation_action_id_;
    }

    if (source.failover_transition_.has_value()) {
      auto transition = ProjectFailoverTransition(*source.failover_transition_);
      if (!transition.ok()) return transition.status();
      projected.failover_transition = std::move(*transition);
    }

    if (absl::Status status = AddManifestReference(projected.manifest_revision,
                                                   projected.manifest_digest,
                                                   &manifest_references);
        !status.ok()) {
      return status;
    }
    if (!group_indices.emplace(projected.group_id, state.groups.size())
             .second) {
      return Inconsistent(absl::StrCat("duplicate group ", projected.group_id));
    }
    state.groups.push_back(std::move(projected));
  }

  // Reconstruct canonical ranges from the authoritative slot table. The
  // committed SetSlotMap command shape is not retained, so adjacent runs are
  // intentionally coalesced here.
  std::optional<std::string> previous_group;
  for (std::uint32_t slot = 0; slot < kMetaSlotCount; ++slot) {
    const std::optional<std::string> owner = stores.topology_.SlotOwner(slot);
    if (!owner.has_value()) {
      previous_group.reset();
      continue;
    }
    const auto group = group_indices.find(*owner);
    if (group == group_indices.end()) {
      return Inconsistent(
          absl::StrCat("slot ", slot, " names unknown group ", *owner));
    }
    std::vector<control::WireSlotRange>& ranges =
        state.groups[group->second].slot_ranges;
    if (previous_group.has_value() && *previous_group == *owner) {
      ranges.back().last = static_cast<std::uint16_t>(slot);
    } else {
      ranges.push_back(
          {static_cast<std::uint16_t>(slot), static_cast<std::uint16_t>(slot)});
    }
    previous_group = *owner;
  }

  for (const MetaOperationRecord& operation :
       stores.operation_.LiveOperationsView()) {
    for (const MetaCurrentDirective& current : operation.current_directives_) {
      if (current.spec_.recipient_node_id_ != node_id) continue;
      if (!ClusterCreateDirectiveReady(operation, current)) continue;
      if (const absl::Status anchor =
              ValidateCommittedDirectiveAnchor(stores, current.spec_);
          !anchor.ok()) {
        return Inconsistent(
            absl::StrCat("stale current directive anchor: ", anchor.message()));
      }
      auto directive = ProjectDirective(operation, current);
      if (!directive.ok()) return directive.status();
      if (absl::Status status = AddManifestReference(
              directive->manifest_revision, directive->manifest_digest,
              &manifest_references);
          !status.ok()) {
        return status;
      }
      state.current_directives.push_back(std::move(*directive));
    }
  }
  std::sort(
      state.current_directives.begin(), state.current_directives.end(),
      [](const control::WireProjectedDirective& left,
         const control::WireProjectedDirective& right) {
        return std::tie(left.identity.operation_id, left.identity.directive_id,
                        left.identity.attempt_id,
                        left.identity.directive_revision) <
               std::tie(right.identity.operation_id,
                        right.identity.directive_id, right.identity.attempt_id,
                        right.identity.directive_revision);
      });

  for (const auto& [revision, digest] : manifest_references) {
    const auto document = stores.population_manifest_.Find(digest);
    if (!document.has_value()) {
      return Inconsistent(absl::StrCat("referenced population manifest at ",
                                       "revision ", revision, " is missing"));
    }
    // Put and snapshot recovery validate immutable manifest content. A
    // projection only resolves its content-addressed reference, never rehashes
    // it.
    control::WireManifestDocument projected;
    projected.revision = revision;
    projected.digest = digest;
    for (const MetaPopulationManifestEntry& entry : document->entries_) {
      if (entry.partition_id_ >= kMetaSlotCount) {
        return Inconsistent("population manifest partition is out of range");
      }
      projected.entries.push_back(
          {static_cast<std::uint16_t>(entry.partition_id_),
           entry.logical_epoch_});
    }
    state.manifests.push_back(std::move(projected));
  }

  return state;
}

}  // namespace

absl::StatusOr<NodeControlBatch> MetaControlProjector::ProjectNode(
    const MetaCommittedView& view, std::string_view node_id) {
  auto state = ProjectNodeState(view.stores(), view.applied_index(), node_id);
  if (!state.ok()) return state.status();
  auto encoded = control::EncodeFullDesiredState(*state);
  if (!encoded.ok()) return encoded.status();
  return NodeControlBatch{std::move(*state), std::move(*encoded)};
}

std::size_t NodeControlBatchRetainedBytes(
    const NodeControlBatch& batch) noexcept {
  std::size_t total = sizeof(NodeControlBatch);
  const auto add = [&](std::size_t bytes) {
    if (bytes > std::numeric_limits<std::size_t>::max() - total) {
      total = std::numeric_limits<std::size_t>::max();
    } else {
      total += bytes;
    }
  };
  const auto add_array = [&](std::size_t count, std::size_t width) {
    if (width != 0 && count > std::numeric_limits<std::size_t>::max() / width) {
      total = std::numeric_limits<std::size_t>::max();
    } else {
      add(count * width);
    }
  };
  const auto add_string = [&](const std::string& value) {
    // Counting SSO capacity again is deliberately conservative and also
    // covers the implementation's trailing NUL without allocator knowledge.
    add(value.capacity());
    add(1);
  };

  add_string(batch.encoded_full_state);
  const control::FullDesiredState& state = batch.full_state;

  add_array(state.meta_directory.capacity(), sizeof(control::WireMetaEndpoint));
  for (const control::WireMetaEndpoint& endpoint : state.meta_directory) {
    add_string(endpoint.host);
    if (endpoint.principal.has_value()) add_string(*endpoint.principal);
  }

  add_array(state.nodes.capacity(), sizeof(control::WireDataEndpoint));
  for (const control::WireDataEndpoint& node : state.nodes) {
    add_string(node.node_id);
    add_string(node.host);
  }

  add_array(state.groups.capacity(), sizeof(control::WireDesiredGroup));
  for (const control::WireDesiredGroup& group : state.groups) {
    add_string(group.group_id);
    add_array(group.members.capacity(), sizeof(control::WireDesiredMember));
    for (const control::WireDesiredMember& member : group.members) {
      add_string(member.node_id);
    }
    if (group.owner_node_id.has_value()) add_string(*group.owner_node_id);
    add_array(group.slot_ranges.capacity(), sizeof(control::WireSlotRange));
    if (group.failover_transition.has_value()) {
      const control::WireFailoverTransition& transition =
          *group.failover_transition;
      if (transition.candidate_action.has_value()) {
        const control::WireFailoverCandidateAction& action =
            *transition.candidate_action;
        add_string(action.candidate.node_id);
        add_string(action.candidate.boot_id);
        add_string(action.domain.source_node_id);
        add_string(action.domain.source_boot_id);
        add_string(action.domain.source_history_id);
      }
    }
  }

  add_array(state.manifests.capacity(), sizeof(control::WireManifestDocument));
  for (const control::WireManifestDocument& manifest : state.manifests) {
    add_array(manifest.entries.capacity(), sizeof(control::WireManifestEntry));
  }

  add_array(state.current_directives.capacity(),
            sizeof(control::WireProjectedDirective));
  for (const control::WireProjectedDirective& directive :
       state.current_directives) {
    add_string(directive.authority.group_id);
    add_string(directive.recipient_node_id);
    add_string(directive.recipient_boot_id);
    add_string(directive.target_node_id);
    add_string(directive.target_boot_id);
    add_string(directive.source_node_id);
    add_string(directive.source_boot_id);
    add_string(directive.source_replication_history_id);
    add_string(directive.payload);
  }
  return total;
}

}  // namespace lavik::meta
