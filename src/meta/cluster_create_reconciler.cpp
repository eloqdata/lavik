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

#include "lavik/meta/cluster_create_reconciler.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <string>
#include <string_view>
#include <vector>

#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "bycorf/io/storage.h"
#include "bycorf/runtime/worker.h"
#include "lavik/cluster/control_protocol.h"
#include "lavik/fault_injection.h"
#include "lavik/meta/cluster_create.h"
#include "lavik/meta/hash.h"
#include "lavik/meta/population_manifest_store.h"
#include "lavik/meta/raft.h"
#include "spdlog/spdlog.h"

namespace lavik::meta {
namespace {
constexpr std::string_view kRootPhaseWaitMetaBarrier = "wait-meta-barrier";
constexpr std::string_view kRootPhaseRegisterData = "register-data";
constexpr std::string_view kRootPhaseCreateGroups = "create-groups";
constexpr std::string_view kRootPhaseSlotMap = "slot-map";
constexpr std::string_view kRootPhasePolicy = "policy";
constexpr std::string_view kRootPhasePopulation = "population";
constexpr std::string_view kRootPhaseWaitDataProjection =
    "wait-data-projection";
constexpr std::string_view kRootPhaseInitializeGroups = "initialize-groups";
constexpr std::string_view kGroupPhaseInitialize =
    "initializing-empty-population";
constexpr std::string_view kGroupPhaseAuthorize = "authorizing-replica-sources";
constexpr std::string_view kGroupPhaseReplicate =
    "replicating-empty-population";
constexpr std::string_view kGroupPhaseReady = "population-ready";
constexpr std::string_view kGroupFailurePrefix = "deterministic-failure:";

bool IsTerminal(MetaOperationLifecycle state) {
  return state == MetaOperationLifecycle::kCompleted ||
         state == MetaOperationLifecycle::kAborted;
}

template <std::size_t N>
std::string Hex(const std::array<std::uint8_t, N>& value) {
  return absl::BytesToHexString(std::string_view(
      reinterpret_cast<const char*>(value.data()), value.size()));
}

// The randomly generated root id supplies the entropy. Domain separation
// gives recovery the SAME child, assignment and directive identities without
// another durable format or a random in-memory value lost between commits.
MetaOperationId DerivedV1Id(const MetaOperationId& root,
                            std::string_view purpose) {
  const auto hash =
      MetaSha256(absl::StrCat("cluster-create-v1/", Hex(root), "/", purpose));
  MetaOperationId id;
  std::copy_n(hash.begin(), id.size(), id.begin());
  return id;
}

using Plan = absl::StatusOr<std::optional<MetaCommand>>;
template <typename Command>
Plan Emit(Command command) {
  return std::optional<MetaCommand>(std::move(command));
}
Plan Conflict(std::string_view reason) {
  return absl::FailedPreconditionError(std::string(reason));
}
Plan Advance(const MetaOperationRecord& operation, std::string_view phase) {
  TransitionOperationPhase command;
  command.operation_id_ = operation.operation_id_;
  command.expected_revision_ = operation.revision_;
  command.kind_phase_blob_ = phase;
  return Emit(std::move(command));
}
Plan Complete(const MetaOperationRecord& operation) {
  CompleteOperation command;
  command.operation_id_ = operation.operation_id_;
  command.expected_revision_ = operation.revision_;
  command.result_ = "cluster-created";
  return Emit(std::move(command));
}
Plan Abort(const MetaOperationRecord& operation, std::string reason) {
  AbortOperation command;
  command.operation_id_ = operation.operation_id_;
  command.expected_revision_ = operation.revision_;
  command.reason_ = std::move(reason);
  return Emit(std::move(command));
}

std::string AutomaticFailoverPolicyContent(
    const ClusterCreateManifestV1& manifest) {
  return absl::StrCat(
      R"({"kind":"automatic-uncontrolled-failover-v1","suspect_after_ms":)",
      manifest.automatic_uncontrolled_failover_suspect_after_ms_, "}");
}

std::string AuthorityLeasePolicyContent(
    const ClusterCreateManifestV1& manifest) {
  return absl::StrCat(R"({"kind":"authority-lease-v1","duration_ms":)",
                      manifest.authority_lease_duration_ms_, "}");
}

// ClusterCreateManifestV1 is normalized before persistence; comparisons below
// adapt its validated tcp:// endpoints to the scheme-free runtime models.
std::string StripValidatedTcpEndpointScheme(std::string_view endpoint) {
  constexpr std::string_view kTcpPrefix = "tcp://";
  return std::string(endpoint.substr(kTcpPrefix.size()));
}

absl::Status ValidateMetaSet(const MetaCommittedView& view,
                             const ClusterCreateManifestV1& manifest,
                             const MetaClusterCreateRaftView& raft) {
  if (raft.local_server_id_ == 0 ||
      raft.members_.size() != manifest.meta_members_.size()) {
    return absl::FailedPreconditionError(
        "creation Meta config differs from intent");
  }
  const auto bindings = view.identity().MetaMembers();
  if (bindings.size() != manifest.meta_members_.size()) {
    return absl::FailedPreconditionError(
        "creation Meta identity directory differs from intent");
  }
  bool local_found = false;
  for (std::size_t index = 0; index < manifest.meta_members_.size(); ++index) {
    const auto& expected = manifest.meta_members_[index];
    const auto& peer = raft.members_[index];
    if (peer.id_ != expected.server_id_ ||
        peer.endpoint_ !=
            StripValidatedTcpEndpointScheme(expected.raft_endpoint_) ||
        peer.principal_ != absl::StrCat("lavik://meta/", expected.server_id_) ||
        peer.data_control_endpoint_ !=
            StripValidatedTcpEndpointScheme(expected.data_control_endpoint_) ||
        peer.ctl_endpoint_ !=
            StripValidatedTcpEndpointScheme(expected.ctl_endpoint_) ||
        peer.dc_id_ != 0 || peer.priority_ != 1 || peer.learner_ ||
        peer.new_joiner_) {
      return absl::FailedPreconditionError(
          "creation Meta config descriptor differs from intent");
    }
    const auto binding = view.identity().FindMetaMember(expected.server_id_);
    if (!binding.has_value() || binding->retired_ ||
        binding->principal_ != peer.principal_ ||
        binding->data_control_endpoint_ != peer.data_control_endpoint_ ||
        binding->ctl_endpoint_ != std::optional(peer.ctl_endpoint_)) {
      return absl::FailedPreconditionError(
          "creation Meta identity binding differs from config descriptor");
    }
    local_found |= expected.server_id_ == raft.local_server_id_;
  }
  if (!local_found) {
    return absl::FailedPreconditionError(
        "local Meta member is absent from creation intent");
  }
  return absl::OkStatus();
}

bool MetaBarrierSatisfied(const MetaOperationRecord& operation,
                          const MetaClusterCreateRaftView& raft) {
  if (operation.operation_seq_ == 0 || raft.max_response_age_us_ == 0) {
    return false;
  }
  for (const auto& member : raft.members_) {
    if (member.id_ == raft.local_server_id_) continue;
    const auto progress = std::find_if(
        raft.peer_progress_.begin(), raft.peer_progress_.end(),
        [&](const auto& item) { return item.server_id_ == member.id_; });
    if (progress == raft.peer_progress_.end() ||
        progress->last_response_age_us_ > raft.max_response_age_us_ ||
        progress->last_sm_committed_index_ < operation.operation_seq_) {
      return false;
    }
  }
  return true;
}

bool ProjectionMatches(const MetaDataControlRuntimeNode& runtime,
                       const MetaTopologyGroupView& group,
                       const MetaGroupAuthorityView& grant,
                       std::uint64_t required_applied_index) {
  // Runtime is published only after Data acknowledges the installed FDS.
  // Unrelated commits advance validated high-water without resending an
  // unchanged projection. Requiring control_revision to catch up
  // would therefore wait forever for an already-current projection.
  if (runtime.validated_committed_high_water_ < required_applied_index ||
      runtime.groups_.size() != 1 || !grant.grant_.has_value())
    return false;
  const auto member = std::find_if(
      group.members_.begin(), group.members_.end(),
      [&](const auto& item) { return item.node_id_ == runtime.node_id_; });
  if (member == group.members_.end()) return false;
  const auto& projected = runtime.groups_.front();
  return projected.group_id_ == group.group_id_ &&
         projected.assignment_id_ == member->assignment_id_ &&
         projected.group_term_ == group.record_.group_term_ &&
         projected.manifest_revision_ ==
             group.record_.population_manifest_revision_ &&
         projected.manifest_digest_ ==
             group.record_.population_manifest_digest_ &&
         projected.partition_replication_epoch_ ==
             group.record_.partition_replication_epoch_;
}

const ClusterCreateManifestV1::DataNode* FindData(
    const ClusterCreateManifestV1& manifest, std::string_view node_id) {
  const auto found =
      std::find_if(manifest.data_nodes_.begin(), manifest.data_nodes_.end(),
                   [&](const auto& node) { return node.node_id_ == node_id; });
  return found == manifest.data_nodes_.end() ? nullptr : &*found;
}

const ClusterCreateManifestV1::Group* FindDeclaration(
    const ClusterCreateManifestV1& manifest, std::string_view group_id) {
  const auto found = std::find_if(
      manifest.groups_.begin(), manifest.groups_.end(),
      [&](const auto& group) { return group.group_id_ == group_id; });
  return found == manifest.groups_.end() ? nullptr : &*found;
}

MetaNodeRole DeclaredRole(const ClusterCreateManifestV1& manifest,
                          std::string_view node_id) {
  for (const auto& group : manifest.groups_) {
    if (group.primary_node_id_ == node_id) return MetaNodeRole::kPrimary;
  }
  return MetaNodeRole::kReplica;
}

MetaAssignmentId V1AssignmentId(const MetaOperationId& root,
                                std::string_view group_id,
                                std::string_view node_id) {
  return DerivedV1Id(
      root, absl::StrCat("assignment/", absl::BytesToHexString(group_id), "/",
                         node_id));
}

std::vector<std::pair<std::string, MetaNodeRole>> DeclaredMembers(
    const ClusterCreateManifestV1::Group& group) {
  std::vector<std::pair<std::string, MetaNodeRole>> members;
  members.reserve(group.replica_node_ids_.size() + 1);
  members.emplace_back(group.primary_node_id_, MetaNodeRole::kPrimary);
  for (const std::string& replica : group.replica_node_ids_)
    members.emplace_back(replica, MetaNodeRole::kReplica);
  return members;
}

PutPopulationManifest V1PopulationManifest(
    const ClusterCreateManifestV1& manifest, std::string_view group_id) {
  PutPopulationManifest population;
  for (const auto& range : manifest.slot_ranges_) {
    if (range.group_id_ != group_id) continue;
    for (std::uint32_t slot = range.first_; slot <= range.last_; ++slot)
      population.entries_.push_back({slot, 1});
  }
  population.manifest_digest_ =
      MetaPopulationManifestStore::CanonicalDigest(population.entries_);
  return population;
}

bool V1SlotMapMatches(const MetaStores& stores,
                      const ClusterCreateManifestV1& manifest, bool* empty) {
  *empty = true;
  bool matches = true;
  std::size_t range_index = 0;
  for (std::uint32_t slot = 0; slot < kMetaSlotCount; ++slot) {
    while (range_index + 1 < manifest.slot_ranges_.size() &&
           slot > manifest.slot_ranges_[range_index].last_)
      ++range_index;
    const auto owner = stores.topology_.SlotOwner(slot);
    *empty &= !owner.has_value();
    matches &= owner == std::optional<std::string>(
                            manifest.slot_ranges_[range_index].group_id_);
  }
  return matches;
}

std::vector<std::string> V1DataEndpoints(
    const ClusterCreateManifestV1::DataNode& node) {
  std::vector<std::string> endpoints;
  if (!node.client_endpoint_.empty())
    endpoints.push_back(node.client_endpoint_);
  if (!node.tls_endpoint_.empty()) endpoints.push_back(node.tls_endpoint_);
  return endpoints;
}

absl::Status ValidateV1Nodes(const MetaStores& stores,
                             const ClusterCreateManifestV1& manifest,
                             bool require_all) {
  for (const MetaNodeRecord& node : stores.identity_.Nodes()) {
    const auto* declaration = FindData(manifest, node.node_id_);
    if (declaration == nullptr || node.retired_ ||
        node.principal_ != absl::StrCat("lavik://node/", node.node_id_) ||
        node.role_ != DeclaredRole(manifest, node.node_id_) ||
        node.endpoints_ != V1DataEndpoints(*declaration)) {
      return absl::FailedPreconditionError(absl::StrCat(
          "creation node differs from intent: node=", node.node_id_));
    }
  }
  if (stores.identity_.NodeCount() > manifest.data_nodes_.size() ||
      (require_all &&
       stores.identity_.NodeCount() != manifest.data_nodes_.size())) {
    return absl::FailedPreconditionError(
        "creation Data membership differs from intent");
  }
  return absl::OkStatus();
}

absl::Status ValidateV1GroupMembers(
    const MetaTopologyGroupView& group,
    const ClusterCreateManifestV1::Group& declaration,
    const MetaOperationId& root, bool require_all) {
  const auto expected = DeclaredMembers(declaration);
  for (const MetaGroupMember& member : group.members_) {
    const auto found = std::find_if(
        expected.begin(), expected.end(),
        [&](const auto& item) { return item.first == member.node_id_; });
    if (found == expected.end() || member.role_ != found->second ||
        member.assignment_id_ !=
            V1AssignmentId(root, declaration.group_id_, member.node_id_)) {
      return absl::FailedPreconditionError(
          absl::StrCat("creation assignment differs from intent: group=",
                       declaration.group_id_, " node=", member.node_id_));
    }
  }
  if (group.members_.size() > expected.size() ||
      (require_all && group.members_.size() != expected.size())) {
    return absl::FailedPreconditionError(
        absl::StrCat("creation Group membership is incomplete: group=",
                     declaration.group_id_));
  }
  return absl::OkStatus();
}

absl::Status ValidateV1GroupsKnown(const MetaStores& stores,
                                   const ClusterCreateManifestV1& manifest,
                                   const MetaOperationId& root,
                                   bool require_all_members) {
  for (const MetaTopologyGroupView& group : stores.topology_.Groups()) {
    const auto* declaration = FindDeclaration(manifest, group.group_id_);
    if (declaration == nullptr)
      return absl::FailedPreconditionError(absl::StrCat(
          "creation has an unknown Group: group=", group.group_id_));
    if (auto status = ValidateV1GroupMembers(group, *declaration, root,
                                             require_all_members);
        !status.ok())
      return status;
  }
  if (stores.topology_.GroupCount() > manifest.groups_.size() ||
      (require_all_members &&
       stores.topology_.GroupCount() != manifest.groups_.size())) {
    return absl::FailedPreconditionError(
        "creation Group set differs from intent");
  }
  return absl::OkStatus();
}

absl::Status ValidateV1FinalTopology(const MetaStores& stores,
                                     const ClusterCreateManifestV1& manifest,
                                     const MetaOperationId& root,
                                     bool allow_failed_group) {
  if (auto status = ValidateV1Nodes(stores, manifest, true); !status.ok())
    return status;
  if (auto status = ValidateV1GroupsKnown(stores, manifest, root, true);
      !status.ok())
    return status;
  bool slots_empty = false;
  if (!V1SlotMapMatches(stores, manifest, &slots_empty) || slots_empty)
    return absl::FailedPreconditionError(
        "creation Slot map differs from intent");
  if (!stores.policy_.CurrentAutomaticUncontrolledFailover().has_value() ||
      !stores.policy_.CurrentAuthorityLease().has_value() ||
      !stores.policy_.CurrentCandidateRecovery().has_value()) {
    return absl::FailedPreconditionError(
        "creation requires all registered global policies");
  }
  for (const auto& declaration : manifest.groups_) {
    const auto group = stores.topology_.FindGroup(declaration.group_id_);
    const auto grant = stores.topology_.AuthorityFor(declaration.group_id_);
    const auto population =
        V1PopulationManifest(manifest, declaration.group_id_);
    if (!group.has_value() ||
        (group->record_.group_term_ != 1 &&
         !(allow_failed_group && group->record_.group_term_ == 2)) ||
        group->record_.population_manifest_revision_ != 1 ||
        group->record_.population_manifest_digest_ !=
            population.manifest_digest_ ||
        group->record_.partition_replication_epoch_ != 1 ||
        !stores.population_manifest_.Contains(population.manifest_digest_) ||
        !grant.has_value()) {
      return absl::FailedPreconditionError(
          absl::StrCat("creation Group anchors differ from intent: group=",
                       declaration.group_id_));
    }
    const bool active = grant->grant_.has_value() &&
                        grant->grant_->owner_ == declaration.primary_node_id_ &&
                        grant->group_term_ == 1 &&
                        group->record_.group_term_ == 1;
    const bool failed = allow_failed_group && !grant->grant_.has_value() &&
                        group->record_.group_term_ == 2;
    if (!active && !failed)
      return absl::FailedPreconditionError(
          absl::StrCat("creation authority differs from intent: group=",
                       declaration.group_id_));
  }
  return absl::OkStatus();
}

std::optional<MetaTerminalReceipt> ReceiptFor(
    const MetaOperationRecord& operation, const MetaOperationId& directive_id,
    const MetaOperationId& attempt_id,
    std::optional<std::uint64_t> directive_revision = std::nullopt) {
  const auto found = std::find_if(
      operation.terminal_receipts_.begin(), operation.terminal_receipts_.end(),
      [&](const auto& receipt) {
        return receipt.key_.directive_id_ == directive_id &&
               receipt.key_.attempt_id_ == attempt_id &&
               (!directive_revision.has_value() ||
                receipt.key_.directive_revision_ == *directive_revision);
      });
  return found == operation.terminal_receipts_.end()
             ? std::nullopt
             : std::optional<MetaTerminalReceipt>(*found);
}

absl::StatusOr<MetaBootIncarnation> ParseBoot(std::string_view value) {
  MetaBootIncarnation result{};
  std::string bytes;
  if (!cluster::control::IsCanonicalIdentity160(value) ||
      !absl::HexStringToBytes(value, &bytes) || bytes.size() != result.size())
    return absl::InvalidArgumentError("Data boot identity is malformed");
  std::copy(bytes.begin(), bytes.end(), result.begin());
  return result;
}

Plan PlanV1GroupStep(const MetaCommittedView& view,
                     const MetaOperationRecord& root,
                     const MetaOperationRecord& operation,
                     const ClusterCreateManifestV1& manifest,
                     const ClusterCreateManifestV1::Group& declaration,
                     const MetaDataControlRuntimeSnapshot& runtime) {
  const auto& stores = view.stores();
  const auto group = stores.topology_.FindGroup(declaration.group_id_);
  const auto grant = stores.topology_.AuthorityFor(declaration.group_id_);
  const auto population = V1PopulationManifest(manifest, declaration.group_id_);
  if (!group.has_value() || !grant.has_value())
    return Conflict(absl::StrCat("group=", declaration.group_id_,
                                 " committed anchors disappeared"));
  const auto primary_member = std::find_if(
      group->members_.begin(), group->members_.end(), [&](const auto& member) {
        return member.node_id_ == declaration.primary_node_id_;
      });
  if (primary_member == group->members_.end())
    return Conflict(absl::StrCat("group=", declaration.group_id_,
                                 " primary assignment disappeared"));
  const std::string intent_prefix =
      absl::StrCat("cluster-create-v1-group ", Hex(root.operation_id_), " ",
                   absl::BytesToHexString(declaration.group_id_), " ");
  if (operation.kind_ != kMetaClusterCreateV1GroupOperationKind ||
      !operation.intent_.starts_with(intent_prefix))
    return Conflict(absl::StrCat("group=", declaration.group_id_,
                                 " operation is not owned by creation"));
  const std::string_view source_boot_text =
      std::string_view(operation.intent_).substr(intent_prefix.size());
  auto source_boot = ParseBoot(source_boot_text);
  if (!source_boot.ok()) return Conflict(source_boot.status().message());

  const auto primary_directive =
      DerivedV1Id(operation.operation_id_, "primary/directive");
  const auto primary_attempt =
      DerivedV1Id(operation.operation_id_, "primary/attempt");
  const auto primary_receipt =
      ReceiptFor(operation, primary_directive, primary_attempt);
  const auto primary_runtime = std::find_if(
      runtime.nodes_.begin(), runtime.nodes_.end(), [&](const auto& node) {
        return node.node_id_ == declaration.primary_node_id_;
      });
  const bool source_incarnation_changed =
      runtime.leader_authority_eligible_ &&
      primary_runtime != runtime.nodes_.end() &&
      (primary_runtime->boot_id_ != source_boot_text ||
       primary_runtime->replication_history_id_ !=
           operation.replication_history_id_);
  auto retain_incarnation_failure = [&](std::string_view node_id,
                                        std::string_view reason) -> Plan {
    // Removing the old directives and retaining the reason is one committed
    // transition. A late result cannot complete that attempt afterwards, and
    // a new Meta leader can still fence/abort after runtime evidence is lost.
    return Advance(operation, absl::StrCat(kGroupFailurePrefix,
                                           "group=", declaration.group_id_,
                                           " node=", node_id, " ", reason));
  };
  auto fence_or_abort = [&](std::string reason) -> Plan {
    if (grant->grant_.has_value()) {
      FenceGroup fence;
      fence.group_id_ = declaration.group_id_;
      fence.expected_term_ = 1;
      fence.new_term_ = 2;
      return Emit(std::move(fence));
    }
    return Abort(operation, std::move(reason));
  };
  if (operation.kind_phase_blob_.starts_with(kGroupFailurePrefix)) {
    return fence_or_abort(
        operation.kind_phase_blob_.substr(kGroupFailurePrefix.size()));
  }

  if (operation.lifecycle_ == MetaOperationLifecycle::kSubmitted) {
    if (!operation.current_directives_.empty() || primary_receipt.has_value())
      return Conflict("submitted Group operation contains progress");
    if (!grant->grant_.has_value())
      return Conflict(absl::StrCat("group=", declaration.group_id_,
                                   " authority is unavailable"));
    if (source_incarnation_changed)
      return retain_incarnation_failure(declaration.primary_node_id_,
                                        "primary boot/history changed before "
                                        "empty-population initialization");
    MetaDirectiveSpec initialize;
    initialize.directive_id_ = primary_directive;
    initialize.attempt_id_ = primary_attempt;
    initialize.recipient_node_id_ = declaration.primary_node_id_;
    initialize.target_node_id_ = declaration.primary_node_id_;
    initialize.target_boot_id_ = *source_boot;
    initialize.assignment_id_ = primary_member->assignment_id_;
    initialize.source_node_id_ = std::string(kMetaNodeIdBytes, '0');
    initialize.group_id_ = declaration.group_id_;
    initialize.group_term_ = 1;
    initialize.population_manifest_revision_ = 1;
    initialize.population_manifest_digest_ = population.manifest_digest_;
    initialize.partition_replication_epoch_ = 1;
    initialize.kind_ = kMetaDirectiveInitializeEmptyPopulation;
    initialize.payload_ = Hex(operation.replication_history_id_);

    TransitionOperationPhase transition;
    transition.operation_id_ = operation.operation_id_;
    transition.expected_revision_ = operation.revision_;
    transition.kind_phase_blob_ = kGroupPhaseInitialize;
    transition.current_directives_.push_back(std::move(initialize));
    return Emit(std::move(transition));
  }

  if (operation.kind_phase_blob_ == kGroupPhaseInitialize) {
    if (!primary_receipt.has_value()) {
      if (!grant->grant_.has_value())
        return Conflict(
            absl::StrCat("group=", declaration.group_id_,
                         " authority changed during initialization"));
      if (operation.current_directives_.size() != 1 ||
          operation.current_directives_.front().spec_.directive_id_ !=
              primary_directive ||
          operation.current_directives_.front().spec_.attempt_id_ !=
              primary_attempt)
        return Conflict(absl::StrCat("group=", declaration.group_id_,
                                     " primary directive was invalidated"));
      if (source_incarnation_changed)
        return retain_incarnation_failure(declaration.primary_node_id_,
                                          "primary boot/history changed during "
                                          "empty-population initialization");
      return std::nullopt;
    }
    if (primary_receipt->status_ != MetaDirectiveResultStatus::kSucceeded)
      return fence_or_abort(
          absl::StrCat("group=", declaration.group_id_,
                       " node=", primary_receipt->recipient_node_id_,
                       " primary initialization: ", primary_receipt->result_));
    if (declaration.replica_node_ids_.empty())
      return Advance(operation, kGroupPhaseReady);
    if (!grant->grant_.has_value())
      return Conflict(absl::StrCat("group=", declaration.group_id_,
                                   " authority changed before replication"));
    if (source_incarnation_changed)
      return retain_incarnation_failure(
          declaration.primary_node_id_,
          "source boot/history changed before replica initialization");

    // Bind the layout from this exact live source incarnation into durable
    // intent. Reconnect/replay must not substitute the target's worker count
    // or re-read a newer source layout while projecting the same directive.
    if (!runtime.leader_authority_eligible_ ||
        primary_runtime == runtime.nodes_.end() ||
        !ProjectionMatches(*primary_runtime, *group, *grant,
                           view.applied_index()))
      return std::nullopt;
    auto rebuild_request = cluster::control::EncodeRebuildRequest(
        {.source_flow_count = primary_runtime->replication_flow_count_});
    if (!rebuild_request.ok())
      return Conflict(rebuild_request.status().message());

    TransitionOperationPhase transition;
    transition.operation_id_ = operation.operation_id_;
    transition.expected_revision_ = operation.revision_;
    transition.kind_phase_blob_ = kGroupPhaseAuthorize;
    transition.current_directives_.reserve(
        declaration.replica_node_ids_.size());
    for (const std::string& replica : declaration.replica_node_ids_) {
      const auto target = std::find_if(
          runtime.nodes_.begin(), runtime.nodes_.end(),
          [&](const auto& node) { return node.node_id_ == replica; });
      const auto target_member = std::find_if(
          group->members_.begin(), group->members_.end(),
          [&](const auto& member) { return member.node_id_ == replica; });
      if (target == runtime.nodes_.end() ||
          target_member == group->members_.end() ||
          !ProjectionMatches(*target, *group, *grant, view.applied_index()))
        return std::nullopt;
      auto target_boot = ParseBoot(target->boot_id_);
      if (!target_boot.ok())
        return Conflict(absl::StrCat("group=", declaration.group_id_,
                                     " node=", replica, " ",
                                     target_boot.status().message()));
      const std::string purpose = absl::StrCat("replica/", replica, "/");
      MetaDirectiveSpec authorize;
      authorize.directive_id_ =
          DerivedV1Id(operation.operation_id_, purpose + "authorize");
      authorize.attempt_id_ =
          DerivedV1Id(operation.operation_id_, purpose + "authorize-attempt");
      authorize.recipient_node_id_ = declaration.primary_node_id_;
      authorize.target_node_id_ = replica;
      authorize.target_boot_id_ = *target_boot;
      authorize.assignment_id_ = target_member->assignment_id_;
      authorize.source_node_id_ = declaration.primary_node_id_;
      authorize.source_assignment_id_ = primary_member->assignment_id_;
      authorize.source_boot_id_ = *source_boot;
      authorize.source_replication_history_id_ =
          operation.replication_history_id_;
      authorize.group_id_ = declaration.group_id_;
      authorize.group_term_ = 1;
      authorize.population_manifest_revision_ = 1;
      authorize.population_manifest_digest_ = population.manifest_digest_;
      authorize.partition_replication_epoch_ = 1;
      authorize.kind_ = kMetaDirectiveAuthorizeSource;
      authorize.payload_ = *rebuild_request;
      transition.current_directives_.push_back(std::move(authorize));
    }
    return Emit(std::move(transition));
  }

  if (operation.kind_phase_blob_ == kGroupPhaseAuthorize) {
    const std::size_t expected_directives =
        declaration.replica_node_ids_.size();
    if (!grant->grant_.has_value()) {
      for (const std::string& replica : declaration.replica_node_ids_) {
        const std::string purpose = absl::StrCat("replica/", replica, "/");
        const auto receipt = ReceiptFor(
            operation,
            DerivedV1Id(operation.operation_id_, purpose + "authorize"),
            DerivedV1Id(operation.operation_id_,
                        purpose + "authorize-attempt"));
        if (receipt.has_value() &&
            receipt->status_ != MetaDirectiveResultStatus::kSucceeded) {
          return fence_or_abort(absl::StrCat(
              "group=", declaration.group_id_,
              " node=", receipt->recipient_node_id_,
              " replica source authorization: ", receipt->result_));
        }
      }
      return Conflict(absl::StrCat("group=", declaration.group_id_,
                                   " authority changed during source "
                                   "authorization"));
    }
    if (!grant->grant_.has_value() ||
        operation.current_directives_.size() != expected_directives ||
        operation.current_directives_.empty()) {
      return Conflict(absl::StrCat("group=", declaration.group_id_,
                                   " source authorization directives were "
                                   "invalidated"));
    }
    const std::uint64_t revision =
        operation.current_directives_.front().directive_revision_;
    for (std::size_t index = 0; index < declaration.replica_node_ids_.size();
         ++index) {
      const std::string& replica = declaration.replica_node_ids_[index];
      const std::string purpose = absl::StrCat("replica/", replica, "/");
      const auto& authorize = operation.current_directives_[index];
      if (revision == 0 || authorize.directive_revision_ != revision ||
          authorize.spec_.directive_id_ !=
              DerivedV1Id(operation.operation_id_, purpose + "authorize") ||
          authorize.spec_.attempt_id_ !=
              DerivedV1Id(operation.operation_id_,
                          purpose + "authorize-attempt") ||
          authorize.spec_.kind_ != kMetaDirectiveAuthorizeSource ||
          authorize.spec_.recipient_node_id_ != declaration.primary_node_id_) {
        return Conflict(
            absl::StrCat("group=", declaration.group_id_, " node=", replica,
                         " source authorization batch differs from intent"));
      }
    }

    std::optional<MetaTerminalReceipt> failed;
    bool all_succeeded = true;
    for (const auto& authorize : operation.current_directives_) {
      const auto receipt = ReceiptFor(operation, authorize.spec_.directive_id_,
                                      authorize.spec_.attempt_id_,
                                      authorize.directive_revision_);
      if (!receipt.has_value()) {
        all_succeeded = false;
        continue;
      }
      if (receipt->status_ != MetaDirectiveResultStatus::kSucceeded) {
        failed = receipt;
        break;
      }
    }
    if (failed.has_value()) {
      return fence_or_abort(absl::StrCat(
          "group=", declaration.group_id_, " node=", failed->recipient_node_id_,
          " replica source authorization: ", failed->result_));
    }
    if (source_incarnation_changed) {
      return retain_incarnation_failure(
          declaration.primary_node_id_,
          "source boot/history changed during replica source authorization");
    }
    if (runtime.leader_authority_eligible_) {
      for (const auto& authorize : operation.current_directives_) {
        const auto target = std::find_if(
            runtime.nodes_.begin(), runtime.nodes_.end(),
            [&](const auto& node) {
              return node.node_id_ == authorize.spec_.target_node_id_;
            });
        if (target != runtime.nodes_.end() &&
            target->boot_id_ != Hex(authorize.spec_.target_boot_id_)) {
          return retain_incarnation_failure(
              authorize.spec_.target_node_id_,
              "target boot changed during replica source authorization");
        }
      }
    }
    if (!all_succeeded) return std::nullopt;

    // Keep each acknowledged authorization byte-identical so the operation
    // store retains its original directive revision. The subsequent FDS can
    // therefore carry the already-installed source capability while adding
    // target work under a distinct, later revision.
    TransitionOperationPhase transition;
    transition.operation_id_ = operation.operation_id_;
    transition.expected_revision_ = operation.revision_;
    transition.kind_phase_blob_ = kGroupPhaseReplicate;
    transition.current_directives_.reserve(expected_directives * 2);
    for (std::size_t index = 0; index < declaration.replica_node_ids_.size();
         ++index) {
      const std::string& replica = declaration.replica_node_ids_[index];
      const std::string purpose = absl::StrCat("replica/", replica, "/");
      const MetaDirectiveSpec& authorize =
          operation.current_directives_[index].spec_;
      transition.current_directives_.push_back(authorize);
      MetaDirectiveSpec rebuild = authorize;
      rebuild.directive_id_ =
          DerivedV1Id(operation.operation_id_, purpose + "rebuild");
      rebuild.attempt_id_ =
          DerivedV1Id(operation.operation_id_, purpose + "rebuild-attempt");
      rebuild.recipient_node_id_ = replica;
      rebuild.kind_ = kMetaDirectiveRebuild;

      transition.current_directives_.push_back(std::move(rebuild));
    }
    return Emit(std::move(transition));
  }

  if (operation.kind_phase_blob_ == kGroupPhaseReplicate) {
    const std::size_t expected_directives =
        declaration.replica_node_ids_.size() * 2;
    if (!grant->grant_.has_value()) {
      for (const std::string& replica : declaration.replica_node_ids_) {
        const std::string purpose = absl::StrCat("replica/", replica, "/");
        for (const std::string_view kind : {"authorize", "rebuild"}) {
          const auto receipt = ReceiptFor(
              operation,
              DerivedV1Id(operation.operation_id_, purpose + std::string(kind)),
              DerivedV1Id(operation.operation_id_,
                          purpose + std::string(kind) + "-attempt"));
          if (receipt.has_value() &&
              receipt->status_ != MetaDirectiveResultStatus::kSucceeded) {
            return fence_or_abort(
                absl::StrCat("group=", declaration.group_id_,
                             " node=", receipt->recipient_node_id_,
                             " replica initialization: ", receipt->result_));
          }
        }
      }
      return Conflict(absl::StrCat("group=", declaration.group_id_,
                                   " authority changed during replication"));
    }
    if (operation.current_directives_.size() != expected_directives ||
        operation.current_directives_.empty()) {
      return Conflict(absl::StrCat("group=", declaration.group_id_,
                                   " replica directives were invalidated"));
    }
    if (!grant->grant_.has_value())
      return Conflict(absl::StrCat("group=", declaration.group_id_,
                                   " replica directives were invalidated"));
    const std::uint64_t authorization_revision =
        operation.current_directives_.front().directive_revision_;
    const std::uint64_t rebuild_revision =
        operation.current_directives_[1].directive_revision_;
    for (std::size_t index = 0; index < declaration.replica_node_ids_.size();
         ++index) {
      const std::string& replica = declaration.replica_node_ids_[index];
      const std::string purpose = absl::StrCat("replica/", replica, "/");
      const auto& authorize = operation.current_directives_[index * 2];
      const auto& rebuild = operation.current_directives_[index * 2 + 1];
      MetaDirectiveSpec expected_rebuild = authorize.spec_;
      expected_rebuild.directive_id_ =
          DerivedV1Id(operation.operation_id_, purpose + "rebuild");
      expected_rebuild.attempt_id_ =
          DerivedV1Id(operation.operation_id_, purpose + "rebuild-attempt");
      expected_rebuild.recipient_node_id_ = replica;
      expected_rebuild.kind_ = kMetaDirectiveRebuild;

      if (authorization_revision == 0 || rebuild_revision == 0 ||
          rebuild_revision <= authorization_revision ||
          authorize.directive_revision_ != authorization_revision ||
          rebuild.directive_revision_ != rebuild_revision ||
          authorize.spec_.directive_id_ !=
              DerivedV1Id(operation.operation_id_, purpose + "authorize") ||
          authorize.spec_.attempt_id_ !=
              DerivedV1Id(operation.operation_id_,
                          purpose + "authorize-attempt") ||
          authorize.spec_.kind_ != kMetaDirectiveAuthorizeSource ||
          authorize.spec_.recipient_node_id_ != declaration.primary_node_id_ ||
          rebuild.spec_ != expected_rebuild)
        return Conflict(
            absl::StrCat("group=", declaration.group_id_, " node=", replica,
                         " replica directive batch differs from intent"));
    }
    std::optional<MetaTerminalReceipt> failed;
    bool all_succeeded = true;
    for (std::size_t index = 0; index < declaration.replica_node_ids_.size();
         ++index) {
      for (std::size_t offset = 0; offset < 2; ++offset) {
        const MetaCurrentDirective& current =
            operation.current_directives_[index * 2 + offset];
        const auto receipt =
            ReceiptFor(operation, current.spec_.directive_id_,
                       current.spec_.attempt_id_, current.directive_revision_);
        if (!receipt.has_value()) {
          all_succeeded = false;
          continue;
        }
        if (receipt->status_ != MetaDirectiveResultStatus::kSucceeded) {
          failed = receipt;
          break;
        }
      }
      if (failed.has_value()) break;
    }
    if (failed.has_value())
      return fence_or_abort(absl::StrCat(
          "group=", declaration.group_id_, " node=", failed->recipient_node_id_,
          " replica initialization: ", failed->result_));
    // Durable success remains historical fact after a restart. For unfinished
    // work, however, neither source capability nor target execution may cross
    // a boot boundary. SendDirectives deliberately skips an old recipient
    // boot, so waiting for its missing receipt could otherwise last forever.
    if (all_succeeded) return Advance(operation, kGroupPhaseReady);
    if (source_incarnation_changed)
      return retain_incarnation_failure(
          declaration.primary_node_id_,
          "source boot/history changed during replica initialization");
    if (runtime.leader_authority_eligible_) {
      for (const auto& current : operation.current_directives_) {
        const auto& spec = current.spec_;
        if (spec.kind_ != kMetaDirectiveRebuild ||
            ReceiptFor(operation, spec.directive_id_, spec.attempt_id_,
                       current.directive_revision_)
                .has_value())
          continue;
        const auto target =
            std::find_if(runtime.nodes_.begin(), runtime.nodes_.end(),
                         [&](const auto& node) {
                           return node.node_id_ == spec.target_node_id_;
                         });
        if (target != runtime.nodes_.end() &&
            target->boot_id_ != Hex(spec.target_boot_id_))
          return retain_incarnation_failure(
              spec.target_node_id_,
              "target boot changed during replica initialization");
      }
    }
    return std::nullopt;
  }

  if (operation.kind_phase_blob_ == kGroupPhaseReady) {
    if (!operation.current_directives_.empty())
      return Conflict(absl::StrCat("group=", declaration.group_id_,
                                   " ready operation retains directives"));
    return Complete(operation);
  }
  return Conflict(absl::StrCat("group=", declaration.group_id_,
                               " has an unknown creation phase"));
}

Plan PlanV1ClusterCreateStep(const MetaCommittedView& view,
                             const MetaOperationRecord& operation,
                             const MetaDataControlRuntimeSnapshot& runtime,
                             const MetaClusterCreateRaftView& raft) {
  if (operation.kind_ != kMetaClusterCreateOperationKind ||
      IsTerminal(operation.lifecycle_))
    return std::nullopt;
  MetaOperationId intent_root{};
  auto manifest = DecodeClusterCreateRequest(operation.intent_, &intent_root);
  if (!manifest.ok())
    return Conflict("creation intent is not a recoverable v1 plan");
  const auto& stores = view.stores();
  if (auto status = detail::ValidateClusterCreateMetaSet(view, *manifest, raft);
      !status.ok()) {
    return Conflict(status.message());
  }
  if (auto status = ValidateV1Nodes(stores, *manifest, false); !status.ok())
    return Conflict(status.message());
  if (auto status = ValidateV1GroupsKnown(stores, *manifest,
                                          operation.operation_id_, false);
      !status.ok())
    return Conflict(status.message());

  const std::string phase = operation.kind_phase_blob_.empty()
                                ? std::string(kRootPhaseWaitMetaBarrier)
                                : operation.kind_phase_blob_;
  if (phase == kRootPhaseWaitMetaBarrier) {
    if (!MetaBarrierSatisfied(operation, raft)) return std::nullopt;
    return Advance(operation, kRootPhasePolicy);
  }
  if (phase == kRootPhasePolicy) {
    // Install the global families before registering any Data recipient or
    // creating a Group. From that point onward every intermediate committed
    // state must be projectable as a complete FDS, including BeginGroupTerm's
    // fenced, pre-authority state.
    if (stores.identity_.NodeCount() != 0 ||
        stores.topology_.GroupCount() != 0) {
      return Conflict(
          "creation reached Policy bootstrap after Data topology appeared");
    }
    if (!stores.policy_.CurrentAutomaticUncontrolledFailover().has_value()) {
      PutPolicy command;
      command.policy_id_ = kAutomaticUncontrolledFailoverPolicyId;
      command.version_ = 1;
      command.content_ = AutomaticFailoverPolicyContent(*manifest);
      return Emit(std::move(command));
    }
    if (!stores.policy_.CurrentAuthorityLease().has_value()) {
      PutPolicy command;
      command.policy_id_ = kAuthorityLeasePolicyId;
      command.version_ = 1;
      command.content_ = AuthorityLeasePolicyContent(*manifest);
      return Emit(std::move(command));
    }
    if (!stores.policy_.CurrentCandidateRecovery().has_value()) {
      PutPolicy command;
      command.policy_id_ = kCandidateRecoveryPolicyId;
      command.version_ = 1;
      command.content_ =
          absl::StrCat("{\"kind\":\"candidate-recovery-v1\",\"budget_ms\":",
                       manifest->candidate_recovery_budget_ms_, "}");
      return Emit(std::move(command));
    }
    return Advance(operation, kRootPhaseRegisterData);
  }
  if (phase == kRootPhaseRegisterData) {
    if (stores.topology_.GroupCount() != 0)
      return Conflict("creation Group appeared before registration completed");
    for (const auto& node : manifest->data_nodes_) {
      if (!stores.identity_.FindNode(node.node_id_).has_value()) {
        RegisterNode command;
        command.node_id_ = node.node_id_;
        command.principal_ = absl::StrCat("lavik://node/", node.node_id_);
        command.role_ = DeclaredRole(*manifest, node.node_id_);
        command.endpoints_ = V1DataEndpoints(node);
        return Emit(std::move(command));
      }
    }
    return Advance(operation, kRootPhaseCreateGroups);
  }

  if (auto status = ValidateV1Nodes(stores, *manifest, true); !status.ok())
    return Conflict(status.message());
  if (phase == kRootPhaseCreateGroups) {
    for (const auto& declaration : manifest->groups_) {
      auto group = stores.topology_.FindGroup(declaration.group_id_);
      if (!group.has_value()) {
        CreateGroup command;
        command.group_id_ = declaration.group_id_;
        command.new_topology_epoch_ = stores.topology_.TopologyEpoch() + 1;
        return Emit(std::move(command));
      }
      if (group->record_.group_term_ > 1 ||
          group->record_.population_manifest_revision_ != 0 ||
          group->record_.partition_replication_epoch_ != 0 ||
          !group->record_.owner_.empty())
        return Conflict(
            absl::StrCat("creation Group advanced unexpectedly: group=",
                         declaration.group_id_));
      if (auto status = ValidateV1GroupMembers(*group, declaration,
                                               operation.operation_id_, false);
          !status.ok())
        return Conflict(status.message());
      for (const auto& [node_id, role] : DeclaredMembers(declaration)) {
        const auto member = std::find_if(
            group->members_.begin(), group->members_.end(),
            [&](const auto& item) { return item.node_id_ == node_id; });
        if (member == group->members_.end()) {
          AssignNodeToGroup command;
          command.group_id_ = declaration.group_id_;
          command.node_id_ = node_id;
          command.assignment_id_ = V1AssignmentId(
              operation.operation_id_, declaration.group_id_, node_id);
          command.role_ = role;
          command.expected_revision_ = group->revision_;
          command.new_topology_epoch_ = stores.topology_.TopologyEpoch() + 1;
          return Emit(std::move(command));
        }
      }
      if (group->record_.group_term_ == 0) {
        BeginGroupTerm command;
        command.group_id_ = declaration.group_id_;
        command.new_term_ = 1;
        return Emit(std::move(command));
      }
    }
    return Advance(operation, kRootPhaseSlotMap);
  }

  if (auto status = ValidateV1GroupsKnown(stores, *manifest,
                                          operation.operation_id_, true);
      !status.ok())
    return Conflict(status.message());
  if (phase == kRootPhaseSlotMap) {
    bool slots_empty = false;
    const bool slots_match = V1SlotMapMatches(stores, *manifest, &slots_empty);
    for (const auto& declaration : manifest->groups_) {
      const auto group = stores.topology_.FindGroup(declaration.group_id_);
      if (!group.has_value() || group->record_.group_term_ != 1 ||
          !group->record_.owner_.empty())
        return Conflict(absl::StrCat("creation Group is not ready for Slots: ",
                                     declaration.group_id_));
    }
    if (slots_empty) {
      SetSlotMap command;
      for (const auto& range : manifest->slot_ranges_)
        command.ranges_.push_back({range.first_, range.last_, range.group_id_});
      command.new_topology_epoch_ = stores.topology_.TopologyEpoch() + 1;
      return Emit(std::move(command));
    }
    if (!slots_match) return Conflict("creation Slot map differs from intent");
    return Advance(operation, kRootPhasePopulation);
  }

  bool slots_empty = false;
  if (!V1SlotMapMatches(stores, *manifest, &slots_empty) || slots_empty)
    return Conflict("creation Slot map differs from intent");
  if (phase == kRootPhasePopulation) {
    for (const auto& declaration : manifest->groups_) {
      const auto population =
          V1PopulationManifest(*manifest, declaration.group_id_);
      if (!stores.population_manifest_.Contains(population.manifest_digest_))
        return Emit(population);
      const auto group = stores.topology_.FindGroup(declaration.group_id_);
      const auto grant = stores.topology_.AuthorityFor(declaration.group_id_);
      if (!group.has_value() || !grant.has_value())
        return Conflict(absl::StrCat("creation Group disappeared: group=",
                                     declaration.group_id_));
      const bool population_empty =
          group->record_.population_manifest_revision_ == 0 &&
          group->record_.partition_replication_epoch_ == 0;
      const bool population_matches =
          group->record_.population_manifest_revision_ == 1 &&
          group->record_.population_manifest_digest_ ==
              population.manifest_digest_ &&
          group->record_.partition_replication_epoch_ == 1;
      if (population_empty) {
        SetGroupReplicationState command;
        command.group_id_ = declaration.group_id_;
        command.new_population_manifest_revision_ = 1;
        command.new_population_manifest_digest_ = population.manifest_digest_;
        command.new_partition_replication_epoch_ = 1;
        command.new_topology_epoch_ = stores.topology_.TopologyEpoch() + 1;
        return Emit(std::move(command));
      }
      if (!population_matches)
        return Conflict(
            absl::StrCat("creation population differs from intent: group=",
                         declaration.group_id_));
      const bool authority_matches =
          grant->grant_.has_value() &&
          grant->grant_->owner_ == declaration.primary_node_id_ &&
          grant->group_term_ == 1;
      if (!authority_matches) {
        if (!group->record_.owner_.empty() || grant->grant_.has_value())
          return Conflict(absl::StrCat("creation authority changed: group=",
                                       declaration.group_id_));
        ActivateAuthority command;
        command.group_id_ = declaration.group_id_;
        command.expected_term_ = 1;
        command.new_owner_ = declaration.primary_node_id_;
        command.new_topology_epoch_ = stores.topology_.TopologyEpoch() + 1;
        return Emit(std::move(command));
      }
    }
    return Advance(operation, kRootPhaseWaitDataProjection);
  }

  if (phase != kRootPhaseWaitDataProjection &&
      phase != kRootPhaseInitializeGroups)
    return Conflict("unknown v1 creation phase");
  if (auto status =
          ValidateV1FinalTopology(stores, *manifest, operation.operation_id_,
                                  phase == kRootPhaseInitializeGroups);
      !status.ok())
    return Conflict(status.message());
  if (phase == kRootPhaseWaitDataProjection) {
    if (!runtime.leader_authority_eligible_) return std::nullopt;
    for (const auto& declaration : manifest->groups_) {
      const auto group = stores.topology_.FindGroup(declaration.group_id_);
      const auto grant = stores.topology_.AuthorityFor(declaration.group_id_);
      for (const auto& [node_id, role] : DeclaredMembers(declaration)) {
        (void)role;
        const auto node = std::find_if(
            runtime.nodes_.begin(), runtime.nodes_.end(),
            [&](const auto& item) { return item.node_id_ == node_id; });
        if (node == runtime.nodes_.end() ||
            !ProjectionMatches(*node, *group, *grant, view.applied_index()))
          return std::nullopt;
      }
    }
    return Advance(operation, kRootPhaseInitializeGroups);
  }

  for (const auto& declaration : manifest->groups_) {
    const MetaOperationId child_id = detail::ClusterCreateV1GroupOperationId(
        operation.operation_id_, declaration.group_id_);
    const auto child = stores.operation_.FindOperation(child_id);
    if (!child.has_value()) {
      if (stores.operation_.OperationKnown(child_id))
        return Conflict(
            absl::StrCat("creation Group operation was archived: group=",
                         declaration.group_id_));
      const auto group = stores.topology_.FindGroup(declaration.group_id_);
      const auto grant = stores.topology_.AuthorityFor(declaration.group_id_);
      const auto primary = std::find_if(
          runtime.nodes_.begin(), runtime.nodes_.end(), [&](const auto& node) {
            return node.node_id_ == declaration.primary_node_id_;
          });
      if (!runtime.leader_authority_eligible_ ||
          primary == runtime.nodes_.end() ||
          !ProjectionMatches(*primary, *group, *grant, view.applied_index()))
        return std::nullopt;
      if (!cluster::control::IsCanonicalIdentity160(primary->boot_id_))
        return Conflict(absl::StrCat("group=", declaration.group_id_,
                                     " node=", declaration.primary_node_id_,
                                     " boot identity is malformed"));
      SubmitOperation submit;
      submit.operation_id_ = child_id;
      submit.kind_ = kMetaClusterCreateV1GroupOperationKind;
      submit.intent_ =
          absl::StrCat("cluster-create-v1-group ", Hex(operation.operation_id_),
                       " ", absl::BytesToHexString(declaration.group_id_), " ",
                       primary->boot_id_);
      submit.intent_hash_ = MetaSha256(submit.intent_);
      submit.replication_history_id_ = primary->replication_history_id_;
      return Emit(std::move(submit));
    }
    if (child->lifecycle_ == MetaOperationLifecycle::kCompleted) continue;
    if (child->lifecycle_ == MetaOperationLifecycle::kAborted)
      return Abort(operation, absl::StrCat("group=", declaration.group_id_, " ",
                                           child->terminal_result_));
    auto next = PlanV1GroupStep(view, operation, *child, *manifest, declaration,
                                runtime);
    if (!next.ok() || next->has_value()) return next;
    return std::nullopt;
  }
  return Complete(operation);
}

}  // namespace

MetaOperationId detail::ClusterCreateV1GroupOperationId(
    const MetaOperationId& root, std::string_view group_id) {
  return DerivedV1Id(root,
                     absl::StrCat("group/", absl::BytesToHexString(group_id)));
}

absl::Status detail::ValidateClusterCreateMetaSet(
    const MetaCommittedView& view, const ClusterCreateManifestV1& manifest,
    const MetaClusterCreateRaftView& raft) {
  return ValidateMetaSet(view, manifest, raft);
}

Plan detail::PlanClusterCreateStep(
    const MetaCommittedView& view, const MetaOperationRecord& operation,
    const MetaDataControlRuntimeSnapshot& runtime,
    const MetaClusterCreateRaftView& raft) {
  if (operation.kind_ == kMetaClusterCreateOperationKind)
    return PlanV1ClusterCreateStep(view, operation, runtime, raft);
  return std::nullopt;
}

struct MetaClusterCreateReconciler::Core {
  bycorf::ForeignExecutor executor_;
  std::shared_ptr<MetaMembershipGate> membership_gate_;
  std::shared_ptr<MetaDataControlRuntimeStatus> runtime_status_;
  std::shared_ptr<MetaRaft> server_;
  std::uint64_t max_peer_response_age_us_ = 0;
  // Worker-owned except the atomic ingress/stop-completion flags below.
  bool running_ = false;
  bool cancelled_ = true;
  bool shutdown_ = false;
  std::vector<std::shared_ptr<std::promise<void>>> waiters_;
  std::atomic<bool> shutdown_complete_{false};
  std::atomic<bool> stopping_{false};
};

MetaClusterCreateReconciler::MetaClusterCreateReconciler(
    bycorf::ForeignExecutor executor, std::shared_ptr<MetaMembershipGate> gate,
    std::shared_ptr<MetaDataControlRuntimeStatus> runtime,
    std::shared_ptr<MetaRaft> server, std::uint64_t max_peer_response_age_us)
    : core_(std::make_shared<Core>()) {
  core_->executor_ = std::move(executor);
  core_->membership_gate_ = std::move(gate);
  core_->runtime_status_ = std::move(runtime);
  core_->server_ = std::move(server);
  core_->max_peer_response_age_us_ = max_peer_response_age_us;
}
MetaClusterCreateReconciler::~MetaClusterCreateReconciler() { Shutdown(); }

void MetaClusterCreateReconciler::Start(MetaLeaderContext& context) {
  const auto core = core_;
  if (!core->executor_.Notify([core, context = &context]() noexcept {
        if (core->shutdown_) return;
        if (core->running_) std::terminate();
        core->cancelled_ = false;
        core->running_ = true;
        bycorf::ThisWorker().self_->Spawn(Run(core, context));
      }))
    std::terminate();
}

void MetaClusterCreateReconciler::Stop(bool permanent) {
  const auto core = core_;
  if (core->shutdown_complete_.load(std::memory_order_acquire)) return;
  if (permanent) core->stopping_.store(true, std::memory_order_release);
  auto complete = std::make_shared<std::promise<void>>();
  auto done = complete->get_future();
  if (!core->executor_.Notify([core, complete, permanent]() noexcept {
        core->shutdown_ |= permanent;
        core->cancelled_ = true;
        if (core->running_)
          core->waiters_.push_back(complete);
        else
          complete->set_value();
      })) {
    if (core->shutdown_complete_.load(std::memory_order_acquire)) return;
    std::terminate();
  }
  done.wait();
  if (permanent)
    core->shutdown_complete_.store(true, std::memory_order_release);
}
void MetaClusterCreateReconciler::CancelAndWait() { Stop(false); }
void MetaClusterCreateReconciler::Shutdown() { Stop(true); }
bool MetaClusterCreateReconciler::accepting() const {
  return !core_->stopping_.load(std::memory_order_acquire);
}

bycorf::Task<absl::Status> MetaClusterCreateReconciler::Run(
    std::shared_ptr<Core> core, MetaLeaderContext* context) {
  std::unique_ptr<MetaMembershipGate::Lease> lease;
  std::string last_cut;
  auto changed = std::make_shared<std::atomic<bool>>(false);
  auto subscribe = [&] {
    return context->SubscribeCommitted([changed](const MetaCommitEvent&) {
      changed->store(true, std::memory_order_release);
    });
  };
  auto subscribed = subscribe();
  while (!core->cancelled_) {
    // CommittedView includes snapshot restoration and WAL replay. There is
    // deliberately no saved process-local task list to reconstruct on boot.
    // Idle polling only reads a notification bit, not the entire metadata
    // aggregate. Overflow resubscribes from an atomic view instead of losing
    // a committed task. Active Data waits still observe volatile runtime.
    if (subscribed.subscription_->needs_resync()) subscribed = subscribe();
    if (changed->exchange(false, std::memory_order_acq_rel))
      subscribed.view_ = context->CommittedView();
    const auto& view = subscribed.view_;
    const auto& lifecycle = view.topology().ClusterLifecycle();
    const bool has_creation =
        lifecycle.state_ == MetaClusterLifecycle::kCreating;
    const auto operation =
        has_creation
            ? view.operation().FindOperation(lifecycle.root_operation_id_)
            : std::optional<MetaOperationRecord>{};
    // The runtime always reports actual peer application. Only this planner
    // imposes the all-genesis-member barrier; writes retain majority semantics.
    if (!operation.has_value())
      lease.reset();
    else {
      if (lease == nullptr) lease = core->membership_gate_->TryAcquire();
      if (lease != nullptr &&
          !operation->kind_phase_blob_.starts_with("recovery-required:")) {
        std::string cut = operation->kind_phase_blob_.empty()
                              ? "submitted"
                              : operation->kind_phase_blob_;
        if (cut == kRootPhaseInitializeGroups) {
          MetaOperationId intent_root{};
          const auto manifest =
              DecodeClusterCreateRequest(operation->intent_, &intent_root);
          if (manifest.ok()) {
            for (const auto& declaration : manifest->groups_) {
              const auto child = view.operation().FindOperation(
                  detail::ClusterCreateV1GroupOperationId(
                      operation->operation_id_, declaration.group_id_));
              if (!child.has_value()) break;
              if (child->lifecycle_ == MetaOperationLifecycle::kCompleted) {
                cut = "child-completed";
                continue;
              }
              if (child->kind_phase_blob_ == kGroupPhaseReady)
                cut = "directive-removed";
              else if (!child->terminal_receipts_.empty())
                cut = "result-committed";
              else if (!child->current_directives_.empty())
                cut = "initialization-issued";
              break;
            }
          }
        }
        if (cut != last_cut) {
          spdlog::info("cluster-create {} phase={}",
                       Hex(operation->operation_id_), cut);
          last_cut = cut;
        }
        // Debug pauses occur on the owner coroutine and still obey shutdown;
        // tests can stop at durable cuts without killing unrelated processes.
        bool paused = false;
        LAVIK_FAULT_INJECT(paused = LAVIK_FAULT_MATCHES(
                               "LAVIK_TEST_PAUSE_CLUSTER_CREATE_PHASE", cut););
        if (!paused) {
          MetaClusterCreateRaftView raft_view;
          raft_view.local_server_id_ = core->server_->get_id();
          raft_view.max_response_age_us_ = core->max_peer_response_age_us_;
          auto membership =
              CaptureMembershipConfig(core->server_->get_config());
          if (membership.ok()) raft_view.members_ = std::move(*membership);
          for (const auto& peer : core->server_->get_peer_info_all()) {
            raft_view.peer_progress_.push_back(
                {static_cast<std::uint32_t>(peer.id_),
                 peer.last_sm_committed_idx_, peer.last_succ_resp_us_});
          }
          auto planned = detail::PlanClusterCreateStep(
              view, *operation, core->runtime_status_->Snapshot(), raft_view);
          if (!planned.ok()) {
            spdlog::warn("cluster-create {} requires recovery: {}",
                         Hex(operation->operation_id_),
                         planned.status().message());
            planned = Advance(
                *operation,
                absl::StrCat("recovery-required:", planned.status().message()));
          }
          if (planned.ok() && planned->has_value() && !core->cancelled_) {
            MetaCommand command = std::move(**planned);
            auto request = cluster::control::GenerateId128();
            if (!request.ok()) std::terminate();
            std::visit([&](auto& c) { c.request_id_ = *request; }, command);
            // An accepted proposal may still commit; the next owner re-reads
            // its effect before deciding whether anything remains to do.
            const auto applied = co_await context->Propose(std::move(command));
            if (core->cancelled_) break;
            // Own completion may precede subscription delivery. Force a new
            // committed view before planning, including domain rejections.
            changed->store(true, std::memory_order_release);
            if (applied.ok() &&
                applied->verdict_ == MetaAuditVerdict::kAccepted)
              continue;
            spdlog::warn("cluster-create {} proposal deferred: {}",
                         Hex(operation->operation_id_),
                         applied.ok()
                             ? applied->detail_
                             : std::string(applied.status().message()));
          }
        }
      }
    }
    const auto slept = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                                 std::chrono::milliseconds(25));
    if (!slept.ok()) break;
  }
  lease.reset();
  core->running_ = false;
  for (const auto& waiter : core->waiters_) waiter->set_value();
  core->waiters_.clear();
  co_return absl::OkStatus();
}
}  // namespace lavik::meta
