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

#include "lavik/meta/commands.h"

#include <algorithm>
#include <limits>
#include <utility>
#include <vector>

#include "absl/strings/str_cat.h"
#include "lavik/meta/value_codec.h"
#include "lavik/status_macros.h"

namespace lavik::meta {
namespace {

// Encode-side cap enforcement. A locally constructed out-of-spec command is a
// proposal-validation failure, not wire corruption, hence kDomainReject;
// nothing out-of-spec ever reaches the wire.
absl::Status CheckCap(std::string_view field, std::size_t size,
                      std::uint32_t cap) {
  if (size > cap) {
    return MetaDomainRejectError(
        absl::StrCat("field ", field, " size ", size, " exceeds cap ", cap));
  }
  return absl::OkStatus();
}

// Writes tag + request_id + actor, the fixed header of every command body.
// The actor fields are ordinary bounded strings on the wire so a follower's
// apply can persist the trusted entry's injected ActorContext into
// audit/journal (see commands.h).
absl::Status WriteCommandHeader(MetaWriter& w, MetaCommandTag tag,
                                const MetaRequestId& request_id,
                                const ActorContext& actor) {
  LAVIK_RETURN_IF_ERROR(CheckCap("actor_principal", actor.principal_.size(),
                                 kMaxMetaPrincipalBytes));
  LAVIK_RETURN_IF_ERROR(CheckCap("readable_time", actor.readable_time_.size(),
                                 kMaxMetaActorReadableTimeBytes));
  w.WriteU16(static_cast<std::uint16_t>(tag));
  WriteFixedArray(w, request_id);
  WriteActorContext(w, actor);
  return absl::OkStatus();
}

absl::StatusOr<std::string> ReadBoundedString(MetaReader& r,
                                              std::uint32_t cap) {
  auto raw = r.ReadString(cap);
  LAVIK_RETURN_IF_ERROR(raw);
  return std::string(*raw);
}

struct MetaCommandHeader {
  MetaRequestId request_id_{};
  ActorContext actor_;
};

absl::StatusOr<MetaCommandHeader> ReadCommandHeader(MetaReader& r) {
  auto request_id = ReadFixedArray<16>(r);
  LAVIK_RETURN_IF_ERROR(request_id);
  auto actor = ReadActorContext(r);
  LAVIK_RETURN_IF_ERROR(actor);
  MetaCommandHeader header;
  header.request_id_ = *request_id;
  header.actor_ = std::move(*actor);
  return header;
}

// ---------------------------------------------------------------------------
// Shared field codecs.
// ---------------------------------------------------------------------------

absl::Status WriteNodeId(MetaWriter& w, const std::string& node_id) {
  LAVIK_RETURN_IF_ERROR(CheckCap("node_id", node_id.size(), kMetaNodeIdBytes));
  w.WriteString(node_id);
  return absl::OkStatus();
}

absl::StatusOr<std::string> ReadNodeId(MetaReader& r) {
  return ReadBoundedString(r, kMetaNodeIdBytes);
}

absl::Status WriteGroupId(MetaWriter& w, const std::string& group_id) {
  LAVIK_RETURN_IF_ERROR(
      CheckCap("group_id", group_id.size(), kMaxMetaGroupIdBytes));
  w.WriteString(group_id);
  return absl::OkStatus();
}

absl::StatusOr<std::string> ReadGroupId(MetaReader& r) {
  return ReadBoundedString(r, kMaxMetaGroupIdBytes);
}

absl::Status WriteEndpoints(MetaWriter& w,
                            const std::vector<std::string>& endpoints) {
  LAVIK_RETURN_IF_ERROR(
      CheckCap("endpoints", endpoints.size(), kMaxMetaEndpointsPerNode));
  for (const std::string& ep : endpoints) {
    LAVIK_RETURN_IF_ERROR(
        CheckCap("endpoint", ep.size(), kMaxMetaEndpointBytes));
  }
  w.WriteList(endpoints, [](MetaWriter& ww, const std::string& ep) {
    ww.WriteString(ep);
  });
  return absl::OkStatus();
}

absl::StatusOr<std::vector<std::string>> ReadEndpoints(MetaReader& r) {
  return r.ReadList<std::string>(kMaxMetaEndpointsPerNode, [](MetaReader& rr) {
    return ReadBoundedString(rr, kMaxMetaEndpointBytes);
  });
}

absl::Status WriteRole(MetaWriter& w, MetaNodeRole role) {
  w.WriteU8(static_cast<std::uint8_t>(role));
  return absl::OkStatus();
}

absl::StatusOr<MetaNodeRole> ReadRole(MetaReader& r) {
  auto role = r.ReadU8();
  LAVIK_RETURN_IF_ERROR(role);
  if (*role != static_cast<std::uint8_t>(MetaNodeRole::kPrimary) &&
      *role != static_cast<std::uint8_t>(MetaNodeRole::kReplica)) {
    return MetaFailStopError("unknown node role");
  }
  return static_cast<MetaNodeRole>(*role);
}

// ---------------------------------------------------------------------------
// Per-command body codecs. Writers return Status for cap failures; readers
// return StatusOr and every failure is the fail-stop class.
// ---------------------------------------------------------------------------

absl::Status WriteCommandBody(MetaWriter& w, const RegisterNode& cmd) {
  LAVIK_RETURN_IF_ERROR(
      CheckCap("principal", cmd.principal_.size(), kMaxMetaPrincipalBytes));
  LAVIK_RETURN_IF_ERROR(WriteCommandHeader(w, MetaCommandTag::kRegisterNode,
                                           cmd.request_id_, cmd.actor_));
  LAVIK_RETURN_IF_ERROR(WriteNodeId(w, cmd.node_id_));
  w.WriteString(cmd.principal_);
  LAVIK_RETURN_IF_ERROR(WriteEndpoints(w, cmd.endpoints_));

  return WriteRole(w, cmd.role_);
}

absl::StatusOr<RegisterNode> ReadRegisterNodeBody(MetaReader& r) {
  auto header = ReadCommandHeader(r);
  LAVIK_RETURN_IF_ERROR(header);
  auto node_id = ReadNodeId(r);
  LAVIK_RETURN_IF_ERROR(node_id);
  auto principal = ReadBoundedString(r, kMaxMetaPrincipalBytes);
  LAVIK_RETURN_IF_ERROR(principal);
  auto endpoints = ReadEndpoints(r);
  LAVIK_RETURN_IF_ERROR(endpoints);

  auto role = ReadRole(r);
  LAVIK_RETURN_IF_ERROR(role);
  RegisterNode cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.node_id_ = std::move(*node_id);
  cmd.principal_ = std::move(*principal);
  cmd.endpoints_ = std::move(*endpoints);

  cmd.role_ = *role;
  return cmd;
}

absl::Status WriteCommandBody(MetaWriter& w, const UpdateNode& cmd) {
  LAVIK_RETURN_IF_ERROR(WriteCommandHeader(w, MetaCommandTag::kUpdateNode,
                                           cmd.request_id_, cmd.actor_));
  LAVIK_RETURN_IF_ERROR(WriteNodeId(w, cmd.node_id_));
  w.WriteU64(cmd.expected_revision_);
  LAVIK_RETURN_IF_ERROR(WriteEndpoints(w, cmd.endpoints_));

  w.WriteU64(cmd.new_topology_epoch_);
  return absl::OkStatus();
}

absl::StatusOr<UpdateNode> ReadUpdateNodeBody(MetaReader& r) {
  auto header = ReadCommandHeader(r);
  LAVIK_RETURN_IF_ERROR(header);
  auto node_id = ReadNodeId(r);
  LAVIK_RETURN_IF_ERROR(node_id);
  auto expected_revision = r.ReadU64();
  LAVIK_RETURN_IF_ERROR(expected_revision);
  auto endpoints = ReadEndpoints(r);
  LAVIK_RETURN_IF_ERROR(endpoints);

  auto topology_epoch = r.ReadU64();
  LAVIK_RETURN_IF_ERROR(topology_epoch);
  UpdateNode cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.node_id_ = std::move(*node_id);
  cmd.expected_revision_ = *expected_revision;
  cmd.endpoints_ = std::move(*endpoints);

  cmd.new_topology_epoch_ = *topology_epoch;
  return cmd;
}

absl::Status WriteCommandBody(MetaWriter& w, const RetireNode& cmd) {
  LAVIK_RETURN_IF_ERROR(WriteCommandHeader(w, MetaCommandTag::kRetireNode,
                                           cmd.request_id_, cmd.actor_));
  LAVIK_RETURN_IF_ERROR(WriteNodeId(w, cmd.node_id_));
  w.WriteU64(cmd.expected_revision_);
  return absl::OkStatus();
}

absl::StatusOr<RetireNode> ReadRetireNodeBody(MetaReader& r) {
  auto header = ReadCommandHeader(r);
  LAVIK_RETURN_IF_ERROR(header);
  auto node_id = ReadNodeId(r);
  LAVIK_RETURN_IF_ERROR(node_id);
  auto expected_revision = r.ReadU64();
  LAVIK_RETURN_IF_ERROR(expected_revision);
  RetireNode cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.node_id_ = std::move(*node_id);
  cmd.expected_revision_ = *expected_revision;
  return cmd;
}

absl::Status WriteCommandBody(MetaWriter& w, const CreateGroup& cmd) {
  LAVIK_RETURN_IF_ERROR(WriteCommandHeader(w, MetaCommandTag::kCreateGroup,
                                           cmd.request_id_, cmd.actor_));
  LAVIK_RETURN_IF_ERROR(WriteGroupId(w, cmd.group_id_));
  w.WriteU64(cmd.new_topology_epoch_);
  return absl::OkStatus();
}

absl::StatusOr<CreateGroup> ReadCreateGroupBody(MetaReader& r) {
  auto header = ReadCommandHeader(r);
  LAVIK_RETURN_IF_ERROR(header);
  auto group_id = ReadGroupId(r);
  LAVIK_RETURN_IF_ERROR(group_id);
  auto topology_epoch = r.ReadU64();
  LAVIK_RETURN_IF_ERROR(topology_epoch);
  CreateGroup cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.group_id_ = std::move(*group_id);
  cmd.new_topology_epoch_ = *topology_epoch;
  return cmd;
}

absl::Status WriteCommandBody(MetaWriter& w, const AssignNodeToGroup& cmd) {
  if (std::all_of(cmd.assignment_id_.begin(), cmd.assignment_id_.end(),
                  [](std::uint8_t byte) { return byte == 0; })) {
    return MetaDomainRejectError("assignment_id must not be zero");
  }
  LAVIK_RETURN_IF_ERROR(WriteCommandHeader(
      w, MetaCommandTag::kAssignNodeToGroup, cmd.request_id_, cmd.actor_));
  LAVIK_RETURN_IF_ERROR(WriteGroupId(w, cmd.group_id_));
  LAVIK_RETURN_IF_ERROR(WriteNodeId(w, cmd.node_id_));
  WriteFixedArray(w, cmd.assignment_id_);
  LAVIK_RETURN_IF_ERROR(WriteRole(w, cmd.role_));
  w.WriteU64(cmd.expected_revision_);
  w.WriteU64(cmd.new_topology_epoch_);
  return absl::OkStatus();
}

absl::StatusOr<AssignNodeToGroup> ReadAssignNodeToGroupBody(MetaReader& r) {
  auto header = ReadCommandHeader(r);
  LAVIK_RETURN_IF_ERROR(header);
  auto group_id = ReadGroupId(r);
  LAVIK_RETURN_IF_ERROR(group_id);
  auto node_id = ReadNodeId(r);
  LAVIK_RETURN_IF_ERROR(node_id);
  auto assignment_id = ReadFixedArray<16>(r);
  LAVIK_RETURN_IF_ERROR(assignment_id);
  auto role = ReadRole(r);
  LAVIK_RETURN_IF_ERROR(role);
  auto expected_revision = r.ReadU64();
  LAVIK_RETURN_IF_ERROR(expected_revision);
  auto topology_epoch = r.ReadU64();
  LAVIK_RETURN_IF_ERROR(topology_epoch);
  AssignNodeToGroup cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.group_id_ = std::move(*group_id);
  cmd.node_id_ = std::move(*node_id);
  cmd.assignment_id_ = *assignment_id;
  cmd.role_ = *role;
  cmd.expected_revision_ = *expected_revision;
  cmd.new_topology_epoch_ = *topology_epoch;
  return cmd;
}

absl::Status WriteCommandBody(MetaWriter& w, const RemoveNodeFromGroup& cmd) {
  LAVIK_RETURN_IF_ERROR(WriteCommandHeader(
      w, MetaCommandTag::kRemoveNodeFromGroup, cmd.request_id_, cmd.actor_));
  LAVIK_RETURN_IF_ERROR(WriteGroupId(w, cmd.group_id_));
  LAVIK_RETURN_IF_ERROR(WriteNodeId(w, cmd.node_id_));
  w.WriteU64(cmd.expected_revision_);
  w.WriteU64(cmd.new_topology_epoch_);
  return absl::OkStatus();
}

absl::StatusOr<RemoveNodeFromGroup> ReadRemoveNodeFromGroupBody(MetaReader& r) {
  auto header = ReadCommandHeader(r);
  LAVIK_RETURN_IF_ERROR(header);
  auto group_id = ReadGroupId(r);
  LAVIK_RETURN_IF_ERROR(group_id);
  auto node_id = ReadNodeId(r);
  LAVIK_RETURN_IF_ERROR(node_id);
  auto expected_revision = r.ReadU64();
  LAVIK_RETURN_IF_ERROR(expected_revision);
  auto topology_epoch = r.ReadU64();
  LAVIK_RETURN_IF_ERROR(topology_epoch);
  RemoveNodeFromGroup cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.group_id_ = std::move(*group_id);
  cmd.node_id_ = std::move(*node_id);
  cmd.expected_revision_ = *expected_revision;
  cmd.new_topology_epoch_ = *topology_epoch;
  return cmd;
}

// Slot range bounds ([0, kMetaSlotCount), first <= last) are structural
// properties of the schema; coverage/overlap across ranges is domain
// validation for the apply layer.
absl::Status CheckSlotRange(const MetaSlotAssignment& range) {
  if (range.first_slot_ > range.last_slot_ ||
      range.last_slot_ >= kMetaSlotCount) {
    return MetaDomainRejectError("slot range out of bounds");
  }
  return absl::OkStatus();
}

absl::Status WriteCommandBody(MetaWriter& w, const SetSlotMap& cmd) {
  LAVIK_RETURN_IF_ERROR(
      CheckCap("ranges", cmd.ranges_.size(), kMaxMetaSlotRangeCount));
  for (const MetaSlotAssignment& range : cmd.ranges_) {
    LAVIK_RETURN_IF_ERROR(CheckSlotRange(range));
    LAVIK_RETURN_IF_ERROR(CheckCap("range group_id", range.group_id_.size(),
                                   kMaxMetaGroupIdBytes));
  }
  LAVIK_RETURN_IF_ERROR(WriteCommandHeader(w, MetaCommandTag::kSetSlotMap,
                                           cmd.request_id_, cmd.actor_));
  w.WriteList(cmd.ranges_, [](MetaWriter& ww, const MetaSlotAssignment& range) {
    ww.WriteU16(range.first_slot_);
    ww.WriteU16(range.last_slot_);
    ww.WriteString(range.group_id_);
  });
  w.WriteU64(cmd.new_topology_epoch_);
  return absl::OkStatus();
}

absl::StatusOr<MetaSlotAssignment> ReadSlotAssignment(MetaReader& r) {
  auto first = r.ReadU16();
  LAVIK_RETURN_IF_ERROR(first);
  auto last = r.ReadU16();
  LAVIK_RETURN_IF_ERROR(last);
  if (*first > *last || *last >= kMetaSlotCount) {
    return MetaFailStopError("slot range out of bounds");
  }
  auto group_id = ReadGroupId(r);
  LAVIK_RETURN_IF_ERROR(group_id);
  return MetaSlotAssignment{*first, *last, std::move(*group_id)};
}

absl::StatusOr<SetSlotMap> ReadSetSlotMapBody(MetaReader& r) {
  auto header = ReadCommandHeader(r);
  LAVIK_RETURN_IF_ERROR(header);
  auto ranges = r.ReadList<MetaSlotAssignment>(
      kMaxMetaSlotRangeCount,
      [](MetaReader& rr) { return ReadSlotAssignment(rr); });
  LAVIK_RETURN_IF_ERROR(ranges);
  auto topology_epoch = r.ReadU64();
  LAVIK_RETURN_IF_ERROR(topology_epoch);
  SetSlotMap cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.ranges_ = std::move(*ranges);
  cmd.new_topology_epoch_ = *topology_epoch;
  return cmd;
}

absl::Status WriteCommandBody(MetaWriter& w,
                              const SetGroupReplicationState& cmd) {
  LAVIK_RETURN_IF_ERROR(
      WriteCommandHeader(w, MetaCommandTag::kSetGroupReplicationState,
                         cmd.request_id_, cmd.actor_));
  LAVIK_RETURN_IF_ERROR(WriteGroupId(w, cmd.group_id_));
  w.WriteU64(cmd.expected_population_manifest_revision_);
  WriteFixedArray(w, cmd.expected_population_manifest_digest_);
  w.WriteU64(cmd.new_population_manifest_revision_);
  WriteFixedArray(w, cmd.new_population_manifest_digest_);
  w.WriteU64(cmd.expected_partition_replication_epoch_);
  w.WriteU64(cmd.new_partition_replication_epoch_);
  w.WriteU64(cmd.new_topology_epoch_);
  return absl::OkStatus();
}

absl::StatusOr<SetGroupReplicationState> ReadSetGroupReplicationStateBody(
    MetaReader& r) {
  auto header = ReadCommandHeader(r);
  LAVIK_RETURN_IF_ERROR(header);
  auto group_id = ReadGroupId(r);
  LAVIK_RETURN_IF_ERROR(group_id);
  auto expected_manifest = r.ReadU64();
  LAVIK_RETURN_IF_ERROR(expected_manifest);
  auto expected_manifest_digest = ReadFixedArray<32>(r);
  LAVIK_RETURN_IF_ERROR(expected_manifest_digest);
  auto new_manifest = r.ReadU64();
  LAVIK_RETURN_IF_ERROR(new_manifest);
  auto new_manifest_digest = ReadFixedArray<32>(r);
  LAVIK_RETURN_IF_ERROR(new_manifest_digest);
  auto expected_partition = r.ReadU64();
  LAVIK_RETURN_IF_ERROR(expected_partition);
  auto new_partition = r.ReadU64();
  LAVIK_RETURN_IF_ERROR(new_partition);
  auto topology_epoch = r.ReadU64();
  LAVIK_RETURN_IF_ERROR(topology_epoch);
  SetGroupReplicationState cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.group_id_ = std::move(*group_id);
  cmd.expected_population_manifest_revision_ = *expected_manifest;
  cmd.expected_population_manifest_digest_ = *expected_manifest_digest;
  cmd.new_population_manifest_revision_ = *new_manifest;
  cmd.new_population_manifest_digest_ = *new_manifest_digest;
  cmd.expected_partition_replication_epoch_ = *expected_partition;
  cmd.new_partition_replication_epoch_ = *new_partition;
  cmd.new_topology_epoch_ = *topology_epoch;
  return cmd;
}

template <std::size_t N>
bool IsZero(const std::array<std::uint8_t, N>& value) {
  return std::all_of(value.begin(), value.end(),
                     [](std::uint8_t byte) { return byte == 0; });
}

bool IsCanonicalNodeId(std::string_view value) {
  return value.size() == kMetaNodeIdBytes &&
         std::all_of(value.begin(), value.end(), [](unsigned char ch) {
           return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f');
         });
}

absl::Status ValidateFailoverGroupId(std::string_view group_id) {
  if (group_id.empty() || group_id.size() > kMaxMetaGroupIdBytes) {
    return MetaDomainRejectError("failover group_id is empty or over cap");
  }
  return absl::OkStatus();
}

absl::Status ValidateFailoverCandidate(const MetaFailoverCandidate& candidate) {
  if (!IsCanonicalNodeId(candidate.node_id_) ||
      IsZero(candidate.assignment_id_) || IsZero(candidate.boot_id_)) {
    return MetaDomainRejectError("invalid failover candidate identity");
  }
  return absl::OkStatus();
}

absl::Status ValidateFailoverDomain(
    const MetaFailoverCompatibilityDomain& domain) {
  if (domain.source_group_term_ == 0 ||
      !IsCanonicalNodeId(domain.source_node_id_) ||
      IsZero(domain.source_assignment_id_) || IsZero(domain.source_boot_id_) ||
      IsZero(domain.source_history_id_) || domain.flow_count_ == 0 ||
      domain.flow_count_ > kMaxMetaFailoverFlowCount) {
    return MetaDomainRejectError("invalid failover compatibility domain");
  }
  return absl::OkStatus();
}

bool IsValidFailoverLoss(MetaFailoverLoss loss) {
  return loss == MetaFailoverLoss::kNone || loss == MetaFailoverLoss::kUnknown;
}

absl::Status ValidateFailoverAuthorization(
    const MetaFailoverAuthorization& authorization) {
  if (authorization.authorized_revision_ == 0 ||
      !IsValidFailoverLoss(authorization.loss_if_cutover_)) {
    return MetaDomainRejectError("invalid failover authorization");
  }
  return absl::OkStatus();
}

absl::Status ValidateFailoverAction(const MetaFailoverCandidateAction& action) {
  if (IsZero(action.action_id_)) {
    return MetaDomainRejectError("failover action id is zero");
  }
  LAVIK_RETURN_IF_ERROR(ValidateFailoverCandidate(action.candidate_));
  if (action.operator_recovery_) {
    if (action.domain_ != MetaFailoverCompatibilityDomain{} ||
        (action.authorization_.has_value() &&
         action.authorization_->loss_if_cutover_ !=
             MetaFailoverLoss::kUnknown)) {
      return MetaDomainRejectError(
          "operator recovery cannot claim a source frontier or lossless "
          "cutover");
    }
  } else
    LAVIK_RETURN_IF_ERROR(ValidateFailoverDomain(action.domain_));
  if (action.authorization_.has_value()) {
    return ValidateFailoverAuthorization(*action.authorization_);
  }
  return absl::OkStatus();
}

absl::Status ValidateFailoverTransitionRef(
    const MetaFailoverTransitionRef& transition) {
  if (IsZero(transition.transition_id_) || transition.revision_ == 0) {
    return MetaDomainRejectError("invalid failover transition reference");
  }
  return absl::OkStatus();
}

absl::Status ValidateFailoverDeadline(std::uint64_t deadline_unix_ms) {
  if (deadline_unix_ms == 0 ||
      deadline_unix_ms > static_cast<std::uint64_t>(
                             std::numeric_limits<std::int64_t>::max())) {
    return MetaDomainRejectError("invalid failover absolute deadline");
  }
  return absl::OkStatus();
}

absl::Status ValidateFailoverTransitionImpl(
    const MetaFailoverTransition& transition) {
  if (IsZero(transition.transition_id_) || transition.revision_ == 0 ||
      transition.target_term_ == 0) {
    return MetaDomainRejectError("invalid failover transition identity");
  }
  if (transition.mode_ != MetaFailoverMode::kControlled &&
      transition.mode_ != MetaFailoverMode::kUncontrolled) {
    return MetaDomainRejectError("invalid failover transition mode");
  }
  if (transition.candidate_action_.has_value()) {
    LAVIK_RETURN_IF_ERROR(
        ValidateFailoverAction(*transition.candidate_action_));
    const MetaFailoverCandidateAction& action = *transition.candidate_action_;
    if (action.domain_.source_group_term_ >= transition.target_term_) {
      return MetaDomainRejectError(
          "failover source term must precede the target term");
    }
    if (action.authorization_.has_value() &&
        action.authorization_->authorized_revision_ > transition.revision_) {
      return MetaDomainRejectError(
          "failover authorization revision exceeds transition revision");
    }
  }

  if (transition.mode_ == MetaFailoverMode::kUncontrolled &&
      transition.candidate_action_.has_value() &&
      transition.candidate_action_->authorization_.has_value() &&
      !transition.candidate_action_->operator_recovery_ &&
      transition.candidate_action_->authorization_->loss_if_cutover_ ==
          MetaFailoverLoss::kUnknown &&
      !transition.recovery_deadline_unix_ms_.has_value()) {
    return MetaDomainRejectError(
        "uncontrolled authorization has no recovery cutoff");
  }

  if (transition.recovery_deadline_unix_ms_.has_value()) {
    if (transition.mode_ != MetaFailoverMode::kUncontrolled) {
      return MetaDomainRejectError(
          "only uncontrolled failover may collect recovery events");
    }
    LAVIK_RETURN_IF_ERROR(
        ValidateFailoverDeadline(*transition.recovery_deadline_unix_ms_));
  }

  if (transition.mode_ == MetaFailoverMode::kControlled) {
    if (!transition.controlled_.has_value() ||
        !transition.candidate_action_.has_value()) {
      return MetaDomainRejectError(
          "controlled failover requires operation and candidate action");
    }
    if (IsZero(transition.controlled_->operation_id_)) {
      return MetaDomainRejectError("controlled failover operation id is zero");
    }
    LAVIK_RETURN_IF_ERROR(ValidateFailoverDeadline(
        transition.controlled_->absolute_deadline_unix_ms_));
    const MetaFailoverCandidateAction& action = *transition.candidate_action_;
    if (action.operator_recovery_ ||
        action.candidate_.node_id_ == action.domain_.source_node_id_) {
      return MetaDomainRejectError(
          "controlled failover candidate must differ from source");
    }
    if (action.authorization_.has_value() &&
        action.authorization_->loss_if_cutover_ != MetaFailoverLoss::kNone) {
      return MetaDomainRejectError(
          "controlled failover authorization must be lossless");
    }
  } else if (transition.controlled_.has_value()) {
    return MetaDomainRejectError(
        "uncontrolled failover cannot reference a controlled operation");
  }
  return absl::OkStatus();
}

absl::Status FailStopDecodedFailover(absl::Status status) {
  if (status.ok()) return status;
  return MetaFailStopError(status.message());
}

void WriteFailoverCandidateUnchecked(MetaWriter& writer,
                                     const MetaFailoverCandidate& candidate) {
  writer.WriteString(candidate.node_id_);
  WriteFixedArray(writer, candidate.assignment_id_);
  WriteFixedArray(writer, candidate.boot_id_);
}

absl::StatusOr<MetaFailoverCandidate> ReadFailoverCandidate(
    MetaReader& reader) {
  auto node_id = ReadNodeId(reader);
  LAVIK_RETURN_IF_ERROR(node_id);
  auto assignment_id = ReadFixedArray<16>(reader);
  LAVIK_RETURN_IF_ERROR(assignment_id);
  auto boot_id = ReadFixedArray<kMetaBootIncarnationBytes>(reader);
  LAVIK_RETURN_IF_ERROR(boot_id);
  MetaFailoverCandidate candidate{std::move(*node_id), *assignment_id,
                                  *boot_id};
  LAVIK_RETURN_IF_ERROR(
      FailStopDecodedFailover(ValidateFailoverCandidate(candidate)));
  return candidate;
}

void WriteFailoverDomainUnchecked(
    MetaWriter& writer, const MetaFailoverCompatibilityDomain& domain) {
  writer.WriteU64(domain.source_group_term_);
  writer.WriteString(domain.source_node_id_);
  WriteFixedArray(writer, domain.source_assignment_id_);
  WriteFixedArray(writer, domain.source_boot_id_);
  WriteFixedArray(writer, domain.source_history_id_);
  writer.WriteU32(domain.flow_count_);
}

absl::StatusOr<MetaFailoverCompatibilityDomain> ReadFailoverDomain(
    MetaReader& reader) {
  auto source_group_term = reader.ReadU64();
  LAVIK_RETURN_IF_ERROR(source_group_term);
  auto source_node_id = ReadNodeId(reader);
  LAVIK_RETURN_IF_ERROR(source_node_id);
  auto source_assignment_id = ReadFixedArray<16>(reader);
  LAVIK_RETURN_IF_ERROR(source_assignment_id);
  auto source_boot_id = ReadFixedArray<kMetaBootIncarnationBytes>(reader);
  LAVIK_RETURN_IF_ERROR(source_boot_id);
  auto source_history_id =
      ReadFixedArray<kMetaReplicationHistoryIdBytes>(reader);
  LAVIK_RETURN_IF_ERROR(source_history_id);
  auto flow_count = reader.ReadU32();
  LAVIK_RETURN_IF_ERROR(flow_count);
  MetaFailoverCompatibilityDomain domain{
      *source_group_term, std::move(*source_node_id), *source_assignment_id,
      *source_boot_id,    *source_history_id,         *flow_count};
  LAVIK_RETURN_IF_ERROR(
      FailStopDecodedFailover(ValidateFailoverDomain(domain)));
  return domain;
}

void WriteFailoverAuthorizationUnchecked(
    MetaWriter& writer, const MetaFailoverAuthorization& authorization) {
  writer.WriteU64(authorization.authorized_revision_);
  writer.WriteU8(static_cast<std::uint8_t>(authorization.loss_if_cutover_));
}

absl::StatusOr<MetaFailoverAuthorization> ReadFailoverAuthorization(
    MetaReader& reader) {
  auto revision = reader.ReadU64();
  LAVIK_RETURN_IF_ERROR(revision);
  auto loss = reader.ReadU8();
  LAVIK_RETURN_IF_ERROR(loss);
  MetaFailoverAuthorization authorization{*revision,
                                          static_cast<MetaFailoverLoss>(*loss)};
  LAVIK_RETURN_IF_ERROR(
      FailStopDecodedFailover(ValidateFailoverAuthorization(authorization)));
  return authorization;
}

void WriteFailoverActionUnchecked(MetaWriter& writer,
                                  const MetaFailoverCandidateAction& action) {
  WriteFixedArray(writer, action.action_id_);
  WriteFailoverCandidateUnchecked(writer, action.candidate_);
  writer.WriteU8(action.operator_recovery_ ? 1 : 0);
  if (!action.operator_recovery_)
    WriteFailoverDomainUnchecked(writer, action.domain_);
  writer.WriteOptional(
      action.authorization_,
      [](MetaWriter& nested, const MetaFailoverAuthorization& authorization) {
        WriteFailoverAuthorizationUnchecked(nested, authorization);
      });
}

absl::StatusOr<MetaFailoverCandidateAction> ReadFailoverAction(
    MetaReader& reader) {
  auto action_id = ReadFixedArray<16>(reader);
  LAVIK_RETURN_IF_ERROR(action_id);
  auto candidate = ReadFailoverCandidate(reader);
  LAVIK_RETURN_IF_ERROR(candidate);
  auto operator_recovery = reader.ReadU8();
  LAVIK_RETURN_IF_ERROR(operator_recovery);
  if (*operator_recovery > 1)
    return FailStopDecodedFailover(
        MetaDomainRejectError("invalid operator recovery flag"));
  absl::StatusOr<MetaFailoverCompatibilityDomain> domain =
      MetaFailoverCompatibilityDomain{};
  if (*operator_recovery == 0) domain = ReadFailoverDomain(reader);
  LAVIK_RETURN_IF_ERROR(domain);
  auto authorization = reader.ReadOptional<MetaFailoverAuthorization>(
      [](MetaReader& nested) { return ReadFailoverAuthorization(nested); });
  LAVIK_RETURN_IF_ERROR(authorization);
  MetaFailoverCandidateAction action{
      *action_id, std::move(*candidate), std::move(*domain),
      std::move(*authorization), *operator_recovery != 0};
  LAVIK_RETURN_IF_ERROR(
      FailStopDecodedFailover(ValidateFailoverAction(action)));
  return action;
}

void WriteFailoverTransitionRefUnchecked(
    MetaWriter& writer, const MetaFailoverTransitionRef& transition) {
  WriteFixedArray(writer, transition.transition_id_);
  writer.WriteU64(transition.revision_);
}

absl::StatusOr<MetaFailoverTransitionRef> ReadFailoverTransitionRef(
    MetaReader& reader) {
  auto transition_id = ReadFixedArray<16>(reader);
  LAVIK_RETURN_IF_ERROR(transition_id);
  auto revision = reader.ReadU64();
  LAVIK_RETURN_IF_ERROR(revision);
  MetaFailoverTransitionRef transition{*transition_id, *revision};
  LAVIK_RETURN_IF_ERROR(
      FailStopDecodedFailover(ValidateFailoverTransitionRef(transition)));
  return transition;
}

void WriteFailoverTransitionUnchecked(
    MetaWriter& writer, const MetaFailoverTransition& transition) {
  WriteFixedArray(writer, transition.transition_id_);
  writer.WriteU64(transition.revision_);
  writer.WriteU8(static_cast<std::uint8_t>(transition.mode_));
  writer.WriteU64(transition.target_term_);
  writer.WriteOptional(
      transition.candidate_action_,
      [](MetaWriter& nested, const MetaFailoverCandidateAction& action) {
        WriteFailoverActionUnchecked(nested, action);
      });
  writer.WriteOptional(
      transition.controlled_,
      [](MetaWriter& nested, const MetaControlledFailover& controlled) {
        WriteFixedArray(nested, controlled.operation_id_);
        nested.WriteU64(controlled.absolute_deadline_unix_ms_);
      });
  writer.WriteOptional(transition.recovery_deadline_unix_ms_,
                       [](MetaWriter& nested, std::uint64_t deadline) {
                         nested.WriteU64(deadline);
                       });
}

absl::StatusOr<MetaFailoverTransition> ReadFailoverTransitionUnchecked(
    MetaReader& reader) {
  auto transition_id = ReadFixedArray<16>(reader);
  LAVIK_RETURN_IF_ERROR(transition_id);
  auto revision = reader.ReadU64();
  LAVIK_RETURN_IF_ERROR(revision);
  auto mode = reader.ReadU8();
  LAVIK_RETURN_IF_ERROR(mode);
  auto target_term = reader.ReadU64();
  LAVIK_RETURN_IF_ERROR(target_term);
  auto action = reader.ReadOptional<MetaFailoverCandidateAction>(
      [](MetaReader& nested) { return ReadFailoverAction(nested); });
  LAVIK_RETURN_IF_ERROR(action);
  auto controlled = reader.ReadOptional<MetaControlledFailover>(
      [](MetaReader& nested) -> absl::StatusOr<MetaControlledFailover> {
        auto operation_id = ReadFixedArray<16>(nested);
        LAVIK_RETURN_IF_ERROR(operation_id);
        auto deadline = nested.ReadU64();
        LAVIK_RETURN_IF_ERROR(deadline);
        return MetaControlledFailover{*operation_id, *deadline};
      });
  LAVIK_RETURN_IF_ERROR(controlled);
  auto recovery_deadline = reader.ReadOptional<std::uint64_t>(
      [](MetaReader& nested) { return nested.ReadU64(); });
  LAVIK_RETURN_IF_ERROR(recovery_deadline);
  return MetaFailoverTransition{
      *transition_id,
      *revision,
      static_cast<MetaFailoverMode>(*mode),
      *target_term,
      std::move(*action),
      std::move(*controlled),
      *recovery_deadline,
  };
}

template <typename Command>
absl::Status ValidateFailoverGroupAnchors(const Command& command) {
  const bool manifest_digest_is_zero =
      IsZero(command.expected_population_manifest_digest_);
  if (!IsCanonicalNodeId(command.expected_owner_node_id_) ||
      IsZero(command.expected_owner_assignment_id_) ||
      command.expected_membership_revision_ == 0 ||
      command.expected_group_term_ == 0 ||
      ((command.expected_population_manifest_revision_ == 0) !=
       manifest_digest_is_zero)) {
    return MetaDomainRejectError("invalid failover group anchors");
  }
  return absl::OkStatus();
}

template <typename Command>
void WriteFailoverGroupAnchorsUnchecked(MetaWriter& writer,
                                        const Command& command) {
  writer.WriteString(command.expected_owner_node_id_);
  WriteFixedArray(writer, command.expected_owner_assignment_id_);
  writer.WriteU64(command.expected_membership_revision_);
  writer.WriteU64(command.expected_group_term_);
  writer.WriteU64(command.expected_population_manifest_revision_);
  WriteFixedArray(writer, command.expected_population_manifest_digest_);
  writer.WriteU64(command.expected_partition_replication_epoch_);
}

template <typename Command>
absl::Status ReadFailoverGroupAnchors(MetaReader& reader, Command& command) {
  auto owner_node_id = ReadNodeId(reader);
  LAVIK_RETURN_IF_ERROR(owner_node_id);
  auto owner_assignment_id = ReadFixedArray<16>(reader);
  LAVIK_RETURN_IF_ERROR(owner_assignment_id);
  auto membership_revision = reader.ReadU64();
  LAVIK_RETURN_IF_ERROR(membership_revision);
  auto group_term = reader.ReadU64();
  LAVIK_RETURN_IF_ERROR(group_term);
  auto manifest_revision = reader.ReadU64();
  LAVIK_RETURN_IF_ERROR(manifest_revision);
  auto manifest_digest = ReadFixedArray<32>(reader);
  LAVIK_RETURN_IF_ERROR(manifest_digest);
  auto partition_epoch = reader.ReadU64();
  LAVIK_RETURN_IF_ERROR(partition_epoch);

  command.expected_owner_node_id_ = std::move(*owner_node_id);
  command.expected_owner_assignment_id_ = *owner_assignment_id;
  command.expected_membership_revision_ = *membership_revision;
  command.expected_group_term_ = *group_term;
  command.expected_population_manifest_revision_ = *manifest_revision;
  command.expected_population_manifest_digest_ = *manifest_digest;
  command.expected_partition_replication_epoch_ = *partition_epoch;
  return absl::OkStatus();
}

absl::Status ValidateFailoverOperation(
    const MetaOperationId& operation_id,
    std::uint64_t expected_operation_revision) {
  if (IsZero(operation_id) || expected_operation_revision ==
                                  std::numeric_limits<std::uint64_t>::max()) {
    return MetaDomainRejectError("invalid controlled failover operation CAS");
  }
  return absl::OkStatus();
}

absl::Status ValidateFailoverReason(std::string_view reason) {
  if (reason.empty() || reason.size() > kMaxMetaAbortReasonBytes) {
    return MetaDomainRejectError("failover reason is empty or over cap");
  }
  return absl::OkStatus();
}

template <typename Command>
absl::Status ValidateFailoverBeginBase(const Command& command) {
  LAVIK_RETURN_IF_ERROR(ValidateFailoverGroupId(command.group_id_));
  if (IsZero(command.transition_id_)) {
    return MetaDomainRejectError("failover transition id is zero");
  }
  LAVIK_RETURN_IF_ERROR(ValidateFailoverGroupAnchors(command));
  if (command.expected_group_term_ ==
          std::numeric_limits<std::uint64_t>::max() ||
      command.target_term_ != command.expected_group_term_ + 1) {
    return MetaDomainRejectError(
        "failover target term must be exactly expected group term plus one");
  }
  return absl::OkStatus();
}

absl::Status ValidateCommand(const BeginControlledFailover& command) {
  LAVIK_RETURN_IF_ERROR(ValidateFailoverBeginBase(command));
  LAVIK_RETURN_IF_ERROR(ValidateFailoverAction(command.candidate_action_));
  if (command.candidate_action_.authorization_.has_value()) {
    return MetaDomainRejectError(
        "controlled failover begin action must be unauthorized");
  }
  if (command.candidate_action_.candidate_.node_id_ ==
      command.expected_owner_node_id_) {
    return MetaDomainRejectError(
        "controlled failover candidate must differ from owner");
  }
  if (command.candidate_action_.domain_.source_group_term_ !=
          command.expected_group_term_ ||
      command.candidate_action_.domain_.source_node_id_ !=
          command.expected_owner_node_id_ ||
      command.candidate_action_.domain_.source_assignment_id_ !=
          command.expected_owner_assignment_id_) {
    return MetaDomainRejectError(
        "controlled failover domain does not match the owner anchor");
  }
  LAVIK_RETURN_IF_ERROR(ValidateFailoverOperation(
      command.operation_id_, command.expected_operation_revision_));
  return ValidateFailoverDeadline(command.absolute_deadline_unix_ms_);
}

absl::Status ValidateCommand(const BeginUncontrolledFailover& command) {
  LAVIK_RETURN_IF_ERROR(ValidateFailoverBeginBase(command));
  if (static_cast<std::uint8_t>(command.trigger_reason_) >
      static_cast<std::uint8_t>(
          MetaAutomaticFailoverReason::kPopulationUnready)) {
    return MetaDomainRejectError("unknown automatic failover trigger reason");
  }
  const bool automatic =
      command.trigger_reason_ != MetaAutomaticFailoverReason::kManual;
  if (command.candidate_action_.has_value()) {
    if (automatic) {
      return MetaDomainRejectError(
          "automatic uncontrolled failover begin must be candidate-less");
    }
    LAVIK_RETURN_IF_ERROR(ValidateFailoverAction(*command.candidate_action_));
    if (command.candidate_action_->authorization_.has_value()) {
      return MetaDomainRejectError(
          "uncontrolled failover begin action must be unauthorized");
    }
    if (command.candidate_action_->domain_.source_group_term_ >=
        command.target_term_) {
      return MetaDomainRejectError(
          "failover source term must precede the target term");
    }
  }
  if (automatic != (command.suspect_duration_ms_ != 0)) {
    return MetaDomainRejectError(
        "automatic failover reason and suspect duration must be paired");
  }
  const bool has_preempted_operation =
      command.preempted_operation_id_.has_value();
  if (has_preempted_operation !=
      command.expected_preempted_operation_revision_.has_value()) {
    return MetaDomainRejectError(
        "preempted operation id and expected revision must be paired");
  }
  if (has_preempted_operation) {
    if (!automatic) {
      return MetaDomainRejectError(
          "only automatic failover may preempt a controlled operation");
    }
    LAVIK_RETURN_IF_ERROR(ValidateFailoverOperation(
        *command.preempted_operation_id_,
        *command.expected_preempted_operation_revision_));
  }
  return absl::OkStatus();
}

absl::Status ValidateCommand(const SetUncontrolledCandidate& command) {
  LAVIK_RETURN_IF_ERROR(ValidateFailoverGroupId(command.group_id_));
  LAVIK_RETURN_IF_ERROR(
      ValidateFailoverTransitionRef(command.expected_transition_));
  if (command.candidate_action_.has_value()) {
    LAVIK_RETURN_IF_ERROR(ValidateFailoverAction(*command.candidate_action_));
    if (command.candidate_action_->authorization_.has_value()) {
      return MetaDomainRejectError(
          "candidate replacement must not carry authorization");
    }
  }
  return absl::OkStatus();
}

absl::Status ValidateCommand(const StartCandidateRecovery& command) {
  LAVIK_RETURN_IF_ERROR(ValidateFailoverGroupId(command.group_id_));
  LAVIK_RETURN_IF_ERROR(
      ValidateFailoverTransitionRef(command.expected_transition_));
  if (IsZero(command.action_id_))
    return MetaDomainRejectError("recovery action id is zero");
  return ValidateFailoverDeadline(command.recovery_deadline_unix_ms_);
}

absl::Status ValidateCommand(const AuthorizeFailoverPrepare& command) {
  LAVIK_RETURN_IF_ERROR(ValidateFailoverGroupId(command.group_id_));
  LAVIK_RETURN_IF_ERROR(
      ValidateFailoverTransitionRef(command.expected_transition_));
  if (IsZero(command.action_id_) ||
      !IsValidFailoverLoss(command.loss_if_cutover_)) {
    return MetaDomainRejectError("invalid failover authorization command");
  }
  return absl::OkStatus();
}

absl::Status ValidateCommand(const AbortControlledFailover& command) {
  LAVIK_RETURN_IF_ERROR(ValidateFailoverOperation(
      command.operation_id_, command.expected_operation_revision_));
  LAVIK_RETURN_IF_ERROR(ValidateFailoverGroupId(command.group_id_));
  if (command.expected_transition_.has_value()) {
    LAVIK_RETURN_IF_ERROR(
        ValidateFailoverTransitionRef(*command.expected_transition_));
  }
  return ValidateFailoverReason(command.reason_);
}

absl::Status ValidateCommand(const DegradeControlledFailover& command) {
  LAVIK_RETURN_IF_ERROR(ValidateFailoverOperation(
      command.operation_id_, command.expected_operation_revision_));
  LAVIK_RETURN_IF_ERROR(ValidateFailoverGroupId(command.group_id_));
  LAVIK_RETURN_IF_ERROR(
      ValidateFailoverTransitionRef(command.expected_transition_));
  if (command.expected_candidate_action_.has_value()) {
    const MetaFailoverCandidateAction& action =
        *command.expected_candidate_action_;
    LAVIK_RETURN_IF_ERROR(ValidateFailoverAction(action));
    if (action.candidate_.node_id_ == action.domain_.source_node_id_ ||
        (action.authorization_.has_value() &&
         action.authorization_->loss_if_cutover_ != MetaFailoverLoss::kNone) ||
        (action.authorization_.has_value() &&
         action.authorization_->authorized_revision_ >
             command.expected_transition_.revision_)) {
      return MetaDomainRejectError(
          "invalid controlled failover action snapshot");
    }
  }
  if (command.retain_candidate_action_ &&
      (!command.expected_candidate_action_.has_value() ||
       !command.expected_candidate_action_->authorization_.has_value() ||
       command.expected_candidate_action_->authorization_->loss_if_cutover_ !=
           MetaFailoverLoss::kNone)) {
    return MetaDomainRejectError(
        "retained candidate must have lossless authorization");
  }
  return ValidateFailoverReason(command.reason_);
}

template <typename Command>
absl::Status ValidateFailoverCommitBase(const Command& command) {
  LAVIK_RETURN_IF_ERROR(ValidateFailoverGroupId(command.group_id_));
  LAVIK_RETURN_IF_ERROR(
      ValidateFailoverTransitionRef(command.expected_transition_));
  if (IsZero(command.action_id_) || command.authorized_revision_ == 0 ||
      command.authorized_revision_ > command.expected_transition_.revision_) {
    return MetaDomainRejectError("invalid failover commit authorization CAS");
  }
  LAVIK_RETURN_IF_ERROR(ValidateFailoverCandidate(command.expected_candidate_));
  LAVIK_RETURN_IF_ERROR(ValidateFailoverGroupAnchors(command));
  if (command.new_topology_epoch_ == 0) {
    return MetaDomainRejectError("invalid failover cutover topology epoch");
  }
  return absl::OkStatus();
}

absl::Status ValidateCommand(const CommitControlledFailover& command) {
  LAVIK_RETURN_IF_ERROR(ValidateFailoverCommitBase(command));
  LAVIK_RETURN_IF_ERROR(ValidateFailoverOperation(
      command.operation_id_, command.expected_operation_revision_));
  if (command.expected_candidate_.node_id_ == command.expected_owner_node_id_) {
    return MetaDomainRejectError(
        "controlled failover candidate must differ from owner");
  }
  if (command.expected_group_term_ ==
      std::numeric_limits<std::uint64_t>::max()) {
    return MetaDomainRejectError("controlled failover group term overflow");
  }
  return absl::OkStatus();
}

absl::Status ValidateCommand(const CommitUncontrolledFailover& command) {
  LAVIK_RETURN_IF_ERROR(ValidateFailoverCommitBase(command));
  if (!IsValidFailoverLoss(command.loss_if_cutover_)) {
    return MetaDomainRejectError("invalid uncontrolled failover loss result");
  }
  return absl::OkStatus();
}

template <typename Command>
void WriteFailoverCommitBaseUnchecked(MetaWriter& writer,
                                      const Command& command) {
  writer.WriteString(command.group_id_);
  WriteFailoverTransitionRefUnchecked(writer, command.expected_transition_);
  WriteFixedArray(writer, command.action_id_);
  writer.WriteU64(command.authorized_revision_);
  WriteFailoverCandidateUnchecked(writer, command.expected_candidate_);
  WriteFailoverGroupAnchorsUnchecked(writer, command);
  writer.WriteU64(command.new_topology_epoch_);
}

template <typename Command>
absl::Status ReadFailoverCommitBase(MetaReader& reader, Command& command) {
  auto group_id = ReadGroupId(reader);
  LAVIK_RETURN_IF_ERROR(group_id);
  auto transition = ReadFailoverTransitionRef(reader);
  LAVIK_RETURN_IF_ERROR(transition);
  auto action_id = ReadFixedArray<16>(reader);
  LAVIK_RETURN_IF_ERROR(action_id);
  auto authorized_revision = reader.ReadU64();
  LAVIK_RETURN_IF_ERROR(authorized_revision);
  auto candidate = ReadFailoverCandidate(reader);
  LAVIK_RETURN_IF_ERROR(candidate);
  command.group_id_ = std::move(*group_id);
  command.expected_transition_ = *transition;
  command.action_id_ = *action_id;
  command.authorized_revision_ = *authorized_revision;
  command.expected_candidate_ = std::move(*candidate);
  LAVIK_RETURN_IF_ERROR(ReadFailoverGroupAnchors(reader, command));
  LAVIK_ASSIGN_OR_RETURN(command.new_topology_epoch_, reader.ReadU64());
  return absl::OkStatus();
}

absl::Status WriteCommandBody(MetaWriter& writer,
                              const BeginControlledFailover& command) {
  LAVIK_RETURN_IF_ERROR(ValidateCommand(command));
  LAVIK_RETURN_IF_ERROR(
      WriteCommandHeader(writer, MetaCommandTag::kBeginControlledFailover,
                         command.request_id_, command.actor_));
  writer.WriteString(command.group_id_);
  WriteFixedArray(writer, command.transition_id_);
  writer.WriteU64(command.target_term_);
  WriteFailoverActionUnchecked(writer, command.candidate_action_);
  WriteFixedArray(writer, command.operation_id_);
  writer.WriteU64(command.expected_operation_revision_);
  writer.WriteU64(command.absolute_deadline_unix_ms_);
  WriteFailoverGroupAnchorsUnchecked(writer, command);
  return absl::OkStatus();
}

absl::StatusOr<BeginControlledFailover> ReadBeginControlledFailoverBody(
    MetaReader& reader) {
  auto header = ReadCommandHeader(reader);
  LAVIK_RETURN_IF_ERROR(header);
  auto group_id = ReadGroupId(reader);
  LAVIK_RETURN_IF_ERROR(group_id);
  auto transition_id = ReadFixedArray<16>(reader);
  LAVIK_RETURN_IF_ERROR(transition_id);
  auto target_term = reader.ReadU64();
  LAVIK_RETURN_IF_ERROR(target_term);
  auto action = ReadFailoverAction(reader);
  LAVIK_RETURN_IF_ERROR(action);
  auto operation_id = ReadFixedArray<16>(reader);
  LAVIK_RETURN_IF_ERROR(operation_id);
  auto operation_revision = reader.ReadU64();
  LAVIK_RETURN_IF_ERROR(operation_revision);
  auto deadline = reader.ReadU64();
  LAVIK_RETURN_IF_ERROR(deadline);
  BeginControlledFailover command;
  command.request_id_ = header->request_id_;
  command.actor_ = std::move(header->actor_);
  command.group_id_ = std::move(*group_id);
  command.transition_id_ = *transition_id;
  command.target_term_ = *target_term;
  command.candidate_action_ = std::move(*action);
  command.operation_id_ = *operation_id;
  command.expected_operation_revision_ = *operation_revision;
  command.absolute_deadline_unix_ms_ = *deadline;
  LAVIK_RETURN_IF_ERROR(ReadFailoverGroupAnchors(reader, command));
  LAVIK_RETURN_IF_ERROR(FailStopDecodedFailover(ValidateCommand(command)));
  return command;
}

absl::Status WriteCommandBody(MetaWriter& writer,
                              const BeginUncontrolledFailover& command) {
  LAVIK_RETURN_IF_ERROR(ValidateCommand(command));
  LAVIK_RETURN_IF_ERROR(
      WriteCommandHeader(writer, MetaCommandTag::kBeginUncontrolledFailover,
                         command.request_id_, command.actor_));
  writer.WriteString(command.group_id_);
  WriteFixedArray(writer, command.transition_id_);
  writer.WriteU64(command.target_term_);
  writer.WriteOptional(
      command.candidate_action_,
      [](MetaWriter& nested, const MetaFailoverCandidateAction& action) {
        WriteFailoverActionUnchecked(nested, action);
      });
  writer.WriteU8(static_cast<std::uint8_t>(command.trigger_reason_));
  writer.WriteU64(command.suspect_duration_ms_);
  writer.WriteOptional(
      command.preempted_operation_id_,
      [&command](MetaWriter& nested, const MetaOperationId& operation_id) {
        WriteFixedArray(nested, operation_id);
        nested.WriteU64(*command.expected_preempted_operation_revision_);
      });
  WriteFailoverGroupAnchorsUnchecked(writer, command);
  return absl::OkStatus();
}

absl::StatusOr<BeginUncontrolledFailover> ReadBeginUncontrolledFailoverBody(
    MetaReader& reader) {
  auto header = ReadCommandHeader(reader);
  LAVIK_RETURN_IF_ERROR(header);
  auto group_id = ReadGroupId(reader);
  LAVIK_RETURN_IF_ERROR(group_id);
  auto transition_id = ReadFixedArray<16>(reader);
  LAVIK_RETURN_IF_ERROR(transition_id);
  auto target_term = reader.ReadU64();
  LAVIK_RETURN_IF_ERROR(target_term);
  auto action = reader.ReadOptional<MetaFailoverCandidateAction>(
      [](MetaReader& nested) { return ReadFailoverAction(nested); });
  LAVIK_RETURN_IF_ERROR(action);
  auto trigger_reason = reader.ReadU8();
  LAVIK_RETURN_IF_ERROR(trigger_reason);
  auto suspect_duration_ms = reader.ReadU64();
  LAVIK_RETURN_IF_ERROR(suspect_duration_ms);
  using PreemptedOperation = std::pair<MetaOperationId, std::uint64_t>;
  auto preempted_operation = reader.ReadOptional<PreemptedOperation>(
      [](MetaReader& nested) -> absl::StatusOr<PreemptedOperation> {
        auto operation_id = ReadFixedArray<16>(nested);
        LAVIK_RETURN_IF_ERROR(operation_id);
        auto expected_revision = nested.ReadU64();
        LAVIK_RETURN_IF_ERROR(expected_revision);
        return PreemptedOperation{*operation_id, *expected_revision};
      });
  LAVIK_RETURN_IF_ERROR(preempted_operation);
  BeginUncontrolledFailover command;
  command.request_id_ = header->request_id_;
  command.actor_ = std::move(header->actor_);
  command.group_id_ = std::move(*group_id);
  command.transition_id_ = *transition_id;
  command.target_term_ = *target_term;
  command.candidate_action_ = std::move(*action);
  command.trigger_reason_ =
      static_cast<MetaAutomaticFailoverReason>(*trigger_reason);
  command.suspect_duration_ms_ = *suspect_duration_ms;
  if (preempted_operation->has_value()) {
    command.preempted_operation_id_ = (*preempted_operation)->first;
    command.expected_preempted_operation_revision_ =
        (*preempted_operation)->second;
  }
  LAVIK_RETURN_IF_ERROR(ReadFailoverGroupAnchors(reader, command));
  LAVIK_RETURN_IF_ERROR(FailStopDecodedFailover(ValidateCommand(command)));
  return command;
}

absl::Status WriteCommandBody(MetaWriter& writer,
                              const SetUncontrolledCandidate& command) {
  LAVIK_RETURN_IF_ERROR(ValidateCommand(command));
  LAVIK_RETURN_IF_ERROR(
      WriteCommandHeader(writer, MetaCommandTag::kSetUncontrolledCandidate,
                         command.request_id_, command.actor_));
  writer.WriteString(command.group_id_);
  WriteFailoverTransitionRefUnchecked(writer, command.expected_transition_);
  writer.WriteOptional(
      command.candidate_action_,
      [](MetaWriter& nested, const MetaFailoverCandidateAction& action) {
        WriteFailoverActionUnchecked(nested, action);
      });
  return absl::OkStatus();
}

absl::StatusOr<SetUncontrolledCandidate> ReadSetUncontrolledCandidateBody(
    MetaReader& reader) {
  auto header = ReadCommandHeader(reader);
  LAVIK_RETURN_IF_ERROR(header);
  auto group_id = ReadGroupId(reader);
  LAVIK_RETURN_IF_ERROR(group_id);
  auto transition = ReadFailoverTransitionRef(reader);
  LAVIK_RETURN_IF_ERROR(transition);
  auto action = reader.ReadOptional<MetaFailoverCandidateAction>(
      [](MetaReader& nested) { return ReadFailoverAction(nested); });
  LAVIK_RETURN_IF_ERROR(action);
  SetUncontrolledCandidate command;
  command.request_id_ = header->request_id_;
  command.actor_ = std::move(header->actor_);
  command.group_id_ = std::move(*group_id);
  command.expected_transition_ = *transition;
  command.candidate_action_ = std::move(*action);
  LAVIK_RETURN_IF_ERROR(FailStopDecodedFailover(ValidateCommand(command)));
  return command;
}

absl::Status WriteCommandBody(MetaWriter& writer,
                              const StartCandidateRecovery& command) {
  LAVIK_RETURN_IF_ERROR(ValidateCommand(command));
  LAVIK_RETURN_IF_ERROR(
      WriteCommandHeader(writer, MetaCommandTag::kStartCandidateRecovery,
                         command.request_id_, command.actor_));
  writer.WriteString(command.group_id_);
  WriteFailoverTransitionRefUnchecked(writer, command.expected_transition_);
  WriteFixedArray(writer, command.action_id_);
  writer.WriteU64(command.recovery_deadline_unix_ms_);
  return absl::OkStatus();
}

absl::StatusOr<StartCandidateRecovery> ReadStartCandidateRecoveryBody(
    MetaReader& reader) {
  auto header = ReadCommandHeader(reader);
  LAVIK_RETURN_IF_ERROR(header);
  auto group = ReadGroupId(reader);
  LAVIK_RETURN_IF_ERROR(group);
  auto transition = ReadFailoverTransitionRef(reader);
  LAVIK_RETURN_IF_ERROR(transition);
  auto action = ReadFixedArray<16>(reader);
  LAVIK_RETURN_IF_ERROR(action);
  auto deadline = reader.ReadU64();
  LAVIK_RETURN_IF_ERROR(deadline);
  StartCandidateRecovery command{header->request_id_,
                                 std::move(header->actor_),
                                 std::move(*group),
                                 *transition,
                                 *action,
                                 *deadline};
  LAVIK_RETURN_IF_ERROR(FailStopDecodedFailover(ValidateCommand(command)));
  return command;
}

absl::Status WriteCommandBody(MetaWriter& writer,
                              const AuthorizeFailoverPrepare& command) {
  LAVIK_RETURN_IF_ERROR(ValidateCommand(command));
  LAVIK_RETURN_IF_ERROR(
      WriteCommandHeader(writer, MetaCommandTag::kAuthorizeFailoverPrepare,
                         command.request_id_, command.actor_));
  writer.WriteString(command.group_id_);
  WriteFailoverTransitionRefUnchecked(writer, command.expected_transition_);
  WriteFixedArray(writer, command.action_id_);
  writer.WriteU8(static_cast<std::uint8_t>(command.loss_if_cutover_));
  return absl::OkStatus();
}

absl::StatusOr<AuthorizeFailoverPrepare> ReadAuthorizeFailoverPrepareBody(
    MetaReader& reader) {
  auto header = ReadCommandHeader(reader);
  LAVIK_RETURN_IF_ERROR(header);
  auto group_id = ReadGroupId(reader);
  LAVIK_RETURN_IF_ERROR(group_id);
  auto transition = ReadFailoverTransitionRef(reader);
  LAVIK_RETURN_IF_ERROR(transition);
  auto action_id = ReadFixedArray<16>(reader);
  LAVIK_RETURN_IF_ERROR(action_id);
  auto loss = reader.ReadU8();
  LAVIK_RETURN_IF_ERROR(loss);
  AuthorizeFailoverPrepare command;
  command.request_id_ = header->request_id_;
  command.actor_ = std::move(header->actor_);
  command.group_id_ = std::move(*group_id);
  command.expected_transition_ = *transition;
  command.action_id_ = *action_id;
  command.loss_if_cutover_ = static_cast<MetaFailoverLoss>(*loss);
  LAVIK_RETURN_IF_ERROR(FailStopDecodedFailover(ValidateCommand(command)));
  return command;
}

absl::Status WriteCommandBody(MetaWriter& writer,
                              const AbortControlledFailover& command) {
  LAVIK_RETURN_IF_ERROR(ValidateCommand(command));
  LAVIK_RETURN_IF_ERROR(
      WriteCommandHeader(writer, MetaCommandTag::kAbortControlledFailover,
                         command.request_id_, command.actor_));
  WriteFixedArray(writer, command.operation_id_);
  writer.WriteU64(command.expected_operation_revision_);
  writer.WriteString(command.group_id_);
  writer.WriteOptional(
      command.expected_transition_,
      [](MetaWriter& nested, const MetaFailoverTransitionRef& transition) {
        WriteFailoverTransitionRefUnchecked(nested, transition);
      });
  writer.WriteString(command.reason_);
  return absl::OkStatus();
}

absl::StatusOr<AbortControlledFailover> ReadAbortControlledFailoverBody(
    MetaReader& reader) {
  auto header = ReadCommandHeader(reader);
  LAVIK_RETURN_IF_ERROR(header);
  auto operation_id = ReadFixedArray<16>(reader);
  LAVIK_RETURN_IF_ERROR(operation_id);
  auto operation_revision = reader.ReadU64();
  LAVIK_RETURN_IF_ERROR(operation_revision);
  auto group_id = ReadGroupId(reader);
  LAVIK_RETURN_IF_ERROR(group_id);
  auto transition = reader.ReadOptional<MetaFailoverTransitionRef>(
      [](MetaReader& nested) { return ReadFailoverTransitionRef(nested); });
  LAVIK_RETURN_IF_ERROR(transition);
  auto reason = ReadBoundedString(reader, kMaxMetaAbortReasonBytes);
  LAVIK_RETURN_IF_ERROR(reason);
  AbortControlledFailover command;
  command.request_id_ = header->request_id_;
  command.actor_ = std::move(header->actor_);
  command.operation_id_ = *operation_id;
  command.expected_operation_revision_ = *operation_revision;
  command.group_id_ = std::move(*group_id);
  command.expected_transition_ = std::move(*transition);
  command.reason_ = std::move(*reason);
  LAVIK_RETURN_IF_ERROR(FailStopDecodedFailover(ValidateCommand(command)));
  return command;
}

absl::Status WriteCommandBody(MetaWriter& writer,
                              const DegradeControlledFailover& command) {
  LAVIK_RETURN_IF_ERROR(ValidateCommand(command));
  LAVIK_RETURN_IF_ERROR(
      WriteCommandHeader(writer, MetaCommandTag::kDegradeControlledFailover,
                         command.request_id_, command.actor_));
  WriteFixedArray(writer, command.operation_id_);
  writer.WriteU64(command.expected_operation_revision_);
  writer.WriteString(command.group_id_);
  WriteFailoverTransitionRefUnchecked(writer, command.expected_transition_);
  writer.WriteOptional(
      command.expected_candidate_action_,
      [](MetaWriter& nested, const MetaFailoverCandidateAction& action) {
        WriteFailoverActionUnchecked(nested, action);
      });
  writer.WriteBool(command.retain_candidate_action_);
  writer.WriteString(command.reason_);
  return absl::OkStatus();
}

absl::StatusOr<DegradeControlledFailover> ReadDegradeControlledFailoverBody(
    MetaReader& reader) {
  auto header = ReadCommandHeader(reader);
  LAVIK_RETURN_IF_ERROR(header);
  auto operation_id = ReadFixedArray<16>(reader);
  LAVIK_RETURN_IF_ERROR(operation_id);
  auto operation_revision = reader.ReadU64();
  LAVIK_RETURN_IF_ERROR(operation_revision);
  auto group_id = ReadGroupId(reader);
  LAVIK_RETURN_IF_ERROR(group_id);
  auto transition = ReadFailoverTransitionRef(reader);
  LAVIK_RETURN_IF_ERROR(transition);
  auto action = reader.ReadOptional<MetaFailoverCandidateAction>(
      [](MetaReader& nested) { return ReadFailoverAction(nested); });
  LAVIK_RETURN_IF_ERROR(action);
  auto retain =
      reader.ReadBool("invalid retained failover candidate presence tag");
  LAVIK_RETURN_IF_ERROR(retain);
  auto reason = ReadBoundedString(reader, kMaxMetaAbortReasonBytes);
  LAVIK_RETURN_IF_ERROR(reason);
  DegradeControlledFailover command;
  command.request_id_ = header->request_id_;
  command.actor_ = std::move(header->actor_);
  command.operation_id_ = *operation_id;
  command.expected_operation_revision_ = *operation_revision;
  command.group_id_ = std::move(*group_id);
  command.expected_transition_ = *transition;
  command.expected_candidate_action_ = std::move(*action);
  command.retain_candidate_action_ = *retain;
  command.reason_ = std::move(*reason);
  LAVIK_RETURN_IF_ERROR(FailStopDecodedFailover(ValidateCommand(command)));
  return command;
}

absl::Status WriteCommandBody(MetaWriter& writer,
                              const CommitControlledFailover& command) {
  LAVIK_RETURN_IF_ERROR(ValidateCommand(command));
  LAVIK_RETURN_IF_ERROR(
      WriteCommandHeader(writer, MetaCommandTag::kCommitControlledFailover,
                         command.request_id_, command.actor_));
  WriteFixedArray(writer, command.operation_id_);
  writer.WriteU64(command.expected_operation_revision_);
  WriteFailoverCommitBaseUnchecked(writer, command);
  return absl::OkStatus();
}

absl::StatusOr<CommitControlledFailover> ReadCommitControlledFailoverBody(
    MetaReader& reader) {
  auto header = ReadCommandHeader(reader);
  LAVIK_RETURN_IF_ERROR(header);
  auto operation_id = ReadFixedArray<16>(reader);
  LAVIK_RETURN_IF_ERROR(operation_id);
  auto operation_revision = reader.ReadU64();
  LAVIK_RETURN_IF_ERROR(operation_revision);
  CommitControlledFailover command;
  command.request_id_ = header->request_id_;
  command.actor_ = std::move(header->actor_);
  command.operation_id_ = *operation_id;
  command.expected_operation_revision_ = *operation_revision;
  LAVIK_RETURN_IF_ERROR(ReadFailoverCommitBase(reader, command));
  LAVIK_RETURN_IF_ERROR(FailStopDecodedFailover(ValidateCommand(command)));
  return command;
}

absl::Status WriteCommandBody(MetaWriter& writer,
                              const CommitUncontrolledFailover& command) {
  LAVIK_RETURN_IF_ERROR(ValidateCommand(command));
  LAVIK_RETURN_IF_ERROR(
      WriteCommandHeader(writer, MetaCommandTag::kCommitUncontrolledFailover,
                         command.request_id_, command.actor_));
  WriteFailoverCommitBaseUnchecked(writer, command);
  writer.WriteU8(static_cast<std::uint8_t>(command.loss_if_cutover_));
  return absl::OkStatus();
}

absl::StatusOr<CommitUncontrolledFailover> ReadCommitUncontrolledFailoverBody(
    MetaReader& reader) {
  auto header = ReadCommandHeader(reader);
  LAVIK_RETURN_IF_ERROR(header);
  CommitUncontrolledFailover command;
  command.request_id_ = header->request_id_;
  command.actor_ = std::move(header->actor_);
  LAVIK_RETURN_IF_ERROR(ReadFailoverCommitBase(reader, command));
  auto loss = reader.ReadU8();
  LAVIK_RETURN_IF_ERROR(loss);
  command.loss_if_cutover_ = static_cast<MetaFailoverLoss>(*loss);
  LAVIK_RETURN_IF_ERROR(FailStopDecodedFailover(ValidateCommand(command)));
  return command;
}

// Shared codec for the {group_id, expected_term} command pair.
template <typename Cmd>
absl::StatusOr<Cmd> ReadGroupTermGate(MetaReader& r) {
  auto header = ReadCommandHeader(r);
  LAVIK_RETURN_IF_ERROR(header);
  auto group_id = ReadGroupId(r);
  LAVIK_RETURN_IF_ERROR(group_id);
  auto expected_term = r.ReadU64();
  LAVIK_RETURN_IF_ERROR(expected_term);
  Cmd cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.group_id_ = std::move(*group_id);
  cmd.expected_term_ = *expected_term;
  return cmd;
}

absl::Status WriteCommandBody(MetaWriter& w, const BeginGroupTerm& cmd) {
  LAVIK_RETURN_IF_ERROR(WriteCommandHeader(w, MetaCommandTag::kBeginGroupTerm,
                                           cmd.request_id_, cmd.actor_));
  LAVIK_RETURN_IF_ERROR(WriteGroupId(w, cmd.group_id_));
  w.WriteU64(cmd.expected_term_);
  w.WriteU64(cmd.new_term_);
  return absl::OkStatus();
}

absl::StatusOr<BeginGroupTerm> ReadBeginGroupTermBody(MetaReader& r) {
  auto cmd = ReadGroupTermGate<BeginGroupTerm>(r);
  LAVIK_RETURN_IF_ERROR(cmd);
  LAVIK_ASSIGN_OR_RETURN(cmd->new_term_, r.ReadU64());
  return cmd;
}

absl::Status WriteCommandBody(MetaWriter& w, const ActivateAuthority& cmd) {
  LAVIK_RETURN_IF_ERROR(WriteCommandHeader(
      w, MetaCommandTag::kActivateAuthority, cmd.request_id_, cmd.actor_));
  LAVIK_RETURN_IF_ERROR(WriteGroupId(w, cmd.group_id_));
  w.WriteU64(cmd.expected_term_);
  LAVIK_RETURN_IF_ERROR(WriteNodeId(w, cmd.new_owner_));
  w.WriteU64(cmd.new_topology_epoch_);
  return absl::OkStatus();
}

absl::StatusOr<ActivateAuthority> ReadActivateAuthorityBody(MetaReader& r) {
  auto header = ReadCommandHeader(r);
  LAVIK_RETURN_IF_ERROR(header);
  auto group_id = ReadGroupId(r);
  LAVIK_RETURN_IF_ERROR(group_id);
  auto expected_term = r.ReadU64();
  LAVIK_RETURN_IF_ERROR(expected_term);
  auto new_owner = ReadNodeId(r);
  LAVIK_RETURN_IF_ERROR(new_owner);
  auto topology_epoch = r.ReadU64();
  LAVIK_RETURN_IF_ERROR(topology_epoch);
  ActivateAuthority cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.group_id_ = std::move(*group_id);
  cmd.expected_term_ = *expected_term;
  cmd.new_owner_ = std::move(*new_owner);
  cmd.new_topology_epoch_ = *topology_epoch;
  return cmd;
}

absl::Status WriteCommandBody(MetaWriter& w, const FenceGroup& cmd) {
  LAVIK_RETURN_IF_ERROR(WriteCommandHeader(w, MetaCommandTag::kFenceGroup,
                                           cmd.request_id_, cmd.actor_));
  LAVIK_RETURN_IF_ERROR(WriteGroupId(w, cmd.group_id_));
  w.WriteU64(cmd.expected_term_);
  w.WriteU64(cmd.new_term_);
  return absl::OkStatus();
}

absl::StatusOr<FenceGroup> ReadFenceGroupBody(MetaReader& r) {
  auto cmd = ReadGroupTermGate<FenceGroup>(r);
  LAVIK_RETURN_IF_ERROR(cmd);
  LAVIK_ASSIGN_OR_RETURN(cmd->new_term_, r.ReadU64());
  return cmd;
}

// ---------------------------------------------------------------------------
// policy codecs.
// ---------------------------------------------------------------------------

absl::Status WriteCommandBody(MetaWriter& w, const PutPolicy& cmd) {
  LAVIK_RETURN_IF_ERROR(
      CheckCap("policy_id", cmd.policy_id_.size(), kMaxMetaPolicyIdBytes));
  LAVIK_RETURN_IF_ERROR(
      CheckCap("content", cmd.content_.size(), kMaxMetaPayloadBytes));
  LAVIK_RETURN_IF_ERROR(WriteCommandHeader(w, MetaCommandTag::kPutPolicy,
                                           cmd.request_id_, cmd.actor_));
  w.WriteString(cmd.policy_id_);
  w.WriteU64(cmd.version_);
  w.WriteString(cmd.content_);
  return absl::OkStatus();
}

absl::StatusOr<PutPolicy> ReadPutPolicyBody(MetaReader& r) {
  auto header = ReadCommandHeader(r);
  LAVIK_RETURN_IF_ERROR(header);
  auto policy_id = ReadBoundedString(r, kMaxMetaPolicyIdBytes);
  LAVIK_RETURN_IF_ERROR(policy_id);
  auto version = r.ReadU64();
  LAVIK_RETURN_IF_ERROR(version);
  auto content = ReadBoundedString(r, kMaxMetaPayloadBytes);
  LAVIK_RETURN_IF_ERROR(content);
  PutPolicy cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.policy_id_ = std::move(*policy_id);
  cmd.version_ = *version;
  cmd.content_ = std::move(*content);
  return cmd;
}

// ---------------------------------------------------------------------------
// operation codecs.
// ---------------------------------------------------------------------------

absl::Status WriteCommandBody(MetaWriter& w, const SubmitOperation& cmd) {
  LAVIK_RETURN_IF_ERROR(
      CheckCap("kind", cmd.kind_.size(), kMaxMetaOperationKindBytes));
  LAVIK_RETURN_IF_ERROR(
      CheckCap("intent", cmd.intent_.size(), kMaxMetaPayloadBytes));
  LAVIK_RETURN_IF_ERROR(WriteCommandHeader(w, MetaCommandTag::kSubmitOperation,
                                           cmd.request_id_, cmd.actor_));
  WriteFixedArray(w, cmd.operation_id_);
  w.WriteString(cmd.kind_);
  w.WriteString(cmd.intent_);
  WriteFixedArray(w, cmd.intent_hash_);
  WriteFixedArray(w, cmd.replication_history_id_);
  return absl::OkStatus();
}

absl::StatusOr<SubmitOperation> ReadSubmitOperationBody(MetaReader& r) {
  auto header = ReadCommandHeader(r);
  LAVIK_RETURN_IF_ERROR(header);
  auto operation_id = ReadFixedArray<16>(r);
  LAVIK_RETURN_IF_ERROR(operation_id);
  auto kind = ReadBoundedString(r, kMaxMetaOperationKindBytes);
  LAVIK_RETURN_IF_ERROR(kind);
  auto intent = ReadBoundedString(r, kMaxMetaPayloadBytes);
  LAVIK_RETURN_IF_ERROR(intent);
  auto intent_hash = ReadFixedArray<32>(r);
  LAVIK_RETURN_IF_ERROR(intent_hash);
  auto replication_history = ReadFixedArray<kMetaReplicationHistoryIdBytes>(r);
  LAVIK_RETURN_IF_ERROR(replication_history);
  SubmitOperation cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.operation_id_ = *operation_id;
  cmd.kind_ = std::move(*kind);
  cmd.intent_ = std::move(*intent);
  cmd.intent_hash_ = *intent_hash;
  cmd.replication_history_id_ = *replication_history;
  return cmd;
}

absl::Status WriteCommandBody(MetaWriter& w,
                              const TransitionOperationPhase& cmd) {
  LAVIK_RETURN_IF_ERROR(CheckCap("kind_phase_blob", cmd.kind_phase_blob_.size(),
                                 kMaxMetaPayloadBytes));
  LAVIK_RETURN_IF_ERROR(CheckCap("current_directives",
                                 cmd.current_directives_.size(),
                                 kMaxMetaDirectivesPerOperation));
  for (const MetaDirectiveSpec& directive : cmd.current_directives_) {
    if (directive.recipient_node_id_.empty() ||
        directive.recipient_node_id_.size() > kMetaNodeIdBytes ||
        directive.target_node_id_.empty() ||
        directive.target_node_id_.size() > kMetaNodeIdBytes ||
        directive.source_node_id_.empty() ||
        directive.source_node_id_.size() > kMetaNodeIdBytes ||
        directive.group_id_.empty() ||
        directive.group_id_.size() > kMaxMetaGroupIdBytes ||
        directive.kind_.empty() ||
        directive.kind_.size() > kMaxMetaDirectiveKindBytes ||
        directive.payload_.size() > kMaxMetaPayloadBytes) {
      return MetaDomainRejectError("invalid directive field size");
    }
  }
  LAVIK_RETURN_IF_ERROR(
      WriteCommandHeader(w, MetaCommandTag::kTransitionOperationPhase,
                         cmd.request_id_, cmd.actor_));
  WriteFixedArray(w, cmd.operation_id_);
  w.WriteU64(cmd.expected_revision_);
  w.WriteString(cmd.kind_phase_blob_);
  w.WriteList(cmd.current_directives_, WriteMetaDirectiveSpec);
  return absl::OkStatus();
}

absl::StatusOr<TransitionOperationPhase> ReadTransitionOperationPhaseBody(
    MetaReader& r) {
  auto header = ReadCommandHeader(r);
  LAVIK_RETURN_IF_ERROR(header);
  auto operation_id = ReadFixedArray<16>(r);
  LAVIK_RETURN_IF_ERROR(operation_id);
  auto expected_revision = r.ReadU64();
  LAVIK_RETURN_IF_ERROR(expected_revision);
  auto blob = ReadBoundedString(r, kMaxMetaPayloadBytes);
  LAVIK_RETURN_IF_ERROR(blob);
  auto directives = r.ReadList<MetaDirectiveSpec>(
      kMaxMetaDirectivesPerOperation,
      [](MetaReader& reader) { return ReadMetaDirectiveSpec(reader); });
  LAVIK_RETURN_IF_ERROR(directives);
  TransitionOperationPhase cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.operation_id_ = *operation_id;
  cmd.expected_revision_ = *expected_revision;
  cmd.kind_phase_blob_ = std::move(*blob);
  cmd.current_directives_ = std::move(*directives);
  return cmd;
}

void WriteTerminalReceiptKey(MetaWriter& w, const MetaTerminalReceiptKey& key) {
  WriteFixedArray(w, key.operation_id_);
  WriteFixedArray(w, key.directive_id_);
  WriteFixedArray(w, key.attempt_id_);
  w.WriteU64(key.directive_revision_);
}

absl::StatusOr<MetaTerminalReceiptKey> ReadTerminalReceiptKey(MetaReader& r) {
  auto operation_id = ReadFixedArray<16>(r);
  LAVIK_RETURN_IF_ERROR(operation_id);
  auto directive_id = ReadFixedArray<16>(r);
  LAVIK_RETURN_IF_ERROR(directive_id);
  auto attempt_id = ReadFixedArray<16>(r);
  LAVIK_RETURN_IF_ERROR(attempt_id);
  auto directive_revision = r.ReadU64();
  LAVIK_RETURN_IF_ERROR(directive_revision);
  return MetaTerminalReceiptKey{*operation_id, *directive_id, *attempt_id,
                                *directive_revision};
}

absl::Status WriteCommandBody(MetaWriter& w, const CommitDirectiveResult& cmd) {
  const auto status = static_cast<std::uint8_t>(cmd.status_);
  if (status <
          static_cast<std::uint8_t>(MetaDirectiveResultStatus::kSucceeded) ||
      status >
          static_cast<std::uint8_t>(MetaDirectiveResultStatus::kRejected)) {
    return MetaDomainRejectError("unknown directive result status");
  }
  if (cmd.recipient_node_id_.empty() ||
      cmd.recipient_node_id_.size() > kMetaNodeIdBytes ||
      cmd.result_.size() > kMaxMetaPayloadBytes) {
    return MetaDomainRejectError("invalid directive result field size");
  }
  LAVIK_RETURN_IF_ERROR(WriteCommandHeader(
      w, MetaCommandTag::kCommitDirectiveResult, cmd.request_id_, cmd.actor_));
  WriteFixedArray(w, cmd.operation_id_);
  WriteFixedArray(w, cmd.directive_id_);
  WriteFixedArray(w, cmd.attempt_id_);
  w.WriteU64(cmd.directive_revision_);
  w.WriteString(cmd.recipient_node_id_);
  WriteFixedArray(w, cmd.recipient_boot_id_);
  WriteFixedArray(w, cmd.assignment_id_);
  w.WriteU8(status);
  w.WriteString(cmd.result_);
  return absl::OkStatus();
}

absl::StatusOr<CommitDirectiveResult> ReadCommitDirectiveResultBody(
    MetaReader& r) {
  auto header = ReadCommandHeader(r);
  LAVIK_RETURN_IF_ERROR(header);
  auto operation_id = ReadFixedArray<16>(r);
  LAVIK_RETURN_IF_ERROR(operation_id);
  auto directive_id = ReadFixedArray<16>(r);
  LAVIK_RETURN_IF_ERROR(directive_id);
  auto attempt_id = ReadFixedArray<16>(r);
  LAVIK_RETURN_IF_ERROR(attempt_id);
  auto directive_revision = r.ReadU64();
  LAVIK_RETURN_IF_ERROR(directive_revision);
  auto recipient_node = ReadBoundedString(r, kMetaNodeIdBytes);
  LAVIK_RETURN_IF_ERROR(recipient_node);
  auto recipient_boot = ReadFixedArray<kMetaBootIncarnationBytes>(r);
  LAVIK_RETURN_IF_ERROR(recipient_boot);
  auto assignment_id = ReadFixedArray<16>(r);
  LAVIK_RETURN_IF_ERROR(assignment_id);
  auto status = r.ReadU8();
  LAVIK_RETURN_IF_ERROR(status);
  if (*status <
          static_cast<std::uint8_t>(MetaDirectiveResultStatus::kSucceeded) ||
      *status >
          static_cast<std::uint8_t>(MetaDirectiveResultStatus::kRejected)) {
    return MetaFailStopError("unknown directive result status");
  }
  auto result = ReadBoundedString(r, kMaxMetaPayloadBytes);
  LAVIK_RETURN_IF_ERROR(result);

  CommitDirectiveResult cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.operation_id_ = *operation_id;
  cmd.directive_id_ = *directive_id;
  cmd.attempt_id_ = *attempt_id;
  cmd.directive_revision_ = *directive_revision;
  cmd.recipient_node_id_ = std::move(*recipient_node);
  cmd.recipient_boot_id_ = *recipient_boot;
  cmd.assignment_id_ = *assignment_id;
  cmd.status_ = static_cast<MetaDirectiveResultStatus>(*status);
  cmd.result_ = std::move(*result);
  return cmd;
}

absl::Status WriteCommandBody(MetaWriter& w, const CompleteOperation& cmd) {
  LAVIK_RETURN_IF_ERROR(
      CheckCap("result", cmd.result_.size(), kMaxMetaPayloadBytes));
  LAVIK_RETURN_IF_ERROR(WriteCommandHeader(
      w, MetaCommandTag::kCompleteOperation, cmd.request_id_, cmd.actor_));
  WriteFixedArray(w, cmd.operation_id_);
  w.WriteU64(cmd.expected_revision_);
  w.WriteString(cmd.result_);
  w.WriteBool(cmd.data_loss_possible_);
  return absl::OkStatus();
}

absl::StatusOr<CompleteOperation> ReadCompleteOperationBody(MetaReader& r) {
  auto header = ReadCommandHeader(r);
  LAVIK_RETURN_IF_ERROR(header);
  auto operation_id = ReadFixedArray<16>(r);
  LAVIK_RETURN_IF_ERROR(operation_id);
  auto expected_revision = r.ReadU64();
  LAVIK_RETURN_IF_ERROR(expected_revision);
  auto result = ReadBoundedString(r, kMaxMetaPayloadBytes);
  LAVIK_RETURN_IF_ERROR(result);
  auto data_loss = r.ReadBool("data_loss_possible must be 0 or 1");
  LAVIK_RETURN_IF_ERROR(data_loss);
  CompleteOperation cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.operation_id_ = *operation_id;
  cmd.expected_revision_ = *expected_revision;
  cmd.result_ = std::move(*result);
  cmd.data_loss_possible_ = *data_loss;
  return cmd;
}

absl::Status WriteCommandBody(MetaWriter& w, const AbortOperation& cmd) {
  LAVIK_RETURN_IF_ERROR(
      CheckCap("reason", cmd.reason_.size(), kMaxMetaAbortReasonBytes));
  LAVIK_RETURN_IF_ERROR(WriteCommandHeader(w, MetaCommandTag::kAbortOperation,
                                           cmd.request_id_, cmd.actor_));
  WriteFixedArray(w, cmd.operation_id_);
  w.WriteU64(cmd.expected_revision_);
  w.WriteString(cmd.reason_);
  return absl::OkStatus();
}

absl::StatusOr<AbortOperation> ReadAbortOperationBody(MetaReader& r) {
  auto header = ReadCommandHeader(r);
  LAVIK_RETURN_IF_ERROR(header);
  auto operation_id = ReadFixedArray<16>(r);
  LAVIK_RETURN_IF_ERROR(operation_id);
  auto expected_revision = r.ReadU64();
  LAVIK_RETURN_IF_ERROR(expected_revision);
  auto reason = ReadBoundedString(r, kMaxMetaAbortReasonBytes);
  LAVIK_RETURN_IF_ERROR(reason);
  AbortOperation cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.operation_id_ = *operation_id;
  cmd.expected_revision_ = *expected_revision;
  cmd.reason_ = std::move(*reason);
  return cmd;
}

absl::Status WriteCommandBody(MetaWriter& w, const ArchiveOperations& cmd) {
  LAVIK_RETURN_IF_ERROR(CheckCap("operation_seqs", cmd.operation_seqs_.size(),
                                 kMaxMetaActiveOperations));
  LAVIK_RETURN_IF_ERROR(WriteCommandHeader(
      w, MetaCommandTag::kArchiveOperations, cmd.request_id_, cmd.actor_));
  w.WriteList(cmd.operation_seqs_,
              [](MetaWriter& ww, std::uint64_t seq) { ww.WriteU64(seq); });
  return absl::OkStatus();
}

absl::StatusOr<ArchiveOperations> ReadArchiveOperationsBody(MetaReader& r) {
  auto header = ReadCommandHeader(r);
  LAVIK_RETURN_IF_ERROR(header);
  auto seqs = r.ReadList<std::uint64_t>(
      kMaxMetaActiveOperations, [](MetaReader& rr) { return rr.ReadU64(); });
  LAVIK_RETURN_IF_ERROR(seqs);
  ArchiveOperations cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.operation_seqs_ = std::move(*seqs);
  return cmd;
}

absl::Status WriteCommandBody(MetaWriter& w, const PruneAudit& cmd) {
  LAVIK_RETURN_IF_ERROR(WriteCommandHeader(w, MetaCommandTag::kPruneAudit,
                                           cmd.request_id_, cmd.actor_));
  w.WriteU64(cmd.through_log_index_);
  return absl::OkStatus();
}

absl::StatusOr<PruneAudit> ReadPruneAuditBody(MetaReader& r) {
  auto header = ReadCommandHeader(r);
  LAVIK_RETURN_IF_ERROR(header);
  auto through = r.ReadU64();
  LAVIK_RETURN_IF_ERROR(through);
  PruneAudit cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.through_log_index_ = *through;
  return cmd;
}

absl::Status WriteCommandBody(MetaWriter& w, const SetAuditPolicy& cmd) {
  LAVIK_RETURN_IF_ERROR(CheckCap("attestation", cmd.attestation_.size(),
                                 kMaxMetaAttestationBytes));
  if (cmd.policy_ != MetaAuditPolicy::kDisabled &&
      cmd.policy_ != MetaAuditPolicy::kBoundedRotate &&
      cmd.policy_ != MetaAuditPolicy::kStrictExport) {
    return MetaDomainRejectError("unknown audit policy");
  }
  LAVIK_RETURN_IF_ERROR(WriteCommandHeader(w, MetaCommandTag::kSetAuditPolicy,
                                           cmd.request_id_, cmd.actor_));
  w.WriteU8(static_cast<std::uint8_t>(cmd.policy_));
  w.WriteString(cmd.attestation_);
  return absl::OkStatus();
}

absl::StatusOr<SetAuditPolicy> ReadSetAuditPolicyBody(MetaReader& r) {
  auto header = ReadCommandHeader(r);
  LAVIK_RETURN_IF_ERROR(header);
  auto policy = r.ReadU8();
  LAVIK_RETURN_IF_ERROR(policy);
  if (*policy > static_cast<std::uint8_t>(MetaAuditPolicy::kStrictExport)) {
    return MetaFailStopError("unknown audit policy tag");
  }
  auto attestation = ReadBoundedString(r, kMaxMetaAttestationBytes);
  LAVIK_RETURN_IF_ERROR(attestation);
  SetAuditPolicy cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.policy_ = static_cast<MetaAuditPolicy>(*policy);
  cmd.attestation_ = std::move(*attestation);
  return cmd;
}

absl::Status WriteCommandBody(MetaWriter& w, const PruneOperationArchive& cmd) {
  LAVIK_RETURN_IF_ERROR(CheckCap("operation_seqs", cmd.operation_seqs_.size(),
                                 kMaxMetaArchivedOperationSummaries));
  LAVIK_RETURN_IF_ERROR(WriteCommandHeader(
      w, MetaCommandTag::kPruneOperationArchive, cmd.request_id_, cmd.actor_));
  w.WriteList(cmd.operation_seqs_,
              [](MetaWriter& ww, std::uint64_t seq) { ww.WriteU64(seq); });
  return absl::OkStatus();
}

absl::StatusOr<PruneOperationArchive> ReadPruneOperationArchiveBody(
    MetaReader& r) {
  auto header = ReadCommandHeader(r);
  LAVIK_RETURN_IF_ERROR(header);
  auto seqs =
      r.ReadList<std::uint64_t>(kMaxMetaArchivedOperationSummaries,
                                [](MetaReader& rr) { return rr.ReadU64(); });
  LAVIK_RETURN_IF_ERROR(seqs);
  PruneOperationArchive cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.operation_seqs_ = std::move(*seqs);
  return cmd;
}

absl::Status WriteCommandBody(MetaWriter& w, const PruneTerminalReceipts& cmd) {
  LAVIK_RETURN_IF_ERROR(CheckCap("terminal receipts", cmd.receipts_.size(),
                                 kMaxMetaTerminalReceiptPrunesPerCommand));
  LAVIK_RETURN_IF_ERROR(WriteCommandHeader(
      w, MetaCommandTag::kPruneTerminalReceipts, cmd.request_id_, cmd.actor_));
  w.WriteList(cmd.receipts_, WriteTerminalReceiptKey);
  return absl::OkStatus();
}

absl::StatusOr<PruneTerminalReceipts> ReadPruneTerminalReceiptsBody(
    MetaReader& r) {
  auto header = ReadCommandHeader(r);
  LAVIK_RETURN_IF_ERROR(header);
  auto receipts = r.ReadList<MetaTerminalReceiptKey>(
      kMaxMetaTerminalReceiptPrunesPerCommand,
      [](MetaReader& reader) { return ReadTerminalReceiptKey(reader); });
  LAVIK_RETURN_IF_ERROR(receipts);
  PruneTerminalReceipts cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.receipts_ = std::move(*receipts);
  return cmd;
}

absl::Status WriteCommandBody(MetaWriter& w, const BindMetaMember& cmd) {
  LAVIK_RETURN_IF_ERROR(
      CheckCap("principal", cmd.principal_.size(), kMaxMetaPrincipalBytes));
  if (cmd.data_control_endpoint_.empty() ||
      cmd.data_control_endpoint_.size() > kMaxMetaEndpointBytes) {
    return MetaDomainRejectError(
        "data_control_endpoint is empty or exceeds its cap");
  }
  if (cmd.ctl_endpoint_.has_value() &&
      (cmd.ctl_endpoint_->empty() ||
       cmd.ctl_endpoint_->size() > kMaxMetaEndpointBytes)) {
    return MetaDomainRejectError("ctl_endpoint is empty or exceeds its cap");
  }
  LAVIK_RETURN_IF_ERROR(WriteCommandHeader(w, MetaCommandTag::kBindMetaMember,
                                           cmd.request_id_, cmd.actor_));
  w.WriteU32(cmd.server_id_);
  w.WriteString(cmd.principal_);
  w.WriteString(cmd.data_control_endpoint_);
  w.WriteBool(cmd.ctl_endpoint_.has_value());
  if (cmd.ctl_endpoint_.has_value()) w.WriteString(*cmd.ctl_endpoint_);
  if (cmd.sentinel_endpoint_.size() > kMaxMetaEndpointBytes)
    return MetaDomainRejectError("Sentinel endpoint exceeds its cap");
  w.WriteString(cmd.sentinel_endpoint_);
  return absl::OkStatus();
}

absl::StatusOr<BindMetaMember> ReadBindMetaMemberBody(MetaReader& r) {
  auto header = ReadCommandHeader(r);
  LAVIK_RETURN_IF_ERROR(header);
  auto server_id = r.ReadU32();
  LAVIK_RETURN_IF_ERROR(server_id);
  auto principal = ReadBoundedString(r, kMaxMetaPrincipalBytes);
  LAVIK_RETURN_IF_ERROR(principal);
  auto data_control_endpoint = ReadBoundedString(r, kMaxMetaEndpointBytes);
  LAVIK_RETURN_IF_ERROR(data_control_endpoint);
  auto has_ctl_endpoint = r.ReadBool("invalid ctl endpoint presence tag");
  LAVIK_RETURN_IF_ERROR(has_ctl_endpoint);
  std::optional<std::string> ctl_endpoint;
  if (*has_ctl_endpoint) {
    LAVIK_ASSIGN_OR_RETURN(ctl_endpoint,
                           ReadBoundedString(r, kMaxMetaEndpointBytes));
  }
  auto sentinel = ReadBoundedString(r, kMaxMetaEndpointBytes);
  LAVIK_RETURN_IF_ERROR(sentinel);
  BindMetaMember cmd;
  cmd.sentinel_endpoint_ = std::move(*sentinel);
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.server_id_ = *server_id;
  cmd.principal_ = std::move(*principal);
  cmd.data_control_endpoint_ = std::move(*data_control_endpoint);
  cmd.ctl_endpoint_ = std::move(ctl_endpoint);
  return cmd;
}

absl::Status WriteCommandBody(MetaWriter& w, const RetireMetaMember& cmd) {
  LAVIK_RETURN_IF_ERROR(WriteCommandHeader(w, MetaCommandTag::kRetireMetaMember,
                                           cmd.request_id_, cmd.actor_));
  w.WriteU32(cmd.server_id_);
  return absl::OkStatus();
}

absl::StatusOr<RetireMetaMember> ReadRetireMetaMemberBody(MetaReader& r) {
  auto header = ReadCommandHeader(r);
  LAVIK_RETURN_IF_ERROR(header);
  auto server_id = r.ReadU32();
  LAVIK_RETURN_IF_ERROR(server_id);
  RetireMetaMember cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.server_id_ = *server_id;
  return cmd;
}

absl::Status WriteCommandBody(MetaWriter& w, const PutPopulationManifest& cmd) {
  if (cmd.entries_.size() > kMetaSlotCount) {
    return MetaDomainRejectError("population manifest entry cap exceeded");
  }
  LAVIK_RETURN_IF_ERROR(WriteCommandHeader(
      w, MetaCommandTag::kPutPopulationManifest, cmd.request_id_, cmd.actor_));
  WriteFixedArray(w, cmd.manifest_digest_);
  w.WriteList(cmd.entries_,
              [](MetaWriter& writer, const MetaPopulationManifestEntry& entry) {
                writer.WriteU32(entry.partition_id_);
                writer.WriteU64(entry.logical_epoch_);
              });
  return absl::OkStatus();
}

absl::StatusOr<PutPopulationManifest> ReadPutPopulationManifestBody(
    MetaReader& r) {
  auto header = ReadCommandHeader(r);
  LAVIK_RETURN_IF_ERROR(header);
  auto digest = ReadFixedArray<32>(r);
  LAVIK_RETURN_IF_ERROR(digest);
  auto entries = r.ReadList<MetaPopulationManifestEntry>(
      kMetaSlotCount,
      [](MetaReader& reader) -> absl::StatusOr<MetaPopulationManifestEntry> {
        auto partition_id = reader.ReadU32();
        LAVIK_RETURN_IF_ERROR(partition_id);
        auto logical_epoch = reader.ReadU64();
        LAVIK_RETURN_IF_ERROR(logical_epoch);
        return MetaPopulationManifestEntry{*partition_id, *logical_epoch};
      });
  LAVIK_RETURN_IF_ERROR(entries);
  PutPopulationManifest cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.manifest_digest_ = *digest;
  cmd.entries_ = std::move(*entries);
  return cmd;
}

absl::Status WriteCommandBody(MetaWriter& w,
                              const PrunePopulationManifest& cmd) {
  LAVIK_RETURN_IF_ERROR(
      WriteCommandHeader(w, MetaCommandTag::kPrunePopulationManifest,
                         cmd.request_id_, cmd.actor_));
  WriteFixedArray(w, cmd.manifest_digest_);
  return absl::OkStatus();
}

absl::StatusOr<PrunePopulationManifest> ReadPrunePopulationManifestBody(
    MetaReader& r) {
  auto header = ReadCommandHeader(r);
  LAVIK_RETURN_IF_ERROR(header);
  auto digest = ReadFixedArray<32>(r);
  LAVIK_RETURN_IF_ERROR(digest);
  PrunePopulationManifest cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.manifest_digest_ = *digest;
  return cmd;
}

}  // namespace

absl::Status ValidateMetaFailoverTransition(
    const MetaFailoverTransition& transition) {
  return ValidateFailoverTransitionImpl(transition);
}

absl::Status WriteMetaFailoverTransition(
    MetaWriter& writer, const MetaFailoverTransition& transition) {
  LAVIK_RETURN_IF_ERROR(ValidateFailoverTransitionImpl(transition));
  WriteFailoverTransitionUnchecked(writer, transition);
  return absl::OkStatus();
}

absl::StatusOr<MetaFailoverTransition> ReadMetaFailoverTransition(
    MetaReader& reader) {
  auto transition = ReadFailoverTransitionUnchecked(reader);
  LAVIK_RETURN_IF_ERROR(transition);
  LAVIK_RETURN_IF_ERROR(
      FailStopDecodedFailover(ValidateFailoverTransitionImpl(*transition)));
  return transition;
}

absl::StatusOr<std::string> EncodeMetaCommand(const MetaCommand& command) {
  MetaWriter w;
  w.WriteU16(kMetaCommandFormatVersion);
  LAVIK_RETURN_IF_ERROR(std::visit(
      [&w](const auto& cmd) -> absl::Status {
        return WriteCommandBody(w, cmd);
      },
      command));
  if (w.buffer().size() > kMaxMetaCommandBytes) {
    return MetaDomainRejectError("command exceeds kMaxMetaCommandBytes");
  }
  return w.TakeBuffer();
}

MetaCommandTag MetaCommandTagOf(const MetaCommand& command) noexcept {
  static_assert(std::variant_size_v<MetaCommand> == 35);
  const std::size_t index = command.index();
  if (index < 8) return static_cast<MetaCommandTag>(index + 1);
  if (index == 8) return MetaCommandTag::kActivateAuthority;
  if (index < 11) return static_cast<MetaCommandTag>(index + 3);
  return static_cast<MetaCommandTag>(index + 4);
}

absl::StatusOr<MetaCommand> DecodeMetaCommand(std::string_view bytes) {
  if (bytes.size() > kMaxMetaCommandBytes) {
    return MetaFailStopError("command exceeds kMaxMetaCommandBytes");
  }
  MetaReader r(bytes);
  auto version = r.ReadU16();
  LAVIK_RETURN_IF_ERROR(version);
  if (*version != kMetaCommandFormatVersion) {
    return MetaFailStopError("unknown schema_version");
  }
  auto tag = r.ReadU16();
  LAVIK_RETURN_IF_ERROR(tag);

  MetaCommand command;
  switch (static_cast<MetaCommandTag>(*tag)) {
    case MetaCommandTag::kRegisterNode: {
      LAVIK_ASSIGN_OR_RETURN(command, ReadRegisterNodeBody(r));
      break;
    }
    case MetaCommandTag::kUpdateNode: {
      LAVIK_ASSIGN_OR_RETURN(command, ReadUpdateNodeBody(r));
      break;
    }
    case MetaCommandTag::kRetireNode: {
      LAVIK_ASSIGN_OR_RETURN(command, ReadRetireNodeBody(r));
      break;
    }
    case MetaCommandTag::kCreateGroup: {
      LAVIK_ASSIGN_OR_RETURN(command, ReadCreateGroupBody(r));
      break;
    }
    case MetaCommandTag::kAssignNodeToGroup: {
      LAVIK_ASSIGN_OR_RETURN(command, ReadAssignNodeToGroupBody(r));
      break;
    }
    case MetaCommandTag::kRemoveNodeFromGroup: {
      LAVIK_ASSIGN_OR_RETURN(command, ReadRemoveNodeFromGroupBody(r));
      break;
    }
    case MetaCommandTag::kSetSlotMap: {
      LAVIK_ASSIGN_OR_RETURN(command, ReadSetSlotMapBody(r));
      break;
    }
    case MetaCommandTag::kBeginGroupTerm: {
      LAVIK_ASSIGN_OR_RETURN(command, ReadBeginGroupTermBody(r));
      break;
    }
    case MetaCommandTag::kActivateAuthority: {
      LAVIK_ASSIGN_OR_RETURN(command, ReadActivateAuthorityBody(r));
      break;
    }
    case MetaCommandTag::kFenceGroup: {
      LAVIK_ASSIGN_OR_RETURN(command, ReadFenceGroupBody(r));
      break;
    }
    case MetaCommandTag::kPutPolicy: {
      LAVIK_ASSIGN_OR_RETURN(command, ReadPutPolicyBody(r));
      break;
    }
    case MetaCommandTag::kSubmitOperation: {
      LAVIK_ASSIGN_OR_RETURN(command, ReadSubmitOperationBody(r));
      break;
    }
    case MetaCommandTag::kTransitionOperationPhase: {
      LAVIK_ASSIGN_OR_RETURN(command, ReadTransitionOperationPhaseBody(r));
      break;
    }
    case MetaCommandTag::kCompleteOperation: {
      LAVIK_ASSIGN_OR_RETURN(command, ReadCompleteOperationBody(r));
      break;
    }
    case MetaCommandTag::kAbortOperation: {
      LAVIK_ASSIGN_OR_RETURN(command, ReadAbortOperationBody(r));
      break;
    }
    case MetaCommandTag::kArchiveOperations: {
      LAVIK_ASSIGN_OR_RETURN(command, ReadArchiveOperationsBody(r));
      break;
    }
    case MetaCommandTag::kPruneAudit: {
      LAVIK_ASSIGN_OR_RETURN(command, ReadPruneAuditBody(r));
      break;
    }
    case MetaCommandTag::kPruneOperationArchive: {
      LAVIK_ASSIGN_OR_RETURN(command, ReadPruneOperationArchiveBody(r));
      break;
    }
    case MetaCommandTag::kBindMetaMember: {
      LAVIK_ASSIGN_OR_RETURN(command, ReadBindMetaMemberBody(r));
      break;
    }
    case MetaCommandTag::kRetireMetaMember: {
      LAVIK_ASSIGN_OR_RETURN(command, ReadRetireMetaMemberBody(r));
      break;
    }
    case MetaCommandTag::kSetGroupReplicationState: {
      LAVIK_ASSIGN_OR_RETURN(command, ReadSetGroupReplicationStateBody(r));
      break;
    }
    case MetaCommandTag::kSetAuditPolicy: {
      LAVIK_ASSIGN_OR_RETURN(command, ReadSetAuditPolicyBody(r));
      break;
    }
    case MetaCommandTag::kPutPopulationManifest: {
      LAVIK_ASSIGN_OR_RETURN(command, ReadPutPopulationManifestBody(r));
      break;
    }
    case MetaCommandTag::kPrunePopulationManifest: {
      LAVIK_ASSIGN_OR_RETURN(command, ReadPrunePopulationManifestBody(r));
      break;
    }
    case MetaCommandTag::kCommitDirectiveResult: {
      LAVIK_ASSIGN_OR_RETURN(command, ReadCommitDirectiveResultBody(r));
      break;
    }
    case MetaCommandTag::kPruneTerminalReceipts: {
      LAVIK_ASSIGN_OR_RETURN(command, ReadPruneTerminalReceiptsBody(r));
      break;
    }
    case MetaCommandTag::kBeginControlledFailover: {
      LAVIK_ASSIGN_OR_RETURN(command, ReadBeginControlledFailoverBody(r));
      break;
    }
    case MetaCommandTag::kBeginUncontrolledFailover: {
      LAVIK_ASSIGN_OR_RETURN(command, ReadBeginUncontrolledFailoverBody(r));
      break;
    }
    case MetaCommandTag::kSetUncontrolledCandidate: {
      LAVIK_ASSIGN_OR_RETURN(command, ReadSetUncontrolledCandidateBody(r));
      break;
    }
    case MetaCommandTag::kAuthorizeFailoverPrepare: {
      LAVIK_ASSIGN_OR_RETURN(command, ReadAuthorizeFailoverPrepareBody(r));
      break;
    }
    case MetaCommandTag::kAbortControlledFailover: {
      LAVIK_ASSIGN_OR_RETURN(command, ReadAbortControlledFailoverBody(r));
      break;
    }
    case MetaCommandTag::kDegradeControlledFailover: {
      LAVIK_ASSIGN_OR_RETURN(command, ReadDegradeControlledFailoverBody(r));
      break;
    }
    case MetaCommandTag::kCommitControlledFailover: {
      LAVIK_ASSIGN_OR_RETURN(command, ReadCommitControlledFailoverBody(r));
      break;
    }
    case MetaCommandTag::kStartCandidateRecovery: {
      LAVIK_ASSIGN_OR_RETURN(command, ReadStartCandidateRecoveryBody(r));
      break;
    }
    case MetaCommandTag::kCommitUncontrolledFailover: {
      LAVIK_ASSIGN_OR_RETURN(command, ReadCommitUncontrolledFailoverBody(r));
      break;
    }
    default:
      return MetaFailStopError("unknown command tag");
  }
  LAVIK_RETURN_IF_ERROR(r.Finish());
  return command;
}

absl::StatusOr<std::string> EncodeMetaGroupRecord(
    const MetaGroupRecord& record) {
  LAVIK_RETURN_IF_ERROR(
      CheckCap("owner", record.owner_.size(), kMetaNodeIdBytes));
  const bool zero_digest =
      std::all_of(record.population_manifest_digest_.begin(),
                  record.population_manifest_digest_.end(),
                  [](std::uint8_t byte) { return byte == 0; });
  if ((record.population_manifest_revision_ == 0) != zero_digest) {
    return MetaDomainRejectError("manifest revision/digest invariant violated");
  }
  MetaWriter w;
  w.WriteU16(kMetaFormatVersion);
  w.WriteString(record.owner_);
  w.WriteU64(record.group_term_);
  w.WriteU64(record.population_manifest_revision_);
  WriteFixedArray(w, record.population_manifest_digest_);
  w.WriteU64(record.partition_replication_epoch_);
  return w.TakeBuffer();
}

absl::StatusOr<MetaGroupRecord> DecodeMetaGroupRecord(std::string_view bytes) {
  MetaReader r(bytes);
  auto version = r.ReadU16();
  LAVIK_RETURN_IF_ERROR(version);
  if (*version != kMetaFormatVersion) {
    return MetaFailStopError("unknown schema_version");
  }
  MetaGroupRecord record;
  LAVIK_ASSIGN_OR_RETURN(record.owner_, r.ReadString(kMetaNodeIdBytes));
  LAVIK_ASSIGN_OR_RETURN(record.group_term_, r.ReadU64());
  LAVIK_ASSIGN_OR_RETURN(record.population_manifest_revision_, r.ReadU64());
  LAVIK_ASSIGN_OR_RETURN(record.population_manifest_digest_,
                         ReadFixedArray<32>(r));
  const bool zero_digest =
      std::all_of(record.population_manifest_digest_.begin(),
                  record.population_manifest_digest_.end(),
                  [](std::uint8_t byte) { return byte == 0; });
  if ((record.population_manifest_revision_ == 0) != zero_digest) {
    return MetaFailStopError("manifest revision/digest invariant violated");
  }
  LAVIK_ASSIGN_OR_RETURN(record.partition_replication_epoch_, r.ReadU64());
  LAVIK_RETURN_IF_ERROR(r.Finish());
  return record;
}

}  // namespace lavik::meta
