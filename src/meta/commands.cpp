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
  if (auto st = CheckCap("actor_principal", actor.principal_.size(),
                         kMaxMetaPrincipalBytes);
      !st.ok()) {
    return st;
  }
  if (auto st = CheckCap("readable_time", actor.readable_time_.size(),
                         kMaxMetaActorReadableTimeBytes);
      !st.ok()) {
    return st;
  }
  w.WriteU16(static_cast<std::uint16_t>(tag));
  WriteFixedArray(w, request_id);
  WriteActorContext(w, actor);
  return absl::OkStatus();
}

absl::StatusOr<std::string> ReadBoundedString(MetaReader& r,
                                              std::uint32_t cap) {
  auto raw = r.ReadString(cap);
  if (!raw.ok()) return raw.status();
  return std::string(*raw);
}

struct MetaCommandHeader {
  MetaRequestId request_id_{};
  ActorContext actor_;
};

absl::StatusOr<MetaCommandHeader> ReadCommandHeader(MetaReader& r) {
  auto request_id = ReadFixedArray<16>(r);
  if (!request_id.ok()) return request_id.status();
  auto actor = ReadActorContext(r);
  if (!actor.ok()) return actor.status();
  MetaCommandHeader header;
  header.request_id_ = *request_id;
  header.actor_ = std::move(*actor);
  return header;
}

// ---------------------------------------------------------------------------
// Shared field codecs.
// ---------------------------------------------------------------------------

absl::Status WriteNodeId(MetaWriter& w, const std::string& node_id) {
  if (auto st = CheckCap("node_id", node_id.size(), kMetaNodeIdBytes);
      !st.ok()) {
    return st;
  }
  w.WriteString(node_id);
  return absl::OkStatus();
}

absl::StatusOr<std::string> ReadNodeId(MetaReader& r) {
  return ReadBoundedString(r, kMetaNodeIdBytes);
}

absl::Status WriteGroupId(MetaWriter& w, const std::string& group_id) {
  if (auto st = CheckCap("group_id", group_id.size(), kMaxMetaGroupIdBytes);
      !st.ok()) {
    return st;
  }
  w.WriteString(group_id);
  return absl::OkStatus();
}

absl::StatusOr<std::string> ReadGroupId(MetaReader& r) {
  return ReadBoundedString(r, kMaxMetaGroupIdBytes);
}

absl::Status WriteEndpoints(MetaWriter& w,
                            const std::vector<std::string>& endpoints) {
  if (auto st =
          CheckCap("endpoints", endpoints.size(), kMaxMetaEndpointsPerNode);
      !st.ok()) {
    return st;
  }
  for (const std::string& ep : endpoints) {
    if (auto st = CheckCap("endpoint", ep.size(), kMaxMetaEndpointBytes);
        !st.ok()) {
      return st;
    }
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
  if (!role.ok()) return role.status();
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
  if (auto st =
          CheckCap("principal", cmd.principal_.size(), kMaxMetaPrincipalBytes);
      !st.ok()) {
    return st;
  }
  if (auto st = WriteCommandHeader(w, MetaCommandTag::kRegisterNode,
                                   cmd.request_id_, cmd.actor_);
      !st.ok()) {
    return st;
  }
  if (auto st = WriteNodeId(w, cmd.node_id_); !st.ok()) return st;
  w.WriteString(cmd.principal_);
  if (auto st = WriteEndpoints(w, cmd.endpoints_); !st.ok()) return st;

  return WriteRole(w, cmd.role_);
}

absl::StatusOr<RegisterNode> ReadRegisterNodeBody(MetaReader& r) {
  auto header = ReadCommandHeader(r);
  if (!header.ok()) return header.status();
  auto node_id = ReadNodeId(r);
  if (!node_id.ok()) return node_id.status();
  auto principal = ReadBoundedString(r, kMaxMetaPrincipalBytes);
  if (!principal.ok()) return principal.status();
  auto endpoints = ReadEndpoints(r);
  if (!endpoints.ok()) return endpoints.status();

  auto role = ReadRole(r);
  if (!role.ok()) return role.status();
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
  if (auto st = WriteCommandHeader(w, MetaCommandTag::kUpdateNode,
                                   cmd.request_id_, cmd.actor_);
      !st.ok()) {
    return st;
  }
  if (auto st = WriteNodeId(w, cmd.node_id_); !st.ok()) return st;
  w.WriteU64(cmd.expected_revision_);
  if (auto st = WriteEndpoints(w, cmd.endpoints_); !st.ok()) return st;

  w.WriteU64(cmd.new_topology_epoch_);
  return absl::OkStatus();
}

absl::StatusOr<UpdateNode> ReadUpdateNodeBody(MetaReader& r) {
  auto header = ReadCommandHeader(r);
  if (!header.ok()) return header.status();
  auto node_id = ReadNodeId(r);
  if (!node_id.ok()) return node_id.status();
  auto expected_revision = r.ReadU64();
  if (!expected_revision.ok()) return expected_revision.status();
  auto endpoints = ReadEndpoints(r);
  if (!endpoints.ok()) return endpoints.status();

  auto topology_epoch = r.ReadU64();
  if (!topology_epoch.ok()) return topology_epoch.status();
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
  if (auto st = WriteCommandHeader(w, MetaCommandTag::kRetireNode,
                                   cmd.request_id_, cmd.actor_);
      !st.ok()) {
    return st;
  }
  if (auto st = WriteNodeId(w, cmd.node_id_); !st.ok()) return st;
  w.WriteU64(cmd.expected_revision_);
  return absl::OkStatus();
}

absl::StatusOr<RetireNode> ReadRetireNodeBody(MetaReader& r) {
  auto header = ReadCommandHeader(r);
  if (!header.ok()) return header.status();
  auto node_id = ReadNodeId(r);
  if (!node_id.ok()) return node_id.status();
  auto expected_revision = r.ReadU64();
  if (!expected_revision.ok()) return expected_revision.status();
  RetireNode cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.node_id_ = std::move(*node_id);
  cmd.expected_revision_ = *expected_revision;
  return cmd;
}

absl::Status WriteCommandBody(MetaWriter& w, const CreateGroup& cmd) {
  if (auto st = WriteCommandHeader(w, MetaCommandTag::kCreateGroup,
                                   cmd.request_id_, cmd.actor_);
      !st.ok()) {
    return st;
  }
  if (auto st = WriteGroupId(w, cmd.group_id_); !st.ok()) return st;
  w.WriteU64(cmd.new_topology_epoch_);
  return absl::OkStatus();
}

absl::StatusOr<CreateGroup> ReadCreateGroupBody(MetaReader& r) {
  auto header = ReadCommandHeader(r);
  if (!header.ok()) return header.status();
  auto group_id = ReadGroupId(r);
  if (!group_id.ok()) return group_id.status();
  auto topology_epoch = r.ReadU64();
  if (!topology_epoch.ok()) return topology_epoch.status();
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
  if (auto st = WriteCommandHeader(w, MetaCommandTag::kAssignNodeToGroup,
                                   cmd.request_id_, cmd.actor_);
      !st.ok()) {
    return st;
  }
  if (auto st = WriteGroupId(w, cmd.group_id_); !st.ok()) return st;
  if (auto st = WriteNodeId(w, cmd.node_id_); !st.ok()) return st;
  WriteFixedArray(w, cmd.assignment_id_);
  if (auto st = WriteRole(w, cmd.role_); !st.ok()) return st;
  w.WriteU64(cmd.expected_revision_);
  w.WriteU64(cmd.new_topology_epoch_);
  return absl::OkStatus();
}

absl::StatusOr<AssignNodeToGroup> ReadAssignNodeToGroupBody(MetaReader& r) {
  auto header = ReadCommandHeader(r);
  if (!header.ok()) return header.status();
  auto group_id = ReadGroupId(r);
  if (!group_id.ok()) return group_id.status();
  auto node_id = ReadNodeId(r);
  if (!node_id.ok()) return node_id.status();
  auto assignment_id = ReadFixedArray<16>(r);
  if (!assignment_id.ok()) return assignment_id.status();
  auto role = ReadRole(r);
  if (!role.ok()) return role.status();
  auto expected_revision = r.ReadU64();
  if (!expected_revision.ok()) return expected_revision.status();
  auto topology_epoch = r.ReadU64();
  if (!topology_epoch.ok()) return topology_epoch.status();
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
  if (auto st = WriteCommandHeader(w, MetaCommandTag::kRemoveNodeFromGroup,
                                   cmd.request_id_, cmd.actor_);
      !st.ok()) {
    return st;
  }
  if (auto st = WriteGroupId(w, cmd.group_id_); !st.ok()) return st;
  if (auto st = WriteNodeId(w, cmd.node_id_); !st.ok()) return st;
  w.WriteU64(cmd.expected_revision_);
  w.WriteU64(cmd.new_topology_epoch_);
  return absl::OkStatus();
}

absl::StatusOr<RemoveNodeFromGroup> ReadRemoveNodeFromGroupBody(MetaReader& r) {
  auto header = ReadCommandHeader(r);
  if (!header.ok()) return header.status();
  auto group_id = ReadGroupId(r);
  if (!group_id.ok()) return group_id.status();
  auto node_id = ReadNodeId(r);
  if (!node_id.ok()) return node_id.status();
  auto expected_revision = r.ReadU64();
  if (!expected_revision.ok()) return expected_revision.status();
  auto topology_epoch = r.ReadU64();
  if (!topology_epoch.ok()) return topology_epoch.status();
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
  if (auto st = CheckCap("ranges", cmd.ranges_.size(), kMaxMetaSlotRangeCount);
      !st.ok()) {
    return st;
  }
  for (const MetaSlotAssignment& range : cmd.ranges_) {
    if (auto st = CheckSlotRange(range); !st.ok()) return st;
    if (auto st = CheckCap("range group_id", range.group_id_.size(),
                           kMaxMetaGroupIdBytes);
        !st.ok()) {
      return st;
    }
  }
  if (auto st = WriteCommandHeader(w, MetaCommandTag::kSetSlotMap,
                                   cmd.request_id_, cmd.actor_);
      !st.ok()) {
    return st;
  }
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
  if (!first.ok()) return first.status();
  auto last = r.ReadU16();
  if (!last.ok()) return last.status();
  if (*first > *last || *last >= kMetaSlotCount) {
    return MetaFailStopError("slot range out of bounds");
  }
  auto group_id = ReadGroupId(r);
  if (!group_id.ok()) return group_id.status();
  return MetaSlotAssignment{*first, *last, std::move(*group_id)};
}

absl::StatusOr<SetSlotMap> ReadSetSlotMapBody(MetaReader& r) {
  auto header = ReadCommandHeader(r);
  if (!header.ok()) return header.status();
  auto ranges = r.ReadList<MetaSlotAssignment>(
      kMaxMetaSlotRangeCount,
      [](MetaReader& rr) { return ReadSlotAssignment(rr); });
  if (!ranges.ok()) return ranges.status();
  auto topology_epoch = r.ReadU64();
  if (!topology_epoch.ok()) return topology_epoch.status();
  SetSlotMap cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.ranges_ = std::move(*ranges);
  cmd.new_topology_epoch_ = *topology_epoch;
  return cmd;
}

absl::Status WriteCommandBody(MetaWriter& w,
                              const SetGroupReplicationState& cmd) {
  if (auto st = WriteCommandHeader(w, MetaCommandTag::kSetGroupReplicationState,
                                   cmd.request_id_, cmd.actor_);
      !st.ok()) {
    return st;
  }
  if (auto st = WriteGroupId(w, cmd.group_id_); !st.ok()) return st;
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
  if (!header.ok()) return header.status();
  auto group_id = ReadGroupId(r);
  if (!group_id.ok()) return group_id.status();
  auto expected_manifest = r.ReadU64();
  if (!expected_manifest.ok()) return expected_manifest.status();
  auto expected_manifest_digest = ReadFixedArray<32>(r);
  if (!expected_manifest_digest.ok()) return expected_manifest_digest.status();
  auto new_manifest = r.ReadU64();
  if (!new_manifest.ok()) return new_manifest.status();
  auto new_manifest_digest = ReadFixedArray<32>(r);
  if (!new_manifest_digest.ok()) return new_manifest_digest.status();
  auto expected_partition = r.ReadU64();
  if (!expected_partition.ok()) return expected_partition.status();
  auto new_partition = r.ReadU64();
  if (!new_partition.ok()) return new_partition.status();
  auto topology_epoch = r.ReadU64();
  if (!topology_epoch.ok()) return topology_epoch.status();
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
  if (auto status = ValidateFailoverCandidate(action.candidate_);
      !status.ok()) {
    return status;
  }
  if (action.operator_recovery_) {
    if (action.domain_ != MetaFailoverCompatibilityDomain{} ||
        (action.authorization_.has_value() &&
         action.authorization_->loss_if_cutover_ !=
             MetaFailoverLoss::kUnknown)) {
      return MetaDomainRejectError(
          "operator recovery cannot claim a source frontier or lossless "
          "cutover");
    }
  } else if (auto status = ValidateFailoverDomain(action.domain_);
             !status.ok()) {
    return status;
  }
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
    if (auto status = ValidateFailoverAction(*transition.candidate_action_);
        !status.ok()) {
      return status;
    }
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

  if (transition.mode_ == MetaFailoverMode::kControlled) {
    if (!transition.controlled_.has_value() ||
        !transition.candidate_action_.has_value()) {
      return MetaDomainRejectError(
          "controlled failover requires operation and candidate action");
    }
    if (IsZero(transition.controlled_->operation_id_)) {
      return MetaDomainRejectError("controlled failover operation id is zero");
    }
    if (auto status = ValidateFailoverDeadline(
            transition.controlled_->absolute_deadline_unix_ms_);
        !status.ok()) {
      return status;
    }
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
  if (!node_id.ok()) return node_id.status();
  auto assignment_id = ReadFixedArray<16>(reader);
  if (!assignment_id.ok()) return assignment_id.status();
  auto boot_id = ReadFixedArray<kMetaBootIncarnationBytes>(reader);
  if (!boot_id.ok()) return boot_id.status();
  MetaFailoverCandidate candidate{std::move(*node_id), *assignment_id,
                                  *boot_id};
  if (auto status =
          FailStopDecodedFailover(ValidateFailoverCandidate(candidate));
      !status.ok()) {
    return status;
  }
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
  if (!source_group_term.ok()) return source_group_term.status();
  auto source_node_id = ReadNodeId(reader);
  if (!source_node_id.ok()) return source_node_id.status();
  auto source_assignment_id = ReadFixedArray<16>(reader);
  if (!source_assignment_id.ok()) return source_assignment_id.status();
  auto source_boot_id = ReadFixedArray<kMetaBootIncarnationBytes>(reader);
  if (!source_boot_id.ok()) return source_boot_id.status();
  auto source_history_id =
      ReadFixedArray<kMetaReplicationHistoryIdBytes>(reader);
  if (!source_history_id.ok()) return source_history_id.status();
  auto flow_count = reader.ReadU32();
  if (!flow_count.ok()) return flow_count.status();
  MetaFailoverCompatibilityDomain domain{
      *source_group_term, std::move(*source_node_id), *source_assignment_id,
      *source_boot_id,    *source_history_id,         *flow_count};
  if (auto status = FailStopDecodedFailover(ValidateFailoverDomain(domain));
      !status.ok()) {
    return status;
  }
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
  if (!revision.ok()) return revision.status();
  auto loss = reader.ReadU8();
  if (!loss.ok()) return loss.status();
  MetaFailoverAuthorization authorization{*revision,
                                          static_cast<MetaFailoverLoss>(*loss)};
  if (auto status =
          FailStopDecodedFailover(ValidateFailoverAuthorization(authorization));
      !status.ok()) {
    return status;
  }
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
  if (!action_id.ok()) return action_id.status();
  auto candidate = ReadFailoverCandidate(reader);
  if (!candidate.ok()) return candidate.status();
  auto operator_recovery = reader.ReadU8();
  if (!operator_recovery.ok()) return operator_recovery.status();
  if (*operator_recovery > 1)
    return FailStopDecodedFailover(
        MetaDomainRejectError("invalid operator recovery flag"));
  absl::StatusOr<MetaFailoverCompatibilityDomain> domain =
      MetaFailoverCompatibilityDomain{};
  if (*operator_recovery == 0) domain = ReadFailoverDomain(reader);
  if (!domain.ok()) return domain.status();
  auto authorization = reader.ReadOptional<MetaFailoverAuthorization>(
      [](MetaReader& nested) { return ReadFailoverAuthorization(nested); });
  if (!authorization.ok()) return authorization.status();
  MetaFailoverCandidateAction action{
      *action_id, std::move(*candidate), std::move(*domain),
      std::move(*authorization), *operator_recovery != 0};
  if (auto status = FailStopDecodedFailover(ValidateFailoverAction(action));
      !status.ok()) {
    return status;
  }
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
  if (!transition_id.ok()) return transition_id.status();
  auto revision = reader.ReadU64();
  if (!revision.ok()) return revision.status();
  MetaFailoverTransitionRef transition{*transition_id, *revision};
  if (auto status =
          FailStopDecodedFailover(ValidateFailoverTransitionRef(transition));
      !status.ok()) {
    return status;
  }
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
}

absl::StatusOr<MetaFailoverTransition> ReadFailoverTransitionUnchecked(
    MetaReader& reader) {
  auto transition_id = ReadFixedArray<16>(reader);
  if (!transition_id.ok()) return transition_id.status();
  auto revision = reader.ReadU64();
  if (!revision.ok()) return revision.status();
  auto mode = reader.ReadU8();
  if (!mode.ok()) return mode.status();
  auto target_term = reader.ReadU64();
  if (!target_term.ok()) return target_term.status();
  auto action = reader.ReadOptional<MetaFailoverCandidateAction>(
      [](MetaReader& nested) { return ReadFailoverAction(nested); });
  if (!action.ok()) return action.status();
  auto controlled = reader.ReadOptional<MetaControlledFailover>(
      [](MetaReader& nested) -> absl::StatusOr<MetaControlledFailover> {
        auto operation_id = ReadFixedArray<16>(nested);
        if (!operation_id.ok()) return operation_id.status();
        auto deadline = nested.ReadU64();
        if (!deadline.ok()) return deadline.status();
        return MetaControlledFailover{*operation_id, *deadline};
      });
  if (!controlled.ok()) return controlled.status();
  return MetaFailoverTransition{
      *transition_id, *revision,          static_cast<MetaFailoverMode>(*mode),
      *target_term,   std::move(*action), std::move(*controlled),
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
  if (!owner_node_id.ok()) return owner_node_id.status();
  auto owner_assignment_id = ReadFixedArray<16>(reader);
  if (!owner_assignment_id.ok()) return owner_assignment_id.status();
  auto membership_revision = reader.ReadU64();
  if (!membership_revision.ok()) return membership_revision.status();
  auto group_term = reader.ReadU64();
  if (!group_term.ok()) return group_term.status();
  auto manifest_revision = reader.ReadU64();
  if (!manifest_revision.ok()) return manifest_revision.status();
  auto manifest_digest = ReadFixedArray<32>(reader);
  if (!manifest_digest.ok()) return manifest_digest.status();
  auto partition_epoch = reader.ReadU64();
  if (!partition_epoch.ok()) return partition_epoch.status();

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
  if (auto status = ValidateFailoverGroupId(command.group_id_); !status.ok()) {
    return status;
  }
  if (IsZero(command.transition_id_)) {
    return MetaDomainRejectError("failover transition id is zero");
  }
  if (auto status = ValidateFailoverGroupAnchors(command); !status.ok()) {
    return status;
  }
  if (command.expected_group_term_ ==
          std::numeric_limits<std::uint64_t>::max() ||
      command.target_term_ != command.expected_group_term_ + 1) {
    return MetaDomainRejectError(
        "failover target term must be exactly expected group term plus one");
  }
  return absl::OkStatus();
}

absl::Status ValidateCommand(const BeginControlledFailover& command) {
  if (auto status = ValidateFailoverBeginBase(command); !status.ok()) {
    return status;
  }
  if (auto status = ValidateFailoverAction(command.candidate_action_);
      !status.ok()) {
    return status;
  }
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
  if (auto status = ValidateFailoverOperation(
          command.operation_id_, command.expected_operation_revision_);
      !status.ok()) {
    return status;
  }
  return ValidateFailoverDeadline(command.absolute_deadline_unix_ms_);
}

absl::Status ValidateCommand(const BeginUncontrolledFailover& command) {
  if (auto status = ValidateFailoverBeginBase(command); !status.ok()) {
    return status;
  }
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
    if (auto status = ValidateFailoverAction(*command.candidate_action_);
        !status.ok()) {
      return status;
    }
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
    if (auto status = ValidateFailoverOperation(
            *command.preempted_operation_id_,
            *command.expected_preempted_operation_revision_);
        !status.ok()) {
      return status;
    }
  }
  return absl::OkStatus();
}

absl::Status ValidateCommand(const SetUncontrolledCandidate& command) {
  if (auto status = ValidateFailoverGroupId(command.group_id_); !status.ok()) {
    return status;
  }
  if (auto status = ValidateFailoverTransitionRef(command.expected_transition_);
      !status.ok()) {
    return status;
  }
  if (command.candidate_action_.has_value()) {
    if (auto status = ValidateFailoverAction(*command.candidate_action_);
        !status.ok()) {
      return status;
    }
    if (command.candidate_action_->authorization_.has_value()) {
      return MetaDomainRejectError(
          "candidate replacement must not carry authorization");
    }
  }
  return absl::OkStatus();
}

absl::Status ValidateCommand(const AuthorizeFailoverPrepare& command) {
  if (auto status = ValidateFailoverGroupId(command.group_id_); !status.ok()) {
    return status;
  }
  if (auto status = ValidateFailoverTransitionRef(command.expected_transition_);
      !status.ok()) {
    return status;
  }
  if (IsZero(command.action_id_) ||
      !IsValidFailoverLoss(command.loss_if_cutover_)) {
    return MetaDomainRejectError("invalid failover authorization command");
  }
  return absl::OkStatus();
}

absl::Status ValidateCommand(const AbortControlledFailover& command) {
  if (auto status = ValidateFailoverOperation(
          command.operation_id_, command.expected_operation_revision_);
      !status.ok()) {
    return status;
  }
  if (auto status = ValidateFailoverGroupId(command.group_id_); !status.ok()) {
    return status;
  }
  if (command.expected_transition_.has_value()) {
    if (auto status =
            ValidateFailoverTransitionRef(*command.expected_transition_);
        !status.ok()) {
      return status;
    }
  }
  return ValidateFailoverReason(command.reason_);
}

absl::Status ValidateCommand(const DegradeControlledFailover& command) {
  if (auto status = ValidateFailoverOperation(
          command.operation_id_, command.expected_operation_revision_);
      !status.ok()) {
    return status;
  }
  if (auto status = ValidateFailoverGroupId(command.group_id_); !status.ok()) {
    return status;
  }
  if (auto status = ValidateFailoverTransitionRef(command.expected_transition_);
      !status.ok()) {
    return status;
  }
  if (command.expected_candidate_action_.has_value()) {
    const MetaFailoverCandidateAction& action =
        *command.expected_candidate_action_;
    if (auto status = ValidateFailoverAction(action); !status.ok()) {
      return status;
    }
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
  if (auto status = ValidateFailoverGroupId(command.group_id_); !status.ok()) {
    return status;
  }
  if (auto status = ValidateFailoverTransitionRef(command.expected_transition_);
      !status.ok()) {
    return status;
  }
  if (IsZero(command.action_id_) || command.authorized_revision_ == 0 ||
      command.authorized_revision_ > command.expected_transition_.revision_) {
    return MetaDomainRejectError("invalid failover commit authorization CAS");
  }
  if (auto status = ValidateFailoverCandidate(command.expected_candidate_);
      !status.ok()) {
    return status;
  }
  if (auto status = ValidateFailoverGroupAnchors(command); !status.ok()) {
    return status;
  }
  if (command.new_topology_epoch_ == 0) {
    return MetaDomainRejectError("invalid failover cutover topology epoch");
  }
  return absl::OkStatus();
}

absl::Status ValidateCommand(const CommitControlledFailover& command) {
  if (auto status = ValidateFailoverCommitBase(command); !status.ok()) {
    return status;
  }
  if (auto status = ValidateFailoverOperation(
          command.operation_id_, command.expected_operation_revision_);
      !status.ok()) {
    return status;
  }
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
  if (auto status = ValidateFailoverCommitBase(command); !status.ok()) {
    return status;
  }
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
  if (!group_id.ok()) return group_id.status();
  auto transition = ReadFailoverTransitionRef(reader);
  if (!transition.ok()) return transition.status();
  auto action_id = ReadFixedArray<16>(reader);
  if (!action_id.ok()) return action_id.status();
  auto authorized_revision = reader.ReadU64();
  if (!authorized_revision.ok()) return authorized_revision.status();
  auto candidate = ReadFailoverCandidate(reader);
  if (!candidate.ok()) return candidate.status();
  command.group_id_ = std::move(*group_id);
  command.expected_transition_ = *transition;
  command.action_id_ = *action_id;
  command.authorized_revision_ = *authorized_revision;
  command.expected_candidate_ = std::move(*candidate);
  if (auto status = ReadFailoverGroupAnchors(reader, command); !status.ok()) {
    return status;
  }
  auto topology_epoch = reader.ReadU64();
  if (!topology_epoch.ok()) return topology_epoch.status();
  command.new_topology_epoch_ = *topology_epoch;
  return absl::OkStatus();
}

absl::Status WriteCommandBody(MetaWriter& writer,
                              const BeginControlledFailover& command) {
  if (auto status = ValidateCommand(command); !status.ok()) return status;
  if (auto status =
          WriteCommandHeader(writer, MetaCommandTag::kBeginControlledFailover,
                             command.request_id_, command.actor_);
      !status.ok()) {
    return status;
  }
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
  if (!header.ok()) return header.status();
  auto group_id = ReadGroupId(reader);
  if (!group_id.ok()) return group_id.status();
  auto transition_id = ReadFixedArray<16>(reader);
  if (!transition_id.ok()) return transition_id.status();
  auto target_term = reader.ReadU64();
  if (!target_term.ok()) return target_term.status();
  auto action = ReadFailoverAction(reader);
  if (!action.ok()) return action.status();
  auto operation_id = ReadFixedArray<16>(reader);
  if (!operation_id.ok()) return operation_id.status();
  auto operation_revision = reader.ReadU64();
  if (!operation_revision.ok()) return operation_revision.status();
  auto deadline = reader.ReadU64();
  if (!deadline.ok()) return deadline.status();
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
  if (auto status = ReadFailoverGroupAnchors(reader, command); !status.ok()) {
    return status;
  }
  if (auto status = FailStopDecodedFailover(ValidateCommand(command));
      !status.ok()) {
    return status;
  }
  return command;
}

absl::Status WriteCommandBody(MetaWriter& writer,
                              const BeginUncontrolledFailover& command) {
  if (auto status = ValidateCommand(command); !status.ok()) return status;
  if (auto status =
          WriteCommandHeader(writer, MetaCommandTag::kBeginUncontrolledFailover,
                             command.request_id_, command.actor_);
      !status.ok()) {
    return status;
  }
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
  if (!header.ok()) return header.status();
  auto group_id = ReadGroupId(reader);
  if (!group_id.ok()) return group_id.status();
  auto transition_id = ReadFixedArray<16>(reader);
  if (!transition_id.ok()) return transition_id.status();
  auto target_term = reader.ReadU64();
  if (!target_term.ok()) return target_term.status();
  auto action = reader.ReadOptional<MetaFailoverCandidateAction>(
      [](MetaReader& nested) { return ReadFailoverAction(nested); });
  if (!action.ok()) return action.status();
  auto trigger_reason = reader.ReadU8();
  if (!trigger_reason.ok()) return trigger_reason.status();
  auto suspect_duration_ms = reader.ReadU64();
  if (!suspect_duration_ms.ok()) return suspect_duration_ms.status();
  using PreemptedOperation = std::pair<MetaOperationId, std::uint64_t>;
  auto preempted_operation = reader.ReadOptional<PreemptedOperation>(
      [](MetaReader& nested) -> absl::StatusOr<PreemptedOperation> {
        auto operation_id = ReadFixedArray<16>(nested);
        if (!operation_id.ok()) return operation_id.status();
        auto expected_revision = nested.ReadU64();
        if (!expected_revision.ok()) return expected_revision.status();
        return PreemptedOperation{*operation_id, *expected_revision};
      });
  if (!preempted_operation.ok()) return preempted_operation.status();
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
  if (auto status = ReadFailoverGroupAnchors(reader, command); !status.ok()) {
    return status;
  }
  if (auto status = FailStopDecodedFailover(ValidateCommand(command));
      !status.ok()) {
    return status;
  }
  return command;
}

absl::Status WriteCommandBody(MetaWriter& writer,
                              const SetUncontrolledCandidate& command) {
  if (auto status = ValidateCommand(command); !status.ok()) return status;
  if (auto status =
          WriteCommandHeader(writer, MetaCommandTag::kSetUncontrolledCandidate,
                             command.request_id_, command.actor_);
      !status.ok()) {
    return status;
  }
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
  if (!header.ok()) return header.status();
  auto group_id = ReadGroupId(reader);
  if (!group_id.ok()) return group_id.status();
  auto transition = ReadFailoverTransitionRef(reader);
  if (!transition.ok()) return transition.status();
  auto action = reader.ReadOptional<MetaFailoverCandidateAction>(
      [](MetaReader& nested) { return ReadFailoverAction(nested); });
  if (!action.ok()) return action.status();
  SetUncontrolledCandidate command;
  command.request_id_ = header->request_id_;
  command.actor_ = std::move(header->actor_);
  command.group_id_ = std::move(*group_id);
  command.expected_transition_ = *transition;
  command.candidate_action_ = std::move(*action);
  if (auto status = FailStopDecodedFailover(ValidateCommand(command));
      !status.ok()) {
    return status;
  }
  return command;
}

absl::Status WriteCommandBody(MetaWriter& writer,
                              const AuthorizeFailoverPrepare& command) {
  if (auto status = ValidateCommand(command); !status.ok()) return status;
  if (auto status =
          WriteCommandHeader(writer, MetaCommandTag::kAuthorizeFailoverPrepare,
                             command.request_id_, command.actor_);
      !status.ok()) {
    return status;
  }
  writer.WriteString(command.group_id_);
  WriteFailoverTransitionRefUnchecked(writer, command.expected_transition_);
  WriteFixedArray(writer, command.action_id_);
  writer.WriteU8(static_cast<std::uint8_t>(command.loss_if_cutover_));
  return absl::OkStatus();
}

absl::StatusOr<AuthorizeFailoverPrepare> ReadAuthorizeFailoverPrepareBody(
    MetaReader& reader) {
  auto header = ReadCommandHeader(reader);
  if (!header.ok()) return header.status();
  auto group_id = ReadGroupId(reader);
  if (!group_id.ok()) return group_id.status();
  auto transition = ReadFailoverTransitionRef(reader);
  if (!transition.ok()) return transition.status();
  auto action_id = ReadFixedArray<16>(reader);
  if (!action_id.ok()) return action_id.status();
  auto loss = reader.ReadU8();
  if (!loss.ok()) return loss.status();
  AuthorizeFailoverPrepare command;
  command.request_id_ = header->request_id_;
  command.actor_ = std::move(header->actor_);
  command.group_id_ = std::move(*group_id);
  command.expected_transition_ = *transition;
  command.action_id_ = *action_id;
  command.loss_if_cutover_ = static_cast<MetaFailoverLoss>(*loss);
  if (auto status = FailStopDecodedFailover(ValidateCommand(command));
      !status.ok()) {
    return status;
  }
  return command;
}

absl::Status WriteCommandBody(MetaWriter& writer,
                              const AbortControlledFailover& command) {
  if (auto status = ValidateCommand(command); !status.ok()) return status;
  if (auto status =
          WriteCommandHeader(writer, MetaCommandTag::kAbortControlledFailover,
                             command.request_id_, command.actor_);
      !status.ok()) {
    return status;
  }
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
  if (!header.ok()) return header.status();
  auto operation_id = ReadFixedArray<16>(reader);
  if (!operation_id.ok()) return operation_id.status();
  auto operation_revision = reader.ReadU64();
  if (!operation_revision.ok()) return operation_revision.status();
  auto group_id = ReadGroupId(reader);
  if (!group_id.ok()) return group_id.status();
  auto transition = reader.ReadOptional<MetaFailoverTransitionRef>(
      [](MetaReader& nested) { return ReadFailoverTransitionRef(nested); });
  if (!transition.ok()) return transition.status();
  auto reason = ReadBoundedString(reader, kMaxMetaAbortReasonBytes);
  if (!reason.ok()) return reason.status();
  AbortControlledFailover command;
  command.request_id_ = header->request_id_;
  command.actor_ = std::move(header->actor_);
  command.operation_id_ = *operation_id;
  command.expected_operation_revision_ = *operation_revision;
  command.group_id_ = std::move(*group_id);
  command.expected_transition_ = std::move(*transition);
  command.reason_ = std::move(*reason);
  if (auto status = FailStopDecodedFailover(ValidateCommand(command));
      !status.ok()) {
    return status;
  }
  return command;
}

absl::Status WriteCommandBody(MetaWriter& writer,
                              const DegradeControlledFailover& command) {
  if (auto status = ValidateCommand(command); !status.ok()) return status;
  if (auto status =
          WriteCommandHeader(writer, MetaCommandTag::kDegradeControlledFailover,
                             command.request_id_, command.actor_);
      !status.ok()) {
    return status;
  }
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
  if (!header.ok()) return header.status();
  auto operation_id = ReadFixedArray<16>(reader);
  if (!operation_id.ok()) return operation_id.status();
  auto operation_revision = reader.ReadU64();
  if (!operation_revision.ok()) return operation_revision.status();
  auto group_id = ReadGroupId(reader);
  if (!group_id.ok()) return group_id.status();
  auto transition = ReadFailoverTransitionRef(reader);
  if (!transition.ok()) return transition.status();
  auto action = reader.ReadOptional<MetaFailoverCandidateAction>(
      [](MetaReader& nested) { return ReadFailoverAction(nested); });
  if (!action.ok()) return action.status();
  auto retain =
      reader.ReadBool("invalid retained failover candidate presence tag");
  if (!retain.ok()) return retain.status();
  auto reason = ReadBoundedString(reader, kMaxMetaAbortReasonBytes);
  if (!reason.ok()) return reason.status();
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
  if (auto status = FailStopDecodedFailover(ValidateCommand(command));
      !status.ok()) {
    return status;
  }
  return command;
}

absl::Status WriteCommandBody(MetaWriter& writer,
                              const CommitControlledFailover& command) {
  if (auto status = ValidateCommand(command); !status.ok()) return status;
  if (auto status =
          WriteCommandHeader(writer, MetaCommandTag::kCommitControlledFailover,
                             command.request_id_, command.actor_);
      !status.ok()) {
    return status;
  }
  WriteFixedArray(writer, command.operation_id_);
  writer.WriteU64(command.expected_operation_revision_);
  WriteFailoverCommitBaseUnchecked(writer, command);
  return absl::OkStatus();
}

absl::StatusOr<CommitControlledFailover> ReadCommitControlledFailoverBody(
    MetaReader& reader) {
  auto header = ReadCommandHeader(reader);
  if (!header.ok()) return header.status();
  auto operation_id = ReadFixedArray<16>(reader);
  if (!operation_id.ok()) return operation_id.status();
  auto operation_revision = reader.ReadU64();
  if (!operation_revision.ok()) return operation_revision.status();
  CommitControlledFailover command;
  command.request_id_ = header->request_id_;
  command.actor_ = std::move(header->actor_);
  command.operation_id_ = *operation_id;
  command.expected_operation_revision_ = *operation_revision;
  if (auto status = ReadFailoverCommitBase(reader, command); !status.ok()) {
    return status;
  }
  if (auto status = FailStopDecodedFailover(ValidateCommand(command));
      !status.ok()) {
    return status;
  }
  return command;
}

absl::Status WriteCommandBody(MetaWriter& writer,
                              const CommitUncontrolledFailover& command) {
  if (auto status = ValidateCommand(command); !status.ok()) return status;
  if (auto status = WriteCommandHeader(
          writer, MetaCommandTag::kCommitUncontrolledFailover,
          command.request_id_, command.actor_);
      !status.ok()) {
    return status;
  }
  WriteFailoverCommitBaseUnchecked(writer, command);
  writer.WriteU8(static_cast<std::uint8_t>(command.loss_if_cutover_));
  return absl::OkStatus();
}

absl::StatusOr<CommitUncontrolledFailover> ReadCommitUncontrolledFailoverBody(
    MetaReader& reader) {
  auto header = ReadCommandHeader(reader);
  if (!header.ok()) return header.status();
  CommitUncontrolledFailover command;
  command.request_id_ = header->request_id_;
  command.actor_ = std::move(header->actor_);
  if (auto status = ReadFailoverCommitBase(reader, command); !status.ok()) {
    return status;
  }
  auto loss = reader.ReadU8();
  if (!loss.ok()) return loss.status();
  command.loss_if_cutover_ = static_cast<MetaFailoverLoss>(*loss);
  if (auto status = FailStopDecodedFailover(ValidateCommand(command));
      !status.ok()) {
    return status;
  }
  return command;
}

// Shared codec for the {group_id, expected_term} command pair.
template <typename Cmd>
absl::StatusOr<Cmd> ReadGroupTermGate(MetaReader& r) {
  auto header = ReadCommandHeader(r);
  if (!header.ok()) return header.status();
  auto group_id = ReadGroupId(r);
  if (!group_id.ok()) return group_id.status();
  auto expected_term = r.ReadU64();
  if (!expected_term.ok()) return expected_term.status();
  Cmd cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.group_id_ = std::move(*group_id);
  cmd.expected_term_ = *expected_term;
  return cmd;
}

absl::Status WriteCommandBody(MetaWriter& w, const BeginGroupTerm& cmd) {
  if (auto st = WriteCommandHeader(w, MetaCommandTag::kBeginGroupTerm,
                                   cmd.request_id_, cmd.actor_);
      !st.ok()) {
    return st;
  }
  if (auto st = WriteGroupId(w, cmd.group_id_); !st.ok()) return st;
  w.WriteU64(cmd.expected_term_);
  w.WriteU64(cmd.new_term_);
  return absl::OkStatus();
}

absl::StatusOr<BeginGroupTerm> ReadBeginGroupTermBody(MetaReader& r) {
  auto cmd = ReadGroupTermGate<BeginGroupTerm>(r);
  if (!cmd.ok()) return cmd.status();
  auto new_term = r.ReadU64();
  if (!new_term.ok()) return new_term.status();
  cmd->new_term_ = *new_term;
  return cmd;
}

absl::Status WriteCommandBody(MetaWriter& w, const ActivateAuthority& cmd) {
  if (auto st = WriteCommandHeader(w, MetaCommandTag::kActivateAuthority,
                                   cmd.request_id_, cmd.actor_);
      !st.ok()) {
    return st;
  }
  if (auto st = WriteGroupId(w, cmd.group_id_); !st.ok()) return st;
  w.WriteU64(cmd.expected_term_);
  if (auto st = WriteNodeId(w, cmd.new_owner_); !st.ok()) return st;
  w.WriteU64(cmd.new_topology_epoch_);
  return absl::OkStatus();
}

absl::StatusOr<ActivateAuthority> ReadActivateAuthorityBody(MetaReader& r) {
  auto header = ReadCommandHeader(r);
  if (!header.ok()) return header.status();
  auto group_id = ReadGroupId(r);
  if (!group_id.ok()) return group_id.status();
  auto expected_term = r.ReadU64();
  if (!expected_term.ok()) return expected_term.status();
  auto new_owner = ReadNodeId(r);
  if (!new_owner.ok()) return new_owner.status();
  auto topology_epoch = r.ReadU64();
  if (!topology_epoch.ok()) return topology_epoch.status();
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
  if (auto st = WriteCommandHeader(w, MetaCommandTag::kFenceGroup,
                                   cmd.request_id_, cmd.actor_);
      !st.ok()) {
    return st;
  }
  if (auto st = WriteGroupId(w, cmd.group_id_); !st.ok()) return st;
  w.WriteU64(cmd.expected_term_);
  w.WriteU64(cmd.new_term_);
  return absl::OkStatus();
}

absl::StatusOr<FenceGroup> ReadFenceGroupBody(MetaReader& r) {
  auto cmd = ReadGroupTermGate<FenceGroup>(r);
  if (!cmd.ok()) return cmd.status();
  auto new_term = r.ReadU64();
  if (!new_term.ok()) return new_term.status();
  cmd->new_term_ = *new_term;
  return cmd;
}

// ---------------------------------------------------------------------------
// policy codecs.
// ---------------------------------------------------------------------------

absl::Status WriteCommandBody(MetaWriter& w, const PutPolicy& cmd) {
  if (auto st =
          CheckCap("policy_id", cmd.policy_id_.size(), kMaxMetaPolicyIdBytes);
      !st.ok()) {
    return st;
  }
  if (auto st = CheckCap("content", cmd.content_.size(), kMaxMetaPayloadBytes);
      !st.ok()) {
    return st;
  }
  if (auto st = WriteCommandHeader(w, MetaCommandTag::kPutPolicy,
                                   cmd.request_id_, cmd.actor_);
      !st.ok()) {
    return st;
  }
  w.WriteString(cmd.policy_id_);
  w.WriteU64(cmd.version_);
  w.WriteString(cmd.content_);
  return absl::OkStatus();
}

absl::StatusOr<PutPolicy> ReadPutPolicyBody(MetaReader& r) {
  auto header = ReadCommandHeader(r);
  if (!header.ok()) return header.status();
  auto policy_id = ReadBoundedString(r, kMaxMetaPolicyIdBytes);
  if (!policy_id.ok()) return policy_id.status();
  auto version = r.ReadU64();
  if (!version.ok()) return version.status();
  auto content = ReadBoundedString(r, kMaxMetaPayloadBytes);
  if (!content.ok()) return content.status();
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
  if (auto st = CheckCap("kind", cmd.kind_.size(), kMaxMetaOperationKindBytes);
      !st.ok()) {
    return st;
  }
  if (auto st = CheckCap("intent", cmd.intent_.size(), kMaxMetaPayloadBytes);
      !st.ok()) {
    return st;
  }
  if (auto st = WriteCommandHeader(w, MetaCommandTag::kSubmitOperation,
                                   cmd.request_id_, cmd.actor_);
      !st.ok()) {
    return st;
  }
  WriteFixedArray(w, cmd.operation_id_);
  w.WriteString(cmd.kind_);
  w.WriteString(cmd.intent_);
  WriteFixedArray(w, cmd.intent_hash_);
  WriteFixedArray(w, cmd.replication_history_id_);
  return absl::OkStatus();
}

absl::StatusOr<SubmitOperation> ReadSubmitOperationBody(MetaReader& r) {
  auto header = ReadCommandHeader(r);
  if (!header.ok()) return header.status();
  auto operation_id = ReadFixedArray<16>(r);
  if (!operation_id.ok()) return operation_id.status();
  auto kind = ReadBoundedString(r, kMaxMetaOperationKindBytes);
  if (!kind.ok()) return kind.status();
  auto intent = ReadBoundedString(r, kMaxMetaPayloadBytes);
  if (!intent.ok()) return intent.status();
  auto intent_hash = ReadFixedArray<32>(r);
  if (!intent_hash.ok()) return intent_hash.status();
  auto replication_history = ReadFixedArray<kMetaReplicationHistoryIdBytes>(r);
  if (!replication_history.ok()) return replication_history.status();
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
  if (auto st = CheckCap("kind_phase_blob", cmd.kind_phase_blob_.size(),
                         kMaxMetaPayloadBytes);
      !st.ok()) {
    return st;
  }
  if (auto st = CheckCap("current_directives", cmd.current_directives_.size(),
                         kMaxMetaDirectivesPerOperation);
      !st.ok()) {
    return st;
  }
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
  if (auto st = WriteCommandHeader(w, MetaCommandTag::kTransitionOperationPhase,
                                   cmd.request_id_, cmd.actor_);
      !st.ok()) {
    return st;
  }
  WriteFixedArray(w, cmd.operation_id_);
  w.WriteU64(cmd.expected_revision_);
  w.WriteString(cmd.kind_phase_blob_);
  w.WriteList(cmd.current_directives_, WriteMetaDirectiveSpec);
  return absl::OkStatus();
}

absl::StatusOr<TransitionOperationPhase> ReadTransitionOperationPhaseBody(
    MetaReader& r) {
  auto header = ReadCommandHeader(r);
  if (!header.ok()) return header.status();
  auto operation_id = ReadFixedArray<16>(r);
  if (!operation_id.ok()) return operation_id.status();
  auto expected_revision = r.ReadU64();
  if (!expected_revision.ok()) return expected_revision.status();
  auto blob = ReadBoundedString(r, kMaxMetaPayloadBytes);
  if (!blob.ok()) return blob.status();
  auto directives = r.ReadList<MetaDirectiveSpec>(
      kMaxMetaDirectivesPerOperation,
      [](MetaReader& reader) { return ReadMetaDirectiveSpec(reader); });
  if (!directives.ok()) return directives.status();
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
  if (!operation_id.ok()) return operation_id.status();
  auto directive_id = ReadFixedArray<16>(r);
  if (!directive_id.ok()) return directive_id.status();
  auto attempt_id = ReadFixedArray<16>(r);
  if (!attempt_id.ok()) return attempt_id.status();
  auto directive_revision = r.ReadU64();
  if (!directive_revision.ok()) return directive_revision.status();
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
  if (auto st = WriteCommandHeader(w, MetaCommandTag::kCommitDirectiveResult,
                                   cmd.request_id_, cmd.actor_);
      !st.ok()) {
    return st;
  }
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
  if (!header.ok()) return header.status();
  auto operation_id = ReadFixedArray<16>(r);
  if (!operation_id.ok()) return operation_id.status();
  auto directive_id = ReadFixedArray<16>(r);
  if (!directive_id.ok()) return directive_id.status();
  auto attempt_id = ReadFixedArray<16>(r);
  if (!attempt_id.ok()) return attempt_id.status();
  auto directive_revision = r.ReadU64();
  if (!directive_revision.ok()) return directive_revision.status();
  auto recipient_node = ReadBoundedString(r, kMetaNodeIdBytes);
  if (!recipient_node.ok()) return recipient_node.status();
  auto recipient_boot = ReadFixedArray<kMetaBootIncarnationBytes>(r);
  if (!recipient_boot.ok()) return recipient_boot.status();
  auto assignment_id = ReadFixedArray<16>(r);
  if (!assignment_id.ok()) return assignment_id.status();
  auto status = r.ReadU8();
  if (!status.ok()) return status.status();
  if (*status <
          static_cast<std::uint8_t>(MetaDirectiveResultStatus::kSucceeded) ||
      *status >
          static_cast<std::uint8_t>(MetaDirectiveResultStatus::kRejected)) {
    return MetaFailStopError("unknown directive result status");
  }
  auto result = ReadBoundedString(r, kMaxMetaPayloadBytes);
  if (!result.ok()) return result.status();

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
  if (auto st = CheckCap("result", cmd.result_.size(), kMaxMetaPayloadBytes);
      !st.ok()) {
    return st;
  }
  if (auto st = WriteCommandHeader(w, MetaCommandTag::kCompleteOperation,
                                   cmd.request_id_, cmd.actor_);
      !st.ok()) {
    return st;
  }
  WriteFixedArray(w, cmd.operation_id_);
  w.WriteU64(cmd.expected_revision_);
  w.WriteString(cmd.result_);
  w.WriteBool(cmd.data_loss_possible_);
  return absl::OkStatus();
}

absl::StatusOr<CompleteOperation> ReadCompleteOperationBody(MetaReader& r) {
  auto header = ReadCommandHeader(r);
  if (!header.ok()) return header.status();
  auto operation_id = ReadFixedArray<16>(r);
  if (!operation_id.ok()) return operation_id.status();
  auto expected_revision = r.ReadU64();
  if (!expected_revision.ok()) return expected_revision.status();
  auto result = ReadBoundedString(r, kMaxMetaPayloadBytes);
  if (!result.ok()) return result.status();
  auto data_loss = r.ReadBool("data_loss_possible must be 0 or 1");
  if (!data_loss.ok()) return data_loss.status();
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
  if (auto st =
          CheckCap("reason", cmd.reason_.size(), kMaxMetaAbortReasonBytes);
      !st.ok()) {
    return st;
  }
  if (auto st = WriteCommandHeader(w, MetaCommandTag::kAbortOperation,
                                   cmd.request_id_, cmd.actor_);
      !st.ok()) {
    return st;
  }
  WriteFixedArray(w, cmd.operation_id_);
  w.WriteU64(cmd.expected_revision_);
  w.WriteString(cmd.reason_);
  return absl::OkStatus();
}

absl::StatusOr<AbortOperation> ReadAbortOperationBody(MetaReader& r) {
  auto header = ReadCommandHeader(r);
  if (!header.ok()) return header.status();
  auto operation_id = ReadFixedArray<16>(r);
  if (!operation_id.ok()) return operation_id.status();
  auto expected_revision = r.ReadU64();
  if (!expected_revision.ok()) return expected_revision.status();
  auto reason = ReadBoundedString(r, kMaxMetaAbortReasonBytes);
  if (!reason.ok()) return reason.status();
  AbortOperation cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.operation_id_ = *operation_id;
  cmd.expected_revision_ = *expected_revision;
  cmd.reason_ = std::move(*reason);
  return cmd;
}

absl::Status WriteCommandBody(MetaWriter& w, const ArchiveOperations& cmd) {
  if (auto st = CheckCap("operation_seqs", cmd.operation_seqs_.size(),
                         kMaxMetaActiveOperations);
      !st.ok()) {
    return st;
  }
  if (auto st = WriteCommandHeader(w, MetaCommandTag::kArchiveOperations,
                                   cmd.request_id_, cmd.actor_);
      !st.ok()) {
    return st;
  }
  w.WriteList(cmd.operation_seqs_,
              [](MetaWriter& ww, std::uint64_t seq) { ww.WriteU64(seq); });
  return absl::OkStatus();
}

absl::StatusOr<ArchiveOperations> ReadArchiveOperationsBody(MetaReader& r) {
  auto header = ReadCommandHeader(r);
  if (!header.ok()) return header.status();
  auto seqs = r.ReadList<std::uint64_t>(
      kMaxMetaActiveOperations, [](MetaReader& rr) { return rr.ReadU64(); });
  if (!seqs.ok()) return seqs.status();
  ArchiveOperations cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.operation_seqs_ = std::move(*seqs);
  return cmd;
}

absl::Status WriteCommandBody(MetaWriter& w, const PruneAudit& cmd) {
  if (auto st = WriteCommandHeader(w, MetaCommandTag::kPruneAudit,
                                   cmd.request_id_, cmd.actor_);
      !st.ok()) {
    return st;
  }
  w.WriteU64(cmd.through_log_index_);
  return absl::OkStatus();
}

absl::StatusOr<PruneAudit> ReadPruneAuditBody(MetaReader& r) {
  auto header = ReadCommandHeader(r);
  if (!header.ok()) return header.status();
  auto through = r.ReadU64();
  if (!through.ok()) return through.status();
  PruneAudit cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.through_log_index_ = *through;
  return cmd;
}

absl::Status WriteCommandBody(MetaWriter& w, const SetAuditPolicy& cmd) {
  if (auto st = CheckCap("attestation", cmd.attestation_.size(),
                         kMaxMetaAttestationBytes);
      !st.ok()) {
    return st;
  }
  if (cmd.policy_ != MetaAuditPolicy::kDisabled &&
      cmd.policy_ != MetaAuditPolicy::kBoundedRotate &&
      cmd.policy_ != MetaAuditPolicy::kStrictExport) {
    return MetaDomainRejectError("unknown audit policy");
  }
  if (auto st = WriteCommandHeader(w, MetaCommandTag::kSetAuditPolicy,
                                   cmd.request_id_, cmd.actor_);
      !st.ok()) {
    return st;
  }
  w.WriteU8(static_cast<std::uint8_t>(cmd.policy_));
  w.WriteString(cmd.attestation_);
  return absl::OkStatus();
}

absl::StatusOr<SetAuditPolicy> ReadSetAuditPolicyBody(MetaReader& r) {
  auto header = ReadCommandHeader(r);
  if (!header.ok()) return header.status();
  auto policy = r.ReadU8();
  if (!policy.ok()) return policy.status();
  if (*policy > static_cast<std::uint8_t>(MetaAuditPolicy::kStrictExport)) {
    return MetaFailStopError("unknown audit policy tag");
  }
  auto attestation = ReadBoundedString(r, kMaxMetaAttestationBytes);
  if (!attestation.ok()) return attestation.status();
  SetAuditPolicy cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.policy_ = static_cast<MetaAuditPolicy>(*policy);
  cmd.attestation_ = std::move(*attestation);
  return cmd;
}

absl::Status WriteCommandBody(MetaWriter& w, const PruneOperationArchive& cmd) {
  if (auto st = CheckCap("operation_seqs", cmd.operation_seqs_.size(),
                         kMaxMetaArchivedOperationSummaries);
      !st.ok()) {
    return st;
  }
  if (auto st = WriteCommandHeader(w, MetaCommandTag::kPruneOperationArchive,
                                   cmd.request_id_, cmd.actor_);
      !st.ok()) {
    return st;
  }
  w.WriteList(cmd.operation_seqs_,
              [](MetaWriter& ww, std::uint64_t seq) { ww.WriteU64(seq); });
  return absl::OkStatus();
}

absl::StatusOr<PruneOperationArchive> ReadPruneOperationArchiveBody(
    MetaReader& r) {
  auto header = ReadCommandHeader(r);
  if (!header.ok()) return header.status();
  auto seqs =
      r.ReadList<std::uint64_t>(kMaxMetaArchivedOperationSummaries,
                                [](MetaReader& rr) { return rr.ReadU64(); });
  if (!seqs.ok()) return seqs.status();
  PruneOperationArchive cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.operation_seqs_ = std::move(*seqs);
  return cmd;
}

absl::Status WriteCommandBody(MetaWriter& w, const PruneTerminalReceipts& cmd) {
  if (auto st = CheckCap("terminal receipts", cmd.receipts_.size(),
                         kMaxMetaTerminalReceiptPrunesPerCommand);
      !st.ok()) {
    return st;
  }
  if (auto st = WriteCommandHeader(w, MetaCommandTag::kPruneTerminalReceipts,
                                   cmd.request_id_, cmd.actor_);
      !st.ok()) {
    return st;
  }
  w.WriteList(cmd.receipts_, WriteTerminalReceiptKey);
  return absl::OkStatus();
}

absl::StatusOr<PruneTerminalReceipts> ReadPruneTerminalReceiptsBody(
    MetaReader& r) {
  auto header = ReadCommandHeader(r);
  if (!header.ok()) return header.status();
  auto receipts = r.ReadList<MetaTerminalReceiptKey>(
      kMaxMetaTerminalReceiptPrunesPerCommand,
      [](MetaReader& reader) { return ReadTerminalReceiptKey(reader); });
  if (!receipts.ok()) return receipts.status();
  PruneTerminalReceipts cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.receipts_ = std::move(*receipts);
  return cmd;
}

absl::Status WriteCommandBody(MetaWriter& w, const BindMetaMember& cmd) {
  if (auto st =
          CheckCap("principal", cmd.principal_.size(), kMaxMetaPrincipalBytes);
      !st.ok()) {
    return st;
  }
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
  if (auto st = WriteCommandHeader(w, MetaCommandTag::kBindMetaMember,
                                   cmd.request_id_, cmd.actor_);
      !st.ok()) {
    return st;
  }
  w.WriteU32(cmd.server_id_);
  w.WriteString(cmd.principal_);
  w.WriteString(cmd.data_control_endpoint_);
  w.WriteBool(cmd.ctl_endpoint_.has_value());
  if (cmd.ctl_endpoint_.has_value()) w.WriteString(*cmd.ctl_endpoint_);
  return absl::OkStatus();
}

absl::StatusOr<BindMetaMember> ReadBindMetaMemberBody(MetaReader& r) {
  auto header = ReadCommandHeader(r);
  if (!header.ok()) return header.status();
  auto server_id = r.ReadU32();
  if (!server_id.ok()) return server_id.status();
  auto principal = ReadBoundedString(r, kMaxMetaPrincipalBytes);
  if (!principal.ok()) return principal.status();
  auto data_control_endpoint = ReadBoundedString(r, kMaxMetaEndpointBytes);
  if (!data_control_endpoint.ok()) return data_control_endpoint.status();
  auto has_ctl_endpoint = r.ReadBool("invalid ctl endpoint presence tag");
  if (!has_ctl_endpoint.ok()) return has_ctl_endpoint.status();
  std::optional<std::string> ctl_endpoint;
  if (*has_ctl_endpoint) {
    auto decoded = ReadBoundedString(r, kMaxMetaEndpointBytes);
    if (!decoded.ok()) return decoded.status();
    ctl_endpoint = std::move(*decoded);
  }
  BindMetaMember cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.server_id_ = *server_id;
  cmd.principal_ = std::move(*principal);
  cmd.data_control_endpoint_ = std::move(*data_control_endpoint);
  cmd.ctl_endpoint_ = std::move(ctl_endpoint);
  return cmd;
}

absl::Status WriteCommandBody(MetaWriter& w, const RetireMetaMember& cmd) {
  if (auto st = WriteCommandHeader(w, MetaCommandTag::kRetireMetaMember,
                                   cmd.request_id_, cmd.actor_);
      !st.ok()) {
    return st;
  }
  w.WriteU32(cmd.server_id_);
  return absl::OkStatus();
}

absl::StatusOr<RetireMetaMember> ReadRetireMetaMemberBody(MetaReader& r) {
  auto header = ReadCommandHeader(r);
  if (!header.ok()) return header.status();
  auto server_id = r.ReadU32();
  if (!server_id.ok()) return server_id.status();
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
  if (auto st = WriteCommandHeader(w, MetaCommandTag::kPutPopulationManifest,
                                   cmd.request_id_, cmd.actor_);
      !st.ok()) {
    return st;
  }
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
  if (!header.ok()) return header.status();
  auto digest = ReadFixedArray<32>(r);
  if (!digest.ok()) return digest.status();
  auto entries = r.ReadList<MetaPopulationManifestEntry>(
      kMetaSlotCount,
      [](MetaReader& reader) -> absl::StatusOr<MetaPopulationManifestEntry> {
        auto partition_id = reader.ReadU32();
        if (!partition_id.ok()) return partition_id.status();
        auto logical_epoch = reader.ReadU64();
        if (!logical_epoch.ok()) return logical_epoch.status();
        return MetaPopulationManifestEntry{*partition_id, *logical_epoch};
      });
  if (!entries.ok()) return entries.status();
  PutPopulationManifest cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.manifest_digest_ = *digest;
  cmd.entries_ = std::move(*entries);
  return cmd;
}

absl::Status WriteCommandBody(MetaWriter& w,
                              const PrunePopulationManifest& cmd) {
  if (auto st = WriteCommandHeader(w, MetaCommandTag::kPrunePopulationManifest,
                                   cmd.request_id_, cmd.actor_);
      !st.ok()) {
    return st;
  }
  WriteFixedArray(w, cmd.manifest_digest_);
  return absl::OkStatus();
}

absl::StatusOr<PrunePopulationManifest> ReadPrunePopulationManifestBody(
    MetaReader& r) {
  auto header = ReadCommandHeader(r);
  if (!header.ok()) return header.status();
  auto digest = ReadFixedArray<32>(r);
  if (!digest.ok()) return digest.status();
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
  if (auto status = ValidateFailoverTransitionImpl(transition); !status.ok()) {
    return status;
  }
  WriteFailoverTransitionUnchecked(writer, transition);
  return absl::OkStatus();
}

absl::StatusOr<MetaFailoverTransition> ReadMetaFailoverTransition(
    MetaReader& reader) {
  auto transition = ReadFailoverTransitionUnchecked(reader);
  if (!transition.ok()) return transition.status();
  if (auto status =
          FailStopDecodedFailover(ValidateFailoverTransitionImpl(*transition));
      !status.ok()) {
    return status;
  }
  return transition;
}

absl::StatusOr<std::string> EncodeMetaCommand(const MetaCommand& command) {
  MetaWriter w;
  w.WriteU16(kMetaCommandFormatVersion);
  auto status = std::visit(
      [&w](const auto& cmd) -> absl::Status {
        return WriteCommandBody(w, cmd);
      },
      command);
  if (!status.ok()) return status;
  if (w.buffer().size() > kMaxMetaCommandBytes) {
    return MetaDomainRejectError("command exceeds kMaxMetaCommandBytes");
  }
  return w.TakeBuffer();
}

MetaCommandTag MetaCommandTagOf(const MetaCommand& command) noexcept {
  static_assert(std::variant_size_v<MetaCommand> == 34);
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
  if (!version.ok()) return version.status();
  if (*version != kMetaCommandFormatVersion) {
    return MetaFailStopError("unknown schema_version");
  }
  auto tag = r.ReadU16();
  if (!tag.ok()) return tag.status();

  MetaCommand command;
  switch (static_cast<MetaCommandTag>(*tag)) {
    case MetaCommandTag::kRegisterNode: {
      auto body = ReadRegisterNodeBody(r);
      if (!body.ok()) return body.status();
      command = std::move(*body);
      break;
    }
    case MetaCommandTag::kUpdateNode: {
      auto body = ReadUpdateNodeBody(r);
      if (!body.ok()) return body.status();
      command = std::move(*body);
      break;
    }
    case MetaCommandTag::kRetireNode: {
      auto body = ReadRetireNodeBody(r);
      if (!body.ok()) return body.status();
      command = std::move(*body);
      break;
    }
    case MetaCommandTag::kCreateGroup: {
      auto body = ReadCreateGroupBody(r);
      if (!body.ok()) return body.status();
      command = std::move(*body);
      break;
    }
    case MetaCommandTag::kAssignNodeToGroup: {
      auto body = ReadAssignNodeToGroupBody(r);
      if (!body.ok()) return body.status();
      command = std::move(*body);
      break;
    }
    case MetaCommandTag::kRemoveNodeFromGroup: {
      auto body = ReadRemoveNodeFromGroupBody(r);
      if (!body.ok()) return body.status();
      command = std::move(*body);
      break;
    }
    case MetaCommandTag::kSetSlotMap: {
      auto body = ReadSetSlotMapBody(r);
      if (!body.ok()) return body.status();
      command = std::move(*body);
      break;
    }
    case MetaCommandTag::kBeginGroupTerm: {
      auto body = ReadBeginGroupTermBody(r);
      if (!body.ok()) return body.status();
      command = std::move(*body);
      break;
    }
    case MetaCommandTag::kActivateAuthority: {
      auto body = ReadActivateAuthorityBody(r);
      if (!body.ok()) return body.status();
      command = std::move(*body);
      break;
    }
    case MetaCommandTag::kFenceGroup: {
      auto body = ReadFenceGroupBody(r);
      if (!body.ok()) return body.status();
      command = std::move(*body);
      break;
    }
    case MetaCommandTag::kPutPolicy: {
      auto body = ReadPutPolicyBody(r);
      if (!body.ok()) return body.status();
      command = std::move(*body);
      break;
    }
    case MetaCommandTag::kSubmitOperation: {
      auto body = ReadSubmitOperationBody(r);
      if (!body.ok()) return body.status();
      command = std::move(*body);
      break;
    }
    case MetaCommandTag::kTransitionOperationPhase: {
      auto body = ReadTransitionOperationPhaseBody(r);
      if (!body.ok()) return body.status();
      command = std::move(*body);
      break;
    }
    case MetaCommandTag::kCompleteOperation: {
      auto body = ReadCompleteOperationBody(r);
      if (!body.ok()) return body.status();
      command = std::move(*body);
      break;
    }
    case MetaCommandTag::kAbortOperation: {
      auto body = ReadAbortOperationBody(r);
      if (!body.ok()) return body.status();
      command = std::move(*body);
      break;
    }
    case MetaCommandTag::kArchiveOperations: {
      auto body = ReadArchiveOperationsBody(r);
      if (!body.ok()) return body.status();
      command = std::move(*body);
      break;
    }
    case MetaCommandTag::kPruneAudit: {
      auto body = ReadPruneAuditBody(r);
      if (!body.ok()) return body.status();
      command = std::move(*body);
      break;
    }
    case MetaCommandTag::kPruneOperationArchive: {
      auto body = ReadPruneOperationArchiveBody(r);
      if (!body.ok()) return body.status();
      command = std::move(*body);
      break;
    }
    case MetaCommandTag::kBindMetaMember: {
      auto body = ReadBindMetaMemberBody(r);
      if (!body.ok()) return body.status();
      command = std::move(*body);
      break;
    }
    case MetaCommandTag::kRetireMetaMember: {
      auto body = ReadRetireMetaMemberBody(r);
      if (!body.ok()) return body.status();
      command = std::move(*body);
      break;
    }
    case MetaCommandTag::kSetGroupReplicationState: {
      auto body = ReadSetGroupReplicationStateBody(r);
      if (!body.ok()) return body.status();
      command = std::move(*body);
      break;
    }
    case MetaCommandTag::kSetAuditPolicy: {
      auto body = ReadSetAuditPolicyBody(r);
      if (!body.ok()) return body.status();
      command = std::move(*body);
      break;
    }
    case MetaCommandTag::kPutPopulationManifest: {
      auto body = ReadPutPopulationManifestBody(r);
      if (!body.ok()) return body.status();
      command = std::move(*body);
      break;
    }
    case MetaCommandTag::kPrunePopulationManifest: {
      auto body = ReadPrunePopulationManifestBody(r);
      if (!body.ok()) return body.status();
      command = std::move(*body);
      break;
    }
    case MetaCommandTag::kCommitDirectiveResult: {
      auto body = ReadCommitDirectiveResultBody(r);
      if (!body.ok()) return body.status();
      command = std::move(*body);
      break;
    }
    case MetaCommandTag::kPruneTerminalReceipts: {
      auto body = ReadPruneTerminalReceiptsBody(r);
      if (!body.ok()) return body.status();
      command = std::move(*body);
      break;
    }
    case MetaCommandTag::kBeginControlledFailover: {
      auto body = ReadBeginControlledFailoverBody(r);
      if (!body.ok()) return body.status();
      command = std::move(*body);
      break;
    }
    case MetaCommandTag::kBeginUncontrolledFailover: {
      auto body = ReadBeginUncontrolledFailoverBody(r);
      if (!body.ok()) return body.status();
      command = std::move(*body);
      break;
    }
    case MetaCommandTag::kSetUncontrolledCandidate: {
      auto body = ReadSetUncontrolledCandidateBody(r);
      if (!body.ok()) return body.status();
      command = std::move(*body);
      break;
    }
    case MetaCommandTag::kAuthorizeFailoverPrepare: {
      auto body = ReadAuthorizeFailoverPrepareBody(r);
      if (!body.ok()) return body.status();
      command = std::move(*body);
      break;
    }
    case MetaCommandTag::kAbortControlledFailover: {
      auto body = ReadAbortControlledFailoverBody(r);
      if (!body.ok()) return body.status();
      command = std::move(*body);
      break;
    }
    case MetaCommandTag::kDegradeControlledFailover: {
      auto body = ReadDegradeControlledFailoverBody(r);
      if (!body.ok()) return body.status();
      command = std::move(*body);
      break;
    }
    case MetaCommandTag::kCommitControlledFailover: {
      auto body = ReadCommitControlledFailoverBody(r);
      if (!body.ok()) return body.status();
      command = std::move(*body);
      break;
    }
    case MetaCommandTag::kCommitUncontrolledFailover: {
      auto body = ReadCommitUncontrolledFailoverBody(r);
      if (!body.ok()) return body.status();
      command = std::move(*body);
      break;
    }
    default:
      return MetaFailStopError("unknown command tag");
  }
  if (auto st = r.Finish(); !st.ok()) return st;
  return command;
}

absl::StatusOr<std::string> EncodeMetaGroupRecord(
    const MetaGroupRecord& record) {
  if (auto st = CheckCap("owner", record.owner_.size(), kMetaNodeIdBytes);
      !st.ok()) {
    return st;
  }
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
  if (!version.ok()) return version.status();
  if (*version != kMetaFormatVersion) {
    return MetaFailStopError("unknown schema_version");
  }
  MetaGroupRecord record;
  auto owner = r.ReadString(kMetaNodeIdBytes);
  if (!owner.ok()) return owner.status();
  record.owner_ = std::string(*owner);
  auto group_term = r.ReadU64();
  if (!group_term.ok()) return group_term.status();
  record.group_term_ = *group_term;
  auto population_manifest_revision = r.ReadU64();
  if (!population_manifest_revision.ok()) {
    return population_manifest_revision.status();
  }
  record.population_manifest_revision_ = *population_manifest_revision;
  auto population_manifest_digest = ReadFixedArray<32>(r);
  if (!population_manifest_digest.ok())
    return population_manifest_digest.status();
  record.population_manifest_digest_ = *population_manifest_digest;
  const bool zero_digest =
      std::all_of(record.population_manifest_digest_.begin(),
                  record.population_manifest_digest_.end(),
                  [](std::uint8_t byte) { return byte == 0; });
  if ((record.population_manifest_revision_ == 0) != zero_digest) {
    return MetaFailStopError("manifest revision/digest invariant violated");
  }
  auto partition_replication_epoch = r.ReadU64();
  if (!partition_replication_epoch.ok()) {
    return partition_replication_epoch.status();
  }
  record.partition_replication_epoch_ = *partition_replication_epoch;
  if (auto st = r.Finish(); !st.ok()) return st;
  return record;
}

}  // namespace lavik::meta
