#include "keylane/meta/commands.h"

#include <algorithm>
#include <utility>
#include <vector>

#include "absl/strings/str_cat.h"
#include "keylane/meta/value_codec.h"

namespace keylane::meta {
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
// its frozen layout has its own header below.
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
  w.WriteU64(cmd.capability_mask_);
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
  auto capability_mask = r.ReadU64();
  if (!capability_mask.ok()) return capability_mask.status();
  auto role = ReadRole(r);
  if (!role.ok()) return role.status();
  RegisterNode cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.node_id_ = std::move(*node_id);
  cmd.principal_ = std::move(*principal);
  cmd.endpoints_ = std::move(*endpoints);
  cmd.capability_mask_ = *capability_mask;
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
  w.WriteU64(cmd.capability_mask_);
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
  auto capability_mask = r.ReadU64();
  if (!capability_mask.ok()) return capability_mask.status();
  auto topology_epoch = r.ReadU64();
  if (!topology_epoch.ok()) return topology_epoch.status();
  UpdateNode cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.node_id_ = std::move(*node_id);
  cmd.expected_revision_ = *expected_revision;
  cmd.endpoints_ = std::move(*endpoints);
  cmd.capability_mask_ = *capability_mask;
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
  if (auto st =
          CheckCap("config_epochs", cmd.config_epochs_.size(), kMaxMetaGroups);
      !st.ok()) {
    return st;
  }
  for (const MetaGroupConfigEpoch& entry : cmd.config_epochs_) {
    if (auto st = CheckCap("config_epoch group_id", entry.group_id_.size(),
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
  w.WriteList(cmd.config_epochs_,
              [](MetaWriter& ww, const MetaGroupConfigEpoch& entry) {
                ww.WriteString(entry.group_id_);
                ww.WriteU64(entry.config_epoch_);
              });
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
  auto config_epochs = r.ReadList<MetaGroupConfigEpoch>(
      kMaxMetaGroups,
      [](MetaReader& rr) -> absl::StatusOr<MetaGroupConfigEpoch> {
        auto group_id = ReadGroupId(rr);
        if (!group_id.ok()) return group_id.status();
        auto epoch = rr.ReadU64();
        if (!epoch.ok()) return epoch.status();
        return MetaGroupConfigEpoch{std::move(*group_id), *epoch};
      });
  if (!config_epochs.ok()) return config_epochs.status();
  SetSlotMap cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.ranges_ = std::move(*ranges);
  cmd.new_topology_epoch_ = *topology_epoch;
  cmd.config_epochs_ = std::move(*config_epochs);
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

// ---------------------------------------------------------------------------
// term/grant codecs.
// ---------------------------------------------------------------------------

absl::Status WriteGrantSpec(MetaWriter& w, const MetaGrantSpec& grant) {
  if (auto st =
          CheckCap("policy_id", grant.policy_id_.size(), kMaxMetaPolicyIdBytes);
      !st.ok()) {
    return st;
  }
  w.WriteU64(grant.lease_duration_ms_);
  w.WriteString(grant.policy_id_);
  w.WriteU64(grant.policy_version_);
  return absl::OkStatus();
}

absl::StatusOr<MetaGrantSpec> ReadGrantSpec(MetaReader& r) {
  auto lease = r.ReadU64();
  if (!lease.ok()) return lease.status();
  auto policy_id = ReadBoundedString(r, kMaxMetaPolicyIdBytes);
  if (!policy_id.ok()) return policy_id.status();
  auto policy_version = r.ReadU64();
  if (!policy_version.ok()) return policy_version.status();
  return MetaGrantSpec{*lease, std::move(*policy_id), *policy_version};
}

// Shared codec for the {group_id, expected_term} command pair.
absl::Status WriteGroupTermGate(MetaWriter& w, MetaCommandTag tag,
                                const MetaRequestId& request_id,
                                const ActorContext& actor,
                                const std::string& group_id,
                                std::uint64_t expected_term) {
  if (auto st = WriteCommandHeader(w, tag, request_id, actor); !st.ok()) {
    return st;
  }
  if (auto st = WriteGroupId(w, group_id); !st.ok()) return st;
  w.WriteU64(expected_term);
  return absl::OkStatus();
}

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

absl::Status WriteCommandBody(MetaWriter& w, const GrantAuthority& cmd) {
  if (auto st = WriteCommandHeader(w, MetaCommandTag::kGrantAuthority,
                                   cmd.request_id_, cmd.actor_);
      !st.ok()) {
    return st;
  }
  if (auto st = WriteGroupId(w, cmd.group_id_); !st.ok()) return st;
  if (auto st = WriteNodeId(w, cmd.node_id_); !st.ok()) return st;
  w.WriteU64(cmd.term_);
  w.WriteU64(cmd.authority_version_);
  return WriteGrantSpec(w, cmd.grant_);
}

absl::StatusOr<GrantAuthority> ReadGrantAuthorityBody(MetaReader& r) {
  auto header = ReadCommandHeader(r);
  if (!header.ok()) return header.status();
  auto group_id = ReadGroupId(r);
  if (!group_id.ok()) return group_id.status();
  auto node_id = ReadNodeId(r);
  if (!node_id.ok()) return node_id.status();
  auto term = r.ReadU64();
  if (!term.ok()) return term.status();
  auto authority_version = r.ReadU64();
  if (!authority_version.ok()) return authority_version.status();
  auto grant = ReadGrantSpec(r);
  if (!grant.ok()) return grant.status();
  GrantAuthority cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.group_id_ = std::move(*group_id);
  cmd.node_id_ = std::move(*node_id);
  cmd.term_ = *term;
  cmd.authority_version_ = *authority_version;
  cmd.grant_ = std::move(*grant);
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
  if (auto st = WriteGrantSpec(w, cmd.grant_); !st.ok()) return st;
  w.WriteU64(cmd.new_authority_version_);
  w.WriteU64(cmd.new_topology_epoch_);
  w.WriteU64(cmd.new_config_epoch_);
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
  auto grant = ReadGrantSpec(r);
  if (!grant.ok()) return grant.status();
  auto authority_version = r.ReadU64();
  if (!authority_version.ok()) return authority_version.status();
  auto topology_epoch = r.ReadU64();
  if (!topology_epoch.ok()) return topology_epoch.status();
  auto config_epoch = r.ReadU64();
  if (!config_epoch.ok()) return config_epoch.status();
  ActivateAuthority cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.group_id_ = std::move(*group_id);
  cmd.expected_term_ = *expected_term;
  cmd.new_owner_ = std::move(*new_owner);
  cmd.grant_ = std::move(*grant);
  cmd.new_authority_version_ = *authority_version;
  cmd.new_topology_epoch_ = *topology_epoch;
  cmd.new_config_epoch_ = *config_epoch;
  return cmd;
}

absl::Status WriteCommandBody(MetaWriter& w, const RevokeGrant& cmd) {
  return WriteGroupTermGate(w, MetaCommandTag::kRevokeGrant, cmd.request_id_,
                            cmd.actor_, cmd.group_id_, cmd.expected_term_);
}

absl::StatusOr<RevokeGrant> ReadRevokeGrantBody(MetaReader& r) {
  return ReadGroupTermGate<RevokeGrant>(r);
}

absl::Status WriteCommandBody(MetaWriter& w, const FenceGroup& cmd) {
  return WriteGroupTermGate(w, MetaCommandTag::kFenceGroup, cmd.request_id_,
                            cmd.actor_, cmd.group_id_, cmd.expected_term_);
}

absl::StatusOr<FenceGroup> ReadFenceGroupBody(MetaReader& r) {
  return ReadGroupTermGate<FenceGroup>(r);
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
  WriteFixedArray(w, cmd.content_hash_);
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
  auto content_hash = ReadFixedArray<32>(r);
  if (!content_hash.ok()) return content_hash.status();
  PutPolicy cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.policy_id_ = std::move(*policy_id);
  cmd.version_ = *version;
  cmd.content_ = std::move(*content);
  cmd.content_hash_ = *content_hash;
  return cmd;
}

absl::Status WriteCommandBody(MetaWriter& w, const RetirePolicy& cmd) {
  if (auto st =
          CheckCap("policy_id", cmd.policy_id_.size(), kMaxMetaPolicyIdBytes);
      !st.ok()) {
    return st;
  }
  if (auto st = WriteCommandHeader(w, MetaCommandTag::kRetirePolicy,
                                   cmd.request_id_, cmd.actor_);
      !st.ok()) {
    return st;
  }
  w.WriteString(cmd.policy_id_);
  w.WriteU64(cmd.version_);
  return absl::OkStatus();
}

absl::StatusOr<RetirePolicy> ReadRetirePolicyBody(MetaReader& r) {
  auto header = ReadCommandHeader(r);
  if (!header.ok()) return header.status();
  auto policy_id = ReadBoundedString(r, kMaxMetaPolicyIdBytes);
  if (!policy_id.ok()) return policy_id.status();
  auto version = r.ReadU64();
  if (!version.ok()) return version.status();
  RetirePolicy cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.policy_id_ = std::move(*policy_id);
  cmd.version_ = *version;
  return cmd;
}

// ---------------------------------------------------------------------------
// operation codecs.
// ---------------------------------------------------------------------------

absl::Status WriteEvidenceSummary(MetaWriter& w,
                                  const MetaEvidenceSummary& ev) {
  if (auto st = CheckCap("node_id", ev.node_id_.size(), kMetaNodeIdBytes);
      !st.ok()) {
    return st;
  }
  if (ev.group_id_.empty()) {
    return MetaDomainRejectError("evidence group_id is empty");
  }
  if (auto st = CheckCap("evidence group_id", ev.group_id_.size(),
                         kMaxMetaGroupIdBytes);
      !st.ok()) {
    return st;
  }
  WriteMetaEvidenceSummary(w, ev);
  return absl::OkStatus();
}

absl::Status WritePolicyReference(MetaWriter& w,
                                  const MetaPolicyReference& reference) {
  if (auto st = CheckCap("operation policy_id", reference.policy_id_.size(),
                         kMaxMetaPolicyIdBytes);
      !st.ok()) {
    return st;
  }
  WriteMetaPolicyReference(w, reference);
  return absl::OkStatus();
}

absl::Status WriteCommandBody(MetaWriter& w, const SubmitOperation& cmd) {
  if (auto st = CheckCap("kind", cmd.kind_.size(), kMaxMetaOperationKindBytes);
      !st.ok()) {
    return st;
  }
  if (auto st = CheckCap("intent", cmd.intent_.size(), kMaxMetaPayloadBytes);
      !st.ok()) {
    return st;
  }
  if (auto st = CheckCap("policy_references", cmd.policy_references_.size(),
                         kMaxMetaPolicyReferencesPerOperation);
      !st.ok()) {
    return st;
  }
  for (const MetaPolicyReference& reference : cmd.policy_references_) {
    if (auto st = CheckCap("operation policy_id", reference.policy_id_.size(),
                           kMaxMetaPolicyIdBytes);
        !st.ok()) {
      return st;
    }
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
  w.WriteList(cmd.policy_references_,
              [](MetaWriter& ww, const MetaPolicyReference& reference) {
                (void)WritePolicyReference(ww, reference);
              });
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
  auto policy_references = r.ReadList<MetaPolicyReference>(
      kMaxMetaPolicyReferencesPerOperation,
      [](MetaReader& rr) { return ReadMetaPolicyReference(rr); });
  if (!policy_references.ok()) return policy_references.status();
  SubmitOperation cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.operation_id_ = *operation_id;
  cmd.kind_ = std::move(*kind);
  cmd.intent_ = std::move(*intent);
  cmd.intent_hash_ = *intent_hash;
  cmd.replication_history_id_ = *replication_history;
  cmd.policy_references_ = std::move(*policy_references);
  return cmd;
}

absl::Status WriteCommandBody(MetaWriter& w,
                              const TransitionOperationPhase& cmd) {
  if (auto st = CheckCap("kind_phase_blob", cmd.kind_phase_blob_.size(),
                         kMaxMetaPayloadBytes);
      !st.ok()) {
    return st;
  }
  if (auto st = CheckCap("evidence", cmd.evidence_.size(),
                         kMaxMetaEvidenceSummariesPerCommand);
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
        directive.payload_.size() > kMaxMetaPayloadBytes ||
        directive.preconditions_.size() > kMaxMetaDirectivePreconditionsBytes) {
      return MetaDomainRejectError("invalid directive field size");
    }
  }
  for (const MetaEvidenceSummary& ev : cmd.evidence_) {
    if (auto st =
            CheckCap("evidence node_id", ev.node_id_.size(), kMetaNodeIdBytes);
        !st.ok()) {
      return st;
    }
    if (ev.group_id_.empty()) {
      return MetaDomainRejectError("evidence group_id is empty");
    }
    if (auto st = CheckCap("evidence group_id", ev.group_id_.size(),
                           kMaxMetaGroupIdBytes);
        !st.ok()) {
      return st;
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
  w.WriteList(cmd.evidence_, [](MetaWriter& ww, const MetaEvidenceSummary& ev) {
    // Element bounds were validated above, so this cannot fail.
    (void)WriteEvidenceSummary(ww, ev);
  });
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
  auto evidence = r.ReadList<MetaEvidenceSummary>(
      kMaxMetaEvidenceSummariesPerCommand,
      [](MetaReader& rr) { return ReadMetaEvidenceSummary(rr); });
  if (!evidence.ok()) return evidence.status();
  TransitionOperationPhase cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.operation_id_ = *operation_id;
  cmd.expected_revision_ = *expected_revision;
  cmd.kind_phase_blob_ = std::move(*blob);
  cmd.current_directives_ = std::move(*directives);
  cmd.evidence_ = std::move(*evidence);
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
  WriteFixedArray(w, cmd.result_hash_);
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
  auto result_hash = ReadFixedArray<32>(r);
  if (!result_hash.ok()) return result_hash.status();
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
  cmd.result_hash_ = *result_hash;
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
  if (auto st = WriteCommandHeader(w, MetaCommandTag::kBindMetaMember,
                                   cmd.request_id_, cmd.actor_);
      !st.ok()) {
    return st;
  }
  w.WriteU32(cmd.server_id_);
  w.WriteString(cmd.principal_);
  w.WriteString(cmd.data_control_endpoint_);
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
  BindMetaMember cmd;
  cmd.request_id_ = header->request_id_;
  cmd.actor_ = std::move(header->actor_);
  cmd.server_id_ = *server_id;
  cmd.principal_ = std::move(*principal);
  cmd.data_control_endpoint_ = std::move(*data_control_endpoint);
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

absl::StatusOr<std::string> EncodeMetaCommand(const MetaCommand& command) {
  MetaWriter w;
  w.WriteU16(kMetaFormatVersion);
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

absl::StatusOr<MetaCommand> DecodeMetaCommand(std::string_view bytes) {
  if (bytes.size() > kMaxMetaCommandBytes) {
    return MetaFailStopError("command exceeds kMaxMetaCommandBytes");
  }
  MetaReader r(bytes);
  auto version = r.ReadU16();
  if (!version.ok()) return version.status();
  if (*version != kMetaFormatVersion) {
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
    case MetaCommandTag::kGrantAuthority: {
      auto body = ReadGrantAuthorityBody(r);
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
    case MetaCommandTag::kRevokeGrant: {
      auto body = ReadRevokeGrantBody(r);
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
    case MetaCommandTag::kRetirePolicy: {
      auto body = ReadRetirePolicyBody(r);
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
  w.WriteU64(record.authority_version_);
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
  auto authority_version = r.ReadU64();
  if (!authority_version.ok()) return authority_version.status();
  record.authority_version_ = *authority_version;
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

}  // namespace keylane::meta
