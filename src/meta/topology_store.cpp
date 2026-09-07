#include "keylane/meta/topology_store.h"

#include <algorithm>
#include <set>

#include "absl/strings/str_cat.h"

namespace keylane::meta {
namespace {

// Field-cap re-validation at the store boundary (commands normally arrive
// via the strict decoder; the store keeps its invariants self-contained).
absl::Status CheckGroupId(const std::string& group_id) {
  if (group_id.empty() || group_id.size() > kMaxMetaGroupIdBytes) {
    return MetaDomainRejectError("group_id empty or over cap");
  }
  return absl::OkStatus();
}

absl::Status CheckNodeId(const std::string& node_id) {
  if (node_id.empty() || node_id.size() > kMetaNodeIdBytes) {
    return MetaDomainRejectError("node_id empty or over cap");
  }
  return absl::OkStatus();
}

// The epoch rule: absolute values, strictly monotonic and gap-free —
// the command must carry exactly current+1. Saturating at u64 max is a
// rejection, never a wrap.
absl::Status CheckNextTopologyEpoch(std::uint64_t current,
                                    std::uint64_t new_epoch) {
  if (current == UINT64_MAX || new_epoch != current + 1) {
    return MetaDomainRejectError(absl::StrCat(
        "new_topology_epoch must be exactly current+1 (", current, ")"));
  }
  return absl::OkStatus();
}

}  // namespace

absl::Status MetaTopologyStore::Apply(const CreateGroup& cmd) {
  if (auto st = CheckGroupId(cmd.group_id_); !st.ok()) return st;
  if (const auto it = groups_.find(cmd.group_id_); it != groups_.end()) {
    // Replay: the group exists exactly as created (never mutated) and the
    // topology epoch already carries this command's value -> idempotent
    // accept. Anything else under this group_id is a conflict.
    const GroupState& group = it->second;
    const bool pristine = group.revision_ == 1 && group.members_.empty() &&
                          group.config_epoch_ == 0 &&
                          group.record_ == MetaGroupRecord{};
    if (pristine && topology_epoch_ == cmd.new_topology_epoch_) {
      return absl::OkStatus();
    }
    return MetaDomainRejectError(
        absl::StrCat("group ", cmd.group_id_, " already exists"));
  }
  if (auto st =
          CheckNextTopologyEpoch(topology_epoch_, cmd.new_topology_epoch_);
      !st.ok()) {
    return st;
  }
  if (groups_.size() >= kMaxMetaGroups) {
    return MetaDomainRejectError("group cap reached");
  }
  GroupState group;
  group.revision_ = 1;
  groups_.emplace(cmd.group_id_, std::move(group));
  topology_epoch_ = cmd.new_topology_epoch_;
  return absl::OkStatus();
}

absl::Status MetaTopologyStore::Apply(const AssignNodeToGroup& cmd) {
  if (auto st = CheckGroupId(cmd.group_id_); !st.ok()) return st;
  if (auto st = CheckNodeId(cmd.node_id_); !st.ok()) return st;
  const auto it = groups_.find(cmd.group_id_);
  if (it == groups_.end()) {
    return MetaDomainRejectError(absl::StrCat("unknown group ", cmd.group_id_));
  }
  GroupState& group = it->second;
  const auto member = group.members_.find(cmd.node_id_);
  // Replay: the member already sits in this group with the same role and the
  // record at the revision this command produces -> idempotent accept.
  if (member != group.members_.end() && member->second == cmd.role_ &&
      group.revision_ == cmd.expected_revision_ + 1 &&
      topology_epoch_ == cmd.new_topology_epoch_) {
    return absl::OkStatus();
  }
  if (group.revision_ != cmd.expected_revision_) {
    return MetaDomainRejectError(
        absl::StrCat("expected_revision CAS conflict on ", cmd.group_id_));
  }
  // One-node-one-group. Cross-store facts (registration, old
  // authority/obligations) are exposed to the apply dispatcher, not checked
  // here.
  if (const auto prior = group_of_node_.find(cmd.node_id_);
      prior != group_of_node_.end()) {
    if (prior->second == cmd.group_id_) {
      return MetaDomainRejectError(absl::StrCat(
          "node already a member of ", cmd.group_id_, " with another role"));
    }
    return MetaDomainRejectError(absl::StrCat(
        "node already a member of ", prior->second, " (one-node-one-group)"));
  }
  if (group.members_.size() >= kMaxMetaNodes) {
    return MetaDomainRejectError("group member cap reached");
  }
  if (auto st =
          CheckNextTopologyEpoch(topology_epoch_, cmd.new_topology_epoch_);
      !st.ok()) {
    return st;
  }
  group.members_.emplace(cmd.node_id_, cmd.role_);
  group_of_node_.emplace(cmd.node_id_, cmd.group_id_);
  group.revision_ = cmd.expected_revision_ + 1;
  topology_epoch_ = cmd.new_topology_epoch_;
  return absl::OkStatus();
}

absl::Status MetaTopologyStore::Apply(const RemoveNodeFromGroup& cmd) {
  if (auto st = CheckGroupId(cmd.group_id_); !st.ok()) return st;
  if (auto st = CheckNodeId(cmd.node_id_); !st.ok()) return st;
  const auto it = groups_.find(cmd.group_id_);
  if (it == groups_.end()) {
    return MetaDomainRejectError(absl::StrCat("unknown group ", cmd.group_id_));
  }
  GroupState& group = it->second;
  const auto member = group.members_.find(cmd.node_id_);
  // Replay: the member is already gone and the record sits at the revision
  // this command produces -> idempotent accept.
  if (member == group.members_.end() &&
      group.revision_ == cmd.expected_revision_ + 1 &&
      topology_epoch_ == cmd.new_topology_epoch_) {
    return absl::OkStatus();
  }
  if (group.revision_ != cmd.expected_revision_) {
    return MetaDomainRejectError(
        absl::StrCat("expected_revision CAS conflict on ", cmd.group_id_));
  }
  if (member == group.members_.end()) {
    return MetaDomainRejectError(
        absl::StrCat("node not a member of ", cmd.group_id_));
  }
  if (auto st =
          CheckNextTopologyEpoch(topology_epoch_, cmd.new_topology_epoch_);
      !st.ok()) {
    return st;
  }
  // No cascade: if the removed node is the record owner, owner_ is left
  // untouched; the apply dispatcher reads the fact and decides.
  group.members_.erase(member);
  group_of_node_.erase(cmd.node_id_);
  group.revision_ = cmd.expected_revision_ + 1;
  topology_epoch_ = cmd.new_topology_epoch_;
  return absl::OkStatus();
}

absl::Status MetaTopologyStore::Apply(const SetSlotMap& cmd) {
  if (cmd.ranges_.size() > kMaxMetaSlotRangeCount ||
      cmd.config_epochs_.size() > kMaxMetaGroups) {
    return MetaDomainRejectError("slot map field count over cap");
  }
  // Structural re-validation (the codec enforces it on the wire; the store
  // keeps its own invariants): in-bounds ranges, non-overlapping, and every
  // referenced group known.
  for (const MetaSlotAssignment& range : cmd.ranges_) {
    if (range.first_slot_ > range.last_slot_ ||
        range.last_slot_ >= kMetaSlotCount) {
      return MetaDomainRejectError("slot range out of bounds");
    }
    if (auto st = CheckGroupId(range.group_id_); !st.ok()) return st;
  }
  {
    std::vector<MetaSlotAssignment> sorted = cmd.ranges_;
    std::sort(sorted.begin(), sorted.end(),
              [](const MetaSlotAssignment& a, const MetaSlotAssignment& b) {
                return a.first_slot_ < b.first_slot_;
              });
    for (std::size_t i = 1; i < sorted.size(); ++i) {
      if (sorted[i].first_slot_ <= sorted[i - 1].last_slot_) {
        return MetaDomainRejectError("overlapping slot ranges");
      }
    }
  }
  for (const MetaGroupConfigEpoch& entry : cmd.config_epochs_) {
    if (auto st = CheckGroupId(entry.group_id_); !st.ok()) return st;
  }

  // Replay: slot map, topology epoch, and every listed config
  // epoch already carry this command's effect -> no-op accept.
  if (topology_epoch_ == cmd.new_topology_epoch_) {
    std::array<std::string, kMetaSlotCount> target;
    for (const MetaSlotAssignment& range : cmd.ranges_) {
      for (std::uint32_t slot = range.first_slot_; slot <= range.last_slot_;
           ++slot) {
        target[slot] = range.group_id_;
      }
    }
    if (target == slots_) {
      const bool config_epochs_match =
          std::all_of(cmd.config_epochs_.begin(), cmd.config_epochs_.end(),
                      [this](const MetaGroupConfigEpoch& entry) {
                        const auto it = groups_.find(entry.group_id_);
                        return it != groups_.end() &&
                               it->second.config_epoch_ == entry.config_epoch_;
                      });
      if (config_epochs_match) return absl::OkStatus();
    }
  }

  if (auto st =
          CheckNextTopologyEpoch(topology_epoch_, cmd.new_topology_epoch_);
      !st.ok()) {
    return st;
  }
  for (const MetaSlotAssignment& range : cmd.ranges_) {
    if (!groups_.contains(range.group_id_)) {
      return MetaDomainRejectError(absl::StrCat(
          "slot range references unknown group ", range.group_id_));
    }
  }
  {
    std::set<std::string> seen;
    for (const MetaGroupConfigEpoch& entry : cmd.config_epochs_) {
      if (!groups_.contains(entry.group_id_)) {
        return MetaDomainRejectError(absl::StrCat(
            "config_epoch references unknown group ", entry.group_id_));
      }
      if (!seen.insert(entry.group_id_).second) {
        return MetaDomainRejectError(
            absl::StrCat("duplicate config_epoch entry for ", entry.group_id_));
      }
    }
  }

  // Absolute replacement: the whole map is rewritten from the ranges.
  slots_.fill(std::string());
  for (const MetaSlotAssignment& range : cmd.ranges_) {
    for (std::uint32_t slot = range.first_slot_; slot <= range.last_slot_;
         ++slot) {
      slots_[slot] = range.group_id_;
    }
  }
  for (const MetaGroupConfigEpoch& entry : cmd.config_epochs_) {
    groups_.find(entry.group_id_)->second.config_epoch_ = entry.config_epoch_;
  }
  topology_epoch_ = cmd.new_topology_epoch_;
  return absl::OkStatus();
}

absl::Status MetaTopologyStore::Apply(const SetGroupReplicationState& cmd) {
  if (auto st = CheckGroupId(cmd.group_id_); !st.ok()) return st;
  const auto it = groups_.find(cmd.group_id_);
  if (it == groups_.end()) {
    return MetaDomainRejectError(absl::StrCat("unknown group ", cmd.group_id_));
  }
  GroupState& group = it->second;
  const MetaGroupRecord& record = group.record_;
  if (record.population_manifest_id_ == cmd.new_population_manifest_id_ &&
      record.partition_replication_epoch_ ==
          cmd.new_partition_replication_epoch_ &&
      topology_epoch_ == cmd.new_topology_epoch_) {
    return absl::OkStatus();
  }
  if (record.population_manifest_id_ != cmd.expected_population_manifest_id_ ||
      record.partition_replication_epoch_ !=
          cmd.expected_partition_replication_epoch_) {
    return MetaDomainRejectError("group replication-state CAS conflict");
  }
  const auto advances_by_at_most_one = [](std::uint64_t expected,
                                          std::uint64_t next) {
    return next == expected || (expected != UINT64_MAX && next == expected + 1);
  };
  if (!advances_by_at_most_one(cmd.expected_population_manifest_id_,
                               cmd.new_population_manifest_id_) ||
      !advances_by_at_most_one(cmd.expected_partition_replication_epoch_,
                               cmd.new_partition_replication_epoch_)) {
    return MetaDomainRejectError(
        "group replication fields must stay unchanged or advance by one");
  }
  if (cmd.new_population_manifest_id_ == cmd.expected_population_manifest_id_ &&
      cmd.new_partition_replication_epoch_ ==
          cmd.expected_partition_replication_epoch_) {
    return MetaDomainRejectError("group replication update has no effect");
  }
  if (auto st =
          CheckNextTopologyEpoch(topology_epoch_, cmd.new_topology_epoch_);
      !st.ok()) {
    return st;
  }
  group.record_.population_manifest_id_ = cmd.new_population_manifest_id_;
  group.record_.partition_replication_epoch_ =
      cmd.new_partition_replication_epoch_;
  topology_epoch_ = cmd.new_topology_epoch_;
  return absl::OkStatus();
}

absl::Status MetaTopologyStore::SetOwner(const std::string& group_id,
                                         const std::string& new_owner) {
  if (new_owner.size() > kMetaNodeIdBytes) {
    return MetaDomainRejectError("owner node_id over cap");
  }
  const auto it = groups_.find(group_id);
  if (it == groups_.end()) {
    return MetaDomainRejectError(absl::StrCat("unknown group ", group_id));
  }
  it->second.record_.owner_ = new_owner;  // absolute; same value = no-op
  return absl::OkStatus();
}

absl::Status MetaTopologyStore::SetGroupTerm(const std::string& group_id,
                                             std::uint64_t term) {
  const auto it = groups_.find(group_id);
  if (it == groups_.end()) {
    return MetaDomainRejectError(absl::StrCat("unknown group ", group_id));
  }
  it->second.record_.group_term_ = term;
  return absl::OkStatus();
}

absl::Status MetaTopologyStore::SetAuthorityVersion(
    const std::string& group_id, std::uint64_t authority_version) {
  const auto it = groups_.find(group_id);
  if (it == groups_.end()) {
    return MetaDomainRejectError(absl::StrCat("unknown group ", group_id));
  }
  it->second.record_.authority_version_ = authority_version;
  return absl::OkStatus();
}

absl::Status MetaTopologyStore::SetPopulationManifestId(
    const std::string& group_id, std::uint64_t manifest_id) {
  const auto it = groups_.find(group_id);
  if (it == groups_.end()) {
    return MetaDomainRejectError(absl::StrCat("unknown group ", group_id));
  }
  it->second.record_.population_manifest_id_ = manifest_id;
  return absl::OkStatus();
}

absl::Status MetaTopologyStore::SetPartitionReplicationEpoch(
    const std::string& group_id, std::uint64_t epoch) {
  const auto it = groups_.find(group_id);
  if (it == groups_.end()) {
    return MetaDomainRejectError(absl::StrCat("unknown group ", group_id));
  }
  it->second.record_.partition_replication_epoch_ = epoch;
  return absl::OkStatus();
}

absl::Status MetaTopologyStore::SetGroupConfigEpoch(
    const std::string& group_id, std::uint64_t config_epoch) {
  const auto it = groups_.find(group_id);
  if (it == groups_.end()) {
    return MetaDomainRejectError(absl::StrCat("unknown group ", group_id));
  }
  it->second.config_epoch_ = config_epoch;
  return absl::OkStatus();
}

absl::Status MetaTopologyStore::SetTopologyEpoch(
    std::uint64_t new_topology_epoch) {
  // Same value already held: idempotent no-op accept.
  if (new_topology_epoch == topology_epoch_) return absl::OkStatus();
  if (auto st = CheckNextTopologyEpoch(topology_epoch_, new_topology_epoch);
      !st.ok()) {
    return st;
  }
  topology_epoch_ = new_topology_epoch;
  return absl::OkStatus();
}

absl::Status MetaTopologyStore::ValidateTopologyEpoch(
    std::uint64_t new_topology_epoch) const {
  if (new_topology_epoch == topology_epoch_) return absl::OkStatus();
  return CheckNextTopologyEpoch(topology_epoch_, new_topology_epoch);
}

std::optional<MetaTopologyGroupView> MetaTopologyStore::FindGroup(
    const std::string& group_id) const {
  const auto it = groups_.find(group_id);
  if (it == groups_.end()) return std::nullopt;
  const GroupState& group = it->second;
  MetaTopologyGroupView view;
  view.group_id_ = group_id;
  view.record_ = group.record_;
  view.config_epoch_ = group.config_epoch_;
  view.revision_ = group.revision_;
  view.members_.reserve(group.members_.size());
  for (const auto& [node_id, role] : group.members_) {
    view.members_.push_back(MetaGroupMember{node_id, role});
  }
  return view;
}

std::optional<std::string> MetaTopologyStore::FindGroupOfNode(
    const std::string& node_id) const {
  const auto it = group_of_node_.find(node_id);
  if (it == group_of_node_.end()) return std::nullopt;
  return it->second;
}

std::optional<std::string> MetaTopologyStore::SlotOwner(
    std::uint32_t slot) const {
  if (slot >= kMetaSlotCount) return std::nullopt;
  if (slots_[slot].empty()) return std::nullopt;
  return slots_[slot];
}

bool MetaTopologyStore::GroupExists(const std::string& group_id) const {
  return groups_.contains(group_id);
}

// Envelope: schema_version u16 | topology_epoch u64 | group count u32 |
// sorted group records | slot run count u32 | sorted runs. See the header
// for the convention and the strictness contract.
std::string MetaTopologyStore::Serialize() const {
  MetaWriter w;
  w.WriteU16(kMetaFormatVersion);
  w.WriteU64(topology_epoch_);
  w.WriteCount(static_cast<std::uint32_t>(groups_.size()));
  for (const auto& [group_id, group] : groups_) {
    w.WriteString(group_id);
    w.WriteString(group.record_.owner_);
    w.WriteU64(group.record_.group_term_);
    w.WriteU64(group.record_.authority_version_);
    w.WriteU64(group.record_.population_manifest_id_);
    w.WriteU64(group.record_.partition_replication_epoch_);
    w.WriteU64(group.config_epoch_);
    w.WriteU64(group.revision_);
    w.WriteCount(static_cast<std::uint32_t>(group.members_.size()));
    for (const auto& [node_id, role] : group.members_) {
      w.WriteString(node_id);
      w.WriteU8(static_cast<std::uint8_t>(role));
    }
  }
  // The slot map as maximal runs of consecutive slots owned by one group.
  // Two passes over the fixed 16384-entry array: count, then emit.
  std::uint32_t run_count = 0;
  for (std::uint32_t slot = 0; slot < kMetaSlotCount; ++slot) {
    if (!slots_[slot].empty() &&
        (slot == 0 || slots_[slot - 1] != slots_[slot])) {
      ++run_count;
    }
  }
  w.WriteCount(run_count);
  for (std::uint32_t slot = 0; slot < kMetaSlotCount; ++slot) {
    if (slots_[slot].empty()) continue;
    if (slot > 0 && slots_[slot - 1] == slots_[slot]) continue;
    std::uint32_t last = slot;
    while (last + 1 < kMetaSlotCount && slots_[last + 1] == slots_[slot]) {
      ++last;
    }
    w.WriteU16(static_cast<std::uint16_t>(slot));
    w.WriteU16(static_cast<std::uint16_t>(last));
    w.WriteString(slots_[slot]);
  }
  return w.TakeBuffer();
}

absl::StatusOr<MetaTopologyStore> MetaTopologyStore::Deserialize(
    std::string_view bytes) {
  MetaReader r(bytes);
  auto version = r.ReadU16();
  if (!version.ok()) return version.status();
  if (*version != kMetaFormatVersion) {
    return MetaFailStopError("unknown schema_version");
  }
  auto topology_epoch = r.ReadU64();
  if (!topology_epoch.ok()) return topology_epoch.status();
  auto group_count = r.ReadCount(kMaxMetaGroups);
  if (!group_count.ok()) return group_count.status();

  MetaTopologyStore store;
  store.topology_epoch_ = *topology_epoch;
  for (std::uint32_t i = 0; i < *group_count; ++i) {
    auto group_id = r.ReadString(kMaxMetaGroupIdBytes);
    if (!group_id.ok()) return group_id.status();
    auto owner = r.ReadString(kMetaNodeIdBytes);
    if (!owner.ok()) return owner.status();
    auto group_term = r.ReadU64();
    if (!group_term.ok()) return group_term.status();
    auto authority_version = r.ReadU64();
    if (!authority_version.ok()) return authority_version.status();
    auto manifest_id = r.ReadU64();
    if (!manifest_id.ok()) return manifest_id.status();
    auto partition_epoch = r.ReadU64();
    if (!partition_epoch.ok()) return partition_epoch.status();
    auto config_epoch = r.ReadU64();
    if (!config_epoch.ok()) return config_epoch.status();
    auto revision = r.ReadU64();
    if (!revision.ok()) return revision.status();
    auto members = r.ReadList<MetaGroupMember>(
        kMaxMetaNodes, [](MetaReader& rr) -> absl::StatusOr<MetaGroupMember> {
          auto node_id = rr.ReadString(kMetaNodeIdBytes);
          if (!node_id.ok()) return node_id.status();
          auto role = rr.ReadU8();
          if (!role.ok()) return role.status();
          if (*role != static_cast<std::uint8_t>(MetaNodeRole::kPrimary) &&
              *role != static_cast<std::uint8_t>(MetaNodeRole::kReplica)) {
            return MetaFailStopError("unknown node role");
          }
          return MetaGroupMember{std::string(*node_id),
                                 static_cast<MetaNodeRole>(*role)};
        });
    if (!members.ok()) return members.status();

    // Invariant enforcement (fail-stop): a corrupt snapshot fails
    // identically on every node.
    if (group_id->empty()) {
      return MetaFailStopError("empty group_id in snapshot");
    }
    if (*revision == 0) {
      return MetaFailStopError("revision 0 in snapshot");
    }
    if (store.groups_.contains(std::string(*group_id))) {
      return MetaFailStopError("duplicate group_id in snapshot");
    }
    GroupState group;
    group.record_.owner_ = std::string(*owner);
    group.record_.group_term_ = *group_term;
    group.record_.authority_version_ = *authority_version;
    group.record_.population_manifest_id_ = *manifest_id;
    group.record_.partition_replication_epoch_ = *partition_epoch;
    group.config_epoch_ = *config_epoch;
    group.revision_ = *revision;
    for (const MetaGroupMember& member : *members) {
      if (member.node_id_.empty()) {
        return MetaFailStopError("empty member node_id in snapshot");
      }
      if (!group.members_.emplace(member.node_id_, member.role_).second) {
        return MetaFailStopError("duplicate member in snapshot");
      }
      // One-node-one-group must hold in the decoded state too.
      if (!store.group_of_node_.emplace(member.node_id_, std::string(*group_id))
               .second) {
        return MetaFailStopError("node in two groups in snapshot");
      }
    }
    store.groups_.emplace(std::string(*group_id), std::move(group));
  }

  auto runs = r.ReadList<MetaSlotAssignment>(
      kMetaSlotCount, [](MetaReader& rr) -> absl::StatusOr<MetaSlotAssignment> {
        auto first = rr.ReadU16();
        if (!first.ok()) return first.status();
        auto last = rr.ReadU16();
        if (!last.ok()) return last.status();
        auto group_id = rr.ReadString(kMaxMetaGroupIdBytes);
        if (!group_id.ok()) return group_id.status();
        if (*first > *last || *last >= kMetaSlotCount) {
          return MetaFailStopError("slot run out of bounds");
        }
        return MetaSlotAssignment{static_cast<std::uint16_t>(*first),
                                  static_cast<std::uint16_t>(*last),
                                  std::string(*group_id)};
      });
  if (!runs.ok()) return runs.status();
  std::uint32_t previous_last = 0;
  for (std::size_t i = 0; i < runs->size(); ++i) {
    const MetaSlotAssignment& run = (*runs)[i];
    // Runs must be strictly ascending; this also rejects overlaps.
    if (i > 0 && run.first_slot_ <= previous_last) {
      return MetaFailStopError("overlapping or unsorted slot runs");
    }
    previous_last = run.last_slot_;
    if (!store.groups_.contains(run.group_id_)) {
      return MetaFailStopError("slot run references unknown group");
    }
    for (std::uint32_t slot = run.first_slot_; slot <= run.last_slot_; ++slot) {
      store.slots_[slot] = run.group_id_;
    }
  }
  if (auto st = r.Finish(); !st.ok()) return st;
  return store;
}

}  // namespace keylane::meta
