#include "keylane/meta/state_apply.h"

#include <algorithm>
#include <array>
#include <limits>
#include <set>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>

#include "absl/strings/str_cat.h"
#include "keylane/meta/control_projector.h"

namespace keylane::meta {
namespace {

// ---------------------------------------------------------------------------
// Dispatch plumbing. Every command yields an ApplyOutcome: the verdict, the
// rejection detail (empty on accept), and the deterministic audit summary.
// ---------------------------------------------------------------------------

struct ApplyOutcome {
  MetaAuditVerdict verdict_ = MetaAuditVerdict::kAccepted;
  std::string detail_;
  std::string summary_;
};

ApplyOutcome Accepted(std::string summary) {
  return ApplyOutcome{MetaAuditVerdict::kAccepted, "", std::move(summary)};
}

// Store rejections are always the kDomainReject class (encoding.h); the
// message becomes the audit record's verdict detail verbatim.
ApplyOutcome Rejected(const absl::Status& status, std::string summary) {
  return ApplyOutcome{MetaAuditVerdict::kRejected,
                      std::string(status.message()), std::move(summary)};
}

ApplyOutcome Rejected(std::string detail, std::string summary) {
  return ApplyOutcome{MetaAuditVerdict::kRejected, std::move(detail),
                      std::move(summary)};
}

ApplyOutcome FromStatus(const absl::Status& status, std::string summary) {
  if (status.ok()) return Accepted(std::move(summary));
  return Rejected(status, std::move(summary));
}

std::string_view RoleName(MetaNodeRole role) {
  return role == MetaNodeRole::kPrimary ? "primary" : "replica";
}

// Lowercase hex, for fixed-size binary ids/hashes in audit summaries.
std::string HexBytes(const std::uint8_t* data, std::size_t size) {
  static constexpr char kHex[] = "0123456789abcdef";
  std::string out;
  out.reserve(size * 2);
  for (std::size_t i = 0; i < size; ++i) {
    out.push_back(kHex[data[i] >> 4]);
    out.push_back(kHex[data[i] & 0xF]);
  }
  return out;
}

template <std::size_t N>
std::string HexBytes(const std::array<std::uint8_t, N>& a) {
  return HexBytes(a.data(), N);
}

// ---------------------------------------------------------------------------
// Cross-store fact helpers (the stores expose per-key fact queries; these
// compose them).
// ---------------------------------------------------------------------------

bool IsMember(const MetaTopologyGroupView& view, const std::string& node_id) {
  for (const MetaGroupMember& member : view.members_) {
    if (member.node_id_ == node_id) return true;
  }
  return false;
}

bool HasAssignment(const MetaTopologyGroupView& view,
                   const std::string& node_id,
                   const MetaAssignmentId& assignment_id) {
  for (const MetaGroupMember& member : view.members_) {
    if (member.node_id_ == node_id && member.assignment_id_ == assignment_id) {
      return true;
    }
  }
  return false;
}

absl::Status ValidateCommittedDirectiveAnchorImpl(
    const MetaStores& stores, const MetaDirectiveSpec& directive) {
  if (!stores.identity_.IsActiveNode(directive.recipient_node_id_) ||
      !stores.identity_.IsActiveNode(directive.target_node_id_) ||
      !stores.identity_.IsActiveNode(directive.source_node_id_)) {
    return MetaDomainRejectError(
        "directive recipient, source, or target is not active");
  }
  const auto group = stores.topology_.FindGroup(directive.group_id_);
  const auto grant = stores.grant_.GroupState(directive.group_id_);
  if (!group.has_value() || !grant.has_value() || grant->fenced_ ||
      !grant->grant_.has_value()) {
    return MetaDomainRejectError("directive group has no active authority");
  }
  if (!HasAssignment(*group, directive.target_node_id_,
                     directive.assignment_id_) ||
      !HasAssignment(*group, directive.source_node_id_,
                     directive.source_assignment_id_)) {
    return MetaDomainRejectError("directive membership or assignment is stale");
  }
  if (group->record_.group_term_ != directive.group_term_ ||
      group->record_.authority_version_ != directive.authority_version_ ||
      grant->grant_->term_ != directive.group_term_ ||
      grant->grant_->authority_version_ != directive.authority_version_ ||
      grant->grant_->grant_revision_ != directive.grant_revision_) {
    return MetaDomainRejectError("directive authority anchor is stale");
  }
  if (group->record_.population_manifest_revision_ !=
          directive.population_manifest_revision_ ||
      group->record_.population_manifest_digest_ !=
          directive.population_manifest_digest_ ||
      group->record_.partition_replication_epoch_ !=
          directive.partition_replication_epoch_) {
    return MetaDomainRejectError("directive population identity is stale");
  }
  return absl::OkStatus();
}

void InvalidateStaleCurrentDirectives(MetaStores& stores) {
  std::vector<MetaTerminalReceiptKey> invalidated;
  for (const MetaOperationRecord& operation :
       stores.operation_.LiveOperations()) {
    for (const MetaCurrentDirective& current : operation.current_directives_) {
      if (ValidateCommittedDirectiveAnchorImpl(stores, current.spec_).ok()) {
        continue;
      }
      invalidated.push_back(MetaTerminalReceiptKey{
          operation.operation_id_, current.spec_.directive_id_,
          current.spec_.attempt_id_, current.directive_revision_});
    }
  }
  stores.operation_.InvalidateCurrentDirectives(invalidated);
}

// A PutPopulationManifest is the only unbounded-history insertion into the
// content-addressed store. Charge it against the exact bytes currently used
// by all seven snapshot blobs, with room for the audit record ApplyCommitted
// appends after dispatch. This is an abuse ceiling tied to the durable format,
// not a workload-sizing guess.
absl::StatusOr<std::uint64_t> SnapshotBytesWithPopulationManifest(
    const MetaStores& stores,
    const MetaPopulationManifestStore& population_manifest) {
  const std::string identity = stores.identity_.Serialize();
  const std::string topology = stores.topology_.Serialize();
  const std::string policy = stores.policy_.Serialize();
  const auto grant = stores.grant_.Serialize();
  if (!grant.ok()) return grant.status();
  const auto operation = stores.operation_.Serialize();
  if (!operation.ok()) return operation.status();
  const std::string population = population_manifest.Serialize();
  const auto audit = stores.audit_.Serialize();
  if (!audit.ok()) return audit.status();

  // Aggregate schema u16 plus seven u32 length prefixes.
  return 2u + (7u * 4u) + identity.size() + topology.size() + policy.size() +
         grant->size() + operation->size() + population.size() + audit->size();
}

constexpr std::uint64_t kMaximumAuditSnapshotGrowth =
    8u + (4u + kMaxMetaPrincipalBytes) + (4u + kMaxMetaAuditSummaryBytes) + 1u +
    (4u + kMaxMetaAuditDetailBytes) + (4u + kMaxMetaAuditReadableTimeBytes) +
    32u;

// Store decoders validate their own representation, but a snapshot is one
// committed aggregate: references and lockstep facts that ApplyCommitted
// protects must be re-established before recovery exposes any store.  Keep
// historical topology owners legal while fenced; only an active grant gives
// that field serving authority and therefore requires a live membership.
absl::Status ValidateDecodedAggregate(const MetaStores& stores) {
  if (stores.topology_.GroupCount() != stores.grant_.GroupCount()) {
    return MetaFailStopError(
        "topology and grant stores have different group sets");
  }

  for (const MetaTopologyGroupView& group : stores.topology_.Groups()) {
    const auto grant = stores.grant_.GroupState(group.group_id_);
    if (!grant.has_value()) {
      return MetaFailStopError(absl::StrCat("topology group ", group.group_id_,
                                            " has no grant-store entry"));
    }
    if (group.record_.group_term_ != grant->group_term_ ||
        group.record_.authority_version_ != grant->last_authority_version_) {
      return MetaFailStopError(absl::StrCat(
          "topology/grant anchors disagree for group ", group.group_id_));
    }
    for (const MetaGroupMember& member : group.members_) {
      if (!stores.identity_.IsActiveNode(member.node_id_)) {
        return MetaFailStopError(absl::StrCat("group ", group.group_id_,
                                              " names inactive member ",
                                              member.node_id_));
      }
    }
    if (group.record_.population_manifest_revision_ != 0 &&
        !stores.population_manifest_.Contains(
            group.record_.population_manifest_digest_)) {
      return MetaFailStopError(
          absl::StrCat("group ", group.group_id_,
                       " references a missing population manifest"));
    }

    if (!grant->grant_.has_value()) continue;
    const MetaGroupGrant& active = *grant->grant_;
    if (grant->fenced_ || group.record_.group_term_ == 0 ||
        group.record_.authority_version_ == 0 ||
        grant->last_grant_revision_ == 0 || group.config_epoch_ == 0 ||
        group.record_.owner_.empty() || group.record_.owner_ != active.owner_ ||
        !stores.identity_.IsActiveNode(active.owner_) ||
        !IsMember(group, active.owner_)) {
      return MetaFailStopError(absl::StrCat(
          "active grant owner is inconsistent for group ", group.group_id_));
    }
    if (!stores.policy_.IsVersionActive(active.spec_.policy_id_,
                                        active.spec_.policy_version_)) {
      return MetaFailStopError(
          absl::StrCat("active grant for group ", group.group_id_,
                       " references a missing or retired policy"));
    }
  }

  for (const MetaOperationRecord& operation :
       stores.operation_.LiveOperations()) {
    if (operation.lifecycle_ == MetaOperationLifecycle::kCompleted ||
        operation.lifecycle_ == MetaOperationLifecycle::kAborted) {
      continue;
    }
    for (const MetaPolicyReference& reference : operation.policy_references_) {
      if (!stores.policy_.IsVersionActive(reference.policy_id_,
                                          reference.version_)) {
        return MetaFailStopError(
            "non-terminal operation references a missing or retired policy");
      }
    }
    for (const MetaEvidenceSummary& evidence : operation.evidence_) {
      const auto evidence_group =
          stores.topology_.FindGroup(evidence.group_id_);
      if (!evidence_group.has_value()) {
        return MetaFailStopError(
            "non-terminal operation evidence references a missing group");
      }
      // Evidence is immutable history, so an older anchor remains valid after
      // the group advances. A future anchor, or a digest inconsistent with the
      // same manifest revision, could never have passed committed apply.
      if (evidence.group_term_ > evidence_group->record_.group_term_ ||
          evidence.population_manifest_revision_ >
              evidence_group->record_.population_manifest_revision_ ||
          evidence.partition_replication_epoch_ >
              evidence_group->record_.partition_replication_epoch_ ||
          (evidence.population_manifest_revision_ ==
               evidence_group->record_.population_manifest_revision_ &&
           evidence.population_manifest_digest_ !=
               evidence_group->record_.population_manifest_digest_)) {
        return MetaFailStopError(
            "non-terminal operation evidence contains impossible anchors");
      }
      if (evidence.population_manifest_revision_ != 0 &&
          !stores.population_manifest_.Contains(
              evidence.population_manifest_digest_)) {
        return MetaFailStopError(
            "non-terminal operation evidence references a missing manifest");
      }
    }
    for (const MetaCurrentDirective& directive :
         operation.current_directives_) {
      if (const absl::Status anchor =
              ValidateCommittedDirectiveAnchorImpl(stores, directive.spec_);
          !anchor.ok()) {
        return MetaFailStopError(absl::StrCat(
            "non-terminal operation contains stale directive anchor: ",
            anchor.message()));
      }
      if (directive.spec_.population_manifest_revision_ != 0 &&
          !stores.population_manifest_.Contains(
              directive.spec_.population_manifest_digest_)) {
        return MetaFailStopError(
            "non-terminal operation directive references a missing manifest");
      }
    }
  }
  return absl::OkStatus();
}

// Validate the exact wire projection before publishing an operation phase.
// Per-operation bounds alone are insufficient because one data node can be
// the recipient of directives from many live operations.  The projector is
// deliberately reused here so this guard cannot drift from protocol field,
// entry-count, or total-object limits.
absl::Status ValidateAffectedFullStateProjections(
    const MetaStores& candidate, std::uint64_t log_index,
    const std::set<std::string>& affected_recipients) {
  if (affected_recipients.empty()) return absl::OkStatus();

  const MetaCommittedView view(candidate, log_index);
  for (const std::string& recipient : affected_recipients) {
    const auto projected = MetaControlProjector::ProjectNode(view, recipient);
    if (!projected.ok()) {
      return MetaDomainRejectError(absl::StrCat(
          "operation phase makes FullDesiredState unprojectable for node ",
          recipient, ": ", projected.status().message()));
    }
  }
  return absl::OkStatus();
}

// Whether the node owns an active grant. The grant store has no per-node
// index; the "grant owner => member of the group" invariant (maintained by
// the ActivateAuthority member check and by rejecting RemoveNodeFromGroup of
// a grant owner) lets the fact be read through the node's current group.
bool NodeHoldsActiveGrant(const MetaStores& stores,
                          const std::string& node_id) {
  const auto group = stores.topology_.FindGroupOfNode(node_id);
  if (!group.has_value()) return false;
  const auto state = stores.grant_.GroupState(*group);
  return state.has_value() && state->grant_.has_value() &&
         state->grant_->owner_ == node_id;
}

bool GroupHasActiveGrant(const MetaStores& stores,
                         std::string_view group_id) {
  const auto state = stores.grant_.GroupState(group_id);
  return state.has_value() && !state->fenced_ && state->grant_.has_value();
}

// A live lease names the projection that granted it. Moving a slot into or
// out of that projection, or changing its config epoch, cannot ride the same
// authority: the old and new owners could otherwise accept the same slot
// until both sessions consume their replacement FullDesiredState. Build the
// complete candidate first so malformed absolute maps are rejected without
// duplicating topology-store validation, then require every affected group to
// be grantless/fenced before publishing any part of the replacement.
absl::Status ValidateSlotMapAuthorityTransition(
    const MetaStores& current, const MetaTopologyStore& candidate) {
  std::set<std::string> affected_groups;
  for (std::uint32_t slot = 0; slot < kMetaSlotCount; ++slot) {
    const auto before = current.topology_.SlotOwner(slot);
    const auto after = candidate.SlotOwner(slot);
    if (before == after) continue;
    if (before.has_value()) affected_groups.insert(*before);
    if (after.has_value()) affected_groups.insert(*after);
  }
  for (const MetaTopologyGroupView& before : current.topology_.Groups()) {
    const auto after = candidate.FindGroup(before.group_id_);
    if (after.has_value() &&
        before.config_epoch_ != after->config_epoch_) {
      affected_groups.insert(before.group_id_);
    }
  }
  for (const std::string& group_id : affected_groups) {
    if (GroupHasActiveGrant(current, group_id)) {
      return MetaDomainRejectError(absl::StrCat(
          "slot ownership or config epoch change for group ", group_id,
          " requires its active grant to be fenced first"));
    }
  }
  return absl::OkStatus();
}

// The replay predicate of ActivateAuthority: BOTH halves already carry
// exactly this command's post-effect (grant half: the same predicate the
// grant store's GrantMatches uses; topology half: owner, authority_version,
// config_epoch, and the cluster topology_epoch).
bool ActivateEffectPresent(const MetaStores& stores,
                           const ActivateAuthority& cmd,
                           const MetaTopologyGroupView& view,
                           const MetaGroupGrantState& grant_state) {
  if (grant_state.fenced_ || !grant_state.grant_.has_value()) return false;
  const MetaGroupGrant& grant = *grant_state.grant_;
  const bool grant_half =
      grant.owner_ == cmd.new_owner_ && grant.term_ == cmd.expected_term_ &&
      grant.authority_version_ == cmd.new_authority_version_ &&
      grant.spec_ == cmd.grant_;
  const bool topology_half =
      view.record_.owner_ == cmd.new_owner_ &&
      view.record_.authority_version_ == cmd.new_authority_version_ &&
      view.config_epoch_ == cmd.new_config_epoch_ &&
      stores.topology_.TopologyEpoch() == cmd.new_topology_epoch_;
  return grant_half && topology_half;
}

// ---------------------------------------------------------------------------
// identity/enrollment: no cross-store inputs (the identity store is the root
// registry). RetireNode additionally consults topology: a node still holding
// group membership cannot retire (the identity store header delegates this
// cross-store rule to the apply layer).
// ---------------------------------------------------------------------------

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const RegisterNode& cmd) {
  (void)log_index;
  return FromStatus(stores.identity_.Apply(cmd),
                    absl::StrCat("RegisterNode node=", cmd.node_id_,
                                 " principal=", cmd.principal_));
}

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const UpdateNode& cmd) {
  (void)log_index;
  std::string summary =
      absl::StrCat("UpdateNode node=", cmd.node_id_,
                   " expected_revision=", cmd.expected_revision_,
                   " topology_epoch=", cmd.new_topology_epoch_);
  const auto current_node = stores.identity_.FindNode(cmd.node_id_);
  const bool identity_effect_present =
      current_node.has_value() && !current_node->retired_ &&
      cmd.expected_revision_ != UINT64_MAX &&
      current_node->revision_ == cmd.expected_revision_ + 1 &&
      current_node->endpoints_ == cmd.endpoints_ &&
      current_node->capability_mask_ == cmd.capability_mask_;
  const bool topology_effect_present =
      stores.topology_.TopologyEpoch() == cmd.new_topology_epoch_;
  if (identity_effect_present != topology_effect_present) {
    return Rejected("UpdateNode replay halves do not agree",
                    std::move(summary));
  }
  // Endpoint changes are topology-visible. Preflight the epoch before the
  // identity write so a rejection cannot leave the aggregate half-mutated.
  if (const absl::Status st =
          stores.topology_.ValidateTopologyEpoch(cmd.new_topology_epoch_);
      !st.ok()) {
    return Rejected(st, std::move(summary));
  }
  if (const absl::Status st = stores.identity_.Apply(cmd); !st.ok()) {
    return Rejected(st, std::move(summary));
  }
  return FromStatus(stores.topology_.SetTopologyEpoch(cmd.new_topology_epoch_),
                    std::move(summary));
}

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const RetireNode& cmd) {
  (void)log_index;
  const std::string summary =
      absl::StrCat("RetireNode node=", cmd.node_id_,
                   " expected_revision=", cmd.expected_revision_);
  // Cross-store: a node with group membership still has topology obligations.
  // "Grant owner => member" (see the file header) makes this also cover an
  // active grant.
  if (stores.topology_.FindGroupOfNode(cmd.node_id_).has_value()) {
    return Rejected(
        absl::StrCat("node ", cmd.node_id_, " still holds group membership"),
        std::move(summary));
  }
  return FromStatus(stores.identity_.Apply(cmd), std::move(summary));
}

// ---------------------------------------------------------------------------
// topology. CreateGroup is the group lifecycle point for BOTH stores: the
// topology table and the grant store's per-group entry must move together.
// ---------------------------------------------------------------------------

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const CreateGroup& cmd) {
  (void)log_index;
  const std::string summary =
      absl::StrCat("CreateGroup group=", cmd.group_id_,
                   " topology_epoch=", cmd.new_topology_epoch_);
  // Topology first: it carries the strictly-more failure modes (epoch rule,
  // pristine replay check) and validates the id caps the grant store repeats.
  const absl::Status status = stores.topology_.Apply(cmd);
  if (!status.ok()) return Rejected(status, std::move(summary));
  // Lockstep invariant: the grant store mirrors the group set. This cannot
  // fail — same id validation, equal group cap, the group was absent until
  // now; a failure means the stores were wired out of sync, so surface it
  // instead of proceeding desynchronized.
  return FromStatus(stores.grant_.AddGroup(cmd.group_id_), std::move(summary));
}

// ---------------------------------------------------------------------------
// topology membership. AssignNodeToGroup carries the one-node-one-group
// cross-store half described in file-header invariant 4;
// RemoveNodeFromGroup keeps
// "grant owner => member" by refusing to strand an active grant.
// ---------------------------------------------------------------------------

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const AssignNodeToGroup& cmd) {
  (void)log_index;
  std::string summary =
      absl::StrCat("AssignNodeToGroup group=", cmd.group_id_,
                   " node=", cmd.node_id_, " role=", RoleName(cmd.role_),
                   " expected_revision=", cmd.expected_revision_,
                   " topology_epoch=", cmd.new_topology_epoch_);
  // Item 1: the assign target must be a registered, non-retired node.
  if (!stores.identity_.IsActiveNode(cmd.node_id_)) {
    return Rejected(
        absl::StrCat("node ", cmd.node_id_, " is not a registered active node"),
        std::move(summary));
  }
  const auto current = stores.topology_.FindGroupOfNode(cmd.node_id_);
  if (!current.has_value() || *current != cmd.group_id_) {
    // Item 4: the command moves the node into a group it is not a member of.
    // The node must carry no durable authority obligation: no current
    // membership, no active grant. Operation intents are deliberately opaque
    // and do not create implicit node obligations.
    if (current.has_value()) {
      return Rejected(
          absl::StrCat("node ", cmd.node_id_, " already a member of ", *current,
                       " (one-node-one-group)"),
          std::move(summary));
    }
    // Unreachable while owner=>member holds; kept as defense in depth.
    if (NodeHoldsActiveGrant(stores, cmd.node_id_)) {
      return Rejected(
          absl::StrCat("node ", cmd.node_id_, " holds an active grant"),
          std::move(summary));
    }
  }
  // Same-group re-entry skips the cross-store half: the topology store itself
  // distinguishes replay (same role, produced revision -> idempotent accept)
  // from a role change (rejection).
  return FromStatus(stores.topology_.Apply(cmd), std::move(summary));
}

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const RemoveNodeFromGroup& cmd) {
  (void)log_index;
  std::string summary = absl::StrCat(
      "RemoveNodeFromGroup group=", cmd.group_id_, " node=", cmd.node_id_,
      " expected_revision=", cmd.expected_revision_,
      " topology_epoch=", cmd.new_topology_epoch_);
  // Keep "grant owner => member": the owner of the group's active grant
  // cannot leave the membership while the grant stands (revoke/fence first).
  // This is what makes the AssignNodeToGroup obligation check sound without a
  // grant-by-node index.
  const auto grant_state = stores.grant_.GroupState(cmd.group_id_);
  if (grant_state.has_value() && grant_state->grant_.has_value() &&
      grant_state->grant_->owner_ == cmd.node_id_) {
    return Rejected(absl::StrCat("node ", cmd.node_id_,
                                 " owns the active grant of ", cmd.group_id_),
                    std::move(summary));
  }
  return FromStatus(stores.topology_.Apply(cmd), std::move(summary));
}

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const SetSlotMap& cmd) {
  (void)log_index;
  std::string summary =
      absl::StrCat("SetSlotMap ranges=", cmd.ranges_.size(),
                   " topology_epoch=", cmd.new_topology_epoch_,
                   " config_epochs=", cmd.config_epochs_.size());
  MetaTopologyStore candidate = stores.topology_;
  if (const absl::Status applied = candidate.Apply(cmd); !applied.ok()) {
    return Rejected(applied, std::move(summary));
  }
  if (const absl::Status safe =
          ValidateSlotMapAuthorityTransition(stores, candidate);
      !safe.ok()) {
    return Rejected(safe, std::move(summary));
  }
  stores.topology_ = std::move(candidate);
  return Accepted(std::move(summary));
}

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const SetGroupReplicationState& cmd) {
  (void)log_index;
  if (cmd.new_population_manifest_revision_ != 0 &&
      !stores.population_manifest_.Contains(
          cmd.new_population_manifest_digest_)) {
    return Rejected(
        "population manifest digest is not committed",
        absl::StrCat("SetGroupReplicationState group=", cmd.group_id_));
  }
  return FromStatus(
      stores.topology_.Apply(cmd),
      absl::StrCat("SetGroupReplicationState group=", cmd.group_id_,
                   " manifest=", cmd.new_population_manifest_revision_,
                   " partition_epoch=", cmd.new_partition_replication_epoch_,
                   " topology_epoch=", cmd.new_topology_epoch_));
}

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const PutPopulationManifest& cmd) {
  (void)log_index;
  std::string summary = absl::StrCat(
      "PutPopulationManifest digest=", HexBytes(cmd.manifest_digest_),
      " entries=", cmd.entries_.size());
  if (stores.population_manifest_.Contains(cmd.manifest_digest_)) {
    return FromStatus(stores.population_manifest_.Put(cmd), std::move(summary));
  }

  MetaPopulationManifestStore candidate = stores.population_manifest_;
  if (absl::Status put = candidate.Put(cmd); !put.ok()) {
    return Rejected(put, std::move(summary));
  }
  auto bytes = SnapshotBytesWithPopulationManifest(stores, candidate);
  if (!bytes.ok()) return Rejected(bytes.status(), std::move(summary));
  if (*bytes > kMaxMetaSnapshotBytes ||
      kMaximumAuditSnapshotGrowth > kMaxMetaSnapshotBytes - *bytes) {
    return Rejected(
        "population manifest exceeds the remaining snapshot byte budget",
        std::move(summary));
  }
  stores.population_manifest_ = std::move(candidate);
  return Accepted(std::move(summary));
}

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const PrunePopulationManifest& cmd) {
  (void)log_index;
  std::string summary = absl::StrCat("PrunePopulationManifest digest=",
                                     HexBytes(cmd.manifest_digest_));
  if (stores.topology_.PopulationManifestInUse(cmd.manifest_digest_)) {
    return Rejected("population manifest is referenced by a group",
                    std::move(summary));
  }
  if (stores.operation_.PopulationManifestInUse(cmd.manifest_digest_)) {
    return Rejected("population manifest is referenced by a live operation",
                    std::move(summary));
  }
  return FromStatus(stores.population_manifest_.Prune(cmd), std::move(summary));
}

// ---------------------------------------------------------------------------
// term/grant. BeginGroupTerm writes
// both halves (grant store term state machine + the committed GroupRecord in
// the topology store); ActivateAuthority is the atomic failover/migration
// commit point (file header item 3).
// ---------------------------------------------------------------------------

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const BeginGroupTerm& cmd) {
  (void)log_index;
  std::string summary =
      absl::StrCat("BeginGroupTerm group=", cmd.group_id_,
                   " term=", cmd.expected_term_, "->", cmd.new_term_);
  // The grant store owns the term state machine (T-1 -> T CAS, re-fence).
  const absl::Status status = stores.grant_.BeginGroupTerm(cmd);
  if (!status.ok()) return Rejected(status, std::move(summary));
  // Mirror the committed term into the topology GroupRecord so the data plane
  // reads it from one record. Fails only on an unknown group — impossible
  // under the group-set lockstep; surface it rather than desynchronize.
  return FromStatus(stores.topology_.SetGroupTerm(cmd.group_id_, cmd.new_term_),
                    std::move(summary));
}

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const GrantAuthority& cmd) {
  std::string summary = absl::StrCat(
      "GrantAuthority group=", cmd.group_id_, " node=", cmd.node_id_,
      " term=", cmd.term_, " authority_version=", cmd.authority_version_,
      " policy=", cmd.grant_.policy_id_, "@", cmd.grant_.policy_version_);
  // Item 1: the renewing owner must be a registered, non-retired node.
  if (!stores.identity_.IsActiveNode(cmd.node_id_)) {
    return Rejected(
        absl::StrCat("node ", cmd.node_id_, " is not a registered active node"),
        std::move(summary));
  }
  // Item 5: the grant's policy reference must be committed and non-retired.
  if (!stores.policy_.IsVersionActive(cmd.grant_.policy_id_,
                                      cmd.grant_.policy_version_)) {
    return Rejected(absl::StrCat("policy ", cmd.grant_.policy_id_, " version ",
                                 cmd.grant_.policy_version_,
                                 " is not committed and active"),
                    std::move(summary));
  }
  return FromStatus(stores.grant_.GrantAuthority(cmd, log_index),
                    std::move(summary));
}

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const ActivateAuthority& cmd) {
  std::string summary = absl::StrCat(
      "ActivateAuthority group=", cmd.group_id_, " owner=", cmd.new_owner_,
      " expected_term=", cmd.expected_term_,
      " authority_version=", cmd.new_authority_version_,
      " topology_epoch=", cmd.new_topology_epoch_,
      " config_epoch=", cmd.new_config_epoch_,
      " policy=", cmd.grant_.policy_id_, "@", cmd.grant_.policy_version_);
  if (cmd.new_config_epoch_ == 0) {
    return Rejected("authority activation requires a nonzero config epoch",
                    std::move(summary));
  }
  // Phase 1: pure validation across all four stores; nothing is written until
  // every check has passed; this is the atomic commit point.
  const absl::Status valid = stores.grant_.ValidateActivate(cmd, log_index);
  if (!valid.ok()) return Rejected(valid, std::move(summary));
  const auto view = stores.topology_.FindGroup(cmd.group_id_);
  if (!view.has_value()) {
    // Lockstep: a successful ValidateActivate implies the group exists here.
    return Rejected(absl::StrCat("unknown group ", cmd.group_id_),
                    std::move(summary));
  }
  const auto grant_state = stores.grant_.GroupState(cmd.group_id_);
  // Replay: both halves already carry exactly this command's effect. Skip the
  // absolute-value checks the command has already consumed (topology_epoch
  // has moved to the command's value); the writes below then no-op. The
  // remaining checks are stable under the command's own post-effect (an
  // active grant pins its owner member/active and its policy active), so a
  // genuine replay passes them anyway.
  if (!grant_state.has_value() ||
      !ActivateEffectPresent(stores, cmd, *view, *grant_state)) {
    const std::uint64_t epoch = stores.topology_.TopologyEpoch();
    if (epoch == std::numeric_limits<std::uint64_t>::max() ||
        cmd.new_topology_epoch_ != epoch + 1) {
      return Rejected(
          absl::StrCat("new_topology_epoch must be exactly current+1 (", epoch,
                       ")"),
          std::move(summary));
    }
    if (!IsMember(*view, cmd.new_owner_)) {
      return Rejected(absl::StrCat("new owner ", cmd.new_owner_,
                                   " is not a member of ", cmd.group_id_),
                      std::move(summary));
    }
    if (!stores.identity_.IsActiveNode(cmd.new_owner_)) {
      return Rejected(absl::StrCat("new owner ", cmd.new_owner_,
                                   " is not a registered active node"),
                      std::move(summary));
    }
    if (!stores.policy_.IsVersionActive(cmd.grant_.policy_id_,
                                        cmd.grant_.policy_version_)) {
      return Rejected(absl::StrCat("policy ", cmd.grant_.policy_id_,
                                   " version ", cmd.grant_.policy_version_,
                                   " is not committed and active"),
                      std::move(summary));
    }
  }
  // Phase 2: the writes, grant half first then the topology half. Every write
  // is validated by phase 1: ApplyGrantPart fail-stops only on a term
  // mismatch (ruled out by ValidateActivate); the topology setters reject only
  // unknown groups (ruled out) and are idempotent no-ops on replay.
  if (const absl::Status st = stores.grant_.ApplyGrantPart(cmd, log_index);
      !st.ok()) {
    return Rejected(st, std::move(summary));
  }
  if (const absl::Status st =
          stores.topology_.SetOwner(cmd.group_id_, cmd.new_owner_);
      !st.ok()) {
    return Rejected(st, std::move(summary));
  }
  if (const absl::Status st = stores.topology_.SetAuthorityVersion(
          cmd.group_id_, cmd.new_authority_version_);
      !st.ok()) {
    return Rejected(st, std::move(summary));
  }
  if (const absl::Status st = stores.topology_.SetGroupConfigEpoch(
          cmd.group_id_, cmd.new_config_epoch_);
      !st.ok()) {
    return Rejected(st, std::move(summary));
  }
  return FromStatus(stores.topology_.SetTopologyEpoch(cmd.new_topology_epoch_),
                    std::move(summary));
}

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const RevokeGrant& cmd) {
  (void)log_index;
  // Grant-store local (term CAS, drop grant, fence); the topology record's
  // owner field is deliberately left stale (no cascade — see the topology
  // store header).
  return FromStatus(stores.grant_.RevokeGrant(cmd),
                    absl::StrCat("RevokeGrant group=", cmd.group_id_,
                                 " expected_term=", cmd.expected_term_));
}

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const FenceGroup& cmd) {
  (void)log_index;
  return FromStatus(stores.grant_.FenceGroup(cmd),
                    absl::StrCat("FenceGroup group=", cmd.group_id_,
                                 " expected_term=", cmd.expected_term_));
}

// ---------------------------------------------------------------------------
// policy. Retirement checks both committed reference owners: active grants
// and live operations. Operation policy references are structured fields on
// SubmitOperation; apply never interprets opaque operation intents.
// ---------------------------------------------------------------------------

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const PutPolicy& cmd) {
  (void)log_index;
  return FromStatus(
      stores.policy_.Apply(cmd),
      absl::StrCat("PutPolicy policy=", cmd.policy_id_,
                   " version=", cmd.version_, " bytes=", cmd.content_.size()));
}

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const RetirePolicy& cmd) {
  (void)log_index;
  std::string summary = absl::StrCat("RetirePolicy policy=", cmd.policy_id_,
                                     " version=", cmd.version_);
  if (stores.grant_.PolicyInUse(cmd.policy_id_, cmd.version_)) {
    return Rejected(
        absl::StrCat("policy ", cmd.policy_id_, " version ", cmd.version_,
                     " is referenced by an active grant"),
        std::move(summary));
  }
  if (stores.operation_.PolicyInUse(cmd.policy_id_, cmd.version_)) {
    return Rejected(
        absl::StrCat("policy ", cmd.policy_id_, " version ", cmd.version_,
                     " is referenced by a non-terminal operation"),
        std::move(summary));
  }
  return FromStatus(stores.policy_.Apply(cmd), std::move(summary));
}

// ---------------------------------------------------------------------------
// operation journal + upgrade. The operation store owns the lifecycle
// machine. The dispatcher supplies the log index and actor, validates policy
// dependencies on submit, and rechecks evidence against committed identity,
// topology, term, manifest, partition-replication, and history anchors before
// each transition.
// ---------------------------------------------------------------------------

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const SubmitOperation& cmd, const ActorContext& actor) {
  // The summary must be a pure function of (command, log index): a replay
  // resolves as a duplicate and any outcome-dependent suffix would change the
  // audit record under the same index, tripping the store's fail-stop.
  std::string summary =
      absl::StrCat("SubmitOperation kind=", cmd.kind_,
                   " id=", HexBytes(cmd.operation_id_), " seq=", log_index);
  // A permanent-id duplicate cannot mutate the existing record. Resolve it
  // before checking current policy state so an old accepted submit remains an
  // idempotent replay after the operation terminates and its policy retires.
  if (stores.operation_.OperationKnown(cmd.operation_id_)) {
    SubmitOperation injected = cmd;
    injected.actor_ = actor;
    const auto result = stores.operation_.SubmitOperation(injected, log_index);
    if (!result.ok()) return Rejected(result.status(), std::move(summary));
    return Accepted(std::move(summary));
  }
  for (const MetaPolicyReference& reference : cmd.policy_references_) {
    if (!stores.policy_.IsVersionActive(reference.policy_id_,
                                        reference.version_)) {
      return Rejected(
          absl::StrCat("operation references uncommitted or retired policy ",
                       reference.policy_id_, " version ", reference.version_),
          std::move(summary));
    }
  }
  // The journal persists the submitter's ActorContext; it is injected by the
  // trusted entry, rides the raft-log encoding (commands.h), and is
  // only copied by apply — so inject it into the command copy handed to the
  // store. (cmd.actor_ already holds the same decoded value; the explicit
  // parameter keeps the dispatch contract independent of the wire path.)
  SubmitOperation injected = cmd;
  injected.actor_ = actor;
  const auto result = stores.operation_.SubmitOperation(injected, log_index);
  if (!result.ok()) return Rejected(result.status(), std::move(summary));
  return Accepted(std::move(summary));
}

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const TransitionOperationPhase& cmd) {
  std::string summary =
      absl::StrCat("TransitionOperationPhase id=", HexBytes(cmd.operation_id_),
                   " expected_revision=", cmd.expected_revision_,
                   " evidence=", cmd.evidence_.size());
  if (stores.operation_.TransitionAlreadyApplied(cmd)) {
    return FromStatus(
        stores.operation_.TransitionOperationPhase(cmd, log_index),
        std::move(summary));
  }
  const auto operation = stores.operation_.FindOperation(cmd.operation_id_);
  if (operation.has_value()) {
    for (const MetaDirectiveSpec& directive : cmd.current_directives_) {
      if (const absl::Status anchor =
              ValidateCommittedDirectiveAnchorImpl(stores, directive);
          !anchor.ok()) {
        return Rejected(anchor, std::move(summary));
      }
    }
    for (const MetaEvidenceSummary& evidence : cmd.evidence_) {
      if (evidence.operation_id_ != cmd.operation_id_) {
        return Rejected("evidence references a different operation",
                        std::move(summary));
      }
      if (std::all_of(operation->replication_history_id_.begin(),
                      operation->replication_history_id_.end(),
                      [](std::uint8_t byte) { return byte == 0; }) ||
          evidence.replication_history_id_ !=
              operation->replication_history_id_) {
        return Rejected("evidence replication history is not committed",
                        std::move(summary));
      }
      if (!stores.identity_.IsActiveNode(evidence.node_id_)) {
        return Rejected("evidence node is not active", std::move(summary));
      }
      const auto group = stores.topology_.FindGroup(evidence.group_id_);
      if (!group.has_value()) {
        return Rejected("evidence group does not exist", std::move(summary));
      }
      if (!HasAssignment(*group, evidence.node_id_, evidence.assignment_id_)) {
        return Rejected("evidence membership or assignment is stale",
                        std::move(summary));
      }
      const auto term = stores.grant_.CurrentGroupTerm(evidence.group_id_);
      if (!term.has_value() || *term != evidence.group_term_ ||
          group->record_.population_manifest_revision_ !=
              evidence.population_manifest_revision_ ||
          group->record_.population_manifest_digest_ !=
              evidence.population_manifest_digest_ ||
          group->record_.partition_replication_epoch_ !=
              evidence.partition_replication_epoch_) {
        return Rejected("evidence term or population identity is stale",
                        std::move(summary));
      }
    }
  }

  // Apply to a candidate aggregate first. A command may satisfy every local
  // operation-store cap while pushing one recipient over the aggregate FDS
  // directive-count or byte limit. Nothing in committed domain state changes
  // until the exact node projections remain encodable.
  std::set<std::string> affected_recipients;
  if (operation.has_value()) {
    for (const MetaCurrentDirective& current : operation->current_directives_) {
      affected_recipients.insert(current.spec_.recipient_node_id_);
    }
  }
  for (const MetaDirectiveSpec& directive : cmd.current_directives_) {
    affected_recipients.insert(directive.recipient_node_id_);
  }

  MetaStores candidate = stores;
  if (const absl::Status status =
          candidate.operation_.TransitionOperationPhase(cmd, log_index);
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }
  if (const absl::Status status = ValidateAffectedFullStateProjections(
          candidate, log_index, affected_recipients);
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }
  stores.operation_ = std::move(candidate.operation_);
  return Accepted(std::move(summary));
}

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const CompleteOperation& cmd) {
  (void)log_index;
  return FromStatus(
      stores.operation_.CompleteOperation(cmd),
      absl::StrCat("CompleteOperation id=", HexBytes(cmd.operation_id_),
                   " expected_revision=", cmd.expected_revision_,
                   " data_loss_possible=", cmd.data_loss_possible_ ? 1 : 0));
}

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const AbortOperation& cmd) {
  (void)log_index;
  return FromStatus(
      stores.operation_.AbortOperation(cmd),
      absl::StrCat("AbortOperation id=", HexBytes(cmd.operation_id_),
                   " expected_revision=", cmd.expected_revision_));
}

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const CommitDirectiveResult& cmd) {
  std::string summary = absl::StrCat(
      "CommitDirectiveResult operation=", HexBytes(cmd.operation_id_),
      " attempt=", HexBytes(cmd.attempt_id_),
      " directive_revision=", cmd.directive_revision_,
      " result_hash=", HexBytes(cmd.result_hash_));
  const MetaTerminalReceiptKey key{cmd.operation_id_, cmd.directive_id_,
                                   cmd.attempt_id_, cmd.directive_revision_};
  // A receipt is immutable committed history: an exact retry must still
  // resolve to its original commit index even if the directive's authority
  // later advances. A first-time result, however, is admissible only while
  // the exact live directive and all of its committed anchors remain current.
  if (!stores.operation_.FindTerminalReceipt(key).has_value()) {
    const auto operation = stores.operation_.FindOperation(cmd.operation_id_);
    if (operation.has_value()) {
      const auto directive = std::find_if(
          operation->current_directives_.begin(),
          operation->current_directives_.end(),
          [&cmd](const MetaCurrentDirective& current) {
            return current.spec_.directive_id_ == cmd.directive_id_ &&
                   current.spec_.attempt_id_ == cmd.attempt_id_ &&
                   current.directive_revision_ == cmd.directive_revision_;
          });
      if (directive != operation->current_directives_.end()) {
        if (const absl::Status anchor =
                ValidateCommittedDirectiveAnchorImpl(stores, directive->spec_);
            !anchor.ok()) {
          return Rejected(anchor, std::move(summary));
        }
      }
    }
  }
  return FromStatus(stores.operation_.CommitDirectiveResult(cmd, log_index),
                    std::move(summary));
}

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const ArchiveOperations& cmd) {
  (void)log_index;
  return FromStatus(
      stores.operation_.ArchiveOperations(cmd),
      absl::StrCat("ArchiveOperations seqs=", cmd.operation_seqs_.size()));
}

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const PruneAudit& cmd) {
  (void)log_index;
  return FromStatus(
      stores.audit_.PruneThrough(cmd.through_log_index_),
      absl::StrCat("PruneAudit through=", cmd.through_log_index_));
}

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const SetAuditPolicy& cmd) {
  (void)log_index;
  const char* name = "unknown";
  switch (cmd.policy_) {
    case MetaAuditPolicy::kDisabled:
      name = "disabled";
      break;
    case MetaAuditPolicy::kBoundedRotate:
      name = "bounded-rotate";
      break;
    case MetaAuditPolicy::kStrictExport:
      name = "strict-export";
      break;
  }
  return FromStatus(stores.audit_.SetPolicy(cmd.policy_),
                    absl::StrCat("SetAuditPolicy policy=", name,
                                 " attestation=", cmd.attestation_));
}

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const PruneOperationArchive& cmd) {
  (void)log_index;
  return FromStatus(
      stores.operation_.PruneArchive(cmd),
      absl::StrCat("PruneOperationArchive seqs=", cmd.operation_seqs_.size()));
}

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const PruneTerminalReceipts& cmd) {
  (void)log_index;
  return FromStatus(
      stores.operation_.PruneTerminalReceipts(cmd),
      absl::StrCat("PruneTerminalReceipts receipts=", cmd.receipts_.size()));
}

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const BindMetaMember& cmd) {
  (void)log_index;
  return FromStatus(stores.identity_.Apply(cmd),
                    absl::StrCat("BindMetaMember id=", cmd.server_id_,
                                 " principal=", cmd.principal_,
                                 " data_control=", cmd.data_control_endpoint_));
}

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const RetireMetaMember& cmd) {
  (void)log_index;
  return FromStatus(stores.identity_.Apply(cmd),
                    absl::StrCat("RetireMetaMember id=", cmd.server_id_));
}

}  // namespace

absl::Status ValidateCommittedDirectiveAnchor(
    const MetaStores& stores, const MetaDirectiveSpec& directive) {
  return ValidateCommittedDirectiveAnchorImpl(stores, directive);
}

absl::StatusOr<std::string> MetaStores::Serialize() const {
  MetaWriter w;
  w.WriteU16(kMetaFormatVersion);
  // Each store blob carries its own u16 schema_version envelope; the length
  // prefix bounds each sub-decode.
  w.WriteString(identity_.Serialize());
  w.WriteString(topology_.Serialize());
  w.WriteString(policy_.Serialize());
  const auto grant = grant_.Serialize();
  if (!grant.ok()) return grant.status();
  w.WriteString(*grant);
  const auto operation = operation_.Serialize();
  if (!operation.ok()) return operation.status();
  w.WriteString(*operation);
  w.WriteString(population_manifest_.Serialize());
  const auto audit = audit_.Serialize();
  if (!audit.ok()) return audit.status();
  w.WriteString(*audit);
  std::string out = w.TakeBuffer();
  if (out.size() > kMaxMetaSnapshotBytes) {
    // Fail-safe: the snapshot byte cap fails the snapshot; it is never
    // silently truncated.
    return MetaDomainRejectError("meta snapshot exceeds the total byte cap");
  }
  return out;
}

absl::StatusOr<MetaStores> MetaStores::Deserialize(std::string_view bytes) {
  if (bytes.size() > kMaxMetaSnapshotBytes) {
    return MetaFailStopError("meta snapshot exceeds the total byte cap");
  }
  MetaReader r(bytes);
  const auto version = r.ReadU16();
  if (!version.ok()) return version.status();
  if (*version != kMetaFormatVersion) {
    return MetaFailStopError("unsupported meta stores schema version");
  }
  // Loose per-blob cap: the aggregate cap dominates; each store's own
  // Deserialize enforces its content caps strictly.
  constexpr std::uint32_t kBlobCap =
      static_cast<std::uint32_t>(kMaxMetaSnapshotBytes);
  const auto identity = r.ReadString(kBlobCap);
  if (!identity.ok()) return identity.status();
  const auto topology = r.ReadString(kBlobCap);
  if (!topology.ok()) return topology.status();
  const auto policy = r.ReadString(kBlobCap);
  if (!policy.ok()) return policy.status();
  const auto grant = r.ReadString(kBlobCap);
  if (!grant.ok()) return grant.status();
  const auto operation = r.ReadString(kBlobCap);
  if (!operation.ok()) return operation.status();
  const auto population_manifest = r.ReadString(kBlobCap);
  if (!population_manifest.ok()) return population_manifest.status();
  const auto audit = r.ReadString(kBlobCap);
  if (!audit.ok()) return audit.status();
  if (absl::Status status = r.Finish(); !status.ok()) return status;

  MetaStores stores;
  auto identity_store = MetaIdentityStore::Deserialize(*identity);
  if (!identity_store.ok()) return identity_store.status();
  stores.identity_ = std::move(*identity_store);
  auto topology_store = MetaTopologyStore::Deserialize(*topology);
  if (!topology_store.ok()) return topology_store.status();
  stores.topology_ = std::move(*topology_store);
  auto policy_store = MetaPolicyStore::Deserialize(*policy);
  if (!policy_store.ok()) return policy_store.status();
  stores.policy_ = std::move(*policy_store);
  auto grant_store = MetaGrantStore::Deserialize(*grant);
  if (!grant_store.ok()) return grant_store.status();
  stores.grant_ = std::move(*grant_store);
  auto operation_store = MetaOperationStore::Deserialize(*operation);
  if (!operation_store.ok()) return operation_store.status();
  stores.operation_ = std::move(*operation_store);
  auto population_manifest_store =
      MetaPopulationManifestStore::Deserialize(*population_manifest);
  if (!population_manifest_store.ok()) {
    return population_manifest_store.status();
  }
  stores.population_manifest_ = std::move(*population_manifest_store);
  auto audit_store = MetaAuditStore::Deserialize(*audit);
  if (!audit_store.ok()) return audit_store.status();
  stores.audit_ = std::move(*audit_store);
  if (absl::Status status = ValidateDecodedAggregate(stores); !status.ok()) {
    return status;
  }
  return stores;
}

MetaApplyResult ApplyCommitted(MetaStores& stores, std::uint64_t log_index,
                               const MetaCommand& command,
                               std::string_view actor_principal,
                               std::string_view readable_time) {
  MetaApplyResult result;
  result.log_index_ = log_index;
  // The variant alternative order matches the MetaCommandTag declaration
  // order exactly (tags 1..29), so the tag is the alternative index + 1. The
  // tests pin this mapping per command.
  result.command_tag_ = static_cast<MetaCommandTag>(command.index() + 1);

  // Guards for caller-contract violations (see the header): reject before
  // dispatch so committed state stays unchanged and identical on every node.
  // No audit write is possible in either case (index 0 is at/below the audit
  // floor; over-cap actor fields make the record unwritable).
  if (log_index == 0) {
    result.verdict_ = MetaAuditVerdict::kRejected;
    result.detail_ = "raft log index 0 is not a committed entry";
    return result;
  }
  if (actor_principal.size() > kMaxMetaPrincipalBytes ||
      readable_time.size() > kMaxMetaAuditReadableTimeBytes) {
    result.verdict_ = MetaAuditVerdict::kRejected;
    result.detail_ = "actor context exceeds the audit record caps";
    return result;
  }

  const ActorContext actor{std::string(actor_principal),
                           std::string(readable_time)};
  const ApplyOutcome outcome = std::visit(
      [&stores, log_index, &actor]<typename Cmd>(const Cmd& cmd) {
        // SubmitOperation is the one dispatch that persists the injected
        // ActorContext into the journal record, so it takes it explicitly.
        if constexpr (std::is_same_v<Cmd, SubmitOperation>) {
          return Dispatch(stores, log_index, cmd, actor);
        } else {
          return Dispatch(stores, log_index, cmd);
        }
      },
      command);
  if (outcome.verdict_ == MetaAuditVerdict::kAccepted) {
    // Directive validity is a derived cross-store invariant, so reconcile it
    // after every accepted mutation rather than relying on a hand-maintained
    // command list. The stores and directive collections are bounded, and an
    // already-current state is a no-op; this also makes future anchor-moving
    // commands fail closed by construction.
    InvalidateStaleCurrentDirectives(stores);
  }
  result.verdict_ = outcome.verdict_;
  result.detail_ = outcome.detail_;

  // Every privileged command appends its audit record, accepted or rejected.
  // Replay reproduces the identical record, so Append is an
  // idempotent no-op and the window does not grow.
  MetaAuditRecord record;
  record.log_index_ = log_index;
  record.actor_principal_ = std::string(actor_principal);
  record.command_summary_ = outcome.summary_;
  record.verdict_ = outcome.verdict_;
  record.verdict_detail_ = outcome.detail_;
  record.readable_time_ = std::string(readable_time);
  // Deterministic cap defense: summaries/details are bounded by construction
  // (bounded fields only); if a future store message ever outgrew the detail
  // cap, substitute deterministically rather than drop the record.
  if (record.command_summary_.size() > kMaxMetaAuditSummaryBytes) {
    record.command_summary_ = "command summary exceeded the audit cap";
  }
  if (record.verdict_detail_.size() > kMaxMetaAuditDetailBytes) {
    record.verdict_detail_ = "rejection detail exceeded the audit cap";
  }
  // Append's remaining failure modes are the store's wiring-bug fail-stops;
  // with the guards above and correct log-index wiring it cannot fail.
  const bool policy_change = std::holds_alternative<SetAuditPolicy>(command);
  const absl::Status audit_status =
      stores.audit_.Append(record, /*force_record=*/policy_change);
  (void)audit_status;
  return result;
}

std::string EncodeMetaApplyResult(const MetaApplyResult& result) {
  MetaWriter w;
  w.WriteU8(1);  // process-local completion payload version
  w.WriteU8(static_cast<std::uint8_t>(result.verdict_));
  w.WriteU64(result.log_index_);
  w.WriteU16(static_cast<std::uint16_t>(result.command_tag_));
  w.WriteString(result.detail_);
  return w.TakeBuffer();
}

absl::StatusOr<MetaApplyResult> DecodeMetaApplyResult(std::string_view bytes) {
  MetaReader r(bytes);
  auto version = r.ReadU8();
  if (!version.ok()) return version.status();
  if (*version != 1) {
    return MetaFailStopError("unknown apply-result payload version");
  }
  auto verdict = r.ReadU8();
  if (!verdict.ok()) return verdict.status();
  if (*verdict != static_cast<std::uint8_t>(MetaAuditVerdict::kAccepted) &&
      *verdict != static_cast<std::uint8_t>(MetaAuditVerdict::kRejected)) {
    return MetaFailStopError("unknown apply-result verdict");
  }
  auto log_index = r.ReadU64();
  if (!log_index.ok()) return log_index.status();
  auto command_tag = r.ReadU16();
  if (!command_tag.ok()) return command_tag.status();
  if (*command_tag <
          static_cast<std::uint16_t>(MetaCommandTag::kRegisterNode) ||
      *command_tag >
          static_cast<std::uint16_t>(MetaCommandTag::kPruneTerminalReceipts)) {
    return MetaFailStopError("unknown apply-result command tag");
  }
  auto detail = r.ReadString(kMaxMetaAuditDetailBytes);
  if (!detail.ok()) return detail.status();
  if (absl::Status status = r.Finish(); !status.ok()) return status;
  return MetaApplyResult{static_cast<MetaAuditVerdict>(*verdict),
                         std::string(*detail), *log_index,
                         static_cast<MetaCommandTag>(*command_tag)};
}

}  // namespace keylane::meta
