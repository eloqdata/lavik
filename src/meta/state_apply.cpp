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

#include "lavik/meta/state_apply.h"

#include <algorithm>
#include <array>
#include <limits>
#include <set>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "absl/strings/str_cat.h"
#include "lavik/cluster/control_protocol.h"
#include "lavik/meta/cluster_create.h"
#include "lavik/meta/failover.h"
#include "lavik/meta/hash.h"

namespace lavik::meta {

// Apply owns the state-machine write lock. Aggregate commands can therefore
// validate their prospective result in place without publishing intermediate
// state. Retain only affected Group/Operation records for domain rejection;
// identity, policy, manifests, audit, and unrelated groups are never copied.
// This guard covers authority/failover and operation/lifecycle transitions;
// those transitions do not change membership indexes or archive records.
class MetaApplyRollback {
 public:
  template <typename Command>
  MetaApplyRollback(MetaStores& stores, const Command& command)
      : stores_(stores),
        topology_epoch_(stores.topology_.topology_epoch_),
        lifecycle_(stores.topology_.cluster_lifecycle_),
        active_count_(stores.operation_.active_count_) {
    if constexpr (requires { command.group_id_; }) {
      group_id_ = command.group_id_;
      const auto group = stores.topology_.groups_.find(command.group_id_);
      if (group != stores.topology_.groups_.end()) {
        group_ = group->second;
      }
    }
    if constexpr (requires { command.operation_id_; }) {
      WatchOperation(command.operation_id_);
    }
  }

  MetaApplyRollback(const MetaApplyRollback&) = delete;
  MetaApplyRollback& operator=(const MetaApplyRollback&) = delete;

  void WatchOperation(const MetaOperationId& id) {
    if (operations_.contains(id)) return;
    operations_.emplace(id, stores_.operation_.FindOperation(id));
  }

  void Commit() { committed_ = true; }

  ~MetaApplyRollback() {
    if (committed_) return;
    stores_.topology_.topology_epoch_ = topology_epoch_;
    stores_.topology_.cluster_lifecycle_ = std::move(lifecycle_);
    if (group_.has_value()) {
      stores_.topology_.groups_.at(group_id_) = std::move(*group_);
    }
    for (auto& [id, before] : operations_) {
      auto current = stores_.operation_.live_.find(id);
      if (before.has_value()) {
        // Existing operation transitions preserve their sequence index and
        // map entry. Only the record's lifecycle/directives/results change.
        stores_.operation_.live_.at(id) = std::move(*before);
      } else if (current != stores_.operation_.live_.end()) {
        stores_.operation_.live_by_seq_.erase(current->second.operation_seq_);
        stores_.operation_.live_.erase(current);
      }
    }
    stores_.operation_.active_count_ = active_count_;
  }

 private:
  MetaStores& stores_;
  std::uint64_t topology_epoch_;
  MetaClusterLifecycleState lifecycle_;
  std::uint32_t active_count_;
  std::string group_id_;
  std::optional<MetaTopologyStore::GroupState> group_;
  std::map<MetaOperationId, std::optional<MetaOperationRecord>> operations_;
  bool committed_ = false;
};

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
  const bool initializes_empty =
      directive.kind_ == kMetaDirectiveInitializeEmptyPopulation;
  if (!stores.identity_.IsActiveNode(directive.recipient_node_id_) ||
      !stores.identity_.IsActiveNode(directive.target_node_id_) ||
      (!initializes_empty &&
       !stores.identity_.IsActiveNode(directive.source_node_id_))) {
    return MetaDomainRejectError(
        "directive recipient, source, or target is not active");
  }
  const auto group = stores.topology_.FindGroup(directive.group_id_);
  const auto grant = stores.topology_.AuthorityFor(directive.group_id_);
  if (!group.has_value() || !grant.has_value()) {
    return MetaDomainRejectError("directive group does not exist");
  }
  if (!grant->grant_.has_value()) {
    return MetaDomainRejectError("directive group has no active authority");
  }
  if (!HasAssignment(*group, directive.target_node_id_,
                     directive.assignment_id_) ||
      (!initializes_empty && !HasAssignment(*group, directive.source_node_id_,
                                            directive.source_assignment_id_))) {
    return MetaDomainRejectError("directive membership or assignment is stale");
  }
  const bool authority_matches =
      group->record_.group_term_ == directive.group_term_ &&
      grant->group_term_ == directive.group_term_;
  if (!authority_matches) {
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

void InvalidateStaleCurrentDirectives(MetaStores& stores,
                                      MetaApplyRollback* rollback = nullptr) {
  std::vector<MetaTerminalReceiptKey> invalidated;
  for (const MetaOperationRecord& operation :
       stores.operation_.LiveOperationsView()) {
    for (const MetaCurrentDirective& current : operation.current_directives_) {
      if (ValidateCommittedDirectiveAnchorImpl(stores, current.spec_).ok()) {
        continue;
      }
      if (rollback != nullptr)
        rollback->WatchOperation(operation.operation_id_);
      invalidated.push_back(MetaTerminalReceiptKey{
          operation.operation_id_, current.spec_.directive_id_,
          current.spec_.attempt_id_, current.directive_revision_});
    }
  }
  stores.operation_.InvalidateCurrentDirectives(invalidated);
}

bool AllCurrentDirectivesHaveCommittedAnchors(const MetaStores& stores) {
  for (const MetaOperationRecord& operation :
       stores.operation_.LiveOperationsView()) {
    for (const MetaCurrentDirective& current : operation.current_directives_) {
      if (!ValidateCommittedDirectiveAnchorImpl(stores, current.spec_).ok()) {
        return false;
      }
    }
  }
  return true;
}

// A PutPopulationManifest is the only unbounded-history insertion into the
// content-addressed store. Charge it against the exact bytes currently used
// by all six snapshot blobs, with room for the audit record ApplyCommitted
// appends after dispatch. This is an abuse ceiling tied to the durable format,
// not a workload-sizing guess.
std::uint64_t SnapshotBytesWithPopulationManifest(
    const MetaStores& stores,
    const MetaPopulationManifestStore& population_manifest) {
  return 2u + 6u * 4u + stores.identity_.SerializedSize() +
         stores.topology_.SerializedSize() + stores.policy_.SerializedSize() +
         stores.operation_.SerializedSize() +
         population_manifest.SerializedSize() + stores.audit_.SerializedSize();
}

constexpr std::uint64_t kMaximumAuditSnapshotGrowth =
    8u + (4u + kMaxMetaPrincipalBytes) + (4u + kMaxMetaAuditSummaryBytes) + 1u +
    (4u + kMaxMetaAuditDetailBytes) + (4u + kMaxMetaAuditReadableTimeBytes) +
    32u;

bool IsTerminal(MetaOperationLifecycle lifecycle) {
  return lifecycle == MetaOperationLifecycle::kCompleted ||
         lifecycle == MetaOperationLifecycle::kAborted;
}

// A terminal command may be replayed only when the retained operation is the
// same canonical Genesis root named by topology. Merely observing compatible
// terminal states on both stores is insufficient: a wrong kind, submit index,
// or embedded intent id would be an aggregate corruption that snapshot decode
// must reject as well.
bool LiveClusterCreateRootMatchesLifecycle(
    const MetaOperationRecord& root,
    const MetaClusterLifecycleState& lifecycle) {
  if (root.operation_id_ != lifecycle.root_operation_id_ ||
      root.kind_ != kMetaClusterCreateOperationKind ||
      root.operation_seq_ != lifecycle.genesis_commit_index_ ||
      root.intent_hash_ != MetaSha256(root.intent_)) {
    return false;
  }
  MetaOperationId intent_root{};
  const auto manifest = DecodeClusterCreateRequest(root.intent_, &intent_root);
  return manifest.ok() && intent_root == lifecycle.root_operation_id_;
}

bool ExistingClusterCreateEffectMatches(const MetaStores& stores,
                                        const MetaOperationId& operation_id,
                                        std::uint64_t log_index) {
  const auto& lifecycle = stores.topology_.ClusterLifecycle();
  if (lifecycle.state_ == MetaClusterLifecycle::kUninitialized ||
      lifecycle.root_operation_id_ != operation_id ||
      lifecycle.genesis_commit_index_ != log_index) {
    return false;
  }
  const auto live = stores.operation_.FindOperation(operation_id);
  const auto archived = stores.operation_.FindArchived(operation_id);
  switch (lifecycle.state_) {
    case MetaClusterLifecycle::kUninitialized:
      return false;
    case MetaClusterLifecycle::kCreating:
      return live.has_value() &&
             LiveClusterCreateRootMatchesLifecycle(*live, lifecycle) &&
             !IsTerminal(live->lifecycle_);
    case MetaClusterLifecycle::kCreated:
      return (live.has_value() &&
              LiveClusterCreateRootMatchesLifecycle(*live, lifecycle) &&
              live->lifecycle_ == MetaOperationLifecycle::kCompleted &&
              live->terminal_result_ == "cluster-created") ||
             (archived.has_value() && archived->operation_seq_ == log_index &&
              archived->terminal_lifecycle_ ==
                  MetaOperationLifecycle::kCompleted &&
              archived->terminal_result_ == "cluster-created");
    case MetaClusterLifecycle::kProvisioningFailed:
      return (live.has_value() &&
              LiveClusterCreateRootMatchesLifecycle(*live, lifecycle) &&
              live->lifecycle_ == MetaOperationLifecycle::kAborted) ||
             (archived.has_value() && archived->operation_seq_ == log_index &&
              archived->terminal_lifecycle_ ==
                  MetaOperationLifecycle::kAborted);
  }
  return false;
}

// Store decoders validate their own representation, but a snapshot is one
// committed aggregate: references and lockstep facts that ApplyCommitted
// protects must be re-established before recovery exposes any store.  Keep
// historical topology owners legal while fenced; only an active grant gives
// that field serving authority and therefore requires a live membership.
absl::Status ValidateDecodedAggregate(const MetaStores& stores) {
  const MetaClusterLifecycleState& lifecycle =
      stores.topology_.ClusterLifecycle();
  const auto root =
      stores.operation_.FindOperation(lifecycle.root_operation_id_);
  const auto archived_root =
      stores.operation_.FindArchived(lifecycle.root_operation_id_);
  const auto live_operations = stores.operation_.LiveOperationsView();
  if (lifecycle.state_ == MetaClusterLifecycle::kCreating) {
    if (!root.has_value() ||
        !LiveClusterCreateRootMatchesLifecycle(*root, lifecycle) ||
        IsTerminal(root->lifecycle_)) {
      return MetaFailStopError(
          "creating cluster lifecycle lacks its exact live root operation");
    }
    if (std::count_if(live_operations.begin(), live_operations.end(),
                      [](const auto& operation) {
                        return operation.kind_ ==
                               kMetaClusterCreateOperationKind;
                      }) != 1) {
      return MetaFailStopError(
          "creating cluster lifecycle does not have one unique live root");
    }
  } else if (lifecycle.state_ == MetaClusterLifecycle::kCreated ||
             lifecycle.state_ == MetaClusterLifecycle::kProvisioningFailed) {
    const MetaOperationLifecycle expected =
        lifecycle.state_ == MetaClusterLifecycle::kCreated
            ? MetaOperationLifecycle::kCompleted
            : MetaOperationLifecycle::kAborted;
    if (root.has_value() &&
        (!LiveClusterCreateRootMatchesLifecycle(*root, lifecycle) ||
         root->lifecycle_ != expected ||
         (expected == MetaOperationLifecycle::kCompleted &&
          root->terminal_result_ != "cluster-created"))) {
      return MetaFailStopError(
          "terminal cluster lifecycle disagrees with its live root");
    }
    if (archived_root.has_value() &&
        (archived_root->operation_seq_ != lifecycle.genesis_commit_index_ ||
         archived_root->terminal_lifecycle_ != expected ||
         (expected == MetaOperationLifecycle::kCompleted &&
          archived_root->terminal_result_ != "cluster-created"))) {
      return MetaFailStopError(
          "terminal cluster lifecycle disagrees with its archived root");
    }
    if (std::any_of(
            live_operations.begin(), live_operations.end(),
            [&](const auto& operation) {
              return operation.kind_ == kMetaClusterCreateOperationKind &&
                     (operation.operation_id_ != lifecycle.root_operation_id_ ||
                      !IsTerminal(operation.lifecycle_));
            })) {
      return MetaFailStopError(
          "terminal cluster lifecycle has another or active creation root");
    }
  }

  if (lifecycle.state_ == MetaClusterLifecycle::kCreated &&
      (!stores.policy_.CurrentAutomaticUncontrolledFailover().has_value() ||
       !stores.policy_.CurrentAuthorityLease().has_value() ||
       !stores.policy_.CurrentCandidateRecovery().has_value())) {
    return MetaFailStopError(
        "Created cluster lacks a registered current global Policy");
  }

  std::set<MetaOperationId> controlled_transition_operations;
  std::set<MetaFailoverTransitionId> failover_transition_ids;
  for (const MetaTopologyGroupView& group : stores.topology_.Groups()) {
    const auto grant = stores.topology_.AuthorityFor(group.group_id_);
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

    if (group.failover_transition_.has_value()) {
      const MetaFailoverTransition& transition = *group.failover_transition_;
      if (lifecycle.state_ != MetaClusterLifecycle::kCreated) {
        return MetaFailStopError(
            "active failover transition exists outside Created lifecycle");
      }
      if (absl::Status status = ValidateMetaFailoverTransition(transition);
          !status.ok()) {
        return MetaFailStopError(absl::StrCat(
            "active failover transition is invalid: ", status.message()));
      }
      if (!failover_transition_ids.insert(transition.transition_id_).second) {
        return MetaFailStopError(
            "active failover transition identity is not globally unique");
      }
      if (group.record_.owner_.empty() ||
          !stores.identity_.IsActiveNode(group.record_.owner_) ||
          !IsMember(group, group.record_.owner_) ||
          group.record_.group_term_ == 0) {
        return MetaFailStopError(
            "active failover transition lacks its exact historical owner "
            "authority");
      }
      if (transition.mode_ == MetaFailoverMode::kControlled) {
        if (group.record_.group_term_ ==
                std::numeric_limits<std::uint64_t>::max() ||
            transition.target_term_ != group.record_.group_term_ + 1 ||
            !grant->grant_.has_value()) {
          return MetaFailStopError(
              "controlled failover transition disagrees with current "
              "authority");
        }
        const MetaFailoverCandidateAction& action =
            *transition.candidate_action_;
        if (action.domain_.source_group_term_ != group.record_.group_term_ ||
            action.domain_.source_node_id_ != group.record_.owner_ ||
            !HasAssignment(group, action.domain_.source_node_id_,
                           action.domain_.source_assignment_id_)) {
          return MetaFailStopError(
              "controlled failover source domain is not the current owner "
              "assignment");
        }

        const MetaControlledFailover& controlled = *transition.controlled_;
        const auto operation =
            stores.operation_.FindOperation(controlled.operation_id_);
        if (!operation.has_value() ||
            operation->kind_ != kFailoverOperationKind ||
            operation->lifecycle_ != MetaOperationLifecycle::kSubmitted ||
            operation->revision_ != 0 ||
            operation->intent_hash_ != MetaSha256(operation->intent_) ||
            !operation->kind_phase_blob_.empty() ||
            !operation->current_directives_.empty() ||
            !operation->terminal_receipts_.empty() ||
            std::any_of(operation->replication_history_id_.begin(),
                        operation->replication_history_id_.end(),
                        [](std::uint8_t byte) { return byte != 0; })) {
          return MetaFailStopError(
              "controlled failover transition lacks its pristine submitted "
              "operation");
        }
        // Controlled Begin consumes an already-submitted operator request;
        // the transition therefore cannot precede or share its log index.
        if (transition.revision_ <= operation->operation_seq_) {
          return MetaFailStopError(
              "controlled failover transition revision does not follow its "
              "operation submission");
        }
        const auto intent = DecodeFailoverOperationIntent(operation->intent_);
        if (!intent.ok() || intent->group_id_ != group.group_id_ ||
            intent->absolute_deadline_unix_ms_ !=
                controlled.absolute_deadline_unix_ms_ ||
            !controlled_transition_operations.insert(controlled.operation_id_)
                 .second) {
          return MetaFailStopError(
              "controlled failover transition and operation intent disagree");
        }
      } else if (transition.target_term_ != group.record_.group_term_ ||
                 grant->grant_.has_value()) {
        return MetaFailStopError(
            "uncontrolled failover transition disagrees with fenced target "
            "term");
      }
      if (transition.candidate_action_.has_value()) {
        const MetaFailoverCandidate& candidate =
            transition.candidate_action_->candidate_;
        if (!stores.identity_.IsActiveNode(candidate.node_id_) ||
            !HasAssignment(group, candidate.node_id_,
                           candidate.assignment_id_)) {
          return MetaFailStopError(
              "active failover transition names a stale candidate "
              "membership");
        }
      }
    }

    if (!grant->grant_.has_value()) continue;
    const MetaActiveAuthorityView& active = *grant->grant_;
    if (group.record_.group_term_ == 0 || group.record_.owner_.empty() ||
        !stores.identity_.IsActiveNode(active.owner_) ||
        !IsMember(group, active.owner_)) {
      return MetaFailStopError(absl::StrCat(
          "active grant owner is inconsistent for group ", group.group_id_));
    }
  }

  for (const MetaOperationRecord& operation :
       stores.operation_.LiveOperationsView()) {
    if (operation.kind_ == kFailoverOperationKind) {
      const bool zero_history =
          std::all_of(operation.replication_history_id_.begin(),
                      operation.replication_history_id_.end(),
                      [](std::uint8_t byte) { return byte == 0; });
      const auto intent = DecodeFailoverOperationIntent(operation.intent_);
      const bool common_shape =
          intent.ok() &&
          operation.intent_hash_ == MetaSha256(operation.intent_) &&
          zero_history && operation.kind_phase_blob_.empty() &&
          operation.current_directives_.empty() &&
          operation.terminal_receipts_.empty();
      const bool submitted =
          operation.lifecycle_ == MetaOperationLifecycle::kSubmitted &&
          operation.revision_ == 0 && operation.terminal_result_.empty() &&
          !operation.data_loss_possible_;
      const bool completed =
          operation.lifecycle_ == MetaOperationLifecycle::kCompleted &&
          operation.revision_ == 1 &&
          operation.terminal_result_ == kFailoverCompletedResult &&
          !operation.data_loss_possible_;
      const bool aborted =
          operation.lifecycle_ == MetaOperationLifecycle::kAborted &&
          operation.revision_ == 1 && !operation.terminal_result_.empty() &&
          !operation.data_loss_possible_;
      if (!common_shape || (!submitted && !completed && !aborted)) {
        return MetaFailStopError(
            "failover operation violates the request-only typed lifecycle");
      }
    }
    if (operation.lifecycle_ == MetaOperationLifecycle::kCompleted ||
        operation.lifecycle_ == MetaOperationLifecycle::kAborted) {
      continue;
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

// Apply validates domain bounds directly. Network projection/encoding belongs
// to the publisher and must never allocate a full FDS during a Raft transition.
absl::Status ValidateAffectedNodeControls(
    const MetaStores& stores, std::uint64_t /*log_index*/,
    const std::set<std::string>& recipients) {
  if (recipients.empty()) return absl::OkStatus();
  if (!stores.policy_.CurrentAuthorityLease().has_value()) {
    return MetaDomainRejectError(
        "node control requires the Authority Lease policy");
  }
  std::map<std::string, std::size_t> counts;
  std::map<std::string, std::uint64_t> bytes;
  for (const auto& operation : stores.operation_.LiveOperationsView()) {
    if (IsTerminal(operation.lifecycle_)) continue;
    for (const auto& current : operation.current_directives_) {
      const auto& spec = current.spec_;
      if (!recipients.contains(spec.recipient_node_id_)) continue;
      if (++counts[spec.recipient_node_id_] >
          cluster::control::kMaxProjectedDirectives)
        return MetaDomainRejectError(
            "current directives exceeds its entry cap");
      // Reserve fixed identity/label overhead per task; bootstrap routing
      // and manifests retain half the object budget. Tasks travel individually
      // after initialization, so no wire projection is needed to check this.
      auto& used = bytes[spec.recipient_node_id_];
      used += spec.payload_.size() + spec.group_id_.size() + 1024u;
      if (used > cluster::control::kMaxFullDesiredStateBytes / 2)
        return MetaDomainRejectError(
            "current directives exceeds its byte budget");
    }
  }
  return absl::OkStatus();
}

// Whether the node owns active authority. The "grant owner => member of the
// group" invariant (maintained by the ActivateAuthority member check and by
// rejecting RemoveNodeFromGroup of a grant owner) lets the fact be read through
// the node's current group.
bool NodeHoldsActiveGrant(const MetaStores& stores,
                          const std::string& node_id) {
  const auto group = stores.topology_.FindGroupOfNode(node_id);
  if (!group.has_value()) return false;
  const auto state = stores.topology_.AuthorityFor(*group);
  return state.has_value() && state->grant_.has_value() &&
         state->grant_->owner_ == node_id;
}

bool GroupHasActiveGrant(const MetaStores& stores, std::string_view group_id) {
  const auto state = stores.topology_.AuthorityFor(group_id);
  return state.has_value() && state->grant_.has_value();
}

bool GroupHasActiveFailover(const MetaStores& stores,
                            const std::string& group_id) {
  const auto group = stores.topology_.FindGroup(group_id);
  return group.has_value() && group->failover_transition_.has_value();
}

absl::Status ApplyBeginGroupTermKernel(MetaStores& stores,
                                       const BeginGroupTerm& command) {
  return stores.topology_.BeginGroupTerm(command);
}

bool ClusterLifecycleAllowsFailover(const MetaStores& stores) {
  return stores.topology_.ClusterLifecycle().state_ ==
         MetaClusterLifecycle::kCreated;
}

absl::Status ValidateFailoverCandidateAgainstGroup(
    const MetaStores& stores, const MetaTopologyGroupView& group,
    const MetaFailoverCandidateAction& action) {
  if (!stores.identity_.IsActiveNode(action.candidate_.node_id_)) {
    return MetaDomainRejectError("failover candidate is not an active node");
  }
  if (!HasAssignment(group, action.candidate_.node_id_,
                     action.candidate_.assignment_id_)) {
    return MetaDomainRejectError(
        "failover candidate membership or assignment is stale");
  }
  return absl::OkStatus();
}

std::set<std::string> GroupRecipients(const MetaTopologyGroupView& group) {
  std::set<std::string> recipients;
  for (const MetaGroupMember& member : group.members_) {
    recipients.insert(member.node_id_);
  }
  return recipients;
}

absl::Status ApplyAuthorityActivationKernel(
    MetaStores& stores, const ActivateAuthority& cmd,
    std::optional<MetaFailoverActionId> activation_action_id);

bool TransitionMatches(const MetaFailoverTransition& transition,
                       const MetaFailoverTransitionRef& expected) {
  return transition.transition_id_ == expected.transition_id_ &&
         transition.revision_ == expected.revision_;
}

std::string_view FailoverLossName(MetaFailoverLoss loss) {
  return loss == MetaFailoverLoss::kNone ? "none" : "unknown";
}

bool IsPristineSubmittedFailoverOperation(
    const MetaOperationRecord& operation) {
  return operation.kind_ == kFailoverOperationKind &&
         operation.lifecycle_ == MetaOperationLifecycle::kSubmitted &&
         operation.revision_ == 0 &&
         operation.intent_hash_ == MetaSha256(operation.intent_) &&
         operation.kind_phase_blob_.empty() &&
         operation.current_directives_.empty() &&
         operation.terminal_receipts_.empty() &&
         std::all_of(operation.replication_history_id_.begin(),
                     operation.replication_history_id_.end(),
                     [](std::uint8_t byte) { return byte == 0; });
}

// This terminal result is part of the replicated Begin post-effect. It is
// intentionally independent of leader-local observations so replay and
// snapshot restoration see one stable reason for automatic preemption.
constexpr std::string_view kAutomaticFailoverPreemptionReason =
    "preempted by automatic uncontrolled failover";

bool FailoverOperationIntentMatches(const MetaOperationRecord& operation,
                                    std::string_view group_id,
                                    std::uint64_t* deadline = nullptr) {
  if (operation.kind_ != kFailoverOperationKind ||
      operation.intent_hash_ != MetaSha256(operation.intent_)) {
    return false;
  }
  const auto intent = DecodeFailoverOperationIntent(operation.intent_);
  if (!intent.ok() || intent->group_id_ != group_id) return false;
  if (deadline != nullptr) *deadline = intent->absolute_deadline_unix_ms_;
  return true;
}

std::vector<MetaOperationRecord> PreemptableControlledRequests(
    const MetaStores& stores, std::string_view group_id) {
  std::vector<MetaOperationRecord> requests;
  for (const MetaOperationRecord& operation :
       stores.operation_.LiveOperationsView()) {
    if (IsPristineSubmittedFailoverOperation(operation) &&
        FailoverOperationIntentMatches(operation, group_id)) {
      requests.push_back(operation);
    }
  }
  // operation_seq is the durable submission order; the id tie-break keeps the
  // traversal total even for a malformed in-memory aggregate that reused a
  // sequence. Apply remains deterministic before fail-stop validation.
  std::sort(requests.begin(), requests.end(),
            [](const auto& left, const auto& right) {
              return std::tie(left.operation_seq_, left.operation_id_) <
                     std::tie(right.operation_seq_, right.operation_id_);
            });
  return requests;
}

absl::Status ValidateAutomaticFailoverPreemption(
    const MetaStores& stores, const BeginUncontrolledFailover& command) {
  const bool has_operation = command.preempted_operation_id_.has_value();
  if (has_operation !=
      command.expected_preempted_operation_revision_.has_value()) {
    return MetaDomainRejectError(
        "preempted operation id and expected revision must be paired");
  }
  if (!has_operation) return absl::OkStatus();
  if (command.trigger_reason_ == MetaAutomaticFailoverReason::kManual) {
    return MetaDomainRejectError(
        "manual uncontrolled failover cannot preempt a controlled request");
  }

  const auto operation =
      stores.operation_.FindOperation(*command.preempted_operation_id_);
  if (!operation.has_value() ||
      !IsPristineSubmittedFailoverOperation(*operation) ||
      operation->revision_ != *command.expected_preempted_operation_revision_) {
    return MetaDomainRejectError(
        "preempted controlled failover operation CAS mismatch");
  }
  if (!FailoverOperationIntentMatches(*operation, command.group_id_)) {
    return MetaDomainRejectError(
        "preempted controlled failover operation targets another group");
  }
  return absl::OkStatus();
}

bool AutomaticFailoverPreemptionEffectPresent(
    const MetaStores& stores, const BeginUncontrolledFailover& command) {
  if (command.trigger_reason_ == MetaAutomaticFailoverReason::kManual) {
    return !command.preempted_operation_id_.has_value() &&
           !command.expected_preempted_operation_revision_.has_value();
  }
  bool hinted_effect_present = true;
  if (command.preempted_operation_id_.has_value()) {
    if (!command.expected_preempted_operation_revision_.has_value() ||
        *command.expected_preempted_operation_revision_ ==
            std::numeric_limits<std::uint64_t>::max()) {
      return false;
    }
    const auto operation =
        stores.operation_.FindOperation(*command.preempted_operation_id_);
    hinted_effect_present =
        operation.has_value() &&
        FailoverOperationIntentMatches(*operation, command.group_id_) &&
        operation->lifecycle_ == MetaOperationLifecycle::kAborted &&
        operation->revision_ ==
            *command.expected_preempted_operation_revision_ + 1 &&
        operation->terminal_result_ == kAutomaticFailoverPreemptionReason &&
        !operation->data_loss_possible_;
  }
  return hinted_effect_present &&
         PreemptableControlledRequests(stores, command.group_id_).empty();
}

bool FailoverAbortEffectPresent(const MetaStores& stores,
                                const AbortControlledFailover& command) {
  const auto operation = stores.operation_.FindOperation(command.operation_id_);
  if (!operation.has_value() ||
      !FailoverOperationIntentMatches(*operation, command.group_id_) ||
      command.expected_operation_revision_ ==
          std::numeric_limits<std::uint64_t>::max() ||
      operation->lifecycle_ != MetaOperationLifecycle::kAborted ||
      operation->revision_ != command.expected_operation_revision_ + 1 ||
      operation->terminal_result_ != command.reason_ ||
      operation->data_loss_possible_) {
    return false;
  }
  const auto group = stores.topology_.FindGroup(command.group_id_);
  if (!command.expected_transition_.has_value()) {
    const bool no_matching_transition =
        !group.has_value() || !group->failover_transition_.has_value() ||
        group->failover_transition_->mode_ != MetaFailoverMode::kControlled ||
        group->failover_transition_->controlled_->operation_id_ !=
            command.operation_id_;
    return no_matching_transition && ValidateDecodedAggregate(stores).ok();
  }
  return group.has_value() && !group->failover_transition_.has_value() &&
         ValidateDecodedAggregate(stores).ok();
}

absl::Status ValidateControlledFailoverOperation(
    const MetaStores& stores, const MetaOperationId& operation_id,
    std::uint64_t expected_revision, std::string_view group_id,
    std::uint64_t absolute_deadline_unix_ms) {
  const auto operation = stores.operation_.FindOperation(operation_id);
  if (!operation.has_value()) {
    return MetaDomainRejectError("unknown controlled failover operation");
  }
  if (!IsPristineSubmittedFailoverOperation(*operation) ||
      operation->revision_ != expected_revision) {
    return MetaDomainRejectError(
        "controlled failover operation is not pristine Submitted state");
  }
  const auto intent = DecodeFailoverOperationIntent(operation->intent_);
  if (!intent.ok() || intent->group_id_ != group_id ||
      intent->absolute_deadline_unix_ms_ != absolute_deadline_unix_ms) {
    return MetaDomainRejectError(
        "controlled failover operation intent or deadline mismatch");
  }
  return absl::OkStatus();
}

template <typename BeginCommand>
absl::Status ValidateFailoverBeginAnchors(
    const MetaStores& stores, const BeginCommand& cmd,
    const MetaTopologyGroupView& group,
    const MetaGroupAuthorityView& grant_state,
    const MetaFailoverCandidateAction* candidate_action) {
  if (group.failover_transition_.has_value()) {
    return MetaDomainRejectError("group already has a failover transition");
  }
  if (cmd.expected_group_term_ == std::numeric_limits<std::uint64_t>::max() ||
      cmd.target_term_ != cmd.expected_group_term_ + 1) {
    return MetaDomainRejectError(
        "failover target term must be exactly current term plus one");
  }
  if (group.record_.owner_ != cmd.expected_owner_node_id_ ||
      !HasAssignment(group, cmd.expected_owner_node_id_,
                     cmd.expected_owner_assignment_id_) ||
      group.revision_ != cmd.expected_membership_revision_ ||
      group.record_.group_term_ != cmd.expected_group_term_ ||
      group.record_.population_manifest_revision_ !=
          cmd.expected_population_manifest_revision_ ||
      group.record_.population_manifest_digest_ !=
          cmd.expected_population_manifest_digest_ ||
      group.record_.partition_replication_epoch_ !=
          cmd.expected_partition_replication_epoch_) {
    return MetaDomainRejectError("failover group anchor is stale");
  }
  if (!stores.identity_.IsActiveNode(cmd.expected_owner_node_id_)) {
    return MetaDomainRejectError("failover owner is not an active node");
  }
  if (grant_state.group_term_ != cmd.expected_group_term_ ||
      !grant_state.grant_.has_value()) {
    return MetaDomainRejectError("failover grant anchor is stale");
  }
  const MetaActiveAuthorityView& active = *grant_state.grant_;
  if (active.owner_ != cmd.expected_owner_node_id_) {
    return MetaDomainRejectError(
        "failover authority does not match the active grant");
  }
  if (candidate_action != nullptr) {
    if constexpr (std::is_same_v<BeginCommand, BeginControlledFailover>) {
      if (candidate_action->domain_.source_group_term_ !=
              cmd.expected_group_term_ ||
          candidate_action->domain_.source_node_id_ !=
              cmd.expected_owner_node_id_ ||
          candidate_action->domain_.source_assignment_id_ !=
              cmd.expected_owner_assignment_id_) {
        return MetaDomainRejectError(
            "controlled failover compatibility domain does not match the "
            "owner anchor");
      }
    }
    if (absl::Status status = ValidateFailoverCandidateAgainstGroup(
            stores, group, *candidate_action);
        !status.ok()) {
      return status;
    }
  }
  return absl::OkStatus();
}

MetaFailoverTransition ControlledTransitionFrom(
    const BeginControlledFailover& cmd, std::uint64_t revision) {
  MetaFailoverTransition transition;
  transition.transition_id_ = cmd.transition_id_;
  transition.revision_ = revision;
  transition.mode_ = MetaFailoverMode::kControlled;
  transition.target_term_ = cmd.target_term_;
  transition.candidate_action_ = cmd.candidate_action_;
  transition.controlled_ =
      MetaControlledFailover{cmd.operation_id_, cmd.absolute_deadline_unix_ms_};
  return transition;
}

bool BeginControlledEffectPresent(const MetaStores& stores,
                                  const BeginControlledFailover& cmd,
                                  std::uint64_t log_index) {
  if (!ClusterLifecycleAllowsFailover(stores)) return false;
  const auto group = stores.topology_.FindGroup(cmd.group_id_);
  const auto grant = stores.topology_.AuthorityFor(cmd.group_id_);
  if (!group.has_value() || !grant.has_value() ||
      !group->failover_transition_.has_value() ||
      *group->failover_transition_ !=
          ControlledTransitionFrom(cmd, log_index) ||
      group->record_.owner_ != cmd.expected_owner_node_id_ ||
      !HasAssignment(*group, cmd.expected_owner_node_id_,
                     cmd.expected_owner_assignment_id_) ||
      group->revision_ != cmd.expected_membership_revision_ ||
      group->record_.group_term_ != cmd.expected_group_term_ ||
      group->record_.population_manifest_revision_ !=
          cmd.expected_population_manifest_revision_ ||
      group->record_.population_manifest_digest_ !=
          cmd.expected_population_manifest_digest_ ||
      group->record_.partition_replication_epoch_ !=
          cmd.expected_partition_replication_epoch_ ||
      grant->group_term_ != cmd.expected_group_term_ ||
      !grant->grant_.has_value()) {
    return false;
  }
  const MetaActiveAuthorityView& active = *grant->grant_;
  return active.owner_ == cmd.expected_owner_node_id_ &&
         ValidateFailoverCandidateAgainstGroup(stores, *group,
                                               cmd.candidate_action_)
             .ok() &&
         ValidateControlledFailoverOperation(
             stores, cmd.operation_id_, cmd.expected_operation_revision_,
             cmd.group_id_, cmd.absolute_deadline_unix_ms_)
             .ok() &&
         ValidateDecodedAggregate(stores).ok();
}

MetaFailoverTransition UncontrolledTransitionFrom(
    const BeginUncontrolledFailover& cmd, std::uint64_t revision) {
  MetaFailoverTransition transition;
  transition.transition_id_ = cmd.transition_id_;
  transition.revision_ = revision;
  transition.mode_ = MetaFailoverMode::kUncontrolled;
  transition.target_term_ = cmd.target_term_;
  transition.candidate_action_ = cmd.candidate_action_;
  return transition;
}

bool BeginUncontrolledEffectPresent(const MetaStores& stores,
                                    const BeginUncontrolledFailover& cmd,
                                    std::uint64_t log_index) {
  if (!ClusterLifecycleAllowsFailover(stores)) return false;
  const auto group = stores.topology_.FindGroup(cmd.group_id_);
  const auto grant = stores.topology_.AuthorityFor(cmd.group_id_);
  if (!group.has_value() || !grant.has_value() ||
      !group->failover_transition_.has_value()) {
    return false;
  }
  const MetaFailoverTransition expected =
      UncontrolledTransitionFrom(cmd, log_index);
  if (*group->failover_transition_ != expected ||
      group->record_.owner_ != cmd.expected_owner_node_id_ ||
      !HasAssignment(*group, cmd.expected_owner_node_id_,
                     cmd.expected_owner_assignment_id_) ||
      group->revision_ != cmd.expected_membership_revision_ ||
      group->record_.group_term_ != cmd.target_term_ ||
      group->record_.population_manifest_revision_ !=
          cmd.expected_population_manifest_revision_ ||
      group->record_.population_manifest_digest_ !=
          cmd.expected_population_manifest_digest_ ||
      group->record_.partition_replication_epoch_ !=
          cmd.expected_partition_replication_epoch_ ||
      grant->group_term_ != cmd.target_term_ || grant->grant_.has_value()) {
    return false;
  }
  const bool candidate_valid = !cmd.candidate_action_.has_value() ||
                               ValidateFailoverCandidateAgainstGroup(
                                   stores, *group, *cmd.candidate_action_)
                                   .ok();
  return candidate_valid &&
         AutomaticFailoverPreemptionEffectPresent(stores, cmd) &&
         AllCurrentDirectivesHaveCommittedAnchors(stores) &&
         ValidateDecodedAggregate(stores).ok();
}

template <typename CommitCommand>
std::uint64_t FailoverCommitTargetTerm(const CommitCommand& command) {
  if constexpr (std::is_same_v<CommitCommand, CommitControlledFailover>) {
    return command.expected_group_term_ + 1;
  }
  return command.expected_group_term_;
}

template <typename CommitCommand>
bool FailoverCommitEffectPresent(const MetaStores& stores,
                                 const CommitCommand& command) {
  if (!ClusterLifecycleAllowsFailover(stores) ||
      (std::is_same_v<CommitCommand, CommitControlledFailover> &&
       command.expected_group_term_ ==
           std::numeric_limits<std::uint64_t>::max())) {
    return false;
  }
  const auto group = stores.topology_.FindGroup(command.group_id_);
  const auto grant = stores.topology_.AuthorityFor(command.group_id_);
  if (!group.has_value() || !grant.has_value() ||
      group->failover_transition_.has_value()) {
    return false;
  }
  const std::uint64_t target_term = FailoverCommitTargetTerm(command);
  if (group->record_.owner_ != command.expected_candidate_.node_id_ ||
      !HasAssignment(*group, command.expected_candidate_.node_id_,
                     command.expected_candidate_.assignment_id_) ||
      !HasAssignment(*group, command.expected_owner_node_id_,
                     command.expected_owner_assignment_id_) ||
      group->revision_ != command.expected_membership_revision_ ||
      group->record_.group_term_ != target_term ||
      group->record_.population_manifest_revision_ !=
          command.expected_population_manifest_revision_ ||
      group->record_.population_manifest_digest_ !=
          command.expected_population_manifest_digest_ ||
      group->record_.partition_replication_epoch_ !=
          command.expected_partition_replication_epoch_ ||
      stores.topology_.TopologyEpoch() != command.new_topology_epoch_ ||
      grant->group_term_ != target_term || !grant->grant_.has_value()) {
    return false;
  }
  const MetaActiveAuthorityView& active = *grant->grant_;
  if (active.owner_ != command.expected_candidate_.node_id_ ||
      active.activation_action_id_ != command.action_id_ ||
      !AllCurrentDirectivesHaveCommittedAnchors(stores)) {
    return false;
  }
  if constexpr (std::is_same_v<CommitCommand, CommitControlledFailover>) {
    const auto operation =
        stores.operation_.FindOperation(command.operation_id_);
    return operation.has_value() &&
           FailoverOperationIntentMatches(*operation, command.group_id_) &&
           operation->lifecycle_ == MetaOperationLifecycle::kCompleted &&
           command.expected_operation_revision_ !=
               std::numeric_limits<std::uint64_t>::max() &&
           operation->revision_ == command.expected_operation_revision_ + 1 &&
           operation->terminal_result_ == kFailoverCompletedResult &&
           !operation->data_loss_possible_ &&
           ValidateDecodedAggregate(stores).ok();
  }
  return ValidateDecodedAggregate(stores).ok();
}

template <typename CommitCommand>
absl::Status ValidateFailoverCommitPrestate(
    const MetaStores& stores, const CommitCommand& command,
    const MetaTopologyGroupView& group,
    const MetaGroupAuthorityView& grant_state,
    const MetaFailoverTransition& transition) {
  constexpr bool kControlled =
      std::is_same_v<CommitCommand, CommitControlledFailover>;
  if (!TransitionMatches(transition, command.expected_transition_) ||
      transition.mode_ != (kControlled ? MetaFailoverMode::kControlled
                                       : MetaFailoverMode::kUncontrolled) ||
      !transition.candidate_action_.has_value()) {
    return MetaDomainRejectError("failover commit transition CAS mismatch");
  }
  const MetaFailoverCandidateAction& action = *transition.candidate_action_;
  if (action.action_id_ != command.action_id_ ||
      action.candidate_ != command.expected_candidate_ ||
      !action.authorization_.has_value() ||
      action.authorization_->authorized_revision_ !=
          command.authorized_revision_) {
    return MetaDomainRejectError("failover commit action CAS mismatch");
  }
  if constexpr (kControlled) {
    if (action.authorization_->loss_if_cutover_ != MetaFailoverLoss::kNone) {
      return MetaDomainRejectError(
          "controlled failover commit must be lossless");
    }
  } else if (action.authorization_->loss_if_cutover_ !=
             command.loss_if_cutover_) {
    return MetaDomainRejectError(
        "uncontrolled failover loss result does not match authorization");
  }

  if (group.record_.owner_ != command.expected_owner_node_id_ ||
      !HasAssignment(group, command.expected_owner_node_id_,
                     command.expected_owner_assignment_id_) ||
      !HasAssignment(group, command.expected_candidate_.node_id_,
                     command.expected_candidate_.assignment_id_) ||
      !stores.identity_.IsActiveNode(command.expected_candidate_.node_id_) ||
      group.revision_ != command.expected_membership_revision_ ||
      group.record_.group_term_ != command.expected_group_term_ ||
      group.record_.population_manifest_revision_ !=
          command.expected_population_manifest_revision_ ||
      group.record_.population_manifest_digest_ !=
          command.expected_population_manifest_digest_ ||
      group.record_.partition_replication_epoch_ !=
          command.expected_partition_replication_epoch_ ||
      grant_state.group_term_ != command.expected_group_term_) {
    return MetaDomainRejectError("failover commit group anchor is stale");
  }
  if constexpr (kControlled) {
    if (!transition.controlled_.has_value() ||
        transition.controlled_->operation_id_ != command.operation_id_ ||
        transition.target_term_ != command.expected_group_term_ + 1 ||
        !grant_state.grant_.has_value() ||
        grant_state.grant_->owner_ != command.expected_owner_node_id_) {
      return MetaDomainRejectError(
          "controlled failover current authority is stale");
    }
    if (absl::Status status = ValidateControlledFailoverOperation(
            stores, command.operation_id_, command.expected_operation_revision_,
            command.group_id_,
            transition.controlled_->absolute_deadline_unix_ms_);
        !status.ok()) {
      return status;
    }
  } else if (transition.target_term_ != command.expected_group_term_ ||
             grant_state.grant_.has_value()) {
    return MetaDomainRejectError(
        "uncontrolled failover is not fenced in its target term");
  }
  return absl::OkStatus();
}

template <typename CommitCommand>
ApplyOutcome ApplyFailoverCommit(MetaStores& stores, std::uint64_t log_index,
                                 const CommitCommand& command) {
  constexpr bool kControlled =
      std::is_same_v<CommitCommand, CommitControlledFailover>;
  MetaFailoverLoss loss = MetaFailoverLoss::kNone;
  if constexpr (!kControlled) loss = command.loss_if_cutover_;
  std::string summary = absl::StrCat(
      kControlled ? "CommitControlledFailover" : "CommitUncontrolledFailover",
      " group=", command.group_id_,
      " transition=", HexBytes(command.expected_transition_.transition_id_),
      " action=", HexBytes(command.action_id_),
      " candidate=", command.expected_candidate_.node_id_,
      " loss=", FailoverLossName(loss), " index=", log_index);
  if (!ClusterLifecycleAllowsFailover(stores)) {
    return Rejected("failover requires cluster lifecycle Created",
                    std::move(summary));
  }
  if (FailoverCommitEffectPresent(stores, command)) {
    return Accepted(std::move(summary));
  }
  const auto group = stores.topology_.FindGroup(command.group_id_);
  const auto grant = stores.topology_.AuthorityFor(command.group_id_);
  if (!group.has_value() || !grant.has_value() ||
      !group->failover_transition_.has_value()) {
    return Rejected("failover commit transition is absent", std::move(summary));
  }
  const MetaFailoverTransition& transition = *group->failover_transition_;
  if (absl::Status status = ValidateFailoverCommitPrestate(
          stores, command, *group, *grant, transition);
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }

  MetaApplyRollback rollback(stores, command);
  const std::uint64_t target_term = transition.target_term_;
  if constexpr (kControlled) {
    BeginGroupTerm begin;
    begin.group_id_ = command.group_id_;
    begin.expected_term_ = command.expected_group_term_;
    begin.new_term_ = target_term;
    if (absl::Status status = ApplyBeginGroupTermKernel(stores, begin);
        !status.ok()) {
      return Rejected(status, std::move(summary));
    }
  }

  ActivateAuthority activate;
  activate.group_id_ = command.group_id_;
  activate.expected_term_ = target_term;
  activate.new_owner_ = command.expected_candidate_.node_id_;
  activate.new_topology_epoch_ = command.new_topology_epoch_;
  if (absl::Status status =
          ApplyAuthorityActivationKernel(stores, activate, command.action_id_);
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }
  if (absl::Status status = stores.topology_.ClearFailoverTransition(
          command.group_id_, command.expected_transition_);
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }
  if constexpr (kControlled) {
    CompleteOperation complete;
    complete.operation_id_ = command.operation_id_;
    complete.expected_revision_ = command.expected_operation_revision_;
    complete.result_ = std::string(kFailoverCompletedResult);
    complete.data_loss_possible_ = false;
    if (absl::Status status = stores.operation_.CompleteOperation(complete);
        !status.ok()) {
      return Rejected(status, std::move(summary));
    }
  }
  InvalidateStaleCurrentDirectives(stores, &rollback);
  if (absl::Status status = ValidateAffectedNodeControls(
          stores, log_index, GroupRecipients(*group));
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }
  rollback.Commit();
  return Accepted(std::move(summary));
}

// Genesis may be accepted only against an environment with no Data-cluster
// ownership facts. Meta identity/configuration and audit history are
// intentionally excluded: they are prerequisites and provenance, not Data
// cluster artifacts. Existing development snapshots predate the lifecycle
// field, so live legacy create operations are also treated as artifacts.
bool HasDataClusterArtifactsImpl(const MetaStores& stores) {
  if (stores.identity_.NodeCount() != 0 || stores.topology_.GroupCount() != 0 ||
      stores.population_manifest_.Size() != 0) {
    return true;
  }
  const auto operations = stores.operation_.LiveOperationsView();
  return std::any_of(
      operations.begin(), operations.end(), [](const auto& operation) {
        return operation.kind_ == kMetaClusterCreateOperationKind ||
               operation.kind_ == kMetaClusterCreateV1GroupOperationKind;
      });
}

std::string ClusterFailureSummary(const MetaOperationId& operation_id) {
  // Abort reasons may contain downstream error text or credentials. The
  // durable topology lifecycle therefore stores an allowlisted diagnostic;
  // the operation journal and Meta logs retain detailed troubleshooting data
  // only under their existing retention/access controls.
  return absl::StrCat("cluster-create provisioning failed; root-operation=",
                      HexBytes(operation_id));
}

// A live lease names the projection that granted it. Moving a slot into or
// out of that projection cannot ride the same authority: the old and new
// owners could otherwise accept the same slot
// until both sessions consume their replacement FullDesiredState. Inspect the
// command's absolute slot map before mutation; only a slot-sized table of
// borrowed group ids is needed, never a copy of the topology store.
absl::Status ValidateSlotMapAuthorityTransition(const MetaStores& current,
                                                const SetSlotMap& command) {
  std::array<std::string_view, kMetaSlotCount> target{};
  for (const MetaSlotAssignment& range : command.ranges_) {
    if (range.first_slot_ > range.last_slot_ ||
        range.last_slot_ >= kMetaSlotCount) {
      return MetaDomainRejectError("slot range out of bounds");
    }
    for (std::uint32_t slot = range.first_slot_; slot <= range.last_slot_;
         ++slot) {
      if (!target[slot].empty()) {
        return MetaDomainRejectError("overlapping slot ranges");
      }
      target[slot] = range.group_id_;
    }
  }
  std::set<std::string> affected_groups;
  for (std::uint32_t slot = 0; slot < kMetaSlotCount; ++slot) {
    const auto before = current.topology_.SlotOwner(slot);
    const std::string_view after = target[slot];
    if (before.value_or("") == after) continue;
    if (before.has_value()) affected_groups.insert(*before);
    if (!after.empty()) affected_groups.insert(std::string(after));
  }
  for (const std::string& group_id : affected_groups) {
    if (GroupHasActiveFailover(current, group_id)) {
      return MetaDomainRejectError(
          absl::StrCat("slot ownership change for group ", group_id,
                       " is blocked by its active failover transition"));
    }
    if (GroupHasActiveGrant(current, group_id)) {
      return MetaDomainRejectError(
          absl::StrCat("slot ownership change for group ", group_id,
                       " requires its active grant to be fenced first"));
    }
  }
  return absl::OkStatus();
}

// Replay requires the Group authority and cluster epoch to match the command.
bool ActivateEffectPresent(const MetaStores& stores,
                           const ActivateAuthority& cmd,
                           const MetaTopologyGroupView& /*view*/,
                           const MetaGroupAuthorityView& grant_state,
                           const std::optional<MetaFailoverActionId>&
                               activation_action_id = std::nullopt) {
  if (!grant_state.grant_.has_value()) return false;
  const MetaActiveAuthorityView& grant = *grant_state.grant_;
  const bool authority_matches =
      grant_state.group_term_ == cmd.expected_term_ &&
      grant.owner_ == cmd.new_owner_ &&
      grant.activation_action_id_ == activation_action_id;
  return authority_matches &&
         stores.topology_.TopologyEpoch() == cmd.new_topology_epoch_;
}

// Shared authority cutover kernel. It owns the same cross-store invariants for
// ordinary activation and failover activation; callers choose whether the
// installed grant is action-bound and perform any workflow-specific transition
// or Operation mutation within the same record-level rollback boundary.
absl::Status ApplyAuthorityActivationKernel(
    MetaStores& stores, const ActivateAuthority& cmd,
    std::optional<MetaFailoverActionId> activation_action_id) {
  const auto view = stores.topology_.FindGroup(cmd.group_id_);
  const auto grant_state = stores.topology_.AuthorityFor(cmd.group_id_);
  if (!view.has_value() || !grant_state.has_value()) {
    return MetaDomainRejectError(absl::StrCat("unknown group ", cmd.group_id_));
  }
  const bool effect_present = ActivateEffectPresent(
      stores, cmd, *view, *grant_state, activation_action_id);
  // The Grant is the one-shot marker for the current term. Only the complete
  // aggregate post-effect is a replay; matching just its Grant half must not
  // permit an active command to rewrite the topology epoch.
  if (grant_state->grant_.has_value() && !effect_present) {
    return MetaDomainRejectError(
        "group term already has a different authority effect");
  }
  if (absl::Status status =
          stores.topology_.ValidateActivate(cmd, activation_action_id);
      !status.ok()) {
    return status;
  }
  if (!effect_present) {
    const std::uint64_t epoch = stores.topology_.TopologyEpoch();
    if (epoch == std::numeric_limits<std::uint64_t>::max() ||
        cmd.new_topology_epoch_ != epoch + 1) {
      return MetaDomainRejectError(absl::StrCat(
          "new_topology_epoch must be exactly current+1 (", epoch, ")"));
    }
    if (!IsMember(*view, cmd.new_owner_)) {
      return MetaDomainRejectError(absl::StrCat(
          "new owner ", cmd.new_owner_, " is not a member of ", cmd.group_id_));
    }
    if (!stores.identity_.IsActiveNode(cmd.new_owner_)) {
      return MetaDomainRejectError(absl::StrCat(
          "new owner ", cmd.new_owner_, " is not a registered active node"));
    }
  }
  if (absl::Status status = stores.topology_.ActivateAuthority(
          cmd, std::move(activation_action_id));
      !status.ok()) {
    return status;
  }
  return stores.topology_.SetTopologyEpoch(cmd.new_topology_epoch_);
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
      current_node->endpoints_ == cmd.endpoints_;
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
// Topology owns the complete Group lifecycle, including its authority.
// ---------------------------------------------------------------------------

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const CreateGroup& cmd) {
  (void)log_index;
  const std::string summary =
      absl::StrCat("CreateGroup group=", cmd.group_id_,
                   " topology_epoch=", cmd.new_topology_epoch_);
  return FromStatus(stores.topology_.Apply(cmd), std::move(summary));
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
  if (const auto target = stores.topology_.FindGroup(cmd.group_id_);
      target.has_value() && target->failover_transition_.has_value()) {
    const bool replay =
        cmd.expected_revision_ != std::numeric_limits<std::uint64_t>::max() &&
        target->revision_ == cmd.expected_revision_ + 1 &&
        stores.topology_.TopologyEpoch() == cmd.new_topology_epoch_ &&
        std::any_of(target->members_.begin(), target->members_.end(),
                    [&cmd](const MetaGroupMember& member) {
                      return member.node_id_ == cmd.node_id_ &&
                             member.assignment_id_ == cmd.assignment_id_ &&
                             member.role_ == cmd.role_;
                    });
    if (!replay) {
      return Rejected(
          "group membership change is blocked by an active failover "
          "transition",
          std::move(summary));
    }
  }
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
  const auto grant_state = stores.topology_.AuthorityFor(cmd.group_id_);
  if (grant_state.has_value() && grant_state->grant_.has_value() &&
      grant_state->grant_->owner_ == cmd.node_id_) {
    return Rejected(absl::StrCat("node ", cmd.node_id_,
                                 " owns the active grant of ", cmd.group_id_),
                    std::move(summary));
  }
  if (const auto target = stores.topology_.FindGroup(cmd.group_id_);
      target.has_value() && target->failover_transition_.has_value()) {
    const bool member_absent =
        std::none_of(target->members_.begin(), target->members_.end(),
                     [&cmd](const MetaGroupMember& member) {
                       return member.node_id_ == cmd.node_id_;
                     });
    const bool replay =
        member_absent &&
        cmd.expected_revision_ != std::numeric_limits<std::uint64_t>::max() &&
        target->revision_ == cmd.expected_revision_ + 1 &&
        stores.topology_.TopologyEpoch() == cmd.new_topology_epoch_;
    if (!replay) {
      return Rejected(
          "group membership change is blocked by an active failover "
          "transition",
          std::move(summary));
    }
  }
  return FromStatus(stores.topology_.Apply(cmd), std::move(summary));
}

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const SetSlotMap& cmd) {
  (void)log_index;
  std::string summary =
      absl::StrCat("SetSlotMap ranges=", cmd.ranges_.size(),
                   " topology_epoch=", cmd.new_topology_epoch_);
  if (const absl::Status safe = ValidateSlotMapAuthorityTransition(stores, cmd);
      !safe.ok()) {
    return Rejected(safe, std::move(summary));
  }
  return FromStatus(stores.topology_.Apply(cmd), std::move(summary));
}

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const SetGroupReplicationState& cmd) {
  (void)log_index;
  std::string summary =
      absl::StrCat("SetGroupReplicationState group=", cmd.group_id_,
                   " manifest=", cmd.new_population_manifest_revision_,
                   " partition_epoch=", cmd.new_partition_replication_epoch_,
                   " topology_epoch=", cmd.new_topology_epoch_);
  if (const auto group = stores.topology_.FindGroup(cmd.group_id_);
      group.has_value() && group->failover_transition_.has_value()) {
    const bool replay =
        group->record_.population_manifest_revision_ ==
            cmd.new_population_manifest_revision_ &&
        group->record_.population_manifest_digest_ ==
            cmd.new_population_manifest_digest_ &&
        group->record_.partition_replication_epoch_ ==
            cmd.new_partition_replication_epoch_ &&
        stores.topology_.TopologyEpoch() == cmd.new_topology_epoch_;
    if (!replay) {
      return Rejected(
          "group replication-state change is blocked by an active failover "
          "transition",
          std::move(summary));
    }
  }
  if (cmd.new_population_manifest_revision_ != 0 &&
      !stores.population_manifest_.Contains(
          cmd.new_population_manifest_digest_)) {
    return Rejected("population manifest digest is not committed",
                    std::move(summary));
  }
  return FromStatus(stores.topology_.Apply(cmd), std::move(summary));
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

  if (cmd.entries_.size() > kMaxMetaPopulationManifestEntries) {
    return Rejected("population manifest entry cap exceeded",
                    std::move(summary));
  }
  // The only growth is this new digest/count/entry sequence. Check its exact
  // durable size before Put validates and inserts the immutable document.
  auto bytes =
      SnapshotBytesWithPopulationManifest(stores, stores.population_manifest_);
  const std::uint64_t growth = 32u + 4u + 12u * cmd.entries_.size();
  if (bytes > kMaxMetaSnapshotBytes ||
      growth + kMaximumAuditSnapshotGrowth > kMaxMetaSnapshotBytes - bytes) {
    return Rejected(
        "population manifest exceeds the remaining snapshot byte budget",
        std::move(summary));
  }
  return FromStatus(stores.population_manifest_.Put(cmd), std::move(summary));
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
// term/grant. BeginGroupTerm and ActivateAuthority are shared aggregate
// kernels: they update authority inside the committed Topology Group. Typed
// failover commands reuse the same kernels for fencing and cutover (file header
// item 3).
// ---------------------------------------------------------------------------

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const BeginGroupTerm& cmd) {
  (void)log_index;
  std::string summary =
      absl::StrCat("BeginGroupTerm group=", cmd.group_id_,
                   " term=", cmd.expected_term_, "->", cmd.new_term_);
  if (GroupHasActiveFailover(stores, cmd.group_id_)) {
    return Rejected(
        "group term change is blocked by an active failover transition",
        std::move(summary));
  }
  MetaApplyRollback rollback(stores, cmd);
  if (absl::Status status = ApplyBeginGroupTermKernel(stores, cmd);
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }
  InvalidateStaleCurrentDirectives(stores, &rollback);
  const auto group = stores.topology_.FindGroup(cmd.group_id_);
  std::set<std::string> recipients;
  if (group.has_value()) {
    for (const MetaGroupMember& member : group->members_) {
      recipients.insert(member.node_id_);
    }
  }
  if (absl::Status status =
          ValidateAffectedNodeControls(stores, log_index, recipients);
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }
  rollback.Commit();
  return Accepted(std::move(summary));
}

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const ActivateAuthority& cmd) {
  std::string summary = absl::StrCat(
      "ActivateAuthority group=", cmd.group_id_, " owner=", cmd.new_owner_,
      " expected_term=", cmd.expected_term_,
      " topology_epoch=", cmd.new_topology_epoch_);
  const auto view = stores.topology_.FindGroup(cmd.group_id_);
  const auto grant_state = stores.topology_.AuthorityFor(cmd.group_id_);
  const bool effect_present =
      view.has_value() && grant_state.has_value() &&
      ActivateEffectPresent(stores, cmd, *view, *grant_state);
  if (view.has_value() && view->failover_transition_.has_value() &&
      !effect_present) {
    return Rejected(
        "authority activation is blocked by an active failover transition",
        std::move(summary));
  }

  MetaApplyRollback rollback(stores, cmd);
  if (absl::Status status =
          ApplyAuthorityActivationKernel(stores, cmd, std::nullopt);
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }
  InvalidateStaleCurrentDirectives(stores, &rollback);
  std::set<std::string> recipients;
  if (view.has_value()) {
    for (const MetaGroupMember& member : view->members_) {
      recipients.insert(member.node_id_);
    }
  }
  if (absl::Status status =
          ValidateAffectedNodeControls(stores, log_index, recipients);
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }
  rollback.Commit();
  return Accepted(std::move(summary));
}

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const FenceGroup& cmd) {
  std::string summary =
      absl::StrCat("FenceGroup group=", cmd.group_id_,
                   " term=", cmd.expected_term_, "->", cmd.new_term_);
  if (GroupHasActiveFailover(stores, cmd.group_id_)) {
    return Rejected("fencing is blocked by an active failover transition",
                    std::move(summary));
  }
  MetaApplyRollback rollback(stores, cmd);
  BeginGroupTerm begin;
  begin.group_id_ = cmd.group_id_;
  begin.expected_term_ = cmd.expected_term_;
  begin.new_term_ = cmd.new_term_;
  if (absl::Status status = ApplyBeginGroupTermKernel(stores, begin);
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }
  InvalidateStaleCurrentDirectives(stores, &rollback);
  const auto group = stores.topology_.FindGroup(cmd.group_id_);
  std::set<std::string> recipients;
  if (group.has_value()) {
    for (const MetaGroupMember& member : group->members_) {
      recipients.insert(member.node_id_);
    }
  }
  if (absl::Status status =
          ValidateAffectedNodeControls(stores, log_index, recipients);
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }
  rollback.Commit();
  return Accepted(std::move(summary));
}

// ---------------------------------------------------------------------------
// Failover transitions are aggregate commands: each variant has a stable audit
// identity and validates every affected store before publishing any mutation.
// ---------------------------------------------------------------------------

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const BeginControlledFailover& cmd) {
  std::string summary =
      absl::StrCat("BeginControlledFailover group=", cmd.group_id_,
                   " transition=", HexBytes(cmd.transition_id_),
                   " operation=", HexBytes(cmd.operation_id_),
                   " target_term=", cmd.target_term_, " index=", log_index);
  if (!ClusterLifecycleAllowsFailover(stores)) {
    return Rejected("failover requires cluster lifecycle Created",
                    std::move(summary));
  }
  if (BeginControlledEffectPresent(stores, cmd, log_index)) {
    return Accepted(std::move(summary));
  }

  const auto group = stores.topology_.FindGroup(cmd.group_id_);
  const auto grant = stores.topology_.AuthorityFor(cmd.group_id_);
  if (!group.has_value() || !grant.has_value()) {
    return Rejected(absl::StrCat("unknown group ", cmd.group_id_),
                    std::move(summary));
  }
  if (absl::Status status = ValidateControlledFailoverOperation(
          stores, cmd.operation_id_, cmd.expected_operation_revision_,
          cmd.group_id_, cmd.absolute_deadline_unix_ms_);
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }
  if (absl::Status status = ValidateFailoverBeginAnchors(
          stores, cmd, *group, *grant, &cmd.candidate_action_);
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }

  MetaFailoverTransition transition = ControlledTransitionFrom(cmd, log_index);
  if (absl::Status status = ValidateMetaFailoverTransition(transition);
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }
  MetaApplyRollback rollback(stores, cmd);
  if (absl::Status status = stores.topology_.InstallFailoverTransition(
          cmd.group_id_, transition, log_index);
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }
  if (absl::Status status = ValidateAffectedNodeControls(
          stores, log_index, GroupRecipients(*group));
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }
  rollback.Commit();
  return Accepted(std::move(summary));
}

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const BeginUncontrolledFailover& cmd) {
  std::string summary = absl::StrCat(
      "BeginUncontrolledFailover group=", cmd.group_id_,
      " transition=", HexBytes(cmd.transition_id_),
      " target_term=", cmd.target_term_, " operator_recovery=",
      cmd.candidate_action_.has_value() &&
              cmd.candidate_action_->operator_recovery_
          ? 1
          : 0,
      " trigger=", MetaAutomaticFailoverReasonName(cmd.trigger_reason_),
      " suspect_ms=", cmd.suspect_duration_ms_, " preempted_operation=",
      cmd.preempted_operation_id_.has_value()
          ? HexBytes(*cmd.preempted_operation_id_)
          : std::string("none"));
  // Typed failover commands are meaningful only after Genesis has reached
  // Created. This is a deterministic apply gate, not merely a proposer-side
  // convenience, so a new Meta leader cannot resume work in another cluster
  // lifecycle.
  if (!ClusterLifecycleAllowsFailover(stores)) {
    return Rejected("failover requires cluster lifecycle Created",
                    std::move(summary));
  }
  // ApplyCommitted is also a public typed seam used independently of the WAL
  // decoder. Preserve the durable shape invariant here so bypassing command
  // encoding cannot install a preselected automatic Candidate.
  if (cmd.trigger_reason_ != MetaAutomaticFailoverReason::kManual &&
      cmd.candidate_action_.has_value()) {
    return Rejected(
        "automatic uncontrolled failover begin must be candidate-less",
        std::move(summary));
  }

  // Replay is checked against the complete compound post-state before any
  // precondition. A later log index carrying the same payload is not a replay:
  // the persisted transition revision still names the original entry.
  if (BeginUncontrolledEffectPresent(stores, cmd, log_index)) {
    return Accepted(std::move(summary));
  }

  if (absl::Status status = ValidateAutomaticFailoverPreemption(stores, cmd);
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }

  const auto group = stores.topology_.FindGroup(cmd.group_id_);
  const auto grant = stores.topology_.AuthorityFor(cmd.group_id_);
  if (!group.has_value() || !grant.has_value()) {
    return Rejected(absl::StrCat("unknown group ", cmd.group_id_),
                    std::move(summary));
  }
  const MetaFailoverCandidateAction* candidate_action =
      cmd.candidate_action_.has_value() ? &*cmd.candidate_action_ : nullptr;
  if (absl::Status status = ValidateFailoverBeginAnchors(
          stores, cmd, *group, *grant, candidate_action);
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }

  MetaFailoverTransition transition =
      UncontrolledTransitionFrom(cmd, log_index);
  if (absl::Status status = ValidateMetaFailoverTransition(transition);
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }

  // Apply only the affected records under the state-machine write lock;
  // rollback retains those records until post-state validation succeeds.
  // BeginGroupTerm is the existing grant-state kernel: it moves T -> T+1 and
  // fences. Topology mirrors the term and owns the transition; the historical
  // owner remains unchanged until cutover.
  MetaApplyRollback rollback(stores, cmd);
  BeginGroupTerm begin_term;
  begin_term.group_id_ = cmd.group_id_;
  begin_term.expected_term_ = cmd.expected_group_term_;
  begin_term.new_term_ = cmd.target_term_;
  if (absl::Status status = ApplyBeginGroupTermKernel(stores, begin_term);
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }
  if (absl::Status status = stores.topology_.InstallFailoverTransition(
          cmd.group_id_, transition, log_index);
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }
  // The optional command pair is a proposer-observed CAS witness, not the
  // complete mutation set. Apply scans the bounded journal so any pristine
  // request that won the append race after proposal construction is aborted
  // in the same atomic delta as the emergency transition.
  if (cmd.trigger_reason_ != MetaAutomaticFailoverReason::kManual) {
    for (const MetaOperationRecord& operation :
         PreemptableControlledRequests(stores, cmd.group_id_)) {
      AbortOperation abort;
      abort.operation_id_ = operation.operation_id_;
      abort.expected_revision_ = operation.revision_;
      abort.reason_ = std::string(kAutomaticFailoverPreemptionReason);
      rollback.WatchOperation(abort.operation_id_);
      if (absl::Status status = stores.operation_.AbortOperation(abort);
          !status.ok()) {
        return Rejected(status, std::move(summary));
      }
    }
  }

  // Advancing the group term invalidates every directive anchored to the old
  // authority. Remove those directives in the same atomic delta before
  // projecting FDS: otherwise projection correctly rejects the stale anchor
  // and an emergency failover can never commit while old work is installed.
  // Publishing operation_ together with topology_/grant_ also makes the
  // invalidation atomic across a Meta leader restart.
  InvalidateStaleCurrentDirectives(stores, &rollback);

  std::set<std::string> affected_recipients;
  for (const MetaGroupMember& member : group->members_) {
    affected_recipients.insert(member.node_id_);
  }
  if (absl::Status status =
          ValidateAffectedNodeControls(stores, log_index, affected_recipients);
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }
  rollback.Commit();
  return Accepted(std::move(summary));
}

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const SetUncontrolledCandidate& cmd) {
  std::string summary = absl::StrCat(
      "SetUncontrolledCandidate group=", cmd.group_id_,
      " transition=", HexBytes(cmd.expected_transition_.transition_id_),
      " action=",
      cmd.candidate_action_.has_value()
          ? HexBytes(cmd.candidate_action_->action_id_)
          : std::string("none"),
      " operator_recovery=",
      cmd.candidate_action_.has_value() &&
              cmd.candidate_action_->operator_recovery_
          ? 1
          : 0,
      " index=", log_index);
  if (!ClusterLifecycleAllowsFailover(stores)) {
    return Rejected("failover requires cluster lifecycle Created",
                    std::move(summary));
  }
  if (log_index <= cmd.expected_transition_.revision_) {
    return Rejected("candidate replacement revision must advance",
                    std::move(summary));
  }
  const auto group = stores.topology_.FindGroup(cmd.group_id_);
  const auto grant = stores.topology_.AuthorityFor(cmd.group_id_);
  if (!group.has_value() || !grant.has_value() ||
      !group->failover_transition_.has_value()) {
    return Rejected("uncontrolled failover transition is absent",
                    std::move(summary));
  }
  const MetaFailoverTransition& current = *group->failover_transition_;
  const bool post_effect =
      current.transition_id_ == cmd.expected_transition_.transition_id_ &&
      current.revision_ == log_index &&
      current.mode_ == MetaFailoverMode::kUncontrolled &&
      current.candidate_action_ == cmd.candidate_action_ &&
      current.target_term_ == group->record_.group_term_ &&
      !grant->grant_.has_value() &&
      ValidateMetaFailoverTransition(current).ok() &&
      ValidateDecodedAggregate(stores).ok();
  if (post_effect) return Accepted(std::move(summary));

  if (!TransitionMatches(current, cmd.expected_transition_) ||
      current.mode_ != MetaFailoverMode::kUncontrolled) {
    return Rejected("uncontrolled failover transition CAS mismatch",
                    std::move(summary));
  }
  // Clearing an installed action is an auditable lifecycle event. A no-op
  // null-to-null revision advance has no action identity to attribute and
  // would make an exact replay indistinguishable from that first application.
  if (!cmd.candidate_action_.has_value() &&
      !current.candidate_action_.has_value()) {
    return Rejected("uncontrolled failover candidate is absent",
                    std::move(summary));
  }
  if (cmd.candidate_action_.has_value()) {
    if (current.candidate_action_.has_value() &&
        current.candidate_action_->action_id_ ==
            cmd.candidate_action_->action_id_) {
      return Rejected("replacement failover action id must be fresh",
                      std::move(summary));
    }
    if (absl::Status status = ValidateFailoverCandidateAgainstGroup(
            stores, *group, *cmd.candidate_action_);
        !status.ok()) {
      return Rejected(status, std::move(summary));
    }
  }
  MetaFailoverTransition replacement = current;
  replacement.revision_ = log_index;
  replacement.candidate_action_ = cmd.candidate_action_;
  if (absl::Status status = ValidateMetaFailoverTransition(replacement);
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }
  MetaApplyRollback rollback(stores, cmd);
  if (absl::Status status = stores.topology_.ReplaceFailoverTransition(
          cmd.group_id_, cmd.expected_transition_, replacement, log_index);
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }
  if (absl::Status status = ValidateAffectedNodeControls(
          stores, log_index, GroupRecipients(*group));
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }
  rollback.Commit();
  return Accepted(std::move(summary));
}

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const StartCandidateRecovery& cmd) {
  std::string summary = absl::StrCat(
      "StartCandidateRecovery group=", cmd.group_id_,
      " transition=", HexBytes(cmd.expected_transition_.transition_id_),
      " action=", HexBytes(cmd.action_id_),
      " deadline=", cmd.recovery_deadline_unix_ms_, " index=", log_index);
  if (!ClusterLifecycleAllowsFailover(stores) ||
      log_index <= cmd.expected_transition_.revision_) {
    return Rejected(
        "recovery requires Created lifecycle and advancing revision",
        std::move(summary));
  }
  const auto group = stores.topology_.FindGroup(cmd.group_id_);
  const auto grant = stores.topology_.AuthorityFor(cmd.group_id_);
  if (!group.has_value() || !grant.has_value() || grant->grant_.has_value() ||
      !group->failover_transition_.has_value()) {
    return Rejected("recovery requires a fenced failover transition",
                    std::move(summary));
  }
  const auto& current = *group->failover_transition_;
  if (current.mode_ != MetaFailoverMode::kUncontrolled ||
      !current.candidate_action_.has_value() ||
      current.candidate_action_->action_id_ != cmd.action_id_ ||
      current.candidate_action_->operator_recovery_ ||
      current.candidate_action_->authorization_.has_value()) {
    return Rejected("recovery requires an unauthorized uncontrolled action",
                    std::move(summary));
  }
  if (current.transition_id_ == cmd.expected_transition_.transition_id_ &&
      current.revision_ == log_index &&
      current.recovery_deadline_unix_ms_ == cmd.recovery_deadline_unix_ms_ &&
      ValidateDecodedAggregate(stores).ok())
    return Accepted(std::move(summary));
  if (!TransitionMatches(current, cmd.expected_transition_) ||
      current.recovery_deadline_unix_ms_.has_value()) {
    return Rejected(
        "recovery cutoff is already fixed or transition CAS is stale",
        std::move(summary));
  }
  auto replacement = current;
  replacement.revision_ = log_index;
  replacement.recovery_deadline_unix_ms_ = cmd.recovery_deadline_unix_ms_;
  if (auto status = ValidateMetaFailoverTransition(replacement); !status.ok()) {
    return Rejected(status, std::move(summary));
  }
  MetaApplyRollback rollback(stores, cmd);
  if (auto status = stores.topology_.ReplaceFailoverTransition(
          cmd.group_id_, cmd.expected_transition_, replacement, log_index);
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }
  if (auto status = ValidateAffectedNodeControls(stores, log_index,
                                                 GroupRecipients(*group));
      !status.ok())
    return Rejected(status, std::move(summary));
  rollback.Commit();
  return Accepted(std::move(summary));
}

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const AuthorizeFailoverPrepare& cmd) {
  std::string summary = absl::StrCat(
      "AuthorizeFailoverPrepare group=", cmd.group_id_,
      " transition=", HexBytes(cmd.expected_transition_.transition_id_),
      " action=", HexBytes(cmd.action_id_),
      " loss=", FailoverLossName(cmd.loss_if_cutover_), " index=", log_index);
  if (!ClusterLifecycleAllowsFailover(stores)) {
    return Rejected("failover requires cluster lifecycle Created",
                    std::move(summary));
  }
  if (log_index <= cmd.expected_transition_.revision_) {
    return Rejected("authorization revision must advance", std::move(summary));
  }
  const auto group = stores.topology_.FindGroup(cmd.group_id_);
  if (!group.has_value() || !group->failover_transition_.has_value()) {
    return Rejected("failover transition is absent", std::move(summary));
  }
  const MetaFailoverTransition& current = *group->failover_transition_;
  const bool post_effect =
      current.transition_id_ == cmd.expected_transition_.transition_id_ &&
      current.revision_ == log_index && current.candidate_action_.has_value() &&
      current.candidate_action_->action_id_ == cmd.action_id_ &&
      current.candidate_action_->authorization_ ==
          std::optional<MetaFailoverAuthorization>(
              MetaFailoverAuthorization{log_index, cmd.loss_if_cutover_}) &&
      ValidateMetaFailoverTransition(current).ok() &&
      ValidateDecodedAggregate(stores).ok();
  if (post_effect) return Accepted(std::move(summary));

  if (!TransitionMatches(current, cmd.expected_transition_) ||
      !current.candidate_action_.has_value() ||
      current.candidate_action_->action_id_ != cmd.action_id_) {
    return Rejected("failover authorization CAS mismatch", std::move(summary));
  }
  if (current.candidate_action_->authorization_.has_value()) {
    return Rejected("failover action is already authorized",
                    std::move(summary));
  }
  if (current.mode_ == MetaFailoverMode::kUncontrolled &&
      !current.candidate_action_->operator_recovery_ &&
      !current.recovery_deadline_unix_ms_.has_value()) {
    return Rejected("uncontrolled prepare requires recovery start",
                    std::move(summary));
  }
  if ((current.mode_ == MetaFailoverMode::kControlled &&
       cmd.loss_if_cutover_ != MetaFailoverLoss::kNone) ||
      (current.mode_ == MetaFailoverMode::kUncontrolled &&
       cmd.loss_if_cutover_ != MetaFailoverLoss::kUnknown)) {
    return Rejected("authorization loss does not match failover mode",
                    std::move(summary));
  }
  MetaFailoverTransition replacement = current;
  replacement.revision_ = log_index;
  replacement.candidate_action_->authorization_ =
      MetaFailoverAuthorization{log_index, cmd.loss_if_cutover_};
  if (absl::Status status = ValidateMetaFailoverTransition(replacement);
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }
  MetaApplyRollback rollback(stores, cmd);
  if (absl::Status status = stores.topology_.ReplaceFailoverTransition(
          cmd.group_id_, cmd.expected_transition_, replacement, log_index);
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }
  if (absl::Status status = ValidateAffectedNodeControls(
          stores, log_index, GroupRecipients(*group));
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }
  rollback.Commit();
  return Accepted(std::move(summary));
}

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const AbortControlledFailover& cmd) {
  std::string summary =
      absl::StrCat("AbortControlledFailover group=", cmd.group_id_,
                   " operation=", HexBytes(cmd.operation_id_), " transition=",
                   cmd.expected_transition_.has_value()
                       ? HexBytes(cmd.expected_transition_->transition_id_)
                       : std::string("none"),
                   " index=", log_index);
  if (!ClusterLifecycleAllowsFailover(stores)) {
    return Rejected("failover requires cluster lifecycle Created",
                    std::move(summary));
  }
  if (FailoverAbortEffectPresent(stores, cmd)) {
    return Accepted(std::move(summary));
  }

  const auto operation = stores.operation_.FindOperation(cmd.operation_id_);
  std::uint64_t deadline = 0;
  if (!operation.has_value() ||
      !FailoverOperationIntentMatches(*operation, cmd.group_id_, &deadline) ||
      !IsPristineSubmittedFailoverOperation(*operation) ||
      operation->revision_ != cmd.expected_operation_revision_) {
    return Rejected("controlled failover operation CAS mismatch",
                    std::move(summary));
  }

  const auto group = stores.topology_.FindGroup(cmd.group_id_);
  if (!cmd.expected_transition_.has_value()) {
    if (group.has_value() && group->failover_transition_.has_value()) {
      const MetaFailoverTransition& transition = *group->failover_transition_;
      if (transition.mode_ == MetaFailoverMode::kControlled &&
          transition.controlled_->operation_id_ == cmd.operation_id_) {
        return Rejected(
            "pre-Begin abort cannot clear an installed controlled transition",
            std::move(summary));
      }
    }
  } else {
    if (!group.has_value() || !group->failover_transition_.has_value() ||
        !TransitionMatches(*group->failover_transition_,
                           *cmd.expected_transition_) ||
        group->failover_transition_->mode_ != MetaFailoverMode::kControlled ||
        group->failover_transition_->controlled_->operation_id_ !=
            cmd.operation_id_ ||
        group->failover_transition_->controlled_->absolute_deadline_unix_ms_ !=
            deadline) {
      return Rejected("post-Begin controlled transition CAS mismatch",
                      std::move(summary));
    }
  }

  MetaApplyRollback rollback(stores, cmd);
  AbortOperation abort;
  abort.operation_id_ = cmd.operation_id_;
  abort.expected_revision_ = cmd.expected_operation_revision_;
  abort.reason_ = cmd.reason_;
  if (absl::Status status = stores.operation_.AbortOperation(abort);
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }
  if (cmd.expected_transition_.has_value()) {
    if (absl::Status status = stores.topology_.ClearFailoverTransition(
            cmd.group_id_, *cmd.expected_transition_);
        !status.ok()) {
      return Rejected(status, std::move(summary));
    }
  }
  const std::set<std::string> recipients =
      group.has_value() ? GroupRecipients(*group) : std::set<std::string>{};
  if (absl::Status status =
          ValidateAffectedNodeControls(stores, log_index, recipients);
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }
  rollback.Commit();
  return Accepted(std::move(summary));
}

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const DegradeControlledFailover& cmd) {
  std::string summary = absl::StrCat(
      "DegradeControlledFailover group=", cmd.group_id_,
      " operation=", HexBytes(cmd.operation_id_),
      " transition=", HexBytes(cmd.expected_transition_.transition_id_),
      " retain_action=", cmd.retain_candidate_action_ ? 1 : 0,
      " index=", log_index);
  if (!ClusterLifecycleAllowsFailover(stores)) {
    return Rejected("failover requires cluster lifecycle Created",
                    std::move(summary));
  }
  if (log_index <= cmd.expected_transition_.revision_) {
    return Rejected("degrade revision must advance", std::move(summary));
  }

  const auto group = stores.topology_.FindGroup(cmd.group_id_);
  const auto grant = stores.topology_.AuthorityFor(cmd.group_id_);
  const auto operation = stores.operation_.FindOperation(cmd.operation_id_);
  const std::optional<MetaFailoverCandidateAction> expected_post_action =
      cmd.retain_candidate_action_ ? cmd.expected_candidate_action_
                                   : std::nullopt;
  const bool operation_post =
      operation.has_value() &&
      FailoverOperationIntentMatches(*operation, cmd.group_id_) &&
      cmd.expected_operation_revision_ !=
          std::numeric_limits<std::uint64_t>::max() &&
      operation->lifecycle_ == MetaOperationLifecycle::kAborted &&
      operation->revision_ == cmd.expected_operation_revision_ + 1 &&
      operation->terminal_result_ == cmd.reason_ &&
      !operation->data_loss_possible_;
  const bool transition_post =
      group.has_value() && grant.has_value() &&
      group->failover_transition_.has_value() &&
      group->failover_transition_->transition_id_ ==
          cmd.expected_transition_.transition_id_ &&
      group->failover_transition_->revision_ == log_index &&
      group->failover_transition_->mode_ == MetaFailoverMode::kUncontrolled &&
      !group->failover_transition_->controlled_.has_value() &&
      group->failover_transition_->candidate_action_ == expected_post_action &&
      group->record_.group_term_ == group->failover_transition_->target_term_ &&
      grant->group_term_ == group->failover_transition_->target_term_ &&
      !grant->grant_.has_value() &&
      AllCurrentDirectivesHaveCommittedAnchors(stores) &&
      ValidateMetaFailoverTransition(*group->failover_transition_).ok() &&
      ValidateDecodedAggregate(stores).ok();
  if (operation_post && transition_post) {
    return Accepted(std::move(summary));
  }

  if (!operation.has_value() ||
      !IsPristineSubmittedFailoverOperation(*operation) ||
      operation->revision_ != cmd.expected_operation_revision_) {
    return Rejected("controlled failover operation CAS mismatch",
                    std::move(summary));
  }
  std::uint64_t deadline = 0;
  if (!FailoverOperationIntentMatches(*operation, cmd.group_id_, &deadline)) {
    return Rejected("controlled failover operation intent mismatch",
                    std::move(summary));
  }
  if (!group.has_value() || !grant.has_value() ||
      !group->failover_transition_.has_value()) {
    return Rejected("controlled failover transition is absent",
                    std::move(summary));
  }
  const MetaFailoverTransition& current = *group->failover_transition_;
  if (!TransitionMatches(current, cmd.expected_transition_) ||
      current.mode_ != MetaFailoverMode::kControlled ||
      current.controlled_->operation_id_ != cmd.operation_id_ ||
      current.controlled_->absolute_deadline_unix_ms_ != deadline ||
      current.candidate_action_ != cmd.expected_candidate_action_ ||
      group->record_.group_term_ == std::numeric_limits<std::uint64_t>::max() ||
      current.target_term_ != group->record_.group_term_ + 1 ||
      grant->group_term_ != group->record_.group_term_ ||
      !grant->grant_.has_value()) {
    return Rejected("controlled failover degrade CAS mismatch",
                    std::move(summary));
  }

  MetaApplyRollback rollback(stores, cmd);
  BeginGroupTerm begin;
  begin.group_id_ = cmd.group_id_;
  begin.expected_term_ = group->record_.group_term_;
  begin.new_term_ = current.target_term_;
  if (absl::Status status = ApplyBeginGroupTermKernel(stores, begin);
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }
  MetaFailoverTransition replacement = current;
  replacement.revision_ = log_index;
  replacement.mode_ = MetaFailoverMode::kUncontrolled;
  replacement.controlled_.reset();
  replacement.candidate_action_ = expected_post_action;
  if (absl::Status status = ValidateMetaFailoverTransition(replacement);
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }
  if (absl::Status status = stores.topology_.ReplaceFailoverTransition(
          cmd.group_id_, cmd.expected_transition_, replacement, log_index);
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }
  AbortOperation abort;
  abort.operation_id_ = cmd.operation_id_;
  abort.expected_revision_ = cmd.expected_operation_revision_;
  abort.reason_ = cmd.reason_;
  if (absl::Status status = stores.operation_.AbortOperation(abort);
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }
  InvalidateStaleCurrentDirectives(stores, &rollback);
  if (absl::Status status = ValidateAffectedNodeControls(
          stores, log_index, GroupRecipients(*group));
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }
  rollback.Commit();
  return Accepted(std::move(summary));
}

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const CommitControlledFailover& cmd) {
  return ApplyFailoverCommit(stores, log_index, cmd);
}

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const CommitUncontrolledFailover& cmd) {
  return ApplyFailoverCommit(stores, log_index, cmd);
}

// ---------------------------------------------------------------------------
// Global registered Policy families. Raw content is validated and versioned
// entirely by MetaPolicyStore.
// ---------------------------------------------------------------------------

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const PutPolicy& cmd) {
  (void)log_index;
  return FromStatus(
      stores.policy_.Apply(cmd),
      absl::StrCat("PutPolicy policy=", cmd.policy_id_,
                   " version=", cmd.version_, " bytes=", cmd.content_.size()));
}

// ---------------------------------------------------------------------------
// operation journal. The operation store owns the lifecycle
// machine. The dispatcher supplies the log index and actor and validates
// each current directive against committed membership and population anchors.
// ---------------------------------------------------------------------------

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const SubmitOperation& cmd, const ActorContext& actor) {
  // The summary must be a pure function of (command, log index): a replay
  // resolves as a duplicate and any outcome-dependent suffix would change the
  // audit record under the same index, tripping the store's fail-stop.
  std::string summary =
      absl::StrCat("SubmitOperation kind=", cmd.kind_,
                   " id=", HexBytes(cmd.operation_id_), " seq=", log_index);
  const bool creation = cmd.kind_ == kMetaClusterCreateOperationKind;
  std::optional<std::string> failover_group_id;
  const auto& lifecycle = stores.topology_.ClusterLifecycle();
  if (cmd.kind_ == kFailoverOperationKind &&
      lifecycle.state_ != MetaClusterLifecycle::kCreated) {
    return Rejected("failover submission requires cluster lifecycle Created",
                    std::move(summary));
  }
  if (cmd.kind_ == kFailoverOperationKind) {
    const auto intent = DecodeFailoverOperationIntent(cmd.intent_);
    if (!intent.ok() || cmd.intent_hash_ != MetaSha256(cmd.intent_) ||
        std::any_of(cmd.replication_history_id_.begin(),
                    cmd.replication_history_id_.end(),
                    [](std::uint8_t byte) { return byte != 0; })) {
      return Rejected(
          "failover submission requires canonical request-only intent",
          std::move(summary));
    }
    failover_group_id = intent->group_id_;
  }
  const bool reuses_genesis_id =
      lifecycle.state_ != MetaClusterLifecycle::kUninitialized &&
      lifecycle.root_operation_id_ == cmd.operation_id_;
  if (reuses_genesis_id && !creation) {
    return Rejected("operation id is permanently reserved by ClusterCreate",
                    std::move(summary));
  }
  if (creation) {
    MetaOperationId intent_root{};
    if (const auto manifest =
            DecodeClusterCreateRequest(cmd.intent_, &intent_root);
        !manifest.ok()) {
      return Rejected(absl::StrCat("invalid canonical cluster-create intent: ",
                                   manifest.status().message()),
                      std::move(summary));
    }
    if (intent_root != cmd.operation_id_) {
      return Rejected("cluster-create intent root id mismatch",
                      std::move(summary));
    }
    if (cmd.intent_hash_ != MetaSha256(cmd.intent_)) {
      return Rejected("cluster-create intent hash mismatch",
                      std::move(summary));
    }
  }

  // A permanent-id duplicate cannot mutate the existing record. Creation
  // replay is an aggregate check: both the journal record and the lifecycle
  // binding must describe the Genesis entry. A one-sided match is rejected.
  if (stores.operation_.OperationKnown(cmd.operation_id_)) {
    if (creation && !ExistingClusterCreateEffectMatches(
                        stores, cmd.operation_id_, log_index)) {
      return Rejected(
          "cluster-create replay does not match the complete Genesis effect",
          std::move(summary));
    }
    SubmitOperation injected = cmd;
    injected.actor_ = actor;
    MetaApplyRollback rollback(stores, cmd);
    const auto result = stores.operation_.SubmitOperation(injected, log_index);
    if (!result.ok()) return Rejected(result.status(), std::move(summary));
    if (creation) {
      if (const absl::Status status =
              stores.topology_.BeginClusterCreate(cmd.operation_id_, log_index);
          !status.ok()) {
        return Rejected(status, std::move(summary));
      }
    }
    rollback.Commit();
    return Accepted(std::move(summary));
  }
  // Begin and Submit may be adjacent in either Raft order. Begin preempts all
  // earlier pristine requests; once its transition is active, this gate keeps
  // every later new request out. Permanent-id replay above remains legal so a
  // client can still discover the abort of a request it already submitted.
  if (failover_group_id.has_value()) {
    const auto group = stores.topology_.FindGroup(*failover_group_id);
    if (group.has_value() && group->failover_transition_.has_value()) {
      return Rejected(
          "controlled submission conflicts with active failover "
          "transition",
          std::move(summary));
    }
  }
  // Creation intent is the first committed mutation and its durable
  // reservation survives a lost proposer/leader. The entry-layer gate is
  // only fast rejection; two different creation ids must not both commit.
  // Existing-id replay above remains legal after topology has been built.
  const bool creation_active = stores.topology_.ClusterLifecycle().state_ ==
                               MetaClusterLifecycle::kCreating;
  if ((creation || cmd.kind_ == kMetaMembershipOperationKind) &&
      (creation_active ||
       stores.operation_.HasActiveKind(kMetaMembershipOperationKind))) {
    return Rejected(
        "another durable Meta membership/creation workflow is active",
        std::move(summary));
  }
  if (creation) {
    if (stores.topology_.ClusterLifecycle().state_ !=
        MetaClusterLifecycle::kUninitialized) {
      return Rejected("cluster has already accepted creation",
                      std::move(summary));
    }
    if (HasDataClusterArtifactsImpl(stores)) {
      return Rejected("cluster creation requires pristine unreserved state",
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
  MetaApplyRollback rollback(stores, cmd);
  const auto result = stores.operation_.SubmitOperation(injected, log_index);
  if (!result.ok()) return Rejected(result.status(), std::move(summary));
  if (creation) {
    if (const absl::Status status =
            stores.topology_.BeginClusterCreate(cmd.operation_id_, log_index);
        !status.ok()) {
      return Rejected(status, std::move(summary));
    }
  }
  rollback.Commit();
  return Accepted(std::move(summary));
}

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const TransitionOperationPhase& cmd) {
  std::string summary =
      absl::StrCat("TransitionOperationPhase id=", HexBytes(cmd.operation_id_),
                   " expected_revision=", cmd.expected_revision_);
  if (stores.operation_.TransitionAlreadyApplied(cmd)) {
    return FromStatus(
        stores.operation_.TransitionOperationPhase(cmd, log_index),
        std::move(summary));
  }
  const auto operation = stores.operation_.FindOperation(cmd.operation_id_);
  if (operation.has_value() && operation->kind_ == kFailoverOperationKind) {
    return Rejected("failover operations do not use generic phase transitions",
                    std::move(summary));
  }
  if (operation.has_value()) {
    for (const MetaDirectiveSpec& directive : cmd.current_directives_) {
      if (directive.kind_ == kMetaDirectiveInitializeEmptyPopulation &&
          directive.payload_ != HexBytes(operation->replication_history_id_)) {
        return Rejected(
            "empty-population target history is not operation-bound",
            std::move(summary));
      }
      if (const absl::Status anchor =
              ValidateCommittedDirectiveAnchorImpl(stores, directive);
          !anchor.ok()) {
        return Rejected(anchor, std::move(summary));
      }
    }
  }

  // A command can satisfy per-operation caps while exceeding a recipient's
  // aggregate projection limits. Retain only this operation's previous record
  // until the count/size validation succeeds under the apply write lock.
  std::set<std::string> affected_recipients;
  if (operation.has_value()) {
    for (const MetaCurrentDirective& current : operation->current_directives_) {
      affected_recipients.insert(current.spec_.recipient_node_id_);
    }
  }
  for (const MetaDirectiveSpec& directive : cmd.current_directives_) {
    affected_recipients.insert(directive.recipient_node_id_);
  }

  MetaApplyRollback rollback(stores, cmd);
  if (const absl::Status status =
          stores.operation_.TransitionOperationPhase(cmd, log_index);
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }
  if (const absl::Status status =
          ValidateAffectedNodeControls(stores, log_index, affected_recipients);
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }
  rollback.Commit();
  return Accepted(std::move(summary));
}

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const CompleteOperation& cmd) {
  (void)log_index;
  std::string summary =
      absl::StrCat("CompleteOperation id=", HexBytes(cmd.operation_id_),
                   " expected_revision=", cmd.expected_revision_,
                   " data_loss_possible=", cmd.data_loss_possible_ ? 1 : 0);
  const auto operation = stores.operation_.FindOperation(cmd.operation_id_);
  if (operation.has_value() && operation->kind_ == kFailoverOperationKind) {
    return Rejected("failover operations require typed completion",
                    std::move(summary));
  }
  const auto& lifecycle = stores.topology_.ClusterLifecycle();
  const bool creation_root =
      (operation.has_value() &&
       operation->kind_ == kMetaClusterCreateOperationKind) ||
      (lifecycle.state_ != MetaClusterLifecycle::kUninitialized &&
       lifecycle.root_operation_id_ == cmd.operation_id_);
  if (!creation_root) {
    return FromStatus(stores.operation_.CompleteOperation(cmd),
                      std::move(summary));
  }
  if (cmd.result_ != "cluster-created") {
    return Rejected("cluster-create root requires result cluster-created",
                    std::move(summary));
  }
  if (!operation.has_value() ||
      !LiveClusterCreateRootMatchesLifecycle(*operation, lifecycle)) {
    return Rejected("cluster-create completion root anchor mismatch",
                    std::move(summary));
  }
  // Creation installs all compiled-in global Policy families before any
  // authority. Keep that ordering as an apply invariant as well as a planner
  // convention: otherwise a malformed or stale trusted command can commit a
  // Created state that only fails closed after a snapshot restore, while WAL
  // replay alone would continue serving the invalid aggregate.
  if (!stores.policy_.CurrentAutomaticUncontrolledFailover().has_value() ||
      !stores.policy_.CurrentAuthorityLease().has_value() ||
      !stores.policy_.CurrentCandidateRecovery().has_value()) {
    return Rejected(
        "cluster-create completion requires all current global Policies",
        std::move(summary));
  }
  const bool operation_effect_applied =
      operation->lifecycle_ == MetaOperationLifecycle::kCompleted &&
      cmd.expected_revision_ != std::numeric_limits<std::uint64_t>::max() &&
      operation->revision_ == cmd.expected_revision_ + 1 &&
      operation->terminal_result_ == cmd.result_ &&
      operation->data_loss_possible_ == cmd.data_loss_possible_;
  const bool lifecycle_effect_applied =
      lifecycle.state_ == MetaClusterLifecycle::kCreated &&
      lifecycle.root_operation_id_ == cmd.operation_id_ &&
      lifecycle.failure_summary_.empty();
  if (operation_effect_applied != lifecycle_effect_applied) {
    return Rejected("cluster-create completion replay halves do not agree",
                    std::move(summary));
  }
  MetaApplyRollback rollback(stores, cmd);
  if (const absl::Status status = stores.operation_.CompleteOperation(cmd);
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }
  if (const absl::Status status =
          stores.topology_.CompleteClusterCreate(cmd.operation_id_);
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }
  rollback.Commit();
  return Accepted(std::move(summary));
}

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const AbortOperation& cmd) {
  (void)log_index;
  std::string summary =
      absl::StrCat("AbortOperation id=", HexBytes(cmd.operation_id_),
                   " expected_revision=", cmd.expected_revision_);
  const auto operation = stores.operation_.FindOperation(cmd.operation_id_);
  if (operation.has_value() && operation->kind_ == kFailoverOperationKind) {
    return Rejected("failover operations require typed abort",
                    std::move(summary));
  }
  const auto& lifecycle = stores.topology_.ClusterLifecycle();
  const bool creation_root =
      (operation.has_value() &&
       operation->kind_ == kMetaClusterCreateOperationKind) ||
      (lifecycle.state_ != MetaClusterLifecycle::kUninitialized &&
       lifecycle.root_operation_id_ == cmd.operation_id_);
  if (!creation_root) {
    return FromStatus(stores.operation_.AbortOperation(cmd),
                      std::move(summary));
  }
  if (!operation.has_value() ||
      !LiveClusterCreateRootMatchesLifecycle(*operation, lifecycle)) {
    return Rejected("cluster-create abort root anchor mismatch",
                    std::move(summary));
  }
  const std::string failure_summary = ClusterFailureSummary(cmd.operation_id_);
  const bool operation_effect_applied =
      operation->lifecycle_ == MetaOperationLifecycle::kAborted &&
      cmd.expected_revision_ != std::numeric_limits<std::uint64_t>::max() &&
      operation->revision_ == cmd.expected_revision_ + 1 &&
      operation->terminal_result_ == cmd.reason_;
  const bool lifecycle_effect_applied =
      lifecycle.state_ == MetaClusterLifecycle::kProvisioningFailed &&
      lifecycle.root_operation_id_ == cmd.operation_id_ &&
      lifecycle.failure_summary_ == failure_summary;
  if (operation_effect_applied != lifecycle_effect_applied) {
    return Rejected("cluster-create abort replay halves do not agree",
                    std::move(summary));
  }
  MetaApplyRollback rollback(stores, cmd);
  if (const absl::Status status = stores.operation_.AbortOperation(cmd);
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }
  if (const absl::Status status = stores.topology_.FailClusterCreate(
          cmd.operation_id_, failure_summary);
      !status.ok()) {
    return Rejected(status, std::move(summary));
  }
  rollback.Commit();
  return Accepted(std::move(summary));
}

ApplyOutcome Dispatch(MetaStores& stores, std::uint64_t log_index,
                      const CommitDirectiveResult& cmd) {
  std::string summary = absl::StrCat(
      "CommitDirectiveResult operation=", HexBytes(cmd.operation_id_),
      " attempt=", HexBytes(cmd.attempt_id_),
      " directive_revision=", cmd.directive_revision_);
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

bool HasDataClusterArtifacts(const MetaStores& stores) {
  return HasDataClusterArtifactsImpl(stores);
}

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
  result.command_tag_ = MetaCommandTagOf(command);

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
          static_cast<std::uint16_t>(MetaCommandTag::kStartCandidateRecovery)) {
    return MetaFailStopError("unknown apply-result command tag");
  }
  auto detail = r.ReadString(kMaxMetaAuditDetailBytes);
  if (!detail.ok()) return detail.status();
  if (absl::Status status = r.Finish(); !status.ok()) return status;
  return MetaApplyResult{static_cast<MetaAuditVerdict>(*verdict),
                         std::string(*detail), *log_index,
                         static_cast<MetaCommandTag>(*command_tag)};
}

}  // namespace lavik::meta
