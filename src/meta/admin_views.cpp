/* Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0 */

#include "lavik/meta/admin_views.h"

#include <algorithm>

#include "lavik/meta/failover.h"
#include "lavik/meta/state_machine.h"

namespace lavik::meta {
namespace {
bool IsTerminal(MetaOperationLifecycle lifecycle) {
  return lifecycle == MetaOperationLifecycle::kCompleted ||
         lifecycle == MetaOperationLifecycle::kAborted;
}
}  // namespace

std::optional<std::string_view> MetaSlotMapCheckView::SlotOwner(
    std::uint32_t slot) const {
  const auto it = std::lower_bound(
      slots_.begin(), slots_.end(), slot,
      [](const auto& range, auto index) { return range.last_slot_ < index; });
  if (it == slots_.end() || slot < it->first_slot_) return std::nullopt;
  return it->group_id_;
}

MetaAdminGroupView MetaStateMachine::CaptureAdminGroup(
    const std::string& group_id) const {
  std::lock_guard lock(mutex_);
  return {{last_committed_idx_.load(), last_state_change_idx_.load()},
          stores_.topology_.ClusterLifecycle(),
          stores_.topology_.TopologyEpoch(),
          stores_.topology_.FindGroup(group_id),
          stores_.topology_.AuthorityFor(group_id)};
}

MetaOperationStatusView MetaStateMachine::CaptureOperationStatus(
    const MetaOperationId& id) const {
  std::lock_guard lock(mutex_);
  MetaOperationStatusView result;
  result.cursor_ = {last_committed_idx_.load(), last_state_change_idx_.load()};
  result.operation_ = stores_.operation_.FindOperation(id);
  if (result.operation_ && result.operation_->kind_ == kFailoverOperationKind &&
      !IsTerminal(result.operation_->lifecycle_)) {
    result.controlled_running_ = stores_.topology_.HasControlledOperation(id);
  }
  return result;
}

MetaCreatePreflightView MetaStateMachine::CaptureCreatePreflight() const {
  std::lock_guard lock(mutex_);
  return {{last_committed_idx_.load(), last_state_change_idx_.load()},
          stores_.topology_.ClusterLifecycle(),
          stores_.identity_.MetaMembers(),
          stores_.operation_.HasActiveKind(kMetaMembershipOperationKind),
          HasDataClusterArtifacts(stores_)};
}

MetaMembershipAdminView MetaStateMachine::CaptureMembershipAdmin() const {
  std::lock_guard lock(mutex_);
  MetaMembershipAdminView result;
  result.cursor_ = {last_committed_idx_.load(), last_state_change_idx_.load()};
  result.lifecycle_ = stores_.topology_.ClusterLifecycle();
  // Preserve LiveOperations' key order while copying only the selected record.
  for (const auto& op : stores_.operation_.LiveOperationsView()) {
    if (op.kind_ == kMetaMembershipOperationKind &&
        !IsTerminal(op.lifecycle_)) {
      result.operation_ = op;
      break;
    }
  }
  if (!result.operation_) result.identity_ = stores_.identity_;
  return result;
}

MetaPromoteView MetaStateMachine::CapturePromote(
    const std::string& group_id) const {
  std::vector<std::string> nodes;
  std::vector<MetaObservationGroupFacts> groups;
  MetaCommittedCursor cursor;
  MetaClusterLifecycleState lifecycle;
  std::optional<MetaTopologyGroupView> group;
  std::optional<MetaGroupAuthorityView> authority;
  {
    std::lock_guard lock(mutex_);
    cursor = {last_committed_idx_.load(), last_state_change_idx_.load()};
    lifecycle = stores_.topology_.ClusterLifecycle();
    group = stores_.topology_.FindGroup(group_id);
    authority = stores_.topology_.AuthorityFor(group_id);
    nodes = stores_.identity_.ActiveNodeIds();
    groups = stores_.topology_.ObservationFacts();
  }
  // Global facts are needed for absence proofs and cross-Group freshness.
  // Their lookup indices are built only after releasing the state lock.
  MetaPromoteView result(
      MetaObservationFactsView(std::move(nodes), std::move(groups), cursor));
  result.lifecycle_ = std::move(lifecycle);
  result.group_ = std::move(group);
  result.authority_ = std::move(authority);
  return result;
}

MetaSlotMapCheckView MetaStateMachine::CaptureSlotMapCheck(
    const std::string& group_id) const {
  std::lock_guard lock(mutex_);
  return {{last_committed_idx_.load(), last_state_change_idx_.load()},
          stores_.topology_.TopologyEpoch(),
          stores_.topology_.GroupExists(group_id),
          stores_.topology_.SlotRanges()};
}

MetaCurrentPolicyView MetaStateMachine::CaptureCurrentPolicy(
    const std::string& policy_id) const {
  std::lock_guard lock(mutex_);
  MetaCurrentPolicyView result;
  result.version_ = stores_.policy_.LatestVersion(policy_id);
  if (result.version_)
    result.current_ = stores_.policy_.FindVersion(policy_id, *result.version_);
  return result;
}

std::uint64_t MetaStateMachine::TopologyEpoch() const {
  std::lock_guard lock(mutex_);
  return stores_.topology_.TopologyEpoch();
}

bool MetaStateMachine::GroupExists(const std::string& group_id) const {
  std::lock_guard lock(mutex_);
  return stores_.topology_.GroupExists(group_id);
}

std::optional<std::uint64_t> MetaStateMachine::CurrentGroupTerm(
    const std::string& group_id) const {
  std::lock_guard lock(mutex_);
  return stores_.topology_.CurrentGroupTerm(group_id);
}

bool MetaStateMachine::ArchivedOperationsExist(
    std::span<const std::uint64_t> seqs) const {
  std::lock_guard lock(mutex_);
  return std::all_of(seqs.begin(), seqs.end(), [&](auto seq) {
    return stores_.operation_.ArchivedSequenceExists(seq);
  });
}

MetaAuditStatus MetaStateMachine::AuditStatus() const {
  std::lock_guard lock(mutex_);
  const auto& audit = stores_.audit_;
  return {audit.policy(), audit.size(), audit.capacity(), audit.dropped_total(),
          audit.dropped_through()};
}

MetaAuditStore MetaStateMachine::CaptureAuditExport() const {
  std::lock_guard lock(mutex_);
  return stores_.audit_;
}

MetaOperationArchiveExport MetaStateMachine::CaptureOperationArchiveExport()
    const {
  std::lock_guard lock(mutex_);
  return stores_.operation_.CaptureArchiveExport();
}

}  // namespace lavik::meta
