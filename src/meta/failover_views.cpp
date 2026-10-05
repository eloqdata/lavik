/* Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0 */

#include "lavik/meta/failover_views.h"

#include <algorithm>
#include <mutex>
#include <utility>

#include "lavik/meta/failover.h"
#include "lavik/meta/hash.h"
#include "lavik/meta/state_machine.h"

namespace lavik::meta {
namespace {
bool ActiveFailover(const MetaOperationRecord& operation) {
  return operation.kind_ == kFailoverOperationKind &&
         operation.lifecycle_ != MetaOperationLifecycle::kCompleted &&
         operation.lifecycle_ != MetaOperationLifecycle::kAborted;
}

template <typename Policy, typename Decode>
std::optional<Policy> DecodeCurrent(
    const std::optional<MetaPolicyVersionView>& raw, Decode decode) {
  if (!raw) return std::nullopt;
  auto typed = decode(raw->content_);
  if (!typed.ok()) return std::nullopt;
  typed->version_ = raw->version_;
  return *typed;
}
}  // namespace

const MetaAutomaticGroupFacts* MetaAutomaticDetectionView::FindGroup(
    std::string_view id) const {
  const auto it = std::lower_bound(
      groups_.begin(), groups_.end(), id,
      [](const auto& group, auto key) { return group.group_id_ < key; });
  return it != groups_.end() && it->group_id_ == id ? &*it : nullptr;
}

const MetaOperationRecord*
MetaAutomaticTriggerView::PreemptableControlledRequest(
    std::string_view group_id) const {
  for (const MetaOperationRecord& operation : operations_) {
    if (operation.kind_ != kFailoverOperationKind ||
        operation.lifecycle_ != MetaOperationLifecycle::kSubmitted ||
        operation.revision_ != 0 || !operation.kind_phase_blob_.empty() ||
        !operation.current_directives_.empty() ||
        !operation.terminal_receipts_.empty() ||
        !std::ranges::all_of(operation.replication_history_id_,
                             [](std::uint8_t byte) { return byte == 0; }) ||
        operation.intent_hash_ != MetaSha256(operation.intent_)) {
      continue;
    }
    const auto intent = DecodeFailoverOperationIntent(operation.intent_);
    if (intent.ok() && intent->group_id_ == group_id) return &operation;
  }
  return nullptr;
}

MetaFailoverDiscovery MetaStateMachine::CaptureFailoverDiscovery() const {
  MetaFailoverDiscovery view;
  {
    std::lock_guard lock(mutex_);
    view.cursor_ = {last_committed_idx_.load(std::memory_order_relaxed),
                    last_state_change_idx_.load(std::memory_order_relaxed)};
    view.work_ = stores_.topology_.FailoverWork();
    // The journal range never escapes the state lock. Whole selected records
    // preserve the existing pristine/intent checks without an operation schema.
    for (const auto& operation : stores_.operation_.LiveOperationsView()) {
      if (ActiveFailover(operation)) view.operations_.push_back(operation);
    }
  }
  std::ranges::sort(view.operations_, {}, &MetaOperationRecord::operation_seq_);
  return view;
}

std::optional<MetaFailoverPlanningView>
MetaStateMachine::CaptureFailoverPlanningView(
    MetaCommittedCursor expected, const std::string& group_id,
    std::optional<MetaOperationId> submitted_operation) const {
  MetaCommittedCursor cursor;
  std::vector<std::string> active_nodes;
  std::vector<MetaObservationGroupFacts> groups;
  std::optional<MetaTopologyGroupView> group;
  std::optional<MetaGroupAuthorityView> authority;
  std::optional<MetaOperationRecord> operation;
  std::optional<MetaPolicyVersionView> recovery;
  std::uint64_t epoch;
  {
    std::lock_guard lock(mutex_);
    if (last_state_change_idx_.load(std::memory_order_relaxed) !=
        expected.state_change_index())
      return std::nullopt;
    cursor = {last_committed_idx_.load(std::memory_order_relaxed),
              last_state_change_idx_.load(std::memory_order_relaxed)};
    group = stores_.topology_.FindGroup(group_id);
    authority = stores_.topology_.AuthorityFor(group_id);
    if (submitted_operation) {
      operation = stores_.operation_.FindOperation(*submitted_operation);
    } else if (group && group->failover_transition_) {
      const auto& transition = *group->failover_transition_;
      if (transition.controlled_)
        operation = stores_.operation_.FindOperation(
            transition.controlled_->operation_id_);
      if (transition.mode_ == MetaFailoverMode::kUncontrolled)
        recovery = stores_.policy_.CurrentVersion(
            std::string(kCandidateRecoveryPolicyId));
    }
    epoch = stores_.topology_.TopologyEpoch();
    active_nodes = stores_.identity_.ActiveNodeIds();
    groups = stores_.topology_.ObservationFacts();
  }
  // LatestForNode validates reports from every Group. Capture this complete
  // compact domain together with the target; a second capture could splice
  // cuts.
  MetaFailoverPlanningView view(MetaObservationFactsView(
      std::move(active_nodes), std::move(groups), cursor));
  view.group_ = std::move(group);
  view.authority_ = std::move(authority);
  view.operation_ = std::move(operation);
  view.topology_epoch_ = epoch;
  view.recovery_policy_ = DecodeCurrent<MetaCandidateRecoveryPolicy>(
      recovery, DecodeCandidateRecoveryPolicy);
  return view;
}

MetaAutomaticDetectionView MetaStateMachine::CaptureAutomaticDetectionView()
    const {
  MetaAutomaticDetectionView view;
  std::optional<MetaPolicyVersionView> automatic, lease;
  {
    std::lock_guard lock(mutex_);
    view.cursor_ = {last_committed_idx_.load(std::memory_order_relaxed),
                    last_state_change_idx_.load(std::memory_order_relaxed)};
    view.lifecycle_ = stores_.topology_.ClusterLifecycle().state_;
    view.topology_epoch_ = stores_.topology_.TopologyEpoch();
    view.groups_ = stores_.topology_.AutomaticDetectionFacts();
    automatic = stores_.policy_.CurrentVersion(
        std::string(kAutomaticUncontrolledFailoverPolicyId));
    lease =
        stores_.policy_.CurrentVersion(std::string(kAuthorityLeasePolicyId));
  }
  view.automatic_ = DecodeCurrent<MetaAutomaticUncontrolledFailoverPolicy>(
      automatic, DecodeAutomaticUncontrolledFailoverPolicy);
  view.lease_ = DecodeCurrent<MetaAuthorityLeasePolicy>(
      lease, DecodeAuthorityLeasePolicy);
  return view;
}

std::optional<MetaAutomaticTriggerView>
MetaStateMachine::CaptureAutomaticTriggerView(
    MetaCommittedCursor expected) const {
  MetaAutomaticTriggerView view;
  {
    std::lock_guard lock(mutex_);
    if (last_state_change_idx_.load(std::memory_order_relaxed) !=
        expected.state_change_index())
      return std::nullopt;
    view.cursor_ = {last_committed_idx_.load(std::memory_order_relaxed),
                    last_state_change_idx_.load(std::memory_order_relaxed)};
    for (const auto& operation : stores_.operation_.LiveOperationsView()) {
      if (operation.kind_ == kFailoverOperationKind &&
          operation.lifecycle_ == MetaOperationLifecycle::kSubmitted)
        view.operations_.push_back(operation);
    }
  }
  std::ranges::sort(view.operations_, {}, &MetaOperationRecord::operation_seq_);
  return view;
}

}  // namespace lavik::meta
