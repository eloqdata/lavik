/* Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0 */
#pragma once

#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "lavik/meta/committed_cursor.h"
#include "lavik/meta/observation_facts_view.h"
#include "lavik/meta/operation_store.h"
#include "lavik/meta/policy_store.h"

namespace lavik::meta {

// Every view owns its records and indices from one state-machine cut. Whole
// selected operations deliberately preserve the existing planner predicates;
// no operation payload is retained by ordinary automatic detection.
struct MetaFailoverDiscovery {
  MetaCommittedCursor cursor_;
  std::vector<MetaOperationRecord> operations_;  // operation_seq order
  std::vector<MetaFailoverWork> work_;           // Group-id order
};

struct MetaFailoverPlanningView {
  explicit MetaFailoverPlanningView(MetaObservationFactsView facts)
      : facts_(std::move(facts)) {}
  MetaObservationFactsView facts_;
  std::optional<MetaTopologyGroupView> group_;
  std::optional<MetaGroupAuthorityView> authority_;
  std::optional<MetaOperationRecord> operation_;
  std::uint64_t topology_epoch_ = 0;
  std::optional<MetaCandidateRecoveryPolicy> recovery_policy_;
};

struct MetaAutomaticDetectionView {
  MetaCommittedCursor cursor_;
  MetaClusterLifecycle lifecycle_ = MetaClusterLifecycle::kUninitialized;
  std::uint64_t topology_epoch_ = 0;
  std::vector<MetaAutomaticGroupFacts> groups_;  // Group-id order
  std::optional<MetaAutomaticUncontrolledFailoverPolicy> automatic_;
  std::optional<MetaAuthorityLeasePolicy> lease_;

  // Returns a borrowed pointer into this owned, Group-id-ordered batch.
  const MetaAutomaticGroupFacts* FindGroup(std::string_view id) const;
};

// Captured only on a detector trigger, conditional on its state-change cut.
// Sorting, intent decoding, and hash checks run after releasing the state lock.
struct MetaAutomaticTriggerView {
  MetaCommittedCursor cursor_;
  std::vector<MetaOperationRecord> operations_;  // operation_seq order
  // Automatic preemption requires the complete pristine request contract,
  // including receipts, zero history and intent hash. Ordinary Submitted
  // planning deliberately keeps its own, distinct admission predicate.
  const MetaOperationRecord* PreemptableControlledRequest(
      std::string_view group_id) const;
};

}  // namespace lavik::meta
