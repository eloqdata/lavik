/* Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0 */
#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "lavik/meta/audit_store.h"
#include "lavik/meta/committed_cursor.h"
#include "lavik/meta/observation_facts_view.h"
#include "lavik/meta/operation_store.h"
#include "lavik/meta/policy_store.h"

namespace lavik::meta {

struct MetaStores;

// Only current values: Policy history and raw JSON are not admission data.
struct MetaProposalAutomaticPolicies {
  std::uint64_t topology_epoch_ = 0;
  std::optional<MetaAutomaticUncontrolledFailoverPolicy> automatic_;
  std::optional<MetaAuthorityLeasePolicy> lease_;
};

// Owned admission state from one committed cut. Retaining this view never
// borrows mutable state-machine storage or blocks a later commit/Install.
class MetaProposalView {
 public:
  // Projects an already owned full cut, notably the fail-safe recovery cut.
  // The caller guarantees stores and cursor describe the same immutable cut.
  static MetaProposalView FromStores(const MetaCommand& command,
                                     const MetaStores& stores,
                                     MetaCommittedCursor cursor);

  std::uint64_t applied_index() const { return data_.cursor_.applied_index(); }
  std::uint64_t state_change_index() const {
    return data_.cursor_.state_change_index();
  }
  const MetaAuditStore& audit() const { return data_.audit_; }
  // A query outside the captured dependency set is a programming error and
  // fails stop; it must never look like an absent committed object.
  const std::optional<MetaOperationHeader>& operation_header(
      const MetaOperationId& id) const;
  const std::optional<MetaTopologyGroupView>& group(
      std::string_view group_id) const;
  const std::optional<MetaGroupAuthorityView>& authority(
      std::string_view group_id) const;
  const MetaCommittedFacts& facts() const;
  // Available only for non-manual BeginUncontrolledFailover, including an
  // uncertain retry. Its hook still bypasses Policy/health re-evaluation.
  const MetaProposalAutomaticPolicies& automatic_policies() const;

 private:
  friend class MetaStateMachine;
  struct CaptureData {
    MetaAuditStore audit_;
    MetaCommittedCursor cursor_;
    std::optional<MetaOperationId> operation_id_;
    std::optional<MetaOperationHeader> operation_;
    std::optional<std::string> group_id_;
    std::optional<MetaTopologyGroupView> group_;
    std::optional<MetaGroupAuthorityView> authority_;
    std::vector<std::string> active_nodes_;
    std::vector<MetaObservationGroupFacts> groups_;
    std::optional<MetaProposalAutomaticPolicies> automatic_;
  };
  static CaptureData Extract(const MetaCommand& command,
                             const MetaStores& stores,
                             MetaCommittedCursor cursor);
  // Finishes lookup indices after Extract's state-lock scope has ended.
  explicit MetaProposalView(CaptureData data);

  CaptureData data_;
  std::optional<MetaObservationFactsView> facts_;
};

}  // namespace lavik::meta
