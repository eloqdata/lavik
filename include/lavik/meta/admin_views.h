/* Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0 */
#pragma once

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "lavik/meta/audit_store.h"
#include "lavik/meta/committed_cursor.h"
#include "lavik/meta/identity_store.h"
#include "lavik/meta/observation_facts_view.h"
#include "lavik/meta/operation_store.h"
#include "lavik/meta/policy_store.h"
#include "lavik/meta/topology_store.h"

namespace lavik::meta {

// Owned Admin inputs captured from one committed cut; readers that need a
// cursor retain it with their records. No pointer refers to live state.
// Consumers validate and encode after capture. Proposal replies use their
// exact apply result; a later view describes current state only.
struct MetaAdminGroupView {
  MetaCommittedCursor cursor_;
  MetaClusterLifecycleState lifecycle_;
  std::uint64_t topology_epoch_ = 0;
  std::optional<MetaTopologyGroupView> group_;
  std::optional<MetaGroupAuthorityView> authority_;
};

struct MetaOperationStatusView {
  MetaCommittedCursor cursor_;
  std::optional<MetaOperationRecord> operation_;
  // Derived from the controlled transition in the same cut, not from the
  // operation's phase or a separately captured topology.
  bool controlled_running_ = false;
};

struct MetaCreatePreflightView {
  MetaCommittedCursor cursor_;
  MetaClusterLifecycleState lifecycle_;
  std::vector<MetaMemberRecord> meta_members_;
  bool active_membership_ = false;
  bool data_artifacts_ = false;
};

struct MetaMembershipAdminView {
  MetaCommittedCursor cursor_;
  MetaClusterLifecycleState lifecycle_;
  std::optional<MetaOperationRecord> operation_;
  // Needed only when admitting a new workflow. The full identity domain
  // preserves principal tombstones and every endpoint conflict predicate.
  // An attached retry carries only its selected operation instead.
  MetaIdentityStore identity_;
};

struct MetaPromoteView {
  explicit MetaPromoteView(MetaObservationFactsView facts)
      : facts_(std::move(facts)) {}
  MetaObservationFactsView facts_;
  MetaClusterLifecycleState lifecycle_;
  std::optional<MetaTopologyGroupView> group_;
  std::optional<MetaGroupAuthorityView> authority_;
};

struct MetaCurrentPolicyView {
  // Preserve the distinction between a missing family and a corrupt latest
  // reference, rather than treating either one as a missing document.
  std::optional<std::uint64_t> version_;
  std::optional<MetaPolicyVersionView> current_;
};

struct MetaAuditStatus {
  MetaAuditPolicy policy_;
  std::size_t size_;
  std::uint32_t capacity_;
  std::uint64_t dropped_total_;
  std::uint64_t dropped_through_;
};

}  // namespace lavik::meta
