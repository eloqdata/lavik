/* Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0 */
#pragma once

#include <optional>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include "lavik/meta/committed_cursor.h"
#include "lavik/meta/identity_store.h"
#include "lavik/meta/observation_store.h"
#include "lavik/meta/operation_store.h"
#include "lavik/meta/policy_store.h"
#include "lavik/meta/population_manifest_store.h"
#include "lavik/meta/topology_store.h"

namespace lavik::meta {
struct MetaStores;

// One owned publication cut shared by every Data session. The global routing
// and observation domain is complete; only operations with current directives
// (and the Creating root) and referenced manifests are retained. Queries borrow
// this immutable view, never the state machine. Copies survive apply/Install.
class MetaDataPublicationView final : public MetaCommittedFacts {
 public:
  // For callers already owning a complete cut, including projection fixtures.
  // Production publication captures directly under the state-machine lock.
  static MetaDataPublicationView FromStores(const MetaStores& stores,
                                            MetaCommittedCursor cursor);
  std::uint64_t applied_index() const { return data_.cursor_.applied_index(); }
  std::uint64_t state_change_index() const {
    return data_.cursor_.state_change_index();
  }
  const MetaClusterLifecycleState& lifecycle() const {
    return data_.lifecycle_;
  }
  std::uint64_t topology_epoch() const { return data_.topology_epoch_; }
  std::span<const MetaNodeRecord> nodes() const { return data_.nodes_; }
  std::span<const MetaMemberRecord> meta_members() const {
    return data_.meta_members_;
  }
  std::span<const MetaTopologyGroupView> groups() const {
    return data_.groups_;
  }
  std::span<const MetaSlotAssignment> slots() const { return data_.slots_; }
  std::span<const MetaOperationRecord> operations() const {
    return data_.operations_;
  }
  const std::optional<MetaAuthorityLeasePolicy>& authority_lease() const {
    return authority_lease_;
  }
  const MetaNodeRecord* FindNode(std::string_view id) const;
  const MetaMemberRecord* FindMetaMember(std::uint32_t id) const;
  const MetaTopologyGroupView* FindGroup(std::string_view id) const;
  const MetaGroupAuthorityView* AuthorityFor(std::string_view id) const;
  // Selection lookup, not a live/archive existence query. Publication asks
  // only for its lifecycle's Creating root and its retained current tasks.
  const MetaOperationRecord* FindOperation(const MetaOperationId& id) const;
  // Resolves only captured Group/directive dependencies. A missing referenced
  // document remains an inconsistent committed state at projection time.
  const MetaPopulationManifestDocument* FindManifest(
      const MetaHash256& digest) const;
  absl::Status ValidateDirectiveAnchor(
      const MetaDirectiveSpec& directive) const;

  bool IsActiveNode(std::string_view node_id) const override;
  std::uint64_t CurrentGroupTerm(std::string_view group_id) const override;
  std::uint64_t CurrentPopulationManifestRevision(
      std::string_view group_id) const override;
  MetaHash256 CurrentPopulationManifestDigest(
      std::string_view group_id) const override;
  std::uint64_t CurrentPartitionReplicationEpoch(
      std::string_view group_id) const override;
  bool AssignmentMatches(std::string_view group_id, std::string_view node_id,
                         const MetaAssignmentId& assignment_id) const override;
  bool IsOwnerAssignment(std::string_view group_id, std::string_view node_id,
                         const MetaAssignmentId& assignment_id) const override;
  bool MayReportFencedOwnerCandidate(
      const MetaCandidateProgressObs& candidate) const override;
  bool IsCurrentFailoverCandidate(
      std::string_view node_id,
      const MetaBootIncarnation& boot_id) const override;
  std::optional<FailoverTransitionView> FailoverTransitionById(
      const MetaFailoverTransitionId& transition_id) const override;

 private:
  friend class MetaStateMachine;
  struct Data {
    MetaCommittedCursor cursor_;
    MetaClusterLifecycleState lifecycle_;
    std::uint64_t topology_epoch_ = 0;
    // Retired identities remain distinguishable from unregistered nodes at
    // bootstrap. Endpoint publication still selects only active identities.
    std::vector<MetaNodeRecord> nodes_;
    std::vector<MetaMemberRecord> meta_members_;
    std::vector<MetaTopologyGroupView> groups_;
    // Same sorted Group domain as groups_, including fenced authorities.
    std::vector<MetaGroupAuthorityView> authorities_;
    std::vector<MetaSlotAssignment> slots_;
    std::vector<MetaOperationRecord> operations_;
    // Digest-ordered captured dependencies; no second index needs building
    // while the state lock is held.
    std::vector<std::pair<MetaHash256, MetaPopulationManifestDocument>>
        manifests_;
  };
  struct CaptureData {
    Data selected_;
    std::optional<MetaPolicyVersionView> lease_policy_;
  };
  static CaptureData Extract(const MetaStores& stores,
                             MetaCommittedCursor cursor);
  // Decode the one current Policy after releasing the state lock.
  explicit MetaDataPublicationView(CaptureData data);
  Data data_;
  std::optional<MetaAuthorityLeasePolicy> authority_lease_;
};

// Exact receipt and, only when absent, its live operation from one cut.
// Archived receipts retain their first committed index. The copied operation
// preserves existing lifecycle/recipient checks without a separate summary.
struct MetaDirectiveResultView {
  MetaCommittedCursor cursor_;
  std::optional<MetaTerminalReceipt> receipt_;
  std::optional<MetaOperationRecord> operation_;
};
}  // namespace lavik::meta
