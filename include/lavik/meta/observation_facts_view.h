/* Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "lavik/meta/committed_cursor.h"
#include "lavik/meta/observation_store.h"
#include "lavik/meta/topology_store.h"

namespace lavik::meta {

// An owned, read-only cut of the committed facts needed by observation
// admission and revalidation. It retains every active node and Group, but no
// endpoints, slots, policy history, operation payloads, manifests, or audit.
// Capture and later queries never borrow state-machine storage; copies and
// moves remain valid after any subsequent apply, Install, or machine teardown.
class MetaObservationFactsView final : public MetaCommittedFacts {
 public:
  std::uint64_t applied_index() const { return cursor_.applied_index(); }
  std::uint64_t state_change_index() const {
    return cursor_.state_change_index();
  }

  // Returns the committed assignment even if its identity is retired. Ingest
  // separately enforces active identity using this same facts cut.
  std::optional<MetaAssignmentId> AssignmentFor(std::string_view group_id,
                                                std::string_view node_id) const;
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
  friend class MetaProposalView;
  // Extraction preserves ordered store traversal. Additional indices are
  // constructed here only after the state-machine lock has been released.
  MetaObservationFactsView(std::vector<std::string> active_nodes,
                           std::vector<MetaObservationGroupFacts> groups,
                           MetaCommittedCursor cursor);
  const MetaObservationGroupFacts* FindGroup(std::string_view group_id) const;
  static bool AssignmentMatches(const MetaObservationGroupFacts& group,
                                std::string_view node_id,
                                const MetaAssignmentId& assignment_id);

  std::vector<std::string> active_nodes_;
  std::vector<MetaObservationGroupFacts> groups_;
  // Group offsets, rather than pointers/string_views, keep both indices valid
  // through ordinary value copies and moves. Keys remain owned by groups_.
  std::vector<std::size_t> transition_index_;
  std::vector<std::size_t> candidate_index_;
  MetaCommittedCursor cursor_;
};

}  // namespace lavik::meta
