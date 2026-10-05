/* Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0 */
#pragma once

#include <algorithm>
#include <map>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

#include "lavik/meta/committed_cursor.h"
#include "lavik/meta/identity_store.h"
#include "lavik/meta/operation_store.h"
#include "lavik/meta/topology_store.h"

namespace lavik::meta {

// Owned workflow inputs, passed to planners by const reference. These records
// never borrow live store memory and survive apply, Install and machine
// teardown. Whole selected operations are intentional: directives and exact
// receipts are recovery inputs, and copying one record keeps their existing
// semantics intact.
struct MetaClusterCreateDiscovery {
  MetaCommittedCursor cursor_;
  MetaClusterLifecycleState lifecycle_;
  std::optional<MetaOperationRecord> root_;
};

struct MetaClusterCreateChild {
  // A missing live record is not necessarily a never-started child. Preserve
  // the archive distinction without copying its historical receipt payloads.
  bool known_ = false;
  std::optional<MetaOperationRecord> operation_;
};

struct MetaClusterCreateView {
  MetaCommittedCursor cursor_;
  MetaOperationRecord root_;
  // Preserve store key order for lookup and deterministic planner selection.
  std::vector<MetaMemberRecord> meta_members_;
  std::vector<MetaNodeRecord> nodes_;
  std::vector<MetaTopologyGroupView> groups_;
  // Same ordered Group domain as groups_. Includes fenced Groups.
  std::vector<MetaGroupAuthorityView> authorities_;
  std::uint64_t topology_epoch_ = 0;
  // Absolute ownership: gaps are unassigned, never inherited from an old cut.
  std::vector<MetaSlotAssignment> slots_;
  bool automatic_failover_policy_ = false;
  bool authority_lease_policy_ = false;
  bool candidate_recovery_policy_ = false;
  std::vector<MetaHash256> present_manifests_;
  std::map<MetaOperationId, MetaClusterCreateChild> children_;

  std::uint64_t applied_index() const { return cursor_.applied_index(); }
  const MetaNodeRecord* FindNode(std::string_view id) const {
    const auto it = std::lower_bound(
        nodes_.begin(), nodes_.end(), id,
        [](const auto& node, auto key) { return node.node_id_ < key; });
    return it != nodes_.end() && it->node_id_ == id ? &*it : nullptr;
  }
  const MetaTopologyGroupView* FindGroup(std::string_view id) const {
    const auto it = std::lower_bound(
        groups_.begin(), groups_.end(), id,
        [](const auto& group, auto key) { return group.group_id_ < key; });
    return it != groups_.end() && it->group_id_ == id ? &*it : nullptr;
  }
  const MetaGroupAuthorityView* AuthorityFor(std::string_view id) const {
    const auto* group = FindGroup(id);
    return group ? &authorities_[group - groups_.data()] : nullptr;
  }
  std::optional<std::string_view> SlotOwner(std::uint32_t slot) const {
    const auto it = std::lower_bound(
        slots_.begin(), slots_.end(), slot,
        [](const auto& range, auto key) { return range.last_slot_ < key; });
    if (it == slots_.end() || slot < it->first_slot_) return std::nullopt;
    return it->group_id_;
  }
  bool ContainsManifest(const MetaHash256& digest) const {
    return std::find(present_manifests_.begin(), present_manifests_.end(),
                     digest) != present_manifests_.end();
  }
};

// Without an active workflow, the initial-binding planner must see the entire
// Meta directory to reject bindings outside the loaded Raft configuration.
// Data identities and operation-journal payloads are never part of discovery.
struct MetaMembershipDiscovery {
  MetaCommittedCursor cursor_;
  std::optional<MetaOperationRecord> operation_;
  std::vector<MetaMemberRecord> initial_bindings_;
};

struct MetaMembershipView {
  MetaCommittedCursor cursor_;
  MetaOperationRecord operation_;
  std::vector<MetaMemberRecord> bindings_;
};

// Both initial and ordinary membership inputs own complete binding records in
// server-id order; callers must preserve that order when using this lookup.
// Returning a value permits the removal planner's retired-flag normalization
// without mutating its captured input.
inline std::optional<MetaMemberRecord> FindMetaBinding(
    std::span<const MetaMemberRecord> bindings, std::uint32_t id) {
  const auto it = std::lower_bound(
      bindings.begin(), bindings.end(), id,
      [](const auto& binding, auto key) { return binding.server_id_ < key; });
  return it != bindings.end() && it->server_id_ == id ? std::optional(*it)
                                                      : std::nullopt;
}

}  // namespace lavik::meta
