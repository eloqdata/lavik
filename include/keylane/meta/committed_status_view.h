#pragma once

// Compact atomic projection used only by cluster status. It deliberately
// excludes audit records, policy bodies, operation payload/evidence, and the
// complete six-store aggregate so an operator read has bounded cost tied to
// current identity/topology rather than retained history.

#include <cstdint>
#include <string>
#include <vector>

#include "keylane/meta/identity_store.h"
#include "keylane/meta/topology_store.h"

namespace keylane::meta {

struct MetaCommittedStatusGroup {
  MetaTopologyGroupView topology_;
  MetaGroupAuthorityView grant_;
  bool manifest_present_ = false;
  bool policy_active_ = false;
};

struct MetaCommittedStatusSlotRange {
  std::uint32_t first_ = 0;
  std::uint32_t last_ = 0;
  std::string group_id_;
};

struct MetaCommittedStatusView {
  std::uint64_t applied_index_ = 0;
  std::uint64_t topology_epoch_ = 0;
  MetaClusterLifecycleState cluster_lifecycle_;
  // Only meaningful with an Uninitialized lifecycle. This is a reportable
  // admission condition, not snapshot corruption.
  bool cluster_non_pristine_ = false;
  bool active_cluster_create_operation_ = false;
  // Derived scalar only: cluster status intentionally excludes Policy
  // identities and raw documents while still reporting the detector's
  // effective threshold during a transient publication gap.
  std::uint64_t automatic_failover_threshold_ms_ = 0;
  // The compact creation projection retains only diagnostic routing facts,
  // never the complete operation intent or destructive workflow evidence.
  std::string active_cluster_create_phase_;
  std::vector<std::string> active_cluster_create_data_nodes_;
  std::vector<MetaMemberRecord> meta_members_;
  std::vector<MetaNodeRecord> data_nodes_;
  std::vector<MetaCommittedStatusGroup> groups_;
  std::vector<MetaCommittedStatusSlotRange> slot_ranges_;
};

}  // namespace keylane::meta
