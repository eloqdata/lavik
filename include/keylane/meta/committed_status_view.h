#pragma once

// Compact atomic projection used only by cluster status. It deliberately
// excludes audit records, policy bodies, operation payload/evidence, and the
// complete seven-store aggregate so an operator read has bounded cost tied to
// current identity/topology rather than retained history.

#include <cstdint>
#include <vector>

#include "keylane/meta/grant_store.h"
#include "keylane/meta/identity_store.h"
#include "keylane/meta/topology_store.h"

namespace keylane::meta {

struct MetaCommittedStatusGroup {
  MetaTopologyGroupView topology_;
  MetaGroupGrantState grant_;
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
  std::vector<MetaMemberRecord> meta_members_;
  std::vector<MetaNodeRecord> data_nodes_;
  std::vector<MetaCommittedStatusGroup> groups_;
  std::vector<MetaCommittedStatusSlotRange> slot_ranges_;
};

}  // namespace keylane::meta
