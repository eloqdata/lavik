#pragma once

#include <string>

#include "keylane/meta/encoding.h"
#include "keylane/meta/topology_store.h"

namespace keylane::meta {

// Fixture-only corruption/setup seam. Production authority transitions cannot
// independently overwrite the Group's term or owner.
class MetaTopologyTestAccess {
 public:
  static absl::Status SetOwner(MetaTopologyStore& store,
                               const std::string& group_id,
                               const std::string& owner) {
    const auto group = store.groups_.find(group_id);
    if (group == store.groups_.end())
      return MetaDomainRejectError("unknown group");
    group->second.record_.owner_ = owner;
    return absl::OkStatus();
  }

  static absl::Status SetGroupTerm(MetaTopologyStore& store,
                                   const std::string& group_id,
                                   std::uint64_t term) {
    const auto group = store.groups_.find(group_id);
    if (group == store.groups_.end())
      return MetaDomainRejectError("unknown group");
    group->second.record_.group_term_ = term;
    return absl::OkStatus();
  }
};

}  // namespace keylane::meta
