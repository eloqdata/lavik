#pragma once

#include "keylane/storage/detail/grouped_collection.h"

namespace keylane::storage {

// Bridges a complete logical Sorted Set callback result to changed physical
// pages. Old page upper bounds remain routing fences for this mutation, so a
// member moving from one end to the other changes its source/destination
// pages, not the intervening collection. Empty pages are durably retired and
// split/link updates share the caller's one root/batch publication boundary.
absl::StatusOr<OrderedCollectionMutationPlan> PlanSortedSetRewrite(
    const OrderedGroupDirectory& directory,
    std::span<const OrderedCollectionEntry> before,
    std::vector<OrderedCollectionEntry> after,
    std::size_t target_bytes = kOrderedGroupTargetBytes);

}  // namespace keylane::storage
