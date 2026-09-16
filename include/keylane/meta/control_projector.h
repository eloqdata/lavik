#pragma once

// Pure committed-state projection for the Meta -> Data control plane.
//
// This module deliberately stops before transport/session concerns: callers
// give it one atomic MetaCommittedView and receive canonical FullDesiredState
// bytes that can be published or chunked later. It performs no I/O and owns
// no mutable state.

#include <cstddef>
#include <string>
#include <string_view>

#include "absl/status/statusor.h"
#include "keylane/cluster/control_protocol.h"
#include "keylane/meta/coordinator.h"

namespace keylane::meta {

struct NodeControlBatch {
  cluster::control::FullDesiredState full_state;
  std::string encoded_full_state;

  bool operator==(const NodeControlBatch&) const = default;
};

// Conservatively counts the batch object and the capacities of every owned
// string/vector. Allocator metadata is outside the C++ object model; callers
// use this retained-capacity weight rather than the canonical wire length.
std::size_t NodeControlBatchRetainedBytes(
    const NodeControlBatch& batch) noexcept;

class MetaControlProjector {
 public:
  // Produces the complete deterministic projection for one active data node.
  // A publisher can derive lease-challenge candidates without another Meta
  // read: they are exactly the groups whose owner_node_id equals the requested
  // node and whose grant_active bit is set, using that group's owner assignment
  // and the projected global Authority Lease Policy ceiling. Fenced owner
  // identity remains projected for heartbeat role classification but cannot
  // produce a challenge. The leader publisher may only lower the projected
  // ceiling to its local leadership-validity limit.
  static absl::StatusOr<NodeControlBatch> ProjectNode(
      const MetaCommittedView& view, std::string_view node_id);
};

}  // namespace keylane::meta
