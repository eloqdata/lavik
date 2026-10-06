/*
 * Copyright (C) 2026 EloqData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

// Pure committed-state projection for the Meta -> Data control plane.
//
// This module deliberately stops before transport/session concerns: callers
// give it one atomic MetaDataPublicationView and receive canonical
// FullDesiredState bytes that can be published or chunked later. It performs no
// I/O and owns no mutable state.

#include <cstddef>
#include <string>
#include <string_view>

#include "absl/status/statusor.h"
#include "lavik/cluster/control_protocol.h"
#include "lavik/meta/data_publication_view.h"

namespace lavik::meta {

struct NodeControlBatch {
  cluster::control::FullDesiredState full_state;
  std::string encoded_full_state;

  // Local publication proof, never encoded. Configuration-only Advance can
  // put control_revision ahead of the state represented by this batch.
  std::uint64_t state_change_index = 0;

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
      const MetaDataPublicationView& view, std::string_view node_id);
};

}  // namespace lavik::meta
