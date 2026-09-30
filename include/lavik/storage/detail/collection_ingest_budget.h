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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string_view>

#include "absl/status/statusor.h"
#include "lavik/storage/detail/grouped_collection.h"
#include "lavik/storage/detail/grouped_hash.h"
#include "lavik/storage/detail/record_index.h"
#include "lavik/storage/sorted_set.h"

namespace lavik::storage {

inline std::size_t SaturatingIngestAdd(std::size_t a, std::size_t b) {
  return b > SIZE_MAX - a ? SIZE_MAX : a + b;
}

inline std::size_t SaturatingIngestMultiply(std::size_t count,
                                            std::size_t width) {
  return count > SIZE_MAX / width ? SIZE_MAX : count * width;
}

struct InitialSortedSetIngestGroups {
  std::size_t ordered_;
  std::size_t members_;
};

// Upper bounds for a fresh dual-index graph, given compact entry bytes (member
// bytes + 12 per item, without a root header). Count pages, not individual
// members, when budgeting encoders, directory entries and retirement receipts.
// These bounds include adversarial prefix distributions and empty Hash leaves.
inline InitialSortedSetIngestGroups BoundInitialSortedSetIngestGroups(
    std::size_t bytes, std::size_t count) {
  if (count == 0) return {0, 0};
  // Greedy ordered splitting fills each consecutive pair beyond one page's
  // entry allowance; an indivisible large member remains one page.
  const auto ordered = std::min(
      count, SaturatingIngestAdd(1, SaturatingIngestMultiply(
                                        bytes / (kCollectionGroupTargetBytes -
                                                 kOrderedGroupHeaderBytes),
                                        2)));
  // The member graph uses member + 8-byte score + 8-byte framing. At each
  // prefix depth, splitting nodes are disjoint and each has more than target
  // minus compact-header entry bytes. There are at most 64 depths. A full
  // binary split tree has one more leaf than internal nodes, including empty
  // siblings. Singletons never split, regardless of their payload size.
  const auto member_bytes =
      SaturatingIngestAdd(bytes, SaturatingIngestMultiply(count, 4));
  const auto splitting_per_depth = std::min(
      count - 1,
      member_bytes / (kCollectionGroupTargetBytes - kHashValueHeaderBytes + 1));
  return {ordered, SaturatingIngestAdd(
                       1, SaturatingIngestMultiply(splitting_per_depth, 64))};
}

// Additional headroom used to decide whether to merge another decoded input
// page. Input ownership is already charged separately. Payload bytes and item
// counts are independent: a million tiny fields need substantially more plan
// metadata than a few large values of the same aggregate size.
//
// This is a conservative planning estimate, not a hard bound on every possible
// hash-prefix distribution or a reservation for future coroutine work. Actual
// page loads, graph publication and undo retain their own admission checks.
inline absl::StatusOr<std::size_t> CollectionIngestBuildBytes(
    ValueType type, std::uint64_t batch_bytes, std::size_t batch_items,
    std::uint64_t page_bytes, std::size_t page_items,
    std::uint64_t previous_bytes = 0, std::size_t previous_items = 0) {
  const auto bytes = SaturatingIngestAdd(batch_bytes, page_bytes);
  const auto count = SaturatingIngestAdd(batch_items, page_items);
  const auto working_count = SaturatingIngestAdd(count, previous_items);
  // Room for the next ordinary decoded RDB page (1 MiB) and fixed grouped
  // operation scratch. This is not an input-batch limit or a minimum batch:
  // an indivisible larger item goes through its own reader/writer admission.
  std::size_t required = 1024 * 1024 + 4 * 4096;
  auto add = [&](std::size_t n, std::size_t width = 1) {
    required =
        SaturatingIngestAdd(required, SaturatingIngestMultiply(n, width));
  };
  // Reading existing leaves can own encoded input and decoded strings
  // together. Incoming strings move into the plan instead of being copied.
  add(previous_bytes, 2);
  const bool hash = type == ValueType::kHash || type == ValueType::kSet;
  const bool sorted = type == ValueType::kSortedSet;
  auto hash_groups = working_count;
  auto ordered_groups = working_count;
  auto new_ordered_groups = count;
  if (sorted && previous_items == 0) {
    // A first batch has no old topology or retirement markers. Bound its
    // actual split output; charging every tiny member as a page prematurely
    // flushes the input and makes later batches rebuild the member graph.
    const auto groups = BoundInitialSortedSetIngestGroups(bytes, count);
    // For large members the depth bound can exceed the existing heuristic;
    // keep its batching policy unless the complete bound is tighter.
    if (SaturatingIngestAdd(groups.ordered_, groups.members_) < count) {
      hash_groups = groups.members_;
      ordered_groups = new_ordered_groups = groups.ordered_;
    }
  }
  if (hash || sorted) {
    // Hash input, growing after-image and parent/child split arrays coexist.
    // Five entry slots cover the input plus two growing split vectors.
    add(working_count, 5 * sizeof(HashEntry));
    // Duplicate validation uses a flat table of borrowed string views;
    // reserve room for load factor and power-of-two capacity rounding.
    add(working_count, 4 * (sizeof(std::string_view) + 1));
    // Existing-topology mutations retain the conservative per-item estimate;
    // fresh Sorted Sets use the complete split-tree bound above.
    add(hash_groups, sizeof(HashGroupSnapshot) + sizeof(HashGroupEncoder) +
                         sizeof(HashGroupMetadata) + sizeof(HashGroupId));
  }
  if (!hash) {
    // Ordered input and split-page entry arrays coexist. Sorted Sets also
    // create a member->score graph that owns a copy of each member string.
    add(working_count, 2 * sizeof(OrderedCollectionEntry));
    add(ordered_groups, sizeof(OrderedGroupSnapshot) +
                            sizeof(OrderedGroupEncoder) +
                            sizeof(OrderedGroupMetadata));
    // The completed graph owns a recovered directory (entries, id lookup and
    // rank ends), not merely the small on-disk page metadata. Location arrays
    // coexist for changed pages, physical-index construction and receipts.
    // Other layouts still allow an indivisible item to occupy a whole group.
    add(new_ordered_groups, sizeof(RecoveredOrderedGroup) +
                                sizeof(std::pair<std::uint64_t, std::size_t>) +
                                sizeof(std::uint64_t) +
                                3 * sizeof(RecordLocation));
    if (sorted) {
      add(bytes);
      add(previous_bytes);
      // Fresh member graphs reserve one incoming envelope. Existing graphs
      // also reconcile a before/after map while both plans remain admitted.
      add(working_count,
          (previous_items == 0 ? 256 : 2 * 256) + sizeof(ScoredMemberView));
    }
  }
  if (bytes == SIZE_MAX || working_count == SIZE_MAX || required == SIZE_MAX)
    return absl::ResourceExhaustedError(
        "collection ingest build size overflow");
  return required;
}

}  // namespace lavik::storage
