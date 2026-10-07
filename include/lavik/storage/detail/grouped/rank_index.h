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
#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <span>
#include <utility>
#include <vector>

#include "absl/container/inlined_vector.h"
#include "absl/numeric/int128.h"
#include "lavik/storage/detail/grouped/metadata_array.h"

namespace lavik::storage {

// Runtime-only rank policies. They do not affect ordered page encodings.
enum class OrderedRankLayout { kCumulative, kFenwick };

struct OrderedRankPosition {
  std::size_t group_index_;
  std::uint64_t offset_;
};

// One untagged, persistent rank storage shared by both algorithms. Its owner
// selects the layout from the immutable ordered root kind; no second runtime
// discriminator or per-view algorithm object is needed.
using OrderedRankStorage = GroupedMetadataArray<std::uint64_t, 256>;

// Stateless algorithms over rank storage. Callers must use the same Layout
// that built the cells. OrderedGroupDirectory enforces that contract by
// selecting from root.kind_ and rejecting kind changes before sharing ranks.
// Storage copies share owner-local metadata chunks. Mutators belong to an
// unpublished directory; failure may leave that builder partially updated,
// never a pinned predecessor. The enclosing directory validates counts and
// admits temporary build/suffix arrays before calling this component.
template <OrderedRankLayout Layout>
class RankOps {
 public:
  using CountChange = std::pair<std::size_t, absl::int128>;

  // Consumes checked cumulative counts. Encoding in place avoids a second
  // whole-directory scratch allocation during recovery and topology changes.
  static absl::StatusOr<OrderedRankStorage> FromCumulative(
      std::vector<std::uint64_t> ends) {
    if constexpr (Layout == OrderedRankLayout::kFenwick) {
      for (std::size_t i = ends.size(); i > 1; --i) ends[i - 1] -= ends[i - 2];
      for (std::size_t i = 0; i < ends.size(); ++i) {
        const auto parent = i | (i + 1);
        if (parent < ends.size()) ends[parent] += ends[i];
      }
    }
    return OrderedRankStorage::From(ends);
  }

  // Returns the count preceding a page; cells.size() denotes the full total.
  static std::uint64_t CountBefore(const OrderedRankStorage& cells,
                                   std::size_t index) noexcept {
    assert(index <= cells.size());
    if (index == 0) return 0;
    if constexpr (Layout == OrderedRankLayout::kFenwick) {
      std::uint64_t count = 0;
      for (; index != 0; index -= index & (~index + 1))
        count += cells[index - 1];
      return count;
    } else {
      return cells[index - 1];
    }
  }

  // The enclosing root must first check rank < item_count. Keeping that total
  // in the root avoids duplicating it in each resident index and querying a
  // Fenwick total on every already-bounds-checked lookup.
  static OrderedRankPosition Locate(const OrderedRankStorage& cells,
                                    std::uint64_t rank) noexcept {
    if constexpr (Layout == OrderedRankLayout::kFenwick) {
      std::size_t index = 0;
      std::uint64_t count = 0;
      for (auto stride = std::bit_floor(cells.size()); stride != 0;
           stride >>= 1) {
        const auto next = index + stride;
        if (next <= cells.size() && cells[next - 1] <= rank - count) {
          count += cells[next - 1];
          index = next;
        }
      }
      return {index, rank - count};
    } else {
      const auto found = std::upper_bound(cells.begin(), cells.end(), rank);
      const std::size_t index = found - cells.begin();
      return {index, rank - CountBefore(cells, index)};
    }
  }

  // Changes have distinct, increasing page ordinals and already-checked page
  // counts. Count-neutral writes retain every rank chunk. A Fenwick batch
  // combines deltas before applying them, so compensating changes cannot cause
  // a spurious intermediate overflow or underflow.
  static absl::Status ApplyCounts(OrderedRankStorage& cells,
                                  std::span<const CountChange> changes,
                                  std::uint64_t item_count) {
    if constexpr (Layout == OrderedRankLayout::kFenwick) {
      return ApplyPartialSums(cells, changes, item_count, cells.size());
    } else {
      absl::int128 delta = 0;
      std::size_t cursor = 0;
      const auto advance = [&](std::size_t end) -> absl::Status {
        if (delta == 0) {
          cursor = end;
          return absl::OkStatus();
        }
        for (; cursor < end; ++cursor) {
          const auto value = absl::int128(cells[cursor]) + delta;
          if (value < 0 || value > item_count)
            return absl::DataLossError("invalid ordered updated rank");
          auto status = cells.Set(cursor, static_cast<std::uint64_t>(value));
          if (!status.ok()) return status;
        }
        return absl::OkStatus();
      };
      for (const auto& [index, change] : changes) {
        auto status = advance(index);
        if (!status.ok()) return status;
        delta += change;
      }
      return advance(cells.size());
    }
  }

  // Stream suffix insertion retains the preceding page ordinals. The suffix
  // carries complete NEW cumulative counts, including prefix_changes; only
  // the old prefix cells should receive those deltas separately. Hiding cell
  // encoding here prevents directory code from treating partial sums as ranks.
  // The suffix may grow but cannot shrink the index on this fast path.
  static absl::StatusOr<OrderedRankStorage> WithSuffix(
      const OrderedRankStorage& cells, std::size_t first,
      std::span<const std::uint64_t> cumulative,
      std::span<const CountChange> prefix_changes, std::uint64_t item_count)
    requires(Layout == OrderedRankLayout::kFenwick)
  {
    assert(first <= cells.size() && cumulative.size() >= cells.size() - first);
    std::vector<std::uint64_t> encoded;
    encoded.reserve(cumulative.size());
    for (std::size_t i = 0; i < cumulative.size(); ++i) {
      const auto index = first + i;
      const auto start = (index + 1) & index;
      absl::int128 before;
      if (start <= first) {
        before = CountBefore(cells, start);
        for (const auto& [changed_index, delta] : prefix_changes)
          if (changed_index < start) before += delta;
      } else {
        before = cumulative[start - first - 1];
      }
      const auto value = absl::int128(cumulative[i]) - before;
      if (value < 0 || value > item_count)
        return absl::DataLossError("invalid Stream suffix rank sum");
      encoded.push_back(static_cast<std::uint64_t>(value));
    }
    const auto old_suffix = cells.size() - first;
    auto appended = cells.Appended(std::span(encoded).subspan(old_suffix));
    if (!appended.ok()) return appended.status();
    auto result = std::move(*appended);
    for (std::size_t i = 0; i < old_suffix; ++i) {
      auto status = result.Set(first + i, encoded[i]);
      if (!status.ok()) return status;
    }
    auto status = ApplyPartialSums(result, prefix_changes, item_count, first);
    if (!status.ok()) return status;
    return result;
  }

 private:
  static absl::Status ApplyPartialSums(
      OrderedRankStorage& cells, std::span<const CountChange> count_changes,
      std::uint64_t item_count, std::size_t existing_size) {
    absl::InlinedVector<CountChange, 32> rank_changes;
    const auto changed_counts =
        std::count_if(count_changes.begin(), count_changes.end(),
                      [](const auto& change) { return change.second != 0; });
    if (changed_counts == 0) return absl::OkStatus();
    const auto updates = changed_counts * (std::bit_width(existing_size) + 1);
    // Page counts are bounded by the 32-bit root group count. This product
    // fits size_t on supported 64-bit targets; scratch is admitted before use.
    auto admission = TryReserveMemory(
        AllocatorUsableSizeForRequest(updates * sizeof(CountChange) + 1024));
    if (!admission) {
      RecordMemoryRejection();
      return absl::ResourceExhaustedError("OOM Stream rank update scratch");
    }
    rank_changes.reserve(updates);
    for (const auto& [index, delta] : count_changes) {
      if (delta == 0) continue;
      for (std::size_t i = index + 1; i <= existing_size; i += i & (~i + 1))
        rank_changes.emplace_back(i - 1, delta);
    }
    std::sort(rank_changes.begin(), rank_changes.end());
    for (std::size_t i = 0; i < rank_changes.size();) {
      const auto index = rank_changes[i].first;
      absl::int128 delta = 0;
      do {
        delta += rank_changes[i++].second;
      } while (i < rank_changes.size() && rank_changes[i].first == index);
      if (delta == 0) continue;
      const auto value = absl::int128(cells[index]) + delta;
      if (value < 0 || value > item_count)
        return absl::DataLossError("invalid Stream updated rank sum");
      auto status = cells.Set(index, static_cast<std::uint64_t>(value));
      if (!status.ok()) return status;
    }
    return absl::OkStatus();
  }
};

using CumulativeRankOps = RankOps<OrderedRankLayout::kCumulative>;
using FenwickRankOps = RankOps<OrderedRankLayout::kFenwick>;

}  // namespace lavik::storage
