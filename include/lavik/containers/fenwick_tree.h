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
#include "lavik/containers/cow_array.h"
#include "lavik/status_macros.h"

namespace lavik {

// Copy-on-write Fenwick tree over counts of ordered groups. Maps an element
// rank to a group ordinal and offset; prefix reads and count updates visit
// logarithmically many cells. Groups are abstract count buckets: no collection
// kind, record identity or physical location is retained here.
//
// Copies share owner-thread CowArray storage. Mutators belong to an unpublished
// view: a failure may partially update that builder, never a pinned
// predecessor. Callers validate counts, bound the population to UINT32_MAX
// groups and admit temporary build/suffix arrays. There is one cell layout,
// so the tree stores no policy tag or duplicate total.
class FenwickTree {
 public:
  struct Position {
    std::size_t group_index_;
    std::uint64_t offset_;
  };
  using CountChange = std::pair<std::size_t, absl::int128>;

  // Consumes checked cumulative counts as build scratch, encoding them into
  // Fenwick cells in place to avoid another full-size scratch array.
  static absl::StatusOr<FenwickTree> FromCumulative(
      std::vector<std::uint64_t> ends) {
    for (std::size_t i = ends.size(); i > 1; --i) ends[i - 1] -= ends[i - 2];
    for (std::size_t i = 0; i < ends.size(); ++i) {
      const auto parent = i | (i + 1);
      if (parent < ends.size()) ends[parent] += ends[i];
    }
    auto cells = Storage::From(ends);
    LAVIK_RETURN_IF_ERROR(cells.status());
    FenwickTree result;
    result.cells_ = std::move(*cells);
    return result;
  }

  // Returns the count preceding a group; index == group count returns the
  // full total.
  std::uint64_t CountBefore(std::size_t index) const noexcept {
    assert(index <= cells_.size());
    std::uint64_t count = 0;
    for (; index != 0; index -= index & (~index + 1))
      count += cells_[index - 1];
    return count;
  }

  // The caller must first check rank < total count. Keeping that total with
  // the caller avoids an extra field or Fenwick sum on every lookup.
  Position Locate(std::uint64_t rank) const noexcept {
    std::size_t index = 0;
    std::uint64_t count = 0;
    for (auto stride = std::bit_floor(cells_.size()); stride != 0;
         stride >>= 1) {
      const auto next = index + stride;
      if (next <= cells_.size() && cells_[next - 1] <= rank - count) {
        count += cells_[next - 1];
        index = next;
      }
    }
    return {index, rank - count};
  }

  // Changes have distinct, increasing group ordinals and checked group counts.
  // Count-neutral writes retain every chunk. Combining deltas before applying
  // them prevents compensating changes from overflowing intermediate sums.
  absl::Status ApplyCounts(std::span<const CountChange> changes,
                           std::uint64_t item_count) {
    return ApplyPartialSums(changes, item_count, cells_.size());
  }

  // Suffix insertion retains the preceding group ordinals. The suffix carries
  // complete NEW cumulative counts, including prefix_changes; only the old
  // prefix cells receive those deltas separately. The suffix may grow but
  // cannot shrink the tree on this path.
  absl::StatusOr<FenwickTree> WithSuffix(
      std::size_t first, std::span<const std::uint64_t> cumulative,
      std::span<const CountChange> prefix_changes,
      std::uint64_t item_count) const {
    assert(first <= cells_.size() &&
           cumulative.size() >= cells_.size() - first);
    std::vector<std::uint64_t> encoded;
    encoded.reserve(cumulative.size());
    for (std::size_t i = 0; i < cumulative.size(); ++i) {
      const auto index = first + i;
      const auto start = (index + 1) & index;
      absl::int128 before;
      if (start <= first) {
        before = CountBefore(start);
        for (const auto& [changed_index, delta] : prefix_changes)
          if (changed_index < start) before += delta;
      } else {
        before = cumulative[start - first - 1];
      }
      const auto value = absl::int128(cumulative[i]) - before;
      if (value < 0 || value > item_count)
        return absl::DataLossError("invalid Fenwick suffix sum");
      encoded.push_back(static_cast<std::uint64_t>(value));
    }
    const auto old_suffix = cells_.size() - first;
    auto appended = cells_.Appended(std::span(encoded).subspan(old_suffix));
    LAVIK_RETURN_IF_ERROR(appended.status());
    FenwickTree result;
    result.cells_ = std::move(*appended);
    for (std::size_t i = 0; i < old_suffix; ++i) {
      LAVIK_RETURN_IF_ERROR(result.cells_.Set(first + i, encoded[i]));
    }
    LAVIK_RETURN_IF_ERROR(
        result.ApplyPartialSums(prefix_changes, item_count, first));
    return result;
  }

  // Scratch-planning bound; the underlying chunks account their own lifetime.
  std::size_t RetainedBytes() const noexcept { return cells_.RetainedBytes(); }

 private:
  // Small populations should not reserve hundreds of unused counters. The
  // default chunks also bound the bytes copied by a sparse count update.
  using Storage = CowArray<std::uint64_t>;
  Storage cells_;

  absl::Status ApplyPartialSums(std::span<const CountChange> count_changes,
                                std::uint64_t item_count,
                                std::size_t existing_size) {
    absl::InlinedVector<CountChange, 32> rank_changes;
    const auto changed_counts =
        std::count_if(count_changes.begin(), count_changes.end(),
                      [](const auto& change) { return change.second != 0; });
    if (changed_counts == 0) return absl::OkStatus();
    const auto updates = changed_counts * (std::bit_width(existing_size) + 1);
    // The caller bounds group counts to UINT32_MAX. This product
    // fits size_t on supported 64-bit targets; scratch is admitted before use.
    auto admission = TryReserveMemory(
        AllocatorUsableSizeForRequest(updates * sizeof(CountChange) + 1024));
    if (!admission) {
      RecordMemoryRejection();
      return absl::ResourceExhaustedError("OOM Fenwick update scratch");
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
      const auto value = absl::int128(cells_[index]) + delta;
      if (value < 0 || value > item_count)
        return absl::DataLossError("invalid Fenwick updated sum");
      LAVIK_RETURN_IF_ERROR(
          cells_.Set(index, static_cast<std::uint64_t>(value)));
    }
    return absl::OkStatus();
  }
};

}  // namespace lavik
