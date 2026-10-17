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
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "lavik/expiration.h"
#include "lavik/memory.h"
#include "lavik/storage/engine.h"

namespace lavik {

enum class SetAggregate : std::uint8_t { kDifference, kIntersection, kUnion };

struct SetAggregateResult {
  // The result can leave the owner callback; its allowance travels with it.
  RetainedMemoryCharge charge_;
  std::vector<std::string> members_;
  std::uint64_t cardinality_ = 0;
};

inline absl::Status SetAggregateOom() {
  return absl::ResourceExhaustedError(
      "OOM Set multi-key working set exceeds maxmemory");
}

inline bool AddSetWorkingBytes(std::size_t* bytes, std::size_t count,
                               std::size_t width = 1) {
  if (count > (std::numeric_limits<std::size_t>::max() - *bytes) / width) {
    return false;
  }
  *bytes += count * width;
  return true;
}

// Compact results may have no exported charge. Establish their retained owner
// before crossing workers, including bounded scan and membership responses.
inline absl::Status AdmitSetReadResult(storage::HashResult* result) {
  if (result->retained_charge_.bytes() != 0) return absl::OkStatus();
  std::size_t bytes = 0;
  if (!AddSetWorkingBytes(&bytes, result->values_.capacity(),
                          sizeof(std::optional<std::string>))) {
    return SetAggregateOom();
  }
  for (const auto& member : result->values_) {
    if (member && !AddSetWorkingBytes(&bytes, member->capacity() + 1)) {
      return SetAggregateOom();
    }
  }
  if (bytes == 0) return absl::OkStatus();
  auto admitted = TryReserveMemory(bytes);
  if (!admitted) return SetAggregateOom();
  result->retained_charge_.Adopt(&*admitted, bytes);
  return absl::OkStatus();
}

// The caller retains every input's key lock for this entire operation. read
// executes on the key owner and returns admitted output that can cross workers.
// Only the current scan page, membership replies, and final output are owned;
// no complete input collection is retained alongside another input collection.
template <typename Reader>
bycorf::Task<absl::StatusOr<SetAggregateResult>> AggregateSetPages(
    std::span<const std::string> arguments, std::size_t first_source,
    std::size_t last_source, SetAggregate kind, bool cardinality_only,
    std::uint64_t limit, Reader& read) {
  SetAggregateResult result;
  // Key holds prevent writes; a pinned command clock also prevents expiration
  // from changing the source between metadata, scan, and membership probes.
  const std::uint64_t now_ms = RedisUnixTimeMillis();
  std::size_t bytes = 4096;
  if (!AddSetWorkingBytes(
          &bytes, last_source - first_source + 1,
          kind == SetAggregate::kUnion ? 64 : sizeof(std::uint64_t))) {
    co_return SetAggregateOom();
  }
  auto admission = TryReserveMemory(bytes);
  if (!admission) co_return SetAggregateOom();
  result.charge_.Adopt(&*admission, bytes);
  std::vector<std::uint64_t> lengths(
      kind == SetAggregate::kUnion ? 0 : last_source - first_source + 1);
  std::size_t source = first_source;
  // Type errors take precedence over an empty intersection/difference or LIMIT.
  // Union has no such early return: its page reads validate every distinct
  // source, so it needs neither a length array nor separate metadata lookups.
  if (kind != SetAggregate::kUnion) {
    for (std::size_t i = first_source; i <= last_source; ++i) {
      storage::HashOperation length;
      length.kind_ = storage::HashOperationKind::kLength;
      length.now_ms_ = now_ms;
      auto checked = co_await read(i, std::move(length));
      if (!checked.ok()) co_return checked.status();
      lengths[i - first_source] = checked->length_;
      if (kind == SetAggregate::kIntersection &&
          checked->length_ < lengths[source - first_source]) {
        source = i;
      }
    }
  }
  if (kind != SetAggregate::kUnion && lengths[source - first_source] == 0) {
    co_return result;
  }
  if (cardinality_only &&
      std::all_of(arguments.begin() + first_source,
                  arguments.begin() + last_source + 1,
                  [&](const auto& key) { return key == arguments[source]; })) {
    result.cardinality_ = lengths[source - first_source];
    if (limit != 0) result.cardinality_ = std::min(result.cardinality_, limit);
    co_return result;
  }
  if (kind == SetAggregate::kDifference) {
    for (std::size_t i = first_source + 1; i <= last_source; ++i) {
      if (arguments[i] == arguments[source]) co_return result;
    }
  }

  absl::flat_hash_set<std::string> unique;
  absl::flat_hash_set<std::string_view> scanned_sources;
  if (kind == SetAggregate::kUnion)
    scanned_sources.reserve(last_source - first_source + 1);
  const auto retain = [&](std::size_t extra) -> absl::Status {
    if (extra == 0) return absl::OkStatus();
    if (extra >
        std::numeric_limits<std::size_t>::max() - result.charge_.bytes()) {
      return SetAggregateOom();
    }
    auto growth = TryReserveMemory(extra);
    if (!growth) return SetAggregateOom();
    // This helper stays on its coordinating worker even when read hops owners.
    result.charge_.Resize(result.charge_.bytes() + extra);
    return absl::OkStatus();
  };
  constexpr std::size_t kMinProbeBatch = 64;
  for (std::size_t current = source; current <= last_source; ++current) {
    if (kind != SetAggregate::kUnion && current != source) break;
    if (kind != SetAggregate::kUnion && lengths[current - first_source] == 0)
      continue;
    if (kind == SetAggregate::kUnion &&
        !scanned_sources.insert(arguments[current]).second) {
      continue;
    }
    const bool limited = cardinality_only && limit != 0;
    std::size_t probe_batch_hint = SIZE_MAX;
    if (limited) {
      probe_batch_hint = lengths[current - first_source] <= kMinProbeBatch
                             ? kMinProbeBatch
                             : static_cast<std::size_t>(std::min<std::uint64_t>(
                                   limit - result.cardinality_, SIZE_MAX));
    }
    std::uint64_t cursor = 0;
    do {
      storage::HashOperation projection;
      projection.kind_ = storage::HashOperationKind::kReadPage;
      projection.now_ms_ = now_ms;
      projection.cursor_ = cursor;
      // Held key intent keeps the routing cursor stable. Consume each leaf in
      // stored order without SCAN's digest index or sorting; owned strings can
      // survive the membership reads below without retaining physical leases.
      auto page = co_await read(current, std::move(projection));
      if (!page.ok()) co_return page.status();
      cursor = page->cursor_;
      std::size_t scratch_bytes = 0;
      if (!AddSetWorkingBytes(&scratch_bytes, page->values_.size(),
                              sizeof(std::string_view) + sizeof(std::size_t))) {
        co_return SetAggregateOom();
      }
      auto scratch = TryReserveMemory(scratch_bytes);
      if (!scratch) co_return SetAggregateOom();
      std::vector<std::size_t> positions;
      positions.reserve(page->values_.size());
      std::size_t batch_size = page->values_.size();
      // For a small source, one bounded batch of cheap comparisons costs less
      // than another storage call after an unsuccessful initial LIMIT probe.
      // Larger sources still start at LIMIT and grow only when needed.
      if (limited) batch_size = std::min(batch_size, probe_batch_hint);
      for (std::size_t batch_begin = 0; batch_begin < page->values_.size();) {
        const auto batch_end = batch_begin + batch_size;
        if (kind != SetAggregate::kUnion) {
          for (std::size_t other = first_source; other <= last_source;
               ++other) {
            if (other == source || arguments[other] == arguments[source] ||
                (kind == SetAggregate::kDifference &&
                 lengths[other - first_source] == 0)) {
              continue;
            }
            storage::HashOperation probe;
            probe.kind_ = storage::HashOperationKind::kGetMany;
            probe.now_ms_ = now_ms;
            probe.fields_.reserve(batch_size);
            positions.clear();
            for (std::size_t i = batch_begin; i < batch_end; ++i) {
              if (!page->values_[i]) continue;
              probe.fields_.push_back(*page->values_[i]);
              positions.push_back(i);
            }
            if (positions.empty()) break;
            const bool single = positions.size() == 1;
            // LIMIT 1 and a shrinking candidate batch need only one bit, not
            // an admitted vector containing an owned empty Set value.
            if (single) probe.kind_ = storage::HashOperationKind::kExists;
            auto present = co_await read(other, std::move(probe));
            if (!present.ok()) co_return present.status();
            if (single) {
              if ((present->integer_ != 0) !=
                  (kind == SetAggregate::kIntersection)) {
                page->values_[positions.front()].reset();
              }
              continue;
            }
            if (present->values_.size() != positions.size()) {
              co_return absl::InternalError(
                  "Set membership result length mismatch");
            }
            for (std::size_t i = 0; i < positions.size(); ++i) {
              if (present->values_[i].has_value() !=
                  (kind == SetAggregate::kIntersection)) {
                page->values_[positions[i]].reset();
              }
            }
          }
        }
        std::size_t retained_bytes = 0;
        for (std::size_t i = batch_begin; i < batch_end; ++i) {
          auto& member = page->values_[i];
          if (!member) continue;
          if (kind == SetAggregate::kUnion && unique.contains(*member)) {
            // Already-owned members need neither admission nor another hash
            // lookup in the insertion pass below.
            member.reset();
            continue;
          }
          if (cardinality_only) {
            ++result.cardinality_;
            if (limit != 0 && result.cardinality_ == limit) co_return result;
          } else if (!AddSetWorkingBytes(&retained_bytes, 1, 256) ||
                     !AddSetWorkingBytes(&retained_bytes,
                                         member->capacity() + 1, 4)) {
            co_return SetAggregateOom();
          }
        }
        if (!cardinality_only) {
          // Admit table/vector growth and overlapping RESP/STORE output before
          // moving strings out of the admitted page. Duplicate union members do
          // not accumulate charges or owning copies.
          auto reserved = retain(retained_bytes);
          if (!reserved.ok()) co_return reserved;
          for (std::size_t i = batch_begin; i < batch_end; ++i) {
            auto& member = page->values_[i];
            if (!member) continue;
            if (kind == SetAggregate::kUnion) {
              unique.insert(std::move(*member));
            } else {
              result.members_.push_back(std::move(*member));
            }
          }
        }
        batch_begin = batch_end;
        // A small LIMIT can stop after its first matches. Sparse/disjoint
        // inputs grow the next membership batch instead of doing a disk read
        // per rejected candidate. Keep that hint across leaves so each new
        // page does not repeat an unsuccessful scalar probe.
        if (limited) {
          probe_batch_hint = std::max(
              probe_batch_hint,
              std::max<std::size_t>(kMinProbeBatch, batch_size > SIZE_MAX / 2
                                                        ? SIZE_MAX
                                                        : batch_size * 2));
        }
        batch_size = page->values_.size() - batch_begin;
        if (limited) batch_size = std::min(batch_size, probe_batch_hint);
      }
    } while (cursor != 0);
    if (kind != SetAggregate::kUnion) break;
  }
  if (kind == SetAggregate::kUnion) {
    result.members_.reserve(unique.size());
    for (auto it = unique.begin(); it != unique.end();) {
      const auto current = it++;
      result.members_.push_back(std::move(unique.extract(current).value()));
    }
  }
  if (!cardinality_only) {
    // Set replies are unordered. STORE and its captured SADD use this same
    // member sequence, so replication needs no lexical ordering either.
    result.cardinality_ = result.members_.size();
  }
  co_return result;
}

}  // namespace lavik
