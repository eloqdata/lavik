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

#include <cmath>
#include <map>
#include <set>
#include <type_traits>
#include <utility>

#include "../impl.h"
#include "absl/container/inlined_vector.h"
#include "lavik/glob.h"
#include "lavik/random_sample.h"
#include "lavik/storage/detail/grouped/scratch.h"
#include "lavik/storage/detail/ordered_compact_codec.h"
#include "read_window.h"
#include "zset_work_budget.h"

namespace lavik::storage {
namespace {

constexpr auto kNoPage = std::numeric_limits<std::size_t>::max();
struct MemberState {
  std::optional<double> before_;
  std::optional<double> after_;
  std::size_t source_ = kNoPage;
  std::size_t destination_ = kNoPage;
  bool touched_ = false;
};
using Members = absl::flat_hash_map<std::string_view, MemberState>;

// Admission outlives the page's entries and, for borrowed scans, its read
// lease.
template <typename Page>
struct AdmittedScanPage {
  MemoryReservation admission_;
  Page page_;
};

bool ReadOnly(const SortedSetOperation& operation) {
  return operation.kind_ != SortedSetOperationKind::kAdd &&
         operation.kind_ != SortedSetOperationKind::kRemove &&
         operation.kind_ != SortedSetOperationKind::kPop &&
         operation.kind_ != SortedSetOperationKind::kRemoveRange;
}

bool ScanRead(const SortedSetOperation& operation) {
  return operation.kind_ == SortedSetOperationKind::kRange ||
         operation.kind_ == SortedSetOperationKind::kRemoveRange ||
         operation.kind_ == SortedSetOperationKind::kRank ||
         operation.kind_ == SortedSetOperationKind::kCount;
}

std::pair<std::uint64_t, std::uint64_t> RankSlice(std::int64_t first,
                                                  std::int64_t last,
                                                  std::uint64_t count) {
  if (count == 0) return {0, 0};
  if (first < 0) first += static_cast<std::int64_t>(count);
  if (last < 0) last += static_cast<std::int64_t>(count);
  first = std::max<std::int64_t>(first, 0);
  if (last < first || static_cast<std::uint64_t>(first) >= count) return {0, 0};
  return {first, std::min<std::uint64_t>(last, count - 1) + 1};
}

bool Matches(const auto& entry, const SortedSetOperation& operation) {
  if (operation.range_mode_ == SortedSetRangeMode::kScore) {
    const auto low = operation.minimum_score_;
    const auto high = operation.maximum_score_;
    return (low.exclusive_ ? entry.score_ > low.value_
                           : entry.score_ >= low.value_) &&
           (high.exclusive_ ? entry.score_ < high.value_
                            : entry.score_ <= high.value_);
  }
  const auto low = operation.minimum_lex_;
  const auto high = operation.maximum_lex_;
  return (low.infinity_ < 0 ||
          (low.infinity_ == 0 &&
           (low.exclusive_ ? entry.value_ > low.value_
                           : entry.value_ >= low.value_))) &&
         (high.infinity_ > 0 ||
          (high.infinity_ == 0 &&
           (high.exclusive_ ? entry.value_ < high.value_
                            : entry.value_ <= high.value_)));
}

absl::StatusOr<SortedSetMember> MakeOutput(auto&& entry, bool prepaid = false) {
  auto admission = prepaid ? std::optional<MemoryReservation>{}
                           : TryReserveMemory(entry.value_.size() + 64);
  if (!prepaid && !admission) {
    RecordMemoryRejection();
    return absl::ResourceExhaustedError(
        "OOM Sorted Set output member admission");
  }
  SortedSetMember output;
  output.member_ = std::forward<decltype(entry)>(entry).value_;
  output.score_ = entry.score_;
  if (prepaid)
    output.retained_charge_.Account(CurrentMemoryAccountingShard(),
                                    output.member_.capacity() + 1);
  else
    output.retained_charge_.Adopt(&*admission, output.member_.capacity() + 1);
  return output;
}

absl::Status AppendOutput(SortedSetMember member, SortedSetResult* result,
                          bool prepaid = false) {
  auto& output = result->members_;
  if (output.size() == output.capacity()) {
    if (output.capacity() >
        std::numeric_limits<std::size_t>::max() / (2 * sizeof(SortedSetMember)))
      return absl::ResourceExhaustedError("Sorted Set output count overflow");
    const auto capacity = std::max<std::size_t>(8, output.capacity() * 2);
    auto admission = prepaid
                         ? std::optional<MemoryReservation>{}
                         : TryReserveMemory(capacity * sizeof(SortedSetMember));
    if (!prepaid && !admission) {
      RecordMemoryRejection();
      return absl::ResourceExhaustedError(
          "OOM Sorted Set output vector admission");
    }
    output.reserve(capacity);
    if (prepaid)
      result->retained_charge_.Account(
          CurrentMemoryAccountingShard(),
          output.capacity() * sizeof(SortedSetMember));
    else
      result->retained_charge_.Adopt(
          &*admission, output.capacity() * sizeof(SortedSetMember));
  }
  output.push_back(std::move(member));
  return absl::OkStatus();
}

// Output metadata grows geometrically, while member bytes move once per page.
// A parallel wave prepays these allocations; serial/compact callers acquire
// admission before growth. Retained accounting always covers the final holder.
absl::Status ReservePackedOutput(SortedSetResult* result,
                                 std::size_t additional_members,
                                 std::size_t additional_blocks,
                                 bool prepaid = false) {
  if (!result->packed_members_) result->packed_members_.emplace();
  auto& packed = *result->packed_members_;
  std::size_t allocation = 0, growth = 0;
  auto capacity = [&](const auto& vector,
                      std::size_t additional) -> std::optional<std::size_t> {
    using Value = typename std::decay_t<decltype(vector)>::value_type;
    if (additional > SIZE_MAX - vector.size()) return std::nullopt;
    const auto needed = vector.size() + additional;
    auto count = vector.capacity();
    while (count < needed) {
      if (count > SIZE_MAX / 2) return std::nullopt;
      count = std::max<std::size_t>(8, count * 2);
    }
    if (count > SIZE_MAX / sizeof(Value)) return std::nullopt;
    if (count != vector.capacity()) {
      const auto bytes = count * sizeof(Value);
      if (bytes > SIZE_MAX - allocation) return std::nullopt;
      allocation += bytes;
      growth += (count - vector.capacity()) * sizeof(Value);
    }
    return count;
  };
  const auto slots = capacity(packed.members_.values_, additional_members);
  const auto scores = capacity(packed.scores_, additional_members);
  const auto blocks = capacity(packed.members_.blocks_, additional_blocks);
  if (!slots || !scores || !blocks ||
      growth > SIZE_MAX - result->retained_charge_.bytes())
    return absl::ResourceExhaustedError("Sorted Set packed output overflow");
  if (allocation == 0) return absl::OkStatus();
  auto admission = prepaid ? std::optional<MemoryReservation>{}
                           : TryReserveMemory(allocation);
  if (!prepaid && !admission) {
    RecordMemoryRejection();
    return absl::ResourceExhaustedError(
        "OOM Sorted Set packed output admission");
  }
  packed.members_.values_.reserve(*slots);
  packed.scores_.reserve(*scores);
  packed.members_.blocks_.reserve(*blocks);
  if (packed.members_.values_.capacity() > *slots ||
      packed.scores_.capacity() > *scores ||
      packed.members_.blocks_.capacity() > *blocks)
    return absl::ResourceExhaustedError(
        "OOM Sorted Set packed capacity exceeds admission");
  result->retained_charge_.Account(CurrentMemoryAccountingShard(),
                                   result->retained_charge_.bytes() + growth);
  return absl::OkStatus();
}

absl::Status AppendPackedCopy(std::string_view member, double score,
                              SortedSetResult* result) {
  if (!result->packed_members_) result->packed_members_.emplace();
  auto& packed = *result->packed_members_;
  const bool new_block =
      packed.members_.blocks_.empty() ||
      member.size() > packed.members_.blocks_.back().capacity() -
                          packed.members_.blocks_.back().size();
  auto reserved = ReservePackedOutput(result, 1, new_block ? 1 : 0);
  if (!reserved.ok()) return reserved;
  if (new_block) {
    const auto capacity = std::max<std::size_t>(4096, member.size());
    if (capacity == SIZE_MAX ||
        capacity + 1 > SIZE_MAX - result->retained_charge_.bytes())
      return absl::ResourceExhaustedError("Sorted Set packed output overflow");
    auto admission = TryReserveMemory(capacity + 1);
    if (!admission) {
      RecordMemoryRejection();
      return absl::ResourceExhaustedError(
          "OOM Sorted Set packed member admission");
    }
    std::string block;
    block.reserve(capacity);
    if (block.capacity() > capacity)
      return absl::ResourceExhaustedError(
          "OOM Sorted Set packed capacity exceeds admission");
    result->retained_charge_.Account(
        CurrentMemoryAccountingShard(),
        result->retained_charge_.bytes() + block.capacity() + 1);
    packed.members_.blocks_.push_back(std::move(block));
  }
  auto& block = packed.members_.blocks_.back();
  packed.members_.values_.push_back(
      {packed.members_.blocks_.size() - 1, block.size(), member.size()});
  block.append(member);
  packed.scores_.push_back(score);
  return absl::OkStatus();
}

absl::Status AppendRangeOutput(SortedSetMember member,
                               const SortedSetOperation& operation,
                               SortedSetResult* result) {
  if (operation.packed_output_ &&
      operation.kind_ == SortedSetOperationKind::kRange)
    return AppendPackedCopy(member.member_, member.score_, result);
  return AppendOutput(std::move(member), result);
}

// Keep routing metadata and reply ownership admitted separately. Sorting draws
// by rank coalesces page reads; the output slot preserves replacement draw
// order.
struct RandomSelection {
  std::optional<MemoryReservation> admission_;
  std::vector<std::pair<std::uint64_t, std::size_t>> draws_;

  absl::Status Prepare(std::uint64_t population, std::int64_t count,
                       bool compact, SortedSetResult* result) {
    if (population == 0 || count == 0) return absl::OkStatus();
    const auto requested = count < 0
                               ? static_cast<std::uint64_t>(-count)
                               : std::min<std::uint64_t>(count, population);
    // Covers temporary ranks, Floyd's hash set (or <=3x subtract vector),
    // and the rank-to-output routing pairs, including container overhead.
    if (requested > std::numeric_limits<std::size_t>::max() / 128)
      return absl::ResourceExhaustedError("Sorted Set random count overflow");
    admission_ = TryReserveMemory(requested * 128);
    if (!admission_) {
      RecordMemoryRejection();
      return absl::ResourceExhaustedError(
          "OOM Sorted Set random rank admission");
    }
    draws_.reserve(requested);
    if (count < 0) {
      for (std::size_t i = 0; i < requested; ++i)
        draws_.emplace_back(RandomRank(population, RandomSampleGenerator()), i);
    } else {
      auto ranks = SampleUniqueRandomRanks(population, requested, compact,
                                           RandomSampleGenerator());
      for (std::size_t i = 0; i < ranks.size(); ++i)
        draws_.emplace_back(ranks[i], i);
    }
    std::sort(draws_.begin(), draws_.end());
    if (requested > result->members_.max_size())
      return absl::ResourceExhaustedError("Sorted Set random reply overflow");
    auto output = TryReserveMemory(requested * sizeof(SortedSetMember));
    if (!output) {
      RecordMemoryRejection();
      return absl::ResourceExhaustedError(
          "OOM Sorted Set random reply admission");
    }
    result->members_.resize(requested);
    result->retained_charge_.Adopt(
        &*output, result->members_.capacity() * sizeof(SortedSetMember));
    return absl::OkStatus();
  }
};

// Pages are score ordered, whereas SCAN cursors are digest prefixes. The first
// pass keeps only COUNT integer prefixes (not members) to find the examination
// boundary. The second pass emits that complete bucket and discovers the next
// cursor. This preserves mutation-tolerant cursor semantics with two bounded
// page scans, including MATCH misses and collisions at the COUNT boundary.
struct ScanBoundary {
  MemoryReservation admission_;
  std::vector<std::uint64_t> prefixes_;
  std::size_t limit_ = 0;
  absl::Status Prepare(const SortedSetOperation& operation,
                       std::uint64_t cardinality) {
    const auto limit = std::min(operation.scan_count_, cardinality);
    if (limit > std::numeric_limits<std::size_t>::max() / sizeof(std::uint64_t))
      return absl::ResourceExhaustedError("Sorted Set scan count overflow");
    auto admission = TryReserveMemory(limit * sizeof(std::uint64_t));
    if (!admission) {
      RecordMemoryRejection();
      return absl::ResourceExhaustedError(
          "OOM Sorted Set scan boundary admission");
    }
    admission_ = std::move(*admission);
    limit_ = limit;
    prefixes_.reserve(limit_);
    return absl::OkStatus();
  }
  void Observe(const auto& entry, std::uint64_t cursor) {
    const auto prefix = ScanCursorPrefix(ComputeDigest(entry.value_));
    if (prefix < cursor || limit_ == 0) return;
    if (prefixes_.size() == limit_) {
      if (prefix >= prefixes_.front()) return;
      std::pop_heap(prefixes_.begin(), prefixes_.end());
      prefixes_.back() = prefix;
    } else {
      prefixes_.push_back(prefix);
    }
    std::push_heap(prefixes_.begin(), prefixes_.end());
  }
  absl::Status Emit(const auto& entry, const SortedSetOperation& operation,
                    SortedSetResult* result) const {
    if (prefixes_.empty()) return absl::OkStatus();
    const auto prefix = ScanCursorPrefix(ComputeDigest(entry.value_));
    if (prefix < operation.scan_cursor_) return absl::OkStatus();
    if (prefix > prefixes_.front()) {
      if (result->next_cursor_ == 0 || prefix < result->next_cursor_)
        result->next_cursor_ = prefix;
      return absl::OkStatus();
    }
    if (operation.scan_pattern_ != "*" &&
        !RedisGlobMatch(operation.scan_pattern_, entry.value_))
      return absl::OkStatus();
    auto copy = MakeOutput(entry);
    if (!copy.ok()) return copy.status();
    return AppendOutput(std::move(*copy), result);
  }
  static void Sort(SortedSetResult* result) {
    std::sort(result->members_.begin(), result->members_.end(),
              [](const auto& left, const auto& right) {
                const auto a = ScanCursorPrefix(ComputeDigest(left.member_));
                const auto b = ScanCursorPrefix(ComputeDigest(right.member_));
                return a < b || (a == b && left.member_ < right.member_);
              });
  }
};

// Physical rank/score order is already the requested order. This cursor keeps
// only the admitted reply, not pointers into pages that the next read releases.
struct ReadCursor {
  const SortedSetOperation& operation_;
  SortedSetResult* result_;
  std::uint64_t first_ = 0, end_ = 0, skipped_ = 0;
  bool done_ = false;
  ReadCursor(const SortedSetOperation& operation, SortedSetResult* result)
      : operation_(operation), result_(result) {
    const auto range =
        RankSlice(operation.first_, operation.last_, result->length_);
    first_ = range.first;
    end_ = range.second;
    done_ = (operation.kind_ == SortedSetOperationKind::kRange ||
             operation.kind_ == SortedSetOperationKind::kRemoveRange) &&
            ((operation.range_mode_ == SortedSetRangeMode::kRank &&
              first_ == end_) ||
             (operation.limit_ &&
              (operation.offset_ < 0 || operation.count_ == 0)));
  }

  absl::Status VisitWithOutput(const auto& entry, std::uint64_t rank,
                               auto append) {
    if (done_) return absl::OkStatus();
    if (operation_.kind_ == SortedSetOperationKind::kRank) {
      if (entry.value_ == operation_.members_.front()) {
        result_->rank_ =
            operation_.reverse_ ? result_->length_ - 1 - rank : rank;
        result_->rank_score_ = entry.score_;
        done_ = true;
      }
      return absl::OkStatus();
    }
    if (operation_.kind_ == SortedSetOperationKind::kCount) {
      if (Matches(entry, operation_)) ++result_->count_;
      return absl::OkStatus();
    }
    if (operation_.range_mode_ == SortedSetRangeMode::kRank) {
      const auto position =
          operation_.reverse_ ? result_->length_ - 1 - rank : rank;
      if (position < first_ || position >= end_) return absl::OkStatus();
    } else if (!Matches(entry, operation_))
      return absl::OkStatus();
    if (operation_.limit_ &&
        skipped_ < static_cast<std::uint64_t>(operation_.offset_)) {
      ++skipped_;
      return absl::OkStatus();
    }
    auto appended = append();
    if (!appended.ok()) return appended;
    if (operation_.limit_ && operation_.count_ >= 0 &&
        result_->member_count() >=
            static_cast<std::uint64_t>(operation_.count_))
      done_ = true;
    if (operation_.range_mode_ == SortedSetRangeMode::kRank &&
        result_->member_count() == end_ - first_)
      done_ = true;
    return absl::OkStatus();
  }

  absl::Status Visit(auto&& entry, std::uint64_t rank, bool prepaid = false) {
    return VisitWithOutput(entry, rank, [&] {
      if (operation_.packed_output_ &&
          operation_.kind_ == SortedSetOperationKind::kRange)
        return AppendPackedCopy(entry.value_, entry.score_, result_);
      auto copied = MakeOutput(std::forward<decltype(entry)>(entry), prepaid);
      if (!copied.ok()) return copied.status();
      return AppendOutput(std::move(*copied), result_, prepaid);
    });
  }
};

bool BetterLexCandidate(const auto& entry, const SortedSetOperation& operation,
                        std::optional<std::string_view> after,
                        const std::optional<SortedSetMember>& candidate) {
  if (!Matches(entry, operation)) return false;
  if (after &&
      (operation.reverse_ ? entry.value_ >= *after : entry.value_ <= *after))
    return false;
  return !candidate || (operation.reverse_ ? entry.value_ > candidate->member_
                                           : entry.value_ < candidate->member_);
}

absl::Status ReadCompact(std::span<const OrderedCollectionEntry> entries,
                         const SortedSetOperation& operation,
                         SortedSetResult* result) {
  ReadCursor cursor(operation, result);
  if (cursor.done_) return absl::OkStatus();
  if (operation.kind_ == SortedSetOperationKind::kRange &&
      operation.range_mode_ == SortedSetRangeMode::kLex) {
    std::optional<SortedSetMember> skipped;
    std::uint64_t offset = operation.limit_ ? operation.offset_ : 0;
    for (;;) {
      const std::optional<std::string_view> after =
          result->member_count() != 0
              ? std::optional<std::string_view>(
                    result->member_view(result->member_count() - 1).member_)
          : skipped ? std::optional<std::string_view>(skipped->member_)
                    : std::nullopt;
      std::optional<SortedSetMember> candidate;
      for (const auto& entry : entries) {
        if (!BetterLexCandidate(entry, operation, after, candidate)) continue;
        auto copied = MakeOutput(entry);
        if (!copied.ok()) return copied.status();
        candidate.emplace(std::move(*copied));
      }
      if (!candidate) break;
      if (offset != 0) {
        skipped.emplace(std::move(*candidate));
        --offset;
        continue;
      }
      skipped.reset();
      auto appended =
          AppendRangeOutput(std::move(*candidate), operation, result);
      if (!appended.ok()) return appended;
      if (operation.limit_ && operation.count_ >= 0 &&
          result->member_count() >=
              static_cast<std::uint64_t>(operation.count_))
        break;
    }
    return absl::OkStatus();
  }
  for (std::size_t i = 0; !cursor.done_ && i < entries.size(); ++i) {
    const auto rank = operation.reverse_ ? entries.size() - 1 - i : i;
    auto status = cursor.Visit(entries[rank], rank);
    if (!status.ok()) return status;
  }
  return absl::OkStatus();
}

absl::Status Validate(const SortedSetOperation& operation) {
  if (operation.kind_ == SortedSetOperationKind::kRandom &&
      operation.count_ == std::numeric_limits<std::int64_t>::min())
    return absl::InvalidArgumentError("Sorted Set random count out of range");
  if (operation.kind_ == SortedSetOperationKind::kScan &&
      operation.scan_count_ == 0)
    return absl::InvalidArgumentError("Sorted Set scan COUNT must be positive");
  if (operation.kind_ == SortedSetOperationKind::kRange && operation.limit_ &&
      operation.range_mode_ == SortedSetRangeMode::kRank)
    return absl::InvalidArgumentError(
        "LIMIT requires score or lexicographic bounds");
  if (operation.kind_ == SortedSetOperationKind::kCount &&
      operation.range_mode_ == SortedSetRangeMode::kRank)
    return absl::InvalidArgumentError(
        "Sorted Set count requires score or lexicographic bounds");
  if (operation.kind_ == SortedSetOperationKind::kRank &&
      operation.members_.size() != 1)
    return absl::InvalidArgumentError("Sorted Set rank requires one member");
  if (ScanRead(operation) &&
      operation.range_mode_ == SortedSetRangeMode::kScore &&
      (std::isnan(operation.minimum_score_.value_) ||
       std::isnan(operation.maximum_score_.value_)))
    return absl::InvalidArgumentError("min or max is not a float");
  if (operation.kind_ != SortedSetOperationKind::kAdd) return absl::OkStatus();
  if (operation.entries_.empty() || (operation.nx_ && operation.xx_) ||
      (operation.gt_ && operation.lt_) ||
      (operation.nx_ && (operation.gt_ || operation.lt_)) ||
      (operation.increment_ && operation.entries_.size() != 1))
    return absl::InvalidArgumentError("invalid Sorted Set add options");
  for (const auto& entry : operation.entries_) {
    if (std::isnan(entry.score_))
      return absl::InvalidArgumentError("value is not a valid float");
    if (entry.member_.size() > kMaxStringBytes)
      return absl::OutOfRangeError("Sorted Set member exceeds 512 MiB");
  }
  return absl::OkStatus();
}

absl::Status PrepareMembers(const SortedSetOperation& operation,
                            Members* members) {
  if (operation.kind_ == SortedSetOperationKind::kAdd) {
    for (const auto& entry : operation.entries_) {
      const auto [_, inserted] = members->try_emplace(entry.member_);
      if (!inserted && operation.reject_existing_)
        return absl::DataLossError("duplicate Sorted Set import member");
    }
  } else {
    for (const auto member : operation.members_) members->try_emplace(member);
  }
  return absl::OkStatus();
}

absl::Status PrepareScoreReply(std::size_t count, SortedSetResult* result) {
  if (count > std::numeric_limits<std::size_t>::max() /
                  (2 * sizeof(std::optional<double>)))
    return absl::ResourceExhaustedError("Sorted Set reply size overflow");
  auto reservation =
      TryReserveMemory(count * 2 * sizeof(std::optional<double>));
  if (!reservation) {
    RecordMemoryRejection();
    return absl::ResourceExhaustedError("OOM Sorted Set reply admission");
  }
  result->scores_.resize(count);
  result->retained_charge_.Adopt(
      &*reservation,
      result->scores_.capacity() * sizeof(std::optional<double>));
  return absl::OkStatus();
}

absl::Status ApplyInputs(const SortedSetOperation& operation, Members* members,
                         SortedSetResult* result) {
  if (operation.kind_ == SortedSetOperationKind::kScores) {
    auto allocated = PrepareScoreReply(operation.members_.size(), result);
    if (!allocated.ok()) return allocated;
    for (std::size_t i = 0; i < operation.members_.size(); ++i)
      result->scores_[i] = members->at(operation.members_[i]).before_;
    return absl::OkStatus();
  }
  if (operation.kind_ == SortedSetOperationKind::kRemove) {
    for (const auto member : operation.members_) {
      auto& state = members->at(member);
      if (!state.after_) continue;
      state.after_.reset();
      state.touched_ = true;
      ++result->changed_;
      --result->length_;
    }
    return absl::OkStatus();
  }
  for (const auto& entry : operation.entries_) {
    auto& state = members->at(entry.member_);
    if (operation.reject_existing_ && state.before_)
      return absl::DataLossError("duplicate Sorted Set import member");
    if (!state.after_) {
      if (operation.xx_) continue;
      state.after_ = entry.score_;
      state.touched_ = true;
      ++result->added_;
      ++result->changed_;
      ++result->length_;
      if (operation.increment_) result->incremented_ = entry.score_;
      continue;
    }
    if (operation.nx_) continue;
    const auto score =
        operation.increment_ ? *state.after_ + entry.score_ : entry.score_;
    if (std::isnan(score))
      return absl::InvalidArgumentError(
          "resulting score is not a number (NaN)");
    if ((operation.gt_ && score <= *state.after_) ||
        (operation.lt_ && score >= *state.after_))
      continue;
    if (score != *state.after_) {
      state.after_ = score;
      state.touched_ = true;
      ++result->changed_;
    }
    if (operation.increment_) result->incremented_ = score;
  }
  if (result->length_ > std::numeric_limits<std::uint32_t>::max())
    return absl::OutOfRangeError(
        "Sorted Set cardinality exceeds durable count");
  return absl::OkStatus();
}

absl::StatusOr<MemoryReservation> ReserveInputs(
    const SortedSetOperation& operation) {
  const auto count = operation.kind_ == SortedSetOperationKind::kAdd
                         ? operation.entries_.size()
                         : operation.members_.size();
  // Empty member inputs construct no map entries. In particular, do not
  // deny range removal's bounded compact workspace at maxmemory just for
  // the scratch budget's fixed overhead.
  if (count == 0) return MemoryReservation{};
  if (count > std::numeric_limits<std::size_t>::max() / 512)
    return absl::ResourceExhaustedError("Sorted Set input size overflow");
  GroupedScratchBudget budget;
  auto status = budget.AddBytes(count * 512);
  if (!status.ok()) return status;
  return budget.Reserve(1);
}

absl::StatusOr<CompactValueUpdate> ApplyCompactSortedSet(
    std::optional<CompactValueView> value, const SortedSetOperation& operation,
    Members& members, SortedSetResult& result,
    std::optional<MemoryReservation>& compact_admission) {
  // ExecuteCompactLocked owns decode/re-encode admission, including the
  // bounded inline workspace that lets shrinking commands run at
  // maxmemory. Range removal adds no input members or retained reply;
  // larger/indirect/external values still pass that layer's admission.
  if (operation.kind_ != SortedSetOperationKind::kRemoveRange) {
    GroupedScratchBudget budget;
    auto status = budget.AddBytes(value ? value->encoded_.size() : 0);
    if (!status.ok()) return status;
    for (const auto& entry : operation.entries_) {
      status = budget.AddBytes(entry.member_.size() + 256);
      if (!status.ok()) return status;
    }
    if (value) {
      if (value->logical_size_ > std::numeric_limits<std::size_t>::max() / 256)
        return absl::ResourceExhaustedError(
            "Sorted Set compact count overflow");
      status = budget.AddBytes(value->logical_size_ * 256);
      if (!status.ok()) return status;
    }
    auto admission = budget.Reserve(4);
    if (!admission.ok()) return admission.status();
    compact_admission.emplace(std::move(*admission));
  }
  std::vector<OrderedCollectionEntry> entries;
  if (value) {
    auto decoded =
        DecodeOrderedCompactValue(OrderedCollectionKind::kSortedSet,
                                  value->encoded_, value->logical_size_);
    if (!decoded.ok()) return decoded.status();
    entries = std::move(*decoded);
  }
  result.key_exists_ = value.has_value();
  result.length_ = entries.size();
  if (operation.kind_ == SortedSetOperationKind::kScan) {
    ScanBoundary boundary;
    auto prepared = boundary.Prepare(operation, entries.size());
    if (!prepared.ok()) return prepared;
    for (const auto& entry : entries)
      boundary.Observe(entry, operation.scan_cursor_);
    for (const auto& entry : entries) {
      auto emitted = boundary.Emit(entry, operation, &result);
      if (!emitted.ok()) return emitted;
    }
    ScanBoundary::Sort(&result);
    return CompactValueUpdate{};
  }
  if (operation.kind_ == SortedSetOperationKind::kRandom) {
    RandomSelection selection;
    auto status =
        selection.Prepare(entries.size(), operation.count_, true, &result);
    if (!status.ok()) return status;
    for (const auto& [rank, slot] : selection.draws_) {
      auto copied = MakeOutput(entries[rank]);
      if (!copied.ok()) return copied.status();
      result.members_[slot] = std::move(*copied);
    }
    return CompactValueUpdate{};
  }
  if (operation.kind_ == SortedSetOperationKind::kRemoveRange) {
    const auto [first, end] =
        RankSlice(operation.first_, operation.last_, entries.size());
    std::size_t rank = 0;
    result.changed_ = std::erase_if(entries, [&](const auto& entry) {
      const auto at = rank++;
      return operation.range_mode_ == SortedSetRangeMode::kRank
                 ? at >= first && at < end
                 : Matches(entry, operation);
    });
    result.length_ = entries.size();
    if (result.changed_ == 0) return CompactValueUpdate{};
    if (entries.empty()) {
      CompactValueUpdate erased;
      erased.changed_ = erased.erase_ = true;
      return erased;
    }
    auto encoded =
        EncodeOrderedCompactValue(OrderedCollectionKind::kSortedSet, entries);
    if (!encoded.ok()) return encoded.status();
    return CompactValueUpdate{
        .changed_ = true,
        .encoded_ = std::move(*encoded),
        .logical_size_ = entries.size(),
        .expire_at_ms_ = std::nullopt,
        .stream_trim_ = {},
        .stream_trim_complete_ = {},
        .stream_pending_removed_ = {},
    };
  }
  if (operation.kind_ == SortedSetOperationKind::kPop) {
    const auto count =
        std::min<std::uint64_t>(operation.pop_count_, entries.size());
    for (std::size_t i = 0; i < count; ++i) {
      const auto at = operation.reverse_ ? entries.size() - 1 - i : i;
      auto copied = MakeOutput(entries[at]);
      if (!copied.ok()) return copied.status();
      auto appended = AppendOutput(std::move(*copied), &result);
      if (!appended.ok()) return appended;
    }
    result.changed_ = count;
    result.length_ -= count;
    if (count == 0) return CompactValueUpdate{};
    if (operation.reverse_)
      entries.erase(entries.end() - count, entries.end());
    else
      entries.erase(entries.begin(), entries.begin() + count);
    if (entries.empty()) {
      CompactValueUpdate erased;
      erased.changed_ = erased.erase_ = true;
      return erased;
    }
    auto encoded =
        EncodeOrderedCompactValue(OrderedCollectionKind::kSortedSet, entries);
    if (!encoded.ok()) return encoded.status();
    return CompactValueUpdate{
        .changed_ = true,
        .encoded_ = std::move(*encoded),
        .logical_size_ = entries.size(),
        .expire_at_ms_ = std::nullopt,
        .stream_trim_ = {},
        .stream_trim_complete_ = {},
        .stream_pending_removed_ = {},
    };
  }
  if (ScanRead(operation)) {
    auto read = ReadCompact(entries, operation, &result);
    if (!read.ok()) return read;
    return CompactValueUpdate{};
  }
  for (const auto& entry : entries) {
    auto found = members.find(entry.value_);
    if (found != members.end()) {
      if (found->second.before_)
        return absl::DataLossError("duplicate persisted Sorted Set member");
      found->second.before_ = found->second.after_ = entry.score_;
    }
  }
  auto status = ApplyInputs(operation, &members, &result);
  if (!status.ok()) return status;
  if (ReadOnly(operation) || result.changed_ == 0) return CompactValueUpdate{};
  std::erase_if(entries, [&](const auto& entry) {
    const auto member = members.find(entry.value_);
    return member != members.end() && member->second.touched_;
  });
  for (const auto& [member, state] : members) {
    if (state.touched_ && state.after_)
      entries.push_back({std::string(member), *state.after_});
  }
  std::sort(entries.begin(), entries.end(), OrderedEntryLess);
  if (entries.empty()) {
    CompactValueUpdate erased;
    erased.changed_ = erased.erase_ = true;
    return erased;
  }
  auto encoded =
      EncodeOrderedCompactValue(OrderedCollectionKind::kSortedSet, entries);
  if (!encoded.ok()) return encoded.status();
  return CompactValueUpdate{
      .changed_ = true,
      .encoded_ = std::move(*encoded),
      .logical_size_ = entries.size(),
      .expire_at_ms_ = std::nullopt,
      .stream_trim_ = {},
      .stream_trim_complete_ = {},
      .stream_pending_removed_ = {},
  };
}

}  // namespace

absl::StatusOr<SortedSetResult> ReadCompactSortedSet(
    const CompactReadResult& value, const SortedSetOperation& operation) {
  if (!value.ok()) return value.status();
  const auto valid = Validate(operation);
  if (!valid.ok()) return valid;
  if (!ReadOnly(operation))
    return absl::InvalidArgumentError("compact Sorted Set read cannot mutate");
  SortedSetResult result;
  result.key_exists_ = value->has_value();
  result.length_ = *value ? (**value).logical_size_ : 0;
  if (operation.kind_ == SortedSetOperationKind::kLength) return result;
  auto input_admission = ReserveInputs(operation);
  if (!input_admission.ok()) return input_admission.status();
  Members members;
  const auto prepared = PrepareMembers(operation, &members);
  if (!prepared.ok()) return prepared;
  std::optional<MemoryReservation> compact_admission;
  const auto status = ApplyCompactRead(value, [&](auto view) {
    return ApplyCompactSortedSet(view, operation, members, result,
                                 compact_admission);
  });
  if (!status.ok()) return status;
  return result;
}

Task<absl::StatusOr<SortedSetResult>>
StorageEngine::Impl::ExecuteSortedSetLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const SortedSetOperation& operation, TxShardWrites* tx,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  const auto valid = Validate(operation);
  if (!valid.ok()) co_return valid;
  auto& store = CurrentStore();
  auto& partition = PartitionForKey(store, key);
  co_await store.store_state_mutex_.Lock();
  UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
  auto& index = partition.indexes_[db_id];
  auto* found = index.Find(digest, key);
  if (found && !found->key_complete()) {
    auto verified = co_await FindVerifiedEntry(store, index, digest, key);
    if (!verified.ok()) co_return verified.status();
    found = *verified;
  }
  const auto now =
      operation.now_ms_ == 0 ? UnixTimeMillis() : operation.now_ms_;
  const bool exists = found && found->value_.kind() == RecordKind::kValue &&
                      !IsExpired(*found, now);
  if (!exists && found && found->value_.grouped()) {
    // A failed tentative root cannot use its newer TTL to look absent. The
    // checked metadata path validates its decision before applying expiry.
    const auto readable = co_await ReadKeyMetadataLocked(db_id, key, digest);
    if (!readable.ok()) co_return readable.status();
  }
  if (exists && found->value_.value_type() != ValueType::kSortedSet)
    co_return absl::InvalidArgumentError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  SortedSetResult result;
  result.key_exists_ = exists;
  result.length_ = exists ? MaterializeIndexLocation(*found).logical_size_ : 0;
  if (exists && found->value_.grouped()) {
    auto view = partition.grouped_objects_[db_id].Lookup(
        key, GroupedObjectVersion{
                 .root_ = MaterializeIndexLocation(*found),
                 .db_epoch_ = EffectiveRecordDbEpoch(partition, db_id),
                 .replication_epoch_ = partition.replication_epoch_,
                 .index_generation_ = partition.grouped_generations_[db_id]});
    if (!view.ok()) co_return view.status();
    if (operation.kind_ == SortedSetOperationKind::kLength) co_return result;
    if (!ReadOnly(operation) && CanPrepareGroupedWriteUnlocked(partition)) {
      const auto snapshot =
          CaptureCompactWriteSnapshot(store, partition, db_id);
      PreparedOrderedMutation mutation;
      auto& plan = mutation.plan_;
      SortedSetMemberMutation members;
      found = nullptr;
      unlock.Unlock();
      LAVIK_FAULT_INJECT({
        const auto paused =
            co_await PauseGroupedWriteForTest(*store.worker_, key, "prepare");
        if (!paused.ok()) co_return paused;
      });
      auto prepared = co_await ExecuteGroupedSortedSetLocked(
          store, partition, db_id, key, digest, operation, *view, tx,
          replication, mutation_precondition, &mutation);
      if (!prepared.ok()) co_return prepared.status();
      if (plan.changed_ && !plan.delete_key_) {
        if (!mutation.members_)
          co_return absl::InternalError("missing prepared member-index plan");
        members = std::move(*mutation.members_);
      }
      co_await store.store_state_mutex_.Lock();
      unlock.Adopt();
      const auto valid = ValidateGroupedWriteSnapshot(
          store, partition, db_id, key, *view, snapshot,
          mutation_precondition != nullptr
              ? mutation_precondition
              : (tx != nullptr ? &tx->mutation_precondition_ : nullptr));
      if (!valid.ok()) co_return valid;
      if (plan.changed_) {
        const auto committed = co_await CommitGroupedOrderedMutationLocked(
            store, partition, db_id, key, digest, *view, std::move(plan),
            (*view)->version().root_.expire_at_ms_, tx, replication,
            mutation_precondition, &members);
        if (!committed.ok()) co_return committed;
      }
      co_return prepared;
    }
    // As with Hash reads, retain the shared key intent but release the store
    // mutex before a potentially long scan. Page readers revalidate this view.
    if (ReadOnly(operation)) unlock.Unlock();
    const auto version =
        (*view) ? std::optional((*view)->version()) : std::nullopt;
    auto read = co_await ExecuteGroupedSortedSetLocked(
        store, partition, db_id, key, digest, operation, std::move(*view), tx,
        replication, mutation_precondition);
    if (!read.ok() && ReadOnly(operation) && version &&
        (EffectiveRecordDbEpoch(partition, db_id) != version->db_epoch_ ||
         partition.replication_epoch_ != version->replication_epoch_ ||
         partition.grouped_generations_[db_id] != version->index_generation_)) {
      SortedSetResult missing;
      {
        if (operation.kind_ == SortedSetOperationKind::kScores) {
          auto allocated =
              PrepareScoreReply(operation.members_.size(), &missing);
          if (!allocated.ok()) co_return allocated;
        }
      }
      co_return missing;
    }
    co_return read;
  }
  if (operation.kind_ == SortedSetOperationKind::kLength) co_return result;
  // The key hold remains owned by the caller across this handoff. Reuse the
  // compact pipeline for a compact/missing value, never for a grouped graph.
  unlock.Unlock();
  auto input_admission = ReserveInputs(operation);
  if (!input_admission.ok()) co_return input_admission.status();
  Members members;
  auto prepared = PrepareMembers(operation, &members);
  if (!prepared.ok()) co_return prepared;
  // The encoded callback result survives until the compact writer returns.
  // Keep its admission outside the synchronous callback's stack frame.
  std::optional<MemoryReservation> compact_admission;
  auto callback = [&](std::optional<CompactValueView> value)
      -> absl::StatusOr<CompactValueUpdate> {
    return ApplyCompactSortedSet(value, operation, members, result,
                                 compact_admission);
  };
  // kAdd also implements ZINCRBY and GEOADD, so the operation kind alone is
  // insufficient: only explicit single-key command admission opts in.
  // Pop, GEOADD and legacy STORE retain their existing state-lock boundary
  // even when their caller has no durable transaction accumulator.
  const bool prepare_unlocked =
      operation.prepare_unlocked_ &&
      (operation.kind_ == SortedSetOperationKind::kAdd ||
       operation.kind_ == SortedSetOperationKind::kRemove);
  const auto status = co_await ExecuteCompactLocked(
      db_id, key, digest, ValueType::kSortedSet, ReadOnly(operation), callback,
      tx, now, replication, prepare_unlocked, mutation_precondition);
  if (!status.ok()) co_return status;
  co_return result;
}

Task<absl::StatusOr<SortedSetResult>>
StorageEngine::Impl::ExecuteGroupedSortedSetLocked(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const SortedSetOperation& operation, GroupedObject::Handle object,
    TxShardWrites* tx, ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition,
    PreparedOrderedMutation* prepared, SortedSetOrderedProbe* ordered_probe) {
  if (!object || !object->is_ordered() ||
      object->ordered_directory().root().kind_ !=
          OrderedCollectionKind::kSortedSet)
    co_return absl::DataLossError("invalid grouped Sorted Set view");
  auto input_admission = ReserveInputs(operation);
  if (!input_admission.ok()) co_return input_admission.status();
  Members members;
  auto status = PrepareMembers(operation, &members);
  if (!status.ok()) co_return status;
  const auto& directory = object->ordered_directory();
  const auto& metadata = directory.groups();
  SortedSetResult result;
  result.key_exists_ = true;
  result.length_ = directory.root().item_count_;
  if (operation.kind_ == SortedSetOperationKind::kLength) co_return result;

  auto add_page_budget = [&](GroupedScratchBudget* budget, std::size_t i) {
    // A retained directory protects logical routing, not the allocation of
    // its old physical records. Earlier page I/O or mutation checkpoints may
    // let GC relocate and reclaim them. Resolve a current physical view
    // without suspending before inspecting its immutable size fields for
    // admission; the loader refreshes again before resolving physical IO.
    const auto& expected = object->version();
    auto current = partition.grouped_objects_[db_id].CurrentForMutation(key);
    if (!current ||
        EffectiveRecordDbEpoch(partition, db_id) != expected.db_epoch_ ||
        partition.replication_epoch_ != expected.replication_epoch_ ||
        partition.grouped_generations_[db_id] != expected.index_generation_)
      return absl::NotFoundError(
          "Sorted Set population changed before admission");
    const auto& version = current->version();
    if (version.db_epoch_ != expected.db_epoch_ ||
        version.replication_epoch_ != expected.replication_epoch_ ||
        version.index_generation_ != expected.index_generation_ ||
        version.root_.mutation_sequence_ != expected.root_.mutation_sequence_ ||
        version.root_.logical_size_ != expected.root_.logical_size_ ||
        version.root_.expire_at_ms_ != expected.root_.expire_at_ms_ ||
        version.root_.value_type() != expected.root_.value_type() ||
        !object->SameLogicalRoot(*current))
      return absl::NotFoundError(
          "Sorted Set logical view changed before admission");
    const auto readable = current->ReadStatus();
    if (!readable.ok()) return readable;
    const auto* entry = current->FindGroup({metadata[i].id_, 0});
    if (!entry) return absl::DataLossError("missing Sorted Set physical page");
    return budget->AddGroup(*entry, current->ExtentsFor({metadata[i].id_, 0}));
  };
  // A serial scan retains one physical read lease and borrowed member views.
  // Keep scratch admitted until both disappear; replies own separate copies.
  SortedSetOrderedProbe local_probe{.source_ = object, .pages_ = {}};
  if (ordered_probe == nullptr) ordered_probe = &local_probe;
  if (ordered_probe->source_.get() != object.get())
    co_return absl::DataLossError("Sorted Set probe logical view mismatch");
  auto& probed_pages = ordered_probe->pages_;
  using OwnedScanPage = SortedSetOrderedProbe::Page;
  auto retain_probe = [&](std::size_t i, OwnedScanPage page) {
    // Reuse is optional, not a batch-sized payload cache. Bound admitted
    // retention (including decoded entry overhead), so large members and
    // wide pops keep the ordinary one-page scan/fallback under maxmemory.
    constexpr std::size_t kProbeBytes = 64 * 1024;
    std::size_t bytes = page.admission_.bytes();
    if (bytes > kProbeBytes) return;
    for (const auto& [index, retained] : probed_pages) {
      if (retained.admission_.bytes() > kProbeBytes - bytes) return;
      bytes += retained.admission_.bytes();
    }
    probed_pages.emplace(i, std::move(page));
  };
  SortedSetPageWorkBudget page_work;
  auto read_page_impl = [&]<typename Page>(std::size_t i,
                                           GroupReadLimiter* limiter = nullptr)
      -> Task<absl::StatusOr<
          std::conditional_t<std::is_same_v<Page, LoadedOrderedGroup>,
                             OwnedScanPage, AdmittedScanPage<Page>>>> {
    using ResultPage =
        std::conditional_t<std::is_same_v<Page, LoadedOrderedGroup>,
                           OwnedScanPage, AdmittedScanPage<Page>>;
    if constexpr (std::is_same_v<Page, LoadedOrderedGroup>) {
      if (auto found = probed_pages.find(i); found != probed_pages.end()) {
        // Owned payloads survive GC relocation, but not logical replacement.
        GroupedScratchBudget budget;
        auto valid = add_page_budget(&budget, i);
        if (!valid.ok()) co_return valid;
        auto page = std::move(found->second);
        probed_pages.erase(found);
        co_return page;
      }
    }
    LAVIK_FAULT_INJECT(
        // An optional one-based directory page isolates routing tests from
        // the existing fail-all-ordered-reads member-index test.
        if (LAVIK_FAULT_MATCHES("LAVIK_FAIL_ZSET_ORDERED_READ_KEY", key) &&
            (std::getenv("LAVIK_FAIL_ZSET_ORDERED_READ_PAGE") == nullptr ||
             LAVIK_FAULT_MATCHES_NTH("LAVIK_FAIL_ZSET_ORDERED_READ_KEY", key,
                                     "LAVIK_FAIL_ZSET_ORDERED_READ_PAGE",
                                     i + 1))) co_return absl::
            UnavailableError("injected ordered-page read failure"););
    GroupedScratchBudget budget;
    auto checked = add_page_budget(&budget, i);
    if (!checked.ok()) co_return checked;
    if (ReadOnly(operation) || prepared != nullptr) {
      // Read-only scans rely on I/O suspension or queued window children for
      // scheduling. Staging reads may complete synchronously; only unlocked
      // mutation preparation adds explicit work checkpoints.
      if (prepared != nullptr && page_work.Add(budget.bytes()))
        co_await bycorf::Yield(*store.worker_);
      if (shutdown_flush_requested_)
        co_return absl::CancelledError("Sorted Set read cancelled by shutdown");
    }
    MemoryReservation admission;
    // A parallel wave admits all owned pages before launching children. Its
    // reservation outlives the returned pages; serial probes own theirs here.
    if (limiter == nullptr) {
      auto reserved = budget.Reserve(2);
      if (!reserved.ok()) co_return reserved.status();
      admission = std::move(*reserved);
    }
    auto load = [&] {
      const auto validation = ReadOnly(operation)
                                  ? SortedSetMemberValidation::kTrustStored
                                  : SortedSetMemberValidation::kVerifyUnique;
      if constexpr (std::is_same_v<Page, LoadedSortedSetPage>)
        return LoadSortedSetPage(store, partition, db_id, key, digest, object,
                                 metadata[i].id_, validation);
      else if constexpr (std::is_same_v<Page, PackedSortedSetValues>)
        return LoadPackedSortedSetPage(store, partition, db_id, key, digest,
                                       object, metadata[i].id_, limiter);
      else
        return LoadOrderedGroupSnapshot(store, partition, db_id, key, digest,
                                        object, metadata[i].id_, false, limiter,
                                        validation);
    };
    auto page = co_await load();
    if (!page.ok()) co_return page.status();
    co_return ResultPage{std::move(admission), std::move(*page)};
  };
  auto read_page = [&](std::size_t i) {
    return read_page_impl.template operator()<LoadedSortedSetPage>(i);
  };
  // Mutation probes own their entries, releasing physical leases before
  // routing. Both wrappers return the loader task without another coroutine.
  auto read_owned_page = [&](std::size_t i) {
    return read_page_impl.template operator()<LoadedOrderedGroup>(i);
  };
  auto remove_selected = [&]() -> Task<absl::Status> {
    const auto count = result.members_.size();
    if (count == 0) co_return absl::OkStatus();
    if (count >
        std::numeric_limits<std::size_t>::max() / sizeof(std::string_view))
      co_return absl::ResourceExhaustedError(
          "Sorted Set selected removal count overflow");
    auto admission = TryReserveMemory(count * sizeof(std::string_view));
    if (!admission) {
      RecordMemoryRejection();
      co_return absl::ResourceExhaustedError(
          "OOM Sorted Set selected removal input admission");
    }
    std::vector<std::string_view> removed;
    removed.reserve(count);
    for (const auto& member : result.members_)
      removed.push_back(member.member_);
    // Selection and deletion share the caller's exclusive key intent. The
    // same immutable logical view is revalidated by every page load and the
    // common writer; all reply allocation finishes before any mutation.
    auto deleted = co_await ExecuteGroupedSortedSetLocked(
        store, partition, db_id, key, digest,
        SortedSetOperation{.kind_ = SortedSetOperationKind::kRemove,
                           .members_ = removed},
        object, tx, replication, mutation_precondition, prepared,
        ordered_probe);
    if (!deleted.ok()) co_return deleted.status();
    result.changed_ = deleted->changed_;
    result.length_ = deleted->length_;
    co_return absl::OkStatus();
  };
  if (operation.kind_ == SortedSetOperationKind::kRandom) {
    RandomSelection selection;
    auto selected =
        selection.Prepare(result.length_, operation.count_, false, &result);
    if (!selected.ok()) co_return selected;
    std::size_t draw = 0;
    while (draw < selection.draws_.size()) {
      const auto position = directory.FindRank(selection.draws_[draw].first);
      if (!position)
        co_return absl::DataLossError("Sorted Set random rank missing");
      const auto i = position->group_index_;
      auto page = co_await read_page(i);
      if (!page.ok()) co_return page.status();
      const auto& entries = page->page_.entries_;
      do {
        const auto [rank, slot] = selection.draws_[draw];
        const auto at = directory.FindRank(rank);
        if (!at || at->group_index_ != i) break;
        if (at->offset_ >= entries.size())
          co_return absl::DataLossError(
              "Sorted Set random page cardinality mismatch");
        auto copied = MakeOutput(entries[at->offset_]);
        if (!copied.ok()) co_return copied.status();
        result.members_[slot] = std::move(*copied);
        ++draw;
      } while (draw < selection.draws_.size());
    }
    co_return result;
  }
  if (operation.kind_ == SortedSetOperationKind::kScan) {
    ScanBoundary boundary;
    auto prepared = boundary.Prepare(operation, result.length_);
    if (!prepared.ok()) co_return prepared;
    for (std::size_t i = 0; i < metadata.size(); ++i) {
      auto page = co_await read_page(i);
      if (!page.ok()) co_return page.status();
      for (const auto& entry : page->page_.entries_)
        boundary.Observe(entry, operation.scan_cursor_);
    }
    if (boundary.prefixes_.empty()) co_return result;
    for (std::size_t i = 0; i < metadata.size(); ++i) {
      auto page = co_await read_page(i);
      if (!page.ok()) co_return page.status();
      for (const auto& entry : page->page_.entries_) {
        auto emitted = boundary.Emit(entry, operation, &result);
        if (!emitted.ok()) co_return emitted;
      }
    }
    ScanBoundary::Sort(&result);
    co_return result;
  }
  if (operation.kind_ == SortedSetOperationKind::kPop) {
    const auto count = std::min(operation.pop_count_, result.length_);
    if (count == 0) co_return result;
    for (std::size_t ordinal = 0;
         result.members_.size() < count && ordinal < metadata.size();
         ++ordinal) {
      const auto i =
          operation.reverse_ ? metadata.size() - 1 - ordinal : ordinal;
      auto page = co_await read_owned_page(i);
      if (!page.ok()) co_return page.status();
      const auto& entries = page->page_.snapshot_.entries_;
      for (std::size_t j = 0;
           result.members_.size() < count && j < entries.size(); ++j) {
        const auto at = operation.reverse_ ? entries.size() - 1 - j : j;
        auto copied = MakeOutput(entries[at]);
        if (!copied.ok()) co_return copied.status();
        auto appended = AppendOutput(std::move(*copied), &result);
        if (!appended.ok()) co_return appended;
      }
      retain_probe(i, std::move(*page));
    }
    if (result.members_.size() != count)
      co_return absl::DataLossError("Sorted Set pop cardinality mismatch");
    auto deleted = co_await remove_selected();
    if (!deleted.ok()) co_return deleted;
    co_return result;
  }
  if (ScanRead(operation) && operation.kind_ != SortedSetOperationKind::kRank) {
    ReadCursor cursor(operation, &result);
    if (cursor.done_) co_return result;
    if (operation.kind_ == SortedSetOperationKind::kRange &&
        operation.range_mode_ == SortedSetRangeMode::kLex) {
      // Physical pages are score-ordered; mixed-score BYLEX historically
      // sorts members independently. Without a resident member index, select
      // one next lexical member per pass: O(N * (offset + output)) work, but
      // only one page, one candidate/boundary, and the admitted reply
      // survive. Pages revalidate epochs without retaining the store mutex
      // across passes; only asynchronous reads suspend the serial scan.
      std::optional<SortedSetMember> skipped;
      std::uint64_t offset = operation.limit_ ? operation.offset_ : 0;
      for (;;) {
        const std::optional<std::string_view> after =
            result.member_count() != 0
                ? std::optional<std::string_view>(
                      result.member_view(result.member_count() - 1).member_)
            : skipped ? std::optional<std::string_view>(skipped->member_)
                      : std::nullopt;
        std::optional<SortedSetMember> candidate;
        for (std::size_t i = 0; i < metadata.size(); ++i) {
          auto page = co_await read_page(i);
          if (!page.ok()) co_return page.status();
          for (const auto& entry : page->page_.entries_) {
            if (!BetterLexCandidate(entry, operation, after, candidate))
              continue;
            auto copied = MakeOutput(entry);
            if (!copied.ok()) co_return copied.status();
            candidate.emplace(std::move(*copied));
          }
        }
        if (!candidate) break;
        if (offset != 0) {
          skipped.emplace(std::move(*candidate));
          --offset;
          continue;
        }
        skipped.reset();
        auto appended =
            AppendRangeOutput(std::move(*candidate), operation, &result);
        if (!appended.ok()) co_return appended;
        if (operation.limit_ && operation.count_ >= 0 &&
            result.member_count() >=
                static_cast<std::uint64_t>(operation.count_))
          break;
      }
      co_return result;
    }
    std::size_t first_page = 0, end_page = metadata.size();
    if ((operation.kind_ == SortedSetOperationKind::kRange ||
         operation.kind_ == SortedSetOperationKind::kRemoveRange) &&
        operation.range_mode_ == SortedSetRangeMode::kRank) {
      const auto begin_rank =
          operation.reverse_ ? result.length_ - cursor.end_ : cursor.first_;
      const auto end_rank =
          operation.reverse_ ? result.length_ - cursor.first_ : cursor.end_;
      first_page = directory.FindRank(begin_rank)->group_index_;
      end_page = directory.FindRank(end_rank - 1)->group_index_ + 1;
    } else if (operation.range_mode_ == SortedSetRangeMode::kScore) {
      first_page = directory.LowerBoundScore(
          operation.minimum_score_.value_, operation.minimum_score_.exclusive_);
      end_page = directory.UpperBoundScore(operation.maximum_score_.value_,
                                           operation.maximum_score_.exclusive_);
      if (first_page >= end_page) co_return result;
    }
    const bool reverse = operation.reverse_;
    std::uint64_t rank = directory.CountBefore(reverse ? end_page : first_page);
    auto page_index = [&](std::size_t ordinal) {
      return reverse ? end_page - 1 - ordinal : first_page + ordinal;
    };
    auto consume_page = [&](auto& entries, bool prepaid) {
      if (reverse) rank -= entries.size();
      for (std::size_t j = 0; !cursor.done_ && j < entries.size(); ++j) {
        const auto index = reverse ? entries.size() - 1 - j : j;
        // A prepaid wave owns decoded strings after releasing its read leases.
        // Transfer selected members into the reply instead of allocating and
        // copying them again; Visit filters before consuming the entry. The
        // serial path still copies its borrowed page views.
        auto visited = prepaid ? cursor.Visit(std::move(entries[index]),
                                              rank + index, true)
                               : cursor.Visit(entries[index], rank + index);
        if (!visited.ok()) return visited;
      }
      if (!reverse) rank += entries.size();
      return absl::OkStatus();
    };
    auto consume_packed_page = [&](PackedSortedSetValues& page, bool prepaid) {
      const auto size = page.scores_.size();
      if (page.members_.blocks_.size() != 1 ||
          page.members_.values_.size() != size)
        return absl::DataLossError("Sorted Set packed page count mismatch");
      if (cursor.done_) return absl::OkStatus();
      auto maximum_output = size;
      if (operation.range_mode_ == SortedSetRangeMode::kRank)
        maximum_output = std::min<std::uint64_t>(
            maximum_output,
            cursor.end_ - cursor.first_ - result.member_count());
      if (operation.limit_ && operation.count_ >= 0)
        maximum_output = std::min<std::uint64_t>(
            maximum_output, static_cast<std::uint64_t>(operation.count_) -
                                result.member_count());
      std::optional<std::size_t> block_index;
      if (reverse) rank -= size;
      for (std::size_t j = 0; !cursor.done_ && j < size; ++j) {
        const auto index = reverse ? size - 1 - j : j;
        auto slice = page.members_.values_[index];
        const auto& block =
            block_index ? result.packed_members_->members_.blocks_[*block_index]
                        : page.members_.blocks_.front();
        const OrderedCollectionEntryView entry{
            std::string_view(block).substr(slice.offset_, slice.size_),
            page.scores_[index]};
        auto visited = cursor.VisitWithOutput(entry, rank + index, [&] {
          if (!block_index) {
            // Empty score-boundary pages need no reply allocation. Reserve
            // only after the cursor has selected the first actual member.
            auto reserved =
                ReservePackedOutput(&result, maximum_output, 1, prepaid);
            if (!reserved.ok()) return reserved;
            auto& output = *result.packed_members_;
            const auto bytes = page.members_.blocks_.front().capacity() + 1;
            if (bytes > SIZE_MAX - result.retained_charge_.bytes())
              return absl::ResourceExhaustedError(
                  "Sorted Set packed output overflow");
            block_index = output.members_.blocks_.size();
            output.members_.blocks_.push_back(
                std::move(page.members_.blocks_.front()));
            // The page/wave reservation remains live until this transfer
            // finishes. Only the selected page block outlives that admission.
            result.retained_charge_.Account(
                CurrentMemoryAccountingShard(),
                result.retained_charge_.bytes() + bytes);
          }
          auto& output = *result.packed_members_;
          slice.block_ = *block_index;
          output.members_.values_.push_back(slice);
          output.scores_.push_back(entry.score_);
          return absl::OkStatus();
        });
        if (!visited.ok()) return visited;
      }
      if (!reverse) rank += size;
      return absl::OkStatus();
    };
    auto read_wave =
        [&]<typename Page>(
            std::size_t ordinal) -> Task<absl::StatusOr<std::size_t>> {
      constexpr bool packed = std::is_same_v<Page, PackedSortedSetValues>;
      using ScanPage =
          std::conditional_t<packed, AdmittedScanPage<Page>, OwnedScanPage>;
      const auto capacity =
          std::min<std::size_t>(end_page - first_page - ordinal,
                                worker_count_ * GroupReadLimiter::kWidth);
      auto plan = GroupReadPlan::TryCreate(
          worker_count_, capacity,
          worker_count_ * sizeof(std::size_t) +
              capacity * sizeof(std::optional<ScanPage>));
      if (!plan) co_return 0;
      std::vector<std::size_t> owner_counts(worker_count_);
      GroupedScratchBudget budget;
      std::uint64_t items = 0;
      std::size_t count = 0;
      // Keep a contiguous logical wave, even though its reads launch in owner
      // order. A LIMIT can finish inside a boundary page whose matching count
      // is unknown; never prefetch beyond that page and expose errors from
      // pages the serial cursor would not have visited.
      for (; count < capacity; ++count) {
        const auto i = page_index(ordinal + count);
        const auto* entry = object->FindGroup({metadata[i].id_, 0});
        if (!entry)
          co_return absl::DataLossError("missing Sorted Set physical page");
        auto owner = BlockOwner(entry->block_id());
        if (owner >= worker_count_) owner = store.worker_->id();
        if (owner_counts[owner] == GroupReadLimiter::kWidth) break;
        auto checked = add_page_budget(&budget, i);
        if (!checked.ok()) co_return checked;
        ++owner_counts[owner];
        plan->Add(owner, count);
        items += metadata[i].item_count_;
        if (operation.range_mode_ == SortedSetRangeMode::kScore &&
            operation.limit_ && operation.count_ >= 0) {
          const auto low = operation.minimum_score_;
          const auto high = operation.maximum_score_;
          const bool whole_page =
              (low.exclusive_ ? metadata[i].min_score_ > low.value_
                              : metadata[i].min_score_ >= low.value_) &&
              (high.exclusive_ ? metadata[i].max_score_ < high.value_
                               : metadata[i].max_score_ <= high.value_);
          const auto needed = static_cast<std::uint64_t>(operation.offset_) -
                              cursor.skipped_ +
                              static_cast<std::uint64_t>(operation.count_) -
                              result.member_count();
          if (!whole_page || items >= needed) {
            ++count;
            break;
          }
        }
      }
      if (count <= 1 || !plan->ReserveWindow<ScanPage>(4096)) co_return 0;
      // Admit encoded/decoded page scratch and output vector growth before
      // launching. Selected strings move into the reply, so no third payload
      // copy needs headroom. Prepay output ownership too: a window reservation
      // must not starve later per-member admission. A small LIMIT admits only
      // this wave, not the entire remaining score range.
      constexpr auto limit = std::numeric_limits<std::size_t>::max();
      if (items > limit - result.member_count()) co_return 0;
      const auto maximum_output = result.member_count() + items;
      const auto previous_capacity =
          packed ? (result.packed_members_
                        ? result.packed_members_->members_.values_.capacity()
                        : 0)
                 : result.members_.capacity();
      auto output_capacity = previous_capacity;
      while (output_capacity < maximum_output) {
        if (output_capacity > limit / 2) co_return 0;
        output_capacity = std::max<std::size_t>(8, output_capacity * 2);
      }
      constexpr auto slot_bytes =
          packed ? sizeof(PackedValues::Slice) + sizeof(double)
                 : sizeof(SortedSetMember);
      if (output_capacity > limit / slot_bytes) co_return 0;
      auto vector_bytes = output_capacity == previous_capacity
                              ? 0
                              : output_capacity * slot_bytes;
      if constexpr (packed) {
        const auto* output = result.packed_members_
                                 ? &result.packed_members_->members_
                                 : nullptr;
        const auto old_blocks = output ? output->blocks_.capacity() : 0;
        const auto size = output ? output->blocks_.size() : 0;
        if (count > limit - size) co_return 0;
        auto blocks = old_blocks;
        while (blocks < size + count) {
          if (blocks > limit / 2) co_return 0;
          blocks = std::max<std::size_t>(8, blocks * 2);
        }
        if (blocks > (limit - vector_bytes) / sizeof(std::string)) co_return 0;
        if (blocks != old_blocks) vector_bytes += blocks * sizeof(std::string);
      }
      if (budget.bytes() > (limit - vector_bytes) / 2) co_return 0;
      auto admission = TryReserveMemory(budget.bytes() * 2 + vector_bytes);
      if (!admission) co_return 0;
      std::vector<std::optional<ScanPage>> pages(count);
      GroupReadWindow<ScanPage> window(store.worker_, worker_count_, count);
      absl::Status read_status;
      for (unsigned owner = 0; owner < plan->owner_count(); ++owner) {
        while (const auto selected = plan->Pop(owner)) {
          if (window.size() == 1 &&
              LAVIK_FAULT_MATCHES("LAVIK_FAIL_ZSET_READ_BATCH_START_KEY",
                                  key)) {
            read_status = absl::ResourceExhaustedError(
                "OOM grouped Sorted Set read batch admission");
            break;
          }
          window.Add(read_page_impl.template operator()<Page>(
                         page_index(ordinal + *selected), window.limiter()),
                     *selected);
        }
        if (!read_status.ok()) break;
      }
      co_await window.Join();
      // The last completion only queues the parent. Population replacement,
      // shutdown or invalidation can occur before it resumes; validate again
      // after joining every child, including a partially launched wave.
      if (shutdown_flush_requested_)
        co_return absl::CancelledError("Sorted Set read cancelled by shutdown");
      GroupedScratchBudget current;
      auto checked = add_page_budget(&current, page_index(ordinal));
      if (!checked.ok()) co_return checked;
      const auto completed =
          window.Collect([&](std::size_t selected, ScanPage&& page) {
            pages[selected].emplace(std::move(page));
            return absl::OkStatus();
          });
      if (!read_status.ok()) co_return read_status;
      if (!completed.ok()) co_return completed;
      // No suspension while transferring prepaid output to retained charges.
      // The enclosing reservation remains live until all owned pages are
      // destroyed, and is released before the next wave can admit anything.
      for (auto& page : pages) {
        const auto visited = [&] {
          if constexpr (packed)
            return consume_packed_page(page->page_, true);
          else
            return consume_page(page->page_.snapshot_.entries_, true);
        }();
        if (!visited.ok()) co_return visited;
        page.reset();
      }
      co_return count;
    };
    for (std::size_t ordinal = 0;
         !cursor.done_ && ordinal < end_page - first_page;) {
      // Mutating range selection and the other scan modes retain their
      // original borrowed-page lifecycle and memory floor.
      if (operation.kind_ == SortedSetOperationKind::kRange &&
          (operation.range_mode_ == SortedSetRangeMode::kRank ||
           operation.range_mode_ == SortedSetRangeMode::kScore) &&
          end_page - first_page - ordinal > 1) {
        absl::StatusOr<std::size_t> consumed;
        if (operation.packed_output_)
          consumed =
              co_await read_wave.template operator()<PackedSortedSetValues>(
                  ordinal);
        else
          consumed = co_await read_wave.template operator()<LoadedOrderedGroup>(
              ordinal);
        if (!consumed.ok()) co_return consumed.status();
        if (*consumed != 0) {
          ordinal += *consumed;
          continue;
        }
      }
      if (operation.packed_output_ &&
          operation.kind_ == SortedSetOperationKind::kRange) {
        auto page =
            co_await read_page_impl.template operator()<PackedSortedSetValues>(
                page_index(ordinal));
        if (!page.ok()) co_return page.status();
        auto visited = consume_packed_page(page->page_, false);
        if (!visited.ok()) co_return visited;
      } else {
        auto page = co_await read_page(page_index(ordinal));
        if (!page.ok()) co_return page.status();
        auto visited = consume_page(page->page_.entries_, false);
        if (!visited.ok()) co_return visited;
      }
      ++ordinal;
    }
    if (operation.kind_ == SortedSetOperationKind::kRemoveRange) {
      auto deleted = co_await remove_selected();
      if (!deleted.ok()) co_return deleted;
      // Names are needed only through selection and atomic writer
      // preparation.
      result.members_.clear();
    }
    co_return result;
  }
  std::size_t remaining_sources = 0;
  std::optional<SortedSetMemberProbe> member_probe;
  {
    // Prefix routing retains only per-group metadata. Exact members and
    // scores are decoded from the selected Hash leaves, never trusted from
    // a digest alone. Batch requests read each selected leaf just once.
    std::set<GroupedRecordId> selected;
    for (const auto& [member, state] : members) {
      const auto* route = object->directory().Find(member);
      if (!route) co_return absl::DataLossError("missing member prefix route");
      selected.insert(route->id_);
    }
    for (const auto id : selected) {
      const auto current =
          partition.grouped_objects_[db_id].CurrentForMutation(key);
      if (!current || !current->SameLogicalRoot(*object) ||
          current->version().db_epoch_ != object->version().db_epoch_ ||
          current->version().replication_epoch_ !=
              object->version().replication_epoch_ ||
          current->version().index_generation_ !=
              object->version().index_generation_)
        co_return absl::NotFoundError("member-index population changed");
      const auto* physical = current->FindGroup(id);
      if (!physical)
        co_return absl::DataLossError("missing member prefix page");
      GroupedScratchBudget budget;
      auto checked = budget.AddGroup(*physical, current->ExtentsFor(id));
      if (!checked.ok()) co_return checked;
      if (ReadOnly(operation) || prepared != nullptr) {
        // Only unlocked mutation preparation adds an explicit checkpoint.
        // The loader refreshes the physical location and logical view after
        // any suspension; read-only lookups rely on their I/O waits.
        if (prepared != nullptr && page_work.Add(budget.bytes()))
          co_await bycorf::Yield(*store.worker_);
        if (shutdown_flush_requested_)
          co_return absl::CancelledError("member read cancelled by shutdown");
      }
      auto admission = budget.Reserve(2);
      if (!admission.ok()) co_return admission.status();
      auto retain_score = [&](const auto& entry) -> absl::Status {
        const auto requested = members.find(entry.field_);
        if (requested == members.end()) return absl::OkStatus();
        auto score = DecodeSortedSetMemberScore(entry.value_);
        if (!score.ok()) return score.status();
        requested->second.before_ = requested->second.after_ = *score;
        ++remaining_sources;
        return absl::OkStatus();
      };
      if (operation.kind_ == SortedSetOperationKind::kScores ||
          operation.kind_ == SortedSetOperationKind::kRank) {
        auto leaf = co_await LoadHashGroupPayload(store, partition, db_id, key,
                                                  digest, object, id);
        if (!leaf.ok()) co_return leaf.status();
        const auto bytes = leaf->loaded_.value();
        const std::string_view payload(
            reinterpret_cast<const char*>(bytes.data()), bytes.size());
        // This read only extracts requested scores from a published leaf.
        // Its writer already checked field uniqueness and routing; retain the
        // lease and validate framing without rehashing unrelated members.
        status = VisitPersistedHashGroupFields(payload, leaf->field_count_,
                                               retain_score);
        if (!status.ok()) co_return status;
        continue;
      }
      HashGroupSnapshot snapshot;
      if (!ReadOnly(operation) && members.size() == 1) {
        auto leaf = co_await LoadHashGroupPayload(store, partition, db_id, key,
                                                  digest, object, id);
        if (!leaf.ok()) co_return leaf.status();
        const auto bytes = leaf->loaded_.value();
        const std::string_view payload(
            reinterpret_cast<const char*>(bytes.data()), bytes.size());
        auto point = HashGroupPointProbe::Create(
            payload, object->directory().root().seed_, members.begin()->first);
        if (!point.ok()) co_return point.status();
        if (*point) {
          status = retain_score(
              HashEntryView{members.begin()->first, (**point).value()});
          if (!status.ok()) co_return status;
          // Copy only one bounded encoded leaf. Its offsets survive moves,
          // and releasing the read lease here avoids holding pool buffers
          // while ordered-page preparation performs more asynchronous reads.
          member_probe.emplace(std::move(*admission), object,
                               HashGroupSnapshot{}, std::move(*point));
          continue;
        }
        LAVIK_FAULT_INJECT(
            if (LAVIK_FAULT_MATCHES("LAVIK_FAIL_ZSET_MEMBER_DECODE_KEY",
                                    key)) co_return absl::
                UnavailableError(
                    "injected member-index owned decode failure"););
        auto decoded = DecodeHashGroup(payload);
        if (!decoded.ok()) co_return decoded.status();
        snapshot = std::move(*decoded);
        for (const auto& entry : snapshot.value_.entries_) {
          if (!id.ContainsHash(
                  ComputeDigest(entry.field_, object->directory().root().seed_)
                      .value_))
            co_return absl::DataLossError("Hash field outside its group route");
        }
      } else {
        LAVIK_FAULT_INJECT(
            if (LAVIK_FAULT_MATCHES("LAVIK_FAIL_ZSET_MEMBER_DECODE_KEY",
                                    key)) co_return absl::
                UnavailableError(
                    "injected member-index owned decode failure"););
        auto leaf = co_await LoadHashGroupSnapshot(store, partition, db_id, key,
                                                   digest, object, id);
        if (!leaf.ok()) co_return leaf.status();
        snapshot = std::move(leaf->snapshot_);
      }
      for (const auto& entry : snapshot.value_.entries_) {
        status = retain_score(entry);
        if (!status.ok()) co_return status;
      }
      // Point writes need this same leaf again to replace its member score.
      // Retain only one admitted leaf, never a batch-sized payload cache.
      // The exclusive key intent and final snapshot validation protect the
      // logical contents across GC relocation and unlocked preparation.
      if (!ReadOnly(operation) && members.size() == 1)
        member_probe.emplace(std::move(*admission), object, std::move(snapshot),
                             std::nullopt);
    }
    if (operation.kind_ == SortedSetOperationKind::kScores) {
      status = ApplyInputs(operation, &members, &result);
      if (!status.ok()) co_return status;
      co_return result;
    }
  }
  if (operation.kind_ == SortedSetOperationKind::kRank) {
    const auto member = operation.members_.front();
    const auto score = members.at(member).before_;
    if (!score) co_return result;
    // The member index supplies the score, not a page identity. Equal-score
    // runs may span pages, so compare exact members within that interval;
    // Fenwick supplies the skipped prefix without loading unrelated pages.
    const auto first = directory.LowerBoundScore(*score);
    const auto end = directory.UpperBoundScore(*score);
    for (std::size_t i = first; i < end; ++i) {
      auto page = co_await read_page(i);
      if (!page.ok()) co_return page.status();
      const auto& entries = page->page_.entries_;
      for (std::size_t j = 0; j < entries.size(); ++j) {
        if (entries[j].value_ != member) continue;
        if (entries[j].score_ != *score)
          co_return absl::DataLossError("Sorted Set member score mismatch");
        const auto rank = directory.CountBefore(i) + j;
        result.rank_ = operation.reverse_ ? result.length_ - 1 - rank : rank;
        result.rank_score_ = *score;
        co_return result;
      }
    }
    // A member found in the hash graph must also exist in the ordered graph
    // under the same retained logical root; absence is corruption, not nil.
    co_return absl::DataLossError("Sorted Set indexed member missing");
  }
  // Pop already knows the exact source pages. Check the member index above
  // against their owned entries, then omit those members from score routing.
  // New point-write probes follow the same path on later planner reads.
  auto locate_sources = [&](std::size_t i,
                            const auto& entries) -> absl::StatusOr<bool> {
    bool matched = false;
    for (const auto& entry : entries) {
      auto member = members.find(entry.value_);
      if (member == members.end()) continue;
      auto& state = member->second;
      if (state.source_ != kNoPage)
        return absl::DataLossError("duplicate persisted Sorted Set member");
      if (!state.before_ || *state.before_ != entry.score_)
        return absl::DataLossError("member-index/ordered score mismatch");
      state.source_ = i;
      --remaining_sources;
      matched = true;
    }
    return matched;
  };
  for (const auto& [i, page] : probed_pages) {
    GroupedScratchBudget budget;
    status = add_page_budget(&budget, i);
    if (!status.ok()) co_return status;
    auto located = locate_sources(i, page.page_.snapshot_.entries_);
    if (!located.ok()) co_return located.status();
  }
  // The two resident doubles bound old-score candidates without reading
  // unrelated pages. Equal-score runs still scan for exact members. Sort
  // and merge requested intervals so a batch reads an overlapping page only
  // once; interval storage is covered by the per-input reservation.
  std::vector<std::pair<std::size_t, std::size_t>> source_ranges;
  source_ranges.reserve(members.size());
  for (const auto& [member, state] : members) {
    if (!state.before_ || state.source_ != kNoPage) continue;
    const auto first = directory.LowerBoundScore(*state.before_);
    const auto end = directory.UpperBoundScore(*state.before_);
    if (first >= end)
      co_return absl::DataLossError("member score lies outside ordered pages");
    source_ranges.emplace_back(first, end);
  }
  std::sort(source_ranges.begin(), source_ranges.end());
  std::size_t scanned_end = 0;
  for (const auto& [first, end] : source_ranges) {
    for (std::size_t i = std::max(first, scanned_end);
         i < end && remaining_sources != 0; ++i) {
      if (probed_pages.contains(i)) continue;
      auto page = co_await read_owned_page(i);
      if (!page.ok()) co_return page.status();
      auto located = locate_sources(i, page->page_.snapshot_.entries_);
      if (!located.ok()) co_return located.status();
      // Equal-score scans may touch unrelated pages. Retain only pages with
      // requested members, so a point update never caches the whole set.
      if (*located) retain_probe(i, std::move(*page));
    }
    scanned_end = std::max(scanned_end, end);
  }
  if (remaining_sources != 0)
    co_return absl::DataLossError(
        "member index refers to missing ordered member");
  status = ApplyInputs(operation, &members, &result);
  if (!status.ok()) co_return status;
  if (ReadOnly(operation) || result.changed_ == 0) co_return result;
  std::optional<MemoryReservation> working_admission;
  OrderedCollectionMutationPlan plan{.root_ = directory.root(),
                                     .expected_sequence_ = directory.sequence(),
                                     .changed_ = true,
                                     .writes_ = {}};
  plan.root_.item_count_ = result.length_;
  if (result.length_ == 0) {
    plan.delete_key_ = true;
    if (prepared != nullptr) {
      prepared->plan_ = std::move(plan);
      co_return result;
    }
    auto written = co_await CommitGroupedOrderedMutationLocked(
        store, partition, db_id, key, digest, object, std::move(plan),
        object->version().root_.expire_at_ms_, tx, replication,
        mutation_precondition);
    if (!written.ok()) co_return written;
    co_return result;
  }

  std::vector<std::pair<std::string_view, MemberState*>> pending;
  std::set<std::size_t> modified;
  for (auto& [member, state] : members) {
    if (!state.touched_) continue;
    if (state.source_ != kNoPage) modified.insert(state.source_);
    if (state.after_) pending.emplace_back(member, &state);
  }
  std::sort(pending.begin(), pending.end(), [](const auto& a, const auto& b) {
    return *a.second->after_ < *b.second->after_ ||
           (*a.second->after_ == *b.second->after_ && a.first < b.first);
  });
  // Original page bounds remain stable fences for the entire batch, even
  // when boundary members move. Scores alone route strict inequalities;
  // only a tie with a page's maximum needs its last member decoded. Skip
  // gaps in the request with an in-memory seek, preserving sequential reads
  // for equal-score runs and reusing each boundary page within the batch.
  std::size_t next = 0;
  for (std::size_t i = 0; next != pending.size() && i < metadata.size(); ++i) {
    i = std::max(
        i, std::min(directory.LowerBoundScore(*pending[next].second->after_),
                    metadata.size() - 1));
    std::optional<OwnedScanPage> boundary;
    while (next != pending.size()) {
      const auto& [member, state] = pending[next];
      if (i + 1 != metadata.size()) {
        if (*state->after_ > metadata[i].max_score_) break;
        if (*state->after_ == metadata[i].max_score_) {
          if (!boundary) {
            auto page = co_await read_owned_page(i);
            if (!page.ok()) co_return page.status();
            boundary.emplace(std::move(*page));
          }
          if (member > boundary->page_.snapshot_.entries_.back().value_) break;
        }
      }
      state->destination_ = i;
      modified.insert(i);
      ++next;
    }
    if (boundary && modified.contains(i)) retain_probe(i, std::move(*boundary));
  }
  if (next != pending.size())
    co_return absl::DataLossError("Sorted Set destination scan ended early");
  std::erase_if(probed_pages, [&](const auto& page) {
    return !modified.contains(page.first);
  });
  std::set<std::size_t> selected = modified;
  for (const auto i : modified) {
    if (i != 0) selected.insert(i - 1);
    if (i + 1 != metadata.size()) selected.insert(i + 1);
  }
  GroupedScratchBudget budget;
  if (selected.size() > std::numeric_limits<std::size_t>::max() / 128)
    co_return absl::ResourceExhaustedError("Sorted Set page metadata overflow");
  status = budget.AddBytes(selected.size() * 128);
  if (!status.ok()) co_return status;
  for (const auto i : selected) {
    status = add_page_budget(&budget, i);
    if (!status.ok()) co_return status;
  }
  for (const auto& [member, state] : members) {
    if (!state.touched_ || !state.after_) continue;
    status = budget.AddBytes(member.size() + 256);
    if (!status.ok()) co_return status;
  }
  auto admission = budget.Reserve(4);
  if (!admission.ok()) co_return admission.status();
  working_admission.emplace(std::move(*admission));
  auto check_plan_read = [&]([[maybe_unused]] std::size_t i) -> absl::Status {
    LAVIK_FAULT_INJECT(
        if (LAVIK_FAULT_MATCHES("LAVIK_FAIL_ZSET_PLAN_READ_KEY", key) &&
            LAVIK_FAULT_MATCHES_NTH("LAVIK_FAIL_ZSET_PLAN_READ_KEY", key,
                                    "LAVIK_FAIL_ZSET_PLAN_READ_PAGE",
                                    i + 1)) return absl::
            UnavailableError("injected Sorted Set plan page read failure"););
    return absl::OkStatus();
  };
  std::map<std::size_t, OrderedGroupSnapshot> loaded;
  // Reserve for all selected pages, but decode an unchanged neighbour only
  // when an overflowing page can reuse its space or a split/retirement changes
  // its link. Ordinary point edits still read only their modified pages.
  for (const auto i : modified) {
    if (probed_pages.contains(i)) {
      auto page = co_await read_owned_page(i);
      if (!page.ok()) co_return page.status();
      loaded.emplace(i, std::move(page->page_.snapshot_));
      // working_admission now covers the transferred strings.
    } else {
      status = check_plan_read(i);
      if (!status.ok()) co_return status;
      if (prepared != nullptr) {
        if (page_work.Add(metadata[i].encoded_bytes_))
          co_await bycorf::Yield(*store.worker_);
        if (shutdown_flush_requested_)
          co_return absl::CancelledError(
              "Sorted Set plan cancelled by shutdown");
      }
      auto page = co_await LoadOrderedGroupSnapshot(
          store, partition, db_id, key, digest, object, metadata[i].id_);
      if (!page.ok()) co_return page.status();
      loaded.emplace(i, std::move(page->snapshot_));
    }
  }
  if (operation.kind_ == SortedSetOperationKind::kAdd &&
      operation.entries_.size() == 1 && pending.size() == 1 &&
      pending.front().second->before_) {
    // A point score update owns the existing member already. Keep that
    // allocation and the page's ordered subsequences instead of deleting the
    // member, copying the request, and sorting the entire destination. Batch
    // inputs retain the general path because repeated members apply in order.
    const auto& [member, state] = pending.front();
    auto& source = loaded.at(state->source_).entries_;
    const auto entry =
        std::find_if(source.begin(), source.end(),
                     [&](const auto& at) { return at.value_ == member; });
    if (entry == source.end())
      co_return absl::DataLossError("missing Sorted Set point-update source");
    entry->score_ = *state->after_;
    if (state->source_ == state->destination_) {
      if (entry != source.begin() && OrderedEntryLess(*entry, *(entry - 1))) {
        const auto position =
            std::lower_bound(source.begin(), entry, *entry, OrderedEntryLess);
        std::rotate(position, entry, entry + 1);
      } else if (entry + 1 != source.end() &&
                 OrderedEntryLess(*(entry + 1), *entry)) {
        const auto position =
            std::lower_bound(entry + 1, source.end(), *entry, OrderedEntryLess);
        std::rotate(entry, entry + 1, position);
      }
    } else {
      auto moved = std::move(*entry);
      source.erase(entry);
      auto& destination = loaded.at(state->destination_).entries_;
      const auto position = std::lower_bound(
          destination.begin(), destination.end(), moved, OrderedEntryLess);
      destination.insert(position, std::move(moved));
    }
  } else {
    for (const auto i : modified) {
      auto& entries = loaded.at(i).entries_;
      std::erase_if(entries, [&](const auto& entry) {
        const auto member = members.find(entry.value_);
        return member != members.end() && member->second.touched_;
      });
    }
    for (const auto& [member, state] : pending)
      loaded.at(state->destination_)
          .entries_.push_back({std::string(member), *state->after_});

    // Removal preserves the order of every surviving entry.
    if (!pending.empty()) {
      for (const auto i : modified) {
        auto& entries = loaded.at(i).entries_;
        std::sort(entries.begin(), entries.end(), OrderedEntryLess);
      }
    }
  }
  // A score move can overflow its destination while freeing the adjacent
  // source. Reuse both already admitted pages when they still fit together;
  // allocating an extra id would force full-directory reconstruction.
  for (const auto i : modified) {
    if (!modified.contains(i + 1)) continue;
    auto balanced = RebalanceSortedSetGroupPair(loaded.at(i), loaded.at(i + 1));
    if (!balanced.ok()) co_return balanced.status();
  }

  // Repeated inserts before a full page otherwise split off one tiny successor
  // per command. Reuse at most one unchanged neighbour before allocating an
  // identity, using resident sizes to avoid speculative reads of full pages.
  // selected already admits both immediate neighbours. A successful pair keeps
  // its links and IDs, so it needs neither a wider interval nor more admission.
  constexpr auto kPagePayloadBytes =
      kCollectionGroupTargetBytes - kOrderedGroupHeaderBytes;
  constexpr auto kPairPayloadBytes = 2 * kPagePayloadBytes;
  auto bounded_payload_bytes = [](const OrderedGroupSnapshot& page) {
    std::size_t bytes = 0;
    for (const auto& entry : page.entries_) {
      const auto entry_bytes = kOrderedEntryHeaderBytes + entry.value_.size();
      if (entry_bytes > kPairPayloadBytes - bytes) return kPairPayloadBytes + 1;
      bytes += entry_bytes;
    }
    return bytes;
  };
  absl::InlinedVector<std::pair<std::size_t, std::size_t>, 4> overflowing;
  for (const auto i : modified) {
    const auto bytes = bounded_payload_bytes(loaded.at(i));
    if (bytes > kPagePayloadBytes && bytes <= kPairPayloadBytes)
      overflowing.emplace_back(i, bytes);
  }
  // Iterate only original overflows: adopted neighbours must fit the target
  // after redistribution and cannot start an outward chain of extra reads.
  for (const auto& [i, bytes] : overflowing) {
    std::size_t neighbor = kNoPage;
    for (const auto candidate : {i == 0 ? kNoPage : i - 1,
                                 i + 1 == metadata.size() ? kNoPage : i + 1}) {
      if (candidate == kNoPage || modified.contains(candidate)) continue;
      const auto encoded = metadata[candidate].encoded_bytes_;
      if (encoded < kOrderedGroupHeaderBytes ||
          encoded - kOrderedGroupHeaderBytes > kPairPayloadBytes - bytes)
        continue;
      if (neighbor == kNoPage || encoded < metadata[neighbor].encoded_bytes_)
        neighbor = candidate;
    }
    if (neighbor == kNoPage) continue;
    if (!loaded.contains(neighbor)) {
      status = check_plan_read(neighbor);
      if (!status.ok()) co_return status;
      auto page = co_await LoadOrderedGroupSnapshot(
          store, partition, db_id, key, digest, object, metadata[neighbor].id_);
      if (!page.ok()) co_return page.status();
      loaded.emplace(neighbor, std::move(page->snapshot_));
    }
    auto balanced =
        neighbor < i
            ? RebalanceSortedSetGroupPair(loaded.at(neighbor), loaded.at(i))
            : RebalanceSortedSetGroupPair(loaded.at(i), loaded.at(neighbor));
    if (!balanced.ok()) co_return balanced.status();
    // Indivisible variable-sized entries can still prevent a legal cut. Keep
    // that unchanged snapshot for a possible link rewrite after the split.
    if (*balanced) modified.insert(neighbor);
  }

  struct Route {
    std::uint64_t id_;
    std::size_t source_;
    std::size_t write_;
  };
  std::vector<Route> route;
  route.reserve(selected.size());
  auto next_id = directory.root().next_group_id_;
  std::uint64_t page_count = metadata.size();
  // Original modified pages have their immediate neighbours in selected;
  // redistributed neighbours retain their outer links. Process contiguous
  // intervals independently: gaps need neither routing nor payload rebuilds.
  for (auto selected_it = selected.begin(); selected_it != selected.end();) {
    const auto begin = *selected_it++;
    auto end = begin + 1;
    while (selected_it != selected.end() && *selected_it == end) {
      ++selected_it;
      ++end;
    }
    route.clear();
    for (std::size_t i = begin; i < end; ++i) {
      if (!modified.contains(i)) {
        route.push_back({metadata[i].id_, i, kNoPage});
        continue;
      }
      auto page = std::move(loaded.at(i));
      if (page.entries_.empty()) {
        page.retired_ = true;
        page.previous_ = page.next_ = 0;
        plan.writes_.push_back(std::move(page));
        continue;
      }
      auto split = SplitOrderedGroup(std::move(page), next_id);
      if (!split.ok()) co_return split.status();
      next_id = split->next_group_id_;
      for (auto& part : split->groups_) {
        route.push_back({part.id_, i, plan.writes_.size()});
        plan.writes_.push_back(std::move(part));
      }
    }
    // A non-global interval includes an unchanged boundary neighbour. The
    // global empty result was handled above by deleting the complete key.
    if (route.empty())
      co_return absl::DataLossError("empty Sorted Set selected interval");
    page_count = page_count - (end - begin) + route.size();
    for (std::size_t i = 0; i < route.size(); ++i) {
      auto& at = route[i];
      const auto previous =
          i == 0 ? metadata[begin].previous_ : route[i - 1].id_;
      const auto following =
          i + 1 == route.size() ? metadata[end - 1].next_ : route[i + 1].id_;
      if (at.write_ == kNoPage) {
        const auto& old = metadata[at.source_];
        if (old.previous_ == previous && old.next_ == following) continue;
        at.write_ = plan.writes_.size();
        if (const auto cached = loaded.find(at.source_);
            cached != loaded.end()) {
          plan.writes_.push_back(std::move(cached->second));
        } else {
          status = check_plan_read(at.source_);
          if (!status.ok()) co_return status;
          // Revalidate the captured logical view and current physical
          // location through the ordinary loader, including GC retry checks.
          auto neighbor = co_await LoadOrderedGroupSnapshot(
              store, partition, db_id, key, digest, object, old.id_);
          if (!neighbor.ok()) co_return neighbor.status();
          plan.writes_.push_back(std::move(neighbor->snapshot_));
        }
      }
      plan.writes_[at.write_].previous_ = previous;
      plan.writes_[at.write_].next_ = following;
    }
    if (begin == 0) plan.root_.first_group_ = route.front().id_;
    if (end == metadata.size()) plan.root_.last_group_ = route.back().id_;
  }
  if (page_count == 0 || page_count > std::numeric_limits<std::uint32_t>::max())
    co_return absl::DataLossError("invalid Sorted Set resulting page count");
  plan.root_.group_count_ = page_count;
  plan.root_.next_group_id_ = next_id;
  // A failed redistribution may have inspected a neighbour whose link stayed
  // unchanged. Keep only the owned after-images through member preparation and
  // publication; no later phase needs those speculative decoded bytes.
  loaded.clear();
  SortedSetMemberMutation member_mutation;
  {
    // Names borrow command inputs, or the outer pop/range selection. Finish
    // index preparation here while those names are alive; only owned leaf
    // after-images escape in PreparedOrderedMutation. Never retain these
    // views after the selected reply is cleared by range removal.
    std::vector<SortedSetMemberChange> changes;
    changes.reserve(members.size());
    for (const auto& [member, state] : members) {
      if (state.touched_)
        changes.push_back({member, state.before_, state.after_});
    }
    LAVIK_FAULT_INJECT({
      if (prepared != nullptr) {
        const auto paused =
            co_await PauseGroupedWriteForTest(*store.worker_, key, "members");
        if (!paused.ok()) co_return paused;
      }
    });
    auto built = co_await PrepareSortedSetMembers(
        store, partition, db_id, key, digest, object, plan, prepared != nullptr,
        std::span<const SortedSetMemberChange>(changes),
        member_probe ? &*member_probe : nullptr);
    if (!built.ok()) co_return built.status();
    member_mutation = std::move(*built);
  }
  if (prepared != nullptr) {
    prepared->pages_ = std::move(*working_admission);
    prepared->plan_ = std::move(plan);
    prepared->members_.emplace(std::move(member_mutation));
    co_return result;
  }
  auto written = co_await CommitGroupedOrderedMutationLocked(
      store, partition, db_id, key, digest, object, std::move(plan),
      object->version().root_.expire_at_ms_, tx, replication,
      mutation_precondition, &member_mutation);
  if (!written.ok()) co_return written;
  co_return result;
}

}  // namespace lavik::storage
