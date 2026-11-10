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

#include <array>
#include <bit>
#include <cmath>
#include <map>
#include <set>
#include <type_traits>
#include <utility>

#include "../impl.h"
#include "absl/container/inlined_vector.h"
#include "lavik/error_detail.h"
#include "lavik/glob.h"
#include "lavik/random_sample.h"
#include "lavik/storage/detail/grouped/scratch.h"
#include "lavik/storage/detail/hash_read.h"
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

// A published ordered page is already framing/identity checked by its
// loader. Only the binary-search probes and exact match need decoding here.
absl::StatusOr<std::optional<std::size_t>> FindRankSlot(
    const auto& page, double score, std::string_view member) {
  if (page.indexed_) {
    const auto& reader = *page.indexed_;
    auto at = reader.LowerBound(score, member);
    if (!at.ok()) return at.status();
    if (*at == reader.size()) return std::nullopt;
    auto entry = reader.At(*at);
    if (!entry.ok()) return entry.status();
    if (entry->score_ == score && entry->value_ == member) return *at;
    return std::nullopt;
  }
  const auto& entries = page.entries_;
  const auto at = std::lower_bound(
      entries.begin(), entries.end(), OrderedCollectionEntryView{member, score},
      [](const auto& entry, const auto& key) {
        return entry.score_ < key.score_ ||
               (entry.score_ == key.score_ && entry.value_ < key.value_);
      });
  if (at != entries.end() && at->score_ == score && at->value_ == member)
    return at - entries.begin();
  return std::nullopt;
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
    return MemoryLimitError("OOM Sorted Set output member admission");
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
      return MemoryLimitError("OOM Sorted Set output vector admission");
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
    return MemoryLimitError("OOM Sorted Set packed output admission");
  }
  packed.members_.values_.reserve(*slots);
  packed.scores_.reserve(*scores);
  packed.members_.blocks_.reserve(*blocks);
  if (packed.members_.values_.capacity() > *slots ||
      packed.scores_.capacity() > *scores ||
      packed.members_.blocks_.capacity() > *blocks)
    return MemoryLimitError("OOM Sorted Set packed capacity exceeds admission");
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
      return MemoryLimitError("OOM Sorted Set packed member admission");
    }
    std::string block;
    block.reserve(capacity);
    if (block.capacity() > capacity)
      return MemoryLimitError(
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
      return MemoryLimitError("OOM Sorted Set random rank admission");
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
      return MemoryLimitError("OOM Sorted Set random reply admission");
    }
    result->members_.resize(requested);
    result->retained_charge_.Adopt(
        &*output, result->members_.capacity() * sizeof(SortedSetMember));
    return absl::OkStatus();
  }
};

// Keep only COUNT integer prefixes to find the examination boundary, then
// emit whole collision buckets. Member-index scans apply this to one leaf;
// score-ordered fallback scans apply it across all pages. Both preserve
// mutation-tolerant digest cursors, including MATCH misses.
struct ScanBoundary {
  MemoryReservation admission_;
  std::vector<std::uint64_t> prefixes_;
  std::size_t limit_ = 0;
  absl::Status Prepare(std::uint64_t count, std::uint64_t cardinality) {
    const auto limit = std::min(count, cardinality);
    if (limit > std::numeric_limits<std::size_t>::max() / sizeof(std::uint64_t))
      return absl::ResourceExhaustedError("Sorted Set scan count overflow");
    auto admission = TryReserveMemory(limit * sizeof(std::uint64_t));
    if (!admission) {
      RecordMemoryRejection();
      return MemoryLimitError("OOM Sorted Set scan boundary admission");
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
    auto& members = result->members_;
    if (members.size() < 2) return;
    const auto without_index = [&] {
      std::sort(members.begin(), members.end(),
                [](const auto& left, const auto& right) {
                  const auto a = ScanCursorPrefix(ComputeDigest(left.member_));
                  const auto b = ScanCursorPrefix(ComputeDigest(right.member_));
                  return a < b || (a == b && left.member_ < right.member_);
                });
    };
    struct ScanOrder {
      std::uint64_t prefix_;
      std::size_t source_;
    };
    // Digest computation is substantially more expensive than comparing two
    // prefixes. Decorate only this reply, then discard the index before
    // returning. Admission is optional: retain the allocation-free comparator
    // when temporary memory is unavailable.
    if (members.size() > SIZE_MAX / sizeof(ScanOrder) ||
        members.size() > std::vector<ScanOrder>{}.max_size()) {
      without_index();
      return;
    }
    auto admission = TryReserveMemory(members.size() * sizeof(ScanOrder));
    if (!admission) {
      without_index();
      return;
    }
    std::vector<ScanOrder> order;
    order.reserve(members.size());
    if (order.capacity() > admission->bytes() / sizeof(ScanOrder)) {
      std::vector<ScanOrder>{}.swap(order);
      without_index();
      return;
    }
    for (std::size_t i = 0; i < members.size(); ++i)
      order.push_back({ScanCursorPrefix(ComputeDigest(members[i].member_)), i});
    std::sort(order.begin(), order.end(),
              [&](const auto& left, const auto& right) {
                return left.prefix_ < right.prefix_ ||
                       (left.prefix_ == right.prefix_ &&
                        members[left.source_].member_ <
                            members[right.source_].member_);
              });
    // Apply each permutation cycle in place. Member strings and their retained
    // charges move together; no second member array or string copies are
    // needed.
    for (std::size_t i = 0; i < order.size(); ++i) {
      if (order[i].source_ == i) continue;
      auto held = std::move(members[i]);
      auto destination = i;
      while (order[destination].source_ != i) {
        const auto source = order[destination].source_;
        members[destination] = std::move(members[source]);
        order[destination].source_ = destination;
        destination = source;
      }
      members[destination] = std::move(held);
      order[destination].source_ = destination;
    }
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

// Locate only the selected rank/score interval in an indexed page. LIMIT
// offsets skip metadata positions rather than decoding skipped members. Full
// scans and BYLEX retain their existing paths because score order is not
// arbitrary member order.
absl::Status ReadIndexedRange(const IndexedOrderedReader& reader,
                              std::uint64_t base_rank, ReadCursor& cursor) {
  if (cursor.done_) return absl::OkStatus();
  const auto& operation = cursor.operation_;
  auto& result = *cursor.result_;
  std::size_t first = 0, end = reader.size();
  if (operation.range_mode_ == SortedSetRangeMode::kRank) {
    const auto low =
        operation.reverse_ ? result.length_ - cursor.end_ : cursor.first_;
    const auto high =
        operation.reverse_ ? result.length_ - cursor.first_ : cursor.end_;
    first = low <= base_rank
                ? 0
                : std::min<std::uint64_t>(low - base_rank, reader.size());
    end = high <= base_rank
              ? 0
              : std::min<std::uint64_t>(high - base_rank, reader.size());
  } else {
    auto low = reader.LowerBoundScore(operation.minimum_score_.value_,
                                      operation.minimum_score_.exclusive_);
    auto high = reader.LowerBoundScore(operation.maximum_score_.value_,
                                       !operation.maximum_score_.exclusive_);
    if (!low.ok()) return low.status();
    if (!high.ok()) return high.status();
    first = *low;
    end = *high;
  }
  if (first >= end) return absl::OkStatus();
  if (operation.kind_ == SortedSetOperationKind::kCount) {
    result.count_ += end - first;
    return absl::OkStatus();
  }
  if (operation.limit_ &&
      cursor.skipped_ < static_cast<std::uint64_t>(operation.offset_)) {
    const auto skip = std::min<std::uint64_t>(
        end - first, operation.offset_ - cursor.skipped_);
    cursor.skipped_ += skip;
    if (operation.reverse_)
      end -= skip;
    else
      first += skip;
  }
  if (operation.limit_ && operation.count_ >= 0) {
    const auto count = std::min<std::uint64_t>(
        end - first, operation.count_ - result.member_count());
    if (operation.reverse_)
      first = end - count;
    else
      end = first + count;
  }
  if (first == end) return absl::OkStatus();
  if (operation.packed_output_) {
    auto begin = reader.At(first), last = reader.At(end - 1);
    if (!begin.ok()) return begin.status();
    if (!last.ok()) return last.status();
    const auto start = begin->value_.data();
    const auto stop = last->value_.data() + last->value_.size();
    if (stop < start)
      return absl::DataLossError("invalid indexed Sorted Set range offsets");
    const auto bytes = static_cast<std::size_t>(stop - start);
    auto reserved = ReservePackedOutput(&result, end - first, 1);
    if (!reserved.ok()) return reserved;
    const auto capacity = std::max(bytes, std::string{}.capacity());
    if (capacity == SIZE_MAX ||
        capacity + 1 > SIZE_MAX - result.retained_charge_.bytes())
      return absl::ResourceExhaustedError("Sorted Set packed output overflow");
    auto admission = TryReserveMemory(capacity + 1);
    if (!admission) {
      RecordMemoryRejection();
      return MemoryLimitError("OOM indexed Sorted Set reply");
    }
    auto& output = *result.packed_members_;
    const auto block = output.members_.blocks_.size();
    // One exact selected block, regardless of member count or reply direction.
    // Reversing descriptors preserves the single copy of contiguous bytes.
    output.members_.blocks_.emplace_back(start, bytes);
    if (output.members_.blocks_.back().capacity() > capacity)
      return MemoryLimitError(
          "OOM Sorted Set packed capacity exceeds admission");
    result.retained_charge_.Account(
        CurrentMemoryAccountingShard(),
        result.retained_charge_.bytes() +
            output.members_.blocks_.back().capacity() + 1);
    for (std::size_t i = 0; i < end - first && !cursor.done_; ++i) {
      const auto index = operation.reverse_ ? end - 1 - i : first + i;
      auto entry = reader.At(index);
      if (!entry.ok()) return entry.status();
      if (entry->value_.data() < start ||
          entry->value_.data() + entry->value_.size() > stop)
        return absl::DataLossError("invalid indexed Sorted Set reply interval");
      auto visited = cursor.VisitWithOutput(*entry, base_rank + index, [&] {
        output.members_.values_.push_back(
            {block, static_cast<std::size_t>(entry->value_.data() - start),
             entry->value_.size()});
        output.scores_.push_back(entry->score_);
        return absl::OkStatus();
      });
      if (!visited.ok()) return visited;
    }
    return absl::OkStatus();
  }
  for (std::size_t i = 0; i < end - first && !cursor.done_; ++i) {
    const auto index = operation.reverse_ ? end - 1 - i : first + i;
    auto entry = reader.At(index);
    if (!entry.ok()) return entry.status();
    auto visited = cursor.Visit(*entry, base_rank + index);
    if (!visited.ok()) return visited;
  }
  return absl::OkStatus();
}

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
    return MemoryLimitError("OOM Sorted Set reply admission");
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

absl::StatusOr<std::optional<CompactValueUpdate>> PlanCompactSortedSetPoint(
    std::optional<CompactValueView> value, const SortedSetOperation& operation,
    Members& members, SortedSetResult& result, OrderedCompactPayload& payload) {
  const bool add = operation.kind_ == SortedSetOperationKind::kAdd;
  if (!value || (add ? operation.entries_.size() != 1
                     : operation.kind_ != SortedSetOperationKind::kRemove ||
                           operation.members_.size() != 1))
    return std::nullopt;
  const auto member =
      add ? operation.entries_.front().member_ : operation.members_.front();
  // Conservatively leave near-threshold additions to the owned promotion
  // path, before applying input semantics or populating the reply.
  if (value->encoded_.size() >= kCollectionPromotionBytes ||
      (add && member.size() + 12 >=
                  kCollectionPromotionBytes - value->encoded_.size()))
    return std::nullopt;
  auto reader = OrderedCompactReader::Open(
      OrderedCollectionKind::kSortedSet, value->encoded_, value->logical_size_);
  if (!reader.ok()) return reader.status();
  // The owned mutation builder upgrades changed v1 values to v2. Decide
  // before applying command semantics so fallback cannot apply inputs twice.
  if (!reader->indexed()) [[unlikely]] {
    // Preserve the point path's complete framing/order validation before
    // the generic builder sorts the after-image. Migration must not repair
    // an invalid source silently.
    OrderedCompactEntryView previous;
    for (std::size_t i = 0; i < reader->size(); ++i) {
      auto entry = reader->Next();
      if (!entry.ok()) return entry.status();
      if (i != 0 && (previous.score_ > entry->score_ ||
                     (previous.score_ == entry->score_ &&
                      previous.value_ > entry->value_)))
        return absl::DataLossError("unsorted compact Sorted Set");
      previous = *entry;
    }
    return std::nullopt;
  }
  auto& state = members.at(member);
  auto slot = reader->indexed()->FindSortedSetMember(member);
  if (!slot.ok()) return slot.status();
  const auto removed = *slot;
  if (removed != reader->size()) {
    if (state.before_)
      return absl::DataLossError("duplicate persisted Sorted Set member");
    auto score = reader->indexed()->Score(removed);
    if (!score.ok()) return score.status();
    state.before_ = state.after_ = *score;
  }
  result.key_exists_ = true;
  result.length_ = value->logical_size_;
  auto applied = ApplyInputs(operation, &members, &result);
  if (!applied.ok()) return applied;
  CompactValueUpdate update;
  if (result.changed_ == 0) return update;
  update.changed_ = true;
  update.logical_size_ = result.length_;
  if (result.length_ == 0) {
    update.erase_ = true;
    return update;
  }
  auto& output = payload.V2(*reader->indexed());
  // Compact values have no member-to-score index, so the validation pass
  // above also resolves the old slot. Reuse it for deletion and binary-seek
  // the new position instead of scanning the page again.
  auto edited = output.EditSortedSetMember(removed, member, state.after_);
  if (!edited.ok()) return edited;
  return update;
}

absl::StatusOr<CompactValueUpdate> ApplyCompactSortedSet(
    std::optional<CompactValueView> value, const SortedSetOperation& operation,
    Members& members, SortedSetResult& result,
    std::optional<MemoryReservation>& compact_admission,
    OrderedCompactPayload* point_payload = nullptr) {
  if (value && value->encoded_.starts_with(kSortedSetMagicV2) &&
      ReadOnly(operation) &&
      (operation.kind_ == SortedSetOperationKind::kRandom ||
       ((operation.kind_ == SortedSetOperationKind::kRange ||
         operation.kind_ == SortedSetOperationKind::kCount) &&
        operation.range_mode_ != SortedSetRangeMode::kLex))) {
    auto source =
        OrderedCompactReader::Open(OrderedCollectionKind::kSortedSet,
                                   value->encoded_, value->logical_size_);
    if (!source.ok()) return source.status();
    assert(source->indexed());
    result.key_exists_ = true;
    result.length_ = source->size();
    if (operation.kind_ == SortedSetOperationKind::kRandom) {
      RandomSelection selection;
      auto status =
          selection.Prepare(result.length_, operation.count_, true, &result);
      if (!status.ok()) return status;
      for (const auto& [rank, slot] : selection.draws_) {
        auto entry = source->indexed()->At(rank);
        if (!entry.ok()) return entry.status();
        auto copied = MakeOutput(*entry);
        if (!copied.ok()) return copied.status();
        result.members_[slot] = std::move(*copied);
      }
    } else {
      ReadCursor cursor(operation, &result);
      auto status = ReadIndexedRange(*source->indexed(), 0, cursor);
      if (!status.ok()) return status;
    }
    return CompactValueUpdate{};
  }
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
  if (point_payload) {
    auto point = PlanCompactSortedSetPoint(value, operation, members, result,
                                           *point_payload);
    if (!point.ok()) return point.status();
    if (*point) return std::move(**point);
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
    auto prepared = boundary.Prepare(operation.scan_count_, entries.size());
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
    auto encoded = EncodeOrderedCompactValue(OrderedCollectionKind::kSortedSet,
                                             entries, 2);
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
    auto encoded = EncodeOrderedCompactValue(OrderedCollectionKind::kSortedSet,
                                             entries, 2);
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
      EncodeOrderedCompactValue(OrderedCollectionKind::kSortedSet, entries, 2);
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

/// Attempts a grouped read on the owner worker while the caller retains key IS.
/// Member-score projections acquire leaf S holds in identity order before
/// capturing a common view and retain them through waits and IO. Store state is
/// released before group waits. A disengaged result requests one whole-key S
/// attempt after the caller drops IS; error results propagate without retry.
/// Every exit releases the group holds and admission owned by this coroutine.
Task<absl::StatusOr<std::optional<SortedSetResult>>>
StorageEngine::Impl::TryConcurrentSortedSetRead(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const SortedSetOperation& operation) {
  const auto valid = Validate(operation);
  if (!valid.ok()) co_return valid;
  auto& store = CurrentStore();
  auto& partition = PartitionForKey(store, key);
  co_await store.store_state_mutex_.Lock();
  UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
  const auto* found = (partition.indexes_.Get(db_id)
                           ? partition.indexes_[db_id].Find(digest, key)
                           : nullptr);
  const auto now = operation.now_ms_ ? operation.now_ms_ : UnixTimeMillis();
  if (!found || !found->key_complete() || !found->value_.grouped() ||
      found->value_.value_type() != ValueType::kSortedSet ||
      IsExpired(*found, now))
    co_return std::nullopt;
  auto view = partition.grouped_objects_[db_id].Lookup(
      key, GroupedObjectVersion{
               .root_ = MaterializeIndexLocation(*found),
               .db_epoch_ = EffectiveRecordDbEpoch(partition, db_id),
               .replication_epoch_ = partition.replication_epoch_,
               .index_generation_ = partition.grouped_generations_[db_id]});
  if (!view.ok()) co_return view.status();
  auto object = std::move(*view);
  if (!object || !object->has_member_index()) co_return std::nullopt;
  if (operation.kind_ == SortedSetOperationKind::kLength) {
    // Publication replaces the resident root without yielding. IS excludes
    // structural replacement; the store lock makes this metadata projection
    // a complete logical read without taking any payload-group holds.
    const auto readable = object->ReadStatus();
    if (!readable.ok()) co_return readable;
    SortedSetResult result;
    result.key_exists_ = true;
    result.length_ = object->ordered_directory().root().item_count_;
    co_return std::optional(std::move(result));
  }

  if (operation.kind_ != SortedSetOperationKind::kScores) {
    unlock.Unlock();
    auto read = co_await TryConcurrentSortedSetPageRead(
        store, partition, db_id, key, digest, operation, object);
    if (!read.ok() &&
        (EffectiveRecordDbEpoch(partition, db_id) !=
             object->version().db_epoch_ ||
         partition.replication_epoch_ != object->version().replication_epoch_ ||
         partition.grouped_generations_[db_id] !=
             object->version().index_generation_))
      co_return std::nullopt;
    co_return read;
  }
  if (operation.members_.size() > SIZE_MAX / sizeof(GroupedRecordId)) {
    RecordMemoryRejection();
    co_return absl::ResourceExhaustedError("Sorted Set lock plan overflow");
  }
  auto admission =
      TryReserveMemory(operation.members_.size() * sizeof(GroupedRecordId));
  if (!admission) {
    RecordMemoryRejection();
    co_return MemoryLimitError("OOM Sorted Set group read plan");
  }
  std::vector<GroupedRecordId> groups;
  groups.reserve(operation.members_.size());
  // Before InitMemoryLimit, a successful reservation is unmetered and carries
  // zero bytes. Only a metered reservation bounds the allocated capacity.
  if (admission->bytes() &&
      groups.capacity() > admission->bytes() / sizeof(GroupedRecordId)) {
    RecordMemoryRejection();
    co_return MemoryLimitError("OOM Sorted Set group read plan");
  }
  for (const auto member : operation.members_) {
    const auto* route = object->directory().Find(member);
    if (!route) co_return absl::DataLossError("missing member prefix route");
    groups.push_back(route->id_);
  }
  std::sort(groups.begin(), groups.end());
  groups.erase(std::unique(groups.begin(), groups.end()), groups.end());
  if (!store.group_locks_)
    store.group_locks_ = std::make_unique<GroupedLockTable>(*store.worker_);
  auto holds = store.group_locks_->MakeLockSet(
      {db_id, tx::FingerprintOf(digest), object->incarnation()});
  unlock.Unlock();
  for (const auto id : groups) {
    co_await holds.Acquire({GroupedLockIndex::kMember, id},
                           GroupedLockMode::kShared);
  }
  // Acquiring a later group can yield to writers. Capture the common logical
  // cut only after all holds exist; never combine scores read before and after
  // a conflicting multi-member mutation. Unrelated leaves may keep changing.
  const auto current =
      partition.grouped_objects_[db_id].CurrentForMutation(key);
  if (!current || current->incarnation() != object->incarnation() ||
      current->version().db_epoch_ != object->version().db_epoch_ ||
      current->version().replication_epoch_ !=
          object->version().replication_epoch_ ||
      current->version().index_generation_ !=
          object->version().index_generation_)
    co_return std::nullopt;
  for (const auto member : operation.members_) {
    const auto* route = current->directory().Find(member);
    if (!route || !std::binary_search(groups.begin(), groups.end(), route->id_))
      co_return std::nullopt;
  }
  const auto readable = current->ReadStatus();
  if (!readable.ok()) co_return readable;
  const auto expires = current->version().root_.expire_at_ms_;
  if (expires &&
      expires <= (operation.now_ms_ ? operation.now_ms_ : UnixTimeMillis()))
    co_return std::nullopt;
  LAVIK_FAULT_INJECT({
    if (LAVIK_FAULT_MATCHES("LAVIK_ZSET_READ_PAUSE_KEY", key)) {
      spdlog::info("zset member read locks held before page IO");
      const auto paused = co_await fault_injection::PauseWhileFileExists(
          "LAVIK_ZSET_READ_PAUSE_FILE");
      if (!paused.ok()) co_return paused;
    }
  });
  auto read = co_await ReadGroupedSortedSetScores(
      store, partition, db_id, key, digest, operation, current, true);
  if (!read.ok()) {
    if (EffectiveRecordDbEpoch(partition, db_id) !=
            current->version().db_epoch_ ||
        partition.replication_epoch_ != current->version().replication_epoch_ ||
        partition.grouped_generations_[db_id] !=
            current->version().index_generation_)
      co_return std::nullopt;
    co_return read.status();
  }
  co_return std::optional(std::move(*read));
}

Task<absl::StatusOr<std::optional<SortedSetResult>>>
StorageEngine::Impl::TryConcurrentSortedSetPageRead(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const SortedSetOperation& operation, GroupedObject::Handle object) {
  if (!store.group_locks_)
    store.group_locks_ = std::make_unique<GroupedLockTable>(*store.worker_);
  auto holds = store.group_locks_->MakeLockSet(
      {db_id, tx::FingerprintOf(digest), object->incarnation()});
  RandomSelection random;
  SortedSetResult random_result;
  if (operation.kind_ == SortedSetOperationKind::kRandom) {
    random_result.key_exists_ = true;
    random_result.length_ = object->ordered_directory().root().item_count_;
    const auto selected = random.Prepare(
        random_result.length_, operation.count_, false, &random_result);
    if (!selected.ok()) co_return selected;
  }
  std::optional<double> member_score;
  if (operation.kind_ == SortedSetOperationKind::kRank) {
    const auto* route = object->directory().Find(operation.members_.front());
    if (!route) co_return absl::DataLossError("missing member prefix route");
    const auto id = route->id_;
    co_await holds.Acquire({GroupedLockIndex::kMember, id},
                           GroupedLockMode::kShared);
    // A waiting reader must use the last holder's new leaf, not the revision
    // observed before acquiring S. IS keeps the prefix topology unchanged.
    auto current = partition.grouped_objects_[db_id].CurrentForMutation(key);
    if (!current || current->incarnation() != object->incarnation() ||
        current->version().db_epoch_ != object->version().db_epoch_ ||
        current->version().replication_epoch_ !=
            object->version().replication_epoch_ ||
        current->version().index_generation_ !=
            object->version().index_generation_)
      co_return std::nullopt;
    object = std::move(current);
    // Rank needs one scalar, not an allocated score reply or an additional
    // score-command coroutine. Both borrowed spans live in this frame until
    // the leaf loader completes; the member hold preserves the score below.
    if (shutdown_flush_requested_)
      co_return absl::CancelledError("member read cancelled by shutdown");
    HashFieldLookup request{.group_ = id,
                            .field_ = operation.members_.front(),
                            .result_index_ = 0,
                            .value_ = std::nullopt};
    auto read = co_await LoadSortedSetMemberScores(
        store, partition, db_id, key, digest, object, std::span(&request, 1),
        std::span(&member_score, 1), nullptr, true, operation.diagnostics_);
    if (!read.ok()) co_return read.status();
  }
  std::size_t rank_page = kNoPage;
  // Enumerate only payloads used by the operation. Counts for complete score
  // pages come from one immutable directory, so ZCOUNT needs just boundary
  // holds. Mixed-score BYLEX has no lexical page index and requires all pages.
  const auto visit = [&](const GroupedObject::Handle& source, auto emit_lock) {
    const auto emit = [&](std::uint64_t id) {
      emit_lock(GroupedLockId{GroupedLockIndex::kOrdered, {id, 0}});
    };
    if (operation.kind_ == SortedSetOperationKind::kScan &&
        source->directory().root().seed_ == CurrentDigestSeed()) {
      const auto& leaves = source->directory().groups();
      const auto* first = leaves.Floor(operation.scan_cursor_);
      if (!first) return false;
      auto route = leaves.find(first->id_.prefix_);
      auto remaining = operation.scan_count_;
      bool boundary = true;
      while (route != leaves.end() && remaining) {
        emit_lock(GroupedLockId{GroupedLockIndex::kMember, route->second.id_});
        // A cursor inside the first leaf has an unknown remaining count.
        // Later leaves are entirely after it, so their resident counts bound
        // this call's examination window even with MATCH misses/collisions.
        const auto count = boundary && operation.scan_cursor_ != route->first
                               ? 0
                               : route->second.field_count_;
        remaining -= std::min(remaining, count);
        boundary = false;
        ++route;
      }
      return true;
    }
    const auto& directory = source->ordered_directory();
    const auto& pages = directory.groups();
    const auto length = directory.root().item_count_;
    if (operation.kind_ == SortedSetOperationKind::kRandom) {
      // Preserve the sampled population. A changed cardinality gets a clean
      // S fallback, instead of biasing or indexing old draws into a new set.
      if (length != random_result.length_) return false;
      for (const auto& [rank, slot] : random.draws_) {
        const auto position = directory.FindRank(rank);
        if (!position) return false;
        emit(pages[position->group_index_].id_);
      }
      return true;
    }
    std::size_t first = 0, end = pages.size();
    if (operation.kind_ == SortedSetOperationKind::kRank) {
      if (!member_score) return true;
      first = directory.LowerBoundScore(*member_score);
      end = directory.UpperBoundScore(*member_score);
      if (end - first <= 1) {
        rank_page = first;
        if (first < pages.size()) emit(pages[first].id_);
        return true;
      }
      while (first < end) {
        const auto mid = first + (end - first) / 2;
        bool less = false;
        if (pages[mid].max_score_ == *member_score) {
          const auto bound =
              pages[mid].max_key_.LessThan(operation.members_.front());
          if (!bound) return false;
          less = *bound;
        }
        if (less)
          first = mid + 1;
        else
          end = mid;
      }
      rank_page = first;
      if (first < pages.size()) emit(pages[first].id_);
      return true;
    }
    if (operation.kind_ == SortedSetOperationKind::kRange &&
        operation.range_mode_ == SortedSetRangeMode::kRank) {
      const auto [begin_rank, end_rank] =
          RankSlice(operation.first_, operation.last_, length);
      if (begin_rank == end_rank) return true;
      const auto low = operation.reverse_ ? length - end_rank : begin_rank;
      const auto high = operation.reverse_ ? length - begin_rank : end_rank;
      first = directory.FindRank(low)->group_index_;
      end = directory.FindRank(high - 1)->group_index_ + 1;
    } else if (operation.range_mode_ == SortedSetRangeMode::kScore) {
      first = directory.LowerBoundScore(operation.minimum_score_.value_,
                                        operation.minimum_score_.exclusive_);
      end = directory.UpperBoundScore(operation.maximum_score_.value_,
                                      operation.maximum_score_.exclusive_);
    }
    if (first >= end || (operation.limit_ && operation.count_ == 0))
      return true;
    const auto whole = [&](std::size_t i) {
      const auto low = operation.minimum_score_,
                 high = operation.maximum_score_;
      return (low.exclusive_ ? pages[i].min_score_ > low.value_
                             : pages[i].min_score_ >= low.value_) &&
             (high.exclusive_ ? pages[i].max_score_ < high.value_
                              : pages[i].max_score_ <= high.value_);
    };
    if (operation.range_mode_ == SortedSetRangeMode::kScore) {
      if (operation.kind_ == SortedSetOperationKind::kCount) {
        // The rank index accounts for the complete interior in O(log G).
        // Do not turn a metadata-only count into a linear lock-plan scan.
        if (!whole(first)) emit(pages[first].id_);
        if (end - 1 != first && !whole(end - 1)) emit(pages[end - 1].id_);
        return true;
      }
      if (operation.limit_) {
        const auto boundary = operation.reverse_ ? end - 1 : first;
        const auto unknown = whole(boundary) ? 0 : pages[boundary].item_count_;
        if (unknown) emit(pages[boundary].id_);
        // Only the starting boundary's match count is unknown before IO.
        // Cover its possible displacement, then skip OFFSET through resident
        // ranks. Large LIMIT offsets must not acquire every skipped page.
        const auto base = directory.CountBefore(first);
        const auto available = directory.CountBefore(end) - base;
        const auto skip = std::min<std::uint64_t>(operation.offset_, available);
        const auto take =
            operation.count_ < 0
                ? available - skip
                : std::min<std::uint64_t>(
                      available - skip,
                      unknown + static_cast<std::uint64_t>(operation.count_));
        if (!take) return true;
        const auto low =
            operation.reverse_ ? base + available - skip - take : base + skip;
        const auto high =
            operation.reverse_ ? base + available - skip : base + skip + take;
        first = directory.FindRank(low)->group_index_;
        end = directory.FindRank(high - 1)->group_index_ + 1;
      }
    }
    for (auto i = first; i < end; ++i) emit(pages[i].id_);
    return true;
  };
  // Boundary counts, small ranges and scan windows usually need only a few
  // holds. Retain their plan in the coroutine; wide plans still pre-admit
  // heap storage before filling it, including when memory is unmetered.
  std::array<GroupedLockId, 4> local_groups;
  std::size_t count = 0;
  if (!visit(object, [&](auto id) {
        if (count < local_groups.size()) local_groups[count] = id;
        ++count;
      }))
    co_return std::nullopt;
  std::optional<MemoryReservation> admission;
  std::vector<GroupedLockId> allocated_groups;
  std::span<GroupedLockId> groups;
  if (count <= local_groups.size()) {
    groups = std::span(local_groups).first(count);
  } else {
    if (count > SIZE_MAX / sizeof(GroupedLockId))
      co_return absl::ResourceExhaustedError(
          "Sorted Set read lock plan overflow");
    admission = TryReserveMemory(count * sizeof(GroupedLockId));
    if (!admission) co_return MemoryLimitError("OOM Sorted Set read lock plan");
    allocated_groups.reserve(count);
    if (admission->bytes() && allocated_groups.capacity() >
                                  admission->bytes() / sizeof(GroupedLockId))
      co_return MemoryLimitError("OOM Sorted Set read lock plan");
    visit(object, [&](auto id) { allocated_groups.push_back(id); });
    groups = allocated_groups;
  }
  std::sort(groups.begin(), groups.end());
  groups =
      groups.first(std::unique(groups.begin(), groups.end()) - groups.begin());
  for (const auto id : groups) {
    co_await holds.Acquire(id, GroupedLockMode::kShared);
  }
  auto current = partition.grouped_objects_[db_id].CurrentForMutation(key);
  if (!current || current->incarnation() != object->incarnation() ||
      current->version().db_epoch_ != object->version().db_epoch_ ||
      current->version().replication_epoch_ !=
          object->version().replication_epoch_ ||
      current->version().index_generation_ !=
          object->version().index_generation_)
    co_return std::nullopt;
  // Published directories are immutable. An unchanged handle preserves the
  // original routes even if a lock wait yielded; changed views still need
  // full coverage validation before becoming the read's common cut.
  if (current.get() != object.get()) {
    bool covered = true;
    if (!visit(current,
               [&](auto id) {
                 covered &=
                     std::binary_search(groups.begin(), groups.end(), id);
               }) ||
        !covered)
      co_return std::nullopt;
  }
  // This final directory is the logical cut: all selected payloads are now
  // frozen, while preceding counts and unrelated bounds belong to this root.
  // A route change while waiting gets one S fallback, never an IX/IS retry
  // loop.
  const auto readable = current->ReadStatus();
  if (!readable.ok()) co_return readable;
  const auto expires = current->version().root_.expire_at_ms_;
  if (expires &&
      expires <= (operation.now_ms_ ? operation.now_ms_ : UnixTimeMillis()))
    co_return std::nullopt;
  LAVIK_FAULT_INJECT({
    if (LAVIK_FAULT_MATCHES("LAVIK_ZSET_RANGE_PAUSE_KEY", key)) {
      for (const auto id : groups)
        spdlog::info("zset read hold index={} prefix={} bits={}",
                     unsigned(id.index_), id.record_.prefix_,
                     unsigned(id.record_.bits_));
      spdlog::info("zset range read locks held before page IO");
      const auto paused = co_await fault_injection::PauseWhileFileExists(
          "LAVIK_ZSET_RANGE_PAUSE_FILE");
      if (!paused.ok()) co_return paused;
    }
  });
  auto read = co_await [&]() {
    if (operation.kind_ == SortedSetOperationKind::kRank)
      return ReadLockedSortedSetRank(store, partition, db_id, key, digest,
                                     operation, current, member_score,
                                     rank_page);
    if (operation.kind_ == SortedSetOperationKind::kRandom)
      return ReadGroupedSortedSetRandom(store, partition, db_id, key, digest,
                                        operation, current, random.draws_,
                                        std::move(random_result), true);
    return ExecuteGroupedSortedSetLocked(store, partition, db_id, key, digest,
                                         operation, current, nullptr, nullptr,
                                         nullptr, nullptr, nullptr, true);
  }();
  if (!read.ok()) {
    if (EffectiveRecordDbEpoch(partition, db_id) !=
            current->version().db_epoch_ ||
        partition.replication_epoch_ != current->version().replication_epoch_ ||
        partition.grouped_generations_[db_id] !=
            current->version().index_generation_)
      co_return std::nullopt;
    co_return read.status();
  }
  co_return std::optional(std::move(*read));
}

// The lock planner has already found and revalidated this exact page. Keep
// rank out of the general scan/mutation coroutine, whose much larger frame
// and repeated score routing have no work to do for this one-page projection.
Task<absl::StatusOr<SortedSetResult>>
StorageEngine::Impl::ReadLockedSortedSetRank(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const SortedSetOperation& operation, GroupedObject::Handle object,
    std::optional<double> score, std::size_t page_index) {
  SortedSetResult result;
  result.key_exists_ = true;
  const auto& directory = object->ordered_directory();
  result.length_ = directory.root().item_count_;
  if (!score) co_return result;
  if (page_index >= directory.groups().size())
    co_return absl::DataLossError("Sorted Set indexed member missing");
  LAVIK_FAULT_INJECT(
      if (LAVIK_FAULT_MATCHES("LAVIK_FAIL_ZSET_ORDERED_READ_KEY", key) &&
          (std::getenv("LAVIK_FAIL_ZSET_ORDERED_READ_PAGE") == nullptr ||
           LAVIK_FAULT_MATCHES_NTH("LAVIK_FAIL_ZSET_ORDERED_READ_KEY", key,
                                   "LAVIK_FAIL_ZSET_ORDERED_READ_PAGE",
                                   page_index + 1))) co_return absl::
          UnavailableError("injected ordered-page read failure"););
  const auto id = directory.groups()[page_index].id_;
  const RecordIndexValue* physical = nullptr;
  auto refreshed =
      RefreshLockedGroup(partition, db_id, key, object, {id, 0}, &physical);
  if (!refreshed.ok()) co_return refreshed.status();
  GroupedScratchBudget budget;
  auto checked = budget.AddGroup(
      *physical, physical->external() ? (*refreshed)->ExtentsFor({id, 0})
                                      : ExtentManifest{});
  if (!checked.ok()) co_return checked;
  if (shutdown_flush_requested_)
    co_return absl::CancelledError("Sorted Set read cancelled by shutdown");
  auto admission = budget.Reserve(2);
  if (!admission.ok()) co_return admission.status();
  auto page = co_await LoadSortedSetPage(
      store, partition, db_id, key, digest, object, id,
      SortedSetMemberValidation::kTrustStored,
      SortedSetPageReadMode::kIndexedProjection, true, operation.diagnostics_);
  if (!page.ok()) co_return page.status();
  auto slot = FindRankSlot(*page, *score, operation.members_.front());
  if (!slot.ok()) co_return slot.status();
  if (!*slot)
    co_return absl::DataLossError("Sorted Set indexed member missing");
  // Use the common cut captured after all S holds, not the newer physical
  // view used for IO: unrelated writes may change preceding page counts.
  const auto rank = directory.CountBefore(page_index) + **slot;
  result.rank_ = operation.reverse_ ? result.length_ - 1 - rank : rank;
  result.rank_score_ = *score;
  co_return result;
}

Task<absl::StatusOr<std::optional<SortedSetResult>>>
StorageEngine::Impl::TryConcurrentSortedSetPoint(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const SortedSetOperation& operation, ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  const auto valid = Validate(operation);
  if (!valid.ok()) co_return valid;
  auto& store = CurrentStore();
  auto& partition = PartitionForKey(store, key);
  if (!CanPrepareGroupedWriteUnlocked(partition)) co_return std::nullopt;
  const auto population = CaptureCompactWriteSnapshot(store, partition, db_id);
  const auto* found = (partition.indexes_.Get(db_id)
                           ? partition.indexes_[db_id].Find(digest, key)
                           : nullptr);
  const auto now = operation.now_ms_ ? operation.now_ms_ : UnixTimeMillis();
  if (!found || !found->key_complete() || !found->value_.grouped() ||
      found->value_.value_type() != ValueType::kSortedSet ||
      IsExpired(*found, now))
    co_return std::nullopt;
  auto view = partition.grouped_objects_[db_id].Lookup(
      key, GroupedObjectVersion{
               .root_ = MaterializeIndexLocation(*found),
               .db_epoch_ = EffectiveRecordDbEpoch(partition, db_id),
               .replication_epoch_ = partition.replication_epoch_,
               .index_generation_ = partition.grouped_generations_[db_id]});
  if (!view.ok()) co_return view.status();
  auto object = std::move(*view);
  const auto member = operation.kind_ == SortedSetOperationKind::kAdd
                          ? operation.entries_.front().member_
                          : operation.members_.front();
  const auto* route = object->directory().Find(member);
  if (!route) co_return absl::DataLossError("missing member prefix route");
  const auto member_id = route->id_;
  if (!store.group_locks_)
    store.group_locks_ = std::make_unique<GroupedLockTable>(*store.worker_);
  auto holds = store.group_locks_->MakeLockSet(
      {db_id, tx::FingerprintOf(digest), object->incarnation()});
  co_await holds.Acquire({GroupedLockIndex::kMember, member_id},
                         GroupedLockMode::kExclusive);
  // A preceding holder may have updated this leaf while we waited. Rebase
  // before reading, rather than mistaking its revision for a failed read.
  auto current = partition.grouped_objects_[db_id].CurrentForMutation(key);
  if (!current || current->incarnation() != object->incarnation() ||
      !current->has_member_index())
    co_return std::nullopt;
  route = current->directory().Find(member);
  if (!route || route->id_ != member_id) co_return std::nullopt;
  object = std::move(current);
  if (route->encoded_bytes_ >
      kCollectionGroupTargetBytes + kHashGroupHeaderBytes)
    co_return std::nullopt;
  auto input_admission = ReserveInputs(operation);
  if (!input_admission.ok()) co_return input_admission.status();
  Members members;
  auto status = PrepareMembers(operation, &members);
  if (!status.ok()) co_return status;
  auto& state = members.begin()->second;
  SortedSetResult result;
  result.key_exists_ = true;
  result.length_ = object->ordered_directory().root().item_count_;
  GroupedScratchBudget leaf_budget;
  const auto* physical = object->FindGroup(member_id);
  if (!physical) co_return absl::DataLossError("missing member prefix page");
  status = leaf_budget.AddGroup(*physical, object->ExtentsFor(member_id));
  if (!status.ok()) co_return status;
  auto leaf_admission = leaf_budget.Reserve(2);
  if (!leaf_admission.ok()) co_return leaf_admission.status();
  std::optional<SortedSetMemberProbe> probe;
  {
    auto leaf = co_await LoadHashGroupPayload(store, partition, db_id, key,
                                              digest, object, member_id, false,
                                              true, operation.diagnostics_);
    if (!leaf.ok()) co_return leaf.status();
    const auto bytes = leaf->loaded_.value();
    const std::string_view payload(reinterpret_cast<const char*>(bytes.data()),
                                   bytes.size());
    auto point = HashGroupPointProbe::Create(
        payload, object->directory().root().seed_, member);
    if (!point.ok()) co_return point.status();
    if (*point) {
      auto score = DecodeSortedSetMemberScore((**point).value());
      if (!score.ok()) co_return score.status();
      state.before_ = state.after_ = *score;
      probe.emplace(std::move(*leaf_admission), object, HashGroupSnapshot{},
                    std::move(*point));
    }
    status = ApplyInputs(operation, &members, &result);
    if (!status.ok()) co_return status;
    if (state.touched_ && !state.before_) {
      constexpr auto overhead = 8 + sizeof(double);
      const auto base_bytes = std::max(payload.size() - kHashGroupHeaderBytes,
                                       std::size_t(kHashValueHeaderBytes));
      if (member.size() > kCollectionGroupTargetBytes - overhead ||
          base_bytes > kCollectionGroupTargetBytes - overhead - member.size())
        co_return std::nullopt;
      GroupedScratchBudget incoming;
      status = incoming.AddBytes(member.size() + 256);
      if (!status.ok()) co_return status;
      auto incoming_admission = incoming.Reserve(1);
      if (!incoming_admission.ok()) co_return incoming_admission.status();
      const auto score = EncodeSortedSetMemberScore(*state.after_);
      const HashEntryView edit{member, score};
      auto inserted =
          ApplyHashGroupEdits(payload, object->directory().root().seed_,
                              HashGroupEditKind::kSetIfAbsent, {&edit, 1});
      if (!inserted.ok()) co_return inserted.status();
      if (inserted->leaves_.size() != 1 ||
          !inserted->leaves_.front().prepared_ ||
          inserted->leaves_.front().id_ != member_id)
        co_return std::nullopt;
      probe.emplace(std::move(*leaf_admission), object, HashGroupSnapshot{},
                    std::nullopt,
                    SortedSetMemberInsertion{
                        std::move(*incoming_admission), member, *state.after_,
                        std::move(inserted->leaves_.front())});
    }
  }
  auto refreshed = RefreshLockedGroup(partition, db_id, key, object, member_id);
  if (!refreshed.ok()) co_return refreshed.status();
  object = std::move(*refreshed);
  if (!state.touched_) {
    co_await store.store_state_mutex_.Lock();
    UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
    refreshed = RefreshLockedGroup(partition, db_id, key, object, member_id);
    if (!refreshed.ok()) co_return refreshed.status();
    status =
        ValidateGroupedWriteSnapshot(store, partition, db_id, key, *refreshed,
                                     population, mutation_precondition);
    if (!status.ok()) co_return status;
    co_return std::optional(std::move(result));
  }
  // Resident bounds can route ordinary point edits without speculative reads
  // of unlocked pages. Ambiguous equal-score/long-member boundaries use X.
  const auto locate =
      [&](const GroupedObject::Handle& source,
          std::optional<double> score) -> std::optional<std::uint64_t> {
    if (!score) return 0;
    const auto& directory = source->ordered_directory();
    const auto& pages = directory.groups();
    if (pages.empty()) return std::nullopt;
#if LAVIK_FAULTS_ENABLED
    std::size_t probes = 0;
    const auto trace_route = [&](bool fallback) {
      if (LAVIK_FAULT_MATCHES("LAVIK_TRACE_ZSET_POINT_ROUTE_KEY", key))
        spdlog::info("zset point route pages={} probes={} fallback={}",
                     pages.size(), probes, unsigned(fallback));
    };
#endif
    // Page maxima are ordered by (score, binary member). Search the resident
    // bounds directly so a long equal-score run remains O(log G), both before
    // locking and during revalidation. The last page also accepts appends past
    // the current maximum; its member bound need not be compared.
    std::size_t first = 0, last = pages.size() - 1;
    while (first < last) {
      const auto mid = first + (last - first) / 2;
      LAVIK_FAULT_INJECT(++probes;);
      bool less = pages[mid].max_score_ < *score;
      if (pages[mid].max_score_ == *score) {
        const auto cached = pages[mid].max_key_.LessThan(member);
        if (!cached) {
          LAVIK_FAULT_INJECT(trace_route(true););
          return std::nullopt;
        }
        less = *cached;
      }
      if (less)
        first = mid + 1;
      else
        last = mid;
    }
    LAVIK_FAULT_INJECT(trace_route(false););
    return pages[first].id_;
  };
  const auto source_id = locate(object, state.before_);
  auto destination_id = locate(object, state.after_);
  if (!source_id || !destination_id) co_return std::nullopt;
  // Cross-page edits can wait for a cold source without holding the shared
  // destination hot spot. This temporary S hold is released before acquiring
  // the complete ordered lock set; retaining it could invert the global lock
  // order or require an S-to-X upgrade. Only owned bytes survive the hold.
  MemoryReservation source_admission;
  std::optional<LoadedOrderedGroup> prefetched_source;
  if (*source_id && *destination_id && *source_id != *destination_id) {
    {
      auto source_hold = store.group_locks_->MakeLockSet(
          {db_id, tx::FingerprintOf(digest), object->incarnation()});
      co_await source_hold.Acquire(
          {GroupedLockIndex::kOrdered, {*source_id, 0}},
          GroupedLockMode::kShared);
      refreshed = RefreshLockedGroup(partition, db_id, key, object, member_id);
      if (!refreshed.ok()) co_return refreshed.status();
      object = std::move(*refreshed);
      if (locate(object, state.before_) != source_id) co_return std::nullopt;
      physical = object->FindGroup({*source_id, 0});
      if (!physical) co_return absl::DataLossError("missing source point page");
      GroupedScratchBudget budget;
      status = budget.AddGroup(*physical, object->ExtentsFor({*source_id, 0}));
      if (!status.ok()) co_return status;
      // Optional headroom: memory pressure keeps the original locked loader,
      // rather than turning speculative retention into a new command OOM.
      auto admission = TryReserveMemory(budget.bytes());
      if (admission) {
        source_admission = std::move(*admission);
        auto page = co_await LoadEncodedSortedSetGroupSnapshot(
            store, partition, db_id, key, digest, object, *source_id, nullptr,
            true, operation.diagnostics_);
        if (!page.ok()) co_return page.status();
        prefetched_source.emplace(std::move(*page));
      }
    }
    LAVIK_FAULT_INJECT({
      if (prefetched_source &&
          LAVIK_FAULT_MATCHES("LAVIK_ZSET_PREFETCH_PAUSE_MEMBER", member)) {
        spdlog::info("zset source prefetch released member={} page={}", member,
                     *source_id);
        auto paused = co_await fault_injection::PauseWhileFileExists(
            "LAVIK_ZSET_PREFETCH_PAUSE_FILE");
        if (!paused.ok()) co_return paused;
      }
    });
    // No ordered hold remains, so a destination boundary changed during the
    // source read does not require whole-key X. Rebase the unlocked route now;
    // the complete lock set below still revalidates it after any lock waits.
    refreshed = RefreshLockedGroup(partition, db_id, key, object, member_id);
    if (!refreshed.ok()) co_return refreshed.status();
    object = std::move(*refreshed);
    if (locate(object, state.before_) != source_id) co_return std::nullopt;
    destination_id = locate(object, state.after_);
    if (!destination_id) co_return std::nullopt;
  }
  std::map<std::uint64_t, GroupedLockMode> selected;
  for (const auto id : {*source_id, *destination_id}) {
    if (!id) continue;
    const auto* page = object->ordered_directory().Find(id);
    if (!page) co_return absl::DataLossError("missing point route");
    selected[id] = GroupedLockMode::kExclusive;
    for (const auto neighbour : {page->previous_, page->next_})
      if (neighbour) selected.try_emplace(neighbour, GroupedLockMode::kShared);
  }
  // Merge both routes before locking: a destination can also be a source's
  // neighbour, and S-to-X upgrades would invalidate the global lock order.
  for (const auto& [id, mode] : selected) {
    co_await holds.Acquire({GroupedLockIndex::kOrdered, {id, 0}}, mode);
  }
  refreshed = RefreshLockedGroup(partition, db_id, key, object, member_id);
  if (!refreshed.ok()) co_return refreshed.status();
  object = std::move(*refreshed);
  if (locate(object, state.before_) != source_id ||
      locate(object, state.after_) != destination_id)
    co_return std::nullopt;
  LAVIK_FAULT_INJECT({
    if (LAVIK_FAULT_MATCHES("LAVIK_ZSET_GROUP_PAUSE_MEMBER", member)) {
      spdlog::info("zset group locks held before page IO member={}", member);
      auto paused = co_await fault_injection::PauseWhileFileExists(
          "LAVIK_ZSET_GROUP_PAUSE_FILE");
      if (!paused.ok()) co_return paused;
    }
  });
  // Topology changes require X, so the selected neighbours still bound these
  // pages. Their shared holds protect boundary validation until publication.
  probe->source_ = object;
  const auto delta =
      int(state.after_.has_value()) - int(state.before_.has_value());
  result.length_ = object->ordered_directory().root().item_count_ + delta;
  if (result.length_ == 0) co_return std::nullopt;
  OrderedCollectionMutationPlan plan{
      .root_ = object->ordered_directory().root(),
      .expected_sequence_ = object->revision(),
      .changed_ = true,
      .writes_ = {}};
  plan.root_.item_count_ = result.length_;
  GroupedScratchBudget pages_budget;
  for (const auto& [id, mode] : selected) {
    if (mode != GroupedLockMode::kExclusive) continue;
    physical = object->FindGroup({id, 0});
    if (!physical) co_return absl::DataLossError("missing ordered point page");
    status = pages_budget.AddGroup(*physical, object->ExtentsFor({id, 0}));
    if (!status.ok()) co_return status;
  }
  status = pages_budget.AddBytes(member.size() + 256);
  if (!status.ok()) co_return status;
  // Speculation must not consume headroom required by the original planner.
  // If the normal scratch would no longer fit, discard the optional source
  // before ordinary admission, without recording a rejected command.
  std::optional<MemoryReservation> prepaid_pages;
  if (prefetched_source &&
      pages_budget.bytes() <= std::numeric_limits<std::size_t>::max() / 4) {
    prepaid_pages = TryReserveMemory(pages_budget.bytes() * 4);
    if (!prepaid_pages) {
      prefetched_source.reset();
      source_admission.Release();
    }
  }
  auto pages_admission =
      prepaid_pages
          ? absl::StatusOr<MemoryReservation>(std::move(*prepaid_pages))
          : pages_budget.Reserve(4);
  if (!pages_admission.ok()) co_return pages_admission.status();
  std::uint64_t protected_bytes = 0;
  for (const auto& [id, mode] : selected) {
    if (mode == GroupedLockMode::kShared) {
      protected_bytes += object->ordered_directory().Find(id)->encoded_bytes_;
      continue;
    }
    LAVIK_FAULT_INJECT({
      // The ordinary planner already owns its source probe at this boundary;
      // keep this hook specific to additional destination-page reads.
      const auto ordinal = object->ordered_directory().FindIndex(id);
      if (id != *source_id &&
          LAVIK_FAULT_MATCHES_NTH("LAVIK_FAIL_ZSET_PLAN_READ_KEY", key,
                                  "LAVIK_FAIL_ZSET_PLAN_READ_PAGE",
                                  *ordinal + 1))
        co_return absl::UnavailableError(
            "injected Sorted Set plan page read failure");
    });
    const auto* current_page = object->FindGroup({id, 0});
    const bool reuse_source =
        prefetched_source && id == *source_id &&
        prefetched_source->sequence_ == current_page->mutation_sequence_;
    if (reuse_source) {
      // Other page loads may have suspended since the common routing check.
      // Preserve the ordinary loader's population and failed-decision checks.
      auto checked = RefreshLockedGroup(partition, db_id, key, object, {id, 0});
      if (!checked.ok()) co_return checked.status();
    }
    LAVIK_FAULT_INJECT({
      if (prefetched_source && id == *source_id &&
          LAVIK_FAULT_MATCHES("LAVIK_TRACE_ZSET_PREFETCH_MEMBER", member))
        spdlog::info("zset source prefetch reuse={} member={}",
                     unsigned(reuse_source), member);
    });
    // A writer may have changed another member in our source page while its
    // S hold was released. Reload under X when the logical version changed;
    // physical relocation alone does not invalidate this owned snapshot.
    absl::StatusOr<LoadedOrderedGroup> page =
        reuse_source
            ? absl::StatusOr<LoadedOrderedGroup>(std::move(*prefetched_source))
            : co_await LoadEncodedSortedSetGroupSnapshot(
                  store, partition, db_id, key, digest, object, id, nullptr,
                  true, operation.diagnostics_);
    if (!page.ok()) co_return page.status();
    auto edited = EditEncodedSortedSetGroup(
        page->snapshot_, member,
        id == *source_id ? state.before_ : std::nullopt,
        id == *destination_id ? state.after_ : std::nullopt,
        kCollectionGroupTargetBytes, SortedSetMemberAbsence::kVerified);
    if (!edited.ok()) co_return edited.status();
    if (!*edited) co_return std::nullopt;
    auto encoder = OrderedGroupEncoder::Create(**edited);
    if (!encoder.ok()) co_return encoder.status();
    protected_bytes += encoder->encoded_bytes();
    plan.writes_.push_back(std::move(**edited));
  }
  // Other IX writers may shrink distant payloads during our append wait,
  // but retiring a page requires key X. Include their persistent headers in
  // the lower bound: half-filled protected pages alone can fall just short
  // of the threshold even for a large collection. The common header size
  // also covers older page layouts without the v2 offset sentinel.
  protected_bytes +=
      (object->ordered_directory().groups().size() - selected.size()) *
      std::uint64_t{kOrderedGroupHeaderBytes};
  // This bound prevents a late compact conversion from needing an X retry
  // after AppendLocked has canonicalized the replication command.
  if (protected_bytes < kCollectionDemotionBytes) co_return std::nullopt;
  const SortedSetMemberChange change{member, state.before_, state.after_};
  auto member_plan = co_await PrepareSortedSetMembers(
      store, partition, db_id, key, digest, object, plan, true,
      std::span(&change, 1), &*probe);
  if (!member_plan.ok()) co_return member_plan.status();
  co_await store.store_state_mutex_.Lock();
  UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
  refreshed = RefreshLockedGroup(partition, db_id, key, object, member_id);
  if (!refreshed.ok()) co_return refreshed.status();
  status =
      ValidateGroupedWriteSnapshot(store, partition, db_id, key, *refreshed,
                                   population, mutation_precondition);
  if (!status.ok()) co_return status;
  bool retry_exclusive = false;
  status = co_await CommitGroupedOrderedMutationLocked(
      store, partition, db_id, key, digest, object, std::move(plan),
      object->version().root_.expire_at_ms_, nullptr, replication,
      mutation_precondition, &*member_plan, &retry_exclusive,
      operation.diagnostics_);
  if (retry_exclusive) co_return std::nullopt;
  if (!status.ok()) co_return status;
  co_return std::optional(std::move(result));
}

Task<absl::StatusOr<std::optional<SortedSetResult>>>
StorageEngine::Impl::TryConcurrentSortedSetBatch(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const SortedSetOperation& operation, ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition,
    const SortedSetSelectionGuard* selection) {
  const auto valid = Validate(operation);
  if (!valid.ok()) co_return valid;
  auto& store = CurrentStore();
  auto& partition = PartitionForKey(store, key);
  if (!CanPrepareGroupedWriteUnlocked(partition)) co_return std::nullopt;
  const auto population = CaptureCompactWriteSnapshot(store, partition, db_id);
  const auto* found = (partition.indexes_.Get(db_id)
                           ? partition.indexes_[db_id].Find(digest, key)
                           : nullptr);
  if (!found || !found->key_complete() || !found->value_.grouped() ||
      found->value_.value_type() != ValueType::kSortedSet ||
      IsExpired(*found,
                operation.now_ms_ ? operation.now_ms_ : UnixTimeMillis()))
    co_return std::nullopt;
  auto view = partition.grouped_objects_[db_id].Lookup(
      key, GroupedObjectVersion{
               .root_ = MaterializeIndexLocation(*found),
               .db_epoch_ = EffectiveRecordDbEpoch(partition, db_id),
               .replication_epoch_ = partition.replication_epoch_,
               .index_generation_ = partition.grouped_generations_[db_id]});
  if (!view.ok()) co_return view.status();
  auto object = std::move(*view);
  if (!object || !object->has_member_index()) co_return std::nullopt;
  const auto incarnation = object->incarnation();
  auto inputs = ReserveInputs(operation);
  if (!inputs.ok()) co_return inputs.status();
  Members members;
  auto status = PrepareMembers(operation, &members);
  if (!status.ok()) co_return status;
  // An unconditional batch wholly beyond one end of the ordered graph
  // sends every distinct operand to the same endpoint. If the scores alone
  // exceed a page, the fixed-topology attempt cannot succeed: avoid reading
  // all member leaves before discovering that and retrying under key X.
  // Use the deduplicated map, strict bounds, and no conditional/increment
  // flags. The ordinary exclusive planner still validates and applies the
  // full operation against its newly acquired view; this is only a hint.
  if (operation.kind_ == SortedSetOperationKind::kAdd && !operation.nx_ &&
      !operation.xx_ && !operation.gt_ && !operation.lt_ &&
      !operation.increment_ && !operation.reject_existing_ &&
      members.size() > kCollectionGroupTargetBytes / sizeof(double)) {
    const auto& pages = object->ordered_directory().groups();
    if (!pages.empty()) {
      bool above = true, below = true;
      for (const auto& entry : operation.entries_) {
        above = above && entry.score_ > pages.back().max_score_;
        below = below && entry.score_ < pages.front().min_score_;
        if (!above && !below) break;
      }
      if (above || below) co_return std::nullopt;
    }
  }
  LAVIK_FAULT_INJECT(
      if (LAVIK_FAULT_MATCHES("LAVIK_FAIL_ZSET_CONCURRENT_BATCH_PROBE_KEY",
                              key)) co_return absl::
          UnavailableError("injected concurrent batch member probe failure"););
  // In addition to the input map, admit the member/ordered lock plans and
  // semantic deltas. Each member contributes at most two pages and four
  // neighbours; no payload-sized cache is retained while reading leaves.
  auto lock_admission = TryReserveMemory(members.size() * 512);
  if (!lock_admission) co_return MemoryLimitError("OOM Sorted Set batch locks");
  std::set<GroupedRecordId> member_groups;
  for (const auto& [member, state] : members) {
    const auto* route = object->directory().Find(member);
    if (!route) co_return absl::DataLossError("missing member prefix route");
    if (route->encoded_bytes_ >
        kCollectionGroupTargetBytes + kHashGroupHeaderBytes)
      co_return std::nullopt;
    member_groups.insert(route->id_);
  }
  if (!store.group_locks_)
    store.group_locks_ = std::make_unique<GroupedLockTable>(*store.worker_);
  auto holds = store.group_locks_->MakeLockSet(
      {db_id, tx::FingerprintOf(digest), object->incarnation()});
  for (const auto id : member_groups) {
    co_await holds.Acquire({GroupedLockIndex::kMember, id},
                           GroupedLockMode::kExclusive);
  }
  auto current = partition.grouped_objects_[db_id].CurrentForMutation(key);
  if (!current || current->incarnation() != object->incarnation() ||
      current->version().db_epoch_ != object->version().db_epoch_ ||
      current->version().replication_epoch_ !=
          object->version().replication_epoch_ ||
      current->version().index_generation_ !=
          object->version().index_generation_)
    co_return std::nullopt;
  object = std::move(current);
  for (const auto& [member, state] : members) {
    const auto* route = object->directory().Find(member);
    if (!route || !member_groups.contains(route->id_)) co_return std::nullopt;
  }
  SortedSetPageWorkBudget work;
  for (const auto id : member_groups) {
    auto physical = RefreshLockedGroup(partition, db_id, key, object, id);
    if (!physical.ok()) co_return physical.status();
    GroupedScratchBudget budget;
    status = budget.AddGroup(*(*physical)->FindGroup(id),
                             (*physical)->ExtentsFor(id));
    if (!status.ok()) co_return status;
    auto admission = budget.Reserve(2);
    if (!admission.ok()) co_return admission.status();
    if (work.Add(budget.bytes())) co_await bycorf::Yield(*store.worker_);
    auto leaf = co_await LoadHashGroupPayload(store, partition, db_id, key,
                                              digest, object, id, false, true,
                                              operation.diagnostics_);
    if (!leaf.ok()) co_return leaf.status();
    const auto bytes = leaf->loaded_.value();
    const std::string_view payload(reinterpret_cast<const char*>(bytes.data()),
                                   bytes.size());
    status = VisitHashGroupFields(
        payload, leaf->field_count_, id, object->directory().root().seed_,
        [&](const HashEntryView& entry) {
          const auto requested = members.find(entry.field_);
          if (requested == members.end()) return absl::OkStatus();
          auto score = DecodeSortedSetMemberScore(entry.value_);
          if (!score.ok()) return score.status();
          requested->second.before_ = requested->second.after_ = *score;
          return absl::OkStatus();
        });
    if (!status.ok()) co_return status;
  }
  SortedSetResult result;
  result.key_exists_ = true;
  result.length_ = object->ordered_directory().root().item_count_;
  status = ApplyInputs(operation, &members, &result);
  if (!status.ok()) co_return status;
  std::vector<SortedSetMemberChange> changes;
  changes.reserve(members.size());
  std::int64_t delta = 0;
  for (auto& [member, state] : members) {
    // Preserve a duplicate operand's round trip to its original score as a
    // write: the reply and WATCH invalidation still reflect those changes.
    if (!state.touched_) continue;
    changes.push_back({member, state.before_, state.after_});
    delta += int(state.after_.has_value()) - int(state.before_.has_value());
  }
  object = partition.grouped_objects_[db_id].CurrentForMutation(key);
  if (!object || object->incarnation() != incarnation) co_return std::nullopt;
  if (selection && changes.size() != members.size()) co_return std::nullopt;
  if (changes.empty()) {
    co_await store.store_state_mutex_.Lock();
    UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
    current = partition.grouped_objects_[db_id].CurrentForMutation(key);
    status = ValidateGroupedWriteSnapshot(store, partition, db_id, key, current,
                                          population, mutation_precondition);
    if (!status.ok()) co_return status;
    result.length_ = current->ordered_directory().root().item_count_;
    co_return std::optional(std::move(result));
  }
  const auto locate =
      [&](const GroupedObject::Handle& source, std::string_view member,
          std::optional<double> score) -> std::optional<std::uint64_t> {
    if (!score) return 0;
    const auto& pages = source->ordered_directory().groups();
    if (pages.empty()) return std::nullopt;
    std::size_t first = 0, last = pages.size() - 1;
    while (first < last) {
      const auto mid = first + (last - first) / 2;
      bool less = pages[mid].max_score_ < *score;
      if (pages[mid].max_score_ == *score) {
        const auto bound = pages[mid].max_key_.LessThan(member);
        if (!bound) return std::nullopt;
        less = *bound;
      }
      if (less)
        first = mid + 1;
      else
        last = mid;
    }
    return pages[first].id_;
  };
  std::map<std::uint64_t, GroupedLockMode> ordered_groups;
  for (auto& [member, state] : members) {
    if (!state.touched_) continue;
    const auto source = locate(object, member, state.before_);
    const auto destination = locate(object, member, state.after_);
    if (!source || !destination) co_return std::nullopt;
    // These are page identities, not ordinal positions. Topology remains
    // fixed under IX, but score boundaries still need post-wait validation.
    state.source_ = *source;
    state.destination_ = *destination;
    for (const auto id : {*source, *destination}) {
      if (!id) continue;
      const auto* page = object->ordered_directory().Find(id);
      if (!page || page->encoded_bytes_ > kCollectionGroupTargetBytes)
        co_return std::nullopt;
      ordered_groups[id] = GroupedLockMode::kExclusive;
      for (const auto neighbour : {page->previous_, page->next_})
        if (neighbour)
          ordered_groups.try_emplace(neighbour, GroupedLockMode::kShared);
    }
  }
  // Selection predicates need protection too. Rank and mixed-score lexical
  // removal depend on the whole ordered graph. Score removal and endpoint
  // pops can fence their candidate interval plus neighbouring boundaries.
  MemoryReservation predicate_admission;
  std::vector<std::uint64_t> predicate_groups;
  if (selection) {
    const auto& source = selection->source_;
    if (source->incarnation() != object->incarnation()) co_return std::nullopt;
    const auto& directory = source->ordered_directory();
    const auto& pages = directory.groups();
    const auto& selected = selection->operation_;
    std::size_t first = 0, end = pages.size();
    if (selected.kind_ == SortedSetOperationKind::kPop) {
      const auto length = directory.root().item_count_;
      const auto count = std::min(selected.pop_count_, length);
      if (!count) co_return std::nullopt;
      first = directory.FindRank(selected.reverse_ ? length - count : 0)
                  ->group_index_;
      end = directory.FindRank(selected.reverse_ ? length - 1 : count - 1)
                ->group_index_ +
            1;
    } else if (selected.range_mode_ == SortedSetRangeMode::kScore) {
      first = directory.LowerBoundScore(selected.minimum_score_.value_,
                                        selected.minimum_score_.exclusive_);
      end = directory.UpperBoundScore(selected.maximum_score_.value_,
                                      selected.maximum_score_.exclusive_);
      if (first >= end) co_return std::nullopt;
    }
    if (first) --first;
    if (end < pages.size()) ++end;
    if (end - first > SIZE_MAX / 128)
      co_return absl::ResourceExhaustedError(
          "Sorted Set predicate locks overflow");
    auto reserved = TryReserveMemory((end - first) * 128);
    if (!reserved) co_return MemoryLimitError("OOM Sorted Set predicate locks");
    predicate_admission = std::move(*reserved);
    predicate_groups.reserve(end - first);
    for (auto i = first; i < end; ++i) {
      predicate_groups.push_back(pages[i].id_);
      ordered_groups.try_emplace(pages[i].id_, GroupedLockMode::kShared);
    }
  }
  for (const auto& [id, mode] : ordered_groups) {
    co_await holds.Acquire({GroupedLockIndex::kOrdered, {id, 0}}, mode);
  }
  for (const auto id : member_groups) {
    auto refreshed = RefreshLockedGroup(partition, db_id, key, object, id);
    if (!refreshed.ok()) co_return refreshed.status();
    current = std::move(*refreshed);
  }
  object = std::move(current);
  for (const auto& [member, state] : members) {
    if (!state.touched_) continue;
    if (locate(object, member, state.before_) != state.source_ ||
        locate(object, member, state.after_) != state.destination_)
      co_return std::nullopt;
  }
  if (selection) {
    for (const auto id : predicate_groups) {
      const auto checked = RefreshLockedGroup(partition, db_id, key,
                                              selection->source_, {id, 0});
      if (!checked.ok()) {
        if (absl::IsAborted(checked.status())) co_return std::nullopt;
        co_return checked.status();
      }
    }
    // The selection's relevant pages still describe exactly the values we
    // selected. Retaining their S/X holds also prevents phantoms until commit,
    // so replaying the original predicate in publication order selects the
    // same members. Unrelated root revisions do not force a retry.
  }
  LAVIK_FAULT_INJECT({
    for (const auto& [member, state] : members) {
      if (!LAVIK_FAULT_MATCHES("LAVIK_ZSET_GROUP_PAUSE_MEMBER", member))
        continue;
      spdlog::info("zset batch group locks held before page IO");
      const auto paused = co_await fault_injection::PauseWhileFileExists(
          "LAVIK_ZSET_GROUP_PAUSE_FILE");
      if (!paused.ok()) co_return paused;
      break;
    }
  });
  const auto length =
      std::int64_t(object->ordered_directory().root().item_count_) + delta;
  if (length <= 0 || length > std::numeric_limits<std::uint32_t>::max())
    co_return std::nullopt;
  result.length_ = length;
  OrderedCollectionMutationPlan plan{
      .root_ = object->ordered_directory().root(),
      .expected_sequence_ = object->revision(),
      .changed_ = true,
      .writes_ = {}};
  plan.root_.item_count_ = result.length_;
  GroupedScratchBudget budget;
  for (const auto& [id, mode] : ordered_groups) {
    if (mode != GroupedLockMode::kExclusive) continue;
    auto physical = RefreshLockedGroup(partition, db_id, key, object, {id, 0});
    if (!physical.ok()) co_return physical.status();
    status = budget.AddGroup(*(*physical)->FindGroup({id, 0}),
                             (*physical)->ExtentsFor({id, 0}));
    if (!status.ok()) co_return status;
  }
  for (const auto& change : changes) {
    status = budget.AddBytes(change.member_.size() + 256);
    if (!status.ok()) co_return status;
  }
  auto pages_admission = budget.Reserve(4);
  if (!pages_admission.ok()) co_return pages_admission.status();
  std::uint64_t protected_bytes = 0;
  for (const auto& [id, mode] : ordered_groups) {
    if (mode == GroupedLockMode::kShared) {
      protected_bytes += object->ordered_directory().Find(id)->encoded_bytes_;
      continue;
    }
    if (work.Add(kCollectionGroupTargetBytes))
      co_await bycorf::Yield(*store.worker_);
    auto page = co_await LoadOrderedGroupSnapshot(
        store, partition, db_id, key, digest, object, id, false, nullptr,
        SortedSetMemberValidation::kVerifyUnique, true, operation.diagnostics_);
    if (!page.ok()) co_return page.status();
    auto& entries = page->snapshot_.entries_;
    std::size_t found_sources = 0, expected_sources = 0;
    // Process the old page once. Repeated point splices would turn a dense
    // batch into O(changes * page size) copying and validation.
    const auto end =
        std::remove_if(entries.begin(), entries.end(), [&](auto& entry) {
          const auto requested = members.find(entry.value_);
          if (requested == members.end() || !requested->second.touched_)
            return false;
          const auto& state = requested->second;
          if (state.source_ != id || !state.before_ ||
              entry.score_ != *state.before_) {
            status = absl::DataLossError("Sorted Set batch source mismatch");
            return false;
          }
          ++found_sources;
          if (state.destination_ == id) {
            entry.score_ = *state.after_;
            return false;
          }
          return true;
        });
    if (!status.ok()) co_return status;
    entries.erase(end, entries.end());
    for (const auto& change : changes) {
      const auto& state = members.at(change.member_);
      expected_sources += state.source_ == id;
      if (state.destination_ == id && state.source_ != id)
        entries.push_back({std::string(change.member_), *state.after_});
    }
    if (found_sources != expected_sources)
      co_return absl::DataLossError("Sorted Set batch member missing");
    if (entries.empty()) co_return std::nullopt;
    std::sort(entries.begin(), entries.end(), [](const auto& a, const auto& b) {
      return a.score_ < b.score_ ||
             (a.score_ == b.score_ && a.value_ < b.value_);
    });
    auto encoder = OrderedGroupEncoder::Create(page->snapshot_);
    if (!encoder.ok()) co_return encoder.status();
    if (encoder->encoded_bytes() > kCollectionGroupTargetBytes)
      co_return std::nullopt;
    protected_bytes += encoder->encoded_bytes();
    plan.writes_.push_back(std::move(page->snapshot_));
  }
  // IX preserves the untouched pages' headers even when another writer
  // removes their members. Keep the same late-demotion proof as point writes.
  protected_bytes +=
      (object->ordered_directory().groups().size() - ordered_groups.size()) *
      std::uint64_t{kOrderedGroupHeaderBytes};
  if (protected_bytes < kCollectionDemotionBytes) co_return std::nullopt;
  auto member_plan = co_await PrepareSortedSetMembers(
      store, partition, db_id, key, digest, object, plan, true,
      std::span<const SortedSetMemberChange>(changes), nullptr, true);
  if (!member_plan.ok()) co_return member_plan.status();
  co_await store.store_state_mutex_.Lock();
  UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
  for (const auto id : member_groups) {
    auto refreshed = RefreshLockedGroup(partition, db_id, key, object, id);
    if (!refreshed.ok()) co_return refreshed.status();
    current = std::move(*refreshed);
  }
  status = ValidateGroupedWriteSnapshot(store, partition, db_id, key, current,
                                        population, mutation_precondition);
  if (!status.ok()) co_return status;
  bool retry_exclusive = false;
  status = co_await CommitGroupedOrderedMutationLocked(
      store, partition, db_id, key, digest, object, std::move(plan),
      object->version().root_.expire_at_ms_, nullptr, replication,
      mutation_precondition, &*member_plan, &retry_exclusive,
      operation.diagnostics_);
  if (retry_exclusive) co_return std::nullopt;
  if (!status.ok()) co_return status;
  co_return std::optional(std::move(result));
}

Task<absl::StatusOr<std::optional<SortedSetResult>>>
StorageEngine::Impl::TryConcurrentSortedSetSelection(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const SortedSetOperation& operation, ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  const auto valid = Validate(operation);
  if (!valid.ok()) co_return valid;
  auto& store = CurrentStore();
  auto& partition = PartitionForKey(store, key);
  if (!CanPrepareGroupedWriteUnlocked(partition)) co_return std::nullopt;
  const auto population = CaptureCompactWriteSnapshot(store, partition, db_id);
  const auto* found = (partition.indexes_.Get(db_id)
                           ? partition.indexes_[db_id].Find(digest, key)
                           : nullptr);
  if (!found || !found->key_complete() || !found->value_.grouped() ||
      found->value_.value_type() != ValueType::kSortedSet ||
      IsExpired(*found,
                operation.now_ms_ ? operation.now_ms_ : UnixTimeMillis()))
    co_return std::nullopt;
  auto view = partition.grouped_objects_[db_id].Lookup(
      key, GroupedObjectVersion{
               .root_ = MaterializeIndexLocation(*found),
               .db_epoch_ = EffectiveRecordDbEpoch(partition, db_id),
               .replication_epoch_ = partition.replication_epoch_,
               .index_generation_ = partition.grouped_generations_[db_id]});
  if (!view.ok()) co_return view.status();
  if (!*view || !(*view)->has_member_index()) co_return std::nullopt;
  SortedSetSelectionGuard selection{*view, operation};
  auto query = operation;
  query.kind_ = SortedSetOperationKind::kRange;
  query.packed_output_ = false;
  if (operation.kind_ == SortedSetOperationKind::kPop) {
    const auto count = std::min(
        operation.pop_count_, (*view)->ordered_directory().root().item_count_);
    if (!count) co_return std::nullopt;
    query.range_mode_ = SortedSetRangeMode::kRank;
    query.first_ = 0;
    query.last_ = count - 1;
  }
  // A guaranteed empty ordered page requires topology changes and key X.
  // Detect it from resident counts/bounds before copying a potentially large
  // selection that the exclusive planner would then have to read again.
  const auto& directory = (*view)->ordered_directory();
  const auto& pages = directory.groups();
  if (query.range_mode_ == SortedSetRangeMode::kRank) {
    auto [low, high] =
        RankSlice(query.first_, query.last_, directory.root().item_count_);
    if (low < high) {
      if (query.reverse_) {
        const auto length = directory.root().item_count_;
        const auto first = length - high;
        high = length - low;
        low = first;
      }
      const auto first = *directory.FindRank(low);
      const auto last = *directory.FindRank(high - 1);
      const bool starts_page = first.offset_ == 0;
      const bool ends_page =
          last.offset_ + 1 == pages[last.group_index_].item_count_;
      if (first.group_index_ == last.group_index_
              ? starts_page && ends_page
              : starts_page || ends_page ||
                    last.group_index_ > first.group_index_ + 1)
        co_return std::nullopt;
    }
  } else if (query.range_mode_ == SortedSetRangeMode::kScore) {
    const auto first = directory.LowerBoundScore(
        query.minimum_score_.value_, query.minimum_score_.exclusive_);
    const auto end = directory.UpperBoundScore(query.maximum_score_.value_,
                                               query.maximum_score_.exclusive_);
    const auto whole = [&](std::size_t i) {
      const auto low = query.minimum_score_, high = query.maximum_score_;
      return (low.exclusive_ ? pages[i].min_score_ > low.value_
                             : pages[i].min_score_ >= low.value_) &&
             (high.exclusive_ ? pages[i].max_score_ < high.value_
                              : pages[i].max_score_ <= high.value_);
    };
    if (first < end && (end - first > 2 || whole(first) || whole(end - 1)))
      co_return std::nullopt;
  }
  // Selection owns S groups only inside this awaited task. Release all of
  // them before acquiring member X followed by ordered X/S: no S->X upgrade,
  // descending acquisition, or retained selection lock can form a cycle.
  auto selected =
      co_await TryConcurrentSortedSetRead(db_id, key, digest, query);
  if (!selected.ok()) co_return selected.status();
  if (!*selected) co_return std::nullopt;
  auto result = std::move(**selected);
  if (result.members_.empty()) {
    co_await store.store_state_mutex_.Lock();
    UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
    const auto current =
        partition.grouped_objects_[db_id].CurrentForMutation(key);
    const auto checked =
        ValidateGroupedWriteSnapshot(store, partition, db_id, key, current,
                                     population, mutation_precondition);
    if (!checked.ok()) co_return checked;
    co_return std::optional(std::move(result));
  }
  LAVIK_FAULT_INJECT({
    if (LAVIK_FAULT_MATCHES("LAVIK_ZSET_SELECTION_PAUSE_KEY", key)) {
      spdlog::info("zset selection released before mutation locks");
      const auto paused = co_await fault_injection::PauseWhileFileExists(
          "LAVIK_ZSET_SELECTION_PAUSE_FILE");
      if (!paused.ok()) co_return paused;
    }
  });
  if (result.members_.size() > SIZE_MAX / sizeof(std::string_view))
    co_return absl::ResourceExhaustedError("Sorted Set selection overflow");
  auto admission =
      TryReserveMemory(result.members_.size() * sizeof(std::string_view));
  if (!admission) co_return MemoryLimitError("OOM Sorted Set selected members");
  std::vector<std::string_view> members;
  members.reserve(result.members_.size());
  for (const auto& member : result.members_) members.push_back(member.member_);
  SortedSetOperation remove{.diagnostics_ = operation.diagnostics_,
                            .kind_ = SortedSetOperationKind::kRemove,
                            .members_ = members};
  remove.now_ms_ = operation.now_ms_;
  auto deleted = co_await TryConcurrentSortedSetBatch(
      db_id, key, digest, remove, replication, mutation_precondition,
      &selection);
  if (!deleted.ok()) co_return deleted.status();
  if (!*deleted) co_return std::nullopt;
  result.changed_ = (**deleted).changed_;
  result.length_ = (**deleted).length_;
  if (operation.kind_ == SortedSetOperationKind::kRemoveRange)
    result.members_.clear();
  co_return std::optional(std::move(result));
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
  TraceSpanScope store_wait(operation.diagnostics_, TraceStage::kStoreLock);
  co_await store.store_state_mutex_.Lock();
  store_wait.End();
  UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
  TraceSpanScope lookup_scope(operation.diagnostics_, TraceStage::kLookup);
  auto* index = partition.indexes_.Get(db_id);
  auto* found = index ? index->Find(digest, key) : nullptr;
  if (found && !found->key_complete()) {
    auto verified = co_await FindVerifiedEntry(store, *index, digest, key);
    if (!verified.ok()) co_return verified.status();
    found = *verified;
  }
  lookup_scope.End();
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
    co_return WrongTypeError();
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
            mutation_precondition, &members, nullptr, operation.diagnostics_);
        if (!committed.ok()) co_return committed;
      }
      co_return prepared;
    }
    // As with Hash reads, retain the shared key intent but release the store
    // mutex before a potentially long scan. Page readers revalidate this view.
    if (ReadOnly(operation)) unlock.Unlock();
    const auto version =
        (*view) ? std::optional((*view)->version()) : std::nullopt;
    // Dispatch score-only reads before entering the general mutation/range
    // coroutine. The non-coroutine selector adds no forwarding frame, while
    // the common population-change handling below remains in force.
    auto read = co_await [&]() -> Task<absl::StatusOr<SortedSetResult>> {
      if (operation.kind_ == SortedSetOperationKind::kScores)
        return ReadGroupedSortedSetScores(store, partition, db_id, key, digest,
                                          operation, std::move(*view));
      return ExecuteGroupedSortedSetLocked(store, partition, db_id, key, digest,
                                           operation, std::move(*view), tx,
                                           replication, mutation_precondition);
    }();
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
  OrderedCompactPayload point_payload;
  std::optional<InlineRecordPayload> inline_payload;
  auto callback = [&](std::optional<CompactValueView> value)
      -> absl::StatusOr<CompactValueUpdate> {
    auto update = ApplyCompactSortedSet(value, operation, members, result,
                                        compact_admission, &point_payload);
    if (update.ok() && !point_payload.empty()) {
      inline_payload.emplace(point_payload);
      update->inline_payload_ = &*inline_payload;
    }
    return update;
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
      tx, now, replication, prepare_unlocked, mutation_precondition,
      {.diagnostics_ = operation.diagnostics_});
  if (!status.ok()) co_return status;
  co_return result;
}

/// Reads scores from a caller-protected view without acquiring key/group locks.
/// The caller retains key S/X, or IS plus every requested member S when
/// group_locked is true, through completion. Each checked leaf is decoded into
/// owned scores before its read lease ends, preserving request order,
/// duplicates and missing members. Admission, IO and validation failures return
/// an error instead of a partial reply.
Task<absl::StatusOr<SortedSetResult>>
StorageEngine::Impl::ReadGroupedSortedSetScores(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const SortedSetOperation& operation, GroupedObject::Handle object,
    bool group_locked) {
  // Keep batch plans/windows out of the single-score coroutine frame. This
  // forwarding function does not suspend and allocates no frame of its own.
  if (operation.members_.size() == 1)
    return ReadGroupedSortedSetSingleScore(store, partition, db_id, key, digest,
                                           operation, std::move(object),
                                           group_locked);
  return ReadGroupedSortedSetScoreBatch(store, partition, db_id, key, digest,
                                        operation, std::move(object),
                                        group_locked);
}

Task<absl::StatusOr<SortedSetResult>>
StorageEngine::Impl::ReadGroupedSortedSetSingleScore(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const SortedSetOperation& operation, GroupedObject::Handle object,
    bool group_locked) {
  assert(operation.members_.size() == 1);
  if (!object || !object->is_ordered() ||
      object->ordered_directory().root().kind_ !=
          OrderedCollectionKind::kSortedSet)
    co_return absl::DataLossError("invalid grouped Sorted Set view");
  SortedSetResult result;
  result.key_exists_ = true;
  result.length_ = object->ordered_directory().root().item_count_;
  const auto fields = operation.members_;
  auto status = PrepareScoreReply(fields.size(), &result);
  if (!status.ok()) co_return status;
  if (shutdown_flush_requested_)
    co_return absl::CancelledError("member read cancelled by shutdown");
  const auto* route = object->directory().Find(fields.front());
  if (!route) co_return absl::DataLossError("missing member prefix route");
  HashFieldLookup request{.group_ = route->id_,
                          .field_ = fields.front(),
                          .result_index_ = 0,
                          .value_ = std::nullopt};
  auto loaded = co_await LoadSortedSetMemberScores(
      store, partition, db_id, key, digest, object, std::span(&request, 1),
      result.scores_, nullptr, group_locked, operation.diagnostics_);
  if (!loaded.ok()) co_return loaded.status();
  co_return result;
}

Task<absl::StatusOr<SortedSetResult>>
StorageEngine::Impl::ReadGroupedSortedSetScoreBatch(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const SortedSetOperation& operation, GroupedObject::Handle object,
    bool group_locked) {
  if (!object || !object->is_ordered() ||
      object->ordered_directory().root().kind_ !=
          OrderedCollectionKind::kSortedSet)
    co_return absl::DataLossError("invalid grouped Sorted Set view");
  SortedSetResult result;
  result.key_exists_ = true;
  result.length_ = object->ordered_directory().root().item_count_;
  const auto fields = operation.members_;
  auto status = PrepareScoreReply(fields.size(), &result);
  if (!status.ok()) co_return status;
  if (fields.empty()) co_return result;
  if (shutdown_flush_requested_)
    co_return absl::CancelledError("member read cancelled by shutdown");
  if (fields.size() > SIZE_MAX / sizeof(HashFieldLookup) ||
      fields.size() > std::vector<HashFieldLookup>{}.max_size()) {
    RecordMemoryRejection();
    co_return absl::ResourceExhaustedError("Sorted Set lookup plan overflow");
  }
  auto scratch = TryReserveMemory(fields.size() * sizeof(HashFieldLookup));
  if (!scratch) {
    RecordMemoryRejection();
    co_return absl::ResourceExhaustedError("OOM Sorted Set lookup plan");
  }
  std::vector<HashFieldLookup> requests;
  requests.reserve(fields.size());
  if (scratch->bytes() &&
      requests.capacity() > scratch->bytes() / sizeof(HashFieldLookup)) {
    RecordMemoryRejection();
    co_return absl::ResourceExhaustedError("OOM Sorted Set lookup plan");
  }
  for (std::size_t i = 0; i < fields.size(); ++i) {
    const auto* route = object->directory().Find(fields[i]);
    if (!route) co_return absl::DataLossError("missing member prefix route");
    requests.push_back({.group_ = route->id_,
                        .field_ = fields[i],
                        .result_index_ = i,
                        .value_ = std::nullopt});
  }
  std::sort(requests.begin(), requests.end(), [](const auto& a, const auto& b) {
    return a.group_ != b.group_ ? a.group_ < b.group_ : a.field_ < b.field_;
  });
  // Each child extracts only its own reply slots and releases its lease
  // before join. Bound each wave and its physical-owner read admission;
  // memory pressure keeps the original serial floor instead of rejecting a
  // request solely because optional concurrency could not be admitted.
  constexpr std::size_t kScoreReadWidth = 8;
  ++store.active_zset_score_batches_;
  struct ActiveScoreBatch {
    std::size_t& count_;
    ~ActiveScoreBatch() {
      assert(count_ != 0);
      --count_;
    }
  } active_batch{store.active_zset_score_batches_};
  for (std::size_t begin = 0; begin < requests.size();) {
    if (shutdown_flush_requested_)
      co_return absl::CancelledError("member read cancelled by shutdown");
    std::array<std::pair<std::size_t, std::size_t>, kScoreReadWidth> selected;
    // Concurrent commands already overlap their reads. Avoid multiplying
    // every active command into eight queued children on this same owner.
    // Recompute after each wave so newly arrived/departed commands affect
    // optional fanout; every command always retains its serial progress path.
    const auto width = std::max<std::size_t>(
        1, kScoreReadWidth / store.active_zset_score_batches_);
    std::size_t count = 0, next = begin;
    while (next < requests.size() && count < width) {
      // Keep large/external leaves at the serial memory floor. Small inline
      // leaves use the existing bounded physical read buffers; no page is
      // retained by a completed child. The request plan certified this id in
      // the immutable routing view before any physical lookup.
      // At width one the loader already validates the physical record;
      // there is no optional wave whose size needs a second physical lookup.
      bool serial = width == 1;
      if (!serial) {
        const auto* physical = object->FindRecord(requests[next].group_);
        if (!physical)
          co_return absl::DataLossError("missing member prefix page");
        serial = physical->external() || physical->total_disk_bytes() >
                                             kCollectionGroupTargetBytes + 4096;
      }
      if (serial && count != 0) break;
      auto end = next + 1;
      while (end < requests.size() &&
             requests[end].group_ == requests[next].group_)
        ++end;
      selected[count++] = {next, end};
      next = end;
      if (serial) break;
    }
    const auto read = [&](std::size_t ordinal, GroupReadLimiter* limiter) {
      const auto [first, end] = selected[ordinal];
      return LoadSortedSetMemberScores(
          store, partition, db_id, key, digest, object,
          std::span(requests).subspan(first, end - first), result.scores_,
          limiter, group_locked, operation.diagnostics_);
    };
    auto window_admission =
        count > 1 ? TryReserveMemory(GroupReadWindow<std::size_t>::StorageBytes(
                                         worker_count_, count) +
                                     (count - 1) * 4096)
                  : std::nullopt;
    if (!window_admission) {
      auto loaded = co_await read(0, nullptr);
      if (!loaded.ok()) co_return loaded.status();
      begin = selected[0].second;
      continue;
    }
    GroupReadWindow<std::size_t> window(store.worker_, worker_count_, count);
    absl::Status start_status;
    for (std::size_t i = 0; i < count; ++i) {
      if (window.size() == 1 &&
          LAVIK_FAULT_MATCHES("LAVIK_FAIL_ZSET_SCORE_READ_BATCH_START_KEY",
                              key)) {
        start_status = MemoryLimitError("OOM Sorted Set score read batch");
        break;
      }
      window.Add(read(i, window.limiter()), i);
    }
    co_await window.Join();
    const auto completed = window.Collect(
        [](std::size_t, std::size_t) { return absl::OkStatus(); });
    // The final child queues the parent. Recheck population/status after that
    // scheduling gap, after joining and reclaiming every started child.
    if (shutdown_flush_requested_)
      co_return absl::CancelledError("member read cancelled by shutdown");
    const auto& version = object->version();
    if (EffectiveRecordDbEpoch(partition, db_id) != version.db_epoch_ ||
        partition.replication_epoch_ != version.replication_epoch_ ||
        partition.grouped_generations_[db_id] != version.index_generation_)
      co_return absl::NotFoundError("member-index population changed");
    const auto readable = object->ReadStatus();
    if (!readable.ok()) co_return readable;
    if (!start_status.ok()) co_return start_status;
    if (!completed.ok()) co_return completed;
    begin = next;
  }
  co_return result;
}

Task<absl::StatusOr<SortedSetResult>>
StorageEngine::Impl::ReadGroupedSortedSetRandom(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const SortedSetOperation& operation, GroupedObject::Handle object,
    std::span<const std::pair<std::uint64_t, std::size_t>> draws,
    SortedSetResult result, bool group_locked) {
  const auto& directory = object->ordered_directory();
  const auto& metadata = directory.groups();
  std::size_t draw = 0;
  while (draw < draws.size()) {
    const auto position = directory.FindRank(draws[draw].first);
    if (!position)
      co_return absl::DataLossError("Sorted Set random rank missing");
    const auto i = position->group_index_;
    const GroupedRecordId id{metadata[i].id_, 0};
    auto current = partition.grouped_objects_[db_id].CurrentForMutation(key);
    if (group_locked) {
      auto refreshed = RefreshLockedGroup(partition, db_id, key, object, id);
      if (!refreshed.ok()) co_return refreshed.status();
      current = std::move(*refreshed);
    } else if (!current || !current->SameLogicalRoot(*object) ||
               current->version().db_epoch_ != object->version().db_epoch_ ||
               current->version().replication_epoch_ !=
                   object->version().replication_epoch_ ||
               current->version().index_generation_ !=
                   object->version().index_generation_) {
      co_return absl::NotFoundError("Sorted Set random source changed");
    }
    const auto readable = current->ReadStatus();
    if (!readable.ok()) co_return readable;
    LAVIK_FAULT_INJECT(
        if (LAVIK_FAULT_MATCHES("LAVIK_FAIL_ZSET_ORDERED_READ_KEY", key) &&
            (std::getenv("LAVIK_FAIL_ZSET_ORDERED_READ_PAGE") == nullptr ||
             LAVIK_FAULT_MATCHES_NTH("LAVIK_FAIL_ZSET_ORDERED_READ_KEY", key,
                                     "LAVIK_FAIL_ZSET_ORDERED_READ_PAGE",
                                     i + 1))) co_return absl::
            UnavailableError("injected ordered-page read failure"););
    const auto* entry = current->FindGroup(id);
    if (!entry) co_return absl::DataLossError("missing Sorted Set random page");
    GroupedScratchBudget budget;
    auto checked = budget.AddGroup(*entry, current->ExtentsFor(id));
    if (!checked.ok()) co_return checked;
    auto admission = budget.Reserve(2);
    if (!admission.ok()) co_return admission.status();
    if (shutdown_flush_requested_)
      co_return absl::CancelledError(
          "Sorted Set random read cancelled by shutdown");
    auto page = co_await LoadSortedSetPage(
        store, partition, db_id, key, digest, object, metadata[i].id_,
        SortedSetMemberValidation::kTrustStored,
        SortedSetPageReadMode::kIndexedProjection, group_locked,
        operation.diagnostics_);
    if (!page.ok()) co_return page.status();
    const auto& entries = page->entries_;
    do {
      const auto [rank, slot] = draws[draw];
      const auto at = directory.FindRank(rank);
      if (!at || at->group_index_ != i) break;
      const auto size =
          page->indexed_ ? page->indexed_->size() : entries.size();
      if (at->offset_ >= size)
        co_return absl::DataLossError(
            "Sorted Set random page cardinality mismatch");
      auto entry = page->indexed_ ? page->indexed_->At(at->offset_)
                                  : absl::StatusOr<OrderedCollectionEntryView>(
                                        entries[at->offset_]);
      if (!entry.ok()) co_return entry.status();
      auto copied = MakeOutput(*entry);
      if (!copied.ok()) co_return copied.status();
      result.members_[slot] = std::move(*copied);
      ++draw;
    } while (draw < draws.size());
  }
  co_return result;
}

Task<absl::StatusOr<SortedSetResult>>
StorageEngine::Impl::ScanGroupedSortedSetMembers(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const SortedSetOperation& operation, GroupedObject::Handle object,
    bool group_locked) {
  SortedSetResult result;
  result.key_exists_ = true;
  result.length_ = object->ordered_directory().root().item_count_;
  const auto& groups = object->directory().groups();
  const auto* first = groups.Floor(operation.scan_cursor_);
  if (!first)
    co_return absl::DataLossError("Sorted Set scan has no member route");
  auto route = groups.find(first->id_.prefix_);
  auto remaining = operation.scan_count_;
  while (route != groups.end() && remaining != 0) {
    const auto id = route->second.id_;
    ++route;
    result.next_cursor_ = route != groups.end() ? route->first : 0;
    // An old logical directory does not pin physical records across reads.
    // Resolve the current allocation before scratch admission; the loader
    // revalidates the same logical root after any suspension.
    GroupedObject::Handle current;
    const RecordIndexValue* physical = nullptr;
    if (group_locked) {
      auto refreshed =
          RefreshLockedGroup(partition, db_id, key, object, id, &physical);
      if (!refreshed.ok()) co_return refreshed.status();
      current = std::move(*refreshed);
    } else {
      current = partition.grouped_objects_[db_id].CurrentForMutation(key);
    }
    if (!current || (!group_locked && !current->SameLogicalRoot(*object)) ||
        current->version().db_epoch_ != object->version().db_epoch_ ||
        current->version().replication_epoch_ !=
            object->version().replication_epoch_ ||
        current->version().index_generation_ !=
            object->version().index_generation_)
      co_return absl::NotFoundError("member-index scan population changed");
    if (!group_locked) physical = current->FindGroup(id);
    if (!physical) co_return absl::DataLossError("missing member scan page");
    GroupedScratchBudget budget;
    auto status = budget.AddGroup(*physical, physical->external()
                                                 ? current->ExtentsFor(id)
                                                 : ExtentManifest{});
    if (!status.ok()) co_return status;
    auto admission = budget.Reserve(2);
    if (!admission.ok()) co_return admission.status();
    if (shutdown_flush_requested_)
      co_return absl::CancelledError("member scan cancelled by shutdown");
    auto leaf = co_await LoadHashGroupPayload(
        store, partition, db_id, key, digest, object, id, false, group_locked,
        operation.diagnostics_);
    if (!leaf.ok()) co_return leaf.status();
    const auto bytes = leaf->loaded_.value();
    const std::string_view payload(reinterpret_cast<const char*>(bytes.data()),
                                   bytes.size());
    ScanBoundary boundary;
    status = boundary.Prepare(remaining, leaf->field_count_);
    if (!status.ok()) co_return status;
    status = VisitPersistedHashGroupFields(
        payload, leaf->field_count_, [&](const HashEntryView& entry) {
          boundary.Observe(OrderedCollectionEntryView{entry.field_},
                           operation.scan_cursor_);
          return absl::OkStatus();
        });
    if (!status.ok()) co_return status;
    status = VisitPersistedHashGroupFields(
        payload, leaf->field_count_, [&](const HashEntryView& entry) {
          auto score = DecodeSortedSetMemberScore(entry.value_);
          if (!score.ok()) return score.status();
          return boundary.Emit(OrderedCollectionEntryView{entry.field_, *score},
                               operation, &result);
        });
    if (!status.ok()) co_return status;
    // MATCH misses count as examined entries. When COUNT lands on a digest
    // collision, Emit includes the entire bucket and returns its successor.
    remaining -= boundary.prefixes_.size();
  }
  ScanBoundary::Sort(&result);
  co_return result;
}

Task<absl::StatusOr<SortedSetResult>>
StorageEngine::Impl::ExecuteGroupedSortedSetLocked(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const SortedSetOperation& operation, GroupedObject::Handle object,
    TxShardWrites* tx, ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition,
    PreparedOrderedMutation* prepared, SortedSetOrderedProbe* ordered_probe,
    bool group_locked, const std::optional<double>* locked_rank_score) {
  assert(locked_rank_score == nullptr ||
         (group_locked && operation.kind_ == SortedSetOperationKind::kRank));
  if (!object || !object->is_ordered() ||
      object->ordered_directory().root().kind_ !=
          OrderedCollectionKind::kSortedSet)
    co_return absl::DataLossError("invalid grouped Sorted Set view");
  // Public ZSCAN cursors use the process digest seed, including compact
  // values. A persisted member directory may have a different seed after
  // cold recovery; keep the score-page fallback so recovered values and
  // compact values share cursor ordering across promotion and demotion.
  if (operation.kind_ == SortedSetOperationKind::kScan &&
      object->directory().root().seed_ == CurrentDigestSeed())
    co_return co_await ScanGroupedSortedSetMembers(
        store, partition, db_id, key, digest, operation, std::move(object),
        group_locked);
  MemoryReservation input_admission;
  Members members;
  absl::Status status;
  if (locked_rank_score == nullptr) {
    auto reserved = ReserveInputs(operation);
    if (!reserved.ok()) co_return reserved.status();
    input_admission = std::move(*reserved);
    status = PrepareMembers(operation, &members);
    if (!status.ok()) co_return status;
  }
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
    if (group_locked) {
      const RecordIndexValue* entry = nullptr;
      auto current = RefreshLockedGroup(partition, db_id, key, object,
                                        {metadata[i].id_, 0}, &entry);
      if (!current.ok()) return current.status();
      return budget->AddGroup(*entry,
                              entry->external()
                                  ? (*current)->ExtentsFor({metadata[i].id_, 0})
                                  : ExtentManifest{});
    }
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
    return budget->AddGroup(
        *entry, entry->external() ? current->ExtentsFor({metadata[i].id_, 0})
                                  : ExtentManifest{});
  };
  // A serial scan retains one physical read lease and borrowed member views.
  // Keep scratch admitted until both disappear; replies own separate copies.
  const bool point_write =
      ordered_probe == nullptr &&
      ((operation.kind_ == SortedSetOperationKind::kAdd &&
        operation.entries_.size() == 1) ||
       (operation.kind_ == SortedSetOperationKind::kRemove &&
        operation.members_.size() == 1));
  SortedSetOrderedProbe local_probe{.source_ = object, .pages_ = {}};
  if (ordered_probe == nullptr) ordered_probe = &local_probe;
  if (ordered_probe->source_.get() != object.get())
    co_return absl::DataLossError("Sorted Set probe logical view mismatch");
  auto& probed_pages = ordered_probe->pages_;
  using OwnedScanPage = SortedSetOrderedProbe::Page;
  auto retain_probe = [&](std::size_t i, OwnedScanPage page) {
    if (point_write &&
        metadata[i].encoded_bytes_ <= kCollectionGroupTargetBytes) {
      // A point writer needs at most its source and destination boundary.
      // Dense ordinary pages can carry much more admission than encoded
      // bytes: decoder headroom is charged per member and twice per read.
      // Keep that full protection, but do not mistake it for page size.
      if (probed_pages.size() < 2) probed_pages.emplace(i, std::move(page));
      return;
    }
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
#if LAVIK_FAULTS_ENABLED
  bool probe_reservation_failure_injected = false;
#endif
  auto reserve_work =
      [&](const GroupedScratchBudget& budget,
          std::size_t copies) -> absl::StatusOr<MemoryReservation> {
    if (point_write && !probed_pages.empty()) {
      if (copies == 0 ||
          budget.bytes() > std::numeric_limits<std::size_t>::max() / copies)
        return absl::ResourceExhaustedError(
            "grouped scratch peak size overflow");
      bool retain_pages = true;
      LAVIK_FAULT_INJECT(
          if (!probe_reservation_failure_injected &&
              LAVIK_FAULT_MATCHES("LAVIK_FAIL_ZSET_PROBE_RETAIN_RESERVE_KEY",
                                  key)) {
            probe_reservation_failure_injected = true;
            retain_pages = false;
          });
      auto reservation = retain_pages
                             ? TryReserveMemory(budget.bytes() * copies)
                             : std::nullopt;
      if (reservation) return std::move(*reservation);
      // Retention must not turn a formerly admissible point edit into OOM.
      // Drop the optional owned pages and their credits together, then use
      // the ordinary read/plan path. Only that final attempt records a
      // rejection; a failed cache-preserving attempt is not a failed command.
      probed_pages.clear();
    }
    return budget.Reserve(copies);
  };
  SortedSetPageWorkBudget page_work;
  auto read_page_impl = [&]<typename Page>(
                            std::size_t i, GroupReadLimiter* limiter = nullptr,
                            SortedSetPageReadMode mode =
                                SortedSetPageReadMode::kFullDecode)
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
      auto reserved = reserve_work(budget, 2);
      if (!reserved.ok()) co_return reserved.status();
      admission = std::move(*reserved);
    }
    auto load = [&] {
      const auto validation = ReadOnly(operation)
                                  ? SortedSetMemberValidation::kTrustStored
                                  : SortedSetMemberValidation::kVerifyUnique;
      if constexpr (std::is_same_v<Page, LoadedSortedSetPage>)
        return LoadSortedSetPage(store, partition, db_id, key, digest, object,
                                 metadata[i].id_, validation, mode,
                                 group_locked, operation.diagnostics_);
      else if constexpr (std::is_same_v<Page, PackedSortedSetValues>)
        return LoadPackedSortedSetPage(store, partition, db_id, key, digest,
                                       object, metadata[i].id_, limiter,
                                       group_locked, operation.diagnostics_);
      else {
        if (point_write)
          return LoadEncodedSortedSetGroupSnapshot(
              store, partition, db_id, key, digest, object, metadata[i].id_,
              limiter, group_locked, operation.diagnostics_);
        return LoadOrderedGroupSnapshot(
            store, partition, db_id, key, digest, object, metadata[i].id_,
            false, limiter, validation, group_locked, operation.diagnostics_);
      }
    };
    auto page = co_await load();
    if (!page.ok()) co_return page.status();
    co_return ResultPage{std::move(admission), std::move(*page)};
  };
  auto read_page = [&](std::size_t i, SortedSetPageReadMode mode) {
    return read_page_impl.template operator()<LoadedSortedSetPage>(i, nullptr,
                                                                   mode);
  };
  // Mutation probes own their payloads, releasing physical leases before
  // routing. Both wrappers return the loader task without another coroutine.
  auto read_owned_page = [&](std::size_t i) {
    return read_page_impl.template operator()<LoadedOrderedGroup>(i);
  };
  auto find_member_page =
      [&](double score, std::string_view member,
          bool known_member) -> Task<absl::StatusOr<std::size_t>> {
    auto first = directory.LowerBoundScore(score);
    auto end = directory.UpperBoundScore(score);
    // For a member already present in the hash graph, a single score-bounded
    // candidate needs no boundary probe: the caller checks its exact entry.
    // In particular, a unique score must not gain a redundant page read.
    if (known_member && end - first <= 1) co_return first;
    std::optional<OwnedScanPage> candidate;
    std::size_t candidate_index = kNoPage;
    // Page maxima are ordered by (score, binary member). Only pages ending
    // at the wanted score need member comparisons. Missing caches (including
    // after recovery) and ambiguous long prefixes still use binary search:
    // read one midpoint's last member at a time, then halve the interval.
    // The result may be the next score's page, or size() for an append.
    while (first < end) {
      const auto mid = first + (end - first) / 2;
      bool less = false;
      if (metadata[mid].max_score_ == score) {
        auto cached = metadata[mid].max_key_.LessThan(member);
        if (cached.has_value()) {
          less = *cached;
        } else {
          std::optional<AdmittedScanPage<LoadedSortedSetPage>> boundary;
          std::string_view last;
          if (const auto retained = probed_pages.find(mid);
              retained != probed_pages.end()) {
            const auto& snapshot = retained->second.page_.snapshot_;
            last = snapshot.encoded_sorted_set_
                       ? snapshot.encoded_sorted_set_->entries().back().value_
                       : std::string_view(snapshot.entries_.back().value_);
          } else if (!ReadOnly(operation)) {
            // Keep at most the latest owned probe. If it is the selected
            // page, the source check or writer can reuse its full validation
            // and admission; unrelated midpoints never fill the probe cache.
            candidate.reset();
            auto page = co_await read_owned_page(mid);
            if (!page.ok()) co_return page.status();
            candidate.emplace(std::move(*page));
            candidate_index = mid;
            const auto& snapshot = candidate->page_.snapshot_;
            last = snapshot.encoded_sorted_set_
                       ? snapshot.encoded_sorted_set_->entries().back().value_
                       : std::string_view(snapshot.entries_.back().value_);
          } else {
            auto page = co_await read_page(
                mid, SortedSetPageReadMode::kIndexedProjection);
            if (!page.ok()) co_return page.status();
            boundary.emplace(std::move(*page));
            if (boundary->page_.indexed_) {
              const auto& reader = *boundary->page_.indexed_;
              auto entry = reader.At(reader.size() - 1);
              if (!entry.ok()) co_return entry.status();
              last = entry->value_;
            } else {
              last = boundary->page_.entries_.back().value_;
            }
          }
          auto remembered = directory.RememberPageMaxKey(mid, last);
          if (!remembered.ok()) co_return remembered;
          less = last < member;
        }
      }
      if (less)
        first = mid + 1;
      else
        end = mid;
    }
    if (candidate && candidate_index == first)
      retain_probe(first, std::move(*candidate));
    co_return first;
  };
  auto sparse_page_search = [&](std::size_t count) {
    return count == 1 ||
           count <= metadata.size() / std::max<std::size_t>(
                                          1, std::bit_width(metadata.size()));
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
      co_return MemoryLimitError(
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
        SortedSetOperation{.diagnostics_ = operation.diagnostics_,
                           .kind_ = SortedSetOperationKind::kRemove,
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
    co_return co_await ReadGroupedSortedSetRandom(
        store, partition, db_id, key, digest, operation, object,
        selection.draws_, std::move(result), group_locked);
  }
  if (operation.kind_ == SortedSetOperationKind::kScan) {
    ScanBoundary boundary;
    auto prepared = boundary.Prepare(operation.scan_count_, result.length_);
    if (!prepared.ok()) co_return prepared;
    for (std::size_t i = 0; i < metadata.size(); ++i) {
      auto page = co_await read_page(i, SortedSetPageReadMode::kFullDecode);
      if (!page.ok()) co_return page.status();
      for (const auto& entry : page->page_.entries_)
        boundary.Observe(entry, operation.scan_cursor_);
    }
    if (boundary.prefixes_.empty()) co_return result;
    for (std::size_t i = 0; i < metadata.size(); ++i) {
      auto page = co_await read_page(i, SortedSetPageReadMode::kFullDecode);
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
          auto page = co_await read_page(i, SortedSetPageReadMode::kFullDecode);
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
    auto whole_score_page = [&](std::size_t i) {
      const auto low = operation.minimum_score_;
      const auto high = operation.maximum_score_;
      return (low.exclusive_ ? metadata[i].min_score_ > low.value_
                             : metadata[i].min_score_ >= low.value_) &&
             (high.exclusive_ ? metadata[i].max_score_ < high.value_
                              : metadata[i].max_score_ <= high.value_);
    };
    auto score_offset_pending = [&] {
      return operation.kind_ == SortedSetOperationKind::kRange &&
             operation.range_mode_ == SortedSetRangeMode::kScore &&
             operation.limit_ &&
             cursor.skipped_ < static_cast<std::uint64_t>(operation.offset_);
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
          const auto needed = static_cast<std::uint64_t>(operation.offset_) -
                              cursor.skipped_ +
                              static_cast<std::uint64_t>(operation.count_) -
                              result.member_count();
          if (!whole_score_page(i) || items >= needed) {
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
            read_status =
                MemoryLimitError("OOM grouped Sorted Set read batch admission");
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
      // Index explicitly: Clang 18 crashes while resolving the range-for
      // iterator comparison for this coroutine lambda's dependent page type.
      for (std::size_t i = 0; i < pages.size(); ++i) {
        auto& page = pages[i];
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
      if (operation.kind_ == SortedSetOperationKind::kCount &&
          operation.range_mode_ == SortedSetRangeMode::kScore &&
          whole_score_page(page_index(ordinal))) {
        // Only the two score-boundary pages can be partially covered. Once
        // inside the interval, count the complete run through the retained
        // rank index instead of reading every payload. Leave the opposite
        // partial page to the normal checked reader, including v1 pages.
        const auto boundary = reverse ? first_page : end_page - 1;
        const auto next = reverse ? first_page + !whole_score_page(boundary)
                                  : end_page - !whole_score_page(boundary);
        const auto next_rank = directory.CountBefore(next);
        result.count_ += reverse ? rank - next_rank : next_rank - rank;
        rank = next_rank;
        ordinal = reverse ? end_page - next : next - first_page;
        continue;
      }
      if (score_offset_pending() && whole_score_page(page_index(ordinal))) {
        // A partial starting page must count its matches first. Once inside
        // the score interval, all pages before the opposite boundary match
        // completely, so the rank index can skip their payloads. Leave the
        // target page's residual offset to its reader, including score
        // filtering if that page straddles the opposite boundary. Subtract
        // before adding the offset so even INT64_MAX safely returns an empty
        // range.
        const auto remaining =
            static_cast<std::uint64_t>(operation.offset_) - cursor.skipped_;
        const auto available = reverse
                                   ? rank - directory.CountBefore(first_page)
                                   : directory.CountBefore(end_page) - rank;
        if (remaining >= available) co_return result;
        const auto target = directory.FindRank(reverse ? rank - 1 - remaining
                                                       : rank + remaining);
        const auto next_rank =
            directory.CountBefore(target->group_index_ + (reverse ? 1 : 0));
        cursor.skipped_ += reverse ? rank - next_rank : next_rank - rank;
        rank = next_rank;
        ordinal = reverse ? end_page - 1 - target->group_index_
                          : target->group_index_ - first_page;
      }
      // Preserve overlapped I/O once the offset is consumed. Read offset
      // boundary pages serially so a wave cannot fetch pages we can skip.
      // Indexed projection also serves single-page requests and the fallback
      // when the bounded window declines admission or stops at a LIMIT
      // boundary.
      if (operation.kind_ == SortedSetOperationKind::kRange &&
          (operation.range_mode_ == SortedSetRangeMode::kRank ||
           operation.range_mode_ == SortedSetRangeMode::kScore) &&
          !score_offset_pending() && end_page - first_page - ordinal > 1) {
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
      if (ReadOnly(operation) &&
          operation.range_mode_ != SortedSetRangeMode::kLex) {
        const auto i = page_index(ordinal);
        const auto base = directory.CountBefore(i);
        auto page =
            co_await read_page(i, SortedSetPageReadMode::kIndexedProjection);
        if (!page.ok()) co_return page.status();
        if (page->page_.indexed_) {
          auto visited = ReadIndexedRange(*page->page_.indexed_, base, cursor);
          if (!visited.ok()) co_return visited;
          rank = reverse ? base : base + metadata[i].item_count_;
        } else if (operation.packed_output_ &&
                   operation.kind_ == SortedSetOperationKind::kRange) {
          // Keep v1's single packed page block and transfer path. Falling
          // back from an indexed probe must not introduce 4 KiB reply blocks.
          auto packed = PackSortedSetPage(page->page_.entries_);
          if (!packed.ok()) co_return packed.status();
          auto visited = consume_packed_page(*packed, false);
          if (!visited.ok()) co_return visited;
        } else {
          auto visited = consume_page(page->page_.entries_, false);
          if (!visited.ok()) co_return visited;
        }
        ++ordinal;
        continue;
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
        auto page = co_await read_page(page_index(ordinal),
                                       SortedSetPageReadMode::kFullDecode);
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
  if (locked_rank_score == nullptr) {
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
      if (!current || (!group_locked && !current->SameLogicalRoot(*object)) ||
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
        auto leaf = co_await LoadHashGroupPayload(
            store, partition, db_id, key, digest, object, id, false,
            group_locked, operation.diagnostics_);
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
        auto leaf = co_await LoadHashGroupPayload(
            store, partition, db_id, key, digest, object, id, false, false,
            operation.diagnostics_);
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
        if (operation.kind_ == SortedSetOperationKind::kAdd &&
            operation.entries_.size() == 1) {
          const auto& entry = operation.entries_.front();
          const auto old_bytes = payload.size() - kHashGroupHeaderBytes;
          const auto base_bytes = std::max(old_bytes, kHashValueHeaderBytes);
          // A missing field does not invalidate the checked byte-splice
          // editor. Keep the ordinary decoder for a split or large member,
          // and never turn repeated request operands into a point insertion.
          constexpr std::size_t kEntryFrameAndScoreBytes = 8 + sizeof(double);
          if (entry.member_.size() <=
                  kCollectionGroupTargetBytes - kEntryFrameAndScoreBytes &&
              base_bytes <= kCollectionGroupTargetBytes -
                                kEntryFrameAndScoreBytes -
                                entry.member_.size()) {
            GroupedScratchBudget incoming;
            status = incoming.AddBytes(entry.member_.size() + 256);
            if (!status.ok()) co_return status;
            auto incoming_admission = incoming.Reserve(1);
            if (!incoming_admission.ok()) co_return incoming_admission.status();
            const auto score = EncodeSortedSetMemberScore(entry.score_);
            const HashEntryView edit{entry.member_, score};
            auto inserted = ApplyHashGroupEdits(
                payload, object->directory().root().seed_,
                HashGroupEditKind::kSetIfAbsent, {&edit, 1});
            if (!inserted.ok()) co_return inserted.status();
            if (!inserted->changed_ || inserted->added_ != 1 ||
                inserted->removed_ != 0 || inserted->leaves_.size() != 1 ||
                !inserted->leaves_.front().prepared_)
              co_return absl::DataLossError(
                  "invalid member-index insertion probe");
            member_probe.emplace(
                std::move(*admission), object, HashGroupSnapshot{},
                std::nullopt,
                SortedSetMemberInsertion{std::move(*incoming_admission),
                                         entry.member_, entry.score_,
                                         std::move(inserted->leaves_.front())});
            continue;
          }
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
        auto leaf = co_await LoadHashGroupSnapshot(
            store, partition, db_id, key, digest, object, id, false, nullptr,
            false, operation.diagnostics_);
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
    // The member S hold spans routing, ordered-lock waits and projection, so
    // its score (or absence) cannot change. Rank still uses the final common
    // directory below because unrelated pages may have changed their counts.
    const auto score =
        locked_rank_score ? *locked_rank_score : members.at(member).before_;
    if (!score) co_return result;
    // Locate one page by its maximum (score, member), then use Fenwick for
    // the skipped prefix without loading the intervening equal-score pages.
    auto found_page = co_await find_member_page(*score, member, true);
    if (!found_page.ok()) co_return found_page.status();
    const auto i = *found_page;
    if (i < metadata.size()) {
      auto page =
          co_await read_page(i, SortedSetPageReadMode::kIndexedProjection);
      if (!page.ok()) co_return page.status();
      auto slot = FindRankSlot(page->page_, *score, member);
      if (!slot.ok()) co_return slot.status();
      if (*slot) {
        const auto rank = directory.CountBefore(i) + **slot;
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
  auto locate_sources =
      [&](std::size_t i, const auto& entries,
          const IndexedOrderedReader* indexed) -> absl::StatusOr<bool> {
    bool matched = false;
    // The member graph supplies the complete ordered key. Sparse requests
    // can binary-search it; dense batches retain one scan instead of doing
    // M log N probes per page. Neither path allocates lookup scratch, and
    // every loaded mutation page has already passed full validation.
    if (members.size() <=
        entries.size() /
            std::max<std::size_t>(1, std::bit_width(entries.size()))) {
      for (auto& [member, state] : members) {
        if (!state.before_ || *state.before_ < metadata[i].min_score_ ||
            *state.before_ > metadata[i].max_score_)
          continue;
        auto position = [&]() -> absl::StatusOr<std::size_t> {
          if (indexed) return indexed->LowerBound(*state.before_, member);
          const auto found = std::lower_bound(
              entries.begin(), entries.end(),
              OrderedCollectionEntryView{member, *state.before_},
              [](const auto& entry, const auto& key) {
                return entry.score_ < key.score_ ||
                       (entry.score_ == key.score_ &&
                        entry.value_ < key.value_);
              });
          return found - entries.begin();
        }();
        if (!position.ok()) return position.status();
        if (*position == entries.size() ||
            entries[*position].score_ != *state.before_ ||
            entries[*position].value_ != member)
          continue;
        if (state.source_ != kNoPage)
          return absl::DataLossError("duplicate persisted Sorted Set member");
        state.source_ = i;
        --remaining_sources;
        matched = true;
      }
      return matched;
    }
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
  auto locate_page_sources =
      [&](std::size_t i,
          const OrderedGroupSnapshot& snapshot) -> absl::StatusOr<bool> {
    if (!snapshot.encoded_sorted_set_)
      return locate_sources(i, snapshot.entries_, nullptr);
    const auto& encoded = *snapshot.encoded_sorted_set_;
    if (encoded.version() != 2)
      return locate_sources(i, encoded.entries(), nullptr);
    auto reader = IndexedOrderedReader::Open(snapshot.kind_, encoded.bytes(),
                                             encoded.entries().size());
    if (!reader.ok()) return reader.status();
    return locate_sources(i, encoded.entries(), &*reader);
  };
  for (const auto& [i, page] : probed_pages) {
    GroupedScratchBudget budget;
    status = add_page_budget(&budget, i);
    if (!status.ok()) co_return status;
    const auto& snapshot = page.page_.snapshot_;
    auto located = locate_page_sources(i, snapshot);
    if (!located.ok()) co_return located.status();
  }
  // Sparse requests binary-search old (score, member) boundaries. Dense
  // batches merge score intervals and scan once rather than paying M log P
  // probes. Both paths coalesce selected pages before loading mutation data.
  std::vector<std::pair<std::size_t, std::size_t>> source_ranges;
  source_ranges.reserve(members.size());
  const bool sparse_sources = sparse_page_search(remaining_sources);
  for (const auto& [member, state] : members) {
    if (!state.before_ || state.source_ != kNoPage) continue;
    auto first = directory.LowerBoundScore(*state.before_);
    auto end = directory.UpperBoundScore(*state.before_);
    if (sparse_sources) {
      auto found = co_await find_member_page(*state.before_, member, true);
      if (!found.ok()) co_return found.status();
      first = *found;
      end = std::min(first + 1, end);
    }
    if (first >= end)
      co_return absl::DataLossError("member score lies outside ordered pages");
    source_ranges.emplace_back(first, end);
  }
  std::sort(source_ranges.begin(), source_ranges.end());
  std::size_t scanned_end = 0;
  for (const auto& [first, end] : source_ranges) {
    for (std::size_t i = std::max(first, scanned_end);
         i < end && remaining_sources != 0; ++i) {
      if (const auto probe = probed_pages.find(i);
          probe != probed_pages.end()) {
        // Binary routing may have retained the final page after the initial
        // probe/source check. Sparse ranges contain only unresolved sources.
        if (sparse_sources) {
          auto located = locate_page_sources(i, probe->second.page_.snapshot_);
          if (!located.ok()) co_return located.status();
        }
        continue;
      }
      auto page = co_await read_owned_page(i);
      if (!page.ok()) co_return page.status();
      const auto& snapshot = page->page_.snapshot_;
      auto located = locate_page_sources(i, snapshot);
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
        mutation_precondition, nullptr, nullptr, operation.diagnostics_);
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
  // when boundary members move. Sparse destinations use binary routing;
  // dense batches retain a single ordered walk with shared boundary reads.
  std::size_t next = 0;
  if (sparse_page_search(pending.size())) {
    for (const auto& [member, state] : pending) {
      auto found = co_await find_member_page(*state->after_, member, false);
      if (!found.ok()) co_return found.status();
      const auto i = std::min(*found, metadata.size() - 1);
      state->destination_ = i;
      modified.insert(i);
      ++next;
    }
  } else {
    for (std::size_t i = 0; next != pending.size() && i < metadata.size();
         ++i) {
      i = std::max(
          i, std::min(directory.LowerBoundScore(*pending[next].second->after_),
                      metadata.size() - 1));
      std::optional<OwnedScanPage> boundary;
      while (next != pending.size()) {
        const auto& [member, state] = pending[next];
        if (i + 1 != metadata.size()) {
          if (*state->after_ > metadata[i].max_score_) break;
          if (*state->after_ == metadata[i].max_score_) {
            auto less = metadata[i].max_key_.LessThan(member);
            if (!less.has_value()) {
              if (!boundary) {
                auto page = co_await read_owned_page(i);
                if (!page.ok()) co_return page.status();
                boundary.emplace(std::move(*page));
              }
              const auto& snapshot = boundary->page_.snapshot_;
              const std::string_view last_member =
                  snapshot.encoded_sorted_set_
                      ? snapshot.encoded_sorted_set_->entries().back().value_
                      : std::string_view(snapshot.entries_.back().value_);
              status = directory.RememberPageMaxKey(i, last_member);
              if (!status.ok()) co_return status;
              less = last_member < member;
            }
            if (*less) break;
          }
        }
        state->destination_ = i;
        modified.insert(i);
        ++next;
      }
      if (boundary && modified.contains(i))
        retain_probe(i, std::move(*boundary));
    }
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
  auto admission = reserve_work(budget, 4);
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
      // working_admission now covers the transferred owned payload.
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
      auto page =
          co_await (point_write ? LoadEncodedSortedSetGroupSnapshot(
                                      store, partition, db_id, key, digest,
                                      object, metadata[i].id_, nullptr, false,
                                      operation.diagnostics_)
                                : LoadOrderedGroupSnapshot(
                                      store, partition, db_id, key, digest,
                                      object, metadata[i].id_, false, nullptr,
                                      SortedSetMemberValidation::kVerifyUnique,
                                      false, operation.diagnostics_));
      if (!page.ok()) co_return page.status();
      loaded.emplace(i, std::move(page->snapshot_));
    }
  }
  bool encoded_point = point_write;
  if (encoded_point) {
    const auto& [member, state] = *members.begin();
    for (const auto i : modified) {
      auto edited = EditEncodedSortedSetGroup(
          loaded.at(i), member,
          i == state.source_ ? state.before_ : std::nullopt,
          i == state.destination_ ? state.after_ : std::nullopt,
          kCollectionGroupTargetBytes,
          // The member graph either proved global absence or we resolved
          // this member in a different source page under the same root.
          SortedSetMemberAbsence::kVerified);
      if (!edited.ok()) co_return edited.status();
      if (!*edited) {
        encoded_point = false;
        plan.writes_.clear();
        break;
      }
      plan.writes_.push_back(std::move(**edited));
    }
  }
  if (!encoded_point) {
    LAVIK_FAULT_INJECT(
        if (point_write &&
            LAVIK_FAULT_MATCHES("LAVIK_FAIL_ZSET_MUTABLE_PLAN_KEY",
                                key)) co_return absl::
            UnavailableError(
                "injected mutable Sorted Set point plan failure"););
    // Empty/overflowing pages need the ordinary retirement, redistribution and
    // split planner. Materialize only on this uncommon path; no encoded page
    // or physical lease escapes into the mutable topology machinery.
    for (auto& [index, page] : loaded) MaterializeOrderedGroupEntries(page);
    if (operation.kind_ == SortedSetOperationKind::kAdd &&
        operation.entries_.size() == 1 && pending.size() == 1 &&
        pending.front().second->before_) {
      // A point score update owns the existing member already. Keep that
      // allocation and the page's ordered subsequences instead of deleting the
      // member, copying the request, and sorting the entire destination. Batch
      // inputs retain the general path because repeated members apply in order.
      const auto& [member, state] = pending.front();
      auto& source = loaded.at(state->source_).entries_;
      const auto entry = std::lower_bound(
          source.begin(), source.end(),
          OrderedCollectionEntryView{member, *state->before_},
          [](const auto& at, const auto& key) {
            return at.score_ < key.score_ ||
                   (at.score_ == key.score_ && at.value_ < key.value_);
          });
      if (entry == source.end() || entry->value_ != member ||
          entry->score_ != *state->before_)
        co_return absl::DataLossError("missing Sorted Set point-update source");
      entry->score_ = *state->after_;
      if (state->source_ == state->destination_) {
        if (entry != source.begin() && OrderedEntryLess(*entry, *(entry - 1))) {
          const auto position =
              std::lower_bound(source.begin(), entry, *entry, OrderedEntryLess);
          std::rotate(position, entry, entry + 1);
        } else if (entry + 1 != source.end() &&
                   OrderedEntryLess(*(entry + 1), *entry)) {
          const auto position = std::lower_bound(entry + 1, source.end(),
                                                 *entry, OrderedEntryLess);
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
    } else if (operation.kind_ == SortedSetOperationKind::kAdd &&
               operation.entries_.size() == 1 && pending.size() == 1) {
      // The untouched destination is already ordered. Insert a new member at
      // its exact score/member position instead of sorting all existing
      // entries. The member-index lookup and ordinary page validation remain
      // mandatory.
      const auto& [member, state] = pending.front();
      auto& destination = loaded.at(state->destination_).entries_;
      OrderedCollectionEntry entry{std::string(member), *state->after_};
      const auto position = std::lower_bound(
          destination.begin(), destination.end(), entry, OrderedEntryLess);
      destination.insert(position, std::move(entry));
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
      auto balanced =
          RebalanceSortedSetGroupPair(loaded.at(i), loaded.at(i + 1));
      if (!balanced.ok()) co_return balanced.status();
    }

    // Repeated inserts before a full page otherwise split off one tiny
    // successor per command. Reuse at most one unchanged neighbour before
    // allocating an identity, using resident sizes to avoid speculative reads
    // of full pages. selected already admits both immediate neighbours. A
    // successful pair keeps its links and IDs, so it needs neither a wider
    // interval nor more admission.
    constexpr auto kPagePayloadBytes =
        kCollectionGroupTargetBytes -
        OrderedGroupFixedBytes(OrderedCollectionKind::kSortedSet);
    constexpr auto kPairPayloadBytes = 2 * kPagePayloadBytes;
    auto bounded_payload_bytes = [](const OrderedGroupSnapshot& page) {
      std::size_t bytes = 0;
      for (const auto& entry : page.entries_) {
        const auto entry_bytes = kOrderedEntryHeaderBytes + entry.value_.size();
        if (entry_bytes > kPairPayloadBytes - bytes)
          return kPairPayloadBytes + 1;
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
      for (const auto candidate :
           {i == 0 ? kNoPage : i - 1,
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
            store, partition, db_id, key, digest, object,
            metadata[neighbor].id_, false, nullptr,
            SortedSetMemberValidation::kVerifyUnique, false,
            operation.diagnostics_);
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
        auto split = SplitCheckedSortedSetGroup(std::move(page), next_id);
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
                store, partition, db_id, key, digest, object, old.id_, false,
                nullptr, SortedSetMemberValidation::kVerifyUnique, false,
                operation.diagnostics_);
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
    if (page_count == 0 ||
        page_count > std::numeric_limits<std::uint32_t>::max())
      co_return absl::DataLossError("invalid Sorted Set resulting page count");
    plan.root_.group_count_ = page_count;
    plan.root_.next_group_id_ = next_id;
  }
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
      mutation_precondition, &member_mutation, nullptr, operation.diagnostics_);
  if (!written.ok()) co_return written;
  co_return result;
}

}  // namespace lavik::storage
