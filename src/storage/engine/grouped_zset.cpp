#include <cmath>
#include <map>
#include <set>

#include "impl.h"
#include "keylane/glob.h"
#include "keylane/storage/detail/grouped_scratch.h"
#include "keylane/storage/detail/ordered_compact_codec.h"

namespace keylane::storage {
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

bool ReadOnly(const SortedSetOperation& operation) {
  return operation.kind_ != SortedSetOperationKind::kAdd &&
         operation.kind_ != SortedSetOperationKind::kRemove &&
         operation.kind_ != SortedSetOperationKind::kPop;
}

bool ScanRead(const SortedSetOperation& operation) {
  return operation.kind_ == SortedSetOperationKind::kRange ||
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

bool Matches(const OrderedCollectionEntry& entry,
             const SortedSetOperation& operation) {
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

absl::StatusOr<SortedSetMember> CopyOutput(
    const OrderedCollectionEntry& entry) {
  auto admission = TryReserveMemory(entry.value_.size() + 64);
  if (!admission) {
    RecordMemoryRejection();
    return absl::ResourceExhaustedError(
        "OOM Sorted Set output member admission");
  }
  SortedSetMember output;
  output.member_ = entry.value_;
  output.score_ = entry.score_;
  output.retained_charge_.Adopt(&*admission, output.member_.capacity() + 1);
  return output;
}

absl::Status AppendOutput(SortedSetMember member, SortedSetResult* result) {
  auto& output = result->members_;
  if (output.size() == output.capacity()) {
    if (output.capacity() >
        std::numeric_limits<std::size_t>::max() / (2 * sizeof(SortedSetMember)))
      return absl::ResourceExhaustedError("Sorted Set output count overflow");
    const auto capacity = std::max<std::size_t>(8, output.capacity() * 2);
    auto admission = TryReserveMemory(capacity * sizeof(SortedSetMember));
    if (!admission) {
      RecordMemoryRejection();
      return absl::ResourceExhaustedError(
          "OOM Sorted Set output vector admission");
    }
    output.reserve(capacity);
    result->retained_charge_.Adopt(&*admission,
                                   output.capacity() * sizeof(SortedSetMember));
  }
  output.push_back(std::move(member));
  return absl::OkStatus();
}

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
  void Observe(const OrderedCollectionEntry& entry, std::uint64_t cursor) {
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
  absl::Status Emit(const OrderedCollectionEntry& entry,
                    const SortedSetOperation& operation,
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
    auto copy = CopyOutput(entry);
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
    done_ = operation.kind_ == SortedSetOperationKind::kRange &&
            ((operation.range_mode_ == SortedSetRangeMode::kRank &&
              first_ == end_) ||
             (operation.limit_ &&
              (operation.offset_ < 0 || operation.count_ == 0)));
  }

  absl::Status Visit(const OrderedCollectionEntry& entry, std::uint64_t rank) {
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
    auto copied = CopyOutput(entry);
    if (!copied.ok()) return copied.status();
    auto appended = AppendOutput(std::move(*copied), result_);
    if (!appended.ok()) return appended;
    if (operation_.limit_ && operation_.count_ >= 0 &&
        result_->members_.size() >=
            static_cast<std::uint64_t>(operation_.count_))
      done_ = true;
    if (operation_.range_mode_ == SortedSetRangeMode::kRank &&
        result_->members_.size() == end_ - first_)
      done_ = true;
    return absl::OkStatus();
  }
};

bool BetterLexCandidate(const OrderedCollectionEntry& entry,
                        const SortedSetOperation& operation,
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
          !result->members_.empty()
              ? std::optional<std::string_view>(result->members_.back().member_)
          : skipped ? std::optional<std::string_view>(skipped->member_)
                    : std::nullopt;
      std::optional<SortedSetMember> candidate;
      for (const auto& entry : entries) {
        if (!BetterLexCandidate(entry, operation, after, candidate)) continue;
        auto copied = CopyOutput(entry);
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
      auto appended = AppendOutput(std::move(*candidate), result);
      if (!appended.ok()) return appended;
      if (operation.limit_ && operation.count_ >= 0 &&
          result->members_.size() >=
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
  if (count > std::numeric_limits<std::size_t>::max() / 512)
    return absl::ResourceExhaustedError("Sorted Set input size overflow");
  GroupedScratchBudget budget;
  auto status = budget.AddBytes(count * 512);
  if (!status.ok()) return status;
  return budget.Reserve(1);
}

}  // namespace

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
      try {
        if (operation.kind_ == SortedSetOperationKind::kScores) {
          auto allocated =
              PrepareScoreReply(operation.members_.size(), &missing);
          if (!allocated.ok()) co_return allocated;
        }
      } catch (const std::bad_alloc&) {
        co_return absl::ResourceExhaustedError(
            "OOM Sorted Set missing reply allocation");
      }
      co_return missing;
    }
    co_return read;
  }
  if (operation.kind_ == SortedSetOperationKind::kLength) co_return result;
  // The key hold remains owned by the caller across this handoff. Reuse the
  // compact pipeline for a compact/missing value, never for a grouped graph.
  unlock.Unlock();
  try {
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
      GroupedScratchBudget budget;
      auto status = budget.AddBytes(value ? value->encoded_.size() : 0);
      if (!status.ok()) return status;
      for (const auto& entry : operation.entries_) {
        status = budget.AddBytes(entry.member_.size() + 256);
        if (!status.ok()) return status;
      }
      if (value) {
        if (value->logical_size_ >
            std::numeric_limits<std::size_t>::max() / 256)
          return absl::ResourceExhaustedError(
              "Sorted Set compact count overflow");
        status = budget.AddBytes(value->logical_size_ * 256);
        if (!status.ok()) return status;
      }
      auto admission = budget.Reserve(4);
      if (!admission.ok()) return admission.status();
      compact_admission.emplace(std::move(*admission));
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
      if (operation.kind_ == SortedSetOperationKind::kPop) {
        const auto count =
            std::min<std::uint64_t>(operation.pop_count_, entries.size());
        for (std::size_t i = 0; i < count; ++i) {
          const auto at = operation.reverse_ ? entries.size() - 1 - i : i;
          auto copied = CopyOutput(entries[at]);
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
        auto encoded = EncodeOrderedCompactValue(
            OrderedCollectionKind::kSortedSet, entries);
        if (!encoded.ok()) return encoded.status();
        return CompactValueUpdate{.changed_ = true,
                                  .encoded_ = std::move(*encoded),
                                  .logical_size_ = entries.size(),
                                  .expire_at_ms_ = std::nullopt};
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
      status = ApplyInputs(operation, &members, &result);
      if (!status.ok()) return status;
      if (ReadOnly(operation) || result.changed_ == 0)
        return CompactValueUpdate{};
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
      return CompactValueUpdate{.changed_ = true,
                                .encoded_ = std::move(*encoded),
                                .logical_size_ = entries.size(),
                                .expire_at_ms_ = std::nullopt};
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
        db_id, key, digest, ValueType::kSortedSet, ReadOnly(operation),
        callback, tx, now, replication, prepare_unlocked,
        mutation_precondition);
    if (!status.ok()) co_return status;
    co_return result;
  } catch (const std::bad_alloc&) {
    co_return absl::ResourceExhaustedError(
        "OOM Sorted Set operation allocation");
  }
}

Task<absl::StatusOr<SortedSetResult>>
StorageEngine::Impl::ExecuteGroupedSortedSetLocked(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const SortedSetOperation& operation, GroupedHashObject::Handle object,
    TxShardWrites* tx, ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  if (!object || !object->is_ordered() ||
      object->ordered_directory().root().kind_ !=
          OrderedCollectionKind::kSortedSet)
    co_return absl::DataLossError("invalid grouped Sorted Set view");
  try {
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
      // its old physical records. Earlier page IO (or the read-side yield)
      // may let GC relocate and reclaim them. Resolve a current physical view
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
          version.root_.mutation_sequence_ !=
              expected.root_.mutation_sequence_ ||
          version.root_.logical_size_ != expected.root_.logical_size_ ||
          version.root_.expire_at_ms_ != expected.root_.expire_at_ms_ ||
          version.root_.value_type() != expected.root_.value_type() ||
          !object->SameLogicalRoot(*current))
        return absl::NotFoundError(
            "Sorted Set logical view changed before admission");
      const auto readable = current->ReadStatus();
      if (!readable.ok()) return readable;
      const auto* entry = current->FindGroup({metadata[i].id_, 0});
      if (!entry)
        return absl::DataLossError("missing Sorted Set physical page");
      return budget->AddGroup(
          entry->value_, current->ExtentsFor({metadata[i].id_, 0}), key.size());
    };
    // Each scan page owns its admission until its decoded strings disappear.
    // Returning just LoadedOrderedGroup would release this reservation too
    // soon.
    struct ScanPage {
      MemoryReservation admission_;
      LoadedOrderedGroup page_;
    };
    auto read_page = [&](std::size_t i) -> Task<absl::StatusOr<ScanPage>> {
      KEYLANE_FAULT_INJECT(
          // An optional one-based directory page isolates routing tests from
          // the existing fail-all-ordered-reads member-index test.
          if (KEYLANE_FAULT_MATCHES("KEYLANE_FAIL_ZSET_ORDERED_READ_KEY",
                                    key) &&
              (std::getenv("KEYLANE_FAIL_ZSET_ORDERED_READ_PAGE") == nullptr ||
               KEYLANE_FAULT_MATCHES_NTH("KEYLANE_FAIL_ZSET_ORDERED_READ_KEY",
                                         key,
                                         "KEYLANE_FAIL_ZSET_ORDERED_READ_PAGE",
                                         i + 1))) co_return absl::
              UnavailableError("injected ordered-page read failure"););
      if (ReadOnly(operation)) {
        co_await celer::Yield(*store.worker_);
        if (shutdown_flush_requested_)
          co_return absl::CancelledError(
              "Sorted Set read cancelled by shutdown");
      }
      GroupedScratchBudget budget;
      auto checked = add_page_budget(&budget, i);
      if (!checked.ok()) co_return checked;
      auto admission = budget.Reserve(2);
      if (!admission.ok()) co_return admission.status();
      auto page = co_await LoadOrderedGroupSnapshot(
          store, partition, db_id, key, digest, object, metadata[i].id_);
      if (!page.ok()) co_return page.status();
      co_return ScanPage{std::move(*admission), std::move(*page)};
    };
    if (operation.kind_ == SortedSetOperationKind::kScan) {
      ScanBoundary boundary;
      auto prepared = boundary.Prepare(operation, result.length_);
      if (!prepared.ok()) co_return prepared;
      for (std::size_t i = 0; i < metadata.size(); ++i) {
        auto page = co_await read_page(i);
        if (!page.ok()) co_return page.status();
        for (const auto& entry : page->page_.snapshot_.entries_)
          boundary.Observe(entry, operation.scan_cursor_);
      }
      if (boundary.prefixes_.empty()) co_return result;
      for (std::size_t i = 0; i < metadata.size(); ++i) {
        auto page = co_await read_page(i);
        if (!page.ok()) co_return page.status();
        for (const auto& entry : page->page_.snapshot_.entries_) {
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
        auto page = co_await read_page(i);
        if (!page.ok()) co_return page.status();
        const auto& entries = page->page_.snapshot_.entries_;
        for (std::size_t j = 0;
             result.members_.size() < count && j < entries.size(); ++j) {
          const auto at = operation.reverse_ ? entries.size() - 1 - j : j;
          auto copied = CopyOutput(entries[at]);
          if (!copied.ok()) co_return copied.status();
          auto appended = AppendOutput(std::move(*copied), &result);
          if (!appended.ok()) co_return appended;
        }
      }
      if (result.members_.size() != count)
        co_return absl::DataLossError("Sorted Set pop cardinality mismatch");
      if (count >
          std::numeric_limits<std::size_t>::max() / sizeof(std::string_view))
        co_return absl::ResourceExhaustedError("Sorted Set pop count overflow");
      auto admission = TryReserveMemory(count * sizeof(std::string_view));
      if (!admission) {
        RecordMemoryRejection();
        co_return absl::ResourceExhaustedError(
            "OOM Sorted Set pop input admission");
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
          object, tx, replication, mutation_precondition);
      if (!deleted.ok()) co_return deleted.status();
      result.changed_ = deleted->changed_;
      result.length_ = deleted->length_;
      co_return result;
    }
    if (ScanRead(operation)) {
      ReadCursor cursor(operation, &result);
      if (cursor.done_) co_return result;
      if (operation.kind_ == SortedSetOperationKind::kRange &&
          operation.range_mode_ == SortedSetRangeMode::kLex) {
        // Physical pages are score-ordered; mixed-score BYLEX historically
        // sorts members independently. Without a resident member index, select
        // one next lexical member per pass: O(N * (offset + output)) work, but
        // only one page, one candidate/boundary, and the admitted reply
        // survive. Every page yields and revalidates epochs, so even a large
        // OFFSET does not monopolize this worker or keep the store mutex across
        // passes.
        std::optional<SortedSetMember> skipped;
        std::uint64_t offset = operation.limit_ ? operation.offset_ : 0;
        for (;;) {
          const std::optional<std::string_view> after =
              !result.members_.empty() ? std::optional<std::string_view>(
                                             result.members_.back().member_)
              : skipped ? std::optional<std::string_view>(skipped->member_)
                        : std::nullopt;
          std::optional<SortedSetMember> candidate;
          for (std::size_t i = 0; i < metadata.size(); ++i) {
            auto page = co_await read_page(i);
            if (!page.ok()) co_return page.status();
            for (const auto& entry : page->page_.snapshot_.entries_) {
              if (!BetterLexCandidate(entry, operation, after, candidate))
                continue;
              auto copied = CopyOutput(entry);
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
          auto appended = AppendOutput(std::move(*candidate), &result);
          if (!appended.ok()) co_return appended;
          if (operation.limit_ && operation.count_ >= 0 &&
              result.members_.size() >=
                  static_cast<std::uint64_t>(operation.count_))
            break;
        }
        co_return result;
      }
      std::size_t first_page = 0, end_page = metadata.size();
      if (operation.kind_ == SortedSetOperationKind::kRange &&
          operation.range_mode_ == SortedSetRangeMode::kRank) {
        const auto begin_rank =
            operation.reverse_ ? result.length_ - cursor.end_ : cursor.first_;
        const auto end_rank =
            operation.reverse_ ? result.length_ - cursor.first_ : cursor.end_;
        first_page = directory.FindRank(begin_rank)->group_index_;
        end_page = directory.FindRank(end_rank - 1)->group_index_ + 1;
      } else if (operation.kind_ != SortedSetOperationKind::kRank &&
                 operation.range_mode_ == SortedSetRangeMode::kScore) {
        first_page =
            directory.LowerBoundScore(operation.minimum_score_.value_,
                                      operation.minimum_score_.exclusive_);
        end_page =
            directory.UpperBoundScore(operation.maximum_score_.value_,
                                      operation.maximum_score_.exclusive_);
        if (first_page >= end_page) co_return result;
      }
      std::uint64_t rank = 0;
      const bool reverse = operation.reverse_;
      for (std::size_t i = 0; i < (reverse ? end_page : first_page); ++i)
        rank += metadata[i].item_count_;
      for (std::size_t ordinal = 0;
           !cursor.done_ && ordinal < end_page - first_page; ++ordinal) {
        const auto i = reverse ? end_page - 1 - ordinal : first_page + ordinal;
        auto page = co_await read_page(i);
        if (!page.ok()) co_return page.status();
        const auto& entries = page->page_.snapshot_.entries_;
        if (reverse) rank -= entries.size();
        for (std::size_t j = 0; !cursor.done_ && j < entries.size(); ++j) {
          const auto index = reverse ? entries.size() - 1 - j : j;
          auto visited = cursor.Visit(entries[index], rank + index);
          if (!visited.ok()) co_return visited;
        }
        if (!reverse) rank += entries.size();
      }
      co_return result;
    }
    const bool indexed = object->has_member_index();
    std::size_t remaining_sources = 0;
    if (indexed) {
      // Prefix routing retains only per-group metadata. Exact members and
      // scores are decoded from the selected Hash leaves, never trusted from
      // a digest alone. Batch requests read each selected leaf just once.
      std::set<HashGroupId> selected;
      for (const auto& [member, state] : members) {
        const auto* route = object->directory().Find(member);
        if (!route)
          co_return absl::DataLossError("missing member prefix route");
        selected.insert(route->id_);
      }
      for (const auto id : selected) {
        if (ReadOnly(operation)) {
          co_await celer::Yield(*store.worker_);
          if (shutdown_flush_requested_)
            co_return absl::CancelledError("member read cancelled by shutdown");
        }
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
        auto checked = budget.AddGroup(physical->value_,
                                       current->ExtentsFor(id), key.size());
        if (!checked.ok()) co_return checked;
        auto admission = budget.Reserve(2);
        if (!admission.ok()) co_return admission.status();
        auto leaf = co_await LoadHashGroupSnapshot(store, partition, db_id, key,
                                                   digest, object, id);
        if (!leaf.ok()) co_return leaf.status();
        for (const auto& entry : leaf->snapshot_.value_.entries_) {
          const auto requested = members.find(entry.field_);
          if (requested == members.end()) continue;
          auto score = DecodeSortedSetMemberScore(entry.value_);
          if (!score.ok()) co_return score.status();
          requested->second.before_ = requested->second.after_ = *score;
          ++remaining_sources;
        }
      }
      if (operation.kind_ == SortedSetOperationKind::kScores) {
        status = ApplyInputs(operation, &members, &result);
        if (!status.ok()) co_return status;
        co_return result;
      }
    }
    // The two resident doubles bound old-score candidates without reading
    // unrelated pages. Equal-score runs still scan for exact members. Sort
    // and merge requested intervals so a batch reads an overlapping page only
    // once; interval storage is covered by the per-input reservation. Legacy
    // roots have no old-score lookup and keep the full membership scan.
    std::vector<std::pair<std::size_t, std::size_t>> source_ranges;
    if (indexed) {
      source_ranges.reserve(members.size());
      for (const auto& [member, state] : members) {
        if (!state.before_) continue;
        const auto first = directory.LowerBoundScore(*state.before_);
        const auto end = directory.UpperBoundScore(*state.before_);
        if (first >= end)
          co_return absl::DataLossError(
              "member score lies outside ordered pages");
        source_ranges.emplace_back(first, end);
      }
      std::sort(source_ranges.begin(), source_ranges.end());
    } else {
      source_ranges.emplace_back(0, metadata.size());
    }
    std::size_t scanned_end = 0;
    for (const auto& [first, end] : source_ranges) {
      for (std::size_t i = std::max(first, scanned_end);
           i < end && (!indexed || remaining_sources != 0); ++i) {
        auto page = co_await read_page(i);
        if (!page.ok()) co_return page.status();
        for (const auto& entry : page->page_.snapshot_.entries_) {
          auto member = members.find(entry.value_);
          if (member == members.end()) continue;
          auto& state = member->second;
          if ((!indexed && state.before_) || state.source_ != kNoPage)
            co_return absl::DataLossError(
                "duplicate persisted Sorted Set member");
          if (indexed && (!state.before_ || *state.before_ != entry.score_))
            co_return absl::DataLossError(
                "member-index/ordered score mismatch");
          state.before_ = state.after_ = entry.score_;
          state.source_ = i;
          if (indexed) --remaining_sources;
        }
      }
      scanned_end = std::max(scanned_end, end);
    }
    if (indexed && remaining_sources != 0)
      co_return absl::DataLossError(
          "member index refers to missing ordered member");
    status = ApplyInputs(operation, &members, &result);
    if (!status.ok()) co_return status;
    if (ReadOnly(operation) || result.changed_ == 0) co_return result;
    std::optional<MemoryReservation> working_admission;
    OrderedCollectionMutationPlan plan{
        .root_ = directory.root(),
        .expected_sequence_ = directory.sequence(),
        .changed_ = true,
        .writes_ = {}};
    plan.root_.item_count_ = result.length_;
    if (result.length_ == 0) {
      plan.delete_key_ = true;
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
    for (std::size_t i = 0; next != pending.size() && i < metadata.size();
         ++i) {
      i = std::max(
          i, std::min(directory.LowerBoundScore(*pending[next].second->after_),
                      metadata.size() - 1));
      std::optional<ScanPage> boundary;
      while (next != pending.size()) {
        const auto& [member, state] = pending[next];
        if (i + 1 != metadata.size()) {
          if (*state->after_ > metadata[i].max_score_) break;
          if (*state->after_ == metadata[i].max_score_) {
            if (!boundary) {
              auto page = co_await read_page(i);
              if (!page.ok()) co_return page.status();
              boundary.emplace(std::move(*page));
            }
            if (member > boundary->page_.snapshot_.entries_.back().value_)
              break;
          }
        }
        state->destination_ = i;
        modified.insert(i);
        ++next;
      }
    }
    if (next != pending.size())
      co_return absl::DataLossError("Sorted Set destination scan ended early");
    std::set<std::size_t> selected = modified;
    for (const auto i : modified) {
      if (i != 0) selected.insert(i - 1);
      if (i + 1 != metadata.size()) selected.insert(i + 1);
    }
    GroupedScratchBudget budget;
    if (metadata.size() > std::numeric_limits<std::size_t>::max() / 128)
      co_return absl::ResourceExhaustedError(
          "Sorted Set page metadata overflow");
    status = budget.AddBytes(metadata.size() * 128);
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
    std::map<std::size_t, OrderedGroupSnapshot> loaded;
    for (const auto i : selected) {
      auto page = co_await LoadOrderedGroupSnapshot(
          store, partition, db_id, key, digest, object, metadata[i].id_);
      if (!page.ok()) co_return page.status();
      loaded.emplace(i, std::move(page->snapshot_));
    }
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

    struct Route {
      std::uint64_t id_;
      std::size_t source_;
      std::size_t write_;
    };
    std::vector<Route> route;
    route.reserve(metadata.size());
    auto next_id = directory.root().next_group_id_;
    // Only routing identities scale with group count. Unmodified page payloads
    // never enter this planner, and erased ids retain explicit marker records.
    for (std::size_t i = 0; i < metadata.size(); ++i) {
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
      std::sort(page.entries_.begin(), page.entries_.end(), OrderedEntryLess);
      auto split = SplitOrderedGroup(std::move(page), next_id);
      if (!split.ok()) co_return split.status();
      next_id = split->next_group_id_;
      for (auto& part : split->groups_) {
        route.push_back({part.id_, i, plan.writes_.size()});
        plan.writes_.push_back(std::move(part));
      }
    }
    if (route.empty() ||
        route.size() > std::numeric_limits<std::uint32_t>::max())
      co_return absl::DataLossError("invalid Sorted Set resulting page count");
    for (std::size_t i = 0; i < route.size(); ++i) {
      auto& at = route[i];
      const auto previous = i == 0 ? 0 : route[i - 1].id_;
      const auto following = i + 1 == route.size() ? 0 : route[i + 1].id_;
      if (at.write_ == kNoPage) {
        const auto& old = metadata[at.source_];
        if (old.previous_ == previous && old.next_ == following) continue;
        const auto neighbor = loaded.find(at.source_);
        if (neighbor == loaded.end())
          co_return absl::DataLossError(
              "Sorted Set changed link lacks admitted neighbour");
        at.write_ = plan.writes_.size();
        plan.writes_.push_back(std::move(neighbor->second));
      }
      plan.writes_[at.write_].previous_ = previous;
      plan.writes_[at.write_].next_ = following;
    }
    plan.root_.first_group_ = route.front().id_;
    plan.root_.last_group_ = route.back().id_;
    plan.root_.group_count_ = route.size();
    plan.root_.next_group_id_ = next_id;
    auto written = co_await CommitGroupedOrderedMutationLocked(
        store, partition, db_id, key, digest, object, std::move(plan),
        object->version().root_.expire_at_ms_, tx, replication,
        mutation_precondition);
    if (!written.ok()) co_return written;
    co_return result;
  } catch (const std::bad_alloc&) {
    co_return absl::ResourceExhaustedError(
        "OOM grouped Sorted Set operation allocation");
  }
}

}  // namespace keylane::storage
