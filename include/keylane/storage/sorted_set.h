#pragma once

#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "keylane/memory.h"

namespace keylane::storage {

enum class SortedSetOperationKind : std::uint8_t {
  kAdd,
  kRemove,
  kScores,
  kLength,
  kRange,
  kRank,
  kCount,
  kPop,
  kScan,
};

enum class SortedSetRangeMode : std::uint8_t { kRank, kScore, kLex };
struct SortedSetScoreBound {
  double value_ = 0;
  bool exclusive_ = false;
};
struct SortedSetLexBound {
  std::string_view value_;
  int infinity_ = 0;  // -1 / +1 denote unbounded low / high endpoints.
  bool exclusive_ = false;
};

struct ScoredMemberView {
  std::string_view member_;
  double score_ = 0;
};

// Inputs are borrowed until the asynchronous operation completes. Add applies
// duplicate members in request order, including conditional/increment rules;
// kScores returns one optional score per requested member, preserving
// duplicates; kRank requires exactly one requested member, and kCount uses
// score/lex bounds.
struct SortedSetOperation {
  SortedSetOperationKind kind_ = SortedSetOperationKind::kLength;
  std::span<const ScoredMemberView> entries_{};
  std::span<const std::string_view> members_{};
  bool nx_ = false;
  bool xx_ = false;
  bool gt_ = false;
  bool lt_ = false;
  bool increment_ = false;
  // Internal restore guard: duplicate input members or an existing member are
  // corruption/errors, not NX-style skips. Validated before any page is staged.
  bool reject_existing_ = false;
  std::uint64_t now_ms_ = 0;
  // Rank endpoints are inclusive Redis indexes in the requested direction;
  // negative indexes count from its end. Score/lex bounds are always normalized
  // low-to-high, even for reverse traversal. A negative LIMIT offset is empty;
  // a negative count is unbounded. LIMIT is not accepted for rank ranges.
  SortedSetRangeMode range_mode_ = SortedSetRangeMode::kRank;
  bool reverse_ = false;
  std::int64_t first_ = 0;
  std::int64_t last_ = -1;
  SortedSetScoreBound minimum_score_{};
  SortedSetScoreBound maximum_score_{};
  SortedSetLexBound minimum_lex_{};
  SortedSetLexBound maximum_lex_{};
  bool limit_ = false;
  std::int64_t offset_ = 0;
  std::int64_t count_ = -1;
  // Pop returns/removes up to pop_count_ endpoints, reverse_ selecting max.
  // Scan keeps digest-prefix cursor ordering and completes a collision bucket
  // even when its cardinality exceeds the COUNT examination hint. MATCH does
  // not change which prefixes were examined. A zero next cursor means EOF.
  std::uint64_t pop_count_ = 1;
  std::uint64_t scan_cursor_ = 0;
  std::uint64_t scan_count_ = 10;
  std::string_view scan_pattern_ = "*";
  // Explicit permission from a single-key ZADD/ZREM/ZINCRBY caller to prepare
  // a new collection or small compact update without the store-state mutex.
  // New grouped values include preparation of both indexes. The exclusive key
  // hold and database admission remain owned by the caller through publication;
  // storage independently checks physical eligibility and revalidates before
  // publishing or returning a no-op.
  // Internal/multi-key operations and GEOADD leave this disabled.
  bool prepare_unlocked_ = false;
};

struct SortedSetMember {
  RetainedMemoryCharge retained_charge_;
  std::string member_;
  double score_ = 0;
};

struct SortedSetResult {
  // Reply ownership can cross coroutine/worker boundaries. Return vector
  // metadata admission only after the vectors themselves have been destroyed.
  RetainedMemoryCharge retained_charge_;
  bool key_exists_ = false;
  std::uint64_t length_ = 0;
  std::uint64_t added_ = 0;
  std::uint64_t changed_ = 0;
  std::optional<double> incremented_;
  std::vector<std::optional<double>> scores_;
  std::optional<std::uint64_t> rank_;
  std::optional<double> rank_score_;
  std::uint64_t count_ = 0;
  std::uint64_t next_cursor_ = 0;
  // Each member owns its string admission; retained_charge_ above owns vector
  // metadata. Output survives awaits/worker hops without releasing its budget.
  std::vector<SortedSetMember> members_;
};

}  // namespace keylane::storage
