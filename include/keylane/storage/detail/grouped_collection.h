#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/status/statusor.h"
#include "keylane/storage/format.h"

namespace keylane::storage {

// Sets use the Hash prefix directory with empty field values. These ordered
// pages are for Lists and Sorted Sets only: hash-prefix order cannot implement
// either List rank or Sorted Set (score, binary member) order.
enum class OrderedCollectionKind : std::uint8_t { kList = 1, kSortedSet = 2 };

struct OrderedCollectionEntry {
  std::string value_;
  double score_ = 0;  // Lists require positive zero in the durable encoding.
  bool operator==(const OrderedCollectionEntry&) const noexcept = default;
};

struct OrderedCollectionRoot {
  OrderedCollectionKind kind_ = OrderedCollectionKind::kList;
  std::uint64_t incarnation_ = 0;
  std::uint64_t item_count_ = 0;
  std::uint64_t first_group_ = 0;
  std::uint64_t last_group_ = 0;
  std::uint64_t next_group_id_ = 1;
  std::uint32_t group_count_ = 0;
  // Root headers carry the replication command sequence; pages use this
  // independent revision so repeated mutations in one replayed command are
  // distinguishable. Zero denotes the command sequence for standalone codecs.
  std::uint64_t revision_ = 0;
  bool operator==(const OrderedCollectionRoot&) const noexcept = default;
};

// An id is never reused within an incarnation. Links are part of the complete
// page snapshot; splitting/removing a page also writes affected neighbours in
// the same transaction. A page contains complete values, never a delta log.
struct OrderedGroupSnapshot {
  OrderedCollectionKind kind_ = OrderedCollectionKind::kList;
  std::uint64_t incarnation_ = 0;
  std::uint64_t id_ = 0;
  std::uint64_t previous_ = 0;
  std::uint64_t next_ = 0;
  bool retired_ = false;
  std::vector<OrderedCollectionEntry> entries_;
};

inline constexpr std::size_t kOrderedGroupHeaderBytes = 64;
inline constexpr std::size_t kOrderedCollectionRootBytes = 72;
inline constexpr std::size_t kOrderedGroupTargetBytes = 8192;

struct OrderedGroupMetadata {
  OrderedCollectionKind kind_ = OrderedCollectionKind::kList;
  std::uint64_t incarnation_ = 0;
  std::uint64_t id_ = 0;
  std::uint64_t previous_ = 0;
  std::uint64_t next_ = 0;
  std::uint32_t item_count_ = 0;
  bool retired_ = false;
};

// Recovery reads this checked envelope only after the outer-header winner and
// every extent checksum are known. Entry payload syntax/order is validated by
// DecodeOrderedGroup when loaded; this parser never claims to inspect values.
absl::StatusOr<OrderedGroupMetadata> DecodeOrderedGroupMetadata(
    std::string_view prefix, std::size_t encoded_bytes);

// The cursor validates before emitting any data and borrows immutable entry
// strings. An extent writer can consume it without a second full-value copy.
// The input snapshot must outlive the cursor and remain unchanged.
class OrderedGroupEncoder {
 public:
  static absl::StatusOr<OrderedGroupEncoder> Create(
      const OrderedGroupSnapshot& group);
  std::size_t encoded_bytes() const noexcept { return encoded_bytes_; }
  // Empty spans are data; nullopt is EOF. Header spans expire on Next/move.
  std::optional<std::string_view> Next() noexcept;

 private:
  const OrderedGroupSnapshot* group_ = nullptr;
  std::array<char, kOrderedGroupHeaderBytes> header_{};
  std::array<char, 12> entry_header_{};
  std::size_t encoded_bytes_ = 0;
  std::size_t entry_ = 0;
  unsigned phase_ = 0;
};

absl::StatusOr<std::string> EncodeOrderedCollectionRoot(
    const OrderedCollectionRoot& root);
absl::StatusOr<OrderedCollectionRoot> DecodeOrderedCollectionRoot(
    std::string_view bytes);
absl::StatusOr<std::string> EncodeOrderedGroup(
    const OrderedGroupSnapshot& group);
absl::StatusOr<OrderedGroupSnapshot> DecodeOrderedGroup(std::string_view bytes);

// Binary member ordering breaks score ties. NaN is invalid; infinities are
// valid. Equal -0/+0 scores have the same order, matching Redis numeric order.
bool OrderedEntryLess(const OrderedCollectionEntry& left,
                      const OrderedCollectionEntry& right) noexcept;
absl::Status ValidateOrderedGroupBoundary(const OrderedGroupSnapshot& left,
                                          const OrderedGroupSnapshot& right);

// Only retained metadata belongs in the directory. Physical checksums,
// enclosing key/DB/replication epochs and page payload checks are the adapter's
// responsibility. record_token is caller-owned identity, never a pointer on
// disk. Sorted Set ordering across pages must additionally be checked using
// ValidateOrderedGroupBoundary when their contents are read or recovered.
struct RecoveredOrderedGroup {
  std::uint64_t incarnation_ = 0;
  std::uint64_t id_ = 0;
  std::uint64_t previous_ = 0;
  std::uint64_t next_ = 0;
  std::uint64_t sequence_ = 0;
  std::uint64_t lsn_ = 0;
  std::uint64_t txid_ = 0;
  std::uint64_t batch_txid_ = 0;
  std::uint64_t item_count_ = 0;
  std::uint64_t record_token_ = 0;
  bool retired_ = false;
};

class OrderedGroupDirectory {
 public:
  // The caller first adjudicates the root's transaction. Only committed
  // candidates at/before that root sequence can participate; missing links,
  // cycles, disconnected pages and aggregate count mismatches are corruption.
  static absl::StatusOr<OrderedGroupDirectory> Recover(
      const OrderedCollectionRoot& root, std::uint64_t root_sequence,
      std::span<const RecoveredOrderedGroup> candidates,
      const absl::flat_hash_set<std::uint64_t>& committed_txids,
      std::uint64_t command_sequence = 0);

  // Complete after-image metadata, not value deltas. Existing adjudicated
  // pages and retirement evidence remain candidates; only changed ids replace
  // them. The physical side index independently COWs only touched pages.
  absl::StatusOr<OrderedGroupDirectory> Apply(
      const OrderedCollectionRoot& root, std::uint64_t revision,
      std::span<const RecoveredOrderedGroup> changed,
      std::uint64_t command_sequence) const;

  struct Position {
    std::size_t group_index_;
    std::uint64_t offset_;
  };
  std::optional<Position> FindRank(std::uint64_t rank) const noexcept;
  const OrderedCollectionRoot& root() const noexcept { return root_; }
  std::uint64_t sequence() const noexcept { return sequence_; }
  std::uint64_t command_sequence() const noexcept { return command_sequence_; }
  const RecoveredOrderedGroup* Find(std::uint64_t id) const noexcept;
  const RecoveredOrderedGroup* FindRecord(std::uint64_t id) const noexcept;
  const std::vector<RecoveredOrderedGroup>& groups() const noexcept {
    return groups_;
  }
  const std::vector<RecoveredOrderedGroup>& retired_groups() const noexcept {
    return retired_;
  }
  // Exact owned vector allocation bytes after recovery. The enclosing side
  // object must reserve/account these before publishing a retained directory;
  // this side-effect-free codec does not own a worker memory budget.
  std::size_t RetainedBytes() const noexcept {
    return groups_.capacity() * sizeof(RecoveredOrderedGroup) +
           retired_.capacity() * sizeof(RecoveredOrderedGroup) +
           ids_.capacity() * sizeof(std::pair<std::uint64_t, std::size_t>) +
           ends_.capacity() * sizeof(std::uint64_t);
  }

 private:
  OrderedCollectionRoot root_;
  std::uint64_t sequence_ = 0;
  std::uint64_t command_sequence_ = 0;
  std::vector<RecoveredOrderedGroup> groups_;
  std::vector<RecoveredOrderedGroup> retired_;
  std::vector<std::pair<std::uint64_t, std::size_t>> ids_;
  std::vector<std::uint64_t> ends_;
};

struct OrderedGroupSplit {
  std::uint64_t next_group_id_ = 0;
  std::vector<OrderedGroupSnapshot> groups_;
};

// Preserves the first page id and allocates monotonically increasing ids for
// later pages. An indivisible 512 MiB item is allowed to exceed target_bytes;
// the ordinary extent layer stores it. The caller must update the following
// neighbour's previous link if this returns more than one page.
absl::StatusOr<OrderedGroupSplit> SplitOrderedGroup(
    OrderedGroupSnapshot group, std::uint64_t next_group_id,
    std::size_t target_bytes = kOrderedGroupTargetBytes);

struct LoadedOrderedGroup {
  std::uint64_t sequence_ = 0;
  OrderedGroupSnapshot snapshot_;
};

struct OrderedCollectionMutationPlan {
  OrderedCollectionRoot root_;
  std::uint64_t expected_sequence_ = 0;
  bool changed_ = false;
  bool delete_key_ = false;
  std::vector<OrderedGroupSnapshot> writes_;
};

// Atomically replaces [rank, rank + erase_count) by entries. This is the
// storage primitive for List push/pop/insert/remove and Sorted Set insertion,
// deletion or score repositioning. The caller supplies all intersected pages
// plus their immediate neighbours; only changed complete pages are returned.
// Noncontiguous List removals or a Sorted Set reposition may be expressed as
// one enclosing splice, preserving intervening entries. This first adapter
// intentionally has no resident per-member Sorted Set index: finding a member
// can require scanning pages. A range read uses FindRank and follows pages.
// The Sorted Set caller must establish member uniqueness outside the loaded
// splice region; this primitive cannot verify unloaded member contents.
//
// The caller must commit every returned page/retirement and root together,
// revalidate expected_sequence under its key lock, preserve causal durability
// of the preceding root, and retire the complete old incarnation when
// delete_key is set. The planner performs no storage or memory publication.
absl::StatusOr<OrderedCollectionMutationPlan> PlanOrderedCollectionSplice(
    const OrderedGroupDirectory& directory,
    std::vector<LoadedOrderedGroup> loaded_groups, std::uint64_t rank,
    std::uint64_t erase_count, std::vector<OrderedCollectionEntry> entries,
    std::size_t target_bytes = kOrderedGroupTargetBytes);

}  // namespace keylane::storage
