#pragma once

#include <algorithm>
#include <array>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "keylane/memory.h"
#include "keylane/storage/detail/hash_codec.h"
#include "keylane/storage/scan_hash_map.h"

namespace keylane::storage {

// Prefixes use the HIGH bits of the persisted-seed field hash. Empty leaves
// still own their range: deleting the last field must not make future inserts
// unroutable. A retired leaf, in contrast, relinquishes its range after split.
struct HashGroupId {
  std::uint64_t prefix_ = 0;
  std::uint8_t bits_ = 0;

  bool valid() const noexcept;
  bool contains(std::uint64_t hash) const noexcept;
  std::uint64_t last() const noexcept;
  auto operator<=>(const HashGroupId&) const noexcept = default;
};

// The containing keyed record supplies database/replication epochs, source
// command sequence, TTL, and the transaction decision. Incarnation changes
// whenever the object is recreated; revision advances on every group update.
struct GroupedHashRoot {
  std::uint64_t incarnation_ = 0;
  DigestSeed seed_{};
  std::uint64_t field_count_ = 0;
  std::uint32_t group_count_ = 0;
  // Local logical group order, distinct from the source command sequence in
  // the containing root record. One replay envelope may write this key more
  // than once. Every mutation allocates a fresh global command-batch id;
  // physical relocation preserves it and cold recovery raises that id floor.
  std::uint64_t revision_ = 0;

  bool operator==(const GroupedHashRoot&) const noexcept = default;
};

struct HashGroupSnapshot {
  std::uint64_t incarnation_ = 0;
  HashGroupId id_{};
  bool retired_ = false;
  HashValue value_;
};

inline constexpr std::size_t kGroupedHashRootBytes = 64;
inline constexpr std::size_t kHashGroupHeaderBytes = 48;
inline constexpr std::size_t kHashGroupPayloadLimit =
    kMaxRecordPayloadBytes - kHashGroupHeaderBytes;

// A bounded-state serializer for inline records and extent writers. Create
// validates the complete snapshot before any bytes can be emitted. The caller
// owns the snapshot and must keep it alive and immutable until serialization
// ends. Field/value spans borrow the original strings; no full-size encoded
// copy is needed by a consumer that fills one extent at a time.
class HashGroupEncoder {
 public:
  static absl::StatusOr<HashGroupEncoder> Create(
      const HashGroupSnapshot& group);
  std::size_t encoded_bytes() const noexcept { return encoded_bytes_; }
  // nullopt means end; an empty span is a valid empty field or value. Metadata
  // spans are valid until the next call or a move/destruction of this cursor.
  std::optional<std::string_view> Next() noexcept;

 private:
  const HashGroupSnapshot* group_ = nullptr;
  std::array<char, kHashGroupHeaderBytes + kHashValueHeaderBytes> header_{};
  std::array<char, 8> lengths_{};
  std::size_t encoded_bytes_ = 0;
  std::size_t entry_ = 0;
  unsigned phase_ = 0;
};

// These versioned, little-endian payload codecs are independent of the outer
// record framing. Outer records must checksum these COMPLETE payloads and
// include all group writes in the same durability/commit boundary as the root.
absl::StatusOr<std::string> EncodeGroupedHashRoot(const GroupedHashRoot& root);
absl::StatusOr<GroupedHashRoot> DecodeGroupedHashRoot(std::string_view bytes);
struct HashGroupMetadata {
  std::uint64_t incarnation_ = 0;
  HashGroupId id_{};
  std::uint32_t field_count_ = 0;
  bool retired_ = false;
};

// Checks only the envelope, for recovery of extent-backed groups without
// materializing their values. The caller must independently verify the
// physical payload checksum and extent identity; this is not a payload check.
absl::StatusOr<HashGroupMetadata> DecodeHashGroupMetadata(
    std::string_view prefix, std::size_t encoded_bytes);
absl::StatusOr<std::string> EncodeHashGroup(const HashGroupSnapshot& group);
absl::StatusOr<HashGroupSnapshot> DecodeHashGroup(std::string_view bytes);

inline constexpr std::size_t kHashGroupTargetBytes = 8 * 1024;
inline constexpr std::size_t kGroupedHashPromotionBytes = 16 * 1024;

// Splits one complete leaf into complete replacement leaves. The input is
// scratch, never the published directory. A large indivisible field or a full
// 64-bit collision may exceed target_bytes; it is never fragmented into a
// field-level read-time log. Empty siblings preserve total routing coverage.
// If more than one leaf is returned, the caller must also retire the input
// leaf in the SAME atomic batch. Nothing is published by this function.
absl::StatusOr<std::vector<HashGroupSnapshot>> SplitHashGroup(
    HashGroupSnapshot group, const DigestSeed& seed,
    std::size_t target_bytes = kHashGroupTargetBytes);

// Builds a full Hash promotion as group snapshots. It deliberately has no
// storage side effects: failure leaves the compact source authoritative.
absl::StatusOr<std::vector<HashGroupSnapshot>> GroupHashValue(
    HashValue value, std::uint64_t incarnation, const DigestSeed& seed,
    std::size_t target_bytes = kHashGroupTargetBytes);

// Recovery input after physical record/checksum and enclosing key/epoch
// validation. record_token is caller-owned identity for its compact location,
// not a persisted pointer. Superseded candidates are discarded by logical seq
// BEFORE physical LSN; a relocation never wins over a newer logical mutation.
struct RecoveredHashGroup {
  std::uint64_t incarnation_ = 0;
  HashGroupId id_{};
  std::uint64_t sequence_ = 0;
  std::uint64_t lsn_ = 0;
  std::uint64_t txid_ = 0;
  // A command-local auxiliary batch can be aborted while its surrounding
  // EXEC transaction commits. Both independent decisions must be present.
  std::uint64_t batch_txid_ = 0;
  std::uint64_t field_count_ = 0;
  std::uint64_t record_token_ = 0;
  bool retired_ = false;
};

// Immutable AVL metadata, one node per group. Copies retain one root and an
// update allocates only the logarithmic search path. Nodes are admitted and
// charged independently, so old snapshot readers retain exactly the nodes
// they still own; no mutation log is replayed by a later read.
template <typename Key>
class HashGroupMap {
  struct Node;
  using Link = std::shared_ptr<const Node>;
  struct Node {
    std::pair<const Key, RecoveredHashGroup> entry_;
    Link left_, right_;
    std::size_t size_;
    unsigned height_;
    Node(Key key, RecoveredHashGroup value, Link left, Link right)
        : entry_(key, value),
          left_(std::move(left)),
          right_(std::move(right)),
          size_(1 + Size(left_) + Size(right_)),
          height_(1 + std::max(Height(left_), Height(right_))) {}
  };

 public:
  class const_iterator {
   public:
    using value_type = std::pair<const Key, RecoveredHashGroup>;
    using reference = const value_type&;
    using pointer = const value_type*;
    reference operator*() const { return path_[depth_ - 1]->entry_; }
    pointer operator->() const { return &operator*(); }
    const_iterator& operator++() {
      const auto* node = path_[depth_ - 1];
      if (node->right_) {
        node = node->right_.get();
        while (node) {
          path_[depth_++] = node;
          node = node->left_.get();
        }
      } else {
        --depth_;
        while (depth_ && path_[depth_ - 1]->right_.get() == node) {
          node = path_[--depth_];
        }
      }
      return *this;
    }
    bool operator==(const const_iterator& other) const {
      return (depth_ ? path_[depth_ - 1] : nullptr) ==
             (other.depth_ ? other.path_[other.depth_ - 1] : nullptr);
    }

   private:
    friend class HashGroupMap;
    // An AVL tree containing at most UINT32_MAX groups is far shallower than
    // this fixed stack. Iteration never allocates retained/scratch memory.
    std::array<const Node*, 96> path_{};
    unsigned depth_ = 0;
  };

  std::size_t size() const noexcept { return Size(root_); }
  bool empty() const noexcept { return !root_; }
  const_iterator begin() const {
    const_iterator it;
    const auto* node = root_.get();
    while (node) {
      it.path_[it.depth_++] = node;
      node = node->left_.get();
    }
    return it;
  }
  const_iterator end() const { return {}; }
  const_iterator find(Key key) const {
    const_iterator it;
    const auto* node = root_.get();
    while (node) {
      it.path_[it.depth_++] = node;
      if (key == node->entry_.first) return it;
      node = key < node->entry_.first ? node->left_.get() : node->right_.get();
    }
    return end();
  }
  const RecoveredHashGroup& at(Key key) const {
    const auto it = find(key);
    if (it == end()) throw std::out_of_range("group directory key");
    return it->second;
  }
  const RecoveredHashGroup* Floor(Key key) const noexcept {
    const Node* found = nullptr;
    auto* node = root_.get();
    while (node) {
      if (node->entry_.first <= key) {
        found = node;
        node = node->right_.get();
      } else
        node = node->left_.get();
    }
    return found ? &found->entry_.second : nullptr;
  }
  absl::Status Set(Key key, RecoveredHashGroup value) {
    auto next = SetNode(root_, key, value);
    if (!next.ok()) return next.status();
    root_ = std::move(*next);
    return absl::OkStatus();
  }
  absl::Status Erase(Key key) {
    auto next = EraseNode(root_, key);
    if (!next.ok()) return next.status();
    root_ = std::move(*next);
    return absl::OkStatus();
  }

 private:
  static std::size_t Size(const Link& node) { return node ? node->size_ : 0; }
  static unsigned Height(const Link& node) { return node ? node->height_ : 0; }
  static absl::StatusOr<Link> Make(Key key, RecoveredHashGroup value, Link left,
                                   Link right) {
    auto reservation =
        TryReserveMemory(AllocatorUsableSizeForRequest(sizeof(Node) + 1024));
    if (!reservation) {
      RecordMemoryRejection();
      return absl::ResourceExhaustedError(
          "OOM group routing exceeds maxmemory");
    }
    RetainedAllocationDomain domain{
        .owner_shard_ = CurrentMemoryAccountingShard(),
        .externally_admitted_ = true,
        .externally_accounted_ = false};
    return Link(std::allocate_shared<Node>(RetainedAllocator<Node>(domain), key,
                                           value, std::move(left),
                                           std::move(right)));
  }
  static absl::StatusOr<Link> Balance(Key key, RecoveredHashGroup value,
                                      Link left, Link right) {
    if (Height(left) > Height(right) + 1) {
      if (Height(left->left_) >= Height(left->right_)) {
        auto next = Make(key, value, left->right_, right);
        if (!next.ok()) return next.status();
        return Make(left->entry_.first, left->entry_.second, left->left_,
                    *next);
      }
      const auto pivot = left->right_;
      auto a = Make(left->entry_.first, left->entry_.second, left->left_,
                    pivot->left_);
      if (!a.ok()) return a.status();
      auto b = Make(key, value, pivot->right_, right);
      if (!b.ok()) return b.status();
      return Make(pivot->entry_.first, pivot->entry_.second, *a, *b);
    }
    if (Height(right) > Height(left) + 1) {
      if (Height(right->right_) >= Height(right->left_)) {
        auto next = Make(key, value, left, right->left_);
        if (!next.ok()) return next.status();
        return Make(right->entry_.first, right->entry_.second, *next,
                    right->right_);
      }
      const auto pivot = right->left_;
      auto a = Make(key, value, left, pivot->left_);
      if (!a.ok()) return a.status();
      auto b = Make(right->entry_.first, right->entry_.second, pivot->right_,
                    right->right_);
      if (!b.ok()) return b.status();
      return Make(pivot->entry_.first, pivot->entry_.second, *a, *b);
    }
    return Make(key, value, std::move(left), std::move(right));
  }
  static absl::StatusOr<Link> SetNode(const Link& node, Key key,
                                      RecoveredHashGroup value) {
    if (!node || key == node->entry_.first) {
      return Make(key, value, node ? node->left_ : Link{},
                  node ? node->right_ : Link{});
    }
    const bool left = key < node->entry_.first;
    auto child = SetNode(left ? node->left_ : node->right_, key, value);
    if (!child.ok()) return child.status();
    return Balance(node->entry_.first, node->entry_.second,
                   left ? *child : node->left_, left ? node->right_ : *child);
  }
  static absl::StatusOr<Link> EraseNode(const Link& node, Key key) {
    if (!node) return Link{};
    if (key == node->entry_.first) {
      if (!node->left_) return node->right_;
      if (!node->right_) return node->left_;
      auto* successor = node->right_.get();
      while (successor->left_) successor = successor->left_.get();
      auto right = EraseNode(node->right_, successor->entry_.first);
      if (!right.ok()) return right.status();
      return Balance(successor->entry_.first, successor->entry_.second,
                     node->left_, *right);
    }
    const bool left = key < node->entry_.first;
    auto child = EraseNode(left ? node->left_ : node->right_, key);
    if (!child.ok()) return child.status();
    return Balance(node->entry_.first, node->entry_.second,
                   left ? *child : node->left_, left ? node->right_ : *child);
  }
  Link root_;
};

// Immutable routing view produced only after complete recovery validation.
// It stores one entry per GROUP, not per field. Persistent metadata nodes
// admit and charge their retained memory; physical pins and disk retirement
// remain the storage adapter's job.
class HashGroupDirectory {
 public:
  // The root must already be a transaction-adjudicated winner. Groups from
  // other incarnations, future mutations and uncommitted transactions are
  // excluded. A nested command needs BOTH outer and batch decisions. Gaps,
  // overlaps and aggregate mismatches fail closed rather than
  // silently selecting a partial value after an interrupted structural write.
  // root_sequence is source command C; candidate.sequence_ is group revision R
  // and is bounded by root.revision_, never by C.
  static absl::StatusOr<HashGroupDirectory> Recover(
      const GroupedHashRoot& root, std::uint64_t root_sequence,
      std::span<const RecoveredHashGroup> candidates,
      const absl::flat_hash_set<std::uint64_t>& committed_txids);

  // The second argument is the containing root's source command sequence C;
  // root.revision_ is the independent local group revision R. C may repeat
  // inside a replay envelope, but must not decrease; R must strictly advance.
  // Applies only changed complete leaves/retired parents to an unpublished
  // metadata view. All candidates are already transaction-adjudicated by the
  // caller; this method never manufactures a durable commit decision.
  absl::StatusOr<HashGroupDirectory> Apply(
      const GroupedHashRoot& root, std::uint64_t sequence,
      std::span<const RecoveredHashGroup> changes) const;

  const RecoveredHashGroup* Find(std::string_view field) const noexcept;
  const GroupedHashRoot& root() const noexcept { return root_; }
  std::uint64_t sequence() const noexcept { return sequence_; }
  std::uint64_t command_sequence() const noexcept { return command_sequence_; }
  const HashGroupMap<std::uint64_t>& groups() const noexcept { return groups_; }
  const HashGroupMap<HashGroupId>& retired_groups() const noexcept {
    return retired_;
  }

 private:
  GroupedHashRoot root_;
  std::uint64_t sequence_ = 0;
  std::uint64_t command_sequence_ = 0;
  HashGroupMap<std::uint64_t> groups_;
  HashGroupMap<HashGroupId> retired_;
};

enum class HashGroupMutationKind { kSet, kSetIfAbsent, kDelete };

struct LoadedHashGroup {
  std::uint64_t sequence_ = 0;
  HashGroupSnapshot snapshot_;
};

struct HashGroupMutationPlan {
  GroupedHashRoot root_;
  // Publication must revalidate this version under the exclusive key lock.
  // It must also preserve the preceding root's transaction dependency until
  // that decision is durable; an uncommitted structural write is not a base
  // on which an independently durable update may be built.
  std::uint64_t expected_sequence_ = 0;
  std::uint64_t affected_fields_ = 0;  // Redis HSET additions / HDEL removals.
  bool changed_ = false;
  bool delete_key_ = false;
  std::vector<HashGroupSnapshot> writes_;
};

// Plans a command using only its affected, validated group snapshots. Inputs
// are scratch owned by this call; failure cannot mutate the current directory.
// All returned writes and the small root after-image need one atomic storage
// batch (or the caller's outer EXEC/Lua transaction). No unchanged group is
// included. On deleting the final field the caller publishes a KEY tombstone
// and retires the complete old incarnation after that tombstone's fence.
absl::StatusOr<HashGroupMutationPlan> PlanHashGroupMutation(
    const HashGroupDirectory& directory,
    std::vector<LoadedHashGroup> loaded_groups, HashGroupMutationKind kind,
    std::span<const std::string_view> fields,
    std::span<const std::string_view> values = {},
    std::size_t target_bytes = kHashGroupTargetBytes);

}  // namespace keylane::storage
