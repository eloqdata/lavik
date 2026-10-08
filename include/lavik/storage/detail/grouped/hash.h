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
#include <array>
#include <cassert>
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
#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "lavik/containers/cow_map.h"
#include "lavik/local_shared_ptr.h"
#include "lavik/memory.h"
#include "lavik/storage/detail/collection_limits.h"
#include "lavik/storage/detail/hash_codec.h"
#include "lavik/storage/scan_hash_map.h"

namespace lavik::storage {

// Shared auxiliary identity: Hash prefixes use the HIGH bits of the persisted
// field hash; ordered pages use {nonzero page id, 0}. The {0, 0} identity is
// reserved for the unsplit Hash range, keeping the two graphs disjoint.
struct GroupedRecordId {
  std::uint64_t prefix_ = 0;
  std::uint8_t bits_ = 0;

  // These helpers describe Hash ranges only; an ordered page is not a prefix.
  // Empty active leaves still own their range, while split parents retire it.
  bool IsHashPrefix() const noexcept;
  bool ContainsHash(std::uint64_t hash) const noexcept;
  std::uint64_t LastHash() const noexcept;
  auto operator<=>(const GroupedRecordId&) const noexcept = default;
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

inline constexpr std::size_t kGroupedHashRootBytes = 64;
inline constexpr std::size_t kHashGroupHeaderBytes = 48;
inline constexpr std::size_t kHashGroupPayloadLimit =
    kMaxRecordPayloadBytes - kHashGroupHeaderBytes;

enum class HashGroupEditKind { kSet, kSetIfAbsent, kDelete };
struct HashGroupEdit;

// Command-owned, fully checked group payload for a small replacement leaf,
// including its envelope. Inline writers borrow these bytes through the write
// instead of allocating and copying another complete serialization.
// Only ApplyHashGroupEdits can construct this certificate. No borrowed request
// or read-buffer data survives it; it is never retained in the resident index.
class PreparedHashGroupPayload {
 public:
  // Compact Hash value bytes, empty for a leaf with no fields. Demotion uses
  // this view to decode fields without treating the group envelope as data.
  std::string_view bytes() const noexcept {
    return std::string_view(bytes_).substr(kHashGroupHeaderBytes);
  }
  // Complete group payload, borrowed for serialization through an inline write
  // or the existing bounded extent cursor.
  std::string_view record_payload() const noexcept { return bytes_; }
  std::uint32_t count() const noexcept { return count_; }
  GroupedRecordId id() const noexcept { return id_; }
  std::uint64_t incarnation() const noexcept { return incarnation_; }

 private:
  friend absl::StatusOr<HashGroupEdit> ApplyHashGroupEdits(
      std::string_view, const DigestSeed&, HashGroupEditKind,
      std::span<const HashEntryView>);
  PreparedHashGroupPayload(std::string bytes, std::uint32_t count,
                           GroupedRecordId id, std::uint64_t incarnation)
      : bytes_(std::move(bytes)),
        count_(count),
        id_(id),
        incarnation_(incarnation) {}
  std::string bytes_;
  std::uint32_t count_;
  GroupedRecordId id_;
  std::uint64_t incarnation_;
};

struct HashGroupSnapshot {
  std::uint64_t incarnation_ = 0;
  GroupedRecordId id_{};
  bool retired_ = false;
  HashValue value_;
  // Mutually exclusive with value_; the encoder checks this invariant.
  std::optional<PreparedHashGroupPayload> prepared_;
  std::size_t field_count() const noexcept {
    return prepared_ ? prepared_->count() : value_.entries_.size();
  }
};

struct HashGroupEdit {
  bool changed_ = false;
  std::uint64_t added_ = 0;
  std::uint64_t removed_ = 0;
  // Empty for no-ops. A split returns complete leaves; the caller publishes a
  // retirement marker for the original leaf in the same command decision.
  std::vector<HashGroupSnapshot> leaves_;
};

// Apply ordered operands to one encoded leaf, checking all old fields for
// uniqueness and routing. HSET uses last-value-wins, NX uses first-value-wins,
// and repeated removals count once. Inputs need only survive this call. Small
// replacements copy views directly into checked bytes; a single edit copies
// unchanged encoded spans without an entry-position table. Splits and oversized
// entries retain the bounded-state owned encoder path. Caller admits page and
// operand scratch before calling; this function performs no storage writes.
absl::StatusOr<HashGroupEdit> ApplyHashGroupEdits(
    std::string_view payload, const DigestSeed& seed, HashGroupEditKind kind,
    std::span<const HashEntryView> edits);

// A bounded-state serializer for inline records and extent writers. Create
// validates owned fields or consumes the checked prepared payload. The caller
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
  GroupedRecordId id_{};
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

// Scans a loader-verified envelope without owning its fields or values. Count
// must come from that envelope; id and seed must come from the checked route.
// Validates all framing, duplicate fields and persisted-seed routing. The
// synchronous visitor borrows payload and may only update unpublished scratch:
// a later entry can invalidate the page, so discard its effects on failure.
// The caller admits page scratch before this call and keeps payload alive.
absl::Status VisitHashGroupFields(
    std::string_view payload, std::uint32_t field_count, GroupedRecordId id,
    const DigestSeed& seed,
    absl::FunctionRef<absl::Status(const HashEntryView&)> visitor);

// Splits one complete leaf into complete replacement leaves. The input is
// scratch, never the published directory. A large indivisible field or a full
// 64-bit collision may exceed target_bytes; it is never fragmented into a
// field-level read-time log. Empty siblings preserve total routing coverage.
// If more than one leaf is returned, the caller must also retire the input
// leaf in the SAME atomic batch. Nothing is published by this function.
absl::StatusOr<std::vector<HashGroupSnapshot>> SplitHashGroup(
    HashGroupSnapshot group, const DigestSeed& seed,
    std::size_t target_bytes = kCollectionGroupTargetBytes);

// Builds a full Hash promotion as group snapshots. It deliberately has no
// storage side effects: failure leaves the compact source authoritative.
absl::StatusOr<std::vector<HashGroupSnapshot>> GroupHashValue(
    HashValue value, std::uint64_t incarnation, const DigestSeed& seed,
    std::size_t target_bytes = kCollectionGroupTargetBytes);

// Common auxiliary metadata for Hash prefixes and ordered pages, after record
// identity validation. Ordered links and bounds live in RecoveredOrderedGroup;
// external payloads are checked after selecting the reachable graph.
// record_token is caller-owned identity for its compact location,
// not a persisted pointer. Superseded candidates are discarded by logical seq
// BEFORE physical LSN; a relocation never wins over a newer logical mutation.
struct RecoveredGroupedRecord {
  std::uint64_t incarnation_ = 0;
  GroupedRecordId id_{};
  std::uint64_t sequence_ = 0;
  std::uint64_t lsn_ = 0;
  std::uint64_t txid_ = 0;
  // A command-local auxiliary batch can be aborted while its surrounding
  // EXEC transaction commits. Both independent decisions must be present.
  std::uint64_t batch_txid_ = 0;
  // Logical size from the auxiliary header: fields, ordered records, or String
  // bytes. Ordered recovery projects it into the page's item_count_.
  std::uint64_t field_count_ = 0;
  // Complete encoded group payload, including its envelope. Zero is reserved
  // for callers that build a directory without physical payload metadata.
  std::uint64_t encoded_bytes_ = 0;
  std::uint64_t record_token_ = 0;
  bool retired_ = false;
};

// Hash routing specializes the metadata container without coupling its AVL
// and overlay implementation to recovery records or field payloads.
template <typename Key>
using HashGroupMap = CowMap<Key, RecoveredGroupedRecord>;

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
      std::span<const RecoveredGroupedRecord> candidates,
      const absl::flat_hash_set<std::uint64_t>& committed_txids);

  // The second argument is the containing root's source command sequence C;
  // root.revision_ is the independent local group revision R. C may repeat
  // inside a replay envelope, but must not decrease; R must strictly advance.
  // Applies only changed complete leaves/retired parents to an unpublished
  // metadata view. All candidates are already transaction-adjudicated by the
  // caller; this method never manufactures a durable commit decision.
  absl::StatusOr<HashGroupDirectory> Apply(
      const GroupedHashRoot& root, std::uint64_t sequence,
      std::span<const RecoveredGroupedRecord> changes) const;

  const RecoveredGroupedRecord* Find(std::string_view field) const noexcept;
  const GroupedHashRoot& root() const noexcept { return root_; }
  std::uint64_t sequence() const noexcept { return sequence_; }
  std::uint64_t command_sequence() const noexcept { return command_sequence_; }
  // Sum of active primary group payloads; retirement markers are excluded.
  std::uint64_t total_group_bytes() const noexcept {
    return total_group_bytes_;
  }
  const HashGroupMap<std::uint64_t>& groups() const noexcept { return groups_; }
  const HashGroupMap<GroupedRecordId>& retired_groups() const noexcept {
    return retired_;
  }

 private:
  GroupedHashRoot root_;
  std::uint64_t sequence_ = 0;
  std::uint64_t command_sequence_ = 0;
  std::uint64_t total_group_bytes_ = 0;
  HashGroupMap<std::uint64_t> groups_;
  HashGroupMap<GroupedRecordId> retired_;
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
    std::size_t target_bytes = kCollectionGroupTargetBytes);

}  // namespace lavik::storage
