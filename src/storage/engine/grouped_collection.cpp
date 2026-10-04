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

#include "lavik/storage/detail/grouped_collection.h"

#include <algorithm>
#include <bit>
#include <cmath>
#include <limits>
#include <map>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "absl/container/inlined_vector.h"
#include "absl/numeric/int128.h"
#include "lavik/storage/detail/stream_records.h"

namespace lavik::storage {
namespace {

// Stream trim/append transfers counts across distant pages. A Fenwick index
// stores bounded partial sums instead of materializing every intervening
// cumulative rank after each transfer. It is runtime metadata only.
void BuildStreamRanks(std::vector<std::uint64_t>& counts) {
  for (std::size_t i = counts.size(); i > 1; --i)
    counts[i - 1] -= counts[i - 2];
  for (std::size_t i = 0; i < counts.size(); ++i) {
    const auto parent = i | (i + 1);
    if (parent < counts.size()) counts[parent] += counts[i];
  }
}

constexpr std::string_view kRootMagic = "LOCROOT1";
constexpr std::string_view kGroupMagic = "LOCGRUP1";
constexpr std::size_t kRootBytes = kOrderedCollectionRootBytes;
constexpr std::size_t kEntryHeaderBytes = 12;

template <typename Buffer>
void Store(Buffer& bytes, std::size_t offset, std::uint64_t value,
           unsigned width) {
  for (unsigned i = 0; i < width; ++i)
    bytes[offset + i] = static_cast<char>(value >> (i * 8));
}

std::uint64_t Load(std::string_view bytes, std::size_t offset, unsigned width) {
  std::uint64_t result = 0;
  for (unsigned i = 0; i < width; ++i)
    result |= std::uint64_t(static_cast<unsigned char>(bytes[offset + i]))
              << (i * 8);
  return result;
}

bool ValidKind(OrderedCollectionKind kind) {
  return kind == OrderedCollectionKind::kString ||
         kind == OrderedCollectionKind::kList ||
         kind == OrderedCollectionKind::kSortedSet ||
         kind == OrderedCollectionKind::kStream;
}

bool ValidRoot(const OrderedCollectionRoot& root) {
  if ((root.kind_ == OrderedCollectionKind::kStream) !=
          root.stream_length_.has_value() ||
      (root.stream_length_ && *root.stream_length_ > UINT32_MAX))
    return false;
  if (root.kind_ == OrderedCollectionKind::kString &&
      (root.item_count_ > kMaxStringBytes || root.first_group_ != 1 ||
       root.group_count_ !=
           (root.item_count_ + kStringGroupBytes - 1) / kStringGroupBytes ||
       root.last_group_ != root.group_count_ ||
       root.next_group_id_ != root.last_group_ + 1))
    return false;
  if (root.member_index_ &&
      (root.kind_ != OrderedCollectionKind::kSortedSet ||
       root.member_index_->incarnation_ != root.incarnation_ ||
       root.member_index_->field_count_ != root.item_count_ ||
       root.member_index_->revision_ == 0 ||
       root.member_index_->revision_ > root.revision_))
    return false;
  return ValidKind(root.kind_) && root.incarnation_ != 0 &&
         root.item_count_ != 0 &&
         root.item_count_ <= std::numeric_limits<std::uint32_t>::max() &&
         root.group_count_ != 0 && root.group_count_ <= root.item_count_ &&
         root.first_group_ != 0 && root.last_group_ != 0 &&
         root.first_group_ < root.next_group_id_ &&
         root.last_group_ < root.next_group_id_ &&
         ((root.group_count_ == 1) == (root.first_group_ == root.last_group_));
}

absl::Status ValidateEntries(OrderedCollectionKind kind,
                             std::span<const OrderedCollectionEntry> entries) {
  if (!ValidKind(kind)) return absl::InvalidArgumentError("invalid page kind");
  absl::flat_hash_set<std::string_view> members;
  if (kind == OrderedCollectionKind::kSortedSet)
    members.reserve(entries.size());
  for (std::size_t i = 0; i < entries.size(); ++i) {
    const auto& entry = entries[i];
    if (entry.value_.size() > kMaxStringBytes || std::isnan(entry.score_) ||
        (kind != OrderedCollectionKind::kSortedSet &&
         std::bit_cast<std::uint64_t>(entry.score_) != 0)) {
      return absl::InvalidArgumentError("invalid ordered page entry");
    }
    if (kind == OrderedCollectionKind::kStream) {
      auto key = StreamRecordKey(entry.value_);
      if (!key.ok()) return key.status();
      if (i != 0) {
        auto previous = StreamRecordKey(entries[i - 1].value_);
        if (!previous.ok() || *previous >= *key)
          return absl::InvalidArgumentError(
              "Stream page repeats/unorders a record key");
      }
    }
    if (kind == OrderedCollectionKind::kSortedSet &&
        (!members.insert(entry.value_).second ||
         (i != 0 && !OrderedEntryLess(entries[i - 1], entry)))) {
      return absl::InvalidArgumentError(
          "Sorted Set page is unordered or repeats a member");
    }
  }
  return absl::OkStatus();
}

absl::StatusOr<std::size_t> ValidateGroup(const OrderedGroupSnapshot& group) {
  if (group.incarnation_ == 0 || group.id_ == 0 ||
      group.previous_ == group.id_ || group.next_ == group.id_ ||
      (group.previous_ != 0 && group.previous_ == group.next_) ||
      group.entries_.size() > std::numeric_limits<std::uint32_t>::max() ||
      (group.retired_ ? (!group.entries_.empty() || group.previous_ != 0 ||
                         group.next_ != 0)
                      : group.entries_.empty())) {
    return absl::InvalidArgumentError("invalid ordered page identity or state");
  }
  auto status = ValidateEntries(group.kind_, group.entries_);
  if (!status.ok()) return status;
  if (group.kind_ == OrderedCollectionKind::kString) {
    const auto size = OrderedGroupSize(group);
    if (group.retired_ ||
        (group.entries_.size() != 1 || size == 0 || size > kStringGroupBytes ||
         group.previous_ != group.id_ - 1 ||
         (group.next_ != 0 &&
          (group.next_ != group.id_ + 1 || size != kStringGroupBytes))))
      return absl::InvalidArgumentError("invalid fixed String segment");
    return kOrderedGroupHeaderBytes + size;
  }
  std::size_t bytes = kOrderedGroupHeaderBytes;
  for (const auto& entry : group.entries_) {
    if (bytes > kMaxRecordPayloadBytes - kEntryHeaderBytes ||
        entry.value_.size() >
            kMaxRecordPayloadBytes - kEntryHeaderBytes - bytes) {
      return absl::OutOfRangeError("ordered page exceeds payload limit");
    }
    bytes += kEntryHeaderBytes + entry.value_.size();
  }
  return bytes;
}

bool SameMetadata(const RecoveredOrderedGroup& left,
                  const RecoveredOrderedGroup& right) {
  return left.previous_ == right.previous_ && left.next_ == right.next_ &&
         left.item_count_ == right.item_count_ &&
         left.encoded_bytes_ == right.encoded_bytes_ &&
         left.retired_ == right.retired_ &&
         std::bit_cast<std::uint64_t>(left.min_score_) ==
             std::bit_cast<std::uint64_t>(right.min_score_) &&
         std::bit_cast<std::uint64_t>(left.max_score_) ==
             std::bit_cast<std::uint64_t>(right.max_score_);
}

OrderedGroupSnapshot Retired(const OrderedCollectionRoot& root,
                             std::uint64_t id) {
  return {.kind_ = root.kind_,
          .incarnation_ = root.incarnation_,
          .id_ = id,
          .retired_ = true,
          .entries_ = {}};
}

}  // namespace

void StreamPageMaxKey::Set(std::string_view key) noexcept {
  size_ = static_cast<std::uint8_t>(std::min(key.size(), prefix_.size()));
  exact_ = key.size() <= prefix_.size();
  std::copy_n(key.data(), size_, prefix_.data());
}

std::optional<bool> StreamPageMaxKey::LessThan(
    std::string_view key) const noexcept {
  if (size_ == 0) return std::nullopt;
  const std::string_view prefix(prefix_.data(), size_);
  if (exact_) return prefix < key;
  const auto common = std::min(prefix.size(), key.size());
  const int compared = prefix.substr(0, common).compare(key.substr(0, common));
  if (compared != 0) return compared < 0;
  // A truncated boundary is strictly beyond its own prefix. If the wanted
  // key shares that prefix and is longer, the suffix must be read from disk.
  if (key.size() <= prefix.size()) return false;
  return std::nullopt;
}

std::optional<bool> StreamPageMaxKey::LessThanOrEqual(
    std::string_view key) const noexcept {
  if (size_ == 0) return std::nullopt;
  const std::string_view prefix(prefix_.data(), size_);
  if (exact_) return prefix <= key;
  const auto common = std::min(prefix.size(), key.size());
  const int compared = prefix.substr(0, common).compare(key.substr(0, common));
  if (compared != 0) return compared < 0;
  if (key.size() <= prefix.size()) return false;
  return std::nullopt;
}

bool OrderedEntryLess(const OrderedCollectionEntry& left,
                      const OrderedCollectionEntry& right) noexcept {
  return left.score_ < right.score_ ||
         (left.score_ == right.score_ && left.value_ < right.value_);
}

std::string EncodeSortedSetMemberScore(double score) {
  std::string bytes(8, '\0');
  Store(bytes, 0, std::bit_cast<std::uint64_t>(score), 8);
  return bytes;
}

absl::StatusOr<double> DecodeSortedSetMemberScore(std::string_view bytes) {
  if (bytes.size() != 8)
    return absl::DataLossError("invalid Sorted Set member score size");
  const auto score = std::bit_cast<double>(Load(bytes, 0, 8));
  if (std::isnan(score))
    return absl::DataLossError("NaN in Sorted Set member index");
  return score;
}

absl::StatusOr<std::string> EncodeOrderedCollectionRoot(
    const OrderedCollectionRoot& root) {
  if (!ValidRoot(root))
    return absl::InvalidArgumentError("invalid ordered collection root");
  std::string bytes(kRootBytes, '\0');
  bytes.replace(0, kRootMagic.size(), kRootMagic);
  Store(bytes, 8, 1, 4);
  Store(bytes, 12, static_cast<unsigned>(root.kind_), 1);
  // All shapes are v1. An explicit presence flag, rather than length alone,
  // prevents a truncated indexed root from becoming a valid ordered-only root.
  Store(bytes, 13, root.member_index_ ? 1 : root.stream_length_ ? 2 : 0, 1);
  Store(bytes, 16, root.incarnation_, 8);
  Store(bytes, 24, root.item_count_, 8);
  Store(bytes, 32, root.first_group_, 8);
  Store(bytes, 40, root.last_group_, 8);
  Store(bytes, 48, root.next_group_id_, 8);
  Store(bytes, 56, root.group_count_, 4);
  Store(bytes, 64, root.revision_, 8);
  if (root.member_index_) {
    auto members = EncodeGroupedHashRoot(*root.member_index_);
    if (!members.ok()) return members.status();
    bytes.append(*members);
  }
  if (root.stream_length_) {
    bytes.resize(kGroupedStreamRootBytes);
    Store(bytes, kRootBytes, *root.stream_length_, 8);
  }
  return bytes;
}

absl::StatusOr<OrderedCollectionRoot> DecodeOrderedCollectionRoot(
    std::string_view bytes) {
  if ((bytes.size() != kRootBytes &&
       bytes.size() != kIndexedSortedSetRootBytes &&
       bytes.size() != kGroupedStreamRootBytes) ||
      !bytes.starts_with(kRootMagic) || Load(bytes, 8, 4) != 1 ||
      Load(bytes, 13, 1) != (bytes.size() == kIndexedSortedSetRootBytes ? 1
                             : bytes.size() == kGroupedStreamRootBytes  ? 2
                                                                        : 0) ||
      Load(bytes, 14, 2) != 0 || Load(bytes, 60, 4) != 0) {
    return absl::DataLossError("invalid ordered root encoding");
  }
  OrderedCollectionRoot root{
      .kind_ = static_cast<OrderedCollectionKind>(Load(bytes, 12, 1)),
      .incarnation_ = Load(bytes, 16, 8),
      .item_count_ = Load(bytes, 24, 8),
      .first_group_ = Load(bytes, 32, 8),
      .last_group_ = Load(bytes, 40, 8),
      .next_group_id_ = Load(bytes, 48, 8),
      .group_count_ = static_cast<std::uint32_t>(Load(bytes, 56, 4)),
      .revision_ = Load(bytes, 64, 8)};
  if (bytes.size() == kIndexedSortedSetRootBytes) {
    auto members = DecodeGroupedHashRoot(bytes.substr(kRootBytes));
    if (!members.ok()) return members.status();
    root.member_index_ = *members;
  }
  if (bytes.size() == kGroupedStreamRootBytes)
    root.stream_length_ = Load(bytes, kRootBytes, 8);
  if (!ValidRoot(root)) return absl::DataLossError("invalid ordered root");
  return root;
}

absl::StatusOr<OrderedGroupEncoder> OrderedGroupEncoder::Create(
    const OrderedGroupSnapshot& group) {
  auto size = ValidateGroup(group);
  if (!size.ok()) return size.status();
  OrderedGroupEncoder encoder;
  encoder.group_ = &group;
  encoder.encoded_bytes_ = *size;
  auto& bytes = encoder.header_;
  std::copy(kGroupMagic.begin(), kGroupMagic.end(), bytes.begin());
  Store(bytes, 8, 1, 4);
  Store(bytes, 12, static_cast<unsigned>(group.kind_), 1);
  Store(bytes, 13, group.retired_, 1);
  Store(bytes, 16, group.incarnation_, 8);
  Store(bytes, 24, group.id_, 8);
  Store(bytes, 32, group.previous_, 8);
  Store(bytes, 40, group.next_, 8);
  Store(bytes, 48, OrderedGroupSize(group), 4);
  Store(bytes, 52, *size, 4);
  return encoder;
}

std::optional<std::string_view> OrderedGroupEncoder::Next() noexcept {
  if (group_ == nullptr) return std::nullopt;
  if (phase_ == 0) {
    phase_ = 1;
    return std::string_view(header_.data(), header_.size());
  }
  if (entry_ == group_->entries_.size()) return std::nullopt;
  const auto& entry = group_->entries_[entry_];
  if (group_->kind_ == OrderedCollectionKind::kString) {
    ++entry_;
    return entry.value_;
  }
  if (phase_ == 1) {
    phase_ = 2;
    Store(entry_header_, 0, entry.value_.size(), 4);
    Store(entry_header_, 4, std::bit_cast<std::uint64_t>(entry.score_), 8);
    return std::string_view(entry_header_.data(), entry_header_.size());
  }
  phase_ = 1;
  ++entry_;
  return entry.value_;
}

absl::StatusOr<std::string> EncodeOrderedGroup(
    const OrderedGroupSnapshot& group) {
  auto encoder = OrderedGroupEncoder::Create(group);
  if (!encoder.ok()) return encoder.status();
  std::string result;
  result.reserve(encoder->encoded_bytes());
  while (auto part = encoder->Next()) result.append(*part);
  return result;
}

absl::StatusOr<OrderedGroupSnapshot> DecodeOrderedGroup(
    std::string_view bytes) {
  if (bytes.size() < kOrderedGroupHeaderBytes ||
      bytes.size() > kMaxRecordPayloadBytes ||
      !bytes.starts_with(kGroupMagic) || Load(bytes, 8, 4) != 1 ||
      Load(bytes, 13, 1) > 1 || Load(bytes, 14, 2) != 0 ||
      Load(bytes, 52, 4) != bytes.size() || Load(bytes, 56, 8) != 0) {
    return absl::DataLossError("invalid ordered page encoding");
  }
  OrderedGroupSnapshot group{
      .kind_ = static_cast<OrderedCollectionKind>(Load(bytes, 12, 1)),
      .incarnation_ = Load(bytes, 16, 8),
      .id_ = Load(bytes, 24, 8),
      .previous_ = Load(bytes, 32, 8),
      .next_ = Load(bytes, 40, 8),
      .retired_ = Load(bytes, 13, 1) != 0,
      .entries_ = {}};
  const auto count = Load(bytes, 48, 4);
  if (group.kind_ == OrderedCollectionKind::kString) {
    if (count != bytes.size() - kOrderedGroupHeaderBytes)
      return absl::DataLossError("String segment length mismatch");
    if (!group.retired_)
      group.entries_.push_back(
          {.value_ = std::string(bytes.substr(kOrderedGroupHeaderBytes))});
    auto valid = ValidateGroup(group);
    if (!valid.ok()) return absl::DataLossError(valid.status().message());
    return group;
  }
  if (count > (bytes.size() - kOrderedGroupHeaderBytes) / kEntryHeaderBytes)
    return absl::DataLossError("ordered page count exceeds its payload");
  group.entries_.reserve(count);
  std::size_t offset = kOrderedGroupHeaderBytes;
  for (std::size_t i = 0; i < count; ++i) {
    if (bytes.size() - offset < kEntryHeaderBytes)
      return absl::DataLossError("truncated ordered entry header");
    const auto length = Load(bytes, offset, 4);
    const auto score = std::bit_cast<double>(Load(bytes, offset + 4, 8));
    offset += kEntryHeaderBytes;
    if (length > kMaxStringBytes || length > bytes.size() - offset)
      return absl::DataLossError("truncated ordered entry value");
    group.entries_.push_back(
        {.value_ = std::string(bytes.substr(offset, length)), .score_ = score});
    offset += length;
  }
  if (offset != bytes.size())
    return absl::DataLossError("ordered page has trailing bytes");
  auto valid = ValidateGroup(group);
  if (!valid.ok()) return absl::DataLossError(valid.status().message());
  return group;
}

absl::StatusOr<OrderedGroupMetadata> DecodeOrderedGroupMetadata(
    std::string_view prefix, std::size_t encoded_bytes) {
  if (prefix.size() < kOrderedGroupHeaderBytes ||
      encoded_bytes < kOrderedGroupHeaderBytes ||
      encoded_bytes > kMaxRecordPayloadBytes ||
      !prefix.starts_with(kGroupMagic) || Load(prefix, 8, 4) != 1 ||
      Load(prefix, 13, 1) > 1 || Load(prefix, 14, 2) != 0 ||
      Load(prefix, 52, 4) != encoded_bytes || Load(prefix, 56, 8) != 0) {
    return absl::DataLossError("invalid ordered page envelope");
  }
  OrderedGroupMetadata result{
      .kind_ = static_cast<OrderedCollectionKind>(Load(prefix, 12, 1)),
      .incarnation_ = Load(prefix, 16, 8),
      .id_ = Load(prefix, 24, 8),
      .previous_ = Load(prefix, 32, 8),
      .next_ = Load(prefix, 40, 8),
      .item_count_ = static_cast<std::uint32_t>(Load(prefix, 48, 4)),
      .retired_ = Load(prefix, 13, 1) != 0};
  if (!ValidKind(result.kind_) || result.incarnation_ == 0 || result.id_ == 0 ||
      result.previous_ == result.id_ || result.next_ == result.id_ ||
      (result.previous_ != 0 && result.previous_ == result.next_) ||
      (result.retired_
           ? (result.item_count_ != 0 || result.previous_ != 0 ||
              result.next_ != 0 || encoded_bytes != kOrderedGroupHeaderBytes)
           : result.item_count_ == 0) ||
      (result.kind_ != OrderedCollectionKind::kString &&
       result.item_count_ >
           (encoded_bytes - kOrderedGroupHeaderBytes) / kEntryHeaderBytes)) {
    return absl::DataLossError("invalid ordered page envelope identity/count");
  }
  if (result.kind_ == OrderedCollectionKind::kString &&
      (result.retired_ ||
       result.item_count_ != encoded_bytes - kOrderedGroupHeaderBytes ||
       result.item_count_ > kStringGroupBytes ||
       (!result.retired_ &&
        (result.previous_ != result.id_ - 1 ||
         (result.next_ != 0 && (result.next_ != result.id_ + 1 ||
                                result.item_count_ != kStringGroupBytes))))))
    return absl::DataLossError("invalid String segment envelope");
  return result;
}

absl::Status OrderedGroupMetadataDecoder::Read(std::string_view bytes) {
  auto fail = [&](std::string_view message) {
    failed_ = true;
    return absl::DataLossError(message);
  };
  if (failed_ || consumed_ > encoded_bytes_ ||
      bytes.size() > encoded_bytes_ - consumed_)
    return fail("ordered metadata stream length mismatch");
  consumed_ += bytes.size();
  while (!bytes.empty()) {
    if (member_remaining_ != 0) {
      const auto skipped = std::min(member_remaining_, bytes.size());
      member_remaining_ -= skipped;
      bytes.remove_prefix(skipped);
      continue;
    }
    if (envelope_ready_ && entries_ == metadata_.item_count_)
      return fail("ordered metadata stream has trailing bytes");
    const auto header_bytes =
        envelope_ready_ ? kEntryHeaderBytes : kOrderedGroupHeaderBytes;
    const auto copied = std::min(header_bytes - header_used_, bytes.size());
    std::copy_n(bytes.data(), copied, header_.data() + header_used_);
    header_used_ += copied;
    bytes.remove_prefix(copied);
    if (header_used_ != header_bytes) continue;
    header_used_ = 0;
    const std::string_view header(header_.data(), header_bytes);
    if (!envelope_ready_) {
      auto metadata = DecodeOrderedGroupMetadata(header, encoded_bytes_);
      if (!metadata.ok()) {
        failed_ = true;
        return metadata.status();
      }
      metadata_ = *metadata;
      envelope_ready_ = true;
      if (metadata_.kind_ == OrderedCollectionKind::kString) {
        entries_ = metadata_.item_count_;
        member_remaining_ = metadata_.item_count_;
      }
      continue;
    }
    const auto length = Load(header, 0, 4);
    const auto score_bits = Load(header, 4, 8);
    const auto score = std::bit_cast<double>(score_bits);
    if (length > kMaxStringBytes || std::isnan(score) ||
        (metadata_.kind_ != OrderedCollectionKind::kSortedSet &&
         score_bits != 0) ||
        (entries_ != 0 && score < metadata_.max_score_))
      return fail("invalid ordered metadata entry length/score");
    if (entries_ == 0) metadata_.min_score_ = score;
    metadata_.max_score_ = score;
    ++entries_;
    member_remaining_ = length;
  }
  return absl::OkStatus();
}

absl::StatusOr<OrderedGroupMetadata> OrderedGroupMetadataDecoder::Finish()
    const {
  if (failed_ || !envelope_ready_ || consumed_ != encoded_bytes_ ||
      header_used_ != 0 || member_remaining_ != 0 ||
      entries_ != metadata_.item_count_)
    return absl::DataLossError("unfinished ordered metadata stream");
  return metadata_;
}

absl::Status ValidateOrderedEntryBoundary(OrderedCollectionKind kind,
                                          const OrderedCollectionEntry& left,
                                          const OrderedCollectionEntry& right) {
  if (kind == OrderedCollectionKind::kStream) {
    auto last = StreamRecordKey(left.value_);
    auto first = StreamRecordKey(right.value_);
    if (!last.ok()) return last.status();
    if (!first.ok()) return first.status();
    // Payload bytes must not make two records with the same routing key
    // appear ordered across a page boundary.
    if (*last >= *first)
      return absl::DataLossError("Stream pages overlap or are unordered");
  } else if (kind == OrderedCollectionKind::kSortedSet &&
             !OrderedEntryLess(left, right)) {
    return absl::DataLossError("Sorted Set pages overlap or are unordered");
  }
  return absl::OkStatus();
}

absl::Status ValidateOrderedGroupBoundary(const OrderedGroupSnapshot& left,
                                          const OrderedGroupSnapshot& right) {
  if (left.kind_ != right.kind_ || left.incarnation_ != right.incarnation_ ||
      left.retired_ || right.retired_ || left.next_ != right.id_ ||
      right.previous_ != left.id_ || left.entries_.empty() ||
      right.entries_.empty()) {
    return absl::DataLossError("inconsistent ordered page neighbours");
  }
  return ValidateOrderedEntryBoundary(left.kind_, left.entries_.back(),
                                      right.entries_.front());
}

absl::StatusOr<OrderedGroupDirectory> OrderedGroupDirectory::Recover(
    const OrderedCollectionRoot& root, std::uint64_t root_sequence,
    std::span<const RecoveredOrderedGroup> candidates,
    const absl::flat_hash_set<std::uint64_t>& committed_txids,
    std::uint64_t command_sequence, std::optional<HashGroupDirectory> members) {
  if (root.member_index_.has_value() != members.has_value() ||
      (members && members->root() != *root.member_index_))
    return absl::DataLossError("Sorted Set member directory/root mismatch");
  if (!ValidRoot(root) || root_sequence == 0 ||
      (root.revision_ != 0 && root.revision_ != root_sequence))
    return absl::DataLossError("invalid ordered root recovery identity");
  // Recovery needs identity lookup, not tree order. Borrow candidate metadata
  // while selecting winners instead of allocating/copying one full record per
  // map node. The input span stays alive until selected records are copied into
  // the owned directory; no borrowed pointer escapes this call.
  absl::flat_hash_map<std::uint64_t, const RecoveredOrderedGroup*> winners;
  winners.reserve(std::min<std::size_t>(candidates.size(), root.group_count_));
  for (const auto& candidate : candidates) {
    if (candidate.incarnation_ != root.incarnation_ ||
        candidate.sequence_ > root_sequence ||
        (candidate.txid_ != 0 && !committed_txids.contains(candidate.txid_)) ||
        (candidate.batch_txid_ != 0 &&
         !committed_txids.contains(candidate.batch_txid_)))
      continue;
    if (candidate.id_ == 0 || candidate.id_ >= root.next_group_id_ ||
        candidate.sequence_ == 0 || candidate.lsn_ == 0 ||
        candidate.record_token_ == 0 ||
        (candidate.retired_ ? (candidate.item_count_ != 0 ||
                               candidate.previous_ != 0 || candidate.next_ != 0)
                            : candidate.item_count_ == 0) ||
        std::isnan(candidate.min_score_) || std::isnan(candidate.max_score_) ||
        candidate.min_score_ > candidate.max_score_) {
      return absl::DataLossError("invalid recovered ordered page");
    }
    auto [it, inserted] = winners.try_emplace(candidate.id_, &candidate);
    if (inserted) continue;
    auto& winner = it->second;
    if (candidate.sequence_ == winner->sequence_ &&
        !SameMetadata(candidate, *winner)) {
      return absl::DataLossError("conflicting ordered page relocation");
    }
    if (candidate.sequence_ > winner->sequence_ ||
        (candidate.sequence_ == winner->sequence_ &&
         candidate.lsn_ > winner->lsn_))
      winner = &candidate;
  }
  OrderedGroupDirectory result;
  std::vector<RecoveredOrderedGroup> groups, retired;
  std::vector<std::pair<std::uint64_t, std::size_t>> ids;
  std::vector<std::uint64_t> ends;
  result.members_ = std::move(members);
  for (auto it = winners.begin(); it != winners.end();) {
    if (it->second->retired_) {
      retired.push_back(*it->second);
      winners.erase(it++);
    } else
      ++it;
  }
  if (winners.size() != root.group_count_)
    return absl::DataLossError("ordered root/page count mismatch");
  result.root_ = root;
  result.root_.revision_ = root_sequence;
  result.sequence_ = root_sequence;
  result.command_sequence_ =
      command_sequence == 0 ? root_sequence : command_sequence;
  groups.reserve(winners.size());
  if (root.kind_ != OrderedCollectionKind::kString) {
    ends.reserve(winners.size());
    ids.reserve(winners.size());
  }
  std::uint64_t id = root.first_group_;
  std::uint64_t previous = 0;
  std::uint64_t count = 0;
  while (id != 0) {
    auto found = winners.find(id);
    if (found == winners.end() || found->second->previous_ != previous ||
        found->second->item_count_ > root.item_count_ - count) {
      return absl::DataLossError("broken ordered page chain or count");
    }
    const auto& group = *found->second;
    if (root.kind_ == OrderedCollectionKind::kString &&
        (group.id_ != groups.size() + 1 ||
         group.item_count_ != std::min<std::uint64_t>(
                                  kStringGroupBytes, root.item_count_ - count)))
      return absl::DataLossError("String segment position/length mismatch");
    if (root.kind_ == OrderedCollectionKind::kSortedSet && !groups.empty() &&
        groups.back().max_score_ > group.min_score_)
      return absl::DataLossError("unordered recovered Sorted Set score bounds");
    if (root.kind_ != OrderedCollectionKind::kString)
      ids.emplace_back(group.id_, groups.size());
    groups.push_back(group);
    count += group.item_count_;
    if (group.encoded_bytes_ >
        std::numeric_limits<std::uint64_t>::max() - result.total_group_bytes_)
      return absl::DataLossError("ordered group byte total overflows");
    result.total_group_bytes_ += group.encoded_bytes_;
    if (root.kind_ != OrderedCollectionKind::kString) ends.push_back(count);
    previous = id;
    id = group.next_;
    winners.erase(found);  // Also detects a cycle without a second set.
  }
  if (!winners.empty() || previous != root.last_group_ ||
      count != root.item_count_)
    return absl::DataLossError(
        "disconnected ordered pages or aggregate mismatch");
  std::sort(ids.begin(), ids.end());
  std::sort(retired.begin(), retired.end(),
            [](const auto& a, const auto& b) { return a.id_ < b.id_; });
  auto group_array = decltype(result.groups_)::From(groups);
  if (!group_array.ok()) return group_array.status();
  auto retired_array = decltype(result.retired_)::From(retired);
  if (!retired_array.ok()) return retired_array.status();
  auto id_array = decltype(result.ids_)::From(ids);
  if (!id_array.ok()) return id_array.status();
  if (root.kind_ == OrderedCollectionKind::kStream) BuildStreamRanks(ends);
  auto end_array = decltype(result.ends_)::From(ends);
  if (!end_array.ok()) return end_array.status();
  result.groups_ = std::move(*group_array);
  result.retired_ = std::move(*retired_array);
  result.ids_ = std::move(*id_array);
  result.ends_ = std::move(*end_array);
  return result;
}

std::optional<std::size_t> OrderedGroupDirectory::FindIndex(
    std::uint64_t id) const noexcept {
  if (root_.kind_ == OrderedCollectionKind::kString)
    return id != 0 && id <= groups_.size() ? std::optional<std::size_t>(id - 1)
                                           : std::nullopt;
  // Append-heavy Streams and Lists usually keep a contiguous run of page
  // identities even after trimming its front. Check that run before the
  // general id index; arbitrary insertions still use the binary search.
  if (id >= root_.first_group_) {
    const auto offset = id - root_.first_group_;
    if (offset < groups_.size() && groups_[offset].id_ == id) return offset;
  }
  const auto found = std::lower_bound(
      ids_.begin(), ids_.end(), id,
      [](const auto& item, auto target) { return item.first < target; });
  return found != ids_.end() && found->first == id
             ? std::optional<std::size_t>(found->second)
             : std::nullopt;
}

const RecoveredOrderedGroup* OrderedGroupDirectory::Find(
    std::uint64_t id) const noexcept {
  const auto index = FindIndex(id);
  return index ? &groups_[*index] : nullptr;
}

const RecoveredOrderedGroup* OrderedGroupDirectory::FindRecord(
    std::uint64_t id) const noexcept {
  if (const auto* active = Find(id)) return active;
  const auto found = std::lower_bound(
      retired_.begin(), retired_.end(), id,
      [](const auto& item, auto target) { return item.id_ < target; });
  return found != retired_.end() && found->id_ == id ? &*found : nullptr;
}

absl::Status OrderedGroupDirectory::RememberStreamPageMaxKey(
    std::size_t index, std::string_view key) const {
  if (root_.kind_ != OrderedCollectionKind::kStream ||
      index >= groups_.size() || key.empty())
    return absl::InvalidArgumentError("invalid Stream page boundary");
  auto& bound = groups_[index].stream_max_key_;
  if (bound.size_ != 0) {
    const std::string_view old(bound.prefix_.data(), bound.size_);
    if (key.size() < old.size() || key.substr(0, old.size()) != old ||
        (bound.exact_ && key.size() != old.size()))
      return absl::DataLossError("Stream page boundary changed within view");
  }
  bound.Set(key);
  return absl::OkStatus();
}

absl::Status OrderedGroupDirectory::RememberStreamHeader(
    std::string_view header) const {
  if (root_.kind_ != OrderedCollectionKind::kStream || header.size() != 48 ||
      !header.starts_with("LXS1") ||
      Load(header, 44, 4) != *root_.stream_length_)
    return absl::DataLossError("invalid Stream directory header");
  if (has_stream_header_ && stream_header() != header)
    return absl::DataLossError("Stream header changed within view");
  std::copy(header.begin(), header.end(), stream_header_.begin());
  has_stream_header_ = true;
  return absl::OkStatus();
}

absl::StatusOr<OrderedGroupDirectory> OrderedGroupDirectory::Apply(
    const OrderedCollectionRoot& root, std::uint64_t revision,
    std::span<const RecoveredOrderedGroup> changed,
    std::uint64_t command_sequence,
    std::span<const RecoveredHashGroup> member_changes) const {
  if (root.kind_ != root_.kind_ || root.incarnation_ != root_.incarnation_ ||
      revision <= sequence_ || command_sequence < command_sequence_ ||
      root.next_group_id_ < root_.next_group_id_) {
    return absl::FailedPreconditionError("stale ordered directory update");
  }
  if (root.kind_ == OrderedCollectionKind::kString) {
    if (!ValidRoot(root) || !member_changes.empty() ||
        root.item_count_ < root_.item_count_)
      return absl::DataLossError("invalid String directory update");
    OrderedGroupDirectory result;
    result.root_ = root;
    result.root_.revision_ = revision;
    result.sequence_ = revision;
    result.command_sequence_ = command_sequence;
    std::vector<RecoveredOrderedGroup> groups(groups_.begin(), groups_.end());
    groups.resize(root.group_count_);
    for (auto item : changed) {
      if (item.id_ == 0 || item.id_ > root.group_count_ || item.retired_ ||
          item.incarnation_ != root.incarnation_ ||
          item.sequence_ != revision ||
          groups[item.id_ - 1].sequence_ == revision)
        return absl::DataLossError("invalid changed String segment");
      item.txid_ = 0;
      item.batch_txid_ = 0;
      groups[item.id_ - 1] = item;
    }
    for (std::size_t i = 0; i < groups.size(); ++i) {
      const auto& item = groups[i];
      if (item.id_ != i + 1 || item.previous_ != i ||
          item.next_ != (i + 1 == groups.size() ? 0 : i + 2) ||
          item.item_count_ != std::min<std::uint64_t>(
                                  kStringGroupBytes,
                                  root.item_count_ - i * kStringGroupBytes) ||
          item.record_token_ == 0 || item.sequence_ == 0 || item.lsn_ == 0)
        return absl::DataLossError("incomplete String segment update");
      if (item.encoded_bytes_ >
          std::numeric_limits<std::uint64_t>::max() - result.total_group_bytes_)
        return absl::DataLossError("String segment byte total overflows");
      result.total_group_bytes_ += item.encoded_bytes_;
    }
    auto array = decltype(result.groups_)::From(groups);
    if (!array.ok()) return array.status();
    result.groups_ = std::move(*array);
    return result;
  }
  absl::flat_hash_set<std::uint64_t> changed_ids;
  changed_ids.reserve(changed.size());
  for (const auto& item : changed) {
    if (item.incarnation_ != root.incarnation_ || item.sequence_ != revision ||
        !changed_ids.insert(item.id_).second ||
        (FindRecord(item.id_) != nullptr && FindRecord(item.id_)->retired_ &&
         !item.retired_))
      return absl::DataLossError("invalid ordered changed page identity");
  }
  auto members = members_;
  if (root.member_index_.has_value() != members.has_value())
    return absl::FailedPreconditionError("cannot change member index format");
  if (members && members->root() != *root.member_index_) {
    auto updated =
        members->Apply(*root.member_index_, command_sequence, member_changes);
    if (!updated.ok()) return updated.status();
    members = std::move(*updated);
  } else if (!member_changes.empty()) {
    return absl::DataLossError("member writes without a new member revision");
  }
  if (members && members->root() != *root.member_index_)
    return absl::DataLossError("Sorted Set member directory/root mismatch");

  // Most writes replace a few pages without changing their order or links.
  // Share unchanged metadata and detach only changed chunks. Structural edits
  // still require complete-chain validation below.
  bool same_topology = root.group_count_ == groups_.size() &&
                       root.first_group_ == root_.first_group_ &&
                       root.last_group_ == root_.last_group_ &&
                       root.next_group_id_ == root_.next_group_id_;
  if (same_topology) {
    for (const auto& item : changed) {
      const auto* previous = Find(item.id_);
      if (previous == nullptr || item.retired_ ||
          item.previous_ != previous->previous_ ||
          item.next_ != previous->next_) {
        same_topology = false;
        break;
      }
    }
  }
  if (same_topology) {
    if (!ValidRoot(root) || (root.revision_ != 0 && root.revision_ != revision))
      return absl::DataLossError("invalid ordered same-topology root");
    OrderedGroupDirectory result;
    result.root_ = root;
    result.root_.revision_ = revision;
    result.sequence_ = revision;
    result.command_sequence_ = command_sequence;
    result.groups_ = groups_;
    result.retired_ = retired_;
    result.ids_ = ids_;
    result.ends_ = ends_;
    result.members_ = std::move(members);
    if (root.kind_ == OrderedCollectionKind::kStream && has_stream_header_) {
      result.stream_header_ = stream_header_;
      result.has_stream_header_ = !changed_ids.contains(root.first_group_);
    }
    absl::InlinedVector<std::pair<std::size_t, absl::int128>, 4> count_changes;
    count_changes.reserve(changed.size());
    absl::int128 count = root_.item_count_;
    absl::int128 bytes = total_group_bytes_;
    for (auto item : changed) {
      const auto found = std::lower_bound(
          ids_.begin(), ids_.end(), item.id_,
          [](const auto& entry, auto id) { return entry.first < id; });
      if (found == ids_.end() || found->first != item.id_)
        return absl::DataLossError("missing ordered same-topology page");
      if (item.item_count_ == 0 || item.record_token_ == 0 || item.lsn_ == 0 ||
          std::isnan(item.min_score_) || std::isnan(item.max_score_) ||
          item.min_score_ > item.max_score_)
        return absl::DataLossError("invalid ordered same-topology page");
      const auto index = found->second;
      const auto& old = groups_[index];
      const auto delta = absl::int128(item.item_count_) - old.item_count_;
      count += delta;
      bytes += absl::int128(item.encoded_bytes_) - old.encoded_bytes_;
      count_changes.emplace_back(index, delta);
      item.txid_ = 0;
      item.batch_txid_ = 0;
      auto status = result.groups_.Set(index, item);
      if (!status.ok()) return status;
    }
    if (count != root.item_count_ || bytes < 0 || bytes > UINT64_MAX)
      return absl::DataLossError("ordered same-topology aggregate mismatch");
    result.total_group_bytes_ = static_cast<std::uint64_t>(bytes);
    // Unchanged pages were validated in the previous immutable view. Only
    // boundaries beside changed pages can introduce a new score inversion.
    std::sort(count_changes.begin(), count_changes.end());
    for (const auto& [index, delta] : count_changes) {
      if (root.kind_ == OrderedCollectionKind::kSortedSet &&
          ((index != 0 && result.groups_[index - 1].max_score_ >
                              result.groups_[index].min_score_) ||
           (index + 1 != groups_.size() &&
            result.groups_[index].max_score_ >
                result.groups_[index + 1].min_score_)))
        return absl::DataLossError("unordered updated Sorted Set score bounds");
    }
    if (root.kind_ == OrderedCollectionKind::kStream) {
      using RankChange = std::pair<std::size_t, absl::int128>;
      absl::InlinedVector<RankChange, 32> rank_changes;
      const auto changed_counts =
          std::count_if(count_changes.begin(), count_changes.end(),
                        [](const auto& change) { return change.second != 0; });
      if (changed_counts == 0) return result;
      const auto updates = changed_counts * (std::bit_width(ends_.size()) + 1);
      // Reserve the complete temporary update list before allocating it.
      // Changed page identities are unique and bounded by the 32-bit root
      // page count, so this product fits size_t on supported 64-bit targets.
      auto admission = TryReserveMemory(
          AllocatorUsableSizeForRequest(updates * sizeof(RankChange) + 1024));
      if (!admission) {
        RecordMemoryRejection();
        return absl::ResourceExhaustedError("OOM Stream rank update scratch");
      }
      rank_changes.reserve(updates);
      for (const auto& [index, delta] : count_changes) {
        if (delta == 0) continue;
        for (std::size_t i = index + 1; i <= ends_.size(); i += i & (~i + 1))
          rank_changes.emplace_back(i - 1, delta);
      }
      std::sort(rank_changes.begin(), rank_changes.end());
      // Combine changes before setting a partial sum: a valid batch may add
      // and remove counts from the same subtree in either command order.
      for (std::size_t i = 0; i < rank_changes.size();) {
        const auto index = rank_changes[i].first;
        absl::int128 delta = 0;
        do {
          delta += rank_changes[i++].second;
        } while (i < rank_changes.size() && rank_changes[i].first == index);
        if (delta == 0) continue;
        const auto value = absl::int128(ends_[index]) + delta;
        if (value < 0 || value > root.item_count_)
          return absl::DataLossError("invalid Stream updated rank sum");
        auto status =
            result.ends_.Set(index, static_cast<std::uint64_t>(value));
        if (!status.ok()) return status;
      }
      return result;
    }
    // Count-neutral replacements share all rank metadata. A redistribution
    // updates only the affected interval, stopping once the net delta is zero.
    absl::int128 delta = 0;
    std::size_t cursor = 0;
    for (std::size_t n = 0; n < count_changes.size(); ++n) {
      const auto [index, change] = count_changes[n];
      if (delta != 0) {
        for (; cursor < index; ++cursor) {
          const auto value = absl::int128(ends_[cursor]) + delta;
          if (value < 0 || value > root.item_count_)
            return absl::DataLossError("invalid ordered updated rank");
          auto status =
              result.ends_.Set(cursor, static_cast<std::uint64_t>(value));
          if (!status.ok()) return status;
        }
      }
      delta += change;
      cursor = index;
    }
    if (delta != 0) {
      for (; cursor < ends_.size(); ++cursor) {
        const auto value = absl::int128(ends_[cursor]) + delta;
        if (value < 0 || value > root.item_count_)
          return absl::DataLossError("invalid ordered updated rank");
        auto status =
            result.ends_.Set(cursor, static_cast<std::uint64_t>(value));
        if (!status.ok()) return status;
      }
    }
    return result;
  }
  // Foreground updates contain one decided replacement per changed identity.
  // The predecessor has already selected and validated all other winners.
  // Do not rebuild recovery's candidate hash map (or copy every record into
  // it) just to select those same winners again. Walk the resulting chain
  // against the small replacement map, checking its complete connectivity.
  if (!ValidRoot(root) || (root.revision_ != 0 && root.revision_ != revision))
    return absl::DataLossError("invalid ordered structural root");
  absl::flat_hash_map<std::uint64_t, const RecoveredOrderedGroup*> replacements;
  replacements.reserve(changed.size());
  std::size_t live_count = groups_.size();
  std::vector<RecoveredOrderedGroup> new_retired;
  for (const auto& item : changed) {
    if (item.id_ == 0 || item.id_ >= root.next_group_id_ || item.lsn_ == 0 ||
        item.record_token_ == 0 ||
        (item.retired_
             ? (item.item_count_ != 0 || item.previous_ != 0 || item.next_ != 0)
             : item.item_count_ == 0) ||
        std::isnan(item.min_score_) || std::isnan(item.max_score_) ||
        item.min_score_ > item.max_score_)
      return absl::DataLossError("invalid ordered structural page");
    replacements.emplace(item.id_, &item);
    const auto* old = FindRecord(item.id_);
    if (old != nullptr && !old->retired_ && item.retired_) --live_count;
    if (old == nullptr && !item.retired_) ++live_count;
    if (item.retired_) new_retired.push_back(item);
  }
  if (live_count != root.group_count_)
    return absl::DataLossError("ordered structural page count mismatch");
  OrderedGroupDirectory rebuilt;
  rebuilt.root_ = root;
  rebuilt.root_.revision_ = revision;
  rebuilt.sequence_ = revision;
  rebuilt.command_sequence_ = command_sequence;
  rebuilt.members_ = std::move(members);
  auto ordinal_admission = TryReserveMemory(AllocatorUsableSizeForRequest(
      groups_.size() * sizeof(std::size_t) + changed.size() * 32 + 1024));
  if (!ordinal_admission) {
    RecordMemoryRejection();
    return absl::ResourceExhaustedError("OOM ordered structural ordinals");
  }
  const auto absent = std::numeric_limits<std::size_t>::max();
  std::vector<std::size_t> positions(groups_.size(), absent);
  std::vector<std::pair<std::uint64_t, std::size_t>> inserted_ids;
  std::vector<RecoveredOrderedGroup> groups;
  std::vector<std::pair<std::uint64_t, std::size_t>> ids;
  std::vector<std::uint64_t> ends;
  groups.reserve(root.group_count_);
  ids.reserve(root.group_count_);
  ends.reserve(root.group_count_);
  std::uint64_t id = root.first_group_, previous = 0, count = 0;
  std::size_t old_cursor = 0, visited_changes = 0;
  while (id != 0) {
    const RecoveredOrderedGroup* item = nullptr;
    const auto replacement = replacements.find(id);
    if (replacement != replacements.end()) {
      item = replacement->second;
      ++visited_changes;
      const auto old_index = FindIndex(id);
      if (old_index)
        positions[*old_index] = groups.size();
      else
        inserted_ids.emplace_back(id, groups.size());
    } else {
      // Whole unchanged runs keep their old ordinal. A split/trim may change
      // the next run's start, so seek once there, then resume sequential
      // access.
      if (old_cursor >= groups_.size() || groups_[old_cursor].id_ != id) {
        const auto position = FindIndex(id);
        if (!position)
          return absl::DataLossError("missing ordered structural page");
        old_cursor = *position;
      }
      positions[old_cursor] = groups.size();
      item = &groups_[old_cursor++];
    }
    if (groups.size() == root.group_count_ || item->retired_ ||
        item->previous_ != previous ||
        item->item_count_ > root.item_count_ - count)
      return absl::DataLossError("broken ordered structural chain or count");
    if (root.kind_ == OrderedCollectionKind::kSortedSet && !groups.empty() &&
        groups.back().max_score_ > item->min_score_)
      return absl::DataLossError("unordered updated Sorted Set score bounds");
    if (item->encoded_bytes_ > UINT64_MAX - rebuilt.total_group_bytes_)
      return absl::DataLossError("ordered group byte total overflows");
    rebuilt.total_group_bytes_ += item->encoded_bytes_;
    groups.push_back(*item);
    groups.back().txid_ = 0;
    groups.back().batch_txid_ = 0;
    count += item->item_count_;
    ends.push_back(count);
    previous = id;
    id = item->next_;
  }
  if (groups.size() != root.group_count_ || previous != root.last_group_ ||
      count != root.item_count_ ||
      visited_changes != changed.size() - new_retired.size())
    return absl::DataLossError("disconnected ordered structural pages");
  // Retired records remain available to relocation/reclamation. Merge their
  // already sorted predecessor index with just this command's tombstones;
  // never re-sort every historical retirement on each append/trim.
  std::sort(new_retired.begin(), new_retired.end(),
            [](const auto& a, const auto& b) { return a.id_ < b.id_; });
  std::vector<RecoveredOrderedGroup> retired;
  retired.reserve(retired_.size() + new_retired.size());
  auto old = retired_.begin();
  for (auto item : new_retired) {
    for (; old != retired_.end() && old->id_ < item.id_; ++old)
      retired.push_back(*old);
    if (old != retired_.end() && old->id_ == item.id_) ++old;
    item.txid_ = 0;
    item.batch_txid_ = 0;
    retired.push_back(item);
  }
  retired.insert(retired.end(), old, retired_.end());
  for (auto& item : retired) {
    item.txid_ = 0;
    item.batch_txid_ = 0;
  }
  // The predecessor identity index is already sorted. Its ordinal changes,
  // not its identities. Reindex surviving old pages and merge only the new
  // identities rather than sorting the entire directory after a middle split.
  std::sort(inserted_ids.begin(), inserted_ids.end());
  auto inserted = inserted_ids.begin();
  for (const auto& [old_id, old_position] : ids_) {
    if (positions[old_position] == absent) continue;
    for (; inserted != inserted_ids.end() && inserted->first < old_id;
         ++inserted)
      ids.push_back(*inserted);
    ids.emplace_back(old_id, positions[old_position]);
  }
  ids.insert(ids.end(), inserted, inserted_ids.end());
  auto group_array = decltype(groups_)::From(groups);
  if (!group_array.ok()) return group_array.status();
  auto retired_array = decltype(retired_)::From(retired);
  if (!retired_array.ok()) return retired_array.status();
  auto id_array = decltype(ids_)::From(ids);
  if (!id_array.ok()) return id_array.status();
  if (root.kind_ == OrderedCollectionKind::kStream) BuildStreamRanks(ends);
  auto end_array = decltype(ends_)::From(ends);
  if (!end_array.ok()) return end_array.status();
  rebuilt.groups_ = std::move(*group_array);
  rebuilt.retired_ = std::move(*retired_array);
  rebuilt.ids_ = std::move(*id_array);
  rebuilt.ends_ = std::move(*end_array);
  if (root.kind_ == OrderedCollectionKind::kStream && has_stream_header_) {
    const auto* old_first = Find(root_.first_group_);
    const auto* new_first = rebuilt.Find(root.first_group_);
    if (old_first != nullptr && new_first != nullptr &&
        old_first->id_ == new_first->id_ &&
        old_first->sequence_ == new_first->sequence_) {
      rebuilt.stream_header_ = stream_header_;
      rebuilt.has_stream_header_ = true;
    }
  }
  return rebuilt;
}

std::uint64_t OrderedGroupDirectory::CountBefore(
    std::size_t index) const noexcept {
  if (index == 0) return 0;
  if (index >= groups_.size()) return root_.item_count_;
  if (root_.kind_ == OrderedCollectionKind::kString)
    return index * kStringGroupBytes;
  if (root_.kind_ == OrderedCollectionKind::kStream) {
    std::uint64_t count = 0;
    for (; index != 0; index -= index & (~index + 1)) count += ends_[index - 1];
    return count;
  }
  return ends_[index - 1];
}

std::optional<OrderedGroupDirectory::Position> OrderedGroupDirectory::FindRank(
    std::uint64_t rank) const noexcept {
  if (rank >= root_.item_count_) return std::nullopt;
  if (root_.kind_ == OrderedCollectionKind::kString)
    return Position{rank / kStringGroupBytes, rank % kStringGroupBytes};
  if (root_.kind_ == OrderedCollectionKind::kStream) {
    std::size_t index = 0;
    std::uint64_t count = 0;
    for (auto stride = std::bit_floor(ends_.size()); stride != 0;
         stride >>= 1) {
      const auto next = index + stride;
      if (next <= ends_.size() && ends_[next - 1] <= rank - count) {
        count += ends_[next - 1];
        index = next;
      }
    }
    return Position{index, rank - count};
  }
  const auto found = std::upper_bound(ends_.begin(), ends_.end(), rank);
  const std::size_t index = found - ends_.begin();
  return Position{index, rank - (index == 0 ? 0 : ends_[index - 1])};
}

std::size_t OrderedGroupDirectory::LowerBoundScore(
    double score, bool exclusive) const noexcept {
  const auto found = std::lower_bound(groups_.begin(), groups_.end(), score,
                                      [exclusive](const auto& page, double at) {
                                        return exclusive ? page.max_score_ <= at
                                                         : page.max_score_ < at;
                                      });
  return found - groups_.begin();
}

std::size_t OrderedGroupDirectory::UpperBoundScore(
    double score, bool exclusive) const noexcept {
  const auto found = std::lower_bound(
      groups_.begin(), groups_.end(), score,
      [exclusive](const auto& page, double at) {
        return exclusive ? page.min_score_ < at : page.min_score_ <= at;
      });
  return found - groups_.begin();
}

absl::StatusOr<bool> RebalanceSortedSetGroupPair(OrderedGroupSnapshot& left,
                                                 OrderedGroupSnapshot& right,
                                                 std::size_t target_bytes) {
  if (left.kind_ != OrderedCollectionKind::kSortedSet ||
      right.kind_ != left.kind_ || left.incarnation_ == 0 ||
      left.incarnation_ != right.incarnation_ || left.id_ == 0 ||
      right.id_ == 0 || left.id_ == right.id_ || left.retired_ ||
      right.retired_ || left.next_ != right.id_ ||
      right.previous_ != left.id_ || left.previous_ == left.id_ ||
      left.previous_ == right.id_ || right.next_ == right.id_ ||
      right.next_ == left.id_ || target_bytes <= kOrderedGroupHeaderBytes ||
      target_bytes > kMaxRecordPayloadBytes) {
    return absl::InvalidArgumentError("invalid Sorted Set rebalance pair");
  }
  const auto capacity = target_bytes - kOrderedGroupHeaderBytes;
  auto payload_bytes = [](const auto& entries) -> absl::StatusOr<std::size_t> {
    std::size_t bytes = 0;
    for (const auto& entry : entries) {
      if (entry.value_.size() > kMaxStringBytes ||
          bytes > std::numeric_limits<std::size_t>::max() - kEntryHeaderBytes -
                      entry.value_.size())
        return absl::OutOfRangeError("Sorted Set rebalance size overflow");
      bytes += kEntryHeaderBytes + entry.value_.size();
    }
    return bytes;
  };
  const auto left_bytes = payload_bytes(left.entries_);
  const auto right_bytes = payload_bytes(right.entries_);
  if (!left_bytes.ok()) return left_bytes.status();
  if (!right_bytes.ok()) return right_bytes.status();
  if (!left.entries_.empty() && !right.entries_.empty() &&
      *left_bytes <= capacity && *right_bytes <= capacity)
    return false;
  // Avoid adding unbounded page sizes: a feasible pair has at most two
  // inline payloads. A large indivisible member must stay on the extent path.
  if (*left_bytes > 2 * capacity || *right_bytes > 2 * capacity - *left_bytes)
    return false;
  const auto total_bytes = *left_bytes + *right_bytes;
  const auto count = left.entries_.size() + right.entries_.size();
  std::size_t prefix = 0, cut = 0;
  auto best_distance = std::numeric_limits<std::size_t>::max();
  for (std::size_t i = 0; i + 1 < count; ++i) {
    const auto& entry = i < left.entries_.size()
                            ? left.entries_[i]
                            : right.entries_[i - left.entries_.size()];
    prefix += kEntryHeaderBytes + entry.value_.size();
    if (prefix > capacity) break;
    if (total_bytes - prefix > capacity) continue;
    const auto distance = i + 1 > left.entries_.size()
                              ? i + 1 - left.entries_.size()
                              : left.entries_.size() - (i + 1);
    if (distance < best_distance) {
      best_distance = distance;
      cut = i + 1;
    }
  }
  if (cut == 0) return false;
  auto valid = ValidateEntries(left.kind_, left.entries_);
  if (!valid.ok()) return valid;
  valid = ValidateEntries(right.kind_, right.entries_);
  if (!valid.ok()) return valid;
  if (!left.entries_.empty() && !right.entries_.empty() &&
      !OrderedEntryLess(left.entries_.back(), right.entries_.front()))
    return absl::InvalidArgumentError("unordered Sorted Set rebalance pair");
  // Reserve before moving anything. Existing active ids survive a score move
  // when its two afterimages still fit, keeping directory publication local
  // instead of rebuilding every group after an unnecessary split.
  if (cut > left.entries_.size()) {
    const auto moved = cut - left.entries_.size();
    left.entries_.reserve(cut);
    for (std::size_t i = 0; i < moved; ++i)
      left.entries_.push_back(std::move(right.entries_[i]));
    right.entries_.erase(right.entries_.begin(),
                         right.entries_.begin() + moved);
  } else {
    std::vector<OrderedCollectionEntry> replacement;
    replacement.reserve(count - cut);
    for (std::size_t i = cut; i < left.entries_.size(); ++i)
      replacement.push_back(std::move(left.entries_[i]));
    for (auto& entry : right.entries_) replacement.push_back(std::move(entry));
    left.entries_.resize(cut);
    right.entries_ = std::move(replacement);
  }
  return true;
}

absl::StatusOr<OrderedGroupSplit> SplitOrderedGroup(OrderedGroupSnapshot group,
                                                    std::uint64_t next_group_id,
                                                    std::size_t target_bytes) {
  if (target_bytes <= kOrderedGroupHeaderBytes ||
      target_bytes > kMaxRecordPayloadBytes ||
      next_group_id <= std::max({group.id_, group.previous_, group.next_}) ||
      group.retired_ || group.entries_.empty()) {
    return absl::InvalidArgumentError("invalid ordered split input");
  }
  // A promotion can exceed one record's aggregate limit. Validate individual
  // values and ordering before partitioning, not as one unsplittable record.
  if (group.incarnation_ == 0 || group.id_ == 0 ||
      group.previous_ == group.id_ || group.next_ == group.id_ ||
      (group.previous_ != 0 && group.previous_ == group.next_)) {
    return absl::InvalidArgumentError("invalid ordered split identity");
  }
  auto valid = ValidateEntries(group.kind_, group.entries_);
  if (!valid.ok()) return valid;
  OrderedGroupSplit result{.next_group_id_ = next_group_id, .groups_ = {}};
  std::size_t begin = 0;
  while (begin != group.entries_.size()) {
    std::size_t end = begin;
    std::size_t bytes = kOrderedGroupHeaderBytes;
    while (end != group.entries_.size()) {
      const auto item_bytes =
          kEntryHeaderBytes + group.entries_[end].value_.size();
      if (end != begin &&
          (bytes > target_bytes || item_bytes > target_bytes - bytes))
        break;
      bytes += item_bytes;
      ++end;
    }
    std::uint64_t id = group.id_;
    if (!result.groups_.empty()) {
      if (result.next_group_id_ == std::numeric_limits<std::uint64_t>::max())
        return absl::ResourceExhaustedError(
            "ordered page identity space exhausted");
      id = result.next_group_id_++;
    }
    OrderedGroupSnapshot page{.kind_ = group.kind_,
                              .incarnation_ = group.incarnation_,
                              .id_ = id,
                              .previous_ = result.groups_.empty()
                                               ? group.previous_
                                               : result.groups_.back().id_,
                              .next_ = group.next_,
                              .entries_ = {}};
    page.entries_.reserve(end - begin);
    for (std::size_t i = begin; i < end; ++i)
      page.entries_.push_back(std::move(group.entries_[i]));
    if (!result.groups_.empty()) result.groups_.back().next_ = id;
    result.groups_.push_back(std::move(page));
    begin = end;
  }
  return result;
}

absl::StatusOr<OrderedCollectionMutationPlan> PlanOrderedCollectionSplice(
    const OrderedGroupDirectory& directory,
    std::vector<LoadedOrderedGroup> loaded_groups, std::uint64_t rank,
    std::uint64_t erase_count, std::vector<OrderedCollectionEntry> entries,
    std::size_t target_bytes) {
  const auto& root = directory.root();
  if (rank > root.item_count_ || erase_count > root.item_count_ - rank ||
      entries.size() > std::numeric_limits<std::uint32_t>::max() -
                           (root.item_count_ - erase_count)) {
    return absl::OutOfRangeError("ordered splice range/count is invalid");
  }
  OrderedCollectionMutationPlan plan{
      .root_ = root, .expected_sequence_ = directory.sequence(), .writes_ = {}};
  if (erase_count == 0 && entries.empty()) return plan;
  // The storage adapter allocates the next revision after this pure plan is
  // built. Do not accidentally retain the source's bound on new page writes.
  plan.root_.revision_ = 0;
  auto valid = ValidateEntries(root.kind_, entries);
  if (!valid.ok()) return valid;

  const auto& groups = directory.groups();
  // End insertion uses the tail; an insertion at a page boundary belongs to
  // the right page. Erasing across a boundary includes every touched page.
  const auto first = directory.FindRank(std::min(rank, root.item_count_ - 1));
  const auto last =
      erase_count == 0 ? first : directory.FindRank(rank + erase_count - 1);
  const std::size_t begin = first->group_index_;
  const std::size_t end = last->group_index_ + 1;
  const std::size_t required_begin = begin == 0 ? begin : begin - 1;
  const std::size_t required_end = end == groups.size() ? end : end + 1;
  std::map<std::uint64_t, OrderedGroupSnapshot> loaded;
  for (auto& candidate : loaded_groups) {
    const auto& page = candidate.snapshot_;
    // Validate only the loaded pages against the directory's identity index;
    // a small splice must not scan every page of a large collection.
    const auto* found = directory.Find(page.id_);
    if (found == nullptr || candidate.sequence_ != found->sequence_ ||
        page.incarnation_ != root.incarnation_ || page.kind_ != root.kind_ ||
        page.retired_ || page.previous_ != found->previous_ ||
        page.next_ != found->next_ ||
        page.entries_.size() != found->item_count_) {
      return absl::AbortedError("ordered splice loaded a stale page");
    }
    auto page_valid = ValidateGroup(page);
    if (!page_valid.ok()) return page_valid.status();
    if (!loaded.emplace(page.id_, std::move(candidate.snapshot_)).second)
      return absl::InvalidArgumentError("ordered splice repeats a loaded page");
  }
  // An equal-sized List replacement preserves the encoded page size, count
  // and both links. Its neighbours cannot participate in a split or removal,
  // and List values impose no cross-page ordering constraint.
  if (root.kind_ == OrderedCollectionKind::kList && erase_count == 1 &&
      entries.size() == 1 && target_bytes == kCollectionGroupTargetBytes) {
    const auto found = loaded.find(groups[begin].id_);
    if (found != loaded.end() &&
        found->second.entries_[first->offset_].value_.size() ==
            entries.front().value_.size()) {
      found->second.entries_[first->offset_] = std::move(entries.front());
      plan.changed_ = true;
      plan.writes_.push_back(std::move(found->second));
      return plan;
    }
  }
  for (std::size_t i = required_begin; i < required_end; ++i) {
    if (!loaded.contains(groups[i].id_))
      return absl::InvalidArgumentError("ordered splice needs adjacent pages");
    if (i != required_begin) {
      auto boundary = ValidateOrderedGroupBoundary(loaded.at(groups[i - 1].id_),
                                                   loaded.at(groups[i].id_));
      if (!boundary.ok()) return boundary;
    }
  }

  OrderedGroupSnapshot replacement{.kind_ = root.kind_,
                                   .incarnation_ = root.incarnation_,
                                   .id_ = groups[begin].id_,
                                   .previous_ = groups[begin].previous_,
                                   .next_ = groups[end - 1].next_,
                                   .entries_ = {}};
  std::uint64_t offset =
      rank == root.item_count_ ? groups[begin].item_count_ : first->offset_;
  std::uint64_t selected_count = 0;
  for (std::size_t i = begin; i < end; ++i)
    selected_count += groups[i].item_count_;
  replacement.entries_.reserve(selected_count - erase_count + entries.size());
  std::uint64_t position = 0;
  bool inserted = false;
  for (std::size_t i = begin; i < end; ++i) {
    for (auto& entry : loaded.at(groups[i].id_).entries_) {
      if (position == offset) {
        for (auto& addition : entries)
          replacement.entries_.push_back(std::move(addition));
        inserted = true;
      }
      if (position < offset || position >= offset + erase_count)
        replacement.entries_.push_back(std::move(entry));
      ++position;
    }
  }
  if (!inserted) {
    for (auto& addition : entries)
      replacement.entries_.push_back(std::move(addition));
  }
  valid = ValidateEntries(root.kind_, replacement.entries_);
  if (!valid.ok()) return valid;
  if (root.kind_ != OrderedCollectionKind::kList &&
      !replacement.entries_.empty()) {
    if (begin != 0 &&
        !ValidateOrderedEntryBoundary(
             root.kind_, loaded.at(groups[begin - 1].id_).entries_.back(),
             replacement.entries_.front())
             .ok())
      return absl::InvalidArgumentError(
          "ordered splice crosses its lower bound");
    if (end != groups.size() &&
        !ValidateOrderedEntryBoundary(
             root.kind_, replacement.entries_.back(),
             loaded.at(groups[end].id_).entries_.front())
             .ok())
      return absl::InvalidArgumentError(
          "ordered splice crosses its upper bound");
  }

  plan.root_.item_count_ = root.item_count_ - erase_count + entries.size();
  plan.changed_ = true;
  if (plan.root_.item_count_ == 0) {
    plan.delete_key_ = true;
    plan.root_.group_count_ = 0;
    plan.root_.first_group_ = plan.root_.last_group_ = 0;
    return plan;
  }
  const auto previous = groups[begin].previous_;
  const auto next = groups[end - 1].next_;
  std::uint64_t new_first = next;
  std::uint64_t new_last = previous;
  std::size_t replacement_count = 0;
  if (!replacement.entries_.empty()) {
    auto split = SplitOrderedGroup(std::move(replacement), root.next_group_id_,
                                   target_bytes);
    if (!split.ok()) return split.status();
    plan.root_.next_group_id_ = split->next_group_id_;
    replacement_count = split->groups_.size();
    new_first = split->groups_.front().id_;
    new_last = split->groups_.back().id_;
    plan.writes_ = std::move(split->groups_);
  }
  const std::size_t first_retired = replacement_count == 0 ? begin : begin + 1;
  for (std::size_t i = first_retired; i < end; ++i)
    plan.writes_.push_back(Retired(root, groups[i].id_));
  if (begin == 0)
    plan.root_.first_group_ = new_first;
  else if (loaded.at(previous).next_ != new_first) {
    auto& neighbour = loaded.at(previous);
    neighbour.next_ = new_first;
    plan.writes_.push_back(std::move(neighbour));
  }
  if (end == groups.size())
    plan.root_.last_group_ = new_last;
  else if (loaded.at(next).previous_ != new_last) {
    auto& neighbour = loaded.at(next);
    neighbour.previous_ = new_last;
    plan.writes_.push_back(std::move(neighbour));
  }
  plan.root_.group_count_ =
      root.group_count_ - (end - begin) + replacement_count;
  return plan;
}

}  // namespace lavik::storage
