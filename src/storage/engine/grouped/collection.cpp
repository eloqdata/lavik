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

#include "lavik/storage/detail/grouped/collection.h"

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

constexpr std::string_view kRootMagic = "LOCROOT1";
constexpr std::string_view kGroupMagic = "LOCGRUP1";
constexpr std::size_t kRootBytes = kOrderedCollectionRootBytes;
constexpr std::size_t kEntryHeaderBytes = kOrderedEntryHeaderBytes;

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
  if ((root.kind_ == OrderedCollectionKind::kSortedSet) !=
      root.member_index_.has_value())
    return false;
  if (root.member_index_ &&
      (root.member_index_->incarnation_ != root.incarnation_ ||
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

bool EntryLess(const auto& left, const auto& right) noexcept {
  return left.score_ < right.score_ ||
         (left.score_ == right.score_ && left.value_ < right.value_);
}

template <typename Entry>
absl::Status ValidateEntrySpan(OrderedCollectionKind kind,
                               std::span<const Entry> entries) {
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
         (i != 0 && !EntryLess(entries[i - 1], entry)))) {
      return absl::InvalidArgumentError(
          "Sorted Set page is unordered or repeats a member");
    }
  }
  return absl::OkStatus();
}

absl::Status ValidateEntries(OrderedCollectionKind kind,
                             std::span<const OrderedCollectionEntry> entries) {
  return ValidateEntrySpan(kind, entries);
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

bool SameMetadata(const OrderedGroupEntry& left,
                  const OrderedGroupEntry& right) {
  return left.previous_ == right.previous_ && left.next_ == right.next_ &&
         left.item_count_ == right.item_count_ &&
         left.encoded_bytes_ == right.encoded_bytes_ &&
         left.retired_ == right.retired_ &&
         std::bit_cast<std::uint64_t>(left.min_score_) ==
             std::bit_cast<std::uint64_t>(right.min_score_) &&
         std::bit_cast<std::uint64_t>(left.max_score_) ==
             std::bit_cast<std::uint64_t>(right.max_score_);
}

// Shared envelope checks apply to recovery winners and foreground replacements;
// chain reachability and type-specific ordering remain the caller's job.
bool ValidPage(const OrderedGroupEntry& page,
               const OrderedCollectionRoot& root) {
  return page.id_ != 0 && page.id_ < root.next_group_id_ &&
         page.sequence_ != 0 && page.lsn_ != 0 && page.record_token_ != 0 &&
         (page.retired_ ? (page.item_count_ == 0 && page.previous_ == 0 &&
                           page.next_ == 0)
                        : page.item_count_ != 0) &&
         !std::isnan(page.min_score_) && !std::isnan(page.max_score_) &&
         page.min_score_ <= page.max_score_;
}

struct PageChanges {
  absl::flat_hash_map<std::uint64_t, const OrderedGroupEntry*> pages_;
  std::uint64_t bytes_ = 0;
  bool same_topology_ = false;
};

// Each input identity must replace its predecessor exactly once. This single
// pass validates the batch and its aggregate deltas before selecting a layout
// update path. Borrowed page pointers stay within the caller's input lifetime.
absl::StatusOr<PageChanges> AnalyzeChanges(
    const OrderedGroupDirectory& directory, const OrderedCollectionRoot& root,
    std::uint64_t revision, std::span<const RecoveredOrderedGroup> changed) {
  if (!ValidRoot(root) || (root.revision_ != 0 && root.revision_ != revision))
    return absl::DataLossError("invalid ordered directory root");
  const auto& before = directory.root();
  const bool list = root.kind_ == OrderedCollectionKind::kList;
  PageChanges result;
  result.pages_.reserve(changed.size());
  result.same_topology_ =
      root.group_count_ == before.group_count_ &&
      root.first_group_ == before.first_group_ &&
      root.last_group_ == before.last_group_ &&
      (list || root.next_group_id_ == before.next_group_id_);
  absl::int128 count = before.item_count_, pages = before.group_count_;
  absl::int128 bytes = directory.total_group_bytes();
  for (const auto& item : changed) {
    const auto* old = directory.FindRecord(item.id_);
    if (item.incarnation_ != root.incarnation_ || item.sequence_ != revision ||
        !ValidPage(item, root) ||
        !result.pages_.emplace(item.id_, &item).second ||
        (old && old->retired_ && !item.retired_))
      return absl::DataLossError("invalid ordered changed page");
    if (old && !old->retired_) {
      count -= old->item_count_;
      bytes -= old->encoded_bytes_;
      --pages;
    }
    if (!item.retired_) {
      count += item.item_count_;
      bytes += item.encoded_bytes_;
      ++pages;
    }
    // List retirement-only replacements do not affect the active ring.
    // Linear directories merge their sorted retirement index structurally.
    if (!(list && item.retired_ && (!old || old->retired_)))
      result.same_topology_ &= old && !old->retired_ && !item.retired_ &&
                               old->previous_ == item.previous_ &&
                               old->next_ == item.next_;
  }
  if (count != root.item_count_ || pages != root.group_count_ || bytes < 0 ||
      bytes > UINT64_MAX)
    return absl::DataLossError("ordered directory aggregate mismatch");
  result.bytes_ = static_cast<std::uint64_t>(bytes);
  return result;
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
  return EntryLess(left, right);
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
  // The kind, extension flag and payload length must agree; Sorted Sets
  // always carry the member graph and Streams carry their logical length.
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

absl::StatusOr<std::vector<std::string>> DecodeOrderedListRange(
    std::string_view bytes, std::size_t first, std::size_t count) {
  auto metadata = DecodeOrderedGroupMetadata(bytes, bytes.size());
  if (!metadata.ok()) return metadata.status();
  if (metadata->kind_ != OrderedCollectionKind::kList || metadata->retired_)
    return absl::DataLossError("List range requires a live List page");
  if (first > metadata->item_count_ || count > metadata->item_count_ - first)
    return absl::OutOfRangeError("List range exceeds page");
  std::vector<std::string> values;
  values.reserve(count);
  std::size_t offset = kOrderedGroupHeaderBytes;
  for (std::size_t i = 0; i < metadata->item_count_; ++i) {
    if (bytes.size() - offset < kEntryHeaderBytes)
      return absl::DataLossError("truncated List entry header");
    const auto length = Load(bytes, offset, 4);
    // Lists require the exact +0 bit pattern, including in skipped entries.
    if (Load(bytes, offset + 4, 8) != 0)
      return absl::DataLossError("invalid List entry score");
    offset += kEntryHeaderBytes;
    if (length > kMaxStringBytes || length > bytes.size() - offset)
      return absl::DataLossError("truncated List entry value");
    if (i >= first && i - first < count)
      values.emplace_back(bytes.substr(offset, length));
    offset += length;
  }
  if (offset != bytes.size())
    return absl::DataLossError("List page has trailing bytes");
  return values;
}

absl::StatusOr<std::vector<OrderedCollectionEntryView>>
DecodeSortedSetGroupViews(std::string_view bytes) {
  auto metadata = DecodeOrderedGroupMetadata(bytes, bytes.size());
  if (!metadata.ok()) return metadata.status();
  if (metadata->kind_ != OrderedCollectionKind::kSortedSet ||
      metadata->retired_)
    return absl::DataLossError(
        "Sorted Set scan requires a live Sorted Set page");
  std::vector<OrderedCollectionEntryView> entries;
  entries.reserve(metadata->item_count_);
  std::size_t offset = kOrderedGroupHeaderBytes;
  for (std::size_t i = 0; i < metadata->item_count_; ++i) {
    if (bytes.size() - offset < kEntryHeaderBytes)
      return absl::DataLossError("truncated ordered entry header");
    const auto length = Load(bytes, offset, 4);
    const auto score = std::bit_cast<double>(Load(bytes, offset + 4, 8));
    offset += kEntryHeaderBytes;
    if (length > kMaxStringBytes || length > bytes.size() - offset)
      return absl::DataLossError("truncated ordered entry value");
    entries.push_back({bytes.substr(offset, length), score});
    offset += length;
  }
  if (offset != bytes.size())
    return absl::DataLossError("ordered page has trailing bytes");
  auto valid = ValidateEntrySpan(
      metadata->kind_, std::span<const OrderedCollectionEntryView>(entries));
  if (!valid.ok()) return absl::DataLossError(valid.message());
  return entries;
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
  absl::flat_hash_map<std::uint64_t, const OrderedGroupEntry*> winners;
  winners.reserve(std::min<std::size_t>(candidates.size(), root.group_count_));
  for (const auto& candidate : candidates) {
    if (candidate.incarnation_ != root.incarnation_ ||
        candidate.sequence_ > root_sequence ||
        (candidate.txid_ != 0 && !committed_txids.contains(candidate.txid_)) ||
        (candidate.batch_txid_ != 0 &&
         !committed_txids.contains(candidate.batch_txid_)))
      continue;
    if (!ValidPage(candidate, root))
      return absl::DataLossError("invalid recovered ordered page");
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
  std::vector<OrderedGroupEntry> groups, retired;
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
  const bool linear_ranks = root.kind_ != OrderedCollectionKind::kString &&
                            root.kind_ != OrderedCollectionKind::kList;
  groups.reserve(winners.size());
  if (linear_ranks) {
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
    if (linear_ranks) ids.emplace_back(group.id_, groups.size());
    groups.push_back(group);
    count += group.item_count_;
    if (group.encoded_bytes_ >
        std::numeric_limits<std::uint64_t>::max() - result.total_group_bytes_)
      return absl::DataLossError("ordered group byte total overflows");
    result.total_group_bytes_ += group.encoded_bytes_;
    if (linear_ranks) ends.push_back(count);
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
  if (root.kind_ == OrderedCollectionKind::kList) {
    result.ids_.emplace<ListSlots>();
    auto retired_array = decltype(result.retired_)::From(retired);
    if (!retired_array.ok()) return retired_array.status();
    result.retired_ = std::move(*retired_array);
    auto& positions = std::get<ListSlots>(result.ids_).positions_;
    for (std::size_t i = 0; i < retired.size(); ++i) {
      auto status = positions.Set(retired[i].id_, {i, true});
      if (!status.ok()) return status;
    }
    auto status = result.BuildListSlots(std::move(groups), 0);
    if (!status.ok()) return status;
    return result;
  }
  auto group_array = decltype(result.groups_)::From(groups);
  if (!group_array.ok()) return group_array.status();
  auto retired_array = decltype(result.retired_)::From(retired);
  if (!retired_array.ok()) return retired_array.status();
  auto id_array = LinearIds::From(ids);
  if (!id_array.ok()) return id_array.status();
  auto end_array = FenwickTree::FromCumulative(std::move(ends));
  if (!end_array.ok()) return end_array.status();
  result.groups_ = std::move(*group_array);
  result.retired_ = std::move(*retired_array);
  result.linear_ids() = std::move(*id_array);
  result.ranks_ = std::move(*end_array);
  return result;
}

std::optional<std::size_t> OrderedGroupDirectory::FindIndex(
    std::uint64_t id) const noexcept {
  if (const auto* list = std::get_if<ListSlots>(&ids_)) {
    const auto* found = list->positions_.Get(id);
    if (!found || found->retired_) return std::nullopt;
    return (found->slot_ + groups_.size() - list->head_) % groups_.size();
  }
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
      linear_ids().begin(), linear_ids().end(), id,
      [](const auto& item, auto target) { return item.first < target; });
  return found != linear_ids().end() && found->first == id
             ? std::optional<std::size_t>(found->second)
             : std::nullopt;
}

const OrderedGroupEntry* OrderedGroupDirectory::Find(
    std::uint64_t id) const noexcept {
  const auto index = FindIndex(id);
  return index ? &groups()[*index] : nullptr;
}

const OrderedGroupEntry* OrderedGroupDirectory::FindRecord(
    std::uint64_t id) const noexcept {
  if (const auto* list = std::get_if<ListSlots>(&ids_)) {
    const auto* found = list->positions_.Get(id);
    if (!found) return nullptr;
    return found->retired_ ? &retired_[found->slot_] : &groups_[found->slot_];
  }
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
  if (has_stream_header() && stream_header() != header)
    return absl::DataLossError("Stream header changed within view");
  std::copy(header.begin(), header.end(), stream_header_.begin());
  return absl::OkStatus();
}

absl::Status OrderedGroupDirectory::BuildListSlots(
    std::vector<OrderedGroupEntry> groups, std::size_t capacity) {
  // Expansion is geometric and never accompanies a pop. Unused slots contain
  // no identity and contribute zero to the Fenwick tree. Recovery starts at
  // head zero; end edits may subsequently wrap without moving the middle.
  // A newly recovered small List needs no spare metadata slots. Its first
  // expansion is cheap; larger rings retain geometric slack for bounded end
  // edits without rebuilding their middle chunks.
  if (capacity == 0 && groups.size() <= 32)
    capacity = groups.size();
  else if (capacity < groups.size())
    capacity = std::min<std::size_t>(
        UINT32_MAX, std::bit_ceil(std::max<std::size_t>(32, groups.size())));
  auto scratch = TryReserveMemory(AllocatorUsableSizeForRequest(
      capacity * (sizeof(OrderedGroupEntry) + sizeof(std::uint64_t)) + 1024));
  if (!scratch) {
    RecordMemoryRejection();
    return absl::ResourceExhaustedError("OOM List ring construction");
  }
  auto& list = std::get<ListSlots>(ids_);
  list.head_ = 0;
  for (std::size_t i = 0; i < groups.size(); ++i) {
    auto status = list.positions_.Set(groups[i].id_, {i, false});
    if (!status.ok()) return status;
  }
  groups.resize(capacity);
  std::vector<std::uint64_t> cumulative;
  cumulative.reserve(capacity);
  std::uint64_t count = 0;
  for (const auto& group : groups)
    cumulative.push_back(count += group.item_count_);
  auto slots = decltype(groups_)::From(groups);
  if (!slots.ok()) return slots.status();
  auto ranks = FenwickTree::FromCumulative(std::move(cumulative));
  if (!ranks.ok()) return ranks.status();
  groups_ = std::move(*slots);
  ranks_ = std::move(*ranks);
  return absl::OkStatus();
}

absl::Status OrderedGroupDirectory::ReplacePages(
    std::span<const RecoveredOrderedGroup> changed) {
  absl::InlinedVector<FenwickTree::CountChange, 4> counts;
  counts.reserve(changed.size());
  const auto* list = std::get_if<ListSlots>(&ids_);
  for (const auto& item : changed) {
    if (item.retired_) continue;
    const auto slot =
        list ? list->positions_.Get(item.id_)->slot_ : *FindIndex(item.id_);
    counts.emplace_back(
        slot, absl::int128(item.item_count_) - groups_[slot].item_count_);
    auto status = groups_.Set(slot, item);
    if (!status.ok()) return status;
  }
  std::sort(counts.begin(), counts.end());
  // Only boundaries beside changed pages can introduce a score inversion.
  if (root_.kind_ == OrderedCollectionKind::kSortedSet) {
    for (const auto& [index, delta] : counts) {
      if ((index != 0 &&
           groups_[index - 1].max_score_ > groups_[index].min_score_) ||
          (index + 1 != groups_.size() &&
           groups_[index].max_score_ > groups_[index + 1].min_score_))
        return absl::DataLossError("unordered updated Sorted Set score bounds");
    }
  }
  return ranks_.ApplyCounts(counts, root_.item_count_);
}

absl::StatusOr<OrderedGroupDirectory> OrderedGroupDirectory::ApplyList(
    const OrderedCollectionRoot& root, std::uint64_t revision,
    std::span<const RecoveredOrderedGroup> changed,
    std::uint64_t command_sequence) const {
  auto scratch = TryReserveMemory(AllocatorUsableSizeForRequest(
      changed.size() * (sizeof(OrderedGroupEntry) * 2 + 256) + 1024));
  if (!scratch) {
    RecordMemoryRejection();
    return absl::ResourceExhaustedError("OOM List directory changes");
  }
  const auto& list = std::get<ListSlots>(ids_);
  auto analysis = AnalyzeChanges(*this, root, revision, changed);
  if (!analysis.ok()) return analysis.status();
  const auto& replacements = analysis->pages_;
  OrderedGroupDirectory result = *this;
  result.root_ = root;
  result.root_.revision_ = revision;
  result.sequence_ = revision;
  result.command_sequence_ = command_sequence;
  result.total_group_bytes_ = analysis->bytes_;
  auto& next_list = std::get<ListSlots>(result.ids_);
  // Retirements append in arrival order, not ID order. Their positions share
  // the identity map with live ring slots, avoiding an O(history) sorted merge
  // when alternating pops retire IDs out of allocation order.
  std::vector<OrderedGroupEntry> retirements;
  for (const auto& item : changed) {
    if (!item.retired_) continue;
    const auto* position = list.positions_.Get(item.id_);
    if (position && position->retired_) {
      auto status = result.retired_.Set(position->slot_, item);
      if (!status.ok()) return status;
    } else {
      auto status = next_list.positions_.Set(
          item.id_, {retired_.size() + retirements.size(), true});
      if (!status.ok()) return status;
      retirements.push_back(item);
    }
  }
  if (!retirements.empty()) {
    auto retired = result.retired_.Appended(retirements);
    if (!retired.ok()) return retired.status();
    result.retired_ = std::move(*retired);
  }
  std::vector<FenwickTree::CountChange> count_changes;
  if (analysis->same_topology_) {
    auto status = result.ReplacePages(changed);
    if (!status.ok()) return status;
    return result;
  } else {
    const auto old_groups = groups();
    std::size_t begin = old_groups.size(), end = 0, changed_live = 0;
    for (const auto& item : changed) {
      if (const auto index = FindIndex(item.id_)) {
        begin = std::min(begin, *index);
        end = std::max(end, *index + 1);
      }
      changed_live += !item.retired_;
    }
    // All changed old live pages lie in [begin,end). Pages outside it retain
    // their checked links, so validate this interval and its two boundaries.
    // Reciprocal links, exact length and changed-identity coverage exclude
    // cycles, hidden/disconnected insertions and unreported removals.
    if (begin == old_groups.size())
      return absl::DataLossError(
          "List topology change has no predecessor page");
    const auto unchanged = old_groups.size() - (end - begin);
    if (root.group_count_ < unchanged)
      return absl::DataLossError("List replacement interval underflows");
    const auto replacement_size = root.group_count_ - unchanged;
    auto interval_scratch = TryReserveMemory(AllocatorUsableSizeForRequest(
        (replacement_size + end - begin) * (sizeof(OrderedGroupEntry) + 256) +
        1024));
    if (!interval_scratch) {
      RecordMemoryRejection();
      return absl::ResourceExhaustedError("OOM List replacement interval");
    }
    const auto right = end == old_groups.size() ? 0 : old_groups[end].id_;
    std::uint64_t previous = begin == 0 ? 0 : old_groups[begin - 1].id_;
    std::uint64_t id =
        begin == 0 ? root.first_group_ : old_groups[begin - 1].next_;
    if ((begin != 0 && root.first_group_ != root_.first_group_) ||
        (right != 0 && root.last_group_ != root_.last_group_))
      return absl::DataLossError("List root changes an untouched boundary");
    std::vector<OrderedGroupEntry> replacement;
    replacement.reserve(replacement_size);
    absl::flat_hash_set<std::uint64_t> visited;
    visited.reserve(replacement_size);
    std::size_t visited_changes = 0;
    while (id != right) {
      const auto found = replacements.find(id);
      const auto* item = found == replacements.end() ? Find(id) : found->second;
      const auto old_index = FindIndex(id);
      if (!item || item->retired_ || item->previous_ != previous ||
          (old_index && (*old_index < begin || *old_index >= end)) ||
          replacement.size() == replacement_size || !visited.insert(id).second)
        return absl::DataLossError("broken List replacement chain");
      replacement.push_back(*item);
      if (found != replacements.end()) {
        ++visited_changes;
      }
      previous = id;
      id = item->next_;
    }
    if (replacement.size() != replacement_size ||
        visited_changes != changed_live ||
        (right == 0 ? previous != root.last_group_
                    : old_groups[end].previous_ != previous))
      return absl::DataLossError("disconnected List replacement pages");
    for (std::size_t i = begin; i < end; ++i) {
      if (visited.contains(old_groups[i].id_)) continue;
      const auto removed = replacements.find(old_groups[i].id_);
      if (removed == replacements.end() || !removed->second->retired_)
        return absl::DataLossError("List removal lacks retirement evidence");
    }
    const auto capacity = groups_.size();
    if (root.group_count_ > capacity ||
        (begin != 0 && end != old_groups.size() &&
         replacement_size != end - begin)) {
      // Capacity growth or a length-changing middle splice may move all slots.
      // Ordinary end edits never enter this path while spare capacity remains.
      auto rebuild_scratch = TryReserveMemory(AllocatorUsableSizeForRequest(
          root.group_count_ * sizeof(OrderedGroupEntry) + 1024));
      if (!rebuild_scratch) {
        RecordMemoryRejection();
        return absl::ResourceExhaustedError("OOM List directory rebuild");
      }
      std::vector<OrderedGroupEntry> rebuilt;
      rebuilt.reserve(root.group_count_);
      rebuilt.insert(rebuilt.end(), old_groups.begin(),
                     old_groups.begin() + begin);
      rebuilt.insert(rebuilt.end(), replacement.begin(), replacement.end());
      rebuilt.insert(rebuilt.end(), old_groups.begin() + end, old_groups.end());
      auto status = result.BuildListSlots(std::move(rebuilt), capacity);
      if (!status.ok()) return status;
      return result;
    }
    if (begin == 0)
      next_list.head_ =
          (list.head_ + end + capacity - replacement_size) % capacity;
    // First clear the old interval, then overlay its replacement. Coalescing
    // by physical slot prevents transient double-counting during head moves.
    std::map<std::size_t, OrderedGroupEntry> slots;
    for (std::size_t i = begin; i < end; ++i)
      slots.emplace((list.head_ + i) % capacity, OrderedGroupEntry{});
    for (std::size_t i = 0; i < replacement.size(); ++i) {
      const auto slot = (next_list.head_ + begin + i) % capacity;
      slots[slot] = replacement[i];
      const auto* old = list.positions_.Get(replacement[i].id_);
      if (!old || old->slot_ != slot) {
        auto status =
            next_list.positions_.Set(replacement[i].id_, {slot, false});
        if (!status.ok()) return status;
      }
    }
    for (const auto& [slot, item] : slots) {
      if (item.id_ == groups_[slot].id_ && !replacements.contains(item.id_))
        continue;
      count_changes.emplace_back(
          slot, absl::int128(item.item_count_) - groups_[slot].item_count_);
      auto status = result.groups_.Set(slot, item);
      if (!status.ok()) return status;
    }
  }
  std::sort(count_changes.begin(), count_changes.end());
  auto status = result.ranks_.ApplyCounts(count_changes, root.item_count_);
  if (!status.ok()) return status;
  return result;
}

absl::StatusOr<OrderedGroupDirectory> OrderedGroupDirectory::Apply(
    const OrderedCollectionRoot& root, std::uint64_t revision,
    std::span<const RecoveredOrderedGroup> changed,
    std::uint64_t command_sequence,
    std::span<const RecoveredGroupedRecord> member_changes) const {
  if (root.kind_ != root_.kind_ || root.incarnation_ != root_.incarnation_ ||
      revision <= sequence_ || command_sequence < command_sequence_ ||
      root.next_group_id_ < root_.next_group_id_) {
    return absl::FailedPreconditionError("stale ordered directory update");
  }
  if (root.kind_ == OrderedCollectionKind::kList) {
    if (!member_changes.empty())
      return absl::DataLossError("List has member changes");
    return ApplyList(root, revision, changed, command_sequence);
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
    std::vector<OrderedGroupEntry> groups(groups_.begin(), groups_.end());
    groups.resize(root.group_count_);
    for (const auto& item : changed) {
      if (item.id_ == 0 || item.id_ > root.group_count_ || item.retired_ ||
          item.incarnation_ != root.incarnation_ ||
          item.sequence_ != revision ||
          groups[item.id_ - 1].sequence_ == revision)
        return absl::DataLossError("invalid changed String segment");
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
  auto analysis = AnalyzeChanges(*this, root, revision, changed);
  if (!analysis.ok()) return analysis.status();
  const auto& replacements = analysis->pages_;
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
  // validate changed links or reconstruct the chain below.
  if (analysis->same_topology_) {
    OrderedGroupDirectory result;
    result.root_ = root;
    result.root_.revision_ = revision;
    result.sequence_ = revision;
    result.command_sequence_ = command_sequence;
    result.groups_ = groups_;
    result.retired_ = retired_;
    result.linear_ids() = linear_ids();
    result.ranks_ = ranks_;
    result.members_ = std::move(members);
    if (root.kind_ == OrderedCollectionKind::kStream && has_stream_header() &&
        !replacements.contains(root.first_group_))
      result.stream_header_ = stream_header_;
    result.total_group_bytes_ = analysis->bytes_;
    auto status = result.ReplacePages(changed);
    if (!status.ok()) return status;
    return result;
  }
  // Foreground updates contain one decided replacement per changed identity.
  // The predecessor has already selected and validated all other winners.
  // Do not rebuild recovery's candidate hash map (or copy every record into
  // it) just to select those same winners again. Check replacements against
  // that adjudicated chain before choosing incremental or full validation.
  std::vector<OrderedGroupEntry> new_retired;
  for (const auto& item : changed)
    if (item.retired_) new_retired.push_back(item);
  OrderedGroupDirectory rebuilt;
  rebuilt.root_ = root;
  rebuilt.root_.revision_ = revision;
  rebuilt.sequence_ = revision;
  rebuilt.command_sequence_ = command_sequence;
  rebuilt.members_ = std::move(members);
  // Stream messages sort before node/group metadata. Appending a message
  // therefore usually inserts near the end rather than after the last page.
  // Preserve the already checked prefix and validate only a bounded suffix;
  // front trims and broad rewrites retain the full reconstruction below.
  if (root.kind_ == OrderedCollectionKind::kStream && changed.size() <= 8 &&
      new_retired.empty() && root.first_group_ == root_.first_group_ &&
      root.group_count_ > groups_.size()) {
    std::size_t first = groups_.size();
    bool fresh_ids = true;
    for (const auto& item : changed) {
      if (const auto index = FindIndex(item.id_)) {
        const auto& old = groups_[*index];
        if (item.previous_ != old.previous_ || item.next_ != old.next_)
          first = std::min(first, *index);
      } else {
        // Fresh identities sort after the existing identity index even when
        // their logical order differs. Old reserved ids use the general path.
        fresh_ids &= item.id_ >= root_.next_group_id_;
      }
    }
    if (fresh_ids && first < groups_.size() && groups_.size() - first <= 256) {
      const auto suffix_size = root.group_count_ - first;
      auto scratch = TryReserveMemory(AllocatorUsableSizeForRequest(
          suffix_size *
              (sizeof(const OrderedGroupEntry*) + 2 * sizeof(std::uint64_t)) +
          1024));
      if (!scratch) {
        RecordMemoryRejection();
        return absl::ResourceExhaustedError("OOM Stream suffix update scratch");
      }
      std::vector<const OrderedGroupEntry*> suffix;
      std::vector<std::uint64_t> prefixes;
      suffix.reserve(suffix_size);
      prefixes.reserve(suffix_size);
      absl::InlinedVector<std::pair<std::size_t, absl::int128>, 8>
          prefix_changes;
      absl::int128 prefix_count = CountBefore(first);
      for (const auto& item : changed) {
        if (const auto index = FindIndex(item.id_); index && *index < first) {
          const auto delta =
              absl::int128(item.item_count_) - groups_[*index].item_count_;
          prefix_changes.emplace_back(*index, delta);
          prefix_count += delta;
        }
      }
      if (prefix_count < 0 || prefix_count > root.item_count_)
        return absl::DataLossError("Stream suffix aggregate overflow");
      absl::int128 count = prefix_count;
      std::uint64_t id = groups_[first].id_;
      std::uint64_t previous = first == 0 ? 0 : groups_[first - 1].id_;
      absl::InlinedVector<std::pair<std::uint64_t, std::size_t>, 8> new_ids;
      while (id != 0) {
        const auto replacement = replacements.find(id);
        const auto old_index = FindIndex(id);
        if (replacement == replacements.end() && !old_index)
          return absl::DataLossError("missing Stream suffix page");
        const auto* item = replacement == replacements.end()
                               ? &groups_[*old_index]
                               : replacement->second;
        if (item->retired_ || item->previous_ != previous ||
            (old_index && *old_index < first) || suffix.size() == suffix_size)
          return absl::DataLossError("broken Stream suffix chain");
        const auto index = first + suffix.size();
        if (!old_index) new_ids.emplace_back(id, index);
        count += item->item_count_;
        if (count > root.item_count_)
          return absl::DataLossError("Stream suffix count overflow");
        prefixes.push_back(static_cast<std::uint64_t>(count));
        suffix.push_back(item);
        previous = id;
        id = item->next_;
      }
      // The prefix keeps all its links. In the suffix, reciprocal previous
      // links forbid cycles and the exact live count requires every old page
      // and fresh identity to remain reachable, including unmodified pages.
      if (suffix.size() != suffix_size || previous != root.last_group_ ||
          count != root.item_count_ ||
          new_ids.size() != root.group_count_ - groups_.size())
        return absl::DataLossError("disconnected Stream suffix pages");
      std::sort(new_ids.begin(), new_ids.end());
      const auto old_suffix_size = groups_.size() - first;
      absl::InlinedVector<OrderedGroupEntry, 8> extended;
      for (std::size_t i = old_suffix_size; i < suffix.size(); ++i) {
        extended.push_back(*suffix[i]);
      }
      auto groups = groups_.Appended(extended);
      if (!groups.ok()) return groups.status();
      auto ids = linear_ids().Appended(new_ids);
      if (!ids.ok()) return ids.status();
      auto ranks =
          ranks_.WithSuffix(first, prefixes, prefix_changes, root.item_count_);
      if (!ranks.ok()) return ranks.status();
      rebuilt.groups_ = std::move(*groups);
      rebuilt.linear_ids() = std::move(*ids);
      rebuilt.ranks_ = std::move(*ranks);
      rebuilt.retired_ = retired_;
      rebuilt.total_group_bytes_ = analysis->bytes_;
      for (std::size_t i = 0; i < suffix.size(); ++i) {
        const auto index = first + i;
        const auto& item = *suffix[i];
        if (i < old_suffix_size) {
          auto status = rebuilt.groups_.Set(index, item);
          if (!status.ok()) return status;
        }
        const auto found = std::lower_bound(
            linear_ids().begin(), linear_ids().end(), item.id_,
            [](const auto& entry, auto id) { return entry.first < id; });
        if (found != linear_ids().end() && found->first == item.id_ &&
            found->second != index) {
          auto status = rebuilt.linear_ids().Set(found - linear_ids().begin(),
                                                 {item.id_, index});
          if (!status.ok()) return status;
        }
      }
      for (const auto& item : changed) {
        if (const auto index = FindIndex(item.id_); index && *index < first) {
          auto status = rebuilt.groups_.Set(*index, item);
          if (!status.ok()) return status;
        }
      }
      if (has_stream_header() && !replacements.contains(root.first_group_)) {
        rebuilt.stream_header_ = stream_header_;
      }
      return rebuilt;
    }
  }
  auto ordinal_admission = TryReserveMemory(AllocatorUsableSizeForRequest(
      groups_.size() * sizeof(std::size_t) + changed.size() * 32 + 1024));
  if (!ordinal_admission) {
    RecordMemoryRejection();
    return absl::ResourceExhaustedError("OOM ordered structural ordinals");
  }
  const auto absent = std::numeric_limits<std::size_t>::max();
  std::vector<std::size_t> positions(groups_.size(), absent);
  std::vector<std::pair<std::uint64_t, std::size_t>> inserted_ids;
  std::vector<OrderedGroupEntry> groups;
  std::vector<std::pair<std::uint64_t, std::size_t>> ids;
  std::vector<std::uint64_t> ends;
  groups.reserve(root.group_count_);
  ids.reserve(root.group_count_);
  ends.reserve(root.group_count_);
  std::uint64_t id = root.first_group_, previous = 0, count = 0;
  std::size_t old_cursor = 0, visited_changes = 0;
  while (id != 0) {
    const OrderedGroupEntry* item = nullptr;
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
  // Most append/split updates create no tombstones. Keep the already
  // adjudicated resident predecessor records shared.
  // Monotonically retired identities need only a persistent tail append.
  if (new_retired.empty()) {
    rebuilt.retired_ = retired_;
  } else if (retired_.empty() ||
             retired_.back().id_ < new_retired.front().id_) {
    auto appended = retired_.Appended(new_retired);
    if (!appended.ok()) return appended.status();
    rebuilt.retired_ = std::move(*appended);
  } else {
    std::vector<OrderedGroupEntry> retired;
    retired.reserve(retired_.size() + new_retired.size());
    auto old = retired_.begin();
    for (auto item : new_retired) {
      for (; old != retired_.end() && old->id_ < item.id_; ++old)
        retired.push_back(*old);
      if (old != retired_.end() && old->id_ == item.id_) ++old;
      retired.push_back(item);
    }
    retired.insert(retired.end(), old, retired_.end());
    auto array = decltype(retired_)::From(retired);
    if (!array.ok()) return array.status();
    rebuilt.retired_ = std::move(*array);
  }
  // The predecessor identity index is already sorted. Its ordinal changes,
  // not its identities. Reindex surviving old pages and merge only the new
  // identities rather than sorting the entire directory after a middle split.
  std::sort(inserted_ids.begin(), inserted_ids.end());
  auto inserted = inserted_ids.begin();
  for (const auto& [old_id, old_position] : linear_ids()) {
    if (positions[old_position] == absent) continue;
    for (; inserted != inserted_ids.end() && inserted->first < old_id;
         ++inserted)
      ids.push_back(*inserted);
    ids.emplace_back(old_id, positions[old_position]);
  }
  ids.insert(ids.end(), inserted, inserted_ids.end());
  // A tail split/append often preserves every existing ordinal. Validation
  // above still walks the complete chain, but publication need not copy its
  // unchanged metadata again. Middle insertion/removal keeps the rebuild
  // path because its shifted ordinals require a different array layout.
  bool preserves_ordinals = groups.size() >= groups_.size();
  for (std::size_t i = 0; preserves_ordinals && i < positions.size(); ++i)
    preserves_ordinals = positions[i] == i;
  auto group_array =
      preserves_ordinals
          ? groups_.Appended(std::span(groups).subspan(groups_.size()))
          : decltype(groups_)::From(groups);
  if (!group_array.ok()) return group_array.status();
  if (preserves_ordinals) {
    for (const auto& item : changed) {
      if (const auto position = FindIndex(item.id_)) {
        auto status = group_array->Set(*position, groups[*position]);
        if (!status.ok()) return status;
      }
    }
  }
  auto id_array = LinearIds::From(ids);
  if (!id_array.ok()) return id_array.status();
  auto end_array = FenwickTree::FromCumulative(std::move(ends));
  if (!end_array.ok()) return end_array.status();
  rebuilt.groups_ = std::move(*group_array);
  rebuilt.linear_ids() = std::move(*id_array);
  rebuilt.ranks_ = std::move(*end_array);
  if (root.kind_ == OrderedCollectionKind::kStream && has_stream_header()) {
    const auto* old_first = Find(root_.first_group_);
    const auto* new_first = rebuilt.Find(root.first_group_);
    if (old_first != nullptr && new_first != nullptr &&
        old_first->id_ == new_first->id_ &&
        old_first->sequence_ == new_first->sequence_) {
      rebuilt.stream_header_ = stream_header_;
    }
  }
  return rebuilt;
}

std::uint64_t OrderedGroupDirectory::CountBefore(
    std::size_t index) const noexcept {
  if (index == 0) return 0;
  if (index >= root_.group_count_) return root_.item_count_;
  if (const auto* list = std::get_if<ListSlots>(&ids_)) {
    const auto before = ranks_.CountBefore(list->head_);
    const auto end = list->head_ + index;
    if (end <= groups_.size()) return ranks_.CountBefore(end) - before;
    return root_.item_count_ - before +
           ranks_.CountBefore(end - groups_.size());
  }
  if (root_.kind_ == OrderedCollectionKind::kString)
    return index * kStringGroupBytes;
  return ranks_.CountBefore(index);
}

std::optional<OrderedGroupDirectory::Position> OrderedGroupDirectory::FindRank(
    std::uint64_t rank) const noexcept {
  if (rank >= root_.item_count_) return std::nullopt;
  if (root_.kind_ == OrderedCollectionKind::kString)
    return Position{rank / kStringGroupBytes, rank % kStringGroupBytes};
  if (const auto* list = std::get_if<ListSlots>(&ids_)) {
    const auto before = ranks_.CountBefore(list->head_);
    const auto first = root_.item_count_ - before;
    auto position = ranks_.Locate(rank < first ? before + rank : rank - first);
    position.group_index_ =
        (position.group_index_ + groups_.size() - list->head_) % groups_.size();
    return position;
  }
  return ranks_.Locate(rank);
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
    const auto current = loaded.find(groups[i].id_);
    if (current == loaded.end()) {
      // List values have no cross-page ordering constraint. The directory
      // already validates links; require a neighbour's payload only if its
      // link actually changes after the split/removal below.
      if (root.kind_ == OrderedCollectionKind::kList && (i < begin || i >= end))
        continue;
      return absl::InvalidArgumentError("ordered splice needs adjacent pages");
    }
    if (i != required_begin) {
      const auto previous = loaded.find(groups[i - 1].id_);
      if (previous != loaded.end()) {
        auto boundary =
            ValidateOrderedGroupBoundary(previous->second, current->second);
        if (!boundary.ok()) return boundary;
      }
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
  else if (groups[begin - 1].next_ != new_first) {
    const auto found = loaded.find(previous);
    if (found == loaded.end())
      return absl::InvalidArgumentError("ordered splice needs previous page");
    auto& neighbour = found->second;
    neighbour.next_ = new_first;
    plan.writes_.push_back(std::move(neighbour));
  }
  if (end == groups.size())
    plan.root_.last_group_ = new_last;
  else if (groups[end].previous_ != new_last) {
    const auto found = loaded.find(next);
    if (found == loaded.end())
      return absl::InvalidArgumentError("ordered splice needs next page");
    auto& neighbour = found->second;
    neighbour.previous_ = new_last;
    plan.writes_.push_back(std::move(neighbour));
  }
  plan.root_.group_count_ =
      root.group_count_ - (end - begin) + replacement_count;
  return plan;
}

}  // namespace lavik::storage
