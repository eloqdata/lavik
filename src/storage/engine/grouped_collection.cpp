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

#include "lavik/storage/detail/stream_records.h"

namespace lavik::storage {
namespace {

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
  return kind == OrderedCollectionKind::kList ||
         kind == OrderedCollectionKind::kSortedSet ||
         kind == OrderedCollectionKind::kStream;
}

bool ValidRoot(const OrderedCollectionRoot& root) {
  if ((root.kind_ == OrderedCollectionKind::kStream) !=
          root.stream_length_.has_value() ||
      (root.stream_length_ && *root.stream_length_ > UINT32_MAX))
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
  // Both shapes are v1. An explicit presence flag, rather than length alone,
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
  Store(bytes, 48, group.entries_.size(), 4);
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
      result.item_count_ >
          (encoded_bytes - kOrderedGroupHeaderBytes) / kEntryHeaderBytes) {
    return absl::DataLossError("invalid ordered page envelope identity/count");
  }
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
  } else if (kind != OrderedCollectionKind::kList &&
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
  std::map<std::uint64_t, RecoveredOrderedGroup> winners;
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
    auto [it, inserted] = winners.emplace(candidate.id_, candidate);
    if (inserted) continue;
    auto& winner = it->second;
    if (candidate.sequence_ == winner.sequence_ &&
        !SameMetadata(candidate, winner)) {
      return absl::DataLossError("conflicting ordered page relocation");
    }
    if (candidate.sequence_ > winner.sequence_ ||
        (candidate.sequence_ == winner.sequence_ &&
         candidate.lsn_ > winner.lsn_))
      winner = candidate;
  }
  OrderedGroupDirectory result;
  result.members_ = std::move(members);
  for (auto it = winners.begin(); it != winners.end();) {
    if (it->second.retired_) {
      result.retired_.push_back(it->second);
      it = winners.erase(it);
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
  result.groups_.reserve(winners.size());
  result.ends_.reserve(winners.size());
  result.ids_.reserve(winners.size());
  std::uint64_t id = root.first_group_;
  std::uint64_t previous = 0;
  std::uint64_t count = 0;
  while (id != 0) {
    auto found = winners.find(id);
    if (found == winners.end() || found->second.previous_ != previous ||
        found->second.item_count_ > root.item_count_ - count) {
      return absl::DataLossError("broken ordered page chain or count");
    }
    const auto& group = found->second;
    if (root.kind_ == OrderedCollectionKind::kSortedSet &&
        !result.groups_.empty() &&
        result.groups_.back().max_score_ > group.min_score_)
      return absl::DataLossError("unordered recovered Sorted Set score bounds");
    result.ids_.emplace_back(group.id_, result.groups_.size());
    result.groups_.push_back(group);
    count += group.item_count_;
    result.ends_.push_back(count);
    previous = id;
    id = group.next_;
    winners.erase(found);  // Also detects a cycle without a second set.
  }
  if (!winners.empty() || previous != root.last_group_ ||
      count != root.item_count_)
    return absl::DataLossError(
        "disconnected ordered pages or aggregate mismatch");
  std::sort(result.ids_.begin(), result.ids_.end());
  return result;
}

const RecoveredOrderedGroup* OrderedGroupDirectory::Find(
    std::uint64_t id) const noexcept {
  const auto found = std::lower_bound(
      ids_.begin(), ids_.end(), id,
      [](const auto& item, auto target) { return item.first < target; });
  return found != ids_.end() && found->first == id ? &groups_[found->second]
                                                   : nullptr;
}

const RecoveredOrderedGroup* OrderedGroupDirectory::FindRecord(
    std::uint64_t id) const noexcept {
  if (const auto* active = Find(id)) return active;
  const auto found = std::lower_bound(
      retired_.begin(), retired_.end(), id,
      [](const auto& item, auto target) { return item.id_ < target; });
  return found != retired_.end() && found->id_ == id ? &*found : nullptr;
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
  std::vector<RecoveredOrderedGroup> candidates;
  candidates.reserve(groups_.size() + retired_.size() + changed.size());
  // These previous candidates are already adjudicated by this root. Their
  // original transaction ids are not re-decided by a later command's batch.
  auto append = [&](RecoveredOrderedGroup item) {
    item.txid_ = 0;
    item.batch_txid_ = 0;
    candidates.push_back(item);
  };
  for (const auto& item : groups_) append(item);
  for (const auto& item : retired_) append(item);
  absl::flat_hash_set<std::uint64_t> ids;
  for (const auto& item : changed) {
    if (item.sequence_ != revision || !ids.insert(item.id_).second ||
        (FindRecord(item.id_) != nullptr && FindRecord(item.id_)->retired_ &&
         !item.retired_)) {
      return absl::DataLossError("invalid ordered changed page identity");
    }
    append(item);
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
  return Recover(root, revision, candidates, {}, command_sequence,
                 std::move(members));
}

std::optional<OrderedGroupDirectory::Position> OrderedGroupDirectory::FindRank(
    std::uint64_t rank) const noexcept {
  if (rank >= root_.item_count_) return std::nullopt;
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
    auto found =
        std::find_if(groups.begin(), groups.end(), [&](const auto& current) {
          return current.id_ == candidate.snapshot_.id_;
        });
    const auto& page = candidate.snapshot_;
    if (found == groups.end() || candidate.sequence_ != found->sequence_ ||
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
