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

#include "lavik/storage/detail/ordered_compact_codec.h"

#include <bit>
#include <cassert>
#include <cmath>
#include <cstring>
#include <limits>

#include "lavik/storage/detail/stream_records.h"

namespace lavik::storage {
namespace {

bool ValidKind(OrderedCollectionKind kind) {
  return kind == OrderedCollectionKind::kList ||
         kind == OrderedCollectionKind::kSortedSet;
}

void Put(std::string& output, std::uint64_t value, unsigned width) {
  for (unsigned i = 0; i < width; ++i) {
    output.push_back(static_cast<char>(value >> (i * 8)));
  }
}

std::uint64_t Get(std::string_view input, std::size_t offset, unsigned width) {
  std::uint64_t value = 0;
  for (unsigned i = 0; i < width; ++i) {
    value |= std::uint64_t(static_cast<unsigned char>(input[offset + i]))
             << (i * 8);
  }
  return value;
}

}  // namespace

absl::StatusOr<IndexedOrderedReader> IndexedOrderedReader::Open(
    OrderedCollectionKind kind, std::string_view body, std::size_t count) {
  const bool sorted = kind == OrderedCollectionKind::kSortedSet;
  const std::size_t stride = sorted ? 12 : 4;
  if (!ValidKind(kind) || body.size() > UINT32_MAX || body.size() < 4 ||
      count > (body.size() - 4) / stride)
    return absl::DataLossError("invalid indexed ordered arrays");
  const auto data = body.size() - count * stride - 4;
  const auto offsets = data + (sorted ? count * 8 : 0);
  if (Get(body, offsets, 4) != 0 || Get(body, offsets + count * 4, 4) != data)
    return absl::DataLossError("invalid indexed ordered sentinels");
  return IndexedOrderedReader(body, count, data, offsets, sorted);
}

absl::StatusOr<double> IndexedOrderedReader::Score(std::size_t index) const {
  if (index >= count_)
    return absl::OutOfRangeError("indexed ordered slot out of range");
  if (!sorted_) return 0.0;
  const auto score =
      std::bit_cast<double>(Get(body_, data_bytes_ + index * 8, 8));
  if (std::isnan(score))
    return absl::DataLossError("NaN indexed ordered score");
  return score;
}

absl::StatusOr<OrderedCollectionEntryView> IndexedOrderedReader::At(
    std::size_t index) const {
  auto score = Score(index);
  if (!score.ok()) return score.status();
  const auto begin = Get(body_, offsets_ + index * 4, 4);
  const auto end = Get(body_, offsets_ + (index + 1) * 4, 4);
  if (begin > end || end > data_bytes_ || end - begin > kMaxStringBytes)
    return absl::DataLossError("invalid indexed ordered interval");
  return OrderedCollectionEntryView{body_.substr(begin, end - begin), *score};
}

template <typename Visitor>
absl::Status IndexedOrderedReader::VisitAll(Visitor&& visitor) const {
  // Open certified both array bounds and the first/last offsets. Carry the
  // previous end forward so each interior offset is loaded only once. Keep
  // this loop independent of At/Score: full-page consumers need neither
  // repeated slot checks nor a StatusOr result for every member and score.
  std::size_t begin = 0;
  for (std::size_t i = 0; i < count_; ++i) {
    const auto end = Get(body_, offsets_ + (i + 1) * 4, 4);
    if (begin > end || end > data_bytes_ || end - begin > kMaxStringBytes)
      return absl::DataLossError("invalid indexed ordered interval");
    const auto score =
        sorted_ ? std::bit_cast<double>(Get(body_, data_bytes_ + i * 8, 8))
                : 0.0;
    if (std::isnan(score))
      return absl::DataLossError("NaN indexed ordered score");
    auto status = visitor(
        i, OrderedCollectionEntryView{
               std::string_view(body_.data() + begin, end - begin), score});
    if (!status.ok()) return status;
    begin = end;
  }
  return absl::OkStatus();
}

absl::StatusOr<std::vector<OrderedCollectionEntryView>>
IndexedOrderedReader::DecodeAll() const {
  std::vector<OrderedCollectionEntryView> entries;
  entries.reserve(count_);
  auto status = VisitAll([&](std::size_t, OrderedCollectionEntryView entry) {
    entries.push_back(entry);
    return absl::OkStatus();
  });
  if (!status.ok()) return status;
  return entries;
}

absl::StatusOr<std::size_t> IndexedOrderedReader::FindSortedSetMember(
    std::string_view member) const {
  if (!sorted_)
    return absl::InvalidArgumentError("member lookup requires Sorted Set");
  std::size_t found = count_;
  OrderedCollectionEntryView previous;
  auto status = VisitAll([&](std::size_t i, OrderedCollectionEntryView entry) {
    if (i != 0 &&
        (previous.score_ > entry.score_ ||
         (previous.score_ == entry.score_ && previous.value_ > entry.value_)))
      return absl::DataLossError("unsorted compact Sorted Set");
    previous = entry;
    if (entry.value_ == member) {
      if (found != count_)
        return absl::DataLossError("duplicate persisted Sorted Set member");
      found = i;
    }
    return absl::OkStatus();
  });
  if (!status.ok()) return status;
  return found;
}

absl::StatusOr<std::size_t> IndexedOrderedReader::LowerBoundScore(
    double value, bool exclusive) const {
  if (!sorted_ || std::isnan(value))
    return absl::InvalidArgumentError("invalid indexed score bound");
  std::size_t first = 0, last = count_;
  while (first < last) {
    const auto middle = first + (last - first) / 2;
    auto score = Score(middle);
    if (!score.ok()) return score.status();
    if (exclusive ? *score <= value : *score < value)
      first = middle + 1;
    else
      last = middle;
  }
  return first;
}

absl::StatusOr<std::size_t> IndexedOrderedReader::LowerBound(
    double score, std::string_view member) const {
  if (!sorted_ || std::isnan(score))
    return absl::InvalidArgumentError("invalid indexed member bound");
  std::size_t first = 0, last = count_;
  while (first < last) {
    const auto middle = first + (last - first) / 2;
    auto entry = At(middle);
    if (!entry.ok()) return entry.status();
    if (entry->score_ < score ||
        (entry->score_ == score && entry->value_ < member))
      first = middle + 1;
    else
      last = middle;
  }
  return first;
}

absl::StatusOr<PackedValues> IndexedOrderedReader::CopyRange(
    std::size_t first, std::size_t count) const {
  if (first > count_ || count > count_ - first)
    return absl::OutOfRangeError("indexed ordered range exceeds page");
  PackedValues result;
  result.values_.reserve(count);
  std::size_t begin = 0, end = 0;
  for (std::size_t i = 0; i < count; ++i) {
    auto entry = At(first + i);
    if (!entry.ok()) return entry.status();
    const auto at =
        static_cast<std::size_t>(entry->value_.data() - body_.data());
    if (i == 0) begin = at;
    end = at + entry->value_.size();
    result.values_.push_back({0, at - begin, entry->value_.size()});
  }
  if (count != 0) result.blocks_.emplace_back(body_.substr(begin, end - begin));
  return result;
}

absl::Status IndexedOrderedSplice::EditSortedSetMember(
    std::size_t removed, std::string_view member, std::optional<double> after) {
  assert(parts_size_ == 0 && phase_ == 0);
  const auto count = source_.size();
  if (!source_.sorted_ || removed > count || (removed == count && !after) ||
      member.size() > kMaxStringBytes || (after && std::isnan(*after)))
    return absl::InvalidArgumentError("invalid indexed Sorted Set edit");
  if (removed != count) {
    auto old = source_.At(removed);
    if (!old.ok()) return old.status();
    if (old->value_ != member)
      return absl::DataLossError("indexed Sorted Set edit source mismatch");
  }
  const auto retained =
      source_.data().size() - (removed != count ? member.size() : 0);
  if (after && member.size() > UINT32_MAX - retained)
    return absl::OutOfRangeError("indexed Sorted Set edit size overflow");
  auto inserted = after ? source_.LowerBound(*after, member)
                        : absl::StatusOr<std::size_t>(count);
  if (!inserted.ok()) return inserted.status();
  // Both positions address the immutable source. Remove before inserting
  // when moving right; insert before removing when moving left (or staying).
  // Equal positions also cover replacing the first/last member and score ties.
  std::size_t first = 0;
  if (removed < *inserted) {
    AppendSource(first, removed);
    first = removed + 1;
  }
  AppendSource(first, *inserted - first);
  if (after) AppendMember(member, *after);
  first = *inserted;
  if (removed >= *inserted && removed != count) {
    AppendSource(first, removed - first);
    first = removed + 1;
  }
  AppendSource(first, count - first);
  return absl::OkStatus();
}

std::string_view IndexedOrderedSplice::SourceBytes(
    const Part& part) const noexcept {
  const auto begin = Get(source_.body_, source_.offsets_ + part.first_ * 4, 4);
  const auto end =
      Get(source_.body_, source_.offsets_ + (part.first_ + part.count_) * 4, 4);
  return source_.body_.substr(begin, end - begin);
}

std::string_view IndexedOrderedSplice::Value(const Part& part,
                                             std::size_t index) const noexcept {
  if (part.inserted_) {
    if (!has_inputs_) return member_.value_;
    return inputs_[reverse_inputs_ ? inputs_.size() - 1 - index : index];
  }
  const auto slot = part.first_ + index;
  const auto begin = Get(source_.body_, source_.offsets_ + slot * 4, 4);
  const auto end = Get(source_.body_, source_.offsets_ + (slot + 1) * 4, 4);
  return source_.body_.substr(begin, end - begin);
}

void IndexedOrderedSplice::Add(Part part, std::size_t bytes) noexcept {
  assert(phase_ == 0 && parts_size_ < parts_.size());
  assert(part.count_ <= UINT32_MAX - count_ &&
         bytes <= UINT32_MAX - data_bytes_);
  parts_[parts_size_++] = part;
  count_ += part.count_;
  data_bytes_ += bytes;
}

void IndexedOrderedSplice::AppendSource(std::size_t first,
                                        std::size_t count) noexcept {
  assert(first <= source_.size() && count <= source_.size() - first);
  if (count == 0) return;
  const Part part{first, count, false};
  Add(part, SourceBytes(part).size());
}

void IndexedOrderedSplice::AppendValues(
    std::span<const std::string_view> values, bool reverse) noexcept {
  assert(!source_.sorted_ && !inserted_);
  if (values.empty()) return;
  inserted_ = has_inputs_ = true;
  inputs_ = values;
  reverse_inputs_ = reverse;
  std::size_t bytes = 0;
  for (auto value : values) bytes += value.size();
  Add({0, values.size(), true}, bytes);
}

void IndexedOrderedSplice::AppendMember(std::string_view member,
                                        double score) noexcept {
  assert(source_.sorted_ && !inserted_);
  inserted_ = true;
  member_ = {member, score};
  Add({0, 1, true}, member.size());
}

std::optional<std::string_view> IndexedOrderedSplice::Next() noexcept {
  auto number = [&](std::uint64_t value, unsigned width) {
    for (unsigned i = 0; i < width; ++i)
      framing_[i] = static_cast<char>(value >> (8 * i));
    return std::string_view(framing_.data(), width);
  };
  if (phase_ == 0) {
    phase_ = 1;
    if (compact_) {
      const auto magic = source_.sorted_ ? kSortedSetMagicV2 : kListMagicV2;
      std::memcpy(framing_.data(), magic.data(), magic.size());
      for (unsigned i = 0; i < 4; ++i)
        framing_[4 + i] = static_cast<char>(count_ >> (8 * i));
      return std::string_view(framing_.data(), framing_.size());
    }
  }
  if (phase_ == 1) {
    if (part_ < parts_size_) {
      const auto& part = parts_[part_];
      if (!part.inserted_) {
        ++part_;
        return SourceBytes(part);
      }
      const auto value = Value(part, item_++);
      if (item_ == part.count_) {
        item_ = 0;
        ++part_;
      }
      return value;
    }
    phase_ = source_.sorted_ ? 2 : 3;
    part_ = 0;
  }
  if (phase_ == 2) {
    if (part_ < parts_size_) {
      const auto& part = parts_[part_++];
      if (!part.inserted_)
        return source_.body_.substr(source_.data_bytes_ + part.first_ * 8,
                                    part.count_ * 8);
      return number(std::bit_cast<std::uint64_t>(member_.score_), 8);
    }
    phase_ = 3;
    part_ = 0;
  }
  if (phase_ == 3) {
    if (part_ == parts_size_) {
      assert(offset_ == data_bytes_);
      phase_ = 4;
      return number(offset_, 4);
    }
    const auto& part = parts_[part_];
    // Unshifted source offsets need no regeneration. Other offsets use only
    // fixed framing storage, consumed before the next call, like v1 lengths.
    if (!part.inserted_ && item_ == 0 &&
        offset_ == Get(source_.body_, source_.offsets_ + part.first_ * 4, 4)) {
      offset_ += SourceBytes(part).size();
      ++part_;
      return source_.body_.substr(source_.offsets_ + part.first_ * 4,
                                  part.count_ * 4);
    }
    const auto value = number(offset_, 4);
    offset_ += Value(part, item_++).size();
    if (item_ == part.count_) {
      item_ = 0;
      ++part_;
    }
    return value;
  }
  return std::nullopt;
}

namespace {
absl::StatusOr<std::string> EncodeIndexedCompactValue(
    OrderedCollectionKind kind,
    std::span<const OrderedCollectionEntry> entries) {
  if (!ValidKind(kind) || entries.size() > UINT32_MAX)
    return absl::InvalidArgumentError("invalid indexed ordered kind/count");
  const bool sorted = kind == OrderedCollectionKind::kSortedSet;
  const std::size_t stride = sorted ? 12 : 4;
  if (entries.size() > (UINT32_MAX - 4) / stride)
    return absl::OutOfRangeError("indexed ordered arrays exceed offset range");
  std::size_t size = entries.size() * stride + 4;
  for (const auto& entry : entries) {
    if (entry.value_.size() > kMaxStringBytes ||
        entry.value_.size() > UINT32_MAX - size || std::isnan(entry.score_) ||
        (!sorted && std::bit_cast<std::uint64_t>(entry.score_) != 0))
      return absl::OutOfRangeError("invalid indexed ordered entry");
    size += entry.value_.size();
  }
  std::string output;
  output.reserve(8 + size);
  output.append(sorted ? kSortedSetMagicV2 : kListMagicV2);
  Put(output, entries.size(), 4);
  for (const auto& entry : entries) output.append(entry.value_);
  if (sorted)
    for (const auto& entry : entries)
      Put(output, std::bit_cast<std::uint64_t>(entry.score_), 8);
  std::size_t offset = 0;
  Put(output, 0, 4);
  for (const auto& entry : entries) {
    offset += entry.value_.size();
    Put(output, offset, 4);
  }
  return output;
}
}  // namespace

absl::StatusOr<OrderedCompactReader> OrderedCompactReader::Open(
    OrderedCollectionKind kind, std::string_view encoded,
    std::uint64_t expected_count) {
  const bool sorted = kind == OrderedCollectionKind::kSortedSet;
  if (ValidKind(kind) && expected_count != 0 && encoded.size() >= 8 &&
      encoded.starts_with(sorted ? kSortedSetMagicV2 : kListMagicV2) &&
      Get(encoded, 4, 4) == expected_count) {
    auto indexed =
        IndexedOrderedReader::Open(kind, encoded.substr(8), expected_count);
    if (!indexed.ok()) return indexed.status();
    OrderedCompactReader reader(encoded, expected_count, sorted);
    reader.indexed_ = *indexed;
    return reader;
  }
  const std::size_t framing = sorted ? 12 : 4;
  if (!ValidKind(kind) || expected_count == 0 || expected_count > UINT32_MAX ||
      encoded.size() < 8 ||
      !encoded.starts_with(sorted ? kSortedSetMagicV1 : kListMagicV1) ||
      Get(encoded, 4, 4) != expected_count ||
      expected_count > (encoded.size() - 8) / framing)
    return absl::DataLossError("invalid ordered full-image framing");
  return OrderedCompactReader(encoded, expected_count, sorted);
}

absl::StatusOr<OrderedCompactEntryView> OrderedCompactReader::Next() {
  if (indexed_) {
    auto entry = indexed_->At(size_ - remaining_);
    if (!entry.ok()) return entry.status();
    --remaining_;
    return OrderedCompactEntryView{entry->value_, entry->score_, entry->value_};
  }
  const std::size_t framing = sorted_ ? 12 : 4;
  if (remaining_ == 0 || framing > bytes_.size() - offset_)
    return absl::DataLossError("truncated ordered full-image entry");
  const auto score =
      sorted_ ? std::bit_cast<double>(Get(bytes_, offset_, 8)) : 0;
  const auto size = Get(bytes_, offset_ + (sorted_ ? 8 : 0), 4);
  if (std::isnan(score) || size > kMaxStringBytes ||
      size > bytes_.size() - offset_ - framing)
    return absl::DataLossError("invalid ordered full-image entry");
  const auto end = offset_ + framing + size;
  if (remaining_ == 1 && end != bytes_.size())
    return absl::DataLossError("trailing ordered full-image bytes");
  OrderedCompactEntryView result{
      .value_ = bytes_.substr(offset_ + framing, size),
      .score_ = score,
      .encoded_ = bytes_.substr(offset_, framing + size)};
  offset_ = end;
  --remaining_;
  return result;
}

absl::StatusOr<std::size_t> AppendOrderedEntrySize(OrderedCollectionKind kind,
                                                   std::size_t encoded_bytes,
                                                   std::size_t item_bytes,
                                                   std::size_t max_bytes) {
  if (!ValidKind(kind))
    return absl::InvalidArgumentError("invalid ordered collection kind");
  const std::size_t framing =
      kind == OrderedCollectionKind::kSortedSet ? 12 : 4;
  if (item_bytes > kMaxStringBytes || max_bytes < framing ||
      encoded_bytes > max_bytes - framing ||
      item_bytes > max_bytes - encoded_bytes - framing) {
    return absl::OutOfRangeError("ordered full-image exceeds encoding limits");
  }
  return encoded_bytes + framing + item_bytes;
}

absl::StatusOr<std::string> EncodeOrderedCompactValue(
    OrderedCollectionKind kind, std::span<const OrderedCollectionEntry> entries,
    std::uint32_t version) {
  if (version != 1 && version != 2)
    return absl::InvalidArgumentError("unsupported ordered encoding version");
  if (kind == OrderedCollectionKind::kStream)
    return EncodeStreamRecords(entries);
  if (kind == OrderedCollectionKind::kString) {
    std::size_t size = 0;
    for (const auto& entry : entries) {
      if (entry.value_.size() > kMaxStringBytes - size)
        return absl::OutOfRangeError("String exceeds maximum length");
      size += entry.value_.size();
    }
    std::string result;
    result.reserve(size);
    for (const auto& entry : entries) result.append(entry.value_);
    return result;
  }
  if (!ValidKind(kind) || entries.empty() ||
      entries.size() > std::numeric_limits<std::uint32_t>::max()) {
    return absl::InvalidArgumentError("invalid ordered full-image kind/count");
  }
  const bool sorted = kind == OrderedCollectionKind::kSortedSet;
  if (version == 2) return EncodeIndexedCompactValue(kind, entries);
  std::string encoded;
  std::size_t size = 8;
  for (const auto& entry : entries) {
    if (std::isnan(entry.score_) ||
        (!sorted && std::bit_cast<std::uint64_t>(entry.score_) != 0)) {
      return absl::OutOfRangeError(
          "ordered full-image exceeds encoding limits");
    }
    auto next = AppendOrderedEntrySize(kind, size, entry.value_.size(),
                                       encoded.max_size());
    if (!next.ok()) return next.status();
    size = *next;
  }
  encoded.reserve(size);
  encoded.append(sorted ? kSortedSetMagicV1 : kListMagicV1);
  Put(encoded, entries.size(), 4);
  for (const auto& entry : entries) {
    if (sorted) Put(encoded, std::bit_cast<std::uint64_t>(entry.score_), 8);
    Put(encoded, entry.value_.size(), 4);
    encoded.append(entry.value_);
  }
  return encoded;
}

absl::StatusOr<std::vector<OrderedCollectionEntry>> DecodeOrderedCompactValue(
    OrderedCollectionKind kind, std::string_view encoded,
    std::uint64_t expected_count) {
  if (kind == OrderedCollectionKind::kStream)
    return DecodeStreamRecords(encoded, expected_count);
  if (encoded.starts_with(kind == OrderedCollectionKind::kSortedSet
                              ? kSortedSetMagicV2
                              : kListMagicV2)) {
    auto reader = OrderedCompactReader::Open(kind, encoded, expected_count);
    if (!reader.ok()) return reader.status();
    std::vector<OrderedCollectionEntry> result;
    result.reserve(expected_count);
    for (std::size_t i = 0; i < expected_count; ++i) {
      auto entry = reader->Next();
      if (!entry.ok()) return entry.status();
      result.push_back({std::string(entry->value_), entry->score_});
    }
    return result;
  }
  if (!ValidKind(kind) || expected_count == 0 ||
      expected_count > std::numeric_limits<std::uint32_t>::max() ||
      encoded.size() < 8) {
    return absl::DataLossError("invalid ordered full-image count/size");
  }
  const bool sorted = kind == OrderedCollectionKind::kSortedSet;
  const std::size_t framing = sorted ? 12 : 4;
  if (!encoded.starts_with(sorted ? kSortedSetMagicV1 : kListMagicV1) ||
      Get(encoded, 4, 4) != expected_count ||
      expected_count > (encoded.size() - 8) / framing) {
    return absl::DataLossError("invalid ordered full-image framing");
  }
  std::vector<OrderedCollectionEntry> result;
  result.reserve(expected_count);
  std::size_t offset = 8;
  for (std::uint64_t i = 0; i < expected_count; ++i) {
    if (framing > encoded.size() - offset) {
      return absl::DataLossError("truncated ordered full-image entry");
    }
    const auto score =
        sorted ? std::bit_cast<double>(Get(encoded, offset, 8)) : 0;
    if (sorted) offset += 8;
    const auto size = Get(encoded, offset, 4);
    offset += 4;
    if (std::isnan(score) || size > kMaxStringBytes ||
        size > encoded.size() - offset) {
      return absl::DataLossError("invalid ordered full-image entry");
    }
    result.push_back(
        {.value_ = std::string(encoded.substr(offset, size)), .score_ = score});
    offset += size;
  }
  if (offset != encoded.size()) {
    return absl::DataLossError("trailing ordered full-image bytes");
  }
  return result;
}

}  // namespace lavik::storage
