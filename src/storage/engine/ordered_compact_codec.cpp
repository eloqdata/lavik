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

namespace {
template <typename Entry>
absl::StatusOr<std::string> EncodeIndexedEntries(
    OrderedCollectionKind kind, std::span<const Entry> entries) {
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
  output.reserve(size);
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

absl::StatusOr<std::string> EncodeIndexedOrderedEntries(
    OrderedCollectionKind kind,
    std::span<const OrderedCollectionEntry> entries) {
  return EncodeIndexedEntries(kind, entries);
}

absl::StatusOr<std::string> EncodeIndexedOrderedEntries(
    OrderedCollectionKind kind,
    std::span<const OrderedCollectionEntryView> entries) {
  return EncodeIndexedEntries(kind, entries);
}

absl::StatusOr<OrderedCompactReader> OrderedCompactReader::Open(
    OrderedCollectionKind kind, std::string_view encoded,
    std::uint64_t expected_count) {
  const bool sorted = kind == OrderedCollectionKind::kSortedSet;
  if (ValidKind(kind) && expected_count != 0 && encoded.size() >= 8 &&
      encoded.starts_with(sorted ? "LZS2" : "LVL2") &&
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
      encoded.size() < 8 || !encoded.starts_with(sorted ? "LZS1" : "LVL1") ||
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
  if (version == 2) {
    auto body = EncodeIndexedOrderedEntries(kind, entries);
    if (!body.ok()) return body.status();
    std::string encoded;
    encoded.reserve(8 + body->size());
    encoded.append(sorted ? "LZS2" : "LVL2");
    Put(encoded, entries.size(), 4);
    encoded.append(*body);
    return encoded;
  }
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
  encoded.append(sorted ? "LZS1" : "LVL1");
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
  if (encoded.starts_with(kind == OrderedCollectionKind::kSortedSet ? "LZS2"
                                                                    : "LVL2")) {
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
  if (!encoded.starts_with(sorted ? "LZS1" : "LVL1") ||
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
