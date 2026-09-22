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
    OrderedCollectionKind kind,
    std::span<const OrderedCollectionEntry> entries) {
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
