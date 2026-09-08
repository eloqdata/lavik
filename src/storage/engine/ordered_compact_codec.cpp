#include "keylane/storage/detail/ordered_compact_codec.h"

#include <bit>
#include <cmath>
#include <limits>

namespace keylane::storage {
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

absl::StatusOr<std::string> EncodeOrderedCompactValue(
    OrderedCollectionKind kind, std::span<const OrderedCollectionEntry> entries,
    std::size_t max_bytes) {
  if (!ValidKind(kind) || entries.empty() ||
      entries.size() > std::numeric_limits<std::uint32_t>::max() ||
      max_bytes < 8 || max_bytes > kMaxRecordPayloadBytes) {
    return absl::InvalidArgumentError("invalid ordered full-image count/limit");
  }
  const bool sorted = kind == OrderedCollectionKind::kSortedSet;
  const std::size_t framing = sorted ? 12 : 4;
  std::size_t size = 8;
  for (const auto& entry : entries) {
    if (entry.value_.size() > kMaxStringBytes || std::isnan(entry.score_) ||
        (!sorted && std::bit_cast<std::uint64_t>(entry.score_) != 0) ||
        framing > max_bytes - size ||
        entry.value_.size() > max_bytes - size - framing) {
      return absl::OutOfRangeError(
          "ordered full-image exceeds encoding limits");
    }
    size += framing + entry.value_.size();
  }
  std::string encoded;
  encoded.reserve(size);
  encoded.append(sorted ? "KZS1" : "KLL1");
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
    std::uint64_t expected_count, std::size_t max_bytes) {
  if (!ValidKind(kind) || expected_count == 0 ||
      expected_count > std::numeric_limits<std::uint32_t>::max() ||
      encoded.size() < 8 || encoded.size() > max_bytes ||
      max_bytes > kMaxRecordPayloadBytes) {
    return absl::DataLossError("invalid ordered full-image count/size");
  }
  const bool sorted = kind == OrderedCollectionKind::kSortedSet;
  const std::size_t framing = sorted ? 12 : 4;
  if (!encoded.starts_with(sorted ? "KZS1" : "KLL1") ||
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

}  // namespace keylane::storage
