#pragma once

#include "keylane/storage/detail/grouped_collection.h"

namespace keylane::storage {

// Logical full-image bridge used by existing callback/RDB integrations.
// This is not the durable grouped representation: aggregate framing has its
// own caller-supplied limit, while each member still obeys the Redis limit.
absl::StatusOr<std::string> EncodeOrderedCompactValue(
    OrderedCollectionKind kind, std::span<const OrderedCollectionEntry> entries,
    std::size_t max_bytes = kMaxRecordPayloadBytes);
absl::StatusOr<std::vector<OrderedCollectionEntry>> DecodeOrderedCompactValue(
    OrderedCollectionKind kind, std::string_view encoded,
    std::uint64_t expected_count,
    std::size_t max_bytes = kMaxRecordPayloadBytes);

}  // namespace keylane::storage
