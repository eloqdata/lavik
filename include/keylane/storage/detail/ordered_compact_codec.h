#pragma once

#include "keylane/storage/detail/grouped_collection.h"

namespace keylane::storage {

// Logical full-image bridge used by existing callback/RDB integrations.
// A collection can span many physical pages, so aggregate bytes have no
// String or record-payload limit here. Each item remains limited to 512 MiB
// and the encoded count to uint32_t. Callers separately admit materialization
// memory and enforce physical-page or destination-buffer bounds.
absl::StatusOr<std::string> EncodeOrderedCompactValue(
    OrderedCollectionKind kind, std::span<const OrderedCollectionEntry> entries);
absl::StatusOr<std::vector<OrderedCollectionEntry>> DecodeOrderedCompactValue(
    OrderedCollectionKind kind, std::string_view encoded,
    std::uint64_t expected_count);

// Checks one item's logical framing and size without allocating its payload.
// All compact encoders share this check even when they own different runtime
// containers. max_bytes is the destination capacity, normally max_size() of
// the output string; failure leaves the caller's accumulated size unchanged.
absl::StatusOr<std::size_t> AppendOrderedEntrySize(
    OrderedCollectionKind kind, std::size_t encoded_bytes,
    std::size_t item_bytes, std::size_t max_bytes);

}  // namespace keylane::storage
