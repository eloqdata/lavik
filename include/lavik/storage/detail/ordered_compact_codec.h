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

#include <optional>
#include <string_view>

#include "lavik/storage/detail/grouped/collection.h"
#include "lavik/storage/detail/ordered_index.h"

namespace lavik::storage {

// Compact signatures identify both collection kind and payload version.
// V1 also frames logical full-image streams; v2 carries the indexed body.
inline constexpr std::string_view kListMagicV1 = "LVL1";
inline constexpr std::string_view kListMagicV2 = "LVL2";
inline constexpr std::string_view kSortedSetMagicV1 = "LZS1";
inline constexpr std::string_view kSortedSetMagicV2 = "LZS2";

struct OrderedCompactEntryView {
  std::string_view value_;
  double score_ = 0;
  // V1 framing plus value; v2 has no per-entry framing, so this is value_.
  std::string_view encoded_;
};

// Checked traversal without per-element ownership. Consume exactly size()
// entries to validate the tail. Returned views borrow the immutable payload;
// copying a reader preserves its position for a second planning pass.
class OrderedCompactReader {
 public:
  static absl::StatusOr<OrderedCompactReader> Open(
      OrderedCollectionKind kind, std::string_view encoded,
      std::uint64_t expected_count);
  std::size_t size() const noexcept { return size_; }
  // Present for physical v2 images; point readers need not traverse unrelated
  // entries. Next remains the strict traversal API for full-image consumers.
  const std::optional<IndexedOrderedReader>& indexed() const noexcept {
    return indexed_;
  }
  absl::StatusOr<OrderedCompactEntryView> Next();

 private:
  OrderedCompactReader(std::string_view bytes, std::size_t count, bool sorted)
      : bytes_(bytes), size_(count), remaining_(count), sorted_(sorted) {}
  std::string_view bytes_;
  std::size_t size_;
  std::size_t remaining_;
  std::size_t offset_ = 8;
  bool sorted_;
  std::optional<IndexedOrderedReader> indexed_;
};

// Keeps the bounded-state v2 encoder inline without heap allocation. The
// encoder borrows source leases and operands; keep them alive through the
// consuming writer's append waits. An unselected payload emits no bytes.
class OrderedCompactPayload {
 public:
  IndexedOrderedSplice& V2(IndexedOrderedReader source) noexcept {
    return payload_.emplace(source);
  }
  std::size_t encoded_bytes() const noexcept {
    return payload_ ? payload_->encoded_bytes() : 0;
  }
  bool empty() const noexcept { return encoded_bytes() == 0; }
  std::optional<std::string_view> Next() noexcept {
    return payload_ ? payload_->Next() : std::nullopt;
  }

 private:
  std::optional<IndexedOrderedSplice> payload_;
};

// Logical full-image bridge used by existing callback/RDB integrations.
// A collection can span many physical pages, so aggregate bytes have no
// String or record-payload limit here. Each item remains limited to 512 MiB
// and the encoded count to uint32_t. Callers separately admit materialization
// memory and enforce physical-page or destination-buffer bounds. The default
// v1 is the logical wire bridge. Physical writers request v2, whose body uses
// u32 offsets and therefore requires a bounded (at most UINT32_MAX-byte) image.
absl::StatusOr<std::string> EncodeOrderedCompactValue(
    OrderedCollectionKind kind, std::span<const OrderedCollectionEntry> entries,
    std::uint32_t version = 1);
absl::StatusOr<std::vector<OrderedCollectionEntry>> DecodeOrderedCompactValue(
    OrderedCollectionKind kind, std::string_view encoded,
    std::uint64_t expected_count);

// Checks one item's logical framing and size without allocating its payload.
// All compact encoders share this check even when they own different runtime
// containers. max_bytes is the destination capacity, normally max_size() of
// the output string; failure leaves the caller's accumulated size unchanged.
absl::StatusOr<std::size_t> AppendOrderedEntrySize(OrderedCollectionKind kind,
                                                   std::size_t encoded_bytes,
                                                   std::size_t item_bytes,
                                                   std::size_t max_bytes);

}  // namespace lavik::storage
