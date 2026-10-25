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

#include "lavik/storage/detail/grouped/collection.h"

namespace lavik::storage {

// Shared v2 body for compact Lists/Sorted Sets and their ordered pages:
// concatenated values, N little-endian doubles for Sorted Sets only, then
// N+1 little-endian u32 offsets into the values (including an end sentinel).
// Empty values have equal adjacent offsets. Compact and grouped envelopes
// carry their own type/count/version; logical streams can still encode v1.
//
// Open checks the array envelope in O(1). At checks the selected interval;
// bounds searches assume the stored ordering established by writers/imports.
// Strict decoders visit every entry and validate ordering/uniqueness. Recovery
// streams all offsets and scores without retaining member bytes. Ordinary
// reads retain physical identity/CRC checks but do not repeat a full-page walk.
// Every view borrows the immutable source, whose lease must outlive this
// reader.
class IndexedOrderedReader {
 public:
  static absl::StatusOr<IndexedOrderedReader> Open(OrderedCollectionKind kind,
                                                   std::string_view body,
                                                   std::size_t count);
  std::size_t size() const noexcept { return count_; }
  std::string_view data() const noexcept {
    return body_.substr(0, data_bytes_);
  }
  absl::StatusOr<OrderedCollectionEntryView> At(std::size_t index) const;
  absl::StatusOr<double> Score(std::size_t index) const;
  // exclusive=false returns the first score >= value; true returns > value.
  absl::StatusOr<std::size_t> LowerBoundScore(double value,
                                              bool exclusive = false) const;
  absl::StatusOr<std::size_t> LowerBound(double score,
                                         std::string_view member) const;
  // Copy only the requested values and their slice descriptors; no read lease
  // escapes. The caller admits output before invoking this bounded projection.
  absl::StatusOr<PackedValues> CopyRange(std::size_t first,
                                         std::size_t count) const;

 private:
  IndexedOrderedReader(std::string_view body, std::size_t count,
                       std::size_t data_bytes, std::size_t offsets, bool sorted)
      : body_(body),
        count_(count),
        data_bytes_(data_bytes),
        offsets_(offsets),
        sorted_(sorted) {}
  std::string_view body_;
  std::size_t count_;
  std::size_t data_bytes_;
  std::size_t offsets_;
  bool sorted_;
};

// Encode only the shared body. The caller supplies its envelope and owns the
// ordering/uniqueness contract; individual sizes and numeric scores are
// checked.
absl::StatusOr<std::string> EncodeIndexedOrderedEntries(
    OrderedCollectionKind kind,
    std::span<const OrderedCollectionEntry> entries);
absl::StatusOr<std::string> EncodeIndexedOrderedEntries(
    OrderedCollectionKind kind,
    std::span<const OrderedCollectionEntryView> entries);

}  // namespace lavik::storage
