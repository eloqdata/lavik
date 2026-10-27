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

#include <array>

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
  friend class IndexedOrderedSplice;
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

// A preflighted v2 end/point edit. At most three surviving source intervals
// and one insertion describe the complete after-image. No per-item vector or
// numeric-array buffer is owned: Next borrows value/score spans and generates
// rebased offsets in fixed framing storage. Sources and operands must remain
// immutable and alive through consumption; the unstarted plan may move.
// Callers validate every source interval, operand size/score and output size
// before constructing this encoder. Encoded bytes are published only after
// the destination writer has consumed the complete plan successfully.
class IndexedOrderedSplice {
 public:
  // Grouped pages reuse the same body beneath their own envelope.
  explicit IndexedOrderedSplice(IndexedOrderedReader source,
                                bool compact = true) noexcept
      : source_(source), compact_(compact) {}
  // Plans one edit on a fully validated Sorted Set source. removed == size()
  // inserts an absent member; otherwise that slot must contain member. The
  // caller resolves member existence before invoking this method. The new
  // position is binary-searched without a second member scan or view array.
  // Call only on an unstarted, empty plan. A missing after score deletes.
  absl::Status EditSortedSetMember(std::size_t removed, std::string_view member,
                                   std::optional<double> after);
  void AppendSource(std::size_t first, std::size_t count) noexcept;
  // One insertion per plan: List operand views (optionally reversed), or a
  // Sorted Set member. Neither method copies operand bytes.
  void AppendValues(std::span<const std::string_view> values,
                    bool reverse) noexcept;
  void AppendMember(std::string_view member, double score) noexcept;
  std::size_t encoded_bytes() const noexcept {
    return (compact_ ? 12 : 4) + data_bytes_ +
           count_ * (source_.sorted_ ? 12 : 4);
  }
  std::optional<std::string_view> Next() noexcept;

 private:
  struct Part {
    std::size_t first_ = 0;
    std::size_t count_ = 0;
    bool inserted_ = false;
  };
  std::string_view SourceBytes(const Part& part) const noexcept;
  std::string_view Value(const Part& part, std::size_t index) const noexcept;
  void Add(Part part, std::size_t bytes) noexcept;
  IndexedOrderedReader source_;
  std::array<Part, 4> parts_{};
  std::span<const std::string_view> inputs_;
  OrderedCollectionEntryView member_{};
  bool reverse_inputs_ = false;
  bool has_inputs_ = false;
  bool inserted_ = false;
  bool compact_ = true;
  std::size_t parts_size_ = 0, count_ = 0, data_bytes_ = 0;
  std::size_t part_ = 0, item_ = 0, offset_ = 0;
  unsigned phase_ = 0;
  std::array<char, 8> framing_{};
};

}  // namespace lavik::storage
