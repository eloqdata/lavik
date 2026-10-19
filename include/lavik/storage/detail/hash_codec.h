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

#include <cstddef>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/statusor.h"
#include "lavik/storage/detail/compact_payload.h"
#include "lavik/storage/format.h"

namespace lavik::storage {

inline constexpr std::size_t kHashValueHeaderBytes = 32;

struct HashEntry {
  Digest digest_{};
  std::string field_;
  std::string value_;
};

struct HashValue {
  std::vector<HashEntry> entries_;
};

struct HashEntryView {
  std::string_view field_;
  std::string_view value_;
};

// Checked, allocation-free traversal of the compact encoding. The payload
// must remain alive and unchanged while the reader or returned views are used.
// Open validates the header; callers must consume size() successful Next calls
// to validate every entry and reject trailing bytes. Copying a reader preserves
// its position, allowing admission to precede construction of an owned result.
// Like the owned decoder, it bounds individual fields/values, not aggregate
// logical bytes; storage callers enforce their physical and memory envelopes.
class HashValueReader {
 public:
  static absl::StatusOr<HashValueReader> Open(std::string_view payload);
  std::size_t size() const { return size_; }
  // Failure does not advance the reader. Calling past the last entry fails.
  absl::StatusOr<HashEntryView> Next();

 private:
  HashValueReader(std::string_view payload, std::size_t count)
      : payload_(payload), size_(count), remaining_(count) {}
  std::string_view payload_;
  std::size_t size_;
  std::size_t remaining_;
  std::size_t offset_ = kHashValueHeaderBytes;
};

// The compact encoding contains complete field/value pairs, never a mutation
// log or a process-local digest. Fixed-width header fields and entry lengths
// are little-endian; decoding reconstructs lookup digests.
// A logical Hash can span many records: aggregate bytes have no String or
// record-payload limit here. Each field/value remains limited to 512 MiB and
// the encoded count to uint32_t. Storage callers enforce physical envelopes
// and admit materialization memory independently of this logical codec.
absl::StatusOr<HashValue> DecodeHashValue(std::string_view payload);
absl::StatusOr<std::string> EncodeHashValue(const HashValue& value);

enum class CompactHashEditKind { kSet, kDelete };

struct CompactHashEditPlan {
  bool changed_ = false;
  std::size_t added_ = 0;
  std::size_t removed_ = 0;
  std::size_t count_ = 0;
  CompactPayload payload_;
};

// Validates the complete old image and builds a bounded splice plan. Sources
// must remain immutable through encoding, including append allocation waits.
// Duplicate operands retain sequential command semantics. Delete accepts no
// values. Nullopt selects promotion without allocating an oversized image.
// Callers admit O(fields.size()) planning space before entering this helper.
absl::StatusOr<std::optional<CompactHashEditPlan>> PlanCompactHashEdit(
    std::string_view payload, std::size_t expected_count,
    std::span<const std::string_view> fields,
    std::span<const std::string_view> values, std::size_t max_bytes,
    CompactHashEditKind kind = CompactHashEditKind::kSet);

// A bounded HSET after-image. No-op results have no payload; a changed result
// owns its bytes independently of both the old payload and request operands.
struct CompactHashEdit {
  bool changed_ = false;
  std::size_t added_ = 0;
  std::size_t count_ = 0;
  std::string payload_;
};

// Edits an existing compact Hash without materializing its fields. Validates
// all framing and the expected count, including entries after a match. Input
// views must stay alive until return. Repeated operands retain sequential HSET
// semantics (last value wins, new fields counted once, transient changes count
// as writes). A nullopt result requests the owned promotion path when the final
// encoding exceeds max_bytes; it does not allocate that oversized after-image.
// Callers admit O(fields.size()) planning space plus the bounded output first.
absl::StatusOr<std::optional<CompactHashEdit>> EditCompactHash(
    std::string_view payload, std::size_t expected_count,
    std::span<const std::string_view> fields,
    std::span<const std::string_view> values, std::size_t max_bytes);

// Checked size accumulation without allocating the supplied field/value.
// Used before serialization; failure leaves the caller's running size intact.
// max_bytes is the destination's capacity bound: a physical group envelope or
// the output string's max_size() for a materialized logical Hash.
absl::StatusOr<std::size_t> AppendHashEntrySize(std::size_t encoded_bytes,
                                                std::size_t field_bytes,
                                                std::size_t value_bytes,
                                                std::size_t max_bytes);

}  // namespace lavik::storage
