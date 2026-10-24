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
// Hash payload versions are independent of the outer record/storage version.
inline constexpr std::uint32_t kLegacyHashValueVersion = 1;
inline constexpr std::uint32_t kHashValueVersion = 2;

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

// V2 uses a version-fixed SipHash-1-2 seed, independent of process lookup and
// grouped routing seeds. Tags are only filters; complete fields resolve every
// collision. Keeping the algorithm stable is part of the durable v2 contract.
std::uint8_t HashFieldFingerprint(std::string_view field) noexcept;

// A bounded-state encoder borrowing immutable entries. V1 is retained for
// logical streaming/compatibility. V2 stores data, u32 offsets (N+1), optional
// u32 field lengths (N), then byte fingerprints (N), without alignment padding.
// All-empty values omit field lengths, including Sets. Logical full images
// whose data exceeds u32 offsets retain v1; physical pages always fit v2.
class HashValueEncoder {
 public:
  static absl::StatusOr<HashValueEncoder> Create(
      std::span<const HashEntry> entries,
      std::uint32_t version = kHashValueVersion);
  static absl::StatusOr<HashValueEncoder> Create(
      std::span<const HashEntryView> entries,
      std::uint32_t version = kHashValueVersion);
  std::size_t encoded_bytes() const noexcept { return encoded_bytes_; }
  // Metadata expires on Next/move; borrowed data expires with the input.
  std::optional<std::string_view> Next() noexcept;
  bool borrowed() const noexcept { return borrowed_; }

 private:
  absl::Status Initialize(std::uint32_t version);
  [[gnu::noinline, gnu::cold]] std::optional<std::string_view>
  NextLegacy() noexcept;
  HashEntryView Entry(std::size_t index) const noexcept;
  std::span<const HashEntry> entries_;
  std::span<const HashEntryView> views_;
  std::array<char, kHashValueHeaderBytes> header_{};
  std::array<char, 256> metadata_{};
  std::size_t count_ = 0, encoded_bytes_ = 0, entry_ = 0, offset_ = 0;
  std::uint32_t version_ = kHashValueVersion;
  unsigned phase_ = 0;
  bool empty_values_ = true, borrowed_ = false;
};

// Checked, allocation-free traversal of either Hash payload version. The
// payload must remain alive and unchanged while the reader or returned views
// are used. Open validates the header; callers must consume size() successful
// Next calls to validate every entry and reject trailing bytes. Copying a
// reader preserves its position, allowing admission to precede construction of
// an owned result. Like the owned decoder, it bounds individual fields/values,
// not aggregate logical bytes; storage callers enforce their physical and
// memory envelopes.
class HashValueReader {
 public:
  static absl::StatusOr<HashValueReader> Open(std::string_view payload);
  std::size_t size() const { return size_; }
  std::uint32_t version() const noexcept { return version_; }
  // V2 random access validates the selected slot, without parsing other
  // entries.
  absl::StatusOr<HashEntryView> At(std::size_t index) const;
  // Published payloads have checked tags. SIMD scans only the contiguous tags;
  // scalar_tags provides the identical layout/algorithm for benchmarks/tests.
  // Search starts at first, independently of Next's cursor. On a hit, slot
  // receives the physical entry ordinal when supplied; it is untouched on a
  // miss or error. Writers use ordinals to apply edits without locating fields
  // again while rebuilding the page, and first to detect duplicate fields.
  absl::StatusOr<std::optional<HashEntryView>> Find(
      std::string_view field, bool scalar_tags = false,
      std::size_t* slot = nullptr, std::size_t first = 0) const;
  // Strict readers/writers validate the persisted filter before trusting it.
  // Ordinary point reads retain the loader's CRC policy and check candidates.
  absl::Status ValidateFingerprints() const;
  // Failure does not advance the reader. Calling past the last entry fails.
  absl::StatusOr<HashEntryView> Next();

 private:
  [[gnu::noinline, gnu::cold]] static absl::StatusOr<HashValueReader>
  OpenLegacy(std::string_view payload);
  [[gnu::noinline, gnu::cold]] absl::StatusOr<HashEntryView> NextLegacy();
  [[gnu::noinline, gnu::cold]]
  absl::StatusOr<std::optional<HashEntryView>> FindLegacy(
      std::string_view field, std::size_t* slot, std::size_t first) const;
  HashValueReader(std::string_view payload, std::size_t count)
      : payload_(payload), size_(count), remaining_(count) {}
  std::string_view payload_;
  std::size_t size_;
  std::size_t remaining_;
  std::size_t offset_ = kHashValueHeaderBytes;
  std::uint32_t version_ = kLegacyHashValueVersion;
  bool empty_values_ = false;
  std::size_t data_bytes_ = 0, offsets_begin_ = 0, lengths_begin_ = 0,
              tags_begin_ = 0;
};

// The compact encoding contains complete field/value pairs, never a mutation
// log or a process-local digest. Fixed-width header fields and entry lengths
// are little-endian; decoding reconstructs lookup digests.
// Logical full images above the v2 offset range use v1.
// A logical Hash can span many records: aggregate bytes have no String or
// record-payload limit here. Each field/value remains limited to 512 MiB and
// the encoded count to uint32_t. Storage callers enforce physical envelopes
// and admit materialization memory independently of this logical codec.
absl::StatusOr<HashValue> DecodeHashValue(std::string_view payload);
absl::StatusOr<std::string> EncodeHashValue(
    const HashValue& value, std::uint32_t version = kHashValueVersion);
absl::StatusOr<std::string> EncodeHashValueViews(
    std::span<const HashEntryView> entries,
    std::uint32_t version = kHashValueVersion);

enum class CompactHashEditKind { kSet, kDelete };

struct CompactHashEditPlan {
  bool changed_ = false;
  std::size_t added_ = 0;
  std::size_t removed_ = 0;
  std::size_t count_ = 0;
  CompactPayload payload_;
};

// Validates the complete old image and owns a bounded v2 after-image. Sources
// need only remain immutable during preparation; no source lease is retained.
// Duplicate operands retain sequential command semantics. Delete accepts no
// values. Nullopt selects promotion without allocating an oversized image.
// Callers admit O(fields.size()) planning space plus bounded output bytes.
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

// Checked v1 size accumulation, also used to bound individual fields/values.
// Failure leaves the caller's running size intact. Indexed payload sizing
// must use HashValueEncoder because v2 framing depends on all values.
absl::StatusOr<std::size_t> AppendHashEntrySize(std::size_t encoded_bytes,
                                                std::size_t field_bytes,
                                                std::size_t value_bytes,
                                                std::size_t max_bytes);

}  // namespace lavik::storage
