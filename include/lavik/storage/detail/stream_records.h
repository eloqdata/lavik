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

#include "lavik/storage/detail/grouped_collection.h"

namespace lavik::storage {

// Stream pages contain sorted logical records. Keys distinguish the header,
// entries, logical macro nodes, groups, consumers and pending entries; neither
// a group nor a PEL is an indivisible storage item. The value after the key is
// its portable LXS1 fragment, followed by a uint32 little-endian key length.
// Page identities never enter that logical format.
absl::StatusOr<std::vector<OrderedCollectionEntry>> DecodeStreamRecords(
    std::string_view encoded, std::uint64_t expected_count);

// Returns the portable fragment after checking the internal record envelope.
// The returned view borrows the page entry and expires with it.
absl::StatusOr<std::string_view> StreamRecordPayload(std::string_view record);

// Returns a group header fragment only if its declared name and fixed fields
// exactly fill the payload. The caller identifies the header by its routing
// key; the borrowed view is safe to read at offsets derived from the name size.
absl::StatusOr<std::string_view> StreamGroupHeaderPayload(
    std::string_view record);

// Extracts the exact binary routing key from an internal record.
absl::StatusOr<std::string_view> StreamRecordKey(std::string_view record);

struct StreamRecordChange {
  std::uint64_t page_id_ = 0;
  std::string key_;
  std::optional<std::string> record_;  // nullopt removes this key.
};

// Applies sparse, already-routed record changes to complete affected pages.
// Supply affected pages and immediate neighbours from one immutable directory;
// the returned root, replacements and retirements require atomic publication.
// No unrelated record payload is materialized by this planner.
absl::StatusOr<OrderedCollectionMutationPlan> PlanStreamRecordChanges(
    const OrderedGroupDirectory& directory,
    std::vector<LoadedOrderedGroup> loaded,
    std::vector<StreamRecordChange> changes, std::uint64_t stream_length,
    std::span<const std::uint64_t> retired_pages = {});

// Checks a complete, ordered Stream record sequence in bounded space before
// ingestion commits. Only the current group prefix and last key are retained;
// callers admit RetainedBytes() independently of consumed input pages.
class StreamRecordValidator {
 public:
  explicit StreamRecordValidator(std::uint64_t length) : length_(length) {}
  // Consumes one record; a validation failure permanently invalidates this
  // sequence. The caller admits temporary key copies before calling Read.
  absl::Status Read(std::string_view record);
  // Checks EOF against all declared message, node, group and PEL counts.
  absl::Status Finish() const;
  std::size_t RetainedBytes() const noexcept {
    return previous_.capacity() + prefix_.capacity() + 2;
  }

 private:
  std::uint64_t length_, entries_ = 0, nodes_ = 0, node_entries_ = 0;
  std::uint64_t groups_ = 0;
  std::uint32_t expected_nodes_ = 0, expected_groups_ = 0;
  std::uint32_t consumers_ = 0, pending_ = 0;
  unsigned phase_ = 0;
  bool failed_ = false;
  std::string previous_, prefix_;
};

// Materializes a portable Stream for legacy consumers. Streaming consumers
// should emit StreamRecordPayload for each ordered record instead.
absl::StatusOr<std::string> EncodeStreamRecords(
    std::span<const OrderedCollectionEntry> records);

}  // namespace lavik::storage
