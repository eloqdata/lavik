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
#include <cstdint>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "lavik/memory.h"
#include "lavik/storage/detail/grouped_object_index.h"

namespace lavik::storage {

// Plans headroom before a foreground operation accumulates decoded pages.
// This is temporary admission, not another persistent object-index charge.
// Keep the returned reservation alive until all planned scratch is destroyed.
// Callers select the pages first: a point read must not reserve the unrelated
// value bytes of the entire collection. Page readers separately check the
// decoded count against this physical envelope before allocating entries.
class GroupedScratchBudget {
 public:
  absl::Status AddGroup(
      const RecordLocation& location,
      const std::shared_ptr<const std::vector<ExtentRef>>& extents) {
    return AddLayout(location.total_disk_bytes(),
                     location.value_type() == ValueType::kString
                         ? 1
                         : location.logical_size_,
                     location.external(), extents);
  }

  // A retained immutable view owns these size fields, not the allocation
  // lifetime of its blocks. Budgeting must not materialize a physical owner
  // or epoch from a possibly retired block after another page's IO/Yield.
  // The page loader separately refreshes and verifies the logical version
  // before reading. GC relocation does not change the decoded page size.
  absl::Status AddGroup(
      const RecordIndexValue& value,
      const std::shared_ptr<const std::vector<ExtentRef>>& extents) {
    return AddLayout(
        value.total_disk_bytes(),
        value.value_type() == ValueType::kString ? 1 : value.logical_size(),
        value.external(), extents);
  }

  // Includes caller-owned copies of incoming fields/items before making them.
  absl::Status AddBytes(std::size_t bytes) {
    if (bytes > std::numeric_limits<std::size_t>::max() - bytes_)
      return absl::ResourceExhaustedError("grouped scratch size overflow");
    bytes_ += bytes;
    return absl::OkStatus();
  }

  absl::StatusOr<MemoryReservation> Reserve(std::size_t copies) const {
    if (copies == 0 ||
        bytes_ > std::numeric_limits<std::size_t>::max() / copies)
      return absl::ResourceExhaustedError("grouped scratch peak size overflow");
    auto reservation = TryReserveMemory(bytes_ * copies);
    if (!reservation) {
      RecordMemoryRejection();
      return absl::ResourceExhaustedError(
          "OOM grouped operation scratch admission");
    }
    return std::move(*reservation);
  }

 private:
  absl::Status AddLayout(
      std::uint64_t payload, std::uint32_t count, bool external,
      const std::shared_ptr<const std::vector<ExtentRef>>& extents) {
    if (external) {
      if (extents == nullptr)
        return absl::DataLossError(
            "grouped scratch extent manifest is missing");
      // Extents contain only page bytes; parent keys live in headers or
      // separate KeyRecords and cannot reduce the decode reservation.
      payload = 0;
      for (const auto& extent : *extents) {
        if (extent.payload_bytes_ > kMaxRecordPayloadBytes - payload)
          return absl::DataLossError("grouped scratch extent size overflow");
        payload += extent.payload_bytes_;
      }
    }
    // Covers vector growth, SSO strings, decoder duplicate validation and
    // page metadata. String bytes themselves are bounded by the payload.
    constexpr std::size_t kEntryOverhead = 256;
    constexpr auto limit = std::numeric_limits<std::size_t>::max();
    if (payload > limit || count > (limit - payload) / kEntryOverhead)
      return absl::ResourceExhaustedError(
          "grouped scratch entry size overflow");
    return AddBytes(payload + count * kEntryOverhead);
  }

  std::size_t bytes_ = 4096;
};

}  // namespace lavik::storage
