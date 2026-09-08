#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "keylane/memory.h"
#include "keylane/storage/detail/grouped_object_index.h"

namespace keylane::storage {

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
      const std::shared_ptr<const std::vector<ExtentRef>>& extents,
      std::size_t key_bytes) {
    return AddLayout(location.total_disk_bytes(), location.logical_size_,
                     location.external(), location.key_external(), extents,
                     key_bytes);
  }

  // A retained immutable view owns these size fields, not the allocation
  // lifetime of its blocks. Budgeting must not materialize a physical owner
  // or epoch from a possibly retired block after another page's IO/Yield.
  // The page loader separately refreshes and verifies the logical version
  // before reading. GC relocation does not change the decoded page size.
  absl::Status AddGroup(
      const RecordIndexValue& value,
      const std::shared_ptr<const std::vector<ExtentRef>>& extents,
      std::size_t key_bytes) {
    return AddLayout(value.total_disk_bytes(), value.logical_size(),
                     value.external(), value.key_external(), extents,
                     key_bytes);
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
      bool key_external,
      const std::shared_ptr<const std::vector<ExtentRef>>& extents,
      std::size_t key_bytes) {
    if (external) {
      if (extents == nullptr)
        return absl::DataLossError(
            "grouped scratch extent manifest is missing");
      payload = 0;
      for (const auto& extent : *extents) {
        if (extent.payload_bytes_ > kMaxRecordPayloadBytes - payload)
          return absl::DataLossError("grouped scratch extent size overflow");
        payload += extent.payload_bytes_;
      }
      if (key_external) {
        if (key_bytes > payload)
          return absl::DataLossError("grouped scratch key exceeds payload");
        payload -= key_bytes;
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

}  // namespace keylane::storage
