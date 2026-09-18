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
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/statusor.h"

namespace lavik {

// Original canonical bytes for one complete flow-local event. A retained
// logical effect contains every transaction participant or control marker.
struct NativeHistoryRecord {
  unsigned flow_id_ = 0;
  std::uint64_t lsn_ = 0;
  std::string canonical_;
};

struct NativeHistoryRange {
  std::uint64_t first_lsn_ = 0;
  std::uint64_t end_lsn_ = 0;  // exclusive
  bool operator==(const NativeHistoryRange&) const = default;
};

struct NativeHistoryChunk {
  std::uint64_t total_bytes_ = 0;
  std::string bytes_;
};

// A copy-out manifest names the complete logical effect containing an event.
// It does not pin payloads; all records must still be fetched and validated.
struct NativeHistoryRecordInfo {
  unsigned flow_id_ = 0;
  std::uint64_t lsn_ = 0;
  std::uint64_t bytes_ = 0;
  bool operator==(const NativeHistoryRecordInfo&) const = default;
};

// One worker's history quota for published primary blocks and optional
// canonical secondary effects, stored with the source backlog's block storage.
// The quota counts allocated block capacity; indexes also enter process memory
// accounting. Primary publication reclaims secondary
// entries synchronously and never waits for a secondary reader. Metadata and
// payload allocations also participate in ordinary retained-memory accounting.
// All access, including PrimaryCharge release, belongs to the owning worker.
// Cross-worker callers must submit work to that owner and receive copies. A
// complete effect stays on its apply worker even when it spans several flows.
class ReplicationHistory {
  struct Impl;

 public:
  // Move-only ownership of published primary bytes. Storage's existing per-flow
  // backlog limits govern primary admission; this charge evicts secondary data
  // before the block becomes visible. A shrink may grandfather pinned primary
  // blocks until existing ACK/revocation rules permit their release.
  class PrimaryCharge {
   public:
    PrimaryCharge() = default;
    PrimaryCharge(const PrimaryCharge&) = delete;
    PrimaryCharge& operator=(const PrimaryCharge&) = delete;
    PrimaryCharge(PrimaryCharge&& other) noexcept;
    PrimaryCharge& operator=(PrimaryCharge&& other) noexcept;
    ~PrimaryCharge();
    void Reset() noexcept;

   private:
    friend class ReplicationHistory;
    PrimaryCharge(std::shared_ptr<Impl> impl, std::size_t bytes);
    std::shared_ptr<Impl> impl_;
    std::size_t bytes_ = 0;
  };

  explicit ReplicationHistory(std::size_t capacity_bytes);
  // Begins a boot-local retained lineage. It discards prior secondary entries,
  // preserving charges held by live primary blocks. No snapshot data is added.
  absl::Status Reset(std::string history_id, unsigned flow_count);
  void SetCapacity(std::size_t capacity_bytes);
  PrimaryCharge ChargePrimary(std::size_t bytes);
  // Secondary data must not starve separately accounted publisher staging or
  // standby allocation during promotion. Reclaims up to this many cache bytes.
  void ReclaimSecondary(std::size_t bytes);

  // Call only after the entire logical effect has applied successfully. An
  // admission failure loses optional coverage, never application progress.
  // Flow LSNs must increase on this worker (gaps are allowed). Duplicate, late
  // or invalid participant records reject the entire cache insertion. Complete
  // effects stay together in a block, so block eviction never splits an effect.
  bool TryRetain(std::string_view history_id,
                 std::vector<NativeHistoryRecord> records);
  // Single-flow completion uses the same block append without allocating a
  // temporary participant vector. The caller retains ownership of the bytes.
  bool TryRetainOne(std::string_view history_id,
                    const NativeHistoryRecord& record);
  // Local intervals may interleave with another worker's intervals for the
  // same origin flow. Export must merge them before applying its wire limit.
  std::vector<std::vector<NativeHistoryRange>> Coverage(
      std::string_view history_id, std::size_t max_ranges_per_flow = 8) const;
  absl::StatusOr<std::vector<NativeHistoryRecordInfo>> DescribeEffect(
      std::string_view history_id, unsigned flow_id, std::uint64_t lsn) const;
  // Copies at most 64 KiB. Returned bytes do not pin the cache: eviction
  // between chunks returns a gap and the receiver must discard the incomplete
  // event.
  absl::StatusOr<NativeHistoryChunk> Read(std::string_view history_id,
                                          unsigned flow_id, std::uint64_t lsn,
                                          std::size_t offset,
                                          std::size_t max_bytes) const;
  std::size_t primary_bytes() const;
  std::size_t secondary_bytes() const;

 private:
  bool RetainSorted(std::string_view history_id,
                    std::span<const NativeHistoryRecord> records);
  std::shared_ptr<Impl> impl_;
};

}  // namespace lavik
