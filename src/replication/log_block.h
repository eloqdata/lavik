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
#include <vector>

#include "absl/status/statusor.h"
#include "lavik/storage/scan_hash_map.h"

namespace lavik::detail {

// Owner-worker storage shared by the source backlog and retained replica
// effects. Callers supply record framing and retention policy; this owns byte
// allocation, append publication and sparse cursor lookup. All records have
// at least 16 bytes and monotonically increasing (LSN, fragment) keys. Replica
// effects use an internal sequence here, never a replacement for origin LSNs.
// Reset reuses the allocation only after the caller has withdrawn coverage.
class ReplicationLogBlock {
 public:
  struct SparseOffset {
    std::uint64_t lsn_ = 0;
    std::uint32_t fragment_index_ = 0;
    std::uint32_t byte_offset_ = 0;
  };

  ReplicationLogBlock();
  ReplicationLogBlock(ReplicationLogBlock&&) noexcept = default;
  ReplicationLogBlock& operator=(ReplicationLogBlock&&) noexcept = default;

  // One admission covers both bytes and the maximum sparse-index allocation.
  // Capacity is charged separately by the caller's history quota. Metadata
  // and bytes both participate in owner-local process memory accounting.
  static absl::StatusOr<ReplicationLogBlock> Allocate(std::size_t capacity);
  static std::size_t AllocationBytes(std::size_t capacity);

  std::span<std::byte> AppendBuffer(std::size_t bytes);
  // Publishes bytes already filled through AppendBuffer, without allocating.
  void CommitAppend(std::uint64_t lsn, std::uint32_t fragment,
                    std::size_t bytes);
  // Returns a record boundary at or before the requested cursor. The caller
  // scans its framing from that boundary to the exact record (or a gap).
  std::uint32_t FindOffset(std::uint64_t lsn, std::uint32_t fragment = 0) const;
  void Reset();
  std::size_t capacity() const { return capacity_; }

  // Framing code accesses the committed prefix only. Memory never moves until
  // the block is destroyed; no returned view may outlive owner-local access.
  struct ByteDeleter {
    storage::RetainedAllocationDomain domain_;
    void operator()(std::byte* pointer) noexcept;
  };
  std::unique_ptr<std::byte, ByteDeleter> bytes_;
  std::uint64_t first_lsn_ = 0;
  std::uint64_t last_lsn_ = 0;
  std::uint32_t committed_bytes_ = 0;
  std::uint32_t frame_count_ = 0;
  bool sealed_ = false;

 private:
  using SparseOffsets =
      std::vector<SparseOffset, storage::RetainedAllocator<SparseOffset>>;
  std::size_t capacity_ = 0;
  SparseOffsets sparse_offsets_;
};

}  // namespace lavik::detail
