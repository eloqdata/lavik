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

#include "log_block.h"

#include <algorithm>
#include <cassert>
#include <limits>
#include <utility>

#include "lavik/memory.h"

namespace lavik::detail {
namespace {
constexpr std::size_t kMinimumRecordBytes = 16;
constexpr std::size_t kSparseStride = 64;
std::size_t SparseCount(std::size_t capacity) {
  return (capacity / kMinimumRecordBytes + kSparseStride - 1) / kSparseStride;
}
}  // namespace

ReplicationLogBlock::ReplicationLogBlock()
    : bytes_(nullptr, ByteDeleter{storage::RetainedAllocationDomain{
                          .externally_admitted_ = true}}),
      sparse_offsets_(storage::RetainedAllocator<SparseOffset>(
          storage::RetainedAllocationDomain{.externally_admitted_ = true})) {}

void ReplicationLogBlock::ByteDeleter::operator()(std::byte* pointer) noexcept {
  storage::DeallocateRetainedBytes(domain_, pointer, alignof(std::max_align_t));
}

std::size_t ReplicationLogBlock::AllocationBytes(std::size_t capacity) {
  if (capacity < kMinimumRecordBytes ||
      capacity > std::numeric_limits<std::uint32_t>::max())
    return std::numeric_limits<std::size_t>::max();
  const auto bytes = AllocatorUsableSizeForRequest(capacity);
  const auto index = AllocatorUsableSizeForRequest(SparseCount(capacity) *
                                                   sizeof(SparseOffset));
  if (index > std::numeric_limits<std::size_t>::max() - bytes)
    return std::numeric_limits<std::size_t>::max();
  return bytes + index;
}

absl::StatusOr<ReplicationLogBlock> ReplicationLogBlock::Allocate(
    std::size_t capacity) {
  const auto bytes = AllocationBytes(capacity);
  if (bytes == std::numeric_limits<std::size_t>::max())
    return absl::ResourceExhaustedError("invalid replication block capacity");
  auto reservation = TryReserveMemory(bytes);
  if (!reservation)
    return absl::ResourceExhaustedError(
        "maxmemory cannot allocate a replication block");
  ReplicationLogBlock block;
  block.sparse_offsets_.reserve(SparseCount(capacity));
  block.bytes_.reset(static_cast<std::byte*>(
      storage::TryAllocateRetainedBytes(block.bytes_.get_deleter().domain_,
                                        capacity, alignof(std::max_align_t))));
  block.capacity_ = capacity;
  reservation->Release();
  return block;
}

std::span<std::byte> ReplicationLogBlock::AppendBuffer(std::size_t bytes) {
  assert(!sealed_ && bytes >= kMinimumRecordBytes &&
         bytes <= capacity_ - committed_bytes_);
  return {bytes_.get() + committed_bytes_, bytes};
}

void ReplicationLogBlock::CommitAppend(std::uint64_t lsn,
                                       std::uint32_t fragment,
                                       std::size_t bytes) {
  assert(lsn != 0 && lsn >= last_lsn_);
  (void)AppendBuffer(bytes);
  if (frame_count_ % kSparseStride == 0) {
    assert(sparse_offsets_.size() < sparse_offsets_.capacity());
    sparse_offsets_.push_back({lsn, fragment, committed_bytes_});
  }
  if (frame_count_ == 0) first_lsn_ = lsn;
  last_lsn_ = lsn;
  committed_bytes_ += static_cast<std::uint32_t>(bytes);
  ++frame_count_;
}

std::uint32_t ReplicationLogBlock::FindOffset(std::uint64_t lsn,
                                              std::uint32_t fragment) const {
  const auto found = std::upper_bound(
      sparse_offsets_.begin(), sparse_offsets_.end(), std::pair{lsn, fragment},
      [](const auto& key, const SparseOffset& entry) {
        return key < std::pair{entry.lsn_, entry.fragment_index_};
      });
  return found == sparse_offsets_.begin() ? 0 : std::prev(found)->byte_offset_;
}

void ReplicationLogBlock::Reset() {
  first_lsn_ = last_lsn_ = 0;
  committed_bytes_ = frame_count_ = 0;
  sealed_ = false;
  sparse_offsets_.clear();
}
}  // namespace lavik::detail
