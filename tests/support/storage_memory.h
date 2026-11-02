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

#include <cstdint>
#include <exception>
#include <string>

#include "lavik/memory.h"
#include "lavik/storage/buffer_pool.h"
#include "lavik/storage/detail/record_index.h"
#include "lavik/storage/format.h"
#include "mimalloc.h"

namespace lavik::test {

// Preserve the working allowance of storage admission tests while accounting
// for fixed primary-map objects and I/O pools, including three maintenance
// readers and arena metadata per worker, one process-wide export workspace and
// one device's metadata workspace. Index page directories remain part of the
// working allowance, as do the collection directories and entry pages.
// The returned value is the actual --max-memory; keep client quotas explicit
// when they are part of the scenario.
inline std::string WithStorageBaseline(std::uint64_t working_limit,
                                       unsigned workers) {
  const storage::RegisteredBufferPoolOptions buffers;
  const auto read_bytes =
      buffers.registered_bytes_ -
      buffers.storage_write_buffer_count_ * buffers.write_buffer_bytes_;
  const auto arena_metadata =
      read_bytes / buffers.alignment_ * sizeof(TlsfRangeAllocator::Metadata) +
      sizeof(TlsfRangeAllocator);
  auto fixed_bytes =
      workers * (buffers.registered_bytes_ + 3 * storage::kStorageBlockBytes +
                 arena_metadata) +
      storage::kStorageBlockBytes + 2 * storage::kDirectIoAlignment;
  for (unsigned worker = 0; worker < workers; ++worker) {
    const auto partitions =
        (storage::kLogicalStorageShards + workers - 1 - worker) / workers;
    const auto bytes = sizeof(storage::RecordIndex) * partitions *
                       storage::kLogicalDatabaseCount;
    // RetainedAllocator charges usable bytes, including alignment rounding.
    // mi_good_size alone underestimates large aligned allocations. Probe the
    // same untouched allocation instead of consuming the working allowance
    // with allocator overhead; no index objects or pages are constructed.
    void* indexes = mi_malloc_aligned(bytes, alignof(storage::RecordIndex));
    if (indexes == nullptr) std::terminate();
    fixed_bytes += mi_usable_size(indexes);
    mi_free(indexes);
  }
  return std::to_string(working_limit + (fixed_bytes * 10 + 8) / 9);
}

}  // namespace lavik::test
