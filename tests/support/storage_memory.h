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
#include <string>

#include "lavik/storage/buffer_pool.h"
#include "lavik/storage/detail/record_index.h"
#include "lavik/storage/format.h"

namespace lavik::test {

// Preserve the working allowance of storage admission tests while accounting
// for fixed primary-map objects and I/O pools, including three maintenance
// readers per worker, one process-wide export workspace and one device's
// metadata workspace. Page directories are already included in working_limit.
// The returned value is the actual --max-memory; keep client quotas explicit
// when they are part of the scenario.
inline std::string WithStorageBaseline(std::uint64_t working_limit,
                                       unsigned workers) {
  const auto fixed_bytes =
      sizeof(storage::RecordIndex) * storage::kLogicalStorageShards *
          storage::kLogicalDatabaseCount +
      workers * (storage::RegisteredBufferPoolOptions{}.registered_bytes_ +
                 3 * storage::kStorageBlockBytes) +
      storage::kStorageBlockBytes + 2 * storage::kDirectIoAlignment;
  return std::to_string(working_limit + (fixed_bytes * 10 + 8) / 9);
}

}  // namespace lavik::test
