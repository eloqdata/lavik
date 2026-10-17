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
#include <string>
#include <string_view>
#include <vector>

namespace lavik::storage {

// Immutable projected bytes that outlive page leases and key holds. Offsets
// survive block moves, including small-string storage. The enclosing result
// owns and transfers admission for the block and slice capacities.
struct PackedValues {
  struct Slice {
    std::size_t block_ = 0;
    std::size_t offset_ = 0;
    std::size_t size_ = 0;
  };
  std::vector<std::string> blocks_;
  std::vector<Slice> values_;

  // The returned view borrows this snapshot, which must remain immutable and
  // alive until the caller has consumed the selected bytes.
  std::string_view view(std::size_t index) const {
    const auto& slice = values_[index];
    return std::string_view(blocks_[slice.block_])
        .substr(slice.offset_, slice.size_);
  }
};

}  // namespace lavik::storage
