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

#include <algorithm>
#include <cstddef>

namespace lavik::storage {

// Bound uninterrupted page work during unlocked Sorted Set preparation without
// scheduling a continuation for every small point-write page. Scratch estimates
// include entry overhead as well as bytes, so many tiny members count as work.
// A single oversized page remains indivisible; its charge forces a scheduling
// opportunity before decoding it. This is local scheduling state, not
// admission.
class SortedSetPageWorkBudget {
 public:
  bool Add(std::size_t bytes) noexcept {
    bytes_ += std::min(bytes, kBytes);
    if (++pages_ < kPages && bytes_ < kBytes) return false;
    bytes_ = 0;
    pages_ = 0;
    return true;
  }

 private:
  static constexpr std::size_t kBytes = 256 * 1024;
  static constexpr std::size_t kPages = 32;
  std::size_t bytes_ = 0;
  std::size_t pages_ = 0;
};

}  // namespace lavik::storage
