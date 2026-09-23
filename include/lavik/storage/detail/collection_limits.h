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

namespace lavik::storage {

// All collection kinds share the same size policy. Promotion leaves room for
// roughly two target-sized groups. Indivisible entries and hash collisions may
// exceed the group target; this is not a hard record or allocation limit.
inline constexpr std::size_t kCollectionGroupTargetBytes = 8 * 1024;
inline constexpr std::size_t kCollectionPromotionBytes = 16 * 1024;

// Bound the input to compact callback work independently of storage layout.
// The byte and entry bounds cap decoding overhead; they are not the resulting
// allocation size. Raising promotion size must not widen the small-inline
// cleanup allowance when the database is already above maxmemory.
inline constexpr std::size_t kCompactWorkspaceInputBytes = 16 * 1024;
inline constexpr std::size_t kCompactWorkspaceInputEntries = 1024;

}  // namespace lavik::storage
