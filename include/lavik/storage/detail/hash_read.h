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
#include <optional>
#include <string_view>

#include "absl/status/statusor.h"

namespace lavik::storage {

struct HashResult;

// Scans a group whose envelope and physical identity the loader has verified;
// field_count must be that envelope's count, and field must route to this
// group. Validates all entry framing and rejects duplicate occurrences of
// field, but leaves unrelated duplicate/route validation to full decodes. A
// returned view borrows payload; nullopt means missing, including in an empty
// routing leaf.
absl::StatusOr<std::optional<std::string_view>> FindHashGroupField(
    std::string_view payload, std::uint32_t field_count,
    std::string_view field);

// Copies one validated lookup value (or a missing-field slot) into a result
// with no vector storage or retained charge. Reserves output memory before
// allocation and retains its charge. On failure, the caller discards result.
// The input view need only live until this synchronous call returns. Allocation
// exceptions propagate to the command coroutine's preparation-error handler.
absl::Status RetainHashLookupValue(HashResult& result,
                                   std::optional<std::string_view> matched);

}  // namespace lavik::storage
