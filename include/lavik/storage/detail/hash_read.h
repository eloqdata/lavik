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
#include <span>
#include <string_view>

#include "absl/status/statusor.h"
#include "lavik/storage/detail/grouped/hash.h"

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

// One request position; repeated fields retain separate positions. The plan is
// sorted by group then field, while result_index preserves the command order.
// field borrows the request; value borrows only the currently loaded page.
struct HashFieldLookup {
  GroupedRecordId group_;
  std::string_view field_;
  std::size_t result_index_ = 0;
  std::optional<std::string_view> value_;
};

// Same checked-envelope contract as FindHashGroupField. Requests must belong
// to one group and be sorted by field; resets their values before scanning.
// Validates the entire encoding, including duplicate requested fields on disk.
absl::Status FindHashGroupFields(std::string_view payload,
                                 std::uint32_t field_count,
                                 std::span<HashFieldLookup> requests);

// Copies this page's matches to their distinct, empty slots in a pre-sized,
// charged result. Admits all copies (including repeated operands) before any
// allocation, and grows the existing charge. Discard result on failure.
absl::Status RetainHashGroupValues(HashResult& result,
                                   std::span<const HashFieldLookup> requests);

// Copies one validated lookup value (or a missing-field slot) into a result
// with no vector storage or retained charge. Reserves output memory before
// allocation and retains its charge. On failure, the caller discards result.
// The input view need only live until this synchronous call returns. Allocation
// exceptions propagate to the command coroutine's preparation-error handler.
absl::Status RetainHashLookupValue(HashResult& result,
                                   std::optional<std::string_view> matched);

}  // namespace lavik::storage
