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

#include <bit>

#include "lavik/storage/detail/grouped/collection.h"

namespace lavik::storage::test {

// Explicit old-format fixtures keep byte-corruption tests independent of the
// current writer version. Identity/envelope validation uses the real encoder.
inline absl::StatusOr<std::string> EncodeOrderedGroupV1(
    const OrderedGroupSnapshot& page) {
  auto encoded = EncodeOrderedGroup(page);
  if (!encoded.ok() || !HasOrderedIndex(page.kind_) || page.retired_)
    return encoded;
  std::string bytes = encoded->substr(0, kOrderedGroupHeaderBytes);
  auto put = [&](std::uint64_t value, unsigned width) {
    for (unsigned i = 0; i < width; ++i)
      bytes.push_back(static_cast<char>(value >> (i * 8)));
  };
  for (const auto& entry : page.entries_) {
    put(entry.value_.size(), 4);
    put(std::bit_cast<std::uint64_t>(entry.score_), 8);
    bytes.append(entry.value_);
  }
  bytes[8] = 1;
  for (unsigned i = 0; i < 4; ++i)
    bytes[52 + i] = static_cast<char>(bytes.size() >> (i * 8));
  return bytes;
}

}  // namespace lavik::storage::test
