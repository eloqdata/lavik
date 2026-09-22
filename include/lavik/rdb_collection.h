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

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string_view>

#include "absl/status/statusor.h"
#include "lavik/storage/collection_page.h"

namespace lavik::rdb {

class StreamFileEncoder;

// Encodes one RDB file entry from independently loaded collection pages.
// The key and current page are borrowed until their Next() sequence drains.
// Metadata spans expire at the next Next() call; strings borrow the original
// page. Consumers may split a span for bounded output/backpressure without
// making a second serialized copy of a large member. Entries from different
// keys MUST NOT interleave at the file sink, even across worker boundaries.
class CollectionFileEncoder {
 public:
  static absl::StatusOr<CollectionFileEncoder> Create(
      std::uint8_t db_id, std::string_view key, storage::ValueType type,
      std::uint64_t item_count, std::uint64_t expire_at_ms);

  // Value-only encoding for DUMP; the caller appends the version/checksum.
  static absl::StatusOr<CollectionFileEncoder> CreateDump(
      storage::ValueType type, std::uint64_t item_count);

  // Drain the initial header with Next() before installing the first page.
  // A page must have the next sequential cursor and the same collection type.
  // Empty prefix-routing pages are legal and still advance the cursor.
  absl::Status StartPage(const storage::CollectionPage& page);
  // nullopt means this header/page drained, not necessarily end-of-key. Empty
  // spans are legitimate empty keys, fields or members. Finish verifies EOF.
  std::optional<std::string_view> Next() noexcept;
  absl::Status Finish() const;

 private:
  std::shared_ptr<StreamFileEncoder> stream_;
  std::string_view Length(std::uint64_t value) noexcept;
  std::array<char, 40> header_{};
  std::array<char, 9> metadata_{};
  std::size_t header_bytes_ = 0;
  std::string_view key_;
  storage::ValueType type_ = storage::ValueType::kNone;
  std::uint64_t expected_ = 0;
  std::uint64_t accepted_ = 0;
  std::uint64_t cursor_ = 0;
  const storage::CollectionPage* page_ = nullptr;
  std::size_t entry_ = 0;
  unsigned header_phase_ = 0;
  unsigned entry_phase_ = 0;
  bool saw_last_ = false;
  bool dump_ = false;
};

}  // namespace lavik::rdb
