#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

#include "absl/status/statusor.h"
#include "keylane/storage/collection_page.h"

namespace keylane::rdb {

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

  // Drain the initial header with Next() before installing the first page.
  // A page must have the next sequential cursor and the same collection type.
  // Empty prefix-routing pages are legal and still advance the cursor.
  absl::Status StartPage(const storage::CollectionPage& page);
  // nullopt means this header/page drained, not necessarily end-of-key. Empty
  // spans are legitimate empty keys, fields or members. Finish verifies EOF.
  std::optional<std::string_view> Next() noexcept;
  absl::Status Finish() const;

 private:
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
};

}  // namespace keylane::rdb
