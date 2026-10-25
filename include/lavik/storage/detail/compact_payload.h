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
#include <cassert>
#include <cstdint>

#include "absl/container/inlined_vector.h"
#include "lavik/storage/detail/record_payload_cursor.h"

namespace lavik::storage {

// A preflighted compact after-image: framing is owned, payload fragments borrow
// immutable read leases or command operands. Keep those owners alive through
// append's allocation waits. The plan may move before encoding, because owned
// framing uses no self-referential views. Encoding consumes it once, only after
// the writer's final suspension; it allocates and publishes nothing itself.
class CompactPayload {
 public:
  void AppendBytes(std::string_view bytes) {
    if (bytes.empty()) return;
    assert(bytes.size() <= SIZE_MAX - bytes_);
    bytes_ += bytes.size();
    parts_.push_back(Part{.borrowed = bytes});
  }

  // Encodes fixed-width little-endian framing, independent of host endianness.
  void AppendUnsigned(std::uint64_t value, unsigned width) {
    assert(width <= 8 && width > 0 && bytes_ <= SIZE_MAX - width);
    if (parts_.empty() || parts_.back().owned_size == 0 ||
        parts_.back().owned_size + width > parts_.back().owned.size())
      parts_.push_back(Part{});
    auto& part = parts_.back();
    for (unsigned i = 0; i < width; ++i)
      part.owned[part.owned_size++] = static_cast<char>(value >> (8 * i));
    bytes_ += width;
  }

  std::size_t encoded_bytes() const noexcept { return bytes_; }
  bool empty() const noexcept { return bytes_ == 0; }
  std::optional<std::string_view> Next() noexcept {
    if (next_ == parts_.size()) return std::nullopt;
    const auto& part = parts_[next_++];
    return part.owned_size
               ? std::string_view(part.owned.data(), part.owned_size)
               : part.borrowed;
  }

 private:
  struct Part {
    std::string_view borrowed{};
    std::array<char, 32> owned{};
    unsigned owned_size = 0;
  };
  absl::InlinedVector<Part, 8> parts_;
  std::size_t bytes_ = 0;
  std::size_t next_ = 0;
};

}  // namespace lavik::storage
