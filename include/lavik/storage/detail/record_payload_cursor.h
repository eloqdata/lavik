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
#include <cstring>
#include <optional>
#include <span>
#include <string>
#include <string_view>

#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace lavik::storage {

// Consumes a preflighted encoder into one inline payload. Unlike extent reads,
// this has no partial-span state or type-erased calls. Every byte is
// overwritten before exposure, including when resize_and_overwrite supplies
// extra capacity. Next() must not throw and its borrowed span is consumed
// before advancing.
template <typename Encoder>
absl::StatusOr<std::string> EncodeInlineRecordPayload(Encoder& encoder) {
  static_assert(noexcept(encoder.Next()));
  const auto bytes = encoder.encoded_bytes();
  std::string payload;
  bool valid = true;
  payload.resize_and_overwrite(bytes, [&](char* output, std::size_t) noexcept {
    std::size_t offset = 0;
    while (auto part = encoder.Next()) {
      if (part->size() > bytes - offset) {
        valid = false;
        return std::size_t{0};
      }
      if (!part->empty())
        std::memcpy(output + offset, part->data(), part->size());
      offset += part->size();
    }
    valid = offset == bytes;
    return valid ? offset : 0;
  });
  if (!valid)
    return absl::DataLossError("inline payload encoder length mismatch");
  return payload;
}

// A non-owning, bounded-state bridge from a complete-value encoder to the
// extent writer. Encoders validate before construction and expose Next() and
// encoded_bytes(). Neither encoder nor its source may move while this cursor
// is live. No spans survive the next encoder call: even encoder-owned framing
// bytes are consumed before advancing. This keeps a 512 MiB element from
// requiring a second 512 MiB serialization buffer.
class RecordPayloadCursor {
 public:
  template <typename Encoder>
  explicit RecordPayloadCursor(Encoder& encoder,
                               std::string_view prefix = {}) noexcept
      : source_(&encoder),
        next_(
            [](void* source) { return static_cast<Encoder*>(source)->Next(); }),
        bytes_(encoder.encoded_bytes() + prefix.size()),
        pending_(prefix) {}

  std::size_t encoded_bytes() const noexcept { return bytes_; }

  // Fills exactly output.size() bytes. A malformed producer fails closed;
  // bytes already staged by this cursor must not be published as a root.
  absl::Status Read(std::span<std::byte> output) {
    if (failed_ || output.size() > bytes_ - consumed_) {
      failed_ = true;
      return absl::DataLossError("payload cursor length mismatch");
    }
    while (!output.empty()) {
      while (pending_.empty()) {
        auto span = next_(source_);
        if (!span.has_value()) {
          failed_ = true;
          return absl::DataLossError("truncated payload encoder");
        }
        pending_ = *span;
        if (pending_.size() > bytes_ - consumed_) {
          failed_ = true;
          return absl::DataLossError("oversized payload encoder span");
        }
      }
      const std::size_t copied = std::min(output.size(), pending_.size());
      std::memcpy(output.data(), pending_.data(), copied);
      output = output.subspan(copied);
      pending_.remove_prefix(copied);
      consumed_ += copied;
    }
    return absl::OkStatus();
  }

  // Must succeed before publishing a manifest. Detects both an advertised
  // length that is too large and trailing bytes beyond the declared length.
  absl::Status Finish() {
    if (failed_ || consumed_ != bytes_ || !pending_.empty()) {
      failed_ = true;
      return absl::DataLossError("unfinished payload encoder");
    }
    while (auto span = next_(source_)) {
      if (!span->empty()) {
        failed_ = true;
        return absl::DataLossError("trailing payload encoder bytes");
      }
    }
    return absl::OkStatus();
  }

 private:
  void* source_;
  std::optional<std::string_view> (*next_)(void*);
  std::size_t bytes_;
  std::size_t consumed_ = 0;
  std::string_view pending_;
  bool failed_ = false;
};

}  // namespace lavik::storage
