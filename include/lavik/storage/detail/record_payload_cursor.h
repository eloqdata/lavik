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

// Consumes a preflighted encoder into an exact, caller-owned destination.
// Next() must not throw; borrowed framing is copied before advancing. A
// malformed producer can partially modify output, so the caller must withhold
// publication of the entire destination until this succeeds.
template <typename Encoder>
absl::Status WriteInlineRecordPayload(Encoder& encoder,
                                      std::span<std::byte> output) {
  static_assert(noexcept(encoder.Next()));
  if (output.size() != encoder.encoded_bytes())
    return absl::DataLossError("inline payload destination length mismatch");
  std::size_t offset = 0;
  while (auto part = encoder.Next()) {
    if (part->size() > output.size() - offset)
      return absl::DataLossError("inline payload encoder length mismatch");
    if (!part->empty())
      std::memcpy(output.data() + offset, part->data(), part->size());
    offset += part->size();
  }
  return offset == output.size()
             ? absl::OkStatus()
             : absl::DataLossError("inline payload encoder length mismatch");
}

// Borrows an unstarted, preflighted encoder and its immutable source through
// the writer's allocation waits; both must stay alive and unmoved. WriteTo
// consumes it exactly once, without suspension or allocation on success. Type
// erasure happens once per payload, rather than once per framing/value
// fragment. No byte is published here.
class InlineRecordPayload {
 public:
  template <typename Encoder>
  explicit InlineRecordPayload(Encoder& encoder) noexcept
      : source_(&encoder),
        write_([](void* source, std::span<std::byte> output) {
          return WriteInlineRecordPayload(*static_cast<Encoder*>(source),
                                          output);
        }),
        bytes_(encoder.encoded_bytes()) {}

  std::size_t encoded_bytes() const noexcept { return bytes_; }
  absl::Status WriteTo(std::span<std::byte> output) const {
    return write_(source_, output);
  }

 private:
  void* source_;
  absl::Status (*write_)(void*, std::span<std::byte>);
  std::size_t bytes_;
};

// Owning convenience form for callers that need contiguous encoded bytes.
// Storage appends use InlineRecordPayload to avoid this temporary allocation.
template <typename Encoder>
absl::StatusOr<std::string> EncodeInlineRecordPayload(Encoder& encoder) {
  const auto bytes = encoder.encoded_bytes();
  std::string payload;
  absl::Status status;
  payload.resize_and_overwrite(bytes, [&](char* output, std::size_t) noexcept {
    status = WriteInlineRecordPayload(
        encoder, std::as_writable_bytes(std::span(output, bytes)));
    return status.ok() ? bytes : 0;
  });
  if (!status.ok()) return status;
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
