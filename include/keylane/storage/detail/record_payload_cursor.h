#pragma once

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <optional>
#include <span>
#include <string_view>

#include "absl/status/status.h"

namespace keylane::storage {

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

}  // namespace keylane::storage
