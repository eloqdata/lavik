#pragma once

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/statusor.h"
#include "keylane/storage/format.h"

namespace keylane::storage {

inline constexpr std::size_t kHashValueHeaderBytes = 32;

struct HashEntry {
  Digest digest_{};
  std::string field_;
  std::string value_;
};

struct HashValue {
  std::vector<HashEntry> entries_;
};

// The compact encoding contains complete field/value pairs, never a mutation
// log or a process-local digest. Fixed-width header fields and entry lengths
// are little-endian; decoding reconstructs lookup digests.
// A logical Hash can span many records: aggregate bytes have no String or
// record-payload limit here. Each field/value remains limited to 512 MiB and
// the encoded count to uint32_t. Storage callers enforce physical envelopes
// and admit materialization memory independently of this logical codec.
absl::StatusOr<HashValue> DecodeHashValue(std::string_view payload);
absl::StatusOr<std::string> EncodeHashValue(const HashValue& value);

// Checked size accumulation without allocating the supplied field/value.
// Used before serialization; failure leaves the caller's running size intact.
// max_bytes is the destination's capacity bound: a physical group envelope or
// the output string's max_size() for a materialized logical Hash.
absl::StatusOr<std::size_t> AppendHashEntrySize(
    std::size_t encoded_bytes, std::size_t field_bytes, std::size_t value_bytes,
    std::size_t max_bytes);

}  // namespace keylane::storage
