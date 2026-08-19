#pragma once

#include <string>
#include <string_view>

#include "absl/status/statusor.h"
#include "keylane/storage/engine.h"

namespace keylane::rdb {

inline constexpr unsigned kVersion = 11;

// Encodes and decodes the value-only payload used by Redis DUMP/RESTORE.
// Expiration is deliberately supplied by RESTORE and is not part of payload.
absl::StatusOr<std::string> EncodeDump(const storage::RawValue& value);
absl::StatusOr<storage::RawValue> DecodeDump(std::string_view payload);

}  // namespace keylane::rdb
