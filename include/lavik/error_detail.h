/*
 * Copyright (C) 2026 EloqData Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <cstdint>
#include <string_view>
#include <utility>

#include "absl/status/status.h"
#include "absl/strings/cord.h"

namespace lavik {

// Status codes alone cannot distinguish a wrong value type from bad numeric
// input, or memory pressure from disk exhaustion. Carry that distinction from
// the source without requiring consumers to interpret human-readable text.
enum class ErrorDetail : std::uint8_t {
  kNone,
  kWrongType,
  kMemoryLimit,
  kSyntax,
  kAuthorityChanged,
  kDiskSpace
};
inline constexpr std::string_view kErrorDetailPayload = "lavik.error-detail";

inline absl::Status WithErrorDetail(absl::Status status, ErrorDetail detail) {
  const char value = static_cast<char>(detail);
  status.SetPayload(kErrorDetailPayload,
                    absl::Cord(std::string_view(&value, 1)));
  return status;
}
inline ErrorDetail GetErrorDetail(const absl::Status& status) {
  const auto payload = status.GetPayload(kErrorDetailPayload);
  if (!payload || payload->size() != 1) return ErrorDetail::kNone;
  return static_cast<ErrorDetail>(*payload->char_begin());
}
inline absl::Status WrongTypeError() {
  return WithErrorDetail(
      absl::InvalidArgumentError(
          "WRONGTYPE Operation against a key holding the wrong kind of value"),
      ErrorDetail::kWrongType);
}
inline absl::Status MemoryLimitError(std::string_view message) {
  return WithErrorDetail(absl::ResourceExhaustedError(message),
                         ErrorDetail::kMemoryLimit);
}
inline absl::Status SyntaxError(std::string_view message) {
  return WithErrorDetail(absl::InvalidArgumentError(message),
                         ErrorDetail::kSyntax);
}

}  // namespace lavik
