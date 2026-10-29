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

#include <array>
#include <cstdint>
#include <string_view>

#include "lavik/error_detail.h"

namespace lavik {

// These are logical command events, not shard attempts or socket writes.
// A queued client command and its later EXEC child belong to separate contexts.
enum class CommandContext : std::uint8_t { kClient, kExec, kScript, kCount };
enum class CommandResult : std::uint8_t {
  kSuccess,
  kFailed,
  kRejected,
  kRedirected,
  kQueued,
  kCancelled,
  kCount
};

// Labels are selected at the error's source, never from encoded RESP or user
// text. New reasons require an explicit bounded vocabulary change.
enum class CommandErrorReason : std::uint8_t {
  kNone,
  kUnknownCommand,
  kArity,
  kSyntax,
  kWrongType,
  kInvalidValue,
  kNoAuth,
  kAuth,
  kPermission,
  kOom,
  kLoading,
  kReadOnly,
  kAuthority,
  kClusterDown,
  kBusy,
  kTryAgain,
  kMoved,
  kAsk,
  kNoScript,
  kScript,
  kExecAbort,
  kStorage,
  kStorageFull,
  kInternal,
  kShutdown,
  kClientError,
  kCancelled,
  kCount
};

inline constexpr std::array<std::string_view, 3> kCommandContextNames{
    "client", "exec", "script"};
inline constexpr std::array<std::string_view, 6> kCommandResultNames{
    "success", "failed", "rejected", "redirected", "queued", "cancelled"};
inline constexpr std::array<std::string_view, 27> kCommandErrorReasonNames{
    "none",         "unknown_command",
    "arity",        "syntax",
    "wrongtype",    "invalid_value",
    "noauth",       "auth",
    "permission",   "oom",
    "loading",      "readonly",
    "authority",    "clusterdown",
    "busy",         "tryagain",
    "moved",        "ask",
    "noscript",     "script",
    "execabort",    "storage",
    "storage_full", "internal",
    "shutdown",     "client_error",
    "cancelled"};
static_assert(kCommandErrorReasonNames.size() ==
              static_cast<unsigned>(CommandErrorReason::kCount));

// The same status can travel through several modules before encoding. The
// wire error prefix remains a compatibility concern of the reply encoder.
inline CommandErrorReason CommandErrorReasonForStatus(
    const absl::Status& status) {
  switch (GetErrorDetail(status)) {
    case ErrorDetail::kWrongType:
      return CommandErrorReason::kWrongType;
    case ErrorDetail::kMemoryLimit:
      return CommandErrorReason::kOom;
    case ErrorDetail::kSyntax:
      return CommandErrorReason::kSyntax;
    case ErrorDetail::kAuthorityChanged:
      return CommandErrorReason::kAuthority;
    case ErrorDetail::kDiskSpace:
      return CommandErrorReason::kStorageFull;
    case ErrorDetail::kNone:
      break;
  }
  switch (status.code()) {
    case absl::StatusCode::kInvalidArgument:
      return CommandErrorReason::kInvalidValue;
    case absl::StatusCode::kOutOfRange:
      return CommandErrorReason::kInvalidValue;
    case absl::StatusCode::kAlreadyExists:
    case absl::StatusCode::kNotFound:
    case absl::StatusCode::kUnimplemented:
      return CommandErrorReason::kClientError;
    case absl::StatusCode::kInternal:
      return CommandErrorReason::kInternal;
    case absl::StatusCode::kPermissionDenied:
      return CommandErrorReason::kPermission;
    case absl::StatusCode::kCancelled:
      return CommandErrorReason::kCancelled;
    default:
      return CommandErrorReason::kStorage;
  }
}

enum class RequestInputError : std::uint8_t {
  kProtocol,
  kQueryBuffer,
  kClientMemory
};
inline constexpr std::array<std::string_view, 3> kRequestInputErrorNames{
    "protocol", "query_buffer", "client_memory"};

struct CommandOutcome {
  CommandResult result_ = CommandResult::kSuccess;
  CommandErrorReason reason_ = CommandErrorReason::kNone;

  [[nodiscard]] bool executed() const noexcept {
    return result_ == CommandResult::kSuccess ||
           result_ == CommandResult::kFailed ||
           result_ == CommandResult::kCancelled;
  }
};

}  // namespace lavik
