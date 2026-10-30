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

// Terminal command classification is independent of shard attempts and socket
// writes. Queueing is retained internally so it cannot become an execution.
enum class CommandResult : std::uint8_t {
  kSuccess,
  kFailed,
  kRejected,
  kRedirected,
  kQueued,
  kCancelled,
  kCount
};

// The wire prefix, independent of the internal cause (e.g. AUTH can return
// ERR or WRONGPASS). User-defined Lua prefixes share OTHER so labels remain
// bounded. Classification happens only when constructing an error reply.
enum class CommandErrorPrefix : std::uint8_t {
  kNone,
  kErr,
  kWrongType,
  kOom,
  kNoAuth,
  kWrongPass,
  kNoPerm,
  kLoading,
  kReadOnly,
  kMasterDown,
  kClusterDown,
  kCrossSlot,
  kMoved,
  kAsk,
  kTryAgain,
  kBusy,
  kBusyGroup,
  kBusyKey,
  kNoGroup,
  kNoScript,
  kExecAbort,
  kNoProto,
  kNotBusy,
  kUnkillable,
  kUnblocked,
  kOther,
  kCount
};
inline constexpr std::array<std::string_view, 26> kCommandErrorPrefixNames{
    "",          "ERR",        "WRONGTYPE",   "OOM",
    "NOAUTH",    "WRONGPASS",  "NOPERM",      "LOADING",
    "READONLY",  "MASTERDOWN", "CLUSTERDOWN", "CROSSSLOT",
    "MOVED",     "ASK",        "TRYAGAIN",    "BUSY",
    "BUSYGROUP", "BUSYKEY",    "NOGROUP",     "NOSCRIPT",
    "EXECABORT", "NOPROTO",    "NOTBUSY",     "UNKILLABLE",
    "UNBLOCKED", "OTHER"};
static_assert(kCommandErrorPrefixNames.size() ==
              static_cast<unsigned>(CommandErrorPrefix::kCount));

inline CommandErrorPrefix CommandErrorPrefixForReply(std::string_view text) {
  // Redis falls back to ERR if no short, space-delimited prefix is present.
  const auto space = text.find(' ');
  if (space == std::string_view::npos || space >= 31)
    return CommandErrorPrefix::kErr;
  const auto prefix = text.substr(0, space);
  for (std::size_t i = 1; i < kCommandErrorPrefixNames.size(); ++i)
    if (prefix == kCommandErrorPrefixNames[i])
      return static_cast<CommandErrorPrefix>(i);
  return CommandErrorPrefix::kOther;
}

// Internal causes preserve execution/admission and cancellation provenance.
// They are not exported as a metric dimension.
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
  // A newly constructed, not-yet-counted error. RecordCommandResult consumes
  // this provenance for errorstats as well as recording the command result.
  // Lua records its individual errors separately and leaves this field empty
  // when propagating the outer failure, preventing child/parent double counts.
  CommandErrorPrefix prefix_ = CommandErrorPrefix::kNone;

  [[nodiscard]] bool executed() const noexcept {
    return result_ == CommandResult::kSuccess ||
           result_ == CommandResult::kFailed;
  }
};

}  // namespace lavik
