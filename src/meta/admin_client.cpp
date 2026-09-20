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

#include "lavik/meta/admin_client.h"

#include "absl/strings/cord.h"

namespace lavik::meta {
namespace {
constexpr std::size_t kMaxCommandBytes = 64 * 1024;
constexpr std::size_t kMaxReplyBytes = 256 * 1024 * 1024;
constexpr std::string_view kRequestNotSentPayload =
    "type.googleapis.com/lavik.meta.admin-request-not-sent";
}  // namespace

absl::Status MarkMetaAdminRequestNotSent(absl::Status status) {
  if (!status.ok()) {
    status.SetPayload(kRequestNotSentPayload, absl::Cord("1"));
  }
  return status;
}

bool MetaAdminRequestDefinitelyNotSent(const absl::Status& status) {
  return status.GetPayload(kRequestNotSentPayload).has_value();
}

absl::StatusOr<std::string> MetaAdminClient::RoundTrip(
    const MetaAdminTarget& target, std::string_view command,
    MetaAdminDeadline deadline) const {
  if (command.empty()) {
    return MarkMetaAdminRequestNotSent(
        absl::InvalidArgumentError("empty command"));
  }
  if (command.find_first_of("\r\n") != std::string_view::npos) {
    return MarkMetaAdminRequestNotSent(
        absl::InvalidArgumentError("command must be exactly one line"));
  }
  if (command.size() + 1 > kMaxCommandBytes) {
    return MarkMetaAdminRequestNotSent(
        absl::ResourceExhaustedError("command exceeds 64 KiB limit"));
  }
  std::string wire(command);
  wire.push_back('\n');

  auto stream = net::SyncStream::Connect(target, deadline);
  if (!stream.ok()) return MarkMetaAdminRequestNotSent(stream.status());
  if (auto status = (*stream)->WriteAll(wire); !status.ok()) return status;
  return (*stream)->ReadLine(kMaxReplyBytes);
}

}  // namespace lavik::meta
