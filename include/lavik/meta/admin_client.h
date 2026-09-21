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

// Raft-free synchronous transport for Meta's one-line administrative
// protocol. Direct administration and cluster discovery share endpoint
// validation, partial I/O, TLS identity checks, response limits, and deadlines.

#include <chrono>
#include <string>
#include <string_view>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "lavik/net/sync_stream.h"

namespace lavik::meta {

using MetaAdminDeadline = net::SyncDeadline;
using MetaAdminTlsOptions = net::SyncTlsOptions;
using MetaAdminTarget = net::SyncTarget;

// Attaches/queries transport evidence that the Admin command itself was not
// written. ClusterCreate uses this distinction after leader discovery:
// connect and TLS-handshake failures are safely pre-mutation, while any write
// or reply failure remains uncertain. The marker survives StatusOr transport
// adapters without relying on error-message parsing.
absl::Status MarkMetaAdminRequestNotSent(absl::Status status);
bool MetaAdminRequestDefinitelyNotSent(const absl::Status& status);

class MetaAdminClient {
 public:
  // Sends one command and returns the reply without its line terminator.
  // The deadline is absolute and is shared by connect, TLS, partial writes,
  // and response reads. Commands containing CR/LF or exceeding 64 KiB and
  // replies exceeding 256 MiB are rejected.
  absl::StatusOr<std::string> RoundTrip(const MetaAdminTarget& target,
                                        std::string_view command,
                                        MetaAdminDeadline deadline) const;
};

}  // namespace lavik::meta
