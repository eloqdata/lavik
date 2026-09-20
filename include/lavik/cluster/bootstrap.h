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

#include <optional>
#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "lavik/cluster/control_protocol.h"
#include "lavik/net/sync_stream.h"

namespace lavik::cluster {

struct DataBootstrapOptions {
  std::vector<std::string> seeds;
  std::string node_id;
  control::ClientServiceCapabilities capabilities;
  std::optional<net::SyncTlsOptions> tls;
  int cancel_fd = -1;
};

// Startup-thread-only discovery of committed client semantics. Retries absent
// Meta, creation and registration without opening storage or a Redis listener.
// Authentication/capability failures are terminal; cancellation interrupts all
// connection, TLS, frame and backoff waits. Nothing is written durably on Data.
absl::StatusOr<control::ServiceDeclaration> BootstrapClientService(
    const DataBootstrapOptions& options);

// Describes the handlers installed by this binary, before storage exists.
// The regular session adds the actual boot's installed mode and DB layout.
control::ClientServiceCapabilities SupportedClientServiceCapabilities();

}  // namespace lavik::cluster
