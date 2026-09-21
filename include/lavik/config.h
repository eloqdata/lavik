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

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "lavik/server.h"

namespace lavik {

// Parses Redis-style binary memory sizes such as "67108864", "64mb", and
// "1gb". A suffix is optional; unsuffixed values are bytes.
absl::StatusOr<std::size_t> ParseMemorySize(std::string_view text);

// Parses Valkey-compatible maxmemory-clients values: an ordinary memory size,
// a percentage from 0% through 100%, or zero to disable the limit.
absl::StatusOr<ClientBufferLimit> ParseClientBufferLimit(std::string_view text);
std::string FormatClientBufferLimit(ClientBufferLimit limit);

// Parses Redis-compatible client-query-buffer-limit values. Redis constrains
// this per-connection hard limit to the range [1 MiB, LONG_MAX].
absl::StatusOr<std::size_t> ParseClientQueryBufferLimit(std::string_view text);

// Tokenizes one Redis configuration line. Whitespace separates arguments,
// single and double quotes preserve whitespace, and an unquoted '#' starts a
// comment. An empty/comment-only line returns an empty vector.
absl::StatusOr<std::vector<std::string>> ParseRedisConfigLine(
    std::string_view line);

// Applies one already-tokenized directive to Lavik's startup options.
// Unsupported directives are rejected instead of being silently ignored.
absl::Status ApplyRedisConfigDirective(
    const std::vector<std::string>& directive, ServerOptions* options);

// Loads a Redis-style configuration file. Errors include the file and line
// number so startup failures can be fixed directly.
absl::Status LoadRedisConfigFile(const std::string& path,
                                 ServerOptions* options);

// Replaces the failover-managed directives in an existing configuration file
// and durably installs the result with a same-directory atomic rename. Other
// directives and comments are preserved verbatim.
absl::Status RewriteRedisConfigFile(const std::string& path,
                                    std::optional<ReplicaOfConfig> upstream,
                                    bool redis_upstream,
                                    unsigned replica_priority);

// Validates cross-field startup constraints after config-file and CLI values
// have both been applied.
absl::Status ValidateServerOptions(const ServerOptions& options);

// Resolve an automatic (zero) shard count after config and CLI overrides.
// Exclusive Meta placement reserves one selected CPU, including when unpinned
// placement would otherwise have been requested (that combination is rejected).
absl::Status ResolveAutomaticShardCount(ServerOptions* options);

// Resolve cyclic shard/control placement against the inherited CPU affinity.
// Empty result means unpinned; explicit CPU IDs must be permitted by the OS.
absl::StatusOr<std::vector<unsigned>> ResolveWorkerCpuIds(
    const ServerOptions& options);

}  // namespace lavik
