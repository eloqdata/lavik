#pragma once

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "keylane/server.h"

namespace keylane {

// Parses Redis-style binary memory sizes such as "67108864", "64mb", and
// "1gb". A suffix is optional; unsuffixed values are bytes.
absl::StatusOr<std::size_t> ParseMemorySize(std::string_view text);

// Tokenizes one Redis configuration line. Whitespace separates arguments,
// single and double quotes preserve whitespace, and an unquoted '#' starts a
// comment. An empty/comment-only line returns an empty vector.
absl::StatusOr<std::vector<std::string>> ParseRedisConfigLine(
    std::string_view line);

// Applies one already-tokenized directive to Keylane's startup options.
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
absl::Status RewriteRedisConfigFile(
    const std::string& path, std::optional<ReplicaOfConfig> upstream,
    bool redis_upstream, unsigned replica_priority);

// Validates cross-field startup constraints after config-file and CLI values
// have both been applied.
absl::Status ValidateServerOptions(const ServerOptions& options);

}  // namespace keylane
