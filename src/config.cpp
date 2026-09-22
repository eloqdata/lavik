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

#include "lavik/config.h"

#include <fcntl.h>
#include <sched.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cctype>
#include <cerrno>
#include <charconv>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <limits>
#include <mutex>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "bycorf/runtime/cross_core.h"
#include "lavik/numeric_endpoint.h"

namespace lavik {
namespace {

template <typename T>
absl::Status ParseUnsigned(std::string_view text, std::string_view name,
                           T* value, bool allow_zero) {
  static_assert(std::is_unsigned_v<T>);
  T parsed = 0;
  const char* begin = text.data();
  const char* end = begin + text.size();
  const auto [parsed_end, error] = std::from_chars(begin, end, parsed);
  if (text.empty() || error != std::errc{} || parsed_end != end ||
      (!allow_zero && parsed == 0)) {
    return absl::InvalidArgumentError(
        absl::StrCat("invalid ", name, " '", text, "'"));
  }
  *value = parsed;
  return absl::OkStatus();
}

template <typename T>
absl::Status ParseSigned(std::string_view text, std::string_view name,
                         T* value) {
  static_assert(std::is_signed_v<T>);
  T parsed = 0;
  const char* begin = text.data();
  const char* end = begin + text.size();
  const auto [parsed_end, error] = std::from_chars(begin, end, parsed);
  if (text.empty() || error != std::errc{} || parsed_end != end) {
    return absl::InvalidArgumentError(
        absl::StrCat("invalid ", name, " '", text, "'"));
  }
  *value = parsed;
  return absl::OkStatus();
}

absl::StatusOr<bool> ParseYesNo(std::string_view text, std::string_view name) {
  if (absl::EqualsIgnoreCase(text, "yes")) return true;
  if (absl::EqualsIgnoreCase(text, "no")) return false;
  return absl::InvalidArgumentError(
      absl::StrCat(name, " must be 'yes' or 'no'"));
}

absl::Status WrongArgumentCount(std::string_view name) {
  return absl::InvalidArgumentError(
      absl::StrCat("wrong number of arguments for '", name, "' directive"));
}

char DecodeEscape(char escaped) {
  switch (escaped) {
    case 'n':
      return '\n';
    case 'r':
      return '\r';
    case 't':
      return '\t';
    case 'b':
      return '\b';
    case 'a':
      return '\a';
    default:
      return escaped;
  }
}

std::string QuoteRedisConfigArgument(std::string_view value) {
  std::string quoted;
  quoted.reserve(value.size() + 2);
  quoted.push_back('"');
  for (char current : value) {
    switch (current) {
      case '\\':
      case '"':
        quoted.push_back('\\');
        quoted.push_back(current);
        break;
      case '\n':
        quoted += "\\n";
        break;
      case '\r':
        quoted += "\\r";
        break;
      case '\t':
        quoted += "\\t";
        break;
      default:
        quoted.push_back(current);
        break;
    }
  }
  quoted.push_back('"');
  return quoted;
}

bool IsLowerHexNodeId(std::string_view value) {
  if (value.size() != 40) return false;
  for (const char c : value) {
    if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) return false;
  }
  return true;
}

// Control sessions authenticate the dialled numeric address against the
// peer's IP SAN. Keeping seeds numeric makes that check deterministic and
// avoids an implicit DNS trust source in the bootstrap path.
bool IsNumericEndpoint(std::string_view endpoint) {
  return ParseNumericEndpoint(endpoint).has_value();
}

absl::Status WriteAll(int fd, std::string_view contents,
                      std::string_view path) {
  while (!contents.empty()) {
    const ssize_t written = ::write(fd, contents.data(), contents.size());
    if (written < 0) {
      if (errno == EINTR) continue;
      return absl::InternalError(
          absl::StrCat("cannot write configuration temporary file for '", path,
                       "': ", std::strerror(errno)));
    }
    if (written == 0) {
      return absl::InternalError(absl::StrCat(
          "short write to configuration temporary file for '", path, "'"));
    }
    contents.remove_prefix(static_cast<std::size_t>(written));
  }
  return absl::OkStatus();
}

std::string ParentDirectory(std::string_view path) {
  const std::size_t separator = path.rfind('/');
  if (separator == std::string_view::npos) return ".";
  if (separator == 0) return "/";
  return std::string(path.substr(0, separator));
}

}  // namespace

absl::StatusOr<std::size_t> ParseMemorySize(std::string_view text) {
  if (text.empty()) {
    return absl::InvalidArgumentError("memory size must not be empty");
  }
  std::size_t digits = 0;
  while (digits < text.size() && text[digits] >= '0' && text[digits] <= '9') {
    ++digits;
  }
  if (digits == 0) {
    return absl::InvalidArgumentError(
        absl::StrCat("invalid memory size '", text, "'"));
  }
  std::uint64_t value = 0;
  const auto parsed = std::from_chars(text.data(), text.data() + digits, value);
  if (parsed.ec != std::errc{} || parsed.ptr != text.data() + digits) {
    return absl::InvalidArgumentError(
        absl::StrCat("invalid memory size '", text, "'"));
  }
  std::string suffix(text.substr(digits));
  absl::AsciiStrToLower(&suffix);
  std::uint64_t multiplier = 1;
  if (suffix.empty() || suffix == "b") {
    multiplier = 1;
  } else if (suffix == "k" || suffix == "kb") {
    multiplier = 1024ULL;
  } else if (suffix == "m" || suffix == "mb") {
    multiplier = 1024ULL * 1024;
  } else if (suffix == "g" || suffix == "gb") {
    multiplier = 1024ULL * 1024 * 1024;
  } else if (suffix == "t" || suffix == "tb") {
    multiplier = 1024ULL * 1024 * 1024 * 1024;
  } else {
    return absl::InvalidArgumentError(
        absl::StrCat("invalid memory size suffix in '", text, "'"));
  }
  if (value > std::numeric_limits<std::size_t>::max() / multiplier) {
    return absl::OutOfRangeError(
        absl::StrCat("memory size is too large: '", text, "'"));
  }
  return static_cast<std::size_t>(value * multiplier);
}

absl::StatusOr<ClientBufferLimit> ParseClientBufferLimit(
    std::string_view text) {
  if (text.ends_with('%')) {
    const std::string_view number = text.substr(0, text.size() - 1);
    if (number.empty()) {
      return absl::InvalidArgumentError(
          "maxmemory-clients percentage must not be empty");
    }
    std::uint64_t percentage = 0;
    const auto parsed = std::from_chars(
        number.data(), number.data() + number.size(), percentage);
    if (parsed.ec != std::errc{} ||
        parsed.ptr != number.data() + number.size() || percentage > 100) {
      return absl::InvalidArgumentError(
          "maxmemory-clients percentage must be between 0% and 100%");
    }
    return ClientBufferLimit{.value_ = percentage, .percentage_ = true};
  }

  auto bytes = ParseMemorySize(text);
  if (!bytes.ok()) return bytes.status();
  return ClientBufferLimit{.value_ = *bytes, .percentage_ = false};
}

std::string FormatClientBufferLimit(ClientBufferLimit limit) {
  std::string result = std::to_string(limit.value_);
  if (limit.percentage_) result.push_back('%');
  return result;
}

absl::StatusOr<std::size_t> ParseClientQueryBufferLimit(std::string_view text) {
  auto bytes = ParseMemorySize(text);
  if (!bytes.ok()) return bytes.status();
  if (*bytes < kMinimumClientQueryBufferLimit ||
      *bytes > static_cast<std::size_t>(std::numeric_limits<long>::max())) {
    return absl::InvalidArgumentError(
        "client-query-buffer-limit must be between 1mb and LONG_MAX bytes");
  }
  return *bytes;
}

absl::StatusOr<std::vector<std::string>> ParseRedisConfigLine(
    std::string_view line) {
  std::vector<std::string> result;
  std::size_t position = 0;
  while (position < line.size()) {
    while (position < line.size() &&
           std::isspace(static_cast<unsigned char>(line[position]))) {
      ++position;
    }
    if (position == line.size() || line[position] == '#') break;

    std::string token;
    char quote = 0;
    if (line[position] == '\'' || line[position] == '"') {
      quote = line[position++];
    }
    bool closed = quote == 0;
    while (position < line.size()) {
      const char current = line[position];
      if (quote != 0) {
        if (current == quote) {
          ++position;
          closed = true;
          break;
        }
      } else if (std::isspace(static_cast<unsigned char>(current)) ||
                 current == '#') {
        break;
      }
      if (current == '\\' && position + 1 < line.size()) {
        token.push_back(DecodeEscape(line[position + 1]));
        position += 2;
      } else {
        token.push_back(current);
        ++position;
      }
    }
    if (!closed) {
      return absl::InvalidArgumentError("unterminated quoted argument");
    }
    if (quote != 0 && position < line.size() &&
        !std::isspace(static_cast<unsigned char>(line[position])) &&
        line[position] != '#') {
      return absl::InvalidArgumentError(
          "quoted argument must be followed by whitespace or a comment");
    }
    result.push_back(std::move(token));
    if (position < line.size() && line[position] == '#') break;
  }
  return result;
}

absl::Status ApplyRedisConfigDirective(
    const std::vector<std::string>& directive, ServerOptions* options) {
  if (options == nullptr) {
    return absl::InvalidArgumentError("server options must not be null");
  }
  if (directive.empty()) return absl::OkStatus();

  const std::string name = absl::AsciiStrToLower(directive.front());
  if (name == "bind") {
    if (directive.size() < 2) return WrongArgumentCount(name);
    std::vector<std::string> addresses(directive.begin() + 1, directive.end());
    for (const std::string& address : addresses) {
      if (address.empty()) {
        return absl::InvalidArgumentError("bind address must not be empty");
      }
    }
    options->bind_addresses_ = std::move(addresses);
    return absl::OkStatus();
  }
  if (name == "port") {
    if (directive.size() != 2) return WrongArgumentCount(name);
    return ParseUnsigned(directive[1], "port", &options->port_, true);
  }
  if (name == "tls-port") {
    if (directive.size() != 2) return WrongArgumentCount(name);
    return ParseUnsigned(directive[1], name, &options->tls_port_, true);
  }
  if (name == "logtostderr" || name == "alsologtostderr") {
    if (directive.size() != 2) return WrongArgumentCount(name);
    auto enabled = ParseYesNo(directive[1], name);
    if (!enabled.ok()) return enabled.status();
    if (name == "logtostderr") {
      options->logging_.log_to_stderr_ = *enabled;
    } else {
      options->logging_.also_log_to_stderr_ = *enabled;
    }
    return absl::OkStatus();
  }
  if (name == "log-dir" || name == "log_dir") {
    if (directive.size() != 2) return WrongArgumentCount(name);
    options->logging_.log_dir_ = directive[1];
    return absl::OkStatus();
  }
  if (name == "max-log-size-mb" || name == "max_log_size_mb") {
    if (directive.size() != 2) return WrongArgumentCount(name);
    return ParseUnsigned(directive[1], name,
                         &options->logging_.max_log_size_mb_, false);
  }
  if (name == "max-log-files" || name == "max_log_files") {
    if (directive.size() != 2) return WrongArgumentCount(name);
    return ParseUnsigned(directive[1], name, &options->logging_.max_log_files_,
                         false);
  }
  if (name == "tls-cert-file" || name == "tls-key-file" ||
      name == "tls-ca-cert-file" || name == "requirepass" ||
      name == "masteruser" || name == "masterauth" || name == "load-rdb" ||
      name == "rdb-dir" || name == "dir" || name == "dbfilename") {
    if (directive.size() != 2) return WrongArgumentCount(name);
    if (name == "tls-cert-file") options->tls_cert_file_ = directive[1];
    if (name == "tls-key-file") options->tls_key_file_ = directive[1];
    if (name == "tls-ca-cert-file") options->tls_ca_cert_file_ = directive[1];
    if (name == "requirepass") options->requirepass_ = directive[1];
    if (name == "masteruser") options->masteruser_ = directive[1];
    if (name == "masterauth") options->masterauth_ = directive[1];
    if (name == "load-rdb") options->load_rdb_file_ = directive[1];
    if (name == "rdb-dir" || name == "dir") options->rdb_dir_ = directive[1];
    if (name == "dbfilename") options->dbfilename_ = directive[1];
    return absl::OkStatus();
  }
  if (name == "tls-auth-clients") {
    if (directive.size() != 2) return WrongArgumentCount(name);
    const std::string value = absl::AsciiStrToLower(directive[1]);
    if (value != "no" && value != "optional" && value != "yes") {
      return absl::InvalidArgumentError(
          "tls-auth-clients must be 'no', 'optional', or 'yes'");
    }
    options->tls_auth_clients_ = value;
    return absl::OkStatus();
  }
  if (name == "tls-replication") {
    if (directive.size() != 2) return WrongArgumentCount(name);
    auto enabled = ParseYesNo(directive[1], name);
    if (!enabled.ok()) return enabled.status();
    options->tls_replication_ = *enabled;
    return absl::OkStatus();
  }
  if (name == "load-rdb-replace") {
    if (directive.size() != 2) return WrongArgumentCount(name);
    auto enabled = ParseYesNo(directive[1], name);
    if (!enabled.ok()) return enabled.status();
    options->load_rdb_replace_ = *enabled;
    return absl::OkStatus();
  }
  if (name == "shutdown-checkpoint") {
    if (directive.size() != 2) return WrongArgumentCount(name);
    auto enabled = ParseYesNo(directive[1], name);
    if (!enabled.ok()) return enabled.status();
    options->shutdown_checkpoint_ = *enabled;
    return absl::OkStatus();
  }
  if (name == "shards" || name == "threads" || name == "io-threads") {
    if (directive.size() != 2) return WrongArgumentCount(name);
    return ParseUnsigned(directive[1], name, &options->shard_count_, false);
  }
  if (name == "meta-exclusive-cpu") {
    if (directive.size() != 2) return WrongArgumentCount(name);
    auto enabled = ParseYesNo(directive[1], name);
    if (!enabled.ok()) return enabled.status();
    options->meta_exclusive_cpu_ = *enabled;
    return absl::OkStatus();
  }
  if (name == "cpus") {
    if (directive.size() != 2) return WrongArgumentCount(name);
    std::vector<unsigned> cpus;
    for (std::string_view item : absl::StrSplit(directive[1], ',')) {
      unsigned cpu = 0;
      auto status = ParseUnsigned(item, name, &cpu, true);
      if (!status.ok()) return status;
      cpus.push_back(cpu);
    }
    options->cpu_ids_ = std::move(cpus);
    return absl::OkStatus();
  }
  if (name == "maxclients") {
    if (directive.size() != 2) return WrongArgumentCount(name);
    return ParseUnsigned(directive[1], name, &options->max_clients_, false);
  }
  if (name == "maxmemory-clients") {
    if (directive.size() != 2) return WrongArgumentCount(name);
    auto limit = ParseClientBufferLimit(directive[1]);
    if (!limit.ok()) return limit.status();
    options->maxmemory_clients_ = *limit;
    return absl::OkStatus();
  }
  if (name == "client-query-buffer-limit") {
    if (directive.size() != 2) return WrongArgumentCount(name);
    auto limit = ParseClientQueryBufferLimit(directive[1]);
    if (!limit.ok()) return limit.status();
    options->client_query_buffer_limit_bytes_ = *limit;
    return absl::OkStatus();
  }
  if (name == "save") {
    // Redis uses `save ""` to disable every automatic snapshot policy. Other
    // forms contain one or more seconds/changes pairs and repeated directives
    // accumulate policies.
    if (directive.size() == 2 && directive[1].empty()) {
      options->rdb_save_rules_.clear();
      return absl::OkStatus();
    }
    if (directive.size() < 3 || directive.size() % 2 == 0) {
      return WrongArgumentCount(name);
    }
    std::vector<RdbSaveRule> parsed;
    parsed.reserve((directive.size() - 1) / 2);
    for (std::size_t i = 1; i < directive.size(); i += 2) {
      RdbSaveRule rule;
      absl::Status status =
          ParseUnsigned(directive[i], "save seconds", &rule.seconds_, false);
      if (!status.ok()) return status;
      status = ParseUnsigned(directive[i + 1], "save changes",
                             &rule.changes_, true);
      if (!status.ok()) return status;
      parsed.push_back(rule);
    }
    options->rdb_save_rules_.insert(options->rdb_save_rules_.end(),
                                    parsed.begin(), parsed.end());
    return absl::OkStatus();
  }
  if (name == "replicaof" || name == "redis-replicaof") {
    if (directive.size() != 3) return WrongArgumentCount(name);
    if (directive[1].empty()) {
      return absl::InvalidArgumentError("replicaof host must not be empty");
    }
    std::uint16_t port = 0;
    absl::Status parsed =
        ParseUnsigned(directive[2], absl::StrCat(name, " port"), &port, false);
    if (!parsed.ok()) return parsed;
    if (name == "replicaof") {
      options->replicaof_ = ReplicaOfConfig{directive[1], port};
    } else {
      options->redis_replicaof_ = ReplicaOfConfig{directive[1], port};
    }
    return absl::OkStatus();
  }
  if (name == "client-mode" || name == "meta-managed") {
    return absl::InvalidArgumentError(
        absl::StrCat(name,
                     " was removed: configure meta-seed and declare "
                     "client_mode in the Meta creation manifest; "
                     "omit Meta seeds for standalone mode"));
  }
  if (name == "announce-ip" || name == "node-id" || name == "meta-seed") {
    if (directive.size() != 2) return WrongArgumentCount(name);
    if (name == "announce-ip") {
      options->announce_ip_ = directive[1];
    } else if (name == "node-id") {
      options->node_id_ = directive[1];
    } else {
      options->meta_seeds_.push_back(directive[1]);
    }
    return absl::OkStatus();
  }
  if (name == "announce-port" || name == "announce-tls-port") {
    if (directive.size() != 2) return WrongArgumentCount(name);
    // 0 follows the corresponding listen port (port / tls-port).
    std::uint16_t port = 0;
    absl::Status parsed = ParseUnsigned(directive[1], name, &port, true);
    if (!parsed.ok()) return parsed;
    if (name == "announce-port") {
      options->announce_port_ = port;
    } else {
      options->announce_tls_port_ = port;
    }
    return absl::OkStatus();
  }
  if (name == "replica-serve-stale-data") {
    if (directive.size() != 2) return WrongArgumentCount(name);
    auto enabled = ParseYesNo(directive[1], name);
    if (!enabled.ok()) return enabled.status();
    options->replication_options_.replica_serve_stale_data_ = *enabled;
    return absl::OkStatus();
  }
  if (name == "replica-read-only") {
    if (directive.size() != 2) return WrongArgumentCount(name);
    auto read_only = ParseYesNo(directive[1], name);
    if (!read_only.ok()) return read_only.status();
    options->replication_options_.replica_read_only_ = *read_only;
    return absl::OkStatus();
  }
  if (name == "replica-priority") {
    if (directive.size() != 2) return WrongArgumentCount(name);
    unsigned priority = 0;
    absl::Status parsed = ParseUnsigned(directive[1], name, &priority, true);
    if (!parsed.ok()) return parsed;
    options->replication_options_.replica_priority_ = priority;
    return absl::OkStatus();
  }

  if (name == "replication-backlog-backpressure") {
    if (directive.size() != 2) return WrongArgumentCount(name);
    auto enabled = ParseYesNo(directive[1], name);
    if (!enabled.ok()) return enabled.status();
    options->replication_options_.backlog_backpressure_ = *enabled;
    return absl::OkStatus();
  }
  if (name == "recv-buffers-per-worker") {
    if (directive.size() != 2) return WrongArgumentCount(name);
    return ParseUnsigned(directive[1], name, &options->recv_buffer_count_,
                         true);
  }
  if (name == "slowlog-log-slower-than") {
    if (directive.size() != 2) return WrongArgumentCount(name);
    absl::Status parsed =
        ParseSigned(directive[1], name, &options->slowlog_log_slower_than_us_);
    if (!parsed.ok()) return parsed;
    if (options->slowlog_log_slower_than_us_ < -1) {
      return absl::InvalidArgumentError(
          "slowlog-log-slower-than must be greater than or equal to -1");
    }
    return absl::OkStatus();
  }
  if (name == "slowlog-max-len") {
    if (directive.size() != 2) return WrongArgumentCount(name);
    return ParseUnsigned(directive[1], name, &options->slowlog_max_len_, true);
  }
  if (name == "foreground-budget-us" || name == "background-budget-us" ||
      name == "background-warrant-percent" ||
      name == "spdk-max-completions-per-poll") {
    if (directive.size() != 2) return WrongArgumentCount(name);
    unsigned value = 0;
    const bool allow_zero = name == "spdk-max-completions-per-poll";
    absl::Status parsed = ParseUnsigned(directive[1], name, &value, allow_zero);
    if (!parsed.ok()) return parsed;
    if (name == "background-warrant-percent" && value > 100) {
      return absl::InvalidArgumentError(
          "background-warrant-percent must be between 1 and 100");
    }
    if (name == "foreground-budget-us") {
      options->foreground_budget_us_ = value;
    } else if (name == "background-budget-us") {
      options->background_budget_us_ = value;
    } else if (name == "background-warrant-percent") {
      options->background_warrant_percent_ = value;
    } else {
      options->spdk_max_completions_per_poll_ = value;
    }
    return absl::OkStatus();
  }
  if (name == "replication-snapshot-batch-size") {
    if (directive.size() != 2) return WrongArgumentCount(name);
    std::size_t count = 0;
    absl::Status parsed = ParseUnsigned(directive[1], name, &count, false);
    if (!parsed.ok()) return parsed;
    if (count > kMaxReplicationSnapshotBatchSize) {
      return absl::InvalidArgumentError(
          "replication-snapshot-batch-size is out of range");
    }
    options->replication_options_.snapshot_batch_size_ = count;
    return absl::OkStatus();
  }
  if (name == "registered-buffer-mb-per-worker" ||
      name == "replication-publish-queue-mb-per-worker") {
    if (directive.size() != 2) return WrongArgumentCount(name);
    std::size_t megabytes = 0;
    absl::Status parsed = ParseUnsigned(directive[1], name, &megabytes, false);
    if (!parsed.ok()) return parsed;
    constexpr std::size_t kMiB = 1024 * 1024;
    if (megabytes > std::numeric_limits<std::size_t>::max() / kMiB) {
      return absl::OutOfRangeError(
          "replication publish queue size is too large");
    }
    if (name == "registered-buffer-mb-per-worker") {
      options->registered_buffer_bytes_ = megabytes * kMiB;
    } else {
      options->replication_publish_queue_bytes_ = megabytes * kMiB;
    }
    return absl::OkStatus();
  }
  if (name == "storage-write-buffers-per-worker") {
    if (directive.size() != 2) return WrongArgumentCount(name);
    unsigned count = 0;
    absl::Status parsed = ParseUnsigned(directive[1], name, &count, false);
    if (!parsed.ok()) return parsed;
    options->storage_write_buffer_count_ = count;
    return absl::OkStatus();
  }
  if (name == "storage-read-buffer-kb") {
    if (directive.size() != 2) return WrongArgumentCount(name);
    std::size_t kilobytes = 0;
    absl::Status parsed = ParseUnsigned(directive[1], name, &kilobytes, false);
    if (!parsed.ok()) return parsed;
    constexpr std::size_t kKiB = 1024;
    if (kilobytes > std::numeric_limits<std::size_t>::max() / kKiB) {
      return absl::OutOfRangeError("storage read buffer size is too large");
    }
    options->storage_read_buffer_bytes_ = kilobytes * kKiB;
    return absl::OkStatus();
  }
  if (name == "repl-backlog-size") {
    if (directive.size() != 2) return WrongArgumentCount(name);
    auto bytes = ParseMemorySize(directive[1]);
    if (!bytes.ok()) return bytes.status();
    if (*bytes == 0) {
      return absl::InvalidArgumentError("repl-backlog-size must be nonzero");
    }
    options->replication_options_.backlog_size_bytes_ = *bytes;
    return absl::OkStatus();
  }
  if (name == "lua-time-limit" || name == "busy-reply-threshold") {
    if (directive.size() != 2) return WrongArgumentCount(name);
    return ParseUnsigned(directive[1], name, &options->lua_time_limit_ms_,
                         true);
  }
  return absl::InvalidArgumentError(
      absl::StrCat("unsupported configuration directive '", name, "'"));
}

absl::Status ValidateServerOptions(const ServerOptions& options) {
  if (options.shard_count_ == 0 ||
      options.shard_count_ > std::numeric_limits<bycorf::WorkerId>::max() - 1) {
    return absl::InvalidArgumentError("shards exceeds runtime worker capacity");
  }
  if (options.meta_exclusive_cpu_ && !options.pin_workers_) {
    return absl::InvalidArgumentError(
        "meta-exclusive-cpu requires pin-workers");
  }
  if (!options.cpu_ids_.empty() && !options.pin_workers_) {
    return absl::InvalidArgumentError("cpus requires pin-workers");
  }
  if (options.network_backend_ != "kernel" &&
      options.network_backend_ != "dpdk")
    return absl::InvalidArgumentError("network must be kernel or dpdk");
  if (options.storage_backend_ != "uring" && options.storage_backend_ != "spdk")
    return absl::InvalidArgumentError("storage must be uring or spdk");
#ifndef LAVIK_KERNEL_BYPASS
  if (options.network_backend_ == "dpdk" || options.storage_backend_ == "spdk")
    return absl::InvalidArgumentError(
        "DPDK/SPDK support requires a build with LAVIK_KERNEL_BYPASS=ON");
#endif
  for (const auto& path : options.data_files_) {
    if (path.starts_with("spdk://") != (options.storage_backend_ == "spdk"))
      return absl::InvalidArgumentError(
          "data-file path does not match selected storage backend");
  }
  const absl::Status logging = ValidateLoggingOptions(options.logging_);
  if (!logging.ok()) return logging;
  if (options.bind_addresses_.empty()) {
    return absl::InvalidArgumentError("at least one bind address is required");
  }
  for (const std::string& address : options.bind_addresses_) {
    if (address.empty()) {
      return absl::InvalidArgumentError("bind address must not be empty");
    }
  }
  if (options.max_clients_ == 0) {
    return absl::InvalidArgumentError("maxclients must be nonzero");
  }
  if (options.client_query_buffer_limit_bytes_ <
          kMinimumClientQueryBufferLimit ||
      options.client_query_buffer_limit_bytes_ >
          static_cast<std::size_t>(std::numeric_limits<long>::max())) {
    return absl::InvalidArgumentError(
        "client-query-buffer-limit must be between 1mb and LONG_MAX bytes");
  }
  // Meta seeds are the only management switch. Client semantics are resolved
  // from committed Meta state on the startup thread before storage exists.
  if (!options.meta_seeds_.empty()) {
    if (!IsLowerHexNodeId(options.node_id_)) {
      return absl::InvalidArgumentError(
          "Meta-managed mode requires node-id as 40 lowercase "
          "hex characters");
    }
    for (const std::string& seed : options.meta_seeds_) {
      if (!IsNumericEndpoint(seed)) {
        return absl::InvalidArgumentError(
            absl::StrCat("meta-seed must be a numeric IP endpoint: ", seed));
      }
    }
    // The data-control connection reuses the replication TLS identity. An
    // opted-in mTLS session cannot silently fall back to a CA-only client.
    if (options.tls_replication_ &&
        (options.tls_ca_cert_file_.empty() || options.tls_cert_file_.empty() ||
         options.tls_key_file_.empty())) {
      return absl::InvalidArgumentError(
          "Meta data-control mTLS requires tls-ca-cert-file, tls-cert-file, "
          "and tls-key-file");
    }
    if (options.replicaof_.has_value() ||
        options.redis_replicaof_.has_value()) {
      return absl::InvalidArgumentError(
          "meta-managed cannot be combined with replicaof or "
          "redis-replicaof");
    }
    if (!options.load_rdb_file_.empty()) {
      return absl::InvalidArgumentError(
          "meta-managed cannot be combined with load-rdb");
    }
    // MOVED and discovery replies must name at least one reachable client
    // endpoint. A zero announce port follows the corresponding listen port,
    // so a TLS-only deployment (port 0, tls-port > 0) resolves a nonzero
    // announced TLS port and is valid. This runs before the generic
    // port/tls-port check below so cluster deployments get this message.
    const std::uint16_t announced_port =
        options.announce_port_ != 0 ? options.announce_port_ : options.port_;
    const std::uint16_t announced_tls_port = options.announce_tls_port_ != 0
                                                 ? options.announce_tls_port_
                                                 : options.tls_port_;
    if (announced_port == 0 && announced_tls_port == 0) {
      return absl::InvalidArgumentError(
          "meta-managed requires an announced client port: set port, "
          "tls-port, announce-port, or announce-tls-port");
    }
  }
  if (options.port_ == 0 && options.tls_port_ == 0) {
    return absl::InvalidArgumentError(
        "port and tls-port cannot both be disabled");
  }
  if (options.port_ != 0 && options.port_ == options.tls_port_) {
    return absl::InvalidArgumentError("port and tls-port must be different");
  }
  if (options.tls_port_ != 0 &&
      (options.tls_cert_file_.empty() || options.tls_key_file_.empty())) {
    return absl::InvalidArgumentError(
        "tls-port requires tls-cert-file and tls-key-file");
  }
  if ((options.tls_cert_file_.empty() != options.tls_key_file_.empty())) {
    return absl::InvalidArgumentError(
        "tls-cert-file and tls-key-file must be configured together");
  }
  if (options.tls_auth_clients_ != "no" &&
      options.tls_auth_clients_ != "optional" &&
      options.tls_auth_clients_ != "yes") {
    return absl::InvalidArgumentError(
        "tls-auth-clients must be 'no', 'optional', or 'yes'");
  }
  if ((options.tls_auth_clients_ == "yes" ||
       options.tls_auth_clients_ == "optional") &&
      options.tls_ca_cert_file_.empty()) {
    return absl::InvalidArgumentError(
        "TLS client authentication requires tls-ca-cert-file");
  }
  if (options.tls_replication_ && options.tls_ca_cert_file_.empty()) {
    return absl::InvalidArgumentError(
        "tls-replication requires tls-ca-cert-file");
  }
  if (options.masteruser_ != "default") {
    return absl::InvalidArgumentError(
        "only the default replication user is currently supported");
  }
  if (!options.load_rdb_file_.empty() && options.replicaof_.has_value()) {
    return absl::InvalidArgumentError(
        "load-rdb and replicaof cannot be configured together");
  }
  if (!options.load_rdb_file_.empty() && options.redis_replicaof_.has_value()) {
    return absl::InvalidArgumentError(
        "load-rdb and redis-replicaof cannot be configured together");
  }
  if (options.replicaof_.has_value() && options.redis_replicaof_.has_value()) {
    return absl::InvalidArgumentError(
        "replicaof and redis-replicaof cannot be configured together");
  }
  if (options.load_rdb_replace_ && options.load_rdb_file_.empty()) {
    return absl::InvalidArgumentError(
        "load-rdb-replace requires load-rdb to be configured");
  }
  if (options.rdb_dir_.empty()) {
    return absl::InvalidArgumentError("rdb-dir must not be empty");
  }
  if (options.dbfilename_.empty() || options.dbfilename_ == "." ||
      options.dbfilename_ == ".." ||
      options.dbfilename_.find('/') != std::string::npos) {
    return absl::InvalidArgumentError(
        "dbfilename must be a plain filename without '/'");
  }
  if (options.storage_write_buffer_count_ == 0) {
    return absl::InvalidArgumentError(
        "storage write buffer count must be nonzero");
  }
  if (options.shard_count_ > std::numeric_limits<std::size_t>::max() /
                                 storage::kStorageBlockBytes ||
      options.replication_options_.backlog_size_bytes_ <
          static_cast<std::size_t>(options.shard_count_) *
              storage::kStorageBlockBytes) {
    return absl::InvalidArgumentError(
        "repl-backlog-size must provide at least one 8 MiB block per worker");
  }
  return absl::OkStatus();
}

namespace {

absl::StatusOr<std::vector<unsigned>> SelectedWorkerCpus(
    const ServerOptions& options) {
  cpu_set_t allowed;
  CPU_ZERO(&allowed);
  if (::sched_getaffinity(0, sizeof(allowed), &allowed) != 0) {
    return absl::InternalError("cannot read inherited CPU affinity");
  }
  std::vector<unsigned> cpus = options.cpu_ids_;
  if (cpus.empty()) {
    for (unsigned cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
      if (CPU_ISSET(cpu, &allowed)) cpus.push_back(cpu);
    }
  }
  if (cpus.empty()) return absl::InvalidArgumentError("no allowed CPUs");
  const auto valid = [&](unsigned cpu) {
    return cpu < CPU_SETSIZE && CPU_ISSET(cpu, &allowed);
  };
  for (unsigned cpu : cpus) {
    if (!valid(cpu)) {
      return absl::InvalidArgumentError(
          absl::StrCat("CPU ", cpu, " is outside inherited affinity"));
    }
  }
  if (options.meta_exclusive_cpu_) {
    if (!options.pin_workers_)
      return absl::InvalidArgumentError(
          "meta-exclusive-cpu requires pin-workers");
    const unsigned reserved = cpus.back();
    // Repeated IDs must not sneak the reserved CPU back into the data pool.
    if (cpus.size() < 2 ||
        std::find(cpus.begin(), cpus.end() - 1, reserved) != cpus.end() - 1) {
      return absl::InvalidArgumentError(
          "meta-exclusive-cpu requires a distinct final CPU and at least two "
          "CPUs");
    }
  }
  return cpus;
}

}  // namespace

absl::Status ResolveAutomaticShardCount(ServerOptions* options) {
  if (options->shard_count_ != 0) return absl::OkStatus();
  auto cpus = SelectedWorkerCpus(*options);
  if (!cpus.ok()) return cpus.status();
  options->shard_count_ = static_cast<unsigned>(cpus->size()) -
                          static_cast<unsigned>(options->meta_exclusive_cpu_);
  return absl::OkStatus();
}

absl::StatusOr<std::vector<unsigned>> ResolveWorkerCpuIds(
    const ServerOptions& options) {
  if (!options.pin_workers_ && !options.meta_exclusive_cpu_)
    return std::vector<unsigned>{};
  auto selected = SelectedWorkerCpus(options);
  if (!selected.ok()) return selected.status();
  const auto& cpus = *selected;
  const auto data_cpu_count = cpus.size() - options.meta_exclusive_cpu_;
  std::vector<unsigned> result;
  const unsigned total = options.shard_count_ + 1;
  result.reserve(total);
  for (unsigned worker = 0; worker < options.shard_count_; ++worker) {
    result.push_back(cpus[worker % data_cpu_count]);
  }
  result.push_back(options.meta_exclusive_cpu_
                       ? cpus.back()
                       : cpus[options.shard_count_ % cpus.size()]);
  return result;
}

absl::Status LoadRedisConfigFile(const std::string& path,
                                 ServerOptions* options) {
  std::ifstream input(path);
  if (!input.is_open()) {
    return absl::NotFoundError(
        absl::StrCat("cannot open configuration file '", path, "'"));
  }

  std::string line;
  std::size_t line_number = 0;
  while (std::getline(input, line)) {
    ++line_number;
    auto directive = ParseRedisConfigLine(line);
    if (!directive.ok()) {
      return absl::InvalidArgumentError(absl::StrCat(
          path, ":", line_number, ": ", directive.status().message()));
    }
    absl::Status applied = ApplyRedisConfigDirective(*directive, options);
    if (!applied.ok()) {
      return absl::InvalidArgumentError(
          absl::StrCat(path, ":", line_number, ": ", applied.message()));
    }
  }
  if (input.bad()) {
    return absl::DataLossError(
        absl::StrCat("failed while reading configuration file '", path, "'"));
  }
  options->config_file_ = path;
  return absl::OkStatus();
}

absl::Status RewriteRedisConfigFile(const std::string& path,
                                    std::optional<ReplicaOfConfig> upstream,
                                    bool redis_upstream,
                                    unsigned replica_priority) {
  if (path.empty()) {
    return absl::FailedPreconditionError(
        "The server is running without a config file");
  }

  // CONFIG REWRITE is an administrative operation. Serialize concurrent
  // rewrites without adding synchronization to ordinary request processing.
  static std::mutex rewrite_mutex;
  const std::lock_guard lock(rewrite_mutex);

  struct stat metadata{};
  if (::stat(path.c_str(), &metadata) != 0) {
    return absl::NotFoundError(absl::StrCat("cannot stat configuration file '",
                                            path, "': ", std::strerror(errno)));
  }
  if (!S_ISREG(metadata.st_mode)) {
    return absl::FailedPreconditionError(
        absl::StrCat("configuration file is not a regular file: '", path, "'"));
  }

  std::ifstream input(path);
  if (!input.is_open()) {
    return absl::NotFoundError(
        absl::StrCat("cannot open configuration file '", path, "'"));
  }
  std::string rewritten;
  std::string line;
  std::size_t line_number = 0;
  while (std::getline(input, line)) {
    ++line_number;
    auto directive = ParseRedisConfigLine(line);
    if (!directive.ok()) {
      return absl::InvalidArgumentError(absl::StrCat(
          path, ":", line_number, ": ", directive.status().message()));
    }
    bool managed = false;
    if (!directive->empty()) {
      const std::string name = absl::AsciiStrToLower(directive->front());
      managed = name == "replicaof" || name == "slaveof" ||
                name == "redis-replicaof" || name == "replica-priority";
    }
    if (line == "# Generated by CONFIG REWRITE") managed = true;
    if (!managed) {
      rewritten += line;
      rewritten.push_back('\n');
    }
  }
  if (input.bad()) {
    return absl::DataLossError(
        absl::StrCat("failed while reading configuration file '", path, "'"));
  }

  while (!rewritten.empty() && rewritten.back() == '\n') {
    rewritten.pop_back();
  }
  if (!rewritten.empty()) rewritten += "\n\n";
  rewritten += "# Generated by CONFIG REWRITE\n";
  if (upstream.has_value()) {
    rewritten += redis_upstream ? "redis-replicaof " : "replicaof ";
    rewritten += QuoteRedisConfigArgument(upstream->host_);
    rewritten += " ";
    rewritten += std::to_string(upstream->port_);
    rewritten += "\n";
  }
  rewritten += "replica-priority ";
  rewritten += std::to_string(replica_priority);
  rewritten += "\n";

  std::string temporary_template = path + ".tmp.XXXXXX";
  std::vector<char> temporary_path(temporary_template.begin(),
                                   temporary_template.end());
  temporary_path.push_back('\0');
  const int fd = ::mkstemp(temporary_path.data());
  if (fd < 0) {
    return absl::InternalError(
        absl::StrCat("cannot create configuration temporary file for '", path,
                     "': ", std::strerror(errno)));
  }
  const std::string temporary_name(temporary_path.data());
  bool renamed = false;
  auto cleanup = [&] {
    if (!renamed) (void)::unlink(temporary_name.c_str());
  };

  absl::Status result;
  if (::fchmod(fd, metadata.st_mode & 07777) != 0) {
    result = absl::InternalError(
        absl::StrCat("cannot set configuration temporary file mode: ",
                     std::strerror(errno)));
  }
  if (result.ok()) result = WriteAll(fd, rewritten, path);
  if (result.ok() && ::fdatasync(fd) != 0) {
    result = absl::InternalError(absl::StrCat(
        "cannot sync configuration temporary file: ", std::strerror(errno)));
  }
  if (::close(fd) != 0 && result.ok()) {
    result = absl::InternalError(absl::StrCat(
        "cannot close configuration temporary file: ", std::strerror(errno)));
  }
  if (!result.ok()) {
    cleanup();
    return result;
  }
  if (::rename(temporary_name.c_str(), path.c_str()) != 0) {
    result =
        absl::InternalError(absl::StrCat("cannot replace configuration file '",
                                         path, "': ", std::strerror(errno)));
    cleanup();
    return result;
  }
  renamed = true;

  const std::string directory = ParentDirectory(path);
  const int directory_fd =
      ::open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (directory_fd < 0) {
    return absl::InternalError(
        absl::StrCat("cannot open configuration directory '", directory,
                     "': ", std::strerror(errno)));
  }
  const int sync_result = ::fsync(directory_fd);
  const int sync_error = errno;
  const int close_result = ::close(directory_fd);
  if (sync_result != 0) {
    return absl::InternalError(
        absl::StrCat("cannot sync configuration directory '", directory,
                     "': ", std::strerror(sync_error)));
  }
  if (close_result != 0) {
    return absl::InternalError(
        absl::StrCat("cannot close configuration directory '", directory,
                     "': ", std::strerror(errno)));
  }
  return absl::OkStatus();
}

}  // namespace lavik
