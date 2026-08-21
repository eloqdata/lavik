#include "keylane/config.h"

#include <cctype>
#include <charconv>
#include <cstdint>
#include <fstream>
#include <limits>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"

namespace keylane {
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
  if (name == "tls-cert-file" || name == "tls-key-file" ||
      name == "tls-ca-cert-file" || name == "requirepass" ||
      name == "masteruser" || name == "masterauth") {
    if (directive.size() != 2) return WrongArgumentCount(name);
    if (name == "tls-cert-file") options->tls_cert_file_ = directive[1];
    if (name == "tls-key-file") options->tls_key_file_ = directive[1];
    if (name == "tls-ca-cert-file") options->tls_ca_cert_file_ = directive[1];
    if (name == "requirepass") options->requirepass_ = directive[1];
    if (name == "masteruser") options->masteruser_ = directive[1];
    if (name == "masterauth") options->masterauth_ = directive[1];
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
  if (name == "threads" || name == "io-threads") {
    if (directive.size() != 2) return WrongArgumentCount(name);
    return ParseUnsigned(directive[1], name, &options->thread_count_, false);
  }
  if (name == "replicaof") {
    if (directive.size() != 3) return WrongArgumentCount(name);
    if (directive[1].empty()) {
      return absl::InvalidArgumentError("replicaof host must not be empty");
    }
    std::uint16_t port = 0;
    absl::Status parsed =
        ParseUnsigned(directive[2], "replicaof port", &port, false);
    if (!parsed.ok()) return parsed;
    options->replicaof_ = ReplicaOfConfig{directive[1], port};
    return absl::OkStatus();
  }
  if (name == "replica-read-only") {
    if (directive.size() != 2) return WrongArgumentCount(name);
    auto read_only = ParseYesNo(directive[1], name);
    if (!read_only.ok()) return read_only.status();
    options->replication_options_.replica_read_only_ = *read_only;
    return absl::OkStatus();
  }
  if (name == "recv-buffers-per-worker") {
    if (directive.size() != 2) return WrongArgumentCount(name);
    return ParseUnsigned(directive[1], name, &options->recv_buffer_count_,
                         true);
  }
  if (name == "foreground-budget-us" || name == "background-budget-us" ||
      name == "background-warrant-percent" ||
      name == "spdk-max-completions-per-poll") {
    if (directive.size() != 2) return WrongArgumentCount(name);
    unsigned value = 0;
    const bool allow_zero = name == "spdk-max-completions-per-poll";
    absl::Status parsed =
        ParseUnsigned(directive[1], name, &value, allow_zero);
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
  return absl::InvalidArgumentError(
      absl::StrCat("unsupported configuration directive '", name, "'"));
}

absl::Status ValidateServerOptions(const ServerOptions& options) {
  if (options.bind_addresses_.empty()) {
    return absl::InvalidArgumentError("at least one bind address is required");
  }
  for (const std::string& address : options.bind_addresses_) {
    if (address.empty()) {
      return absl::InvalidArgumentError("bind address must not be empty");
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
  if (options.storage_write_buffer_count_ == 0) {
    return absl::InvalidArgumentError(
        "storage write buffer count must be nonzero");
  }
  if (options.thread_count_ > std::numeric_limits<std::size_t>::max() /
                                  storage::kStorageBlockBytes ||
      options.replication_options_.backlog_size_bytes_ <
          static_cast<std::size_t>(options.thread_count_) *
              storage::kStorageBlockBytes) {
    return absl::InvalidArgumentError(
        "repl-backlog-size must provide at least one 8 MiB block per worker");
  }
  return absl::OkStatus();
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

}  // namespace keylane
