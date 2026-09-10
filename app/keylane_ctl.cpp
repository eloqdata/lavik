// Raft-free Meta CLI for direct commands, cluster readiness, and first-cluster
// creation. Multi-step orchestration stays behind ClusterOperator/Meta Admin.

#include <fcntl.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "keylane/meta/admin_client.h"
#include "keylane/meta/cluster_create.h"
#include "keylane/meta/cluster_status.h"
#include "keylane/numeric_endpoint.h"
#include "keylane/version.h"

namespace {

constexpr std::size_t kMaxCommandBytes = 64 * 1024;

[[noreturn]] void Fail(std::string message);

struct RedisCliResult {
  int exit_code_ = -1;
  std::string output_;
  bool timed_out_ = false;
};

class ClusterCreateTimeout final : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

RedisCliResult RunProcess(std::vector<std::string> arguments,
                          std::chrono::steady_clock::time_point deadline) {
  int output_pipe[2];
  if (::pipe(output_pipe) != 0) Fail("cannot create redis-cli output pipe");
  std::vector<char*> argv;
  argv.reserve(arguments.size() + 1);
  for (std::string& argument : arguments) argv.push_back(argument.data());
  argv.push_back(nullptr);

  const pid_t child = ::fork();
  if (child < 0) {
    (void)::close(output_pipe[0]);
    (void)::close(output_pipe[1]);
    Fail("cannot start redis-cli");
  }
  if (child == 0) {
    (void)::close(output_pipe[0]);
    if (::dup2(output_pipe[1], STDOUT_FILENO) < 0 ||
        ::dup2(output_pipe[1], STDERR_FILENO) < 0) {
      _exit(126);
    }
    (void)::close(output_pipe[1]);
    ::execvp(argv.front(), argv.data());
    _exit(errno == ENOENT ? 127 : 126);
  }
  (void)::close(output_pipe[1]);
  const int flags = ::fcntl(output_pipe[0], F_GETFL, 0);
  if (flags < 0 || ::fcntl(output_pipe[0], F_SETFL, flags | O_NONBLOCK) < 0) {
    (void)::kill(child, SIGKILL);
    while (::waitpid(child, nullptr, 0) < 0 && errno == EINTR) {
    }
    (void)::close(output_pipe[0]);
    Fail("cannot make redis-cli output nonblocking");
  }

  RedisCliResult result;
  int status = 0;
  bool child_exited = false;
  bool output_closed = false;
  std::array<char, 4096> buffer{};
  while (!child_exited || !output_closed) {
    for (;;) {
      const ssize_t bytes =
          ::read(output_pipe[0], buffer.data(), buffer.size());
      if (bytes > 0) {
        if (result.output_.size() + static_cast<std::size_t>(bytes) >
            kMaxCommandBytes) {
          if (!child_exited) {
            (void)::kill(child, SIGKILL);
            while (::waitpid(child, nullptr, 0) < 0 && errno == EINTR) {
            }
          }
          (void)::close(output_pipe[0]);
          Fail("redis-cli output exceeds 64 KiB");
        }
        result.output_.append(buffer.data(), static_cast<std::size_t>(bytes));
        continue;
      }
      if (bytes == 0) output_closed = true;
      if (bytes < 0 && errno == EINTR) continue;
      break;
    }
    if (!child_exited) {
      const pid_t waited = ::waitpid(child, &status, WNOHANG);
      if (waited == child) {
        child_exited = true;
      } else if (waited < 0 && errno != EINTR) {
        (void)::kill(child, SIGKILL);
        while (::waitpid(child, nullptr, 0) < 0 && errno == EINTR) {
        }
        (void)::close(output_pipe[0]);
        Fail("waitpid failed for redis-cli");
      }
    }
    // Descendants may inherit stdout/stderr even after redis-cli itself exits.
    // The one absolute deadline therefore bounds pipe drain as well as the
    // direct child; otherwise a leaked descriptor can hang post-commit
    // verification forever.
    if (std::chrono::steady_clock::now() >= deadline) {
      result.timed_out_ = true;
      if (!child_exited) {
        (void)::kill(child, SIGKILL);
        while (::waitpid(child, &status, 0) < 0 && errno == EINTR) {
        }
        child_exited = true;
      }
      output_closed = true;
    }
    if (!child_exited || !output_closed) {
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
  }
  (void)::close(output_pipe[0]);
  if (WIFEXITED(status)) result.exit_code_ = WEXITSTATUS(status);
  return result;
}

std::string TrimLineEnd(std::string value) {
  while (!value.empty() && (value.back() == '\n' || value.back() == '\r')) {
    value.pop_back();
  }
  return value;
}

RedisCliResult RunRedisCli(
    const keylane::NumericEndpoint& endpoint,
    std::initializer_list<std::string_view> command,
    std::chrono::steady_clock::time_point deadline) {
  std::vector<std::string> arguments{
      "redis-cli", "-c", "--raw", "-h", endpoint.host_,
      "-p", std::to_string(endpoint.port_)};
  for (std::string_view argument : command) arguments.emplace_back(argument);
  return RunProcess(std::move(arguments), deadline);
}

std::uint16_t RedisKeySlot(std::string_view key) {
  const std::size_t open = key.find('{');
  if (open != std::string_view::npos) {
    const std::size_t close = key.find('}', open + 1);
    if (close != std::string_view::npos && close != open + 1) {
      key = key.substr(open + 1, close - open - 1);
    }
  }
  std::uint16_t crc = 0;
  for (const unsigned char byte : key) {
    crc = static_cast<std::uint16_t>(crc ^ (byte << 8));
    for (unsigned bit = 0; bit < 8; ++bit) {
      crc = static_cast<std::uint16_t>(
          (crc & 0x8000U) != 0 ? (crc << 1) ^ 0x1021U : crc << 1);
    }
  }
  return static_cast<std::uint16_t>(crc & 0x3fffU);
}

std::string RequireRedisCli(
    const keylane::NumericEndpoint& endpoint,
    std::initializer_list<std::string_view> command,
    std::chrono::steady_clock::time_point deadline, std::string_view check) {
  RedisCliResult result = RunRedisCli(endpoint, command, deadline);
  if (result.timed_out_) {
    throw ClusterCreateTimeout(std::string(check) + " timed out");
  }
  if (result.exit_code_ != 0) {
    Fail(std::string(check) + " failed: " + TrimLineEnd(result.output_));
  }
  return TrimLineEnd(std::move(result.output_));
}

struct Options {
  std::string socket_path_;
  std::string address_;
  std::string tls_ca_;
  std::string tls_cert_;
  std::string tls_key_;
  std::string tls_server_name_;
  int timeout_ms_ = 5000;
  bool timeout_explicit_ = false;
  bool cluster_status_ = false;
  bool cluster_create_ = false;
  bool json_ = false;
  bool yes_ = false;
  bool allow_plaintext_admin_ = false;
  std::string manifest_path_;
  std::vector<std::string> command_;
};

[[noreturn]] void Fail(std::string message) {
  throw std::runtime_error(std::move(message));
}

void PrintUsage(const char* program) {
  std::fprintf(
      stderr,
      "Usage:\n"
      "  %s --socket PATH [--timeout-ms N] COMMAND [ARG...]\n"
      "  %s --addr IP:PORT [--tls-ca FILE --tls-cert FILE --tls-key FILE]\n"
      "     [--tls-server-name NAME] [--timeout-ms N] COMMAND [ARG...]\n"
      "  %s cluster-status (--socket PATH | --addr IP:PORT)\n"
      "     [--tls-ca FILE --tls-cert FILE --tls-key FILE]\n"
      "     [--allow-plaintext-admin] [--timeout-ms N] [--json]\n"
      "  %s cluster-create --manifest FILE (--socket PATH | --addr IP:PORT)\n"
      "     [--tls-ca FILE --tls-cert FILE --tls-key FILE]\n"
      "     [--allow-plaintext-admin] [--timeout-ms N] [--yes]\n"
      "\n"
      "Direct commands are sent to the specified Meta node as one line.\n"
      "status reports that node's state; cluster-status discovers the leader\n"
      "and reports cluster readiness. cluster-create creates the v1 single-\n"
      "Data topology and verifies it with redis-cli. Options may precede\n"
      "either local cluster command.\n"
      "Durability recovery uses: abortop ID, archiveoperations SEQ..., then\n"
      "exportoperations and pruneoperations SEQ....\n"
      "Exit status is 0 for an OK reply, 2 for an ERR reply, and 1 for a\n"
      "local, connection, TLS, or malformed-protocol failure.\n"
      "cluster-status exits 0 for READY, 2 for NOT READY, 3 for RETRYABLE,\n"
      "and 1 for fatal errors. TCP discovery requires mTLS or explicit\n"
      "--allow-plaintext-admin; --json applies only to cluster-status.\n"
      "cluster-create exits 0 after Redis verification, 2 for an explicit\n"
      "Meta rejection, 3 for a possibly committed interruption/timeout, and\n"
      "1 for local manifest, confirmation, dependency, or protocol errors.\n",
      program, program, program, program);
}

bool ParseInt(std::string_view text, int min, int max, int* result) {
  int value = 0;
  const auto [end, error] =
      std::from_chars(text.data(), text.data() + text.size(), value);
  if (error != std::errc() || end != text.data() + text.size() || value < min ||
      value > max) {
    return false;
  }
  *result = value;
  return true;
}

std::string_view OptionValue(int argc, char** argv, int* index,
                             std::string_view argument,
                             std::string_view option) {
  const std::string prefix = std::string(option) + "=";
  if (argument.starts_with(prefix)) return argument.substr(prefix.size());
  if (argument != option || *index + 1 >= argc) {
    Fail(std::string(option) + " requires a value");
  }
  ++*index;
  return argv[*index];
}

Options ParseOptions(int argc, char** argv, bool* early_exit) {
  Options options;
  int index = 1;
  for (; index < argc; ++index) {
    const std::string_view argument(argv[index]);
    if (argument == "--") {
      ++index;
      break;
    }
    if (!argument.starts_with("--")) {
      if (!options.cluster_status_ && !options.cluster_create_ &&
          argument == "cluster-status") {
        options.cluster_status_ = true;
        continue;
      }
      if (!options.cluster_status_ && !options.cluster_create_ &&
          argument == "cluster-create") {
        options.cluster_create_ = true;
        continue;
      }
      if (options.cluster_status_ || options.cluster_create_) {
        Fail(std::string(options.cluster_status_ ? "cluster-status"
                                                 : "cluster-create") +
             " does not take positional arguments");
      }
      // Direct command operands belong to the server, even when they look
      // like CLI options. Reserved local cluster commands continue option
      // parsing; -- can force a verbatim direct command.
      break;
    }
    if (argument == "--help") {
      PrintUsage(argv[0]);
      *early_exit = true;
      return options;
    }
    if (argument == "--version") {
      std::cout << "keylane-ctl " << keylane::kVersion << '\n';
      *early_exit = true;
      return options;
    }
    if (argument == "--json") {
      options.json_ = true;
      continue;
    }
    if (argument == "--allow-plaintext-admin") {
      options.allow_plaintext_admin_ = true;
      continue;
    }
    if (argument == "--yes") {
      options.yes_ = true;
      continue;
    }
    auto assign = [&](std::string_view name, std::string* output) {
      const std::string prefix = std::string(name) + "=";
      if (argument == name || argument.starts_with(prefix)) {
        *output = OptionValue(argc, argv, &index, argument, name);
        if (output->empty()) Fail(std::string(name) + " must not be empty");
        return true;
      }
      return false;
    };
    if (assign("--socket", &options.socket_path_) ||
        assign("--addr", &options.address_) ||
        assign("--tls-ca", &options.tls_ca_) ||
        assign("--tls-cert", &options.tls_cert_) ||
        assign("--tls-key", &options.tls_key_) ||
        assign("--tls-server-name", &options.tls_server_name_) ||
        assign("--manifest", &options.manifest_path_)) {
      continue;
    }
    if (argument == "--timeout-ms" || argument.starts_with("--timeout-ms=")) {
      const std::string_view value =
          OptionValue(argc, argv, &index, argument, "--timeout-ms");
      if (!ParseInt(value, 1, 3'600'000, &options.timeout_ms_)) {
        Fail("--timeout-ms must be an integer from 1 through 3600000");
      }
      options.timeout_explicit_ = true;
      continue;
    }
    Fail("unknown option: " + std::string(argument));
  }

  for (; index < argc; ++index) options.command_.emplace_back(argv[index]);
  if (options.cluster_status_ || options.cluster_create_) {
    if (!options.command_.empty()) {
      Fail(std::string(options.cluster_status_ ? "cluster-status"
                                               : "cluster-create") +
           " does not take positional arguments");
    }
    if (options.cluster_create_ && options.manifest_path_.empty()) {
      Fail("cluster-create requires --manifest FILE");
    }
    if (options.cluster_status_ && !options.manifest_path_.empty()) {
      Fail("--manifest applies only to cluster-create");
    }
    if (options.cluster_status_ && options.yes_) {
      Fail("--yes applies only to cluster-create");
    }
  } else {
    if (options.command_.empty()) Fail("a Meta command is required");
    if (options.json_ || options.allow_plaintext_admin_ || options.yes_ ||
        !options.manifest_path_.empty()) {
      Fail("cluster-only options cannot be used with a direct command");
    }
  }
  if (options.socket_path_.empty() == options.address_.empty()) {
    Fail("exactly one of --socket and --addr is required");
  }
  const bool tls_any = !options.tls_ca_.empty() || !options.tls_cert_.empty() ||
                       !options.tls_key_.empty();
  const bool tls_all = !options.tls_ca_.empty() && !options.tls_cert_.empty() &&
                       !options.tls_key_.empty();
  if ((options.cluster_status_ || options.cluster_create_ ||
       !options.address_.empty()) &&
      tls_any != tls_all) {
    Fail("--tls-ca, --tls-cert, and --tls-key must be given together");
  }
  if (options.cluster_status_ || options.cluster_create_) {
    if (!options.tls_server_name_.empty()) {
      Fail("cluster-status/cluster-create does not accept --tls-server-name; "
           "discovered Meta endpoints are verified by IP SAN");
    }
    // With a Unix seed, TLS credentials authorize any discovered remote
    // leader. Direct Unix commands have no redirect and reject TLS options.
    if (!options.address_.empty()) {
      auto endpoint = keylane::ParseNumericEndpoint(options.address_);
      if (!endpoint.has_value()) {
        Fail("--addr must be numeric IPv4:port or [IPv6]:port");
      }
      options.address_ = keylane::FormatNumericEndpoint(*endpoint);
      if (!tls_all && !options.allow_plaintext_admin_) {
        Fail("plaintext TCP admin requires --allow-plaintext-admin");
      }
    }
    if (options.cluster_create_ && options.json_) {
      Fail("--json applies only to cluster-status");
    }
    if (options.cluster_create_ && !options.timeout_explicit_) {
      options.timeout_ms_ = 120000;
    }
    return options;
  }
  if (!options.tls_server_name_.empty() && !tls_all) {
    Fail("--tls-server-name requires TLS options");
  }
  if (!options.socket_path_.empty() &&
      (tls_any || !options.tls_server_name_.empty())) {
    Fail("TLS options apply only to --addr");
  }
  return options;
}

std::string BuildCommand(const std::vector<std::string>& arguments) {
  std::string command;
  for (const std::string& argument : arguments) {
    if (argument.empty() ||
        std::any_of(argument.begin(), argument.end(),
                    [](unsigned char ch) { return std::isspace(ch) != 0; })) {
      Fail("command arguments must be non-empty and whitespace-free");
    }
    if (!command.empty()) command.push_back(' ');
    command.append(argument);
  }
  if (command.size() + 1 > kMaxCommandBytes) {
    Fail("command exceeds 64 KiB limit");
  }
  return command;
}

bool IsReply(std::string_view reply, std::string_view prefix) {
  return reply == prefix ||
         (reply.starts_with(prefix) && reply.size() > prefix.size() &&
          reply[prefix.size()] == ' ');
}

keylane::meta::MetaAdminTarget AdminTarget(const Options& options) {
  keylane::meta::MetaAdminTarget target;
  if (!options.socket_path_.empty()) {
    target.transport_ = keylane::meta::MetaAdminTarget::Transport::kUnix;
    target.endpoint_ = options.socket_path_;
  } else {
    target.transport_ =
        options.tls_ca_.empty()
            ? keylane::meta::MetaAdminTarget::Transport::kTcpPlaintext
            : keylane::meta::MetaAdminTarget::Transport::kTcpMtls;
    target.endpoint_ = options.address_;
    target.tls_.ca_file_ = options.tls_ca_;
    target.tls_.certificate_file_ = options.tls_cert_;
    target.tls_.private_key_file_ = options.tls_key_;
    target.tls_.server_name_ = options.tls_server_name_;
  }
  return target;
}

int RunClusterStatus(const Options& options) {
  keylane::meta::ClusterStatusOptions status_options;
  status_options.tls_.ca_file_ = options.tls_ca_;
  status_options.tls_.certificate_file_ = options.tls_cert_;
  status_options.tls_.private_key_file_ = options.tls_key_;
  status_options.allow_plaintext_admin_ = options.allow_plaintext_admin_;
  status_options.deadline_ = std::chrono::steady_clock::now() +
                             std::chrono::milliseconds(options.timeout_ms_);
  keylane::meta::ClusterOperator cluster;
  auto outcome = cluster.Status(AdminTarget(options), status_options);
  if (!outcome.ok()) Fail(std::string(outcome.status().message()));
  auto rendered = options.json_
                      ? keylane::meta::RenderClusterStatusJson(*outcome)
                      : keylane::meta::RenderClusterStatusText(*outcome);
  if (!rendered.ok()) Fail(std::string(rendered.status().message()));
  // Render completely before writing so fatal paths leave stdout empty.
  std::string output = std::move(*rendered);
  if (output.empty() || output.back() != '\n') output.push_back('\n');
  if (std::fwrite(output.data(), output.size(), 1, stdout) != 1) {
    Fail("failed to write stdout");
  }
  switch (outcome->result_) {
    case keylane::meta::ClusterStatusResult::kReady:
      return 0;
    case keylane::meta::ClusterStatusResult::kNotReady:
      return 2;
    case keylane::meta::ClusterStatusResult::kRetryable:
      return 3;
  }
  return 1;
}

std::string ReadManifest(const std::string& path) {
  std::ifstream input(path, std::ios::binary);
  if (!input) Fail("cannot open manifest: " + path);
  std::string contents(64 * 1024 + 1, '\0');
  input.read(contents.data(), static_cast<std::streamsize>(contents.size()));
  contents.resize(static_cast<std::size_t>(input.gcount()));
  if (input.bad()) Fail("failed to read manifest: " + path);
  if (contents.size() > 64 * 1024) {
    Fail("cluster manifest exceeds 64 KiB");
  }
  return contents;
}

void VerifyClusterWithRedisCli(
    const keylane::meta::ClusterCreateManifestV1& manifest,
    std::chrono::steady_clock::time_point deadline) {
  RedisCliResult version =
      RunProcess({"redis-cli", "--version"}, deadline);
  if (version.timed_out_) {
    throw ClusterCreateTimeout("redis-cli PATH preflight timed out");
  }
  if (version.exit_code_ == 127) {
    Fail("redis-cli was not found on PATH");
  }
  if (version.exit_code_ != 0) {
    Fail("redis-cli PATH preflight failed: " +
         TrimLineEnd(std::move(version.output_)));
  }

  constexpr std::string_view kTcpPrefix = "tcp://";
  auto endpoint = keylane::ParseNumericEndpoint(
      std::string_view(manifest.client_endpoint_).substr(kTcpPrefix.size()));
  if (!endpoint.has_value()) Fail("manifest Data endpoint became invalid");

  const std::string info = RequireRedisCli(
      *endpoint, {"CLUSTER", "INFO"}, deadline, "CLUSTER INFO");
  if (info.find("cluster_state:ok") == std::string::npos) {
    Fail("CLUSTER INFO did not report cluster_state:ok");
  }

  const std::string slots = RequireRedisCli(
      *endpoint, {"CLUSTER", "SLOTS"}, deadline, "CLUSTER SLOTS");
  std::vector<std::string_view> slot_lines;
  std::string_view remaining = slots;
  while (!remaining.empty()) {
    const std::size_t newline = remaining.find('\n');
    std::string_view line = remaining.substr(0, newline);
    if (!line.empty() && line.back() == '\r') line.remove_suffix(1);
    slot_lines.push_back(line);
    if (newline == std::string_view::npos) break;
    remaining.remove_prefix(newline + 1);
  }
  if (slot_lines.size() != 5 || slot_lines[0] != "0" ||
      slot_lines[1] != "16383" || slot_lines[2] != endpoint->host_ ||
      slot_lines[3] != std::to_string(endpoint->port_) ||
      slot_lines[4] != manifest.data_node_id_) {
    Fail("CLUSTER SLOTS did not return the requested full range and endpoint");
  }

  const std::string probe_key =
      "keylane:cluster-create:" + std::to_string(::getpid()) + ":" +
      std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
  const std::string expected_slot = std::to_string(RedisKeySlot(probe_key));
  const std::string observed_slot = RequireRedisCli(
      *endpoint, {"CLUSTER", "KEYSLOT", probe_key}, deadline,
      "CLUSTER KEYSLOT");
  if (observed_slot != expected_slot) {
    Fail("CLUSTER KEYSLOT disagreed with the Keylane CRC16 calculation");
  }

  const std::string probe_value = "keylane-cluster-create-ok";
  const std::string set = RequireRedisCli(
      *endpoint, {"SET", probe_key, probe_value}, deadline, "SET probe");
  if (set != "OK") {
    (void)RunRedisCli(*endpoint, {"DEL", probe_key}, deadline);
    Fail("SET probe returned an unexpected reply");
  }
  std::string get;
  try {
    get = RequireRedisCli(*endpoint, {"GET", probe_key}, deadline,
                          "GET probe");
  } catch (...) {
    // SET may have committed even when a later verification command fails.
    // Cleanup is best-effort here so the original diagnostic and exit
    // classification remain intact.
    (void)RunRedisCli(*endpoint, {"DEL", probe_key}, deadline);
    throw;
  }
  const std::string deleted = RequireRedisCli(
      *endpoint, {"DEL", probe_key}, deadline, "DEL probe cleanup");
  if (get != probe_value || deleted != "1") {
    Fail("redis-cli write/read/cleanup probe did not round-trip exactly");
  }
}

int RunClusterCreate(const Options& options) {
  auto manifest = keylane::meta::ParseClusterCreateManifest(
      ReadManifest(options.manifest_path_));
  if (!manifest.ok()) Fail(std::string(manifest.status().message()));

  std::cout << "Cluster create plan (schema v1)\n"
            << "  Meta member: " << manifest->meta_member_id_ << '\n'
            << "  Data node: " << manifest->data_node_id_ << '\n'
            << "  Client endpoint: " << manifest->client_endpoint_ << '\n'
            << "  Group/primary: " << manifest->group_id_ << " / "
            << manifest->primary_node_id_ << '\n'
            << "  Slots: " << manifest->first_slot_ << '-'
            << manifest->last_slot_
            << "\nWARNING: existing data on the Data node will be erased.\n";
  if (!options.yes_) {
    std::cout << "Type yes to continue: " << std::flush;
    std::string confirmation;
    if (!std::getline(std::cin, confirmation) || confirmation != "yes") {
      std::cerr << "keylane-ctl: cluster creation cancelled; no mutation was sent\n";
      return 1;
    }
  }

  keylane::meta::ClusterStatusOptions create_options;
  create_options.tls_.ca_file_ = options.tls_ca_;
  create_options.tls_.certificate_file_ = options.tls_cert_;
  create_options.tls_.private_key_file_ = options.tls_key_;
  create_options.allow_plaintext_admin_ = options.allow_plaintext_admin_;
  create_options.deadline_ = std::chrono::steady_clock::now() +
                             std::chrono::milliseconds(options.timeout_ms_);
  keylane::meta::ClusterOperator cluster;
  auto outcome = cluster.Create(AdminTarget(options), *manifest, create_options);
  if (!outcome.ok()) {
    std::cerr << "keylane-ctl: " << outcome.status().message() << '\n';
    if (outcome.status().code() == absl::StatusCode::kFailedPrecondition) {
      return 2;
    }
    if (outcome.status().code() == absl::StatusCode::kDeadlineExceeded ||
        outcome.status().code() == absl::StatusCode::kUnavailable ||
        outcome.status().code() == absl::StatusCode::kAborted) {
      std::cerr << "keylane-ctl: creation may be partially committed; run "
                   "cluster-status before taking further action\n";
      return 3;
    }
    return 1;
  }
  try {
    VerifyClusterWithRedisCli(*manifest, create_options.deadline_);
  } catch (const ClusterCreateTimeout& error) {
    std::cerr << "keylane-ctl: " << error.what() << '\n'
              << "keylane-ctl: creation committed but verification timed out; "
                 "run cluster-status before taking further action\n";
    return 3;
  }
  std::cout << "Cluster READY: committed=" << outcome->committed_index_
            << " operation=" << outcome->operation_id_ << '\n';
  return 0;
}

int Run(const Options& options) {
  if (options.cluster_status_) return RunClusterStatus(options);
  if (options.cluster_create_) return RunClusterCreate(options);
  const auto target = AdminTarget(options);
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(options.timeout_ms_);
  keylane::meta::MetaAdminClient client;
  auto reply =
      client.RoundTrip(target, BuildCommand(options.command_), deadline);
  if (!reply.ok()) Fail(std::string(reply.status().message()));
  std::cout << *reply << '\n';
  if (IsReply(*reply, "OK")) return 0;
  if (IsReply(*reply, "ERR")) return 2;
  Fail("server returned a malformed reply");
}

}  // namespace

int main(int argc, char** argv) {
  (void)::signal(SIGPIPE, SIG_IGN);
  try {
    bool early_exit = false;
    const Options options = ParseOptions(argc, argv, &early_exit);
    if (early_exit) return 0;
    return Run(options);
  } catch (const std::exception& error) {
    std::fprintf(stderr, "keylane-ctl: %s\n", error.what());
    return 1;
  }
}
