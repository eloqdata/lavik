// Raft-free Meta CLI for direct administrative commands and cluster readiness.

#include <signal.h>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "keylane/meta/admin_client.h"
#include "keylane/meta/cluster_status.h"
#include "keylane/numeric_endpoint.h"
#include "keylane/version.h"

namespace {

constexpr std::size_t kMaxCommandBytes = 64 * 1024;

struct Options {
  std::string socket_path_;
  std::string address_;
  std::string tls_ca_;
  std::string tls_cert_;
  std::string tls_key_;
  std::string tls_server_name_;
  int timeout_ms_ = 5000;
  bool cluster_status_ = false;
  bool json_ = false;
  bool allow_plaintext_admin_ = false;
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
      "\n"
      "Direct commands are sent to the specified Meta node as one line.\n"
      "status reports that node's state; cluster-status discovers the leader\n"
      "and reports cluster readiness. Options may precede cluster-status.\n"
      "Durability recovery uses: abortop ID, archiveoperations SEQ..., then\n"
      "exportoperations and pruneoperations SEQ....\n"
      "Exit status is 0 for an OK reply, 2 for an ERR reply, and 1 for a\n"
      "local, connection, TLS, or malformed-protocol failure.\n"
      "cluster-status exits 0 for READY, 2 for NOT READY, 3 for RETRYABLE,\n"
      "and 1 for fatal errors. TCP discovery requires mTLS or explicit\n"
      "--allow-plaintext-admin; --json applies only to cluster-status.\n",
      program, program, program);
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
      if (!options.cluster_status_ && argument == "cluster-status") {
        options.cluster_status_ = true;
        continue;
      }
      if (options.cluster_status_) {
        Fail("cluster-status does not take positional arguments");
      }
      // Direct command operands belong to the server, even when they look
      // like CLI options. Only the reserved cluster-status command continues
      // local option parsing; -- can force a verbatim direct command.
      break;
    }
    if (argument == "--help") {
      PrintUsage(argv[0]);
      *early_exit = true;
      return options;
    }
    if (argument == "--version") {
      std::cout << "keylane-meta-ctl " << keylane::kVersion << '\n';
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
        assign("--tls-server-name", &options.tls_server_name_)) {
      continue;
    }
    if (argument == "--timeout-ms" || argument.starts_with("--timeout-ms=")) {
      const std::string_view value =
          OptionValue(argc, argv, &index, argument, "--timeout-ms");
      if (!ParseInt(value, 1, 3'600'000, &options.timeout_ms_)) {
        Fail("--timeout-ms must be an integer from 1 through 3600000");
      }
      continue;
    }
    Fail("unknown option: " + std::string(argument));
  }

  for (; index < argc; ++index) options.command_.emplace_back(argv[index]);
  if (options.cluster_status_) {
    if (!options.command_.empty()) {
      Fail("cluster-status does not take positional arguments");
    }
  } else {
    if (options.command_.empty()) Fail("a Meta command is required");
    if (options.json_ || options.allow_plaintext_admin_) {
      Fail("--json and --allow-plaintext-admin apply only to cluster-status");
    }
  }
  if (options.socket_path_.empty() == options.address_.empty()) {
    Fail("exactly one of --socket and --addr is required");
  }
  const bool tls_any = !options.tls_ca_.empty() || !options.tls_cert_.empty() ||
                       !options.tls_key_.empty();
  const bool tls_all = !options.tls_ca_.empty() && !options.tls_cert_.empty() &&
                       !options.tls_key_.empty();
  if ((options.cluster_status_ || !options.address_.empty()) &&
      tls_any != tls_all) {
    Fail("--tls-ca, --tls-cert, and --tls-key must be given together");
  }
  if (options.cluster_status_) {
    if (!options.tls_server_name_.empty()) {
      Fail(
          "cluster-status verifies IP SANs and does not accept "
          "--tls-server-name");
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

int Run(const Options& options) {
  if (options.cluster_status_) return RunClusterStatus(options);
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
    std::fprintf(stderr, "keylane-meta-ctl: %s\n", error.what());
    return 1;
  }
}
