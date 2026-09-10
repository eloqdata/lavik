// Cluster-wide operator CLI. This binary is intentionally Raft-free and uses
// only the public ClusterOperator seam rather than executing another CLI.

#include <signal.h>

#include <charconv>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include "keylane/meta/cluster_status.h"
#include "keylane/numeric_endpoint.h"
#include "keylane/version.h"

namespace {

struct Options {
  std::string socket_;
  std::string address_;
  keylane::meta::MetaAdminTlsOptions tls_;
  bool allow_plaintext_admin_ = false;
  bool json_ = false;
  int timeout_ms_ = 5000;
};

[[noreturn]] void Fail(std::string message) {
  throw std::runtime_error(std::move(message));
}

void PrintUsage(const char* program) {
  std::fprintf(stderr,
               "Usage:\n"
               "  %s status (--socket PATH | --addr IP:PORT)\n"
               "     [--tls-ca FILE --tls-cert FILE --tls-key FILE]\n"
               "     [--allow-plaintext-admin] [--timeout-ms N] [--json]\n",
               program);
}

std::string_view Value(int argc, char** argv, int* index,
                       std::string_view argument, std::string_view name) {
  const std::string prefix = std::string(name) + "=";
  if (argument.starts_with(prefix)) return argument.substr(prefix.size());
  if (argument != name || *index + 1 >= argc) {
    Fail(std::string(name) + " requires a value");
  }
  return argv[++*index];
}

Options Parse(int argc, char** argv, bool* early_exit) {
  if (argc >= 2 && std::string_view(argv[1]) == "--help") {
    PrintUsage(argv[0]);
    *early_exit = true;
    return {};
  }
  if (argc >= 2 && std::string_view(argv[1]) == "--version") {
    std::cout << "keylane-cluster " << keylane::kVersion << '\n';
    *early_exit = true;
    return {};
  }
  if (argc < 2 || std::string_view(argv[1]) != "status") {
    Fail("the only supported command is status");
  }
  Options options;
  for (int index = 2; index < argc; ++index) {
    const std::string_view argument(argv[index]);
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
        *output = Value(argc, argv, &index, argument, name);
        if (output->empty()) Fail(std::string(name) + " must not be empty");
        return true;
      }
      return false;
    };
    if (assign("--socket", &options.socket_) ||
        assign("--addr", &options.address_) ||
        assign("--tls-ca", &options.tls_.ca_file_) ||
        assign("--tls-cert", &options.tls_.certificate_file_) ||
        assign("--tls-key", &options.tls_.private_key_file_)) {
      continue;
    }
    if (argument == "--timeout-ms" || argument.starts_with("--timeout-ms=")) {
      const std::string_view value =
          Value(argc, argv, &index, argument, "--timeout-ms");
      int timeout = 0;
      const auto [end, error] =
          std::from_chars(value.data(), value.data() + value.size(), timeout);
      if (error != std::errc() || end != value.data() + value.size() ||
          timeout < 1 || timeout > 3'600'000) {
        Fail("--timeout-ms must be an integer from 1 through 3600000");
      }
      options.timeout_ms_ = timeout;
      continue;
    }
    Fail("unknown option: " + std::string(argument));
  }
  if (options.socket_.empty() == options.address_.empty()) {
    Fail("exactly one of --socket and --addr is required");
  }
  const bool tls_any = !options.tls_.ca_file_.empty() ||
                       !options.tls_.certificate_file_.empty() ||
                       !options.tls_.private_key_file_.empty();
  const bool tls_all = !options.tls_.ca_file_.empty() &&
                       !options.tls_.certificate_file_.empty() &&
                       !options.tls_.private_key_file_.empty();
  if (tls_any != tls_all) {
    Fail("--tls-ca, --tls-cert, and --tls-key must be given together");
  }
  // A TLS triple with a UDS seed is meaningful: discovery may redirect to a
  // remote member, and every learned TCP endpoint then uses that identity.
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

int Run(const Options& options) {
  keylane::meta::MetaAdminTarget seed;
  if (!options.socket_.empty()) {
    seed.transport_ = keylane::meta::MetaAdminTarget::Transport::kUnix;
    seed.endpoint_ = options.socket_;
  } else {
    seed.transport_ =
        options.tls_.ca_file_.empty()
            ? keylane::meta::MetaAdminTarget::Transport::kTcpPlaintext
            : keylane::meta::MetaAdminTarget::Transport::kTcpMtls;
    seed.endpoint_ = options.address_;
    seed.tls_ = options.tls_;
  }
  keylane::meta::ClusterStatusOptions status_options;
  status_options.tls_ = options.tls_;
  status_options.allow_plaintext_admin_ = options.allow_plaintext_admin_;
  status_options.deadline_ = std::chrono::steady_clock::now() +
                             std::chrono::milliseconds(options.timeout_ms_);
  keylane::meta::ClusterOperator cluster;
  auto outcome = cluster.Status(seed, status_options);
  if (!outcome.ok()) Fail(std::string(outcome.status().message()));
  auto rendered = options.json_
                      ? keylane::meta::RenderClusterStatusJson(*outcome)
                      : keylane::meta::RenderClusterStatusText(*outcome);
  if (!rendered.ok()) Fail(std::string(rendered.status().message()));
  // Build the complete representation before the first stdout write. Fatal
  // paths above therefore leave stdout empty, including rendering failures.
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

}  // namespace

int main(int argc, char** argv) {
  (void)::signal(SIGPIPE, SIG_IGN);
  try {
    bool early_exit = false;
    Options options = Parse(argc, argv, &early_exit);
    if (early_exit) return 0;
    return Run(options);
  } catch (const std::exception& error) {
    std::fprintf(stderr, "keylane-cluster: %s\n", error.what());
    return 1;
  }
}
