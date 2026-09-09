// One-shot adapter over the shared, Raft-free Meta administrative client.

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
      "\n"
      "The command is sent as one LF-terminated Meta control-protocol line.\n"
      "Durability recovery uses: abortop ID, archiveoperations SEQ..., then\n"
      "exportoperations and pruneoperations SEQ....\n"
      "Exit status is 0 for an OK reply, 2 for an ERR reply, and 1 for a\n"
      "local, connection, TLS, or malformed-protocol failure.\n",
      program, program);
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
    if (!argument.starts_with("--")) break;
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
  if (options.command_.empty()) Fail("a Meta command is required");
  if (options.socket_path_.empty() == options.address_.empty()) {
    Fail("exactly one of --socket and --addr is required");
  }
  const bool tls_any = !options.tls_ca_.empty() || !options.tls_cert_.empty() ||
                       !options.tls_key_.empty();
  const bool tls_all = !options.tls_ca_.empty() && !options.tls_cert_.empty() &&
                       !options.tls_key_.empty();
  if (!options.address_.empty() && tls_any != tls_all) {
    Fail("--tls-ca, --tls-cert, and --tls-key must be given together");
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

int Run(const Options& options) {
  keylane::meta::MetaAdminTarget target;
  if (!options.socket_path_.empty()) {
    target.transport_ = keylane::meta::MetaAdminTarget::Transport::kUnix;
    target.endpoint_ = options.socket_path_;
  } else {
    target.transport_ = options.tls_ca_.empty()
                            ? keylane::meta::MetaAdminTarget::Transport::kTcpPlaintext
                            : keylane::meta::MetaAdminTarget::Transport::kTcpMtls;
    target.endpoint_ = options.address_;
    target.tls_.ca_file_ = options.tls_ca_;
    target.tls_.certificate_file_ = options.tls_cert_;
    target.tls_.private_key_file_ = options.tls_key_;
    target.tls_.server_name_ = options.tls_server_name_;
  }
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(options.timeout_ms_);
  keylane::meta::MetaAdminClient client;
  auto reply = client.RoundTrip(target, BuildCommand(options.command_), deadline);
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
