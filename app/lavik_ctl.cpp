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

// Raft-free Meta CLI for direct commands, cluster readiness, first-cluster
// creation, and controlled failover admission. Multi-step orchestration stays
// behind the operator APIs and committed Meta workflows.

#include <signal.h>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "lavik/meta/admin_client.h"
#include "lavik/meta/cluster_create.h"
#include "lavik/meta/cluster_status.h"
#include "lavik/numeric_endpoint.h"
#include "lavik/version.h"

namespace {

constexpr std::size_t kMaxCommandBytes = 64 * 1024;

[[noreturn]] void Fail(std::string message);

struct StatusFailureGuidance {
  std::string_view explanation_;
  std::string_view next_action_;
};

StatusFailureGuidance ExplainStatusFailure(const absl::Status& status) {
  switch (status.code()) {
    case absl::StatusCode::kInvalidArgument:
    case absl::StatusCode::kFailedPrecondition:
      return {"the local status request or protocol configuration is invalid",
              "check the endpoint, transport flags, and CLI/server versions; "
              "then retry cluster-status"};
    case absl::StatusCode::kUnauthenticated:
    case absl::StatusCode::kPermissionDenied:
      return {"Meta rejected the Admin connection identity",
              "check the CA, client certificate, key, server identity, and "
              "Admin authorization before retrying"};
    case absl::StatusCode::kDataLoss:
      return {"the status reply was truncated, incompatible, or corrupt",
              "preserve Meta data; compare CLI/server versions and inspect "
              "transport and Meta logs before retrying"};
    case absl::StatusCode::kCancelled:
    case absl::StatusCode::kDeadlineExceeded:
    case absl::StatusCode::kUnavailable:
    case absl::StatusCode::kAborted:
      return {"no trustworthy Meta status cut was available",
              "verify Meta Admin connectivity and quorum; then retry "
              "cluster-status"};
    default:
      return {"cluster-status failed before a trustworthy cut was available",
              "preserve the error details and inspect Meta logs before "
              "retrying"};
  }
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
  bool failover_ = false;
  bool json_ = false;
  bool yes_ = false;
  bool allow_plaintext_admin_ = false;
  std::string manifest_path_;
  std::string failover_group_;
  int failover_timeout_ms_ = 120'000;
  bool failover_timeout_explicit_ = false;
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
      "  %s failover GROUP (--socket PATH | --addr IP:PORT)\n"
      "     [--tls-ca FILE --tls-cert FILE --tls-key FILE]\n"
      "     [--allow-plaintext-admin] [--timeout-ms N]\n"
      "     [--failover-timeout-ms N]\n"
      "\n"
      "Recovery: --socket PATH promote GROUP --node NODE --accept-data-loss\n"
      "Select one readable recovered population when no automatic candidate "
      "exists.\n"
      "Direct commands are sent to the specified Meta node as one line.\n"
      "status reports that node's state; cluster-status discovers the leader\n"
      "and reports cluster readiness. cluster-create creates the v1 multi-\n"
      "Data, multi-Group topology and returns after its Genesis commit. Use\n"
      "cluster-status to follow creation and serving readiness. Options may\n"
      "precede\n"
      "any local cluster command.\n"
      "Durability recovery uses: abortop ID, archiveoperations SEQ..., then\n"
      "exportoperations and pruneoperations SEQ....\n"
      "Exit status is 0 for an OK reply, 2 for an ERR reply, and 1 for a\n"
      "local, connection, TLS, or malformed-protocol failure.\n"
      "cluster-status exits 0 for READY, 2 for NOT READY, 3 for RETRYABLE,\n"
      "and 1 for fatal errors. TCP discovery requires mTLS or explicit\n"
      "--allow-plaintext-admin; --json applies only to cluster-status.\n"
      "cluster-create exits 0 after Genesis commit, 2 for an explicit\n"
      "Meta rejection, 3 for a possibly committed interruption/timeout, and\n"
      "1 for local manifest, confirmation, or pre-mutation transport errors.\n"
      "failover exits 0 after request commit, 2 for an explicit rejection,\n"
      "3 for an uncertain proposal outcome, and 1 for local/transport "
      "errors known to occur before submission.\n",
      program, program, program, program, program);
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
          !options.failover_ && argument == "cluster-status") {
        options.cluster_status_ = true;
        continue;
      }
      if (!options.cluster_status_ && !options.cluster_create_ &&
          !options.failover_ && argument == "cluster-create") {
        options.cluster_create_ = true;
        continue;
      }
      if (!options.cluster_status_ && !options.cluster_create_ &&
          !options.failover_ && argument == "failover") {
        options.failover_ = true;
        continue;
      }
      if (options.failover_ && options.failover_group_.empty()) {
        options.failover_group_ = std::string(argument);
        continue;
      }
      if (options.cluster_status_ || options.cluster_create_ ||
          options.failover_) {
        Fail(std::string(options.cluster_status_   ? "cluster-status"
                         : options.cluster_create_ ? "cluster-create"
                                                   : "failover") +
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
      std::cout << "lavik-ctl " << lavik::kVersion << '\n';
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
    if (argument == "--failover-timeout-ms" ||
        argument.starts_with("--failover-timeout-ms=")) {
      const std::string_view value =
          OptionValue(argc, argv, &index, argument, "--failover-timeout-ms");
      if (!ParseInt(value, 1, 86'400'000, &options.failover_timeout_ms_)) {
        Fail(
            "--failover-timeout-ms must be an integer from 1 through 86400000");
      }
      options.failover_timeout_explicit_ = true;
      continue;
    }
    Fail("unknown option: " + std::string(argument));
  }

  for (; index < argc; ++index) options.command_.emplace_back(argv[index]);
  if (options.cluster_status_ || options.cluster_create_ || options.failover_) {
    if (!options.command_.empty()) {
      Fail(std::string(options.cluster_status_   ? "cluster-status"
                       : options.cluster_create_ ? "cluster-create"
                                                 : "failover") +
           " does not take positional arguments");
    }
    if (options.cluster_create_ && options.manifest_path_.empty()) {
      Fail("cluster-create requires --manifest FILE");
    }
    if (options.cluster_status_ && !options.manifest_path_.empty()) {
      Fail("--manifest applies only to cluster-create");
    }
    if (options.failover_ && options.failover_group_.empty()) {
      Fail("failover requires GROUP");
    }
    if (!options.failover_ && options.failover_timeout_explicit_) {
      Fail("--failover-timeout-ms applies only to failover");
    }
    if (options.failover_ && !options.manifest_path_.empty()) {
      Fail("--manifest applies only to cluster-create");
    }
    if ((options.cluster_status_ || options.failover_) && options.yes_) {
      Fail("--yes applies only to cluster-create");
    }
    if ((options.cluster_create_ || options.failover_) && options.json_) {
      Fail("--json applies only to cluster-status");
    }
  } else {
    if (options.command_.empty()) Fail("a Meta command is required");
    if (options.json_ || options.allow_plaintext_admin_ || options.yes_ ||
        !options.manifest_path_.empty() || options.failover_timeout_explicit_) {
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
       options.failover_ || !options.address_.empty()) &&
      tls_any != tls_all) {
    Fail("--tls-ca, --tls-cert, and --tls-key must be given together");
  }
  if (options.cluster_status_ || options.cluster_create_ || options.failover_) {
    if (!options.tls_server_name_.empty()) {
      Fail(
          "cluster-status/cluster-create/failover does not accept "
          "--tls-server-name; "
          "discovered Meta endpoints are verified by IP SAN");
    }
    // With a Unix seed, TLS credentials authorize any discovered remote
    // leader. Direct Unix commands have no redirect and reject TLS options.
    if (!options.address_.empty()) {
      auto endpoint = lavik::ParseNumericEndpoint(options.address_);
      if (!endpoint.has_value()) {
        Fail("--addr must be numeric IPv4:port or [IPv6]:port");
      }
      options.address_ = lavik::FormatNumericEndpoint(*endpoint);
      if (!tls_all && !options.allow_plaintext_admin_) {
        Fail("plaintext TCP admin requires --allow-plaintext-admin");
      }
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

lavik::meta::MetaAdminTarget AdminTarget(const Options& options) {
  lavik::meta::MetaAdminTarget target;
  if (!options.socket_path_.empty()) {
    target.transport_ = lavik::meta::MetaAdminTarget::Transport::kUnix;
    target.endpoint_ = options.socket_path_;
  } else {
    target.transport_ =
        options.tls_ca_.empty()
            ? lavik::meta::MetaAdminTarget::Transport::kTcpPlaintext
            : lavik::meta::MetaAdminTarget::Transport::kTcpMtls;
    target.endpoint_ = options.address_;
    target.tls_.ca_file_ = options.tls_ca_;
    target.tls_.certificate_file_ = options.tls_cert_;
    target.tls_.private_key_file_ = options.tls_key_;
    target.tls_.server_name_ = options.tls_server_name_;
  }
  return target;
}

int RunClusterStatus(const Options& options) {
  lavik::meta::ClusterStatusOptions status_options;
  status_options.tls_.ca_file_ = options.tls_ca_;
  status_options.tls_.certificate_file_ = options.tls_cert_;
  status_options.tls_.private_key_file_ = options.tls_key_;
  status_options.allow_plaintext_admin_ = options.allow_plaintext_admin_;
  status_options.deadline_ = std::chrono::steady_clock::now() +
                             std::chrono::milliseconds(options.timeout_ms_);
  lavik::meta::ClusterOperator cluster;
  auto outcome = cluster.Status(AdminTarget(options), status_options);
  if (!outcome.ok()) {
    const StatusFailureGuidance guidance =
        ExplainStatusFailure(outcome.status());
    std::cerr << "lavik-ctl: cluster-status failed: "
              << outcome.status().message() << '\n'
              << "status=unavailable\n"
              << "status_explanation=" << guidance.explanation_ << '\n'
              << "next_action=" << guidance.next_action_ << '\n';
    return 1;
  }
  auto rendered = options.json_
                      ? lavik::meta::RenderClusterStatusJson(*outcome)
                      : lavik::meta::RenderClusterStatusText(*outcome);
  if (!rendered.ok()) {
    std::cerr << "lavik-ctl: cluster-status rendering failed: "
              << rendered.status().message() << '\n'
              << "status=unavailable\n"
              << "status_explanation=the received status was internally "
                 "inconsistent\n"
              << "next_action=preserve Meta data and inspect the server logs "
                 "before retrying\n";
    return 1;
  }
  // Render completely before writing so fatal paths leave stdout empty.
  std::string output = std::move(*rendered);
  if (output.empty() || output.back() != '\n') output.push_back('\n');
  if (std::fwrite(output.data(), output.size(), 1, stdout) != 1) {
    Fail("failed to write stdout");
  }
  switch (outcome->result_) {
    case lavik::meta::ClusterStatusResult::kReady:
      return 0;
    case lavik::meta::ClusterStatusResult::kNotReady:
      return 2;
    case lavik::meta::ClusterStatusResult::kRetryable:
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

int RunClusterCreate(const Options& options) {
  auto manifest = lavik::meta::ParseClusterCreateManifest(
      ReadManifest(options.manifest_path_));
  if (!manifest.ok()) Fail(std::string(manifest.status().message()));
  // Encoding is part of local admission so an oversized normalized topology
  // is rejected before the destructive confirmation prompt.
  lavik::meta::MetaOperationId size_check_id{};
  size_check_id.fill(1);
  auto encoded =
      lavik::meta::EncodeClusterCreateRequest(*manifest, size_check_id);
  if (!encoded.ok()) Fail(std::string(encoded.status().message()));

  std::cout << "Cluster create plan (schema v1)\n";
  for (const auto& member : manifest->meta_members_) {
    std::cout << "  Meta member: " << member.server_id_
              << " raft=" << member.raft_endpoint_
              << " data-control=" << member.data_control_endpoint_
              << " ctl=" << member.ctl_endpoint_ << '\n';
  }
  std::cout << "  Slot layout: "
            << (manifest->slots_generated_ ? "contiguous-even" : "explicit")
            << '\n';
  for (const auto& node : manifest->data_nodes_) {
    std::cout << "  Data node: " << node.node_id_ << " @ "
              << node.client_endpoint_;
    if (!node.tls_endpoint_.empty()) {
      if (!node.client_endpoint_.empty()) std::cout << ", ";
      std::cout << node.tls_endpoint_;
    }
    std::cout << '\n';
  }
  for (const auto& group : manifest->groups_) {
    std::cout << "  Group: " << group.group_id_
              << " primary=" << group.primary_node_id_ << " replicas=";
    if (group.replica_node_ids_.empty()) {
      std::cout << "none";
    } else {
      for (std::size_t index = 0; index < group.replica_node_ids_.size();
           ++index) {
        if (index != 0) std::cout << ',';
        std::cout << group.replica_node_ids_[index];
      }
    }
    std::cout << '\n';
  }
  for (const auto& range : manifest->slot_ranges_) {
    std::cout << "  Slots: " << range.first_ << '-' << range.last_ << " -> "
              << range.group_id_ << '\n';
  }
  std::cout << "WARNING: existing data on all Data nodes will be erased.\n";
  if (!options.yes_) {
    std::cout << "Type yes to continue: " << std::flush;
    std::string confirmation;
    if (!std::getline(std::cin, confirmation) || confirmation != "yes") {
      std::cerr
          << "lavik-ctl: cluster creation cancelled; no mutation was sent\n";
      return 1;
    }
  }

  lavik::meta::ClusterStatusOptions create_options;
  create_options.tls_.ca_file_ = options.tls_ca_;
  create_options.tls_.certificate_file_ = options.tls_cert_;
  create_options.tls_.private_key_file_ = options.tls_key_;
  create_options.allow_plaintext_admin_ = options.allow_plaintext_admin_;
  create_options.deadline_ = std::chrono::steady_clock::now() +
                             std::chrono::milliseconds(options.timeout_ms_);
  lavik::meta::ClusterOperator cluster;
  auto outcome =
      cluster.Create(AdminTarget(options), *manifest, create_options);
  if (!outcome.ok()) {
    std::cerr << "lavik-ctl: " << outcome.status().message() << '\n';
    if (outcome.status().code() == absl::StatusCode::kFailedPrecondition) {
      return 2;
    }
    if (outcome.status().code() == absl::StatusCode::kDeadlineExceeded ||
        outcome.status().code() == absl::StatusCode::kUnavailable ||
        outcome.status().code() == absl::StatusCode::kAborted) {
      std::cerr << "lavik-ctl: creation may be partially committed; run "
                   "cluster-status before taking further action\n";
      return 3;
    }
    return 1;
  }
  std::cout << "Cluster create accepted: genesis committed="
            << outcome->genesis_commit_index_
            << " operation=" << outcome->operation_id_ << '\n'
            << "Run cluster-status to follow creation and runtime readiness.\n";
  return 0;
}

int RunFailover(const Options& options) {
  lavik::meta::ClusterStatusOptions cluster_options;
  cluster_options.tls_.ca_file_ = options.tls_ca_;
  cluster_options.tls_.certificate_file_ = options.tls_cert_;
  cluster_options.tls_.private_key_file_ = options.tls_key_;
  cluster_options.allow_plaintext_admin_ = options.allow_plaintext_admin_;
  cluster_options.deadline_ = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(options.timeout_ms_);
  lavik::meta::FailoverRequestOptions request{
      .group_id_ = options.failover_group_,
      .transition_timeout_ =
          std::chrono::milliseconds(options.failover_timeout_ms_),
      .operation_id_ = std::nullopt,
      .absolute_deadline_unix_ms_ = std::nullopt,
  };
  lavik::meta::ClusterOperator cluster;
  auto outcome =
      cluster.Failover(AdminTarget(options), request, cluster_options);
  if (!outcome.ok()) {
    std::cerr << "lavik-ctl: failover failed: " << outcome.status().message()
              << '\n';
    if (outcome.status().code() == absl::StatusCode::kInvalidArgument ||
        outcome.status().code() == absl::StatusCode::kFailedPrecondition ||
        outcome.status().code() == absl::StatusCode::kResourceExhausted) {
      return 2;
    }
    if (outcome.status().code() == absl::StatusCode::kAborted) return 3;
    return 1;
  }
  std::cout << "Controlled failover accepted: commit="
            << outcome->submission_commit_index_
            << " operation=" << outcome->operation_id_ << '\n'
            << "Use getop " << outcome->operation_id_
            << " to follow the terminal result.\n";
  return 0;
}

int Run(const Options& options) {
  if (options.cluster_status_) return RunClusterStatus(options);
  if (options.cluster_create_) return RunClusterCreate(options);
  if (options.failover_) return RunFailover(options);
  const auto target = AdminTarget(options);
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(options.timeout_ms_);
  lavik::meta::MetaAdminClient client;
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
    std::fprintf(stderr, "lavik-ctl: %s\n", error.what());
    return 1;
  }
}
