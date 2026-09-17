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

// Entry point of the Raft-backed meta control plane.
// lavik-meta is the only lavik artifact that links NuRaft: cluster
// membership and other control-plane metadata live on the meta plane, while
// the data-plane binary (lavik) and its tests stay Raft-free by
// construction (guarded in the root CMakeLists.txt).
//
// Process layout:
//   - main thread: CLI parse, assembly, startup waits, signal polling, and
//     the ordered teardown. raft_server construction/teardown happen here;
//     NuRaft's public API is thread-safe.
//   - one bycorf Runtime worker: ctl/Data Node control transports, with
//     authentication determined by the selected listener mode.
//   - one bounded proposal-executor thread: synchronous entry into NuRaft's
//     mutation/snapshot APIs, keeping their locks and WAL IO off Bycorf.
//   - NuRaft native Asio workers: peer RPC and timers. NuRaft commit/append
//     threads perform synchronous durability IO; completion and role events
//     return to Bycorf through the Runtime's foreign executor mailbox.
//
// Teardown order (main thread, on SIGTERM/SIGINT):
//   workflow reconcilers Shutdown() -> ctl Shutdown() ->
//   Data control Shutdown() -> coordinator demotion ->
//   proposal executor drain -> raft_launcher::shutdown() ->
//   MetaStateMachine::WaitForSnapshotWriterIdle() -> release Raft ref -> Bycorf
//   Runtime stop + join -> coordinator release.
// shutdown() joins the commit thread — the only producer of automatic
// snapshot jobs — and the writer drain lets an in-flight when_done reach the
// still-alive core before reset (the shutdown contract in
// state_machine.h). Once those producers quiesce, ForeignExecutor drains its
// accepted notifications before the generic Bycorf Runtime is stopped, and
// raft_server owns the
// nuraft::context through a unique_ptr member — the caller must never delete
// the context itself.

#include <signal.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <exception>
#include <fstream>
#include <functional>
#include <future>
#include <iterator>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "bycorf/runtime/runtime.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"
// NuRaft's headers are not -Wpedantic-clean.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include "libnuraft/basic_types.hxx"
#include "libnuraft/callback.hxx"
#include "libnuraft/cluster_config.hxx"
#include "libnuraft/context.hxx"
#include "libnuraft/launcher.hxx"
#include "libnuraft/logger.hxx"
#include "libnuraft/raft_params.hxx"
#include "libnuraft/raft_server.hxx"
#include "libnuraft/rpc_listener.hxx"
#include "libnuraft/srv_config.hxx"
#pragma GCC diagnostic pop

#include "lavik/cluster/meta_client.h"
#include "lavik/meta/automatic_failover_reconciler.h"
#include "lavik/meta/cluster_create.h"
#include "lavik/meta/cluster_create_reconciler.h"
#include "lavik/meta/coordinator.h"
#include "lavik/meta/ctl_server.h"
#include "lavik/meta/data_control_runtime_status.h"
#include "lavik/meta/data_control_server.h"
#include "lavik/meta/failover.h"
#include "lavik/meta/failover_reconciler.h"
#include "lavik/meta/identity_verifier.h"
#include "lavik/meta/membership_reconciler.h"
#include "lavik/meta/nuraft_asio_transport.h"
#include "lavik/meta/nuraft_log_store.h"
#include "lavik/meta/nuraft_state_mgr.h"
#include "lavik/meta/observation_store.h"
#include "lavik/meta/proposal_executor.h"
#include "lavik/meta/state_machine.h"
#include "lavik/numeric_endpoint.h"
#include "lavik/version.h"

namespace {

using lavik::meta::MetaAsioTransportConfig;
using lavik::meta::MetaCoordinator;
using lavik::meta::MetaCoordinatorOptions;
using lavik::meta::MetaCtlServer;
using lavik::meta::MetaCtlServerOptions;
using lavik::meta::MetaDataControlServer;
using lavik::meta::MetaDataControlServerOptions;
using lavik::meta::MetaLeadershipRelay;
using lavik::meta::MetaMembershipGate;
using lavik::meta::MetaProposalExecutor;
using lavik::meta::MetaStateMachine;
using lavik::meta::NuraftMemberConfig;
using lavik::meta::NuraftStartupMode;
using lavik::meta::NuraftStateMgr;
using lavik::meta::NuraftStateMgrOpenOptions;

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

struct CliOptions {
  int id_ = 0;
  bool has_id_ = false;
  // Process-local listeners. The initial manifest and then durable NuRaft
  // config remain authoritative for advertised membership endpoints.
  std::string raft_addr_;
  // Kept distinct from the NuRaft endpoint: data nodes neither speak nor
  // discover through the Raft transport.
  std::string data_control_addr_;
  std::string data_dir_;
  std::string ctl_addr_;    // required remote "ip:port" control surface
  std::string ctl_socket_;  // default: <data-dir>/meta-admin.sock
  std::vector<uid_t> ctl_allowed_uids_;
  std::string ctl_tls_ca_;
  std::string ctl_tls_cert_;
  std::string ctl_tls_key_;
  std::string initial_cluster_manifest_;
  std::string tls_ca_;
  std::string tls_cert_;
  std::string tls_key_;
  int heartbeat_ms_ = 100;
  int election_ms_low_ = 300;
  int election_ms_high_ = 600;
  int snapshot_distance_ = 1000;
  // Zero keeps no reserve: every snapshot compacts the whole prefix. This
  // also makes compaction observable in small process-level test clusters
  // (NuRaft's default of 100000 would suppress it at that scale).
  int reserved_log_items_ = 0;
  int client_req_timeout_ms_ = 3000;
  int snapshot_sync_timeout_ms_ = 0;  // 0 = NuRaft default
  int raft_log_level_ = 4;            // NuRaft level: 6=trace .. 1=fatal
  int raft_io_threads_ = 2;
};

struct EndpointParts {
  std::string host_;
  std::uint16_t port_ = 0;
};

void PrintUsage(const char* program) {
  std::fprintf(
      stderr,
      "usage: %s --id N --addr ip:port --data-control-addr ip:port "
      "--data-dir PATH "
      "[--ctl-socket PATH] --ctl-addr ip:port "
      "[--initial-cluster-manifest FILE]\n"
      "          [--tls-ca F --tls-cert F --tls-key F]\n"
      "          [--ctl-allow-uid N] [--ctl-tls-ca F --ctl-tls-cert F "
      "--ctl-tls-key F]\n"
      "          [--heartbeat-ms N] [--election-ms-low N] [--election-ms-high "
      "N]\n"
      "          [--snapshot-distance N] [--reserved-log-items N]\n"
      "          [--client-req-timeout-ms N] [--snapshot-sync-timeout-ms N]\n"
      "          [--raft-io-threads N] [--raft-log-level 1..6] [--version] "
      "[--help]\n",
      program);
}

bool ParseInt(std::string_view text, int min_value, int max_value, int* out) {
  if (text.empty()) {
    return false;
  }
  try {
    std::size_t used = 0;
    const int value = std::stoi(std::string(text), &used);
    if (used != text.size() || value < min_value || value > max_value) {
      return false;
    }
    *out = value;
    return true;
  } catch (...) {
    return false;
  }
}

// "ip:port" with a numeric IPv4/IPv6 host (the bycorf transport does no DNS).
absl::StatusOr<EndpointParts> ParseEndpointArg(std::string_view text) {
  auto endpoint = lavik::ParseNumericEndpoint(text);
  if (!endpoint.has_value()) {
    return absl::InvalidArgumentError(
        "expected numeric IPv4:port or [IPv6]:port");
  }
  return EndpointParts{.host_ = std::move(endpoint->host_),
                       .port_ = endpoint->port_};
}

// Compact standalone parsing: every option is "--name value", "--name=value",
// or a bare boolean flag. Unknown flags are fatal.
absl::StatusOr<CliOptions> ParseCli(int argc, char** argv, const char* program,
                                    bool* early_exit, int* early_exit_code) {
  CliOptions options;
  for (int ii = 1; ii < argc; ++ii) {
    std::string_view arg(argv[ii]);
    std::string_view name = arg;
    std::string_view inline_value;
    const std::size_t eq = arg.find('=');
    if (eq != std::string_view::npos && arg.substr(0, 2) == "--") {
      name = arg.substr(0, eq);
      inline_value = arg.substr(eq + 1);
    }

    if (name == "--help") {
      PrintUsage(program);
      *early_exit = true;
      *early_exit_code = 0;
      return options;
    }
    if (name == "--version") {
      std::printf("lavik-meta %.*s (nuraft %s)\n",
                  static_cast<int>(lavik::kVersion.size()),
                  lavik::kVersion.data(), LAVIK_NURAFT_PINNED_COMMIT);
      *early_exit = true;
      *early_exit_code = 0;
      return options;
    }
    std::string_view value = inline_value;
    if (eq == std::string_view::npos) {
      if (ii + 1 >= argc) {
        return absl::Status(absl::StatusCode::kInvalidArgument,
                            std::string(name) + " requires a value");
      }
      value = argv[++ii];
    }

    if (name == "--id") {
      if (!ParseInt(value, 1, 0x7fffffff, &options.id_)) {
        return absl::Status(absl::StatusCode::kInvalidArgument,
                            "--id must be a positive integer");
      }
      options.has_id_ = true;
    } else if (name == "--addr") {
      options.raft_addr_ = std::string(value);
    } else if (name == "--data-control-addr") {
      options.data_control_addr_ = std::string(value);
    } else if (name == "--data-dir") {
      options.data_dir_ = std::string(value);
    } else if (name == "--ctl-addr") {
      options.ctl_addr_ = std::string(value);
    } else if (name == "--ctl-socket") {
      options.ctl_socket_ = std::string(value);
    } else if (name == "--initial-cluster-manifest") {
      options.initial_cluster_manifest_ = std::string(value);
    } else if (name == "--ctl-allow-uid") {
      int uid = 0;
      if (!ParseInt(value, 0, 0x7fffffff, &uid)) {
        return absl::InvalidArgumentError("bad --ctl-allow-uid");
      }
      options.ctl_allowed_uids_.push_back(static_cast<uid_t>(uid));
    } else if (name == "--ctl-tls-ca") {
      options.ctl_tls_ca_ = std::string(value);
    } else if (name == "--ctl-tls-cert") {
      options.ctl_tls_cert_ = std::string(value);
    } else if (name == "--ctl-tls-key") {
      options.ctl_tls_key_ = std::string(value);
    } else if (name == "--tls-ca") {
      options.tls_ca_ = std::string(value);
    } else if (name == "--tls-cert") {
      options.tls_cert_ = std::string(value);
    } else if (name == "--tls-key") {
      options.tls_key_ = std::string(value);
    } else if (name == "--heartbeat-ms") {
      if (!ParseInt(value, 10, 60000, &options.heartbeat_ms_)) {
        return absl::Status(absl::StatusCode::kInvalidArgument,
                            "bad --heartbeat-ms");
      }
    } else if (name == "--election-ms-low") {
      if (!ParseInt(value, 20, 600000, &options.election_ms_low_)) {
        return absl::Status(absl::StatusCode::kInvalidArgument,
                            "bad --election-ms-low");
      }
    } else if (name == "--election-ms-high") {
      if (!ParseInt(value, 20, 600000, &options.election_ms_high_)) {
        return absl::Status(absl::StatusCode::kInvalidArgument,
                            "bad --election-ms-high");
      }
    } else if (name == "--snapshot-distance") {
      if (!ParseInt(value, 0, 0x7fffffff, &options.snapshot_distance_)) {
        return absl::Status(absl::StatusCode::kInvalidArgument,
                            "bad --snapshot-distance");
      }
    } else if (name == "--reserved-log-items") {
      if (!ParseInt(value, 0, 0x7fffffff, &options.reserved_log_items_)) {
        return absl::Status(absl::StatusCode::kInvalidArgument,
                            "bad --reserved-log-items");
      }
    } else if (name == "--client-req-timeout-ms") {
      if (!ParseInt(value, 100, 600000, &options.client_req_timeout_ms_)) {
        return absl::Status(absl::StatusCode::kInvalidArgument,
                            "bad --client-req-timeout-ms");
      }
    } else if (name == "--snapshot-sync-timeout-ms") {
      if (!ParseInt(value, 0, 600000, &options.snapshot_sync_timeout_ms_)) {
        return absl::Status(absl::StatusCode::kInvalidArgument,
                            "bad --snapshot-sync-timeout-ms");
      }
    } else if (name == "--raft-log-level") {
      if (!ParseInt(value, 1, 6, &options.raft_log_level_)) {
        return absl::Status(absl::StatusCode::kInvalidArgument,
                            "bad --raft-log-level");
      }
    } else if (name == "--raft-io-threads") {
      if (!ParseInt(value, 1, 128, &options.raft_io_threads_)) {
        return absl::Status(absl::StatusCode::kInvalidArgument,
                            "bad --raft-io-threads");
      }
    } else {
      return absl::Status(absl::StatusCode::kInvalidArgument,
                          "unknown argument: " + std::string(name));
    }
  }

  if (!options.has_id_) {
    return absl::Status(absl::StatusCode::kInvalidArgument, "--id is required");
  }
  if (options.raft_addr_.empty()) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "--addr is required");
  }
  if (options.data_control_addr_.empty()) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "--data-control-addr is required");
  }
  if (options.data_dir_.empty()) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "--data-dir is required");
  }
  if (options.ctl_socket_.empty()) {
    options.ctl_socket_ = options.data_dir_ + "/meta-admin.sock";
  }
  if (!options.ctl_socket_.empty() && options.ctl_allowed_uids_.empty()) {
    options.ctl_allowed_uids_.push_back(::getuid());
  }
  // Raft follows the data-plane convention: plaintext is the default, while
  // supplying any TLS input opts into mTLS and therefore requires a complete
  // identity. Partial configuration must not silently downgrade to plaintext.
  const bool tls_any = !options.tls_ca_.empty() || !options.tls_cert_.empty() ||
                       !options.tls_key_.empty();
  const bool tls_all = !options.tls_ca_.empty() && !options.tls_cert_.empty() &&
                       !options.tls_key_.empty();
  if (tls_any != tls_all) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "--tls-ca, --tls-cert and --tls-key must be given "
                        "together (or not at all)");
  }
  const bool ctl_tls_any = !options.ctl_tls_ca_.empty() ||
                           !options.ctl_tls_cert_.empty() ||
                           !options.ctl_tls_key_.empty();
  const bool ctl_tls_all = !options.ctl_tls_ca_.empty() &&
                           !options.ctl_tls_cert_.empty() &&
                           !options.ctl_tls_key_.empty();
  if (ctl_tls_any != ctl_tls_all) {
    return absl::InvalidArgumentError(
        "--ctl-tls-ca, --ctl-tls-cert and --ctl-tls-key must be given "
        "together");
  }
  if (options.ctl_addr_.empty() && ctl_tls_any) {
    return absl::InvalidArgumentError("ctl TLS options require --ctl-addr");
  }
  if (options.election_ms_low_ >= options.election_ms_high_) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "--election-ms-low must be < --election-ms-high");
  }
  if (options.heartbeat_ms_ * 2 > options.election_ms_low_) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "--heartbeat-ms must be <= half of --election-ms-low");
  }
  return options;
}

// ---------------------------------------------------------------------------
// NuRaft logger -> spdlog (stderr, node-id prefix from the process pattern)
// ---------------------------------------------------------------------------

class MetaNuraftLogger : public nuraft::logger {
 public:
  explicit MetaNuraftLogger(int level) : level_(level) {}

  void set_level(int level) override {
    level_.store(level, std::memory_order_release);
  }
  int get_level() override { return level_.load(std::memory_order_acquire); }

  void put_details(int level, const char* source_file, const char* func_name,
                   size_t line_number, const std::string& log_line) override {
    const char* base = std::strrchr(source_file, '/');
    spdlog::log(MapLevel(level), "[raft] {}:{} {}: {}",
                base != nullptr ? base + 1 : source_file, line_number,
                func_name, log_line);
  }

  void debug(const std::string& log_line) override {
    spdlog::debug("[raft] {}", log_line);
  }
  void info(const std::string& log_line) override {
    spdlog::info("[raft] {}", log_line);
  }
  void warn(const std::string& log_line) override {
    spdlog::warn("[raft] {}", log_line);
  }
  void err(const std::string& log_line) override {
    spdlog::error("[raft] {}", log_line);
  }

 private:
  static spdlog::level::level_enum MapLevel(int level) {
    // NuRaft levels: 6=trace, 5=debug, 4=info, 3=warn, 2=error, 1=fatal.
    switch (level) {
      case 6:
        return spdlog::level::trace;
      case 5:
        return spdlog::level::debug;
      case 4:
        return spdlog::level::info;
      case 3:
        return spdlog::level::warn;
      case 2:
        return spdlog::level::err;
      default:
        return spdlog::level::critical;
    }
  }

  std::atomic<int> level_;
};

// ---------------------------------------------------------------------------
// NuRaft raft_callback_: rare role/config transitions -> spdlog. The process
// gates grep these lines to assert leader-change internals directly (the
// externally observable leader=1/committed signals alone do not prove the
// callback path fired).
// ---------------------------------------------------------------------------

// Name of a callback event worth an audit line; nullptr = skip. The chatty
// per-request events (ProcessReq, HeartBeat, append-entry traffic) are never
// logged, so the trail stays greppable.
const char* RaftEventName(nuraft::cb_func::Type type) {
  switch (type) {
    case nuraft::cb_func::BecomeLeader:
      return "BecomeLeader";
    case nuraft::cb_func::BecomeFollower:
      return "BecomeFollower";
    case nuraft::cb_func::LeaderSmCatchingUp:
      return "LeaderSmCatchingUp";
    case nuraft::cb_func::NewConfig:
      return "NewConfig";
    case nuraft::cb_func::JoinedCluster:
      return "JoinedCluster";
    case nuraft::cb_func::RemovedFromCluster:
      return "RemovedFromCluster";
    case nuraft::cb_func::ResignationFromLeader:
      return "ResignationFromLeader";
    default:
      return nullptr;
  }
}

// init_options::raft_callback_ hook. NuRaft invokes callbacks from its own
// threads and, in the sm-catchup path (handle_commit.cxx), while holding
// raft_server::lock_, so this must stay fast and never block: it logs the
// rare transitions and always returns Ok (never vetoes the operation).
nuraft::cb_func::ReturnCode RaftEventCallback(nuraft::cb_func::Type type,
                                              nuraft::cb_func::Param* param) {
  const char* event = RaftEventName(type);
  if (event == nullptr) {
    return nuraft::cb_func::Ok;
  }
  // ctx is a ulong term for the three role-transition events and a ulong
  // config log index for NewConfig; the remaining logged events have none.
  std::string_view ctx_key;
  nuraft::ulong ctx_value = 0;
  switch (type) {
    case nuraft::cb_func::BecomeLeader:
    case nuraft::cb_func::BecomeFollower:
    case nuraft::cb_func::LeaderSmCatchingUp:
      ctx_key = "term";
      ctx_value = *static_cast<const nuraft::ulong*>(param->ctx);
      break;
    case nuraft::cb_func::NewConfig:
      ctx_key = "log_idx";
      ctx_value = *static_cast<const nuraft::ulong*>(param->ctx);
      break;
    default:
      break;
  }
  if (!ctx_key.empty()) {
    spdlog::info("[raft-cb] event={} my_id={} leader_id={} {}={}", event,
                 param->myId, param->leaderId, ctx_key, ctx_value);
  } else {
    spdlog::info("[raft-cb] event={} my_id={} leader_id={}", event, param->myId,
                 param->leaderId);
  }
  return nuraft::cb_func::Ok;
}

// ---------------------------------------------------------------------------
// Signals: handler flips a flag; the main loop polls it (async-signal-safe).
// ---------------------------------------------------------------------------

volatile sig_atomic_t g_shutdown_requested = 0;
volatile sig_atomic_t g_last_shutdown_signal = 0;

void ShutdownSignalHandler(int signal) {
  g_last_shutdown_signal = signal;
  g_shutdown_requested = 1;
}

absl::Status InstallShutdownSignalHandlers() {
  struct sigaction action{};
  sigemptyset(&action.sa_mask);
  action.sa_handler = ShutdownSignalHandler;
  if (::sigaction(SIGINT, &action, nullptr) != 0 ||
      ::sigaction(SIGTERM, &action, nullptr) != 0) {
    return absl::Status(absl::StatusCode::kInternal, "sigaction setup failed");
  }
  return absl::OkStatus();
}

// Polls an asynchronously-published bind status: kUnavailable means the
// worker has not reported yet; anything else is final.
absl::Status WaitForBound(
    const std::function<absl::Status()>& probe,
    std::chrono::milliseconds timeout = std::chrono::milliseconds(5000)) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    absl::Status status = probe();
    if (status.code() != absl::StatusCode::kUnavailable) {
      return status;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  return absl::Status(absl::StatusCode::kDeadlineExceeded,
                      "listener bind did not complete in time");
}

absl::StatusOr<std::string> ReadInitialClusterManifest(
    const std::string& path) {
  std::ifstream input(path, std::ios::binary);
  if (!input) {
    return absl::NotFoundError("cannot open initial cluster manifest: " + path);
  }
  std::string contents(64 * 1024 + 1, '\0');
  input.read(contents.data(), static_cast<std::streamsize>(contents.size()));
  contents.resize(static_cast<std::size_t>(input.gcount()));
  if (input.bad()) {
    return absl::DataLossError("failed to read initial cluster manifest: " +
                               path);
  }
  if (contents.size() > 64 * 1024) {
    return absl::ResourceExhaustedError(
        "initial cluster manifest exceeds 64 KiB");
  }
  return contents;
}

// ParseClusterCreateManifest has already established the canonical tcp://
// scheme; this adapter supplies NuRaft's scheme-free endpoint representation.
std::string StripValidatedTcpEndpointScheme(
    std::string_view manifest_endpoint) {
  constexpr std::string_view kTcpPrefix = "tcp://";
  return std::string(manifest_endpoint.substr(kTcpPrefix.size()));
}

}  // namespace

int main(int argc, char** argv) {
  bool early_exit = false;
  int early_exit_code = 0;
  auto parsed = ParseCli(argc, argv, argv[0], &early_exit, &early_exit_code);
  if (early_exit) {
    return early_exit_code;
  }
  if (!parsed.ok()) {
    std::fprintf(stderr, "lavik-meta: %s\n",
                 std::string(parsed.status().message()).c_str());
    PrintUsage(argv[0]);
    return 1;
  }
  const CliOptions options = *parsed;
  auto raft_endpoint = ParseEndpointArg(options.raft_addr_);
  if (!raft_endpoint.ok()) {
    std::fprintf(stderr, "lavik-meta: --addr: %s\n",
                 std::string(raft_endpoint.status().message()).c_str());
    return 1;
  }
  auto data_control_endpoint = ParseEndpointArg(options.data_control_addr_);
  if (!data_control_endpoint.ok()) {
    std::fprintf(stderr, "lavik-meta: --data-control-addr: %s\n",
                 std::string(data_control_endpoint.status().message()).c_str());
    return 1;
  }
  std::optional<EndpointParts> ctl_endpoint;
  std::string ctl_endpoint_text;
  if (!options.ctl_addr_.empty()) {
    auto parsed_ctl = ParseEndpointArg(options.ctl_addr_);
    if (!parsed_ctl.ok()) {
      std::fprintf(stderr, "lavik-meta: --ctl-addr: %s\n",
                   std::string(parsed_ctl.status().message()).c_str());
      return 1;
    }
    if (parsed_ctl->host_ == "0.0.0.0" || parsed_ctl->host_ == "::") {
      std::fprintf(stderr,
                   "lavik-meta: --ctl-addr must be a concrete routable "
                   "numeric address\n");
      return 1;
    }
    ctl_endpoint = std::move(*parsed_ctl);
    ctl_endpoint_text =
        ctl_endpoint->host_.find(':') == std::string::npos
            ? ctl_endpoint->host_ + ":" + std::to_string(ctl_endpoint->port_)
            : "[" + ctl_endpoint->host_ +
                  "]:" + std::to_string(ctl_endpoint->port_);
  }
  if (ctl_endpoint_text.empty()) {
    std::fprintf(stderr,
                 "lavik-meta: --ctl-addr is required for the durable Meta "
                 "member descriptor\n");
    return 1;
  }

  // One process-wide logger to stderr; the pattern carries the node id so
  // interleaved multi-node smoke logs stay attributable.
  spdlog::set_default_logger(spdlog::stderr_color_mt("meta"));
  spdlog::set_pattern("[n" + std::to_string(options.id_) +
                      "] %Y-%m-%dT%H:%M:%S.%e [%^%l%$] %v");
  spdlog::set_level(spdlog::level::info);

  const absl::Status signals = InstallShutdownSignalHandlers();
  if (!signals.ok()) {
    spdlog::critical("signal setup failed: {}", signals.message());
    return 1;
  }

  // --- durable state (synchronous file IO, main thread) ---
  const NuraftMemberConfig local_member{
      .server_id_ = options.id_,
      .raft_endpoint_ = lavik::FormatNumericEndpoint(
          {.host_ = raft_endpoint->host_, .port_ = raft_endpoint->port_}),
      .principal_ = "lavik://meta/" + std::to_string(options.id_),
      .data_control_endpoint_ =
          lavik::FormatNumericEndpoint({.host_ = data_control_endpoint->host_,
                                        .port_ = data_control_endpoint->port_}),
      .ctl_endpoint_ = ctl_endpoint_text,
  };
  NuraftStateMgrOpenOptions state_options{
      .data_dir_ = options.data_dir_,
      .local_member_ = local_member,
      .initial_cluster_ = std::nullopt,
  };
  if (!options.initial_cluster_manifest_.empty()) {
    auto bytes = ReadInitialClusterManifest(options.initial_cluster_manifest_);
    if (!bytes.ok()) {
      spdlog::critical("initial cluster manifest read failed: {}",
                       bytes.status().message());
      return 1;
    }
    auto manifest = lavik::meta::ParseClusterCreateManifest(*bytes);
    if (!manifest.ok()) {
      spdlog::critical("initial cluster manifest parse failed: {}",
                       manifest.status().message());
      return 1;
    }
    std::vector<NuraftMemberConfig> initial;
    initial.reserve(manifest->meta_members_.size());
    for (const auto& member : manifest->meta_members_) {
      initial.push_back({
          .server_id_ = static_cast<std::int32_t>(member.server_id_),
          .raft_endpoint_ =
              StripValidatedTcpEndpointScheme(member.raft_endpoint_),
          .principal_ = "lavik://meta/" + std::to_string(member.server_id_),
          .data_control_endpoint_ =
              StripValidatedTcpEndpointScheme(member.data_control_endpoint_),
          .ctl_endpoint_ =
              StripValidatedTcpEndpointScheme(member.ctl_endpoint_),
      });
    }
    state_options.initial_cluster_ = std::move(initial);
  }
  auto mgr_or = NuraftStateMgr::Open(std::move(state_options));
  if (!mgr_or.ok()) {
    spdlog::critical("state manager open failed: {}",
                     mgr_or.status().message());
    return 1;
  }
  nuraft::ptr<NuraftStateMgr> state_mgr(std::move(*mgr_or));
  auto machine_or = MetaStateMachine::Open(options.data_dir_);
  if (!machine_or.ok()) {
    // The current durable layout is intentionally incompatible with the
    // legacy single-file WAL and "LSN1" snapshots. Those formats held no
    // production data, so boot refuses them loudly and the remedy is to wipe
    // the directory rather than attempt migration.
    spdlog::critical("state machine open failed: {}",
                     machine_or.status().message());
    return 1;
  }
  nuraft::ptr<MetaStateMachine> state_machine(std::move(*machine_or));
  state_machine->AttachStateMgr(state_mgr);

  MetaAsioTransportConfig transport_config;
  transport_config.bind_address_ = raft_endpoint->host_;
  transport_config.tls_ca_cert_file_ = options.tls_ca_;
  transport_config.tls_cert_file_ = options.tls_cert_;
  transport_config.tls_key_file_ = options.tls_key_;
  transport_config.io_threads_ =
      static_cast<std::size_t>(options.raft_io_threads_);
  auto asio_options_or = lavik::meta::BuildMetaAsioOptions(
      transport_config, state_mgr, state_machine);
  if (!asio_options_or.ok()) {
    spdlog::critical("Raft Asio transport setup failed: {}",
                     asio_options_or.status().message());
    return 1;
  }
  nuraft::asio_service::options asio_options = std::move(*asio_options_or);

  // --- Bycorf runtime ---
  // One worker owns ctl/Data Node transport. Runtime owns its thread,
  // MPSC mailbox, and wake eventfd. NuRaft posts typed notifications directly
  // through the worker's foreign executor without touching Bycorf TLS.
  bycorf::Runtime bycorf_runtime;
  std::promise<absl::Status> init_promise;
  std::future<absl::Status> init_future = init_promise.get_future();
  bycorf_runtime.Start(
      /*thread_count=*/
      1,
      [&init_promise](unsigned, bycorf::Worker& worker) {
        const absl::Status init = worker.Init(bycorf::WorkerOptions{});
        init_promise.set_value(init);
        if (!init.ok()) {
          return 1;
        }
        worker.Run();
        // With one worker there are no cross-worker frames to coordinate, but
        // cleanup still belongs on the worker thread for thread-affine state.
        worker.Shutdown();
        worker.DestroyDetachedTasks();
        return 0;
      },
      /*pin_workers=*/false);
  const bycorf::ForeignExecutor foreign_executor =
      bycorf_runtime.GetForeignExecutor(/*worker_id=*/0);
  const absl::Status worker_init = init_future.get();
  if (!worker_init.ok()) {
    spdlog::critical("worker init failed: {}", worker_init.message());
    bycorf_runtime.WaitUntilStopped();
    return 1;
  }

  // --- raft core ---
  nuraft::raft_params params;
  params.with_election_timeout_lower(options.election_ms_low_);
  params.with_election_timeout_upper(options.election_ms_high_);
  params.with_hb_interval(options.heartbeat_ms_);
  // Data-control leases are never allowed to outlive NuRaft's own belief in
  // leadership. NuRaft's zero default expands to 20 heartbeats, which is
  // longer than this deployment's election lower bound and can overlap a new
  // leader; pin expiry to that lower bound explicitly.
  params.with_leadership_expiry(options.election_ms_low_);
  params.with_snapshot_enabled(options.snapshot_distance_);
  params.with_reserved_log_items(options.reserved_log_items_);
  params.with_client_req_timeout(options.client_req_timeout_ms_);
  if (options.snapshot_sync_timeout_ms_ > 0) {
    params.snapshot_sync_ctx_timeout_ = options.snapshot_sync_timeout_ms_;
  }
  // The ctl surface rejects non-leader writes instead of relying on NuRaft's
  // follower auto-forwarding path.
  params.auto_forwarding_ = false;
  params.return_method_ = nuraft::raft_params::async_handler;
  params.parallel_log_appending_ = false;
  params.wait_for_sm_catchup_on_becoming_leader_ = true;
  // Followers must attach their SM commit index to AppendEntries responses so
  // a creation leader can prove the fixed genesis barrier. The leader-scoped
  // cluster-create reconciler disables this mode outside wait-meta-barrier:
  // on a leader NuRaft also changes every client completion from local commit
  // to all-peer SM commit, which would otherwise destroy majority availability.
  params.track_peers_sm_commit_idx_ = true;

  auto raft_logger =
      std::make_shared<MetaNuraftLogger>(options.raft_log_level_);

  nuraft::raft_server::init_options init_opts;
  init_opts.skip_initial_election_timeout_ =
      state_mgr->startup_mode() == NuraftStartupMode::kWaitingJoiner;
  // Construction necessarily precedes MetaCoordinator assembly because the
  // coordinator needs the raft_server. The relay retains every role edge
  // from that window and remains the shutdown lifetime barrier for callbacks
  // already accepted by Bycorf's foreign mailbox.
  auto leadership_relay = std::make_shared<MetaLeadershipRelay>();
  init_opts.raft_callback_ = [foreign_executor, leadership_relay](
                                 nuraft::cb_func::Type type,
                                 nuraft::cb_func::Param* param) {
    const nuraft::cb_func::ReturnCode logged = RaftEventCallback(type, param);
    int role = -1;
    if (type == nuraft::cb_func::BecomeLeader) {
      role = 1;
    } else if (type == nuraft::cb_func::BecomeFollower) {
      role = 0;
    }
    if (role != -1) {
      // Record this exact edge before deferring delivery. Reading a shared
      // "latest role" in the worker would collapse Leader -> Follower ->
      // Leader and skip the authority-revocation barrier; recording before
      // Notify also preserves callback order if mailbox producers interleave.
      role == 1 ? leadership_relay->RecordLeaderEdge()
                : leadership_relay->RecordFollowerEdge();
      const bool accepted = foreign_executor.Notify(
          [leadership_relay]() noexcept { leadership_relay->Drain(); });
      if (!accepted) {
        // This path is quiesced before Runtime shutdown. Losing a role edge
        // here would leave leader-only control logic in the wrong state.
        std::terminate();
      }
    }
    return logged;
  };
  nuraft::raft_launcher launcher;
  nuraft::ptr<nuraft::raft_server> server =
      launcher.init(state_machine, state_mgr, raft_logger, raft_endpoint->port_,
                    asio_options, params, init_opts);
  if (server == nullptr) {
    spdlog::critical("failed to start NuRaft Asio listener on {}",
                     options.raft_addr_);
    foreign_executor.WaitUntilIdle();
    bycorf_runtime.RequestStop();
    bycorf_runtime.WaitUntilStopped();
    return 1;
  }

  int exit_code = 0;
  std::vector<std::shared_ptr<MetaCtlServer>> ctl_servers;
  std::shared_ptr<MetaDataControlServer> data_control;
  const std::uint32_t observation_ttl_ms = static_cast<std::uint32_t>(
      std::max(options.election_ms_high_, options.heartbeat_ms_ * 3));
  lavik::meta::MetaObservationStore::Limits observation_limits;
  observation_limits.ttl_ms_ = observation_ttl_ms;
  auto obs_store =
      std::make_shared<lavik::meta::MetaObservationStore>(observation_limits);
  auto proposal_executor = std::make_unique<MetaProposalExecutor>();
  auto membership_gate = std::make_shared<MetaMembershipGate>();
  auto cluster_status_service =
      std::make_shared<lavik::meta::MetaClusterStatusService>();
  auto data_control_runtime_status =
      std::make_shared<lavik::meta::MetaDataControlRuntimeStatus>();
  auto automatic_failover_diagnostics =
      std::make_shared<lavik::meta::MetaAutomaticFailoverDiagnosticsRegistry>();
  nuraft::ptr<nuraft::log_store> raft_log_store = state_mgr->load_log_store();
  auto* wal = static_cast<lavik::meta::NuraftLogStore*>(raft_log_store.get());
  MetaCoordinatorOptions coordinator_options;
  coordinator_options.proposal_executor_ = proposal_executor.get();
  coordinator_options.foreign_executor_ = foreign_executor;
  std::shared_ptr<MetaCoordinator> coordinator =
      std::make_shared<MetaCoordinator>(server, *state_machine, *wal,
                                        *obs_store, coordinator_options);
  coordinator->AddValidateHook(lavik::meta::ValidateFailoverProposal);
  leadership_relay->Attach(*coordinator);
  auto cluster_create_reconciler =
      std::make_shared<lavik::meta::MetaClusterCreateReconciler>(
          foreign_executor, membership_gate, data_control_runtime_status,
          server, static_cast<std::uint64_t>(observation_ttl_ms) * 1000);
  auto membership_reconciler =
      std::make_shared<lavik::meta::MetaMembershipReconciler>(
          foreign_executor, *proposal_executor, server, state_machine,
          state_mgr, membership_gate);
  const std::uint64_t leader_observation_grace_ms =
      static_cast<std::uint64_t>(std::max<std::int64_t>(
          observation_ttl_ms,
          static_cast<std::int64_t>(options.election_ms_high_) +
              lavik::cluster::MetaReconnectBackoff::MaximumWindow().count()));
  lavik::meta::MetaAutomaticFailoverReconcilerOptions
      automatic_failover_options;
  automatic_failover_options.data_control_runtime_status_ =
      data_control_runtime_status;
  automatic_failover_options.diagnostics_ = automatic_failover_diagnostics;
  automatic_failover_options.observation_ttl_ms_ = observation_ttl_ms;
  automatic_failover_options.observation_grace_ms_ =
      leader_observation_grace_ms;
  auto automatic_failover_reconciler =
      std::make_shared<lavik::meta::MetaAutomaticFailoverReconciler>(
          foreign_executor, std::move(automatic_failover_options));
  coordinator->AddValidateHook(
      automatic_failover_reconciler->validation_hook());

  lavik::meta::MetaFailoverReconcilerOptions failover_options;
  // A replacement leader starts with no volatile observations. Its absence
  // warmup must span both the Raft election and Data's longest reconnect
  // sleep; otherwise a healthy prepared candidate can be aborted just before
  // it redials the new leader. Candidate disconnect and typed action-failure
  // evidence remain immediate; an exact source disconnect uses its independent
  // recovery grace.
  failover_options.observation_grace_ms_ = leader_observation_grace_ms;
  failover_options.authority_exclusion_ms_ =
      2 * static_cast<std::uint64_t>(options.election_ms_low_);
  auto failover_reconciler =
      std::make_shared<lavik::meta::MetaFailoverReconciler>(
          foreign_executor, std::move(failover_options));
  coordinator->AddValidateHook(failover_reconciler->validation_hook());

  if (exit_code == 0) {
    MetaDataControlServerOptions control_options;
    control_options.server_id_ = static_cast<std::uint32_t>(options.id_);
    control_options.bind_host_ = data_control_endpoint->host_;
    control_options.port_ = data_control_endpoint->port_;
    control_options.tls_ca_cert_file_ = options.tls_ca_;
    control_options.tls_cert_file_ = options.tls_cert_;
    control_options.tls_key_file_ = options.tls_key_;
    control_options.local_ctl_endpoint_ = ctl_endpoint_text;
    control_options.runtime_status_ = data_control_runtime_status;
    control_options.observation_ttl_ms_ = observation_ttl_ms;
    control_options.leadership_validity_ms_ =
        static_cast<std::uint32_t>(options.election_ms_low_);
    // Retain one additional full maximum-lease window after the theoretical
    // prior expiry. The margin is derived from the same Raft leadership bound
    // rather than an unrelated wall-clock constant.
    control_options.lease_handoff_safety_margin_ms_ =
        control_options.leadership_validity_ms_;
    auto control_or =
        MetaDataControlServer::Create(foreign_executor, server, *coordinator,
                                      obs_store, std::move(control_options));
    if (!control_or.ok()) {
      spdlog::critical("data-control server create failed: {}",
                       control_or.status().message());
      exit_code = 1;
    } else {
      data_control = *control_or;
    }
  }

  if (exit_code == 0) {
    std::vector<MetaCtlServerOptions> ctl_option_set;
    if (!options.ctl_socket_.empty()) {
      MetaCtlServerOptions ctl_options;
      ctl_options.local_data_control_endpoint_ = options.data_control_addr_;
      ctl_options.local_ctl_endpoint_ = ctl_endpoint_text;
      ctl_options.cluster_status_service_ = cluster_status_service;
      ctl_options.data_control_runtime_status_ = data_control_runtime_status;
      ctl_options.automatic_failover_diagnostics_ =
          automatic_failover_diagnostics;
      ctl_options.cluster_create_reconciler_ = cluster_create_reconciler;
      ctl_options.membership_reconciler_ = membership_reconciler;
      ctl_options.observation_ttl_ms_ = observation_ttl_ms;
      ctl_options.transport_ = MetaCtlServerOptions::Transport::kUnix;
      ctl_options.unix_socket_path_ = options.ctl_socket_;
      ctl_options.allowed_uids_ = options.ctl_allowed_uids_;
      ctl_option_set.push_back(std::move(ctl_options));
    }
    if (ctl_endpoint.has_value()) {
      MetaCtlServerOptions ctl_options;
      ctl_options.local_data_control_endpoint_ = options.data_control_addr_;
      ctl_options.local_ctl_endpoint_ = ctl_endpoint_text;
      ctl_options.cluster_status_service_ = cluster_status_service;
      ctl_options.data_control_runtime_status_ = data_control_runtime_status;
      ctl_options.automatic_failover_diagnostics_ =
          automatic_failover_diagnostics;
      ctl_options.cluster_create_reconciler_ = cluster_create_reconciler;
      ctl_options.membership_reconciler_ = membership_reconciler;
      ctl_options.observation_ttl_ms_ = observation_ttl_ms;
      ctl_options.transport_ =
          options.ctl_tls_ca_.empty()
              ? MetaCtlServerOptions::Transport::kTcpPlaintext
              : MetaCtlServerOptions::Transport::kTcpMtls;
      ctl_options.bind_host_ = ctl_endpoint->host_;
      ctl_options.port_ = ctl_endpoint->port_;
      ctl_options.tls_ca_cert_file_ = options.ctl_tls_ca_;
      ctl_options.tls_cert_file_ = options.ctl_tls_cert_;
      ctl_options.tls_key_file_ = options.ctl_tls_key_;
      ctl_option_set.push_back(std::move(ctl_options));
    }
    for (auto& ctl_options : ctl_option_set) {
      auto ctl_or = MetaCtlServer::Create(
          foreign_executor, server, state_machine, coordinator, obs_store,
          *proposal_executor, membership_gate, std::move(ctl_options));
      if (!ctl_or.ok()) {
        spdlog::critical("ctl server create failed: {}",
                         ctl_or.status().message());
        exit_code = 1;
        break;
      }
      ctl_servers.push_back(*ctl_or);
    }
    if (exit_code == 0) {
      for (const auto& ctl : ctl_servers) {
        ctl->Start();
      }
      for (const auto& ctl : ctl_servers) {
        const absl::Status ctl_bound =
            WaitForBound([&ctl] { return ctl->status(); });
        if (!ctl_bound.ok()) {
          spdlog::critical("ctl listener bind failed: {}", ctl_bound.message());
          exit_code = 1;
          break;
        }
      }
    }
    if (exit_code != 0) {
      for (const auto& ctl : ctl_servers) {
        ctl->Shutdown();
      }
    }
  }

  if (exit_code == 0) {
    // Bind Admin first. If either Admin transport fails, Data-control has
    // never accepted a connection and rollback cannot transiently expose a
    // control endpoint for a process that will not become operational.
    data_control->StartListener();
    const absl::Status control_bound =
        WaitForBound([&data_control] { return data_control->status(); });
    if (!control_bound.ok()) {
      spdlog::critical("data-control listener bind failed: {}",
                       control_bound.message());
      exit_code = 1;
      for (const auto& ctl : ctl_servers) {
        ctl->Shutdown();
      }
    }
  }

  if (exit_code == 0) {
    // Register leader-scoped Data publication only after every configured
    // listener has bound. In particular, initial reconciliation must not
    // publish the durable advertised descriptor before a failing local Admin
    // bind has rolled startup back.
    coordinator->RunAsLeader(membership_reconciler);
    coordinator->RunAsLeader(cluster_create_reconciler);
    coordinator->RunAsLeader(data_control);
    coordinator->RunAsLeader(automatic_failover_reconciler);
    coordinator->RunAsLeader(failover_reconciler);
  }

  if (exit_code == 0) {
    std::string ctl_display = options.ctl_socket_;
    if (!options.ctl_addr_.empty()) {
      if (!ctl_display.empty()) ctl_display += ",";
      ctl_display += options.ctl_addr_;
    }
    spdlog::info(
        "node {} up: raft={} data-control={} ctl={} data-dir={} startup={} "
        "tls={}",
        options.id_, options.raft_addr_, options.data_control_addr_,
        ctl_display, options.data_dir_,
        static_cast<int>(state_mgr->startup_mode()),
        transport_config.TlsEnabled());
    while (g_shutdown_requested == 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    spdlog::info("node {} shutting down on signal {}", options.id_,
                 static_cast<int>(g_last_shutdown_signal));
  }

  // Stop durable workflows before draining Admin waiters. Local accepted
  // proposals/API entries may finish, but no remote Data or membership result
  // is needed to join; the next leader reconstructs work from committed state.
  failover_reconciler->Shutdown();
  automatic_failover_reconciler->Shutdown();
  cluster_create_reconciler->Shutdown();
  membership_reconciler->Shutdown();
  // Stop both ingress surfaces first, then synchronously revoke the
  // leader-scoped publisher before quiescing NuRaft/Asio while the Bycorf
  // worker mailbox and snapshot writer remain alive.
  // Admin goes first so no new capture can race Data-control teardown.
  for (const auto& ctl : ctl_servers) {
    ctl->Shutdown();
  }
  if (data_control != nullptr) {
    data_control->Shutdown();
  }
  // Detach first so callbacks racing shutdown cannot enqueue a later Leader
  // edge behind this final demotion. Coordinator teardown independently
  // cancels any reconciler still running if it reaches destruction before
  // this queued edge is consumed.
  leadership_relay->DetachAndStop();
  coordinator->BecomeFollower();
  // No new Bycorf ingress or leader work is accepted. Drain queued NuRaft
  // mutation/snapshot entry before stopping its Asio service; cmd_result
  // completions can still use the live foreign executor while shutdown
  // resolves rounds.
  proposal_executor->Shutdown();
  if (!launcher.shutdown()) {
    spdlog::error("NuRaft Asio shutdown did not quiesce within its timeout");
    exit_code = 1;
  }
  // Snapshot-writer drain between shutdown() and reset(), per the shutdown
  // contract in state_machine.h: an in-flight when_done must reach the
  // core while it is still alive.
  state_machine->WaitForSnapshotWriterIdle();
  server.reset();
  // Every foreign producer is now quiescent. Drain its accepted mailbox
  // prefix before stopping the generic Runtime, keeping this lifecycle policy
  // out of Bycorf's data-plane Worker loop.
  foreign_executor.WaitUntilIdle();
  bycorf_runtime.RequestStop();
  bycorf_runtime.WaitUntilStopped();
  if (bycorf_runtime.exit_code() != 0) exit_code = 1;
  coordinator.reset();

  if (exit_code == 0) {
    spdlog::info("node {} stopped", options.id_);
  }
  return exit_code;
}
