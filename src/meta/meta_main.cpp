// Entry point of the Raft-backed meta control plane (issue #19).
// keylane_meta is the only keylane artifact that links NuRaft: cluster
// membership and other control-plane metadata live on the meta plane, while
// the data-plane binary (keylane) and its tests stay Raft-free by
// construction (guarded in the root CMakeLists.txt).
//
// Process layout:
//   - main thread: CLI parse, assembly, startup waits, signal polling, and
//     the ordered teardown. raft_server construction/teardown happen here;
//     NuRaft's public API is thread-safe.
//   - one celer worker thread: the MetaCelerBridge drain loop (NuRaft timer
//     tasks and RPC transport) plus the authenticated ctl line server (UDS
//     peer credentials by default, or explicit TCP mutual TLS).
//   - NuRaft background threads (commit/append): call back into the worker
//     only through the bridge; durability IO runs on them inside
//     NuraftLogStore/NuraftStateMgr, while MetaStateMachine hands snapshot
//     file IO to its own writer thread (meta_state_machine.h).
//
// Teardown order (main thread, on SIGTERM/SIGINT):
//   raft_server::shutdown() -> MetaStateMachine::WaitForSnapshotWriterIdle()
//   -> release local refs -> ctl Shutdown() -> listener shutdown() ->
//   bridge Stop() -> worker stop + join.
// shutdown() joins the commit thread — the only producer of automatic
// snapshot jobs — and the writer drain lets an in-flight when_done reach the
// still-alive core before reset (the shutdown contract in
// meta_state_machine.h). The bridge's FIFO inbox drains the posted shutdown
// closures before the drain loop exits, and raft_server owns the
// nuraft::context through a unique_ptr member — the caller must never delete
// the context itself.

#include <arpa/inet.h>
#include <signal.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <coroutine>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>
#include <future>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "celer/runtime/cross_core.h"
#include "celer/runtime/worker.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"
// NuRaft's headers are not -Wpedantic-clean; see nuraft_scheduler.h.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include "libnuraft/basic_types.hxx"
#include "libnuraft/callback.hxx"
#include "libnuraft/cluster_config.hxx"
#include "libnuraft/context.hxx"
#include "libnuraft/logger.hxx"
#include "libnuraft/raft_params.hxx"
#include "libnuraft/raft_server.hxx"
#include "libnuraft/rpc_listener.hxx"
#include "libnuraft/srv_config.hxx"
#pragma GCC diagnostic pop

#include "keylane/version.h"
#include "meta/meta_coordinator.h"
#include "meta/meta_ctl_server.h"
#include "meta/meta_identity_verifier.h"
#include "meta/meta_observation_store.h"
#include "meta/meta_state_machine.h"
#include "meta/nuraft_log_store.h"
#include "meta/nuraft_rpc_client.h"
#include "meta/nuraft_rpc_listener.h"
#include "meta/nuraft_scheduler.h"
#include "meta/nuraft_state_mgr.h"

namespace {

using keylane::meta::MetaCelerBridge;
using keylane::meta::MetaCoordinator;
using keylane::meta::MetaCoordinatorOptions;
using keylane::meta::MetaCtlServer;
using keylane::meta::MetaCtlServerOptions;
using keylane::meta::MetaStateMachine;
using keylane::meta::MetaTransportConfig;
using keylane::meta::NuraftDelayedTaskScheduler;
using keylane::meta::NuraftRpcClientFactory;
using keylane::meta::NuraftRpcListener;
using keylane::meta::NuraftStateMgr;

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

struct CliOptions {
  int id_ = 0;
  bool has_id_ = false;
  std::string raft_addr_;  // "ip:port": raft bind AND advertised endpoint
  std::string data_dir_;
  std::string ctl_addr_;    // optional remote "ip:port" mTLS control surface
  std::string ctl_socket_;  // default: <data-dir>/meta-admin.sock
  std::vector<uid_t> ctl_allowed_uids_;
  std::string ctl_tls_ca_;
  std::string ctl_tls_cert_;
  std::string ctl_tls_key_;
  bool bootstrap_ = false;
  std::string tls_ca_;
  std::string tls_cert_;
  std::string tls_key_;
  bool unsafe_allow_plaintext_raft_ = false;
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
};

struct EndpointParts {
  std::string host_;
  std::uint16_t port_ = 0;
};

void PrintUsage(const char* program) {
  std::fprintf(
      stderr,
      "usage: %s --id N --addr ip:port --data-dir PATH "
      "[--ctl-socket PATH | --ctl-addr ip:port] "
      "[--bootstrap]\n"
      "          [--tls-ca F --tls-cert F --tls-key F]\n"
      "          [--unsafe-allow-plaintext-raft]\n"
      "          [--ctl-allow-uid N] [--ctl-tls-ca F --ctl-tls-cert F "
      "--ctl-tls-key F]\n"
      "          [--heartbeat-ms N] [--election-ms-low N] [--election-ms-high "
      "N]\n"
      "          [--snapshot-distance N] [--reserved-log-items N]\n"
      "          [--client-req-timeout-ms N] [--snapshot-sync-timeout-ms N]\n"
      "          [--raft-log-level 1..6] [--version] [--help]\n",
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

// "ip:port" with a numeric IPv4/IPv6 host (the celer transport does no DNS).
absl::StatusOr<EndpointParts> ParseEndpointArg(std::string_view text) {
  const std::size_t colon = text.rfind(':');
  if (colon == std::string_view::npos || colon == 0 ||
      colon + 1 == text.size()) {
    return absl::Status(absl::StatusCode::kInvalidArgument, "expected ip:port");
  }
  EndpointParts parts;
  parts.host_ = std::string(text.substr(0, colon));
  int port = 0;
  if (!ParseInt(text.substr(colon + 1), 1, 65535, &port)) {
    return absl::Status(absl::StatusCode::kInvalidArgument, "invalid port");
  }
  in_addr addr4{};
  in6_addr addr6{};
  if (::inet_pton(AF_INET, parts.host_.c_str(), &addr4) != 1 &&
      ::inet_pton(AF_INET6, parts.host_.c_str(), &addr6) != 1) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "host is not a numeric IPv4/IPv6 address");
  }
  parts.port_ = static_cast<std::uint16_t>(port);
  return parts;
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
      std::printf("keylane_meta %.*s (nuraft %s)\n",
                  static_cast<int>(keylane::kVersion.size()),
                  keylane::kVersion.data(), KEYLANE_NURAFT_PINNED_COMMIT);
      *early_exit = true;
      *early_exit_code = 0;
      return options;
    }
    if (name == "--bootstrap") {
      options.bootstrap_ = true;
      continue;
    }
    if (name == "--unsafe-allow-plaintext-raft") {
      options.unsafe_allow_plaintext_raft_ = true;
      continue;
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
    } else if (name == "--data-dir") {
      options.data_dir_ = std::string(value);
    } else if (name == "--ctl-addr") {
      options.ctl_addr_ = std::string(value);
    } else if (name == "--ctl-socket") {
      options.ctl_socket_ = std::string(value);
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
  if (options.data_dir_.empty()) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "--data-dir is required");
  }
  if (!options.ctl_addr_.empty() && !options.ctl_socket_.empty()) {
    return absl::InvalidArgumentError(
        "--ctl-addr and --ctl-socket are mutually exclusive");
  }
  if (options.ctl_addr_.empty() && options.ctl_socket_.empty()) {
    options.ctl_socket_ = options.data_dir_ + "/meta-admin.sock";
  }
  if (!options.ctl_socket_.empty() && options.ctl_allowed_uids_.empty()) {
    options.ctl_allowed_uids_.push_back(::getuid());
  }
  // mTLS is all-or-nothing: any one of the three files enables the check.
  const bool tls_any = !options.tls_ca_.empty() || !options.tls_cert_.empty() ||
                       !options.tls_key_.empty();
  const bool tls_all = !options.tls_ca_.empty() && !options.tls_cert_.empty() &&
                       !options.tls_key_.empty();
  if (tls_any != tls_all) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "--tls-ca, --tls-cert and --tls-key must be given "
                        "together (or not at all)");
  }
  if (!tls_all && !options.unsafe_allow_plaintext_raft_) {
    return absl::InvalidArgumentError(
        "Raft mTLS is required; test-only plaintext transport requires the "
        "explicit --unsafe-allow-plaintext-raft flag");
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
  if (!options.ctl_addr_.empty() && !ctl_tls_all) {
    return absl::InvalidArgumentError(
        "--ctl-addr requires complete ctl mTLS options; plaintext TCP admin "
        "is forbidden");
  }
  if (!options.ctl_socket_.empty() && ctl_tls_any) {
    return absl::InvalidArgumentError(
        "ctl TLS options apply only to --ctl-addr");
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
  struct sigaction action {};
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

}  // namespace

int main(int argc, char** argv) {
  bool early_exit = false;
  int early_exit_code = 0;
  auto parsed = ParseCli(argc, argv, argv[0], &early_exit, &early_exit_code);
  if (early_exit) {
    return early_exit_code;
  }
  if (!parsed.ok()) {
    std::fprintf(stderr, "keylane_meta: %s\n",
                 std::string(parsed.status().message()).c_str());
    PrintUsage(argv[0]);
    return 1;
  }
  const CliOptions options = *parsed;
  auto raft_endpoint = ParseEndpointArg(options.raft_addr_);
  if (!raft_endpoint.ok()) {
    std::fprintf(stderr, "keylane_meta: --addr: %s\n",
                 std::string(raft_endpoint.status().message()).c_str());
    return 1;
  }
  std::optional<EndpointParts> ctl_endpoint;
  if (!options.ctl_addr_.empty()) {
    auto parsed_ctl = ParseEndpointArg(options.ctl_addr_);
    if (!parsed_ctl.ok()) {
      std::fprintf(stderr, "keylane_meta: --ctl-addr: %s\n",
                   std::string(parsed_ctl.status().message()).c_str());
      return 1;
    }
    ctl_endpoint = std::move(*parsed_ctl);
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

  // --- transport adapters (allocation only; no celer objects touched) ---
  auto bridge_or = MetaCelerBridge::Create();
  if (!bridge_or.ok()) {
    spdlog::critical("bridge create failed: {}", bridge_or.status().message());
    return 1;
  }
  std::shared_ptr<MetaCelerBridge> bridge = *bridge_or;

  MetaTransportConfig transport_config;
  transport_config.tls_ca_cert_file_ = options.tls_ca_;
  transport_config.tls_cert_file_ = options.tls_cert_;
  transport_config.tls_key_file_ = options.tls_key_;

  auto factory_or = NuraftRpcClientFactory::Create(bridge, transport_config);
  if (!factory_or.ok()) {
    spdlog::critical("rpc client factory create failed: {}",
                     factory_or.status().message());
    return 1;
  }
  std::shared_ptr<NuraftRpcClientFactory> factory = *factory_or;
  auto scheduler = std::make_shared<NuraftDelayedTaskScheduler>(bridge);
  auto listener_or = NuraftRpcListener::Create(
      bridge, transport_config, raft_endpoint->host_, raft_endpoint->port_);
  if (!listener_or.ok()) {
    spdlog::critical("rpc listener create failed: {}",
                     listener_or.status().message());
    return 1;
  }
  std::shared_ptr<NuraftRpcListener> listener = *listener_or;

  // --- durable state (synchronous file IO, main thread) ---
  auto mgr_or =
      NuraftStateMgr::Open(options.data_dir_, options.id_, options.raft_addr_);
  if (!mgr_or.ok()) {
    spdlog::critical("state manager open failed: {}",
                     mgr_or.status().message());
    return 1;
  }
  nuraft::ptr<NuraftStateMgr> state_mgr(std::move(*mgr_or));
  auto machine_or = MetaStateMachine::Open(options.data_dir_);
  if (!machine_or.ok()) {
    // The formal state machine's durable layout is intentionally incompatible
    // with the prototype (WAL v1 raft_log.dat is rejected by NuraftLogStore,
    // "LSN1" snapshots fail the "MSN1" framing check): it held
    // no production data, so the remedy is to wipe the directory, and boot
    // refuses it loudly instead of migrating.
    spdlog::critical("state machine open failed: {}",
                     machine_or.status().message());
    return 1;
  }
  nuraft::ptr<MetaStateMachine> state_machine(std::move(*machine_or));

  factory->SetPeerSchemaVerifier(
      [state_machine](std::int32_t peer_id,
                      keylane::meta::wire::SchemaRange peer_schema) {
        const keylane::meta::MetaStores stores =
            state_machine->StoresSnapshot();
        if (stores.active_write_schema_ < peer_schema.min_schema_ ||
            stores.active_write_schema_ > peer_schema.max_schema_) {
          return absl::PermissionDeniedError(
              "Raft target binary cannot read the committed write schema");
        }
        const auto binding = stores.identity_.FindMetaMember(
            static_cast<std::uint32_t>(peer_id));
        if (!binding.has_value() || binding->retired_ ||
            binding->min_schema_ != peer_schema.min_schema_ ||
            binding->max_schema_ != peer_schema.max_schema_) {
          return absl::PermissionDeniedError(
              "Raft target handshake schema range differs from its committed "
              "member binding");
        }
        return absl::OkStatus();
      });

  {
    const bool tls_enabled = transport_config.TlsEnabled();
    listener->SetIdentityVerifier([state_mgr, state_machine, tls_enabled](
                                      std::int32_t claimed_id,
                                      std::span<const std::string> uri_sans,
                                      keylane::meta::wire::SchemaRange
                                          peer_schema) {
      const keylane::meta::MetaStores committed_stores =
          state_machine->StoresSnapshot();
      if (committed_stores.active_write_schema_ < peer_schema.min_schema_ ||
          committed_stores.active_write_schema_ > peer_schema.max_schema_) {
        return absl::PermissionDeniedError(
            "Raft peer binary cannot read the committed write schema");
      }
      const nuraft::ptr<nuraft::cluster_config> config =
          state_mgr->load_config();
      if (config == nullptr) {
        return absl::PermissionDeniedError(
            "Raft peer has no committed cluster configuration");
      }
      for (const nuraft::ptr<nuraft::srv_config>& member :
           config->get_servers()) {
        if (member != nullptr && member->get_id() == claimed_id) {
          if (tls_enabled) {
            const absl::Status certificate =
                keylane::meta::VerifyRaftPeerIdentity(claimed_id, uri_sans,
                                                      member->get_aux());
            if (!certificate.ok()) return certificate;
          }
          auto descriptor =
              keylane::meta::MetaMemberIdentity::DecodeAux(member->get_aux());
          if (!descriptor.ok()) return descriptor.status();
          if (descriptor->min_schema_ != peer_schema.min_schema_ ||
              descriptor->max_schema_ != peer_schema.max_schema_) {
            return absl::PermissionDeniedError(
                "Raft peer handshake schema range differs from its committed "
                "member binding");
          }
          const auto committed = committed_stores.identity_.FindMetaMember(
              static_cast<std::uint32_t>(claimed_id));
          if (!committed.has_value() &&
              state_machine->last_commit_index() == 0) {
            // A joiner installs the leader's cluster config before its
            // first application entry/snapshot. During that narrow
            // bootstrap window the config aux plus CA-authenticated
            // certificate is the only durable identity it can know.
            return absl::OkStatus();
          }
          if (!committed.has_value() || committed->retired_ ||
              committed->principal_ != descriptor->principal_ ||
              committed->min_schema_ != descriptor->min_schema_ ||
              committed->max_schema_ != descriptor->max_schema_) {
            return absl::PermissionDeniedError(
                "Raft member lacks matching committed identity binding");
          }
          return absl::OkStatus();
        }
      }
      // A pristine joiner has not received the leader's configuration
      // yet. Admit only the CA-authenticated canonical meta identity for
      // the claimed id; NuRaft still limits this bootstrap path to join
      // protocol messages. Once a config containing the peer commits,
      // the persisted aux binding above becomes mandatory.
      if (!tls_enabled) return absl::OkStatus();
      auto bootstrap_identity =
          keylane::meta::AuthenticateMetaUriSans(uri_sans);
      if (!bootstrap_identity.ok()) return bootstrap_identity.status();
      if (bootstrap_identity->role_ !=
              keylane::meta::MetaPrincipalRole::kMetaMember ||
          bootstrap_identity->subject_id_ != std::to_string(claimed_id)) {
        return absl::PermissionDeniedError(
            "Raft bootstrap certificate does not match request source id");
      }
      return absl::OkStatus();
    });
  }

  // --- celer worker thread: bridge drain loop, raft transport, ctl server ---
  // A standalone Worker still requires its cross-core mailbox set (the
  // Runtime normally provides it); a single-slot CrossCore satisfies the
  // invariant without ever carrying traffic — all foreign-thread ingress
  // goes through the bridge instead.
  celer::CrossCore cross_core(1);
  celer::Worker worker;
  worker.BindCrossCore(/*id=*/0, &cross_core);
  std::promise<absl::Status> init_promise;
  std::future<absl::Status> init_future = init_promise.get_future();
  std::thread worker_thread([&worker, &bridge, &init_promise] {
    const absl::Status init = worker.Init(celer::WorkerOptions{});
    init_promise.set_value(init);
    if (!init.ok()) {
      return;
    }
    worker.Spawn(bridge->Run(worker));
    worker.Run();
  });
  const absl::Status worker_init = init_future.get();
  if (!worker_init.ok()) {
    spdlog::critical("worker init failed: {}", worker_init.message());
    worker_thread.join();
    return 1;
  }

  // --- raft core ---
  nuraft::raft_params params;
  params.with_election_timeout_lower(options.election_ms_low_);
  params.with_election_timeout_upper(options.election_ms_high_);
  params.with_hb_interval(options.heartbeat_ms_);
  params.with_snapshot_enabled(options.snapshot_distance_);
  params.with_reserved_log_items(options.reserved_log_items_);
  params.with_client_req_timeout(options.client_req_timeout_ms_);
  if (options.snapshot_sync_timeout_ms_ > 0) {
    params.snapshot_sync_ctx_timeout_ = options.snapshot_sync_timeout_ms_;
  }
  // The celer listener answers process_req synchronously and has no async-cb
  // support, so follower-side auto-forwarding must stay off; the ctl surface
  // rejects non-leader writes instead.
  params.auto_forwarding_ = false;
  params.return_method_ = nuraft::raft_params::async_handler;
  params.wait_for_sm_catchup_on_becoming_leader_ = true;

  auto raft_logger =
      std::make_shared<MetaNuraftLogger>(options.raft_log_level_);

  // raft_server takes ownership of ctx via its std::unique_ptr<context>
  // member; never delete ctx by hand (double-delete trap).
  nuraft::context* ctx =
      new nuraft::context(state_mgr, state_machine, listener, raft_logger,
                          factory, scheduler, params);
  nuraft::raft_server::init_options init_opts;
  init_opts.skip_initial_election_timeout_ = !options.bootstrap_;
  // Construction necessarily precedes MetaCoordinator assembly because the
  // coordinator needs the raft_server. Remember the latest role edge so an
  // election racing that short window is replayed immediately on attach.
  auto coordinator_target =
      std::make_shared<std::atomic<MetaCoordinator*>>(nullptr);
  auto pending_role = std::make_shared<std::atomic<int>>(-1);
  init_opts.raft_callback_ = [coordinator_target, pending_role](
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
      pending_role->store(role, std::memory_order_release);
      if (MetaCoordinator* target =
              coordinator_target->load(std::memory_order_acquire);
          target != nullptr) {
        role == 1 ? target->BecomeLeader() : target->BecomeFollower();
      }
    }
    return logged;
  };
  nuraft::ptr<nuraft::raft_server> server =
      nuraft::cs_new<nuraft::raft_server>(ctx, init_opts);

  int exit_code = 0;
  std::shared_ptr<MetaCtlServer> ctl;
  auto obs_store = std::make_shared<keylane::meta::MetaObservationStore>();
  nuraft::ptr<nuraft::log_store> raft_log_store = state_mgr->load_log_store();
  auto* wal = static_cast<keylane::meta::NuraftLogStore*>(raft_log_store.get());
  MetaCoordinatorOptions coordinator_options;
  coordinator_options.resume_hook_ =
      [bridge](std::coroutine_handle<> continuation) {
        bridge->Post([continuation](celer::Worker& owner) {
          owner.Enqueue(continuation);
        });
      };
  std::shared_ptr<MetaCoordinator> coordinator =
      std::make_shared<MetaCoordinator>(server, *state_machine, *wal,
                                        *obs_store, coordinator_options);
  coordinator->AddValidateHook([server](
                                   const keylane::meta::MetaCommand& command,
                                   const keylane::meta::MetaCommittedView&,
                                   const keylane::meta::MetaObservationStore&) {
    const auto* set = std::get_if<keylane::meta::SetSchemaVersion>(&command);
    if (set == nullptr) return absl::OkStatus();
    const nuraft::ptr<nuraft::cluster_config> config = server->get_config();
    if (config == nullptr) {
      return keylane::meta::MetaDomainRejectError(
          "cannot attest schema support without a Raft configuration");
    }
    for (const nuraft::ptr<nuraft::srv_config>& member :
         config->get_servers()) {
      if (member == nullptr) continue;
      auto identity =
          keylane::meta::MetaMemberIdentity::DecodeAux(member->get_aux());
      if (!identity.ok() ||
          set->new_active_write_schema_ < identity->min_schema_ ||
          set->new_active_write_schema_ > identity->max_schema_) {
        return keylane::meta::MetaDomainRejectError(
            "not every Raft member attests support for the requested "
            "write schema");
      }
    }
    return absl::OkStatus();
  });
  coordinator_target->store(coordinator.get(), std::memory_order_release);
  const int role_before_attach = pending_role->load(std::memory_order_acquire);
  if (role_before_attach == 1) {
    coordinator->BecomeLeader();
  } else if (role_before_attach == 0) {
    coordinator->BecomeFollower();
  }

  nuraft::ptr<nuraft::msg_handler> handler(server);
  listener->listen(handler);
  const absl::Status raft_bound =
      WaitForBound([&listener] { return listener->listen_status(); });
  if (!raft_bound.ok()) {
    spdlog::critical("raft listener bind failed: {}", raft_bound.message());
    exit_code = 1;
  }

  if (exit_code == 0) {
    MetaCtlServerOptions ctl_options;
    if (ctl_endpoint.has_value()) {
      ctl_options.transport_ = MetaCtlServerOptions::Transport::kTcpMtls;
      ctl_options.bind_host_ = ctl_endpoint->host_;
      ctl_options.port_ = ctl_endpoint->port_;
      ctl_options.tls_ca_cert_file_ = options.ctl_tls_ca_;
      ctl_options.tls_cert_file_ = options.ctl_tls_cert_;
      ctl_options.tls_key_file_ = options.ctl_tls_key_;
    } else {
      ctl_options.transport_ = MetaCtlServerOptions::Transport::kUnix;
      ctl_options.unix_socket_path_ = options.ctl_socket_;
      ctl_options.allowed_uids_ = options.ctl_allowed_uids_;
    }
    auto ctl_or =
        MetaCtlServer::Create(bridge, server, state_machine, coordinator,
                              obs_store, std::move(ctl_options));
    if (!ctl_or.ok()) {
      spdlog::critical("ctl server create failed: {}",
                       ctl_or.status().message());
      exit_code = 1;
    } else {
      ctl = *ctl_or;
      ctl->Start();
      const absl::Status ctl_bound =
          WaitForBound([&ctl] { return ctl->status(); });
      if (!ctl_bound.ok()) {
        spdlog::critical("ctl listener bind failed: {}", ctl_bound.message());
        exit_code = 1;
      }
    }
  }

  if (exit_code == 0) {
    const std::string ctl_display =
        !options.ctl_socket_.empty() ? options.ctl_socket_ : options.ctl_addr_;
    spdlog::info("node {} up: raft={} ctl={} data-dir={} bootstrap={} tls={}",
                 options.id_, options.raft_addr_, ctl_display,
                 options.data_dir_, options.bootstrap_,
                 transport_config.TlsEnabled());
    while (g_shutdown_requested == 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    spdlog::info("node {} shutting down on signal {}", options.id_,
                 static_cast<int>(g_last_shutdown_signal));
  }

  // Ordered teardown; see the file-level comment. The two shutdown posts land
  // in the bridge inbox before Stop(), so the final drain executes them.
  server->shutdown();
  coordinator_target->store(nullptr, std::memory_order_release);
  // Snapshot-writer drain between shutdown() and reset(), per the shutdown
  // contract in meta_state_machine.h: an in-flight when_done must reach the
  // core while it is still alive.
  state_machine->WaitForSnapshotWriterIdle();
  server.reset();
  if (ctl != nullptr) {
    ctl->Shutdown();
  }
  listener->shutdown();
  bridge->Stop();
  worker.RequestStop();
  worker_thread.join();
  coordinator.reset();

  if (exit_code == 0) {
    spdlog::info("node {} stopped", options.id_);
  }
  return exit_code;
}
