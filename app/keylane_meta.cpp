// Entry point of the Raft-backed meta control plane.
// keylane-meta is the only keylane artifact that links NuRaft: cluster
// membership and other control-plane metadata live on the meta plane, while
// the data-plane binary (keylane) and its tests stay Raft-free by
// construction (guarded in the root CMakeLists.txt).
//
// Process layout:
//   - main thread: CLI parse, assembly, startup waits, signal polling, and
//     the ordered teardown. raft_server construction/teardown happen here;
//     NuRaft's public API is thread-safe.
//   - one celer Runtime worker: ctl/future Data Node control transport, with
//     authentication determined by the selected listener mode.
//   - one bounded proposal-executor thread: synchronous entry into NuRaft's
//     mutation/snapshot APIs, keeping their locks and WAL IO off Celer.
//   - NuRaft native Asio workers: peer RPC and timers. NuRaft commit/append
//     threads perform synchronous durability IO; completion and role events
//     return to Celer through the Runtime's foreign executor mailbox.
//
// Teardown order (main thread, on SIGTERM/SIGINT):
//   ctl Shutdown() -> proposal executor drain -> raft_launcher::shutdown()
//   -> MetaStateMachine::WaitForSnapshotWriterIdle() -> release Raft ref ->
//   Celer Runtime stop + join -> coordinator release.
// shutdown() joins the commit thread — the only producer of automatic
// snapshot jobs — and the writer drain lets an in-flight when_done reach the
// still-alive core before reset (the shutdown contract in
// state_machine.h). Once those producers quiesce, ForeignExecutor drains its
// accepted notifications before the generic Celer Runtime is stopped, and
// raft_server owns the
// nuraft::context through a unique_ptr member — the caller must never delete
// the context itself.

#include <arpa/inet.h>
#include <signal.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <exception>
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
#include "celer/runtime/runtime.h"
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

#include "keylane/meta/coordinator.h"
#include "keylane/meta/ctl_server.h"
#include "keylane/meta/identity_verifier.h"
#include "keylane/meta/nuraft_asio_transport.h"
#include "keylane/meta/nuraft_log_store.h"
#include "keylane/meta/nuraft_state_mgr.h"
#include "keylane/meta/observation_store.h"
#include "keylane/meta/proposal_executor.h"
#include "keylane/meta/state_machine.h"
#include "keylane/version.h"

namespace {

using keylane::meta::MetaAsioTransportConfig;
using keylane::meta::MetaCoordinator;
using keylane::meta::MetaCoordinatorOptions;
using keylane::meta::MetaCtlServer;
using keylane::meta::MetaCtlServerOptions;
using keylane::meta::MetaMembershipGate;
using keylane::meta::MetaProposalExecutor;
using keylane::meta::MetaStateMachine;
using keylane::meta::NuraftStateMgr;

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

struct CliOptions {
  int id_ = 0;
  bool has_id_ = false;
  std::string raft_addr_;  // "ip:port": raft bind AND advertised endpoint
  std::string data_dir_;
  std::string ctl_addr_;    // optional remote "ip:port" control surface
  std::string ctl_socket_;  // default: <data-dir>/meta-admin.sock
  std::vector<uid_t> ctl_allowed_uids_;
  std::string ctl_tls_ca_;
  std::string ctl_tls_cert_;
  std::string ctl_tls_key_;
  bool bootstrap_ = false;
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
      "usage: %s --id N --addr ip:port --data-dir PATH "
      "[--ctl-socket PATH | --ctl-addr ip:port] "
      "[--bootstrap]\n"
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
      std::printf("keylane-meta %.*s (nuraft %s)\n",
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

}  // namespace

int main(int argc, char** argv) {
  bool early_exit = false;
  int early_exit_code = 0;
  auto parsed = ParseCli(argc, argv, argv[0], &early_exit, &early_exit_code);
  if (early_exit) {
    return early_exit_code;
  }
  if (!parsed.ok()) {
    std::fprintf(stderr, "keylane-meta: %s\n",
                 std::string(parsed.status().message()).c_str());
    PrintUsage(argv[0]);
    return 1;
  }
  const CliOptions options = *parsed;
  auto raft_endpoint = ParseEndpointArg(options.raft_addr_);
  if (!raft_endpoint.ok()) {
    std::fprintf(stderr, "keylane-meta: --addr: %s\n",
                 std::string(raft_endpoint.status().message()).c_str());
    return 1;
  }
  std::optional<EndpointParts> ctl_endpoint;
  if (!options.ctl_addr_.empty()) {
    auto parsed_ctl = ParseEndpointArg(options.ctl_addr_);
    if (!parsed_ctl.ok()) {
      std::fprintf(stderr, "keylane-meta: --ctl-addr: %s\n",
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
    // The current durable layout is intentionally incompatible with the
    // legacy single-file WAL and "LSN1" snapshots. Those formats held no
    // production data, so boot refuses them loudly and the remedy is to wipe
    // the directory rather than attempt migration.
    spdlog::critical("state machine open failed: {}",
                     machine_or.status().message());
    return 1;
  }
  nuraft::ptr<MetaStateMachine> state_machine(std::move(*machine_or));

  MetaAsioTransportConfig transport_config;
  transport_config.bind_address_ = raft_endpoint->host_;
  transport_config.tls_ca_cert_file_ = options.tls_ca_;
  transport_config.tls_cert_file_ = options.tls_cert_;
  transport_config.tls_key_file_ = options.tls_key_;
  transport_config.io_threads_ =
      static_cast<std::size_t>(options.raft_io_threads_);
  auto asio_options_or = keylane::meta::BuildMetaAsioOptions(
      transport_config, state_mgr, state_machine);
  if (!asio_options_or.ok()) {
    spdlog::critical("Raft Asio transport setup failed: {}",
                     asio_options_or.status().message());
    return 1;
  }
  nuraft::asio_service::options asio_options = std::move(*asio_options_or);

  // --- Celer runtime ---
  // One worker owns ctl/future Data Node transport. Runtime owns its thread,
  // MPSC mailbox, and wake eventfd. NuRaft posts typed notifications directly
  // through the worker's foreign executor without touching Celer TLS.
  celer::Runtime celer_runtime;
  std::promise<absl::Status> init_promise;
  std::future<absl::Status> init_future = init_promise.get_future();
  celer_runtime.Start(
      /*thread_count=*/
      1,
      [&init_promise](unsigned, celer::Worker& worker) {
        const absl::Status init = worker.Init(celer::WorkerOptions{});
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
  const celer::ForeignExecutor foreign_executor =
      celer_runtime.GetForeignExecutor(/*worker_id=*/0);
  const absl::Status worker_init = init_future.get();
  if (!worker_init.ok()) {
    spdlog::critical("worker init failed: {}", worker_init.message());
    celer_runtime.WaitUntilStopped();
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
  // The ctl surface rejects non-leader writes instead of relying on NuRaft's
  // follower auto-forwarding path.
  params.auto_forwarding_ = false;
  params.return_method_ = nuraft::raft_params::async_handler;
  params.parallel_log_appending_ = false;
  params.wait_for_sm_catchup_on_becoming_leader_ = true;

  auto raft_logger =
      std::make_shared<MetaNuraftLogger>(options.raft_log_level_);

  nuraft::raft_server::init_options init_opts;
  init_opts.skip_initial_election_timeout_ = !options.bootstrap_;
  // Construction necessarily precedes MetaCoordinator assembly because the
  // coordinator needs the raft_server. Remember the latest role edge so an
  // election racing that short window is replayed immediately on attach.
  auto coordinator_target =
      std::make_shared<std::atomic<MetaCoordinator*>>(nullptr);
  auto pending_role = std::make_shared<std::atomic<int>>(-1);
  init_opts.raft_callback_ = [foreign_executor, coordinator_target,
                              pending_role](nuraft::cb_func::Type type,
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
      // NuRaft/Asio threads never call coordinator/Celer-owned state
      // directly. Stale queued edges read the latest role when drained.
      const bool accepted = foreign_executor.Notify(
          [coordinator_target, pending_role]() noexcept {
            MetaCoordinator* target =
                coordinator_target->load(std::memory_order_acquire);
            if (target == nullptr) return;
            const int current = pending_role->load(std::memory_order_acquire);
            current == 1 ? target->BecomeLeader() : target->BecomeFollower();
          });
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
    celer_runtime.RequestStop();
    celer_runtime.WaitUntilStopped();
    return 1;
  }

  int exit_code = 0;
  std::shared_ptr<MetaCtlServer> ctl;
  auto obs_store = std::make_shared<keylane::meta::MetaObservationStore>();
  auto proposal_executor = std::make_unique<MetaProposalExecutor>();
  auto membership_gate = std::make_shared<MetaMembershipGate>();
  nuraft::ptr<nuraft::log_store> raft_log_store = state_mgr->load_log_store();
  auto* wal = static_cast<keylane::meta::NuraftLogStore*>(raft_log_store.get());
  MetaCoordinatorOptions coordinator_options;
  coordinator_options.proposal_executor_ = proposal_executor.get();
  coordinator_options.foreign_executor_ = foreign_executor;
  std::shared_ptr<MetaCoordinator> coordinator =
      std::make_shared<MetaCoordinator>(server, *state_machine, *wal,
                                        *obs_store, coordinator_options);
  coordinator_target->store(coordinator.get(), std::memory_order_release);
  const int role_before_attach = pending_role->load(std::memory_order_acquire);
  if (role_before_attach == 1) {
    coordinator->BecomeLeader();
  } else if (role_before_attach == 0) {
    coordinator->BecomeFollower();
  }

  if (exit_code == 0) {
    MetaCtlServerOptions ctl_options;
    if (ctl_endpoint.has_value()) {
      ctl_options.transport_ =
          options.ctl_tls_ca_.empty()
              ? MetaCtlServerOptions::Transport::kTcpPlaintext
              : MetaCtlServerOptions::Transport::kTcpMtls;
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
    auto ctl_or = MetaCtlServer::Create(
        foreign_executor, server, state_machine, coordinator, obs_store,
        *proposal_executor, membership_gate, std::move(ctl_options));
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

  // Stop ctl ingress first, then quiesce NuRaft/Asio while the Celer worker
  // mailbox and snapshot writer remain alive.
  if (ctl != nullptr) {
    ctl->Shutdown();
  }
  coordinator->BecomeFollower();
  coordinator_target->store(nullptr, std::memory_order_release);
  // No new Celer ingress or leader work is accepted. Drain queued NuRaft
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
  // out of Celer's data-plane Worker loop.
  foreign_executor.WaitUntilIdle();
  celer_runtime.RequestStop();
  celer_runtime.WaitUntilStopped();
  if (celer_runtime.exit_code() != 0) exit_code = 1;
  coordinator.reset();

  if (exit_code == 0) {
    spdlog::info("node {} stopped", options.id_);
  }
  return exit_code;
}
