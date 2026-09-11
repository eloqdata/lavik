#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <array>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "celer/net/server.h"
#include "celer/net/tcp_listener.h"
#include "celer/net/tcp_stream.h"
#include "celer/runtime/worker.h"
#include "gtest/gtest.h"
#include "keylane/cluster/control_protocol.h"
#include "keylane/cluster/meta_client.h"
#include "keylane/cluster/node_control.h"
#include "keylane/command.h"
#include "keylane/memory.h"
#include "keylane/metrics.h"
#include "keylane/replication.h"
#include "keylane/storage/engine.h"
#include "keylane/tx/tx_shard.h"
#include "tests/support/process.h"

namespace {
using namespace std::chrono_literals;

constexpr std::uint64_t kMiB = 1024 * 1024;

absl::Status TestFailure(std::string_view message) {
  return absl::FailedPreconditionError(std::string(message));
}

struct NativeStreamPair {
  celer::TcpStream source_;
  celer::TcpStream peer_;
};

celer::Task<absl::StatusOr<NativeStreamPair>> OpenNativeStreamPair(
    celer::Worker& worker) {
  celer::TcpListener listener;
  absl::Status bound = listener.Bind(&worker, "127.0.0.1", 0);
  if (!bound.ok()) co_return bound;
  sockaddr_in address{};
  socklen_t size = sizeof(address);
  if (::getsockname(listener.NativeFd(), reinterpret_cast<sockaddr*>(&address),
                    &size) != 0) {
    const int error = errno;
    listener.Close().IgnoreError();
    co_return absl::UnknownError(std::string("getsockname failed: ") +
                                 std::strerror(error));
  }
  auto peer = co_await celer::ConnectTcp(
      worker, "127.0.0.1", ntohs(address.sin_port), std::chrono::seconds(5));
  if (!peer.ok()) {
    listener.Close().IgnoreError();
    co_return peer.status();
  }
  auto source = co_await listener.Accept();
  listener.Close().IgnoreError();
  if (!source.ok()) {
    peer->Close().IgnoreError();
    co_return source.status();
  }
  co_return NativeStreamPair{.source_ = celer::TcpStream(*source),
                             .peer_ = std::move(*peer)};
}

celer::Task<absl::StatusOr<std::string>> ReadNativeLine(
    celer::TcpStream& stream) {
  std::string line;
  std::array<std::byte, 1> byte{};
  while (line.size() < 4096) {
    auto read = co_await stream.ReadSome(byte);
    if (!read.ok()) co_return read.status();
    if (*read == 0) co_return absl::UnavailableError("native peer closed");
    line.push_back(static_cast<char>(byte[0]));
    if (line.ends_with("\r\n")) co_return line;
  }
  co_return absl::ResourceExhaustedError("native protocol line is too long");
}

std::vector<std::string_view> SplitNativeLine(std::string_view line) {
  std::vector<std::string_view> words;
  while (!line.empty()) {
    while (!line.empty() && (line.front() == ' ' || line.front() == '\r' ||
                             line.front() == '\n')) {
      line.remove_prefix(1);
    }
    if (line.empty()) break;
    const std::size_t end = line.find_first_of(" \r\n");
    words.push_back(line.substr(0, end));
    if (end == std::string_view::npos) break;
    line.remove_prefix(end);
  }
  return words;
}

celer::Task<absl::Status> ServeNativeAndClose(
    keylane::ReplicationManager* replication, celer::TcpStream stream,
    std::vector<std::string> args, std::uint64_t client_id) {
  absl::Status status = co_await replication->ServeNativeConnection(
      stream, std::move(args), client_id, "127.0.0.1", false);
  stream.Close().IgnoreError();
  co_return status;
}

std::string PopulationGroupToken(std::string_view group_id) {
  // Population group ids are opaque application strings, so the native wire
  // carries their bytes as a delimiter-safe lowercase hex continuity token.
  constexpr char kHex[] = "0123456789abcdef";
  std::string token;
  token.reserve(group_id.size() * 2);
  for (unsigned char byte : group_id) {
    token.push_back(kHex[byte >> 4]);
    token.push_back(kHex[byte & 0x0f]);
  }
  return token;
}

std::vector<std::string> PopulationControlArgs(
    const keylane::RebuildIdentity& requested, std::uint64_t applied_lsn,
    std::string transport_target_boot,
    std::optional<std::string> transport_group_token = std::nullopt) {
  std::string group_token =
      transport_group_token.value_or(PopulationGroupToken(requested.group_id_));
  return {
      "KLPSYNC",
      "1",
      "?" + requested.target_node_id_ + ":6380",
      std::move(group_token),
      requested.source_history_id_,
      std::string(40, '1'),
      std::move(transport_target_boot),
      std::to_string(applied_lsn),
      "POPULATION",
      requested.group_id_,
      requested.assignment_id_,
      requested.source_assignment_id_,
      std::to_string(requested.term_),
      std::to_string(requested.directive_revision_),
      requested.authority_id_,
      requested.source_node_id_,
      requested.source_boot_id_,
      requested.source_history_id_,
      requested.target_node_id_,
      requested.target_boot_id_,
      requested.operation_id_,
      requested.directive_id_,
      requested.attempt_id_,
      std::to_string(requested.manifest_revision_),
      requested.manifest_id_.Hex(),
      std::to_string(requested.partition_replication_epoch_),
  };
}

void EnsureTxRuntime() {
  if (keylane::tx::TxRuntime::Get() == nullptr) {
    keylane::tx::TxRuntime::Create(1);
  }
}

class SourceHistoryHoldService final : public celer::Service {
 public:
  SourceHistoryHoldService(keylane::storage::StorageEngine* storage,
                           keylane::ReplicationManager* replication)
      : storage_(storage), replication_(replication) {}

  void Prepare(unsigned thread_count) override {
    if (thread_count != 1) {
      result_ = TestFailure("source history hold test requires one worker");
    }
  }

  celer::Task<absl::Status> Run(celer::Worker& worker,
                                celer::ServiceContext) override {
    keylane::BindMemoryAccountingShard(worker.id());
    keylane::tx::TxRuntime::Get()->shard(worker.id()).Bind(worker);
    if (result_.ok()) result_ = co_await storage_->InitializeWorker(worker);
    if (result_.ok()) {
      replication_->StorageReady(worker);
      result_ = co_await Exercise();
    }
    replication_->RequestShutdown();
    absl::Status quiesced = co_await replication_->QuiesceForShutdown();
    if (result_.ok() && !quiesced.ok()) result_ = quiesced;
    worker.RequestStop();
    co_return result_;
  }

  void Stop() noexcept override {}

  const absl::Status& result() const noexcept { return result_; }

 private:
  celer::Task<absl::Status> VerifyPopulationFlowMode(
      const keylane::RebuildIdentity& requested, std::uint64_t applied_lsn,
      std::uint64_t requested_flow_lsn, std::string_view expected_mode,
      std::optional<std::string> transport_group_token = std::nullopt) {
    celer::Worker& worker = *celer::ThisWorker().self_;
    auto control = co_await OpenNativeStreamPair(worker);
    if (!control.ok()) co_return control.status();
    celer::TcpStream control_peer = std::move(control->peer_);
    std::vector<std::string> control_args =
        PopulationControlArgs(requested, applied_lsn, requested.target_boot_id_,
                              std::move(transport_group_token));
    worker.Spawn(ServeNativeAndClose(replication_, std::move(control->source_),
                                     std::move(control_args), 901));
    auto control_line = co_await ReadNativeLine(control_peer);
    if (!control_line.ok()) {
      control_peer.Close().IgnoreError();
      co_return control_line.status();
    }
    const std::vector<std::string_view> control_words =
        SplitNativeLine(*control_line);
    if (control_words.size() != 8 || control_words.front() != "+KLFULLRESYNC" ||
        control_words[3] != PopulationGroupToken(requested.group_id_)) {
      control_peer.Close().IgnoreError();
      co_return TestFailure(
          "population KLPSYNC returned the wrong group token");
    }
    const std::string session_id(control_words[1]);
    const std::string capability(control_words[7]);

    // KLPSYNC always returns session metadata under the historical reply name;
    // the all-flow KLFLOW barrier below is the protocol point that chooses
    // CONTINUE versus FULL.
    auto flow = co_await OpenNativeStreamPair(worker);
    if (!flow.ok()) {
      control_peer.Close().IgnoreError();
      co_return flow.status();
    }
    celer::TcpStream flow_peer = std::move(flow->peer_);
    std::vector<std::string> flow_args{"KLFLOW",
                                       "1",
                                       session_id,
                                       "0",
                                       std::to_string(requested_flow_lsn),
                                       "0",
                                       capability};
    worker.Spawn(ServeNativeAndClose(replication_, std::move(flow->source_),
                                     std::move(flow_args), 902));
    auto flow_line = co_await ReadNativeLine(flow_peer);
    flow_peer.Close().IgnoreError();
    control_peer.Close().IgnoreError();
    if (!flow_line.ok()) co_return flow_line.status();
    const std::vector<std::string_view> flow_words =
        SplitNativeLine(*flow_line);
    if (flow_words.size() != 4 || flow_words[0] != "+KLFLOW" ||
        flow_words[1] != session_id || flow_words[2] != "0" ||
        flow_words[3] != expected_mode) {
      co_return TestFailure("population KLFLOW selected the wrong mode");
    }
    co_return co_await replication_
        ->ClearClusterRebuildSourceAuthorizationsForSessionReplacement();
  }

  celer::Task<absl::Status> VerifyPopulationControlRejected(
      const keylane::RebuildIdentity& requested, std::uint64_t applied_lsn,
      std::string transport_target_boot) {
    celer::Worker& worker = *celer::ThisWorker().self_;
    auto control = co_await OpenNativeStreamPair(worker);
    if (!control.ok()) co_return control.status();
    celer::TcpStream control_peer = std::move(control->peer_);
    std::vector<std::string> control_args = PopulationControlArgs(
        requested, applied_lsn, std::move(transport_target_boot));
    worker.Spawn(ServeNativeAndClose(replication_, std::move(control->source_),
                                     std::move(control_args), 903));
    auto control_line = co_await ReadNativeLine(control_peer);
    control_peer.Close().IgnoreError();
    if (control_line.ok()) {
      // An unexpected admission may already own a registered master session;
      // join it before failing so later lifecycle assertions stay
      // deterministic.
      (void)co_await replication_
          ->ClearClusterRebuildSourceAuthorizationsForSessionReplacement();
      co_return TestFailure(
          "population KLPSYNC accepted mismatched continuity anchors");
    }
    co_return absl::OkStatus();
  }

  celer::Task<absl::StatusOr<keylane::RebuildIdentity>> InitializePopulation() {
    const keylane::ReplicationIdentity local =
        co_await replication_->ObserveIdentity();
    auto manifest = keylane::PopulationManifest::Create({});
    if (!manifest.ok()) co_return manifest.status();
    keylane::RebuildIdentity identity{
        .group_id_ = "group-1",
        .assignment_id_ = "00000000000000000000000000000001",
        .term_ = 1,
        .directive_revision_ = 1,
        .authority_id_ = "initial-authority",
        .target_node_id_ = local.local_node_id_,
        .target_boot_id_ = local.boot_id_,
        .target_history_id_ = local.local_history_id_,
        .operation_id_ = "initial-operation",
        .directive_id_ = "initialize-empty",
        .attempt_id_ = "initial-attempt",
        .manifest_revision_ = 1,
        .manifest_id_ = manifest->id(),
        .partition_replication_epoch_ = 1,
    };
    auto started = co_await replication_->StartEmptyPopulationInitialization(
        identity, *manifest);
    if (!started.ok()) co_return started.status();
    absl::Status completed = co_await started->Await();
    if (!completed.ok()) co_return completed;
    co_return identity;
  }

  celer::Task<absl::Status> Exercise() {
    auto population = co_await InitializePopulation();
    if (!population.ok()) co_return population.status();
    const keylane::ReplicationIdentity local =
        co_await replication_->ObserveIdentity();
    keylane::SourceHistoryHoldDesired desired{
        .group_id_ = population->group_id_,
        .recovery_generation_ = 17,
        .source_assignment_id_ = population->assignment_id_,
        .source_boot_id_ = local.boot_id_,
        .source_history_id_ = local.local_history_id_,
        .manifest_revision_ = population->manifest_revision_,
        .manifest_id_ = population->manifest_id_,
        .partition_replication_epoch_ =
            population->partition_replication_epoch_,
    };
    absl::Status reconciled =
        co_await replication_->ReconcileClusterSourceHistoryHold(desired);
    if (!reconciled.ok()) co_return reconciled;

    keylane::RebuildDirective authorization{
        .identity_ =
            {
                .group_id_ = population->group_id_,
                .assignment_id_ = "candidate-assignment",
                .term_ = population->term_ + 1,
                .directive_revision_ = 2,
                .authority_id_ = "candidate-authority",
                .source_node_id_ = local.local_node_id_,
                .source_assignment_id_ = population->assignment_id_,
                .source_boot_id_ = local.boot_id_,
                .source_history_id_ = local.local_history_id_,
                .target_node_id_ = std::string(40, 'e'),
                .target_boot_id_ = std::string(40, 'f'),
                .operation_id_ = "failover-operation",
                .directive_id_ = "authorize-source",
                .attempt_id_ = "authorize-attempt",
                .manifest_revision_ = population->manifest_revision_,
                .manifest_id_ = population->manifest_id_,
                .partition_replication_epoch_ =
                    population->partition_replication_epoch_,
            },
        .flow_count_ = 1,
        .safe_source_active_ = true,
    };
    auto wrong_scope = authorization;
    wrong_scope.identity_.source_assignment_id_ = "stale-assignment";
    absl::Status denied =
        co_await replication_->AuthorizeClusterRebuildSource(wrong_scope);
    if (denied.code() != absl::StatusCode::kFailedPrecondition) {
      co_return TestFailure(
          "desired history hold bypassed exact source authorization");
    }
    absl::Status authorized =
        co_await replication_->AuthorizeClusterRebuildSource(authorization);
    if (!authorized.ok()) co_return authorized;

    auto stale_freeze =
        co_await replication_->FreezeAndAuthorizeClusterRebuildSource(
            desired.recovery_generation_ - 1, authorization);
    if (stale_freeze.ok() ||
        stale_freeze.status().code() != absl::StatusCode::kFailedPrecondition) {
      co_return TestFailure("frozen source accepted a stale hold generation");
    }
    auto wrong_frozen_scope = authorization;
    wrong_frozen_scope.identity_.source_assignment_id_ = "stale-assignment";
    auto mismatched_freeze =
        co_await replication_->FreezeAndAuthorizeClusterRebuildSource(
            desired.recovery_generation_, std::move(wrong_frozen_scope));
    if (mismatched_freeze.ok() || mismatched_freeze.status().code() !=
                                      absl::StatusCode::kFailedPrecondition) {
      co_return TestFailure("frozen source accepted a mismatched source scope");
    }

    auto frozen = co_await replication_->FreezeAndAuthorizeClusterRebuildSource(
        desired.recovery_generation_, authorization);
    if (!frozen.ok()) co_return frozen.status();
    if (frozen->recovery_generation_ != desired.recovery_generation_ ||
        frozen->watermark_.history_id_ != desired.source_history_id_ ||
        frozen->watermark_.next_lsns_.size() != 1) {
      co_return TestFailure("frozen source returned incomplete frontier data");
    }
    absl::Status published =
        co_await storage_->PublishEphemeralReplicationCommand(
            0, {"PING", "after-freeze"});
    if (!published.ok()) co_return published;
    auto replay_authorization = authorization;
    replay_authorization.identity_.operation_id_ = "replayed-operation";
    replay_authorization.identity_.directive_id_ = "replayed-directive";
    replay_authorization.identity_.attempt_id_ = "replayed-attempt";
    auto replayed =
        co_await replication_->FreezeAndAuthorizeClusterRebuildSource(
            desired.recovery_generation_, std::move(replay_authorization));
    if (!replayed.ok()) co_return replayed.status();
    if (replayed->recovery_generation_ != frozen->recovery_generation_ ||
        replayed->watermark_.history_id_ != frozen->watermark_.history_id_ ||
        replayed->watermark_.next_lsns_ != frozen->watermark_.next_lsns_) {
      co_return TestFailure("frozen source replay changed its final frontier");
    }
    auto fallback_candidate = authorization;
    fallback_candidate.identity_.assignment_id_ = "fallback-assignment";
    fallback_candidate.identity_.target_node_id_ = std::string(40, 'c');
    fallback_candidate.identity_.target_boot_id_ = std::string(40, 'b');
    fallback_candidate.identity_.operation_id_ = "fallback-operation";
    fallback_candidate.identity_.directive_id_ = "fallback-directive";
    fallback_candidate.identity_.attempt_id_ = "fallback-attempt";
    auto fallback =
        co_await replication_->FreezeAndAuthorizeClusterRebuildSource(
            desired.recovery_generation_, std::move(fallback_candidate));
    if (!fallback.ok()) co_return fallback.status();
    if (fallback->watermark_.history_id_ != frozen->watermark_.history_id_ ||
        fallback->watermark_.next_lsns_ != frozen->watermark_.next_lsns_) {
      co_return TestFailure(
          "fallback candidate did not reuse the source-wide frozen frontier");
    }

    reconciled =
        co_await replication_->ReconcileClusterSourceHistoryHold(std::nullopt);
    if (!reconciled.ok()) co_return reconciled;
    auto released =
        co_await replication_->FreezeAndAuthorizeClusterRebuildSource(
            desired.recovery_generation_, authorization);
    if (released.ok() ||
        released.status().code() != absl::StatusCode::kFailedPrecondition) {
      co_return TestFailure("released source-history hold replayed evidence");
    }
    reconciled =
        co_await replication_->ReconcileClusterSourceHistoryHold(desired);
    if (!reconciled.ok()) co_return reconciled;
    auto unarmed =
        co_await replication_->FreezeAndAuthorizeClusterRebuildSource(
            desired.recovery_generation_, authorization);
    if (unarmed.ok() ||
        unarmed.status().code() != absl::StatusCode::kFailedPrecondition) {
      co_return TestFailure("FDS reconcile re-armed a released history hold");
    }
    authorized =
        co_await replication_->AuthorizeClusterRebuildSource(authorization);
    if (!authorized.ok()) co_return authorized;
    auto refrozen =
        co_await replication_->FreezeAndAuthorizeClusterRebuildSource(
            desired.recovery_generation_, authorization);
    if (!refrozen.ok()) co_return refrozen.status();
    if (refrozen->watermark_.next_lsns_ == frozen->watermark_.next_lsns_) {
      co_return TestFailure(
          "re-armed frozen source reused evidence across hold release");
    }

    // A full-state replacement withdraws live sockets before replaying its
    // complete current authorization. The target may still present the
    // delivery identity under which this exact data relationship first became
    // Ready; only the stable export scope is a continuity anchor.
    absl::Status replaced =
        co_await replication_
            ->ClearClusterRebuildSourceAuthorizationsForSessionReplacement();
    if (!replaced.ok()) co_return replaced;
    auto replacement_authorization = authorization;
    ++replacement_authorization.identity_.term_;
    ++replacement_authorization.identity_.directive_revision_;
    replacement_authorization.identity_.authority_id_ = "replacement-authority";
    replacement_authorization.identity_.operation_id_ = "replacement-operation";
    replacement_authorization.identity_.directive_id_ = "replacement-directive";
    replacement_authorization.identity_.attempt_id_ = "replacement-attempt";
    authorized = co_await replication_->AuthorizeClusterRebuildSource(
        replacement_authorization);
    if (!authorized.ok()) co_return authorized;
    absl::Status stale_control_authorization =
        co_await replication_->AuthorizeClusterRebuildSource(authorization);
    if (stale_control_authorization.code() !=
        absl::StatusCode::kFailedPrecondition) {
      co_return TestFailure(
          "old delivery identity was accepted as current control authority");
    }
    const auto prior_delivery_identity = authorization.identity_;
    if (absl::Status continued = co_await VerifyPopulationFlowMode(
            prior_delivery_identity, refrozen->watermark_.next_lsns_.front(),
            refrozen->watermark_.next_lsns_.front(), "CONTINUE");
        !continued.ok()) {
      co_return continued;
    }
    authorized = co_await replication_->AuthorizeClusterRebuildSource(
        replacement_authorization);
    if (!authorized.ok()) co_return authorized;
    // args[3] is the previous session's continuity token, while POPULATION
    // carries the raw group id. A valid token for another group may start a
    // safe FULL transfer, but must never inherit this group's backlog.
    if (absl::Status refused_token = co_await VerifyPopulationFlowMode(
            prior_delivery_identity, refrozen->watermark_.next_lsns_.front(),
            refrozen->watermark_.next_lsns_.front(), "FULL",
            PopulationGroupToken("another-group"));
        !refused_token.ok()) {
      co_return refused_token;
    }
    authorized = co_await replication_->AuthorizeClusterRebuildSource(
        replacement_authorization);
    if (!authorized.ok()) co_return authorized;
    if (absl::Status refused = co_await VerifyPopulationFlowMode(
            prior_delivery_identity,
            refrozen->watermark_.next_lsns_.front() + 1,
            refrozen->watermark_.next_lsns_.front(), "FULL");
        !refused.ok()) {
      co_return refused;
    }
    authorized = co_await replication_->AuthorizeClusterRebuildSource(
        replacement_authorization);
    if (!authorized.ok()) co_return authorized;
    if (absl::Status continued_from_initial = co_await VerifyPopulationFlowMode(
            prior_delivery_identity, 1, 1, "CONTINUE");
        !continued_from_initial.ok()) {
      co_return continued_from_initial;
    }
    authorized = co_await replication_->AuthorizeClusterRebuildSource(
        replacement_authorization);
    if (!authorized.ok()) co_return authorized;
    if (absl::Status denied_boot = co_await VerifyPopulationControlRejected(
            prior_delivery_identity, refrozen->watermark_.next_lsns_.front(),
            std::string(40, '0'));
        !denied_boot.ok()) {
      co_return denied_boot;
    }
    std::vector<keylane::RebuildIdentity> scope_mismatches;
    auto mismatch = prior_delivery_identity;
    mismatch.group_id_ = "group-2";
    scope_mismatches.push_back(mismatch);
    mismatch = prior_delivery_identity;
    mismatch.source_history_id_ = std::string(40, '9');
    scope_mismatches.push_back(mismatch);
    mismatch = prior_delivery_identity;
    ++mismatch.manifest_revision_;
    scope_mismatches.push_back(mismatch);
    mismatch = prior_delivery_identity;
    mismatch.manifest_id_.bytes_.front() ^= 0xff;
    scope_mismatches.push_back(mismatch);
    mismatch = prior_delivery_identity;
    ++mismatch.partition_replication_epoch_;
    scope_mismatches.push_back(mismatch);
    mismatch = prior_delivery_identity;
    mismatch.target_boot_id_ = std::string(40, '0');
    scope_mismatches.push_back(mismatch);
    for (const auto& scope_mismatch : scope_mismatches) {
      if (absl::Status denied_scope = co_await VerifyPopulationControlRejected(
              scope_mismatch, refrozen->watermark_.next_lsns_.front(),
              scope_mismatch.target_boot_id_);
          !denied_scope.ok()) {
        co_return denied_scope;
      }
    }

    const auto source_assignment =
        keylane::cluster::AssignmentId::Parse(population->assignment_id_);
    const auto candidate_assignment = keylane::cluster::AssignmentId::Parse(
        "00000000000000000000000000000002");
    const auto source_node =
        keylane::cluster::NodeId::Parse(local.local_node_id_);
    const auto source_boot = keylane::cluster::NodeId::Parse(local.boot_id_);
    const auto source_history =
        keylane::cluster::NodeId::Parse(local.local_history_id_);
    const auto target_node =
        keylane::cluster::NodeId::Parse(std::string(40, 'e'));
    const auto target_boot =
        keylane::cluster::NodeId::Parse(std::string(40, 'f'));
    const auto operation = keylane::cluster::OperationId::Parse(
        "00000000000000000000000000000003");
    const auto directive = keylane::cluster::DirectiveId::Parse(
        "00000000000000000000000000000004");
    const auto attempt =
        keylane::cluster::AttemptId::Parse("00000000000000000000000000000005");
    if (!source_assignment.has_value() || !candidate_assignment.has_value() ||
        !source_node.has_value() || !source_boot.has_value() ||
        !source_history.has_value() || !target_node.has_value() ||
        !target_boot.has_value() || !operation.has_value() ||
        !directive.has_value() || !attempt.has_value()) {
      co_return TestFailure("adapter test identity is non-canonical");
    }
    keylane::cluster::NodeDirective adapter_frozen{
        .anchor_ = {.group_id_ = population->group_id_,
                    .assignment_id_ = *candidate_assignment,
                    .group_term_ = replacement_authorization.identity_.term_,
                    .authority_version_ = 1,
                    .grant_revision_ = 1},
        .operation_id_ = *operation,
        .directive_id_ = *directive,
        .attempt_id_ = *attempt,
        .directive_revision_ = 3,
        .kind_ = keylane::cluster::NodeDirective::Kind::kAuthorizeSource,
        .target_node_id_ = *target_node,
        .target_boot_id_ = *target_boot,
        .source_node_id_ = *source_node,
        .source_assignment_id_ = *source_assignment,
        .source_boot_id_ = *source_boot,
        .source_replication_history_id_ = *source_history,
        .flow_count_ = 1,
        .manifest_revision_ = population->manifest_revision_,
        .manifest_digest_ = population->manifest_id_.bytes_,
        .partition_replication_epoch_ =
            population->partition_replication_epoch_,
        .frozen_source_ =
            keylane::cluster::FrozenSourceInput{
                .recovery_generation_ = desired.recovery_generation_,
                .excluded_group_term_ =
                    replacement_authorization.identity_.term_ - 1,
                .excluded_authority_version_ = 1,
                .excluded_grant_revision_ = 1,
            },
    };
    std::unique_ptr<keylane::cluster::NodeControlActions> adapter =
        keylane::cluster::CreateReplicationNodeControlActions(*replication_);
    auto stale_adapter = adapter_frozen;
    --stale_adapter.frozen_source_->recovery_generation_;
    keylane::cluster::NodeDirectiveCompletion adapter_completion =
        co_await adapter->StartDirective(std::move(stale_adapter));
    auto adapter_result = adapter_completion.terminal_result();
    if (!adapter_completion.started() || !adapter_result.has_value() ||
        adapter_result->ok() ||
        adapter_result->status().code() !=
            absl::StatusCode::kFailedPrecondition) {
      co_return TestFailure("frozen source adapter discarded execution status");
    }

    adapter_completion = co_await adapter->StartDirective(adapter_frozen);
    adapter_result = adapter_completion.terminal_result();
    if (!adapter_completion.started() || !adapter_result.has_value() ||
        !adapter_result->ok()) {
      co_return TestFailure("frozen source adapter did not return evidence");
    }
    auto adapter_evidence =
        keylane::cluster::control::DecodeFrozenSourceEvidence(**adapter_result);
    if (!adapter_evidence.ok() ||
        adapter_evidence->recovery_generation != desired.recovery_generation_ ||
        adapter_evidence->source_history_id != desired.source_history_id_ ||
        adapter_evidence->final_next_lsns != refrozen->watermark_.next_lsns_) {
      co_return TestFailure("frozen source adapter encoded the wrong capture");
    }
    const std::string first_adapter_result = **adapter_result;
    adapter_completion = co_await adapter->StartDirective(adapter_frozen);
    adapter_result = adapter_completion.terminal_result();
    if (!adapter_completion.started() || !adapter_result.has_value() ||
        !adapter_result->ok() || **adapter_result != first_adapter_result) {
      co_return TestFailure("frozen source adapter replay changed proof bytes");
    }

    keylane::cluster::NodeDirective ordinary = adapter_frozen;
    ordinary.directive_revision_ = 4;
    ordinary.directive_id_ = keylane::cluster::DirectiveId::FromBytes(
        keylane::cluster::DirectiveId::Bytes{6});
    ordinary.attempt_id_ = keylane::cluster::AttemptId::FromBytes(
        keylane::cluster::AttemptId::Bytes{7});
    ordinary.frozen_source_.reset();
    keylane::cluster::NodeDirectiveCompletion ordinary_completion =
        co_await adapter->StartDirective(std::move(ordinary));
    auto ordinary_result = ordinary_completion.terminal_result();
    if (!ordinary_completion.started() || !ordinary_result.has_value() ||
        !ordinary_result->ok() || !(**ordinary_result).empty()) {
      co_return TestFailure(
          "ordinary source authorization changed its status-only result");
    }

    reconciled =
        co_await replication_->ReconcileClusterSourceHistoryHold(desired);
    if (!reconciled.ok()) {
      co_return TestFailure("exact source-history hold replay was rejected");
    }
    auto stale = desired;
    --stale.recovery_generation_;
    reconciled =
        co_await replication_->ReconcileClusterSourceHistoryHold(stale);
    if (reconciled.code() != absl::StatusCode::kFailedPrecondition) {
      co_return TestFailure("source-history hold generation regressed");
    }

    absl::Status cleared =
        co_await replication_
            ->ClearClusterRebuildSourceAuthorizationsForSessionReplacement();
    if (!cleared.ok()) co_return cleared;
    cleared = co_await replication_->RevokeClusterRebuildSourceAuthorizations();
    if (!cleared.ok()) co_return cleared;
    auto retained = co_await replication_->CaptureNativeReplicationWatermark();
    if (!retained.ok() || !retained->has_value() ||
        (**retained).history_id_ != local.local_history_id_) {
      co_return TestFailure(
          "authorization cleanup released the armed source-history hold");
    }

    auto mismatched = desired;
    ++mismatched.recovery_generation_;
    mismatched.source_history_id_ = std::string(40, 'd');
    reconciled = co_await replication_->ReconcileClusterSourceHistoryHold(
        std::move(mismatched));
    if (!reconciled.ok()) co_return reconciled;
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    do {
      retained = co_await replication_->CaptureNativeReplicationWatermark();
      if (!retained.ok()) co_return retained.status();
      if (!retained->has_value()) break;
      cleared = co_await celer::SleepFor(*celer::ThisWorker().self_, 10ms);
      if (!cleared.ok()) co_return cleared;
    } while (std::chrono::steady_clock::now() < deadline);
    if (retained->has_value()) {
      co_return TestFailure(
          "source-history mismatch did not release idle history");
    }
    co_return co_await replication_->ReconcileClusterSourceHistoryHold(
        std::nullopt);
  }

  keylane::storage::StorageEngine* storage_ = nullptr;
  keylane::ReplicationManager* replication_ = nullptr;
  absl::Status result_ = absl::OkStatus();
};

TEST(SourceHistoryHoldIntegrationTest,
     ReconcilesBootLocalAuthorizationAndReleaseLifecycle) {
  keylane::test::TempDirectory directory("source-history-hold");
  const std::filesystem::path data = directory.path() / "node.data";
  keylane::test::CreateDataFile(data, 128 * kMiB);

  keylane::storage::StorageEngineOptions storage_options;
  storage_options.data_files_ = {data.string()};
  storage_options.expiration_authority_ = false;
  storage_options.buffers_.registered_bytes_ = 64 * kMiB;
  storage_options.replication_publish_queue_bytes_ = 16 * kMiB;
  keylane::storage::StorageEngine storage(std::move(storage_options));
  keylane::InitWorkerMetrics(1);
  ASSERT_TRUE(keylane::InitMemoryLimit(512 * kMiB, 1).ok());
  ASSERT_TRUE(storage.Prepare(1).ok());

  keylane::ReplicationOptions replication_options;
  replication_options.cluster_enabled_ = true;
  replication_options.cluster_population_managed_ = true;
  replication_options.node_id_override_ = std::string(40, '7');
  keylane::ReplicationManager replication(
      &storage, std::move(replication_options), std::nullopt);
  keylane::InitStorage(&storage, &replication);
  EnsureTxRuntime();

  SourceHistoryHoldService service(&storage, &replication);
  celer::Server server;
  server.AddService(&service);
  celer::ServerOptions runtime;
  runtime.thread_count_ = 1;
  runtime.pin_workers_ = false;
  runtime.recv_buffer_count_ = 0;
  ASSERT_TRUE(server.Start(runtime).ok());
  server.WaitUntilStopped();
  EXPECT_TRUE(service.result().ok()) << service.result();
}

}  // namespace
