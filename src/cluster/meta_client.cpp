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

#include "lavik/cluster/meta_client.h"

#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "bycorf/io/storage.h"
#include "bycorf/net/tcp_stream.h"
#include "bycorf/net/tls.h"
#include "bycorf/runtime/sync.h"
#include "bycorf/runtime/worker.h"
#include "lavik/cluster/control_transport.h"
#include "lavik/cluster/meta_control.h"
#include "lavik/cluster/node_control.h"
#include "lavik/cluster/topology.h"
#include "lavik/metrics.h"
#include "lavik/numeric_endpoint.h"
#include "lavik/replication.h"
#include "spdlog/spdlog.h"

namespace lavik::cluster {
namespace {

using namespace std::chrono_literals;

constexpr auto kConnectTimeout = 10s;
constexpr auto kHandshakeTimeout = 10s;
constexpr auto kMaximumSessionProgressTimeout = 10s;
constexpr auto kDeadlinePollInterval = 25ms;
constexpr std::size_t kSessionWriteQueueBytes = 4 * control::kMaxFrameBytes;

bool SameEndpoint(const MetaControlEndpoint& left,
                  const MetaControlEndpoint& right) {
  return left.host_ == right.host_ && left.port_ == right.port_;
}

std::string EndpointText(std::string_view host, std::uint16_t port) {
  if (host.find(':') != std::string_view::npos) {
    return absl::StrCat("[", host, "]:", port);
  }
  return absl::StrCat(host, ":", port);
}

bool IsZero(const control::WireId128& value) {
  return std::all_of(value.begin(), value.end(),
                     [](std::uint8_t byte) { return byte == 0; });
}

absl::StatusOr<std::uint64_t> Entropy64() {
  auto generated = control::GenerateId128();
  if (!generated.ok()) return generated.status();
  std::uint64_t value = 0;
  std::memcpy(&value, generated->data(), sizeof(value));
  return value;
}

struct SocketDeadlineState {
  bycorf::TcpStream* stream_ = nullptr;
  bycorf::Worker* worker_ = nullptr;
  std::chrono::steady_clock::time_point deadline_{};
  bool armed_ = false;
  bool running_ = false;
  bool expired_ = false;
};

bycorf::Task<absl::Status> WatchSocketDeadline(
    std::shared_ptr<SocketDeadlineState> state) {
  while (state->armed_ && state->stream_ != nullptr) {
    const auto now = std::chrono::steady_clock::now();
    if (now >= state->deadline_) {
      state->expired_ = true;
      state->armed_ = false;
      (void)state->stream_->Close();
      break;
    }
    const absl::Status slept = co_await bycorf::SleepFor(
        *state->worker_,
        std::min(
            state->deadline_ - now,
            std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                kDeadlinePollInterval)));
    if (!slept.ok()) break;
  }
  state->running_ = false;
  co_return absl::OkStatus();
}

// One watchdog is reused for every read in a session. Resetting an armed
// deadline is cheap, and the short poll ceiling lets a smaller negotiated
// timeout take effect even while the previous deadline's timer is asleep.
class SocketDeadline {
 public:
  SocketDeadline(bycorf::Worker& worker, bycorf::TcpStream& stream)
      : state_(std::make_shared<SocketDeadlineState>()) {
    state_->stream_ = &stream;
    state_->worker_ = &worker;
  }

  ~SocketDeadline() {
    state_->armed_ = false;
    state_->stream_ = nullptr;
  }

  SocketDeadline(const SocketDeadline&) = delete;
  SocketDeadline& operator=(const SocketDeadline&) = delete;

  absl::Status Arm(std::chrono::milliseconds timeout) {
    if (timeout.count() <= 0) {
      return absl::InvalidArgumentError("socket deadline must be positive");
    }
    state_->deadline_ = std::chrono::steady_clock::now() + timeout;
    state_->expired_ = false;
    state_->armed_ = true;
    if (!state_->running_) {
      state_->running_ = true;
      state_->worker_->Spawn(WatchSocketDeadline(state_));
    }
    return absl::OkStatus();
  }

  bool Disarm() noexcept {
    state_->armed_ = false;
    return state_->expired_;
  }

 private:
  std::shared_ptr<SocketDeadlineState> state_;
};

ProjectionBasis ToDomain(const control::WireProjectionBasis& basis) {
  return ProjectionBasis{
      .control_revision_ = basis.control_revision,
  };
}

AuthorityAnchor ToDomain(const control::WireAuthorityAnchor& anchor) {
  return AuthorityAnchor{
      .group_id_ = anchor.group_id,
      .assignment_id_ = AssignmentId::FromBytes(anchor.assignment_id),
      .group_term_ = anchor.group_term,
  };
}

control::LeaseChallenge ChallengeFor(const control::WireDesiredGroup& group,
                                     std::uint64_t control_revision,
                                     const control::WireId128& nonce) {
  return control::LeaseChallenge{
      .nonce = nonce,
      .control_revision = control_revision,
      .group_id = group.group_id,
      .assignment_id = *group.owner_assignment_id,
      .group_term = group.group_term,
  };
}

control::Directive LiveDirective(const control::WireProjectedDirective& source,
                                 const control::WireId128& session_id,
                                 std::uint64_t source_index) {
  return control::Directive{
      .session_id = session_id,
      .basis = {.control_revision = source_index},
      .authority = source.authority,
      .identity = source.identity,
      .recipient_node_id = source.recipient_node_id,
      .recipient_boot_id = source.recipient_boot_id,
      .target_node_id = source.target_node_id,
      .target_boot_id = source.target_boot_id,
      .source_node_id = source.source_node_id,
      .source_assignment_id = source.source_assignment_id,
      .source_boot_id = source.source_boot_id,
      .source_replication_history_id = source.source_replication_history_id,
      .manifest_revision = source.manifest_revision,
      .manifest_digest = source.manifest_digest,
      .partition_replication_epoch = source.partition_replication_epoch,
      .kind = source.kind,
      .payload = source.payload,

  };
}

class StringTransferSink final : public control::LargeObjectSink {
 public:
  absl::Status Begin(const control::TransferStart& start) override {
    if (start.kind != control::TransferKind::kFullDesiredState &&
        start.kind != control::TransferKind::kNodeControlUpdate) {
      return absl::InvalidArgumentError(
          "Data accepts only bootstrap or control update transfers");
    }
    bytes_.clear();
    kind_ = start.kind;
    expected_ = start.total_length;
    committed_ = false;
    return absl::OkStatus();
  }

  absl::Status Write(std::uint64_t offset, std::string_view bytes) override {
    if (offset != bytes_.size()) {
      return absl::InvalidArgumentError("non-contiguous transfer chunk");
    }
    if (bytes_.size() > expected_ || bytes.size() > expected_ - bytes_.size()) {
      return absl::ResourceExhaustedError("transfer exceeds declared size");
    }
    bytes_.append(bytes);
    return absl::OkStatus();
  }

  absl::Status Commit() override {
    if (bytes_.size() != expected_) {
      return absl::InvalidArgumentError("incomplete transfer");
    }
    committed_ = true;
    return absl::OkStatus();
  }

  void Abort() noexcept override {
    bytes_.clear();
    kind_.reset();
    expected_ = 0;
    committed_ = false;
  }

  bool committed() const noexcept { return committed_; }
  std::optional<control::TransferKind> kind() const noexcept { return kind_; }
  std::string TakeBytes() {
    committed_ = false;
    return std::move(bytes_);
  }

 private:
  std::string bytes_;
  std::optional<control::TransferKind> kind_;
  std::uint64_t expected_ = 0;
  bool committed_ = false;
};

struct ReceivedTransfer {
  control::TransferKind kind_;
  std::string bytes_;
};

RebuildDirective NativePopulationDirective(const NodeDirective& directive,
                                           const PopulationManifest& manifest) {
  // Replication's native population handshake predates the structured Meta
  // anchor. Give both source and target the same canonical string identity
  // over every committed authority field so neither side can accidentally
  // collapse two assignments or terms into one authorization.
  const std::string authority_id =
      EncodeRebuildAuthorityIdentity(directive.anchor_);
  const bool initializes_empty =
      directive.kind_ == NodeDirective::Kind::kInitializeEmptyPopulation;
  return RebuildDirective{
      .identity_ =
          {
              .group_id_ = directive.anchor_.group_id_,
              .assignment_id_ = directive.anchor_.assignment_id_.ToHexString(),
              .term_ = directive.anchor_.group_term_,
              .directive_revision_ = directive.directive_revision_,
              .authority_id_ = authority_id,
              .source_node_id_ = initializes_empty
                                     ? std::string{}
                                     : directive.source_node_id_.ToHexString(),
              .source_assignment_id_ =
                  initializes_empty
                      ? std::string{}
                      : directive.source_assignment_id_.ToHexString(),
              .source_boot_id_ = initializes_empty
                                     ? std::string{}
                                     : directive.source_boot_id_.ToHexString(),
              .source_history_id_ =
                  initializes_empty
                      ? std::string{}
                      : directive.source_replication_history_id_.ToHexString(),
              .target_node_id_ = directive.target_node_id_.ToHexString(),
              .target_boot_id_ = directive.target_boot_id_.ToHexString(),
              .target_history_id_ =
                  initializes_empty ? directive.payload_ : std::string{},
              .operation_id_ = directive.operation_id_.ToHexString(),
              .directive_id_ = directive.directive_id_.ToHexString(),
              .attempt_id_ = directive.attempt_id_.ToHexString(),
              .manifest_revision_ = directive.manifest_revision_,
              .manifest_id_ = manifest.id(),
              .partition_replication_epoch_ =
                  directive.partition_replication_epoch_,
          },
      .flow_count_ = directive.flow_count_,
      .safe_source_active_ = !initializes_empty,
  };
}

class ReplicationNodeControlActions final : public NodeControlActions {
 public:
  ReplicationNodeControlActions(ReplicationManager& replication, bool use_tls)
      : replication_(replication), use_tls_(use_tls) {}

  absl::Status RevokeSourceAuthorizations() override {
    return absl::FailedPreconditionError(
        "ReplicationManager source revocation requires an asynchronous "
        "NodeControl transition");
  }

  bycorf::Task<absl::Status> RevokeSourceAuthorizationsAndWait() override {
    co_return co_await replication_.RevokeClusterRebuildSourceAuthorizations();
  }

  bycorf::Task<absl::Status>
  ClearSourceAuthorizationsForSessionReplacementAndWait(
      bool preserve_established_exports) override {
    co_return co_await replication_
        .ClearClusterRebuildSourceAuthorizationsForSessionReplacement(
            preserve_established_exports);
  }

  bycorf::Task<absl::Status>
  RefreshSourceAuthorizationsForFdsReplacementAndWait(
      bool preserve_current_population_exports,
      std::size_t expected_authorization_replays) override {
    co_return co_await replication_
        .RefreshClusterRebuildSourceAuthorizationsForFdsReplacement(
            preserve_current_population_exports,
            expected_authorization_replays);
  }

  bycorf::Task<absl::Status> ReconcileClusterControl(
      std::optional<DesiredClusterControl> desired) override {
    if (!desired.has_value()) {
      absl::Status recovery_status =
          co_await replication_.ReconcileClusterRecovery(std::nullopt);
      absl::Status result =
          co_await replication_.ReconcileClusterFailoverAction(std::nullopt,
                                                               std::nullopt);
      absl::Status pause_status =
          co_await replication_.ReconcileClusterSourcePause(std::nullopt);
      if (result.ok()) result = std::move(pause_status);
      absl::Status follow_status =
          co_await replication_.ReconcileClusterFollowOwner(std::nullopt);
      if (result.ok()) result = std::move(follow_status);
      if (result.ok()) result = std::move(recovery_status);
      co_return result;
    }
    const ReplicationIdentity local_identity =
        co_await replication_.ObserveIdentity();
    auto translated = detail::TranslateClusterFailoverControl(
        *desired, local_identity, use_tls_);
    if (!translated.ok()) co_return translated.status();
    absl::Status result = absl::OkStatus();
    // Always attempt every applicable level-triggered intent so replacement
    // or removal cannot strand cleanup behind another subsystem's failure.
    // Returning the first failure keeps FullStateApplied fail closed.
    absl::Status pause_status =
        co_await replication_.ReconcileClusterSourcePause(
            std::move(translated->source_pause_));
    if (result.ok()) result = std::move(pause_status);
    if (translated->reconcile_follow_owner_) {
      absl::Status follow_status =
          co_await replication_.ReconcileClusterFollowOwner(
              std::move(translated->follow_owner_));
      if (result.ok()) result = std::move(follow_status);
    }
    absl::Status recovery_status =
        co_await replication_.ReconcileClusterRecovery(
            std::move(translated->recovery_));
    if (result.ok()) result = std::move(recovery_status);
    absl::Status action_status =
        co_await replication_.ReconcileClusterFailoverAction(
            std::move(translated->candidate_action_),
            translated->pending_activation_action_id_);
    if (result.ok()) result = std::move(action_status);
    co_return result;
  }

  bycorf::Task<absl::Status> ActivatePreparedPromotion(
      PreparedFailoverActivation activation) override {
    co_return co_await replication_.ActivateClusterPreparedPromotion(
        detail::TranslateClusterFailoverActivation(activation));
  }

  bycorf::Task<absl::Status> EnableExpirationAuthorityUntil(
      MonotonicTime deadline) override {
    co_return co_await replication_.EnableClusterExpirationAuthorityUntil(
        deadline.time_since_epoch());
  }

  bycorf::Task<absl::Status> EnableSourceAdmissionForLease(
      MonotonicTime deadline) override {
    co_return co_await replication_.EnableClusterRebuildSourceAdmissionUntil(
        deadline.time_since_epoch());
  }

  bycorf::Task<absl::Status> RevokeExpirationAuthority() override {
    co_return co_await replication_.RevokeClusterExpirationAuthority();
  }

  bycorf::Task<absl::Status> ReconcilePopulation(
      std::optional<PopulationReadiness> desired,
      bool population_transition_expected) override {
    std::optional<DesiredClusterPopulation> translated;
    if (desired.has_value()) {
      translated = DesiredClusterPopulation{
          .group_id_ = std::move(desired->group_id_),
          .assignment_id_ = desired->assignment_id_.ToHexString(),
          .term_ = desired->group_term_,
          .manifest_revision_ = desired->manifest_revision_,
          .manifest_id_ = PopulationManifestId{desired->manifest_digest_},
          .partition_replication_epoch_ = desired->partition_replication_epoch_,
          .population_transition_expected_ = population_transition_expected,
      };
    }
    co_return co_await replication_.ReconcileClusterPopulation(
        std::move(translated));
  }

  bycorf::Task<absl::Status> CancelInProgressPopulation(
      bool preserve_current_follow_attempt) override {
    co_return co_await replication_.CancelInProgressClusterPopulation(
        preserve_current_follow_attempt);
  }

  bycorf::Task<absl::Status> CancelPopulationForShutdown() override {
    co_return co_await replication_.CancelClusterRebuildForShutdown();
  }

  std::optional<NodeDirectiveCompletion> FindCompletedPopulation(
      const NodeDirective& directive) const override {
    std::vector<PopulationManifestEntry> entries;
    entries.reserve(directive.manifest_entries_.size());
    for (const auto& entry : directive.manifest_entries_)
      entries.push_back({entry.partition_id_, entry.logical_epoch_});
    auto manifest = PopulationManifest::Create(std::move(entries));
    if (!manifest.ok() || manifest->id().bytes_ != directive.manifest_digest_)
      return std::nullopt;
    auto completed = replication_.FindCompletedClusterPopulation(
        NativePopulationDirective(directive, *manifest));
    if (!completed.has_value()) return std::nullopt;
    return NodeDirectiveCompletion(
        [completion = std::move(*completed)] { return completion.result(); });
  }

  bycorf::Task<NodeDirectiveCompletion> StartDirective(
      NodeDirective directive) override {
    if (directive.kind_ == NodeDirective::Kind::kRevokeSources) {
      co_return NodeDirectiveCompletion::StartedTerminal(
          co_await replication_.RevokeClusterRebuildSourceAuthorizations());
    }

    std::vector<PopulationManifestEntry> entries;
    entries.reserve(directive.manifest_entries_.size());
    for (const NodeManifestEntry& entry : directive.manifest_entries_) {
      entries.push_back(PopulationManifestEntry{
          .partition_id_ = entry.partition_id_,
          .logical_epoch_ = entry.logical_epoch_,
      });
    }
    auto manifest = PopulationManifest::Create(std::move(entries));
    if (!manifest.ok()) {
      co_return NodeDirectiveCompletion::Rejected(manifest.status());
    }
    if (manifest->id().bytes_ != directive.manifest_digest_) {
      co_return NodeDirectiveCompletion::Rejected(absl::FailedPreconditionError(
          "normalized directive manifest digest does not match its "
          "entries"));
    }
    const bool initializes_empty =
        directive.kind_ == NodeDirective::Kind::kInitializeEmptyPopulation;
    RebuildDirective rebuild = NativePopulationDirective(directive, *manifest);
    if (directive.kind_ == NodeDirective::Kind::kAuthorizeSource) {
      co_return NodeDirectiveCompletion::StartedTerminal(
          co_await replication_.AuthorizeClusterRebuildSource(
              std::move(rebuild)));
    }
    if (initializes_empty) {
      auto started = co_await replication_.StartEmptyPopulationInitialization(
          std::move(rebuild.identity_), std::move(*manifest));
      if (!started.ok()) {
        co_return NodeDirectiveCompletion::StartedTerminal(started.status());
      }
      co_return NodeDirectiveCompletion(
          [completion = std::move(*started)]() { return completion.result(); });
    }
    auto started = co_await replication_.StartClusterRebuildDirective(
        ReplicaOfConfig{.host_ = std::move(directive.source_host_),
                        .port_ = directive.source_port_},
        std::move(rebuild), std::move(*manifest));
    if (!started.ok()) {
      // NodeControl already crossed its admission boundary and published the
      // target population not-ready before entering the native manager. A
      // terminal native error is therefore a started execution outcome, unlike
      // a controller precondition rejection that made no local transition.
      co_return NodeDirectiveCompletion::StartedTerminal(started.status());
    }
    co_return NodeDirectiveCompletion(
        [completion = std::move(*started)]() { return completion.result(); });
  }

  bycorf::Task<absl::Status> ApplyDirective(NodeDirective directive) override {
    NodeDirectiveCompletion completion =
        co_await StartDirective(std::move(directive));
    co_return co_await completion.Await();
  }

  absl::Status DrainAssignment(const AuthorityAnchor&) override {
    // NodeControlInstaller owns the replaced ServingState counters. Source
    // sessions were already joined by RevokeSourceAuthorizationsAndWait().
    return absl::OkStatus();
  }

 private:
  ReplicationManager& replication_;
  const bool use_tls_;
};

}  // namespace

absl::StatusOr<detail::ClusterFailoverReconcileInput>
detail::TranslateClusterFailoverControl(
    const DesiredClusterControl& desired,
    const ReplicationIdentity& local_identity, bool use_tls) {
  detail::ClusterFailoverReconcileInput translated;
  // The activation id belongs to the committed owner's grant. It is a local
  // handoff only on that owner; every other member merely observes the grant's
  // provenance. In particular, a later candidate must not reconcile the old
  // owner's activation id alongside its own new action.
  const bool local_is_committed_owner =
      desired.owner_.has_value() &&
      desired.owner_->node_id_.ToHexString() == local_identity.local_node_id_;
  if (local_is_committed_owner && desired.activation_action_id_.has_value()) {
    translated.pending_activation_action_id_ =
        desired.activation_action_id_->bytes();
  }

  if (!desired.failover_transition_.has_value()) {
    if (!desired.steady_replication_enabled_ ||
        desired.population_transition_expected_) {
      return translated;
    }
    translated.reconcile_follow_owner_ = true;
    if (!desired.owner_.has_value()) return translated;
    const auto local = std::find_if(
        desired.identity_.members_.begin(), desired.identity_.members_.end(),
        [&](const PreparedMemberAssignment& member) {
          return member.node_id_.ToHexString() == local_identity.local_node_id_;
        });
    if (local == desired.identity_.members_.end()) {
      return absl::FailedPreconditionError(
          "local Data identity is absent from its desired Group membership");
    }
    if (!desired.owner_endpoint_.has_value() ||
        desired.owner_endpoint_->node_id_ != desired.owner_->node_id_ ||
        desired.owner_endpoint_->host_.empty()) {
      return absl::FailedPreconditionError(
          "desired Group Owner has no matching committed endpoint");
    }
    const bool local_is_owner = local->node_id_ == desired.owner_->node_id_;
    std::optional<ReplicaOfConfig> owner_endpoint;
    if (!local_is_owner) {
      const std::uint16_t port = use_tls ? desired.owner_endpoint_->tls_port_
                                         : desired.owner_endpoint_->port_;
      if (port == 0) {
        return absl::FailedPreconditionError(
            use_tls ? "desired Group Owner offers no TLS replication port"
                    : "desired Group Owner offers no plain replication port");
      }
      owner_endpoint = ReplicaOfConfig{.host_ = desired.owner_endpoint_->host_,
                                       .port_ = port};
    }
    DesiredClusterUpstream follow{
        .group_id_ = desired.identity_.group_id_,
        .group_term_ = desired.identity_.group_term_,
        .local_node_id_ = local->node_id_.ToHexString(),
        .local_assignment_id_ = local->assignment_id_.ToHexString(),
        .local_boot_id_ = local_identity.boot_id_,
        .owner_node_id_ = desired.owner_->node_id_.ToHexString(),
        .owner_assignment_id_ = desired.owner_->assignment_id_.ToHexString(),
        .owner_endpoint_ = std::move(owner_endpoint),
        .manifest_revision_ = desired.identity_.manifest_revision_,
        .manifest_id_ =
            PopulationManifestId{desired.identity_.manifest_digest_},
        .manifest_entries_ = desired.manifest_entries_,
        .partition_replication_epoch_ =
            desired.identity_.partition_replication_epoch_,
        .members_ = {},
    };
    follow.members_.reserve(desired.identity_.members_.size());
    for (const PreparedMemberAssignment& member : desired.identity_.members_) {
      follow.members_.push_back(ClusterReplicationMember{
          .node_id_ = member.node_id_.ToHexString(),
          .assignment_id_ = member.assignment_id_.ToHexString(),
      });
    }
    translated.follow_owner_ = std::move(follow);
    return translated;
  }

  // Candidate preparation owns its existing native ingress. Reconfiguring
  // the ordinary relationship before Cutover could cancel that exact Ready
  // population or make promotion activation observe a live upstream.
  if (!desired.failover_transition_->candidate_action_.has_value()) {
    return translated;
  }
  const PreparedFailoverTransition& transition = *desired.failover_transition_;
  const PreparedFailoverAction& action = *transition.candidate_action_;
  DesiredClusterFailoverAction candidate_action{
      .transition_id_ = transition.transition_id_.bytes(),
      .action_id_ = action.action_id_.bytes(),
      .transition_revision_ = transition.revision_,
      .mode_ = transition.mode_ == PreparedFailoverMode::kControlled
                   ? ClusterFailoverMode::kControlled
                   : ClusterFailoverMode::kUncontrolled,
      .target_term_ = transition.target_term_,
      .committed_group_term_ = desired.identity_.group_term_,
      .committed_grant_active_ = desired.grant_active_,
      .authorized_revision_ =
          action.authorization_.has_value()
              ? std::optional<std::uint64_t>(
                    action.authorization_->authorized_revision_)
              : std::nullopt,
      .group_id_ = desired.identity_.group_id_,
      .candidate_node_id_ = action.candidate_.node_id_.ToHexString(),
      .candidate_assignment_id_ =
          action.candidate_.assignment_id_.ToHexString(),
      .candidate_boot_id_ = action.candidate_.boot_id_.ToHexString(),
      .domain_ =
          {
              .source_group_term_ = action.domain_.source_group_term_,
              .source_node_id_ = action.domain_.source_node_id_.ToHexString(),
              .source_assignment_id_ =
                  action.domain_.source_assignment_id_.ToHexString(),
              .source_boot_id_ = action.domain_.source_boot_id_.ToHexString(),
              .source_history_id_ =
                  action.domain_.source_history_id_.ToHexString(),
              .flow_count_ = action.domain_.flow_count_,
          },
      .manifest_revision_ = desired.identity_.manifest_revision_,
      .manifest_id_ = PopulationManifestId{desired.identity_.manifest_digest_},
      .partition_replication_epoch_ =
          desired.identity_.partition_replication_epoch_,
      .operator_recovery_ = action.operator_recovery_,
      .manifest_entries_ = desired.manifest_entries_,
      .recovery_deadline_unix_ms_ = transition.recovery_deadline_unix_ms_,
  };
  if (action.operator_recovery_) candidate_action.domain_ = {};
  if (candidate_action.candidate_node_id_ == local_identity.local_node_id_ &&
      candidate_action.candidate_boot_id_ == local_identity.boot_id_) {
    translated.candidate_action_ = candidate_action;
  }
  if (transition.mode_ == PreparedFailoverMode::kUncontrolled &&
      transition.recovery_deadline_unix_ms_.has_value() &&
      !action.authorization_.has_value() && !action.operator_recovery_) {
    DesiredClusterRecovery recovery;
    recovery.action_ = candidate_action;
    recovery.local_node_id_ = local_identity.local_node_id_;
    recovery.local_boot_id_ = local_identity.boot_id_;
    for (const auto& member : desired.identity_.members_) {
      ClusterRecoveryPeer peer;
      peer.member_ = {member.node_id_.ToHexString(),
                      member.assignment_id_.ToHexString()};
      if (peer.member_.node_id_ == local_identity.local_node_id_)
        recovery.local_assignment_id_ = peer.member_.assignment_id_;
      const auto endpoint =
          std::ranges::find(desired.member_endpoints_, member.node_id_,
                            &PreparedReplicationEndpoint::node_id_);
      if (endpoint != desired.member_endpoints_.end()) {
        peer.endpoint_ = {endpoint->host_,
                          use_tls ? endpoint->tls_port_ : endpoint->port_};
      }
      recovery.members_.push_back(std::move(peer));
    }
    if (recovery.local_assignment_id_.empty())
      return absl::FailedPreconditionError(
          "recovery recipient is not a current Group member");
    translated.recovery_ = std::move(recovery);
  }

  const std::optional<PreparedMemberAssignment>& owner = desired.owner_;
  const PreparedFailoverCompatibilityDomain& domain = action.domain_;
  if (transition.mode_ == PreparedFailoverMode::kControlled &&
      owner.has_value() && owner->node_id_ == domain.source_node_id_ &&
      owner->assignment_id_ == domain.source_assignment_id_ &&
      domain.source_group_term_ == desired.identity_.group_term_ &&
      domain.source_node_id_.ToHexString() == local_identity.local_node_id_ &&
      domain.source_boot_id_.ToHexString() == local_identity.boot_id_ &&
      domain.source_history_id_.ToHexString() ==
          local_identity.local_history_id_) {
    translated.source_pause_ = DesiredClusterSourcePause{
        .transition_id_ = transition.transition_id_.bytes(),
        .transition_revision_ = transition.revision_,
        .group_id_ = desired.identity_.group_id_,
        .source_node_id_ = domain.source_node_id_.ToHexString(),
        .source_assignment_id_ = domain.source_assignment_id_.ToHexString(),
        .source_boot_id_ = domain.source_boot_id_.ToHexString(),
        .source_history_id_ = domain.source_history_id_.ToHexString(),
        .source_group_term_ = domain.source_group_term_,
        .flow_count_ = domain.flow_count_,
        .manifest_revision_ = desired.identity_.manifest_revision_,
        .manifest_id_ =
            PopulationManifestId{desired.identity_.manifest_digest_},
        .partition_replication_epoch_ =
            desired.identity_.partition_replication_epoch_,
    };
  }
  return translated;
}

ClusterFailoverActivation detail::TranslateClusterFailoverActivation(
    const PreparedFailoverActivation& activation) {
  return ClusterFailoverActivation{
      .action_id_ = activation.action_id_.bytes(),
      .group_id_ = activation.group_id_,
      .candidate_node_id_ = activation.candidate_node_id_.ToHexString(),
      .candidate_assignment_id_ =
          activation.candidate_assignment_id_.ToHexString(),
      .candidate_boot_id_ = activation.candidate_boot_id_.ToHexString(),
      .target_term_ = activation.target_term_,
      .manifest_revision_ = activation.manifest_revision_,
      .manifest_id_ = PopulationManifestId{activation.manifest_digest_},
      .partition_replication_epoch_ = activation.partition_replication_epoch_,
  };
}

absl::StatusOr<std::optional<control::FailoverObservation>>
detail::ProjectClusterFailoverObservation(
    const ClusterFailoverActionStatus& status) {
  if (status.state_ != ClusterFailoverActionState::kPrepared &&
      status.state_ != ClusterFailoverActionState::kFailed &&
      status.state_ != ClusterFailoverActionState::kRecoveryComplete) {
    return std::optional<control::FailoverObservation>{};
  }
  if (!status.action_.has_value()) {
    return absl::FailedPreconditionError(
        "terminal failover action status is missing its committed action");
  }
  const DesiredClusterFailoverAction& action = *status.action_;
  const auto candidate_node = NodeId::Parse(action.candidate_node_id_);
  const auto candidate_assignment =
      AssignmentId::Parse(action.candidate_assignment_id_);
  const auto candidate_boot = NodeId::Parse(action.candidate_boot_id_);
  if (!candidate_node.has_value() || !candidate_assignment.has_value() ||
      !candidate_boot.has_value()) {
    return absl::FailedPreconditionError(
        "terminal failover action has a non-canonical candidate identity");
  }

  if (status.state_ == ClusterFailoverActionState::kRecoveryComplete) {
    if (action.mode_ != ClusterFailoverMode::kUncontrolled ||
        !action.recovery_deadline_unix_ms_.has_value() ||
        !status.recovery_.has_value() ||
        status.recovery_->applied_next_lsns_.size() !=
            action.domain_.flow_count_ ||
        std::ranges::any_of(status.recovery_->applied_next_lsns_,
                            [](std::uint64_t lsn) { return lsn == 0; }) ||
        !control::IsRecoveryCompletionReason(
            status.recovery_->completion_reason_)) {
      return absl::FailedPreconditionError(
          "recovery completion lacks a complete drained cut");
    }
    return std::optional<control::FailoverObservation>(
        control::CandidateRecoveryComplete{
            .transition_id = action.transition_id_,
            .action_id = action.action_id_,
            .candidate_node_id = candidate_node->ToHexString(),
            .candidate_assignment_id = candidate_assignment->bytes(),
            .candidate_boot_id = candidate_boot->ToHexString(),
            .recovery_deadline_unix_ms = *action.recovery_deadline_unix_ms_,
            .applied_next_lsns = status.recovery_->applied_next_lsns_,
            .completion_reason = status.recovery_->completion_reason_,
        });
  }
  if (status.state_ == ClusterFailoverActionState::kFailed) {
    return std::optional<control::FailoverObservation>(control::ActionFailed{
        .transition_id = action.transition_id_,
        .action_id = action.action_id_,
        .candidate_node_id = candidate_node->ToHexString(),
        .candidate_assignment_id = candidate_assignment->bytes(),
        .candidate_boot_id = candidate_boot->ToHexString(),
        .population_manifest_revision = action.manifest_revision_,
        .population_manifest_digest = action.manifest_id_.bytes_,
        .partition_replication_epoch = action.partition_replication_epoch_,
        .failure_class = status.failure_class_,
        .failure_detail = status.failure_detail_,
    });
  }

  if (!status.prepared_.has_value()) {
    return absl::FailedPreconditionError(
        "prepared failover action status is missing its exact context");
  }
  const ClusterFailoverPreparedContext& prepared = *status.prepared_;
  if (prepared.transition_id_ != action.transition_id_ ||
      prepared.action_id_ != action.action_id_ ||
      (!action.operator_recovery_ && prepared.promotion_.parent_history_id_ !=
                                         action.domain_.source_history_id_)) {
    return absl::FailedPreconditionError(
        "prepared failover context does not match its committed action");
  }
  return std::optional<control::FailoverObservation>(control::CandidatePrepared{
      .transition_id = prepared.transition_id_,
      .action_id = prepared.action_id_,
      .candidate_node_id = candidate_node->ToHexString(),
      .candidate_assignment_id = candidate_assignment->bytes(),
      .candidate_boot_id = candidate_boot->ToHexString(),
      .prepared_context_id = prepared.context_id_,
  });
}

absl::StatusOr<std::optional<control::FailoverObservation>>
detail::ProjectClusterSourcePauseObservation(
    const ClusterSourcePauseStatus& status) {
  if (!status.desired_.has_value() || !status.stable_next_lsns_.has_value() ||
      !status.failure_detail_.empty()) {
    return std::optional<control::FailoverObservation>{};
  }
  const DesiredClusterSourcePause& desired = *status.desired_;
  if (desired.flow_count_ == 0 ||
      status.stable_next_lsns_->size() != desired.flow_count_) {
    return std::optional<control::FailoverObservation>{};
  }
  const auto source_node = NodeId::Parse(desired.source_node_id_);
  const auto source_assignment =
      AssignmentId::Parse(desired.source_assignment_id_);
  const auto source_boot = NodeId::Parse(desired.source_boot_id_);
  const auto source_history = NodeId::Parse(desired.source_history_id_);
  if (!source_node.has_value() || !source_assignment.has_value() ||
      !source_boot.has_value() || !source_history.has_value()) {
    return absl::FailedPreconditionError(
        "stable source pause has a non-canonical source identity");
  }
  return std::optional<control::FailoverObservation>(control::SourcePaused{
      .transition_id = desired.transition_id_,
      .source_node_id = source_node->ToHexString(),
      .source_assignment_id = source_assignment->bytes(),
      .source_boot_id = source_boot->ToHexString(),
      .source_history_id = source_history->ToHexString(),
      .source_group_term = desired.source_group_term_,
      .stable_next_lsns = *status.stable_next_lsns_,
  });
}

bool detail::ReplicationIdentityRequiresMetaReconnect(
    const ReplicationIdentity& established,
    const ReplicationIdentity& latest) noexcept {
  return latest.local_node_id_ != established.local_node_id_ ||
         latest.boot_id_ != established.boot_id_ ||
         latest.local_history_id_ != established.local_history_id_;
}

bool detail::FailoverActionAllowsHistoryTransitionOnCurrentMetaSession(
    const ReplicationIdentity& established, const ReplicationIdentity& latest,
    const ClusterFailoverActionStatus& status,
    std::span<const control::WireDesiredGroup> groups) noexcept {
  if (established.local_node_id_ != latest.local_node_id_ ||
      established.boot_id_ != latest.boot_id_ ||
      established.local_history_id_ == latest.local_history_id_ ||
      !status.action_.has_value()) {
    return false;
  }
  const DesiredClusterFailoverAction& action = *status.action_;
  const auto group = std::ranges::find_if(
      groups, [&](const control::WireDesiredGroup& candidate) {
        return candidate.group_id == action.group_id_;
      });
  if (group == groups.end() || !group->failover_transition.has_value()) {
    return false;
  }
  const control::WireFailoverTransition& transition =
      *group->failover_transition;
  if (!transition.candidate_action.has_value()) return false;
  const control::WireFailoverCandidateAction& wire_action =
      *transition.candidate_action;
  ClusterFailoverMode wire_mode;
  if (transition.mode == control::WireFailoverMode::kControlled) {
    wire_mode = ClusterFailoverMode::kControlled;
  } else if (transition.mode == control::WireFailoverMode::kUncontrolled) {
    wire_mode = ClusterFailoverMode::kUncontrolled;
  } else {
    return false;
  }
  const std::optional<std::uint64_t> authorized_revision =
      wire_action.authorization.has_value()
          ? std::optional(wire_action.authorization->authorized_revision)
          : std::nullopt;
  const ClusterFailoverCompatibilityDomain& domain = action.domain_;
  const control::WireFailoverCompatibilityDomain& wire_domain =
      wire_action.domain;
  const bool exact_action =
      transition.transition_id == action.transition_id_ &&
      transition.revision == action.transition_revision_ &&
      wire_mode == action.mode_ &&
      transition.target_term == action.target_term_ &&
      group->group_term == action.committed_group_term_ &&
      group->grant_active == action.committed_grant_active_ &&
      wire_action.action_id == action.action_id_ &&
      authorized_revision == action.authorized_revision_ &&
      action.authorized_revision_.has_value() &&
      wire_action.candidate.node_id == action.candidate_node_id_ &&
      AssignmentId::FromBytes(wire_action.candidate.assignment_id)
              .ToHexString() == action.candidate_assignment_id_ &&
      wire_action.candidate.boot_id == action.candidate_boot_id_ &&
      action.candidate_node_id_ == latest.local_node_id_ &&
      action.candidate_boot_id_ == latest.boot_id_ &&
      wire_action.operator_recovery == action.operator_recovery_ &&
      (action.operator_recovery_ ||
       (wire_domain.source_group_term == domain.source_group_term_ &&
        wire_domain.source_node_id == domain.source_node_id_ &&
        AssignmentId::FromBytes(wire_domain.source_assignment_id)
                .ToHexString() == domain.source_assignment_id_ &&
        wire_domain.source_boot_id == domain.source_boot_id_ &&
        wire_domain.source_history_id == domain.source_history_id_ &&
        wire_domain.flow_count == domain.flow_count_)) &&
      group->manifest_revision == action.manifest_revision_ &&
      group->manifest_digest == action.manifest_id_.bytes_ &&
      group->partition_replication_epoch == action.partition_replication_epoch_;
  if (!exact_action) return false;

  if (status.state_ == ClusterFailoverActionState::kPreparing) {
    return !status.prepared_.has_value();
  }
  if (status.state_ != ClusterFailoverActionState::kPrepared ||
      !status.prepared_.has_value()) {
    return false;
  }
  const ClusterFailoverPreparedContext& prepared = *status.prepared_;
  return prepared.transition_id_ == action.transition_id_ &&
         prepared.action_id_ == action.action_id_ &&
         (action.operator_recovery_ || prepared.promotion_.parent_history_id_ ==
                                           action.domain_.source_history_id_) &&
         prepared.promotion_.child_history_id_ == latest.local_history_id_;
}

detail::MetaSessionReplicationIdentityDecision
detail::EvaluateMetaSessionReplicationIdentity(
    const ReplicationIdentity& established, const ReplicationIdentity& latest,
    const ClusterFailoverActionStatus& status,
    std::span<const control::WireDesiredGroup> groups) noexcept {
  if (!ReplicationIdentityRequiresMetaReconnect(established, latest)) {
    return {};
  }
  if (FailoverActionAllowsHistoryTransitionOnCurrentMetaSession(
          established, latest, status, groups)) {
    return {.requires_reconnect_ = false, .suppress_ordinary_role_ = true};
  }
  return {.requires_reconnect_ = true, .suppress_ordinary_role_ = false};
}

absl::StatusOr<MetaControlEndpoint> ParseNumericControlEndpoint(
    std::string_view endpoint) {
  auto parsed = lavik::ParseNumericEndpoint(endpoint);
  if (!parsed.has_value()) {
    return absl::InvalidArgumentError(
        "control endpoint must be IPv4:port or [IPv6]:port");
  }
  return MetaControlEndpoint{.host_ = std::move(parsed->host_),
                             .port_ = parsed->port_,
                             .principal_ = std::nullopt};
}

absl::Status ValidateUniqueControlPrincipal(
    std::span<const std::string> uri_sans, std::string_view expected) {
  if (uri_sans.size() != 1) {
    return absl::UnauthenticatedError(
        "control peer must present exactly one URI SAN");
  }
  if (uri_sans.front() != expected) {
    return absl::PermissionDeniedError(
        "control peer URI SAN does not match its protocol identity");
  }
  return absl::OkStatus();
}

absl::Status ValidateDialedMetaIdentity(
    const MetaControlEndpoint& dialed,
    const control::WireMetaEndpoint& hello_member) {
  if (dialed.server_id_ == 0) {
    if (dialed.principal_.has_value()) {
      return absl::InvalidArgumentError(
          "unresolved Meta seed unexpectedly has a pinned principal");
    }
    return absl::OkStatus();
  }
  if (dialed.server_id_ != hello_member.server_id) {
    return absl::FailedPreconditionError(
        "ServerHello member does not match the learned dial target");
  }
  if (!dialed.principal_.has_value() || !hello_member.principal.has_value()) {
    return absl::FailedPreconditionError(
        "learned Meta identity binding is incomplete");
  }
  if (*dialed.principal_ != *hello_member.principal) {
    return absl::PermissionDeniedError(
        "ServerHello principal differs from the learned identity binding");
  }
  return absl::OkStatus();
}

absl::Status ValidateLiveDirective(const control::Directive& directive,
                                   const control::NodeControlState& desired,
                                   std::string_view local_node_id,
                                   std::string_view local_boot_id) {
  if (directive.basis.control_revision != desired.local.revision) {
    return absl::FailedPreconditionError(
        "directive does not name the installed local control");
  }
  if (directive.recipient_node_id != local_node_id ||
      directive.recipient_boot_id != local_boot_id) {
    return absl::FailedPreconditionError(
        "directive does not target the local node incarnation");
  }
  switch (directive.kind) {
    case control::WireDirectiveKind::kRebuild:
    case control::WireDirectiveKind::kInitializeEmptyPopulation:
      if (directive.recipient_node_id != directive.target_node_id ||
          directive.recipient_boot_id != directive.target_boot_id) {
        return absl::FailedPreconditionError(
            "population directive recipient is not its target incarnation");
      }
      break;
    case control::WireDirectiveKind::kAuthorizeSource:
    case control::WireDirectiveKind::kRevokeSources:
      if (directive.recipient_node_id != directive.source_node_id ||
          directive.recipient_boot_id != directive.source_boot_id) {
        return absl::FailedPreconditionError(
            "source directive recipient is not its source incarnation");
      }
      break;
  }
  if (directive.kind ==
          control::WireDirectiveKind::kInitializeEmptyPopulation &&
      (directive.source_node_id != std::string(40, '0') ||
       !IsZero(directive.source_assignment_id) ||
       directive.source_boot_id != std::string(40, '0') ||
       directive.source_replication_history_id != std::string(40, '0'))) {
    return absl::InvalidArgumentError(
        "empty population directive must not name a source");
  }

  const auto group =
      std::find_if(desired.local.groups.begin(), desired.local.groups.end(),
                   [&](const control::WireDesiredGroup& candidate) {
                     return candidate.group_id == directive.authority.group_id;
                   });
  if (group == desired.local.groups.end() ||
      group->group_term != directive.authority.group_term ||
      group->partition_replication_epoch !=
          directive.partition_replication_epoch) {
    return absl::FailedPreconditionError(
        "directive authority is not current in the installed local control");
  }
  const auto target = std::find_if(
      group->members.begin(), group->members.end(),
      [&](const control::WireDesiredMember& member) {
        return member.node_id == directive.target_node_id &&
               member.assignment_id == directive.authority.assignment_id;
      });
  if (target == group->members.end()) {
    return absl::FailedPreconditionError(
        "directive target assignment is not current in the installed local "
        "control");
  }
  if (directive.kind !=
          control::WireDirectiveKind::kInitializeEmptyPopulation &&
      std::none_of(group->members.begin(), group->members.end(),
                   [&](const control::WireDesiredMember& member) {
                     return member.node_id == directive.source_node_id &&
                            member.assignment_id ==
                                directive.source_assignment_id;
                   })) {
    return absl::FailedPreconditionError(
        "directive source assignment is not current in the installed local "
        "control");
  }

  return absl::OkStatus();
}

control::DirectiveResultStatus ClassifyDirectiveResultStatus(
    const absl::Status& status, bool started) noexcept {
  if (status.ok()) return control::DirectiveResultStatus::kSucceeded;
  // The admission disposition, rather than a lossy StatusCode convention,
  // distinguishes a rejected request from work that ran and failed.
  return started ? control::DirectiveResultStatus::kFailed
                 : control::DirectiveResultStatus::kRejected;
}

absl::Status detail::ValidateResolvedLeaseGrantDuration(
    std::uint32_t granted_duration_ms, std::uint32_t challenged_duration_ms) {
  if (challenged_duration_ms == 0 ||
      granted_duration_ms != challenged_duration_ms) {
    return absl::InvalidArgumentError(
        absl::StrCat("lease grant duration does not equal the challenged local "
                     "control duration: ",
                     "granted_ms=", granted_duration_ms,
                     ",challenged_ms=", challenged_duration_ms));
  }
  return absl::OkStatus();
}

absl::Status FitHeartbeatToSingleFrame(control::Heartbeat& heartbeat) {
  const auto fits = [&]() -> absl::StatusOr<bool> {
    auto encoded = control::EncodeMessage(control::WireMessage(heartbeat));
    if (!encoded.ok()) {
      if (encoded.status().code() == absl::StatusCode::kResourceExhausted) {
        return false;
      }
      return encoded.status();
    }
    return encoded->size() <= control::kMaxFramePayloadBytes;
  };
  auto initial = fits();
  if (!initial.ok()) return initial.status();
  if (*initial) return absl::OkStatus();

  const auto shorten_summary = [&]() -> absl::StatusOr<bool> {
    constexpr std::string_view kSuffix = "...[truncated]";
    const std::string original = heartbeat.health.summary;
    heartbeat.health.summary.clear();
    auto base = fits();
    if (!base.ok()) return base.status();
    if (!*base) {
      heartbeat.health.summary = original;
      return false;
    }

    std::string best;
    std::size_t low = 0;
    std::size_t high = std::min(original.size(), control::kMaxOpaqueFieldBytes);
    while (low <= high) {
      const std::size_t prefix = low + (high - low) / 2;
      heartbeat.health.summary.assign(original.data(), prefix);
      if (prefix < original.size()) heartbeat.health.summary.append(kSuffix);
      auto candidate = fits();
      if (!candidate.ok()) return candidate.status();
      if (*candidate) {
        best = heartbeat.health.summary;
        low = prefix + 1;
      } else {
        if (prefix == 0) break;
        high = prefix - 1;
      }
    }
    heartbeat.health.summary = std::move(best);
    return true;
  };

  auto summary_fit = shorten_summary();
  if (!summary_fit.ok()) return summary_fit.status();
  if (*summary_fit) return absl::OkStatus();

  if (!std::holds_alternative<control::ReplicaCandidate>(
          heartbeat.role_information)) {
    return absl::ResourceExhaustedError(
        "heartbeat authority fields exceed the single-frame protocol limit");
  }
  heartbeat.role_information = control::NoRoleInformation{};
  heartbeat.health.summary = absl::StrCat(
      "candidate progress omitted: single-frame limit",
      heartbeat.health.summary.empty() ? "" : "; ", heartbeat.health.summary);
  summary_fit = shorten_summary();
  if (!summary_fit.ok()) return summary_fit.status();
  if (*summary_fit) return absl::OkStatus();
  return absl::ResourceExhaustedError(
      "heartbeat fixed fields exceed the single-frame protocol limit");
}

std::string EncodeRebuildAuthorityIdentity(const AuthorityAnchor& anchor) {
  constexpr char kHex[] = "0123456789abcdef";
  std::string group_hex;
  group_hex.reserve(anchor.group_id_.size() * 2);
  for (const unsigned char byte : anchor.group_id_) {
    group_hex.push_back(kHex[byte >> 4]);
    group_hex.push_back(kHex[byte & 0x0f]);
  }
  return absl::StrCat("v1/", group_hex, "/",
                      anchor.assignment_id_.ToHexString(), "/",
                      anchor.group_term_);
}

std::chrono::milliseconds MetaReconnectBackoff::Next(
    std::uint64_t entropy) noexcept {
  const auto current = window_;
  const std::uint64_t choices = static_cast<std::uint64_t>(current.count()) + 1;
  const auto delay =
      std::chrono::milliseconds(static_cast<std::int64_t>(entropy % choices));
  window_ = std::min(window_ * 2, MaximumWindow());
  return delay;
}

MetaEndpointDirectory::MetaEndpointDirectory(
    std::vector<MetaControlEndpoint> seeds)
    : seeds_(std::move(seeds)) {}

absl::Status MetaEndpointDirectory::Update(
    std::span<const control::WireMetaEndpoint> committed_directory,
    std::optional<std::uint32_t> leader_id) {
  std::vector<MetaControlEndpoint> parsed;
  parsed.reserve(committed_directory.size());
  for (const control::WireMetaEndpoint& endpoint : committed_directory) {
    if (endpoint.server_id == 0) {
      return absl::InvalidArgumentError("Meta directory has server id zero");
    }
    const std::string expected_principal =
        absl::StrCat("lavik://meta/", endpoint.server_id);
    if (!endpoint.principal.has_value() ||
        *endpoint.principal != expected_principal) {
      return absl::InvalidArgumentError(
          "Meta directory has a missing or non-canonical principal binding");
    }
    auto value =
        ParseNumericControlEndpoint(EndpointText(endpoint.host, endpoint.port));
    if (!value.ok()) return value.status();
    value->server_id_ = endpoint.server_id;
    value->principal_ = endpoint.principal;
    const auto duplicate = std::find_if(
        parsed.begin(), parsed.end(), [&](const MetaControlEndpoint& existing) {
          return existing.server_id_ == value->server_id_ ||
                 SameEndpoint(existing, *value);
        });
    if (duplicate != parsed.end()) {
      return absl::InvalidArgumentError("Meta directory contains duplicates");
    }
    parsed.push_back(std::move(*value));
  }
  if (leader_id.has_value() &&
      std::none_of(parsed.begin(), parsed.end(), [&](const auto& endpoint) {
        return endpoint.server_id_ == *leader_id;
      })) {
    leader_id.reset();
  }
  learned_ = std::move(parsed);
  leader_id_ = leader_id;
  return absl::OkStatus();
}

absl::Status MetaEndpointDirectory::Refresh(
    std::span<const control::WireMetaEndpoint> committed_directory) {
  return Update(committed_directory, leader_id_);
}

std::vector<MetaControlEndpoint> MetaEndpointDirectory::Candidates() const {
  std::vector<MetaControlEndpoint> result;
  result.reserve(learned_.size() + seeds_.size());
  const auto append = [&](const MetaControlEndpoint& endpoint) {
    if (std::none_of(result.begin(), result.end(), [&](const auto& existing) {
          return SameEndpoint(existing, endpoint) &&
                 existing.server_id_ == endpoint.server_id_;
        })) {
      result.push_back(endpoint);
    }
  };
  if (leader_id_.has_value()) {
    const auto leader = std::find_if(
        learned_.begin(), learned_.end(), [&](const auto& endpoint) {
          return endpoint.server_id_ == *leader_id_;
        });
    if (leader != learned_.end()) append(*leader);
  }
  for (const MetaControlEndpoint& endpoint : learned_) append(endpoint);
  for (const MetaControlEndpoint& endpoint : seeds_) append(endpoint);
  return result;
}

bool MetaLeaseChallengeRotation::IsCommittedOwner(
    std::span<const control::WireDesiredGroup> groups,
    std::string_view local_node_id) noexcept {
  return std::ranges::any_of(groups, [&](const auto& group) {
    if (!group.owner_node_id.has_value() ||
        *group.owner_node_id != local_node_id) {
      return false;
    }
    const bool fenced_historical_owner =
        !group.grant_active && group.owner_assignment_id.has_value() &&
        std::ranges::any_of(group.members,
                            [&](const auto& member) {
                              return member.node_id == local_node_id &&
                                     member.assignment_id ==
                                         *group.owner_assignment_id;
                            }) &&
        group.failover_transition.has_value() &&
        group.failover_transition->mode ==
            control::WireFailoverMode::kUncontrolled &&
        group.failover_transition->target_term == group.group_term;
    return !fenced_historical_owner;
  });
}

absl::StatusOr<std::optional<control::CandidateProgress>>
detail::ProjectReplicaCandidateProgress(
    std::span<const control::WireDesiredGroup> groups,
    std::string_view local_node_id, const ReplicationIdentity& current_identity,
    const PopulationReadiness& readiness, const RebuildIdentity& identity,
    std::span<const std::uint64_t> applied_next_lsns,
    bool failover_candidate_eligible, bool recovered) {
  if (!failover_candidate_eligible) {
    return std::optional<control::CandidateProgress>{};
  }
  const auto group = std::ranges::find_if(
      groups, [&](const control::WireDesiredGroup& candidate) {
        return candidate.group_id == readiness.group_id_;
      });
  if (group == groups.end() || group->group_term != readiness.group_term_ ||
      group->manifest_revision != readiness.manifest_revision_ ||
      group->manifest_digest != readiness.manifest_digest_ ||
      group->partition_replication_epoch !=
          readiness.partition_replication_epoch_) {
    return absl::FailedPreconditionError(
        "candidate readiness does not match the installed local control");
  }
  const auto local_member = std::ranges::find_if(
      group->members, [&](const control::WireDesiredMember& member) {
        return member.node_id == local_node_id &&
               member.assignment_id == readiness.assignment_id_.bytes();
      });
  if (local_member == group->members.end()) {
    return absl::FailedPreconditionError(
        "candidate readiness does not match the local member assignment");
  }

  const bool topology_owner =
      group->owner_node_id.has_value() &&
      *group->owner_node_id == local_node_id &&
      group->owner_assignment_id.has_value() &&
      *group->owner_assignment_id == local_member->assignment_id;
  const bool fenced_historical_owner =
      topology_owner && !group->grant_active &&
      group->failover_transition.has_value() &&
      group->failover_transition->mode ==
          control::WireFailoverMode::kUncontrolled &&
      group->failover_transition->target_term == group->group_term;
  if (topology_owner && !fenced_historical_owner) {
    return std::optional<control::CandidateProgress>{};
  }

  if (current_identity.local_node_id_ != local_node_id ||
      identity.group_id_ != readiness.group_id_ ||
      identity.assignment_id_ != readiness.assignment_id_.ToHexString() ||
      identity.target_node_id_ != local_node_id ||
      identity.target_boot_id_ != current_identity.boot_id_ ||
      readiness.group_term_ < identity.term_ ||
      identity.manifest_revision_ != readiness.manifest_revision_ ||
      identity.manifest_id_.bytes_ != readiness.manifest_digest_ ||
      identity.partition_replication_epoch_ !=
          readiness.partition_replication_epoch_ ||
      applied_next_lsns.empty() ||
      std::ranges::any_of(
          applied_next_lsns, [](std::uint64_t lsn) { return lsn == 0; })) {
    return absl::FailedPreconditionError(
        "candidate population identity is incomplete or stale");
  }

  std::uint64_t source_group_term = identity.term_;
  std::string source_node_id = identity.source_node_id_;
  control::WireId128 source_assignment_id{};
  std::string source_boot_id = identity.source_boot_id_;
  std::string source_history_id = identity.source_history_id_;
  if (fenced_historical_owner && !recovered) {
    if (readiness.group_term_ <= 1 ||
        !control::IsCanonicalIdentity160(current_identity.boot_id_) ||
        !control::IsCanonicalIdentity160(current_identity.local_history_id_)) {
      return absl::FailedPreconditionError(
          "fenced historical owner has no exact boot-local source lineage");
    }
    source_group_term = readiness.group_term_ - 1;
    source_node_id = std::string(local_node_id);
    source_assignment_id = local_member->assignment_id;
    source_boot_id = current_identity.boot_id_;
    source_history_id = current_identity.local_history_id_;
  } else {
    const auto source_assignment =
        AssignmentId::Parse(identity.source_assignment_id_);
    if (!source_assignment.has_value()) {
      return absl::FailedPreconditionError(
          "Ready population source assignment is not canonical");
    }
    source_assignment_id = source_assignment->bytes();
  }

  return std::optional<control::CandidateProgress>(control::CandidateProgress{
      .group_id = readiness.group_id_,
      .assignment_id = readiness.assignment_id_.bytes(),
      .group_term = readiness.group_term_,
      .source_group_term = source_group_term,
      .manifest_revision = readiness.manifest_revision_,
      .manifest_digest = readiness.manifest_digest_,
      .partition_replication_epoch = readiness.partition_replication_epoch_,
      .source_node_id = std::move(source_node_id),
      .source_assignment_id = source_assignment_id,
      .source_boot_id = std::move(source_boot_id),
      .source_history_id = std::move(source_history_id),
      .applied_next_lsns = std::vector<std::uint64_t>(applied_next_lsns.begin(),
                                                      applied_next_lsns.end()),
      .recovered = recovered,
  });
}

std::optional<std::size_t> MetaLeaseChallengeRotation::Next(
    std::span<const control::WireDesiredGroup> groups,
    std::string_view local_node_id) noexcept {
  if (groups.empty()) {
    next_index_ = 0;
    return std::nullopt;
  }
  next_index_ %= groups.size();
  for (std::size_t offset = 0; offset < groups.size(); ++offset) {
    const std::size_t index = (next_index_ + offset) % groups.size();
    const control::WireDesiredGroup& group = groups[index];
    if (!group.grant_active || !group.owner_node_id.has_value() ||
        !group.owner_assignment_id.has_value() ||
        *group.owner_node_id != local_node_id) {
      continue;
    }
    next_index_ = (index + 1) % groups.size();
    return index;
  }
  return std::nullopt;
}

detail::MetaTransferAbortDisposition detail::ClassifyMetaTransferAbort(
    const control::TransferAbort& abort,
    std::optional<control::TransferKind> active_kind) noexcept {
  return abort.reason == control::TransferAbortReason::
                             kFullDesiredStateSuperseded &&
                 (active_kind == control::TransferKind::kFullDesiredState ||
                  active_kind == control::TransferKind::kNodeControlUpdate)
             ? MetaTransferAbortDisposition::kContinueAuthenticatedSession
             : MetaTransferAbortDisposition::kFailSession;
}

void detail::MetaHeartbeatProjectionGate::RequestPause(
    std::optional<std::uint64_t> outstanding_sequence) noexcept {
  pause_requested_ = true;
  if (outstanding_sequence.has_value()) {
    superseded_ack_ = *outstanding_sequence;
  }
}

bool detail::MetaHeartbeatProjectionGate::ConsumeSupersededAck(
    std::uint64_t sequence) noexcept {
  if (!superseded_ack_.has_value() || *superseded_ack_ != sequence) {
    return false;
  }
  superseded_ack_.reset();
  return true;
}

detail::MetaSessionRunResult::MetaSessionRunResult(
    absl::Status operational_status)
    : operational_status_(std::move(operational_status)),
      cleanup_status_(absl::OkStatus()) {}

detail::MetaSessionRunResult::MetaSessionRunResult(
    absl::Status operational_status, absl::Status cleanup_status)
    : operational_status_(std::move(operational_status)),
      cleanup_status_(std::move(cleanup_status)) {}

const absl::Status& detail::MetaSessionRunResult::report_status()
    const noexcept {
  return cleanup_status_.ok() ? operational_status_ : cleanup_status_;
}

struct MetaControlClientService::Impl {
  Impl(MetaControlClientOptions options, NodeControlInstaller& installer,
       TopologyCache& topology, ReplicationManager& replication,
       std::vector<MetaControlEndpoint> seeds)
      : options_(std::move(options)),
        installer_(installer),
        topology_(topology),
        replication_(replication),
        directory_(std::move(seeds)) {}

  struct DirectiveWork {
    control::Directive wire_;
    NodeDirective normalized_;
  };

  struct PendingHeartbeat {
    std::uint64_t sequence_ = 0;
    std::optional<control::LeaseChallenge> challenge_;
    std::uint32_t challenged_authority_lease_duration_ms_ = 0;
  };

  // Every field is worker-zero-owned. Detached session tasks retain this
  // object, while RunSession joins those tasks before destroying the writer
  // and stream they reference.
  struct SessionState {
    bycorf::Worker* worker_ = nullptr;
    bycorf::TcpStream* stream_ = nullptr;
    control::ControlSessionWriter* writer_ = nullptr;
    SessionIdentity session_;
    std::string boot_id_;
    ReplicationIdentity replication_identity_;
    std::shared_ptr<const control::NodeControlState> desired_;
    std::chrono::milliseconds heartbeat_interval_{};
    std::chrono::milliseconds observation_ttl_{};
    std::chrono::milliseconds progress_timeout_{};
    std::uint32_t meta_server_id_ = 0;
    std::uint64_t raft_term_ = 0;
    bool* valid_heartbeat_ack_ = nullptr;

    control::LeaseChallengeTracker challenge_tracker_;
    MetaLeaseChallengeRotation challenge_rotation_;
    std::optional<PendingHeartbeat> pending_heartbeat_;
    // Set by the sole reader before it performs any awaited lease transition.
    // This closes the Ack-before-Arm race: the producer skips arming a new
    // watchdog once receipt is known, but still waits for pending_heartbeat_
    // to clear after the transition finishes.
    bool heartbeat_ack_observed_ = false;
    std::unique_ptr<control::ControlDeadlineWatchdog> heartbeat_ack_deadline_;
    std::unique_ptr<control::ControlDeadlineWatchdog>
        inbound_transfer_deadline_;
    std::deque<DirectiveWork> directive_queue_;
    std::vector<control::WireDirectiveIdentity> accepted_directives_;
    std::vector<control::WireDirectiveIdentity> pending_results_;
    std::optional<absl::Status> terminal_error_;
    bycorf::AsyncNotification heartbeat_changed_;
    bycorf::AsyncNotification tasks_changed_;
    std::size_t active_tasks_ = 0;
    std::size_t directive_completion_tasks_ = 0;
    std::size_t target_population_completion_tasks_ = 0;
    std::size_t source_completion_tasks_ = 0;
    std::uint64_t directive_generation_ = 1;
    detail::MetaHeartbeatProjectionGate heartbeat_projection_gate_;
    bool heartbeat_running_ = false;
    bool directive_runner_running_ = false;
    bool directive_dispatch_enabled_ = true;
    bool closing_ = false;

    void Fail(absl::Status status) {
      if (status.ok()) return;
      if (!terminal_error_.has_value()) terminal_error_ = std::move(status);
      closing_ = true;
      if (heartbeat_ack_deadline_ != nullptr) {
        (void)heartbeat_ack_deadline_->Disarm();
      }
      if (inbound_transfer_deadline_ != nullptr) {
        (void)inbound_transfer_deadline_->Disarm();
      }
      directive_dispatch_enabled_ = false;
      directive_queue_.clear();
      heartbeat_changed_.NotifyAll(*worker_);
      tasks_changed_.NotifyAll(*worker_);
      if (stream_ != nullptr) (void)stream_->Close();
    }
  };

  class RunCompletionGuard {
   public:
    explicit RunCompletionGuard(Impl& owner) : owner_(owner) {}
    ~RunCompletionGuard() { owner_.FinishRun(std::move(result_)); }

    RunCompletionGuard(const RunCompletionGuard&) = delete;
    RunCompletionGuard& operator=(const RunCompletionGuard&) = delete;

    void SetResult(const absl::Status& result) { result_ = result; }

   private:
    Impl& owner_;
    // Coroutine return_value() may move the expression before local objects
    // are destroyed. Keep an independent copy so FinishRun never observes a
    // moved-from absl::Status while publishing the shutdown result.
    absl::Status result_ = absl::UnknownError(
        "Meta control service ended without a cleanup result");
  };

  void BeginRun() {
    std::lock_guard lock(shutdown_mu_);
    run_started_ = true;
  }

  void FinishRun(absl::Status result) noexcept {
    {
      std::lock_guard lock(shutdown_mu_);
      run_status_ = std::move(result);
      run_finished_ = true;
    }
    shutdown_cv_.notify_all();
  }

  void RequestStop() noexcept {
    stopping_.store(true, std::memory_order_release);
  }

  bycorf::Task<absl::Status> CancelPopulationForShutdown() {
    if (shutdown_population_result_.has_value()) {
      co_return *shutdown_population_result_;
    }
    shutdown_population_result_ =
        co_await installer_.CancelPopulationForShutdownTransition();
    co_return *shutdown_population_result_;
  }

  absl::Status WaitUntilQuiesced() {
    RequestStop();
    std::unique_lock lock(shutdown_mu_);
    // If worker zero has not started, stopping_ prevents it from entering a
    // session later; there is consequently no control mutation to join.
    if (!run_started_) return absl::OkStatus();
    shutdown_cv_.wait(lock, [this] { return run_finished_; });
    return run_status_;
  }

  static void DisableDirectiveDispatch(
      const std::shared_ptr<SessionState>& state) {
    state->directive_dispatch_enabled_ = false;
    state->directive_queue_.clear();
    // A runner may be asleep behind a long-lived completion. Wake it so the
    // barrier observes dispatch=false and exits without waiting for work that
    // the local control/fence/session transition is about to reconcile or
    // cancel.
    state->tasks_changed_.NotifyAll(*state->worker_);
  }

  static void RequestHeartbeatPause(
      const std::shared_ptr<SessionState>& state) {
    std::optional<std::uint64_t> outstanding;
    if (state->pending_heartbeat_.has_value()) {
      // The peer may already have emitted this Ack behind TransferEnd. Forget
      // its authority decision now, but retain the sequence so the sole reader
      // can consume that exact stale response after the local control
      // transition.
      outstanding = state->pending_heartbeat_->sequence_;
      state->pending_heartbeat_.reset();
      state->heartbeat_ack_observed_ = false;
      state->challenge_tracker_.Cancel();
      (void)state->heartbeat_ack_deadline_->Disarm();
    }
    state->heartbeat_projection_gate_.RequestPause(outstanding);
    state->heartbeat_changed_.NotifyAll(*state->worker_);
  }

  bycorf::Task<absl::Status> WaitForHeartbeatQuiesced(
      const std::shared_ptr<SessionState>& state) {
    while (!state->closing_ && state->heartbeat_running_ &&
           !state->heartbeat_projection_gate_.quiesced()) {
      co_await state->tasks_changed_.Wait();
    }
    if (state->terminal_error_.has_value()) {
      co_return *state->terminal_error_;
    }
    co_return absl::OkStatus();
  }

  static void ResumeHeartbeat(const std::shared_ptr<SessionState>& state) {
    state->heartbeat_projection_gate_.Resume();
    state->heartbeat_changed_.NotifyAll(*state->worker_);
  }

  bycorf::Task<bool> QuiesceHeartbeatIfRequested(
      const std::shared_ptr<SessionState>& state) {
    if (!state->heartbeat_projection_gate_.pause_requested()) co_return false;
    state->heartbeat_projection_gate_.MarkQuiesced(true);
    state->tasks_changed_.NotifyAll(*state->worker_);
    while (!state->closing_ &&
           state->heartbeat_projection_gate_.pause_requested()) {
      co_await state->heartbeat_changed_.Wait();
    }
    state->heartbeat_projection_gate_.MarkQuiesced(false);
    state->tasks_changed_.NotifyAll(*state->worker_);
    // Restart the iteration even after a successful resume: every observation
    // and challenge must be derived from the newly installed desired object.
    co_return true;
  }

  bycorf::Task<absl::StatusOr<control::WireMessage>> ReadWithDeadline(
      control::ControlFrameStream& frames, SocketDeadline& deadline,
      std::chrono::milliseconds timeout, std::string_view phase) {
    if (absl::Status armed = deadline.Arm(timeout); !armed.ok()) {
      co_return armed;
    }
    auto message = co_await frames.ReadMessage();
    if (deadline.Disarm()) {
      co_return absl::DeadlineExceededError(
          absl::StrCat(phase, " made no progress before its deadline"));
    }
    co_return message;
  }

  bycorf::Task<absl::StatusOr<ReceivedTransfer>> ReceiveTransfer(
      control::ControlFrameStream& frames, SocketDeadline& deadline,
      std::chrono::milliseconds progress_timeout,
      std::optional<control::WireMessage> first = std::nullopt) {
    StringTransferSink sink;
    control::LargeObjectReassembler reassembler(sink);
    std::optional<control::WireMessage> next = std::move(first);
    while (!sink.committed()) {
      if (stopping_.load(std::memory_order_acquire)) {
        co_return absl::CancelledError("Meta control client stopped");
      }
      if (!next.has_value()) {
        auto read = co_await ReadWithDeadline(
            frames, deadline, progress_timeout, "control object transfer");
        if (!read.ok()) co_return read.status();
        next = std::move(*read);
      }
      const bool aborted =
          std::holds_alternative<control::TransferAbort>(*next);
      absl::Status accepted = std::visit(
          [&](const auto& message) -> absl::Status {
            using T = std::decay_t<decltype(message)>;
            if constexpr (std::is_same_v<T, control::TransferStart> ||
                          std::is_same_v<T, control::TransferChunk> ||
                          std::is_same_v<T, control::TransferEnd> ||
                          std::is_same_v<T, control::TransferAbort>) {
              return reassembler.Accept(message);
            }
            return absl::InvalidArgumentError(
                "non-transfer message interrupted control object transfer");
          },
          *next);
      next.reset();
      if (!accepted.ok()) co_return accepted;
      if (aborted) {
        co_return absl::AbortedError("peer aborted control object transfer");
      }
    }
    if (!sink.kind().has_value()) {
      co_return absl::InternalError(
          "committed control object transfer has no kind");
    }
    co_return ReceivedTransfer{.kind_ = *sink.kind(),
                               .bytes_ = sink.TakeBytes()};
  }

  bycorf::Task<absl::StatusOr<control::FullDesiredState>> ReceiveFullState(
      control::ControlFrameStream& frames, SocketDeadline& deadline,
      std::chrono::milliseconds progress_timeout,
      std::optional<control::WireMessage> first = std::nullopt) {
    if (!first.has_value()) {
      auto read = co_await ReadWithDeadline(frames, deadline, progress_timeout,
                                            "initial FullDesiredState");
      if (!read.ok()) co_return read.status();
      first = std::move(*read);
    }
    if (auto* direct = std::get_if<control::FullDesiredState>(&*first)) {
      co_return std::move(*direct);
    }
    auto transfer = co_await ReceiveTransfer(frames, deadline, progress_timeout,
                                             std::move(first));
    if (!transfer.ok()) co_return transfer.status();
    if (transfer->kind_ != control::TransferKind::kFullDesiredState) {
      co_return absl::InvalidArgumentError(
          "expected initial FullDesiredState transfer");
    }
    co_return control::DecodeFullDesiredState(std::move(transfer->bytes_));
  }

  bycorf::Task<absl::Status> Install(const control::NodeControlState& desired,
                                     std::string_view local_boot_id) {
    auto prepared = PrepareNodeControlState(desired, options_.node_id_,
                                            options_.request_worker_count_);
    if (!prepared.ok()) co_return prepared.status();
    // Directory parsing is part of the all-or-nothing local control boundary.
    // Validate into a private copy before publishing topology; committing the
    // copy after the awaited installer transition cannot fail.
    MetaEndpointDirectory refreshed_directory = directory_;
    if (absl::Status refreshed =
            refreshed_directory.Refresh(desired.directory.endpoints);
        !refreshed.ok()) {
      co_return refreshed;
    }
    const ProjectionBasis basis{
        .control_revision_ = desired.local.revision,
    };
    const bool local_population_transition_expected = std::any_of(
        desired.tasks.begin(), desired.tasks.end(),
        [&](const control::WireProjectedDirective& directive) {
          return (directive.kind == control::WireDirectiveKind::kRebuild ||
                  directive.kind ==
                      control::WireDirectiveKind::kInitializeEmptyPopulation) &&
                 directive.recipient_node_id == options_.node_id_ &&
                 directive.recipient_boot_id == local_boot_id;
        });
    const std::size_t expected_source_authorization_replays = std::count_if(
        desired.tasks.begin(), desired.tasks.end(),
        [&](const control::WireProjectedDirective& directive) {
          return directive.kind ==
                     control::WireDirectiveKind::kAuthorizeSource &&
                 directive.recipient_node_id == options_.node_id_ &&
                 directive.recipient_boot_id == local_boot_id;
        });
    absl::Status installed = co_await installer_.InstallFullStateTransition(
        std::move(*prepared), basis, local_population_transition_expected,
        expected_source_authorization_replays);
    if (!installed.ok()) co_return installed;
    directory_ = std::move(refreshed_directory);
    RecordClusterControlFullStateApplied();
    co_return absl::OkStatus();
  }

  bycorf::Task<absl::Status> SendApplied(
      control::ControlSessionWriter& writer,
      const control::NodeControlState& desired,
      control::WireId128 request_id = {}) {
    co_return co_await writer.Write(
        control::MessagePriority::kReliable,
        control::WireMessage(control::FullStateApplied{
            .request_id = request_id,
            .control_revision = desired.local.revision,
        }));
  }

  bycorf::Task<absl::Status> SendDirectiveResult(
      control::ControlSessionWriter& writer,
      const control::DirectiveResult& result) {
    const control::WireMessage message(result);
    auto encoded = control::EncodeMessage(message);
    if (!encoded.ok()) co_return encoded.status();
    if (encoded->size() <= control::kMaxFramePayloadBytes) {
      co_return co_await writer.Write(control::MessagePriority::kReliable,
                                      message);
    }

    // The transfer contains the ordinary DirectiveResult payload, so the
    // receiver runs the exact same schema validation after transfer validation.
    // There is no per-chunk acknowledgement; ResultCommitted remains the
    // application-level acknowledgement for the completed result.
    auto object_id = control::GenerateId128();
    if (!object_id.ok()) co_return object_id.status();
    auto owned = std::make_shared<const std::string>(std::move(*encoded));
    co_return co_await writer.WriteTransfer(
        control::TransferKind::kDirectiveResult, *object_id, std::move(owned));
  }

  bycorf::Task<absl::Status> HandleFence(const control::Fence& fence,
                                         const SessionIdentity& session,
                                         std::string_view boot_id) {
    if (fence.session_id != session.session_id_.bytes() ||
        fence.target_boot_id != boot_id) {
      co_return absl::FailedPreconditionError(
          "fence does not target the current session and boot");
    }
    co_return co_await installer_.ApplyFenceTransition(AuthorityMessage{
        .kind_ = AuthorityMessage::Kind::kFence,
        .session_ = session,
        .projection_ = ToDomain(fence.basis),
        .anchor_ = ToDomain(fence.reject_through),
        .sent_at_ = MonotonicTime{},
        .granted_duration_ = MonotonicDuration::zero(),
    });
  }

  absl::StatusOr<NodeDirective> NormalizeDirective(
      const control::Directive& directive, const SessionIdentity& session,
      std::string_view boot_id, std::string_view local_history_id,
      const control::NodeControlState& desired) const {
    if (directive.session_id != session.session_id_.bytes()) {
      return absl::FailedPreconditionError(
          "directive does not name the current session");
    }
    if (absl::Status live = ValidateLiveDirective(directive, desired,
                                                  options_.node_id_, boot_id);
        !live.ok()) {
      return live;
    }
    NodeDirective::Kind kind = NodeDirective::Kind::kReplication;
    switch (directive.kind) {
      case control::WireDirectiveKind::kRebuild:
        kind = NodeDirective::Kind::kReplication;
        break;
      case control::WireDirectiveKind::kAuthorizeSource:
        kind = NodeDirective::Kind::kAuthorizeSource;
        break;
      case control::WireDirectiveKind::kRevokeSources:
        kind = NodeDirective::Kind::kRevokeSources;
        break;
      case control::WireDirectiveKind::kInitializeEmptyPopulation:
        kind = NodeDirective::Kind::kInitializeEmptyPopulation;
        break;
      default:
        return absl::InvalidArgumentError("unknown directive kind");
    }

    const bool rebuild = kind == NodeDirective::Kind::kReplication ||
                         kind == NodeDirective::Kind::kAuthorizeSource;
    std::uint32_t flow_count = 0;
    if (rebuild) {
      auto request = control::DecodeRebuildRequest(directive.payload);
      if (!request.ok()) return request.status();
      flow_count = request->source_flow_count;
    }
    const auto target_node = NodeId::Parse(directive.target_node_id);
    const auto target_boot = NodeId::Parse(directive.target_boot_id);
    const auto source_node = NodeId::Parse(directive.source_node_id);
    const auto source_boot = NodeId::Parse(directive.source_boot_id);
    const auto source_history =
        NodeId::Parse(directive.source_replication_history_id);
    if (!target_node.has_value() || !target_boot.has_value() ||
        !source_node.has_value() || !source_boot.has_value() ||
        !source_history.has_value()) {
      return absl::InvalidArgumentError(
          "directive contains a non-canonical 160-bit identity");
    }
    if (kind == NodeDirective::Kind::kInitializeEmptyPopulation &&
        directive.payload != local_history_id) {
      return absl::FailedPreconditionError(
          "empty population directive uses stale target history");
    }

    std::string source_host;
    std::uint16_t source_port = 0;
    const auto source_endpoint =
        std::find_if(desired.routing.nodes.begin(), desired.routing.nodes.end(),
                     [&](const control::WireDataEndpoint& endpoint) {
                       return endpoint.node_id == directive.source_node_id;
                     });
    if (source_endpoint != desired.routing.nodes.end()) {
      source_host = source_endpoint->host;
      source_port = options_.tls_context_ == nullptr
                        ? source_endpoint->port
                        : source_endpoint->tls_port;
    }

    std::vector<NodeManifestEntry> manifest_entries;
    if (kind != NodeDirective::Kind::kRevokeSources) {
      const auto manifest = std::find_if(
          desired.local.manifests.begin(), desired.local.manifests.end(),
          [&](const control::WireManifestDocument& candidate) {
            return candidate.revision == directive.manifest_revision &&
                   candidate.digest == directive.manifest_digest;
          });
      if (manifest == desired.local.manifests.end()) {
        return absl::FailedPreconditionError(
            "directive references a manifest outside the installed projection");
      }
      manifest_entries.reserve(manifest->entries.size());
      for (const control::WireManifestEntry& entry : manifest->entries) {
        manifest_entries.push_back(NodeManifestEntry{
            .partition_id_ = entry.partition_id,
            .logical_epoch_ = entry.logical_epoch,
        });
      }
    }

    return NodeDirective{
        .projection_ = ToDomain(directive.basis),
        .anchor_ = ToDomain(directive.authority),
        .operation_id_ =
            OperationId::FromBytes(directive.identity.operation_id),
        .directive_id_ =
            DirectiveId::FromBytes(directive.identity.directive_id),
        .attempt_id_ = AttemptId::FromBytes(directive.identity.attempt_id),
        .directive_revision_ = directive.identity.directive_revision,
        .kind_ = kind,
        .target_node_id_ = *target_node,
        .target_boot_id_ = *target_boot,
        .source_node_id_ =
            kind == NodeDirective::Kind::kInitializeEmptyPopulation
                ? NodeId{}
                : *source_node,
        .source_assignment_id_ =
            kind == NodeDirective::Kind::kInitializeEmptyPopulation
                ? AssignmentId{}
                : AssignmentId::FromBytes(directive.source_assignment_id),
        .source_boot_id_ =
            kind == NodeDirective::Kind::kInitializeEmptyPopulation
                ? NodeId{}
                : *source_boot,
        .source_replication_history_id_ =
            kind == NodeDirective::Kind::kInitializeEmptyPopulation
                ? NodeId{}
                : *source_history,
        .source_host_ = std::move(source_host),
        .source_port_ = source_port,
        .flow_count_ = flow_count,
        .manifest_revision_ = directive.manifest_revision,
        .manifest_digest_ = directive.manifest_digest,
        .partition_replication_epoch_ = directive.partition_replication_epoch,
        .manifest_entries_ = std::move(manifest_entries),
        .payload_ = rebuild ? std::string{} : directive.payload,

    };
  }

  bycorf::Task<absl::Status> SendTerminalDirectiveResult(
      control::ControlSessionWriter& writer,
      const std::shared_ptr<SessionState>& state,
      const control::Directive& directive, const absl::Status& applied,
      bool started) {
    const std::string result =
        applied.ok() ? "ok" : std::string(applied.message());
    control::DirectiveResult response{
        .session_id = directive.session_id,
        .recipient_boot_id = directive.recipient_boot_id,
        .assignment_id = directive.authority.assignment_id,
        .identity = directive.identity,
        .status = ClassifyDirectiveResultStatus(applied, started),
        .result = result,
    };
    if (state->pending_results_.size() >= control::kMaxProjectedDirectives) {
      co_return absl::ResourceExhaustedError(
          "too many unacknowledged directive results");
    }
    // The completion is immutable for this attempt; Meta checks duplicate
    // result bodies against its durable receipt before acknowledging identity.
    state->pending_results_.push_back(response.identity);
    RecordClusterControlDirectiveResult(applied.ok());
    co_return co_await SendDirectiveResult(writer, response);
  }

  bycorf::Task<absl::Status> ObserveDirectiveCompletion(
      std::shared_ptr<SessionState> state, control::Directive directive,
      NodeDirectiveCompletion completion, std::uint64_t generation,
      bool target_population_work) {
    absl::Status result = absl::OkStatus();
    while (result.ok() && !state->closing_ &&
           generation == state->directive_generation_) {
      std::optional<absl::Status> terminal = completion.result();
      if (terminal.has_value()) {
        result = co_await SendTerminalDirectiveResult(
            *state->writer_, state, directive, *terminal, completion.started());
        break;
      }
      result = co_await bycorf::SleepFor(*state->worker_, 10ms);
      if (!result.ok()) break;
    }
    --state->directive_completion_tasks_;
    if (target_population_work) {
      --state->target_population_completion_tasks_;
    } else {
      --state->source_completion_tasks_;
    }
    --state->active_tasks_;
    state->tasks_changed_.NotifyAll(*state->worker_);
    if (!result.ok()) state->Fail(result);
    co_return result;
  }

  bycorf::Task<absl::Status> RunDirectiveExecutor(
      std::shared_ptr<SessionState> state) {
    absl::Status result = absl::OkStatus();
    while (!state->closing_ && state->directive_dispatch_enabled_ &&
           !state->directive_queue_.empty()) {
      DirectiveWork work = std::move(state->directive_queue_.front());
      state->directive_queue_.pop_front();
      const bool target_population_work =
          work.normalized_.kind_ == NodeDirective::Kind::kReplication ||
          work.normalized_.kind_ ==
              NodeDirective::Kind::kInitializeEmptyPopulation;
      // Target-population completions may overlap only other target admissions,
      // which is the path ReplicationManager uses for exact replay and rebuild
      // supersession. Source authorization/revocation remains a serialized
      // barrier and can neither overtake nor be overtaken by that work.
      while (!state->closing_ && state->directive_dispatch_enabled_ &&
             (target_population_work
                  ? state->source_completion_tasks_ != 0
                  : state->directive_completion_tasks_ != 0)) {
        co_await state->tasks_changed_.Wait();
      }
      if (state->closing_ || !state->directive_dispatch_enabled_) break;
      NodeDirectiveCompletion started =
          co_await installer_.StartDirective(std::move(work.normalized_));
      result = co_await state->writer_->Write(
          control::MessagePriority::kReliable,
          control::WireMessage(control::DirectiveResponse{
              .session_id = work.wire_.session_id,
              .recipient_boot_id = work.wire_.recipient_boot_id,
              .identity = work.wire_.identity,
              .started = started.started(),
          }));
      if (!result.ok()) break;
      ++state->directive_completion_tasks_;
      if (target_population_work) {
        ++state->target_population_completion_tasks_;
      } else {
        ++state->source_completion_tasks_;
      }
      ++state->active_tasks_;
      state->worker_->Spawn(ObserveDirectiveCompletion(
          state, std::move(work.wire_), std::move(started),
          state->directive_generation_, target_population_work));
    }
    if (state->closing_ || !state->directive_dispatch_enabled_) {
      state->directive_queue_.clear();
    }
    state->directive_runner_running_ = false;
    --state->active_tasks_;
    state->tasks_changed_.NotifyAll(*state->worker_);
    if (!result.ok()) state->Fail(result);
    co_return result;
  }

  bycorf::Task<absl::Status> QueueDirective(
      const std::shared_ptr<SessionState>& state,
      const control::Directive& directive) {
    if (!state->directive_dispatch_enabled_) {
      co_return absl::FailedPreconditionError(
          "directive arrived while authority replacement is pending");
    }
    if (std::find(state->accepted_directives_.begin(),
                  state->accepted_directives_.end(),
                  directive.identity) != state->accepted_directives_.end()) {
      co_return absl::OkStatus();
    }
    if (state->accepted_directives_.size() >=
        control::kMaxProjectedDirectives) {
      co_return absl::ResourceExhaustedError(
          "too many directives were delivered in one session projection");
    }
    auto normalized = NormalizeDirective(
        directive, state->session_, state->boot_id_,
        state->replication_identity_.local_history_id_, *state->desired_);
    if (!normalized.ok()) co_return normalized.status();
    state->accepted_directives_.push_back(directive.identity);
    state->directive_queue_.push_back(DirectiveWork{
        .wire_ = directive, .normalized_ = std::move(*normalized)});
    if (!state->directive_runner_running_) {
      state->directive_runner_running_ = true;
      ++state->active_tasks_;
      state->worker_->Spawn(RunDirectiveExecutor(state));
    }
    co_return absl::OkStatus();
  }

  bycorf::Task<absl::Status> WaitForDirectiveExecutor(
      const std::shared_ptr<SessionState>& state) {
    while (state->directive_runner_running_) {
      co_await state->tasks_changed_.Wait();
    }
    if (state->terminal_error_.has_value()) {
      co_return *state->terminal_error_;
    }
    co_return absl::OkStatus();
  }

  bycorf::Task<absl::Status> CancelAndWaitForDirectiveCompletions(
      const std::shared_ptr<SessionState>& state) {
    ++state->directive_generation_;
    while (state->directive_completion_tasks_ != 0) {
      co_await state->tasks_changed_.Wait();
    }
    if (state->terminal_error_.has_value()) {
      co_return *state->terminal_error_;
    }
    co_return absl::OkStatus();
  }

  bycorf::Task<absl::Status> QueueCurrentTasks(
      const std::shared_ptr<SessionState>& state) {
    // Copy only this request's execution envelopes: QueueDirective may await
    // transport backpressure while the immutable selected state remains live.
    for (const auto& task : state->desired_->tasks) {
      if (task.recipient_boot_id != state->boot_id_) continue;
      auto directive = LiveDirective(task, state->session_.session_id_.bytes(),
                                     state->desired_->local.revision);
      if (auto status = co_await QueueDirective(state, directive); !status.ok())
        co_return status;
    }
    co_return absl::OkStatus();
  }

  bycorf::Task<absl::Status> ApplyControlUpdate(
      const std::shared_ptr<SessionState>& state,
      control::ControlSessionWriter& writer,
      const control::NodeControlUpdate& update) {
    auto next = std::make_shared<control::NodeControlState>(*state->desired_);
    if (auto status = control::ApplyNodeControlUpdate(*next, update);
        !status.ok())
      co_return status;
    MetaEndpointDirectory directory = directory_;
    if (auto status = directory.Refresh(next->directory.endpoints);
        !status.ok())
      co_return status;
    const bool local_changed = next->local != state->desired_->local;
    const bool tasks_changed =
        next->tasks_revision != state->desired_->tasks_revision;
    const bool routing_changed = next->routing != state->desired_->routing;
    if (std::chrono::milliseconds(control::DataHeartbeatIntervalMs(
            next->local.lease_duration_ms)) > state->observation_ttl_)
      co_return absl::InvalidArgumentError(
          "heartbeat interval exceeds observation TTL");
    if (local_changed || tasks_changed) {
      DisableDirectiveDispatch(state);
      RequestHeartbeatPause(state);
      if (auto status = co_await WaitForHeartbeatQuiesced(state); !status.ok())
        co_return status;
      if (auto status = co_await WaitForDirectiveExecutor(state); !status.ok())
        co_return status;
      // Cancel only completion observers. Matching native operations survive
      // reconciliation and their idempotent requests attach new observers.
      if (auto status = co_await CancelAndWaitForDirectiveCompletions(state);
          !status.ok())
        co_return status;
      if (auto status = co_await Install(*next, state->boot_id_); !status.ok())
        co_return status;
      state->accepted_directives_.clear();
      state->challenge_rotation_.Reset();
    } else if (routing_changed) {
      auto prepared = PrepareNodeControlState(*next, options_.node_id_,
                                              options_.request_worker_count_);
      if (!prepared.ok()) co_return prepared.status();
      if (auto status = installer_.InstallRouting(std::move(*prepared));
          !status.ok())
        co_return status;
    }
    directory_ = std::move(directory);
    state->desired_ = std::move(next);
    state->heartbeat_interval_ =
        std::chrono::milliseconds(control::DataHeartbeatIntervalMs(
            state->desired_->local.lease_duration_ms));
    if (auto status =
            co_await SendApplied(writer, *state->desired_, update.request_id);
        !status.ok())
      co_return status;
    state->directive_dispatch_enabled_ = true;
    ResumeHeartbeat(state);
    co_return co_await QueueCurrentTasks(state);
  }

  absl::Status HandleResultAck(const std::shared_ptr<SessionState>& state,
                               const control::WireMessage& message) {
    const control::WireDirectiveIdentity* identity = nullptr;
    if (const auto* committed =
            std::get_if<control::ResultCommitted>(&message)) {
      if (committed->session_id != state->session_.session_id_.bytes() ||
          committed->recipient_boot_id != state->boot_id_ ||
          committed->committed_index == 0) {
        return absl::InvalidArgumentError(
            "ResultCommitted does not name this session or a commit");
      }
      identity = &committed->identity;
    } else if (const auto* forgotten =
                   std::get_if<control::ResultNoLongerTracked>(&message)) {
      if (forgotten->session_id != state->session_.session_id_.bytes() ||
          forgotten->recipient_boot_id != state->boot_id_) {
        return absl::InvalidArgumentError(
            "ResultNoLongerTracked does not name this session");
      }
      identity = &forgotten->identity;
    } else {
      return absl::InternalError("non-result acknowledgement was dispatched");
    }
    const auto pending = std::find(state->pending_results_.begin(),
                                   state->pending_results_.end(), *identity);
    if (pending == state->pending_results_.end()) {
      return absl::FailedPreconditionError(
          "directive result acknowledgement is unknown or conflicts");
    }
    state->pending_results_.erase(pending);
    return absl::OkStatus();
  }

  bool LocalPopulationReady() const {
    const std::shared_ptr<const ServingState> current = topology_.Current();
    if (current == nullptr || current->SelfNodeIndex() == kNoNodeIndex) {
      return false;
    }
    return std::any_of(current->Groups().begin(), current->Groups().end(),
                       [&](const GroupView& group) {
                         const bool local_member =
                             group.primary_node_index_ ==
                                 current->SelfNodeIndex() ||
                             std::find(group.replica_node_indices_.begin(),
                                       group.replica_node_indices_.end(),
                                       current->SelfNodeIndex()) !=
                                 group.replica_node_indices_.end();
                         return local_member && group.population_ready_;
                       });
  }

  absl::StatusOr<std::optional<PopulationReadiness>> PopulationProof(
      const ClusterPopulationStatus& population,
      const control::NodeControlState& desired) const {
    if (population.state_ != ReplicationGroupState::kReady ||
        !population.ready_token_.has_value()) {
      return std::optional<PopulationReadiness>{};
    }
    const RebuildIdentity& ready = population.ready_token_->identity();
    const auto group = std::find_if(
        desired.local.groups.begin(), desired.local.groups.end(),
        [&](const control::WireDesiredGroup& candidate) {
          return candidate.group_id == ready.group_id_ &&
                 population.ready_token_->CanCarryForwardToTerm(
                     candidate.group_term) &&
                 candidate.manifest_revision == ready.manifest_revision_ &&
                 candidate.manifest_digest == ready.manifest_id_.bytes_ &&
                 candidate.partition_replication_epoch ==
                     ready.partition_replication_epoch_;
        });
    if (group == desired.local.groups.end() ||
        population.local_node_id_ != options_.node_id_ ||
        ready.target_node_id_ != options_.node_id_) {
      return absl::FailedPreconditionError(
          "local population proof does not match the installed local control");
    }
    const auto local_member =
        std::find_if(group->members.begin(), group->members.end(),
                     [&](const control::WireDesiredMember& member) {
                       return member.node_id == options_.node_id_;
                     });
    if (local_member == group->members.end()) {
      return absl::FailedPreconditionError(
          "local population proof names a non-member");
    }
    const AssignmentId local_assignment =
        AssignmentId::FromBytes(local_member->assignment_id);
    if (ready.assignment_id_ != local_assignment.ToHexString()) {
      return absl::FailedPreconditionError(
          "local population proof names a stale member assignment");
    }
    return std::optional<PopulationReadiness>(PopulationReadiness{
        .group_id_ = group->group_id,
        .assignment_id_ = local_assignment,
        .group_term_ = group->group_term,
        .manifest_revision_ = group->manifest_revision,
        .manifest_digest_ = group->manifest_digest,
        .partition_replication_epoch_ = group->partition_replication_epoch,
    });
  }

  bycorf::Task<absl::Status> SleepHeartbeatInterval(
      const std::shared_ptr<SessionState>& state) {
    const auto deadline =
        std::chrono::steady_clock::now() + state->heartbeat_interval_;
    while (!state->closing_ &&
           !state->heartbeat_projection_gate_.pause_requested()) {
      const auto now = std::chrono::steady_clock::now();
      if (now >= deadline) break;
      const absl::Status slept = co_await bycorf::SleepFor(
          *state->worker_,
          std::min(
              deadline - now,
              std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                  kDeadlinePollInterval)));
      if (!slept.ok()) co_return slept;
    }
    co_return absl::OkStatus();
  }

  bycorf::Task<absl::Status> RunHeartbeatProducer(
      std::shared_ptr<SessionState> state) {
    absl::Status result = absl::OkStatus();
    std::uint64_t heartbeat_sequence = 1;
    while (!state->closing_) {
      if (co_await QuiesceHeartbeatIfRequested(state)) continue;
      const ReplicationIdentity latest =
          co_await replication_.ObserveIdentity();
      if (state->heartbeat_projection_gate_.pause_requested()) continue;
      if (detail::ReplicationIdentityRequiresMetaReconnect(
              state->replication_identity_, latest)) {
        const ClusterFailoverActionStatus identity_transition_status =
            co_await replication_.cluster_failover_action_status();
        if (state->heartbeat_projection_gate_.pause_requested()) continue;
        if (detail::EvaluateMetaSessionReplicationIdentity(
                state->replication_identity_, latest,
                identity_transition_status, state->desired_->local.groups)
                .requires_reconnect_) {
          result = absl::FailedPreconditionError(
              "replication boot or history changed during the Meta session");
          break;
        }
      }
      ClusterPopulationStatus population =
          co_await replication_.cluster_population_status();
      if (state->heartbeat_projection_gate_.pause_requested()) continue;
      auto readiness = PopulationProof(population, *state->desired_);
      const bool losing_readiness =
          (!readiness.ok() || !readiness->has_value()) &&
          LocalPopulationReady();
      if (losing_readiness) {
        // Stop admission of new directives before invalidating the local proof.
        // A directive already inside ReplicationManager is joined, followed by
        // a second revocation barrier so it cannot resurrect a source export.
        DisableDirectiveDispatch(state);
      }
      const std::optional<PopulationReadiness> proof =
          readiness.ok() && !population.recovered_
              ? *readiness
              : std::optional<PopulationReadiness>{};
      result = co_await installer_.SetPopulationReadinessTransition(proof);
      if (!result.ok()) break;
      if (state->heartbeat_projection_gate_.pause_requested()) continue;
      if (!readiness.ok()) {
        result = readiness.status();
        break;
      }
      if (losing_readiness) {
        result = co_await WaitForDirectiveExecutor(state);
        if (!result.ok()) break;
        if (state->heartbeat_projection_gate_.pause_requested()) continue;
        result = co_await installer_.RevokeSourceAuthorizationsTransition();
        if (!result.ok()) break;
        if (state->heartbeat_projection_gate_.pause_requested()) continue;
      }

      ClusterSourcePauseStatus source_pause_status =
          co_await replication_.cluster_source_pause_status();
      if (state->heartbeat_projection_gate_.pause_requested()) continue;
      // A first identity mismatch may have sampled Preparing before the native
      // action published Prepared or Failed. Resample after the other awaited
      // heartbeat inputs, then use this one action snapshot for both the final
      // identity decision and the observation placed on the wire.
      ClusterFailoverActionStatus failover_status =
          co_await replication_.cluster_failover_action_status();
      if (state->heartbeat_projection_gate_.pause_requested()) continue;
      const ReplicationIdentity after_failover_status =
          co_await replication_.ObserveIdentity();
      if (state->heartbeat_projection_gate_.pause_requested()) continue;
      const detail::MetaSessionReplicationIdentityDecision identity_decision =
          detail::EvaluateMetaSessionReplicationIdentity(
              state->replication_identity_, after_failover_status,
              failover_status, state->desired_->local.groups);
      if (identity_decision.requires_reconnect_) {
        result = absl::FailedPreconditionError(
            "replication boot or history changed during failover heartbeat "
            "projection");
        break;
      }
      auto failover_observation =
          detail::ProjectClusterFailoverObservation(failover_status);
      if (!failover_observation.ok()) {
        result = failover_observation.status();
        break;
      }
      auto source_pause_observation =
          detail::ProjectClusterSourcePauseObservation(source_pause_status);
      if (!source_pause_observation.ok()) {
        result = source_pause_observation.status();
        break;
      }
      if (failover_observation->has_value() &&
          source_pause_observation->has_value()) {
        result = absl::FailedPreconditionError(
            "one Data incarnation cannot report both candidate action and "
            "source pause evidence");
        break;
      }
      if (source_pause_observation->has_value()) {
        failover_observation = std::move(source_pause_observation);
      }

      control::Heartbeat heartbeat{
          .session_id = state->session_.session_id_.bytes(),
          .heartbeat_sequence = heartbeat_sequence,
          .health = {},
          .role_information = control::NoRoleInformation{},
          .failover_observation = std::move(*failover_observation),
      };
      heartbeat.health.active_groups = static_cast<std::uint32_t>(std::count_if(
          state->desired_->local.groups.begin(),
          state->desired_->local.groups.end(),
          [&](const control::WireDesiredGroup& group) {
            return std::any_of(group.members.begin(), group.members.end(),
                               [&](const control::WireDesiredMember& member) {
                                 return member.node_id == options_.node_id_;
                               });
          }));
      heartbeat.health.storage_ready = installer_.storage_ready();
      heartbeat.health.population_ready =
          readiness->has_value() && !population.recovered_;
      heartbeat.health.summary = population.failure_reason_;
      const bool local_is_committed_owner =
          MetaLeaseChallengeRotation::IsCommittedOwner(
              state->desired_->local.groups, options_.node_id_);
      const std::optional<std::size_t> local_group_index =
          state->challenge_rotation_.Next(state->desired_->local.groups,
                                          options_.node_id_);
      std::uint32_t challenged_authority_lease_duration_ms = 0;
      std::optional<control::LeaseChallenge> heartbeat_challenge;
      if (!identity_decision.suppress_ordinary_role_ &&
          local_group_index.has_value() &&
          (!population.operator_recovery_identity_.has_value() ||
           heartbeat_sequence % 2 != 0)) {
        const control::WireDesiredGroup& local_group =
            state->desired_->local.groups[*local_group_index];
        auto nonce = control::GenerateId128();
        if (!nonce.ok()) {
          result = nonce.status();
          break;
        }
        heartbeat_challenge =
            ChallengeFor(local_group, state->desired_->local.revision, *nonce);
        heartbeat.role_information = control::AuthorityLeaseRequest{
            .challenge = *heartbeat_challenge,
        };
        challenged_authority_lease_duration_ms =
            state->desired_->local.lease_duration_ms;
        result = state->challenge_tracker_.Begin(
            state->session_.session_id_.bytes(), state->boot_id_,
            *heartbeat_challenge);
        if (!result.ok()) break;
      } else if (!identity_decision.suppress_ordinary_role_ &&
                 !local_is_committed_owner && readiness->has_value() &&
                 population.ready_token_.has_value() &&
                 population.applied_next_lsns_.has_value()) {
        auto candidate = detail::ProjectReplicaCandidateProgress(
            state->desired_->local.groups, options_.node_id_,
            after_failover_status, **readiness,
            population.ready_token_->identity(), *population.applied_next_lsns_,
            population.failover_candidate_eligible_, population.recovered_);
        if (!candidate.ok()) {
          result = candidate.status();
          break;
        }
        if (candidate->has_value()) {
          heartbeat.role_information =
              control::ReplicaCandidate{.progress = std::move(**candidate)};
        }
      }
      if (!identity_decision.suppress_ordinary_role_ &&
          population.operator_recovery_identity_.has_value()) {
        const auto& scope = *population.operator_recovery_identity_;
        for (const auto& group : state->desired_->local.groups) {
          if (group.group_id != scope.group_id_ ||
              group.group_term < scope.term_ ||
              group.manifest_revision != scope.manifest_revision_ ||
              group.manifest_digest != scope.manifest_id_.bytes_ ||
              group.partition_replication_epoch !=
                  scope.partition_replication_epoch_)
            continue;
          for (const auto& member : group.members) {
            if (member.node_id != options_.node_id_ ||
                AssignmentId::FromBytes(member.assignment_id).ToHexString() !=
                    scope.assignment_id_)
              continue;
            // Availability never doubles as a lease challenge. Alternating
            // these reports lets Meta observe an unready former Owner and
            // lets an operator start a fence before the detector threshold.
            // Meta retains availability across the intervening lease request
            // without extending the availability report's original TTL.
            if (heartbeat_challenge.has_value()) continue;
            heartbeat.role_information = control::ReplicaCandidate{
                .progress = {.group_id = group.group_id,
                             .assignment_id = member.assignment_id,
                             .group_term = group.group_term,
                             .manifest_revision = group.manifest_revision,
                             .manifest_digest = group.manifest_digest,
                             .partition_replication_epoch =
                                 group.partition_replication_epoch,
                             .source_node_id = {},
                             .source_boot_id = {},
                             .source_history_id = {},
                             .applied_next_lsns = {},
                             .operator_recovery = true}};
            heartbeat.health.summary =
                "rebuild-required: operator recovery available";
          }
        }
      }
      result = FitHeartbeatToSingleFrame(heartbeat);
      if (!result.ok()) {
        state->challenge_tracker_.Cancel();
        break;
      }
      state->pending_heartbeat_ = PendingHeartbeat{
          .sequence_ = heartbeat_sequence,
          .challenge_ = heartbeat_challenge,
          .challenged_authority_lease_duration_ms_ =
              challenged_authority_lease_duration_ms,
      };
      state->heartbeat_ack_observed_ = false;
      auto sent_at_ms = std::make_shared<std::optional<std::int64_t>>();
      const std::optional<control::WireId128> challenge_nonce =
          heartbeat_challenge.has_value()
              ? std::optional<control::WireId128>(heartbeat_challenge->nonce)
              : std::nullopt;
      result = co_await state->writer_->Write(
          control::MessagePriority::kAuthority,
          control::WireMessage(std::move(heartbeat)),
          [state, sent_at_ms, challenge_nonce] {
            if (!challenge_nonce.has_value()) return;
            *sent_at_ms = LeaseClockMillis();
            if (!state->challenge_tracker_
                     .MarkWritten(*challenge_nonce, **sent_at_ms)
                     .ok()) {
              sent_at_ms->reset();
            }
          });
      if (!result.ok()) break;
      if (state->heartbeat_projection_gate_.pause_requested()) {
        // RequestHeartbeatPause detached this sequence from authority before
        // waiting for the write. The successful write means Meta will still
        // advance its business sequence, so resume at the following value and
        // consume this one's Ack through the superseded-Ack path.
        if (heartbeat_sequence == std::numeric_limits<std::uint64_t>::max()) {
          result = absl::OutOfRangeError(
              "Meta control heartbeat sequence is exhausted");
          break;
        }
        ++heartbeat_sequence;
        continue;
      }
      if (challenge_nonce.has_value() && !sent_at_ms->has_value()) {
        result = absl::InternalError("lease challenge was not marked sent");
        break;
      }
      const bool ack_still_pending =
          state->pending_heartbeat_.has_value() &&
          state->pending_heartbeat_->sequence_ == heartbeat_sequence;
      if (ack_still_pending && !state->heartbeat_ack_observed_) {
        result = state->heartbeat_ack_deadline_->Arm(state->progress_timeout_);
        if (!result.ok()) break;
      }
      while (!state->closing_ && state->pending_heartbeat_.has_value() &&
             state->pending_heartbeat_->sequence_ == heartbeat_sequence) {
        co_await state->heartbeat_changed_.Wait();
      }
      if (state->closing_) break;
      if (heartbeat_sequence == std::numeric_limits<std::uint64_t>::max()) {
        result = absl::OutOfRangeError(
            "Meta control heartbeat sequence is exhausted");
        break;
      }
      ++heartbeat_sequence;
      result = co_await SleepHeartbeatInterval(state);
      if (!result.ok()) break;
    }

    state->heartbeat_projection_gate_.MarkQuiesced(false);
    state->heartbeat_running_ = false;
    --state->active_tasks_;
    state->tasks_changed_.NotifyAll(*state->worker_);
    if (!result.ok()) state->Fail(result);
    co_return result;
  }

  bycorf::Task<absl::Status> HandleHeartbeatAck(
      const std::shared_ptr<SessionState>& state,
      const control::HeartbeatAck& ack) {
    if (ack.session_id == state->session_.session_id_.bytes() &&
        state->heartbeat_projection_gate_.ConsumeSupersededAck(
            ack.heartbeat_sequence)) {
      // TransferEnd may overtake the response to the last heartbeat derived
      // from the previous local control. RequestHeartbeatPause detached that
      // sequence from the authority tracker before the replacement was
      // installed, so consume the response for wire progress without applying
      // its stale lease decision to the new projection. The old watchdog was
      // disarmed by RequestHeartbeatPause; touching it here could disarm the
      // watchdog already armed for the first heartbeat under the replacement
      // local control.
      *state->valid_heartbeat_ack_ = true;
      co_return absl::OkStatus();
    }
    if (!state->pending_heartbeat_.has_value() ||
        ack.session_id != state->session_.session_id_.bytes() ||
        ack.heartbeat_sequence != state->pending_heartbeat_->sequence_) {
      co_return absl::InvalidArgumentError(
          "HeartbeatAck does not match the outstanding heartbeat");
    }
    (void)state->heartbeat_ack_deadline_->Disarm();
    state->heartbeat_ack_observed_ = true;
    state->heartbeat_changed_.NotifyAll(*state->worker_);
    const PendingHeartbeat pending = *state->pending_heartbeat_;
    if (auto* grant = std::get_if<control::LeaseGranted>(&ack.lease_decision)) {
      if (!pending.challenge_.has_value() || grant->leader_id == 0 ||
          grant->leader_id != state->meta_server_id_ ||
          grant->raft_term != state->raft_term_ ||
          grant->leadership_generation == 0 ||
          grant->data_boot_id != state->boot_id_) {
        co_return absl::InvalidArgumentError(
            "lease grant does not match the accepted leader or local control "
            "authority");
      }
      if (absl::Status exact_duration =
              detail::ValidateResolvedLeaseGrantDuration(
                  grant->granted_duration_ms,
                  pending.challenged_authority_lease_duration_ms_);
          !exact_duration.ok()) {
        co_return exact_duration;
      }
      auto deadline_ms = state->challenge_tracker_.AcceptGrant(
          state->session_.session_id_.bytes(), *grant, LeaseClockMillis());
      // An exact but expired Grant is consumed by the tracker and must still
      // end this session. Continuing with heartbeat N+1 would make Meta treat
      // it as causal proof that Data installed Ack N. Session invalidation
      // revokes any older retained authority before reauthentication starts a
      // new causal sequence whose first valid Grant can safely restore service.
      if (!deadline_ms.ok()) co_return deadline_ms.status();
      const std::int64_t grant_ms = grant->granted_duration_ms;
      const auto grant_sent_at =
          MonotonicTime(std::chrono::milliseconds(*deadline_ms - grant_ms));
      // Authority transitions are barriers for the short admission lane. A
      // queued directive was validated before this grant and must be replayed
      // from the current projection instead of overtaking lease installation.
      const bool discarded_directive =
          !state->directive_queue_.empty() ||
          (state->directive_runner_running_ &&
           state->directive_completion_tasks_ != 0);
      DisableDirectiveDispatch(state);
      if (absl::Status joined = co_await WaitForDirectiveExecutor(state);
          !joined.ok()) {
        co_return joined;
      }
      absl::Status authority =
          co_await installer_.ApplyLeaseGrantTransition(AuthorityMessage{
              .kind_ = AuthorityMessage::Kind::kLeaseGrant,
              .session_ = state->session_,
              .projection_ =
                  ProjectionBasis{
                      .control_revision_ = grant->control_revision,
                  },
              .anchor_ =
                  AuthorityAnchor{
                      .group_id_ = grant->group_id,
                      .assignment_id_ =
                          AssignmentId::FromBytes(grant->assignment_id),
                      .group_term_ = grant->group_term,
                  },
              .sent_at_ = grant_sent_at,
              .granted_duration_ =
                  std::chrono::milliseconds(grant->granted_duration_ms),
          });
      if (!authority.ok()) co_return authority;
      if (discarded_directive) {
        co_return absl::AbortedError(
            "lease installation overtook queued directive admission");
      }
      state->directive_dispatch_enabled_ = true;
      RecordClusterControlLeaseGrant();
    } else if (const auto* denied =
                   std::get_if<control::LeaseDenied>(&ack.lease_decision)) {
      if (!pending.challenge_.has_value() ||
          denied->nonce != pending.challenge_->nonce) {
        co_return absl::InvalidArgumentError(
            "lease denial does not match the pending challenge");
      }
      state->challenge_tracker_.Cancel();
      RecordClusterControlLeaseDenial();
    } else if (const auto* stale = std::get_if<control::LeaseStateOutOfDate>(
                   &ack.lease_decision)) {
      if (!pending.challenge_.has_value() ||
          stale->nonce != pending.challenge_->nonce) {
        co_return absl::InvalidArgumentError(
            "out-of-date decision does not match the pending challenge");
      }
      // A commit can overtake the heartbeat projected from the preceding local
      // control. The publisher on this same session will deliver the
      // replacement; do not tear the session down and thereby revoke source
      // exports needed by that very transition. The old finite lease is not
      // renewed and normal local control installation still invalidates stale
      // authority before Ack.
      state->challenge_tracker_.Cancel();
      RecordClusterControlLeaseDenial();
    } else if (pending.challenge_.has_value()) {
      co_return absl::InvalidArgumentError(
          "HeartbeatAck omitted the lease decision");
    } else if (!std::holds_alternative<control::NoChallenge>(
                   ack.lease_decision)) {
      co_return absl::InvalidArgumentError(
          "HeartbeatAck returned a lease decision without a challenge");
    }
    *state->valid_heartbeat_ack_ = true;
    state->pending_heartbeat_.reset();
    state->heartbeat_ack_observed_ = false;
    state->heartbeat_changed_.NotifyAll(*state->worker_);
    co_return absl::OkStatus();
  }

  bycorf::Task<detail::MetaSessionRunResult> RunSession(
      bycorf::Worker& worker, const MetaControlEndpoint& endpoint,
      bool* valid_heartbeat_ack) {
    auto connected = co_await bycorf::ConnectTcp(
        worker, endpoint.host_, endpoint.port_, kConnectTimeout);
    if (!connected.ok()) co_return connected.status();
    bycorf::TcpStream stream = std::move(*connected);
    if (stopping_.load(std::memory_order_acquire)) {
      (void)stream.Close();
      // No session state was installed, so this is successful quiescence.
      // Returning Cancelled would make the outer shutdown join report an
      // unsafe cleanup even though the authoritative population-cancel step
      // still runs before Run() completes.
      co_return absl::OkStatus();
    }
    control::ControlFrameStream frames(stream);
    control::ControlSessionWriter writer(frames, kSessionWriteQueueBytes);
    if (absl::Status prepared = frames.Prepare(); !prepared.ok()) {
      co_return prepared;
    }
    SocketDeadline socket_deadline(worker, stream);
    if (options_.tls_context_ != nullptr) {
      if (absl::Status armed = socket_deadline.Arm(kHandshakeTimeout);
          !armed.ok()) {
        co_return armed;
      }
      absl::Status tls = co_await stream.StartTls(
          options_.tls_context_, /*server=*/false, endpoint.host_);
      if (socket_deadline.Disarm()) {
        co_return absl::DeadlineExceededError(
            "Meta control TLS handshake timed out");
      }
      if (!tls.ok()) co_return tls;
    }

    const ReplicationIdentity replication_identity =
        co_await replication_.ObserveIdentity();
    if (!control::IsCanonicalIdentity160(replication_identity.boot_id_) ||
        !control::IsCanonicalIdentity160(
            replication_identity.local_history_id_)) {
      co_return absl::FailedPreconditionError(
          "replication boot/history identity is not ready");
    }
    if (absl::Status armed = socket_deadline.Arm(kHandshakeTimeout);
        !armed.ok()) {
      co_return armed;
    }
    absl::Status hello_sent = co_await writer.Write(
        control::MessagePriority::kReliable,
        control::WireMessage(control::ClientHello{
            .node_id = options_.node_id_,
            .boot_id = replication_identity.boot_id_,
            .replication_history_id = replication_identity.local_history_id_,
            .replication_flow_count = options_.request_worker_count_,
        }));
    if (!hello_sent.ok()) {
      const bool expired = socket_deadline.Disarm();
      co_return expired
          ? absl::DeadlineExceededError("Meta control ClientHello timed out")
          : hello_sent;
    }
    auto hello_message = co_await frames.ReadMessage();
    if (socket_deadline.Disarm()) {
      co_return absl::DeadlineExceededError(
          "Meta control ServerHello timed out");
    }
    if (!hello_message.ok()) co_return hello_message.status();
    const auto* hello = std::get_if<control::ServerHello>(&*hello_message);
    if (hello == nullptr) {
      co_return absl::InvalidArgumentError("expected ServerHello");
    }
    if (hello->meta_server_id == 0) {
      co_return absl::InvalidArgumentError(
          "ServerHello has an empty Meta server identity");
    }
    if (endpoint.server_id_ != 0 &&
        endpoint.server_id_ != hello->meta_server_id) {
      co_return absl::FailedPreconditionError(
          "ServerHello identity does not match the learned dial target");
    }
    // Parse into a temporary directory. A TLS peer with a trusted chain/IP SAN
    // but the wrong URI identity must not poison discovery before this
    // handshake is rejected.
    MetaEndpointDirectory next_directory = directory_;
    if (absl::Status updated =
            next_directory.Update(hello->directory, hello->leader_id);
        !updated.ok()) {
      co_return updated;
    }
    const auto hello_member =
        std::find_if(hello->directory.begin(), hello->directory.end(),
                     [&](const control::WireMetaEndpoint& member) {
                       return member.server_id == hello->meta_server_id;
                     });
    if (hello_member == hello->directory.end() ||
        !hello_member->principal.has_value()) {
      co_return absl::InvalidArgumentError(
          "ServerHello identity is absent from the committed directory");
    }
    if (absl::Status pinned =
            ValidateDialedMetaIdentity(endpoint, *hello_member);
        !pinned.ok()) {
      co_return pinned;
    }
    if (hello->negotiated_version != control::kProtocolVersion ||
        hello->raft_term == 0) {
      co_return absl::InvalidArgumentError(
          "ServerHello has an invalid protocol version or Raft term");
    }
    if (options_.tls_context_ != nullptr) {
      auto sans = stream.PeerCertificateUriSans();
      if (!sans.ok()) co_return sans.status();
      const std::string& expected_principal = endpoint.principal_.has_value()
                                                  ? *endpoint.principal_
                                                  : *hello_member->principal;
      if (absl::Status identity =
              ValidateUniqueControlPrincipal(*sans, expected_principal);
          !identity.ok()) {
        co_return identity;
      }
    }
    if (hello->disposition != control::ServerHelloDisposition::kAccepted) {
      directory_ = std::move(next_directory);
      co_return absl::UnavailableError("connected Meta node is not leader");
    }
    if (!hello->leader_id.has_value() ||
        *hello->leader_id != hello->meta_server_id ||
        IsZero(hello->session_id) || hello->session_generation == 0 ||
        hello->observation_ttl_ms == 0 ||
        hello->session_progress_timeout_ms == 0) {
      co_return absl::InvalidArgumentError("invalid accepted ServerHello");
    }
    directory_ = std::move(next_directory);
    const auto progress_timeout =
        std::min(std::chrono::milliseconds(hello->session_progress_timeout_ms),
                 std::chrono::duration_cast<std::chrono::milliseconds>(
                     kMaximumSessionProgressTimeout));
    const auto boot = NodeId::Parse(replication_identity.boot_id_);
    if (!boot.has_value()) {
      co_return absl::FailedPreconditionError("invalid local boot identity");
    }
    const SessionIdentity session{
        .session_id_ = SessionId::FromBytes(hello->session_id),
        .generation_ = hello->session_generation,
        .data_boot_id_ = *boot,
    };
    std::shared_ptr<SessionState> state;
    // Keep cleanup outside the body coroutine so every return path joins the
    // heartbeat/directive producers and then awaits source-session cleanup.
    auto run_established = [&]() -> bycorf::Task<absl::Status> {
      auto initial =
          co_await ReceiveFullState(frames, socket_deadline, progress_timeout);
      if (!initial.ok()) co_return initial.status();
      if (control::DataHeartbeatIntervalMs(
              initial->authority_lease_duration_ms) >
          hello->observation_ttl_ms) {
        co_return absl::InvalidArgumentError(
            "initial local control heartbeat interval exceeds observation TTL");
      }
      if (stopping_.load(std::memory_order_acquire)) {
        co_return absl::CancelledError("Meta control client stopped");
      }
      auto selected =
          control::SelectNodeControlState(*initial, options_.node_id_);
      initial = control::FullDesiredState{};
      if (absl::Status installed =
              co_await Install(selected, replication_identity.boot_id_);
          !installed.ok()) {
        co_return installed;
      }
      if (absl::Status applied = co_await SendApplied(writer, selected);
          !applied.ok()) {
        co_return applied;
      }
      state = std::make_shared<SessionState>();
      state->worker_ = &worker;
      state->stream_ = &stream;
      state->writer_ = &writer;
      state->session_ = session;
      state->boot_id_ = replication_identity.boot_id_;
      state->replication_identity_ = replication_identity;
      state->desired_ =
          std::make_shared<control::NodeControlState>(std::move(selected));
      state->heartbeat_interval_ =
          std::chrono::milliseconds(control::DataHeartbeatIntervalMs(
              state->desired_->local.lease_duration_ms));
      state->observation_ttl_ =
          std::chrono::milliseconds(hello->observation_ttl_ms);
      state->progress_timeout_ = progress_timeout;
      state->meta_server_id_ = hello->meta_server_id;
      state->raft_term_ = hello->raft_term;
      state->valid_heartbeat_ack_ = valid_heartbeat_ack;
      const std::weak_ptr<SessionState> weak_state = state;
      state->heartbeat_ack_deadline_ =
          std::make_unique<control::ControlDeadlineWatchdog>(
              worker, [weak_state] {
                if (const auto current = weak_state.lock()) {
                  current->Fail(absl::DeadlineExceededError(
                      "Meta control HeartbeatAck made no progress before its "
                      "deadline"));
                }
              });
      state->inbound_transfer_deadline_ =
          std::make_unique<control::ControlDeadlineWatchdog>(
              worker, [weak_state] {
                if (const auto current = weak_state.lock()) {
                  current->Fail(absl::DeadlineExceededError(
                      "inbound Meta control object made no progress before "
                      "its deadline"));
                }
              });
      state->heartbeat_running_ = true;
      ++state->active_tasks_;
      worker.Spawn(RunHeartbeatProducer(state));
      SetClusterControlConnected(true);
      if (auto status = co_await QueueCurrentTasks(state); !status.ok())
        co_return status;

      StringTransferSink transfer_sink;
      control::LargeObjectReassembler transfer_reassembler(transfer_sink);
      bool transfer_active = false;
      while (!stopping_.load(std::memory_order_acquire) &&
             !worker.stop_requested() && !state->closing_) {
        auto incoming = co_await ReadWithDeadline(
            frames, socket_deadline, progress_timeout, "Meta control read");
        if (!incoming.ok()) {
          co_return state->terminal_error_.has_value() ? *state->terminal_error_
                                                       : incoming.status();
        }
        if (stopping_.load(std::memory_order_acquire)) {
          co_return absl::CancelledError("Meta control client stopped");
        }

        const bool is_transfer = std::visit(
            [](const auto& message) {
              using T = std::decay_t<decltype(message)>;
              return std::is_same_v<T, control::TransferStart> ||
                     std::is_same_v<T, control::TransferChunk> ||
                     std::is_same_v<T, control::TransferEnd> ||
                     std::is_same_v<T, control::TransferAbort>;
            },
            *incoming);
        if (is_transfer) {
          const bool starts =
              std::holds_alternative<control::TransferStart>(*incoming);
          const auto* abort = std::get_if<control::TransferAbort>(&*incoming);
          const bool aborts = abort != nullptr;
          // LargeObjectReassembler clears the sink when it consumes Abort, so
          // retain the authenticated object's type for the session decision.
          const std::optional<control::TransferKind> aborted_kind =
              aborts ? transfer_sink.kind() : std::nullopt;
          const bool advances =
              starts ||
              std::holds_alternative<control::TransferChunk>(*incoming);
          const bool finishes =
              aborts || std::holds_alternative<control::TransferEnd>(*incoming);
          if (starts && transfer_active) {
            co_return absl::FailedPreconditionError(
                "overlapping inbound control transfers are not supported");
          }
          const absl::Status accepted = std::visit(
              [&](const auto& message) -> absl::Status {
                using T = std::decay_t<decltype(message)>;
                if constexpr (std::is_same_v<T, control::TransferStart> ||
                              std::is_same_v<T, control::TransferChunk> ||
                              std::is_same_v<T, control::TransferEnd> ||
                              std::is_same_v<T, control::TransferAbort>) {
                  return transfer_reassembler.Accept(message);
                }
                return absl::InternalError(
                    "non-transfer reached the transfer reassembler");
              },
              *incoming);
          if (!accepted.ok()) co_return accepted;
          if (advances) {
            if (absl::Status armed =
                    state->inbound_transfer_deadline_->Arm(progress_timeout);
                !armed.ok()) {
              co_return armed;
            }
          }
          if (finishes) (void)state->inbound_transfer_deadline_->Disarm();
          if (starts) transfer_active = true;
          if (aborts) {
            transfer_active = false;
            if (detail::ClassifyMetaTransferAbort(*abort, aborted_kind) ==
                detail::MetaTransferAbortDisposition::
                    kContinueAuthenticatedSession) {
              // The old projection remains installed. Heartbeat production is
              // intentionally untouched while Meta retries the latest local
              // control on this authenticated session.
              continue;
            }
            co_return absl::AbortedError(
                "Meta aborted an inbound control transfer");
          }
          if (!transfer_sink.committed()) continue;
          transfer_active = false;
          if (!transfer_sink.kind().has_value()) {
            co_return absl::InternalError(
                "committed inbound transfer has no kind");
          }
          ReceivedTransfer transfer{
              .kind_ = *transfer_sink.kind(),
              .bytes_ = transfer_sink.TakeBytes(),
          };
          if (transfer.kind_ == control::TransferKind::kNodeControlUpdate) {
            auto replacement =
                control::DecodeNodeControlUpdate(std::move(transfer.bytes_));
            if (!replacement.ok()) co_return replacement.status();
            if (absl::Status applied = co_await ApplyControlUpdate(
                    state, writer, std::move(*replacement));
                !applied.ok()) {
              co_return applied;
            }
            continue;
          }
          co_return absl::InvalidArgumentError(
              "unexpected transfer kind from Meta");
        }

        // Ordinary control messages are dispatched even while a large object
        // is being reassembled. The one reader therefore never makes an
        // authority frame wait behind every bulk chunk.
        if (auto* replacement =
                std::get_if<control::NodeControlUpdate>(&*incoming)) {
          // Meta serializes complete-object senders. A direct projection in
          // the middle of another object would otherwise publish new control
          // state while retaining an incomplete old payload, so fail closed.
          if (transfer_active) {
            co_return absl::FailedPreconditionError(
                "direct control update interrupted an inbound transfer");
          }
          if (absl::Status applied = co_await ApplyControlUpdate(
                  state, writer, std::move(*replacement));
              !applied.ok()) {
            co_return applied;
          }
          continue;
        }
        if (const auto* ack = std::get_if<control::HeartbeatAck>(&*incoming)) {
          if (absl::Status handled = co_await HandleHeartbeatAck(state, *ack);
              !handled.ok()) {
            co_return handled;
          }
          continue;
        }
        if (const auto* fence = std::get_if<control::Fence>(&*incoming)) {
          DisableDirectiveDispatch(state);
          if (absl::Status fenced = co_await HandleFence(
                  *fence, session, replication_identity.boot_id_);
              !fenced.ok()) {
            co_return fenced;
          }
          if (absl::Status joined = co_await WaitForDirectiveExecutor(state);
              !joined.ok()) {
            co_return joined;
          }
          if (absl::Status cancelled =
                  co_await CancelAndWaitForDirectiveCompletions(state);
              !cancelled.ok()) {
            co_return cancelled;
          }
          if (absl::Status revoked =
                  co_await installer_.RevokeSourceAuthorizationsTransition();
              !revoked.ok()) {
            co_return revoked;
          }
          if (absl::Status acknowledged = co_await writer.Write(
                  control::MessagePriority::kAuthority,
                  control::WireMessage(control::FenceAck{
                      .session_id = fence->session_id,
                      .target_boot_id = fence->target_boot_id,
                      .reject_through = fence->reject_through,
                  }));
              !acknowledged.ok()) {
            co_return acknowledged;
          }
          continue;
        }
        if (std::holds_alternative<control::ResultCommitted>(*incoming) ||
            std::holds_alternative<control::ResultNoLongerTracked>(*incoming)) {
          if (absl::Status handled = HandleResultAck(state, *incoming);
              !handled.ok()) {
            co_return handled;
          }
          continue;
        }
        co_return absl::InvalidArgumentError(
            transfer_active
                ? "unexpected message during inbound control transfer"
                : "unexpected message in established Meta control session");
      }
      co_return absl::CancelledError("Meta control client stopped");
    };

    absl::Status session_status = co_await run_established();
    SetClusterControlConnected(false);
    if (state != nullptr) {
      state->closing_ = true;
      DisableDirectiveDispatch(state);
      state->heartbeat_changed_.NotifyAll(worker);
    }
    // Transport loss is already known; close the old write lease without an
    // await before joining heartbeat/admission/completion coroutines. The
    // later transition performs source and target population cleanup.
    (void)stream.Close();
    absl::Status immediate_invalidation = absl::OkStatus();
    absl::Status shutdown_cancel = absl::OkStatus();
    if (stopping_.load(std::memory_order_acquire)) {
      // A rebuild executor can be awaiting a native replication session that
      // is independent of this control socket. NodeControl first closes and
      // drains action admission, then resolves that exact attempt before this
      // client joins the executor.
      shutdown_cancel = co_await CancelPopulationForShutdown();
    } else {
      immediate_invalidation = installer_.InvalidateSessionNow(session);
    }
    if (state != nullptr) {
      while (state->active_tasks_ != 0) {
        co_await state->tasks_changed_.Wait();
      }
      state->stream_ = nullptr;
      state->writer_ = nullptr;
    }
    absl::Status cleanup = co_await installer_.LoseSessionTransition(
        session, "Meta control session ended");
    absl::Status cleanup_status = std::move(shutdown_cancel);
    if (cleanup_status.ok() && !immediate_invalidation.ok()) {
      cleanup_status = std::move(immediate_invalidation);
    }
    if (cleanup_status.ok() && !cleanup.ok()) {
      cleanup_status = std::move(cleanup);
    }
    co_return detail::MetaSessionRunResult(std::move(session_status),
                                           std::move(cleanup_status));
  }

  MetaControlClientOptions options_;
  NodeControlInstaller& installer_;
  TopologyCache& topology_;
  ReplicationManager& replication_;
  MetaEndpointDirectory directory_;
  MetaReconnectBackoff backoff_;
  std::atomic<bool> stopping_{false};
  unsigned prepared_thread_count_ = 0;
  // Worker-zero-owned, first result wins. Stop can arrive while connected or
  // during reconnect backoff; both paths must join the same preserve-nothing
  // target-population barrier before WaitUntilQuiesced returns.
  std::optional<absl::Status> shutdown_population_result_;

  std::mutex shutdown_mu_;
  std::condition_variable shutdown_cv_;
  bool run_started_ = false;
  bool run_finished_ = false;
  absl::Status run_status_;
};

absl::StatusOr<std::unique_ptr<MetaControlClientService>>
MetaControlClientService::Create(MetaControlClientOptions options,
                                 NodeControlInstaller& installer,
                                 TopologyCache& topology,
                                 ReplicationManager& replication) {
  if (!control::IsCanonicalIdentity160(options.node_id_)) {
    return absl::InvalidArgumentError(
        "Meta control node id must be 40 lowercase hexadecimal characters");
  }
  if (options.request_worker_count_ == 0 || options.seeds_.empty()) {
    return absl::InvalidArgumentError(
        "Meta control requires workers and at least one seed");
  }
  std::vector<MetaControlEndpoint> seeds;
  seeds.reserve(options.seeds_.size());
  for (const std::string& seed : options.seeds_) {
    auto parsed = ParseNumericControlEndpoint(seed);
    if (!parsed.ok()) return parsed.status();
    if (std::none_of(seeds.begin(), seeds.end(), [&](const auto& existing) {
          return SameEndpoint(existing, *parsed);
        })) {
      seeds.push_back(std::move(*parsed));
    }
  }
  auto impl = std::make_unique<Impl>(std::move(options), installer, topology,
                                     replication, std::move(seeds));
  return std::unique_ptr<MetaControlClientService>(
      new MetaControlClientService(std::move(impl)));
}

MetaControlClientService::MetaControlClientService(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

MetaControlClientService::~MetaControlClientService() = default;

void MetaControlClientService::Prepare(unsigned thread_count) {
  impl_->prepared_thread_count_ = thread_count;
}

bycorf::Task<absl::Status> MetaControlClientService::Run(
    bycorf::Worker& worker, bycorf::ServiceContext) {
  if (worker.id() != 0) co_return absl::OkStatus();
  impl_->BeginRun();
  absl::Status run_status = absl::OkStatus();
  Impl::RunCompletionGuard completed(*impl_);
  if (impl_->prepared_thread_count_ != impl_->options_.request_worker_count_) {
    run_status = absl::FailedPreconditionError(
        "Meta control worker count changed after configuration");
    completed.SetResult(run_status);
    co_return run_status;
  }

  bool attempted = false;
  while (!impl_->stopping_.load(std::memory_order_acquire) &&
         !worker.stop_requested()) {
    bool valid_ack = false;
    const std::vector<MetaControlEndpoint> candidates =
        impl_->directory_.Candidates();
    for (const MetaControlEndpoint& endpoint : candidates) {
      if (impl_->stopping_.load(std::memory_order_acquire) ||
          worker.stop_requested()) {
        break;
      }
      if (attempted) RecordClusterControlReconnect();
      attempted = true;
      const detail::MetaSessionRunResult session =
          co_await impl_->RunSession(worker, endpoint, &valid_ack);
      const absl::Status& reported = session.report_status();
      if (!reported.ok() && reported.code() != absl::StatusCode::kCancelled) {
        spdlog::warn("Meta control session to {} ended: {}",
                     EndpointText(endpoint.host_, endpoint.port_),
                     reported.message());
        if (reported.code() == absl::StatusCode::kInvalidArgument ||
            reported.code() == absl::StatusCode::kDataLoss) {
          RecordClusterControlProtocolError();
        }
      }
      if ((impl_->stopping_.load(std::memory_order_acquire) ||
           worker.stop_requested()) &&
          !session.shutdown_status().ok()) {
        run_status = session.shutdown_status();
        break;
      }
      if (valid_ack) {
        impl_->backoff_.Reset();
        break;
      }
    }
    if (impl_->stopping_.load(std::memory_order_acquire) ||
        worker.stop_requested()) {
      break;
    }
    auto entropy = Entropy64();
    if (!entropy.ok()) {
      if (impl_->stopping_.load(std::memory_order_acquire)) break;
      run_status = entropy.status();
      completed.SetResult(run_status);
      co_return run_status;
    }
    auto remaining = impl_->backoff_.Next(*entropy);
    while (remaining > std::chrono::milliseconds::zero() &&
           !impl_->stopping_.load(std::memory_order_acquire) &&
           !worker.stop_requested()) {
      const auto slice = std::min(remaining, kDeadlinePollInterval);
      const absl::Status slept = co_await bycorf::SleepFor(worker, slice);
      if (!slept.ok()) {
        if (impl_->stopping_.load(std::memory_order_acquire)) break;
        run_status = slept;
        completed.SetResult(run_status);
        co_return run_status;
      }
      remaining -= slice;
    }
  }
  if (impl_->stopping_.load(std::memory_order_acquire)) {
    const absl::Status shutdown_population =
        co_await impl_->CancelPopulationForShutdown();
    if (run_status.ok() && !shutdown_population.ok()) {
      run_status = shutdown_population;
    }
  }
  completed.SetResult(run_status);
  co_return run_status;
}

void MetaControlClientService::Stop() noexcept { impl_->RequestStop(); }

absl::Status MetaControlClientService::WaitUntilQuiesced() {
  return impl_->WaitUntilQuiesced();
}

std::unique_ptr<NodeControlActions> CreateReplicationNodeControlActions(
    ReplicationManager& replication, bool use_tls) {
  return std::make_unique<ReplicationNodeControlActions>(replication, use_tls);
}

}  // namespace lavik::cluster
