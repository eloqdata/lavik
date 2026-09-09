#include "keylane/cluster/meta_client.h"

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
#include "celer/io/storage.h"
#include "celer/net/tcp_stream.h"
#include "celer/net/tls.h"
#include "celer/runtime/sync.h"
#include "celer/runtime/worker.h"
#include "keylane/cluster/control_transport.h"
#include "keylane/cluster/meta_control.h"
#include "keylane/cluster/node_control.h"
#include "keylane/cluster/topology.h"
#include "keylane/metrics.h"
#include "keylane/numeric_endpoint.h"
#include "keylane/replication.h"
#include "spdlog/spdlog.h"

namespace keylane::cluster {
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
  celer::TcpStream* stream_ = nullptr;
  celer::Worker* worker_ = nullptr;
  std::chrono::steady_clock::time_point deadline_{};
  bool armed_ = false;
  bool running_ = false;
  bool expired_ = false;
};

celer::Task<absl::Status> WatchSocketDeadline(
    std::shared_ptr<SocketDeadlineState> state) {
  while (state->armed_ && state->stream_ != nullptr) {
    const auto now = std::chrono::steady_clock::now();
    if (now >= state->deadline_) {
      state->expired_ = true;
      state->armed_ = false;
      (void)state->stream_->Close();
      break;
    }
    const absl::Status slept = co_await celer::SleepFor(
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
  SocketDeadline(celer::Worker& worker, celer::TcpStream& stream)
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
      .source_meta_applied_index_ = basis.source_meta_applied_index,
      .projection_hash_ = basis.projection_hash,
  };
}

AuthorityAnchor ToDomain(const control::WireAuthorityAnchor& anchor) {
  return AuthorityAnchor{
      .group_id_ = anchor.group_id,
      .assignment_id_ = AssignmentId::FromBytes(anchor.assignment_id),
      .group_term_ = anchor.group_term,
      .authority_version_ = anchor.authority_version,
      .grant_revision_ = anchor.grant_revision,
  };
}

control::LeaseChallenge ChallengeFor(
    const control::WireDesiredGroup& group,
    const control::WireHash256& projection_hash,
    const control::WireId128& nonce) {
  return control::LeaseChallenge{
      .nonce = nonce,
      .projection_hash = projection_hash,
      .group_id = group.group_id,
      .assignment_id = *group.owner_assignment_id,
      .group_term = group.group_term,
      .authority_version = group.authority_version,
      .grant_revision = group.grant_revision,
  };
}

class StringTransferSink final : public control::LargeObjectSink {
 public:
  absl::Status Begin(const control::TransferStart& start) override {
    if (start.kind != control::TransferKind::kFullDesiredState &&
        start.kind != control::TransferKind::kDirectivePayload) {
      return absl::InvalidArgumentError(
          "Data accepts only FullDesiredState or Directive transfers");
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

class ReplicationNodeControlActions final : public NodeControlActions {
 public:
  explicit ReplicationNodeControlActions(ReplicationManager& replication)
      : replication_(replication) {}

  absl::Status RevokeSourceAuthorizations() override {
    return absl::FailedPreconditionError(
        "ReplicationManager source revocation requires an asynchronous "
        "NodeControl transition");
  }

  celer::Task<absl::Status> RevokeSourceAuthorizationsAndWait() override {
    co_return co_await replication_.RevokeClusterRebuildSourceAuthorizations();
  }

  celer::Task<absl::Status>
  ClearSourceAuthorizationsForSessionReplacementAndWait() override {
    co_return co_await replication_
        .ClearClusterRebuildSourceAuthorizationsForSessionReplacement();
  }

  celer::Task<absl::Status> ReconcilePopulation(
      std::optional<PopulationReadiness> desired,
      bool rebuild_expected) override {
    std::optional<DesiredClusterPopulation> translated;
    if (desired.has_value()) {
      translated = DesiredClusterPopulation{
          .group_id_ = std::move(desired->group_id_),
          .assignment_id_ = desired->assignment_id_.ToHexString(),
          .term_ = desired->group_term_,
          .manifest_revision_ = desired->manifest_revision_,
          .manifest_id_ = PopulationManifestId{desired->manifest_digest_},
          .partition_replication_epoch_ = desired->partition_replication_epoch_,
          .rebuild_expected_ = rebuild_expected,
      };
    }
    co_return co_await replication_.ReconcileClusterPopulation(
        std::move(translated));
  }

  celer::Task<absl::Status> CancelInProgressPopulation() override {
    co_return co_await replication_.CancelInProgressClusterPopulation();
  }

  celer::Task<absl::Status> CancelPopulationForShutdown() override {
    co_return co_await replication_.CancelClusterRebuildForShutdown();
  }

  celer::Task<NodeDirectiveCompletion> StartDirective(
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

    // Replication's native population handshake predates the structured Meta
    // anchor. Give both source and target the same canonical string identity
    // over every committed authority field so neither side can accidentally
    // collapse two assignments or grant revisions into one authorization.
    const std::string authority_id =
        EncodeRebuildAuthorityIdentity(directive.anchor_);
    RebuildDirective rebuild{
        .identity_ =
            {
                .group_id_ = directive.anchor_.group_id_,
                .assignment_id_ =
                    directive.anchor_.assignment_id_.ToHexString(),
                .term_ = directive.anchor_.group_term_,
                .directive_revision_ = directive.directive_revision_,
                .authority_id_ = authority_id,
                .source_node_id_ = directive.source_node_id_.ToHexString(),
                .source_assignment_id_ =
                    directive.source_assignment_id_.ToHexString(),
                .source_boot_id_ = directive.source_boot_id_.ToHexString(),
                .source_history_id_ =
                    directive.source_replication_history_id_.ToHexString(),
                .target_node_id_ = directive.target_node_id_.ToHexString(),
                .target_boot_id_ = directive.target_boot_id_.ToHexString(),
                .operation_id_ = directive.operation_id_.ToHexString(),
                .directive_id_ = directive.directive_id_.ToHexString(),
                .attempt_id_ = directive.attempt_id_.ToHexString(),
                .manifest_revision_ = directive.manifest_revision_,
                .manifest_id_ = manifest->id(),
                .partition_replication_epoch_ =
                    directive.partition_replication_epoch_,
            },
        .flow_count_ = directive.flow_count_,
        .safe_source_active_ = true,
    };
    if (directive.kind_ == NodeDirective::Kind::kAuthorizeSource) {
      co_return NodeDirectiveCompletion::StartedTerminal(
          co_await replication_.AuthorizeClusterRebuildSource(
              std::move(rebuild)));
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

  celer::Task<absl::Status> ApplyDirective(NodeDirective directive) override {
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
};

}  // namespace

absl::StatusOr<MetaControlEndpoint> ParseNumericControlEndpoint(
    std::string_view endpoint) {
  auto parsed = keylane::ParseNumericEndpoint(endpoint);
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
                                   const control::FullDesiredState& desired,
                                   std::string_view local_node_id,
                                   std::string_view local_boot_id) {
  if (directive.recipient_node_id != local_node_id ||
      directive.recipient_boot_id != local_boot_id) {
    return absl::FailedPreconditionError(
        "directive does not target the local node incarnation");
  }
  switch (directive.kind) {
    case control::WireDirectiveKind::kRebuild:
      if (directive.recipient_node_id != directive.target_node_id ||
          directive.recipient_boot_id != directive.target_boot_id) {
        return absl::FailedPreconditionError(
            "rebuild directive recipient is not its target incarnation");
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

  const auto group =
      std::find_if(desired.groups.begin(), desired.groups.end(),
                   [&](const control::WireDesiredGroup& candidate) {
                     return candidate.group_id == directive.authority.group_id;
                   });
  if (group == desired.groups.end() ||
      group->group_term != directive.authority.group_term ||
      group->authority_version != directive.authority.authority_version ||
      group->grant_revision != directive.authority.grant_revision ||
      group->partition_replication_epoch !=
          directive.partition_replication_epoch) {
    return absl::FailedPreconditionError(
        "directive authority is not current in the installed FDS");
  }
  const auto target = std::find_if(
      group->members.begin(), group->members.end(),
      [&](const control::WireDesiredMember& member) {
        return member.node_id == directive.target_node_id &&
               member.assignment_id == directive.authority.assignment_id;
      });
  if (target == group->members.end()) {
    return absl::FailedPreconditionError(
        "directive target assignment is not current in the installed FDS");
  }
  if (std::none_of(group->members.begin(), group->members.end(),
                   [&](const control::WireDesiredMember& member) {
                     return member.node_id == directive.source_node_id &&
                            member.assignment_id ==
                                directive.source_assignment_id;
                   })) {
    return absl::FailedPreconditionError(
        "directive source assignment is not current in the installed FDS");
  }

  const control::WireProjectedDirective projected{
      .basis = directive.basis,
      .authority = directive.authority,
      .identity = directive.identity,
      .recipient_node_id = directive.recipient_node_id,
      .recipient_boot_id = directive.recipient_boot_id,
      .target_node_id = directive.target_node_id,
      .target_boot_id = directive.target_boot_id,
      .source_node_id = directive.source_node_id,
      .source_assignment_id = directive.source_assignment_id,
      .source_boot_id = directive.source_boot_id,
      .source_replication_history_id = directive.source_replication_history_id,
      .manifest_revision = directive.manifest_revision,
      .manifest_digest = directive.manifest_digest,
      .partition_replication_epoch = directive.partition_replication_epoch,
      .kind = directive.kind,
      .payload = directive.payload,
      .preconditions = directive.preconditions,
      .storage_mutating = directive.storage_mutating,
      .force = directive.force,
  };
  if (std::find(desired.current_directives.begin(),
                desired.current_directives.end(),
                projected) == desired.current_directives.end()) {
    return absl::FailedPreconditionError(
        "live directive is not an exact member of the installed FDS");
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
                      anchor.group_term_, "/", anchor.authority_version_, "/",
                      anchor.grant_revision_);
}

std::chrono::milliseconds MetaReconnectBackoff::Next(
    std::uint64_t entropy) noexcept {
  const auto current = window_;
  const std::uint64_t choices = static_cast<std::uint64_t>(current.count()) + 1;
  const auto delay =
      std::chrono::milliseconds(static_cast<std::int64_t>(entropy % choices));
  window_ = std::min(window_ * 2, std::chrono::milliseconds(10000));
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
        absl::StrCat("keylane://meta/", endpoint.server_id);
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
    return group.owner_node_id.has_value() &&
           *group.owner_node_id == local_node_id;
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
    std::uint32_t challenged_grant_duration_ms_ = 0;
  };

  struct PendingResult {
    control::WireDirectiveIdentity identity_;
    control::WireHash256 result_hash_{};
  };

  // Every field is worker-zero-owned. Detached session tasks retain this
  // object, while RunSession joins those tasks before destroying the writer
  // and stream they reference.
  struct SessionState {
    celer::Worker* worker_ = nullptr;
    celer::TcpStream* stream_ = nullptr;
    control::ControlSessionWriter* writer_ = nullptr;
    SessionIdentity session_;
    std::string boot_id_;
    ReplicationStatus replication_status_;
    std::shared_ptr<const control::FullDesiredState> desired_;
    std::chrono::milliseconds heartbeat_interval_{};
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
    std::unique_ptr<control::ControlDeadlineWatchdog>
        heartbeat_ack_deadline_;
    std::unique_ptr<control::ControlDeadlineWatchdog>
        inbound_transfer_deadline_;
    std::deque<DirectiveWork> directive_queue_;
    std::vector<control::WireDirectiveIdentity> accepted_directives_;
    std::vector<PendingResult> pending_results_;
    std::optional<absl::Status> terminal_error_;
    celer::AsyncNotification heartbeat_changed_;
    celer::AsyncNotification tasks_changed_;
    std::size_t active_tasks_ = 0;
    std::size_t directive_completion_tasks_ = 0;
    std::size_t rebuild_completion_tasks_ = 0;
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

  celer::Task<absl::Status> CancelPopulationForShutdown() {
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
    // the FDS/fence/session transition is about to reconcile or cancel.
    state->tasks_changed_.NotifyAll(*state->worker_);
  }

  static void RequestHeartbeatPause(
      const std::shared_ptr<SessionState>& state) {
    std::optional<std::uint64_t> outstanding;
    if (state->pending_heartbeat_.has_value()) {
      // The peer may already have emitted this Ack behind TransferEnd. Forget
      // its authority decision now, but retain the sequence so the sole reader
      // can consume that exact stale response after the FDS transition.
      outstanding = state->pending_heartbeat_->sequence_;
      state->pending_heartbeat_.reset();
      state->heartbeat_ack_observed_ = false;
      state->challenge_tracker_.Cancel();
      (void)state->heartbeat_ack_deadline_->Disarm();
    }
    state->heartbeat_projection_gate_.RequestPause(outstanding);
    state->heartbeat_changed_.NotifyAll(*state->worker_);
  }

  celer::Task<absl::Status> WaitForHeartbeatQuiesced(
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

  celer::Task<bool> QuiesceHeartbeatIfRequested(
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

  celer::Task<absl::StatusOr<control::WireMessage>> ReadWithDeadline(
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

  celer::Task<absl::StatusOr<ReceivedTransfer>> ReceiveTransfer(
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

  celer::Task<absl::StatusOr<control::FullDesiredState>> ReceiveFullState(
      control::ControlFrameStream& frames, SocketDeadline& deadline,
      std::chrono::milliseconds progress_timeout,
      std::optional<control::WireMessage> first = std::nullopt) {
    if (!first.has_value()) {
      auto read = co_await ReadWithDeadline(
          frames, deadline, progress_timeout, "initial FullDesiredState");
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

  celer::Task<absl::Status> Install(const control::FullDesiredState& desired,
                                    std::string_view local_boot_id) {
    auto prepared = PrepareMetaFullState(desired, options_.node_id_,
                                         options_.request_worker_count_);
    if (!prepared.ok()) co_return prepared.status();
    // Directory parsing is part of the all-or-nothing FDS boundary. Validate
    // into a private copy before publishing topology; committing the copy after
    // the awaited installer transition cannot fail.
    MetaEndpointDirectory refreshed_directory = directory_;
    if (absl::Status refreshed =
            refreshed_directory.Refresh(desired.meta_directory);
        !refreshed.ok()) {
      co_return refreshed;
    }
    const ProjectionBasis basis{
        .source_meta_applied_index_ = desired.source_meta_applied_index,
        .projection_hash_ = desired.projection_hash,
    };
    const bool local_rebuild_expected = std::any_of(
        desired.current_directives.begin(), desired.current_directives.end(),
        [&](const control::WireProjectedDirective& directive) {
          return directive.kind == control::WireDirectiveKind::kRebuild &&
                 directive.recipient_node_id == options_.node_id_ &&
                 directive.recipient_boot_id == local_boot_id;
        });
    absl::Status installed = co_await installer_.InstallFullStateTransition(
        std::move(*prepared), basis, local_rebuild_expected);
    if (!installed.ok()) co_return installed;
    directory_ = std::move(refreshed_directory);
    RecordClusterControlFullStateApplied();
    co_return absl::OkStatus();
  }

  celer::Task<absl::Status> SendApplied(
      control::ControlSessionWriter& writer,
      const control::FullDesiredState& desired) {
    co_return co_await writer.Write(
        control::MessagePriority::kReliable,
        control::WireMessage(control::FullStateApplied{
            .source_meta_applied_index = desired.source_meta_applied_index,
            .projection_hash = desired.projection_hash,
            .object_hash = desired.object_hash,
        }));
  }

  celer::Task<absl::Status> SendDirectiveResult(
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
    // receiver runs the exact same schema validation after SHA verification.
    // There is no per-chunk acknowledgement; ResultCommitted remains the
    // application-level acknowledgement for the completed result.
    auto object_id = control::GenerateId128();
    if (!object_id.ok()) co_return object_id.status();
    auto owned = std::make_shared<const std::string>(std::move(*encoded));
    co_return co_await writer.WriteTransfer(
        control::TransferKind::kDirectiveResult, *object_id, std::move(owned));
  }

  celer::Task<absl::Status> SendOperationEvidence(
      control::ControlSessionWriter& writer,
      const control::OperationEvidence& evidence) {
    const control::WireMessage message(evidence);
    auto encoded = control::EncodeMessage(message);
    if (!encoded.ok()) co_return encoded.status();
    if (encoded->size() > control::kMaxOperationEvidenceTransferBytes) {
      co_return absl::ResourceExhaustedError(
          "encoded operation evidence exceeds its transfer cap");
    }
    if (encoded->size() <= control::kMaxFramePayloadBytes) {
      co_return co_await writer.Write(control::MessagePriority::kSoft, message);
    }
    auto object_id = control::GenerateId128();
    if (!object_id.ok()) co_return object_id.status();
    auto owned = std::make_shared<const std::string>(std::move(*encoded));
    co_return co_await writer.WriteTransfer(
        control::TransferKind::kObservationEvidence, *object_id,
        std::move(owned));
  }

  static std::string_view DirectiveKindName(
      control::WireDirectiveKind kind) noexcept {
    switch (kind) {
      case control::WireDirectiveKind::kRebuild:
        return "rebuild";
      case control::WireDirectiveKind::kAuthorizeSource:
        return "authorize-source";
      case control::WireDirectiveKind::kRevokeSources:
        return "revoke-sources";
    }
    return "unknown";
  }

  control::OperationEvidence EvidenceForDirective(
      const control::Directive& directive, std::string_view phase,
      std::string evidence) const {
    const bool executes_on_target =
        directive.kind == control::WireDirectiveKind::kRebuild;
    const std::string kind_phase =
        absl::StrCat(DirectiveKindName(directive.kind), ":", phase);
    control::OperationEvidence report{
        .session_id = directive.session_id,
        .reporter_boot_id = directive.recipient_boot_id,
        .assignment_id = executes_on_target ? directive.authority.assignment_id
                                            : directive.source_assignment_id,
        .operation_id = directive.identity.operation_id,
        .kind_phase = kind_phase,
        .evidence_hash = control::ComputeSha256(evidence),
        .evidence = std::move(evidence),
        .group_id = directive.authority.group_id,
        .group_term = directive.authority.group_term,
        .manifest_revision = directive.manifest_revision,
        .partition_replication_epoch = directive.partition_replication_epoch,
        .replication_history_id = directive.source_replication_history_id,
    };
    return report;
  }

  celer::Task<absl::Status> HandleFence(const control::Fence& fence,
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
      std::string_view boot_id,
      const control::FullDesiredState& desired) const {
    if (directive.session_id != session.session_id_.bytes()) {
      return absl::FailedPreconditionError(
          "directive does not name the current session");
    }
    if (absl::Status live = ValidateLiveDirective(directive, desired,
                                                  options_.node_id_, boot_id);
        !live.ok()) {
      return live;
    }
    NodeDirective::Kind kind;
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

    std::string source_host;
    std::uint16_t source_port = 0;
    const auto source_endpoint =
        std::find_if(desired.nodes.begin(), desired.nodes.end(),
                     [&](const control::WireDataEndpoint& endpoint) {
                       return endpoint.node_id == directive.source_node_id;
                     });
    if (source_endpoint != desired.nodes.end()) {
      source_host = source_endpoint->host;
      source_port = options_.tls_context_ == nullptr
                        ? source_endpoint->port
                        : source_endpoint->tls_port;
    }

    std::vector<NodeManifestEntry> manifest_entries;
    if (kind != NodeDirective::Kind::kRevokeSources) {
      const auto manifest = std::find_if(
          desired.manifests.begin(), desired.manifests.end(),
          [&](const control::WireManifestDocument& candidate) {
            return candidate.revision == directive.manifest_revision &&
                   candidate.digest == directive.manifest_digest;
          });
      if (manifest == desired.manifests.end()) {
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
        .source_node_id_ = *source_node,
        .source_assignment_id_ =
            AssignmentId::FromBytes(directive.source_assignment_id),
        .source_boot_id_ = *source_boot,
        .source_replication_history_id_ = *source_history,
        .source_host_ = std::move(source_host),
        .source_port_ = source_port,
        .flow_count_ = options_.request_worker_count_,
        .manifest_revision_ = directive.manifest_revision,
        .manifest_digest_ = directive.manifest_digest,
        .partition_replication_epoch_ = directive.partition_replication_epoch,
        .manifest_entries_ = std::move(manifest_entries),
        .payload_ = directive.payload,
        .preconditions_ = directive.preconditions,
        .storage_mutating_ = directive.storage_mutating,
        .force_ = directive.force,
    };
  }

  celer::Task<absl::StatusOr<NodeDirectiveCompletion>> StartDirective(
      control::ControlSessionWriter& writer,
      const control::Directive& directive, NodeDirective normalized) {
    NodeDirectiveCompletion completion =
        co_await installer_.StartDirective(std::move(normalized));
    if (!completion.started()) co_return completion;

    absl::Status sent = co_await writer.Write(
        control::MessagePriority::kReliable,
        control::WireMessage(control::DirectiveReceipt{
            .session_id = directive.session_id,
            .recipient_boot_id = directive.recipient_boot_id,
            .identity = directive.identity,
            .stage = control::DirectiveReceiptStage::kStarted,
        }));
    if (!sent.ok()) co_return sent;
    co_return completion;
  }

  celer::Task<absl::Status> SendDirectiveCompleted(
      control::ControlSessionWriter& writer,
      const control::Directive& directive) {
    co_return co_await writer.Write(
        control::MessagePriority::kReliable,
        control::WireMessage(control::DirectiveReceipt{
            .session_id = directive.session_id,
            .recipient_boot_id = directive.recipient_boot_id,
            .identity = directive.identity,
            .stage = control::DirectiveReceiptStage::kCompleted,
        }));
  }

  celer::Task<absl::Status> SendTerminalDirectiveResult(
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
        .result_hash = control::ComputeSha256(result),
        .result = result,
    };
    if (state->pending_results_.size() >= control::kMaxProjectedDirectives) {
      co_return absl::ResourceExhaustedError(
          "too many unacknowledged directive results");
    }
    state->pending_results_.push_back(PendingResult{
        .identity_ = response.identity,
        .result_hash_ = response.result_hash,
    });
    RecordClusterControlDirectiveResult(applied.ok());
    co_return co_await SendDirectiveResult(writer, response);
  }

  celer::Task<absl::Status> ObserveDirectiveCompletion(
      std::shared_ptr<SessionState> state, control::Directive directive,
      NodeDirectiveCompletion completion, std::uint64_t generation,
      bool rebuild) {
    absl::Status result = absl::OkStatus();
    if (completion.started()) {
      result = co_await SendOperationEvidence(
          *state->writer_,
          EvidenceForDirective(directive, "started", "started"));
    }
    while (result.ok() && !state->closing_ &&
           generation == state->directive_generation_) {
      std::optional<absl::Status> terminal = completion.result();
      if (terminal.has_value()) {
        result = co_await SendDirectiveCompleted(*state->writer_, directive);
        if (result.ok() && completion.started()) {
          std::string terminal_evidence =
              terminal->ok() ? "succeeded" : std::string(terminal->message());
          result = co_await SendOperationEvidence(
              *state->writer_,
              EvidenceForDirective(directive, "completed",
                                   std::move(terminal_evidence)));
        }
        if (result.ok()) {
          result = co_await SendTerminalDirectiveResult(*state->writer_, state,
                                                        directive, *terminal,
                                                        completion.started());
        }
        break;
      }
      result = co_await celer::SleepFor(*state->worker_, 10ms);
      if (!result.ok()) break;
    }
    --state->directive_completion_tasks_;
    if (rebuild) {
      --state->rebuild_completion_tasks_;
    } else {
      --state->source_completion_tasks_;
    }
    --state->active_tasks_;
    state->tasks_changed_.NotifyAll(*state->worker_);
    if (!result.ok()) state->Fail(result);
    co_return result;
  }

  celer::Task<absl::Status> RunDirectiveExecutor(
      std::shared_ptr<SessionState> state) {
    absl::Status result = absl::OkStatus();
    while (!state->closing_ && state->directive_dispatch_enabled_ &&
           !state->directive_queue_.empty()) {
      DirectiveWork work = std::move(state->directive_queue_.front());
      state->directive_queue_.pop_front();
      const bool rebuild =
          work.normalized_.kind_ == NodeDirective::Kind::kReplication;
      // Rebuild completions may overlap only other rebuild admissions, which
      // is the path ReplicationManager uses for exact replay/supersession.
      // Source authorization/revocation remains a serialized barrier and can
      // neither overtake nor be overtaken by target population work.
      while (!state->closing_ && state->directive_dispatch_enabled_ &&
             (rebuild ? state->source_completion_tasks_ != 0
                      : state->directive_completion_tasks_ != 0)) {
        co_await state->tasks_changed_.Wait();
      }
      if (state->closing_ || !state->directive_dispatch_enabled_) break;
      auto started = co_await StartDirective(*state->writer_, work.wire_,
                                             std::move(work.normalized_));
      if (!started.ok()) {
        result = started.status();
        break;
      }
      ++state->directive_completion_tasks_;
      if (rebuild) {
        ++state->rebuild_completion_tasks_;
      } else {
        ++state->source_completion_tasks_;
      }
      ++state->active_tasks_;
      state->worker_->Spawn(ObserveDirectiveCompletion(
          state, std::move(work.wire_), std::move(*started),
          state->directive_generation_, rebuild));
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

  celer::Task<absl::Status> QueueDirective(
      const std::shared_ptr<SessionState>& state,
      const control::Directive& directive) {
    if (!state->directive_dispatch_enabled_) {
      co_return absl::FailedPreconditionError(
          "directive arrived while authority replacement is pending");
    }
    if (state->accepted_directives_.size() >=
        control::kMaxProjectedDirectives) {
      co_return absl::ResourceExhaustedError(
          "too many directives were delivered in one session projection");
    }
    if (std::find(state->accepted_directives_.begin(),
                  state->accepted_directives_.end(),
                  directive.identity) != state->accepted_directives_.end()) {
      co_return absl::AlreadyExistsError(
          "directive identity was delivered twice in one projection");
    }
    auto normalized = NormalizeDirective(directive, state->session_,
                                         state->boot_id_, *state->desired_);
    if (!normalized.ok()) co_return normalized.status();
    absl::Status accepted = co_await state->writer_->Write(
        control::MessagePriority::kReliable,
        control::WireMessage(control::DirectiveReceipt{
            .session_id = directive.session_id,
            .recipient_boot_id = directive.recipient_boot_id,
            .identity = directive.identity,
            .stage = control::DirectiveReceiptStage::kAccepted,
        }));
    if (!accepted.ok()) co_return accepted;

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

  celer::Task<absl::Status> WaitForDirectiveExecutor(
      const std::shared_ptr<SessionState>& state) {
    while (state->directive_runner_running_) {
      co_await state->tasks_changed_.Wait();
    }
    if (state->terminal_error_.has_value()) {
      co_return *state->terminal_error_;
    }
    co_return absl::OkStatus();
  }

  celer::Task<absl::Status> CancelAndWaitForDirectiveCompletions(
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

  celer::Task<absl::Status> ApplyFullStateReplacement(
      const std::shared_ptr<SessionState>& state,
      control::ControlSessionWriter& writer,
      control::FullDesiredState replacement) {
    DisableDirectiveDispatch(state);
    RequestHeartbeatPause(state);
    if (absl::Status quiesced = co_await WaitForHeartbeatQuiesced(state);
        !quiesced.ok()) {
      co_return quiesced;
    }
    if (absl::Status joined = co_await WaitForDirectiveExecutor(state);
        !joined.ok()) {
      co_return joined;
    }
    // Completion belongs to the projection under which its directive was
    // accepted. Stop only the wire observers here; the FDS transition below
    // either preserves the matching native population or retires it before
    // FullStateApplied.
    if (absl::Status cancelled =
            co_await CancelAndWaitForDirectiveCompletions(state);
        !cancelled.ok()) {
      co_return cancelled;
    }
    if (absl::Status installed = co_await Install(replacement, state->boot_id_);
        !installed.ok()) {
      co_return installed;
    }
    state->desired_ = std::make_shared<control::FullDesiredState>(
        std::move(replacement));
    state->accepted_directives_.clear();
    state->challenge_rotation_.Reset();
    if (absl::Status applied = co_await SendApplied(writer, *state->desired_);
        !applied.ok()) {
      co_return applied;
    }
    // Both producers resume only after the replacement acknowledgement is
    // completely written. Every subsequent admission, readiness proof, and
    // challenge then derives from the same desired object.
    state->directive_dispatch_enabled_ = true;
    ResumeHeartbeat(state);
    co_return absl::OkStatus();
  }

  absl::Status HandleResultAck(const std::shared_ptr<SessionState>& state,
                               const control::WireMessage& message) {
    const control::WireDirectiveIdentity* identity = nullptr;
    const control::WireHash256* result_hash = nullptr;
    if (const auto* committed =
            std::get_if<control::ResultCommitted>(&message)) {
      if (committed->session_id != state->session_.session_id_.bytes() ||
          committed->recipient_boot_id != state->boot_id_ ||
          committed->committed_index == 0) {
        return absl::InvalidArgumentError(
            "ResultCommitted does not name this session or a commit");
      }
      identity = &committed->identity;
      result_hash = &committed->result_hash;
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
    const auto pending = std::find_if(
        state->pending_results_.begin(), state->pending_results_.end(),
        [&](const PendingResult& result) {
          return result.identity_ == *identity &&
                 (result_hash == nullptr ||
                  result.result_hash_ == *result_hash);
        });
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
      const control::FullDesiredState& desired) const {
    if (population.state_ != ReplicationGroupState::kReady ||
        !population.ready_token_.has_value()) {
      return std::optional<PopulationReadiness>{};
    }
    const RebuildIdentity& ready = population.ready_token_->identity();
    const auto group = std::find_if(
        desired.groups.begin(), desired.groups.end(),
        [&](const control::WireDesiredGroup& candidate) {
          return candidate.group_id == ready.group_id_ &&
                 candidate.group_term >= ready.term_ &&
                 candidate.manifest_revision == ready.manifest_revision_ &&
                 candidate.manifest_digest == ready.manifest_id_.bytes_ &&
                 candidate.partition_replication_epoch ==
                     ready.partition_replication_epoch_;
        });
    if (group == desired.groups.end() ||
        population.local_node_id_ != options_.node_id_ ||
        ready.target_node_id_ != options_.node_id_) {
      return absl::FailedPreconditionError(
          "local population proof does not match the installed FDS");
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

  celer::Task<absl::Status> SleepHeartbeatInterval(
      const std::shared_ptr<SessionState>& state) {
    const auto deadline =
        std::chrono::steady_clock::now() + state->heartbeat_interval_;
    while (!state->closing_ &&
           !state->heartbeat_projection_gate_.pause_requested()) {
      const auto now = std::chrono::steady_clock::now();
      if (now >= deadline) break;
      const absl::Status slept = co_await celer::SleepFor(
          *state->worker_,
          std::min(
              deadline - now,
              std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                  kDeadlinePollInterval)));
      if (!slept.ok()) co_return slept;
    }
    co_return absl::OkStatus();
  }

  celer::Task<absl::Status> RunHeartbeatProducer(
      std::shared_ptr<SessionState> state) {
    absl::Status result = absl::OkStatus();
    std::uint64_t heartbeat_sequence = 1;
    while (!state->closing_) {
      if (co_await QuiesceHeartbeatIfRequested(state)) continue;
      const ReplicationStatus latest = co_await replication_.Observe();
      if (state->heartbeat_projection_gate_.pause_requested()) continue;
      if (latest.boot_id_ != state->boot_id_ ||
          latest.local_history_id_ !=
              state->replication_status_.local_history_id_) {
        result = absl::FailedPreconditionError(
            "replication boot or history changed during the Meta session");
        break;
      }
      const ClusterPopulationStatus population =
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
          readiness.ok() ? *readiness : std::optional<PopulationReadiness>{};
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

      control::Heartbeat heartbeat{
          .session_id = state->session_.session_id_.bytes(),
          .heartbeat_sequence = heartbeat_sequence,
          .health = {},
          .role_information = control::NoRoleInformation{},
      };
      heartbeat.health.active_groups = static_cast<std::uint32_t>(std::count_if(
          state->desired_->groups.begin(), state->desired_->groups.end(),
          [&](const control::WireDesiredGroup& group) {
            return std::any_of(group.members.begin(), group.members.end(),
                               [&](const control::WireDesiredMember& member) {
                                 return member.node_id == options_.node_id_;
                               });
          }));
      heartbeat.health.storage_ready = installer_.storage_ready();
      heartbeat.health.population_ready = readiness->has_value();
      heartbeat.health.summary = population.failure_reason_;
      const bool local_is_committed_owner =
          MetaLeaseChallengeRotation::IsCommittedOwner(
              state->desired_->groups, options_.node_id_);
      const std::optional<std::size_t> local_group_index =
          state->challenge_rotation_.Next(state->desired_->groups,
                                          options_.node_id_);
      std::uint32_t challenged_grant_duration_ms = 0;
      std::optional<control::LeaseChallenge> heartbeat_challenge;
      if (local_group_index.has_value()) {
        const control::WireDesiredGroup& local_group =
            state->desired_->groups[*local_group_index];
        auto nonce = control::GenerateId128();
        if (!nonce.ok()) {
          result = nonce.status();
          break;
        }
        heartbeat_challenge =
            ChallengeFor(local_group, state->desired_->projection_hash, *nonce);
        heartbeat.role_information = control::AuthorityLeaseRequest{
            .challenge = *heartbeat_challenge,
        };
        challenged_grant_duration_ms = local_group.grant_duration_ms;
        result = state->challenge_tracker_.Begin(
            state->session_.session_id_.bytes(), state->boot_id_,
            *heartbeat_challenge);
        if (!result.ok()) break;
      } else if (!local_is_committed_owner && readiness->has_value() &&
                 population.ready_token_.has_value() &&
                 population.applied_next_lsns_.has_value()) {
        const ReadyToken& ready = *population.ready_token_;
        const RebuildIdentity& identity = ready.identity();
        const auto source_assignment =
            AssignmentId::Parse(identity.source_assignment_id_);
        if (!source_assignment.has_value()) {
          result = absl::FailedPreconditionError(
              "Ready population source assignment is not canonical");
          break;
        }
        heartbeat.role_information = control::ReplicaCandidate{
            .progress =
                {
                    .group_id = identity.group_id_,
                    .assignment_id = (*readiness)->assignment_id_.bytes(),
                    .group_term = (*readiness)->group_term_,
                    .manifest_revision = (*readiness)->manifest_revision_,
                    .manifest_digest = (*readiness)->manifest_digest_,
                    .partition_replication_epoch =
                        (*readiness)->partition_replication_epoch_,
                    .source_node_id = identity.source_node_id_,
                    .source_assignment_id = source_assignment->bytes(),
                    .source_boot_id = identity.source_boot_id_,
                    .source_history_id = identity.source_history_id_,
                    .applied_next_lsns = *population.applied_next_lsns_,
                },
        };
      }
      result = FitHeartbeatToSingleFrame(heartbeat);
      if (!result.ok()) {
        state->challenge_tracker_.Cancel();
        break;
      }
      state->pending_heartbeat_ = PendingHeartbeat{
          .sequence_ = heartbeat_sequence,
          .challenge_ = heartbeat_challenge,
          .challenged_grant_duration_ms_ = challenged_grant_duration_ms,
      };
      state->heartbeat_ack_observed_ = false;
      auto sent_at_ms = std::make_shared<std::optional<std::int64_t>>();
      const std::optional<control::WireId128> challenge_nonce =
          heartbeat_challenge.has_value()
              ? std::optional<control::WireId128>(heartbeat_challenge->nonce)
              : std::nullopt;
      result = co_await state->writer_->Write(
          control::MessagePriority::kAuthority, control::WireMessage(heartbeat),
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
        result =
            state->heartbeat_ack_deadline_->Arm(state->progress_timeout_);
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

  celer::Task<absl::Status> HandleHeartbeatAck(
      const std::shared_ptr<SessionState>& state,
      const control::HeartbeatAck& ack) {
    if (ack.session_id == state->session_.session_id_.bytes() &&
        state->heartbeat_projection_gate_.ConsumeSupersededAck(
            ack.heartbeat_sequence)) {
      // TransferEnd may overtake the response to the last heartbeat derived
      // from the previous FDS. RequestHeartbeatPause detached that sequence
      // from the authority tracker before the replacement was installed, so
      // consume the response for wire progress without applying its stale
      // lease decision to the new projection. The old watchdog was disarmed
      // by RequestHeartbeatPause; touching it here could disarm the watchdog
      // already armed for the first heartbeat under the replacement FDS.
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
          grant->data_boot_id != state->boot_id_ ||
          grant->granted_duration_ms > pending.challenged_grant_duration_ms_) {
        co_return absl::InvalidArgumentError(
            "lease grant does not match the accepted leader or policy");
      }
      auto deadline_ms = state->challenge_tracker_.AcceptGrant(
          state->session_.session_id_.bytes(), *grant, LeaseClockMillis());
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
                      .source_meta_applied_index_ =
                          state->desired_->source_meta_applied_index,
                      .projection_hash_ = grant->projection_hash,
                  },
              .anchor_ =
                  AuthorityAnchor{
                      .group_id_ = grant->group_id,
                      .assignment_id_ =
                          AssignmentId::FromBytes(grant->assignment_id),
                      .group_term_ = grant->group_term,
                      .authority_version_ = grant->authority_version,
                      .grant_revision_ = grant->grant_revision,
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
      state->challenge_tracker_.Cancel();
      co_return absl::FailedPreconditionError(
          "Meta reports the installed projection out of date");
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

  celer::Task<detail::MetaSessionRunResult> RunSession(
      celer::Worker& worker, const MetaControlEndpoint& endpoint,
      bool* valid_heartbeat_ack) {
    auto connected = co_await celer::ConnectTcp(
        worker, endpoint.host_, endpoint.port_, kConnectTimeout);
    if (!connected.ok()) co_return connected.status();
    celer::TcpStream stream = std::move(*connected);
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

    const ReplicationStatus replication_status =
        co_await replication_.Observe();
    if (!control::IsCanonicalIdentity160(replication_status.boot_id_) ||
        !control::IsCanonicalIdentity160(
            replication_status.local_history_id_)) {
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
            .boot_id = replication_status.boot_id_,
            .replication_history_id = replication_status.local_history_id_,
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
      const std::string& expected_principal =
          endpoint.principal_.has_value() ? *endpoint.principal_
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
        hello->heartbeat_interval_ms == 0 ||
        hello->observation_ttl_ms < hello->heartbeat_interval_ms ||
        hello->session_progress_timeout_ms == 0) {
      co_return absl::InvalidArgumentError("invalid accepted ServerHello");
    }
    directory_ = std::move(next_directory);
    const auto progress_timeout =
        std::min(std::chrono::milliseconds(hello->session_progress_timeout_ms),
                 std::chrono::duration_cast<std::chrono::milliseconds>(
                     kMaximumSessionProgressTimeout));
    const auto boot = NodeId::Parse(replication_status.boot_id_);
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
    auto run_established = [&]() -> celer::Task<absl::Status> {
      auto initial =
          co_await ReceiveFullState(frames, socket_deadline, progress_timeout);
      if (!initial.ok()) co_return initial.status();
      if (stopping_.load(std::memory_order_acquire)) {
        co_return absl::CancelledError("Meta control client stopped");
      }
      if (absl::Status installed =
              co_await Install(*initial, replication_status.boot_id_);
          !installed.ok()) {
        co_return installed;
      }
      if (absl::Status applied = co_await SendApplied(writer, *initial);
          !applied.ok()) {
        co_return applied;
      }
      state = std::make_shared<SessionState>();
      state->worker_ = &worker;
      state->stream_ = &stream;
      state->writer_ = &writer;
      state->session_ = session;
      state->boot_id_ = replication_status.boot_id_;
      state->replication_status_ = replication_status;
      state->desired_ =
          std::make_shared<control::FullDesiredState>(std::move(*initial));
      state->heartbeat_interval_ =
          std::chrono::milliseconds(hello->heartbeat_interval_ms);
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
          const bool aborts =
              std::holds_alternative<control::TransferAbort>(*incoming);
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
          if (transfer.kind_ == control::TransferKind::kFullDesiredState) {
            auto replacement =
                control::DecodeFullDesiredState(std::move(transfer.bytes_));
            if (!replacement.ok()) co_return replacement.status();
            if (absl::Status applied = co_await ApplyFullStateReplacement(
                    state, writer, std::move(*replacement));
                !applied.ok()) {
              co_return applied;
            }
            continue;
          }
          if (transfer.kind_ == control::TransferKind::kDirectivePayload) {
            auto decoded = control::DecodeMessage(
                control::MessageType::kDirective, transfer.bytes_);
            if (!decoded.ok()) co_return decoded.status();
            const auto* directive = std::get_if<control::Directive>(&*decoded);
            if (directive == nullptr) {
              co_return absl::InternalError(
                  "Directive transfer decoded as the wrong message type");
            }
            if (absl::Status queued =
                    co_await QueueDirective(state, *directive);
                !queued.ok()) {
              co_return queued;
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
                std::get_if<control::FullDesiredState>(&*incoming)) {
          // Meta serializes complete-object senders. A direct projection in
          // the middle of another object would otherwise publish new control
          // state while retaining an incomplete old payload, so fail closed.
          if (transfer_active) {
            co_return absl::FailedPreconditionError(
                "direct FullDesiredState interrupted an inbound transfer");
          }
          if (absl::Status applied = co_await ApplyFullStateReplacement(
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
                  *fence, session, replication_status.boot_id_);
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
        if (const auto* directive =
                std::get_if<control::Directive>(&*incoming)) {
          if (absl::Status queued = co_await QueueDirective(state, *directive);
              !queued.ok()) {
            co_return queued;
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
      shutdown_cancel =
          co_await CancelPopulationForShutdown();
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

celer::Task<absl::Status> MetaControlClientService::Run(celer::Worker& worker,
                                                        celer::ServiceContext) {
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
      if (!reported.ok() &&
          reported.code() != absl::StatusCode::kCancelled) {
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
      const absl::Status slept = co_await celer::SleepFor(worker, slice);
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
    ReplicationManager& replication) {
  return std::make_unique<ReplicationNodeControlActions>(replication);
}

}  // namespace keylane::cluster
