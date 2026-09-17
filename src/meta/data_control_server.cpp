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

#include "lavik/meta/data_control_server.h"

#include <arpa/inet.h>
#include <sys/socket.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <exception>
#include <future>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <set>
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
#include "bycorf/net/connection.h"
#include "bycorf/net/tcp_listener.h"
#include "bycorf/net/tcp_stream.h"
#include "bycorf/net/tls.h"
#include "bycorf/runtime/sync.h"
#include "bycorf/runtime/worker.h"
#include "lavik/cluster/control_transport.h"
#include "lavik/cluster/lease_clock.h"
#include "lavik/meta/cluster_create.h"
#include "lavik/meta/control_projector.h"
#include "lavik/meta/hash.h"
#include "lavik/meta/identity_verifier.h"
#include "lavik/meta/observation_store.h"
#include "lavik/numeric_endpoint.h"
#include "spdlog/spdlog.h"

// NuRaft's headers are not -Wpedantic-clean.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include "libnuraft/cluster_config.hxx"
#include "libnuraft/raft_server.hxx"
#pragma GCC diagnostic pop

namespace lavik::meta {
namespace {

namespace control = lavik::cluster::control;
using namespace std::chrono_literals;

constexpr auto kHandshakeTimeout = 10s;
constexpr std::uint32_t kMaxSessionProgressTimeoutMs = 10'000;
constexpr std::size_t kProjectionBuildReservationBytes =
    2 * static_cast<std::size_t>(control::kMaxFullDesiredStateBytes);

std::int64_t ActiveClockMillis() noexcept {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

absl::Status DeferInbound(std::deque<control::WireMessage>* deferred,
                          control::WireMessage message,
                          std::size_t max_messages) {
  // Fence and replacement handshakes may cross an already-sent directive
  // response. Bound that reordering window by the same byte budget as the
  // writer (every ordinary WireMessage is at most one frame) so a peer cannot
  // turn an authority acknowledgement wait into unbounded buffering.
  if (deferred->size() >= max_messages) {
    return absl::ResourceExhaustedError(
        "too many messages preceded an authority acknowledgement");
  }
  deferred->push_back(std::move(message));
  return absl::OkStatus();
}

template <std::size_t N>
bool IsZero(const std::array<std::uint8_t, N>& value) {
  return std::all_of(value.begin(), value.end(),
                     [](std::uint8_t byte) { return byte == 0; });
}

int HexNybble(char value) {
  if (value >= '0' && value <= '9') return value - '0';
  if (value >= 'a' && value <= 'f') return value - 'a' + 10;
  return -1;
}

template <std::size_t N>
absl::StatusOr<std::array<std::uint8_t, N>> ParseIdentity(
    std::string_view encoded, std::string_view field) {
  if (encoded.size() != N * 2) {
    return absl::InvalidArgumentError(
        absl::StrCat(field, " must be ", N * 2, " lowercase hex digits"));
  }
  std::array<std::uint8_t, N> result{};
  for (std::size_t i = 0; i < N; ++i) {
    const int high = HexNybble(encoded[i * 2]);
    const int low = HexNybble(encoded[i * 2 + 1]);
    if (high < 0 || low < 0) {
      return absl::InvalidArgumentError(
          absl::StrCat(field, " is not canonical lowercase hex"));
    }
    result[i] = static_cast<std::uint8_t>((high << 4) | low);
  }
  return result;
}

absl::StatusOr<control::WireMetaEndpoint> ParseMetaEndpoint(
    const MetaMemberRecord& member) {
  auto endpoint = lavik::ParseNumericEndpoint(member.data_control_endpoint_);
  if (!endpoint.has_value()) {
    return absl::InvalidArgumentError(
        "committed Meta endpoint must be numeric IPv4:port or [IPv6]:port");
  }
  return control::WireMetaEndpoint{
      .server_id = member.server_id_,
      .host = std::move(endpoint->host_),
      .port = endpoint->port_,
      .principal = member.principal_,
  };
}

bool IsTerminal(MetaOperationLifecycle lifecycle) {
  return lifecycle == MetaOperationLifecycle::kCompleted ||
         lifecycle == MetaOperationLifecycle::kAborted;
}

std::int64_t NowUnixMillis() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

struct DeadlineState {
  // Owner-worker only. The coroutine and the operation it guards are always
  // driven by the same worker, so no synchronization is needed.
  bool complete_ = false;
};

struct SessionCommitSignal {
  // One signal is shared by every session in a leader generation. The
  // coordinator callback publishes the cursor before posting the worker
  // notification; each session compares that cursor with its own validated
  // projection index, so one fast publisher cannot clear another's work.
  std::atomic<std::uint64_t> published_index_{0};
  std::atomic<bool> delivery_failed_{false};
  bycorf::AsyncNotification changed_;
};

// Bycorf's current TcpListener::Close closes the listening fd but does not
// cancel an already armed multishot accept. Wake that one waiter through the
// listener before closing it so Meta can synchronously join its accept loop.
absl::StatusOr<int> OpenShutdownAcceptWakeSocket(std::string_view bind_host,
                                                 std::uint16_t port) {
  sockaddr_storage destination{};
  socklen_t destination_size = 0;
  int family = AF_UNSPEC;

  sockaddr_in address4{};
  address4.sin_family = AF_INET;
  address4.sin_port = htons(port);
  if (::inet_pton(AF_INET, std::string(bind_host).c_str(),
                  &address4.sin_addr) == 1) {
    if (address4.sin_addr.s_addr == htonl(INADDR_ANY)) {
      address4.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    }
    family = AF_INET;
    destination_size = sizeof(address4);
    std::memcpy(&destination, &address4, sizeof(address4));
  } else {
    sockaddr_in6 address6{};
    address6.sin6_family = AF_INET6;
    address6.sin6_port = htons(port);
    if (::inet_pton(AF_INET6, std::string(bind_host).c_str(),
                    &address6.sin6_addr) != 1) {
      return absl::InvalidArgumentError(
          "data-control shutdown wake host is not numeric");
    }
    if (IN6_IS_ADDR_UNSPECIFIED(&address6.sin6_addr)) {
      address6.sin6_addr = in6addr_loopback;
    }
    family = AF_INET6;
    destination_size = sizeof(address6);
    std::memcpy(&destination, &address6, sizeof(address6));
  }

  const int fd =
      ::socket(family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
  if (fd < 0) {
    return absl::ErrnoToStatus(errno,
                               "create data-control shutdown wake socket");
  }
  if (::connect(fd, reinterpret_cast<const sockaddr*>(&destination),
                destination_size) != 0 &&
      errno != EINPROGRESS && errno != EALREADY && errno != EISCONN) {
    const absl::Status status =
        absl::ErrnoToStatus(errno, "connect data-control shutdown wake socket");
    (void)::close(fd);
    return status;
  }
  return fd;
}

bool PublishCommitIndex(SessionCommitSignal& signal, std::uint64_t index) {
  std::uint64_t published =
      signal.published_index_.load(std::memory_order_relaxed);
  while (published < index && !signal.published_index_.compare_exchange_weak(
                                  published, index, std::memory_order_release,
                                  std::memory_order_relaxed)) {
  }
  return published < index;
}

bool CommitPending(const SessionCommitSignal& signal,
                   std::uint64_t validated_index) {
  return signal.published_index_.load(std::memory_order_acquire) >
         validated_index;
}

bycorf::Task<absl::Status> CloseAtDeadline(bycorf::Worker& worker,
                                           bycorf::Connection* connection,
                                           std::shared_ptr<DeadlineState> state,
                                           std::chrono::milliseconds timeout,
                                           std::string operation) {
  const absl::Status slept = co_await bycorf::SleepFor(worker, timeout);
  if (slept.ok() && !state->complete_) {
    worker.BeginClose(
        connection,
        absl::DeadlineExceededError(absl::StrCat(operation, " timed out")),
        bycorf::CloseMode::kIdleTimeout);
  }
  co_return slept;
}

std::shared_ptr<DeadlineState> ArmDeadline(bycorf::Worker& worker,
                                           bycorf::Connection* connection,
                                           std::chrono::milliseconds timeout,
                                           std::string operation) {
  auto state = std::make_shared<DeadlineState>();
  worker.Spawn(CloseAtDeadline(worker, connection, state, timeout,
                               std::move(operation)));
  return state;
}

// One session owns one instance, hence exactly one frame reader and writer.
// Every producer goes through the bounded priority queue before sequence
// assignment; large objects use the same path one chunk at a time.
class SessionIo {
 public:
  SessionIo(bycorf::Worker& worker, bycorf::Connection* connection,
            bycorf::TcpStream& stream, std::size_t queue_bytes,
            std::chrono::milliseconds progress_timeout,
            std::chrono::milliseconds established_read_timeout)
      : worker_(worker),
        connection_(connection),
        established_read_timeout_(established_read_timeout),
        frames_(stream, progress_timeout),
        writer_(frames_, queue_bytes),
        read_deadline_(worker, [&worker, connection] {
          worker.BeginClose(connection,
                            absl::DeadlineExceededError(
                                "control session read idle/progress timed out"),
                            bycorf::CloseMode::kIdleTimeout);
        }) {}

  absl::Status Prepare() noexcept { return frames_.Prepare(); }

  bycorf::Task<absl::StatusOr<control::WireMessage>> Read() {
    if (absl::Status armed = read_deadline_.Arm(established_read_timeout_);
        !armed.ok()) {
      co_return armed;
    }
    auto message = co_await frames_.ReadMessage();
    if (read_deadline_.Disarm()) {
      co_return absl::DeadlineExceededError(
          "control session read idle/progress timed out");
    }
    co_return message;
  }

  // TLS and ClientHello share one outer deadline, so arming a second timer
  // around the Hello would create two close owners for the same connection.
  bycorf::Task<absl::StatusOr<control::WireMessage>> ReadHandshake() {
    co_return co_await frames_.ReadMessage();
  }

  bycorf::Task<absl::Status> Send(control::MessagePriority priority,
                                  control::WireMessage message) {
    co_return co_await writer_.Write(priority, std::move(message));
  }

  void FailDeadline(std::string_view operation) {
    worker_.BeginClose(
        connection_,
        absl::DeadlineExceededError(absl::StrCat(operation, " timed out")),
        bycorf::CloseMode::kIdleTimeout);
  }

 private:
  bycorf::Worker& worker_;
  bycorf::Connection* connection_;
  // Covers the permitted heartbeat-idle interval plus one complete fixed I/O
  // progress budget. The frame stream retains its separate fixed write budget.
  const std::chrono::milliseconds established_read_timeout_;
  control::ControlFrameStream frames_;
  control::ControlSessionWriter writer_;
  control::ControlDeadlineWatchdog read_deadline_;
};

// Data-to-Meta large messages remain bounded by their typed protocol caps.
// The reassembler verifies sequence and declared length before Commit
// exposes bytes to the ordinary typed message decoder.
class ClientTransferSink final : public control::LargeObjectSink {
 public:
  absl::Status Begin(const control::TransferStart& start) override {
    if (start.kind != control::TransferKind::kDirectiveResult) {
      return absl::InvalidArgumentError(
          "Meta accepts only directive result transfers");
    }
    const std::uint64_t limit = control::kMaxDirectiveResultTransferBytes;
    if (start.total_length > limit) {
      return absl::ResourceExhaustedError(
          "client transfer exceeds its typed cap");
    }
    bytes_.clear();
    bytes_.reserve(static_cast<std::size_t>(start.total_length));
    kind_ = start.kind;
    limit_ = limit;
    committed_ = false;
    return absl::OkStatus();
  }

  absl::Status Write(std::uint64_t offset, std::string_view bytes) override {
    if (!kind_.has_value() || offset != bytes_.size() ||
        bytes_.size() > limit_ || bytes.size() > limit_ - bytes_.size()) {
      return absl::FailedPreconditionError(
          "client transfer sink offset or length mismatch");
    }
    bytes_.append(bytes);
    return absl::OkStatus();
  }

  absl::Status Commit() override {
    committed_ = true;
    return absl::OkStatus();
  }

  void Abort() noexcept override {
    bytes_.clear();
    kind_.reset();
    limit_ = 0;
    committed_ = false;
  }

  bool committed() const noexcept { return committed_; }
  std::optional<control::TransferKind> kind() const noexcept { return kind_; }
  std::string Take() {
    committed_ = false;
    kind_.reset();
    limit_ = 0;
    return std::move(bytes_);
  }

 private:
  std::string bytes_;
  std::optional<control::TransferKind> kind_;
  std::uint64_t limit_ = 0;
  bool committed_ = false;
};

bool SameApplied(const control::FullStateApplied& applied,
                 const NodeControlBatch& batch) {
  return applied.control_revision == batch.full_state.control_revision;
}

control::WireAuthorityAnchor GroupAnchor(
    const control::WireDesiredGroup& group) {
  return control::WireAuthorityAnchor{
      .group_id = group.group_id,
      .assignment_id = *group.owner_assignment_id,
      .group_term = group.group_term,
  };
}

bool SameLiveAuthority(const control::WireDesiredGroup& old_group,
                       const control::FullDesiredState& latest,
                       std::string_view node_id) {
  const auto current = std::find_if(
      latest.groups.begin(), latest.groups.end(),
      [&](const auto& group) { return group.group_id == old_group.group_id; });
  return current != latest.groups.end() && current->grant_active &&
         current->owner_node_id.has_value() &&
         current->owner_assignment_id.has_value() &&
         *current->owner_node_id == node_id &&
         GroupAnchor(*current) == GroupAnchor(old_group);
}

bycorf::Task<absl::Status> FenceSupersededAuthority(
    SessionIo& io, const NodeControlBatch& installed,
    const control::FullDesiredState* latest, std::string_view node_id,
    std::string_view boot_id, const control::WireId128& session_id,
    std::deque<control::WireMessage>* deferred,
    std::size_t max_deferred_messages,
    std::vector<control::WireAuthorityAnchor>* fenced,
    control::ControlDeadlineWatchdog& ack_deadline,
    std::chrono::milliseconds progress_timeout) {
  const std::vector<control::WireAuthorityAnchor> superseded =
      UnfencedSupersededAuthorities(installed.full_state, latest, node_id,
                                    *fenced);
  for (const control::WireAuthorityAnchor& anchor : superseded) {
    const control::Fence fence{
        .session_id = session_id,
        .target_boot_id = std::string(boot_id),
        .basis =
            control::WireProjectionBasis{
                .control_revision = installed.full_state.control_revision,
            },
        .reject_through = anchor,
    };
    if (absl::Status sent = co_await io.Send(
            control::MessagePriority::kAuthority, control::WireMessage(fence));
        !sent.ok()) {
      co_return sent;
    }
    if (absl::Status armed = ack_deadline.Arm(progress_timeout); !armed.ok()) {
      co_return armed;
    }
    auto incoming = co_await io.Read();
    if (!incoming.ok()) {
      const bool expired = ack_deadline.Disarm();
      co_return expired ? absl::DeadlineExceededError(
                              "FenceAck made no progress before its deadline")
                        : incoming.status();
    }
    while (true) {
      const auto* ack = std::get_if<control::FenceAck>(&*incoming);
      if (ack != nullptr) {
        if (ack->session_id != session_id || ack->target_boot_id != boot_id ||
            ack->reject_through != anchor) {
          (void)ack_deadline.Disarm();
          co_return absl::FailedPreconditionError(
              "FenceAck does not match the superseded authority fence");
        }
        (void)ack_deadline.Disarm();
        break;
      }
      // Results for directives sent earlier can precede the FenceAck in the
      // opposite TCP direction. Preserve their exact order and handle them
      // after the authority message and heartbeat Ack complete.
      if (absl::Status buffered = DeferInbound(deferred, std::move(*incoming),
                                               max_deferred_messages);
          !buffered.ok()) {
        (void)ack_deadline.Disarm();
        co_return buffered;
      }
      incoming = co_await io.Read();
      if (!incoming.ok()) {
        const bool expired = ack_deadline.Disarm();
        co_return expired ? absl::DeadlineExceededError(
                                "FenceAck made no progress before its deadline")
                          : incoming.status();
      }
    }
    // Record only after the drain-bearing acknowledgement. If the connection
    // fails earlier, the session closes and no duplicate suppression is
    // needed; if it succeeds, every later chunk boundary can skip this exact
    // authority anchor.
    fenced->push_back(anchor);
  }
  co_return absl::OkStatus();
}

bool ReceiptMatches(const MetaTerminalReceipt& receipt,
                    const control::DirectiveResult& result,
                    std::string_view node_id,
                    const MetaBootIncarnation& boot_id) {
  const auto status = static_cast<std::uint8_t>(receipt.status_);
  const auto wire_status = static_cast<std::uint8_t>(result.status);
  return receipt.recipient_node_id_ == node_id &&
         receipt.recipient_boot_id_ == boot_id &&
         receipt.assignment_id_ == result.assignment_id &&
         status == wire_status && receipt.result_ == result.result;
}

control::ResultCommitted ResultAck(const control::DirectiveResult& result,
                                   const MetaTerminalReceipt& receipt) {
  return control::ResultCommitted{
      .session_id = result.session_id,
      .recipient_boot_id = result.recipient_boot_id,
      .identity = result.identity,
      .committed_index = receipt.committed_index_,
  };
}

control::ResultNoLongerTracked NoLongerTracked(
    const control::DirectiveResult& result) {
  return control::ResultNoLongerTracked{
      .session_id = result.session_id,
      .recipient_boot_id = result.recipient_boot_id,
      .identity = result.identity,
  };
}

}  // namespace

detail::MetaCommittedViewCache::MetaCommittedViewCache(Loader loader)
    : loader_(std::move(loader)) {}

std::shared_ptr<const MetaCommittedView> detail::MetaCommittedViewCache::Adopt(
    MetaCommittedView view) {
  if (cached_ == nullptr || view.applied_index() >= cached_->applied_index()) {
    cached_ = std::make_shared<const MetaCommittedView>(std::move(view));
  }
  return cached_;
}

absl::StatusOr<std::shared_ptr<const MetaCommittedView>>
detail::MetaCommittedViewCache::Get(std::uint64_t minimum_applied_index) {
  if (cached_ != nullptr && cached_->applied_index() >= minimum_applied_index) {
    return cached_;
  }
  auto loaded = std::make_shared<const MetaCommittedView>(loader_());
  if (loaded->applied_index() < minimum_applied_index) {
    return absl::InternalError(
        "atomic committed view fell behind its published high-water");
  }
  cached_ = std::move(loaded);
  return cached_;
}

bool detail::TransferBoundaryNeedsProjectionValidation(
    std::uint64_t published_index, std::uint64_t committed_high_water,
    std::uint64_t validated_index) noexcept {
  return published_index > validated_index ||
         committed_high_water > validated_index;
}

void detail::RecordEquivalentTransferBoundary(
    std::uint64_t applied_index, std::uint64_t* validated_index) noexcept {
  *validated_index = std::max(*validated_index, applied_index);
}

detail::MetaPublisherTransferDisposition detail::ClassifyPublisherSupersession(
    bool receiver_can_apply) noexcept {
  return receiver_can_apply
             ? MetaPublisherTransferDisposition::
                   kAwaitExactAppliedAndRetryInSession
             : MetaPublisherTransferDisposition::kRetryBeforeApplyInSession;
}

void detail::MetaPublisherAdoptionGate::ObserveAppliedReceipt() noexcept {
  pending_ = true;
}

void detail::MetaPublisherAdoptionGate::MarkProjectionAdopted() noexcept {
  pending_ = false;
}

bool detail::MetaPublisherAdoptionGate::pending() const noexcept {
  return pending_;
}

absl::StatusOr<std::optional<MetaObservedFailoverProjection>>
detail::FailoverProjectionForHeartbeat(
    const control::FullDesiredState& installed, std::string_view node_id,
    const MetaBootIncarnation& boot) {
  std::optional<MetaObservedFailoverProjection> result;
  for (const control::WireDesiredGroup& group : installed.groups) {
    if (!group.failover_transition.has_value() ||
        !group.failover_transition->candidate_action.has_value()) {
      continue;
    }
    const control::WireFailoverTransition& transition =
        *group.failover_transition;
    const control::WireFailoverCandidateAction& action =
        *transition.candidate_action;
    if (action.candidate.node_id != node_id) continue;
    auto candidate_boot = ParseIdentity<20>(
        action.candidate.boot_id, "installed failover candidate boot id");
    if (!candidate_boot.ok()) return candidate_boot.status();
    if (*candidate_boot != boot) continue;
    if (result.has_value()) {
      return absl::FailedPreconditionError(
          "installed FDS binds one Data incarnation to multiple failover "
          "candidate actions");
    }
    result = MetaObservedFailoverProjection{
        .group_id_ = group.group_id,
        .group_term_ = group.group_term,
        .transition_id_ = transition.transition_id,
        .transition_revision_ = transition.revision,
        .action_id_ = action.action_id,
        .candidate_node_id_ = action.candidate.node_id,
        .candidate_assignment_id_ = action.candidate.assignment_id,
        .candidate_boot_id_ = *candidate_boot,
    };
  }
  return result;
}

absl::StatusOr<std::optional<MetaObservedOwnerProjection>>
detail::OwnerProjectionForHeartbeat(const control::FullDesiredState& installed,
                                    std::string_view node_id) {
  std::optional<MetaObservedOwnerProjection> result;
  for (const control::WireDesiredGroup& group : installed.groups) {
    if (!group.owner_node_id.has_value() || *group.owner_node_id != node_id) {
      continue;
    }
    if (!group.owner_assignment_id.has_value()) {
      return absl::FailedPreconditionError(
          "installed Owner is missing its assignment identity");
    }
    if (result.has_value()) {
      return absl::FailedPreconditionError(
          "installed FDS binds one Data node as Owner of multiple Groups");
    }
    result = MetaObservedOwnerProjection{
        .group_id_ = group.group_id,
        .owner_node_id_ = *group.owner_node_id,
        .owner_assignment_id_ = *group.owner_assignment_id,
        .group_term_ = group.group_term,
        .control_revision_ = installed.control_revision,
        .authority_lease_duration_ms_ = installed.authority_lease_duration_ms,
    };
  }
  return result;
}

std::optional<std::uint64_t> detail::ConfirmedLeaseForHeartbeat(
    const std::optional<control::HeartbeatAck>& previous_ack,
    std::uint64_t heartbeat_sequence, std::string_view authenticated_boot_id,
    const std::optional<MetaObservedOwnerProjection>& owner_projection) {
  if (!previous_ack.has_value() || !owner_projection.has_value() ||
      heartbeat_sequence <= previous_ack->heartbeat_sequence) {
    return std::nullopt;
  }
  const auto* granted =
      std::get_if<control::LeaseGranted>(&previous_ack->lease_decision);
  if (granted == nullptr || granted->granted_duration_ms == 0 ||
      granted->data_boot_id != authenticated_boot_id ||
      granted->control_revision != owner_projection->control_revision_ ||
      granted->group_id != owner_projection->group_id_ ||
      granted->assignment_id != owner_projection->owner_assignment_id_ ||
      granted->group_term != owner_projection->group_term_) {
    return std::nullopt;
  }
  if (granted->granted_duration_ms !=
      owner_projection->authority_lease_duration_ms_) {
    return std::nullopt;
  }
  return previous_ack->heartbeat_sequence;
}

absl::Status detail::ApplyLeadershipValidityLimit(
    NodeControlBatch& batch, std::uint32_t leadership_validity_ms) {
  if (leadership_validity_ms == 0 ||
      batch.full_state.authority_lease_duration_ms == 0) {
    return absl::FailedPreconditionError(
        "authority lease and leadership-validity durations must be nonzero");
  }
  control::FullDesiredState& state = batch.full_state;
  state.authority_lease_duration_ms =
      std::min(state.authority_lease_duration_ms, leadership_validity_ms);

  auto encoded = control::EncodeFullDesiredState(state);
  if (!encoded.ok()) return encoded.status();
  batch.encoded_full_state = std::move(*encoded);
  return absl::OkStatus();
}

MetaHeartbeatObservationResult IngestHeartbeatObservations(
    MetaObservationStore& observations, const MetaCommittedFacts& facts,
    std::string_view node_id, const MetaBootIncarnation& boot,
    const MetaReplicationHistoryId& session_history, std::uint64_t generation,
    const control::HeartbeatHealth& health,
    const control::HeartbeatRoleInformation& role_information,
    std::int64_t now_unix_ms) {
  return IngestHeartbeatObservations(
      observations, facts, node_id, boot, session_history, generation, health,
      role_information, std::nullopt, now_unix_ms);
}

MetaHeartbeatObservationResult IngestHeartbeatObservations(
    MetaObservationStore& observations, const MetaCommittedFacts& facts,
    std::string_view node_id, const MetaBootIncarnation& boot,
    const MetaReplicationHistoryId& session_history, std::uint64_t generation,
    const control::HeartbeatHealth& health,
    const control::HeartbeatRoleInformation& role_information,
    const std::optional<control::FailoverObservation>& failover_observation,
    std::int64_t now_unix_ms) {
  return IngestHeartbeatObservations(
      observations, facts, node_id, boot, session_history, generation, health,
      role_information, failover_observation, std::nullopt, now_unix_ms);
}

MetaHeartbeatObservationResult IngestHeartbeatObservations(
    MetaObservationStore& observations, const MetaCommittedFacts& facts,
    std::string_view node_id, const MetaBootIncarnation& boot,
    const MetaReplicationHistoryId& session_history, std::uint64_t generation,
    const control::HeartbeatHealth& health,
    const control::HeartbeatRoleInformation& role_information,
    const std::optional<control::FailoverObservation>& failover_observation,
    std::optional<MetaObservedFailoverProjection> failover_projection,
    std::int64_t now_unix_ms) {
  return IngestHeartbeatObservations(
      observations, facts, node_id, boot, session_history, generation, health,
      role_information, failover_observation, std::move(failover_projection),
      std::nullopt, 0, std::nullopt, now_unix_ms, /*now_steady_ms=*/0);
}

MetaHeartbeatObservationResult IngestHeartbeatObservations(
    MetaObservationStore& observations, const MetaCommittedFacts& facts,
    std::string_view node_id, const MetaBootIncarnation& boot,
    const MetaReplicationHistoryId& session_history, std::uint64_t generation,
    const control::HeartbeatHealth& health,
    const control::HeartbeatRoleInformation& role_information,
    const std::optional<control::FailoverObservation>& failover_observation,
    std::optional<MetaObservedFailoverProjection> failover_projection,
    std::optional<MetaObservedOwnerProjection> owner_projection,
    std::uint64_t heartbeat_sequence,
    std::optional<std::uint64_t> confirmed_grant_sequence,
    std::int64_t now_unix_ms, std::uint64_t now_steady_ms) {
  (void)observations.MaybeSweepExpired(now_unix_ms);
  const MetaObservationIdentity identity{std::string(node_id), boot,
                                         generation};
  MetaHeartbeatObservationResult result{
      .status = control::ObservationStatus::kAccepted, .detail = {}};
  const auto record_rejection = [&](std::string_view component,
                                    const absl::Status& status) {
    result.status = control::ObservationStatus::kRejected;
    if (!result.detail.empty()) result.detail.append("; ");
    result.detail.append(component);
    result.detail.append(": ");
    result.detail.append(status.message());
  };

  MetaNodeHealthObs health_observation{
      .storage_ready_ = health.storage_ready,
      .population_ready_ = health.population_ready,
      .draining_ = health.draining,
      .active_groups_ = health.active_groups,
      // The typed fields already carry machine-readable health. Retain only
      // Data's bounded diagnostic text here so a legal maximum-size summary
      // cannot exceed the observation field cap through an internal prefix.
      .health_ = health.summary,
  };

  std::optional<MetaCandidateProgressObs> candidate_observation;
  if (const auto* replica =
          std::get_if<control::ReplicaCandidate>(&role_information)) {
    const control::CandidateProgress& candidate = replica->progress;
    auto source_boot =
        ParseIdentity<20>(candidate.source_boot_id, "candidate source boot id");
    auto source_history = ParseIdentity<20>(candidate.source_history_id,
                                            "candidate source history id");
    if (!health.storage_ready ||
        (!health.population_ready && !candidate.recovered &&
         !candidate.operator_recovery) ||
        health.draining) {
      record_rejection("candidate",
                       absl::FailedPreconditionError(
                           "candidate heartbeat is not ready and healthy"));
    } else if (!source_boot.ok() && !candidate.operator_recovery) {
      record_rejection("candidate", source_boot.status());
    } else if (!source_history.ok() && !candidate.operator_recovery) {
      record_rejection("candidate", source_history.status());
    } else {
      MetaCandidateProgressObs progress{
          .node_id_ = std::string(node_id),
          .boot_incarnation_ = boot,
          .session_generation_ = generation,
          .group_id_ = candidate.group_id,
          .assignment_id_ = candidate.assignment_id,
          .group_term_ = candidate.group_term,
          .population_manifest_revision_ = candidate.manifest_revision,
          .population_manifest_digest_ = candidate.manifest_digest,
          .partition_replication_epoch_ = candidate.partition_replication_epoch,
          .replication_history_id_ = session_history,
          .source_group_term_ = candidate.source_group_term,
          .source_node_id_ = candidate.source_node_id,
          .source_assignment_id_ = candidate.source_assignment_id,
          .source_boot_incarnation_ = candidate.operator_recovery
                                          ? MetaBootIncarnation{}
                                          : *source_boot,
          .source_replication_history_id_ = candidate.operator_recovery
                                                ? MetaReplicationHistoryId{}
                                                : *source_history,
          .applied_next_lsns_ = candidate.applied_next_lsns,

          .storage_ready_ = health.storage_ready,
          .population_ready_ = health.population_ready,
          .draining_ = health.draining,
          .recovered_ = candidate.recovered,
          .operator_recovery_ = candidate.operator_recovery,
      };
      candidate_observation = std::move(progress);
    }
  }

  std::optional<MetaFailoverObservationObs> failover_observation_value;
  if (failover_observation.has_value()) {
    std::visit(
        [&](const auto& wire) {
          using Wire = std::decay_t<decltype(wire)>;
          const auto committed =
              facts.FailoverTransitionById(wire.transition_id);
          if (!committed.has_value()) {
            record_rejection("failover", absl::FailedPreconditionError(
                                             "transition is not committed"));
            return;
          }
          if constexpr (std::is_same_v<Wire, control::SourcePaused>) {
            auto source_boot =
                ParseIdentity<20>(wire.source_boot_id, "paused source boot id");
            auto source_history = ParseIdentity<20>(wire.source_history_id,
                                                    "paused source history id");
            if (!source_boot.ok()) {
              record_rejection("failover", source_boot.status());
              return;
            }
            if (!source_history.ok()) {
              record_rejection("failover", source_history.status());
              return;
            }
            failover_observation_value = MetaFailoverObservationObs{
                .payload_ = MetaSourcePausedObs{
                    .group_id_ = committed->group_id_,
                    .transition_id_ = wire.transition_id,
                    .source_node_id_ = wire.source_node_id,
                    .source_assignment_id_ = wire.source_assignment_id,
                    .source_boot_id_ = *source_boot,
                    .source_history_id_ = *source_history,
                    .source_group_term_ = wire.source_group_term,
                    .stable_next_lsns_ = wire.stable_next_lsns,
                }};
          } else {
            auto candidate_boot = ParseIdentity<20>(
                wire.candidate_boot_id, "failover candidate boot id");
            if (!candidate_boot.ok()) {
              record_rejection("failover", candidate_boot.status());
              return;
            }
            if constexpr (std::is_same_v<Wire, control::CandidatePrepared>) {
              failover_observation_value = MetaFailoverObservationObs{
                  .payload_ = MetaCandidatePreparedObs{
                      .group_id_ = committed->group_id_,
                      .transition_id_ = wire.transition_id,
                      .action_id_ = wire.action_id,
                      .candidate_node_id_ = wire.candidate_node_id,
                      .candidate_assignment_id_ = wire.candidate_assignment_id,
                      .candidate_boot_id_ = *candidate_boot,
                      .prepared_context_id_ = wire.prepared_context_id,
                  }};
            } else if constexpr (std::is_same_v<
                                     Wire,
                                     control::CandidateRecoveryComplete>) {
              failover_observation_value = MetaFailoverObservationObs{
                  .payload_ = MetaCandidateRecoveryCompleteObs{
                      .group_id_ = committed->group_id_,
                      .transition_id_ = wire.transition_id,
                      .action_id_ = wire.action_id,
                      .candidate_node_id_ = wire.candidate_node_id,
                      .candidate_assignment_id_ = wire.candidate_assignment_id,
                      .candidate_boot_id_ = *candidate_boot,
                      .recovery_deadline_unix_ms_ =
                          wire.recovery_deadline_unix_ms,
                      .applied_next_lsns_ = wire.applied_next_lsns,
                      .completion_reason_ = wire.completion_reason,
                  }};
            } else {
              failover_observation_value = MetaFailoverObservationObs{
                  .payload_ = MetaActionFailedObs{
                      .group_id_ = committed->group_id_,
                      .transition_id_ = wire.transition_id,
                      .action_id_ = wire.action_id,
                      .candidate_node_id_ = wire.candidate_node_id,
                      .candidate_assignment_id_ = wire.candidate_assignment_id,
                      .candidate_boot_id_ = *candidate_boot,
                      .population_manifest_revision_ =
                          wire.population_manifest_revision,
                      .population_manifest_digest_ =
                          wire.population_manifest_digest,
                      .partition_replication_epoch_ =
                          wire.partition_replication_epoch,
                      .failure_class_ = wire.failure_class,
                      .failure_detail_ = wire.failure_detail,
                  }};
            }
          }
        },
        *failover_observation);
  }
  MetaObservationStore::HeartbeatReplaceResult replaced =
      observations.ReplaceHeartbeat(
          identity, std::move(health_observation),
          std::move(candidate_observation),
          std::move(failover_observation_value), std::move(failover_projection),
          std::move(owner_projection), heartbeat_sequence,
          confirmed_grant_sequence, facts, now_unix_ms, now_steady_ms,
          std::holds_alternative<control::AuthorityLeaseRequest>(
              role_information));
  if (!replaced.boot_status_.ok()) {
    record_rejection("boot", replaced.boot_status_);
  }
  if (!replaced.health_status_.ok()) {
    record_rejection("health", replaced.health_status_);
  }
  if (!replaced.candidate_status_.ok()) {
    record_rejection("candidate", replaced.candidate_status_);
  }
  if (!replaced.failover_status_.ok()) {
    record_rejection("failover", replaced.failover_status_);
  }
  return result;
}

std::vector<control::WireAuthorityAnchor> UnfencedSupersededAuthorities(
    const control::FullDesiredState& installed,
    const control::FullDesiredState* latest, std::string_view node_id,
    std::span<const control::WireAuthorityAnchor> already_fenced) {
  std::vector<control::WireAuthorityAnchor> result;
  for (const control::WireDesiredGroup& group : installed.groups) {
    if (!group.grant_active || !group.owner_node_id.has_value() ||
        !group.owner_assignment_id.has_value() ||
        *group.owner_node_id != node_id ||
        (latest != nullptr && SameLiveAuthority(group, *latest, node_id))) {
      continue;
    }
    const control::WireAuthorityAnchor anchor = GroupAnchor(group);
    if (std::find(already_fenced.begin(), already_fenced.end(), anchor) ==
        already_fenced.end()) {
      result.push_back(anchor);
    }
  }
  return result;
}

MetaReplacementDisposition EvaluateReplacementDisposition(
    const control::FullDesiredState& replacement,
    const control::FullDesiredState& latest) {
  return control::SameDesiredState(replacement, latest)
             ? MetaReplacementDisposition::kContinue
             : MetaReplacementDisposition::kAbortSuperseded;
}

namespace {
MetaReplacementDisposition EvaluateNodeReplacement(
    const control::FullDesiredState& installed,
    const control::FullDesiredState& latest, std::string_view node_id) {
  const auto before = control::SelectNodeControlState(installed, node_id);
  auto after = control::SelectNodeControlState(latest, node_id);
  const auto changes = control::DiffNodeControlState(before, after);
  return changes.routing || changes.local || changes.directory || changes.tasks
             ? MetaReplacementDisposition::kAbortSuperseded
             : MetaReplacementDisposition::kContinue;
}
}  // namespace

absl::StatusOr<std::vector<control::WireMetaEndpoint>>
BuildCommittedMetaDirectory(const MetaCommittedView& view) {
  std::vector<control::WireMetaEndpoint> directory;
  for (const MetaMemberRecord& member : view.identity().MetaMembers()) {
    if (member.retired_) continue;
    auto endpoint = ParseMetaEndpoint(member);
    if (!endpoint.ok()) return endpoint.status();
    directory.push_back(std::move(*endpoint));
  }
  std::sort(directory.begin(), directory.end(),
            [](const auto& left, const auto& right) {
              return left.server_id < right.server_id;
            });
  std::set<std::pair<std::string, std::uint16_t>> endpoints;
  for (std::size_t i = 1; i < directory.size(); ++i) {
    if (directory[i - 1].server_id == directory[i].server_id) {
      return absl::FailedPreconditionError(
          "committed Meta directory contains a duplicate server id");
    }
  }
  for (const control::WireMetaEndpoint& endpoint : directory) {
    if (!endpoints.emplace(endpoint.host, endpoint.port).second) {
      return absl::FailedPreconditionError(
          "committed Meta directory contains a duplicate endpoint");
    }
  }
  // ServerHello is deliberately non-fragmentable: a client must authenticate
  // one complete committed redirect directory before following any member.
  // Probe the largest actual shape (leader id present) here so a leader never
  // announces acceptance and discovers only in the writer that its directory
  // cannot fit one frame.
  control::ServerHello probe{
      .disposition = control::ServerHelloDisposition::kAccepted,
      .negotiated_version = control::kProtocolVersion,
      .meta_server_id = 1,
      .raft_term = 0,
      .session_id = {},
      .session_generation = 0,
      .leader_id = 1,
      .directory = std::move(directory),
      .observation_ttl_ms = 1,
      .session_progress_timeout_ms = 1,
  };
  control::WireMessage probe_message(std::move(probe));
  auto encoded = control::EncodeMessage(probe_message);
  if (!encoded.ok()) return encoded.status();
  if (encoded->size() > control::kMaxFramePayloadBytes) {
    return absl::ResourceExhaustedError(
        "committed Meta directory cannot fit in ServerHello");
  }
  return std::move(std::get<control::ServerHello>(probe_message).directory);
}

control::LeaseDecision EvaluateLeaseChallenge(
    const std::optional<control::LeaseChallenge>& challenge,
    const control::HeartbeatHealth& health,
    const MetaLeaseEvaluation& evaluation) {
  if (!challenge.has_value()) return control::NoChallenge{};
  const std::uint64_t current_index =
      evaluation.desired_ == nullptr ? 0
                                     : evaluation.desired_->control_revision;
  if (evaluation.desired_ == nullptr ||
      challenge->control_revision != evaluation.applied_projection_index_ ||
      challenge->control_revision != current_index) {
    return control::LeaseStateOutOfDate{challenge->nonce, current_index};
  }
  if (!evaluation.leader_valid_) {
    return control::LeaseDenied{challenge->nonce,
                                control::LeaseDenialReason::kNotLeader,
                                current_index};
  }

  const auto group = std::find_if(
      evaluation.desired_->groups.begin(), evaluation.desired_->groups.end(),
      [&](const control::WireDesiredGroup& candidate) {
        return candidate.group_id == challenge->group_id;
      });
  if (group == evaluation.desired_->groups.end() ||
      !group->owner_node_id.has_value() ||
      !group->owner_assignment_id.has_value() ||
      *group->owner_node_id != evaluation.node_id_ ||
      *group->owner_assignment_id != challenge->assignment_id ||
      group->group_term != challenge->group_term) {
    return control::LeaseDenied{challenge->nonce,
                                control::LeaseDenialReason::kAuthorityMismatch,
                                current_index};
  }
  if (!group->grant_active ||
      evaluation.desired_->authority_lease_duration_ms == 0) {
    return control::LeaseDenied{challenge->nonce,
                                control::LeaseDenialReason::kGrantInactive,
                                current_index};
  }
  if (!health.storage_ready || !health.population_ready || health.draining) {
    return control::LeaseDenied{challenge->nonce,
                                control::LeaseDenialReason::kNodeNotReady,
                                current_index};
  }
  const std::uint32_t duration =
      std::min(evaluation.desired_->authority_lease_duration_ms,
               evaluation.leadership_validity_ms_);
  if (duration == 0) {
    return control::LeaseDenied{challenge->nonce,
                                control::LeaseDenialReason::kNotLeader,
                                current_index};
  }
  return control::LeaseGranted{
      .nonce = challenge->nonce,
      .leader_id = evaluation.server_id_,
      .raft_term = evaluation.raft_term_,
      .leadership_generation = evaluation.leadership_generation_,
      .data_boot_id = evaluation.boot_id_,
      .control_revision = current_index,
      .group_id = challenge->group_id,
      .assignment_id = challenge->assignment_id,
      .group_term = challenge->group_term,
      .granted_duration_ms = duration,
  };
}

control::LeaseDecision MetaLeaseHandoffGuard::Enforce(
    control::LeaseDecision decision, std::string_view node_id,
    std::int64_t now_lease_clock_ms) {
  const auto* grant = std::get_if<control::LeaseGranted>(&decision);
  if (grant == nullptr) return decision;

  const control::WireAuthorityAnchor authority{
      .group_id = grant->group_id,
      .assignment_id = grant->assignment_id,
      .group_term = grant->group_term,
  };
  auto entry = std::find_if(
      entries_.begin(), entries_.end(), [&](const Entry& candidate) {
        return candidate.authority_.group_id == authority.group_id;
      });
  const bool same_identity =
      entry != entries_.end() && entry->authority_ == authority &&
      entry->node_id_ == node_id &&
      entry->data_boot_id_ == grant->data_boot_id &&
      entry->leadership_generation_ == grant->leadership_generation;
  if (!same_identity) {
    if (entry == entries_.end()) {
      // Committed projections contain at most kMaxProjectedGroups. Evicting
      // only loses elapsed-time evidence and therefore causes another safe
      // wait; it can never make a new authority eligible early.
      if (entries_.size() >= control::kMaxProjectedGroups) {
        entries_.erase(entries_.begin());
      }
      entries_.push_back(Entry{});
      entry = std::prev(entries_.end());
    }
    entry->authority_ = authority;
    entry->node_id_ = node_id;
    entry->data_boot_id_ = grant->data_boot_id;
    entry->leadership_generation_ = grant->leadership_generation;
    const std::int64_t max_time = std::numeric_limits<std::int64_t>::max();
    entry->eligible_after_ms_ =
        quarantine_ms_ > static_cast<std::uint64_t>(max_time) ||
                now_lease_clock_ms >
                    max_time - static_cast<std::int64_t>(quarantine_ms_)
            ? max_time
            : now_lease_clock_ms + static_cast<std::int64_t>(quarantine_ms_);
  }
  if (now_lease_clock_ms < entry->eligible_after_ms_) {
    return control::LeaseDenied{
        .nonce = grant->nonce,
        .reason = control::LeaseDenialReason::kAuthorityHandoffPending,
        .current_control_revision = grant->control_revision,
    };
  }
  return decision;
}

control::LeaseDecision MetaLeaseHandoffGuard::Enforce(
    control::LeaseDecision decision,
    const std::optional<control::LeaseChallenge>& challenge,
    const MetaLeaseEvaluation& evaluation, std::int64_t now_lease_clock_ms) {
  const auto* denied = std::get_if<control::LeaseDenied>(&decision);
  if (denied == nullptr ||
      denied->reason != control::LeaseDenialReason::kNodeNotReady) {
    return Enforce(std::move(decision), evaluation.node_id_,
                   now_lease_clock_ms);
  }

  const control::HeartbeatHealth serviceable{
      .storage_ready = true,
      .population_ready = true,
      .draining = false,
      .active_groups = 0,
      .summary = {},
  };
  control::LeaseDecision candidate =
      EvaluateLeaseChallenge(challenge, serviceable, evaluation);
  control::LeaseDecision guarded =
      Enforce(std::move(candidate), evaluation.node_id_, now_lease_clock_ms);
  const auto* handoff = std::get_if<control::LeaseDenied>(&guarded);
  if (handoff != nullptr &&
      handoff->reason == control::LeaseDenialReason::kAuthorityHandoffPending) {
    return guarded;
  }
  return decision;
}

void MetaLeaderRuntimeGuard::Reset(std::int64_t now_suspend_clock_ms,
                                   std::int64_t now_active_clock_ms) noexcept {
  baseline_suspend_clock_ms_ = now_suspend_clock_ms;
  baseline_active_clock_ms_ = now_active_clock_ms;
  eligible_active_clock_ms_ = now_active_clock_ms;
  initialized_ = true;
  quarantined_ = false;
}

MetaLeaderRuntimeDisposition MetaLeaderRuntimeGuard::Observe(
    std::int64_t now_suspend_clock_ms,
    std::int64_t now_active_clock_ms) noexcept {
  if (!initialized_) {
    Reset(now_suspend_clock_ms, now_active_clock_ms);
    return MetaLeaderRuntimeDisposition::kEligible;
  }

  const bool clock_regressed =
      now_suspend_clock_ms < baseline_suspend_clock_ms_ ||
      now_active_clock_ms < baseline_active_clock_ms_;
  const std::uint64_t suspend_elapsed =
      clock_regressed ? 0
                      : static_cast<std::uint64_t>(now_suspend_clock_ms -
                                                   baseline_suspend_clock_ms_);
  const std::uint64_t active_elapsed =
      clock_regressed ? 0
                      : static_cast<std::uint64_t>(now_active_clock_ms -
                                                   baseline_active_clock_ms_);
  const bool suspend_gap =
      clock_regressed ||
      (suspend_elapsed >= active_elapsed &&
       suspend_elapsed - active_elapsed >= leadership_validity_ms_);
  if (suspend_gap) {
    const std::int64_t max_time = std::numeric_limits<std::int64_t>::max();
    eligible_active_clock_ms_ =
        leadership_validity_ms_ > static_cast<std::uint64_t>(max_time) ||
                now_active_clock_ms > max_time - static_cast<std::int64_t>(
                                                     leadership_validity_ms_)
            ? max_time
            : now_active_clock_ms +
                  static_cast<std::int64_t>(leadership_validity_ms_);
    baseline_suspend_clock_ms_ = now_suspend_clock_ms;
    baseline_active_clock_ms_ = now_active_clock_ms;
    quarantined_ = true;
    return MetaLeaderRuntimeDisposition::kQuarantineStarted;
  }

  if (quarantined_ && now_active_clock_ms < eligible_active_clock_ms_) {
    return MetaLeaderRuntimeDisposition::kQuarantined;
  }
  if (quarantined_) {
    // Active runtime has now supplied the liveness interval that did not pass
    // during suspend. Start measuring any later accumulated suspend from this
    // proven cut rather than repeatedly charging the completed quarantine.
    baseline_suspend_clock_ms_ = now_suspend_clock_ms;
    baseline_active_clock_ms_ = now_active_clock_ms;
    quarantined_ = false;
  }
  return MetaLeaderRuntimeDisposition::kEligible;
}

struct MetaDataControlServer::Core {
  bycorf::ForeignExecutor foreign_executor_;
  nuraft::ptr<nuraft::raft_server> server_;
  // Non-owning. MetaCoordinator owns this reconciler and therefore outlives
  // every Core access; keeping this edge non-owning avoids a cycle.
  MetaCoordinator* coordinator_ = nullptr;
  std::unique_ptr<detail::MetaCommittedViewCache> committed_view_cache_;
  std::shared_ptr<MetaObservationStore> observations_;
  MetaDataControlServerOptions options_;
  std::shared_ptr<bycorf::TlsContext> tls_context_;
  std::unique_ptr<MetaLeaseHandoffGuard> lease_handoff_guard_;
  std::unique_ptr<MetaLeaderRuntimeGuard> leader_runtime_guard_;
  std::unique_ptr<detail::PendingHandshakeLimiter> pending_handshakes_;
  std::unique_ptr<detail::RetainedProjectionLimiter> projection_limiter_;

  mutable std::mutex status_mu_;
  absl::Status status_ =
      absl::UnavailableError("data-control listener bind has not run");

  std::atomic<std::uint64_t> accepted_sessions_{0};
  std::atomic<std::uint64_t> live_session_tasks_{0};
  std::atomic<std::uint64_t> live_authority_session_tasks_{0};
  std::atomic<std::uint64_t> live_leader_tasks_{0};
  std::atomic<std::uint64_t> active_sessions_{0};
  std::atomic<std::uint64_t> redirected_sessions_{0};
  std::atomic<std::uint64_t> rejected_sessions_{0};
  std::atomic<std::uint64_t> protocol_errors_{0};
  std::atomic<std::uint64_t> full_states_sent_{0};
  std::atomic<std::uint64_t> lease_grants_{0};
  std::atomic<std::uint64_t> lease_denials_{0};
  std::atomic<std::uint64_t> observations_accepted_{0};
  std::atomic<std::uint64_t> observations_rejected_{0};
  std::atomic<std::uint64_t> directive_results_committed_{0};
  // Cross-thread completion latch for the public lifecycle methods. Once
  // true, Shutdown has already drained every worker-owned task, so a later
  // reconciler cancellation or destructor need not touch a stopped Runtime.
  std::atomic<bool> shutdown_complete_{false};

  // Worker-thread only below.
  bycorf::Worker* worker_ = nullptr;
  bycorf::TcpListener listener_;
  int shutdown_accept_wake_fd_ = -1;
  bool listening_ = false;
  bool shutdown_ = false;
  bool accept_loop_running_ = false;
  bool leader_active_ = false;
  bool leader_ready_for_data_ = false;
  std::uint64_t leadership_generation_ = 0;
  MetaLeaderContext* leader_context_ = nullptr;
  std::shared_ptr<SessionCommitSignal> leader_commit_signal_;
  std::shared_ptr<MetaCommitSubscription> leader_commit_subscription_;
  std::vector<bycorf::Connection*> sessions_;
  std::map<bycorf::Connection*, std::uint64_t> authority_session_generation_;
  std::map<std::uint64_t, std::size_t> authority_sessions_by_generation_;
  std::map<std::uint64_t, std::size_t> leader_tasks_by_generation_;
  std::map<std::uint64_t, std::vector<std::shared_ptr<std::promise<void>>>>
      generation_drain_waiters_;
  std::vector<std::shared_ptr<std::promise<void>>> shutdown_drain_waiters_;
  detail::BoundNodeSessionRegistry bound_node_sessions_;
  std::map<std::string, std::uint64_t> next_session_generation_;
};

std::shared_ptr<MetaDataControlServer>
MetaDataControlServer::LifecycleHarnessForTest(
    bycorf::ForeignExecutor foreign_executor, bool shutdown_complete) {
  auto core = std::make_shared<Core>();
  core->foreign_executor_ = foreign_executor;
  core->options_.runtime_status_ =
      std::make_shared<MetaDataControlRuntimeStatus>();
  core->shutdown_complete_.store(shutdown_complete, std::memory_order_release);
  return std::shared_ptr<MetaDataControlServer>(
      new MetaDataControlServer(std::move(core)));
}

// Worker-local coordination shared by the established session's sole reader
// and commit publisher. These tasks may produce writes concurrently, but
// SessionIo funnels every frame through one
// ControlSessionWriter. Only SessionLoop reads and resolves the single pending
// publisher acknowledgement.
struct LiveSessionState {
  std::shared_ptr<MetaDataControlServer::Core> core_;
  bycorf::Worker* worker_ = nullptr;
  bycorf::Connection* connection_ = nullptr;
  SessionIo* io_ = nullptr;
  std::shared_ptr<MetaCommitSubscription> commit_subscription_;
  std::shared_ptr<SessionCommitSignal> commit_signal_;
  std::string node_id_;
  std::string boot_id_;
  control::WireId128 session_id_{};
  MetaReplicationHistoryId replication_history_id_{};
  std::uint32_t replication_flow_count_ = 0;
  std::uint64_t session_generation_ = 0;
  std::uint64_t leadership_generation_ = 0;

  std::shared_ptr<const NodeControlBatch> installed_;
  // Highest synchronously published command commit whose semantic node
  // projection has been compared with installed_. Heartbeats and update
  // boundaries deny authority if the coordinator high-water advances first,
  // even while the subscription callback is still queued cross-thread.
  std::uint64_t validated_committed_high_water_ = 0;
  control::NodeControlState selected_;
  std::vector<control::WireAuthorityAnchor> fenced_authorities_;

  std::optional<control::WireAuthorityAnchor> expected_fence_;
  std::optional<control::FullStateApplied> expected_applied_;
  std::unique_ptr<control::ControlDeadlineWatchdog> fence_ack_deadline_;
  std::unique_ptr<control::ControlDeadlineWatchdog> applied_ack_deadline_;
  std::unique_ptr<control::ControlDeadlineWatchdog> inbound_transfer_deadline_;
  bool fence_received_ = false;
  bool applied_received_ = false;
  detail::MetaPublisherAdoptionGate publisher_adoption_gate_;
  bycorf::AsyncNotification response_changed_;

  std::optional<absl::Status> terminal_error_;
  std::size_t active_tasks_ = 0;
  bool publisher_running_ = false;
  bool projection_superseded_ = false;
  bool closing_ = false;
  bycorf::AsyncNotification tasks_changed_;
};

namespace {

bool StillLeader(const MetaDataControlServer::Core& core,
                 std::uint64_t generation) {
  return core.leader_active_ && core.leadership_generation_ == generation &&
         core.leader_context_ != nullptr && core.server_->is_leader() &&
         core.server_->is_leader_sm_fully_caught_up();
}

absl::StatusOr<std::shared_ptr<const MetaCommittedView>> CommittedViewAtLeast(
    MetaDataControlServer::Core& core, std::uint64_t minimum_index) {
  if (core.committed_view_cache_ == nullptr) {
    return absl::FailedPreconditionError(
        "data-control committed-view cache is not initialized");
  }
  return core.committed_view_cache_->Get(minimum_index);
}

bool ProjectionCurrent(const LiveSessionState& state) {
  return !state.projection_superseded_ &&
         !CommitPending(*state.commit_signal_,
                        state.validated_committed_high_water_) &&
         state.validated_committed_high_water_ >=
             state.core_->coordinator_->CommittedHighWater();
}

void FulfillWaiters(std::vector<std::shared_ptr<std::promise<void>>> waiters) {
  for (const auto& waiter : waiters) waiter->set_value();
}

void NotifyGenerationDrained(MetaDataControlServer::Core& core,
                             std::uint64_t generation) {
  if (core.authority_sessions_by_generation_.contains(generation) ||
      core.leader_tasks_by_generation_.contains(generation)) {
    return;
  }
  const auto found = core.generation_drain_waiters_.find(generation);
  if (found == core.generation_drain_waiters_.end()) return;
  auto waiters = std::move(found->second);
  core.generation_drain_waiters_.erase(found);
  FulfillWaiters(std::move(waiters));
}

void NotifyShutdownDrained(MetaDataControlServer::Core& core) {
  if (!core.shutdown_ || core.accept_loop_running_ || !core.sessions_.empty() ||
      !core.leader_tasks_by_generation_.empty() ||
      core.shutdown_drain_waiters_.empty()) {
    return;
  }
  auto waiters = std::move(core.shutdown_drain_waiters_);
  core.shutdown_drain_waiters_.clear();
  FulfillWaiters(std::move(waiters));
}

absl::Status BindAuthoritySession(MetaDataControlServer::Core& core,
                                  bycorf::Connection* connection,
                                  std::uint64_t generation) {
  if (!core.authority_session_generation_.emplace(connection, generation)
           .second) {
    return absl::AlreadyExistsError(
        "data-control session already has a leadership generation");
  }
  ++core.authority_sessions_by_generation_[generation];
  core.live_authority_session_tasks_.fetch_add(1, std::memory_order_relaxed);
  return absl::OkStatus();
}

void StartLeaderTask(MetaDataControlServer::Core& core,
                     std::uint64_t generation) {
  ++core.leader_tasks_by_generation_[generation];
  core.live_leader_tasks_.fetch_add(1, std::memory_order_relaxed);
}

void FinishLeaderTask(MetaDataControlServer::Core& core,
                      std::uint64_t generation) {
  const auto count = core.leader_tasks_by_generation_.find(generation);
  if (count == core.leader_tasks_by_generation_.end()) return;
  if (--count->second == 0) core.leader_tasks_by_generation_.erase(count);
  core.live_leader_tasks_.fetch_sub(1, std::memory_order_relaxed);
  NotifyGenerationDrained(core, generation);
  NotifyShutdownDrained(core);
}

void UnregisterSession(MetaDataControlServer::Core& core,
                       bycorf::Connection* connection) {
  const auto session =
      std::find(core.sessions_.begin(), core.sessions_.end(), connection);
  if (session != core.sessions_.end()) {
    *session = core.sessions_.back();
    core.sessions_.pop_back();
    core.live_session_tasks_.fetch_sub(1, std::memory_order_relaxed);
  }
}

void RemoveSessionBindings(MetaDataControlServer::Core& core,
                           bycorf::Connection* connection,
                           std::string_view node_id,
                           const control::WireId128* session_id) {
  if (const auto authority =
          core.authority_session_generation_.find(connection);
      authority != core.authority_session_generation_.end()) {
    const std::uint64_t generation = authority->second;
    core.authority_session_generation_.erase(authority);
    core.live_authority_session_tasks_.fetch_sub(1, std::memory_order_relaxed);
    const auto count = core.authority_sessions_by_generation_.find(generation);
    if (count != core.authority_sessions_by_generation_.end() &&
        --count->second == 0) {
      core.authority_sessions_by_generation_.erase(count);
      NotifyGenerationDrained(core, generation);
    }
  }
  if (!node_id.empty()) {
    core.bound_node_sessions_.Release(node_id, connection);
    // An old session can finish after its per-node binding has already moved
    // to a replacement. Remove only the incarnation owned by this coroutine;
    // otherwise late teardown could erase the replacement's current status.
    core.options_.runtime_status_->Remove(node_id, session_id);
  }
}

void CloseConnectionNow(bycorf::Worker& worker, bycorf::Connection* connection,
                        absl::Status status) {
  // shutdown(2) makes the transport revocation externally visible before the
  // reconciler acknowledges demotion. BeginClose then performs Bycorf's normal
  // in-flight operation drain and connection retirement.
  if (connection != nullptr && connection->file_.fd_ >= 0) {
    (void)::shutdown(connection->file_.fd_, SHUT_RDWR);
  }
  worker.BeginClose(connection, std::move(status),
                    bycorf::CloseMode::kLocalClose);
}

bool AuthoritySessionsAllowed(MetaDataControlServer::Core& core,
                              std::uint64_t generation) {
  if (!StillLeader(core, generation) || !core.leader_ready_for_data_ ||
      !core.server_->is_leader_alive()) {
    return core.options_.runtime_status_->SetLeaderAuthorityEligible(generation,
                                                                     false);
  }
  const MetaLeaderRuntimeDisposition runtime =
      core.leader_runtime_guard_->Observe(cluster::LeaseClockMillis(),
                                          ActiveClockMillis());
  if (runtime == MetaLeaderRuntimeDisposition::kQuarantineStarted) {
    spdlog::warn(
        "Meta data-control detected a suspend gap; quarantining authority "
        "for {} ms of active runtime",
        core.options_.leadership_validity_ms_);
    // NuRaft's cached live-leader flag may itself be stale after suspend.
    // Immediate resignation is synchronous for a multi-member cluster, so
    // this generation cannot become eligible merely because the Bycorf worker
    // runs before NuRaft's next heartbeat timer. NuRaft intentionally keeps a
    // sole member leader; the active-time guard safely covers that case.
    core.server_->yield_leadership(/*immediate_yield=*/true);
    if (core.worker_ != nullptr) {
      std::vector<bycorf::Connection*> sessions;
      for (const auto& [connection, session_generation] :
           core.authority_session_generation_) {
        if (session_generation == generation) sessions.push_back(connection);
      }
      for (bycorf::Connection* connection : sessions) {
        CloseConnectionNow(
            *core.worker_, connection,
            absl::UnavailableError(
                "Meta authority is quarantined after host suspend"));
      }
    }
  }
  const bool eligible = runtime == MetaLeaderRuntimeDisposition::kEligible;
  // Runtime status owns generation matching and fail-closed revision
  // saturation, so authorization must use the effective stored result rather
  // than the guard's requested value.
  return core.options_.runtime_status_->SetLeaderAuthorityEligible(generation,
                                                                   eligible);
}

struct BudgetedNodeControlBatch final : NodeControlBatch {
  BudgetedNodeControlBatch(NodeControlBatch batch,
                           detail::RetainedProjectionLimiter::Permit permit)
      : NodeControlBatch(std::move(batch)), permit_(std::move(permit)) {}

  BudgetedNodeControlBatch(BudgetedNodeControlBatch&&) noexcept = default;
  BudgetedNodeControlBatch& operator=(BudgetedNodeControlBatch&&) noexcept =
      default;

  detail::RetainedProjectionLimiter::Permit permit_;
};

absl::StatusOr<BudgetedNodeControlBatch> ProjectNodeBounded(
    MetaDataControlServer::Core& core, const MetaCommittedView& view,
    std::string_view node_id) {
  auto permit =
      core.projection_limiter_->TryAcquire(kProjectionBuildReservationBytes);
  if (!permit.has_value()) {
    return absl::ResourceExhaustedError(
        "Meta retained-projection byte budget cannot admit another build");
  }
  auto projected = MetaControlProjector::ProjectNode(view, node_id);
  if (!projected.ok()) return projected.status();
  if (absl::Status resolved = detail::ApplyLeadershipValidityLimit(
          *projected, core.options_.leadership_validity_ms_);
      !resolved.ok()) {
    return resolved;
  }
  if (absl::Status charged =
          permit->Resize(NodeControlBatchRetainedBytes(*projected));
      !charged.ok()) {
    return charged;
  }
  return BudgetedNodeControlBatch(std::move(*projected), std::move(*permit));
}

std::shared_ptr<const NodeControlBatch> RetainProjection(
    BudgetedNodeControlBatch projection) {
  auto owner =
      std::make_shared<BudgetedNodeControlBatch>(std::move(projection));
  const NodeControlBatch* batch = owner.get();
  return std::shared_ptr<const NodeControlBatch>(std::move(owner), batch);
}

void FailLiveSession(const std::shared_ptr<LiveSessionState>& state,
                     absl::Status status) {
  if (status.ok()) return;
  if (!state->terminal_error_.has_value()) {
    state->terminal_error_ = status;
  }
  if (!state->closing_) {
    state->closing_ = true;
    state->commit_signal_->changed_.NotifyAll(*state->worker_);
    state->response_changed_.NotifyAll(*state->worker_);
    CloseConnectionNow(*state->worker_, state->connection_, std::move(status));
  }
}

void FinishLiveSessionTask(const std::shared_ptr<LiveSessionState>& state,
                           bool* running) {
  *running = false;
  if (state->active_tasks_ != 0) --state->active_tasks_;
  state->tasks_changed_.NotifyAll(*state->worker_);
}

absl::StatusOr<MetaMemberIdentity> LocalConfiguredIdentity(
    const MetaDataControlServer::Core& core,
    const nuraft::ptr<nuraft::cluster_config>& config) {
  if (config == nullptr) {
    return absl::FailedPreconditionError(
        "NuRaft has no committed membership configuration");
  }
  for (const nuraft::ptr<nuraft::srv_config>& member : config->get_servers()) {
    if (member == nullptr ||
        member->get_id() != static_cast<int>(core.options_.server_id_)) {
      continue;
    }
    auto identity = MetaMemberIdentity::DecodeAux(member->get_aux());
    if (!identity.ok()) return identity.status();
    if (identity->server_id_ != member->get_id()) {
      return absl::FailedPreconditionError(
          "local NuRaft member aux identity has a mismatched server id");
    }
    return *identity;
  }
  return absl::FailedPreconditionError(
      "local server is absent from NuRaft membership");
}

absl::Status ValidateCommittedConfigBindings(
    const nuraft::ptr<nuraft::cluster_config>& config,
    const MetaCommittedView& view) {
  if (config == nullptr) {
    return absl::FailedPreconditionError(
        "NuRaft has no committed membership configuration");
  }
  for (const nuraft::ptr<nuraft::srv_config>& member : config->get_servers()) {
    if (member == nullptr || member->get_id() <= 0) {
      return absl::FailedPreconditionError(
          "NuRaft membership contains an invalid member");
    }
    auto identity = MetaMemberIdentity::DecodeAux(member->get_aux());
    if (!identity.ok() || identity->server_id_ != member->get_id()) {
      return absl::FailedPreconditionError(
          "NuRaft member has invalid canonical aux identity");
    }
    const auto committed = view.identity().FindMetaMember(
        static_cast<std::uint32_t>(member->get_id()));
    if (!committed.has_value() || committed->retired_ ||
        committed->principal_ != identity->principal_ ||
        committed->data_control_endpoint_ != identity->data_control_endpoint_ ||
        committed->ctl_endpoint_ !=
            std::optional<std::string>(identity->ctl_endpoint_)) {
      return absl::FailedPreconditionError(
          "NuRaft member descriptor differs from its committed identity "
          "binding");
    }
    if (!ParseMetaEndpoint(*committed).ok()) {
      return absl::FailedPreconditionError(
          "NuRaft member lacks a usable committed data-control endpoint");
    }
  }
  return absl::OkStatus();
}

bool ActiveClusterCreateDeclaresNode(const MetaCommittedView& view,
                                     std::string_view node_id) {
  const auto& lifecycle = view.topology().ClusterLifecycle();
  if (lifecycle.state_ != MetaClusterLifecycle::kCreating) return false;
  const auto operation =
      view.operation().FindOperation(lifecycle.root_operation_id_);
  if (!operation.has_value() ||
      operation->kind_ != kMetaClusterCreateOperationKind ||
      operation->lifecycle_ == MetaOperationLifecycle::kCompleted ||
      operation->lifecycle_ == MetaOperationLifecycle::kAborted) {
    return false;
  }
  MetaOperationId intent_root{};
  auto manifest = DecodeClusterCreateRequest(operation->intent_, &intent_root);
  return manifest.ok() && intent_root == lifecycle.root_operation_id_ &&
         std::any_of(
             manifest->data_nodes_.begin(), manifest->data_nodes_.end(),
             [&](const auto& node) { return node.node_id_ == node_id; });
}

bycorf::Task<absl::Status> ReconcileLocalMetaMember(
    std::shared_ptr<MetaDataControlServer::Core> core,
    std::uint64_t generation) {
  struct CompletionGuard {
    std::shared_ptr<MetaDataControlServer::Core> core_;
    std::uint64_t generation_;
    ~CompletionGuard() { FinishLeaderTask(*core_, generation_); }
  } completion{core, generation};

  while (StillLeader(*core, generation)) {
    const nuraft::ptr<nuraft::cluster_config> config =
        core->server_->get_config();
    auto local_identity = LocalConfiguredIdentity(*core, config);
    absl::Status status = local_identity.status();
    if (local_identity.ok()) {
      // Membership reconciliation owns every BindMetaMember effect. The Data
      // publisher only opens after the complete config descriptor and
      // committed identity directory agree, including remote endpoints.
      auto view =
          CommittedViewAtLeast(*core, core->coordinator_->CommittedHighWater());
      status = view.ok() ? ValidateCommittedConfigBindings(config, **view)
                         : view.status();
    }

    if (status.ok()) {
      if (StillLeader(*core, generation)) {
        // Membership validity opens the Data listener, but this leader-scoped
        // task deliberately remains alive. An absent Owner produces no session
        // traffic, so Hello/heartbeat checks alone cannot keep the automatic
        // failover detector's authority bracket current across quorum loss or
        // host suspend.
        core->leader_ready_for_data_ = true;
        (void)AuthoritySessionsAllowed(*core, generation);
      }
    } else {
      // A config/identity disagreement after an earlier valid cut revokes the
      // same generation immediately. Session-side probes also consult
      // leader_ready_for_data_, so they cannot race this failure by restoring
      // eligibility from Raft liveness alone.
      core->leader_ready_for_data_ = false;
      core->options_.runtime_status_->SetLeaderAuthorityEligible(generation,
                                                                 false);
      spdlog::warn("data-control leader membership reconciliation: {}",
                   status.message());
    }
    const absl::Status waited =
        co_await bycorf::SleepFor(*core->worker_, 100ms);
    if (!waited.ok()) co_return waited;
  }
  co_return absl::CancelledError(
      "Meta leadership changed during membership reconciliation");
}

absl::Status ValidateHello(const control::ClientHello& hello) {
  if (hello.minimum_version > control::kProtocolVersion ||
      hello.maximum_version < control::kProtocolVersion ||
      hello.minimum_version > hello.maximum_version) {
    return absl::InvalidArgumentError("no supported control protocol version");
  }
  if (!control::IsCanonicalIdentity160(hello.node_id) ||
      !control::IsCanonicalIdentity160(hello.boot_id) ||
      !control::IsCanonicalIdentity160(hello.replication_history_id)) {
    return absl::InvalidArgumentError(
        "ClientHello identities must be canonical 160-bit lowercase hex");
  }
  return absl::OkStatus();
}

control::ServerHello BuildServerHello(
    const MetaDataControlServer::Core& core,
    std::vector<control::WireMetaEndpoint> directory, bool accepted,
    const control::WireId128& session_id = {},
    std::uint64_t session_generation = 0) {
  const std::int32_t leader = core.server_->get_leader();
  std::optional<std::uint32_t> leader_id;
  if (leader > 0 &&
      std::any_of(directory.begin(), directory.end(), [&](const auto& member) {
        return member.server_id == static_cast<std::uint32_t>(leader);
      })) {
    leader_id = static_cast<std::uint32_t>(leader);
  }
  control::ServerHelloDisposition disposition;
  if (accepted) {
    disposition = control::ServerHelloDisposition::kAccepted;
  } else {
    disposition = leader_id.has_value()
                      ? control::ServerHelloDisposition::kNotLeader
                      : control::ServerHelloDisposition::kLeaderUnknown;
  }
  return control::ServerHello{
      .disposition = disposition,
      .negotiated_version = control::kProtocolVersion,
      .meta_server_id = core.options_.server_id_,
      .raft_term = core.server_->get_term(),
      .session_id = session_id,
      .session_generation = session_generation,
      .leader_id = leader_id,
      .directory = std::move(directory),
      .observation_ttl_ms = core.options_.observation_ttl_ms_,
      .session_progress_timeout_ms = core.options_.session_progress_timeout_ms_,
  };
}

bycorf::Task<absl::Status> AwaitApplied(
    SessionIo& io, const NodeControlBatch& batch,
    std::deque<control::WireMessage>* deferred = nullptr,
    std::size_t max_deferred_messages = 0) {
  while (true) {
    auto incoming = co_await io.Read();
    if (!incoming.ok()) co_return incoming.status();
    if (const auto* applied =
            std::get_if<control::FullStateApplied>(&*incoming)) {
      if (!SameApplied(*applied, batch)) {
        co_return absl::FailedPreconditionError(
            "FullStateApplied does not identify the transferred projection");
      }
      co_return absl::OkStatus();
    }
    if (deferred == nullptr) {
      co_return absl::FailedPreconditionError(
          "message interrupted the initial FullStateApplied handshake");
    }
    // The client may finish a previously delivered directive before it
    // reaches the replacement transfer in the server-to-client stream.
    // Preserve those responses for the normal session dispatcher.
    if (absl::Status buffered =
            DeferInbound(deferred, std::move(*incoming), max_deferred_messages);
        !buffered.ok()) {
      co_return buffered;
    }
  }
}

bycorf::Task<absl::Status> SendFullState(
    const std::shared_ptr<MetaDataControlServer::Core>& core, SessionIo& io,
    std::shared_ptr<const NodeControlBatch> batch, std::string_view node_id,
    std::uint64_t generation) {
  if (batch == nullptr)
    co_return absl::InvalidArgumentError("bootstrap batch is empty");
  std::uint64_t validated_index = batch->full_state.control_revision;
  const auto validate = [&]() -> absl::Status {
    if (!AuthoritySessionsAllowed(*core, generation))
      return absl::CancelledError("Meta leadership ended during bootstrap");
    const auto high_water = core->coordinator_->CommittedHighWater();
    if (high_water <= validated_index) return absl::OkStatus();
    auto view = CommittedViewAtLeast(*core, high_water);
    if (!view.ok()) return view.status();
    auto latest = ProjectNodeBounded(*core, **view, node_id);
    if (!latest.ok()) return latest.status();
    if (EvaluateNodeReplacement(batch->full_state, latest->full_state,
                                node_id) !=
        MetaReplacementDisposition::kContinue)
      return absl::AbortedError(
          "bootstrap control was superseded before delivery");
    validated_index = (*view)->applied_index();
    return absl::OkStatus();
  };
  const std::string_view bytes = batch->encoded_full_state;
  if (auto status = validate(); !status.ok()) co_return status;
  if (bytes.size() <= control::kMaxFramePayloadBytes)
    co_return co_await io.Send(control::MessagePriority::kReliable,
                               control::WireMessage(batch->full_state));
  auto object_id = control::GenerateId128();
  if (!object_id.ok()) co_return object_id.status();
  if (auto status =
          co_await io.Send(control::MessagePriority::kReliable,
                           control::WireMessage(control::TransferStart{
                               .kind = control::TransferKind::kFullDesiredState,
                               .object_id = *object_id,
                               .total_length = bytes.size()}));
      !status.ok())
    co_return status;
  for (std::size_t offset = 0; offset < bytes.size();
       offset += control::kControlTransferChunkBytes) {
    if (auto status = validate(); !status.ok()) co_return status;
    if (auto status = co_await io.Send(
            control::MessagePriority::kBulk,
            control::WireMessage(control::TransferChunk{
                .object_id = *object_id,
                .offset = offset,
                .bytes = std::string(bytes.substr(
                    offset, control::kControlTransferChunkBytes))}));
        !status.ok())
      co_return status;
  }
  // Data executes tasks as soon as End permits installation. A superseded
  // bootstrap closes without End; no obsolete task becomes admissible.
  if (auto status = validate(); !status.ok()) co_return status;
  co_return co_await io.Send(
      control::MessagePriority::kReliable,
      control::WireMessage(control::TransferEnd{*object_id}));
}

bycorf::Task<absl::Status> AbortSupersededReplacement(
    SessionIo& io, const control::WireId128& object_id, bool transfer_active) {
  if (transfer_active) {
    if (absl::Status aborted = co_await io.Send(
            control::MessagePriority::kReliable,
            control::WireMessage(control::TransferAbort{
                .object_id = object_id,
                .reason =
                    control::TransferAbortReason::kFullDesiredStateSuperseded,
            }));
        !aborted.ok()) {
      co_return aborted;
    }
  }
  co_return absl::OkStatus();
}

bycorf::Task<absl::Status> ValidateBootstrapApplied(
    const std::shared_ptr<MetaDataControlServer::Core>& core, SessionIo& io,
    const NodeControlBatch& installed, std::string_view node_id,
    std::string_view boot_id, const control::WireId128& session_id,
    std::uint64_t leadership_generation, SessionCommitSignal& commit_signal,
    const MetaCommitSubscription& commit_subscription,
    std::uint64_t* validated_committed_high_water,
    std::deque<control::WireMessage>* deferred,
    std::size_t max_deferred_messages,
    std::vector<control::WireAuthorityAnchor>* fenced) {
  if (!AuthoritySessionsAllowed(*core, leadership_generation)) {
    co_return absl::CancelledError(
        "Meta authority is unavailable during bootstrap validation");
  }
  if (commit_signal.delivery_failed_.load(std::memory_order_acquire) ||
      commit_subscription.needs_resync()) {
    co_return absl::ResourceExhaustedError(
        "Meta commit subscription overflowed during bootstrap validation");
  }

  // Always take one fresh atomic view here instead of relying solely on a
  // mailbox dirty bit. Tasks were current at the final delivery boundary;
  // changes concurrent with delivery now fence or close the installed session
  // before it can receive new authority.
  const std::uint64_t high_water = core->coordinator_->CommittedHighWater();
  auto cached_view = CommittedViewAtLeast(*core, high_water);
  if (!cached_view.ok()) co_return cached_view.status();
  const MetaCommittedView& view = **cached_view;
  auto latest = ProjectNodeBounded(*core, view, node_id);
  const control::FullDesiredState* latest_state =
      latest.ok() ? &latest->full_state : nullptr;
  control::ControlDeadlineWatchdog fence_ack_deadline(
      *core->worker_, [&io] { io.FailDeadline("FenceAck"); });
  if (absl::Status status = co_await FenceSupersededAuthority(
          io, installed, latest_state, node_id, boot_id, session_id, deferred,
          max_deferred_messages, fenced, fence_ack_deadline,
          std::chrono::milliseconds(
              core->options_.session_progress_timeout_ms_));
      !status.ok()) {
    co_return status;
  }
  if (!latest.ok()) co_return latest.status();
  if (EvaluateNodeReplacement(installed.full_state, latest->full_state,
                              node_id) ==
      MetaReplacementDisposition::kAbortSuperseded) {
    co_return absl::AbortedError(
        "bootstrap state changed concurrently with installation");
  }
  *validated_committed_high_water = view.applied_index();
  co_return absl::OkStatus();
}

absl::StatusOr<bool> HandlePublisherResponse(
    const std::shared_ptr<LiveSessionState>& state,
    const control::WireMessage& message) {
  if (const auto* ack = std::get_if<control::FenceAck>(&message)) {
    if (!state->expected_fence_.has_value() || state->fence_received_) {
      return absl::FailedPreconditionError(
          "unexpected FenceAck in established data-control session");
    }
    if (ack->session_id != state->session_id_ ||
        ack->target_boot_id != state->boot_id_ ||
        ack->reject_through != *state->expected_fence_) {
      return absl::FailedPreconditionError(
          "FenceAck does not match the publisher's pending fence");
    }
    (void)state->fence_ack_deadline_->Disarm();
    state->fence_received_ = true;
    state->response_changed_.NotifyAll(*state->worker_);
    return true;
  }
  if (const auto* applied = std::get_if<control::FullStateApplied>(&message)) {
    if (!state->expected_applied_.has_value() || state->applied_received_) {
      return absl::FailedPreconditionError(
          "unexpected FullStateApplied in established data-control session");
    }
    if (*applied != *state->expected_applied_) {
      return absl::FailedPreconditionError(
          "FullStateApplied does not identify the publisher's projection");
    }
    (void)state->applied_ack_deadline_->Disarm();
    state->applied_received_ = true;
    state->publisher_adoption_gate_.ObserveAppliedReceipt();
    state->response_changed_.NotifyAll(*state->worker_);
    return true;
  }
  return false;
}

bycorf::Task<absl::Status> AwaitPublisherFence(
    const std::shared_ptr<LiveSessionState>& state) {
  if (!state->fence_received_) {
    const absl::Status armed =
        state->fence_ack_deadline_->Arm(std::chrono::milliseconds(
            state->core_->options_.session_progress_timeout_ms_));
    if (!armed.ok()) co_return armed;
  }
  while (!state->closing_ && !state->fence_received_) {
    co_await state->response_changed_.Wait();
  }
  (void)state->fence_ack_deadline_->Disarm();
  if (state->closing_) {
    co_return state->terminal_error_.value_or(
        absl::CancelledError("data-control session closed"));
  }
  state->fence_received_ = false;
  state->expected_fence_.reset();
  co_return absl::OkStatus();
}

bycorf::Task<absl::Status> AwaitPublisherApplied(
    const std::shared_ptr<LiveSessionState>& state) {
  while (!state->closing_ && !state->applied_received_) {
    co_await state->response_changed_.Wait();
  }
  (void)state->applied_ack_deadline_->Disarm();
  if (state->closing_) {
    co_return state->terminal_error_.value_or(
        absl::CancelledError("data-control session closed"));
  }
  state->applied_received_ = false;
  state->expected_applied_.reset();
  co_return absl::OkStatus();
}

void ClearPublisherFence(const std::shared_ptr<LiveSessionState>& state) {
  (void)state->fence_ack_deadline_->Disarm();
  state->fence_received_ = false;
  state->expected_fence_.reset();
}

void ClearPublisherApplied(const std::shared_ptr<LiveSessionState>& state) {
  (void)state->applied_ack_deadline_->Disarm();
  state->applied_received_ = false;
  state->expected_applied_.reset();
}

bycorf::Task<absl::Status> FenceSupersededAuthorityLive(
    const std::shared_ptr<LiveSessionState>& state,
    const NodeControlBatch& installed,
    const control::FullDesiredState* latest) {
  const auto superseded = UnfencedSupersededAuthorities(
      installed.full_state, latest, state->node_id_,
      state->fenced_authorities_);
  for (const control::WireAuthorityAnchor& anchor : superseded) {
    if (!AuthoritySessionsAllowed(*state->core_,
                                  state->leadership_generation_)) {
      co_return absl::CancelledError(
          "Meta authority is unavailable before Fence publication");
    }
    if (state->expected_fence_.has_value()) {
      co_return absl::InternalError(
          "publisher attempted overlapping FenceAck handshakes");
    }
    state->expected_fence_ = anchor;
    state->fence_received_ = false;
    const control::Fence fence{
        .session_id = state->session_id_,
        .target_boot_id = state->boot_id_,
        .basis =
            control::WireProjectionBasis{
                .control_revision = installed.full_state.control_revision,
            },
        .reject_through = anchor,
    };
    if (absl::Status sent = co_await state->io_->Send(
            control::MessagePriority::kAuthority, control::WireMessage(fence));
        !sent.ok()) {
      ClearPublisherFence(state);
      co_return sent;
    }
    if (absl::Status acknowledged = co_await AwaitPublisherFence(state);
        !acknowledged.ok()) {
      ClearPublisherFence(state);
      co_return acknowledged;
    }
    if (!AuthoritySessionsAllowed(*state->core_,
                                  state->leadership_generation_)) {
      co_return absl::CancelledError(
          "Meta authority became unavailable during Fence publication");
    }
    state->fenced_authorities_.push_back(anchor);
  }
  co_return absl::OkStatus();
}

bycorf::Task<absl::StatusOr<MetaReplacementDisposition>>
CheckLiveTransferBoundary(const std::shared_ptr<LiveSessionState>& state,
                          const NodeControlBatch& installed,
                          const NodeControlBatch& replacement) {
  if (!AuthoritySessionsAllowed(*state->core_, state->leadership_generation_)) {
    co_return absl::CancelledError(
        "Meta authority is unavailable during FullDesiredState publication");
  }
  if (state->commit_signal_->delivery_failed_.load(std::memory_order_acquire) ||
      state->commit_subscription_->needs_resync()) {
    co_return absl::ResourceExhaustedError(
        "Meta commit subscription overflowed during FullDesiredState "
        "publication");
  }
  const std::uint64_t high_water =
      state->core_->coordinator_->CommittedHighWater();
  const std::uint64_t published_index =
      state->commit_signal_->published_index_.load(std::memory_order_acquire);
  if (!detail::TransferBoundaryNeedsProjectionValidation(
          published_index, high_water,
          state->validated_committed_high_water_)) {
    co_return MetaReplacementDisposition::kContinue;
  }

  auto cached_view = CommittedViewAtLeast(*state->core_, high_water);
  if (!cached_view.ok()) co_return cached_view.status();
  const MetaCommittedView& view = **cached_view;
  auto latest = ProjectNodeBounded(*state->core_, view, state->node_id_);
  const control::FullDesiredState* latest_state =
      latest.ok() ? &latest->full_state : nullptr;
  if (absl::Status fenced =
          co_await FenceSupersededAuthorityLive(state, installed, latest_state);
      !fenced.ok()) {
    co_return fenced;
  }
  if (!latest.ok()) co_return latest.status();
  const MetaReplacementDisposition disposition = EvaluateNodeReplacement(
      replacement.full_state, latest->full_state, state->node_id_);
  if (disposition == MetaReplacementDisposition::kContinue) {
    // Keep projection_superseded_ set until the complete object is Applied
    // and the final stable-view check succeeds. This cursor only avoids
    // re-projecting the same semantic no-op commit at every 64 KiB boundary.
    detail::RecordEquivalentTransferBoundary(
        view.applied_index(), &state->validated_committed_high_water_);
  }
  co_return disposition;
}

bycorf::Task<absl::StatusOr<detail::MetaPublisherTransferDisposition>>
SendControlUpdateLive(const std::shared_ptr<LiveSessionState>& state,
                      const NodeControlBatch& installed,
                      const NodeControlBatch& replacement,
                      const control::NodeControlUpdate& update) {
  auto encoded = control::EncodeNodeControlUpdate(update);
  if (!encoded.ok()) co_return encoded.status();
  const std::string_view bytes = *encoded;
  if (bytes.size() > control::kMaxFullDesiredStateBytes) {
    co_return absl::ResourceExhaustedError(
        "FullDesiredState transfer exceeds its object-size limit");
  }
  if (state->expected_applied_.has_value()) {
    co_return absl::InternalError(
        "publisher attempted overlapping FullStateApplied handshakes");
  }

  if (bytes.size() <= control::kMaxFramePayloadBytes) {
    auto boundary =
        co_await CheckLiveTransferBoundary(state, installed, replacement);
    if (!boundary.ok()) co_return boundary.status();
    if (*boundary == MetaReplacementDisposition::kAbortSuperseded) {
      co_return detail::ClassifyPublisherSupersession(
          /*receiver_can_apply=*/false);
    }

    // Arm before the single frame for the same reason the streamed path arms
    // before TransferEnd: the reader may observe an immediate Applied while
    // this producer is still returning from the socket write.
    state->expected_applied_ = control::FullStateApplied{
        update.request_id, replacement.full_state.control_revision};
    state->applied_received_ = false;
    if (absl::Status armed =
            state->applied_ack_deadline_->Arm(std::chrono::milliseconds(
                state->core_->options_.session_progress_timeout_ms_));
        !armed.ok()) {
      ClearPublisherApplied(state);
      co_return armed;
    }
    if (absl::Status sent = co_await state->io_->Send(
            control::MessagePriority::kReliable, control::WireMessage(update));
        !sent.ok()) {
      ClearPublisherApplied(state);
      co_return sent;
    }
    boundary =
        co_await CheckLiveTransferBoundary(state, installed, replacement);
    if (!boundary.ok()) {
      ClearPublisherApplied(state);
      co_return boundary.status();
    }
    if (*boundary == MetaReplacementDisposition::kAbortSuperseded) {
      if (absl::Status applied = co_await AwaitPublisherApplied(state);
          !applied.ok()) {
        co_return applied;
      }
      co_return detail::ClassifyPublisherSupersession(
          /*receiver_can_apply=*/true);
    }
    if (absl::Status applied = co_await AwaitPublisherApplied(state);
        !applied.ok()) {
      co_return applied;
    }
    co_return detail::MetaPublisherTransferDisposition::kApplied;
  }

  auto object_id = control::GenerateId128();
  if (!object_id.ok()) co_return object_id.status();
  const control::TransferStart start{
      .kind = control::TransferKind::kNodeControlUpdate,
      .object_id = *object_id,
      .total_length = bytes.size(),
  };
  if (absl::Status sent = co_await state->io_->Send(
          control::MessagePriority::kReliable, control::WireMessage(start));
      !sent.ok()) {
    co_return sent;
  }

  auto boundary =
      co_await CheckLiveTransferBoundary(state, installed, replacement);
  if (!boundary.ok()) {
    co_return boundary.status();
  }
  if (*boundary == MetaReplacementDisposition::kAbortSuperseded) {
    if (absl::Status aborted = co_await AbortSupersededReplacement(
            *state->io_, *object_id, /*transfer_active=*/true);
        !aborted.ok()) {
      co_return aborted;
    }
    co_return detail::ClassifyPublisherSupersession(
        /*receiver_can_apply=*/false);
  }

  for (std::size_t offset = 0; offset < bytes.size();
       offset += control::kControlTransferChunkBytes) {
    const std::size_t count =
        std::min(control::kControlTransferChunkBytes, bytes.size() - offset);
    if (absl::Status sent = co_await state->io_->Send(
            control::MessagePriority::kBulk,
            control::WireMessage(control::TransferChunk{
                .object_id = *object_id,
                .offset = offset,
                .bytes = std::string(bytes.substr(offset, count)),
            }));
        !sent.ok()) {
      co_return sent;
    }
    boundary =
        co_await CheckLiveTransferBoundary(state, installed, replacement);
    if (!boundary.ok()) {
      co_return boundary.status();
    }
    if (*boundary == MetaReplacementDisposition::kAbortSuperseded) {
      if (absl::Status aborted = co_await AbortSupersededReplacement(
              *state->io_, *object_id, /*transfer_active=*/true);
          !aborted.ok()) {
        co_return aborted;
      }
      co_return detail::ClassifyPublisherSupersession(
          /*receiver_can_apply=*/false);
    }
  }

  // Arm before TransferEnd so the sole reader can accept an immediate Applied
  // response. FenceAck is tracked independently, allowing a commit discovered
  // after End to revoke old authority while Applied is also in flight.
  state->expected_applied_ = control::FullStateApplied{
      update.request_id, replacement.full_state.control_revision};
  state->applied_received_ = false;
  if (absl::Status armed =
          state->applied_ack_deadline_->Arm(std::chrono::milliseconds(
              state->core_->options_.session_progress_timeout_ms_));
      !armed.ok()) {
    ClearPublisherApplied(state);
    co_return armed;
  }
  if (absl::Status sent = co_await state->io_->Send(
          control::MessagePriority::kReliable,
          control::WireMessage(control::TransferEnd{*object_id}));
      !sent.ok()) {
    ClearPublisherApplied(state);
    co_return sent;
  }
  boundary = co_await CheckLiveTransferBoundary(state, installed, replacement);
  if (!boundary.ok()) {
    ClearPublisherApplied(state);
    co_return boundary.status();
  }
  if (*boundary == MetaReplacementDisposition::kAbortSuperseded) {
    if (absl::Status applied = co_await AwaitPublisherApplied(state);
        !applied.ok()) {
      co_return applied;
    }
    co_return detail::ClassifyPublisherSupersession(
        /*receiver_can_apply=*/true);
  }
  if (absl::Status applied = co_await AwaitPublisherApplied(state);
      !applied.ok()) {
    co_return applied;
  }
  co_return detail::MetaPublisherTransferDisposition::kApplied;
}

bycorf::Task<absl::Status> SessionPublisherBody(
    const std::shared_ptr<LiveSessionState>& state) {
  while (
      !state->closing_ &&
      AuthoritySessionsAllowed(*state->core_, state->leadership_generation_)) {
    if (state->commit_signal_->delivery_failed_.load(
            std::memory_order_acquire) ||
        state->commit_subscription_->needs_resync()) {
      co_return absl::ResourceExhaustedError(
          "Meta commit subscription overflowed for data-control publisher");
    }
    const std::uint64_t high_water =
        state->core_->coordinator_->CommittedHighWater();
    if (!CommitPending(*state->commit_signal_,
                       state->validated_committed_high_water_) &&
        state->validated_committed_high_water_ >= high_water) {
      co_await state->commit_signal_->changed_.Wait();
      continue;
    }

    auto cached_view = CommittedViewAtLeast(*state->core_, high_water);
    if (!cached_view.ok()) co_return cached_view.status();
    const MetaCommittedView& view = **cached_view;
    auto latest = ProjectNodeBounded(*state->core_, view, state->node_id_);
    std::shared_ptr<const NodeControlBatch> installed = state->installed_;
    const control::FullDesiredState* latest_state =
        latest.ok() ? &latest->full_state : nullptr;
    if (!latest.ok() ||
        EvaluateNodeReplacement(installed->full_state, latest->full_state,
                                state->node_id_) ==
            MetaReplacementDisposition::kAbortSuperseded) {
      state->projection_superseded_ = true;
    }
    if (absl::Status fenced = co_await FenceSupersededAuthorityLive(
            state, *installed, latest_state);
        !fenced.ok()) {
      co_return fenced;
    }
    if (!latest.ok()) co_return latest.status();
    if (EvaluateNodeReplacement(installed->full_state, latest->full_state,
                                state->node_id_) ==
        MetaReplacementDisposition::kContinue) {
      // No selected object changed. Advance only Meta's validation cursor.
      state->validated_committed_high_water_ = std::max(
          state->validated_committed_high_water_, view.applied_index());
      state->core_->options_.runtime_status_->MarkValidated(
          state->node_id_, state->session_id_,
          state->validated_committed_high_water_);
      state->projection_superseded_ = false;
      continue;
    }

    state->core_->options_.runtime_status_->Remove(state->node_id_,
                                                   &state->session_id_);
    auto next =
        control::SelectNodeControlState(latest->full_state, state->node_id_);
    auto update = control::DiffNodeControlState(state->selected_, next);
    auto request_id = control::GenerateId128();
    if (!request_id.ok()) co_return request_id.status();
    update.request_id = *request_id;
    latest->full_state.control_revision = next.local.revision;
    auto published =
        co_await SendControlUpdateLive(state, *installed, *latest, update);
    if (!published.ok()) {
      co_return published.status();
    }
    if (*published ==
        detail::MetaPublisherTransferDisposition::kRetryBeforeApplyInSession) {
      // A started transfer was reset with an object-local TransferAbort. If no
      // frame was visible yet, there was nothing to reset. In both cases keep
      // the authenticated session and its boot observation while the next
      // iteration projects the latest commit.
      continue;
    }
    state->core_->full_states_sent_.fetch_add(1, std::memory_order_relaxed);
    state->selected_ = std::move(next);
    state->installed_ = RetainProjection(std::move(*latest));
    state->publisher_adoption_gate_.MarkProjectionAdopted();
    state->response_changed_.NotifyAll(*state->worker_);
    // The live installation now owns the replacement. Drop the publisher's
    // old-generation snapshot before reserving the stable-check build, or a
    // coroutine-local reference would turn the intended two-generation bound
    // into three simultaneous projection charges.
    installed.reset();

    if (*published == detail::MetaPublisherTransferDisposition::
                          kAwaitExactAppliedAndRetryInSession) {
      // Data installed this exact intermediate object and its Applied was
      // consumed above. Retain it as the real session baseline, but skip
      // lease publication until the latest control state is installed.
      continue;
    }

    // Data has applied this object, but a commit may have landed behind its
    // End/Ack exchange. Re-enter the publisher before granting new authority
    // against an intermediate control state.
    const std::uint64_t stable_high_water =
        state->core_->coordinator_->CommittedHighWater();
    auto cached_stable_view =
        CommittedViewAtLeast(*state->core_, stable_high_water);
    if (!cached_stable_view.ok()) co_return cached_stable_view.status();
    const MetaCommittedView& stable_view = **cached_stable_view;
    auto stable =
        ProjectNodeBounded(*state->core_, stable_view, state->node_id_);
    if (!stable.ok() ||
        EvaluateNodeReplacement(state->installed_->full_state,
                                stable->full_state, state->node_id_) ==
            MetaReplacementDisposition::kAbortSuperseded) {
      continue;
    }
    state->validated_committed_high_water_ = std::max(
        state->validated_committed_high_water_, stable_view.applied_index());
    state->projection_superseded_ = false;
    state->core_->options_.runtime_status_->PublishCurrent(
        state->node_id_, state->boot_id_, state->session_id_,
        state->replication_history_id_, state->replication_flow_count_,
        state->session_generation_, state->leadership_generation_,
        state->validated_committed_high_water_, state->installed_->full_state);
  }
  co_return absl::CancelledError(
      "data-control publisher stopped with its leadership generation");
}

bycorf::Task<absl::Status> RunSessionPublisher(
    std::shared_ptr<LiveSessionState> state) {
  absl::Status status = co_await SessionPublisherBody(state);
  FinishLiveSessionTask(state, &state->publisher_running_);
  if (!status.ok() && !state->closing_) {
    FailLiveSession(state, status);
  }
  co_return status;
}

bycorf::Task<absl::Status> HandleDirectiveResult(
    const std::shared_ptr<MetaDataControlServer::Core>& core, SessionIo& io,
    const control::DirectiveResult& result, std::string_view node_id,
    const MetaBootIncarnation& boot_id, std::uint64_t leadership_generation,
    const control::WireId128& session_id) {
  auto parsed_boot = ParseIdentity<20>(result.recipient_boot_id,
                                       "directive result recipient boot id");
  if (!parsed_boot.ok() || *parsed_boot != boot_id ||
      result.session_id != session_id) {
    co_return absl::InvalidArgumentError(
        "directive result session or boot mismatch");
  }

  const MetaTerminalReceiptKey key{
      result.identity.operation_id,
      result.identity.directive_id,
      result.identity.attempt_id,
      result.identity.directive_revision,
  };
  auto view = core->coordinator_->CommittedView();
  if (const auto receipt = view.operation().FindTerminalReceipt(key);
      receipt.has_value()) {
    if (!ReceiptMatches(*receipt, result, node_id, boot_id)) {
      co_return absl::AlreadyExistsError(
          "directive result conflicts with its committed receipt");
    }
    co_return co_await io.Send(
        control::MessagePriority::kReliable,
        control::WireMessage(ResultAck(result, *receipt)));
  }
  const auto operation =
      view.operation().FindOperation(result.identity.operation_id);
  const MetaCurrentDirective* tracked = nullptr;
  if (operation.has_value() && !IsTerminal(operation->lifecycle_)) {
    const auto match = std::find_if(
        operation->current_directives_.begin(),
        operation->current_directives_.end(),
        [&](const MetaCurrentDirective& directive) {
          return directive.spec_.directive_id_ ==
                     result.identity.directive_id &&
                 directive.spec_.attempt_id_ == result.identity.attempt_id &&
                 directive.directive_revision_ ==
                     result.identity.directive_revision;
        });
    if (match != operation->current_directives_.end()) tracked = &*match;
  }
  if (tracked == nullptr) {
    co_return co_await io.Send(control::MessagePriority::kReliable,
                               control::WireMessage(NoLongerTracked(result)));
  }
  // Final outcomes are validated against committed task identity, independent
  // of optional admission or progress responses on this connection.
  const MetaDirectiveSpec& spec = tracked->spec_;
  if (!IsKnownMetaDirective(spec.kind_)) {
    co_return absl::FailedPreconditionError(
        "tracked directive kind is not executable by data control");
  }
  const bool executes_on_target = IsMetaPopulationDirective(spec.kind_);
  const std::string& expected_node =
      executes_on_target ? spec.target_node_id_ : spec.source_node_id_;
  const MetaBootIncarnation& expected_boot =
      executes_on_target ? spec.target_boot_id_ : spec.source_boot_id_;
  if (spec.recipient_node_id_ != expected_node || expected_node != node_id ||
      expected_boot != boot_id || spec.assignment_id_ != result.assignment_id) {
    co_return absl::PermissionDeniedError(
        "directive result does not match its recipient or authority anchor");
  }
  if (!AuthoritySessionsAllowed(*core, leadership_generation)) {
    co_return absl::FailedPreconditionError(
        "Meta authority is unavailable before directive result proposal");
  }
  auto request_id = control::GenerateId128();
  if (!request_id.ok()) co_return request_id.status();
  MetaDirectiveResultStatus status;
  switch (result.status) {
    case control::DirectiveResultStatus::kSucceeded:
      status = MetaDirectiveResultStatus::kSucceeded;
      break;
    case control::DirectiveResultStatus::kFailed:
      status = MetaDirectiveResultStatus::kFailed;
      break;
    case control::DirectiveResultStatus::kRejected:
      status = MetaDirectiveResultStatus::kRejected;
      break;
  }
  CommitDirectiveResult command{
      .request_id_ = *request_id,
      .actor_ = {},
      .operation_id_ = result.identity.operation_id,
      .directive_id_ = result.identity.directive_id,
      .attempt_id_ = result.identity.attempt_id,
      .directive_revision_ = result.identity.directive_revision,
      .recipient_node_id_ = std::string(node_id),
      .recipient_boot_id_ = boot_id,
      .assignment_id_ = result.assignment_id,
      .status_ = status,
      .result_ = result.result,
  };
  MetaLeaderContext* context = core->leader_context_;
  auto proposed = co_await context->Propose(MetaCommand(std::move(command)));
  if (!proposed.ok()) co_return proposed.status();

  view = core->coordinator_->CommittedView();
  const auto receipt = view.operation().FindTerminalReceipt(key);
  if (!receipt.has_value()) {
    // An accepted Raft index with a domain rejection means the operation no
    // longer tracks this attempt. An uncertain proposal status was returned
    // above instead, forcing a retry rather than a false negative ack.
    co_return co_await io.Send(control::MessagePriority::kReliable,
                               control::WireMessage(NoLongerTracked(result)));
  }
  if (!ReceiptMatches(*receipt, result, node_id, boot_id)) {
    co_return absl::AlreadyExistsError(
        "committed directive receipt does not match submitted result");
  }
  core->directive_results_committed_.fetch_add(1, std::memory_order_relaxed);
  co_return co_await io.Send(control::MessagePriority::kReliable,
                             control::WireMessage(ResultAck(result, *receipt)));
}

bycorf::Task<absl::Status> RunEstablishedSession(
    const std::shared_ptr<LiveSessionState>& state,
    const MetaBootIncarnation& boot_id,
    const MetaReplicationHistoryId& replication_history_id,
    std::uint64_t session_generation,
    std::deque<control::WireMessage> deferred) {
  control::HeartbeatSequenceWindow heartbeat_window;
  std::optional<control::HeartbeatAck> cached_ack;
  ClientTransferSink client_transfer_sink;
  control::LargeObjectReassembler client_reassembler(client_transfer_sink);

  while (
      !state->closing_ &&
      AuthoritySessionsAllowed(*state->core_, state->leadership_generation_)) {
    absl::StatusOr<control::WireMessage> incoming =
        deferred.empty()
            ? co_await state->io_->Read()
            : absl::StatusOr<control::WireMessage>(std::move(deferred.front()));
    if (!deferred.empty()) deferred.pop_front();
    if (!incoming.ok()) co_return incoming.status();
    // The read may have spanned host suspend. Recheck before replaying a
    // cached lease Ack or consuming any authority-bearing client message.
    if (!AuthoritySessionsAllowed(*state->core_,
                                  state->leadership_generation_)) {
      co_return absl::UnavailableError(
          "Meta authority became unavailable while awaiting session input");
    }
    if (state->commit_signal_->delivery_failed_.load(
            std::memory_order_acquire) ||
        state->commit_subscription_->needs_resync()) {
      co_return absl::ResourceExhaustedError(
          "Meta commit subscription overflowed for data-control session");
    }

    auto publisher_response = HandlePublisherResponse(state, *incoming);
    if (!publisher_response.ok()) co_return publisher_response.status();
    if (*publisher_response) {
      // FullStateApplied wakes the publisher but does not itself change the
      // projection used below. Do not read a buffered heartbeat until the
      // publisher has made the acknowledged object the session baseline.
      while (!state->closing_ && state->publisher_adoption_gate_.pending()) {
        co_await state->response_changed_.Wait();
      }
      if (state->closing_) {
        co_return state->terminal_error_.value_or(
            absl::CancelledError("data-control session closed"));
      }
      continue;
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
      const bool advances =
          starts || std::holds_alternative<control::TransferChunk>(*incoming);
      const bool finishes =
          std::holds_alternative<control::TransferEnd>(*incoming) ||
          std::holds_alternative<control::TransferAbort>(*incoming);
      const absl::Status accepted = std::visit(
          [&](const auto& message) -> absl::Status {
            using T = std::decay_t<decltype(message)>;
            if constexpr (std::is_same_v<T, control::TransferStart> ||
                          std::is_same_v<T, control::TransferChunk> ||
                          std::is_same_v<T, control::TransferEnd> ||
                          std::is_same_v<T, control::TransferAbort>) {
              return client_reassembler.Accept(message);
            }
            return absl::InternalError(
                "non-transfer reached client object reassembler");
          },
          *incoming);
      if (!accepted.ok()) co_return accepted;
      if (advances) {
        const absl::Status armed =
            state->inbound_transfer_deadline_->Arm(std::chrono::milliseconds(
                state->core_->options_.session_progress_timeout_ms_));
        if (!armed.ok()) co_return armed;
      }
      if (finishes) (void)state->inbound_transfer_deadline_->Disarm();
      if (!client_transfer_sink.committed()) continue;
      if (!client_transfer_sink.kind().has_value()) {
        co_return absl::InternalError(
            "committed client transfer has no typed kind");
      }
      auto decoded = control::DecodeMessage(
          control::MessageType::kDirectiveResult, client_transfer_sink.Take());
      if (!decoded.ok()) co_return decoded.status();
      incoming = std::move(*decoded);
    }

    if (const auto* heartbeat = std::get_if<control::Heartbeat>(&*incoming)) {
      if (heartbeat->session_id != state->session_id_) {
        co_return absl::InvalidArgumentError("heartbeat session id mismatch");
      }
      if (absl::Status sequence =
              heartbeat_window.Observe(heartbeat->heartbeat_sequence);
          !sequence.ok()) {
        co_return sequence;
      }

      // Observation ingestion is independent from publisher progress and
      // lease validation. In particular, a rejected challenge or candidate
      // cannot suppress an otherwise valid boot/health observation.
      const std::shared_ptr<const NodeControlBatch> installed =
          state->installed_;
      auto failover_projection = detail::FailoverProjectionForHeartbeat(
          installed->full_state, state->node_id_, boot_id);
      if (!failover_projection.ok()) {
        co_return failover_projection.status();
      }
      auto owner_projection = detail::OwnerProjectionForHeartbeat(
          installed->full_state, state->node_id_);
      if (!owner_projection.ok()) {
        co_return owner_projection.status();
      }
      std::optional<std::uint64_t> confirmed_grant_sequence =
          detail::ConfirmedLeaseForHeartbeat(
              cached_ack, heartbeat->heartbeat_sequence, state->boot_id_,
              *owner_projection);
      const std::uint64_t committed_high_water =
          state->core_->coordinator_->CommittedHighWater();
      if (committed_high_water > state->validated_committed_high_water_ &&
          PublishCommitIndex(*state->commit_signal_, committed_high_water)) {
        // Snapshot install has no per-entry subscription callback. A live
        // heartbeat that detects its synchronous store watermark wakes the
        // shared publisher feed; lease evaluation below remains denied until
        // this session validates or installs the newer projection.
        state->commit_signal_->changed_.NotifyAll(*state->worker_);
      }
      auto cached_view =
          CommittedViewAtLeast(*state->core_, committed_high_water);
      if (!cached_view.ok()) co_return cached_view.status();
      const MetaCommittedView& latest_view = **cached_view;
      MetaStoresFacts facts(latest_view.stores());
      const std::int64_t heartbeat_received_unix_ms = NowUnixMillis();
      const std::uint64_t heartbeat_received_steady_ms =
          static_cast<std::uint64_t>(ActiveClockMillis());
      MetaHeartbeatObservationResult observation = IngestHeartbeatObservations(
          *state->core_->observations_, facts, state->node_id_, boot_id,
          replication_history_id, session_generation, heartbeat->health,
          heartbeat->role_information, heartbeat->failover_observation,
          std::move(*failover_projection), std::move(*owner_projection),
          heartbeat->heartbeat_sequence, confirmed_grant_sequence,
          heartbeat_received_unix_ms, heartbeat_received_steady_ms);
      state->core_->options_.runtime_status_->RecordHealth(
          state->node_id_, state->session_id_, heartbeat->health,
          heartbeat_received_unix_ms);
      if (observation.status == control::ObservationStatus::kAccepted) {
        state->core_->observations_accepted_.fetch_add(
            1, std::memory_order_relaxed);
      } else {
        state->core_->observations_rejected_.fetch_add(
            1, std::memory_order_relaxed);
      }

      const bool leader_valid =
          AuthoritySessionsAllowed(*state->core_,
                                   state->leadership_generation_) &&
          !CommitPending(*state->commit_signal_,
                         state->validated_committed_high_water_) &&
          !state->projection_superseded_ &&
          state->validated_committed_high_water_ >= committed_high_water;
      std::optional<control::LeaseChallenge> challenge;
      if (const auto* authority = std::get_if<control::AuthorityLeaseRequest>(
              &heartbeat->role_information)) {
        challenge = authority->challenge;
      }
      const MetaLeaseEvaluation lease_evaluation{
          .leader_valid_ = leader_valid,
          .server_id_ = state->core_->options_.server_id_,
          .raft_term_ = state->core_->server_->get_term(),
          .leadership_generation_ = state->leadership_generation_,
          .leadership_validity_ms_ =
              state->core_->options_.leadership_validity_ms_,
          .node_id_ = state->node_id_,
          .boot_id_ = state->boot_id_,
          .applied_projection_index_ = installed->full_state.control_revision,
          .desired_ = &installed->full_state,
      };
      control::LeaseDecision lease =
          state->core_->lease_handoff_guard_->Enforce(
              EvaluateLeaseChallenge(challenge, heartbeat->health,
                                     lease_evaluation),
              challenge, lease_evaluation, cluster::LeaseClockMillis());
      if (std::holds_alternative<control::LeaseGranted>(lease)) {
        state->core_->lease_grants_.fetch_add(1, std::memory_order_relaxed);
      } else if (!std::holds_alternative<control::NoChallenge>(lease)) {
        state->core_->lease_denials_.fetch_add(1, std::memory_order_relaxed);
      }
      cached_ack = control::HeartbeatAck{
          .session_id = state->session_id_,
          .heartbeat_sequence = heartbeat->heartbeat_sequence,
          .observation_status = observation.status,
          .observation_detail = std::move(observation.detail),
          .lease_decision = std::move(lease),
      };
      // A failed stream write may still have delivered the complete Ack.
      // Publish a handoff marker or possible finite Grant before attempting
      // the send, then let exact session/authority progress retire it.
      if (absl::Status attempted =
              state->core_->observations_->RecordOwnerLeaseDecisionAttempt(
                  MetaObservationIdentity{state->node_id_, boot_id,
                                          session_generation},
                  cached_ack->heartbeat_sequence, cached_ack->lease_decision);
          !attempted.ok()) {
        co_return attempted;
      }
      if (absl::Status sent =
              co_await state->io_->Send(control::MessagePriority::kAuthority,
                                        control::WireMessage(*cached_ack));
          !sent.ok()) {
        co_return sent;
      }
      if (absl::Status written =
              state->core_->observations_->RecordOwnerLeaseDecisionWritten(
                  MetaObservationIdentity{state->node_id_, boot_id,
                                          session_generation},
                  cached_ack->heartbeat_sequence, cached_ack->lease_decision);
          !written.ok()) {
        co_return written;
      }
      state->core_->options_.runtime_status_->RecordLeaseDecisionWritten(
          state->node_id_, state->session_id_, cached_ack->heartbeat_sequence,
          cached_ack->lease_decision, NowUnixMillis());
      continue;
    }

    if (const auto* result =
            std::get_if<control::DirectiveResult>(&*incoming)) {
      if (absl::Status handled = co_await HandleDirectiveResult(
              state->core_, *state->io_, *result, state->node_id_, boot_id,
              state->leadership_generation_, state->session_id_);
          !handled.ok()) {
        co_return handled;
      }
      continue;
    }
    if (const auto* receipt =
            std::get_if<control::DirectiveResponse>(&*incoming)) {
      if (receipt->session_id != state->session_id_ ||
          receipt->recipient_boot_id != state->boot_id_) {
        co_return absl::InvalidArgumentError(
            "directive receipt does not name the current session/boot");
      }
      // Admission is advisory. A superseded task may reply after replacement;
      // only its final outcome attempts a committed-state transition.
      continue;
    }
    co_return absl::InvalidArgumentError("unexpected data-control message");
  }
  if (state->terminal_error_.has_value()) co_return *state->terminal_error_;
  co_return absl::CancelledError("Meta leadership changed");
}

}  // namespace

std::chrono::milliseconds detail::EstablishedSessionReadTimeout(
    std::uint32_t observation_ttl_ms,
    std::uint32_t session_progress_timeout_ms) noexcept {
  const std::uint64_t total_ms =
      static_cast<std::uint64_t>(observation_ttl_ms) +
      static_cast<std::uint64_t>(session_progress_timeout_ms);
  static_assert(
      std::numeric_limits<std::chrono::milliseconds::rep>::digits >= 33,
      "milliseconds must represent two uint32 millisecond intervals");
  return std::chrono::milliseconds(
      static_cast<std::chrono::milliseconds::rep>(total_ms));
}

class MetaDataControlServer::SessionConnectionBorrow {
 public:
  SessionConnectionBorrow(CorePtr core, bycorf::Connection* connection)
      : core_(std::move(core)), connection_(connection) {
    core_->sessions_.push_back(connection_);
    core_->live_session_tasks_.fetch_add(1, std::memory_order_relaxed);
    bycorf::BorrowConnectionStorage(connection_);
  }

  SessionConnectionBorrow(SessionConnectionBorrow&& other) noexcept
      : core_(std::move(other.core_)), connection_(other.connection_) {
    other.connection_ = nullptr;
  }
  SessionConnectionBorrow(const SessionConnectionBorrow&) = delete;
  SessionConnectionBorrow& operator=(const SessionConnectionBorrow&) = delete;
  SessionConnectionBorrow& operator=(SessionConnectionBorrow&&) = delete;

  ~SessionConnectionBorrow() {
    if (connection_ == nullptr) return;
    UnregisterSession(*core_, connection_);
    bycorf::ReleaseConnectionStorage(connection_);
    NotifyShutdownDrained(*core_);
  }

 private:
  CorePtr core_;
  bycorf::Connection* connection_;
};

detail::PendingHandshakeLimiter::Permit::Permit(Permit&& other) noexcept
    : owner_(std::exchange(other.owner_, nullptr)) {}

detail::PendingHandshakeLimiter::Permit&
detail::PendingHandshakeLimiter::Permit::operator=(Permit&& other) noexcept {
  if (this == &other) return *this;
  Release();
  owner_ = std::exchange(other.owner_, nullptr);
  return *this;
}

detail::PendingHandshakeLimiter::Permit::~Permit() { Release(); }

void detail::PendingHandshakeLimiter::Permit::Release() noexcept {
  if (owner_ == nullptr) return;
  PendingHandshakeLimiter* owner = std::exchange(owner_, nullptr);
  owner->Release();
}

std::optional<detail::PendingHandshakeLimiter::Permit>
detail::PendingHandshakeLimiter::TryAcquire() {
  if (pending_ >= limit_) return std::nullopt;
  ++pending_;
  return Permit(this);
}

void detail::PendingHandshakeLimiter::Release() noexcept {
  if (pending_ == 0) std::terminate();
  --pending_;
}

detail::RetainedProjectionLimiter::Permit::Permit(Permit&& other) noexcept
    : owner_(std::exchange(other.owner_, nullptr)),
      bytes_(std::exchange(other.bytes_, 0)) {}

detail::RetainedProjectionLimiter::Permit&
detail::RetainedProjectionLimiter::Permit::operator=(Permit&& other) noexcept {
  if (this == &other) return *this;
  Release();
  owner_ = std::exchange(other.owner_, nullptr);
  bytes_ = std::exchange(other.bytes_, 0);
  return *this;
}

detail::RetainedProjectionLimiter::Permit::~Permit() { Release(); }

absl::Status detail::RetainedProjectionLimiter::Permit::Resize(
    std::size_t bytes) {
  if (owner_ == nullptr) {
    return absl::FailedPreconditionError(
        "retained-projection permit has already been released");
  }
  if (!owner_->Resize(*this, bytes)) {
    return absl::ResourceExhaustedError(
        "Meta retained-projection byte budget is exhausted");
  }
  return absl::OkStatus();
}

void detail::RetainedProjectionLimiter::Permit::Release() noexcept {
  if (owner_ == nullptr) return;
  RetainedProjectionLimiter* owner = std::exchange(owner_, nullptr);
  owner->Release(std::exchange(bytes_, 0));
}

std::optional<detail::RetainedProjectionLimiter::Permit>
detail::RetainedProjectionLimiter::TryAcquire(std::size_t bytes) {
  if (bytes > limit_ - retained_bytes_) return std::nullopt;
  retained_bytes_ += bytes;
  return Permit(this, bytes);
}

bool detail::RetainedProjectionLimiter::Resize(Permit& permit,
                                               std::size_t bytes) noexcept {
  if (bytes > permit.bytes_) {
    const std::size_t increase = bytes - permit.bytes_;
    if (increase > limit_ - retained_bytes_) return false;
    retained_bytes_ += increase;
  } else {
    retained_bytes_ -= permit.bytes_ - bytes;
  }
  permit.bytes_ = bytes;
  return true;
}

void detail::RetainedProjectionLimiter::Release(std::size_t bytes) noexcept {
  if (bytes > retained_bytes_) std::terminate();
  retained_bytes_ -= bytes;
}

bool detail::BoundNodeSessionRegistry::TryClaim(
    std::string_view node_id, bycorf::Connection* connection) {
  return sessions_.emplace(node_id, connection).second;
}

void detail::BoundNodeSessionRegistry::Release(
    std::string_view node_id, bycorf::Connection* connection) noexcept {
  const auto session = sessions_.find(node_id);
  if (session != sessions_.end() && session->second == connection) {
    sessions_.erase(session);
  }
}

absl::Status MetaDataControlServer::ValidateOptions(
    const MetaDataControlServerOptions& options) {
  if (options.server_id_ == 0 || options.bind_host_.empty() ||
      options.port_ == 0) {
    return absl::InvalidArgumentError(
        "data-control server id and numeric bind endpoint are required");
  }
  in_addr address4{};
  in6_addr address6{};
  if (::inet_pton(AF_INET, options.bind_host_.c_str(), &address4) != 1 &&
      ::inet_pton(AF_INET6, options.bind_host_.c_str(), &address6) != 1) {
    return absl::InvalidArgumentError(
        "data-control bind host must be a numeric IP address");
  }
  if (!options.local_ctl_endpoint_.empty()) {
    auto ctl = lavik::ParseNumericEndpoint(options.local_ctl_endpoint_);
    in_addr ctl_address4{};
    in6_addr ctl_address6{};
    if (!ctl.has_value() ||
        (::inet_pton(AF_INET, ctl->host_.c_str(), &ctl_address4) == 1 &&
         ctl_address4.s_addr == htonl(INADDR_ANY)) ||
        (::inet_pton(AF_INET6, ctl->host_.c_str(), &ctl_address6) == 1 &&
         IN6_IS_ADDR_UNSPECIFIED(&ctl_address6))) {
      return absl::InvalidArgumentError(
          "local ctl endpoint must be a concrete numeric IP:port");
    }
  }
  const unsigned tls_fields = !options.tls_ca_cert_file_.empty() +
                              !options.tls_cert_file_.empty() +
                              !options.tls_key_file_.empty();
  if (tls_fields != 0 && tls_fields != 3) {
    return absl::InvalidArgumentError(
        "data-control mTLS requires CA, certificate, and key together");
  }
  const std::uint32_t maximum_heartbeat_interval_ms =
      std::max<std::uint32_t>(1, options.leadership_validity_ms_ / 3);
  if (options.observation_ttl_ms_ == 0 ||
      maximum_heartbeat_interval_ms > options.observation_ttl_ms_ ||
      options.session_progress_timeout_ms_ == 0 ||
      options.session_progress_timeout_ms_ > kMaxSessionProgressTimeoutMs ||
      options.leadership_validity_ms_ == 0 ||
      options.lease_handoff_safety_margin_ms_ <
          options.leadership_validity_ms_ ||
      options.max_pending_handshakes_ == 0 ||
      options.max_pending_handshakes_ > control::kMaxProjectedNodes ||
      options.max_write_queue_bytes_ < control::kMaxFrameBytes ||
      options.max_retained_projection_bytes_ <
          kProjectionBuildReservationBytes) {
    return absl::InvalidArgumentError(
        "data-control timing, handoff, handshake, writer, or projection "
        "bound is invalid");
  }
  return absl::OkStatus();
}

absl::StatusOr<std::shared_ptr<MetaDataControlServer>>
MetaDataControlServer::Create(
    bycorf::ForeignExecutor foreign_executor,
    nuraft::ptr<nuraft::raft_server> server, MetaCoordinator& coordinator,
    std::shared_ptr<MetaObservationStore> observations,
    MetaDataControlServerOptions options) {
  if (!foreign_executor.valid() || server == nullptr ||
      observations == nullptr) {
    return absl::InvalidArgumentError(
        "data-control server dependencies must be valid");
  }
  if (absl::Status valid = ValidateOptions(options); !valid.ok()) return valid;
  auto core = std::make_shared<Core>();
  core->foreign_executor_ = foreign_executor;
  core->server_ = std::move(server);
  core->coordinator_ = &coordinator;
  core->committed_view_cache_ =
      std::make_unique<detail::MetaCommittedViewCache>(
          [&coordinator] { return coordinator.CommittedView(); });
  core->observations_ = std::move(observations);
  core->options_ = std::move(options);
  if (core->options_.runtime_status_ == nullptr) {
    core->options_.runtime_status_ =
        std::make_shared<MetaDataControlRuntimeStatus>();
  }
  core->lease_handoff_guard_ = std::make_unique<MetaLeaseHandoffGuard>(
      core->options_.leadership_validity_ms_,
      core->options_.lease_handoff_safety_margin_ms_);
  core->leader_runtime_guard_ = std::make_unique<MetaLeaderRuntimeGuard>(
      core->options_.leadership_validity_ms_);
  core->pending_handshakes_ = std::make_unique<detail::PendingHandshakeLimiter>(
      core->options_.max_pending_handshakes_);
  core->projection_limiter_ =
      std::make_unique<detail::RetainedProjectionLimiter>(
          core->options_.max_retained_projection_bytes_);
  if (!core->options_.tls_ca_cert_file_.empty()) {
    auto tls = bycorf::TlsContext::CreateServer(bycorf::TlsServerOptions{
        .cert_file_ = core->options_.tls_cert_file_,
        .key_file_ = core->options_.tls_key_file_,
        .ca_cert_file_ = core->options_.tls_ca_cert_file_,
        .client_auth_ = bycorf::TlsClientAuth::kRequired,
    });
    if (!tls.ok()) return tls.status();
    core->tls_context_ = std::move(*tls);
  }
  return std::shared_ptr<MetaDataControlServer>(
      new MetaDataControlServer(std::move(core)));
}

MetaDataControlServer::~MetaDataControlServer() { Shutdown(); }

void MetaDataControlServer::StartListener() {
  CorePtr core = core_;
  if (!core->foreign_executor_.Notify([core]() noexcept {
        if (core->shutdown_ || core->listening_) return;
        core->worker_ = bycorf::ThisWorker().self_;
        const absl::Status bound = core->listener_.Bind(
            core->worker_, core->options_.bind_host_, core->options_.port_,
            /*backlog=*/128, /*reuse_port=*/false);
        {
          std::lock_guard<std::mutex> lock(core->status_mu_);
          core->status_ = bound;
        }
        if (!bound.ok()) return;
        core->listening_ = true;
        core->accept_loop_running_ = true;
        core->worker_->Spawn(AcceptLoop(core));
      })) {
    std::lock_guard<std::mutex> lock(core->status_mu_);
    core->status_ = absl::UnavailableError("Bycorf worker is stopping");
  }
}

void MetaDataControlServer::Shutdown() {
  CorePtr core = core_;
  if (core->shutdown_complete_.load(std::memory_order_acquire)) return;
  auto complete = std::make_shared<std::promise<void>>();
  std::future<void> done = complete->get_future();
  if (!core->foreign_executor_.Notify([core, complete]() noexcept {
        core->shutdown_drain_waiters_.push_back(complete);
        if (!core->shutdown_) {
          core->shutdown_ = true;
          core->listening_ = false;
          core->leader_active_ = false;
          core->leader_ready_for_data_ = false;
          core->leader_context_ = nullptr;
          core->leader_commit_subscription_.reset();
          core->leader_commit_signal_.reset();
          core->options_.runtime_status_->EndLeadership(
              core->leadership_generation_);
          if (core->accept_loop_running_) {
            // TcpListener::Close does not currently cancel an armed accept.
            // A local connection gives that sole waiter a normal completion;
            // AcceptLoop recognizes shutdown, retires both socket ends, then
            // closes the listener before satisfying the synchronous drain.
            absl::Status wake_status = absl::UnavailableError(
                "data-control shutdown accept wake was not attempted");
            for (int attempt = 0; attempt != 3; ++attempt) {
              auto wake = OpenShutdownAcceptWakeSocket(
                  core->options_.bind_host_, core->options_.port_);
              if (wake.ok()) {
                core->shutdown_accept_wake_fd_ = *wake;
                wake_status = absl::OkStatus();
                break;
              }
              wake_status = wake.status();
            }
            if (!wake_status.ok()) {
              spdlog::critical(
                  "cannot wake data-control accept loop for shutdown: {}",
                  wake_status.message());
              // Returning would strand Shutdown's join forever, while
              // pretending the accept task drained would destroy live
              // coroutine state later. Fail closed on this resource-exhausted
              // process-teardown path.
              std::terminate();
            }
          } else {
            (void)core->listener_.Close();
          }
          if (core->worker_ != nullptr) {
            // Close callbacks may retire sessions once this mailbox turn
            // yields; iterate a stable snapshot instead of coupling
            // correctness to BeginClose's current non-reentrant behavior.
            const std::vector<bycorf::Connection*> sessions = core->sessions_;
            for (bycorf::Connection* connection : sessions) {
              CloseConnectionNow(*core->worker_, connection,
                                 absl::CancelledError("data-control shutdown"));
            }
          }
        }
        NotifyShutdownDrained(*core);
      })) {
    // Notify may reject for allocation failure as well as Runtime teardown.
    // Production stops foreign ingress only after this synchronous drain; a
    // rejection before completion therefore cannot be reported as a
    // successful shutdown without leaving authority-bearing work alive.
    if (core->shutdown_complete_.load(std::memory_order_acquire)) return;
    std::terminate();
  }
  done.wait();
  core->shutdown_complete_.store(true, std::memory_order_release);
}

absl::Status MetaDataControlServer::status() const {
  std::lock_guard<std::mutex> lock(core_->status_mu_);
  return core_->status_;
}

MetaDataControlMetricsSnapshot MetaDataControlServer::metrics() const noexcept {
  return MetaDataControlMetricsSnapshot{
      .live_session_tasks_ =
          core_->live_session_tasks_.load(std::memory_order_relaxed),
      .live_authority_session_tasks_ =
          core_->live_authority_session_tasks_.load(std::memory_order_relaxed),
      .live_leader_tasks_ =
          core_->live_leader_tasks_.load(std::memory_order_relaxed),
      .active_sessions_ =
          core_->active_sessions_.load(std::memory_order_relaxed),
      .accepted_sessions_ =
          core_->accepted_sessions_.load(std::memory_order_relaxed),
      .redirected_sessions_ =
          core_->redirected_sessions_.load(std::memory_order_relaxed),
      .rejected_sessions_ =
          core_->rejected_sessions_.load(std::memory_order_relaxed),
      .protocol_errors_ =
          core_->protocol_errors_.load(std::memory_order_relaxed),
      .full_states_sent_ =
          core_->full_states_sent_.load(std::memory_order_relaxed),
      .lease_grants_ = core_->lease_grants_.load(std::memory_order_relaxed),
      .lease_denials_ = core_->lease_denials_.load(std::memory_order_relaxed),
      .observations_accepted_ =
          core_->observations_accepted_.load(std::memory_order_relaxed),
      .observations_rejected_ =
          core_->observations_rejected_.load(std::memory_order_relaxed),
      .directive_results_committed_ =
          core_->directive_results_committed_.load(std::memory_order_relaxed),
  };
}

void MetaDataControlServer::Start(MetaLeaderContext& context) {
  StartOnExecutor(&context);
}

void MetaDataControlServer::StartOnExecutor(MetaLeaderContext* context) {
  CorePtr core = core_;
  if (!core->foreign_executor_.Notify([core, context]() noexcept {
        if (core->shutdown_) return;
        if (context == nullptr) std::terminate();
        bycorf::Worker* worker = bycorf::ThisWorker().self_;
        if (core->worker_ == nullptr) core->worker_ = worker;
        auto commit_signal = std::make_shared<SessionCommitSignal>();
        bycorf::ForeignExecutor commit_executor = core->foreign_executor_;
        MetaSubscriptionStart commit_start = context->SubscribeCommitted(
            [commit_signal, commit_executor,
             worker](const MetaCommitEvent& event) mutable {
              if (PublishCommitIndex(*commit_signal, event.log_index_) &&
                  !commit_executor.Notify([commit_signal, worker]() noexcept {
                    commit_signal->changed_.NotifyAll(*worker);
                  })) {
                commit_signal->delivery_failed_.store(
                    true, std::memory_order_release);
              }
            });
        (void)PublishCommitIndex(*commit_signal,
                                 commit_start.view_.applied_index());
        core->committed_view_cache_->Adopt(std::move(commit_start.view_));
        core->leader_commit_subscription_ =
            std::shared_ptr<MetaCommitSubscription>(
                std::move(commit_start.subscription_));
        core->leader_commit_signal_ = std::move(commit_signal);
        ++core->leadership_generation_;
        if (core->leadership_generation_ == 0) ++core->leadership_generation_;
        core->options_.runtime_status_->BeginLeadership(
            core->leadership_generation_);
        core->lease_handoff_guard_->Reset();
        core->leader_runtime_guard_->Reset(cluster::LeaseClockMillis(),
                                           ActiveClockMillis());
        core->leader_context_ = context;
        core->leader_active_ = true;
        core->leader_ready_for_data_ = false;
        StartLeaderTask(*core, core->leadership_generation_);
        worker->Spawn(
            ReconcileLocalMetaMember(core, core->leadership_generation_));
      })) {
    // The coordinator has already committed this reconciler's Start edge and
    // will not replay it during the same leader epoch. Returning here would
    // silently leave data control unavailable until another role change.
    std::terminate();
  }
}

void MetaDataControlServer::CancelAndWait() {
  CorePtr core = core_;
  // Process teardown closes and joins the entire server before stopping the
  // Runtime. A later coordinator cancellation is already satisfied and must
  // not attempt to enqueue into that stopped executor.
  if (core->shutdown_complete_.load(std::memory_order_acquire)) return;
  auto complete = std::make_shared<std::promise<void>>();
  std::future<void> done = complete->get_future();
  if (!core->foreign_executor_.Notify([core, complete]() noexcept {
        const std::uint64_t cancelled_generation = core->leadership_generation_;
        core->leader_active_ = false;
        core->leader_ready_for_data_ = false;
        core->leader_context_ = nullptr;
        core->leader_commit_subscription_.reset();
        core->leader_commit_signal_.reset();
        core->options_.runtime_status_->EndLeadership(cancelled_generation);
        core->generation_drain_waiters_[cancelled_generation].push_back(
            complete);
        if (core->worker_ != nullptr) {
          std::vector<bycorf::Connection*> sessions;
          for (const auto& [connection, generation] :
               core->authority_session_generation_) {
            if (generation == cancelled_generation) {
              sessions.push_back(connection);
            }
          }
          for (bycorf::Connection* connection : sessions) {
            CloseConnectionNow(*core->worker_, connection,
                               absl::CancelledError("Meta leadership changed"));
          }
        }
        NotifyGenerationDrained(*core, cancelled_generation);
      })) {
    // Demotion is an uncancellable authority barrier. Treating allocation or
    // executor rejection as success could let a later leader Start reuse the
    // old generation and sessions before the handoff quarantine is reset.
    if (core->shutdown_complete_.load(std::memory_order_acquire)) return;
    std::terminate();
  }
  done.wait();
}

bycorf::Task<absl::Status> MetaDataControlServer::AcceptLoop(CorePtr core) {
  while (core->listening_) {
    auto accepted = co_await core->listener_.Accept();
    if (!accepted.ok()) {
      if (!core->listening_) break;
      const absl::Status slept =
          co_await bycorf::SleepFor(*core->worker_, 10ms);
      if (!slept.ok() && !core->listening_) break;
      continue;
    }
    bycorf::Connection* connection = *accepted;
    if (!core->listening_) {
      CloseConnectionNow(
          *core->worker_, connection,
          absl::CancelledError("data-control listener is shutting down"));
      if (core->shutdown_accept_wake_fd_ >= 0) {
        (void)::shutdown(core->shutdown_accept_wake_fd_, SHUT_RDWR);
        (void)::close(core->shutdown_accept_wake_fd_);
        core->shutdown_accept_wake_fd_ = -1;
      }
      (void)core->listener_.Close();
      break;
    }
    auto handshake_permit = core->pending_handshakes_->TryAcquire();
    if (!handshake_permit.has_value()) {
      core->rejected_sessions_.fetch_add(1, std::memory_order_relaxed);
      CloseConnectionNow(*core->worker_, connection,
                         absl::ResourceExhaustedError(
                             "too many pending data-control handshakes"));
      continue;
    }
    // Frame ownership closes the accept/shutdown race: even if Spawn rejects
    // the task before its body runs, destruction unregisters the session and
    // releases its storage borrow.
    core->worker_->Spawn(
        SessionLoop(core, bycorf::TcpStream(connection), connection,
                    std::move(*handshake_permit),
                    SessionConnectionBorrow(core, connection)));
  }
  if (core->shutdown_accept_wake_fd_ >= 0) {
    (void)::shutdown(core->shutdown_accept_wake_fd_, SHUT_RDWR);
    (void)::close(core->shutdown_accept_wake_fd_);
    core->shutdown_accept_wake_fd_ = -1;
  }
  (void)core->listener_.Close();
  core->accept_loop_running_ = false;
  NotifyShutdownDrained(*core);
  co_return absl::OkStatus();
}

bycorf::Task<absl::Status> MetaDataControlServer::SessionLoop(
    CorePtr core, bycorf::TcpStream stream, bycorf::Connection* connection,
    detail::PendingHandshakeLimiter::Permit handshake_permit,
    SessionConnectionBorrow borrow) {
  // This frame-owned parameter keeps the task registered and the Connection
  // storage pinned through final suspend, including when shutdown prevents
  // the coroutine body from starting.
  (void)borrow;
  std::string node_id;
  std::optional<MetaObservationIdentity> observation_identity;
  std::optional<control::WireId128> status_session_id;
  bool accepted_session = false;
  bool redirected_session = false;
  // Declared before every body-local transport/subscription object so its
  // destructor clears semantic bindings only after the commit subscription
  // and writer stop touching the leader context. The frame-owned borrow keeps
  // the task registered through final suspend.
  struct SessionCompletionGuard {
    CorePtr core_;
    bycorf::Connection* connection_;
    std::string* node_id_;
    std::optional<MetaObservationIdentity>* observation_identity_;
    std::optional<control::WireId128>* status_session_id_;
    bool* accepted_;
    detail::PendingHandshakeLimiter::Permit* handshake_permit_;
    ~SessionCompletionGuard() {
      // Release while core_ still pins the limiter. The ordinary authenticated
      // path released earlier; Permit::Release is intentionally idempotent.
      handshake_permit_->Release();
      if (*accepted_) {
        core_->active_sessions_.fetch_sub(1, std::memory_order_relaxed);
      }
      if (observation_identity_->has_value()) {
        core_->observations_->InvalidateCandidateOnDisconnect(
            **observation_identity_, NowUnixMillis());
      }
      RemoveSessionBindings(*core_, connection_, *node_id_,
                            status_session_id_->has_value()
                                ? &status_session_id_->value()
                                : nullptr);
    }
  } completion{core,
               connection,
               &node_id,
               &observation_identity,
               &status_session_id,
               &accepted_session,
               &handshake_permit};
  SessionIo io(
      *core->worker_, connection, stream, core->options_.max_write_queue_bytes_,
      std::chrono::milliseconds(core->options_.session_progress_timeout_ms_),
      detail::EstablishedSessionReadTimeout(
          core->options_.observation_ttl_ms_,
          core->options_.session_progress_timeout_ms_));
  const auto finish = [&](absl::Status status, bool protocol_error = false) {
    if (protocol_error) {
      core->protocol_errors_.fetch_add(1, std::memory_order_relaxed);
    }
    if (!accepted_session && !redirected_session && !status.ok()) {
      core->rejected_sessions_.fetch_add(1, std::memory_order_relaxed);
    }
    (void)stream.Close();
    return status;
  };
  if (absl::Status prepared = io.Prepare(); !prepared.ok()) {
    co_return finish(prepared);
  }

  auto handshake_deadline = ArmDeadline(
      *core->worker_, connection, kHandshakeTimeout, "TLS and ClientHello");
  std::optional<MetaPrincipalIdentity> tls_identity;
  if (core->tls_context_ != nullptr) {
    const absl::Status tls =
        co_await stream.StartTls(core->tls_context_, /*server=*/true);
    if (!tls.ok()) {
      handshake_deadline->complete_ = true;
      co_return finish(tls);
    }
    auto sans = stream.PeerCertificateUriSans();
    if (!sans.ok()) {
      handshake_deadline->complete_ = true;
      co_return finish(sans.status());
    }
    if (sans->size() != 1) {
      handshake_deadline->complete_ = true;
      co_return finish(absl::UnauthenticatedError(
          "data-control client must present exactly one URI SAN"));
    }
    auto identity = AuthenticateMetaUriSans(*sans);
    if (!identity.ok() || identity->role_ != MetaPrincipalRole::kDataNode) {
      handshake_deadline->complete_ = true;
      co_return finish(
          identity.ok() ? absl::PermissionDeniedError(
                              "control certificate is not a data-node identity")
                        : identity.status());
    }
    tls_identity = std::move(*identity);
  }

  auto hello_message = co_await io.ReadHandshake();
  handshake_deadline->complete_ = true;
  if (!hello_message.ok()) co_return finish(hello_message.status(), true);
  const auto* hello = std::get_if<control::ClientHello>(&*hello_message);
  if (hello == nullptr) {
    co_return finish(absl::InvalidArgumentError("expected ClientHello"), true);
  }
  if (absl::Status valid = ValidateHello(*hello); !valid.ok()) {
    co_return finish(valid, true);
  }
  node_id = hello->node_id;
  if (tls_identity.has_value() && tls_identity->subject_id_ != node_id) {
    co_return finish(absl::PermissionDeniedError(
        "data-node certificate does not match ClientHello"));
  }

  auto cached_view =
      CommittedViewAtLeast(*core, core->coordinator_->CommittedHighWater());
  if (!cached_view.ok()) co_return finish(cached_view.status());
  std::shared_ptr<const MetaCommittedView> view = *cached_view;
  const auto node = view->identity().FindNode(node_id);
  if (!node.has_value() || node->retired_) {
    if (!node.has_value() && ActiveClusterCreateDeclaresNode(*view, node_id)) {
      core->options_.runtime_status_->NoteUnregisteredRetry(
          node_id, core->leadership_generation_);
    }
    co_return finish(absl::PermissionDeniedError(
        "data node is not in the active committed registry"));
  }
  if (tls_identity.has_value() &&
      tls_identity->principal_ != node->principal_) {
    co_return finish(absl::PermissionDeniedError(
        "data-node certificate does not match its committed binding"));
  }
  auto directory = BuildCommittedMetaDirectory(*view);
  if (!directory.ok()) co_return finish(directory.status());
  const bool accepted_leader =
      core->leader_ready_for_data_ &&
      AuthoritySessionsAllowed(*core, core->leadership_generation_);
  if (!accepted_leader) {
    const absl::Status sent =
        co_await io.Send(control::MessagePriority::kReliable,
                         control::WireMessage(BuildServerHello(
                             *core, std::move(*directory), false)));
    core->redirected_sessions_.fetch_add(1, std::memory_order_relaxed);
    redirected_session = true;
    // The completion guard releases the permit only after the bounded write
    // completes, so repeated valid Hellos cannot accumulate redirect tasks.
    co_return finish(sent);
  }

  // One leader-scoped subscription fans out an O(1) cursor notification to
  // every session. Refresh through the shared immutable cache here so both a
  // commit racing the first registry read and a follower snapshot install are
  // represented without retaining a private MetaStores copy per connection.
  const std::uint64_t leadership_generation = core->leadership_generation_;
  std::shared_ptr<SessionCommitSignal> commit_signal =
      core->leader_commit_signal_;
  std::shared_ptr<MetaCommitSubscription> commit_subscription =
      core->leader_commit_subscription_;
  if (commit_signal == nullptr || commit_subscription == nullptr) {
    co_return finish(
        absl::CancelledError("Meta leadership subscription is unavailable"));
  }
  cached_view =
      CommittedViewAtLeast(*core, core->coordinator_->CommittedHighWater());
  if (!cached_view.ok()) co_return finish(cached_view.status());
  view = *cached_view;

  // The first registry/directory read was sufficient for a follower redirect,
  // but an accepted session must bind its identity and Hello to the same
  // atomic view used by its initial desired-state projection.
  const auto accepted_node = view->identity().FindNode(node_id);
  if (!accepted_node.has_value() || accepted_node->retired_) {
    co_return finish(absl::PermissionDeniedError(
        "data node left the active committed registry during handshake"));
  }
  if (tls_identity.has_value() &&
      (tls_identity->subject_id_ != node_id ||
       tls_identity->principal_ != accepted_node->principal_)) {
    co_return finish(absl::PermissionDeniedError(
        "data-node certificate no longer matches its committed binding"));
  }
  directory = BuildCommittedMetaDirectory(*view);
  if (!directory.ok()) co_return finish(directory.status());
  auto boot_id = ParseIdentity<20>(hello->boot_id, "data boot id");
  if (!boot_id.ok()) co_return finish(boot_id.status(), true);
  auto replication_history_id = ParseIdentity<20>(
      hello->replication_history_id, "data replication history id");
  if (!replication_history_id.ok()) {
    co_return finish(replication_history_id.status(), true);
  }
  // A committed node owns at most one leader-session setup or live session.
  // First-owner semantics keep a stalled incumbent bounded; the Data client
  // retries after the incumbent exits under its progress deadline.
  if (!core->bound_node_sessions_.TryClaim(node_id, connection)) {
    co_return finish(absl::AlreadyExistsError(
        "data node already has a bound control session"));
  }
  if (absl::Status bound =
          BindAuthoritySession(*core, connection, leadership_generation);
      !bound.ok()) {
    co_return finish(bound);
  }
  // From this point the per-node slot, leadership-generation registry, and
  // completion guard bound all projection/FDS ownership, so anonymous setup
  // capacity can be reused safely.
  handshake_permit.Release();

  auto projected = ProjectNodeBounded(*core, *view, node_id);
  if (!projected.ok()) co_return finish(projected.status());
  std::shared_ptr<const NodeControlBatch> batch =
      RetainProjection(std::move(*projected));
  view.reset();
  auto session_id = control::GenerateId128();
  if (!session_id.ok()) co_return finish(session_id.status());
  status_session_id = *session_id;
  std::uint64_t& next_generation = core->next_session_generation_[node_id];
  if (next_generation == std::numeric_limits<std::uint64_t>::max()) {
    co_return finish(absl::ResourceExhaustedError(
        "data-node session generation is exhausted"));
  }
  const std::uint64_t session_generation = ++next_generation;
  observation_identity =
      MetaObservationIdentity{node_id, *boot_id, session_generation};
  const absl::Status adopted = core->observations_->AdoptSession(
      *observation_identity, NowUnixMillis(), *replication_history_id);
  if (!adopted.ok()) co_return finish(adopted);

  std::deque<control::WireMessage> deferred;
  std::vector<control::WireAuthorityAnchor> fenced_authorities;
  const std::size_t max_deferred_messages =
      core->options_.max_write_queue_bytes_ / control::kMaxFrameBytes;

  if (absl::Status sent =
          co_await io.Send(control::MessagePriority::kReliable,
                           control::WireMessage(BuildServerHello(
                               *core, std::move(*directory), true, *session_id,
                               session_generation)));
      !sent.ok()) {
    co_return finish(sent);
  }
  if (absl::Status sent = co_await SendFullState(core, io, batch, node_id,
                                                 leadership_generation);
      !sent.ok()) {
    co_return finish(sent);
  }
  core->full_states_sent_.fetch_add(1, std::memory_order_relaxed);
  if (absl::Status applied = co_await AwaitApplied(io, *batch); !applied.ok()) {
    co_return finish(applied, true);
  }
  std::uint64_t validated_committed_high_water = 0;
  if (absl::Status current = co_await ValidateBootstrapApplied(
          core, io, *batch, node_id, hello->boot_id, *session_id,
          leadership_generation, *commit_signal, *commit_subscription,
          &validated_committed_high_water, &deferred, max_deferred_messages,
          &fenced_authorities);
      !current.ok()) {
    co_return finish(current);
  }
  auto live = std::make_shared<LiveSessionState>();
  live->core_ = core;
  live->worker_ = core->worker_;
  live->connection_ = connection;
  live->io_ = &io;
  live->commit_subscription_ = std::move(commit_subscription);
  live->commit_signal_ = commit_signal;
  live->node_id_ = node_id;
  live->boot_id_ = hello->boot_id;
  live->session_id_ = *session_id;
  live->replication_history_id_ = *replication_history_id;
  live->replication_flow_count_ = hello->replication_flow_count;
  live->session_generation_ = session_generation;
  live->leadership_generation_ = leadership_generation;
  // Transfer the sole retained-projection owner into live session state. The
  // SessionLoop coroutine frame outlives the publisher, so copying here would
  // pin the initial generation after every later replacement.
  live->installed_ = std::move(batch);
  live->selected_ =
      control::SelectNodeControlState(live->installed_->full_state, node_id);
  live->validated_committed_high_water_ = validated_committed_high_water;
  live->fenced_authorities_ = std::move(fenced_authorities);
  const std::weak_ptr<LiveSessionState> weak_live = live;
  live->fence_ack_deadline_ =
      std::make_unique<control::ControlDeadlineWatchdog>(
          *live->worker_, [weak_live] {
            if (const auto state = weak_live.lock()) {
              FailLiveSession(
                  state, absl::DeadlineExceededError(
                             "FenceAck made no progress before its deadline"));
            }
          });
  live->applied_ack_deadline_ =
      std::make_unique<control::ControlDeadlineWatchdog>(
          *live->worker_, [weak_live] {
            if (const auto state = weak_live.lock()) {
              FailLiveSession(
                  state, absl::DeadlineExceededError(
                             "FullStateApplied made no progress before its "
                             "deadline"));
            }
          });
  live->inbound_transfer_deadline_ =
      std::make_unique<control::ControlDeadlineWatchdog>(
          *live->worker_, [weak_live] {
            if (const auto state = weak_live.lock()) {
              FailLiveSession(
                  state,
                  absl::DeadlineExceededError(
                      "inbound control object made no progress before its "
                      "deadline"));
            }
          });
  core->options_.runtime_status_->PublishCurrent(
      node_id, hello->boot_id, *session_id, *replication_history_id,
      hello->replication_flow_count, session_generation, leadership_generation,
      live->validated_committed_high_water_, live->installed_->full_state);
  core->accepted_sessions_.fetch_add(1, std::memory_order_relaxed);
  core->active_sessions_.fetch_add(1, std::memory_order_relaxed);
  accepted_session = true;

  // The reader is SessionLoop's awaited child. Publisher and directive tasks
  // are explicitly joined below before the borrowed SessionIo and commit
  // subscription can be destroyed.
  live->publisher_running_ = true;
  ++live->active_tasks_;
  live->worker_->Spawn(RunSessionPublisher(live));
  absl::Status session_status =
      co_await RunEstablishedSession(live, *boot_id, *replication_history_id,
                                     session_generation, std::move(deferred));

  live->closing_ = true;
  if (!live->terminal_error_.has_value() && !session_status.ok()) {
    live->terminal_error_ = session_status;
  }
  live->commit_signal_->changed_.NotifyAll(*live->worker_);
  live->response_changed_.NotifyAll(*live->worker_);
  CloseConnectionNow(*live->worker_, live->connection_, session_status);
  while (live->active_tasks_ != 0) {
    co_await live->tasks_changed_.Wait();
  }
  const bool protocol_error =
      session_status.code() == absl::StatusCode::kInvalidArgument ||
      session_status.code() == absl::StatusCode::kFailedPrecondition ||
      session_status.code() == absl::StatusCode::kAlreadyExists ||
      session_status.code() == absl::StatusCode::kPermissionDenied ||
      session_status.code() == absl::StatusCode::kOutOfRange;
  if (protocol_error) {
    core->protocol_errors_.fetch_add(1, std::memory_order_relaxed);
  }
  // CloseConnectionNow above deliberately runs before joining the detached
  // publisher/directive tasks. The session borrow keeps storage live during
  // that drain; returning directly still records that the worker owns
  // transport retirement and avoids a redundant stream close.
  co_return session_status;
}

}  // namespace lavik::meta
