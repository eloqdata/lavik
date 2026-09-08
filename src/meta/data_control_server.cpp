#include "keylane/meta/data_control_server.h"

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
#include "celer/io/storage.h"
#include "celer/net/connection.h"
#include "celer/net/tcp_listener.h"
#include "celer/net/tcp_stream.h"
#include "celer/net/tls.h"
#include "celer/runtime/sync.h"
#include "celer/runtime/worker.h"
#include "keylane/cluster/control_transport.h"
#include "keylane/meta/control_projector.h"
#include "keylane/meta/hash.h"
#include "keylane/meta/identity_verifier.h"
#include "keylane/meta/observation_store.h"
#include "keylane/numeric_endpoint.h"
#include "spdlog/spdlog.h"

// NuRaft's headers are not -Wpedantic-clean.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include "libnuraft/cluster_config.hxx"
#include "libnuraft/raft_server.hxx"
#pragma GCC diagnostic pop

namespace keylane::meta {
namespace {

namespace control = keylane::cluster::control;
using namespace std::chrono_literals;

constexpr auto kHandshakeTimeout = 10s;
constexpr std::uint32_t kMaxSessionProgressTimeoutMs = 10'000;

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
  auto endpoint = keylane::ParseNumericEndpoint(member.data_control_endpoint_);
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

class ViewFacts final : public MetaCommittedFacts {
 public:
  explicit ViewFacts(const MetaCommittedView& view) : view_(view) {}

  bool IsActiveNode(std::string_view node_id) const override {
    return view_.identity().IsActiveNode(std::string(node_id));
  }
  std::uint64_t CurrentGroupTerm(std::string_view group_id) const override {
    return view_.grant().CurrentGroupTerm(group_id).value_or(0);
  }
  std::uint64_t CurrentPopulationManifestRevision(
      std::string_view group_id) const override {
    const auto group = view_.topology().FindGroup(std::string(group_id));
    return group.has_value() ? group->record_.population_manifest_revision_ : 0;
  }
  std::uint64_t CurrentPartitionReplicationEpoch(
      std::string_view group_id) const override {
    const auto group = view_.topology().FindGroup(std::string(group_id));
    return group.has_value() ? group->record_.partition_replication_epoch_ : 0;
  }
  bool AssignmentMatches(std::string_view group_id, std::string_view node_id,
                         const MetaAssignmentId& assignment_id) const override {
    const auto group = view_.topology().FindGroup(std::string(group_id));
    return group.has_value() &&
           std::any_of(group->members_.begin(), group->members_.end(),
                       [&](const MetaGroupMember& member) {
                         return member.node_id_ == node_id &&
                                member.assignment_id_ == assignment_id;
                       });
  }
  bool OperationNonTerminal(const MetaOperationId& id) const override {
    const auto operation = view_.operation().FindOperation(id);
    return operation.has_value() && !IsTerminal(operation->lifecycle_);
  }
  bool HistoryBoundToOperation(
      const MetaOperationId& id,
      const MetaReplicationHistoryId& history_id) const override {
    const auto operation = view_.operation().FindOperation(id);
    return operation.has_value() &&
           !IsZero(operation->replication_history_id_) &&
           operation->replication_history_id_ == history_id;
  }

 private:
  const MetaCommittedView& view_;
};

std::int64_t NowUnixMillis() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

std::int64_t NowMonotonicMillis() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
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
  celer::AsyncNotification changed_;
};

// Celer's current TcpListener::Close closes the listening fd but does not
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

celer::Task<absl::Status> CloseAtDeadline(celer::Worker& worker,
                                          celer::Connection* connection,
                                          std::shared_ptr<DeadlineState> state,
                                          std::chrono::milliseconds timeout,
                                          std::string operation) {
  const absl::Status slept = co_await celer::SleepFor(worker, timeout);
  if (slept.ok() && !state->complete_) {
    worker.BeginClose(
        connection,
        absl::DeadlineExceededError(absl::StrCat(operation, " timed out")),
        celer::CloseMode::kIdleTimeout);
  }
  co_return slept;
}

std::shared_ptr<DeadlineState> ArmDeadline(celer::Worker& worker,
                                           celer::Connection* connection,
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
  SessionIo(celer::Worker& worker, celer::Connection* connection,
            celer::TcpStream& stream, std::size_t queue_bytes,
            std::chrono::milliseconds progress_timeout)
      : worker_(worker),
        connection_(connection),
        progress_timeout_(progress_timeout),
        frames_(stream, progress_timeout),
        writer_(frames_, queue_bytes),
        read_deadline_(worker, [&worker, connection] {
          worker.BeginClose(
              connection,
              absl::DeadlineExceededError(
                  "control session read progress timed out"),
              celer::CloseMode::kIdleTimeout);
        }) {}

  absl::Status Prepare() noexcept { return frames_.Prepare(); }

  celer::Task<absl::StatusOr<control::WireMessage>> Read() {
    if (absl::Status armed = read_deadline_.Arm(progress_timeout_);
        !armed.ok()) {
      co_return armed;
    }
    auto message = co_await frames_.ReadMessage();
    if (read_deadline_.Disarm()) {
      co_return absl::DeadlineExceededError(
          "control session read progress timed out");
    }
    co_return message;
  }

  // TLS and ClientHello share one outer deadline, so arming a second timer
  // around the Hello would create two close owners for the same connection.
  celer::Task<absl::StatusOr<control::WireMessage>> ReadHandshake() {
    co_return co_await frames_.ReadMessage();
  }

  celer::Task<absl::Status> Send(control::MessagePriority priority,
                                 control::WireMessage message) {
    co_return co_await writer_.Write(priority, std::move(message));
  }

  celer::Task<absl::Status> SendTransfer(
      control::TransferKind kind, std::shared_ptr<const std::string> bytes) {
    auto object_id = control::GenerateId128();
    if (!object_id.ok()) co_return object_id.status();
    co_return co_await writer_.WriteTransfer(kind, *object_id,
                                             std::move(bytes));
  }

  celer::Task<absl::Status> SendFullDesiredState(
      std::shared_ptr<const std::string> encoded) {
    co_return co_await writer_.WriteFullDesiredState(std::move(encoded));
  }

  void FailDeadline(std::string_view operation) {
    worker_.BeginClose(
        connection_,
        absl::DeadlineExceededError(absl::StrCat(operation, " timed out")),
        celer::CloseMode::kIdleTimeout);
  }

 private:
  celer::Worker& worker_;
  celer::Connection* connection_;
  const std::chrono::milliseconds progress_timeout_;
  control::ControlFrameStream frames_;
  control::ControlSessionWriter writer_;
  control::ControlDeadlineWatchdog read_deadline_;
};

// Data-to-Meta large messages remain bounded by their typed protocol caps.
// The reassembler verifies sequence, declared length, and whole-object SHA
// before Commit exposes bytes to the ordinary typed message decoder.
class ClientTransferSink final : public control::LargeObjectSink {
 public:
  absl::Status Begin(const control::TransferStart& start) override {
    if (start.kind != control::TransferKind::kDirectiveResult &&
        start.kind != control::TransferKind::kObservationEvidence) {
      return absl::InvalidArgumentError(
          "Meta accepts only result or evidence client transfers");
    }
    const std::uint64_t limit =
        start.kind == control::TransferKind::kDirectiveResult
            ? control::kMaxDirectiveResultTransferBytes
            : control::kMaxOperationEvidenceTransferBytes;
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
  return applied.source_meta_applied_index ==
             batch.full_state.source_meta_applied_index &&
         applied.projection_hash == batch.full_state.projection_hash &&
         applied.object_hash == batch.full_state.object_hash;
}

control::FullStateApplied AppliedReceipt(const NodeControlBatch& batch) {
  return control::FullStateApplied{
      .source_meta_applied_index = batch.full_state.source_meta_applied_index,
      .projection_hash = batch.full_state.projection_hash,
      .object_hash = batch.full_state.object_hash,
  };
}

control::Directive LiveDirective(const control::WireProjectedDirective& source,
                                 const control::WireId128& session_id) {
  return control::Directive{
      .session_id = session_id,
      .basis = source.basis,
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
      .preconditions = source.preconditions,
      .storage_mutating = source.storage_mutating,
      .force = source.force,
  };
}

control::WireAuthorityAnchor GroupAnchor(
    const control::WireDesiredGroup& group) {
  return control::WireAuthorityAnchor{
      .group_id = group.group_id,
      .assignment_id = *group.owner_assignment_id,
      .group_term = group.group_term,
      .authority_version = group.authority_version,
      .grant_revision = group.grant_revision,
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

celer::Task<absl::Status> FenceSupersededAuthority(
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
                .source_meta_applied_index =
                    installed.full_state.source_meta_applied_index,
                .projection_hash = installed.full_state.projection_hash,
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
      co_return expired
                    ? absl::DeadlineExceededError(
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
        co_return expired
                      ? absl::DeadlineExceededError(
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

celer::Task<absl::Status> SendDirectives(
    SessionIo& io, const NodeControlBatch& batch,
    const control::WireId128& session_id, std::string_view node_id,
    std::string_view boot_id, MetaDirectiveReceiptTracker& receipt_tracker,
    std::function<bool()> should_continue = std::function<bool()>{},
    std::function<void(bool)> transfer_state = std::function<void(bool)>{}) {
  for (const control::WireProjectedDirective& directive :
       batch.full_state.current_directives) {
    if (directive.recipient_node_id != node_id) {
      co_return absl::FailedPreconditionError(
          "node projection contains a directive for another recipient");
    }
    // A durable directive is incarnation-scoped. A different boot must not
    // execute it merely because the stable node id reconnected.
    if (directive.recipient_boot_id != boot_id) continue;
    auto needs_dispatch = receipt_tracker.NeedsDispatch(directive.identity);
    if (!needs_dispatch.ok()) co_return needs_dispatch.status();
    if (!*needs_dispatch) continue;
    if (should_continue && !should_continue()) {
      co_return absl::CancelledError(
          "directive projection was superseded before dispatch completed");
    }
    auto delivery = ClassifyDirectiveDelivery(directive, session_id);
    if (!delivery.ok()) co_return delivery.status();
    const control::Directive live = LiveDirective(directive, session_id);
    absl::Status sent;
    if (*delivery == MetaDirectiveDelivery::kFrame) {
      sent = co_await io.Send(control::MessagePriority::kReliable,
                              control::WireMessage(live));
    } else {
      auto encoded = control::EncodeMessage(control::WireMessage(live));
      if (!encoded.ok()) co_return encoded.status();
      auto owned = std::make_shared<const std::string>(std::move(*encoded));
      if (transfer_state) transfer_state(true);
      sent = co_await io.SendTransfer(control::TransferKind::kDirectivePayload,
                                      std::move(owned));
      if (transfer_state) transfer_state(false);
    }
    if (!sent.ok()) {
      co_return sent;
    }
    if (absl::Status recorded =
            receipt_tracker.MarkDispatched(directive.identity);
        !recorded.ok()) {
      co_return recorded;
    }
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
         status == wire_status && receipt.result_hash_ == result.result_hash &&
         receipt.result_ == result.result;
}

control::ResultCommitted ResultAck(const control::DirectiveResult& result,
                                   const MetaTerminalReceipt& receipt) {
  return control::ResultCommitted{
      .session_id = result.session_id,
      .recipient_boot_id = result.recipient_boot_id,
      .identity = result.identity,
      .result_hash = result.result_hash,
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

std::shared_ptr<const MetaCommittedView>
detail::MetaCommittedViewCache::Adopt(MetaCommittedView view) {
  if (cached_ == nullptr ||
      view.applied_index() >= cached_->applied_index()) {
    cached_ =
        std::make_shared<const MetaCommittedView>(std::move(view));
  }
  return cached_;
}

absl::StatusOr<std::shared_ptr<const MetaCommittedView>>
detail::MetaCommittedViewCache::Get(std::uint64_t minimum_applied_index) {
  if (cached_ != nullptr &&
      cached_->applied_index() >= minimum_applied_index) {
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

MetaHeartbeatObservationResult IngestHeartbeatObservations(
    MetaObservationStore& observations, const MetaCommittedFacts& facts,
    std::string_view node_id, const MetaBootIncarnation& boot,
    const MetaReplicationHistoryId& session_history, std::uint64_t generation,
    const control::HeartbeatHealth& health,
    const std::optional<control::CandidateProgress>& candidate,
    std::int64_t now_unix_ms) {
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

  const absl::Status boot_status =
      observations.Ingest(MetaObservation{.identity_ = identity,
                                          .payload_ = MetaNodeBootObs{},
                                          .received_unix_ms_ = now_unix_ms},
                          facts, now_unix_ms);
  if (!boot_status.ok()) record_rejection("boot", boot_status);

  std::string health_text = absl::StrCat(
      "storage_ready=", health.storage_ready ? 1 : 0,
      ",population_ready=", health.population_ready ? 1 : 0,
      ",draining=", health.draining ? 1 : 0,
      ",active_groups=", health.active_groups, ",summary=", health.summary);
  const absl::Status health_status = observations.Ingest(
      MetaObservation{.identity_ = identity,
                      .payload_ = MetaNodeHealthObs{std::move(health_text)},
                      .received_unix_ms_ = now_unix_ms},
      facts, now_unix_ms);
  if (!health_status.ok()) record_rejection("health", health_status);

  if (candidate.has_value()) {
    auto history = ParseIdentity<20>(candidate->replication_history_id,
                                     "candidate replication history id");
    if (!history.ok()) {
      record_rejection("candidate", history.status());
    } else if (*history != session_history) {
      record_rejection("candidate",
                       absl::FailedPreconditionError(
                           "replication history does not match ClientHello"));
    } else {
      MetaCandidateProgressObs progress{
          .node_id_ = std::string(node_id),
          .boot_incarnation_ = boot,
          .group_id_ = candidate->group_id,
          .assignment_id_ = candidate->assignment_id,
          .group_term_ = candidate->group_term,
          .population_manifest_revision_ = candidate->manifest_revision,
          .partition_replication_epoch_ =
              candidate->partition_replication_epoch,
          .replication_history_id_ = *history,
          .applied_flow_vector_ = candidate->applied_flow_vector,
          .backlog_coverage_ = candidate->backlog_coverage,
          .readiness_ = candidate->readiness,
      };
      const absl::Status candidate_status =
          observations.Ingest(MetaObservation{.identity_ = identity,
                                              .payload_ = std::move(progress),
                                              .received_unix_ms_ = now_unix_ms},
                              facts, now_unix_ms);
      if (!candidate_status.ok()) {
        record_rejection("candidate", candidate_status);
      }
    }
  }
  return result;
}

absl::Status IngestOperationEvidenceObservation(
    MetaObservationStore& observations, const MetaCommittedFacts& facts,
    std::string_view node_id, const MetaBootIncarnation& boot,
    std::uint64_t generation, const control::WireId128& session_id,
    const control::OperationEvidence& evidence, std::int64_t now_unix_ms) {
  if (evidence.session_id != session_id) {
    return absl::InvalidArgumentError("operation evidence session id mismatch");
  }
  auto reporter_boot =
      ParseIdentity<20>(evidence.reporter_boot_id, "evidence reporter boot id");
  if (!reporter_boot.ok()) return reporter_boot.status();
  if (*reporter_boot != boot) {
    return absl::InvalidArgumentError(
        "operation evidence reporter boot mismatch");
  }
  if (evidence.evidence_hash != control::ComputeSha256(evidence.evidence)) {
    return absl::DataLossError("operation evidence content hash mismatch");
  }
  auto history = ParseIdentity<20>(evidence.replication_history_id,
                                   "evidence replication history id");
  if (!history.ok()) return history.status();

  (void)observations.MaybeSweepExpired(now_unix_ms);
  MetaOperationEvidenceObs payload{
      .node_id_ = std::string(node_id),
      .boot_incarnation_ = boot,
      .assignment_id_ = evidence.assignment_id,
      .operation_id_ = evidence.operation_id,
      .kind_phase_ = evidence.kind_phase,
      .evidence_hash_ = evidence.evidence_hash,
      .evidence_ = evidence.evidence,
      .group_id_ = evidence.group_id,
      .group_term_ = evidence.group_term,
      .population_manifest_revision_ = evidence.manifest_revision,
      .partition_replication_epoch_ = evidence.partition_replication_epoch,
      .replication_history_id_ = *history,
  };
  return observations.Ingest(
      MetaObservation{
          .identity_ =
              MetaObservationIdentity{std::string(node_id), boot, generation},
          .payload_ = std::move(payload),
          .received_unix_ms_ = now_unix_ms,
      },
      facts, now_unix_ms);
}

absl::StatusOr<MetaDirectiveDelivery> ClassifyDirectiveDelivery(
    const control::WireProjectedDirective& directive,
    const control::WireId128& session_id) {
  auto encoded = control::EncodeMessage(
      control::WireMessage(LiveDirective(directive, session_id)));
  if (!encoded.ok()) return encoded.status();
  if (encoded->size() > control::kMaxDirectiveTransferBytes) {
    return absl::ResourceExhaustedError(
        "encoded directive exceeds its transfer cap");
  }
  return encoded->size() <= control::kMaxFramePayloadBytes
             ? MetaDirectiveDelivery::kFrame
             : MetaDirectiveDelivery::kTransfer;
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
  return replacement.projection_hash == latest.projection_hash
             ? MetaReplacementDisposition::kContinue
             : MetaReplacementDisposition::kAbortSuperseded;
}

absl::Status MetaDirectiveReceiptTracker::Rebuild(
    std::span<const control::WireProjectedDirective> directives,
    std::string_view node_id, std::string_view boot_id) {
  std::vector<Entry> replacement;
  replacement.reserve(directives.size());
  for (const control::WireProjectedDirective& directive : directives) {
    if (directive.recipient_node_id != node_id) {
      return absl::FailedPreconditionError(
          "node projection contains a receipt identity for another node");
    }
    if (directive.recipient_boot_id != boot_id) continue;
    if (std::any_of(replacement.begin(), replacement.end(),
                    [&](const Entry& entry) {
                      return entry.identity_ == directive.identity;
                    })) {
      return absl::FailedPreconditionError(
          "node projection contains duplicate directive receipt identity");
    }
    replacement.push_back(Entry{.identity_ = directive.identity});
  }
  entries_ = std::move(replacement);
  return absl::OkStatus();
}

absl::StatusOr<bool> MetaDirectiveReceiptTracker::NeedsDispatch(
    const control::WireDirectiveIdentity& identity) const {
  const auto entry = std::find_if(
      entries_.begin(), entries_.end(),
      [&](const Entry& value) { return value.identity_ == identity; });
  if (entry == entries_.end()) {
    return absl::FailedPreconditionError(
        "directive dispatch identity is not in the installed projection");
  }
  return !entry->dispatched_;
}

absl::Status MetaDirectiveReceiptTracker::MarkDispatched(
    const control::WireDirectiveIdentity& identity) {
  const auto entry = std::find_if(
      entries_.begin(), entries_.end(),
      [&](const Entry& value) { return value.identity_ == identity; });
  if (entry == entries_.end()) {
    return absl::FailedPreconditionError(
        "directive dispatch identity is not in the installed projection");
  }
  entry->dispatched_ = true;
  return absl::OkStatus();
}

bool MetaDirectiveReceiptTracker::HasUndispatched() const noexcept {
  return std::any_of(entries_.begin(), entries_.end(),
                     [](const Entry& entry) { return !entry.dispatched_; });
}

absl::Status MetaDirectiveReceiptTracker::Observe(
    const control::WireDirectiveIdentity& identity,
    control::DirectiveReceiptStage stage) {
  const std::uint8_t next = static_cast<std::uint8_t>(stage);
  if (next < static_cast<std::uint8_t>(
                 control::DirectiveReceiptStage::kAccepted) ||
      next > static_cast<std::uint8_t>(
                 control::DirectiveReceiptStage::kCompleted)) {
    return absl::InvalidArgumentError("directive receipt stage is unknown");
  }
  const auto entry = std::find_if(
      entries_.begin(), entries_.end(),
      [&](const Entry& value) { return value.identity_ == identity; });
  if (entry == entries_.end()) {
    return absl::FailedPreconditionError(
        "directive receipt identity is not in the installed projection");
  }
  if (entry->stage_ == next) return absl::OkStatus();
  const bool rejected_before_start =
      entry->stage_ == static_cast<std::uint8_t>(
                           control::DirectiveReceiptStage::kAccepted) &&
      stage == control::DirectiveReceiptStage::kCompleted;
  if (!rejected_before_start && next != entry->stage_ + 1) {
    return absl::FailedPreconditionError(
        "directive receipt stage skipped or moved backwards");
  }
  if (stage == control::DirectiveReceiptStage::kStarted) {
    entry->started_ = true;
  }
  entry->stage_ = next;
  return absl::OkStatus();
}

absl::Status MetaDirectiveReceiptTracker::ValidateResult(
    const control::WireDirectiveIdentity& identity,
    control::DirectiveResultStatus status) const {
  const auto entry = std::find_if(
      entries_.begin(), entries_.end(),
      [&](const Entry& value) { return value.identity_ == identity; });
  if (entry == entries_.end() ||
      entry->stage_ != static_cast<std::uint8_t>(
                           control::DirectiveReceiptStage::kCompleted)) {
    return absl::FailedPreconditionError(
        "directive result arrived before its Completed receipt");
  }
  const bool result_started =
      status != control::DirectiveResultStatus::kRejected;
  if (entry->started_ != result_started) {
    return absl::FailedPreconditionError(
        "directive result status conflicts with its receipt path");
  }
  return absl::OkStatus();
}

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
      .heartbeat_interval_ms = 1,
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

absl::StatusOr<MetaLocalMemberBindingDisposition>
EvaluateLocalMetaMemberBinding(const MetaCommittedView& view,
                               std::uint32_t server_id,
                               std::string_view principal,
                               std::string_view data_control_endpoint) {
  const auto existing = view.identity().FindMetaMember(server_id);
  if (!existing.has_value()) {
    return MetaLocalMemberBindingDisposition::kNeedsBind;
  }
  if (existing->retired_) {
    return absl::FailedPreconditionError(
        "local Meta-member identity binding is retired");
  }
  if (existing->principal_ != principal ||
      existing->data_control_endpoint_ != data_control_endpoint) {
    return absl::FailedPreconditionError(
        "local Meta-member binding conflicts with process identity");
  }
  return MetaLocalMemberBindingDisposition::kAlreadyBound;
}

control::LeaseDecision EvaluateLeaseChallenge(
    const std::optional<control::LeaseChallenge>& challenge,
    const control::HeartbeatHealth& health,
    const MetaLeaseEvaluation& evaluation) {
  if (!challenge.has_value()) return control::NoChallenge{};
  const control::WireHash256 current_hash =
      evaluation.desired_ == nullptr ? control::WireHash256{}
                                     : evaluation.desired_->projection_hash;
  if (evaluation.desired_ == nullptr ||
      challenge->projection_hash != evaluation.applied_projection_hash_ ||
      challenge->projection_hash != current_hash) {
    return control::LeaseStateOutOfDate{challenge->nonce, current_hash};
  }
  if (!evaluation.leader_valid_) {
    return control::LeaseDenied{
        challenge->nonce, control::LeaseDenialReason::kNotLeader, current_hash};
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
      group->group_term != challenge->group_term ||
      group->authority_version != challenge->authority_version ||
      group->grant_revision != challenge->grant_revision) {
    return control::LeaseDenied{challenge->nonce,
                                control::LeaseDenialReason::kAuthorityMismatch,
                                current_hash};
  }
  if (!group->grant_active || group->grant_duration_ms == 0) {
    return control::LeaseDenied{challenge->nonce,
                                control::LeaseDenialReason::kGrantInactive,
                                current_hash};
  }
  if (!health.storage_ready || !health.population_ready || health.draining) {
    return control::LeaseDenied{challenge->nonce,
                                control::LeaseDenialReason::kNodeNotReady,
                                current_hash};
  }
  const std::uint32_t duration =
      std::min(group->grant_duration_ms, evaluation.leadership_validity_ms_);
  if (duration == 0) {
    return control::LeaseDenied{
        challenge->nonce, control::LeaseDenialReason::kNotLeader, current_hash};
  }
  return control::LeaseGranted{
      .nonce = challenge->nonce,
      .leader_id = evaluation.server_id_,
      .raft_term = evaluation.raft_term_,
      .leadership_generation = evaluation.leadership_generation_,
      .data_boot_id = evaluation.boot_id_,
      .projection_hash = current_hash,
      .group_id = challenge->group_id,
      .assignment_id = challenge->assignment_id,
      .group_term = challenge->group_term,
      .authority_version = challenge->authority_version,
      .grant_revision = challenge->grant_revision,
      .granted_duration_ms = duration,
  };
}

control::LeaseDecision MetaLeaseHandoffGuard::Enforce(
    control::LeaseDecision decision, std::string_view node_id,
    std::int64_t now_monotonic_ms) {
  const auto* grant = std::get_if<control::LeaseGranted>(&decision);
  if (grant == nullptr) return decision;

  const control::WireAuthorityAnchor authority{
      .group_id = grant->group_id,
      .assignment_id = grant->assignment_id,
      .group_term = grant->group_term,
      .authority_version = grant->authority_version,
      .grant_revision = grant->grant_revision,
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
        now_monotonic_ms >
                max_time - static_cast<std::int64_t>(maximum_prior_lease_ms_)
            ? max_time
            : now_monotonic_ms + maximum_prior_lease_ms_;
  }
  if (now_monotonic_ms < entry->eligible_after_ms_) {
    return control::LeaseDenied{
        .nonce = grant->nonce,
        .reason = control::LeaseDenialReason::kAuthorityHandoffPending,
        .current_projection_hash = grant->projection_hash,
    };
  }
  return decision;
}

struct MetaDataControlServer::Core {
  celer::ForeignExecutor foreign_executor_;
  nuraft::ptr<nuraft::raft_server> server_;
  // Non-owning. MetaCoordinator owns this reconciler and therefore outlives
  // every Core access; keeping this edge non-owning avoids a cycle.
  MetaCoordinator* coordinator_ = nullptr;
  std::unique_ptr<detail::MetaCommittedViewCache> committed_view_cache_;
  std::shared_ptr<MetaObservationStore> observations_;
  MetaDataControlServerOptions options_;
  std::shared_ptr<celer::TlsContext> tls_context_;
  std::unique_ptr<MetaLeaseHandoffGuard> lease_handoff_guard_;

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
  celer::Worker* worker_ = nullptr;
  celer::TcpListener listener_;
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
  std::vector<celer::Connection*> sessions_;
  std::map<celer::Connection*, std::uint64_t> authority_session_generation_;
  std::map<std::uint64_t, std::size_t> authority_sessions_by_generation_;
  std::map<std::uint64_t, std::size_t> leader_tasks_by_generation_;
  std::map<std::uint64_t, std::vector<std::shared_ptr<std::promise<void>>>>
      generation_drain_waiters_;
  std::vector<std::shared_ptr<std::promise<void>>> shutdown_drain_waiters_;
  std::map<std::string, celer::Connection*> session_by_node_;
  std::map<std::string, std::uint64_t> next_session_generation_;
};

std::shared_ptr<MetaDataControlServer>
MetaDataControlServer::LifecycleHarnessForTest(
    celer::ForeignExecutor foreign_executor, bool shutdown_complete) {
  auto core = std::make_shared<Core>();
  core->foreign_executor_ = foreign_executor;
  core->shutdown_complete_.store(shutdown_complete, std::memory_order_release);
  return std::shared_ptr<MetaDataControlServer>(
      new MetaDataControlServer(std::move(core)));
}

// Worker-local coordination shared by the established session's sole reader,
// commit publisher, and directive sender. The auxiliary tasks may produce
// writes concurrently, but SessionIo funnels every frame through one
// ControlSessionWriter. Only SessionLoop reads and resolves the single pending
// publisher acknowledgement.
struct LiveSessionState {
  std::shared_ptr<MetaDataControlServer::Core> core_;
  celer::Worker* worker_ = nullptr;
  celer::Connection* connection_ = nullptr;
  SessionIo* io_ = nullptr;
  std::shared_ptr<MetaCommitSubscription> commit_subscription_;
  std::shared_ptr<SessionCommitSignal> commit_signal_;
  std::string node_id_;
  std::string boot_id_;
  control::WireId128 session_id_{};
  std::uint64_t leadership_generation_ = 0;

  std::shared_ptr<const NodeControlBatch> installed_;
  // Highest synchronously published command commit whose semantic node
  // projection has been compared with installed_. Heartbeats and directive
  // boundaries deny authority if the coordinator high-water advances first,
  // even while the subscription callback is still queued cross-thread.
  std::uint64_t validated_committed_high_water_ = 0;
  MetaDirectiveReceiptTracker receipt_tracker_;
  std::vector<control::WireAuthorityAnchor> fenced_authorities_;

  std::optional<control::WireAuthorityAnchor> expected_fence_;
  std::optional<control::FullStateApplied> expected_applied_;
  std::unique_ptr<control::ControlDeadlineWatchdog> fence_ack_deadline_;
  std::unique_ptr<control::ControlDeadlineWatchdog> applied_ack_deadline_;
  std::unique_ptr<control::ControlDeadlineWatchdog>
      inbound_transfer_deadline_;
  bool fence_received_ = false;
  bool applied_received_ = false;
  celer::AsyncNotification response_changed_;

  std::optional<absl::Status> terminal_error_;
  std::size_t active_tasks_ = 0;
  bool publisher_running_ = false;
  bool directive_sender_running_ = false;
  bool directive_transfer_active_ = false;
  bool projection_superseded_ = false;
  bool closing_ = false;
  celer::AsyncNotification tasks_changed_;
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
                                  celer::Connection* connection,
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

void RemoveSession(MetaDataControlServer::Core& core,
                   celer::Connection* connection,
                   std::string_view node_id = {}) {
  const auto session =
      std::find(core.sessions_.begin(), core.sessions_.end(), connection);
  if (session != core.sessions_.end()) {
    *session = core.sessions_.back();
    core.sessions_.pop_back();
    core.live_session_tasks_.fetch_sub(1, std::memory_order_relaxed);
  }
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
    const auto by_node = core.session_by_node_.find(std::string(node_id));
    if (by_node != core.session_by_node_.end() &&
        by_node->second == connection) {
      core.session_by_node_.erase(by_node);
    }
  }
  NotifyShutdownDrained(core);
}

void CloseConnectionNow(celer::Worker& worker, celer::Connection* connection,
                        absl::Status status) {
  // shutdown(2) makes the transport revocation externally visible before the
  // reconciler acknowledges demotion. BeginClose then performs Celer's normal
  // in-flight operation drain and connection retirement.
  if (connection != nullptr && connection->file_.fd_ >= 0) {
    (void)::shutdown(connection->file_.fd_, SHUT_RDWR);
  }
  worker.BeginClose(connection, std::move(status),
                    celer::CloseMode::kLocalClose);
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

std::string LocalDataControlEndpoint(
    const MetaDataControlServerOptions& options) {
  return keylane::FormatNumericEndpoint(
      {.host_ = options.bind_host_, .port_ = options.port_});
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
    const MetaCommittedView& view, std::uint32_t local_server_id,
    std::string_view local_endpoint) {
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
        committed->principal_ != identity->principal_) {
      return absl::FailedPreconditionError(
          "NuRaft member lacks its active committed identity binding");
    }
    if (static_cast<std::uint32_t>(member->get_id()) == local_server_id &&
        committed->data_control_endpoint_ != local_endpoint) {
      return absl::FailedPreconditionError(
          "local committed data-control endpoint differs from process config");
    }
    // Remote legacy members cannot be repaired from this process because
    // their data-control endpoint is not present in NuRaft's peer descriptor.
    // Requiring a non-empty, parseable committed endpoint keeps discovery
    // fail-closed until an operator supplies that durable fact.
    if (!ParseMetaEndpoint(*committed).ok()) {
      return absl::FailedPreconditionError(
          "NuRaft member lacks a usable committed data-control endpoint");
    }
  }
  return absl::OkStatus();
}

celer::Task<absl::Status> ReconcileLocalMetaMember(
    std::shared_ptr<MetaDataControlServer::Core> core,
    MetaLeaderContext& context, std::uint64_t generation) {
  struct CompletionGuard {
    std::shared_ptr<MetaDataControlServer::Core> core_;
    std::uint64_t generation_;
    ~CompletionGuard() { FinishLeaderTask(*core_, generation_); }
  } completion{core, generation};

  const std::string local_endpoint = LocalDataControlEndpoint(core->options_);
  while (StillLeader(*core, generation)) {
    const nuraft::ptr<nuraft::cluster_config> config =
        core->server_->get_config();
    auto local_identity = LocalConfiguredIdentity(*core, config);
    absl::Status status = local_identity.status();
    if (local_identity.ok()) {
      MetaCommittedView view = context.CommittedView();
      auto binding = EvaluateLocalMetaMemberBinding(
          view, core->options_.server_id_, local_identity->principal_,
          local_endpoint);
      status = binding.status();
      if (binding.ok() &&
          *binding == MetaLocalMemberBindingDisposition::kNeedsBind) {
        auto request_id = control::GenerateId128();
        if (!request_id.ok()) {
          status = request_id.status();
        } else {
          BindMetaMember bind{
              .request_id_ = *request_id,
              .actor_ = {},
              .server_id_ = core->options_.server_id_,
              .principal_ = local_identity->principal_,
              .data_control_endpoint_ = local_endpoint,
          };
          auto proposed =
              co_await context.Propose(MetaCommand(std::move(bind)));
          if (!proposed.ok()) {
            status = proposed.status();
          } else if (proposed->verdict_ != MetaAuditVerdict::kAccepted) {
            status = absl::FailedPreconditionError(
                absl::StrCat("bootstrap Meta-member binding was rejected: ",
                             proposed->detail_));
          } else {
            status = absl::OkStatus();
          }
        }
      } else if (binding.ok()) {
        status = absl::OkStatus();
      }

      if (status.ok()) {
        // Re-read both sides after a possible proposal. A dynamic membership
        // workflow durably binds remote members before add_srv; a legacy
        // configuration that lacks those records remains unavailable.
        status = ValidateCommittedConfigBindings(
            core->server_->get_config(), context.CommittedView(),
            core->options_.server_id_, local_endpoint);
      }
    }

    if (status.ok()) {
      if (StillLeader(*core, generation)) {
        core->leader_ready_for_data_ = true;
      }
      co_return absl::OkStatus();
    }
    spdlog::warn("data-control leader membership reconciliation: {}",
                 status.message());
    const absl::Status waited = co_await celer::SleepFor(*core->worker_, 100ms);
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
      .heartbeat_interval_ms = core.options_.heartbeat_interval_ms_,
      .observation_ttl_ms = core.options_.observation_ttl_ms_,
      .session_progress_timeout_ms = core.options_.session_progress_timeout_ms_,
  };
}

celer::Task<absl::Status> AwaitApplied(
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

celer::Task<absl::Status> SendFullState(
    SessionIo& io, std::shared_ptr<const NodeControlBatch> batch) {
  if (batch == nullptr) {
    co_return absl::InvalidArgumentError("FullDesiredState batch is empty");
  }
  // Aliasing retains the entire immutable projection batch while exposing its
  // encoded member directly; a cancelled producer therefore cannot strand a
  // borrowed 512 MiB view in the shared writer queue.
  const std::string* encoded_member = &batch->encoded_full_state;
  auto encoded =
      std::shared_ptr<const std::string>(std::move(batch), encoded_member);
  co_return co_await io.SendFullDesiredState(std::move(encoded));
}

celer::Task<absl::Status> AbortSupersededReplacement(
    SessionIo& io, const control::WireId128& object_id, bool transfer_active) {
  if (transfer_active) {
    if (absl::Status aborted =
            co_await io.Send(control::MessagePriority::kReliable,
                             control::WireMessage(control::TransferAbort{
                                 .object_id = object_id,
                                 .reason = 1,
                             }));
        !aborted.ok()) {
      co_return aborted;
    }
  }
  co_return absl::AbortedError(
      "FullDesiredState was superseded before directive dispatch");
}

celer::Task<absl::Status> EnsureCurrentBeforeDirectiveDispatch(
    const std::shared_ptr<MetaDataControlServer::Core>& core, SessionIo& io,
    const NodeControlBatch& installed, std::string_view node_id,
    std::string_view boot_id, const control::WireId128& session_id,
    std::uint64_t leadership_generation, SessionCommitSignal& commit_signal,
    const MetaCommitSubscription& commit_subscription,
    std::uint64_t* validated_committed_high_water,
    std::deque<control::WireMessage>* deferred,
    std::size_t max_deferred_messages,
    std::vector<control::WireAuthorityAnchor>* fenced) {
  if (!StillLeader(*core, leadership_generation)) {
    co_return absl::CancelledError(
        "Meta leadership changed before directive dispatch");
  }
  if (commit_signal.delivery_failed_.load(std::memory_order_acquire) ||
      commit_subscription.needs_resync()) {
    co_return absl::ResourceExhaustedError(
        "Meta commit subscription overflowed before directive dispatch");
  }

  // Always take one fresh atomic view here instead of relying solely on a
  // mailbox dirty bit. This is the final fail-closed barrier after the Data
  // node has applied an object and before any storage-mutating directive from
  // that object can be emitted.
  const std::uint64_t high_water = core->coordinator_->CommittedHighWater();
  auto cached_view = CommittedViewAtLeast(*core, high_water);
  if (!cached_view.ok()) co_return cached_view.status();
  const MetaCommittedView& view = **cached_view;
  auto latest = MetaControlProjector::ProjectNode(view, node_id);
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
  if (EvaluateReplacementDisposition(installed.full_state,
                                     latest->full_state) ==
      MetaReplacementDisposition::kAbortSuperseded) {
    co_return absl::AbortedError(
        "installed FullDesiredState was superseded before directive "
        "dispatch");
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
    state->response_changed_.NotifyAll(*state->worker_);
    return true;
  }
  return false;
}

celer::Task<absl::Status> AwaitPublisherFence(
    const std::shared_ptr<LiveSessionState>& state) {
  if (!state->fence_received_) {
    const absl::Status armed = state->fence_ack_deadline_->Arm(
        std::chrono::milliseconds(
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

celer::Task<absl::Status> AwaitPublisherApplied(
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

celer::Task<absl::Status> FenceSupersededAuthorityLive(
    const std::shared_ptr<LiveSessionState>& state,
    const NodeControlBatch& installed,
    const control::FullDesiredState* latest) {
  const auto superseded = UnfencedSupersededAuthorities(
      installed.full_state, latest, state->node_id_,
      state->fenced_authorities_);
  for (const control::WireAuthorityAnchor& anchor : superseded) {
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
                .source_meta_applied_index =
                    installed.full_state.source_meta_applied_index,
                .projection_hash = installed.full_state.projection_hash,
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
    state->fenced_authorities_.push_back(anchor);
  }
  co_return absl::OkStatus();
}

celer::Task<absl::StatusOr<MetaReplacementDisposition>>
CheckLiveTransferBoundary(const std::shared_ptr<LiveSessionState>& state,
                          const NodeControlBatch& installed,
                          const NodeControlBatch& replacement) {
  if (!StillLeader(*state->core_, state->leadership_generation_)) {
    co_return absl::CancelledError(
        "Meta leadership changed during FullDesiredState publication");
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
  auto latest = MetaControlProjector::ProjectNode(view, state->node_id_);
  const control::FullDesiredState* latest_state =
      latest.ok() ? &latest->full_state : nullptr;
  if (absl::Status fenced =
          co_await FenceSupersededAuthorityLive(state, installed, latest_state);
      !fenced.ok()) {
    co_return fenced;
  }
  if (!latest.ok()) co_return latest.status();
  const MetaReplacementDisposition disposition =
      EvaluateReplacementDisposition(replacement.full_state,
                                     latest->full_state);
  if (disposition == MetaReplacementDisposition::kContinue) {
    // Keep projection_superseded_ set until the complete object is Applied
    // and the final stable-view check succeeds. This cursor only avoids
    // re-projecting the same semantic no-op commit at every 64 KiB boundary.
    detail::RecordEquivalentTransferBoundary(
        view.applied_index(), &state->validated_committed_high_water_);
  }
  co_return disposition;
}

celer::Task<absl::Status> SendReplacementFullStateLive(
    const std::shared_ptr<LiveSessionState>& state,
    const NodeControlBatch& installed, const NodeControlBatch& replacement) {
  const std::string_view bytes = replacement.encoded_full_state;
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
      co_return absl::AbortedError(
          "FullDesiredState was superseded before its direct frame");
    }

    // Arm before the single frame for the same reason the streamed path arms
    // before TransferEnd: the reader may observe an immediate Applied while
    // this producer is still returning from the socket write.
    state->expected_applied_ = AppliedReceipt(replacement);
    state->applied_received_ = false;
    if (absl::Status armed = state->applied_ack_deadline_->Arm(
            std::chrono::milliseconds(
                state->core_->options_.session_progress_timeout_ms_));
        !armed.ok()) {
      ClearPublisherApplied(state);
      co_return armed;
    }
    if (absl::Status sent = co_await state->io_->Send(
            control::MessagePriority::kReliable,
            control::WireMessage(replacement.full_state));
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
      ClearPublisherApplied(state);
      co_return absl::AbortedError(
          "FullDesiredState was superseded after its direct frame");
    }
    co_return co_await AwaitPublisherApplied(state);
  }

  auto object_id = control::GenerateId128();
  if (!object_id.ok()) co_return object_id.status();
  const control::TransferStart start{
      .kind = control::TransferKind::kFullDesiredState,
      .object_id = *object_id,
      .total_length = bytes.size(),
      .sha256 = control::ComputeSha256(bytes),
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
    co_return co_await AbortSupersededReplacement(*state->io_, *object_id,
                                                  /*transfer_active=*/true);
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
      co_return co_await AbortSupersededReplacement(*state->io_, *object_id,
                                                    /*transfer_active=*/true);
    }
  }

  // Arm before TransferEnd so the sole reader can accept an immediate Applied
  // response. FenceAck is tracked independently, allowing a commit discovered
  // after End to revoke old authority while Applied is also in flight.
  state->expected_applied_ = AppliedReceipt(replacement);
  state->applied_received_ = false;
  if (absl::Status armed = state->applied_ack_deadline_->Arm(
          std::chrono::milliseconds(
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
    ClearPublisherApplied(state);
    co_return absl::AbortedError(
        "FullDesiredState was superseded after TransferEnd");
  }
  co_return co_await AwaitPublisherApplied(state);
}

celer::Task<absl::Status> RunDirectiveSender(
    std::shared_ptr<LiveSessionState> state) {
  const std::shared_ptr<const NodeControlBatch> batch = state->installed_;
  absl::Status status = co_await SendDirectives(
      *state->io_, *batch, state->session_id_, state->node_id_, state->boot_id_,
      state->receipt_tracker_,
      [state, batch] {
        return !state->closing_ && ProjectionCurrent(*state) &&
               state->installed_ == batch;
      },
      [state](bool active) { state->directive_transfer_active_ = active; });
  state->directive_transfer_active_ = false;
  const bool superseded =
      status.code() == absl::StatusCode::kCancelled &&
      (state->projection_superseded_ ||
       CommitPending(*state->commit_signal_,
                     state->validated_committed_high_water_) ||
       state->installed_ != batch);
  FinishLiveSessionTask(state, &state->directive_sender_running_);
  if (!status.ok() && !superseded && !state->closing_) {
    FailLiveSession(state, status);
  }
  co_return status;
}

void StartDirectiveSender(const std::shared_ptr<LiveSessionState>& state) {
  if (state->closing_ || state->directive_sender_running_ ||
      !ProjectionCurrent(*state) ||
      state->installed_->full_state.current_directives.empty() ||
      !state->receipt_tracker_.HasUndispatched()) {
    return;
  }
  state->directive_sender_running_ = true;
  ++state->active_tasks_;
  state->worker_->Spawn(RunDirectiveSender(state));
}

celer::Task<absl::Status> SessionPublisherBody(
    const std::shared_ptr<LiveSessionState>& state) {
  while (!state->closing_ &&
         StillLeader(*state->core_, state->leadership_generation_)) {
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
    auto latest = MetaControlProjector::ProjectNode(view, state->node_id_);
    const std::shared_ptr<const NodeControlBatch> installed = state->installed_;
    const control::FullDesiredState* latest_state =
        latest.ok() ? &latest->full_state : nullptr;
    if (!latest.ok() || EvaluateReplacementDisposition(installed->full_state,
                                                       latest->full_state) ==
                            MetaReplacementDisposition::kAbortSuperseded) {
      state->projection_superseded_ = true;
    }
    if (absl::Status fenced = co_await FenceSupersededAuthorityLive(
            state, *installed, latest_state);
        !fenced.ok()) {
      co_return fenced;
    }
    if (!latest.ok()) co_return latest.status();
    if (EvaluateReplacementDisposition(installed->full_state,
                                       latest->full_state) ==
        MetaReplacementDisposition::kContinue) {
      // An unrelated commit can interrupt the sender at its next directive
      // boundary without changing this node's semantic projection. Preserve
      // per-session dispatch progress: StartDirectiveSender is a no-op when all
      // identities were already written and otherwise resumes only the unsent
      // suffix retained by receipt_tracker_.
      state->validated_committed_high_water_ = std::max(
          state->validated_committed_high_water_, view.applied_index());
      state->projection_superseded_ = false;
      StartDirectiveSender(state);
      continue;
    }

    if (state->directive_sender_running_) {
      // The Fence above is an independent authority producer: it enters the
      // shared writer's authority lane and overtakes a long Directive object
      // between chunks. Do not overlap the new FDS transfer with that object;
      // after a transfer was in flight, close and let reconnect start from the
      // newest atomic projection.
      if (state->directive_transfer_active_) {
        co_return absl::AbortedError(
            "projection superseded an in-flight Directive transfer");
      }
      while (!state->closing_ && state->directive_sender_running_) {
        co_await state->tasks_changed_.Wait();
      }
      if (state->closing_) break;
      continue;
    }

    if (absl::Status sent =
            co_await SendReplacementFullStateLive(state, *installed, *latest);
        !sent.ok()) {
      co_return sent;
    }
    state->core_->full_states_sent_.fetch_add(1, std::memory_order_relaxed);
    state->installed_ = std::make_shared<NodeControlBatch>(std::move(*latest));

    // Data has applied this object, but a commit may have landed behind its
    // End/Ack exchange. Re-enter the publisher before rebuilding receipt
    // state or dispatching any directive from an intermediate projection.
    const std::uint64_t stable_high_water =
        state->core_->coordinator_->CommittedHighWater();
    auto cached_stable_view =
        CommittedViewAtLeast(*state->core_, stable_high_water);
    if (!cached_stable_view.ok()) co_return cached_stable_view.status();
    const MetaCommittedView& stable_view = **cached_stable_view;
    auto stable =
        MetaControlProjector::ProjectNode(stable_view, state->node_id_);
    if (!stable.ok() ||
        EvaluateReplacementDisposition(state->installed_->full_state,
                                       stable->full_state) ==
            MetaReplacementDisposition::kAbortSuperseded) {
      continue;
    }
    state->validated_committed_high_water_ = std::max(
        state->validated_committed_high_water_, stable_view.applied_index());
    if (absl::Status rebuilt = state->receipt_tracker_.Rebuild(
            state->installed_->full_state.current_directives, state->node_id_,
            state->boot_id_);
        !rebuilt.ok()) {
      co_return rebuilt;
    }
    state->projection_superseded_ = false;
    StartDirectiveSender(state);
  }
  co_return absl::CancelledError(
      "data-control publisher stopped with its leadership generation");
}

celer::Task<absl::Status> RunSessionPublisher(
    std::shared_ptr<LiveSessionState> state) {
  absl::Status status = co_await SessionPublisherBody(state);
  FinishLiveSessionTask(state, &state->publisher_running_);
  if (!status.ok() && !state->closing_) {
    FailLiveSession(state, status);
  }
  co_return status;
}

celer::Task<absl::Status> HandleDirectiveResult(
    const std::shared_ptr<MetaDataControlServer::Core>& core, SessionIo& io,
    const control::DirectiveResult& result, std::string_view node_id,
    const MetaBootIncarnation& boot_id, std::uint64_t leadership_generation,
    const control::WireId128& session_id,
    const MetaDirectiveReceiptTracker& receipt_tracker) {
  auto parsed_boot = ParseIdentity<20>(result.recipient_boot_id,
                                       "directive result recipient boot id");
  if (!parsed_boot.ok() || *parsed_boot != boot_id ||
      result.session_id != session_id ||
      result.result_hash != control::ComputeSha256(result.result)) {
    co_return absl::InvalidArgumentError(
        "directive result session, boot, or content hash mismatch");
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
  // Matching committed results above and no-longer-tracked attempts are
  // idempotent across receipt tracker replacement. A first live result must be
  // consistent with the exact execution path observed in this session.
  if (absl::Status receipt_path =
          receipt_tracker.ValidateResult(result.identity, result.status);
      !receipt_path.ok()) {
    co_return receipt_path;
  }
  const MetaDirectiveSpec& spec = tracked->spec_;
  if (spec.kind_ != "rebuild" && spec.kind_ != "authorize-source" &&
      spec.kind_ != "revoke-sources") {
    co_return absl::FailedPreconditionError(
        "tracked directive kind is not executable by data control");
  }
  const bool executes_on_target = spec.kind_ == "rebuild";
  const std::string& expected_node =
      executes_on_target ? spec.target_node_id_ : spec.source_node_id_;
  const MetaBootIncarnation& expected_boot =
      executes_on_target ? spec.target_boot_id_ : spec.source_boot_id_;
  if (spec.recipient_node_id_ != expected_node || expected_node != node_id ||
      expected_boot != boot_id || spec.assignment_id_ != result.assignment_id) {
    co_return absl::PermissionDeniedError(
        "directive result does not match its recipient or authority anchor");
  }
  if (!StillLeader(*core, leadership_generation)) {
    co_return absl::FailedPreconditionError(
        "leadership changed before directive result proposal");
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
      .result_hash_ = result.result_hash,
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

celer::Task<absl::Status> RunEstablishedSession(
    const std::shared_ptr<LiveSessionState>& state,
    const MetaBootIncarnation& boot_id,
    const MetaReplicationHistoryId& replication_history_id,
    std::uint64_t session_generation,
    std::deque<control::WireMessage> deferred) {
  control::HeartbeatSequenceWindow heartbeat_window;
  std::optional<control::HeartbeatAck> cached_ack;
  ClientTransferSink client_transfer_sink;
  control::LargeObjectReassembler client_reassembler(client_transfer_sink);

  while (!state->closing_ &&
         StillLeader(*state->core_, state->leadership_generation_)) {
    absl::StatusOr<control::WireMessage> incoming =
        deferred.empty()
            ? co_await state->io_->Read()
            : absl::StatusOr<control::WireMessage>(std::move(deferred.front()));
    if (!deferred.empty()) deferred.pop_front();
    if (!incoming.ok()) co_return incoming.status();
    if (state->commit_signal_->delivery_failed_.load(
            std::memory_order_acquire) ||
        state->commit_subscription_->needs_resync()) {
      co_return absl::ResourceExhaustedError(
          "Meta commit subscription overflowed for data-control session");
    }

    auto publisher_response = HandlePublisherResponse(state, *incoming);
    if (!publisher_response.ok()) co_return publisher_response.status();
    if (*publisher_response) continue;

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
        const absl::Status armed = state->inbound_transfer_deadline_->Arm(
            std::chrono::milliseconds(
                state->core_->options_.session_progress_timeout_ms_));
        if (!armed.ok()) co_return armed;
      }
      if (finishes) (void)state->inbound_transfer_deadline_->Disarm();
      if (!client_transfer_sink.committed()) continue;
      if (!client_transfer_sink.kind().has_value()) {
        co_return absl::InternalError(
            "committed client transfer has no typed kind");
      }
      const control::TransferKind kind = *client_transfer_sink.kind();
      const control::MessageType message_type =
          kind == control::TransferKind::kDirectiveResult
              ? control::MessageType::kDirectiveResult
              : control::MessageType::kOperationEvidence;
      auto decoded =
          control::DecodeMessage(message_type, client_transfer_sink.Take());
      if (!decoded.ok()) co_return decoded.status();
      const bool right_type =
          (kind == control::TransferKind::kDirectiveResult &&
           std::holds_alternative<control::DirectiveResult>(*decoded)) ||
          (kind == control::TransferKind::kObservationEvidence &&
           std::holds_alternative<control::OperationEvidence>(*decoded));
      if (!right_type) {
        co_return absl::InvalidArgumentError(
            "client transfer decoded to the wrong message type");
      }
      incoming = std::move(*decoded);
    }

    if (const auto* heartbeat = std::get_if<control::Heartbeat>(&*incoming)) {
      if (heartbeat->session_id != state->session_id_) {
        co_return absl::InvalidArgumentError("heartbeat session id mismatch");
      }
      auto encoded = control::EncodeMessage(control::WireMessage(*heartbeat));
      if (!encoded.ok()) co_return encoded.status();
      auto sequence = heartbeat_window.Observe(
          heartbeat->heartbeat_sequence, control::ComputeSha256(*encoded));
      if (!sequence.ok()) co_return sequence.status();
      if (*sequence ==
          control::HeartbeatSequenceDisposition::kReplayCachedAck) {
        if (!cached_ack.has_value()) {
          co_return absl::InternalError("heartbeat ack cache is empty");
        }
        if (absl::Status sent =
                co_await state->io_->Send(control::MessagePriority::kAuthority,
                                          control::WireMessage(*cached_ack));
            !sent.ok()) {
          co_return sent;
        }
        continue;
      }

      // Observation ingestion is independent from publisher progress and
      // lease validation. In particular, a rejected challenge or candidate
      // cannot suppress an otherwise valid boot/health observation.
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
      ViewFacts facts(latest_view);
      MetaHeartbeatObservationResult observation = IngestHeartbeatObservations(
          *state->core_->observations_, facts, state->node_id_, boot_id,
          replication_history_id, session_generation, heartbeat->health,
          heartbeat->candidate, NowUnixMillis());
      if (observation.status == control::ObservationStatus::kAccepted) {
        state->core_->observations_accepted_.fetch_add(
            1, std::memory_order_relaxed);
      } else {
        state->core_->observations_rejected_.fetch_add(
            1, std::memory_order_relaxed);
      }

      const std::shared_ptr<const NodeControlBatch> installed =
          state->installed_;
      const bool leader_valid =
          StillLeader(*state->core_, state->leadership_generation_) &&
          state->core_->server_->is_leader_alive() &&
          !CommitPending(*state->commit_signal_,
                         state->validated_committed_high_water_) &&
          !state->projection_superseded_ &&
          state->validated_committed_high_water_ >= committed_high_water;
      control::LeaseDecision lease =
          state->core_->lease_handoff_guard_->Enforce(
              EvaluateLeaseChallenge(
                  heartbeat->challenge, heartbeat->health,
                  MetaLeaseEvaluation{
                      .leader_valid_ = leader_valid,
                      .server_id_ = state->core_->options_.server_id_,
                      .raft_term_ = state->core_->server_->get_term(),
                      .leadership_generation_ = state->leadership_generation_,
                      .leadership_validity_ms_ =
                          state->core_->options_.leadership_validity_ms_,
                      .node_id_ = state->node_id_,
                      .boot_id_ = state->boot_id_,
                      .applied_projection_hash_ =
                          installed->full_state.projection_hash,
                      .desired_ = &installed->full_state,
                  }),
              state->node_id_, NowMonotonicMillis());
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
      if (absl::Status sent =
              co_await state->io_->Send(control::MessagePriority::kAuthority,
                                        control::WireMessage(*cached_ack));
          !sent.ok()) {
        co_return sent;
      }
      continue;
    }

    if (const auto* evidence =
            std::get_if<control::OperationEvidence>(&*incoming)) {
      const std::uint64_t committed_high_water =
          state->core_->coordinator_->CommittedHighWater();
      auto cached_view =
          CommittedViewAtLeast(*state->core_, committed_high_water);
      if (!cached_view.ok()) co_return cached_view.status();
      const MetaCommittedView& latest_view = **cached_view;
      const ViewFacts facts(latest_view);
      const absl::Status ingested = IngestOperationEvidenceObservation(
          *state->core_->observations_, facts, state->node_id_, boot_id,
          session_generation, state->session_id_, *evidence, NowUnixMillis());
      if (ingested.ok()) {
        state->core_->observations_accepted_.fetch_add(
            1, std::memory_order_relaxed);
      } else {
        if (ingested.code() != absl::StatusCode::kFailedPrecondition) {
          co_return ingested;
        }
        // Operation evidence is soft/latest-wins state. A stale report is
        // audited and discarded without taking down the otherwise current
        // authority session; malformed framing/schema was already rejected
        // before this domain admission point.
        state->core_->observations_rejected_.fetch_add(
            1, std::memory_order_relaxed);
      }
      continue;
    }

    if (const auto* result =
            std::get_if<control::DirectiveResult>(&*incoming)) {
      if (absl::Status handled = co_await HandleDirectiveResult(
              state->core_, *state->io_, *result, state->node_id_, boot_id,
              state->leadership_generation_, state->session_id_,
              state->receipt_tracker_);
          !handled.ok()) {
        co_return handled;
      }
      continue;
    }
    if (const auto* receipt =
            std::get_if<control::DirectiveReceipt>(&*incoming)) {
      if (receipt->session_id != state->session_id_ ||
          receipt->recipient_boot_id != state->boot_id_) {
        co_return absl::InvalidArgumentError(
            "directive receipt does not name the current session/boot");
      }
      if (absl::Status observed = state->receipt_tracker_.Observe(
              receipt->identity, receipt->stage);
          !observed.ok()) {
        co_return observed;
      }
      continue;
    }
    co_return absl::InvalidArgumentError("unexpected data-control message");
  }
  if (state->terminal_error_.has_value()) co_return *state->terminal_error_;
  co_return absl::CancelledError("Meta leadership changed");
}

}  // namespace

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
  const unsigned tls_fields = !options.tls_ca_cert_file_.empty() +
                              !options.tls_cert_file_.empty() +
                              !options.tls_key_file_.empty();
  if (tls_fields != 0 && tls_fields != 3) {
    return absl::InvalidArgumentError(
        "data-control mTLS requires CA, certificate, and key together");
  }
  if (options.heartbeat_interval_ms_ == 0 ||
      options.observation_ttl_ms_ < options.heartbeat_interval_ms_ ||
      options.session_progress_timeout_ms_ == 0 ||
      options.session_progress_timeout_ms_ > kMaxSessionProgressTimeoutMs ||
      options.heartbeat_interval_ms_ >= options.session_progress_timeout_ms_ ||
      options.leadership_validity_ms_ == 0 ||
      options.max_write_queue_bytes_ < control::kMaxFrameBytes) {
    return absl::InvalidArgumentError(
        "data-control timing or writer bound is invalid");
  }
  return absl::OkStatus();
}

absl::StatusOr<std::shared_ptr<MetaDataControlServer>>
MetaDataControlServer::Create(
    celer::ForeignExecutor foreign_executor,
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
  core->lease_handoff_guard_ = std::make_unique<MetaLeaseHandoffGuard>(
      core->options_.leadership_validity_ms_);
  if (!core->options_.tls_ca_cert_file_.empty()) {
    auto tls = celer::TlsContext::CreateServer(celer::TlsServerOptions{
        .cert_file_ = core->options_.tls_cert_file_,
        .key_file_ = core->options_.tls_key_file_,
        .ca_cert_file_ = core->options_.tls_ca_cert_file_,
        .client_auth_ = celer::TlsClientAuth::kRequired,
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
        core->worker_ = celer::ThisWorker().self_;
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
    core->status_ = absl::UnavailableError("Celer worker is stopping");
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
            const std::vector<celer::Connection*> sessions = core->sessions_;
            for (celer::Connection* connection : sessions) {
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
        celer::Worker* worker = celer::ThisWorker().self_;
        if (core->worker_ == nullptr) core->worker_ = worker;
        auto commit_signal = std::make_shared<SessionCommitSignal>();
        celer::ForeignExecutor commit_executor = core->foreign_executor_;
        MetaSubscriptionStart commit_start = context->SubscribeCommitted(
            [commit_signal, commit_executor,
             worker](const MetaCommitEvent& event) mutable {
              if (PublishCommitIndex(*commit_signal, event.log_index_) &&
                  !commit_executor.Notify(
                      [commit_signal, worker]() noexcept {
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
        core->lease_handoff_guard_->Reset();
        core->leader_context_ = context;
        core->leader_active_ = true;
        core->leader_ready_for_data_ = false;
        StartLeaderTask(*core, core->leadership_generation_);
        worker->Spawn(ReconcileLocalMetaMember(core, *context,
                                               core->leadership_generation_));
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
        core->generation_drain_waiters_[cancelled_generation].push_back(
            complete);
        if (core->worker_ != nullptr) {
          std::vector<celer::Connection*> sessions;
          for (const auto& [connection, generation] :
               core->authority_session_generation_) {
            if (generation == cancelled_generation) {
              sessions.push_back(connection);
            }
          }
          for (celer::Connection* connection : sessions) {
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

celer::Task<absl::Status> MetaDataControlServer::AcceptLoop(CorePtr core) {
  while (core->listening_) {
    auto accepted = co_await core->listener_.Accept();
    if (!accepted.ok()) {
      if (!core->listening_) break;
      const absl::Status slept = co_await celer::SleepFor(*core->worker_, 10ms);
      if (!slept.ok() && !core->listening_) break;
      continue;
    }
    celer::Connection* connection = *accepted;
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
    core->sessions_.push_back(connection);
    core->live_session_tasks_.fetch_add(1, std::memory_order_relaxed);
    core->worker_->Spawn(
        SessionLoop(core, celer::TcpStream(connection), connection));
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

celer::Task<absl::Status> MetaDataControlServer::SessionLoop(
    CorePtr core, celer::TcpStream stream, celer::Connection* connection) {
  std::string node_id;
  bool accepted_session = false;
  bool redirected_session = false;
  // Declared before every session-local transport/subscription object so its
  // destructor runs last. Generation/shutdown waiters are therefore released
  // only after the complete SessionLoop (including commit subscription and
  // writer) has stopped touching the leader context and socket.
  struct SessionCompletionGuard {
    CorePtr core_;
    celer::Connection* connection_;
    std::string* node_id_;
    bool* accepted_;
    ~SessionCompletionGuard() {
      if (*accepted_) {
        core_->active_sessions_.fetch_sub(1, std::memory_order_relaxed);
      }
      RemoveSession(*core_, connection_, *node_id_);
    }
  } completion{core, connection, &node_id, &accepted_session};
  SessionIo io(
      *core->worker_, connection, stream, core->options_.max_write_queue_bytes_,
      std::chrono::milliseconds(core->options_.session_progress_timeout_ms_));
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

  auto cached_view = CommittedViewAtLeast(
      *core, core->coordinator_->CommittedHighWater());
  if (!cached_view.ok()) co_return finish(cached_view.status());
  std::shared_ptr<const MetaCommittedView> view = *cached_view;
  const auto node = view->identity().FindNode(node_id);
  if (!node.has_value() || node->retired_) {
    co_return finish(absl::PermissionDeniedError(
        "data node is not in the active committed registry"));
  }
  if (tls_identity.has_value() &&
      (tls_identity->subject_id_ != node_id ||
       tls_identity->principal_ != node->principal_)) {
    co_return finish(absl::PermissionDeniedError(
        "data-node certificate does not match its committed binding"));
  }

  auto directory = BuildCommittedMetaDirectory(*view);
  if (!directory.ok()) co_return finish(directory.status());
  const bool accepted_leader =
      core->leader_active_ && core->leader_ready_for_data_ &&
      core->leader_context_ != nullptr && core->server_->is_leader() &&
      core->server_->is_leader_sm_fully_caught_up();
  if (!accepted_leader) {
    const absl::Status sent =
        co_await io.Send(control::MessagePriority::kReliable,
                         control::WireMessage(BuildServerHello(
                             *core, std::move(*directory), false)));
    core->redirected_sessions_.fetch_add(1, std::memory_order_relaxed);
    redirected_session = true;
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
  cached_view = CommittedViewAtLeast(
      *core, core->coordinator_->CommittedHighWater());
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

  auto projected = MetaControlProjector::ProjectNode(*view, node_id);
  if (!projected.ok()) co_return finish(projected.status());
  auto batch = std::make_shared<NodeControlBatch>(std::move(*projected));
  view.reset();
  auto session_id = control::GenerateId128();
  if (!session_id.ok()) co_return finish(session_id.status());
  std::uint64_t& next_generation = core->next_session_generation_[node_id];
  if (next_generation == std::numeric_limits<std::uint64_t>::max()) {
    co_return finish(absl::ResourceExhaustedError(
        "data-node session generation is exhausted"));
  }
  const std::uint64_t session_generation = ++next_generation;
  auto boot_id = ParseIdentity<20>(hello->boot_id, "data boot id");
  if (!boot_id.ok()) co_return finish(boot_id.status(), true);
  auto replication_history_id = ParseIdentity<20>(
      hello->replication_history_id, "data replication history id");
  if (!replication_history_id.ok()) {
    co_return finish(replication_history_id.status(), true);
  }
  if (absl::Status bound =
          BindAuthoritySession(*core, connection, leadership_generation);
      !bound.ok()) {
    co_return finish(bound);
  }

  if (const auto old = core->session_by_node_.find(node_id);
      old != core->session_by_node_.end() && old->second != connection) {
    CloseConnectionNow(*core->worker_, old->second,
                       absl::CancelledError("superseded data-node session"));
  }
  core->session_by_node_[node_id] = connection;
  const absl::Status adopted = core->observations_->AdoptSession(
      MetaObservationIdentity{node_id, *boot_id, session_generation},
      NowUnixMillis());
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
  if (absl::Status sent = co_await SendFullState(io, batch); !sent.ok()) {
    co_return finish(sent);
  }
  core->full_states_sent_.fetch_add(1, std::memory_order_relaxed);
  if (absl::Status applied = co_await AwaitApplied(io, *batch); !applied.ok()) {
    co_return finish(applied, true);
  }
  std::uint64_t validated_committed_high_water = 0;
  if (absl::Status current = co_await EnsureCurrentBeforeDirectiveDispatch(
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
  live->leadership_generation_ = leadership_generation;
  live->installed_ = batch;
  live->validated_committed_high_water_ = validated_committed_high_water;
  live->fenced_authorities_ = std::move(fenced_authorities);
  const std::weak_ptr<LiveSessionState> weak_live = live;
  live->fence_ack_deadline_ =
      std::make_unique<control::ControlDeadlineWatchdog>(
          *live->worker_, [weak_live] {
            if (const auto state = weak_live.lock()) {
              FailLiveSession(
                  state,
                  absl::DeadlineExceededError(
                      "FenceAck made no progress before its deadline"));
            }
          });
  live->applied_ack_deadline_ =
      std::make_unique<control::ControlDeadlineWatchdog>(
          *live->worker_, [weak_live] {
            if (const auto state = weak_live.lock()) {
              FailLiveSession(
                  state,
                  absl::DeadlineExceededError(
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
  if (absl::Status rebuilt = live->receipt_tracker_.Rebuild(
          live->installed_->full_state.current_directives, node_id,
          hello->boot_id);
      !rebuilt.ok()) {
    co_return finish(rebuilt, true);
  }
  core->accepted_sessions_.fetch_add(1, std::memory_order_relaxed);
  core->active_sessions_.fetch_add(1, std::memory_order_relaxed);
  accepted_session = true;

  // The reader is SessionLoop's awaited child. Publisher and directive tasks
  // are explicitly joined below before the borrowed SessionIo and commit
  // subscription can be destroyed.
  live->publisher_running_ = true;
  ++live->active_tasks_;
  live->worker_->Spawn(RunSessionPublisher(live));
  StartDirectiveSender(live);
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
  co_return finish(std::move(session_status), protocol_error);
}

}  // namespace keylane::meta
