#include <arpa/inet.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "celer/net/server.h"
#include "gtest/gtest.h"
#include "keylane/command.h"
#include "keylane/fault_injection.h"
#include "keylane/memory.h"
#include "keylane/metrics.h"
#include "keylane/replication.h"
#include "keylane/storage/engine.h"
#include "keylane/tx/tx_shard.h"
#include "tests/support/process.h"

namespace {
using namespace std::chrono_literals;

constexpr std::uint64_t kMiB = 1024 * 1024;
constexpr std::uint64_t kPartitionReplicationEpoch = 23;

absl::Status TestFailure(std::string_view message) {
  return absl::FailedPreconditionError(std::string(message));
}

bool IsCanonicalReplicationId(std::string_view value) {
  if (value.size() != 40) return false;
  for (const unsigned char digit : value) {
    if ((digit < '0' || digit > '9') && (digit < 'a' || digit > 'f')) {
      return false;
    }
  }
  return true;
}

void EnsureTxRuntime() {
  // TxRuntime is process-global and intentionally has no teardown API. This
  // integration binary starts several one-worker servers sequentially, so
  // later cases reuse and rebind the same idle shard.
  if (keylane::tx::TxRuntime::Get() == nullptr) {
    keylane::tx::TxRuntime::Create(1);
  }
}

celer::Task<absl::Status> CheckLightweightQueries(
    const keylane::ReplicationManager& replication,
    const std::optional<keylane::ReplicaOfConfig>& expected_upstream,
    std::string_view phase) {
  const keylane::ReplicationIdentity identity =
      co_await replication.ObserveIdentity();
  const auto upstream = replication.upstream();
  const keylane::ReplicationStatus status = co_await replication.Observe();
  if (!IsCanonicalReplicationId(identity.local_node_id_) ||
      !IsCanonicalReplicationId(identity.boot_id_) ||
      !IsCanonicalReplicationId(identity.local_history_id_) ||
      identity.local_node_id_ != status.local_node_id_ ||
      identity.boot_id_ != status.boot_id_ ||
      identity.local_history_id_ != status.local_history_id_) {
    co_return TestFailure(std::string(phase) +
                          ": lightweight identity disagrees with Observe");
  }
  if (upstream != expected_upstream || upstream != status.upstream_) {
    co_return TestFailure(std::string(phase) +
                          ": lightweight upstream did not track configuration");
  }
  co_return absl::OkStatus();
}

std::string RespBulk(std::string_view value) {
  return "$" + std::to_string(value.size()) + "\r\n" + std::string(value) +
         "\r\n";
}

// A system-boundary peer that returns one well-formed KLFULLRESYNC carrying the
// wrong group, then stalls later connections. This exercises target identity
// validation at the wire boundary and keeps subsequent REBUILDING states
// deterministic without exposing a test-only manager state mutation.
class StallingNativeSource {
 public:
  StallingNativeSource(std::string expected_target_node_id,
                       std::uint16_t target_port)
      : expected_client_identity_(RespBulk("?" + expected_target_node_id + ":" +
                                           std::to_string(target_port))),
        expected_population_target_(RespBulk(expected_target_node_id)),
        expected_population_epoch_(
            RespBulk(std::to_string(kPartitionReplicationEpoch))),
        first_response_("+KLFULLRESYNC 1 " + std::string(40, 'a') + " " +
                        std::string(40, 'e') + " " + std::string(40, 'b') +
                        " " + std::string(40, 'c') + " 1 " +
                        std::string(40, 'f') + "\r\n") {
    listener_ = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (listener_ < 0) {
      error_.store(errno == 0 ? EIO : errno, std::memory_order_release);
      return;
    }
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (::bind(listener_, reinterpret_cast<const sockaddr*>(&address),
               sizeof(address)) != 0 ||
        ::listen(listener_, 4) != 0) {
      error_.store(errno == 0 ? EIO : errno, std::memory_order_release);
      (void)::close(listener_);
      listener_ = -1;
      return;
    }
    socklen_t size = sizeof(address);
    if (::getsockname(listener_, reinterpret_cast<sockaddr*>(&address),
                      &size) != 0) {
      error_.store(errno == 0 ? EIO : errno, std::memory_order_release);
      (void)::close(listener_);
      listener_ = -1;
      return;
    }
    port_ = ntohs(address.sin_port);
    thread_ =
        std::jthread([this](std::stop_token stop) { AcceptConnections(stop); });
  }

  StallingNativeSource(const StallingNativeSource&) = delete;
  StallingNativeSource& operator=(const StallingNativeSource&) = delete;

  ~StallingNativeSource() {
    thread_.request_stop();
    const int connection = connection_.load(std::memory_order_acquire);
    if (connection >= 0) (void)::shutdown(connection, SHUT_RDWR);
    if (listener_ >= 0) (void)::shutdown(listener_, SHUT_RDWR);
    if (thread_.joinable()) thread_.join();
    if (listener_ >= 0) (void)::close(listener_);
  }

  std::uint16_t port() const noexcept { return port_; }
  unsigned accepted() const noexcept {
    return accepted_.load(std::memory_order_acquire);
  }
  unsigned closed() const noexcept {
    return closed_.load(std::memory_order_acquire);
  }
  int error() const noexcept { return error_.load(std::memory_order_acquire); }
  bool saw_directive_identity() const noexcept {
    return saw_directive_identity_.load(std::memory_order_acquire);
  }
  bool saw_configured_node_identity() const noexcept {
    return saw_configured_node_identity_.load(std::memory_order_acquire);
  }
  bool saw_source_assignment() const noexcept {
    return saw_source_assignment_.load(std::memory_order_acquire);
  }
  bool saw_population_epoch() const noexcept {
    return saw_population_epoch_.load(std::memory_order_acquire);
  }

 private:
  void AcceptConnections(std::stop_token stop) noexcept {
    while (!stop.stop_requested()) {
      pollfd listener{.fd = listener_, .events = POLLIN, .revents = 0};
      const int ready = ::poll(&listener, 1, 50);
      if (ready < 0) {
        if (errno == EINTR) continue;
        if (!stop.stop_requested()) {
          error_.store(errno == 0 ? EIO : errno, std::memory_order_release);
        }
        return;
      }
      if (ready == 0) continue;
      if ((listener.revents & POLLIN) == 0) {
        if (!stop.stop_requested())
          error_.store(EIO, std::memory_order_release);
        return;
      }

      const int connection =
          ::accept4(listener_, nullptr, nullptr, SOCK_CLOEXEC | SOCK_NONBLOCK);
      if (connection < 0) {
        if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) continue;
        if (!stop.stop_requested()) {
          error_.store(errno == 0 ? EIO : errno, std::memory_order_release);
        }
        return;
      }
      connection_.store(connection, std::memory_order_release);
      const unsigned accepted =
          accepted_.fetch_add(1, std::memory_order_acq_rel) + 1;
      if (accepted == 1) {
        // Read the complete identity prefix before answering so this test also
        // pins the pre-release POPULATION field order at the network boundary.
        constexpr std::string_view kDirectiveIdentity =
            "$11\r\noperation-a\r\n$11\r\ndirective-a\r\n"
            "$9\r\nattempt-1\r\n$1\r\n1\r\n$64\r\n";
        constexpr std::string_view kSourceAssignment =
            "$19\r\nsource-assignment-a\r\n";
        std::string request;
        const auto deadline = std::chrono::steady_clock::now() + 5s;
        while (
            (request.find(kDirectiveIdentity) == std::string::npos ||
             request.find(kSourceAssignment) == std::string::npos ||
             request.find(expected_client_identity_) == std::string::npos ||
             request.find(expected_population_target_) == std::string::npos ||
             request.find(expected_population_epoch_) == std::string::npos) &&
            std::chrono::steady_clock::now() < deadline) {
          pollfd peer{.fd = connection, .events = POLLIN, .revents = 0};
          const int activity = ::poll(&peer, 1, 50);
          if (activity < 0) {
            if (errno == EINTR) continue;
            break;
          }
          if (activity == 0 || (peer.revents & POLLIN) == 0) continue;
          char buffer[4096];
          const ssize_t received =
              ::recv(connection, buffer, sizeof(buffer), 0);
          if (received <= 0) break;
          request.append(buffer, static_cast<std::size_t>(received));
        }
        if (request.find(kDirectiveIdentity) == std::string::npos) {
          error_.store(EPROTO, std::memory_order_release);
          (void)::close(connection);
          return;
        }
        if (request.find(kSourceAssignment) == std::string::npos) {
          error_.store(EPROTO, std::memory_order_release);
          (void)::close(connection);
          return;
        }
        if (request.find(expected_client_identity_) == std::string::npos ||
            request.find(expected_population_target_) == std::string::npos ||
            request.find(expected_population_epoch_) == std::string::npos) {
          error_.store(EPROTO, std::memory_order_release);
          (void)::close(connection);
          return;
        }
        saw_directive_identity_.store(true, std::memory_order_release);
        saw_source_assignment_.store(true, std::memory_order_release);
        saw_population_epoch_.store(true, std::memory_order_release);
        saw_configured_node_identity_.store(true, std::memory_order_release);
        const ssize_t sent = ::send(connection, first_response_.data(),
                                    first_response_.size(), MSG_NOSIGNAL);
        if (sent != static_cast<ssize_t>(first_response_.size())) {
          error_.store(errno == 0 ? EIO : errno, std::memory_order_release);
          (void)::close(connection);
          return;
        }
      }

      bool peer_closed = false;
      while (!stop.stop_requested() && !peer_closed) {
        pollfd peer{
            .fd = connection, .events = POLLIN | POLLRDHUP, .revents = 0};
        const int activity = ::poll(&peer, 1, 50);
        if (activity < 0) {
          if (errno == EINTR) continue;
          error_.store(errno == 0 ? EIO : errno, std::memory_order_release);
          break;
        }
        if (activity == 0) continue;
        if ((peer.revents & (POLLHUP | POLLRDHUP | POLLERR | POLLNVAL)) != 0) {
          peer_closed = true;
          break;
        }
        if ((peer.revents & POLLIN) == 0) continue;
        char buffer[4096];
        const ssize_t received = ::recv(connection, buffer, sizeof(buffer), 0);
        if (received == 0) {
          peer_closed = true;
        } else if (received < 0 && errno != EINTR && errno != EAGAIN &&
                   errno != EWOULDBLOCK) {
          error_.store(errno == 0 ? EIO : errno, std::memory_order_release);
          break;
        }
      }

      int expected = connection;
      (void)connection_.compare_exchange_strong(
          expected, -1, std::memory_order_acq_rel, std::memory_order_acquire);
      (void)::close(connection);
      if (peer_closed) closed_.fetch_add(1, std::memory_order_acq_rel);
    }
  }

  int listener_ = -1;
  std::uint16_t port_ = 0;
  const std::string expected_client_identity_;
  const std::string expected_population_target_;
  const std::string expected_population_epoch_;
  const std::string first_response_;
  std::jthread thread_;
  std::atomic<int> connection_{-1};
  std::atomic<unsigned> accepted_{0};
  std::atomic<unsigned> closed_{0};
  std::atomic<int> error_{0};
  std::atomic<bool> saw_directive_identity_{false};
  std::atomic<bool> saw_source_assignment_{false};
  std::atomic<bool> saw_population_epoch_{false};
  std::atomic<bool> saw_configured_node_identity_{false};
};

keylane::RebuildDirective TargetDirective(
    const keylane::ClusterPopulationStatus& target,
    const keylane::PopulationManifest& manifest) {
  return keylane::RebuildDirective{
      .identity_ =
          {
              .group_id_ = std::string(40, 'd'),
              .assignment_id_ = "assignment-a",
              .term_ = 7,
              .directive_revision_ = 1,
              .authority_id_ = "authority-a",
              .source_node_id_ = std::string(40, 'a'),
              .source_assignment_id_ = "source-assignment-a",
              .source_boot_id_ = std::string(40, 'b'),
              .source_history_id_ = std::string(40, 'c'),
              .target_node_id_ = target.local_node_id_,
              .target_boot_id_ = target.local_boot_id_,
              .operation_id_ = "operation-a",
              .directive_id_ = "directive-a",
              .attempt_id_ = "attempt-1",
              .manifest_revision_ = 1,
              .manifest_id_ = manifest.id(),
              .partition_replication_epoch_ = kPartitionReplicationEpoch,
          },
      .flow_count_ = 1,
      .safe_source_active_ = true,
  };
}

celer::Task<absl::Status> WaitForPeerCount(celer::Worker& worker,
                                           const StallingNativeSource& source,
                                           bool closed, unsigned expected,
                                           std::string_view description) {
  const auto count = [&] {
    return closed ? source.closed() : source.accepted();
  };
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (count() < expected && std::chrono::steady_clock::now() < deadline) {
    absl::Status waited = co_await celer::SleepFor(worker, 1ms);
    if (!waited.ok()) co_return waited;
  }
  if (count() < expected) {
    co_return absl::DeadlineExceededError(std::string(description));
  }
  co_return absl::OkStatus();
}

class ReplicationManagerService final : public celer::Service {
 public:
  ReplicationManagerService(keylane::storage::StorageEngine* storage,
                            keylane::ReplicationManager* replication,
                            StallingNativeSource* source,
                            std::string expected_node_id)
      : storage_(storage),
        replication_(replication),
        source_(source),
        expected_node_id_(std::move(expected_node_id)) {}

  void Prepare(unsigned thread_count) override {
    if (thread_count != 1) {
      result_ = TestFailure("replication manager test requires one worker");
    }
  }

  celer::Task<absl::Status> Run(celer::Worker& worker,
                                celer::ServiceContext) override {
    keylane::BindMemoryAccountingShard(worker.id());
    keylane::tx::TxRuntime::Get()->shard(worker.id()).Bind(worker);
    if (result_.ok()) result_ = co_await storage_->InitializeWorker(worker);
    if (result_.ok()) {
      replication_->StorageReady(worker);
      result_ = co_await Exercise(worker);
    }
    worker.RequestStop();
    co_return result_;
  }

  void Stop() noexcept override {}

  const absl::Status& result() const noexcept { return result_; }

 private:
  celer::Task<absl::Status> Exercise(celer::Worker& worker) {
    if (source_->port() == 0 || source_->error() != 0) {
      co_return TestFailure("stalling native source failed to start");
    }

    const keylane::ClusterPopulationStatus initial =
        co_await replication_->cluster_population_status();
    if (initial.local_node_id_ != expected_node_id_ ||
        !IsCanonicalReplicationId(initial.local_boot_id_) ||
        initial.state_ != keylane::ReplicationGroupState::kNotReady ||
        initial.ready_token_.has_value()) {
      co_return TestFailure(
          "cluster population did not use the configured node identity");
    }
    if (const absl::Status query = co_await CheckLightweightQueries(
            *replication_, std::nullopt, "cluster startup");
        !query.ok()) {
      co_return query;
    }
    absl::Status startup_wait = co_await celer::SleepFor(worker, 50ms);
    if (!startup_wait.ok()) co_return startup_wait;
    if (source_->accepted() != 0) {
      co_return TestFailure(
          "cluster-enabled manager used a standalone initial upstream");
    }

    auto manifest =
        keylane::PopulationManifest::Create({{42, 9}, {16'383, 11}});
    if (!manifest.ok()) co_return manifest.status();
    keylane::RebuildDirective directive = TargetDirective(initial, *manifest);
    const keylane::ReplicaOfConfig upstream{"127.0.0.1", source_->port()};

    keylane::RebuildDirective wrong_boot = directive;
    wrong_boot.identity_.target_boot_id_ = std::string(40, 'd');
    absl::Status applied = co_await replication_->ApplyClusterRebuildDirective(
        upstream, wrong_boot, *manifest);
    if (applied.code() != absl::StatusCode::kFailedPrecondition) {
      co_return TestFailure("cluster rebuild accepted the wrong target boot");
    }

    auto other_manifest = keylane::PopulationManifest::Create({{7, 1}});
    if (!other_manifest.ok()) co_return other_manifest.status();
    applied = co_await replication_->ApplyClusterRebuildDirective(
        upstream, directive, *other_manifest);
    if (applied.code() != absl::StatusCode::kFailedPrecondition) {
      co_return TestFailure("cluster rebuild accepted a mismatched manifest");
    }

    applied = co_await replication_->ApplyClusterRebuildDirective(
        keylane::ReplicaOfConfig{}, directive, *manifest);
    if (applied.code() != absl::StatusCode::kInvalidArgument) {
      co_return TestFailure(
          "cluster rebuild accepted an empty source endpoint");
    }
    const keylane::ClusterPopulationStatus after_invalid =
        co_await replication_->cluster_population_status();
    if (after_invalid.state_ != keylane::ReplicationGroupState::kNotReady ||
        after_invalid.ready_token_.has_value()) {
      co_return TestFailure("an invalid directive changed population state");
    }

    const keylane::ReplicationStatus replication_status =
        co_await replication_->Observe();
    if (replication_status.local_node_id_ != expected_node_id_ ||
        replication_status.boot_id_ != initial.local_boot_id_) {
      co_return TestFailure(
          "replication status did not preserve the configured node identity");
    }
    keylane::RebuildDirective source_authorization = directive;
    source_authorization.identity_.source_node_id_ = initial.local_node_id_;
    source_authorization.identity_.source_boot_id_ = initial.local_boot_id_;
    source_authorization.identity_.source_history_id_ =
        replication_status.local_history_id_;
    source_authorization.identity_.target_node_id_ = std::string(40, 'e');
    source_authorization.identity_.target_boot_id_ = std::string(40, 'f');
    absl::Status authorized =
        co_await replication_->AuthorizeClusterRebuildSource(
            std::move(source_authorization));
    if (authorized.code() != absl::StatusCode::kFailedPrecondition) {
      co_return TestFailure(
          "cold cluster node authorized itself as an active primary source");
    }
    absl::Status revoked =
        co_await replication_->RevokeClusterRebuildSourceAuthorizations();
    if (!revoked.ok()) {
      co_return TestFailure(
          "empty cluster source revocation was not idempotent");
    }

    applied = co_await replication_->ApplyClusterRebuildDirective(
        upstream, directive, *manifest);
    if (applied.code() != absl::StatusCode::kFailedPrecondition) {
      co_return TestFailure(
          "rebuild admission was reported as terminal success before the "
          "source identity failure");
    }
    absl::Status peer = co_await WaitForPeerCount(
        worker, *source_, false, 1,
        "native source did not receive the mismatched-group connection");
    if (!peer.ok()) co_return peer;
    peer = co_await WaitForPeerCount(
        worker, *source_, true, 1,
        "target did not reject the mismatched source group");
    if (!peer.ok()) co_return peer;
    if (!source_->saw_directive_identity()) {
      co_return TestFailure(
          "native POPULATION handshake omitted the directive identity");
    }
    if (!source_->saw_source_assignment()) {
      co_return TestFailure(
          "native POPULATION handshake omitted the source assignment");
    }
    if (!source_->saw_population_epoch()) {
      co_return TestFailure(
          "native POPULATION handshake omitted the partition replication "
          "epoch");
    }
    if (!source_->saw_configured_node_identity()) {
      co_return TestFailure(
          "native POPULATION handshake did not use the configured node id");
    }

    keylane::ClusterPopulationStatus after_mismatch;
    const auto mismatch_deadline = std::chrono::steady_clock::now() + 5s;
    do {
      after_mismatch = co_await replication_->cluster_population_status();
      if (after_mismatch.state_ == keylane::ReplicationGroupState::kNotReady)
        break;
      absl::Status waited = co_await celer::SleepFor(worker, 1ms);
      if (!waited.ok()) co_return waited;
    } while (std::chrono::steady_clock::now() < mismatch_deadline);
    if (after_mismatch.state_ != keylane::ReplicationGroupState::kNotReady ||
        after_mismatch.ready_token_.has_value()) {
      co_return TestFailure(
          "mismatched source group did not retire the rebuild attempt");
    }

    directive.identity_.directive_revision_ = 2;
    directive.identity_.attempt_id_ = "attempt-2";
    auto started = co_await replication_->StartClusterRebuildDirective(
        upstream, directive, *manifest);
    if (!started.ok()) co_return started.status();
    keylane::ClusterRebuildCompletion in_progress = std::move(*started);
    const keylane::ClusterPopulationStatus rebuilding =
        co_await replication_->cluster_population_status();
    if (rebuilding.state_ != keylane::ReplicationGroupState::kRebuilding ||
        rebuilding.ready_token_.has_value()) {
      co_return TestFailure("accepted cluster directive was not REBUILDING");
    }
    peer = co_await WaitForPeerCount(
        worker, *source_, false, 2,
        "native source did not receive the replacement rebuild connection");
    if (!peer.ok()) co_return peer;

    if (const absl::Status query = co_await CheckLightweightQueries(
            *replication_, upstream, "native rebuild connected");
        !query.ok()) {
      co_return query;
    }
    auto replay = co_await replication_->StartClusterRebuildDirective(
        upstream, directive, *manifest);
    if (!replay.ok()) {
      co_return TestFailure(
          "exact in-progress directive replay was not idempotent");
    }

    const std::uint16_t conflicting_port =
        source_->port() == std::numeric_limits<std::uint16_t>::max()
            ? static_cast<std::uint16_t>(source_->port() - 1)
            : static_cast<std::uint16_t>(source_->port() + 1);
    auto conflicting = co_await replication_->StartClusterRebuildDirective(
        keylane::ReplicaOfConfig{"127.0.0.1", conflicting_port}, directive,
        *manifest);
    if (conflicting.status().code() != absl::StatusCode::kFailedPrecondition) {
      co_return TestFailure(
          "exact directive replay accepted a conflicting source endpoint");
    }
    if (source_->accepted() != 2 || source_->closed() != 1) {
      co_return TestFailure(
          "conflicting endpoint replay disturbed the accepted session");
    }

    keylane::RebuildDirective replacement = directive;
    replacement.identity_.directive_revision_ = 3;
    replacement.identity_.attempt_id_ = "attempt-3";
    auto replacement_started =
        co_await replication_->StartClusterRebuildDirective(
            upstream, replacement, *manifest);
    if (!replacement_started.ok()) co_return replacement_started.status();
    if ((co_await in_progress.Await()).code() != absl::StatusCode::kCancelled ||
        (co_await replay->Await()).code() != absl::StatusCode::kCancelled) {
      co_return TestFailure(
          "superseded rebuild did not resolve every exact-attempt waiter");
    }

    peer = co_await WaitForPeerCount(
        worker, *source_, true, 2,
        "superseded cluster rebuild did not close its old control socket");
    if (!peer.ok()) co_return peer;
    peer = co_await WaitForPeerCount(
        worker, *source_, false, 3,
        "replacement cluster rebuild did not open a new control connection");
    if (!peer.ok()) co_return peer;
    if (source_->error() != 0) {
      co_return TestFailure("stalling native source encountered an I/O error");
    }

    const keylane::ClusterPopulationStatus replaced =
        co_await replication_->cluster_population_status();
    if (replaced.state_ != keylane::ReplicationGroupState::kRebuilding ||
        replaced.ready_token_.has_value()) {
      co_return TestFailure(
          "replacement directive did not remain fail-closed while rebuilding");
    }

    auto stale = co_await replication_->StartClusterRebuildDirective(
        upstream, directive, *manifest);
    if (stale.status().code() != absl::StatusCode::kFailedPrecondition) {
      co_return TestFailure("supersession did not reject the stale directive");
    }

    keylane::DesiredClusterPopulation desired{
        .group_id_ = replacement.identity_.group_id_,
        .assignment_id_ = replacement.identity_.assignment_id_,
        .term_ = replacement.identity_.term_,
        .manifest_revision_ = replacement.identity_.manifest_revision_,
        .manifest_id_ = replacement.identity_.manifest_id_,
        .partition_replication_epoch_ =
            replacement.identity_.partition_replication_epoch_,
        .population_transition_expected_ = true,
    };
    absl::Status reconciled =
        co_await replication_->ReconcileClusterPopulation(desired);
    if (!reconciled.ok() || replacement_started->result().has_value()) {
      co_return TestFailure(
          "matching FDS reconciliation retired its live rebuild successor");
    }

    ++desired.partition_replication_epoch_;
    reconciled = co_await replication_->ReconcileClusterPopulation(desired);
    if (!reconciled.ok() || (co_await replacement_started->Await()).code() !=
                                absl::StatusCode::kCancelled) {
      co_return TestFailure(
          "FDS population epoch change did not retire the stale rebuild");
    }
    if (const absl::Status query = co_await CheckLightweightQueries(
            *replication_, std::nullopt, "population epoch reset");
        !query.ok()) {
      co_return query;
    }

    keylane::RebuildDirective after_reconcile = replacement;
    after_reconcile.identity_.directive_revision_ = 4;
    after_reconcile.identity_.attempt_id_ = "attempt-4";
    after_reconcile.identity_.partition_replication_epoch_ =
        desired.partition_replication_epoch_;
    auto restarted = co_await replication_->StartClusterRebuildDirective(
        upstream, after_reconcile, *manifest);
    if (!restarted.ok()) {
      co_return TestFailure(
          "FDS reconciliation permanently closed rebuild admission");
    }
    if (const absl::Status query = co_await CheckLightweightQueries(
            *replication_, upstream, "rebuild after reset");
        !query.ok()) {
      co_return query;
    }

    desired.population_transition_expected_ = false;
    reconciled = co_await replication_->ReconcileClusterPopulation(desired);
    if (!reconciled.ok() ||
        (co_await restarted->Await()).code() != absl::StatusCode::kCancelled) {
      co_return TestFailure(
          "FDS directive removal did not retire the orphan rebuild");
    }

    keylane::RebuildDirective after_directive_removal = after_reconcile;
    after_directive_removal.identity_.directive_revision_ = 5;
    after_directive_removal.identity_.attempt_id_ = "attempt-5";
    auto after_removal = co_await replication_->StartClusterRebuildDirective(
        upstream, after_directive_removal, *manifest);
    if (!after_removal.ok()) {
      co_return TestFailure(
          "directive removal reconciliation permanently closed admission");
    }
    absl::Status session_cancelled =
        co_await replication_->CancelInProgressClusterPopulation();
    if (!session_cancelled.ok() || (co_await after_removal->Await()).code() !=
                                       absl::StatusCode::kCancelled) {
      co_return TestFailure(
          "control loss did not cancel and retire an in-progress rebuild");
    }
    if (const absl::Status query = co_await CheckLightweightQueries(
            *replication_, std::nullopt, "control session cancellation");
        !query.ok()) {
      co_return query;
    }

    keylane::RebuildDirective at_shutdown = after_directive_removal;
    at_shutdown.identity_.directive_revision_ = 6;
    at_shutdown.identity_.attempt_id_ = "attempt-6";
    const unsigned accepted_before_shutdown = source_->accepted();
    auto shutdown_attempt =
        co_await replication_->StartClusterRebuildDirective(
            upstream, at_shutdown, *manifest);
    if (!shutdown_attempt.ok()) co_return shutdown_attempt.status();
    peer = co_await WaitForPeerCount(
        worker, *source_, false, accepted_before_shutdown + 1,
        "shutdown test did not enter its outbound control handshake");
    if (!peer.ok()) co_return peer;

    replication_->RequestShutdown();
    absl::Status cancelled =
        co_await replication_->QuiesceForShutdown();
    if (!cancelled.ok()) co_return cancelled;
    if ((co_await shutdown_attempt->Await()).code() !=
        absl::StatusCode::kCancelled) {
      co_return TestFailure(
          "process shutdown did not cancel the active rebuild completion");
    }
    peer = co_await WaitForPeerCount(
        worker, *source_, true, accepted_before_shutdown + 1,
        "process shutdown did not close the outbound control handshake");
    if (!peer.ok()) co_return peer;

    co_return absl::OkStatus();
  }

  keylane::storage::StorageEngine* storage_ = nullptr;
  keylane::ReplicationManager* replication_ = nullptr;
  StallingNativeSource* source_ = nullptr;
  std::string expected_node_id_;
  absl::Status result_ = absl::OkStatus();
};

enum class EmptyPopulationExpectation {
  kReady,
  kRecoverableFailure,
  kFailedStopped,
};

class EmptyPopulationService final : public celer::Service {
 public:
  EmptyPopulationService(keylane::storage::StorageEngine* storage,
                         keylane::ReplicationManager* replication,
                         EmptyPopulationExpectation expectation =
                             EmptyPopulationExpectation::kReady)
      : storage_(storage),
        replication_(replication),
        expectation_(expectation) {}

  void Prepare(unsigned thread_count) override {
    if (thread_count != 1) {
      result_ = TestFailure("empty population test requires one worker");
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
    worker.RequestStop();
    co_return result_;
  }

  void Stop() noexcept override {}

  const absl::Status& result() const noexcept { return result_; }

 private:
  celer::Task<absl::Status> Exercise() {
    const keylane::ReplicationIdentity local =
        co_await replication_->ObserveIdentity();
    const keylane::ClusterPopulationStatus cold =
        co_await replication_->cluster_population_status();
    if (cold.state_ != keylane::ReplicationGroupState::kNotReady ||
        cold.ready_token_.has_value() || !replication_->is_loading() ||
        !replication_->reject_writes()) {
      co_return TestFailure(
          "Meta-managed Data did not start fenced before initialization");
    }

    std::vector<keylane::PopulationManifestEntry> entries;
    entries.reserve(keylane::kReplicationPartitionCount);
    for (std::uint32_t partition = 0;
         partition < keylane::kReplicationPartitionCount; ++partition) {
      entries.push_back({partition, 1});
    }
    auto manifest =
        keylane::PopulationManifest::Create(std::move(entries));
    if (!manifest.ok()) co_return manifest.status();

    keylane::RebuildIdentity identity{
        .group_id_ = "group-1",
        .assignment_id_ = "assignment-a",
        .term_ = 1,
        .directive_revision_ = 1,
        .authority_id_ = "authority-1",
        .target_node_id_ = local.local_node_id_,
        .target_boot_id_ = local.boot_id_,
        .target_history_id_ = local.local_history_id_,
        .operation_id_ = "operation-a",
        .directive_id_ = "directive-a",
        .attempt_id_ = "attempt-a",
        .manifest_revision_ = 1,
        .manifest_id_ = manifest->id(),
        .partition_replication_epoch_ = 1,
    };
    for (const keylane::RebuildIdentity& stale : {
             [&] {
               auto value = identity;
               value.target_boot_id_ = std::string(40, 'f');
               return value;
             }(),
             [&] {
               auto value = identity;
               value.target_history_id_ = std::string(40, 'e');
               return value;
             }(),
         }) {
      auto rejected =
          co_await replication_->StartEmptyPopulationInitialization(stale,
                                                                    *manifest);
      if (rejected.ok() ||
          rejected.status().code() !=
              absl::StatusCode::kFailedPrecondition) {
        co_return TestFailure(
            "empty population accepted stale boot or history identity");
      }
    }
    auto started = co_await replication_->StartEmptyPopulationInitialization(
        identity, *manifest);
    if (!started.ok()) co_return started.status();
    const absl::Status completed = co_await started->Await();
    if (expectation_ != EmptyPopulationExpectation::kReady) {
      if (completed.ok()) {
        co_return TestFailure(
            "injected empty-population failure unexpectedly completed");
      }
      const keylane::ClusterPopulationStatus failed =
          co_await replication_->cluster_population_status();
      const keylane::ReplicationStatus observed =
          co_await replication_->Observe();
      const bool expect_fail_stop =
          expectation_ == EmptyPopulationExpectation::kFailedStopped;
      const keylane::ReplicationGroupState expected_state =
          expect_fail_stop ? keylane::ReplicationGroupState::kFailedStopped
                           : keylane::ReplicationGroupState::kNotReady;
      if (failed.state_ != expected_state ||
          failed.ready_token_.has_value() ||
          observed.failed_stopped_ != expect_fail_stop ||
          (expect_fail_stop && (failed.failure_reason_.empty() ||
                                observed.failure_reason_.empty() ||
                                !storage_->ReplicaRecoveryFenced())) ||
          !replication_->is_loading() || !replication_->reject_writes()) {
        co_return TestFailure(
            "failed empty population did not retain its serving fence");
      }
      auto retry = co_await replication_->StartEmptyPopulationInitialization(
          identity, *manifest);
      if (retry.ok() ||
          retry.status().code() != absl::StatusCode::kFailedPrecondition) {
        co_return TestFailure(
            "failed-stopped empty population accepted an exact retry");
      }
      co_return absl::OkStatus();
    }
    if (!completed.ok()) {
      co_return completed;
    }

    const keylane::ClusterPopulationStatus ready =
        co_await replication_->cluster_population_status();
    if (ready.state_ != keylane::ReplicationGroupState::kReady ||
        !ready.ready_token_.has_value() ||
        ready.ready_token_->identity() != identity ||
        !ready.ready_token_->cut_vector().empty() ||
        replication_->is_loading() || replication_->reject_writes()) {
      co_return TestFailure(
          "empty population did not publish the source-less ReadyToken");
    }

    keylane::DesiredClusterPopulation desired{
        .group_id_ = identity.group_id_,
        .assignment_id_ = identity.assignment_id_,
        .term_ = identity.term_,
        .manifest_revision_ = identity.manifest_revision_,
        .manifest_id_ = identity.manifest_id_,
        .partition_replication_epoch_ =
            identity.partition_replication_epoch_,
        .population_transition_expected_ = false,
    };
    if (absl::Status reconciled =
            co_await replication_->ReconcileClusterPopulation(desired);
        !reconciled.ok()) {
      co_return reconciled;
    }
    const keylane::ClusterPopulationStatus after_removal =
        co_await replication_->cluster_population_status();
    if (after_removal.state_ != keylane::ReplicationGroupState::kReady ||
        !after_removal.ready_token_.has_value() ||
        after_removal.ready_token_->identity() != identity) {
      co_return TestFailure(
          "matching FDS without the directive retired the ReadyToken");
    }
    co_return absl::OkStatus();
  }

  keylane::storage::StorageEngine* storage_ = nullptr;
  keylane::ReplicationManager* replication_ = nullptr;
  EmptyPopulationExpectation expectation_ =
      EmptyPopulationExpectation::kReady;
  absl::Status result_ = absl::OkStatus();
};

class StandaloneIdentityService final : public celer::Service {
 public:
  StandaloneIdentityService(keylane::ReplicationManager* first,
                            keylane::ReplicationManager* second,
                            keylane::ReplicationManager* static_cluster)
      : first_(first), second_(second), static_cluster_(static_cluster) {}

  void Prepare(unsigned) override {}

  celer::Task<absl::Status> Run(celer::Worker& worker,
                                celer::ServiceContext) override {
    result_ = co_await CheckLightweightQueries(*first_, std::nullopt,
                                               "standalone primary");
    if (result_.ok()) {
      result_ = co_await CheckLightweightQueries(
          *second_, keylane::ReplicaOfConfig{"127.0.0.1", 1},
          "configured standalone replica");
    }
    if (result_.ok()) {
      result_ = co_await CheckLightweightQueries(*static_cluster_, std::nullopt,
                                                 "static cluster primary");
    }
    if (!result_.ok()) {
      worker.RequestStop();
      co_return result_;
    }
    const keylane::ReplicationStatus first_status = co_await first_->Observe();
    const keylane::ReplicationStatus second_status =
        co_await second_->Observe();
    const keylane::ClusterPopulationStatus first_population =
        co_await first_->cluster_population_status();
    const keylane::ClusterPopulationStatus second_population =
        co_await second_->cluster_population_status();
    if (!IsCanonicalReplicationId(first_status.local_node_id_) ||
        !IsCanonicalReplicationId(second_status.local_node_id_) ||
        first_status.local_node_id_ == second_status.local_node_id_ ||
        first_population.local_node_id_ != first_status.local_node_id_ ||
        second_population.local_node_id_ != second_status.local_node_id_) {
      result_ = TestFailure(
          "default replication node identities were not unique canonical ids");
      worker.RequestStop();
      co_return result_;
    }

    const keylane::ReplicationStatus static_status =
        co_await static_cluster_->Observe();
    if (static_status.role_ != keylane::ReplicationRole::kMaster ||
        static_status.upstream_.has_value() || static_cluster_->is_loading() ||
        static_cluster_->reject_writes()) {
      result_ = TestFailure(
          "static cluster policy did not ignore a standalone initial "
          "upstream while preserving storage-based readiness");
      worker.RequestStop();
      co_return result_;
    }
    keylane::ReplicationDirective set_upstream{
        .kind_ = keylane::ReplicationDirective::Kind::kSetUpstream,
        .upstream_ = keylane::ReplicaOfConfig{"127.0.0.1", 1},
    };
    result_ = co_await static_cluster_->ApplyDirective(set_upstream);
    if (result_.code() != absl::StatusCode::kFailedPrecondition) {
      result_ = TestFailure(
          "static cluster manager accepted a standalone upstream directive");
      worker.RequestStop();
      co_return result_;
    }
    set_upstream.kind_ = keylane::ReplicationDirective::Kind::kAddUpstream;
    result_ = co_await static_cluster_->ApplyDirective(set_upstream);
    if (result_.code() != absl::StatusCode::kFailedPrecondition) {
      result_ = TestFailure(
          "static cluster manager accepted an added standalone upstream");
      worker.RequestStop();
      co_return result_;
    }
    auto manifest = keylane::PopulationManifest::Create({{0, 1}});
    if (!manifest.ok()) {
      result_ = manifest.status();
      worker.RequestStop();
      co_return result_;
    }
    const keylane::ClusterPopulationStatus static_population =
        co_await static_cluster_->cluster_population_status();
    auto rebuild = co_await static_cluster_->StartClusterRebuildDirective(
        keylane::ReplicaOfConfig{"127.0.0.1", 1},
        TargetDirective(static_population, *manifest), *manifest);
    if (rebuild.status().code() != absl::StatusCode::kFailedPrecondition) {
      result_ = TestFailure(
          "static cluster manager accepted a Meta population rebuild");
      worker.RequestStop();
      co_return result_;
    }
    result_ =
        co_await static_cluster_->ReconcileClusterPopulation(std::nullopt);
    if (result_.code() != absl::StatusCode::kFailedPrecondition) {
      result_ =
          TestFailure("static cluster manager accepted FDS reconciliation");
      worker.RequestStop();
      co_return result_;
    }
    result_ = co_await static_cluster_->CancelInProgressClusterPopulation();
    if (result_.code() != absl::StatusCode::kFailedPrecondition) {
      result_ = TestFailure(
          "static cluster manager accepted control-loss cancellation");
      worker.RequestStop();
      co_return result_;
    }
    result_ = co_await static_cluster_->CancelClusterRebuildForShutdown();
    if (result_.code() != absl::StatusCode::kFailedPrecondition) {
      result_ = TestFailure(
          "static cluster manager accepted Meta population shutdown");
      worker.RequestStop();
      co_return result_;
    }

    result_ =
        co_await first_
            ->ClearClusterRebuildSourceAuthorizationsForSessionReplacement();
    if (result_.code() != absl::StatusCode::kFailedPrecondition) {
      result_ = TestFailure(
          "standalone manager accepted cluster session authorization cleanup");
      worker.RequestStop();
      co_return result_;
    }
    result_ = co_await first_->RevokeClusterRebuildSourceAuthorizations();
    if (result_.code() != absl::StatusCode::kFailedPrecondition) {
      result_ =
          TestFailure("standalone manager accepted cluster source revocation");
    } else {
      result_ = absl::OkStatus();
    }
    worker.RequestStop();
    co_return result_;
  }

  void Stop() noexcept override {}

  const absl::Status& result() const noexcept { return result_; }

 private:
  keylane::ReplicationManager* first_ = nullptr;
  keylane::ReplicationManager* second_ = nullptr;
  keylane::ReplicationManager* static_cluster_ = nullptr;
  absl::Status result_ = absl::OkStatus();
};

TEST(ReplicationManagerIntegrationTest,
     ClusterControlApiStaysFailClosedAndSupersedesWholeSession) {
  const std::string expected_node_id(40, '9');
  constexpr std::uint16_t kReplicationPort = 6380;
  keylane::test::TempDirectory directory("cluster-manager-api");
  const std::filesystem::path data = directory.path() / "node.data";
  keylane::test::CreateDataFile(data, 128 * kMiB);

  StallingNativeSource source(expected_node_id, kReplicationPort);
  ASSERT_NE(source.port(), 0);
  ASSERT_EQ(source.error(), 0) << std::strerror(source.error());

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
  replication_options.node_id_override_ = expected_node_id;
  replication_options.listen_port_ = kReplicationPort;
  keylane::ReplicationManager replication(
      &storage, std::move(replication_options),
      keylane::ReplicaOfConfig{"127.0.0.1", source.port()});
  keylane::InitStorage(&storage, &replication);
  EnsureTxRuntime();

  ReplicationManagerService service(&storage, &replication, &source,
                                    expected_node_id);
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

TEST(ReplicationManagerIntegrationTest,
     InitializesAndRetainsSourceLessEmptyPopulation) {
  const std::string expected_node_id(40, '8');
  keylane::test::TempDirectory directory("empty-population");
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
  replication_options.node_id_override_ = expected_node_id;
  keylane::ReplicationManager replication(
      &storage, std::move(replication_options), std::nullopt);
  keylane::InitStorage(&storage, &replication);
  EnsureTxRuntime();

  EmptyPopulationService service(&storage, &replication);
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

#if KEYLANE_FAULTS_ENABLED
TEST(ReplicationManagerIntegrationTest,
     EmptyPopulationPromotionFailureRemainsFailedStopped) {
  ASSERT_EQ(::setenv("KEYLANE_REPLICATION_FAIL_PROMOTE_ONCE", "1", 1), 0);
  struct FaultReset {
    ~FaultReset() { (void)::unsetenv("KEYLANE_REPLICATION_FAIL_PROMOTE_ONCE"); }
  } fault_reset;

  const std::string expected_node_id(40, '9');
  keylane::test::TempDirectory directory("empty-population-promote-failure");
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
  replication_options.node_id_override_ = expected_node_id;
  keylane::ReplicationManager replication(
      &storage, std::move(replication_options), std::nullopt);
  keylane::InitStorage(&storage, &replication);
  EnsureTxRuntime();

  EmptyPopulationService service(
      &storage, &replication, EmptyPopulationExpectation::kFailedStopped);
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

void RunRecoverableEmptyPopulationFault(const char* environment_name,
                                        char node_id_digit,
                                        std::string_view directory_name) {
  ASSERT_EQ(::setenv(environment_name, "1", 1), 0);
  struct FaultReset {
    const char* name_;
    ~FaultReset() { (void)::unsetenv(name_); }
  } fault_reset{environment_name};

  const std::string expected_node_id(40, node_id_digit);
  keylane::test::TempDirectory directory(directory_name);
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
  replication_options.node_id_override_ = expected_node_id;
  keylane::ReplicationManager replication(
      &storage, std::move(replication_options), std::nullopt);
  keylane::InitStorage(&storage, &replication);
  EnsureTxRuntime();

  EmptyPopulationService service(
      &storage, &replication,
      EmptyPopulationExpectation::kRecoverableFailure);
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

TEST(ReplicationManagerIntegrationTest,
     EmptyPopulationResetFailureRemainsLoading) {
  RunRecoverableEmptyPopulationFault(
      "KEYLANE_REPLICATION_FAIL_EMPTY_RESET_ONCE", 'a',
      "empty-population-reset-failure");
}

TEST(ReplicationManagerIntegrationTest,
     EmptyPopulationCatalogFailureRemainsLoading) {
  RunRecoverableEmptyPopulationFault(
      "KEYLANE_REPLICATION_FAIL_EMPTY_CATALOG_ONCE", 'b',
      "empty-population-catalog-failure");
}
#endif

TEST(ReplicationManagerIntegrationTest,
     StandaloneManagersGenerateDistinctCanonicalNodeIdentities) {
  keylane::test::TempDirectory directory("standalone-manager-revoke");
  const std::filesystem::path data = directory.path() / "node.data";
  keylane::test::CreateDataFile(data, 128 * kMiB);

  keylane::storage::StorageEngineOptions storage_options;
  storage_options.data_files_ = {data.string()};
  keylane::storage::StorageEngine storage(std::move(storage_options));
  ASSERT_TRUE(storage.Prepare(1).ok());

  keylane::ReplicationManager first(&storage, keylane::ReplicationOptions{},
                                    std::nullopt);
  keylane::ReplicationManager second(&storage, keylane::ReplicationOptions{},
                                     keylane::ReplicaOfConfig{"127.0.0.1", 1});
  keylane::ReplicationOptions static_options;
  static_options.cluster_enabled_ = true;
  keylane::ReplicationManager static_cluster(
      &storage, std::move(static_options),
      keylane::ReplicaOfConfig{"127.0.0.1", 1});
  StandaloneIdentityService service(&first, &second, &static_cluster);
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
