#include <arpa/inet.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <limits>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "celer/net/server.h"
#include "gtest/gtest.h"
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

// A system-boundary peer that accepts a native control connection but never
// returns KLFULLRESYNC. Keeping the connection at that public protocol boundary
// makes REBUILDING deterministic without duplicating the replication source or
// exposing a test-only manager state mutation.
class StallingNativeSource {
 public:
  StallingNativeSource() {
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
      accepted_.fetch_add(1, std::memory_order_acq_rel);

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
  std::jthread thread_;
  std::atomic<int> connection_{-1};
  std::atomic<unsigned> accepted_{0};
  std::atomic<unsigned> closed_{0};
  std::atomic<int> error_{0};
};

keylane::RebuildDirective TargetDirective(
    const keylane::ClusterPopulationStatus& target,
    const keylane::PopulationManifest& manifest) {
  return keylane::RebuildDirective{
      .identity_ =
          {
              .group_id_ = "group-a",
              .assignment_id_ = "assignment-a",
              .term_ = 7,
              .directive_revision_ = 1,
              .authority_id_ = "authority-a",
              .source_node_id_ = std::string(40, 'a'),
              .source_boot_id_ = std::string(40, 'b'),
              .source_history_id_ = std::string(40, 'c'),
              .target_node_id_ = target.local_node_id_,
              .target_boot_id_ = target.local_boot_id_,
              .operation_id_ = "operation-a",
              .attempt_id_ = "attempt-1",
              .manifest_id_ = manifest.id(),
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
                            StallingNativeSource* source)
      : storage_(storage), replication_(replication), source_(source) {}

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
    if (initial.local_node_id_.empty() || initial.local_boot_id_.empty() ||
        initial.state_ != keylane::ReplicationGroupState::kNotReady ||
        initial.ready_token_.has_value()) {
      co_return TestFailure(
          "cluster population did not start with boot-scoped NOT_READY state");
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
    if (!applied.ok()) co_return applied;
    const keylane::ClusterPopulationStatus rebuilding =
        co_await replication_->cluster_population_status();
    if (rebuilding.state_ != keylane::ReplicationGroupState::kRebuilding ||
        rebuilding.ready_token_.has_value()) {
      co_return TestFailure("accepted cluster directive was not REBUILDING");
    }
    absl::Status peer = co_await WaitForPeerCount(
        worker, *source_, false, 1,
        "native source did not receive the first cluster rebuild connection");
    if (!peer.ok()) co_return peer;

    applied = co_await replication_->ApplyClusterRebuildDirective(
        upstream, directive, *manifest);
    if (!applied.ok()) {
      co_return TestFailure(
          "exact in-progress directive replay was not idempotent");
    }

    const std::uint16_t conflicting_port =
        source_->port() == std::numeric_limits<std::uint16_t>::max()
            ? static_cast<std::uint16_t>(source_->port() - 1)
            : static_cast<std::uint16_t>(source_->port() + 1);
    applied = co_await replication_->ApplyClusterRebuildDirective(
        keylane::ReplicaOfConfig{"127.0.0.1", conflicting_port}, directive,
        *manifest);
    if (applied.code() != absl::StatusCode::kFailedPrecondition) {
      co_return TestFailure(
          "exact directive replay accepted a conflicting source endpoint");
    }
    if (source_->accepted() != 1 || source_->closed() != 0) {
      co_return TestFailure(
          "conflicting endpoint replay disturbed the accepted session");
    }

    keylane::RebuildDirective replacement = directive;
    replacement.identity_.directive_revision_ = 2;
    replacement.identity_.attempt_id_ = "attempt-2";
    applied = co_await replication_->ApplyClusterRebuildDirective(
        upstream, replacement, *manifest);
    if (!applied.ok()) co_return applied;

    peer = co_await WaitForPeerCount(
        worker, *source_, true, 1,
        "superseded cluster rebuild did not close its old control socket");
    if (!peer.ok()) co_return peer;
    peer = co_await WaitForPeerCount(
        worker, *source_, false, 2,
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

    applied = co_await replication_->ApplyClusterRebuildDirective(
        upstream, directive, *manifest);
    if (applied.code() != absl::StatusCode::kFailedPrecondition) {
      co_return TestFailure("supersession did not reject the stale directive");
    }

    co_return absl::OkStatus();
  }

  keylane::storage::StorageEngine* storage_ = nullptr;
  keylane::ReplicationManager* replication_ = nullptr;
  StallingNativeSource* source_ = nullptr;
  absl::Status result_ = absl::OkStatus();
};

TEST(ReplicationManagerIntegrationTest,
     ClusterControlApiStaysFailClosedAndSupersedesWholeSession) {
  keylane::test::TempDirectory directory("cluster-manager-api");
  const std::filesystem::path data = directory.path() / "node.data";
  keylane::test::CreateDataFile(data, 128 * kMiB);

  StallingNativeSource source;
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
  replication_options.listen_port_ = 6380;
  keylane::ReplicationManager replication(
      &storage, std::move(replication_options),
      keylane::ReplicaOfConfig{"127.0.0.1", source.port()});
  keylane::InitStorage(&storage, &replication);
  keylane::tx::TxRuntime::Create(1);

  ReplicationManagerService service(&storage, &replication, &source);
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
