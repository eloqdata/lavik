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

#include <arpa/inet.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "bycorf/net/connection.h"
#include "bycorf/net/server.h"
#include "bycorf/runtime/cross_core.h"
#include "gtest/gtest.h"
#include "lavik/cluster/lease_clock.h"
#include "lavik/command.h"
#include "lavik/fault_injection.h"
#include "lavik/memory.h"
#include "lavik/metrics.h"
#include "lavik/replication.h"
#include "lavik/storage/engine.h"
#include "lavik/tx/tx_shard.h"
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
  if (lavik::tx::TxRuntime::Get() == nullptr) {
    lavik::tx::TxRuntime::Create(1);
  }
}

bycorf::Task<absl::Status> AwaitFaultBarrier(const std::filesystem::path& path,
                                             std::string_view description) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  do {
    std::error_code error;
    if (std::filesystem::exists(path, error)) co_return absl::OkStatus();
    if (error) {
      co_return absl::InternalError(absl::StrCat(
          "could not observe ", description, ": ", error.message()));
    }
    absl::Status waited = co_await bycorf::SleepFor(
        *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
    if (!waited.ok()) co_return waited;
  } while (std::chrono::steady_clock::now() < deadline);
  co_return TestFailure(absl::StrCat(description, " was not acknowledged"));
}

bycorf::Task<absl::Status> CheckLightweightQueries(
    const lavik::ReplicationManager& replication,
    const std::optional<lavik::ReplicaOfConfig>& expected_upstream,
    std::string_view phase) {
  const lavik::ReplicationIdentity identity =
      co_await replication.ObserveIdentity();
  const auto upstream = replication.upstream();
  const lavik::ReplicationStatus status = co_await replication.Observe();
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

std::string HexString(std::string_view value) {
  constexpr char kHex[] = "0123456789abcdef";
  std::string result;
  result.reserve(value.size() * 2);
  for (unsigned char byte : value) {
    result.push_back(kHex[byte >> 4]);
    result.push_back(kHex[byte & 0x0f]);
  }
  return result;
}

// A system-boundary peer that returns one well-formed LVFULLRESYNC carrying the
// wrong group, then stalls later connections. This exercises target identity
// validation at the wire boundary and keeps subsequent REBUILDING states
// deterministic without exposing a test-only manager state mutation.
class StallingNativeSource {
 public:
  StallingNativeSource(std::string expected_target_node_id,
                       std::uint16_t target_port,
                       unsigned lease_suspended_responses = 0)
      : expected_client_identity_(RespBulk("?" + expected_target_node_id + ":" +
                                           std::to_string(target_port))),
        expected_population_target_(RespBulk(expected_target_node_id)),
        expected_population_epoch_(
            RespBulk(std::to_string(kPartitionReplicationEpoch))),
        first_response_("+LVFULLRESYNC 1 " + std::string(40, 'a') + " " +
                        std::string(40, 'e') + " " + std::string(40, 'b') +
                        " " + std::string(40, 'c') + " 1 " +
                        std::string(40, 'f') + "\r\n"),
        lease_suspended_responses_(lease_suspended_responses) {
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
      if (accepted <= lease_suspended_responses_) {
        constexpr std::string_view kSuspended = "-LVLEASESUSPENDED\r\n";
        const ssize_t sent = ::send(connection, kSuspended.data(),
                                    kSuspended.size(), MSG_NOSIGNAL);
        if (sent != static_cast<ssize_t>(kSuspended.size())) {
          error_.store(errno == 0 ? EIO : errno, std::memory_order_release);
          (void)::close(connection);
          return;
        }
      } else if (accepted == lease_suspended_responses_ + 1) {
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
  const unsigned lease_suspended_responses_ = 0;
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

// A steady-state Owner endpoint that can hold the authenticated control
// handshake before export readiness, or publish one exact source incarnation
// and then stall its data flow. This keeps the test focused on the target's
// desired-state boundary rather than reimplementing native FULL in a fixture.
class FollowOwnerSource {
 public:
  FollowOwnerSource(std::string source_node_id, std::string source_boot_id,
                    std::string source_history_id, std::string group_token,
                    bool export_ready, std::string flow_mode = "FULL")
      : source_node_id_(std::move(source_node_id)),
        source_boot_id_(std::move(source_boot_id)),
        source_history_id_(std::move(source_history_id)),
        group_token_(std::move(group_token)),
        export_ready_(export_ready),
        flow_mode_(std::move(flow_mode)) {
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
        ::listen(listener_, 8) != 0) {
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

  FollowOwnerSource(const FollowOwnerSource&) = delete;
  FollowOwnerSource& operator=(const FollowOwnerSource&) = delete;

  ~FollowOwnerSource() {
    thread_.request_stop();
    if (listener_ >= 0) (void)::shutdown(listener_, SHUT_RDWR);
    if (thread_.joinable()) thread_.join();
    {
      std::lock_guard lock(connections_mutex_);
      for (int connection : connections_) {
        (void)::shutdown(connection, SHUT_RDWR);
      }
    }
    for (std::jthread& handler : handlers_) handler.request_stop();
    for (std::jthread& handler : handlers_) {
      if (handler.joinable()) handler.join();
    }
    if (listener_ >= 0) (void)::close(listener_);
  }

  std::uint16_t port() const noexcept { return port_; }
  unsigned controls() const noexcept {
    return controls_.load(std::memory_order_acquire);
  }
  unsigned flows() const noexcept {
    return flows_.load(std::memory_order_acquire);
  }
  unsigned closed() const noexcept {
    return closed_.load(std::memory_order_acquire);
  }
  bool saw_follow_scope() const noexcept {
    return saw_follow_scope_.load(std::memory_order_acquire);
  }
  bool saw_resume_proof() const noexcept {
    return saw_resume_proof_.load(std::memory_order_acquire);
  }
  bool sent_continue() const noexcept {
    return sent_continue_.load(std::memory_order_acquire);
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
        if (!stop.stop_requested()) {
          error_.store(EIO, std::memory_order_release);
        }
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
      {
        std::lock_guard lock(connections_mutex_);
        connections_.push_back(connection);
      }
      handlers_.emplace_back([this, connection](std::stop_token handler_stop) {
        HandleConnection(connection, handler_stop);
        {
          std::lock_guard lock(connections_mutex_);
          const auto found =
              std::find(connections_.begin(), connections_.end(), connection);
          if (found != connections_.end()) connections_.erase(found);
        }
        (void)::close(connection);
      });
    }
  }

  void HandleConnection(int connection, std::stop_token stop) noexcept {
    std::string request;
    bool replied = false;
    bool control = false;
    while (!stop.stop_requested()) {
      pollfd peer{.fd = connection, .events = POLLIN | POLLRDHUP, .revents = 0};
      const int activity = ::poll(&peer, 1, 50);
      if (activity < 0) {
        if (errno == EINTR) continue;
        error_.store(errno == 0 ? EIO : errno, std::memory_order_release);
        return;
      }
      if (activity > 0 &&
          (peer.revents & (POLLHUP | POLLRDHUP | POLLERR | POLLNVAL)) != 0) {
        closed_.fetch_add(1, std::memory_order_acq_rel);
        return;
      }
      if (activity > 0 && (peer.revents & POLLIN) != 0) {
        char buffer[4096];
        const ssize_t received = ::recv(connection, buffer, sizeof(buffer), 0);
        if (received == 0) {
          closed_.fetch_add(1, std::memory_order_acq_rel);
          return;
        }
        if (received < 0) {
          if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
            continue;
          }
          error_.store(errno == 0 ? EIO : errno, std::memory_order_release);
          return;
        }
        request.append(buffer, static_cast<std::size_t>(received));
        if (!control && request.find("LVPSYNC") != std::string::npos) {
          control = true;
          controls_.fetch_add(1, std::memory_order_acq_rel);
        } else if (request.find("LVFLOW") != std::string::npos) {
          flows_.fetch_add(1, std::memory_order_acq_rel);
          const std::string response = "+LVFLOW 1 0 " + flow_mode_ + "\r\n";
          const ssize_t sent = ::send(connection, response.data(),
                                      response.size(), MSG_NOSIGNAL);
          if (sent != static_cast<ssize_t>(response.size())) {
            error_.store(errno == 0 ? EIO : errno, std::memory_order_release);
            return;
          }
          if (flow_mode_ == "CONTINUE") {
            sent_continue_.store(true, std::memory_order_release);
          }
          replied = true;
        }
      }
      if (!control || replied || !export_ready_) continue;
      constexpr std::string_view kFollow = "$6\r\nFOLLOW\r\n";
      if (request.find(kFollow) == std::string::npos) continue;
      saw_follow_scope_.store(true, std::memory_order_release);
      if (request.find(RespBulk(group_token_)) != std::string::npos &&
          request.find(RespBulk(source_history_id_)) != std::string::npos) {
        saw_resume_proof_.store(true, std::memory_order_release);
      }
      const std::string response = "+LVFULLRESYNC 1 " + source_node_id_ + " " +
                                   group_token_ + " " + source_boot_id_ + " " +
                                   source_history_id_ + " 1 " +
                                   std::string(40, 'f') + "\r\n";
      const ssize_t sent =
          ::send(connection, response.data(), response.size(), MSG_NOSIGNAL);
      if (sent != static_cast<ssize_t>(response.size())) {
        error_.store(errno == 0 ? EIO : errno, std::memory_order_release);
        return;
      }
      replied = true;
    }
  }

  int listener_ = -1;
  std::uint16_t port_ = 0;
  std::string source_node_id_;
  std::string source_boot_id_;
  std::string source_history_id_;
  std::string group_token_;
  bool export_ready_ = false;
  std::string flow_mode_;
  std::jthread thread_;
  std::vector<std::jthread> handlers_;
  std::mutex connections_mutex_;
  std::vector<int> connections_;
  std::atomic<unsigned> controls_{0};
  std::atomic<unsigned> flows_{0};
  std::atomic<unsigned> closed_{0};
  std::atomic<bool> saw_follow_scope_{false};
  std::atomic<bool> saw_resume_proof_{false};
  std::atomic<bool> sent_continue_{false};
  std::atomic<int> error_{0};
};

lavik::RebuildDirective TargetDirective(
    const lavik::ClusterPopulationStatus& target,
    const lavik::PopulationManifest& manifest) {
  return lavik::RebuildDirective{
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

bycorf::Task<absl::Status> WaitForPeerCount(bycorf::Worker& worker,
                                            const StallingNativeSource& source,
                                            bool closed, unsigned expected,
                                            std::string_view description) {
  const auto count = [&] {
    return closed ? source.closed() : source.accepted();
  };
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (count() < expected && std::chrono::steady_clock::now() < deadline) {
    absl::Status waited = co_await bycorf::SleepFor(worker, 1ms);
    if (!waited.ok()) co_return waited;
  }
  if (count() < expected) {
    co_return absl::DeadlineExceededError(std::string(description));
  }
  co_return absl::OkStatus();
}

class ReplicationManagerService final : public bycorf::Service {
 public:
  ReplicationManagerService(lavik::storage::StorageEngine* storage,
                            lavik::ReplicationManager* replication,
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

  bycorf::Task<absl::Status> Run(bycorf::Worker& worker,
                                 bycorf::ServiceContext) override {
    lavik::BindMemoryAccountingShard(worker.id());
    lavik::tx::TxRuntime::Get()->shard(worker.id()).Bind(worker);
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
  bycorf::Task<absl::Status> Exercise(bycorf::Worker& worker) {
    if (source_->port() == 0 || source_->error() != 0) {
      co_return TestFailure("stalling native source failed to start");
    }

    const lavik::ClusterPopulationStatus initial =
        co_await replication_->cluster_population_status();
    if (initial.local_node_id_ != expected_node_id_ ||
        !IsCanonicalReplicationId(initial.local_boot_id_) ||
        initial.state_ != lavik::ReplicationGroupState::kNotReady ||
        initial.ready_token_.has_value()) {
      co_return TestFailure(
          "cluster population did not use the configured node identity");
    }
    if (const absl::Status query = co_await CheckLightweightQueries(
            *replication_, std::nullopt, "cluster startup");
        !query.ok()) {
      co_return query;
    }
    absl::Status startup_wait = co_await bycorf::SleepFor(worker, 50ms);
    if (!startup_wait.ok()) co_return startup_wait;
    if (source_->accepted() != 0) {
      co_return TestFailure(
          "cluster-enabled manager used a standalone initial upstream");
    }

    auto manifest = lavik::PopulationManifest::Create({{42, 9}, {16'383, 11}});
    if (!manifest.ok()) co_return manifest.status();
    lavik::RebuildDirective directive = TargetDirective(initial, *manifest);
    const lavik::ReplicaOfConfig upstream{"127.0.0.1", source_->port()};

    lavik::RebuildDirective wrong_boot = directive;
    wrong_boot.identity_.target_boot_id_ = std::string(40, 'd');
    absl::Status applied = co_await replication_->ApplyClusterRebuildDirective(
        upstream, wrong_boot, *manifest);
    if (applied.code() != absl::StatusCode::kFailedPrecondition) {
      co_return TestFailure("cluster rebuild accepted the wrong target boot");
    }

    auto other_manifest = lavik::PopulationManifest::Create({{7, 1}});
    if (!other_manifest.ok()) co_return other_manifest.status();
    applied = co_await replication_->ApplyClusterRebuildDirective(
        upstream, directive, *other_manifest);
    if (applied.code() != absl::StatusCode::kFailedPrecondition) {
      co_return TestFailure("cluster rebuild accepted a mismatched manifest");
    }

    applied = co_await replication_->ApplyClusterRebuildDirective(
        lavik::ReplicaOfConfig{}, directive, *manifest);
    if (applied.code() != absl::StatusCode::kInvalidArgument) {
      co_return TestFailure(
          "cluster rebuild accepted an empty source endpoint");
    }
    const lavik::ClusterPopulationStatus after_invalid =
        co_await replication_->cluster_population_status();
    if (after_invalid.state_ != lavik::ReplicationGroupState::kNotReady ||
        after_invalid.ready_token_.has_value()) {
      co_return TestFailure("an invalid directive changed population state");
    }

    const lavik::ReplicationStatus replication_status =
        co_await replication_->Observe();
    if (replication_status.local_node_id_ != expected_node_id_ ||
        replication_status.boot_id_ != initial.local_boot_id_) {
      co_return TestFailure(
          "replication status did not preserve the configured node identity");
    }
    lavik::RebuildDirective source_authorization = directive;
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

    lavik::ClusterPopulationStatus after_mismatch;
    const auto mismatch_deadline = std::chrono::steady_clock::now() + 5s;
    do {
      after_mismatch = co_await replication_->cluster_population_status();
      if (after_mismatch.state_ == lavik::ReplicationGroupState::kNotReady)
        break;
      absl::Status waited = co_await bycorf::SleepFor(worker, 1ms);
      if (!waited.ok()) co_return waited;
    } while (std::chrono::steady_clock::now() < mismatch_deadline);
    if (after_mismatch.state_ != lavik::ReplicationGroupState::kNotReady ||
        after_mismatch.ready_token_.has_value()) {
      co_return TestFailure(
          "mismatched source group did not retire the rebuild attempt");
    }

    directive.identity_.directive_revision_ = 2;
    directive.identity_.attempt_id_ = "attempt-2";
    auto started = co_await replication_->StartClusterRebuildDirective(
        upstream, directive, *manifest);
    if (!started.ok()) co_return started.status();
    lavik::ClusterRebuildCompletion in_progress = std::move(*started);
    const lavik::ClusterPopulationStatus rebuilding =
        co_await replication_->cluster_population_status();
    if (rebuilding.state_ != lavik::ReplicationGroupState::kRebuilding ||
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
        lavik::ReplicaOfConfig{"127.0.0.1", conflicting_port}, directive,
        *manifest);
    if (conflicting.status().code() != absl::StatusCode::kFailedPrecondition) {
      co_return TestFailure(
          "exact directive replay accepted a conflicting source endpoint");
    }
    if (source_->accepted() != 2 || source_->closed() != 1) {
      co_return TestFailure(
          "conflicting endpoint replay disturbed the accepted session");
    }

    lavik::RebuildDirective replacement = directive;
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

    const lavik::ClusterPopulationStatus replaced =
        co_await replication_->cluster_population_status();
    if (replaced.state_ != lavik::ReplicationGroupState::kRebuilding ||
        replaced.ready_token_.has_value()) {
      co_return TestFailure(
          "replacement directive did not remain fail-closed while rebuilding");
    }

    auto stale = co_await replication_->StartClusterRebuildDirective(
        upstream, directive, *manifest);
    if (stale.status().code() != absl::StatusCode::kFailedPrecondition) {
      co_return TestFailure("supersession did not reject the stale directive");
    }

    lavik::DesiredClusterPopulation desired{
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

    lavik::RebuildDirective after_reconcile = replacement;
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

    lavik::RebuildDirective after_directive_removal = after_reconcile;
    after_directive_removal.identity_.directive_revision_ = 5;
    after_directive_removal.identity_.attempt_id_ = "attempt-5";
    auto after_removal = co_await replication_->StartClusterRebuildDirective(
        upstream, after_directive_removal, *manifest);
    if (!after_removal.ok()) {
      co_return TestFailure(
          "directive removal reconciliation permanently closed admission");
    }
    absl::Status session_cancelled =
        co_await replication_->CancelInProgressClusterPopulation(
            /*preserve_current_follow_attempt=*/false);
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

    lavik::RebuildDirective at_shutdown = after_directive_removal;
    at_shutdown.identity_.directive_revision_ = 6;
    at_shutdown.identity_.attempt_id_ = "attempt-6";
    const unsigned accepted_before_shutdown = source_->accepted();
    auto shutdown_attempt = co_await replication_->StartClusterRebuildDirective(
        upstream, at_shutdown, *manifest);
    if (!shutdown_attempt.ok()) co_return shutdown_attempt.status();
    peer = co_await WaitForPeerCount(
        worker, *source_, false, accepted_before_shutdown + 1,
        "shutdown test did not enter its outbound control handshake");
    if (!peer.ok()) co_return peer;

    replication_->RequestShutdown();
    absl::Status cancelled = co_await replication_->QuiesceForShutdown();
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

  lavik::storage::StorageEngine* storage_ = nullptr;
  lavik::ReplicationManager* replication_ = nullptr;
  StallingNativeSource* source_ = nullptr;
  std::string expected_node_id_;
  absl::Status result_ = absl::OkStatus();
};

class TargetLeaseAdmissionRetryService final : public bycorf::Service {
 public:
  TargetLeaseAdmissionRetryService(lavik::storage::StorageEngine* storage,
                                   lavik::ReplicationManager* replication,
                                   StallingNativeSource* source,
                                   unsigned expected_connections,
                                   absl::StatusCode expected_terminal)
      : storage_(storage),
        replication_(replication),
        source_(source),
        expected_connections_(expected_connections),
        expected_terminal_(expected_terminal) {}

  void Prepare(unsigned thread_count) override {
    if (thread_count != 1) {
      result_ = TestFailure("target lease retry test requires one worker");
    }
  }

  bycorf::Task<absl::Status> Run(bycorf::Worker& worker,
                                 bycorf::ServiceContext) override {
    lavik::BindMemoryAccountingShard(worker.id());
    lavik::tx::TxRuntime::Get()->shard(worker.id()).Bind(worker);
    if (result_.ok()) result_ = co_await storage_->InitializeWorker(worker);
    if (result_.ok()) {
      replication_->StorageReady(worker);
      result_ = co_await Exercise(worker);
    }
    replication_->RequestShutdown();
    const absl::Status quiesced = co_await replication_->QuiesceForShutdown();
    if (result_.ok() && !quiesced.ok()) result_ = quiesced;
    worker.RequestStop();
    co_return result_;
  }

  void Stop() noexcept override {}
  const absl::Status& result() const noexcept { return result_; }

 private:
  bycorf::Task<absl::Status> Exercise(bycorf::Worker& worker) {
    if (source_->port() == 0 || source_->error() != 0) {
      co_return TestFailure("scripted native source failed to start");
    }
    const lavik::ClusterPopulationStatus initial =
        co_await replication_->cluster_population_status();
    auto manifest = lavik::PopulationManifest::Create({{42, 9}, {16'383, 11}});
    if (!manifest.ok()) co_return manifest.status();
    lavik::RebuildDirective directive = TargetDirective(initial, *manifest);
    const lavik::ReplicaOfConfig upstream{"127.0.0.1", source_->port()};
    const auto started_at = std::chrono::steady_clock::now();
    auto started = co_await replication_->StartClusterRebuildDirective(
        upstream, directive, *manifest);
    if (!started.ok()) co_return started.status();
    const absl::Status terminal = co_await started->Await();
    const auto elapsed = std::chrono::steady_clock::now() - started_at;
    if (terminal.code() != expected_terminal_) {
      co_return TestFailure(absl::StrCat("target lease retry returned ",
                                         terminal, " instead of status code ",
                                         static_cast<int>(expected_terminal_)));
    }
    absl::Status reached = co_await WaitForPeerCount(
        worker, *source_, false, expected_connections_,
        "target did not perform the expected bounded lease retries");
    if (!reached.ok()) co_return reached;
    const auto minimum =
        std::chrono::milliseconds(800 * (expected_connections_ - 1));
    if (elapsed < minimum) {
      co_return TestFailure("lease admission retries did not use fixed delay");
    }
    const unsigned accepted_at_terminal = source_->accepted();
    absl::Status waited = co_await bycorf::SleepFor(worker, 1200ms);
    if (!waited.ok()) co_return waited;
    if (source_->accepted() != accepted_at_terminal) {
      co_return TestFailure(
          "terminal non-lease/protocol failure started another retry");
    }
    const lavik::ClusterPopulationStatus population =
        co_await replication_->cluster_population_status();
    if (population.state_ != lavik::ReplicationGroupState::kNotReady ||
        population.ready_token_.has_value()) {
      co_return TestFailure("terminal retry outcome retained a rebuild proof");
    }
    co_return absl::OkStatus();
  }

  lavik::storage::StorageEngine* storage_ = nullptr;
  lavik::ReplicationManager* replication_ = nullptr;
  StallingNativeSource* source_ = nullptr;
  unsigned expected_connections_ = 0;
  absl::StatusCode expected_terminal_ = absl::StatusCode::kUnknown;
  absl::Status result_ = absl::OkStatus();
};

enum class EmptyPopulationExpectation {
  kReady,
  kRecoverableFailure,
  kFailedStopped,
};

class EmptyPopulationService final : public bycorf::Service {
 public:
  EmptyPopulationService(lavik::storage::StorageEngine* storage,
                         lavik::ReplicationManager* replication,
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

  bycorf::Task<absl::Status> Run(bycorf::Worker& worker,
                                 bycorf::ServiceContext) override {
    lavik::BindMemoryAccountingShard(worker.id());
    lavik::tx::TxRuntime::Get()->shard(worker.id()).Bind(worker);
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
  bycorf::Task<absl::Status> Exercise() {
    const lavik::ReplicationIdentity local =
        co_await replication_->ObserveIdentity();
    const lavik::ClusterPopulationStatus cold =
        co_await replication_->cluster_population_status();
    if (cold.state_ != lavik::ReplicationGroupState::kNotReady ||
        cold.ready_token_.has_value() || !replication_->is_loading() ||
        !replication_->reject_writes()) {
      co_return TestFailure(
          "Meta-managed Data did not start fenced before initialization");
    }

    std::vector<lavik::PopulationManifestEntry> entries;
    entries.reserve(lavik::kReplicationPartitionCount);
    for (std::uint32_t partition = 0;
         partition < lavik::kReplicationPartitionCount; ++partition) {
      entries.push_back({partition, 1});
    }
    auto manifest = lavik::PopulationManifest::Create(std::move(entries));
    if (!manifest.ok()) co_return manifest.status();

    lavik::RebuildIdentity identity{
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
    for (const lavik::RebuildIdentity& stale : {
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
      auto rejected = co_await replication_->StartEmptyPopulationInitialization(
          stale, *manifest);
      if (rejected.ok() ||
          rejected.status().code() != absl::StatusCode::kFailedPrecondition) {
        co_return TestFailure(
            "empty population accepted stale boot or history identity");
      }
    }
    auto started = co_await replication_->StartEmptyPopulationInitialization(
        identity, *manifest);
    if (!started.ok()) co_return started.status();
    std::optional<lavik::ClusterRebuildCompletion> in_progress_replay;
    if (expectation_ == EmptyPopulationExpectation::kReady) {
      auto replay = co_await replication_->StartEmptyPopulationInitialization(
          identity, *manifest);
      if (!replay.ok()) co_return replay.status();
      in_progress_replay = std::move(*replay);
    }
    const absl::Status completed = co_await started->Await();
    if (expectation_ != EmptyPopulationExpectation::kReady) {
      if (completed.ok()) {
        co_return TestFailure(
            "injected empty-population failure unexpectedly completed");
      }
      const lavik::ClusterPopulationStatus failed =
          co_await replication_->cluster_population_status();
      const lavik::ReplicationStatus observed =
          co_await replication_->Observe();
      const bool expect_fail_stop =
          expectation_ == EmptyPopulationExpectation::kFailedStopped;
      const lavik::ReplicationGroupState expected_state =
          expect_fail_stop ? lavik::ReplicationGroupState::kFailedStopped
                           : lavik::ReplicationGroupState::kNotReady;
      if (failed.state_ != expected_state || failed.ready_token_.has_value() ||
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
    if (!in_progress_replay.has_value()) {
      co_return TestFailure("empty population replay lost the original result");
    }
    // GCC 13 ICEs if these suspension results remain inside the surrounding
    // compound conditions after the worker-owner regression enlarged this TU.
    const absl::Status in_progress_result =
        co_await in_progress_replay->Await();
    if (in_progress_result != completed)
      co_return TestFailure("empty population replay lost the original result");

    const lavik::ClusterPopulationStatus ready =
        co_await replication_->cluster_population_status();
    if (ready.state_ != lavik::ReplicationGroupState::kReady ||
        !ready.ready_token_.has_value() ||
        ready.ready_token_->identity() != identity ||
        !ready.ready_token_->cut_vector().empty() ||
        replication_->is_loading() || replication_->reject_writes()) {
      co_return TestFailure(
          "empty population did not publish the source-less ReadyToken");
    }

    // Ready is only a population fact. Before NodeControl installs a finite
    // write lease, an expired read must not acquire background mutation
    // authority. Queueing the same key after a finite grant proves the test is
    // observing expiration admission rather than a dormant worker.
    constexpr std::string_view kLeaseProbeKey = "empty-lease-probe-{foo}";
    absl::Status configured = storage_->ConfigureActiveExpiration(
        lavik::storage::ActiveExpirationConfigKey::kIntervalMs, 1);
    if (!configured.ok()) co_return configured;
    configured = storage_->ConfigureActiveExpiration(
        lavik::storage::ActiveExpirationConfigKey::kDeletesPerCycle, 1);
    if (!configured.ok()) co_return configured;
    auto lease_probe =
        co_await storage_->Set(0, kLeaseProbeKey, "expired",
                               lavik::storage::SetOptions{.expire_at_ms_ = 1});
    if (!lease_probe.ok()) co_return lease_probe.status();
    auto absent = co_await storage_->Get(0, kLeaseProbeKey);
    if (absent.ok() || absent.status().code() != absl::StatusCode::kNotFound) {
      co_return TestFailure("expiration lease probe was not logically absent");
    }
    absl::Status waited = co_await bycorf::SleepFor(
        *bycorf::ThisWorker().self_, std::chrono::milliseconds(50));
    if (!waited.ok()) co_return waited;
    if (storage_->LocalSize(0) != 1) {
      co_return TestFailure(
          "empty population installed expiration authority without a lease");
    }
    absl::Status expiration =
        co_await replication_->EnableClusterExpirationAuthorityUntil(
            std::chrono::nanoseconds::max() - std::chrono::nanoseconds(1));
    if (!expiration.ok()) co_return expiration;
    absent = co_await storage_->Get(0, kLeaseProbeKey);
    if (absent.ok() || absent.status().code() != absl::StatusCode::kNotFound) {
      co_return TestFailure("finite expiration lease revived an expired key");
    }
    for (std::size_t attempt = 0; attempt < 1000 && storage_->LocalSize(0) != 0;
         ++attempt) {
      waited = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                         std::chrono::milliseconds(1));
      if (!waited.ok()) co_return waited;
    }
    expiration = co_await replication_->RevokeClusterExpirationAuthority();
    if (!expiration.ok()) co_return expiration;
    if (storage_->LocalSize(0) != 0) {
      co_return TestFailure(
          "finite expiration lease did not admit queued cleanup");
    }

    // A completed replay must reuse the original result, not start another
    // destructive reset. Keep post-initialization data and its DB epoch as
    // observable evidence that neither replay nor a rejected mismatch resets.
    constexpr std::string_view kReplayKey = "after-empty-initialization";
    constexpr std::string_view kReplayValue = "must-survive-replay";
    auto written = co_await storage_->Set(0, kReplayKey, kReplayValue);
    if (!written.ok()) co_return written.status();
    const std::uint64_t db_epoch = storage_->DbEpoch(0);
    auto lookup = replication_->FindCompletedClusterPopulation(
        lavik::RebuildDirective{.identity_ = identity});
    if (!lookup.has_value() ||
        lookup->result() != std::optional<absl::Status>(completed)) {
      co_return TestFailure("non-mutating replay lookup lost completed result");
    }
    auto replay = co_await replication_->StartEmptyPopulationInitialization(
        identity, *manifest);
    if (!replay.ok()) co_return replay.status();
    const absl::Status replay_result = co_await replay->Await();
    if (replay->result() != std::optional<absl::Status>(completed) ||
        replay_result != completed) {
      co_return TestFailure(
          "completed empty population did not replay its result");
    }

    for (auto field : {&lavik::RebuildIdentity::group_id_,
                       &lavik::RebuildIdentity::assignment_id_,
                       &lavik::RebuildIdentity::target_node_id_,
                       &lavik::RebuildIdentity::target_boot_id_,
                       &lavik::RebuildIdentity::target_history_id_,
                       &lavik::RebuildIdentity::operation_id_,
                       &lavik::RebuildIdentity::directive_id_,
                       &lavik::RebuildIdentity::attempt_id_}) {
      auto mismatched = identity;
      mismatched.*field += "-other";
      if (replication_
              ->FindCompletedClusterPopulation(
                  lavik::RebuildDirective{.identity_ = mismatched})
              .has_value()) {
        co_return TestFailure("completed lookup accepted mismatched identity");
      }
      auto rejected = co_await replication_->StartEmptyPopulationInitialization(
          mismatched, *manifest);
      if (rejected.ok() ||
          rejected.status().code() != absl::StatusCode::kFailedPrecondition) {
        co_return TestFailure(
            "empty population replay accepted another identity");
      }
    }
    auto different_manifest = lavik::PopulationManifest::Create({{0, 2}});
    if (!different_manifest.ok()) co_return different_manifest.status();
    auto wrong_manifest =
        co_await replication_->StartEmptyPopulationInitialization(
            identity, *different_manifest);
    if (wrong_manifest.ok() || wrong_manifest.status().code() !=
                                   absl::StatusCode::kFailedPrecondition) {
      co_return TestFailure(
          "empty population replay accepted another manifest");
    }

    lavik::DesiredClusterPopulation desired{
        .group_id_ = identity.group_id_,
        .assignment_id_ = identity.assignment_id_,
        .term_ = identity.term_,
        .manifest_revision_ = identity.manifest_revision_,
        .manifest_id_ = identity.manifest_id_,
        .partition_replication_epoch_ = identity.partition_replication_epoch_,
        .population_transition_expected_ = false,
    };
    if (absl::Status reconciled =
            co_await replication_->ReconcileClusterPopulation(desired);
        !reconciled.ok()) {
      co_return reconciled;
    }
    const lavik::ClusterPopulationStatus after_removal =
        co_await replication_->cluster_population_status();
    if (after_removal.state_ != lavik::ReplicationGroupState::kReady ||
        !after_removal.ready_token_.has_value() ||
        after_removal.ready_token_->identity() != identity) {
      co_return TestFailure(
          "matching FDS without the directive retired the ReadyToken");
    }
    auto retained_replay =
        co_await replication_->StartEmptyPopulationInitialization(identity,
                                                                  *manifest);
    if (!retained_replay.ok()) co_return retained_replay.status();
    if (retained_replay->result() != std::optional<absl::Status>(completed) ||
        storage_->DbEpoch(0) != db_epoch || replication_->is_loading() ||
        replication_->reject_writes()) {
      co_return TestFailure(
          "empty population replay disturbed the ready dataset");
    }
    {
      auto preserved = co_await storage_->Get(0, kReplayKey);
      if (!preserved.ok()) co_return preserved.status();
      const auto bytes = preserved->value_bytes();
      if (std::string_view(reinterpret_cast<const char*>(bytes.data()),
                           bytes.size()) != kReplayValue) {
        co_return TestFailure("empty population replay erased subsequent data");
      }
    }

    // An old completion handle remains historical evidence, not permission to
    // reuse a READY proof after the desired population has changed.
    ++desired.partition_replication_epoch_;
    if (absl::Status reconciled =
            co_await replication_->ReconcileClusterPopulation(desired);
        !reconciled.ok()) {
      co_return reconciled;
    }
    auto invalidated_replay =
        co_await replication_->StartEmptyPopulationInitialization(identity,
                                                                  *manifest);
    if (replication_
            ->FindCompletedClusterPopulation(
                lavik::RebuildDirective{.identity_ = identity})
            .has_value()) {
      co_return TestFailure("completed lookup revived an invalidated proof");
    }
    if (invalidated_replay.ok() || invalidated_replay.status().code() !=
                                       absl::StatusCode::kFailedPrecondition) {
      co_return TestFailure(
          "empty population replay revived an invalidated proof");
    }
    const auto invalidated = co_await replication_->cluster_population_status();
    if (invalidated.state_ != lavik::ReplicationGroupState::kNotReady ||
        invalidated.ready_token_.has_value() || !replication_->is_loading() ||
        !replication_->reject_writes()) {
      co_return TestFailure(
          "invalidated empty population lost its serving fence");
    }
    co_return absl::OkStatus();
  }

  lavik::storage::StorageEngine* storage_ = nullptr;
  lavik::ReplicationManager* replication_ = nullptr;
  EmptyPopulationExpectation expectation_ = EmptyPopulationExpectation::kReady;
  absl::Status result_ = absl::OkStatus();
};

class StandaloneIdentityService final : public bycorf::Service {
 public:
  StandaloneIdentityService(lavik::ReplicationManager* first,
                            lavik::ReplicationManager* second)
      : first_(first), second_(second) {}

  void Prepare(unsigned) override {}

  bycorf::Task<absl::Status> Run(bycorf::Worker& worker,
                                 bycorf::ServiceContext) override {
    result_ = co_await CheckLightweightQueries(*first_, std::nullopt,
                                               "standalone primary");
    if (result_.ok()) {
      result_ = co_await CheckLightweightQueries(
          *second_, lavik::ReplicaOfConfig{"127.0.0.1", 1},
          "configured standalone replica");
    }
    if (!result_.ok()) {
      worker.RequestStop();
      co_return result_;
    }
    const lavik::ReplicationStatus first_status = co_await first_->Observe();
    const lavik::ReplicationStatus second_status = co_await second_->Observe();
    const lavik::ClusterPopulationStatus first_population =
        co_await first_->cluster_population_status();
    const lavik::ClusterPopulationStatus second_population =
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
  lavik::ReplicationManager* first_ = nullptr;
  lavik::ReplicationManager* second_ = nullptr;
  absl::Status result_ = absl::OkStatus();
};

// No storage recovery is started: accepted rebuilds retain their control
// state without dialing a source or mutating a device. This isolates owner
// routing and snapshot publication from transfer timing and TxRuntime's
// process-global one-worker fixture used by the storage tests above.
class CrossWorkerControlService final : public bycorf::Service {
 public:
  explicit CrossWorkerControlService(lavik::ReplicationManager& replication)
      : replication_(replication) {}

  void Prepare(unsigned) override {}
  void Stop() noexcept override {}

  bycorf::Task<absl::Status> Run(bycorf::Worker& worker,
                                 bycorf::ServiceContext) override {
    if (worker.id() == 1) {
      result_ = co_await Exercise();
      finished_.store(true, std::memory_order_release);
    } else {
      while (!finished_.load(std::memory_order_acquire)) {
        auto waited = co_await bycorf::SleepFor(worker, 1ms);
        if (!waited.ok()) co_return waited;
      }
    }
    worker.RequestStop();
    co_return absl::OkStatus();
  }

  bool ready_for_shutdown() const { return ready_for_shutdown_.load(); }
  bool finished() const { return finished_.load(); }
  void ShutdownRequested() { shutdown_requested_.store(true); }
  const absl::Status& result() const { return result_; }

 private:
  bycorf::Task<absl::Status> CheckEveryWorker(
      std::optional<lavik::ReplicaOfConfig> expected) {
    for (unsigned owner = 0; owner < 2; ++owner) {
      auto checked = co_await bycorf::SubmitTaskTo(owner, [this, expected] {
        return CheckLightweightQueries(replication_, expected,
                                       "cross-worker control");
      });
      if (!checked.ok()) co_return checked;
    }
    // A remote observation must return to its originating worker; both this
    // loop and subsequent directive admission intentionally run off-owner.
    if (bycorf::ThisWorker().id_ != 1)
      co_return TestFailure("control query migrated its caller");
    co_return absl::OkStatus();
  }

  bycorf::Task<absl::Status> Exercise() {
    auto checked = co_await CheckEveryWorker(std::nullopt);
    if (!checked.ok()) co_return checked;
    const auto initial = co_await replication_.cluster_population_status();
    auto manifest = lavik::PopulationManifest::Create({{42, 9}});
    if (!manifest.ok()) co_return manifest.status();
    auto directive = TargetDirective(initial, *manifest);
    std::optional<lavik::ClusterRebuildCompletion> previous;
    for (unsigned revision = 1; revision <= 8; ++revision) {
      directive.identity_.directive_revision_ = revision;
      directive.identity_.attempt_id_ = "attempt-" + std::to_string(revision);
      directive.identity_.directive_id_ =
          "directive-" + std::to_string(revision);
      lavik::ReplicaOfConfig upstream{
          "127.0.0.1", static_cast<std::uint16_t>(6400 + revision)};
      auto started = co_await replication_.StartClusterRebuildDirective(
          upstream, directive, *manifest);
      if (!started.ok()) co_return started.status();
      if (previous.has_value() && (!previous->result().has_value() ||
                                   !absl::IsCancelled(*previous->result()))) {
        co_return TestFailure("supersession did not retire the old attempt");
      }
      previous = std::move(*started);
      checked = co_await CheckEveryWorker(upstream);
      if (!checked.ok()) co_return checked;
      const auto population = co_await replication_.cluster_population_status();
      if (population.state_ != lavik::ReplicationGroupState::kRebuilding ||
          population.ready_token_.has_value()) {
        co_return TestFailure("remote heartbeat observed an incoherent proof");
      }
      const bool completed = co_await bycorf::SubmitTo(0, [this, directive] {
        return replication_.FindCompletedClusterPopulation(directive)
            .has_value();
      });
      if (completed)
        co_return TestFailure("unfinished rebuild replayed as ready");
    }

    ready_for_shutdown_.store(true, std::memory_order_release);
    while (!shutdown_requested_.load(std::memory_order_acquire)) {
      (void)co_await replication_.Observe();
      auto waited = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_, 1ms);
      if (!waited.ok()) co_return waited;
    }
    auto cancelled = co_await replication_.CancelClusterRebuildForShutdown();
    if (!cancelled.ok()) co_return cancelled;
    if (!previous->result().has_value() ||
        !absl::IsCancelled(*previous->result())) {
      co_return TestFailure("shutdown did not retire the accepted attempt");
    }
    const auto population = co_await replication_.cluster_population_status();
    if (population.state_ != lavik::ReplicationGroupState::kNotReady ||
        population.ready_token_.has_value()) {
      co_return TestFailure("shutdown retained population readiness");
    }
    co_return co_await CheckEveryWorker(std::nullopt);
  }

  lavik::ReplicationManager& replication_;
  std::atomic<bool> ready_for_shutdown_{false};
  std::atomic<bool> shutdown_requested_{false};
  std::atomic<bool> finished_{false};
  absl::Status result_ =
      absl::UnknownError("cross-worker exercise did not run");
};

TEST(ReplicationManagerIntegrationTest,
     CrossWorkerControlQueriesSupersessionAndMainThreadShutdown) {
  lavik::test::TempDirectory directory("owner-local-replication");
  const auto data = directory.path() / "node.data";
  lavik::test::CreateDataFile(data, 256 * kMiB);
  lavik::storage::StorageEngineOptions storage_options;
  storage_options.data_files_ = {data.string()};
  storage_options.expiration_authority_ = false;
  lavik::storage::StorageEngine storage(std::move(storage_options));
  ASSERT_TRUE(storage.Prepare(2).ok());
  lavik::ReplicationOptions options;
  options.cluster_enabled_ = true;
  lavik::ReplicationManager replication(&storage, options, std::nullopt);
  CrossWorkerControlService service(replication);
  bycorf::Server server;
  server.AddService(&service);
  bycorf::ServerOptions runtime;
  runtime.thread_count_ = 2;
  runtime.pin_workers_ = false;
  runtime.recv_buffer_count_ = 0;
  ASSERT_TRUE(server.Start(runtime).ok());
  const auto deadline = std::chrono::steady_clock::now() + 10s;
  while (!service.ready_for_shutdown() && !service.finished() &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  const bool ready = service.ready_for_shutdown();
  replication.RequestShutdown();
  service.ShutdownRequested();
  server.WaitUntilStopped();
  EXPECT_TRUE(ready) << service.result();
  EXPECT_TRUE(service.result().ok()) << service.result();
}

class PromotionPrepareService final : public bycorf::Service {
 public:
  PromotionPrepareService(lavik::storage::StorageEngine* storage,
                          lavik::ReplicationManager* replication,
                          std::string fault_stage)
      : storage_(storage),
        replication_(replication),
        fault_stage_(std::move(fault_stage)) {}

  void Prepare(unsigned thread_count) override {
    if (thread_count != 1) {
      result_ = TestFailure("promotion prepare test requires one worker");
    }
  }

  bycorf::Task<absl::Status> Run(bycorf::Worker& worker,
                                 bycorf::ServiceContext) override {
    lavik::BindMemoryAccountingShard(worker.id());
    lavik::tx::TxRuntime::Get()->shard(worker.id()).Bind(worker);
    if (result_.ok()) result_ = co_await storage_->InitializeWorker(worker);
    if (result_.ok()) {
      replication_->StorageReady(worker);
      result_ = co_await Exercise();
    }
    replication_->RequestShutdown();
    (void)co_await replication_->QuiesceForShutdown();
    worker.RequestStop();
    co_return result_;
  }

  void Stop() noexcept override {}

  const absl::Status& result() const noexcept { return result_; }

 private:
  bycorf::Task<absl::Status> Exercise() {
    auto manifest = lavik::PopulationManifest::Create({});
    if (!manifest.ok()) co_return manifest.status();
    const lavik::ClusterPopulationStatus initial =
        co_await replication_->cluster_population_status();
    lavik::RebuildIdentity identity{
        .group_id_ = std::string(40, 'd'),
        .assignment_id_ = "candidate-assignment",
        .term_ = 7,
        .directive_revision_ = 1,
        .authority_id_ = "excluded-authority",
        .source_node_id_ = std::string(40, 'a'),
        .source_assignment_id_ = "source-assignment",
        .source_boot_id_ = std::string(40, 'b'),
        .source_history_id_ = std::string(40, 'c'),
        .target_node_id_ = initial.local_node_id_,
        .target_boot_id_ = initial.local_boot_id_,
        .operation_id_ = "failover-operation",
        .directive_id_ = "promotion-prepare",
        .attempt_id_ = "promotion-attempt",
        .manifest_revision_ = 1,
        .manifest_id_ = manifest->id(),
        .partition_replication_epoch_ = kPartitionReplicationEpoch,
    };
    lavik::ClusterPromotionPrepareDirective directive{
        .identity_ = identity,
        .parent_history_id_ = identity.source_history_id_,
        // Exercise a two-flow source on this one-worker target. Promotion
        // freezes the source-flow domain published by ReplicaAppliedFrontier,
        // not the target worker layout.
        .required_applied_next_lsns_ = {1, 3},
        .excluded_group_term_ = identity.term_,
    };

    auto started =
        co_await replication_->StartClusterPromotionPrepareDirective(directive);
    if (!started.ok()) co_return started.status();
    auto prepared = co_await started->Await();
    if (!fault_stage_.empty()) {
      if (prepared.ok()) {
        co_return TestFailure("injected promotion prepare unexpectedly passed");
      }
      const lavik::ReplicationStatus status = co_await replication_->Observe();
      if (!status.failed_stopped_ || !replication_->is_loading() ||
          !replication_->reject_writes()) {
        co_return TestFailure(
            "uncertain promotion prepare did not fail-stop the node");
      }
      auto replay =
          co_await replication_->StartClusterPromotionPrepareDirective(
              directive);
      if (!replay.ok() || (co_await replay->Await()).ok()) {
        co_return TestFailure(
            "failed promotion prepare replay changed its terminal result");
      }
      auto base = storage_->RecoverPromotionBase();
      if (!base.ok()) co_return base.status();
      const bool base_expected = fault_stage_ == "child-history" ||
                                 fault_stage_ == "evidence-publication";
      if (base->has_value() != base_expected) {
        co_return TestFailure(
            "promotion fault crossed an unexpected durability boundary");
      }
      co_return absl::OkStatus();
    }

    if (!prepared.ok()) co_return prepared.status();
    if (prepared->parent_history_id_ != directive.parent_history_id_ ||
        prepared->frozen_applied_next_lsns_ !=
            directive.required_applied_next_lsns_ ||
        prepared->population_generation_ == 0 ||
        prepared->catalog_generation_ == 0 ||
        !IsCanonicalReplicationId(prepared->child_history_id_) ||
        prepared->child_history_id_ == prepared->parent_history_id_) {
      co_return TestFailure("promotion prepare returned incomplete evidence");
    }
    auto base = storage_->RecoverPromotionBase();
    if (!base.ok() || !base->has_value() ||
        (**base).group_id_ != identity.group_id_ ||
        (**base).parent_history_id_ != directive.parent_history_id_ ||
        (**base).parent_frontier_.flow_cursors_ !=
            directive.required_applied_next_lsns_) {
      co_return TestFailure("promotion prepare did not persist its base");
    }
    const lavik::ReplicationStatus status = co_await replication_->Observe();
    const lavik::ClusterPopulationStatus population =
        co_await replication_->cluster_population_status();
    if (status.role_ != lavik::ReplicationRole::kSyncing ||
        status.failed_stopped_ || !replication_->is_loading() ||
        !replication_->reject_writes() ||
        population.state_ != lavik::ReplicationGroupState::kReady ||
        !population.ready_token_.has_value() ||
        population.applied_next_lsns_.has_value()) {
      co_return TestFailure(
          "prepared cluster promotion exposed serving or candidate authority");
    }
    auto watermark = co_await replication_->CaptureNativeReplicationWatermark();
    if (!watermark.ok() || !watermark->has_value()) {
      co_return TestFailure(
          "prepared promotion did not create a child history");
    }

    auto replay =
        co_await replication_->StartClusterPromotionPrepareDirective(directive);
    if (!replay.ok()) co_return replay.status();
    auto replayed = co_await replay->Await();
    if (!replayed.ok() || *replayed != *prepared) {
      co_return TestFailure("exact promotion prepare replay changed evidence");
    }
    lavik::ClusterPromotionPrepareDirective conflict = directive;
    conflict.identity_.attempt_id_ = "conflicting-attempt";
    auto conflicting =
        co_await replication_->StartClusterPromotionPrepareDirective(conflict);
    if (conflicting.status().code() != absl::StatusCode::kFailedPrecondition) {
      co_return TestFailure("promotion prepare accepted conflicting anchors");
    }

    std::vector<lavik::ClusterPromotionPrepareDirective> stale_directives;
    conflict = directive;
    conflict.identity_.assignment_id_ = "stale-assignment";
    stale_directives.push_back(conflict);
    conflict = directive;
    conflict.identity_.target_boot_id_ = std::string(40, '8');
    stale_directives.push_back(conflict);
    conflict = directive;
    ++conflict.identity_.term_;
    ++conflict.excluded_group_term_;
    stale_directives.push_back(conflict);
    conflict = directive;
    ++conflict.identity_.manifest_revision_;
    stale_directives.push_back(conflict);
    conflict = directive;
    ++conflict.identity_.partition_replication_epoch_;
    stale_directives.push_back(conflict);
    conflict = directive;
    conflict.identity_.source_history_id_ = std::string(40, '7');
    conflict.parent_history_id_ = conflict.identity_.source_history_id_;
    stale_directives.push_back(conflict);
    conflict = directive;
    ++conflict.required_applied_next_lsns_.front();
    stale_directives.push_back(conflict);
    for (auto& stale : stale_directives) {
      auto rejected =
          co_await replication_->StartClusterPromotionPrepareDirective(stale);
      if (rejected.ok()) {
        co_return TestFailure(
            "promotion prepare accepted a changed identity anchor");
      }
    }

    lavik::RebuildDirective unauthorized_export{
        .identity_ =
            {
                .group_id_ = identity.group_id_,
                .assignment_id_ = "downstream-assignment",
                .term_ = identity.term_,
                .directive_revision_ = 2,
                .authority_id_ = "future-authority",
                .source_node_id_ = initial.local_node_id_,
                .source_assignment_id_ = identity.assignment_id_,
                .source_boot_id_ = initial.local_boot_id_,
                .source_history_id_ = prepared->child_history_id_,
                .target_node_id_ = std::string(40, 'e'),
                .target_boot_id_ = std::string(40, 'f'),
                .operation_id_ = "downstream-operation",
                .directive_id_ = "downstream-rebuild",
                .attempt_id_ = "downstream-attempt",
                .manifest_revision_ = identity.manifest_revision_,
                .manifest_id_ = identity.manifest_id_,
                .partition_replication_epoch_ =
                    identity.partition_replication_epoch_,
            },
        .flow_count_ = 1,
        .safe_source_active_ = true,
    };
    const absl::Status authorized =
        co_await replication_->AuthorizeClusterRebuildSource(
            std::move(unauthorized_export));
    if (authorized.code() != absl::StatusCode::kFailedPrecondition) {
      co_return TestFailure(
          "prepared cluster promotion authorized downstream export");
    }
    co_return absl::OkStatus();
  }

  lavik::storage::StorageEngine* storage_ = nullptr;
  lavik::ReplicationManager* replication_ = nullptr;
  std::string fault_stage_;
  absl::Status result_ = absl::OkStatus();
};

class ScopedPromotionFaults {
 public:
  explicit ScopedPromotionFaults(std::string_view stage) {
    EXPECT_EQ(::setenv("LAVIK_REPLICATION_SEED_READY_PROMOTION_CANDIDATE",
                       "promotion-attempt", 1),
              0);
    if (!stage.empty()) {
      EXPECT_EQ(::setenv("LAVIK_REPLICATION_FAIL_PROMOTION_PREPARE_AT",
                         std::string(stage).c_str(), 1),
                0);
    }
  }
  ~ScopedPromotionFaults() {
    (void)::unsetenv("LAVIK_REPLICATION_SEED_READY_PROMOTION_CANDIDATE");
    (void)::unsetenv("LAVIK_REPLICATION_FAIL_PROMOTION_PREPARE_AT");
  }
};

void RunPromotionPrepareCase(std::string_view fault_stage) {
#if !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires a Debug/fault build for candidate seeding";
#endif
  ScopedPromotionFaults faults(fault_stage);
  lavik::test::TempDirectory directory(
      fault_stage.empty() ? "cluster-promotion-prepare"
                          : "cluster-promotion-" + std::string(fault_stage));
  const std::filesystem::path data = directory.path() / "node.data";
  lavik::test::CreateDataFile(data, 128 * kMiB);

  lavik::storage::StorageEngineOptions storage_options;
  storage_options.data_files_ = {data.string()};
  storage_options.expiration_authority_ = false;
  storage_options.buffers_.registered_bytes_ = 64 * kMiB;
  storage_options.replication_publish_queue_bytes_ = 16 * kMiB;
  lavik::storage::StorageEngine storage(std::move(storage_options));
  lavik::InitWorkerMetrics(1);
  ASSERT_TRUE(lavik::InitMemoryLimit(512 * kMiB, 1).ok());
  ASSERT_TRUE(storage.Prepare(1).ok());

  lavik::ReplicationOptions replication_options;
  replication_options.cluster_enabled_ = true;
  replication_options.node_id_override_ = std::string(40, '9');
  lavik::ReplicationManager replication(
      &storage, std::move(replication_options), std::nullopt);
  lavik::InitStorage(&storage, &replication);
  if (lavik::tx::TxRuntime::Get() == nullptr) {
    lavik::tx::TxRuntime::Create(1);
  }

  PromotionPrepareService service(&storage, &replication,
                                  std::string(fault_stage));
  bycorf::Server server;
  server.AddService(&service);
  bycorf::ServerOptions runtime;
  runtime.thread_count_ = 1;
  runtime.pin_workers_ = false;
  runtime.recv_buffer_count_ = 0;
  ASSERT_TRUE(server.Start(runtime).ok());
  server.WaitUntilStopped();
  EXPECT_TRUE(service.result().ok()) << service.result();
}

class PromotionPrepareFailureIntegrationTest
    : public testing::TestWithParam<const char*> {};

enum class PreparedActionDisposition {
  kRetainForActivation,
  kRemove,
  kReplace,
  kRemoveWhilePreparing,
  kRemoveWhilePreparingThenResumeFollow,
  kReplaceWhilePreparing,
  kRemoveAfterDurabilityBoundary,
  kRemoveAfterDurabilityBoundaryWithPublicationFailure,
};

struct PromotionFaultBarrierPaths {
  std::filesystem::path prepare_entered_;
  std::filesystem::path runner_waiting_;
  std::filesystem::path runner_terminal_;
};

class FailoverActionReconcileService final : public bycorf::Service {
 public:
  FailoverActionReconcileService(
      lavik::storage::StorageEngine* storage,
      lavik::ReplicationManager* replication, bool expect_watchdog = false,
      PreparedActionDisposition disposition =
          PreparedActionDisposition::kRetainForActivation,
      PromotionFaultBarrierPaths fault_barriers = {},
      FollowOwnerSource* continuation_source = nullptr)
      : storage_(storage),
        replication_(replication),
        expect_watchdog_(expect_watchdog),
        disposition_(disposition),
        fault_barriers_(std::move(fault_barriers)),
        continuation_source_(continuation_source) {}

  void Prepare(unsigned thread_count) override {
    if (thread_count != 1) {
      result_ = TestFailure("failover action test requires one worker");
    }
  }

  bycorf::Task<absl::Status> Run(bycorf::Worker& worker,
                                 bycorf::ServiceContext) override {
    lavik::BindMemoryAccountingShard(worker.id());
    lavik::tx::TxRuntime::Get()->shard(worker.id()).Bind(worker);
    if (result_.ok()) result_ = co_await storage_->InitializeWorker(worker);
    if (result_.ok()) {
      replication_->StorageReady(worker);
      result_ = co_await Exercise();
    }
    replication_->RequestShutdown();
    absl::Status quiesced = co_await replication_->QuiesceForShutdown();
    const bool expected_prepare_fail_stop =
        disposition_ ==
        PreparedActionDisposition::
            kRemoveAfterDurabilityBoundaryWithPublicationFailure;
    if (result_.ok() && expected_prepare_fail_stop) {
      constexpr std::string_view kExpectedShutdownFailure =
          "replication is failed-stopped until restart: cluster "
          "promotion-prepare evidence publication outcome is uncertain: "
          "injected promotion-prepare evidence publication failure";
      if (quiesced.code() != absl::StatusCode::kFailedPrecondition ||
          quiesced.message() != kExpectedShutdownFailure) {
        result_ = TestFailure(absl::StrCat(
            "post-durability fail-stop returned an unexpected shutdown "
            "result: ",
            quiesced.ToString()));
      }
    } else if (result_.ok() && !quiesced.ok()) {
      result_ = quiesced;
    }
    if (result_.ok()) {
      const lavik::ClusterFailoverActionStatus status =
          co_await replication_->cluster_failover_action_status();
      if (status.state_ != lavik::ClusterFailoverActionState::kNone ||
          status.action_.has_value()) {
        result_ = TestFailure(
            "shutdown retained a committed failover action observation");
      }
    }
    worker.RequestStop();
    co_return result_;
  }

  void Stop() noexcept override {}

  const absl::Status& result() const noexcept { return result_; }

 private:
  bycorf::Task<bool> EveryReplicationLogIs(
      lavik::storage::ReplicationLogState expected) {
    for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
      const auto state = co_await bycorf::SubmitTo(worker, [this] {
        return storage_->LocalReplicationLogInfo().state_;
      });
      if (state != expected) co_return false;
    }
    co_return true;
  }

  bycorf::Task<absl::Status> Exercise() {
    const lavik::ClusterPopulationStatus population =
        co_await replication_->cluster_population_status();
    lavik::DesiredClusterFailoverAction action;
    action.transition_id_.fill(1);
    action.action_id_.fill(2);
    action.transition_revision_ = 7;
    action.mode_ = lavik::ClusterFailoverMode::kControlled;
    action.target_term_ = 2;
    action.committed_group_term_ = 1;
    action.committed_grant_active_ = true;
    action.group_id_ = std::string(40, 'd');
    action.candidate_node_id_ = population.local_node_id_;
    action.candidate_assignment_id_ = "candidate-assignment";
    action.candidate_boot_id_ = population.local_boot_id_;
    action.domain_ = lavik::ClusterFailoverCompatibilityDomain{
        .source_group_term_ = 1,
        .source_node_id_ = std::string(40, 'a'),
        .source_assignment_id_ = "source-assignment",
        .source_boot_id_ = std::string(40, 'b'),
        .source_history_id_ = std::string(40, 'c'),
        .flow_count_ = 1,
    };
    action.manifest_revision_ = 1;
    auto manifest = lavik::PopulationManifest::Create({});
    if (!manifest.ok()) co_return manifest.status();
    action.manifest_id_ = manifest->id();
    action.partition_replication_epoch_ = kPartitionReplicationEpoch;

    absl::Status reconciled =
        co_await replication_->ReconcileClusterFailoverAction(action);
    if (!reconciled.ok()) co_return reconciled;
    lavik::ClusterFailoverActionStatus status =
        co_await replication_->cluster_failover_action_status();
    if (!status.action_.has_value() || *status.action_ != action ||
        status.state_ !=
            lavik::ClusterFailoverActionState::kWaitingForAuthorization) {
      co_return TestFailure(
          "unauthorized action was not retained at the authorization gate");
    }

    reconciled = co_await replication_->ReconcileClusterFailoverAction(action);
    if (!reconciled.ok()) co_return reconciled;
    status = co_await replication_->cluster_failover_action_status();
    if (!status.action_.has_value() || *status.action_ != action ||
        status.state_ !=
            lavik::ClusterFailoverActionState::kWaitingForAuthorization) {
      co_return TestFailure("exact unauthorized action replay was not a no-op");
    }
    if (expect_watchdog_) {
      // The short fault watchdog belongs to an installed authorization, not
      // to the earlier desired action. Waiting here longer than that budget
      // must leave the candidate at its one-way authorization gate.
      absl::Status waited = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(50));
      if (!waited.ok()) co_return waited;
      status = co_await replication_->cluster_failover_action_status();
      if (status.state_ !=
              lavik::ClusterFailoverActionState::kWaitingForAuthorization ||
          status.failure_class_ == "watchdog") {
        co_return TestFailure(
            "failover watchdog started before local authorization install");
      }
    }

    ++action.transition_revision_;
    action.authorized_revision_ = action.transition_revision_;
    reconciled = co_await replication_->ReconcileClusterFailoverAction(action);
    if (!reconciled.ok()) co_return reconciled;
    if (expect_watchdog_) {
      reconciled =
          co_await replication_->ReconcileClusterFailoverAction(action);
      if (!reconciled.ok()) co_return reconciled;
    }
    const bool remove_while_preparing =
        disposition_ == PreparedActionDisposition::kRemoveWhilePreparing ||
        disposition_ ==
            PreparedActionDisposition::kRemoveWhilePreparingThenResumeFollow ||
        disposition_ ==
            PreparedActionDisposition::kRemoveAfterDurabilityBoundary ||
        disposition_ ==
            PreparedActionDisposition::
                kRemoveAfterDurabilityBoundaryWithPublicationFailure;
    const bool replace_while_preparing =
        disposition_ == PreparedActionDisposition::kReplaceWhilePreparing;
    const bool remove_after_durability =
        disposition_ ==
            PreparedActionDisposition::kRemoveAfterDurabilityBoundary ||
        disposition_ ==
            PreparedActionDisposition::
                kRemoveAfterDurabilityBoundaryWithPublicationFailure;
    if (remove_while_preparing || replace_while_preparing) {
      const auto preparing_deadline =
          std::chrono::steady_clock::now() + std::chrono::seconds(5);
      do {
        status = co_await replication_->cluster_failover_action_status();
        if (status.state_ == lavik::ClusterFailoverActionState::kPreparing) {
          break;
        }
        absl::Status waited = co_await bycorf::SleepFor(
            *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
        if (!waited.ok()) co_return waited;
      } while (std::chrono::steady_clock::now() < preparing_deadline);
      if (status.state_ != lavik::ClusterFailoverActionState::kPreparing) {
        co_return TestFailure(
            "failover action did not enter the in-flight prepare cut");
      }
      absl::Status barrier = co_await AwaitFaultBarrier(
          fault_barriers_.prepare_entered_,
          remove_after_durability ? "post-durability promotion barrier"
                                  : "pre-durability promotion barrier");
      if (!barrier.ok()) co_return barrier;
      barrier =
          co_await AwaitFaultBarrier(fault_barriers_.runner_waiting_,
                                     "failover runner completion-wait barrier");
      if (!barrier.ok()) co_return barrier;

      lavik::ClusterFailoverActionId replacement_action_id{};
      replacement_action_id.fill(3);
      std::optional<lavik::DesiredClusterFailoverAction> replacement;
      if (replace_while_preparing) {
        replacement = action;
        replacement->action_id_ = replacement_action_id;
        ++replacement->transition_revision_;
        replacement->authorized_revision_.reset();
      }
      reconciled = co_await replication_->ReconcileClusterFailoverAction(
          std::move(replacement));
      if (!reconciled.ok()) co_return reconciled;
      barrier = co_await AwaitFaultBarrier(
          fault_barriers_.runner_terminal_,
          "superseded failover runner terminal barrier");
      if (!barrier.ok()) co_return barrier;
      status = co_await replication_->cluster_failover_action_status();
      if (replace_while_preparing) {
        if (!status.action_.has_value() ||
            status.action_->action_id_ != replacement_action_id ||
            status.state_ !=
                lavik::ClusterFailoverActionState::kWaitingForAuthorization ||
            !status.failure_class_.empty() || !status.failure_detail_.empty() ||
            status.prepared_.has_value()) {
          co_return TestFailure(
              "cancelled action published a terminal observation into its "
              "replacement");
        }
      } else if (status.action_.has_value() ||
                 status.state_ != lavik::ClusterFailoverActionState::kNone ||
                 !status.failure_class_.empty() ||
                 !status.failure_detail_.empty() ||
                 status.prepared_.has_value()) {
        co_return TestFailure(
            "cancelled action retained or published boot-local progress");
      }
      if (!co_await EveryReplicationLogIs(
              lavik::storage::ReplicationLogState::kDisabled)) {
        co_return TestFailure(
            "in-flight action cleanup left an obsolete replication log");
      }
      if (disposition_ ==
              PreparedActionDisposition::
                  kRemoveAfterDurabilityBoundaryWithPublicationFailure &&
          !(co_await replication_->Observe()).failed_stopped_) {
        co_return TestFailure(
            "injected post-durability failure did not fail-stop the node");
      }
      if (remove_after_durability) {
        co_return absl::OkStatus();
      }

      if (continuation_source_ != nullptr) {
        lavik::DesiredClusterUpstream follow{
            .group_id_ = action.group_id_,
            .group_term_ = action.domain_.source_group_term_,
            .local_node_id_ = action.candidate_node_id_,
            .local_assignment_id_ = action.candidate_assignment_id_,
            .local_boot_id_ = action.candidate_boot_id_,
            .owner_node_id_ = action.domain_.source_node_id_,
            .owner_assignment_id_ = action.domain_.source_assignment_id_,
            .owner_endpoint_ =
                lavik::ReplicaOfConfig{"127.0.0.1",
                                       continuation_source_->port()},
            .manifest_revision_ = action.manifest_revision_,
            .manifest_id_ = action.manifest_id_,
            .partition_replication_epoch_ = action.partition_replication_epoch_,
            .members_ =
                {
                    {action.candidate_node_id_,
                     action.candidate_assignment_id_},
                    {action.domain_.source_node_id_,
                     action.domain_.source_assignment_id_},
                },
        };
        reconciled = co_await replication_->ReconcileClusterFollowOwner(follow);
        if (!reconciled.ok()) co_return reconciled;
        const auto follow_deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while ((!continuation_source_->saw_resume_proof() ||
                !continuation_source_->sent_continue()) &&
               std::chrono::steady_clock::now() < follow_deadline) {
          absl::Status waited = co_await bycorf::SleepFor(
              *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
          if (!waited.ok()) co_return waited;
        }
        if (!continuation_source_->saw_resume_proof() ||
            !continuation_source_->sent_continue()) {
          co_return TestFailure(
              "cancelled replica Candidate did not enter FollowOwner "
              "CONTINUE from its retained proof");
        }
        const lavik::ClusterPopulationStatus retained =
            co_await replication_->cluster_population_status();
        if (retained.state_ != lavik::ReplicationGroupState::kReady ||
            !retained.ready_token_.has_value()) {
          co_return TestFailure(
              "matching FollowOwner replaced the cancelled Candidate's "
              "Ready population");
        }
        auto continued_write = co_await storage_->Set(
            0, "cancelled-candidate-continue", "applied");
        if (!continued_write.ok()) {
          co_return TestFailure(absl::StrCat(
              "cancelled Candidate retained a destructive FULL write fence: ",
              continued_write.status().ToString()));
        }
        reconciled =
            co_await replication_->ReconcileClusterFollowOwner(std::nullopt);
        if (!reconciled.ok()) co_return reconciled;
      }

      // Before durability mutation, supersession is a local cancellation
      // boundary rather than a request to finish obsolete preparation. Reuse
      // the same Ready population to prove that cleanup preserved the
      // candidate and released promotion admission for the next action.
      lavik::DesiredClusterFailoverAction successor = action;
      successor.action_id_ = replacement_action_id;
      ++successor.transition_revision_;
      successor.authorized_revision_.reset();
      if (remove_while_preparing) {
        reconciled =
            co_await replication_->ReconcileClusterFailoverAction(successor);
        if (!reconciled.ok()) co_return reconciled;
      }
      ++successor.transition_revision_;
      successor.authorized_revision_ = successor.transition_revision_;
      reconciled =
          co_await replication_->ReconcileClusterFailoverAction(successor);
      if (!reconciled.ok()) co_return reconciled;

      const auto successor_deadline =
          std::chrono::steady_clock::now() + std::chrono::seconds(5);
      do {
        status = co_await replication_->cluster_failover_action_status();
        if (status.state_ == lavik::ClusterFailoverActionState::kPrepared ||
            status.state_ == lavik::ClusterFailoverActionState::kFailed) {
          break;
        }
        absl::Status waited = co_await bycorf::SleepFor(
            *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
        if (!waited.ok()) co_return waited;
      } while (std::chrono::steady_clock::now() < successor_deadline);
      if (!status.action_.has_value() || *status.action_ != successor ||
          status.state_ != lavik::ClusterFailoverActionState::kPrepared ||
          !status.prepared_.has_value()) {
        co_return TestFailure(absl::StrCat(
            "in-flight action cleanup did not release the Ready population "
            "for its successor: state=",
            static_cast<int>(status.state_), " failure-class=",
            status.failure_class_, " detail=", status.failure_detail_));
      }
      reconciled =
          co_await replication_->ReconcileClusterFailoverAction(std::nullopt);
      if (!reconciled.ok()) co_return reconciled;
      co_return absl::OkStatus();
    }
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    do {
      status = co_await replication_->cluster_failover_action_status();
      if (status.state_ == lavik::ClusterFailoverActionState::kPrepared ||
          status.state_ == lavik::ClusterFailoverActionState::kFailed) {
        break;
      }
      absl::Status waited = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(10));
      if (!waited.ok()) co_return waited;
    } while (std::chrono::steady_clock::now() < deadline);
    if (expect_watchdog_) {
      if (status.state_ != lavik::ClusterFailoverActionState::kFailed ||
          status.failure_class_ != "watchdog" || !status.action_.has_value() ||
          *status.action_ != action) {
        co_return TestFailure(
            "watchdog did not publish the exact terminal ActionFailed");
      }
      lavik::ClusterPopulationStatus failed_population =
          co_await replication_->cluster_population_status();
      if (failed_population.ready_token_.has_value() &&
          failed_population.failover_candidate_eligible_) {
        co_return TestFailure(
            "terminal action failure did not suppress the same population");
      }

      lavik::DesiredClusterFailoverAction replacement = action;
      replacement.action_id_.fill(3);
      ++replacement.transition_revision_;
      replacement.authorized_revision_.reset();
      reconciled =
          co_await replication_->ReconcileClusterFailoverAction(replacement);
      if (!reconciled.ok()) co_return reconciled;
      status = co_await replication_->cluster_failover_action_status();
      failed_population = co_await replication_->cluster_population_status();
      if (!status.action_.has_value() || *status.action_ != replacement ||
          status.state_ !=
              lavik::ClusterFailoverActionState::kWaitingForAuthorization ||
          (failed_population.ready_token_.has_value() &&
           failed_population.failover_candidate_eligible_)) {
        co_return TestFailure(
            "replacement action re-enabled a terminally failed population");
      }

      reconciled =
          co_await replication_->ReconcileClusterFailoverAction(std::nullopt);
      if (!reconciled.ok()) co_return reconciled;
      reconciled =
          co_await replication_->ReconcileClusterPopulation(std::nullopt);
      if (!reconciled.ok()) co_return reconciled;

      // The failure latch names one exact boot-local population/domain. A
      // destructive replacement in the same boot must be eligible again;
      // otherwise the latch would permanently remove this node from election.
      const lavik::ReplicationIdentity refreshed =
          co_await replication_->ObserveIdentity();
      lavik::RebuildIdentity replacement_population{
          .group_id_ = action.group_id_,
          .assignment_id_ = action.candidate_assignment_id_,
          .term_ = action.target_term_,
          .directive_revision_ = action.transition_revision_ + 1,
          .authority_id_ = "replacement-population-authority",
          .target_node_id_ = refreshed.local_node_id_,
          .target_boot_id_ = refreshed.boot_id_,
          .target_history_id_ = refreshed.local_history_id_,
          .operation_id_ = "replacement-population-operation",
          .directive_id_ = "replacement-population-directive",
          .attempt_id_ = "replacement-population-attempt",
          .manifest_revision_ = action.manifest_revision_,
          .manifest_id_ = action.manifest_id_,
          .partition_replication_epoch_ =
              action.partition_replication_epoch_ + 1,
      };
      auto initialized =
          co_await replication_->StartEmptyPopulationInitialization(
              replacement_population, *manifest);
      if (!initialized.ok()) co_return initialized.status();
      absl::Status ready = co_await initialized->Await();
      if (!ready.ok()) co_return ready;
      failed_population = co_await replication_->cluster_population_status();
      if (!failed_population.failover_candidate_eligible_) {
        co_return TestFailure(
            "a replacement population remained suppressed by an old action");
      }

      // Leave an authorized action polling for the retired population. The
      // service shutdown path below must cancel and join that runner before it
      // retires the replacement population.
      lavik::DesiredClusterFailoverAction shutdown_action = action;
      shutdown_action.action_id_.fill(4);
      ++shutdown_action.transition_revision_;
      shutdown_action.authorized_revision_ =
          shutdown_action.transition_revision_;
      reconciled = co_await replication_->ReconcileClusterFailoverAction(
          shutdown_action);
      if (!reconciled.ok()) co_return reconciled;
      co_return absl::OkStatus();
    }

    if (status.state_ != lavik::ClusterFailoverActionState::kPrepared ||
        !status.prepared_.has_value() ||
        status.prepared_->transition_id_ != action.transition_id_ ||
        status.prepared_->action_id_ != action.action_id_ ||
        std::all_of(status.prepared_->context_id_.begin(),
                    status.prepared_->context_id_.end(),
                    [](std::uint8_t byte) { return byte == 0; })) {
      co_return TestFailure(
          "authorized action did not publish action-bound prepared context");
    }
    if (!co_await EveryReplicationLogIs(
            lavik::storage::ReplicationLogState::kActive)) {
      co_return TestFailure(
          "prepared action did not leave every child replication log active");
    }
    const lavik::ClusterFailoverPreparedContext prepared = *status.prepared_;
    reconciled = co_await replication_->ReconcileClusterFailoverAction(action);
    if (!reconciled.ok()) co_return reconciled;
    status = co_await replication_->cluster_failover_action_status();
    if (status.state_ != lavik::ClusterFailoverActionState::kPrepared ||
        status.prepared_ != prepared) {
      co_return TestFailure("prepared action replay repeated local effects");
    }

    // A controlled transition may retain this exact authorized action while
    // fencing the source and advancing to its target term. That committed
    // downgrade changes authority context, not the candidate attempt: tearing
    // it down here would discard the already-prepared child history before
    // Meta can commit the uncontrolled cutover.
    lavik::DesiredClusterFailoverAction degraded = action;
    ++degraded.transition_revision_;
    degraded.mode_ = lavik::ClusterFailoverMode::kUncontrolled;
    degraded.committed_group_term_ = degraded.target_term_;
    degraded.committed_grant_active_ = false;
    reconciled =
        co_await replication_->ReconcileClusterFailoverAction(degraded);
    if (!reconciled.ok()) co_return reconciled;
    status = co_await replication_->cluster_failover_action_status();
    if (!status.action_.has_value() || *status.action_ != degraded ||
        status.state_ != lavik::ClusterFailoverActionState::kPrepared ||
        status.prepared_ != prepared) {
      co_return TestFailure(
          "retained controlled downgrade restarted the prepared action");
    }
    if (!co_await EveryReplicationLogIs(
            lavik::storage::ReplicationLogState::kActive)) {
      co_return TestFailure(
          "retained controlled downgrade retired prepared child backlog");
    }

    if (disposition_ != PreparedActionDisposition::kRetainForActivation) {
      if (disposition_ == PreparedActionDisposition::kReplace) {
        lavik::DesiredClusterFailoverAction replacement = action;
        replacement.action_id_.fill(3);
        ++replacement.transition_revision_;
        replacement.authorized_revision_.reset();
        reconciled =
            co_await replication_->ReconcileClusterFailoverAction(replacement);
        if (!reconciled.ok()) co_return reconciled;
        status = co_await replication_->cluster_failover_action_status();
        if (!status.action_.has_value() || *status.action_ != replacement ||
            status.state_ !=
                lavik::ClusterFailoverActionState::kWaitingForAuthorization) {
          co_return TestFailure(
              "replacement action was not installed after retiring prepare");
        }
      } else {
        reconciled =
            co_await replication_->ReconcileClusterFailoverAction(std::nullopt);
        if (!reconciled.ok()) co_return reconciled;
      }
      if (!co_await EveryReplicationLogIs(
              lavik::storage::ReplicationLogState::kDisabled)) {
        co_return TestFailure(
            "removed or replaced prepared action retained child backlog");
      }
      const lavik::ReplicationIdentity retired =
          co_await replication_->ObserveIdentity();
      if (retired.local_history_id_ == prepared.promotion_.child_history_id_) {
        co_return TestFailure(
            "removed or replaced action retained its child history identity");
      }
      co_return absl::OkStatus();
    }

    reconciled = co_await replication_->ReconcileClusterFailoverAction(
        std::nullopt, action.action_id_);
    if (!reconciled.ok()) co_return reconciled;
    status = co_await replication_->cluster_failover_action_status();
    if (status.action_.has_value() ||
        status.state_ != lavik::ClusterFailoverActionState::kNone) {
      co_return TestFailure("action removal retained boot-local progress");
    }
    auto retained = co_await replication_->FindClusterFailoverPreparedContext(
        action.action_id_);
    if (retained != prepared) {
      co_return TestFailure(
          "matching cutover action did not retain its prepared context");
    }
    if (!co_await EveryReplicationLogIs(
            lavik::storage::ReplicationLogState::kActive)) {
      co_return TestFailure(
          "pending activation retired the cutover winner child backlog");
    }
    const lavik::ReplicationIdentity pending_activation_identity =
        co_await replication_->ObserveIdentity();
    if (pending_activation_identity.local_history_id_ !=
        prepared.promotion_.child_history_id_) {
      co_return TestFailure(
          "pending activation replaced the cutover winner child history");
    }

    lavik::ClusterFailoverActivation activation{
        .action_id_ = action.action_id_,
        .group_id_ = action.group_id_,
        .candidate_node_id_ = action.candidate_node_id_,
        .candidate_assignment_id_ = action.candidate_assignment_id_,
        .candidate_boot_id_ = action.candidate_boot_id_,
        .target_term_ = action.target_term_,
        .manifest_revision_ = action.manifest_revision_,
        .manifest_id_ = action.manifest_id_,
        .partition_replication_epoch_ = action.partition_replication_epoch_,
    };
    absl::Status paused = co_await storage_->QuiesceExpiration();
    if (!paused.ok()) co_return paused;
    struct ExpirationResume {
      lavik::storage::StorageEngine* storage_;
      ~ExpirationResume() { storage_->ResumeExpiration(); }
    } expiration_resume{storage_};
    if (storage_->ExpirationPauseCount() != 1) {
      co_return TestFailure("activation fixture did not own one expiry pause");
    }

    lavik::ClusterFailoverActivation mismatched = activation;
    mismatched.action_id_.fill(9);
    absl::Status activated =
        co_await replication_->ActivateClusterPreparedPromotion(mismatched);
    if (activated.code() != absl::StatusCode::kFailedPrecondition ||
        !replication_->is_loading() || !replication_->reject_writes() ||
        storage_->ExpirationPauseCount() != 1) {
      co_return TestFailure(
          "mismatched action activated or resumed a fenced promotion");
    }
    mismatched = activation;
    ++mismatched.partition_replication_epoch_;
    activated =
        co_await replication_->ActivateClusterPreparedPromotion(mismatched);
    if (activated.code() != absl::StatusCode::kFailedPrecondition ||
        !replication_->is_loading() || storage_->ExpirationPauseCount() != 1) {
      co_return TestFailure("stale population activated prepared promotion");
    }

    activated =
        co_await replication_->ActivateClusterPreparedPromotion(activation);
    if (!activated.ok() || replication_->is_loading() ||
        replication_->reject_writes() || replication_->is_replica() ||
        storage_->ExpirationPauseCount() != 1) {
      co_return activated.ok()
          ? TestFailure("matching activation did not open only the role")
          : activated;
    }
    activated =
        co_await replication_->ActivateClusterPreparedPromotion(activation);
    if (!activated.ok() || storage_->ExpirationPauseCount() != 1) {
      co_return TestFailure(
          "exact activation replay repeated promotion or resumed expiration");
    }

    absl::Status expiration =
        co_await replication_->EnableClusterExpirationAuthorityUntil(
            std::chrono::nanoseconds::zero());
    if (expiration.code() != absl::StatusCode::kDeadlineExceeded) {
      co_return TestFailure("elapsed expiration lease was accepted");
    }
    expiration = co_await replication_->EnableClusterExpirationAuthorityUntil(
        std::chrono::nanoseconds::max() - std::chrono::nanoseconds(1));
    if (!expiration.ok()) co_return expiration;
    expiration = co_await replication_->RevokeClusterExpirationAuthority();
    if (!expiration.ok() || storage_->ExpirationPauseCount() != 1) {
      co_return TestFailure(
          "expiration revocation failed to preserve an outer pause");
    }

    reconciled = co_await replication_->ReconcileClusterFailoverAction(
        std::nullopt, action.action_id_);
    if (!reconciled.ok()) co_return reconciled;
    retained = co_await replication_->FindClusterFailoverPreparedContext(
        action.action_id_);
    if (retained != prepared) {
      co_return TestFailure("matching cutover replay cleared prepared context");
    }
    lavik::ClusterFailoverActionId different_action;
    different_action.fill(3);
    reconciled = co_await replication_->ReconcileClusterFailoverAction(
        std::nullopt, different_action);
    if (!reconciled.ok()) co_return reconciled;
    retained = co_await replication_->FindClusterFailoverPreparedContext(
        action.action_id_);
    if (retained.has_value()) {
      co_return TestFailure(
          "mismatched activation id retained a stale prepared context");
    }
    if (!co_await EveryReplicationLogIs(
            lavik::storage::ReplicationLogState::kActive)) {
      co_return TestFailure(
          "post-activation cleanup retired the winner replication log");
    }
    reconciled =
        co_await replication_->ReconcileClusterFailoverAction(std::nullopt);
    if (!reconciled.ok()) co_return reconciled;
    if (!co_await EveryReplicationLogIs(
            lavik::storage::ReplicationLogState::kActive)) {
      co_return TestFailure(
          "ordinary reconciliation retired the activated winner backlog");
    }
    co_return absl::OkStatus();
  }

  lavik::storage::StorageEngine* storage_ = nullptr;
  lavik::ReplicationManager* replication_ = nullptr;
  bool expect_watchdog_ = false;
  PreparedActionDisposition disposition_ =
      PreparedActionDisposition::kRetainForActivation;
  PromotionFaultBarrierPaths fault_barriers_;
  FollowOwnerSource* continuation_source_ = nullptr;
  absl::Status result_ = absl::OkStatus();
};

enum class NativeActionDisposition {
  kPrepare,
  kCancelWhilePreparing,
  kShutdownWhilePreparing,
};

class NativeFailoverActionService final : public bycorf::Service {
 public:
  NativeFailoverActionService(
      lavik::storage::StorageEngine* storage,
      lavik::ReplicationManager* replication,
      NativeActionDisposition disposition = NativeActionDisposition::kPrepare,
      PromotionFaultBarrierPaths fault_barriers = {})
      : storage_(storage),
        replication_(replication),
        disposition_(disposition),
        fault_barriers_(std::move(fault_barriers)) {}

  void Prepare(unsigned thread_count) override {
    if (thread_count != 1) {
      result_ = TestFailure("native failover action test requires one worker");
    }
  }

  bycorf::Task<absl::Status> Run(bycorf::Worker& worker,
                                 bycorf::ServiceContext) override {
    lavik::BindMemoryAccountingShard(worker.id());
    lavik::tx::TxRuntime::Get()->shard(worker.id()).Bind(worker);
    if (result_.ok()) result_ = co_await storage_->InitializeWorker(worker);
    if (result_.ok()) {
      replication_->StorageReady(worker);
      result_ = co_await Exercise();
    }
    replication_->RequestShutdown();
    (void)co_await replication_->QuiesceForShutdown();
    worker.RequestStop();
    co_return result_;
  }

  void Stop() noexcept override {}
  const absl::Status& result() const noexcept { return result_; }

 private:
  bycorf::Task<bool> EveryReplicationLogIs(
      lavik::storage::ReplicationLogState expected) {
    for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
      const auto state = co_await bycorf::SubmitTo(worker, [this] {
        return storage_->LocalReplicationLogInfo().state_;
      });
      if (state != expected) co_return false;
    }
    co_return true;
  }

  bycorf::Task<absl::Status> Exercise() {
    const lavik::ReplicationIdentity local =
        co_await replication_->ObserveIdentity();
    auto manifest = lavik::PopulationManifest::Create({});
    if (!manifest.ok()) co_return manifest.status();
    lavik::RebuildIdentity identity{
        .group_id_ = "native-group",
        .assignment_id_ = "native-assignment",
        .term_ = 1,
        .directive_revision_ = 1,
        .authority_id_ = "initial-authority",
        .target_node_id_ = local.local_node_id_,
        .target_boot_id_ = local.boot_id_,
        .target_history_id_ = local.local_history_id_,
        .operation_id_ = "initial-operation",
        .directive_id_ = "initial-directive",
        .attempt_id_ = "initial-attempt",
        .manifest_revision_ = 1,
        .manifest_id_ = manifest->id(),
        .partition_replication_epoch_ = 1,
    };
    auto initialized =
        co_await replication_->StartEmptyPopulationInitialization(identity,
                                                                  *manifest);
    if (!initialized.ok()) co_return initialized.status();
    absl::Status ready = co_await initialized->Await();
    if (!ready.ok()) co_return ready;

    lavik::DesiredClusterFailoverAction action;
    action.transition_id_.fill(4);
    action.action_id_.fill(5);
    action.transition_revision_ = 9;
    action.mode_ = lavik::ClusterFailoverMode::kUncontrolled;
    action.target_term_ = 2;
    action.committed_group_term_ = 2;
    action.authorized_revision_ = 9;
    action.group_id_ = identity.group_id_;
    action.candidate_node_id_ = local.local_node_id_;
    action.candidate_assignment_id_ = identity.assignment_id_;
    action.candidate_boot_id_ = local.boot_id_;
    action.domain_ = lavik::ClusterFailoverCompatibilityDomain{
        .source_group_term_ = 1,
        .source_node_id_ = local.local_node_id_,
        .source_assignment_id_ = identity.assignment_id_,
        .source_boot_id_ = local.boot_id_,
        .source_history_id_ = local.local_history_id_,
        .flow_count_ = 1,
    };
    action.manifest_revision_ = identity.manifest_revision_;
    action.manifest_id_ = identity.manifest_id_;
    action.partition_replication_epoch_ = identity.partition_replication_epoch_;

    lavik::DesiredClusterFailoverAction unfenced = action;
    unfenced.committed_grant_active_ = true;
    absl::Status rejected =
        co_await replication_->ReconcileClusterFailoverAction(unfenced);
    if (rejected.code() != absl::StatusCode::kFailedPrecondition) {
      co_return TestFailure(
          "uncontrolled action started before target-term fencing");
    }

    absl::Status reconciled =
        co_await replication_->ReconcileClusterFailoverAction(action);
    if (!reconciled.ok()) co_return reconciled;
    lavik::ClusterFailoverActionStatus status;
    if (disposition_ != NativeActionDisposition::kPrepare) {
      const auto preparing_deadline =
          std::chrono::steady_clock::now() + std::chrono::seconds(5);
      do {
        status = co_await replication_->cluster_failover_action_status();
        if (status.state_ == lavik::ClusterFailoverActionState::kPreparing) {
          break;
        }
        absl::Status waited = co_await bycorf::SleepFor(
            *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
        if (!waited.ok()) co_return waited;
      } while (std::chrono::steady_clock::now() < preparing_deadline);
      if (status.state_ != lavik::ClusterFailoverActionState::kPreparing) {
        co_return TestFailure(
            "self-origin action did not enter the in-flight prepare cut");
      }
      absl::Status barrier = co_await AwaitFaultBarrier(
          fault_barriers_.prepare_entered_,
          "self-origin pre-durability promotion barrier");
      if (!barrier.ok()) co_return barrier;
      barrier = co_await AwaitFaultBarrier(
          fault_barriers_.runner_waiting_,
          "self-origin failover runner completion-wait barrier");
      if (!barrier.ok()) co_return barrier;

      if (disposition_ == NativeActionDisposition::kShutdownWhilePreparing) {
        reconciled = co_await replication_->CancelClusterRebuildForShutdown();
      } else {
        reconciled =
            co_await replication_->ReconcileClusterFailoverAction(std::nullopt);
      }
      if (!reconciled.ok()) co_return reconciled;
      barrier = co_await AwaitFaultBarrier(
          fault_barriers_.runner_terminal_,
          "self-origin superseded failover runner terminal barrier");
      if (!barrier.ok()) co_return barrier;
      status = co_await replication_->cluster_failover_action_status();
      if (status.state_ != lavik::ClusterFailoverActionState::kNone ||
          status.action_.has_value() || !status.failure_class_.empty() ||
          !status.failure_detail_.empty() || status.prepared_.has_value()) {
        co_return TestFailure(
            "cancelled self-origin action published a terminal observation");
      }
      if (disposition_ == NativeActionDisposition::kShutdownWhilePreparing) {
        auto promotion_base = storage_->RecoverPromotionBase();
        if (!promotion_base.ok()) co_return promotion_base.status();
        const lavik::ReplicationStatus stopped =
            co_await replication_->Observe();
        if (promotion_base->has_value()) {
          co_return TestFailure(
              "shutdown let a superseded self-origin prepare cross the "
              "durability boundary");
        }
        if (stopped.role_ == lavik::ReplicationRole::kMaster ||
            !replication_->is_loading()) {
          co_return TestFailure(
              "shutdown reopened a cancelled self-origin primary role");
        }
        co_return absl::OkStatus();
      }

      const lavik::ReplicationStatus restored =
          co_await replication_->Observe();
      if (restored.role_ != lavik::ReplicationRole::kMaster ||
          restored.local_history_id_ != local.local_history_id_ ||
          replication_->is_loading() ||
          !co_await EveryReplicationLogIs(
              lavik::storage::ReplicationLogState::kActive)) {
        co_return TestFailure(
            "safe self-origin cancellation did not restore its original "
            "fenced primary population");
      }

      action.action_id_.fill(6);
      ++action.transition_revision_;
      action.authorized_revision_ = action.transition_revision_;
      reconciled =
          co_await replication_->ReconcileClusterFailoverAction(action);
      if (!reconciled.ok()) co_return reconciled;
    }
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    do {
      status = co_await replication_->cluster_failover_action_status();
      if (status.state_ == lavik::ClusterFailoverActionState::kPrepared ||
          status.state_ == lavik::ClusterFailoverActionState::kFailed) {
        break;
      }
      absl::Status waited = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(10));
      if (!waited.ok()) co_return waited;
    } while (std::chrono::steady_clock::now() < deadline);
    if (status.state_ != lavik::ClusterFailoverActionState::kPrepared ||
        !status.prepared_.has_value() ||
        status.prepared_->promotion_.parent_history_id_ !=
            local.local_history_id_ ||
        status.prepared_->promotion_.frozen_applied_next_lsns_.size() != 1 ||
        !replication_->is_loading() || !replication_->reject_writes()) {
      co_return TestFailure(
          "former Owner native population was not prepared while fenced");
    }
    co_return co_await replication_->ReconcileClusterFailoverAction(
        std::nullopt);
  }

  lavik::storage::StorageEngine* storage_ = nullptr;
  lavik::ReplicationManager* replication_ = nullptr;
  NativeActionDisposition disposition_ = NativeActionDisposition::kPrepare;
  PromotionFaultBarrierPaths fault_barriers_;
  absl::Status result_ = absl::OkStatus();
};

class ClusterSourcePauseService final : public bycorf::Service {
 public:
  ClusterSourcePauseService(lavik::storage::StorageEngine* storage,
                            lavik::ReplicationManager* replication)
      : storage_(storage), replication_(replication) {}

  void Prepare(unsigned thread_count) override {
    if (thread_count != 1) {
      result_ = TestFailure("source pause test requires one worker");
    }
  }

  bycorf::Task<absl::Status> Run(bycorf::Worker& worker,
                                 bycorf::ServiceContext) override {
    lavik::BindMemoryAccountingShard(worker.id());
    lavik::tx::TxRuntime::Get()->shard(worker.id()).Bind(worker);
    if (result_.ok()) result_ = co_await storage_->InitializeWorker(worker);
    if (result_.ok()) {
      replication_->StorageReady(worker);
      result_ = co_await Exercise();
    }
    replication_->RequestShutdown();
    const absl::Status quiesced = co_await replication_->QuiesceForShutdown();
    if (result_.ok() && !quiesced.ok()) result_ = quiesced;
    if (result_.ok() && storage_->ExpirationPauseCount() != 0) {
      result_ = TestFailure("shutdown leaked the controlled source pause");
    }
    worker.RequestStop();
    co_return result_;
  }

  void Stop() noexcept override {}
  const absl::Status& result() const noexcept { return result_; }

 private:
  bycorf::Task<absl::Status> Exercise() {
    const lavik::ReplicationIdentity local =
        co_await replication_->ObserveIdentity();
    auto manifest = lavik::PopulationManifest::Create({});
    if (!manifest.ok()) co_return manifest.status();
    lavik::RebuildIdentity identity{
        .group_id_ = "source-pause-group",
        .assignment_id_ = "source-pause-assignment",
        .term_ = 1,
        .directive_revision_ = 1,
        .authority_id_ = "source-pause-authority",
        .target_node_id_ = local.local_node_id_,
        .target_boot_id_ = local.boot_id_,
        .target_history_id_ = local.local_history_id_,
        .operation_id_ = "source-pause-population-operation",
        .directive_id_ = "source-pause-population-directive",
        .attempt_id_ = "source-pause-population-attempt",
        .manifest_revision_ = 1,
        .manifest_id_ = manifest->id(),
        .partition_replication_epoch_ = 1,
    };
    auto initialized =
        co_await replication_->StartEmptyPopulationInitialization(identity,
                                                                  *manifest);
    if (!initialized.ok()) co_return initialized.status();
    if (absl::Status ready = co_await initialized->Await(); !ready.ok()) {
      co_return ready;
    }

    lavik::DesiredClusterSourcePause pause{
        .transition_revision_ = 11,
        .group_id_ = identity.group_id_,
        .source_node_id_ = local.local_node_id_,
        .source_assignment_id_ = identity.assignment_id_,
        .source_boot_id_ = local.boot_id_,
        .source_history_id_ = std::string(40, 'e'),
        .source_group_term_ = identity.term_,
        .flow_count_ = 1,
        .manifest_revision_ = identity.manifest_revision_,
        .manifest_id_ = identity.manifest_id_,
        .partition_replication_epoch_ = identity.partition_replication_epoch_,
    };
    pause.transition_id_.fill(6);

    // A capture/domain failure keeps the single pause so exact replay or an
    // FDS replacement cannot create an expiry-mutation window. It must not
    // expose SourcePaused until every captured anchor is exact.
    absl::Status reconciled =
        co_await replication_->ReconcileClusterSourcePause(pause);
    if (reconciled.code() != absl::StatusCode::kFailedPrecondition ||
        storage_->ExpirationPauseCount() != 1) {
      co_return TestFailure(
          "failed source pause did not retain exactly one expiration pause");
    }
    lavik::ClusterSourcePauseStatus status =
        co_await replication_->cluster_source_pause_status();
    if (!status.desired_.has_value() || *status.desired_ != pause ||
        status.stable_next_lsns_.has_value() ||
        status.failure_detail_.empty()) {
      co_return TestFailure("failed capture published SourcePaused evidence");
    }
    reconciled = co_await replication_->ReconcileClusterSourcePause(pause);
    if (reconciled.code() != absl::StatusCode::kFailedPrecondition ||
        storage_->ExpirationPauseCount() != 1) {
      co_return TestFailure("failed source pause replay leaked a pause count");
    }

    lavik::DesiredClusterSourcePause replacement = pause;
    replacement.transition_id_.fill(7);
    ++replacement.transition_revision_;
    replacement.source_history_id_ = local.local_history_id_;
    reconciled =
        co_await replication_->ReconcileClusterSourcePause(replacement);
    if (!reconciled.ok() || storage_->ExpirationPauseCount() != 1) {
      co_return reconciled.ok()
          ? TestFailure("source pause replacement opened expiry")
          : reconciled;
    }
    status = co_await replication_->cluster_source_pause_status();
    if (!status.desired_.has_value() || *status.desired_ != replacement ||
        !status.stable_next_lsns_.has_value() ||
        status.stable_next_lsns_->size() != 1 ||
        !status.failure_detail_.empty()) {
      co_return TestFailure(
          "matching source pause did not publish a stable exact frontier");
    }
    const std::vector<std::uint64_t> stable = *status.stable_next_lsns_;

    reconciled =
        co_await replication_->ReconcileClusterSourcePause(replacement);
    status = co_await replication_->cluster_source_pause_status();
    if (!reconciled.ok() || status.stable_next_lsns_ != stable ||
        storage_->ExpirationPauseCount() != 1) {
      co_return TestFailure("exact source pause replay repeated local effects");
    }

    reconciled =
        co_await replication_->ReconcileClusterSourcePause(std::nullopt);
    if (!reconciled.ok() || storage_->ExpirationPauseCount() != 0) {
      co_return TestFailure("source pause removal did not resume expiration");
    }
    status = co_await replication_->cluster_source_pause_status();
    if (status.desired_.has_value() || status.stable_next_lsns_.has_value()) {
      co_return TestFailure("source pause removal retained an observation");
    }
    reconciled =
        co_await replication_->ReconcileClusterSourcePause(std::nullopt);
    if (!reconciled.ok() || storage_->ExpirationPauseCount() != 0) {
      co_return TestFailure("source pause removal replay underflowed pairing");
    }
    reconciled =
        co_await replication_->ReconcileClusterSourcePause(replacement);
    if (!reconciled.ok() || storage_->ExpirationPauseCount() != 1) {
      co_return TestFailure(
          "source pause could not be reinstalled for shutdown");
    }
    co_return absl::OkStatus();
  }

  lavik::storage::StorageEngine* storage_ = nullptr;
  lavik::ReplicationManager* replication_ = nullptr;
  absl::Status result_ = absl::OkStatus();
};

class FollowOwnerReconcileService final : public bycorf::Service {
 public:
  FollowOwnerReconcileService(lavik::storage::StorageEngine* storage,
                              lavik::ReplicationManager* replication,
                              FollowOwnerSource* unavailable,
                              FollowOwnerSource* replacement)
      : storage_(storage),
        replication_(replication),
        unavailable_(unavailable),
        replacement_(replacement) {}

  void Prepare(unsigned thread_count) override {
    if (thread_count != 1) {
      result_ = TestFailure("follow-owner test requires one worker");
    }
  }

  bycorf::Task<absl::Status> Run(bycorf::Worker& worker,
                                 bycorf::ServiceContext) override {
    lavik::BindMemoryAccountingShard(worker.id());
    lavik::tx::TxRuntime::Get()->shard(worker.id()).Bind(worker);
    if (result_.ok()) result_ = co_await storage_->InitializeWorker(worker);
    if (result_.ok()) {
      replication_->StorageReady(worker);
      result_ = co_await Exercise(worker);
    }
    replication_->RequestShutdown();
    const absl::Status quiesced = co_await replication_->QuiesceForShutdown();
    if (result_.ok() && !quiesced.ok()) result_ = quiesced;
    worker.RequestStop();
    co_return result_;
  }

  void Stop() noexcept override {}
  const absl::Status& result() const noexcept { return result_; }

 private:
  bycorf::Task<absl::Status> WaitUntil(bycorf::Worker& worker,
                                       const std::function<bool()>& predicate,
                                       std::string_view failure) {
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (!predicate() && std::chrono::steady_clock::now() < deadline) {
      absl::Status waited = co_await bycorf::SleepFor(worker, 1ms);
      if (!waited.ok()) co_return waited;
    }
    co_return predicate() ? absl::OkStatus()
                          : absl::DeadlineExceededError(std::string(failure));
  }

  bycorf::Task<absl::Status> RestartSteadyFollowFull(
      bycorf::Worker& worker, const lavik::DesiredClusterUpstream& desired) {
    const unsigned controls_before = replacement_->controls();
    absl::Status reconciled =
        co_await replication_->ReconcileClusterFollowOwner(desired);
    if (!reconciled.ok()) co_return reconciled;
    absl::Status waited = co_await WaitUntil(
        worker, [&] { return replacement_->controls() > controls_before; },
        "replacement population did not restart its control handshake");
    if (!waited.ok()) co_return waited;

    const auto deadline = std::chrono::steady_clock::now() + 5s;
    do {
      const lavik::ClusterPopulationStatus population =
          co_await replication_->cluster_population_status();
      if (population.state_ == lavik::ReplicationGroupState::kRebuilding &&
          !population.ready_token_.has_value() &&
          replication_->upstream().has_value()) {
        co_return absl::OkStatus();
      }
      waited = co_await bycorf::SleepFor(worker, 1ms);
      if (!waited.ok()) co_return waited;
    } while (std::chrono::steady_clock::now() < deadline);
    co_return absl::DeadlineExceededError(
        "replacement population did not restart steady FollowOwner FULL");
  }

  bycorf::Task<absl::Status> VerifyPopulationReplacementRetiresFull(
      bycorf::Worker& worker,
      const lavik::DesiredClusterUpstream& follow_desired,
      lavik::DesiredClusterPopulation population_replacement,
      std::string_view replacement_kind) {
    const unsigned closed_before = replacement_->closed();
    absl::Status reconciled = co_await replication_->ReconcileClusterPopulation(
        std::move(population_replacement));
    if (!reconciled.ok()) co_return reconciled;
    absl::Status waited = co_await WaitUntil(
        worker, [&] { return replacement_->closed() > closed_before; },
        "population replacement did not close steady FollowOwner ingress");
    if (!waited.ok()) co_return waited;

    const lavik::ClusterPopulationStatus population =
        co_await replication_->cluster_population_status();
    if (population.state_ != lavik::ReplicationGroupState::kNotReady ||
        population.ready_token_.has_value() ||
        replication_->upstream().has_value()) {
      co_return TestFailure(absl::StrCat(
          replacement_kind,
          " replacement preserved a stale steady FollowOwner FULL"));
    }
    co_return co_await RestartSteadyFollowFull(worker, follow_desired);
  }

  bycorf::Task<absl::Status> Exercise(bycorf::Worker& worker) {
    const lavik::ReplicationIdentity local =
        co_await replication_->ObserveIdentity();
    auto manifest = lavik::PopulationManifest::Create({});
    if (!manifest.ok()) co_return manifest.status();
    lavik::RebuildIdentity identity{
        .group_id_ = "follow-group",
        .assignment_id_ = "follower-assignment",
        .term_ = 1,
        .directive_revision_ = 1,
        .authority_id_ = "follow-initial-authority",
        .target_node_id_ = local.local_node_id_,
        .target_boot_id_ = local.boot_id_,
        .target_history_id_ = local.local_history_id_,
        .operation_id_ = "follow-initial-operation",
        .directive_id_ = "follow-initial-directive",
        .attempt_id_ = "follow-initial-attempt",
        .manifest_revision_ = 1,
        .manifest_id_ = manifest->id(),
        .partition_replication_epoch_ = 1,
    };
    auto initialized =
        co_await replication_->StartEmptyPopulationInitialization(identity,
                                                                  *manifest);
    if (!initialized.ok()) co_return initialized.status();
    if (absl::Status ready = co_await initialized->Await(); !ready.ok()) {
      co_return ready;
    }

    lavik::DesiredClusterUpstream desired{
        .group_id_ = identity.group_id_,
        .group_term_ = 2,
        .local_node_id_ = local.local_node_id_,
        .local_assignment_id_ = identity.assignment_id_,
        .local_boot_id_ = local.boot_id_,
        .owner_node_id_ = std::string(40, 'a'),
        .owner_assignment_id_ = "owner-assignment-a",
        .owner_endpoint_ =
            lavik::ReplicaOfConfig{"127.0.0.1", unavailable_->port()},
        .manifest_revision_ = identity.manifest_revision_,
        .manifest_id_ = identity.manifest_id_,
        .partition_replication_epoch_ = identity.partition_replication_epoch_,
        .members_ =
            {
                {local.local_node_id_, identity.assignment_id_},
                {std::string(40, 'a'), "owner-assignment-a"},
            },
    };
    absl::Status reconciled =
        co_await replication_->ReconcileClusterFollowOwner(desired);
    if (!reconciled.ok()) co_return reconciled;
    absl::Status waited = co_await WaitUntil(
        worker, [&] { return unavailable_->controls() == 1; },
        "follow owner did not start its control handshake");
    if (!waited.ok()) co_return waited;

    const lavik::ClusterPopulationStatus before_export_ready =
        co_await replication_->cluster_population_status();
    const lavik::ReplicationIdentity after_fence =
        co_await replication_->ObserveIdentity();
    if (before_export_ready.state_ != lavik::ReplicationGroupState::kReady ||
        !before_export_ready.ready_token_.has_value() ||
        after_fence.local_history_id_ == local.local_history_id_ ||
        !replication_->is_loading() || !replication_->is_replica()) {
      co_return TestFailure(
          "follow fence did not preserve Ready while retiring former Owner "
          "history");
    }

    reconciled = co_await replication_->ReconcileClusterFollowOwner(desired);
    if (!reconciled.ok()) co_return reconciled;
    waited = co_await bycorf::SleepFor(worker, 50ms);
    if (!waited.ok()) co_return waited;
    if (unavailable_->controls() != 1) {
      co_return TestFailure("exact follow desired replay restarted ingress");
    }

    lavik::DesiredClusterUpstream replacement = desired;
    replacement.owner_node_id_ = std::string(40, 'b');
    replacement.owner_assignment_id_ = "owner-assignment-b";
    replacement.owner_endpoint_ =
        lavik::ReplicaOfConfig{"127.0.0.1", replacement_->port()};
    replacement.members_.back() = {replacement.owner_node_id_,
                                   replacement.owner_assignment_id_};
    reconciled =
        co_await replication_->ReconcileClusterFollowOwner(replacement);
    if (!reconciled.ok()) co_return reconciled;
    waited = co_await WaitUntil(
        worker,
        [&] {
          return unavailable_->closed() >= 1 && replacement_->controls() >= 1;
        },
        "owner replacement did not join old ingress before reconnecting");
    if (!waited.ok()) co_return waited;
    waited = co_await WaitUntil(
        worker, [&] { return replacement_->saw_follow_scope(); },
        "replacement source did not receive steady FOLLOW scope");
    if (!waited.ok()) co_return waited;

    // The replacement source has published an authenticated/export-ready
    // incarnation. Only now may the existing coordinator enter destructive
    // FULL and withdraw the old Ready proof.
    const auto destructive_deadline = std::chrono::steady_clock::now() + 5s;
    lavik::ClusterPopulationStatus after_export_ready;
    do {
      after_export_ready = co_await replication_->cluster_population_status();
      if (after_export_ready.state_ ==
          lavik::ReplicationGroupState::kRebuilding) {
        break;
      }
      waited = co_await bycorf::SleepFor(worker, 1ms);
      if (!waited.ok()) co_return waited;
    } while (std::chrono::steady_clock::now() < destructive_deadline);
    if (after_export_ready.state_ !=
            lavik::ReplicationGroupState::kRebuilding ||
        after_export_ready.ready_token_.has_value()) {
      co_return TestFailure(
          "destructive FollowOwner FULL retained a Ready candidate proof");
    }

    // A steady FollowOwner FULL rotates the local replication history, which
    // deliberately makes the Data-to-Meta session reconnect. That session
    // replacement must retire session-scoped population directives without
    // cancelling the level-triggered FDS relationship that caused the
    // rotation; otherwise every sufficiently slow FULL cancels itself before
    // it can publish a replacement population.
    const unsigned replacement_closed_at_control_loss = replacement_->closed();
    reconciled = co_await replication_->CancelInProgressClusterPopulation(
        /*preserve_current_follow_attempt=*/true);
    const lavik::ClusterPopulationStatus after_control_loss =
        co_await replication_->cluster_population_status();
    if (!reconciled.ok() ||
        after_control_loss.state_ !=
            lavik::ReplicationGroupState::kRebuilding ||
        after_control_loss.ready_token_.has_value() ||
        !replication_->upstream().has_value() ||
        replacement_->closed() != replacement_closed_at_control_loss) {
      co_return TestFailure(
          "Meta session replacement cancelled its current steady "
          "FollowOwner FULL");
    }

    // Every follower applies this decision independently: there is no Meta
    // rebuild queue or Data-side admission controller that stages destructive
    // FULL across members. Consequently all followers may reach this exact
    // candidate-ineligible state together. If the new Owner fails before any
    // FULL finishes, the next uncontrolled transition is deliberately allowed
    // to remain fenced with candidate=null until some population becomes Ready.

    // NodeControl applies steady replication before population readiness for
    // one FDS. The ordinary FollowOwner FULL attempt is not backed by an
    // operation directive, so the following population reconciliation must
    // preserve it even though population_transition_expected is false. A
    // candidate-less failover Begin advances only the authority term; it does
    // not change the physical population being copied.
    lavik::DesiredClusterPopulation desired_population{
        .group_id_ = desired.group_id_,
        .assignment_id_ = desired.local_assignment_id_,
        .term_ = desired.group_term_,
        .manifest_revision_ = desired.manifest_revision_,
        .manifest_id_ = desired.manifest_id_,
        .partition_replication_epoch_ = desired.partition_replication_epoch_,
        .population_transition_expected_ = false,
    };
    ++desired_population.term_;
    reconciled =
        co_await replication_->ReconcileClusterPopulation(desired_population);
    const lavik::ClusterPopulationStatus after_population_reconcile =
        co_await replication_->cluster_population_status();
    if (!reconciled.ok() ||
        after_population_reconcile.state_ !=
            lavik::ReplicationGroupState::kRebuilding ||
        !replication_->upstream().has_value() || replacement_->closed() != 0) {
      co_return TestFailure(
          "candidate-less term fence retired its steady FollowOwner FULL "
          "attempt");
    }

    // The authority-only exception is deliberately narrow. Replacing the
    // Owner still replaces the exact session/context ownership and must cancel
    // the in-flight FULL before connecting to the new source.
    const unsigned replacement_closed_before = replacement_->closed();
    const unsigned unavailable_controls_before = unavailable_->controls();
    reconciled = co_await replication_->ReconcileClusterFollowOwner(desired);
    if (!reconciled.ok()) co_return reconciled;
    waited = co_await WaitUntil(
        worker,
        [&] {
          return replacement_->closed() > replacement_closed_before &&
                 unavailable_->controls() > unavailable_controls_before;
        },
        "owner replacement did not retire the in-flight steady FULL");
    if (!waited.ok()) co_return waited;
    const lavik::ClusterPopulationStatus after_owner_replacement =
        co_await replication_->cluster_population_status();
    if (after_owner_replacement.state_ !=
            lavik::ReplicationGroupState::kNotReady ||
        after_owner_replacement.ready_token_.has_value()) {
      co_return TestFailure(
          "owner replacement preserved a stale steady FollowOwner FULL");
    }
    reconciled = co_await RestartSteadyFollowFull(worker, replacement);
    if (!reconciled.ok()) co_return reconciled;

    lavik::DesiredClusterPopulation population_replacement = desired_population;
    population_replacement.assignment_id_ = "replacement-assignment";
    reconciled = co_await VerifyPopulationReplacementRetiresFull(
        worker, replacement, std::move(population_replacement), "assignment");
    if (!reconciled.ok()) co_return reconciled;

    auto replacement_manifest = lavik::PopulationManifest::Create({{0, 2}});
    if (!replacement_manifest.ok()) co_return replacement_manifest.status();
    population_replacement = desired_population;
    ++population_replacement.manifest_revision_;
    population_replacement.manifest_id_ = replacement_manifest->id();
    reconciled = co_await VerifyPopulationReplacementRetiresFull(
        worker, replacement, std::move(population_replacement), "manifest");
    if (!reconciled.ok()) co_return reconciled;

    population_replacement = desired_population;
    ++population_replacement.partition_replication_epoch_;
    reconciled = co_await VerifyPopulationReplacementRetiresFull(
        worker, replacement, std::move(population_replacement),
        "partition epoch");
    if (!reconciled.ok()) co_return reconciled;

    reconciled =
        co_await replication_->ReconcileClusterFollowOwner(std::nullopt);
    if (!reconciled.ok() || replication_->upstream().has_value() ||
        !replication_->is_replica() || !replication_->is_loading()) {
      co_return TestFailure(
          "follow desired removal did not clean and fence ingress");
    }
    co_return absl::OkStatus();
  }

  lavik::storage::StorageEngine* storage_ = nullptr;
  lavik::ReplicationManager* replication_ = nullptr;
  FollowOwnerSource* unavailable_ = nullptr;
  FollowOwnerSource* replacement_ = nullptr;
  absl::Status result_ = absl::OkStatus();
};

class FollowOwnerSourceAuthorizationService final : public bycorf::Service {
 public:
  FollowOwnerSourceAuthorizationService(
      lavik::storage::StorageEngine* storage,
      lavik::ReplicationManager* replication,
      std::filesystem::path admission_entered = {},
      std::filesystem::path revocation_closed = {})
      : storage_(storage),
        replication_(replication),
        admission_entered_(std::move(admission_entered)),
        revocation_closed_(std::move(revocation_closed)) {}

  void Prepare(unsigned thread_count) override {
    if (thread_count != 1) {
      result_ = TestFailure("follow source test requires one worker");
    }
  }

  bycorf::Task<absl::Status> Run(bycorf::Worker& worker,
                                 bycorf::ServiceContext) override {
    lavik::BindMemoryAccountingShard(worker.id());
    lavik::tx::TxRuntime::Get()->shard(worker.id()).Bind(worker);
    if (result_.ok()) result_ = co_await storage_->InitializeWorker(worker);
    if (result_.ok()) {
      replication_->StorageReady(worker);
      result_ = co_await Exercise(worker);
    }
    replication_->RequestShutdown();
    const absl::Status quiesced = co_await replication_->QuiesceForShutdown();
    if (result_.ok() && !quiesced.ok()) result_ = quiesced;
    for (int peer : peer_fds_) {
      if (peer >= 0) (void)::close(peer);
    }
    worker.RequestStop();
    co_return result_;
  }

  void Stop() noexcept override {}
  const absl::Status& result() const noexcept { return result_; }

 private:
  struct RequestResult {
    absl::Status status_ = absl::UnknownError("native request did not finish");
    bool done_ = false;
  };

  struct Peer {
    std::shared_ptr<bycorf::TcpStream> stream_;
    int peer_fd_ = -1;
  };

  absl::StatusOr<Peer> OpenPeer(bycorf::Worker& worker) {
    const int listener = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (listener < 0) return absl::ErrnoToStatus(errno, "socket");
    struct ListenerGuard {
      int fd_;
      ~ListenerGuard() { (void)::close(fd_); }
    } listener_guard{listener};
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (::bind(listener, reinterpret_cast<const sockaddr*>(&address),
               sizeof(address)) != 0 ||
        ::listen(listener, 1) != 0) {
      return absl::ErrnoToStatus(errno, "bind/listen");
    }
    socklen_t size = sizeof(address);
    if (::getsockname(listener, reinterpret_cast<sockaddr*>(&address), &size) !=
        0) {
      return absl::ErrnoToStatus(errno, "getsockname");
    }
    const int peer = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (peer < 0) return absl::ErrnoToStatus(errno, "peer socket");
    if (::connect(peer, reinterpret_cast<const sockaddr*>(&address),
                  sizeof(address)) != 0) {
      const absl::Status failure = absl::ErrnoToStatus(errno, "connect");
      (void)::close(peer);
      return failure;
    }
    const int accepted =
        ::accept4(listener, nullptr, nullptr, SOCK_CLOEXEC | SOCK_NONBLOCK);
    if (accepted < 0) {
      const absl::Status failure = absl::ErrnoToStatus(errno, "accept");
      (void)::close(peer);
      return failure;
    }
    const int flags = ::fcntl(peer, F_GETFL, 0);
    if (flags < 0 || ::fcntl(peer, F_SETFL, flags | O_NONBLOCK) != 0) {
      const absl::Status failure = absl::ErrnoToStatus(errno, "fcntl");
      (void)::close(peer);
      (void)::close(accepted);
      return failure;
    }
    bycorf::Connection connection;
    connection.worker_ = &worker;
    connection.file_.fd_ = accepted;
    connection.closed_ = false;
    bycorf::Connection* registered =
        worker.AddConnection(std::move(connection));
    if (registered == nullptr) {
      (void)::close(peer);
      (void)::close(accepted);
      return absl::InternalError("could not register native test connection");
    }
    peer_fds_.push_back(peer);
    return Peer{.stream_ = std::make_shared<bycorf::TcpStream>(registered),
                .peer_fd_ = peer};
  }

  bycorf::Task<absl::Status> RunNativeRequest(
      std::shared_ptr<bycorf::TcpStream> stream, std::vector<std::string> args,
      std::uint64_t client_id, RequestResult* result) {
    result->status_ = co_await replication_->ServeNativeConnection(
        *stream, std::move(args), client_id, "127.0.0.1", false);
    stream->Close().IgnoreError();
    result->done_ = true;
    co_return absl::OkStatus();
  }

  bycorf::Task<absl::Status> RunSourceRevocation(RequestResult* result) {
    result->status_ =
        co_await replication_->RevokeClusterRebuildSourceAuthorizations();
    result->done_ = true;
    co_return absl::OkStatus();
  }

  bycorf::Task<absl::StatusOr<std::string>> ReadPeerLine(
      bycorf::Worker& worker, int peer, std::string_view description) {
    std::string response;
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (response.find("\r\n") == std::string::npos &&
           std::chrono::steady_clock::now() < deadline) {
      char buffer[512];
      const ssize_t received = ::recv(peer, buffer, sizeof(buffer), 0);
      if (received > 0) {
        response.append(buffer, static_cast<std::size_t>(received));
        continue;
      }
      if (received == 0) {
        co_return absl::UnavailableError(std::string(description) +
                                         " closed before a response");
      }
      if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK) {
        co_return absl::ErrnoToStatus(errno, description);
      }
      absl::Status waited = co_await bycorf::SleepFor(worker, 1ms);
      if (!waited.ok()) co_return waited;
    }
    if (response.find("\r\n") == std::string::npos) {
      co_return absl::DeadlineExceededError(std::string(description));
    }
    response.resize(response.find("\r\n"));
    co_return response;
  }

  bycorf::Task<absl::Status> WaitDone(bycorf::Worker& worker,
                                      const RequestResult& result,
                                      std::string_view description) {
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (!result.done_ && std::chrono::steady_clock::now() < deadline) {
      absl::Status waited = co_await bycorf::SleepFor(worker, 1ms);
      if (!waited.ok()) co_return waited;
    }
    co_return result.done_
        ? absl::OkStatus()
        : absl::DeadlineExceededError(std::string(description));
  }

  static std::vector<std::string_view> Words(const std::string& line) {
    std::vector<std::string_view> result;
    std::string_view remaining(line);
    while (!remaining.empty()) {
      const std::size_t separator = remaining.find(' ');
      result.push_back(remaining.substr(0, separator));
      if (separator == std::string_view::npos) break;
      remaining.remove_prefix(separator + 1);
    }
    return result;
  }

  bycorf::Task<absl::Status> Exercise(bycorf::Worker& worker) {
    const lavik::ReplicationIdentity local =
        co_await replication_->ObserveIdentity();
    auto manifest = lavik::PopulationManifest::Create({});
    if (!manifest.ok()) co_return manifest.status();
    lavik::RebuildIdentity identity{
        .group_id_ = "source-follow-group",
        .assignment_id_ = "source-assignment",
        .term_ = 1,
        .directive_revision_ = 1,
        .authority_id_ = "source-follow-authority",
        .target_node_id_ = local.local_node_id_,
        .target_boot_id_ = local.boot_id_,
        .target_history_id_ = local.local_history_id_,
        .operation_id_ = "source-follow-operation",
        .directive_id_ = "source-follow-directive",
        .attempt_id_ = "source-follow-attempt",
        .manifest_revision_ = 1,
        .manifest_id_ = manifest->id(),
        .partition_replication_epoch_ = 1,
    };
    auto initialized =
        co_await replication_->StartEmptyPopulationInitialization(identity,
                                                                  *manifest);
    if (!initialized.ok()) co_return initialized.status();
    if (absl::Status ready = co_await initialized->Await(); !ready.ok()) {
      co_return ready;
    }
    auto watermark = co_await replication_->CaptureNativeReplicationWatermark();
    if (!watermark.ok() || !watermark->has_value() ||
        (*watermark)->next_lsns_.size() != 1) {
      co_return watermark.ok()
          ? TestFailure("source history was not export-ready")
          : watermark.status();
    }

    // POPULATION capability currentness and finite lease admission are
    // independent. Expiry closes only new handshakes; it neither scans the FDS
    // ledger nor cancels a session already published under the exact
    // capability. The wire marker lets a target distinguish this transient
    // edge from arbitrary peer/protocol failure.
    const std::string population_target_node(40, 'a');
    const std::string population_target_boot(40, 'b');
    lavik::RebuildDirective source_authorization{
        .identity_ =
            {
                .group_id_ = identity.group_id_,
                .assignment_id_ = "population-target-assignment",
                .term_ = 2,
                .directive_revision_ = 10,
                .authority_id_ = "population-source-authority",
                .source_node_id_ = local.local_node_id_,
                .source_assignment_id_ = identity.assignment_id_,
                .source_boot_id_ = local.boot_id_,
                .source_history_id_ = (*watermark)->history_id_,
                .target_node_id_ = population_target_node,
                .target_boot_id_ = population_target_boot,
                .operation_id_ = "population-operation",
                .directive_id_ = "population-authorize-directive",
                .attempt_id_ = "population-authorize-attempt",
                .manifest_revision_ = identity.manifest_revision_,
                .manifest_id_ = identity.manifest_id_,
                .partition_replication_epoch_ =
                    identity.partition_replication_epoch_,
            },
        .flow_count_ = 1,
        .safe_source_active_ = true,
    };
    absl::Status authorized =
        co_await replication_->AuthorizeClusterRebuildSource(
            source_authorization);
    if (!authorized.ok()) co_return authorized;
    const auto population_control_args = [&] {
      return std::vector<std::string>{
          "LVPSYNC",
          "1",
          "?" + population_target_node + ":6380",
          "?",
          "?",
          std::string(40, 'c'),
          population_target_boot,
          "?",
          "POPULATION",
          identity.group_id_,
          "population-target-assignment",
          identity.assignment_id_,
          "2",
          "11",
          "population-source-authority",
          local.local_node_id_,
          local.boot_id_,
          (*watermark)->history_id_,
          population_target_node,
          population_target_boot,
          "population-operation",
          "population-rebuild-directive",
          "population-rebuild-attempt",
          "1",
          manifest->id().Hex(),
          "1",
      };
    };
    if (!admission_entered_.empty()) {
      // Hold one authorized LVPSYNC after its optimistic gate check but before
      // registry publication. Strong revoke must close the shared gate, wait
      // for that unpublished control, and force its second check to return the
      // typed pre-mutation retry marker instead of publishing stale authority.
      const auto crossing_deadline =
          lavik::cluster::LeaseClockNow() + std::chrono::seconds(30);
      absl::Status enabled =
          co_await replication_->EnableClusterRebuildSourceAdmissionUntil(
              crossing_deadline.time_since_epoch());
      if (!enabled.ok()) co_return enabled;
      auto crossing = OpenPeer(worker);
      if (!crossing.ok()) co_return crossing.status();
      auto crossing_result = std::make_shared<RequestResult>();
      retained_requests_.push_back(crossing_result);
      worker.Spawn(RunNativeRequest(crossing->stream_,
                                    population_control_args(), 96,
                                    crossing_result.get()));
      absl::Status barrier = co_await AwaitFaultBarrier(
          admission_entered_,
          "population source admission publication barrier");
      if (!barrier.ok()) {
        (void)::shutdown(crossing->peer_fd_, SHUT_RDWR);
        (void)co_await WaitDone(worker, *crossing_result,
                                "failed admission barrier cleanup");
        co_return barrier;
      }

      // Population bootstrap may itself execute an empty strong retirement.
      // Discard any earlier acknowledgement so the next file creation proves
      // that this exact concurrently spawned revoker closed the gate.
      std::error_code stale_ack_error;
      (void)std::filesystem::remove(revocation_closed_, stale_ack_error);
      if (stale_ack_error) {
        co_return absl::InternalError(absl::StrCat(
            "could not reset population source revocation barrier: ",
            stale_ack_error.message()));
      }
      auto revocation_result = std::make_shared<RequestResult>();
      retained_requests_.push_back(revocation_result);
      worker.Spawn(RunSourceRevocation(revocation_result.get()));
      barrier = co_await AwaitFaultBarrier(
          revocation_closed_, "population source revocation gate barrier");
      if (!barrier.ok()) co_return barrier;
      if (revocation_result->done_) {
        co_return TestFailure(
            "source revocation did not join the unpublished LVPSYNC");
      }
      std::error_code release_error;
      const bool released =
          std::filesystem::remove(admission_entered_, release_error);
      if (!released || release_error) {
        co_return absl::InternalError(absl::StrCat(
            "could not release population source admission barrier: ",
            release_error.message()));
      }

      auto crossing_reply = co_await ReadPeerLine(
          worker, crossing->peer_fd_, "revoked population source response");
      if (!crossing_reply.ok()) co_return crossing_reply.status();
      if (*crossing_reply != "-LVLEASESUSPENDED") {
        co_return TestFailure(
            "revocation crossing did not return the lease-suspended marker");
      }
      absl::Status waited = co_await WaitDone(
          worker, *crossing_result, "revoked population source completion");
      if (!waited.ok()) co_return waited;
      if (crossing_result->status_.code() != absl::StatusCode::kUnavailable) {
        co_return TestFailure(
            "revocation crossing returned an unsafe terminal status");
      }
      waited = co_await WaitDone(worker, *revocation_result,
                                 "population source revocation completion");
      if (!waited.ok()) co_return waited;
      if (!revocation_result->status_.ok()) {
        co_return revocation_result->status_;
      }
      co_return absl::OkStatus();
    }
    auto suspended = OpenPeer(worker);
    if (!suspended.ok()) co_return suspended.status();
    RequestResult suspended_result;
    worker.Spawn(RunNativeRequest(suspended->stream_, population_control_args(),
                                  91, &suspended_result));
    auto suspended_reply = co_await ReadPeerLine(
        worker, suspended->peer_fd_, "suspended population source response");
    if (!suspended_reply.ok()) co_return suspended_reply.status();
    if (*suspended_reply != "-LVLEASESUSPENDED") {
      co_return TestFailure(
          "lease-suspended population source did not return its wire marker");
    }
    absl::Status waited = co_await WaitDone(
        worker, suspended_result, "suspended population source completion");
    if (!waited.ok()) co_return waited;
    if (suspended_result.status_.code() != absl::StatusCode::kUnavailable) {
      co_return TestFailure(
          "lease-suspended population source returned the wrong status");
    }

    // Meta may renew the lease only after the first target attempt observes
    // the suspended admission gate. The still-current capability must retain
    // the history it names across that gap; otherwise renewal reopens an
    // authorization that no target can satisfy.
    waited = co_await bycorf::SleepFor(worker, 100ms);
    if (!waited.ok()) co_return waited;
    const lavik::ReplicationIdentity retained =
        co_await replication_->ObserveIdentity();
    if (retained.local_history_id_ != (*watermark)->history_id_) {
      co_return TestFailure(
          "current population source capability did not retain its history");
    }

    const auto live_deadline =
        lavik::cluster::LeaseClockNow() + std::chrono::seconds(5);
    absl::Status enabled =
        co_await replication_->EnableClusterRebuildSourceAdmissionUntil(
            live_deadline.time_since_epoch());
    if (!enabled.ok()) co_return enabled;

    // FDS installation clears the old capability before its authorize-source
    // directive replays. A target that independently received its rebuild may
    // race into this exact gap: native admission must expose the typed retry
    // marker, while the replay reservation keeps the named source history
    // alive even though no export session exists yet.
    absl::Status refreshed =
        co_await replication_
            ->RefreshClusterRebuildSourceAuthorizationsForFdsReplacement(
                /*preserve_established_exports=*/true,
                /*expected_authorization_replays=*/1);
    if (!refreshed.ok()) co_return refreshed;
    auto replay_gap = OpenPeer(worker);
    if (!replay_gap.ok()) co_return replay_gap.status();
    RequestResult replay_gap_result;
    worker.Spawn(RunNativeRequest(replay_gap->stream_,
                                  population_control_args(), 95,
                                  &replay_gap_result));
    auto replay_gap_reply = co_await ReadPeerLine(
        worker, replay_gap->peer_fd_, "FDS replay-gap population response");
    if (!replay_gap_reply.ok()) co_return replay_gap_reply.status();
    if (*replay_gap_reply != "-LVLEASESUSPENDED") {
      co_return TestFailure(
          "FDS replay gap did not return the lease-suspended marker");
    }
    waited = co_await WaitDone(worker, replay_gap_result,
                               "FDS replay-gap population completion");
    if (!waited.ok()) co_return waited;
    if (replay_gap_result.status_.code() != absl::StatusCode::kUnavailable) {
      co_return TestFailure("FDS replay gap returned the wrong status");
    }
    waited = co_await bycorf::SleepFor(worker, 20ms);
    if (!waited.ok()) co_return waited;
    const lavik::ReplicationIdentity replay_gap_identity =
        co_await replication_->ObserveIdentity();
    if (replay_gap_identity.local_history_id_ != (*watermark)->history_id_) {
      co_return TestFailure(
          "FDS replay reservation did not retain source history");
    }
    authorized = co_await replication_->AuthorizeClusterRebuildSource(
        source_authorization);
    if (!authorized.ok()) co_return authorized;

    auto established = OpenPeer(worker);
    if (!established.ok()) co_return established.status();
    RequestResult established_result;
    worker.Spawn(RunNativeRequest(established->stream_,
                                  population_control_args(), 92,
                                  &established_result));
    auto established_reply = co_await ReadPeerLine(
        worker, established->peer_fd_, "admitted population source response");
    if (!established_reply.ok()) co_return established_reply.status();
    if (!established_reply->starts_with("+LVFULLRESYNC ")) {
      co_return TestFailure("valid lease did not admit the population source");
    }
    // A later FDS may retain this exact authorization while adding the target
    // rebuild directive. The target can already have received LVFULLRESYNC but
    // not yet published every flow/ONLINE marker when the source installs that
    // FDS. Exact-scope replacement must grandfather that published POPULATION
    // session; otherwise the target observes an unclassified peer-close after
    // admission and cannot safely retry it as a lease edge.
    refreshed =
        co_await replication_
            ->RefreshClusterRebuildSourceAuthorizationsForFdsReplacement(
                /*preserve_established_exports=*/true,
                /*expected_authorization_replays=*/1);
    if (!refreshed.ok()) co_return refreshed;
    waited = co_await bycorf::SleepFor(worker, 20ms);
    if (!waited.ok()) co_return waited;
    if (established_result.done_) {
      co_return TestFailure(
          "exact FDS refresh cancelled an admitted pre-ONLINE population "
          "session");
    }
    authorized = co_await replication_->AuthorizeClusterRebuildSource(
        source_authorization);
    if (!authorized.ok()) co_return authorized;
    absl::Status expiration =
        co_await replication_->RevokeClusterExpirationAuthority();
    if (!expiration.ok()) co_return expiration;
    waited = co_await bycorf::SleepFor(worker, 20ms);
    if (!waited.ok()) co_return waited;
    if (established_result.done_) {
      co_return TestFailure(
          "lease expiry cancelled an already-published population session");
    }
    auto after_expiry = OpenPeer(worker);
    if (!after_expiry.ok()) co_return after_expiry.status();
    RequestResult after_expiry_result;
    worker.Spawn(RunNativeRequest(after_expiry->stream_,
                                  population_control_args(), 93,
                                  &after_expiry_result));
    auto after_expiry_reply = co_await ReadPeerLine(
        worker, after_expiry->peer_fd_, "post-expiry population response");
    if (!after_expiry_reply.ok()) co_return after_expiry_reply.status();
    if (*after_expiry_reply != "-LVLEASESUSPENDED") {
      co_return TestFailure("lease expiry did not close new source admission");
    }
    // Native admission checks the absolute deadline synchronously. This stays
    // closed after the cut even when no expiry timer invokes the cleanup API.
    const auto short_deadline =
        lavik::cluster::LeaseClockNow() + std::chrono::milliseconds(5);
    enabled = co_await replication_->EnableClusterRebuildSourceAdmissionUntil(
        short_deadline.time_since_epoch());
    if (!enabled.ok()) co_return enabled;
    waited = co_await bycorf::SleepFor(worker, 10ms);
    if (!waited.ok()) co_return waited;
    auto deadline_expired = OpenPeer(worker);
    if (!deadline_expired.ok()) co_return deadline_expired.status();
    RequestResult deadline_expired_result;
    worker.Spawn(RunNativeRequest(deadline_expired->stream_,
                                  population_control_args(), 94,
                                  &deadline_expired_result));
    auto deadline_expired_reply = co_await ReadPeerLine(
        worker, deadline_expired->peer_fd_, "deadline-expired source response");
    if (!deadline_expired_reply.ok()) co_return deadline_expired_reply.status();
    if (*deadline_expired_reply != "-LVLEASESUSPENDED") {
      co_return TestFailure(
          "native admission ignored an elapsed source lease deadline");
    }
    absl::Status cleared =
        co_await replication_
            ->ClearClusterRebuildSourceAuthorizationsForSessionReplacement(
                /*preserve_established_exports=*/true);
    if (!cleared.ok()) co_return cleared;
    waited = co_await WaitDone(worker, established_result,
                               "strong population source cleanup");
    if (!waited.ok()) co_return waited;

    lavik::DesiredClusterUpstream desired{
        .group_id_ = identity.group_id_,
        .group_term_ = 2,
        .local_node_id_ = local.local_node_id_,
        .local_assignment_id_ = identity.assignment_id_,
        .local_boot_id_ = local.boot_id_,
        .owner_node_id_ = local.local_node_id_,
        .owner_assignment_id_ = identity.assignment_id_,
        .manifest_revision_ = identity.manifest_revision_,
        .manifest_id_ = identity.manifest_id_,
        .partition_replication_epoch_ = identity.partition_replication_epoch_,
        .members_ =
            {
                {local.local_node_id_, identity.assignment_id_},
                {std::string(40, '1'), "target-assignment-1"},
                {std::string(40, '2'), "target-assignment-2"},
            },
    };
    absl::Status reconciled =
        co_await replication_->ReconcileClusterFollowOwner(desired);
    if (!reconciled.ok()) co_return reconciled;
    const std::string group_token = HexString(identity.group_id_);
    const auto control_args = [&](std::string node, std::string assignment,
                                  std::string incarnation, std::string boot,
                                  bool matching_history) {
      return std::vector<std::string>{
          "LVPSYNC",
          "1",
          "?" + node + ":6380",
          matching_history ? group_token : "?",
          matching_history ? (*watermark)->history_id_ : "?",
          std::move(incarnation),
          std::move(boot),
          matching_history ? std::to_string((*watermark)->next_lsns_.front())
                           : "?",
          "FOLLOW",
          identity.group_id_,
          std::move(assignment),
          identity.assignment_id_,
          "2",
          local.local_node_id_,
          "1",
          manifest->id().Hex(),
          "1",
      };
    };

    auto first = OpenPeer(worker);
    auto second = OpenPeer(worker);
    if (!first.ok()) co_return first.status();
    if (!second.ok()) co_return second.status();
    RequestResult first_control;
    RequestResult second_control;
    worker.Spawn(RunNativeRequest(
        first->stream_,
        control_args(std::string(40, '1'), "target-assignment-1",
                     std::string(40, '5'), std::string(40, '3'), true),
        101, &first_control));
    worker.Spawn(RunNativeRequest(
        second->stream_,
        control_args(std::string(40, '2'), "target-assignment-2",
                     std::string(40, '6'), std::string(40, '4'), false),
        102, &second_control));
    auto first_reply = co_await ReadPeerLine(worker, first->peer_fd_,
                                             "first steady source response");
    auto second_reply = co_await ReadPeerLine(worker, second->peer_fd_,
                                              "second steady source response");
    if (!first_reply.ok()) co_return first_reply.status();
    if (!second_reply.ok()) co_return second_reply.status();
    const std::vector<std::string_view> first_words = Words(*first_reply);
    const std::vector<std::string_view> second_words = Words(*second_reply);
    if (first_words.size() != 8 || second_words.size() != 8 ||
        first_words[0] != "+LVFULLRESYNC" ||
        second_words[0] != "+LVFULLRESYNC" || first_words[3] != group_token ||
        second_words[3] != group_token) {
      co_return TestFailure(
          "concurrent followers did not receive the exact steady export");
    }

    reconciled = co_await replication_->ReconcileClusterFollowOwner(desired);
    if (!reconciled.ok()) co_return reconciled;
    waited = co_await bycorf::SleepFor(worker, 20ms);
    if (!waited.ok()) co_return waited;
    if (first_control.done_ || second_control.done_) {
      co_return TestFailure(
          "exact source desired replay restarted established exports");
    }

    auto unauthorized = OpenPeer(worker);
    if (!unauthorized.ok()) co_return unauthorized.status();
    RequestResult unauthorized_result;
    worker.Spawn(RunNativeRequest(
        unauthorized->stream_,
        control_args(std::string(40, '7'), "target-assignment-7",
                     std::string(40, '8'), std::string(40, '9'), false),
        103, &unauthorized_result));
    waited = co_await WaitDone(worker, unauthorized_result,
                               "unauthorized steady source request");
    if (!waited.ok()) co_return waited;
    if (unauthorized_result.status_.code() !=
        absl::StatusCode::kPermissionDenied) {
      co_return TestFailure(
          "steady source accepted a target outside the desired membership");
    }

    auto first_flow = OpenPeer(worker);
    auto second_flow = OpenPeer(worker);
    if (!first_flow.ok()) co_return first_flow.status();
    if (!second_flow.ok()) co_return second_flow.status();
    RequestResult first_flow_result;
    RequestResult second_flow_result;
    worker.Spawn(
        RunNativeRequest(first_flow->stream_,
                         {"LVFLOW", "1", std::string(first_words[1]), "0",
                          std::to_string((*watermark)->next_lsns_.front()), "1",
                          std::string(first_words[7])},
                         104, &first_flow_result));
    worker.Spawn(RunNativeRequest(second_flow->stream_,
                                  {"LVFLOW", "1", std::string(second_words[1]),
                                   "0", "1", "0", std::string(second_words[7])},
                                  105, &second_flow_result));
    auto first_mode = co_await ReadPeerLine(worker, first_flow->peer_fd_,
                                            "same-history flow mode");
    auto second_mode = co_await ReadPeerLine(worker, second_flow->peer_fd_,
                                             "mismatched-history flow mode");
    if (!first_mode.ok()) co_return first_mode.status();
    if (!second_mode.ok()) co_return second_mode.status();
    if (!first_mode->ends_with(" CONTINUE") ||
        !second_mode->ends_with(" FULL")) {
      co_return TestFailure(
          "steady source did not reuse native CONTINUE/FULL selection");
    }

    reconciled =
        co_await replication_->ReconcileClusterFollowOwner(std::nullopt);
    if (!reconciled.ok()) co_return reconciled;
    waited =
        co_await WaitDone(worker, first_control, "first steady export cleanup");
    if (!waited.ok()) co_return waited;
    waited = co_await WaitDone(worker, second_control,
                               "second steady export cleanup");
    if (!waited.ok()) co_return waited;
    // Both probes have ended without an online downstream. Let the standalone
    // idle-history monitor's 10 ms tick run: Meta still owns this population
    // and its advertised history even while no replica is connected.
    waited = co_await bycorf::SleepFor(worker, 50ms);
    if (!waited.ok()) co_return waited;
    const auto retained_history =
        co_await replication_->CaptureNativeReplicationWatermark();
    const auto retained_identity = co_await replication_->ObserveIdentity();
    if (!retained_history.ok() || !retained_history->has_value() ||
        (*retained_history)->history_id_ != (*watermark)->history_id_ ||
        retained_identity.local_history_id_ != (*watermark)->history_id_) {
      co_return TestFailure(
          "idle source cleanup retired Meta's owned replication history");
    }
    const lavik::ClusterPopulationStatus population =
        co_await replication_->cluster_population_status();
    if (population.state_ != lavik::ReplicationGroupState::kReady ||
        !population.ready_token_.has_value() || replication_->is_replica()) {
      co_return TestFailure(
          "steady source cleanup retired the Owner population or role");
    }
    co_return absl::OkStatus();
  }

  lavik::storage::StorageEngine* storage_ = nullptr;
  lavik::ReplicationManager* replication_ = nullptr;
  std::filesystem::path admission_entered_;
  std::filesystem::path revocation_closed_;
  std::vector<std::shared_ptr<RequestResult>> retained_requests_;
  std::vector<int> peer_fds_;
  absl::Status result_ = absl::OkStatus();
};

TEST(ReplicationManagerIntegrationTest,
     ClusterControlApiStaysFailClosedAndSupersedesWholeSession) {
  const std::string expected_node_id(40, '9');
  constexpr std::uint16_t kReplicationPort = 6380;
  lavik::test::TempDirectory directory("cluster-manager-api");
  const std::filesystem::path data = directory.path() / "node.data";
  lavik::test::CreateDataFile(data, 128 * kMiB);

  StallingNativeSource source(expected_node_id, kReplicationPort);
  ASSERT_NE(source.port(), 0);
  ASSERT_EQ(source.error(), 0) << std::strerror(source.error());

  lavik::storage::StorageEngineOptions storage_options;
  storage_options.data_files_ = {data.string()};
  storage_options.expiration_authority_ = false;
  storage_options.buffers_.registered_bytes_ = 64 * kMiB;
  storage_options.replication_publish_queue_bytes_ = 16 * kMiB;
  lavik::storage::StorageEngine storage(std::move(storage_options));
  lavik::InitWorkerMetrics(1);
  ASSERT_TRUE(lavik::InitMemoryLimit(512 * kMiB, 1).ok());
  ASSERT_TRUE(storage.Prepare(1).ok());

  lavik::ReplicationOptions replication_options;
  replication_options.cluster_enabled_ = true;
  replication_options.node_id_override_ = expected_node_id;
  replication_options.listen_port_ = kReplicationPort;
  lavik::ReplicationManager replication(
      &storage, std::move(replication_options),
      lavik::ReplicaOfConfig{"127.0.0.1", source.port()});
  lavik::InitStorage(&storage, &replication);
  EnsureTxRuntime();

  ReplicationManagerService service(&storage, &replication, &source,
                                    expected_node_id);
  bycorf::Server server;
  server.AddService(&service);
  bycorf::ServerOptions runtime;
  runtime.thread_count_ = 1;
  runtime.pin_workers_ = false;
  runtime.recv_buffer_count_ = 0;
  ASSERT_TRUE(server.Start(runtime).ok());
  server.WaitUntilStopped();
  EXPECT_TRUE(service.result().ok()) << service.result();
}

void RunTargetLeaseAdmissionRetryCase(unsigned suspended_responses,
                                      unsigned expected_connections,
                                      absl::StatusCode expected_terminal,
                                      std::string_view directory_name) {
  const std::string expected_node_id(40, '9');
  constexpr std::uint16_t kReplicationPort = 6380;
  lavik::test::TempDirectory directory{std::string(directory_name)};
  const std::filesystem::path data = directory.path() / "node.data";
  lavik::test::CreateDataFile(data, 128 * kMiB);

  StallingNativeSource source(expected_node_id, kReplicationPort,
                              suspended_responses);
  ASSERT_NE(source.port(), 0);
  ASSERT_EQ(source.error(), 0) << std::strerror(source.error());
  lavik::storage::StorageEngineOptions storage_options;
  storage_options.data_files_ = {data.string()};
  storage_options.expiration_authority_ = false;
  storage_options.buffers_.registered_bytes_ = 64 * kMiB;
  storage_options.replication_publish_queue_bytes_ = 16 * kMiB;
  lavik::storage::StorageEngine storage(std::move(storage_options));
  lavik::InitWorkerMetrics(1);
  ASSERT_TRUE(lavik::InitMemoryLimit(512 * kMiB, 1).ok());
  ASSERT_TRUE(storage.Prepare(1).ok());

  lavik::ReplicationOptions replication_options;
  replication_options.cluster_enabled_ = true;
  replication_options.node_id_override_ = expected_node_id;
  replication_options.listen_port_ = kReplicationPort;
  lavik::ReplicationManager replication(
      &storage, std::move(replication_options), std::nullopt);
  lavik::InitStorage(&storage, &replication);
  EnsureTxRuntime();

  TargetLeaseAdmissionRetryService service(
      &storage, &replication, &source, expected_connections, expected_terminal);
  bycorf::Server server;
  server.AddService(&service);
  bycorf::ServerOptions runtime;
  runtime.thread_count_ = 1;
  runtime.pin_workers_ = false;
  runtime.recv_buffer_count_ = 0;
  ASSERT_TRUE(server.Start(runtime).ok());
  server.WaitUntilStopped();
  EXPECT_TRUE(service.result().ok()) << service.result();
  EXPECT_EQ(source.error(), 0) << std::strerror(source.error());
}

TEST(ReplicationManagerIntegrationTest,
     PopulationTargetRetriesOnlyExplicitLeaseMarkerBeforeMutation) {
  RunTargetLeaseAdmissionRetryCase(
      /*suspended_responses=*/1, /*expected_connections=*/2,
      absl::StatusCode::kFailedPrecondition, "target-lease-marker-retry");
}

TEST(ReplicationManagerIntegrationTest,
     PopulationTargetBoundsLeaseMarkerRetriesBeforeMutation) {
  RunTargetLeaseAdmissionRetryCase(
      /*suspended_responses=*/4, /*expected_connections=*/4,
      absl::StatusCode::kUnavailable, "target-lease-marker-retry-limit");
}

TEST(ReplicationManagerIntegrationTest,
     InitializesAndRetainsSourceLessEmptyPopulation) {
  const std::string expected_node_id(40, '8');
  lavik::test::TempDirectory directory("empty-population");
  const std::filesystem::path data = directory.path() / "node.data";
  lavik::test::CreateDataFile(data, 128 * kMiB);

  lavik::storage::StorageEngineOptions storage_options;
  storage_options.data_files_ = {data.string()};
  storage_options.expiration_authority_ = false;
  storage_options.buffers_.registered_bytes_ = 64 * kMiB;
  storage_options.replication_publish_queue_bytes_ = 16 * kMiB;
  lavik::storage::StorageEngine storage(std::move(storage_options));
  lavik::InitWorkerMetrics(1);
  ASSERT_TRUE(lavik::InitMemoryLimit(512 * kMiB, 1).ok());
  ASSERT_TRUE(storage.Prepare(1).ok());

  lavik::ReplicationOptions replication_options;
  replication_options.cluster_enabled_ = true;
  replication_options.node_id_override_ = expected_node_id;
  lavik::ReplicationManager replication(
      &storage, std::move(replication_options), std::nullopt);
  lavik::InitStorage(&storage, &replication);
  EnsureTxRuntime();

  EmptyPopulationService service(&storage, &replication);
  bycorf::Server server;
  server.AddService(&service);
  bycorf::ServerOptions runtime;
  runtime.thread_count_ = 1;
  runtime.pin_workers_ = false;
  runtime.recv_buffer_count_ = 0;
  ASSERT_TRUE(server.Start(runtime).ok());
  server.WaitUntilStopped();
  EXPECT_TRUE(service.result().ok()) << service.result();
}

#if LAVIK_FAULTS_ENABLED
TEST(ReplicationManagerIntegrationTest,
     EmptyPopulationPromotionFailureRemainsFailedStopped) {
  ASSERT_EQ(::setenv("LAVIK_REPLICATION_FAIL_PROMOTE_ONCE", "1", 1), 0);
  struct FaultReset {
    ~FaultReset() { (void)::unsetenv("LAVIK_REPLICATION_FAIL_PROMOTE_ONCE"); }
  } fault_reset;

  const std::string expected_node_id(40, '9');
  lavik::test::TempDirectory directory("empty-population-promote-failure");
  const std::filesystem::path data = directory.path() / "node.data";
  lavik::test::CreateDataFile(data, 128 * kMiB);

  lavik::storage::StorageEngineOptions storage_options;
  storage_options.data_files_ = {data.string()};
  storage_options.expiration_authority_ = false;
  storage_options.buffers_.registered_bytes_ = 64 * kMiB;
  storage_options.replication_publish_queue_bytes_ = 16 * kMiB;
  lavik::storage::StorageEngine storage(std::move(storage_options));
  lavik::InitWorkerMetrics(1);
  ASSERT_TRUE(lavik::InitMemoryLimit(512 * kMiB, 1).ok());
  ASSERT_TRUE(storage.Prepare(1).ok());

  lavik::ReplicationOptions replication_options;
  replication_options.cluster_enabled_ = true;
  replication_options.node_id_override_ = expected_node_id;
  lavik::ReplicationManager replication(
      &storage, std::move(replication_options), std::nullopt);
  lavik::InitStorage(&storage, &replication);
  EnsureTxRuntime();

  EmptyPopulationService service(&storage, &replication,
                                 EmptyPopulationExpectation::kFailedStopped);
  bycorf::Server server;
  server.AddService(&service);
  bycorf::ServerOptions runtime;
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
  lavik::test::TempDirectory directory(directory_name);
  const std::filesystem::path data = directory.path() / "node.data";
  lavik::test::CreateDataFile(data, 128 * kMiB);

  lavik::storage::StorageEngineOptions storage_options;
  storage_options.data_files_ = {data.string()};
  storage_options.expiration_authority_ = false;
  storage_options.buffers_.registered_bytes_ = 64 * kMiB;
  storage_options.replication_publish_queue_bytes_ = 16 * kMiB;
  lavik::storage::StorageEngine storage(std::move(storage_options));
  lavik::InitWorkerMetrics(1);
  ASSERT_TRUE(lavik::InitMemoryLimit(512 * kMiB, 1).ok());
  ASSERT_TRUE(storage.Prepare(1).ok());

  lavik::ReplicationOptions replication_options;
  replication_options.cluster_enabled_ = true;
  replication_options.node_id_override_ = expected_node_id;
  lavik::ReplicationManager replication(
      &storage, std::move(replication_options), std::nullopt);
  lavik::InitStorage(&storage, &replication);
  EnsureTxRuntime();

  EmptyPopulationService service(
      &storage, &replication, EmptyPopulationExpectation::kRecoverableFailure);
  bycorf::Server server;
  server.AddService(&service);
  bycorf::ServerOptions runtime;
  runtime.thread_count_ = 1;
  runtime.pin_workers_ = false;
  runtime.recv_buffer_count_ = 0;
  ASSERT_TRUE(server.Start(runtime).ok());
  server.WaitUntilStopped();
  EXPECT_TRUE(service.result().ok()) << service.result();
}

TEST(ReplicationManagerIntegrationTest,
     EmptyPopulationResetFailureRemainsLoading) {
  RunRecoverableEmptyPopulationFault("LAVIK_REPLICATION_FAIL_EMPTY_RESET_ONCE",
                                     'a', "empty-population-reset-failure");
}

TEST(ReplicationManagerIntegrationTest,
     EmptyPopulationCatalogFailureRemainsLoading) {
  RunRecoverableEmptyPopulationFault(
      "LAVIK_REPLICATION_FAIL_EMPTY_CATALOG_ONCE", 'b',
      "empty-population-catalog-failure");
}
#endif

TEST(ReplicationManagerIntegrationTest,
     StandaloneManagersGenerateDistinctCanonicalNodeIdentities) {
  lavik::test::TempDirectory directory("standalone-manager-revoke");
  const std::filesystem::path data = directory.path() / "node.data";
  lavik::test::CreateDataFile(data, 128 * kMiB);

  lavik::storage::StorageEngineOptions storage_options;
  storage_options.data_files_ = {data.string()};
  lavik::storage::StorageEngine storage(std::move(storage_options));
  ASSERT_TRUE(storage.Prepare(1).ok());

  lavik::ReplicationManager first(&storage, lavik::ReplicationOptions{},
                                  std::nullopt);
  lavik::ReplicationManager second(&storage, lavik::ReplicationOptions{},
                                   lavik::ReplicaOfConfig{"127.0.0.1", 1});
  StandaloneIdentityService service(&first, &second);
  bycorf::Server server;
  server.AddService(&service);
  bycorf::ServerOptions runtime;
  runtime.thread_count_ = 1;
  runtime.pin_workers_ = false;
  runtime.recv_buffer_count_ = 0;
  ASSERT_TRUE(server.Start(runtime).ok());
  server.WaitUntilStopped();
  EXPECT_TRUE(service.result().ok()) << service.result();
}

TEST(ReplicationManagerIntegrationTest,
     MetaManagedPromotionAcceptsPriorTermReadyPopulationAndStaysFenced) {
  RunPromotionPrepareCase({});
}

void RunFailoverActionDispositionCase(PreparedActionDisposition disposition,
                                      std::string_view fixture_name) {
#if !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires a Debug/fault build for candidate seeding";
#endif
  const bool replace_while_preparing =
      disposition == PreparedActionDisposition::kReplaceWhilePreparing;
  const bool remove_while_preparing =
      disposition == PreparedActionDisposition::kRemoveWhilePreparing ||
      disposition ==
          PreparedActionDisposition::kRemoveWhilePreparingThenResumeFollow;
  const bool remove_after_durability =
      disposition ==
          PreparedActionDisposition::kRemoveAfterDurabilityBoundary ||
      disposition == PreparedActionDisposition::
                         kRemoveAfterDurabilityBoundaryWithPublicationFailure;
  const bool fail_after_child_history =
      disposition == PreparedActionDisposition::
                         kRemoveAfterDurabilityBoundaryWithPublicationFailure;
  const bool stall_before_durability =
      replace_while_preparing || remove_while_preparing;
  const bool stall_promotion =
      stall_before_durability || remove_after_durability;
  lavik::test::TempDirectory directory(fixture_name);
  PromotionFaultBarrierPaths fault_barriers{
      .prepare_entered_ = directory.path() / "promotion-prepare-entered",
      .runner_waiting_ = directory.path() / "failover-runner-waiting",
      .runner_terminal_ = directory.path() / "failover-runner-terminal",
  };
  ASSERT_EQ(::setenv("LAVIK_REPLICATION_SEED_READY_PROMOTION_CANDIDATE",
                     "02020202020202020202020202020202", 1),
            0);
  if (stall_before_durability) {
    ASSERT_EQ(::setenv("LAVIK_REPLICATION_STALL_PROMOTION_ACTION",
                       "02020202020202020202020202020202", 1),
              0);
    ASSERT_EQ(::setenv("LAVIK_REPLICATION_PROMOTION_PRE_DURABILITY_BARRIER_ACK_"
                       "PATH",
                       fault_barriers.prepare_entered_.c_str(), 1),
              0);
  }
  if (remove_after_durability) {
    ASSERT_EQ(::setenv("LAVIK_REPLICATION_STALL_PROMOTION_AFTER_DURABILITY_"
                       "BOUNDARY",
                       "02020202020202020202020202020202", 1),
              0);
    ASSERT_EQ(
        ::setenv("LAVIK_REPLICATION_PROMOTION_POST_DURABILITY_BARRIER_ACK_"
                 "PATH",
                 fault_barriers.prepare_entered_.c_str(), 1),
        0);
  }
  if (stall_promotion) {
    ASSERT_EQ(::setenv("LAVIK_REPLICATION_FAILOVER_RUNNER_WAITING_ACK_PATH",
                       fault_barriers.runner_waiting_.c_str(), 1),
              0);
    ASSERT_EQ(::setenv("LAVIK_REPLICATION_FAILOVER_RUNNER_TERMINAL_ACK_PATH",
                       fault_barriers.runner_terminal_.c_str(), 1),
              0);
  }
  if (fail_after_child_history) {
    ASSERT_EQ(::setenv("LAVIK_REPLICATION_FAIL_PROMOTION_PREPARE_AT",
                       "evidence-publication", 1),
              0);
  }
  struct SeedReset {
    bool stall_before_durability_;
    bool stall_after_durability_;
    bool fail_after_child_history_;
    bool promotion_barriers_;
    ~SeedReset() {
      (void)::unsetenv("LAVIK_REPLICATION_SEED_READY_PROMOTION_CANDIDATE");
      if (stall_before_durability_) {
        (void)::unsetenv("LAVIK_REPLICATION_STALL_PROMOTION_ACTION");
        (void)::unsetenv(
            "LAVIK_REPLICATION_PROMOTION_PRE_DURABILITY_BARRIER_ACK_PATH");
      }
      if (stall_after_durability_) {
        (void)::unsetenv(
            "LAVIK_REPLICATION_STALL_PROMOTION_AFTER_DURABILITY_BOUNDARY");
        (void)::unsetenv(
            "LAVIK_REPLICATION_PROMOTION_POST_DURABILITY_BARRIER_ACK_PATH");
      }
      if (fail_after_child_history_) {
        (void)::unsetenv("LAVIK_REPLICATION_FAIL_PROMOTION_PREPARE_AT");
      }
      if (promotion_barriers_) {
        (void)::unsetenv("LAVIK_REPLICATION_FAILOVER_RUNNER_WAITING_ACK_PATH");
        (void)::unsetenv("LAVIK_REPLICATION_FAILOVER_RUNNER_TERMINAL_ACK_PATH");
      }
    }
  } seed_reset{stall_before_durability, remove_after_durability,
               fail_after_child_history, stall_promotion};
  const std::filesystem::path data = directory.path() / "node.data";
  lavik::test::CreateDataFile(data, 128 * kMiB);

  lavik::storage::StorageEngineOptions storage_options;
  storage_options.data_files_ = {data.string()};
  storage_options.expiration_authority_ = false;
  storage_options.buffers_.registered_bytes_ = 64 * kMiB;
  storage_options.replication_publish_queue_bytes_ = 16 * kMiB;
  lavik::storage::StorageEngine storage(std::move(storage_options));
  lavik::InitWorkerMetrics(1);
  ASSERT_TRUE(lavik::InitMemoryLimit(512 * kMiB, 1).ok());
  ASSERT_TRUE(storage.Prepare(1).ok());

  lavik::ReplicationOptions options;
  options.cluster_enabled_ = true;
  options.node_id_override_ = std::string(40, '9');
  lavik::ReplicationManager replication(&storage, std::move(options),
                                        std::nullopt);
  lavik::InitStorage(&storage, &replication);
  EnsureTxRuntime();

  std::unique_ptr<FollowOwnerSource> continuation_source;
  if (disposition ==
      PreparedActionDisposition::kRemoveWhilePreparingThenResumeFollow) {
    continuation_source = std::make_unique<FollowOwnerSource>(
        std::string(40, 'a'), std::string(40, 'b'), std::string(40, 'c'),
        HexString(std::string(40, 'd')), /*export_ready=*/true, "CONTINUE");
    ASSERT_NE(continuation_source->port(), 0);
    ASSERT_EQ(continuation_source->error(), 0)
        << std::strerror(continuation_source->error());
  }
  FailoverActionReconcileService service(
      &storage, &replication, /*expect_watchdog=*/false, disposition,
      std::move(fault_barriers), continuation_source.get());
  bycorf::Server server;
  server.AddService(&service);
  bycorf::ServerOptions runtime;
  runtime.thread_count_ = 1;
  runtime.pin_workers_ = false;
  runtime.recv_buffer_count_ = 0;
  ASSERT_TRUE(server.Start(runtime).ok());
  server.WaitUntilStopped();
  EXPECT_TRUE(service.result().ok()) << service.result();
}

TEST(ReplicationManagerIntegrationTest,
     FailoverActionWaitsForCommittedAuthorizationAndCleansUpByDesiredState) {
  RunFailoverActionDispositionCase(
      PreparedActionDisposition::kRetainForActivation,
      "cluster-failover-action-gate");
}

TEST(ReplicationManagerIntegrationTest,
     PreparedFailoverActionRemovalRetiresEveryChildReplicationLog) {
  RunFailoverActionDispositionCase(PreparedActionDisposition::kRemove,
                                   "cluster-failover-action-remove");
}

TEST(ReplicationManagerIntegrationTest,
     PreparedFailoverActionReplacementRetiresEveryChildReplicationLog) {
  RunFailoverActionDispositionCase(PreparedActionDisposition::kReplace,
                                   "cluster-failover-action-replace");
}

TEST(ReplicationManagerIntegrationTest,
     InFlightFailoverActionRemovalBeforeDurabilityPreservesPopulation) {
  RunFailoverActionDispositionCase(
      PreparedActionDisposition::kRemoveWhilePreparing,
      "cluster-failover-inflight-remove");
}

TEST(ReplicationManagerIntegrationTest,
     CancelledReplicaCandidateCanResumeFollowOwnerContinuation) {
  RunFailoverActionDispositionCase(
      PreparedActionDisposition::kRemoveWhilePreparingThenResumeFollow,
      "cluster-failover-inflight-cancel-follow");
}

TEST(ReplicationManagerIntegrationTest,
     InFlightFailoverActionReplacementBeforeDurabilityPreservesPopulation) {
  RunFailoverActionDispositionCase(
      PreparedActionDisposition::kReplaceWhilePreparing,
      "cluster-failover-inflight-replace");
}

TEST(ReplicationManagerIntegrationTest,
     InFlightFailoverActionRemovalAfterDurabilityJoinsAndRetiresChild) {
  RunFailoverActionDispositionCase(
      PreparedActionDisposition::kRemoveAfterDurabilityBoundary,
      "cluster-failover-inflight-post-durability-remove");
}

TEST(ReplicationManagerIntegrationTest,
     InFlightFailoverActionFailureAfterDurabilityRetiresChild) {
  RunFailoverActionDispositionCase(
      PreparedActionDisposition::
          kRemoveAfterDurabilityBoundaryWithPublicationFailure,
      "cluster-failover-inflight-post-durability-failure");
}

void RunFailoverActionWatchdogCase(std::string_view fault_variable,
                                   std::string_view fixture_name,
                                   bool seed_population = true) {
#if !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires a Debug/fault build for candidate seeding";
#endif
  const std::string fault_name(fault_variable);
  if (seed_population) {
    ASSERT_EQ(::setenv("LAVIK_REPLICATION_SEED_READY_PROMOTION_CANDIDATE",
                       "02020202020202020202020202020202", 1),
              0);
  }
  if (!fault_name.empty()) {
    ASSERT_EQ(
        ::setenv(fault_name.c_str(), "02020202020202020202020202020202", 1), 0);
  }
  ASSERT_EQ(::setenv("LAVIK_REPLICATION_ACTION_WATCHDOG_MS", "20", 1), 0);
  struct FaultReset {
    std::string fault_name_;
    bool seeded_;
    ~FaultReset() {
      if (seeded_) {
        (void)::unsetenv("LAVIK_REPLICATION_SEED_READY_PROMOTION_CANDIDATE");
      }
      if (!fault_name_.empty()) (void)::unsetenv(fault_name_.c_str());
      (void)::unsetenv("LAVIK_REPLICATION_ACTION_WATCHDOG_MS");
    }
  } fault_reset{fault_name, seed_population};

  lavik::test::TempDirectory directory{std::string(fixture_name)};
  const std::filesystem::path data = directory.path() / "node.data";
  lavik::test::CreateDataFile(data, 128 * kMiB);
  lavik::storage::StorageEngineOptions storage_options;
  storage_options.data_files_ = {data.string()};
  storage_options.expiration_authority_ = false;
  storage_options.buffers_.registered_bytes_ = 64 * kMiB;
  storage_options.replication_publish_queue_bytes_ = 16 * kMiB;
  lavik::storage::StorageEngine storage(std::move(storage_options));
  lavik::InitWorkerMetrics(1);
  ASSERT_TRUE(lavik::InitMemoryLimit(512 * kMiB, 1).ok());
  ASSERT_TRUE(storage.Prepare(1).ok());

  lavik::ReplicationOptions options;
  options.cluster_enabled_ = true;
  options.node_id_override_ = std::string(40, '9');
  lavik::ReplicationManager replication(&storage, std::move(options),
                                        std::nullopt);
  lavik::InitStorage(&storage, &replication);
  EnsureTxRuntime();

  FailoverActionReconcileService service(&storage, &replication,
                                         /*expect_watchdog=*/true);
  bycorf::Server server;
  server.AddService(&service);
  bycorf::ServerOptions runtime;
  runtime.thread_count_ = 1;
  runtime.pin_workers_ = false;
  runtime.recv_buffer_count_ = 0;
  ASSERT_TRUE(server.Start(runtime).ok());
  server.WaitUntilStopped();
  EXPECT_TRUE(service.result().ok()) << service.result();
}

TEST(ReplicationManagerIntegrationTest,
     TerminalFailoverActionFailureSuppressesSamePopulationAcrossReplacement) {
  RunFailoverActionWatchdogCase("LAVIK_REPLICATION_STALL_PROMOTION_ACTION",
                                "cluster-failover-action-watchdog");
}

TEST(ReplicationManagerIntegrationTest,
     FailoverActionWatchdogBoundsRetryablePromotionAdmission) {
  RunFailoverActionWatchdogCase(
      "LAVIK_REPLICATION_RETRY_FAILOVER_PROMOTION_ADMISSION",
      "cluster-failover-admission-watchdog");
}

TEST(ReplicationManagerIntegrationTest,
     AuthorizedFailoverActionWithoutReadyPopulationTimesOut) {
  RunFailoverActionWatchdogCase(
      /*fault_variable=*/{}, "cluster-failover-population-watchdog",
      /*seed_population=*/false);
}

TEST(ReplicationManagerIntegrationTest,
     UncontrolledActionPreparesFormerOwnerNativePopulationAfterFence) {
  lavik::test::TempDirectory directory("cluster-native-failover-action");
  const std::filesystem::path data = directory.path() / "node.data";
  lavik::test::CreateDataFile(data, 128 * kMiB);
  lavik::storage::StorageEngineOptions storage_options;
  storage_options.data_files_ = {data.string()};
  storage_options.expiration_authority_ = false;
  storage_options.buffers_.registered_bytes_ = 64 * kMiB;
  storage_options.replication_publish_queue_bytes_ = 16 * kMiB;
  lavik::storage::StorageEngine storage(std::move(storage_options));
  lavik::InitWorkerMetrics(1);
  ASSERT_TRUE(lavik::InitMemoryLimit(512 * kMiB, 1).ok());
  ASSERT_TRUE(storage.Prepare(1).ok());

  lavik::ReplicationOptions options;
  options.cluster_enabled_ = true;
  options.node_id_override_ = std::string(40, '9');
  lavik::ReplicationManager replication(&storage, std::move(options),
                                        std::nullopt);
  lavik::InitStorage(&storage, &replication);
  EnsureTxRuntime();

  NativeFailoverActionService service(&storage, &replication);
  bycorf::Server server;
  server.AddService(&service);
  bycorf::ServerOptions runtime;
  runtime.thread_count_ = 1;
  runtime.pin_workers_ = false;
  runtime.recv_buffer_count_ = 0;
  ASSERT_TRUE(server.Start(runtime).ok());
  server.WaitUntilStopped();
  EXPECT_TRUE(service.result().ok()) << service.result();
}

void RunInFlightSelfOriginActionCase(NativeActionDisposition disposition,
                                     std::string_view fixture_name) {
#if !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires a Debug/fault build for the prepare stall";
#endif
  lavik::test::TempDirectory directory(fixture_name);
  PromotionFaultBarrierPaths fault_barriers{
      .prepare_entered_ = directory.path() / "promotion-prepare-entered",
      .runner_waiting_ = directory.path() / "failover-runner-waiting",
      .runner_terminal_ = directory.path() / "failover-runner-terminal",
  };
  ASSERT_EQ(::setenv("LAVIK_REPLICATION_STALL_PROMOTION_ACTION",
                     "05050505050505050505050505050505", 1),
            0);
  ASSERT_EQ(::setenv("LAVIK_REPLICATION_PROMOTION_PRE_DURABILITY_BARRIER_ACK_"
                     "PATH",
                     fault_barriers.prepare_entered_.c_str(), 1),
            0);
  ASSERT_EQ(::setenv("LAVIK_REPLICATION_FAILOVER_RUNNER_WAITING_ACK_PATH",
                     fault_barriers.runner_waiting_.c_str(), 1),
            0);
  ASSERT_EQ(::setenv("LAVIK_REPLICATION_FAILOVER_RUNNER_TERMINAL_ACK_PATH",
                     fault_barriers.runner_terminal_.c_str(), 1),
            0);
  struct StallReset {
    ~StallReset() {
      (void)::unsetenv("LAVIK_REPLICATION_STALL_PROMOTION_ACTION");
      (void)::unsetenv(
          "LAVIK_REPLICATION_PROMOTION_PRE_DURABILITY_BARRIER_ACK_PATH");
      (void)::unsetenv("LAVIK_REPLICATION_FAILOVER_RUNNER_WAITING_ACK_PATH");
      (void)::unsetenv("LAVIK_REPLICATION_FAILOVER_RUNNER_TERMINAL_ACK_PATH");
    }
  } stall_reset;

  const std::filesystem::path data = directory.path() / "node.data";
  lavik::test::CreateDataFile(data, 128 * kMiB);
  lavik::storage::StorageEngineOptions storage_options;
  storage_options.data_files_ = {data.string()};
  storage_options.expiration_authority_ = false;
  storage_options.buffers_.registered_bytes_ = 64 * kMiB;
  storage_options.replication_publish_queue_bytes_ = 16 * kMiB;
  lavik::storage::StorageEngine storage(std::move(storage_options));
  lavik::InitWorkerMetrics(1);
  ASSERT_TRUE(lavik::InitMemoryLimit(512 * kMiB, 1).ok());
  ASSERT_TRUE(storage.Prepare(1).ok());

  lavik::ReplicationOptions options;
  options.cluster_enabled_ = true;
  options.node_id_override_ = std::string(40, '9');
  lavik::ReplicationManager replication(&storage, std::move(options),
                                        std::nullopt);
  lavik::InitStorage(&storage, &replication);
  EnsureTxRuntime();

  NativeFailoverActionService service(&storage, &replication, disposition,
                                      std::move(fault_barriers));
  bycorf::Server server;
  server.AddService(&service);
  bycorf::ServerOptions runtime;
  runtime.thread_count_ = 1;
  runtime.pin_workers_ = false;
  runtime.recv_buffer_count_ = 0;
  ASSERT_TRUE(server.Start(runtime).ok());
  server.WaitUntilStopped();
  EXPECT_TRUE(service.result().ok()) << service.result();
}

TEST(ReplicationManagerIntegrationTest,
     InFlightSelfOriginActionRemovalPreservesPopulationForSuccessor) {
  RunInFlightSelfOriginActionCase(
      NativeActionDisposition::kCancelWhilePreparing,
      "cluster-native-failover-action-cancel");
}

TEST(ReplicationManagerIntegrationTest,
     ShutdownCancelsSelfOriginActionBeforeDurability) {
  RunInFlightSelfOriginActionCase(
      NativeActionDisposition::kShutdownWhilePreparing,
      "cluster-native-failover-action-shutdown");
}

TEST(ReplicationManagerIntegrationTest,
     ControlledSourcePauseIsStableAndExactlyPaired) {
  lavik::test::TempDirectory directory("cluster-source-pause");
  const std::filesystem::path data = directory.path() / "node.data";
  lavik::test::CreateDataFile(data, 128 * kMiB);
  lavik::storage::StorageEngineOptions storage_options;
  storage_options.data_files_ = {data.string()};
  storage_options.expiration_authority_ = false;
  storage_options.buffers_.registered_bytes_ = 64 * kMiB;
  storage_options.replication_publish_queue_bytes_ = 16 * kMiB;
  lavik::storage::StorageEngine storage(std::move(storage_options));
  lavik::InitWorkerMetrics(1);
  ASSERT_TRUE(lavik::InitMemoryLimit(512 * kMiB, 1).ok());
  ASSERT_TRUE(storage.Prepare(1).ok());

  lavik::ReplicationOptions options;
  options.cluster_enabled_ = true;
  options.node_id_override_ = std::string(40, '9');
  lavik::ReplicationManager replication(&storage, std::move(options),
                                        std::nullopt);
  lavik::InitStorage(&storage, &replication);
  EnsureTxRuntime();

  ClusterSourcePauseService service(&storage, &replication);
  bycorf::Server server;
  server.AddService(&service);
  bycorf::ServerOptions runtime;
  runtime.thread_count_ = 1;
  runtime.pin_workers_ = false;
  runtime.recv_buffer_count_ = 0;
  ASSERT_TRUE(server.Start(runtime).ok());
  server.WaitUntilStopped();
  EXPECT_TRUE(service.result().ok()) << service.result();
}

TEST(ReplicationManagerIntegrationTest,
     FollowOwnerDestructiveFullRecordsAcceptedSecondFailureCandidateGap) {
  const std::string local_node_id(40, '9');
  constexpr std::uint16_t kReplicationPort = 6381;
  FollowOwnerSource unavailable(std::string(40, 'a'), std::string(40, 'c'),
                                std::string(40, 'd'), HexString("follow-group"),
                                /*export_ready=*/false);
  FollowOwnerSource replacement(std::string(40, 'b'), std::string(40, 'e'),
                                std::string(40, 'f'), HexString("follow-group"),
                                /*export_ready=*/true);
  ASSERT_NE(unavailable.port(), 0);
  ASSERT_NE(replacement.port(), 0);
  ASSERT_EQ(unavailable.error(), 0) << std::strerror(unavailable.error());
  ASSERT_EQ(replacement.error(), 0) << std::strerror(replacement.error());

  lavik::test::TempDirectory directory("cluster-follow-owner");
  const std::filesystem::path data = directory.path() / "node.data";
  lavik::test::CreateDataFile(data, 128 * kMiB);
  lavik::storage::StorageEngineOptions storage_options;
  storage_options.data_files_ = {data.string()};
  storage_options.expiration_authority_ = false;
  storage_options.buffers_.registered_bytes_ = 64 * kMiB;
  storage_options.replication_publish_queue_bytes_ = 16 * kMiB;
  lavik::storage::StorageEngine storage(std::move(storage_options));
  lavik::InitWorkerMetrics(1);
  ASSERT_TRUE(lavik::InitMemoryLimit(512 * kMiB, 1).ok());
  ASSERT_TRUE(storage.Prepare(1).ok());

  lavik::ReplicationOptions options;
  options.cluster_enabled_ = true;
  options.node_id_override_ = local_node_id;
  options.listen_port_ = kReplicationPort;
  lavik::ReplicationManager replication(&storage, std::move(options),
                                        std::nullopt);
  lavik::InitStorage(&storage, &replication);
  EnsureTxRuntime();

  FollowOwnerReconcileService service(&storage, &replication, &unavailable,
                                      &replacement);
  bycorf::Server server;
  server.AddService(&service);
  bycorf::ServerOptions runtime;
  runtime.thread_count_ = 1;
  runtime.pin_workers_ = false;
  runtime.recv_buffer_count_ = 0;
  ASSERT_TRUE(server.Start(runtime).ok());
  server.WaitUntilStopped();
  EXPECT_TRUE(service.result().ok()) << service.result();
  EXPECT_EQ(unavailable.error(), 0) << std::strerror(unavailable.error());
  EXPECT_EQ(replacement.error(), 0) << std::strerror(replacement.error());
}

TEST(ReplicationManagerIntegrationTest,
     FollowOwnerSourceAuthorizesMembersAndReusesNativeModes) {
  const std::string local_node_id(40, '9');
  lavik::test::TempDirectory directory("cluster-follow-owner-source");
  const std::filesystem::path data = directory.path() / "node.data";
  lavik::test::CreateDataFile(data, 128 * kMiB);
  lavik::storage::StorageEngineOptions storage_options;
  storage_options.data_files_ = {data.string()};
  storage_options.expiration_authority_ = false;
  storage_options.buffers_.registered_bytes_ = 64 * kMiB;
  storage_options.replication_publish_queue_bytes_ = 16 * kMiB;
  lavik::storage::StorageEngine storage(std::move(storage_options));
  lavik::InitWorkerMetrics(1);
  ASSERT_TRUE(lavik::InitMemoryLimit(512 * kMiB, 1).ok());
  ASSERT_TRUE(storage.Prepare(1).ok());

  lavik::ReplicationOptions options;
  options.cluster_enabled_ = true;
  options.node_id_override_ = local_node_id;
  lavik::ReplicationManager replication(&storage, std::move(options),
                                        std::nullopt);
  lavik::InitStorage(&storage, &replication);
  EnsureTxRuntime();

  FollowOwnerSourceAuthorizationService service(&storage, &replication);
  bycorf::Server server;
  server.AddService(&service);
  bycorf::ServerOptions runtime;
  runtime.thread_count_ = 1;
  runtime.pin_workers_ = false;
  runtime.recv_buffer_count_ = 0;
  ASSERT_TRUE(server.Start(runtime).ok());
  server.WaitUntilStopped();
  EXPECT_TRUE(service.result().ok()) << service.result();
}

TEST(ReplicationManagerIntegrationTest,
     PopulationSourceRevokeWinsAdmissionPublicationCrossing) {
#if !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires a Debug/fault build for the admission barrier";
#endif
  lavik::test::TempDirectory directory(
      "population-source-revoke-admission-crossing");
  const std::filesystem::path admission_entered =
      directory.path() / "admission-entered";
  const std::filesystem::path revocation_closed =
      directory.path() / "revocation-closed";
  ASSERT_EQ(::setenv("LAVIK_REPLICATION_SOURCE_ADMISSION_BARRIER_PATH",
                     admission_entered.c_str(), 1),
            0);
  ASSERT_EQ(::setenv("LAVIK_REPLICATION_SOURCE_REVOCATION_BARRIER_ACK_PATH",
                     revocation_closed.c_str(), 1),
            0);
  struct FaultReset {
    ~FaultReset() {
      (void)::unsetenv("LAVIK_REPLICATION_SOURCE_ADMISSION_BARRIER_PATH");
      (void)::unsetenv("LAVIK_REPLICATION_SOURCE_REVOCATION_BARRIER_ACK_PATH");
    }
  } fault_reset;

  const std::filesystem::path data = directory.path() / "node.data";
  lavik::test::CreateDataFile(data, 128 * kMiB);
  lavik::storage::StorageEngineOptions storage_options;
  storage_options.data_files_ = {data.string()};
  storage_options.expiration_authority_ = false;
  storage_options.buffers_.registered_bytes_ = 64 * kMiB;
  storage_options.replication_publish_queue_bytes_ = 16 * kMiB;
  lavik::storage::StorageEngine storage(std::move(storage_options));
  lavik::InitWorkerMetrics(1);
  ASSERT_TRUE(lavik::InitMemoryLimit(512 * kMiB, 1).ok());
  ASSERT_TRUE(storage.Prepare(1).ok());

  lavik::ReplicationOptions options;
  options.cluster_enabled_ = true;
  options.node_id_override_ = std::string(40, '9');
  lavik::ReplicationManager replication(&storage, std::move(options),
                                        std::nullopt);
  lavik::InitStorage(&storage, &replication);
  EnsureTxRuntime();

  FollowOwnerSourceAuthorizationService service(
      &storage, &replication, admission_entered, revocation_closed);
  bycorf::Server server;
  server.AddService(&service);
  bycorf::ServerOptions runtime;
  runtime.thread_count_ = 1;
  runtime.pin_workers_ = false;
  runtime.recv_buffer_count_ = 0;
  ASSERT_TRUE(server.Start(runtime).ok());
  server.WaitUntilStopped();
  EXPECT_TRUE(service.result().ok()) << service.result();
}

TEST_P(PromotionPrepareFailureIntegrationTest,
       UncertainStageFailStopsWithoutPublishingAuthority) {
  RunPromotionPrepareCase(GetParam());
}

INSTANTIATE_TEST_SUITE_P(PromotionPrepareBoundaries,
                         PromotionPrepareFailureIntegrationTest,
                         testing::Values("storage-barrier", "promotion-base",
                                         "child-history",
                                         "evidence-publication"));

}  // namespace
