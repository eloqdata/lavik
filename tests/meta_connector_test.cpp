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

#include "lavik/cluster/meta_connector.h"

#include <arpa/inet.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <functional>
#include <future>
#include <memory>
#include <vector>

#include "bycorf/io/storage.h"
#include "bycorf/runtime/runtime.h"
#include "bycorf/runtime/worker.h"
#include "gtest/gtest.h"

namespace {
using namespace std::chrono_literals;
using lavik::cluster::MetaControlEndpoint;
using lavik::cluster::detail::ConnectMetaEndpoint;

struct Listener {
  int fd_ = -1;
  int filler_ = -1;
  std::uint16_t port_ = 0;
  ~Listener() {
    if (filler_ >= 0) ::close(filler_);
    if (fd_ >= 0) ::close(fd_);
  }
  bool Open(bool blackhole = false) {
    fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (fd_ < 0 ||
        ::bind(fd_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) !=
            0 ||
        ::listen(fd_, blackhole ? 0 : 16) != 0)
      return false;
    socklen_t length = sizeof(address);
    if (::getsockname(fd_, reinterpret_cast<sockaddr*>(&address), &length) != 0)
      return false;
    port_ = ntohs(address.sin_port);
    if (blackhole) {
      // Fill the one queue slot Linux permits for backlog zero. Further SYNs
      // time out without depending on host routes or privileged firewall rules.
      filler_ = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
      if (filler_ < 0 ||
          ::connect(filler_, reinterpret_cast<sockaddr*>(&address),
                    sizeof(address)) != 0)
        return false;
    }
    return true;
  }
  MetaControlEndpoint Endpoint() const { return {"127.0.0.1", port_}; }
};

using Scenario = std::function<bycorf::Task<absl::Status>(bycorf::Worker&)>;
bycorf::Task<absl::Status> Complete(Scenario* scenario, bycorf::Worker& worker,
                                    std::promise<absl::Status>* finished) {
  finished->set_value(co_await (*scenario)(worker));
  co_return absl::OkStatus();
}
absl::Status RunScenario(Scenario scenario) {
  bycorf::Runtime runtime;
  std::promise<absl::Status> ready, finished;
  auto initialized = ready.get_future();
  auto done = finished.get_future();
  runtime.Start(
      1,
      [&](unsigned, bycorf::Worker& worker) {
        bycorf::WorkerOptions options;
        options.recv_buffer_count_ = 0;
        const auto status = worker.Init(options);
        ready.set_value(status);
        if (!status.ok()) return 1;
        worker.Spawn(Complete(&scenario, worker, &finished));
        worker.Run();
        worker.Shutdown();
        worker.DestroyDetachedTasks();
        return 0;
      },
      false);
  auto status = initialized.get();
  if (status.ok()) {
    if (done.wait_for(15s) == std::future_status::ready)
      status = done.get();
    else
      status = absl::DeadlineExceededError("Meta dial scenario timed out");
  }
  runtime.RequestStop();
  runtime.WaitUntilStopped();
  return status;
}

TEST(MetaConnector, TwoBlackholesDoNotDelayReachableThirdEndpoint) {
  Listener first, second, live;
  ASSERT_TRUE(first.Open(true));
  ASSERT_TRUE(second.Open(true));
  ASSERT_TRUE(live.Open());
  std::vector endpoints{first.Endpoint(), second.Endpoint(), live.Endpoint()};
  std::atomic<bool> stopping{false};
  EXPECT_TRUE(
      RunScenario([&](bycorf::Worker& worker) -> bycorf::Task<absl::Status> {
        bool attempted = false;
        const auto started = std::chrono::steady_clock::now();
        auto connected = co_await ConnectMetaEndpoint(worker, endpoints,
                                                      stopping, &attempted);
        if (!connected.ok()) co_return connected.status();
        EXPECT_EQ((*connected)->index_, 2);
        EXPECT_TRUE(attempted);
        EXPECT_LT(std::chrono::steady_clock::now() - started, 2s);
        // The returned connection remains usable after the losing attempts
        // join.
        const std::string text = "selected";
        auto sent = co_await (*connected)
                        ->stream_.WriteAll(std::as_bytes(std::span(text)));
        if (!sent.ok()) co_return sent;
        const int peer =
            ::accept4(live.fd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
        EXPECT_GE(peer, 0);
        if (peer >= 0) {
          char bytes[16]{};
          const auto count = ::recv(peer, bytes, sizeof(bytes), MSG_DONTWAIT);
          EXPECT_EQ(count, text.size());
          if (count > 0) EXPECT_EQ(std::string_view(bytes, count), text);
          ::close(peer);
        }
        co_return absl::OkStatus();
      }).ok());
}

TEST(MetaConnector, FastPreferredEndpointDoesNotDialFallback) {
  Listener preferred, fallback;
  ASSERT_TRUE(preferred.Open());
  ASSERT_TRUE(fallback.Open());
  std::vector endpoints{preferred.Endpoint(), fallback.Endpoint()};
  std::atomic<bool> stopping{false};
  EXPECT_TRUE(
      RunScenario([&](bycorf::Worker& worker) -> bycorf::Task<absl::Status> {
        auto connected =
            co_await ConnectMetaEndpoint(worker, endpoints, stopping, nullptr);
        if (!connected.ok()) co_return connected.status();
        EXPECT_EQ((*connected)->index_, 0);
        // Inspect readiness without accepting: fallback must have no
        // queued peer.
        pollfd descriptor{fallback.fd_, POLLIN, 0};
        EXPECT_EQ(::poll(&descriptor, 1, 0), 0);
        co_return absl::OkStatus();
      }).ok());
}

TEST(MetaConnector, ImmediateFailuresReuseSlotsAndPreserveEndpointIdentity) {
  Listener live;
  ASSERT_TRUE(live.Open());
  std::vector<MetaControlEndpoint> endpoints(5, {"127.0.0.1", 0});
  auto endpoint = live.Endpoint();
  endpoint.server_id_ = 17;
  endpoint.principal_ = "lavik://meta/17";
  endpoints.push_back(endpoint);
  std::atomic<bool> stopping{false};
  EXPECT_TRUE(
      RunScenario([&](bycorf::Worker& worker) -> bycorf::Task<absl::Status> {
        const auto started = std::chrono::steady_clock::now();
        auto connected =
            co_await ConnectMetaEndpoint(worker, endpoints, stopping, nullptr);
        if (!connected.ok()) co_return connected.status();
        EXPECT_EQ((*connected)->index_, 5);
        EXPECT_EQ(endpoints[(*connected)->index_], endpoint);
        EXPECT_LT(std::chrono::steady_clock::now() - started, 2s);
        co_return absl::OkStatus();
      }).ok());
}

bycorf::Task<absl::Status> StopAfter(bycorf::Worker& worker,
                                     std::atomic<bool>& stopping) {
  (void)co_await bycorf::SleepFor(worker, 250ms);
  stopping.store(true, std::memory_order_release);
  co_return absl::OkStatus();
}

TEST(MetaConnector, StopCancelsAndJoinsPendingConnections) {
  Listener first, second;
  ASSERT_TRUE(first.Open(true));
  ASSERT_TRUE(second.Open(true));
  std::vector endpoints{first.Endpoint(), second.Endpoint()};
  std::atomic<bool> stopping{false};
  EXPECT_TRUE(
      RunScenario([&](bycorf::Worker& worker) -> bycorf::Task<absl::Status> {
        worker.Spawn(StopAfter(worker, stopping));
        const auto started = std::chrono::steady_clock::now();
        auto connected =
            co_await ConnectMetaEndpoint(worker, endpoints, stopping, nullptr);
        EXPECT_FALSE(connected.ok());
        if (!connected.ok()) EXPECT_TRUE(absl::IsCancelled(connected.status()));
        EXPECT_LT(std::chrono::steady_clock::now() - started, 2s);
        co_return absl::OkStatus();
      }).ok());
}

TEST(MetaConnector, EmptyAndAllRefusedEndpointsReturnFailure) {
  std::atomic<bool> stopping{false};
  EXPECT_TRUE(
      RunScenario([&](bycorf::Worker& worker) -> bycorf::Task<absl::Status> {
        for (const auto endpoints : {std::vector<MetaControlEndpoint>{},
                                     std::vector<MetaControlEndpoint>{
                                         {"127.0.0.1", 0}, {"127.0.0.1", 0}}}) {
          auto connected = co_await ConnectMetaEndpoint(worker, endpoints,
                                                        stopping, nullptr);
          EXPECT_FALSE(connected.ok());
          if (!connected.ok())
            EXPECT_TRUE(absl::IsUnavailable(connected.status()));
        }
        co_return absl::OkStatus();
      }).ok());
}
TEST(MetaConnector, FollowerRetryKeepsASlotAndHonorsBackoff) {
  Listener first, second, third, follower;
  ASSERT_TRUE(first.Open(true));
  ASSERT_TRUE(second.Open(true));
  ASSERT_TRUE(third.Open(true));
  ASSERT_TRUE(follower.Open());
  const std::vector endpoints{first.Endpoint(), second.Endpoint(),
                              third.Endpoint(), follower.Endpoint()};
  std::atomic<bool> stopping{false};
  EXPECT_TRUE(
      RunScenario([&](bycorf::Worker& worker) -> bycorf::Task<absl::Status> {
        lavik::cluster::detail::MetaConnectSchedule schedule;
        schedule.Refresh(endpoints);
        const auto retry_at = std::chrono::steady_clock::now() + 350ms;
        schedule.SessionEnded(3, true, retry_at);
        schedule.Refresh(endpoints);
        auto connected = co_await schedule.Connect(worker, stopping, nullptr);
        if (!connected.ok()) co_return connected.status();
        EXPECT_EQ((*connected)->index_, 3);
        EXPECT_GE(std::chrono::steady_clock::now(), retry_at);
        EXPECT_LT(std::chrono::steady_clock::now(), retry_at + 2s);
        co_return absl::OkStatus();
      }).ok());
}

TEST(MetaConnector, FollowerWinsDoNotStarveCandidatesBeyondDeadPrefix) {
  std::array<Listener, 5> dead;
  Listener follower, leader;
  std::vector<MetaControlEndpoint> endpoints;
  for (auto& listener : dead) {
    ASSERT_TRUE(listener.Open(true));
    endpoints.push_back(listener.Endpoint());
  }
  ASSERT_TRUE(follower.Open());
  ASSERT_TRUE(leader.Open());
  endpoints.push_back(follower.Endpoint());
  endpoints.push_back(leader.Endpoint());
  std::atomic<bool> stopping{false};
  EXPECT_TRUE(
      RunScenario([&](bycorf::Worker& worker) -> bycorf::Task<absl::Status> {
        lavik::cluster::detail::MetaConnectSchedule schedule;
        schedule.Refresh(endpoints);
        schedule.SessionEnded(5, true,
                              std::chrono::steady_clock::now() + 120ms);
        const auto started = std::chrono::steady_clock::now();
        for (int round = 0; round < 4; ++round) {
          // A fresh authenticated Hello may repeat the directory verbatim. This
          // must not reset progress through attempts cancelled by the fast
          // peer.
          schedule.Refresh(endpoints);
          auto connected = co_await schedule.Connect(worker, stopping, nullptr);
          if (!connected.ok()) co_return connected.status();
          EXPECT_LT(std::chrono::steady_clock::now() - started, 2s);
          if ((*connected)->index_ == 6) co_return absl::OkStatus();
          EXPECT_EQ((*connected)->index_, 5);
          schedule.SessionEnded((*connected)->index_, true,
                                std::chrono::steady_clock::now() + 120ms);
          const int peer = ::accept4(follower.fd_, nullptr, nullptr,
                                     SOCK_NONBLOCK | SOCK_CLOEXEC);
          EXPECT_GE(peer, 0);
          if (peer >= 0) ::close(peer);
        }
        co_return absl::InternalError("follower starved the later leader");
      }).ok());
}

TEST(MetaConnector, ChangedLeaderHintOverridesOldDialOrder) {
  Listener first, second;
  ASSERT_TRUE(first.Open());
  ASSERT_TRUE(second.Open());
  std::atomic<bool> stopping{false};
  EXPECT_TRUE(
      RunScenario([&](bycorf::Worker& worker) -> bycorf::Task<absl::Status> {
        lavik::cluster::detail::MetaConnectSchedule schedule;
        std::vector endpoints{first.Endpoint(), second.Endpoint()};
        schedule.Refresh(endpoints);
        {
          auto connected = co_await schedule.Connect(worker, stopping, nullptr);
          if (!connected.ok()) co_return connected.status();
          EXPECT_EQ(schedule.Endpoint((*connected)->index_), first.Endpoint());
        }
        schedule.Refresh(endpoints);
        {
          auto connected = co_await schedule.Connect(worker, stopping, nullptr);
          if (!connected.ok()) co_return connected.status();
          EXPECT_EQ(schedule.Endpoint((*connected)->index_), second.Endpoint());
        }
        // A new hint names the most recently tried endpoint. Without its
        // preference, the least-recently attempted endpoint would win.
        std::reverse(endpoints.begin(), endpoints.end());
        schedule.Refresh(endpoints);
        auto connected = co_await schedule.Connect(worker, stopping, nullptr);
        if (!connected.ok()) co_return connected.status();
        EXPECT_EQ(schedule.Endpoint((*connected)->index_), second.Endpoint());
        co_return absl::OkStatus();
      }).ok());
}

TEST(MetaConnector, StopDuringDeferredFollowerRetryCompletesPromptly) {
  Listener dead, follower;
  ASSERT_TRUE(dead.Open(true));
  ASSERT_TRUE(follower.Open());
  std::vector endpoints{dead.Endpoint(), follower.Endpoint()};
  std::atomic<bool> stopping{false};
  EXPECT_TRUE(
      RunScenario([&](bycorf::Worker& worker) -> bycorf::Task<absl::Status> {
        lavik::cluster::detail::MetaConnectSchedule schedule;
        schedule.Refresh(endpoints);
        schedule.SessionEnded(1, true, std::chrono::steady_clock::now() + 5s);
        worker.Spawn(StopAfter(worker, stopping));
        const auto started = std::chrono::steady_clock::now();
        auto connected = co_await schedule.Connect(worker, stopping, nullptr);
        EXPECT_FALSE(connected.ok());
        if (!connected.ok()) EXPECT_TRUE(absl::IsCancelled(connected.status()));
        EXPECT_LT(std::chrono::steady_clock::now() - started, 2s);
        co_return absl::OkStatus();
      }).ok());
}

TEST(MetaConnector, EndpointReuseDoesNotInheritPriorIdentityBackoff) {
  Listener live;
  ASSERT_TRUE(live.Open());
  std::atomic<bool> stopping{false};
  EXPECT_TRUE(
      RunScenario([&](bycorf::Worker& worker) -> bycorf::Task<absl::Status> {
        lavik::cluster::detail::MetaConnectSchedule schedule;
        auto endpoint = live.Endpoint();
        endpoint.server_id_ = 7;
        endpoint.principal_ = "lavik://meta/7";
        schedule.Refresh(std::span(&endpoint, 1));
        schedule.SessionEnded(0, true, std::chrono::steady_clock::now() + 5s);
        endpoint.server_id_ = 8;
        endpoint.principal_ = "lavik://meta/8";
        schedule.Refresh(std::span(&endpoint, 1));
        const auto started = std::chrono::steady_clock::now();
        auto connected = co_await schedule.Connect(worker, stopping, nullptr);
        if (!connected.ok()) co_return connected.status();
        EXPECT_EQ(schedule.Endpoint((*connected)->index_), endpoint);
        EXPECT_LT(std::chrono::steady_clock::now() - started, 2s);
        co_return absl::OkStatus();
      }).ok());
}

}  // namespace
