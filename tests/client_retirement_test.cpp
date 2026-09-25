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

#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <future>
#include <thread>

#include "absl/cleanup/cleanup.h"
#include "bycorf/runtime/foreign_executor.h"
#include "bycorf/runtime/runtime.h"
#include "bycorf/runtime/worker.h"
#include "gtest/gtest.h"
#include "lavik/command.h"
#include "lavik/session.h"

namespace lavik {
namespace {
using namespace std::chrono_literals;

TEST(ClientRetirementTest,
     DelayedNotificationClosesReconnectsButPreservesInternalConnections) {
  std::array<std::array<int, 2>, 6> sockets;
  for (auto& pair : sockets)
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, pair.data()), 0);
  absl::Cleanup close_sockets = [&] {
    for (auto& pair : sockets)
      for (int fd : pair) close(fd);
  };
  SetServerInfo("127.0.0.1", 0, 1, "");
  bycorf::Runtime runtime;
  std::array<std::promise<absl::Status>, 2> initialized;
  std::atomic<bool> release{false};
  std::promise<void> registered, replaced, retired, accepted, cleaned;
  ConnectionContext old, reconnect, fresh;
  runtime.Start(
      2,
      [&](unsigned id, bycorf::Worker& worker) {
        auto status = worker.Init();
        initialized[id].set_value(status);
        if (!status.ok()) return 1;
        worker.Run();
        worker.Shutdown();
        worker.DestroyDetachedTasks();
        return 0;
      },
      false);
  absl::Cleanup stop = [&] {
    release.store(true, std::memory_order_release);
    runtime.RequestStop();
    runtime.WaitUntilStopped();
  };
  for (auto& ready : initialized) ASSERT_TRUE(ready.get_future().get().ok());
  auto data = runtime.GetForeignExecutor(0);
  auto control = runtime.GetForeignExecutor(1);
  ASSERT_TRUE(data.Notify([&]() noexcept {
    RegisterClientConnection(1, sockets[0][0], "normal", false, false, 0, &old);
    RegisterClientConnection(2, sockets[1][0], "reused", false);
    RegisterClientConnection(3, sockets[2][0], "replica", false, true);
    RegisterClientConnection(4, sockets[3][0], "donor", false, false, 44);
    registered.set_value();
    // Deliberately hold this test worker so the control notification cannot
    // run until a new connection has reused an old connection's exact fd.
    while (!release.load(std::memory_order_acquire)) std::this_thread::yield();
    UnregisterClientConnection(2);
    EXPECT_EQ(dup2(sockets[4][0], sockets[1][0]), sockets[1][0]);
    RegisterClientConnection(5, sockets[1][0], "reconnect", false, false, 0,
                             &reconnect);
    replaced.set_value();
  }));
  ASSERT_EQ(registered.get_future().wait_for(5s), std::future_status::ready);
  ASSERT_TRUE(control.Notify([&]() noexcept {
    RetireClientConnections();
    retired.set_value();
  }));
  ASSERT_EQ(retired.get_future().wait_for(5s), std::future_status::ready);
  release.store(true, std::memory_order_release);
  ASSERT_EQ(replaced.get_future().wait_for(5s), std::future_status::ready);
  pollfd closed{.fd = sockets[0][1], .events = POLLIN, .revents = 0};
  ASSERT_EQ(poll(&closed, 1, 5000), 1);
  char byte;
  EXPECT_EQ(recv(sockets[0][1], &byte, 1, 0), 0);
  closed.fd = sockets[4][1];
  ASSERT_EQ(poll(&closed, 1, 5000), 1);
  EXPECT_EQ(recv(sockets[4][1], &byte, 1, 0), 0);
  for (unsigned index : {2u, 3u}) {
    EXPECT_EQ(recv(sockets[index][1], &byte, 1, MSG_DONTWAIT), -1);
    EXPECT_EQ(errno, EAGAIN);
  }
  // The sweep closes the reconnect too. A connection registered after the
  // sweep remains usable until another transition requests cleanup.
  ASSERT_TRUE(data.Notify([&]() noexcept {
    EXPECT_TRUE(old.closing_);
    EXPECT_TRUE(reconnect.closing_);
    RegisterClientConnection(6, sockets[5][0], "after-sweep", false, false, 0,
                             &fresh);
    EXPECT_FALSE(fresh.closing_);
    accepted.set_value();
  }));
  ASSERT_EQ(accepted.get_future().wait_for(5s), std::future_status::ready);
  EXPECT_EQ(recv(sockets[5][1], &byte, 1, MSG_DONTWAIT), -1);
  EXPECT_EQ(errno, EAGAIN);
  ASSERT_TRUE(control.Notify([]() noexcept { RetireClientConnections(); }));
  closed.fd = sockets[5][1];
  ASSERT_EQ(poll(&closed, 1, 5000), 1);
  EXPECT_EQ(recv(sockets[5][1], &byte, 1, 0), 0);
  ASSERT_TRUE(data.Notify([&]() noexcept {
    EXPECT_TRUE(fresh.closing_);
    for (unsigned id : {1u, 3u, 4u, 5u, 6u}) UnregisterClientConnection(id);
    cleaned.set_value();
  }));
  ASSERT_EQ(cleaned.get_future().wait_for(5s), std::future_status::ready);
}

}  // namespace
}  // namespace lavik
