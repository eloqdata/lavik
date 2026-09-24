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
#include <sys/resource.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <string_view>

#include "bycorf/runtime/runtime.h"
#include "bycorf/runtime/worker.h"
#include "gtest/gtest.h"
#include "lavik/meta/automatic_failover_detector.h"
#include "lavik/meta/data_control_runtime_status.h"
#include "lavik/meta/raft.h"
#include "lavik/meta/sentinel_server.h"
#include "lavik/meta/state_machine.h"
#include "lavik/password_authenticator.h"
#include "support/meta_raft.h"
#include "support/test_data_path.h"

namespace {

TEST(PasswordAuthenticatorTest, IndependentPasswordsAndBinaryInput) {
  const lavik::PasswordAuthenticator data("data-secret");
  const lavik::PasswordAuthenticator sentinel(std::string("s\0ecret", 7));
  EXPECT_TRUE(data.required());
  EXPECT_TRUE(data.Authenticate("default", "data-secret"));
  EXPECT_FALSE(sentinel.Authenticate("default", "data-secret"));
  EXPECT_TRUE(sentinel.Authenticate("default", std::string("s\0ecret", 7)));
  EXPECT_FALSE(sentinel.Authenticate("default", "s"));
  EXPECT_FALSE(sentinel.Authenticate("other", std::string("s\0ecret", 7)));
  const lavik::PasswordAuthenticator no_password("");
  EXPECT_FALSE(no_password.required());
  EXPECT_TRUE(no_password.Authenticate("default", "anything"));
  EXPECT_FALSE(no_password.Authenticate("other", "anything"));
}

// Exercise the public server options through an actual connection, using a
// smaller output ceiling than the input ceiling to make output rejection
// observable without changing production CLI defaults. Discovery needs real
// authority inputs, so the fixture carries a single-node Raft stack; the
// non-discovery tests never wait for its election because none of them issues
// a discovery verb.
class SentinelRuntime {
 public:
  // never_leader extends the Raft bootstrap with two unreachable voters on
  // privileged ports (nothing can answer there), so the node campaigns
  // forever without quorum: a deterministic non-leader, instead of a
  // single-voter fixture that wins its election within ~150 ms.
  explicit SentinelRuntime(bool never_leader = false) {
    const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
    dir_ = lavik::test::TestDataDirectory() /
           ("lavik_sentinel_server_" + std::string(info->name()) + "_" +
            std::to_string(::getpid()));
    std::error_code ignored;
    std::filesystem::remove_all(dir_, ignored);
    runtime_.Start(
        1,
        [this](unsigned, bycorf::Worker& worker) {
          const auto status = worker.Init(bycorf::WorkerOptions{});
          initialized_.set_value(status);
          if (!status.ok()) return 1;
          worker.Run();
          worker.Shutdown();
          worker.DestroyDetachedTasks();
          return 0;
        },
        false);
    auto machine = lavik::meta::MetaStateMachine::Open(dir_);
    if (!machine.ok()) {
      machine_status_ = machine.status();
      return;
    }
    machine_ =
        std::shared_ptr<lavik::meta::MetaStateMachine>(std::move(*machine));
    auto raft_options = lavik::test::SingleMetaOptions(dir_);
    if (never_leader) {
      for (std::int32_t id = 2; id <= 3; ++id) {
        const lavik::meta::MetaMemberIdentity identity{
            id, "lavik://meta/" + std::to_string(id), "127.0.0.1:1",
            "127.0.0.1:2"};
        raft_options.initial_.push_back(
            std::make_shared<lavik::meta::MetaRaftMember>(
                id, 0, "127.0.0.1:" + std::to_string(id),
                identity.EncodeAux()));
      }
    }
    auto raft = lavik::meta::MetaRaft::Open(std::move(raft_options), *machine_);
    if (!raft.ok()) {
      machine_status_ = raft.status();
      return;
    }
    raft_ = std::move(*raft);
    runtime_status_ =
        std::make_shared<lavik::meta::MetaDataControlRuntimeStatus>();
    diagnostics_ = std::make_shared<
        lavik::meta::MetaAutomaticFailoverDiagnosticsRegistry>();
  }
  ~SentinelRuntime() {
    if (server_) server_->Shutdown();
    server_.reset();
    if (raft_) {
      raft_->shutdown();
      raft_.reset();
    }
    machine_.reset();
    runtime_.GetForeignExecutor(0).WaitUntilIdle();
    runtime_.RequestStop();
    runtime_.WaitUntilStopped();
    std::error_code ignored;
    std::filesystem::remove_all(dir_, ignored);
  }

  lavik::meta::MetaSentinelDiscoveryDependencies DiscoveryDependencies() {
    lavik::meta::MetaSentinelDiscoveryDependencies dependencies;
    dependencies.raft_ = raft_;
    dependencies.state_machine_ = machine_.get();
    dependencies.runtime_status_ = runtime_status_;
    dependencies.diagnostics_ = diagnostics_;
    dependencies.observation_ttl_ms_ = 500;
    dependencies.leader_observation_grace_ms_ = 5000;
    return dependencies;
  }

  std::filesystem::path dir_;
  absl::Status machine_status_;
  std::promise<absl::Status> initialized_;
  bycorf::Runtime runtime_;
  std::shared_ptr<lavik::meta::MetaStateMachine> machine_;
  std::shared_ptr<lavik::meta::MetaRaft> raft_;
  std::shared_ptr<lavik::meta::MetaDataControlRuntimeStatus> runtime_status_;
  std::shared_ptr<lavik::meta::MetaAutomaticFailoverDiagnosticsRegistry>
      diagnostics_;
  std::shared_ptr<lavik::meta::MetaSentinelServer> server_;
};

TEST(MetaSentinelServerTest,
     OutputLimitClosesConnectionAndShutdownIsIdempotent) {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  ASSERT_GE(fd, 0);
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  ASSERT_EQ(::bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)),
            0);
  socklen_t size = sizeof(address);
  ASSERT_EQ(::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &size), 0);
  ::close(fd);
  SentinelRuntime runtime;
  ASSERT_TRUE(runtime.initialized_.get_future().get().ok());
  ASSERT_TRUE(runtime.machine_status_.ok()) << runtime.machine_status_;
  lavik::meta::MetaSentinelServerOptions options;
  options.address_ = "127.0.0.1:" + std::to_string(ntohs(address.sin_port));
  options.reply_limit_ = 128;
  auto created = lavik::meta::MetaSentinelServer::Create(
      runtime.runtime_.GetForeignExecutor(0), runtime.DiscoveryDependencies(),
      std::move(options));
  ASSERT_TRUE(created.ok()) << created.status();
  runtime.server_ = *created;
  ASSERT_TRUE(runtime.server_->Start().ok());
  const int client = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  ASSERT_GE(client, 0);
  const timeval timeout{.tv_sec = 3, .tv_usec = 0};
  ASSERT_EQ(
      ::setsockopt(client, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)),
      0);
  ASSERT_EQ(
      ::connect(client, reinterpret_cast<sockaddr*>(&address), sizeof(address)),
      0);
  const std::string request =
      "*2\r\n$4\r\nPING\r\n$256\r\n" + std::string(256, 'x') + "\r\n";
  ASSERT_EQ(::send(client, request.data(), request.size(), MSG_NOSIGNAL),
            request.size());
  std::string response;
  char buffer[512];
  ssize_t received;
  while ((received = ::recv(client, buffer, sizeof(buffer), 0)) > 0)
    response.append(buffer, received);
  EXPECT_EQ(received, 0);
  EXPECT_EQ(response, "-ERR Sentinel reply limit exceeded\r\n");
  ::close(client);
  runtime.server_->Shutdown();
  runtime.server_->Shutdown();
}

TEST(MetaSentinelServerTest, ShutdownDrainsAcceptWhenWakeSocketCannotBeOpened) {
  // Isolate the descriptor limit and timeout from other tests and the Go
  // runtime's background threads. The exec-based child starts its own worker.
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  ASSERT_EXIT(
      {
        // The child now also boots the single-node Raft fixture before the
        // descriptor-limit dance; keep headroom for loaded CI hosts.
        ::alarm(30);
        {
          const int client = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
          ASSERT_GE(client, 0);
          sockaddr_in address{};
          address.sin_family = AF_INET;
          address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
          ASSERT_EQ(::bind(client, reinterpret_cast<sockaddr*>(&address),
                           sizeof(address)),
                    0);
          socklen_t size = sizeof(address);
          ASSERT_EQ(::getsockname(client, reinterpret_cast<sockaddr*>(&address),
                                  &size),
                    0);
          ::close(client);

          SentinelRuntime runtime;
          ASSERT_TRUE(runtime.initialized_.get_future().get().ok());
          ASSERT_TRUE(runtime.machine_status_.ok());
          lavik::meta::MetaSentinelServerOptions options;
          options.address_ =
              "127.0.0.1:" + std::to_string(ntohs(address.sin_port));
          auto created = lavik::meta::MetaSentinelServer::Create(
              runtime.runtime_.GetForeignExecutor(0),
              runtime.DiscoveryDependencies(), std::move(options));
          ASSERT_TRUE(created.ok());
          runtime.server_ = std::move(*created);
          ASSERT_TRUE(runtime.server_->Start().ok());

          const int peer = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
          ASSERT_GE(peer, 0);
          ASSERT_EQ(::connect(peer, reinterpret_cast<sockaddr*>(&address),
                              sizeof(address)),
                    0);
          ASSERT_EQ(::send(peer, "PING\r\n", 6, MSG_NOSIGNAL), 6);
          char reply[7];
          ASSERT_EQ(::recv(peer, reply, sizeof(reply), MSG_WAITALL),
                    sizeof(reply));
          ASSERT_EQ(std::string(reply, sizeof(reply)), "+PONG\r\n");

          // The test targets the Sentinel accept loop. Stop its unused Raft
          // fixture before exhausting descriptors so an election/WAL open
          // cannot fail-stop the child while Shutdown is under test.
          runtime.raft_->shutdown();

          rlimit previous{};
          ASSERT_EQ(::getrlimit(RLIMIT_NOFILE, &previous), 0);
          rlimit exhausted = previous;
          exhausted.rlim_cur = 0;
          ASSERT_EQ(::setrlimit(RLIMIT_NOFILE, &exhausted), 0);
          ASSERT_EQ(::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0), -1);
          ASSERT_EQ(errno, EMFILE);
          // The listener is accepting again and the established session is
          // idle. Shutdown must drain both without creating another socket.
          runtime.server_->Shutdown();
          ASSERT_EQ(::setrlimit(RLIMIT_NOFILE, &previous), 0);
          ASSERT_EQ(::recv(peer, reply, sizeof(reply), 0), 0);
          ::close(peer);
          runtime.server_->Shutdown();
          runtime.server_.reset();
        }
        ::_exit(0);
      },
      ::testing::ExitedWithCode(0), "");
}

bool WaitFor(const std::function<bool()>& predicate,
             std::chrono::milliseconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (predicate()) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  return predicate();
}

// Reads exactly one complete reply of the expected size, or "" on EOF/timeout.
std::string ReadReply(int client, std::size_t size) {
  std::string reply(size, '\0');
  std::size_t received = 0;
  while (received < size) {
    const ssize_t chunk =
        ::recv(client, reply.data() + received, size - received, 0);
    if (chunk <= 0) return std::string();
    received += static_cast<std::size_t>(chunk);
  }
  return reply;
}

// Binds an ephemeral loopback port, releases it, and starts the Sentinel
// server of this fixture there.
void StartServerOnEphemeralPort(SentinelRuntime& runtime,
                                sockaddr_in* address) {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  ASSERT_GE(fd, 0);
  address->sin_family = AF_INET;
  address->sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  ASSERT_EQ(::bind(fd, reinterpret_cast<sockaddr*>(address), sizeof(*address)),
            0);
  socklen_t size = sizeof(*address);
  ASSERT_EQ(::getsockname(fd, reinterpret_cast<sockaddr*>(address), &size), 0);
  ::close(fd);
  lavik::meta::MetaSentinelServerOptions options;
  options.address_ = "127.0.0.1:" + std::to_string(ntohs(address->sin_port));
  auto created = lavik::meta::MetaSentinelServer::Create(
      runtime.runtime_.GetForeignExecutor(0), runtime.DiscoveryDependencies(),
      std::move(options));
  ASSERT_TRUE(created.ok()) << created.status();
  runtime.server_ = *created;
  ASSERT_TRUE(runtime.server_->Start().ok());
}

// The six discovery verbs are leader-only: a node that is not a caught-up
// leader closes the connection without a reply so client seed lists rotate,
// while management verbs keep their explicit error on any node. On the
// leader, an uninitialized cluster answers from committed state: empty
// MASTERS, null address, and the "No such master" error for named verbs.
TEST(MetaSentinelServerTest, DiscoveryVerbsRequireCaughtUpLeader) {
  const auto connect = [](const sockaddr_in& address) {
    const int client = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    EXPECT_GE(client, 0);
    const timeval timeout{.tv_sec = 3, .tv_usec = 0};
    EXPECT_EQ(::setsockopt(client, SOL_SOCKET, SO_RCVTIMEO, &timeout,
                           sizeof(timeout)),
              0);
    EXPECT_EQ(::connect(client, reinterpret_cast<const sockaddr*>(&address),
                        sizeof(address)),
              0);
    return client;
  };
  const auto send = [](int client, std::string_view request) {
    ASSERT_EQ(::send(client, request.data(), request.size(), MSG_NOSIGNAL),
              request.size());
  };
  const auto expect_reply = [&](int client, std::string_view expected) {
    EXPECT_EQ(ReadReply(client, expected.size()), expected);
  };

  {  // Phase 1: a quorumless fixture can never win an election, so the drop
     // behavior is deterministic rather than racing the election timeout.
    SentinelRuntime runtime(/*never_leader=*/true);
    ASSERT_TRUE(runtime.initialized_.get_future().get().ok());
    ASSERT_TRUE(runtime.machine_status_.ok()) << runtime.machine_status_;
    sockaddr_in address{};
    StartServerOnEphemeralPort(runtime, &address);
    EXPECT_FALSE(runtime.raft_->is_leader());
    for (std::string_view request :
         {"*3\r\n$8\r\nSENTINEL\r\n$23\r\nGET-MASTER-ADDR-BY-NAME\r\n$"
          "2\r\ng1\r\n",
          "*3\r\n$8\r\nSENTINEL\r\n$6\r\nMASTER\r\n$2\r\ng1\r\n",
          "*2\r\n$8\r\nSENTINEL\r\n$7\r\nMASTERS\r\n",
          "*3\r\n$8\r\nSENTINEL\r\n$8\r\nREPLICAS\r\n$2\r\ng1\r\n",
          "*3\r\n$8\r\nSENTINEL\r\n$6\r\nSLAVES\r\n$2\r\ng1\r\n"}) {
      const int client = connect(address);
      send(client, request);
      char buffer[1];
      EXPECT_EQ(::recv(client, buffer, sizeof(buffer), 0), 0) << request;
      ::close(client);
    }
    // A management verb answers its explicit refusal without dropping, and a
    // malformed discovery verb keeps its deterministic arity error.
    const int client = connect(address);
    send(client, "*3\r\n$8\r\nSENTINEL\r\n$7\r\nMONITOR\r\n$1\r\nx\r\n");
    expect_reply(client, "-ERR SENTINEL subcommand is not supported\r\n");
    send(client, "*3\r\n$8\r\nSENTINEL\r\n$7\r\nMASTERS\r\n$1\r\nx\r\n");
    expect_reply(client,
                 "-ERR wrong number of arguments for 'sentinel|masters' "
                 "command\r\n");
    send(client, "*1\r\n$4\r\nPING\r\n");
    expect_reply(client, "+PONG\r\n");
    ::close(client);
    runtime.server_->Shutdown();
  }

  {  // Phase 2: the single-voter fixture elects itself; poll the leader
     // triplet to a caught-up state before expecting authoritative answers.
    SentinelRuntime runtime;
    ASSERT_TRUE(runtime.initialized_.get_future().get().ok());
    ASSERT_TRUE(runtime.machine_status_.ok()) << runtime.machine_status_;
    sockaddr_in address{};
    StartServerOnEphemeralPort(runtime, &address);
    ASSERT_TRUE(
        WaitFor([&] { return runtime.raft_->is_leader_sm_fully_caught_up(); },
                std::chrono::seconds(15)));
    const int leader = connect(address);
    send(leader, "*2\r\n$8\r\nSENTINEL\r\n$7\r\nMASTERS\r\n");
    expect_reply(leader, "*0\r\n");
    send(leader,
         "*3\r\n$8\r\nSENTINEL\r\n$23\r\nGET-MASTER-ADDR-BY-NAME\r\n$"
         "2\r\ng1\r\n");
    expect_reply(leader, "*-1\r\n");
    send(leader, "*3\r\n$8\r\nSENTINEL\r\n$6\r\nMASTER\r\n$2\r\ng1\r\n");
    expect_reply(leader, "-ERR No such master with that name\r\n");
    send(leader, "*3\r\n$8\r\nSENTINEL\r\n$6\r\nSLAVES\r\n$2\r\ng1\r\n");
    expect_reply(leader, "-ERR No such master with that name\r\n");
    // The session survived every discovery answer.
    send(leader, "*1\r\n$4\r\nPING\r\n");
    expect_reply(leader, "+PONG\r\n");
    ::close(leader);
    runtime.server_->Shutdown();
  }
}

TEST(MetaSentinelServerTest, ResetCannotEraseAuthorityAcrossEligibilityABA) {
  SentinelRuntime runtime;
  ASSERT_TRUE(runtime.initialized_.get_future().get().ok());
  ASSERT_TRUE(runtime.machine_status_.ok());
  sockaddr_in address{};
  StartServerOnEphemeralPort(runtime, &address);
  ASSERT_TRUE(
      WaitFor([&] { return runtime.raft_->is_leader_sm_fully_caught_up(); },
              std::chrono::seconds(15)));
  runtime.runtime_status_->BeginLeadership(1);
  ASSERT_TRUE(runtime.runtime_status_->SetLeaderAuthorityEligible(1, true));
  const int client = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  ASSERT_GE(client, 0);
  const timeval timeout{.tv_sec = 3, .tv_usec = 0};
  ASSERT_EQ(
      ::setsockopt(client, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)),
      0);
  ASSERT_EQ(
      ::connect(client, reinterpret_cast<sockaddr*>(&address), sizeof(address)),
      0);
  // RESET may be parsed while the discovery response is still queued. Both
  // queued and already-written replies must retain the TCP authority lifetime.
  const std::string pipeline = "SENTINEL MASTERS\r\nRESET\r\n";
  ASSERT_EQ(::send(client, pipeline.data(), pipeline.size(), MSG_NOSIGNAL),
            pipeline.size());
  EXPECT_EQ(ReadReply(client, 12), "*0\r\n+RESET\r\n");
  // Make both eligibility edges inside one worker turn: polling only the
  // final boolean would miss revocation, but its revision must still close us.
  std::promise<void> changed;
  ASSERT_TRUE(runtime.runtime_.GetForeignExecutor(0).Notify([&]() noexcept {
    runtime.runtime_status_->SetLeaderAuthorityEligible(1, false);
    runtime.runtime_status_->SetLeaderAuthorityEligible(1, true);
    changed.set_value();
  }));
  changed.get_future().get();
  char byte;
  EXPECT_EQ(::recv(client, &byte, 1, 0), 0);
  ::close(client);
  runtime.server_->Shutdown();
}

}  // namespace
