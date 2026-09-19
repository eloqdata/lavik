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
#include <sys/socket.h>
#include <unistd.h>

#include <future>
#include <memory>
#include <string>

#include "bycorf/runtime/runtime.h"
#include "bycorf/runtime/worker.h"
#include "gtest/gtest.h"
#include "lavik/meta/sentinel_server.h"
#include "lavik/password_authenticator.h"

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
// observable without changing production CLI defaults.
class SentinelRuntime {
 public:
  SentinelRuntime() {
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
  }
  ~SentinelRuntime() {
    if (server_) server_->Shutdown();
    runtime_.GetForeignExecutor(0).WaitUntilIdle();
    runtime_.RequestStop();
    runtime_.WaitUntilStopped();
  }
  std::promise<absl::Status> initialized_;
  bycorf::Runtime runtime_;
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
  lavik::meta::MetaSentinelServerOptions options;
  options.address_ = "127.0.0.1:" + std::to_string(ntohs(address.sin_port));
  options.reply_limit_ = 128;
  auto created = lavik::meta::MetaSentinelServer::Create(
      runtime.runtime_.GetForeignExecutor(0), std::move(options));
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

}  // namespace
