/* Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0 */
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <string>

#include "gtest/gtest.h"
#include "lavik/meta/metrics.h"

namespace lavik::meta {
namespace {
int Connect(std::uint16_t port) {
  const int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = htons(port);
  if (connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) !=
      0) {
    close(fd);
    return -1;
  }
  const timeval timeout{4, 0};
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
  return fd;
}

TEST(MetaMetrics, RecoveryWithoutRuntimeAndShutdownWithPartialHeader) {
  int reservation = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  ASSERT_GE(reservation, 0);
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  ASSERT_EQ(
      bind(reservation, reinterpret_cast<sockaddr*>(&address), sizeof(address)),
      0);
  socklen_t size = sizeof(address);
  ASSERT_EQ(
      getsockname(reservation, reinterpret_cast<sockaddr*>(&address), &size),
      0);
  const auto port = ntohs(address.sin_port);
  close(reservation);
  auto started = MetaMetricsServer::Start("127.0.0.1", port);
  ASSERT_TRUE(started.ok()) << started.status();
  auto server = std::move(*started);
  server->SetPhase(MetaMetricsServer::Phase::kRecovering);
  int fd = Connect(port);
  ASSERT_GE(fd, 0);
  const std::string request =
      "GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n";
  ASSERT_EQ(send(fd, request.data(), request.size(), MSG_NOSIGNAL),
            request.size());
  std::string response;
  char chunk[4096];
  for (;;) {
    const auto received = recv(fd, chunk, sizeof(chunk), 0);
    ASSERT_GE(received, 0);
    if (!received) break;
    response.append(chunk, received);
  }
  close(fd);
  EXPECT_TRUE(response.starts_with("HTTP/1.1 200"));
  EXPECT_NE(response.find("lavik_meta_phase{phase=\"recovering\"} 1\n"),
            std::string::npos);
  EXPECT_NE(response.find("lavik_meta_source_available{source=\"raft\"} 0\n"),
            std::string::npos);
  EXPECT_EQ(response.find("lavik_meta_raft_term "), std::string::npos);
  EXPECT_EQ(response.find("etcd_snap_db_"), std::string::npos);
  fd = Connect(port);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(send(fd, "GET /", 5, MSG_NOSIGNAL), 5);
  const auto before = std::chrono::steady_clock::now();
  server->Stop();
  EXPECT_LT(std::chrono::steady_clock::now() - before, std::chrono::seconds(1));
  // Unread partial input may be reset instead of receiving orderly EOF.
  EXPECT_LE(recv(fd, chunk, sizeof(chunk), 0), 0);
  close(fd);
  EXPECT_EQ(Connect(port), -1);
  server->Stop();  // idempotent destruction barrier
}
}  // namespace
}  // namespace lavik::meta
