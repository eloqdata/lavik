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

#include <sys/wait.h>

#include <chrono>
#include <csignal>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "tests/support/process.h"

namespace {
using namespace std::chrono_literals;
using lavik::test::ChildProcess;
using lavik::test::Connect;
using lavik::test::CreateDataFile;
using lavik::test::PortReservation;
using lavik::test::RespClient;
using lavik::test::TempDirectory;
using lavik::test::WaitUntil;

std::string g_lavik_binary;

TEST(PopulationIntegrationTest, ManagedStartupWaitsForMetaWithoutOpeningRedis) {
  ASSERT_FALSE(g_lavik_binary.empty());
  TempDirectory directory("cluster-population");
  const std::filesystem::path data = directory.path() / "node.data";
  const std::filesystem::path log = directory.path() / "node.log";
  CreateDataFile(data, 128ULL * 1024 * 1024);
  PortReservation reservation;
  const std::uint16_t port = reservation.ReleaseForSpawn();
  ChildProcess process(
      {g_lavik_binary, "--port", std::to_string(port), "--node-id",
       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "--meta-seed", "127.0.0.1:1",
       "--threads", "1", "--no-pin-workers", "--logtostderr",
       "--recv-buffers-per-worker", "0", "--data-file", data.string()},
      log);

  WaitUntil("bootstrap retries unavailable Meta", 20s, [&] {
    return lavik::test::ReadFile(log).find("waiting for Meta bootstrap:") !=
           std::string::npos;
  });
  EXPECT_THROW(Connect(port, 200ms), std::runtime_error);
  ASSERT_EQ(::kill(process.pid(), SIGINT), 0);
  const int status = process.Wait(2s);
  EXPECT_TRUE(WIFEXITED(status));
  EXPECT_EQ(WEXITSTATUS(status), 0);
}

}  // namespace

int main(int argc, char** argv) {
  if (argc != 2) {
    return 2;
  }
  g_lavik_binary = argv[1];
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
