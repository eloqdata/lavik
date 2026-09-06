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
using keylane::test::ChildProcess;
using keylane::test::Connect;
using keylane::test::CreateDataFile;
using keylane::test::PortReservation;
using keylane::test::RespClient;
using keylane::test::TempDirectory;
using keylane::test::WaitUntil;

std::string g_keylane_binary;

TEST(PopulationIntegrationTest,
     ClusterProcessStartsFailClosedAndRejectsStandaloneRoleControl) {
  ASSERT_FALSE(g_keylane_binary.empty());
  TempDirectory directory("cluster-population");
  const std::filesystem::path data = directory.path() / "node.data";
  const std::filesystem::path log = directory.path() / "node.log";
  CreateDataFile(data, 128ULL * 1024 * 1024);
  PortReservation reservation;
  const std::uint16_t port = reservation.ReleaseForSpawn();
  ChildProcess process(
      {g_keylane_binary, "--cluster-enabled", "--port", std::to_string(port),
       "--threads", "1", "--no-pin-workers", "--logtostderr",
       "--recv-buffers-per-worker", "0", "--data-file", data.string()},
      log);

  WaitUntil("cluster node startup", 20s, [&] {
    RespClient client = Connect(port, 200ms);
    return client.Command({"PING"}) == "+PONG";
  });
  RespClient client = Connect(port);
  EXPECT_EQ(client.Command({"GET", "unassigned"}),
            "-LOADING Keylane is loading the dataset from the primary");
  EXPECT_EQ(client.Command({"SET", "unassigned", "value"}),
            "-LOADING Keylane is loading the dataset from the primary");
  EXPECT_EQ(client.Command({"REPLICAOF", "NO", "ONE"}),
            "-ERR REPLICAOF is unavailable in cluster-managed mode");
  process.Stop(SIGINT);
}

}  // namespace

int main(int argc, char** argv) {
  if (argc != 2) {
    return 2;
  }
  g_keylane_binary = argv[1];
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
