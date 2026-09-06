#include <chrono>
#include <csignal>
#include <cstdint>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "tests/support/process.h"

namespace {
using namespace std::chrono_literals;
using keylane::test::ChildProcess;
using keylane::test::Connect;
using keylane::test::CreateDataFile;
using keylane::test::PortReservation;
using keylane::test::ReadFile;
using keylane::test::RespClient;
using keylane::test::TempDirectory;
using keylane::test::WaitUntil;

std::string g_keylane_binary;

std::vector<std::string> ServerArguments(std::uint16_t port,
                                         const std::filesystem::path& data) {
  return {g_keylane_binary,
          "--port",
          std::to_string(port),
          "--threads",
          "1",
          "--no-pin-workers",
          "--logtostderr",
          "--recv-buffers-per-worker",
          "0",
          "--data-file",
          data.string()};
}

TEST(RebuildFailureIntegrationTest,
     PromotionUncertaintyStopsTheWholeAttemptWithoutRetrying) {
  ASSERT_FALSE(g_keylane_binary.empty());
  TempDirectory directory("cluster-rebuild-failure");
  const std::filesystem::path source_data = directory.path() / "source.data";
  const std::filesystem::path target_data = directory.path() / "target.data";
  const std::filesystem::path source_log = directory.path() / "source.log";
  const std::filesystem::path target_log = directory.path() / "target.log";
  CreateDataFile(source_data, 128ULL * 1024 * 1024);
  CreateDataFile(target_data, 128ULL * 1024 * 1024);

  PortReservation source_reservation;
  PortReservation target_reservation;
  const std::uint16_t source_port = source_reservation.ReleaseForSpawn();
  const std::uint16_t target_port = target_reservation.ReleaseForSpawn();
  ChildProcess source(ServerArguments(source_port, source_data), source_log);
  ChildProcess target(
      ServerArguments(target_port, target_data), target_log,
      {{"KEYLANE_REPLICATION_FAIL_PROMOTE_ONCE", "1"},
       {"KEYLANE_REPLICATION_PAUSE_AFTER_FAIL_STOP_MS", "1000"}});

  WaitUntil("source startup", 20s, [&] {
    RespClient client = Connect(source_port, 200ms);
    return client.Command({"PING"}) == "+PONG";
  });
  WaitUntil("target startup", 20s, [&] {
    RespClient client = Connect(target_port, 200ms);
    return client.Command({"PING"}) == "+PONG";
  });

  RespClient source_client = Connect(source_port);
  RespClient target_client = Connect(target_port);
  ASSERT_EQ(source_client.Command({"SET", "population{failure}", "complete"}),
            "+OK");
  ASSERT_EQ(target_client.Command(
                {"REPLICAOF", "127.0.0.1", std::to_string(source_port)}),
            "+OK");

  // Exercise the ownership handoff window directly: promotion has already
  // made the storage outcome uncertain, but the background coordinator has
  // not yet copied the session-local reason into the node-level latch.
  WaitUntil("session-local promotion failure", 30s, [&] {
    return ReadFile(target_log)
               .find(
                   "replica session entered fail-stop before coordinator "
                   "latch") != std::string::npos;
  });
  EXPECT_TRUE(
      target_client.Command({"REPLICAOF", "NO", "ONE"}).starts_with("-ERR"));

  std::string info;
  WaitUntil("promotion failure terminal state", 30s, [&] {
    info = target_client.Command({"INFO", "replication"});
    return info.find("keylane_replication_failed_stopped:1") !=
               std::string::npos ||
           info.find("keylane_replication_state:online") != std::string::npos;
  });
  ASSERT_NE(info.find("keylane_replication_failed_stopped:1"),
            std::string::npos)
      << info;
  EXPECT_NE(info.find("keylane_replication_state:connecting"),
            std::string::npos);
  EXPECT_EQ(info.find("keylane_replication_state:online"), std::string::npos);
  EXPECT_TRUE(target_client.Command({"GET", "population{failure}"})
                  .starts_with("-LOADING"));

  // FAILED_STOPPED is current-boot terminal. It must neither reconnect in the
  // background nor let standalone role control turn uncertain storage into a
  // writable primary.
  std::this_thread::sleep_for(1500ms);
  info = target_client.Command({"INFO", "replication"});
  EXPECT_NE(info.find("keylane_replication_failed_stopped:1"),
            std::string::npos);
  EXPECT_EQ(info.find("keylane_replication_state:online"), std::string::npos);
  EXPECT_TRUE(
      target_client.Command({"REPLICAOF", "NO", "ONE"}).starts_with("-ERR"));

  target.Stop(SIGINT);
  source.Stop(SIGINT);
}

}  // namespace

int main(int argc, char** argv) {
  if (argc != 2) return 2;
  g_keylane_binary = argv[1];
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
