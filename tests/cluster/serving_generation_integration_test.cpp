#include <chrono>
#include <csignal>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <future>
#include <string>
#include <string_view>
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

TEST(ServingGenerationIntegrationTest,
     BlockedCommandCannotConsumeAReplacementDataset) {
  ASSERT_FALSE(g_keylane_binary.empty());
  TempDirectory directory("serving-generation");
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
  const auto server_arguments = [](std::uint16_t port,
                                   const std::filesystem::path& data) {
    return std::vector<std::string>{
        g_keylane_binary,
        "--port",
        std::to_string(port),
        "--threads",
        "1",
        "--no-pin-workers",
        "--logtostderr",
        "--recv-buffers-per-worker",
        "1024",
        "--data-file",
        data.string(),
    };
  };
  ChildProcess source(server_arguments(source_port, source_data), source_log);
  ChildProcess target(server_arguments(target_port, target_data), target_log);

  WaitUntil("source startup", 20s, [&] {
    RespClient client = Connect(source_port, 200ms);
    return client.Command({"PING"}) == "+PONG";
  });
  WaitUntil("target startup", 20s, [&] {
    RespClient client = Connect(target_port, 200ms);
    return client.Command({"PING"}) == "+PONG";
  });

  RespClient source_client = Connect(source_port);
  ASSERT_EQ(source_client.Command({"RPUSH", "generation-fence", "value"}),
            ":1");

  std::promise<std::uint64_t> waiter_id_promise;
  std::future<std::uint64_t> waiter_id = waiter_id_promise.get_future();
  std::future<std::string> waiter = std::async(std::launch::async, [&] {
    try {
      RespClient client = Connect(target_port);
      const std::string id_reply = client.Command({"CLIENT", "ID"});
      waiter_id_promise.set_value(std::stoull(id_reply.substr(1)));
      return client.Command({"BLPOP", "generation-fence", "8"});
    } catch (...) {
      // Avoid hanging the controlling thread if setup fails before publishing
      // the waiter id. set_exception itself fails after set_value, in which
      // case the waiter future still carries the original exception.
      try {
        waiter_id_promise.set_exception(std::current_exception());
      } catch (const std::future_error&) {
      }
      throw;
    }
  });
  const std::uint64_t blocked_client_id = waiter_id.get();

  RespClient target_client = Connect(target_port);
  WaitUntil("BLPOP waiter admission", 10s, [&] {
    const std::string reply = target_client.Command(
        {"CLIENT", "LIST", "ID", std::to_string(blocked_client_id)});
    return reply.find(" flags=b ") != std::string::npos;
  });

  // FUNCTION has no key view, but it still owns the selected database's
  // serving gate. Keep a keyless-only EXEC admitted in the old generation so
  // this test also proves that transaction path cannot cross replacement.
  RespClient transaction_client = Connect(target_port);
  ASSERT_EQ(transaction_client.Command({"MULTI"}), "+OK");
  ASSERT_EQ(transaction_client.Command({"FUNCTION", "LIST"}), "+QUEUED");

  ASSERT_EQ(target_client.Command(
                {"REPLICAOF", "127.0.0.1", std::to_string(source_port)}),
            "+OK");
  ASSERT_EQ(waiter.wait_for(15s), std::future_status::ready);
  const std::string waiter_reply = waiter.get();
  EXPECT_TRUE(waiter_reply.starts_with("-TRYAGAIN ") ||
              waiter_reply.starts_with("-LOADING "))
      << waiter_reply;

  WaitUntil("target full sync", 20s, [&] {
    const std::string reply = target_client.Command({"INFO", "REPLICATION"});
    return reply.find("keylane_replication_state:online") != std::string::npos;
  });
  const std::string transaction_reply = transaction_client.Command({"EXEC"});
  EXPECT_TRUE(transaction_reply.starts_with("-TRYAGAIN ") ||
              transaction_reply.starts_with("-LOADING "))
      << transaction_reply;
  ASSERT_EQ(target_client.Command({"READONLY"}), "+OK");
  EXPECT_EQ(target_client.Command({"LRANGE", "generation-fence", "0", "-1"}),
            "*1\r\n$5\r\nvalue");

  target.Stop(SIGINT);
  source.Stop(SIGINT);
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
