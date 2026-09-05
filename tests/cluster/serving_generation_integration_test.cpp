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

std::vector<std::string> ServerArguments(std::uint16_t port,
                                         const std::filesystem::path& data) {
  return {
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
}

void WaitForStartup(std::string_view label, std::uint16_t port) {
  WaitUntil(label, 20s, [port] {
    RespClient client = Connect(port, 200ms);
    return client.Command({"PING"}) == "+PONG";
  });
}

TEST(ServingGenerationIntegrationTest,
     AdmittedCommandsCannotConsumeAReplacementDataset) {
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
  ChildProcess source(ServerArguments(source_port, source_data), source_log);
  ChildProcess target(
      ServerArguments(target_port, target_data), target_log,
      {{"KEYLANE_COMMAND_PAUSE_BEFORE_DB_ADMISSION_MS", "5000"}});

  WaitForStartup("source startup", source_port);
  WaitForStartup("target startup", target_port);

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

  // KEYS owns its database gate instead of using the ordinary dispatch path.
  // Suspend it after old-population admission but before gate acquisition so
  // the full sync can replace the dataset first; it must reject that stale
  // admission before committing an array header for the new population.
  std::future<std::string> stale_keys = std::async(std::launch::async, [&] {
    RespClient client = Connect(target_port);
    return client.Command({"KEYS", "*"});
  });
  WaitUntil("KEYS pre-gate pause", 10s, [&] {
    return keylane::test::ReadFile(target_log)
               .find(
                   "client command admitted; pausing before database "
                   "admission") != std::string::npos;
  });

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
  ASSERT_EQ(stale_keys.wait_for(15s), std::future_status::ready);
  const std::string keys_reply = stale_keys.get();
  EXPECT_TRUE(keys_reply.starts_with("-TRYAGAIN ") ||
              keys_reply.starts_with("-LOADING "))
      << keys_reply;
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

TEST(ServingGenerationIntegrationTest, WatchIsBoundToItsServingGeneration) {
  ASSERT_FALSE(g_keylane_binary.empty());
  TempDirectory directory("watch-serving-generation");
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
      {{"KEYLANE_COMMAND_PAUSE_BEFORE_DB_ADMISSION_MS", "5000"}});

  WaitForStartup("WATCH source startup", source_port);
  WaitForStartup("WATCH target startup", target_port);

  RespClient source_client = Connect(source_port);
  ASSERT_EQ(source_client.Command({"SET", "generation-watch", "replacement"}),
            "+OK");

  // Pause after dispatch captured the old generation but before WATCH takes
  // database admission. The reset must win first; when WATCH resumes it must
  // reject that stale admission rather than registering after reset's
  // MarkAllWatched pass and remaining clean against the replacement.
  RespClient watcher = Connect(target_port);
  std::future<std::string> stale_watch = std::async(std::launch::async, [&] {
    return watcher.Command({"WATCH", "generation-watch"});
  });
  WaitUntil("WATCH pre-gate pause", 10s, [&] {
    return keylane::test::ReadFile(target_log)
               .find(
                   "client command admitted; pausing before database "
                   "admission") != std::string::npos;
  });

  RespClient target_client = Connect(target_port);
  ASSERT_EQ(target_client.Command(
                {"REPLICAOF", "127.0.0.1", std::to_string(source_port)}),
            "+OK");
  ASSERT_EQ(stale_watch.wait_for(10s), std::future_status::ready);
  const std::string watch_reply = stale_watch.get();
  EXPECT_TRUE(watch_reply.starts_with("-TRYAGAIN ") ||
              watch_reply.starts_with("-LOADING "))
      << watch_reply;

  WaitUntil("WATCH target full sync", 20s, [&] {
    const std::string reply = target_client.Command({"INFO", "REPLICATION"});
    return reply.find("keylane_replication_state:online") != std::string::npos;
  });
  ASSERT_EQ(watcher.Command({"READONLY"}), "+OK");
  EXPECT_EQ(watcher.Command({"GET", "generation-watch"}), "$11\r\nreplacement");

  // A completed WATCH also belongs to the generation in which it observed
  // the key. Promotion changes the serving generation without another
  // population reset, so this proves EXEC uses the retained token rather than
  // depending only on reset's MarkAllWatched side effect.
  ASSERT_EQ(source_client.Command({"SET", "promotion-proof", "ready"}), "+OK");
  WaitUntil("WATCH promotion proof apply", 10s, [&] {
    return watcher.Command({"GET", "promotion-proof"}) == "$5\r\nready";
  });
  ASSERT_EQ(watcher.Command({"WATCH", "generation-watch"}), "+OK");
  ASSERT_EQ(target_client.Command({"REPLICAOF", "NO", "ONE"}), "+OK");
  ASSERT_EQ(watcher.Command({"READWRITE"}), "+OK");
  ASSERT_EQ(watcher.Command({"MULTI"}), "+OK");
  ASSERT_EQ(watcher.Command({"GET", "generation-watch"}), "+QUEUED");
  EXPECT_EQ(watcher.Command({"EXEC"}), "*-1");

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
