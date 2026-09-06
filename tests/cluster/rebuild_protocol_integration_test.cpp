#include <chrono>
#include <csignal>
#include <cstdint>
#include <filesystem>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
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
                                         const std::filesystem::path& data,
                                         unsigned threads = 1) {
  return {g_keylane_binary,
          "--port",
          std::to_string(port),
          "--threads",
          std::to_string(threads),
          "--no-pin-workers",
          "--logtostderr",
          "--recv-buffers-per-worker",
          "0",
          "--data-file",
          data.string()};
}

void WaitForStartup(std::uint16_t port, std::string_view label) {
  WaitUntil(label, 20s, [=] {
    RespClient client = Connect(port, 200ms);
    return client.Command({"PING"}) == "+PONG";
  });
}

std::size_t CountOccurrences(std::string_view text, std::string_view needle) {
  std::size_t count = 0;
  for (std::size_t position = 0;
       (position = text.find(needle, position)) != std::string_view::npos;
       position += needle.size()) {
    ++count;
  }
  return count;
}

TEST(RebuildProtocolIntegrationTest,
     SourceCannotPublishOnlineBeforeTargetFlowProof) {
  ASSERT_FALSE(g_keylane_binary.empty());
  TempDirectory directory("cluster-early-online");
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
  ChildProcess source(ServerArguments(source_port, source_data), source_log,
                      {{"KEYLANE_REPLICATION_EARLY_ONLINE", "1"}});
  ChildProcess target(ServerArguments(target_port, target_data), target_log);
  WaitForStartup(source_port, "early-online source startup");
  WaitForStartup(target_port, "early-online target startup");

  RespClient source_client = Connect(source_port);
  RespClient target_client = Connect(target_port);
  ASSERT_EQ(source_client.Command({"SET", "online-proof", "complete"}), "+OK");
  ASSERT_EQ(target_client.Command(
                {"REPLICAOF", "127.0.0.1", std::to_string(source_port)}),
            "+OK");

  // The injected source keeps the control connection open after its premature
  // marker. A target that trusts that marker remains ONLINE; a target that
  // proves local flow state closes it and retries while staying fail-closed.
  for (int sample = 0; sample < 20; ++sample) {
    const std::string info = target_client.Command({"INFO", "replication"});
    ASSERT_EQ(info.find("keylane_replication_state:online"), std::string::npos)
        << info;
    std::this_thread::sleep_for(100ms);
  }
  EXPECT_TRUE(
      target_client.Command({"GET", "online-proof"}).starts_with("-LOADING"));

  target.Stop(SIGINT);
  source.Stop(SIGINT);
}

TEST(RebuildProtocolIntegrationTest,
     PostCutResetCannotDetachThePromotedPopulation) {
  ASSERT_FALSE(g_keylane_binary.empty());
  TempDirectory directory("cluster-post-cut-reset");
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
  ChildProcess source(ServerArguments(source_port, source_data), source_log,
                      {{"KEYLANE_REPLICATION_POST_CUT_RESET_ONCE", "1"}});
  ChildProcess target(ServerArguments(target_port, target_data), target_log);
  WaitForStartup(source_port, "post-cut source startup");
  WaitForStartup(target_port, "post-cut target startup");

  RespClient source_client = Connect(source_port);
  RespClient target_client = Connect(target_port);
  ASSERT_EQ(source_client.Command({"SET", "promoted-proof", "preserved"}),
            "+OK");
  ASSERT_EQ(target_client.Command(
                {"REPLICAOF", "127.0.0.1", std::to_string(source_port)}),
            "+OK");

  WaitUntil("post-cut reset injection", 30s, [&] {
    return ReadFile(source_log).find("injected post-cut reset") !=
           std::string::npos;
  });
  WaitUntil("post-cut reconnect", 30s, [&] {
    const std::string info = target_client.Command({"INFO", "replication"});
    return info.find("keylane_replication_state:online") != std::string::npos;
  });
  ASSERT_EQ(target_client.Command({"READONLY"}), "+OK");
  EXPECT_EQ(target_client.Command({"GET", "promoted-proof"}),
            "$9\r\npreserved");

  target.Stop(SIGINT);
  source.Stop(SIGINT);
}

TEST(RebuildProtocolIntegrationTest,
     DivergentOnlineTailInvalidatesEveryContinuationCursor) {
  ASSERT_FALSE(g_keylane_binary.empty());
  TempDirectory directory("cluster-divergent-tail");
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
  ChildProcess source(ServerArguments(source_port, source_data), source_log,
                      {{"KEYLANE_REPLICATION_DIVERGENT_TAIL_ONCE", "1"}});
  ChildProcess target(ServerArguments(target_port, target_data), target_log);
  WaitForStartup(source_port, "divergent-tail source startup");
  WaitForStartup(target_port, "divergent-tail target startup");

  RespClient source_client = Connect(source_port);
  RespClient target_client = Connect(target_port);
  ASSERT_EQ(source_client.Command({"SET", "population-before-gap", "ready"}),
            "+OK");
  ASSERT_EQ(target_client.Command(
                {"REPLICAOF", "127.0.0.1", std::to_string(source_port)}),
            "+OK");
  WaitUntil("initial full rebuild", 30s, [&] {
    const std::string info = target_client.Command({"INFO", "replication"});
    return info.find("keylane_replication_state:online") != std::string::npos;
  });

  ASSERT_EQ(source_client.Command({"SET", "population-after-gap", "replayed"}),
            "+OK");
  WaitUntil("divergent tail injection", 30s, [&] {
    return ReadFile(source_log).find("injected divergent replication tail") !=
           std::string::npos;
  });
  WaitUntil("whole-population retry", 45s, [&] {
    return CountOccurrences(ReadFile(source_log), "selected=FULL") >= 2;
  });
  WaitUntil("replacement population online", 30s, [&] {
    const std::string info = target_client.Command({"INFO", "replication"});
    return info.find("keylane_replication_state:online") != std::string::npos;
  });
  ASSERT_EQ(target_client.Command({"READONLY"}), "+OK");
  EXPECT_EQ(target_client.Command({"GET", "population-after-gap"}),
            "$8\r\nreplayed");

  target.Stop(SIGINT);
  source.Stop(SIGINT);
}

TEST(RebuildProtocolIntegrationTest,
     CorruptFullSyncFrameRequiresAFreshFullSync) {
  ASSERT_FALSE(g_keylane_binary.empty());
  TempDirectory directory("cluster-full-sync-checksum");
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
  ChildProcess source(
      ServerArguments(source_port, source_data), source_log,
      {{"KEYLANE_REPLICATION_CORRUPT_FULLSYNC_RECORD_FRAME_ONCE", "1"}});
  ChildProcess target(ServerArguments(target_port, target_data), target_log);
  WaitForStartup(source_port, "checksum source startup");
  WaitForStartup(target_port, "checksum target startup");

  RespClient source_client = Connect(source_port);
  RespClient target_client = Connect(target_port);
  ASSERT_EQ(source_client.Command({"SET", "checksum-proof", "preserved"}),
            "+OK");
  ASSERT_EQ(target_client.Command(
                {"REPLICAOF", "127.0.0.1", std::to_string(source_port)}),
            "+OK");

  WaitUntil("wire checksum rejection", 30s, [&] {
    return ReadFile(target_log).find("replication frame CRC32C mismatch") !=
           std::string::npos;
  });
  WaitUntil("fresh full sync after checksum rejection", 45s, [&] {
    return CountOccurrences(ReadFile(source_log), "selected=FULL") >= 2;
  });
  WaitUntil("checksum replacement population online", 30s, [&] {
    const std::string info = target_client.Command({"INFO", "replication"});
    return info.find("keylane_replication_state:online") != std::string::npos;
  });
  ASSERT_EQ(target_client.Command({"READONLY"}), "+OK");
  EXPECT_EQ(target_client.Command({"GET", "checksum-proof"}),
            "$9\r\npreserved");

  target.Stop(SIGINT);
  source.Stop(SIGINT);
}

TEST(RebuildProtocolIntegrationTest,
     TargetCrashDiscardsPartialAndBootScopedReadinessProof) {
  ASSERT_FALSE(g_keylane_binary.empty());
  TempDirectory directory("cluster-target-crash");
  const std::filesystem::path source_data = directory.path() / "source.data";
  const std::filesystem::path target_data = directory.path() / "target.data";
  const std::filesystem::path source_log = directory.path() / "source.log";
  const std::filesystem::path target_log = directory.path() / "target.log";
  CreateDataFile(source_data, 256ULL * 1024 * 1024);
  CreateDataFile(target_data, 256ULL * 1024 * 1024);

  PortReservation source_reservation;
  PortReservation target_reservation;
  const std::uint16_t source_port = source_reservation.ReleaseForSpawn();
  const std::uint16_t target_port = target_reservation.ReleaseForSpawn();
  ChildProcess source(
      ServerArguments(source_port, source_data), source_log,
      {{"KEYLANE_REPLICATION_PAUSE_FULLSYNC_AFTER_HANDOFF_MS", "5000"}});
  ChildProcess target(ServerArguments(target_port, target_data), target_log);
  WaitForStartup(source_port, "crash source startup");
  WaitForStartup(target_port, "crash target startup");

  // This key is in partition zero, the first partition handed off by the
  // single flow. The source pause therefore observes a persisted reset plus a
  // real candidate record and handoff, while the rest of the population is
  // still incomplete.
  constexpr std::string_view key = "crash-boundary-28251";
  {
    RespClient source_client = Connect(source_port);
    RespClient target_client = Connect(target_port);
    ASSERT_EQ(source_client.Command({"CLUSTER", "KEYSLOT", key}), ":0");
    ASSERT_EQ(source_client.Command({"SET", key, "source-population"}), "+OK");
    ASSERT_EQ(target_client.Command({"SET", key, "old-target-population"}),
              "+OK");
    ASSERT_EQ(target_client.Command(
                  {"REPLICAOF", "127.0.0.1", std::to_string(source_port)}),
              "+OK");
    WaitUntil("acknowledged handoff pause", 30s, [&] {
      return ReadFile(source_log)
                 .find("paused full sync after acknowledged handoff") !=
             std::string::npos;
    });
    target.Stop(SIGKILL);
  }

  auto cluster_arguments = ServerArguments(target_port, target_data);
  cluster_arguments.push_back("--cluster-enabled");
  target = ChildProcess(cluster_arguments, target_log);
  WaitForStartup(target_port, "partial cluster target recovery");
  {
    RespClient target_client = Connect(target_port);
    EXPECT_TRUE(target_client.Command({"GET", key}).starts_with("-LOADING"));
    EXPECT_TRUE(target_client.Command({"SET", key, "forbidden"})
                    .starts_with("-LOADING"));
  }
  // Preserve the exact partial on-disk population for the fresh-sync check.
  target.Stop(SIGKILL);

  target = ChildProcess(ServerArguments(target_port, target_data), target_log);
  WaitForStartup(target_port, "partial target standalone recovery");
  {
    RespClient target_client = Connect(target_port);
    // The durable full-sync fence survives a standalone restart. Neither the
    // invalidated old value nor the unpromoted candidate may be exposed while
    // the replacement population is incomplete.
    EXPECT_TRUE(target_client.Command({"GET", key}).starts_with("-LOADING"));
    ASSERT_EQ(target_client.Command(
                  {"REPLICAOF", "127.0.0.1", std::to_string(source_port)}),
              "+OK");
    WaitUntil("fresh full sync after target crash", 45s, [&] {
      return CountOccurrences(ReadFile(source_log), "selected=FULL") >= 2;
    });
    WaitUntil("post-crash replacement population online", 60s, [&] {
      const std::string info = target_client.Command({"INFO", "replication"});
      return info.find("keylane_replication_state:online") != std::string::npos;
    });
    ASSERT_EQ(target_client.Command({"READONLY"}), "+OK");
    EXPECT_EQ(target_client.Command({"GET", key}), "$17\r\nsource-population");
  }

  // Even a fully promoted SSD image cannot recreate the current-boot cluster
  // readiness capability after process loss.
  target.Stop(SIGKILL);
  target = ChildProcess(cluster_arguments, target_log);
  WaitForStartup(target_port, "promoted cluster target recovery");
  {
    RespClient target_client = Connect(target_port);
    EXPECT_TRUE(target_client.Command({"GET", key}).starts_with("-LOADING"));
    EXPECT_TRUE(target_client.Command({"SET", key, "forbidden"})
                    .starts_with("-LOADING"));
  }

  target.Stop(SIGINT);
  source.Stop(SIGINT);
}

TEST(RebuildProtocolIntegrationTest,
     PeerFlowCancellationAfterCommittedCommandResumesFromAppliedCursor) {
  ASSERT_FALSE(g_keylane_binary.empty());
  TempDirectory directory("cluster-cancel-after-command-apply");
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
  ChildProcess source(ServerArguments(source_port, source_data, 2), source_log);
  ChildProcess target(
      ServerArguments(target_port, target_data, 2), target_log,
      {{"KEYLANE_REPLICATION_CANCEL_PEER_FLOW_AFTER_COMMAND_APPLY_ONCE",
        "cancelled-apply-counter"}});
  WaitForStartup(source_port, "cancel-after-apply source startup");
  WaitForStartup(target_port, "cancel-after-apply target startup");

  RespClient source_client = Connect(source_port);
  RespClient target_client = Connect(target_port);
  ASSERT_EQ(target_client.Command(
                {"REPLICAOF", "127.0.0.1", std::to_string(source_port)}),
            "+OK");
  WaitUntil("initial two-flow full rebuild", 30s, [&] {
    const std::string info = target_client.Command({"INFO", "replication"});
    return info.find("keylane_replication_state:online") != std::string::npos &&
           CountOccurrences(ReadFile(source_log), "selected=FULL") >= 2;
  });
  ASSERT_EQ(target_client.Command({"READONLY"}), "+OK");
  for (unsigned index = 0; index < 64; ++index) {
    ASSERT_EQ(source_client.Command(
                  {"SET", "online-warmup-" + std::to_string(index), "ready"}),
              "+OK");
  }
  WaitUntil("two-flow online cursors advance", 30s, [&] {
    for (unsigned index = 0; index < 64; ++index) {
      if (target_client.Command(
              {"GET", "online-warmup-" + std::to_string(index)}) !=
          "$5\r\nready") {
        return false;
      }
    }
    return true;
  });

  ASSERT_EQ(source_client.Command({"INCR", "cancelled-apply-counter"}), ":1");
  WaitUntil("peer flow cancellation after apply", 30s, [&] {
    return ReadFile(target_log)
               .find(
                   "injected peer-flow session cancellation after command "
                   "apply") != std::string::npos;
  });
  // Storage completion is the applied boundary. Even when another flow
  // cancels the transport before ACK, the committed flow cursor must advance
  // so reconnect resumes rather than replaying the increment or rebuilding.
  WaitUntil("committed command resumes from all-flow cursors", 45s, [&] {
    return CountOccurrences(ReadFile(source_log), "selected=CONTINUE") >= 2;
  });
  WaitUntil("continued population online after cancelled apply", 30s, [&] {
    const std::string info = target_client.Command({"INFO", "replication"});
    return info.find("keylane_replication_state:online") != std::string::npos;
  });
  EXPECT_EQ(target_client.Command({"GET", "cancelled-apply-counter"}),
            "$1\r\n1");

  target.Stop(SIGINT);
  source.Stop(SIGINT);
}

TEST(RebuildProtocolIntegrationTest,
     FullSyncCutInstallsResumeVectorBeforeAcknowledgement) {
  ASSERT_FALSE(g_keylane_binary.empty());
  TempDirectory directory("cluster-full-sync-cut");
  const std::filesystem::path source_data = directory.path() / "source.data";
  const std::filesystem::path target_data = directory.path() / "target.data";
  const std::filesystem::path source_log = directory.path() / "source.log";
  const std::filesystem::path target_log = directory.path() / "target.log";
  CreateDataFile(source_data, 256ULL * 1024 * 1024);
  CreateDataFile(target_data, 256ULL * 1024 * 1024);

  PortReservation source_reservation;
  PortReservation target_reservation;
  const std::uint16_t source_port = source_reservation.ReleaseForSpawn();
  const std::uint16_t target_port = target_reservation.ReleaseForSpawn();
  ChildProcess source(ServerArguments(source_port, source_data, 2), source_log);
  ChildProcess target(ServerArguments(target_port, target_data, 2), target_log,
                      {{"KEYLANE_REPLICATION_DROP_AFTER_FULLSYNC_CUT", "2"}});
  WaitForStartup(source_port, "cut-vector source startup");
  WaitForStartup(target_port, "cut-vector target startup");

  RespClient source_client = Connect(source_port);
  RespClient target_client = Connect(target_port);
  ASSERT_EQ(target_client.Command({"READONLY"}), "+OK");
  // These hash tags map to different workers when the source has two workers.
  // Assert the slots so a hash change cannot silently reduce this to one-flow
  // coverage.
  const std::string counter0 = "cut-counter-{foo}";
  const std::string counter1 = "cut-counter-{user1000}";
  ASSERT_EQ(source_client.Command({"CLUSTER", "KEYSLOT", counter0}), ":12182");
  ASSERT_EQ(source_client.Command({"CLUSTER", "KEYSLOT", counter1}), ":3443");

  ASSERT_EQ(source_client.Command({"SET", counter0, "0"}), "+OK");
  ASSERT_EQ(source_client.Command({"SET", counter1, "0"}), "+OK");
  ASSERT_EQ(target_client.Command(
                {"REPLICAOF", "127.0.0.1", std::to_string(source_port)}),
            "+OK");
  WaitUntil("initial cut-vector full sync", 60s, [&] {
    const std::string info = target_client.Command({"INFO", "replication"});
    return info.find("keylane_replication_state:online") != std::string::npos;
  });

  // Advance both old-history flow cursors past the special initial cursor. The
  // durable values become the restarted source's baseline; the process-local
  // cursors must be replaced by the new source history's complete cut vector.
  ASSERT_EQ(source_client.Command({"SET", counter0, "0"}), "+OK");
  ASSERT_EQ(source_client.Command({"SET", counter1, "0"}), "+OK");
  WaitUntil("old-history flow cursors", 30s, [&] {
    return target_client.Command({"GET", counter0}) == "$1\r\n0" &&
           target_client.Command({"GET", counter1}) == "$1\r\n0";
  });

  source.Stop(SIGINT);
  source = ChildProcess(
      ServerArguments(source_port, source_data, 2), source_log,
      {{"KEYLANE_REPLICATION_PAUSE_FULLSYNC_BEFORE_CUT_MS", "1500"}});
  WaitForStartup(source_port, "restarted cut-vector source startup");
  source_client = Connect(source_port);
  WaitUntil("replacement full-sync flows", 30s, [&] {
    const std::string info = target_client.Command({"INFO", "replication"});
    return info.find("keylane_connected_flows:2") != std::string::npos;
  });

  for (unsigned index = 1; index <= 5; ++index) {
    ASSERT_EQ(source_client.Command({"INCR", counter0}),
              ":" + std::to_string(index));
    ASSERT_EQ(source_client.Command({"INCR", counter1}),
              ":" + std::to_string(index));
  }

  WaitUntil("disconnect after cut acknowledgement", 30s, [&] {
    return ReadFile(target_log)
               .find(
                   "injected disconnect after full-sync cut "
                   "acknowledgement") != std::string::npos;
  });
  WaitUntil("replacement population online", 60s, [&] {
    const std::string info = target_client.Command({"INFO", "replication"});
    return info.find("keylane_replication_state:online") != std::string::npos;
  });
  EXPECT_EQ(source_client.Command({"GET", counter0}), "$1\r\n5");
  EXPECT_EQ(source_client.Command({"GET", counter1}), "$1\r\n5");
  EXPECT_EQ(target_client.Command({"GET", counter0}), "$1\r\n5");
  EXPECT_EQ(target_client.Command({"GET", counter1}), "$1\r\n5");

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
