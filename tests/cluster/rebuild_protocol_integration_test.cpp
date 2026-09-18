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
#include <unistd.h>

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
using lavik::test::ChildProcess;
using lavik::test::Connect;
using lavik::test::CreateDataFile;
using lavik::test::PortReservation;
using lavik::test::ReadFile;
using lavik::test::RespClient;
using lavik::test::TempDirectory;
using lavik::test::WaitUntil;

std::string g_lavik_binary;

std::vector<std::string> ServerArguments(std::uint16_t port,
                                         const std::filesystem::path& data,
                                         unsigned threads = 1) {
  return {g_lavik_binary,
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
     IndependentHandoffsCompleteBeforeFirstAckAndRemoveStalePartitions) {
#if !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires the deterministic handoff completion barrier";
#endif
  TempDirectory directory("async-handoff-order");
  const auto source_data = directory.path() / "source.data";
  const auto target_data = directory.path() / "target.data";
  const auto source_log = directory.path() / "source.log";
  const auto target_log = directory.path() / "target.log";
  CreateDataFile(source_data, 128ULL * 1024 * 1024);
  CreateDataFile(target_data, 128ULL * 1024 * 1024);
  PortReservation source_reservation;
  PortReservation target_reservation;
  const auto source_port = source_reservation.ReleaseForSpawn();
  const auto target_port = target_reservation.ReleaseForSpawn();
  ChildProcess source(ServerArguments(source_port, source_data), source_log);
  ChildProcess target(
      ServerArguments(target_port, target_data), target_log,
      {{"LAVIK_REPLICATION_HOLD_FIRST_HANDOFF_UNTIL_NEXT_ACK", "1"}});
  WaitForStartup(source_port, "async handoff source");
  WaitForStartup(target_port, "async handoff target");
  RespClient source_client = Connect(source_port);
  RespClient target_client = Connect(target_port);
  ASSERT_EQ(source_client.Command({"SET", "retained", "value"}), "+OK");
  ASSERT_EQ(target_client.Command({"SET", "stale", "must-disappear"}), "+OK");
  // Standalone FULL must still scan nonzero DBs while empty DBs take the fast
  // path. Reset also has to remove the target's old keys from those DBs.
  ASSERT_EQ(source_client.Command({"SELECT", "15"}), "+OK");
  ASSERT_EQ(source_client.Command({"SET", "retained", "db15"}), "+OK");
  ASSERT_EQ(source_client.Command({"SELECT", "0"}), "+OK");
  ASSERT_EQ(target_client.Command({"SELECT", "15"}), "+OK");
  ASSERT_EQ(target_client.Command({"SET", "stale", "old-db15"}), "+OK");
  ASSERT_EQ(target_client.Command({"SELECT", "0"}), "+OK");
  ASSERT_EQ(target_client.Command(
                {"REPLICAOF", "127.0.0.1", std::to_string(source_port)}),
            "+OK");
  // Partition zero cannot ACK until another partition has ACKed. A sender
  // that waits after every handoff, or a serial target, cannot finish this
  // FULL.
  WaitUntil("out-of-order handoff completes FULL", 20s, [&] {
    return target_client.Command({"INFO", "replication"})
               .find("lavik_replication_state:online") != std::string::npos;
  });
  const std::string log = ReadFile(target_log);
  const auto held = log.find("holding first partition handoff");
  const auto later = log.find("acknowledged async partition handoff 1");
  const auto first = log.find("acknowledged async partition handoff 0");
  ASSERT_NE(held, std::string::npos);
  ASSERT_NE(later, std::string::npos);
  ASSERT_NE(first, std::string::npos);
  EXPECT_LT(held, later);
  EXPECT_LT(later, first);
  EXPECT_EQ(CountOccurrences(ReadFile(source_log), "selected=FULL"), 1);
  ASSERT_EQ(target_client.Command({"READONLY"}), "+OK");
  EXPECT_EQ(target_client.Command({"GET", "retained"}), "$5\r\nvalue");
  EXPECT_EQ(target_client.Command({"EXISTS", "stale"}), ":0");
  ASSERT_EQ(target_client.Command({"SELECT", "15"}), "+OK");
  EXPECT_EQ(target_client.Command({"GET", "retained"}), "$4\r\ndb15");
  EXPECT_EQ(target_client.Command({"EXISTS", "stale"}), ":0");
  ASSERT_EQ(target_client.Command({"SELECT", "0"}), "+OK");
  ASSERT_EQ(source_client.Command({"SET", "retained", "after-cut"}), "+OK");
  WaitUntil("online writes follow the handoff cut", 10s, [&] {
    return target_client.Command({"GET", "retained"}) == "$9\r\nafter-cut";
  });
}

TEST(RebuildProtocolIntegrationTest, CancellationJoinsOutstandingHandoffTasks) {
#if !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires the deterministic handoff completion barrier";
#endif
  TempDirectory directory("async-handoff-cancel");
  const auto source_data = directory.path() / "source.data";
  const auto target_data = directory.path() / "target.data";
  const auto target_log = directory.path() / "target.log";
  CreateDataFile(source_data, 128ULL * 1024 * 1024);
  CreateDataFile(target_data, 128ULL * 1024 * 1024);
  PortReservation source_reservation;
  PortReservation target_reservation;
  const auto source_port = source_reservation.ReleaseForSpawn();
  const auto target_port = target_reservation.ReleaseForSpawn();
  ChildProcess source(ServerArguments(source_port, source_data),
                      directory.path() / "source.log");
  ChildProcess target(
      ServerArguments(target_port, target_data), target_log,
      {{"LAVIK_REPLICATION_HOLD_FIRST_HANDOFF_UNTIL_NEXT_ACK", "cancel"}});
  WaitForStartup(source_port, "cancel handoff source");
  WaitForStartup(target_port, "cancel handoff target");
  RespClient target_client = Connect(target_port);
  ASSERT_EQ(target_client.Command(
                {"REPLICAOF", "127.0.0.1", std::to_string(source_port)}),
            "+OK");
  WaitUntil("handoff remains outstanding", 10s, [&] {
    return ReadFile(target_log).find("holding first partition handoff") !=
           std::string::npos;
  });
  EXPECT_EQ(target_client.Command({"INFO", "replication"})
                .find("lavik_replication_state:online"),
            std::string::npos);
  // Return from role change proves cancellation joined tasks holding the old
  // stream; the withheld partition must not later publish an ACK or readiness.
  EXPECT_EQ(target_client.Command({"REPLICAOF", "NO", "ONE"}), "+OK");
  // An interrupted destructive FULL has no complete population to promote.
  EXPECT_TRUE(
      target_client.Command({"GET", "after-cancel"}).starts_with("-LOADING"));
  EXPECT_EQ(ReadFile(target_log).find("acknowledged async partition handoff 0"),
            std::string::npos);
}

TEST(RebuildProtocolIntegrationTest,
     SourceCannotPublishOnlineBeforeTargetFlowProof) {
#if !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP()
      << "requires a Debug/fault server for premature online injection";
#endif
  ASSERT_FALSE(g_lavik_binary.empty());
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
                      {{"LAVIK_REPLICATION_EARLY_ONLINE", "1"}});
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
    ASSERT_EQ(info.find("lavik_replication_state:online"), std::string::npos)
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
#if !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires a Debug/fault server for post-cut reset injection";
#endif
  ASSERT_FALSE(g_lavik_binary.empty());
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
                      {{"LAVIK_REPLICATION_POST_CUT_RESET_ONCE", "1"}});
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
    return info.find("lavik_replication_state:online") != std::string::npos;
  });
  ASSERT_EQ(target_client.Command({"READONLY"}), "+OK");
  EXPECT_EQ(target_client.Command({"GET", "promoted-proof"}),
            "$9\r\npreserved");

  target.Stop(SIGINT);
  source.Stop(SIGINT);
}

void CheckDivergentOnlineTail(unsigned workers, unsigned failing_flow) {
#if !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires a Debug/fault server for divergent tail injection";
#endif
  ASSERT_FALSE(g_lavik_binary.empty());
  TempDirectory directory("cluster-divergent-tail");
  const std::filesystem::path source_data = directory.path() / "source.data";
  const std::filesystem::path target_data = directory.path() / "target.data";
  const std::filesystem::path source_log = directory.path() / "source.log";
  const std::filesystem::path target_log = directory.path() / "target.log";
  CreateDataFile(source_data, workers * 128ULL * 1024 * 1024);
  CreateDataFile(target_data, workers * 128ULL * 1024 * 1024);

  PortReservation source_reservation;
  PortReservation target_reservation;
  const std::uint16_t source_port = source_reservation.ReleaseForSpawn();
  const std::uint16_t target_port = target_reservation.ReleaseForSpawn();
  ChildProcess source(ServerArguments(source_port, source_data, workers),
                      source_log,
                      {{"LAVIK_REPLICATION_DIVERGENT_TAIL_ONCE", "1"},
                       {"LAVIK_REPLICATION_DIVERGENT_TAIL_FLOW",
                        std::to_string(failing_flow)}});
  ChildProcess target(ServerArguments(target_port, target_data, workers),
                      target_log);
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
    return info.find("lavik_replication_state:online") != std::string::npos;
  });

  std::string after_gap = "population-after-gap";
  for (;;) {
    const auto slot = source_client.Command({"CLUSTER", "KEYSLOT", after_gap});
    ASSERT_FALSE(slot.empty());
    ASSERT_EQ(slot.front(), ':');
    if (std::stoul(slot.substr(1)) % workers == failing_flow) break;
    after_gap.push_back('x');
    ASSERT_LT(after_gap.size(), 128u);
  }
  ASSERT_EQ(source_client.Command({"SET", after_gap, "replayed"}), "+OK");
  WaitUntil("divergent tail injection", 30s, [&] {
    return ReadFile(source_log).find("injected divergent replication tail") !=
           std::string::npos;
  });
  WaitUntil("whole-population retry", 45s, [&] {
    return CountOccurrences(ReadFile(source_log), "selected=FULL") >=
           2 * workers;
  });
  WaitUntil("replacement population online", 30s, [&] {
    const std::string info = target_client.Command({"INFO", "replication"});
    return info.find("lavik_replication_state:online") != std::string::npos;
  });
  ASSERT_EQ(target_client.Command({"READONLY"}), "+OK");
  EXPECT_NE(
      ReadFile(target_log).find("invalidated native replication continuation"),
      std::string::npos);
  EXPECT_EQ(target_client.Command({"GET", after_gap}), "$8\r\nreplayed");

  target.Stop(SIGINT);
  source.Stop(SIGINT);
}

TEST(RebuildProtocolIntegrationTest,
     DivergentOnlineTailInvalidatesEveryContinuationCursor) {
  CheckDivergentOnlineTail(1, 0);
}

TEST(RebuildProtocolIntegrationTest,
     NonzeroWorkerInvalidatesContinuationThroughControlOwner) {
  // The corrupt frame is consumed on target worker 1, while all manager
  // state belongs to worker 0. Recovery must wait for the owner invalidation
  // and rebuild both flows, not continue from either old cursor.
  CheckDivergentOnlineTail(2, 1);
}

TEST(RebuildProtocolIntegrationTest,
     ShutdownClosesTargetSocketsWithoutReadingOwnerSessionState) {
  ASSERT_FALSE(g_lavik_binary.empty());
  TempDirectory directory("replication-target-shutdown");
  const auto source_data = directory.path() / "source.data";
  const auto target_data = directory.path() / "target.data";
  CreateDataFile(source_data, 256ULL * 1024 * 1024);
  CreateDataFile(target_data, 256ULL * 1024 * 1024);
  PortReservation source_reservation;
  PortReservation target_reservation;
  const auto source_port = source_reservation.ReleaseForSpawn();
  const auto target_port = target_reservation.ReleaseForSpawn();
  ChildProcess source(ServerArguments(source_port, source_data, 2),
                      directory.path() / "source.log");
  ChildProcess target(ServerArguments(target_port, target_data, 2),
                      directory.path() / "target.log");
  WaitForStartup(source_port, "shutdown source startup");
  WaitForStartup(target_port, "shutdown target startup");
  RespClient client = Connect(target_port);
  ASSERT_EQ(
      client.Command({"REPLICAOF", "127.0.0.1", std::to_string(source_port)}),
      "+OK");
  WaitUntil("target online before shutdown", 30s, [&] {
    return client.Command({"INFO", "replication"})
               .find("lavik_replication_state:online") != std::string::npos;
  });

  // No peer can acknowledge or close these connections. Main's transport
  // cancellation must wake worker zero and both flow owners, then let the
  // normal owner-side join finish before storage shutdown.
  source.Pause();
  ASSERT_EQ(::kill(target.pid(), SIGTERM), 0);
  const int status = target.Wait(5s);
  source.Resume();
  ASSERT_TRUE(WIFEXITED(status));
  EXPECT_EQ(WEXITSTATUS(status), 0);
  source.Stop(SIGINT);
}

TEST(RebuildProtocolIntegrationTest,
     CorruptFullSyncFrameRequiresAFreshFullSync) {
#if !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires a Debug/fault server for corrupt frame injection";
#endif
  ASSERT_FALSE(g_lavik_binary.empty());
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
      {{"LAVIK_REPLICATION_CORRUPT_FULLSYNC_RECORD_FRAME_ONCE", "1"}});
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
    return info.find("lavik_replication_state:online") != std::string::npos;
  });
  ASSERT_EQ(target_client.Command({"READONLY"}), "+OK");
  EXPECT_EQ(target_client.Command({"GET", "checksum-proof"}),
            "$9\r\npreserved");

  target.Stop(SIGINT);
  source.Stop(SIGINT);
}

TEST(RebuildProtocolIntegrationTest,
     TargetCrashDiscardsPartialAndFreshSyncRecoversPopulation) {
#if !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires a Debug/fault server for the partial handoff pause";
#endif
  ASSERT_FALSE(g_lavik_binary.empty());
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
      {{"LAVIK_REPLICATION_PAUSE_FULLSYNC_AFTER_HANDOFF_MS", "5000"}});
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
      return info.find("lavik_replication_state:online") != std::string::npos;
    });
    ASSERT_EQ(target_client.Command({"READONLY"}), "+OK");
    EXPECT_EQ(target_client.Command({"GET", key}), "$17\r\nsource-population");
  }

  target.Stop(SIGINT);
  source.Stop(SIGINT);
}

TEST(RebuildProtocolIntegrationTest,
     PeerFlowCancellationAfterCommittedCommandResumesFromAppliedCursor) {
#if !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires a Debug/fault server for peer-flow cancellation";
#endif
  ASSERT_FALSE(g_lavik_binary.empty());
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
      {{"LAVIK_REPLICATION_CANCEL_PEER_FLOW_AFTER_COMMAND_APPLY_ONCE",
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
    return info.find("lavik_replication_state:online") != std::string::npos &&
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
    return info.find("lavik_replication_state:online") != std::string::npos;
  });
  EXPECT_EQ(target_client.Command({"GET", "cancelled-apply-counter"}),
            "$1\r\n1");

  target.Stop(SIGINT);
  source.Stop(SIGINT);
}

TEST(RebuildProtocolIntegrationTest,
     FullSyncCutInstallsResumeVectorBeforeAcknowledgement) {
#if !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires a Debug/fault server for cut pause and disconnect";
#endif
  ASSERT_FALSE(g_lavik_binary.empty());
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
                      {{"LAVIK_REPLICATION_DROP_AFTER_FULLSYNC_CUT", "2"}});
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
    return info.find("lavik_replication_state:online") != std::string::npos;
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
      {{"LAVIK_REPLICATION_PAUSE_FULLSYNC_BEFORE_CUT_MS", "1500"}});
  WaitForStartup(source_port, "restarted cut-vector source startup");
  source_client = Connect(source_port);
  WaitUntil("replacement full-sync flows", 30s, [&] {
    const std::string info = target_client.Command({"INFO", "replication"});
    return info.find("lavik_connected_flows:2") != std::string::npos;
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
    return info.find("lavik_replication_state:online") != std::string::npos;
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
  g_lavik_binary = argv[1];
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
