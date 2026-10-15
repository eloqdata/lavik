/*
 * Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <algorithm>
#include <cstdint>
#include <string_view>

#include "grouped/write_e2e_support.h"

namespace {
using namespace grouped_e2e;

constexpr std::uint64_t kTriggerBytes = 128 * 1024;
constexpr unsigned kLongFlushTimerMs = 600'000;

std::uint64_t Stat(Client& client, std::string_view name) {
  const auto info = client.Command({"INFO", "STATS"});
  Check(info.kind_ == '$', "INFO failed");
  const auto prefix = std::string(name) + ':';
  const auto offset = info.text_.find(prefix);
  Check(offset != std::string::npos, "missing storage statistic");
  return std::stoull(info.text_.substr(offset + prefix.size()));
}

bool WaitForLog(const Server& server, std::string_view message) {
  const auto until = std::chrono::steady_clock::now() + 10s;
  while (std::chrono::steady_clock::now() < until) {
    if (server.Log().find(message) != std::string::npos) return true;
    if (!server.Running()) return false;
    std::this_thread::sleep_for(1ms);
  }
  return false;
}

std::vector<std::uint64_t> SubmissionSizes(const std::string& log) {
  constexpr std::string_view prefix = "test flush data submission bytes=";
  std::vector<std::uint64_t> sizes;
  for (std::size_t offset = 0;
       (offset = log.find(prefix, offset)) != std::string::npos;) {
    offset += prefix.size();
    sizes.push_back(std::stoull(log.substr(offset)));
  }
  return sizes;
}

class SnapshotGate {
 public:
  explicit SnapshotGate(const PrivateDisk& disk)
      : path_(disk.path() + ".flush-gate"),
        variable_("LAVIK_FLUSH_SNAPSHOT_PAUSE_FILE", path_.c_str()) {
    std::ofstream file(path_);
    Check(file.good(), "create flush snapshot gate");
  }
  ~SnapshotGate() { Release(); }
  void Release() { ::unlink(path_.c_str()); }

 private:
  std::string path_;
  ScopedEnvironment variable_;
};

// These values stay below grouped promotion. No transaction decision or
// explicit durability fence can accidentally trigger the flush under test.
std::size_t AppendCompact(Client& client, std::vector<std::string>& keys,
                          const std::string& value) {
  const std::string key = "flush-key-" + std::to_string(keys.size());
  Check(client.Command({"SET", key, value}).text_ == "OK", "SET failed");
  keys.push_back(key);
  return AlignRecord(RecordHeaderBytes(key.size()) + value.size());
}

void CheckRecovered(const PrivateDisk& disk,
                    const std::vector<std::string>& keys,
                    const std::string& value) {
  Server recovered(disk, 1);
  Client client(recovered.port());
  for (const auto& key : keys)
    EXPECT_EQ(client.Command({"GET", key}).text_, value) << key;
  EXPECT_EQ(recovered.Wait(true), 0) << recovered.Log();
}

TEST(AdaptiveFlushE2e, DirtyThresholdStartsFlushWithoutWaitingForAge) {
  PrivateDisk disk;
  std::vector<std::string> keys;
  const std::string value(4096, 't');
  {
    Server server(disk, 1, {}, {}, false, 2, "1G", {}, {}, 0,
                  kLongFlushTimerMs);
    Client client(server.port());
    std::size_t appended = AppendCompact(client, keys, value);
    EXPECT_GT(Stat(client, "storage_dirty_staging_bytes"), 0);
    EXPECT_LT(Stat(client, "storage_dirty_staging_bytes"), kTriggerBytes);
    EXPECT_EQ(Stat(client, "storage_flushes_pending"), 0);
    while (appended < kTriggerBytes)
      appended += AppendCompact(client, keys, value);
    // INFO only observes completion; it does not request a flush. The 10 min
    // timer is deliberately outside the fixture's deadlock watchdog.
    client.Durable();
    EXPECT_EQ(Stat(client, "storage_dirty_staging_bytes"), 0);
    EXPECT_EQ(Stat(client, "storage_tx_commits_pending"), 0);
    // Destruction kills the child, so recovery cannot be helped by shutdown.
  }
  CheckRecovered(disk, keys, value);
}

TEST(AdaptiveFlushE2e, InflightTailContinuesAndUsesAlreadyAvailableBatch) {
#if !LAVIK_TEST_FAULTS_AVAILABLE
  GTEST_SKIP() << "requires a deterministic snapshot pause";
#endif
  PrivateDisk disk;
  std::vector<std::string> keys;
  const std::string value(4096, 'b');
  {
    SnapshotGate gate(disk);
    ScopedEnvironment trace("LAVIK_TRACE_FLUSH_SUBMISSIONS", "1");
    Server server(disk, 1, {}, {}, false, 2, "1G", {}, {}, 0,
                  kLongFlushTimerMs);
    Client client(server.port());
    std::size_t first_snapshot = 0;
    while (first_snapshot < kTriggerBytes)
      first_snapshot += AppendCompact(client, keys, value);
    ASSERT_TRUE(WaitForLog(
        server, "fault pause reached: LAVIK_FLUSH_SNAPSHOT_PAUSE_FILE"))
        << server.Log();

    // No rollover, timer or further foreground request can rescue this tail:
    // it is appended while the first snapshot is in flight, then left idle.
    std::size_t tail = 0;
    while (tail < 2 * 1024 * 1024) tail += AppendCompact(client, keys, value);
    EXPECT_EQ(Stat(client, "storage_flushes_pending"), 1);
    EXPECT_TRUE(SubmissionSizes(server.Log()).empty());
    gate.Release();
    client.Durable();

    const auto sizes = SubmissionSizes(server.Log());
    ASSERT_FALSE(sizes.empty());
    EXPECT_LT(sizes.front(), 1024 * 1024);
    EXPECT_NE(std::find(sizes.begin(), sizes.end(), 1024 * 1024), sizes.end());
    for (const auto bytes : sizes) EXPECT_LE(bytes, 1024 * 1024);
    // One small first snapshot plus a tail bounded by three submissions. This
    // fails if an already available MiB is still fragmented into 128 KiB I/O.
    EXPECT_LE(sizes.size(), 4);
    EXPECT_EQ(Stat(client, "storage_dirty_staging_bytes"), 0);
  }
  CheckRecovered(disk, keys, value);
}

TEST(AdaptiveFlushE2e, AgeTimerFlushesPrefixBelowThreshold) {
  PrivateDisk disk;
  std::vector<std::string> keys;
  const std::string value(4096, 's');
  {
    Server server(disk, 1);
    Client client(server.port());
    ASSERT_LT(AppendCompact(client, keys, value), kTriggerBytes);
    client.Durable();
    EXPECT_EQ(Stat(client, "storage_dirty_staging_bytes"), 0);
  }
  CheckRecovered(disk, keys, value);
}

}  // namespace

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  if (argc != 2) return 2;
  grouped_e2e::server_binary = argv[1];
  return RUN_ALL_TESTS();
}
