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

#include <array>
#include <chrono>
#include <future>

#include "absl/cleanup/cleanup.h"
#include "bycorf/runtime/cross_core.h"
#include "bycorf/runtime/foreign_executor.h"
#include "bycorf/runtime/runtime.h"
#include "bycorf/runtime/worker.h"
#include "gtest/gtest.h"
#include "lavik/command_table.h"
#include "lavik/metrics.h"

namespace lavik {
namespace {

using namespace std::chrono_literals;

struct Snapshots {
  WorkerMetricsSnapshot empty_, full_, gauges_, totals_, info_, reset_, reused_;
  WorkerMetricsSnapshot errors_;
};

bycorf::Task<absl::Status> CollectSnapshots(
    std::promise<Snapshots>* completed) {
  Snapshots snapshots;
  snapshots.empty_ = co_await CollectWorkerMetrics();
  for (unsigned worker = 0; worker < 2; ++worker) {
    co_await bycorf::SubmitTo(worker, [worker] {
      for (unsigned i = 0; i < 3 + worker; ++i) {
        RecordCommandMetric(CommandKind::kGet, 100);
        RecordCommandResult(static_cast<std::size_t>(CommandKind::kGet), {},
                            100);
      }
      // The same global prefix is seen on two owners. Contexts share one
      // commandstats row; errorstats has no command or context dimension.
      RecordCommandResult(
          static_cast<std::size_t>(CommandKind::kGet),
          {CommandResult::kRedirected, CommandErrorReason::kMoved,
           CommandErrorPrefix::kMoved},
          0);
      RecordCommandResult(
          static_cast<std::size_t>(CommandKind::kLPush),
          {CommandResult::kFailed, CommandErrorReason::kWrongType,
           CommandErrorPrefix::kWrongType},
          50);
      RecordCommandResult(static_cast<std::size_t>(CommandKind::kGet), {}, 30);
      RecordCommandResult(static_cast<std::size_t>(CommandKind::kGet),
                          {CommandResult::kQueued}, 0);
      RecordCommandResult(
          static_cast<std::size_t>(CommandKind::kGet),
          {CommandResult::kCancelled, CommandErrorReason::kCancelled}, 100);
      // A subcommand identity outside the legacy CommandKind array must also
      // survive sparse sorting, aggregation and INFO projection.
      RecordCommandResult(CommandIdentityCount() - 1, {}, 20);
      return true;
    });
  }
  snapshots.full_ = co_await CollectWorkerMetrics();
  snapshots.gauges_ =
      co_await CollectWorkerMetrics(CommandMetricsDetail::kNone);
  snapshots.totals_ =
      co_await CollectWorkerMetrics(CommandMetricsDetail::kTotalCalls);
  snapshots.info_ =
      co_await CollectWorkerMetrics(CommandMetricsDetail::kCommandStats);
  snapshots.errors_ =
      co_await CollectWorkerMetrics(CommandMetricsDetail::kErrorStats);
  const auto reset = co_await ResetCommandMetrics();
  if (!reset.ok()) co_return reset;
  snapshots.reset_ = co_await CollectWorkerMetrics();
  RecordCommandMetric(CommandKind::kGet, 200);
  RecordCommandResult(static_cast<std::size_t>(CommandKind::kGet), {}, 200);
  RecordCommandResult(static_cast<std::size_t>(CommandKind::kLPush),
                      {CommandResult::kFailed, CommandErrorReason::kWrongType,
                       CommandErrorPrefix::kWrongType},
                      40);
  snapshots.reused_ = co_await CollectWorkerMetrics();
  completed->set_value(std::move(snapshots));
  co_return absl::OkStatus();
}

TEST(MetricsSnapshotTest, SparseOwnersPreserveTotalsProjectionAndReset) {
  InitWorkerMetrics(2);
  bycorf::Runtime runtime;
  std::array<std::promise<absl::Status>, 2> initialized;
  std::promise<Snapshots> completed;
  auto result = completed.get_future();
  runtime.Start(
      2,
      [&](unsigned id, bycorf::Worker& worker) {
        const auto status = worker.Init();
        initialized[id].set_value(status);
        if (!status.ok()) return 1;
        worker.Run();
        worker.Shutdown();
        worker.DestroyDetachedTasks();
        return 0;
      },
      false);
  absl::Cleanup stop = [&] {
    runtime.RequestStop();
    runtime.WaitUntilStopped();
  };
  for (auto& ready : initialized) ASSERT_TRUE(ready.get_future().get().ok());
  ASSERT_TRUE(runtime.GetForeignExecutor(0).Notify([&]() noexcept {
    bycorf::ThisWorker().self_->Spawn(CollectSnapshots(&completed));
  }));
  ASSERT_EQ(result.wait_for(10s), std::future_status::ready);
  const auto snapshots = result.get();
  auto expect_empty_details = [](const CommandMetricsSnapshot& commands) {
    EXPECT_TRUE(commands.commands_.empty());

    EXPECT_TRUE(commands.errors_.empty());
    EXPECT_TRUE(commands.stats_.empty());
  };
  expect_empty_details(snapshots.empty_.commands_);
  expect_empty_details(snapshots.gauges_.commands_);
  expect_empty_details(snapshots.totals_.commands_);
  EXPECT_EQ(snapshots.totals_.TotalCalls(), 7);

  const auto& full = snapshots.full_.commands_;
  EXPECT_EQ(snapshots.full_.TotalCalls(), 7);
  ASSERT_EQ(full.commands_.size(), 1);
  EXPECT_EQ(full.commands_[0].index_,
            static_cast<std::size_t>(CommandKind::kGet));
  EXPECT_EQ(full.commands_[0].totals_.calls_, 7);
  EXPECT_EQ(full.commands_[0].totals_.latency_ticks_, 700);
  std::uint64_t buckets = 0;
  for (auto bin : full.commands_[0].totals_.latency_bins_) buckets += bin;
  EXPECT_EQ(buckets, 7);
  ASSERT_EQ(full.stats_.size(), 3);
  ASSERT_EQ(full.errors_.size(), 2);
  for (const auto& error : full.errors_) EXPECT_EQ(error.totals_, 2);
  EXPECT_EQ(full.stats_[0].index_, static_cast<std::size_t>(CommandKind::kGet));
  EXPECT_EQ(full.stats_[0].totals_.calls_, 9);
  EXPECT_EQ(full.stats_[0].totals_.rejected_calls_, 2);
  EXPECT_EQ(full.stats_[0].totals_.execution_ticks_, 760);
  EXPECT_EQ(full.stats_.back().index_, CommandIdentityCount() - 1);

  const auto& info = snapshots.info_.commands_;
  EXPECT_TRUE(info.commands_.empty());

  EXPECT_TRUE(info.errors_.empty());
  ASSERT_EQ(info.stats_.size(), 3);
  EXPECT_EQ(info.stats_[0].totals_.calls_, 9);
  EXPECT_EQ(info.stats_[0].totals_.rejected_calls_, 2);
  EXPECT_EQ(info.stats_[0].totals_.execution_ticks_, 760);
  EXPECT_EQ(info.stats_[1].totals_.failed_calls_, 2);
  EXPECT_TRUE(snapshots.errors_.commands_.commands_.empty());
  EXPECT_TRUE(snapshots.errors_.commands_.stats_.empty());
  EXPECT_EQ(snapshots.errors_.commands_.errors_.size(), 2);

  const auto& reset = snapshots.reset_.commands_;
  EXPECT_EQ(snapshots.reset_.TotalCalls(), 0);
  EXPECT_TRUE(reset.commands_.empty());

  EXPECT_TRUE(reset.stats_.empty());
  EXPECT_TRUE(reset.errors_.empty());
  const auto& reused = snapshots.reused_.commands_;
  EXPECT_EQ(snapshots.reused_.TotalCalls(), 1);
  ASSERT_EQ(reused.commands_.size(), 1);
  EXPECT_EQ(reused.commands_[0].totals_.calls_, 1);
  ASSERT_EQ(reused.stats_.size(), 2);
  EXPECT_EQ(reused.stats_[0].totals_.calls_, 1);
  ASSERT_EQ(reused.errors_.size(), 1);
  EXPECT_EQ(reused.errors_[0].index_,
            static_cast<unsigned>(CommandErrorPrefix::kWrongType));
  EXPECT_EQ(reused.errors_[0].totals_, 1);
}

}  // namespace
}  // namespace lavik
