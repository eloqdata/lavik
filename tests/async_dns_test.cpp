// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0
#include "lavik/async_dns.h"

#include <gtest/gtest.h>

#include <future>
#include <vector>

#include "bycorf/runtime/runtime.h"
#include "replication_internal.h"

TEST(AsyncDns, SlowFailedLookupDoesNotBlockCallerOrNumericLookup) {
  const auto begin = std::chrono::steady_clock::now();
  auto query = lavik::AsyncDnsQuery::Start("slow.lavik.invalid", 1234);
  ASSERT_TRUE(query);
  EXPECT_LT(std::chrono::steady_clock::now() - begin,
            std::chrono::milliseconds(100));
  EXPECT_FALSE(query->done_.load(std::memory_order_acquire));
  auto numeric = lavik::AsyncDnsQuery::Start("127.0.0.1", 1234);
  ASSERT_TRUE(numeric);
  EXPECT_TRUE(numeric->done_.load(std::memory_order_acquire));
  const auto deadline = begin + std::chrono::seconds(5);
  while (!query->done_.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < deadline)
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  ASSERT_TRUE(query->done_.load(std::memory_order_acquire));
  EXPECT_EQ(query->result_, EAI_AGAIN);
  EXPECT_EQ(query->addresses_, nullptr);
}

TEST(AsyncDns, SaturationBoundsUncancelableLookupsAndKeepsNumericFastPath) {
  std::vector<std::shared_ptr<lavik::AsyncDnsQuery>> queries;
  for (unsigned i = 0; i < 32; ++i)
    queries.push_back(lavik::AsyncDnsQuery::Start("slow.lavik.invalid", 1));
  EXPECT_FALSE(lavik::AsyncDnsQuery::Start("slow.lavik.invalid", 1));
  EXPECT_TRUE(lavik::AsyncDnsQuery::Start("::1", 1));
  queries
      .clear();  // Abandoning callers must leave only resolver-owned storage.
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (lavik::AsyncDnsQuery::in_flight_.load() &&
         std::chrono::steady_clock::now() < deadline)
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  EXPECT_EQ(lavik::AsyncDnsQuery::in_flight_.load(), 0u);
}

namespace {
using namespace std::chrono_literals;
using lavik::replication_internal::ResolveRecoveryAddress;
using lavik::replication_internal::SocketSet;

struct ResolutionBatch {
  SocketSet sockets;
  std::vector<absl::Status> results{64};
  unsigned completed = 0;
  unsigned completed_at_timer = 0;
};

bycorf::Task<absl::Status> ResolveOne(std::shared_ptr<ResolutionBatch> batch,
                                      unsigned index) {
  auto result =
      co_await ResolveRecoveryAddress("slow.localhost", 1234, &batch->sockets);
  batch->results[index] = result.status();
  ++batch->completed;
  co_return absl::OkStatus();
}

bycorf::Task<absl::Status> ResolveBatch(
    bycorf::Worker& worker, std::shared_ptr<ResolutionBatch> batch, bool cancel,
    std::shared_ptr<std::promise<absl::Status>> completion) {
  // More flows than resolver slots must wait without blocking worker timers
  // or failing the session. Cancellation must also wake capacity waiters.
  for (unsigned i = 0; i < batch->results.size(); ++i)
    worker.Spawn(ResolveOne(batch, i));
  auto waited = co_await bycorf::SleepFor(worker, 25ms);
  batch->completed_at_timer = batch->completed;
  if (cancel) batch->sockets.Cancel();
  while (waited.ok() && batch->completed != batch->results.size())
    waited = co_await bycorf::SleepFor(worker, 1ms);
  completion->set_value(waited);
  co_return absl::OkStatus();
}

void CheckReplicationResolutionBatch(bool cancel) {
  auto batch = std::make_shared<ResolutionBatch>();
  auto completion = std::make_shared<std::promise<absl::Status>>();
  auto result = completion->get_future();
  bycorf::Runtime runtime;
  runtime.Start(
      1,
      [batch, cancel, completion](unsigned, bycorf::Worker& worker) {
        auto initialized = worker.Init();
        if (!initialized.ok()) {
          completion->set_value(initialized);
          return 1;
        }
        worker.Spawn(ResolveBatch(worker, batch, cancel, completion));
        worker.Run();
        worker.Shutdown();
        worker.DestroyDetachedTasks();
        return 0;
      },
      false);
  const bool finished = result.wait_for(5s) == std::future_status::ready;
  batch->sockets.Cancel();
  runtime.RequestStop();
  runtime.WaitUntilStopped();
  ASSERT_TRUE(finished);
  ASSERT_TRUE(result.get().ok());
  EXPECT_EQ(batch->completed_at_timer, 0u);
  EXPECT_EQ(batch->completed, batch->results.size());
  for (const auto& status : batch->results) {
    if (cancel)
      EXPECT_TRUE(absl::IsCancelled(status)) << status;
    else
      EXPECT_TRUE(status.ok()) << status;
  }
  // Cancelled libc lookups own their storage until they complete; do not let
  // them occupy slots belonging to the next test.
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (lavik::AsyncDnsQuery::in_flight_.load() &&
         std::chrono::steady_clock::now() < deadline)
    std::this_thread::sleep_for(1ms);
  EXPECT_EQ(lavik::AsyncDnsQuery::in_flight_.load(), 0u);
}
}  // namespace

TEST(AsyncDns, ReplicationWaitsForCapacityAndResumesAllFlows) {
  CheckReplicationResolutionBatch(false);
}

TEST(AsyncDns, ReplicationCapacityWaitIsCancelledWithSession) {
  CheckReplicationResolutionBatch(true);
}
