// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

#include <sched.h>

#include <atomic>
#include <chrono>
#include <future>
#include <limits>
#include <thread>

#include "bycorf/io/storage.h"
#include "bycorf/net/server.h"
#include "gtest/gtest.h"
#include "lavik/config.h"

namespace {
using namespace std::chrono_literals;

unsigned FirstAllowedCpu() {
  cpu_set_t allowed;
  CPU_ZERO(&allowed);
  if (::sched_getaffinity(0, sizeof(allowed), &allowed) != 0)
    return CPU_SETSIZE;
  for (unsigned cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
    if (CPU_ISSET(cpu, &allowed)) return cpu;
  }
  return CPU_SETSIZE;
}

TEST(WorkerPlacementTest, CpuPolicyIncludesControlWorkerAndCycles) {
  const unsigned cpu = FirstAllowedCpu();
  ASSERT_LT(cpu, CPU_SETSIZE);
  lavik::ServerOptions options;
  options.shard_count_ = 3;
  options.cpu_ids_ = {cpu};
  auto mapped = lavik::ResolveWorkerCpuIds(options);
  ASSERT_TRUE(mapped.ok()) << mapped.status();
  EXPECT_EQ(*mapped, (std::vector<unsigned>{cpu, cpu, cpu, cpu}));
  options.cpu_ids_ = {CPU_SETSIZE};
  EXPECT_FALSE(lavik::ResolveWorkerCpuIds(options).ok());
  options.pin_workers_ = false;
  EXPECT_FALSE(lavik::ValidateServerOptions(options).ok());
  options.cpu_ids_.clear();
  ASSERT_TRUE(lavik::ResolveWorkerCpuIds(options).ok());
  EXPECT_TRUE(lavik::ResolveWorkerCpuIds(options)->empty());
}

TEST(WorkerPlacementTest, ShardAndCpuConfigurationIsExplicit) {
  lavik::ServerOptions options;
  ASSERT_TRUE(lavik::ApplyRedisConfigDirective({"shards", "2"}, &options).ok());
  ASSERT_TRUE(
      lavik::ApplyRedisConfigDirective({"cpus", "2,4,2"}, &options).ok());
  EXPECT_EQ(options.shard_count_, 2u);
  EXPECT_EQ(options.cpu_ids_, (std::vector<unsigned>{2, 4, 2}));
  EXPECT_FALSE(
      lavik::ApplyRedisConfigDirective({"cpus", "2,,4"}, &options).ok());
  EXPECT_FALSE(lavik::ApplyRedisConfigDirective({"cpus", "-1"}, &options).ok());
  ASSERT_TRUE(
      lavik::ApplyRedisConfigDirective({"threads", "1"}, &options).ok());
  EXPECT_EQ(options.shard_count_, 1u);
}

struct PlacementState {
  std::atomic<bool> busy{false};
  std::atomic<bool> finished{false};
  std::atomic<unsigned> ticks_during_busy{0};
  std::atomic<unsigned> runs[2]{};
  std::atomic<unsigned> finalized[2]{};
  std::atomic<int> cpu[2]{{-1}, {-1}};
};

class PlacementService final : public bycorf::Service {
 public:
  PlacementService(PlacementState& state, unsigned role)
      : state_(state), role_(role) {
    SetWorkers({role});
  }
  void Prepare(unsigned) override {}
  void Stop() noexcept override {}
  void FinalizeWorker(bycorf::Worker& worker) noexcept override {
    state_.finalized[worker.id()].fetch_add(1);
  }
  bycorf::Task<absl::Status> Run(bycorf::Worker& worker,
                                 bycorf::ServiceContext) override {
    if (worker.id() != role_)
      co_return absl::InternalError("incorrect placement");
    state_.runs[worker.id()].fetch_add(1);
    state_.cpu[worker.id()].store(::sched_getcpu());
    if (role_ == 0) {
      // Deliberately never yield: a second runtime thread on the SAME logical
      // CPU must still run its timer through ordinary OS preemption.
      state_.busy.store(true);
      const auto end = std::chrono::steady_clock::now() + 300ms;
      while (std::chrono::steady_clock::now() < end) {
        std::atomic_signal_fence(std::memory_order_seq_cst);
      }
      state_.finished.store(true);
    } else {
      while (!state_.finished.load() && !worker.stop_requested()) {
        auto slept = co_await bycorf::SleepFor(worker, 5ms);
        if (!slept.ok()) co_return slept;
        if (state_.busy.load() && !state_.finished.load()) {
          state_.ticks_during_busy.fetch_add(1);
        }
      }
    }
    co_return absl::OkStatus();
  }

 private:
  PlacementState& state_;
  unsigned role_;
};

TEST(WorkerPlacementTest, ControlTimersSurviveNonYieldingDataOnSharedCpu) {
  const unsigned cpu = FirstAllowedCpu();
  ASSERT_LT(cpu, CPU_SETSIZE);
  PlacementState state;
  PlacementService data(state, 0), control(state, 1);
  bycorf::Server server;
  server.AddService(&data);
  server.AddService(&control);
  bycorf::ServerOptions options;
  options.thread_count_ = 2;
  options.cpu_ids_ = {cpu};
  options.recv_buffer_count_ = 0;
  ASSERT_TRUE(server.Start(options).ok());
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!state.finished.load() && !server.stopped() &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(10ms);
  }
  server.RequestStop();
  server.WaitUntilStopped();
  ASSERT_TRUE(state.finished.load());
  EXPECT_GE(state.ticks_during_busy.load(), 2u);
  for (unsigned id = 0; id < 2; ++id) {
    EXPECT_EQ(state.runs[id].load(), 1u);
    EXPECT_EQ(state.finalized[id].load(), 1u);
    EXPECT_EQ(state.cpu[id].load(), static_cast<int>(cpu));
  }
}

TEST(WorkerPlacementTest, InvalidServiceAndCpuPlacementNeverStartsServices) {
  PlacementState state;
  PlacementService data(state, 0);
  data.SetWorkers({2});
  bycorf::ServerOptions options;
  options.thread_count_ = 2;
  bycorf::Server server;
  server.AddService(&data);
  EXPECT_FALSE(server.Start(options).ok());
  EXPECT_EQ(state.runs[0].load(), 0u);

  bycorf::Runtime runtime;
  std::atomic<bool> entered{false};
  const std::vector<unsigned> cpus{CPU_SETSIZE};
  EXPECT_THROW(runtime.Start(
                   1,
                   [&](unsigned, bycorf::Worker&) {
                     entered.store(true);
                     return 0;
                   },
                   true, cpus),
               std::invalid_argument);
  EXPECT_FALSE(entered.load());
}
}  // namespace
