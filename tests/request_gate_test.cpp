#include "request_gate.h"

#include <atomic>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

namespace keylane {
namespace {

TEST(RequestGateTest, CloseRejectsNewEntriesAndWaitsForEveryWorker) {
  constexpr unsigned kWorkers = 8;
  RequestGate gate;
  gate.Prepare(kWorkers);
  for (unsigned worker = 0; worker < kWorkers; ++worker) {
    ASSERT_TRUE(gate.TryEnter(worker));
  }

  gate.Close();
  EXPECT_TRUE(gate.closed());
  for (unsigned worker = 0; worker < kWorkers; ++worker) {
    EXPECT_FALSE(gate.TryEnter(worker));
  }

  std::atomic<bool> drained{false};
  std::thread waiter([&] {
    gate.WaitUntilEmpty();
    drained.store(true, std::memory_order_release);
  });
  for (unsigned worker = 0; worker < kWorkers; ++worker) {
    EXPECT_FALSE(drained.load(std::memory_order_acquire));
    gate.Leave(worker);
  }
  waiter.join();
  EXPECT_TRUE(drained.load(std::memory_order_acquire));
}

TEST(RequestGateTest, ConcurrentCloseCannotLoseAnAdmittedEntry) {
  constexpr unsigned kWorkers = 8;
  RequestGate gate;
  gate.Prepare(kWorkers);

  std::atomic<unsigned> ready{0};
  std::atomic<bool> start{false};
  std::atomic<unsigned> executing{0};
  std::vector<std::thread> workers;
  workers.reserve(kWorkers);
  for (unsigned worker = 0; worker < kWorkers; ++worker) {
    workers.emplace_back([&, worker] {
      ready.fetch_add(1, std::memory_order_release);
      while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
      while (gate.TryEnter(worker)) {
        executing.fetch_add(1, std::memory_order_acq_rel);
        std::this_thread::yield();
        executing.fetch_sub(1, std::memory_order_acq_rel);
        gate.Leave(worker);
      }
    });
  }
  while (ready.load(std::memory_order_acquire) != kWorkers) {
    std::this_thread::yield();
  }
  start.store(true, std::memory_order_release);
  std::this_thread::yield();

  gate.Close();
  gate.WaitUntilEmpty();
  EXPECT_EQ(executing.load(std::memory_order_acquire), 0U);
  for (std::thread& worker : workers) worker.join();
  for (unsigned worker = 0; worker < kWorkers; ++worker) {
    EXPECT_FALSE(gate.TryEnter(worker));
  }
}

}  // namespace
}  // namespace keylane
