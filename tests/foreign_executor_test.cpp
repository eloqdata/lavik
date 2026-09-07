#include "celer/runtime/foreign_executor.h"

#include <atomic>
#include <chrono>
#include <coroutine>
#include <future>
#include <stdexcept>
#include <thread>
#include <vector>

#include "absl/status/status.h"
#include "celer/runtime/cross_core.h"
#include "celer/runtime/runtime.h"
#include "celer/runtime/task.h"
#include "celer/runtime/worker.h"
#include "gtest/gtest.h"

namespace celer {
namespace {
using namespace std::chrono_literals;

struct CaptureSuspension {
  std::promise<std::coroutine_handle<>>* suspended_;

  bool await_ready() const noexcept { return false; }
  void await_suspend(std::coroutine_handle<> handle) const noexcept {
    suspended_->set_value(handle);
  }
  void await_resume() const noexcept {}
};

Task<absl::Status> SuspendUntilForeignResume(
    std::promise<std::coroutine_handle<>>* suspended,
    std::promise<void>* resumed) {
  co_await CaptureSuspension{suspended};
  resumed->set_value();
  co_return absl::OkStatus();
}

TEST(ForeignExecutorTest, TypedNotificationsUseTargetWorkerMpscMailbox) {
  Runtime runtime;
  std::promise<absl::Status> initialized;
  std::future<absl::Status> init_result = initialized.get_future();
  runtime.Start(
      1,
      [&initialized](unsigned, Worker& worker) {
        const absl::Status status = worker.Init();
        initialized.set_value(status);
        if (!status.ok()) return 1;
        worker.Run();
        worker.Shutdown();
        worker.DestroyDetachedTasks();
        return 0;
      },
      false);
  ASSERT_TRUE(init_result.get().ok());

  ForeignExecutor executor = runtime.GetForeignExecutor(0);
  constexpr unsigned kProducerCount = 4;
  constexpr unsigned kNotificationsPerProducer = 1000;
  constexpr unsigned kTotal = kProducerCount * kNotificationsPerProducer;
  std::atomic<unsigned> delivered{0};
  std::atomic<bool> wrong_worker{false};
  std::promise<void> complete;
  std::future<void> completed = complete.get_future();

  // Give the otherwise idle worker an opportunity to park so this also
  // exercises the foreign eventfd wake rather than only busy-loop delivery.
  std::this_thread::sleep_for(20ms);
  std::vector<std::thread> producers;
  for (unsigned producer = 0; producer < kProducerCount; ++producer) {
    producers.emplace_back([&] {
      for (unsigned sequence = 0; sequence < kNotificationsPerProducer;
           ++sequence) {
        while (!executor.Notify([&]() noexcept {
          if (ThisWorker().self_ == nullptr || ThisWorker().id_ != 0) {
            wrong_worker.store(true, std::memory_order_relaxed);
          }
          if (delivered.fetch_add(1, std::memory_order_acq_rel) + 1 == kTotal) {
            complete.set_value();
          }
        })) {
          std::this_thread::yield();
        }
      }
    });
  }
  for (std::thread& producer : producers) producer.join();
  executor.WaitUntilIdle();

  ASSERT_EQ(completed.wait_for(10s), std::future_status::ready);
  EXPECT_EQ(delivered.load(std::memory_order_acquire), kTotal);
  EXPECT_FALSE(wrong_worker.load(std::memory_order_relaxed));
  runtime.RequestStop();
  runtime.WaitUntilStopped();
  EXPECT_EQ(runtime.exit_code(), 0);
}

TEST(ForeignExecutorTest, ResumeSchedulesCoroutineWithoutCallableWrapper) {
  Runtime runtime;
  std::promise<absl::Status> initialized;
  std::future<absl::Status> init_result = initialized.get_future();
  std::promise<std::coroutine_handle<>> suspended;
  std::future<std::coroutine_handle<>> suspension = suspended.get_future();
  std::promise<void> resumed;
  std::future<void> resumed_signal = resumed.get_future();
  runtime.Start(
      1,
      [&](unsigned, Worker& worker) {
        const absl::Status status = worker.Init();
        initialized.set_value(status);
        if (!status.ok()) return 1;
        worker.Spawn(SuspendUntilForeignResume(&suspended, &resumed));
        worker.Run();
        worker.Shutdown();
        worker.DestroyDetachedTasks();
        return 0;
      },
      false);
  ASSERT_TRUE(init_result.get().ok());

  ForeignExecutor executor = runtime.GetForeignExecutor(0);
  const std::coroutine_handle<> continuation = suspension.get();
  ASSERT_TRUE(executor.Resume(continuation));
  ASSERT_EQ(resumed_signal.wait_for(10s), std::future_status::ready);
  runtime.RequestStop();
  runtime.WaitUntilStopped();
  EXPECT_EQ(runtime.exit_code(), 0);
}

TEST(ForeignExecutorTest, RejectsInvalidRuntimeAndWorker) {
  Runtime runtime;
  EXPECT_THROW((void)runtime.GetForeignExecutor(0), std::logic_error);

  std::promise<absl::Status> initialized;
  std::future<absl::Status> init_result = initialized.get_future();
  runtime.Start(
      1,
      [&](unsigned, Worker& worker) {
        const absl::Status status = worker.Init();
        initialized.set_value(status);
        if (!status.ok()) return 1;
        worker.Run();
        worker.Shutdown();
        worker.DestroyDetachedTasks();
        return 0;
      },
      false);
  ASSERT_TRUE(init_result.get().ok());
  EXPECT_THROW((void)runtime.GetForeignExecutor(1), std::out_of_range);
  ForeignExecutor executor = runtime.GetForeignExecutor(0);
  runtime.RequestStop();
  runtime.WaitUntilStopped();
  EXPECT_FALSE(executor.Notify([]() noexcept {}));
}

}  // namespace
}  // namespace celer
