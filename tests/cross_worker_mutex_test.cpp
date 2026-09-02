#include <atomic>
#include <chrono>

#include "absl/status/status.h"
#include "celer/io/storage.h"
#include "celer/net/server.h"
#include "celer/net/service.h"
#include "celer/runtime/sync.h"
#include "celer/runtime/worker.h"
#include "gtest/gtest.h"

namespace celer {
namespace {

using namespace std::chrono_literals;

class CrossWorkerMutexService final : public Service {
 public:
  explicit CrossWorkerMutexService(Server* server) : server_(server) {}

  void Prepare(unsigned thread_count) override {
    prepared_ = thread_count == 2;
  }

  Task<absl::Status> Run(Worker& worker, ServiceContext) override {
    if (!prepared_) {
      failed_.store(true, std::memory_order_release);
      server_->RequestStop();
      co_return absl::FailedPreconditionError(
          "cross-worker mutex test requires two workers");
    }
    if (worker.id() == 0) {
      co_return co_await HoldMutex(worker);
    }
    co_return co_await WaitForMutex(worker);
  }

  void Stop() noexcept override {}

  bool probe_ran_while_locked() const noexcept {
    return probe_ran_while_locked_.load(std::memory_order_acquire);
  }
  bool waiter_acquired() const noexcept {
    return waiter_acquired_.load(std::memory_order_acquire);
  }
  bool failed() const noexcept {
    return failed_.load(std::memory_order_acquire);
  }

 private:
  Task<absl::Status> ProbeUnrelatedCoroutine() {
    unrelated_ran_.store(true, std::memory_order_release);
    co_return absl::OkStatus();
  }

  Task<absl::Status> HoldMutex(Worker& worker) {
    co_await mutex_.Lock(worker);
    {
      CrossWorkerMutex::Guard lock(&mutex_);
      owner_acquired_.store(true, std::memory_order_release);
      const auto deadline = std::chrono::steady_clock::now() + 2s;
      while (!unrelated_ran_.load(std::memory_order_acquire) &&
             std::chrono::steady_clock::now() < deadline) {
        absl::Status slept = co_await SleepFor(worker, 1ms);
        if (!slept.ok()) co_return slept;
      }
      probe_ran_while_locked_.store(
          unrelated_ran_.load(std::memory_order_acquire),
          std::memory_order_release);
    }

    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (!waiter_acquired_.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < deadline) {
      absl::Status slept = co_await SleepFor(worker, 1ms);
      if (!slept.ok()) co_return slept;
    }
    if (!waiter_acquired_.load(std::memory_order_acquire)) {
      failed_.store(true, std::memory_order_release);
    }
    server_->RequestStop();
    co_return absl::OkStatus();
  }

  Task<absl::Status> WaitForMutex(Worker& worker) {
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (!owner_acquired_.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < deadline) {
      absl::Status slept = co_await SleepFor(worker, 1ms);
      if (!slept.ok()) co_return slept;
    }
    if (!owner_acquired_.load(std::memory_order_acquire)) {
      failed_.store(true, std::memory_order_release);
      server_->RequestStop();
      co_return absl::DeadlineExceededError(
          "mutex owner did not acquire before deadline");
    }

    // This task is ready on worker 1 before the following lock attempt. It can
    // run only if contention suspends this coroutine instead of parking the
    // worker pthread.
    worker.Spawn(ProbeUnrelatedCoroutine());
    co_await mutex_.Lock(worker);
    CrossWorkerMutex::Guard lock(&mutex_);
    waiter_acquired_.store(true, std::memory_order_release);
    co_return absl::OkStatus();
  }

  Server* server_ = nullptr;
  CrossWorkerMutex mutex_;
  bool prepared_ = false;
  std::atomic<bool> owner_acquired_{false};
  std::atomic<bool> unrelated_ran_{false};
  std::atomic<bool> probe_ran_while_locked_{false};
  std::atomic<bool> waiter_acquired_{false};
  std::atomic<bool> failed_{false};
};

TEST(CrossWorkerMutexTest, SuspendsOnlyTheContendingCoroutine) {
  Server server;
  CrossWorkerMutexService service(&server);
  server.AddService(&service);
  ServerOptions options;
  options.thread_count_ = 2;
  options.pin_workers_ = false;
  options.recv_buffer_count_ = 0;
  ASSERT_TRUE(server.Start(options).ok());
  server.WaitUntilStopped();

  EXPECT_FALSE(service.failed());
  EXPECT_TRUE(service.probe_ran_while_locked());
  EXPECT_TRUE(service.waiter_acquired());
}

}  // namespace
}  // namespace celer
