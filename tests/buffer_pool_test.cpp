#include "keylane/storage/buffer_pool.h"

#include <cstddef>
#include <cstdint>
#include <vector>

#include "absl/status/status.h"
#include "celer/net/server.h"
#include "celer/runtime/task.h"
#include "celer/runtime/worker.h"
#include "gtest/gtest.h"

namespace keylane::storage {
namespace {

class BufferPoolWaitService final : public celer::Service {
 public:
  void Prepare(unsigned thread_count) override {
    prepared_ = thread_count == 1;
  }

  celer::Task<absl::Status> Run(celer::Worker& worker,
                                celer::ServiceContext) override {
    if (!prepared_) {
      result_ = absl::FailedPreconditionError(
          "buffer-pool wait test requires one worker");
      worker.RequestStop();
      co_return result_;
    }
    RegisteredBufferPoolOptions options;
    options.registered_bytes_ = 16 * kMiB;
    options.storage_write_buffer_count_ = 1;
    options.write_buffer_bytes_ = 8 * kMiB;
    result_ = pool_.Init(worker, options);
    if (!result_.ok()) {
      worker.RequestStop();
      co_return result_;
    }

    std::uint16_t held_storage = 0;
    if (!pool_.TryAcquireWriteBuffer(&held_storage) || held_storage == 0) {
      result_ = absl::FailedPreconditionError(
          "test could not reserve the storage write buffer");
      worker.RequestStop();
      co_return result_;
    }
    storage_waiter_started_ = false;
    storage_waiter_acquired_ = false;
    worker.Spawn(AcquireStorageAfterRelease());
    co_await celer::Yield(worker);
    if (!storage_waiter_started_ || storage_waiter_acquired_) {
      result_ = absl::FailedPreconditionError(
          "storage-buffer waiter did not suspend on storage exhaustion");
      pool_.ReleaseWriteBuffer(held_storage);
      worker.RequestStop();
      co_return result_;
    }
    pool_.ReleaseWriteBuffer(held_storage);
    for (unsigned attempt = 0; attempt < 100 && !storage_waiter_acquired_;
         ++attempt) {
      co_await celer::Yield(worker);
    }
    if (!storage_waiter_acquired_) {
      result_ = absl::DeadlineExceededError(
          "storage-buffer release did not wake its waiter");
      worker.RequestStop();
      co_return result_;
    }

    std::vector<ReadBufferLease> held_reads;
    held_reads.reserve(pool_.read_buffer_count());
    for (std::size_t i = 0; i < pool_.read_buffer_count(); ++i) {
      auto acquired = co_await pool_.AcquireReadBuffer();
      if (!acquired.ok() || acquired->buffer_id() == 0) {
        result_ = absl::FailedPreconditionError(
            "test could not reserve every fixed read buffer");
        worker.RequestStop();
        co_return result_;
      }
      held_reads.push_back(std::move(*acquired));
    }
    read_waiter_started_ = false;
    read_waiter_acquired_ = false;
    worker.Spawn(AcquireReadAfterRelease());
    co_await celer::Yield(worker);
    if (!read_waiter_started_ || read_waiter_acquired_ ||
        pool_.overflow_read_buffer_count() != 0) {
      result_ = absl::FailedPreconditionError(
          "read-buffer exhaustion allocated overflow instead of waiting");
      worker.RequestStop();
      co_return result_;
    }
    held_reads.back().Reset();
    held_reads.pop_back();
    for (unsigned attempt = 0; attempt < 100 && !read_waiter_acquired_;
         ++attempt) {
      co_await celer::Yield(worker);
    }
    if (!read_waiter_acquired_ || pool_.overflow_read_buffer_count() != 0) {
      result_ = absl::DeadlineExceededError(
          "fixed-read-buffer release did not wake its waiter");
      worker.RequestStop();
      co_return result_;
    }

    // Overflow leases encode their one-based release id in the same token as
    // fixed-buffer ids. Exercise destruction and reuse so a representation
    // change cannot silently return an overflow buffer through the fixed path.
    auto overflow = co_await pool_.AcquireReadBuffer(2 * kMiB);
    if (!overflow.ok() || overflow->buffer_id() != 0 ||
        overflow->registered() || pool_.overflow_read_buffer_count() != 1) {
      result_ = absl::FailedPreconditionError(
          "oversized read did not acquire an overflow lease");
      worker.RequestStop();
      co_return result_;
    }
    std::byte* overflow_data = overflow->bytes().data();
    overflow->Reset();
    auto reused = co_await pool_.AcquireReadBuffer(2 * kMiB);
    if (!reused.ok() || reused->bytes().data() != overflow_data ||
        pool_.overflow_read_buffer_count() != 1) {
      result_ = absl::FailedPreconditionError(
          "released overflow read buffer was not reusable");
    }
    worker.RequestStop();
    co_return result_;
  }

  void Stop() noexcept override {}

  const absl::Status& result() const noexcept { return result_; }

 private:
  celer::Task<absl::Status> AcquireStorageAfterRelease() {
    storage_waiter_started_ = true;
    std::uint16_t acquired = 0;
    while (!pool_.TryAcquireWriteBuffer(&acquired)) {
      co_await pool_.WaitForWriteBuffer();
    }
    storage_waiter_acquired_ = true;
    pool_.ReleaseWriteBuffer(acquired);
    co_return absl::OkStatus();
  }

  celer::Task<absl::Status> AcquireReadAfterRelease() {
    read_waiter_started_ = true;
    auto acquired = co_await pool_.AcquireReadBuffer();
    if (!acquired.ok() || acquired->buffer_id() == 0) {
      result_ = absl::FailedPreconditionError(
          "read waiter did not receive a fixed buffer");
      co_return result_;
    }
    read_waiter_acquired_ = true;
    acquired->Reset();
    co_return absl::OkStatus();
  }

  RegisteredBufferPool pool_;
  bool prepared_ = false;
  bool storage_waiter_started_ = false;
  bool storage_waiter_acquired_ = false;
  bool read_waiter_started_ = false;
  bool read_waiter_acquired_ = false;
  absl::Status result_ =
      absl::UnknownError("buffer-pool wait service did not run");
};

TEST(BufferPoolTest, StorageWriteBufferWaiterIsReusable) {
  BufferPoolWaitService service;
  celer::Server server;
  server.AddService(&service);
  celer::ServerOptions options;
  options.thread_count_ = 1;
  options.pin_workers_ = false;
  options.recv_buffer_count_ = 0;
  ASSERT_TRUE(server.Start(options).ok());
  server.WaitUntilStopped();
  EXPECT_TRUE(service.result().ok()) << service.result();
}

}  // namespace
}  // namespace keylane::storage
