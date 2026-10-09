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

#include "lavik/storage/buffer_pool.h"

#include <fcntl.h>
#include <sys/eventfd.h>
#include <sys/resource.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <vector>

#include "absl/status/status.h"
#include "bycorf/net/server.h"
#include "bycorf/runtime/task.h"
#include "bycorf/runtime/worker.h"
#include "gtest/gtest.h"

namespace lavik::storage {
namespace {

class BufferPoolWaitService final : public bycorf::Service {
 public:
  void Prepare(unsigned thread_count) override {
    prepared_ = thread_count == 1;
  }

  bycorf::Task<absl::Status> Run(bycorf::Worker& worker,
                                 bycorf::ServiceContext) override {
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
    co_await bycorf::Yield(worker);
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
      co_await bycorf::Yield(worker);
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
    co_await bycorf::Yield(worker);
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
      co_await bycorf::Yield(worker);
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
  bycorf::Task<absl::Status> AcquireStorageAfterRelease() {
    storage_waiter_started_ = true;
    std::uint16_t acquired = 0;
    while (!pool_.TryAcquireWriteBuffer(&acquired)) {
      co_await pool_.WaitForWriteBuffer();
    }
    storage_waiter_acquired_ = true;
    pool_.ReleaseWriteBuffer(acquired);
    co_return absl::OkStatus();
  }

  bycorf::Task<absl::Status> AcquireReadAfterRelease() {
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
  bycorf::Server server;
  server.AddService(&service);
  bycorf::ServerOptions options;
  options.thread_count_ = 1;
  options.pin_workers_ = false;
  options.recv_buffer_count_ = 0;
  ASSERT_TRUE(server.Start(options).ok());
  server.WaitUntilStopped();
  EXPECT_TRUE(service.result().ok()) << service.result();
}

// Change only the soft limit, after Worker startup. Each case restores it and
// closes its ring before another test can register buffers against the limit.
class BufferPoolMemlockTest : public ::testing::Test {
 protected:
  struct Completion : bycorf::IoCompletion {
    unsigned calls = 0;
    int result = -1;
    void Complete(bycorf::Worker&, int value, unsigned) override {
      ++calls;
      result = value;
    }
  };

  void SetUp() override {
    const int rc = getrlimit(RLIMIT_MEMLOCK, &original_limit_);
    limit_saved_ = rc == 0;
    ASSERT_EQ(rc, 0);
    saved_context_ = bycorf::MutableThisWorker();
    int fd = mkstemp(path_);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(ftruncate(fd, 4096), 0);
    close(fd);
    options_.registered_bytes_ = 8 * kMiB;
    options_.storage_write_buffer_count_ = 2;
    options_.write_buffer_bytes_ = 2 * kMiB;
    options_.read_slot_bytes_ = 12 * kKiB;
  }

  void TearDown() override {
    // Pool memory stays alive until fixed I/O has drained and the registered
    // table is gone. Restore the caller's worker context after shutdown.
    if (worker_) worker_->Shutdown();
    pool_.reset();
    if (wake_fd_ >= 0) close(wake_fd_);
    unlink(path_);
    if (limit_saved_) EXPECT_EQ(setrlimit(RLIMIT_MEMLOCK, &original_limit_), 0);
    bycorf::MutableThisWorker() = std::move(saved_context_);
  }

  void Init(rlim_t limit, unsigned runtime_workers = 1) {
    if (limit > original_limit_.rlim_max) {
      GTEST_SKIP() << "test needs a higher memlock hard limit";
    }
    cross_core_ = std::make_unique<bycorf::CrossCore>(runtime_workers);
    wake_fd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    ASSERT_GE(wake_fd_, 0);
    cross_core_->mailbox(0).wake_fd_ = wake_fd_;
    worker_ = std::make_unique<bycorf::Worker>();
    worker_->BindCrossCore(0, cross_core_.get());
    bycorf::SetThisWorker(0, cross_core_.get(), worker_.get());
    ASSERT_TRUE(worker_->Init({.recv_buffer_count_ = 0}).ok());
    ASSERT_TRUE(worker_->RegisterFixedFiles(1).ok());
    Completion opened;
    ASSERT_TRUE(
        worker_->SubmitOpenDirect(path_, O_RDWR | O_DIRECT, 0, {0}, &opened)
            .ok());
    ASSERT_NO_FATAL_FAILURE(Drain(opened));
    ASSERT_EQ(opened.result, 0);

    rlimit reduced = original_limit_;
    reduced.rlim_cur = limit;
    ASSERT_EQ(setrlimit(RLIMIT_MEMLOCK, &reduced), 0);
    pool_ = std::make_unique<RegisteredBufferPool>();
    auto status = pool_->Init(*worker_, options_);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(pool_->write_buffer_count(), 2);
    ASSERT_EQ(pool_->read_buffer_count(),
              (options_.registered_bytes_ - 2 * options_.write_buffer_bytes_) /
                  options_.read_slot_bytes_);
    ASSERT_FALSE(pool_->buffer_registered(0));
    ASSERT_FALSE(pool_->buffer_registered(65535));
  }

  void Drain(Completion& completion) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (completion.calls == 0 &&
           std::chrono::steady_clock::now() < deadline) {
      worker_->RunOnce(false);
    }
    ASSERT_EQ(completion.calls, 1);
  }

  // Exercise actual O_DIRECT I/O through both registered and ordinary paths.
  // Holding every read lease checks that pool capacity did not silently shrink;
  // Releasing and reacquiring checks that a shared registration index never
  // gets mistaken for a slot's release id.
  void CheckIo(unsigned expected_writes, unsigned min_reads, unsigned max_reads,
               unsigned last_region = 0) {
    unsigned registered_writes = 0;
    for (unsigned i = 0; i < pool_->write_buffer_count(); ++i) {
      std::uint16_t id = 0;
      ASSERT_TRUE(pool_->TryAcquireWriteBuffer(&id));
      auto buffer = pool_->write_buffer(id);
      EXPECT_EQ(buffer.index_, last_region);
      buffer.size_ = 4096;
      std::fill_n(buffer.data_, buffer.size_, std::byte{0x5a});
      Completion written;
      const bool registered = pool_->buffer_registered(id);
      registered_writes += registered;
      auto status = registered
                        ? worker_->SubmitWriteFixed({0}, buffer, 0, &written)
                        : worker_->SubmitWrite(
                              {0}, {buffer.data_, buffer.size_}, 0, &written);
      ASSERT_TRUE(status.ok()) << status;
      ASSERT_NO_FATAL_FAILURE(Drain(written));
      ASSERT_EQ(written.result, 4096);
      // Keep both write slots held so the next iteration checks the other id.
    }
    EXPECT_EQ(registered_writes, expected_writes);
    for (unsigned id = 1; id <= pool_->write_buffer_count(); ++id)
      pool_->ReleaseWriteBuffer(id);

    std::vector<std::byte*> slot_addresses(pool_->read_buffer_count());
    for (unsigned round = 0; round < 2; ++round) {
      unsigned registered_reads = 0;
      std::vector<ReadBufferLease> leases;
      for (unsigned i = 0; i < pool_->read_buffer_count(); ++i) {
        auto acquire = pool_->AcquireReadBuffer();
        ASSERT_TRUE(acquire.await_ready());
        auto result = acquire.await_resume();
        ASSERT_TRUE(result.ok()) << result.status();
        auto& lease = leases.emplace_back(std::move(*result));
        registered_reads += lease.registered();
        auto buffer = lease.io_buffer();
        // Use the same 4 KiB file to exercise both small slots and the larger
        // slots in the multi-arena case, including its final registered region.
        buffer.size_ = 4096;
        EXPECT_GE(buffer.index_, 0);
        EXPECT_LE(buffer.index_, last_region);
        EXPECT_EQ(lease.registered_buffer().index_, buffer.index_);
        ASSERT_GE(lease.buffer_id(), 3);
        ASSERT_LT(lease.buffer_id() - 3, slot_addresses.size());
        if (round == 0) {
          EXPECT_EQ(lease.buffer_id(), i + 3);
          slot_addresses[i] = lease.bytes().data();
          if (i == 0) EXPECT_EQ(buffer.index_, 0);
          if (i + 1 == pool_->read_buffer_count())
            EXPECT_EQ(buffer.index_, last_region);
          if (i != 0 && buffer.index_ == leases[i - 1].io_buffer().index_) {
            EXPECT_EQ(lease.bytes().data(),
                      slot_addresses[i - 1] + leases[i - 1].bytes().size());
          }
        } else {
          EXPECT_EQ(lease.bytes().data(),
                    slot_addresses[lease.buffer_id() - 3]);
        }
        Completion read;
        auto status = lease.registered()
                          ? worker_->SubmitReadFixed({0}, buffer, 0, &read)
                          : worker_->SubmitRead(
                                {0}, {buffer.data_, buffer.size_}, 0, &read);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_NO_FATAL_FAILURE(Drain(read));
        ASSERT_EQ(read.result, 4096);
        EXPECT_TRUE(
            std::all_of(buffer.data_, buffer.data_ + buffer.size_,
                        [](std::byte b) { return b == std::byte{0x5a}; }));
      }
      EXPECT_GE(registered_reads, min_reads);
      EXPECT_LE(registered_reads, max_reads);
      EXPECT_EQ(pool_->available_read_buffers(), 0);
      EXPECT_EQ(pool_->overflow_read_buffer_count(), 0);
      leases.clear();
      EXPECT_EQ(pool_->available_read_buffers(), pool_->read_buffer_count());
    }
  }

  rlimit original_limit_{};
  bool limit_saved_ = false;
  bycorf::CurrentWorker saved_context_;
  std::unique_ptr<bycorf::CrossCore> cross_core_;
  std::unique_ptr<bycorf::Worker> worker_;
  std::unique_ptr<RegisteredBufferPool> pool_;
  RegisteredBufferPoolOptions options_;
  int wake_fd_ = -1;
  char path_[64] = "/tmp/lavik-buffer-memlock-XXXXXX";
};

TEST_F(BufferPoolMemlockTest, FullRegistrationPreservesConfiguredPool) {
  ASSERT_NO_FATAL_FAILURE(Init(12 * kMiB));
  if (IsSkipped()) return;
  ASSERT_TRUE(pool_->buffers_registered());
  ASSERT_NO_FATAL_FAILURE(CheckIo(2, 341, 341));
}

TEST_F(BufferPoolMemlockTest, DefaultSlotIncludesFramingAndUsesIndexZero) {
  options_.read_slot_bytes_ = RegisteredBufferPoolOptions{}.read_slot_bytes_;
  ASSERT_EQ(options_.read_slot_bytes_, 32 * kKiB);
  ASSERT_EQ(options_.read_payload_bytes(), 24 * kKiB);
  ASSERT_NO_FATAL_FAILURE(Init(12 * kMiB));
  if (IsSkipped()) return;
  ASSERT_TRUE(pool_->buffers_registered());
  ASSERT_NO_FATAL_FAILURE(CheckIo(2, 128, 128));

  auto ordinary = pool_->AcquireReadBuffer(24 * kKiB).await_resume();
  ASSERT_TRUE(ordinary.ok());
  EXPECT_EQ(ordinary->bytes().size(), 32 * kKiB);
  EXPECT_EQ(ordinary->io_buffer().size_, 24 * kKiB);
  EXPECT_EQ(ordinary->io_buffer().index_, 0);
  EXPECT_TRUE(ordinary->registered());
  auto overflow = pool_->AcquireReadBuffer(24 * kKiB + 1).await_resume();
  ASSERT_TRUE(overflow.ok());
  EXPECT_EQ(overflow->buffer_id(), 0);
  EXPECT_FALSE(overflow->registered());
  EXPECT_GE(overflow->io_buffer().size_, 24 * kKiB + 1);
}

TEST_F(BufferPoolMemlockTest, LowMemlockPrioritizesReadSlots) {
  ASSERT_NO_FATAL_FAILURE(Init(2 * kMiB));
  if (IsSkipped()) return;
  ASSERT_FALSE(pool_->buffers_registered());
  ASSERT_NO_FATAL_FAILURE(CheckIo(0, 1, 170));
}

TEST_F(BufferPoolMemlockTest, RemainingBudgetRegistersSomeWriteSlots) {
  ASSERT_NO_FATAL_FAILURE(Init(6 * kMiB + 512 * kKiB));
  if (IsSkipped()) return;
  ASSERT_FALSE(pool_->buffers_registered());
  ASSERT_NO_FATAL_FAILURE(CheckIo(1, 341, 341));
}

TEST_F(BufferPoolMemlockTest, RetryBudgetAccountsForOtherWorkers) {
  // Only worker 0 is needed to verify that the retry uses one quarter of the
  // allowance.
  ASSERT_NO_FATAL_FAILURE(Init(4 * kMiB, 4));
  if (IsSkipped()) return;
  ASSERT_FALSE(pool_->buffers_registered());
  ASSERT_NO_FATAL_FAILURE(CheckIo(0, 1, 85));
}

TEST_F(BufferPoolMemlockTest, InsufficientMemlockKeepsWholeUnregisteredPool) {
  ASSERT_NO_FATAL_FAILURE(Init(0));
  if (IsSkipped()) return;
  ASSERT_FALSE(pool_->buffers_registered());
  ASSERT_NO_FATAL_FAILURE(CheckIo(0, 0, 0));
}

TEST_F(BufferPoolMemlockTest, MoreSlotsThanKernelRegistrationEntries) {
  // More than Linux's 16384 registered entries, but only one read arena entry.
  // Exercise the final slot too: a truncated registration must not pass this.
  constexpr unsigned kSlots = 16400;
  options_.registered_bytes_ = 4 * kMiB + kSlots * 12 * kKiB;
  ASSERT_NO_FATAL_FAILURE(Init(256 * kMiB));
  if (IsSkipped()) return;
  ASSERT_TRUE(pool_->buffers_registered());
  ASSERT_NO_FATAL_FAILURE(CheckIo(2, kSlots, kSlots));
}

TEST_F(BufferPoolMemlockTest, LargePoolSpansMultipleRegisteredAllocations) {
  options_.registered_bytes_ = 1024 * kMiB + 32 * kMiB;
  options_.read_slot_bytes_ = kMiB + 8 * kKiB;
  const unsigned slots =
      (options_.registered_bytes_ - 4 * kMiB) / (kMiB + 8 * kKiB);
  ASSERT_NO_FATAL_FAILURE(Init(1152 * kMiB));
  if (IsSkipped()) return;
  ASSERT_TRUE(pool_->buffers_registered());
  // Reads span two allocations; both write slots share the second allocation
  // with its remaining reads. Kernel I/O verifies indices on both sides.
  ASSERT_NO_FATAL_FAILURE(CheckIo(2, slots, slots, 1));
}

TEST_F(BufferPoolMemlockTest, PartialRegistrationSpansMultipleAllocations) {
  options_.registered_bytes_ = 1024 * kMiB + 32 * kMiB;
  options_.read_slot_bytes_ = kMiB + 8 * kKiB;
  const unsigned slots =
      (options_.registered_bytes_ - 4 * kMiB) / (kMiB + 8 * kKiB);
  ASSERT_NO_FATAL_FAILURE(Init(1055 * kMiB));
  if (IsSkipped()) return;
  ASSERT_FALSE(pool_->buffers_registered());
  // The last region contains both registered and ordinary write slots, after
  // all reads. Sharing its registration index must not register the tail slot.
  ASSERT_NO_FATAL_FAILURE(CheckIo(1, slots, slots, 1));
}

}  // namespace
}  // namespace lavik::storage
