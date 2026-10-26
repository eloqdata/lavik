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
#include <sys/mman.h>
#include <sys/prctl.h>
#include <sys/resource.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "absl/cleanup/cleanup.h"
#include "absl/status/status.h"
#include "bycorf/net/server.h"
#include "bycorf/runtime/task.h"
#include "bycorf/runtime/worker.h"
#include "gtest/gtest.h"
#include "support/read_buffer_fault.h"

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
    options.registered_bytes_ = 24 * kMiB;
    options.storage_write_buffer_count_ = 2;
    options.write_buffer_bytes_ = 8 * kMiB;
    result_ = pool_.Init(worker, options);
    if (!result_.ok()) {
      worker.RequestStop();
      co_return result_;
    }

    std::uint16_t other_storage = 0;
    if (!pool_.TryAcquireWriteBuffer(&other_storage)) {
      result_ = absl::FailedPreconditionError(
          "test could not reserve the other storage write buffer");
      worker.RequestStop();
      co_return result_;
    }
    absl::Cleanup release_other = [&] {
      pool_.ReleaseWriteBuffer(other_storage);
    };
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
    {
      read_waiter_started_ = false;
      read_waiter_acquired_ = false;
      worker.Spawn(AcquireReadAfterRelease());
      co_await bycorf::Yield(worker);
      if (!read_waiter_started_ || read_waiter_acquired_ ||
          pool_.output_buffer_count() != 0) {
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
      if (!read_waiter_acquired_ || pool_.output_buffer_count() != 0) {
        result_ = absl::DeadlineExceededError(
            "fixed-read-buffer release did not wake its waiter");
        worker.RequestStop();
        co_return result_;
      }

      auto oversized =
          co_await pool_.AcquireReadBuffer(pool_.read_capacity_bytes());
      if (!absl::IsResourceExhausted(oversized.status())) {
        result_ = absl::FailedPreconditionError(
            "oversized allocation failure waited for an unusable fixed slot");
      }
      worker.RequestStop();
      co_return result_;
    }
  }

  void Stop() noexcept override {}

  const absl::Status& result() const noexcept { return result_; }

 private:
  bycorf::Task<absl::Status> AcquireStorageAfterRelease() {
    storage_waiter_started_ = true;
    const auto acquired = co_await pool_.AcquireWriteBuffer();
    if (!acquired.ok()) co_return acquired.status();
    storage_waiter_acquired_ = true;
    pool_.ReleaseWriteBuffer(*acquired);
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
  ASSERT_TRUE(InitMemoryLimit(1024 * kMiB, 1).ok());
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

TEST(BufferPoolTest, RejectsFewerThanTwoWriteSlotsBeforeAllocation) {
  bycorf::Worker worker;
  RegisteredBufferPool pool;
  RegisteredBufferPoolOptions options;
  for (const std::size_t count : {0, 1}) {
    options.storage_write_buffer_count_ = count;
    const auto status = pool.Init(worker, options);
    EXPECT_TRUE(absl::IsInvalidArgument(status)) << status;
    EXPECT_FALSE(pool.initialized());
    EXPECT_EQ(pool.available_write_buffers(), 0);
  }
}

TEST(BufferPoolTest, ReadAllocationFailureWaitsForFixedSlot) {
#if !LAVIK_FAULTS_ENABLED
  GTEST_SKIP() << "requires read buffer allocation failure injection";
#endif
  lavik::test::ScopedReadBufferAllocationFailure failure;
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
    // Exact memlock boundaries require base pages. Pinning an 8 KiB slice of
    // a transparent huge page can charge the entire huge page on some kernels.
    // The production fallback permits that extra charge; these tests isolate
    // registration priority from the host's THP policy.
    original_thp_disabled_ = prctl(PR_GET_THP_DISABLE, 0, 0, 0, 0);
    ASSERT_GE(original_thp_disabled_, 0);
    ASSERT_EQ(prctl(PR_SET_THP_DISABLE, 1, 0, 0, 0), 0);
    const int rc = getrlimit(RLIMIT_MEMLOCK, &original_limit_);
    limit_saved_ = rc == 0;
    ASSERT_EQ(rc, 0);
    saved_context_ = bycorf::MutableThisWorker();
    int fd = mkstemp(path_);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(ftruncate(fd, 8 * kKiB), 0);
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
    for (const auto& buffer : device_buffers_)
      EXPECT_EQ(munmap(buffer.data_, buffer.size_), 0);
    if (wake_fd_ >= 0) close(wake_fd_);
    unlink(path_);
    if (limit_saved_) EXPECT_EQ(setrlimit(RLIMIT_MEMLOCK, &original_limit_), 0);
    if (original_thp_disabled_ >= 0)
      EXPECT_EQ(prctl(PR_SET_THP_DISABLE, original_thp_disabled_, 0, 0, 0), 0);
    bycorf::MutableThisWorker() = std::move(saved_context_);
  }

  void Init(rlim_t limit, unsigned runtime_workers = 1, bool init_pool = true) {
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
    if (pool_ == nullptr) pool_ = std::make_unique<RegisteredBufferPool>();
    if (!init_pool) return;
    auto status = pool_->Init(*worker_, options_, device_buffers_);
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

  void AddDeviceBuffers(unsigned count, std::size_t bytes = 8 * kKiB) {
    for (unsigned i = 0; i < count; ++i) {
      // A fresh mapping cannot inherit a heap page collapsed before SetUp
      // disabled THP. Ownership stays here, just as it does in the allocator
      // that lends device workspaces to the production pool.
      auto* data =
          static_cast<std::byte*>(mmap(nullptr, bytes, PROT_READ | PROT_WRITE,
                                       MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
      ASSERT_NE(data, MAP_FAILED);
      device_buffers_.push_back({.data_ = data, .size_ = bytes});
    }
  }

  void CheckWorkspaceIo(bycorf::FixedBuffer buffer, bool registered) {
    ASSERT_EQ(pool_->workspace_registered(buffer), registered);
    // Exercise the complete two-page header and then a metadata-page-sized
    // subrange, including fixed index zero and fallback to ordinary I/O.
    std::fill_n(buffer.data_, 8 * kKiB, std::byte{0x37});
    auto write = buffer;
    write.size_ = 8 * kKiB;
    Completion written;
    auto status = registered
                      ? worker_->SubmitWriteFixed({0}, write, 0, &written)
                      : worker_->SubmitWrite({0}, {write.data_, write.size_}, 0,
                                             &written);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_NO_FATAL_FAILURE(Drain(written));
    ASSERT_EQ(written.result, 8 * kKiB);
    auto read = buffer;
    read.data_ += 4096;
    read.size_ = 4096;
    ASSERT_EQ(pool_->workspace_registered(read), registered);
    std::fill_n(read.data_, read.size_, std::byte{0});
    Completion received;
    status = registered ? worker_->SubmitReadFixed({0}, read, 4096, &received)
                        : worker_->SubmitRead({0}, {read.data_, read.size_},
                                              4096, &received);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_NO_FATAL_FAILURE(Drain(received));
    ASSERT_EQ(received.result, 4096);
    EXPECT_TRUE(std::all_of(read.data_, read.data_ + read.size_,
                            [](std::byte b) { return b == std::byte{0x37}; }));
  }

  // Exercise actual O_DIRECT I/O through both registered and ordinary paths.
  // Holding every read lease checks that pool capacity did not silently shrink;
  // Releasing and reacquiring checks that a shared registration index never
  // gets mistaken for a slot's release id.
  void CheckIo(unsigned expected_writes, unsigned min_reads, unsigned max_reads,
               unsigned last_region = 0, unsigned first_region = 0) {
    unsigned registered_writes = 0;
    for (unsigned i = 0; i < pool_->write_buffer_count(); ++i) {
      std::uint16_t id = 0;
      ASSERT_TRUE(pool_->TryAcquireWriteBuffer(&id));
      auto buffer = pool_->write_buffer(id);
      EXPECT_EQ(buffer.index_, last_region + id);
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
        ASSERT_EQ(lease.buffer_id(),
                  1 + i * options_.read_slot_bytes_ / options_.alignment_);
        if (round == 0) {
          slot_addresses[i] = lease.bytes().data();
          if (i == 0) EXPECT_EQ(buffer.index_, first_region);
          if (i + 1 == pool_->read_buffer_count())
            EXPECT_EQ(buffer.index_, last_region);
          if (i != 0 && buffer.index_ == leases[i - 1].io_buffer().index_) {
            EXPECT_EQ(lease.bytes().data(),
                      slot_addresses[i - 1] + leases[i - 1].bytes().size());
          }
        } else {
          EXPECT_EQ(lease.bytes().data(), slot_addresses[i]);
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
      EXPECT_EQ(pool_->output_buffer_count(), 0);
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
  std::vector<bycorf::FixedBuffer> device_buffers_;
  int original_thp_disabled_ = -1;
  RegisteredBufferPoolOptions options_;
  int wake_fd_ = -1;
  char path_[64] = "/tmp/lavik-buffer-memlock-XXXXXX";
};

TEST_F(BufferPoolMemlockTest, MaintenanceReadsDoNotBorrowExhaustedPools) {
  ASSERT_TRUE(InitMemoryLimit(1024 * kMiB, 1).ok());
  ASSERT_NO_FATAL_FAILURE(Init(0));
  const auto retained = WorkerMemoryAccountingBytes(0);
  std::vector<ReadBufferLease> held_reads;
  while (pool_->available_read_buffers() != 0) {
    auto acquired = pool_->AcquireReadBuffer().await_resume();
    ASSERT_TRUE(acquired.ok());
    held_reads.push_back(std::move(*acquired));
  }
  std::vector<std::uint16_t> held_writes;
  while (pool_->available_write_buffers() != 0) {
    std::uint16_t id = 0;
    ASSERT_TRUE(pool_->TryAcquireWriteBuffer(&id));
    held_writes.push_back(id);
  }
  absl::Cleanup release_writes = [&] {
    for (const auto id : held_writes) pool_->ReleaseWriteBuffer(id);
  };
  constexpr std::array kinds{MaintenanceReadKind::kDefrag,
                             MaintenanceReadKind::kTxCleaner,
                             MaintenanceReadKind::kTombRaider};
  std::array<bycorf::FixedBuffer, 3> scans;
  std::array<Completion, 3> reads;
  for (std::size_t i = 0; i < kinds.size(); ++i) {
    scans[i] = pool_->maintenance_read_buffer(kinds[i]);
    ASSERT_EQ(scans[i].size_, 8 * kMiB);
    ASSERT_NE(scans[i].data_, nullptr);
    EXPECT_EQ(
        reinterpret_cast<std::uintptr_t>(scans[i].data_) % options_.alignment_,
        0);
    // Hold all three images concurrently, including across real direct I/O.
    // Reusing a write/read slot or another scanner's memory corrupts a tail.
    std::fill_n(scans[i].data_, scans[i].size_, std::byte(i + 1));
    for (const auto& lease : held_reads)
      EXPECT_NE(scans[i].data_, lease.bytes().data());
    for (const auto id : held_writes)
      EXPECT_NE(scans[i].data_, pool_->write_buffer(id).data_);
    ASSERT_TRUE(
        worker_->SubmitRead({0}, {scans[i].data_, 4096}, 0, &reads[i]).ok());
  }
  for (std::size_t i = 0; i < kinds.size(); ++i) {
    ASSERT_NO_FATAL_FAILURE(Drain(reads[i]));
    ASSERT_EQ(reads[i].result, 4096);
    EXPECT_EQ(scans[i].data_[0], std::byte{0});
    EXPECT_EQ(scans[i].data_[scans[i].size_ - 1], std::byte(i + 1));
    EXPECT_EQ(pool_->maintenance_read_buffer(kinds[i]).data_, scans[i].data_);
  }
  EXPECT_EQ(pool_->available_write_buffers(), 0);
  EXPECT_EQ(pool_->available_read_buffers(), 0);
  EXPECT_EQ(pool_->output_buffer_count(), 0);
  EXPECT_EQ(WorkerMemoryAccountingBytes(0), retained);
}

TEST_F(BufferPoolMemlockTest,
       MaintenanceAllocationFailureNamesTaskAndRollsBack) {
#if LAVIK_FAULTS_ENABLED
  ASSERT_TRUE(InitMemoryLimit(1024 * kMiB, 1).ok());
  ASSERT_NO_FATAL_FAILURE(Init(0, 1, false));
  const auto retained = WorkerMemoryAccountingBytes(0);
  constexpr const char* variable = "LAVIK_FAIL_MAINTENANCE_BUFFER_ALLOCATION";
  std::optional<std::string> previous;
  if (const auto* value = std::getenv(variable)) previous = value;
  absl::Cleanup restore = [&] {
    if (previous)
      ::setenv(variable, previous->c_str(), 1);
    else
      ::unsetenv(variable);
  };
  for (const auto* name : {"defrag", "tx-cleaner", "tomb-raider"}) {
    ASSERT_EQ(::setenv(variable, name, 1), 0);
    const auto status = pool_->Init(*worker_, options_);
    EXPECT_TRUE(absl::IsResourceExhausted(status)) << status;
    EXPECT_NE(status.message().find(name), std::string_view::npos);
    EXPECT_NE(status.message().find("worker 0"), std::string_view::npos);
    EXPECT_NE(status.message().find("8388608 bytes"), std::string_view::npos);
    EXPECT_FALSE(pool_->initialized());
    EXPECT_EQ(pool_->available_write_buffers(), 0);
    EXPECT_EQ(WorkerMemoryAccountingBytes(0), retained);
  }
  ASSERT_EQ(::unsetenv(variable), 0);
  EXPECT_TRUE(pool_->Init(*worker_, options_).ok());
#else
  GTEST_SKIP() << "allocation failure test requires fault hooks";
#endif
}

TEST_F(BufferPoolMemlockTest, PreparedMaintenanceSurvivesWorkerInitialization) {
#if !LAVIK_FAULTS_ENABLED
  GTEST_SKIP() << "requires allocation failure injection";
#endif
  ASSERT_TRUE(InitMemoryLimit(1024 * kMiB, 1).ok());
  const auto baseline = WorkerMemoryAccountingBytes(0);
  pool_ = std::make_unique<RegisteredBufferPool>();
  ASSERT_TRUE(pool_->PrepareMaintenanceBuffers(0, options_.alignment_).ok());
  ASSERT_FALSE(pool_->initialized());
  constexpr std::array kinds{MaintenanceReadKind::kDefrag,
                             MaintenanceReadKind::kTxCleaner,
                             MaintenanceReadKind::kTombRaider};
  std::array<std::byte*, 3> images;
  for (std::size_t i = 0; i != kinds.size(); ++i) {
    images[i] = pool_->maintenance_read_buffer(kinds[i]).data_;
    images[i][0] = std::byte(i + 1);
  }
  EXPECT_EQ(WorkerMemoryAccountingBytes(0), baseline + 24 * kMiB);
  EXPECT_TRUE(absl::IsFailedPrecondition(
      pool_->PrepareMaintenanceBuffers(1, options_.alignment_)));
  EXPECT_TRUE(absl::IsFailedPrecondition(
      pool_->PrepareMaintenanceBuffers(0, options_.alignment_ * 2)));
  ASSERT_EQ(setenv("LAVIK_FAIL_MAINTENANCE_BUFFER_ALLOCATION", "defrag", 1), 0);
  absl::Cleanup restore = [] {
    unsetenv("LAVIK_FAIL_MAINTENANCE_BUFFER_ALLOCATION");
  };
  ASSERT_NO_FATAL_FAILURE(Init(0));
  for (std::size_t i = 0; i != kinds.size(); ++i) {
    EXPECT_EQ(pool_->maintenance_read_buffer(kinds[i]).data_, images[i]);
    EXPECT_EQ(images[i][0], std::byte(i + 1));
  }
  EXPECT_EQ(WorkerMemoryAccountingBytes(0),
            baseline + 24 * kMiB +
                pool_->write_buffer_count() * options_.write_buffer_bytes_ +
                pool_->read_capacity_bytes() +
                pool_->read_capacity_bytes() / options_.alignment_ *
                    sizeof(TlsfRangeAllocator::Metadata) +
                sizeof(TlsfRangeAllocator));
  worker_->Shutdown();
  worker_.reset();
  bycorf::MutableThisWorker() = saved_context_;
  pool_.reset();
  EXPECT_EQ(WorkerMemoryAccountingBytes(0), baseline);
}

TEST_F(BufferPoolMemlockTest, AccountsArenaMetadataAndHeapOutputs) {
  ASSERT_TRUE(InitMemoryLimit(1024 * kMiB, 2).ok());
  const auto baseline = WorkerMemoryAccountingBytes(0);
  const auto other_baseline = WorkerMemoryAccountingBytes(1);
  ASSERT_NO_FATAL_FAILURE(Init(0, 2));
  const auto fixed_bytes =
      pool_->write_buffer_count() * options_.write_buffer_bytes_ +
      pool_->read_capacity_bytes() +
      pool_->read_capacity_bytes() / options_.alignment_ *
          sizeof(TlsfRangeAllocator::Metadata) +
      sizeof(TlsfRangeAllocator) + 24 * kMiB;
  const auto fixed_total = baseline + fixed_bytes;
  EXPECT_EQ(WorkerMemoryAccountingBytes(0), fixed_total);
  EXPECT_EQ(WorkerMemoryAccountingBytes(1), other_baseline);

  std::uint16_t write_id = 0;
  ASSERT_TRUE(pool_->TryAcquireWriteBuffer(&write_id));
  auto read = pool_->AcquireReadBuffer().await_resume();
  ASSERT_TRUE(read.ok());
  EXPECT_EQ(WorkerMemoryAccountingBytes(0), fixed_total);
  read->Reset();
  pool_->ReleaseWriteBuffer(write_id);
  EXPECT_EQ(WorkerMemoryAccountingBytes(0), fixed_total);

  // I/O needed for reads or reclamation remains available when retained
  // admission is closed. Its capacity must still appear in the counters.
  ASSERT_TRUE(InitMemoryLimit(1, 2).ok());
  struct RestoreLimit {
    ~RestoreLimit() { (void)InitMemoryLimit(1024 * kMiB, 2); }
  } restore_limit;
  auto first = pool_->AllocateReadOutputBuffer(kMiB);
  auto second = pool_->AllocateReadOutputBuffer(2 * kMiB);
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(second.ok());
  const auto second_bytes = second->bytes().size();
  EXPECT_EQ(pool_->output_buffer_count(), 2);
  EXPECT_EQ(WorkerMemoryAccountingBytes(0),
            fixed_total + first->bytes().size() + second_bytes);
  first->Reset();
  EXPECT_EQ(pool_->output_buffer_count(), 1);
  EXPECT_EQ(WorkerMemoryAccountingBytes(0), fixed_total + second_bytes);

  // A remote return only queues the release. Keep its storage and charge until
  // the owner consumes that message, even if an earlier slot is reused.
  bycorf::SetThisWorker(1, cross_core_.get(), nullptr);
  second->Reset();
  bycorf::SetThisWorker(0, cross_core_.get(), worker_.get());
  EXPECT_EQ(WorkerMemoryAccountingBytes(0), fixed_total + second_bytes);
  auto reused_read = pool_->AllocateReadOutputBuffer(3 * kMiB);
  ASSERT_TRUE(reused_read.ok());
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (pool_->output_buffer_count() != 1 &&
         std::chrono::steady_clock::now() < deadline) {
    worker_->RunOnce(false);
  }
  EXPECT_EQ(pool_->output_buffer_count(), 1);
  EXPECT_EQ(WorkerMemoryAccountingBytes(0),
            fixed_total + reused_read->bytes().size());
  reused_read->Reset();
  EXPECT_EQ(pool_->output_buffer_count(), 0);
  EXPECT_EQ(WorkerMemoryAccountingBytes(0), fixed_total);
  EXPECT_EQ(WorkerMemoryAccountingBytes(1), other_baseline);

  worker_->Shutdown();
  worker_.reset();
  bycorf::MutableThisWorker() = saved_context_;
  pool_.reset();
  EXPECT_EQ(WorkerMemoryAccountingBytes(0), baseline);
  EXPECT_EQ(WorkerMemoryAccountingBytes(1), other_baseline);
}

TEST_F(BufferPoolMemlockTest, WriteSlotsTransferInFifoOrderWithoutBarging) {
  ASSERT_NO_FATAL_FAILURE(Init(0));
  std::uint16_t held = 0, other = 0;
  ASSERT_TRUE(pool_->TryAcquireWriteBuffer(&held));
  ASSERT_TRUE(pool_->TryAcquireWriteBuffer(&other));
  std::vector<unsigned> order;
  auto acquire = [&](unsigned position) -> bycorf::Task<std::uint16_t> {
    const auto id = co_await pool_->AcquireWriteBuffer();
    order.push_back(position);
    EXPECT_TRUE(id.ok()) << id.status();
    co_return id.value_or(0);
  };
  auto first = acquire(1);
  auto second = acquire(2);
  auto first_handle = std::move(first).ReleaseHandle();
  first = bycorf::Task<std::uint16_t>(first_handle);
  auto second_handle = std::move(second).ReleaseHandle();
  second = bycorf::Task<std::uint16_t>(second_handle);
  first_handle.resume();
  second_handle.resume();
  ASSERT_FALSE(first.done());
  ASSERT_FALSE(second.done());
  ASSERT_TRUE(pool_->has_write_waiters());

  pool_->ReleaseWriteBuffer(held);
  std::uint16_t stolen = 0;
  EXPECT_FALSE(pool_->TryAcquireWriteBuffer(&stolen));
  EXPECT_EQ(pool_->available_write_buffers(), 0);
  auto third = acquire(3);
  auto third_handle = std::move(third).ReleaseHandle();
  third = bycorf::Task<std::uint16_t>(third_handle);
  third_handle.resume();
  ASSERT_FALSE(third.done());

  auto finish = [&](auto& task, unsigned position) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!task.done() && std::chrono::steady_clock::now() < deadline)
      worker_->RunOnce(false);
    ASSERT_TRUE(task.done());
    EXPECT_EQ(order.back(), position);
    EXPECT_EQ(std::move(task).TakeResult(), held);
    pool_->ReleaseWriteBuffer(held);
  };
  ASSERT_NO_FATAL_FAILURE(finish(first, 1));
  EXPECT_FALSE(pool_->TryAcquireWriteBuffer(&stolen));
  ASSERT_NO_FATAL_FAILURE(finish(second, 2));
  EXPECT_FALSE(pool_->TryAcquireWriteBuffer(&stolen));
  ASSERT_NO_FATAL_FAILURE(finish(third, 3));
  EXPECT_EQ(order, (std::vector<unsigned>{1, 2, 3}));
  EXPECT_FALSE(pool_->has_write_waiters());
  EXPECT_EQ(pool_->available_write_buffers(), 1);
  pool_->ReleaseWriteBuffer(other);
  EXPECT_EQ(pool_->available_write_buffers(), 2);
}

TEST_F(BufferPoolMemlockTest, FailedFlushWakesQueuedAndAssignedWriters) {
  ASSERT_NO_FATAL_FAILURE(Init(0));
  std::uint16_t first_slot = 0, second_slot = 0;
  ASSERT_TRUE(pool_->TryAcquireWriteBuffer(&first_slot));
  ASSERT_TRUE(pool_->TryAcquireWriteBuffer(&second_slot));
  auto acquire = [&]() -> bycorf::Task<absl::Status> {
    auto acquired = co_await pool_->AcquireWriteBuffer();
    co_return acquired.status();
  };
  auto first = acquire();
  auto second = acquire();
  auto first_handle = std::move(first).ReleaseHandle();
  first = decltype(first)(first_handle);
  auto second_handle = std::move(second).ReleaseHandle();
  second = decltype(second)(second_handle);
  first_handle.resume();
  second_handle.resume();
  // Fail after the first handoff but before its continuation runs. That slot
  // is safe to return; the other remains owned by the failed staging block.
  pool_->ReleaseWriteBuffer(first_slot);
  pool_->FailWriteWaiters();
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while ((!first.done() || !second.done()) &&
         std::chrono::steady_clock::now() < deadline)
    worker_->RunOnce(false);
  ASSERT_TRUE(first.done());
  ASSERT_TRUE(second.done());
  EXPECT_TRUE(absl::IsFailedPrecondition(std::move(first).TakeResult()));
  EXPECT_TRUE(absl::IsFailedPrecondition(std::move(second).TakeResult()));
  EXPECT_FALSE(pool_->has_write_waiters());
  EXPECT_EQ(pool_->available_write_buffers(), 1);
  std::uint16_t acquired = 0;
  EXPECT_FALSE(pool_->TryAcquireWriteBuffer(&acquired));
  auto after_failure = pool_->AcquireWriteBuffer();
  ASSERT_TRUE(after_failure.await_ready());
  EXPECT_TRUE(
      absl::IsFailedPrecondition(after_failure.await_resume().status()));
  pool_->ReleaseWriteBuffer(second_slot);
  EXPECT_EQ(pool_->available_write_buffers(), 2);
}

TEST_F(BufferPoolMemlockTest, RepeatedReadinessOwnsOneArenaRange) {
  ASSERT_NO_FATAL_FAILURE(Init(0));
  const auto available = pool_->available_read_bytes();
  const auto retained = WorkerMemoryAccountingBytes(0);
  {
    auto acquire = pool_->AcquireReadBuffer(8 * kKiB);
    ASSERT_TRUE(acquire.await_ready());
    ASSERT_TRUE(acquire.await_ready());
    EXPECT_EQ(pool_->available_read_bytes(), available - 16 * kKiB);
    EXPECT_EQ(pool_->output_buffer_count(), 0);
  }
  EXPECT_EQ(pool_->available_read_bytes(), available);
  EXPECT_EQ(WorkerMemoryAccountingBytes(0), retained);
}

TEST_F(BufferPoolMemlockTest,
       VariableRangesCoalesceWithoutChangingRetainedMemory) {
  ASSERT_NO_FATAL_FAILURE(Init(0));
  const auto retained = WorkerMemoryAccountingBytes(0);
  const auto capacity = pool_->read_capacity_bytes();
  auto first = pool_->TryAcquireReadBuffer(1);
  auto second = pool_->TryAcquireReadBuffer(24 * kKiB);
  auto third = pool_->TryAcquireReadBuffer(40 * kKiB);
  ASSERT_TRUE(first.ok() && second.ok() && third.ok());
  EXPECT_EQ(first->bytes().size(), 12 * kKiB);
  EXPECT_EQ(second->bytes().size(), 32 * kKiB);
  EXPECT_EQ(third->bytes().size(), 48 * kKiB);
  const auto* base = first->bytes().data();
  EXPECT_EQ(second->bytes().data(), base + 12 * kKiB);
  EXPECT_EQ(third->bytes().data(), base + 44 * kKiB);
  EXPECT_EQ(pool_->available_read_bytes(), capacity - 92 * kKiB);
  second->Reset();
  first->Reset();
  third->Reset();
  auto whole = pool_->TryAcquireReadBuffer(capacity - 8 * kKiB);
  ASSERT_TRUE(whole.ok());
  EXPECT_EQ(whole->bytes().data(), base);
  EXPECT_EQ(pool_->available_read_bytes(), 0);
  EXPECT_EQ(pool_->output_buffer_count(), 0);
  EXPECT_EQ(WorkerMemoryAccountingBytes(0), retained);
}

TEST_F(BufferPoolMemlockTest, FragmentedArenaShrinksChunksAndRecoversCapacity) {
  options_.registered_bytes_ = 2 * options_.write_buffer_bytes_ + 64 * kKiB;
  options_.read_slot_bytes_ = 32 * kKiB;
  ASSERT_NO_FATAL_FAILURE(Init(0));
  std::vector<ReadBufferLease> held;
  for (unsigned i = 0; i < 4; ++i) {
    auto read = pool_->TryAcquireReadBuffer(4 * kKiB);
    ASSERT_TRUE(read.ok());
    held.push_back(std::move(*read));
  }
  auto tail = pool_->TryAcquireReadBuffer(8 * kKiB);
  ASSERT_TRUE(tail.ok());
  held[0].Reset();
  held[2].Reset();
  ASSERT_EQ(pool_->available_read_bytes(), 24 * kKiB);
  EXPECT_TRUE(absl::IsResourceExhausted(
      pool_->TryAcquireReadBuffer(16 * kKiB).status()));
  auto acquire = pool_->AcquireReadChunk();
  ASSERT_TRUE(acquire.await_ready());
  auto chunk = acquire.await_resume();
  ASSERT_TRUE(chunk.ok());
  EXPECT_EQ(chunk->io_buffer().size_, 4 * kKiB);
  chunk->Reset();
  held.clear();
  tail->Reset();
  auto whole = pool_->TryAcquireReadBuffer(56 * kKiB);
  ASSERT_TRUE(whole.ok());
}

TEST_F(BufferPoolMemlockTest, DifferentSizeReadWaitersCannotBarge) {
  options_.registered_bytes_ = 2 * options_.write_buffer_bytes_ + 24 * kKiB;
  ASSERT_NO_FATAL_FAILURE(Init(0));
  auto held = pool_->TryAcquireReadBuffer(16 * kKiB);
  ASSERT_TRUE(held.ok());
  std::vector<unsigned> order;
  auto acquire =
      [&](unsigned position,
          std::size_t bytes) -> bycorf::Task<absl::StatusOr<ReadBufferLease>> {
    auto lease = co_await pool_->AcquireReadBuffer(bytes);
    order.push_back(position);
    co_return lease;
  };
  auto first = acquire(1, 16 * kKiB);
  auto second = acquire(2, 4 * kKiB);
  auto start = [](auto& task) {
    auto handle = std::move(task).ReleaseHandle();
    task = std::remove_reference_t<decltype(task)>(handle);
    handle.resume();
  };
  start(first);
  start(second);
  EXPECT_FALSE(first.done());
  EXPECT_FALSE(second.done());
  held->Reset();
  EXPECT_FALSE(pool_->TryAcquireReadBuffer(4 * kKiB).ok());
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!first.done() && std::chrono::steady_clock::now() < deadline)
    worker_->RunOnce(false);
  ASSERT_TRUE(first.done());
  EXPECT_FALSE(second.done());
  auto returned = std::move(first).TakeResult();
  ASSERT_TRUE(returned.ok());
  returned->Reset();
  while (!second.done() && std::chrono::steady_clock::now() < deadline)
    worker_->RunOnce(false);
  ASSERT_TRUE(second.done());
  auto smaller = std::move(second).TakeResult();
  ASSERT_TRUE(smaller.ok());
  EXPECT_EQ(order, (std::vector<unsigned>{1, 2}));
}

TEST_F(BufferPoolMemlockTest, RemoteRangeReturnWaitsForItsOwner) {
  ASSERT_NO_FATAL_FAILURE(Init(0, 2));
  const auto capacity = pool_->read_capacity_bytes();
  auto read = pool_->TryAcquireReadBuffer(40 * kKiB);
  ASSERT_TRUE(read.ok());
  bycorf::SetThisWorker(1, cross_core_.get(), nullptr);
  read->Reset();
  bycorf::SetThisWorker(0, cross_core_.get(), worker_.get());
  EXPECT_EQ(pool_->available_read_bytes(), capacity - 48 * kKiB);
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (pool_->available_read_bytes() != capacity &&
         std::chrono::steady_clock::now() < deadline)
    worker_->RunOnce(false);
  EXPECT_EQ(pool_->available_read_bytes(), capacity);
  auto whole = pool_->TryAcquireReadBuffer(capacity - 8 * kKiB);
  ASSERT_TRUE(whole.ok());
}

TEST_F(BufferPoolMemlockTest, AssemblyOutputBypassesStorageAllocationAndSlots) {
#if !LAVIK_FAULTS_ENABLED
  GTEST_SKIP() << "requires read buffer allocation failure injection";
#endif
  ASSERT_TRUE(InitMemoryLimit(1024 * kMiB, 1).ok());
  ASSERT_NO_FATAL_FAILURE(Init(0));
  const auto baseline = WorkerMemoryAccountingBytes(0);
  const auto available = pool_->available_read_buffers();
  lavik::test::ScopedReadBufferAllocationFailure failure;
  EXPECT_TRUE(absl::IsResourceExhausted(
      pool_->TryAcquireReadBuffer(pool_->read_capacity_bytes()).status()));
  for (const auto payload : {std::size_t{1}, kMiB}) {
    auto output = pool_->AllocateReadOutputBuffer(payload);
    ASSERT_TRUE(output.ok()) << output.status();
    EXPECT_EQ(output->buffer_id(), 0);
    EXPECT_FALSE(output->registered());
    EXPECT_GE(output->io_buffer().size_, payload);
    EXPECT_GE(output->headroom_bytes(), 32);
    EXPECT_GE(output->tailroom_bytes(), 2);
    EXPECT_EQ(pool_->available_read_buffers(), available);
    EXPECT_EQ(WorkerMemoryAccountingBytes(0),
              baseline + output->bytes().size());
    auto moved = std::move(*output);
    EXPECT_FALSE(output->valid());
    moved.Reset();
    EXPECT_EQ(WorkerMemoryAccountingBytes(0), baseline);
    EXPECT_EQ(pool_->output_buffer_count(), 0);
  }
}

TEST_F(BufferPoolMemlockTest, AssemblyOutputRemoteReleaseAndMixedIdReuse) {
  ASSERT_TRUE(InitMemoryLimit(1024 * kMiB, 2).ok());
  ASSERT_NO_FATAL_FAILURE(Init(0, 2));
  const auto baseline = WorkerMemoryAccountingBytes(0);
  const auto other_baseline = WorkerMemoryAccountingBytes(1);
  // Reuse one dynamic ID across both allocators. Its current allocation must
  // determine the free operation, not the allocation that created the ID.
  auto disk = pool_->AcquireReadBuffer(kMiB).await_resume();
  ASSERT_TRUE(disk.ok());
  disk->Reset();
  auto output = pool_->AllocateReadOutputBuffer(2 * kMiB);
  ASSERT_TRUE(output.ok());
  const auto output_bytes = output->bytes().size();
  bycorf::SetThisWorker(1, cross_core_.get(), nullptr);
  output->Reset();
  bycorf::SetThisWorker(0, cross_core_.get(), worker_.get());
  EXPECT_EQ(WorkerMemoryAccountingBytes(0), baseline + output_bytes);
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (pool_->output_buffer_count() != 0 &&
         std::chrono::steady_clock::now() < deadline) {
    worker_->RunOnce(false);
  }
  EXPECT_EQ(pool_->output_buffer_count(), 0);
  EXPECT_EQ(WorkerMemoryAccountingBytes(0), baseline);
  auto reused = pool_->AcquireReadBuffer(kMiB).await_resume();
  ASSERT_TRUE(reused.ok());
  reused->Reset();
  EXPECT_EQ(WorkerMemoryAccountingBytes(0), baseline);
  EXPECT_EQ(WorkerMemoryAccountingBytes(1), other_baseline);
}

TEST_F(BufferPoolMemlockTest,
       AssemblyOutputRejectsInvalidSizesWithoutCharging) {
  RegisteredBufferPool uninitialized;
  EXPECT_TRUE(absl::IsFailedPrecondition(
      uninitialized.AllocateReadOutputBuffer(1).status()));
  ASSERT_TRUE(InitMemoryLimit(1024 * kMiB, 1).ok());
  ASSERT_NO_FATAL_FAILURE(Init(0));
  const auto baseline = WorkerMemoryAccountingBytes(0);
  for (const auto bytes :
       {std::numeric_limits<std::size_t>::max(),
        std::size_t{std::numeric_limits<std::uint32_t>::max()}}) {
    EXPECT_TRUE(
        absl::IsOutOfRange(pool_->AllocateReadOutputBuffer(bytes).status()));
  }
  EXPECT_EQ(pool_->output_buffer_count(), 0);
  EXPECT_EQ(WorkerMemoryAccountingBytes(0), baseline);
}

TEST_F(BufferPoolMemlockTest, EmptyReadArenaRejectsStartup) {
  ASSERT_NO_FATAL_FAILURE(Init(0, 1, false));
  options_.registered_bytes_ = 2 * options_.write_buffer_bytes_;
  const auto status = pool_->Init(*worker_, options_);
  EXPECT_TRUE(absl::IsInvalidArgument(status)) << status;
  EXPECT_FALSE(pool_->initialized());
}

TEST_F(BufferPoolMemlockTest, DeviceWorkspacesPrecedeReadsAndStayWhole) {
  // Kernels that charge the io_uring queues to memlock need headroom outside
  // the registered buffers. Scale these workspaces so one fits with queue
  // overhead, but two cannot fit even on kernels that charge only buffers.
  ASSERT_NO_FATAL_FAILURE(AddDeviceBuffers(2, kMiB));
  ASSERT_NO_FATAL_FAILURE(Init(kMiB + 512 * kKiB));
  if (IsSkipped()) return;
  ASSERT_NO_FATAL_FAILURE(CheckWorkspaceIo(device_buffers_[0], true));
  ASSERT_NO_FATAL_FAILURE(CheckWorkspaceIo(device_buffers_[1], false));
  ASSERT_NO_FATAL_FAILURE(CheckIo(0, 0, 0, 2, 2));
  // A valid index from another allocation does not authorize fixed I/O.
  auto wrong_index = device_buffers_[1];
  wrong_index.index_ = device_buffers_[0].index_;
  EXPECT_FALSE(pool_->workspace_registered(wrong_index));
}

TEST_F(BufferPoolMemlockTest, DeviceWorkspacesThenReadSlotsUseRemainingBudget) {
  options_.read_slot_bytes_ = 256 * kKiB;
  ASSERT_NO_FATAL_FAILURE(AddDeviceBuffers(2, kMiB));
  // Spare memlock covers the ring itself, but is smaller than another slot.
  ASSERT_NO_FATAL_FAILURE(
      Init(2 * kMiB + 2 * options_.read_slot_bytes_ + 128 * kKiB));
  if (IsSkipped()) return;
  for (const auto& buffer : device_buffers_)
    ASSERT_NO_FATAL_FAILURE(CheckWorkspaceIo(buffer, true));
  ASSERT_NO_FATAL_FAILURE(CheckIo(0, 2, 2, 2, 2));
}

TEST_F(BufferPoolMemlockTest, MaintenanceRegistersAfterAllOrdinarySlots) {
  ASSERT_TRUE(InitMemoryLimit(1024 * kMiB, 1).ok());
  const auto baseline = WorkerMemoryAccountingBytes(0);
  ASSERT_NO_FATAL_FAILURE(AddDeviceBuffers(2));
  const auto addresses = device_buffers_;
  // Leave room for kernel ring accounting, but not a second 8 MiB workspace.
  ASSERT_NO_FATAL_FAILURE(Init(16 * kMiB + 512 * kKiB));
  if (IsSkipped()) return;
  ASSERT_TRUE(pool_->buffers_registered());
  ASSERT_NO_FATAL_FAILURE(CheckIo(2, 341, 341, 2, 2));
  for (std::size_t i = 0; i < device_buffers_.size(); ++i) {
    EXPECT_EQ(device_buffers_[i].data_, addresses[i].data_);
    ASSERT_NO_FATAL_FAILURE(CheckWorkspaceIo(device_buffers_[i], true));
  }
  constexpr std::array kinds{MaintenanceReadKind::kDefrag,
                             MaintenanceReadKind::kTxCleaner,
                             MaintenanceReadKind::kTombRaider};
  for (std::size_t i = 0; i < kinds.size(); ++i)
    ASSERT_NO_FATAL_FAILURE(
        CheckWorkspaceIo(pool_->maintenance_read_buffer(kinds[i]), i == 0));
  // Registering caller-owned images neither reallocates nor charges them.
  EXPECT_EQ(WorkerMemoryAccountingBytes(0),
            baseline + 24 * kMiB + 2 * options_.write_buffer_bytes_ +
                pool_->read_capacity_bytes() +
                pool_->read_capacity_bytes() / options_.alignment_ *
                    sizeof(TlsfRangeAllocator::Metadata) +
                sizeof(TlsfRangeAllocator));
}

TEST_F(BufferPoolMemlockTest, AllFixedWorkspacesRegisterWithEnoughMemlock) {
  ASSERT_NO_FATAL_FAILURE(AddDeviceBuffers(2));
  ASSERT_NO_FATAL_FAILURE(Init(40 * kMiB));
  if (IsSkipped()) return;
  for (const auto& buffer : device_buffers_)
    ASSERT_NO_FATAL_FAILURE(CheckWorkspaceIo(buffer, true));
  for (const auto kind :
       {MaintenanceReadKind::kDefrag, MaintenanceReadKind::kTxCleaner,
        MaintenanceReadKind::kTombRaider})
    ASSERT_NO_FATAL_FAILURE(
        CheckWorkspaceIo(pool_->maintenance_read_buffer(kind), true));
  ASSERT_NO_FATAL_FAILURE(CheckIo(2, 341, 341, 2, 2));
}

TEST_F(BufferPoolMemlockTest, DeviceAndMaintenanceWorkspacesAllowZeroMemlock) {
  ASSERT_NO_FATAL_FAILURE(AddDeviceBuffers(2));
  ASSERT_NO_FATAL_FAILURE(Init(0));
  for (const auto& buffer : device_buffers_)
    ASSERT_NO_FATAL_FAILURE(CheckWorkspaceIo(buffer, false));
  for (const auto kind :
       {MaintenanceReadKind::kDefrag, MaintenanceReadKind::kTxCleaner,
        MaintenanceReadKind::kTombRaider})
    ASSERT_NO_FATAL_FAILURE(
        CheckWorkspaceIo(pool_->maintenance_read_buffer(kind), false));
  ASSERT_NO_FATAL_FAILURE(CheckIo(0, 0, 0, 2, 2));
}

TEST_F(BufferPoolMemlockTest, FullRegistrationPreservesConfiguredPool) {
  ASSERT_NO_FATAL_FAILURE(Init(12 * kMiB));
  if (IsSkipped()) return;
  ASSERT_TRUE(pool_->buffers_registered());
  ASSERT_NO_FATAL_FAILURE(CheckIo(2, 341, 341));
}

TEST_F(BufferPoolMemlockTest, VariableRangesIncludeFramingAndUseIndexZero) {
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
  auto larger = pool_->AcquireReadBuffer(24 * kKiB + 1).await_resume();
  ASSERT_TRUE(larger.ok());
  EXPECT_NE(larger->buffer_id(), 0);
  EXPECT_TRUE(larger->registered());
  EXPECT_EQ(larger->io_buffer().size_, 28 * kKiB);
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
  ASSERT_NO_FATAL_FAILURE(CheckIo(2, slots - 1, slots - 1, 1));
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
  ASSERT_NO_FATAL_FAILURE(CheckIo(1, slots - 1, slots - 1, 1));
}

}  // namespace
}  // namespace lavik::storage
