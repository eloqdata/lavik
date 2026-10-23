/*
 * Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <fcntl.h>
#include <unistd.h>

#include "../src/storage/engine/impl.h"
#include "absl/cleanup/cleanup.h"
#include "bycorf/net/server.h"
#include "gtest/gtest.h"
#include "lavik/metrics.h"
#include "lavik/tx/tx_shard.h"
#include "support/test_data_path.h"

namespace lavik::storage {

class RedisExportBufferTestPeer {
 public:
  static void CheckAllocation() {
    ASSERT_TRUE(InitMemoryLimit(1024 * kMiB, 2).ok());
    RefreshMemoryStats();
    const auto before = GetMemoryStats().used_bytes_;
    const auto first_share = GetWorkerMemoryStats(0).retained_bytes_;
    const auto second_share = GetWorkerMemoryStats(1).retained_bytes_;
    {
      StorageEngine::Impl::RedisExportBuffer buffer;
      ASSERT_EQ(setenv("LAVIK_FAIL_REDIS_EXPORT_BUFFER_ALLOCATION", "1", 1), 0);
      absl::Cleanup clear = [] {
        unsetenv("LAVIK_FAIL_REDIS_EXPORT_BUFFER_ALLOCATION");
      };
      const auto failed = buffer.Init();
      EXPECT_TRUE(absl::IsResourceExhausted(failed));
      EXPECT_NE(failed.message().find("Redis export buffer"),
                std::string::npos);
      EXPECT_NE(failed.message().find("8388608 bytes"), std::string::npos);
      EXPECT_EQ(buffer.data_, nullptr);
      EXPECT_EQ(buffer.charge_.bytes(), 0);
      RefreshMemoryStats();
      EXPECT_EQ(GetMemoryStats().used_bytes_, before);
      ASSERT_EQ(unsetenv("LAVIK_FAIL_REDIS_EXPORT_BUFFER_ALLOCATION"), 0);
      ASSERT_TRUE(buffer.Init().ok());
      auto* allocated = buffer.data_;
      ASSERT_NE(allocated, nullptr);
      EXPECT_EQ(
          reinterpret_cast<std::uintptr_t>(allocated) % kDirectIoAlignment, 0);
      ASSERT_TRUE(buffer.Init().ok());
      EXPECT_EQ(buffer.data_, allocated);
      EXPECT_EQ(buffer.charge_.bytes(), kStorageBlockBytes);
      RefreshMemoryStats();
      EXPECT_EQ(GetMemoryStats().used_bytes_, before + kStorageBlockBytes);
      EXPECT_EQ(GetWorkerMemoryStats(0).retained_bytes_,
                first_share + 4 * kMiB);
      EXPECT_EQ(GetWorkerMemoryStats(1).retained_bytes_,
                second_share + 4 * kMiB);
    }
    RefreshMemoryStats();
    EXPECT_EQ(GetMemoryStats().used_bytes_, before);
  }

  static Task<absl::Status> Exercise(StorageEngine& storage,
                                     std::uint64_t session, bool cancel) {
    auto& impl = *storage.impl_;
    auto& store = impl.CurrentStore();
    auto& disk = store.redis_export_disk_backlog_;
    auto& pool = store.buffers_;
    std::vector<ReadBufferLease> reads;
    // Block allocation/return persists bitmap metadata through one ordinary
    // small read slot. Keep it available during capture and cleanup only.
    while (pool.available_read_buffers() > 1) {
      auto read = co_await pool.AcquireReadBuffer();
      if (!read.ok()) co_return read.status();
      reads.push_back(std::move(*read));
    }
    std::vector<std::uint16_t> writes;
    std::uint16_t id = 0;
    while (pool.TryAcquireWriteBuffer(&id)) writes.push_back(id);
    absl::Cleanup release_writes = [&] {
      for (auto slot : writes) pool.ReleaseWriteBuffer(slot);
    };
    auto status =
        co_await storage.StartRedisExportDiskBacklog(session, 32 * kMiB);
    if (!status.ok()) co_return status;
    auto* const buffer = disk.buffer_;
    if (buffer == nullptr || buffer != impl.redis_export_buffer_.data_)
      co_return absl::InternalError("export did not acquire dedicated buffer");
    const unsigned other = 1 - store.worker_->id();
    auto duplicate = co_await bycorf::SubmitTaskTo(other, [&storage, session] {
      return storage.StartRedisExportDiskBacklog(session + 100, 32 * kMiB);
    });
    if (!absl::IsAlreadyExists(duplicate))
      co_return absl::InternalError("another worker acquired active workspace");
    // An unsuccessful competing start/cleanup must not release our lease.
    status = co_await bycorf::SubmitTaskTo(other, [&storage, session] {
      return storage.ReleaseRedisExportDiskBacklog(session + 100);
    });
    if (!status.ok() || !impl.redis_export_buffer_.in_use_.load())
      co_return absl::InternalError(
          "foreign cleanup released active workspace");
    const auto wrong =
        co_await storage.ReleaseRedisExportDiskBacklog(session + 1);
    if (!absl::IsFailedPrecondition(wrong))
      co_return absl::InternalError("wrong session released active workspace");

    // Cross an 8 MiB rollover, leaving the second block unsealed. The buffer
    // must remain usable even when retained growth and every write slot are
    // unavailable; overflow allocation is also faulted by the fixture.
    RetainedMemoryCharge pressure;
    pressure.Account(CurrentMemoryAccountingShard(), 2 * 1024 * kMiB);
    const std::string first(9 * kMiB + 17, 'a');
    const std::string second(4097, 'b');
    status = co_await storage.AppendRedisExportDiskBytes(session, first);
    if (!status.ok()) co_return status;
    status = co_await storage.AppendRedisExportDiskBytes(session, second);
    if (!status.ok()) co_return status;
    if (disk.blocks_.size() != 2 || disk.active_buffer_ != buffer)
      co_return absl::InternalError("export rollover lost its workspace");
    if (!cancel) {
      auto stopped = co_await storage.StopRedisExportDiskBacklog(session);
      if (!stopped.ok()) co_return stopped.status();
      if (*stopped != 3 || disk.active_buffer_ != nullptr)
        co_return absl::InternalError("export stop did not seal capture");
      auto last_read = co_await pool.AcquireReadBuffer();
      if (!last_read.ok()) co_return last_read.status();
      reads.push_back(std::move(*last_read));
      if (pool.available_read_buffers() != 0)
        co_return absl::InternalError("replay fixture did not exhaust reads");
      ReplicationLogCursor next{.lsn_ = 1};
      std::string replayed;
      while (next.lsn_ != *stopped) {
        auto batch = co_await storage.ReadRedisExportDiskBacklog(session, next,
                                                                 64 * kKiB, 1);
        if (!batch.ok()) co_return batch.status();
        for (const auto& frame : batch->frames_) replayed += frame.payload_;
        next = batch->next_;
      }
      if (replayed != first + second)
        co_return absl::DataLossError("export replay changed payload bytes");
      reads.pop_back();
    }
    pressure.Reset();
    status = co_await storage.ReleaseRedisExportDiskBacklog(session);
    if (!status.ok()) co_return status;
    status = co_await storage.ReleaseRedisExportDiskBacklog(session);
    if (!status.ok()) co_return status;
    if (disk.buffer_ != nullptr || disk.active_buffer_ != nullptr ||
        !disk.blocks_.empty() || impl.redis_export_buffer_.in_use_.load() ||
        impl.redis_export_buffer_.data_ != buffer ||
        impl.redis_export_buffer_.charge_.bytes() != kStorageBlockBytes ||
        pool.overflow_read_buffer_count() != 0 ||
        pool.available_read_buffers() != 1 ||
        pool.available_write_buffers() != 0)
      co_return absl::InternalError("export cleanup changed fixed capacity");
    co_return absl::OkStatus();
  }
};

namespace {

class RedisExportBufferService : public bycorf::Service {
 public:
  explicit RedisExportBufferService(StorageEngine& storage)
      : storage_(storage) {}
  void Prepare(unsigned) override {}
  void Stop() noexcept override {}
  void FinalizeWorker(bycorf::Worker& worker) noexcept override {
    storage_.FinalizeWorker(worker);
  }
  Task<absl::Status> Run(bycorf::Worker& worker,
                         bycorf::ServiceContext) override {
    BindMemoryAccountingShard(worker.id());
    tx::TxRuntime::Get()->shard(worker.id()).Bind(worker);
    initialized_[worker.id()] = co_await storage_.InitializeWorker(worker);
    ready_.fetch_add(1, std::memory_order_release);
    if (worker.id() == 0) {
      while (ready_.load(std::memory_order_acquire) != 2)
        co_await bycorf::Yield(worker);
      result_ = initialized_[0].ok() ? initialized_[1] : initialized_[0];
      // Cancel on worker 0, reconnect/replay on worker 1, then reuse on worker
      // 0.
      for (unsigned attempt = 0; result_.ok() && attempt != 3; ++attempt) {
        result_ = co_await bycorf::SubmitTaskTo(attempt % 2, [this, attempt] {
          return RedisExportBufferTestPeer::Exercise(storage_, attempt + 1,
                                                     attempt == 0);
        });
      }
      finished_.store(true, std::memory_order_release);
    } else {
      while (!finished_.load(std::memory_order_acquire))
        co_await bycorf::Yield(worker);
    }
    worker.RequestStop();
    co_return absl::OkStatus();
  }
  StorageEngine& storage_;
  std::array<absl::Status, 2> initialized_;
  std::atomic<unsigned> ready_{0};
  std::atomic<bool> finished_{false};
  absl::Status result_ = absl::UnknownError("test did not run");
};

TEST(RedisExportBufferTest, AllocationFailureAndLifetimeAccounting) {
#if !LAVIK_FAULTS_ENABLED
  GTEST_SKIP() << "requires allocation failure injection";
#endif
  RedisExportBufferTestPeer::CheckAllocation();
}

TEST(RedisExportBufferTest,
     CaptureReplayAndCancelReuseAcrossWorkersWithoutOverflow) {
#if !LAVIK_FAULTS_ENABLED
  GTEST_SKIP() << "requires overflow allocation failure injection";
#endif
  ASSERT_EQ(setenv("LAVIK_FAIL_READ_BUFFER_ALLOCATION", "1", 1), 0);
  absl::Cleanup clear = [] { unsetenv("LAVIK_FAIL_READ_BUFFER_ALLOCATION"); };
  const auto path = test::TestDataPath("lavik-export-buffer-" +
                                       std::to_string(getpid()) + ".data");
  const int fd =
      open(path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(fd, 0);
  absl::Cleanup remove = [&] { unlink(path.c_str()); };
  ASSERT_EQ(ftruncate(fd, 256 * kMiB), 0);
  close(fd);
  InitWorkerMetrics(2);
  ASSERT_TRUE(InitMemoryLimit(1024 * kMiB, 2).ok());
  if (tx::TxRuntime::Get() == nullptr) tx::TxRuntime::Create(2);
  StorageEngineOptions options;
  options.data_files_ = {path};
  options.database_count_ = 1;
  options.expiration_authority_ = false;
  options.defrag_paused_ = true;
  options.tomb_raider_interval_ms_ = 0;
  options.tx_cleaner_cooldown_ms_ = 0;
  StorageEngine storage(std::move(options));
  ASSERT_TRUE(storage.Prepare(2).ok());
  RedisExportBufferService service(storage);
  bycorf::Server server;
  server.AddService(&service);
  bycorf::ServerOptions runtime;
  runtime.thread_count_ = 2;
  runtime.pin_workers_ = false;
  runtime.recv_buffer_count_ = 0;
  ASSERT_TRUE(server.Start(runtime).ok());
  server.WaitUntilStopped();
  EXPECT_TRUE(service.result_.ok()) << service.result_;
}

}  // namespace
}  // namespace lavik::storage
