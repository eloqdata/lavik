/*
 * Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <fcntl.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <optional>
#include <string>
#include <vector>

#include "../src/storage/engine/impl.h"
#include "absl/cleanup/cleanup.h"
#include "bycorf/net/server.h"
#include "gtest/gtest.h"
#include "lavik/metrics.h"
#include "lavik/tx/tx_shard.h"
#include "support/test_data_path.h"

namespace lavik::storage {

class WriteBufferTestPeer {
 public:
  enum class Scenario {
    kProgress,
    kEpochChanged,
    kSourceChanged,
    kNoStaging,
    kCheckpoint,
    kCheckpointAfterSubmit,
    kCheckpointMissingSlot,
    kRecoveryRead,
  };

  static Task<absl::Status> RunRecoveryRead(StorageEngine& storage) {
    auto& impl = *storage.impl_;
    auto& store = impl.CurrentStore();
    auto& worker = *store.worker_;
    auto status = co_await storage.ConfigureDefrag(
        {.action_ = DefragConfigAction::kPause});
    if (!status.ok()) co_return status;
    auto wrote = co_await storage.Set(0, "recovery-source", "durable-value");
    if (!wrote.ok()) co_return wrote.status();
    impl.SealActiveBlocks(store);
    while (store.flush_running_ || store.standby_prefetch_pending_)
      co_await bycorf::Yield(worker);

    std::vector<std::uint16_t> writes;
    std::uint16_t id = 0;
    while (store.buffers_.TryAcquireWriteBuffer(&id)) writes.push_back(id);
    absl::Cleanup release = [&] {
      for (auto slot : writes) store.buffers_.ReleaseWriteBuffer(slot);
      unsetenv("LAVIK_FAIL_READ_BUFFER_ALLOCATION");
    };
    setenv("LAVIK_FAIL_READ_BUFFER_ALLOCATION", "1", 1);
    std::vector<ReadBufferLease> reads;
    while (store.buffers_.available_read_buffers() != 0) {
      auto acquired = co_await store.buffers_.AcquireReadBuffer();
      if (!acquired.ok()) co_return acquired.status();
      reads.push_back(std::move(*acquired));
    }
    const auto retained = WorkerMemoryAccountingBytes(worker.id());
    {
      std::vector<RecoveryBatch> batches(1);
      std::vector<std::uint64_t> zero_blocks;
      absl::flat_hash_set<std::uint64_t> committed;
      status = co_await impl.ScanAssignedBlocks(store, &batches, &zero_blocks,
                                                &committed, false);
      if (!status.ok()) co_return status;
      if (batches[0].records_.size() != 1 ||
          batches[0].records_[0].key_ != "recovery-source")
        co_return absl::DataLossError(
            "recovery did not read the durable record");
    }
    if (store.buffers_.available_read_buffers() != 0 ||
        store.buffers_.available_write_buffers() != 0 ||
        store.buffers_.overflow_read_buffer_count() != 0 ||
        WorkerMemoryAccountingBytes(worker.id()) != retained)
      co_return absl::InternalError(
          "recovery allocated or borrowed ordinary buffers");
    co_return absl::OkStatus();
  }

  static Task<absl::Status> RunCheckpoint(StorageEngine& storage,
                                          Scenario scenario) {
    auto& impl = *storage.impl_;
    auto& store = impl.CurrentStore();
    auto& worker = *store.worker_;
    auto paused = co_await storage.ConfigureDefrag(
        {.action_ = DefragConfigAction::kPause});
    if (!paused.ok()) co_return paused;
    if (store.buffers_.available_write_buffers() != 2)
      co_return absl::InternalError("checkpoint fixture needs two free slots");

    std::uint16_t held = 0;
    absl::Cleanup release = [&] {
      if (held != 0) store.buffers_.ReleaseWriteBuffer(held);
    };
    if (scenario == Scenario::kCheckpointMissingSlot &&
        !store.buffers_.TryAcquireWriteBuffer(&held))
      co_return absl::InternalError("could not hold checkpoint fixture slot");

    // This empty shard has no foreground or maintenance writer. Its capacity
    // chunk still submits real direct I/O through the normal checkpoint path.
    const auto status = co_await impl.BuildShutdownCheckpointShard(store, 1);
    if (scenario == Scenario::kCheckpointMissingSlot) {
      if (!absl::IsFailedPrecondition(status) ||
          store.buffers_.available_write_buffers() != 1)
        co_return absl::InternalError(
            "partial checkpoint acquisition did not return its first slot");
      co_return absl::OkStatus();
    }
    if (scenario == Scenario::kCheckpointAfterSubmit) {
      if (status.message() !=
          "injected checkpoint failure after write submission")
        co_return absl::InternalError("checkpoint did not reach failure hook");
      // No yield occurs between submission, injected failure, and this check.
      // The unused slot is returned; the submitted one belongs to completion.
      if (store.buffers_.available_write_buffers() != 1)
        co_return absl::InternalError(
            "abandoned checkpoint returned a slot before I/O completed");
    } else if (!status.ok()) {
      co_return status;
    }
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (store.buffers_.available_write_buffers() != 2 &&
           std::chrono::steady_clock::now() < deadline)
      co_await bycorf::Yield(worker);
    if (store.buffers_.available_write_buffers() != 2)
      co_return absl::DeadlineExceededError(
          "checkpoint completion did not return its write slot");
    co_return absl::OkStatus();
  }

  static Task<absl::Status> Run(StorageEngine& storage, Scenario scenario) {
    if (scenario == Scenario::kRecoveryRead)
      co_return co_await RunRecoveryRead(storage);
    if (scenario == Scenario::kCheckpoint ||
        scenario == Scenario::kCheckpointAfterSubmit ||
        scenario == Scenario::kCheckpointMissingSlot)
      co_return co_await RunCheckpoint(storage, scenario);
    auto& impl = *storage.impl_;
    auto& store = impl.CurrentStore();
    auto& worker = *store.worker_;
    auto status = co_await storage.ConfigureDefrag(
        {.action_ = DefragConfigAction::kPause});
    if (!status.ok()) co_return status;
    constexpr std::string_view key = "relocation-fifo-source";
    constexpr std::string_view value = "original-value";
    auto wrote = co_await storage.Set(0, key, value);
    if (!wrote.ok()) co_return wrote.status();
    auto& partition = impl.PartitionForKey(store, key);
    auto& index = partition.indexes_[0];
    const auto digest = ComputeDigest(key);
    auto* entry = index.Find(digest, key);
    if (entry == nullptr) co_return absl::InternalError("missing source");
    const auto source = impl.MaterializeIndexLocation(*entry);
    RecordHeader record;
    std::string_view decoded_key;
    const auto buffer = impl.StagingBufferFor(
        store, *impl.FindBlockState(store, source.block_id()));
    if (!DecodeRecordHeader(
            {buffer.data_ + source.record_offset(), source.total_disk_bytes()},
            &record, &decoded_key))
      co_return absl::InternalError("invalid source record");
    impl.SealActiveBlocks(store);
    while (store.flush_running_ || store.standby_prefetch_pending_)
      co_await bycorf::Yield(worker);

    // Keep a ready physical allocation, then hold the foreground allocation
    // gate throughout the relocation. A maintenance writer must never join
    // that gate: its owner can itself be waiting for defrag to reclaim space.
    if (!store.standby_block_) {
      co_await store.store_state_mutex_.Lock();
      auto reserved = co_await impl.AcquireWriteBlock(store, true, true);
      store.store_state_mutex_.Unlock(worker);
      if (!reserved.ok()) co_return reserved.status();
      store.standby_block_ = *reserved;
    }

    std::vector<std::uint16_t> held;
    const auto keep_free = scenario == Scenario::kNoStaging ? 0u : 1u;
    while (store.buffers_.available_write_buffers() > keep_free) {
      std::uint16_t id = 0;
      if (!store.buffers_.TryAcquireWriteBuffer(&id))
        co_return absl::InternalError("could not hold fixture write slot");
      held.push_back(id);
    }
    absl::Cleanup release_slots = [&] {
      for (const auto id : held) store.buffers_.ReleaseWriteBuffer(id);
    };
    if (scenario != Scenario::kNoStaging) {
      wrote = co_await storage.Set(0, "occupy-last-write-slot", "payload");
      if (!wrote.ok()) co_return wrote.status();
      while (store.standby_prefetch_pending_) co_await bycorf::Yield(worker);
      if (!store.standby_block_)
        co_return absl::InternalError("fixture has no standby block");
      // Queue real flush I/O, but start relocation before the worker runs it.
      impl.SealActiveBlocks(store);
    }
    co_await store.active_block_allocation_mutex_.Lock();
    absl::Cleanup release_gate = [&] {
      store.active_block_allocation_mutex_.Unlock(worker);
    };
    const auto retained = WorkerMemoryAccountingBytes(worker.id());
    auto relocation = impl.RelocateIfCurrent(0, key, value, record, source);
    auto handle = std::move(relocation).ReleaseHandle();
    relocation = decltype(relocation)(handle);
    handle.resume();
    if (relocation.done() || !store.buffers_.has_write_waiters())
      co_return absl::InternalError(
          "relocation did not queue for a fixed slot");

    // Acquiring store state proves the waiter released it. Change the exact
    // population or physical source while it sleeps, then let real flush
    // complete. Neither stale source may overwrite the new state.
    co_await store.store_state_mutex_.Lock();
    if (scenario == Scenario::kEpochChanged) ++partition.replication_epoch_;
    if (scenario == Scenario::kSourceChanged) {
      auto replacement = source;
      // Allocation epochs come from BlockState when a compact index location
      // is materialized. Change the indexed offset to model a newer physical
      // record; merely changing a temporary RecordLocation epoch has no effect.
      replacement.metadata_ = RecordLocation::PackedMetadata::Encode(
          source.record_offset() + kRecordAlignment, source.total_disk_bytes(),
          source.block_owner(), false, false, false, source.shielding(), false,
          false, source.kind(), source.value_type());
      auto changed = impl.ReplaceIndexLocation(store, index, entry, digest,
                                               replacement, nullptr);
      if (!changed.ok()) {
        store.store_state_mutex_.Unlock(worker);
        co_return changed.status();
      }
    }
    store.store_state_mutex_.Unlock(worker);

    // This trailing borrower only progresses if relocation seals its newly
    // installed stream even on stale-source exits. It cannot steal the first
    // flush's slot from the already queued relocation.
    auto acquire_after = [&]() -> Task<absl::Status> {
      auto acquired = co_await store.buffers_.AcquireWriteBuffer();
      if (!acquired.ok()) co_return acquired.status();
      if (!relocation.done())
        co_return absl::InternalError("late writer overtook relocation");
      store.buffers_.ReleaseWriteBuffer(*acquired);
      co_return absl::OkStatus();
    };
    auto trailing = acquire_after();
    auto trailing_handle = std::move(trailing).ReleaseHandle();
    trailing = decltype(trailing)(trailing_handle);
    trailing_handle.resume();
    if (scenario == Scenario::kNoStaging) {
      // An in-flight extent writer can own a slot without a staging entry.
      // Relocation must wait for its return just like any other FIFO borrower.
      store.buffers_.ReleaseWriteBuffer(held.back());
      held.pop_back();
    }
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while ((!relocation.done() || !trailing.done()) &&
           std::chrono::steady_clock::now() < deadline)
      co_await bycorf::Yield(worker);
    if (!relocation.done() || !trailing.done())
      co_return absl::DeadlineExceededError("write-slot handoff stalled");
    auto result = std::move(relocation).TakeResult();
    if (!result.ok()) co_return result.status();
    status = std::move(trailing).TakeResult();
    if (!status.ok()) co_return status;
    const bool moves_source =
        scenario == Scenario::kProgress || scenario == Scenario::kNoStaging;
    if (result->has_value() != moves_source)
      co_return absl::InternalError("unexpected relocation publication");
    const auto current =
        impl.MaterializeIndexLocation(*index.Find(digest, key));
    if (moves_source && current.SamePhysicalRecord(source))
      co_return absl::InternalError("relocation did not move the source");
    if (scenario == Scenario::kEpochChanged &&
        !current.SamePhysicalRecord(source))
      co_return absl::InternalError("relocation crossed a population change");
    if (scenario == Scenario::kSourceChanged &&
        current.record_offset() != source.record_offset() + kRecordAlignment)
      co_return absl::InternalError("relocation overwrote a changed source");
    if (store.buffers_.has_write_waiters() ||
        store.buffers_.available_write_buffers() != 1)
      co_return absl::InternalError(
          "write slot was not returned after handoff");
    if (WorkerMemoryAccountingBytes(worker.id()) >= retained + 8 * kMiB)
      co_return absl::InternalError("relocation grew the target buffer pool");
    co_return absl::OkStatus();
  }
};

namespace {
class WriteBufferService final : public bycorf::Service {
 public:
  WriteBufferService(StorageEngine& storage,
                     WriteBufferTestPeer::Scenario scenario)
      : storage_(storage), scenario_(scenario) {}
  void Prepare(unsigned) override {}
  Task<absl::Status> Run(bycorf::Worker& worker,
                         bycorf::ServiceContext) override {
    BindMemoryAccountingShard(worker.id());
    tx::TxRuntime::Get()->shard(worker.id()).Bind(worker);
    result_ = co_await storage_.InitializeWorker(worker);
    if (result_.ok())
      result_ = co_await WriteBufferTestPeer::Run(storage_, scenario_);
    worker.RequestStop();
    co_return result_;
  }
  void Stop() noexcept override {}
  void FinalizeWorker(bycorf::Worker& worker) noexcept override {
    storage_.FinalizeWorker(worker);
  }
  StorageEngine& storage_;
  WriteBufferTestPeer::Scenario scenario_;
  absl::Status result_ = absl::UnknownError("service did not run");
};

class StorageWriteBufferTest
    : public testing::TestWithParam<WriteBufferTestPeer::Scenario> {};

void RunWriteBufferScenario(WriteBufferTestPeer::Scenario scenario) {
  constexpr const char* fault = "LAVIK_FAIL_CHECKPOINT_AFTER_SUBMIT";
  std::optional<std::string> previous;
  if (const char* value = std::getenv(fault)) previous = value;
  absl::Cleanup restore = [&] {
    if (previous)
      ::setenv(fault, previous->c_str(), 1);
    else
      ::unsetenv(fault);
  };
  if (scenario == WriteBufferTestPeer::Scenario::kCheckpointAfterSubmit) {
#if !LAVIK_FAULTS_ENABLED
    GTEST_SKIP() << "requires checkpoint submission failure injection";
#endif
    ASSERT_EQ(::setenv(fault, "1", 1), 0);
  } else {
    ASSERT_EQ(::unsetenv(fault), 0);
  }
  const auto path = test::TestDataPath("lavik-write-fifo-" +
                                       std::to_string(getpid()) + ".data");
  const int fd =
      open(path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(fd, 0);
  absl::Cleanup remove = [&] { unlink(path.c_str()); };
  ASSERT_EQ(ftruncate(fd, 256 * kMiB), 0);
  close(fd);
  InitWorkerMetrics(1);
  ASSERT_TRUE(InitMemoryLimit(1024 * kMiB, 1).ok());
  if (tx::TxRuntime::Get() == nullptr) tx::TxRuntime::Create(1);
  StorageEngineOptions options;
  options.data_files_ = {path};
  options.database_count_ = 1;
  options.expiration_authority_ = false;
  options.tomb_raider_interval_ms_ = 0;
  options.tx_cleaner_cooldown_ms_ = 0;
  options.buffers_.storage_write_buffer_count_ = 2;
  StorageEngine storage(std::move(options));
  ASSERT_TRUE(storage.Prepare(1).ok());
  WriteBufferService service(storage, scenario);
  bycorf::Server server;
  server.AddService(&service);
  bycorf::ServerOptions runtime;
  runtime.thread_count_ = 1;
  runtime.pin_workers_ = false;
  runtime.recv_buffer_count_ = 0;
  ASSERT_TRUE(server.Start(runtime).ok());
  server.WaitUntilStopped();
  EXPECT_TRUE(service.result_.ok()) << service.result_;
}

TEST_P(StorageWriteBufferTest,
       RelocationWaitsWithoutHoldingStoreOrAllocationGate) {
  RunWriteBufferScenario(GetParam());
}

class StorageCheckpointBufferTest
    : public testing::TestWithParam<WriteBufferTestPeer::Scenario> {};

TEST(StorageRecoveryBufferTest, ScansWithoutOrdinarySlotsOrOverflow) {
#if !LAVIK_FAULTS_ENABLED
  GTEST_SKIP() << "requires allocation failure injection";
#endif
  RunWriteBufferScenario(WriteBufferTestPeer::Scenario::kRecoveryRead);
}

TEST_P(StorageCheckpointBufferTest, KeepsBorrowedSlotsUntilIoCompletes) {
  RunWriteBufferScenario(GetParam());
}

INSTANTIATE_TEST_SUITE_P(
    Shutdown, StorageCheckpointBufferTest,
    testing::Values(WriteBufferTestPeer::Scenario::kCheckpoint,
                    WriteBufferTestPeer::Scenario::kCheckpointAfterSubmit,
                    WriteBufferTestPeer::Scenario::kCheckpointMissingSlot));

INSTANTIATE_TEST_SUITE_P(
    DefragFifo, StorageWriteBufferTest,
    testing::Values(WriteBufferTestPeer::Scenario::kProgress,
                    WriteBufferTestPeer::Scenario::kEpochChanged,
                    WriteBufferTestPeer::Scenario::kSourceChanged,
                    WriteBufferTestPeer::Scenario::kNoStaging));
}  // namespace
}  // namespace lavik::storage
