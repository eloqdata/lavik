/*
 * Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <sys/eventfd.h>

#include "../src/storage/engine/impl.h"
#include "gtest/gtest.h"
#include "support/read_buffer_fault.h"

namespace lavik::storage {

// Build a tiny published index and deliver deterministic CQEs without issuing
// disk I/O. The network e2e covers real submission; this fixture can invalidate
// admission exactly between snapshot and completion, and inspect every lease.
class RemoteGetTestPeer : public testing::Test {
 protected:
  using Awaitable = StorageEngine::RemoteReadAwaitable;

  void SetUp() override {
    saved_context_ = std::move(bycorf::MutableThisWorker());
    wake_fd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    ASSERT_GE(wake_fd_, 0);
    cross_core_.mailbox(0).wake_fd_ = wake_fd_;
    worker_.BindCrossCore(0, &cross_core_);
    owner_.BindCrossCore(1, &cross_core_);
    bycorf::SetThisWorker(0, &cross_core_, &worker_);
    auto status = worker_.Init({.recv_buffer_count_ = 0});
    ASSERT_TRUE(status.ok()) << status;
    auto& engine = *storage_.impl_;
    engine.worker_count_ = 2;
    engine.stores_.push_back(
        std::make_unique<StorageEngine::Impl::WorkerStore>());
    engine.stores_.push_back(
        std::make_unique<StorageEngine::Impl::WorkerStore>());
    engine.stores_[0]->engine_ = &engine;
    engine.stores_[0]->worker_ = &worker_;
    auto& source = *engine.stores_[1];
    source.engine_ = &engine;
    source.worker_ = &owner_;
    source.partitions_.resize(1);
    source.partitions_[0].id_ = 1;
    source.partitions_[0].indexes_ = std::span(&index_, 1);
    source.remote_reads_ready_.store(true);
    source.remote_population_generation_.store(2);
    engine.db_epochs_[0].store(1);
    engine.remote_replication_epochs_[1].store(1);
    engine.devices_.push_back(StorageDevice{.capacity_blocks_ = 2});
    engine.block_state_lookup_.push_back({&block_, 1, 2});
    engine.stores_[0]->files_.push_back(bycorf::FixedFile{});
    block_.Reset(1, 7);
    block_.allocated_ = true;

    RegisteredBufferPoolOptions options;
    options.storage_write_buffer_count_ = 2;
    options.registered_bytes_ =
        2 * options.write_buffer_bytes_ + options.read_slot_bytes_;
    ASSERT_TRUE(Pool().Init(worker_, options).ok());
    ASSERT_EQ(Pool().available_read_buffers(), 1);

    for (unsigned i = 0;; ++i) {
      key_ = "remote-" + std::to_string(i);
      if (RedisSlot(key_) == 1) break;
    }
    auto arena = std::make_shared<ScanHashMapEntryArena>(63);
    ASSERT_TRUE(arena->PreallocateDirectory());
    index_ = RecordIndex(std::move(arena));
    header_.header_bytes_ = RecordHeaderBytes(key_.size());
    header_.key_bytes_ = key_.size();
    header_.value_type_ = ValueType::kString;
    header_.logical_size_ = value_.size();
    header_.payload_bytes_ = value_.size();
    header_.total_disk_bytes_ =
        AlignRecord(header_.header_bytes_ + value_.size());
    header_.allocation_epoch_ = 7;
    header_.mutation_sequence_ = 9;
    header_.payload_checksum_ = Crc32c(std::as_bytes(std::span(value_)));
    const auto metadata = RecordLocation::PackedMetadata::Encode(
        kBlockHeaderBytes + 8, header_.total_disk_bytes_, 1, false, false,
        false, false, false, false, RecordKind::kValue, ValueType::kString);
    ASSERT_NE(
        index_.InsertNew(ComputeDigest(key_), key_,
                         RecordLocation(1, 9, 7, 0, value_.size(), metadata)),
        nullptr);
    index_.EnableConcurrentReads();
  }

  void TearDown() override {
    worker_.Shutdown();
    storage_.impl_.reset();
    if (wake_fd_ >= 0) close(wake_fd_);
    bycorf::MutableThisWorker() = std::move(saved_context_);
  }

  RegisteredBufferPool& Pool() { return storage_.impl_->stores_[0]->buffers_; }
  auto Read() {
    return storage_.TryRemoteRead(0, key_, 1, ValueType::kString, false
#if LAVIK_ENABLE_TRACE
                                  ,
                                  &trace_
#endif
    );
  }

  void Publish(ValueType type, bool grouped = false, bool staged = false,
               bool external = false) {
    header_.value_type_ = type;
    header_.logical_size_ = type == ValueType::kString ? value_.size() : 2;
    const auto metadata = RecordLocation::PackedMetadata::Encode(
        kBlockHeaderBytes + 8, header_.total_disk_bytes_, 1, staged, external,
        false, false, false, false, RecordKind::kValue, type, false, grouped);
    auto writer = index_.CloseAllReaders();
    auto* entry = index_.Find(ComputeDigest(key_), key_);
    ASSERT_NE(entry, nullptr);
    entry->assign(RecordLocation(1, 9, 7, 0, header_.logical_size_, metadata));
  }

  enum class UnpinPoint { kBeforeRelease, kAfterCount, kAfterRelease };

  void CheckFlushedBufferRelease(UnpinPoint point) {
    auto& engine = *storage_.impl_;
    auto& store = *engine.stores_[0];
    const auto available = Pool().available_write_buffers();
    block_.Reset(0, 7);
    block_.allocated_ = true;
    block_.committed_bytes_ = kBlockHeaderBytes + 4096;
    block_.live_bytes_ =
        4096;  // No defrag: only the flush may return this slot.
    block_.staging_slot_ = store.staging_slots_.size();
    store.staging_slots_.emplace_back();
    ASSERT_TRUE(Pool().TryAcquireWriteBuffer(
        &store.staging_slots_.back().write_buffer_id_));
    ASSERT_EQ(Pool().available_write_buffers(), available - 1);
    ASSERT_TRUE(block_.TryRemotePin());
    auto unpin = [&] {
      bycorf::SetThisWorker(1, &cross_core_, &owner_);
      engine.UnpinRemoteBlock(block_, 0, 1);
      bycorf::SetThisWorker(0, &cross_core_, &worker_);
    };
    if (point == UnpinPoint::kBeforeRelease) unpin();
#if LAVIK_FAULTS_ENABLED
    bool injected = false;
    if (point == UnpinPoint::kAfterCount) {
      engine.staging_release_count_test_hook_ = [&] {
        injected = true;
        unpin();
      };
    }
#endif
    engine.ReleaseFlushedStagingBuffer(store, block_, 1);
#if LAVIK_FAULTS_ENABLED
    engine.staging_release_count_test_hook_ = {};
    if (point == UnpinPoint::kAfterCount) {
      ASSERT_TRUE(injected);
      EXPECT_EQ(block_.pin_count(), 0);
    }
#endif
    if (point != UnpinPoint::kBeforeRelease) {
      EXPECT_TRUE(block_.release_pending_);
      EXPECT_FALSE(block_.TryRemotePin());
      EXPECT_EQ(Pool().available_write_buffers(), available - 1);
      if (point == UnpinPoint::kAfterRelease) unpin();
      // A one-way notification alone must finish the release; no later local
      // read/unpin or maintenance pass is allowed to rescue a lost wakeup.
      const auto until =
          std::chrono::steady_clock::now() + std::chrono::seconds(5);
      while (block_.release_pending_ &&
             std::chrono::steady_clock::now() < until)
        worker_.RunOnce(false);
    }
    EXPECT_EQ(block_.pin_count(), 0);
    EXPECT_FALSE(block_.release_pending_);
    EXPECT_EQ(block_.staging_slot_, 0);
    EXPECT_EQ(Pool().available_write_buffers(), available);
  }

  void Complete(Awaitable& read, int bytes = -1) {
    ASSERT_TRUE(read.io_.has_value());
    const auto io = read.lease_.io_buffer();
    auto record =
        std::span(io.data_ + read.record_headroom_, header_.total_disk_bytes_);
    header_.db_epoch_ = read.db_epoch_;
    header_.replication_epoch_ = read.replication_epoch_;
    ASSERT_TRUE(
        EncodeRecordHeader(header_, key_, record.first(header_.header_bytes_)));
    std::memcpy(record.data() + header_.header_bytes_, value_.data(),
                value_.size());
    // No awaiting handle was registered: Complete stores the result, and the
    // worker ignores the empty continuation. await_resume is otherwise real.
    read.io_->Complete(worker_, bytes == -1 ? read.read_bytes_ : bytes, 0);
  }

  void Invalidate(unsigned which) {
    auto& engine = *storage_.impl_;
    switch (which) {
      case 0:
        engine.stores_[1]->remote_population_generation_.fetch_add(2);
        break;
      case 1:
        engine.db_epochs_[0].fetch_add(1);
        break;
      case 2:
        engine.remote_replication_epochs_[1].fetch_add(1);
        break;
      case 3:
        engine.replica_loading_.store(true);
        break;
      case 4:
        engine.replica_recovery_fenced_.store(true);
        break;
      case 5:
        engine.request_serving_fenced_until_restart_.store(true);
        break;
    }
  }

  void ResetFences() {
    auto& engine = *storage_.impl_;
    engine.replica_loading_.store(false);
    engine.replica_recovery_fenced_.store(false);
    engine.request_serving_fenced_until_restart_.store(false);
  }

  bycorf::CurrentWorker saved_context_;
  bycorf::CrossCore cross_core_{2};
  int wake_fd_ = -1;
  bycorf::Worker worker_;
  bycorf::Worker owner_;
  RecordIndex index_;
  BlockState block_;
  StorageEngine storage_{StorageEngineOptions{}};
  std::string key_;
  std::string value_ = "remote value";
  RecordHeader header_;
#if LAVIK_ENABLE_TRACE
  ReadLatencyTrace trace_;
#endif
};

TEST_F(RemoteGetTestPeer, SynchronousMissAndClosedIndexFallback) {
  {
    auto read = storage_.TryRemoteRead(0, "absent", 1, ValueType::kString, false
#if LAVIK_ENABLE_TRACE
                                       ,
                                       &trace_
#endif
    );
    ASSERT_TRUE(read.await_ready());
    auto result = read.await_resume();
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(absl::IsNotFound(result->status()));
#if LAVIK_ENABLE_TRACE
    EXPECT_NE(trace_.lookup_done_ns_, 0);
    EXPECT_FALSE(trace_.hit_);
    EXPECT_FALSE(trace_.disk_read_);
#endif
  }
  auto writer = index_.CloseAllReaders();
  auto read = Read();
  ASSERT_TRUE(read.await_ready());
  EXPECT_FALSE(read.await_resume().has_value());
  EXPECT_EQ(block_.pin_count(), 0);
}

TEST_F(RemoteGetTestPeer, UnawaitedReadReleasesPinAndBuffer) {
  {
    auto read = Read();
    EXPECT_FALSE(read.await_ready());
    EXPECT_EQ(block_.pin_count(), 1);
    EXPECT_EQ(Pool().available_read_buffers(), 0);
  }
  EXPECT_EQ(block_.pin_count(), 0);
  EXPECT_EQ(Pool().available_read_buffers(), 1);
}

TEST_F(RemoteGetTestPeer,
       ExhaustedPoolAllocationFailureFallsBackWithoutWaiting) {
#if !LAVIK_FAULTS_ENABLED
  GTEST_SKIP() << "requires read buffer allocation failure injection";
#endif
  lavik::test::ScopedReadBufferAllocationFailure failure;
  {
    auto held = Pool().AcquireReadBuffer().await_resume();
    ASSERT_TRUE(held.ok());
    auto read = Read();
    ASSERT_TRUE(read.await_ready());
    EXPECT_FALSE(read.await_resume().has_value());
  }
  EXPECT_EQ(block_.pin_count(), 0);
  EXPECT_EQ(Pool().available_read_buffers(), 1);
}

TEST_F(RemoteGetTestPeer, ExhaustedPoolUsesAndReleasesTemporaryReplyLease) {
  auto held = Pool().AcquireReadBuffer().await_resume();
  ASSERT_TRUE(held.ok());
  Awaitable::Result result;
  {
    auto read = Read();
    ASSERT_FALSE(read.await_ready());
    EXPECT_EQ(Pool().overflow_read_buffer_count(), 1);
    ASSERT_NO_FATAL_FAILURE(Complete(read));
    result = read.await_resume();
    ASSERT_TRUE(result && result->ok());
    EXPECT_EQ(result->value().view().encoded_, value_);
  }
  EXPECT_EQ(block_.pin_count(), 0);
  EXPECT_EQ(Pool().available_read_buffers(), 0);
  EXPECT_EQ(Pool().overflow_read_buffer_count(), 1);
  result.reset();
  EXPECT_EQ(Pool().overflow_read_buffer_count(), 0);
}

TEST_F(RemoteGetTestPeer, CompletionTransfersReplyLeaseAndReleasesPin) {
  Awaitable::Result result;
  {
    auto read = Read();
    ASSERT_FALSE(read.await_ready());
    ASSERT_NO_FATAL_FAILURE(Complete(read));
#if LAVIK_ENABLE_TRACE
    EXPECT_NE(trace_.lookup_done_ns_, 0);
    EXPECT_TRUE(trace_.hit_);
    EXPECT_NE(trace_.buffer_acquired_ns_, 0);
    const auto completed = trace_.io_complete_ns_;
    EXPECT_NE(completed, 0);
#endif
    result = read.await_resume();
#if LAVIK_ENABLE_TRACE
    EXPECT_EQ(trace_.io_complete_ns_, completed);
    EXPECT_GE(trace_.decode_done_ns_, completed);
#endif
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result->ok()) << result->status();
    EXPECT_EQ(result->value().view().encoded_, value_);
  }
  EXPECT_EQ(block_.pin_count(), 0);
  EXPECT_EQ(Pool().available_read_buffers(), 0);
  result.reset();
  EXPECT_EQ(Pool().available_read_buffers(), 1);
}

TEST_F(RemoteGetTestPeer, CollectionPayloadsRetainMetadataAndLease) {
  for (auto type : {ValueType::kHash, ValueType::kSet, ValueType::kList,
                    ValueType::kSortedSet, ValueType::kStream}) {
    Publish(type);
    Awaitable::Result result;
    {
      auto read = storage_.TryRemoteRead(0, key_, 1, type);
      ASSERT_FALSE(read.await_ready()) << static_cast<int>(type);
      EXPECT_EQ(block_.pin_count(), 1);
      // The index reader has already drained; the pin alone protects I/O.
      auto writer = index_.CloseAllReaders();
      ASSERT_NO_FATAL_FAILURE(Complete(read));
      result = read.await_resume();
      ASSERT_TRUE(result && result->ok());
      EXPECT_EQ(result->value().view().encoded_, value_);
      EXPECT_EQ(result->value().view().logical_size_, 2);
    }
    EXPECT_EQ(block_.pin_count(), 0);
    EXPECT_EQ(Pool().available_read_buffers(), 0);
    result.reset();
    EXPECT_EQ(Pool().available_read_buffers(), 1);
  }
}

TEST_F(RemoteGetTestPeer, MetadataDoesNotAcquirePinOrBuffer) {
  for (auto type : {ValueType::kHash, ValueType::kSet, ValueType::kList,
                    ValueType::kSortedSet, ValueType::kStream}) {
    Publish(type);
    auto held = Pool().AcquireReadBuffer().await_resume();
    ASSERT_TRUE(held.ok());
    auto read = storage_.TryRemoteRead(0, key_, 1, type, true);
    ASSERT_TRUE(read.await_ready());
    auto result = read.await_resume();
    ASSERT_TRUE(result && result->ok());
    EXPECT_EQ(result->value().view().logical_size_, 2);
    EXPECT_TRUE(result->value().view().encoded_.empty());
    EXPECT_EQ(block_.pin_count(), 0);
  }
}

TEST_F(RemoteGetTestPeer, UnsupportedRepresentationsAndWrongTypeFallBack) {
  for (auto type : {ValueType::kHash, ValueType::kSet, ValueType::kList,
                    ValueType::kSortedSet, ValueType::kStream}) {
    for (unsigned representation = 0; representation < 4; ++representation) {
      Publish(type, representation == 1, representation == 2,
              representation == 3);
      auto read = storage_.TryRemoteRead(
          0, key_, 1, representation == 0 ? ValueType::kString : type);
      ASSERT_TRUE(read.await_ready());
      EXPECT_FALSE(read.await_resume());
      EXPECT_EQ(block_.pin_count(), 0);
      EXPECT_EQ(Pool().available_read_buffers(), 1);
    }
  }
}

TEST_F(RemoteGetTestPeer, StringReplyTransfersLeaseWithoutCopying) {
  std::optional<DiskValue> reply;
  {
    auto read = Read();
    ASSERT_NO_FATAL_FAILURE(Complete(read));
    auto result = read.await_resume();
    ASSERT_TRUE(result && result->ok());
    const auto payload = result->value().view().encoded_;
    auto encoded = std::move(result->value()).IntoDiskValue();
    ASSERT_TRUE(encoded.ok()) << encoded.status();
    EXPECT_EQ(reinterpret_cast<const char*>(encoded->value_bytes().data()),
              payload.data());
    reply.emplace(std::move(*encoded));
  }
  EXPECT_EQ(block_.pin_count(), 0);
  EXPECT_EQ(Pool().available_read_buffers(), 0);
  reply.reset();
  EXPECT_EQ(Pool().available_read_buffers(), 1);
}

TEST_F(RemoteGetTestPeer, PopulationChangesDiscardCompletedRead) {
  // Reset only test admission so each independent invalidation starts with a
  // fresh snapshot; production serving fences remain irreversible.
  for (unsigned which = 0; which < 6; ++which) {
    {
      auto read = Read();
      ASSERT_FALSE(read.await_ready()) << which;
      ASSERT_NO_FATAL_FAILURE(Complete(read));
      Invalidate(which);
      EXPECT_FALSE(read.await_resume().has_value()) << which;
    }
    EXPECT_EQ(block_.pin_count(), 0);
    EXPECT_EQ(Pool().available_read_buffers(), 1);
    // Epoch changes persist and are snapshotted by the next read.
    ResetFences();
  }
}

TEST_F(RemoteGetTestPeer, CorruptShortAndFailedReadsReleaseResources) {
  for (int completion : {0, -EIO, 4096}) {
    {
      auto read = Read();
      ASSERT_FALSE(read.await_ready());
      header_.payload_checksum_ ^= 1;
      ASSERT_NO_FATAL_FAILURE(Complete(read, completion));
      header_.payload_checksum_ ^= 1;
      auto result = read.await_resume();
      ASSERT_TRUE(result.has_value());
      EXPECT_FALSE(result->ok());
    }
    EXPECT_EQ(block_.pin_count(), 0);
    EXPECT_EQ(Pool().available_read_buffers(), 1);
  }
}

TEST_F(RemoteGetTestPeer, ReadsRecordWithoutCrc) {
  header_.no_crc_ = true;
  header_.payload_checksum_ ^= 1;
  {
    auto read = Read();
    ASSERT_FALSE(read.await_ready());
    ASSERT_NO_FATAL_FAILURE(Complete(read));
    auto result = read.await_resume();
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result->ok()) << result->status();
    EXPECT_EQ(result->value().view().encoded_, value_);
  }
  EXPECT_EQ(block_.pin_count(), 0);
  EXPECT_EQ(Pool().available_read_buffers(), 1);
}

TEST_F(RemoteGetTestPeer, FlushReturnsStagingBufferAcrossLastRemoteUnpin) {
  ASSERT_NO_FATAL_FAILURE(
      CheckFlushedBufferRelease(UnpinPoint::kBeforeRelease));
  ASSERT_NO_FATAL_FAILURE(CheckFlushedBufferRelease(UnpinPoint::kAfterRelease));
#if LAVIK_FAULTS_ENABLED
  // The owner has observed one pin, but the reader exits before it can mark
  // release_pending_. Moving CloseRemotePins after the count strands the slot
  // here: the reader saw an open gate and did not post a notification.
  ASSERT_NO_FATAL_FAILURE(CheckFlushedBufferRelease(UnpinPoint::kAfterCount));
#endif
}

}  // namespace lavik::storage
