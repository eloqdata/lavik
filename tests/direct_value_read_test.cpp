/*
 * Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <sys/eventfd.h>

#include "../src/storage/engine/impl.h"
#include "gtest/gtest.h"

namespace lavik::storage {

// Deliver deterministic CQEs to test invalidation and resource ownership at
// exact suspension boundaries. End-to-end storage tests cover real submission.
class DirectValueReadTestPeer : public testing::Test {
 protected:
  using Awaitable = StorageEngine::Impl::DirectValueRead;

  void SetUp() override {
    saved_context_ = std::move(bycorf::MutableThisWorker());
    wake_fd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    ASSERT_GE(wake_fd_, 0);
    cross_core_.mailbox(0).wake_fd_ = wake_fd_;
    worker_.BindCrossCore(0, &cross_core_);
    bycorf::SetThisWorker(0, &cross_core_, &worker_);
    auto status = worker_.Init({.recv_buffer_count_ = 0});
    ASSERT_TRUE(status.ok()) << status;
    auto& engine = *storage_.impl_;
    engine.worker_count_ = 1;
    engine.stores_.push_back(
        std::make_unique<StorageEngine::Impl::WorkerStore>());
    auto& store = *engine.stores_[0];
    store.worker_ = &worker_;
    store.partitions_.resize(1);
    store.partitions_[0].replication_epoch_ = 1;
    store.files_.push_back(bycorf::FixedFile{});
    engine.db_epochs_[0].store(1);
    engine.devices_.push_back(StorageDevice{.capacity_blocks_ = 2});
    engine.block_state_lookup_.push_back({&block_, 1, 2});
    block_.Reset(0, 7);
    block_.allocated_ = true;

    RegisteredBufferPoolOptions options;
    options.storage_write_buffer_count_ = 1;
    options.registered_bytes_ =
        options.write_buffer_bytes_ + options.read_slot_bytes_;
    ASSERT_TRUE(Pool().Init(worker_, options).ok());
    ASSERT_EQ(Pool().available_read_buffers(), 1);

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
        kBlockHeaderBytes + 8, header_.total_disk_bytes_, 0, false, false,
        false, false, false, false, RecordKind::kValue, ValueType::kString);
    location_ = RecordLocation(1, 9, 7, 0, value_.size(), metadata);
  }

  void TearDown() override {
    worker_.Shutdown();
    storage_.impl_.reset();
    if (wake_fd_ >= 0) close(wake_fd_);
    bycorf::MutableThisWorker() = std::move(saved_context_);
  }

  RegisteredBufferPool& Pool() { return storage_.impl_->stores_[0]->buffers_; }
  auto Read() {
    auto& engine = *storage_.impl_;
    auto& store = *engine.stores_[0];
    return Awaitable(engine, store, store.partitions_[0], 0, key_, location_,
                     nullptr);
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

  void Invalidate(bool database) {
    auto& engine = *storage_.impl_;
    if (database)
      engine.db_epochs_[0].fetch_add(1);
    else
      ++engine.stores_[0]->partitions_[0].replication_epoch_;
  }

  bycorf::CurrentWorker saved_context_;
  bycorf::CrossCore cross_core_{1};
  int wake_fd_ = -1;
  bycorf::Worker worker_;
  BlockState block_;
  StorageEngine storage_{StorageEngineOptions{}};
  std::string key_ = "direct-key";
  std::string value_ = "direct value";
  RecordHeader header_;
  RecordLocation location_;
};

TEST_F(DirectValueReadTestPeer, StagedAndFreeingBlocksFallBackSynchronously) {
  location_.set_in_memory(true);
  {
    auto read = Read();
    ASSERT_TRUE(read.await_ready());
    EXPECT_FALSE(read.await_resume().has_value());
  }
  location_.set_in_memory(false);
  block_.freeing_ = true;
  {
    auto read = Read();
    ASSERT_TRUE(read.await_ready());
    EXPECT_FALSE(read.await_resume().has_value());
  }
  EXPECT_EQ(block_.pins_, 0);
  EXPECT_EQ(Pool().available_read_buffers(), 1);
}

TEST_F(DirectValueReadTestPeer, UnawaitedReadReleasesPinAndBuffer) {
  {
    auto read = Read();
    EXPECT_FALSE(read.await_ready());
    EXPECT_EQ(block_.pins_, 1);
    EXPECT_EQ(Pool().available_read_buffers(), 0);
  }
  EXPECT_EQ(block_.pins_, 0);
  EXPECT_EQ(Pool().available_read_buffers(), 1);
}

TEST_F(DirectValueReadTestPeer, ExhaustedPoolFallsBackWithoutEnqueuingWaiter) {
  {
    auto held = Pool().AcquireReadBuffer().await_resume();
    ASSERT_TRUE(held.ok());
    auto read = Read();
    ASSERT_TRUE(read.await_ready());
    EXPECT_FALSE(read.await_resume().has_value());
  }
  EXPECT_EQ(block_.pins_, 0);
  EXPECT_EQ(Pool().available_read_buffers(), 1);
}

TEST_F(DirectValueReadTestPeer, CompletionTransfersLeaseAndReleasesPin) {
  Awaitable::Result result;
  {
    auto read = Read();
    ASSERT_FALSE(read.await_ready());
    ASSERT_NO_FATAL_FAILURE(Complete(read));
    result = read.await_resume();
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result->ok()) << result->status();
    auto bytes = result->value().value();
    EXPECT_EQ(std::string_view(reinterpret_cast<const char*>(bytes.data()),
                               bytes.size()),
              value_);
  }
  EXPECT_EQ(block_.pins_, 0);
  EXPECT_EQ(Pool().available_read_buffers(), 0);
  result.reset();
  EXPECT_EQ(Pool().available_read_buffers(), 1);
}

TEST_F(DirectValueReadTestPeer, PopulationChangesDiscardCompletedRead) {
  for (bool database : {false, true}) {
    {
      auto read = Read();
      ASSERT_FALSE(read.await_ready());
      ASSERT_NO_FATAL_FAILURE(Complete(read));
      Invalidate(database);
      auto result = read.await_resume();
      ASSERT_TRUE(result.has_value());
      EXPECT_TRUE(absl::IsNotFound(result->status()));
    }
    EXPECT_EQ(block_.pins_, 0);
    EXPECT_EQ(Pool().available_read_buffers(), 1);
  }
}

TEST_F(DirectValueReadTestPeer, ErrorsReleaseResourcesAndStaleIdentityRetries) {
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
    EXPECT_EQ(block_.pins_, 0);
    EXPECT_EQ(Pool().available_read_buffers(), 1);
  }
  {
    auto read = Read();
    ASSERT_FALSE(read.await_ready());
    ++header_.allocation_epoch_;
    ASSERT_NO_FATAL_FAILURE(Complete(read));
    EXPECT_FALSE(read.await_resume().has_value());
  }
  EXPECT_EQ(block_.pins_, 0);
  EXPECT_EQ(Pool().available_read_buffers(), 1);
}

}  // namespace lavik::storage
