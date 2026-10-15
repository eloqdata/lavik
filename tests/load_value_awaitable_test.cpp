/*
 * Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <sys/eventfd.h>

#include <chrono>

#include "../src/storage/engine/impl.h"
#include "gtest/gtest.h"

namespace lavik::storage {

// Deliver deterministic CQEs for identity races and real I/O for buffer
// handoff.
class LoadValueAwaitableTestPeer : public testing::Test {
 protected:
  using Awaitable = StorageEngine::Impl::LoadValueAwaitable;
  using Result = Awaitable::Result;
  using ExtentRead = StorageEngine::Impl::ReadExtentAwaitable;

  void SetUp() override {
    saved_context_ = std::move(bycorf::MutableThisWorker());
    wake_fd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    ASSERT_GE(wake_fd_, 0);
    cross_core_.mailbox(0).wake_fd_ = wake_fd_;
    worker_.BindCrossCore(0, &cross_core_);
    bycorf::SetThisWorker(0, &cross_core_, &worker_);
    ASSERT_TRUE(worker_.Init({.recv_buffer_count_ = 0}).ok());
    auto& engine = *storage_.impl_;
    engine.worker_count_ = 1;
    engine.stores_.push_back(
        std::make_unique<StorageEngine::Impl::WorkerStore>());
    auto& store = *engine.stores_[0];
    store.engine_ = &engine;
    store.worker_ = &worker_;
    store.partitions_.resize(1);
    store.partition_indexes_.resize(kLogicalDatabaseCount);
    store.partitions_[0].indexes_ = store.partition_indexes_;
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

  auto& Store() { return *storage_.impl_->stores_[0]; }
  RegisteredBufferPool& Pool() { return Store().buffers_; }
  auto Read() {
    return storage_.impl_->LoadValue(Store(), Store().partitions_[0], 0, key_,
                                     digest_, location_, {});
  }
  auto ReadLocal() {
    return storage_.impl_->LoadValueLocal(Store(), 0, key_, location_, 1);
  }
  bool HasHelper(const Awaitable& read) { return read.slow_.valid(); }

  void Encode(std::byte* data) {
    ASSERT_TRUE(EncodeRecordHeader(header_, key_,
                                   std::span(data, header_.header_bytes_)));
    std::memcpy(data + header_.header_bytes_, value_.data(), value_.size());
  }
  void Complete(Awaitable& read, int bytes = -1) {
    ASSERT_NE(read.pinned_, nullptr);
    Encode(read.lease_.io_buffer().data_ + read.record_headroom_);
    read.Complete(worker_, bytes == -1 ? read.read_bytes_ : bytes, 0);
  }
  void Stage() {
    location_.set_in_memory(true);
    block_.in_memory_ = true;
    if (block_.staging_slot_ == 0) {
      block_.staging_slot_ = Store().staging_slots_.size();
      Store().staging_slots_.emplace_back();
      ASSERT_TRUE(Pool().TryAcquireWriteBuffer(
          &Store().staging_slots_.back().write_buffer_id_));
    }
    auto buffer = storage_.impl_->StagingBufferFor(Store(), block_);
    Encode(buffer.data_ + location_.record_offset());
  }
  void IndexLocation() {
    ASSERT_NE(Store()
                  .partitions_[0]
                  .indexes_[0]
                  .InsertOrAssign(digest_, key_, location_)
                  .entry_,
              nullptr);
  }
  void Invalidate(bool database) {
    if (database)
      storage_.impl_->db_epochs_[0].fetch_add(1);
    else
      ++Store().partitions_[0].replication_epoch_;
  }
  void CandidatePopulation() {
    storage_.impl_->replica_loading_.store(true);
    auto& partition = Store().partitions_[0];
    partition.replica_sync_ = std::make_unique<
        StorageEngine::Impl::WorkerStore::PartitionStore::ReplicaSyncState>();
    partition.replica_sync_->local_db_epochs_[0] = 4;
    header_.db_epoch_ = 4;
  }
  void ExpectValue(const Result& result) {
    ASSERT_TRUE(result.ok()) << result.status();
    auto bytes = result->value();
    EXPECT_EQ(std::string_view(reinterpret_cast<const char*>(bytes.data()),
                               bytes.size()),
              value_);
  }

  // Simulate only the submission, retaining the real completion -> retry ->
  // parent continuation chain, with an owned Task to catch early/double resume.
  Task<Result> AwaitCompletion(Awaitable& read) {
    struct Arm {
      Awaitable& read_;
      bool await_ready() const { return false; }
      void await_suspend(std::coroutine_handle<> parent) {
        read_.awaiting_ = parent;
      }
      void await_resume() const {}
    };
    co_await Arm{read};
    co_return read.await_resume();
  }
  Task<Result> AwaitExisting(Awaitable& read) { co_return co_await read; }
  Task<absl::Status> AwaitExtent(ExtentRead& read) { co_return co_await read; }
  bool BufferWaitIsBackground(Awaitable& read) {
    return read.task_class_ == bycorf::TaskClass::kBackground;
  }
  auto ExtentSlice(ExtentRef ref, std::size_t offset,
                   std::span<std::byte> destination) {
    return storage_.impl_->ReadExtentSlice(Store(), ref, 0, offset,
                                           destination);
  }

  // Use a real fixed-file read for pool -> I/O handoff, rather than completing
  // the request before the continuation actually submits it.
  void InstallDiskImage(std::span<const std::byte> bytes) {
    char path[] = "/tmp/lavik-awaitable-XXXXXX";
    const int fd = mkstemp(path);
    ASSERT_GE(fd, 0);
    const auto offset = storage_.impl_->FileOffset(1).second;
    const auto written = pwrite(fd, bytes.data(), bytes.size(), offset);
    close(fd);
    ASSERT_EQ(written, static_cast<ssize_t>(bytes.size()));
    struct Opened : bycorf::IoCompletion {
      bool done_ = false;
      int result_ = -1;
      void Complete(bycorf::Worker&, int result, unsigned) override {
        result_ = result;
        done_ = true;
      }
    } opened;
    ASSERT_TRUE(worker_.RegisterFixedFiles(1).ok());
    auto submitted =
        worker_.SubmitOpenDirect(path, O_RDONLY | O_DIRECT, 0, {0}, &opened);
    if (submitted.ok()) {
      const auto until =
          std::chrono::steady_clock::now() + std::chrono::seconds(5);
      while (!opened.done_ && std::chrono::steady_clock::now() < until)
        worker_.RunOnce(false);
    }
    unlink(path);
    ASSERT_TRUE(submitted.ok()) << submitted;
    ASSERT_TRUE(opened.done_);
    ASSERT_EQ(opened.result_, 0);
  }
  void InstallRecord() {
    std::vector<std::byte> bytes(
        AlignDirect(location_.record_offset() + location_.total_disk_bytes()));
    Encode(bytes.data() + location_.record_offset());
    InstallDiskImage(bytes);
  }
  ExtentRef ExtentReference(bool no_crc = false) {
    return {1, 7, static_cast<std::uint32_t>(value_.size()),
            no_crc ? 0 : Crc32c(std::as_bytes(std::span(value_)))};
  }
  void InstallExtent(bool corrupt = false, bool no_crc = false) {
    block_.kind_ = BlockKind::kPayloadExtent;
    auto ref = ExtentReference(no_crc);
    std::vector<std::byte> bytes(
        AlignDirect(kBlockHeaderBytes + value_.size()));
    BlockHeader header;
    header.block_id_ = 1;
    header.allocation_epoch_ = 7;
    header.kind_ = BlockKind::kPayloadExtent;
    header.extent_no_crc_ = no_crc;
    header.layout_worker_count_ = 1;
    header.committed_bytes_ = kBlockHeaderBytes + value_.size();
    header.extent_payload_bytes_ = value_.size();
    header.extent_payload_checksum_ = ref.payload_checksum_;
    EncodeBlockHeader(header, std::span<std::byte, kBlockHeaderSlotBytes>(
                                  bytes.data(), kBlockHeaderSlotBytes));
    std::memcpy(bytes.data() + kBlockHeaderBytes, value_.data(), value_.size());
    // Corrupt beyond the slice returned by the test, inside the actual payload.
    if (corrupt) bytes[kBlockHeaderBytes + value_.size() - 1] ^= std::byte{1};
    InstallDiskImage(bytes);
  }

  template <typename T>
  auto Start(Task<T>& task, bool background = false) {
    auto handle = std::move(task).operator co_await().await_suspend(
        std::noop_coroutine());
    if (background) worker_.RegisterBackground(handle);
    handle.resume();
    return handle;
  }
  template <typename T>
  void Drain(Task<T>& task) {
    const auto until =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!task.done() && std::chrono::steady_clock::now() < until)
      worker_.RunOnce(false);
    ASSERT_TRUE(task.done());
  }
  bool HelperIsBackground(Awaitable& read) {
    return worker_.IsBackground(
        std::move(read.slow_).operator co_await().handle_);
  }

  bycorf::CurrentWorker saved_context_;
  bycorf::CrossCore cross_core_{1};
  int wake_fd_ = -1;
  bycorf::Worker worker_;
  BlockState block_;
  StorageEngine storage_{StorageEngineOptions{}};
  std::string key_ = "direct-key";
  std::string value_ = "direct value";
  Digest digest_{17};
  RecordHeader header_;
  RecordLocation location_;
};

TEST_F(LoadValueAwaitableTestPeer, BufferWaitersPreserveFifoHandoff) {
  ASSERT_NO_FATAL_FAILURE(InstallRecord());
  auto held = Pool().AcquireReadBuffer().await_resume();
  ASSERT_TRUE(held.ok());
  auto first = Read();
  auto second = Read();
  auto first_task = AwaitExisting(first);
  auto second_task = AwaitExisting(second);
  Start(first_task, true);
  Start(second_task);
  EXPECT_TRUE(BufferWaitIsBackground(first));
  EXPECT_EQ(block_.pin_count(), 0);
  held->Reset();
  // Handoff schedules I/O; it must not resume inside the release stack.
  EXPECT_FALSE(first_task.done());
  EXPECT_EQ(Pool().available_read_buffers(), 0);
  ASSERT_NO_FATAL_FAILURE(Drain(first_task));
  EXPECT_FALSE(second_task.done());
  // Taking/destroying the first result hands its sole slot to the second.
  ExpectValue(std::move(first_task).TakeResult());
  ASSERT_NO_FATAL_FAILURE(Drain(second_task));
  ExpectValue(std::move(second_task).TakeResult());
  EXPECT_FALSE(HasHelper(first));
  EXPECT_FALSE(HasHelper(second));
  EXPECT_EQ(Pool().available_read_buffers(), 1);
}

TEST_F(LoadValueAwaitableTestPeer, PopulationChangeDuringBufferWaitSkipsIo) {
  auto held = Pool().AcquireReadBuffer().await_resume();
  ASSERT_TRUE(held.ok());
  auto read = Read();
  auto task = AwaitExisting(read);
  Start(task);
  Invalidate(true);
  held->Reset();
  ASSERT_NO_FATAL_FAILURE(Drain(task));
  EXPECT_TRUE(absl::IsNotFound(std::move(task).TakeResult().status()));
  EXPECT_FALSE(HasHelper(read));
  EXPECT_EQ(block_.pin_count(), 0);
  EXPECT_EQ(Pool().available_read_buffers(), 1);
}

TEST_F(LoadValueAwaitableTestPeer, RelocationDuringBufferWaitUsesNewLocation) {
  auto held = Pool().AcquireReadBuffer().await_resume();
  ASSERT_TRUE(held.ok());
  auto read = Read();
  auto task = AwaitExisting(read);
  Start(task, true);
  EXPECT_EQ(block_.pin_count(), 0);
  // No physical pointer was retained while waiting: recycle before handoff.
  block_.Reset(0, 8);
  block_.allocated_ = true;
  header_.allocation_epoch_ = 8;
  location_ = RecordLocation(1, 9, 8, 0, value_.size(), location_.metadata_);
  Stage();
  IndexLocation();
  held->Reset();
  ASSERT_NO_FATAL_FAILURE(Drain(task));
  ExpectValue(std::move(task).TakeResult());
  EXPECT_TRUE(HasHelper(read));
  EXPECT_EQ(block_.pin_count(), 0);
  EXPECT_EQ(Pool().available_read_buffers(), 1);
}

TEST_F(LoadValueAwaitableTestPeer, ExtentWaitRetainsPinAndReturnsSlice) {
  ASSERT_NO_FATAL_FAILURE(InstallExtent());
  auto held = Pool().AcquireReadBuffer().await_resume();
  ASSERT_TRUE(held.ok());
  std::array<std::byte, 3> output{};
  {
    auto read = ExtentSlice(ExtentReference(), 2, output);
    EXPECT_FALSE(read.await_ready());
    EXPECT_EQ(block_.pin_count(), 1);
    auto task = AwaitExtent(read);
    Start(task, true);
    held->Reset();
    EXPECT_FALSE(task.done());
    ASSERT_NO_FATAL_FAILURE(Drain(task));
    auto status = std::move(task).TakeResult();
    EXPECT_TRUE(status.ok()) << status;
    EXPECT_EQ(std::string_view(reinterpret_cast<const char*>(output.data()),
                               output.size()),
              std::string_view(value_).substr(2, output.size()));
  }
  EXPECT_EQ(block_.pin_count(), 0);
  EXPECT_EQ(Pool().available_read_buffers(), 1);
}

TEST_F(LoadValueAwaitableTestPeer, ExtentSliceChecksWholePayloadBeforeCopying) {
  ASSERT_NO_FATAL_FAILURE(InstallExtent(true));
  std::array<std::byte, 2> output{std::byte{0x55}, std::byte{0x55}};
  {
    auto read = ExtentSlice(ExtentReference(), 0, output);
    auto task = AwaitExtent(read);
    Start(task);
    ASSERT_NO_FATAL_FAILURE(Drain(task));
    auto status = std::move(task).TakeResult();
    EXPECT_TRUE(absl::IsInternal(status)) << status;
    EXPECT_EQ(status.message(), "extent payload checksum mismatch");
    EXPECT_EQ(output[0], std::byte{0x55});
    EXPECT_EQ(output[1], std::byte{0x55});
  }
  EXPECT_EQ(block_.pin_count(), 0);
  EXPECT_EQ(Pool().available_read_buffers(), 1);
}

TEST_F(LoadValueAwaitableTestPeer, ExtentWithoutCrcSkipsPayloadCheck) {
  ASSERT_NO_FATAL_FAILURE(InstallExtent(true, true));
  std::array<std::byte, 2> output{};
  {
    auto read = ExtentSlice(ExtentReference(true), 0, output);
    auto task = AwaitExtent(read);
    Start(task);
    ASSERT_NO_FATAL_FAILURE(Drain(task));
    auto status = std::move(task).TakeResult();
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(std::string_view(reinterpret_cast<const char*>(output.data()),
                               output.size()),
              value_.substr(0, output.size()));
  }
  EXPECT_EQ(block_.pin_count(), 0);
}

TEST_F(LoadValueAwaitableTestPeer, RecordWithoutCrcWorksOnDiskAndInStaging) {
  // The encoder must clear this stale CRC when the persisted policy is off.
  header_.no_crc_ = true;
  header_.payload_checksum_ ^= 1;
  {
    auto read = Read();
    ASSERT_NO_FATAL_FAILURE(Complete(read));
    ExpectValue(read.await_resume());
  }
  ASSERT_NO_FATAL_FAILURE(Stage());
  {
    auto read = Read();
    ASSERT_TRUE(read.await_ready());
    ExpectValue(read.await_resume());
  }
  EXPECT_EQ(block_.pin_count(), 0);
}

TEST_F(LoadValueAwaitableTestPeer, CompletionTransfersLeaseAndReleasesPin) {
  std::optional<Result> result;
  {
    auto read = Read();
    ASSERT_FALSE(read.await_ready());
    ASSERT_NO_FATAL_FAILURE(Complete(read));
    result = read.await_resume();
    ExpectValue(*result);
  }
  EXPECT_EQ(block_.pin_count(), 0);
  EXPECT_EQ(Pool().available_read_buffers(), 0);
  result.reset();
  EXPECT_EQ(Pool().available_read_buffers(), 1);
}

TEST_F(LoadValueAwaitableTestPeer, PopulationChangesDiscardCompletedRead) {
  for (bool database : {false, true}) {
    // Exercise valid/mismatched metadata at completion and the scheduling gap.
    for (int timing : {0, 1, 2}) {
      SCOPED_TRACE(testing::Message()
                   << "database=" << database << " timing=" << timing);
      {
        auto read = Read();
        if (timing != 2) Invalidate(database);
        if (timing == 1) ++header_.allocation_epoch_;
        ASSERT_NO_FATAL_FAILURE(Complete(read));
        if (timing == 1) --header_.allocation_epoch_;
        if (timing == 2) Invalidate(database);
        EXPECT_FALSE(HasHelper(read));
        EXPECT_TRUE(absl::IsNotFound(read.await_resume().status()));
      }
      EXPECT_EQ(block_.pin_count(), 0);
      EXPECT_EQ(Pool().available_read_buffers(), 1);
      if (database)
        ++header_.db_epoch_;
      else
        ++header_.replication_epoch_;
    }
  }
}

TEST_F(LoadValueAwaitableTestPeer, CandidatePopulationUsesMappedEpoch) {
  CandidatePopulation();
  auto read = Read();
  EXPECT_FALSE(HasHelper(read));
  ASSERT_NO_FATAL_FAILURE(Complete(read));
  ExpectValue(read.await_resume());
}

TEST_F(LoadValueAwaitableTestPeer, ErrorsReleaseResources) {
  for (int completion : {0, -EIO, 4096}) {
    {
      auto read = Read();
      ASSERT_FALSE(read.await_ready());
      header_.payload_checksum_ ^= 1;
      ASSERT_NO_FATAL_FAILURE(Complete(read, completion));
      header_.payload_checksum_ ^= 1;
      // A full CQE parses metadata only; CRC failure is discovered when the
      // caller resumes, not while the I/O poller is processing completions.
      if (completion == 4096) EXPECT_FALSE(read.await_ready());
      EXPECT_FALSE(read.await_resume().ok());
    }
    EXPECT_EQ(block_.pin_count(), 0);
    EXPECT_EQ(Pool().available_read_buffers(), 1);
  }
}

TEST_F(LoadValueAwaitableTestPeer, LocalReadReturnsStaleLocationToItsCaller) {
  block_.freeing_ = true;
  auto read = ReadLocal();
  ASSERT_TRUE(read.await_ready());
  EXPECT_FALSE(HasHelper(read));
  EXPECT_TRUE(absl::IsAborted(read.await_resume().status()));
}

TEST_F(LoadValueAwaitableTestPeer,
       RelocationRetriesBeforeResumingBackgroundCaller) {
  auto read = Read();
  auto task = AwaitCompletion(read);
  Start(task, true);
  ++header_.allocation_epoch_;
  ASSERT_NO_FATAL_FAILURE(Complete(read));
  EXPECT_FALSE(task.done());
  EXPECT_TRUE(HelperIsBackground(read));
  EXPECT_EQ(block_.pin_count(), 0);
  EXPECT_EQ(Pool().available_read_buffers(), 1);
  --header_.allocation_epoch_;
  // The new physical record has the same logical mutation and lives in staging.
  location_.metadata_ = RecordLocation::PackedMetadata::Encode(
      location_.record_offset() + 512, location_.total_disk_bytes(), 0, false,
      false, false, false, false, false, RecordKind::kValue,
      ValueType::kString);
  Stage();
  IndexLocation();
  ASSERT_NO_FATAL_FAILURE(Drain(task));
  ExpectValue(std::move(task).TakeResult());
}

TEST_F(LoadValueAwaitableTestPeer, UnchangedBadLocationIsCorruption) {
  IndexLocation();
  auto read = Read();
  auto task = AwaitCompletion(read);
  Start(task);
  ++header_.allocation_epoch_;
  ASSERT_NO_FATAL_FAILURE(Complete(read));
  ASSERT_NO_FATAL_FAILURE(Drain(task));
  EXPECT_TRUE(absl::IsInternal(std::move(task).TakeResult().status()));
}

TEST_F(LoadValueAwaitableTestPeer, ConcurrentLogicalMutationIsNotRelocation) {
  auto read = Read();
  auto task = AwaitCompletion(read);
  Start(task);
  ++header_.mutation_sequence_;
  ASSERT_NO_FATAL_FAILURE(Complete(read));
  ++location_.mutation_sequence_;
  IndexLocation();
  ASSERT_NO_FATAL_FAILURE(Drain(task));
  EXPECT_TRUE(absl::IsNotFound(std::move(task).TakeResult().status()));
}

}  // namespace lavik::storage
