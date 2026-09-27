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

#include "impl.h"

namespace lavik::storage {
namespace {

bool Before(ReplicationLogCursor left, ReplicationLogCursor right) {
  return left.lsn_ < right.lsn_ ||
         (left.lsn_ == right.lsn_ &&
          left.fragment_index_ < right.fragment_index_);
}

ReplicationLogCursor After(const ReplicationFrameHeader& frame) {
  if ((frame.flags_ & static_cast<std::uint8_t>(ReplicationFrameFlag::kLast)) !=
      0) {
    return {.lsn_ = frame.lsn_ + 1, .fragment_index_ = 0};
  }
  return {.lsn_ = frame.lsn_, .fragment_index_ = frame.fragment_index_ + 1};
}

}  // namespace

Task<absl::Status> StorageEngine::Impl::StartRedisExportDiskBacklog(
    std::uint64_t session_id, std::size_t capacity_bytes) {
  WorkerStore& store = CurrentStore();
  auto& disk = store.redis_export_disk_backlog_;
  co_await disk.mutex_.Lock();
  UnlockGuard unlock(&disk.mutex_, store.worker_);
  if (session_id == 0 || capacity_bytes < kStorageBlockBytes) {
    co_return absl::InvalidArgumentError(
        "invalid Redis export disk backlog bounds");
  }
  if (disk.session_id_ != 0 || disk.active_buffer_ != nullptr ||
      !disk.blocks_.empty()) {
    co_return absl::AlreadyExistsError("Redis export disk backlog is active");
  }
  disk.session_id_ = session_id;
  disk.start_lsn_ = 1;
  disk.next_stream_lsn_ = 1;
  disk.max_blocks_ = capacity_bytes / kStorageBlockBytes;
  LAVIK_FAULT_INJECT(
      if (const char* limit = std::getenv("LAVIK_TEST_REDIS_EXPORT_ONE_BLOCK");
          limit != nullptr && std::strcmp(limit, "1") == 0) {
        disk.max_blocks_ = 1;
      });
  disk.capturing_ = true;
  disk.failure_.reset();
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::AppendRedisExportDiskBytes(
    std::uint64_t session_id, std::string_view bytes) {
  WorkerStore& store = CurrentStore();
  auto& disk = store.redis_export_disk_backlog_;
  if (disk.session_id_ != session_id || !disk.capturing_ ||
      bytes.size() > kMaxRecordPayloadBytes ||
      disk.next_stream_lsn_ == std::numeric_limits<std::uint64_t>::max()) {
    co_return absl::FailedPreconditionError(
        "Redis export disk stream cannot accept this command");
  }
  ReplicationFrameHeader frame{
      .header_bytes_ = sizeof(ReplicationFrameHeader),
      .kind_ = ReplicationEventKind::kMutation,
      .flags_ = static_cast<std::uint8_t>(ReplicationFrameFlag::kFirst) |
                static_cast<std::uint8_t>(ReplicationFrameFlag::kLast),
      .lsn_ = disk.next_stream_lsn_,
      .partition_sequence_ = disk.next_stream_lsn_,
      .payload_bytes_ = static_cast<std::uint32_t>(bytes.size()),
  };
  absl::Status appended = co_await AppendRedisExportDiskFrame(
      store, frame,
      std::span<const std::byte>(
          reinterpret_cast<const std::byte*>(bytes.data()), bytes.size()));
  if (!appended.ok()) co_return appended;
  ++disk.next_stream_lsn_;
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::SealRedisExportDiskBlock(
    WorkerStore& store) {
  auto& disk = store.redis_export_disk_backlog_;
  if (disk.active_buffer_ == nullptr || disk.blocks_.empty()) {
    co_return absl::FailedPreconditionError("no Redis export block to seal");
  }
  auto& block = disk.blocks_.back();
  if (block.frame_count_ == 0) {
    co_return absl::InternalError("empty Redis export block");
  }
  BlockHeader header{
      .block_id_ = block.id_,
      .writer_id_ = static_cast<std::uint32_t>(store.worker_->id()),
      .allocation_epoch_ = block.allocation_epoch_,
      .committed_bytes_ = block.committed_bytes_,
      .record_count_ = block.frame_count_,
      .layout_worker_count_ = worker_count_,
      .kind_ = BlockKind::kRedisExportBacklog,
  };
  EncodeBlockHeader(header, std::span<std::byte, kBlockHeaderSlotBytes>(
                                disk.active_buffer_, kBlockHeaderSlotBytes));
  const std::size_t write_bytes = AlignDirect(block.committed_bytes_);
  const auto [file_index, offset] = FileOffset(block.id_);
  auto written = co_await WriteStorageBuffer(
      *store.worker_, store.files_[file_index],
      std::span<const std::byte>(disk.active_buffer_, write_bytes), false, {},
      offset);
  if (!written.ok() || *written != write_bytes) {
    co_return written.ok()
        ? absl::InternalError("short Redis export backlog block write")
        : written.status();
  }
  block.sealed_ = true;
  bycorf::FreeStorageBuffer(disk.active_buffer_, kDirectIoAlignment);
  disk.active_buffer_ = nullptr;
  disk.active_buffer_charge_.Reset();
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::AppendRedisExportDiskFrame(
    WorkerStore& store, const ReplicationFrameHeader& source,
    std::span<const std::byte> payload) {
  auto& disk = store.redis_export_disk_backlog_;
  co_await disk.mutex_.Lock();
  UnlockGuard unlock(&disk.mutex_, store.worker_);
  if (!disk.capturing_ || disk.failure_.has_value() ||
      source.lsn_ < disk.start_lsn_) {
    co_return absl::OkStatus();
  }
  const bool source_first =
      (source.flags_ &
       static_cast<std::uint8_t>(ReplicationFrameFlag::kFirst)) != 0;
  const bool source_last =
      (source.flags_ &
       static_cast<std::uint8_t>(ReplicationFrameFlag::kLast)) != 0;
  if (source_first) {
    disk.active_event_lsn_ = source.lsn_;
    disk.next_fragment_index_ = 0;
  } else if (disk.active_event_lsn_ != source.lsn_) {
    disk.failure_ =
        absl::InternalError("Redis export event fragments are out of order");
    disk.capturing_ = false;
    co_return *disk.failure_;
  }

  std::size_t consumed = 0;
  bool emitted = false;
  while (!emitted || consumed < payload.size()) {
    if (disk.active_buffer_ == nullptr) {
      if (disk.blocks_.size() >= disk.max_blocks_) {
        disk.failure_ = absl::ResourceExhaustedError(
            "Redis export disk backlog capacity exhausted");
        break;
      }
      auto reserved =
          co_await AllocateBlock(store, AllocationPurpose::kRedisExportBacklog);
      if (!reserved.ok()) {
        disk.failure_ = reserved.status();
        break;
      }
      auto staging = TryReserveMemory(kStorageBlockBytes);
      if (!staging.has_value()) {
        RecordMemoryRejection();
        disk.failure_ = absl::ResourceExhaustedError(
            "Redis export disk staging exceeds maxmemory");
        absl::Status returned =
            co_await ReturnColdBlocks({reserved->block_id_});
        if (!returned.ok()) disk.failure_ = returned;
        break;
      }
      auto* buffer = static_cast<std::byte*>(bycorf::AllocateStorageBuffer(
          kStorageBlockBytes, kDirectIoAlignment));
      if (buffer == nullptr) {
        disk.failure_ = absl::ResourceExhaustedError(
            "Redis export disk staging buffer allocation failed");
        absl::Status returned =
            co_await ReturnColdBlocks({reserved->block_id_});
        if (!returned.ok()) disk.failure_ = returned;
        break;
      }
      std::fill_n(buffer, kStorageBlockBytes, std::byte{0});
      disk.active_buffer_ = buffer;
      disk.active_buffer_charge_.Adopt(&*staging, kStorageBlockBytes);
      disk.blocks_.push_back(WorkerStore::RedisExportDiskBacklog::Block{
          .id_ = reserved->block_id_,
          .allocation_epoch_ = reserved->allocation_epoch_,
      });
    }
    auto& block = disk.blocks_.back();
    const std::size_t remaining = kStorageBlockBytes - block.committed_bytes_;
    if (remaining <= sizeof(ReplicationFrameHeader)) {
      absl::Status sealed = co_await SealRedisExportDiskBlock(store);
      if (!sealed.ok()) {
        disk.failure_ = sealed;
        break;
      }
      continue;
    }
    const std::size_t part = std::min(
        payload.size() - consumed, remaining - sizeof(ReplicationFrameHeader));
    const std::size_t frame_bytes =
        AlignRecord(sizeof(ReplicationFrameHeader) + part);
    const std::size_t frame_offset = block.committed_bytes_;
    std::byte* frame_data = disk.active_buffer_ + frame_offset;
    if (part != 0) {
      std::memcpy(frame_data + sizeof(ReplicationFrameHeader),
                  payload.data() + consumed, part);
    }
    std::fill(frame_data + sizeof(ReplicationFrameHeader) + part,
              frame_data + frame_bytes, std::byte{0});
    const bool first = disk.next_fragment_index_ == 0;
    const bool last = source_last && consumed + part == payload.size();
    ReplicationFrameHeader frame = source;
    frame.flags_ = static_cast<std::uint8_t>(
        (first ? static_cast<std::uint8_t>(ReplicationFrameFlag::kFirst) : 0) |
        (last ? static_cast<std::uint8_t>(ReplicationFrameFlag::kLast) : 0));
    frame.payload_bytes_ = static_cast<std::uint32_t>(part);
    frame.total_disk_bytes_ = static_cast<std::uint32_t>(frame_bytes);
    frame.fragment_index_ = disk.next_fragment_index_++;
    frame.payload_checksum_ = Crc32c(std::span<const std::byte>(
        frame_data + sizeof(ReplicationFrameHeader), part));
    if (!EncodeReplicationFrameHeader(
            frame, std::span<std::byte, sizeof(ReplicationFrameHeader)>(
                       frame_data, sizeof(ReplicationFrameHeader)))) {
      disk.failure_ = absl::InternalError("Redis export frame encoding failed");
      break;
    }
    if (block.frame_count_ == 0) block.first_lsn_ = source.lsn_;
    block.last_lsn_ = source.lsn_;
    block.committed_bytes_ += static_cast<std::uint32_t>(frame_bytes);
    ++block.frame_count_;
    consumed += part;
    emitted = true;
    if (block.committed_bytes_ == kStorageBlockBytes) {
      absl::Status sealed = co_await SealRedisExportDiskBlock(store);
      if (!sealed.ok()) {
        disk.failure_ = sealed;
        break;
      }
    }
  }
  if (source_last) disk.active_event_lsn_ = 0;
  if (disk.failure_.has_value()) {
    // A failed export never takes down the primary's ordinary replication log.
    disk.capturing_ = false;
    co_return *disk.failure_;
  }
  co_return absl::OkStatus();
}

Task<absl::StatusOr<std::uint64_t>>
StorageEngine::Impl::StopRedisExportDiskBacklog(std::uint64_t session_id) {
  WorkerStore& store = CurrentStore();
  auto& disk = store.redis_export_disk_backlog_;
  co_await disk.mutex_.Lock();
  UnlockGuard unlock(&disk.mutex_, store.worker_);
  if (disk.session_id_ != session_id || session_id == 0) {
    co_return absl::FailedPreconditionError(
        "Redis export disk session changed");
  }
  disk.capturing_ = false;
  if (disk.failure_.has_value()) co_return *disk.failure_;
  if (disk.active_event_lsn_ != 0) {
    co_return absl::InternalError("Redis export stopped within an event");
  }
  if (disk.active_buffer_ != nullptr) {
    absl::Status sealed = co_await SealRedisExportDiskBlock(store);
    if (!sealed.ok()) {
      disk.failure_ = sealed;
      co_return sealed;
    }
  }
  disk.end_lsn_ = disk.next_stream_lsn_;
  spdlog::info("Redis PSYNC disk backlog session={} blocks={} commands={}",
               session_id, disk.blocks_.size(), disk.end_lsn_ - 1);
  if (!disk.blocks_.empty()) {
    LAVIK_MAYBE_CRASH_AT("redis_export_backlog_stopped");
  }
  co_return disk.end_lsn_;
}

Task<absl::StatusOr<ReplicationLogBatch>>
StorageEngine::Impl::ReadRedisExportDiskBacklog(std::uint64_t session_id,
                                                ReplicationLogCursor next,
                                                std::size_t max_bytes,
                                                std::size_t max_frames) {
  WorkerStore& store = CurrentStore();
  auto& disk = store.redis_export_disk_backlog_;
  co_await disk.mutex_.Lock();
  UnlockGuard unlock(&disk.mutex_, store.worker_);
  if (disk.session_id_ != session_id || disk.end_lsn_ == 0 ||
      disk.failure_.has_value() || next.lsn_ < disk.start_lsn_ ||
      next.lsn_ > disk.end_lsn_ || max_bytes == 0 || max_frames == 0) {
    co_return absl::FailedPreconditionError(
        "Redis export disk backlog is unavailable for this cursor");
  }
  ReplicationLogBatch batch{.next_ = next, .frames_ = {}};
  if (next.lsn_ == disk.end_lsn_) {
    if (next.fragment_index_ != 0) {
      co_return absl::InvalidArgumentError("invalid Redis export end cursor");
    }
    batch.at_tail_ = true;
    co_return batch;
  }
  std::size_t total_bytes = 0;
  for (const auto& block : disk.blocks_) {
    if (block.last_lsn_ < batch.next_.lsn_) continue;
    if (!block.sealed_) {
      co_return absl::InternalError("unsealed Redis export disk block");
    }
    const std::size_t read_bytes = AlignDirect(block.committed_bytes_);
    auto acquired = co_await store.buffers_.AcquireReadBuffer(read_bytes);
    if (!acquired.ok()) co_return acquired.status();
    ReadBufferLease lease = std::move(*acquired);
    FixedBuffer buffer = lease.io_buffer();
    if (buffer.size_ < read_bytes) {
      co_return absl::ResourceExhaustedError(
          "Redis export read buffer is too small");
    }
    buffer.size_ = read_bytes;
    const auto [file_index, offset] = FileOffset(block.id_);
    auto read =
        co_await ReadStorageBuffer(*store.worker_, store.files_[file_index],
                                   buffer, lease.registered(), offset);
    if (!read.ok() || *read != read_bytes) {
      co_return read.ok()
          ? absl::InternalError("short Redis export disk backlog read")
          : read.status();
    }
    BlockHeader decoded{};
    if (!DecodeBlockHeaderPages(std::span<const std::byte, kBlockHeaderBytes>(
                                    buffer.data_, kBlockHeaderBytes),
                                &decoded) ||
        decoded.kind_ != BlockKind::kRedisExportBacklog ||
        decoded.block_id_ != block.id_ ||
        decoded.allocation_epoch_ != block.allocation_epoch_ ||
        decoded.committed_bytes_ != block.committed_bytes_) {
      co_return absl::DataLossError("invalid Redis export disk block header");
    }
    std::uint32_t frame_offset = kBlockHeaderBytes;
    while (frame_offset < block.committed_bytes_) {
      const std::span<const std::byte> available(
          buffer.data_ + frame_offset, block.committed_bytes_ - frame_offset);
      ReplicationFrameHeader frame{};
      if (!DecodeReplicationFrameHeader(available, &frame) ||
          frame.total_disk_bytes_ > available.size()) {
        co_return absl::DataLossError("invalid Redis export disk frame");
      }
      const ReplicationLogCursor found{
          .lsn_ = frame.lsn_, .fragment_index_ = frame.fragment_index_};
      if (Before(found, batch.next_)) {
        frame_offset += frame.total_disk_bytes_;
        continue;
      }
      if (Before(batch.next_, found)) {
        co_return absl::DataLossError("Redis export disk cursor has a gap");
      }
      const auto payload = available.subspan(sizeof(ReplicationFrameHeader),
                                             frame.payload_bytes_);
      if (Crc32c(payload) != frame.payload_checksum_) {
        co_return absl::DataLossError("Redis export disk frame CRC mismatch");
      }
      const std::size_t emitted_bytes =
          sizeof(ReplicationFrameHeader) + frame.payload_bytes_;
      if (!batch.frames_.empty() && (batch.frames_.size() >= max_frames ||
                                     total_bytes + emitted_bytes > max_bytes)) {
        batch.at_tail_ = batch.next_.lsn_ == disk.end_lsn_;
        co_return batch;
      }
      ReplicationLogFrame output{.header_ = frame, .payload_ = {}};
      output.payload_.assign(reinterpret_cast<const char*>(payload.data()),
                             payload.size());
      batch.frames_.push_back(std::move(output));
      total_bytes += emitted_bytes;
      batch.next_ = After(frame);
      frame_offset += frame.total_disk_bytes_;
      if (batch.frames_.size() >= max_frames || total_bytes >= max_bytes) {
        batch.at_tail_ = batch.next_.lsn_ == disk.end_lsn_;
        co_return batch;
      }
    }
  }
  if (batch.frames_.empty()) {
    co_return absl::DataLossError("Redis export disk cursor was not retained");
  }
  batch.at_tail_ = batch.next_.lsn_ == disk.end_lsn_;
  co_return batch;
}

Task<absl::Status> StorageEngine::Impl::ReleaseRedisExportDiskBacklog(
    std::uint64_t session_id) {
  WorkerStore& store = CurrentStore();
  auto& disk = store.redis_export_disk_backlog_;
  co_await disk.mutex_.Lock();
  UnlockGuard unlock(&disk.mutex_, store.worker_);
  if (disk.session_id_ == 0) co_return absl::OkStatus();
  if (disk.session_id_ != session_id) {
    co_return absl::FailedPreconditionError(
        "Redis export disk session changed");
  }
  disk.capturing_ = false;
  if (disk.active_buffer_ != nullptr) {
    bycorf::FreeStorageBuffer(disk.active_buffer_, kDirectIoAlignment);
    disk.active_buffer_ = nullptr;
  }
  disk.active_buffer_charge_.Reset();
  std::vector<std::uint64_t> released;
  released.reserve(disk.blocks_.size());
  for (const auto& block : disk.blocks_) released.push_back(block.id_);
  disk.blocks_.clear();
  disk.session_id_ = 0;
  disk.start_lsn_ = 0;
  disk.end_lsn_ = 0;
  disk.next_stream_lsn_ = 1;
  disk.active_event_lsn_ = 0;
  disk.next_fragment_index_ = 0;
  disk.max_blocks_ = 0;
  disk.failure_.reset();
  if (released.empty()) co_return absl::OkStatus();
  co_return co_await ReturnColdBlocks(std::move(released));
}

}  // namespace lavik::storage
