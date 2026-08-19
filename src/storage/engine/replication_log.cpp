#include "impl.h"

#include <optional>

#include "keylane/replication_command.h"

namespace keylane::storage {
namespace {

constexpr std::size_t kSparseFrameStride = 64;

bool CursorBefore(const ReplicationLogCursor& left,
                  const ReplicationLogCursor& right) noexcept {
  return left.lsn_ < right.lsn_ ||
         (left.lsn_ == right.lsn_ &&
          left.fragment_index_ < right.fragment_index_);
}

ReplicationLogCursor FrameCursor(const ReplicationFrameHeader& frame) {
  return {.lsn_ = frame.lsn_, .fragment_index_ = frame.fragment_index_};
}

ReplicationLogCursor CursorAfter(const ReplicationFrameHeader& frame) {
  if ((frame.flags_ & static_cast<std::uint8_t>(ReplicationFrameFlag::kLast)) !=
      0) {
    return {.lsn_ = frame.lsn_ + 1, .fragment_index_ = 0};
  }
  return {.lsn_ = frame.lsn_, .fragment_index_ = frame.fragment_index_ + 1};
}

absl::Status InvalidState(std::string_view message) {
  return absl::Status(absl::StatusCode::kFailedPrecondition, message);
}

std::optional<std::size_t> TransactionLogicalBytes(
    const ReplicationTransaction& transaction) {
  std::size_t logical_bytes = sizeof(ReplicationEventKind) +
                              sizeof(transaction.db_id_) +
                              sizeof(std::uint16_t) + sizeof(std::uint64_t);
  for (const std::string& arg : transaction.envelope_args_) {
    if (arg.size() > std::numeric_limits<std::size_t>::max() - logical_bytes ||
        sizeof(std::uint32_t) > std::numeric_limits<std::size_t>::max() -
                                    logical_bytes - arg.size()) {
      return std::nullopt;
    }
    logical_bytes += sizeof(std::uint32_t) + arg.size();
  }
  return logical_bytes;
}

}  // namespace

Task<absl::Status> StorageEngine::Impl::EnableReplicationLog(
    std::uint64_t log_epoch, std::size_t capacity_bytes) {
  WorkerStore& store = CurrentStore();
  auto& log = store.replication_log_;
  co_await log.mutex_.Lock();
  UnlockGuard unlock(&log.mutex_, store.worker_);
  if (log.state_ != ReplicationLogState::kDisabled) {
    if (log.state_ == ReplicationLogState::kActive) {
      co_return absl::OkStatus();
    }
    co_return InvalidState("replication log is already enabled");
  }
  if (log_epoch == 0 || capacity_bytes == 0) {
    co_return absl::Status(
        absl::StatusCode::kInvalidArgument,
        "replication log epoch and capacity must be nonzero");
  }
  log.state_ = ReplicationLogState::kActive;
  log.log_epoch_ = log_epoch;
  log.next_lsn_ = 1;
  log.max_blocks_ =
      std::max<std::size_t>(1, capacity_bytes / kStorageBlockBytes);
  log.publish_queue_.clear();
  log.publish_queue_bytes_ = 0;
  log.reserved_memory_bytes_ = std::min(
      log.max_blocks_ * kStorageBlockBytes,
      options_.replication_publish_queue_bytes_);
  log.max_publish_queue_bytes_ = log.reserved_memory_bytes_;
  co_return absl::OkStatus();
}

bool StorageEngine::Impl::ReplicationLogActive() const noexcept {
  return CurrentStore().replication_log_.state_ == ReplicationLogState::kActive;
}

bool StorageEngine::Impl::TryEnqueueReplicationCommand(
    ReplicationCommandAppend command) {
  WorkerStore& store = CurrentStore();
  auto& log = store.replication_log_;
  if (log.state_ != ReplicationLogState::kActive) return false;

  std::size_t logical_bytes = sizeof(command.kind_) + sizeof(command.db_id_) +
                              sizeof(command.partition_id_) +
                              sizeof(command.partition_sequence_);
  for (const std::string& arg : command.args_) {
    if (arg.size() > std::numeric_limits<std::size_t>::max() - logical_bytes ||
        sizeof(std::uint32_t) > std::numeric_limits<std::size_t>::max() -
                                    logical_bytes - arg.size()) {
      log.state_ = ReplicationLogState::kInvalid;
      log.publish_queue_.clear();
      log.publish_queue_bytes_ = 0;
      spdlog::warn("replication publisher queue size overflow");
      return false;
    }
    logical_bytes += sizeof(std::uint32_t) + arg.size();
  }
  if (command.args_.empty() || logical_bytes > log.max_publish_queue_bytes_ ||
      log.publish_queue_bytes_ > log.max_publish_queue_bytes_ - logical_bytes) {
    log.state_ = ReplicationLogState::kInvalid;
    log.publish_queue_.clear();
    log.publish_queue_bytes_ = 0;
    spdlog::warn(
        "replication publisher cannot keep up; invalidating backlog for full "
        "resynchronization");
    return false;
  }

  log.publish_queue_bytes_ += logical_bytes;
  log.publish_queue_.push_back(
      WorkerStore::ReplicationLogRuntime::PendingCommand{
          .log_epoch_ = log.log_epoch_,
          .logical_bytes_ = logical_bytes,
          .append_ = std::move(command),
          .fence_ = nullptr,
          .transaction_ = nullptr,
      });
  if (!log.publisher_running_) {
    log.publisher_running_ = true;
    store.worker_->Spawn(DrainReplicationPublishQueue(&store));
  }
  return true;
}

bool StorageEngine::Impl::TryEnqueueReplicationTransaction(
    std::shared_ptr<ReplicationTransaction> transaction) {
  WorkerStore& store = CurrentStore();
  auto& log = store.replication_log_;
  if (log.state_ != ReplicationLogState::kActive || transaction == nullptr ||
      transaction->envelope_args_.empty()) {
    return false;
  }

  std::size_t logical_bytes = sizeof(ReplicationEventKind) +
                              sizeof(transaction->db_id_) +
                              sizeof(std::uint16_t) + sizeof(std::uint64_t);
  for (const std::string& arg : transaction->envelope_args_) {
    if (arg.size() > std::numeric_limits<std::size_t>::max() - logical_bytes ||
        sizeof(std::uint32_t) > std::numeric_limits<std::size_t>::max() -
                                    logical_bytes - arg.size()) {
      log.state_ = ReplicationLogState::kInvalid;
      spdlog::warn("replication transaction queue size overflow");
      return false;
    }
    logical_bytes += sizeof(std::uint32_t) + arg.size();
  }
  if (logical_bytes > log.max_publish_queue_bytes_ ||
      log.publish_queue_bytes_ > log.max_publish_queue_bytes_ - logical_bytes) {
    log.state_ = ReplicationLogState::kInvalid;
    spdlog::warn(
        "replication transaction publisher cannot keep up; invalidating "
        "backlog for full resynchronization");
    return false;
  }

  log.publish_queue_bytes_ += logical_bytes;
  log.publish_queue_.push_back(
      WorkerStore::ReplicationLogRuntime::PendingCommand{
          .log_epoch_ = log.log_epoch_,
          .logical_bytes_ = logical_bytes,
          .append_ = {},
          .fence_ = nullptr,
          .transaction_ = std::move(transaction),
      });
  if (!log.publisher_running_) {
    log.publisher_running_ = true;
    store.worker_->Spawn(DrainReplicationPublishQueue(&store));
  }
  return true;
}

Task<absl::StatusOr<std::uint64_t>>
StorageEngine::Impl::FenceReplicationLog() {
  WorkerStore& store = CurrentStore();
  auto& log = store.replication_log_;
  if (log.state_ != ReplicationLogState::kActive) {
    co_return InvalidState("replication log is not active");
  }

  auto fence = std::make_shared<WorkerStore::ReplicationLogRuntime::PublishFence>();
  log.publish_queue_.push_back(
      WorkerStore::ReplicationLogRuntime::PendingCommand{
          .log_epoch_ = log.log_epoch_,
          .logical_bytes_ = 0,
          .append_ = {},
          .fence_ = fence,
          .transaction_ = nullptr,
      });
  if (!log.publisher_running_) {
    log.publisher_running_ = true;
    store.worker_->Spawn(DrainReplicationPublishQueue(&store));
  }
  while (!fence->complete_) {
    co_await fence->ready_.Wait();
  }
  if (!fence->status_.ok()) co_return fence->status_;
  co_return fence->next_lsn_;
}

Task<absl::Status> StorageEngine::Impl::DrainReplicationPublishQueue(
    WorkerStore* store) {
  auto& log = store->replication_log_;
  while (!log.publish_queue_.empty()) {
    auto pending = std::move(log.publish_queue_.front());
    log.publish_queue_.pop_front();
    if (pending.fence_ != nullptr) {
      if (log.state_ == ReplicationLogState::kActive &&
          pending.log_epoch_ == log.log_epoch_) {
        pending.fence_->next_lsn_ = log.next_lsn_;
        pending.fence_->status_ = absl::OkStatus();
      } else {
        pending.fence_->status_ =
            InvalidState("replication log changed before publisher fence");
      }
      pending.fence_->complete_ = true;
      pending.fence_->ready_.NotifyAll(*store->worker_);
      continue;
    }
    if (pending.transaction_ != nullptr) {
      while (pending.transaction_->resolution_.load(
                 std::memory_order_acquire) ==
             ReplicationTransactionResolution::kPending) {
        co_await celer::Yield(*store->worker_);
      }
      if (pending.transaction_->resolution_.load(std::memory_order_acquire) ==
          ReplicationTransactionResolution::kDiscard) {
        if (log.publish_queue_bytes_ >= pending.logical_bytes_) {
          log.publish_queue_bytes_ -= pending.logical_bytes_;
        }
        continue;
      }
      const auto final_logical_bytes =
          TransactionLogicalBytes(*pending.transaction_);
      if (!final_logical_bytes.has_value()) {
        log.state_ = ReplicationLogState::kInvalid;
        spdlog::warn("replication transaction queue size overflow");
        break;
      }
      if (*final_logical_bytes > pending.logical_bytes_) {
        const std::size_t growth =
            *final_logical_bytes - pending.logical_bytes_;
        if (growth > log.max_publish_queue_bytes_ -
                         std::min(log.publish_queue_bytes_,
                                  log.max_publish_queue_bytes_)) {
          log.state_ = ReplicationLogState::kInvalid;
          spdlog::warn(
              "canonical replication transaction exceeds publisher queue");
          break;
        }
        log.publish_queue_bytes_ += growth;
      } else {
        log.publish_queue_bytes_ -=
            std::min(log.publish_queue_bytes_,
                     pending.logical_bytes_ - *final_logical_bytes);
      }
      pending.logical_bytes_ = *final_logical_bytes;
      pending.append_ = ReplicationCommandAppend{
          .kind_ = ReplicationEventKind::kTransaction,
          .db_id_ = pending.transaction_->db_id_,
          .partition_id_ =
              static_cast<std::uint16_t>(celer::ThisWorker().id_),
          .partition_sequence_ = pending.transaction_->id_,
          .args_ = pending.transaction_->envelope_args_,
      };
    }
    if (log.state_ != ReplicationLogState::kActive ||
        pending.log_epoch_ != log.log_epoch_) {
      if (log.publish_queue_bytes_ >= pending.logical_bytes_) {
        log.publish_queue_bytes_ -= pending.logical_bytes_;
      }
      continue;
    }

    std::vector<std::string_view> args;
    args.reserve(pending.append_.args_.size());
    for (const std::string& arg : pending.append_.args_) args.push_back(arg);
    auto source =
        ReplicationCommandPayloadSource::Create(pending.append_.db_id_, args);
    if (!source.ok()) {
      log.state_ = ReplicationLogState::kInvalid;
      spdlog::warn("replication command encoding failed: {}",
                   source.status().message());
      break;
    }
    auto appended = co_await AppendReplicationLog(ReplicationLogAppend{
        .kind_ = pending.append_.kind_,
        .partition_id_ = pending.append_.partition_id_,
        .partition_sequence_ = pending.append_.partition_sequence_,
        .payload_ = {},
        .payload_source_ = &*source,
    });
    if (!appended.ok()) {
      log.state_ = ReplicationLogState::kInvalid;
      spdlog::warn("replication backlog append failed: {}",
                   appended.status().message());
      break;
    }
    if (log.publish_queue_bytes_ >= pending.logical_bytes_) {
      log.publish_queue_bytes_ -= pending.logical_bytes_;
    }
  }
  if (log.state_ != ReplicationLogState::kActive) {
    for (auto& pending : log.publish_queue_) {
      if (pending.fence_ == nullptr) continue;
      pending.fence_->status_ =
          InvalidState("replication publisher failed before fence");
      pending.fence_->complete_ = true;
      pending.fence_->ready_.NotifyAll(*store->worker_);
    }
    log.publish_queue_.clear();
    log.publish_queue_bytes_ = 0;
  }
  log.publisher_running_ = false;
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::PublishFlushDbReplication(
    std::uint8_t db_id, std::uint64_t db_epoch) {
  if (db_id >= kLogicalDatabaseCount || db_epoch == 0) {
    co_return absl::Status(absl::StatusCode::kInvalidArgument,
                           "invalid FLUSHDB replication barrier");
  }
  for (unsigned target = 0; target < worker_count_; ++target) {
    auto publish = [this, db_id, db_epoch]() -> Task<absl::Status> {
      if (ReplicationLogActive()) {
        (void)TryEnqueueReplicationCommand(ReplicationCommandAppend{
            .kind_ = ReplicationEventKind::kControl,
            .db_id_ = db_id,
            // Control events do not belong to a partition. Zero is the
            // canonical transport placeholder; receivers key the barrier by
            // (db_id, db_epoch) carried in the command payload.
            .partition_id_ = 0,
            .partition_sequence_ = db_epoch,
            .args_ = {"FLUSHDB", std::to_string(db_epoch)},
        });
      }
      co_return absl::OkStatus();
    };
    absl::Status published;
    if (target == celer::ThisWorker().id_) {
      published = co_await publish();
    } else {
      published = co_await celer::SubmitTaskTo(target, publish);
    }
    if (!published.ok()) co_return published;
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::ReclaimReplicationLogPrefix(
    WorkerStore& store, std::uint64_t keep_from_lsn, bool force_all) {
  auto& log = store.replication_log_;
  std::vector<std::uint64_t> released;
  while (!log.blocks_.empty()) {
    const auto& front = log.blocks_.front();
    if (!force_all && (!front.sealed_ || front.last_lsn_ >= keep_from_lsn)) {
      break;
    }
    released.push_back(front.block_id_);
    log.blocks_.pop_front();
  }
  if (released.empty()) {
    co_return absl::OkStatus();
  }
  co_await store.store_state_mutex_.Lock();
  for (const std::uint64_t block_id : released) {
    DestroyBlockState(store, block_id);
  }
  store.store_state_mutex_.Unlock(*store.worker_);
  absl::Status returned = co_await ReturnColdBlocks(std::move(released));
  if (!returned.ok()) {
    log.state_ = ReplicationLogState::kInvalid;
  }
  co_return returned;
}

Task<absl::Status> StorageEngine::Impl::EnsureReplicationLogActiveBlock(
    WorkerStore& store, std::uint64_t protected_lsn) {
  auto& log = store.replication_log_;
  if (log.active_buffer_ != nullptr) {
    co_return absl::OkStatus();
  }
  if (log.max_blocks_ == 0) {
    co_return InvalidState("replication log has no capacity");
  }

  while (log.blocks_.size() >= log.max_blocks_) {
    if (log.blocks_.empty() || !log.blocks_.front().sealed_ ||
        log.blocks_.front().last_lsn_ >= protected_lsn) {
      co_return absl::Status(
          absl::StatusCode::kResourceExhausted,
          "one replication event exceeds the configured backlog capacity");
    }
    std::uint64_t evicted_through = log.blocks_.front().last_lsn_;
    std::vector<std::uint64_t> released;
    do {
      evicted_through =
          std::max(evicted_through, log.blocks_.front().last_lsn_);
      released.push_back(log.blocks_.front().block_id_);
      log.blocks_.pop_front();
    } while (!log.blocks_.empty() && log.blocks_.front().sealed_ &&
             log.blocks_.front().first_lsn_ <= evicted_through);

    co_await store.store_state_mutex_.Lock();
    for (const std::uint64_t block_id : released) {
      DestroyBlockState(store, block_id);
    }
    store.store_state_mutex_.Unlock(*store.worker_);
    absl::Status returned = co_await ReturnColdBlocks(std::move(released));
    if (!returned.ok()) {
      co_return returned;
    }
  }

  auto reserved =
      co_await AllocateBlock(store, AllocationPurpose::kReplication);
  if (!reserved.ok()) {
    co_return reserved.status();
  }
  auto* buffer = static_cast<std::byte*>(
      celer::AllocateStorageBuffer(kStorageBlockBytes, kDirectIoAlignment));
  if (buffer == nullptr) {
    absl::Status returned = co_await ReturnReservedBlock(*reserved);
    co_return returned.ok()
        ? absl::Status(absl::StatusCode::kResourceExhausted,
                       "failed to allocate replication staging block")
        : returned;
  }
  std::fill_n(buffer, kStorageBlockBytes, std::byte{0});

  co_await store.store_state_mutex_.Lock();
  BlockState& state = CreateBlockState(store, reserved->block_id_);
  state.writer_id_ = static_cast<std::uint16_t>(store.worker_->id());
  state.layout_worker_count_ = static_cast<std::uint16_t>(worker_count_);
  state.allocation_epoch_ = reserved->allocation_epoch_;
  state.committed_bytes_ = kBlockHeaderBytes;
  state.live_bytes_ = 0;
  state.allocated_ = true;
  state.in_memory_ = true;
  state.kind_ = BlockKind::kReplicationLog;
  store.store_state_mutex_.Unlock(*store.worker_);

  log.active_buffer_ = buffer;
  log.blocks_.push_back(WorkerStore::ReplicationLogBlock{
      .block_id_ = reserved->block_id_,
      .allocation_epoch_ = reserved->allocation_epoch_,
      .sparse_offsets_ = {},
  });
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::SealReplicationLogActiveBlock(
    WorkerStore& store) {
  auto& log = store.replication_log_;
  if (log.active_buffer_ == nullptr || log.blocks_.empty() ||
      log.blocks_.back().sealed_) {
    co_return InvalidState("replication log has no active block");
  }
  auto& block = log.blocks_.back();
  if (block.frame_count_ == 0 || block.first_lsn_ == 0) {
    co_return InvalidState("cannot seal an empty replication block");
  }

  BlockHeader header{
      .block_id_ = block.block_id_,
      .writer_id_ = static_cast<std::uint32_t>(store.worker_->id()),
      .allocation_epoch_ = block.allocation_epoch_,
      .committed_bytes_ = block.committed_bytes_,
      .record_count_ = block.frame_count_,
      .max_lsn_ = 0,
      .layout_worker_count_ = worker_count_,
      .kind_ = BlockKind::kReplicationLog,
      .replication_log_epoch_ = log.log_epoch_,
      .first_replication_lsn_ = block.first_lsn_,
      .last_replication_lsn_ = block.last_lsn_,
  };
  EncodeBlockHeader(header, std::span<std::byte, kBlockHeaderSlotBytes>(
                                log.active_buffer_, kBlockHeaderSlotBytes));
  const std::size_t write_bytes = AlignDirect(block.committed_bytes_);
  const auto [file_index, offset] = FileOffset(block.block_id_);
  auto written = co_await WriteStorageBuffer(
      *store.worker_, store.files_[file_index],
      std::span<const std::byte>(log.active_buffer_, write_bytes), false, {},
      offset);
  if (!written.ok() || *written != write_bytes) {
    celer::FreeStorageBuffer(log.active_buffer_, kDirectIoAlignment);
    log.active_buffer_ = nullptr;
    log.state_ = ReplicationLogState::kInvalid;
    co_return written.ok()
        ? absl::Status(absl::StatusCode::kInternal,
                       "short replication backlog block write")
        : written.status();
  }

  block.sealed_ = true;
  co_await store.store_state_mutex_.Lock();
  BlockState* state = FindBlockState(store, block.block_id_);
  if (state == nullptr || state->allocation_epoch_ != block.allocation_epoch_) {
    store.store_state_mutex_.Unlock(*store.worker_);
    celer::FreeStorageBuffer(log.active_buffer_, kDirectIoAlignment);
    log.active_buffer_ = nullptr;
    log.state_ = ReplicationLogState::kInvalid;
    co_return absl::Status(absl::StatusCode::kInternal,
                           "replication block lost its allocation identity");
  }
  state->committed_bytes_ = block.committed_bytes_;
  state->live_bytes_ = block.committed_bytes_ - kBlockHeaderBytes;
    // Runtime backlog persistence is intentionally write-through to the
    // storage block but does not enter the foreground fdatasync boundary.
    // TODO(replication): add an explicit durable-replication policy/ACK mode.
    state->in_memory_ = false;
  store.store_state_mutex_.Unlock(*store.worker_);
  celer::FreeStorageBuffer(log.active_buffer_, kDirectIoAlignment);
  log.active_buffer_ = nullptr;
  co_return absl::OkStatus();
}

Task<absl::StatusOr<std::uint64_t>> StorageEngine::Impl::AppendReplicationLog(
    ReplicationLogAppend event) {
  WorkerStore& store = CurrentStore();
  auto& log = store.replication_log_;
  co_await log.mutex_.Lock();
  UnlockGuard unlock(&log.mutex_, store.worker_);
  if (log.state_ != ReplicationLogState::kActive) {
    co_return InvalidState("replication log is not active");
  }
  if (event.partition_id_ >= kLogicalStorageShards ||
      event.partition_sequence_ == 0 ||
      (event.payload_source_ != nullptr && !event.payload_.empty()) ||
      (event.kind_ != ReplicationEventKind::kMutation &&
       event.kind_ != ReplicationEventKind::kTransaction &&
       event.kind_ != ReplicationEventKind::kControl)) {
    co_return absl::Status(absl::StatusCode::kInvalidArgument,
                           "invalid replication event metadata");
  }
  const std::uint64_t logical_payload_bytes =
      event.payload_source_ == nullptr ? event.payload_.size()
                                       : event.payload_source_->size();
  if (logical_payload_bytes > kMaxRecordPayloadBytes ||
      logical_payload_bytes > std::numeric_limits<std::size_t>::max()) {
    co_return absl::Status(absl::StatusCode::kOutOfRange,
                           "replication event exceeds the 1 GiB limit");
  }
  if (log.next_lsn_ == std::numeric_limits<std::uint64_t>::max()) {
    log.state_ = ReplicationLogState::kInvalid;
    co_return absl::Status(absl::StatusCode::kResourceExhausted,
                           "replication LSN space is exhausted");
  }

  const std::size_t frame_payload_capacity =
      kStorageBlockBytes - kBlockHeaderBytes - sizeof(ReplicationFrameHeader);
  const std::size_t payload_size =
      static_cast<std::size_t>(logical_payload_bytes);
  const std::size_t minimum_blocks =
      payload_size == 0 ? 1
                        : (payload_size + frame_payload_capacity - 1) /
                              frame_payload_capacity;
  if (minimum_blocks > log.max_blocks_) {
    log.state_ = ReplicationLogState::kInvalid;
    co_return absl::Status(
        absl::StatusCode::kResourceExhausted,
        "replication event is larger than the configured backlog");
  }

  const std::uint64_t lsn = log.next_lsn_;
  const std::size_t single_frame_bytes =
      payload_size <= frame_payload_capacity
          ? AlignRecord(sizeof(ReplicationFrameHeader) + payload_size)
          : kStorageBlockBytes;
  if (log.active_buffer_ != nullptr &&
      log.blocks_.back().committed_bytes_ + single_frame_bytes >
          kStorageBlockBytes) {
    // Never start an event in a block that cannot hold it whole. Events larger
    // than one block get dedicated fragment blocks, including a sealed partial
    // final block. Consequently trimming one LSN can never leave its leading
    // fragment behind while retaining later events from the same block.
    absl::Status sealed = co_await SealReplicationLogActiveBlock(store);
    if (!sealed.ok()) {
      co_return sealed;
    }
  }
  const bool multi_block_event = payload_size > frame_payload_capacity;
  std::size_t payload_offset = 0;
  std::uint32_t fragment_index = 0;
  bool emitted = false;
  do {
    absl::Status ready = co_await EnsureReplicationLogActiveBlock(store, lsn);
    if (!ready.ok()) {
      log.state_ = ReplicationLogState::kInvalid;
      co_return ready;
    }
    auto& block = log.blocks_.back();
    const std::size_t remaining_block =
        kStorageBlockBytes - block.committed_bytes_;
    const std::size_t minimum_frame_bytes =
        sizeof(ReplicationFrameHeader) +
        (payload_offset < payload_size ? 1 : 0);
    if (remaining_block < minimum_frame_bytes) {
      absl::Status sealed = co_await SealReplicationLogActiveBlock(store);
      if (!sealed.ok()) {
        co_return sealed;
      }
      continue;
    }

    const std::size_t payload_bytes =
        std::min(payload_size - payload_offset,
                 remaining_block - sizeof(ReplicationFrameHeader));
    const std::size_t frame_bytes =
        AlignRecord(sizeof(ReplicationFrameHeader) + payload_bytes);
    const bool first = fragment_index == 0;
    const bool last = payload_offset + payload_bytes == payload_size;
    const std::uint32_t frame_offset = block.committed_bytes_;
    std::span<std::byte> payload(
        log.active_buffer_ + frame_offset + sizeof(ReplicationFrameHeader),
        payload_bytes);
    if (event.payload_source_ != nullptr) {
      absl::Status loaded =
          co_await event.payload_source_->Read(payload_offset, payload);
      if (!loaded.ok()) {
        log.state_ = ReplicationLogState::kInvalid;
        co_return loaded;
      }
    } else if (payload_bytes != 0) {
      std::memcpy(payload.data(), event.payload_.data() + payload_offset,
                  payload_bytes);
    }
    std::fill(log.active_buffer_ + frame_offset +
                  sizeof(ReplicationFrameHeader) + payload_bytes,
              log.active_buffer_ + frame_offset + frame_bytes, std::byte{0});
    ReplicationFrameHeader header{
        .header_bytes_ = sizeof(ReplicationFrameHeader),
        .kind_ = event.kind_,
        .flags_ = static_cast<std::uint8_t>(
            (first ? static_cast<std::uint8_t>(ReplicationFrameFlag::kFirst)
                   : 0) |
            (last ? static_cast<std::uint8_t>(ReplicationFrameFlag::kLast)
                  : 0)),
        .lsn_ = lsn,
        .partition_sequence_ = event.partition_sequence_,
        .payload_bytes_ = static_cast<std::uint32_t>(payload_bytes),
        .total_disk_bytes_ = static_cast<std::uint32_t>(frame_bytes),
        .fragment_index_ = fragment_index,
        .partition_id_ = event.partition_id_,
        .payload_checksum_ = Crc32c(payload),
    };
    if (!EncodeReplicationFrameHeader(
            header, std::span<std::byte, sizeof(ReplicationFrameHeader)>(
                        log.active_buffer_ + frame_offset,
                        sizeof(ReplicationFrameHeader)))) {
      log.state_ = ReplicationLogState::kInvalid;
      co_return absl::Status(absl::StatusCode::kInternal,
                             "failed to encode replication frame");
    }
    if (block.frame_count_ % kSparseFrameStride == 0) {
      block.sparse_offsets_.push_back({
          .lsn_ = lsn,
          .fragment_index_ = fragment_index,
          .byte_offset_ = frame_offset,
      });
    }
    if (block.frame_count_ == 0) {
      block.first_lsn_ = lsn;
    }
    block.last_lsn_ = lsn;
    block.committed_bytes_ += static_cast<std::uint32_t>(frame_bytes);
    ++block.frame_count_;
    payload_offset += payload_bytes;
    ++fragment_index;
    emitted = true;
    if (block.committed_bytes_ == kStorageBlockBytes) {
      absl::Status sealed = co_await SealReplicationLogActiveBlock(store);
      if (!sealed.ok()) {
        co_return sealed;
      }
    }
  } while (!emitted || payload_offset < payload_size);

  if (multi_block_event && log.active_buffer_ != nullptr) {
    absl::Status sealed = co_await SealReplicationLogActiveBlock(store);
    if (!sealed.ok()) {
      co_return sealed;
    }
  }

  ++log.next_lsn_;
  co_return lsn;
}

Task<absl::StatusOr<ReplicationLogBatch>>
StorageEngine::Impl::ReadReplicationLog(ReplicationLogCursor next,
                                        std::size_t max_bytes,
                                        std::size_t max_frames) {
  WorkerStore& store = CurrentStore();
  auto& log = store.replication_log_;
  co_await log.mutex_.Lock();
  UnlockGuard unlock(&log.mutex_, store.worker_);
  if (log.state_ != ReplicationLogState::kActive) {
    co_return InvalidState("replication log is not active");
  }
  if (next.lsn_ == 0 || max_bytes == 0 || max_frames == 0) {
    co_return absl::Status(absl::StatusCode::kInvalidArgument,
                           "invalid replication log read bounds");
  }
  const std::uint64_t tail_lsn = log.next_lsn_ - 1;
  const std::uint64_t floor_lsn =
      log.blocks_.empty() ? log.next_lsn_ : log.blocks_.front().first_lsn_;
  if (next.lsn_ < floor_lsn) {
    co_return absl::Status(absl::StatusCode::kOutOfRange,
                           "replication cursor is below the backlog floor; "
                           "full synchronization is required");
  }
  ReplicationLogBatch batch{.next_ = next, .frames_ = {}};
  if (next.lsn_ > tail_lsn) {
    if (next.lsn_ != tail_lsn + 1 || next.fragment_index_ != 0) {
      co_return absl::Status(absl::StatusCode::kInvalidArgument,
                             "replication cursor is beyond the backlog tail");
    }
    batch.at_tail_ = true;
    co_return batch;
  }

  std::size_t total_bytes = 0;
  auto block_it =
      std::lower_bound(log.blocks_.begin(), log.blocks_.end(), next.lsn_,
                       [](const WorkerStore::ReplicationLogBlock& block,
                          std::uint64_t lsn) { return block.last_lsn_ < lsn; });
  for (; block_it != log.blocks_.end(); ++block_it) {
    const auto& block = *block_it;
    ReadBufferLease lease;
    const std::byte* bytes = nullptr;
    if (block.sealed_) {
      const std::size_t read_bytes = AlignDirect(block.committed_bytes_);
      auto acquired = co_await store.buffers_.AcquireReadBuffer(read_bytes);
      if (!acquired.ok()) {
        co_return acquired.status();
      }
      lease = std::move(*acquired);
      FixedBuffer buffer = lease.io_buffer();
      if (buffer.size_ < read_bytes) {
        co_return absl::Status(absl::StatusCode::kResourceExhausted,
                               "replication read buffer is too small");
      }
      buffer.size_ = read_bytes;
      const auto [file_index, offset] = FileOffset(block.block_id_);
      auto read =
          co_await ReadStorageBuffer(*store.worker_, store.files_[file_index],
                                     buffer, lease.registered(), offset);
      if (!read.ok() || *read != read_bytes) {
        log.state_ = ReplicationLogState::kInvalid;
        co_return read.ok()
            ? absl::Status(absl::StatusCode::kInternal,
                           "short replication backlog block read")
            : read.status();
      }
      bytes = buffer.data_;
    } else {
      if (&block != &log.blocks_.back() || log.active_buffer_ == nullptr) {
        log.state_ = ReplicationLogState::kInvalid;
        co_return absl::Status(absl::StatusCode::kInternal,
                               "replication active block is unavailable");
      }
      bytes = log.active_buffer_;
    }

    std::uint32_t offset = kBlockHeaderBytes;
    const auto sparse = std::upper_bound(
        block.sparse_offsets_.begin(), block.sparse_offsets_.end(), batch.next_,
        [](const ReplicationLogCursor& cursor,
           const WorkerStore::ReplicationSparseOffset& entry) {
          return CursorBefore(
              cursor,
              {.lsn_ = entry.lsn_, .fragment_index_ = entry.fragment_index_});
        });
    if (sparse != block.sparse_offsets_.begin()) {
      offset = std::prev(sparse)->byte_offset_;
    }
    while (offset < block.committed_bytes_) {
      ReplicationFrameHeader header{};
      const std::span<const std::byte> available(
          bytes + offset, block.committed_bytes_ - offset);
      if (!DecodeReplicationFrameHeader(available, &header) ||
          header.total_disk_bytes_ > available.size()) {
        log.state_ = ReplicationLogState::kInvalid;
        co_return absl::Status(absl::StatusCode::kInternal,
                               "invalid replication frame in backlog");
      }
      const ReplicationLogCursor frame_cursor = FrameCursor(header);
      if (CursorBefore(frame_cursor, batch.next_)) {
        offset += header.total_disk_bytes_;
        continue;
      }
      if (CursorBefore(batch.next_, frame_cursor)) {
        co_return absl::Status(
            absl::StatusCode::kInvalidArgument,
            "replication cursor does not identify a retained frame");
      }
      const auto payload = available.subspan(sizeof(ReplicationFrameHeader),
                                             header.payload_bytes_);
      if (Crc32c(payload) != header.payload_checksum_) {
        log.state_ = ReplicationLogState::kInvalid;
        co_return absl::Status(absl::StatusCode::kInternal,
                               "replication frame payload checksum mismatch");
      }
      const std::size_t emitted_bytes =
          sizeof(ReplicationFrameHeader) + header.payload_bytes_;
      if (!batch.frames_.empty() && (batch.frames_.size() >= max_frames ||
                                     total_bytes + emitted_bytes > max_bytes)) {
        batch.at_tail_ = batch.next_.lsn_ > tail_lsn;
        co_return batch;
      }
      ReplicationLogFrame frame{.header_ = header, .payload_ = {}};
      frame.payload_.assign(reinterpret_cast<const char*>(payload.data()),
                            payload.size());
      batch.frames_.push_back(std::move(frame));
      total_bytes += emitted_bytes;
      batch.next_ = CursorAfter(header);
      offset += header.total_disk_bytes_;
      if (batch.frames_.size() >= max_frames || total_bytes >= max_bytes) {
        batch.at_tail_ = batch.next_.lsn_ > tail_lsn;
        co_return batch;
      }
    }
  }
  if (batch.frames_.empty()) {
    co_return absl::Status(absl::StatusCode::kInvalidArgument,
                           "replication cursor was not found in backlog");
  }
  batch.at_tail_ = batch.next_.lsn_ > tail_lsn;
  co_return batch;
}

Task<absl::Status> StorageEngine::Impl::TrimReplicationLog(
    std::uint64_t keep_from_lsn) {
  WorkerStore& store = CurrentStore();
  auto& log = store.replication_log_;
  co_await log.mutex_.Lock();
  UnlockGuard unlock(&log.mutex_, store.worker_);
  if (log.state_ != ReplicationLogState::kActive) {
    co_return InvalidState("replication log is not active");
  }
  if (keep_from_lsn == 0 || keep_from_lsn > log.next_lsn_) {
    co_return absl::Status(absl::StatusCode::kInvalidArgument,
                           "invalid replication trim LSN");
  }
  co_return co_await ReclaimReplicationLogPrefix(store, keep_from_lsn);
}

Task<absl::Status> StorageEngine::Impl::DisableReplicationLog() {
  WorkerStore& store = CurrentStore();
  auto& log = store.replication_log_;
  co_await log.mutex_.Lock();
  UnlockGuard unlock(&log.mutex_, store.worker_);
  if (log.state_ == ReplicationLogState::kDisabled) {
    co_return absl::OkStatus();
  }
  if (log.active_buffer_ != nullptr) {
    celer::FreeStorageBuffer(log.active_buffer_, kDirectIoAlignment);
    log.active_buffer_ = nullptr;
  }
  absl::Status reclaimed =
      co_await ReclaimReplicationLogPrefix(store, 0, /*force_all=*/true);
  if (!reclaimed.ok()) {
    co_return reclaimed;
  }
  log.state_ = ReplicationLogState::kDisabled;
  log.log_epoch_ = 0;
  log.next_lsn_ = 1;
  log.max_blocks_ = 0;
  for (auto& pending : log.publish_queue_) {
    if (pending.fence_ == nullptr) continue;
    pending.fence_->status_ =
        InvalidState("replication log disabled before publisher fence");
    pending.fence_->complete_ = true;
    pending.fence_->ready_.NotifyAll(*store.worker_);
  }
  log.publish_queue_.clear();
  log.publish_queue_bytes_ = 0;
  log.max_publish_queue_bytes_ = 0;
  log.reserved_memory_bytes_ = 0;
  co_return absl::OkStatus();
}

ReplicationLogInfo StorageEngine::Impl::LocalReplicationLogInfo() const {
  const auto& log = CurrentStore().replication_log_;
  return ReplicationLogInfo{
      .state_ = log.state_,
      .log_epoch_ = log.log_epoch_,
      .floor_lsn_ =
          log.blocks_.empty() ? log.next_lsn_ : log.blocks_.front().first_lsn_,
      .tail_lsn_ = log.next_lsn_ - 1,
      .block_count_ = log.blocks_.size(),
      .capacity_bytes_ = log.max_blocks_ * kStorageBlockBytes,
      .publish_queue_bytes_ = log.publish_queue_bytes_,
      .publish_queue_capacity_bytes_ = log.reserved_memory_bytes_,
  };
}

}  // namespace keylane::storage
