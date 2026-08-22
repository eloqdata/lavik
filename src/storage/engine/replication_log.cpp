#include <new>
#include <optional>

#include "impl.h"
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

bool PublisherHasCapacity(std::size_t logical_bytes, std::size_t capacity,
                          std::size_t queued, std::size_t admitted) noexcept {
  const std::size_t occupied =
      queued > std::numeric_limits<std::size_t>::max() - admitted
          ? std::numeric_limits<std::size_t>::max()
          : queued + admitted;
  // An item larger than the configured waterline may proceed only while it is
  // the exclusive heap-backed item.
  return logical_bytes > capacity ? occupied == 0
                                  : occupied <= capacity - logical_bytes;
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
  log.publisher_admitted_bytes_ = 0;
  log.retained_lsn_by_session_.clear();
  log.capacity_backpressured_ = false;
  log.capacity_waits_ = 0;
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::SetReplicationLogCapacity(
    std::size_t capacity_bytes) {
  if (capacity_bytes < kStorageBlockBytes ||
      capacity_bytes % kStorageBlockBytes != 0) {
    co_return absl::InvalidArgumentError(
        "replication backlog capacity must be a positive multiple of 8 MiB");
  }
  WorkerStore& store = CurrentStore();
  auto& log = store.replication_log_;
  const std::size_t requested_blocks = capacity_bytes / kStorageBlockBytes;
  // Cursor progress is worker-local and deliberately does not acquire the log
  // append mutex. Apply a resize and wake a publisher before attempting any
  // eager trimming: growth is an administrative escape hatch for an ACK-
  // backpressured publisher, and must not queue behind that publisher's wait.
  if (log.state_ == ReplicationLogState::kActive &&
      !log.retained_lsn_by_session_.empty()) {
    if (requested_blocks > log.max_blocks_) {
      log.capacity_backpressured_ = false;
    }
    log.max_blocks_ = requested_blocks;
    log.retention_advanced_.NotifyAll(*store.worker_);
    // A shrink beneath live retained history is a target quota. Existing
    // blocks remain until ACK progress permits complete-event reclamation.
    co_return absl::OkStatus();
  }
  co_await log.mutex_.Lock();
  UnlockGuard unlock(&log.mutex_, store.worker_);
  if (log.state_ == ReplicationLogState::kDisabled) {
    co_return absl::OkStatus();
  }
  if (log.state_ == ReplicationLogState::kInvalid) {
    log.max_blocks_ = requested_blocks;
    co_return absl::OkStatus();
  }

  log.max_blocks_ = requested_blocks;
  std::optional<std::uint64_t> retained_lsn;
  for (const auto& [session_id, lsn] : log.retained_lsn_by_session_) {
    (void)session_id;
    retained_lsn = !retained_lsn.has_value() || lsn < *retained_lsn
                       ? std::optional<std::uint64_t>(lsn)
                       : retained_lsn;
  }
  while (log.blocks_.size() > log.max_blocks_) {
    if (log.blocks_.empty() || !log.blocks_.front().sealed_) break;
    if (retained_lsn.has_value() &&
        log.blocks_.front().last_lsn_ >= *retained_lsn) {
      // Shrinking the reconnect window must not punch a hole beneath a live
      // ONLINE replica. Keep the excess blocks for now; subsequent appends
      // backpressure until ACK progress makes the new quota attainable.
      break;
    }
    // Never split a fragmented logical event while reducing the retained
    // reconnect window.
    std::uint64_t evicted_through = log.blocks_.front().last_lsn_;
    do {
      evicted_through =
          std::max(evicted_through, log.blocks_.front().last_lsn_);
      log.blocks_.pop_front();
    } while (!log.blocks_.empty() && log.blocks_.front().sealed_ &&
             log.blocks_.front().first_lsn_ <= evicted_through);
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::SetReplicationPublishQueueCapacity(
    std::size_t capacity_bytes) {
  if (capacity_bytes == 0) {
    co_return absl::InvalidArgumentError(
        "replication publish queue capacity must be nonzero");
  }
  replication_publish_queue_bytes_.store(capacity_bytes,
                                         std::memory_order_release);
  WorkerStore& store = CurrentStore();
  store.replication_log_.publisher_capacity_ready_.NotifyAll(*store.worker_);
  store.fullsync_publisher_capacity_ready_.NotifyAll(*store.worker_);
  co_return absl::OkStatus();
}

bool StorageEngine::Impl::ReplicationLogActive() const noexcept {
  return CurrentStore().replication_log_.state_ == ReplicationLogState::kActive;
}

Task<absl::StatusOr<ReplicationPublisherAdmission>>
StorageEngine::Impl::AcquireReplicationPublisherAdmission(
    std::size_t logical_bytes,
    std::optional<ReplicationPublisherTarget> target) {
  WorkerStore& store = CurrentStore();
  auto& log = store.replication_log_;
  if (target.has_value() &&
      (target->partition_id_ >= kLogicalStorageShards ||
       target->db_id_ >= kLogicalDatabaseCount ||
       target->partition_id_ % worker_count_ != store.worker_->id())) {
    co_return absl::InvalidArgumentError(
        "replication publisher target does not belong to this worker");
  }
  auto session_needs_credit = [&](std::uint64_t session_id) {
    if (!target.has_value()) return true;
    const auto& partition = PartitionFor(store, target->partition_id_);
    const auto capture = partition.fullsync_subscribers_.find(session_id);
    return capture != partition.fullsync_subscribers_.end() &&
           capture->second.db_phases_[target->db_id_] !=
               WorkerStore::FullSyncCapture::DbPhase::kUnstarted;
  };
  if (logical_bytes == 0) logical_bytes = 1;
  if (store.replication_publisher_next_ticket_ ==
      std::numeric_limits<std::uint64_t>::max()) {
    co_return absl::ResourceExhaustedError(
        "replication publisher admission ticket space exhausted");
  }
  const std::uint64_t ticket = store.replication_publisher_next_ticket_++;
  bool recorded_fullsync_wait = false;
  for (;;) {
    if (ticket != store.replication_publisher_serving_ticket_) {
      co_await store.replication_publisher_admission_ready_.Wait();
      continue;
    }
    const std::size_t capacity =
        replication_publish_queue_bytes_.load(std::memory_order_acquire);
    const bool log_available =
        log.state_ != ReplicationLogState::kActive ||
        PublisherHasCapacity(logical_bytes, capacity, log.publish_queue_bytes_,
                             log.publisher_admitted_bytes_);
    bool available = log_available;
    if (available) {
      for (const auto& [session_id, session] : store.fullsync_sessions_) {
        if (session.db_epoch_invalidated_ ||
            !session_needs_credit(session_id)) {
          continue;
        }
        if (!PublisherHasCapacity(logical_bytes, capacity,
                                  session.publish_queue_bytes_,
                                  session.publisher_admitted_bytes_)) {
          available = false;
          break;
        }
      }
    }
    if (available) {
      ReplicationPublisherAdmission admission;
      if (log.state_ == ReplicationLogState::kActive) {
        admission.log_epoch_ = log.log_epoch_;
        log.publisher_admitted_bytes_ += logical_bytes;
      }
      admission.fullsync_session_ids_.reserve(store.fullsync_sessions_.size());
      admission.fullsync_unstarted_guards_.reserve(
          target.has_value() ? store.fullsync_sessions_.size() : 0);
      for (auto& [session_id, session] : store.fullsync_sessions_) {
        if (session.db_epoch_invalidated_) continue;
        if (session_needs_credit(session_id)) {
          session.publisher_admitted_bytes_ += logical_bytes;
          admission.fullsync_session_ids_.push_back(session_id);
        } else if (target.has_value()) {
          const std::uint32_t target_id =
              (static_cast<std::uint32_t>(target->partition_id_) << 8) |
              target->db_id_;
          ++session.unstarted_admissions_[target_id];
          admission.fullsync_unstarted_guards_.push_back(
              ReplicationPublisherAdmission::UnstartedGuard{
                  .session_id_ = session_id,
                  .partition_id_ = target->partition_id_,
                  .db_id_ = target->db_id_,
              });
        }
      }
      ++store.replication_publisher_serving_ticket_;
      store.replication_publisher_admission_ready_.NotifyAll(*store.worker_);
      co_return admission;
    }
    // Either queue may free space. The worker-lifetime notification is also
    // signalled when a full-sync session is cancelled, so a disconnected
    // replica can never leave source writes asleep on destroyed state.
    if (!log_available) {
      co_await log.publisher_capacity_ready_.Wait();
    } else {
      if (!recorded_fullsync_wait) {
        ++store.fullsync_publisher_capacity_waits_;
        recorded_fullsync_wait = true;
      }
      co_await store.fullsync_publisher_capacity_ready_.Wait();
    }
  }
}

std::optional<ReplicationPublisherAdmission>
StorageEngine::Impl::TryAcquireFullSyncReplacementAdmission(
    std::size_t logical_bytes, ReplicationPublisherTarget target) {
  WorkerStore& store = CurrentStore();
  assert(target.partition_id_ < kLogicalStorageShards);
  assert(target.db_id_ < kLogicalDatabaseCount);
  assert(target.partition_id_ % worker_count_ == store.worker_->id());
  if (logical_bytes == 0) logical_bytes = 1;

  ReplicationPublisherAdmission admission;
  if (store.fullsync_sessions_.empty()) return admission;
  // Active expiration is background maintenance. Never bypass a foreground
  // publisher that already owns or is waiting for the admission ticket.
  if (store.replication_publisher_next_ticket_ !=
      store.replication_publisher_serving_ticket_) {
    return std::nullopt;
  }

  const auto& partition = PartitionFor(store, target.partition_id_);
  auto session_needs_credit = [&](std::uint64_t session_id) {
    const auto capture = partition.fullsync_subscribers_.find(session_id);
    return capture != partition.fullsync_subscribers_.end() &&
           capture->second.db_phases_[target.db_id_] !=
               WorkerStore::FullSyncCapture::DbPhase::kUnstarted;
  };
  const std::size_t capacity =
      replication_publish_queue_bytes_.load(std::memory_order_acquire);
  for (const auto& [session_id, session] : store.fullsync_sessions_) {
    if (session.db_epoch_invalidated_ || !session_needs_credit(session_id)) {
      continue;
    }
    if (!PublisherHasCapacity(logical_bytes, capacity,
                              session.publish_queue_bytes_,
                              session.publisher_admitted_bytes_)) {
      return std::nullopt;
    }
  }

  admission.fullsync_session_ids_.reserve(store.fullsync_sessions_.size());
  admission.fullsync_unstarted_guards_.reserve(store.fullsync_sessions_.size());
  for (auto& [session_id, session] : store.fullsync_sessions_) {
    if (session.db_epoch_invalidated_) continue;
    if (session_needs_credit(session_id)) {
      session.publisher_admitted_bytes_ += logical_bytes;
      admission.fullsync_session_ids_.push_back(session_id);
      continue;
    }
    const std::uint32_t target_id =
        (static_cast<std::uint32_t>(target.partition_id_) << 8) | target.db_id_;
    ++session.unstarted_admissions_[target_id];
    admission.fullsync_unstarted_guards_.push_back(
        ReplicationPublisherAdmission::UnstartedGuard{
            .session_id_ = session_id,
            .partition_id_ = target.partition_id_,
            .db_id_ = target.db_id_,
        });
  }
  return admission;
}

void StorageEngine::Impl::ReleaseReplicationPublisherAdmission(
    const ReplicationPublisherAdmission& admission, std::size_t logical_bytes) {
  WorkerStore& store = CurrentStore();
  auto& log = store.replication_log_;
  if (logical_bytes == 0) logical_bytes = 1;
  if (admission.log_epoch_ != 0 && log.log_epoch_ == admission.log_epoch_) {
    log.publisher_admitted_bytes_ -=
        std::min(log.publisher_admitted_bytes_, logical_bytes);
    log.publisher_capacity_ready_.NotifyAll(*store.worker_);
  }
  for (std::uint64_t session_id : admission.fullsync_session_ids_) {
    auto session = store.fullsync_sessions_.find(session_id);
    if (session == store.fullsync_sessions_.end()) continue;
    session->second.publisher_admitted_bytes_ -=
        std::min(session->second.publisher_admitted_bytes_, logical_bytes);
  }
  for (const auto& guard : admission.fullsync_unstarted_guards_) {
    auto session = store.fullsync_sessions_.find(guard.session_id_);
    if (session == store.fullsync_sessions_.end()) continue;
    const std::uint32_t target_id =
        (static_cast<std::uint32_t>(guard.partition_id_) << 8) | guard.db_id_;
    auto count = session->second.unstarted_admissions_.find(target_id);
    if (count == session->second.unstarted_admissions_.end()) continue;
    if (--count->second == 0) {
      session->second.unstarted_admissions_.erase(count);
    }
  }
  store.fullsync_publisher_capacity_ready_.NotifyAll(*store.worker_);
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
  if (command.args_.empty()) return false;
  if (logical_bytes >
      std::numeric_limits<std::size_t>::max() - log.publish_queue_bytes_) {
    log.state_ = ReplicationLogState::kInvalid;
    spdlog::warn("replication publisher queue accounting overflow");
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
  if (logical_bytes >
      std::numeric_limits<std::size_t>::max() - log.publish_queue_bytes_) {
    log.state_ = ReplicationLogState::kInvalid;
    spdlog::warn("replication transaction queue accounting overflow");
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

Task<absl::StatusOr<std::uint64_t>> StorageEngine::Impl::FenceReplicationLog() {
  WorkerStore& store = CurrentStore();
  auto& log = store.replication_log_;
  if (log.state_ != ReplicationLogState::kActive) {
    co_return InvalidState("replication log is not active");
  }

  auto fence =
      std::make_shared<WorkerStore::ReplicationLogRuntime::PublishFence>();
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
      while (
          pending.transaction_->resolution_.load(std::memory_order_acquire) ==
          ReplicationTransactionResolution::kPending) {
        co_await celer::Yield(*store->worker_);
      }
      if (pending.transaction_->resolution_.load(std::memory_order_acquire) ==
          ReplicationTransactionResolution::kDiscard) {
        if (log.publish_queue_bytes_ >= pending.logical_bytes_) {
          log.publish_queue_bytes_ -= pending.logical_bytes_;
        }
        log.publisher_capacity_ready_.NotifyAll(*store->worker_);
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
        if (growth > std::numeric_limits<std::size_t>::max() -
                         log.publish_queue_bytes_) {
          log.state_ = ReplicationLogState::kInvalid;
          spdlog::warn(
              "canonical replication transaction queue accounting overflow");
          break;
        }
        log.publish_queue_bytes_ += growth;
      } else {
        log.publish_queue_bytes_ -=
            std::min(log.publish_queue_bytes_,
                     pending.logical_bytes_ - *final_logical_bytes);
        log.publisher_capacity_ready_.NotifyAll(*store->worker_);
      }
      pending.logical_bytes_ = *final_logical_bytes;
      pending.append_ = ReplicationCommandAppend{
          .kind_ = ReplicationEventKind::kTransaction,
          .db_id_ = pending.transaction_->db_id_,
          .partition_id_ = static_cast<std::uint16_t>(celer::ThisWorker().id_),
          .partition_sequence_ = pending.transaction_->id_,
          .args_ = pending.transaction_->envelope_args_,
      };
    }
    if (log.state_ != ReplicationLogState::kActive ||
        pending.log_epoch_ != log.log_epoch_) {
      if (log.publish_queue_bytes_ >= pending.logical_bytes_) {
        log.publish_queue_bytes_ -= pending.logical_bytes_;
      }
      log.publisher_capacity_ready_.NotifyAll(*store->worker_);
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
    log.publisher_capacity_ready_.NotifyAll(*store->worker_);
  }
  if (log.state_ != ReplicationLogState::kActive) {
    for (std::size_t index = 0; index < log.publish_queue_.size(); ++index) {
      auto& pending = log.publish_queue_[index];
      if (pending.fence_ == nullptr) continue;
      pending.fence_->status_ =
          InvalidState("replication publisher failed before fence");
      pending.fence_->complete_ = true;
      pending.fence_->ready_.NotifyAll(*store->worker_);
    }
    log.publish_queue_.clear();
    log.publish_queue_bytes_ = 0;
    log.publisher_capacity_ready_.NotifyAll(*store->worker_);
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
  const std::uint64_t barrier_id =
      next_replication_control_id_.fetch_add(1, std::memory_order_relaxed);
  if (barrier_id == 0 ||
      barrier_id == std::numeric_limits<std::uint64_t>::max()) {
    next_replication_control_id_.store(
        std::numeric_limits<std::uint64_t>::max(), std::memory_order_relaxed);
    co_return absl::OutOfRangeError(
        "replication control barrier identity exhausted");
  }
  for (unsigned target = 0; target < worker_count_; ++target) {
    auto publish = [this, db_id, db_epoch, barrier_id]() -> Task<absl::Status> {
      if (ReplicationLogActive()) {
        (void)TryEnqueueReplicationCommand(ReplicationCommandAppend{
            .kind_ = ReplicationEventKind::kControl,
            .db_id_ = db_id,
            // Control events do not belong to a partition. Zero is the
            // canonical transport placeholder; receivers key the barrier by
            // its explicit history-local identity carried in the payload.
            .partition_id_ = 0,
            .partition_sequence_ = barrier_id,
            .args_ = {"FLUSHDB", std::to_string(barrier_id),
                      std::to_string(db_epoch)},
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

Task<absl::Status> StorageEngine::Impl::PublishFlushAllReplication(
    const std::array<std::uint64_t, kLogicalDatabaseCount>& db_epochs) {
  if (std::any_of(db_epochs.begin(), db_epochs.end(),
                  [](std::uint64_t epoch) { return epoch == 0; })) {
    co_return absl::InvalidArgumentError(
        "invalid FLUSHALL replication barrier");
  }
  const std::uint64_t barrier_id =
      next_replication_control_id_.fetch_add(1, std::memory_order_relaxed);
  if (barrier_id == 0 ||
      barrier_id == std::numeric_limits<std::uint64_t>::max()) {
    next_replication_control_id_.store(
        std::numeric_limits<std::uint64_t>::max(), std::memory_order_relaxed);
    co_return absl::OutOfRangeError(
        "replication control barrier identity exhausted");
  }
  std::vector<std::string> args;
  args.reserve(2 + kLogicalDatabaseCount);
  args.emplace_back("FLUSHALL");
  args.push_back(std::to_string(barrier_id));
  for (const std::uint64_t epoch : db_epochs) {
    args.push_back(std::to_string(epoch));
  }
  for (unsigned target = 0; target < worker_count_; ++target) {
    auto publish = [this, barrier_id, args]() mutable -> Task<absl::Status> {
      if (ReplicationLogActive()) {
        (void)TryEnqueueReplicationCommand(ReplicationCommandAppend{
            .kind_ = ReplicationEventKind::kControl,
            .db_id_ = 0,
            .partition_id_ = 0,
            .partition_sequence_ = barrier_id,
            .args_ = std::move(args),
        });
      }
      co_return absl::OkStatus();
    };
    absl::Status published;
    if (target == celer::ThisWorker().id_) {
      published = co_await publish();
    } else {
      published = co_await celer::SubmitTaskTo(target, std::move(publish));
    }
    if (!published.ok()) co_return published;
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::ReclaimReplicationLogPrefix(
    WorkerStore& store, std::uint64_t keep_from_lsn, bool force_all) {
  auto& log = store.replication_log_;
  while (!log.blocks_.empty()) {
    const auto& front = log.blocks_.front();
    if (!force_all && (!front.sealed_ || front.last_lsn_ >= keep_from_lsn)) {
      break;
    }
    log.blocks_.pop_front();
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::EnsureReplicationLogActiveBlock(
    WorkerStore& store, std::uint64_t protected_lsn) {
  auto& log = store.replication_log_;
  if (!log.blocks_.empty() && !log.blocks_.back().sealed_) {
    co_return absl::OkStatus();
  }
  if (log.max_blocks_ == 0) {
    co_return InvalidState("replication log has no capacity");
  }

  auto retained_lsn = [&]() -> std::optional<std::uint64_t> {
    std::optional<std::uint64_t> retained_lsn;
    for (const auto& [session_id, lsn] : log.retained_lsn_by_session_) {
      (void)session_id;
      if (!retained_lsn.has_value() || lsn < *retained_lsn) {
        retained_lsn = lsn;
      }
    }
    return retained_lsn;
  };
  auto evict_event = [&]() {
    std::uint64_t evicted_through = log.blocks_.front().last_lsn_;
    do {
      evicted_through =
          std::max(evicted_through, log.blocks_.front().last_lsn_);
      log.blocks_.pop_front();
    } while (!log.blocks_.empty() && log.blocks_.front().sealed_ &&
             log.blocks_.front().first_lsn_ <= evicted_through);
  };

  for (;;) {
    const std::size_t low_watermark =
        log.max_blocks_ - std::max<std::size_t>(1, log.max_blocks_ / 4);
    const auto retained = retained_lsn();
    const std::uint64_t keep_from = retained.has_value()
                                        ? std::min(protected_lsn, *retained)
                                        : protected_lsn;
    const bool evictable = !log.blocks_.empty() &&
                           log.blocks_.front().sealed_ &&
                           log.blocks_.front().last_lsn_ < keep_from;

    // Low-water hysteresis exists to avoid repeatedly waking foreground
    // writers while an ACK-pinned replica is still behind. Once the final pin
    // disappears (normally on disconnect), preserve the ordinary circular
    // reconnect window and evict only what the next allocation needs. Eagerly
    // draining to the low watermark here can discard a just-disconnected
    // replica's cursor even though it still fits inside the configured quota.
    if (log.capacity_backpressured_ && !retained.has_value()) {
      log.capacity_backpressured_ = false;
    }
    if (log.capacity_backpressured_) {
      if (log.blocks_.size() <= low_watermark) {
        log.capacity_backpressured_ = false;
      } else if (evictable) {
        evict_event();
        continue;
      } else if (retained.has_value()) {
        // High/low hysteresis is intentional: waking for one ACK and admitting
        // one write can leave a slower replica permanently glued to the high
        // watermark. Drain to the low watermark before reopening writers.
        co_await log.retention_advanced_.Wait();
        if (log.state_ != ReplicationLogState::kActive) {
          co_return InvalidState(
              "replication log stopped while waiting for replica ACK");
        }
        continue;
      } else {
        log.capacity_backpressured_ = false;
      }
    }

    if (log.blocks_.size() < log.max_blocks_) break;
    if (evictable) {
      evict_event();
      continue;
    }
    if (!log.blocks_.empty() &&
        log.blocks_.front().last_lsn_ >= protected_lsn &&
        (!retained.has_value() || *retained >= protected_lsn)) {
      // One logical event may exceed the configured window. It owns an
      // exclusive heap-backed run of chunks and temporarily exceeds the
      // waterline; following writes remain behind it until it is reclaimable.
      break;
    }
    if (retained.has_value()) {
      log.capacity_backpressured_ = true;
      ++log.capacity_waits_;
      co_await log.retention_advanced_.Wait();
      if (log.state_ != ReplicationLogState::kActive) {
        co_return InvalidState(
            "replication log stopped while waiting for replica ACK");
      }
      continue;
    }
    evict_event();
  }

  if (WouldExceedMemoryLimit(kStorageBlockBytes)) {
    co_return absl::ResourceExhaustedError(
        "maxmemory cannot allocate an in-memory replication backlog chunk");
  }
  std::unique_ptr<std::byte[]> bytes(new (std::nothrow)
                                         std::byte[kStorageBlockBytes]);
  if (bytes == nullptr) {
    co_return absl::ResourceExhaustedError(
        "cannot allocate an in-memory replication backlog chunk");
  }
  log.blocks_.push_back(WorkerStore::ReplicationLogBlock{
      .bytes_ = std::move(bytes),
      .sparse_offsets_ = {},
  });
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::SealReplicationLogActiveBlock(
    WorkerStore& store) {
  auto& log = store.replication_log_;
  if (log.blocks_.empty() || log.blocks_.back().sealed_) {
    co_return InvalidState("replication log has no active block");
  }
  auto& block = log.blocks_.back();
  if (block.frame_count_ == 0 || block.first_lsn_ == 0) {
    co_return InvalidState("cannot seal an empty replication block");
  }

  block.sealed_ = true;
  co_return absl::OkStatus();
}

Task<absl::StatusOr<std::uint64_t>> StorageEngine::Impl::AppendReplicationLog(
    ReplicationLogAppend event) {
  WorkerStore& store = CurrentStore();
  auto& log = store.replication_log_;
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
      kStorageBlockBytes - sizeof(ReplicationFrameHeader);
  const std::size_t payload_size =
      static_cast<std::size_t>(logical_payload_bytes);
  const std::uint64_t lsn = log.next_lsn_;
  const std::size_t single_frame_bytes =
      payload_size <= frame_payload_capacity
          ? AlignRecord(sizeof(ReplicationFrameHeader) + payload_size)
          : kStorageBlockBytes;
  if (!log.blocks_.empty() && !log.blocks_.back().sealed_ &&
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
        block.bytes_.get() + frame_offset + sizeof(ReplicationFrameHeader),
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
    std::fill(block.bytes_.get() + frame_offset +
                  sizeof(ReplicationFrameHeader) + payload_bytes,
              block.bytes_.get() + frame_offset + frame_bytes, std::byte{0});
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
                        block.bytes_.get() + frame_offset,
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

  if (multi_block_event && !log.blocks_.empty() &&
      !log.blocks_.back().sealed_) {
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
    if (block.bytes_ == nullptr) {
      log.state_ = ReplicationLogState::kInvalid;
      co_return absl::InternalError(
          "replication memory backlog block is unavailable");
    }
    const std::byte* bytes = block.bytes_.get();

    std::uint32_t offset = 0;
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

absl::Status StorageEngine::Impl::RetainReplicationLog(
    std::uint64_t session_id, std::uint64_t keep_from_lsn) {
  WorkerStore& store = CurrentStore();
  auto& log = store.replication_log_;
  if (session_id == 0 || keep_from_lsn == 0) {
    return absl::InvalidArgumentError(
        "replication retention identity and LSN must be nonzero");
  }
  if (log.state_ != ReplicationLogState::kActive) {
    return InvalidState("replication log is not active");
  }
  const std::uint64_t floor_lsn =
      log.blocks_.empty() ? log.next_lsn_ : log.blocks_.front().first_lsn_;
  if (keep_from_lsn < floor_lsn || keep_from_lsn > log.next_lsn_) {
    return absl::OutOfRangeError(
        "replication retention cursor is outside the backlog");
  }
  auto [found, inserted] =
      log.retained_lsn_by_session_.try_emplace(session_id, keep_from_lsn);
  if (!inserted) {
    if (keep_from_lsn < found->second) {
      return absl::InvalidArgumentError(
          "replication retention cursor cannot move backwards");
    }
    found->second = keep_from_lsn;
  }
  log.retention_advanced_.NotifyAll(*store.worker_);
  return absl::OkStatus();
}

void StorageEngine::Impl::ReleaseReplicationLogRetention(
    std::uint64_t session_id) {
  WorkerStore& store = CurrentStore();
  auto& log = store.replication_log_;
  if (log.retained_lsn_by_session_.erase(session_id) != 0) {
    log.retention_advanced_.NotifyAll(*store.worker_);
  }
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
  for (const auto& [session_id, retained_lsn] : log.retained_lsn_by_session_) {
    (void)session_id;
    keep_from_lsn = std::min(keep_from_lsn, retained_lsn);
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
  absl::Status reclaimed =
      co_await ReclaimReplicationLogPrefix(store, 0, /*force_all=*/true);
  if (!reclaimed.ok()) {
    co_return reclaimed;
  }
  log.state_ = ReplicationLogState::kDisabled;
  log.log_epoch_ = 0;
  log.next_lsn_ = 1;
  log.max_blocks_ = 0;
  log.capacity_backpressured_ = false;
  for (std::size_t index = 0; index < log.publish_queue_.size(); ++index) {
    auto& pending = log.publish_queue_[index];
    if (pending.fence_ == nullptr) continue;
    pending.fence_->status_ =
        InvalidState("replication log disabled before publisher fence");
    pending.fence_->complete_ = true;
    pending.fence_->ready_.NotifyAll(*store.worker_);
  }
  log.publish_queue_.clear();
  log.publish_queue_bytes_ = 0;
  log.publisher_admitted_bytes_ = 0;
  log.retained_lsn_by_session_.clear();
  log.retention_advanced_.NotifyAll(*store.worker_);
  log.publisher_capacity_ready_.NotifyAll(*store.worker_);
  co_return absl::OkStatus();
}

ReplicationLogInfo StorageEngine::Impl::LocalReplicationLogInfo() const {
  const WorkerStore& store = CurrentStore();
  const auto& log = store.replication_log_;
  std::size_t fullsync_queue_bytes = 0;
  std::size_t fullsync_admitted_bytes = 0;
  auto saturating_add = [](std::size_t left, std::size_t right) {
    return right > std::numeric_limits<std::size_t>::max() - left
               ? std::numeric_limits<std::size_t>::max()
               : left + right;
  };
  for (const auto& [session_id, session] : store.fullsync_sessions_) {
    (void)session_id;
    fullsync_queue_bytes =
        saturating_add(fullsync_queue_bytes, session.publish_queue_bytes_);
    fullsync_admitted_bytes = saturating_add(fullsync_admitted_bytes,
                                             session.publisher_admitted_bytes_);
  }
  const std::size_t per_session_capacity =
      replication_publish_queue_bytes_.load(std::memory_order_acquire);
  const std::size_t fullsync_capacity =
      store.fullsync_sessions_.empty()
          ? 0
          : (per_session_capacity > std::numeric_limits<std::size_t>::max() /
                                        store.fullsync_sessions_.size()
                 ? std::numeric_limits<std::size_t>::max()
                 : per_session_capacity * store.fullsync_sessions_.size());
  return ReplicationLogInfo{
      .state_ = log.state_,
      .log_epoch_ = log.log_epoch_,
      .floor_lsn_ =
          log.blocks_.empty() ? log.next_lsn_ : log.blocks_.front().first_lsn_,
      .tail_lsn_ = log.next_lsn_ - 1,
      .block_count_ = log.blocks_.size(),
      .capacity_bytes_ = log.max_blocks_ * kStorageBlockBytes,
      .publish_queue_bytes_ = log.publish_queue_bytes_,
      .publish_queue_capacity_bytes_ =
          replication_publish_queue_bytes_.load(std::memory_order_acquire),
      .fullsync_publish_queue_bytes_ = fullsync_queue_bytes,
      .fullsync_publisher_admitted_bytes_ = fullsync_admitted_bytes,
      .fullsync_publish_queue_capacity_bytes_ = fullsync_capacity,
      .fullsync_session_count_ = store.fullsync_sessions_.size(),
      .retained_cursor_count_ = log.retained_lsn_by_session_.size(),
      .backpressure_waits_ = log.capacity_waits_,
      .fullsync_backpressure_waits_ = store.fullsync_publisher_capacity_waits_,
      .capacity_backpressured_ = log.capacity_backpressured_,
  };
}

}  // namespace keylane::storage
