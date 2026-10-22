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

#include <atomic>
#include <cstdlib>
#include <new>
#include <optional>

#include "impl.h"
#include "lavik/fault_pause.h"
#include "lavik/memory.h"
#include "lavik/replication_command.h"

namespace lavik::storage {
namespace {

bool AddAllocationCharge(std::size_t requested, std::size_t* total) noexcept {
  const std::size_t usable = AllocatorUsableSizeForRequest(requested);
  if (usable == std::numeric_limits<std::size_t>::max() ||
      usable > std::numeric_limits<std::size_t>::max() - *total) {
    return false;
  }
  *total += usable;
  return true;
}

template <typename Queue>
absl::Status PrepareAdmittedQueueSlot(Queue* queue,
                                      std::size_t admitted_items) {
  if (admitted_items == std::numeric_limits<std::size_t>::max() ||
      queue->size() >
          std::numeric_limits<std::size_t>::max() - admitted_items - 1) {
    return absl::ResourceExhaustedError(
        "replication publisher item count overflow");
  }
  const std::size_t required = queue->size() + admitted_items + 1;
  if (queue->growth_bytes_for_capacity(required) ==
      std::numeric_limits<std::size_t>::max()) {
    return absl::ResourceExhaustedError(
        "replication publisher queue is too large");
  }
  queue->PrepareCapacity(required);
  return absl::OkStatus();
}

template <typename String>
bool AddCommandArgumentAllocationBytes(std::span<const String> args,
                                       std::size_t* total) noexcept {
  const std::size_t sso_capacity = std::string{}.capacity();
  for (const auto& arg : args) {
    // Captured arguments can move here with spare capacity; accounting only
    // their current length would forget the retained allocation after adopt.
    const std::size_t capacity = arg.capacity();
    if (capacity <= sso_capacity) continue;
    if (capacity == std::numeric_limits<std::size_t>::max() ||
        !AddAllocationCharge(capacity + 1, total)) {
      return false;
    }
  }
  return true;
}

template <typename String>
std::optional<std::size_t> CommandStagingBytes(
    std::span<const String> args) noexcept {
  std::size_t bytes = 0;
  for (const auto& arg : args) {
    if (arg.size() > std::numeric_limits<std::size_t>::max() - bytes)
      return std::nullopt;
    bytes += arg.size();
  }
  return ReplicationCommandStagingBytes(args.size(), bytes);
}

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

std::optional<std::size_t> TransactionStagingBytes(
    const ReplicationTransaction& transaction, unsigned worker) noexcept {
  const bool carries_payload = worker == transaction.payload_flow_;
  const std::size_t argument_count =
      1 + (carries_payload ? transaction.command_args_.size() : 0);
  std::size_t total = kReplicationPublisherItemMetadataBytes;
  if (argument_count >
      (std::numeric_limits<std::size_t>::max() - total) / sizeof(std::string)) {
    return std::nullopt;
  }
  total += argument_count * sizeof(std::string);
  if (transaction.envelope_metadata_.size() >
      std::numeric_limits<std::size_t>::max() - total) {
    return std::nullopt;
  }
  total += transaction.envelope_metadata_.size();
  if (carries_payload) {
    for (const std::string& arg : transaction.command_args_) {
      if (arg.size() > std::numeric_limits<std::size_t>::max() - total) {
        return std::nullopt;
      }
      total += arg.size();
    }
  }
  return total;
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

void AccountReplicationCommand(ReplicationCommandAppend* command) {
  if (command == nullptr || command->retained_owner_ != nullptr) return;
  std::size_t bytes = 0;
  if (!AddAllocationCharge(sizeof(RetainedMemoryCharge) + 64, &bytes) ||
      (command->args_.capacity() != 0 &&
       !AddAllocationCharge(command->args_.capacity() * sizeof(std::string),
                            &bytes))) {
    throw std::bad_alloc();
  }
  for (const auto& argument : command->args_) {
    if (argument.capacity() > std::string{}.capacity() &&
        !AddAllocationCharge(argument.capacity() + 1, &bytes)) {
      throw std::bad_alloc();
    }
  }
  auto charge = std::make_shared<RetainedMemoryCharge>();
  charge->Account(CurrentMemoryAccountingShard(), bytes);
  command->retained_owner_ = std::move(charge);
}

std::optional<std::size_t> ReplicationCommandStagingBytes(
    std::size_t argument_count, std::size_t argument_bytes) noexcept {
  // The published journal still owns strings. Estimate that representation,
  // independently of whether the source request owns strings or packed bytes.
  // The fixed allowance covers the ring item, vector control object, shared
  // owner control block, and event metadata without allocator size classes:
  // this conservative staging waterline is separate from used_memory.
  std::size_t total = kReplicationPublisherItemMetadataBytes;
  if (argument_count >
      (std::numeric_limits<std::size_t>::max() - total) / sizeof(std::string))
    return std::nullopt;
  total += argument_count * sizeof(std::string);
  if (argument_bytes > std::numeric_limits<std::size_t>::max() - total)
    return std::nullopt;
  return total + argument_bytes;
}

std::optional<std::size_t> ReplicationCommandStagingBytes(
    std::span<const std::string> args) noexcept {
  return CommandStagingBytes(args);
}

std::optional<std::size_t> ReplicationCommandStagingBytes(
    std::span<const std::string_view> args) noexcept {
  return CommandStagingBytes(args);
}

std::optional<std::size_t> ReplicationTransactionAllocationBytes(
    std::size_t participant_capacity, std::span<const std::string> prefix,
    const std::vector<std::string>& command_args) noexcept {
  std::size_t total = 0;
  if (command_args.capacity() != 0 &&
      (command_args.capacity() >
           std::numeric_limits<std::size_t>::max() / sizeof(std::string) ||
       !AddAllocationCharge(command_args.capacity() * sizeof(std::string),
                            &total))) {
    return std::nullopt;
  }
  if (participant_capacity != 0 &&
      (participant_capacity >
           std::numeric_limits<std::size_t>::max() / sizeof(unsigned) ||
       !AddAllocationCharge(participant_capacity * sizeof(unsigned), &total))) {
    return std::nullopt;
  }
  if (!AddCommandArgumentAllocationBytes(prefix, &total) ||
      !AddCommandArgumentAllocationBytes(
          std::span<const std::string>(command_args), &total) ||
      !AddAllocationCharge(sizeof(ReplicationTransaction) + 64, &total)) {
    return std::nullopt;
  }
  // Codec sources own their buffers and allocation charges independently.
  return total;
}

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
  const std::size_t staging_bytes =
      replication_publish_queue_bytes_.load(std::memory_order_acquire);
  auto staging = TryReserveMemory(staging_bytes);
  if (!staging.has_value()) {
    if (const auto& history = CurrentStore().replication_history_) {
      history->ReclaimSecondary(staging_bytes);
      staging = TryReserveMemory(staging_bytes);
    }
  }
  if (!staging.has_value()) {
    RecordMemoryRejection();
    co_return absl::ResourceExhaustedError(
        "insufficient retained-memory budget for replication publisher "
        "staging");
  }
  // History activation admits its initial block. Accepted writes allocate
  // subsequent blocks only when publication actually consumes them.
  auto standby = AllocateReplicationLogBlock();
  if (!standby.ok()) co_return standby.status();
  log.publisher_staging_charge_.Adopt(&*staging, staging_bytes);
  log.state_ = ReplicationLogState::kActive;
  log.log_epoch_ = log_epoch;
  log.next_lsn_ = 1;
  log.max_blocks_ =
      std::max<std::size_t>(1, capacity_bytes / kStorageBlockBytes);
  log.append_min_blocks_ = 0;
  log.append_admitted_ = false;
  log.borrowed_blocks_ = 0;
  log.spare_blocks_.clear();
  log.publish_queue_.clear();
  log.standby_block_.emplace(std::move(*standby));
  log.publish_queue_bytes_ = 0;
  log.publisher_admitted_bytes_ = 0;
  log.publisher_admitted_items_ = 0;
  log.retained_lsn_by_session_.clear();
  log.capacity_backpressured_ = false;
  log.capacity_waits_ = 0;
  log.coverage_revocations_ = 0;
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
  // This is a target quota while consumers pin older events or an accepted
  // append is still producing fragments. ACK/completion eagerly retries it.
  log.max_blocks_ = requested_blocks;
  if (log.state_ == ReplicationLogState::kActive)
    ReclaimReplicationLogDebt(store);
  log.retention_advanced_.NotifyAll(*store.worker_);
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::SetReplicationBacklogBackpressure(
    bool enabled) {
  replication_backlog_backpressure_.store(enabled, std::memory_order_release);
  WorkerStore& store = CurrentStore();
  auto& log = store.replication_log_;
  if (!enabled) {
    ReclaimReplicationLogDebt(store);
    log.retention_advanced_.NotifyAll(*store.worker_);
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::SetReplicationPublishQueueCapacity(
    std::size_t capacity_bytes) {
  if (capacity_bytes == 0) {
    co_return absl::InvalidArgumentError(
        "replication publish queue capacity must be nonzero");
  }
  WorkerStore& store = CurrentStore();
  std::size_t growth = 0;
  auto add_growth = [&](const RetainedMemoryCharge& charge) {
    if (charge.bytes() >= capacity_bytes) return true;
    const std::size_t delta = capacity_bytes - charge.bytes();
    if (delta > std::numeric_limits<std::size_t>::max() - growth) return false;
    growth += delta;
    return true;
  };
  if ((store.replication_log_.state_ != ReplicationLogState::kDisabled &&
       !add_growth(store.replication_log_.publisher_staging_charge_)) ||
      !std::all_of(store.fullsync_sessions_.begin(),
                   store.fullsync_sessions_.end(), [&](const auto& item) {
                     return add_growth(item.second.publisher_staging_charge_);
                   })) {
    RecordMemoryRejection();
    co_return absl::ResourceExhaustedError(
        "replication publisher staging budget overflow");
  }
  std::optional<MemoryReservation> reservation;
  if (growth != 0) {
    reservation = TryReserveMemory(growth);
    if (!reservation.has_value()) {
      RecordMemoryRejection();
      co_return absl::ResourceExhaustedError(
          "insufficient retained-memory budget for replication publisher "
          "staging");
    }
  }
  auto grow_charge = [&](RetainedMemoryCharge* charge) {
    if (charge->bytes() >= capacity_bytes) return;
    if (charge->bytes() == 0) {
      charge->Account(CurrentMemoryAccountingShard(), capacity_bytes);
    } else {
      charge->Resize(capacity_bytes);
    }
  };
  if (store.replication_log_.state_ != ReplicationLogState::kDisabled) {
    grow_charge(&store.replication_log_.publisher_staging_charge_);
  }
  for (auto& [session_id, session] : store.fullsync_sessions_) {
    (void)session_id;
    grow_charge(&session.publisher_staging_charge_);
  }
  if (reservation.has_value()) reservation->Release();
  // A decrease changes admission immediately, but old ring capacity cannot be
  // returned without reallocating the queue. Its previous high-water charge
  // therefore remains until the log/session is destroyed.
  replication_publish_queue_bytes_.store(capacity_bytes,
                                         std::memory_order_release);
  store.replication_log_.publisher_capacity_ready_.NotifyAll(*store.worker_);
  store.fullsync_publisher_capacity_ready_.NotifyAll(*store.worker_);
  co_return absl::OkStatus();
}

bool StorageEngine::Impl::ReplicationLogActive() const noexcept {
  return CurrentStore().replication_log_.state_ == ReplicationLogState::kActive;
}

absl::StatusOr<std::uint64_t> NextReplicationPublisherPriority() {
  static std::atomic<std::uint64_t> next{1};
  std::uint64_t priority = next.load(std::memory_order_relaxed);
  do {
    if (priority == std::numeric_limits<std::uint64_t>::max()) {
      return absl::ResourceExhaustedError(
          "replication publisher priority space exhausted");
    }
  } while (!next.compare_exchange_weak(priority, priority + 1,
                                       std::memory_order_relaxed));
  return priority;
}

Task<absl::StatusOr<ReplicationPublisherAdmission>>
StorageEngine::Impl::AcquireReplicationPublisherAdmission(
    std::size_t logical_bytes,
    std::optional<ReplicationPublisherTarget> target) {
  WorkerStore& store = CurrentStore();
  if (store.replication_publisher_waiters_head_ == nullptr) {
    // The ordinary owner-local write neither allocates a queue node nor
    // contends on a global priority counter when capacity is already free.
    auto granted = TryGrantReplicationPublisherAdmission(logical_bytes, target);
    if (!granted.ok()) co_return granted.status();
    if (granted->has_value()) co_return std::move(**granted);
  }
  auto priority = NextReplicationPublisherPriority();
  if (!priority.ok()) co_return priority.status();
  ReplicationPublisherWaiter waiter{.priority_ = *priority};
  for (;;) {
    auto granted =
        TryAcquireReplicationPublisherAdmission(waiter, logical_bytes, target);
    if (!granted.ok()) {
      FinishReplicationPublisherAdmission(waiter);
      co_return granted.status();
    }
    if (granted->has_value()) {
      FinishReplicationPublisherAdmission(waiter);
      co_return std::move(**granted);
    }
    auto ready = co_await WaitReplicationPublisherAdmission(
        waiter, logical_bytes, target);
    if (!ready.ok()) {
      FinishReplicationPublisherAdmission(waiter);
      co_return ready;
    }
  }
}

absl::StatusOr<std::optional<ReplicationPublisherAdmission>>
StorageEngine::Impl::TryGrantReplicationPublisherAdmission(
    std::size_t logical_bytes,
    std::optional<ReplicationPublisherTarget> target) {
  WorkerStore& store = CurrentStore();
  auto& log = store.replication_log_;
  if (target.has_value() &&
      (target->partition_id_ >= kLogicalStorageShards ||
       target->db_id_ >= kLogicalDatabaseCount ||
       target->partition_id_ % worker_count_ != store.worker_->id())) {
    return absl::InvalidArgumentError(
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
  logical_bytes = std::max<std::size_t>(logical_bytes, 1);
  const std::size_t capacity =
      replication_publish_queue_bytes_.load(std::memory_order_acquire);
  if (log.state_ == ReplicationLogState::kActive &&
      (log.capacity_backpressured_ ||
       !PublisherHasCapacity(logical_bytes, capacity, log.publish_queue_bytes_,
                             log.publisher_admitted_bytes_))) {
    return std::nullopt;
  }
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
  ReplicationPublisherAdmission admission;
  if (log.state_ == ReplicationLogState::kActive) {
    admission.log_epoch_ = log.log_epoch_;
  }
  admission.fullsync_session_ids_.reserve(store.fullsync_sessions_.size());
  admission.fullsync_unstarted_guards_.reserve(
      target.has_value() ? store.fullsync_sessions_.size() : 0);
  for (const auto& [session_id, session] : store.fullsync_sessions_) {
    if (session.db_epoch_invalidated_) continue;
    if (session_needs_credit(session_id)) {
      admission.fullsync_session_ids_.push_back(session_id);
    } else if (target.has_value()) {
      admission.fullsync_unstarted_guards_.push_back(
          ReplicationPublisherAdmission::UnstartedGuard{
              .session_id_ = session_id,
              .partition_id_ = target->partition_id_,
              .db_id_ = target->db_id_,
          });
    }
  }

  // The queue's fixed staging charge already covers its ring slots. Admission
  // reserves those slots, but event payloads and backlog blocks are allocated
  // on demand after admission, with immediate retained-memory accounting.
  absl::Status prepared;
  if (admission.log_epoch_ != 0) {
    prepared = PrepareAdmittedQueueSlot(&log.publish_queue_,
                                        log.publisher_admitted_items_);
  }
  for (std::uint64_t session_id : admission.fullsync_session_ids_) {
    if (!prepared.ok()) break;
    auto& session = store.fullsync_sessions_.at(session_id);
    prepared = PrepareAdmittedQueueSlot(&session.publish_queue_,
                                        session.publisher_admitted_items_);
  }
  if (!prepared.ok()) {
    RecordMemoryRejection();
    return prepared;
  }

  if (admission.log_epoch_ != 0) {
    log.publisher_admitted_bytes_ += logical_bytes;
    ++log.publisher_admitted_items_;
  }
  for (std::uint64_t session_id : admission.fullsync_session_ids_) {
    auto& session = store.fullsync_sessions_.at(session_id);
    session.publisher_admitted_bytes_ += logical_bytes;
    ++session.publisher_admitted_items_;
  }
  for (const auto& guard : admission.fullsync_unstarted_guards_) {
    const std::uint32_t target_id =
        (static_cast<std::uint32_t>(guard.partition_id_) << 8) | guard.db_id_;
    ++store.fullsync_sessions_.at(guard.session_id_)
          .unstarted_admissions_[target_id];
  }
  return admission;
}

absl::StatusOr<std::optional<ReplicationPublisherAdmission>>
StorageEngine::Impl::TryAcquireReplicationPublisherAdmission(
    ReplicationPublisherWaiter& waiter, std::size_t logical_bytes,
    std::optional<ReplicationPublisherTarget> target) {
  WorkerStore& store = CurrentStore();
  if (waiter.priority_ == 0) {
    return absl::InvalidArgumentError("publisher admission priority is zero");
  }
  if (target.has_value() &&
      (target->partition_id_ >= kLogicalStorageShards ||
       target->db_id_ >= kLogicalDatabaseCount ||
       target->partition_id_ % worker_count_ != store.worker_->id())) {
    return absl::InvalidArgumentError(
        "replication publisher target does not belong to this worker");
  }
  if (!waiter.queued_) {
    // Newer priorities normally append in O(1). An older cross-worker request
    // may arrive late; insertion never evicts an existing tentative grant.
    // Its coordinator must return that grant if any other participant fails.
    auto* previous = store.replication_publisher_waiters_tail_;
    while (previous != nullptr && previous->priority_ > waiter.priority_) {
      previous = previous->previous_;
    }
    if (previous != nullptr && previous->priority_ == waiter.priority_) {
      return absl::InvalidArgumentError(
          "duplicate publisher admission priority");
    }
    waiter.previous_ = previous;
    waiter.next_ = previous == nullptr
                       ? store.replication_publisher_waiters_head_
                       : previous->next_;
    if (waiter.next_ != nullptr)
      waiter.next_->previous_ = &waiter;
    else
      store.replication_publisher_waiters_tail_ = &waiter;
    if (previous != nullptr)
      previous->next_ = &waiter;
    else
      store.replication_publisher_waiters_head_ = &waiter;
    waiter.queued_ = true;
  }
  if (store.replication_publisher_waiters_head_ != &waiter) {
    return std::nullopt;
  }
  return TryGrantReplicationPublisherAdmission(logical_bytes, target);
}

Task<absl::Status> StorageEngine::Impl::WaitReplicationPublisherAdmission(
    ReplicationPublisherWaiter& waiter, std::size_t logical_bytes,
    std::optional<ReplicationPublisherTarget> target) {
  WorkerStore& store = CurrentStore();
  auto& log = store.replication_log_;
  logical_bytes = std::max<std::size_t>(logical_bytes, 1);
  bool recorded_fullsync_wait = false;
  for (;;) {
    if (!waiter.queued_) {
      co_return absl::CancelledError("publisher admission intent cancelled");
    }
    if (store.replication_publisher_waiters_head_ != &waiter) {
      co_await store.replication_publisher_admission_ready_.Wait();
      continue;
    }
    const std::size_t capacity =
        replication_publish_queue_bytes_.load(std::memory_order_acquire);
    if (log.state_ == ReplicationLogState::kActive &&
        (log.capacity_backpressured_ ||
         !PublisherHasCapacity(logical_bytes, capacity,
                               log.publish_queue_bytes_,
                               log.publisher_admitted_bytes_))) {
      co_await log.publisher_capacity_ready_.Wait();
      continue;
    }
    bool fullsync_available = true;
    for (const auto& [session_id, session] : store.fullsync_sessions_) {
      if (session.db_epoch_invalidated_) continue;
      if (target.has_value()) {
        const auto& partition = PartitionFor(store, target->partition_id_);
        const auto capture = partition.fullsync_subscribers_.find(session_id);
        if (capture == partition.fullsync_subscribers_.end() ||
            capture->second.db_phases_[target->db_id_] ==
                WorkerStore::FullSyncCapture::DbPhase::kUnstarted) {
          continue;
        }
      }
      if (!PublisherHasCapacity(logical_bytes, capacity,
                                session.publish_queue_bytes_,
                                session.publisher_admitted_bytes_)) {
        fullsync_available = false;
        break;
      }
    }
    if (fullsync_available) co_return absl::OkStatus();
    if (!recorded_fullsync_wait) {
      ++store.fullsync_publisher_capacity_waits_;
      recorded_fullsync_wait = true;
    }
    // Notifications belong to the worker, never a removable FULL session.
    // The predicate check and Wait registration do not yield in between.
    co_await store.fullsync_publisher_capacity_ready_.Wait();
  }
}

void StorageEngine::Impl::FinishReplicationPublisherAdmission(
    ReplicationPublisherWaiter& waiter) {
  if (!waiter.queued_) return;
  WorkerStore& store = CurrentStore();
  if (waiter.previous_ != nullptr)
    waiter.previous_->next_ = waiter.next_;
  else
    store.replication_publisher_waiters_head_ = waiter.next_;
  if (waiter.next_ != nullptr)
    waiter.next_->previous_ = waiter.previous_;
  else
    store.replication_publisher_waiters_tail_ = waiter.previous_;
  waiter.previous_ = nullptr;
  waiter.next_ = nullptr;
  waiter.queued_ = false;
  store.replication_publisher_admission_ready_.NotifyAll(*store.worker_);
  // Cancellation may remove a head currently asleep on either capacity
  // notification. Wake it as well so its coordinator can join all waiters
  // before destroying the intrusive nodes.
  store.replication_log_.publisher_capacity_ready_.NotifyAll(*store.worker_);
  store.fullsync_publisher_capacity_ready_.NotifyAll(*store.worker_);
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
  if (store.replication_publisher_waiters_head_ != nullptr) {
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
  for (const auto& [session_id, session] : store.fullsync_sessions_) {
    if (session.db_epoch_invalidated_) continue;
    if (session_needs_credit(session_id)) {
      admission.fullsync_session_ids_.push_back(session_id);
      continue;
    }
    admission.fullsync_unstarted_guards_.push_back(
        ReplicationPublisherAdmission::UnstartedGuard{
            .session_id_ = session_id,
            .partition_id_ = target.partition_id_,
            .db_id_ = target.db_id_,
        });
  }

  for (std::uint64_t session_id : admission.fullsync_session_ids_) {
    auto& session = store.fullsync_sessions_.at(session_id);
    if (!PrepareAdmittedQueueSlot(&session.publish_queue_,
                                  session.publisher_admitted_items_)
             .ok()) {
      RecordMemoryRejection();
      return std::nullopt;
    }
  }

  for (std::uint64_t session_id : admission.fullsync_session_ids_) {
    auto& session = store.fullsync_sessions_.at(session_id);
    session.publisher_admitted_bytes_ += logical_bytes;
    ++session.publisher_admitted_items_;
  }
  for (const auto& guard : admission.fullsync_unstarted_guards_) {
    const std::uint32_t target_id =
        (static_cast<std::uint32_t>(guard.partition_id_) << 8) | guard.db_id_;
    ++store.fullsync_sessions_.at(guard.session_id_)
          .unstarted_admissions_[target_id];
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
    if (log.publisher_admitted_items_ != 0) --log.publisher_admitted_items_;
    log.publisher_capacity_ready_.NotifyAll(*store.worker_);
  }
  for (std::uint64_t session_id : admission.fullsync_session_ids_) {
    auto session = store.fullsync_sessions_.find(session_id);
    if (session == store.fullsync_sessions_.end()) continue;
    session->second.publisher_admitted_bytes_ -=
        std::min(session->second.publisher_admitted_bytes_, logical_bytes);
    if (session->second.publisher_admitted_items_ != 0) {
      --session->second.publisher_admitted_items_;
    }
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
  static_assert(sizeof(WorkerStore::ReplicationLogRuntime::PendingCommand) <=
                kReplicationPublisherItemMetadataBytes);
  WorkerStore& store = CurrentStore();
  auto& log = store.replication_log_;
  if (log.state_ != ReplicationLogState::kActive) return false;

  if (command.args_.empty()) return false;
  AccountReplicationCommand(&command);
  const auto staging_bytes = ReplicationCommandStagingBytes(command.args_);
  if (!staging_bytes.has_value()) {
    log.state_ = ReplicationLogState::kInvalid;
    spdlog::warn("replication publisher staging size overflow");
    return false;
  }
  if (*staging_bytes >
      std::numeric_limits<std::size_t>::max() - log.publish_queue_bytes_) {
    log.state_ = ReplicationLogState::kInvalid;
    spdlog::warn("replication publisher queue accounting overflow");
    return false;
  }

  log.publish_queue_bytes_ += *staging_bytes;
  WorkerStore::ReplicationLogRuntime::PendingCommand pending{
      .log_epoch_ = log.log_epoch_,
      .staging_bytes_ = *staging_bytes,
      .append_ = std::move(command),
      .fence_ = nullptr,
      .transaction_ = nullptr,
  };
  if (log.publish_queue_.size() == log.publish_queue_.capacity()) {
    log.publish_queue_bytes_ -= *staging_bytes;
    log.state_ = ReplicationLogState::kInvalid;
    RecordMemoryRejection();
    spdlog::warn(
        "replication publisher queue has no admitted slot; invalidating "
        "history");
    if (!log.publisher_running_) {
      log.publisher_running_ = true;
      store.worker_->Spawn(DrainReplicationPublishQueue(&store));
    }
    return false;
  }
  log.publish_queue_.push_back_prepared(std::move(pending));
  if (!log.publisher_running_) {
    log.publisher_running_ = true;
    store.worker_->Spawn(DrainReplicationPublishQueue(&store));
  }
  return true;
}

Task<absl::Status> StorageEngine::Impl::PublishEphemeralReplicationCommand(
    std::uint16_t partition_id, std::vector<std::string> args,
    MutationPrecondition mutation_precondition) {
  if (partition_id >= kLogicalStorageShards ||
      partition_id % worker_count_ != bycorf::ThisWorker().id_) {
    co_return absl::FailedPreconditionError(
        "ephemeral replication partition does not belong to this worker");
  }
  if (args.empty()) {
    co_return absl::InvalidArgumentError(
        "ephemeral replication command is empty");
  }

  const auto staging_bytes = ReplicationCommandStagingBytes(args);
  if (!staging_bytes.has_value()) {
    co_return absl::ResourceExhaustedError(
        "ephemeral replication command staging size overflow");
  }

  std::size_t event_bytes = ReplicationCommandFixedHeaderBytes(args.size());
  if (args.size() >
          static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max()) ||
      args.size() > (std::numeric_limits<std::size_t>::max() - event_bytes) /
                        sizeof(std::uint32_t)) {
    co_return absl::OutOfRangeError(
        "replication command argument count exceeds encoding bounds");
  }
  event_bytes += args.size() * sizeof(std::uint32_t);
  for (const auto& argument : args) {
    if (argument.size() > kMaxNativeReplicationArgumentBytes ||
        argument.size() >
            std::numeric_limits<std::size_t>::max() - event_bytes) {
      co_return absl::OutOfRangeError(
          "replication command exceeds encoding bounds");
    }
    event_bytes += argument.size();
  }
  auto admission = co_await AcquireReplicationPublisherAdmission(*staging_bytes,
                                                                 std::nullopt);
  if (!admission.ok()) co_return admission.status();

  auto publication = PrepareAdmittedReplicationCommand(
      *admission, ReplicationEventKind::kEphemeral, partition_id,
      std::move(args), std::nullopt);
  if (!publication.ok()) {
    ReleaseReplicationPublisherAdmission(*admission, *staging_bytes);
    co_return publication.status();
  }
  // Admission may suspend behind backlog or full-sync backpressure. Validate
  // external authority only after that wait and after all allocation, leaving
  // no suspension between this check and the synchronous publication cut.
  absl::Status permitted = mutation_precondition.Validate();
  if (!permitted.ok()) {
    ReleaseReplicationPublisherAdmission(*admission, *staging_bytes);
    co_return permitted;
  }
  absl::Status published =
      PublishPreparedReplicationCommand(*admission, std::move(*publication));
  ReleaseReplicationPublisherAdmission(*admission, *staging_bytes);
  co_return published;
}

absl::StatusOr<PreparedReplicationCommandPublication>
StorageEngine::Impl::PrepareAdmittedReplicationCommand(
    const ReplicationPublisherAdmission& admission, ReplicationEventKind kind,
    std::uint16_t partition_id, std::vector<std::string> args,
    std::optional<std::vector<std::string>> fullsync_projection,
    std::shared_ptr<const void> retained_owner) {
  if (partition_id >= kLogicalStorageShards ||
      partition_id % worker_count_ != bycorf::ThisWorker().id_ ||
      (kind != ReplicationEventKind::kCatalogMutation &&
       kind != ReplicationEventKind::kEphemeral) ||
      args.empty()) {
    return absl::FailedPreconditionError(
        "invalid admitted replication command publication");
  }
  const std::uint64_t sequence =
      next_replication_ephemeral_id_.fetch_add(1, std::memory_order_relaxed);
  if (sequence == 0 || sequence == std::numeric_limits<std::uint64_t>::max()) {
    next_replication_ephemeral_id_.store(
        std::numeric_limits<std::uint64_t>::max(), std::memory_order_relaxed);
    return absl::ResourceExhaustedError(
        "admitted replication sequence space exhausted");
  }
  ReplicationCommandAppend command{
      .kind_ = kind,
      .db_id_ = 0,
      .partition_id_ = partition_id,
      .partition_sequence_ = sequence,
      .retained_owner_ = std::move(retained_owner),
      .args_ = std::move(args),
  };
  AccountReplicationCommand(&command);
  PreparedReplicationCommandPublication publication;
  if (!admission.fullsync_session_ids_.empty() &&
      (!fullsync_projection.has_value() || !fullsync_projection->empty())) {
    if (fullsync_projection.has_value()) {
      auto fullsync_command =
          std::make_shared<ReplicationCommandAppend>(ReplicationCommandAppend{
              .kind_ = command.kind_,
              .db_id_ = command.db_id_,
              .partition_id_ = command.partition_id_,
              .partition_sequence_ = command.partition_sequence_,
              .args_ = std::move(*fullsync_projection),
          });
      AccountReplicationCommand(fullsync_command.get());
      publication.fullsync_command_ = std::move(fullsync_command);
    } else {
      auto fullsync_command =
          std::make_shared<ReplicationCommandAppend>(command);
      fullsync_command->retained_owner_.reset();
      AccountReplicationCommand(fullsync_command.get());
      publication.fullsync_command_ = std::move(fullsync_command);
    }
  }
  if (admission.log_epoch_ != 0) {
    publication.backlog_command_ = std::move(command);
  }
  return publication;
}

absl::Status StorageEngine::Impl::PublishPreparedReplicationCommand(
    const ReplicationPublisherAdmission& admission,
    PreparedReplicationCommandPublication publication) {
  WorkerStore& store = CurrentStore();
  for (const std::uint64_t session_id : admission.fullsync_session_ids_) {
    // A session may be cancelled after admission. Active-session failures
    // invalidate that full sync inside the helper; neither case may prevent
    // the live backlog or other sessions from receiving the command.
    (void)TryEnqueueFullSyncCommand(store, session_id,
                                    publication.fullsync_command_);
  }
  if (admission.log_epoch_ == 0) return absl::OkStatus();
  auto& log = store.replication_log_;
  if (log.log_epoch_ != admission.log_epoch_) {
    return absl::OkStatus();
  }
  if (!publication.backlog_command_.has_value() ||
      !TryEnqueueReplicationCommand(std::move(*publication.backlog_command_))) {
    // The mutation may already be durable. Keeping this history resumable
    // would let a replica continue across a missing event.
    log.state_ = ReplicationLogState::kInvalid;
    return absl::InternalError(
        "admitted command did not fit the replication backlog");
  }
  return absl::OkStatus();
}

absl::Status StorageEngine::Impl::PublishLateAdmittedReplicationCommand(
    const ReplicationPublisherAdmission& admission, ReplicationEventKind kind,
    std::uint16_t partition_id, std::vector<std::string> args,
    std::optional<std::vector<std::string>> fullsync_projection,
    std::shared_ptr<const void> retained_owner) {
  auto publication = PrepareAdmittedReplicationCommand(
      admission, kind, partition_id, std::move(args),
      std::move(fullsync_projection), std::move(retained_owner));
  if (!publication.ok()) {
    WorkerStore& store = CurrentStore();
    if (admission.log_epoch_ != 0 &&
        store.replication_log_.log_epoch_ == admission.log_epoch_) {
      store.replication_log_.state_ = ReplicationLogState::kInvalid;
    }
    for (const std::uint64_t session_id : admission.fullsync_session_ids_) {
      if (store.fullsync_sessions_.contains(session_id)) {
        InvalidateFullSyncSession(store, session_id);
      }
    }
    return publication.status();
  }
  return PublishPreparedReplicationCommand(admission, std::move(*publication));
}

bool StorageEngine::Impl::TryEnqueueReplicationTransaction(
    std::shared_ptr<ReplicationTransaction> transaction) {
  WorkerStore& store = CurrentStore();
  auto& log = store.replication_log_;
  if (log.state_ != ReplicationLogState::kActive || transaction == nullptr ||
      transaction->envelope_metadata_.empty() ||
      transaction->command_args_.empty()) {
    return false;
  }

  const auto staging_bytes =
      TransactionStagingBytes(*transaction, bycorf::ThisWorker().id_);
  if (!staging_bytes.has_value()) {
    log.state_ = ReplicationLogState::kInvalid;
    spdlog::warn("replication transaction staging size overflow");
    return false;
  }
  if (*staging_bytes >
      std::numeric_limits<std::size_t>::max() - log.publish_queue_bytes_) {
    log.state_ = ReplicationLogState::kInvalid;
    spdlog::warn("replication transaction queue accounting overflow");
    return false;
  }

  log.publish_queue_bytes_ += *staging_bytes;
  WorkerStore::ReplicationLogRuntime::PendingCommand pending{
      .log_epoch_ = log.log_epoch_,
      .staging_bytes_ = *staging_bytes,
      .append_ = {},
      .fence_ = nullptr,
      .transaction_ = std::move(transaction),
  };
  // The payload is owned once by the shared transaction. Participant markers
  // retain only that shared_ptr and must not charge or copy the command body.
  if (log.publish_queue_.size() == log.publish_queue_.capacity()) {
    log.publish_queue_bytes_ -= *staging_bytes;
    RecordMemoryRejection();
    return false;
  }
  log.publish_queue_.push_back_prepared(std::move(pending));
  if (!log.publisher_running_) {
    log.publisher_running_ = true;
    store.worker_->Spawn(DrainReplicationPublishQueue(&store));
  }
  return true;
}

namespace {

absl::StatusOr<ReplicationCommandPayloadSource>
BuildReplicationTransactionEnvelope(const ReplicationTransaction& transaction,
                                    unsigned worker) {
  assert(!transaction.envelope_metadata_.empty());
  const bool carries_payload = worker == transaction.payload_flow_;
  // Borrow the immutable transaction strings until append completes. Only the
  // small view table is copied; even a large canonical body has one owner.
  std::vector<std::string_view, RetainedAllocator<std::string_view>> args{
      RetainedAllocator<std::string_view>(
          RetainedAllocationDomain{.externally_admitted_ = true})};
  args.reserve(carries_payload ? 1 + transaction.command_args_.size() : 1);
  args.push_back(transaction.envelope_metadata_);
  if (carries_payload) {
    for (const auto& argument : transaction.command_args_)
      args.push_back(argument);
  }
  return ReplicationCommandPayloadSource::Create(transaction.db_id_, args);
}

}  // namespace

Task<absl::StatusOr<std::uint64_t>> StorageEngine::Impl::FenceReplicationLog(
    std::function<bool()> cancelled) {
  WorkerStore& store = CurrentStore();
  auto& log = store.replication_log_;
  if (log.state_ != ReplicationLogState::kActive) {
    co_return InvalidState("replication log is not active");
  }

  constexpr std::size_t kFenceStagingBytes =
      kReplicationPublisherItemMetadataBytes;
  for (;;) {
    if (cancelled && cancelled())
      co_return absl::CancelledError("publisher fence cancelled");
    const std::size_t capacity =
        replication_publish_queue_bytes_.load(std::memory_order_acquire);
    if (PublisherHasCapacity(kFenceStagingBytes, capacity,
                             log.publish_queue_bytes_,
                             log.publisher_admitted_bytes_)) {
      break;
    }
    if (cancelled) {
      auto waited = co_await bycorf::SleepFor(*store.worker_,
                                              std::chrono::milliseconds(1));
      if (!waited.ok()) co_return waited;
    } else {
      co_await log.publisher_capacity_ready_.Wait();
    }
    if (log.state_ != ReplicationLogState::kActive) {
      co_return InvalidState(
          "replication log stopped while waiting for publisher fence space");
    }
  }

  auto fence =
      std::make_shared<WorkerStore::ReplicationLogRuntime::PublishFence>();
  {
    if (log.publisher_admitted_items_ ==
            std::numeric_limits<std::size_t>::max() ||
        log.publish_queue_.size() > std::numeric_limits<std::size_t>::max() -
                                        log.publisher_admitted_items_ - 1) {
      co_return absl::ResourceExhaustedError(
          "replication publisher fence queue is too large");
    }
    const std::size_t minimum_capacity =
        log.publish_queue_.size() + log.publisher_admitted_items_ + 1;
    if (log.publish_queue_.growth_bytes_for_capacity(minimum_capacity) ==
        std::numeric_limits<std::size_t>::max()) {
      co_return absl::ResourceExhaustedError(
          "replication publisher fence queue is too large");
    }
    // The fixed publisher charge already admitted this ring capacity. A
    // physical allocation failure here is process OOM and follows the
    // fail-fast policy instead of being confused with maxmemory rejection.
    log.publish_queue_.PrepareCapacity(minimum_capacity);
    log.publish_queue_.push_back_prepared(
        WorkerStore::ReplicationLogRuntime::PendingCommand{
            .log_epoch_ = log.log_epoch_,
            .staging_bytes_ = kFenceStagingBytes,
            .append_ = {},
            .fence_ = fence,
            .transaction_ = nullptr,
        });
    // Fence payloads are small, but an arbitrary number may wait behind a
    // pending transaction. Charging the same conservative metadata allowance
    // as ordinary items keeps the externally-accounted ring within its fixed
    // staging budget.
    log.publish_queue_bytes_ += kFenceStagingBytes;
  }
  if (!log.publisher_running_) {
    log.publisher_running_ = true;
    store.worker_->Spawn(DrainReplicationPublishQueue(&store));
  }
  while (!fence->complete_) {
    if (cancelled) {
      if (cancelled())
        co_return absl::CancelledError("publisher fence cancelled");
      auto waited = co_await bycorf::SleepFor(*store.worker_,
                                              std::chrono::milliseconds(1));
      if (!waited.ok()) co_return waited;
    } else {
      co_await fence->ready_.Wait();
    }
  }
  if (!fence->status_.ok()) co_return fence->status_;
  co_return fence->next_lsn_;
}

Task<absl::Status> StorageEngine::Impl::DrainReplicationPublishQueue(
    WorkerStore* store) {
  auto& log = store->replication_log_;
  // Scratch belongs to this publisher invocation, not the log epoch: a
  // suspended old publisher must never share buffers with a replacement.
  ReplicationCommandPayloadSource command_scratch;
  while (!log.publish_queue_.empty()) {
    struct ReclaimPublicationDebt {
      Impl* impl;
      WorkerStore* store;
      std::uint64_t epoch;
      ~ReclaimPublicationDebt() {
        if (store->replication_log_.log_epoch_ == epoch)
          impl->ReclaimReplicationLogDebt(*store);
      }
    } reclaim{this, store, log.publish_queue_.front().log_epoch_};
    // Drop codec views and canonical payload references before reclaiming debt
    // and waking admissions. Retained scratch capacity stays charged. An old
    // publisher cannot reclaim a new epoch.
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
      if (log.publish_queue_bytes_ >= pending.staging_bytes_) {
        log.publish_queue_bytes_ -= pending.staging_bytes_;
      }
      log.publisher_capacity_ready_.NotifyAll(*store->worker_);
      continue;
    }
    if (pending.transaction_ != nullptr) {
      while (
          pending.log_epoch_ == log.log_epoch_ &&
          log.state_ == ReplicationLogState::kActive &&
          pending.transaction_->resolution_.load(std::memory_order_acquire) ==
              ReplicationTransactionResolution::kPending) {
        co_await bycorf::Yield(*store->worker_);
      }
      // Disable can replace this history while the transaction is unresolved.
      // Its accounting was already reset: an old resolution
      // must neither invalidate the new epoch nor consume its queue credits.
      if (pending.log_epoch_ != log.log_epoch_) continue;
      if (log.state_ != ReplicationLogState::kActive) break;
      const ReplicationTransactionResolution resolution =
          pending.transaction_->resolution_.load(std::memory_order_acquire);
      if (resolution == ReplicationTransactionResolution::kDiscard) {
        if (log.publish_queue_bytes_ >= pending.staging_bytes_) {
          log.publish_queue_bytes_ -= pending.staging_bytes_;
        }
        log.publisher_capacity_ready_.NotifyAll(*store->worker_);
        continue;
      }
      if (resolution == ReplicationTransactionResolution::kInvalidate) {
        // The primary mutation is already visible, so dropping only this
        // marker would let replicas continue from a history with a hole.
        // Invalidating every participant's log forces the safe full-sync path.
        log.state_ = ReplicationLogState::kInvalid;
        spdlog::warn(
            "replication transaction payload unavailable; invalidating "
            "history");
        break;
      }
      const auto final_staging_bytes = TransactionStagingBytes(
          *pending.transaction_, bycorf::ThisWorker().id_);
      if (!final_staging_bytes.has_value()) {
        log.state_ = ReplicationLogState::kInvalid;
        spdlog::warn("replication transaction queue size overflow");
        break;
      }
      if (*final_staging_bytes > pending.staging_bytes_) {
        const std::size_t queued_without_pending =
            log.publish_queue_bytes_ -
            std::min(log.publish_queue_bytes_, pending.staging_bytes_);
        if (*final_staging_bytes >
            std::numeric_limits<std::size_t>::max() - queued_without_pending) {
          log.state_ = ReplicationLogState::kInvalid;
          spdlog::warn(
              "canonical replication transaction queue accounting overflow");
          break;
        }
        // The shared canonical payload already accounts its retained memory.
        // This FIFO has one consumer, so waiting for queued_without_pending to
        // shrink would wait on items that only this coroutine can drain. Let
        // the oldest admitted item temporarily exceed the byte waterline;
        // subsequent admissions remain blocked until this item is published.
        log.publish_queue_bytes_ =
            queued_without_pending + *final_staging_bytes;
      } else {
        log.publish_queue_bytes_ -=
            std::min(log.publish_queue_bytes_,
                     pending.staging_bytes_ - *final_staging_bytes);
        log.publisher_capacity_ready_.NotifyAll(*store->worker_);
      }
      pending.staging_bytes_ = *final_staging_bytes;
      pending.append_ = ReplicationCommandAppend{
          .kind_ = ReplicationEventKind::kTransaction,
          .db_id_ = pending.transaction_->db_id_,
          .partition_id_ = static_cast<std::uint16_t>(bycorf::ThisWorker().id_),
          .partition_sequence_ = pending.transaction_->id_,
          .args_ = {},
      };
#if LAVIK_FAULTS_ENABLED
      static std::atomic<bool> catalog_transaction_publish_failed{false};
      const char* failure_marker =
          std::getenv("LAVIK_FAIL_REPLICATION_TRANSACTION_CONTAINING_ONCE");
      if (failure_marker != nullptr && *failure_marker != '\0' &&
          std::any_of(pending.transaction_->command_args_.begin(),
                      pending.transaction_->command_args_.end(),
                      [failure_marker](const std::string& argument) {
                        return argument.find(failure_marker) !=
                               std::string::npos;
                      }) &&
          !catalog_transaction_publish_failed.exchange(
              true, std::memory_order_acq_rel)) {
        log.state_ = ReplicationLogState::kInvalid;
        spdlog::warn(
            "injecting replication transaction publication failure for {}",
            failure_marker);
        break;
      }
#endif
    }
    if (log.state_ != ReplicationLogState::kActive ||
        pending.log_epoch_ != log.log_epoch_) {
      if (log.publish_queue_bytes_ >= pending.staging_bytes_) {
        log.publish_queue_bytes_ -= pending.staging_bytes_;
      }
      log.publisher_capacity_ready_.NotifyAll(*store->worker_);
      continue;
    }

    // Clear views only after AppendReplicationLog has finished reading them,
    // and before pending's strings and the publication debt are released.
    struct ClearEncodingScratch {
      ReplicationCommandPayloadSource* source;
      ~ClearEncodingScratch() { source->ClearForReuse(); }
    } clear_scratch{&command_scratch};
    std::optional<ReplicationCommandPayloadSource> transaction_source;
    ReplicationCommandPayloadSource* command_source = &command_scratch;
    absl::Status encoded;
    if (pending.transaction_ != nullptr) {
      auto source = BuildReplicationTransactionEnvelope(
          *pending.transaction_, bycorf::ThisWorker().id_);
      encoded = source.status();
      if (source.ok()) {
        transaction_source.emplace(std::move(*source));
        command_source = &*transaction_source;
      }
    } else {
      encoded =
          command_scratch.Reset(pending.append_.db_id_, pending.append_.args_);
    }
    if (!encoded.ok()) {
      log.state_ = ReplicationLogState::kInvalid;
      spdlog::warn("replication command encoding failed: {}",
                   encoded.message());
      break;
    }
    auto appended = co_await AppendReplicationLog(
        ReplicationLogAppend{
            .kind_ = pending.append_.kind_,
            .partition_id_ = pending.append_.partition_id_,
            .partition_sequence_ = pending.append_.partition_sequence_,
            .payload_ = {},
            .payload_source_ = command_source,
        },
        /*already_admitted=*/true);
    if (!appended.ok()) {
      // Disable may wake an old publisher after replacing its history. Its
      // pending charge was already cleared; keep draining any new epoch's
      // queue without invalidating it or subtracting the old item's charge.
      if (pending.log_epoch_ != log.log_epoch_) {
        continue;
      }
      if (log.state_ == ReplicationLogState::kActive) {
        log.state_ = ReplicationLogState::kInvalid;
      }
      spdlog::warn("replication backlog append failed: {}",
                   appended.status().message());
      break;
    }
    if (log.publish_queue_bytes_ >= pending.staging_bytes_) {
      log.publish_queue_bytes_ -= pending.staging_bytes_;
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
  log.retention_advanced_.NotifyAll(*store->worker_);
  co_return absl::OkStatus();
}

Task<absl::StatusOr<PreparedFlushPublication>>
StorageEngine::Impl::PrepareFlushReplication(
    std::optional<std::uint8_t> db_id,
    const std::array<std::uint64_t, kLogicalDatabaseCount>& db_epochs) {
  if ((db_id.has_value() &&
       (*db_id >= kLogicalDatabaseCount || db_epochs[*db_id] == 0)) ||
      (!db_id.has_value() &&
       std::any_of(db_epochs.begin(), db_epochs.end(),
                   [](std::uint64_t epoch) { return epoch == 0; }))) {
    co_return absl::InvalidArgumentError("invalid FLUSH replication barrier");
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
  std::vector<std::string> args{db_id ? "FLUSHDB" : "FLUSHALL",
                                std::to_string(barrier_id)};
  if (db_id)
    args.push_back(std::to_string(db_epochs[*db_id]));
  else
    for (const auto epoch : db_epochs) args.push_back(std::to_string(epoch));
  PreparedFlushPublication publication;
  publication.flows_.reserve(worker_count_);
  for (unsigned target = 0; target < worker_count_; ++target) {
    auto capture = [this]() -> Task<std::uint64_t> {
      co_return CurrentStore().replication_log_.log_epoch_;
    };
    std::uint64_t epoch;
    if (target == bycorf::ThisWorker().id_)
      epoch = co_await capture();
    else
      epoch = co_await bycorf::SubmitTaskTo(target, capture);
    publication.flows_.push_back({
        .log_epoch_ = epoch,
        .command_ =
            {
                .kind_ = ReplicationEventKind::kControl,
                .db_id_ = db_id.value_or(0),
                // Control barriers are history-local, not partition
                // mutations.
                .partition_id_ = 0,
                .partition_sequence_ = barrier_id,
                .args_ = args,
            },
    });
    AccountReplicationCommand(&publication.flows_.back().command_);
  }
  co_return publication;
}

Task<absl::Status> StorageEngine::Impl::PublishFlushReplication(
    PreparedFlushPublication publication) {
  assert(publication.flows_.size() == worker_count_);
  for (unsigned target = 0; target < worker_count_; ++target) {
    auto publish = [this, target,
                    flow = std::move(publication.flows_[target])]() mutable
        -> Task<absl::Status> {
      auto& log = CurrentStore().replication_log_;
      // Never put an old-history barrier into a replacement history. Failed
      // enqueue already invalidates the flow, as for ordinary writes; its
      // sessions are cancelled and rebuilt by the replication manager.
      LAVIK_FAULT_INJECT(if (target == 1) {
        const char* fail = std::getenv("LAVIK_FLUSH_PUBLISH_FAIL_FILE");
        if (fail != nullptr && ::access(fail, F_OK) == 0) {
          // A prefix of flows already received the barrier. Exercise ordinary
          // history invalidation and session/barrier cancellation from here.
          log.state_ = ReplicationLogState::kInvalid;
        }
      });
      if (log.log_epoch_ == flow.log_epoch_ && ReplicationLogActive()) {
        (void)TryEnqueueReplicationCommand(std::move(flow.command_));
      }
      co_return absl::OkStatus();
    };
    if (target == bycorf::ThisWorker().id_)
      co_await publish();
    else
      co_await bycorf::SubmitTaskTo(target, std::move(publish));
    LAVIK_FAULT_INJECT(if (target == 0) {
      auto paused = co_await fault_injection::PauseWhileFileExists(
          "LAVIK_FLUSH_AFTER_FIRST_FLOW_HOLD_FILE");
      if (!paused.ok()) co_return paused;
    });
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::ReclaimReplicationLogPrefix(
    WorkerStore& store, std::uint64_t keep_from_lsn, bool force_all) {
  auto& log = store.replication_log_;
  while (!log.blocks_.empty()) {
    const auto& front = log.blocks_.front();
    if (!force_all && (!front.sealed_ || front.last_lsn_ >= keep_from_lsn ||
                       front.last_lsn_ >= log.next_lsn_)) {
      break;
    }
    log.blocks_.pop_front();
    if (log.borrowed_blocks_ != 0) --log.borrowed_blocks_;
  }
  if (!force_all) ReclaimReplicationLogDebt(store);
  co_return absl::OkStatus();
}

absl::Status StorageEngine::Impl::EnsureReplicationLogPool(
    WorkerStore& store, std::size_t required_blocks) {
  auto& log = store.replication_log_;
  const std::size_t before = log.spare_blocks_.size();
  std::size_t owned = log.blocks_.size() + log.spare_blocks_.size() +
                      (log.standby_block_.has_value() ? 1 : 0);
  while (owned < required_blocks) {
    auto block = AllocateReplicationLogBlock();
    if (!block.ok()) {
      // A failed raw-API expansion must preserve the existing pool and its
      // memory ownership, without retaining a partially admitted expansion.
      while (log.spare_blocks_.size() > before) log.spare_blocks_.pop_back();
      return block.status();
    }
    log.spare_blocks_.push_back(std::move(*block));
    ++owned;
  }
  if (log.spare_blocks_.size() != before) {
    // A publisher may be waiting for a pinned allocation below the configured
    // quota after an optional growth failed. Newly owned blocks unblock it
    // even if that consumer has not advanced its ACK yet.
    log.retention_advanced_.NotifyAll(*store.worker_);
  }
  return absl::OkStatus();
}

void StorageEngine::Impl::TrimReplicationLogPool(WorkerStore& store) {
  auto& log = store.replication_log_;
  // Raw appends own the prepared complete-event pool until all fragments are
  // committed. Otherwise an idle allocation is the first place to repay a
  // borrowed block; freeing any block returns the same allocator-byte budget.
  const std::size_t keep = std::max(log.max_blocks_, log.append_min_blocks_);
  auto excess = [&] {
    return log.blocks_.size() + log.spare_blocks_.size() +
               (log.standby_block_.has_value() ? 1 : 0) >
           keep;
  };
  auto repay = [&] {
    if (log.borrowed_blocks_ != 0) --log.borrowed_blocks_;
  };
  while (!log.spare_blocks_.empty() &&
         (excess() ||
          (log.borrowed_blocks_ != 0 && log.append_min_blocks_ == 0))) {
    log.spare_blocks_.pop_back();
    repay();
  }
  if (log.standby_block_.has_value() &&
      (excess() ||
       (log.borrowed_blocks_ != 0 && log.append_min_blocks_ == 0))) {
    log.standby_block_.reset();
    repay();
  }
}

void StorageEngine::Impl::ReclaimReplicationLogDebt(WorkerStore& store) {
  auto& log = store.replication_log_;
  // A payload source can suspend with a direct view into its active block.
  // The append guard retries reclamation after the whole event is visible.
  if (log.append_min_blocks_ != 0) return;
  const auto borrowed_before = log.borrowed_blocks_;
  const auto blocks_before = log.blocks_.size();
  TrimReplicationLogPool(store);
  auto pressure = [&] {
    return log.borrowed_blocks_ != 0 || log.blocks_.size() > log.max_blocks_;
  };
  if (pressure() &&
      !replication_backlog_backpressure_.load(std::memory_order_acquire) &&
      !log.retained_lsn_by_session_.empty()) {
    ++log.coverage_revocations_;
    log.retained_lsn_by_session_.clear();
  }
  std::uint64_t keep_from = log.next_lsn_;
  for (const auto& [session_id, lsn] : log.retained_lsn_by_session_) {
    (void)session_id;
    keep_from = std::min(keep_from, lsn);
  }
  while (pressure() && !log.blocks_.empty() &&
         log.blocks_.front().last_lsn_ < keep_from) {
    // Fragmented events occupy dedicated blocks. Reclaim through their final
    // fragment in one synchronous step so the visible floor is always an
    // event boundary. A fully ACKed active block is equally safe to destroy.
    std::uint64_t evicted_through = log.blocks_.front().last_lsn_;
    do {
      evicted_through =
          std::max(evicted_through, log.blocks_.front().last_lsn_);
      log.blocks_.pop_front();
      if (log.borrowed_blocks_ != 0) --log.borrowed_blocks_;
    } while (!log.blocks_.empty() &&
             log.blocks_.front().first_lsn_ <= evicted_through);
  }
  const bool was_backpressured = log.capacity_backpressured_;
  log.capacity_backpressured_ = pressure();
  if (!was_backpressured && log.capacity_backpressured_) ++log.capacity_waits_;
  if (borrowed_before != log.borrowed_blocks_ ||
      blocks_before != log.blocks_.size() ||
      was_backpressured != log.capacity_backpressured_) {
    log.retention_advanced_.NotifyAll(*store.worker_);
  }
  if (was_backpressured && !log.capacity_backpressured_)
    log.publisher_capacity_ready_.NotifyAll(*store.worker_);
}

auto StorageEngine::Impl::AllocateReplicationLogBlock(bool already_admitted)
    -> absl::StatusOr<WorkerStore::ReplicationLogBlock> {
  using Block = ::lavik::detail::ReplicationLogBlock;
  auto allocated = Block::Allocate(kStorageBlockBytes);
  if (!allocated.ok()) {
    if (const auto& history = CurrentStore().replication_history_) {
      history->ReclaimSecondary(Block::AllocationBytes(kStorageBlockBytes));
      allocated = Block::Allocate(kStorageBlockBytes);
    }
  }
  if (!allocated.ok() && already_admitted) {
    // The dataset mutation is committed. A soft budget must not tear a hole
    // in its replication history; retained allocators still account both the
    // payload bytes and sparse index before another admission can run.
    allocated = Block::Allocate(kStorageBlockBytes, true);
    if (allocated.ok()) ++CurrentStore().replication_log_.borrowed_blocks_;
  }
  if (!allocated.ok()) {
    RecordMemoryRejection();
    return allocated.status();
  }
  return WorkerStore::ReplicationLogBlock(std::move(*allocated));
}

Task<absl::Status> StorageEngine::Impl::EnsureReplicationLogActiveBlock(
    WorkerStore& store, std::uint64_t protected_lsn) {
  auto& log = store.replication_log_;
  const std::uint64_t log_epoch = log.log_epoch_;
  if (!log.blocks_.empty() && !log.blocks_.back().sealed_)
    co_return absl::OkStatus();
  if (log.max_blocks_ == 0)
    co_return InvalidState("replication log has no capacity");

  for (;;) {
    std::uint64_t keep_from = protected_lsn;
    for (const auto& [session_id, lsn] : log.retained_lsn_by_session_) {
      (void)session_id;
      keep_from = std::min(keep_from, lsn);
    }
    const bool evictable = !log.blocks_.empty() &&
                           log.blocks_.front().sealed_ &&
                           log.blocks_.front().last_lsn_ < keep_from;
    const std::size_t target =
        std::max(log.max_blocks_, log.append_min_blocks_);
    if (log.blocks_.size() >= target && evictable) {
      // Reuse a complete safe event before growing the reconnect window.
      std::uint64_t through = log.blocks_.front().last_lsn_;
      do {
        through = std::max(through, log.blocks_.front().last_lsn_);
        auto& retired = log.blocks_.front();
        retired.history_charge_.Reset();
        retired.Reset();
        log.spare_blocks_.push_back(std::move(retired));
        log.blocks_.pop_front();
      } while (!log.blocks_.empty() &&
               log.blocks_.front().first_lsn_ <= through);
    }
    if (log.append_admitted_ || log.blocks_.size() < target) {
      if (log.spare_blocks_.empty() && !log.standby_block_.has_value()) {
        auto allocated = AllocateReplicationLogBlock(log.append_admitted_);
        if (!allocated.ok()) co_return allocated.status();
        log.spare_blocks_.push_back(std::move(*allocated));
      }
      break;
    }
    if (!replication_backlog_backpressure_.load(std::memory_order_acquire)) {
      ++log.coverage_revocations_;
      log.retained_lsn_by_session_.clear();
      continue;
    }
    // Only raw callers wait here. Accepted publication continues so replicas
    // can receive every participant marker and the complete payload.
    log.capacity_backpressured_ = true;
    ++log.capacity_waits_;
    co_await log.retention_advanced_.Wait();
    if (log.state_ != ReplicationLogState::kActive ||
        log.log_epoch_ != log_epoch)
      co_return InvalidState("replication log stopped while waiting for ACK");
  }

  ReplicationHistory::PrimaryCharge history_charge;
  if (const auto& history = store.replication_history_)
    history_charge = history->ChargePrimary(kStorageBlockBytes);
  if (!log.spare_blocks_.empty()) {
    log.blocks_.push_back(std::move(log.spare_blocks_.back()));
    log.spare_blocks_.pop_back();
  } else {
    log.blocks_.push_back(std::move(*log.standby_block_));
    log.standby_block_.reset();
  }
  log.blocks_.back().history_charge_ = std::move(history_charge);
  if (log.append_admitted_ &&
      (log.borrowed_blocks_ != 0 || log.blocks_.size() > log.max_blocks_)) {
    if (!log.capacity_backpressured_) ++log.capacity_waits_;
    log.capacity_backpressured_ = true;
  }
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
    ReplicationLogAppend event, bool already_admitted) {
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
       event.kind_ != ReplicationEventKind::kControl &&
       event.kind_ != ReplicationEventKind::kEphemeral &&
       event.kind_ != ReplicationEventKind::kCatalogMutation)) {
    co_return absl::Status(absl::StatusCode::kInvalidArgument,
                           "invalid replication event metadata");
  }
  const std::uint64_t logical_payload_bytes =
      event.payload_source_ == nullptr ? event.payload_.size()
                                       : event.payload_source_->size();
  if (logical_payload_bytes > std::numeric_limits<std::size_t>::max()) {
    co_return absl::Status(absl::StatusCode::kOutOfRange,
                           "replication event exceeds addressable size");
  }
  const std::size_t frame_payload_capacity =
      kStorageBlockBytes - sizeof(ReplicationFrameHeader);
  const std::size_t required_blocks =
      logical_payload_bytes == 0
          ? 1
          : 1 + (logical_payload_bytes - 1) / frame_payload_capacity;
  // Replication events span blocks independently of the storage-record size
  // limit. Every fragment index must remain representable before any append.
  if (required_blocks - 1 > std::numeric_limits<std::uint32_t>::max()) {
    co_return absl::OutOfRangeError(
        "replication event exceeds the fragment index range");
  }
  if (!already_admitted) {
    // The raw API has no preceding dataset mutation. Establish its complete
    // guarantee before writing even the first fragment, independently of the
    // configured reconnect-window target.
    absl::Status prepared = EnsureReplicationLogPool(store, required_blocks);
    if (!prepared.ok()) co_return prepared;
  }
  if (log.next_lsn_ == std::numeric_limits<std::uint64_t>::max()) {
    log.state_ = ReplicationLogState::kInvalid;
    co_return absl::Status(absl::StatusCode::kResourceExhausted,
                           "replication LSN space is exhausted");
  }

  const std::size_t payload_size =
      static_cast<std::size_t>(logical_payload_bytes);
  const std::uint64_t log_epoch = log.log_epoch_;
  // Protect the incomplete event from ACK/configuration-triggered trimming.
  // Accepted writes may borrow blocks and never wait for an ACK mid-event.
  log.append_min_blocks_ = required_blocks;
  log.append_admitted_ = already_admitted;
  struct AppendGuard {
    Impl* impl;
    WorkerStore* store;
    WorkerStore::ReplicationLogRuntime& log;
    std::uint64_t epoch;
    ~AppendGuard() {
      if (log.log_epoch_ == epoch) {
        log.append_min_blocks_ = 0;
        const bool admitted = std::exchange(log.append_admitted_, false);
        if (admitted)
          impl->ReclaimReplicationLogDebt(*store);
        else {
          impl->TrimReplicationLogPool(*store);
          if (log.borrowed_blocks_ == 0 &&
              log.blocks_.size() <= log.max_blocks_) {
            log.capacity_backpressured_ = false;
            log.publisher_capacity_ready_.NotifyAll(*store->worker_);
          }
        }
      }
    }
  } append_guard{this, &store, log, log_epoch};
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
      if (log.state_ == ReplicationLogState::kActive &&
          log.log_epoch_ == log_epoch) {
        log.state_ = ReplicationLogState::kInvalid;
      }
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
    auto frame = block.AppendBuffer(frame_bytes);
    auto payload = frame.subspan(sizeof(ReplicationFrameHeader), payload_bytes);
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
    block.CommitAppend(lsn, fragment_index, frame_bytes);
    payload_offset += payload_bytes;
    if (!last) ++fragment_index;
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
  // This worker-local copy-out never suspends. Reading committed history must
  // remain independent of an append waiting for the consumer's ACK.
  // next_lsn_ is the commit boundary: never expose an unfinished event.
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

    std::uint32_t offset =
        block.FindOffset(batch.next_.lsn_, batch.next_.fragment_index_);
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
      if (header.lsn_ > tail_lsn) {
        batch.at_tail_ = true;
        co_return batch;
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
    if (keep_from_lsn == found->second) return absl::OkStatus();
    found->second = keep_from_lsn;
  }
  ReclaimReplicationLogDebt(store);
  log.retention_advanced_.NotifyAll(*store.worker_);
  return absl::OkStatus();
}

void StorageEngine::Impl::ReleaseReplicationLogRetention(
    std::uint64_t session_id) {
  WorkerStore& store = CurrentStore();
  auto& log = store.replication_log_;
  if (log.retained_lsn_by_session_.erase(session_id) != 0) {
    ReclaimReplicationLogDebt(store);
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
  if (log.state_ != ReplicationLogState::kDisabled) {
    absl::Status reclaimed =
        co_await ReclaimReplicationLogPrefix(store, 0, /*force_all=*/true);
    if (!reclaimed.ok()) {
      log.mutex_.Unlock(*store.worker_);
      co_return reclaimed;
    }
    log.state_ = ReplicationLogState::kDisabled;
    log.log_epoch_ = 0;
    log.next_lsn_ = 1;
    log.max_blocks_ = 0;
    log.append_min_blocks_ = 0;
    log.append_admitted_ = false;
    log.borrowed_blocks_ = 0;
    log.spare_blocks_.clear();
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
    log.publisher_admitted_items_ = 0;
    log.publisher_staging_charge_.Reset();
    log.retained_lsn_by_session_.clear();
    log.retention_advanced_.NotifyAll(*store.worker_);
    log.publisher_capacity_ready_.NotifyAll(*store.worker_);
  }
  log.standby_block_.reset();
  log.mutex_.Unlock(*store.worker_);

  co_return absl::OkStatus();
}

bool StorageEngine::Impl::ReplicationRetentionBlocksPublication(
    std::uint64_t session_id) const {
  const auto& log = CurrentStore().replication_log_;
  const auto retained = log.retained_lsn_by_session_.find(session_id);
  return log.capacity_backpressured_ && !log.blocks_.empty() &&
         retained != log.retained_lsn_by_session_.end() &&
         retained->second <= log.blocks_.front().last_lsn_;
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
      .coverage_revocations_ = log.coverage_revocations_,
      .backpressure_waits_ = log.capacity_waits_,
      .fullsync_backpressure_waits_ = store.fullsync_publisher_capacity_waits_,
      .capacity_backpressured_ = log.capacity_backpressured_,
  };
}

}  // namespace lavik::storage
