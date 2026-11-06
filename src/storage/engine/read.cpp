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

#include "absl/crc/crc32c.h"
#include "impl.h"
#include "lavik/error_detail.h"
#include "lavik/random_sample.h"

namespace lavik::storage {
namespace {

class BatchReadAwaiter;

// One stable waiter per wave entry; the shard still has only one awaiting
// coroutine. A shared producer owns the DMA tag independently of this object.
struct BatchReadOperation final : ReadFlightWaiter {
  void Complete(Worker& worker, int result, unsigned flags) override;
  void ReceiveRead(Worker& worker, ReadResultBuffer&& buffer, int result,
                   unsigned flags) override;
  void Finish(Worker& worker, int result);
  absl::Status Submit();
  bool TryJoin();
  void Joined();

  ReadLatencyTrace* trace_ = nullptr;
  std::uint64_t io_start_ns_ = 0;
  BatchReadAwaiter* batch_ = nullptr;
  Worker* worker_ = nullptr;
  FixedFile file_{};
  FixedBuffer buffer_{};
  std::uint64_t offset_ = 0;
  bool registered_ = false;
  bool joined_ = false;
  bool completed_ = false;
  bool pending_in_batch_ = false;
  ReadFlightRegistry* flights_ = nullptr;
  ReadFlightKey key_;
  std::size_t hash_ = 0;
  ReadBufferLease* lease_ = nullptr;
  bool* owns_pin_ = nullptr;
  void* pin_context_ = nullptr;
  void (*release_pin_)(void*) = nullptr;
  ReadResultBuffer result_buffer_;
  absl::Status submit_status_ = absl::OkStatus();
  int result_ = 0;
};

class BatchReadAwaiter {
 public:
  explicit BatchReadAwaiter(std::span<BatchReadOperation*> operations)
      : operations_(operations) {}

  bool await_ready() const noexcept { return operations_.empty(); }

  bool await_suspend(std::coroutine_handle<> awaiting) {
    awaiting_ = awaiting;
    // Register the complete barrier before any submission can complete or
    // join. Pre-delivered followers and synchronous rejections retire exactly
    // once; no callback can enqueue the parent during this submission loop.
    for (auto* operation : operations_) {
      operation->batch_ = this;
      if (!operation->completed_) {
        operation->pending_in_batch_ = true;
        ++remaining_;
      }
    }
    submitting_ = true;
    for (auto* operation : operations_) {
      if (operation->completed_) continue;
      operation->submit_status_ = operation->Submit();
      if (!operation->submit_status_.ok()) {
        operation->completed_ = true;
        Complete(*operation, *operation->worker_);
      }
    }
    submitting_ = false;
    return remaining_ != 0;
  }

  void await_resume() const noexcept {}

  void Complete(BatchReadOperation& operation, Worker& worker) noexcept {
    assert(operation.pending_in_batch_ && remaining_ != 0);
    operation.pending_in_batch_ = false;
    if (--remaining_ == 0 && !submitting_) worker.Enqueue(awaiting_);
  }

 private:
  std::span<BatchReadOperation*> operations_;
  std::coroutine_handle<> awaiting_{};
  std::size_t remaining_ = 0;
  bool submitting_ = false;
};

void BatchReadOperation::Joined() {
  joined_ = true;
  release_pin_(pin_context_);
  lease_->Reset();
  if (trace_) io_start_ns_ = trace_->NowNanos();
}

bool BatchReadOperation::TryJoin() {
  if (!flights_ || !flights_->TryJoin(key_, hash_, *this)) return false;
  Joined();
  return true;
}

absl::Status BatchReadOperation::Submit() {
  if (flight_attached()) return absl::OkStatus();
  bycorf::IoCompletion* tag = this;
  if (flights_) {
    const auto admission =
        flights_->Start(key_, hash_, *lease_, *owns_pin_, *this);
    if (admission.outcome == ReadFlightRegistry::Outcome::kJoined) {
      Joined();
      return absl::OkStatus();
    }
    if (admission.outcome == ReadFlightRegistry::Outcome::kStarted)
      tag = admission.tag;
  }
  if (trace_) io_start_ns_ = trace_->NowNanos();
  auto status =
      registered_
          ? worker_->SubmitReadFixed(file_, buffer_, offset_, tag)
          : worker_->SubmitRead(file_, std::span(buffer_.data_, buffer_.size_),
                                offset_, tag);
  if (!status.ok()) {
    if (tag != this) flights_->Reject(tag, *lease_, *owns_pin_);
    if (trace_) trace_->RecordIo(io_start_ns_, trace_->NowNanos());
  }
  return status;
}

void BatchReadOperation::Finish(Worker& worker, int result) {
  result_ = result;
  completed_ = true;
  if (trace_) {
    if (joined_)
      trace_->RecordFlightWait(io_start_ns_, trace_->NowNanos());
    else
      trace_->RecordIo(io_start_ns_, trace_->NowNanos());
  }
  if (pending_in_batch_) batch_->Complete(*this, worker);
}

void BatchReadOperation::Complete(Worker& worker, int result, unsigned) {
  if (result >= 0) worker.RecordStorageReadCompletion(result);
  Finish(worker, result);
}

void BatchReadOperation::ReceiveRead(Worker& worker, ReadResultBuffer&& buffer,
                                     int result, unsigned) {
  result_buffer_ = std::move(buffer);
  Finish(worker, result);
}

absl::Status BatchReadError(int error) {
  std::string message = "read failed: " + std::string(std::strerror(error)) +
                        " (errno=" + std::to_string(error) + ")";
  switch (error) {
    case EAGAIN:
    case EBUSY:
      return absl::UnavailableError(std::move(message));
    case ECANCELED:
      return absl::CancelledError(std::move(message));
    case EINVAL:
      return absl::InvalidArgumentError(std::move(message));
    case EBADF:
      return absl::FailedPreconditionError(std::move(message));
    case ENOSPC:
    case EMFILE:
    case ENFILE:
    case ENOMEM:
      return absl::ResourceExhaustedError(std::move(message));
    case ENOENT:
      return absl::NotFoundError(std::move(message));
    default:
      return absl::UnknownError(std::move(message));
  }
}

}  // namespace

Task<absl::StatusOr<std::optional<std::string>>>
StorageEngine::Impl::RandomKeyLocal(std::uint8_t db_id) {
  assert(db_id < kLogicalDatabaseCount);
  WorkerStore& store = CurrentStore();
  bool revalidation_failed = false;
  auto materialize = [&](WorkerStore::PartitionStore& partition,
                         RecordIndex& index, RecordIndex::Entry* selected)
      -> Task<absl::StatusOr<std::optional<std::string>>> {
    if (selected->key_complete()) {
      co_return std::optional<std::string>(std::string(selected->key()));
    }
    const std::uintptr_t identity = reinterpret_cast<std::uintptr_t>(selected);
    const std::uint32_t hash =
        RecordIndex::AddressHash(selected->external_key_digest());
    const RecordLocation location = MaterializeIndexLocation(*selected);
    const std::uint32_t key_bytes = selected->logical_key_size();
    const std::uint64_t index_generation = store.index_generations_[db_id];
    const std::uint64_t db_epoch = DbEpoch(db_id);
    const std::uint64_t replication_epoch = partition.replication_epoch_;
    auto loaded = co_await LoadOutOfIndexKey(store, location, key_bytes);
    if (!loaded.ok()) co_return loaded.status();
    RecordIndex::Entry* current = index.FindAddress(identity, hash);
    if (store.index_generations_[db_id] != index_generation ||
        DbEpoch(db_id) != db_epoch ||
        partition.replication_epoch_ != replication_epoch ||
        current == nullptr ||
        !MaterializeIndexLocation(*current).SamePhysicalRecord(location) ||
        current->value_.kind() != RecordKind::kValue ||
        IsExpiredNow(*current)) {
      co_return std::optional<std::string>{};
    }
    co_return std::optional<std::string>(std::move(*loaded));
  };

  // The counters include keys whose deadline passed but whose tombstone has
  // not been appended yet. Rejection sampling preserves a uniform choice
  // among live keys in the usual case; after enough expired hits, fall back
  // to a bounded-memory scan so a database with many stale expirations cannot
  // incorrectly look empty.
  constexpr unsigned kRandomAttempts = 100;
  for (unsigned attempt = 0; attempt < kRandomAttempts; ++attempt) {
    const std::size_t population = store.live_key_count_[db_id];
    if (population == 0) {
      co_return std::optional<std::string>{};
    }
    std::uint64_t rank = RandomRank(population, RandomSampleGenerator());
    WorkerStore::PartitionStore* selected_partition = nullptr;
    for (WorkerStore::PartitionStore& partition : store.partitions_) {
      const std::size_t size = partition.live_key_count_[db_id];
      if (rank < size) {
        selected_partition = &partition;
        break;
      }
      rank -= size;
    }
    if (selected_partition == nullptr) {
      continue;
    }

    RecordIndex& index = selected_partition->indexes_[db_id];
    RecordIndex::Entry* selected =
        index.FairRandomEntry(RandomSampleGenerator()());
    if (selected == nullptr) {
      continue;
    }

    if (selected->value_.kind() != RecordKind::kValue) continue;

    const std::uint64_t now_ms = UnixTimeMillis();
    if (IsExpired(*selected, now_ms)) {
      QueueExpiredCandidate(store, selected_partition->id_, db_id, *selected,
                            selected->key());
      continue;
    }
    auto key = co_await materialize(*selected_partition, index, selected);
    if (!key.ok()) co_return key.status();
    if (key->has_value()) co_return std::move(*key);
    revalidation_failed = true;
  }

  // Scan the worker's indexes directly on the pathological fallback. Calling
  // ScanPartition once per empty logical partition would create thousands of
  // coroutine round trips when the only counted records are expired.
  for (WorkerStore::PartitionStore& partition : store.partitions_) {
    RecordIndex& index = partition.indexes_[db_id];
    RecordIndex::Entry* selected = nullptr;
    const std::uint64_t now_ms = UnixTimeMillis();
    index.ForEachWhile([&](RecordIndex::Entry& entry) {
      if (entry.value_.kind() != RecordKind::kValue) return true;
      if (IsExpired(entry, now_ms)) {
        QueueExpiredCandidate(store, partition.id_, db_id, entry, entry.key());
      } else {
        selected = &entry;
        return false;
      }
      return true;
    });
    if (selected == nullptr) continue;
    auto key = co_await materialize(partition, index, selected);
    if (!key.ok()) co_return key.status();
    if (key->has_value()) co_return std::move(*key);
    revalidation_failed = true;
  }
  if (revalidation_failed) {
    co_return absl::AbortedError("keyspace changed during RANDOMKEY");
  }
  co_return std::optional<std::string>{};
}

StorageEngine::RemoteReadAwaitable::RemoteReadAwaitable(
    Impl& engine, std::uint8_t db_id, std::string_view key, std::uint16_t slot,
    ValueType type, bool metadata_only, ReadLatencyTrace* trace)
    : key_(key),
      slot_(slot),
      db_id_(db_id),
      type_(type),
      metadata_only_(metadata_only),
      trace_(trace) {
  engine.PrepareRemoteRead(*this);
}

void StorageEngine::Impl::PrepareRemoteRead(RemoteReadAwaitable& result) {
  const auto db_id = result.db_id_;
  const auto key = result.key_;
  const auto slot = result.slot_;
  const unsigned owner = slot % worker_count_;
  auto& local = CurrentStore();
  if (owner == local.worker_->id() || db_id >= options_.database_count_ ||
      replica_loading_.load(std::memory_order_acquire) ||
      replica_recovery_fenced_.load(std::memory_order_acquire) ||
      request_serving_fenced_until_restart_.load(std::memory_order_acquire))
    return;
  auto& source = *stores_[owner];
  if (!source.remote_reads_ready_.load(std::memory_order_acquire)) return;
  const auto generation =
      source.remote_population_generation_.load(std::memory_order_acquire);
  if (generation & 1) return;
  const auto db_epoch = DbEpoch(db_id);
  const auto replication_epoch =
      remote_replication_epochs_[slot].load(std::memory_order_acquire);
  result.pin_.owner_ = owner;
  result.generation_ = generation;
  result.db_epoch_ = db_epoch;
  result.replication_epoch_ = replication_epoch;
  TraceSpanScope lookup(result.trace_ ? result.trace_->diagnostics_ : nullptr,
                        TraceStage::kLookup);
  auto& index = PartitionFor(source, slot).indexes_[db_id];
  index.PrefetchRead();
  const auto digest = ComputeDigest(key);
  auto reader = index.TryRead(digest);
  if (!reader) return;
  const auto* entry = std::as_const(index).Find(digest, key);
  if (result.trace_ && !result.trace_->diagnostics_)
    result.trace_->lookup_done_ns_ = result.trace_->NowNanos();
  if (!result.Current(*this)) return;
  if (entry && !entry->key_complete()) return;
  if (entry == nullptr || entry->value_.kind() == RecordKind::kTombstone) {
    // Ordinary absence needs only a code; a message allocates an Abseil
    // StatusRep.
    result.ready_.emplace(absl::NotFoundError(""));
    return;
  }
  // Cold representations and expiration retain the complete owner state
  // machine (including expiry scheduling and indirect-key collision checks).
  if (entry->value_.grouped() || entry->value_.external() ||
      entry->value_.in_memory() || entry->value_.value_type() != result.type_ ||
      IsExpiredNow(*entry))
    return;
  const RecordLocation location = MaterializeIndexLocation(*entry);
  if (result.metadata_only_) {
    result.ready_.emplace(
        CompactReadValue{.lease_ = {},
                         .logical_size_ = location.logical_size_,
                         .expire_at_ms_ = location.expire_at_ms_});
    return;
  }
  // Whole-value Stream/GEO callbacks rely on the owner's bounded inline
  // workspace allowance. Larger workspaces retain its admission path.
  if ((result.type_ == ValueType::kStream ||
       result.type_ == ValueType::kSortedSet) &&
      (location.total_disk_bytes() >= kCompactWorkspaceInputBytes ||
       location.logical_size_ > kCompactWorkspaceInputEntries))
    return;
  if (location.block_owner() != owner) return;
  // io_uring opens every configured file on every worker. Only SPDK builds
  // the controller-specific qpair owner directory.
  if (bycorf::SpdkStorageEnabled()) {
    const auto device = DeviceIndexForBlock(location.block_id());
    if (!std::binary_search(device_owners_[device].begin(),
                            device_owners_[device].end(),
                            static_cast<std::uint16_t>(local.worker_->id())))
      return;
  }
  auto& block = BlockStateAt(location.block_id());
  if (!block.TryRemotePin()) return;
  result.pin_.engine_ = this;
  result.pin_.owns_ = true;
  result.pin_.block_id_ = location.block_id();
  result.location_ = location;
  if (block.owner_.load(std::memory_order_acquire) != owner ||
      block.allocation_epoch_ != location.allocation_epoch() ||
      !result.Current(*this))
    return;
  // The only index-to-I/O handoff: no suspension or borrowed entry survives it.
  reader.Reset();

  LAVIK_FAULT_INJECT(
      if (LAVIK_FAULT_MATCHES("LAVIK_FAIL_VALUE_READ_KEY", key)) {
        result.ready_.emplace(
            absl::InternalError("injected value payload read failure"));
        return;
      });
  const auto [file_id, block_offset] = FileOffset(location.block_id());
  const auto absolute_offset = block_offset + location.record_offset();
  const auto mask = static_cast<std::uint64_t>(direct_io_alignment_ - 1);
  const auto aligned_offset = absolute_offset & ~mask;
  result.record_headroom_ = absolute_offset - aligned_offset;
  result.read_bytes_ = (result.record_headroom_ + location.total_disk_bytes() +
                        direct_io_alignment_ - 1) &
                       ~mask;

  ReadFlightRegistry* registry = nullptr;
  if (result.read_bytes_ <= local.buffers_.preferred_read_payload_bytes()) {
    registry = ReadFlights(local);
  }
  result.io_.emplace(result, *local.worker_, local.files_[file_id],
                     aligned_offset, registry);
  if (result.io_->TryJoin()) return;

  // Admission is only an optimization. If the local arena cannot fit the
  // record immediately, let the owner reader stream it or wait for a chunk.
  if (result.trace_) {
    result.trace_->buffer_acquire_start_ns_ = result.trace_->NowNanos();
  }
  auto acquired = local.buffers_.TryAcquireReadBuffer(result.read_bytes_);
  if (!acquired.ok()) {
    // The owner reader can stream a record through smaller arena ranges.
    // Leave this admission attempt empty so its caller takes that route.
    result.io_.reset();
    if (absl::IsResourceExhausted(acquired.status())) return;
    result.ready_.emplace(acquired.status());
    return;
  }
  result.lease_ = std::move(*acquired);
  if (result.trace_) {
    result.trace_->buffer_acquired_ns_ = result.trace_->NowNanos();
    result.trace_->RecordBuffer(result.trace_->buffer_acquire_start_ns_,
                                result.trace_->buffer_acquired_ns_);
  }
  result.io_->SetBuffer();
}

ReadFlightRegistry* StorageEngine::Impl::ReadFlights(WorkerStore& store) {
  if (!store.read_flights_attempted_) [[unlikely]] {
    store.read_flights_attempted_ = true;
    store.read_flights_ = ReadFlightRegistry::TryCreate(
        *store.worker_, store.buffers_, this,
        [](void* context, std::uint64_t block_id, unsigned owner,
           bool quiesced) noexcept {
          auto& engine = *static_cast<Impl*>(context);
          auto& block = engine.BlockStateAt(block_id);
          if (quiesced) {
            // Stores may already be gone after the all-worker barrier. The
            // directory survives; no maintenance wakeup is legal here.
            const auto old =
                block.pins_.fetch_sub(1, std::memory_order_release);
            assert((old & BlockState::kPinMask) != 0);
            (void)old;
          } else if (bycorf::ThisWorker().id_ == owner) {
            engine.UnpinBlock(*engine.stores_[owner], block, block_id);
          } else {
            engine.UnpinRemoteBlock(block, owner, block_id);
          }
        });
  }
  return store.read_flights_.get();
}

StorageEngine::RemoteReadAwaitable::TracedReadIo::TracedReadIo(
    RemoteReadAwaitable& read, bycorf::Worker& worker, bycorf::FixedFile file,
    std::uint64_t offset, ReadFlightRegistry* registry)
    : read_(read),
      registry_(registry),
      worker_(worker),
      file_(file),
      offset_(offset),
      trace_(read.trace_) {
  if (registry_) hash_ = absl::Hash<ReadFlightKey>{}(Key());
}

ReadFlightKey StorageEngine::RemoteReadAwaitable::TracedReadIo::Key() const {
  const auto& location = read_.location_;
  return {.engine = reinterpret_cast<std::uintptr_t>(read_.pin_.engine_),
          .block = location.block_id(),
          .allocation_epoch = location.allocation_epoch(),
          .mutation_sequence = location.mutation_sequence_,
          .aligned_offset = offset_,
          .read_bytes = read_.read_bytes_,
          .db_epoch = read_.db_epoch_,
          .replication_epoch = read_.replication_epoch_,
          .population_generation = read_.generation_,
          .file = file_.index_,
          .record_offset = location.record_offset(),
          .record_bytes = location.total_disk_bytes(),
          .block_owner = static_cast<std::uint16_t>(location.block_owner()),
          .db = read_.db_id_};
}

void StorageEngine::RemoteReadAwaitable::TracedReadIo::Joined() {
  // The matching producer now owns the physical pin. This request retains its
  // own captured logical context and must validate it again after completion.
  read_.pin_.Reset();
  read_.lease_.Reset();
  if (trace_) [[unlikely]]
    trace_->flight_wait_start_ns_ = trace_->NowNanos();
}
bool StorageEngine::RemoteReadAwaitable::TracedReadIo::TryJoin() {
  if (!registry_ || !registry_->TryJoin(Key(), hash_, *this)) return false;
  Joined();
  return true;
}
void StorageEngine::RemoteReadAwaitable::TracedReadIo::SetBuffer() {
  buffer_ = read_.lease_.io_buffer();
  assert(read_.read_bytes_ <= buffer_.size_);
  buffer_.size_ = read_.read_bytes_;
  fixed_ = read_.lease_.registered();
}

bool StorageEngine::RemoteReadAwaitable::TracedReadIo::await_suspend(
    std::coroutine_handle<> awaiting) {
  if (flight_attached()) return Suspend(awaiting, absl::OkStatus());
  bycorf::IoCompletion* tag = this;
  if (registry_) {
    const auto admission =
        registry_->Start(Key(), hash_, read_.lease_, read_.pin_.owns_, *this);
    if (admission.outcome == ReadFlightRegistry::Outcome::kJoined) {
      Joined();
      return Suspend(awaiting, absl::OkStatus());
    }
    if (admission.outcome == ReadFlightRegistry::Outcome::kStarted)
      tag = admission.tag;
  }
  if (trace_) [[unlikely]] {
    trace_->disk_read_ = true;
    trace_->io_submit_ns_ = trace_->NowNanos();
  }
  auto status = fixed_ ? worker_.SubmitReadFixed(file_, buffer_, offset_, tag)
                       : worker_.SubmitRead(
                             file_, std::span(buffer_.data_, buffer_.size_),
                             offset_, tag);
  if (!status.ok()) [[unlikely]] {
    if (tag != this) registry_->Reject(tag, read_.lease_, read_.pin_.owns_);
    FinishTrace();
  }
  return Suspend(awaiting, std::move(status));
}

void StorageEngine::RemoteReadAwaitable::TracedReadIo::FinishTrace() {
  if (!trace_) [[likely]]
    return;
  trace_->io_complete_ns_ = trace_->NowNanos();
  if (trace_->flight_wait_start_ns_) {
    trace_->RecordFlightWait(trace_->flight_wait_start_ns_,
                             trace_->io_complete_ns_);
  } else {
    trace_->RecordIo(trace_->io_submit_ns_, trace_->io_complete_ns_);
  }
}
void StorageEngine::RemoteReadAwaitable::TracedReadIo::Complete(
    bycorf::Worker& worker, int result, unsigned flags) {
  if (result >= 0) worker.RecordStorageReadCompletion(result);
  FinishTrace();
  OneShotIoAwaitable::Complete(worker, result, flags);
}
void StorageEngine::RemoteReadAwaitable::TracedReadIo::ReceiveRead(
    bycorf::Worker& worker, ReadResultBuffer&& buffer, int result,
    unsigned flags) {
  // This slot holds only ownership until await_resume independently validates
  // the record. No request inherits another request's success conclusion.
  read_.ready_.emplace(CompactReadValue{.lease_ = std::move(buffer)});
  FinishTrace();
  OneShotIoAwaitable::Complete(worker, result, flags);
}

absl::StatusOr<std::size_t>
StorageEngine::RemoteReadAwaitable::TracedReadIo::await_resume() {
  auto result = Resume("compact record read failed");
  if (!result.ok()) return result.status();
  return static_cast<std::size_t>(*result);
}

void StorageEngine::RemoteReadAwaitable::Pin::Reset() {
  if (std::exchange(owns_, false))
    engine_->UnpinRemoteBlock(engine_->BlockStateAt(block_id_), owner_,
                              block_id_);
}
StorageEngine::RemoteReadAwaitable::Pin::~Pin() { Reset(); }

bool StorageEngine::RemoteReadAwaitable::Current(
    const Impl& engine) const noexcept {
  return engine.stores_[pin_.owner_]->remote_population_generation_.load(
             std::memory_order_acquire) == generation_ &&
         engine.DbEpoch(db_id_) == db_epoch_ &&
         engine.remote_replication_epochs_[slot_].load(
             std::memory_order_acquire) == replication_epoch_ &&
         !engine.replica_loading_.load(std::memory_order_acquire) &&
         !engine.replica_recovery_fenced_.load(std::memory_order_acquire) &&
         !engine.request_serving_fenced_until_restart_.load(
             std::memory_order_acquire);
}

StorageEngine::RemoteReadAwaitable::Result
StorageEngine::RemoteReadAwaitable::await_resume() {
  if (!io_) {
    if (pin_.engine_ != nullptr && !Current(*pin_.engine_)) return std::nullopt;
    return std::move(ready_);
  }
  auto read = io_->await_resume();
  auto& engine = *pin_.engine_;
  if (!read.ok() || *read != read_bytes_) {
    if (!Current(engine) || (!read.ok() && absl::IsAborted(read.status())))
      return std::nullopt;
    return read.ok() ? absl::InternalError("short compact record read")
                     : read.status();
  }
  TraceSpanScope decode_scope(trace_ ? trace_->diagnostics_ : nullptr,
                              TraceStage::kDecode);
  ReadResultBuffer buffer = io_->flight_completed()
                                ? std::move(ready_->value().lease_)
                                : ReadResultBuffer(std::move(lease_));
  auto loaded =
      engine.DecodeRemoteValue(std::move(buffer), db_id_, key_, location_,
                               replication_epoch_, record_headroom_, db_epoch_);
  if (trace_) trace_->decode_done_ns_ = trace_->NowNanos();
  if (!Current(engine) || (!loaded.ok() && absl::IsAborted(loaded.status())))
    return std::nullopt;
  if (!loaded.ok()) return loaded.status();
  return loaded;
}

// Keep optimistic-read admission out of the already large command dispatcher.
// LTO otherwise duplicates this wrapper and expands unrelated command cases,
// increasing their instruction-cache footprint even though only GET uses it.
[[gnu::noinline]] Task<absl::StatusOr<DiskValue>> StorageEngine::Impl::Get(
    std::uint8_t db_id, std::string_view key, ReadLatencyTrace* trace,
    std::optional<std::uint16_t> routed_partition_id) {
  assert(db_id < kLogicalDatabaseCount);
  const Digest digest = ComputeDigest(key);
  const bool optimistic = tx::CurrentTxShard().CanReadOptimistically(db_id);
  return GetWithLockState(db_id, key, digest, trace, routed_partition_id,
                          !optimistic, optimistic);
}

Task<absl::StatusOr<DiskValue>> StorageEngine::Impl::GetLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    ReadLatencyTrace* trace, std::optional<std::uint16_t> routed_partition_id) {
  assert(db_id < kLogicalDatabaseCount);
  return GetWithLockState(db_id, key, digest, trace, routed_partition_id,
                          false);
}

Task<absl::StatusOr<DiskValue>> StorageEngine::Impl::GetWithLockState(
    std::uint8_t db_id, std::string_view key, Digest digest,
    ReadLatencyTrace* trace, std::optional<std::uint16_t> routed_partition_id,
    bool acquire_key_lock, bool optimistic_read) {
  assert(db_id < kLogicalDatabaseCount);
  assert(!routed_partition_id.has_value() ||
         *routed_partition_id == RedisSlot(key));
  tx::TxShard::Guard key_lock;
  if (acquire_key_lock) {
    if (trace && !trace->diagnostics_)
      trace->key_lock_start_ns_ = trace->NowNanos();
    TraceSpanScope key_wait(trace ? trace->diagnostics_ : nullptr,
                            TraceStage::kKeyLock);
    key_lock = co_await tx::CurrentTxShard().AcquireKey(
        db_id, tx::FingerprintOf(digest), tx::LockMode::kShared,
        lavik::storage::RedisSlot(key));
  }
  if (acquire_key_lock && trace && !trace->diagnostics_)
    trace->key_lock_acquired_ns_ = trace->NowNanos();
  WorkerStore& store = CurrentStore();
  auto& partition = routed_partition_id.has_value()
                        ? PartitionFor(store, *routed_partition_id)
                        : PartitionForKey(store, key);
  while (true) {
    TraceSpanScope lookup_scope(trace ? trace->diagnostics_ : nullptr,
                                TraceStage::kLookup);
    auto& index = partition.indexes_[db_id];
    auto* found = index.Find(digest, key);
    if (found != nullptr && !found->key_complete()) [[unlikely]] {
      if (optimistic_read) {
        // Verifying an out-of-index key suspends before we know this is the
        // requested key, so there is no lock-free linearization point yet.
        lookup_scope.End();
        TraceSpanScope retry_wait(trace ? trace->diagnostics_ : nullptr,
                                  TraceStage::kKeyLock);
        key_lock = co_await tx::CurrentTxShard().AcquireKey(
            db_id, tx::FingerprintOf(digest), tx::LockMode::kShared,
            lavik::storage::RedisSlot(key));
        optimistic_read = false;
        continue;
      }
      auto resolved = co_await FindVerifiedEntry(store, index, digest, key);
      if (!resolved.ok()) {
        co_return resolved.status();
      }
      found = *resolved;
    }
    if (found == nullptr || found->value_.kind() == RecordKind::kTombstone) {
      if (trace != nullptr && !trace->diagnostics_) {
        trace->lookup_done_ns_ = trace->NowNanos();
      }
      // GET maps absence to nil by code, so avoid allocating a diagnostic
      // message.
      co_return absl::NotFoundError("");
    }
    const auto readable = ValidateGroupedRead(partition, db_id, key, found);
    if (!readable.ok()) co_return readable;
    if (IsExpiredNow(*found)) {
      QueueExpiredCandidate(store, partition.id_, db_id, *found, key);
      if (trace != nullptr && !trace->diagnostics_) {
        trace->lookup_done_ns_ = trace->NowNanos();
      }
      co_return absl::NotFoundError("");
    }
    if (found->value_.value_type() != ValueType::kString) {
      co_return WrongTypeError();
    }
    if (trace != nullptr && !trace->diagnostics_) {
      trace->lookup_done_ns_ = trace->NowNanos();
    }

    lookup_scope.End();
    auto loaded = co_await LoadValue(
        store, partition, db_id, key, digest, MaterializeIndexLocation(*found),
        ExtentsFor(store, found), nullptr, /*coalesce=*/true, trace);
    if (optimistic_read && !loaded.ok() &&
        (loaded.status().code() == absl::StatusCode::kNotFound ||
         loaded.status().code() == absl::StatusCode::kAborted)) {
      // The key existed at the optimistic lookup, but a concurrent writer may
      // retire its physical record before the I/O path pins it. Do not turn
      // that race into a spurious nil; take the original lock path and reread.
      TraceSpanScope retry_wait(trace ? trace->diagnostics_ : nullptr,
                                TraceStage::kKeyLock);
      key_lock = co_await tx::CurrentTxShard().AcquireKey(
          db_id, tx::FingerprintOf(digest), tx::LockMode::kShared,
          lavik::storage::RedisSlot(key));
      optimistic_read = false;
      continue;
    }
    if (!loaded.ok()) {
      co_return loaded.status();
    }

    co_return EncodeDiskValue(std::move(*loaded));
  }
}

Task<std::vector<BatchGetValue>> StorageEngine::Impl::BatchGetLocked(
    std::uint8_t db_id, std::span<const BatchGetRequest> requests,
    OnlineLatencyTrace* diagnostics) {
  assert(db_id < kLogicalDatabaseCount);
  auto detail = MakeReadTrace(diagnostics);
  TraceSpanScope lookup_scope(diagnostics, TraceStage::kLookup);
  WorkerStore& store = CurrentStore();

  std::vector<BatchGetValue> results;
  results.reserve(requests.size());
  for (std::size_t i = 0; i < requests.size(); ++i) {
    results.emplace_back(std::optional<std::string>{});
  }

  struct Candidate {
    std::size_t result_index_ = 0;
    WorkerStore::PartitionStore* partition_ = nullptr;
    RecordLocation location_{};
    std::uint64_t replication_epoch_ = 0;
    std::uint64_t db_epoch_ = 0;
    std::uint64_t generation_ = 0;
  };
  std::vector<Candidate> candidates;
  std::vector<std::size_t> fallbacks;
  candidates.reserve(requests.size());
  fallbacks.reserve(requests.size());

  const std::uint64_t now_ms = UnixTimeMillis();
  for (std::size_t i = 0; i < requests.size(); ++i) {
    const BatchGetRequest& request = requests[i];
    auto& partition = PartitionForKey(store, request.key_);
    auto& index = partition.indexes_[db_id];
    auto* found = index.Find(request.digest_, request.key_);
    if (found != nullptr && !found->key_complete()) [[unlikely]] {
      // Key verification can itself require multiple extents or a remote block
      // owner. Keep it on the complete existing state machine rather than
      // duplicating that cold path in the flat batch reader.
      fallbacks.push_back(i);
      continue;
    }
    if (found == nullptr || found->value_.kind() == RecordKind::kTombstone) {
      continue;
    }
    if (found->value_.grouped()) {
      fallbacks.push_back(i);
      continue;
    }
    if (IsExpired(*found, now_ms)) {
      QueueExpiredCandidate(store, partition.id_, db_id, *found, request.key_);
      continue;
    }
    // Redis MGET returns nil for a non-string key rather than failing the whole
    // command.
    if (found->value_.value_type() != ValueType::kString) {
      continue;
    }

    const RecordLocation location = MaterializeIndexLocation(*found);
    if (location.grouped() || location.external() ||
        location.block_owner() != store.worker_->id() || location.in_memory()) {
      fallbacks.push_back(i);
      continue;
    }
    candidates.push_back(Candidate{
        .result_index_ = i,
        .partition_ = &partition,
        .location_ = location,
        .replication_epoch_ = partition.replication_epoch_,
        .db_epoch_ = EffectiveRecordDbEpoch(partition, db_id),
        .generation_ = store.index_generations_[db_id],
    });
  }

  lookup_scope.End();
  struct PendingRead {
    PendingRead(WorkerStore* store, BlockState* state, Candidate candidate,
                std::size_t record_headroom, std::size_t read_bytes)
        : store_(store),
          state_(state),
          candidate_(candidate),
          record_headroom_(record_headroom),
          read_bytes_(read_bytes) {
      operation_.lease_ = &lease_;
      operation_.owns_pin_ = &owns_pin_;
      operation_.pin_context_ = this;
      operation_.release_pin_ = [](void* context) {
        static_cast<PendingRead*>(context)->Unpin();
      };
    }

    PendingRead(const PendingRead&) = delete;
    PendingRead& operator=(const PendingRead&) = delete;

    ~PendingRead() { Unpin(); }
    void Unpin() {
      if (std::exchange(owns_pin_, false))
        store_->engine_->UnpinBlock(*store_, *state_,
                                    candidate_.location_.block_id());
    }

    WorkerStore* store_ = nullptr;
    BlockState* state_ = nullptr;
    Candidate candidate_{};
    ReadBufferLease lease_;
    bool owns_pin_ = false;
    std::size_t record_headroom_ = 0;
    std::size_t read_bytes_ = 0;
    BatchReadOperation operation_;
  };

  std::size_t next = 0;
  while (next < candidates.size()) {
    std::vector<std::unique_ptr<PendingRead>> wave;
    std::vector<BatchReadOperation*> operations;
    // Joined requests do not consume DMA ranges, so arena exhaustion no longer
    // bounds their metadata. Limit entries by the arena's preferred-buffer
    // count, including when every candidate joins the same producer.
    const auto wave_limit =
        std::min(candidates.size() - next,
                 std::max(std::size_t{1}, store.buffers_.read_buffer_count()));
    wave.reserve(wave_limit);
    operations.reserve(wave_limit);

    while (next < candidates.size() && wave.size() < wave_limit) {
      const Candidate candidate = candidates[next];
      const RecordLocation& location = candidate.location_;
      const auto [file_id, block_offset] = FileOffset(location.block_id());
      const std::uint64_t absolute_offset =
          block_offset + location.record_offset();
      const std::uint64_t direct_io_mask =
          static_cast<std::uint64_t>(direct_io_alignment_ - 1);
      const std::uint64_t aligned_offset = absolute_offset & ~direct_io_mask;
      const std::size_t record_headroom =
          static_cast<std::size_t>(absolute_offset - aligned_offset);
      const std::size_t record_span =
          record_headroom + location.total_disk_bytes();
      const std::size_t read_bytes =
          (record_span + direct_io_alignment_ - 1) & ~direct_io_mask;
      auto* state = FindBlockState(store, location.block_id());
      if (!state || !state->allocated_ || state->freeing_ ||
          state->allocation_epoch_ != location.allocation_epoch() ||
          (location.in_memory() && state->in_memory_)) {
        fallbacks.push_back(candidate.result_index_);
        ++next;
        continue;
      }
      auto pending = std::make_unique<PendingRead>(&store, state, candidate,
                                                   record_headroom, read_bytes);
      auto& operation = pending->operation_;
      operation.trace_ = detail.get();
      if (detail) detail->disk_read_ = true;
      operation.worker_ = store.worker_;
      operation.file_ = store.files_[file_id];
      operation.offset_ = aligned_offset;
      if (read_bytes <= store.buffers_.read_capacity_bytes()) {
        operation.flights_ = ReadFlights(store);
        if (operation.flights_) {
          operation.key_ = {
              .engine = reinterpret_cast<std::uintptr_t>(this),
              .block = location.block_id(),
              .allocation_epoch = location.allocation_epoch(),
              .mutation_sequence = location.mutation_sequence_,
              .aligned_offset = aligned_offset,
              .read_bytes = read_bytes,
              .db_epoch = candidate.db_epoch_,
              .replication_epoch = candidate.replication_epoch_,
              .population_generation = candidate.generation_,
              .file = store.files_[file_id].index_,
              .record_offset = location.record_offset(),
              .record_bytes = location.total_disk_bytes(),
              .block_owner = static_cast<std::uint16_t>(location.block_owner()),
              .db = db_id};
          operation.hash_ = absl::Hash<ReadFlightKey>{}(operation.key_);
        }
      }
      if (operation.TryJoin()) {
        operations.push_back(&operation);
        wave.push_back(std::move(pending));
        ++next;
        continue;
      }
      // Never wait while this wave owns unsubmitted ranges. Submit/drain it
      // first; a record that cannot fit an empty wave uses the chunked reader.
      TraceSpanScope buffer(diagnostics, TraceStage::kReadBuffer);
      auto acquired = store.buffers_.TryAcquireReadBuffer(read_bytes);
      buffer.End();
      if (!acquired.ok()) {
        if (absl::IsResourceExhausted(acquired.status())) {
          if (!wave.empty()) break;
          fallbacks.push_back(candidate.result_index_);
        } else {
          results[candidate.result_index_] = acquired.status();
        }
        ++next;
        continue;
      }
      pending->lease_ = std::move(*acquired);
      state->PinLocal();
      pending->owns_pin_ = true;
      FixedBuffer io = pending->lease_.io_buffer();
      if (read_bytes > io.size_) {
        results[candidate.result_index_] = absl::OutOfRangeError(
            "record exceeds registered read buffer capacity");
        ++next;
        continue;
      }
      pending->operation_.buffer_ = io;
      pending->operation_.buffer_.size_ = read_bytes;
      pending->operation_.offset_ = aligned_offset;
      pending->operation_.registered_ = pending->lease_.registered();
      operations.push_back(&pending->operation_);
      wave.push_back(std::move(pending));
      ++next;
    }

    if (wave.empty()) continue;
    co_await BatchReadAwaiter(operations);

    std::vector<std::size_t> retry;
    for (const std::unique_ptr<PendingRead>& pending : wave) {
      const std::size_t index = pending->candidate_.result_index_;
      const BatchGetRequest& request = requests[index];
      BatchReadOperation& operation = pending->operation_;
      if (!operation.submit_status_.ok()) {
        results[index] = operation.submit_status_;
        continue;
      }
      if (operation.result_ < 0) {
        results[index] = BatchReadError(-operation.result_);
        continue;
      }
      if (static_cast<std::size_t>(operation.result_) != pending->read_bytes_) {
        results[index] = absl::InternalError("short compact record read");
        continue;
      }

      if (pending->candidate_.partition_->replication_epoch_ !=
              pending->candidate_.replication_epoch_ ||
          EffectiveRecordDbEpoch(*pending->candidate_.partition_, db_id) !=
              pending->candidate_.db_epoch_ ||
          store.index_generations_[db_id] != pending->candidate_.generation_)
          [[unlikely]] {
        results[index] = std::optional<std::string>{};
        continue;
      }

      TraceSpanScope decode(diagnostics, TraceStage::kDecode);
      const RecordLocation& location = pending->candidate_.location_;
      const auto* io_data = operation.result_buffer_.valid()
                                ? operation.result_buffer_.io_data()
                                : pending->lease_.io_buffer().data_;
      const std::byte* record_data = io_data + pending->record_headroom_;
      const std::span<const std::byte> record_bytes(
          record_data, location.total_disk_bytes());
      RecordHeader record{};
      std::string_view disk_key;
      if (!DecodeRecordHeader(record_bytes, &record, &disk_key) ||
          record.db_id_ != db_id ||
          (!record.key_indirect_ && disk_key != request.key_) ||
          record.kind_ != RecordKind::kValue ||
          record.db_epoch_ != pending->candidate_.db_epoch_ ||
          record.mutation_sequence_ != location.mutation_sequence_ ||
          record.replication_epoch_ != pending->candidate_.replication_epoch_ ||
          record.allocation_epoch_ != location.allocation_epoch() ||
          record.expire_at_ms_ != location.expire_at_ms_ ||
          record.value_type_ != location.value_type() ||
          record.external_ != location.external() ||
          record.key_indirect_ != location.key_indirect() ||
          record.logical_size_ != location.logical_size_ ||
          record.total_disk_bytes_ != location.total_disk_bytes()) {
        retry.push_back(index);
        continue;
      }
      const std::byte* payload_data = record_data + record.header_bytes_;
      if (record.key_indirect_ && record.key_bytes_ != request.key_.size()) {
        retry.push_back(index);
        continue;
      }
      const std::size_t value_bytes = record.payload_bytes_;
      if (value_bytes != record.logical_size_) {
        results[index] =
            absl::InternalError("inline string length does not match metadata");
        continue;
      }
      if (!ValidateRecordPayload(
              record, std::span<const std::byte>(payload_data,
                                                 record.payload_bytes_))) {
        results[index] = absl::InternalError("record value checksum mismatch");
        continue;
      }
      const char* value = reinterpret_cast<const char*>(payload_data);
      results[index] = std::optional<std::string>(
          std::in_place, value, static_cast<std::size_t>(value_bytes));
    }
    // Releasing the wave returns all fixed buffers before any relocation retry
    // can suspend and lets the next wave use the bounded pool.
    wave.clear();
    fallbacks.insert(fallbacks.end(), retry.begin(), retry.end());
  }

  for (const std::size_t index : fallbacks) {
    const BatchGetRequest& request = requests[index];
    auto value =
        co_await GetLocked(db_id, request.key_, request.digest_, detail.get());
    if (value.ok()) {
      const std::span<const std::byte> bytes = value->value_bytes();
      results[index] = std::optional<std::string>(
          std::in_place, reinterpret_cast<const char*>(bytes.data()),
          bytes.size());
    } else if (value.status().code() == absl::StatusCode::kNotFound ||
               value.status().message().starts_with("WRONGTYPE ")) {
      results[index] = std::optional<std::string>{};
    } else {
      results[index] = value.status();
    }
  }

  co_return results;
}

Task<absl::StatusOr<std::uint64_t>> StorageEngine::Impl::StringLength(
    std::uint8_t db_id, std::string_view key, OnlineLatencyTrace* diagnostics) {
  assert(db_id < kLogicalDatabaseCount);
  const Digest digest = ComputeDigest(key);
  TraceSpanScope key_wait(diagnostics, TraceStage::kKeyLock);
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest), tx::LockMode::kShared,
      lavik::storage::RedisSlot(key));
  key_wait.End();
  co_return co_await StringLengthLocked(db_id, key, digest, diagnostics);
}

Task<absl::StatusOr<std::uint64_t>> StorageEngine::Impl::StringLengthLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    OnlineLatencyTrace* diagnostics) {
  TraceSpanScope lookup(diagnostics, TraceStage::kLookup);
  assert(db_id < kLogicalDatabaseCount);
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionForKey(store, key);
  auto& index = partition.indexes_[db_id];
  auto* found = index.Find(digest, key);
  if (found != nullptr && !found->key_complete()) [[unlikely]] {
    auto resolved = co_await FindVerifiedEntry(store, index, digest, key);
    if (!resolved.ok()) {
      co_return resolved.status();
    }
    found = *resolved;
  }
  const auto readable = ValidateGroupedRead(partition, db_id, key, found);
  if (!readable.ok()) co_return readable;
  const bool expired = found != nullptr && IsExpiredNow(*found);
  if (found == nullptr || found->value_.kind() != RecordKind::kValue ||
      expired) {
    if (expired) {
      QueueExpiredCandidate(store, partition.id_, db_id, *found, key);
    }
    co_return absl::Status(absl::StatusCode::kNotFound, "key not found");
  }
  if (found->value_.value_type() != ValueType::kString) {
    co_return WrongTypeError();
  }
  co_return found->value_.logical_size();
}

Task<absl::StatusOr<ExpirationInfo>> StorageEngine::ReadKeyMetadata(
    std::uint8_t db_id, std::string_view key, OnlineLatencyTrace* diagnostics) {
  return impl_->ReadKeyMetadata(db_id, key, diagnostics);
}

Task<absl::StatusOr<ExpirationInfo>> StorageEngine::ReadKeyMetadataLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    OnlineLatencyTrace* diagnostics) {
  return impl_->ReadKeyMetadataLocked(db_id, key, digest, diagnostics);
}

Task<absl::StatusOr<ExpirationInfo>> StorageEngine::Impl::ReadKeyMetadata(
    std::uint8_t db_id, std::string_view key, OnlineLatencyTrace* diagnostics) {
  if (db_id >= options_.database_count_)
    co_return absl::InvalidArgumentError("invalid logical database");
  const auto digest = ComputeDigest(key);
  TraceSpanScope key_wait(diagnostics, TraceStage::kKeyLock);
  auto hold = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest), tx::LockMode::kShared,
      lavik::storage::RedisSlot(key));
  key_wait.End();
  co_return co_await ReadKeyMetadataLocked(db_id, key, digest, diagnostics);
}

Task<absl::StatusOr<ExpirationInfo>> StorageEngine::Impl::ReadKeyMetadataLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    OnlineLatencyTrace* diagnostics) {
  if (db_id >= options_.database_count_ || digest != ComputeDigest(key))
    co_return absl::InvalidArgumentError("invalid metadata key identity");
  auto& store = CurrentStore();
  auto& partition = PartitionForKey(store, key);
  TraceSpanScope lookup(diagnostics, TraceStage::kLookup);
  auto& index = partition.indexes_[db_id];
  auto* found = index.Find(digest, key);
  if (found != nullptr && !found->key_complete()) [[unlikely]] {
    auto resolved = co_await FindVerifiedEntry(store, index, digest, key);
    if (!resolved.ok()) co_return resolved.status();
    found = *resolved;
  }
  if (found == nullptr || found->value_.kind() != RecordKind::kValue)
    co_return ExpirationInfo{};
  if (found->value_.grouped()) {
    // Check before applying the tentative root's TTL: a failed replacement
    // cannot disguise itself as an expired/missing predecessor either.
    auto readable = partition.grouped_objects_[db_id].Lookup(
        key, GroupedObjectVersion{
                 .root_ = MaterializeIndexLocation(*found),
                 .db_epoch_ = EffectiveRecordDbEpoch(partition, db_id),
                 .replication_epoch_ = partition.replication_epoch_,
                 .index_generation_ = partition.grouped_generations_[db_id]});
    if (!readable.ok()) co_return readable.status();
  }
  if (IsExpiredNow(*found)) {
    QueueExpiredCandidate(store, partition.id_, db_id, *found, key);
    co_return ExpirationInfo{};
  }
  co_return ExpirationInfo{.exists_ = true,
                           .expire_at_ms_ = ExpireAt(*found),
                           .value_type_ = found->value_.value_type()};
}

Task<ExpirationInfo> StorageEngine::Impl::GetExpiration(std::uint8_t db_id,
                                                        std::string_view key) {
  assert(db_id < kLogicalDatabaseCount);
  const Digest digest = ComputeDigest(key);
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest), tx::LockMode::kShared,
      lavik::storage::RedisSlot(key));
  co_return co_await GetExpirationLocked(db_id, key, digest);
}

Task<ExpirationInfo> StorageEngine::Impl::GetExpirationLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest) {
  assert(db_id < kLogicalDatabaseCount);
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionForKey(store, key);
  auto& index = partition.indexes_[db_id];
  auto* found = index.Find(digest, key);
  if (found != nullptr && !found->key_complete()) [[unlikely]] {
    auto resolved = co_await FindVerifiedEntry(store, index, digest, key);
    if (!resolved.ok()) {
      co_return ExpirationInfo{};
    }
    found = *resolved;
  }
  if (found == nullptr || found->value_.kind() != RecordKind::kValue) {
    co_return ExpirationInfo{};
  }
  if (IsExpiredNow(*found)) {
    QueueExpiredCandidate(store, partition.id_, db_id, *found, key);
    co_return ExpirationInfo{};
  }
  co_return ExpirationInfo{
      .exists_ = true,
      .expire_at_ms_ = ExpireAt(*found),
      .value_type_ = found->value_.value_type(),
  };
}

Task<absl::StatusOr<RawValue>> StorageEngine::Impl::ReadRawValueLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    OnlineLatencyTrace* diagnostics) {
  assert(db_id < kLogicalDatabaseCount);
  auto detail = MakeReadTrace(diagnostics);
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionForKey(store, key);
  TraceSpanScope store_wait(diagnostics, TraceStage::kStoreLock);
  co_await store.store_state_mutex_.Lock();
  store_wait.End();
  TraceSpanScope lookup_scope(diagnostics, TraceStage::kLookup);
  UnlockGuard unlock(&store.store_state_mutex_, store.worker_);

  auto& index = partition.indexes_[db_id];
  auto* found = index.Find(digest, key);
  if (found != nullptr && !found->key_complete()) [[unlikely]] {
    auto resolved = co_await FindVerifiedEntry(store, index, digest, key);
    if (!resolved.ok()) co_return resolved.status();
    found = *resolved;
  }
  if (found != nullptr && found->value_.grouped() && IsExpiredNow(*found)) {
    auto metadata = co_await ReadKeyMetadataLocked(db_id, key, digest);
    if (!metadata.ok()) co_return metadata.status();
  }
  if (found == nullptr || found->value_.kind() != RecordKind::kValue ||
      IsExpiredNow(*found)) {
    if (found != nullptr && found->value_.kind() == RecordKind::kValue) {
      QueueExpiredCandidate(store, partition.id_, db_id, *found, key);
    }
    co_return absl::NotFoundError("key not found");
  }

  const RecordLocation location = MaterializeIndexLocation(*found);
  const ExtentManifest extents = ExtentsFor(store, found);
  const std::uint64_t index_generation = store.index_generations_[db_id];
  const std::uint64_t db_epoch = DbEpoch(db_id);
  const std::uint64_t replication_epoch = partition.replication_epoch_;
  unlock.Unlock();
  lookup_scope.End();

  auto loaded =
      co_await LoadValue(store, partition, db_id, key, digest, location,
                         extents, nullptr, /*coalesce=*/false, detail.get());
  if (!loaded.ok()) co_return loaded.status();
  if (store.index_generations_[db_id] != index_generation ||
      DbEpoch(db_id) != db_epoch ||
      partition.replication_epoch_ != replication_epoch) {
    co_return absl::NotFoundError("key not found");
  }
  const auto bytes = loaded->value();
  co_return RawValue{
      .encoded_ = std::string(reinterpret_cast<const char*>(bytes.data()),
                              bytes.size()),
      .logical_size_ = location.logical_size_,
      .expire_at_ms_ = location.expire_at_ms_,
      .value_type_ = location.value_type(),
  };
}

Task<absl::StatusOr<RawValue>> StorageEngine::Impl::ReadRawValue(
    std::uint8_t db_id, std::string_view key) {
  assert(db_id < kLogicalDatabaseCount);
  const Digest digest = ComputeDigest(key);
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest), tx::LockMode::kShared,
      lavik::storage::RedisSlot(key));
  co_return co_await ReadRawValueLocked(db_id, key, digest);
}

Task<bool> StorageEngine::Impl::KeyLive(std::uint8_t db_id,
                                        std::string_view key,
                                        const Digest& digest) {
  assert(db_id < kLogicalDatabaseCount);
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionForKey(store, key);
  auto& index = partition.indexes_[db_id];
  auto* found = index.Find(digest, key);
  if (found != nullptr && !found->key_complete()) [[unlikely]] {
    auto resolved = co_await FindVerifiedEntry(store, index, digest, key);
    if (!resolved.ok()) {
      co_return false;
    }
    found = *resolved;
  }
  co_return found != nullptr && found->value_.kind() == RecordKind::kValue &&
      !IsExpiredNow(*found);
}

Task<bool> StorageEngine::Impl::Exists(std::uint8_t db_id,
                                       std::string_view key) {
  assert(db_id < kLogicalDatabaseCount);
  const Digest digest = ComputeDigest(key);
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest), tx::LockMode::kShared,
      lavik::storage::RedisSlot(key));
  co_return co_await ExistsLocked(db_id, key, digest);
}

Task<bool> StorageEngine::Impl::ExistsLocked(std::uint8_t db_id,
                                             std::string_view key,
                                             const Digest& digest) {
  assert(db_id < kLogicalDatabaseCount);
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionForKey(store, key);
  auto& index = partition.indexes_[db_id];
  auto* found = index.Find(digest, key);
  if (found != nullptr && !found->key_complete()) [[unlikely]] {
    auto resolved = co_await FindVerifiedEntry(store, index, digest, key);
    if (!resolved.ok()) {
      co_return false;
    }
    found = *resolved;
  }
  if (found == nullptr || found->value_.kind() != RecordKind::kValue) {
    co_return false;
  }
  if (IsExpiredNow(*found)) {
    QueueExpiredCandidate(store, partition.id_, db_id, *found, key);
    co_return false;
  }
  co_return true;
}

std::size_t StorageEngine::Impl::DirectGetValueLimit() const noexcept {
  // Framing margins are already outside the I/O capacity. Do not subtract
  // them again when selecting the ordinary-slot direct reply path.
  return options_.buffers_.read_payload_bytes();
}

absl::Status StorageEngine::Impl::LoadedValue::MakeOwned(
    RegisteredBufferPool& pool) {
  if (!lease_.retains_read_arena()) return absl::OkStatus();
  auto output = pool.AllocateReadOutputBuffer(value_bytes_);
  if (!output.ok()) return output.status();
  const auto source = value();
  std::memcpy(output->io_buffer().data_, source.data(), source.size());
  value_offset_ = output->headroom_bytes();
  lease_ = std::move(*output);
  return absl::OkStatus();
}

absl::StatusOr<DiskValue> StorageEngine::Impl::EncodeDiskValue(
    LoadedValue loaded) {
  return CompactReadValue{
      .lease_ = std::move(loaded.lease_),
      .value_offset_ = static_cast<std::uint32_t>(loaded.value_offset_),
      .value_bytes_ = static_cast<std::uint32_t>(loaded.value_bytes_)}
      .IntoDiskValue();
}

std::array<std::span<const std::byte>, 3> DiskValue::network_parts(
    std::span<std::byte, 32> header) const noexcept {
  if (!shared()) return {network_bytes(), {}, {}};
  auto* text = reinterpret_cast<char*>(header.data());
  text[0] = '$';
  auto [end, error] =
      std::to_chars(text + 1, text + header.size() - 2, network_size_);
  assert(error == std::errc{});
  (void)error;
  *end++ = '\r';
  *end++ = '\n';
  static constexpr std::array<std::byte, 2> tail{std::byte{'\r'},
                                                 std::byte{'\n'}};
  return {header.first(static_cast<std::size_t>(end - text)), value_bytes(),
          tail};
}

void DiskValue::AppendNetworkBytes(std::string& output) const {
  std::array<std::byte, 32> header;
  for (auto part : network_parts(header)) {
    if (!part.empty())
      output.append(reinterpret_cast<const char*>(part.data()), part.size());
  }
}

absl::StatusOr<DiskValue> CompactReadValue::IntoDiskValue() && {
  if (lease_.shared())
    return DiskValue(std::move(lease_), value_offset_, value_bytes_);
  const std::size_t value_offset = value_offset_;
  const std::size_t value_bytes = value_bytes_;
  std::span<std::byte> buffer = lease_.exclusive_bytes();
  char length[32];
  auto [end, error] =
      std::to_chars(length, length + sizeof(length), value_bytes);
  if (error != std::errc{}) {
    lease_.Reset();
    return absl::Status(absl::StatusCode::kInternal,
                        "bulk length formatting failed");
  }
  const std::size_t digits = static_cast<std::size_t>(end - length);
  const std::size_t prefix_bytes = digits + 3;
  if (value_offset < prefix_bytes || value_offset > buffer.size() ||
      value_bytes > buffer.size() - value_offset ||
      buffer.size() - value_offset - value_bytes < 2) {
    lease_.Reset();
    return absl::Status(absl::StatusCode::kInternal,
                        "value lacks RESP framing headroom or tailroom");
  }
  std::byte* prefix = buffer.data() + value_offset - prefix_bytes;
  prefix[0] = std::byte{'$'};
  std::memcpy(prefix + 1, length, digits);
  prefix[digits + 1] = std::byte{'\r'};
  prefix[digits + 2] = std::byte{'\n'};
  buffer[value_offset + value_bytes] = std::byte{'\r'};
  buffer[value_offset + value_bytes + 1] = std::byte{'\n'};
  const std::size_t network_offset = value_offset - prefix_bytes;
  return DiskValue(std::move(lease_), network_offset,
                   prefix_bytes + value_bytes + 2);
}

Task<absl::StatusOr<StorageEngine::Impl::LoadedValue>>
StorageEngine::Impl::LoadValueSlow(WorkerStore& key_store,
                                   WorkerStore::PartitionStore& partition,
                                   std::uint8_t db_id, std::string_view key,
                                   Digest digest, RecordLocation location,
                                   ExtentManifest extents,
                                   std::optional<absl::Status> failure,
                                   bool coalesce, ReadLatencyTrace* trace) {
  assert(!location.grouped());
  // The caller already resolved the key's partition for the index lookup.
  // Reuse it across retries instead of recomputing the Redis slot.
  const std::uint64_t replication_epoch = partition.replication_epoch_;
  // Candidate populations use a locally mapped epoch until promotion. Capture
  // it on the key owner; the physical block owner may own another partition.
  const std::optional<std::uint64_t> db_epoch =
      partition.replica_sync_
          ? std::make_optional(EffectiveRecordDbEpoch(partition, db_id))
          : std::nullopt;
  while (true) {
    assert(location.block_owner() < worker_count_);
    // Every branch assigns `loaded` before it is observed; the placeholder
    // does not need an allocated error message.
    absl::StatusOr<LoadedValue> loaded;

    // An external value's manifest is already decoded in this index entry,
    // so the record's own block holds nothing worth reading. Assemble here
    // and let each extent go straight to its block's owner, rather than
    // hopping to the record's owner first and having it acquire the output
    // buffer from its pool and hand the lease back across workers.
    if (failure) {
      loaded = std::move(*failure);
      failure.reset();
    } else if (location.external()) {
      loaded = co_await LoadExternalValueLocal(
          key_store, location, std::move(extents), false, trace);
    } else if (location.block_owner() == key_store.worker_->id()) {
      loaded =
          co_await LoadValueLocal(key_store, db_id, key, location,
                                  replication_epoch, db_epoch, coalesce, trace);
    } else {
      const unsigned owner = location.block_owner();
      std::string owned_key(key);
      loaded = co_await bycorf::SubmitTaskTo(
          owner,
          [this, owner, db_id, key = std::move(owned_key), location,
           replication_epoch, db_epoch, coalesce,
           trace]() mutable -> Task<absl::StatusOr<LoadedValue>> {
            co_return co_await LoadValueLocal(*stores_[owner], db_id, key,
                                              location, replication_epoch,
                                              db_epoch, coalesce, trace);
          });
    }
    // Replica reset is partition-wide and does not take each key lock. Reject
    // a result that crossed an epoch change, including a successful extent
    // read, rather than returning a value from the retired generation.
    if (partition.replication_epoch_ != replication_epoch ||
        (db_epoch && EffectiveRecordDbEpoch(partition, db_id) != *db_epoch))
        [[unlikely]] {
      co_return absl::Status(absl::StatusCode::kNotFound, "key not found");
    }
    if (loaded.ok() || loaded.status().code() != absl::StatusCode::kAborted) {
      co_return loaded;
    }

    // Defrag relocates records without acquiring key locks. A reader can copy
    // an index location, suspend while dispatching to the block owner or
    // waiting for a read buffer, and arrive after the source block is freed.
    // Resolve the key on its owner and follow the current physical location.
    // The caller's key lock serializes logical mutations, so only a physical
    // relocation of the same mutation is retryable.
    auto& index = partition.indexes_[db_id];
    auto resolved = co_await FindVerifiedEntry(key_store, index, digest, key);
    if (!resolved.ok()) {
      co_return resolved.status();
    }
    auto* current = *resolved;
    if (current == nullptr || current->value_.kind() != RecordKind::kValue ||
        current->value_.mutation_sequence_ != location.mutation_sequence_) {
      // Database-epoch invalidation is not serialized by an individual key
      // lock. If it removed or replaced this logical mutation while the read
      // was suspended, report ordinary absence; snapshot callers already
      // skip NotFound and foreground GET produces a nil reply.
      co_return absl::Status(absl::StatusCode::kNotFound, "key not found");
    }
    if (MaterializeIndexLocation(*current).SamePhysicalRecord(location)) {
      // The index still endorses the location that failed validation, so this
      // is corruption rather than a relocation race. Preserve a hard error.
      co_return absl::Status(absl::StatusCode::kInternal,
                             loaded.status().message());
    }
    location = MaterializeIndexLocation(*current);
    extents = ExtentsFor(key_store, current);
  }
}

StorageEngine::Impl::ReadExtentAwaitable StorageEngine::Impl::ReadExtentOnOwner(
    ExtentRef ref, std::uint32_t extent_index, std::byte* destination,
    ReadLatencyTrace* trace) {
  return ReadExtentAwaitable(*this, CurrentStore(), ref, extent_index, 0,
                             std::span(destination, ref.payload_bytes_), true,
                             trace);
}

StorageEngine::Impl::ReadExtentAwaitable StorageEngine::Impl::ReadExtentSlice(
    WorkerStore& store, ExtentRef ref, std::uint32_t extent_index,
    std::size_t source_offset, std::span<std::byte> destination,
    ReadLatencyTrace* trace) {
  return ReadExtentAwaitable(*this, store, ref, extent_index, source_offset,
                             destination, false, trace);
}

Task<absl::Status>
StorageEngine::Impl::ReadExtentAwaitable::RemoteRead::operator()() {
  // SubmitTaskTo requires a Task on the physical owner. The local awaitable
  // below supplies the I/O and pool continuation without another loader frame.
  co_return co_await engine_->ReadExtentSlice(*engine_->stores_[owner_], ref_,
                                              extent_index_, source_offset_,
                                              destination_, trace_);
}

StorageEngine::Impl::ReadExtentAwaitable::ReadExtentAwaitable(
    Impl& engine, WorkerStore& store, ExtentRef ref, std::uint32_t extent_index,
    std::size_t source_offset, std::span<std::byte> destination, bool route,
    ReadLatencyTrace* trace)
    : engine_(engine),
      store_(store),
      ref_(ref),
      extent_index_(extent_index),
      source_offset_(source_offset),
      destination_(destination),
      trace_(trace) {
  if (source_offset > ref.payload_bytes_ ||
      destination.size() > ref.payload_bytes_ - source_offset) {
    ready_ = absl::InvalidArgumentError("extent slice is out of range");
    return;
  }
  if (route) {
    const unsigned owner = engine.BlockOwner(ref.block_id_);
    if (owner >= engine.worker_count_) {
      ready_ = absl::AbortedError("stale or missing external extent");
      return;
    }
    if (owner != store.worker_->id()) {
      remote_.emplace(bycorf::SubmitTaskTo(
          owner, RemoteRead{&engine, owner, ref, extent_index, source_offset,
                            destination, trace}));
      return;
    }
  }
  // Recheck the physical owner and incarnation after routing. Retain the
  // extent pin across pool waiting as well as I/O, matching manifest reads.
  auto* state = engine.FindBlockState(store, ref.block_id_);
  if (!state || !state->allocated_ || state->freeing_ ||
      state->kind_ != BlockKind::kPayloadExtent ||
      state->allocation_epoch_ != ref.allocation_epoch_) {
    ready_ = absl::AbortedError("stale or missing external extent");
    return;
  }
  state->PinLocal();
  pinned_ = state;
  read_bytes_ = AlignDirect(kBlockHeaderBytes + ref.payload_bytes_);
  PrepareBuffer();
}

StorageEngine::Impl::ReadExtentAwaitable::~ReadExtentAwaitable() {
  if (pinned_) engine_.UnpinBlock(store_, *pinned_, ref_.block_id_);
}

void StorageEngine::Impl::ReadExtentAwaitable::PrepareBuffer() {
  const auto started = trace_ ? trace_->NowNanos() : 0;
  auto acquired = store_.buffers_.TryAcquireReadBuffer(read_bytes_);
  // Force the extent's full-range probe to fail independently of record reads
  // so end-to-end recovery tests can exercise streaming without stalling it.
  LAVIK_FAULT_INJECT(
      if (acquired.ok() &&
          LAVIK_FAULT_MATCHES("LAVIK_FAIL_EXTENT_BUFFER_ALLOCATION", "1")) {
        acquired = absl::ResourceExhaustedError(
            "injected extent buffer allocation failure");
      });
  if (acquired.ok()) {
    lease_ = std::move(*acquired);
    if (trace_) trace_->RecordBuffer(started, trace_->NowNanos());
  } else if (absl::IsResourceExhausted(acquired.status()) &&
             store_.buffers_.read_capacity_bytes() != 0) {
    // Stream through a smaller arena range without any dynamic DMA allocation.
    // Retain the pin while chunk acquisition supplies FIFO backpressure.
    chunked_ = ReadInChunks();
  } else {
    ready_ = acquired.status();
  }
}

std::coroutine_handle<> StorageEngine::Impl::ReadExtentAwaitable::await_suspend(
    std::coroutine_handle<> awaiting) {
  if (remote_) {
    remote_->await_suspend(awaiting);
    return std::noop_coroutine();
  }
  return SuspendRead(awaiting);
}

std::coroutine_handle<> StorageEngine::Impl::ReadExtentAwaitable::SuspendRead(
    std::coroutine_handle<> awaiting) {
  if (chunked_.valid()) {
    auto child =
        std::move(chunked_).operator co_await().await_suspend(awaiting);
    if (store_.worker_->IsBackground(awaiting))
      store_.worker_->RegisterBackground(child);
    return child;
  }
  if (ready_) return awaiting;
  return SubmitRead(awaiting) ? std::noop_coroutine() : awaiting;
}

bool StorageEngine::Impl::ReadExtentAwaitable::SubmitRead(
    std::coroutine_handle<> awaiting) {
  auto buffer = lease_.io_buffer();
  buffer.size_ = read_bytes_;
  const auto [file_id, offset] = engine_.FileOffset(ref_.block_id_);
  if (trace_) io_start_ns_ = trace_->NowNanos();
  return Suspend(
      awaiting, lease_.registered()
                    ? store_.worker_->SubmitReadFixed(store_.files_[file_id],
                                                      buffer, offset, this)
                    : store_.worker_->SubmitRead(
                          store_.files_[file_id],
                          std::span(buffer.data_, buffer.size_), offset, this));
}

absl::Status StorageEngine::Impl::ReadExtentAwaitable::await_resume() {
  if (remote_) return remote_->await_resume();
  if (ready_) return std::move(*ready_);
  if (chunked_.valid()) return std::move(chunked_).TakeResult();
  if (trace_) trace_->RecordIo(io_start_ns_, trace_->NowNanos());
  auto read =
      Resume(lease_.registered() ? "fixed-buffer read failed" : "read failed");
  if (!read.ok()) return read.status();
  store_.worker_->RecordStorageReadCompletion(*read);
  if (static_cast<std::size_t>(*read) != read_bytes_)
    return absl::InternalError("short extent block read");
  TraceSpanScope decode_scope(trace_ ? trace_->diagnostics_ : nullptr,
                              TraceStage::kDecode);
  const auto buffer = lease_.io_buffer();
  BlockHeader header{};
  if (!DecodeHeader(std::span<const std::byte, kBlockHeaderBytes>(
                        buffer.data_, kBlockHeaderBytes),
                    header))
    return absl::InternalError("extent header does not match manifest");
  const auto payload = std::span<const std::byte>(
      buffer.data_ + kBlockHeaderBytes, ref_.payload_bytes_);
  // A checksummed extent is validated in full even for a partial read.
  // This runs in the resumed task's budget, never on the I/O completion poller.
  if (!header.extent_no_crc_ && Crc32c(payload) != ref_.payload_checksum_)
    return absl::InternalError("extent payload checksum mismatch");
  std::memcpy(destination_.data(), payload.data() + source_offset_,
              destination_.size());
  return absl::OkStatus();
}

bool StorageEngine::Impl::ReadExtentAwaitable::DecodeHeader(
    std::span<const std::byte, kBlockHeaderBytes> bytes,
    BlockHeader& header) const {
  return DecodeBlockHeaderPages(bytes, &header) &&
         header.kind_ == BlockKind::kPayloadExtent &&
         header.block_id_ == ref_.block_id_ &&
         header.allocation_epoch_ == ref_.allocation_epoch_ &&
         header.extent_index_ == extent_index_ &&
         header.extent_payload_bytes_ == ref_.payload_bytes_ &&
         header.extent_payload_checksum_ == ref_.payload_checksum_;
}

Task<absl::Status> StorageEngine::Impl::ReadExtentAwaitable::ReadInChunks() {
  const auto buffer_start = trace_ ? trace_->NowNanos() : 0;
  auto acquired = co_await store_.buffers_.AcquireReadChunk();
  if (trace_) trace_->RecordBuffer(buffer_start, trace_->NowNanos());
  if (!acquired.ok()) co_return acquired.status();
  ReadBufferLease chunk = std::move(*acquired);
  const auto capacity = chunk.io_buffer().size_;
  const auto [file_id, block_offset] = engine_.FileOffset(ref_.block_id_);
  // Even the smallest configured slot can read the duplicated header pages
  // separately. The pin retained by this awaitable prevents block recycling.
  std::array<std::byte, kBlockHeaderBytes> header_bytes;
  for (std::size_t offset = 0; offset < header_bytes.size();) {
    auto buffer = chunk.io_buffer();
    buffer.size_ = std::min(capacity, header_bytes.size() - offset);
    const auto io_start = trace_ ? trace_->NowNanos() : 0;
    auto read = co_await ReadStorageBuffer(
        *store_.worker_, store_.files_[file_id], buffer, chunk.registered(),
        block_offset + offset);
    LAVIK_FAULT_INJECT(
        if (trace_ &&
            LAVIK_FAULT_MATCHES("LAVIK_LATENCY_PAUSE_READ_COMPLETION", "1")) {
          auto delayed = co_await fault_injection::PauseWhileFileExists(
              "LAVIK_LATENCY_PAUSE_READ_COMPLETION_FILE");
          if (!delayed.ok()) co_return delayed;
        });
    if (trace_) trace_->RecordIo(io_start, trace_->NowNanos());
    if (!read.ok()) co_return read.status();
    store_.worker_->RecordStorageReadCompletion(*read);
    if (*read != buffer.size_)
      co_return absl::InternalError("short extent block read");
    TraceSpanScope decode(trace_ ? trace_->diagnostics_ : nullptr,
                          TraceStage::kDecode);
    std::memcpy(header_bytes.data() + offset, buffer.data_, buffer.size_);
    offset += buffer.size_;
  }
  BlockHeader header{};
  {
    TraceSpanScope decode(trace_ ? trace_->diagnostics_ : nullptr,
                          TraceStage::kDecode);
    if (!DecodeHeader(header_bytes, header))
      co_return absl::InternalError("extent header does not match manifest");
  }

  // Preserve the full-read contract: no caller-visible bytes change on I/O or
  // checksum failure, including corruption outside a requested slice. Only
  // the requested slice is staged, in ordinary heap memory, never DMA memory.
  auto staged = store_.buffers_.AllocateReadOutputBuffer(destination_.size());
  if (!staged.ok()) co_return staged.status();
  auto* output = staged->io_buffer().data_;
  absl::crc32c_t checksum{0};
  for (std::size_t offset = 0; offset < ref_.payload_bytes_;) {
    auto buffer = chunk.io_buffer();
    const auto remaining = ref_.payload_bytes_ - offset;
    buffer.size_ = std::min(capacity, AlignDirect(remaining));
    const auto io_start = trace_ ? trace_->NowNanos() : 0;
    auto read = co_await ReadStorageBuffer(
        *store_.worker_, store_.files_[file_id], buffer, chunk.registered(),
        block_offset + kBlockHeaderBytes + offset);
    LAVIK_FAULT_INJECT(
        if (trace_ &&
            LAVIK_FAULT_MATCHES("LAVIK_LATENCY_PAUSE_READ_COMPLETION", "1")) {
          auto delayed = co_await fault_injection::PauseWhileFileExists(
              "LAVIK_LATENCY_PAUSE_READ_COMPLETION_FILE");
          if (!delayed.ok()) co_return delayed;
        });
    if (trace_) trace_->RecordIo(io_start, trace_->NowNanos());
    if (!read.ok()) co_return read.status();
    store_.worker_->RecordStorageReadCompletion(*read);
    if (*read != buffer.size_)
      co_return absl::InternalError("short extent block read");
    TraceSpanScope decode(trace_ ? trace_->diagnostics_ : nullptr,
                          TraceStage::kDecode);
    const auto payload_bytes = std::min(remaining, buffer.size_);
    if (!header.extent_no_crc_) {
      checksum = absl::ExtendCrc32c(
          checksum,
          absl::string_view(reinterpret_cast<const char*>(buffer.data_),
                            payload_bytes));
    }
    const auto begin = std::max(offset, source_offset_);
    const auto end =
        std::min(offset + payload_bytes, source_offset_ + destination_.size());
    if (begin < end)
      std::memcpy(output + begin - source_offset_,
                  buffer.data_ + begin - offset, end - begin);
    offset += payload_bytes;
  }
  TraceSpanScope decode(trace_ ? trace_->diagnostics_ : nullptr,
                        TraceStage::kDecode);
  if (!header.extent_no_crc_ &&
      static_cast<std::uint32_t>(checksum) != ref_.payload_checksum_)
    co_return absl::InternalError("extent payload checksum mismatch");
  if (!destination_.empty())
    std::memcpy(destination_.data(), output, destination_.size());
  co_return absl::OkStatus();
}

Task<absl::StatusOr<std::string>> StorageEngine::Impl::LoadOutOfIndexKey(
    WorkerStore& store, const RecordLocation& location, std::size_t key_bytes) {
  const unsigned owner = location.block_owner();
  if (owner >= worker_count_)
    co_return absl::InternalError("indirect key block has no owner");
  auto find = [this, owner, location]() -> absl::StatusOr<IndirectKeyHandle> {
    auto& physical = *stores_[owner];
    auto* state = FindBlockState(physical, location.block_id());
    if (state == nullptr || !state->allocated_ || state->freeing_ ||
        state->allocation_epoch_ != location.allocation_epoch())
      return absl::AbortedError("indirect key record moved");
    auto block = physical.indirect_key_references_.find(
        {location.block_id(), location.allocation_epoch()});
    if (block == physical.indirect_key_references_.end())
      return absl::DataLossError("indirect key block has no references");
    auto entry = block->second.find(location.record_offset());
    if (entry == block->second.end())
      return absl::DataLossError("indirect key record has no UUID");
    return entry->second;
  };
  absl::StatusOr<IndirectKeyHandle> found =
      absl::InternalError("unresolved key");
  if (owner == store.worker_->id())
    found = find();
  else
    found = co_await bycorf::SubmitTo(owner, std::move(find));
  if (!found.ok()) co_return found.status();
  auto key = co_await LoadIndirectKey(std::move(*found));
  if (key.ok() && key->size() != key_bytes)
    co_return absl::DataLossError("indirect key length mismatch");
  co_return key;
}

Task<absl::StatusOr<RecordIndex::Entry*>>
StorageEngine::Impl::FindVerifiedEntry(
    WorkerStore& store, RecordIndex& index, const Digest& digest,
    std::string_view key, const RecordLocation& verified_location) {
  // Grouped readers already checked the parent key before capturing their
  // view. Re-reading a multi-megabyte key for every small page adds IO and
  // copies proportional to key bytes times page count. Selection never
  // suspends, and the allocation epoch prevents a reused block from matching.
  // Complete index keys still compare full bytes, while a digest collision
  // between indirect keys must also match the previously checked record.
  auto* current = index.FindCandidateIf(
      digest, key, [&](const RecordIndex::Entry& candidate) {
        return candidate.key_complete() ||
               MaterializeIndexLocation(candidate).SamePhysicalRecord(
                   verified_location);
      });
  if (current != nullptr) co_return current;
  // Relocation may preserve the logical key while changing physical identity.
  // Keep the asynchronous verifier and its retry/membership checks as fallback.
  co_return co_await FindVerifiedEntry(store, index, digest, key);
}

Task<absl::StatusOr<RecordIndex::Entry*>>
StorageEngine::Impl::FindVerifiedEntry(WorkerStore& store, RecordIndex& index,
                                       const Digest& digest,
                                       std::string_view key) {
  struct Candidate {
    std::uintptr_t entry_address_ = 0;
    ExtentManifest extents_;
    RecordLocation location_{};
    std::uint32_t hash_ = 0;
  };
  for (;;) {
    RecordIndex::Entry* first = index.Find(digest, key);
    if (first == nullptr || first->key_complete()) [[likely]] {
      co_return first;
    }
    std::vector<Candidate> candidates;
    for (RecordIndex::Entry* entry : index.FindCandidates(digest, key)) {
      candidates.push_back(Candidate{
          .entry_address_ = reinterpret_cast<std::uintptr_t>(entry),
          .extents_ = ExtentsFor(store, entry),
          .location_ = MaterializeIndexLocation(*entry),
          .hash_ = RecordIndex::AddressHash(entry->external_key_digest()),
      });
    }
    bool changed = false;
    for (const Candidate& candidate : candidates) {
      auto actual =
          co_await LoadOutOfIndexKey(store, candidate.location_, key.size());
      absl::StatusOr<bool> verified =
          actual.ok() ? absl::StatusOr<bool>(*actual == key)
                      : absl::StatusOr<bool>(actual.status());
      RecordIndex::Entry* current =
          index.FindAddress(candidate.entry_address_, candidate.hash_);
      const bool still_current =
          current != nullptr &&
          MaterializeIndexLocation(*current).SamePhysicalRecord(
              candidate.location_);
      if (!verified.ok()) {
        if (verified.status().code() == absl::StatusCode::kAborted &&
            !still_current) {
          changed = true;
          break;
        }
        co_return verified.status().code() == absl::StatusCode::kAborted
            ? absl::Status(absl::StatusCode::kInternal,
                           verified.status().message())
            : verified.status();
      }
      if (!still_current) {
        changed = true;
        break;
      }
      if (*verified) {
        co_return current;
      }
    }
    if (!changed) {
      co_return static_cast<RecordIndex::Entry*>(nullptr);
    }
  }
}

Task<absl::StatusOr<StorageEngine::Impl::LoadedValue>>
StorageEngine::Impl::LoadExternalValueLocal(WorkerStore& store,
                                            const RecordLocation& location,
                                            ExtentManifest extents,
                                            bool grouped_payload,
                                            ReadLatencyTrace* trace) {
  if (!location.external() || extents == nullptr) {
    co_return absl::Status(absl::StatusCode::kInternal,
                           "external value has no valid extent manifest");
  }
  std::uint64_t value_bytes = 0;
  for (const ExtentRef& ref : *extents) {
    if (ref.payload_bytes_ == 0 ||
        ref.payload_bytes_ > kMaxRecordPayloadBytes - value_bytes) {
      co_return absl::Status(absl::StatusCode::kInternal,
                             "invalid external value manifest");
    }
    value_bytes += ref.payload_bytes_;
  }
  if (location.value_type() == ValueType::kString && !grouped_payload &&
      value_bytes != location.logical_size_) {
    co_return absl::Status(absl::StatusCode::kInternal,
                           "external string length does not match metadata");
  }
  if (trace != nullptr) {
    trace->buffer_acquire_start_ns_ = trace->NowNanos();
  }
  // Extents are read into their own I/O leases and copied here. The output
  // needs neither DMA memory nor a fixed slot; holding a slot while awaiting
  // extent reads could also deadlock concurrent assembly in a bounded pool.
  auto acquired = store.buffers_.AllocateReadOutputBuffer(value_bytes);
  if (!acquired.ok()) {
    co_return acquired.status();
  }
  ReadBufferLease output = std::move(*acquired);
  FixedBuffer destination = output.io_buffer();
  if (destination.size_ < value_bytes) {
    co_return absl::Status(absl::StatusCode::kOutOfRange,
                           "external value exceeds read buffer capacity");
  }
  if (trace != nullptr) {
    trace->buffer_acquired_ns_ = trace->NowNanos();
    trace->RecordBuffer(trace->buffer_acquire_start_ns_,
                        trace->buffer_acquired_ns_);
    trace->disk_read_ = true;
  }
  if (trace != nullptr) {
    trace->io_submit_ns_ = trace->NowNanos();
  }
  // Validate the complete manifest before reading into the output buffer.
  std::size_t checked_bytes = 0;
  for (const ExtentRef& ref : *extents) {
    if (BlockOwner(ref.block_id_) >= worker_count_) {
      co_return absl::AbortedError("stale or missing external extent");
    }
    if (ref.payload_bytes_ > value_bytes - checked_bytes)
      co_return absl::InternalError(
          "external value length does not match manifest");
    checked_bytes += ref.payload_bytes_;
  }
  if (checked_bytes != value_bytes)
    co_return absl::InternalError(
        "external value length does not match manifest");
  std::size_t output_offset = 0;
  for (std::size_t index = 0; index < extents->size(); ++index) {
    const ExtentRef& ref = extents->at(index);
    const absl::Status read =
        co_await ReadExtentOnOwner(ref, static_cast<std::uint32_t>(index),
                                   destination.data_ + output_offset, trace);
    if (!read.ok()) co_return read;
    output_offset += ref.payload_bytes_;
  }
  if (output_offset != value_bytes) {
    co_return absl::Status(absl::StatusCode::kInternal,
                           "external value length does not match manifest");
  }
  if (trace != nullptr) {
    trace->io_complete_ns_ = trace->NowNanos();

    trace->decode_done_ns_ = trace->io_complete_ns_;
  }
  const std::size_t value_offset =
      static_cast<std::size_t>(destination.data_ - output.bytes().data());
  co_return LoadedValue{std::move(output), value_offset, output_offset};
}

StorageEngine::Impl::LoadValueAwaitable StorageEngine::Impl::LoadValue(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    RecordLocation location, ExtentManifest extents,
    GroupedObject::Handle grouped_snapshot, bool coalesce,
    ReadLatencyTrace* trace) {
  return LoadValueAwaitable(*this, store, partition, db_id, key, digest,
                            location, std::move(extents),
                            std::move(grouped_snapshot), coalesce, trace);
}

StorageEngine::Impl::LoadValueAwaitable StorageEngine::Impl::LoadValueLocal(
    WorkerStore& store, std::uint8_t db_id, std::string_view key,
    RecordLocation location, std::uint64_t replication_epoch,
    std::optional<std::uint64_t> expected_db_epoch, bool coalesce,
    ReadLatencyTrace* trace) {
  return LoadValueAwaitable(*this, store, db_id, key, location,
                            replication_epoch, expected_db_epoch, coalesce,
                            trace);
}

StorageEngine::Impl::LoadValueAwaitable::LoadValueAwaitable(
    Impl& engine, WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    RecordLocation location, ExtentManifest extents,
    GroupedObject::Handle grouped_snapshot, bool coalesce,
    ReadLatencyTrace* trace)
    : engine_(engine),
      store_(store),
      // Grouped snapshots have their own population/pin protocol.
      partition_(location.grouped() ? nullptr : &partition),
      key_(key),
      digest_(digest),
      location_(location),
      trace_(trace),
      replication_epoch_(partition.replication_epoch_),
      expected_db_epoch_(engine.EffectiveRecordDbEpoch(partition, db_id)),
      db_id_(db_id),
      coalesce_(coalesce) {
  if (location.grouped()) {
    // The grouped task owns its graph/version protocol. Keep its borrowed
    // digest in this awaitable, not in the constructor's argument lifetime.
    slow_ =
        engine.LoadGroupedValue(store, partition, db_id, key, digest_, location,
                                std::move(grouped_snapshot),
                                GroupedValueEncoding::kLogicalStream, trace);
  } else if (location.external() ||
             location.block_owner() != store.worker_->id()) {
    slow_ =
        engine.LoadValueSlow(store, partition, db_id, key, digest, location,
                             std::move(extents), std::nullopt, coalesce, trace);
  } else {
    PrepareLocal();
    if (ready_ && absl::IsAborted(ready_->status())) Retry(ready_->status());
  }
}

StorageEngine::Impl::LoadValueAwaitable::LoadValueAwaitable(
    Impl& engine, WorkerStore& store, std::uint8_t db_id, std::string_view key,
    RecordLocation location, std::uint64_t replication_epoch,
    std::optional<std::uint64_t> expected_db_epoch, bool coalesce,
    ReadLatencyTrace* trace)
    : engine_(engine),
      store_(store),
      key_(key),
      location_(location),
      trace_(trace),
      replication_epoch_(replication_epoch),
      expected_db_epoch_(expected_db_epoch),
      db_id_(db_id),
      coalesce_(coalesce) {
  PrepareLocal();
}

StorageEngine::Impl::LoadValueAwaitable::~LoadValueAwaitable() { Unpin(); }

void StorageEngine::Impl::LoadValueAwaitable::Unpin() {
  auto* state = std::exchange(pinned_, nullptr);
  if (!state) return;
  engine_.UnpinBlock(store_, *state, location_.block_id());
}

ReadFlightKey StorageEngine::Impl::LoadValueAwaitable::FlightKey() const {
  return {.engine = reinterpret_cast<std::uintptr_t>(&engine_),
          .block = location_.block_id(),
          .allocation_epoch = location_.allocation_epoch(),
          .mutation_sequence = location_.mutation_sequence_,
          .aligned_offset = aligned_offset_,
          .read_bytes = read_bytes_,
          .db_epoch = expected_db_epoch_.value_or(engine_.DbEpoch(db_id_)),
          .replication_epoch = replication_epoch_,
          .population_generation = population_generation_,
          .file = store_.files_[file_id_].index_,
          .record_offset = location_.record_offset(),
          .record_bytes = location_.total_disk_bytes(),
          .block_owner = static_cast<std::uint16_t>(location_.block_owner()),
          .db = db_id_};
}

void StorageEngine::Impl::LoadValueAwaitable::Joined() {
  // The independent producer retains the pin. Each waiter still validates
  // its own record header and logical population after it is scheduled.
  Unpin();
  lease_.Reset();
  if (trace_) [[unlikely]] {
    trace_->disk_read_ = true;
    trace_->flight_wait_start_ns_ = trace_->NowNanos();
  }
}

void StorageEngine::Impl::LoadValueAwaitable::PrepareLocal() {
  LAVIK_FAULT_INJECT(if (LAVIK_FAULT_MATCHES("LAVIK_FAIL_VALUE_READ_KEY",
                                             key_)) {
    ready_.emplace(absl::InternalError("injected value payload read failure"));
    return;
  });
  if (location_.external()) {
    ready_.emplace(
        absl::InternalError("external value was dispatched as inline"));
    return;
  }
  const auto [file_id, block_offset] = engine_.FileOffset(location_.block_id());
  file_id_ = file_id;
  const auto absolute = block_offset + location_.record_offset();
  const auto mask =
      static_cast<std::uint64_t>(engine_.direct_io_alignment_ - 1);
  aligned_offset_ = absolute & ~mask;
  record_headroom_ = absolute - aligned_offset_;
  read_bytes_ =
      (record_headroom_ + location_.total_disk_bytes() + mask) & ~mask;
  // Bypass callers must not create or probe a registry, even if another read
  // already has a Flight for this record. Admission survives retries and hops.
  // Probe before taking scarce DMA capacity. A borrowed staging pointer is
  // used only synchronously; buffer waiting below resolves it again.
  auto* candidate = coalesce_
                        ? engine_.FindBlockState(store_, location_.block_id())
                        : nullptr;
  const auto* staged =
      candidate && location_.in_memory() && candidate->in_memory_
          ? engine_.StagingFor(store_, *candidate)
          : nullptr;
  if (candidate && candidate->allocated_ && !candidate->freeing_ &&
      candidate->allocation_epoch_ == location_.allocation_epoch() &&
      (!staged || location_.record_offset() < staged->buffer_begin_) &&
      read_bytes_ <= store_.buffers_.read_capacity_bytes()) {
    flights_ = engine_.ReadFlights(store_);
    if (flights_) {
      population_generation_ =
          partition_ ? store_.index_generations_[db_id_] : 0;
      flight_hash_ = absl::Hash<ReadFlightKey>{}(FlightKey());
      if (flights_->TryJoin(FlightKey(), flight_hash_, *this)) {
        Joined();
        return;
      }
    }
  } else {
    flights_ = nullptr;
  }
  bool chunked = false;
  if (!lease_.valid()) {
    if (trace_) [[unlikely]]
      trace_->buffer_acquire_start_ns_ = trace_->NowNanos();
    auto acquired = store_.buffers_.TryAcquireReadBuffer(read_bytes_);
    if (!acquired.ok() && absl::IsResourceExhausted(acquired.status()) &&
        read_bytes_ <= engine_.direct_io_alignment_) {
      // The smallest read cannot benefit from chunking. Wait for an exact
      // range without first allocating/copying a separate output image.
      auto acquire = store_.buffers_.AcquireReadBuffer(read_bytes_);
      if (!acquire.await_ready()) {
        buffer_wait_.emplace(std::move(acquire));
        return;
      }
      acquired = acquire.await_resume();
    }
    if (!acquired.ok() && absl::IsResourceExhausted(acquired.status()) &&
        store_.buffers_.read_capacity_bytes() != 0) {
      // Oversized records (including KeyRecords and indivisible collection
      // entries) need not fit a DMA allocation. Own the assembled image in
      // ordinary memory; arena ranges provide the actual I/O.
      acquired = store_.buffers_.AllocateReadOutputBuffer(read_bytes_);
      chunked = true;
    }
    if (!acquired.ok()) {
      ready_.emplace(acquired.status());
      return;
    }
    lease_ = std::move(*acquired);
  }
  if (trace_) [[unlikely]] {
    trace_->buffer_acquired_ns_ = trace_->NowNanos();
    trace_->RecordBuffer(trace_->buffer_acquire_start_ns_,
                         trace_->buffer_acquired_ns_);
  }
  // Resolve staging memory only after acquiring the buffer: waiting can let
  // flush/defrag retire it. The staged copy itself never suspends, so only
  // disk I/O needs a physical pin.
  auto* state = engine_.FindBlockState(store_, location_.block_id());
  if (!state || !state->allocated_ || state->freeing_ ||
      state->allocation_epoch_ != location_.allocation_epoch()) {
    ready_.emplace(absl::AbortedError("stale index block epoch"));
    return;
  }
  // Locations retained outside the primary index can keep their staged bit
  // after a flush. A rebound write buffer contains only its new tail.
  const auto* staging = location_.in_memory() && state->in_memory_
                            ? engine_.StagingFor(store_, *state)
                            : nullptr;
  if (staging != nullptr && location_.record_offset() >= staging->buffer_begin_)
      [[unlikely]] {
    auto buffer = engine_.StagingBufferFor(store_, *state);
    if (!buffer.data_ ||
        location_.record_offset() + location_.total_disk_bytes() >
            buffer.size_) {
      ready_.emplace(absl::InternalError("invalid in-memory location"));
      return;
    }
    if (trace_) [[unlikely]] {
      trace_->io_submit_ns_ = trace_->buffer_acquired_ns_;
      trace_->io_complete_ns_ = trace_->buffer_acquired_ns_;
    }
    const auto* data = buffer.data_ + location_.record_offset();
    auto payload = ParseRecord(data);
    if (!payload.ok())
      ready_.emplace(payload.status());
    else
      ready_.emplace(FinishRecord(data, *payload, true));
    return;
  }
  if (read_bytes_ > lease_.io_buffer().size_) {
    ready_.emplace(absl::OutOfRangeError(
        "record exceeds registered read buffer capacity"));
    return;
  }
  state->PinLocal();
  pinned_ = state;
  if (trace_) [[unlikely]]
    trace_->disk_read_ = true;
  if (chunked) slow_ = ReadInChunks();
}

Task<StorageEngine::Impl::LoadValueAwaitable::Result>
StorageEngine::Impl::LoadValueAwaitable::ReadInChunks() {
  // PrepareLocal pins the physical incarnation before this task can wait.
  // Keep the pin across every chunk, and validate the complete private image
  // with the same decoder/checksum rules as a single read before publishing it.
  const auto buffer_start = trace_ ? trace_->NowNanos() : 0;
  auto acquired = co_await store_.buffers_.AcquireReadChunk();
  if (trace_) [[unlikely]]
    trace_->RecordBuffer(buffer_start, trace_->NowNanos());
  if (!acquired.ok()) co_return acquired.status();
  ReadBufferLease chunk = std::move(*acquired);
  if (PopulationChanged()) co_return absl::NotFoundError("key not found");
  if (trace_) [[unlikely]]
    trace_->io_submit_ns_ = trace_->NowNanos();
  for (std::size_t offset = 0; offset < read_bytes_;) {
    auto buffer = chunk.io_buffer();
    buffer.size_ = std::min(buffer.size_, read_bytes_ - offset);
    const auto io_start = trace_ ? trace_->NowNanos() : 0;
    auto read = co_await ReadStorageBuffer(
        *store_.worker_, store_.files_[file_id_], buffer, chunk.registered(),
        aligned_offset_ + offset);
    LAVIK_FAULT_INJECT(
        if (trace_ &&
            LAVIK_FAULT_MATCHES("LAVIK_LATENCY_PAUSE_READ_COMPLETION", "1")) {
          auto delayed = co_await fault_injection::PauseWhileFileExists(
              "LAVIK_LATENCY_PAUSE_READ_COMPLETION_FILE");
          if (!delayed.ok()) co_return delayed;
        });
    if (trace_) [[unlikely]]
      trace_->io_complete_ns_ = trace_->NowNanos();
    if (trace_) [[unlikely]]
      trace_->RecordIo(io_start, trace_->io_complete_ns_);
    if (!read.ok()) co_return read.status();
    store_.worker_->RecordStorageReadCompletion(*read);
    if (*read != buffer.size_)
      co_return absl::InternalError("short compact record read");
    TraceSpanScope decode(trace_ ? trace_->diagnostics_ : nullptr,
                          TraceStage::kDecode);
    std::memcpy(lease_.io_buffer().data_ + offset, buffer.data_, buffer.size_);
    offset += buffer.size_;
  }
  if (PopulationChanged()) co_return absl::NotFoundError("key not found");
  const auto* data = lease_.io_buffer().data_ + record_headroom_;
  auto parsed = ParseRecord(data);
  if (!parsed.ok()) {
    if (partition_ && absl::IsAborted(parsed.status())) {
      // Release the slot before retrying: the successor may need the same
      // sole slot. Do not replace slow_ while its own task is still running.
      chunk.Reset();
      Unpin();
      lease_.Reset();
      co_return co_await engine_.LoadValueSlow(
          store_, *partition_, db_id_, key_, digest_, location_, {},
          parsed.status(), coalesce_, trace_);
    }
    co_return parsed.status();
  }
  co_return FinishRecord(data, *parsed, false);
}

bool StorageEngine::Impl::LoadValueAwaitable::PopulationChanged() const {
  return partition_ && (partition_->replication_epoch_ != replication_epoch_ ||
                        engine_.EffectiveRecordDbEpoch(*partition_, db_id_) !=
                            *expected_db_epoch_);
}

void StorageEngine::Impl::LoadValueAwaitable::Retry(absl::Status failure) {
  assert(partition_ && absl::IsAborted(failure));
  Unpin();
  lease_.Reset();
  completed_buffer_.Reset();
  ready_.reset();
  // Inline reads have no extent manifest. Seed the failure so the slow path
  // resolves the index before any further I/O and preserves corruption errors
  // when the index still endorses the failed physical record.
  slow_ = engine_.LoadValueSlow(store_, *partition_, db_id_, key_, digest_,
                                location_, {}, std::move(failure), coalesce_,
                                trace_);
}

void StorageEngine::Impl::LoadValueAwaitable::FinishBufferWait() {
  auto acquired = buffer_wait_->await_resume();
  buffer_wait_.reset();
  if (PopulationChanged()) {
    ready_.emplace(absl::NotFoundError("key not found"));
  } else if (!acquired.ok()) {
    ready_.emplace(acquired.status());
  } else {
    lease_ = std::move(*acquired);
    PrepareLocal();
    if (partition_ && ready_ && absl::IsAborted(ready_->status()))
      Retry(ready_->status());
  }
}

void StorageEngine::Impl::LoadValueAwaitable::BufferReady(
    bycorf::RemoteWork* work) {
  auto& read = *static_cast<LoadValueAwaitable*>(work);
  read.FinishBufferWait();
  auto next = read.SuspendRead(read.awaiting_);
  if (next != std::noop_coroutine()) read.store_.worker_->Enqueue(next);
}

std::coroutine_handle<> StorageEngine::Impl::LoadValueAwaitable::await_suspend(
    std::coroutine_handle<> awaiting) {
  if (buffer_wait_) {
    awaiting_ = awaiting;
    run_fn_ = &BufferReady;
    task_class_ = store_.worker_->IsBackground(awaiting)
                      ? bycorf::TaskClass::kBackground
                      : bycorf::CurrentTaskClass();
    if (buffer_wait_->SuspendWith(*this)) return std::noop_coroutine();
    FinishBufferWait();
  }
  return SuspendRead(awaiting);
}

std::coroutine_handle<> StorageEngine::Impl::LoadValueAwaitable::SuspendRead(
    std::coroutine_handle<> awaiting) {
  if (slow_.valid()) {
    auto child = std::move(slow_).operator co_await().await_suspend(awaiting);
    // A pool handoff or direct continuation can run outside the background
    // caller's scheduled slice. Preserve its class for chunking and retries.
    if (store_.worker_->IsBackground(awaiting))
      store_.worker_->RegisterBackground(child);
    return child;
  }
  if (ready_ || flight_completed()) return awaiting;
  if (flight_attached()) {
    Suspend(awaiting, absl::OkStatus());
    return std::noop_coroutine();
  }
  auto buffer = lease_.io_buffer();
  buffer.size_ = read_bytes_;
  const bool fixed = lease_.registered();
  bycorf::IoCompletion* tag = this;
  auto* pinned = pinned_;
  bool owns_pin = pinned != nullptr;
  if (flights_) {
    const auto admission =
        flights_->Start(FlightKey(), flight_hash_, lease_, owns_pin, *this);
    if (admission.outcome == ReadFlightRegistry::Outcome::kJoined) {
      Joined();
      Suspend(awaiting, absl::OkStatus());
      return std::noop_coroutine();
    }
    if (admission.outcome == ReadFlightRegistry::Outcome::kStarted) {
      tag = admission.tag;
      pinned_ = nullptr;
    }
  }
  if (trace_) [[unlikely]]
    trace_->io_submit_ns_ = trace_->NowNanos();
  auto status =
      fixed ? store_.worker_->SubmitReadFixed(store_.files_[file_id_], buffer,
                                              aligned_offset_, tag)
            : store_.worker_->SubmitRead(store_.files_[file_id_],
                                         std::span(buffer.data_, buffer.size_),
                                         aligned_offset_, tag);
  if (!status.ok() && tag != this) {
    flights_->Reject(tag, lease_, owns_pin);
    pinned_ = pinned;
  }
  if (Suspend(awaiting, status)) return std::noop_coroutine();
  if (trace_) [[unlikely]] {
    trace_->io_complete_ns_ = trace_->NowNanos();
    trace_->RecordIo(trace_->io_submit_ns_, trace_->io_complete_ns_);
  }
  if (partition_ && absl::IsAborted(status)) {
    Retry(std::move(status));
    return SuspendRead(awaiting);
  }
  return awaiting;
}

void StorageEngine::Impl::LoadValueAwaitable::Complete(bycorf::Worker& worker,
                                                       int bytes,
                                                       unsigned flags) {
  if (bytes >= 0) worker.RecordStorageReadCompletion(bytes);
  FinishCompletion(worker, bytes, flags);
}

void StorageEngine::Impl::LoadValueAwaitable::ReceiveRead(
    bycorf::Worker& worker, ReadResultBuffer&& buffer, int bytes,
    unsigned flags) {
  completed_buffer_ = std::move(buffer);
  FinishCompletion(worker, bytes, flags);
}

void StorageEngine::Impl::LoadValueAwaitable::FinishCompletion(
    bycorf::Worker& worker, int bytes, unsigned flags) {
  if (trace_) [[unlikely]] {
    trace_->io_complete_ns_ = trace_->NowNanos();
    if (trace_->flight_wait_start_ns_)
      trace_->RecordFlightWait(trace_->flight_wait_start_ns_,
                               trace_->io_complete_ns_);
    else
      trace_->RecordIo(trace_->io_submit_ns_, trace_->io_complete_ns_);
  }
  if (partition_ && bytes >= 0 &&
      static_cast<std::size_t>(bytes) == read_bytes_) {
    // Only metadata is examined on the completion poller. CRC and large-value
    // copies stay in the caller's scheduled slice. A stale identity must select
    // the retry continuation before the caller's co_await can finish.
    auto parsed =
        ParseRecord((completed_buffer_.valid() ? completed_buffer_.io_data()
                                               : lease_.io_buffer().data_) +
                    record_headroom_);
    if (parsed.ok())
      payload_ = *parsed;
    else if (absl::IsAborted(parsed.status()) && !PopulationChanged()) {
      Retry(parsed.status());
      if (!awaiting_) return;
      auto child =
          std::move(slow_).operator co_await().await_suspend(awaiting_);
      // CQEs are polled outside the parent's task class. Preserve background
      // scheduling when a backup/maintenance read starts its retry here.
      if (worker.IsBackground(awaiting_)) worker.RegisterBackground(child);
      worker.Enqueue(child);
      return;
    } else
      ready_.emplace(parsed.status());
  }
  OneShotIoAwaitable::Complete(worker, bytes, flags);
}

StorageEngine::Impl::LoadValueAwaitable::Result
StorageEngine::Impl::LoadValueAwaitable::await_resume() {
  // Fence successful reads here, after the scheduling gap following the CQE.
  // Completion only needs this check when deciding whether to start a retry.
  if (PopulationChanged()) return absl::NotFoundError("key not found");
  if (slow_.valid()) return std::move(slow_).TakeResult();
  if (ready_) return std::move(*ready_);
  auto read =
      Resume(lease_.registered() ? "fixed-buffer read failed" : "read failed");
  if (!read.ok()) return read.status();
  if (static_cast<std::size_t>(*read) != read_bytes_)
    return absl::InternalError("short compact record read");
  const auto* data = (completed_buffer_.valid() ? completed_buffer_.io_data()
                                                : lease_.io_buffer().data_) +
                     record_headroom_;
  if (!payload_) {
    auto parsed = ParseRecord(data);
    if (!parsed.ok()) return parsed.status();
    payload_ = *parsed;
  }
  return FinishRecord(data, *payload_, false);
}

absl::StatusOr<StorageEngine::Impl::LoadValueAwaitable::Payload>
StorageEngine::Impl::LoadValueAwaitable::ParseRecord(
    const std::byte* data) const {
  TraceSpanScope decode(trace_ ? trace_->diagnostics_ : nullptr,
                        TraceStage::kDecode);
  RecordHeader record{};
  std::string_view disk_key;
  if (!DecodeRecordHeader(std::span(data, location_.total_disk_bytes()),
                          &record, &disk_key) ||
      record.db_id_ != db_id_ || (!record.key_indirect_ && disk_key != key_) ||
      record.kind_ != RecordKind::kValue ||
      record.db_epoch_ != (expected_db_epoch_ ? *expected_db_epoch_
                                              : engine_.DbEpoch(db_id_)) ||
      record.mutation_sequence_ != location_.mutation_sequence_ ||
      record.replication_epoch_ != replication_epoch_ ||
      record.allocation_epoch_ != location_.allocation_epoch() ||
      record.expire_at_ms_ != location_.expire_at_ms_ ||
      record.value_type_ != location_.value_type() ||
      record.external_ != location_.external() ||
      record.key_indirect_ != location_.key_indirect() ||
      record.logical_size_ != location_.logical_size_ ||
      record.total_disk_bytes_ != location_.total_disk_bytes()) {
    return absl::AbortedError("record does not match in-memory location");
  }
  if (record.key_indirect_ && record.key_bytes_ != key_.size())
    return absl::AbortedError("record key does not match location");
  if (record.value_type_ == ValueType::kString && !record.grouped_ &&
      !record.auxiliary_group_ && record.payload_bytes_ != record.logical_size_)
    return absl::InternalError("inline value length does not match metadata");
  return Payload{record.txid_, record.header_bytes_, record.payload_bytes_,
                 record.payload_checksum_, record.no_crc_};
}

StorageEngine::Impl::LoadValueAwaitable::Result
StorageEngine::Impl::LoadValueAwaitable::FinishRecord(const std::byte* data,
                                                      Payload payload,
                                                      bool staged) {
  // Staged and chunked reads finish before await_resume; time the work here.
  TraceSpanScope decode(trace_ ? trace_->diagnostics_ : nullptr,
                        TraceStage::kDecode);
  const auto* value = data + payload.offset_;
  if (!payload.no_crc_ &&
      Crc32c(std::span(value, payload.bytes_)) != payload.checksum_)
    return absl::InternalError("record value checksum mismatch");
  if (completed_buffer_.valid()) {
    const auto offset =
        static_cast<std::size_t>(value - completed_buffer_.bytes().data());
    if (trace_) [[unlikely]]
      trace_->decode_done_ns_ = trace_->NowNanos();
    return LoadedValue{std::move(completed_buffer_), offset, payload.bytes_,
                       payload.txid_};
  }
  if (staged || payload.bytes_ > engine_.DirectGetValueLimit()) {
    std::memmove(lease_.io_buffer().data_, value, payload.bytes_);
    value = lease_.io_buffer().data_;
  }
  const auto offset = static_cast<std::size_t>(value - lease_.bytes().data());
  if (trace_) [[unlikely]]
    trace_->decode_done_ns_ = trace_->NowNanos();
  return LoadedValue{std::move(lease_), offset, payload.bytes_, payload.txid_};
}

absl::StatusOr<CompactReadValue> StorageEngine::Impl::DecodeRemoteValue(
    ReadResultBuffer lease, std::uint8_t db_id, std::string_view key,
    RecordLocation location, std::uint64_t replication_epoch,
    std::size_t record_headroom, std::uint64_t expected_db_epoch) {
  const auto* io_data = lease.io_data();
  RecordHeader record{};
  std::string_view disk_key;
  const std::byte* record_data = io_data + record_headroom;
  std::span<const std::byte> record_bytes(record_data,
                                          location.total_disk_bytes());
  if (!DecodeRecordHeader(record_bytes, &record, &disk_key) ||
      record.db_id_ != db_id || (!record.key_indirect_ && disk_key != key) ||
      record.kind_ != RecordKind::kValue ||
      record.db_epoch_ != expected_db_epoch ||
      record.mutation_sequence_ != location.mutation_sequence_ ||
      record.replication_epoch_ != replication_epoch ||
      record.allocation_epoch_ != location.allocation_epoch() ||
      record.expire_at_ms_ != location.expire_at_ms_ ||
      record.value_type_ != location.value_type() ||
      record.external_ != location.external() ||
      record.key_indirect_ != location.key_indirect() ||
      record.logical_size_ != location.logical_size_ ||
      record.total_disk_bytes_ != location.total_disk_bytes()) {
    return absl::Status(absl::StatusCode::kAborted,
                        "record does not match in-memory location");
  }
  const std::byte* payload_data = record_data + record.header_bytes_;
  if (record.key_indirect_ && record.key_bytes_ != key.size()) {
    return absl::Status(absl::StatusCode::kAborted,
                        "record key does not match location");
  }
  const std::size_t value_bytes = record.payload_bytes_;
  if (record.value_type_ == ValueType::kString && !record.grouped_ &&
      !record.auxiliary_group_ && value_bytes != record.logical_size_) {
    return absl::Status(absl::StatusCode::kInternal,
                        "inline value length does not match metadata");
  }
  if (!ValidateRecordPayload(
          record,
          std::span<const std::byte>(payload_data, record.payload_bytes_))) {
    return absl::Status(absl::StatusCode::kInternal,
                        "record value checksum mismatch");
  }
  const std::byte* framed_value = payload_data;
  if (!lease.shared() && value_bytes > DirectGetValueLimit()) {
    auto exclusive = lease.TakeExclusive();
    std::memmove(exclusive.io_buffer().data_, payload_data, value_bytes);
    framed_value = exclusive.io_buffer().data_;
    lease = ReadResultBuffer(std::move(exclusive));
  }
  const std::size_t value_offset =
      static_cast<std::size_t>(framed_value - lease.bytes().data());
  return CompactReadValue{
      .lease_ = std::move(lease),
      .value_offset_ = static_cast<std::uint32_t>(value_offset),
      .value_bytes_ = static_cast<std::uint32_t>(value_bytes),
      .logical_size_ = location.logical_size_,
      .expire_at_ms_ = location.expire_at_ms_};
}

}  // namespace lavik::storage
