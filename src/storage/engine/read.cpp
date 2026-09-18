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
#include "lavik/random_sample.h"

namespace lavik::storage {
namespace {

class BatchReadAwaiter;

// One address-stable io_uring/SPDK tag. A whole MGET shard owns a vector of
// these ordinary objects and has only one awaiting coroutine.
struct BatchReadOperation final : bycorf::IoCompletion {
  void Complete(Worker& worker, int result, unsigned flags) override;

  BatchReadAwaiter* batch_ = nullptr;
  Worker* worker_ = nullptr;
  FixedFile file_{};
  FixedBuffer buffer_{};
  std::uint64_t offset_ = 0;
  bool registered_ = false;
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
    remaining_ = operations_.size();
    for (BatchReadOperation* operation : operations_) {
      operation->batch_ = this;
      operation->submit_status_ =
          operation->registered_
              ? operation->worker_->SubmitReadFixed(
                    operation->file_, operation->buffer_, operation->offset_,
                    operation)
              : operation->worker_->SubmitRead(
                    operation->file_,
                    std::span<std::byte>(operation->buffer_.data_,
                                         operation->buffer_.size_),
                    operation->offset_, operation);
      if (!operation->submit_status_.ok()) {
        --remaining_;
      }
    }
    // A full io_uring SQ may submit and reap earlier operations while this
    // loop is still preparing the batch. Unvisited operations are already in
    // remaining_, so those completions cannot resume us early. The operation
    // passed to each SubmitRead is itself guaranteed to complete
    // asynchronously.
    return remaining_ != 0;
  }

  void await_resume() const noexcept {}

  void Complete(Worker& worker) noexcept {
    assert(remaining_ != 0);
    if (--remaining_ == 0) worker.Enqueue(awaiting_);
  }

 private:
  std::span<BatchReadOperation*> operations_;
  std::coroutine_handle<> awaiting_{};
  std::size_t remaining_ = 0;
};

void BatchReadOperation::Complete(Worker& worker, int result, unsigned flags) {
  (void)flags;
  result_ = result;
  batch_->Complete(worker);
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
    const ExtentManifest extents = ExtentsFor(store, selected);
    const std::uint32_t key_bytes = selected->logical_key_size();
    const std::uint64_t index_generation = store.index_generations_[db_id];
    const std::uint64_t db_epoch = DbEpoch(db_id);
    const std::uint64_t replication_epoch = partition.replication_epoch_;
    auto loaded =
        co_await LoadOutOfIndexKey(store, location, extents, key_bytes);
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
    key_lock = co_await tx::CurrentTxShard().AcquireKey(
        db_id, tx::FingerprintOf(digest), tx::LockMode::kShared);
  }
  WorkerStore& store = CurrentStore();
  auto& partition = routed_partition_id.has_value()
                        ? PartitionFor(store, *routed_partition_id)
                        : PartitionForKey(store, key);
  while (true) {
    auto& index = partition.indexes_[db_id];
    auto* found = index.Find(digest, key);
    if (found != nullptr && !found->key_complete()) [[unlikely]] {
      if (optimistic_read) {
        // Verifying an out-of-index key suspends before we know this is the
        // requested key, so there is no lock-free linearization point yet.
        key_lock = co_await tx::CurrentTxShard().AcquireKey(
            db_id, tx::FingerprintOf(digest), tx::LockMode::kShared);
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
      if (trace != nullptr) {
        trace->lookup_done_ns_ = ReadTraceNowNanos();
      }
      co_return absl::Status(absl::StatusCode::kNotFound, "key not found");
    }
    if (IsExpiredNow(*found)) {
      QueueExpiredCandidate(store, partition.id_, db_id, *found, key);
      if (trace != nullptr) {
        trace->lookup_done_ns_ = ReadTraceNowNanos();
      }
      co_return absl::Status(absl::StatusCode::kNotFound, "key not found");
    }
    if (found->value_.value_type() != ValueType::kString) {
      co_return absl::Status(
          absl::StatusCode::kInvalidArgument,
          "WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    if (trace != nullptr) {
      trace->hit_ = true;
      trace->lookup_done_ns_ = ReadTraceNowNanos();
    }

    auto loaded = co_await LoadValue(store, partition, db_id, key, digest,
                                     MaterializeIndexLocation(*found),
                                     ExtentsFor(store, found), trace);
    if (optimistic_read && !loaded.ok() &&
        (loaded.status().code() == absl::StatusCode::kNotFound ||
         loaded.status().code() == absl::StatusCode::kAborted)) {
      // The key existed at the optimistic lookup, but a concurrent writer may
      // retire its physical record before the I/O path pins it. Do not turn
      // that race into a spurious nil; take the original lock path and reread.
      key_lock = co_await tx::CurrentTxShard().AcquireKey(
          db_id, tx::FingerprintOf(digest), tx::LockMode::kShared);
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
    std::uint8_t db_id, std::span<const BatchGetRequest> requests) {
  assert(db_id < kLogicalDatabaseCount);
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
    if (location.external() || location.block_owner() != store.worker_->id() ||
        location.in_memory()) {
      fallbacks.push_back(i);
      continue;
    }
    candidates.push_back(Candidate{
        .result_index_ = i,
        .partition_ = &partition,
        .location_ = location,
        .replication_epoch_ = partition.replication_epoch_,
    });
  }

  struct PendingRead {
    PendingRead(WorkerStore* store, BlockState* state, Candidate candidate,
                ReadBufferLease lease, std::size_t record_headroom,
                std::size_t read_bytes)
        : store_(store),
          state_(state),
          candidate_(candidate),
          lease_(std::move(lease)),
          record_headroom_(record_headroom),
          read_bytes_(read_bytes) {
      ++state_->pins_;
    }

    PendingRead(const PendingRead&) = delete;
    PendingRead& operator=(const PendingRead&) = delete;

    ~PendingRead() {
      --state_->pins_;
      if (state_->pins_ == 0 && state_->release_pending_) {
        StorageEngine::Impl::ReleaseStagingBuffer(*store_, *state_);
      }
    }

    WorkerStore* store_ = nullptr;
    BlockState* state_ = nullptr;
    Candidate candidate_{};
    ReadBufferLease lease_;
    std::size_t record_headroom_ = 0;
    std::size_t read_bytes_ = 0;
    BatchReadOperation operation_;
  };

  std::size_t next = 0;
  while (next < candidates.size()) {
    std::vector<std::unique_ptr<PendingRead>> wave;
    std::vector<BatchReadOperation*> operations;
    wave.reserve(
        std::min(candidates.size() - next, store.buffers_.read_buffer_count()));

    while (next < candidates.size()) {
      const Candidate candidate = candidates[next];
      const RecordLocation& location = candidate.location_;
      const std::uint64_t absolute_offset =
          FileOffset(location.block_id()).second + location.record_offset();
      const std::uint64_t direct_io_mask =
          static_cast<std::uint64_t>(direct_io_alignment_ - 1);
      const std::uint64_t aligned_offset = absolute_offset & ~direct_io_mask;
      const std::size_t record_headroom =
          static_cast<std::size_t>(absolute_offset - aligned_offset);
      const std::size_t record_span =
          record_headroom + location.total_disk_bytes();
      const std::size_t read_bytes =
          (record_span + direct_io_alignment_ - 1) & ~direct_io_mask;
      const bool ordinary_buffer =
          read_bytes <= options_.buffers_.read_payload_bytes_;
      if (!wave.empty() && ordinary_buffer &&
          store.buffers_.available_read_buffers() == 0) {
        break;
      }

      auto acquired = co_await store.buffers_.AcquireReadBuffer(read_bytes);
      if (!acquired.ok()) {
        results[candidate.result_index_] = acquired.status();
        ++next;
        continue;
      }
      ReadBufferLease lease = std::move(*acquired);
      BlockState* state = FindBlockState(store, location.block_id());
      if (state == nullptr || !state->allocated_ || state->freeing_ ||
          state->allocation_epoch_ != location.allocation_epoch() ||
          (location.in_memory() && state->in_memory_)) {
        fallbacks.push_back(candidate.result_index_);
        ++next;
        continue;
      }

      auto pending = std::make_unique<PendingRead>(&store, state, candidate,
                                                   std::move(lease),
                                                   record_headroom, read_bytes);
      FixedBuffer io = pending->lease_.io_buffer();
      if (read_bytes > io.size_) {
        results[candidate.result_index_] = absl::OutOfRangeError(
            "record exceeds registered read buffer capacity");
        ++next;
        continue;
      }
      const auto [file_id, unused_block_offset] =
          FileOffset(location.block_id());
      (void)unused_block_offset;
      pending->operation_.worker_ = store.worker_;
      pending->operation_.file_ = store.files_[file_id];
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
          pending->candidate_.replication_epoch_) [[unlikely]] {
        results[index] = std::optional<std::string>{};
        continue;
      }

      const RecordLocation& location = pending->candidate_.location_;
      const FixedBuffer io = pending->lease_.io_buffer();
      const std::byte* record_data = io.data_ + pending->record_headroom_;
      const std::span<const std::byte> record_bytes(
          record_data, location.total_disk_bytes());
      RecordHeader record{};
      std::string_view disk_key;
      if (!DecodeRecordHeader(record_bytes, &record, &disk_key) ||
          record.db_id_ != db_id ||
          (!record.key_external_ && disk_key != request.key_) ||
          record.kind_ != RecordKind::kValue ||
          record.db_epoch_ != DbEpoch(db_id) ||
          record.mutation_sequence_ != location.mutation_sequence_ ||
          record.replication_epoch_ != pending->candidate_.replication_epoch_ ||
          record.allocation_epoch_ != location.allocation_epoch() ||
          record.expire_at_ms_ != location.expire_at_ms_ ||
          record.value_type_ != location.value_type() ||
          record.external_ != location.external() ||
          record.key_external_ != location.key_external() ||
          record.logical_size_ != location.logical_size_ ||
          record.total_disk_bytes_ != location.total_disk_bytes()) {
        retry.push_back(index);
        continue;
      }
      const std::byte* payload_data = record_data + record.header_bytes_;
      const std::size_t key_prefix =
          record.key_external_ ? record.key_bytes_ : 0;
      if (record.payload_bytes_ < key_prefix ||
          (record.key_external_ &&
           (record.key_bytes_ != request.key_.size() ||
            std::memcmp(payload_data, request.key_.data(),
                        request.key_.size()) != 0))) {
        retry.push_back(index);
        continue;
      }
      const std::size_t value_bytes = record.payload_bytes_ - key_prefix;
      if (value_bytes != record.logical_size_) {
        results[index] =
            absl::InternalError("inline string length does not match metadata");
        continue;
      }
      if (Crc32c(std::span<const std::byte>(payload_data,
                                            record.payload_bytes_)) !=
          record.payload_checksum_) {
        results[index] = absl::InternalError("record value checksum mismatch");
        continue;
      }
      const char* value =
          reinterpret_cast<const char*>(payload_data + key_prefix);
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
        co_await GetLocked(db_id, request.key_, request.digest_, nullptr);
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
    std::uint8_t db_id, std::string_view key) {
  assert(db_id < kLogicalDatabaseCount);
  const Digest digest = ComputeDigest(key);
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest), tx::LockMode::kShared);
  co_return co_await StringLengthLocked(db_id, key, digest);
}

Task<absl::StatusOr<std::uint64_t>> StorageEngine::Impl::StringLengthLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest) {
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
  const bool expired = found != nullptr && IsExpiredNow(*found);
  if (found == nullptr || found->value_.kind() != RecordKind::kValue ||
      expired) {
    if (expired) {
      QueueExpiredCandidate(store, partition.id_, db_id, *found, key);
    }
    co_return absl::Status(absl::StatusCode::kNotFound, "key not found");
  }
  if (found->value_.value_type() != ValueType::kString) {
    co_return absl::Status(
        absl::StatusCode::kInvalidArgument,
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  }
  co_return found->value_.logical_size();
}

Task<absl::StatusOr<ExpirationInfo>> StorageEngine::ReadKeyMetadata(
    std::uint8_t db_id, std::string_view key) {
  return impl_->ReadKeyMetadata(db_id, key);
}

Task<absl::StatusOr<ExpirationInfo>> StorageEngine::ReadKeyMetadataLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest) {
  return impl_->ReadKeyMetadataLocked(db_id, key, digest);
}

Task<absl::StatusOr<ExpirationInfo>> StorageEngine::Impl::ReadKeyMetadata(
    std::uint8_t db_id, std::string_view key) {
  if (db_id >= options_.database_count_)
    co_return absl::InvalidArgumentError("invalid logical database");
  const auto digest = ComputeDigest(key);
  auto hold = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest), tx::LockMode::kShared);
  co_return co_await ReadKeyMetadataLocked(db_id, key, digest);
}

Task<absl::StatusOr<ExpirationInfo>> StorageEngine::Impl::ReadKeyMetadataLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest) {
  if (db_id >= options_.database_count_ || digest != ComputeDigest(key))
    co_return absl::InvalidArgumentError("invalid metadata key identity");
  auto& store = CurrentStore();
  auto& partition = PartitionForKey(store, key);
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
      db_id, tx::FingerprintOf(digest), tx::LockMode::kShared);
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
    std::uint8_t db_id, std::string_view key, const Digest& digest) {
  assert(db_id < kLogicalDatabaseCount);
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionForKey(store, key);
  co_await store.store_state_mutex_.Lock();
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

  auto loaded = co_await LoadValue(store, partition, db_id, key, digest,
                                   location, extents);
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
      db_id, tx::FingerprintOf(digest), tx::LockMode::kShared);
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
      db_id, tx::FingerprintOf(digest), tx::LockMode::kShared);
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
  const RegisteredBufferPoolOptions& buffers = options_.buffers_;
  // Keep the direct-from-disk framing path conservative: with the defaults,
  // values through 1 MiB - 8 KiB avoid the value copy. Larger values retain
  // the materializing memmove path below.
  const std::size_t framing_reserve =
      buffers.read_headroom_bytes_ + buffers.read_tailroom_bytes_;
  return buffers.read_payload_bytes_ > framing_reserve
             ? buffers.read_payload_bytes_ - framing_reserve
             : 0;
}

absl::StatusOr<DiskValue> StorageEngine::Impl::EncodeDiskValue(
    LoadedValue loaded) {
  ReadBufferLease lease = std::move(loaded.lease_);
  const std::size_t value_offset = loaded.value_offset_;
  const std::size_t value_bytes = loaded.value_bytes_;
  std::span<std::byte> buffer = lease.bytes();
  char length[32];
  auto [end, error] =
      std::to_chars(length, length + sizeof(length), value_bytes);
  if (error != std::errc{}) {
    return absl::Status(absl::StatusCode::kInternal,
                        "bulk length formatting failed");
  }
  const std::size_t digits = static_cast<std::size_t>(end - length);
  const std::size_t prefix_bytes = digits + 3;
  if (value_offset < prefix_bytes || value_offset > buffer.size() ||
      value_bytes > buffer.size() - value_offset ||
      buffer.size() - value_offset - value_bytes < 2) {
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
  return DiskValue(std::move(lease), network_offset,
                   prefix_bytes + value_bytes + 2);
}

Task<absl::StatusOr<StorageEngine::Impl::LoadedValue>>
StorageEngine::Impl::LoadValue(WorkerStore& key_store,
                               WorkerStore::PartitionStore& partition,
                               std::uint8_t db_id, std::string_view key,
                               const Digest& digest, RecordLocation location,
                               ExtentManifest extents, ReadLatencyTrace* trace,
                               GroupedHashObject::Handle grouped_snapshot) {
  if (location.grouped()) {
    co_return co_await LoadGroupedValue(key_store, partition, db_id, key,
                                        digest, location,
                                        std::move(grouped_snapshot));
  }
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
    // Every branch below assigns `loaded` before it is observed. Keep the
    // required placeholder message-free so the common local GET path does not
    // construct and then immediately discard an error payload.
    absl::StatusOr<LoadedValue> loaded;

    // An external value's manifest is already decoded in this index entry,
    // so the record's own block holds nothing worth reading. Assemble here
    // and let each extent go straight to its block's owner, rather than
    // hopping to the record's owner first and having it acquire the output
    // buffer from its pool and hand the lease back across workers.
    if (location.external()) {
      loaded = co_await LoadExternalValueLocal(
          key_store, location, std::move(extents), key.size(), trace);
    } else if (location.block_owner() == key_store.worker_->id()) {
      loaded = co_await LoadValueLocal(key_store, db_id, key, location,
                                       replication_epoch, trace, db_epoch);
    } else {
      const unsigned owner = location.block_owner();
      std::string owned_key(key);
      loaded = co_await bycorf::SubmitTaskTo(
          owner,
          [this, owner, db_id, key = std::move(owned_key), location,
           replication_epoch, db_epoch,
           trace]() mutable -> Task<absl::StatusOr<LoadedValue>> {
            co_return co_await LoadValueLocal(*stores_[owner], db_id, key,
                                              location, replication_epoch,
                                              trace, db_epoch);
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

Task<absl::Status> StorageEngine::Impl::ReadExtentInto(
    WorkerStore& store, ExtentRef ref, std::uint32_t extent_index,
    std::byte* destination) {
  BlockState* state = FindBlockState(store, ref.block_id_);
  if (state == nullptr || !state->allocated_ || state->freeing_ ||
      state->kind_ != BlockKind::kPayloadExtent ||
      state->allocation_epoch_ != ref.allocation_epoch_) {
    co_return absl::Status(absl::StatusCode::kAborted,
                           "stale or missing external extent");
  }
  ++state->pins_;
  struct ExtentPin {
    BlockState* state_;
    ~ExtentPin() { --state_->pins_; }
  } pin{state};
  const std::size_t read_bytes =
      AlignDirect(kBlockHeaderBytes + ref.payload_bytes_);
  auto temp_acquired = co_await store.buffers_.AcquireReadBuffer(read_bytes);
  if (!temp_acquired.ok()) {
    co_return temp_acquired.status();
  }
  ReadBufferLease temp = std::move(*temp_acquired);
  FixedBuffer io = temp.io_buffer();
  io.size_ = read_bytes;
  const auto [file_id, block_offset] = FileOffset(ref.block_id_);
  auto read = co_await ReadStorageBuffer(*store.worker_, store.files_[file_id],
                                         io, temp.registered(), block_offset);
  if (!read.ok()) {
    co_return read.status();
  }
  if (*read != read_bytes) {
    co_return absl::Status(absl::StatusCode::kInternal,
                           "short extent block read");
  }
  BlockHeader header{};
  if (!DecodeBlockHeaderPages(std::span<const std::byte, kBlockHeaderBytes>(
                                  io.data_, kBlockHeaderBytes),
                              &header) ||
      header.kind_ != BlockKind::kPayloadExtent ||
      header.block_id_ != ref.block_id_ ||
      header.allocation_epoch_ != ref.allocation_epoch_ ||
      header.extent_index_ != extent_index ||
      header.extent_payload_bytes_ != ref.payload_bytes_ ||
      header.extent_payload_checksum_ != ref.payload_checksum_) {
    co_return absl::Status(absl::StatusCode::kInternal,
                           "extent header does not match manifest");
  }
  const auto payload = std::span<const std::byte>(io.data_ + kBlockHeaderBytes,
                                                  ref.payload_bytes_);
  if (Crc32c(payload) != ref.payload_checksum_) {
    co_return absl::Status(absl::StatusCode::kInternal,
                           "extent payload checksum mismatch");
  }
  std::memcpy(destination, payload.data(), payload.size());
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::ReadExtentSlice(
    WorkerStore& store, ExtentRef ref, std::uint32_t extent_index,
    std::size_t source_offset, std::span<std::byte> destination) {
  if (source_offset > ref.payload_bytes_ ||
      destination.size() > ref.payload_bytes_ - source_offset) {
    co_return absl::InvalidArgumentError("extent slice is out of range");
  }
  BlockState* state = FindBlockState(store, ref.block_id_);
  if (state == nullptr || !state->allocated_ || state->freeing_ ||
      state->kind_ != BlockKind::kPayloadExtent ||
      state->allocation_epoch_ != ref.allocation_epoch_) {
    co_return absl::AbortedError("stale or missing external extent");
  }
  ++state->pins_;
  struct ExtentPin {
    BlockState* state_;
    ~ExtentPin() { --state_->pins_; }
  } pin{state};
  const std::size_t read_bytes =
      AlignDirect(kBlockHeaderBytes + ref.payload_bytes_);
  auto acquired = co_await store.buffers_.AcquireReadBuffer(read_bytes);
  if (!acquired.ok()) co_return acquired.status();
  ReadBufferLease buffer = std::move(*acquired);
  FixedBuffer io = buffer.io_buffer();
  io.size_ = read_bytes;
  const auto [file_id, block_offset] = FileOffset(ref.block_id_);
  auto read = co_await ReadStorageBuffer(*store.worker_, store.files_[file_id],
                                         io, buffer.registered(), block_offset);
  if (!read.ok()) co_return read.status();
  if (*read != read_bytes) {
    co_return absl::InternalError("short extent block read");
  }
  BlockHeader header{};
  if (!DecodeBlockHeaderPages(std::span<const std::byte, kBlockHeaderBytes>(
                                  io.data_, kBlockHeaderBytes),
                              &header) ||
      header.kind_ != BlockKind::kPayloadExtent ||
      header.block_id_ != ref.block_id_ ||
      header.allocation_epoch_ != ref.allocation_epoch_ ||
      header.extent_index_ != extent_index ||
      header.extent_payload_bytes_ != ref.payload_bytes_ ||
      header.extent_payload_checksum_ != ref.payload_checksum_) {
    co_return absl::InternalError("extent header does not match manifest");
  }
  const auto payload = std::span<const std::byte>(io.data_ + kBlockHeaderBytes,
                                                  ref.payload_bytes_);
  if (Crc32c(payload) != ref.payload_checksum_) {
    co_return absl::InternalError("extent payload checksum mismatch");
  }
  std::memcpy(destination.data(), payload.data() + source_offset,
              destination.size());
  co_return absl::OkStatus();
}

Task<absl::StatusOr<std::string>> StorageEngine::Impl::LoadExternalKey(
    WorkerStore& store, ExtentManifest extents, std::size_t key_bytes) {
  if (extents == nullptr || key_bytes == 0 || key_bytes > MaxKeyBytes()) {
    co_return absl::Status(absl::StatusCode::kInternal,
                           "external key has no valid extent manifest");
  }
  std::string key;
  key.resize(key_bytes);
  std::size_t offset = 0;
  for (std::size_t index = 0; index < extents->size() && offset < key.size();
       ++index) {
    const ExtentRef& ref = extents->at(index);
    const std::uint16_t owner = BlockOwner(ref.block_id_);
    if (owner >= worker_count_) {
      co_return absl::Status(absl::StatusCode::kInternal,
                             "stale or missing external key extent");
    }
    const std::size_t remaining = key.size() - offset;
    std::vector<std::byte> partial;
    std::byte* destination = nullptr;
    if (ref.payload_bytes_ <= remaining) {
      destination = reinterpret_cast<std::byte*>(key.data() + offset);
    } else {
      try {
        partial.resize(ref.payload_bytes_);
      } catch (const std::length_error&) {
        co_return absl::ResourceExhaustedError(
            "external key extent buffer is too large");
      }
      destination = partial.data();
    }
    absl::Status read = absl::OkStatus();
    if (owner == store.worker_->id()) {
      read = co_await ReadExtentInto(
          store, ref, static_cast<std::uint32_t>(index), destination);
    } else {
      read = co_await bycorf::SubmitTaskTo(
          owner,
          [this, owner, ref, index, destination]() -> Task<absl::Status> {
            co_return co_await ReadExtentInto(*stores_[owner], ref,
                                              static_cast<std::uint32_t>(index),
                                              destination);
          });
    }
    if (!read.ok()) {
      co_return read;
    }
    const std::size_t copy_bytes =
        std::min<std::size_t>(ref.payload_bytes_, key.size() - offset);
    if (!partial.empty()) {
      std::memcpy(key.data() + offset, partial.data(), copy_bytes);
    }
    offset += copy_bytes;
  }
  if (offset != key.size()) {
    co_return absl::Status(absl::StatusCode::kInternal,
                           "external key manifest is truncated");
  }
  co_return key;
}

Task<absl::StatusOr<std::string>> StorageEngine::Impl::LoadOutOfIndexKey(
    WorkerStore& store, const RecordLocation& location, ExtentManifest extents,
    std::size_t key_bytes) {
  if (location.external()) {
    co_return co_await LoadExternalKey(store, std::move(extents), key_bytes);
  }
  if (location.block_owner() == store.worker_->id()) {
    co_return co_await LoadInlineRecordKeyLocal(store, location, key_bytes);
  }
  const unsigned owner = location.block_owner();
  if (owner >= worker_count_) {
    co_return absl::Status(absl::StatusCode::kInternal,
                           "inline key block has no owner");
  }
  co_return co_await bycorf::SubmitTaskTo(
      owner,
      [this, owner, location,
       key_bytes]() -> Task<absl::StatusOr<std::string>> {
        co_return co_await LoadInlineRecordKeyLocal(*stores_[owner], location,
                                                    key_bytes);
      });
}

Task<absl::StatusOr<std::string>> StorageEngine::Impl::LoadInlineRecordKeyLocal(
    WorkerStore& store, const RecordLocation& location, std::size_t key_bytes) {
  if (!location.key_external() || location.external() || key_bytes == 0 ||
      key_bytes > MaxKeyBytes()) {
    co_return absl::Status(absl::StatusCode::kInternal,
                           "key is not stored in the record payload");
  }
  BlockState* state = FindBlockState(store, location.block_id());
  if (state == nullptr || !state->allocated_ || state->freeing_ ||
      state->allocation_epoch_ != location.allocation_epoch()) {
    co_return absl::Status(absl::StatusCode::kAborted,
                           "stale inline key record");
  }
  const std::byte* record_data = nullptr;
  ReadBufferLease lease;
  if (location.in_memory() && state->in_memory_) [[unlikely]] {
    const FixedBuffer staging = StagingBufferFor(store, *state);
    if (staging.data_ == nullptr ||
        location.record_offset() + location.total_disk_bytes() >
            staging.size_) {
      co_return absl::Status(absl::StatusCode::kInternal,
                             "invalid staged inline key record");
    }
    record_data = staging.data_ + location.record_offset();
  } else {
    const auto [file_id, block_offset] = FileOffset(location.block_id());
    const std::uint64_t absolute_offset =
        block_offset + location.record_offset();
    const std::uint64_t mask = direct_io_alignment_ - 1;
    const std::uint64_t aligned_offset = absolute_offset & ~mask;
    const std::size_t headroom =
        static_cast<std::size_t>(absolute_offset - aligned_offset);
    const std::size_t read_bytes =
        (headroom + location.total_disk_bytes() + direct_io_alignment_ - 1) &
        ~static_cast<std::size_t>(mask);
    ++state->pins_;
    struct PinGuard {
      WorkerStore* store_;
      BlockState* state_;
      ~PinGuard() {
        --state_->pins_;
        if (state_->pins_ == 0 && state_->release_pending_) {
          ReleaseStagingBuffer(*store_, *state_);
        }
      }
    } pin{&store, state};
    auto acquired = co_await store.buffers_.AcquireReadBuffer(read_bytes);
    if (!acquired.ok()) {
      co_return acquired.status();
    }
    lease = std::move(*acquired);
    FixedBuffer io = lease.io_buffer();
    io.size_ = read_bytes;
    auto read =
        co_await ReadStorageBuffer(*store.worker_, store.files_[file_id], io,
                                   lease.registered(), aligned_offset);
    if (!read.ok()) {
      co_return read.status();
    }
    if (*read != read_bytes) {
      co_return absl::Status(absl::StatusCode::kInternal,
                             "short inline key record read");
    }
    record_data = io.data_ + headroom;
  }
  RecordHeader record{};
  std::string_view header_key;
  if (!DecodeRecordHeader(
          std::span<const std::byte>(record_data, location.total_disk_bytes()),
          &record, &header_key) ||
      !record.key_external_ || record.external_ ||
      record.key_bytes_ != key_bytes || record.payload_bytes_ < key_bytes ||
      record.allocation_epoch_ != location.allocation_epoch() ||
      record.mutation_sequence_ != location.mutation_sequence_) {
    co_return absl::Status(absl::StatusCode::kAborted,
                           "inline key record changed");
  }
  const char* key_data =
      reinterpret_cast<const char*>(record_data + record.header_bytes_);
  co_return std::string(key_data, key_bytes);
}

Task<absl::StatusOr<bool>> StorageEngine::Impl::VerifyExternalKey(
    WorkerStore& store, const RecordIndex::Entry& entry, std::string_view key) {
  if (entry.key_complete()) [[likely]] {
    co_return entry.key() == key;
  }
  if (!entry.value_.key_external() || entry.logical_key_size() != key.size())
      [[unlikely]] {
    co_return false;
  }
  if (!entry.value_.external()) {
    co_return co_await VerifyInlineRecordKey(
        store, MaterializeIndexLocation(entry), key);
  }
  co_return co_await VerifyExternalKeyExtents(store, ExtentsFor(store, &entry),
                                              key);
}

Task<absl::StatusOr<bool>> StorageEngine::Impl::VerifyExternalKeyExtents(
    WorkerStore& store, ExtentManifest extents, std::string_view key) {
  if (extents == nullptr) {
    co_return absl::Status(absl::StatusCode::kInternal,
                           "external key manifest is missing");
  }
  std::vector<std::byte> buffer;
  std::size_t offset = 0;
  for (std::size_t index = 0; index < extents->size() && offset < key.size();
       ++index) {
    const ExtentRef& ref = extents->at(index);
    buffer.resize(ref.payload_bytes_);
    const std::uint16_t owner = BlockOwner(ref.block_id_);
    if (owner >= worker_count_) {
      co_return absl::Status(absl::StatusCode::kInternal,
                             "stale or missing external key extent");
    }
    absl::Status read = absl::OkStatus();
    if (owner == store.worker_->id()) {
      read = co_await ReadExtentInto(
          store, ref, static_cast<std::uint32_t>(index), buffer.data());
    } else {
      read = co_await bycorf::SubmitTaskTo(
          owner,
          [this, owner, ref, index,
           destination = buffer.data()]() -> Task<absl::Status> {
            co_return co_await ReadExtentInto(*stores_[owner], ref,
                                              static_cast<std::uint32_t>(index),
                                              destination);
          });
    }
    if (!read.ok()) {
      co_return read;
    }
    const std::size_t compare_bytes =
        std::min<std::size_t>(ref.payload_bytes_, key.size() - offset);
    if (std::memcmp(buffer.data(), key.data() + offset, compare_bytes) != 0) {
      co_return false;
    }
    offset += compare_bytes;
  }
  co_return offset == key.size();
}

Task<absl::StatusOr<bool>> StorageEngine::Impl::VerifyInlineRecordKey(
    WorkerStore& store, const RecordLocation& location, std::string_view key) {
  if (location.block_owner() == store.worker_->id()) {
    co_return co_await VerifyInlineRecordKeyLocal(store, location, key);
  }
  const unsigned owner = location.block_owner();
  if (owner >= worker_count_) {
    co_return absl::Status(absl::StatusCode::kInternal,
                           "inline key block has no owner");
  }
  std::string owned_key(key);
  co_return co_await bycorf::SubmitTaskTo(
      owner,
      [this, owner, location,
       key = std::move(owned_key)]() -> Task<absl::StatusOr<bool>> {
        co_return co_await VerifyInlineRecordKeyLocal(*stores_[owner], location,
                                                      key);
      });
}

Task<absl::StatusOr<bool>> StorageEngine::Impl::VerifyInlineRecordKeyLocal(
    WorkerStore& store, const RecordLocation& location, std::string_view key) {
  if (!location.key_external() || location.external()) {
    co_return absl::Status(absl::StatusCode::kInternal,
                           "key is not stored in the record payload");
  }
  BlockState* state = FindBlockState(store, location.block_id());
  if (state == nullptr || !state->allocated_ || state->freeing_ ||
      state->allocation_epoch_ != location.allocation_epoch()) {
    co_return absl::Status(absl::StatusCode::kAborted,
                           "stale inline key record");
  }
  const std::byte* record_data = nullptr;
  ReadBufferLease lease;
  if (location.in_memory() && state->in_memory_) [[unlikely]] {
    const FixedBuffer staging = StagingBufferFor(store, *state);
    if (staging.data_ == nullptr ||
        location.record_offset() + location.total_disk_bytes() >
            staging.size_) {
      co_return absl::Status(absl::StatusCode::kInternal,
                             "invalid staged inline key record");
    }
    record_data = staging.data_ + location.record_offset();
  } else {
    const auto [file_id, block_offset] = FileOffset(location.block_id());
    const std::uint64_t absolute_offset =
        block_offset + location.record_offset();
    const std::uint64_t mask = direct_io_alignment_ - 1;
    const std::uint64_t aligned_offset = absolute_offset & ~mask;
    const std::size_t headroom =
        static_cast<std::size_t>(absolute_offset - aligned_offset);
    const std::size_t read_bytes =
        (headroom + location.total_disk_bytes() + direct_io_alignment_ - 1) &
        ~static_cast<std::size_t>(mask);
    ++state->pins_;
    struct PinGuard {
      WorkerStore* store_;
      BlockState* state_;
      ~PinGuard() {
        --state_->pins_;
        if (state_->pins_ == 0 && state_->release_pending_) {
          ReleaseStagingBuffer(*store_, *state_);
        }
      }
    } pin{&store, state};
    auto acquired = co_await store.buffers_.AcquireReadBuffer(read_bytes);
    if (!acquired.ok()) {
      co_return acquired.status();
    }
    lease = std::move(*acquired);
    FixedBuffer io = lease.io_buffer();
    io.size_ = read_bytes;
    auto read =
        co_await ReadStorageBuffer(*store.worker_, store.files_[file_id], io,
                                   lease.registered(), aligned_offset);
    if (!read.ok()) {
      co_return read.status();
    }
    if (*read != read_bytes) {
      co_return absl::Status(absl::StatusCode::kInternal,
                             "short inline key record read");
    }
    record_data = io.data_ + headroom;
  }
  RecordHeader record{};
  std::string_view header_key;
  const auto record_bytes =
      std::span<const std::byte>(record_data, location.total_disk_bytes());
  if (!DecodeRecordHeader(record_bytes, &record, &header_key) ||
      !record.key_external_ || record.external_ ||
      record.key_bytes_ != key.size() ||
      record.payload_bytes_ < record.key_bytes_ ||
      record.allocation_epoch_ != location.allocation_epoch() ||
      record.mutation_sequence_ != location.mutation_sequence_) {
    co_return absl::Status(absl::StatusCode::kAborted,
                           "inline key record changed");
  }
  const std::byte* payload = record_data + record.header_bytes_;
  co_return std::memcmp(payload, key.data(), key.size()) == 0;
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
      absl::StatusOr<bool> verified(false);
      if (candidate.location_.external()) {
        verified =
            co_await VerifyExternalKeyExtents(store, candidate.extents_, key);
      } else {
        verified =
            co_await VerifyInlineRecordKey(store, candidate.location_, key);
      }
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
                                            std::size_t key_bytes,
                                            ReadLatencyTrace* trace) {
  if (!location.external() || extents == nullptr) {
    co_return absl::Status(absl::StatusCode::kInternal,
                           "external value has no valid extent manifest");
  }
  const std::uint64_t key_prefix = location.key_external() ? key_bytes : 0;
  std::uint64_t extent_bytes = 0;
  for (const ExtentRef& ref : *extents) {
    if (ref.payload_bytes_ == 0 ||
        ref.payload_bytes_ > kMaxRecordPayloadBytes - extent_bytes) {
      co_return absl::Status(absl::StatusCode::kInternal,
                             "invalid external value manifest");
    }
    extent_bytes += ref.payload_bytes_;
  }
  if (extent_bytes < key_prefix) {
    co_return absl::Status(absl::StatusCode::kInternal,
                           "external key exceeds extent payload");
  }
  const std::uint64_t value_bytes = extent_bytes - key_prefix;
  if (location.value_type() == ValueType::kString &&
      value_bytes != location.logical_size_) {
    co_return absl::Status(absl::StatusCode::kInternal,
                           "external string length does not match metadata");
  }
  if (trace != nullptr) {
    trace->buffer_acquire_start_ns_ = ReadTraceNowNanos();
  }
  auto acquired = co_await store.buffers_.AcquireReadBuffer(
      static_cast<std::size_t>(value_bytes));
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
    trace->buffer_acquired_ns_ = ReadTraceNowNanos();
    trace->heap_read_buffer_ = output.buffer_id() == 0;
    trace->disk_read_ = true;
  }
  if (trace != nullptr) {
    trace->io_submit_ns_ = ReadTraceNowNanos();
  }
  std::uint64_t extent_offset = 0;
  std::size_t output_offset = 0;
  for (std::size_t index = 0; index < extents->size(); ++index) {
    const ExtentRef& ref = extents->at(index);
    if (extent_offset + ref.payload_bytes_ > extent_bytes) {
      co_return absl::Status(absl::StatusCode::kInternal,
                             "extent header does not match manifest");
    }
    const std::uint64_t extent_end = extent_offset + ref.payload_bytes_;
    if (extent_end <= key_prefix) {
      extent_offset = extent_end;
      continue;
    }
    const std::size_t source_offset = static_cast<std::size_t>(
        key_prefix > extent_offset ? key_prefix - extent_offset : 0);
    const std::size_t copy_bytes = ref.payload_bytes_ - source_offset;
    // The manifest is held by the record's owner, but each extent block has
    // its own owner, and after a worker-count change the two are unrelated.
    // Hop to the block's owner exactly as LoadValue does for records.
    const std::uint16_t owner = BlockOwner(ref.block_id_);
    if (owner >= worker_count_) {
      co_return absl::Status(absl::StatusCode::kInternal,
                             "stale or missing external extent");
    }
    std::vector<std::byte> partial;
    std::byte* target = destination.data_ + output_offset;
    if (source_offset != 0) {
      partial.resize(ref.payload_bytes_);
      target = partial.data();
    }
    // if/else, not ?:, to keep the two co_awaits in separate full
    // expressions (GCC coroutine frame-slot aliasing).
    absl::Status read = absl::OkStatus();
    if (owner == store.worker_->id()) {
      read = co_await ReadExtentInto(store, ref,
                                     static_cast<std::uint32_t>(index), target);
    } else {
      read = co_await bycorf::SubmitTaskTo(
          owner, [this, owner, ref, index, target]() -> Task<absl::Status> {
            co_return co_await ReadExtentInto(*stores_[owner], ref,
                                              static_cast<std::uint32_t>(index),
                                              target);
          });
    }
    if (!read.ok()) {
      co_return read;
    }
    if (!partial.empty()) {
      std::memcpy(destination.data_ + output_offset,
                  partial.data() + source_offset, copy_bytes);
    }
    output_offset += copy_bytes;
    extent_offset = extent_end;
  }
  if (extent_offset != extent_bytes || output_offset != value_bytes) {
    co_return absl::Status(absl::StatusCode::kInternal,
                           "external value length does not match manifest");
  }
  if (trace != nullptr) {
    trace->io_complete_ns_ = ReadTraceNowNanos();
    trace->decode_done_ns_ = trace->io_complete_ns_;
  }
  const std::size_t value_offset =
      static_cast<std::size_t>(destination.data_ - output.bytes().data());
  co_return LoadedValue{std::move(output), value_offset, output_offset};
}

Task<absl::StatusOr<StorageEngine::Impl::LoadedValue>>
StorageEngine::Impl::LoadValueLocal(
    WorkerStore& store, std::uint8_t db_id, std::string_view key,
    RecordLocation location, std::uint64_t replication_epoch,
    ReadLatencyTrace* trace, std::optional<std::uint64_t> expected_db_epoch) {
  LAVIK_FAULT_INJECT(if (LAVIK_FAULT_MATCHES("LAVIK_FAIL_VALUE_READ_KEY",
                                             key)) co_return absl::
                         InternalError("injected value payload read failure"););
  if (location.external()) {
    co_return absl::Status(absl::StatusCode::kInternal,
                           "external value was dispatched as inline");
  }
  // TODO: Coalesce concurrent reads of the same aligned disk page. Key an
  // in-flight table by
  // (file_id, aligned offset, aligned length), submit one read, and fan the
  // decoded result out to all waiting coroutines. In-flight operations must
  // retain values/leases, never flat_hash_map iterators or element pointers.
  const auto [file_id, block_offset] = FileOffset(location.block_id());
  const std::uint64_t absolute_offset = block_offset + location.record_offset();
  const std::uint64_t direct_io_mask =
      static_cast<std::uint64_t>(direct_io_alignment_ - 1);
  const std::uint64_t aligned_offset = absolute_offset & ~direct_io_mask;
  const std::size_t record_headroom =
      static_cast<std::size_t>(absolute_offset - aligned_offset);
  const std::size_t record_span = record_headroom + location.total_disk_bytes();
  const std::size_t read_bytes =
      (record_span + direct_io_alignment_ - 1) & ~direct_io_mask;

  // Acquire the output buffer before resolving any block state. This is the
  // only suspension the staged-copy path would otherwise have, and it used
  // to sit between reading the staging pointer and using it, so that path
  // needed a pin to stop a flush from handing the staging buffer back. With
  // the acquisition hoisted, the staged copy runs straight through on a
  // worker that cannot preempt it, and only the disk read below pins. The
  // disk sizing covers the staged copy too: read_bytes is at least
  // total_disk_bytes, which is header plus payload.
  if (trace != nullptr) {
    trace->buffer_acquire_start_ns_ = ReadTraceNowNanos();
  }
  auto acquired = co_await store.buffers_.AcquireReadBuffer(read_bytes);
  if (!acquired.ok()) {
    co_return acquired.status();
  }
  ReadBufferLease lease = std::move(*acquired);
  if (trace != nullptr) {
    trace->buffer_acquired_ns_ = ReadTraceNowNanos();
    trace->heap_read_buffer_ = lease.buffer_id() == 0;
  }

  BlockState* state = FindBlockState(store, location.block_id());
  if (state == nullptr || !state->allocated_ || state->freeing_ ||
      state->allocation_epoch_ != location.allocation_epoch()) {
    co_return absl::Status(absl::StatusCode::kAborted,
                           "stale index block epoch");
  }

  // Only records appended since the last flush live in a staging buffer, so
  // on a read-mostly workload this branch is rare. Keep the disk read on the
  // straight-line path.
  if (location.in_memory() && state->in_memory_) [[unlikely]] {
    auto in_mem_buffer = StagingBufferFor(store, *state);
    if (!in_mem_buffer.data_ || in_mem_buffer.size_ == 0 ||
        location.record_offset() + location.total_disk_bytes() >
            in_mem_buffer.size_) {
      co_return absl::Status(absl::StatusCode::kInternal,
                             "invalid in-memory location");
    }
    if (trace != nullptr) {
      trace->io_submit_ns_ = trace->buffer_acquired_ns_;
      trace->io_complete_ns_ = trace->buffer_acquired_ns_;
    }
    FixedBuffer io = lease.io_buffer();
    if (location.external()) {
      co_return absl::Status(absl::StatusCode::kInternal,
                             "external value requires extent loading");
    }
    const std::byte* record_bytes =
        in_mem_buffer.data_ + location.record_offset();
    RecordHeader record{};
    std::string_view disk_key;
    if (!DecodeRecordHeader(std::span<const std::byte>(
                                record_bytes, location.total_disk_bytes()),
                            &record, &disk_key) ||
        record.db_id_ != db_id || (!record.key_external_ && disk_key != key) ||
        record.kind_ != RecordKind::kValue ||
        record.db_epoch_ != expected_db_epoch.value_or(DbEpoch(db_id)) ||
        record.mutation_sequence_ != location.mutation_sequence_ ||
        record.replication_epoch_ != replication_epoch ||
        record.allocation_epoch_ != location.allocation_epoch() ||
        record.expire_at_ms_ != location.expire_at_ms_ ||
        record.value_type_ != location.value_type() ||
        record.external_ != location.external() ||
        record.key_external_ != location.key_external() ||
        location.logical_size_ != record.logical_size_ ||
        location.total_disk_bytes() != record.total_disk_bytes_) {
      co_return absl::Status(absl::StatusCode::kAborted,
                             "record does not match in-memory location");
    }
    const std::byte* payload_data = record_bytes + record.header_bytes_;
    const std::size_t key_prefix = record.key_external_ ? record.key_bytes_ : 0;
    if (record.payload_bytes_ < key_prefix ||
        (record.key_external_ &&
         (record.key_bytes_ != key.size() ||
          std::memcmp(payload_data, key.data(), key.size()) != 0))) {
      co_return absl::Status(absl::StatusCode::kAborted,
                             "record key does not match location");
    }
    const std::size_t value_bytes = record.payload_bytes_ - key_prefix;
    if (record.value_type_ == ValueType::kString &&
        value_bytes != record.logical_size_) {
      co_return absl::Status(absl::StatusCode::kInternal,
                             "inline value length does not match metadata");
    }
    std::memcpy(io.data_, payload_data + key_prefix, value_bytes);
    if (Crc32c(std::span<const std::byte>(
            payload_data, record.payload_bytes_)) != record.payload_checksum_) {
      co_return absl::Status(absl::StatusCode::kInternal,
                             "record value checksum mismatch");
    }
    if (trace != nullptr) {
      trace->decode_done_ns_ = ReadTraceNowNanos();
    }
    const std::size_t value_offset =
        static_cast<std::size_t>(io.data_ - lease.bytes().data());
    co_return LoadedValue{std::move(lease), value_offset, value_bytes,
                          record.txid_};
  }

  // Only the disk read suspends while holding the BlockState pointer, so it
  // is the only path that has to keep the state alive with a pin.
  ++state->pins_;
  struct PinGuard {
    WorkerStore* store_ = nullptr;
    BlockState* state_ = nullptr;
    ~PinGuard() {
      if (state_ == nullptr) {
        return;
      }
      --state_->pins_;
      if (state_->pins_ == 0 && state_->release_pending_) {
        ReleaseStagingBuffer(*store_, *state_);
      }
    }
  } pin{&store, state};

  if (trace != nullptr) {
    trace->disk_read_ = true;
  }
  FixedBuffer io = lease.io_buffer();
  if (read_bytes > io.size_) {
    co_return absl::Status(absl::StatusCode::kOutOfRange,
                           "record exceeds registered read buffer capacity");
  }
  FixedBuffer record_buffer = io;
  record_buffer.size_ = read_bytes;
  if (trace != nullptr) {
    trace->io_submit_ns_ = ReadTraceNowNanos();
  }
  auto read = co_await ReadStorageBuffer(*store.worker_, store.files_[file_id],
                                         record_buffer, lease.registered(),
                                         aligned_offset);
  if (trace != nullptr) {
    trace->io_complete_ns_ = ReadTraceNowNanos();
  }
  if (!read.ok()) {
    co_return read.status();
  }
  if (*read != read_bytes) {
    co_return absl::Status(absl::StatusCode::kInternal,
                           "short compact record read");
  }

  RecordHeader record{};
  std::string_view disk_key;
  const std::byte* record_data = io.data_ + record_headroom;
  std::span<const std::byte> record_bytes(record_data,
                                          location.total_disk_bytes());
  if (!DecodeRecordHeader(record_bytes, &record, &disk_key) ||
      record.db_id_ != db_id || (!record.key_external_ && disk_key != key) ||
      record.kind_ != RecordKind::kValue ||
      record.db_epoch_ != expected_db_epoch.value_or(DbEpoch(db_id)) ||
      record.mutation_sequence_ != location.mutation_sequence_ ||
      record.replication_epoch_ != replication_epoch ||
      record.allocation_epoch_ != location.allocation_epoch() ||
      record.expire_at_ms_ != location.expire_at_ms_ ||
      record.value_type_ != location.value_type() ||
      record.external_ != location.external() ||
      record.key_external_ != location.key_external() ||
      record.logical_size_ != location.logical_size_ ||
      record.total_disk_bytes_ != location.total_disk_bytes()) {
    co_return absl::Status(absl::StatusCode::kAborted,
                           "record does not match in-memory location");
  }
  const std::byte* payload_data = record_data + record.header_bytes_;
  const std::size_t key_prefix = record.key_external_ ? record.key_bytes_ : 0;
  if (record.payload_bytes_ < key_prefix ||
      (record.key_external_ &&
       (record.key_bytes_ != key.size() ||
        std::memcmp(payload_data, key.data(), key.size()) != 0))) {
    co_return absl::Status(absl::StatusCode::kAborted,
                           "record key does not match location");
  }
  const std::size_t value_bytes = record.payload_bytes_ - key_prefix;
  if (record.value_type_ == ValueType::kString &&
      value_bytes != record.logical_size_) {
    co_return absl::Status(absl::StatusCode::kInternal,
                           "inline value length does not match metadata");
  }
  const std::byte* value_data = payload_data + key_prefix;
  if (Crc32c(std::span<const std::byte>(payload_data, record.payload_bytes_)) !=
      record.payload_checksum_) {
    co_return absl::Status(absl::StatusCode::kInternal,
                           "record value checksum mismatch");
  }
  const std::byte* framed_value = value_data;
  if (value_bytes > DirectGetValueLimit()) {
    std::memmove(io.data_, value_data, value_bytes);
    framed_value = io.data_;
  }
  if (trace != nullptr) {
    trace->decode_done_ns_ = ReadTraceNowNanos();
  }
  const std::size_t value_offset =
      static_cast<std::size_t>(framed_value - lease.bytes().data());
  co_return LoadedValue{std::move(lease), value_offset, value_bytes,
                        record.txid_};
}

}  // namespace lavik::storage
