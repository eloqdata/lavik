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

struct StorageEngine::Impl::SnapshotReadJoin {
  std::size_t pending_ = 0;
  std::coroutine_handle<> waiter_;
  absl::Status error_;

  void Complete(absl::Status status) {
    if (!status.ok() && error_.ok()) {
      error_ = std::move(status);
    }
    assert(pending_ != 0);
    if (--pending_ == 0 && waiter_) {
      const auto waiter = std::exchange(waiter_, {});
      bycorf::ThisWorker().self_->Enqueue(waiter);
    }
  }

  auto Join() {
    struct Awaiter {
      SnapshotReadJoin* join_;
      bool await_ready() const noexcept { return join_->pending_ == 0; }
      void await_suspend(std::coroutine_handle<> waiter) const noexcept {
        join_->waiter_ = waiter;
      }
      void await_resume() const noexcept {}
    };
    return Awaiter{this};
  }
};

Task<absl::Status> StorageEngine::Impl::PinFullSyncExtents(
    ExtentManifest extents) {
  if (extents == nullptr) {
    co_return absl::InvalidArgumentError("full-sync value has no extents");
  }
  // Keep same-worker and cross-worker suspensions in separate statements.
  // GCC 13 can reuse the wrong coroutine-frame slot for co_await in both ?:
  // arms.
  std::size_t pinned = 0;
  for (const ExtentRef& ref : *extents) {
    const unsigned owner = BlockOwner(ref.block_id_);
    if (owner >= worker_count_) break;
    auto pin = [this, owner, ref]() -> Task<absl::Status> {
      WorkerStore& extent_store = *stores_[owner];
      co_await extent_store.store_state_mutex_.Lock();
      UnlockGuard unlock(&extent_store.store_state_mutex_,
                         extent_store.worker_);
      BlockState* state = FindBlockState(extent_store, ref.block_id_);
      if (state == nullptr || !state->allocated_ || state->freeing_ ||
          state->kind_ != BlockKind::kPayloadExtent ||
          state->allocation_epoch_ != ref.allocation_epoch_) {
        co_return absl::AbortedError("stale full-sync value extent");
      }
      ++state->pins_;
      co_return absl::OkStatus();
    };
    absl::Status status;
    if (owner == bycorf::ThisWorker().id_) {
      status = co_await pin();
    } else {
      status = co_await bycorf::SubmitTaskTo(owner, pin);
    }
    if (!status.ok()) break;
    ++pinned;
  }
  if (pinned == extents->size()) co_return absl::OkStatus();
  auto partial = std::make_shared<std::vector<ExtentRef>>(
      extents->begin(), extents->begin() + pinned);
  (void)co_await ReleaseFullSyncExtents(
      std::shared_ptr<const std::vector<ExtentRef>>(std::move(partial)));
  co_return absl::AbortedError("failed to pin full-sync value extents");
}

Task<absl::Status> StorageEngine::Impl::ReleaseFullSyncExtents(
    ExtentManifest extents) {
  if (extents == nullptr) co_return absl::OkStatus();
  for (const ExtentRef& ref : *extents) {
    const unsigned owner = BlockOwner(ref.block_id_);
    if (owner >= worker_count_) continue;
    auto release = [this, owner, ref]() -> Task<absl::Status> {
      WorkerStore& extent_store = *stores_[owner];
      co_await extent_store.store_state_mutex_.Lock();
      UnlockGuard unlock(&extent_store.store_state_mutex_,
                         extent_store.worker_);
      BlockState* state = FindBlockState(extent_store, ref.block_id_);
      if (state != nullptr && state->allocated_ &&
          state->allocation_epoch_ == ref.allocation_epoch_ &&
          state->pins_ != 0) {
        --state->pins_;
      }
      co_return absl::OkStatus();
    };
    absl::Status status;
    if (owner == bycorf::ThisWorker().id_) {
      status = co_await release();
    } else {
      status = co_await bycorf::SubmitTaskTo(owner, release);
    }
    if (!status.ok()) co_return status;
  }
  co_return absl::OkStatus();
}

Task<absl::StatusOr<std::uint64_t>> StorageEngine::Impl::PinFullSyncValue(
    WorkerStore& store, std::uint64_t session_id,
    WorkerStore::PartitionStore& partition, RecordLocation location,
    ExtentManifest extents, std::size_t key_bytes) {
  if (!location.external() || extents == nullptr) {
    co_return absl::InvalidArgumentError(
        "only external full-sync values can be pinned");
  }
  std::uint64_t extent_bytes = 0;
  for (const ExtentRef& ref : *extents) {
    if (ref.payload_bytes_ > kMaxRecordPayloadBytes - extent_bytes) {
      co_return absl::InternalError("invalid full-sync extent manifest");
    }
    extent_bytes += ref.payload_bytes_;
  }
  const std::size_t key_prefix = location.key_external() ? key_bytes : 0;
  if (extent_bytes < key_prefix) {
    co_return absl::InternalError("full-sync extent manifest is truncated");
  }
  absl::Status pinned = co_await PinFullSyncExtents(extents);
  if (!pinned.ok()) co_return pinned;
  auto capture = partition.fullsync_subscribers_.find(session_id);
  const auto session = store.fullsync_sessions_.find(session_id);
  if (session == store.fullsync_sessions_.end() ||
      session->second.db_epoch_invalidated_ ||
      capture == partition.fullsync_subscribers_.end() ||
      capture->second.next_pinned_value_id_ == 0) {
    store.worker_->Spawn(ReleaseFullSyncExtents(std::move(extents)));
    co_return absl::FailedPreconditionError(
        "full-sync capture ended while pinning value");
  }
  const std::uint64_t id = capture->second.next_pinned_value_id_;
  auto prepared = PrepareFullSyncPinnedValueInsert(capture->second);
  if (!prepared.ok()) {
    store.worker_->Spawn(ReleaseFullSyncExtents(std::move(extents)));
    co_return prepared;
  }
  auto [_, inserted] = capture->second.pinned_values_.emplace(
      id, WorkerStore::FullSyncCapture::PinnedValue{
              .extents_ = extents,
              .key_bytes_ = key_prefix,
              .value_bytes_ = extent_bytes - key_prefix,
          });
  if (!inserted) {
    InvalidateFullSyncSession(store, session_id);
    store.worker_->Spawn(ReleaseFullSyncExtents(std::move(extents)));
    co_return absl::InternalError("duplicate full-sync pinned value id");
  }
  ++capture->second.next_pinned_value_id_;
  co_return id;
}

Task<absl::Status> StorageEngine::Impl::ReadSnapshotRecord(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    RecordIndex& index, std::uint8_t db_id, const std::string* key,
    std::uint64_t session_id, std::uint64_t baseline_version,
    std::optional<SnapshotRecord>* output, SnapshotReadJoin* join) {
  absl::Status status;
  {
    const Digest digest = ComputeDigest(*key);
    auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
        db_id, tx::FingerprintOf(digest), tx::LockMode::kShared);
    const std::string& coverage_key = *key;
    auto capture = partition.fullsync_subscribers_.find(session_id);
    const auto active_session = store.fullsync_sessions_.find(session_id);
    if (active_session == store.fullsync_sessions_.end() ||
        active_session->second.db_epoch_invalidated_ ||
        capture == partition.fullsync_subscribers_.end() ||
        capture->second.phase_ !=
            WorkerStore::FullSyncCapture::Phase::kCapturing ||
        capture->second.db_phases_[db_id] !=
            WorkerStore::FullSyncCapture::DbPhase::kScanning ||
        capture->second.latest_by_key_[db_id].contains(coverage_key) ||
        capture->second.key_phases_.Find(digest, coverage_key) != nullptr) {
      key_lock.Reset();
      join->Complete(absl::OkStatus());
      co_return absl::OkStatus();
    }
    auto resolved = co_await FindVerifiedEntry(store, index, digest, *key);
    bool phase_inserted = false;
    if (!resolved.ok()) {
      status = resolved.status();
    } else {
      capture = partition.fullsync_subscribers_.find(session_id);
      const auto resumed_session = store.fullsync_sessions_.find(session_id);
      if (capture == partition.fullsync_subscribers_.end() ||
          resumed_session == store.fullsync_sessions_.end() ||
          resumed_session->second.db_epoch_invalidated_ ||
          capture->second.phase_ !=
              WorkerStore::FullSyncCapture::Phase::kCapturing ||
          capture->second.db_phases_[db_id] !=
              WorkerStore::FullSyncCapture::DbPhase::kScanning) {
        status = absl::FailedPreconditionError(
            "full-sync capture ended during snapshot lookup");
      }
      auto* current = *resolved;
      if (status.ok() && current != nullptr &&
          current->value_.kind() == RecordKind::kValue) {
        const RecordLocation location = MaterializeIndexLocation(*current);
        if (IsExpiredNow(location)) {
          QueueExpiredCandidate(store, partition.id_, db_id, *current, *key);
        } else if (!TryConsumeFullSyncCoverageCredit(
                       store, session_id, capture->second,
                       coverage_key.size() <= options_.inline_key_max_bytes_
                           ? coverage_key.size()
                           : sizeof(Digest),
                       /*allocates_arena_entry=*/true)) {
          status = absl::ResourceExhaustedError(
              "full-sync coverage memory credit exhausted");
        } else {
          const auto phase = capture->second.key_phases_.InsertOrAssign(
              digest, coverage_key,
              WorkerStore::FullSyncCapture::KeyPhase::kBaselineInflight,
              coverage_key.size() <= options_.inline_key_max_bytes_);
          if (phase.rejected_) {
            InvalidateFullSyncSession(store, session_id);
            status = absl::ResourceExhaustedError(
                "full-sync coverage entry capacity exhausted");
          } else {
            phase_inserted = true;
          }
          if (status.ok()) {
            const ExtentManifest extents = ExtentsFor(store, current);
            std::uint64_t value_bytes = location.logical_size_;
            if (location.external()) {
              value_bytes = 0;
              for (const ExtentRef& ref : *extents) {
                value_bytes += ref.payload_bytes_;
              }
              if (location.key_external()) {
                value_bytes -=
                    std::min<std::uint64_t>(value_bytes, key->size());
              }
            }
            if (location.grouped() ||
                (location.external() &&
                 value_bytes > kReplicationTransferBytes)) {
              absl::StatusOr<std::uint64_t> source_id;
              if (location.grouped()) {
                source_id = co_await PinFullSyncCollection(
                    store, session_id, partition, db_id, *key, digest, location,
                    extents, &value_bytes);
              } else {
                source_id =
                    co_await PinFullSyncValue(store, session_id, partition,
                                              location, extents, key->size());
              }
              if (!source_id.ok()) {
                status = source_id.status();
              } else {
                output->emplace(SnapshotRecord{
                    .kind_ = SnapshotRecord::Kind::kValue,
                    .db_id_ = db_id,
                    .db_epoch_ = DbEpoch(db_id),
                    .mutation_sequence_ = baseline_version,
                    .expire_at_ms_ = location.expire_at_ms_,
                    .value_type_ = location.value_type(),
                    .logical_size_ = location.logical_size_,
                    .source_id_ = *source_id,
                    .source_value_bytes_ = value_bytes,
                    .key_digest_ = digest,
                    .key_ = *key,
                    .value_ = {},
                });
              }
            } else {
              auto loaded = co_await LoadValue(store, partition, db_id, *key,
                                               digest, location, extents);
              if (!loaded.ok()) {
                if (loaded.status().code() != absl::StatusCode::kNotFound) {
                  status = loaded.status();
                }
              } else {
                const std::span<const std::byte> value = loaded->value();
                output->emplace(SnapshotRecord{
                    .kind_ = SnapshotRecord::Kind::kValue,
                    .db_id_ = db_id,
                    .db_epoch_ = DbEpoch(db_id),
                    .mutation_sequence_ = baseline_version,
                    .expire_at_ms_ = location.expire_at_ms_,
                    .value_type_ = location.value_type(),
                    .logical_size_ = location.logical_size_,
                    .key_digest_ = digest,
                    .key_ = *key,
                    .value_ =
                        std::string(reinterpret_cast<const char*>(value.data()),
                                    value.size()),
                });
              }
            }
          }
        }
      }
    }
    if (phase_inserted && (!status.ok() || !output->has_value())) {
      capture = partition.fullsync_subscribers_.find(session_id);
      if (capture != partition.fullsync_subscribers_.end()) {
        auto* phase = capture->second.key_phases_.Find(digest, coverage_key);
        if (phase != nullptr &&
            phase->value_ ==
                WorkerStore::FullSyncCapture::KeyPhase::kBaselineInflight) {
          capture->second.key_phases_.Erase(phase);
        }
      }
    }
    if (status.ok() && output->has_value()) {
      capture = partition.fullsync_subscribers_.find(session_id);
      const auto active_session = store.fullsync_sessions_.find(session_id);
      if (capture == partition.fullsync_subscribers_.end() ||
          active_session == store.fullsync_sessions_.end() ||
          active_session->second.db_epoch_invalidated_ ||
          capture->second.phase_ !=
              WorkerStore::FullSyncCapture::Phase::kCapturing) {
        if ((*output)->source_id_ != 0 &&
            capture != partition.fullsync_subscribers_.end()) {
          ReleaseFullSyncValue(session_id, partition.id_,
                               (*output)->source_id_);
        }
        output->reset();
      }
    }
    // Release the shared key hold before waking the parent so foreground
    // writes never wait for frame cleanup.
    key_lock.Reset();
  }
  join->Complete(std::move(status));
  co_return absl::OkStatus();
}

Task<absl::StatusOr<SnapshotRecord>>
StorageEngine::Impl::ReadFullSyncOverrideRecord(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint64_t session_id, const SnapshotRecord& requested) {
  const Digest digest = ComputeDigest(requested.key_);
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      requested.db_id_, tx::FingerprintOf(digest), tx::LockMode::kShared);
  auto& index = partition.indexes_[requested.db_id_];
  auto resolved =
      co_await FindVerifiedEntry(store, index, digest, requested.key_);
  if (!resolved.ok()) co_return resolved.status();

  const RecordIndex::Entry* current = *resolved;
  if (current == nullptr || current->value_.kind() != RecordKind::kValue ||
      IsExpiredNow(*current)) {
    const std::uint64_t sequence =
        current == nullptr ? requested.mutation_sequence_
                           : std::max(requested.mutation_sequence_,
                                      current->value_.mutation_sequence_);
    key_lock.Reset();
    co_return SnapshotRecord{
        .kind_ = SnapshotRecord::Kind::kDelete,
        .db_id_ = requested.db_id_,
        .db_epoch_ = DbEpoch(requested.db_id_),
        .mutation_sequence_ = sequence,
        .key_ = requested.key_,
        .value_ = {},
    };
  }

  const RecordLocation location = MaterializeIndexLocation(*current);
  const ExtentManifest extents = ExtentsFor(store, current);
  std::uint64_t value_bytes = location.logical_size_;
  if (location.external()) {
    value_bytes = 0;
    for (const ExtentRef& ref : *extents) value_bytes += ref.payload_bytes_;
    if (location.key_external()) {
      value_bytes -=
          std::min<std::uint64_t>(value_bytes, requested.key_.size());
    }
  }
  if (location.grouped() ||
      (location.external() && value_bytes > kReplicationTransferBytes)) {
    absl::StatusOr<std::uint64_t> source_id;
    if (location.grouped()) {
      source_id = co_await PinFullSyncCollection(
          store, session_id, partition, requested.db_id_, requested.key_,
          digest, location, extents, &value_bytes);
    } else {
      source_id =
          co_await PinFullSyncValue(store, session_id, partition, location,
                                    extents, requested.key_.size());
    }
    if (!source_id.ok()) co_return source_id.status();
    key_lock.Reset();
    co_return SnapshotRecord{
        .kind_ = SnapshotRecord::Kind::kValue,
        .db_id_ = requested.db_id_,
        .db_epoch_ = DbEpoch(requested.db_id_),
        .mutation_sequence_ =
            std::max(requested.mutation_sequence_, location.mutation_sequence_),
        .expire_at_ms_ = location.expire_at_ms_,
        .value_type_ = location.value_type(),
        .logical_size_ = location.logical_size_,
        .source_id_ = *source_id,
        .source_value_bytes_ = value_bytes,
        .key_ = requested.key_,
        .value_ = {},
    };
  }
  auto loaded = co_await LoadValue(store, partition, requested.db_id_,
                                   requested.key_, digest, location, extents);
  if (!loaded.ok()) co_return loaded.status();
  const std::span<const std::byte> value = loaded->value();
  SnapshotRecord result{
      .kind_ = SnapshotRecord::Kind::kValue,
      .db_id_ = requested.db_id_,
      .db_epoch_ = DbEpoch(requested.db_id_),
      .mutation_sequence_ =
          std::max(requested.mutation_sequence_, location.mutation_sequence_),
      .expire_at_ms_ = location.expire_at_ms_,
      .value_type_ = location.value_type(),
      .logical_size_ = location.logical_size_,
      .key_ = requested.key_,
      .value_ = std::string(reinterpret_cast<const char*>(value.data()),
                            value.size()),
  };
  key_lock.Reset();
  co_return result;
}

ScanPartitionAwaitable StorageEngine::Impl::ScanPartition(
    std::uint16_t partition_id, std::uint8_t db_id, std::uint64_t cursor,
    std::size_t count, std::uint64_t now_ms, std::size_t max_bytes) {
  assert(db_id < kLogicalDatabaseCount);
  assert(count > 0);
  if (now_ms == 0) {
    now_ms = UnixTimeMillis();
  }
  const std::size_t max_iterations =
      count > std::numeric_limits<std::size_t>::max() / 10
          ? std::numeric_limits<std::size_t>::max()
          : count * 10;
  ScanPartitionState state;
  state.index_ = &PartitionFor(CurrentStore(), partition_id).indexes_[db_id];
  state.now_ms_ = now_ms;
  state.count_ = count;
  state.max_bytes_ = max_bytes;
  state.max_iterations_ = max_iterations;
  state.result_.cursor_ = cursor;

  if (ScanPartitionInline(&state)) {
    return ScanPartitionAwaitable(std::move(state.result_));
  }
  return ScanPartitionAwaitable(ResumeScanPartition(std::move(state)));
}

bool StorageEngine::Impl::ScanPartitionInline(ScanPartitionState* state) {
  assert(state != nullptr);
  assert(state->index_ != nullptr);
  assert(state->external_.empty());
  auto add_bytes = [state](std::size_t value) {
    state->bytes_ =
        value > std::numeric_limits<std::size_t>::max() - state->bytes_
            ? std::numeric_limits<std::size_t>::max()
            : state->bytes_ + value;
  };

  do {
    state->result_.cursor_ = state->index_->Scan(
        state->result_.cursor_, [&](const RecordIndex::Entry& entry) {
          if (entry.value_.kind() == RecordKind::kValue &&
              !IsExpired(entry, state->now_ms_)) {
            if (entry.key_complete()) [[likely]] {
              add_bytes(entry.key().size());
              state->result_.keys_.emplace_back(entry.key());
              state->result_.value_types_.push_back(entry.value_.value_type());
              std::size_t value_bytes = entry.value_.logical_size();
              if (entry.value_.value_type() != ValueType::kString) {
                value_bytes = entry.value_.total_disk_bytes();
              }
              if (entry.value_.external()) {
                value_bytes = 0;
                const ExtentManifest extents =
                    ExtentsFor(CurrentStore(), &entry);
                for (const ExtentRef& ref : *extents) {
                  value_bytes += ref.payload_bytes_;
                }
                value_bytes -=
                    std::min(value_bytes, entry.value_.key_external()
                                              ? entry.logical_key_size()
                                              : std::size_t{0});
              }
              state->result_.value_bytes_.push_back(value_bytes);
              add_bytes(value_bytes);
            } else [[unlikely]] {
              ExtentManifest extents = ExtentsFor(CurrentStore(), &entry);
              std::size_t value_bytes = 0;
              if (extents != nullptr) {
                for (const ExtentRef& ref : *extents) {
                  value_bytes += ref.payload_bytes_;
                }
              }
              value_bytes -= std::min<std::size_t>(
                  value_bytes,
                  entry.value_.key_external() ? entry.logical_key_size() : 0);
              state->external_.push_back(ScanPartitionState::ExternalCandidate{
                  .entry_address_ = reinterpret_cast<std::uintptr_t>(&entry),
                  .extents_ = std::move(extents),
                  .location_ = MaterializeIndexLocation(entry),
                  .hash_ =
                      RecordIndex::AddressHash(entry.external_key_digest()),
                  .key_bytes_ = entry.logical_key_size(),
                  .value_bytes_ = value_bytes,
              });
            }
          }
        });
    ++state->iterations_;
    if (!state->external_.empty()) return false;
  } while (state->result_.cursor_ != 0 &&
           state->result_.keys_.size() < state->count_ &&
           state->bytes_ < state->max_bytes_ &&
           state->iterations_ < state->max_iterations_);
  return true;
}

Task<absl::StatusOr<ScanBatch>> StorageEngine::Impl::ResumeScanPartition(
    ScanPartitionState state) {
  auto add_bytes = [&state](std::size_t value) {
    state.bytes_ =
        value > std::numeric_limits<std::size_t>::max() - state.bytes_
            ? std::numeric_limits<std::size_t>::max()
            : state.bytes_ + value;
  };

  while (true) {
    for (std::size_t index = 0; index < state.external_.size(); ++index) {
      // Keep the metadata needed after the read in the coroutine frame, while
      // transferring the manifest to the child that materializes the key.
      ScanPartitionState::ExternalCandidate candidate =
          std::move(state.external_[index]);
      auto key = co_await LoadOutOfIndexKey(CurrentStore(), candidate.location_,
                                            std::move(candidate.extents_),
                                            candidate.key_bytes_);
      if (!key.ok()) {
        co_return key.status();
      }
      const RecordIndex::Entry* current =
          state.index_->FindAddress(candidate.entry_address_, candidate.hash_);
      if (current == nullptr) continue;
      if (MaterializeIndexLocation(*current).SamePhysicalRecord(
              candidate.location_) &&
          current->value_.kind() == RecordKind::kValue &&
          !IsExpired(*current, state.now_ms_)) {
        add_bytes(key->size());
        state.result_.keys_.push_back(std::move(*key));
        state.result_.value_types_.push_back(current->value_.value_type());
        state.result_.value_bytes_.push_back(candidate.value_bytes_);
        add_bytes(candidate.value_bytes_);
      }
    }
    state.external_.clear();
    if (state.result_.cursor_ == 0 ||
        state.result_.keys_.size() >= state.count_ ||
        state.bytes_ >= state.max_bytes_ ||
        state.iterations_ >= state.max_iterations_ ||
        ScanPartitionInline(&state)) {
      co_return std::move(state.result_);
    }
  }
}

std::uint64_t StorageEngine::Impl::FullSyncCoverageEntryBytes(
    std::size_t logical_key_bytes) const noexcept {
  // Coverage uses the current inline threshold even when recovery materialized
  // a record written under a different threshold. Larger keys contribute only
  // the digest retained by the coverage map. The factor of two matches the
  // worst case where key identity is present in both owners during replacement.
  const std::uint64_t retained_key_bytes =
      logical_key_bytes <= options_.inline_key_max_bytes_
          ? static_cast<std::uint64_t>(logical_key_bytes)
          : sizeof(Digest);
  assert(retained_key_bytes <= (std::numeric_limits<std::uint64_t>::max() -
                                kFullSyncReplacementMetadataBytes) /
                                   2);
  return kFullSyncReplacementMetadataBytes + retained_key_bytes * 2;
}

void StorageEngine::Impl::AddFullSyncCoverageEntry(
    WorkerStore::PartitionStore& partition, std::uint8_t db_id,
    std::size_t logical_key_bytes) noexcept {
  assert(db_id < kLogicalDatabaseCount);
  const std::uint64_t bytes = FullSyncCoverageEntryBytes(logical_key_bytes);
  assert(partition.fullsync_coverage_bytes_[db_id] <=
         std::numeric_limits<std::uint64_t>::max() - bytes);
  partition.fullsync_coverage_bytes_[db_id] += bytes;
}

void StorageEngine::Impl::RemoveFullSyncCoverageEntry(
    WorkerStore::PartitionStore& partition, std::uint8_t db_id,
    std::size_t logical_key_bytes) noexcept {
  assert(db_id < kLogicalDatabaseCount);
  const std::uint64_t bytes = FullSyncCoverageEntryBytes(logical_key_bytes);
  assert(partition.fullsync_coverage_bytes_[db_id] >= bytes);
  partition.fullsync_coverage_bytes_[db_id] -= bytes;
}

absl::StatusOr<FullSyncSessionStart> StorageEngine::Impl::BeginFullSyncSession(
    std::uint64_t session_id) {
  WorkerStore& store = CurrentStore();
  auto [session, inserted] = store.fullsync_sessions_.try_emplace(session_id);
  if (!inserted) {
    if (session->second.db_epoch_invalidated_) {
      return absl::FailedPreconditionError(
          "full-sync session was invalidated by a database epoch advance");
    }
    return FullSyncSessionStart{.db_epochs_ = session->second.db_epochs_};
  }
  const std::size_t staging_bytes =
      replication_publish_queue_bytes_.load(std::memory_order_acquire);
  auto staging = TryReserveMemory(staging_bytes);
  if (!staging.has_value()) {
    store.fullsync_sessions_.erase(session);
    RecordMemoryRejection();
    return absl::ResourceExhaustedError(
        "insufficient retained-memory budget for full-sync publisher staging");
  }
  session->second.publisher_staging_charge_.Adopt(&*staging, staging_bytes);
  for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
    session->second.db_epochs_[db_id] = DbEpoch(db_id);
  }
  // One worker scans one partition at a time. Reserve reusable headroom for
  // the largest owner-local coverage map before exposing the session. Each
  // first-seen identity converts part of this logical reserve into live
  // allocator accounting; clearing the partition restores that credit for the
  // next handoff. Foreground growth therefore cannot consume the promised
  // headroom while the session is active.
  // A dirty key may occupy a std::map replacement node plus one ScanHashMap
  // coverage entry. Both are metadata-only; value bytes are deliberately
  // excluded. Keep a conservative fixed allowance for bucket/node/control
  // overhead and both possible logical key copies. This is reservation
  // accounting, not an on-disk format constant.
  constexpr std::size_t kCoverageControlBytes = 64 * 1024;
  constexpr std::size_t kCoverageFixedBytes =
      ScanHashMapEntryArena::kSmallSpanAdmissionBytes + kCoverageControlBytes;
  std::size_t largest_partition_db_bytes = 0;
  for (const auto& partition : store.partitions_) {
    for (const std::uint64_t coverage_bytes :
         partition.fullsync_coverage_bytes_) {
      if (coverage_bytes >
          std::numeric_limits<std::size_t>::max() - kCoverageFixedBytes) {
        store.fullsync_sessions_.erase(session);
        return absl::ResourceExhaustedError(
            "full-sync coverage reservation overflow");
      }
      largest_partition_db_bytes = std::max(
          largest_partition_db_bytes,
          kCoverageFixedBytes + static_cast<std::size_t>(coverage_bytes));
    }
  }
  const std::size_t reserve =
      std::max(kCoverageFixedBytes, largest_partition_db_bytes);
  if (!TryReserveFullSyncMemory(reserve)) {
    store.fullsync_sessions_.erase(session);
    return absl::ResourceExhaustedError(
        "insufficient retained-memory budget for full-sync coverage");
  }
  session->second.reserved_memory_bytes_ = reserve;
  session->second.available_memory_bytes_ = reserve;
  return FullSyncSessionStart{.db_epochs_ = session->second.db_epochs_};
}

bool StorageEngine::Impl::FullSyncSessionValid(
    std::uint64_t session_id) const noexcept {
  const WorkerStore& store = CurrentStore();
  const auto session = store.fullsync_sessions_.find(session_id);
  return session != store.fullsync_sessions_.end() &&
         !session->second.db_epoch_invalidated_;
}

void StorageEngine::Impl::EndFullSyncSession(std::uint64_t session_id) {
  WorkerStore& store = CurrentStore();
  for (auto& partition : store.partitions_) {
    auto capture = partition.fullsync_subscribers_.find(session_id);
    if (capture == partition.fullsync_subscribers_.end()) continue;
    ClearFullSyncCapture(store, session_id, capture->second);
    partition.fullsync_subscribers_.erase(capture);
  }
  auto session = store.fullsync_sessions_.find(session_id);
  if (session != store.fullsync_sessions_.end()) {
    session->second.publish_queue_.clear();
    session->second.publish_queue_bytes_ = 0;
    session->second.publisher_admitted_bytes_ = 0;
    session->second.publisher_admitted_items_ = 0;
    assert(session->second.available_memory_bytes_ ==
           session->second.reserved_memory_bytes_);
    ReleaseFullSyncMemory(session->second.available_memory_bytes_);
    store.fullsync_sessions_.erase(session);
  }
  store.fullsync_publisher_capacity_ready_.NotifyAll(*store.worker_);
}

absl::StatusOr<PartitionReplicationStart>
StorageEngine::Impl::BeginPartitionReplication(std::uint64_t session_id,
                                               std::uint16_t partition_id,
                                               std::uint8_t db_count) {
  if (db_count == 0 || db_count > options_.database_count_) {
    return absl::InvalidArgumentError("invalid full-sync database count");
  }
  WorkerStore& store = CurrentStore();
  const auto session = store.fullsync_sessions_.find(session_id);
  if (session == store.fullsync_sessions_.end()) {
    return absl::FailedPreconditionError(
        "full-sync session has not been started on this worker");
  }
  if (session->second.db_epoch_invalidated_) {
    return absl::FailedPreconditionError(
        "full-sync session was invalidated by a database epoch advance");
  }
  auto& partition = PartitionFor(store, partition_id);
  auto [capture, inserted] =
      partition.fullsync_subscribers_.try_emplace(session_id);
  if (!inserted) {
    ClearFullSyncCapture(store, session_id, capture->second);
  }
  capture->second.baseline_version_ = partition.mutation_sequence_;
  // Cluster export excludes DBs 1..15. They require neither a scan nor a
  // per-DB completion, but must not hold the partition completion barrier.
  capture->second.db_phases_.fill(
      WorkerStore::FullSyncCapture::DbPhase::kTailing);
  std::fill_n(capture->second.db_phases_.begin(), db_count,
              WorkerStore::FullSyncCapture::DbPhase::kUnstarted);
  capture->second.phase_ = WorkerStore::FullSyncCapture::Phase::kCapturing;
  PartitionReplicationStart result;
  result.baseline_version_ = capture->second.baseline_version_;
  result.db_epochs_ = session->second.db_epochs_;
  return result;
}

absl::StatusOr<bool> StorageEngine::Impl::TrySkipEmptyPartitionDbReplication(
    std::uint64_t session_id, std::uint16_t partition_id, std::uint8_t db_id) {
  if (db_id >= options_.database_count_) {
    return absl::InvalidArgumentError("invalid full-sync database");
  }
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionFor(store, partition_id);
  const auto session = store.fullsync_sessions_.find(session_id);
  auto capture = partition.fullsync_subscribers_.find(session_id);
  if (session == store.fullsync_sessions_.end() ||
      session->second.db_epoch_invalidated_ ||
      capture == partition.fullsync_subscribers_.end() ||
      capture->second.phase_ !=
          WorkerStore::FullSyncCapture::Phase::kCapturing ||
      capture->second.db_phases_[db_id] !=
          WorkerStore::FullSyncCapture::DbPhase::kUnstarted) {
    return absl::FailedPreconditionError("full-sync database is not unstarted");
  }
  const std::uint32_t target_id =
      (static_cast<std::uint32_t>(partition_id) << 8) | db_id;
  if (partition.live_key_count_[db_id] != 0 ||
      session->second.unstarted_admissions_.contains(target_id)) {
    return false;
  }
  // No suspension separates the empty/admission check from TAILING. A write
  // admitted afterwards must reserve publish credit instead of relying on a
  // future baseline scan. No scanning state was created, so nothing to clear.
  capture->second.db_phases_[db_id] =
      WorkerStore::FullSyncCapture::DbPhase::kTailing;
  return true;
}

absl::Status StorageEngine::Impl::BeginPartitionDbReplication(
    std::uint64_t session_id, std::uint16_t partition_id, std::uint8_t db_id) {
  if (db_id >= options_.database_count_) {
    return absl::InvalidArgumentError("invalid full-sync database");
  }
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionFor(store, partition_id);
  const auto session = store.fullsync_sessions_.find(session_id);
  if (session == store.fullsync_sessions_.end() ||
      session->second.db_epoch_invalidated_) {
    return absl::FailedPreconditionError("full-sync session is not active");
  }
  auto capture = partition.fullsync_subscribers_.find(session_id);
  if (capture == partition.fullsync_subscribers_.end() ||
      capture->second.phase_ !=
          WorkerStore::FullSyncCapture::Phase::kCapturing) {
    return absl::FailedPreconditionError(
        "full-sync partition capture is not active");
  }
  for (std::uint8_t other = 0; other < kLogicalDatabaseCount; ++other) {
    if (other != db_id &&
        capture->second.db_phases_[other] ==
            WorkerStore::FullSyncCapture::DbPhase::kScanning) {
      return absl::FailedPreconditionError(
          "another database is already scanning in this partition");
    }
  }
  if (capture->second.db_phases_[db_id] !=
      WorkerStore::FullSyncCapture::DbPhase::kUnstarted) {
    return absl::FailedPreconditionError(
        "full-sync database has already started");
  }
  const std::uint32_t target_id =
      (static_cast<std::uint32_t>(partition_id) << 8) | db_id;
  if (session->second.unstarted_admissions_.contains(target_id)) {
    return absl::UnavailableError(
        "full-sync database has an admitted UNSTARTED write");
  }
  capture->second.key_phases_.Clear();
  capture->second.db_phases_[db_id] =
      WorkerStore::FullSyncCapture::DbPhase::kScanning;
  return absl::OkStatus();
}

void StorageEngine::Impl::EndPartitionReplication(std::uint64_t session_id,
                                                  std::uint16_t partition_id) {
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionFor(store, partition_id);
  auto capture = partition.fullsync_subscribers_.find(session_id);
  if (capture == partition.fullsync_subscribers_.end()) return;
  auto session = store.fullsync_sessions_.find(session_id);
  if (session != store.fullsync_sessions_.end()) {
    session->second.publish_queue_bytes_ -=
        std::min(session->second.publish_queue_bytes_,
                 capture->second.replacement_credit_bytes_);
  }
  ClearFullSyncCapture(store, session_id, capture->second);
  partition.fullsync_subscribers_.erase(capture);
  store.fullsync_publisher_capacity_ready_.NotifyAll(*store.worker_);
}

Task<absl::StatusOr<PartitionSnapshotBatch>>
StorageEngine::Impl::SnapshotPartition(std::uint64_t session_id,
                                       std::uint16_t partition_id,
                                       std::uint8_t db_id, std::uint64_t cursor,
                                       std::size_t count,
                                       std::size_t read_concurrency,
                                       std::size_t max_bytes) {
  if (db_id >= options_.database_count_ || count == 0 ||
      read_concurrency == 0 || max_bytes == 0) {
    co_return absl::Status(absl::StatusCode::kInvalidArgument,
                           "invalid partition snapshot request");
  }
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionFor(store, partition_id);
  const auto session = store.fullsync_sessions_.find(session_id);
  auto capture = partition.fullsync_subscribers_.find(session_id);
  if (session == store.fullsync_sessions_.end() ||
      session->second.db_epoch_invalidated_ ||
      capture == partition.fullsync_subscribers_.end() ||
      capture->second.phase_ !=
          WorkerStore::FullSyncCapture::Phase::kCapturing ||
      capture->second.db_phases_[db_id] !=
          WorkerStore::FullSyncCapture::DbPhase::kScanning) {
    co_return absl::Status(absl::StatusCode::kFailedPrecondition,
                           "full-sync partition capture is not active");
  }
  const std::uint64_t baseline_version = capture->second.baseline_version_;
  auto& index = partition.indexes_[db_id];
  const std::uint64_t now_ms = UnixTimeMillis();
  if (capture->second.pending_snapshot_keys_.empty()) {
    auto scanned = co_await ScanPartition(partition_id, db_id, cursor, count,
                                          now_ms, max_bytes);
    if (!scanned.ok()) {
      co_return scanned.status();
    }
    capture = partition.fullsync_subscribers_.find(session_id);
    if (capture == partition.fullsync_subscribers_.end()) {
      co_return absl::FailedPreconditionError(
          "full-sync partition capture ended during scan");
    }
    assert(scanned->keys_.size() == scanned->value_types_.size());
    assert(scanned->keys_.size() == scanned->value_bytes_.size());
    for (std::size_t i = 0; i < scanned->keys_.size(); ++i) {
      capture->second.pending_snapshot_keys_.push_back(
          WorkerStore::FullSyncCapture::PendingSnapshotKey{
              .key_ = std::move(scanned->keys_[i]),
              .value_type_ = scanned->value_types_[i],
              .value_bytes_ = scanned->value_bytes_[i],
          });
    }
    capture->second.pending_snapshot_cursor_ = scanned->cursor_;
  }

  std::vector<std::string> keys;
  keys.reserve(std::min(count, capture->second.pending_snapshot_keys_.size()));
  std::size_t selected_bytes = 0;
  while (!capture->second.pending_snapshot_keys_.empty() &&
         keys.size() < count) {
    const auto& pending = capture->second.pending_snapshot_keys_.front();
    constexpr std::size_t kRecordMetadataBytes = 128;
    std::size_t record_bytes = pending.value_bytes_;
    record_bytes = record_bytes > std::numeric_limits<std::size_t>::max() -
                                      pending.key_.size()
                       ? std::numeric_limits<std::size_t>::max()
                       : record_bytes + pending.key_.size();
    record_bytes = record_bytes > std::numeric_limits<std::size_t>::max() -
                                      kRecordMetadataBytes
                       ? std::numeric_limits<std::size_t>::max()
                       : record_bytes + kRecordMetadataBytes;
    if (!keys.empty() && (selected_bytes >= max_bytes ||
                          record_bytes > max_bytes - selected_bytes)) {
      break;
    }
    selected_bytes =
        record_bytes > std::numeric_limits<std::size_t>::max() - selected_bytes
            ? std::numeric_limits<std::size_t>::max()
            : selected_bytes + record_bytes;
    keys.push_back(
        std::move(capture->second.pending_snapshot_keys_.front().key_));
    capture->second.pending_snapshot_keys_.pop_front();
  }
  const bool has_pending = !capture->second.pending_snapshot_keys_.empty();
  const std::uint64_t next =
      has_pending && capture->second.pending_snapshot_cursor_ == 0
          ? std::numeric_limits<std::uint64_t>::max()
          : capture->second.pending_snapshot_cursor_;
  if (!has_pending) capture->second.pending_snapshot_cursor_ = 0;

  PartitionSnapshotBatch batch;
  batch.cursor_ = next;
  batch.records_.reserve(keys.size());
  std::vector<std::optional<SnapshotRecord>> records(keys.size());
  for (std::size_t first = 0; first < keys.size();) {
    const std::size_t last =
        first + std::min(read_concurrency, keys.size() - first);
    SnapshotReadJoin join;
    join.pending_ = last - first;
    for (std::size_t i = first; i < last; ++i) {
      // Snapshot work is an online replication task, so it remains on the
      // foreground queue. The bounded wave yields naturally between groups.
      store.worker_->Spawn(
          ReadSnapshotRecord(store, partition, index, db_id, &keys[i],
                             session_id, baseline_version, &records[i], &join));
    }
    co_await join.Join();
    if (!join.error_.ok()) {
      co_return join.error_;
    }
    first = last;
  }
  for (std::optional<SnapshotRecord>& record : records) {
    if (record.has_value()) {
      batch.records_.push_back(std::move(*record));
    }
  }
  co_return batch;
}

Task<absl::StatusOr<PartitionFullSyncBatch>>
StorageEngine::Impl::ReadPartitionFullSyncOverrides(std::uint64_t session_id,
                                                    std::uint16_t partition_id,
                                                    std::size_t count,
                                                    std::size_t max_bytes) {
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionFor(store, partition_id);
  const auto session = store.fullsync_sessions_.find(session_id);
  auto capture = partition.fullsync_subscribers_.find(session_id);
  if (session == store.fullsync_sessions_.end() ||
      session->second.db_epoch_invalidated_ ||
      capture == partition.fullsync_subscribers_.end()) {
    co_return absl::FailedPreconditionError(
        "full-sync partition capture is not active");
  }
  PartitionFullSyncBatch batch;
  if (count == 0 || max_bytes == 0) co_return batch;
  std::vector<SnapshotRecord> requested;
  requested.reserve(std::min(count, capture->second.overrides_.size()));
  std::size_t requested_bytes = 0;
  for (const auto& [sequence, record] : capture->second.overrides_) {
    (void)sequence;
    constexpr std::size_t kRecordMetadataBytes = 128;
    const bool predictable = record.kind_ == SnapshotRecord::Kind::kDelete ||
                             record.value_type_ == ValueType::kString;
    std::size_t estimated =
        record.key_.size() >
                std::numeric_limits<std::size_t>::max() - kRecordMetadataBytes
            ? std::numeric_limits<std::size_t>::max()
            : kRecordMetadataBytes + record.key_.size();
    if (record.kind_ == SnapshotRecord::Kind::kValue) {
      estimated = record.logical_size_ >
                          std::numeric_limits<std::size_t>::max() - estimated
                      ? std::numeric_limits<std::size_t>::max()
                      : estimated + record.logical_size_;
    }
    if (!requested.empty() && (!predictable || requested_bytes >= max_bytes ||
                               estimated > max_bytes - requested_bytes)) {
      break;
    }
    requested.push_back(record);
    requested_bytes =
        estimated > std::numeric_limits<std::size_t>::max() - requested_bytes
            ? std::numeric_limits<std::size_t>::max()
            : requested_bytes + estimated;
    if (requested.size() == count) break;
    // Collection logical_size is cardinality, not encoded bytes. Keep such a
    // value exclusive because its materialized size cannot be predicted from
    // replacement metadata.
    if (!predictable) break;
  }
  batch.records_.reserve(requested.size());
  std::size_t batch_bytes = 0;
  for (const SnapshotRecord& record : requested) {
    auto loaded = co_await ReadFullSyncOverrideRecord(store, partition,
                                                      session_id, record);
    if (!loaded.ok()) co_return loaded.status();
    constexpr std::size_t kRecordMetadataBytes = 128;
    const bool streamed = loaded->source_id_ != 0;
    const std::uint64_t effective_value_bytes =
        streamed ? loaded->source_value_bytes_ : loaded->value_.size();
    std::size_t record_bytes = loaded->key_.size();
    record_bytes =
        effective_value_bytes >
                std::numeric_limits<std::size_t>::max() - record_bytes
            ? std::numeric_limits<std::size_t>::max()
            : record_bytes + static_cast<std::size_t>(effective_value_bytes);
    record_bytes = record_bytes > std::numeric_limits<std::size_t>::max() -
                                      kRecordMetadataBytes
                       ? std::numeric_limits<std::size_t>::max()
                       : record_bytes + kRecordMetadataBytes;
    if (!batch.records_.empty() && (streamed || batch_bytes >= max_bytes ||
                                    record_bytes > max_bytes - batch_bytes)) {
      ReleaseFullSyncValue(session_id, partition_id, loaded->source_id_);
      break;
    }
    batch_bytes =
        record_bytes > std::numeric_limits<std::size_t>::max() - batch_bytes
            ? std::numeric_limits<std::size_t>::max()
            : batch_bytes + record_bytes;
    batch.records_.push_back(std::move(*loaded));
    // A streamed record owns the flow's single large-value staging slot until
    // it is sent and ACKed. Do not materialize (and pin) another record in the
    // same batch, even if its stale replacement metadata looked small.
    if (streamed) break;
  }
  co_return batch;
}

Task<absl::StatusOr<SnapshotRecord>>
StorageEngine::Impl::MaterializeFullSyncPublishRecord(
    std::uint64_t session_id, std::uint16_t partition_id,
    const SnapshotRecord& requested) {
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionFor(store, partition_id);
  const auto session = store.fullsync_sessions_.find(session_id);
  const auto capture = partition.fullsync_subscribers_.find(session_id);
  if (session == store.fullsync_sessions_.end() ||
      session->second.db_epoch_invalidated_ ||
      capture == partition.fullsync_subscribers_.end()) {
    co_return absl::FailedPreconditionError(
        "full-sync publish session is not active");
  }
  co_return co_await ReadFullSyncOverrideRecord(store, partition, session_id,
                                                requested);
}

Task<absl::StatusOr<std::string>> StorageEngine::Impl::ReadFullSyncValueChunk(
    std::uint64_t session_id, std::uint16_t partition_id,
    std::uint64_t source_id, std::uint64_t offset, std::size_t max_bytes) {
  if (source_id == 0 || max_bytes == 0) {
    co_return absl::InvalidArgumentError("invalid full-sync value chunk");
  }
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionFor(store, partition_id);
  auto capture = partition.fullsync_subscribers_.find(session_id);
  if (capture == partition.fullsync_subscribers_.end()) {
    co_return absl::FailedPreconditionError(
        "full-sync value capture is not active");
  }
  auto pinned = capture->second.pinned_values_.find(source_id);
  if (pinned == capture->second.pinned_values_.end() ||
      offset >= pinned->second.value_bytes_) {
    co_return absl::OutOfRangeError("full-sync value chunk is out of range");
  }
  if (pinned->second.collection_ != nullptr) {
    auto collection = pinned->second.collection_;
    co_return co_await ReadFullSyncCollectionChunk(std::move(collection),
                                                   offset, max_bytes);
  }
  const ExtentManifest extents = pinned->second.extents_;
  const std::size_t key_bytes = pinned->second.key_bytes_;
  const std::size_t count = static_cast<std::size_t>(
      std::min<std::uint64_t>(max_bytes, pinned->second.value_bytes_ - offset));
  std::string result(count, '\0');
  std::uint64_t absolute = key_bytes + offset;
  std::size_t written = 0;
  std::uint64_t extent_start = 0;
  for (std::size_t index = 0; index < extents->size() && written < count;
       ++index) {
    const ExtentRef ref = extents->at(index);
    const std::uint64_t extent_end = extent_start + ref.payload_bytes_;
    if (absolute >= extent_end) {
      extent_start = extent_end;
      continue;
    }
    const std::size_t source_offset = static_cast<std::size_t>(
        absolute > extent_start ? absolute - extent_start : 0);
    const std::size_t slice = std::min<std::size_t>(
        count - written, ref.payload_bytes_ - source_offset);
    const unsigned owner = BlockOwner(ref.block_id_);
    if (owner >= worker_count_) {
      co_return absl::InternalError("full-sync extent owner is invalid");
    }
    auto read = [this, owner, ref, index, source_offset,
                 destination =
                     reinterpret_cast<std::byte*>(result.data()) + written,
                 slice]() -> Task<absl::Status> {
      co_return co_await ReadExtentSlice(
          *stores_[owner], ref, static_cast<std::uint32_t>(index),
          source_offset, std::span(destination, slice));
    };
    // This is the same GCC 13 coroutine-frame invariant as extent pin/release
    // above; do not fold these suspension points back into a conditional.
    absl::Status status;
    if (owner == store.worker_->id()) {
      status = co_await read();
    } else {
      status = co_await bycorf::SubmitTaskTo(owner, read);
    }
    if (!status.ok()) co_return status;
    written += slice;
    absolute += slice;
    extent_start = extent_end;
  }
  if (written != count) {
    co_return absl::InternalError("full-sync value manifest is truncated");
  }
  co_return result;
}

void StorageEngine::Impl::ReleaseFullSyncValue(std::uint64_t session_id,
                                               std::uint16_t partition_id,
                                               std::uint64_t source_id) {
  if (source_id == 0) return;
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionFor(store, partition_id);
  auto capture = partition.fullsync_subscribers_.find(session_id);
  if (capture == partition.fullsync_subscribers_.end()) return;
  auto pinned = capture->second.pinned_values_.find(source_id);
  if (pinned == capture->second.pinned_values_.end()) return;
  auto collection = std::move(pinned->second.collection_);
  ExtentManifest extents = std::move(pinned->second.extents_);
  capture->second.pinned_values_.erase(pinned);
  if (collection != nullptr) {
    active_settlements_.fetch_add(1, std::memory_order_acq_rel);
    store.worker_->Spawn(ReleaseFullSyncCollection(std::move(collection)));
  } else {
    store.worker_->Spawn(ReleaseFullSyncExtents(std::move(extents)));
  }
}

void StorageEngine::Impl::AcknowledgePartitionFullSyncOverrides(
    std::uint64_t session_id, std::uint16_t partition_id,
    std::span<const SnapshotRecord> records) {
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionFor(store, partition_id);
  const auto session = store.fullsync_sessions_.find(session_id);
  auto capture = partition.fullsync_subscribers_.find(session_id);
  if (session == store.fullsync_sessions_.end() ||
      session->second.db_epoch_invalidated_ ||
      capture == partition.fullsync_subscribers_.end()) {
    return;
  }
  for (const SnapshotRecord& acknowledged : records) {
    ReleaseFullSyncValue(session_id, partition_id, acknowledged.source_id_);
    auto& latest_by_key = capture->second.latest_by_key_[acknowledged.db_id_];
    auto latest = latest_by_key.find(acknowledged.key_);
    if (latest == latest_by_key.end() ||
        latest->second > acknowledged.mutation_sequence_) {
      continue;
    }
    auto current = capture->second.overrides_.find(latest->second);
    if (current == capture->second.overrides_.end()) continue;
    const bool returns_to_coverage =
        capture->second.phase_ ==
            WorkerStore::FullSyncCapture::Phase::kCapturing &&
        capture->second.db_phases_[acknowledged.db_id_] ==
            WorkerStore::FullSyncCapture::DbPhase::kScanning;
    if (returns_to_coverage &&
        !TryConsumeFullSyncArenaCredit(store, session_id, capture->second)) {
      return;
    }
    std::size_t credit = kFullSyncReplacementMetadataBytes;
    if (acknowledged.key_.size() <=
        (std::numeric_limits<std::size_t>::max() - credit) / 2) {
      credit += acknowledged.key_.size() * 2;
    } else {
      credit = capture->second.replacement_credit_bytes_;
    }
    latest_by_key.erase(latest);
    capture->second.overrides_.erase(current);
    capture->second.replacement_credit_bytes_ -=
        std::min(capture->second.replacement_credit_bytes_, credit);
    session->second.publish_queue_bytes_ -=
        std::min(session->second.publish_queue_bytes_, credit);
    if (returns_to_coverage) {
      // This identity already owns coverage credit transferred from the
      // acknowledged override; no new session headroom is consumed here.
      const auto restored = capture->second.key_phases_.InsertOrAssign(
          ComputeDigest(acknowledged.key_), acknowledged.key_,
          WorkerStore::FullSyncCapture::KeyPhase::kTailingWithOverrideCredit,
          acknowledged.key_.size() <= options_.inline_key_max_bytes_);
      if (restored.rejected_) {
        InvalidateFullSyncSession(store, session_id);
        return;
      }
    }
  }
  if (capture->second.overrides_.empty()) {
    // Release the key-to-sequence lookup as soon as every materialized
    // replacement is acknowledged. The identity credit remains consumed
    // while key_phases_ still represents the completed scan window and is
    // restored when the entire partition capture is cleared.
    for (auto& latest : capture->second.latest_by_key_) {
      latest.clear();
      latest.rehash(0);
    }
  }
  store.fullsync_publisher_capacity_ready_.NotifyAll(*store.worker_);
}

void StorageEngine::Impl::AcknowledgePartitionSnapshotRecords(
    std::uint64_t session_id, std::uint16_t partition_id,
    std::span<const SnapshotRecord> records) {
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionFor(store, partition_id);
  auto capture = partition.fullsync_subscribers_.find(session_id);
  if (capture == partition.fullsync_subscribers_.end() ||
      capture->second.phase_ !=
          WorkerStore::FullSyncCapture::Phase::kCapturing) {
    return;
  }
  for (const SnapshotRecord& record : records) {
    ReleaseFullSyncValue(session_id, partition_id, record.source_id_);
    const std::string& key = record.key_;
    auto* phase = capture->second.key_phases_.Find(record.key_digest_, key);
    if (phase == nullptr ||
        phase->value_ !=
            WorkerStore::FullSyncCapture::KeyPhase::kBaselineInflight) {
      continue;
    }
    // A transaction participant may have fallen back to a newer replacement
    // while this baseline was in flight. In that case the baseline ACK must
    // not make the key command-eligible yet.
    if (capture->second.latest_by_key_[record.db_id_].contains(record.key_)) {
      capture->second.key_phases_.Erase(phase);
    } else {
      phase->value_ = WorkerStore::FullSyncCapture::KeyPhase::kTailing;
    }
  }
}

absl::Status StorageEngine::Impl::CompletePartitionReplication(
    std::uint64_t session_id, std::uint16_t partition_id) {
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionFor(store, partition_id);
  auto capture = partition.fullsync_subscribers_.find(session_id);
  if (capture == partition.fullsync_subscribers_.end()) {
    return absl::FailedPreconditionError(
        "full-sync partition capture is not active");
  }
  if (!capture->second.overrides_.empty()) {
    return absl::UnavailableError(
        "full-sync partition still has pending replacements");
  }
  if (std::any_of(capture->second.db_phases_.begin(),
                  capture->second.db_phases_.end(), [](auto phase) {
                    return phase !=
                           WorkerStore::FullSyncCapture::DbPhase::kTailing;
                  })) {
    return absl::FailedPreconditionError(
        "full-sync partition still has an incomplete database");
  }
  capture->second.phase_ = WorkerStore::FullSyncCapture::Phase::kTailing;
  for (auto& latest : capture->second.latest_by_key_) {
    latest.clear();
    latest.rehash(0);
  }
  capture->second.key_phases_.Clear();
  return absl::OkStatus();
}

absl::Status StorageEngine::Impl::CompletePartitionDbReplication(
    std::uint64_t session_id, std::uint16_t partition_id, std::uint8_t db_id) {
  if (db_id >= options_.database_count_) {
    return absl::InvalidArgumentError("invalid full-sync database");
  }
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionFor(store, partition_id);
  auto capture = partition.fullsync_subscribers_.find(session_id);
  if (capture == partition.fullsync_subscribers_.end() ||
      capture->second.db_phases_[db_id] !=
          WorkerStore::FullSyncCapture::DbPhase::kScanning) {
    return absl::FailedPreconditionError(
        "full-sync database scan is not active");
  }
  const bool pending = std::any_of(
      capture->second.overrides_.begin(), capture->second.overrides_.end(),
      [db_id](const auto& entry) { return entry.second.db_id_ == db_id; });
  if (pending) {
    return absl::UnavailableError(
        "full-sync database still has pending replacements");
  }
  if (!capture->second.pinned_values_.empty()) {
    return absl::UnavailableError(
        "full-sync database still has unacknowledged value streams");
  }
  capture->second.db_phases_[db_id] =
      WorkerStore::FullSyncCapture::DbPhase::kTailing;
  // Only one DB scans at a time. With no replacement left for this DB, all
  // coverage structures belong to the completed scan and can be destroyed
  // before its logical credit is made available to the next DB.
  capture->second.key_phases_ =
      ScanHashMap<WorkerStore::FullSyncCapture::KeyPhase>{};
  decltype(capture->second.pending_snapshot_keys_){}.swap(
      capture->second.pending_snapshot_keys_);
  capture->second.pending_snapshot_cursor_ = 0;
  for (auto& latest : capture->second.latest_by_key_) {
    latest.clear();
    latest.rehash(0);
  }
  capture->second.pinned_values_.clear();
  capture->second.pinned_values_.rehash(0);
  RestoreFullSyncCoverageCredit(store, session_id, capture->second);
  return absl::OkStatus();
}

absl::StatusOr<std::vector<FullSyncPublishItem>>
StorageEngine::Impl::PeekFullSyncPublishItems(std::uint64_t session_id,
                                              std::size_t max_items) {
  WorkerStore& store = CurrentStore();
  auto session = store.fullsync_sessions_.find(session_id);
  if (session == store.fullsync_sessions_.end() ||
      session->second.db_epoch_invalidated_) {
    return absl::FailedPreconditionError(
        "full-sync publish session is not active");
  }
  if (max_items == 0) {
    return absl::InvalidArgumentError(
        "full-sync publish batch size must be nonzero");
  }
  std::vector<FullSyncPublishItem> result;
  result.reserve(std::min(max_items, session->second.publish_queue_.size()));
  const std::size_t count =
      std::min(max_items, session->second.publish_queue_.size());
  for (std::size_t index = 0; index < count; ++index) {
    const auto& pending = session->second.publish_queue_[index];
    result.push_back(FullSyncPublishItem{.id_ = pending.id_,
                                         .command_ = pending.command_,
                                         .record_ = pending.record_});
  }
  return result;
}

absl::StatusOr<FullSyncPublishQueueInfo>
StorageEngine::Impl::GetFullSyncPublishQueueInfo(
    std::uint64_t session_id) const {
  const WorkerStore& store = CurrentStore();
  const auto session = store.fullsync_sessions_.find(session_id);
  if (session == store.fullsync_sessions_.end() ||
      session->second.db_epoch_invalidated_) {
    return absl::FailedPreconditionError(
        "full-sync publish session is not active");
  }
  return FullSyncPublishQueueInfo{
      .queued_bytes_ = session->second.publish_queue_bytes_,
      .admitted_bytes_ = session->second.publisher_admitted_bytes_,
      .capacity_bytes_ =
          replication_publish_queue_bytes_.load(std::memory_order_acquire),
  };
}

void StorageEngine::Impl::AcknowledgeFullSyncPublishItem(
    std::uint64_t session_id, std::uint64_t item_id) {
  WorkerStore& store = CurrentStore();
  auto session = store.fullsync_sessions_.find(session_id);
  if (session == store.fullsync_sessions_.end() ||
      session->second.publish_queue_.empty() ||
      session->second.publish_queue_.front().id_ != item_id) {
    return;
  }
  const std::size_t bytes =
      session->second.publish_queue_.front().staging_bytes_;
  session->second.publish_queue_.pop_front();
  session->second.publish_queue_bytes_ -=
      std::min(session->second.publish_queue_bytes_, bytes);
  store.fullsync_publisher_capacity_ready_.NotifyAll(*store.worker_);
}

Task<absl::StatusOr<std::uint64_t>> StorageEngine::Impl::ResetReplicaPartition(
    std::uint16_t partition_id,
    std::span<const std::uint64_t, kLogicalDatabaseCount> source_db_epochs,
    std::uint64_t persisted_replication_epoch, bool replica_lock_held) {
  WorkerStore& store = CurrentStore();
  std::unique_ptr<UnlockGuard> replica_unlock;
  if (!replica_lock_held) {
    co_await store.replica_apply_mutex_.Lock();
    replica_unlock = std::make_unique<UnlockGuard>(&store.replica_apply_mutex_,
                                                   store.worker_);
  }
  if (persisted_replication_epoch == 0) {
    for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
      absl::Status advanced =
          co_await AdvanceDbEpoch(db_id, source_db_epochs[db_id]);
      if (!advanced.ok()) {
        co_return advanced;
      }
    }
  }

  auto& partition = PartitionFor(store, partition_id);
  if (partition.replication_epoch_ ==
      std::numeric_limits<std::uint64_t>::max()) {
    co_return absl::Status(absl::StatusCode::kOutOfRange,
                           "partition replication epoch exhausted");
  }
  const auto aborted_stage = co_await AbortReplicaValueStage(store, partition);
  if (!aborted_stage.ok()) co_return aborted_stage;
  // Stop this worker's append stream before making the new epoch durable.
  // Otherwise a concurrent command could append an old-epoch record after
  // the metadata commit and receive OK even though restart must discard it.
  co_await store.store_state_mutex_.Lock();
  UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
  const std::uint64_t next_epoch = partition.replication_epoch_ + 1;
  if (persisted_replication_epoch != 0 &&
      persisted_replication_epoch != next_epoch) {
    co_return absl::Status(absl::StatusCode::kAborted,
                           "replica reset superseded by another session");
  }
  if (persisted_replication_epoch == 0) {
    absl::Status persisted = co_await PersistEpochValue(
        kLogicalDatabaseCount + partition_id, next_epoch);
    if (!persisted.ok()) {
      co_return persisted;
    }
  }

  struct OldKey {
    std::uint8_t db_id_ = 0;
    std::string key_;
  };
  std::vector<OldKey> old_keys;
  for (std::uint8_t db_id = 0; db_id < options_.database_count_; ++db_id) {
    struct ExternalKey {
      RecordLocation location_;
      ExtentManifest extents_;
      std::uint32_t key_bytes_ = 0;
    };
    std::vector<ExternalKey> external_keys;
    partition.indexes_[db_id].ForEach([&](const RecordIndex::Entry& entry) {
      if (entry.key_complete()) [[likely]] {
        old_keys.push_back(
            OldKey{.db_id_ = db_id, .key_ = std::string(entry.key())});
      } else [[unlikely]] {
        external_keys.push_back(ExternalKey{
            .location_ = MaterializeIndexLocation(entry),
            .extents_ = ExtentsFor(store, &entry),
            .key_bytes_ = entry.logical_key_size(),
        });
      }
    });
    for (const ExternalKey& external : external_keys) {
      auto key = co_await LoadOutOfIndexKey(
          store, external.location_, external.extents_, external.key_bytes_);
      if (!key.ok()) {
        co_return key.status();
      }
      old_keys.push_back(OldKey{.db_id_ = db_id, .key_ = std::move(*key)});
    }
  }
  partition.replication_epoch_ = next_epoch;
  partition.mutation_sequence_ = 0;
  for (auto& [session_id, capture] : partition.fullsync_subscribers_) {
    ClearFullSyncCapture(store, session_id, capture);
  }
  partition.fullsync_subscribers_.clear();
  partition.replica_value_stage_.reset();
  for (const OldKey& old : old_keys) {
    const std::uint8_t db_id = old.db_id_;
    const std::string& key = old.key_;
    const Digest digest = ComputeDigest(key);
    const bool key_external = key.size() > options_.inline_key_max_bytes_;
    const bool external =
        AlignRecord(RecordHeaderBytes(key.size(), key_external) +
                    (key_external ? key.size() : 0)) >
        kStorageBlockBytes - kBlockHeaderBytes;
    ExtentManifest extents;
    std::string manifest;
    if (external) [[unlikely]] {
      auto written = co_await WriteExtentValueLocked(store, key);
      if (!written.ok()) {
        co_return written.status();
      }
      extents = std::move(*written);
      manifest = EncodeManifest(*extents);
    }
    absl::Status tombstone = co_await WriteRecordLocked(
        store, db_id, key, manifest, RecordKind::kTombstone, ValueType::kNone,
        0, digest, 0, 0, false, false, external, key_external, 0, extents,
        nullptr, nullptr, nullptr);
    if (!tombstone.ok()) {
      if (extents != nullptr) [[unlikely]] {
        SpawnExtentReclaim(store, extents);
      }
      co_return tombstone;
    }
  }
  co_return next_epoch;
}

Task<absl::StatusOr<std::vector<ReplicaPartitionEpoch>>>
StorageEngine::Impl::ResetReplicaPartitions(
    std::uint64_t session_id, std::span<const ReplicaPartitionReset> resets) {
  if (session_id == 0 || resets.empty()) {
    co_return absl::Status(absl::StatusCode::kInvalidArgument,
                           "replica reset batch is empty");
  }
  WorkerStore& store = CurrentStore();
  co_await store.replica_apply_mutex_.Lock();
  UnlockGuard replica_unlock(&store.replica_apply_mutex_, store.worker_);
  std::array<bool, kLogicalStorageShards> seen{};
  std::array<std::uint64_t, kLogicalDatabaseCount> local_db_epochs{};
  for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
    const std::uint64_t current = DbEpoch(db_id);
    if (current == std::numeric_limits<std::uint64_t>::max()) {
      co_return absl::OutOfRangeError("database epoch exhausted");
    }
    local_db_epochs[db_id] = current + 1;
  }
  std::vector<std::pair<std::size_t, std::uint64_t>> epoch_updates;
  epoch_updates.reserve(resets.size());
  for (const ReplicaPartitionReset& reset : resets) {
    if (reset.partition_id_ >= kLogicalStorageShards ||
        reset.partition_id_ % worker_count_ != bycorf::ThisWorker().id_ ||
        seen[reset.partition_id_]) {
      co_return absl::Status(absl::StatusCode::kInvalidArgument,
                             "invalid replica reset batch partition");
    }
    seen[reset.partition_id_] = true;
    for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
      if (reset.db_epochs_[db_id] == 0) {
        co_return absl::Status(absl::StatusCode::kInvalidArgument,
                               "invalid replica reset batch database epoch");
      }
    }
    const auto& partition = PartitionFor(CurrentStore(), reset.partition_id_);
    if (partition.replica_sync_ != nullptr) {
      co_return absl::FailedPreconditionError(
          "replica partition is already being synchronized");
    }
    if (partition.replica_candidate_epoch_ ==
        std::numeric_limits<std::uint64_t>::max()) {
      co_return absl::Status(absl::StatusCode::kOutOfRange,
                             "partition replication epoch exhausted");
    }
    epoch_updates.emplace_back(kLogicalDatabaseCount + reset.partition_id_,
                               partition.replica_candidate_epoch_ + 1);
  }
  // The persisted candidate partition epochs exclude the old population.
  // Candidate writes also carry local_db_epochs (current + 1), which promotion
  // alone persists; recovery's earlier DB-epoch filter therefore excludes an
  // interrupted partial candidate before it decodes or follows its extents.
  absl::Status persisted = co_await PersistEpochValues(epoch_updates);
  if (!persisted.ok()) co_return persisted;

  co_await store.store_state_mutex_.Lock();
  UnlockGuard write_unlock(&store.store_state_mutex_, store.worker_);
  std::vector<ReplicaPartitionEpoch> result;
  result.reserve(resets.size());
  for (std::size_t index = 0; index < resets.size(); ++index) {
    const ReplicaPartitionReset& reset = resets[index];
    auto& partition = PartitionFor(store, reset.partition_id_);
    if (partition.replica_sync_ != nullptr) {
      co_return absl::FailedPreconditionError(
          "replica partition is already being synchronized");
    }
    auto sync =
        std::make_unique<WorkerStore::PartitionStore::ReplicaSyncState>();
    sync->session_id_ = session_id;
    sync->replication_epoch_ = epoch_updates[index].second;
    sync->source_db_epochs_ = reset.db_epochs_;
    sync->local_db_epochs_ = local_db_epochs;
    for (std::uint8_t db_id = 0; db_id < options_.database_count_; ++db_id) {
      ++partition.grouped_generations_[db_id];
      QueueDetachedIndex(store, partition.indexes_[db_id], db_id,
                         &partition.grouped_objects_[db_id]);
      partition.fullsync_coverage_bytes_[db_id] = 0;
      if (store.live_key_count_[db_id] < partition.live_key_count_[db_id])
          [[unlikely]] {
        co_return absl::InternalError(
            "replica reset found inconsistent live-key accounting");
      }
      store.live_key_count_[db_id] -= partition.live_key_count_[db_id];
      partition.live_key_count_[db_id] = 0;
      partition.expiring_key_count_[db_id] = 0;
    }
    partition.replica_candidate_epoch_ = epoch_updates[index].second;
    partition.replication_epoch_ = epoch_updates[index].second;
    partition.mutation_sequence_ = 0;
    partition.replica_value_stage_.reset();
    partition.replica_sync_ = std::move(sync);
    result.push_back(ReplicaPartitionEpoch{
        .partition_id_ = reset.partition_id_,
        .replication_epoch_ = epoch_updates[index].second,
    });
  }
  for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
    ++store.index_generations_[db_id];
    tx::CurrentTxShard().MarkAllWatched(db_id);
  }
  replica_loading_.store(true, std::memory_order_release);
  EnsureDetachedReclaim(store);
  co_return result;
}

Task<absl::Status> StorageEngine::Impl::ResetPartitionsDetach(
    std::span<const std::uint16_t> partition_ids) {
  if (partition_ids.empty()) co_return absl::OkStatus();
  if (bycorf::ThisWorker().id_ != 0) {
    std::vector<std::uint16_t> copied(partition_ids.begin(),
                                      partition_ids.end());
    co_return co_await bycorf::SubmitTaskTo(
        0, [this, copied = std::move(copied)]() {
          return ResetPartitionsDetach(copied);
        });
  }

  std::array<bool, kLogicalStorageShards> seen{};
  std::vector<std::vector<std::uint16_t>> by_worker(worker_count_);
  for (const std::uint16_t partition_id : partition_ids) {
    if (partition_id >= kLogicalStorageShards || seen[partition_id]) {
      co_return absl::InvalidArgumentError(
          "invalid or duplicate partition in targeted reset");
    }
    seen[partition_id] = true;
    by_worker[partition_id % worker_count_].push_back(partition_id);
  }
  for (unsigned worker = 0; worker < worker_count_; ++worker) {
    if (by_worker[worker].empty()) continue;
    absl::Status reset;
    if (worker == 0) {
      reset = co_await ResetPartitionsDetachLocal(by_worker[worker]);
    } else {
      reset = co_await bycorf::SubmitTaskTo(
          worker, [this, ids = std::move(by_worker[worker])]() {
            return ResetPartitionsDetachLocal(ids);
          });
    }
    if (!reset.ok()) co_return reset;
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::ResetPartitionsDetachLocal(
    std::span<const std::uint16_t> partition_ids) {
  if (partition_ids.empty()) co_return absl::OkStatus();
  WorkerStore& store = CurrentStore();
  co_await store.replica_apply_mutex_.Lock();
  UnlockGuard replica_unlock(&store.replica_apply_mutex_, store.worker_);

  std::vector<std::pair<std::size_t, std::uint64_t>> epoch_updates;
  epoch_updates.reserve(partition_ids.size());
  for (const std::uint16_t partition_id : partition_ids) {
    if (partition_id >= kLogicalStorageShards ||
        partition_id % worker_count_ != bycorf::ThisWorker().id_) {
      co_return absl::InvalidArgumentError(
          "targeted reset partition belongs to another worker");
    }
    const auto& partition = PartitionFor(store, partition_id);
    if (partition.replica_sync_ != nullptr) {
      co_return absl::FailedPreconditionError(
          "targeted reset conflicts with native replica synchronization");
    }
    if (partition.replica_candidate_epoch_ ==
        std::numeric_limits<std::uint64_t>::max()) {
      co_return absl::OutOfRangeError("partition replication epoch exhausted");
    }
    epoch_updates.emplace_back(kLogicalDatabaseCount + partition_id,
                               partition.replica_candidate_epoch_ + 1);
  }
  // Durably fence old records before making the detached indexes invisible.
  absl::Status persisted = co_await PersistEpochValues(epoch_updates);
  if (!persisted.ok()) co_return persisted;

  co_await store.store_state_mutex_.Lock();
  UnlockGuard write_unlock(&store.store_state_mutex_, store.worker_);
  for (std::size_t i = 0; i < partition_ids.size(); ++i) {
    auto& partition = PartitionFor(store, partition_ids[i]);
    for (std::uint8_t db_id = 0; db_id < options_.database_count_; ++db_id) {
      ++partition.grouped_generations_[db_id];
      QueueDetachedIndex(store, partition.indexes_[db_id], db_id,
                         &partition.grouped_objects_[db_id]);
      partition.fullsync_coverage_bytes_[db_id] = 0;
      if (store.live_key_count_[db_id] < partition.live_key_count_[db_id]) {
        co_return absl::InternalError(
            "targeted reset found inconsistent live-key accounting");
      }
      store.live_key_count_[db_id] -= partition.live_key_count_[db_id];
      partition.live_key_count_[db_id] = 0;
      partition.expiring_key_count_[db_id] = 0;
    }
    partition.replica_candidate_epoch_ = epoch_updates[i].second;
    partition.replication_epoch_ = epoch_updates[i].second;
    partition.mutation_sequence_ = 0;
    partition.replica_value_stage_.reset();
  }
  for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
    ++store.index_generations_[db_id];
    tx::CurrentTxShard().MarkAllWatched(db_id);
  }
  EnsureDetachedReclaim(store);
  co_return absl::OkStatus();
}

// TODO(replication): ResetReplicaPartition above holds store_state_mutex across
// its whole tombstone loop (unlock_writer_while_waiting=false), so on a full
// device its inline block allocation waits for reclaim progress while the
// flush that would free space is itself waiting for this store_state_mutex — a
// three-way stall that never resolves. When the epoch redesign lands, the
// loop should release the mutex around allocation waits and revalidate
// (db_epoch, replication_epoch, index_generation) afterwards, the same
// expected-version handoff defrag relocation uses.
//
// Replica apply deliberately bypasses source publication. ReplicationManager
// rejects downstream native sessions while an upstream is configured, so this
// path never has to act as a cascading relay.
//
// TODO(replication): this path also bypasses the command layer's database
// gates (file-static in command.cpp), which KEYS and FLUSHDB close to get an
// exclusive, still keyspace. On a replica, apply traffic keeps mutating the
// index between KEYS's counting and emitting passes — the announced *N can
// disagree with the emitted element count, desynchronizing that client's
// RESP stream — and FLUSHDB's drain-then-detach exclusivity assumption does
// not hold either. The redesign should either route apply through the gates
// or pause application while a gated operation is in flight.
Task<absl::Status> StorageEngine::Impl::HandoffReplicaPartition(
    std::uint64_t session_id, std::uint16_t partition_id,
    std::uint64_t replication_epoch) {
  WorkerStore& store = CurrentStore();
  if (partition_id >= kLogicalStorageShards ||
      partition_id % worker_count_ != bycorf::ThisWorker().id_) {
    co_return absl::InvalidArgumentError(
        "replica handoff partition belongs to another worker");
  }
  co_await store.replica_apply_mutex_.Lock();
  UnlockGuard replica_unlock(&store.replica_apply_mutex_, store.worker_);
  co_await store.store_state_mutex_.Lock();
  UnlockGuard write_unlock(&store.store_state_mutex_, store.worker_);
  auto& partition = PartitionFor(store, partition_id);
  auto* sync = partition.replica_sync_.get();
  if (sync == nullptr || sync->session_id_ != session_id ||
      sync->replication_epoch_ != replication_epoch) {
    co_return absl::FailedPreconditionError("stale replica partition handoff");
  }
  if (sync->stream_failed_ || partition.replica_value_stage_.has_value()) {
    co_return absl::FailedPreconditionError(
        "replica partition handoff interrupted a large value");
  }
  sync->tailing_ = true;
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::BeginReplicaTailCommand(
    std::uint64_t session_id, std::uint16_t partition_id,
    std::uint64_t partition_sequence) {
  if (partition_sequence == 0 || partition_id >= kLogicalStorageShards ||
      partition_id % worker_count_ != bycorf::ThisWorker().id_) {
    co_return absl::InvalidArgumentError(
        "invalid replica tail command identity");
  }
  WorkerStore& store = CurrentStore();
  co_await store.store_state_mutex_.Lock();
  UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
  auto& partition = PartitionFor(store, partition_id);
  auto* sync = partition.replica_sync_.get();
  if (sync == nullptr || sync->session_id_ != session_id ||
      sync->command_sequence_.has_value() || sync->stream_failed_ ||
      partition.replica_value_stage_.has_value()) {
    co_return absl::FailedPreconditionError(
        "replica command is outside its apply window");
  }
  sync->command_sequence_ = partition_sequence;
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::EndReplicaTailCommand(
    std::uint64_t session_id, std::uint16_t partition_id,
    std::uint64_t partition_sequence) {
  WorkerStore& store = CurrentStore();
  co_await store.store_state_mutex_.Lock();
  UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
  auto& partition = PartitionFor(store, partition_id);
  auto* sync = partition.replica_sync_.get();
  if (sync == nullptr || sync->session_id_ != session_id ||
      sync->command_sequence_ != partition_sequence) {
    co_return absl::FailedPreconditionError(
        "replica tail command context changed during apply");
  }
  sync->command_sequence_.reset();
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::ApplyReplicaRecords(
    std::uint64_t session_id, std::uint16_t partition_id,
    std::uint64_t replication_epoch, std::span<const SnapshotRecord> records) {
  WorkerStore& store = CurrentStore();
  if (partition_id >= kLogicalStorageShards ||
      partition_id % worker_count_ != store.worker_->id())
    co_return absl::InvalidArgumentError(
        "invalid replica apply partition owner");
  co_await store.replica_apply_mutex_.Lock();
  UnlockGuard replica_unlock(&store.replica_apply_mutex_, store.worker_);
  absl::Status status;
  try {
    status = co_await ApplyReplicaRecordsLocked(session_id, partition_id,
                                                replication_epoch, records);
  } catch (const std::bad_alloc&) {
    status =
        absl::ResourceExhaustedError("replica collection allocation failed");
  }
  auto& partition = PartitionFor(store, partition_id);
  auto* sync = partition.replica_sync_.get();
  if (!status.ok() && sync && sync->session_id_ == session_id &&
      sync->replication_epoch_ == replication_epoch) {
    sync->stream_failed_ = true;
    const auto aborted = co_await AbortReplicaValueStage(store, partition);
    if (!aborted.ok()) co_return aborted;
  }
  co_return status;
}

Task<absl::Status> StorageEngine::Impl::ApplyReplicaRecordsLocked(
    std::uint64_t session_id, std::uint16_t partition_id,
    std::uint64_t replication_epoch, std::span<const SnapshotRecord> records) {
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionFor(store, partition_id);
  auto* sync = partition.replica_sync_.get();
  if (sync == nullptr || sync->session_id_ != session_id ||
      replication_epoch != sync->replication_epoch_ || sync->stream_failed_) {
    co_return absl::Status(absl::StatusCode::kFailedPrecondition,
                           "stale partition replication epoch");
  }
  for (const SnapshotRecord& record : records) {
    std::optional<SnapshotRecord> materialized;
    const SnapshotRecord* effective = &record;
    if (record.db_id_ >= options_.database_count_ ||
        RedisSlot(record.key_) != partition_id) {
      co_return absl::Status(absl::StatusCode::kInvalidArgument,
                             "replica record belongs to another partition");
    }
    if (record.db_epoch_ != sync->source_db_epochs_[record.db_id_]) {
      co_return absl::Status(absl::StatusCode::kFailedPrecondition,
                             "replica record database epoch changed");
    }

    if (record.kind_ == SnapshotRecord::Kind::kValueBegin) {
      std::uint64_t value_logical_size = 0;
      if (record.value_.size() == sizeof(value_logical_size)) {
        for (std::size_t byte = 0; byte < sizeof(value_logical_size); ++byte) {
          value_logical_size |=
              static_cast<std::uint64_t>(
                  static_cast<unsigned char>(record.value_[byte]))
              << (byte * 8);
        }
      }
      const bool nonempty_collection =
          record.value_type_ == ValueType::kList ||
          record.value_type_ == ValueType::kHash ||
          record.value_type_ == ValueType::kSet ||
          record.value_type_ == ValueType::kSortedSet;
      if (partition.replica_value_stage_.has_value() ||
          (record.value_type_ != ValueType::kString &&
           record.value_type_ != ValueType::kList &&
           record.value_type_ != ValueType::kHash &&
           record.value_type_ != ValueType::kSet &&
           record.value_type_ != ValueType::kSortedSet &&
           record.value_type_ != ValueType::kStream) ||
          record.value_.size() != sizeof(value_logical_size) ||
          (nonempty_collection && value_logical_size == 0) ||
          (record.value_type_ == ValueType::kString &&
           value_logical_size != record.logical_size_) ||
          value_logical_size > std::numeric_limits<std::uint32_t>::max() ||
          record.logical_size_ == 0 ||
          (!nonempty_collection && record.logical_size_ > kMaxBitmapBytes) ||
          record.chunk_count_ == 0 ||
          record.chunk_count_ !=
              (record.logical_size_ - 1) / kReplicationTransferBytes + 1) {
        co_return absl::Status(absl::StatusCode::kInvalidArgument,
                               "invalid replicated large value begin frame");
      }
      constexpr std::size_t kReplicaStageBookkeepingAllowance = 4096;
      std::size_t stage_bytes = kReplicaStageBookkeepingAllowance;
      if (record.key_.size() >
              std::numeric_limits<std::size_t>::max() - stage_bytes ||
          (!nonempty_collection &&
           record.logical_size_ > std::numeric_limits<std::size_t>::max() -
                                      stage_bytes - record.key_.size())) {
        co_return absl::ResourceExhaustedError(
            "replica large-value staging size overflow");
      }
      stage_bytes += record.key_.size();
      if (!nonempty_collection)
        stage_bytes += static_cast<std::size_t>(record.logical_size_);
      auto stage_reservation = TryReserveMemory(stage_bytes);
      if (!stage_reservation.has_value()) {
        RecordMemoryRejection();
        co_return absl::ResourceExhaustedError(
            "replica large-value staging exceeds this worker's retained-memory "
            "budget");
      }
      partition.replica_value_stage_ = ReplicaValueStage{
          .db_id_ = record.db_id_,
          .db_epoch_ = record.db_epoch_,
          .mutation_sequence_ = record.mutation_sequence_,
          .expire_at_ms_ = record.expire_at_ms_,
          .logical_size_ = value_logical_size,
          .encoded_size_ = record.logical_size_,
          .next_chunk_ = 0,
          .chunk_count_ = record.chunk_count_,
          .value_type_ = record.value_type_,
          .key_ = record.key_,
          .value_ = {},
          .collection_ = nullptr,
          .memory_charge_ = {},
      };
      // Reserve once while the memory permit is live. Chunks append within
      // this capacity, so a peer cannot create an unaccounted allocation at
      // an arbitrary point later in the stream.
      partition.replica_value_stage_->memory_charge_.Adopt(&*stage_reservation,
                                                           stage_bytes);
      if (nonempty_collection) {
        const auto started = co_await BeginReplicaCollection(
            store, partition, *partition.replica_value_stage_);
        if (!started.ok()) co_return started;
      } else {
        partition.replica_value_stage_->value_.reserve(
            static_cast<std::size_t>(record.logical_size_));
      }
      continue;
    }
    if (record.kind_ == SnapshotRecord::Kind::kValueChunk) {
      auto& stage = partition.replica_value_stage_;
      if (!stage.has_value() || stage->db_id_ != record.db_id_ ||
          stage->db_epoch_ != record.db_epoch_ ||
          stage->mutation_sequence_ != record.mutation_sequence_ ||
          stage->key_ != record.key_ ||
          stage->next_chunk_ != record.chunk_index_ ||
          stage->chunk_count_ != record.chunk_count_ || record.value_.empty() ||
          record.value_.size() > kReplicationTransferBytes ||
          record.value_.size() > stage->encoded_size_ ||
          (stage->collection_ ? stage->collection_->received_bytes_
                              : stage->value_.size()) >
              stage->encoded_size_ - record.value_.size()) {
        co_return absl::Status(absl::StatusCode::kInvalidArgument,
                               "invalid replicated large value chunk frame");
      }
      if (stage->collection_) {
        const auto consumed = co_await ConsumeReplicaCollection(
            store, partition, *stage, record.value_, false);
        if (!consumed.ok()) co_return consumed;
      } else {
        stage->value_.append(record.value_);
      }
      ++stage->next_chunk_;
      continue;
    }
    if (record.kind_ == SnapshotRecord::Kind::kValueCommit) {
      auto& stage = partition.replica_value_stage_;
      if (!stage.has_value() || stage->db_id_ != record.db_id_ ||
          stage->db_epoch_ != record.db_epoch_ ||
          stage->mutation_sequence_ != record.mutation_sequence_ ||
          stage->key_ != record.key_ || !record.value_.empty() ||
          stage->next_chunk_ != stage->chunk_count_ ||
          record.chunk_index_ != stage->chunk_count_ ||
          (stage->collection_ ? stage->collection_->received_bytes_
                              : stage->value_.size()) != stage->encoded_size_) {
        co_return absl::Status(absl::StatusCode::kInvalidArgument,
                               "invalid replicated large value commit frame");
      }
      if (stage->collection_) {
        const auto complete = co_await ConsumeReplicaCollection(
            store, partition, *stage, {}, true);
        if (!complete.ok()) co_return complete;
        stage.reset();
        continue;
      }
      materialized.emplace(SnapshotRecord{
          .kind_ = SnapshotRecord::Kind::kValue,
          .db_id_ = stage->db_id_,
          .db_epoch_ = stage->db_epoch_,
          .mutation_sequence_ = stage->mutation_sequence_,
          .expire_at_ms_ = stage->expire_at_ms_,
          .value_type_ = stage->value_type_,
          .logical_size_ = stage->logical_size_,
          .key_ = std::move(stage->key_),
          .value_ = std::move(stage->value_),
      });
      stage.reset();
      effective = &*materialized;
    } else if (partition.replica_value_stage_.has_value()) {
      co_return absl::Status(
          absl::StatusCode::kInvalidArgument,
          "replicated large value frame sequence interrupted");
    }

    const SnapshotRecord& applied = *effective;
    const Digest digest = ComputeDigest(applied.key_);
    auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
        applied.db_id_, tx::FingerprintOf(digest), tx::LockMode::kExclusive);
    co_await store.store_state_mutex_.Lock();
    UnlockGuard write_unlock(&store.store_state_mutex_, store.worker_);
    sync = partition.replica_sync_.get();
    if (sync == nullptr || sync->session_id_ != session_id ||
        replication_epoch != sync->replication_epoch_) [[unlikely]] {
      co_return absl::Status(absl::StatusCode::kFailedPrecondition,
                             "stale partition replication epoch");
    }
    auto& index = partition.indexes_[applied.db_id_];
    auto resolved =
        co_await FindVerifiedEntry(store, index, digest, applied.key_);
    if (!resolved.ok()) {
      co_return resolved.status();
    }
    auto* current = *resolved;
    if (current != nullptr &&
        current->value_.mutation_sequence_ >= applied.mutation_sequence_) {
      continue;
    }
    const RecordKind kind = applied.kind_ == SnapshotRecord::Kind::kValue
                                ? RecordKind::kValue
                                : RecordKind::kTombstone;
    const ValueType value_type =
        kind == RecordKind::kValue ? applied.value_type_ : ValueType::kNone;
    if (kind == RecordKind::kValue && value_type == ValueType::kNone) {
      co_return absl::Status(absl::StatusCode::kInvalidArgument,
                             "replicated value has no Redis type");
    }
    const bool nonempty_collection =
        value_type == ValueType::kList || value_type == ValueType::kHash ||
        value_type == ValueType::kSet || value_type == ValueType::kSortedSet;
    if (kind == RecordKind::kValue &&
        ((nonempty_collection && applied.logical_size_ == 0) ||
         (value_type == ValueType::kString &&
          applied.logical_size_ != applied.value_.size()))) {
      co_return absl::InvalidArgumentError(
          "replicated value has inconsistent logical size");
    }
    if (kind == RecordKind::kValue &&
        applied.logical_size_ > std::numeric_limits<std::uint32_t>::max()) {
      co_return absl::InvalidArgumentError(
          "replicated logical size exceeds record metadata");
    }
    absl::Status written;
    const ExplicitWriteRoot write_root{
        .index_ = &index,
        .live_key_count_ = &partition.live_key_count_[applied.db_id_],
        .store_live_key_count_ = &store.live_key_count_[applied.db_id_],
        .expiring_key_count_ = &partition.expiring_key_count_[applied.db_id_],
        .replication_epoch_ = sync->replication_epoch_,
        .db_epoch_ = sync->local_db_epochs_[applied.db_id_],
    };
    const bool key_external =
        applied.key_.size() > options_.inline_key_max_bytes_;
    const std::uint64_t logical_payload_bytes =
        static_cast<std::uint64_t>(applied.value_.size()) +
        (key_external ? applied.key_.size() : 0);
    const std::size_t inline_bytes =
        AlignRecord(RecordHeaderBytes(applied.key_.size(), key_external, false,
                                      applied.expire_at_ms_ != 0) +
                    static_cast<std::size_t>(logical_payload_bytes));
    if (inline_bytes > kStorageBlockBytes - kBlockHeaderBytes) [[unlikely]] {
      auto extents = co_await WriteExtentValueLocked(
          store,
          key_external ? std::string_view(applied.key_) : std::string_view{},
          applied.value_);
      if (!extents.ok()) {
        co_return extents.status();
      }
      const std::string manifest = EncodeManifest(**extents);
      written = co_await WriteRecordLocked(
          store, applied.db_id_, applied.key_, manifest, kind, value_type,
          applied.expire_at_ms_, digest, /*txid=*/0, applied.mutation_sequence_,
          false, true, true, key_external, applied.logical_size_, *extents,
          nullptr, nullptr, nullptr, nullptr, nullptr, &write_root);
      if (!written.ok()) {
        SpawnExtentReclaim(store, *extents);
      }
    } else {
      written = co_await WriteRecordLocked(
          store, applied.db_id_, applied.key_, applied.value_, kind, value_type,
          kind == RecordKind::kValue ? applied.expire_at_ms_ : 0, digest,
          /*txid=*/0, applied.mutation_sequence_, false, true, false,
          key_external, applied.logical_size_, nullptr, nullptr, nullptr,
          nullptr, nullptr, nullptr, &write_root);
    }
    if (!written.ok()) {
      co_return written;
    }
    partition.mutation_sequence_ =
        std::max(partition.mutation_sequence_, applied.mutation_sequence_);
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::DrainReplicaRootWritesLocal(
    WorkerStore& store) {
  co_await store.replica_apply_mutex_.Lock();
  UnlockGuard replica_unlock(&store.replica_apply_mutex_, store.worker_);
  {
    co_await store.store_state_mutex_.Lock();
    UnlockGuard write_unlock(&store.store_state_mutex_, store.worker_);
    FlushActiveBlock(store);
  }
  while (true) {
    bool done = false;
    bool failed = false;
    {
      co_await store.store_state_mutex_.Lock();
      UnlockGuard write_unlock(&store.store_state_mutex_, store.worker_);
      done = !store.flush_running_ && store.flush_queue_.empty();
      failed = store.write_failed_ || RuntimeFailureLatched();
    }
    if (failed) {
      co_return absl::InternalError(
          "storage write failed while draining replica root");
    }
    if (done) co_return absl::OkStatus();
    absl::Status waited =
        co_await bycorf::SleepFor(*store.worker_, std::chrono::milliseconds(1));
    if (!waited.ok()) co_return waited;
  }
}

Task<absl::Status> StorageEngine::Impl::PromoteReplicaRoot(
    std::uint64_t session_id) {
  if (session_id == 0) {
    co_return absl::InvalidArgumentError("invalid replica root session");
  }
  if (bycorf::ThisWorker().id_ != 0) {
    co_return co_await bycorf::SubmitTaskTo(
        0, [this, session_id]() { return PromoteReplicaRoot(session_id); });
  }

  const auto& first_partition = PartitionFor(*stores_[0], 0);
  if (first_partition.replica_sync_ == nullptr ||
      first_partition.replica_sync_->session_id_ != session_id) {
    co_return absl::FailedPreconditionError("replica sync state is empty");
  }
  const std::array<std::uint64_t, kLogicalDatabaseCount> local_db_epochs =
      first_partition.replica_sync_->local_db_epochs_;
  const std::array<std::uint64_t, kLogicalDatabaseCount> source_db_epochs =
      first_partition.replica_sync_->source_db_epochs_;
  for (unsigned target = 0; target < worker_count_; ++target) {
    auto validate = [this, target, session_id, local_db_epochs,
                     source_db_epochs]() -> absl::Status {
      WorkerStore& store = *stores_[target];
      for (const auto& partition : store.partitions_) {
        if (partition.replica_sync_ == nullptr ||
            partition.replica_sync_->session_id_ != session_id ||
            !partition.replica_sync_->tailing_ ||
            partition.replica_sync_->stream_failed_ ||
            partition.replica_value_stage_.has_value()) {
          return absl::FailedPreconditionError(
              "replica synchronization is incomplete");
        }
        if (local_db_epochs != partition.replica_sync_->local_db_epochs_ ||
            source_db_epochs != partition.replica_sync_->source_db_epochs_) {
          return absl::FailedPreconditionError(
              "replica synchronization database epochs disagree");
        }
      }
      return absl::OkStatus();
    };
    absl::Status valid =
        target == 0 ? validate() : co_await bycorf::SubmitTo(target, validate);
    if (!valid.ok()) co_return valid;
  }
  for (unsigned target = 0; target < worker_count_; ++target) {
    auto drain = [this, target]() {
      return DrainReplicaRootWritesLocal(*stores_[target]);
    };
    // Keep owner selection outside a conditional expression with two
    // co_await operands. GCC 13 can reuse the prior iteration's awaiter when
    // lowering that shape in this coroutine, causing worker zero to be drained
    // or published twice while another worker is skipped.
    absl::Status drained;
    if (target == 0) {
      drained = co_await drain();
    } else {
      drained = co_await bycorf::SubmitTaskTo(target, drain);
    }
    if (!drained.ok()) co_return drained;
  }

  for (unsigned target = 0; target < worker_count_; ++target) {
    auto settle = [this, target]() {
      return AwaitDetachedReclaim(*stores_[target]);
    };
    absl::Status settled;
    if (target == 0) {
      settled = co_await settle();
    } else {
      settled = co_await bycorf::SubmitTaskTo(target, settle);
    }
    if (!settled.ok()) co_return settled;
  }

  std::vector<std::pair<std::size_t, std::uint64_t>> epoch_updates;
  epoch_updates.reserve(kLogicalDatabaseCount);
  for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
    epoch_updates.emplace_back(db_id, local_db_epochs[db_id]);
  }
  absl::Status persisted = co_await PersistEpochValues(epoch_updates);
  if (!persisted.ok()) co_return persisted;
  for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
    db_epochs_[db_id].store(local_db_epochs[db_id], std::memory_order_release);
    replica_source_db_epochs_[db_id].store(source_db_epochs[db_id],
                                           std::memory_order_release);
  }

  for (unsigned target = 0; target < worker_count_; ++target) {
    auto publish = [this, target, session_id]() -> Task<absl::Status> {
      WorkerStore& store = *stores_[target];
      co_await store.replica_apply_mutex_.Lock();
      UnlockGuard replica_unlock(&store.replica_apply_mutex_, store.worker_);
      co_await store.store_state_mutex_.Lock();
      UnlockGuard write_unlock(&store.store_state_mutex_, store.worker_);
      for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
        // Promotion changes visibility, not this candidate's index identity.
        // Reset already advanced the generation when it detached the old
        // population. Advancing again would invalidate every grouped view
        // built in the candidate despite retaining its exact physical root.
        tx::CurrentTxShard().MarkAllWatched(db_id);
      }
      for (auto& partition : store.partitions_) {
        auto* sync = partition.replica_sync_.get();
        if (sync == nullptr || sync->session_id_ != session_id ||
            !sync->tailing_) {
          co_return absl::FailedPreconditionError(
              "replica synchronization changed during promotion");
        }
        partition.replica_value_stage_.reset();
        partition.replica_sync_.reset();
      }
      co_return absl::OkStatus();
    };
    absl::Status published;
    if (target == 0) {
      published = co_await publish();
    } else {
      published = co_await bycorf::SubmitTaskTo(target, publish);
    }
    if (!published.ok()) co_return published;
  }
  std::array<std::byte, 2 * kLogicalDatabaseCount * sizeof(std::uint64_t)>
      population_bytes{};
  std::memcpy(population_bytes.data(), local_db_epochs.data(),
              kLogicalDatabaseCount * sizeof(std::uint64_t));
  std::memcpy(
      population_bytes.data() + kLogicalDatabaseCount * sizeof(std::uint64_t),
      source_db_epochs.data(), kLogicalDatabaseCount * sizeof(std::uint64_t));
  if (ReplicaRecoveryFenced()) {
    absl::Status completed = co_await CompleteReplicaFullSync(
        session_id, PopulationToken{
                        .generation_ = session_id,
                        .digest_ = Crc64(population_bytes),
                    });
    if (!completed.ok()) co_return completed;
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::AbortReplicaRoot(
    std::uint64_t session_id) {
  if (session_id == 0) co_return absl::OkStatus();
  if (bycorf::ThisWorker().id_ != 0) {
    co_return co_await bycorf::SubmitTaskTo(
        0, [this, session_id]() { return AbortReplicaRoot(session_id); });
  }
  // A stream can own an uncommitted grouped root and a cross-frame key hold.
  // Settle it before draining: a post-root writer failure makes drain fail,
  // but must not strand its undo journal, dependency pins or generation lease.
  for (unsigned target = 0; target < worker_count_; ++target) {
    auto cancel = [this, target, session_id]() -> Task<absl::Status> {
      auto& store = *stores_[target];
      co_await store.replica_apply_mutex_.Lock();
      UnlockGuard replica_unlock(&store.replica_apply_mutex_, store.worker_);
      for (auto& partition : store.partitions_) {
        if (!partition.replica_sync_ ||
            partition.replica_sync_->session_id_ != session_id)
          continue;
        const auto aborted = co_await AbortReplicaValueStage(store, partition);
        if (!aborted.ok()) co_return aborted;
      }
      co_return absl::OkStatus();
    };
    // Same GCC 13 double-co_await workaround as PromoteReplicaRoot above.
    absl::Status cancelled;
    if (target == 0) {
      cancelled = co_await cancel();
    } else {
      cancelled = co_await bycorf::SubmitTaskTo(target, cancel);
    }
    if (!cancelled.ok()) co_return cancelled;
  }
  for (unsigned target = 0; target < worker_count_; ++target) {
    auto drain = [this, target]() {
      return DrainReplicaRootWritesLocal(*stores_[target]);
    };
    // Same GCC 13 double-co_await workaround as PromoteReplicaRoot above.
    absl::Status drained;
    if (target == 0) {
      drained = co_await drain();
    } else {
      drained = co_await bycorf::SubmitTaskTo(target, drain);
    }
    if (!drained.ok()) co_return drained;
  }
  for (unsigned target = 0; target < worker_count_; ++target) {
    auto discard = [this, target, session_id]() -> Task<absl::Status> {
      WorkerStore& store = *stores_[target];
      {
        co_await store.replica_apply_mutex_.Lock();
        UnlockGuard replica_unlock(&store.replica_apply_mutex_, store.worker_);
        co_await store.store_state_mutex_.Lock();
        UnlockGuard write_unlock(&store.store_state_mutex_, store.worker_);
        bool discarded_any = false;
        for (auto& partition : store.partitions_) {
          auto* sync = partition.replica_sync_.get();
          if (sync == nullptr || sync->session_id_ != session_id) continue;
          discarded_any = true;
          for (std::uint8_t db_id = 0; db_id < options_.database_count_;
               ++db_id) {
            ++partition.grouped_generations_[db_id];
            QueueDetachedIndex(store, partition.indexes_[db_id], db_id,
                               &partition.grouped_objects_[db_id]);
            partition.fullsync_coverage_bytes_[db_id] = 0;
            if (store.live_key_count_[db_id] < partition.live_key_count_[db_id])
                [[unlikely]] {
              co_return absl::InternalError(
                  "replica abort found inconsistent live-key accounting");
            }
            store.live_key_count_[db_id] -= partition.live_key_count_[db_id];
            partition.live_key_count_[db_id] = 0;
            partition.expiring_key_count_[db_id] = 0;
          }
          partition.mutation_sequence_ = 0;
          partition.replica_value_stage_.reset();
          partition.replica_sync_.reset();
        }
        if (discarded_any) {
          for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
            ++store.index_generations_[db_id];
            tx::CurrentTxShard().MarkAllWatched(db_id);
          }
        }
        EnsureDetachedReclaim(store);
      }
      // Abort is the resource handoff between attempts. Reuse the existing
      // worker-local detached-index drain so rapid retries cannot accumulate
      // old index arenas or unsettled record-block accounting.
      co_return co_await AwaitDetachedReclaim(store);
    };
    absl::Status discarded;
    if (target == 0) {
      discarded = co_await discard();
    } else {
      discarded = co_await bycorf::SubmitTaskTo(target, discard);
    }
    if (!discarded.ok()) co_return discarded;
  }
  co_return absl::OkStatus();
}

}  // namespace lavik::storage
