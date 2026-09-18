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

#include <time.h>

#include <exception>

#include "absl/strings/cord.h"
#include "impl.h"

namespace lavik::storage {

std::uint32_t StorageEngine::Impl::ActiveExpirationConfigValue(
    ActiveExpirationConfigKey key) const noexcept {
  switch (key) {
    case ActiveExpirationConfigKey::kIntervalMs:
      return active_expiration_interval_ms_.load(std::memory_order_relaxed);
    case ActiveExpirationConfigKey::kMapStepsPerCycle:
      return active_expiration_map_steps_per_cycle_.load(
          std::memory_order_relaxed);
    case ActiveExpirationConfigKey::kDeletesPerCycle:
      return active_expiration_deletes_per_cycle_.load(
          std::memory_order_relaxed);
    case ActiveExpirationConfigKey::kIndexMaintenanceStepsPerCycle:
      return active_expiration_index_maintenance_steps_per_cycle_.load(
          std::memory_order_relaxed);
  }
  std::unreachable();
}

absl::Status StorageEngine::Impl::ConfigureActiveExpiration(
    ActiveExpirationConfigKey key, std::uint64_t value) {
  // Zero would busy-poll the timer or indefinitely starve one maintenance
  // phase. Pacing is independent of the existing authority/pause boundaries.
  if (value == 0 || value > std::numeric_limits<std::uint32_t>::max()) {
    return absl::InvalidArgumentError(
        "active expiration value must be between 1 and 4294967295");
  }
  const auto bounded = static_cast<std::uint32_t>(value);
  switch (key) {
    case ActiveExpirationConfigKey::kIntervalMs:
      active_expiration_interval_ms_.store(bounded, std::memory_order_relaxed);
      break;
    case ActiveExpirationConfigKey::kMapStepsPerCycle:
      active_expiration_map_steps_per_cycle_.store(bounded,
                                                   std::memory_order_relaxed);
      break;
    case ActiveExpirationConfigKey::kDeletesPerCycle:
      active_expiration_deletes_per_cycle_.store(bounded,
                                                 std::memory_order_relaxed);
      break;
    case ActiveExpirationConfigKey::kIndexMaintenanceStepsPerCycle:
      active_expiration_index_maintenance_steps_per_cycle_.store(
          bounded, std::memory_order_relaxed);
      break;
    default:
      return absl::InvalidArgumentError("unknown active expiration setting");
  }
  return absl::OkStatus();
}

namespace {

constexpr absl::string_view kExpirationAuthorityCancellationTypeUrl =
    "type.googleapis.com/lavik.storage.ExpirationAuthorityCancellation";
constexpr std::size_t kMaxQueuedExpiredCandidates = 4096;

std::chrono::nanoseconds BootTimeSinceEpoch() noexcept {
  timespec now{};
  if (::clock_gettime(CLOCK_BOOTTIME, &now) != 0) {
    // Mixing clock epochs could extend a finite authority after suspend. The
    // supported runtime is Linux, where CLOCK_BOOTTIME is always available.
    std::terminate();
  }
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::seconds(now.tv_sec) + std::chrono::nanoseconds(now.tv_nsec));
}

}  // namespace

bool StorageEngine::Impl::ExpirationAuthorityIsValid(
    const ExpirationAuthorityGrant* authority) noexcept {
  if (authority == nullptr ||
      !authority->active_.load(std::memory_order_acquire)) {
    return false;
  }
  return authority->deadline_since_boot_ == std::chrono::nanoseconds::max() ||
         BootTimeSinceEpoch() < authority->deadline_since_boot_;
}

std::shared_ptr<StorageEngine::Impl::ExpirationAuthorityGrant>
StorageEngine::Impl::CurrentExpirationAuthority() const noexcept {
  auto authority = active_expiration_authority_.load(std::memory_order_acquire);
  return ExpirationAuthorityIsValid(authority.get()) ? std::move(authority)
                                                     : nullptr;
}

absl::Status StorageEngine::Impl::ValidateExpirationAuthority(
    const void* context) {
  const auto* authority = static_cast<const ExpirationAuthorityGrant*>(context);
  if (ExpirationAuthorityIsValid(authority)) return absl::OkStatus();
  absl::Status cancelled = absl::FailedPreconditionError(
      "expiration authority was revoked or expired");
  // The payload survives the storage append unchanged and distinguishes this
  // expected cancellation from an unrelated failure that happened while the
  // same token was concurrently revoked or expired.
  cancelled.SetPayload(kExpirationAuthorityCancellationTypeUrl,
                       absl::Cord("cancelled"));
  return cancelled;
}

bool StorageEngine::Impl::IsExpirationAuthorityCancellation(
    const absl::Status& status) noexcept {
  return status.code() == absl::StatusCode::kFailedPrecondition &&
         status.GetPayload(kExpirationAuthorityCancellationTypeUrl).has_value();
}

void StorageEngine::Impl::SetExpirationAuthority(bool authority) noexcept {
  auto current = active_expiration_authority_.load(std::memory_order_acquire);
  const auto valid_permanent = [](const ExpirationAuthorityGrant* grant) {
    return grant != nullptr && grant->active_.load(std::memory_order_acquire) &&
           grant->deadline_since_boot_ == std::chrono::nanoseconds::max();
  };
  if (authority && valid_permanent(current.get())) {
    // Repeating the legacy enable must not invalidate work admitted under the
    // same permanent authority. This is both the common call path and the
    // allocation-free behavior callers had before exact grants existed.
    expiration_authority_.store(true, std::memory_order_release);
    return;
  }

  std::shared_ptr<ExpirationAuthorityGrant> replacement;
  if (authority) {
    try {
      replacement = std::make_shared<ExpirationAuthorityGrant>(
          std::chrono::nanoseconds::max());
    } catch (const std::bad_alloc&) {
      // The legacy API cannot report allocation failure. Installing no grant
      // is the only fail-closed outcome, and a later call may retry.
    }
  }

  if (!authority) {
    expiration_authority_.store(false, std::memory_order_release);
  }
  for (;;) {
    if (authority && valid_permanent(current.get())) {
      // A concurrent enabler may have installed the permanent grant after the
      // first load. Preserve its identity instead of replacing it again.
      expiration_authority_.store(true, std::memory_order_release);
      return;
    }
    if (current != nullptr) {
      // Invalidate before publishing its replacement. Work already carrying
      // the old capability will then fail its final mutation precondition.
      current->active_.store(false, std::memory_order_release);
    }
    if (active_expiration_authority_.compare_exchange_weak(
            current, replacement, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      if (authority) {
        expiration_authority_.store(replacement != nullptr,
                                    std::memory_order_release);
      }
      return;
    }
  }
}

absl::Status StorageEngine::Impl::SetExpirationAuthorityUntil(
    std::chrono::nanoseconds deadline_since_boot) noexcept {
  if (BootTimeSinceEpoch() >= deadline_since_boot) {
    return absl::DeadlineExceededError(
        "expiration authority deadline has already elapsed");
  }
  std::shared_ptr<ExpirationAuthorityGrant> replacement;
  try {
    replacement =
        std::make_shared<ExpirationAuthorityGrant>(deadline_since_boot);
  } catch (const std::bad_alloc&) {
    return absl::ResourceExhaustedError(
        "failed to allocate expiration authority grant");
  }
  if (BootTimeSinceEpoch() >= deadline_since_boot) {
    return absl::DeadlineExceededError(
        "expiration authority deadline elapsed during installation");
  }

  auto current = active_expiration_authority_.load(std::memory_order_acquire);
  for (;;) {
    if (current != nullptr) {
      current->active_.store(false, std::memory_order_release);
    }
    if (active_expiration_authority_.compare_exchange_weak(
            current, replacement, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      return absl::OkStatus();
    }
  }
}

Task<absl::Status> StorageEngine::Impl::QuiesceExpiration() {
  expiration_pause_count_.fetch_add(1, std::memory_order_acq_rel);
  // Drain the in-flight expiration cycle on every worker. The flag spans a
  // whole cycle, so once it drops every tombstone of that cycle has landed —
  // no matter where the cycle suspended along the way. A cycle raises the
  // flag before it checks the pause count, so it either sees the increment
  // above and abstains, or is seen here and waited out.
  for (unsigned target = 0; target < worker_count_; ++target) {
    absl::Status drained = co_await bycorf::SubmitTaskTo(
        target, [this, target]() -> Task<absl::Status> {
          WorkerStore& store = *stores_[target];
          while (store.expiry_cycle_running_) {
            absl::Status waited = co_await bycorf::SleepFor(
                *store.worker_, std::chrono::milliseconds(1));
            if (!waited.ok()) {
              co_return waited;
            }
          }
          co_return absl::OkStatus();
        });
    if (!drained.ok()) {
      ResumeExpiration();
      co_return drained;
    }
  }
  co_return absl::OkStatus();
}

void StorageEngine::Impl::QueueExpiredCandidate(WorkerStore& store,
                                                std::uint16_t partition_id,
                                                std::uint8_t db_id,
                                                const RecordIndex::Entry& entry,
                                                std::string_view known_key) {
  auto expiration_authority = CurrentExpirationAuthority();
  if (expiration_authority == nullptr ||
      store.expired_candidates_.size() >= kMaxQueuedExpiredCandidates ||
      entry.value_.kind() != RecordKind::kValue || ExpireAt(entry) == 0) {
    return;
  }
  const std::string_view key = entry.key_complete() ? entry.key() : known_key;
  if (key.empty() && entry.logical_key_size() != 0) {
    return;
  }
  store.expired_candidates_.push_back(WorkerStore::ExpireCandidate{
      .partition_id_ = partition_id,
      .db_id_ = db_id,
      .digest_ = entry.key_complete() ? ComputeDigest(key)
                                      : entry.external_key_digest(),
      .mutation_sequence_ = entry.value_.mutation_sequence_,
      .expire_at_ms_ = ExpireAt(entry),
      .key_ = std::string(key),
      .expiration_authority_ = std::move(expiration_authority),
  });
}

void StorageEngine::Impl::AdvanceExpiryMap(WorkerStore& store) {
  store.expiry_scan_cursor_ = 0;
  ++store.expiry_db_cursor_;
  if (store.expiry_db_cursor_ == options_.database_count_) {
    store.expiry_db_cursor_ = 0;
    ++store.expiry_partition_cursor_;
    if (store.expiry_partition_cursor_ == store.partitions_.size()) {
      store.expiry_partition_cursor_ = 0;
    }
  }
}

std::size_t StorageEngine::Impl::DiscardStaleExpirationCandidates(
    WorkerStore& store, std::size_t max_candidates) noexcept {
  std::size_t discarded = 0;
  while (discarded < max_candidates && !store.expired_candidates_.empty() &&
         !ExpirationAuthorityIsValid(
             store.expired_candidates_.front().expiration_authority_.get())) {
    store.expired_candidates_.pop_front();
    ++discarded;
  }
  return discarded;
}

Task<absl::Status> StorageEngine::Impl::ExpireCandidate(
    WorkerStore& store, WorkerStore::ExpireCandidate candidate) {
  if (candidate.partition_id_ >= kLogicalStorageShards ||
      candidate.db_id_ >= options_.database_count_ ||
      expiration_pause_count_.load(std::memory_order_acquire) != 0) {
    co_return absl::OkStatus();
  }
  const MutationPrecondition expiration_precondition(
      candidate.expiration_authority_, &ValidateExpirationAuthority);
  if (!expiration_precondition.Validate().ok()) {
    // Revocation and deadline expiry are ordinary maintenance cancellation.
    // The still-indexed key may be rediscovered under a later exact grant.
    co_return absl::OkStatus();
  }
  auto& partition = PartitionFor(store, candidate.partition_id_);
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      candidate.db_id_, tx::FingerprintOf(candidate.digest_),
      tx::LockMode::kExclusive);
  co_await store.store_state_mutex_.Lock();
  UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
  auto resolved =
      co_await FindVerifiedEntry(store, partition.indexes_[candidate.db_id_],
                                 candidate.digest_, candidate.key_);
  if (!resolved.ok()) {
    co_return resolved.status();
  }
  auto* current = *resolved;
  const auto matches_candidate = [&candidate](const RecordIndex::Entry* entry) {
    return entry != nullptr && entry->value_.kind() == RecordKind::kValue &&
           entry->value_.mutation_sequence_ == candidate.mutation_sequence_ &&
           ExpireAt(*entry) == candidate.expire_at_ms_ && IsExpiredNow(*entry);
  };
  if (!matches_candidate(current)) {
    co_return absl::OkStatus();
  }
  // Prefer a durable delete so a later wall-clock rollback cannot expose the
  // expired value again. If the device has no foreground space left, an
  // unshielded record is nevertheless safe to retire in memory: there is no
  // older live version for this record to hide, and recovery still observes
  // its own expiration timestamp. This is the full-disk escape valve that
  // lets expiration free blocks which can then accept durable tombstones.
  absl::Status durable;
#if LAVIK_FAULTS_ENABLED
  std::optional<absl::Status> injected_durable;
  if (expiration_test_hook_) {
    injected_durable =
        expiration_test_hook_(ExpirationTestPoint::kBeforeDurableAppend);
  }
  if (injected_durable.has_value()) {
    durable = std::move(*injected_durable);
  } else {
#endif
    durable = co_await AppendLocked(
        store, partition, candidate.db_id_, candidate.key_, candidate.digest_,
        {}, RecordKind::kTombstone, ValueType::kNone, 0, nullptr, 0, nullptr,
        nullptr, nullptr, nullptr, true, nullptr, &expiration_precondition);
#if LAVIK_FAULTS_ENABLED
  }
#endif
  if (IsExpirationAuthorityCancellation(durable)) co_return absl::OkStatus();
  if (durable.ok() || durable.code() != absl::StatusCode::kResourceExhausted)
    co_return durable;

  // Append may release store_state_mutex_ while acquiring space or pinning
  // transaction dependencies. The key hold excludes logical writes, but GC
  // may replace the index entry and the group's physical coordinates.
  resolved =
      co_await FindVerifiedEntry(store, partition.indexes_[candidate.db_id_],
                                 candidate.digest_, candidate.key_);
  if (!resolved.ok()) co_return resolved.status();
  current = *resolved;
  if (!matches_candidate(current)) co_return absl::OkStatus();
  if (current->value_.shielding()) co_return durable;

  const RecordLocation dropped = MaterializeIndexLocation(*current);
  const ExtentManifest dropped_extents = ExtentsFor(store, current);
  const ExtentManifest dropped_dependent_extents =
      DependentExtentsFor(store, current);
  GroupedHashObject::Handle grouped;
  std::vector<RetiredRecord> grouped_retirements;
  if (dropped.grouped()) {
    try {
      auto view = partition.grouped_objects_[candidate.db_id_].Lookup(
          candidate.key_,
          GroupedObjectVersion{
              .root_ = dropped,
              .db_epoch_ = EffectiveRecordDbEpoch(partition, candidate.db_id_),
              .replication_epoch_ = partition.replication_epoch_,
              .index_generation_ =
                  partition.grouped_generations_[candidate.db_id_]});
      if (!view.ok()) co_return view.status();
      if (*view == nullptr)
        co_return absl::DataLossError("missing expired grouped view");
      grouped = std::move(*view);
      // Prepare the complete graph before detaching either index, without
      // allocating disk space or decoding values. This includes split-parent
      // retirement records and preserves external parent-key extent debt.
      // Admission failure leaves the expired key indexed for a later retry.
      auto retired = CollectGroupedRetirements(grouped, nullptr);
      if (!retired.ok()) co_return retired.status();
      grouped_retirements = std::move(*retired);
    } catch (const std::bad_alloc&) {
      co_return absl::ResourceExhaustedError(
          "OOM preparing expired grouped retirements");
    }
  }

  // Append can fail before reaching its publication precondition. The
  // disk-full fallback has its own no-await mutation cut and must validate the
  // same capability immediately before its first side effect.
#if LAVIK_FAULTS_ENABLED
  if (expiration_test_hook_) {
    (void)expiration_test_hook_(ExpirationTestPoint::kBeforeDiskFullFallback);
  }
#endif
  if (!expiration_precondition.Validate().ok()) {
    co_return absl::OkStatus();
  }
  tx::CurrentTxShard().MarkWatched(candidate.db_id_,
                                   tx::FingerprintOf(candidate.digest_));
  const std::uint64_t sequence = ++partition.mutation_sequence_;
  if (!partition.fullsync_subscribers_.empty()) [[unlikely]] {
    FullSyncOnCommit(store, partition,
                     SnapshotRecord{
                         .kind_ = SnapshotRecord::Kind::kDelete,
                         .db_id_ = candidate.db_id_,
                         .db_epoch_ = DbEpoch(candidate.db_id_),
                         .mutation_sequence_ = sequence,
                         .expire_at_ms_ = 0,
                         .value_type_ = ValueType::kNone,
                         .key_ = candidate.key_,
                         .value_ = {},
                     },
                     candidate.digest_);
  }
  // No suspension separates physical capture, side-view removal and root
  // removal. Once detached, GC cannot relocate these records; their live-byte
  // charges keep their blocks allocated until settlement on each owner.
  if (grouped != nullptr) {
    const auto removed = partition.grouped_objects_[candidate.db_id_].Erase(
        candidate.key_, grouped);
    if (!removed.ok()) {
      store.write_failed_ = true;
      co_return removed;
    }
  }
  --partition.live_key_count_[candidate.db_id_];
  --store.live_key_count_[candidate.db_id_];
  --partition.expiring_key_count_[candidate.db_id_];
  store.external_manifests_.erase(current);
  const std::size_t logical_key_bytes = current->logical_key_size();
  const bool erased = partition.indexes_[candidate.db_id_].Erase(current);
  assert(erased);
  if (erased) {
    RemoveFullSyncCoverageEntry(partition, candidate.db_id_, logical_key_bytes);
  }
  if (dropped.external() && !dropped.key_external()) {
    SpawnExtentReclaim(store, dropped_extents);
  }
  absl::Status dead = co_await MarkRecordDead(
      RetiredRecordOf(dropped, dropped_dependent_extents));
  for (const auto& record : grouped_retirements) {
    if (!dead.ok()) break;
    dead = co_await MarkRecordDead(record);
  }
  if (!dead.ok()) LatchRuntimeFailure(store);
  co_return dead;
}

Task<absl::Status> StorageEngine::Impl::ActiveExpiration(WorkerStore* store) {
  while (!store->worker_->stop_requested()) {
    const auto interval = std::chrono::milliseconds(
        ActiveExpirationConfigValue(ActiveExpirationConfigKey::kIntervalMs));
    absl::Status waited = co_await bycorf::SleepFor(*store->worker_, interval);
    if (!waited.ok()) {
      co_return waited;
    }
    // Raise the flag before checking the pause count — with no suspension
    // between the two, a cycle QuiesceExpiration's increment misses is
    // already visible to its drain. The guard drops the flag on every exit
    // from the cycle: abstain, shutdown, delete error, or completion.
    store->expiry_cycle_running_ = true;
    struct CycleGuard {
      bool* running_;
      ~CycleGuard() { *running_ = false; }
    } cycle_guard{&store->expiry_cycle_running_};
    if (expiration_pause_count_.load(std::memory_order_acquire) != 0) {
      continue;  // a stable-keyspace scan (KEYS) is in flight
    }
    if (store->worker_->stop_requested() ||
        shutdown_flush_requested_.load(std::memory_order_acquire)) {
      break;
    }

    // Sample once after waking: CONFIG can run while this cycle yields, but
    // cannot extend an in-flight batch by repeatedly raising its budget.
    const std::size_t map_steps = ActiveExpirationConfigValue(
        ActiveExpirationConfigKey::kMapStepsPerCycle);
    const std::size_t deletes = ActiveExpirationConfigValue(
        ActiveExpirationConfigKey::kDeletesPerCycle);
    const std::size_t maintenance_steps = ActiveExpirationConfigValue(
        ActiveExpirationConfigKey::kIndexMaintenanceStepsPerCycle);

    // Resizing changes no logical state and also runs on replicas. Share the
    // expiration pause/drain boundary so stable scans and shutdown checkpoints
    // cannot race bucket migration. Keep this batch non-suspending on the
    // owner; a stalled admission advances to the next map and retries on a
    // later pass, while active rehashes retain their position across cycles.
    for (std::size_t step = 0;
         step < maintenance_steps && !store->partitions_.empty(); ++step) {
      auto& partition =
          store->partitions_[store->index_maintenance_partition_cursor_];
      if (partition.indexes_[store->index_maintenance_db_cursor_].Maintain()) {
        continue;
      }
      if (++store->index_maintenance_db_cursor_ == options_.database_count_) {
        store->index_maintenance_db_cursor_ = 0;
        if (++store->index_maintenance_partition_cursor_ ==
            store->partitions_.size()) {
          store->index_maintenance_partition_cursor_ = 0;
        }
      }
    }
#if LAVIK_FAULTS_ENABLED
    // Only logical expiration is disabled; index maintenance still runs.
    if (std::getenv("LAVIK_DISABLE_ACTIVE_EXPIRATION") != nullptr) continue;
#endif
    // Revoked grants may leave a full queue that would otherwise hide every
    // candidate discovered under the replacement grant. Retire the stale
    // prefix before advancing a scan cursor, under the same operator-visible
    // budget as actual and failed deletion attempts. If the budget cannot
    // reach the first live grant, leave both queue and scan position for the
    // next cycle.
    std::size_t processed = DiscardStaleExpirationCandidates(*store, deletes);
    if (!store->expired_candidates_.empty() &&
        !ExpirationAuthorityIsValid(
            store->expired_candidates_.front().expiration_authority_.get())) {
      continue;
    }
    if (CurrentExpirationAuthority() == nullptr) continue;

    const std::uint64_t now_ms = UnixTimeMillis();
    for (std::size_t step = 0;
         step < map_steps && !store->partitions_.empty() &&
         store->expired_candidates_.size() < kMaxQueuedExpiredCandidates;
         ++step) {
      auto& partition = store->partitions_[store->expiry_partition_cursor_];
      const std::uint8_t db_id = store->expiry_db_cursor_;
      if (partition.expiring_key_count_[db_id] == 0) {
        AdvanceExpiryMap(*store);
      } else {
        auto& index = partition.indexes_[db_id];
        struct ExternalExpired {
          std::uintptr_t entry_address_ = 0;
          ExtentManifest extents_;
          RecordLocation location_{};
          std::uint32_t hash_ = 0;
          std::uint32_t key_bytes_ = 0;
        };
        std::vector<ExternalExpired> external_expired;
        store->expiry_scan_cursor_ = index.Scan(
            store->expiry_scan_cursor_, [&](RecordIndex::Entry& entry) {
              if (IsExpired(entry, now_ms)) {
                if (entry.key_complete()) [[likely]] {
                  QueueExpiredCandidate(*store, partition.id_, db_id, entry);
                } else {
                  external_expired.push_back(ExternalExpired{
                      .entry_address_ =
                          reinterpret_cast<std::uintptr_t>(&entry),
                      .extents_ = ExtentsFor(*store, &entry),
                      .location_ = MaterializeIndexLocation(entry),
                      .hash_ =
                          RecordIndex::AddressHash(entry.external_key_digest()),
                      .key_bytes_ = entry.logical_key_size(),
                  });
                }
              }
            });
        for (const ExternalExpired& candidate : external_expired) {
          auto key = co_await LoadOutOfIndexKey(*store, candidate.location_,
                                                candidate.extents_,
                                                candidate.key_bytes_);
          if (!key.ok()) {
            co_return key.status();
          }
          RecordIndex::Entry* current =
              index.FindAddress(candidate.entry_address_, candidate.hash_);
          if (current == nullptr) continue;
          if (MaterializeIndexLocation(*current).SamePhysicalRecord(
                  candidate.location_) &&
              IsExpired(*current, now_ms)) {
            QueueExpiredCandidate(*store, partition.id_, db_id, *current, *key);
          }
        }
        if (store->expiry_scan_cursor_ == 0) {
          AdvanceExpiryMap(*store);
        }
        // A populated index can run callbacks and external-key reads. Keep the
        // original per-map checkpoint for dense TTL workloads; only empty maps
        // use the bounded batching fast path below.
        co_await bycorf::Yield(*store->worker_);
      }
    }
    // Empty maps are the common case across 16,384 partitions and 16 DBs.
    // Yielding once per map makes a complete pass depend on hundreds of
    // thousands of scheduler turns, so a key scanned just before its TTL can
    // remain uncollected for longer than a full-device reclaim can tolerate.
    // The per-cycle empty-map bound limits this batch while one yield
    // preserves fairness before candidate deletion begins.
    co_await bycorf::Yield(*store->worker_);

    bool warned_failure = false;
    while (processed < deletes && !store->expired_candidates_.empty()) {
      // A revoked, replaced, or elapsed grant can leave a queue prefix behind.
      // Drop that prefix before asking full-sync consumers for replacement
      // credit. Cancellation cleanup shares the configured per-cycle work
      // bound with actual and failed deletion attempts.
      processed +=
          DiscardStaleExpirationCandidates(*store, deletes - processed);
      if (processed == deletes || store->expired_candidates_.empty()) break;
      const WorkerStore::ExpireCandidate& front =
          store->expired_candidates_.front();
      std::size_t replacement_bytes = kFullSyncReplacementMetadataBytes;
      if (front.key_.size() >
          (std::numeric_limits<std::size_t>::max() - replacement_bytes) / 2) {
        co_return absl::ResourceExhaustedError(
            "active-expiration replacement identity is too large");
      }
      replacement_bytes += front.key_.size() * 2;
      auto admission = TryAcquireFullSyncReplacementAdmission(
          replacement_bytes,
          ReplicationPublisherTarget{.partition_id_ = front.partition_id_,
                                     .db_id_ = front.db_id_});
      if (!admission.has_value()) {
        // Expiration is maintenance and expired records are already logically
        // invisible. Leave the candidate queued and retry next cycle rather
        // than waiting behind a slow full-sync replica or foreground writer.
        break;
      }
      WorkerStore::ExpireCandidate candidate =
          std::move(store->expired_candidates_.front());
      store->expired_candidates_.pop_front();
      absl::Status expired =
          co_await ExpireCandidate(*store, std::move(candidate));
      ReleaseReplicationPublisherAdmission(*admission, replacement_bytes);
      if (!expired.ok()) {
        // Do not terminate this worker's lifetime expiration coroutine. The
        // key remains indexed as expired and a later map pass will enqueue it
        // again, while the warning keeps persistent write failures visible.
        if (!warned_failure) {
          spdlog::warn("worker[{}] active expiration failed: {}",
                       store->worker_->id(), expired.ToString());
          warned_failure = true;
        }
        ++processed;
        co_await bycorf::Yield(*store->worker_);
        continue;
      }
      ++processed;
      co_await bycorf::Yield(*store->worker_);
    }
  }
  co_return absl::OkStatus();
}

}  // namespace lavik::storage
