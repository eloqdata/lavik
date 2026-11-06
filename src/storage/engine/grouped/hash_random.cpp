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

#include <algorithm>
#include <limits>

#include "../impl.h"
#include "lavik/error_detail.h"
#include "lavik/random_sample.h"
#include "lavik/storage/detail/grouped/scratch.h"
#include "lavik/storage/detail/hash_read.h"

namespace lavik::storage {

Task<absl::StatusOr<HashResult>>
StorageEngine::Impl::ExecuteGroupedHashRandomLocked(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const HashOperation& operation, GroupedObject::Handle object,
    ValueType value_type, TxShardWrites* tx,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition, bool* retry_exclusive) {
  const bool pop = operation.kind_ == HashOperationKind::kPopRandom;
  if (!object || object->is_ordered() ||
      (pop && (value_type != ValueType::kSet || operation.count_ < 0 ||
               operation.with_values_)))
    co_return absl::InvalidArgumentError("invalid grouped random operation");
  const auto needs_key_lock = [&] {
    assert(retry_exclusive);
    *retry_exclusive = true;
    return absl::AbortedError("Hash sampling requires whole-key lock");
  };
  const auto observed = CaptureCompactWriteSnapshot(store, partition, db_id);
  HashResult result;
  result.key_exists_ = true;
  result.length_ = object->version().root_.logical_size_;
  if (operation.count_ == std::numeric_limits<std::int64_t>::min())
    co_return absl::OutOfRangeError("value is out of range");
  const bool repeat = operation.count_provided_ && operation.count_ < 0;
  auto requested = !operation.count_provided_
                       ? std::uint64_t{1}
                       : static_cast<std::uint64_t>(repeat ? -operation.count_
                                                           : operation.count_);
  if (!repeat) requested = std::min(requested, result.length_);
  if (requested == 0 || result.length_ == 0) co_return result;
  if (retry_exclusive && pop && requested == result.length_)
    co_return needs_key_lock();
  constexpr auto max_size = std::numeric_limits<std::size_t>::max();
  if (requested > (max_size - 4096) / 256)
    co_return absl::ResourceExhaustedError("random sample count overflow");
  // The rank sampler may temporarily retain the population when selecting
  // more than a third of it. Its memory is still O(requested), not O(values).
  auto metadata = TryReserveMemory(4096 + requested * 256);
  if (!metadata) {
    RecordMemoryRejection();
    co_return MemoryLimitError("OOM random sample metadata");
  }
  TraceSpanScope sampling(operation.diagnostics_, TraceStage::kValueProcess);
  std::vector<std::uint64_t> chosen;
  if (repeat) {
    chosen.reserve(requested);
    for (std::uint64_t i = 0; i < requested; ++i)
      chosen.push_back(RandomRank(result.length_, RandomSampleGenerator()));
  } else {
    chosen = SampleUniqueRandomRanks(result.length_, requested, false,
                                     RandomSampleGenerator());
  }
  // Sort only the I/O schedule. Output slots preserve the original draw
  // order, including independent negative-count draws with replacement.
  std::vector<std::pair<std::uint64_t, std::size_t>> samples;
  samples.reserve(chosen.size());
  for (std::size_t i = 0; i < chosen.size(); ++i)
    samples.emplace_back(chosen[i], i);
  std::sort(samples.begin(), samples.end());
  const std::size_t width = operation.with_values_ ? 2 : 1;
  result.values_.resize(requested * width);
  std::size_t output_bytes =
      result.values_.capacity() * sizeof(result.values_[0]);
  result.retained_charge_.Account(CurrentMemoryAccountingShard(), output_bytes);
  struct Selection {
    GroupedRecordId id_;
    std::uint64_t base_;
    std::size_t begin_, end_;
  };
  std::vector<Selection> selected;
  selected.reserve(samples.size());
  std::uint64_t base = 0;
  std::size_t cursor = 0;
  for (const auto& [prefix, route] : object->directory().groups()) {
    if (base > result.length_ || route.field_count_ > result.length_ - base)
      co_return absl::DataLossError("random sample directory count overflow");
    const auto end = base + route.field_count_;
    const auto first = cursor;
    while (cursor < samples.size() && samples[cursor].first < end) ++cursor;
    if (cursor != first) selected.push_back({route.id_, base, first, cursor});
    base = end;
  }
  if (base != result.length_ || cursor != samples.size())
    co_return absl::DataLossError("random sample directory count mismatch");

  std::unique_ptr<GroupedLockTable::LockSet> holds;
  if (retry_exclusive) {
    assert(tx == nullptr);
    if (!store.group_locks_)
      store.group_locks_ = std::make_unique<GroupedLockTable>(*store.worker_);
    holds.reset(new GroupedLockTable::LockSet(store.group_locks_->MakeLockSet(
        {db_id, tx::FingerprintOf(digest), object->incarnation()})));
    std::vector<GroupedRecordId> ids;
    ids.reserve(selected.size());
    for (const auto& selection : selected) ids.push_back(selection.id_);
    std::sort(ids.begin(), ids.end());
    for (const auto id : ids) {
      co_await holds->Acquire(
          {GroupedLockIndex::kMember, id},
          pop ? GroupedLockMode::kExclusive : GroupedLockMode::kShared);
    }
    const auto current =
        partition.grouped_objects_[db_id].CurrentForMutation(key);
    // Ranks depend on counts in unselected leaves too. A logical publication
    // during acquisition invalidates the draw; take one whole-key retry rather
    // than repeatedly resampling under contention. Physical GC is harmless.
    if (!current || !object->SameLogicalRoot(*current) ||
        current->version().db_epoch_ != object->version().db_epoch_ ||
        current->version().replication_epoch_ !=
            object->version().replication_epoch_ ||
        current->version().index_generation_ !=
            object->version().index_generation_ ||
        current->version().root_.expire_at_ms_ !=
            object->version().root_.expire_at_ms_)
      co_return needs_key_lock();
    object = current;
    LAVIK_FAULT_INJECT({
      if (LAVIK_FAULT_MATCHES("LAVIK_HASH_SAMPLE_PAUSE_KEY", key)) {
        for (const auto id : ids)
          spdlog::info("hash sample group held prefix={} bits={}", id.prefix_,
                       id.bits_);
        spdlog::info("hash sample groups held before page IO pop={}", pop);
        const auto paused = co_await fault_injection::PauseWhileFileExists(
            "LAVIK_HASH_SAMPLE_PAUSE_FILE");
        if (!paused.ok()) co_return paused;
      }
    });
  }

  auto add_page = [&](GroupedScratchBudget* budget, GroupedRecordId id) {
    const auto* record = object->FindGroup(id);
    return record == nullptr
               ? absl::DataLossError("random sample page is missing")
               : budget->AddGroup(*record, object->ExtentsFor(id));
  };
  std::optional<MemoryReservation> write_scratch;
  if (pop) {
    GroupedScratchBudget budget;
    for (const auto& selection : selected) {
      const auto added = add_page(&budget, selection.id_);
      if (!added.ok()) co_return added;
    }
    auto admission = budget.Reserve(4);
    if (!admission.ok()) co_return admission.status();
    write_scratch.emplace(std::move(*admission));
  }
  HashValue remaining;
  std::vector<GroupedRecordId> changed;
  changed.reserve(selected.size());
  sampling.End();
  for (std::size_t page_index = 0; page_index < selected.size(); ++page_index) {
    if (shutdown_flush_requested_)
      co_return absl::CancelledError("random sample interrupted by shutdown");
    if (!pop && page_index != 0) co_await bycorf::Yield(*store.worker_);
    const auto& selection = selected[page_index];
    if (!pop) {
      auto loaded = co_await LoadHashGroupPayload(
          store, partition, db_id, key, digest, object, selection.id_, false,
          retry_exclusive != nullptr, operation.diagnostics_);
      if (!loaded.ok()) co_return loaded.status();
      TraceSpanScope process(operation.diagnostics_, TraceStage::kValueProcess);
      const auto bytes = loaded->loaded_.value();
      const std::string_view payload(
          reinterpret_cast<const char*>(bytes.data()), bytes.size());
      std::vector<HashRankLookup> requests;
      requests.reserve(selection.end_ - selection.begin_);
      for (auto i = selection.begin_; i < selection.end_; ++i)
        requests.push_back({.rank_ = samples[i].first - selection.base_,
                            .result_index_ = samples[i].second});
      auto status = FindHashGroupRanks(payload, loaded->field_count_, requests);
      if (!status.ok()) co_return status;
      status = RetainHashRankValues(result, requests, operation.with_values_);
      if (!status.ok()) co_return status;
      // Borrowed entries die with this page. Only sampled output survives,
      // including separate copies for repeated draws in their original order.
      continue;
    }
    auto loaded = co_await LoadHashGroupSnapshot(
        store, partition, db_id, key, digest, object, selection.id_, false,
        nullptr, retry_exclusive != nullptr, operation.diagnostics_);
    if (!loaded.ok()) co_return loaded.status();
    TraceSpanScope process(operation.diagnostics_, TraceStage::kValueProcess);
    auto& entries = loaded->snapshot_.value_.entries_;
    for (auto i = selection.begin_; i < selection.end_; ++i) {
      const auto offset = samples[i].first - selection.base_;
      if (offset >= entries.size())
        co_return absl::DataLossError("random sample rank exceeds page");
      auto& entry = entries[offset];
      const auto slot = samples[i].second * width;
      result.values_[slot].emplace(std::move(entry.field_));
      for (std::size_t field = 0; field < width; ++field) {
        const auto capacity = result.values_[slot + field]->capacity();
        if (capacity >= max_size - output_bytes)
          co_return absl::ResourceExhaustedError(
              "random sample output overflow");
        output_bytes += capacity + 1;
      }
    }
    // Pops choose unique members, so their reply strings move out of the
    // strict decoded page. Retain their charge before releasing write scratch.
    result.retained_charge_.Resize(output_bytes);
    if (pop) {
      for (auto i = selection.end_; i != selection.begin_;) {
        --i;
        entries.erase(entries.begin() + (samples[i].first - selection.base_));
      }
      for (auto& entry : entries)
        remaining.entries_.push_back(std::move(entry));
      changed.push_back(selection.id_);
    }
  }
  if (!pop) co_return result;
  result.integer_ = requested;
  result.length_ -= requested;
  result.key_exists_ = result.length_ != 0;
  result.changed_ = true;
  std::optional<HashGroupMutationPlan> prepared;
  std::optional<UnlockGuard> unlock;
  if (retry_exclusive) {
    auto plan = PrepareGroupedHashMutation(object, std::move(remaining),
                                           changed, result.length_);
    if (!plan.ok()) co_return plan.status();
    prepared.emplace(std::move(*plan));
    co_await store.store_state_mutex_.Lock();
    unlock.emplace(&store.store_state_mutex_, store.worker_);
    auto validated = object;
    for (const auto id : changed) {
      auto current = RefreshLockedGroup(partition, db_id, key, object, id);
      if (!current.ok()) co_return current.status();
      validated = std::move(*current);
    }
    const auto valid =
        ValidateGroupedWriteSnapshot(store, partition, db_id, key, validated,
                                     observed, mutation_precondition);
    if (!valid.ok()) co_return valid;
  }
  if (replication != nullptr) {
    // Replica replay must remove the selected members, never draw again.
    replication->args_.clear();
    replication->args_.reserve(result.values_.size() + 2);
    replication->args_.emplace_back("SREM");
    replication->args_.emplace_back(key);
    for (const auto& member : result.values_)
      replication->args_.push_back(*member);
  }
  // if/else, not ?:, to keep the two co_awaits in separate full
  // expressions. GCC 13 can reuse the wrong coroutine-frame slot when both
  // arms of ?: contain co_await, which can publish the tombstone path for a
  // non-empty result.
  absl::Status status;
  if (result.key_exists_) {
    status = co_await CommitGroupedHashMutationLocked(
        store, partition, db_id, key, digest, object, std::move(remaining),
        std::move(changed), result.length_, value_type,
        object->version().root_.expire_at_ms_, tx, replication,
        mutation_precondition, prepared ? &*prepared : nullptr, retry_exclusive,
        operation.diagnostics_);
  } else {
    auto detail = MakeSetTrace(operation.diagnostics_);
    status = co_await AppendLocked(
        store, partition, db_id, key, digest, {}, RecordKind::kTombstone,
        ValueType::kNone, 0, tx, 0, nullptr, nullptr, replication, true,
        nullptr, mutation_precondition, nullptr, nullptr, detail.get());
  }
  if (!status.ok()) co_return status;
  co_return result;
}

}  // namespace lavik::storage
