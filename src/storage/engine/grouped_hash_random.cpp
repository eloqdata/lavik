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
#include <new>

#include "impl.h"
#include "lavik/random_sample.h"
#include "lavik/storage/detail/grouped_scratch.h"

namespace lavik::storage {

Task<absl::StatusOr<HashResult>>
StorageEngine::Impl::ExecuteGroupedHashRandomLocked(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const HashOperation& operation, GroupedHashObject::Handle object,
    ValueType value_type, TxShardWrites* tx,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  try {
    const bool pop = operation.kind_ == HashOperationKind::kPopRandom;
    if (!object || object->is_ordered() ||
        (pop && (value_type != ValueType::kSet || operation.count_ < 0 ||
                 operation.with_values_)))
      co_return absl::InvalidArgumentError("invalid grouped random operation");
    HashResult result;
    result.key_exists_ = true;
    result.length_ = object->version().root_.logical_size_;
    if (operation.count_ == std::numeric_limits<std::int64_t>::min())
      co_return absl::OutOfRangeError("value is out of range");
    const bool repeat = operation.count_provided_ && operation.count_ < 0;
    auto requested = !operation.count_provided_
                         ? std::uint64_t{1}
                         : static_cast<std::uint64_t>(
                               repeat ? -operation.count_ : operation.count_);
    if (!repeat) requested = std::min(requested, result.length_);
    if (requested == 0 || result.length_ == 0) co_return result;
    constexpr auto max_size = std::numeric_limits<std::size_t>::max();
    if (requested > (max_size - 4096) / 256)
      co_return absl::ResourceExhaustedError("random sample count overflow");
    // The rank sampler may temporarily retain the population when selecting
    // more than a third of it. Its memory is still O(requested), not O(values).
    auto metadata = TryReserveMemory(4096 + requested * 256);
    if (!metadata) {
      RecordMemoryRejection();
      co_return absl::ResourceExhaustedError("OOM random sample metadata");
    }
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
    result.retained_charge_.Account(CurrentMemoryAccountingShard(),
                                    output_bytes);
    struct Selection {
      HashGroupId id_;
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

    auto add_page = [&](GroupedScratchBudget* budget, HashGroupId id) {
      const auto* record = object->FindGroup(id);
      return record == nullptr
                 ? absl::DataLossError("random sample page is missing")
                 : budget->AddGroup(record->value_, object->ExtentsFor(id));
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
    std::vector<HashGroupId> changed;
    changed.reserve(selected.size());
    for (std::size_t page_index = 0; page_index < selected.size();
         ++page_index) {
      if (shutdown_flush_requested_)
        co_return absl::CancelledError("random sample interrupted by shutdown");
      if (!pop && page_index != 0) co_await bycorf::Yield(*store.worker_);
      const auto& selection = selected[page_index];
      std::optional<MemoryReservation> page_scratch;
      if (!pop) {
        GroupedScratchBudget budget;
        const auto added = add_page(&budget, selection.id_);
        if (!added.ok()) co_return added;
        auto admission = budget.Reserve(1);
        if (!admission.ok()) co_return admission.status();
        page_scratch.emplace(std::move(*admission));
      }
      auto loaded = co_await LoadHashGroupSnapshot(
          store, partition, db_id, key, digest, object, selection.id_);
      if (!loaded.ok()) co_return loaded.status();
      auto& entries = loaded->snapshot_.value_.entries_;
      std::size_t duplicate_bytes = 0;
      for (auto i = selection.begin_; i < selection.end_; ++i) {
        const auto offset = samples[i].first - selection.base_;
        if (offset >= entries.size())
          co_return absl::DataLossError("random sample rank exceeds page");
        if (i + 1 == selection.end_ || samples[i].first != samples[i + 1].first)
          continue;
        const auto& entry = entries[offset];
        const auto bytes = entry.field_.size() +
                           (operation.with_values_ ? entry.value_.size() : 0);
        if (duplicate_bytes > max_size - 64 ||
            bytes > max_size - duplicate_bytes - 64)
          co_return absl::ResourceExhaustedError(
              "random sample output overflow");
        duplicate_bytes += bytes + 64;
      }
      auto copies = TryReserveMemory(duplicate_bytes);
      if (!copies) {
        RecordMemoryRejection();
        co_return absl::ResourceExhaustedError(
            "OOM repeated random sample output");
      }
      for (auto i = selection.begin_; i < selection.end_; ++i) {
        auto& entry = entries[samples[i].first - selection.base_];
        const auto slot = samples[i].second * width;
        const bool move =
            i + 1 == selection.end_ || samples[i].first != samples[i + 1].first;
        if (move) {
          result.values_[slot].emplace(std::move(entry.field_));
          if (operation.with_values_)
            result.values_[slot + 1].emplace(std::move(entry.value_));
        } else {
          result.values_[slot].emplace(entry.field_);
          if (operation.with_values_)
            result.values_[slot + 1].emplace(entry.value_);
        }
        for (std::size_t field = 0; field < width; ++field) {
          const auto capacity = result.values_[slot + field]->capacity();
          if (capacity >= max_size - output_bytes)
            co_return absl::ResourceExhaustedError(
                "random sample output overflow");
          output_bytes += capacity + 1;
        }
      }
      // Unique sampled strings move out of the decoded page; only repeated
      // draws allocate another payload. Transfer their charge before releasing
      // this page's admission so the next page accounts for retained output.
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
          mutation_precondition);
    } else {
      status = co_await AppendLocked(
          store, partition, db_id, key, digest, {}, RecordKind::kTombstone,
          ValueType::kNone, 0, tx, 0, nullptr, nullptr, replication, nullptr,
          true, nullptr, mutation_precondition);
    }
    if (!status.ok()) co_return status;
    co_return result;
  } catch (const std::bad_alloc&) {
    co_return absl::ResourceExhaustedError(
        "OOM allocating grouped random operation");
  }
}

}  // namespace lavik::storage
