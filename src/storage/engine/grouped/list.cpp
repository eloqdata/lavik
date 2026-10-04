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

#include <array>

#include "../impl.h"
#include "lavik/storage/detail/grouped/scratch.h"

namespace lavik::storage {
namespace {

// Keep reads owned by the parent, rather than detaching tasks that borrow its
// key, view and admission. Every launched read is joined, including when a
// later task/queue allocation fails. Shutdown destroys this owned child tree
// only after the worker has quiesced storage I/O.
class ListReadBatch {
 public:
  static constexpr std::size_t kWidth = 64;
  static constexpr std::size_t kPayloadBytes = 512 * 1024;
  using Result = absl::StatusOr<std::vector<std::string>>;
  using Read = Task<Result>;

  explicit ListReadBatch(bycorf::Worker* worker) : worker_(worker) {}

  void Add(Read read) {
    assert(size_ < kWidth);
    read.SetCompletionCallback(
        this, [](void* context, std::coroutine_handle<>) noexcept {
          auto& batch = *static_cast<ListReadBatch*>(context);
          assert(batch.pending_ != 0);
          if (--batch.pending_ == 0 && batch.waiter_)
            batch.worker_->Enqueue(std::exchange(batch.waiter_, {}));
        });
    const auto handle = std::move(read).ReleaseHandle();
    reads_[size_] = Read(handle);
    // If enqueue throws, this slot owns an unstarted task. The earlier slots
    // must still finish before the batch and its borrowed inputs disappear.
    worker_->Enqueue(handle);
    ++size_;
    ++pending_;
  }

  auto Join() {
    struct Awaiter {
      ListReadBatch* batch_;
      bool await_ready() const noexcept { return batch_->pending_ == 0; }
      void await_suspend(std::coroutine_handle<> waiter) const noexcept {
        batch_->waiter_ = waiter;
      }
      void await_resume() const noexcept {}
    };
    return Awaiter{this};
  }

  std::size_t size() const noexcept { return size_; }
  Result Take(std::size_t i) { return std::move(reads_[i]).TakeResult(); }

 private:
  bycorf::Worker* worker_;
  std::size_t size_ = 0;
  std::size_t pending_ = 0;
  std::coroutine_handle<> waiter_;
  std::array<Read, kWidth> reads_;
};

// Keep the task window out of the common List operation's coroutine frame:
// point reads and writes should not allocate space for unused parallel reads.
// The callable is owned here, and borrows inputs from the awaiting parent.
template <typename Load>
Task<absl::Status> AppendListReadWindow(bycorf::Worker* worker,
                                        std::size_t count, Load load,
                                        std::vector<std::string>& output) {
  ListReadBatch batch(worker);
  absl::Status status;
  try {
    for (std::size_t j = 0; j < count; ++j) batch.Add(load(j));
  } catch (const std::bad_alloc&) {
    status =
        absl::ResourceExhaustedError("OOM grouped List read batch allocation");
  }
  co_await batch.Join();
  if (!status.ok()) co_return status;
  try {
    for (std::size_t j = 0; j < batch.size(); ++j) {
      auto values = batch.Take(j);
      if (!values.ok()) co_return values.status();
      for (auto& value : *values) output.push_back(std::move(value));
    }
  } catch (const std::bad_alloc&) {
    co_return absl::ResourceExhaustedError(
        "OOM allocating grouped List operation");
  }
  co_return absl::OkStatus();
}

std::uint64_t Magnitude(std::int64_t value) {
  return value < 0 ? static_cast<std::uint64_t>(-(value + 1)) + 1
                   : static_cast<std::uint64_t>(value);
}

std::optional<std::uint64_t> ListIndex(std::int64_t index,
                                       std::uint64_t count) {
  if (index < 0) {
    const auto distance = Magnitude(index);
    return distance <= count ? std::optional(count - distance) : std::nullopt;
  }
  return static_cast<std::uint64_t>(index) < count
             ? std::optional(static_cast<std::uint64_t>(index))
             : std::nullopt;
}

std::pair<std::uint64_t, std::uint64_t> ListRange(std::int64_t first,
                                                  std::int64_t last,
                                                  std::uint64_t count) {
  if (count == 0) return {0, 0};
  const auto begin = first < 0 && Magnitude(first) > count
                         ? 0
                         : ListIndex(first, count).value_or(count);
  if (last < 0 && Magnitude(last) > count) return {count, count};
  const auto end = last >= 0 && static_cast<std::uint64_t>(last) >= count
                       ? count
                       : ListIndex(last, count).value_or(count - 1) + 1;
  return begin < end ? std::pair(begin, end) : std::pair(count, count);
}

absl::Status AppendPosition(std::uint64_t position, ListResult* result) {
  auto& output = result->positions_;
  if (output.size() == output.capacity()) {
    if (output.capacity() >
        std::numeric_limits<std::size_t>::max() / (2 * sizeof(std::int64_t)))
      return absl::ResourceExhaustedError("List position output overflow");
    const auto capacity = std::max<std::size_t>(8, output.capacity() * 2);
    auto admission = TryReserveMemory(capacity * sizeof(std::int64_t));
    if (!admission) {
      RecordMemoryRejection();
      return absl::ResourceExhaustedError("OOM List position output");
    }
    output.reserve(capacity);
    result->retained_charge_.Adopt(&*admission,
                                   output.capacity() * sizeof(std::int64_t));
  }
  output.push_back(position);
  return absl::OkStatus();
}

}  // namespace

Task<absl::StatusOr<ListResult>> StorageEngine::Impl::ExecuteGroupedListLocked(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const ListOperation& operation, GroupedHashObject::Handle object,
    TxShardWrites* tx, ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition,
    PreparedOrderedMutation* prepared) {
  try {
    if (object == nullptr || !object->is_ordered() ||
        object->ordered_directory().root().kind_ !=
            OrderedCollectionKind::kList) {
      co_return absl::DataLossError("invalid grouped List view");
    }
    const auto& directory = object->ordered_directory();
    const auto count = directory.root().item_count_;
    ListResult result;
    result.key_exists_ = true;
    result.length_ = count;
    if (operation.kind_ == ListOperationKind::kLength) co_return result;
    const bool read_only = operation.kind_ == ListOperationKind::kLength ||
                           operation.kind_ == ListOperationKind::kIndex ||
                           operation.kind_ == ListOperationKind::kRange ||
                           operation.kind_ == ListOperationKind::kPosition;
    // Searching a disk-backed List must not retain its unrelated values. Only
    // LPOS's actual output and one decoded page survive each iteration; LINSERT
    // retains just the pivot rank and reloads its affected interval below.
    const bool find_pivot =
        operation.kind_ == ListOperationKind::kInsertBefore ||
        operation.kind_ == ListOperationKind::kInsertAfter;
    std::optional<std::uint64_t> pivot_rank;
    if (find_pivot || operation.kind_ == ListOperationKind::kPosition) {
      try {
        const bool reverse = !find_pivot && operation.rank_ < 0;
        const auto wanted = Magnitude(operation.rank_);
        const auto limit = !operation.count_provided_ ? 1
                           : operation.count_ == 0
                               ? std::numeric_limits<std::uint64_t>::max()
                               : operation.count_;
        const auto inspect = !find_pivot && operation.max_length_provided_ &&
                                     operation.max_length_ != 0
                                 ? std::min(count, operation.max_length_)
                                 : count;
        std::uint64_t visited = 0, matches = 0;
        for (std::size_t step = 0;
             step < directory.groups().size() && visited < inspect &&
             !pivot_rank && (find_pivot || result.positions_.size() < limit);
             ++step) {
          if (shutdown_flush_requested_)
            co_return absl::CancelledError(
                "List search interrupted by shutdown");
          if (step != 0) co_await bycorf::Yield(*store.worker_);
          const auto index =
              reverse ? directory.groups().size() - 1 - step : step;
          const HashGroupId id{directory.groups()[index].id_, 0};
          const auto* physical = object->FindGroup(id);
          if (physical == nullptr)
            co_return absl::DataLossError("missing List search page");
          GroupedScratchBudget budget;
          auto added =
              budget.AddGroup(physical->value_, object->ExtentsFor(id));
          if (!added.ok()) co_return added;
          auto scratch = budget.Reserve(1);
          if (!scratch.ok()) co_return scratch.status();
          auto page = co_await LoadOrderedGroupSnapshot(
              store, partition, db_id, key, digest, object, id.prefix_);
          if (!page.ok()) co_return page.status();
          const auto& entries = page->snapshot_.entries_;
          for (std::size_t item = 0; item < entries.size() && visited < inspect;
               ++item, ++visited) {
            const auto offset = reverse ? entries.size() - 1 - item : item;
            const auto position = reverse ? count - 1 - visited : visited;
            if (entries[offset].value_ !=
                (find_pivot ? operation.pivot_ : operation.value_))
              continue;
            if (find_pivot) {
              pivot_rank = position;
              break;
            }
            if (++matches >= wanted) {
              auto appended = AppendPosition(position, &result);
              if (!appended.ok()) co_return appended;
              if (result.positions_.size() >= limit) break;
            }
          }
        }
      } catch (const std::bad_alloc&) {
        co_return absl::ResourceExhaustedError(
            "OOM allocating List search output");
      }
      if (!find_pivot) co_return result;
      if (!pivot_rank) {
        result.integer_ = -1;
        co_return result;
      }
    }
    // Incoming items are copied before page selection. Hold their independent
    // admission through the command, including the final mutation snapshots.
    GroupedScratchBudget incoming_budget;
    for (const auto value : operation.values_) {
      auto added = incoming_budget.AddBytes(value.size());
      if (!added.ok()) co_return added;
      added = incoming_budget.AddBytes(256);
      if (!added.ok()) co_return added;
    }
    const auto incoming_added =
        incoming_budget.AddBytes(operation.value_.size());
    if (!incoming_added.ok()) co_return incoming_added;
    auto incoming_scratch = incoming_budget.Reserve(4);
    if (!incoming_scratch.ok()) co_return incoming_scratch.status();
    std::uint64_t rank = 0;
    std::uint64_t erase_count = 0;
    std::vector<OrderedCollectionEntry> insertions;
    bool scan = false;
    switch (operation.kind_) {
      case ListOperationKind::kLength:
        co_return result;
      case ListOperationKind::kIndex: {
        const auto index = ListIndex(operation.first_, count);
        if (!index) co_return result;
        rank = *index;
        erase_count = 1;
        break;
      }
      case ListOperationKind::kRange: {
        const auto range =
            ListRange(operation.first_, operation.second_, count);
        rank = range.first;
        erase_count = range.second - range.first;
        if (erase_count == 0) co_return result;
        break;
      }
      case ListOperationKind::kPushLeft:
      case ListOperationKind::kPushLeftIfExists:
      case ListOperationKind::kPushRight:
      case ListOperationKind::kPushRightIfExists: {
        if (operation.values_.empty()) co_return result;
        const bool left =
            operation.kind_ == ListOperationKind::kPushLeft ||
            operation.kind_ == ListOperationKind::kPushLeftIfExists;
        rank = left ? 0 : count;
        for (auto value : operation.values_) {
          if (value.size() > kMaxStringBytes) {
            co_return absl::OutOfRangeError(
                "List element exceeds storage limits");
          }
          insertions.push_back({.value_ = std::string(value)});
        }
        if (left) std::reverse(insertions.begin(), insertions.end());
        break;
      }
      case ListOperationKind::kPopLeft:
      case ListOperationKind::kPopRight:
        erase_count = std::min(count, operation.count_);
        if (erase_count == 0) co_return result;
        rank = operation.kind_ == ListOperationKind::kPopLeft
                   ? 0
                   : count - erase_count;
        break;
      case ListOperationKind::kSet: {
        const auto index = ListIndex(operation.first_, count);
        if (!index) co_return absl::OutOfRangeError("index out of range");
        if (operation.value_.size() > kMaxStringBytes) {
          co_return absl::OutOfRangeError(
              "List element exceeds storage limits");
        }
        rank = *index;
        erase_count = 1;
        insertions.push_back({.value_ = std::string(operation.value_)});
        break;
      }
      case ListOperationKind::kInsertBefore:
      case ListOperationKind::kInsertAfter:
        if (operation.value_.size() > kMaxStringBytes)
          co_return absl::OutOfRangeError(
              "List element exceeds storage limits");
        rank =
            *pivot_rank + (operation.kind_ == ListOperationKind::kInsertAfter);
        insertions.push_back({.value_ = std::string(operation.value_)});
        result.integer_ = count + 1;
        break;
      case ListOperationKind::kRemove:
      case ListOperationKind::kTrim:
      case ListOperationKind::kMoveWithin:
        scan = true;
        erase_count = count;
        break;
      case ListOperationKind::kPosition:
        co_return absl::InternalError(
            "List position search was not dispatched");
    }

    const auto first = directory.FindRank(std::min(rank, count - 1));
    const auto last =
        erase_count == 0 ? first : directory.FindRank(rank + erase_count - 1);
    std::size_t begin_page = first->group_index_;
    std::size_t end_page = last->group_index_ + 1;
    if (!read_only) {
      // Links belong to their complete page snapshots. A split/removal can
      // change either neighbour, so pin its logical contents in the same plan.
      if (begin_page != 0) --begin_page;
      if (end_page != directory.groups().size()) ++end_page;
    }
    GroupedScratchBudget page_budget;
    for (std::size_t i = begin_page; i < end_page; ++i) {
      const HashGroupId id{directory.groups()[i].id_, 0};
      const auto* entry = object->FindGroup(id);
      if (entry == nullptr)
        co_return absl::DataLossError("missing List scratch page");
      const auto added =
          page_budget.AddGroup(entry->value_, object->ExtentsFor(id));
      if (!added.ok()) co_return added;
    }
    const auto read_width =
        std::min(ListReadBatch::kWidth, end_page - begin_page);
    if (read_only && read_width > 1) {
      // The ordinary budget includes one loader's fixed scratch. Admit the
      // window owner and additional loader frames before any parallel IO.
      const auto added = page_budget.AddBytes(read_width * 4096);
      if (!added.ok()) co_return added;
    }
    // Reads own each requested string once: page results move into the reply.
    // The per-entry allowance covers both vectors' string headers; physical
    // read buffers have independent accounting, including oversized pages.
    // No mutation/encoding copies are needed, even with a wave in flight.
    auto page_scratch = page_budget.Reserve(read_only ? 1 : 4);
    if (!page_scratch.ok()) co_return page_scratch.status();
    auto retain_output = [&]() -> absl::Status {
      std::size_t bytes = result.values_.capacity() * sizeof(std::string);
      for (const auto& value : result.values_) {
        if (value.capacity() >= std::numeric_limits<std::size_t>::max() - bytes)
          return absl::ResourceExhaustedError("List reply size overflow");
        bytes += value.capacity() + 1;
      }
      if (bytes > page_scratch->bytes())
        return absl::ResourceExhaustedError("List reply exceeds admission");
      // Keep the scratch reservation through publication too. A write reply
      // takes its retained charge before mutation; handing it off afterward
      // must not introduce a new failure after the root is already visible.
      result.retained_charge_.Account(CurrentMemoryAccountingShard(), bytes);
      return absl::OkStatus();
    };
    if (read_only) {
      // A bounded wave overlaps storage waits while retaining rank order in
      // the reply. Each loader checks its complete page and releases its lease
      // after copying only the requested values. LINDEX keeps its direct path.
      std::uint64_t remaining = erase_count;
      std::size_t offset = first->offset_;
      if (end_page - begin_page > 1) result.values_.reserve(erase_count);
      auto population_changed = [&] {
        return EffectiveRecordDbEpoch(partition, db_id) !=
                   object->version().db_epoch_ ||
               partition.replication_epoch_ !=
                   object->version().replication_epoch_ ||
               partition.grouped_generations_[db_id] !=
                   object->version().index_generation_;
      };
      absl::Status read_status;
      if (read_width == 1) {
        auto values = co_await LoadOrderedListRange(
            store, partition, db_id, key, digest, object,
            directory.groups()[begin_page].id_, offset, remaining);
        if (values.ok())
          result.values_ = std::move(*values);
        else
          read_status = values.status();
      } else {
        for (std::size_t i = begin_page; i < end_page;) {
          std::size_t pages = 0;
          std::uint64_t bytes = 0;
          while (pages < read_width && i + pages < end_page) {
            const auto size = directory.groups()[i + pages].encoded_bytes_;
            // Overlap ordinary pages, but never amplify oversized elements
            // into a window of many large physical buffers. One indivisible
            // page may exceed the byte bound and is then read alone.
            if (pages != 0 && (bytes >= ListReadBatch::kPayloadBytes ||
                               size > ListReadBatch::kPayloadBytes - bytes))
              break;
            bytes += size;
            ++pages;
          }
          read_status = co_await AppendListReadWindow(
              store.worker_, pages,
              [&](std::size_t j) {
                LAVIK_FAULT_INJECT(if (j == 1) {
                  LAVIK_FAULT_BAD_ALLOC("LAVIK_FAIL_LIST_READ_BATCH_START_KEY",
                                        key);
                });
                const auto& route = directory.groups()[i + j];
                const auto take = std::min<std::uint64_t>(
                    remaining, route.item_count_ - offset);
                auto read =
                    LoadOrderedListRange(store, partition, db_id, key, digest,
                                         object, route.id_, offset, take);
                remaining -= take;
                offset = 0;
                return read;
              },
              result.values_);
          // The window joins after the last loader's checks. A flush can run
          // in that gap even if every child decoded successfully. Its values
          // are owned, but must not be published for a replaced population.
          if (population_changed()) co_return ListResult{};
          if (!read_status.ok()) break;
          i += pages;
        }
      }
      if (!read_status.ok()) {
        if (population_changed()) co_return ListResult{};
        co_return read_status;
      }
      const auto retained = retain_output();
      if (!retained.ok()) co_return retained;
      co_return result;
    }
    std::vector<LoadedOrderedGroup> loaded;
    loaded.reserve(end_page - begin_page);
    std::optional<LoadedOrderedGroup> set_page;
    bool same_size_set = false;
    if (operation.kind_ == ListOperationKind::kSet) {
      if (prepared != nullptr) co_await bycorf::Yield(*store.worker_);
      auto page = co_await LoadOrderedGroupSnapshot(
          store, partition, db_id, key, digest, object,
          directory.groups()[first->group_index_].id_);
      if (!page.ok()) co_return page.status();
      if (first->offset_ >= page->snapshot_.entries_.size())
        co_return absl::DataLossError("List replacement rank exceeds page");
      same_size_set = page->snapshot_.entries_[first->offset_].value_.size() ==
                      operation.value_.size();
      set_page.emplace(std::move(*page));
      // The splice planner verifies that an equal-sized replacement preserves
      // the complete page and links. Retain the conservative page admission,
      // but avoid reading neighbours whose contents cannot affect this write.
      if (same_size_set) loaded.push_back(std::move(*set_page));
    }
    for (std::size_t i = begin_page; !same_size_set && i < end_page; ++i) {
      if (set_page && i == first->group_index_) {
        loaded.push_back(std::move(*set_page));
        continue;
      }
      if (prepared != nullptr) co_await bycorf::Yield(*store.worker_);
      auto page = co_await LoadOrderedGroupSnapshot(store, partition, db_id,
                                                    key, digest, object,
                                                    directory.groups()[i].id_);
      if (!page.ok()) {
        co_return page.status();
      }
      loaded.push_back(std::move(*page));
    }
    if (operation.kind_ == ListOperationKind::kPopLeft ||
        operation.kind_ == ListOperationKind::kPopRight) {
      std::uint64_t remaining = erase_count;
      std::uint64_t offset = first->offset_;
      for (std::size_t i = first->group_index_;
           remaining != 0 && i <= last->group_index_; ++i) {
        const auto& entries = loaded[i - begin_page].snapshot_.entries_;
        for (; remaining != 0 && offset < entries.size();
             ++offset, --remaining) {
          result.values_.push_back(entries[offset].value_);
        }
        offset = 0;
      }
      if (operation.kind_ == ListOperationKind::kPopRight) {
        std::reverse(result.values_.begin(), result.values_.end());
      }
      const auto retained = retain_output();
      if (!retained.ok()) co_return retained;
    }

    if (scan) {
      // Scanning commands inspect complete pages, but views borrow their item
      // strings. We copy only the actual replacement interval into the writer;
      // a search or no-op does not serialize or rewrite the collection.
      std::vector<std::string_view> before;
      before.reserve(count);
      for (const auto& page : loaded) {
        for (const auto& entry : page.snapshot_.entries_) {
          before.push_back(entry.value_);
        }
      }
      auto after = before;
      switch (operation.kind_) {
        case ListOperationKind::kRemove: {
          const auto limit = operation.first_ == 0
                                 ? std::numeric_limits<std::uint64_t>::max()
                                 : Magnitude(operation.first_);
          std::uint64_t removed = 0;
          if (operation.first_ >= 0) {
            auto end =
                std::remove_if(after.begin(), after.end(), [&](auto value) {
                  if (value != operation.value_ || removed == limit)
                    return false;
                  ++removed;
                  return true;
                });
            after.erase(end, after.end());
          } else {
            for (std::size_t i = after.size(); i != 0 && removed < limit;) {
              if (after[--i] == operation.value_) {
                after.erase(after.begin() + i);
                ++removed;
              }
            }
          }
          result.integer_ = removed;
          break;
        }
        case ListOperationKind::kTrim: {
          const auto range =
              ListRange(operation.first_, operation.second_, count);
          after = std::vector<std::string_view>(before.begin() + range.first,
                                                before.begin() + range.second);
          break;
        }
        case ListOperationKind::kMoveWithin: {
          const bool source_left = operation.first_ != 0;
          const bool destination_left = operation.second_ != 0;
          const auto moved = source_left ? after.front() : after.back();
          result.values_.emplace_back(moved);
          const auto retained = retain_output();
          if (!retained.ok()) co_return retained;
          if (source_left == destination_left || count == 1) co_return result;
          if (source_left)
            after.erase(after.begin());
          else
            after.pop_back();
          if (destination_left)
            after.insert(after.begin(), moved);
          else
            after.push_back(moved);
          break;
        }
        default:
          co_return absl::InternalError("invalid grouped List scan operation");
      }
      std::size_t common_prefix = 0;
      while (common_prefix < before.size() && common_prefix < after.size() &&
             before[common_prefix] == after[common_prefix])
        ++common_prefix;
      std::size_t common_suffix = 0;
      while (common_suffix < before.size() - common_prefix &&
             common_suffix < after.size() - common_prefix &&
             before[before.size() - common_suffix - 1] ==
                 after[after.size() - common_suffix - 1])
        ++common_suffix;
      rank = common_prefix;
      erase_count = before.size() - common_prefix - common_suffix;
      for (std::size_t i = common_prefix; i < after.size() - common_suffix;
           ++i) {
        insertions.push_back({.value_ = std::string(after[i])});
      }
      if (erase_count == 0 && insertions.empty()) {
        if (operation.kind_ != ListOperationKind::kMoveWithin) co_return result;
        // Rotating equal strings still constitutes a List mutation for WATCH,
        // like the compact path. Force one complete page's identical
        // after-image instead of falsely classifying that operation as a no-op.
        rank = 0;
        erase_count = 1;
        insertions.push_back({.value_ = std::string(before.front())});
      }
    }
    auto plan = PlanOrderedCollectionSplice(directory, std::move(loaded), rank,
                                            erase_count, std::move(insertions));
    if (!plan.ok()) co_return plan.status();
    result.changed_ = plan->changed_;
    result.length_ = plan->root_.item_count_;
    if (!result.changed_) co_return result;
    if (prepared != nullptr) {
      prepared->pages_ = std::move(*page_scratch);
      prepared->inputs_ = std::move(*incoming_scratch);
      prepared->plan_ = std::move(*plan);
      co_return result;
    }
    const auto status = co_await CommitGroupedOrderedMutationLocked(
        store, partition, db_id, key, digest, object, std::move(*plan),
        object->version().root_.expire_at_ms_, tx, replication,
        mutation_precondition);
    if (!status.ok()) co_return status;
    co_return result;
  } catch (const std::bad_alloc&) {
    co_return absl::ResourceExhaustedError(
        "OOM allocating grouped List operation");
  }
}

}  // namespace lavik::storage
