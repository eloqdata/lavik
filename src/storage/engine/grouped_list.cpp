#include "impl.h"
#include "keylane/storage/detail/grouped_scratch.h"

namespace keylane::storage {
namespace {

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
    const MutationPrecondition* mutation_precondition) {
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
          if (step != 0) co_await celer::Yield(*store.worker_);
          const auto index =
              reverse ? directory.groups().size() - 1 - step : step;
          const HashGroupId id{directory.groups()[index].id_, 0};
          const auto* physical = object->FindGroup(id);
          if (physical == nullptr)
            co_return absl::DataLossError("missing List search page");
          GroupedScratchBudget budget;
          auto added = budget.AddGroup(physical->value_, object->ExtentsFor(id),
                                       key.size());
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
      const auto added = page_budget.AddGroup(
          entry->value_, object->ExtentsFor(id), key.size());
      if (!added.ok()) co_return added;
    }
    // Scan adapters and range replies may retain several pages plus replacement
    // entries. They must fail with OOM before accumulating an unbounded vector.
    auto page_scratch = page_budget.Reserve(4);
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
    std::vector<LoadedOrderedGroup> loaded;
    loaded.reserve(end_page - begin_page);
    for (std::size_t i = begin_page; i < end_page; ++i) {
      auto page = co_await LoadOrderedGroupSnapshot(store, partition, db_id,
                                                    key, digest, object,
                                                    directory.groups()[i].id_);
      if (!page.ok()) {
        if (read_only && (EffectiveRecordDbEpoch(partition, db_id) !=
                              object->version().db_epoch_ ||
                          partition.replication_epoch_ !=
                              object->version().replication_epoch_ ||
                          partition.grouped_generations_[db_id] !=
                              object->version().index_generation_)) {
          co_return ListResult{};
        }
        co_return page.status();
      }
      loaded.push_back(std::move(*page));
    }
    if (operation.kind_ == ListOperationKind::kIndex ||
        operation.kind_ == ListOperationKind::kRange ||
        operation.kind_ == ListOperationKind::kPopLeft ||
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
      if (read_only) co_return result;
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

}  // namespace keylane::storage
