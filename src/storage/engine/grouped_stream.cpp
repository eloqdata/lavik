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
#include "lavik/storage/detail/grouped_scratch.h"
#include "lavik/storage/detail/stream_records.h"

namespace lavik::storage {
namespace {
std::uint32_t Count(std::string_view value, std::size_t at = 0) {
  std::uint32_t count = 0;
  for (unsigned i = 0; i < 4; ++i)
    count |= std::uint32_t(static_cast<unsigned char>(value[at + i]))
             << (8 * i);
  return count;
}
void SetCount(std::string& value, std::size_t at, std::uint32_t count) {
  for (unsigned i = 0; i < 4; ++i) value[at + i] = count >> (8 * i);
}
std::string Record(std::string_view key, std::string_view payload) {
  std::string out(key);
  out.append(payload);
  const auto at = out.size();
  out.resize(at + 4);
  SetCount(out, at, key.size());
  return out;
}
std::string GroupPrefix(std::string_view group) {
  std::string key(1, '\5');
  for (char ch : group) {
    key.push_back(ch);
    if (ch == '\0') key.push_back('\xff');
  }
  key.append("\0\0", 2);
  return key;
}
std::string ConsumerKey(std::string_view prefix, std::string_view name) {
  std::string key(prefix);
  key.push_back('\1');
  for (char ch : name) {
    key.push_back(ch);
    if (ch == '\0') key.push_back('\xff');
  }
  key.append("\0\0", 2);
  return key;
}
std::string IdKey(std::string_view prefix,
                  const std::array<std::uint64_t, 2>& id) {
  std::string key(prefix);
  for (auto half : id)
    for (unsigned i = 8; i != 0; --i) key.push_back(half >> ((i - 1) * 8));
  return key;
}
std::array<std::uint64_t, 2> ReadId(std::string_view payload) {
  std::array<std::uint64_t, 2> id{};
  for (unsigned half = 0; half < 2; ++half)
    for (unsigned i = 0; i < 8; ++i)
      id[half] |=
          std::uint64_t(static_cast<unsigned char>(payload[half * 8 + i]))
          << (8 * i);
  return id;
}

}  // namespace

// Request-local page access. The key intent protects logical identity while
// loaders retry physical relocation. Admission precedes every decoded page,
// and cache entries never escape the owner worker or the command lifetime.
struct StorageEngine::Impl::StreamPageAccess {
  Impl& engine_;
  WorkerStore& store_;
  WorkerStore::PartitionStore& partition_;
  std::uint8_t db_id_;
  std::string_view key_;
  const Digest& digest_;
  GroupedHashObject::Handle object_;
  std::vector<MemoryReservation> reservations_{};
  std::map<std::size_t, LoadedOrderedGroup> pages_{};
  std::map<std::size_t, MemoryReservation> page_reservations_{};

  Task<absl::Status> Load(std::size_t index) {
    try {
      if (pages_.contains(index)) co_return absl::OkStatus();
      const auto& groups = object_->ordered_directory().groups();
      const HashGroupId id{groups[index].id_, 0};
      const auto* physical = object_->FindGroup(id);
      if (!physical) co_return absl::DataLossError("missing Stream page");
      GroupedScratchBudget budget;
      auto added = budget.AddGroup(physical->value_, object_->ExtentsFor(id));
      if (!added.ok()) co_return added;
      auto admitted = budget.Reserve(6);
      if (!admitted.ok()) co_return admitted.status();
      page_reservations_.emplace(index, std::move(*admitted));
      auto page = co_await engine_.LoadOrderedGroupSnapshot(
          store_, partition_, db_id_, key_, digest_, object_, id.prefix_);
      if (!page.ok()) co_return page.status();
      pages_.emplace(index, std::move(*page));
      co_return absl::OkStatus();
    } catch (const std::bad_alloc&) {
      RecordMemoryRejection();
      co_return absl::ResourceExhaustedError("OOM grouped Stream workspace");
    }
  }
  Task<absl::StatusOr<std::size_t>> Route(std::string_view wanted) {
    try {
      const auto size = object_->ordered_directory().groups().size();
      std::size_t first = 0, last = size;
      while (first < last) {
        const auto middle = first + (last - first) / 2;
        auto status = co_await Load(middle);
        if (!status.ok()) co_return status;
        auto bound =
            StreamRecordKey(pages_.at(middle).snapshot_.entries_.back().value_);
        if (!bound.ok()) co_return bound.status();
        if (*bound < wanted)
          first = middle + 1;
        else
          last = middle;
      }
      co_return std::min(first, size - 1);
    } catch (const std::bad_alloc&) {
      RecordMemoryRejection();
      co_return absl::ResourceExhaustedError("OOM grouped Stream workspace");
    }
  }
  Task<absl::StatusOr<std::optional<std::string>>> Find(
      std::string_view wanted) {
    try {
      auto index = co_await Route(wanted);
      if (!index.ok()) co_return index.status();
      auto status = co_await Load(*index);
      if (!status.ok()) co_return status;
      for (const auto& entry : pages_.at(*index).snapshot_.entries_) {
        auto key = StreamRecordKey(entry.value_);
        if (!key.ok()) co_return key.status();
        if (*key == wanted) co_return std::optional(entry.value_);
      }
      co_return std::nullopt;
    } catch (const std::bad_alloc&) {
      RecordMemoryRejection();
      co_return absl::ResourceExhaustedError("OOM grouped Stream workspace");
    }
  }
  // Returns the predecessor at or below a routing key. Logical node records
  // are keyed by their first live ID, independently of physical page splits.
  Task<absl::StatusOr<std::optional<std::string>>> Previous(
      std::string_view wanted) {
    auto index = co_await Route(wanted);
    if (!index.ok()) co_return index.status();
    for (;;) {
      auto status = co_await Load(*index);
      if (!status.ok()) co_return status;
      const auto& entries = pages_.at(*index).snapshot_.entries_;
      for (auto it = entries.rbegin(); it != entries.rend(); ++it) {
        auto key = StreamRecordKey(it->value_);
        if (!key.ok()) co_return key.status();
        if (*key <= wanted) co_return std::optional(it->value_);
      }
      if (*index == 0) co_return std::nullopt;
      --*index;
    }
  }
  Task<absl::StatusOr<std::string>> Required(std::string_view wanted) {
    try {
      auto record = co_await Find(wanted);
      if (!record.ok()) co_return record.status();
      if (!*record)
        co_return absl::DataLossError("missing Stream metadata record");
      co_return std::move(**record);
    } catch (const std::bad_alloc&) {
      RecordMemoryRejection();
      co_return absl::ResourceExhaustedError("OOM grouped Stream workspace");
    }
  }
  Task<absl::StatusOr<std::vector<std::string>>> Scan(std::string_view low,
                                                      std::string_view high,
                                                      std::uint64_t count,
                                                      bool exclusive = false) {
    try {
      std::vector<std::string> result;
      if (count == 0) co_return result;
      auto first = co_await Route(low);
      if (!first.ok()) co_return first.status();
      for (auto index = *first;
           index < object_->ordered_directory().groups().size(); ++index) {
        auto status = co_await Load(index);
        if (!status.ok()) co_return status;
        for (const auto& entry : pages_.at(index).snapshot_.entries_) {
          auto key = StreamRecordKey(entry.value_);
          if (!key.ok()) co_return key.status();
          if (*key >= high) co_return result;
          if (*key < low || (exclusive && *key == low)) continue;
          result.push_back(entry.value_);
          if (result.size() == count) co_return result;
        }
        co_await bycorf::Yield(*store_.worker_);
      }
      co_return result;
    } catch (const std::bad_alloc&) {
      RecordMemoryRejection();
      co_return absl::ResourceExhaustedError("OOM grouped Stream workspace");
    }
  }
  Task<absl::Status> Visit(
      std::string_view low, std::string_view high,
      const std::function<absl::Status(std::string_view)>& visitor) {
    auto first = co_await Route(low);
    if (!first.ok()) co_return first.status();
    for (auto i = *first; i < object_->ordered_directory().groups().size();
         ++i) {
      const bool cached = pages_.contains(i);
      auto status = co_await Load(i);
      if (!status.ok()) co_return status;
      bool done = false;
      for (const auto& entry : pages_.at(i).snapshot_.entries_) {
        auto key = StreamRecordKey(entry.value_);
        if (!key.ok()) co_return key.status();
        if (*key >= high) {
          done = true;
          break;
        }
        if (*key < low) continue;
        status = visitor(entry.value_);
        if (!status.ok()) co_return status;
      }
      if (!cached) {
        pages_.erase(i);
        page_reservations_.erase(i);
      }
      if (done) break;
      co_await bycorf::Yield(*store_.worker_);
    }
    co_return absl::OkStatus();
  }
  Task<absl::StatusOr<std::vector<std::string>>> ScanPending(
      std::string_view prefix, const StreamPendingAccess& access) {
    std::vector<std::string> result;
    const auto& range = access.range_;
    if (range.count_ == 0) co_return result;
    const auto low = IdKey(std::string(prefix) + '\3', range.first_);
    const auto high = IdKey(std::string(prefix) + '\3', range.last_);
    auto first = co_await Route(low);
    if (!first.ok()) co_return first.status();
    for (auto i = *first; i < object_->ordered_directory().groups().size();
         ++i) {
      const bool cached = pages_.contains(i);
      auto status = co_await Load(i);
      if (!status.ok()) co_return status;
      const auto before = result.size();
      for (const auto& entry : pages_.at(i).snapshot_.entries_) {
        auto key = StreamRecordKey(entry.value_);
        if (!key.ok()) co_return key.status();
        if (*key > high || (range.last_exclusive_ && *key == high))
          co_return result;
        if (*key < low || (range.first_exclusive_ && *key == low)) continue;
        auto payload = StreamRecordPayload(entry.value_);
        if (!payload.ok() || payload->size() < 36)
          co_return absl::DataLossError("invalid Stream pending row");
        const auto owner_bytes = Count(*payload, 16);
        if (owner_bytes != payload->size() - 36)
          co_return absl::DataLossError("invalid Stream pending owner");
        if (access.consumer_ &&
            payload->substr(20, owner_bytes) != *access.consumer_)
          continue;
        const auto delivered = ReadId(payload->substr(20 + owner_bytes))[0];
        if (access.now_ms_ - std::min(access.now_ms_, delivered) <
            access.min_idle_ms_)
          continue;
        result.push_back(entry.value_);
        if (result.size() == range.count_) co_return result;
      }
      // A consumer filter can scan an arbitrarily large unrelated PEL. Its
      // rejected pages and admission must not accumulate behind the cursor.
      if (!cached && before == result.size()) {
        pages_.erase(i);
        page_reservations_.erase(i);
      }
      co_await bycorf::Yield(*store_.worker_);
    }
    co_return result;
  }
  // Interior pages are entirely inside the range: their checked directory
  // cardinalities suffice. Only the two boundary pages need decoding.
  Task<absl::StatusOr<std::uint64_t>> CountRange(std::string_view low,
                                                 std::string_view high) {
    if (low >= high) co_return 0;
    auto first = co_await Route(low), last = co_await Route(high);
    if (!first.ok()) co_return first.status();
    if (!last.ok()) co_return last.status();
    std::uint64_t count = 0;
    const auto& groups = object_->ordered_directory().groups();
    for (auto i = *first; i <= *last; ++i) {
      if (i != *first && i != *last) {
        count += groups[i].item_count_;
        continue;
      }
      auto status = co_await Load(i);
      if (!status.ok()) co_return status;
      for (const auto& entry : pages_.at(i).snapshot_.entries_) {
        auto key = StreamRecordKey(entry.value_);
        if (!key.ok()) co_return key.status();
        if (*key >= low && *key < high) ++count;
      }
    }
    co_return count;
  }
  Task<absl::StatusOr<std::optional<std::string>>> Select(std::string_view low,
                                                          std::string_view high,
                                                          std::uint64_t rank) {
    auto first = co_await Route(low), last = co_await Route(high);
    if (!first.ok()) co_return first.status();
    if (!last.ok()) co_return last.status();
    const auto& groups = object_->ordered_directory().groups();
    for (auto i = *first; i <= *last; ++i) {
      if (i != *first && i != *last && rank >= groups[i].item_count_) {
        rank -= groups[i].item_count_;
        continue;
      }
      auto status = co_await Load(i);
      if (!status.ok()) co_return status;
      for (const auto& entry : pages_.at(i).snapshot_.entries_) {
        auto key = StreamRecordKey(entry.value_);
        if (!key.ok()) co_return key.status();
        if (*key >= low && *key < high) {
          if (rank == 0) co_return std::optional(entry.value_);
          --rank;
        }
      }
    }
    co_return std::nullopt;
  }
  static void Change(std::vector<StreamRecordChange>& changes, std::string key,
                     std::optional<std::string> record) {
    auto found =
        std::find_if(changes.begin(), changes.end(),
                     [&](const auto& item) { return item.key_ == key; });
    if (found == changes.end())
      changes.push_back({.key_ = std::move(key), .record_ = std::move(record)});
    else
      found->record_ = std::move(record);
  }
  Task<absl::Status> EraseRange(std::string_view low, std::string_view high,
                                std::vector<StreamRecordChange>& changes,
                                std::vector<std::uint64_t>& retired) {
    if (low >= high) co_return absl::OkStatus();
    auto first = co_await Route(low), last = co_await Route(high);
    if (!first.ok()) co_return first.status();
    if (!last.ok()) co_return last.status();
    std::erase_if(changes, [&](const auto& item) {
      return item.key_ >= low && item.key_ < high;
    });
    const auto& groups = object_->ordered_directory().groups();
    for (auto i = *first; i <= *last; ++i) {
      if (i != *first && i != *last) {
        retired.push_back(groups[i].id_);
        continue;
      }
      auto status = co_await Load(i);
      if (!status.ok()) co_return status;
      for (const auto& entry : pages_.at(i).snapshot_.entries_) {
        auto key = StreamRecordKey(entry.value_);
        if (!key.ok()) co_return key.status();
        if (*key >= low && *key < high)
          Change(changes, std::string(*key), std::nullopt);
      }
    }
    co_return absl::OkStatus();
  }
  Task<absl::StatusOr<std::uint64_t>> ErasePendingConsumer(
      std::string_view prefix, std::string_view owner,
      std::vector<StreamRecordChange>& changes,
      std::vector<std::uint64_t>& retired) {
    const std::string low = std::string(prefix) + '\3';
    std::string high(prefix);
    high.back() = '\1';
    auto first = co_await Route(low), last = co_await Route(high);
    if (!first.ok()) co_return first.status();
    if (!last.ok()) co_return last.status();
    std::uint64_t removed = 0;
    const auto& groups = object_->ordered_directory().groups();
    for (auto i = *first; i <= *last; ++i) {
      const bool cached = pages_.contains(i);
      auto status = co_await Load(i);
      if (!status.ok()) co_return status;
      std::vector<std::string> matches;
      const auto& entries = pages_.at(i).snapshot_.entries_;
      for (const auto& entry : entries) {
        auto key = StreamRecordKey(entry.value_);
        if (!key.ok()) co_return key.status();
        if (*key < low || *key >= high) continue;
        auto payload = StreamRecordPayload(entry.value_);
        if (!payload.ok() || payload->size() < 36 ||
            Count(*payload, 16) != payload->size() - 36)
          co_return absl::DataLossError(
              "invalid pending consumer removal record");
        if (payload->substr(20, Count(*payload, 16)) == owner)
          matches.emplace_back(*key);
      }
      removed += matches.size();
      if (matches.size() == entries.size()) {
        retired.push_back(groups[i].id_);
        pages_.erase(i);
        page_reservations_.erase(i);
      } else if (matches.empty() && !cached) {
        pages_.erase(i);
        page_reservations_.erase(i);
      } else {
        // PEL IDs in this scan are unique and disjoint from the consumer
        // header changes, so append directly rather than quadratic
        // deduplication.
        for (auto& key : matches)
          changes.push_back({.key_ = std::move(key), .record_ = std::nullopt});
      }
      co_await bycorf::Yield(*store_.worker_);
    }
    co_return removed;
  }
  Task<absl::StatusOr<StreamTrimResult>> Trim(
      const StreamTrimRequest& trim, std::vector<StreamRecordChange>& changes,
      std::vector<std::uint64_t>& retired, std::uint64_t length) {
    const std::string entries_begin(1, '\1'), entries_end(1, '\2');
    const std::string nodes_begin(1, '\3'), nodes_end(1, '\4');
    const auto old_length = object_->ordered_directory().root().logical_size();
    std::optional<std::string> appended;
    for (const auto& item : changes)
      if (item.key_.starts_with(entries_begin) && item.record_)
        appended = item.record_;
    auto select = [&](std::uint64_t rank)
        -> Task<absl::StatusOr<std::optional<std::string>>> {
      if (rank == old_length) co_return appended;
      co_return co_await Select(entries_begin, entries_end, rank);
    };
    auto node_at =
        [&](std::string_view entry_key) -> Task<absl::StatusOr<std::string>> {
      std::string wanted(entry_key);
      wanted[0] = '\3';
      auto previous = co_await Previous(wanted);
      if (!previous.ok()) co_return previous.status();
      std::optional<std::string> result;
      if (*previous && (**previous).front() == '\3')
        result = std::move(**previous);
      // An append may create a new node or increment the old tail node.
      for (const auto& change : changes) {
        if (!change.key_.starts_with(nodes_begin) || !change.record_ ||
            change.key_ > wanted)
          continue;
        auto current =
            result ? StreamRecordKey(*result)
                   : absl::StatusOr<std::string_view>(std::string_view{});
        if (!current.ok()) co_return current.status();
        if (!result || change.key_ >= *current) result = *change.record_;
      }
      if (!result) co_return absl::DataLossError("missing Stream trim node");
      co_return std::move(*result);
    };
    std::uint64_t remove = 0;
    if (trim.max_length_) {
      remove = length > *trim.max_length_ ? length - *trim.max_length_ : 0;
    } else {
      auto count = co_await CountRange(entries_begin,
                                       IdKey(entries_begin, trim.min_id_));
      if (!count.ok()) co_return count.status();
      remove = *count;
      if (appended) {
        auto value = StreamRecordPayload(*appended);
        if (!value.ok()) co_return value.status();
        if (ReadId(*value) < trim.min_id_) ++remove;
      }
    }
    if (trim.approximate_ && trim.limit_ != 0)
      remove = std::min(remove, trim.limit_);
    if (remove > length)
      co_return absl::DataLossError("Stream trim length mismatch");
    std::optional<std::string> first, node;
    if (remove < length) {
      auto selected = co_await select(remove);
      if (!selected.ok()) co_return selected.status();
      if (!*selected)
        co_return absl::DataLossError("missing Stream trim boundary");
      first = std::move(**selected);
      auto key = StreamRecordKey(*first);
      if (!key.ok()) co_return key.status();
      auto containing = co_await node_at(*key);
      if (!containing.ok()) co_return containing.status();
      node = std::move(*containing);
      if (trim.approximate_ && remove != 0) {
        auto start = StreamRecordKey(*node);
        if (!start.ok()) co_return start.status();
        std::string boundary(*start);
        boundary[0] = '\1';
        if (boundary != *key) {
          auto count = co_await CountRange(entries_begin, boundary);
          if (!count.ok()) co_return count.status();
          remove = *count;
          selected = co_await select(remove);
          if (!selected.ok()) co_return selected.status();
          if (!*selected)
            co_return absl::DataLossError("missing Stream node boundary");
          first = std::move(**selected);
        }
      }
    }
    StreamTrimResult result{.removed_ = remove, .length_ = length - remove};
    if (first) {
      auto value = StreamRecordPayload(*first);
      if (!value.ok()) co_return value.status();
      result.first_id_ = ReadId(*value);
    }
    if (remove == 0) co_return result;
    std::string message_boundary = entries_end, node_boundary = nodes_end;
    std::uint64_t remaining_nodes = 0;
    if (first) {
      auto key = StreamRecordKey(*first), node_key = StreamRecordKey(*node);
      auto node_payload = StreamRecordPayload(*node);
      if (!key.ok() || !node_key.ok() || !node_payload.ok() ||
          node_payload->size() != 4)
        co_return absl::DataLossError("invalid Stream trim boundary");
      message_boundary = *key;
      node_boundary = *node_key;
      auto count = co_await CountRange(nodes_begin, node_boundary);
      if (!count.ok()) co_return count.status();
      auto all_nodes = co_await Required(entries_end);
      if (!all_nodes.ok()) co_return all_nodes.status();
      for (const auto& change : changes)
        if (change.key_ == entries_end && change.record_)
          all_nodes = *change.record_;
      auto total = StreamRecordPayload(*all_nodes);
      if (!total.ok() || total->size() != 4 || Count(*total) <= *count)
        co_return absl::DataLossError("invalid Stream trim node count");
      remaining_nodes = Count(*total) - *count;
      std::string old_first(node_boundary);
      old_first[0] = '\1';
      auto preceding = co_await CountRange(entries_begin, old_first);
      if (!preceding.ok()) co_return preceding.status();
      const auto within = remove - *preceding;
      if (within >= Count(*node_payload))
        co_return absl::DataLossError("invalid partial Stream trim node");
      if (within != 0) {
        std::string new_key(message_boundary), count_bytes(4, '\0');
        new_key[0] = '\3';
        SetCount(count_bytes, 0, Count(*node_payload) - within);
        Change(changes, node_boundary, std::nullopt);
        Change(changes, new_key, Record(new_key, count_bytes));
      }
    }
    auto status =
        co_await EraseRange(entries_begin, message_boundary, changes, retired);
    if (!status.ok()) co_return status;
    status = co_await EraseRange(nodes_begin, node_boundary, changes, retired);
    if (!status.ok()) co_return status;
    std::string count(4, '\0');
    SetCount(count, 0, remaining_nodes);
    Change(changes, entries_end, Record(entries_end, count));
    auto header = co_await Required(std::string_view("\0", 1));
    if (!header.ok()) co_return header.status();
    for (const auto& change : changes)
      if (change.key_ == std::string_view("\0", 1) && change.record_)
        header = *change.record_;
    auto payload = StreamRecordPayload(*header);
    if (!payload.ok() || payload->size() != 48)
      co_return absl::DataLossError("invalid Stream trim header");
    std::string bytes(*payload);
    SetCount(bytes, 44, result.length_);
    Change(changes, std::string("\0", 1),
           Record(std::string_view("\0", 1), bytes));
    co_return result;
  }
  Task<absl::StatusOr<OrderedCollectionMutationPlan>> Plan(
      std::vector<StreamRecordChange> changes, std::uint64_t length,
      std::vector<std::uint64_t> retired = {}) {
    try {
      const auto& directory = object_->ordered_directory();
      const auto& groups = directory.groups();
      GroupedScratchBudget budget;
      auto added = budget.AddBytes(groups.size() * 64);
      if (!added.ok()) co_return added;
      auto admitted = budget.Reserve(1);
      if (!admitted.ok()) co_return admitted.status();
      // The returned plan owns directory-sized state through publication.
      // Keep its admission with this command's cache, beyond this coroutine.
      reservations_.push_back(std::move(*admitted));
      if (changes.empty() && retired.empty()) {
        // A Changed callback can intentionally publish identical metadata
        // (e.g. XGROUP SETID or replay of its exact after-state). Retain the
        // mutation/replication sequence semantics with one unchanged page.
        OrderedCollectionMutationPlan plan{
            .root_ = directory.root(),
            .expected_sequence_ = directory.sequence(),
            .changed_ = true,
            .writes_ = {std::move(pages_.begin()->second.snapshot_)}};
        plan.root_.revision_ = 0;
        co_return plan;
      }
      const std::set<std::uint64_t> retired_set(retired.begin(), retired.end());
      for (std::size_t i = 0; i < groups.size(); ++i) {
        if (!retired_set.contains(groups[i].id_) &&
            (retired_set.contains(groups[i].previous_) ||
             retired_set.contains(groups[i].next_))) {
          auto status = co_await Load(i);
          if (!status.ok()) co_return status;
        }
      }
      for (auto& change : changes) {
        auto index = co_await Route(change.key_);
        if (!index.ok()) co_return index.status();
        for (auto adjacent = *index == 0 ? 0 : *index - 1;
             adjacent < std::min(*index + 2, groups.size()); ++adjacent) {
          if (retired_set.contains(groups[adjacent].id_)) continue;
          auto status = co_await Load(adjacent);
          if (!status.ok()) co_return status;
        }
        change.page_id_ = groups[*index].id_;
      }
      std::vector<LoadedOrderedGroup> loaded;
      loaded.reserve(pages_.size());
      for (auto& [index, page] : pages_) loaded.push_back(std::move(page));
      co_return PlanStreamRecordChanges(directory, std::move(loaded),
                                        std::move(changes), length, retired);
    } catch (const std::bad_alloc&) {
      RecordMemoryRejection();
      co_return absl::ResourceExhaustedError("OOM grouped Stream workspace");
    }
  }
};

Task<absl::Status> StorageEngine::Impl::ExecuteGroupedStreamAppendLocked(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    GroupedHashObject::Handle object, const CompactValueCallback& callback,
    std::uint32_t node_max_entries, TxShardWrites* tx,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  try {
    if (!object->is_ordered() || object->ordered_directory().root().kind_ !=
                                     OrderedCollectionKind::kStream)
      co_return absl::DataLossError("invalid grouped Stream append view");
    const auto& directory = object->ordered_directory();
    const auto length = directory.root().logical_size();
    StreamPageAccess cache{*this, store, partition, db_id, key, digest, object};
    auto& pages = cache.pages_;
    auto load = [&](std::size_t index) { return cache.Load(index); };
    auto route = [&](std::string_view wanted) { return cache.Route(wanted); };
    auto find = [&](std::string_view wanted) { return cache.Required(wanted); };
    auto header = co_await find(std::string_view("\0", 1));
    if (!header.ok()) co_return header.status();
    auto header_payload = StreamRecordPayload(*header);
    if (!header_payload.ok()) co_return header_payload.status();
    if (header_payload->size() != 48 || !header_payload->starts_with("LXS1") ||
        Count(*header_payload, 44) != length)
      co_return absl::DataLossError("Stream length disagrees with root");
    std::string partial(*header_payload);
    SetCount(partial, 44, 0);
    partial.append(8, '\0');
    auto update = callback(
        CompactValueView{.encoded_ = partial,
                         .logical_size_ = 0,
                         .expire_at_ms_ = object->version().root_.expire_at_ms_,
                         .stream_incremental_trim_ = true});
    if (!update.ok()) co_return update.status();
    if (!update->changed_) co_return absl::OkStatus();
    if (update->erase_ || update->reuse_encoded_ ||
        update->logical_size_ != 1 || length == UINT32_MAX)
      co_return absl::OutOfRangeError("invalid Stream append result/count");
    GroupedScratchBudget incoming;
    auto added = incoming.AddBytes(update->encoded_.size());
    if (!added.ok()) co_return added;
    auto admitted = incoming.Reserve(6);
    if (!admitted.ok()) co_return admitted.status();
    auto records = DecodeStreamRecords(update->encoded_, 1);
    if (!records.ok()) co_return records.status();
    // One header, entry, node-count, node and group-count, with no groups. This
    // rejects a callback violating its append-only access contract before
    // write.
    if (records->size() != 5)
      co_return absl::InvalidArgumentError("Stream append changed group state");
    auto new_header = StreamRecordPayload((*records)[0].value_);
    if (!new_header.ok()) co_return new_header.status();
    std::string header_bytes(*new_header);
    SetCount(header_bytes, 44, length + 1);
    (*records)[0].value_ = Record(std::string_view("\0", 1), header_bytes);
    auto node_count = co_await find(std::string_view("\2", 1));
    if (!node_count.ok()) co_return node_count.status();
    auto node_payload = StreamRecordPayload(*node_count);
    if (!node_payload.ok() || node_payload->size() != 4)
      co_return absl::DataLossError("invalid Stream node count");
    const auto nodes = Count(*node_payload);
    if ((nodes == 0) != (length == 0) || nodes > length)
      co_return absl::DataLossError("invalid Stream node aggregate");
    std::optional<std::string> last_node;
    if (nodes != 0) {
      auto index = co_await route(std::string_view("\4", 1));
      if (!index.ok()) co_return index.status();
      auto status = co_await load(*index);
      if (!status.ok()) co_return status;
      for (const auto& entry : pages.at(*index).snapshot_.entries_) {
        if (entry.value_.front() == '\3') last_node = entry.value_;
      }
      if (!last_node && *index != 0) {
        status = co_await load(*index - 1);
        if (!status.ok()) co_return status;
        const auto& entry = pages.at(*index - 1).snapshot_.entries_.back();
        if (entry.value_.front() == '\3') last_node = entry.value_;
      }
      if (!last_node) co_return absl::DataLossError("missing Stream tail node");
    }
    std::vector<std::string> replacements;
    replacements.push_back(std::move((*records)[0].value_));
    replacements.push_back(std::move((*records)[1].value_));
    std::uint32_t tail_count = 0;
    if (last_node) {
      auto payload = StreamRecordPayload(*last_node);
      if (!payload.ok() || payload->size() != 4)
        co_return absl::DataLossError("invalid Stream tail node");
      tail_count = Count(*payload);
      if (tail_count == 0 || tail_count > length)
        co_return absl::DataLossError("invalid Stream tail count");
    }
    if (!last_node || tail_count >= node_max_entries) {
      replacements.push_back(std::move((*records)[3].value_));
      std::string count_bytes(4, '\0');
      SetCount(count_bytes, 0, nodes + 1);
      replacements.push_back(Record(std::string_view("\2", 1), count_bytes));
    } else {
      auto tail_key = StreamRecordKey(*last_node);
      if (!tail_key.ok()) co_return tail_key.status();
      std::string count_bytes(4, '\0');
      SetCount(count_bytes, 0, tail_count + 1);
      replacements.push_back(Record(*tail_key, count_bytes));
    }
    std::vector<StreamRecordChange> changes;
    for (auto& replacement : replacements) {
      auto record_key = StreamRecordKey(replacement);
      if (!record_key.ok()) co_return record_key.status();
      changes.push_back({.key_ = std::string(*record_key),
                         .record_ = std::move(replacement)});
    }
    std::uint64_t final_length = length + 1;
    std::vector<std::uint64_t> retired;
    if (update->stream_trim_) {
      auto result = co_await cache.Trim(*update->stream_trim_, changes, retired,
                                        final_length);
      if (!result.ok()) co_return result.status();
      final_length = result->length_;
      if (update->stream_trim_complete_) update->stream_trim_complete_(*result);
    }
    auto plan = co_await cache.Plan(std::move(changes), final_length,
                                    std::move(retired));
    if (!plan.ok()) co_return plan.status();
    co_return co_await CommitGroupedOrderedMutationLocked(
        store, partition, db_id, key, digest, object, std::move(*plan),
        update->expire_at_ms_.value_or(object->version().root_.expire_at_ms_),
        tx, replication, mutation_precondition);
  } catch (const std::bad_alloc&) {
    RecordMemoryRejection();
    co_return absl::ResourceExhaustedError("OOM grouped Stream workspace");
  }
}

Task<absl::Status> StorageEngine::Impl::ExecuteGroupedStreamHeaderLocked(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    GroupedHashObject::Handle object, const CompactValueCallback& callback,
    TxShardWrites* tx, ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  try {
    StreamPageAccess cache{*this, store, partition, db_id, key, digest, object};
    auto header = co_await cache.Required(std::string_view("\0", 1));
    if (!header.ok()) co_return header.status();
    auto payload = StreamRecordPayload(*header);
    const auto length = object->ordered_directory().root().logical_size();
    if (!payload.ok() || payload->size() != 48 || Count(*payload, 44) != length)
      co_return absl::DataLossError("invalid Stream metadata header");
    std::string partial(*payload);
    SetCount(partial, 44, length != 0);
    if (length != 0) {
      auto last = co_await cache.Select(std::string_view("\1", 1),
                                        std::string_view("\2", 1), length - 1);
      if (!last.ok()) co_return last.status();
      if (!*last) co_return absl::DataLossError("missing Stream last message");
      auto value = StreamRecordPayload(**last);
      if (!value.ok() || value->size() < 20)
        co_return absl::DataLossError("invalid Stream last message");
      partial.append(value->substr(0, 16));
      partial.append(4, '\0');
    }
    const auto at = partial.size();
    partial.resize(at + (length == 0 ? 8 : 12), '\0');
    if (length != 0) {
      SetCount(partial, at, 1);
      SetCount(partial, at + 4, 1);
    }
    auto update = callback(
        CompactValueView{.encoded_ = partial,
                         .logical_size_ = length != 0,
                         .expire_at_ms_ = object->version().root_.expire_at_ms_,
                         .stream_length_ = length});
    if (!update.ok()) co_return update.status();
    if (!update->changed_) co_return absl::OkStatus();
    if (update->erase_ || update->reuse_encoded_ ||
        update->encoded_.size() != partial.size() ||
        update->encoded_.substr(44) != partial.substr(44))
      co_return absl::InvalidArgumentError(
          "Stream metadata callback changed entries");
    std::string bytes = update->encoded_.substr(0, 48);
    SetCount(bytes, 44, length);
    std::vector<StreamRecordChange> changes;
    changes.push_back({.key_ = std::string("\0", 1),
                       .record_ = Record(std::string_view("\0", 1), bytes)});
    auto plan = co_await cache.Plan(std::move(changes), length);
    if (!plan.ok()) co_return plan.status();
    co_return co_await CommitGroupedOrderedMutationLocked(
        store, partition, db_id, key, digest, object, std::move(*plan),
        object->version().root_.expire_at_ms_, tx, replication,
        mutation_precondition);
  } catch (const std::bad_alloc&) {
    RecordMemoryRejection();
    co_return absl::ResourceExhaustedError("OOM grouped Stream metadata");
  }
}

Task<absl::Status> StorageEngine::Impl::ExecuteGroupedStreamInspect(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    GroupedHashObject::Handle object, const CompactValueCallback& callback,
    const StreamInspectAccess& access) {
  try {
    using Kind = StreamInspectAccess::Kind;
    StreamPageAccess cache{*this, store, partition, db_id, key, digest, object};
    StreamInspection inspection;
    const auto length = object->ordered_directory().root().logical_size();
    auto retain = [&](std::size_t bytes) -> absl::Status {
      GroupedScratchBudget budget;
      auto status = budget.AddBytes(bytes + 128);
      if (!status.ok()) return status;
      auto reservation = budget.Reserve(6);
      if (!reservation.ok()) return reservation.status();
      cache.reservations_.push_back(std::move(*reservation));
      return absl::OkStatus();
    };
    auto header = co_await cache.Required(std::string_view("\0", 1));
    if (!header.ok()) co_return header.status();
    auto payload = StreamRecordPayload(*header);
    if (!payload.ok() || payload->size() != 48 || Count(*payload, 44) != length)
      co_return absl::DataLossError("invalid Stream inspection header");
    std::string partial(*payload);
    auto nodes = co_await cache.Required(std::string_view("\2", 1));
    auto groups = co_await cache.Required(std::string_view("\4", 1));
    if (!nodes.ok()) co_return nodes.status();
    if (!groups.ok()) co_return groups.status();
    auto node_count = StreamRecordPayload(*nodes),
         group_count = StreamRecordPayload(*groups);
    if (!node_count.ok() || !group_count.ok() || node_count->size() != 4 ||
        group_count->size() != 4)
      co_return absl::DataLossError("invalid Stream inspection counts");
    inspection.nodes_ = Count(*node_count);
    inspection.groups_ = Count(*group_count);
    std::optional<std::array<std::uint64_t, 2>> first_id;
    std::vector<std::string> entries;
    if (length != 0) {
      auto first = co_await cache.Select(std::string_view("\1", 1),
                                         std::string_view("\2", 1), 0);
      if (!first.ok()) co_return first.status();
      if (!*first) co_return absl::DataLossError("missing Stream first entry");
      auto value = StreamRecordPayload(**first);
      if (!value.ok() || value->size() < 20)
        co_return absl::DataLossError("invalid Stream first entry");
      first_id = ReadId(*value);
      if (access.kind_ == Kind::kStream) {
        entries.push_back(std::move(**first));
        if (length != 1) {
          auto last = co_await cache.Select(
              std::string_view("\1", 1), std::string_view("\2", 1), length - 1);
          if (!last.ok()) co_return last.status();
          if (!*last)
            co_return absl::DataLossError("missing Stream last entry");
          entries.push_back(std::move(**last));
        }
      } else if (access.kind_ == Kind::kFull) {
        // Message output uses its own pinned cursor. Inspection needs only the
        // first ID for header/lag semantics, even for FULL COUNT 0.
        entries.push_back(std::move(**first));
      }
    }
    SetCount(partial, 44, entries.size());
    for (const auto& entry : entries) {
      auto value = StreamRecordPayload(entry);
      if (!value.ok()) co_return value.status();
      partial.append(*value);
    }
    auto at = partial.size();
    partial.resize(at + (entries.empty() ? 8 : 12), '\0');
    if (!entries.empty()) {
      SetCount(partial, at, 1);
      SetCount(partial, at + 4, entries.size());
    }
    const auto group_count_offset = partial.size() - 4;
    const bool single =
        access.kind_ == Kind::kConsumers || access.kind_ == Kind::kPending;
    std::string cursor =
        single ? GroupPrefix(access.group_) : std::string(1, '\5');
    while (access.kind_ != Kind::kStream) {
      std::optional<std::string> selected;
      if (single) {
        auto found = co_await cache.Find(cursor + '\0');
        if (!found.ok()) co_return found.status();
        selected = std::move(*found);
      } else {
        auto found =
            co_await cache.Select(cursor, std::string_view("\6", 1), 0);
        if (!found.ok()) co_return found.status();
        selected = std::move(*found);
      }
      if (!selected) break;
      auto value = StreamGroupHeaderPayload(*selected);
      if (!value.ok())
        co_return absl::DataLossError("invalid Stream inspection group");
      std::string name(value->substr(4, Count(*value)));
      const auto prefix = GroupPrefix(name);
      std::string end(prefix);
      end.back() = '\1';
      auto status = retain(value->size());
      if (!status.ok()) co_return status;
      std::string group_header(*value);
      StreamGroupSummary summary{
          .name_ = name, .consumers_ = Count(*value, value->size() - 4)};
      auto count = co_await cache.Required(prefix + '\2');
      if (!count.ok()) co_return count.status();
      auto count_value = StreamRecordPayload(*count);
      if (!count_value.ok() || count_value->size() != 4)
        co_return absl::DataLossError("invalid Stream inspection PEL count");
      summary.pending_ = Count(*count_value);
      std::vector<std::string> consumers, pending;
      if (access.kind_ == Kind::kConsumers || access.kind_ == Kind::kFull) {
        status =
            co_await cache.Visit(prefix + '\1', prefix + '\2',
                                 [&](std::string_view record) -> absl::Status {
                                   auto value = StreamRecordPayload(record);
                                   if (!value.ok()) return value.status();
                                   auto admitted = retain(value->size());
                                   if (!admitted.ok()) return admitted;
                                   consumers.emplace_back(*value);
                                   return absl::OkStatus();
                                 });
        if (!status.ok()) co_return status;
        if (consumers.size() != summary.consumers_)
          co_return absl::DataLossError(
              "Stream inspection consumer count mismatch");
      }
      if (access.kind_ != Kind::kGroups) {
        std::uint64_t seen = 0;
        status = co_await cache.Visit(
            prefix + '\3', end, [&](std::string_view record) -> absl::Status {
              auto value = StreamRecordPayload(record);
              if (!value.ok() || value->size() < 36 ||
                  Count(*value, 16) != value->size() - 36)
                return absl::DataLossError(
                    "invalid Stream inspection pending row");
              std::string owner(value->substr(20, Count(*value, 16)));
              if (!summary.consumer_pending_.contains(owner)) {
                auto admitted = retain(owner.size());
                if (!admitted.ok()) return admitted;
              }
              auto& owner_count = summary.consumer_pending_[owner];
              ++owner_count;
              ++seen;
              const auto id = ReadId(*value);
              if (!summary.first_pending_) summary.first_pending_ = id;
              summary.last_pending_ = id;
              if (access.kind_ == Kind::kFull &&
                  (access.count_ == 0 || seen <= access.count_ ||
                   owner_count <= access.count_)) {
                auto admitted = retain(value->size());
                if (!admitted.ok()) return admitted;
                pending.emplace_back(*value);
              }
              return absl::OkStatus();
            });
        if (!status.ok()) co_return status;
        if (seen != summary.pending_)
          co_return absl::DataLossError(
              "Stream inspection pending count mismatch");
      }
      SetCount(group_header, group_header.size() - 4, consumers.size());
      partial.append(group_header);
      for (const auto& consumer : consumers) partial.append(consumer);
      at = partial.size();
      partial.resize(at + 4);
      SetCount(partial, at, pending.size());
      for (const auto& row : pending) partial.append(row);
      inspection.summaries_.push_back(std::move(summary));
      if (single) break;
      cursor = std::move(end);
      co_await bycorf::Yield(*store.worker_);
    }
    SetCount(partial, group_count_offset, inspection.summaries_.size());
    auto update = callback(
        CompactValueView{.encoded_ = partial,
                         .logical_size_ = entries.size(),
                         .expire_at_ms_ = object->version().root_.expire_at_ms_,
                         .stream_length_ = length,
                         .stream_first_id_ = first_id,
                         .stream_inspection_ = &inspection});
    if (!update.ok()) co_return update.status();
    if (update->changed_)
      co_return absl::InvalidArgumentError(
          "Stream inspection callback mutated");
    co_return absl::OkStatus();
  } catch (const std::bad_alloc&) {
    RecordMemoryRejection();
    co_return absl::ResourceExhaustedError("OOM grouped Stream inspection");
  }
}

Task<absl::Status> StorageEngine::Impl::ExecuteGroupedStreamTrimLocked(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    GroupedHashObject::Handle object, const CompactValueCallback& callback,
    TxShardWrites* tx, ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  try {
    StreamPageAccess cache{*this, store, partition, db_id, key, digest, object};
    auto header = co_await cache.Required(std::string_view("\0", 1));
    if (!header.ok()) co_return header.status();
    auto payload = StreamRecordPayload(*header);
    const auto length = object->ordered_directory().root().logical_size();
    if (!payload.ok() || payload->size() != 48 || Count(*payload, 44) != length)
      co_return absl::DataLossError("invalid Stream trim header");
    std::string partial(*payload);
    SetCount(partial, 44, 0);
    partial.append(8, '\0');
    auto update = callback(
        CompactValueView{.encoded_ = partial,
                         .logical_size_ = 0,
                         .expire_at_ms_ = object->version().root_.expire_at_ms_,
                         .stream_incremental_trim_ = true});
    if (!update.ok()) co_return update.status();
    if (!update->changed_) co_return absl::OkStatus();
    if (!update->stream_trim_ || update->erase_ || update->reuse_encoded_ ||
        update->encoded_ != partial)
      co_return absl::InvalidArgumentError("invalid incremental Stream trim");
    std::vector<StreamRecordChange> changes;
    std::vector<std::uint64_t> retired;
    auto result =
        co_await cache.Trim(*update->stream_trim_, changes, retired, length);
    if (!result.ok()) co_return result.status();
    if (update->stream_trim_complete_) update->stream_trim_complete_(*result);
    if (result->removed_ == 0) co_return absl::OkStatus();
    auto plan = co_await cache.Plan(std::move(changes), result->length_,
                                    std::move(retired));
    if (!plan.ok()) co_return plan.status();
    co_return co_await CommitGroupedOrderedMutationLocked(
        store, partition, db_id, key, digest, object, std::move(*plan),
        object->version().root_.expire_at_ms_, tx, replication,
        mutation_precondition);
  } catch (const std::bad_alloc&) {
    RecordMemoryRejection();
    co_return absl::ResourceExhaustedError("OOM grouped Stream trim");
  }
}

Task<absl::Status> StorageEngine::Impl::ExecuteGroupedStreamDeleteLocked(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    GroupedHashObject::Handle object, const CompactValueCallback& callback,
    const StreamDeleteAccess& access, TxShardWrites* tx,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  try {
    StreamPageAccess cache{*this, store, partition, db_id, key, digest, object};
    auto header = co_await cache.Required(std::string_view("\0", 1));
    if (!header.ok()) co_return header.status();
    auto payload = StreamRecordPayload(*header);
    const auto length = object->ordered_directory().root().logical_size();
    if (!payload.ok() || payload->size() != 48 || Count(*payload, 44) != length)
      co_return absl::DataLossError("invalid Stream delete header");
    std::string partial(*payload);
    std::set<std::array<std::uint64_t, 2>> ids(access.ids_.begin(),
                                               access.ids_.end());
    std::set<std::string> deleted;
    std::map<std::string, std::pair<std::uint32_t, std::uint32_t>> nodes;
    std::vector<StreamRecordChange> changes;
    for (const auto& id : ids) {
      const auto entry_key = IdKey(std::string_view("\1", 1), id);
      auto entry = co_await cache.Find(entry_key);
      if (!entry.ok()) co_return entry.status();
      if (!*entry) continue;
      auto value = StreamRecordPayload(**entry);
      if (!value.ok() || value->size() < 20)
        co_return absl::DataLossError("invalid Stream delete entry");
      // XDEL only needs identity, not field payloads. The synthetic view is
      // delete-only and is checked below before any page can be published.
      partial.append(value->substr(0, 16));
      partial.append(4, '\0');
      deleted.insert(entry_key);
      changes.push_back({.key_ = entry_key, .record_ = std::nullopt});
      auto node = co_await cache.Previous(IdKey(std::string_view("\3", 1), id));
      if (!node.ok()) co_return node.status();
      if (!*node || (**node).front() != '\3')
        co_return absl::DataLossError("missing Stream delete node");
      auto node_key = StreamRecordKey(**node);
      auto node_value = StreamRecordPayload(**node);
      if (!node_key.ok() || !node_value.ok() || node_value->size() != 4)
        co_return absl::DataLossError("invalid Stream delete node");
      auto& counts = nodes[std::string(*node_key)];
      counts.first = Count(*node_value);
      ++counts.second;
    }
    SetCount(partial, 44, deleted.size());
    auto at = partial.size();
    partial.resize(at + (deleted.empty() ? 8 : 12), '\0');
    if (!deleted.empty()) {
      SetCount(partial, at, 1);
      SetCount(partial, at + 4, deleted.size());
    }
    auto update = callback(CompactValueView{
        .encoded_ = partial,
        .logical_size_ = deleted.size(),
        .expire_at_ms_ = object->version().root_.expire_at_ms_});
    if (!update.ok()) co_return update.status();
    if (!update->changed_) co_return absl::OkStatus();
    if (deleted.empty() || update->erase_ || update->reuse_encoded_ ||
        update->logical_size_ != 0 || update->encoded_.size() != 56 ||
        update->encoded_.substr(0, 20) != partial.substr(0, 20) ||
        update->encoded_.substr(36, 8) != partial.substr(36, 8) ||
        update->encoded_.substr(44) != std::string(12, '\0'))
      co_return absl::InvalidArgumentError("invalid partial Stream deletion");
    auto node_count = co_await cache.Required(std::string_view("\2", 1));
    if (!node_count.ok()) co_return node_count.status();
    auto count_payload = StreamRecordPayload(*node_count);
    if (!count_payload.ok() || count_payload->size() != 4)
      co_return absl::DataLossError("invalid Stream node count");
    auto remaining_nodes = Count(*count_payload);
    for (const auto& [node_key, counts] : nodes) {
      const auto [live, removed] = counts;
      if (removed > live || remaining_nodes == 0)
        co_return absl::DataLossError("Stream delete node count mismatch");
      std::string next_key = node_key;
      if (removed == live) {
        --remaining_nodes;
        changes.push_back({.key_ = node_key, .record_ = std::nullopt});
        continue;
      }
      std::string first_key = node_key;
      first_key[0] = '\1';
      if (deleted.contains(first_key)) {
        auto next = co_await cache.Scan(first_key, std::string_view("\2", 1),
                                        deleted.size() + 1, true);
        if (!next.ok()) co_return next.status();
        bool found = false;
        for (const auto& record : *next) {
          auto candidate = StreamRecordKey(record);
          if (!candidate.ok()) co_return candidate.status();
          if (deleted.contains(std::string(*candidate))) continue;
          next_key = *candidate;
          next_key[0] = '\3';
          found = true;
          break;
        }
        if (!found)
          co_return absl::DataLossError("missing surviving Stream ID");
        changes.push_back({.key_ = node_key, .record_ = std::nullopt});
      }
      std::string count(4, '\0');
      SetCount(count, 0, live - removed);
      changes.push_back({.key_ = next_key, .record_ = Record(next_key, count)});
    }
    std::string count(4, '\0');
    SetCount(count, 0, remaining_nodes);
    changes.push_back({.key_ = std::string("\2", 1),
                       .record_ = Record(std::string_view("\2", 1), count)});
    std::string new_header = update->encoded_.substr(0, 48);
    SetCount(new_header, 44, length - deleted.size());
    changes.push_back(
        {.key_ = std::string("\0", 1),
         .record_ = Record(std::string_view("\0", 1), new_header)});
    auto plan =
        co_await cache.Plan(std::move(changes), length - deleted.size());
    if (!plan.ok()) co_return plan.status();
    co_return co_await CommitGroupedOrderedMutationLocked(
        store, partition, db_id, key, digest, object, std::move(*plan),
        object->version().root_.expire_at_ms_, tx, replication,
        mutation_precondition);
  } catch (const std::bad_alloc&) {
    RecordMemoryRejection();
    co_return absl::ResourceExhaustedError("OOM grouped Stream deletion");
  }
}

Task<absl::Status> StorageEngine::Impl::ExecuteGroupedStreamAckLocked(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    GroupedHashObject::Handle object, const CompactValueCallback& callback,
    const StreamAckAccess& access, TxShardWrites* tx,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  try {
    if (!object->is_ordered() || object->ordered_directory().root().kind_ !=
                                     OrderedCollectionKind::kStream)
      co_return absl::DataLossError("invalid Stream ACK view");
    StreamPageAccess cache{*this, store, partition, db_id, key, digest, object};
    auto header = co_await cache.Required(std::string_view("\0", 1));
    if (!header.ok()) co_return header.status();
    auto payload = StreamRecordPayload(*header);
    if (!payload.ok() || payload->size() != 48)
      co_return absl::DataLossError("invalid Stream ACK header");
    std::string prefix(1, '\5');
    for (char ch : access.group_) {
      prefix.push_back(ch);
      if (ch == '\0') prefix.push_back('\xff');
    }
    prefix.append("\0\0", 2);
    auto group = co_await cache.Find(prefix + '\0');
    if (!group.ok()) co_return group.status();
    std::string partial(*payload);
    SetCount(partial, 44, 0);
    partial.append(8, '\0');
    std::uint32_t total_pending = 0;
    std::vector<std::string> pending;
    std::vector<StreamRecordChange> changes;
    if (*group) {
      SetCount(partial, 52, 1);
      auto group_payload = StreamRecordPayload(**group);
      if (!group_payload.ok() || group_payload->size() < 32)
        co_return absl::DataLossError("invalid Stream ACK group");
      std::string group_header(*group_payload);
      SetCount(group_header, group_header.size() - 4, 0);
      partial.append(group_header);
      auto count_record = co_await cache.Required(prefix + '\2');
      if (!count_record.ok()) co_return count_record.status();
      auto count_payload = StreamRecordPayload(*count_record);
      if (!count_payload.ok() || count_payload->size() != 4)
        co_return absl::DataLossError("invalid Stream ACK pending count");
      total_pending = Count(*count_payload);
      std::set<std::array<std::uint64_t, 2>> ids(access.ids_.begin(),
                                                 access.ids_.end());
      for (const auto& id : ids) {
        std::string pending_key = prefix + '\3';
        for (auto half : id)
          for (unsigned i = 8; i != 0; --i)
            pending_key.push_back(half >> ((i - 1) * 8));
        auto record = co_await cache.Find(pending_key);
        if (!record.ok()) co_return record.status();
        if (!*record) continue;
        auto item = StreamRecordPayload(**record);
        if (!item.ok()) co_return item.status();
        pending.emplace_back(*item);
        changes.push_back(
            {.key_ = std::move(pending_key), .record_ = std::nullopt});
      }
      if (pending.size() > total_pending)
        co_return absl::DataLossError("Stream PEL count mismatch");
      partial.append(4, '\0');
    }
    const std::string empty = partial;
    if (*group) {
      SetCount(partial, partial.size() - 4, pending.size());
      for (const auto& item : pending) partial.append(item);
    }
    auto update = callback(CompactValueView{
        .encoded_ = partial,
        .logical_size_ = 0,
        .expire_at_ms_ = object->version().root_.expire_at_ms_});
    if (!update.ok()) co_return update.status();
    if (!update->changed_) co_return absl::OkStatus();
    // The ACK contract can only remove the requested, existing PEL entries.
    // An unexpected callback after-image must never erase unloaded group state.
    if (changes.empty() || update->encoded_ != empty || update->erase_ ||
        update->reuse_encoded_)
      co_return absl::InvalidArgumentError("invalid sparse Stream ACK result");
    std::string count_bytes(4, '\0');
    SetCount(count_bytes, 0, total_pending - pending.size());
    changes.push_back(
        {.key_ = prefix + '\2', .record_ = Record(prefix + '\2', count_bytes)});
    auto plan = co_await cache.Plan(
        std::move(changes), object->ordered_directory().root().logical_size());
    if (!plan.ok()) co_return plan.status();
    co_return co_await CommitGroupedOrderedMutationLocked(
        store, partition, db_id, key, digest, object, std::move(*plan),
        object->version().root_.expire_at_ms_, tx, replication,
        mutation_precondition);
  } catch (const std::bad_alloc&) {
    RecordMemoryRejection();
    co_return absl::ResourceExhaustedError("OOM grouped Stream workspace");
  }
}

Task<absl::Status> StorageEngine::Impl::ExecuteGroupedStreamGroupLocked(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    GroupedHashObject::Handle object, const CompactValueCallback& callback,
    const StreamGroupAccess& access, bool read_only, TxShardWrites* tx,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  try {
    if (!object->is_ordered() || object->ordered_directory().root().kind_ !=
                                     OrderedCollectionKind::kStream)
      co_return absl::DataLossError("invalid partial Stream group view");
    StreamPageAccess cache{*this, store, partition, db_id, key, digest, object};
    auto header = co_await cache.Required(std::string_view("\0", 1));
    if (!header.ok()) co_return header.status();
    auto header_payload = StreamRecordPayload(*header);
    if (!header_payload.ok() || header_payload->size() != 48)
      co_return absl::DataLossError("invalid partial Stream header");
    const auto prefix = GroupPrefix(access.group_);
    auto group = co_await cache.Find(prefix + '\0');
    if (!group.ok()) co_return group.status();
    std::uint32_t total_consumers = 0, total_pending = 0;
    std::string original_group, original_count;
    std::vector<std::string> entries, consumers, pending;
    std::set<std::string> allowed;
    std::set<std::array<std::uint64_t, 2>> pending_ids(
        access.pending_ids_.begin(), access.pending_ids_.end());
    if (*group) {
      original_group = **group;
      auto payload = StreamGroupHeaderPayload(original_group);
      if (!payload.ok() || Count(*payload) != access.group_.size())
        co_return absl::DataLossError("invalid partial Stream group header");
      total_consumers = Count(*payload, payload->size() - 4);
      const auto last_id =
          ReadId(payload->substr(4 + access.group_.size(), 16));
      if (access.read_new_count_) {
        if (access.entry_ids_only_) {
          auto status = co_await cache.Visit(
              IdKey(std::string_view("\1", 1), last_id),
              std::string_view("\2", 1),
              [&](std::string_view record) -> absl::Status {
                auto payload = StreamRecordPayload(record);
                if (!payload.ok() || payload->size() < 20)
                  return absl::DataLossError("invalid delivery message");
                const auto id = ReadId(*payload);
                if (id <= last_id) return absl::OkStatus();
                if (entries.size() == *access.read_new_count_)
                  return absl::OutOfRangeError("delivery window complete");
                auto charge = TryReserveMemory(512);
                if (!charge)
                  return absl::ResourceExhaustedError(
                      "OOM Stream delivery IDs");
                cache.reservations_.push_back(std::move(*charge));
                std::string value(payload->substr(0, 16));
                value.append(12, '\0');
                SetCount(value, 16, 2);  // Valid placeholder field/value pair;
                                         // fields are immutable.
                entries.push_back(
                    Record(IdKey(std::string_view("\1", 1), id), value));
                return absl::OkStatus();
              });
          if (!status.ok() && !absl::IsOutOfRange(status)) co_return status;
        } else {
          auto selected = co_await cache.Scan(
              IdKey(std::string_view("\1", 1), last_id),
              std::string_view("\2", 1), *access.read_new_count_, true);
          if (!selected.ok()) co_return selected.status();
          entries = std::move(*selected);
        }
        for (const auto& entry : entries) {
          auto value = StreamRecordPayload(entry);
          if (!value.ok() || value->size() < 20)
            co_return absl::DataLossError("invalid partial Stream entry");
          pending_ids.insert(ReadId(*value));
        }
      } else {
        std::set<std::array<std::uint64_t, 2>> ids(access.entry_ids_.begin(),
                                                   access.entry_ids_.end());
        for (const auto& id : ids) {
          auto entry =
              co_await cache.Find(IdKey(std::string_view("\1", 1), id));
          if (!entry.ok()) co_return entry.status();
          if (*entry) entries.push_back(std::move(**entry));
        }
      }
      std::set<std::string> names(access.consumers_.begin(),
                                  access.consumers_.end());
      for (const auto& name : names) {
        const auto consumer_key = ConsumerKey(prefix, name);
        allowed.insert(consumer_key);
        auto consumer = co_await cache.Find(consumer_key);
        if (!consumer.ok()) co_return consumer.status();
        if (*consumer) consumers.push_back(std::move(**consumer));
      }
      auto count = co_await cache.Required(prefix + '\2');
      if (!count.ok()) co_return count.status();
      original_count = std::move(*count);
      auto count_payload = StreamRecordPayload(original_count);
      if (!count_payload.ok() || count_payload->size() != 4)
        co_return absl::DataLossError("invalid partial Stream PEL count");
      total_pending = Count(*count_payload);
      if (access.pending_scan_) {
        auto selected =
            co_await cache.ScanPending(prefix, *access.pending_scan_);
        if (!selected.ok()) co_return selected.status();
        for (const auto& row : *selected) {
          auto payload = StreamRecordPayload(row);
          if (!payload.ok()) co_return payload.status();
          const auto id = ReadId(*payload);
          pending_ids.insert(id);
          if (access.pending_scan_->load_entries_) {
            auto entry =
                co_await cache.Find(IdKey(std::string_view("\1", 1), id));
            if (!entry.ok()) co_return entry.status();
            if (*entry) {
              if (access.entry_ids_only_) {
                auto payload = StreamRecordPayload(**entry);
                if (!payload.ok() || payload->size() < 20)
                  co_return absl::DataLossError("invalid history entry");
                auto charge = TryReserveMemory(512);
                if (!charge)
                  co_return absl::ResourceExhaustedError(
                      "OOM Stream history IDs");
                cache.reservations_.push_back(std::move(*charge));
                std::string value(payload->substr(0, 16));
                value.append(12, '\0');
                SetCount(value, 16, 2);  // Valid placeholder field/value pair;
                                         // fields are immutable.
                entries.push_back(
                    Record(IdKey(std::string_view("\1", 1), id), value));
                // ID probes must not accumulate the immutable message bodies
                // while planning mutations to a separate PEL key range.
                for (auto page = cache.pages_.begin();
                     page != cache.pages_.end();) {
                  const auto& rows = page->second.snapshot_.entries_;
                  auto first = StreamRecordKey(rows.front().value_),
                       last = StreamRecordKey(rows.back().value_);
                  if (first.ok() && last.ok() && first->front() == '\1' &&
                      last->front() == '\1') {
                    cache.page_reservations_.erase(page->first);
                    page = cache.pages_.erase(page);
                  } else
                    ++page;
                }
              } else
                entries.push_back(std::move(**entry));
            }
          }
        }
      }
      for (const auto& id : pending_ids) {
        const auto pending_key = IdKey(prefix + '\3', id);
        allowed.insert(pending_key);
        auto item = co_await cache.Find(pending_key);
        if (!item.ok()) co_return item.status();
        if (*item) pending.push_back(std::move(**item));
      }
    }
    if (consumers.size() > total_consumers || pending.size() > total_pending)
      co_return absl::DataLossError("partial Stream group count mismatch");
    // New PEL rows repeat their owner name and group routing prefix. Charge
    // those new bytes before the callback can allocate them; loading existing
    // pages alone does not cover a new long consumer or previously absent IDs.
    GroupedScratchBudget incoming_budget;
    std::size_t names = access.group_.size();
    for (const auto& consumer : access.consumers_) {
      if (consumer.size() > SIZE_MAX - names)
        co_return absl::ResourceExhaustedError("Stream consumer size overflow");
      names += consumer.size();
    }
    const auto copies =
        entries.size() + pending_ids.size() + access.consumers_.size() + 4;
    if (names != 0 && copies > SIZE_MAX / names)
      co_return absl::ResourceExhaustedError("Stream PEL owner size overflow");
    auto incoming_size = incoming_budget.AddBytes(names * copies);
    if (!incoming_size.ok()) co_return incoming_size;
    auto incoming_charge = incoming_budget.Reserve(8);
    if (!incoming_charge.ok()) co_return incoming_charge.status();
    std::string partial(*header_payload);
    SetCount(partial, 44, entries.size());
    for (const auto& entry : entries) {
      auto payload = StreamRecordPayload(entry);
      if (!payload.ok()) co_return payload.status();
      partial.append(*payload);
    }
    auto at = partial.size();
    partial.resize(at + (entries.empty() ? 8 : 12), '\0');
    if (!entries.empty()) {
      SetCount(partial, at, 1);
      SetCount(partial, at + 4, entries.size());
    }
    if (*group) {
      SetCount(partial, partial.size() - 4, 1);
      auto payload = StreamRecordPayload(original_group);
      if (!payload.ok()) co_return payload.status();
      std::string group_header(*payload);
      SetCount(group_header, group_header.size() - 4, consumers.size());
      partial.append(group_header);
      for (const auto& consumer : consumers) {
        auto value = StreamRecordPayload(consumer);
        if (!value.ok()) co_return value.status();
        partial.append(*value);
      }
      at = partial.size();
      partial.resize(at + 4);
      SetCount(partial, at, pending.size());
      for (const auto& item : pending) {
        auto value = StreamRecordPayload(item);
        if (!value.ok()) co_return value.status();
        partial.append(*value);
      }
    }
    std::optional<std::array<std::uint64_t, 2>> first_id;
    if (object->ordered_directory().root().logical_size() != 0) {
      auto first = co_await cache.Scan(std::string_view("\1", 1),
                                       std::string_view("\2", 1), 1);
      if (!first.ok()) co_return first.status();
      if (first->empty())
        co_return absl::DataLossError("missing first Stream entry");
      auto payload = StreamRecordPayload(first->front());
      if (!payload.ok() || payload->size() < 20)
        co_return absl::DataLossError("invalid first Stream entry");
      first_id = ReadId(*payload);
    }
    auto update = callback(CompactValueView{
        .encoded_ = partial,
        .logical_size_ = entries.size(),
        .expire_at_ms_ = object->version().root_.expire_at_ms_,
        .stream_length_ = object->ordered_directory().root().logical_size(),
        .stream_first_id_ = first_id});
    if (!update.ok()) co_return update.status();
    if (!update->changed_) co_return absl::OkStatus();
    if (read_only || update->erase_ || update->reuse_encoded_ ||
        update->logical_size_ != entries.size())
      co_return absl::InvalidArgumentError(
          "partial Stream group changed key/entries");
    if (access.destroy_) {
      std::string empty(*header_payload);
      SetCount(empty, 44, 0);
      empty.append(8, '\0');
      if (!entries.empty() || update->encoded_ != empty)
        co_return absl::InvalidArgumentError(
            "invalid Stream group destruction");
      if (!*group) co_return absl::OkStatus();
      std::vector<StreamRecordChange> changes;
      std::vector<std::uint64_t> retired;
      std::string end(prefix);
      end.back() = '\1';
      auto status = co_await cache.EraseRange(prefix, end, changes, retired);
      if (!status.ok()) co_return status;
      auto count_record = co_await cache.Required(std::string_view("\4", 1));
      if (!count_record.ok()) co_return count_record.status();
      auto payload = StreamRecordPayload(*count_record);
      if (!payload.ok() || payload->size() != 4 || Count(*payload) == 0)
        co_return absl::DataLossError("invalid Stream group count");
      std::string count(4, '\0');
      SetCount(count, 0, Count(*payload) - 1);
      changes.push_back({.key_ = std::string("\4", 1),
                         .record_ = Record(std::string_view("\4", 1), count)});
      auto plan = co_await cache.Plan(
          std::move(changes), object->ordered_directory().root().logical_size(),
          std::move(retired));
      if (!plan.ok()) co_return plan.status();
      co_return co_await CommitGroupedOrderedMutationLocked(
          store, partition, db_id, key, digest, object, std::move(*plan),
          object->version().root_.expire_at_ms_, tx, replication,
          mutation_precondition);
    }
    if (!*group && !access.create_)
      co_return absl::InvalidArgumentError("partial Stream group was created");
    auto before = DecodeStreamRecords(partial, entries.size());
    auto after = DecodeStreamRecords(update->encoded_, entries.size());
    if (!before.ok()) co_return before.status();
    if (!after.ok()) co_return after.status();
    std::map<std::string, std::string> old_records, new_records;
    for (auto& item : *before) {
      auto key = StreamRecordKey(item.value_);
      if (!key.ok()) co_return key.status();
      old_records.emplace(std::string(*key), std::move(item.value_));
    }
    for (auto& item : *after) {
      auto key = StreamRecordKey(item.value_);
      if (!key.ok()) co_return key.status();
      new_records.emplace(std::string(*key), std::move(item.value_));
    }
    const auto group_key = prefix + '\0', count_key = prefix + '\2';
    allowed.insert(group_key);
    allowed.insert(count_key);
    if (!new_records.contains(group_key) || !new_records.contains(count_key))
      co_return absl::InvalidArgumentError("partial Stream group was deleted");
    auto new_group = StreamRecordPayload(new_records.at(group_key));
    auto new_count = StreamRecordPayload(new_records.at(count_key));
    if (!new_group.ok() || !new_count.ok() || new_count->size() != 4 ||
        new_group->size() < 32)
      co_return absl::InvalidArgumentError(
          "invalid partial Stream group result");
    const auto new_consumers =
        std::uint64_t(total_consumers - consumers.size()) +
        Count(*new_group, new_group->size() - 4);
    const auto new_pending =
        std::uint64_t(total_pending - pending.size()) + Count(*new_count);
    if (new_consumers > UINT32_MAX || new_pending > UINT32_MAX)
      co_return absl::OutOfRangeError("Stream group count overflow");
    std::string group_payload(*new_group), count_payload(*new_count);
    SetCount(group_payload, group_payload.size() - 4, new_consumers);
    SetCount(count_payload, 0, new_pending);
    if (*group) {
      old_records[group_key] = std::move(original_group);
      old_records[count_key] = std::move(original_count);
    } else {
      allowed.insert(std::string("\4", 1));
      for (const auto& name : access.consumers_)
        allowed.insert(ConsumerKey(prefix, name));
      for (const auto& id : access.pending_ids_)
        allowed.insert(IdKey(prefix + '\3', id));
      auto count_record = co_await cache.Required(std::string_view("\4", 1));
      if (!count_record.ok()) co_return count_record.status();
      auto payload = StreamRecordPayload(*count_record);
      if (!payload.ok() || payload->size() != 4 ||
          Count(*payload) == UINT32_MAX)
        co_return absl::DataLossError("invalid Stream group count");
      std::string count(4, '\0');
      SetCount(count, 0, Count(*payload) + 1);
      old_records[std::string("\4", 1)] = std::move(*count_record);
      new_records[std::string("\4", 1)] =
          Record(std::string_view("\4", 1), count);
    }
    new_records[group_key] = Record(group_key, group_payload);
    new_records[count_key] = Record(count_key, count_payload);
    std::vector<StreamRecordChange> changes;
    for (auto& [record_key, value] : old_records) {
      const auto found = new_records.find(record_key);
      if (found != new_records.end() && found->second == value) continue;
      if (!allowed.contains(record_key))
        co_return absl::InvalidArgumentError(
            "partial Stream group changed unloaded state");
      if (found == new_records.end())
        changes.push_back({.key_ = record_key, .record_ = std::nullopt});
    }
    for (auto& [record_key, value] : new_records) {
      const auto found = old_records.find(record_key);
      if (found != old_records.end() && found->second == value) continue;
      if (!allowed.contains(record_key))
        co_return absl::InvalidArgumentError(
            "partial Stream group inserted unloaded state");
      changes.push_back({.key_ = record_key, .record_ = std::move(value)});
    }
    std::vector<std::uint64_t> retired;
    if (access.remove_consumer_) {
      const auto consumer_key = ConsumerKey(prefix, *access.remove_consumer_);
      if (new_records.contains(consumer_key) ||
          !old_records.contains(consumer_key))
        co_return absl::InvalidArgumentError("invalid Stream consumer removal");
      auto removed = co_await cache.ErasePendingConsumer(
          prefix, *access.remove_consumer_, changes, retired);
      if (!removed.ok()) co_return removed.status();
      if (*removed > total_pending)
        co_return absl::DataLossError("Stream consumer removal count mismatch");
      std::string count(4, '\0');
      SetCount(count, 0, total_pending - *removed);
      StreamPageAccess::Change(changes, count_key, Record(count_key, count));
      if (update->stream_pending_removed_)
        update->stream_pending_removed_(*removed);
    }
    auto plan = co_await cache.Plan(
        std::move(changes), object->ordered_directory().root().logical_size(),
        std::move(retired));
    if (!plan.ok()) co_return plan.status();
    co_return co_await CommitGroupedOrderedMutationLocked(
        store, partition, db_id, key, digest, object, std::move(*plan),
        object->version().root_.expire_at_ms_, tx, replication,
        mutation_precondition);
  } catch (const std::bad_alloc&) {
    RecordMemoryRejection();
    co_return absl::ResourceExhaustedError("OOM grouped Stream workspace");
  }
}

Task<absl::Status> StorageEngine::Impl::ExecuteGroupedStreamRange(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    GroupedHashObject::Handle object, const CompactValueCallback& callback,
    const StreamRangeAccess& range) {
  try {
    if (!object->is_ordered() || object->ordered_directory().root().kind_ !=
                                     OrderedCollectionKind::kStream)
      co_return absl::DataLossError("invalid grouped Stream range view");
    const auto& groups = object->ordered_directory().groups();
    auto make_key = [](const std::array<std::uint64_t, 2>& id) {
      std::string key(1, '\1');
      for (auto part : id)
        for (unsigned i = 8; i != 0; --i) key.push_back(part >> ((i - 1) * 8));
      return key;
    };
    const auto low = make_key(range.first_), high = make_key(range.last_);
    std::vector<MemoryReservation> retained;
    std::vector<std::string> entries;
    std::string header;
    std::optional<MemoryReservation> probe_charge;
    auto load =
        [&](std::size_t index) -> Task<absl::StatusOr<LoadedOrderedGroup>> {
      try {
        const HashGroupId id{groups[index].id_, 0};
        const auto* physical = object->FindGroup(id);
        if (!physical)
          co_return absl::DataLossError("missing Stream probe page");
        GroupedScratchBudget budget;
        auto added = budget.AddGroup(physical->value_, object->ExtentsFor(id));
        if (!added.ok()) co_return added;
        auto admitted = budget.Reserve(2);
        if (!admitted.ok()) co_return admitted.status();
        probe_charge.emplace(std::move(*admitted));
        co_return co_await LoadOrderedGroupSnapshot(
            store, partition, db_id, key, digest, object, groups[index].id_);
      } catch (const std::bad_alloc&) {
        RecordMemoryRejection();
        co_return absl::ResourceExhaustedError("OOM grouped Stream probe");
      }
    };
    {
      auto first = co_await load(0);
      if (!first.ok()) co_return first.status();
      auto payload =
          StreamRecordPayload(first->snapshot_.entries_.front().value_);
      if (!payload.ok() || payload->size() != 48 ||
          !payload->starts_with("LXS1"))
        co_return absl::DataLossError("invalid Stream range header");
      header = *payload;
    }
    std::size_t first = 0, last = groups.size();
    const auto& wanted = range.reverse_ ? high : low;
    if (range.count_ != 0 && low <= high) {
      while (first < last) {
        const auto middle = first + (last - first) / 2;
        auto page = co_await load(middle);
        if (!page.ok()) co_return page.status();
        auto bound = StreamRecordKey(page->snapshot_.entries_.back().value_);
        if (!bound.ok()) co_return bound.status();
        if (*bound < wanted)
          first = middle + 1;
        else
          last = middle;
      }
      auto index = std::min(first, groups.size() - 1);
      bool done = false;
      while (!done) {
        if (shutdown_flush_requested_)
          co_return absl::CancelledError(
              "Stream range interrupted by shutdown");
        co_await bycorf::Yield(*store.worker_);
        const HashGroupId id{groups[index].id_, 0};
        const auto* physical = object->FindGroup(id);
        if (!physical)
          co_return absl::DataLossError("missing Stream range page");
        GroupedScratchBudget budget;
        auto added = budget.AddGroup(physical->value_, object->ExtentsFor(id));
        if (!added.ok()) co_return added;
        // Retained selected entries and the callback's decode/reply copies must
        // stay admitted after this page's temporary decoder has been destroyed.
        auto admitted = budget.Reserve(6);
        if (!admitted.ok()) co_return admitted.status();
        auto page = co_await load(index);
        if (!page.ok()) co_return page.status();
        const auto old_count = entries.size();
        const auto& values = page->snapshot_.entries_;
        for (std::size_t n = 0; n < values.size(); ++n) {
          const auto& entry =
              values[range.reverse_ ? values.size() - 1 - n : n];
          auto record_key = StreamRecordKey(entry.value_);
          if (!record_key.ok()) co_return record_key.status();
          const bool below =
              range.first_exclusive_ ? *record_key <= low : *record_key < low;
          const bool above =
              range.last_exclusive_ ? *record_key >= high : *record_key > high;
          if ((range.reverse_ && below) || (!range.reverse_ && above)) {
            done = true;
            break;
          }
          if (below || above) continue;
          auto payload = StreamRecordPayload(entry.value_);
          if (!payload.ok()) co_return payload.status();
          entries.emplace_back(*payload);
          if (entries.size() == range.count_) {
            done = true;
            break;
          }
        }
        if (entries.size() != old_count)
          retained.push_back(std::move(*admitted));
        if (range.reverse_) {
          if (index == 0) break;
          --index;
        } else if (++index == groups.size())
          break;
      }
    }
    SetCount(header, 44, entries.size());
    if (range.reverse_) std::reverse(entries.begin(), entries.end());
    for (const auto& entry : entries) header.append(entry);
    const auto at = header.size();
    header.resize(at + (entries.empty() ? 8 : 12), '\0');
    if (!entries.empty()) {
      SetCount(header, at, 1);
      SetCount(header, at + 4, entries.size());
    }
    auto update = callback(CompactValueView{
        .encoded_ = header,
        .logical_size_ = entries.size(),
        .expire_at_ms_ = object->version().root_.expire_at_ms_});
    if (!update.ok()) co_return update.status();
    if (update->changed_)
      co_return absl::InvalidArgumentError("Stream range callback mutated");
    co_return absl::OkStatus();
  } catch (const std::bad_alloc&) {
    RecordMemoryRejection();
    co_return absl::ResourceExhaustedError("OOM grouped Stream workspace");
  }
}

}  // namespace lavik::storage
