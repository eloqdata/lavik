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

  Task<absl::Status> Load(std::size_t index) {
    try {
      if (pages_.contains(index)) co_return absl::OkStatus();
      const auto& groups = object_->ordered_directory().groups();
      const HashGroupId id{groups[index].id_, 0};
      const auto* physical = object_->FindGroup(id);
      if (!physical) co_return absl::DataLossError("missing Stream page");
      GroupedScratchBudget budget;
      auto added = budget.AddGroup(physical->value_, object_->ExtentsFor(id),
                                   key_.size());
      if (!added.ok()) co_return added;
      auto admitted = budget.Reserve(6);
      if (!admitted.ok()) co_return admitted.status();
      reservations_.push_back(std::move(*admitted));
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
  Task<absl::StatusOr<OrderedCollectionMutationPlan>> Plan(
      std::vector<StreamRecordChange> changes, std::uint64_t length) {
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
      if (changes.empty()) {
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
      for (auto& change : changes) {
        auto index = co_await Route(change.key_);
        if (!index.ok()) co_return index.status();
        for (auto adjacent = *index == 0 ? 0 : *index - 1;
             adjacent < std::min(*index + 2, groups.size()); ++adjacent) {
          auto status = co_await Load(adjacent);
          if (!status.ok()) co_return status;
        }
        change.page_id_ = groups[*index].id_;
      }
      std::vector<LoadedOrderedGroup> loaded;
      loaded.reserve(pages_.size());
      for (auto& [index, page] : pages_) loaded.push_back(std::move(page));
      co_return PlanStreamRecordChanges(directory, std::move(loaded),
                                        std::move(changes), length);
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
    auto update = callback(CompactValueView{
        .encoded_ = partial,
        .logical_size_ = 0,
        .expire_at_ms_ = object->version().root_.expire_at_ms_});
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
    auto plan = co_await cache.Plan(std::move(changes), length + 1);
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
    const StreamGroupAccess& access, TxShardWrites* tx,
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
      auto payload = StreamRecordPayload(original_group);
      if (!payload.ok() || payload->size() < 32 ||
          Count(*payload) != access.group_.size())
        co_return absl::DataLossError("invalid partial Stream group header");
      total_consumers = Count(*payload, payload->size() - 4);
      const auto last_id =
          ReadId(payload->substr(4 + access.group_.size(), 16));
      if (access.read_new_count_) {
        auto selected = co_await cache.Scan(
            IdKey(std::string_view("\1", 1), last_id),
            std::string_view("\2", 1), *access.read_new_count_, true);
        if (!selected.ok()) co_return selected.status();
        entries = std::move(*selected);
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
    if (!*group || update->erase_ || update->reuse_encoded_ ||
        update->logical_size_ != entries.size())
      co_return absl::InvalidArgumentError(
          "partial Stream group changed key/entries");
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
    old_records[group_key] = std::move(original_group);
    old_records[count_key] = std::move(original_count);
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
        auto added = budget.AddGroup(physical->value_, object->ExtentsFor(id),
                                     key.size());
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
        auto added = budget.AddGroup(physical->value_, object->ExtentsFor(id),
                                     key.size());
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
