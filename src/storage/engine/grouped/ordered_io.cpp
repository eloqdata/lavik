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

#include "../impl.h"
#include "lavik/status_macros.h"

namespace lavik::storage {

template <typename Result, typename Decode>
Task<absl::StatusOr<Result>> StorageEngine::Impl::LoadOrderedGroup(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    GroupedObject::Handle object, std::uint64_t page_id, bool pinned,
    Decode decode) {
  if (object == nullptr || !object->is_ordered()) {
    co_return absl::DataLossError("missing ordered collection view");
  }
  const auto original = object->version();
  const auto root = object->ordered_directory().root();
  const GroupedRecordId id{page_id, 0};
  for (;;) {
    LAVIK_CO_RETURN_IF_ERROR(object->ReadStatus());
    if (EffectiveRecordDbEpoch(partition, db_id) != original.db_epoch_ ||
        partition.replication_epoch_ != original.replication_epoch_ ||
        partition.grouped_generations_[db_id] != original.index_generation_) {
      co_return absl::NotFoundError("ordered population changed before read");
    }
    if (!pinned) {
      auto resolved = co_await FindVerifiedEntry(
          store, partition.indexes_[db_id], digest, key, original.root_);
      LAVIK_CO_RETURN_IF_ERROR(resolved.status());
      if (*resolved == nullptr || !(*resolved)->value_.grouped() ||
          (*resolved)->value_.mutation_sequence_ !=
              original.root_.mutation_sequence_) {
        co_return absl::NotFoundError("ordered root changed during read");
      }
      auto current = partition.grouped_objects_[db_id].Lookup(
          digest, key,
          GroupedObjectVersion{
              .root_ = MaterializeIndexLocation(**resolved),
              .db_epoch_ = original.db_epoch_,
              .replication_epoch_ = original.replication_epoch_,
              .index_generation_ = original.index_generation_});
      LAVIK_CO_RETURN_IF_ERROR(current.status());
      if (*current == nullptr || !(*current)->is_ordered() ||
          (*current)->ordered_directory().root() != root) {
        co_return absl::NotFoundError("ordered logical version changed");
      }
      object = std::move(*current);
    }
    const auto* entry = object->FindGroup(id);
    if (entry == nullptr) co_return absl::DataLossError("missing ordered page");
    // A retained side view owns metadata, not allocation lifetime. Capture the
    // current physical epoch before suspending, and retry a GC move only while
    // the logical root is unchanged. Historical snapshots instead own pins.
    const auto location = MaterializeIndexLocation(*entry);
    const auto extents = object->ExtentsFor(id);
    absl::StatusOr<LoadedValue> loaded;
    if (location.external()) {
      loaded = co_await LoadExternalValueLocal(store, location, extents,
                                               nullptr, true);
    } else if (location.block_owner() == store.worker_->id()) {
      loaded = co_await LoadValueLocal(store, db_id, key, location,
                                       original.replication_epoch_, nullptr,
                                       original.db_epoch_);
    } else {
      const unsigned owner = location.block_owner();
      // The caller keeps key alive across this awaited hop. Borrowing it avoids
      // copying a multi-megabyte parent once for every small ordered page.
      loaded = co_await bycorf::SubmitTaskTo(
          owner,
          [this, owner, db_id, key, location,
           epoch = original.replication_epoch_,
           db_epoch =
               original.db_epoch_]() -> Task<absl::StatusOr<LoadedValue>> {
            co_return co_await LoadValueLocal(*stores_[owner], db_id, key,
                                              location, epoch, nullptr,
                                              db_epoch);
          });
    }
    if (EffectiveRecordDbEpoch(partition, db_id) != original.db_epoch_ ||
        partition.replication_epoch_ != original.replication_epoch_ ||
        partition.grouped_generations_[db_id] != original.index_generation_) {
      co_return absl::NotFoundError("ordered population changed during read");
    }
    LAVIK_CO_RETURN_IF_ERROR(object->ReadStatus());
    if (loaded.ok()) {
      const auto bytes = loaded->value();
      const std::string_view payload(
          reinterpret_cast<const char*>(bytes.data()), bytes.size());
      const auto envelope = DecodeOrderedGroupMetadata(payload, payload.size());
      LAVIK_CO_RETURN_IF_ERROR(envelope.status());
      // The external page admission uses the captured count; validate it
      // before decoding can reserve storage from untrusted payload metadata.
      const auto* route = object->ordered_directory().Find(page_id);
      if (envelope->kind_ != root.kind_ ||
          envelope->incarnation_ != root.incarnation_ ||
          envelope->id_ != page_id || envelope->retired_ ||
          envelope->item_count_ != location.logical_size_ || route == nullptr ||
          envelope->previous_ != route->previous_ ||
          envelope->next_ != route->next_) {
        co_return absl::DataLossError(
            "ordered page physical identity mismatch");
      }
      co_return decode(payload, location.mutation_sequence_, *route,
                       std::move(*loaded));
    }
    if (pinned || !absl::IsAborted(loaded.status())) co_return loaded.status();
    // The next iteration refreshes the view before materializing an entry.
    // A persistent IO abort without relocation is corruption, not a spin.
    const auto current =
        partition.grouped_objects_[db_id].CurrentForMutation(key);
    const auto* moved = current == nullptr ? nullptr : current->FindGroup(id);
    if (moved == nullptr ||
        MaterializeIndexLocation(*moved).SamePhysicalRecord(location)) {
      co_return absl::DataLossError(loaded.status().message());
    }
  }
}

Task<absl::StatusOr<LoadedOrderedGroup>>
StorageEngine::Impl::LoadOrderedGroupSnapshot(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    GroupedObject::Handle object, std::uint64_t page_id, bool pinned) {
  return LoadOrderedGroup<LoadedOrderedGroup>(
      store, partition, db_id, key, digest, std::move(object), page_id, pinned,
      [](std::string_view payload, std::uint64_t sequence,
         const OrderedGroupEntry& route,
         LoadedValue&&) -> absl::StatusOr<LoadedOrderedGroup> {
        auto decoded = DecodeOrderedGroup(payload);
        LAVIK_RETURN_IF_ERROR(decoded.status());
        if (decoded->kind_ == OrderedCollectionKind::kSortedSet &&
            (decoded->entries_.front().score_ != route.min_score_ ||
             decoded->entries_.back().score_ != route.max_score_))
          return absl::DataLossError("ordered page score bounds mismatch");
        return LoadedOrderedGroup{.sequence_ = sequence,
                                  .snapshot_ = std::move(*decoded)};
      });
}

Task<absl::StatusOr<std::vector<std::string>>>
StorageEngine::Impl::LoadOrderedListRange(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    GroupedObject::Handle object, std::uint64_t page_id, std::size_t first,
    std::size_t count) {
  return LoadOrderedGroup<std::vector<std::string>>(
      store, partition, db_id, key, digest, std::move(object), page_id, false,
      [first, count](std::string_view payload, std::uint64_t,
                     const OrderedGroupEntry&, LoadedValue&&) {
        return DecodeOrderedListRange(payload, first, count);
      });
}

Task<absl::StatusOr<StorageEngine::Impl::LoadedSortedSetPage>>
StorageEngine::Impl::LoadSortedSetPage(WorkerStore& store,
                                       WorkerStore::PartitionStore& partition,
                                       std::uint8_t db_id, std::string_view key,
                                       const Digest& digest,
                                       GroupedObject::Handle object,
                                       std::uint64_t page_id) {
  return LoadOrderedGroup<LoadedSortedSetPage>(
      store, partition, db_id, key, digest, std::move(object), page_id, false,
      [](std::string_view payload, std::uint64_t,
         const OrderedGroupEntry& route,
         LoadedValue&& loaded) -> absl::StatusOr<LoadedSortedSetPage> {
        auto entries = DecodeSortedSetGroupViews(payload);
        LAVIK_RETURN_IF_ERROR(entries.status());
        if (entries->front().score_ != route.min_score_ ||
            entries->back().score_ != route.max_score_)
          return absl::DataLossError("ordered page score bounds mismatch");
        return LoadedSortedSetPage{std::move(loaded), std::move(*entries)};
      });
}

Task<absl::StatusOr<std::vector<OrderedCollectionEntry>>>
StorageEngine::Impl::LoadGroupedOrderedValue(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    GroupedObject::Handle object, bool pinned) {
  if (object == nullptr || !object->is_ordered()) {
    co_return absl::DataLossError("missing ordered collection view");
  }
  std::vector<OrderedCollectionEntry> result;
  std::uint64_t logical_size = 0;
  for (const auto& metadata : object->ordered_directory().groups()) {
    auto page = co_await LoadOrderedGroupSnapshot(
        store, partition, db_id, key, digest, object, metadata.id_, pinned);
    LAVIK_CO_RETURN_IF_ERROR(page.status());
    if (page->snapshot_.previous_ != metadata.previous_ ||
        page->snapshot_.next_ != metadata.next_ ||
        OrderedGroupSize(page->snapshot_) != metadata.item_count_) {
      co_return absl::DataLossError(
          "ordered page links disagree with directory");
    }
    if (!result.empty()) {
      LAVIK_CO_RETURN_IF_ERROR(ValidateOrderedEntryBoundary(
          object->ordered_directory().root().kind_, result.back(),
          page->snapshot_.entries_.front()));
    }
    logical_size += OrderedGroupSize(page->snapshot_);
    for (auto& entry : page->snapshot_.entries_) {
      result.push_back(std::move(entry));
    }
  }
  if (logical_size != object->ordered_directory().root().item_count_) {
    co_return absl::DataLossError("ordered collection aggregate mismatch");
  }
  co_return result;
}

}  // namespace lavik::storage
