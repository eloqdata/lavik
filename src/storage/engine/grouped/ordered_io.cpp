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
#include "lavik/storage/detail/grouped/scratch.h"
#include "read_window.h"

namespace lavik::storage {

template <typename Result, typename Decode>
Task<absl::StatusOr<Result>> StorageEngine::Impl::LoadOrderedGroup(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    GroupedObject::Handle object, std::uint64_t page_id, bool pinned,
    Decode decode, GroupReadLimiter* limiter) {
  if (object == nullptr || !object->is_ordered()) {
    co_return absl::DataLossError("missing ordered collection view");
  }
  const auto original = object->version();
  const auto root = object->ordered_directory().root();
  const GroupedRecordId id{page_id, 0};
  for (;;) {
    const auto readable = object->ReadStatus();
    if (!readable.ok()) co_return readable;
    if (EffectiveRecordDbEpoch(partition, db_id) != original.db_epoch_ ||
        partition.replication_epoch_ != original.replication_epoch_ ||
        partition.grouped_generations_[db_id] != original.index_generation_) {
      co_return absl::NotFoundError("ordered population changed before read");
    }
    if (!pinned) {
      auto resolved = co_await FindVerifiedEntry(
          store, partition.indexes_[db_id], digest, key, original.root_);
      if (!resolved.ok()) co_return resolved.status();
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
      if (!current.ok()) co_return current.status();
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
    GroupReadLimiter::Permit permit;
    if (limiter != nullptr) {
      if (location.block_owner() >= worker_count_)
        co_return absl::DataLossError("ordered page has no physical owner");
      permit = co_await limiter->Acquire(location.block_owner());
    }
    // The read lease must be released before handing this owner's permit to
    // another child. Completed children retain owned results, not read leases.
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
    const auto after_io = object->ReadStatus();
    if (!after_io.ok()) co_return after_io;
    if (loaded.ok()) {
      const auto bytes = loaded->value();
      const std::string_view payload(
          reinterpret_cast<const char*>(bytes.data()), bytes.size());
      const auto envelope = DecodeOrderedGroupMetadata(payload, payload.size());
      if (!envelope.ok()) co_return envelope.status();
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
    GroupedObject::Handle object, std::uint64_t page_id, bool pinned,
    GroupReadLimiter* limiter, SortedSetMemberValidation validation) {
  return LoadOrderedGroup<LoadedOrderedGroup>(
      store, partition, db_id, key, digest, std::move(object), page_id, pinned,
      [validation](std::string_view payload, std::uint64_t sequence,
                   const OrderedGroupEntry& route,
                   LoadedValue&&) -> absl::StatusOr<LoadedOrderedGroup> {
        auto decoded = DecodeOrderedGroup(payload, validation);
        if (!decoded.ok()) return decoded.status();
        if (decoded->kind_ == OrderedCollectionKind::kSortedSet) {
          if (decoded->entries_.front().score_ != route.min_score_ ||
              decoded->entries_.back().score_ != route.max_score_)
            return absl::DataLossError("ordered page score bounds mismatch");
        }
        return LoadedOrderedGroup{.sequence_ = sequence,
                                  .snapshot_ = std::move(*decoded)};
      },
      limiter);
}

Task<absl::StatusOr<LoadedOrderedGroup>>
StorageEngine::Impl::LoadEncodedSortedSetGroupSnapshot(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    GroupedObject::Handle object, std::uint64_t page_id,
    GroupReadLimiter* limiter) {
  return LoadOrderedGroup<LoadedOrderedGroup>(
      store, partition, db_id, key, digest, std::move(object), page_id, false,
      [](std::string_view payload, std::uint64_t sequence,
         const OrderedGroupEntry& route,
         LoadedValue&&) -> absl::StatusOr<LoadedOrderedGroup> {
        auto decoded = DecodeEncodedSortedSetGroup(payload);
        if (!decoded.ok()) return decoded.status();
        const auto entries = decoded->encoded_sorted_set_->entries();
        if (entries.front().score_ != route.min_score_ ||
            entries.back().score_ != route.max_score_)
          return absl::DataLossError("ordered page score bounds mismatch");
        return LoadedOrderedGroup{.sequence_ = sequence,
                                  .snapshot_ = std::move(*decoded)};
      },
      limiter);
}

Task<absl::StatusOr<ListResult>> StorageEngine::Impl::LoadOrderedListRange(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    GroupedObject::Handle object, std::uint64_t page_id, std::size_t first,
    std::size_t count, GroupReadLimiter* limiter, bool packed_output) {
  return LoadOrderedGroup<ListResult>(
      store, partition, db_id, key, digest, std::move(object), page_id, false,
      [first, count, packed_output](
          std::string_view payload, std::uint64_t, const OrderedGroupEntry&,
          LoadedValue&&) -> absl::StatusOr<ListResult> {
        // The parent holds aggregate scratch until every child is joined.
        // Pages own no charge: only the final result retains the admitted
        // bytes, after this loader has released its physical read lease.
        ListResult result;
        if (packed_output) {
          auto decoded = DecodePackedOrderedListRange(payload, first, count);
          if (!decoded.ok()) return decoded.status();
          result.packed_values_ =
              std::make_unique<PackedValues>(std::move(*decoded));
        } else {
          auto decoded = DecodeOrderedListRange(payload, first, count);
          if (!decoded.ok()) return decoded.status();
          result.values_ = std::move(*decoded);
        }
        return result;
      },
      limiter);
}

Task<absl::StatusOr<StorageEngine::Impl::LoadedSortedSetPage>>
StorageEngine::Impl::LoadSortedSetPage(WorkerStore& store,
                                       WorkerStore::PartitionStore& partition,
                                       std::uint8_t db_id, std::string_view key,
                                       const Digest& digest,
                                       GroupedObject::Handle object,
                                       std::uint64_t page_id,
                                       SortedSetMemberValidation validation) {
  return LoadOrderedGroup<LoadedSortedSetPage>(
      store, partition, db_id, key, digest, std::move(object), page_id, false,
      [validation](
          std::string_view payload, std::uint64_t,
          const OrderedGroupEntry& route,
          LoadedValue&& loaded) -> absl::StatusOr<LoadedSortedSetPage> {
        auto entries = DecodeSortedSetGroupViews(payload, validation);
        if (!entries.ok()) return entries.status();
        if (entries->front().score_ != route.min_score_ ||
            entries->back().score_ != route.max_score_)
          return absl::DataLossError("ordered page score bounds mismatch");
        return LoadedSortedSetPage{std::move(loaded), std::move(*entries)};
      });
}

Task<absl::StatusOr<PackedSortedSetValues>>
StorageEngine::Impl::LoadPackedSortedSetPage(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    GroupedObject::Handle object, std::uint64_t page_id,
    GroupReadLimiter* limiter) {
  return LoadOrderedGroup<PackedSortedSetValues>(
      store, partition, db_id, key, digest, std::move(object), page_id, false,
      [](std::string_view payload, std::uint64_t,
         const OrderedGroupEntry& route,
         LoadedValue&&) -> absl::StatusOr<PackedSortedSetValues> {
        auto entries = DecodeSortedSetGroupViews(
            payload, SortedSetMemberValidation::kTrustStored);
        if (!entries.ok()) return entries.status();
        if (entries->front().score_ != route.min_score_ ||
            entries->back().score_ != route.max_score_)
          return absl::DataLossError("ordered page score bounds mismatch");
        std::size_t bytes = 0;
        for (const auto& entry : *entries) {
          if (entry.value_.size() > SIZE_MAX - bytes)
            return absl::ResourceExhaustedError(
                "Sorted Set page size overflow");
          bytes += entry.value_.size();
        }
        PackedSortedSetValues result;
        result.members_.blocks_.emplace_back(bytes, '\0');
        result.members_.values_.reserve(entries->size());
        result.scores_.reserve(entries->size());
        auto& block = result.members_.blocks_.front();
        std::size_t offset = 0;
        for (const auto& entry : *entries) {
          result.members_.values_.push_back({0, offset, entry.value_.size()});
          if (!entry.value_.empty())
            std::memcpy(block.data() + offset, entry.value_.data(),
                        entry.value_.size());
          offset += entry.value_.size();
          result.scores_.push_back(entry.score_);
        }
        return result;
      },
      limiter);
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
    if (!page.ok()) co_return page.status();
    if (page->snapshot_.previous_ != metadata.previous_ ||
        page->snapshot_.next_ != metadata.next_ ||
        OrderedGroupSize(page->snapshot_) != metadata.item_count_) {
      co_return absl::DataLossError(
          "ordered page links disagree with directory");
    }
    if (!result.empty()) {
      auto boundary = ValidateOrderedEntryBoundary(
          object->ordered_directory().root().kind_, result.back(),
          page->snapshot_.entries_.front());
      if (!boundary.ok()) co_return boundary;
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

Task<absl::StatusOr<StorageEngine::Impl::LoadedValue>>
StorageEngine::Impl::LoadGroupedStringValue(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    GroupedObject::Handle object, bool pinned) {
  const auto& directory = object->ordered_directory();
  const auto& root = directory.root();
  if (root.item_count_ == 0 || root.item_count_ > kMaxStringBytes ||
      root.item_count_ != object->version().root_.logical_size_ ||
      root.group_count_ !=
          (root.item_count_ + kStringGroupBytes - 1) / kStringGroupBytes ||
      root.first_group_ != 1 || root.last_group_ != root.group_count_ ||
      root.next_group_id_ != root.last_group_ + 1) {
    co_return absl::DataLossError("grouped String aggregate mismatch");
  }
  const auto size = static_cast<std::size_t>(root.item_count_);
  const auto& options = options_.buffers_;
  // The output is assembled by copying pages, so it needs no DMA allocation.
  // It must not hold a fixed slot while children wait for their read buffers.
  // Match AllocateReadOutputBuffer's capacity for scratch admission.
  const auto minimum_output = std::max(size, options.alignment_);
  constexpr auto limit = std::numeric_limits<std::uint32_t>::max();
  if (options.alignment_ == 0 || options.alignment_ > limit ||
      minimum_output > limit - (options.alignment_ - 1)) {
    co_return absl::ResourceExhaustedError(
        "grouped String output size overflow");
  }
  const auto aligned_output =
      (minimum_output + options.alignment_ - 1) & ~(options.alignment_ - 1);
  if (options.read_headroom_bytes_ > limit - aligned_output ||
      options.read_tailroom_bytes_ >
          limit - aligned_output - options.read_headroom_bytes_) {
    co_return absl::ResourceExhaustedError(
        "grouped String output size overflow");
  }
  const auto output_bytes = aligned_output + options.read_headroom_bytes_ +
                            options.read_tailroom_bytes_;
  std::size_t page_scratch = 0;
  const auto groups = directory.groups();
  std::size_t ordinal = 0;
  for (const auto& metadata : directory.groups()) {
    // Fixed segment positions let children copy into disjoint output slices,
    // independently of owner and completion order. Validate before launching.
    if (metadata.id_ != ordinal + 1 ||
        metadata.item_count_ !=
            std::min(kStringGroupBytes, size - ordinal * kStringGroupBytes))
      co_return absl::DataLossError("grouped String segment layout mismatch");
    ++ordinal;
    const GroupedRecordId id{metadata.id_, 0};
    const auto* entry = object->FindGroup(id);
    if (entry == nullptr)
      co_return absl::DataLossError("missing grouped materialization page");
    GroupedScratchBudget page;
    const auto status = page.AddGroup(*entry, object->ExtentsFor(id));
    if (!status.ok()) co_return status;
    page_scratch = std::max(page_scratch, page.bytes());
  }
  // The required admission preserves the serial reader's memory floor. Extra
  // window metadata and page scratch are optional: scarce headroom reduces
  // concurrency instead of rejecting a read that can still run serially.
  // No decoded segment strings or second whole-value image coexist here.
  GroupedScratchBudget budget;
  auto status = budget.AddBytes(AllocatorUsableSizeForRequest(output_bytes));
  if (!status.ok()) co_return status;
  status = budget.AddBytes(page_scratch);
  if (!status.ok()) co_return status;
  auto scratch = budget.Reserve(1);
  if (!scratch.ok()) co_return scratch.status();

  auto plan = GroupReadPlan::TryCreate(worker_count_, groups.size());
  if (plan) {
    for (std::size_t i = 0; i < groups.size(); ++i) {
      const auto* entry = object->FindGroup({groups[i].id_, 0});
      // Retained metadata can predate relocation. Read only an atomic owner
      // hint here; each child refreshes before acquiring its actual permit.
      auto owner = BlockOwner(entry->block_id());
      if (owner >= worker_count_) owner = store.worker_->id();
      plan->Add(owner, i);
    }
    if (!plan->ReserveWindow<std::size_t>(page_scratch)) plan.reset();
  }

  auto buffer = store.buffers_.AllocateReadOutputBuffer(minimum_output);
  if (!buffer.ok()) co_return buffer.status();
  const auto output = buffer->io_buffer();
  if (output.size_ < size)
    co_return absl::ResourceExhaustedError("grouped output buffer too small");
  // Destroy children before their borrowed output, key, view and admission.
  // No child is detached, including when shutdown destroys this parent.
  std::optional<GroupReadWindow<std::size_t>> window;
  if (plan) window.emplace(store.worker_, worker_count_, plan->width());
  auto read = [&](std::size_t i) {
    const auto& metadata = groups[i];
    return LoadOrderedGroup<std::size_t>(
        store, partition, db_id, key, digest, object, metadata.id_, pinned,
        [metadata, destination = std::span(output.data_ + i * kStringGroupBytes,
                                           metadata.item_count_)](
            std::string_view payload, std::uint64_t,
            const OrderedGroupEntry& route,
            LoadedValue&&) -> absl::StatusOr<std::size_t> {
          // LoadOrderedGroup already checked CRC, physical/logical identity,
          // and the fixed String segment envelope without decoding its body.
          // Reconcile against the retained directory before exposing bytes.
          const auto bytes = payload.substr(kOrderedGroupHeaderBytes);
          if (route.previous_ != metadata.previous_ ||
              route.next_ != metadata.next_ ||
              bytes.size() != metadata.item_count_) {
            return absl::DataLossError(
                "ordered page links disagree with directory");
          }
          if (bytes.size() > destination.size())
            return absl::DataLossError("grouped String aggregate mismatch");
          std::memcpy(destination.data(), bytes.data(), bytes.size());
          return bytes.size();
        },
        window ? window->limiter() : nullptr);
  };
  std::size_t written = 0;
  if (!window) {
    for (std::size_t i = 0; i < groups.size(); ++i) {
      auto copied = co_await read(i);
      if (!copied.ok()) co_return copied.status();
      written += *copied;
    }
  } else {
    while (!plan->empty()) {
      absl::Status start_status;
      for (unsigned owner = 0; owner < plan->owner_count(); ++owner) {
        for (std::size_t n = 0; n < GroupReadLimiter::kWidth; ++n) {
          const auto i = plan->Pop(owner);
          if (!i) break;
          // Exercise a partial start without recovering from C++ allocation
          // failure, which remains fatal by policy.
          if (window->size() == 1 &&
              LAVIK_FAULT_MATCHES("LAVIK_FAIL_STRING_READ_BATCH_START_KEY",
                                  key)) {
            start_status = absl::ResourceExhaustedError(
                "OOM grouped String read batch admission");
            break;
          }
          window->Add(read(*i), *i);
        }
        if (!start_status.ok()) break;
      }
      co_await window->Join();
      // Joining adds a scheduling gap after the last child's validation.
      // A population replacement in that gap invalidates even successful I/O.
      const auto& version = object->version();
      if (EffectiveRecordDbEpoch(partition, db_id) != version.db_epoch_ ||
          partition.replication_epoch_ != version.replication_epoch_ ||
          partition.grouped_generations_[db_id] != version.index_generation_)
        co_return absl::NotFoundError("ordered population changed during read");
      const auto readable = object->ReadStatus();
      if (!readable.ok()) co_return readable;
      const auto completed =
          window->Collect([&](std::size_t, std::size_t bytes) {
            written += bytes;
            return absl::OkStatus();
          });
      if (!start_status.ok()) co_return start_status;
      if (!completed.ok()) co_return completed;
    }
  }
  if (written != size)
    co_return absl::DataLossError("grouped String aggregate mismatch");
  const auto offset =
      static_cast<std::size_t>(output.data_ - buffer->bytes().data());
  co_return LoadedValue{std::move(*buffer), offset, written};
}

}  // namespace lavik::storage
