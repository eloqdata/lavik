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
#include "lavik/storage/detail/hash_read.h"
#include "lavik/storage/detail/ordered_compact_codec.h"
#include "read_window.h"

namespace lavik::storage {

absl::Status StorageEngine::Impl::ValidateGroupedRead(
    WorkerStore::PartitionStore& partition, std::uint8_t db_id,
    std::string_view key, const RecordIndex::Entry* entry) {
  if (!entry || !entry->value_.grouped()) return absl::OkStatus();
  auto object = partition.grouped_objects_[db_id].Lookup(
      key, GroupedObjectVersion{
               .root_ = MaterializeIndexLocation(*entry),
               .db_epoch_ = EffectiveRecordDbEpoch(partition, db_id),
               .replication_epoch_ = partition.replication_epoch_,
               .index_generation_ = partition.grouped_generations_[db_id]});
  return object.status();
}

template <typename Result, typename Decode>
Task<absl::StatusOr<Result>> StorageEngine::Impl::LoadHashGroup(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    GroupedObject::Handle object, GroupedRecordId id, bool pinned,
    Decode decode, GroupReadLimiter* limiter) {
  if (object == nullptr) co_return absl::DataLossError("missing grouped view");
  if (object->is_ordered() && !object->has_member_index())
    co_return absl::DataLossError("ordered view has no prefix groups");
  const auto original = object->version();
  const auto root_identity = object->directory().root();
  const auto incarnation = root_identity.incarnation_;
  for (;;) {
    const auto readable = object->ReadStatus();
    if (!readable.ok()) co_return readable;
    if (EffectiveRecordDbEpoch(partition, db_id) != original.db_epoch_ ||
        partition.replication_epoch_ != original.replication_epoch_ ||
        partition.grouped_generations_[db_id] != original.index_generation_) {
      co_return absl::NotFoundError("grouped population changed before read");
    }
    if (!pinned) {
      // The caller may have read another group since capturing this view.
      // Refresh before materializing compact locations, not only after IO
      // fails: otherwise a reused block could lend an old entry a new epoch.
      auto resolved = co_await FindVerifiedEntry(
          store, partition.indexes_[db_id], digest, key, original.root_);
      if (!resolved.ok()) co_return resolved.status();
      if (*resolved == nullptr || !(*resolved)->value_.grouped() ||
          (*resolved)->value_.mutation_sequence_ !=
              original.root_.mutation_sequence_) {
        co_return absl::NotFoundError("grouped root changed during read");
      }
      auto current = partition.grouped_objects_[db_id].Lookup(
          key, GroupedObjectVersion{
                   .root_ = MaterializeIndexLocation(**resolved),
                   .db_epoch_ = original.db_epoch_,
                   .replication_epoch_ = original.replication_epoch_,
                   .index_generation_ = original.index_generation_,
               });
      if (!current.ok()) co_return current.status();
      if (*current == nullptr ||
          ((*current)->is_ordered() && !(*current)->has_member_index()) ||
          (*current)->directory().root() != root_identity) {
        co_return absl::NotFoundError(
            "grouped logical version changed during read");
      }
      object = std::move(*current);
    }
    const auto* entry = object->FindGroup(id);
    if (entry == nullptr) co_return absl::DataLossError("missing Hash group");
    // Materialize BEFORE the first suspension. An old compact entry cannot
    // borrow a subsequently reused block's current epoch. Snapshot callers
    // hold graph pins; an ordinary reader captures current identity here and
    // retries against a refreshed view if GC moves it during IO.
    const RecordLocation location = MaterializeIndexLocation(*entry);
    // The captured record says whether a manifest exists; inline pages need
    // no second physical-index lookup. External reads retain the owned handle
    // across suspension exactly as before.
    auto extents =
        location.external() ? object->ExtentsFor(id) : ExtentManifest{};
    GroupReadLimiter::Permit permit;
    if (limiter != nullptr) {
      if (location.block_owner() >= worker_count_)
        co_return absl::DataLossError("Hash group has no physical owner");
      permit = co_await limiter->Acquire(location.block_owner());
    }
    // The synchronous decoder and page lease finish before handing the permit
    // to another child. Queued reads reacquire it for every relocation retry.
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
      // Batched readers join before releasing their caller's key. Borrow it
      // across the awaited hop so concurrency does not multiply large key
      // copies. Other payload readers retain their existing copy admission.
      std::optional<MemoryReservation> key_admission;
      std::string owned_key;
      if (limiter == nullptr) {
        key_admission =
            TryReserveMemory(AllocatorUsableSizeForRequest(key.size() + 1));
        if (!key_admission) {
          RecordMemoryRejection();
          co_return absl::ResourceExhaustedError(
              "OOM grouped parent key copy admission");
        }
        owned_key.assign(key);
      }
      loaded = co_await bycorf::SubmitTaskTo(
          owner,
          [this, owner, db_id,
           read_key = limiter ? key : std::string_view(owned_key), location,
           epoch = original.replication_epoch_,
           db_epoch =
               original.db_epoch_]() -> Task<absl::StatusOr<LoadedValue>> {
            co_return co_await LoadValueLocal(*stores_[owner], db_id, read_key,
                                              location, epoch, nullptr,
                                              db_epoch);
          });
    }
    if (EffectiveRecordDbEpoch(partition, db_id) != original.db_epoch_ ||
        partition.replication_epoch_ != original.replication_epoch_ ||
        partition.grouped_generations_[db_id] != original.index_generation_) {
      co_return absl::NotFoundError("grouped population changed during read");
    }
    const auto after_io = object->ReadStatus();
    if (!after_io.ok()) co_return after_io;
    if (loaded.ok()) {
      const auto bytes = loaded->value();
      const std::string_view payload(
          reinterpret_cast<const char*>(bytes.data()), bytes.size());
      const auto envelope = DecodeHashGroupMetadata(payload, payload.size());
      if (!envelope.ok()) co_return envelope.status();
      // Streaming readers admit a page from its captured physical metadata.
      // Reject corrupt counts before any decoder allocates its vectors.
      if (envelope->incarnation_ != incarnation || envelope->id_ != id ||
          envelope->retired_ ||
          envelope->field_count_ != location.logical_size_) {
        co_return absl::DataLossError("Hash group physical identity mismatch");
      }
      co_return decode(payload, location.mutation_sequence_,
                       envelope->field_count_, std::move(*loaded));
    }
    if (pinned || loaded.status().code() != absl::StatusCode::kAborted) {
      co_return loaded.status();
    }
    auto resolved = co_await FindVerifiedEntry(store, partition.indexes_[db_id],
                                               digest, key);
    if (!resolved.ok()) co_return resolved.status();
    if (*resolved == nullptr || !(*resolved)->value_.grouped() ||
        (*resolved)->value_.mutation_sequence_ !=
            original.root_.mutation_sequence_) {
      co_return absl::NotFoundError("grouped root changed during read");
    }
    const auto root = MaterializeIndexLocation(**resolved);
    auto current = partition.grouped_objects_[db_id].Lookup(
        key, GroupedObjectVersion{
                 .root_ = root,
                 .db_epoch_ = original.db_epoch_,
                 .replication_epoch_ = original.replication_epoch_,
                 .index_generation_ = original.index_generation_,
             });
    if (!current.ok()) co_return current.status();
    if (*current == nullptr ||
        ((*current)->is_ordered() && !(*current)->has_member_index()) ||
        (*current)->directory().root() != root_identity) {
      co_return absl::NotFoundError(
          "grouped logical version changed during read");
    }
    const auto* moved = (*current)->FindGroup(id);
    if (moved == nullptr ||
        MaterializeIndexLocation(*moved).SamePhysicalRecord(location)) {
      co_return absl::DataLossError(loaded.status().message());
    }
    object = std::move(*current);
  }
}

Task<absl::StatusOr<StorageEngine::Impl::LoadedHashGroupPayload>>
StorageEngine::Impl::LoadHashGroupPayload(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    GroupedObject::Handle object, GroupedRecordId id, bool pinned) {
  return LoadHashGroup<LoadedHashGroupPayload>(
      store, partition, db_id, key, digest, std::move(object), id, pinned,
      [](std::string_view, std::uint64_t sequence, std::uint32_t field_count,
         LoadedValue&& loaded) -> absl::StatusOr<LoadedHashGroupPayload> {
        return LoadedHashGroupPayload{.loaded_ = std::move(loaded),
                                      .sequence_ = sequence,
                                      .field_count_ = field_count};
      });
}

Task<absl::StatusOr<LoadedHashGroup>>
StorageEngine::Impl::LoadHashGroupSnapshot(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    GroupedObject::Handle object, GroupedRecordId id, bool pinned,
    GroupReadLimiter* limiter) {
  return LoadHashGroup<LoadedHashGroup>(
      store, partition, db_id, key, digest, object, id, pinned,
      [object, id](std::string_view payload, std::uint64_t sequence,
                   std::uint32_t,
                   LoadedValue&&) -> absl::StatusOr<LoadedHashGroup> {
        // The core validates the view before invoking this decoder. Keep its
        // immutable seed alive without dereferencing a possibly null input
        // while constructing the unstarted task.
        auto decoded = DecodeHashGroup(payload);
        if (!decoded.ok()) return decoded.status();
        for (const auto& field : decoded->value_.entries_) {
          if (!id.ContainsHash(
                  ComputeDigest(field.field_, object->directory().root().seed_)
                      .value_))
            return absl::DataLossError("Hash field outside its group route");
        }
        return LoadedHashGroup{.sequence_ = sequence,
                               .snapshot_ = std::move(*decoded)};
      },
      limiter);
}

Task<absl::StatusOr<HashValue>> StorageEngine::Impl::LoadGroupedHashValue(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    GroupedObject::Handle object, bool pinned) {
  if (object == nullptr) co_return absl::DataLossError("missing grouped view");
  HashValue result;
  for (const auto& [prefix, metadata] : object->directory().groups()) {
    auto loaded = co_await LoadHashGroupSnapshot(
        store, partition, db_id, key, digest, object, metadata.id_, pinned);
    if (!loaded.ok()) co_return loaded.status();
    for (auto& field : loaded->snapshot_.value_.entries_)
      result.entries_.push_back(std::move(field));
  }
  if (result.entries_.size() != object->directory().root().field_count_)
    co_return absl::DataLossError("grouped Hash aggregate mismatch");
  co_return result;
}

Task<absl::StatusOr<HashResult>> StorageEngine::Impl::ReadGroupedHashResult(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    GroupedObject::Handle object, HashOperationKind kind, bool with_values) {
  if (object == nullptr) co_return absl::DataLossError("missing grouped view");
  const auto& directory = object->directory();
  const auto count = directory.root().field_count_;
  const bool snapshot = kind == HashOperationKind::kReadSnapshot;
  const std::size_t width =
      kind == HashOperationKind::kGetAll || (snapshot && with_values) ? 2 : 1;
  const std::size_t slot_bytes = snapshot ? sizeof(HashReadSnapshot::Slice)
                                          : sizeof(std::optional<std::string>);
  std::size_t retained = sizeof(HashResult);
  if (snapshot) {
    if (directory.groups().size() >
        (SIZE_MAX - retained - sizeof(HashReadSnapshot)) / sizeof(std::string))
      co_return absl::ResourceExhaustedError("OOM Hash output is too large");
    retained += sizeof(HashReadSnapshot) +
                directory.groups().size() * sizeof(std::string);
  }
  if (count > (SIZE_MAX - retained) / (width * slot_bytes))
    co_return absl::ResourceExhaustedError("OOM Hash output is too large");
  retained += count * width * slot_bytes;
  auto output = TryReserveMemory(retained);
  if (!output) {
    RecordMemoryRejection();
    co_return absl::ResourceExhaustedError("OOM Hash output retention");
  }
  HashResult result;
  result.key_exists_ = true;
  result.length_ = count;
  if (snapshot) {
    result.snapshot_ = std::make_unique<HashReadSnapshot>();
    result.snapshot_->blocks_.reserve(directory.groups().size());
    result.snapshot_->values_.resize(count * width);
    retained =
        sizeof(HashResult) + sizeof(HashReadSnapshot) +
        result.snapshot_->blocks_.capacity() * sizeof(std::string) +
        result.snapshot_->values_.capacity() * sizeof(HashReadSnapshot::Slice);
  } else {
    result.values_.resize(count * width);
    retained = sizeof(HashResult) +
               result.values_.capacity() * sizeof(result.values_[0]);
  }
  if (retained > output->bytes())
    co_return absl::ResourceExhaustedError("OOM Hash output retention");
  result.retained_charge_.Adopt(&*output, retained);
  auto read = [&](GroupedRecordId id, GroupReadLimiter* limiter = nullptr) {
    return LoadHashGroup<HashResult>(
        store, partition, db_id, key, digest, object, id, false,
        [kind, with_values](std::string_view payload, std::uint64_t,
                            std::uint32_t field_count,
                            LoadedValue&&) -> absl::StatusOr<HashResult> {
          if (field_count == 0) return HashResult{};
          // Only final reply bytes survive this callback. In particular HKEYS
          // and SMEMBERS never copy values into an intermediate HashValue.
          auto result =
              ReadHashPayloadResult(payload.substr(kHashGroupHeaderBytes), kind,
                                    field_count, with_values);
          if (!result.ok() && absl::IsInternal(result.status()))
            return absl::DataLossError(result.status().message());
          return result;
        },
        limiter);
  };
  struct Selection {
    GroupedRecordId id_;
    std::size_t offset_;
    std::size_t count_;
  };
  const auto consume = [&](const Selection& selection,
                           HashResult&& page) -> absl::Status {
    if (page.length_ != selection.count_)
      return absl::DataLossError("grouped Hash aggregate mismatch");
    if (selection.count_ == 0) return absl::OkStatus();
    std::size_t payload_bytes = 0;
    if (snapshot) {
      if (!page.snapshot_ || page.snapshot_->blocks_.size() != 1 ||
          page.snapshot_->values_.size() != selection.count_ * width)
        return absl::DataLossError("grouped Hash snapshot aggregate mismatch");
      const auto block = result.snapshot_->blocks_.size();
      payload_bytes = page.snapshot_->blocks_.front().capacity() + 1;
      result.snapshot_->blocks_.push_back(
          std::move(page.snapshot_->blocks_.front()));
      for (std::size_t i = 0; i < page.snapshot_->values_.size(); ++i) {
        auto slice = page.snapshot_->values_[i];
        slice.block_ = block;
        result.snapshot_->values_[selection.offset_ * width + i] = slice;
      }
    } else {
      if (page.values_.size() != selection.count_ * width)
        return absl::DataLossError("grouped Hash aggregate mismatch");
      for (const auto& value : page.values_)
        payload_bytes += value->capacity() + 1;
      std::move(page.values_.begin(), page.values_.end(),
                result.values_.begin() + selection.offset_ * width);
    }
    if (payload_bytes > SIZE_MAX - result.retained_charge_.bytes())
      return absl::ResourceExhaustedError("OOM Hash output is too large");
    // Strings move without another payload allocation. Transfer their charge
    // before child ownership ends; the parent's slot storage was admitted up
    // front and each child's temporary slot array remains charged until join.
    result.retained_charge_.Resize(result.retained_charge_.bytes() +
                                   payload_bytes);
    page.retained_charge_.Resize(page.retained_charge_.bytes() - payload_bytes);
    return absl::OkStatus();
  };
  const auto validate = [&]() -> absl::Status {
    const auto& version = object->version();
    if (EffectiveRecordDbEpoch(partition, db_id) != version.db_epoch_ ||
        partition.replication_epoch_ != version.replication_epoch_ ||
        partition.grouped_generations_[db_id] != version.index_generation_)
      return absl::NotFoundError("grouped population changed during read");
    return object->ReadStatus();
  };
  std::optional<GroupReadPlan> plan;
  std::vector<Selection> selected;
  if (directory.groups().size() <= SIZE_MAX / sizeof(Selection)) {
    plan = GroupReadPlan::TryCreate(
        worker_count_, directory.groups().size(),
        AllocatorUsableSizeForRequest(directory.groups().size() *
                                      sizeof(Selection)));
    if (plan) {
      selected.reserve(directory.groups().size());
      std::size_t offset = 0;
      for (const auto& [prefix, metadata] : directory.groups()) {
        if (metadata.field_count_ > count - offset)
          co_return absl::DataLossError("grouped Hash aggregate mismatch");
        const auto* entry = object->FindGroup(metadata.id_);
        if (entry == nullptr)
          co_return absl::DataLossError("missing Hash group");
        auto owner = BlockOwner(entry->block_id());
        if (owner >= worker_count_) owner = store.worker_->id();
        plan->Add(owner, selected.size());
        selected.push_back({metadata.id_, offset, metadata.field_count_});
        offset += metadata.field_count_;
      }
      if (offset != count)
        co_return absl::DataLossError("grouped Hash aggregate mismatch");
      if (!plan->ReserveWindow<HashResult>(4096)) {
        std::vector<Selection>().swap(selected);
        plan.reset();
      }
    }
  }
  if (!plan) {
    std::size_t offset = 0;
    for (const auto& [prefix, metadata] : directory.groups()) {
      if (metadata.field_count_ > count - offset)
        co_return absl::DataLossError("grouped Hash aggregate mismatch");
      auto loaded = co_await read(metadata.id_);
      if (!loaded.ok()) co_return loaded.status();
      const auto valid = validate();
      if (!valid.ok()) co_return valid;
      const auto consumed = consume(
          {metadata.id_, offset, metadata.field_count_}, std::move(*loaded));
      if (!consumed.ok()) co_return consumed;
      offset += metadata.field_count_;
    }
    if (offset != count)
      co_return absl::DataLossError("grouped Hash aggregate mismatch");
    co_return result;
  }
  // Owner waves may finish out of directory order. Slot offsets preserve the
  // traversal order, and every child releases its physical lease before join.
  GroupReadWindow<HashResult> window(store.worker_, worker_count_,
                                     plan->width());
  while (!plan->empty()) {
    absl::Status start_status;
    for (unsigned owner = 0; owner < plan->owner_count(); ++owner) {
      for (std::size_t n = 0; n < GroupReadLimiter::kWidth; ++n) {
        const auto ordinal = plan->Pop(owner);
        if (!ordinal) break;
        if (window.size() == 1 &&
            LAVIK_FAULT_MATCHES("LAVIK_FAIL_HASH_READ_BATCH_START_KEY", key)) {
          start_status = absl::ResourceExhaustedError(
              "OOM grouped Hash read batch admission");
          break;
        }
        window.Add(read(selected[*ordinal].id_, window.limiter()), *ordinal);
      }
      if (!start_status.ok()) break;
    }
    co_await window.Join();
    const auto valid = validate();
    if (!valid.ok()) co_return valid;
    const auto completed =
        window.Collect([&](std::size_t ordinal, HashResult&& page) {
          return consume(selected[ordinal], std::move(page));
        });
    if (!start_status.ok()) co_return start_status;
    if (!completed.ok()) co_return completed;
  }
  co_return result;
}

Task<absl::StatusOr<StorageEngine::Impl::LoadedValue>>
StorageEngine::Impl::LoadGroupedValue(WorkerStore& store,
                                      WorkerStore::PartitionStore& partition,
                                      std::uint8_t db_id, std::string_view key,
                                      const Digest& digest,
                                      RecordLocation location,
                                      GroupedObject::Handle snapshot) {
  const bool pinned = snapshot != nullptr;
  if (!pinned) {
    auto found = partition.grouped_objects_[db_id].Lookup(
        key, GroupedObjectVersion{
                 .root_ = location,
                 .db_epoch_ = EffectiveRecordDbEpoch(partition, db_id),
                 .replication_epoch_ = partition.replication_epoch_,
                 .index_generation_ = partition.grouped_generations_[db_id],
             });
    if (!found.ok()) co_return found.status();
    snapshot = std::move(*found);
  } else if (!snapshot->version().root_.SamePhysicalRecord(location)) {
    co_return absl::DataLossError("snapshot grouped root identity mismatch");
  }
  if (snapshot == nullptr)
    co_return absl::DataLossError("missing grouped materialization view");
  if (snapshot->is_ordered() && snapshot->ordered_directory().root().kind_ ==
                                    OrderedCollectionKind::kString) {
    co_return co_await LoadGroupedStringValue(
        store, partition, db_id, key, digest, std::move(snapshot), pinned);
  }
  GroupedScratchBudget budget;
  auto include_group = [&](GroupedRecordId id) -> absl::Status {
    const auto* entry = snapshot->FindGroup(id);
    if (entry == nullptr)
      return absl::DataLossError("missing grouped materialization page");
    return budget.AddGroup(*entry, snapshot->ExtentsFor(id));
  };
  if (snapshot->is_ordered()) {
    for (const auto& metadata : snapshot->ordered_directory().groups()) {
      const auto admitted = include_group({metadata.id_, 0});
      if (!admitted.ok()) co_return admitted;
    }
  } else {
    for (const auto& [prefix, metadata] : snapshot->directory().groups()) {
      const auto admitted = include_group(metadata.id_);
      if (!admitted.ok()) co_return admitted;
    }
  }
  // Legacy full-image consumers hold both decoded entries and their encoded
  // image. Reserve before the first page, not after constructing that image.
  // The returned read lease follows the buffer pool's separate lifetime; this
  // temporary reservation ends after materialization, before the network send.
  auto scratch = budget.Reserve(2);
  if (!scratch.ok()) co_return scratch.status();
  absl::StatusOr<std::string> encoded;
  if (snapshot != nullptr && snapshot->is_ordered()) {
    auto value = co_await LoadGroupedOrderedValue(store, partition, db_id, key,
                                                  digest, snapshot, pinned);
    if (!value.ok()) co_return value.status();
    encoded = EncodeOrderedCompactValue(
        snapshot->ordered_directory().root().kind_, *value);
  } else {
    auto value = co_await LoadGroupedHashValue(store, partition, db_id, key,
                                               digest, snapshot, pinned);
    if (!value.ok()) co_return value.status();
    encoded = EncodeHashValue(*value);
  }
  if (!encoded.ok()) co_return encoded.status();
  auto buffer = co_await store.buffers_.AcquireReadBuffer(encoded->size());
  if (!buffer.ok()) co_return buffer.status();
  const auto output = buffer->io_buffer();
  if (output.size_ < encoded->size()) {
    co_return absl::ResourceExhaustedError("grouped output buffer too small");
  }
  std::memcpy(output.data_, encoded->data(), encoded->size());
  const auto offset =
      static_cast<std::size_t>(output.data_ - buffer->bytes().data());
  co_return LoadedValue{std::move(*buffer), offset, encoded->size()};
}

}  // namespace lavik::storage
