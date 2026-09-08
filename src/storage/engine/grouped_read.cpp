#include "impl.h"
#include "keylane/storage/detail/grouped_scratch.h"
#include "keylane/storage/detail/ordered_compact_codec.h"

namespace keylane::storage {

Task<absl::StatusOr<LoadedHashGroup>>
StorageEngine::Impl::LoadHashGroupSnapshot(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    GroupedHashObject::Handle object, HashGroupId id, bool pinned) {
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
          store, partition.indexes_[db_id], digest, key);
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
    auto extents = object->ExtentsFor(id);
    absl::StatusOr<LoadedValue> loaded;
    if (location.external()) {
      loaded = co_await LoadExternalValueLocal(store, location, extents,
                                               key.size(), nullptr);
    } else if (location.block_owner() == store.worker_->id()) {
      loaded = co_await LoadValueLocal(store, db_id, key, location,
                                       original.replication_epoch_, nullptr,
                                       original.db_epoch_);
    } else {
      const unsigned owner = location.block_owner();
      loaded = co_await celer::SubmitTaskTo(
          owner,
          [this, owner, db_id, owned_key = std::string(key), location,
           epoch = original.replication_epoch_,
           db_epoch =
               original.db_epoch_]() -> Task<absl::StatusOr<LoadedValue>> {
            co_return co_await LoadValueLocal(*stores_[owner], db_id, owned_key,
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
      // Reject corrupt counts before the full decoder allocates its vectors.
      if (envelope->incarnation_ != incarnation || envelope->id_ != id ||
          envelope->retired_ ||
          envelope->field_count_ != location.logical_size_) {
        co_return absl::DataLossError("Hash group physical identity mismatch");
      }
      auto decoded = DecodeHashGroup(payload);
      if (!decoded.ok()) co_return decoded.status();
      for (const auto& field : decoded->value_.entries_) {
        if (!id.contains(
                ComputeDigest(field.field_, object->directory().root().seed_)
                    .value_)) {
          co_return absl::DataLossError("Hash field outside its group route");
        }
      }
      co_return LoadedHashGroup{.sequence_ = location.mutation_sequence_,
                                .snapshot_ = std::move(*decoded)};
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

Task<absl::StatusOr<HashValue>> StorageEngine::Impl::LoadGroupedHashValue(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    GroupedHashObject::Handle object, bool pinned) {
  if (object == nullptr) co_return absl::DataLossError("missing grouped view");
  HashValue result;
  for (const auto& [prefix, metadata] : object->directory().groups()) {
    (void)prefix;
    auto loaded = co_await LoadHashGroupSnapshot(
        store, partition, db_id, key, digest, object, metadata.id_, pinned);
    if (!loaded.ok()) co_return loaded.status();
    for (auto& field : loaded->snapshot_.value_.entries_) {
      result.entries_.push_back(std::move(field));
    }
  }
  if (result.entries_.size() != object->directory().root().field_count_) {
    co_return absl::DataLossError("grouped Hash aggregate mismatch");
  }
  co_return result;
}

Task<absl::StatusOr<StorageEngine::Impl::LoadedValue>>
StorageEngine::Impl::LoadGroupedValue(WorkerStore& store,
                                      WorkerStore::PartitionStore& partition,
                                      std::uint8_t db_id, std::string_view key,
                                      const Digest& digest,
                                      RecordLocation location,
                                      GroupedHashObject::Handle snapshot) {
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
  GroupedScratchBudget budget;
  auto include_group = [&](HashGroupId id) -> absl::Status {
    const auto* entry = snapshot->FindGroup(id);
    if (entry == nullptr)
      return absl::DataLossError("missing grouped materialization page");
    return budget.AddGroup(entry->value_, snapshot->ExtentsFor(id), key.size());
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
  // The returned read buffer has its own independent admitted lifetime.
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
    encoded = EncodeHashValue(*value, kMaxRecordPayloadBytes);
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

}  // namespace keylane::storage
