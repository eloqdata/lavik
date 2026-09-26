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

namespace lavik::storage {

Task<absl::StatusOr<HashGroupLocation>>
StorageEngine::Impl::WriteOrderedGroupRecordLocked(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const OrderedGroupSnapshot& snapshot, OrderedGroupEncoder encoder,
    std::uint64_t revision, TxShardWrites& tx, std::uint64_t batch_txid) {
  // A page owns a complete snapshot. Its cursor borrows the entries until
  // every extent is durable; only then may the manifest enter the root batch.
  // In particular a single large List item is never split into a read-time
  // operation log, nor copied into another equally large serialization buffer.
  const bool key_indirect = key.size() > kInlineKeyMaxBytes;
  if (!ValidRecordKeySize(key.size()) ||
      encoder.encoded_bytes() > kMaxRecordPayloadBytes) {
    co_return absl::OutOfRangeError(
        "ordered page exceeds record payload limit");
  }
  const std::size_t inline_bytes = AlignRecord(
      RecordHeaderBytes(key.size(), key_indirect, true, false, true) +
      encoder.encoded_bytes());
  const bool external = inline_bytes > kStorageBlockBytes - kBlockHeaderBytes ||
                        inline_bytes > options_.buffers_.write_buffer_bytes_;
  ExtentManifest extents;
  std::string payload;
  if (external) {
    RecordPayloadCursor cursor(encoder, std::string_view{});
    auto written = co_await WriteExtentValueLocked(store, {}, {}, &cursor, key);
    if (!written.ok()) co_return written.status();
    extents = std::move(*written);
    payload = EncodeManifest(*extents);
    LAVIK_MAYBE_CRASH_AT("group-extents-durable-before-record");
  } else {
    payload.resize(encoder.encoded_bytes());
    RecordPayloadCursor cursor(encoder);
    auto status = cursor.Read(std::as_writable_bytes(std::span(payload)));
    if (status.ok()) status = cursor.Finish();
    if (!status.ok()) co_return status;
  }
  const HashGroupId id{snapshot.id_, 0};
  const GroupRecordWrite identity{
      .auxiliary_ = true,
      .incarnation_ = snapshot.incarnation_,
      .id_ = id,
      .retired_ = snapshot.retired_,
      .batch_txid_ = batch_txid,
      .prepare_root_ = {},
      .changed_groups_ = {},
  };
  const ValueType type = OrderedValueType(snapshot.kind_);
  RecordLocation location;
  auto status = co_await WriteRecordLocked(
      store, db_id, key, payload, RecordKind::kValue, type, 0, digest, tx.txid_,
      revision, false, true, external, key_indirect, OrderedGroupSize(snapshot),
      extents, &location, nullptr, &tx, nullptr, nullptr, nullptr, nullptr,
      &partition, &identity);
  if (!status.ok()) {
    if (extents != nullptr) SpawnExtentReclaim(store, extents);
    co_return status;
  }
  LAVIK_MAYBE_CRASH_AT("group-record-staged-before-root");
  co_return HashGroupLocation{.id_ = id,
                              .location_ = location,
                              .extents_ = std::move(extents),
                              .retired_ = snapshot.retired_};
}

Task<absl::StatusOr<LoadedOrderedGroup>>
StorageEngine::Impl::LoadOrderedGroupSnapshot(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    GroupedHashObject::Handle object, std::uint64_t page_id, bool pinned) {
  if (object == nullptr || !object->is_ordered()) {
    co_return absl::DataLossError("missing ordered collection view");
  }
  const auto original = object->version();
  const auto root = object->ordered_directory().root();
  const HashGroupId id{page_id, 0};
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
          store, partition.indexes_[db_id], digest, key);
      if (!resolved.ok()) co_return resolved.status();
      if (*resolved == nullptr || !(*resolved)->value_.grouped() ||
          (*resolved)->value_.mutation_sequence_ !=
              original.root_.mutation_sequence_) {
        co_return absl::NotFoundError("ordered root changed during read");
      }
      auto current = partition.grouped_objects_[db_id].Lookup(
          key, GroupedObjectVersion{
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
    absl::StatusOr<LoadedValue> loaded;
    if (location.external()) {
      loaded = co_await LoadExternalValueLocal(store, location, extents,
                                               key.size(), nullptr, true);
    } else if (location.block_owner() == store.worker_->id()) {
      loaded = co_await LoadValueLocal(store, db_id, key, location,
                                       original.replication_epoch_, nullptr,
                                       original.db_epoch_);
    } else {
      const unsigned owner = location.block_owner();
      loaded = co_await bycorf::SubmitTaskTo(
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
      auto decoded = DecodeOrderedGroup(payload);
      if (!decoded.ok()) co_return decoded.status();
      if (root.kind_ == OrderedCollectionKind::kSortedSet &&
          (decoded->entries_.front().score_ != route->min_score_ ||
           decoded->entries_.back().score_ != route->max_score_))
        co_return absl::DataLossError("ordered page score bounds mismatch");
      co_return LoadedOrderedGroup{.sequence_ = location.mutation_sequence_,
                                   .snapshot_ = std::move(*decoded)};
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

Task<absl::StatusOr<std::vector<OrderedCollectionEntry>>>
StorageEngine::Impl::LoadGroupedOrderedValue(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    GroupedHashObject::Handle object, bool pinned) {
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

}  // namespace lavik::storage
