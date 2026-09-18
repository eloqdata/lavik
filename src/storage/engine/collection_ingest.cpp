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
namespace {

using IngestPhysical = std::tuple<std::uint64_t, std::uint64_t, std::uint32_t>;

// The generation lease is intentionally opaque. Extending its owner with an
// admitted journal charge keeps merged outer receipts accounted until their
// actual commit/abort owner releases them, without retaining the ingest itself.
struct IngestReceiptLease {
  RetainedMemoryCharge charge_;
  std::shared_ptr<void> generation_;
};

template <typename T>
absl::Status ReserveIngestVector(std::vector<T>& values, std::size_t required,
                                 RetainedMemoryCharge& charge) {
  if (required <= values.capacity()) return absl::OkStatus();
  const auto doubled =
      values.capacity() <= std::numeric_limits<std::size_t>::max() / 2
          ? values.capacity() * 2
          : required;
  const auto capacity = std::max(required, doubled);
  if (capacity >
      (std::numeric_limits<std::size_t>::max() - charge.bytes()) / sizeof(T))
    return absl::ResourceExhaustedError("collection ingest capacity overflow");
  const auto bytes = capacity * sizeof(T);
  auto reservation = TryReserveMemory(bytes);
  if (!reservation) {
    RecordMemoryRejection();
    return absl::ResourceExhaustedError("OOM collection ingest metadata");
  }
  values.reserve(capacity);
  charge.Resize(charge.bytes() + bytes);
  return absl::OkStatus();
}

template <typename T>
void MoveIngestVector(std::vector<T>& destination, std::vector<T>& source) {
  for (auto& item : source) destination.push_back(std::move(item));
  source.clear();
}

}  // namespace

Task<absl::StatusOr<RestoreRawResult>> StorageEngine::RestoreCollectionValue(
    std::uint8_t db_id, std::string_view key, ValueType type,
    std::uint64_t expire_at_ms, bool replace,
    std::optional<std::uint64_t> expected_items, CollectionPageReader reader,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  return impl_->RestoreCollectionValue(db_id, key, type, expire_at_ms, replace,
                                       expected_items, std::move(reader),
                                       replication, mutation_precondition);
}

Task<absl::StatusOr<RestoreRawResult>>
StorageEngine::RestoreCollectionValueLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    ValueType type, std::uint64_t expire_at_ms, bool replace,
    std::optional<std::uint64_t> expected_items, CollectionPageReader reader,
    TxShardWrites* tx, ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  return impl_->RestoreCollectionValueLocked(
      db_id, key, digest, type, expire_at_ms, replace, expected_items,
      std::move(reader), tx, replication, mutation_precondition);
}

Task<absl::StatusOr<RestoreRawResult>>
StorageEngine::Impl::RestoreCollectionValue(
    std::uint8_t db_id, std::string_view key, ValueType type,
    std::uint64_t expire_at_ms, bool replace,
    std::optional<std::uint64_t> expected_items, CollectionPageReader reader,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  const auto digest = ComputeDigest(key);
  auto hold = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest), tx::LockMode::kExclusive);
  co_return co_await RestoreCollectionValueLocked(
      db_id, key, digest, type, expire_at_ms, replace, expected_items,
      std::move(reader), nullptr, replication, mutation_precondition);
}

Task<absl::StatusOr<RestoreRawResult>>
StorageEngine::Impl::RestoreCollectionValueLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    ValueType type, std::uint64_t expire_at_ms, bool replace,
    std::optional<std::uint64_t> expected_items, CollectionPageReader reader,
    TxShardWrites* outer, ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  if (db_id >= options_.database_count_ || digest != ComputeDigest(key) ||
      !reader ||
      (type != ValueType::kHash && type != ValueType::kSet &&
       type != ValueType::kList && type != ValueType::kSortedSet) ||
      (expected_items &&
       *expected_items > std::numeric_limits<std::uint32_t>::max()))
    co_return absl::InvalidArgumentError("invalid collection restore input");
  const auto metadata = co_await ReadKeyMetadataLocked(db_id, key, digest);
  if (!metadata.ok()) co_return metadata.status();
  const bool exists = metadata->exists_;
  if (exists && !replace) co_return RestoreRawResult{.busy_ = true};
  if (expire_at_ms != 0 && expire_at_ms <= UnixTimeMillis()) {
    if (!exists) co_return RestoreRawResult{};
    auto deleted = co_await DeleteLocked(db_id, key, digest, outer, replication,
                                         mutation_precondition);
    if (!deleted.ok()) co_return deleted.status();
    co_return RestoreRawResult{.changed_ = *deleted, .deleted_ = *deleted};
  }

  WorkerStore& store = CurrentStore();
  auto& partition = PartitionForKey(store, key);
  if (key.size() > std::numeric_limits<std::size_t>::max() - 4096)
    co_return absl::ResourceExhaustedError("collection key size overflow");
  auto initial_admission = TryReserveMemory(4096 + key.size());
  if (!initial_admission) {
    RecordMemoryRejection();
    co_return absl::ResourceExhaustedError("OOM collection ingest state");
  }
  auto state = std::make_shared<ReplicaCollectionStage>();
  auto receipt_owner = std::make_shared<IngestReceiptLease>();
  state->decoded_charge_.Account(CurrentMemoryAccountingShard(), 0);
  state->undo_charge_.Account(CurrentMemoryAccountingShard(),
                              initial_admission->bytes());
  auto& writes = state->writes_;
  const bool previous_collect_undo = outer != nullptr && outer->collect_undo_;
  const auto previous_retirements =
      outer != nullptr ? outer->retirements_.size() : 0;
  const auto previous_dataset_changes =
      outer != nullptr ? outer->dataset_changes_ : 0;
  ReplicaValueStage stage{.db_id_ = db_id,
                          .expire_at_ms_ = expire_at_ms,
                          .value_type_ = type,
                          .key_ = std::string(key),
                          .value_ = {},
                          .collection_ = state,
                          .memory_charge_ = {}};
  initial_admission.reset();
  if (outer) {
    if (outer->grouped_ingest_batch_ != nullptr)
      co_return absl::FailedPreconditionError("nested collection restore");
    const auto decision = PrepareGroupedDecision(*outer);
    if (!decision.ok()) co_return decision.status();
    // Transaction callbacks are serial on each owner. Temporarily owning the
    // whole accumulator preserves its prefix/capacity and permits noexcept
    // return on abort; no vector merge may allocate on an OOM rollback path.
    // Page readers must not reenter this same owner/transaction accumulator.
    writes = std::move(*outer);
    if (mutation_precondition != nullptr) {
      writes.mutation_precondition_ = *mutation_precondition;
    }
  } else {
    // The key intent is held, but no transaction lease or store mutex is held
    // yet. Ingest batches later borrow this accumulator and must not attempt
    // to clean their own still-uncommitted generation between input pages.
    co_await store.store_state_mutex_.Lock();
    const auto predecessor = co_await AwaitGroupedDependencyLocked(
        store, partition.grouped_objects_[db_id].CurrentForMutation(key), 0);
    store.store_state_mutex_.Unlock(*store.worker_);
    if (!predecessor.ok()) co_return predecessor;
    const auto space = co_await BeforeGroupedTransaction(store, 1U << 20);
    if (!space.ok()) co_return space;
    InitializeTxWrites(tx::TxRuntime::Get()->next_txid_.fetch_add(
                           1, std::memory_order_relaxed),
                       std::span(&writes, 1),
                       mutation_precondition != nullptr
                           ? *mutation_precondition
                           : MutationPrecondition{});
  }
  // Even an unexpected allocator exception in compensation must return the
  // borrowed accumulator. Such an exception is fail-stop, not a successful
  // command rollback; the shared decision prevents the caller committing it.
  struct ReturnAccumulator {
    WorkerStore& store_;
    TxShardWrites& writes_;
    TxShardWrites* outer_;
    bool collect_undo_;
    RetainedMemoryCharge& charge_;
    std::shared_ptr<IngestReceiptLease> owner_;
    bool completed_ = false;
    ~ReturnAccumulator() {
      if (!completed_) {
        store_.write_failed_ = true;
        if (writes_.grouped_decision_) writes_.grouped_decision_->FailPending();
      }
      writes_.collect_undo_ = collect_undo_;
      writes_.grouped_ingest_batch_ = nullptr;
      owner_->charge_ = std::move(charge_);
      if (outer_ != nullptr) *outer_ = std::move(writes_);
    }
  } return_accumulator{
      store,        writes, outer, previous_collect_undo, state->undo_charge_,
      receipt_owner};
  writes.collect_undo_ = true;
  receipt_owner->generation_ = writes.generation_lease_;
  writes.generation_lease_ = receipt_owner;
  TxShardWrites batch;
  batch.txid_ =
      tx::TxRuntime::Get()->next_txid_.fetch_add(1, std::memory_order_relaxed);
  batch.generation_ = writes.generation_;
  batch.generation_lease_ = writes.generation_lease_;
  const auto batch_decision = PrepareGroupedDecision(batch);
  if (!batch_decision.ok()) {
    return_accumulator.completed_ = true;
    co_return batch_decision.status();
  }
  writes.grouped_ingest_batch_ = &batch;
  TxUndoLog prefix;
  RecordIndex::Entry* prefix_address = nullptr;
  std::vector<IngestPhysical> restored_groups;
  std::optional<MemoryReservation> rollback_allowance;
  bool isolated = false;
  bool decision_committed = false;
  bool joined = false;
  std::size_t prefix_size = 0;

  // Driver and settlement are kept separate: page buffers and parser scratch
  // die when run() returns, before the reserved rollback headroom is released.
  auto run = [&]() -> Task<absl::Status> {
    co_await store.store_state_mutex_.Lock();
    UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
    auto found = co_await FindVerifiedEntry(store, partition.indexes_[db_id],
                                            digest, key);
    if (!found.ok()) co_return found.status();
    prefix_address = *found;
    const auto old = partition.grouped_objects_[db_id].CurrentForMutation(key);
    const auto old_records = old ? old->record_count() : 0;
    auto reserved =
        ReserveIngestVector(restored_groups, old_records, state->undo_charge_);
    if (!reserved.ok()) co_return reserved;
    constexpr auto width = 4 * sizeof(RecoveredOrderedGroup) +
                           4 * sizeof(RecoveredHashGroup) +
                           4 * sizeof(HashGroupId);
    constexpr auto maximum = std::numeric_limits<std::size_t>::max();
    if (key.size() > (maximum - 16384) / 4 ||
        old_records > (maximum - 16384 - key.size() * 4) / width)
      co_return absl::ResourceExhaustedError(
          "collection rollback size overflow");
    std::size_t compensation_bytes =
        16384 + key.size() * 4 + old_records * width;
    if (*found != nullptr && !(*found)->value_.grouped()) {
      const auto location = MaterializeIndexLocation(**found);
      std::uint64_t payload = location.total_disk_bytes();
      if (location.external()) {
        payload = 0;
        if (auto extents = ExtentsFor(store, *found); extents)
          for (const auto& extent : *extents) payload += extent.payload_bytes_;
      }
      if (payload > (maximum - compensation_bytes) / 2)
        co_return absl::ResourceExhaustedError(
            "collection rollback size overflow");
      compensation_bytes += payload * 2;
    }
    rollback_allowance = TryReserveMemory(compensation_bytes);
    if (!rollback_allowance) {
      RecordMemoryRejection();
      co_return absl::ResourceExhaustedError(
          "OOM collection rollback headroom");
    }
    auto& journal = store.tx_undo_[writes.txid_];
    auto prefix_admission = TryReserveMemory(
        8192 + journal.entries_.size() * sizeof(TxUndoEntry) * 4);
    if (!prefix_admission) {
      RecordMemoryRejection();
      co_return absl::ResourceExhaustedError("OOM collection undo prefix");
    }
    journal.ReserveOneEntry();
    state->undo_charge_.Resize(state->undo_charge_.bytes() +
                               prefix_admission->bytes());
    prefix_size = journal.entries_.size();
    prefix = std::move(journal);
    journal = TxUndoLog{};
    // Squash replaces only its rolling receipt-vector charge. Keep the key,
    // suspended prefix and restored-identity table as its fixed retained base.
    state->undo_overhead_bytes_ = state->undo_charge_.bytes();
    isolated = true;
    unlock.Unlock();

    RetainedMemoryCharge merge_charge;
    merge_charge.Account(CurrentMemoryAccountingShard(), 0);
    std::vector<RetainedMemoryCharge> input_charges;
    CollectionPage merged{.value_type_ = type};
    std::uint64_t merged_bytes = 0;
    bool first_write = true;
    auto flush = [&]() -> Task<absl::Status> {
      if (merged.size() == 0) co_return absl::OkStatus();
      const auto count = merged.size();
      if (count >
          std::numeric_limits<std::uint32_t>::max() - state->applied_count_)
        co_return absl::DataLossError("collection cardinality overflow");
      absl::Status written;
      if (type == ValueType::kSortedSet && state->applied_count_ != 0) {
        std::vector<ScoredMemberView> entries;
        auto admitted = ReserveIngestVector(entries, count, merge_charge);
        if (!admitted.ok()) co_return admitted;
        for (const auto& item : merged.scored_members_)
          entries.push_back({item.member_, item.score_});
        co_await store.store_state_mutex_.Lock();
        UnlockGuard write_unlock(&store.store_state_mutex_, store.worker_);
        const auto current =
            partition.grouped_objects_[db_id].CurrentForMutation(key);
        auto added = co_await ExecuteGroupedSortedSetLocked(
            store, partition, db_id, key, digest,
            SortedSetOperation{.kind_ = SortedSetOperationKind::kAdd,
                               .entries_ = entries,
                               .reject_existing_ = true},
            current, &writes, nullptr);
        written = added.ok() ? SquashReplicaCollectionUndo(store, *state)
                             : added.status();
      } else {
        if (type == ValueType::kSortedSet)
          std::sort(merged.scored_members_.begin(),
                    merged.scored_members_.end(),
                    [](const auto& left, const auto& right) {
                      return std::tie(left.score_, left.member_) <
                             std::tie(right.score_, right.member_);
                    });
        written = co_await WriteReplicaCollectionPage(store, partition, stage,
                                                      std::move(merged));
      }
      merged = CollectionPage{.value_type_ = type};
      input_charges.clear();
      merge_charge.Resize(input_charges.capacity() *
                          sizeof(RetainedMemoryCharge));
      merged_bytes = 0;
      if (!written.ok()) co_return written;
      state->applied_count_ += count;
      if (first_write) {
        co_await store.store_state_mutex_.Lock();
        UnlockGuard journal_unlock(&store.store_state_mutex_, store.worker_);
        const auto& original = store.tx_undo_.at(writes.txid_).entries_.front();
        if (original.previous_grouped_retirements_) {
          if (original.previous_grouped_retirements_->size() >
              restored_groups.capacity())
            co_return absl::InternalError(
                "collection predecessor grew under key lock");
          for (const auto& item : *original.previous_grouped_retirements_)
            restored_groups.emplace_back(item.block_id_, item.allocation_epoch_,
                                         item.record_offset_);
          std::sort(restored_groups.begin(), restored_groups.end());
        }
        first_write = false;
      }
      co_return absl::OkStatus();
    };

    bool done = false;
    while (!done) {
      auto page = co_await reader();
      if (!page.ok()) co_return page.status();
      if (page->value_type_ != type)
        co_return absl::DataLossError("collection input changes type");
      auto bytes = CollectionCompactEncoder::MeasurePage(*page);
      if (!bytes.ok()) co_return bytes.status();
      done = page->done_;
      if (page->size() == 0) continue;
      if (merged.size() != 0 && *bytes > 1024 * 1024 - merged_bytes) {
        auto written = co_await flush();
        if (!written.ok()) co_return written;
      }
      auto append = [&](auto& destination, auto& source) {
        auto admitted = ReserveIngestVector(
            destination, destination.size() + source.size(), merge_charge);
        if (admitted.ok()) MoveIngestVector(destination, source);
        return admitted;
      };
      absl::Status merged_status;
      if (type == ValueType::kHash)
        merged_status = append(merged.fields_, page->fields_);
      else if (type == ValueType::kSortedSet)
        merged_status = append(merged.scored_members_, page->scored_members_);
      else
        merged_status = append(merged.elements_, page->elements_);
      if (!merged_status.ok()) co_return merged_status;
      auto admitted = ReserveIngestVector(
          input_charges, input_charges.size() + 1, merge_charge);
      if (!admitted.ok()) co_return admitted;
      input_charges.push_back(std::move(page->retained_charge_));
      merged_bytes += *bytes;
      if (merged_bytes >= 1024 * 1024 || done) {
        auto written = co_await flush();
        if (!written.ok()) co_return written;
      }
    }
    auto written = co_await flush();
    if (!written.ok()) co_return written;
    if (state->applied_count_ == 0 ||
        (expected_items && *expected_items != state->applied_count_))
      co_return absl::DataLossError("collection EOF cardinality mismatch");
    if (replication != nullptr) {
      co_await store.store_state_mutex_.Lock();
      UnlockGuard metadata_unlock(&store.store_state_mutex_, store.worker_);
      written = co_await UpdateGroupedExpirationLocked(
          store, partition, db_id, key, digest,
          partition.grouped_objects_[db_id].CurrentForMutation(key),
          expire_at_ms, &writes, replication);
      if (!written.ok()) co_return written;
      written = SquashReplicaCollectionUndo(store, *state);
      if (!written.ok()) co_return written;
    }
    co_return absl::OkStatus();
  };

  absl::Status status;
  try {
    status = co_await run();
  } catch (const std::bad_alloc&) {
    RecordMemoryRejection();
    status = absl::ResourceExhaustedError("OOM collection restore");
  }
  // No source page remains live now. Prepare the journal join and batch fence
  // before committing the independent command decision; all later ownership
  // transfers are moves into capacity admitted before the first mutation.
  if (status.ok()) {
    try {
      co_await store.store_state_mutex_.Lock();
      UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
      auto& suffix = store.tx_undo_.at(writes.txid_);
      if (suffix.entries_.size() != 1) {
        status = absl::InternalError("collection restore lost squashed undo");
      } else {
        auto* current = suffix.Current(suffix.entries_.front().entry_handle_);
        if (prefix_address != nullptr)
          prefix.NoAllocReplace(prefix_address, current);
        prefix_address = current;
        if (previous_collect_undo) {
          auto original = suffix.entries_.front();
          const auto handle = prefix.Track(current);
          if (!handle)
            status =
                absl::ResourceExhaustedError("collection prefix handle limit");
          else {
            original.entry_handle_ = *handle;
            prefix.entries_.push_back(std::move(original));
            joined = true;
          }
        }
      }
      if (status.ok()) {
        status = ReserveIngestVector(batch.fences_, writes.fences_.size(),
                                     state->undo_charge_);
        if (status.ok())
          batch.fences_.assign(writes.fences_.begin(), writes.fences_.end());
      }
    } catch (const std::bad_alloc&) {
      status =
          absl::ResourceExhaustedError("OOM collection commit preparation");
    }
  }
  if (status.ok()) {
    writes.grouped_ingest_batch_ = nullptr;
    writes.dataset_changes_ = previous_dataset_changes + 1;
    status = co_await CommitTxWrites(batch.txid_, {&batch});
    if (status.ok()) {
      decision_committed = true;
      LAVIK_MAYBE_CRASH_AT("group-batch-durable-before-outer-decision");
      if (outer == nullptr)
        status = co_await CommitTxWrites(writes.txid_, {&writes});
    }
    if (!status.ok()) {
      // A commit IO error has an uncertain durable outcome. Never acknowledge
      // success or let an enclosing transaction commit after this boundary.
      store.write_failed_ = true;
      if (writes.grouped_decision_) writes.grouped_decision_->FailPending();
    }
  }

  if (isolated && !status.ok() && !decision_committed && !store.write_failed_) {
    (*batch_decision)->FailPending();
    writes.grouped_ingest_batch_ = nullptr;
    writes.collect_undo_ = false;
    if (joined) {
      prefix.entries_.resize(prefix_size);
      joined = false;
    }
    // The reserved credit was held during every page allocation. Releasing it
    // immediately before root-only compensation, after page destruction,
    // guarantees that helper's first metadata admission can consume it.
    rollback_allowance.reset();
    const auto rolled_back = co_await RollbackTxLocal(
        writes.txid_, outer != nullptr ? &writes : nullptr,
        /*discard_uncommitted_absent=*/outer == nullptr, &prefix,
        /*grouped_root_only=*/outer != nullptr);
    if (!rolled_back.ok()) {
      store.write_failed_ = true;
      if (writes.grouped_decision_) writes.grouped_decision_->FailPending();
      status = rolled_back;
    } else if (outer != nullptr) {
      // Root-only compensation reuses the original auxiliaries. Cancel only
      // this restore's retirement suffix, never prior commands' receipts.
      // All failed new records still retire through the eventual outer fence.
      std::size_t kept = previous_retirements;
      for (std::size_t i = previous_retirements; i < writes.retirements_.size();
           ++i) {
        const auto& record = writes.retirements_[i];
        const bool restored = std::binary_search(
            restored_groups.begin(), restored_groups.end(),
            IngestPhysical{record.block_id_, record.allocation_epoch_,
                           record.record_offset_});
        if (!restored) {
          if (kept != i)
            writes.retirements_[kept] = std::move(writes.retirements_[i]);
          ++kept;
          continue;
        }
        if (record.dependency_pinned_) {
          const auto owner = record.block_owner_;
          auto release =
              [this, owner, block = record.block_id_,
               epoch = record.allocation_epoch_]() -> Task<absl::Status> {
            auto& physical = *stores_[owner];
            co_await physical.store_state_mutex_.Lock();
            UnlockGuard unlock(&physical.store_state_mutex_, physical.worker_);
            UnpinTxDependencyLocal(physical, block, epoch);
            co_return absl::OkStatus();
          };
          if (owner == store.worker_->id())
            (void)co_await release();
          else
            (void)co_await bycorf::SubmitTaskTo(owner, release);
        }
      }
      writes.retirements_.resize(kept);
      writes.dataset_changes_ = previous_dataset_changes;
    } else {
      for (const auto& receipt : writes.retirements_) {
        if (!receipt.aborted_auxiliary_) continue;
        const auto dead = co_await MarkRecordDead(
            RetiredRecord{.block_id_ = receipt.block_id_,
                          .allocation_epoch_ = receipt.allocation_epoch_,
                          .total_disk_bytes_ = receipt.total_disk_bytes_,
                          .block_owner_ = receipt.block_owner_,
                          .record_offset_ = receipt.record_offset_,
                          .tx_tagged_ = receipt.tx_tagged_,
                          .dependent_extents_ = receipt.dependent_extents_,
                          .immediate_extents_ = receipt.immediate_extents_,
                          .extra_dependent_extents_ = {},
                          .retained_owner_ = receipt.retained_owner_});
        if (!dead.ok()) {
          status = dead;
          store.write_failed_ = true;
          break;
        }
      }
    }
    // Rollback put the prefix into its existing map slot. Its cached Entry
    // address may have changed while the suffix was isolated.
    co_await store.store_state_mutex_.Lock();
    UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
    auto current = co_await FindVerifiedEntry(store, partition.indexes_[db_id],
                                              digest, key);
    auto& returned = store.tx_undo_.at(writes.txid_);
    if (current.ok() && *current != nullptr && prefix_address != nullptr)
      returned.NoAllocReplace(prefix_address, *current);
    if (returned.entries_.empty()) store.tx_undo_.erase(writes.txid_);
    isolated = false;
  }
  if (isolated) {
    co_await store.store_state_mutex_.Lock();
    UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
    auto& slot = store.tx_undo_.at(writes.txid_);
    TxUndoLog suffix = std::move(slot);
    slot = std::move(prefix);
    auto cleared = co_await ClearGroupedUndoSlots(store, suffix);
    if (!cleared.ok()) {
      store.write_failed_ = true;
      status = cleared;
      if (writes.grouped_decision_) writes.grouped_decision_->FailPending();
    }
    if (store.tx_undo_.at(writes.txid_).entries_.empty())
      store.tx_undo_.erase(writes.txid_);
  }
  if (status.ok() && outer == nullptr) PublishCommittedFullSyncEffects(&writes);
  return_accumulator.completed_ = true;
  if (!status.ok()) co_return status;
  co_return RestoreRawResult{.changed_ = true};
}

}  // namespace lavik::storage
