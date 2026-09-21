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

absl::Status StorageEngine::Impl::SquashReplicaCollectionUndo(
    WorkerStore& store, ReplicaCollectionStage& state) {
  auto found = store.tx_undo_.find(state.writes_.txid_);
  if (found == store.tx_undo_.end() || found->second.entries_.empty())
    return absl::InternalError("replica collection write lost its undo");
  auto& entries = found->second.entries_;
  if (entries.size() == 1) return absl::OkStatus();
  if (entries.size() != 2 ||
      entries.front().entry_handle_ != entries.back().entry_handle_ ||
      !entries.back().previous_)
    return absl::InternalError("replica collection undo changed key identity");
  auto& original = entries.front();
  const auto& latest = entries.back();
  const auto count = [](const auto& records) {
    return records ? records->size() : std::size_t{0};
  };
  const auto applied_count = count(original.applied_grouped_retirements_) +
                             count(latest.applied_grouped_retirements_) + 1;
  const auto pin_count = count(original.previous_grouped_retirements_) +
                         count(latest.previous_grouped_retirements_) +
                         (latest.previous_dependency_pinned_ ? 1 : 0);
  if (applied_count > std::numeric_limits<std::size_t>::max() - pin_count ||
      applied_count + pin_count >
          (std::numeric_limits<std::size_t>::max() - 1024) /
              (2 * sizeof(RetiredRecord)))
    return absl::ResourceExhaustedError("replica undo squash size overflow");
  const auto bytes =
      1024 + 2 * sizeof(RetiredRecord) * (applied_count + pin_count);
  if (bytes >
      std::numeric_limits<std::size_t>::max() - state.undo_overhead_bytes_)
    return absl::ResourceExhaustedError("replica undo charge overflow");
  auto admission = TryReserveMemory(bytes);
  if (!admission) {
    RecordMemoryRejection();
    return absl::ResourceExhaustedError("OOM replica undo squash admission");
  }
  auto applied = std::make_shared<std::vector<RetiredRecord>>();
  auto pins = std::make_shared<std::vector<RetiredRecord>>();
  applied->reserve(applied_count);
  pins->reserve(pin_count);
  const auto append = [](auto& destination, const auto& source) {
    if (source)
      destination.insert(destination.end(), source->begin(), source->end());
  };
  append(*applied, original.applied_grouped_retirements_);
  append(*applied, latest.applied_grouped_retirements_);
  auto intermediate = RetiredRecordOf(
      *latest.previous_,
      latest.previous_->key_external() ? latest.previous_extents_ : nullptr);
  applied->push_back(intermediate);
  append(*pins, original.previous_grouped_retirements_);
  append(*pins, latest.previous_grouped_retirements_);
  if (latest.previous_dependency_pinned_) {
    intermediate.dependency_pinned_ = true;
    pins->push_back(std::move(intermediate));
  }
  // New physical auxiliaries and intermediate roots appear once in the abort
  // list. The other list only releases actual predecessor pins; the original
  // graph must survive an abort. The stable undo handle continues naming the
  // current root after removing the intermediate directory's last undo owner.
  original.applied_grouped_retirements_ = std::move(applied);
  original.previous_grouped_retirements_ = std::move(pins);
  entries.pop_back();
  state.undo_charge_.Resize(state.undo_overhead_bytes_ + bytes);
  return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::AbortReplicaValueStage(
    WorkerStore& store, WorkerStore::PartitionStore& partition) {
  if (!partition.replica_value_stage_) co_return absl::OkStatus();
  auto state = partition.replica_value_stage_->collection_;
  if (state && !state->settled_) {
    if (!state->skip_) {
      if (state->writes_.grouped_decision_)
        state->writes_.grouped_decision_->FailPending();
      auto rolled_back = co_await RollbackTxLocal(
          state->writes_.txid_, nullptr, /*discard_uncommitted_absent=*/true);
      if (!rolled_back.ok()) {
        store.write_failed_ = true;
        co_return rolled_back;
      }
      for (const auto& receipt : state->writes_.retirements_) {
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
        if (!dead.ok()) co_return dead;
      }
    }
    state->settled_ = true;
    state->key_hold_.Reset();
  }
  partition.replica_value_stage_.reset();
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::BeginReplicaCollection(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    ReplicaValueStage& stage) {
  auto state = std::make_shared<ReplicaCollectionStage>();
  state->decoded_charge_.Account(CurrentMemoryAccountingShard(), 0);
  state->undo_charge_.Account(CurrentMemoryAccountingShard(), 0);
  auto decoder = CollectionCompactDecoder::Create(
      stage.value_type_, stage.logical_size_, stage.encoded_size_,
      [owner = state.get()](std::size_t bytes) {
        if (bytes > std::numeric_limits<std::size_t>::max() -
                        owner->decoded_charge_.bytes())
          return absl::ResourceExhaustedError(
              "replica collection page admission overflow");
        auto reservation = TryReserveMemory(bytes);
        if (!reservation) {
          RecordMemoryRejection();
          return absl::ResourceExhaustedError(
              "OOM replica collection page admission failed");
        }
        owner->decoded_charge_.Resize(owner->decoded_charge_.bytes() + bytes);
        return absl::OkStatus();
      });
  if (!decoder.ok()) co_return decoder.status();
  state->decoder_.emplace(std::move(*decoder));
  const auto digest = ComputeDigest(stage.key_);
  state->key_hold_ = co_await tx::CurrentTxShard().AcquireKey(
      stage.db_id_, tx::FingerprintOf(digest), tx::LockMode::kExclusive);
  co_await store.store_state_mutex_.Lock();
  UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
  auto current = co_await FindVerifiedEntry(
      store, partition.indexes_[stage.db_id_], digest, stage.key_);
  if (!current.ok()) co_return current.status();
  state->skip_ = *current != nullptr && (*current)->value_.mutation_sequence_ >=
                                            stage.mutation_sequence_;
  // A superseded snapshot still passes full wire framing/count/length checks,
  // but never becomes a logical object. Do not build a global uniqueness
  // index or read the newer object's pages to validate discarded members;
  // exact duplicate/order checks belong to the normal ingestion path below.
  if (!state->skip_) {
    const auto txid = tx::TxRuntime::Get()->next_txid_.fetch_add(
        1, std::memory_order_relaxed);
    InitializeTxWrites(txid, std::span(&state->writes_, 1),
                       MutationPrecondition{});
    state->writes_.collect_undo_ = true;
  }
  stage.collection_ = std::move(state);
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::ConsumeReplicaCollection(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    ReplicaValueStage& stage, std::string_view input, bool finish) {
  auto state = stage.collection_;
  if (!state || state->settled_ || !state->decoder_)
    co_return absl::FailedPreconditionError(
        "replica collection stream is not active");
  if (input.size() > stage.encoded_size_ - state->received_bytes_)
    co_return absl::InvalidArgumentError("replica collection stream overrun");
  state->received_bytes_ += input.size();
  // Decoder pages bound parsing memory, not transaction size. Coalesce only
  // complete entries into a bounded write batch so a many-small-item object
  // does not require one durable child decision per 8 KiB physical page.
  // The decoder receipts continue owning moved strings until the batch dies;
  // this separate charge covers extra vector capacity admitted before growth.
  constexpr std::size_t kWriteBatchBytes = 1024 * 1024;
  RetainedMemoryCharge merge_charge;
  merge_charge.Account(CurrentMemoryAccountingShard(), 0);
  CollectionPage batch{.value_type_ = stage.value_type_};
  std::size_t batch_admission = 0;
  std::uint64_t batch_bytes = 0;
  auto flush_batch = [&]() -> Task<absl::Status> {
    if (batch.size() == 0) co_return absl::OkStatus();
    const auto count = batch.size();
    const auto written = co_await WriteReplicaCollectionPage(
        store, partition, stage, std::move(batch));
    batch = CollectionPage{.value_type_ = stage.value_type_};
    merge_charge.Resize(0);
    state->decoded_charge_.Resize(state->decoded_charge_.bytes() -
                                  batch_admission);
    batch_admission = 0;
    batch_bytes = 0;
    if (written.ok()) state->applied_count_ += count;
    co_return written;
  };
  auto merge = [&](auto& destination, auto& source) -> absl::Status {
    using Entry = typename std::decay_t<decltype(destination)>::value_type;
    const auto required = destination.size() + source.size();
    if (required > destination.capacity()) {
      const auto capacity = std::max(required, destination.capacity() * 2);
      if (capacity >
          (std::numeric_limits<std::size_t>::max() - merge_charge.bytes()) /
              sizeof(Entry))
        return absl::ResourceExhaustedError("replica merge size overflow");
      const auto bytes = capacity * sizeof(Entry);
      auto reservation = TryReserveMemory(bytes);
      if (!reservation) {
        RecordMemoryRejection();
        return absl::ResourceExhaustedError("OOM replica merge admission");
      }
      destination.reserve(capacity);
      // Keep the conservative growth peak until all merged buffers die.
      merge_charge.Resize(merge_charge.bytes() + bytes);
    }
    for (auto& entry : source) destination.push_back(std::move(entry));
    return absl::OkStatus();
  };
  auto drain = [&]() -> Task<absl::Status> {
    while (state->decoder_->page_ready()) {
      std::size_t admission = 0;
      auto page = state->decoder_->TakePage(&admission);
      if (!page.ok()) co_return page.status();
      const auto count = page->size();
      if (state->skip_) {
        page = absl::CancelledError("page has been discarded");
        state->decoded_charge_.Resize(state->decoded_charge_.bytes() -
                                      admission);
        state->applied_count_ += count;
        continue;
      }
      auto measured = CollectionCompactEncoder::MeasurePage(*page);
      if (!measured.ok()) co_return measured.status();
      if (batch.size() != 0 && *measured > kWriteBatchBytes - batch_bytes) {
        const auto written = co_await flush_batch();
        if (!written.ok()) co_return written;
      }
      absl::Status merged;
      if (stage.value_type_ == ValueType::kHash)
        merged = merge(batch.fields_, page->fields_);
      else if (stage.value_type_ == ValueType::kSortedSet)
        merged = merge(batch.scored_members_, page->scored_members_);
      else
        merged = merge(batch.elements_, page->elements_);
      if (!merged.ok()) co_return merged;
      batch_admission += admission;
      batch_bytes += *measured;
      // The old page's vector capacity can die now; its transferred string
      // allowance is deliberately retained until flush_batch destroys them.
      page = absl::CancelledError("page has been consumed");
      if (batch_bytes >= kWriteBatchBytes) {
        const auto written = co_await flush_batch();
        if (!written.ok()) co_return written;
      }
    }
    co_return absl::OkStatus();
  };
  while (!input.empty()) {
    auto consumed = state->decoder_->Consume(input);
    if (!consumed.ok()) co_return consumed.status();
    if (*consumed == 0 && !state->decoder_->page_ready())
      co_return absl::InternalError(
          "replica collection decoder made no progress");
    input.remove_prefix(*consumed);
    const auto written = co_await drain();
    if (!written.ok()) co_return written;
  }
  if (!finish) co_return co_await flush_batch();
  if (state->received_bytes_ != stage.encoded_size_)
    co_return absl::InvalidArgumentError("truncated replica collection stream");
  auto complete = state->decoder_->Finish();
  if (!complete.ok()) co_return complete;
  complete = co_await drain();
  if (!complete.ok()) co_return complete;
  complete = co_await flush_batch();
  if (!complete.ok()) co_return complete;
  if (state->applied_count_ != stage.logical_size_)
    co_return absl::DataLossError("replica collection cardinality mismatch");
  if (!state->skip_) {
    complete = co_await CommitTxWrites(state->writes_.txid_, {&state->writes_});
    if (!complete.ok()) co_return complete;
    // The outer decision is now durable. A later undo-discard failure must
    // fail-stop, never restore a predecessor whose replacement is committed.
    state->settled_ = true;
    complete = co_await DiscardTxUndoLocal(state->writes_.txid_);
    if (!complete.ok()) {
      store.write_failed_ = true;
      state->key_hold_.Reset();
      co_return complete;
    }
    state->writes_.collect_undo_ = false;
  }
  state->settled_ = true;
  state->key_hold_.Reset();
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::WriteReplicaCollectionPage(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    ReplicaValueStage& stage, CollectionPage page) {
  if (page.size() == 0) co_return absl::OkStatus();
  auto state = stage.collection_;
  co_await store.store_state_mutex_.Lock();
  UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
  auto* sync = partition.replica_sync_.get();
  const bool native_snapshot = state->writes_.grouped_ingest_batch_ == nullptr;
  if (native_snapshot && (!sync || sync->command_sequence_))
    co_return absl::FailedPreconditionError(
        "collection snapshot conflicts with a replica command");
  if (native_snapshot) sync->command_sequence_ = stage.mutation_sequence_;
  struct CommandScope {
    WorkerStore::PartitionStore::ReplicaSyncState* sync_;
    ~CommandScope() {
      if (sync_ != nullptr) sync_->command_sequence_.reset();
    }
  } command_scope{native_snapshot ? sync : nullptr};
  const auto digest = ComputeDigest(stage.key_);
  GroupedHashObject::Handle previous;
  if (state->applied_count_ != 0) {
    previous =
        partition.grouped_objects_[stage.db_id_].CurrentForMutation(stage.key_);
    if (!previous || (native_snapshot &&
                      previous->command_sequence() != stage.mutation_sequence_))
      co_return absl::DataLossError("replica collection staged root changed");
  }
  // Admit the next bounded journal/fence before publication. Squashing then
  // retains only the original/current graphs and linear physical receipts.
  constexpr std::size_t undo_bytes = 4096;
  if (undo_bytes >
      std::numeric_limits<std::size_t>::max() - state->undo_charge_.bytes())
    co_return absl::ResourceExhaustedError("replica undo admission overflow");
  auto reservation = TryReserveMemory(undo_bytes);
  if (!reservation) {
    RecordMemoryRejection();
    co_return absl::ResourceExhaustedError(
        "OOM replica collection transaction metadata admission failed");
  }
  state->undo_charge_.Resize(state->undo_charge_.bytes() + undo_bytes);
  state->undo_overhead_bytes_ += undo_bytes;

  if (stage.value_type_ == ValueType::kHash ||
      stage.value_type_ == ValueType::kSet) {
    HashValue incoming;
    incoming.entries_.reserve(page.size());
    if (stage.value_type_ == ValueType::kHash) {
      for (auto& field : page.fields_)
        incoming.entries_.push_back({.digest_ = ComputeDigest(field.field_),
                                     .field_ = std::move(field.field_),
                                     .value_ = std::move(field.value_)});
    } else {
      for (auto& member : page.elements_)
        incoming.entries_.push_back({.digest_ = ComputeDigest(member),
                                     .field_ = std::move(member),
                                     .value_ = {}});
    }
    HashValue after;
    std::vector<HashGroupId> touched;
    if (previous) {
      for (const auto& field : incoming.entries_) {
        const auto* route = previous->directory().Find(field.field_);
        if (!route) co_return absl::DataLossError("replica Hash route missing");
        if (std::find(touched.begin(), touched.end(), route->id_) !=
            touched.end())
          continue;
        touched.push_back(route->id_);
        auto group = co_await LoadHashGroupSnapshot(
            store, partition, stage.db_id_, stage.key_, digest, previous,
            route->id_);
        if (!group.ok()) co_return group.status();
        for (auto& field : group->snapshot_.value_.entries_)
          after.entries_.push_back(std::move(field));
      }
    }
    for (auto& field : incoming.entries_)
      after.entries_.push_back(std::move(field));
    // SplitHashGroup validates exact field identities before any batch writes.
    // Equal fields route to the same group, so this covers both incoming and
    // earlier staged entries. Rechecking the combined image here would add a
    // whole-image sort/scan on top of that required per-group validation.
    const auto written = co_await CommitGroupedHashMutationLocked(
        store, partition, stage.db_id_, stage.key_, digest, previous,
        std::move(after), std::move(touched),
        state->applied_count_ + page.size(), stage.value_type_,
        stage.expire_at_ms_, &state->writes_, nullptr);
    co_return written.ok() ? SquashReplicaCollectionUndo(store, *state)
                           : written;
  }

  const auto kind = stage.value_type_ == ValueType::kList
                        ? OrderedCollectionKind::kList
                        : OrderedCollectionKind::kSortedSet;
  std::vector<OrderedCollectionEntry> entries;
  entries.reserve(page.size());
  if (kind == OrderedCollectionKind::kList) {
    for (auto& item : page.elements_)
      entries.push_back({.value_ = std::move(item)});
  } else {
    // Every streamed Sorted Set starts with a fresh indexed root. Page
    // planning rejects repeated members within the batch; preparing the
    // member index rejects collisions with earlier, untouched ordered pages,
    // including the same member at another score, before either graph writes.
    // Keep that invariant explicit instead of scanning all prior pages here.
    if (previous && !previous->has_member_index())
      co_return absl::DataLossError(
          "collection staged Sorted Set has no member index");
    for (auto& item : page.scored_members_)
      entries.push_back(
          {.value_ = std::move(item.member_), .score_ = item.score_});
  }
  OrderedCollectionMutationPlan plan;
  if (!previous) {
    OrderedGroupSnapshot initial{.kind_ = kind,
                                 .incarnation_ = 1,
                                 .id_ = 1,
                                 .entries_ = std::move(entries)};
    auto split = SplitOrderedGroup(std::move(initial), 2);
    if (!split.ok()) co_return split.status();
    plan.root_ = {
        .kind_ = kind,
        .incarnation_ = 1,
        .item_count_ = page.size(),
        .first_group_ = split->groups_.front().id_,
        .last_group_ = split->groups_.back().id_,
        .next_group_id_ = split->next_group_id_,
        .group_count_ = static_cast<std::uint32_t>(split->groups_.size())};
    plan.changed_ = true;
    plan.writes_ = std::move(split->groups_);
  } else {
    const auto& directory = previous->ordered_directory();
    std::vector<LoadedOrderedGroup> loaded;
    const auto count = directory.groups().size();
    for (std::size_t i = count > 1 ? count - 2 : 0; i < count; ++i) {
      auto old = co_await LoadOrderedGroupSnapshot(
          store, partition, stage.db_id_, stage.key_, digest, previous,
          directory.groups()[i].id_);
      if (!old.ok()) co_return old.status();
      loaded.push_back(std::move(*old));
    }
    auto appended = PlanOrderedCollectionSplice(directory, std::move(loaded),
                                                directory.root().item_count_, 0,
                                                std::move(entries));
    if (!appended.ok()) co_return appended.status();
    plan = std::move(*appended);
  }
  const auto written = co_await CommitGroupedOrderedMutationLocked(
      store, partition, stage.db_id_, stage.key_, digest, previous,
      std::move(plan), stage.expire_at_ms_, &state->writes_, nullptr);
  co_return written.ok() ? SquashReplicaCollectionUndo(store, *state) : written;
}

}  // namespace lavik::storage
