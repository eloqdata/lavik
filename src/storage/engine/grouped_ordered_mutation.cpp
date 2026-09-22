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

#include <charconv>

#include "impl.h"
#include "lavik/storage/detail/grouped_scratch.h"

namespace lavik::storage {
namespace {

bool SameLogicalView(const GroupedHashObject::Handle& before,
                     const GroupedHashObject::Handle& current) {
  if (!before || !current) return before == current;
  const auto& a = before->version();
  const auto& b = current->version();
  return a.db_epoch_ == b.db_epoch_ &&
         a.replication_epoch_ == b.replication_epoch_ &&
         a.index_generation_ == b.index_generation_ &&
         a.root_.mutation_sequence_ == b.root_.mutation_sequence_ &&
         a.root_.logical_size_ == b.root_.logical_size_ &&
         a.root_.expire_at_ms_ == b.root_.expire_at_ms_ &&
         a.root_.value_type() == b.root_.value_type() &&
         before->SameLogicalRoot(*current);
}

}  // namespace

Task<absl::Status> StorageEngine::Impl::CommitGroupedOrderedMutationLocked(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    GroupedHashObject::Handle previous, OrderedCollectionMutationPlan plan,
    std::uint64_t expire_at_ms, TxShardWrites* tx,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition,
    SortedSetMemberMutation* prepared_members) {
  if (!plan.changed_) co_return absl::OkStatus();
  if (previous && (!previous->is_ordered() ||
                   plan.expected_sequence_ != previous->revision()))
    co_return absl::AbortedError("ordered mutation plan is stale");
  if (plan.delete_key_) {
    // The adapter already owns store_state_mutex_; DeleteLocked would try to
    // acquire it again. The tombstone append owns the same graph-retirement
    // and transaction/replication bookkeeping as the ordinary delete path.
    co_return co_await AppendLocked(
        store, partition, db_id, key, digest, {}, RecordKind::kTombstone,
        ValueType::kNone, 0, tx, 0, nullptr, nullptr, replication, nullptr,
        true, nullptr, mutation_precondition);
  }
  const auto field_count = plan.root_.logical_size();
  const auto value_type = OrderedValueType(plan.root_.kind_);
  if ((field_count == 0 && value_type != ValueType::kStream) ||
      field_count > std::numeric_limits<std::uint32_t>::max() ||
      plan.writes_.empty() ||
      (previous && previous->version().root_.value_type() != value_type))
    co_return absl::InvalidArgumentError(
        "invalid ordered mutation cardinality/type");
  const auto db_epoch = EffectiveRecordDbEpoch(partition, db_id);
  const auto replication_epoch = partition.replication_epoch_;
  const auto index_generation = store.index_generations_[db_id];
  const auto grouped_generation = partition.grouped_generations_[db_id];
  auto& side = partition.grouped_objects_[db_id];
  auto source_side = side.CurrentForMutation(key);
  if (previous && !SameLogicalView(previous, source_side)) {
    co_return absl::AbortedError("grouped mutation source changed");
  }
  const bool outer_transaction = tx != nullptr;
  TxShardWrites standalone;
  if (!tx) {
    const auto predecessor =
        co_await AwaitGroupedDependencyLocked(store, source_side, 0);
    if (!predecessor.ok()) co_return predecessor;
    std::uint64_t append_bytes = kBlockHeaderSlotBytes;
    for (const auto& page : plan.writes_) {
      append_bytes += kBlockHeaderSlotBytes;
      for (const auto& entry : page.entries_)
        append_bytes += (entry.value_.size() + 16) *
                        (value_type == ValueType::kSortedSet &&
                                 (!previous || previous->has_member_index())
                             ? 2
                             : 1);
    }
    // A borrowed outer transaction must never wait on its own generation.
    // Only standalone admission coordinates reclaim before taking its lease.
    store.store_state_mutex_.Unlock(*store.worker_);
    absl::Status space;
    try {
      space = co_await BeforeGroupedTransaction(store, append_bytes);
    } catch (const std::bad_alloc&) {
      // Even allocation of the pressure coroutine frame must return with the
      // caller's lock invariant intact.
      RecordMemoryRejection();
      space = absl::ResourceExhaustedError("OOM grouped pressure coordinator");
    }
    co_await store.store_state_mutex_.Lock();
    if (!space.ok()) co_return space;
    InitializeTxWrites(tx::TxRuntime::Get()->next_txid_.fetch_add(
                           1, std::memory_order_relaxed),
                       std::span(&standalone, 1),
                       mutation_precondition != nullptr
                           ? *mutation_precondition
                           : MutationPrecondition{});
    tx = &standalone;
  }
  const auto dependency =
      co_await AwaitGroupedDependencyLocked(store, source_side, tx->txid_);
  if (!dependency.ok()) co_return dependency;
  if (EffectiveRecordDbEpoch(partition, db_id) != db_epoch ||
      partition.replication_epoch_ != replication_epoch ||
      store.index_generations_[db_id] != index_generation ||
      !SameLogicalView(source_side, side.CurrentForMutation(key))) {
    co_return absl::AbortedError("grouped population changed before mutation");
  }
  // Waiting for the predecessor decision releases the store lock. GC may
  // publish another physical view of this same logical root in that window;
  // the publication reservation compares handle identity, not just C/R.
  // Refresh only after the population/logical checks, while the lock is held.
  source_side = side.CurrentForMutation(key);
  std::uint64_t sequence = 0;
  if (replica_loading_.load(std::memory_order_acquire)) {
    const auto* sync = partition.replica_sync_.get();
    if (!sync || !sync->command_sequence_) {
      co_return absl::FailedPreconditionError(
          "grouped replay has no command sequence");
    }
    sequence = *sync->command_sequence_;
  } else {
    sequence = ++partition.mutation_sequence_;
  }
  if (sequence == 0 || (previous && sequence < previous->command_sequence())) {
    co_return absl::FailedPreconditionError(
        "grouped mutation has a stale source command sequence");
  }
  TxShardWrites batch;
  if (outer_transaction) {
    batch.txid_ = tx::TxRuntime::Get()->next_txid_.fetch_add(
        1, std::memory_order_relaxed);
    batch.generation_ = tx->generation_;
    batch.generation_lease_ = tx->generation_lease_;
    if (tx->grouped_ingest_batch_ == nullptr) {
      // The child commit may live on another worker from the outer decision.
      // A distinct decision forces its own durable fence without marking the
      // still-uncommitted outer transaction durable prematurely.
      const auto child_decision = PrepareGroupedDecision(batch);
      if (!child_decision.ok()) co_return child_decision.status();
    }
  }
  const auto revision = outer_transaction ? batch.txid_ : tx->txid_;
  // A multi-page restore has one command decision, but each intermediate
  // graph still receives a unique revision. Committing individual pages would
  // resurrect a failed restore's auxiliaries if its enclosing EXEC commits.
  const auto command_batch = tx->grouped_ingest_batch_ != nullptr
                                 ? tx->grouped_ingest_batch_->txid_
                                 : batch.txid_;
  if (revision == 0 || (previous && revision <= previous->revision())) {
    co_return absl::OutOfRangeError(
        "group revision allocation did not advance");
  }
  plan.root_.revision_ = revision;
  if (!previous) {
    plan.root_.incarnation_ = revision;
    for (auto& page : plan.writes_) page.incarnation_ = revision;
  } else if (plan.root_.incarnation_ != previous->incarnation()) {
    co_return absl::InvalidArgumentError(
        "ordered mutation changes incarnation");
  }
  // Derive both graphs before staging either one. This shared writer also
  // covers full-image callbacks and import, not only ZADD's typed fast path.
  absl::StatusOr<SortedSetMemberMutation> member_mutation;
  // Keep suspension out of ?: (GCC coroutine conditional lowering).
  if (prepared_members != nullptr)
    member_mutation = std::move(*prepared_members);
  else
    member_mutation = co_await PrepareSortedSetMembers(
        store, partition, db_id, key, digest, previous, plan);
  if (!member_mutation.ok()) co_return member_mutation.status();
  auto& member_plan = member_mutation->plan_;
  if (value_type == ValueType::kSortedSet &&
      (!previous || previous->has_member_index())) {
    if (!previous && prepared_members != nullptr) {
      if (member_plan.expected_sequence_ != 0)
        co_return absl::AbortedError("prepared member creation is stale");
      member_plan.root_.incarnation_ = revision;
      for (auto& page : member_plan.writes_) page.incarnation_ = revision;
    }
    // Prepared pages keep the predecessor incarnation; the durable revision
    // is assigned only after reacquiring store state and admitting this batch.
    if (member_plan.root_.incarnation_ != plan.root_.incarnation_ ||
        member_plan.root_.field_count_ != plan.root_.item_count_)
      co_return absl::AbortedError("prepared member index is stale");
    // An ordered-only topology rewrite may leave the member graph untouched;
    // keep its existing root/revision instead of manufacturing a new version.
    if (member_plan.changed_) member_plan.root_.revision_ = revision;
    plan.root_.member_index_ = member_plan.root_;
  }
  auto root_payload = EncodeOrderedCollectionRoot(plan.root_);
  if (!root_payload.ok()) co_return root_payload.status();
  // Validate every indivisible field/envelope before the first disk write.
  // Keep both graphs' checked encoders until writing so each page is validated
  // only once at this boundary. Both plans stay unmoved and immutable while
  // the encoders borrow their snapshots across allocation/extent IO waits.
  GroupedScratchBudget encoder_budget;
  for (std::size_t i = 0; i < plan.writes_.size(); ++i) {
    auto added = encoder_budget.AddBytes(sizeof(OrderedGroupEncoder));
    if (!added.ok()) co_return added;
  }
  for (std::size_t i = 0; i < member_plan.writes_.size(); ++i) {
    auto added = encoder_budget.AddBytes(sizeof(HashGroupEncoder));
    if (!added.ok()) co_return added;
  }
  auto encoder_admission = encoder_budget.Reserve(1);
  if (!encoder_admission.ok()) co_return encoder_admission.status();
  std::vector<OrderedGroupEncoder> ordered_encoders;
  std::vector<HashGroupEncoder> member_encoders;
  try {
    // No current-command pages have been staged if preflight allocation fails.
    LAVIK_FAULT_BAD_ALLOC("LAVIK_FAIL_GROUP_ENCODER_PREPARE_KEY", key);
    ordered_encoders.reserve(plan.writes_.size());
    member_encoders.reserve(member_plan.writes_.size());
    const bool external_key = key.size() > options_.inline_key_max_bytes_ ||
                              RecordHeaderBytes(key.size(), false, true, false,
                                                true) > kBlockHeaderSlotBytes;
    for (const auto& snapshot : plan.writes_) {
      auto encoder = OrderedGroupEncoder::Create(snapshot);
      if (!encoder.ok()) co_return encoder.status();
      if (external_key &&
          key.size() > kMaxRecordPayloadBytes - encoder->encoded_bytes()) {
        co_return absl::OutOfRangeError(
            "group snapshot and parent key exceed payload limit");
      }
      ordered_encoders.push_back(std::move(*encoder));
    }
    for (const auto& snapshot : member_plan.writes_) {
      auto encoder = HashGroupEncoder::Create(snapshot);
      if (!encoder.ok()) co_return encoder.status();
      if (external_key &&
          key.size() > kMaxRecordPayloadBytes - encoder->encoded_bytes())
        co_return absl::OutOfRangeError(
            "member snapshot and parent key exceed payload limit");
      member_encoders.push_back(std::move(*encoder));
    }
  } catch (const std::bad_alloc&) {
    RecordMemoryRejection();
    co_return absl::ResourceExhaustedError("OOM preparing group encoders");
  }
  auto decision = PrepareGroupedDecision(*tx);
  if (!decision.ok()) co_return decision.status();
  if (outer_transaction) {
    // Auxiliary records retain the outer transaction tag AND this command's
    // independent batch tag. An errored EXEC command never commits its batch,
    // even if the surrounding EXEC later commits all its successful commands.
    auto batch_decision = PrepareGroupedDecision(batch);
    if (!batch_decision.ok()) co_return batch_decision.status();
  }
  if (!SameLogicalView(source_side, side.CurrentForMutation(key)))
    co_return absl::AbortedError("member-index source changed during prepare");
  source_side = side.CurrentForMutation(key);
  auto reserved = side.PreparePublish(key, source_side);
  if (!reserved.ok()) co_return reserved.status();
  std::optional<GroupedObjectIndex::Publication> publication(
      std::move(*reserved));
  std::vector<HashGroupLocation> written;
  written.reserve(plan.writes_.size() + member_plan.writes_.size());
  // A failed batch in an outer transaction retains its staged bytes until the
  // outer commit retires them. Its existing fence must not point at a block we
  // recycled early; the absent batch decision makes those bytes invisible.
  auto abandon = [&]() -> Task<absl::Status> {
    for (const auto& record : written) {
      const auto& location = record.location_;
      if (outer_transaction) {
        tx->retirements_.push_back(TxShardWrites::Retired{
            .block_id_ = location.block_id(),
            .allocation_epoch_ = location.allocation_epoch(),
            .total_disk_bytes_ = location.total_disk_bytes(),
            .block_owner_ = location.block_owner(),
            .record_offset_ = location.record_offset(),
            .tx_tagged_ = location.tx_tagged(),
            .aborted_auxiliary_ = true,
            .dependent_extents_ =
                location.key_external() ? record.extents_ : nullptr,
            .immediate_extents_ =
                location.key_external() ? nullptr : record.extents_});
      } else {
        auto retired = RetiredRecordOf(
            location, location.key_external() ? record.extents_ : nullptr);
        retired.immediate_extents_ =
            location.key_external() ? nullptr : record.extents_;
        const auto dead = co_await MarkRecordDead(retired);
        if (!dead.ok()) {
          store.write_failed_ = true;
          co_return dead;
        }
      }
    }
    co_return absl::OkStatus();
  };
  for (std::size_t i = 0; i < plan.writes_.size() + member_plan.writes_.size();
       ++i) {
    // Fail before the selected auxiliary begins. Already staged earlier
    // auxiliaries exercise command-local batch abort inside an outer EXEC.
    // The key is explicit so unrelated client/maintenance writes are untouched.
    LAVIK_FAULT_INJECT(
        if (LAVIK_FAULT_MATCHES_NTH("LAVIK_FAIL_GROUP_AUX_KEY", key,
                                    "LAVIK_FAIL_GROUP_AUX_NTH",
                                    written.size() + 1)) {
          const auto abandoned = co_await abandon();
          if (!abandoned.ok()) co_return abandoned;
          co_return absl::ResourceExhaustedError(
              "OOM injected grouped auxiliary admission failure");
        });
    absl::StatusOr<HashGroupLocation> group;
    if (i < plan.writes_.size()) {
      group = co_await WriteOrderedGroupRecordLocked(
          store, partition, db_id, key, digest, plan.writes_[i],
          std::move(ordered_encoders[i]), revision, *tx, command_batch);
    } else {
      group = co_await WriteHashGroupRecordLocked(
          store, partition, db_id, key, digest,
          member_plan.writes_[i - plan.writes_.size()],
          std::move(member_encoders[i - plan.writes_.size()]), revision, *tx,
          ValueType::kSortedSet, command_batch);
    }
    if (!group.ok()) {
      const auto original = group.status();
      const auto abandoned = co_await abandon();
      co_return abandoned.ok() ? original : abandoned;
    }
    written.push_back(std::move(*group));
  }
  std::vector<RecoveredOrderedGroup> candidates;
  std::vector<RecoveredHashGroup> member_candidates;
  std::vector<HashGroupId> written_ids;
  candidates.reserve(written.size());
  written_ids.reserve(written.size());
  for (std::size_t i = 0; i < written.size(); ++i) {
    const auto& group = written[i];
    written_ids.push_back(group.id_);
    if (i >= plan.writes_.size()) {
      member_candidates.push_back(
          {.incarnation_ = plan.root_.incarnation_,
           .id_ = group.id_,
           .sequence_ = revision,
           .lsn_ = revision,
           .txid_ = tx->txid_,
           .batch_txid_ = command_batch,
           .field_count_ = group.location_.logical_size_,
           .retired_ = group.retired_});
      continue;
    }
    const auto& page = plan.writes_[i];
    candidates.push_back(
        {.incarnation_ = plan.root_.incarnation_,
         .id_ = page.id_,
         .previous_ = page.previous_,
         .next_ = page.next_,
         .sequence_ = revision,
         .lsn_ = revision,
         .txid_ = tx->txid_,
         .batch_txid_ = command_batch,
         .item_count_ = group.location_.logical_size_,
         .record_token_ = page.id_,
         .retired_ = group.retired_,
         .min_score_ = page.entries_.empty() ? 0 : page.entries_.front().score_,
         .max_score_ =
             page.entries_.empty() ? 0 : page.entries_.back().score_});
  }
  GroupedHashObject::PreparedHandle builder;
  GroupRecordWrite root_write{
      .prepared_root_ = &builder,
      .publication_ = &*publication,
      .prepare_root_ =
          [&](const GroupedObjectVersion& physical) -> absl::Status {
        if (physical.db_epoch_ != db_epoch ||
            physical.replication_epoch_ != replication_epoch ||
            physical.index_generation_ != grouped_generation ||
            store.index_generations_[db_id] != index_generation) {
          return absl::AbortedError(
              "grouped population changed before publication");
        }
        const auto current = side.CurrentForMutation(key);
        if (!SameLogicalView(source_side, current)) {
          return absl::AbortedError(
              "grouped source changed before publication");
        }
        GroupedObjectVersion version = physical;
        version.decision_ = *decision;
        std::optional<HashGroupDirectory> members;
        if (!previous && plan.root_.member_index_) {
          auto recovered = HashGroupDirectory::Recover(
              *plan.root_.member_index_, sequence, member_candidates,
              absl::flat_hash_set<std::uint64_t>{tx->txid_, command_batch});
          if (!recovered.ok()) return recovered.status();
          members = std::move(*recovered);
        }
        absl::StatusOr<OrderedGroupDirectory> directory =
            previous ? current->ordered_directory().Apply(plan.root_, revision,
                                                          candidates, sequence,
                                                          member_candidates)
                     : OrderedGroupDirectory::Recover(
                           plan.root_, revision, candidates,
                           absl::flat_hash_set<std::uint64_t>{tx->txid_,
                                                              command_batch},
                           sequence, std::move(members));
        if (!directory.ok()) return directory.status();
        auto prepared =
            previous ? GroupedHashObject::PrepareUpdateOrdered(
                           current, version, std::move(*directory), written)
                     : GroupedHashObject::PrepareCreateOrdered(
                           version, std::move(*directory), written,
                           store.record_index_entry_arena_);
        if (!prepared.ok()) return prepared.status();
        builder = std::move(*prepared);
        if (source_side) {
          // Root GC may have changed its physical address as well as moving
          // individual groups. Existing side entries need no new capacity.
          publication.reset();
          auto refreshed = side.PreparePublish(key, current);
          if (!refreshed.ok()) return refreshed.status();
          publication.emplace(std::move(*refreshed));
        }
        return absl::OkStatus();
      },
      .changed_groups_ = written_ids,
      .root_incarnation_ = plan.root_.incarnation_};
  GroupMutationWrite mutation{.sequence_ = sequence, .root_ = &root_write};
  LAVIK_MAYBE_CRASH_AT("group-batch-before-root");
  const auto appended = co_await AppendLocked(
      store, partition, db_id, key, digest, *root_payload, RecordKind::kValue,
      value_type, expire_at_ms, tx, field_count, nullptr, nullptr, replication,
      nullptr, true, nullptr, mutation_precondition, &mutation);
  if (!appended.ok()) {
    if (store.write_failed_) {
      // A root may already be staged/published on a fail-stopped path. Its
      // graph must remain intact until shutdown; it is no longer an orphan
      // batch that ordinary command-local cleanup may retire.
      (*decision)->FailPending();
      co_return appended;
    }
    const auto abandoned = co_await abandon();
    co_return abandoned.ok() ? appended : abandoned;
  }
  LAVIK_MAYBE_CRASH_AT("group-root-staged-before-batch-decision");
  if (tx->grouped_ingest_batch_ != nullptr) co_return absl::OkStatus();
  // WriteRecord's staged-root guard ends when it returns. Publication is
  // still incomplete until the batch/queue owns its decision: a fence-vector
  // or coroutine allocation here must poison that root, not expose an
  // indefinitely Pending graph after returning OOM.
  struct HandoffGuard {
    WorkerStore* store_;
    GroupedCommitDecision* decision_;
    bool completed_ = false;
    ~HandoffGuard() {
      if (completed_) return;
      decision_->FailPending();
      store_->write_failed_ = true;
    }
  } handoff{&store, decision->get()};
  bool unlocked_for_handoff = false;
  absl::Status handoff_status;
  try {
    LAVIK_FAULT_BAD_ALLOC("LAVIK_FAIL_GROUP_HANDOFF_KEY", key);
    if (outer_transaction) {
      // Waiting for this decision before returning the command makes the outer
      // coordinator's later commit causally depend on the complete auxiliary
      // batch. The normal outer decision remains the root's publication gate.
      batch.fences_ = tx->fences_;
      const bool inject_batch_failure =
          LAVIK_FAULT_MATCHES("LAVIK_FAIL_GROUP_BATCH_KEY", key);
      store.store_state_mutex_.Unlock(*store.worker_);
      unlocked_for_handoff = true;
      absl::Status committed;
      if (inject_batch_failure) {
        // Make the failed root observable to cold recovery, rather than merely
        // testing a process-local staged buffer that disappears on shutdown.
        for (const auto& fence : batch.fences_) {
          committed = co_await AwaitRelocationDurable(RelocationDurabilityFence{
              .block_id_ = fence.block_id_,
              .allocation_epoch_ = fence.allocation_epoch_,
              .block_owner_ = fence.block_owner_,
              .committed_bytes_ = fence.committed_bytes_});
          if (!committed.ok()) break;
        }
        if (committed.ok()) {
          committed =
              absl::InternalError("injected grouped batch commit I/O failure");
        }
      } else {
        committed = co_await CommitTxWrites(batch.txid_, {&batch});
      }
      co_await store.store_state_mutex_.Lock();
      unlocked_for_handoff = false;
      if (!committed.ok()) {
        (*decision)->FailPending();
        store.write_failed_ = true;
        co_return committed;
      }
      LAVIK_MAYBE_CRASH_AT("group-batch-durable-before-outer-decision");
    } else {
      PublishCommittedFullSyncEffects(&standalone);
      std::vector<TxShardWrites> receipts;
      receipts.push_back(std::move(standalone));
      const auto transaction_id = receipts.front().txid_;
      if (!EnqueueTxCommit(transaction_id, std::move(receipts))) {
        store.store_state_mutex_.Unlock(*store.worker_);
        unlocked_for_handoff = true;
        const auto capacity = co_await WaitForTxCommitCapacity();
        co_await store.store_state_mutex_.Lock();
        unlocked_for_handoff = false;
        if (!capacity.ok()) co_return capacity;
      }
    }
  } catch (const std::bad_alloc&) {
    RecordMemoryRejection();
    handoff_status = absl::ResourceExhaustedError(
        "OOM completing grouped publication handoff");
  }
  // The caller's unlock guard still owns the store mutex. Restore that
  // invariant even if creating/awaiting the commit task threw after unlock.
  if (unlocked_for_handoff) co_await store.store_state_mutex_.Lock();
  if (!handoff_status.ok()) co_return handoff_status;
  handoff.completed_ = true;
  co_return absl::OkStatus();
}

}  // namespace lavik::storage
