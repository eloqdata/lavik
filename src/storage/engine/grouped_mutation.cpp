#include <charconv>

#include "impl.h"

namespace keylane::storage {
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

absl::StatusOr<HashGroupMutationPlan> BuildMutation(
    const GroupedHashObject::Handle& previous, HashValue after_image,
    std::span<const HashGroupId> changed_groups, std::uint64_t field_count,
    std::uint64_t sequence) {
  HashGroupMutationPlan plan;
  plan.changed_ = true;
  plan.root_.revision_ = sequence;
  if (!previous) {
    if (after_image.entries_.size() != field_count) {
      return absl::DataLossError("promotion after-image cardinality mismatch");
    }
    // This is the fresh local command-batch id, NOT the source command
    // sequence. A replay envelope may delete and recreate the same key.
    plan.root_.incarnation_ = sequence;
    plan.root_.seed_ = CurrentDigestSeed();
    auto groups =
        GroupHashValue(std::move(after_image), sequence, plan.root_.seed_);
    if (!groups.ok()) return groups.status();
    if (groups->size() > std::numeric_limits<std::uint32_t>::max()) {
      return absl::OutOfRangeError(
          "grouped Hash directory exceeds storage limit");
    }
    plan.root_.field_count_ = field_count;
    plan.root_.group_count_ = groups->size();
    plan.writes_ = std::move(*groups);
    return plan;
  }
  if (changed_groups.empty()) {
    return absl::InvalidArgumentError("grouped mutation has no changed groups");
  }
  plan.root_ = previous->directory().root();
  plan.root_.revision_ = sequence;
  plan.root_.field_count_ = field_count;
  plan.expected_sequence_ = previous->directory().sequence();
  std::map<HashGroupId, HashGroupSnapshot> changed;
  std::uint64_t expected_count = previous->directory().root().field_count_;
  for (const auto id : changed_groups) {
    if (!previous->FindGroup(id) ||
        !changed
             .emplace(id,
                      HashGroupSnapshot{.incarnation_ = plan.root_.incarnation_,
                                        .id_ = id})
             .second) {
      return absl::FailedPreconditionError("stale grouped mutation route");
    }
    expected_count -= previous->FindGroup(id)->value_.logical_size();
  }
  for (auto& entry : after_image.entries_) {
    const auto* route = previous->directory().Find(entry.field_);
    if (!route) return absl::DataLossError("grouped after-image has no route");
    const auto selected = changed.find(route->id_);
    if (selected != changed.end()) {
      selected->second.value_.entries_.push_back(std::move(entry));
      ++expected_count;
    }
  }
  if (expected_count != field_count) {
    return absl::DataLossError("grouped after-image cardinality mismatch");
  }
  for (auto& [id, snapshot] : changed) {
    auto replacements = SplitHashGroup(std::move(snapshot), plan.root_.seed_);
    if (!replacements.ok()) return replacements.status();
    if (replacements->size() > 1) {
      if (replacements->size() - 1 >
          std::numeric_limits<std::uint32_t>::max() - plan.root_.group_count_) {
        return absl::OutOfRangeError(
            "grouped Hash directory exceeds storage limit");
      }
      plan.root_.group_count_ += replacements->size() - 1;
      plan.writes_.push_back({.incarnation_ = plan.root_.incarnation_,
                              .id_ = id,
                              .retired_ = true});
    }
    for (auto& replacement : *replacements) {
      plan.writes_.push_back(std::move(replacement));
    }
  }
  return plan;
}

}  // namespace

Task<absl::Status> StorageEngine::Impl::CommitGroupedHashMutationLocked(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    GroupedHashObject::Handle previous, HashValue after_image,
    std::vector<HashGroupId> changed_groups, std::uint64_t field_count,
    ValueType value_type, std::uint64_t expire_at_ms, TxShardWrites* tx,
    ReplicationCommandAppend* replication) {
  if (field_count == 0 ||
      field_count > std::numeric_limits<std::uint32_t>::max() ||
      (value_type != ValueType::kHash && value_type != ValueType::kSet)) {
    co_return absl::OutOfRangeError(
        "invalid grouped collection cardinality/type");
  }
  if (previous && previous->version().root_.value_type() != value_type) {
    co_return absl::InvalidArgumentError("grouped mutation changes value type");
  }
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
    for (const auto& entry : after_image.entries_)
      append_bytes += entry.field_.size() + entry.value_.size() + 64;
    // No generation lease may be held while asking the cleaner to make room.
    // The population checks below also cover GC during this unlocked wait.
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
                       std::span(&standalone, 1));
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
  if (revision == 0 ||
      (previous && revision <= previous->directory().sequence())) {
    co_return absl::OutOfRangeError(
        "group revision allocation did not advance");
  }
  auto plan = BuildMutation(previous, std::move(after_image), changed_groups,
                            field_count, revision);
  if (!plan.ok()) co_return plan.status();
  auto root_payload = EncodeGroupedHashRoot(plan->root_);
  if (!root_payload.ok()) co_return root_payload.status();
  // Validate every indivisible field/envelope before the first disk write.
  // The extent writer consumes the same checked cursor incrementally.
  const bool external_key = key.size() > options_.inline_key_max_bytes_ ||
                            RecordHeaderBytes(key.size(), false, true, false,
                                              true) > kBlockHeaderSlotBytes;
  for (const auto& snapshot : plan->writes_) {
    const auto encoder = HashGroupEncoder::Create(snapshot);
    if (!encoder.ok()) co_return encoder.status();
    if (external_key &&
        key.size() > kMaxRecordPayloadBytes - encoder->encoded_bytes()) {
      co_return absl::OutOfRangeError(
          "group snapshot and parent key exceed payload limit");
    }
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
  auto reserved = side.PreparePublish(key, source_side);
  if (!reserved.ok()) co_return reserved.status();
  std::optional<GroupedObjectIndex::Publication> publication(
      std::move(*reserved));
  std::vector<HashGroupLocation> written;
  written.reserve(plan->writes_.size());
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
  for (const auto& snapshot : plan->writes_) {
    // Fail before the selected auxiliary begins. Already staged earlier
    // auxiliaries exercise command-local batch abort inside an outer EXEC.
    // The key is explicit so unrelated client/maintenance writes are untouched.
    KEYLANE_FAULT_INJECT(
        if (KEYLANE_FAULT_MATCHES_NTH("KEYLANE_FAIL_GROUP_AUX_KEY", key,
                                      "KEYLANE_FAIL_GROUP_AUX_NTH",
                                      written.size() + 1)) {
          const auto abandoned = co_await abandon();
          if (!abandoned.ok()) co_return abandoned;
          co_return absl::ResourceExhaustedError(
              "OOM injected grouped auxiliary admission failure");
        });
    auto group = co_await WriteHashGroupRecordLocked(
        store, partition, db_id, key, digest, snapshot, revision, *tx,
        value_type, command_batch);
    if (!group.ok()) {
      const auto original = group.status();
      const auto abandoned = co_await abandon();
      co_return abandoned.ok() ? original : abandoned;
    }
    written.push_back(std::move(*group));
  }
  std::vector<RecoveredHashGroup> candidates;
  std::vector<HashGroupId> written_ids;
  candidates.reserve(written.size());
  written_ids.reserve(written.size());
  for (const auto& group : written) {
    written_ids.push_back(group.id_);
    candidates.push_back({.incarnation_ = plan->root_.incarnation_,
                          .id_ = group.id_,
                          .sequence_ = revision,
                          .txid_ = tx->txid_,
                          .batch_txid_ = command_batch,
                          .field_count_ = group.location_.logical_size_,
                          .retired_ = group.retired_});
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
        absl::StatusOr<HashGroupDirectory> directory =
            previous
                ? current->directory().Apply(plan->root_, sequence, candidates)
                : HashGroupDirectory::Recover(
                      plan->root_, sequence, candidates,
                      absl::flat_hash_set<std::uint64_t>{tx->txid_,
                                                         command_batch});
        if (!directory.ok()) return directory.status();
        auto prepared =
            previous ? GroupedHashObject::PrepareUpdate(
                           current, version, std::move(*directory), written)
                     : GroupedHashObject::PrepareCreate(
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
      .root_incarnation_ = plan->root_.incarnation_};
  GroupMutationWrite mutation{.sequence_ = sequence, .root_ = &root_write};
  KEYLANE_MAYBE_CRASH_AT("group-batch-before-root");
  const auto appended = co_await AppendLocked(
      store, partition, db_id, key, digest, *root_payload, RecordKind::kValue,
      value_type, expire_at_ms, tx, field_count, nullptr, nullptr, replication,
      nullptr, true, nullptr, &mutation);
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
  KEYLANE_MAYBE_CRASH_AT("group-root-staged-before-batch-decision");
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
    KEYLANE_FAULT_BAD_ALLOC("KEYLANE_FAIL_GROUP_HANDOFF_KEY", key);
    if (outer_transaction) {
      // Waiting for this decision before returning the command makes the outer
      // coordinator's later commit causally depend on the complete auxiliary
      // batch. The normal outer decision remains the root's publication gate.
      batch.fences_ = tx->fences_;
      const bool inject_batch_failure =
          KEYLANE_FAULT_MATCHES("KEYLANE_FAIL_GROUP_BATCH_KEY", key);
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
      KEYLANE_MAYBE_CRASH_AT("group-batch-durable-before-outer-decision");
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

}  // namespace keylane::storage
