#include "impl.h"

namespace keylane::storage {
namespace {

bool SameMetadataSource(const GroupedHashObject::Handle& expected,
                        const GroupedHashObject::Handle& current) {
  if (!expected || !current) return false;
  const auto& a = expected->version();
  const auto& b = current->version();
  return a.db_epoch_ == b.db_epoch_ &&
         a.replication_epoch_ == b.replication_epoch_ &&
         a.index_generation_ == b.index_generation_ &&
         a.root_.mutation_sequence_ == b.root_.mutation_sequence_ &&
         a.root_.expire_at_ms_ == b.root_.expire_at_ms_ &&
         a.root_.value_type() == b.root_.value_type() &&
         expected->SameLogicalRoot(*current);
}

}  // namespace

Task<absl::Status> StorageEngine::Impl::UpdateGroupedExpirationLocked(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    GroupedHashObject::Handle previous, std::uint64_t expire_at_ms,
    TxShardWrites* tx, ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  auto& side = partition.grouped_objects_[db_id];
  if (!SameMetadataSource(previous, side.CurrentForMutation(key)))
    co_return absl::AbortedError("grouped expiration source changed");
  const auto db_epoch = EffectiveRecordDbEpoch(partition, db_id);
  const auto replication_epoch = partition.replication_epoch_;
  const auto index_generation = store.index_generations_[db_id];
  const auto grouped_generation = partition.grouped_generations_[db_id];
  const bool outer_transaction = tx != nullptr;
  TxShardWrites standalone;
  if (!tx) {
    InitializeTxWrites(tx::TxRuntime::Get()->next_txid_.fetch_add(
                           1, std::memory_order_relaxed),
                       std::span(&standalone, 1),
                       mutation_precondition != nullptr
                           ? *mutation_precondition
                           : MutationPrecondition{});
    tx = &standalone;
  }
  // No auxiliary is rewritten, but a new independent root cannot inherit
  // pages whose preceding transaction is still undecided. A shared outer
  // transaction is already one causal decision and never waits on itself.
  const auto dependency =
      co_await AwaitGroupedDependencyLocked(store, previous, tx->txid_);
  if (!dependency.ok()) co_return dependency;
  if (EffectiveRecordDbEpoch(partition, db_id) != db_epoch ||
      partition.replication_epoch_ != replication_epoch ||
      store.index_generations_[db_id] != index_generation ||
      !SameMetadataSource(previous, side.CurrentForMutation(key))) {
    co_return absl::AbortedError("grouped expiration population changed");
  }
  std::uint64_t sequence = 0;
  if (replica_loading_.load(std::memory_order_acquire)) {
    const auto* sync = partition.replica_sync_.get();
    if (!sync || !sync->command_sequence_)
      co_return absl::FailedPreconditionError(
          "grouped expiration replay has no command sequence");
    sequence = *sync->command_sequence_;
  } else {
    sequence = ++partition.mutation_sequence_;
  }
  if (sequence == 0 || sequence < previous->command_sequence())
    co_return absl::FailedPreconditionError(
        "stale grouped expiration sequence");
  absl::StatusOr<std::string> payload;
  if (previous->is_ordered()) {
    auto root = previous->ordered_directory().root();
    // Normalize a standalone-codec root whose revision defaults to C. Once
    // TTL advances C, the value revision must remain explicit and unchanged.
    root.revision_ = previous->revision();
    payload = EncodeOrderedCollectionRoot(root);
  } else {
    payload = EncodeGroupedHashRoot(previous->directory().root());
  }
  if (!payload.ok()) co_return payload.status();
  auto decision = PrepareGroupedDecision(*tx);
  if (!decision.ok()) co_return decision.status();
  auto reserved = side.PreparePublish(key, side.CurrentForMutation(key));
  if (!reserved.ok()) co_return reserved.status();
  std::optional<GroupedObjectIndex::Publication> publication(
      std::move(*reserved));
  GroupedHashObject::PreparedHandle builder;
  GroupRecordWrite root_write{
      .prepared_root_ = &builder,
      .publication_ = &*publication,
      .prepare_root_ =
          [&](const GroupedObjectVersion& physical) -> absl::Status {
        const auto current = side.CurrentForMutation(key);
        if (physical.db_epoch_ != db_epoch ||
            physical.replication_epoch_ != replication_epoch ||
            physical.index_generation_ != grouped_generation ||
            store.index_generations_[db_id] != index_generation ||
            !SameMetadataSource(previous, current)) {
          return absl::AbortedError(
              "grouped expiration source changed before publish");
        }
        auto version = physical;
        version.decision_ = *decision;
        auto prepared =
            GroupedHashObject::PrepareMetadataUpdate(current, version);
        if (!prepared.ok()) return prepared.status();
        builder = std::move(*prepared);
        publication.reset();
        auto refreshed = side.PreparePublish(key, current);
        if (!refreshed.ok()) return refreshed.status();
        publication.emplace(std::move(*refreshed));
        return absl::OkStatus();
      },
      .changed_groups_ = {},
      .root_incarnation_ = previous->incarnation()};
  GroupMutationWrite mutation{.sequence_ = sequence, .root_ = &root_write};
  auto appended = co_await AppendLocked(
      store, partition, db_id, key, digest, *payload, RecordKind::kValue,
      previous->version().root_.value_type(), expire_at_ms, tx,
      previous->version().root_.logical_size_, nullptr, nullptr, replication,
      nullptr, true, nullptr, mutation_precondition, &mutation);
  if (!appended.ok()) {
    if (store.write_failed_) (*decision)->FailPending();
    co_return appended;
  }
  if (!outer_transaction) {
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
        handoff_status = capacity;
      }
    } catch (const std::bad_alloc&) {
      RecordMemoryRejection();
      handoff_status = absl::ResourceExhaustedError(
          "OOM completing grouped expiration handoff");
    }
    if (unlocked_for_handoff) co_await store.store_state_mutex_.Lock();
    if (!handoff_status.ok()) {
      // The new TTL is a published root too. Failure to hand its decision to
      // a commit owner cannot make that tentative expiry readable or durable.
      (*decision)->FailPending();
      store.write_failed_ = true;
      co_return handoff_status;
    }
    handoff.completed_ = true;
  }
  co_return absl::OkStatus();
}

}  // namespace keylane::storage
