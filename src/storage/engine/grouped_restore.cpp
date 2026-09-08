#include "impl.h"

namespace keylane::storage {

Task<absl::Status> StorageEngine::Impl::RestoreGroupedViewLocked(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    GroupedHashObject::Handle previous, TxShardWrites* compensation,
    TxUndoLog* replacement_undo) {
  try {
    if (!previous || !compensation || compensation->collect_undo_ ||
        compensation->grouped_ingest_batch_ != nullptr)
      co_return absl::InvalidArgumentError("invalid grouped root compensation");
    const auto db_epoch = EffectiveRecordDbEpoch(partition, db_id);
    const auto replication_epoch = partition.replication_epoch_;
    const auto grouped_generation = partition.grouped_generations_[db_id];
    const auto index_generation = store.index_generations_[db_id];
    const auto& old_version = previous->version();
    if (old_version.db_epoch_ != db_epoch ||
        old_version.replication_epoch_ != replication_epoch ||
        old_version.index_generation_ != grouped_generation)
      co_return absl::AbortedError("grouped compensation population changed");
    auto& side = partition.grouped_objects_[db_id];
    const auto replaced = side.CurrentForMutation(key);
    auto same_replaced = [&](const GroupedHashObject::Handle& current) {
      if (!replaced || !current) return replaced == current;
      return current->command_sequence() == replaced->command_sequence() &&
             current->version().root_.expire_at_ms_ ==
                 replaced->version().root_.expire_at_ms_ &&
             current->SameLogicalRoot(*replaced);
    };

    std::uint64_t sequence = 0;
    if (replica_loading_.load(std::memory_order_acquire)) {
      const auto* sync = partition.replica_sync_.get();
      if (!sync || !sync->command_sequence_)
        co_return absl::FailedPreconditionError(
            "grouped compensation replay has no command sequence");
      sequence = *sync->command_sequence_;
    } else {
      sequence = ++partition.mutation_sequence_;
    }
    const auto revision = tx::TxRuntime::Get()->next_txid_.fetch_add(
        1, std::memory_order_relaxed);
    if (sequence == 0 || sequence < previous->command_sequence() ||
        (replaced && sequence < replaced->command_sequence()) ||
        revision == 0 || revision <= previous->revision() ||
        (replaced && revision <= replaced->revision()))
      co_return absl::FailedPreconditionError(
          "stale grouped root compensation");

    // Restoring the old root revision would lose to an already durable failed
    // intermediate root at the same replay C. Keep the old incarnation/pages,
    // but give the compensating root a fresh, strictly greater R. The caller
    // must leave the failed ingest's shared auxiliary batch uncommitted.
    constexpr std::size_t kPerRecordScratch =
        4 * sizeof(RecoveredOrderedGroup) + 4 * sizeof(RecoveredHashGroup) +
        4 * sizeof(HashGroupId);
    const auto count = previous->record_count();
    // Different incarnations already make Append retire the whole replacement
    // graph. Only an in-incarnation restore needs an explicit touched-id list.
    const auto replaced_count =
        replaced && replaced->incarnation() == previous->incarnation()
            ? replaced->record_count()
            : 0;
    const auto limit = std::numeric_limits<std::size_t>::max();
    if (count > limit - replaced_count ||
        count + replaced_count > (limit - 4096) / kPerRecordScratch)
      co_return absl::ResourceExhaustedError(
          "grouped compensation metadata overflow");
    auto scratch =
        TryReserveMemory(4096 + (count + replaced_count) * kPerRecordScratch);
    if (!scratch) {
      RecordMemoryRejection();
      co_return absl::ResourceExhaustedError(
          "OOM grouped compensation metadata");
    }
    std::optional<HashGroupDirectory> hash_directory;
    std::optional<OrderedGroupDirectory> ordered_directory;
    absl::StatusOr<std::string> payload;
    if (previous->is_ordered()) {
      auto root = previous->ordered_directory().root();
      root.revision_ = revision;
      auto rebuilt =
          previous->ordered_directory().Apply(root, revision, {}, sequence);
      if (!rebuilt.ok()) co_return rebuilt.status();
      ordered_directory.emplace(std::move(*rebuilt));
      payload = EncodeOrderedCollectionRoot(root);
    } else {
      auto root = previous->directory().root();
      root.revision_ = revision;
      std::vector<RecoveredHashGroup> candidates;
      candidates.reserve(count);
      auto append = [&](RecoveredHashGroup record) {
        // These are the original journal's adjudicated pages. Their previous
        // runtime decision ids do not become new durable commit assertions.
        record.txid_ = 0;
        record.batch_txid_ = 0;
        candidates.push_back(record);
      };
      for (const auto& [prefix, record] : previous->directory().groups())
        append(record);
      for (const auto& [id, record] : previous->directory().retired_groups())
        append(record);
      auto rebuilt =
          HashGroupDirectory::Recover(root, sequence, candidates, {});
      if (!rebuilt.ok()) co_return rebuilt.status();
      hash_directory.emplace(std::move(*rebuilt));
      payload = EncodeGroupedHashRoot(root);
    }
    if (!payload.ok()) co_return payload.status();
    std::vector<HashGroupId> changed;
    changed.reserve(replaced_count);
    if (replaced_count != 0) {
      // This also covers compensation inside the same incarnation. Retirement
      // compares exact coordinates and skips any old page shared by both views.
      replaced->ForEachRecord([&](HashGroupId id, const auto&, const auto&,
                                  bool) { changed.push_back(id); });
    }
    auto decision = PrepareGroupedDecision(*compensation);
    if (!decision.ok()) co_return decision.status();
    auto reserved = side.PreparePublish(key, replaced);
    if (!reserved.ok()) co_return reserved.status();
    std::optional<GroupedObjectIndex::Publication> publication(
        std::move(*reserved));
    GroupedHashObject::PreparedHandle builder;
    GroupRecordWrite root_write{
        .prepared_root_ = &builder,
        .publication_ = &*publication,
        .prepare_root_ =
            [&](const GroupedObjectVersion& physical) -> absl::Status {
          try {
            const auto current = side.CurrentForMutation(key);
            if (physical.db_epoch_ != db_epoch ||
                physical.replication_epoch_ != replication_epoch ||
                physical.index_generation_ != grouped_generation ||
                store.index_generations_[db_id] != index_generation ||
                !same_replaced(current))
              return absl::AbortedError(
                  "grouped compensation changed before publish");
            auto version = physical;
            version.decision_ = *decision;
            // The original journal retains the old physical pages and their
            // pins. Share them directly; neither group payloads nor extent
            // values are read or rewritten as part of this compensation.
            auto prepared = previous->is_ordered()
                                ? GroupedHashObject::PrepareUpdateOrdered(
                                      previous, version, *ordered_directory, {})
                                : GroupedHashObject::PrepareUpdate(
                                      previous, version, *hash_directory, {});
            if (!prepared.ok()) return prepared.status();
            builder = std::move(*prepared);
            publication.reset();
            auto refreshed = side.PreparePublish(key, current);
            if (!refreshed.ok()) return refreshed.status();
            publication.emplace(std::move(*refreshed));
            return absl::OkStatus();
          } catch (const std::bad_alloc&) {
            // This callback runs inside Append's coroutine, not this frame.
            return absl::ResourceExhaustedError(
                "OOM preparing grouped compensation publication");
          }
        },
        .changed_groups_ = changed,
        .root_incarnation_ = previous->incarnation()};
    GroupMutationWrite mutation{.sequence_ = sequence, .root_ = &root_write};
    // The caller filters its failed receipt set: a restored old auxiliary must
    // not be reclaimed when the outer transaction later commits. This helper
    // only publishes the compensation and updates stable undo entry handles.
    co_return co_await AppendLocked(
        store, partition, db_id, key, digest, *payload, RecordKind::kValue,
        old_version.root_.value_type(), old_version.root_.expire_at_ms_,
        compensation, old_version.root_.logical_size_, nullptr, nullptr,
        nullptr, nullptr, true, replacement_undo, &mutation);
  } catch (const std::bad_alloc&) {
    // A compensation failure is reported to the ingest driver, which must
    // poison the outer decision rather than commit a partially restored key.
    co_return absl::ResourceExhaustedError(
        "OOM allocating grouped compensation metadata");
  }
}

}  // namespace keylane::storage
