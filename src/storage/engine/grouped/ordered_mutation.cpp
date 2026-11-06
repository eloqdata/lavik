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

#include "../impl.h"
#include "dependency_guard.h"
#include "lavik/error_detail.h"
#include "lavik/storage/detail/grouped/scratch.h"
#include "lavik/storage/detail/ordered_compact_codec.h"
#include "lavik/storage/detail/stream_records.h"

namespace lavik::storage {
namespace {

bool SameLogicalView(const GroupedObject::Handle& before,
                     const GroupedObject::Handle& current) {
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
    GroupedObject::Handle previous, OrderedCollectionMutationPlan plan,
    std::uint64_t expire_at_ms, TxShardWrites* tx,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition,
    SortedSetMemberMutation* prepared_members, OnlineLatencyTrace* diagnostics,
    bool* retry_exclusive) {
  if (retry_exclusive) *retry_exclusive = false;
  const auto needs_x = [&]() {
    assert(retry_exclusive);
    *retry_exclusive = true;
    return absl::AbortedError("Sorted Set plan requires key X");
  };
  if (!diagnostics && tx) diagnostics = tx->diagnostics_;
  if (!plan.changed_) co_return absl::OkStatus();
  if (previous && (!previous->is_ordered() ||
                   plan.expected_sequence_ != previous->revision()))
    co_return absl::AbortedError("ordered mutation plan is stale");
  if (plan.delete_key_) {
    if (retry_exclusive) co_return needs_x();
    // The adapter already owns store_state_mutex_; DeleteLocked would try to
    // acquire it again. The tombstone append owns the same graph-retirement
    // and transaction/replication bookkeeping as the ordinary delete path.
    auto detail = MakeSetTrace(diagnostics);
    co_return co_await AppendLocked(
        store, partition, db_id, key, digest, {}, RecordKind::kTombstone,
        ValueType::kNone, 0, tx, 0, nullptr, nullptr, replication, true,
        nullptr, mutation_precondition, nullptr, nullptr, detail.get());
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
  const bool standalone_request = tx == nullptr;
  // A prebuilt dual-index plan can outlive a disjoint publication. The
  // bounded writer below rechecks this merge after every resource wait; other
  // callers still require the exact source root and retain whole-key X.
  const auto source_matches = [&](const GroupedObject::Handle& current) {
    if (SameLogicalView(previous, current)) return true;
    if (!previous || !current || !current->is_ordered() ||
        !standalone_request || !prepared_members ||
        replica_loading_.load(std::memory_order_acquire))
      return false;
    const auto& old_version = previous->version();
    const auto& now_version = current->version();
    if (old_version.db_epoch_ != now_version.db_epoch_ ||
        old_version.replication_epoch_ != now_version.replication_epoch_ ||
        old_version.index_generation_ != now_version.index_generation_ ||
        old_version.root_.expire_at_ms_ != now_version.root_.expire_at_ms_)
      return false;
    auto planned = plan.root_;
    planned.member_index_ = prepared_members->plan_.root_;
    return MergeSortedSetPublication(
               previous->ordered_directory(), current->ordered_directory(),
               planned, plan.writes_, prepared_members->plan_.writes_)
        .ok();
  };
  if (previous && !source_matches(source_side)) {
    co_return absl::AbortedError("grouped mutation source changed");
  }
  const bool outer_transaction = tx != nullptr;
  TxShardWrites standalone;
  if (!tx) {
    const auto predecessor =
        co_await PrepareGroupedDependencyLocked(store, source_side, nullptr);
    if (!predecessor.ok()) co_return predecessor;
    // Cleanup runs independently; opening a transaction does not require an
    // older transaction block to become reclaimable first.
    const auto space = BeforeGroupedTransaction(store);
    if (!space.ok()) co_return space;
    InitializeTxWrites(tx::TxRuntime::Get()->next_txid_.fetch_add(
                           1, std::memory_order_relaxed),
                       std::span(&standalone, 1),
                       mutation_precondition != nullptr
                           ? *mutation_precondition
                           : MutationPrecondition{});
    tx = &standalone;
  }
  tx->diagnostics_ = diagnostics;
  GroupedDependencyGuard dependency_guard(*tx, outer_transaction);
  const auto dependency =
      co_await PrepareGroupedDependencyLocked(store, source_side, tx);
  if (!dependency.ok()) co_return dependency;
  if (EffectiveRecordDbEpoch(partition, db_id) != db_epoch ||
      partition.replication_epoch_ != replication_epoch ||
      store.index_generations_[db_id] != index_generation ||
      (!SameLogicalView(source_side, side.CurrentForMutation(key)) &&
       !source_matches(side.CurrentForMutation(key)))) {
    co_return absl::AbortedError("grouped population changed before mutation");
  }
  // Cross-coordinator dependency waits release the store lock.
  // GC may publish another physical view of this same logical root meanwhile;
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
  batch.diagnostics_ = diagnostics;
  const bool own_batch =
      outer_transaction && tx->grouped_ingest_batch_ == nullptr;
  if (own_batch) {
    batch.txid_ = tx::TxRuntime::Get()->next_txid_.fetch_add(
        1, std::memory_order_relaxed);
    batch.transaction_lease_ = tx->transaction_lease_;
    // The child commit may live on another worker from the outer decision.
    // A distinct decision forces its own durable fence without marking the
    // still-uncommitted outer transaction durable prematurely.
    const auto child_decision = PrepareGroupedDecision(batch);
    if (!child_decision.ok()) co_return child_decision.status();
  }
  // A multi-page restore has one command decision, but each intermediate
  // graph still receives a unique revision. Committing individual pages would
  // resurrect a failed restore's auxiliaries if its enclosing EXEC commits.
  const auto command_batch = tx->grouped_ingest_batch_ != nullptr
                                 ? tx->grouped_ingest_batch_->txid_
                                 : batch.txid_;
  if (previous && plan.root_.incarnation_ != previous->incarnation()) {
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
  if (value_type == ValueType::kSortedSet) {
    if (!previous && member_plan.expected_sequence_ != 0)
      co_return absl::AbortedError("prepared member creation is stale");
    if ((previous &&
         member_plan.root_.incarnation_ != plan.root_.incarnation_) ||
        member_plan.root_.field_count_ != plan.root_.item_count_)
      co_return absl::AbortedError("prepared member index is stale");
  }
  auto planned_root = plan.root_;
  if (value_type == ValueType::kSortedSet)
    planned_root.member_index_ = member_plan.root_;
  // Only existing, topology-preserving standalone ZSET updates can use a
  // single bounded publication range. Large/structural and EXEC/Lua writes
  // keep the established key-X writer; no revision is reserved while an
  // eligible publication waits for its append resources.
  bool prepared_publication =
      previous && !outer_transaction &&
      previous->version().root_.expire_at_ms_ == expire_at_ms &&
      !replica_loading_.load(std::memory_order_acquire) &&
      value_type == ValueType::kSortedSet &&
      MergeSortedSetPublication(previous->ordered_directory(),
                                previous->ordered_directory(), planned_root,
                                plan.writes_, member_plan.writes_)
          .ok();
  if (retry_exclusive && !prepared_publication) co_return needs_x();
  // Member preparation can suspend on page IO. Allocate only after both
  // logical plans are ready, independently of the earlier transaction and
  // batch IDs. Existing key X protection orders the remaining staging waits.
  // Reserve the fixed-size root payload using the old revision. The late
  // callback overwrites it with publication order before staging any byte.
  auto revision =
      prepared_publication ? previous->revision() : AllocateGroupedRevision();
  if (!prepared_publication &&
      (revision == 0 || (previous && revision <= previous->revision()))) {
    co_return absl::OutOfRangeError(
        "group revision allocation did not advance");
  }
  plan.root_.revision_ = revision;
  if (!previous) {
    plan.root_.incarnation_ = revision;
    for (auto& page : plan.writes_) page.incarnation_ = revision;
  }
  if (value_type == ValueType::kSortedSet) {
    if (!previous) {
      member_plan.root_.incarnation_ = revision;
      for (auto& page : member_plan.writes_) page.incarnation_ = revision;
    }
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
    auto added = encoder_budget.AddBytes(
        sizeof(OrderedGroupEncoder) +
        (prepared_publication ? sizeof(PreparedGroupRecord) : 0));
    if (!added.ok()) co_return added;
  }
  for (std::size_t i = 0; i < member_plan.writes_.size(); ++i) {
    auto added = encoder_budget.AddBytes(
        sizeof(HashGroupEncoder) +
        (prepared_publication ? sizeof(PreparedGroupRecord) : 0));
    if (!added.ok()) co_return added;
  }
  auto encoder_admission = encoder_budget.Reserve(1);
  if (!encoder_admission.ok()) co_return encoder_admission.status();
  std::vector<OrderedGroupEncoder> ordered_encoders;
  std::vector<HashGroupEncoder> member_encoders;
  std::vector<std::uint64_t> ordered_sizes;
  std::vector<std::uint64_t> member_sizes;
  {
    // Reject preparation before any current-command pages are staged.
    if (LAVIK_FAULT_MATCHES("LAVIK_FAIL_GROUP_ENCODER_PREPARE_KEY", key)) {
      RecordMemoryRejection();
      co_return MemoryLimitError("OOM preparing group encoders");
    }
    ordered_encoders.reserve(plan.writes_.size());
    member_encoders.reserve(member_plan.writes_.size());
    ordered_sizes.reserve(plan.writes_.size());
    member_sizes.reserve(member_plan.writes_.size());
    for (const auto& snapshot : plan.writes_) {
      auto encoder = OrderedGroupEncoder::Create(snapshot);
      if (!encoder.ok()) co_return encoder.status();
      if (encoder->encoded_bytes() > kMaxRecordPayloadBytes) {
        co_return absl::OutOfRangeError(
            "group snapshot and parent key exceed payload limit");
      }
      ordered_sizes.push_back(encoder->encoded_bytes());
      ordered_encoders.push_back(std::move(*encoder));
    }
    for (const auto& snapshot : member_plan.writes_) {
      auto encoder = HashGroupEncoder::Create(snapshot);
      if (!encoder.ok()) co_return encoder.status();
      if (encoder->encoded_bytes() > kMaxRecordPayloadBytes)
        co_return absl::OutOfRangeError(
            "member snapshot and parent key exceed payload limit");
      member_sizes.push_back(encoder->encoded_bytes());
      member_encoders.push_back(std::move(*encoder));
    }
  }
  PreparedGroupBatch prepared_batch;
  if (prepared_publication) {
    const auto header_bytes = RecordHeaderBytes(
        key.size(), key.size() > kInlineKeyMaxBytes, true, false, true);
    std::size_t bytes = AlignRecord(
        RecordHeaderBytes(key.size(), key.size() > kInlineKeyMaxBytes, true,
                          expire_at_ms != 0) +
        root_payload->size());
    for (auto size : ordered_sizes) bytes += AlignRecord(header_bytes + size);
    for (auto size : member_sizes) bytes += AlignRecord(header_bytes + size);
    prepared_publication = bytes <= kStorageBlockBytes - kBlockHeaderBytes &&
                           bytes <= options_.buffers_.write_buffer_bytes_;
    if (prepared_publication) {
      prepared_batch.records_.reserve(ordered_sizes.size() +
                                      member_sizes.size());
      for (std::size_t i = 0; i < ordered_sizes.size(); ++i) {
        const auto size = AlignRecord(header_bytes + ordered_sizes[i]);
        prepared_batch.records_.push_back(PreparedGroupRecord{
            .id_ = {plan.writes_[i].id_, 0},
            .logical_size_ = OrderedGroupSize(plan.writes_[i]),
            .payload_ = InlineRecordPayload(ordered_encoders[i]),
            .disk_bytes_ = size,
            .location_ = {}});
        prepared_batch.disk_bytes_ += size;
      }
      for (std::size_t i = 0; i < member_sizes.size(); ++i) {
        const auto size = AlignRecord(header_bytes + member_sizes[i]);
        prepared_batch.records_.push_back(PreparedGroupRecord{
            .id_ = member_plan.writes_[i].id_,
            .logical_size_ = member_plan.writes_[i].field_count(),
            .payload_ = InlineRecordPayload(member_encoders[i]),
            .disk_bytes_ = size,
            .location_ = {}});
        prepared_batch.disk_bytes_ += size;
      }
    } else {
      if (retry_exclusive) co_return needs_x();
      revision = AllocateGroupedRevision();
      if (revision == 0 || revision <= previous->revision())
        co_return absl::OutOfRangeError(
            "group revision allocation did not advance");
      plan.root_.revision_ = revision;
      if (member_plan.changed_) member_plan.root_.revision_ = revision;
      plan.root_.member_index_ = member_plan.root_;
      root_payload = EncodeOrderedCollectionRoot(plan.root_);
      if (!root_payload.ok()) co_return root_payload.status();
    }
  }
  // Ordered page envelopes and per-entry framing dominate the compact
  // encoding. The Sorted Set member graph is excluded: it repeats members
  // solely for point lookup and has no counterpart in a compact value.
  std::optional<std::string> compact_payload;
  {
    if (previous != nullptr && tx->grouped_ingest_batch_ == nullptr) {
      std::uint64_t total = previous->ordered_directory().total_group_bytes();
      for (std::size_t i = 0; i < plan.writes_.size(); ++i) {
        const auto& page = plan.writes_[i];
        if (const auto* old = previous->ordered_directory().Find(page.id_)) {
          if (old->encoded_bytes_ > total)
            co_return absl::DataLossError("invalid ordered byte total");
          total -= old->encoded_bytes_;
        }
        if (!page.retired_) {
          if (ordered_sizes[i] > UINT64_MAX - total)
            co_return absl::OutOfRangeError("ordered byte total overflows");
          total += ordered_sizes[i];
        }
      }
      if (total < kCollectionGroupTargetBytes) {
        if (retry_exclusive) co_return needs_x();
        if (!SameLogicalView(previous, side.CurrentForMutation(key)))
          co_return absl::AbortedError(
              "Sorted Set demotion needs current key X");
        GroupedScratchBudget budget;
        for (const auto& metadata : previous->ordered_directory().groups()) {
          const bool replaced = std::any_of(
              plan.writes_.begin(), plan.writes_.end(),
              [&](const auto& page) { return page.id_ == metadata.id_; });
          if (replaced) continue;
          const GroupedRecordId id{metadata.id_, 0};
          const auto* entry = previous->FindGroup(id);
          if (entry == nullptr)
            co_return absl::DataLossError("missing ordered page for demotion");
          const auto added = budget.AddGroup(*entry, previous->ExtentsFor(id));
          if (!added.ok()) co_return added;
        }
        auto added = budget.AddBytes(2 * kCollectionGroupTargetBytes +
                                     plan.root_.item_count_ *
                                         sizeof(OrderedCollectionEntry));
        if (!added.ok()) co_return added;
        auto scratch = budget.Reserve(2);
        if (!scratch.ok()) co_return scratch.status();
        std::map<std::uint64_t, const OrderedGroupSnapshot*> changed_pages;
        for (const auto& page : plan.writes_)
          changed_pages.emplace(page.id_, &page);
        std::vector<OrderedCollectionEntry> compact;
        compact.reserve(plan.root_.item_count_);
        std::set<std::uint64_t> visited;
        std::uint64_t id = plan.root_.first_group_;
        while (id != 0) {
          if (!visited.insert(id).second ||
              visited.size() > plan.root_.group_count_)
            co_return absl::DataLossError("ordered demotion page cycle");
          if (const auto changed = changed_pages.find(id);
              changed != changed_pages.end()) {
            const auto& page = *changed->second;
            if (page.retired_)
              co_return absl::DataLossError("retired demotion page in chain");
            if (page.kind_ == OrderedCollectionKind::kString)
              compact.push_back(
                  {.value_ = std::string(OrderedStringBytes(page))});
            else if (page.encoded_sorted_set_) {
              for (const auto& entry : page.encoded_sorted_set_->entries())
                compact.push_back({std::string(entry.value_), entry.score_});
            } else {
              const auto entries = OrderedGroupEntries(page);
              compact.insert(compact.end(), entries.begin(), entries.end());
            }
            id = page.next_;
          } else {
            const auto* route = previous->ordered_directory().Find(id);
            if (route == nullptr)
              co_return absl::DataLossError("missing demotion page route");
            auto loaded = co_await LoadOrderedGroupSnapshot(
                store, partition, db_id, key, digest, previous, id, false);
            if (!loaded.ok()) co_return loaded.status();
            for (auto& entry : loaded->snapshot_.entries_)
              compact.push_back(std::move(entry));
            id = route->next_;
          }
        }
        if (visited.size() != plan.root_.group_count_ ||
            (plan.root_.kind_ != OrderedCollectionKind::kString &&
             compact.size() != plan.root_.item_count_))
          co_return absl::DataLossError("ordered demotion count mismatch");
        auto encoded = EncodeOrderedCompactValue(
            plan.root_.kind_, compact,
            HasOrderedIndex(plan.root_.kind_) ? 2 : 1);
        if (!encoded.ok()) co_return encoded.status();
        if (encoded->size() >= kCollectionGroupTargetBytes)
          co_return absl::InternalError("ordered demotion byte bound failed");
        compact_payload = std::move(*encoded);
      }
    }
  }
  if (compact_payload) {
    // Standalone demotion appends an ordinary untagged complete value and
    // hands no transaction receipt to the queue. Keep its prior dependency
    // boundary until that path has its own commit/failure handoff.
    if (!outer_transaction) {
      const auto durable =
          co_await AwaitGroupedDependencyLocked(store, source_side, tx->txid_);
      if (!durable.ok()) co_return durable;
    }
    if (!SameLogicalView(source_side, side.CurrentForMutation(key)) ||
        EffectiveRecordDbEpoch(partition, db_id) != db_epoch ||
        partition.replication_epoch_ != replication_epoch ||
        store.index_generations_[db_id] != index_generation)
      co_return absl::AbortedError("grouped source changed before demotion");
    const auto demoted = co_await AppendLocked(
        store, partition, db_id, key, digest, *compact_payload,
        RecordKind::kValue, value_type, expire_at_ms,
        outer_transaction ? tx : nullptr, field_count, nullptr, nullptr,
        replication, true, nullptr, mutation_precondition);
    if (demoted.ok() || store.write_failed_) dependency_guard.Keep();
    co_return demoted;
  }
  auto decision = PrepareGroupedDecision(*tx, !outer_transaction);
  if (!decision.ok()) co_return decision.status();
  if (own_batch) {
    // Auxiliary records retain the outer transaction tag AND this command's
    // independent batch tag. An errored EXEC command never commits its batch,
    // even if the surrounding EXEC later commits all its successful commands.
    auto batch_decision = PrepareGroupedDecision(batch);
    if (!batch_decision.ok()) co_return batch_decision.status();
  }
  if (!prepared_publication && previous &&
      !SameLogicalView(previous, side.CurrentForMutation(key)))
    co_return absl::AbortedError("member-index source changed during prepare");
  source_side = side.CurrentForMutation(key);
  auto reserved = side.PreparePublish(key, source_side);
  if (!reserved.ok()) co_return reserved.status();
  std::optional<GroupedObjectIndex::Publication> publication(
      std::move(*reserved));
  std::vector<GroupedRecordLocation> written;
  written.reserve(plan.writes_.size() + member_plan.writes_.size());
  // A failed batch in an outer transaction retains its staged bytes until the
  // outer commit retires them. Its existing fence must not point at a block we
  // recycled early; the absent batch decision makes those bytes invisible.
  auto abandon = [&]() -> Task<absl::Status> {
    // A prepared batch owns only a private append tail until root publication.
    // On pre-publication failure none of its predicted locations are records.
    if (prepared_publication) co_return absl::OkStatus();
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
            .dependent_extents_ = nullptr,
            .immediate_extents_ = record.extents_});
      } else {
        auto retired = RetiredRecordOf(location, nullptr);
        retired.immediate_extents_ = record.extents_;
        const auto dead = co_await MarkRecordDead(retired);
        if (!dead.ok()) {
          store.write_failed_ = true;
          co_return dead;
        }
      }
    }
    co_return absl::OkStatus();
  };
  if (prepared_publication) {
    for (const auto& page : prepared_batch.records_) {
      GroupedRecordLocation record{
          .id_ = page.id_, .location_ = {}, .extents_ = nullptr};
      record.location_.logical_size_ = page.logical_size_;
      written.push_back(std::move(record));
    }
  }
  for (std::size_t i = 0; !prepared_publication &&
                          i < plan.writes_.size() + member_plan.writes_.size();
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
          co_return MemoryLimitError(
              "OOM injected grouped auxiliary admission failure");
        });
    absl::StatusOr<GroupedRecordLocation> group;
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
  std::vector<RecoveredGroupedRecord> member_candidates;
  std::vector<GroupedRecordId> written_ids;
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
           .encoded_bytes_ = member_sizes[i - plan.writes_.size()],
           .retired_ = group.retired_});
      continue;
    }
    const auto& page = plan.writes_[i];
    const auto entries = page.encoded_sorted_set_
                             ? std::span<const OrderedCollectionEntry>{}
                             : OrderedGroupEntries(page);
    RecoveredOrderedGroup candidate{
        {.incarnation_ = plan.root_.incarnation_,
         .id_ = page.id_,
         .previous_ = page.previous_,
         .next_ = page.next_,
         .sequence_ = revision,
         .lsn_ = revision,
         .item_count_ = group.location_.logical_size_,
         .encoded_bytes_ = ordered_sizes[i],
         .record_token_ = page.id_,
         .retired_ = group.retired_,
         .min_score_ = page.encoded_sorted_set_
                           ? page.encoded_sorted_set_->entries().front().score_
                       : entries.empty() ? 0
                                         : entries.front().score_,
         .max_score_ = page.encoded_sorted_set_
                           ? page.encoded_sorted_set_->entries().back().score_
                       : entries.empty() ? 0
                                         : entries.back().score_},
        tx->txid_,
        command_batch};
    if (plan.root_.kind_ == OrderedCollectionKind::kStream && !page.retired_) {
      auto max_key = StreamRecordKey(page.entries_.back().value_);
      if (!max_key.ok()) co_return max_key.status();
      candidate.max_key_.Set(*max_key);
    } else if (plan.root_.kind_ == OrderedCollectionKind::kSortedSet &&
               !page.retired_) {
      // Publish the boundary with this logical page version, including edits
      // that keep its ID and count but replace its last member.
      candidate.max_key_.Set(
          page.encoded_sorted_set_
              ? page.encoded_sorted_set_->entries().back().value_
              : std::string_view(entries.back().value_));
    }
    candidates.push_back(std::move(candidate));
  }
  if (prepared_publication) {
    prepared_batch.prepare_ = [&]() -> absl::Status {
      const auto current = side.CurrentForMutation(key);
      if (!current || !current->is_ordered() ||
          EffectiveRecordDbEpoch(partition, db_id) != db_epoch ||
          partition.replication_epoch_ != replication_epoch ||
          store.index_generations_[db_id] != index_generation ||
          partition.grouped_generations_[db_id] != grouped_generation ||
          current->version().root_.expire_at_ms_ != expire_at_ms)
        return absl::AbortedError(
            "Sorted Set population changed before publication");
      auto merged = MergeSortedSetPublication(
          previous->ordered_directory(), current->ordered_directory(),
          planned_root, plan.writes_, member_plan.writes_);
      if (!merged.ok()) return merged.status();
      absl::int128 total = current->ordered_directory().total_group_bytes();
      for (std::size_t i = 0; i < plan.writes_.size(); ++i) {
        total -= current->ordered_directory()
                     .Find(plan.writes_[i].id_)
                     ->encoded_bytes_;
        total += ordered_sizes[i];
      }
      if (total < kCollectionGroupTargetBytes)
        return absl::AbortedError("Sorted Set demotion needs key X");
      auto retained = RetainGroupedDependency(store, current, tx);
      if (!retained.ok()) return retained;
      revision = AllocateGroupedRevision();
      if (revision == 0 || revision <= current->revision())
        return absl::OutOfRangeError(
            "group revision allocation did not advance");
      sequence = ++partition.mutation_sequence_;
      if (sequence == 0 || sequence < current->command_sequence())
        return absl::OutOfRangeError("group command sequence did not advance");
      merged->revision_ = revision;
      if (!member_plan.writes_.empty())
        merged->member_index_->revision_ = revision;
      auto encoded = EncodeOrderedCollectionRoot(*merged);
      if (!encoded.ok()) return encoded.status();
      // AppendLocked borrows this string's allocation through capacity waits.
      // ZSET roots have fixed width; overwrite without invalidating its view.
      if (encoded->size() != root_payload->size())
        return absl::InternalError("prepared root changed encoded size");
      std::memcpy(root_payload->data(), encoded->data(), encoded->size());
      plan.root_ = std::move(*merged);
      prepared_batch.revision_ = revision;
      prepared_batch.sequence_ = sequence;
      prepared_batch.logical_size_ = plan.root_.item_count_;
      for (auto& page : candidates) page.sequence_ = page.lsn_ = revision;
      for (auto& page : member_candidates)
        page.sequence_ = page.lsn_ = revision;
      source_side = current;
      return absl::OkStatus();
    };
  }
  GroupedObject::PreparedHandle builder;
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
        if (prepared_publication) {
          for (std::size_t i = 0; i < written.size(); ++i)
            written[i].location_ = prepared_batch.records_[i].location_;
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
        if (plan.root_.kind_ == OrderedCollectionKind::kStream) {
          for (const auto& page : plan.writes_) {
            if (page.id_ != plan.root_.first_group_ || page.retired_) continue;
            auto header = StreamRecordPayload(page.entries_.front().value_);
            if (!header.ok()) return header.status();
            auto remembered = directory->RememberStreamHeader(*header);
            if (!remembered.ok()) return remembered;
            break;
          }
        }
        auto prepared =
            previous ? GroupedObject::PrepareUpdateOrdered(
                           current, version, std::move(*directory), written)
                     : GroupedObject::PrepareCreateOrdered(
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
      .root_incarnation_ = plan.root_.incarnation_,
      .batch_ = prepared_publication ? &prepared_batch : nullptr};
  GroupMutationWrite mutation{.sequence_ = sequence, .root_ = &root_write};
  LAVIK_FAULT_INJECT(if (prepared_publication &&
                         LAVIK_FAULT_MATCHES("LAVIK_GROUP_PREPARE_PAUSE_KEY",
                                             key)) {
    spdlog::info("grouped publication prepared before revision key={}", key);
    store.store_state_mutex_.Unlock(*store.worker_);
    auto paused = co_await fault_injection::PauseWhileFileExists(
        "LAVIK_GROUP_PREPARE_PAUSE_FILE");
    co_await store.store_state_mutex_.Lock();
    if (!paused.ok()) co_return paused;
  });
  LAVIK_MAYBE_CRASH_AT("group-batch-before-root");
  const auto appended = co_await AppendLocked(
      store, partition, db_id, key, digest, *root_payload, RecordKind::kValue,
      value_type, expire_at_ms, tx, field_count, nullptr, nullptr, replication,
      true, nullptr, mutation_precondition, &mutation);
  if (!appended.ok()) {
    if (store.write_failed_) {
      // A root may already be staged/published on a fail-stopped path. Its
      // graph must remain intact until shutdown; it is no longer an orphan
      // batch that ordinary command-local cleanup may retire.
      dependency_guard.Keep();
      (*decision)->FailPending();
      co_return appended;
    }
    const auto abandoned = co_await abandon();
    co_return abandoned.ok() ? appended : abandoned;
  }
  dependency_guard.Keep();
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

  {
    if (LAVIK_FAULT_MATCHES("LAVIK_FAIL_GROUP_HANDOFF_KEY", key)) {
      RecordMemoryRejection();
      co_return MemoryLimitError("OOM completing grouped publication handoff");
    }
    if (outer_transaction) {
      // Waiting for this decision before returning the command makes the outer
      // coordinator's later commit causally depend on the complete auxiliary
      // batch. The normal outer decision remains the root's publication gate.
      batch.fences_ = tx->fences_;
      batch.grouped_predecessor_ = tx->grouped_predecessor_;
      batch.grouped_dependencies_ = tx->grouped_dependencies_;
      const bool inject_batch_failure =
          LAVIK_FAULT_MATCHES("LAVIK_FAIL_GROUP_BATCH_KEY", key);
      store.store_state_mutex_.Unlock(*store.worker_);
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
        committed = co_await CommitTxWrites(batch.txid_, {&batch}, nullptr,
                                            nullptr, diagnostics);
      }
      co_await store.store_state_mutex_.Lock();
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
        const auto capacity = co_await WaitForTxCommitCapacity(diagnostics);
        co_await store.store_state_mutex_.Lock();
        if (!capacity.ok()) co_return capacity;
      }
    }
  }

  handoff.completed_ = true;
  co_return absl::OkStatus();
}

}  // namespace lavik::storage
