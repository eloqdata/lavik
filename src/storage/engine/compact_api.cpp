#include "impl.h"
#include "keylane/storage/detail/grouped_scratch.h"
#include "keylane/storage/detail/grouped_sorted_rewrite.h"
#include "keylane/storage/detail/ordered_compact_codec.h"

namespace keylane::storage {
namespace {

absl::StatusOr<OrderedCollectionMutationPlan> PrepareSortedSetGroups(
    std::string_view encoded, std::uint64_t count) {
  auto entries = DecodeOrderedCompactValue(OrderedCollectionKind::kSortedSet,
                                           encoded, count);
  if (!entries.ok()) return entries.status();
  OrderedGroupSnapshot initial{.kind_ = OrderedCollectionKind::kSortedSet,
                               .incarnation_ = 1,
                               .id_ = 1,
                               .entries_ = std::move(*entries)};
  auto split = SplitOrderedGroup(std::move(initial), 2);
  if (!split.ok()) return split.status();
  return OrderedCollectionMutationPlan{
      .root_ = {.kind_ = OrderedCollectionKind::kSortedSet,
                .incarnation_ = 1,
                .item_count_ = count,
                .first_group_ = split->groups_.front().id_,
                .last_group_ = split->groups_.back().id_,
                .next_group_id_ = split->next_group_id_,
                .group_count_ =
                    static_cast<std::uint32_t>(split->groups_.size())},
      .changed_ = true,
      .writes_ = std::move(split->groups_)};
}

}  // namespace

Task<absl::Status> StorageEngine::Impl::ExecuteCompact(
    std::uint8_t db_id, std::string_view key, ValueType value_type,
    bool read_only, const CompactValueCallback& callback, std::uint64_t now_ms,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  assert(db_id < kLogicalDatabaseCount);
  const Digest digest = ComputeDigest(key);
  auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
      db_id, tx::FingerprintOf(digest),
      read_only ? tx::LockMode::kShared : tx::LockMode::kExclusive);
  co_return co_await ExecuteCompactLocked(db_id, key, digest, value_type,
                                          read_only, callback, nullptr, now_ms,
                                          replication, false,
                                          mutation_precondition);
}

Task<absl::Status> StorageEngine::Impl::ExecuteCompactLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    ValueType value_type, bool read_only, const CompactValueCallback& callback,
    TxShardWrites* tx, std::uint64_t now_ms,
    ReplicationCommandAppend* replication,
    bool prepare_unlocked,
    const MutationPrecondition* mutation_precondition) {
  assert(db_id < kLogicalDatabaseCount);
  if (value_type != ValueType::kString && value_type != ValueType::kSortedSet &&
      value_type != ValueType::kStream) {
    co_return absl::InvalidArgumentError("unsupported compact value type");
  }

  try {
    WorkerStore& store = CurrentStore();
    auto& partition = PartitionForKey(store, key);
    co_await store.store_state_mutex_.Lock();
    UnlockGuard unlock(&store.store_state_mutex_, store.worker_);

    auto& index = partition.indexes_[db_id];
    auto* found = index.Find(digest, key);
    if (found != nullptr && !found->key_complete()) [[unlikely]] {
      auto resolved = co_await FindVerifiedEntry(store, index, digest, key);
      if (!resolved.ok()) co_return resolved.status();
      found = *resolved;
    }

    const bool stored_value =
        found != nullptr && found->value_.kind() == RecordKind::kValue;
    if (now_ms == 0) now_ms = UnixTimeMillis();
    const bool exists = stored_value && !IsExpired(*found, now_ms);
    if (value_type == ValueType::kSortedSet && !exists && found &&
        found->value_.grouped()) {
      // A tentative failed root must not hide behind its uncommitted TTL in
      // legacy callbacks either. Ordinary String reads keep their old path.
      const auto readable = co_await ReadKeyMetadataLocked(db_id, key, digest);
      if (!readable.ok()) co_return readable.status();
    }
    if (exists && found->value_.value_type() != value_type) {
      co_return absl::InvalidArgumentError(
          "WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    const std::uint64_t observed_index_generation =
        store.index_generations_[db_id];
    const std::uint64_t observed_db_epoch = DbEpoch(db_id);
    const std::uint64_t observed_replication_epoch =
        partition.replication_epoch_;
    auto read_epoch_changed = [&]() {
      return read_only &&
             (store.index_generations_[db_id] != observed_index_generation ||
              DbEpoch(db_id) != observed_db_epoch ||
              partition.replication_epoch_ != observed_replication_epoch);
    };
    const RecordLocation location =
        exists ? MaterializeIndexLocation(*found) : RecordLocation{};
    const ExtentManifest extents =
        exists ? ExtentsFor(store, found) : ExtentManifest{};
    // Only typed single-key ZADD/ZREM/ZINCRBY opt in. Arbitrary callbacks may
    // carry additional ownership or side-effect assumptions; tx == nullptr
    // alone is not permission to unlock a legacy or multi-key operation.
    const bool unlocked_compact_write =
        value_type == ValueType::kSortedSet && prepare_unlocked && !read_only &&
        exists &&
        CanPrepareCompactWriteUnlocked(store, partition, found, location, tx);
    const bool unlocked_create =
        value_type == ValueType::kSortedSet && prepare_unlocked && !read_only &&
        !exists && CanPrepareCollectionCreateUnlocked(partition, tx);
    std::optional<CompactWriteSnapshot> write_snapshot;
    if (unlocked_compact_write || unlocked_create)
      write_snapshot = CaptureCompactWriteSnapshot(store, partition, db_id);
    GroupedHashObject::Handle grouped;
    if (exists && location.grouped()) {
      auto object = partition.grouped_objects_[db_id].Lookup(
          key, GroupedObjectVersion{
                   .root_ = location,
                   .db_epoch_ = EffectiveRecordDbEpoch(partition, db_id),
                   .replication_epoch_ = observed_replication_epoch,
                   .index_generation_ = partition.grouped_generations_[db_id]});
      if (!object.ok()) co_return object.status();
      grouped = std::move(*object);
    }
    std::optional<MemoryReservation> callback_admission;
    // Keep the preexisting bounded inline workspace available when the
    // database is already over maxmemory: shrinking commands must still be
    // able to release data. Both wire size and entry overhead are bounded;
    // this is not an exemption for grouped or external full-image callbacks.
    // Growth still passes command/write admission, and multiplicative replies
    // (such as repeated ZRANDMEMBER output) have their own frontend admission.
    // No String operation acquires this collection-only budget.
    const bool bounded_inline =
        value_type == ValueType::kSortedSet && exists && !grouped &&
        !location.external() && !location.key_external() &&
        location.total_disk_bytes() < kGroupedHashPromotionBytes &&
        location.logical_size_ <= 1024;
    if (value_type == ValueType::kSortedSet && exists && !bounded_inline) {
      GroupedScratchBudget budget;
      if (grouped) {
        if (!grouped->is_ordered() ||
            grouped->ordered_directory().root().kind_ !=
                OrderedCollectionKind::kSortedSet)
          co_return absl::DataLossError("invalid Sorted Set callback view");
        for (const auto& page : grouped->ordered_directory().groups()) {
          const HashGroupId id{page.id_, 0};
          const auto* entry = grouped->FindGroup(id);
          if (!entry)
            co_return absl::DataLossError("missing Sorted Set callback page");
          auto admitted = budget.AddGroup(entry->value_,
                                          grouped->ExtentsFor(id), key.size());
          if (!admitted.ok()) co_return admitted;
        }
      } else {
        auto admitted = budget.AddGroup(found->value_, extents, key.size());
        if (!admitted.ok()) co_return admitted;
      }
      // The loader's scratch ends when it returns the encoded read buffer.
      // Legacy callbacks then decode, mutate and encode again; grouped writes
      // additionally hold before/after entries and a sparse rewrite plan.
      // Keep conservative headroom through publication, not just through load.
      // Replies with multiplicity (ZRANDMEMBER) admit their separate output.
      auto admitted = budget.Reserve(6);
      if (!admitted.ok()) co_return admitted.status();
      callback_admission.emplace(std::move(*admitted));
    }
    std::optional<LoadedValue> loaded;
    std::optional<CompactValueView> view;
    if (read_only || unlocked_compact_write || unlocked_create) {
      // Keep only copied physical identity and owned read buffers across IO.
      // The caller's key hold excludes logical writes, but not GC relocation.
      found = nullptr;
      unlock.Unlock();
    }
    KEYLANE_FAULT_INJECT(if (unlocked_create) {
      KEYLANE_FAULT_BAD_ALLOC("KEYLANE_FAIL_COLLECTION_CREATE_PREPARE_KEY",
                              key);
    });
    KEYLANE_FAULT_INJECT(if (unlocked_compact_write) {
      auto paused = co_await PauseCompactWriteForTest(*store.worker_, key);
      if (!paused.ok()) co_return paused;
    });
    if (exists) {
      auto value = co_await LoadValue(store, partition, db_id, key, digest,
                                      location, extents);
      if (!value.ok()) {
        if (!read_epoch_changed()) co_return value.status();
      } else {
        loaded.emplace(std::move(*value));
        const auto bytes = loaded->value();
        view = CompactValueView{
            .encoded_ = std::string_view(
                reinterpret_cast<const char*>(bytes.data()), bytes.size()),
            .logical_size_ = location.logical_size_,
            .expire_at_ms_ = location.expire_at_ms_,
        };
      }
    }

    auto update = callback(view);
    if (!update.ok()) co_return update.status();
    // A missing key may be created directly as a large graph. Keep both
    // indexes private/admitted until absence and population are revalidated;
    // the commit adapter stamps their placeholder incarnations together.
    std::optional<MemoryReservation> create_admission;
    std::optional<OrderedCollectionMutationPlan> created_groups;
    SortedSetMemberMutation created_members;
    if (unlocked_create && update->changed_ && !update->erase_ &&
        update->encoded_.size() >= kGroupedHashPromotionBytes) {
      GroupedScratchBudget budget;
      auto added = budget.AddBytes(update->encoded_.size());
      if (!added.ok()) co_return added;
      if (update->logical_size_ > std::numeric_limits<std::size_t>::max() / 256)
        co_return absl::ResourceExhaustedError(
            "Sorted Set creation count overflow");
      added = budget.AddBytes(update->logical_size_ * 256);
      if (!added.ok()) co_return added;
      auto admitted = budget.Reserve(1);
      if (!admitted.ok()) co_return admitted.status();
      create_admission.emplace(std::move(*admitted));
      auto plan =
          PrepareSortedSetGroups(update->encoded_, update->logical_size_);
      if (!plan.ok()) co_return plan.status();
      created_groups.emplace(std::move(*plan));
      KEYLANE_FAULT_INJECT({
        const auto paused = co_await PauseGroupedWriteForTest(
            *store.worker_, key, "create-members");
        if (!paused.ok()) co_return paused;
      });
      auto members = co_await PrepareSortedSetMembers(
          store, partition, db_id, key, digest, nullptr, *created_groups, true);
      if (!members.ok()) co_return members.status();
      created_members = std::move(*members);
    }
    if (unlocked_compact_write || unlocked_create) {
      KEYLANE_FAULT_INJECT(if (unlocked_create) {
        const auto paused =
            co_await PauseGroupedWriteForTest(*store.worker_, key, "create");
        if (!paused.ok()) co_return paused;
      });
      co_await store.store_state_mutex_.Lock();
      unlock.Adopt();
      absl::Status valid;
      if (unlocked_create) {
        auto* current = index.Find(digest, key);
        if (current != nullptr && !current->key_complete()) {
          auto verified = co_await FindVerifiedEntry(store, index, digest, key);
          if (!verified.ok()) co_return verified.status();
          current = *verified;
        }
        valid = ValidateCollectionCreateSnapshot(
            store, partition, db_id, current, now_ms, *write_snapshot,
            mutation_precondition);
      } else {
        valid = ValidateCompactWriteSnapshot(store, partition, db_id, key,
                                             digest, location, *write_snapshot);
      }
      if (!valid.ok()) co_return valid;
      // Validate even a no-op before returning success. Never retry this
      // callback: it already populated the typed command's private result.
      // Command DB admission remains held through AppendLocked's later waits;
      // direct engine population replacement requires the same caller gate.
    }
    if (read_only) {
      if (update->changed_) {
        co_return absl::InternalError("read-only compact operation mutated");
      }
      co_return absl::OkStatus();
    }
    if (!update->changed_) co_return absl::OkStatus();
    if (update->erase_ && !exists) co_return absl::OkStatus();
    if (update->reuse_encoded_ &&
        (!exists || update->erase_ || !update->encoded_.empty())) {
      co_return absl::InternalError("invalid compact payload reuse");
    }
    if (!update->erase_ && update->logical_size_ == 0 &&
        value_type == ValueType::kSortedSet) {
      co_return absl::InvalidArgumentError(
          "nonempty compact payload has zero cardinality");
    }

    const RecordKind kind =
        update->erase_ ? RecordKind::kTombstone : RecordKind::kValue;
    const ValueType published_type =
        update->erase_ ? ValueType::kNone : value_type;
    const std::uint64_t expire_at_ms =
        update->erase_ ? 0
                       : update->expire_at_ms_.value_or(
                             exists ? location.expire_at_ms_ : 0);
    const std::string_view encoded = update->reuse_encoded_
                                         ? view->encoded_
                                         : std::string_view(update->encoded_);
    const std::uint64_t logical_size =
        update->reuse_encoded_ ? location.logical_size_ : update->logical_size_;
    const bool promote_ordered = encoded.size() >= kGroupedHashPromotionBytes;
    if (!update->erase_ && value_type == ValueType::kSortedSet &&
        (grouped != nullptr || promote_ordered)) {
      if (created_groups)
        co_return co_await CommitGroupedOrderedMutationLocked(
            store, partition, db_id, key, digest, nullptr,
            std::move(*created_groups), expire_at_ms, tx, replication,
            mutation_precondition, &created_members);
      OrderedCollectionMutationPlan plan;
      if (grouped == nullptr) {
        auto created = PrepareSortedSetGroups(encoded, logical_size);
        if (!created.ok()) co_return created.status();
        plan = std::move(*created);
      } else {
        auto after = DecodeOrderedCompactValue(
            OrderedCollectionKind::kSortedSet, encoded, logical_size);
        if (!after.ok()) co_return after.status();
        if (!grouped->is_ordered() ||
            grouped->ordered_directory().root().kind_ !=
                OrderedCollectionKind::kSortedSet ||
            !view.has_value()) {
          co_return absl::DataLossError("invalid grouped Sorted Set view");
        }
        auto before =
            DecodeOrderedCompactValue(OrderedCollectionKind::kSortedSet,
                                      view->encoded_, location.logical_size_);
        if (!before.ok()) co_return before.status();
        const auto& directory = grouped->ordered_directory();
        // Existing Redis Sorted Set callbacks still compute a complete logical
        // result. Route it against old page boundaries so a score moving across
        // the entire set does not rewrite the unaffected intervening pages.
        auto mutation =
            PlanSortedSetRewrite(directory, *before, std::move(*after));
        if (!mutation.ok()) co_return mutation.status();
        plan = std::move(*mutation);
        if (!plan.changed_) {
          // Preserve explicit identical-value/metadata mutation semantics from
          // the callback. A single page suffices; EXPIRE/PERSIST themselves use
          // the separate root-only metadata path.
          const auto& first = directory.groups().front();
          OrderedGroupSnapshot page{
              .kind_ = directory.root().kind_,
              .incarnation_ = directory.root().incarnation_,
              .id_ = first.id_,
              .previous_ = first.previous_,
              .next_ = first.next_,
              .entries_ = {}};
          for (std::size_t i = 0; i < first.item_count_; ++i) {
            page.entries_.push_back(std::move((*before)[i]));
          }
          plan.changed_ = true;
          plan.writes_.push_back(std::move(page));
        }
      }
      co_return co_await CommitGroupedOrderedMutationLocked(
          store, partition, db_id, key, digest, grouped, std::move(plan),
          expire_at_ms, tx, replication, mutation_precondition);
    }
    absl::Status status = co_await AppendLocked(
        store, partition, db_id, key, digest,
        update->erase_ ? std::string_view{} : encoded, kind, published_type,
        expire_at_ms, tx, update->erase_ ? 0 : logical_size, nullptr, nullptr,
        replication, nullptr, true, nullptr, mutation_precondition);
    co_return status;
  } catch (const std::bad_alloc&) {
    if (value_type != ValueType::kSortedSet) throw;
    // Callback/planner allocations precede durable mutation. Group writers
    // own the fail-stop handling for any failure after root staging.
    co_return absl::ResourceExhaustedError(
        "OOM Sorted Set full-image callback allocation");
  }
}

}  // namespace keylane::storage
