#include "impl.h"
#include "keylane/storage/detail/grouped_scratch.h"

namespace keylane::storage {

Task<absl::StatusOr<TransferValue>>
StorageEngine::Impl::ReadValueForTransferLocked(std::uint8_t db_id,
                                                std::string_view key,
                                                const Digest& digest) {
  try {
    // Only the callback escapes this coroutine. Its shared owner pins the exact
    // source graph, so RENAME may delete the source on another shard while the
    // destination continues reading pages. It does not borrow an RDB session.
    struct Source {
      RetainedMemoryCharge charge_;
      WorkerStore* store_ = nullptr;
      WorkerStore::PartitionStore* partition_ = nullptr;
      WorkerStore::PartitionStore::RdbSnapshotValue saved_;
      std::string key_;
      Digest digest_{};
      std::uint8_t db_id_ = 0;
      HashGroupMap<std::uint64_t>::const_iterator hash_cursor_;
      std::size_t cursor_ = 0;
      std::uint64_t emitted_ = 0;
      bool done_ = false;
      bool reading_ = false;

      bool Valid(const Impl& engine) const {
        const auto& version = saved_.grouped_->version();
        return !engine.shutdown_flush_requested_ &&
               engine.EffectiveRecordDbEpoch(*partition_, db_id_) ==
                   version.db_epoch_ &&
               partition_->replication_epoch_ == version.replication_epoch_ &&
               partition_->grouped_generations_[db_id_] ==
                   version.index_generation_;
      }

      static Task<absl::Status> Release(Impl* engine,
                                        std::unique_ptr<Source> source) {
        const auto owner = source->store_->worker_->id();
        if (owner != celer::ThisWorker().id_) {
          // Metadata may be the last owner of source index pages. Destroy that
          // metadata on its owner too, not just the physical block pin
          // counters.
          co_return co_await celer::SubmitTaskTo(
              owner, [engine, source = std::move(source)]() mutable {
                return Release(engine, std::move(source));
              });
        }
        // Register once when the source is allocated, not when its last reader
        // disappears. Shutdown cannot miss a callback held on a different
        // shard.
        struct Settlement {
          Impl* engine_;
          std::unique_ptr<Source>* source_;
          ~Settlement() {
            source_->reset();
            engine_->active_settlements_.fetch_sub(1,
                                                   std::memory_order_acq_rel);
          }
        } settlement{engine, &source};
        co_return co_await engine->ReleaseRdbSnapshotValue(&source->saved_);
      }

      static Task<absl::StatusOr<CollectionPage>> Read(
          Impl* engine, std::shared_ptr<Source> source) {
        try {
          const auto owner = source->store_->worker_->id();
          if (owner != celer::ThisWorker().id_) {
            co_return co_await celer::SubmitTaskTo(
                owner, [engine, source] { return Read(engine, source); });
          }
          if (!source->Valid(*engine) || source->done_ || source->reading_)
            co_return absl::CancelledError(
                "collection transfer source is not active");
          struct ReadGuard {
            bool& reading_;
            ~ReadGuard() { reading_ = false; }
          } read_guard{source->reading_};
          source->reading_ = true;
          const auto object = source->saved_.grouped_;
          HashGroupId id;
          if (object->is_ordered()) {
            if (source->cursor_ >= object->ordered_directory().groups().size())
              co_return absl::DataLossError(
                  "collection transfer cursor overflow");
            id = {object->ordered_directory().groups()[source->cursor_].id_, 0};
          } else {
            if (source->hash_cursor_ == object->directory().groups().end())
              co_return absl::DataLossError(
                  "collection transfer cursor overflow");
            id = source->hash_cursor_->second.id_;
          }
          const auto* physical = object->FindGroup(id);
          if (physical == nullptr)
            co_return absl::DataLossError(
                "collection transfer page is missing");
          GroupedScratchBudget budget;
          const auto included = budget.AddGroup(
              physical->value_, object->ExtentsFor(id), source->key_.size());
          if (!included.ok()) co_return included;
          auto admission = budget.Reserve(1);
          if (!admission.ok()) co_return admission.status();
          CollectionPage page{.value_type_ =
                                  source->saved_.location_.value_type()};
          if (object->is_ordered()) {
            auto loaded = co_await engine->LoadOrderedGroupSnapshot(
                *source->store_, *source->partition_, source->db_id_,
                source->key_, source->digest_, object, id.prefix_, true);
            if (!loaded.ok()) co_return loaded.status();
            const auto count = loaded->snapshot_.entries_.size();
            if (page.value_type_ == ValueType::kList) {
              page.elements_.reserve(count);
              for (auto& entry : loaded->snapshot_.entries_)
                page.elements_.push_back(std::move(entry.value_));
            } else {
              page.scored_members_.reserve(count);
              for (auto& entry : loaded->snapshot_.entries_)
                page.scored_members_.push_back(
                    {std::move(entry.value_), entry.score_});
            }
            page.done_ = source->cursor_ + 1 ==
                         object->ordered_directory().groups().size();
          } else {
            auto loaded = co_await engine->LoadHashGroupSnapshot(
                *source->store_, *source->partition_, source->db_id_,
                source->key_, source->digest_, object, id, true);
            if (!loaded.ok()) co_return loaded.status();
            const auto count = loaded->snapshot_.value_.entries_.size();
            if (page.value_type_ == ValueType::kHash)
              page.fields_.reserve(count);
            else
              page.elements_.reserve(count);
            for (auto& entry : loaded->snapshot_.value_.entries_) {
              if (page.value_type_ == ValueType::kHash) {
                page.fields_.push_back(
                    {std::move(entry.field_), std::move(entry.value_)});
              } else {
                if (!entry.value_.empty())
                  co_return absl::DataLossError(
                      "Set transfer contains Hash payload");
                page.elements_.push_back(std::move(entry.field_));
              }
            }
            ++source->hash_cursor_;
            page.done_ =
                source->hash_cursor_ == object->directory().groups().end();
          }
          if (!source->Valid(*engine))
            co_return absl::CancelledError(
                "collection transfer population changed");
          const auto total = source->saved_.location_.logical_size_;
          if (source->emitted_ > total ||
              page.size() > total - source->emitted_ ||
              (page.done_ && page.size() != total - source->emitted_))
            co_return absl::DataLossError("collection transfer count mismatch");
          source->emitted_ += page.size();
          source->done_ = page.done_;
          page.next_cursor_ = ++source->cursor_;
          const auto bytes = page.RetainedBytes();
          if (bytes > admission->bytes())
            co_return absl::ResourceExhaustedError(
                "collection transfer page exceeds admission");
          page.retained_charge_.Adopt(&*admission, bytes);
          co_return page;
        } catch (const std::bad_alloc&) {
          // Celer terminates on an uncaught coroutine exception. Admission
          // bounds logical memory, but an allocator can still reject it.
          co_return absl::ResourceExhaustedError(
              "OOM allocating collection transfer page");
        }
      }
    };

    auto& store = CurrentStore();
    auto& partition = PartitionForKey(store, key);
    for (;;) {
      co_await store.store_state_mutex_.Lock();
      UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
      auto found = co_await FindVerifiedEntry(store, partition.indexes_[db_id],
                                              digest, key);
      if (!found.ok()) co_return found.status();
      if (*found == nullptr || (*found)->value_.kind() != RecordKind::kValue)
        co_return absl::NotFoundError("key not found");
      const auto location = MaterializeIndexLocation(**found);
      if (!location.grouped()) {
        if (IsExpiredNow(**found))
          co_return absl::NotFoundError("key not found");
        unlock.Unlock();
        auto raw = co_await ReadRawValueLocked(db_id, key, digest);
        if (!raw.ok()) co_return raw.status();
        co_return TransferValue{.metadata_ = std::move(*raw), .reader_ = {}};
      }
      auto object = partition.grouped_objects_[db_id].Lookup(
          key, GroupedObjectVersion{
                   .root_ = location,
                   .db_epoch_ = EffectiveRecordDbEpoch(partition, db_id),
                   .replication_epoch_ = partition.replication_epoch_,
                   .index_generation_ = partition.grouped_generations_[db_id]});
      if (!object.ok()) co_return object.status();
      // A failed publication is an error even if its uncommitted TTL would
      // make the key appear expired. Never disguise an uncertain root as nil.
      if (IsExpiredNow(**found)) co_return absl::NotFoundError("key not found");
      if (*object == nullptr)
        co_return absl::DataLossError("collection transfer view is missing");
      if (key.size() >
          std::numeric_limits<std::size_t>::max() - sizeof(Source) - 256)
        co_return absl::ResourceExhaustedError(
            "collection transfer metadata overflow");
      const auto budget = sizeof(Source) + key.size() + 256;
      auto admission = TryReserveMemory(budget);
      if (!admission) {
        RecordMemoryRejection();
        co_return absl::ResourceExhaustedError(
            "OOM collection transfer metadata");
      }
      // The callback may die on the destination owner. Its deleter schedules an
      // explicitly tracked release; it never modifies source pin state
      // remotely.
      auto raw_source = std::make_unique<Source>();
      raw_source->store_ = &store;
      raw_source->partition_ = &partition;
      active_settlements_.fetch_add(1, std::memory_order_acq_rel);
      std::shared_ptr<Source> source(
          raw_source.release(), [this](Source* value) {
            celer::ThisWorker().self_->Spawn(
                Source::Release(this, std::unique_ptr<Source>(value)));
          });
      source->key_ = key;
      source->db_id_ = db_id;
      source->digest_ = digest;
      source->saved_.location_ = location;
      source->saved_.extents_ = ExtentsFor(store, *found);
      source->saved_.grouped_ = std::move(*object);
      source->charge_.Adopt(&*admission, budget);
      auto prepared = PrepareGroupedSnapshotPins(&source->saved_);
      if (!prepared.ok()) co_return prepared;
      unlock.Unlock();
      const auto pinned = co_await PinRdbSnapshotValue(&source->saved_);
      if (!pinned.ok()) {
        if (pinned.code() == absl::StatusCode::kAborted &&
            !shutdown_flush_requested_) {
          // GC may need this same owner to finish relocating the block. Release
          // the failed snapshot and yield so neither GC nor pin cleanup
          // starves.
          source.reset();
          co_await celer::Yield(*store.worker_);
          continue;
        }
        co_return pinned;
      }
      if (!source->Valid(*this))
        co_return absl::CancelledError(
            "collection transfer population changed");
      if (!source->saved_.grouped_->is_ordered())
        source->hash_cursor_ =
            source->saved_.grouped_->directory().groups().begin();
      TransferValue result;
      result.metadata_.logical_size_ = location.logical_size_;
      result.metadata_.expire_at_ms_ = location.expire_at_ms_;
      result.metadata_.value_type_ = location.value_type();
      result.reader_ = [this, source] { return Source::Read(this, source); };
      co_return result;
    }
  } catch (const std::bad_alloc&) {
    co_return absl::ResourceExhaustedError(
        "OOM allocating collection transfer metadata");
  }
}

Task<absl::Status> StorageEngine::Impl::WriteValueForTransferLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const TransferValue& value, TxShardWrites* tx,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  if (!value.reader_)
    co_return co_await WriteRawValueLocked(db_id, key, digest, value.metadata_,
                                           tx, replication,
                                           mutation_precondition);
  auto restored = co_await RestoreCollectionValueLocked(
      db_id, key, digest, value.metadata_.value_type_,
      value.metadata_.expire_at_ms_, true, value.metadata_.logical_size_,
      value.reader_, tx, replication, mutation_precondition);
  co_return restored.ok() ? absl::OkStatus() : restored.status();
}

Task<absl::StatusOr<TransferValue>> StorageEngine::ReadValueForTransferLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest) {
  return impl_->ReadValueForTransferLocked(db_id, key, digest);
}

Task<absl::Status> StorageEngine::WriteValueForTransferLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const TransferValue& value, TxShardWrites* tx,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  return impl_->WriteValueForTransferLocked(db_id, key, digest, value, tx,
                                            replication,
                                            mutation_precondition);
}

}  // namespace keylane::storage
