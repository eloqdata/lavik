#include "impl.h"

namespace keylane::storage {
namespace {

using RetiredExtentIdentity = std::pair<std::uint64_t, std::uint64_t>;

bool PhysicalLess(const RetiredRecord& left, const RetiredRecord& right) {
  return std::tie(left.block_id_, left.allocation_epoch_, left.record_offset_) <
         std::tie(right.block_id_, right.allocation_epoch_,
                  right.record_offset_);
}

bool SamePhysical(const RetiredRecord& left, const RetiredRecord& right) {
  return left.block_id_ == right.block_id_ &&
         left.allocation_epoch_ == right.allocation_epoch_ &&
         left.record_offset_ == right.record_offset_;
}

struct OwnedRetirementManifest {
  RetainedMemoryCharge charge_;
  std::vector<ExtentRef> refs_;
};

struct GroupedRetirementCharge {
  RetainedMemoryCharge charge_;
};

absl::StatusOr<ExtentManifest> UnsharedGroupedExtents(
    const ExtentManifest& previous,
    const std::vector<RetiredExtentIdentity>& replacement_extents) {
  if (previous == nullptr || previous->empty()) return ExtentManifest{};
  auto retained = [&](const ExtentRef& ref) {
    return std::binary_search(
        replacement_extents.begin(), replacement_extents.end(),
        RetiredExtentIdentity{ref.block_id_, ref.allocation_epoch_});
  };
  const auto removed = static_cast<std::size_t>(
      std::count_if(previous->begin(), previous->end(), retained));
  if (removed == 0) return previous;  // Keep its existing admitted owner.
  if (removed == previous->size()) return ExtentManifest{};
  const auto count = previous->size() - removed;
  constexpr std::size_t overhead =
      sizeof(OwnedRetirementManifest) + 4 * sizeof(void*);
  auto reservation = TryReserveMemory(overhead + count * sizeof(ExtentRef));
  if (!reservation) {
    RecordMemoryRejection();
    return absl::ResourceExhaustedError(
        "OOM grouped retirement manifest admission failed");
  }
  auto owner = std::make_shared<OwnedRetirementManifest>();
  owner->refs_.reserve(count);
  for (const auto& ref : *previous) {
    if (!retained(ref)) owner->refs_.push_back(ref);
  }
  owner->charge_.Adopt(&*reservation,
                       overhead + owner->refs_.capacity() * sizeof(ExtentRef));
  const auto* view = &owner->refs_;
  return ExtentManifest(std::move(owner), view);
}

}  // namespace

absl::StatusOr<std::vector<RetiredRecord>>
StorageEngine::Impl::CollectGroupedRetirements(
    const GroupedHashObject::Handle& previous,
    const GroupedHashObject::Handle& replacement,
    std::optional<std::span<const HashGroupId>> touched) {
  std::vector<RetiredRecord> result;
  if (previous == nullptr || previous == replacement) return result;
  const auto records = touched ? touched->size() : previous->record_count();
  if (records == 0) return result;
  const auto maximum = std::numeric_limits<std::size_t>::max();
  constexpr std::size_t owner_bytes = sizeof(GroupedRetirementCharge) + 1024;
  // The prepared batch, pin guard, undo and transaction/commit copies may
  // coexist. Charge that bounded peak conservatively through one shared owner;
  // the ordinary String staging identity remains unchanged.
  constexpr std::size_t record_bytes =
      4 * sizeof(RetiredRecord) + sizeof(TxShardWrites::Retired);
  if (records > (maximum - owner_bytes) / record_bytes) {
    return absl::ResourceExhaustedError(
        "grouped retirement batch is too large");
  }
  auto reservation = TryReserveMemory(owner_bytes + records * record_bytes);
  if (!reservation) {
    RecordMemoryRejection();
    return absl::ResourceExhaustedError(
        "OOM grouped retirement batch admission failed");
  }
  std::vector<RetiredExtentIdentity> replacement_extents;
  result.reserve(records);
  absl::Status status;
  auto collect = [&](HashGroupId id, const RecordIndex::Entry& old_entry,
                     const ExtentManifest& old_extents, bool) {
    if (!status.ok()) return;
    // The adapter calls this before suspension/publication. Never defer
    // materialization of a compact old entry until after the root's fence:
    // that would substitute a reused BlockState's allocation epoch.
    const RecordLocation old_location = MaterializeIndexLocation(old_entry);
    replacement_extents.clear();
    if (replacement != nullptr) {
      const auto* next = replacement->FindRecord(id);
      if (next != nullptr &&
          old_location.SamePhysicalRecord(MaterializeIndexLocation(*next)))
        return;
      // The side-index ownership contract forbids two different group ids
      // from sharing extents. Only the same id's physical replacement can
      // retain this manifest; inspecting unrelated groups would make a
      // one-field HSET scale with the whole object.
      if (auto extents = replacement->ExtentsFor(id); extents != nullptr) {
        for (const auto& ref : *extents)
          replacement_extents.emplace_back(ref.block_id_,
                                           ref.allocation_epoch_);
        std::sort(replacement_extents.begin(), replacement_extents.end());
      }
    }
    auto extents = UnsharedGroupedExtents(old_extents, replacement_extents);
    if (!extents.ok()) {
      status = extents.status();
      return;
    }
    RetiredRecord retired = RetiredRecordOf(old_location);
    // The checked outer header supplies group identity/retirement state,
    // so a stale value-only payload is not needed for winner selection.
    // An external parent key remains necessary to classify that record
    // until its source block disappears.
    if (old_location.key_external())
      retired.dependent_extents_ = std::move(*extents);
    else
      retired.immediate_extents_ = std::move(*extents);
    result.push_back(std::move(retired));
  };
  if (touched) {
    std::vector<HashGroupId> ids(touched->begin(), touched->end());
    std::sort(ids.begin(), ids.end());
    if (std::adjacent_find(ids.begin(), ids.end()) != ids.end())
      return absl::InvalidArgumentError(
          "group retirement repeats a touched id");
    for (const auto id : ids) {
      if (const auto* entry = previous->FindRecord(id); entry != nullptr)
        collect(id, *entry, previous->ExtentsFor(id), false);
    }
  } else {
    previous->ForEachRecord(collect);
  }
  if (!status.ok()) return status;
  if (!result.empty()) {
    auto owner = std::make_shared<GroupedRetirementCharge>();
    owner->charge_.Adopt(&*reservation,
                         owner_bytes + result.capacity() * record_bytes);
    for (auto& record : result) record.retained_owner_ = owner;
  }
  return result;
}

StorageEngine::Impl::GroupedRetirementPins::~GroupedRetirementPins() {
  if (std::none_of(pins_.begin(), pins_.end(),
                   [](const auto& pin) { return pin.dependency_pinned_; }))
    return;
  engine_->active_settlements_.fetch_add(1, std::memory_order_acq_rel);
  store_->worker_->Spawn(
      engine_->ReleaseGroupedRetirementPins(store_, std::move(pins_)));
}

void StorageEngine::Impl::GroupedRetirementPins::Sort() {
  std::sort(pins_.begin(), pins_.end(), PhysicalLess);
  sorted_count_ = pins_.size();
}

bool StorageEngine::Impl::GroupedRetirementPins::Contains(
    const RetiredRecord& record) const {
  const auto end = pins_.begin() + sorted_count_;
  const auto found = std::lower_bound(pins_.begin(), end, record, PhysicalLess);
  return found != end && SamePhysical(*found, record) &&
         found->dependency_pinned_;
}

bool StorageEngine::Impl::GroupedRetirementPins::Take(
    const RetiredRecord& record) {
  const auto end = pins_.begin() + sorted_count_;
  const auto found = std::lower_bound(pins_.begin(), end, record, PhysicalLess);
  if (found == end || !SamePhysical(*found, record) ||
      !found->dependency_pinned_)
    return false;
  found->dependency_pinned_ = false;
  return true;
}

Task<absl::Status> StorageEngine::Impl::PrepinGroupedRetirementsLocked(
    WorkerStore& store, const GroupedHashObject::Handle& previous,
    std::optional<std::span<const HashGroupId>> touched, bool include_root,
    std::unique_ptr<GroupedRetirementPins>* pins) {
  if (previous == nullptr) co_return absl::OkStatus();
  auto records = CollectGroupedRetirements(previous, nullptr, touched);
  if (!records.ok()) co_return records.status();
  if (include_root && previous->version().root_.tx_tagged()) {
    // TTL-only writes have an empty child set, but still supersede a tagged
    // root. Its transaction block can belong to another physical owner after
    // restart with a different worker layout. Pin before staging, exactly as
    // for children; a key-owner-local post-publication lookup can miss it.
    constexpr std::size_t bytes = sizeof(GroupedRetirementCharge) +
                                  4 * sizeof(void*) + 4 * sizeof(RetiredRecord);
    auto reservation = TryReserveMemory(bytes);
    if (!reservation) {
      RecordMemoryRejection();
      co_return absl::ResourceExhaustedError(
          "OOM grouped root dependency admission failed");
    }
    auto owner = std::make_shared<GroupedRetirementCharge>();
    owner->charge_.Adopt(&*reservation, bytes);
    auto root = RetiredRecordOf(previous->version().root_);
    root.retained_owner_ = std::move(owner);
    records->push_back(std::move(root));
  }
  if (std::none_of(records->begin(), records->end(),
                   [](const auto& record) { return record.tx_tagged_; }))
    co_return absl::OkStatus();
  if (*pins == nullptr) {
    auto reservation = TryReserveMemory(sizeof(GroupedRetirementPins) + 32);
    if (!reservation) {
      RecordMemoryRejection();
      co_return absl::ResourceExhaustedError(
          "OOM group dependency guard admission failed");
    }
    *pins = std::make_unique<GroupedRetirementPins>(this, &store);
    (*pins)->charge_.Adopt(&*reservation, sizeof(GroupedRetirementPins) + 32);
  }
  auto& guard = **pins;
  guard.Sort();
  // Each new candidate retains the charged batch owner. reserve happens before
  // the first successful physical pin so allocation failure cannot strand one.
  guard.pins_.reserve(guard.pins_.size() + records->size());
  for (auto& record : *records) {
    if (!record.tx_tagged_ || guard.Contains(record)) continue;
    const auto owner = record.block_owner_;
    if (owner >= worker_count_)
      co_return absl::DataLossError("group dependency has no owner");
    auto pin = [this, owner, record]() -> Task<absl::Status> {
      auto& target = *stores_[owner];
      co_await target.store_state_mutex_.Lock();
      UnlockGuard unlock(&target.store_state_mutex_, target.worker_);
      const auto found = target.tx_blocks_.find(record.block_id_);
      if (found == target.tx_blocks_.end() ||
          found->second.allocation_epoch_ != record.allocation_epoch_)
        co_return absl::AbortedError("group dependency moved before pin");
      if (found->second.dependency_pins_ ==
          std::numeric_limits<std::uint32_t>::max())
        co_return absl::ResourceExhaustedError(
            "group dependency pin count exhausted");
      ++found->second.dependency_pins_;
      tx_cleaner_dirty_.store(true, std::memory_order_release);
      co_return absl::OkStatus();
    };
    // No staging pointer or committed offset has been captured at this point.
    // Reacquire the caller's mutex on every outcome, then its append loop must
    // resolve the key/view and capacity again before root construction.
    store.store_state_mutex_.Unlock(*store.worker_);
    auto status = owner == store.worker_->id()
                      ? co_await pin()
                      : co_await celer::SubmitTaskTo(owner, pin);
    co_await store.store_state_mutex_.Lock();
    if (!status.ok()) {
      guard.Sort();
      co_return status;
    }
    record.dependency_pinned_ = true;
    guard.pins_.push_back(std::move(record));
  }
  guard.Sort();
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::ReleaseGroupedRetirementPins(
    WorkerStore* store, std::vector<RetiredRecord> pins) {
  struct Settlement {
    std::atomic<std::uint32_t>* active_;
    ~Settlement() { active_->fetch_sub(1, std::memory_order_acq_rel); }
  } settlement{&active_settlements_};
  for (const auto& record : pins) {
    if (!record.dependency_pinned_) continue;
    const auto owner = record.block_owner_;
    auto release = [this, owner, record]() -> Task<absl::Status> {
      auto& target = *stores_[owner];
      co_await target.store_state_mutex_.Lock();
      UnlockGuard unlock(&target.store_state_mutex_, target.worker_);
      UnpinTxDependencyLocal(target, record.block_id_,
                             record.allocation_epoch_);
      co_return absl::OkStatus();
    };
    if (owner == store->worker_->id())
      (void)co_await release();
    else
      (void)co_await celer::SubmitTaskTo(owner, release);
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::ClearGroupedUndoSlots(
    WorkerStore& store, const TxUndoLog& undo) {
  for (const auto& item : undo.entries_) {
    // A compact-to-grouped promotion also leaves a reserved null slot when
    // rollback restores the compact value. Its new-graph discard batch is
    // the ownership evidence even though there was no previous grouped view.
    if (item.previous_grouped_ == nullptr &&
        item.applied_grouped_retirements_ == nullptr)
      continue;
    const auto* current = undo.Current(item.entry_handle_);
    if (current->value_.grouped()) continue;
    std::string external_key;
    if (!current->key_complete()) {
      auto key = co_await LoadOutOfIndexKey(
          store, MaterializeIndexLocation(*current), ExtentsFor(store, current),
          current->logical_key_size());
      if (!key.ok()) co_return key.status();
      external_key = std::move(*key);
    }
    const auto key = current->key_complete() ? current->key()
                                             : std::string_view(external_key);
    PartitionForKey(store, key)
        .grouped_objects_[item.db_id_]
        .EraseNullSlot(key);
  }
  co_return absl::OkStatus();
}

}  // namespace keylane::storage
