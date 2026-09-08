#include <bit>
#include <deque>
#include <map>

#include "impl.h"
#include "keylane/storage/detail/grouped_scratch.h"

namespace keylane::storage {

Task<absl::StatusOr<StorageEngine::Impl::SortedSetMemberMutation>>
StorageEngine::Impl::PrepareSortedSetMembers(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    GroupedHashObject::Handle previous,
    const OrderedCollectionMutationPlan& ordered) {
  SortedSetMemberMutation result;
  if (ordered.root_.kind_ != OrderedCollectionKind::kSortedSet ||
      (previous && !previous->has_member_index()))
    co_return result;

  auto add_group = [&](GroupedScratchBudget& budget,
                       HashGroupId id) -> absl::Status {
    // Prior IO may have allowed physical relocation. Never use an old
    // manifest to admit the next read; the loader refreshes again before IO.
    const auto current =
        partition.grouped_objects_[db_id].CurrentForMutation(key);
    if (!current || !current->SameLogicalRoot(*previous) ||
        current->version().db_epoch_ != previous->version().db_epoch_ ||
        current->version().replication_epoch_ !=
            previous->version().replication_epoch_ ||
        current->version().index_generation_ !=
            previous->version().index_generation_)
      return absl::AbortedError("member-index source population changed");
    const auto* entry = current->FindGroup(id);
    if (!entry) return absl::DataLossError("missing member-index source page");
    return budget.AddGroup(entry->value_, current->ExtentsFor(id), key.size());
  };
  GroupedScratchBudget metadata_budget;
  absl::Status status;
  for (const auto& page : ordered.writes_) {
    std::uint64_t count = page.entries_.size();
    if (previous) {
      if (const auto* old = previous->ordered_directory().Find(page.id_))
        count += old->item_count_;
    }
    if (count > std::numeric_limits<std::size_t>::max() / 256)
      co_return absl::ResourceExhaustedError("member-index metadata overflow");
    status = metadata_budget.AddBytes(count * 256);
    if (!status.ok()) co_return status;
  }
  auto metadata_admission = metadata_budget.Reserve(1);
  if (!metadata_admission.ok()) co_return metadata_admission.status();

  struct Change {
    std::optional<double> before_;
    std::optional<double> after_;
  };
  struct RemovedMember {
    MemoryReservation admission_;
    std::string member_;
  };
  // After-image strings already belong to the caller's admitted plan. Read
  // old pages one at a time and borrow those after strings for matching
  // members; retain an old string only when that member was actually removed.
  // Holding all old pages here would multiply the large-member mutation's peak.
  std::deque<RemovedMember> removed;
  absl::flat_hash_map<std::string_view, Change> changes;
  for (const auto& page : ordered.writes_) {
    for (const auto& entry : page.entries_) {
      auto& change = changes[entry.value_];
      if (change.after_)
        co_return absl::DataLossError("duplicate ordered replacement member");
      change.after_ = entry.score_;
    }
  }
  for (const auto& page : ordered.writes_) {
    if (!previous || !previous->ordered_directory().Find(page.id_)) continue;
    GroupedScratchBudget read_budget;
    status = add_group(read_budget, {page.id_, 0});
    if (!status.ok()) co_return status;
    auto read_admission = read_budget.Reserve(2);
    if (!read_admission.ok()) co_return read_admission.status();
    auto loaded = co_await LoadOrderedGroupSnapshot(
        store, partition, db_id, key, digest, previous, page.id_);
    if (!loaded.ok()) co_return loaded.status();
    for (auto& entry : loaded->snapshot_.entries_) {
      auto found = changes.find(entry.value_);
      if (found == changes.end()) {
        auto admission = TryReserveMemory(entry.value_.capacity() + 1);
        if (!admission) {
          RecordMemoryRejection();
          co_return absl::ResourceExhaustedError(
              "OOM grouped operation scratch admission");
        }
        removed.push_back({std::move(*admission), std::move(entry.value_)});
        found = changes.try_emplace(removed.back().member_).first;
      }
      if (found->second.before_)
        co_return absl::DataLossError("duplicate ordered source member");
      found->second.before_ = entry.score_;
    }
  }
  absl::erase_if(changes, [](const auto& item) {
    const auto& change = item.second;
    return change.before_ && change.after_ &&
           std::bit_cast<std::uint64_t>(*change.before_) ==
               std::bit_cast<std::uint64_t>(*change.after_);
  });
  // Only newly inserted members need another string copy in the Hash graph.
  // Score updates replace eight value bytes in place in the decoded leaf.
  // Prefix splitting moves strings, and large-page encoding streams them;
  // entry overhead covers vector growth and validation, not N payload copies.
  GroupedScratchBudget incoming;
  for (const auto& [member, change] : changes) {
    if (!change.after_ || change.before_) continue;
    status = incoming.AddBytes(member.size() + 256);
    if (!status.ok()) co_return status;
  }
  auto scratch = incoming.Reserve(1);
  if (!scratch.ok()) co_return scratch.status();
  result.scratch_ = std::move(*scratch);
  auto& plan = result.plan_;
  if (!previous) {
    plan.root_ = {.incarnation_ = ordered.root_.incarnation_,
                  .seed_ = CurrentDigestSeed(),
                  .field_count_ = ordered.root_.item_count_,
                  .revision_ = ordered.root_.revision_};
    HashValue value;
    value.entries_.reserve(changes.size());
    for (const auto& [member, change] : changes) {
      value.entries_.push_back(
          {.digest_ = ComputeDigest(member),
           .field_ = std::string(member),
           .value_ = EncodeSortedSetMemberScore(*change.after_)});
    }
    if (value.entries_.size() != plan.root_.field_count_)
      co_return absl::DataLossError("member-index promotion count mismatch");
    auto groups = GroupHashValue(std::move(value), plan.root_.incarnation_,
                                 plan.root_.seed_);
    if (!groups.ok()) co_return groups.status();
    if (groups->size() > std::numeric_limits<std::uint32_t>::max())
      co_return absl::OutOfRangeError("too many member-index groups");
    plan.root_.group_count_ = groups->size();
    plan.writes_ = std::move(*groups);
    plan.changed_ = true;
    co_return result;
  }
  plan.root_ = previous->directory().root();
  if (changes.empty()) co_return result;

  std::map<HashGroupId, HashGroupSnapshot> leaves;
  GroupedScratchBudget leaf_budget;
  for (const auto& [member, change] : changes) {
    const auto* route = previous->directory().Find(member);
    if (!route) co_return absl::DataLossError("member has no prefix route");
    if (leaves.try_emplace(route->id_).second) {
      status = add_group(leaf_budget, route->id_);
      if (!status.ok()) co_return status;
    }
  }
  // One retained decoded leaf plus decoder/inline encoder headroom. The
  // incoming member copies have their own reservation above.
  auto admission = leaf_budget.Reserve(2);
  if (!admission.ok()) co_return admission.status();
  result.leaves_ = std::move(*admission);
  for (auto& [id, leaf] : leaves) {
    auto loaded = co_await LoadHashGroupSnapshot(store, partition, db_id, key,
                                                 digest, previous, id);
    if (!loaded.ok()) co_return loaded.status();
    leaf = std::move(loaded->snapshot_);
    for (const auto& entry : leaf.value_.entries_) {
      const auto found = changes.find(entry.field_);
      if (found == changes.end()) continue;
      auto score = DecodeSortedSetMemberScore(entry.value_);
      if (!score.ok()) co_return score.status();
      if (!found->second.before_ || *score != *found->second.before_)
        co_return absl::DataLossError("ordered/member-index score mismatch");
    }
  }
  // Verify exact presence before applying changes; a missing old member
  // must not silently heal a corrupt index under a new root.
  for (const auto& [member, change] : changes) {
    auto& entries =
        leaves.at(previous->directory().Find(member)->id_).value_.entries_;
    const auto found =
        std::find_if(entries.begin(), entries.end(),
                     [&](const auto& entry) { return entry.field_ == member; });
    if ((found != entries.end()) != change.before_.has_value())
      co_return absl::DataLossError("ordered/member-index membership mismatch");
  }
  for (auto& [id, leaf] : leaves) {
    for (auto& entry : leaf.value_.entries_) {
      const auto changed = changes.find(entry.field_);
      if (changed != changes.end() && changed->second.after_)
        entry.value_ = EncodeSortedSetMemberScore(*changed->second.after_);
    }
    std::erase_if(leaf.value_.entries_, [&](const auto& entry) {
      const auto changed = changes.find(entry.field_);
      return changed != changes.end() && !changed->second.after_;
    });
  }
  for (const auto& [member, change] : changes) {
    if (!change.after_ || change.before_) continue;
    leaves.at(previous->directory().Find(member)->id_)
        .value_.entries_.push_back(
            {.digest_ = ComputeDigest(member),
             .field_ = std::string(member),
             .value_ = EncodeSortedSetMemberScore(*change.after_)});
  }
  plan.root_.revision_ = ordered.root_.revision_;
  plan.root_.field_count_ = ordered.root_.item_count_;
  plan.changed_ = true;
  for (auto& [id, leaf] : leaves) {
    auto split = SplitHashGroup(std::move(leaf), plan.root_.seed_);
    if (!split.ok()) co_return split.status();
    if (split->size() > 1) {
      if (split->size() - 1 >
          std::numeric_limits<std::uint32_t>::max() - plan.root_.group_count_)
        co_return absl::OutOfRangeError("too many member-index groups");
      plan.root_.group_count_ += split->size() - 1;
      plan.writes_.push_back({.incarnation_ = plan.root_.incarnation_,
                              .id_ = id,
                              .retired_ = true,
                              .value_ = {}});
    }
    for (auto& leaf : *split) plan.writes_.push_back(std::move(leaf));
  }
  co_return result;
}

}  // namespace keylane::storage
