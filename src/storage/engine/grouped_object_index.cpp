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

#include "lavik/storage/detail/grouped_object_index.h"

#include <algorithm>
#include <array>
#include <bit>
#include <tuple>

#include "lavik/local_shared_ptr.h"

namespace lavik::storage {
namespace {

// The binary identity includes prefix length: splitting a zero-prefixed
// parent creates a child with the same prefix but a different identity.
std::array<char, 9> GroupKey(HashGroupId id) {
  std::array<char, 9> key{};
  for (unsigned i = 0; i < 8; ++i) key[i] = id.prefix_ >> (i * 8);
  key[8] = id.bits_;
  return key;
}

// Shared production arenas use external admission. Reserve before any map
// mutation; the arena accounts the actual retained allocation itself. Local
// test/recovery callers use the same contract, not an unaccounted heap map.
template <typename Map>
absl::Status Insert(Map& map, std::string_view key,
                    const typename Map::StoredValue& value) {
  const Digest digest = ComputeDigest(key);
  if (!map.CanAllocateEntry(key, true, false)) {
    return absl::ResourceExhaustedError(
        "grouped index entry capacity exhausted");
  }
  const auto bytes =
      map.RequiredAllocationBytes(digest, key, true, false, true);
  auto reservation = TryReserveMemory(bytes);
  if (!reservation.has_value()) {
    RecordMemoryRejection();
    return absl::ResourceExhaustedError("OOM grouped index exceeds maxmemory");
  }
  if (map.InsertNew(digest, key, value) == nullptr) {
    return absl::ResourceExhaustedError("grouped index allocation failed");
  }
  return absl::OkStatus();
}

std::shared_ptr<ScanHashMapEntryArena> MakeArena() {
  return std::make_shared<ScanHashMapEntryArena>(
      ScanHashMapEntryArena::kMaximumPageId, true, false,
      CurrentMemoryAccountingShard());
}

struct OwnedGroupManifest {
  // Readers can retain a manifest after the directory disappears. Keep the
  // payload's charge in the same shared ownership as the actual vector.
  RetainedMemoryCharge payload_charge_;
  std::vector<ExtentRef> refs_;
};

absl::StatusOr<std::shared_ptr<const std::vector<ExtentRef>>> CopyManifest(
    const std::vector<ExtentRef>& refs, RetainedAllocationDomain domain) {
  const auto payload_bytes =
      AllocatorUsableSizeForRequest(refs.size() * sizeof(ExtentRef));
  auto reservation =
      TryReserveMemory(payload_bytes + AllocatorUsableSizeForRequest(
                                           sizeof(OwnedGroupManifest) + 1024));
  if (!reservation.has_value()) {
    RecordMemoryRejection();
    return absl::ResourceExhaustedError("OOM group manifest exceeds maxmemory");
  }
  auto owner = std::allocate_shared<OwnedGroupManifest>(
      RetainedAllocator<OwnedGroupManifest>(domain));
  owner->payload_charge_.Account(domain.owner_shard_, payload_bytes);
  owner->refs_ = refs;
  const auto* view = &owner->refs_;
  return std::shared_ptr<const std::vector<ExtentRef>>(std::move(owner), view);
}

bool ValidGroupLocation(const RecordLocation& location) {
  return location.kind() == RecordKind::kValue &&
         (location.value_type() == ValueType::kHash ||
          location.value_type() == ValueType::kString ||
          location.value_type() == ValueType::kSet ||
          location.value_type() == ValueType::kList ||
          location.value_type() == ValueType::kSortedSet ||
          location.value_type() == ValueType::kStream) &&
         !location.grouped() && location.expire_at_ms_ == 0 &&
         location.allocation_epoch() != 0 &&
         location.record_offset() >= kBlockHeaderBytes &&
         location.total_disk_bytes() != 0 &&
         location.total_disk_bytes() <=
             kStorageBlockBytes - location.record_offset();
}

template <typename T>
absl::StatusOr<std::shared_ptr<T>> AllocateObject(
    const std::shared_ptr<ScanHashMapEntryArena>& arena) {
  auto reservation =
      TryReserveMemory(AllocatorUsableSizeForRequest(sizeof(T) + 1024));
  if (!reservation) {
    RecordMemoryRejection();
    return absl::ResourceExhaustedError(
        "OOM grouped metadata exceeds maxmemory");
  }
  return std::allocate_shared<T>(
      RetainedAllocator<T>(arena->allocation_domain()));
}

template <typename T>
absl::StatusOr<LocalSharedPtr<T>> AllocateLocalObject(
    const std::shared_ptr<ScanHashMapEntryArena>& arena) {
  auto reservation =
      TryReserveMemory(AllocatorUsableSizeForRequest(sizeof(T) + 1024));
  if (!reservation) {
    RecordMemoryRejection();
    return absl::ResourceExhaustedError(
        "OOM grouped metadata exceeds maxmemory");
  }
  return AllocateLocalShared<T>(
      RetainedAllocator<T>(arena->allocation_domain()));
}

struct OwnedDirectory {
  HashGroupDirectory directory_;
};

struct OwnedOrderedDirectory {
  RetainedMemoryCharge charge_;
  OrderedGroupDirectory directory_;
};

absl::StatusOr<std::shared_ptr<const OrderedGroupDirectory>> OwnDirectory(
    OrderedGroupDirectory directory,
    const std::shared_ptr<ScanHashMapEntryArena>& arena) {
  auto reservation = TryReserveMemory(directory.RetainedBytes());
  if (!reservation) {
    RecordMemoryRejection();
    return absl::ResourceExhaustedError(
        "OOM ordered directory exceeds maxmemory");
  }
  auto owner = AllocateObject<OwnedOrderedDirectory>(arena);
  if (!owner.ok()) return owner.status();
  (*owner)->charge_.Adopt(&*reservation, directory.RetainedBytes());
  (*owner)->directory_ = std::move(directory);
  const auto* view = &(*owner)->directory_;
  return std::shared_ptr<const OrderedGroupDirectory>(std::move(*owner), view);
}

absl::Status ValidateRoot(const GroupedObjectVersion& version,
                          const OrderedGroupDirectory& directory) {
  const auto& root = version.root_;
  const auto type = OrderedValueType(directory.root().kind_);
  if (!root.grouped() || root.kind() != RecordKind::kValue ||
      root.value_type() != type || root.mutation_sequence_ == 0 ||
      root.mutation_sequence_ < directory.command_sequence() ||
      root.logical_size_ != directory.root().logical_size() ||
      (root.logical_size_ == 0 && type != ValueType::kStream) ||
      root.allocation_epoch() == 0 ||
      root.record_offset() < kBlockHeaderBytes ||
      root.record_offset() >= kStorageBlockBytes ||
      root.total_disk_bytes() == 0 ||
      root.total_disk_bytes() > kStorageBlockBytes - root.record_offset() ||
      version.db_epoch_ == 0 || version.replication_epoch_ == 0) {
    return absl::DataLossError("ordered root/directory mismatch");
  }
  return absl::OkStatus();
}

absl::Status ValidateLocation(const HashGroupLocation& group,
                              const GroupedObjectVersion& version,
                              const HashGroupDirectory& directory);

absl::Status ValidateLocation(const HashGroupLocation& group,
                              const GroupedObjectVersion& version,
                              const OrderedGroupDirectory& directory) {
  if (!IsOrderedPageId(group.id_)) {
    if (const auto* members = directory.member_directory())
      return ValidateLocation(group, version, *members);
    return absl::DataLossError("member page without a member index");
  }
  const auto& location = group.location_;
  const auto* route = directory.FindRecord(group.id_.prefix_);
  if (group.id_.prefix_ == 0 || group.id_.bits_ != 0 ||
      !ValidGroupLocation(location) ||
      location.value_type() != version.root_.value_type() ||
      location.mutation_sequence_ == 0 ||
      location.mutation_sequence_ > directory.sequence() ||
      location.external() != (group.extents_ != nullptr) ||
      location.SamePhysicalRecord(version.root_) || route == nullptr ||
      route->sequence_ != location.mutation_sequence_ ||
      route->item_count_ != location.logical_size_ ||
      route->retired_ != group.retired_) {
    return absl::DataLossError("ordered page location/route mismatch");
  }
  if (group.extents_) {
    if (group.extents_->empty() || group.extents_->size() > kMaxStringExtents)
      return absl::DataLossError("invalid ordered extent count");
    std::uint64_t bytes = 0;
    for (const auto& extent : *group.extents_) {
      if (extent.block_id_ == kInvalidBlockId ||
          extent.allocation_epoch_ == 0 || extent.payload_bytes_ == 0 ||
          extent.payload_bytes_ > kExtentPayloadBytes ||
          extent.payload_bytes_ > kMaxRecordPayloadBytes - bytes)
        return absl::DataLossError("invalid ordered extent reference");
      bytes += extent.payload_bytes_;
    }
  }
  return absl::OkStatus();
}

absl::StatusOr<std::shared_ptr<const HashGroupDirectory>> OwnDirectory(
    HashGroupDirectory directory,
    const std::shared_ptr<ScanHashMapEntryArena>& arena) {
  auto owner = AllocateObject<OwnedDirectory>(arena);
  if (!owner.ok()) return owner.status();
  // Persistent routing nodes carry their own retained charges. Charging the
  // logical node count here would charge unchanged shared paths repeatedly.
  (*owner)->directory_ = std::move(directory);
  const auto* view = &(*owner)->directory_;
  return std::shared_ptr<const HashGroupDirectory>(std::move(*owner), view);
}

absl::Status ValidateRoot(const GroupedObjectVersion& version,
                          const HashGroupDirectory& directory) {
  const auto& root = version.root_;
  if (!root.grouped() || root.kind() != RecordKind::kValue ||
      (root.value_type() != ValueType::kHash &&
       root.value_type() != ValueType::kSet) ||
      root.mutation_sequence_ == 0 || root.logical_size_ == 0 ||
      root.mutation_sequence_ < directory.command_sequence() ||
      root.logical_size_ != directory.root().field_count_ ||
      root.allocation_epoch() == 0 ||
      root.record_offset() < kBlockHeaderBytes ||
      root.record_offset() >= kStorageBlockBytes ||
      root.total_disk_bytes() == 0 ||
      root.total_disk_bytes() > kStorageBlockBytes - root.record_offset() ||
      version.db_epoch_ == 0 || version.replication_epoch_ == 0) {
    return absl::DataLossError("grouped object root/directory mismatch");
  }
  return absl::OkStatus();
}

absl::Status ValidateLocation(const HashGroupLocation& group,
                              const GroupedObjectVersion& version,
                              const HashGroupDirectory& directory) {
  const auto& location = group.location_;
  if (!group.id_.valid() || !ValidGroupLocation(location) ||
      location.value_type() != version.root_.value_type() ||
      location.mutation_sequence_ == 0 ||
      location.mutation_sequence_ > directory.sequence() ||
      location.external() != (group.extents_ != nullptr) ||
      location.SamePhysicalRecord(version.root_)) {
    return absl::DataLossError("grouped object has invalid physical group");
  }
  if (group.retired_) {
    // A retired payload is tiny, but an external parent key can still require
    // extents. Those bytes remain necessary to decode the marker on recovery.
    if (location.logical_size_ != 0) {
      return absl::DataLossError("invalid retired group marker");
    }
    const auto active = directory.groups().find(group.id_.prefix_);
    if (active != directory.groups().end() && active->second.id_ == group.id_) {
      return absl::DataLossError("retired group still owns an active route");
    }
    const auto marker = directory.retired_groups().find(group.id_);
    if (marker == directory.retired_groups().end() ||
        marker->second.sequence_ != location.mutation_sequence_) {
      return absl::DataLossError("retired marker does not match directory");
    }
  } else {
    const auto route = directory.groups().find(group.id_.prefix_);
    if (route == directory.groups().end() || route->second.id_ != group.id_ ||
        route->second.sequence_ != location.mutation_sequence_ ||
        route->second.field_count_ != location.logical_size_) {
      return absl::DataLossError("group location does not match its route");
    }
  }
  if (group.extents_ != nullptr) {
    if (group.extents_->empty() || group.extents_->size() > kMaxStringExtents) {
      return absl::DataLossError("invalid group extent count");
    }
    std::uint64_t bytes = 0;
    for (const auto& extent : *group.extents_) {
      if (extent.block_id_ == kInvalidBlockId ||
          extent.allocation_epoch_ == 0 || extent.payload_bytes_ == 0 ||
          extent.payload_bytes_ > kExtentPayloadBytes ||
          extent.payload_bytes_ > kMaxRecordPayloadBytes - bytes) {
        return absl::DataLossError(
            "grouped object has invalid extent reference");
      }
      bytes += extent.payload_bytes_;
    }
  }
  return absl::OkStatus();
}

// Index pages cap the cost of one copy-on-write mutation. The trie uses the
// complete 72-bit group identity, not a potentially colliding runtime digest;
// a path has a hard bound and lookup never walks earlier object versions.
constexpr std::size_t kGroupIndexPageEntries = 64;
bool IdentityBit(HashGroupId id, unsigned depth) {
  return depth < 64 ? ((id.prefix_ >> (63 - depth)) & 1)
                    : ((id.bits_ >> (71 - depth)) & 1);
}

struct GroupIndexPage {
  RetainedMemoryCharge ids_charge_;
  std::vector<HashGroupId> ids_;
  std::uint64_t retired_ = 0;
  RecordIndex locations_;
  ScanHashMap<std::shared_ptr<const std::vector<ExtentRef>>> extents_;
};

struct GroupIndexNode {
  // Like routing nodes, these links never leave the key owner, even when the
  // indexed physical blocks belong to other workers. Outer transfer handles
  // route final metadata cleanup back here; manifests may still cross workers.
  LocalSharedPtr<const GroupIndexPage> page_;
  LocalSharedPtr<const GroupIndexNode> children_[2];
  std::size_t size_ = 0;
};

using NodeHandle = LocalSharedPtr<const GroupIndexNode>;

absl::StatusOr<NodeHandle> BuildPhysical(
    std::span<const HashGroupLocation> records, unsigned depth,
    const std::shared_ptr<ScanHashMapEntryArena>& arena) {
  if (records.empty()) return NodeHandle{};
  auto node = AllocateLocalObject<GroupIndexNode>(arena);
  if (!node.ok()) return node.status();
  (*node)->size_ = records.size();
  if (records.size() > kGroupIndexPageEntries) {
    if (depth == 72)
      return absl::DataLossError("duplicate physical group identity");
    auto middle = std::partition_point(
        records.begin(), records.end(),
        [depth](const auto& group) { return !IdentityBit(group.id_, depth); });
    const auto n = static_cast<std::size_t>(middle - records.begin());
    for (unsigned branch = 0; branch != 2; ++branch) {
      auto child = BuildPhysical(branch ? records.subspan(n) : records.first(n),
                                 depth + 1, arena);
      if (!child.ok()) return child.status();
      (*node)->children_[branch] = std::move(*child);
    }
    return NodeHandle(std::move(*node));
  }
  auto page = AllocateLocalObject<GroupIndexPage>(arena);
  if (!page.ok()) return page.status();
  const auto ids_bytes =
      AllocatorUsableSizeForRequest(records.size() * sizeof(HashGroupId));
  auto ids_reservation = TryReserveMemory(ids_bytes);
  if (!ids_reservation) {
    RecordMemoryRejection();
    return absl::ResourceExhaustedError("OOM group page exceeds maxmemory");
  }
  (*page)->ids_charge_.Account(arena->allocation_domain().owner_shard_,
                               ids_bytes);
  (*page)->ids_.reserve(records.size());
  ids_reservation.reset();
  (*page)->locations_.SetEntryArena(arena);
  (*page)->extents_.SetEntryArena(arena);
  for (const auto& record : records) {
    const auto bytes = GroupKey(record.id_);
    const std::string_view key(bytes.data(), bytes.size());
    const auto digest = ComputeDigest(key);
    auto& index = (*page)->locations_;
    if (!index.CanAllocateEntry(key, true, false)) {
      return absl::ResourceExhaustedError("group location capacity exhausted");
    }
    auto reservation = TryReserveMemory(
        index.RequiredAllocationBytes(digest, key, true, false, true));
    if (!reservation) {
      RecordMemoryRejection();
      return absl::ResourceExhaustedError(
          "OOM group locations exceed maxmemory");
    }
    if (index.InsertNew(digest, key, record.location_) == nullptr) {
      return absl::ResourceExhaustedError("group location allocation failed");
    }
    reservation.reset();
    if (record.retired_) (*page)->retired_ |= 1ULL << (*page)->ids_.size();
    (*page)->ids_.push_back(record.id_);
    if (record.extents_) {
      // Build callers own copies already admitted by CopyManifest, including
      // unchanged manifests shared from an old immutable page.
      const auto inserted = Insert((*page)->extents_, key, record.extents_);
      if (!inserted.ok()) return inserted;
    }
  }
  (*node)->page_ = std::move(*page);
  return NodeHandle(std::move(*node));
}

const GroupIndexPage* FindPage(const NodeHandle& root, HashGroupId id) {
  // The immutable root keeps the whole path alive during this non-suspending
  // lookup; walking borrowed pointers need not touch even the local counts.
  const auto* node = root.get();
  unsigned depth = 0;
  while (node && !node->page_)
    node = node->children_[IdentityBit(id, depth++)].get();
  return node ? node->page_.get() : nullptr;
}

std::shared_ptr<const std::vector<ExtentRef>> ManifestFor(
    const GroupIndexPage& page, HashGroupId id) {
  const auto bytes = GroupKey(id);
  const std::string_view key(bytes.data(), bytes.size());
  const auto* entry = page.extents_.Find(ComputeDigest(key), key);
  return entry ? entry->value_ : nullptr;
}

absl::StatusOr<NodeHandle> UpdatePhysical(
    const NodeHandle& node, std::span<const HashGroupLocation> changed,
    unsigned depth, const std::shared_ptr<ScanHashMapEntryArena>& arena) {
  if (changed.empty()) return node;
  if (!node) return BuildPhysical(changed, depth, arena);
  if (node->page_) {
    std::map<HashGroupId, HashGroupLocation> records;
    const auto& page = *node->page_;
    for (std::size_t i = 0; i < page.ids_.size(); ++i) {
      const auto id = page.ids_[i];
      const auto bytes = GroupKey(id);
      const std::string_view key(bytes.data(), bytes.size());
      const auto* entry = page.locations_.Find(ComputeDigest(key), key);
      // Epoch/owner remain the physical block's authority. A compact entry
      // is copied byte-for-byte through this temporary location; these two
      // fields do not enter the compact destination RecordIndex.
      records.emplace(
          id, HashGroupLocation{.id_ = id,
                                .location_ = RecordIndexEntryPolicy::Load(
                                    entry->value_, nullptr, 1, 0),
                                .extents_ = ManifestFor(page, id),
                                .retired_ = ((page.retired_ >> i) & 1) != 0});
    }
    for (const auto& record : changed)
      records.insert_or_assign(record.id_, record);
    std::vector<HashGroupLocation> merged;
    merged.reserve(records.size());
    for (auto& [id, record] : records) merged.push_back(std::move(record));
    return BuildPhysical(merged, depth, arena);
  }
  auto replacement = AllocateLocalObject<GroupIndexNode>(arena);
  if (!replacement.ok()) return replacement.status();
  const auto middle = std::partition_point(
      changed.begin(), changed.end(),
      [depth](const auto& group) { return !IdentityBit(group.id_, depth); });
  const auto n = static_cast<std::size_t>(middle - changed.begin());
  for (unsigned branch = 0; branch != 2; ++branch) {
    auto child = UpdatePhysical(node->children_[branch],
                                branch ? changed.subspan(n) : changed.first(n),
                                depth + 1, arena);
    if (!child.ok()) return child.status();
    (*replacement)->children_[branch] = std::move(*child);
    if ((*replacement)->children_[branch]) {
      (*replacement)->size_ += (*replacement)->children_[branch]->size_;
    }
  }
  return NodeHandle(std::move(*replacement));
}

void VisitPhysical(const NodeHandle& node,
                   const GroupedHashObject::RecordVisitor& visitor) {
  if (!node) return;
  if (!node->page_) {
    VisitPhysical(node->children_[0], visitor);
    VisitPhysical(node->children_[1], visitor);
    return;
  }
  const auto& page = *node->page_;
  for (std::size_t i = 0; i < page.ids_.size(); ++i) {
    const auto id = page.ids_[i];
    const auto bytes = GroupKey(id);
    const std::string_view key(bytes.data(), bytes.size());
    visitor(id, *page.locations_.Find(ComputeDigest(key), key),
            ManifestFor(page, id), ((page.retired_ >> i) & 1) != 0);
  }
}

}  // namespace

struct GroupedHashPhysicalState {
  std::shared_ptr<ScanHashMapEntryArena> arena_;
  NodeHandle root_;
  struct StringPage {
    // The owner keeps compact RecordIndex entries alive. Direct pointers are
    // immutable and used only on the key owner, including destruction.
    NodeHandle owner_;
    std::array<const RecordIndex::Entry*, kGroupIndexPageEntries> entries_{};
  };
  RetainedMemoryCharge string_pages_charge_;
  std::vector<LocalSharedPtr<const StringPage>> string_pages_;
  std::size_t string_size_ = 0;
};

namespace {

// String positions are dense and stable. A paged vector avoids hashing/trie
// lookup while sharing untouched 64-entry pages with snapshots and undo views.
absl::Status BuildStringPhysical(GroupedHashPhysicalState& output,
                                 const GroupedHashPhysicalState* previous,
                                 std::span<const HashGroupLocation> changed,
                                 std::size_t count) {
  using Page = GroupedHashPhysicalState::StringPage;
  const auto pages =
      (count + kGroupIndexPageEntries - 1) / kGroupIndexPageEntries;
  const auto bytes =
      AllocatorUsableSizeForRequest(pages * sizeof(LocalSharedPtr<const Page>));
  auto admission = TryReserveMemory(bytes);
  if (!admission) {
    RecordMemoryRejection();
    return absl::ResourceExhaustedError("OOM String index vector");
  }
  output.string_pages_charge_.Account(
      output.arena_->allocation_domain().owner_shard_, bytes);
  output.string_pages_.resize(pages);
  output.string_size_ = count;
  if (previous)
    std::copy(previous->string_pages_.begin(), previous->string_pages_.end(),
              output.string_pages_.begin());
  std::size_t cursor = 0;
  while (cursor < changed.size()) {
    const auto page_id =
        (changed[cursor].id_.prefix_ - 1) / kGroupIndexPageEntries;
    if (page_id >= pages || !IsOrderedPageId(changed[cursor].id_))
      return absl::DataLossError("invalid String index position");
    auto stop = cursor + 1;
    while (stop < changed.size() &&
           (changed[stop].id_.prefix_ - 1) / kGroupIndexPageEntries == page_id)
      ++stop;
    std::vector<HashGroupLocation> records;
    const auto first = page_id * kGroupIndexPageEntries;
    const auto end = std::min(count, first + kGroupIndexPageEntries);
    records.reserve(end - first);
    for (auto position = first; position < end; ++position) {
      if (cursor < stop && changed[cursor].id_.prefix_ == position + 1) {
        records.push_back(changed[cursor++]);
      } else if (previous && position < previous->string_size_) {
        const auto& page = *previous->string_pages_[page_id];
        const auto* entry = page.entries_[position % kGroupIndexPageEntries];
        records.push_back(
            {.id_ = {position + 1, 0},
             .location_ =
                 RecordIndexEntryPolicy::Load(entry->value_, nullptr, 1, 0),
             .extents_ = ManifestFor(*page.owner_->page_, {position + 1, 0})});
      } else
        return absl::DataLossError("missing String index segment");
    }
    auto owner = BuildPhysical(records, 0, output.arena_);
    if (!owner.ok()) return owner.status();
    auto page = AllocateLocalObject<Page>(output.arena_);
    if (!page.ok()) return page.status();
    (*page)->owner_ = std::move(*owner);
    for (const auto& record : records) {
      const auto key = GroupKey(record.id_);
      const std::string_view view(key.data(), key.size());
      (*page)->entries_[(record.id_.prefix_ - 1) % kGroupIndexPageEntries] =
          (*page)->owner_->page_->locations_.Find(ComputeDigest(view), view);
    }
    output.string_pages_[page_id] = std::move(*page);
  }
  return absl::OkStatus();
}

absl::Status BuildPhysicalState(GroupedHashPhysicalState& output,
                                const GroupedHashPhysicalState* previous,
                                std::span<const HashGroupLocation> changed) {
  if ((!changed.empty() &&
       changed.front().location_.value_type() == ValueType::kString) ||
      (previous && previous->string_size_ != 0)) {
    const auto count =
        std::max<std::size_t>(previous ? previous->string_size_ : 0,
                              changed.empty() ? 0 : changed.back().id_.prefix_);
    return BuildStringPhysical(output, previous, changed, count);
  }
  auto root = previous
                  ? UpdatePhysical(previous->root_, changed, 0, output.arena_)
                  : BuildPhysical(changed, 0, output.arena_);
  if (!root.ok()) return root.status();
  output.root_ = std::move(*root);
  return absl::OkStatus();
}

}  // namespace

bool GroupedObjectVersion::Matches(
    const GroupedObjectVersion& other) const noexcept {
  const auto& a = root_;
  const auto& b = other.root_;
  // Flushing and shield bookkeeping can change runtime flags without
  // replacing the root. TTL and representation, in contrast, are logical
  // metadata and must agree even when a caller supplies the same address.
  return db_epoch_ == other.db_epoch_ &&
         replication_epoch_ == other.replication_epoch_ &&
         index_generation_ == other.index_generation_ &&
         a.block_id() == b.block_id() &&
         a.allocation_epoch() == b.allocation_epoch() &&
         a.record_offset() == b.record_offset() &&
         a.total_disk_bytes() == b.total_disk_bytes() &&
         a.mutation_sequence_ == b.mutation_sequence_ &&
         a.expire_at_ms_ == b.expire_at_ms_ &&
         a.logical_size_ == b.logical_size_ && a.kind() == b.kind() &&
         a.value_type() == b.value_type() && a.grouped() == b.grouped() &&
         a.external() == b.external() && a.key_external() == b.key_external();
}

absl::StatusOr<GroupedHashObject::Handle> GroupedHashObject::Create(
    GroupedObjectVersion version, HashGroupDirectory directory,
    std::span<const HashGroupLocation> locations,
    std::shared_ptr<ScanHashMapEntryArena> arena) {
  auto prepared =
      PrepareCreate(version, std::move(directory), locations, std::move(arena));
  if (!prepared.ok()) return prepared.status();
  return Handle(std::move(*prepared));
}

absl::StatusOr<GroupedHashObject::PreparedHandle>
GroupedHashObject::PrepareCreate(GroupedObjectVersion version,
                                 HashGroupDirectory directory,
                                 std::span<const HashGroupLocation> locations,
                                 std::shared_ptr<ScanHashMapEntryArena> arena) {
  const auto valid_root = ValidateRoot(version, directory);
  if (!valid_root.ok()) return valid_root;
  if (arena == nullptr) arena = MakeArena();
  if (!arena->externally_admitted() || arena->externally_accounted()) {
    return absl::InvalidArgumentError(
        "grouped object requires an admitted worker arena");
  }
  std::vector<HashGroupLocation> records(locations.begin(), locations.end());
  std::sort(records.begin(), records.end(),
            [](const auto& a, const auto& b) { return a.id_ < b.id_; });
  absl::flat_hash_set<std::tuple<std::uint64_t, std::uint64_t, std::uint32_t>>
      physical_records;
  absl::flat_hash_set<std::uint64_t> extent_blocks;
  std::map<std::uint64_t, std::pair<std::uint64_t, std::uint16_t>> allocations;
  allocations.emplace(
      version.root_.block_id(),
      std::pair(version.root_.allocation_epoch(), version.root_.block_owner()));
  std::size_t active = 0;
  std::optional<HashGroupId> previous;
  for (auto& record : records) {
    const auto valid = ValidateLocation(record, version, directory);
    if (!valid.ok()) return valid;
    const auto& location = record.location_;
    if (previous == record.id_ ||
        !physical_records
             .emplace(location.block_id(), location.allocation_epoch(),
                      location.record_offset())
             .second) {
      return absl::DataLossError("duplicate group or physical record");
    }
    previous = record.id_;
    active += !record.retired_;
    const auto identity =
        std::pair(location.allocation_epoch(), location.block_owner());
    const auto [allocation, inserted] =
        allocations.try_emplace(location.block_id(), identity);
    if (!inserted && allocation->second != identity) {
      return absl::DataLossError(
          "grouped object has conflicting block allocations");
    }
    if (record.extents_) {
      for (const auto& extent : *record.extents_) {
        if (!extent_blocks.insert(extent.block_id_).second) {
          return absl::DataLossError("different groups share an extent block");
        }
      }
      auto owned = CopyManifest(*record.extents_, arena->allocation_domain());
      if (!owned.ok()) return owned.status();
      record.extents_ = std::move(*owned);
    }
  }
  if (active != directory.root().group_count_ ||
      records.size() - active != directory.retired_groups().size()) {
    return absl::DataLossError("grouped object has missing active locations");
  }
  for (const auto block : extent_blocks) {
    if (allocations.contains(block)) {
      return absl::DataLossError("group extent aliases a records block");
    }
  }
  auto physical = AllocateObject<GroupedHashPhysicalState>(arena);
  if (!physical.ok()) return physical.status();
  (*physical)->arena_ = arena;
  const auto built = BuildPhysicalState(**physical, nullptr, records);
  if (!built.ok()) return built;
  auto owned_directory = OwnDirectory(std::move(directory), arena);
  if (!owned_directory.ok()) return owned_directory.status();
  auto object = AllocateObject<GroupedHashObject>(arena);
  if (!object.ok()) return object.status();
  (*object)->version_ = version;
  (*object)->directory_ = std::move(*owned_directory);
  (*object)->physical_ = std::move(*physical);
  return std::move(*object);
}

absl::StatusOr<GroupedHashObject::PreparedHandle>
GroupedHashObject::PrepareUpdate(
    const Handle& expected, GroupedObjectVersion provisional_version,
    HashGroupDirectory directory,
    std::span<const HashGroupLocation> changed_locations) {
  if (!expected || expected->is_ordered() ||
      provisional_version.root_.mutation_sequence_ <
          expected->command_sequence() ||
      provisional_version.db_epoch_ != expected->version_.db_epoch_ ||
      provisional_version.replication_epoch_ !=
          expected->version_.replication_epoch_ ||
      provisional_version.index_generation_ !=
          expected->version_.index_generation_ ||
      directory.root().incarnation_ !=
          expected->directory().root().incarnation_ ||
      directory.root().seed_ != expected->directory().root().seed_ ||
      directory.sequence() < expected->directory().sequence() ||
      directory.command_sequence() < expected->directory().command_sequence()) {
    return absl::FailedPreconditionError(
        "grouped update changes population or incarnation");
  }
  const auto valid_root = ValidateRoot(provisional_version, directory);
  if (!valid_root.ok()) return valid_root;
  auto arena = expected->physical_->arena_;
  std::vector<HashGroupLocation> changed(changed_locations.begin(),
                                         changed_locations.end());
  std::sort(changed.begin(), changed.end(),
            [](const auto& a, const auto& b) { return a.id_ < b.id_; });
  std::optional<HashGroupId> previous;
  std::int64_t active = expected->group_count();
  std::int64_t fields = expected->directory().root().field_count_;
  for (auto& record : changed) {
    const auto valid = ValidateLocation(record, provisional_version, directory);
    if (!valid.ok()) return valid;
    if (previous == record.id_)
      return absl::DataLossError("duplicate group update");
    previous = record.id_;
    if (const auto* old = expected->FindGroup(record.id_)) {
      --active;
      fields -= old->value_.logical_size();
    }
    active += !record.retired_;
    if (!record.retired_) fields += record.location_.logical_size_;
    if (record.extents_) {
      auto owned = CopyManifest(*record.extents_, arena->allocation_domain());
      if (!owned.ok()) return owned.status();
      record.extents_ = std::move(*owned);
    }
  }
  if (active != directory.root().group_count_ ||
      fields != static_cast<std::int64_t>(directory.root().field_count_)) {
    return absl::DataLossError(
        "group update omits a split retirement or child");
  }
  auto physical = AllocateObject<GroupedHashPhysicalState>(arena);
  if (!physical.ok()) return physical.status();
  (*physical)->arena_ = arena;
  const auto built =
      BuildPhysicalState(**physical, expected->physical_.get(), changed);
  if (!built.ok()) return built;
  auto owned_directory = OwnDirectory(std::move(directory), arena);
  if (!owned_directory.ok()) return owned_directory.status();
  auto object = AllocateObject<GroupedHashObject>(arena);
  if (!object.ok()) return object.status();
  (*object)->version_ = provisional_version;
  (*object)->directory_ = std::move(*owned_directory);
  (*object)->physical_ = std::move(*physical);
  return std::move(*object);
}

absl::StatusOr<GroupedHashObject::Handle> GroupedHashObject::CreateOrdered(
    GroupedObjectVersion version, OrderedGroupDirectory directory,
    std::span<const HashGroupLocation> locations,
    std::shared_ptr<ScanHashMapEntryArena> arena) {
  auto prepared = PrepareCreateOrdered(version, std::move(directory), locations,
                                       std::move(arena));
  if (!prepared.ok()) return prepared.status();
  return Handle(std::move(*prepared));
}

absl::StatusOr<GroupedHashObject::PreparedHandle>
GroupedHashObject::PrepareCreateOrdered(
    GroupedObjectVersion version, OrderedGroupDirectory directory,
    std::span<const HashGroupLocation> locations,
    std::shared_ptr<ScanHashMapEntryArena> arena) {
  const auto valid_root = ValidateRoot(version, directory);
  if (!valid_root.ok()) return valid_root;
  if (arena == nullptr) arena = MakeArena();
  if (!arena->externally_admitted() || arena->externally_accounted()) {
    return absl::InvalidArgumentError(
        "grouped object requires an admitted worker arena");
  }
  std::vector<HashGroupLocation> records(locations.begin(), locations.end());
  std::sort(records.begin(), records.end(),
            [](const auto& a, const auto& b) { return a.id_ < b.id_; });
  absl::flat_hash_set<std::tuple<std::uint64_t, std::uint64_t, std::uint32_t>>
      physical_records;
  absl::flat_hash_set<std::uint64_t> extent_blocks;
  std::map<std::uint64_t, std::pair<std::uint64_t, std::uint16_t>> allocations;
  allocations.emplace(
      version.root_.block_id(),
      std::pair(version.root_.allocation_epoch(), version.root_.block_owner()));
  std::size_t active = 0;
  std::optional<HashGroupId> previous;
  for (auto& record : records) {
    const auto valid = ValidateLocation(record, version, directory);
    if (!valid.ok()) return valid;
    const auto& location = record.location_;
    if (previous == record.id_ ||
        !physical_records
             .emplace(location.block_id(), location.allocation_epoch(),
                      location.record_offset())
             .second) {
      return absl::DataLossError("duplicate group or physical record");
    }
    previous = record.id_;
    active += !record.retired_;
    const auto identity =
        std::pair(location.allocation_epoch(), location.block_owner());
    const auto [allocation, inserted] =
        allocations.try_emplace(location.block_id(), identity);
    if (!inserted && allocation->second != identity) {
      return absl::DataLossError(
          "grouped object has conflicting block allocations");
    }
    if (record.extents_) {
      for (const auto& extent : *record.extents_) {
        if (!extent_blocks.insert(extent.block_id_).second) {
          return absl::DataLossError("different groups share an extent block");
        }
      }
      auto owned = CopyManifest(*record.extents_, arena->allocation_domain());
      if (!owned.ok()) return owned.status();
      record.extents_ = std::move(*owned);
    }
  }
  const auto* members = directory.member_directory();
  if (active != static_cast<std::uint64_t>(directory.root().group_count_) +
                    (members ? members->root().group_count_ : 0) ||
      records.size() - active !=
          directory.retired_groups().size() +
              (members ? members->retired_groups().size() : 0)) {
    return absl::DataLossError("grouped object has missing active locations");
  }
  for (const auto block : extent_blocks) {
    if (allocations.contains(block)) {
      return absl::DataLossError("group extent aliases a records block");
    }
  }
  auto physical = AllocateObject<GroupedHashPhysicalState>(arena);
  if (!physical.ok()) return physical.status();
  (*physical)->arena_ = arena;
  const auto built = BuildPhysicalState(**physical, nullptr, records);
  if (!built.ok()) return built;
  auto owned_directory = OwnDirectory(std::move(directory), arena);
  if (!owned_directory.ok()) return owned_directory.status();
  auto object = AllocateObject<GroupedHashObject>(arena);
  if (!object.ok()) return object.status();
  (*object)->version_ = version;
  (*object)->ordered_directory_ = std::move(*owned_directory);
  (*object)->physical_ = std::move(*physical);
  return std::move(*object);
}

absl::StatusOr<GroupedHashObject::PreparedHandle>
GroupedHashObject::PrepareUpdateOrdered(
    const Handle& expected, GroupedObjectVersion provisional_version,
    OrderedGroupDirectory directory,
    std::span<const HashGroupLocation> changed_locations) {
  if (!expected || !expected->is_ordered() ||
      provisional_version.root_.mutation_sequence_ <
          expected->command_sequence() ||
      provisional_version.db_epoch_ != expected->version_.db_epoch_ ||
      provisional_version.replication_epoch_ !=
          expected->version_.replication_epoch_ ||
      provisional_version.index_generation_ !=
          expected->version_.index_generation_ ||
      directory.root().incarnation_ !=
          expected->ordered_directory().root().incarnation_ ||
      directory.root().kind_ != expected->ordered_directory().root().kind_ ||
      directory.sequence() < expected->ordered_directory().sequence() ||
      directory.command_sequence() <
          expected->ordered_directory().command_sequence()) {
    return absl::FailedPreconditionError(
        "grouped update changes population or incarnation");
  }
  const auto valid_root = ValidateRoot(provisional_version, directory);
  if (!valid_root.ok()) return valid_root;
  if (expected->has_member_index() && directory.member_directory() &&
      expected->directory().root() != directory.member_directory()->root() &&
      std::none_of(
          changed_locations.begin(), changed_locations.end(),
          [](const auto& record) { return !IsOrderedPageId(record.id_); }))
    return absl::DataLossError(
        "member revision changed without physical writes");
  auto arena = expected->physical_->arena_;
  std::vector<HashGroupLocation> changed(changed_locations.begin(),
                                         changed_locations.end());
  std::sort(changed.begin(), changed.end(),
            [](const auto& a, const auto& b) { return a.id_ < b.id_; });
  std::optional<HashGroupId> previous;
  std::int64_t active =
      expected->group_count() + (expected->has_member_index()
                                     ? expected->directory().root().group_count_
                                     : 0);
  std::int64_t fields = expected->ordered_directory().root().item_count_ *
                        (expected->has_member_index() ? 2 : 1);
  for (auto& record : changed) {
    const auto valid = ValidateLocation(record, provisional_version, directory);
    if (!valid.ok()) return valid;
    if (previous == record.id_)
      return absl::DataLossError("duplicate group update");
    previous = record.id_;
    if (const auto* old = expected->FindGroup(record.id_)) {
      --active;
      fields -= old->value_.logical_size();
    }
    active += !record.retired_;
    if (!record.retired_) fields += record.location_.logical_size_;
    if (record.extents_) {
      auto owned = CopyManifest(*record.extents_, arena->allocation_domain());
      if (!owned.ok()) return owned.status();
      record.extents_ = std::move(*owned);
    }
  }
  const auto* members = directory.member_directory();
  if (active != static_cast<std::int64_t>(directory.root().group_count_) +
                    (members ? members->root().group_count_ : 0) ||
      fields != static_cast<std::int64_t>(directory.root().item_count_) *
                    (members ? 2 : 1)) {
    return absl::DataLossError(
        "group update omits a split retirement or child");
  }
  auto physical = AllocateObject<GroupedHashPhysicalState>(arena);
  if (!physical.ok()) return physical.status();
  (*physical)->arena_ = arena;
  const auto built =
      BuildPhysicalState(**physical, expected->physical_.get(), changed);
  if (!built.ok()) return built;
  auto owned_directory = OwnDirectory(std::move(directory), arena);
  if (!owned_directory.ok()) return owned_directory.status();
  auto object = AllocateObject<GroupedHashObject>(arena);
  if (!object.ok()) return object.status();
  (*object)->version_ = provisional_version;
  (*object)->ordered_directory_ = std::move(*owned_directory);
  (*object)->physical_ = std::move(*physical);
  return std::move(*object);
}

absl::StatusOr<GroupedHashObject::PreparedHandle>
GroupedHashObject::PrepareMetadataUpdate(const Handle& expected,
                                         GroupedObjectVersion version) {
  if (!expected || version.db_epoch_ != expected->version_.db_epoch_ ||
      version.replication_epoch_ != expected->version_.replication_epoch_ ||
      version.index_generation_ != expected->version_.index_generation_ ||
      version.root_.mutation_sequence_ < expected->command_sequence() ||
      version.root_.value_type() != expected->version_.root_.value_type() ||
      version.root_.logical_size_ != expected->version_.root_.logical_size_) {
    return absl::FailedPreconditionError(
        "metadata update changes grouped value");
  }
  const auto valid = expected->is_ordered()
                         ? ValidateRoot(version, expected->ordered_directory())
                         : ValidateRoot(version, expected->directory());
  if (!valid.ok()) return valid;
  auto object = AllocateObject<GroupedHashObject>(expected->physical_->arena_);
  if (!object.ok()) return object.status();
  if (!version.decision_) version.decision_ = expected->version_.decision_;
  (*object)->version_ = std::move(version);
  (*object)->directory_ = expected->directory_;
  (*object)->ordered_directory_ = expected->ordered_directory_;
  (*object)->physical_ = expected->physical_;
  return std::move(*object);
}

absl::Status GroupedHashObject::FinalizeRoot(
    PreparedHandle& prepared, GroupedObjectVersion exact_version) {
  if (!prepared || prepared.use_count() != 1) {
    return absl::FailedPreconditionError("grouped builder is already shared");
  }
  const auto valid =
      prepared->is_ordered()
          ? ValidateRoot(exact_version, prepared->ordered_directory())
          : ValidateRoot(exact_version, prepared->directory());
  if (!valid.ok()) return valid;
  const auto& old = prepared->version_;
  if (old.db_epoch_ != exact_version.db_epoch_ ||
      old.replication_epoch_ != exact_version.replication_epoch_ ||
      old.index_generation_ != exact_version.index_generation_ ||
      old.root_.mutation_sequence_ != exact_version.root_.mutation_sequence_ ||
      old.root_.logical_size_ != exact_version.root_.logical_size_ ||
      old.root_.value_type() != exact_version.root_.value_type() ||
      old.root_.expire_at_ms_ != exact_version.root_.expire_at_ms_) {
    return absl::FailedPreconditionError(
        "root finalization changes logical version");
  }
  // Physical staging/GC only supplies durable identity. Its default runtime
  // decision must not erase the pending causal dependency admitted by the
  // logical mutation builder.
  if (!exact_version.decision_)
    exact_version.decision_ = prepared->version_.decision_;
  prepared->version_ = std::move(exact_version);
  return absl::OkStatus();
}

absl::StatusOr<GroupedHashObject::Handle> GroupedHashObject::RelocateRoot(
    const Handle& expected, GroupedObjectVersion replacement) {
  if (!expected)
    return absl::InvalidArgumentError("missing grouped relocation source");
  const auto valid =
      expected->is_ordered()
          ? ValidateRoot(replacement, expected->ordered_directory())
          : ValidateRoot(replacement, expected->directory());
  if (!valid.ok()) return valid;
  const auto& old = expected->version_;
  if (old.db_epoch_ != replacement.db_epoch_ ||
      old.replication_epoch_ != replacement.replication_epoch_ ||
      old.index_generation_ != replacement.index_generation_ ||
      old.root_.value_type() != replacement.root_.value_type() ||
      old.root_.expire_at_ms_ != replacement.root_.expire_at_ms_) {
    return absl::FailedPreconditionError(
        "root relocation changes logical version");
  }
  auto object = AllocateObject<GroupedHashObject>(expected->physical_->arena_);
  if (!object.ok()) return object.status();
  if (!replacement.decision_)
    replacement.decision_ = expected->version_.decision_;
  (*object)->version_ = std::move(replacement);
  (*object)->directory_ = expected->directory_;
  (*object)->ordered_directory_ = expected->ordered_directory_;
  (*object)->physical_ = expected->physical_;
  return Handle(std::move(*object));
}

absl::StatusOr<GroupedHashObject::PreparedHandle>
GroupedHashObject::PrepareRootRelocation(const Handle& expected) {
  if (!expected)
    return absl::InvalidArgumentError("missing root relocation source");
  auto object = AllocateObject<GroupedHashObject>(expected->physical_->arena_);
  if (!object.ok()) return object.status();
  (*object)->version_ = expected->version_;
  (*object)->directory_ = expected->directory_;
  (*object)->ordered_directory_ = expected->ordered_directory_;
  (*object)->physical_ = expected->physical_;
  return std::move(*object);
}

absl::Status GroupedHashObject::FinalizeRootRelocation(
    PreparedHandle& prepared, const Handle& current,
    GroupedObjectVersion exact_version) {
  if (!prepared || !current || !prepared->version_.Matches(current->version_) ||
      !prepared->SameLogicalRoot(*current)) {
    return absl::AbortedError("root relocation source changed");
  }
  const auto finalized = FinalizeRoot(prepared, exact_version);
  if (!finalized.ok()) return finalized;
  prepared->directory_ = current->directory_;
  prepared->ordered_directory_ = current->ordered_directory_;
  prepared->physical_ = current->physical_;
  return absl::OkStatus();
}

absl::StatusOr<GroupedHashObject::Handle> GroupedHashObject::RelocateGroup(
    const Handle& expected, HashGroupId id,
    const RecordLocation& expected_location, const RecordLocation& replacement,
    std::shared_ptr<const std::vector<ExtentRef>> extents) {
  if (!expected)
    return absl::InvalidArgumentError("missing group relocation source");
  const auto* current = expected->FindRecord(id);
  if (!current || current->value_.block_id() != expected_location.block_id() ||
      current->value_.record_offset() != expected_location.record_offset() ||
      current->value_.total_disk_bytes() !=
          expected_location.total_disk_bytes() ||
      current->value_.mutation_sequence_ !=
          expected_location.mutation_sequence_ ||
      replacement.mutation_sequence_ != expected_location.mutation_sequence_ ||
      replacement.logical_size_ != expected_location.logical_size_) {
    return absl::AbortedError("group relocation source changed");
  }
  HashGroupLocation changed{.id_ = id,
                            .location_ = replacement,
                            .extents_ = std::move(extents),
                            .retired_ = expected->FindGroup(id) == nullptr};
  const auto valid = expected->is_ordered()
                         ? ValidateLocation(changed, expected->version(),
                                            expected->ordered_directory())
                         : ValidateLocation(changed, expected->version(),
                                            expected->directory());
  if (!valid.ok()) return valid;
  auto arena = expected->physical_->arena_;
  if (changed.extents_) {
    auto owned = CopyManifest(*changed.extents_, arena->allocation_domain());
    if (!owned.ok()) return owned.status();
    changed.extents_ = std::move(*owned);
  }
  auto physical = AllocateObject<GroupedHashPhysicalState>(arena);
  if (!physical.ok()) return physical.status();
  (*physical)->arena_ = arena;
  const auto built = BuildPhysicalState(**physical, expected->physical_.get(),
                                        std::span(&changed, 1));
  if (!built.ok()) return built;
  auto object = AllocateObject<GroupedHashObject>(arena);
  if (!object.ok()) return object.status();
  (*object)->version_ = expected->version_;
  (*object)->directory_ = expected->directory_;
  (*object)->ordered_directory_ = expected->ordered_directory_;
  (*object)->physical_ = std::move(*physical);
  return Handle(std::move(*object));
}

const RecordIndex::Entry* GroupedHashObject::FindGroup(
    std::string_view field) const {
  if (is_ordered() && !has_member_index()) return nullptr;
  const auto* route = directory().Find(field);
  return route == nullptr ? nullptr : FindRecord(route->id_);
}

const RecordIndex::Entry* GroupedHashObject::FindGroup(HashGroupId id) const {
  if (is_ordered() && IsOrderedPageId(id)) {
    return id.bits_ == 0 && ordered_directory_->Find(id.prefix_) != nullptr
               ? FindRecord(id)
               : nullptr;
  }
  if (is_ordered() && !has_member_index()) return nullptr;
  const auto route = directory().groups().find(id.prefix_);
  if (route == directory().groups().end() || route->second.id_ != id)
    return nullptr;
  return FindRecord(id);
}

const RecordIndex::Entry* GroupedHashObject::FindRecord(HashGroupId id) const {
  if (physical_->string_size_) {
    if (!IsOrderedPageId(id) || id.prefix_ > physical_->string_size_)
      return nullptr;
    const auto index = id.prefix_ - 1;
    return physical_->string_pages_[index / kGroupIndexPageEntries]
        ->entries_[index % kGroupIndexPageEntries];
  }
  if (!(is_ordered() && IsOrderedPageId(id)) &&
      (!id.valid() || (is_ordered() && !has_member_index())))
    return nullptr;
  const auto* page = FindPage(physical_->root_, id);
  if (!page) return nullptr;
  const auto bytes = GroupKey(id);
  const std::string_view key(bytes.data(), bytes.size());
  return page->locations_.Find(ComputeDigest(key), key);
}

std::shared_ptr<const std::vector<ExtentRef>> GroupedHashObject::ExtentsFor(
    HashGroupId id) const {
  if (physical_->string_size_) {
    if (!IsOrderedPageId(id) || id.prefix_ > physical_->string_size_)
      return nullptr;
    const auto& page =
        physical_->string_pages_[(id.prefix_ - 1) / kGroupIndexPageEntries];
    return ManifestFor(*page->owner_->page_, id);
  }
  if (!(is_ordered() && IsOrderedPageId(id)) &&
      (!id.valid() || (is_ordered() && !has_member_index())))
    return nullptr;
  const auto* page = FindPage(physical_->root_, id);
  return page ? ManifestFor(*page, id) : nullptr;
}

std::size_t GroupedHashObject::record_count() const noexcept {
  if (physical_->string_size_) return physical_->string_size_;
  return physical_->root_ ? physical_->root_->size_ : 0;
}

bool GroupedHashObject::SameLogicalRoot(
    const GroupedHashObject& other) const noexcept {
  if (is_ordered() != other.is_ordered() || revision() != other.revision())
    return false;
  return is_ordered()
             ? ordered_directory().root() == other.ordered_directory().root()
             : directory().root() == other.directory().root();
}

void GroupedHashObject::ForEachRecord(const RecordVisitor& visitor) const {
  if (physical_->string_size_) {
    for (std::size_t i = 0; i < physical_->string_size_; ++i) {
      const auto& page = physical_->string_pages_[i / kGroupIndexPageEntries];
      const HashGroupId id{i + 1, 0};
      visitor(id, *page->entries_[i % kGroupIndexPageEntries],
              ManifestFor(*page->owner_->page_, id), false);
    }
    return;
  }
  VisitPhysical(physical_->root_, visitor);
}

GroupedObjectIndex::GroupedObjectIndex(
    std::shared_ptr<ScanHashMapEntryArena> arena)
    : objects_(arena == nullptr ? MakeArena() : arena) {
  assert(arena == nullptr ||
         (arena->externally_admitted() && !arena->externally_accounted()));
}

GroupedObjectIndex::Handle GroupedObjectIndex::CurrentForMutation(
    std::string_view key) const {
  const auto* entry = objects_.Find(ComputeDigest(key), key);
  return entry ? entry->value_ : nullptr;
}

absl::Status GroupedHashObject::ReadStatus() const {
  const auto& decision = version_.decision_;
  if (decision && decision->state_.load(std::memory_order_acquire) ==
                      GroupedCommitDecision::State::kFailed)
    return absl::FailedPreconditionError(
        "grouped object transaction decision failed");
  return absl::OkStatus();
}

absl::StatusOr<GroupedObjectIndex::Handle> GroupedObjectIndex::Lookup(
    std::string_view key, const GroupedObjectVersion& version,
    bool allow_failed) const {
  if (!version.root_.grouped()) return Handle{};
  const auto* entry = objects_.Find(ComputeDigest(key), key);
  if (entry == nullptr || !entry->value_ ||
      !entry->value_->version().Matches(version)) {
    return absl::DataLossError("grouped root has no matching object directory");
  }
  if (!allow_failed) {
    const auto readable = entry->value_->ReadStatus();
    if (!readable.ok()) return readable;
  }
  return entry->value_;
}

GroupedObjectIndex::Publication::Publication(GroupedObjectIndex* index,
                                             std::string_view key,
                                             Handle expected,
                                             bool inserted) noexcept
    : index_(index),
      key_(key),
      expected_(std::move(expected)),
      inserted_(inserted) {}

GroupedObjectIndex::Publication::Publication(Publication&& other) noexcept
    : index_(std::exchange(other.index_, nullptr)),
      key_(other.key_),
      expected_(std::move(other.expected_)),
      inserted_(other.inserted_) {}

GroupedObjectIndex::Publication::~Publication() {
  if (!index_ || !inserted_) return;
  auto* entry = index_->objects_.Find(ComputeDigest(key_), key_);
  if (entry && !entry->value_)
    index_->objects_.Erase(ComputeDigest(key_), key_);
}

absl::Status GroupedObjectIndex::Publication::Commit(Handle replacement) {
  if (!index_ || !replacement) {
    return absl::InvalidArgumentError("invalid reserved grouped publication");
  }
  auto* entry = index_->objects_.Find(ComputeDigest(key_), key_);
  if (!entry || entry->value_ != expected_) {
    return absl::AbortedError("reserved grouped publication changed");
  }
  entry->value_ = std::move(replacement);
  index_ = nullptr;
  return absl::OkStatus();
}

absl::Status GroupedObjectIndex::Publication::RefreshExpected(
    const Handle& current) {
  if (!index_ || !expected_ || !current ||
      !expected_->version().Matches(current->version()) ||
      !expected_->SameLogicalRoot(*current)) {
    return absl::AbortedError("reserved grouped root changed");
  }
  const auto* entry = index_->objects_.Find(ComputeDigest(key_), key_);
  if (!entry || entry->value_ != current) {
    return absl::AbortedError("grouped side view changed before refresh");
  }
  expected_ = current;
  return absl::OkStatus();
}

absl::StatusOr<GroupedObjectIndex::Publication>
GroupedObjectIndex::PreparePublish(std::string_view key,
                                   const Handle& expected) {
  if (key.size() > kMaxStringBytes) {
    return absl::InvalidArgumentError("invalid grouped publication key");
  }
  auto* entry = objects_.Find(ComputeDigest(key), key);
  if ((entry ? entry->value_ : Handle{}) != expected) {
    return absl::AbortedError("grouped object changed before reservation");
  }
  if (entry) return Publication(this, key, expected, false);
  auto inserted = Insert(objects_, key, Handle{});
  if (!inserted.ok()) return inserted;
  return Publication(this, key, expected, true);
}

absl::Status GroupedObjectIndex::Publish(std::string_view key,
                                         const Handle& expected,
                                         Handle replacement) {
  if (replacement == nullptr || key.size() > kMaxStringBytes) {
    return absl::InvalidArgumentError("invalid grouped object publication");
  }
  const Digest digest = ComputeDigest(key);
  auto* entry = objects_.Find(digest, key);
  if ((entry == nullptr ? Handle{} : entry->value_) != expected) {
    return absl::AbortedError("grouped object changed before publication");
  }
  if (entry != nullptr) {
    entry->value_ = std::move(replacement);
    return absl::OkStatus();
  }
  return Insert(objects_, key, replacement);
}

absl::Status GroupedObjectIndex::Erase(std::string_view key,
                                       const Handle& expected) {
  const Digest digest = ComputeDigest(key);
  auto* entry = objects_.Find(digest, key);
  if ((entry == nullptr ? Handle{} : entry->value_) != expected) {
    return absl::AbortedError("grouped object changed before removal");
  }
  if (entry != nullptr) objects_.Erase(digest, key);
  return absl::OkStatus();
}

absl::Status GroupedObjectIndex::ClearKeepingSlot(std::string_view key,
                                                  const Handle& expected) {
  if (expected == nullptr) {
    return absl::InvalidArgumentError("grouped undo slot requires an old view");
  }
  auto* entry = objects_.Find(ComputeDigest(key), key);
  if (entry == nullptr || entry->value_ != expected) {
    return absl::AbortedError("grouped object changed before undo reservation");
  }
  entry->value_.reset();
  return absl::OkStatus();
}

void GroupedObjectIndex::EraseNullSlot(std::string_view key) {
  const Digest digest = ComputeDigest(key);
  auto* entry = objects_.Find(digest, key);
  if (entry != nullptr && entry->value_ == nullptr) objects_.Erase(digest, key);
}

GroupedObjectIndex GroupedObjectIndex::Detach() noexcept {
  return GroupedObjectIndex(objects_.Detach());
}

}  // namespace lavik::storage
