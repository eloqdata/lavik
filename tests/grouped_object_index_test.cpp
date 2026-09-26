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

#include <set>
#include <string>

#include "gtest/gtest.h"
#include "lavik/storage/detail/grouped_scratch.h"

namespace lavik::storage {
namespace {

// Legacy fixture sequences model both order domains equally. Tests for replay
// below call the production API directly with deliberately different C and R.
absl::StatusOr<HashGroupDirectory> Recover(
    GroupedHashRoot root, std::uint64_t sequence,
    std::span<const RecoveredHashGroup> candidates,
    const absl::flat_hash_set<std::uint64_t>& committed) {
  root.revision_ = sequence;
  return HashGroupDirectory::Recover(root, sequence, candidates, committed);
}

absl::StatusOr<HashGroupDirectory> Apply(
    const HashGroupDirectory& directory, GroupedHashRoot root,
    std::uint64_t sequence, std::span<const RecoveredHashGroup> changes) {
  root.revision_ = sequence;
  return directory.Apply(root, sequence, changes);
}

RecordLocation GroupLocation(std::uint64_t block, std::uint64_t seq,
                             std::uint32_t count, bool grouped = false,
                             bool external = false) {
  return RecordLocation(
      block, seq, 17, 0, count,
      RecordLocation::PackedMetadata::Encode(
          kBlockHeaderBytes, 256, 0, true, external, false, false, false, false,
          RecordKind::kValue, ValueType::kHash, false, grouped));
}

struct ObjectInput {
  GroupedObjectVersion version_;
  HashGroupDirectory directory_;
  std::vector<HashGroupLocation> locations_;
};

ObjectInput Input(std::uint64_t sequence = 5, std::uint64_t incarnation = 1,
                  unsigned field_count = 100, std::size_t target_bytes = 1024) {
  HashValue value;
  for (unsigned i = 0; i < field_count; ++i) {
    std::string field = "field" + std::to_string(i);
    value.entries_.push_back({.digest_ = ComputeDigest(field),
                              .field_ = field,
                              .value_ = std::string(128, 'x')});
  }
  const DigestSeed seed{1, 2, 3, 4};
  auto groups =
      GroupHashValue(std::move(value), incarnation, seed, target_bytes);
  EXPECT_TRUE(groups.ok()) << groups.status();
  if (!groups.ok()) return {};
  GroupedHashRoot root{
      .incarnation_ = incarnation,
      .seed_ = seed,
      .field_count_ = field_count,
      .group_count_ = static_cast<std::uint32_t>(groups->size())};
  std::vector<RecoveredHashGroup> candidates;
  ObjectInput input;
  input.version_ = {.root_ = GroupLocation(999999, sequence, field_count, true),
                    .db_epoch_ = 1,
                    .replication_epoch_ = 2,
                    .index_generation_ = 3};
  for (const auto& group : *groups) {
    const std::uint64_t block = candidates.size() + 1;
    candidates.push_back({.incarnation_ = incarnation,
                          .id_ = group.id_,
                          .sequence_ = sequence,
                          .lsn_ = sequence,
                          .field_count_ = group.value_.entries_.size(),
                          .record_token_ = block});
    input.locations_.push_back(
        {.id_ = group.id_,
         .location_ =
             GroupLocation(block, sequence, group.value_.entries_.size()),
         .extents_ = nullptr});
  }
  auto directory = Recover(root, sequence, candidates, {});
  EXPECT_TRUE(directory.ok()) << directory.status();
  if (directory.ok()) input.directory_ = std::move(*directory);
  return input;
}

absl::StatusOr<GroupedHashObject::Handle> Create(ObjectInput input) {
  return GroupedHashObject::Create(input.version_, std::move(input.directory_),
                                   input.locations_);
}

TEST(GroupedObjectIndexTest,
     PhysicalGroupsUseRevisionNotSourceCommandSequence) {
  auto input = Input(100);
  std::vector<RecoveredHashGroup> records;
  for (const auto& [prefix, group] : input.directory_.groups())
    records.push_back(group);
  auto directory =
      HashGroupDirectory::Recover(input.directory_.root(), 7, records, {});
  ASSERT_TRUE(directory.ok()) << directory.status();
  input.version_.root_.mutation_sequence_ = 7;
  auto object =
      GroupedHashObject::Create(input.version_, *directory, input.locations_);
  ASSERT_TRUE(object.ok()) << object.status();
  EXPECT_EQ((*object)->version().root_.mutation_sequence_, 7);
  EXPECT_EQ((*object)->directory().sequence(), 100);
  EXPECT_EQ((*object)->directory().command_sequence(), 7);
}

// Restore process-wide admission even after ASSERT_* exits a test early.
class GroupedMemoryScope {
 public:
  GroupedMemoryScope() : shard_(CurrentMemoryAccountingShard()) {
    EXPECT_TRUE(InitMemoryLimit(1024ULL * 1024 * 1024, 1).ok());
    BindMemoryAccountingShard(0);
  }
  ~GroupedMemoryScope() {
    EXPECT_TRUE(InitMemoryLimit(1024ULL * 1024 * 1024, 1).ok());
    BindMemoryAccountingShard(shard_ == 0 ? kMaxMemoryWorkers : shard_ - 1);
  }

 private:
  unsigned shard_;
};

TEST(GroupedScratchBudgetTest, RejectsOverflowAndMissingExtentMetadata) {
  GroupedScratchBudget overflow;
  EXPECT_EQ(overflow.AddBytes(std::numeric_limits<std::size_t>::max()).code(),
            absl::StatusCode::kResourceExhausted);
  EXPECT_EQ(
      overflow.Reserve(std::numeric_limits<std::size_t>::max()).status().code(),
      absl::StatusCode::kResourceExhausted);
  GroupedScratchBudget external;
  EXPECT_EQ(
      external.AddGroup(GroupLocation(1, 1, 1, false, true), nullptr, 1).code(),
      absl::StatusCode::kDataLoss);
}

TEST(GroupedScratchBudgetTest, AdmitsSelectedScratchBeforeAllocation) {
  GroupedMemoryScope memory;
  GroupedScratchBudget selected;
  ASSERT_TRUE(selected.AddGroup(GroupLocation(1, 1, 3), nullptr, 1).ok());
  {
    auto admitted = selected.Reserve(4);
    ASSERT_TRUE(admitted.ok()) << admitted.status();
  }
  // Aggregate scratch cannot evade admission simply because each physical
  // leaf would fit. No value-sized allocation is needed to reject this plan.
  GroupedScratchBudget aggregate;
  ASSERT_TRUE(aggregate.AddBytes(1024ULL * 1024 * 1024).ok());
  EXPECT_EQ(aggregate.Reserve(2).status().code(),
            absl::StatusCode::kResourceExhausted);
  EXPECT_TRUE(selected.Reserve(4).ok());
}

TEST(GroupedScratchBudgetTest, RetainedIndexMetadataNeedsNoLiveBlock) {
  GroupedMemoryScope memory;
  for (const bool external : {false, true}) {
    // There is intentionally no engine or allocated BlockState behind these
    // identities. An old side view remains usable for admission after GC has
    // retired its physical allocation, including an extent-backed page.
    const auto location = GroupLocation(999999, 17, 3, false, external);
    const RecordIndexValue compact(location);
    std::shared_ptr<const std::vector<ExtentRef>> extents;
    if (external)
      extents = std::make_shared<const std::vector<ExtentRef>>(
          std::initializer_list<ExtentRef>{
              {.block_id_ = 999998, .payload_bytes_ = 9000},
              {.block_id_ = 999997, .payload_bytes_ = 3000}});
    GroupedScratchBudget runtime_budget, retained_budget;
    ASSERT_TRUE(runtime_budget.AddGroup(location, extents, 1).ok());
    ASSERT_TRUE(retained_budget.AddGroup(compact, extents, 1).ok());
    auto runtime = runtime_budget.Reserve(1);
    auto retained = retained_budget.Reserve(1);
    ASSERT_TRUE(runtime.ok()) << runtime.status();
    ASSERT_TRUE(retained.ok()) << retained.status();
    EXPECT_EQ(runtime->bytes(), retained->bytes());
    EXPECT_EQ(retained->bytes(), 4096 + (external ? 12000 : 256) + 3 * 256);
  }
}

TEST(GroupedObjectIndexTest, PublishOomPreservesExistingKeyAndCanBeRetried) {
  GroupedMemoryScope memory;
  auto old = Create(Input());
  auto next = Create(Input(6));
  ASSERT_TRUE(old.ok());
  ASSERT_TRUE(next.ok());
  GroupedObjectIndex index;
  ASSERT_TRUE(index.Publish("old", nullptr, *old).ok());
  const std::string new_key(4096, 'n');
  ASSERT_TRUE(InitMemoryLimit(1, 1).ok());
  const auto rejected_before = GetMemoryStats().rejected_commands_;
  const auto rejected = index.Publish(new_key, nullptr, *next);
  EXPECT_EQ(rejected.code(), absl::StatusCode::kResourceExhausted);
  EXPECT_TRUE(rejected.message().starts_with("OOM "));
  EXPECT_GT(GetMemoryStats().rejected_commands_, rejected_before);
  EXPECT_EQ(index.size(), 1);
  auto preserved = index.Lookup("old", (*old)->version());
  ASSERT_TRUE(preserved.ok());
  EXPECT_EQ(*preserved, *old);
  EXPECT_FALSE(index.Lookup(new_key, (*next)->version()).ok());
  // Replacing an already admitted handle and releasing state do not need
  // fresh capacity. This is required to recover from memory pressure.
  EXPECT_TRUE(index.Publish("old", *old, *next).ok());
  auto detached = index.Detach();
  EXPECT_TRUE(index.empty());
  EXPECT_TRUE(detached.Erase("old", *next).ok());
  ASSERT_TRUE(InitMemoryLimit(1024ULL * 1024 * 1024, 1).ok());
  EXPECT_TRUE(index.Publish(new_key, nullptr, *next).ok());
  EXPECT_EQ(index.size(), 1);
}

TEST(GroupedObjectIndexTest, PartialConstructionFailureUnwindsGroupEntries) {
  GroupedMemoryScope memory;
  auto old = Create(Input());
  ASSERT_TRUE(old.ok());
  GroupedObjectIndex index;
  ASSERT_TRUE(index.Publish("hash", nullptr, *old).ok());
  // Thousands of leaf entries cannot fit one arena page. The first page is
  // populated before a later entry reaches the deterministic capacity limit.
  auto input = Input(6, 1, 5000, 256);
  ASSERT_GT(input.locations_.size(), 2000);
  auto arena = std::make_shared<ScanHashMapEntryArena>(1, true, false);
  for (unsigned attempt = 0; attempt < 3; ++attempt) {
    auto rejected = GroupedHashObject::Create(input.version_, input.directory_,
                                              input.locations_, arena);
    EXPECT_EQ(rejected.status().code(), absl::StatusCode::kResourceExhausted);
    EXPECT_EQ(arena->allocated_pages(), 0);
    auto preserved = index.Lookup("hash", (*old)->version());
    ASSERT_TRUE(preserved.ok());
    EXPECT_EQ(*preserved, *old);
    EXPECT_NE((*preserved)->FindGroup("field0"), nullptr);
    RefreshMemoryStats();
    EXPECT_EQ(GetMemoryStats().admission_pending_bytes_, 0);
  }
  // Failed builds return their handles/slots; the same bounded arena remains
  // usable with its retained directory bookkeeping and recycled page IDs.
  auto small = Input(7);
  auto recovered = GroupedHashObject::Create(small.version_, small.directory_,
                                             small.locations_, arena);
  ASSERT_TRUE(recovered.ok()) << recovered.status();
  EXPECT_TRUE(index.Publish("hash", *old, *recovered).ok());
}

TEST(GroupedObjectIndexTest,
     RealMaxmemoryFailureAfterPartialBuildCanBeRetried) {
  GroupedMemoryScope memory;
  auto input = Input(6, 1, 5000, 256);
  const auto baseline = WorkerMemoryAccountingBytes(0);
  std::uint64_t complete_bytes = 0;
  {
    auto measured = Create(input);
    ASSERT_TRUE(measured.ok());
    complete_bytes = WorkerMemoryAccountingBytes(0) - baseline;
  }
  ASSERT_GT(complete_bytes, 1024);
  ASSERT_EQ(WorkerMemoryAccountingBytes(0), baseline);
  // Leave enough capacity to start populating an arena, but not to retain
  // the whole object. This exercises the real maxmemory gate, not an injected
  // status or the deterministic maximum-page-ID limit.
  const auto steady = baseline + complete_bytes - 1024;
  const std::uint64_t maximum = (steady * 10 + 8) / 9;
  ASSERT_TRUE(InitMemoryLimit(maximum, 1).ok());
  auto arena = std::make_shared<ScanHashMapEntryArena>(
      ScanHashMapEntryArena::kMaximumPageId, true, false);
  auto rejected = GroupedHashObject::Create(input.version_, input.directory_,
                                            input.locations_, arena);
  EXPECT_EQ(rejected.status().code(), absl::StatusCode::kResourceExhausted);
  EXPECT_TRUE(rejected.status().message().starts_with("OOM "));
  EXPECT_EQ(arena->allocated_pages(), 0);
  // The arena's recycled-page directory is allocated only after a page has
  // been admitted. Its remaining charge proves this was a mid-build failure.
  EXPECT_GT(WorkerMemoryAccountingBytes(0), baseline);
  RefreshMemoryStats();
  EXPECT_EQ(GetMemoryStats().admission_pending_bytes_, 0);
  ASSERT_TRUE(InitMemoryLimit(1024ULL * 1024 * 1024, 1).ok());
  auto retry = GroupedHashObject::Create(input.version_, input.directory_,
                                         input.locations_, arena);
  ASSERT_TRUE(retry.ok()) << retry.status();
  EXPECT_EQ((*retry)->group_count(), input.locations_.size());
  retry->reset();
  arena.reset();
  EXPECT_EQ(WorkerMemoryAccountingBytes(0), baseline);
}

TEST(GroupedObjectIndexTest,
     DetachedMetadataStaysChargedUntilLastReaderReleasesIt) {
  GroupedMemoryScope memory;
  constexpr unsigned owner = 0;  // Worker ID, not the accounting slot (1).
  const auto baseline = WorkerMemoryAccountingBytes(owner);
  GroupedHashObject::Handle reader;
  {
    auto object = Create(Input());
    ASSERT_TRUE(object.ok());
    reader = *object;
    GroupedObjectIndex index;
    ASSERT_TRUE(index.Publish("key", nullptr, *object).ok());
    auto detached = index.Detach();
    ASSERT_TRUE(detached.Erase("key", *object).ok());
  }
  EXPECT_GT(WorkerMemoryAccountingBytes(owner), baseline);
  EXPECT_NE(reader->FindGroup("field99"), nullptr);
  // Reclamation may run elsewhere; charges must return to the allocator's
  // owner, not to the thread currently dropping the last shared handle.
  BindMemoryAccountingShard(kMaxMemoryWorkers);
  reader.reset();
  EXPECT_EQ(WorkerMemoryAccountingBytes(owner), baseline);
}

TEST(GroupedObjectIndexTest,
     ExtentManifestBytesAreIncludedInRetainedAccounting) {
  GroupedMemoryScope memory;
  constexpr unsigned owner = 0;
  auto input = Input();
  // The immutable directory is now retained/accounted from construction,
  // including while this reusable recovery input shares its routing nodes.
  const auto baseline = WorkerMemoryAccountingBytes(owner);
  auto& group = input.locations_[0];
  group.location_ = GroupLocation(group.location_.block_id(), 5,
                                  group.location_.logical_size_, false, true);
  auto manifest = std::make_shared<std::vector<ExtentRef>>();
  manifest->push_back({.block_id_ = 12345,
                       .allocation_epoch_ = 17,
                       .payload_bytes_ = 1,
                       .payload_checksum_ = 0});
  group.extents_ = manifest;
  std::int64_t small_bytes = 0;
  std::shared_ptr<const std::vector<ExtentRef>> reader;
  {
    auto object = Create(input);
    ASSERT_TRUE(object.ok());
    reader = (*object)->ExtentsFor(group.id_);
    ASSERT_NE(reader, nullptr);
  }
  // Measure only the immutable manifest owner and payload. The surrounding
  // graph has aligned arena allocations whose actual usable sizes can vary
  // with their addresses (especially with mimalloc debug padding).
  small_bytes = WorkerMemoryAccountingBytes(owner) - baseline;
  reader.reset();
  EXPECT_EQ(WorkerMemoryAccountingBytes(owner), baseline);
  for (unsigned i = 1; i < 64; ++i) {
    manifest->push_back({.block_id_ = 12345 + i,
                         .allocation_epoch_ = 17,
                         .payload_bytes_ = 1,
                         .payload_checksum_ = 0});
  }
  {
    auto object = Create(input);
    ASSERT_TRUE(object.ok());
    reader = (*object)->ExtentsFor(group.id_);
    ASSERT_NE(reader, nullptr);
  }
  {
    // CopyManifest accounts allocator-usable payload bytes, not a linear
    // sizeof(ExtentRef) delta. For example, 24 bytes may occupy a 32-byte
    // size class while a 64-entry payload needs exactly 1536 bytes.
    const auto payload_growth =
        static_cast<std::int64_t>(
            AllocatorUsableSizeForRequest(64 * sizeof(ExtentRef))) -
        static_cast<std::int64_t>(
            AllocatorUsableSizeForRequest(sizeof(ExtentRef)));
    EXPECT_EQ(WorkerMemoryAccountingBytes(owner) - baseline - small_bytes,
              payload_growth);
  }
  reader.reset();
  EXPECT_EQ(WorkerMemoryAccountingBytes(owner), baseline);
}

TEST(GroupedObjectIndexTest, ManifestReaderRetainsItsOwnMemoryCharge) {
  GroupedMemoryScope memory;
  const auto baseline = WorkerMemoryAccountingBytes(0);
  std::shared_ptr<const std::vector<ExtentRef>> reader;
  {
    auto input = Input();
    auto& group = input.locations_[0];
    group.location_ = GroupLocation(group.location_.block_id(), 5,
                                    group.location_.logical_size_, false, true);
    group.extents_ = std::make_shared<const std::vector<ExtentRef>>(
        std::initializer_list<ExtentRef>{{.block_id_ = 12345,
                                          .allocation_epoch_ = 17,
                                          .payload_bytes_ = 1,
                                          .payload_checksum_ = 0}});
    auto object = Create(input);
    ASSERT_TRUE(object.ok());
    reader = (*object)->ExtentsFor(group.id_);
    ASSERT_NE(reader, nullptr);
  }
  // The object and every index entry are gone, but the actual vector remains
  // live through the read handle. Its charge must follow that same lifetime.
  EXPECT_GT(WorkerMemoryAccountingBytes(0), baseline);
  EXPECT_EQ(reader->at(0).block_id_, 12345);
  BindMemoryAccountingShard(kMaxMemoryWorkers);
  reader.reset();
  EXPECT_EQ(WorkerMemoryAccountingBytes(0), baseline);
}

TEST(GroupedObjectIndexTest, StoresOneCompactPhysicalIndexEntryPerGroup) {
  auto input = Input();
  auto object = GroupedHashObject::Create(input.version_, input.directory_,
                                          input.locations_);
  ASSERT_TRUE(object.ok()) << object.status();
  EXPECT_EQ((*object)->group_count(), input.locations_.size());
  EXPECT_LT((*object)->group_count(), 100);
  EXPECT_EQ(sizeof(RecordIndex::Entry), 24);
  for (unsigned i = 0; i < 100; ++i) {
    const std::string field = "field" + std::to_string(i);
    const auto* route = input.directory_.Find(field);
    ASSERT_NE(route, nullptr);
    const auto* location = (*object)->FindGroup(field);
    ASSERT_NE(location, nullptr);
    EXPECT_EQ(location->value_.block_id(), route->record_token_);
    EXPECT_EQ(location->value_.mutation_sequence_, route->sequence_);
    EXPECT_EQ(location->value_.logical_size(), route->field_count_);
    EXPECT_FALSE(location->has_extra());
    EXPECT_EQ(location, (*object)->FindGroup(route->id_));
  }
  EXPECT_EQ((*object)->FindGroup(HashGroupId{1, 0}), nullptr);
  EXPECT_EQ((*object)->ExtentsFor(HashGroupId{1, 0}), nullptr);
}

TEST(GroupedObjectIndexTest, NonGroupedRootBypassesSideTable) {
  GroupedObjectIndex index;
  GroupedObjectVersion compact{.root_ = GroupLocation(1, 1, 1)};
  auto missing = index.Lookup("ordinary", compact);
  ASSERT_TRUE(missing.ok());
  EXPECT_EQ(*missing, nullptr);
  auto grouped = Input().version_;
  EXPECT_EQ(index.Lookup("missing", grouped).status().code(),
            absl::StatusCode::kDataLoss);
}

TEST(GroupedObjectIndexTest, LookupRequiresPhysicalRootAndPopulationVersion) {
  auto object = Create(Input());
  ASSERT_TRUE(object.ok()) << object.status();
  GroupedObjectIndex index;
  ASSERT_TRUE(index.Publish("key", nullptr, *object).ok());
  auto exact = index.Lookup("key", (*object)->version());
  ASSERT_TRUE(exact.ok());
  EXPECT_EQ(*exact, *object);
  for (unsigned change = 0; change < 7; ++change) {
    auto version = (*object)->version();
    switch (change) {
      case 0:
        ++version.db_epoch_;
        break;
      case 1:
        ++version.replication_epoch_;
        break;
      case 2:
        ++version.index_generation_;
        break;
      case 3:
        ++version.root_.mutation_sequence_;
        break;
      case 4:
        ++version.root_.logical_size_;
        break;
      case 5:
        version.root_.expire_at_ms_ = 100;
        break;
      case 6:
        version.root_ = GroupLocation(1234, 5, 100, true);
        break;
    }
    EXPECT_EQ(index.Lookup("key", version).status().code(),
              absl::StatusCode::kDataLoss)
        << change;
  }
  auto flushed = (*object)->version();
  flushed.root_.set_in_memory(false);
  flushed.root_.set_shielding(true);
  flushed.root_.set_unclaimed(true);
  EXPECT_TRUE(index.Lookup("key", flushed).ok());
}

TEST(GroupedObjectIndexTest, PublishAndRollbackAreCompareAndSwap) {
  auto old = Create(Input(5));
  auto next = Create(Input(6));
  ASSERT_TRUE(old.ok());
  ASSERT_TRUE(next.ok());
  GroupedObjectIndex index;
  ASSERT_TRUE(index.Publish("key", nullptr, *old).ok());
  EXPECT_EQ(index.Publish("key", nullptr, *next).code(),
            absl::StatusCode::kAborted);
  ASSERT_TRUE(index.Publish("key", *old, *next).ok());
  EXPECT_EQ(index.Publish("key", *old, *next).code(),
            absl::StatusCode::kAborted);
  ASSERT_TRUE(index.Publish("key", *next, *old).ok());
  auto restored = index.Lookup("key", (*old)->version());
  ASSERT_TRUE(restored.ok());
  EXPECT_EQ(*restored, *old);
  EXPECT_EQ(index.Erase("key", *next).code(), absl::StatusCode::kAborted);
  ASSERT_TRUE(index.Erase("key", *old).ok());
  EXPECT_TRUE(index.empty());
  EXPECT_TRUE(index.Erase("key", nullptr).ok());
  EXPECT_EQ(index.Publish("key", nullptr, nullptr).code(),
            absl::StatusCode::kInvalidArgument);
}

TEST(GroupedObjectIndexTest,
     UndoPlaceholderRestoresWithoutAllocationAtMaxmemory) {
  GroupedMemoryScope memory;
  auto old = Create(Input());
  ASSERT_TRUE(old.ok());
  GroupedObjectIndex index;
  ASSERT_TRUE(index.Publish("key", nullptr, *old).ok());
  const auto retained_before = WorkerMemoryAccountingBytes(0);
  ASSERT_TRUE(index.ClearKeepingSlot("key", *old).ok());
  EXPECT_EQ(index.size(), 1);
  EXPECT_FALSE(index.Lookup("key", (*old)->version()).ok());
  EXPECT_EQ(WorkerMemoryAccountingBytes(0), retained_before);
  ASSERT_TRUE(InitMemoryLimit(1, 1).ok());
  auto restore = index.PreparePublish("key", nullptr);
  ASSERT_TRUE(restore.ok()) << restore.status();
  EXPECT_TRUE(restore->Commit(*old).ok());
  auto restored = index.Lookup("key", (*old)->version());
  ASSERT_TRUE(restored.ok());
  EXPECT_EQ(*restored, *old);
  EXPECT_EQ(WorkerMemoryAccountingBytes(0), retained_before);
}

TEST(GroupedObjectIndexTest, SettlingOlderUndoDoesNotEraseRepublishedView) {
  auto old = Create(Input());
  auto next = Create(Input(6));
  ASSERT_TRUE(old.ok());
  ASSERT_TRUE(next.ok());
  GroupedObjectIndex index;
  ASSERT_TRUE(index.Publish("key", nullptr, *old).ok());
  ASSERT_TRUE(index.ClearKeepingSlot("key", *old).ok());
  ASSERT_TRUE(index.Publish("key", nullptr, *next).ok());
  index.EraseNullSlot("key");
  auto current = index.Lookup("key", (*next)->version());
  ASSERT_TRUE(current.ok());
  EXPECT_EQ(*current, *next);
  ASSERT_TRUE(index.ClearKeepingSlot("key", *next).ok());
  index.EraseNullSlot("key");
  EXPECT_TRUE(index.empty());
  index.EraseNullSlot("key");
  EXPECT_TRUE(index.empty());
}

TEST(GroupedObjectIndexTest, StaleUndoClearCannotRemoveCurrentView) {
  auto old = Create(Input());
  auto next = Create(Input(6));
  ASSERT_TRUE(old.ok());
  ASSERT_TRUE(next.ok());
  GroupedObjectIndex index;
  ASSERT_TRUE(index.Publish("key", nullptr, *next).ok());
  EXPECT_EQ(index.ClearKeepingSlot("key", *old).code(),
            absl::StatusCode::kAborted);
  EXPECT_EQ(index.ClearKeepingSlot("missing", *next).code(),
            absl::StatusCode::kAborted);
  EXPECT_EQ(index.ClearKeepingSlot("key", nullptr).code(),
            absl::StatusCode::kInvalidArgument);
  auto current = index.Lookup("key", (*next)->version());
  ASSERT_TRUE(current.ok());
  EXPECT_EQ(*current, *next);
}

TEST(GroupedObjectIndexTest,
     RecreatedKeyRejectsOldWriterDespiteMatchingSequence) {
  auto old = Create(Input(5, 1));
  auto recreated = Create(Input(5, 2));
  ASSERT_TRUE(old.ok());
  ASSERT_TRUE(recreated.ok());
  GroupedObjectIndex index;
  ASSERT_TRUE(index.Publish("key", nullptr, *old).ok());
  ASSERT_TRUE(index.Erase("key", *old).ok());
  ASSERT_TRUE(index.Publish("key", nullptr, *recreated).ok());
  EXPECT_EQ(index.Publish("key", *old, *old).code(),
            absl::StatusCode::kAborted);
  EXPECT_EQ(index.Erase("key", *old).code(), absl::StatusCode::kAborted);
  EXPECT_EQ(index.size(), 1);
}

TEST(GroupedObjectIndexTest, DetachPreservesOldPopulationAndSharesArena) {
  auto object = Create(Input());
  ASSERT_TRUE(object.ok());
  GroupedObjectIndex index;
  const std::string binary_key("a\0b", 3);
  for (unsigned i = 0; i < 2000; ++i) {
    ASSERT_TRUE(index.Publish(std::to_string(i), nullptr, *object).ok());
  }
  ASSERT_TRUE(index.Publish(binary_key, nullptr, *object).ok());
  auto detached = index.Detach();
  EXPECT_TRUE(index.empty());
  EXPECT_EQ(detached.size(), 2001);
  ASSERT_TRUE(index.Publish(binary_key, nullptr, *object).ok());
  ASSERT_TRUE(detached.Erase(binary_key, *object).ok());
  EXPECT_TRUE(index.Lookup(binary_key, (*object)->version()).ok());
  std::set<std::string> visited;
  detached.ForEach([&](std::string_view key, const auto& handle) {
    EXPECT_EQ(handle, *object);
    EXPECT_TRUE(visited.emplace(key).second);
  });
  EXPECT_EQ(visited.size(), 2000);
  for (unsigned i = 0; i < 2000; ++i) {
    EXPECT_TRUE(detached.Lookup(std::to_string(i), (*object)->version()).ok());
  }
}

TEST(GroupedObjectIndexTest,
     MetadataHandleOutlivesRemovalWithoutPretendingToPinDisk) {
  auto object = Create(Input());
  ASSERT_TRUE(object.ok());
  std::weak_ptr<const GroupedHashObject> weak = *object;
  GroupedObjectIndex index;
  ASSERT_TRUE(index.Publish("key", nullptr, *object).ok());
  auto reader = index.Lookup("key", (*object)->version());
  ASSERT_TRUE(reader.ok());
  ASSERT_TRUE(index.Erase("key", *object).ok());
  object->reset();
  EXPECT_FALSE(weak.expired());
  EXPECT_NE((*reader)->FindGroup("field0"), nullptr);
  reader->reset();
  EXPECT_TRUE(weak.expired());
}

TEST(GroupedObjectIndexTest, RejectsIncompleteDuplicateAndStaleGroupLocations) {
  for (unsigned change = 0; change < 8; ++change) {
    auto input = Input();
    ASSERT_GT(input.locations_.size(), 1);
    switch (change) {
      case 0:
        input.locations_.pop_back();
        break;
      case 1:
        input.locations_[1] = input.locations_[0];
        break;
      case 2:
        ++input.locations_[0].location_.mutation_sequence_;
        break;
      case 3:
        ++input.locations_[0].location_.logical_size_;
        break;
      case 4:
        input.locations_[0].id_ = {1, 0};
        break;
      case 5:
        // A TTL-only root may be newer than its shared value directory, but
        // never older than the directory's source command.
        --input.version_.root_.mutation_sequence_;
        break;
      case 6:
        input.version_.db_epoch_ = 0;
        break;
      case 7:
        input.locations_[0].location_.expire_at_ms_ = 123;
        break;
    }
    EXPECT_EQ(Create(std::move(input)).status().code(),
              absl::StatusCode::kDataLoss)
        << change;
  }
}

TEST(GroupedObjectIndexTest, ExternalGroupManifestIsSparseAndRequired) {
  auto input = Input();
  const auto old = input.locations_[0].location_;
  input.locations_[0].location_ = GroupLocation(
      old.block_id(), old.mutation_sequence_, old.logical_size_, false, true);
  EXPECT_FALSE(Create(input).ok());
  auto extents = std::make_shared<std::vector<ExtentRef>>();
  input.locations_[0].extents_ = extents;
  EXPECT_FALSE(Create(input).ok());
  extents->push_back({.block_id_ = 777,
                      .allocation_epoch_ = 17,
                      .payload_bytes_ = 1024,
                      .payload_checksum_ = 99});
  auto object = Create(input);
  ASSERT_TRUE(object.ok()) << object.status();
  const auto retained = (*object)->ExtentsFor(input.locations_[0].id_);
  ASSERT_NE(retained, nullptr);
  EXPECT_NE(retained, extents);
  EXPECT_EQ(*retained, *extents);
  for (std::size_t i = 1; i < input.locations_.size(); ++i) {
    EXPECT_EQ((*object)->ExtentsFor(input.locations_[i].id_), nullptr);
  }
  (*extents)[0].allocation_epoch_ = 0;
  EXPECT_EQ((*retained)[0].allocation_epoch_, 17);
  EXPECT_EQ(Create(input).status().code(), absl::StatusCode::kDataLoss);
}

TEST(GroupedObjectIndexTest, ArenaCapacityFailurePublishesNothing) {
  auto arena = std::make_shared<ScanHashMapEntryArena>(0, true, false);
  auto input = Input();
  auto rejected = GroupedHashObject::Create(input.version_, input.directory_,
                                            input.locations_, arena);
  EXPECT_EQ(rejected.status().code(), absl::StatusCode::kResourceExhausted);
  EXPECT_EQ(arena->allocated_pages(), 0);
  auto object = Create(Input());
  ASSERT_TRUE(object.ok());
  GroupedObjectIndex index(arena);
  EXPECT_EQ(index.Publish("key", nullptr, *object).code(),
            absl::StatusCode::kResourceExhausted);
  EXPECT_TRUE(index.empty());
  EXPECT_EQ(arena->allocated_pages(), 0);
}

TEST(GroupedObjectIndexTest, RetainedMetadataIsReleasedWithLastHandle) {
  ASSERT_TRUE(InitMemoryLimit(64 * 1024 * 1024, 1).ok());
  BindMemoryAccountingShard(0);
  const auto before = WorkerMemoryAccountingBytes(0);
  {
    auto object = Create(Input());
    ASSERT_TRUE(object.ok());
    GroupedObjectIndex index;
    ASSERT_TRUE(index.Publish("key", nullptr, *object).ok());
    auto old = index.Detach();
    EXPECT_GT(WorkerMemoryAccountingBytes(0), before);
  }
  EXPECT_EQ(WorkerMemoryAccountingBytes(0), before);
  BindMemoryAccountingShard(kMaxMemoryWorkers);
  ASSERT_TRUE(InitMemoryLimit(1024ULL * 1024 * 1024, 1).ok());
}

TEST(GroupedObjectIndexTest, MaxmemoryRejectsConstructionWithoutTerminating) {
  auto input = Input();
  ASSERT_TRUE(InitMemoryLimit(1, 1).ok());
  auto rejected = Create(std::move(input));
  EXPECT_EQ(rejected.status().code(), absl::StatusCode::kResourceExhausted);
  ASSERT_TRUE(InitMemoryLimit(1024ULL * 1024 * 1024, 1).ok());
}

TEST(GroupedObjectIndexTest,
     RejectsConflictingAllocationsBeforeCompactingLocations) {
  for (bool change_epoch : {false, true}) {
    auto input = Input();
    ASSERT_GT(input.locations_.size(), 1);
    const auto first = input.locations_[0].location_;
    const auto second = input.locations_[1].location_;
    input.locations_[1].location_ = RecordLocation(
        first.block_id(), second.mutation_sequence_,
        first.allocation_epoch() + (change_epoch ? 1 : 0), 0,
        second.logical_size_,
        RecordLocation::PackedMetadata::Encode(
            kBlockHeaderBytes + 256, 256, change_epoch ? 0 : 1, true, false,
            false, false, false, false, RecordKind::kValue, ValueType::kHash));
    EXPECT_EQ(Create(std::move(input)).status().code(),
              absl::StatusCode::kDataLoss);
  }
}

TEST(GroupedObjectIndexTest, MultipleGroupsCanShareOnePackedRecordsBlock) {
  auto input = Input();
  for (std::size_t i = 0; i < input.locations_.size(); ++i) {
    const auto old = input.locations_[i].location_;
    input.locations_[i].location_ = RecordLocation(
        123, old.mutation_sequence_, old.allocation_epoch(), 0,
        old.logical_size_,
        RecordLocation::PackedMetadata::Encode(
            kBlockHeaderBytes + i * 256, 256, 0, true, false, false, false,
            false, false, RecordKind::kValue, ValueType::kHash));
  }
  auto object = Create(std::move(input));
  ASSERT_TRUE(object.ok()) << object.status();
  EXPECT_EQ((*object)->FindGroup("field0")->value_.block_id(), 123);
  EXPECT_EQ((*object)->FindGroup("field99")->value_.block_id(), 123);
}

TEST(GroupedObjectIndexTest,
     IncrementalUpdateSharesUntouchedPagesAndRoutingNodes) {
  GroupedMemoryScope memory;
  auto input = Input(5, 1, 2000, 256);
  auto old = Create(input);
  ASSERT_TRUE(old.ok()) << old.status();
  const auto changed_id = input.locations_[0].id_;
  auto candidate = input.directory_.groups().at(changed_id.prefix_);
  candidate.sequence_ = 6;
  candidate.record_token_ = 500000;
  auto directory = Apply(input.directory_, input.directory_.root(), 6,
                         std::span(&candidate, 1));
  ASSERT_TRUE(directory.ok()) << directory.status();
  auto version = input.version_;
  version.root_.mutation_sequence_ = 6;
  HashGroupLocation changed{
      .id_ = changed_id,
      .location_ = GroupLocation(500000, 6, candidate.field_count_)};
  auto next = GroupedHashObject::PrepareUpdate(*old, version, *directory,
                                               std::span(&changed, 1));
  ASSERT_TRUE(next.ok()) << next.status();
  std::size_t shared_entries = 0;
  std::size_t shared_routes = 0;
  for (const auto& location : input.locations_) {
    shared_entries +=
        (*old)->FindRecord(location.id_) == (*next)->FindRecord(location.id_);
    shared_routes += &(*old)->directory().groups().at(location.id_.prefix_) ==
                     &(*next)->directory().groups().at(location.id_.prefix_);
  }
  EXPECT_GE(shared_entries, input.locations_.size() - 64);
  EXPECT_GE(shared_routes, input.locations_.size() - 32);
  EXPECT_EQ((*old)->FindRecord(changed_id)->value_.mutation_sequence_, 5);
  EXPECT_EQ((*next)->FindRecord(changed_id)->value_.mutation_sequence_, 6);
  version.root_ = GroupLocation(600000, 6, version.root_.logical_size_, true);
  EXPECT_TRUE(GroupedHashObject::FinalizeRoot(*next, version).ok());
  EXPECT_EQ((*next)->version().root_.block_id(), 600000);
  EXPECT_EQ((*old)->version().root_.block_id(), 999999);
}

TEST(GroupedObjectIndexTest, SplitRetainsParentMarkerAcrossFurtherMutations) {
  auto input = Input(5, 1, 2, 4096);
  ASSERT_EQ(input.locations_.size(), 1);
  auto old = Create(input);
  ASSERT_TRUE(old.ok());
  auto root = input.directory_.root();
  root.group_count_ = 2;
  const std::array<RecoveredHashGroup, 3> changes{
      {{.incarnation_ = 1, .id_ = {0, 0}, .sequence_ = 6, .retired_ = true},
       {.incarnation_ = 1, .id_ = {0, 1}, .sequence_ = 6, .field_count_ = 1},
       {.incarnation_ = 1,
        .id_ = {1ULL << 63, 1},
        .sequence_ = 6,
        .field_count_ = 1}}};
  auto directory = Apply(input.directory_, root, 6, changes);
  ASSERT_TRUE(directory.ok()) << directory.status();
  ASSERT_EQ(directory->retired_groups().size(), 1);
  const std::array<HashGroupLocation, 3> locations{
      {{.id_ = {0, 0}, .location_ = GroupLocation(10, 6, 0), .retired_ = true},
       {.id_ = {0, 1}, .location_ = GroupLocation(11, 6, 1)},
       {.id_ = {1ULL << 63, 1}, .location_ = GroupLocation(12, 6, 1)}}};
  auto version = input.version_;
  version.root_.mutation_sequence_ = 6;
  auto next =
      GroupedHashObject::PrepareUpdate(*old, version, *directory, locations);
  ASSERT_TRUE(next.ok()) << next.status();
  EXPECT_EQ((*next)->group_count(), 2);
  EXPECT_EQ((*next)->record_count(), 3);
  EXPECT_EQ((*next)->FindGroup(HashGroupId{0, 0}), nullptr);
  ASSERT_NE((*next)->FindRecord({0, 0}), nullptr);
  EXPECT_EQ((*next)->FindRecord({0, 0})->value_.block_id(), 10);
  unsigned active = 0, retired = 0;
  (*next)->ForEachRecord([&](HashGroupId, const auto&, const auto&,
                             bool marker) { marker ? ++retired : ++active; });
  EXPECT_EQ(active, 2);
  EXPECT_EQ(retired, 1);
  auto missing_marker = GroupedHashObject::Create(
      version, *directory, std::span(locations).subspan(1));
  EXPECT_EQ(missing_marker.status().code(), absl::StatusCode::kDataLoss);
  auto rebuilt = GroupedHashObject::Create(version, *directory, locations);
  ASSERT_TRUE(rebuilt.ok()) << rebuilt.status();
  auto newer = changes[1];
  newer.sequence_ = 7;
  auto again = Apply(*directory, root, 7, std::span(&newer, 1));
  ASSERT_TRUE(again.ok()) << again.status();
  EXPECT_EQ(again->retired_groups().at({0, 0}).sequence_, 6);
  // An old complete parent physically surviving a GC cycle is still defeated
  // by its retained marker when cold recovery adjudicates the same incarnation.
  std::vector<RecoveredHashGroup> candidates(changes.begin(), changes.end());
  candidates.push_back(input.directory_.groups().at(0));
  auto recovered = Recover(root, 6, candidates, {});
  ASSERT_TRUE(recovered.ok()) << recovered.status();
  EXPECT_EQ(recovered->groups().size(), 2);
  EXPECT_EQ(recovered->retired_groups().size(), 1);
}

TEST(GroupedObjectIndexTest,
     RootRelocationSharesAllMetadataAndRefreshesGroupGc) {
  auto input = Input();
  auto old = Create(input);
  ASSERT_TRUE(old.ok());
  GroupedObjectIndex index;
  ASSERT_TRUE(index.Publish("key", nullptr, *old).ok());
  auto publication = index.PreparePublish("key", *old);
  ASSERT_TRUE(publication.ok());
  auto builder = GroupedHashObject::PrepareRootRelocation(*old);
  ASSERT_TRUE(builder.ok());
  auto source = input.locations_[0];
  auto destination = GroupLocation(77777, source.location_.mutation_sequence_,
                                   source.location_.logical_size_);
  auto relocated = GroupedHashObject::RelocateGroup(
      *old, source.id_, source.location_, destination);
  ASSERT_TRUE(relocated.ok()) << relocated.status();
  ASSERT_TRUE(index.Publish("key", *old, *relocated).ok());
  auto version = input.version_;
  version.root_ = GroupLocation(88888, 5, 100, true);
  ASSERT_TRUE(
      GroupedHashObject::FinalizeRootRelocation(*builder, *relocated, version)
          .ok());
  ASSERT_TRUE(publication->RefreshExpected(*relocated).ok());
  ASSERT_TRUE(publication->Commit(std::move(*builder)).ok());
  auto current = index.Lookup("key", version);
  ASSERT_TRUE(current.ok());
  EXPECT_EQ((*current)->FindRecord(source.id_)->value_.block_id(), 77777);
  EXPECT_NE((*old)->FindRecord(source.id_)->value_.block_id(), 77777);
  auto direct = GroupedHashObject::RelocateRoot(*current, input.version_);
  ASSERT_TRUE(direct.ok());
  for (const auto& group : input.locations_) {
    EXPECT_EQ((*direct)->FindRecord(group.id_),
              (*current)->FindRecord(group.id_));
  }
}

TEST(GroupedObjectIndexTest,
     ReservedPublicationAndFinalizationAllocateNothing) {
  GroupedMemoryScope memory;
  auto input = Input();
  auto builder = GroupedHashObject::PrepareCreate(
      input.version_, input.directory_, input.locations_);
  ASSERT_TRUE(builder.ok());
  GroupedObjectIndex index;
  {
    auto canceled = index.PreparePublish("cancel", nullptr);
    ASSERT_TRUE(canceled.ok());
    EXPECT_FALSE(index.Lookup("cancel", input.version_).ok());
  }
  EXPECT_TRUE(index.empty());
  auto publication = index.PreparePublish("key", nullptr);
  ASSERT_TRUE(publication.ok());
  ASSERT_TRUE(InitMemoryLimit(1, 1).ok());
  auto exact = input.version_;
  exact.root_ = GroupLocation(999998, 5, 100, true);
  EXPECT_TRUE(GroupedHashObject::FinalizeRoot(*builder, exact).ok());
  EXPECT_TRUE(publication->Commit(std::move(*builder)).ok());
  EXPECT_TRUE(index.Lookup("key", exact).ok());
}

TEST(GroupedObjectIndexTest,
     IncrementalMetadataOomLeavesPublishedObjectIntact) {
  GroupedMemoryScope memory;
  auto input = Input();
  auto old = Create(input);
  ASSERT_TRUE(old.ok());
  auto change = input.directory_.groups().begin()->second;
  change.sequence_ = 6;
  auto directory = Apply(input.directory_, input.directory_.root(), 6,
                         std::span(&change, 1));
  ASSERT_TRUE(directory.ok());
  auto version = input.version_;
  version.root_.mutation_sequence_ = 6;
  HashGroupLocation physical{
      .id_ = change.id_,
      .location_ = GroupLocation(98765, 6, change.field_count_)};
  const auto before = WorkerMemoryAccountingBytes(0);
  ASSERT_TRUE(InitMemoryLimit(1, 1).ok());
  auto rejected = GroupedHashObject::PrepareUpdate(*old, version, *directory,
                                                   std::span(&physical, 1));
  EXPECT_EQ(rejected.status().code(), absl::StatusCode::kResourceExhausted);
  EXPECT_TRUE(rejected.status().message().starts_with("OOM "));
  EXPECT_EQ(WorkerMemoryAccountingBytes(0), before);
  EXPECT_EQ((*old)->FindRecord(change.id_)->value_.mutation_sequence_, 5);
  auto rejected_route = Apply(input.directory_, input.directory_.root(), 6,
                              std::span(&change, 1));
  EXPECT_EQ(rejected_route.status().code(),
            absl::StatusCode::kResourceExhausted);
  EXPECT_EQ(input.directory_.sequence(), 5);
}

TEST(GroupedObjectIndexTest,
     DirectoryIncrementalValidationRejectsPartialSplits) {
  auto input = Input(5, 1, 2, 4096);
  auto root = input.directory_.root();
  root.group_count_ = 2;
  const std::array<RecoveredHashGroup, 2> incomplete{
      {{.incarnation_ = 1, .id_ = {0, 0}, .sequence_ = 6, .retired_ = true},
       {.incarnation_ = 1, .id_ = {0, 1}, .sequence_ = 6, .field_count_ = 2}}};
  EXPECT_EQ(Apply(input.directory_, root, 6, incomplete).status().code(),
            absl::StatusCode::kDataLoss);
  EXPECT_EQ(input.directory_.groups().size(), 1);
  EXPECT_TRUE(input.directory_.retired_groups().empty());
}

TEST(GroupedObjectIndexTest, RetiredMarkerCanKeepAnExternalParentKey) {
  auto input = Input(5, 1, 2, 4096);
  auto root = input.directory_.root();
  root.group_count_ = 2;
  const std::array<RecoveredHashGroup, 3> changes{
      {{.incarnation_ = 1, .id_ = {0, 0}, .sequence_ = 6, .retired_ = true},
       {.incarnation_ = 1, .id_ = {0, 1}, .sequence_ = 6, .field_count_ = 1},
       {.incarnation_ = 1,
        .id_ = {1ULL << 63, 1},
        .sequence_ = 6,
        .field_count_ = 1}}};
  auto directory = Apply(input.directory_, root, 6, changes);
  ASSERT_TRUE(directory.ok());
  auto marker =
      RecordLocation(10, 6, 17, 0, 0,
                     RecordLocation::PackedMetadata::Encode(
                         kBlockHeaderBytes, 256, 0, true, true, true, false,
                         false, false, RecordKind::kValue, ValueType::kHash));
  auto manifest = std::make_shared<const std::vector<ExtentRef>>(
      std::vector<ExtentRef>{{.block_id_ = 20,
                              .allocation_epoch_ = 17,
                              .payload_bytes_ = 1024,
                              .payload_checksum_ = 1}});
  const std::array<HashGroupLocation, 3> locations{
      {{.id_ = {0, 0},
        .location_ = marker,
        .extents_ = manifest,
        .retired_ = true},
       {.id_ = {0, 1}, .location_ = GroupLocation(11, 6, 1)},
       {.id_ = {1ULL << 63, 1}, .location_ = GroupLocation(12, 6, 1)}}};
  auto version = input.version_;
  version.root_.mutation_sequence_ = 6;
  auto object = GroupedHashObject::Create(version, *directory, locations);
  ASSERT_TRUE(object.ok()) << object.status();
  const auto* retained = (*object)->FindRecord({0, 0});
  ASSERT_NE(retained, nullptr);
  EXPECT_TRUE(retained->value_.key_indirect());
  EXPECT_TRUE(retained->value_.external());
  ASSERT_NE((*object)->ExtentsFor({0, 0}), nullptr);
  EXPECT_EQ((*object)->ExtentsFor({0, 0})->at(0).block_id_, 20);
}

TEST(GroupedObjectIndexTest,
     GroupRecoveryRequiresOuterAndCommandBatchDecisions) {
  auto input = Input(5, 1, 2, 4096);
  auto old = input.directory_.groups().at(0);
  auto prepared = old;
  prepared.sequence_ = 6;
  prepared.txid_ = 77;
  prepared.batch_txid_ = 100;
  prepared.record_token_ = 999;
  const std::array<RecoveredHashGroup, 2> candidates{old, prepared};
  for (const auto& decisions : {absl::flat_hash_set<std::uint64_t>{},
                                absl::flat_hash_set<std::uint64_t>{77},
                                absl::flat_hash_set<std::uint64_t>{100}}) {
    auto recovered = Recover(input.directory_.root(), 6, candidates, decisions);
    ASSERT_TRUE(recovered.ok()) << recovered.status();
    EXPECT_EQ(recovered->groups().at(0).sequence_, 5);
  }
  auto committed = Recover(input.directory_.root(), 6, candidates, {77, 100});
  ASSERT_TRUE(committed.ok());
  EXPECT_EQ(committed->groups().at(0).sequence_, 6);
  EXPECT_EQ(committed->groups().at(0).record_token_, 999);
}

TEST(GroupedObjectIndexTest, RootPhysicalFinalizationKeepsPendingDecision) {
  auto input = Input();
  auto decision = std::make_shared<GroupedCommitDecision>(77);
  input.version_.decision_ = decision;
  auto builder = GroupedHashObject::PrepareCreate(
      input.version_, input.directory_, input.locations_);
  ASSERT_TRUE(builder.ok());
  auto physical = input.version_;
  physical.decision_.reset();
  physical.root_ = GroupLocation(555555, 5, 100, true);
  ASSERT_TRUE(GroupedHashObject::FinalizeRoot(*builder, physical).ok());
  EXPECT_EQ((*builder)->version().decision_, decision);
  GroupedHashObject::Handle current = std::move(*builder);
  physical.root_ = GroupLocation(555556, 5, 100, true);
  auto relocated = GroupedHashObject::RelocateRoot(current, physical);
  ASSERT_TRUE(relocated.ok());
  EXPECT_EQ((*relocated)->version().decision_, decision);
  EXPECT_EQ(decision->state_.load(), GroupedCommitDecision::State::kPending);
}

TEST(GroupedObjectIndexTest, FailedPublishedDecisionCannotBeRead) {
  auto input = Input();
  auto decision = std::make_shared<GroupedCommitDecision>(77);
  input.version_.decision_ = decision;
  auto object = GroupedHashObject::Create(input.version_, input.directory_,
                                          input.locations_);
  ASSERT_TRUE(object.ok()) << object.status();
  GroupedObjectIndex index;
  ASSERT_TRUE(index.Publish("key", nullptr, *object).ok());
  // Pending is a valid read-your-writes view under the outer key intent.
  EXPECT_TRUE(index.Lookup("key", input.version_).ok());
  EXPECT_TRUE((*object)->ReadStatus().ok());
  decision->FailPending();
  EXPECT_EQ(index.Lookup("key", input.version_).status().code(),
            absl::StatusCode::kFailedPrecondition);
  // A retained snapshot cannot bypass the decision check after an IO await.
  EXPECT_EQ((*object)->ReadStatus().code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_TRUE(index.Lookup("key", input.version_, true).ok());
  auto wrong = input.version_;
  ++wrong.db_epoch_;
  EXPECT_EQ(index.Lookup("key", wrong, true).status().code(),
            absl::StatusCode::kDataLoss);
}

TEST(GroupedObjectIndexTest,
     MetadataOnlyUpdateSharesHashDirectoryAndPhysicalPages) {
  auto input = Input();
  auto old = GroupedHashObject::Create(input.version_, input.directory_,
                                       input.locations_);
  ASSERT_TRUE(old.ok()) << old.status();
  auto version = input.version_;
  version.root_ = RecordLocation(
      1000000, 6, 17, 123456, 100,
      RecordLocation::PackedMetadata::Encode(
          kBlockHeaderBytes, 256, 0, true, false, false, false, false, false,
          RecordKind::kValue, ValueType::kHash, true, true));
  auto updated = GroupedHashObject::PrepareMetadataUpdate(*old, version);
  ASSERT_TRUE(updated.ok()) << updated.status();
  EXPECT_TRUE(GroupedHashObject::FinalizeRoot(*updated, version).ok());
  EXPECT_EQ(&(*old)->directory(), &(*updated)->directory());
  for (const auto& group : input.locations_)
    EXPECT_EQ((*old)->FindRecord(group.id_), (*updated)->FindRecord(group.id_));
  EXPECT_EQ((*updated)->command_sequence(), 6);
  EXPECT_EQ((*updated)->directory().command_sequence(), 5);
  EXPECT_EQ((*updated)->revision(), 5);
  EXPECT_EQ((*updated)->version().root_.expire_at_ms_, 123456);
  EXPECT_EQ((*old)->version().root_.expire_at_ms_, 0);
  EXPECT_TRUE((*old)->SameLogicalRoot(**updated));
}

}  // namespace
}  // namespace lavik::storage
