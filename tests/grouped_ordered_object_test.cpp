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

#include <set>

#include "gtest/gtest.h"
#include "lavik/storage/detail/grouped_object_index.h"

namespace lavik::storage {
namespace {

TEST(StreamPageMaxKeyTest, ExactIdsAndLongNamesPreserveOrdering) {
  StreamPageMaxKey boundary;
  EXPECT_FALSE(boundary.LessThan("\1"));
  EXPECT_FALSE(boundary.LessThanOrEqual("\1"));
  const std::string id = std::string("\1", 1) + std::string(16, '\x7f');
  boundary.Set(id);
  EXPECT_EQ(boundary.LessThan(id), false);
  EXPECT_EQ(boundary.LessThanOrEqual(id), true);
  EXPECT_EQ(boundary.LessThan(id + '\1'), true);
  EXPECT_EQ(boundary.LessThanOrEqual(id + '\1'), true);
  EXPECT_EQ(boundary.LessThan(std::string("\1", 1) + std::string(16, '\x7e')),
            false);

  const std::string long_name = std::string("\5", 1) + std::string(80, 'n');
  boundary.Set(long_name);
  EXPECT_EQ(boundary.LessThan(id), false);
  EXPECT_EQ(boundary.LessThan(std::string("\6", 1)), true);
  EXPECT_FALSE(boundary.LessThan(long_name));
  EXPECT_FALSE(boundary.LessThanOrEqual(long_name));
  EXPECT_EQ(
      boundary.LessThan(long_name.substr(0, StreamPageMaxKey::kPrefixBytes)),
      false);
}

TEST(StreamDirectoryTest, RetainsUnchangedHeaderAndInvalidatesReplacement) {
  OrderedCollectionRoot root{.kind_ = OrderedCollectionKind::kStream,
                             .incarnation_ = 17,
                             .item_count_ = 2,
                             .first_group_ = 1,
                             .last_group_ = 2,
                             .next_group_id_ = 3,
                             .group_count_ = 2,
                             .revision_ = 1,
                             .stream_length_ = 1};
  std::vector<RecoveredOrderedGroup> pages{{.incarnation_ = 17,
                                            .id_ = 1,
                                            .next_ = 2,
                                            .sequence_ = 1,
                                            .lsn_ = 1,
                                            .item_count_ = 1,
                                            .record_token_ = 1},
                                           {.incarnation_ = 17,
                                            .id_ = 2,
                                            .previous_ = 1,
                                            .sequence_ = 1,
                                            .lsn_ = 2,
                                            .item_count_ = 1,
                                            .record_token_ = 2}};
  auto directory = OrderedGroupDirectory::Recover(root, 1, pages, {});
  ASSERT_TRUE(directory.ok()) << directory.status();
  EXPECT_EQ(directory->CountBefore(0), 0);
  EXPECT_EQ(directory->CountBefore(1), 1);
  EXPECT_EQ(directory->CountBefore(2), 2);
  std::string header(48, '\0');
  header.replace(0, 4, "LXS1");
  header[44] = 1;
  ASSERT_TRUE(directory->RememberStreamHeader(header).ok());
  const std::string id = std::string("\1", 1) + std::string(16, '\x7f');
  ASSERT_TRUE(directory->RememberStreamPageMaxKey(1, id).ok());

  root.revision_ = 2;
  auto unchanged = directory->Apply(root, 2, {}, 2);
  ASSERT_TRUE(unchanged.ok()) << unchanged.status();
  EXPECT_EQ(unchanged->stream_header(), header);
  EXPECT_EQ(unchanged->groups()[1].stream_max_key_.LessThan(id), false);

  auto replacement = pages[0];
  replacement.sequence_ = 3;
  replacement.lsn_ = 3;
  root.revision_ = 3;
  auto changed = unchanged->Apply(root, 3, std::span(&replacement, 1), 3);
  ASSERT_TRUE(changed.ok()) << changed.status();
  EXPECT_TRUE(changed->stream_header().empty());
  EXPECT_EQ(changed->groups()[1].stream_max_key_.LessThan(id), false);
}

RecordLocation OrderedLocation(std::uint64_t block, std::uint64_t sequence,
                               std::uint32_t count, ValueType type,
                               bool root = false, std::uint64_t expiry = 0) {
  return RecordLocation(
      block, sequence, 17, expiry, count,
      RecordLocation::PackedMetadata::Encode(
          kBlockHeaderBytes, 256, 0, true, false, false, false, false, false,
          RecordKind::kValue, type, expiry != 0, root));
}

struct OrderedInput {
  GroupedObjectVersion version_;
  OrderedGroupDirectory directory_;
  std::vector<HashGroupLocation> locations_;
};

OrderedInput OrderedFixture(ValueType type = ValueType::kList) {
  const auto kind = type == ValueType::kList
                        ? OrderedCollectionKind::kList
                        : OrderedCollectionKind::kSortedSet;
  OrderedCollectionRoot root{.kind_ = kind,
                             .incarnation_ = 17,
                             .item_count_ = 4,
                             .first_group_ = 1,
                             .last_group_ = 3,
                             .next_group_id_ = 4,
                             .group_count_ = 2,
                             .revision_ = 3};
  std::vector<RecoveredOrderedGroup> candidates{{.incarnation_ = 17,
                                                 .id_ = 1,
                                                 .next_ = 3,
                                                 .sequence_ = 3,
                                                 .lsn_ = 1,
                                                 .item_count_ = 2,
                                                 .record_token_ = 1},
                                                {.incarnation_ = 17,
                                                 .id_ = 2,
                                                 .sequence_ = 3,
                                                 .lsn_ = 2,
                                                 .record_token_ = 2,
                                                 .retired_ = true},
                                                {.incarnation_ = 17,
                                                 .id_ = 3,
                                                 .previous_ = 1,
                                                 .sequence_ = 3,
                                                 .lsn_ = 3,
                                                 .item_count_ = 2,
                                                 .record_token_ = 3}};
  auto directory = OrderedGroupDirectory::Recover(root, 3, candidates, {}, 7);
  EXPECT_TRUE(directory.ok()) << directory.status();
  OrderedInput input;
  if (!directory.ok()) return input;
  input.directory_ = std::move(*directory);
  input.version_ = {.root_ = OrderedLocation(999, 7, 4, type, true),
                    .db_epoch_ = 1,
                    .replication_epoch_ = 2,
                    .index_generation_ = 3};
  for (const auto& candidate : candidates) {
    input.locations_.push_back(
        {.id_ = {candidate.id_, 0},
         .location_ =
             OrderedLocation(candidate.id_, 3, candidate.item_count_, type),
         .extents_ = nullptr,
         .retired_ = candidate.retired_});
  }
  return input;
}

TEST(GroupedOrderedObjectTest,
     PhysicalIndexPreservesViewsWhenNewIdsDivergeBeforeSharedPrefix) {
  // More than one physical leaf, with a long common identity prefix. The
  // synthetic chains cover insertions on both sides of that old prefix;
  // page ids are opaque and their chain order is independent of numeric order.
  for (const std::uint64_t base : {0ULL, 1ULL << 40}) {
    constexpr std::uint32_t count = 130;
    constexpr auto type = ValueType::kList;
    OrderedCollectionRoot root{.kind_ = OrderedCollectionKind::kList,
                               .incarnation_ = 17,
                               .item_count_ = count,
                               .first_group_ = base + 1,
                               .last_group_ = base + count,
                               .next_group_id_ = base + count + 1,
                               .group_count_ = count,
                               .revision_ = 3};
    std::vector<RecoveredOrderedGroup> records;
    std::vector<HashGroupLocation> locations;
    for (std::uint64_t i = 1; i <= count; ++i) {
      records.push_back({.incarnation_ = 17,
                         .id_ = base + i,
                         .previous_ = i == 1 ? 0 : base + i - 1,
                         .next_ = i == count ? 0 : base + i + 1,
                         .sequence_ = 3,
                         .lsn_ = i,
                         .item_count_ = 1,
                         .record_token_ = i});
      locations.push_back(
          {.id_ = {base + i, 0}, .location_ = OrderedLocation(i, 3, 1, type)});
    }
    auto directory = OrderedGroupDirectory::Recover(root, 3, records, {}, 7);
    ASSERT_TRUE(directory.ok()) << directory.status();
    GroupedObjectVersion version{
        .root_ = OrderedLocation(999, 7, count, type, true),
        .db_epoch_ = 1,
        .replication_epoch_ = 2,
        .index_generation_ = 3};
    auto old = GroupedHashObject::CreateOrdered(version, *directory, locations);
    ASSERT_TRUE(old.ok()) << old.status();
    const std::uint64_t added_id = base == 0 ? 1ULL << 40 : 1;
    auto tail = records.back();
    tail.next_ = added_id;
    tail.sequence_ = 4;
    tail.lsn_ = 200;
    tail.record_token_ = 200;
    const std::array<RecoveredOrderedGroup, 2> changed{
        tail, RecoveredOrderedGroup{.incarnation_ = 17,
                                    .id_ = added_id,
                                    .previous_ = root.last_group_,
                                    .sequence_ = 4,
                                    .lsn_ = 201,
                                    .item_count_ = 1,
                                    .record_token_ = 201}};
    root.last_group_ = added_id;
    root.next_group_id_ = std::max(root.next_group_id_, added_id + 1);
    ++root.group_count_;
    ++root.item_count_;
    root.revision_ = 4;
    auto next_directory = directory->Apply(root, 4, changed, 8);
    ASSERT_TRUE(next_directory.ok()) << next_directory.status();
    const std::array<HashGroupLocation, 2> replacements{
        HashGroupLocation{.id_ = {base + count, 0},
                          .location_ = OrderedLocation(200, 4, 1, type)},
        HashGroupLocation{.id_ = {added_id, 0},
                          .location_ = OrderedLocation(201, 4, 1, type)}};
    version.root_ = OrderedLocation(1000, 8, count + 1, type, true);
    auto next = GroupedHashObject::PrepareUpdateOrdered(
        *old, version, *next_directory, replacements);
    ASSERT_TRUE(next.ok()) << next.status();
    for (std::uint64_t i = 1; i <= count; ++i) {
      SCOPED_TRACE(i);
      ASSERT_NE((*old)->FindRecord({base + i, 0}), nullptr);
      EXPECT_EQ((*old)->FindRecord({base + i, 0})->value_.block_id(), i);
      ASSERT_NE((*next)->FindRecord({base + i, 0}), nullptr);
      EXPECT_EQ((*next)->FindRecord({base + i, 0})->value_.block_id(),
                i == count ? 200 : i);
    }
    EXPECT_EQ((*old)->FindRecord({added_id, 0}), nullptr);
    ASSERT_NE((*next)->FindRecord({added_id, 0}), nullptr);
    EXPECT_EQ((*next)->FindRecord({added_id, 0})->value_.block_id(), 201);
    EXPECT_EQ((*next)->FindRecord({added_id, 1}), nullptr);
    EXPECT_EQ((*next)->FindRecord({base + count + 1000, 0}), nullptr);
    // Relocation after prefix expansion must update only the new view, and
    // traversal must still visit each old and newly inserted identity once.
    auto moved = GroupedHashObject::RelocateGroup(
        *next, {base + 65, 0}, locations[64].location_,
        OrderedLocation(300, 3, 1, type));
    ASSERT_TRUE(moved.ok()) << moved.status();
    EXPECT_EQ((*moved)->FindRecord({base + 65, 0})->value_.block_id(), 300);
    EXPECT_EQ((*next)->FindRecord({base + 65, 0})->value_.block_id(), 65);
    std::set<HashGroupId> visited;
    (*moved)->ForEachRecord(
        [&](HashGroupId id, const auto&, const auto&, bool retired) {
          EXPECT_FALSE(retired);
          EXPECT_TRUE(visited.insert(id).second);
        });
    EXPECT_EQ(visited.size(), count + 1);
  }
}

TEST(GroupedOrderedObjectTest, StringVectorSharesUntouchedPagesAndOldViews) {
  constexpr std::uint32_t count = 130;
  constexpr auto type = ValueType::kString;
  OrderedCollectionRoot root{.kind_ = OrderedCollectionKind::kString,
                             .incarnation_ = 17,
                             .item_count_ = count * kStringGroupBytes,
                             .first_group_ = 1,
                             .last_group_ = count,
                             .next_group_id_ = count + 1,
                             .group_count_ = count,
                             .revision_ = 3};
  std::vector<RecoveredOrderedGroup> candidates;
  std::vector<HashGroupLocation> locations;
  for (std::uint64_t id = 1; id <= count; ++id) {
    candidates.push_back({.incarnation_ = 17,
                          .id_ = id,
                          .previous_ = id - 1,
                          .next_ = id == count ? 0 : id + 1,
                          .sequence_ = 3,
                          .lsn_ = id,
                          .item_count_ = kStringGroupBytes,
                          .record_token_ = id});
    locations.push_back(
        {.id_ = {id, 0},
         .location_ = OrderedLocation(id, 3, kStringGroupBytes, type),
         .extents_ = nullptr});
  }
  auto directory = OrderedGroupDirectory::Recover(root, 3, candidates, {}, 7);
  ASSERT_TRUE(directory.ok()) << directory.status();
  GroupedObjectVersion version{
      .root_ = OrderedLocation(999, 7, root.item_count_, type, true),
      .db_epoch_ = 1,
      .replication_epoch_ = 2,
      .index_generation_ = 3};
  auto old = GroupedHashObject::CreateOrdered(version, *directory, locations);
  ASSERT_TRUE(old.ok()) << old.status();
  root.revision_ = 4;
  auto changed = candidates[64];
  changed.sequence_ = changed.lsn_ = 4;
  auto next_directory = directory->Apply(root, 4, std::span(&changed, 1), 8);
  ASSERT_TRUE(next_directory.ok()) << next_directory.status();
  version.root_ = OrderedLocation(1000, 8, root.item_count_, type, true);
  HashGroupLocation replacement{
      .id_ = {65, 0},
      .location_ = OrderedLocation(1001, 4, kStringGroupBytes, type),
      .extents_ = nullptr};
  auto next = GroupedHashObject::PrepareUpdateOrdered(
      *old, version, *next_directory, std::span(&replacement, 1));
  ASSERT_TRUE(next.ok()) << next.status();
  EXPECT_EQ((*old)->FindRecord({65, 0})->value_.block_id(), 65);
  EXPECT_EQ((*next)->FindRecord({65, 0})->value_.block_id(), 1001);
  for (const auto id : {1, 64, 129, 130})
    EXPECT_EQ((*old)->FindRecord({static_cast<std::uint64_t>(id), 0}),
              (*next)->FindRecord({static_cast<std::uint64_t>(id), 0}));
  EXPECT_EQ((*next)->FindRecord({0, 0}), nullptr);
  EXPECT_EQ((*next)->FindRecord({131, 0}), nullptr);
  EXPECT_EQ((*next)->FindRecord({65, 1}), nullptr);
  auto relocated = GroupedHashObject::RelocateGroup(
      *next, {65, 0}, replacement.location_,
      OrderedLocation(1002, 4, kStringGroupBytes, type));
  ASSERT_TRUE(relocated.ok()) << relocated.status();
  EXPECT_EQ((*next)->FindRecord({65, 0})->value_.block_id(), 1001);
  EXPECT_EQ((*relocated)->FindRecord({65, 0})->value_.block_id(), 1002);
  std::size_t visited = 0;
  (*relocated)
      ->ForEachRecord([&](HashGroupId id, const RecordIndex::Entry& entry,
                          const auto& extents, bool retired) {
        EXPECT_EQ(&entry, (*relocated)->FindRecord(id));
        EXPECT_FALSE(extents);
        EXPECT_FALSE(retired);
        ++visited;
      });
  EXPECT_EQ(visited, count);
}

TEST(GroupedOrderedObjectTest, BothKindsRetainRetiredPhysicalRecordsAndRanks) {
  for (const auto type : {ValueType::kList, ValueType::kSortedSet}) {
    auto input = OrderedFixture(type);
    auto object = GroupedHashObject::CreateOrdered(
        input.version_, input.directory_, input.locations_);
    ASSERT_TRUE(object.ok()) << object.status();
    EXPECT_TRUE((*object)->is_ordered());
    EXPECT_EQ((*object)->incarnation(), 17);
    EXPECT_EQ((*object)->revision(), 3);
    EXPECT_EQ((*object)->command_sequence(), 7);
    EXPECT_EQ((*object)->group_count(), 2);
    EXPECT_EQ((*object)->record_count(), 3);
    EXPECT_NE((*object)->FindGroup(HashGroupId{1, 0}), nullptr);
    EXPECT_EQ((*object)->FindGroup(HashGroupId{2, 0}), nullptr);
    EXPECT_NE((*object)->FindRecord({2, 0}), nullptr);
    EXPECT_EQ((*object)->FindRecord({1, 1}), nullptr);
    EXPECT_EQ((*object)->FindGroup("member"), nullptr);
    const auto position = (*object)->ordered_directory().FindRank(2);
    ASSERT_TRUE(position.has_value());
    EXPECT_EQ(position->group_index_, 1);
    EXPECT_EQ(position->offset_, 0);
    std::size_t active = 0, retired = 0;
    (*object)->ForEachRecord([&](HashGroupId, const RecordIndex::Entry& entry,
                                 const auto&, bool marker) {
      EXPECT_EQ(entry.value_.value_type(), type);
      marker ? ++retired : ++active;
    });
    EXPECT_EQ(active, 2);
    EXPECT_EQ(retired, 1);
  }
}

TEST(GroupedOrderedObjectTest, MemberIndexSharesPhysicalLifecycleAndOldViews) {
  auto input = OrderedFixture(ValueType::kSortedSet);
  auto root = input.directory_.root();
  root.member_index_ = GroupedHashRoot{
      .incarnation_ = 17, .field_count_ = 4, .group_count_ = 1, .revision_ = 3};
  RecoveredHashGroup member{.incarnation_ = 17,
                            .id_ = {0, 0},
                            .sequence_ = 3,
                            .lsn_ = 3,
                            .field_count_ = 4};
  auto members = HashGroupDirectory::Recover(*root.member_index_, 7,
                                             std::span(&member, 1), {});
  ASSERT_TRUE(members.ok()) << members.status();
  std::vector<RecoveredOrderedGroup> pages = input.directory_.groups();
  for (const auto& page : input.directory_.retired_groups())
    pages.push_back(page);
  EXPECT_FALSE(OrderedGroupDirectory::Recover(root, 3, pages, {}, 7).ok());
  auto directory =
      OrderedGroupDirectory::Recover(root, 3, pages, {}, 7, *members);
  ASSERT_TRUE(directory.ok()) << directory.status();
  EXPECT_FALSE(GroupedHashObject::CreateOrdered(input.version_, *directory,
                                                input.locations_)
                   .ok());
  input.locations_.push_back(
      {.id_ = {0, 0},
       .location_ = OrderedLocation(100, 3, 4, ValueType::kSortedSet),
       .extents_ = nullptr});
  auto object = GroupedHashObject::CreateOrdered(input.version_, *directory,
                                                 input.locations_);
  ASSERT_TRUE(object.ok()) << object.status();
  EXPECT_TRUE((*object)->has_member_index());
  EXPECT_EQ((*object)->group_count(), 2);
  EXPECT_EQ((*object)->record_count(), 4);
  EXPECT_NE((*object)->FindGroup("arbitrary member"), nullptr);
  EXPECT_NE((*object)->FindGroup(HashGroupId{0, 0}), nullptr);
  EXPECT_NE((*object)->FindGroup(HashGroupId{1, 0}), nullptr);

  auto relocated = GroupedHashObject::RelocateGroup(
      *object, {0, 0}, input.locations_.back().location_,
      OrderedLocation(101, 3, 4, ValueType::kSortedSet));
  ASSERT_TRUE(relocated.ok()) << relocated.status();
  EXPECT_EQ((*object)->FindGroup(HashGroupId{0, 0})->value_.block_id(), 100);
  EXPECT_EQ((*relocated)->FindGroup(HashGroupId{0, 0})->value_.block_id(), 101);
  EXPECT_TRUE((*relocated)->SameLogicalRoot(**object));

  root.revision_ = 4;
  root.member_index_->revision_ = 4;
  member.sequence_ = member.lsn_ = 4;
  auto updated_directory =
      directory->Apply(root, 4, {}, 8, std::span(&member, 1));
  ASSERT_TRUE(updated_directory.ok()) << updated_directory.status();
  auto version = input.version_;
  version.root_ = OrderedLocation(1000, 8, 4, ValueType::kSortedSet, true);
  HashGroupLocation replacement{
      .id_ = {0, 0},
      .location_ = OrderedLocation(102, 4, 4, ValueType::kSortedSet),
      .extents_ = nullptr};
  auto updated = GroupedHashObject::PrepareUpdateOrdered(
      *relocated, version, *updated_directory, std::span(&replacement, 1));
  ASSERT_TRUE(updated.ok()) << updated.status();
  EXPECT_EQ((*updated)->FindGroup(HashGroupId{0, 0})->value_.block_id(), 102);
  EXPECT_EQ((*updated)->FindGroup(HashGroupId{1, 0})->value_.block_id(), 1);
  EXPECT_EQ((*relocated)->directory().root().revision_, 3);
  EXPECT_EQ((*updated)->directory().root().revision_, 4);
  EXPECT_FALSE(GroupedHashObject::PrepareUpdateOrdered(*relocated, version,
                                                       *updated_directory, {})
                   .ok());
}

TEST(GroupedOrderedObjectTest, UpdatesSameCommandRevisionAndPreservesOldView) {
  auto input = OrderedFixture();
  auto old = GroupedHashObject::CreateOrdered(input.version_, input.directory_,
                                              input.locations_);
  ASSERT_TRUE(old.ok()) << old.status();
  auto root = input.directory_.root();
  root.revision_ = 4;
  root.item_count_ = 5;
  auto changed = *input.directory_.Find(3);
  changed.sequence_ = 4;
  changed.lsn_ = 4;
  changed.item_count_ = 3;
  auto directory = input.directory_.Apply(root, 4, std::span(&changed, 1), 7);
  ASSERT_TRUE(directory.ok()) << directory.status();
  EXPECT_EQ(directory->retired_groups().size(), 1);
  auto version = input.version_;
  version.root_ = OrderedLocation(999, 7, 5, ValueType::kList, true);
  HashGroupLocation physical{
      .id_ = {3, 0},
      .location_ = OrderedLocation(30, 4, 3, ValueType::kList),
      .extents_ = nullptr};
  auto next = GroupedHashObject::PrepareUpdateOrdered(*old, version, *directory,
                                                      std::span(&physical, 1));
  ASSERT_TRUE(next.ok()) << next.status();
  version.root_ = OrderedLocation(1000, 7, 5, ValueType::kList, true);
  EXPECT_TRUE(GroupedHashObject::FinalizeRoot(*next, version).ok());
  EXPECT_EQ((*old)->FindRecord({3, 0})->value_.block_id(), 3);
  EXPECT_EQ((*next)->FindRecord({3, 0})->value_.block_id(), 30);
  EXPECT_EQ((*next)->FindRecord({1, 0})->value_.block_id(), 1);
  EXPECT_FALSE((*old)->SameLogicalRoot(**next));

  auto moved_version = version;
  moved_version.root_ = OrderedLocation(1001, 7, 5, ValueType::kList, true);
  auto moved = GroupedHashObject::RelocateRoot(*next, moved_version);
  ASSERT_TRUE(moved.ok()) << moved.status();
  EXPECT_TRUE((*moved)->SameLogicalRoot(**next));
  const auto marker = input.locations_[1].location_;
  auto relocated = GroupedHashObject::RelocateGroup(
      *moved, {2, 0}, marker, OrderedLocation(200, 3, 0, ValueType::kList));
  ASSERT_TRUE(relocated.ok()) << relocated.status();
  EXPECT_EQ((*relocated)->FindRecord({2, 0})->value_.block_id(), 200);
  EXPECT_EQ((*relocated)->FindGroup(HashGroupId{2, 0}), nullptr);
}

TEST(GroupedOrderedObjectTest,
     MetadataOnlyUpdateSharesDirectoryAndPhysicalPages) {
  for (const auto type : {ValueType::kList, ValueType::kSortedSet}) {
    auto input = OrderedFixture(type);
    auto old = GroupedHashObject::CreateOrdered(
        input.version_, input.directory_, input.locations_);
    ASSERT_TRUE(old.ok()) << old.status();
    auto version = input.version_;
    version.root_ = OrderedLocation(1000, 8, 4, type, true, 123456);
    auto updated = GroupedHashObject::PrepareMetadataUpdate(*old, version);
    ASSERT_TRUE(updated.ok()) << updated.status();
    EXPECT_TRUE(GroupedHashObject::FinalizeRoot(*updated, version).ok());
    EXPECT_EQ(&(*old)->ordered_directory(), &(*updated)->ordered_directory());
    EXPECT_EQ((*old)->FindRecord({1, 0}), (*updated)->FindRecord({1, 0}));
    EXPECT_EQ((*old)->FindRecord({2, 0}), (*updated)->FindRecord({2, 0}));
    EXPECT_EQ((*updated)->command_sequence(), 8);
    EXPECT_EQ((*updated)->ordered_directory().command_sequence(), 7);
    EXPECT_EQ((*updated)->revision(), 3);
    EXPECT_EQ((*updated)->version().root_.expire_at_ms_, 123456);
    EXPECT_EQ((*old)->version().root_.expire_at_ms_, 0);
    EXPECT_TRUE((*old)->SameLogicalRoot(**updated));
    auto moved = version;
    moved.root_ = OrderedLocation(1001, 8, 4, type, true, 123456);
    EXPECT_TRUE(GroupedHashObject::RelocateRoot(*updated, moved).ok());
    auto stale = version;
    stale.root_ = OrderedLocation(1002, 7, 4, type, true);
    EXPECT_FALSE(
        GroupedHashObject::PrepareMetadataUpdate(*updated, stale).ok());
  }
}

TEST(GroupedOrderedObjectTest, InvalidTypeMissingMarkerAndOomCannotPublish) {
  auto input = OrderedFixture();
  auto incomplete = input.locations_;
  incomplete.erase(incomplete.begin() + 1);
  EXPECT_FALSE(GroupedHashObject::CreateOrdered(input.version_,
                                                input.directory_, incomplete)
                   .ok());
  auto wrong = input.version_;
  wrong.root_ = OrderedLocation(999, 7, 4, ValueType::kHash, true);
  EXPECT_FALSE(GroupedHashObject::CreateOrdered(wrong, input.directory_,
                                                input.locations_)
                   .ok());
  auto old = GroupedHashObject::CreateOrdered(input.version_, input.directory_,
                                              input.locations_);
  ASSERT_TRUE(old.ok()) << old.status();
  struct ResetMemory {
    ~ResetMemory() { (void)InitMemoryLimit(1024ULL * 1024 * 1024, 1); }
  } reset;
  ASSERT_TRUE(InitMemoryLimit(1, 1).ok());
  auto oom = GroupedHashObject::PrepareRootRelocation(*old);
  EXPECT_EQ(oom.status().code(), absl::StatusCode::kResourceExhausted);
  EXPECT_EQ((*old)->record_count(), 3);
  EXPECT_EQ((*old)->FindRecord({3, 0})->value_.block_id(), 3);
}

}  // namespace
}  // namespace lavik::storage
