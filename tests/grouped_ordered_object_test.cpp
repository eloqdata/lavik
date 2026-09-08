#include "gtest/gtest.h"
#include "keylane/storage/detail/grouped_object_index.h"

namespace keylane::storage {
namespace {

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
}  // namespace keylane::storage
