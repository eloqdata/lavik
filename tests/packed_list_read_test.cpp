/*
 * Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <limits>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "lavik/storage/detail/grouped/collection.h"
#include "lavik/storage/detail/ordered_compact_codec.h"
#include "lavik/storage/engine.h"
#include "support/ordered_v1.h"

namespace lavik::storage {
namespace {

std::string CompactList(const std::vector<std::string>& values) {
  std::string bytes = "LVL1";
  auto append_u32 = [&](std::uint32_t value) {
    for (unsigned byte = 0; byte < 4; ++byte)
      bytes.push_back(static_cast<char>(value >> (byte * 8)));
  };
  append_u32(values.size());
  for (const auto& value : values) {
    append_u32(value.size());
    bytes += value;
  }
  return bytes;
}

CompactReadResult CompactView(const std::string& bytes, std::size_t count) {
  return std::optional<CompactValueView>{
      {.encoded_ = bytes, .logical_size_ = count}};
}

OrderedGroupSnapshot ListPage(const std::vector<std::string>& values) {
  OrderedGroupSnapshot page{
      .kind_ = OrderedCollectionKind::kList, .incarnation_ = 17, .id_ = 1};
  for (const auto& value : values) page.entries_.push_back({.value_ = value});
  return page;
}

TEST(PackedListPageTest, ProjectsEveryIntervalWithoutBorrowingTheSource) {
  const std::vector<std::string> items{"", std::string("a\0b", 3),
                                       std::string(8192, 'x'), "tail"};
  auto encoded = EncodeOrderedGroup(ListPage(items));
  ASSERT_TRUE(encoded.ok());
  for (std::size_t first = 0; first <= items.size(); ++first) {
    for (std::size_t count = 0; count <= items.size() - first; ++count) {
      auto source = *encoded;
      auto result = DecodePackedOrderedListRange(source, first, count);
      ASSERT_TRUE(result.ok()) << result.status();
      EXPECT_EQ(result->blocks_.size(), count == 0 ? 0 : 1);
      source.assign(source.size(), '\xff');
      auto moved = std::move(*result);
      ASSERT_EQ(moved.values_.size(), count);
      for (std::size_t i = 0; i < count; ++i)
        EXPECT_EQ(moved.view(i), items[first + i]);
    }
  }
  auto selected = DecodePackedOrderedListRange(*encoded, 1, 1);
  ASSERT_TRUE(selected.ok());
  ASSERT_EQ(selected->blocks_.size(), 1);
  EXPECT_EQ(selected->blocks_[0].size(), 3);
}

class ListPageCorruptionTest : public testing::TestWithParam<bool> {
 protected:
  absl::Status Decode(std::string_view bytes, std::size_t first,
                      std::size_t count) {
    return GetParam()
               ? DecodePackedOrderedListRange(bytes, first, count).status()
               : DecodeOrderedListRange(bytes, first, count).status();
  }
};

TEST_P(ListPageCorruptionTest, V1ValidatesSkippedEntriesAndEnvelope) {
  auto page = ListPage({"item-0", "item-1", "item-2"});
  auto encoded = test::EncodeOrderedGroupV1(page);
  ASSERT_TRUE(encoded.ok());
  for (const auto index : {0, 2}) {
    const auto offset = kOrderedGroupHeaderBytes + index * (12 + 6);
    auto bad = *encoded;
    bad[offset + 11] = '\x80';  // Even skipped -0 scores are invalid.
    EXPECT_TRUE(absl::IsDataLoss(Decode(bad, 1, 1)));
    bad = *encoded;
    for (int byte = 0; byte < 4; ++byte) bad[offset + byte] = '\xff';
    EXPECT_TRUE(absl::IsDataLoss(Decode(bad, 1, 1)));
  }
  auto bad = *encoded;
  bad[48] = 2;  // A smaller count cannot leave trailing entries.
  EXPECT_TRUE(absl::IsDataLoss(Decode(bad, 0, 1)));
  bad = *encoded;
  bad[56] = 1;  // Reserved envelope bytes remain checked.
  EXPECT_TRUE(absl::IsDataLoss(Decode(bad, 0, 1)));
  for (std::size_t length = 0; length < encoded->size(); ++length)
    EXPECT_FALSE(Decode(encoded->substr(0, length), 0, 0).ok());
  EXPECT_TRUE(absl::IsOutOfRange(Decode(*encoded, 4, 0)));
  EXPECT_TRUE(absl::IsOutOfRange(Decode(*encoded, 1, SIZE_MAX)));
  auto sorted = page;
  sorted.kind_ = OrderedCollectionKind::kSortedSet;
  auto sorted_bytes = test::EncodeOrderedGroupV1(sorted);
  ASSERT_TRUE(sorted_bytes.ok());
  EXPECT_TRUE(absl::IsDataLoss(Decode(*sorted_bytes, 0, 1)));
  page.entries_.clear();
  page.retired_ = true;
  auto retired = test::EncodeOrderedGroupV1(page);
  ASSERT_TRUE(retired.ok());
  EXPECT_TRUE(absl::IsDataLoss(Decode(*retired, 0, 0)));
}

INSTANTIATE_TEST_SUITE_P(OwnedAndPacked, ListPageCorruptionTest,
                         testing::Bool(),
                         [](const testing::TestParamInfo<bool>& info) {
                           return info.param ? "Packed" : "Owned";
                         });

class PackedCompactListTest : public testing::Test {
 protected:
  void SetUp() override {
    previous_shard_ = CurrentMemoryAccountingShard();
    ASSERT_TRUE(InitMemoryLimit(1024ULL * 1024 * 1024, 1).ok());
    BindMemoryAccountingShard(0);
  }
  void TearDown() override {
    EXPECT_TRUE(InitMemoryLimit(1024ULL * 1024 * 1024, 1).ok());
    BindMemoryAccountingShard(previous_shard_ == 0 ? kMaxMemoryWorkers
                                                   : previous_shard_ - 1);
  }
  unsigned previous_shard_ = 0;
};

TEST_F(PackedCompactListTest, MatchesOwnedRangesAndRetainsOnlySelectedBytes) {
  const std::vector<std::string> items{"", std::string("a\0b", 3),
                                       std::string(4096, 'x'), "tail"};
  const auto encoded = CompactList(items);
  const std::vector<std::int64_t> bounds{
      std::numeric_limits<std::int64_t>::min(), -6, -4, -2, -1, 0, 1, 3, 4, 100,
      std::numeric_limits<std::int64_t>::max()};
  const auto before = GetWorkerMemoryStats(0);
  for (const auto first : bounds) {
    for (const auto last : bounds) {
      SCOPED_TRACE(testing::Message() << first << ',' << last);
      {
        const ListOperation owned{.kind_ = ListOperationKind::kRange,
                                  .first_ = first,
                                  .second_ = last};
        auto expected =
            ReadCompactList(CompactView(encoded, items.size()), owned);
        ASSERT_TRUE(expected.ok());
        auto source = encoded;
        auto operation = owned;
        operation.packed_output_ = true;
        auto result =
            ReadCompactList(CompactView(source, items.size()), operation);
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE(result->key_exists_);
        EXPECT_EQ(result->length_, items.size());
        EXPECT_TRUE(result->values_.empty());
        source.assign(source.size(), '\xff');
        auto moved = std::move(*result);
        ASSERT_EQ(moved.value_count(), expected->values_.size());
        for (std::size_t i = 0; i < moved.value_count(); ++i)
          EXPECT_EQ(moved.value_view(i), expected->values_[i]);
        EXPECT_EQ(GetWorkerMemoryStats(0).retained_bytes_,
                  before.retained_bytes_ + moved.retained_charge_.bytes());
        EXPECT_EQ(GetWorkerMemoryStats(0).admission_pending_bytes_,
                  before.admission_pending_bytes_);
      }
      EXPECT_EQ(GetWorkerMemoryStats(0).retained_bytes_,
                before.retained_bytes_);
    }
  }
  const ListOperation one{.kind_ = ListOperationKind::kRange,
                          .first_ = 1,
                          .second_ = 1,
                          .packed_output_ = true};
  auto result = ReadCompactList(CompactView(encoded, items.size()), one);
  ASSERT_TRUE(result.ok());
  ASSERT_NE(result->packed_values_, nullptr);
  ASSERT_EQ(result->packed_values_->blocks_.size(), 1);
  EXPECT_EQ(result->packed_values_->blocks_[0].size(), 3);
}

TEST_F(PackedCompactListTest, MissingAndEmptyReplyHaveNoPayloadCharge) {
  const ListOperation operation{.kind_ = ListOperationKind::kRange,
                                .first_ = 0,
                                .second_ = -1,
                                .packed_output_ = true};
  const auto before = GetWorkerMemoryStats(0);
  auto missing = ReadCompactList(std::optional<CompactValueView>{}, operation);
  ASSERT_TRUE(missing.ok());
  EXPECT_FALSE(missing->key_exists_);
  EXPECT_EQ(missing->value_count(), 0);
  EXPECT_EQ(missing->retained_charge_.bytes(), 0);
  const auto bytes = CompactList({});
  auto empty = ReadCompactList(CompactView(bytes, 0), operation);
  ASSERT_TRUE(empty.ok());
  EXPECT_TRUE(empty->key_exists_);
  EXPECT_EQ(empty->value_count(), 0);
  EXPECT_EQ(GetWorkerMemoryStats(0).retained_bytes_, before.retained_bytes_);
}

TEST_F(PackedCompactListTest, RejectsCorruptionOutsideSelectedRange) {
  const auto encoded = CompactList({"item-0", "item-1", "item-2"});
  const ListOperation operation{.kind_ = ListOperationKind::kRange,
                                .first_ = 1,
                                .second_ = 1,
                                .packed_output_ = true};
  const auto before = GetWorkerMemoryStats(0);
  for (const auto index : {0, 2}) {
    auto bad = encoded;
    for (int byte = 0; byte < 4; ++byte) bad[8 + index * 10 + byte] = '\xff';
    EXPECT_FALSE(ReadCompactList(CompactView(bad, 3), operation).ok());
  }
  auto trailing = encoded + "x";
  EXPECT_FALSE(ReadCompactList(CompactView(trailing, 3), operation).ok());
  EXPECT_FALSE(ReadCompactList(CompactView(encoded, 4), operation).ok());
  for (std::size_t length = 0; length < encoded.size(); ++length) {
    auto truncated = encoded.substr(0, length);
    EXPECT_FALSE(ReadCompactList(CompactView(truncated, 3), operation).ok());
  }
  EXPECT_EQ(GetWorkerMemoryStats(0).retained_bytes_, before.retained_bytes_);
  EXPECT_EQ(GetWorkerMemoryStats(0).admission_pending_bytes_,
            before.admission_pending_bytes_);
}

TEST_F(PackedCompactListTest, FlagDoesNotChangeMutableConsumerRepresentation) {
  const auto encoded = CompactList({"first", "second"});
  const ListOperation operation{
      .kind_ = ListOperationKind::kIndex, .first_ = 1, .packed_output_ = true};
  auto result = ReadCompactList(CompactView(encoded, 2), operation);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result->packed_values_, nullptr);
  EXPECT_EQ(result->values_, std::vector<std::string>{"second"});
}

TEST_F(PackedCompactListTest, IndexedRangesAndIndexesMatchV1) {
  const std::vector<std::string> items{"", std::string("a\0b", 3),
                                       std::string(8192, 'x'), "tail"};
  std::vector<OrderedCollectionEntry> entries;
  for (const auto& item : items) entries.push_back({item, 0});
  auto indexed =
      EncodeOrderedCompactValue(OrderedCollectionKind::kList, entries, 2);
  ASSERT_TRUE(indexed.ok());
  const auto legacy = CompactList(items);
  const std::vector<std::int64_t> bounds{INT64_MIN, -6, -4, -2,  -1,       0,
                                         1,         3,  4,  100, INT64_MAX};
  for (auto first : bounds) {
    for (auto last : bounds) {
      const ListOperation expected_op{
          .kind_ = ListOperationKind::kRange, .first_ = first, .second_ = last};
      auto expected =
          ReadCompactList(CompactView(legacy, items.size()), expected_op);
      ASSERT_TRUE(expected.ok());
      for (bool packed : {false, true}) {
        auto source = *indexed;
        auto op = expected_op;
        op.packed_output_ = packed;
        auto result = ReadCompactList(CompactView(source, items.size()), op);
        ASSERT_TRUE(result.ok()) << result.status();
        source.assign(source.size(), '\xff');
        auto moved = std::move(*result);
        ASSERT_EQ(moved.value_count(), expected->value_count());
        for (std::size_t i = 0; i < moved.value_count(); ++i)
          EXPECT_EQ(moved.value_view(i), expected->value_view(i));
      }
    }
    const ListOperation op{.kind_ = ListOperationKind::kIndex, .first_ = first};
    auto expected = ReadCompactList(CompactView(legacy, items.size()), op);
    auto result = ReadCompactList(CompactView(*indexed, items.size()), op);
    ASSERT_TRUE(expected.ok());
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(result->values_, expected->values_);
  }
}

TEST_F(PackedCompactListTest,
       IndexedProductionReadsDoNotDecodeUnselectedEntries) {
  std::vector<OrderedCollectionEntry> entries;
  for (std::size_t i = 0; i < 128; ++i)
    entries.push_back({std::to_string(i), 0});
  auto bytes =
      EncodeOrderedCompactValue(OrderedCollectionKind::kList, entries, 2);
  ASSERT_TRUE(bytes.ok());
  const auto offset = bytes->size() - (entries.size() + 1) * 4 + 9 * 4;
  for (unsigned i = 0; i < 4; ++i) (*bytes)[offset + i] = '\xff';
  EXPECT_FALSE(DecodeOrderedCompactValue(OrderedCollectionKind::kList, *bytes,
                                         entries.size())
                   .ok());
  auto result = ReadCompactList(
      CompactView(*bytes, entries.size()),
      ListOperation{.kind_ = ListOperationKind::kIndex, .first_ = -1});
  ASSERT_TRUE(result.ok()) << result.status();
  ASSERT_EQ(result->values_.size(), 1);
  EXPECT_EQ(result->values_.front(), "127");
  EXPECT_FALSE(ReadCompactList(CompactView(*bytes, entries.size()),
                               ListOperation{.kind_ = ListOperationKind::kIndex,
                                             .first_ = 8})
                   .ok());
  auto range = ReadCompactList(CompactView(*bytes, entries.size()),
                               ListOperation{.kind_ = ListOperationKind::kRange,
                                             .first_ = 100,
                                             .second_ = 102,
                                             .packed_output_ = true});
  ASSERT_TRUE(range.ok());
  ASSERT_EQ(range->value_count(), 3);
  bytes->assign(bytes->size(), '\xff');
  for (std::size_t i = 0; i < 3; ++i)
    EXPECT_EQ(range->value_view(i), std::to_string(100 + i));
}

TEST_F(PackedCompactListTest,
       IndexedSortedSetQueriesMatchV1BoundsLimitsAndReverse) {
  const auto infinity = std::numeric_limits<double>::infinity();
  const std::vector<OrderedCollectionEntry> entries{
      {"lo", -infinity}, {"", -2}, {std::string("a\0", 2), -0.0},
      {"b", 0},          {"c", 0}, {"d", 1.25},
      {"hi", infinity}};
  auto legacy =
      EncodeOrderedCompactValue(OrderedCollectionKind::kSortedSet, entries);
  auto indexed =
      EncodeOrderedCompactValue(OrderedCollectionKind::kSortedSet, entries, 2);
  ASSERT_TRUE(legacy.ok());
  ASSERT_TRUE(indexed.ok());
  auto compare = [&](const SortedSetOperation& op) {
    auto expected =
        ReadCompactSortedSet(CompactView(*legacy, entries.size()), op);
    auto source = *indexed;
    auto result = ReadCompactSortedSet(CompactView(source, entries.size()), op);
    ASSERT_TRUE(expected.ok()) << expected.status();
    ASSERT_TRUE(result.ok()) << result.status();
    source.assign(source.size(), '\xff');
    EXPECT_EQ(result->count_, expected->count_);
    ASSERT_EQ(result->member_count(), expected->member_count());
    for (std::size_t i = 0; i < result->member_count(); ++i) {
      EXPECT_EQ(result->member_view(i).member_,
                expected->member_view(i).member_);
      EXPECT_EQ(result->member_view(i).score_, expected->member_view(i).score_);
    }
  };
  for (bool reverse : {false, true})
    for (bool packed : {false, true}) {
      for (std::int64_t first : {INT64_MIN, std::int64_t(-8), std::int64_t(-1),
                                 std::int64_t(0), std::int64_t(2), INT64_MAX})
        for (std::int64_t last :
             {std::int64_t(-1), std::int64_t(0), std::int64_t(3), INT64_MAX})
          compare(SortedSetOperation{.kind_ = SortedSetOperationKind::kRange,
                                     .reverse_ = reverse,
                                     .first_ = first,
                                     .last_ = last,
                                     .packed_output_ = packed});
      for (double low : {-infinity, -2.0, -0.0, 1.25, infinity})
        for (double high : {-2.0, 0.0, 1.25, infinity})
          for (bool exclusive : {false, true}) {
            SortedSetOperation op{.kind_ = SortedSetOperationKind::kRange,
                                  .range_mode_ = SortedSetRangeMode::kScore,
                                  .reverse_ = reverse,
                                  .minimum_score_ = {low, exclusive},
                                  .maximum_score_ = {high, !exclusive},
                                  .packed_output_ = packed};
            compare(op);
            op.limit_ = true;
            for (std::int64_t offset : {-1, 0, 2, 20})
              for (std::int64_t count : {-1, 0, 1, 3}) {
                op.offset_ = offset;
                op.count_ = count;
                compare(op);
              }
          }
    }
  for (double low : {-infinity, -2.0, -0.0, 1.25, infinity})
    for (double high : {-2.0, 0.0, 1.25, infinity})
      for (bool exclusive : {false, true})
        compare(SortedSetOperation{.kind_ = SortedSetOperationKind::kCount,
                                   .range_mode_ = SortedSetRangeMode::kScore,
                                   .minimum_score_ = {low, exclusive},
                                   .maximum_score_ = {high, !exclusive}});
  // One boundary pair is enough to check that reply flags do not affect count.
  for (const auto* bytes : {&*legacy, &*indexed}) {
    auto count = ReadCompactSortedSet(
        CompactView(*bytes, entries.size()),
        SortedSetOperation{.kind_ = SortedSetOperationKind::kCount,
                           .range_mode_ = SortedSetRangeMode::kScore,
                           .reverse_ = true,
                           .minimum_score_ = {0, false},
                           .maximum_score_ = {1.25, false},
                           .packed_output_ = true});
    ASSERT_TRUE(count.ok());
    EXPECT_EQ(count->count_, 4);
    EXPECT_EQ(count->member_count(), 0);
  }
}

TEST_F(PackedCompactListTest, IndexedSortedSetReplyOwnsOneSelectedBlock) {
  std::vector<OrderedCollectionEntry> entries;
  for (unsigned i = 0; i < 20; ++i)
    entries.push_back({std::string(512 + i, static_cast<char>(i)), double(i)});
  auto encoded =
      EncodeOrderedCompactValue(OrderedCollectionKind::kSortedSet, entries, 2);
  ASSERT_TRUE(encoded.ok());
  for (bool reverse : {false, true}) {
    for (const auto mode :
         {SortedSetRangeMode::kRank, SortedSetRangeMode::kScore}) {
      SortedSetOperation operation{.kind_ = SortedSetOperationKind::kRange,
                                   .range_mode_ = mode,
                                   .reverse_ = reverse,
                                   .first_ = 2,
                                   .last_ = 17,
                                   .packed_output_ = true};
      operation.minimum_score_.value_ = 2;
      operation.maximum_score_.value_ = 17;
      if (mode == SortedSetRangeMode::kScore) {
        operation.limit_ = true;
        operation.offset_ = 1;
        operation.count_ = 14;
      }
      const std::size_t first = mode == SortedSetRangeMode::kRank ? 2 : 3;
      const std::size_t end = mode == SortedSetRangeMode::kRank ? 18 : 17;
      auto source = *encoded;
      auto result =
          ReadCompactSortedSet(CompactView(source, entries.size()), operation);
      ASSERT_TRUE(result.ok()) << result.status();
      ASSERT_TRUE(result->packed_members_);
      const auto& packed = *result->packed_members_;
      ASSERT_EQ(packed.members_.blocks_.size(), 1);
      ASSERT_EQ(packed.members_.values_.size(), end - first);
      std::size_t bytes = 0;
      for (auto i = first; i < end; ++i) bytes += entries[i].value_.size();
      EXPECT_EQ(packed.members_.blocks_[0].size(), bytes);
      source.assign(source.size(), 'x');
      for (std::size_t i = 0; i < end - first; ++i) {
        const auto index = reverse ? end - 1 - i : first + i;
        EXPECT_EQ(result->member_view(i).member_, entries[index].value_);
        EXPECT_EQ(result->member_view(i).score_, entries[index].score_);
      }
    }
  }
}

}  // namespace
}  // namespace lavik::storage
