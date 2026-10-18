/*
 * Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <limits>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "lavik/storage/detail/grouped/collection.h"
#include "lavik/storage/engine.h"

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

TEST(PackedListPageTest, ValidatesSkippedEntriesAndEnvelope) {
  auto page = ListPage({"item-0", "item-1", "item-2"});
  auto encoded = EncodeOrderedGroup(page);
  ASSERT_TRUE(encoded.ok());
  for (const auto index : {0, 2}) {
    const auto offset = kOrderedGroupHeaderBytes + index * (12 + 6);
    auto bad = *encoded;
    bad[offset + 11] = '\x80';  // Even skipped -0 scores are invalid.
    EXPECT_TRUE(
        absl::IsDataLoss(DecodePackedOrderedListRange(bad, 1, 1).status()));
    bad = *encoded;
    for (int byte = 0; byte < 4; ++byte) bad[offset + byte] = '\xff';
    EXPECT_TRUE(
        absl::IsDataLoss(DecodePackedOrderedListRange(bad, 1, 1).status()));
  }
  auto bad = *encoded;
  bad[48] = 2;
  EXPECT_TRUE(
      absl::IsDataLoss(DecodePackedOrderedListRange(bad, 0, 1).status()));
  bad = *encoded;
  bad[56] = 1;
  EXPECT_TRUE(
      absl::IsDataLoss(DecodePackedOrderedListRange(bad, 0, 1).status()));
  for (std::size_t length = 0; length < encoded->size(); ++length)
    EXPECT_FALSE(
        DecodePackedOrderedListRange(encoded->substr(0, length), 0, 0).ok());
  EXPECT_TRUE(absl::IsOutOfRange(
      DecodePackedOrderedListRange(*encoded, 4, 0).status()));
  EXPECT_TRUE(absl::IsOutOfRange(
      DecodePackedOrderedListRange(*encoded, 1, SIZE_MAX).status()));
  page.entries_.clear();
  page.retired_ = true;
  auto retired = EncodeOrderedGroup(page);
  ASSERT_TRUE(retired.ok());
  EXPECT_TRUE(
      absl::IsDataLoss(DecodePackedOrderedListRange(*retired, 0, 0).status()));
}

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

}  // namespace
}  // namespace lavik::storage
