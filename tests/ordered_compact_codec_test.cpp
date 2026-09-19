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

#include "lavik/storage/detail/ordered_compact_codec.h"

#include <bit>
#include <limits>

#include "gtest/gtest.h"

namespace lavik::storage {
namespace {

TEST(OrderedCompactCodecTest, ListUsesExistingLogicalWireImage) {
  const std::vector<OrderedCollectionEntry> entries{
      {.value_ = "a"}, {.value_ = std::string("b\0c", 3)}};
  auto encoded =
      EncodeOrderedCompactValue(OrderedCollectionKind::kList, entries);
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  const std::string expected(
      "LVL1\x02\0\0\0\x01\0\0\0"
      "a\x03\0\0\0"
      "b\0c",
      20);
  EXPECT_EQ(*encoded, expected);
  auto decoded =
      DecodeOrderedCompactValue(OrderedCollectionKind::kList, *encoded, 2);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(*decoded, entries);
}

TEST(OrderedCompactCodecTest, SortedScoresRetainInfinityAndNegativeZero) {
  const std::vector<OrderedCollectionEntry> entries{
      {.value_ = "lo", .score_ = -std::numeric_limits<double>::infinity()},
      {.value_ = "zero", .score_ = -0.0},
      {.value_ = "hi", .score_ = std::numeric_limits<double>::infinity()}};
  auto encoded =
      EncodeOrderedCompactValue(OrderedCollectionKind::kSortedSet, entries);
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  EXPECT_TRUE(encoded->starts_with("LZS1"));
  auto decoded =
      DecodeOrderedCompactValue(OrderedCollectionKind::kSortedSet, *encoded, 3);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(*decoded, entries);
  EXPECT_EQ(std::bit_cast<std::uint64_t>((*decoded)[1].score_),
            std::bit_cast<std::uint64_t>(-0.0));
}

TEST(OrderedCompactCodecTest, BoundsAndFramingFailClosed) {
  const std::vector<OrderedCollectionEntry> entries{{.value_ = "abc"}};
  auto encoded =
      EncodeOrderedCompactValue(OrderedCollectionKind::kList, entries);
  ASSERT_TRUE(encoded.ok());
  EXPECT_EQ(encoded->size(), 15);
  for (std::size_t size = 0; size < encoded->size(); ++size) {
    EXPECT_FALSE(
        DecodeOrderedCompactValue(OrderedCollectionKind::kList,
                                  std::string_view(*encoded).substr(0, size), 1)
            .ok());
  }
  EXPECT_FALSE(
      DecodeOrderedCompactValue(OrderedCollectionKind::kList, *encoded, 2)
          .ok());
  EXPECT_FALSE(
      DecodeOrderedCompactValue(OrderedCollectionKind::kList, *encoded + "x", 1)
          .ok());
  EXPECT_FALSE(
      DecodeOrderedCompactValue(OrderedCollectionKind::kSortedSet, *encoded, 1)
          .ok());
}

TEST(OrderedCompactCodecTest, LargeItemAndInvalidScore) {
  std::vector<OrderedCollectionEntry> entries{
      {.value_ = std::string(9 * 1024 * 1024, 'x')}};
  auto encoded =
      EncodeOrderedCompactValue(OrderedCollectionKind::kList, entries);
  ASSERT_TRUE(encoded.ok());
  auto decoded =
      DecodeOrderedCompactValue(OrderedCollectionKind::kList, *encoded, 1);
  ASSERT_TRUE(decoded.ok());
  EXPECT_EQ(*decoded, entries);
  entries.front().score_ = std::numeric_limits<double>::quiet_NaN();
  EXPECT_FALSE(
      EncodeOrderedCompactValue(OrderedCollectionKind::kSortedSet, entries)
          .ok());
  entries.front().score_ = -0.0;
  EXPECT_FALSE(
      EncodeOrderedCompactValue(OrderedCollectionKind::kList, entries).ok());
}

TEST(OrderedCompactCodecTest, SizingSeparatesItemsFromAggregateCapacity) {
  const auto limit = std::string().max_size();
  for (const auto kind :
       {OrderedCollectionKind::kList, OrderedCollectionKind::kSortedSet}) {
    const std::size_t framing =
        kind == OrderedCollectionKind::kSortedSet ? 12 : 4;
    std::size_t bytes = 8;
    for (unsigned i = 0; i < 3; ++i) {
      auto next = AppendOrderedEntrySize(kind, bytes, kMaxStringBytes, limit);
      ASSERT_TRUE(next.ok()) << next.status();
      bytes = *next;
    }
    EXPECT_EQ(bytes, 8 + 3 * (framing + kMaxStringBytes));
    EXPECT_GT(bytes, kMaxRecordPayloadBytes);
    EXPECT_FALSE(
        AppendOrderedEntrySize(kind, 0, kMaxStringBytes + 1, limit).ok());
    EXPECT_FALSE(AppendOrderedEntrySize(kind, 0, 0, framing - 1).ok());
    EXPECT_FALSE(
        AppendOrderedEntrySize(kind, limit - framing + 1, 0, limit).ok());
    EXPECT_FALSE(AppendOrderedEntrySize(kind, limit - framing, 1, limit).ok());
    auto exact = AppendOrderedEntrySize(kind, limit - framing, 0, limit);
    ASSERT_TRUE(exact.ok());
    EXPECT_EQ(*exact, limit);
  }
}

void CheckLargeLogicalRoundTrip(OrderedCollectionKind kind) {
  // Boundary coverage retains only the source and encoding, then the
  // encoding and decoded items, keeping payload memory near 2 GiB per case.
  constexpr std::size_t kItemBytes = kMaxRecordPayloadBytes / 3 + 1;
  std::vector<OrderedCollectionEntry> entries;
  for (unsigned i = 0; i < 3; ++i) {
    entries.push_back({.value_ = std::string(kItemBytes, 'a' + i),
                       .score_ = kind == OrderedCollectionKind::kSortedSet
                                     ? static_cast<double>(i)
                                     : 0});
  }
  auto encoded = EncodeOrderedCompactValue(kind, entries);
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  ASSERT_GT(encoded->size(), kMaxRecordPayloadBytes);
  entries.clear();
  auto decoded = DecodeOrderedCompactValue(kind, *encoded, 3);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  ASSERT_EQ(decoded->size(), 3);
  for (std::size_t i = 0; i < decoded->size(); ++i) {
    const auto& entry = (*decoded)[i];
    EXPECT_EQ(entry.value_.size(), kItemBytes);
    EXPECT_EQ(entry.value_.find_first_not_of(static_cast<char>('a' + i)),
              std::string::npos);
    EXPECT_EQ(entry.score_, kind == OrderedCollectionKind::kSortedSet
                                ? static_cast<double>(i)
                                : 0);
  }
}

TEST(OrderedCompactCodecTest, LargeListRoundTripsBeyondRecordLimit) {
  CheckLargeLogicalRoundTrip(OrderedCollectionKind::kList);
}

TEST(OrderedCompactCodecTest, LargeSortedSetRoundTripsBeyondRecordLimit) {
  CheckLargeLogicalRoundTrip(OrderedCollectionKind::kSortedSet);
}

}  // namespace
}  // namespace lavik::storage
