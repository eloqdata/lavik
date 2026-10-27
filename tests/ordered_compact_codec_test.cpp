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

#include <algorithm>
#include <bit>
#include <limits>

#include "gtest/gtest.h"
#include "support/ordered_v1.h"

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

TEST(OrderedCompactCodecTest, BorrowedReaderChecksEveryTruncatedBoundary) {
  const std::vector<OrderedCollectionEntry> entries{
      {.value_ = "", .score_ = 0},
      {.value_ = std::string("a\0b", 3), .score_ = 0},
      {.value_ = std::string(32, 'm'), .score_ = 0}};
  for (auto kind :
       {OrderedCollectionKind::kList, OrderedCollectionKind::kSortedSet}) {
    auto bytes = EncodeOrderedCompactValue(kind, entries);
    ASSERT_TRUE(bytes.ok());
    for (std::size_t length = 0; length <= bytes->size(); ++length) {
      auto reader = OrderedCompactReader::Open(
          kind, std::string_view(*bytes).substr(0, length), entries.size());
      bool valid = reader.ok();
      if (valid) {
        for (std::size_t i = 0; i < entries.size(); ++i) {
          auto entry = reader->Next();
          if (!entry.ok()) {
            valid = false;
            break;
          }
          EXPECT_EQ(entry->value_, entries[i].value_);
          EXPECT_EQ(entry->score_, entries[i].score_);
        }
      }
      EXPECT_EQ(valid, length == bytes->size());
    }
    auto reader = OrderedCompactReader::Open(kind, *bytes, entries.size());
    ASSERT_TRUE(reader.ok());
    auto copied = *reader;
    for (std::size_t i = 0; i < entries.size(); ++i) {
      auto a = reader->Next();
      auto b = copied.Next();
      ASSERT_TRUE(a.ok());
      ASSERT_TRUE(b.ok());
      EXPECT_EQ(a->encoded_, b->encoded_);
    }
    EXPECT_FALSE(reader->Next().ok());
    const auto trailing = *bytes + "x";
    auto broken = OrderedCompactReader::Open(kind, trailing, entries.size());
    ASSERT_TRUE(broken.ok());
    ASSERT_TRUE(broken->Next().ok());
    ASSERT_TRUE(broken->Next().ok());
    EXPECT_FALSE(broken->Next().ok());
  }
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

void IndexPut(std::string& bytes, std::size_t offset, std::uint64_t value,
              unsigned width) {
  for (unsigned i = 0; i < width; ++i)
    bytes[offset + i] = static_cast<char>(value >> (8 * i));
}

TEST(IndexedOrderedTest, CompactAndGroupedShareBodyAndPreserveV1) {
  for (auto kind :
       {OrderedCollectionKind::kList, OrderedCollectionKind::kSortedSet}) {
    for (std::size_t count : {1, 2, 17, 257}) {
      std::vector<OrderedCollectionEntry> entries;
      for (std::size_t i = 0; i < count; ++i)
        entries.push_back(
            {i % 3 == 0 ? std::string{}
                        : std::string(i % 37, '\0') + std::to_string(i),
             kind == OrderedCollectionKind::kSortedSet ? double(i) : 0});
      // Sorted Sets cannot contain duplicate empty names even at different
      // scores.
      if (kind == OrderedCollectionKind::kSortedSet)
        for (std::size_t i = 1; i < count; ++i)
          entries[i].value_ += std::to_string(i);
      auto compact = EncodeOrderedCompactValue(kind, entries, 2);
      ASSERT_TRUE(compact.ok()) << compact.status();
      EXPECT_EQ(compact->substr(0, 4),
                kind == OrderedCollectionKind::kList ? "LVL2" : "LZS2");
      OrderedGroupSnapshot page{
          .kind_ = kind, .incarnation_ = 17, .id_ = 3, .entries_ = entries};
      auto grouped = EncodeOrderedGroup(page);
      ASSERT_TRUE(grouped.ok()) << grouped.status();
      EXPECT_EQ((*grouped)[8], 2);
      EXPECT_EQ(compact->substr(8), grouped->substr(kOrderedGroupHeaderBytes));
      auto reader = OrderedCompactReader::Open(kind, *compact, count);
      ASSERT_TRUE(reader.ok());
      ASSERT_TRUE(reader->indexed());
      for (std::size_t i = 0; i < count; ++i) {
        auto at = reader->indexed()->At(count - i - 1);
        auto next = reader->Next();
        ASSERT_TRUE(at.ok());
        ASSERT_TRUE(next.ok());
        EXPECT_EQ(at->value_, entries[count - i - 1].value_);
        EXPECT_EQ(next->value_, entries[i].value_);
        EXPECT_EQ(at->score_, entries[count - i - 1].score_);
      }
      EXPECT_FALSE(reader->Next().ok());
      auto decoded = DecodeOrderedGroup(*grouped);
      ASSERT_TRUE(decoded.ok());
      EXPECT_EQ(decoded->entries_, entries);
      auto legacy = EncodeOrderedCompactValue(kind, entries);
      ASSERT_TRUE(legacy.ok());
      auto old_reader = OrderedCompactReader::Open(kind, *legacy, count);
      ASSERT_TRUE(old_reader.ok());
      EXPECT_FALSE(old_reader->indexed());
      auto old_page = test::EncodeOrderedGroupV1(page);
      ASSERT_TRUE(old_page.ok());
      auto old_decoded = DecodeOrderedGroup(*old_page);
      ASSERT_TRUE(old_decoded.ok());
      EXPECT_EQ(old_decoded->entries_, entries);
      for (const auto* wire : {&*grouped, &*old_page}) {
        for (std::size_t chunk : {1, 3, 4, 7, 8, 63, 64, 65, 4096}) {
          OrderedGroupMetadataDecoder decoder(wire->size());
          for (std::size_t at = 0; at < wire->size(); at += chunk)
            ASSERT_TRUE(
                decoder.Read(std::string_view(*wire).substr(at, chunk)).ok());
          auto metadata = decoder.Finish();
          ASSERT_TRUE(metadata.ok()) << metadata.status();
          EXPECT_EQ(metadata->item_count_, count);
          EXPECT_EQ(metadata->min_score_, entries.front().score_);
          EXPECT_EQ(metadata->max_score_, entries.back().score_);
        }
      }
    }
  }
}

TEST(IndexedOrderedTest,
     BinaryBoundsMatchLinearReferenceIncludingTiesAndInfinities) {
  const auto infinity = std::numeric_limits<double>::infinity();
  std::vector<OrderedCollectionEntry> entries;
  for (double score : {-infinity, -2.0, -0.0, 0.0, 1.25, infinity})
    for (auto member : {std::string{}, std::string("a\0", 2),
                        std::string("a\xff", 2), std::string("z")})
      entries.push_back({member, score});
  std::sort(entries.begin(), entries.end(), OrderedEntryLess);
  auto body =
      EncodeOrderedCompactValue(OrderedCollectionKind::kSortedSet, entries, 2);
  ASSERT_TRUE(body.ok());
  auto reader = IndexedOrderedReader::Open(OrderedCollectionKind::kSortedSet,
                                           std::string_view(*body).substr(8),
                                           entries.size());
  ASSERT_TRUE(reader.ok());
  for (double score :
       {-infinity, -3.0, -2.0, -1.0, -0.0, 0.0, 0.5, 1.25, 2.0, infinity}) {
    for (bool exclusive : {false, true}) {
      const auto expected = std::find_if(entries.begin(), entries.end(),
                                         [&](const auto& entry) {
                                           return exclusive
                                                      ? entry.score_ > score
                                                      : entry.score_ >= score;
                                         }) -
                            entries.begin();
      auto found = reader->LowerBoundScore(score, exclusive);
      ASSERT_TRUE(found.ok());
      EXPECT_EQ(*found, expected);
    }
    for (auto member : {std::string{}, std::string("a"), std::string("a\0", 2),
                        std::string("zz")}) {
      const OrderedCollectionEntry needle{member, score};
      const auto expected = std::lower_bound(entries.begin(), entries.end(),
                                             needle, OrderedEntryLess) -
                            entries.begin();
      auto found = reader->LowerBound(score, member);
      ASSERT_TRUE(found.ok());
      EXPECT_EQ(*found, expected);
    }
  }
}

TEST(IndexedOrderedTest, RejectsTruncatedArraysBadOffsetsAndUnknownVersions) {
  for (auto kind :
       {OrderedCollectionKind::kList, OrderedCollectionKind::kSortedSet}) {
    const std::vector<OrderedCollectionEntry> entries{
        {"", 0}, {std::string("a\0b", 3), 0}, {"tail", 1}};
    auto values = entries;
    if (kind == OrderedCollectionKind::kList) values.back().score_ = 0;
    auto encoded = EncodeOrderedCompactValue(kind, values, 2);
    ASSERT_TRUE(encoded.ok());
    for (std::size_t size = 0; size < encoded->size(); ++size) {
      const auto truncated = encoded->substr(0, size);
      auto reader = OrderedCompactReader::Open(kind, truncated, values.size());
      bool valid = reader.ok();
      if (reader.ok())
        for (std::size_t i = 0; i < values.size(); ++i) {
          if (!reader->Next().ok()) {
            valid = false;
            break;
          }
        }
      EXPECT_FALSE(valid) << size;
    }
    auto bad = *encoded;
    bad[3] = '3';
    EXPECT_FALSE(OrderedCompactReader::Open(kind, bad, values.size()).ok());
    const auto offsets = encoded->size() - (values.size() + 1) * 4;
    for (std::size_t i = 0; i <= values.size(); ++i) {
      bad = *encoded;
      IndexPut(bad, offsets + i * 4, UINT32_MAX, 4);
      EXPECT_FALSE(DecodeOrderedCompactValue(kind, bad, values.size()).ok());
    }
    if (kind == OrderedCollectionKind::kSortedSet) {
      bad = *encoded;
      IndexPut(bad, offsets - values.size() * 8 + 8,
               std::bit_cast<std::uint64_t>(
                   std::numeric_limits<double>::quiet_NaN()),
               8);
      EXPECT_FALSE(DecodeOrderedCompactValue(kind, bad, values.size()).ok());
    }
  }
}

TEST(IndexedOrderedTest,
     GroupedRecoveryValidatesEveryOffsetAndScoreWithoutMemberStorage) {
  for (const auto kind :
       {OrderedCollectionKind::kList, OrderedCollectionKind::kSortedSet}) {
    OrderedGroupSnapshot page{
        .kind_ = kind, .incarnation_ = 17, .id_ = 3, .entries_ = {}};
    for (unsigned i = 0; i < 32; ++i)
      page.entries_.push_back(
          {std::to_string(i), kind == OrderedCollectionKind::kList ? 0.0 : i});
    auto encoded = EncodeOrderedGroup(page);
    ASSERT_TRUE(encoded.ok());
    auto rejected = [&](const std::string& bytes) {
      for (std::size_t chunk : {1, 7, 4096}) {
        OrderedGroupMetadataDecoder decoder(bytes.size());
        bool valid = true;
        for (std::size_t at = 0; at < bytes.size(); at += chunk) {
          if (!decoder.Read(std::string_view(bytes).substr(at, chunk)).ok()) {
            valid = false;
            break;
          }
        }
        EXPECT_FALSE(valid && decoder.Finish().ok());
      }
    };
    const auto offsets = encoded->size() - (page.entries_.size() + 1) * 4;
    for (std::size_t i = 0; i <= page.entries_.size(); ++i) {
      auto bad = *encoded;
      IndexPut(bad, offsets + i * 4, UINT32_MAX, 4);
      rejected(bad);
    }
    if (kind == OrderedCollectionKind::kSortedSet) {
      for (double score : {-1.0, std::numeric_limits<double>::quiet_NaN()}) {
        auto bad = *encoded;
        IndexPut(bad, offsets - page.entries_.size() * 8 + 16 * 8,
                 std::bit_cast<std::uint64_t>(score), 8);
        rejected(bad);
      }
    }
  }
}

TEST(IndexedOrderedTest,
     PointReadsDoNotWalkUnrelatedEntriesAndRangesOwnTheirBytes) {
  std::vector<OrderedCollectionEntry> entries;
  for (std::size_t i = 0; i < 128; ++i)
    entries.push_back({std::to_string(i), 0});
  auto body =
      EncodeOrderedCompactValue(OrderedCollectionKind::kList, entries, 2);
  ASSERT_TRUE(body.ok());
  const auto offsets = body->size() - (entries.size() + 1) * 4;
  // The strict decoder must reject this page, but a point read checks its own
  // bounded interval, not a full-page walk hidden behind the offset lookup.
  IndexPut(*body, offsets + 9 * 4, UINT32_MAX, 4);
  auto reader = IndexedOrderedReader::Open(OrderedCollectionKind::kList,
                                           std::string_view(*body).substr(8),
                                           entries.size());
  ASSERT_TRUE(reader.ok());
  EXPECT_FALSE(reader->At(8).ok());
  auto last = reader->At(127);
  ASSERT_TRUE(last.ok());
  EXPECT_EQ(last->value_, "127");
  auto packed = reader->CopyRange(100, 3);
  ASSERT_TRUE(packed.ok());
  body->assign(body->size(), '\xff');
  for (std::size_t i = 0; i < 3; ++i)
    EXPECT_EQ(packed->view(i), std::to_string(100 + i));
}

// Test the real append adapters after moving an unstarted plan. Framing views
// are ephemeral; one-byte extent reads must not overwrite a still-pending span.
void CheckIndexedSplice(const OrderedCompactPayload& source,
                        OrderedCollectionKind kind,
                        const std::vector<OrderedCollectionEntry>& entries) {
  auto expected = EncodeOrderedCompactValue(kind, entries, 2);
  ASSERT_TRUE(expected.ok());
  EXPECT_EQ(source.encoded_bytes(), expected->size());
  for (const std::size_t chunk : {1, 3, 8, 257, 4096}) {
    auto copied = source;
    auto moved = std::move(copied);
    RecordPayloadCursor cursor(moved);
    std::string output(expected->size(), '\0');
    for (std::size_t at = 0; at < output.size(); at += chunk) {
      const auto size = std::min(chunk, output.size() - at);
      ASSERT_TRUE(
          cursor
              .Read(std::as_writable_bytes(std::span(output.data() + at, size)))
              .ok());
    }
    ASSERT_TRUE(cursor.Finish().ok());
    EXPECT_EQ(output, *expected);
  }
  auto copied = source;
  InlineRecordPayload payload(copied);
  std::string output(expected->size() + 2, 'x');
  ASSERT_TRUE(payload
                  .WriteTo(std::as_writable_bytes(
                      std::span(output.data() + 1, expected->size())))
                  .ok());
  EXPECT_EQ(output.substr(1, expected->size()), *expected);
  EXPECT_EQ(output.front(), 'x');
  EXPECT_EQ(output.back(), 'x');
}

TEST(IndexedOrderedSpliceTest, ListEndEditsBorrowValuesAndStreamOffsets) {
  // The indexed alternative fits inside the existing v1 plan's inline space;
  // cardinality changes neither the plan size nor its number of descriptors.
  static_assert(sizeof(IndexedOrderedSplice) <= sizeof(CompactPayload));
  const std::vector<std::string_view> inputs{"", "new",
                                             std::string_view("a\0b", 3)};
  for (std::size_t count : {1, 8, 257}) {
    std::vector<OrderedCollectionEntry> entries;
    for (std::size_t i = 0; i < count; ++i)
      entries.push_back({std::string(i % 13, static_cast<char>(i)), 0});
    auto bytes =
        EncodeOrderedCompactValue(OrderedCollectionKind::kList, entries, 2);
    ASSERT_TRUE(bytes.ok());
    auto reader =
        OrderedCompactReader::Open(OrderedCollectionKind::kList, *bytes, count);
    ASSERT_TRUE(reader.ok());
    for (const auto first : {std::size_t{0}, count / 2, count}) {
      for (const auto end : {first, count}) {
        for (bool left : {false, true}) {
          OrderedCompactPayload plan;
          auto& splice = plan.V2(*reader->indexed());
          if (left) splice.AppendValues(inputs, true);
          splice.AppendSource(first, end - first);
          if (!left) splice.AppendValues(inputs, false);
          std::vector<OrderedCollectionEntry> expected(entries.begin() + first,
                                                       entries.begin() + end);
          for (const auto value : inputs) {
            if (left)
              expected.insert(expected.begin(), {std::string(value), 0});
            else
              expected.push_back({std::string(value), 0});
          }
          CheckIndexedSplice(plan, OrderedCollectionKind::kList, expected);
        }
      }
    }
    OrderedCompactPayload plan;
    plan.V2(*reader->indexed()).AppendSource(0, count);
    ASSERT_TRUE(plan.Next());  // Compact envelope.
    auto values = plan.Next();
    ASSERT_TRUE(values);
    EXPECT_EQ(values->data(), bytes->data() + 8);
    EXPECT_EQ(*values, reader->indexed()->data());
  }
}

TEST(IndexedOrderedSpliceTest, SortedSetMovesDeleteAndInsertWithoutViewArrays) {
  const std::vector<OrderedCollectionEntry> entries{{"", -10},
                                                    {std::string("a\0b", 3), 0},
                                                    {"bb", 0},
                                                    {std::string(8000, 'c'), 5},
                                                    {"tail", 10}};
  auto bytes =
      EncodeOrderedCompactValue(OrderedCollectionKind::kSortedSet, entries, 2);
  ASSERT_TRUE(bytes.ok());
  auto reader = OrderedCompactReader::Open(OrderedCollectionKind::kSortedSet,
                                           *bytes, entries.size());
  ASSERT_TRUE(reader.ok());
  for (std::size_t removed = 0; removed <= entries.size(); ++removed) {
    for (const std::optional<double> score :
         {std::optional<double>{}, std::optional<double>{-20},
          std::optional<double>{0}, std::optional<double>{7},
          std::optional<double>{20}}) {
      if (removed == entries.size() && !score) continue;
      const auto member = removed == entries.size()
                              ? std::string_view("new")
                              : std::string_view(entries[removed].value_);
      auto expected = entries;
      if (removed < entries.size()) expected.erase(expected.begin() + removed);
      if (score) expected.push_back({std::string(member), *score});
      std::sort(expected.begin(), expected.end(), OrderedEntryLess);
      OrderedCompactPayload plan;
      auto& splice = plan.V2(*reader->indexed());
      // Derive source runs from the independent final vector rather than the
      // production planner's old-position/new-position traversal.
      for (std::size_t i = 0; i < expected.size();) {
        if (score && expected[i].value_ == member) {
          splice.AppendMember(member, *score);
          ++i;
          continue;
        }
        const auto found =
            std::find(entries.begin(), entries.end(), expected[i]);
        ASSERT_NE(found, entries.end());
        const auto first = static_cast<std::size_t>(found - entries.begin());
        std::size_t count = 1;
        while (i + count < expected.size() && first + count < entries.size() &&
               first + count != removed &&
               expected[i + count] == entries[first + count])
          ++count;
        splice.AppendSource(first, count);
        i += count;
      }
      CheckIndexedSplice(plan, OrderedCollectionKind::kSortedSet, expected);

      OrderedCompactPayload located;
      ASSERT_TRUE(located.V2(*reader->indexed())
                      .EditSortedSetMember(removed, member, score)
                      .ok());
      CheckIndexedSplice(located, OrderedCollectionKind::kSortedSet, expected);

      IndexedOrderedSplice body(*reader->indexed(), /*compact=*/false);
      ASSERT_TRUE(body.EditSortedSetMember(removed, member, score).ok());
      std::string output;
      output.reserve(body.encoded_bytes());
      while (auto part = body.Next()) output.append(*part);
      const auto encoded = EncodeOrderedCompactValue(
          OrderedCollectionKind::kSortedSet, expected, 2);
      ASSERT_TRUE(encoded.ok());
      EXPECT_EQ(output, encoded->substr(8));
      EXPECT_EQ(output.size(), body.encoded_bytes());
    }
  }
}

TEST(IndexedOrderedSpliceTest, PointLookupHandlesDenseTiesAndInvalidSlots) {
  std::vector<OrderedCollectionEntry> entries;
  for (std::size_t i = 0; i < 257; ++i)
    entries.push_back({std::string(i % 17, '\0') + std::to_string(i),
                       static_cast<double>(i / 32)});
  std::sort(entries.begin(), entries.end(), OrderedEntryLess);
  auto bytes =
      EncodeOrderedCompactValue(OrderedCollectionKind::kSortedSet, entries, 2);
  ASSERT_TRUE(bytes.ok());
  auto reader = OrderedCompactReader::Open(OrderedCollectionKind::kSortedSet,
                                           *bytes, entries.size());
  ASSERT_TRUE(reader.ok());
  for (std::size_t removed = 0; removed < entries.size(); ++removed) {
    for (double score :
         {-std::numeric_limits<double>::infinity(), -0.0, 0.0, 4.0, 32768.0,
          std::numeric_limits<double>::infinity()}) {
      auto expected = entries;
      expected[removed].score_ = score;
      std::sort(expected.begin(), expected.end(), OrderedEntryLess);
      OrderedCompactPayload plan;
      ASSERT_TRUE(
          plan.V2(*reader->indexed())
              .EditSortedSetMember(removed, entries[removed].value_, score)
              .ok());
      CheckIndexedSplice(plan, OrderedCollectionKind::kSortedSet, expected);
    }
  }
  IndexedOrderedSplice invalid(*reader->indexed());
  EXPECT_FALSE(invalid.EditSortedSetMember(0, "missing", 1).ok());
  EXPECT_FALSE(invalid.EditSortedSetMember(entries.size() + 1, "new", 1).ok());
  EXPECT_FALSE(
      invalid.EditSortedSetMember(entries.size(), "new", std::nullopt).ok());
  EXPECT_FALSE(
      invalid
          .EditSortedSetMember(0, entries.front().value_,
                               std::numeric_limits<double>::quiet_NaN())
          .ok());
}

}  // namespace
}  // namespace lavik::storage
