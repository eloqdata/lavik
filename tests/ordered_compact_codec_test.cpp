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
#include <initializer_list>
#include <limits>

#include "gtest/gtest.h"
#include "lavik/storage/detail/compact_payload.h"
#include "support/ordered_v1.h"

namespace lavik::storage {
namespace {

void IndexPut(std::string& bytes, std::size_t offset, std::uint64_t value,
              unsigned width) {
  for (unsigned i = 0; i < width; ++i)
    bytes[offset + i] = static_cast<char>(value >> (8 * i));
}

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

TEST(OrderedCompactCodecTest, OwnedAndBorrowedReadersValidateBothVersions) {
  const std::vector<OrderedCollectionEntry> entries{
      {"", 0}, {std::string("a\0b", 3), 0}, {std::string(32, 'm'), 0}};
  for (const auto kind :
       {OrderedCollectionKind::kList, OrderedCollectionKind::kSortedSet}) {
    for (const unsigned version : {1, 2}) {
      SCOPED_TRACE(version);
      auto bytes = EncodeOrderedCompactValue(kind, entries, version);
      ASSERT_TRUE(bytes.ok());
      auto check = [&](std::string_view input, std::size_t count, bool valid) {
        auto owned = DecodeOrderedCompactValue(kind, input, count);
        EXPECT_EQ(owned.ok(), valid);
        if (owned.ok()) EXPECT_EQ(*owned, entries);
        auto reader = OrderedCompactReader::Open(kind, input, count);
        bool readable = reader.ok();
        for (std::size_t i = 0; readable && i < count; ++i)
          readable = reader->Next().ok();
        EXPECT_EQ(readable, valid);
      };
      for (std::size_t size = 0; size <= bytes->size(); ++size) {
        SCOPED_TRACE(size);
        check(std::string_view(*bytes).substr(0, size), entries.size(),
              size == bytes->size());
      }
      check(*bytes, entries.size() + 1, false);
      check(*bytes + "x", entries.size(), false);
      auto bad = *bytes;
      bad[3] = '3';
      check(bad, entries.size(), false);
      const auto other = kind == OrderedCollectionKind::kList
                             ? OrderedCollectionKind::kSortedSet
                             : OrderedCollectionKind::kList;
      EXPECT_FALSE(
          OrderedCompactReader::Open(other, *bytes, entries.size()).ok());
      EXPECT_FALSE(
          DecodeOrderedCompactValue(other, *bytes, entries.size()).ok());
      auto reader = OrderedCompactReader::Open(kind, *bytes, entries.size());
      ASSERT_TRUE(reader.ok());
      EXPECT_EQ(reader->indexed().has_value(), version == 2);
      auto copied = *reader;
      for (const auto& expected : entries) {
        auto a = reader->Next();
        auto b = copied.Next();
        ASSERT_TRUE(a.ok());
        ASSERT_TRUE(b.ok());
        EXPECT_EQ(a->value_, expected.value_);
        EXPECT_EQ(a->score_, expected.score_);
        EXPECT_EQ(a->encoded_, b->encoded_);
      }
      EXPECT_FALSE(reader->Next().ok());
      if (version == 1) continue;
      const auto offsets = bytes->size() - (entries.size() + 1) * 4;
      for (std::size_t i = 0; i <= entries.size(); ++i) {
        bad = *bytes;
        IndexPut(bad, offsets + i * 4, UINT32_MAX, 4);
        check(bad, entries.size(), false);
      }
      if (kind == OrderedCollectionKind::kSortedSet) {
        bad = *bytes;
        IndexPut(bad, offsets - entries.size() * 8 + 8,
                 std::bit_cast<std::uint64_t>(
                     std::numeric_limits<double>::quiet_NaN()),
                 8);
        check(bad, entries.size(), false);
      }
    }
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
      auto old_page = test::EncodeOrderedGroupV1(page);
      ASSERT_TRUE(old_page.ok());
      auto old_decoded = DecodeOrderedGroup(*old_page);
      ASSERT_TRUE(old_decoded.ok());
      EXPECT_EQ(old_decoded->entries_, entries);
    }
  }
}

TEST(IndexedOrderedTest, BulkDecodeMatchesCheckedSlotsIncludingCorruption) {
  for (const auto kind :
       {OrderedCollectionKind::kList, OrderedCollectionKind::kSortedSet}) {
    const auto check = [&](std::string_view body, std::size_t count) {
      auto reader = IndexedOrderedReader::Open(kind, body, count);
      if (!reader.ok()) return;
      auto bulk = reader->DecodeAll();
      std::vector<OrderedCollectionEntryView> slots;
      bool valid = true;
      for (std::size_t i = 0; i < count; ++i) {
        auto slot = reader->At(i);
        if (!slot.ok()) {
          valid = false;
          break;
        }
        slots.push_back(*slot);
      }
      ASSERT_EQ(bulk.ok(), valid);
      if (!valid) {
        EXPECT_FALSE(reader->FindSortedSetMember("missing").ok());
        return;
      }
      ASSERT_EQ(bulk->size(), slots.size());
      for (std::size_t i = 0; i < slots.size(); ++i) {
        EXPECT_EQ((*bulk)[i].value_, slots[i].value_);
        EXPECT_EQ((*bulk)[i].value_.data(), slots[i].value_.data());
        EXPECT_EQ(std::bit_cast<std::uint64_t>((*bulk)[i].score_),
                  std::bit_cast<std::uint64_t>(slots[i].score_));
      }
      if (kind == OrderedCollectionKind::kList) {
        EXPECT_TRUE(absl::IsInvalidArgument(
            reader->FindSortedSetMember("missing").status()));
        return;
      }
      for (const auto& target : {std::string{}, std::string("a\0b", 3),
                                 std::string("tail"), std::string("missing")}) {
        bool ordered = true;
        std::size_t found = count;
        for (std::size_t i = 0; i < slots.size(); ++i) {
          if (i != 0 && (slots[i - 1].score_ > slots[i].score_ ||
                         (slots[i - 1].score_ == slots[i].score_ &&
                          slots[i - 1].value_ > slots[i].value_)))
            ordered = false;
          if (slots[i].value_ == target) {
            if (found != count) ordered = false;
            found = i;
          }
        }
        auto actual = reader->FindSortedSetMember(target);
        ASSERT_EQ(actual.ok(), ordered);
        if (actual.ok()) EXPECT_EQ(*actual, found);
      }
    };
    check(std::string(4, '\0'), 0);
    const auto infinity = std::numeric_limits<double>::infinity();
    std::vector<OrderedCollectionEntry> entries{{"", -infinity},
                                                {std::string("a\0b", 3), -0.0},
                                                {"c", 0.0},
                                                {"tail", infinity}};
    if (kind == OrderedCollectionKind::kList)
      for (auto& entry : entries) entry.score_ = 0;
    auto encoded = EncodeOrderedCompactValue(kind, entries, 2);
    ASSERT_TRUE(encoded.ok());
    const auto body = encoded->substr(8);
    check(body, entries.size());
    // Negative infinity with a nonzero mantissa also covers a NaN score.
    // Mutating the offset array exercises descending and out-of-body spans.
    for (std::size_t i = 0; i < body.size(); ++i) {
      for (const unsigned mask : {1u, 0x80u, 0xffu}) {
        auto bad = body;
        bad[i] ^= static_cast<char>(mask);
        SCOPED_TRACE(i);
        SCOPED_TRACE(mask);
        check(bad, entries.size());
      }
    }
  }
}

TEST(IndexedOrderedTest, CompactMemberScanChecksTailAndTargetDuplicates) {
  std::vector<OrderedCollectionEntry> entries{
      {"dup", 0}, {"middle", 1}, {"dup", 2}};
  auto encoded =
      EncodeOrderedCompactValue(OrderedCollectionKind::kSortedSet, entries, 2);
  ASSERT_TRUE(encoded.ok());
  auto reader = IndexedOrderedReader::Open(OrderedCollectionKind::kSortedSet,
                                           std::string_view(*encoded).substr(8),
                                           entries.size());
  ASSERT_TRUE(reader.ok());
  EXPECT_TRUE(absl::IsDataLoss(reader->FindSortedSetMember("dup").status()));
  EXPECT_EQ(*reader->FindSortedSetMember("middle"), 1);
  EXPECT_EQ(*reader->FindSortedSetMember("missing"), entries.size());
  // Finding the first member cannot bypass validation of a later score.
  const auto scores = encoded->size() - (entries.size() * 12 + 4);
  auto bad = *encoded;
  IndexPut(
      bad, scores + 2 * 8,
      std::bit_cast<std::uint64_t>(std::numeric_limits<double>::quiet_NaN()),
      8);
  auto corrupt = IndexedOrderedReader::Open(OrderedCollectionKind::kSortedSet,
                                            std::string_view(bad).substr(8),
                                            entries.size());
  ASSERT_TRUE(corrupt.ok());
  EXPECT_TRUE(
      absl::IsDataLoss(corrupt->FindSortedSetMember("middle").status()));
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

// The edit matrix uses inline output. Representative cases also move the plan
// through chunked readers; one-byte reads must not overwrite pending framing.
void CheckIndexedSplice(const OrderedCompactPayload& source,
                        OrderedCollectionKind kind,
                        const std::vector<OrderedCollectionEntry>& entries,
                        std::initializer_list<std::size_t> chunks = {}) {
  auto expected = EncodeOrderedCompactValue(kind, entries, 2);
  ASSERT_TRUE(expected.ok());
  EXPECT_EQ(source.encoded_bytes(), expected->size());
  for (const std::size_t chunk : chunks) {
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
          CheckIndexedSplice(plan, OrderedCollectionKind::kList, expected,
                             {1, 4096});
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

TEST(IndexedOrderedSpliceTest,
     SortedSetEditsMatchModelAcrossTiesAndRejectInvalidSlots) {
  for (const std::size_t count : {5, 257}) {
    std::vector<OrderedCollectionEntry> entries{{"", -10},
                                                {std::string("a\0b", 3), 0},
                                                {"bb", 0},
                                                {std::string(8000, 'c'), 5},
                                                {"tail", 10}};
    if (count > entries.size()) entries[3].value_.resize(32);
    for (std::size_t i = entries.size(); i < count; ++i)
      entries.push_back({std::string(i % 17, '\0') + std::to_string(i),
                         static_cast<double>(i / 32)});
    std::sort(entries.begin(), entries.end(), OrderedEntryLess);
    auto bytes = EncodeOrderedCompactValue(OrderedCollectionKind::kSortedSet,
                                           entries, 2);
    ASSERT_TRUE(bytes.ok());
    auto reader = OrderedCompactReader::Open(OrderedCollectionKind::kSortedSet,
                                             *bytes, entries.size());
    ASSERT_TRUE(reader.ok());
    auto check_edit = [&](std::size_t removed, std::optional<double> score,
                          std::initializer_list<std::size_t> chunks = {}) {
      SCOPED_TRACE(removed);
      SCOPED_TRACE(score ? std::to_string(*score) : "delete");
      const auto member = removed == entries.size()
                              ? std::string_view("new")
                              : std::string_view(entries[removed].value_);
      auto expected = entries;
      if (removed < entries.size()) expected.erase(expected.begin() + removed);
      if (score) expected.push_back({std::string(member), *score});
      std::sort(expected.begin(), expected.end(), OrderedEntryLess);
      OrderedCompactPayload located;
      ASSERT_TRUE(located.V2(*reader->indexed())
                      .EditSortedSetMember(removed, member, score)
                      .ok());
      CheckIndexedSplice(located, OrderedCollectionKind::kSortedSet, expected,
                         chunks);
    };
    for (std::size_t removed = 0; removed <= entries.size(); ++removed) {
      for (const std::optional<double> score :
           {std::optional<double>{}, std::optional<double>{-20},
            std::optional<double>{-0.0}, std::optional<double>{0},
            std::optional<double>{7}, std::optional<double>{20},
            std::optional<double>{32768},
            std::optional<double>{-std::numeric_limits<double>::infinity()},
            std::optional<double>{std::numeric_limits<double>::infinity()}}) {
        if (removed == entries.size() && !score) continue;
        check_edit(removed, score);
      }
    }
    if (count == 5) {
      // Left move, empty member moving right, insertion, deletion, and a large
      // member crossing extent chunks cover adapter-specific boundaries.
      const std::pair<std::size_t, std::optional<double>> cases[]{
          {4, -20}, {0, 7}, {5, 0}, {1, std::nullopt}, {3, 20}};
      for (const auto& [removed, score] : cases)
        check_edit(removed, score, {1, 4096});
    }
    IndexedOrderedSplice invalid(*reader->indexed());
    EXPECT_FALSE(invalid.EditSortedSetMember(0, "missing", 1).ok());
    EXPECT_FALSE(
        invalid.EditSortedSetMember(entries.size() + 1, "new", 1).ok());
    EXPECT_FALSE(
        invalid.EditSortedSetMember(entries.size(), "new", std::nullopt).ok());
    EXPECT_FALSE(
        invalid
            .EditSortedSetMember(0, entries.front().value_,
                                 std::numeric_limits<double>::quiet_NaN())
            .ok());
  }
}

}  // namespace
}  // namespace lavik::storage
