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

#include "lavik/storage/detail/grouped_collection.h"

#include <algorithm>
#include <bit>
#include <limits>
#include <map>
#include <string>
#include <vector>

#include "gtest/gtest.h"

namespace lavik::storage {
namespace {

OrderedGroupSnapshot Page(
    std::uint64_t id = 1, std::size_t count = 3,
    OrderedCollectionKind kind = OrderedCollectionKind::kList) {
  OrderedGroupSnapshot page{.kind_ = kind, .incarnation_ = 17, .id_ = id};
  for (std::size_t i = 0; i < count; ++i) {
    page.entries_.push_back({.value_ = "item-" + std::to_string(i),
                             .score_ = kind == OrderedCollectionKind::kList
                                           ? 0
                                           : static_cast<double>(i)});
  }
  return page;
}

OrderedCollectionRoot Root(const std::vector<OrderedGroupSnapshot>& pages,
                           std::uint64_t next_id) {
  OrderedCollectionRoot root{
      .kind_ = pages.front().kind_,
      .incarnation_ = pages.front().incarnation_,
      .first_group_ = pages.front().id_,
      .last_group_ = pages.back().id_,
      .next_group_id_ = next_id,
      .group_count_ = static_cast<std::uint32_t>(pages.size())};
  for (const auto& page : pages) root.item_count_ += page.entries_.size();
  return root;
}

std::vector<RecoveredOrderedGroup> Candidates(
    const std::vector<OrderedGroupSnapshot>& pages, std::uint64_t seq = 1,
    std::uint64_t txid = 0) {
  std::vector<RecoveredOrderedGroup> result;
  for (const auto& page : pages) {
    result.push_back(
        {.incarnation_ = page.incarnation_,
         .id_ = page.id_,
         .previous_ = page.previous_,
         .next_ = page.next_,
         .sequence_ = seq,
         .lsn_ = seq,
         .txid_ = txid,
         .item_count_ = page.entries_.size(),
         .record_token_ = page.id_,
         .retired_ = page.retired_,
         .min_score_ = page.entries_.empty() ? 0 : page.entries_.front().score_,
         .max_score_ =
             page.entries_.empty() ? 0 : page.entries_.back().score_});
  }
  return result;
}

std::vector<LoadedOrderedGroup> Loaded(
    const std::vector<OrderedGroupSnapshot>& pages, std::uint64_t seq = 1) {
  std::vector<LoadedOrderedGroup> result;
  for (const auto& page : pages) result.push_back({seq, page});
  return result;
}

std::vector<OrderedCollectionEntry> Materialize(
    const OrderedGroupDirectory& directory,
    const std::vector<OrderedGroupSnapshot>& before,
    const std::vector<OrderedGroupSnapshot>& writes) {
  std::map<std::uint64_t, const OrderedGroupSnapshot*> pages;
  for (const auto& page : before) pages[page.id_] = &page;
  for (const auto& page : writes) pages[page.id_] = &page;
  std::vector<OrderedCollectionEntry> result;
  for (const auto& group : directory.groups()) {
    const auto& entries = pages.at(group.id_)->entries_;
    result.insert(result.end(), entries.begin(), entries.end());
  }
  return result;
}

TEST(GroupedCollectionTest, RootAndPageRoundTripBothKinds) {
  for (auto kind :
       {OrderedCollectionKind::kList, OrderedCollectionKind::kSortedSet}) {
    auto page = Page(1, 3, kind);
    auto root = Root({page}, 2);
    auto encoded_root = EncodeOrderedCollectionRoot(root);
    ASSERT_TRUE(encoded_root.ok()) << encoded_root.status();
    auto decoded_root = DecodeOrderedCollectionRoot(*encoded_root);
    ASSERT_TRUE(decoded_root.ok()) << decoded_root.status();
    EXPECT_EQ(*decoded_root, root);
    auto bytes = EncodeOrderedGroup(page);
    ASSERT_TRUE(bytes.ok()) << bytes.status();
    auto decoded = DecodeOrderedGroup(*bytes);
    ASSERT_TRUE(decoded.ok()) << decoded.status();
    EXPECT_EQ(decoded->entries_, page.entries_);
    EXPECT_EQ(decoded->kind_, kind);
    EXPECT_EQ(decoded->incarnation_, page.incarnation_);
    EXPECT_EQ(decoded->id_, 1);
    EXPECT_EQ(decoded->previous_, 0);
    EXPECT_EQ(decoded->next_, 0);
    EXPECT_FALSE(decoded->retired_);
  }
}

TEST(GroupedCollectionTest, StringAndStreamHaveDistinctDurableKinds) {
  for (const auto kind :
       {OrderedCollectionKind::kStream, OrderedCollectionKind::kString}) {
    OrderedCollectionRoot root{.kind_ = kind,
                               .incarnation_ = 17,
                               .item_count_ = 4,
                               .first_group_ = 1,
                               .last_group_ = 1,
                               .next_group_id_ = 2,
                               .group_count_ = 1};
    const bool stream = kind == OrderedCollectionKind::kStream;
    if (stream) root.stream_length_ = 0;
    auto encoded = EncodeOrderedCollectionRoot(root);
    ASSERT_TRUE(encoded.ok()) << encoded.status();
    EXPECT_EQ(static_cast<unsigned char>((*encoded)[12]), stream ? 3 : 4);
    auto decoded = DecodeOrderedCollectionRoot(*encoded);
    ASSERT_TRUE(decoded.ok()) << decoded.status();
    EXPECT_EQ(*decoded, root);
    EXPECT_EQ(OrderedKind(OrderedValueType(kind)), kind);
    // Stream requires its length extension; raw String roots must not acquire
    // that interpretation merely because a kind byte is changed.
    (*encoded)[12] = static_cast<char>(stream ? OrderedCollectionKind::kString
                                              : OrderedCollectionKind::kStream);
    EXPECT_FALSE(DecodeOrderedCollectionRoot(*encoded).ok());
  }
}

TEST(GroupedCollectionTest, StringSegmentBoundariesDoNotOrderPayloadBytes) {
  OrderedGroupSnapshot left{
      .kind_ = OrderedCollectionKind::kString,
      .incarnation_ = 17,
      .id_ = 1,
      .next_ = 2,
      .entries_ = {{.value_ = std::string(kStringGroupBytes, 'z')}}};
  auto right = left;
  right.id_ = 2;
  right.previous_ = 1;
  right.next_ = 0;
  // Fixed segments are ordered by position, not by their arbitrary bytes.
  EXPECT_TRUE(ValidateOrderedGroupBoundary(left, right).ok());
  right.entries_.front().value_.assign(kStringGroupBytes, 'a');
  EXPECT_TRUE(ValidateOrderedGroupBoundary(left, right).ok());
  right.previous_ = 0;
  EXPECT_FALSE(ValidateOrderedGroupBoundary(left, right).ok());
}

TEST(GroupedCollectionTest, SortedSetRebalancePreservesOrderAndActiveIds) {
  // Sweep both directions, empty afterimages and mixed member sizes. The
  // two physical pages must remain independently encodable after a move.
  for (std::size_t old_cut = 0; old_cut <= 14; ++old_cut) {
    for (const bool mixed : {false, true}) {
      auto left = Page(11, 0, OrderedCollectionKind::kSortedSet);
      auto right = Page(42, 0, OrderedCollectionKind::kSortedSet);
      left.previous_ = 7;
      left.next_ = 42;
      right.previous_ = 11;
      right.next_ = 99;
      std::vector<OrderedCollectionEntry> all;
      for (std::size_t i = 0; i < 14; ++i) {
        all.push_back({std::string(mixed ? 700 + 37 * i : 1024, 'a' + i),
                       static_cast<double>(i)});
        (i < old_cut ? left.entries_ : right.entries_).push_back(all.back());
      }
      auto balanced = RebalanceSortedSetGroupPair(left, right);
      ASSERT_TRUE(balanced.ok()) << balanced.status();
      EXPECT_EQ(left.id_, 11);
      EXPECT_EQ(left.previous_, 7);
      EXPECT_EQ(left.next_, 42);
      EXPECT_EQ(right.id_, 42);
      EXPECT_EQ(right.previous_, 11);
      EXPECT_EQ(right.next_, 99);
      auto actual = left.entries_;
      actual.insert(actual.end(), right.entries_.begin(), right.entries_.end());
      EXPECT_EQ(actual, all);
      auto encoded_left = EncodeOrderedGroup(left);
      auto encoded_right = EncodeOrderedGroup(right);
      ASSERT_TRUE(encoded_left.ok()) << encoded_left.status();
      ASSERT_TRUE(encoded_right.ok()) << encoded_right.status();
      EXPECT_LE(encoded_left->size(), kCollectionGroupTargetBytes);
      EXPECT_LE(encoded_right->size(), kCollectionGroupTargetBytes);
      EXPECT_EQ(DecodeOrderedGroup(*encoded_left)->entries_, left.entries_);
      EXPECT_EQ(DecodeOrderedGroup(*encoded_right)->entries_, right.entries_);
      if (!mixed) EXPECT_EQ(*balanced, old_cut != 7);
    }
  }
}

TEST(GroupedCollectionTest, SortedSetRebalanceFallbackDoesNotMutatePages) {
  auto left = Page(1, 0, OrderedCollectionKind::kSortedSet);
  auto right = Page(2, 0, OrderedCollectionKind::kSortedSet);
  left.next_ = 2;
  right.previous_ = 1;
  for (std::size_t i = 0; i < 16; ++i)
    (i < 6 ? left.entries_ : right.entries_)
        .push_back({std::string(1024, 'a' + i), static_cast<double>(i)});
  const auto before_left = left.entries_;
  const auto before_right = right.entries_;
  auto balanced = RebalanceSortedSetGroupPair(left, right);
  ASSERT_TRUE(balanced.ok());
  EXPECT_FALSE(*balanced);
  EXPECT_EQ(left.entries_, before_left);
  EXPECT_EQ(right.entries_, before_right);
  right.entries_.resize(1);
  right.entries_[0].value_.assign(kCollectionGroupTargetBytes * 2, 'x');
  const auto oversized = right.entries_;
  balanced = RebalanceSortedSetGroupPair(left, right);
  ASSERT_TRUE(balanced.ok());
  EXPECT_FALSE(*balanced);
  EXPECT_EQ(right.entries_, oversized);
  EXPECT_EQ(left.entries_, before_left);
  right.previous_ = 0;
  EXPECT_FALSE(RebalanceSortedSetGroupPair(left, right).ok());
}

TEST(GroupedCollectionTest, SortedSetRebalanceRejectsInvalidOrdering) {
  auto left = Page(1, 1, OrderedCollectionKind::kSortedSet);
  auto right = Page(2, 0, OrderedCollectionKind::kSortedSet);
  left.next_ = 2;
  right.previous_ = 1;
  for (unsigned i = 0; i < 8; ++i)
    right.entries_.push_back({std::string(1024, 'a' + i), double(i)});
  left.entries_[0] = {std::string(1024, 'z'), 100};
  const auto before_left = left.entries_;
  const auto before_right = right.entries_;
  EXPECT_FALSE(RebalanceSortedSetGroupPair(left, right).ok());
  EXPECT_EQ(left.entries_, before_left);
  EXPECT_EQ(right.entries_, before_right);
}

TEST(GroupedCollectionTest, StringSegmentsValidateLengthsAndDirectPositions) {
  OrderedGroupSnapshot first{
      .kind_ = OrderedCollectionKind::kString,
      .incarnation_ = 17,
      .id_ = 1,
      .next_ = 2,
      .entries_ = {{.value_ = std::string(kStringGroupBytes, 'a')}}};
  OrderedGroupSnapshot tail{.kind_ = OrderedCollectionKind::kString,
                            .incarnation_ = 17,
                            .id_ = 2,
                            .previous_ = 1,
                            .entries_ = {{.value_ = "tail"}}};
  auto root = Root({first, tail}, 3);
  root.item_count_ = kStringGroupBytes + 4;
  auto candidates = Candidates({first, tail});
  candidates[0].item_count_ = kStringGroupBytes;
  candidates[1].item_count_ = 4;
  auto directory = OrderedGroupDirectory::Recover(root, 1, candidates, {});
  ASSERT_TRUE(directory.ok()) << directory.status();
  EXPECT_EQ(directory->Find(1), &directory->groups()[0]);
  EXPECT_EQ(directory->Find(2), &directory->groups()[1]);
  EXPECT_EQ(directory->Find(0), nullptr);
  EXPECT_EQ(directory->Find(3), nullptr);
  auto position = directory->FindRank(kStringGroupBytes + 2);
  ASSERT_TRUE(position.has_value());
  EXPECT_EQ(position->group_index_, 1);
  EXPECT_EQ(position->offset_, 2);
  EXPECT_FALSE(directory->FindRank(root.item_count_).has_value());
  auto encoded = EncodeOrderedGroup(first);
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  EXPECT_EQ(encoded->size(), kOrderedGroupHeaderBytes + kStringGroupBytes);
  auto decoded = DecodeOrderedGroup(*encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(decoded->entries_, first.entries_);
  OrderedGroupMetadataDecoder metadata(encoded->size());
  for (std::size_t offset = 0; offset < encoded->size(); offset += 13)
    ASSERT_TRUE(
        metadata.Read(std::string_view(*encoded).substr(offset, 13)).ok());
  ASSERT_TRUE(metadata.Finish().ok());
  EXPECT_EQ(metadata.Finish()->item_count_, kStringGroupBytes);
  first.entries_[0].value_.pop_back();
  EXPECT_FALSE(EncodeOrderedGroup(first).ok());
  candidates[0].item_count_--;
  candidates[1].item_count_++;
  EXPECT_FALSE(OrderedGroupDirectory::Recover(root, 1, candidates, {}).ok());
  tail.entries_.clear();
  tail.previous_ = 0;
  tail.retired_ = true;
  EXPECT_FALSE(EncodeOrderedGroup(tail).ok());
  auto retired = EncodeOrderedGroup(Page());
  ASSERT_TRUE(retired.ok());
  (*retired)[12] = static_cast<char>(OrderedCollectionKind::kString);
  (*retired)[13] = 1;
  EXPECT_FALSE(DecodeOrderedGroup(*retired).ok());
}

TEST(GroupedCollectionTest, BothRootShapesUseV1AndCheckMemberIndexPresence) {
  auto root = Root({Page(1, 3, OrderedCollectionKind::kSortedSet)}, 2);
  root.revision_ = 9;
  const auto ordered_only = EncodeOrderedCollectionRoot(root);
  ASSERT_TRUE(ordered_only.ok());
  EXPECT_EQ(ordered_only->size(), kOrderedCollectionRootBytes);
  EXPECT_EQ(ordered_only->substr(8, 4), std::string("\x01\0\0\0", 4));
  EXPECT_EQ((*ordered_only)[13], '\0');
  auto decoded_ordered_only = DecodeOrderedCollectionRoot(*ordered_only);
  ASSERT_TRUE(decoded_ordered_only.ok());
  EXPECT_EQ(*decoded_ordered_only, root);
  auto wrong_presence = *ordered_only;
  wrong_presence[13] = 1;
  EXPECT_FALSE(DecodeOrderedCollectionRoot(wrong_presence).ok());
  root.member_index_ = GroupedHashRoot{.incarnation_ = root.incarnation_,
                                       .field_count_ = 3,
                                       .group_count_ = 1,
                                       .revision_ = 7};
  auto bytes = EncodeOrderedCollectionRoot(root);
  ASSERT_TRUE(bytes.ok()) << bytes.status();
  EXPECT_EQ(bytes->size(), kIndexedSortedSetRootBytes);
  EXPECT_EQ(bytes->substr(8, 4), std::string("\x01\0\0\0", 4));
  EXPECT_EQ((*bytes)[13], '\x01');
  auto decoded = DecodeOrderedCollectionRoot(*bytes);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(*decoded, root);
  for (std::size_t n = 0; n < bytes->size(); ++n)
    EXPECT_FALSE(DecodeOrderedCollectionRoot(bytes->substr(0, n)).ok()) << n;
  EXPECT_FALSE(DecodeOrderedCollectionRoot(*bytes + "x").ok());
  auto malformed = *bytes;
  malformed[8] = 2;
  EXPECT_FALSE(DecodeOrderedCollectionRoot(malformed).ok());
  for (unsigned char flag : {0, 2, 255}) {
    malformed = *bytes;
    malformed[13] = static_cast<char>(flag);
    EXPECT_FALSE(DecodeOrderedCollectionRoot(malformed).ok());
  }
  root.member_index_->field_count_ = 2;
  EXPECT_FALSE(EncodeOrderedCollectionRoot(root).ok());
  root.member_index_->field_count_ = 3;
  root.member_index_->revision_ = 10;
  EXPECT_FALSE(EncodeOrderedCollectionRoot(root).ok());
  root.member_index_->revision_ = 7;
  root.kind_ = OrderedCollectionKind::kList;
  EXPECT_FALSE(EncodeOrderedCollectionRoot(root).ok());
}

TEST(GroupedCollectionTest, MemberScoresAreExactAndRejectMalformedValues) {
  for (const double score :
       {0.0, -0.0, 0.1, -19.5, std::numeric_limits<double>::infinity(),
        -std::numeric_limits<double>::infinity()}) {
    auto bytes = EncodeSortedSetMemberScore(score);
    ASSERT_EQ(bytes.size(), 8);
    auto decoded = DecodeSortedSetMemberScore(bytes);
    ASSERT_TRUE(decoded.ok()) << decoded.status();
    EXPECT_EQ(std::bit_cast<std::uint64_t>(*decoded),
              std::bit_cast<std::uint64_t>(score));
  }
  EXPECT_FALSE(DecodeSortedSetMemberScore("1").ok());
  EXPECT_FALSE(
      DecodeSortedSetMemberScore(
          EncodeSortedSetMemberScore(std::numeric_limits<double>::quiet_NaN()))
          .ok());
  EXPECT_FALSE(IsOrderedPageId({0, 0}));
  EXPECT_FALSE(IsOrderedPageId({0, 1}));
  EXPECT_FALSE(IsOrderedPageId({std::uint64_t{1} << 63, 1}));
  EXPECT_TRUE(IsOrderedPageId({1, 0}));
}

TEST(GroupedCollectionTest,
     CodecsRejectTruncationReservedBitsAndInvalidIdentity) {
  auto page = Page();
  auto root = EncodeOrderedCollectionRoot(Root({page}, 2));
  ASSERT_TRUE(root.ok());
  for (std::size_t i = 0; i < root->size(); ++i)
    EXPECT_FALSE(DecodeOrderedCollectionRoot(root->substr(0, i)).ok());
  for (std::size_t offset : {0, 8, 12, 13, 15, 16, 48, 60, 63}) {
    auto broken = *root;
    broken[offset] = static_cast<char>(0xff);
    if (offset != 16 && offset != 48)
      EXPECT_FALSE(DecodeOrderedCollectionRoot(broken).ok()) << offset;
  }
  auto bytes = EncodeOrderedGroup(page);
  ASSERT_TRUE(bytes.ok());
  for (std::size_t i = 0; i < bytes->size(); ++i)
    EXPECT_FALSE(DecodeOrderedGroup(bytes->substr(0, i)).ok());
  EXPECT_FALSE(DecodeOrderedGroup(*bytes + "x").ok());
  for (std::size_t offset : {0, 8, 12, 13, 14, 48, 52, 56, 63, 64}) {
    auto broken = *bytes;
    broken[offset] = static_cast<char>(0xff);
    EXPECT_FALSE(DecodeOrderedGroup(broken).ok()) << offset;
  }
  page.previous_ = page.id_;
  EXPECT_FALSE(EncodeOrderedGroup(page).ok());
  page.previous_ = 0;
  page.entries_.clear();
  EXPECT_FALSE(EncodeOrderedGroup(page).ok());
  page.retired_ = true;
  auto retirement = EncodeOrderedGroup(page);
  ASSERT_TRUE(retirement.ok());
  EXPECT_EQ(retirement->size(), kOrderedGroupHeaderBytes);
  EXPECT_TRUE(DecodeOrderedGroup(*retirement).ok());
  page.next_ = 2;
  EXPECT_FALSE(EncodeOrderedGroup(page).ok());
}

TEST(GroupedCollectionTest,
     SortedSetChecksOrderBinaryTiesNanAndDuplicateMembers) {
  auto page = Page(1, 0, OrderedCollectionKind::kSortedSet);
  page.entries_ = {{"minus", -std::numeric_limits<double>::infinity()},
                   {std::string("a\0", 2), -0.0},
                   {std::string("a\xff", 2), 0.0},
                   {"plus", std::numeric_limits<double>::infinity()}};
  EXPECT_TRUE(EncodeOrderedGroup(page).ok());
  std::swap(page.entries_[1], page.entries_[2]);
  EXPECT_FALSE(EncodeOrderedGroup(page).ok());
  std::swap(page.entries_[1], page.entries_[2]);
  page.entries_.back().score_ = std::numeric_limits<double>::quiet_NaN();
  EXPECT_FALSE(EncodeOrderedGroup(page).ok());
  page.entries_.back() = {"minus", 2};
  EXPECT_FALSE(EncodeOrderedGroup(page).ok());
  auto list = Page();
  list.entries_[0].score_ = -0.0;
  EXPECT_FALSE(EncodeOrderedGroup(list).ok());
}

TEST(GroupedCollectionTest,
     ExtentEncoderBorrowsOversizedItemAndPreservesEmptyItem) {
  auto page = Page(1, 0);
  page.entries_ = {{"", 0}, {std::string(9 * 1024 * 1024, 'x'), 0}};
  auto encoder = OrderedGroupEncoder::Create(page);
  ASSERT_TRUE(encoder.ok()) << encoder.status();
  std::string materialized;
  std::size_t empty_spans = 0;
  bool borrowed = false;
  while (auto part = encoder->Next()) {
    if (part->empty()) ++empty_spans;
    if (part->size() == page.entries_[1].value_.size()) {
      EXPECT_EQ(part->data(), page.entries_[1].value_.data());
      borrowed = true;
    }
    while (!part->empty()) {
      const auto count = std::min<std::size_t>(4093, part->size());
      materialized.append(part->substr(0, count));
      part->remove_prefix(count);
    }
  }
  EXPECT_EQ(empty_spans, 1);
  EXPECT_TRUE(borrowed);
  EXPECT_EQ(materialized.size(), encoder->encoded_bytes());
  auto encoded = EncodeOrderedGroup(page);
  ASSERT_TRUE(encoded.ok());
  EXPECT_EQ(materialized, *encoded);
  auto decoded = DecodeOrderedGroup(materialized);
  ASSERT_TRUE(decoded.ok());
  EXPECT_EQ(decoded->entries_, page.entries_);
  auto split = SplitOrderedGroup(std::move(page), 2);
  ASSERT_TRUE(split.ok());
  ASSERT_EQ(split->groups_.size(), 2);
  EXPECT_EQ(split->groups_[1].entries_.size(), 1);
  EXPECT_GT(split->groups_[1].entries_[0].value_.size(), kExtentPayloadBytes);
}

TEST(GroupedCollectionTest, SplitPreservesOrderIdsAndNeighbourLinks) {
  auto page = Page(7, 12);
  page.previous_ = 3;
  page.next_ = 9;
  const auto entries = page.entries_;
  auto split = SplitOrderedGroup(std::move(page), 20, 104);
  ASSERT_TRUE(split.ok()) << split.status();
  ASSERT_GT(split->groups_.size(), 1);
  EXPECT_EQ(split->groups_.front().id_, 7);
  EXPECT_EQ(split->groups_.front().previous_, 3);
  EXPECT_EQ(split->groups_.back().next_, 9);
  EXPECT_EQ(split->next_group_id_, 20 + split->groups_.size() - 1);
  std::vector<OrderedCollectionEntry> rebuilt;
  for (std::size_t i = 0; i < split->groups_.size(); ++i) {
    const auto& group = split->groups_[i];
    if (i != 0)
      EXPECT_TRUE(
          ValidateOrderedGroupBoundary(split->groups_[i - 1], group).ok());
    auto bytes = EncodeOrderedGroup(group);
    ASSERT_TRUE(bytes.ok());
    EXPECT_LE(bytes->size(), 104);
    rebuilt.insert(rebuilt.end(), group.entries_.begin(), group.entries_.end());
  }
  EXPECT_EQ(rebuilt, entries);
  EXPECT_FALSE(SplitOrderedGroup(Page(), 1).ok());
  EXPECT_FALSE(SplitOrderedGroup(Page(), 2, 64).ok());
  EXPECT_FALSE(
      SplitOrderedGroup(Page(), std::numeric_limits<std::uint64_t>::max(), 80)
          .ok());
}

TEST(GroupedCollectionTest,
     DirectoryRecoversCommittedSequenceBeforeRelocationLsn) {
  auto split = SplitOrderedGroup(Page(1, 10), 2, 104);
  ASSERT_TRUE(split.ok());
  auto root = Root(split->groups_, split->next_group_id_);
  auto candidates = Candidates(split->groups_);
  auto garbage = candidates.front();
  garbage.sequence_ = 9;
  garbage.lsn_ = 99;
  garbage.previous_ = 99;
  candidates.push_back(garbage);  // Future logical mutation is excluded.
  garbage.sequence_ = 2;
  garbage.txid_ = 88;
  candidates.push_back(garbage);  // Uncommitted candidate is excluded.
  garbage = candidates.front();
  garbage.lsn_ = 20;
  garbage.record_token_ = 123;
  candidates.push_back(garbage);
  auto directory = OrderedGroupDirectory::Recover(root, 2, candidates, {});
  ASSERT_TRUE(directory.ok()) << directory.status();
  EXPECT_EQ(directory->groups().front().record_token_, 123);
  for (std::uint64_t rank = 0; rank < root.item_count_; ++rank) {
    auto position = directory->FindRank(rank);
    ASSERT_TRUE(position.has_value());
    EXPECT_LT(position->offset_,
              directory->groups()[position->group_index_].item_count_);
    std::uint64_t reconstructed = position->offset_;
    for (std::size_t i = 0; i < position->group_index_; ++i)
      reconstructed += directory->groups()[i].item_count_;
    EXPECT_EQ(rank, reconstructed);
  }
  EXPECT_FALSE(directory->FindRank(root.item_count_).has_value());
  EXPECT_FALSE(OrderedGroupDirectory::Recover(root, 2, candidates, {88}).ok());
  garbage = candidates.front();
  garbage.lsn_ = 100;
  ++garbage.item_count_;
  candidates.push_back(garbage);
  EXPECT_FALSE(OrderedGroupDirectory::Recover(root, 2, candidates, {}).ok());
}

TEST(GroupedCollectionTest, SparseSameTopologyUpdateMatchesFullRecovery) {
  std::vector<OrderedGroupSnapshot> pages;
  for (std::uint64_t id = 1; id <= 128; ++id) {
    auto page = Page(id, 2);
    page.previous_ = id == 1 ? 0 : id - 1;
    page.next_ = id == 128 ? 0 : id + 1;
    pages.push_back(std::move(page));
  }
  auto root = Root(pages, 129);
  root.revision_ = 1;
  auto records = Candidates(pages);
  auto original = OrderedGroupDirectory::Recover(root, 1, records, {});
  ASSERT_TRUE(original.ok()) << original.status();

  auto changed = records[63];
  changed.sequence_ = changed.lsn_ = 2;
  changed.item_count_ = 4;
  root.item_count_ += 2;
  root.revision_ = 2;
  auto updated = original->Apply(root, 2, std::span(&changed, 1), 2);
  ASSERT_TRUE(updated.ok()) << updated.status();
  records.push_back(changed);
  auto recovered = OrderedGroupDirectory::Recover(root, 2, records, {});
  ASSERT_TRUE(recovered.ok()) << recovered.status();
  ASSERT_EQ(updated->groups().size(), recovered->groups().size());
  for (std::size_t i = 0; i < updated->groups().size(); ++i)
    EXPECT_EQ(updated->groups()[i].item_count_,
              recovered->groups()[i].item_count_);
  for (std::uint64_t rank = 0; rank < root.item_count_; ++rank) {
    const auto fast = updated->FindRank(rank);
    const auto complete = recovered->FindRank(rank);
    ASSERT_TRUE(fast && complete);
    EXPECT_EQ(fast->group_index_, complete->group_index_);
    EXPECT_EQ(fast->offset_, complete->offset_);
  }
  EXPECT_EQ(original->root().item_count_, 256);

  // Only the changed metadata chunk detaches; a pinned predecessor retains
  // its own counts even when the new view redistributes records across pages.
  EXPECT_EQ(&updated->groups()[0], &original->groups()[0]);
  EXPECT_NE(&updated->groups()[63], &original->groups()[63]);
  EXPECT_EQ(original->groups()[63].item_count_, 2);
  auto second = records[95];
  second.sequence_ = second.lsn_ = 2;
  second.item_count_ = 1;
  changed.item_count_ = 3;
  root.item_count_ = 256;
  const std::array redistributed{second, changed};  // Deliberately unordered.
  auto balanced = original->Apply(root, 2, redistributed, 2);
  ASSERT_TRUE(balanced.ok()) << balanced.status();
  for (std::size_t i = 0; i <= pages.size(); ++i) {
    const auto expected = 2 * i + (i > 63 && i <= 95 ? 1 : 0);
    EXPECT_EQ(balanced->CountBefore(i), expected) << i;
    EXPECT_EQ(original->CountBefore(i), 2 * i) << i;
  }
  EXPECT_EQ(&balanced->groups()[127], &original->groups()[127]);
  EXPECT_NE(&balanced->groups()[95], &original->groups()[95]);
  auto overflow = changed;
  overflow.item_count_ = UINT64_MAX;
  EXPECT_FALSE(original->Apply(root, 2, std::span(&overflow, 1), 2).ok());
  struct ResetMemory {
    ~ResetMemory() { (void)InitMemoryLimit(1024ULL * 1024 * 1024, 1); }
  } reset_memory;
  ASSERT_TRUE(InitMemoryLimit(1, 1).ok());
  EXPECT_EQ(original->Apply(root, 2, redistributed, 2).status().code(),
            absl::StatusCode::kResourceExhausted);
  EXPECT_EQ(original->groups()[63].item_count_, 2);
  ASSERT_TRUE(InitMemoryLimit(1024ULL * 1024 * 1024, 1).ok());

  changed.min_score_ = 1;
  changed.max_score_ = 0;
  EXPECT_FALSE(original->Apply(root, 2, std::span(&changed, 1), 2).ok());
}

TEST(GroupedCollectionTest, PageLookupHandlesContiguousAndSparseIds) {
  auto first = Page(7, 2);
  auto second = Page(8, 2);
  auto third = Page(12, 2);
  first.next_ = 8;
  second.previous_ = 7;
  second.next_ = 12;
  third.previous_ = 8;
  const std::vector pages{first, second, third};
  auto directory =
      OrderedGroupDirectory::Recover(Root(pages, 13), 1, Candidates(pages), {});
  ASSERT_TRUE(directory.ok()) << directory.status();
  EXPECT_EQ(directory->Find(7), &directory->groups()[0]);
  EXPECT_EQ(directory->Find(8), &directory->groups()[1]);
  EXPECT_EQ(directory->Find(12), &directory->groups()[2]);
  EXPECT_EQ(directory->Find(9), nullptr);
}

TEST(GroupedCollectionTest,
     DirectoryRejectsMissingCyclesDisconnectedAndWrongCounts) {
  auto split = SplitOrderedGroup(Page(1, 8), 2, 84);
  ASSERT_TRUE(split.ok());
  auto root = Root(split->groups_, split->next_group_id_);
  const auto good = Candidates(split->groups_);
  auto missing = good;
  missing.pop_back();
  EXPECT_FALSE(OrderedGroupDirectory::Recover(root, 1, missing, {}).ok());
  auto cycle = good;
  cycle.back().next_ = cycle.front().id_;
  EXPECT_FALSE(OrderedGroupDirectory::Recover(root, 1, cycle, {}).ok());
  auto disconnected = good;
  disconnected.front().next_ = 0;
  EXPECT_FALSE(OrderedGroupDirectory::Recover(root, 1, disconnected, {}).ok());
  auto counts = good;
  ++counts[1].item_count_;
  EXPECT_FALSE(OrderedGroupDirectory::Recover(root, 1, counts, {}).ok());
  auto backwards = good;
  backwards[1].previous_ = 0;
  EXPECT_FALSE(OrderedGroupDirectory::Recover(root, 1, backwards, {}).ok());
}

TEST(GroupedCollectionTest,
     EveryListSpliceMatchesVectorAndReplaysOnlyWithCommit) {
  const auto original = Page(1, 8).entries_;
  auto split = SplitOrderedGroup(Page(1, 8), 2, 104);
  ASSERT_TRUE(split.ok());
  const auto root = Root(split->groups_, split->next_group_id_);
  const auto candidates = Candidates(split->groups_);
  auto directory = OrderedGroupDirectory::Recover(root, 1, candidates, {});
  ASSERT_TRUE(directory.ok());
  for (std::size_t rank = 0; rank <= original.size(); ++rank) {
    for (std::size_t count = 0; count <= original.size() - rank; ++count) {
      for (std::size_t additions = 0; additions != 4; ++additions) {
        SCOPED_TRACE("rank=" + std::to_string(rank) +
                     " count=" + std::to_string(count) +
                     " additions=" + std::to_string(additions));
        std::vector<OrderedCollectionEntry> insertions(additions, {"new", 0});
        auto expected = original;
        expected.erase(expected.begin() + rank,
                       expected.begin() + rank + count);
        expected.insert(expected.begin() + rank, insertions.begin(),
                        insertions.end());
        auto plan = PlanOrderedCollectionSplice(
            *directory, Loaded(split->groups_), rank, count, insertions, 104);
        ASSERT_TRUE(plan.ok()) << plan.status();
        EXPECT_EQ(plan->expected_sequence_, 1);
        if (expected.empty()) {
          EXPECT_TRUE(plan->delete_key_);
          EXPECT_TRUE(plan->writes_.empty());
          continue;
        }
        auto updated = candidates;
        const auto writes = Candidates(plan->writes_, 2, 91);
        updated.insert(updated.end(), writes.begin(), writes.end());
        auto old_recovery =
            OrderedGroupDirectory::Recover(root, 1, updated, {});
        ASSERT_TRUE(old_recovery.ok()) << old_recovery.status();
        EXPECT_EQ(Materialize(*old_recovery, split->groups_, {}), original);
        auto recovery = OrderedGroupDirectory::Recover(
            plan->root_, plan->changed_ ? 2 : 1, updated, {91});
        ASSERT_TRUE(recovery.ok()) << recovery.status();
        EXPECT_EQ(Materialize(*recovery, split->groups_, plan->writes_),
                  expected);
        std::map<std::uint64_t, OrderedGroupSnapshot> pages;
        for (const auto& page : split->groups_) pages[page.id_] = page;
        for (const auto& page : plan->writes_) pages[page.id_] = page;
        for (std::size_t i = 1; i < recovery->groups().size(); ++i)
          EXPECT_TRUE(ValidateOrderedGroupBoundary(
                          pages.at(recovery->groups()[i - 1].id_),
                          pages.at(recovery->groups()[i].id_))
                          .ok());
      }
    }
  }
}

TEST(GroupedCollectionTest, SameSizeListReplacementNeedsOnlyItsOwnPage) {
  std::vector<OrderedGroupSnapshot> pages{Page(1, 2), Page(2, 2), Page(3, 2)};
  for (std::size_t i = 0; i < pages.size(); ++i) {
    pages[i].previous_ = i;
    pages[i].next_ = i == 2 ? 0 : i + 2;
  }
  auto directory =
      OrderedGroupDirectory::Recover(Root(pages, 4), 1, Candidates(pages), {});
  ASSERT_TRUE(directory.ok()) << directory.status();
  for (const auto rank : {0U, 2U, 3U, 5U}) {
    const auto index = rank / 2;
    auto plan = PlanOrderedCollectionSplice(*directory, Loaded({pages[index]}),
                                            rank, 1, {{.value_ = "change"}});
    ASSERT_TRUE(plan.ok()) << plan.status();
    ASSERT_EQ(plan->writes_.size(), 1);
    EXPECT_TRUE(plan->changed_);
    EXPECT_EQ(plan->root_.item_count_, 6);
    EXPECT_EQ(plan->root_.group_count_, 3);
    EXPECT_EQ(plan->root_.revision_, 0);
    const auto& page = plan->writes_.front();
    EXPECT_EQ(page.id_, pages[index].id_);
    EXPECT_EQ(page.previous_, pages[index].previous_);
    EXPECT_EQ(page.next_, pages[index].next_);
    EXPECT_EQ(page.entries_[rank % 2].value_, "change");
    EXPECT_EQ(page.entries_[1 - rank % 2], pages[index].entries_[1 - rank % 2]);
  }
  // Growth still requires neighbours for splitting; stale pages must never
  // pass through the shortcut even when the replacement has the same size.
  EXPECT_EQ(PlanOrderedCollectionSplice(*directory, Loaded({pages[1]}), 2, 1,
                                        {{.value_ = "longer!"}})
                .status()
                .code(),
            absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(PlanOrderedCollectionSplice(*directory, Loaded({pages[1]}, 2), 2, 1,
                                        {{.value_ = "change"}})
                .status()
                .code(),
            absl::StatusCode::kAborted);
  // LSET of identical bytes remains a mutation for WATCH and replication.
  auto identical = PlanOrderedCollectionSplice(*directory, Loaded({pages[1]}),
                                               2, 1, {pages[1].entries_[0]});
  ASSERT_TRUE(identical.ok()) << identical.status();
  EXPECT_TRUE(identical->changed_);
}

TEST(GroupedCollectionTest, SpliceRejectsStaleMissingDuplicateAndInvalidInput) {
  auto split = SplitOrderedGroup(Page(1, 8), 2, 104);
  ASSERT_TRUE(split.ok());
  auto directory = OrderedGroupDirectory::Recover(
      Root(split->groups_, split->next_group_id_), 1,
      Candidates(split->groups_), {});
  ASSERT_TRUE(directory.ok());
  auto stale = Loaded(split->groups_);
  stale[0].sequence_ = 9;
  EXPECT_EQ(PlanOrderedCollectionSplice(*directory, std::move(stale), 0, 1, {})
                .status()
                .code(),
            absl::StatusCode::kAborted);
  auto missing = Loaded(split->groups_);
  missing.erase(missing.begin() + 1);
  EXPECT_FALSE(
      PlanOrderedCollectionSplice(*directory, std::move(missing), 0, 1, {})
          .ok());
  auto duplicate = Loaded(split->groups_);
  duplicate.push_back(duplicate.front());
  EXPECT_FALSE(
      PlanOrderedCollectionSplice(*directory, std::move(duplicate), 0, 1, {})
          .ok());
  EXPECT_FALSE(
      PlanOrderedCollectionSplice(*directory, Loaded(split->groups_), 9, 0, {})
          .ok());
  EXPECT_FALSE(
      PlanOrderedCollectionSplice(*directory, Loaded(split->groups_), 7, 2, {})
          .ok());
  EXPECT_FALSE(PlanOrderedCollectionSplice(*directory, Loaded(split->groups_),
                                           0, 1, {{"nonzero list score", 1}})
                   .ok());
}

TEST(GroupedCollectionTest,
     SortedSetSpliceChecksBoundsAndRepositionsCompleteEntries) {
  auto split =
      SplitOrderedGroup(Page(1, 8, OrderedCollectionKind::kSortedSet), 2, 104);
  ASSERT_TRUE(split.ok());
  auto directory = OrderedGroupDirectory::Recover(
      Root(split->groups_, split->next_group_id_), 1,
      Candidates(split->groups_), {});
  ASSERT_TRUE(directory.ok());
  auto insert = PlanOrderedCollectionSplice(*directory, Loaded(split->groups_),
                                            2, 0, {{"new", 1.5}}, 104);
  ASSERT_TRUE(insert.ok()) << insert.status();
  EXPECT_FALSE(PlanOrderedCollectionSplice(*directory, Loaded(split->groups_),
                                           2, 0, {{"out of order", -1}}, 104)
                   .ok());
  auto replacement = Page(1, 8, OrderedCollectionKind::kSortedSet).entries_;
  auto moved = replacement.front();
  replacement.erase(replacement.begin());
  moved.score_ = 6.5;
  replacement.insert(replacement.begin() + 6, moved);
  // One enclosing splice is atomic even when a score update crosses pages.
  auto reposition = PlanOrderedCollectionSplice(
      *directory, Loaded(split->groups_), 0, 8, replacement, 104);
  ASSERT_TRUE(reposition.ok()) << reposition.status();
  auto candidates = Candidates(split->groups_);
  auto writes = Candidates(reposition->writes_, 2, 8);
  candidates.insert(candidates.end(), writes.begin(), writes.end());
  auto recovered =
      OrderedGroupDirectory::Recover(reposition->root_, 2, candidates, {8});
  ASSERT_TRUE(recovered.ok()) << recovered.status();
  EXPECT_EQ(Materialize(*recovered, split->groups_, reposition->writes_),
            replacement);
}

TEST(GroupedCollectionTest,
     EveryPartialStructuralBatchKeepsOldRootRecoverable) {
  auto split = SplitOrderedGroup(Page(1, 8), 2, 104);
  ASSERT_TRUE(split.ok());
  const auto root = Root(split->groups_, split->next_group_id_);
  const auto old_records = Candidates(split->groups_);
  auto directory = OrderedGroupDirectory::Recover(root, 1, old_records, {});
  ASSERT_TRUE(directory.ok());
  auto plan = PlanOrderedCollectionSplice(*directory, Loaded(split->groups_), 2,
                                          4, {{"replacement", 0}}, 104);
  ASSERT_TRUE(plan.ok()) << plan.status();
  auto new_records = Candidates(plan->writes_, 2, 81);
  ASSERT_GT(new_records.size(), 1);
  for (std::size_t count = 0; count <= new_records.size(); ++count) {
    auto records = old_records;
    records.insert(records.end(), new_records.begin(),
                   new_records.begin() + count);
    auto restored = OrderedGroupDirectory::Recover(root, 1, records, {});
    ASSERT_TRUE(restored.ok()) << restored.status();
    EXPECT_EQ(Materialize(*restored, split->groups_, {}), Page(1, 8).entries_);
    // Publishing a committed root without all child snapshots is a protocol
    // violation, not permission to recover a silently truncated collection.
    if (count != new_records.size())
      EXPECT_FALSE(
          OrderedGroupDirectory::Recover(plan->root_, 2, records, {81}).ok());
    else
      EXPECT_TRUE(
          OrderedGroupDirectory::Recover(plan->root_, 2, records, {81}).ok());
  }
}

TEST(GroupedCollectionTest, RecoveryFindsRetiredPagesFromUnorderedCandidates) {
  std::vector<OrderedGroupSnapshot> pages{Page()};
  auto root = Root(pages, 82);
  auto candidates = Candidates(pages);
  for (std::uint64_t id = 2; id < 82; ++id)
    candidates.push_back({.incarnation_ = root.incarnation_,
                          .id_ = id,
                          .sequence_ = 1,
                          .lsn_ = 1,
                          .record_token_ = id,
                          .retired_ = true});
  std::reverse(candidates.begin(), candidates.end());
  auto recovered = OrderedGroupDirectory::Recover(root, 1, candidates, {});
  ASSERT_TRUE(recovered.ok()) << recovered.status();
  EXPECT_EQ(recovered->groups().size(), 1);
  EXPECT_EQ(recovered->retired_groups().size(), 80);
  for (std::uint64_t id = 2; id < 82; ++id) {
    EXPECT_EQ(recovered->Find(id), nullptr);
    const auto* retired = recovered->FindRecord(id);
    ASSERT_NE(retired, nullptr) << id;
    EXPECT_TRUE(retired->retired_);
    EXPECT_EQ(retired->record_token_, id);
  }
  EXPECT_EQ(recovered->FindRecord(82), nullptr);
}

TEST(GroupedCollectionTest, RetirementEvidenceCannotDisappearBeforeOlderPage) {
  auto split = SplitOrderedGroup(Page(1, 8), 2, 104);
  ASSERT_TRUE(split.ok());
  auto root = Root(split->groups_, split->next_group_id_);
  auto records = Candidates(split->groups_);
  auto directory = OrderedGroupDirectory::Recover(root, 1, records, {});
  ASSERT_TRUE(directory.ok());
  EXPECT_GE(directory->RetainedBytes(),
            directory->groups().size() *
                (sizeof(RecoveredOrderedGroup) + sizeof(std::uint64_t)));
  auto plan = PlanOrderedCollectionSplice(*directory, Loaded(split->groups_), 2,
                                          4, {}, 104);
  ASSERT_TRUE(plan.ok());
  auto updates = Candidates(plan->writes_, 2, 44);
  records.insert(records.end(), updates.begin(), updates.end());
  EXPECT_TRUE(
      OrderedGroupDirectory::Recover(plan->root_, 2, records, {44}).ok());
  auto retirement =
      std::find_if(records.begin(), records.end(),
                   [](const auto& record) { return record.retired_; });
  ASSERT_NE(retirement, records.end());
  const auto retired_id = retirement->id_;
  records.erase(retirement);
  EXPECT_FALSE(
      OrderedGroupDirectory::Recover(plan->root_, 2, records, {44}).ok());
  // Once the older physical incarnation of this page is also gone, no marker
  // is needed for that id. Root counts/links alone never resurrect its bytes.
  std::erase_if(records,
                [&](const auto& record) { return record.id_ == retired_id; });
  EXPECT_TRUE(
      OrderedGroupDirectory::Recover(plan->root_, 2, records, {44}).ok());
}

TEST(GroupedCollectionTest, EnvelopeOnlyDecodeAndDualDecisionRetirement) {
  auto page = Page(1, 3);
  auto bytes = EncodeOrderedGroup(page);
  ASSERT_TRUE(bytes.ok());
  const auto prefix =
      std::string_view(*bytes).substr(0, kOrderedGroupHeaderBytes);
  auto metadata = DecodeOrderedGroupMetadata(prefix, bytes->size());
  ASSERT_TRUE(metadata.ok()) << metadata.status();
  EXPECT_EQ(metadata->id_, 1);
  EXPECT_EQ(metadata->item_count_, 3);
  EXPECT_FALSE(DecodeOrderedGroupMetadata(prefix, bytes->size() - 1).ok());
  EXPECT_FALSE(
      DecodeOrderedGroupMetadata(prefix.substr(0, 63), bytes->size()).ok());

  auto root = Root({page}, 3);
  root.revision_ = 10;
  auto candidates = Candidates({page}, 10, 100);
  candidates.front().batch_txid_ = 200;
  candidates.push_back({.incarnation_ = 17,
                        .id_ = 2,
                        .sequence_ = 9,
                        .lsn_ = 2,
                        .record_token_ = 2,
                        .retired_ = true});
  EXPECT_FALSE(
      OrderedGroupDirectory::Recover(root, 10, candidates, {100}, 7).ok());
  auto directory =
      OrderedGroupDirectory::Recover(root, 10, candidates, {100, 200}, 7);
  ASSERT_TRUE(directory.ok()) << directory.status();
  EXPECT_EQ(directory->command_sequence(), 7);
  EXPECT_EQ(directory->sequence(), 10);
  EXPECT_EQ(directory->retired_groups().size(), 1);
  EXPECT_EQ(directory->Find(2), nullptr);
  EXPECT_NE(directory->FindRecord(2), nullptr);
  root.revision_ = 11;
  auto changed = candidates.front();
  changed.sequence_ = 11;
  changed.lsn_ = 11;
  auto updated = directory->Apply(root, 11, std::span(&changed, 1), 7);
  ASSERT_TRUE(updated.ok()) << updated.status();
  EXPECT_EQ(updated->retired_groups().size(), 1);
  EXPECT_EQ(updated->Find(1)->sequence_, 11);
  EXPECT_EQ(directory->Find(1)->sequence_, 10);
  auto revived = candidates.back();
  revived.sequence_ = 11;
  revived.retired_ = false;
  revived.item_count_ = 1;
  EXPECT_FALSE(directory->Apply(root, 11, std::span(&revived, 1), 7).ok());
}

TEST(GroupedCollectionTest,
     StreamRanksMatchCountsAcrossSparseUpdatesAndSnapshots) {
  for (const std::size_t pages : {1, 2, 31, 32, 255, 256, 257, 1025, 32769}) {
    std::vector<RecoveredOrderedGroup> records;
    std::vector<std::uint64_t> counts;
    OrderedCollectionRoot root{
        .kind_ = OrderedCollectionKind::kStream,
        .incarnation_ = 17,
        .first_group_ = 1,
        .last_group_ = pages,
        .next_group_id_ = pages + 1,
        .group_count_ = static_cast<std::uint32_t>(pages),
        .stream_length_ = 0};
    for (std::size_t n = 0; n < pages; ++n) {
      counts.push_back(3 + n % 7);
      root.item_count_ += counts.back();
      records.push_back({.incarnation_ = 17,
                         .id_ = n + 1,
                         .previous_ = n,
                         .next_ = n + 1 == pages ? 0 : n + 2,
                         .sequence_ = 1,
                         .lsn_ = 1,
                         .item_count_ = counts.back(),
                         .record_token_ = n + 1});
    }
    auto directory = OrderedGroupDirectory::Recover(root, 1, records, {});
    ASSERT_TRUE(directory.ok()) << directory.status();
    auto check = [](const OrderedGroupDirectory& d, const auto& expected) {
      std::uint64_t prefix = 0;
      for (std::size_t n = 0; n < expected.size(); ++n) {
        EXPECT_EQ(d.CountBefore(n), prefix);
        for (auto offset :
             {std::uint64_t{0}, expected[n] / 2, expected[n] - 1}) {
          const auto rank = d.FindRank(prefix + offset);
          ASSERT_TRUE(rank.has_value());
          EXPECT_EQ(rank->group_index_, n);
          EXPECT_EQ(rank->offset_, offset);
        }
        prefix += expected[n];
      }
      EXPECT_EQ(d.CountBefore(expected.size()), prefix);
      EXPECT_FALSE(d.FindRank(prefix).has_value());
    };
    check(*directory, counts);
    for (std::uint64_t revision = 2; revision != 8; ++revision) {
      auto pinned = *directory;
      const auto before = counts;
      std::vector<RecoveredOrderedGroup> changes;
      for (std::size_t n = 0; n < pages; ++n) {
        if (n != 0 && n != pages / 2 && n + 1 != pages) continue;
        auto changed = directory->groups()[n];
        changed.item_count_ = (revision + n) % 11 + 1;
        changed.sequence_ = changed.lsn_ = revision;
        root.item_count_ -= counts[n];
        root.item_count_ += changed.item_count_;
        counts[n] = changed.item_count_;
        changes.push_back(changed);
      }
      root.revision_ = revision;
      auto updated = directory->Apply(root, revision, changes, revision);
      ASSERT_TRUE(updated.ok()) << updated.status();
      check(*updated, counts);
      check(pinned, before);
      directory = std::move(updated);
    }
  }
}

TEST(GroupedCollectionTest, StructuralUpdatesMatchRecoveryAcrossSplices) {
  auto first = Page(1, 2), second = Page(2, 3), third = Page(3, 4);
  first.next_ = 2;
  second.previous_ = 1;
  second.next_ = 3;
  third.previous_ = 2;
  auto root = Root({first, second, third}, 4);
  auto records = Candidates({first, second, third});
  auto directory = OrderedGroupDirectory::Recover(root, 1, records, {});
  ASSERT_TRUE(directory.ok()) << directory.status();
  for (std::uint64_t revision = 2; revision != 102; ++revision) {
    // Replace an interior page with a fresh identity, leave both neighbours
    // connected, and retain increasingly many tombstones for GC lookup.
    auto left = *directory->Find(root.first_group_);
    auto middle = *directory->Find(left.next_);
    auto right = *directory->Find(middle.next_);
    auto inserted = middle;
    inserted.id_ = root.next_group_id_++;
    inserted.item_count_ = revision % 7 + 1;
    left.next_ = inserted.id_;
    right.previous_ = inserted.id_;
    middle.retired_ = true;
    middle.item_count_ = 0;
    middle.previous_ = middle.next_ = 0;
    std::vector<RecoveredOrderedGroup> changes{right, inserted, middle, left};
    for (auto& item : changes) {
      item.sequence_ = item.lsn_ = revision;
      item.encoded_bytes_ = item.retired_ ? 64 : 128 * item.item_count_;
      item.txid_ = revision + 1000;
      item.batch_txid_ = revision + 2000;
    }
    root.revision_ = revision;
    root.item_count_ =
        left.item_count_ + inserted.item_count_ + right.item_count_;
    auto updated = directory->Apply(root, revision, changes, revision);
    ASSERT_TRUE(updated.ok()) << updated.status();
    records.insert(records.end(), changes.begin(), changes.end());
    absl::flat_hash_set<std::uint64_t> committed;
    for (const auto& item : records) {
      committed.insert(item.txid_);
      committed.insert(item.batch_txid_);
    }
    auto recovered = OrderedGroupDirectory::Recover(root, revision, records,
                                                    committed, revision);
    ASSERT_TRUE(recovered.ok()) << recovered.status();
    EXPECT_EQ(updated->root(), recovered->root());
    EXPECT_EQ(updated->total_group_bytes(), recovered->total_group_bytes());
    EXPECT_EQ(updated->retired_groups().size(), revision - 1);
    for (std::size_t n = 0; n < updated->groups().size(); ++n) {
      const auto& actual = updated->groups()[n];
      const auto& expected = recovered->groups()[n];
      EXPECT_EQ(actual.id_, expected.id_);
      EXPECT_EQ(actual.previous_, expected.previous_);
      EXPECT_EQ(actual.next_, expected.next_);
      EXPECT_EQ(actual.item_count_, expected.item_count_);
      EXPECT_EQ(updated->FindIndex(actual.id_), n);
      EXPECT_EQ(updated->CountBefore(n), recovered->CountBefore(n));
      EXPECT_EQ(updated->FindRank(updated->CountBefore(n))->group_index_, n);
    }
    EXPECT_EQ(directory->Find(left.id_)->next_, middle.id_);
    for (const auto& item : updated->retired_groups()) {
      const auto* expected = recovered->FindRecord(item.id_);
      ASSERT_NE(expected, nullptr);
      EXPECT_EQ(item.sequence_, expected->sequence_);
      EXPECT_TRUE(item.retired_);
    }
    directory = std::move(updated);
  }
}

TEST(GroupedCollectionTest, StructuralUpdatesRejectDisconnectedOrInvalidPages) {
  auto first = Page(1, 2), last = Page(2, 3);
  first.next_ = 2;
  last.previous_ = 1;
  auto root = Root({first, last}, 3);
  auto directory =
      OrderedGroupDirectory::Recover(root, 1, Candidates({first, last}), {});
  ASSERT_TRUE(directory.ok()) << directory.status();
  auto inserted = Candidates({Page(3, 1)}, 2).front();
  inserted.previous_ = 2;
  auto tail = *directory->Find(2);
  tail.next_ = 3;
  tail.sequence_ = tail.lsn_ = 2;
  root.revision_ = 2;
  root.next_group_id_ = 4;
  root.last_group_ = 3;
  root.group_count_ = 3;
  root.item_count_ = 6;
  std::vector<RecoveredOrderedGroup> changes{inserted, tail};
  ASSERT_TRUE(directory->Apply(root, 2, changes, 2).ok());
  auto reject = [&](auto alter) {
    auto broken = changes;
    alter(broken);
    EXPECT_FALSE(directory->Apply(root, 2, broken, 2).ok());
    EXPECT_EQ(directory->Find(2)->next_, 0);
  };
  reject([](auto& c) { c[0].previous_ = 1; });
  reject([](auto& c) { c[0].next_ = 2; });
  reject([](auto& c) { c[1].next_ = 0; });
  reject([](auto& c) { c[0].record_token_ = 0; });
  reject([](auto& c) { c[0].id_ = 4; });
  reject([](auto& c) { c[0].item_count_ = 0; });
  reject([](auto& c) {
    c[0].max_score_ = std::numeric_limits<double>::quiet_NaN();
  });
  reject([](auto& c) { c.push_back(c.front()); });
  auto overflow = changes;
  overflow[0].encoded_bytes_ = UINT64_MAX;
  overflow[1].encoded_bytes_ = UINT64_MAX;
  EXPECT_FALSE(directory->Apply(root, 2, overflow, 2).ok());
}

TEST(GroupedCollectionTest, StreamingMetadataRebuildsBoundsAcrossAnyFraming) {
  const auto infinity = std::numeric_limits<double>::infinity();
  auto page = Page(1, 0, OrderedCollectionKind::kSortedSet);
  page.entries_ = {{"", -infinity},
                   {"a" + std::string(65537, 'x'), -0.0},
                   {"b", 0.0},
                   {"c", 1.5},
                   {"d", infinity}};
  for (auto kind :
       {OrderedCollectionKind::kSortedSet, OrderedCollectionKind::kList}) {
    page.kind_ = kind;
    if (kind == OrderedCollectionKind::kList)
      for (auto& entry : page.entries_) entry.score_ = 0;
    const auto encoded = EncodeOrderedGroup(page);
    ASSERT_TRUE(encoded.ok()) << encoded.status();
    for (const std::size_t chunk : {1, 7, 12, 63, 64, 65, 4096, 65536}) {
      OrderedGroupMetadataDecoder decoder(encoded->size());
      EXPECT_TRUE(decoder.Read({}).ok());
      for (std::size_t offset = 0; offset < encoded->size(); offset += chunk)
        ASSERT_TRUE(
            decoder.Read(std::string_view(*encoded).substr(offset, chunk)).ok())
            << "chunk=" << chunk << " offset=" << offset;
      const auto metadata = decoder.Finish();
      ASSERT_TRUE(metadata.ok()) << metadata.status();
      EXPECT_EQ(metadata->id_, page.id_);
      EXPECT_EQ(metadata->item_count_, page.entries_.size());
      EXPECT_EQ(metadata->min_score_, page.entries_.front().score_);
      EXPECT_EQ(metadata->max_score_, page.entries_.back().score_);
    }
  }
  page.retired_ = true;
  page.entries_.clear();
  const auto encoded = EncodeOrderedGroup(page);
  ASSERT_TRUE(encoded.ok());
  OrderedGroupMetadataDecoder decoder(encoded->size());
  ASSERT_TRUE(decoder.Read(*encoded).ok());
  const auto retired = decoder.Finish();
  ASSERT_TRUE(retired.ok());
  EXPECT_TRUE(retired->retired_);
  EXPECT_EQ(retired->min_score_, 0);
  EXPECT_EQ(retired->max_score_, 0);
}

TEST(GroupedCollectionTest,
     StreamingMetadataRejectsTruncationAndInvalidScores) {
  auto page = Page(1, 2, OrderedCollectionKind::kSortedSet);
  const auto encoded = EncodeOrderedGroup(page);
  ASSERT_TRUE(encoded.ok());
  for (std::size_t size = 0; size < encoded->size(); ++size) {
    OrderedGroupMetadataDecoder decoder(encoded->size());
    ASSERT_TRUE(decoder.Read(std::string_view(*encoded).substr(0, size)).ok());
    EXPECT_FALSE(decoder.Finish().ok()) << "truncated size=" << size;
  }
  OrderedGroupMetadataDecoder extra(encoded->size());
  ASSERT_TRUE(extra.Read(*encoded).ok());
  EXPECT_FALSE(extra.Read("x").ok());
  EXPECT_FALSE(extra.Finish().ok());
  for (const double score : {-1.0, std::numeric_limits<double>::quiet_NaN()}) {
    auto corrupt = *encoded;
    const auto second_score =
        kOrderedGroupHeaderBytes + 12 + page.entries_[0].value_.size() + 4;
    const auto bits = std::bit_cast<std::uint64_t>(score);
    for (unsigned byte = 0; byte < 8; ++byte)
      corrupt[second_score + byte] = static_cast<char>(bits >> (8 * byte));
    OrderedGroupMetadataDecoder decoder(corrupt.size());
    EXPECT_FALSE(decoder.Read(corrupt).ok());
    EXPECT_FALSE(decoder.Finish().ok());
  }
}

TEST(GroupedCollectionTest, ScoreBoundsSeekGapsTiesInfinitiesAndExclusiveEnds) {
  const auto infinity = std::numeric_limits<double>::infinity();
  const std::vector<std::pair<double, double>> bounds = {
      {-infinity, -1}, {-0.0, 0.0}, {0, 0}, {0, 10}, {30, infinity}};
  std::vector<OrderedGroupSnapshot> pages;
  for (std::size_t i = 0; i < bounds.size(); ++i) {
    pages.push_back(
        {.kind_ = OrderedCollectionKind::kSortedSet,
         .incarnation_ = 17,
         .id_ = i + 1,
         .previous_ = i,
         .next_ = i + 1 == bounds.size() ? 0 : i + 2,
         .entries_ = {{std::to_string(i) + "a", bounds[i].first},
                      {std::to_string(i) + "b", bounds[i].second}}});
  }
  auto root = Root(pages, pages.size() + 1);
  const auto records = Candidates(pages);
  auto directory = OrderedGroupDirectory::Recover(root, 1, records, {});
  ASSERT_TRUE(directory.ok()) << directory.status();
  for (double score :
       {-infinity, -2.0, -1.0, -0.0, 0.0, 1.0, 10.0, 11.0, 30.0, infinity}) {
    for (bool exclusive : {false, true}) {
      std::size_t lower = 0, upper = 0;
      while (lower < bounds.size() &&
             (exclusive ? bounds[lower].second <= score
                        : bounds[lower].second < score))
        ++lower;
      while (upper < bounds.size() &&
             (exclusive ? bounds[upper].first < score
                        : bounds[upper].first <= score))
        ++upper;
      EXPECT_EQ(directory->LowerBoundScore(score, exclusive), lower) << score;
      EXPECT_EQ(directory->UpperBoundScore(score, exclusive), upper) << score;
    }
  }
  EXPECT_EQ(directory->LowerBoundScore(0), 1);
  EXPECT_EQ(directory->UpperBoundScore(0), 4);
  EXPECT_EQ(directory->LowerBoundScore(11), directory->UpperBoundScore(11));

  // A new logical view replaces the changed fence, while suspended readers
  // retain the previous directory's score range and rank counts.
  root.revision_ = 2;
  auto changed = records[3];
  changed.sequence_ = changed.lsn_ = 2;
  changed.max_score_ = 20;
  auto updated = directory->Apply(root, 2, std::span(&changed, 1), 2);
  ASSERT_TRUE(updated.ok()) << updated.status();
  EXPECT_EQ(directory->LowerBoundScore(15), 4);
  EXPECT_EQ(updated->LowerBoundScore(15), 3);
  EXPECT_EQ(updated->FindRank(6)->group_index_, 3);
}

TEST(GroupedCollectionTest, RecoveryRejectsInvalidAndNonMonotoneScoreBounds) {
  auto page = Page(1, 2, OrderedCollectionKind::kSortedSet);
  page.next_ = 2;
  auto next = Page(2, 2, OrderedCollectionKind::kSortedSet);
  next.previous_ = 1;
  for (auto& entry : next.entries_) entry.score_ += 2;
  const auto root = Root({page, next}, 3);
  auto records = Candidates({page, next});
  const auto good = records;
  ASSERT_TRUE(OrderedGroupDirectory::Recover(root, 1, records, {}).ok());
  records[1].min_score_ = 0;
  EXPECT_FALSE(OrderedGroupDirectory::Recover(root, 1, records, {}).ok());
  records = good;
  records[1].min_score_ = 4;
  EXPECT_FALSE(OrderedGroupDirectory::Recover(root, 1, records, {}).ok());
  records = good;
  records[1].max_score_ = std::numeric_limits<double>::quiet_NaN();
  EXPECT_FALSE(OrderedGroupDirectory::Recover(root, 1, records, {}).ok());
  records = good;
  auto conflicting = records[1];
  conflicting.lsn_ = 2;
  conflicting.max_score_ = 4;
  records.push_back(conflicting);
  EXPECT_FALSE(OrderedGroupDirectory::Recover(root, 1, records, {}).ok());
}

}  // namespace
}  // namespace lavik::storage
