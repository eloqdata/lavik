#include "keylane/storage/detail/grouped_collection.h"

#include <algorithm>
#include <bit>
#include <limits>
#include <map>
#include <string>
#include <vector>

#include "gtest/gtest.h"

namespace keylane::storage {
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
    result.push_back({.incarnation_ = page.incarnation_,
                      .id_ = page.id_,
                      .previous_ = page.previous_,
                      .next_ = page.next_,
                      .sequence_ = seq,
                      .lsn_ = seq,
                      .txid_ = txid,
                      .item_count_ = page.entries_.size(),
                      .record_token_ = page.id_,
                      .retired_ = page.retired_});
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

}  // namespace
}  // namespace keylane::storage
