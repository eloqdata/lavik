#include "keylane/storage/detail/grouped_sorted_rewrite.h"

#include <algorithm>
#include <map>

#include "gtest/gtest.h"

namespace keylane::storage {
namespace {

struct SortedFixture {
  std::vector<OrderedCollectionEntry> entries_;
  std::vector<OrderedGroupSnapshot> pages_;
  OrderedGroupDirectory directory_;
};

SortedFixture Fixture() {
  SortedFixture fixture;
  std::vector<RecoveredOrderedGroup> candidates;
  for (std::uint64_t id = 1; id <= 10; ++id) {
    OrderedGroupSnapshot page{.kind_ = OrderedCollectionKind::kSortedSet,
                              .incarnation_ = 17,
                              .id_ = id,
                              .previous_ = id - 1,
                              .next_ = id == 10 ? 0 : id + 1,
                              .entries_ = {}};
    for (unsigned item = 0; item < 4; ++item) {
      const auto score = (id - 1) * 4 + item;
      page.entries_.push_back({.value_ = "m" + std::to_string(score),
                               .score_ = static_cast<double>(score)});
      fixture.entries_.push_back(page.entries_.back());
    }
    candidates.push_back({.incarnation_ = 17,
                          .id_ = id,
                          .previous_ = page.previous_,
                          .next_ = page.next_,
                          .sequence_ = 1,
                          .lsn_ = 1,
                          .item_count_ = 4,
                          .record_token_ = id});
    fixture.pages_.push_back(std::move(page));
  }
  OrderedCollectionRoot root{.kind_ = OrderedCollectionKind::kSortedSet,
                             .incarnation_ = 17,
                             .item_count_ = 40,
                             .first_group_ = 1,
                             .last_group_ = 10,
                             .next_group_id_ = 11,
                             .group_count_ = 10,
                             .revision_ = 1};
  fixture.directory_ = *OrderedGroupDirectory::Recover(root, 1, candidates, {});
  return fixture;
}

void CheckApplied(const SortedFixture& fixture,
                  OrderedCollectionMutationPlan plan,
                  const std::vector<OrderedCollectionEntry>& expected) {
  if (expected.empty()) {
    EXPECT_TRUE(plan.delete_key_);
    return;
  }
  std::map<std::uint64_t, OrderedGroupSnapshot> pages;
  for (const auto& page : fixture.pages_) pages.emplace(page.id_, page);
  std::vector<RecoveredOrderedGroup> changed;
  for (const auto& page : plan.writes_) {
    pages[page.id_] = page;
    changed.push_back({.incarnation_ = page.incarnation_,
                       .id_ = page.id_,
                       .previous_ = page.previous_,
                       .next_ = page.next_,
                       .sequence_ = 2,
                       .lsn_ = 2,
                       .item_count_ = page.entries_.size(),
                       .record_token_ = page.id_,
                       .retired_ = page.retired_});
  }
  plan.root_.revision_ = 2;
  auto directory = fixture.directory_.Apply(plan.root_, 2, changed, 2);
  ASSERT_TRUE(directory.ok()) << directory.status();
  std::vector<OrderedCollectionEntry> actual;
  for (const auto& metadata : directory->groups()) {
    const auto& page = pages.at(metadata.id_);
    EXPECT_FALSE(page.retired_);
    EXPECT_EQ(page.previous_, metadata.previous_);
    EXPECT_EQ(page.next_, metadata.next_);
    for (const auto& entry : page.entries_) actual.push_back(entry);
  }
  EXPECT_EQ(actual, expected);
}

TEST(GroupedSortedRewriteTest, ScoreMovingAcrossWholeSetTouchesOnlyEnds) {
  const auto fixture = Fixture();
  for (bool to_right : {false, true}) {
    auto after = fixture.entries_;
    if (to_right)
      after.front().score_ = 1000;
    else
      after.back().score_ = -1000;
    std::sort(after.begin(), after.end(), OrderedEntryLess);
    auto plan =
        PlanSortedSetRewrite(fixture.directory_, fixture.entries_, after, 192);
    ASSERT_TRUE(plan.ok()) << plan.status();
    ASSERT_EQ(plan->writes_.size(), 2);
    EXPECT_EQ(plan->writes_.front().id_, 1);
    EXPECT_EQ(plan->writes_.back().id_, 10);
    CheckApplied(fixture, std::move(*plan), after);
  }
}

TEST(GroupedSortedRewriteTest, EmptyPageRetiresAndUpdatesBothNeighbours) {
  const auto fixture = Fixture();
  auto after = fixture.entries_;
  after.erase(after.begin() + 16, after.begin() + 20);
  auto plan =
      PlanSortedSetRewrite(fixture.directory_, fixture.entries_, after, 192);
  ASSERT_TRUE(plan.ok()) << plan.status();
  EXPECT_EQ(plan->root_.group_count_, 9);
  ASSERT_EQ(plan->writes_.size(), 3);
  EXPECT_EQ(std::count_if(plan->writes_.begin(), plan->writes_.end(),
                          [](const auto& page) { return page.retired_; }),
            1);
  CheckApplied(fixture, std::move(*plan), after);
}

TEST(GroupedSortedRewriteTest, SplitAndDisjointEditsPreserveUntouchedPages) {
  const auto fixture = Fixture();
  for (unsigned iteration = 0; iteration < 80; ++iteration) {
    auto after = fixture.entries_;
    after[iteration % after.size()].score_ = 1000 + iteration;
    after.erase(after.begin() + ((iteration * 7) % after.size()));
    for (unsigned item = 0; item < 12; ++item) {
      after.push_back({.value_ = "new-" + std::to_string(item),
                       .score_ = 8.0 + static_cast<double>(item) / 100});
    }
    std::sort(after.begin(), after.end(), OrderedEntryLess);
    auto plan =
        PlanSortedSetRewrite(fixture.directory_, fixture.entries_, after, 128);
    ASSERT_TRUE(plan.ok()) << plan.status();
    EXPECT_GT(plan->root_.next_group_id_, 11);
    CheckApplied(fixture, std::move(*plan), after);
  }
}

TEST(GroupedSortedRewriteTest, NoopDeletionAndInvalidOrder) {
  const auto fixture = Fixture();
  auto noop = PlanSortedSetRewrite(fixture.directory_, fixture.entries_,
                                   fixture.entries_);
  ASSERT_TRUE(noop.ok());
  EXPECT_FALSE(noop->changed_);
  EXPECT_TRUE(noop->writes_.empty());
  auto erased = PlanSortedSetRewrite(fixture.directory_, fixture.entries_, {});
  ASSERT_TRUE(erased.ok());
  EXPECT_TRUE(erased->delete_key_);
  auto unordered = fixture.entries_;
  std::reverse(unordered.begin(), unordered.end());
  EXPECT_FALSE(
      PlanSortedSetRewrite(fixture.directory_, fixture.entries_, unordered)
          .ok());
}

}  // namespace
}  // namespace keylane::storage
