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

#include <algorithm>
#include <deque>
#include <map>
#include <random>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "lavik/memory.h"
#include "lavik/storage/detail/grouped/collection.h"

namespace lavik::storage {
namespace {

// Exercise the real payload planner and incremental directory publication.
// Recovery consumes all historical page versions, not just the final chain.
struct ListSpliceState {
  OrderedGroupDirectory directory_;
  std::map<std::uint64_t, LoadedOrderedGroup> pages_;
  std::vector<RecoveredOrderedGroup> records_;

  static absl::StatusOr<std::vector<RecoveredOrderedGroup>> Records(
      const std::vector<OrderedGroupSnapshot>& pages, std::uint64_t revision) {
    std::vector<RecoveredOrderedGroup> records;
    for (const auto& page : pages) {
      auto encoded = EncodeOrderedGroup(page);
      if (!encoded.ok()) return encoded.status();
      records.push_back({{.incarnation_ = page.incarnation_,
                          .id_ = page.id_,
                          .previous_ = page.previous_,
                          .next_ = page.next_,
                          .sequence_ = revision,
                          .lsn_ = revision,
                          .item_count_ = page.entries_.size(),
                          .encoded_bytes_ = encoded->size(),
                          .record_token_ = page.id_,
                          .retired_ = page.retired_}});
    }
    return records;
  }

  absl::Status Initialize(std::vector<OrderedCollectionEntry> entries,
                          std::size_t target_bytes) {
    auto split = SplitOrderedGroup({.kind_ = OrderedCollectionKind::kList,
                                    .incarnation_ = 17,
                                    .id_ = 1,
                                    .entries_ = std::move(entries)},
                                   2, target_bytes);
    if (!split.ok()) return split.status();
    OrderedCollectionRoot root{
        .kind_ = OrderedCollectionKind::kList,
        .incarnation_ = 17,
        .first_group_ = split->groups_.front().id_,
        .last_group_ = split->groups_.back().id_,
        .next_group_id_ = split->next_group_id_,
        .group_count_ = static_cast<std::uint32_t>(split->groups_.size()),
        .revision_ = 1};
    for (const auto& page : split->groups_)
      root.item_count_ += page.entries_.size();
    auto records = Records(split->groups_, 1);
    if (!records.ok()) return records.status();
    auto directory = OrderedGroupDirectory::Recover(root, 1, *records, {}, 1);
    if (!directory.ok()) return directory.status();
    directory_ = std::move(*directory);
    records_ = std::move(*records);
    for (auto& page : split->groups_)
      pages_.emplace(page.id_, LoadedOrderedGroup{1, std::move(page)});
    return absl::OkStatus();
  }

  absl::Status Splice(std::uint64_t rank, std::uint64_t erased,
                      std::vector<OrderedCollectionEntry> inserted,
                      std::size_t target_bytes) {
    const auto first =
        directory_.FindRank(std::min(rank, directory_.root().item_count_ - 1));
    const auto last =
        erased == 0 ? first : directory_.FindRank(rank + erased - 1);
    const auto begin = first->group_index_ == 0 ? 0 : first->group_index_ - 1;
    const auto end =
        std::min(last->group_index_ + 2, directory_.groups().size());
    std::vector<LoadedOrderedGroup> loaded;
    for (auto i = begin; i < end; ++i)
      loaded.push_back(pages_.at(directory_.groups()[i].id_));
    auto plan =
        PlanOrderedCollectionSplice(directory_, std::move(loaded), rank, erased,
                                    std::move(inserted), target_bytes);
    if (!plan.ok()) return plan.status();
    const auto revision = directory_.sequence() + 1;
    auto records = Records(plan->writes_, revision);
    if (!records.ok()) return records.status();
    auto directory =
        directory_.Apply(plan->root_, revision, *records, revision);
    if (!directory.ok()) return directory.status();
    directory_ = std::move(*directory);
    records_.insert(records_.end(), records->begin(), records->end());
    for (auto& page : plan->writes_) {
      const auto id = page.id_;
      if (page.retired_)
        pages_.erase(id);
      else
        pages_[id] = LoadedOrderedGroup{revision, std::move(page)};
    }
    return absl::OkStatus();
  }

  void CheckValues(const OrderedGroupDirectory& directory,
                   const std::deque<std::string>& expected,
                   std::size_t target_bytes) const {
    ASSERT_EQ(directory.root().item_count_, expected.size());
    std::size_t position = 0;
    for (const auto& group : directory.groups()) {
      const auto& page = pages_.at(group.id_).snapshot_;
      EXPECT_EQ(group.previous_, page.previous_);
      EXPECT_EQ(group.next_, page.next_);
      ASSERT_EQ(group.item_count_, page.entries_.size());
      EXPECT_TRUE(group.encoded_bytes_ <= target_bytes ||
                  page.entries_.size() == 1);
      for (const auto& entry : page.entries_) {
        ASSERT_LT(position, expected.size());
        EXPECT_EQ(entry.value_, expected[position++]);
      }
    }
    EXPECT_EQ(position, expected.size());
  }
};

class ListRingTest : public testing::Test {
 protected:
  void SetUp() override {
    shard_ = CurrentMemoryAccountingShard();
    ASSERT_TRUE(InitMemoryLimit(1024ULL * 1024 * 1024, 1).ok());
    BindMemoryAccountingShard(0);
  }
  void TearDown() override {
    EXPECT_TRUE(InitMemoryLimit(1024ULL * 1024 * 1024, 1).ok());
    BindMemoryAccountingShard(shard_ == 0 ? kMaxMemoryWorkers : shard_ - 1);
    EXPECT_EQ(GetMemoryStats().admission_pending_bytes_, 0);
  }
  static absl::StatusOr<OrderedGroupDirectory> Create(std::size_t count) {
    OrderedCollectionRoot root{
        .kind_ = OrderedCollectionKind::kList,
        .incarnation_ = 17,
        .first_group_ = 1,
        .last_group_ = count,
        .next_group_id_ = count + 1,
        .group_count_ = static_cast<std::uint32_t>(count),
        .revision_ = 1};
    std::vector<RecoveredOrderedGroup> records;
    for (std::size_t i = 0; i < count; ++i) {
      records.push_back({{.incarnation_ = 17,
                          .id_ = i + 1,
                          .previous_ = i,
                          .next_ = i + 1 == count ? 0 : i + 2,
                          .sequence_ = 1,
                          .lsn_ = 1,
                          .item_count_ = 1 + i % 7,
                          .encoded_bytes_ = 64 + 16 * (1 + i % 7),
                          .record_token_ = i + 1}});
      root.item_count_ += records.back().item_count_;
    }
    return OrderedGroupDirectory::Recover(root, 1, records, {}, 1);
  }
  struct Edit {
    OrderedCollectionRoot root_;
    std::vector<RecoveredOrderedGroup> expected_, changed_;
  };
  // Build a reference after-image by ordinary vector splicing, independently
  // of the ring's slot arithmetic and incremental boundary validation.
  static Edit Splice(const OrderedGroupDirectory& old, std::size_t begin,
                     std::size_t erased, std::vector<std::uint64_t> counts) {
    Edit edit{.root_ = old.root(),
              .expected_ = {old.groups().begin(), old.groups().end()}};
    const auto revision = ++edit.root_.revision_;
    std::vector<RecoveredOrderedGroup> inserted;
    for (std::size_t i = 0; i < counts.size(); ++i) {
      const auto id =
          i == 0 ? old.groups()[begin].id_ : edit.root_.next_group_id_++;
      inserted.push_back({{.incarnation_ = 17,
                           .id_ = id,
                           .sequence_ = revision,
                           .lsn_ = revision,
                           .item_count_ = counts[i],
                           .encoded_bytes_ = 64 + counts[i] * 16,
                           .record_token_ = id}});
    }
    for (std::size_t i = begin + (counts.empty() ? 0 : 1); i < begin + erased;
         ++i)
      edit.changed_.push_back({{.incarnation_ = 17,
                                .id_ = old.groups()[i].id_,
                                .sequence_ = revision,
                                .lsn_ = revision,
                                .encoded_bytes_ = 64,
                                .record_token_ = old.groups()[i].id_,
                                .retired_ = true}});
    edit.expected_.erase(edit.expected_.begin() + begin,
                         edit.expected_.begin() + begin + erased);
    edit.expected_.insert(edit.expected_.begin() + begin, inserted.begin(),
                          inserted.end());
    edit.root_.item_count_ = 0;
    edit.root_.group_count_ = edit.expected_.size();
    edit.root_.first_group_ = edit.expected_.front().id_;
    edit.root_.last_group_ = edit.expected_.back().id_;
    for (std::size_t i = 0; i < edit.expected_.size(); ++i) {
      auto& page = edit.expected_[i];
      page.previous_ = i == 0 ? 0 : edit.expected_[i - 1].id_;
      page.next_ =
          i + 1 == edit.expected_.size() ? 0 : edit.expected_[i + 1].id_;
      edit.root_.item_count_ += page.item_count_;
      const auto* before = old.Find(page.id_);
      if (!before || page.sequence_ == revision ||
          before->previous_ != page.previous_ || before->next_ != page.next_) {
        page.sequence_ = page.lsn_ = revision;
        edit.changed_.push_back(page);
      }
    }
    std::reverse(edit.changed_.begin(), edit.changed_.end());
    return edit;
  }
  static absl::StatusOr<OrderedGroupDirectory> Apply(
      const OrderedGroupDirectory& directory, const Edit& edit) {
    return directory.Apply(edit.root_, edit.root_.revision_, edit.changed_,
                           edit.root_.revision_);
  }
  static void Check(const OrderedGroupDirectory& directory,
                    const std::vector<RecoveredOrderedGroup>& expected) {
    ASSERT_EQ(directory.groups().size(), expected.size());
    EXPECT_EQ(directory.groups().end() - directory.groups().begin(),
              expected.size());
    std::uint64_t prefix = 0, bytes = 0;
    for (std::size_t i = 0; i < expected.size(); ++i) {
      const auto& page = directory.groups()[i];
      EXPECT_EQ(page.id_, expected[i].id_);
      EXPECT_EQ(page.previous_, expected[i].previous_);
      EXPECT_EQ(page.next_, expected[i].next_);
      EXPECT_EQ(page.item_count_, expected[i].item_count_);
      EXPECT_EQ(directory.Find(page.id_), &page);
      EXPECT_EQ(directory.FindIndex(page.id_), i);
      EXPECT_EQ(directory.CountBefore(i), prefix);
      for (const auto offset : {std::uint64_t{0}, page.item_count_ - 1}) {
        const auto position = directory.FindRank(prefix + offset);
        ASSERT_TRUE(position.has_value());
        EXPECT_EQ(position->group_index_, i);
        EXPECT_EQ(position->offset_, offset);
      }
      prefix += page.item_count_;
      bytes += page.encoded_bytes_;
    }
    EXPECT_EQ(directory.CountBefore(expected.size()), prefix);
    EXPECT_EQ(directory.root().item_count_, prefix);
    EXPECT_EQ(directory.total_group_bytes(), bytes);
    EXPECT_FALSE(directory.FindRank(prefix));
    for (const auto& page : directory.retired_groups()) {
      EXPECT_EQ(directory.Find(page.id_), nullptr);
      EXPECT_EQ(directory.FindRecord(page.id_), &page);
    }
  }

 private:
  unsigned shard_;
};

TEST_F(ListRingTest, SmallRecoveryStaysCompactThroughFirstGrowthAndWrap) {
  for (const std::size_t count : {1, 22}) {
    const auto baseline = WorkerMemoryAccountingBytes(0);
    auto directory = Create(count);
    ASSERT_TRUE(directory.ok());
    // Include the ID map as well as page metadata and rank cells. Reserving
    // 32 page slots up front would exceed this bound for either small input.
    EXPECT_LT(WorkerMemoryAccountingBytes(0) - baseline, count * 256 + 256);
    const auto original = *directory;
    const std::vector<RecoveredOrderedGroup> expected(original.groups().begin(),
                                                      original.groups().end());
    for (std::size_t step = 0; step < 8; ++step) {
      const auto begin = step % 2 == 0 ? 0 : directory->groups().size() - 1;
      auto edit = Splice(*directory, begin, 1, {2, 3});
      auto updated = Apply(*directory, edit);
      ASSERT_TRUE(updated.ok()) << updated.status();
      Check(*updated, edit.expected_);
      Check(original, expected);
      directory = std::move(updated);
    }
  }
}

TEST_F(ListRingTest, PrependPopCyclesKeepPagesPacked) {
  constexpr std::size_t kItems = 4096, kValueBytes = 256;
  const auto target = kCollectionGroupTargetBytes;
  std::vector<OrderedCollectionEntry> entries;
  std::deque<std::string> expected;
  for (std::size_t i = 0; i < kItems; ++i) {
    auto value = std::to_string(i);
    value.resize(kValueBytes, 'a');
    expected.push_back(value);
    entries.push_back({.value_ = std::move(value)});
  }
  ListSpliceState state;
  ASSERT_TRUE(state.Initialize(std::move(entries), target).ok());
  const auto original = state.directory_;
  const std::vector<RecoveredOrderedGroup> original_groups(
      original.groups().begin(), original.groups().end());
  const auto first_id = original.root().first_group_;
  const auto initial_pages = original.groups().size();
  const auto capacity = (target - kOrderedGroupHeaderBytes) /
                        (kOrderedEntryHeaderBytes + kValueBytes);
  for (std::size_t step = 0; step < kItems; ++step) {
    auto value = std::to_string(step);
    value.resize(kValueBytes, 'b');
    ASSERT_TRUE(state.Splice(0, 0, {{.value_ = value}}, target).ok());
    expected.push_front(std::move(value));
    ASSERT_TRUE(state.Splice(kItems, 1, {}, target).ok());
    expected.pop_back();
    // Keep at most two partially filled end pages. A head that remains full
    // after splitting violates this bound on the very next prepend.
    ASSERT_LE(state.directory_.groups().size(), initial_pages + 1);
    ASSERT_EQ(state.directory_.root().first_group_, first_id);
  }
  EXPECT_LE(
      state.directory_.root().next_group_id_ - original.root().next_group_id_,
      (kItems + capacity - 1) / capacity);
  state.CheckValues(state.directory_, expected, target);
  Check(original, original_groups);
  auto recovered = OrderedGroupDirectory::Recover(
      state.directory_.root(), state.directory_.sequence(), state.records_, {},
      state.directory_.sequence());
  ASSERT_TRUE(recovered.ok()) << recovered.status();
  state.CheckValues(*recovered, expected, target);
  const std::vector<RecoveredOrderedGroup> current(
      state.directory_.groups().begin(), state.directory_.groups().end());
  Check(*recovered, current);
}

TEST_F(ListRingTest,
       BatchedPrependPreservesOrderAcrossVariableAndOversizedItems) {
  for (const auto target : {std::size_t{128}, kCollectionGroupTargetBytes}) {
    SCOPED_TRACE(target);
    ListSpliceState state;
    std::deque<std::string> expected;
    std::vector<OrderedCollectionEntry> entries;
    for (std::size_t i = 0; i < 8; ++i) {
      std::string value(1 + i * target / 16, 'a' + i);
      expected.push_back(value);
      entries.push_back({.value_ = std::move(value)});
    }
    ASSERT_TRUE(state.Initialize(std::move(entries), target).ok());
    const auto original = state.directory_;
    const std::vector<RecoveredOrderedGroup> original_groups(
        original.groups().begin(), original.groups().end());
    for (const auto sizes :
         {std::vector<std::size_t>{0, 1, target / 4, target / 2, target},
          std::vector<std::size_t>{target * 2, 3, target + 1, 2, 0},
          std::vector<std::size_t>{target / 3, 5, target / 2, 1}}) {
      std::vector<OrderedCollectionEntry> inserted;
      for (const auto size : sizes)
        inserted.push_back(
            {.value_ = std::string(size, 'k' + inserted.size())});
      // The command adapter already reverses LPUSH arguments. The planner
      // must retain this supplied logical order even while packing backwards.
      for (auto it = inserted.rbegin(); it != inserted.rend(); ++it)
        expected.push_front(it->value_);
      const auto first_id = state.directory_.root().first_group_;
      ASSERT_TRUE(state.Splice(0, 0, std::move(inserted), target).ok());
      EXPECT_EQ(state.directory_.root().first_group_, first_id);
      state.CheckValues(state.directory_, expected, target);
      auto recovered = OrderedGroupDirectory::Recover(
          state.directory_.root(), state.directory_.sequence(), state.records_,
          {}, state.directory_.sequence());
      ASSERT_TRUE(recovered.ok()) << recovered.status();
      state.CheckValues(*recovered, expected, target);
    }
    Check(original, original_groups);
  }
}

TEST_F(ListRingTest,
       WrapGrowthMiddleSplicesAndSnapshotsMatchVectorAndRecovery) {
  auto directory = Create(31);
  ASSERT_TRUE(directory.ok());
  std::vector<OrderedGroupDirectory> snapshots;
  std::vector<std::vector<RecoveredOrderedGroup>> expected_snapshots;
  std::mt19937 random(827);
  for (std::size_t step = 0; step < 400; ++step) {
    const auto size = directory->groups().size();
    std::size_t begin = 0, erased = 1;
    std::vector<std::uint64_t> counts{1 + random() % 9, 1 + random() % 9};
    if (step >= 80) {
      switch (step % 8) {
        case 0:
          break;
        case 1:
          begin = size - 1;
          break;
        case 2:
          counts.clear();
          break;
        case 3:
          begin = size - 1;
          counts.clear();
          break;
        case 4:
          counts.resize(1);
          break;
        case 5:
          begin = size / 2;
          break;
        case 6:
          begin = size / 2;
          erased = 2;
          counts.resize(1);
          break;
        case 7:
          begin = size - 1;
          counts.resize(1);
          break;
      }
    }
    auto edit = Splice(*directory, begin, erased, counts);
    if (step % 17 == 0) {
      snapshots.push_back(*directory);
      expected_snapshots.emplace_back(directory->groups().begin(),
                                      directory->groups().end());
    }
    auto updated = Apply(*directory, edit);
    ASSERT_TRUE(updated.ok()) << "step " << step << ": " << updated.status();
    Check(*updated, edit.expected_);
    if (step % 19 == 0) {
      auto records = edit.expected_;
      records.insert(records.end(), updated->retired_groups().begin(),
                     updated->retired_groups().end());
      auto recovered = OrderedGroupDirectory::Recover(
          edit.root_, edit.root_.revision_, records, {}, edit.root_.revision_);
      ASSERT_TRUE(recovered.ok()) << recovered.status();
      Check(*recovered, edit.expected_);
    }
    directory = std::move(updated);
  }
  for (std::size_t i = 0; i < snapshots.size(); ++i)
    Check(snapshots[i], expected_snapshots[i]);
}

TEST_F(ListRingTest, EndSplitsAndPopsFitBoundedHeadroomAndShareMiddleChunks) {
  auto directory = Create(8193);
  ASSERT_TRUE(directory.ok());
  const auto original = *directory;
  const auto original_bytes = WorkerMemoryAccountingBytes(0);
  // Rebuilding 8193 page metadata entries cannot fit this budget. End edits
  // must only copy boundary chunks, AVL paths and Fenwick cells.
  const auto steady = GetWorkerMemoryStats(0).retained_bytes_ + 256 * 1024;
  ASSERT_TRUE(InitMemoryLimit((steady * 10 + 8) / 9, 1).ok());
  for (std::size_t step = 0; step < 8; ++step) {
    const bool tail = step % 4 >= 2;
    const bool pop = step % 2;
    const auto begin = tail ? directory->groups().size() - 1 : 0;
    auto edit = Splice(
        *directory, begin, 1,
        pop ? std::vector<std::uint64_t>{} : std::vector<std::uint64_t>{2, 3});
    auto updated = Apply(*directory, edit);
    ASSERT_TRUE(updated.ok()) << updated.status();
    EXPECT_EQ(updated->Find(4097), original.Find(4097));
    Check(*updated, edit.expected_);
    EXPECT_LT(WorkerMemoryAccountingBytes(0) - original_bytes, 256 * 1024);
    directory = std::move(updated);
  }
  EXPECT_EQ(original.groups().size(), 8193);
  EXPECT_EQ(original.groups().front().id_, 1);
}

TEST_F(ListRingTest, InvalidLinksMissingRetirementsAndFailuresPreserveOldView) {
  auto directory = Create(65);
  ASSERT_TRUE(directory.ok());
  const auto original = std::vector<RecoveredOrderedGroup>(
      directory->groups().begin(), directory->groups().end());
  auto split = Splice(*directory, 0, 1, {2, 3});
  auto broken = split;
  broken.changed_.erase(
      std::remove_if(broken.changed_.begin(), broken.changed_.end(),
                     [](const auto& p) { return p.id_ == 2; }),
      broken.changed_.end());
  EXPECT_EQ(Apply(*directory, broken).status().code(),
            absl::StatusCode::kDataLoss);
  broken = split;
  broken.root_.last_group_ = 1;
  EXPECT_EQ(Apply(*directory, broken).status().code(),
            absl::StatusCode::kDataLoss);
  broken = split;
  broken.changed_.front().next_ = broken.changed_.front().id_;
  EXPECT_EQ(Apply(*directory, broken).status().code(),
            absl::StatusCode::kDataLoss);
  auto pop = Splice(*directory, 0, 1, {});
  broken = pop;
  broken.changed_.erase(
      std::remove_if(broken.changed_.begin(), broken.changed_.end(),
                     [](const auto& p) { return p.retired_; }),
      broken.changed_.end());
  EXPECT_EQ(Apply(*directory, broken).status().code(),
            absl::StatusCode::kDataLoss);
  const auto before = WorkerMemoryAccountingBytes(0);
  ASSERT_TRUE(InitMemoryLimit(1, 1).ok());
  EXPECT_EQ(Apply(*directory, split).status().code(),
            absl::StatusCode::kResourceExhausted);
  EXPECT_EQ(WorkerMemoryAccountingBytes(0), before);
  Check(*directory, original);
  ASSERT_TRUE(InitMemoryLimit(1024ULL * 1024 * 1024, 1).ok());
  std::size_t rejected = 0;
  for (const auto headroom : {1024, 4096, 8192, 16384, 32768, 65536, 131072}) {
    const auto steady = GetWorkerMemoryStats(0).retained_bytes_ + headroom;
    ASSERT_TRUE(InitMemoryLimit((steady * 10 + 8) / 9, 1).ok());
    {
      auto attempt = Apply(*directory, pop);
      if (!attempt.ok()) {
        ++rejected;
        EXPECT_EQ(attempt.status().code(),
                  absl::StatusCode::kResourceExhausted);
      }
      Check(*directory, original);
    }
    EXPECT_EQ(WorkerMemoryAccountingBytes(0), before);
    EXPECT_EQ(GetMemoryStats().admission_pending_bytes_, 0);
  }
  EXPECT_GT(rejected, 1);
  ASSERT_TRUE(InitMemoryLimit(1024ULL * 1024 * 1024, 1).ok());
  auto updated = Apply(*directory, pop);
  ASSERT_TRUE(updated.ok()) << updated.status();
  auto resurrect = Splice(*updated, 0, 1, {4});
  resurrect.changed_.push_back(original.front());
  resurrect.changed_.back().sequence_ = resurrect.changed_.back().lsn_ =
      resurrect.root_.revision_;
  EXPECT_EQ(Apply(*updated, resurrect).status().code(),
            absl::StatusCode::kDataLoss);
  Check(*directory, original);
  EXPECT_TRUE(Apply(*directory, split).ok());
}

}  // namespace
}  // namespace lavik::storage
