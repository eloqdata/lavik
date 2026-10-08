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
#include <random>
#include <vector>

#include "gtest/gtest.h"
#include "lavik/memory.h"
#include "lavik/storage/detail/grouped/collection.h"

namespace lavik::storage {
namespace {

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
      records.push_back({.incarnation_ = 17,
                         .id_ = i + 1,
                         .previous_ = i,
                         .next_ = i + 1 == count ? 0 : i + 2,
                         .sequence_ = 1,
                         .lsn_ = 1,
                         .item_count_ = 1 + i % 7,
                         .encoded_bytes_ = 64 + 16 * (1 + i % 7),
                         .record_token_ = i + 1});
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
      inserted.push_back({.incarnation_ = 17,
                          .id_ = id,
                          .sequence_ = revision,
                          .lsn_ = revision,
                          .item_count_ = counts[i],
                          .encoded_bytes_ = 64 + counts[i] * 16,
                          .record_token_ = id});
    }
    for (std::size_t i = begin + (counts.empty() ? 0 : 1); i < begin + erased;
         ++i)
      edit.changed_.push_back({.incarnation_ = 17,
                               .id_ = old.groups()[i].id_,
                               .sequence_ = revision,
                               .lsn_ = revision,
                               .encoded_bytes_ = 64,
                               .record_token_ = old.groups()[i].id_,
                               .retired_ = true});
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
