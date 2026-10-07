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

#include <array>
#include <cstddef>
#include <cstdint>
#include <map>
#include <random>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "lavik/map_index.h"
#include "lavik/memory.h"
#include "lavik/ordered_index.h"

namespace lavik {
namespace {

// Restore process-wide admission even after ASSERT_* exits a test early.
class IndexMemoryScope {
 public:
  IndexMemoryScope() : shard_(CurrentMemoryAccountingShard()) {
    EXPECT_TRUE(InitMemoryLimit(1024ULL * 1024 * 1024, 1).ok());
    BindMemoryAccountingShard(0);
  }
  ~IndexMemoryScope() {
    EXPECT_TRUE(InitMemoryLimit(1024ULL * 1024 * 1024, 1).ok());
    BindMemoryAccountingShard(shard_ == 0 ? kMaxMemoryWorkers : shard_ - 1);
  }

 private:
  unsigned shard_;
};

TEST(MapIndexTest, BufferedUpdatesPreserveSnapshotsAndEveryLookup) {
  IndexMemoryScope memory;
  using Map = MapIndex<std::uint64_t, std::uint64_t>;
  Map map;
  std::map<std::uint64_t, std::uint64_t> expected;
  for (std::uint64_t key = 0; key < 1152; ++key) {
    ASSERT_TRUE(map.Set(key, 0).ok());
    expected[key] = 0;
  }
  std::vector<std::pair<Map, decltype(expected)>> snapshots;
  std::mt19937 random(731);
  auto check = [](const Map& actual, const auto& values) {
    ASSERT_EQ(actual.size(), values.size());
    auto it = actual.begin();
    for (const auto& [key, sequence] : values) {
      ASSERT_NE(it, actual.end());
      EXPECT_EQ(it->first, key);
      EXPECT_EQ(it->second, sequence);
      ASSERT_NE(actual.Get(key), nullptr);
      EXPECT_EQ(*actual.Get(key), sequence);
      EXPECT_EQ(actual.at(key), sequence);
      EXPECT_EQ(actual.find(key)->second, sequence);
      ++it;
    }
    EXPECT_EQ(it, actual.end());
    for (std::uint64_t key = 0; key < 1154; ++key) {
      auto floor = values.upper_bound(key);
      const auto* found = actual.Floor(key);
      if (floor == values.begin()) {
        EXPECT_EQ(found, nullptr);
      } else {
        --floor;
        ASSERT_NE(found, nullptr);
        EXPECT_EQ(*found, floor->second);
      }
      if (!values.contains(key)) {
        EXPECT_EQ(actual.Get(key), nullptr);
        EXPECT_EQ(actual.find(key), actual.end());
      }
    }
  };
  // Both repeatedly hot routes and dispersed edits exercise replacement,
  // batch folding, deletion of shadowed entries and reinsertion. Untouched
  // routes keep the map large enough to exercise buffering throughout.
  for (std::uint64_t revision = 1; revision <= 800; ++revision) {
    const std::uint64_t key = random() % (revision % 2 ? 8 : 128);
    if (revision % 7 == 0) {
      ASSERT_TRUE(map.Erase(key).ok());
      expected.erase(key);
    } else {
      ASSERT_TRUE(map.SetBuffered(key, revision).ok());
      expected[key] = revision;
    }
    check(map, expected);
    if (revision % 37 == 0) snapshots.emplace_back(map, expected);
  }
  map = Map{};
  for (const auto& [snapshot, values] : snapshots) check(snapshot, values);
}

TEST(OrderedIndexTest, SparseTransfersMatchCountsAndPreserveSnapshots) {
  IndexMemoryScope memory;
  std::vector<std::uint64_t> counts(1025, 10), ends;
  std::uint64_t total = 0;
  for (const auto count : counts) ends.push_back(total += count);
  auto original = OrderedIndex::FromCumulative(ends);
  ASSERT_TRUE(original.ok()) << original.status();
  auto current = *original;
  const auto check = [](const auto& index, const auto& values) {
    std::uint64_t sum = 0;
    for (std::size_t i = 0; i < values.size(); ++i) {
      EXPECT_EQ(index.CountBefore(i), sum);
      for (const auto offset : {std::uint64_t{0}, values[i] - 1}) {
        const auto position = index.Locate(sum + offset);
        EXPECT_EQ(position.group_index_, i);
        EXPECT_EQ(position.offset_, offset);
      }
      sum += values[i];
    }
    EXPECT_EQ(index.CountBefore(values.size()), sum);
  };
  for (std::size_t step = 0; step < 8; ++step) {
    const auto before = current;
    const auto old_counts = counts;
    std::vector<OrderedIndex::CountChange> changes;
    for (const auto i : {step, 255 + step, 1024 - step}) {
      const auto replacement = 1 + (i + step) % 19;
      changes.emplace_back(i, absl::int128(replacement) - counts[i]);
      total = total - counts[i] + replacement;
      counts[i] = replacement;
    }
    ASSERT_TRUE(current.ApplyCounts(changes, total).ok());
    check(current, counts);
    check(before, old_counts);
  }
  check(*original, std::vector<std::uint64_t>(1025, 10));
}

TEST(OrderedIndexTest, AdmissionFailurePreservesPublishedIndex) {
  IndexMemoryScope memory;
  auto original = OrderedIndex::FromCumulative({3, 7, 9});
  ASSERT_TRUE(original.ok());
  auto builder = *original;
  const std::array<OrderedIndex::CountChange, 1> neutral{{{1, 0}}};
  const std::array<OrderedIndex::CountChange, 1> changed{{{1, 1}}};
  ASSERT_TRUE(InitMemoryLimit(1, 1).ok());
  EXPECT_TRUE(builder.ApplyCounts(neutral, 9).ok());
  EXPECT_EQ(builder.ApplyCounts(changed, 10).code(),
            absl::StatusCode::kResourceExhausted);
  EXPECT_EQ(original->CountBefore(2), 7);
  EXPECT_EQ(original->Locate(7).group_index_, 2);
  ASSERT_TRUE(InitMemoryLimit(1024ULL * 1024 * 1024, 1).ok());
  builder = *original;
  ASSERT_TRUE(builder.ApplyCounts(changed, 10).ok());
  EXPECT_EQ(builder.CountBefore(2), 8);
  EXPECT_EQ(original->CountBefore(2), 7);
}

TEST(OrderedIndexTest, FenwickSuffixAppliesPrefixTransfersExactlyOnce) {
  IndexMemoryScope memory;
  for (const std::size_t first : {0, 1, 3, 4, 255, 256, 257}) {
    std::vector<std::uint64_t> ends(258);
    for (std::size_t i = 0; i < ends.size(); ++i) ends[i] = (i + 1) * 3;
    auto original = OrderedIndex::FromCumulative(ends);
    ASSERT_TRUE(original.ok());
    std::vector<OrderedIndex::CountChange> prefix_changes;
    if (first != 0) prefix_changes.emplace_back(0, 2);
    std::vector<std::uint64_t> suffix;
    auto total = first * 3 + (first == 0 ? 0 : 2);
    for (std::size_t i = first; i < 261; ++i)
      suffix.push_back(total += 1 + i % 7);
    auto extended = original->WithSuffix(first, suffix, prefix_changes, total);
    ASSERT_TRUE(extended.ok()) << extended.status();
    for (std::size_t i = 0; i <= 261; ++i) {
      const auto expected =
          i <= first ? i * 3 + (i == 0 ? 0 : 2) : suffix[i - first - 1];
      EXPECT_EQ(extended->CountBefore(i), expected);
      if (i != 261) EXPECT_EQ(extended->Locate(expected).group_index_, i);
    }
    EXPECT_EQ(original->CountBefore(258), 258 * 3);
  }
}

}  // namespace
}  // namespace lavik
