#include "keylane/random_sample.h"

#include <algorithm>
#include <cstdint>
#include <random>
#include <unordered_set>

#include "gtest/gtest.h"

namespace keylane {
namespace {

TEST(RandomSampleTest, ChoosesValkeyStrategies) {
  EXPECT_EQ(ChooseRandomSampleStrategy(100, 1, false),
            RandomSampleStrategy::kDirect);
  EXPECT_EQ(ChooseRandomSampleStrategy(100, 100, false),
            RandomSampleStrategy::kAll);
  EXPECT_EQ(ChooseRandomSampleStrategy(100, 10, true),
            RandomSampleStrategy::kCompactUnique);
  EXPECT_EQ(ChooseRandomSampleStrategy(10, 4, false),
            RandomSampleStrategy::kSubtract);
  EXPECT_EQ(ChooseRandomSampleStrategy(100, 10, false),
            RandomSampleStrategy::kFloyd);
}

TEST(RandomSampleTest, CompactUniqueSelectionIsOrderedAndDistinct) {
  std::mt19937_64 generator(1);
  const auto ranks = SampleUniqueRandomRanks(100, 25, true, generator);
  ASSERT_EQ(ranks.size(), 25);
  EXPECT_TRUE(std::is_sorted(ranks.begin(), ranks.end()));
  EXPECT_EQ(
      std::unordered_set<std::uint64_t>(ranks.begin(), ranks.end()).size(),
      ranks.size());
  EXPECT_LT(ranks.back(), 100);
}

TEST(RandomSampleTest, GenericStrategiesReturnRequestedDistinctRanks) {
  for (const auto [population, requested] :
       {std::pair<std::uint64_t, std::uint64_t>{10, 4}, {100, 10}}) {
    std::mt19937_64 generator(population);
    const auto ranks =
        SampleUniqueRandomRanks(population, requested, false, generator);
    ASSERT_EQ(ranks.size(), requested);
    EXPECT_EQ(
        std::unordered_set<std::uint64_t>(ranks.begin(), ranks.end()).size(),
        ranks.size());
    EXPECT_TRUE(std::ranges::all_of(
        ranks, [population](std::uint64_t rank) { return rank < population; }));
  }
}

TEST(RandomSampleTest, FloydSelectionReturnsRequestedDistinctRanks) {
  std::mt19937_64 generator(2);
  const auto ranks = SampleUniqueRandomRanks(1'000, 300, false, generator);
  ASSERT_EQ(ranks.size(), 300);
  EXPECT_EQ(
      std::unordered_set<std::uint64_t>(ranks.begin(), ranks.end()).size(),
      ranks.size());
  EXPECT_TRUE(std::ranges::all_of(
      ranks, [](std::uint64_t rank) { return rank < 1'000; }));
}

TEST(RandomSampleTest, SequentialSelectionUsesConstantSamplingState) {
  std::mt19937_64 generator(3);
  std::uint64_t remaining_population = 1'000;
  std::uint64_t remaining_requested = 700;
  std::uint64_t selected = 0;
  while (remaining_requested != 0) {
    if (SelectCurrentRandomRank(remaining_population, remaining_requested,
                                generator)) {
      ++selected;
      --remaining_requested;
    }
    --remaining_population;
  }
  EXPECT_EQ(selected, 700);
  EXPECT_LE(remaining_population, 300);
}

}  // namespace
}  // namespace keylane
