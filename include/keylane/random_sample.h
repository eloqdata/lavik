#ifndef KEYLANE_RANDOM_SAMPLE_H_
#define KEYLANE_RANDOM_SAMPLE_H_

#include <algorithm>
#include <cstdint>
#include <numeric>
#include <random>
#include <unordered_set>
#include <vector>

namespace keylane {

// Keep these choices aligned with Valkey's HRANDFIELD, SRANDMEMBER, and
// ZRANDMEMBER implementations. Compact collections are sampled in one pass;
// larger collections switch between subtracting from the complete population
// and Floyd sampling at the same 3x crossover used by Valkey. Floyd produces
// the same uniform subsets as rejection sampling without an unbounded retry
// loop.
enum class RandomSampleStrategy : std::uint8_t {
  kDirect,
  kAll,
  kCompactUnique,
  kSubtract,
  kFloyd,
};

inline constexpr std::uint64_t kRandomSampleBatchLimit = 1000;
inline constexpr std::uint64_t kRandomSubStrategyMultiplier = 3;

inline RandomSampleStrategy ChooseRandomSampleStrategy(
    std::uint64_t population, std::uint64_t requested,
    bool compact_collection) {
  if (requested <= 1) return RandomSampleStrategy::kDirect;
  if (requested >= population) return RandomSampleStrategy::kAll;
  if (compact_collection) return RandomSampleStrategy::kCompactUnique;
  if (requested > population / kRandomSubStrategyMultiplier) {
    return RandomSampleStrategy::kSubtract;
  }
  return RandomSampleStrategy::kFloyd;
}

inline std::mt19937_64& RandomSampleGenerator() {
  thread_local std::mt19937_64 generator(std::random_device{}());
  return generator;
}

template <typename Generator>
std::uint64_t RandomRank(std::uint64_t population, Generator& generator) {
  std::uniform_int_distribution<std::uint64_t> distribution(0, population - 1);
  return distribution(generator);
}

template <typename Generator>
bool SelectCurrentRandomRank(std::uint64_t remaining_population,
                             std::uint64_t remaining_requested,
                             Generator& generator) {
  if (remaining_requested == 0) return false;
  if (remaining_requested >= remaining_population) return true;
  std::uniform_int_distribution<std::uint64_t> choose(0,
                                                      remaining_population - 1);
  return choose(generator) < remaining_requested;
}

template <typename Generator>
std::vector<std::uint64_t> SampleUniqueRandomRanks(std::uint64_t population,
                                                   std::uint64_t requested,
                                                   bool compact_collection,
                                                   Generator& generator) {
  requested = std::min(requested, population);
  std::vector<std::uint64_t> ranks;
  if (requested == 0) return ranks;

  switch (
      ChooseRandomSampleStrategy(population, requested, compact_collection)) {
    case RandomSampleStrategy::kDirect:
      ranks.push_back(RandomRank(population, generator));
      return ranks;
    case RandomSampleStrategy::kAll:
      ranks.resize(static_cast<std::size_t>(population));
      std::iota(ranks.begin(), ranks.end(), 0);
      return ranks;
    case RandomSampleStrategy::kCompactUnique: {
      // The same one-pass selection used by Valkey's listpack path. Every
      // subset has equal probability and selected entries retain collection
      // order, so the compact representation is traversed only once.
      ranks.reserve(static_cast<std::size_t>(requested));
      std::uint64_t remaining = population;
      for (std::uint64_t rank = 0; requested != 0; ++rank, --remaining) {
        if (SelectCurrentRandomRank(remaining, requested, generator)) {
          ranks.push_back(rank);
          --requested;
        }
      }
      return ranks;
    }
    case RandomSampleStrategy::kSubtract:
      ranks.resize(static_cast<std::size_t>(population));
      std::iota(ranks.begin(), ranks.end(), 0);
      while (ranks.size() > requested) {
        std::uniform_int_distribution<std::size_t> remove(0, ranks.size() - 1);
        const std::size_t at = remove(generator);
        ranks[at] = ranks.back();
        ranks.pop_back();
      }
      return ranks;
    case RandomSampleStrategy::kFloyd: {
      ranks.reserve(static_cast<std::size_t>(requested));
      std::unordered_set<std::uint64_t> selected;
      selected.reserve(static_cast<std::size_t>(requested));
      for (std::uint64_t offset = 0; offset < requested; ++offset) {
        const std::uint64_t upper = population - requested + offset;
        std::uniform_int_distribution<std::uint64_t> choose(0, upper);
        const std::uint64_t candidate = choose(generator);
        const std::uint64_t rank =
            selected.contains(candidate) ? upper : candidate;
        selected.insert(rank);
        ranks.push_back(rank);
      }
      return ranks;
    }
  }
  return ranks;
}

}  // namespace keylane

#endif  // KEYLANE_RANDOM_SAMPLE_H_
