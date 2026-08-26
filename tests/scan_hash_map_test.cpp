#include "keylane/storage/scan_hash_map.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <unordered_map>

#include "keylane/storage/format.h"

namespace {

using keylane::storage::ComputeDigest;
using keylane::storage::Digest;
using keylane::storage::ScanHashMap;

#define ASSERT_CHECK(condition, message) ASSERT_TRUE(condition) << message

}  // namespace

TEST(ScanHashMapTest, InsertScanMoveDetachAndErase) {
  ScanHashMap<std::uint64_t> map;
  ASSERT_CHECK(!map.has_allocated_storage(),
               "default map unexpectedly owns storage");
  constexpr std::uint64_t kInitial = 10000;

  for (std::uint64_t i = 0; i < kInitial; ++i) {
    const std::string key = "key-" + std::to_string(i);
    auto inserted = map.InsertOrAssign(ComputeDigest(key), key, i);
    ASSERT_CHECK(inserted.inserted_ && inserted.entry_->value_ == i,
                 "initial insert failed");
  }
  ASSERT_CHECK(map.size() == kInitial, "unexpected map size");
  ASSERT_CHECK(map.has_allocated_storage(),
               "populated map does not report its storage");

  for (std::uint64_t i = 0; i < kInitial; ++i) {
    const std::string key = "key-" + std::to_string(i);
    auto* found = map.Find(ComputeDigest(key), key);
    ASSERT_CHECK(found != nullptr && found->value_ == i, "lookup failed");
  }

  Digest collision{};
  auto first = map.InsertOrAssign(collision, "collision-a", 11);
  auto second = map.InsertOrAssign(collision, "collision-b", 22);
  ASSERT_CHECK(first.inserted_ && second.inserted_ &&
                   map.Find(collision, "collision-a")->value_ == 11 &&
                   map.Find(collision, "collision-b")->value_ == 22,
               "full-key collision handling failed");
  for (std::uint64_t i = 0; i < 100; ++i) {
    const std::string key = "collision-chain-" + std::to_string(i);
    map.InsertOrAssign(collision, key, i);
  }
  for (std::uint64_t i = 0; i < 100; ++i) {
    const std::string key = "collision-chain-" + std::to_string(i);
    auto* found = map.Find(collision, key);
    ASSERT_CHECK(found != nullptr && found->value_ == i,
                 "overflow-chain collision lookup failed");
  }

  std::unordered_map<std::string, unsigned> seen;
  std::uint64_t cursor = 0;
  do {
    cursor = map.Scan(
        cursor, [&](const auto& entry) { ++seen[std::string(entry.key())]; });
  } while (cursor != 0);
  ASSERT_CHECK(seen.size() == map.size(), "stable scan missed entries");
  for (const auto& [key, count] : seen) {
    (void)key;
    ASSERT_CHECK(count == 1, "stable scan returned a duplicate");
  }

  seen.clear();
  cursor =
      map.Scan(0, [&](const auto& entry) { ++seen[std::string(entry.key())]; });
  for (std::uint64_t i = kInitial; i < kInitial + 20000; ++i) {
    const std::string key = "key-" + std::to_string(i);
    map.InsertOrAssign(ComputeDigest(key), key, i);
  }
  while (cursor != 0) {
    cursor = map.Scan(
        cursor, [&](const auto& entry) { ++seen[std::string(entry.key())]; });
  }
  for (std::uint64_t i = 0; i < kInitial; ++i) {
    const std::string key = "key-" + std::to_string(i);
    ASSERT_CHECK(seen.contains(key), "expanding scan missed an existing key");
  }

  auto* stable = map.Find(ComputeDigest("key-0"), "key-0");
  auto assigned = map.InsertOrAssign(ComputeDigest("key-0"), "key-0", 42);
  ASSERT_CHECK(stable == assigned.entry_ && !assigned.inserted_,
               "assign changed the stable entry address");
  ScanHashMap<std::uint64_t> moved(std::move(map));
  ASSERT_CHECK(
      map.empty() && moved.Find(ComputeDigest("key-0"), "key-0")->value_ == 42,
      "map move failed");

  // Detach hands the whole population to the caller and leaves the source empty
  // and immediately usable. Entry addresses survive the move, which is what
  // lets callers keep raw Entry pointers into a detached population.
  const std::size_t detached_size = moved.size();
  auto* before_detach = moved.Find(ComputeDigest("key-0"), "key-0");
  ScanHashMap<std::uint64_t> detached = moved.Detach();
  ASSERT_CHECK(moved.empty() && moved.size() == 0,
               "detach left entries in the source");
  ASSERT_CHECK(
      !moved.has_allocated_storage() && detached.has_allocated_storage(),
      "detach did not transfer allocated storage");
  ASSERT_CHECK(moved.Find(ComputeDigest("key-0"), "key-0") == nullptr,
               "detached entry is still reachable from the source");
  ASSERT_CHECK(
      detached.size() == detached_size &&
          detached.Find(ComputeDigest("key-0"), "key-0") == before_detach,
      "detach did not carry the population over unchanged");

  // The source must accept a fresh population, including a key that the
  // detached one still holds.
  auto reinserted = moved.InsertOrAssign(ComputeDigest("key-0"), "key-0", 7);
  ASSERT_CHECK(reinserted.inserted_ && moved.size() == 1 &&
                   moved.Find(ComputeDigest("key-0"), "key-0")->value_ == 7 &&
                   detached.Find(ComputeDigest("key-0"), "key-0")->value_ == 42,
               "source and detached populations are not independent");

  // Erase: every other key of a fresh population, verifying removal, size,
  // survivor lookups, scan completeness, and slot reuse by re-insertion.
  ScanHashMap<std::uint64_t> erasable;
  constexpr std::uint64_t kErasePopulation = 10000;
  for (std::uint64_t i = 0; i < kErasePopulation; ++i) {
    const std::string key = "erase-" + std::to_string(i);
    erasable.InsertOrAssign(ComputeDigest(key), key, i);
  }
  for (std::uint64_t i = 0; i < kErasePopulation; i += 2) {
    const std::string key = "erase-" + std::to_string(i);
    ASSERT_CHECK(erasable.Erase(ComputeDigest(key), key), "erase failed");
  }
  ASSERT_CHECK(erasable.size() == kErasePopulation / 2,
               "erase left a wrong population count");
  for (std::uint64_t i = 0; i < kErasePopulation; ++i) {
    const std::string key = "erase-" + std::to_string(i);
    auto* found = erasable.Find(ComputeDigest(key), key);
    if (i % 2 == 0) {
      ASSERT_CHECK(found == nullptr, "erased key is still reachable");
    } else {
      ASSERT_CHECK(found != nullptr && found->value_ == i,
                   "erase disturbed a surviving key");
    }
  }
  ASSERT_CHECK(!erasable.Erase(ComputeDigest("erase-0"), "erase-0"),
               "double erase reported success");
  seen.clear();
  cursor = 0;
  do {
    cursor = erasable.Scan(
        cursor, [&](const auto& entry) { ++seen[std::string(entry.key())]; });
  } while (cursor != 0);
  ASSERT_CHECK(seen.size() == erasable.size(),
               "scan after erase missed or duplicated survivors");
  for (std::uint64_t i = 0; i < kErasePopulation; i += 2) {
    const std::string key = "erase-" + std::to_string(i);
    auto again = erasable.InsertOrAssign(ComputeDigest(key), key, i + 1);
    ASSERT_CHECK(again.inserted_, "reinsert after erase failed");
  }
  ASSERT_CHECK(erasable.size() == kErasePopulation,
               "reinsert after erase left a wrong count");

  // Erase inside one overflow chain: all keys share a digest, so they pile
  // into a single bucket chain and exercise the hole-filling compaction.
  ScanHashMap<std::uint64_t> chained;
  for (std::uint64_t i = 0; i < 64; ++i) {
    const std::string key = "chain-" + std::to_string(i);
    chained.InsertOrAssign(collision, key, i);
  }
  for (std::uint64_t i = 0; i < 64; i += 3) {
    const std::string key = "chain-" + std::to_string(i);
    ASSERT_CHECK(chained.Erase(collision, key), "chained erase failed");
  }
  for (std::uint64_t i = 0; i < 64; ++i) {
    const std::string key = "chain-" + std::to_string(i);
    auto* found = chained.Find(collision, key);
    if (i % 3 == 0) {
      ASSERT_CHECK(found == nullptr, "erased chained key is reachable");
    } else {
      ASSERT_CHECK(found != nullptr && found->value_ == i,
                   "chain compaction lost a surviving key");
    }
  }
  while (chained.size() != 0) {
    std::string victim;
    std::uint64_t drain_cursor = 0;
    do {
      drain_cursor = chained.Scan(drain_cursor, [&](const auto& entry) {
        if (victim.empty()) {
          victim = entry.key();
        }
      });
    } while (drain_cursor != 0 && victim.empty());
    ASSERT_CHECK(chained.Erase(collision, victim), "chain drain erase failed");
  }
  ASSERT_CHECK(
      chained.Find(collision, "chain-1") == nullptr && chained.size() == 0,
      "chain drain left residue");
}

TEST(ScanHashMapTest, ExternalKeyStoresOnlyDigestAndLogicalLength) {
  ScanHashMap<std::uint64_t> map;
  const std::string key(8192, 'x');
  const Digest digest = ComputeDigest(key);
  auto inserted = map.InsertOrAssign(digest, key, 42, false);
  ASSERT_TRUE(inserted.inserted_);
  EXPECT_FALSE(inserted.entry_->key_complete());
  EXPECT_TRUE(inserted.entry_->key().empty());
  EXPECT_EQ(inserted.entry_->logical_key_size(), key.size());
  EXPECT_EQ(inserted.entry_->external_key_digest(), digest);
  EXPECT_EQ(map.Find(digest, key), inserted.entry_);
  EXPECT_TRUE(map.Erase(inserted.entry_));
  EXPECT_TRUE(map.empty());
}

TEST(ScanHashMapTest, SaturatedAddressSpaceUsesBucketChains) {
  // A two-bit test table reaches the same state as production at 2^32
  // buckets, without requiring an enormous allocation.
  ScanHashMap<std::uint64_t, 2> map;
  constexpr std::uint64_t kEntries = 256;
  for (std::uint64_t i = 0; i < kEntries; ++i) {
    const std::string key = "saturated-" + std::to_string(i);
    map.InsertOrAssign(ComputeDigest(key), key, i);
  }

  EXPECT_EQ(map.allocated_bucket_count(), 4);
  EXPECT_EQ(map.size(), kEntries);
  for (std::uint64_t i = 0; i < kEntries; ++i) {
    const std::string key = "saturated-" + std::to_string(i);
    auto* found = map.Find(ComputeDigest(key), key);
    ASSERT_NE(found, nullptr);
    EXPECT_EQ(found->value_, i);
  }

  for (std::uint64_t i = 0; i < kEntries; i += 2) {
    const std::string key = "saturated-" + std::to_string(i);
    EXPECT_TRUE(map.Erase(ComputeDigest(key), key));
  }
  EXPECT_EQ(map.allocated_bucket_count(), 4);
  EXPECT_EQ(map.size(), kEntries / 2);
}

TEST(ScanHashMapTest,
     ScanDoesNotMissStableEntriesWhenMutationOccursBetweenCalls) {
  ScanHashMap<std::uint64_t> map;
  constexpr std::uint64_t kStable = 12000;
  constexpr std::uint64_t kChurn = 8000;
  for (std::uint64_t i = 0; i < kStable; ++i) {
    const std::string key = "stable-" + std::to_string(i);
    map.InsertOrAssign(ComputeDigest(key), key, i);
  }
  for (std::uint64_t i = 0; i < kChurn; ++i) {
    const std::string key = "churn-" + std::to_string(i);
    map.InsertOrAssign(ComputeDigest(key), key, i);
  }

  std::unordered_map<std::string, unsigned> seen;
  std::uint64_t cursor = 0;
  std::uint64_t round = 0;
  do {
    cursor = map.Scan(cursor, [&](const auto& entry) {
      const std::string key(entry.key());
      if (key.starts_with("stable-")) ++seen[key];
    });

    // Deleting and reinserting unrelated keys compacts bucket chains while
    // the larger insert population repeatedly advances incremental rehash.
    for (std::uint64_t i = round * 32; i < std::min(kChurn, round * 32 + 32);
         ++i) {
      const std::string key = "churn-" + std::to_string(i);
      ASSERT_TRUE(map.Erase(ComputeDigest(key), key));
    }
    for (std::uint64_t i = 0; i < 8; ++i) {
      const std::string key = "growth-" + std::to_string(round * 8 + i);
      map.InsertOrAssign(ComputeDigest(key), key, i);
    }
    ++round;
  } while (cursor != 0);

  ASSERT_EQ(seen.size(), kStable);
  for (std::uint64_t i = 0; i < kStable; ++i) {
    const std::string key = "stable-" + std::to_string(i);
    ASSERT_TRUE(seen.contains(key)) << key;
  }
}

TEST(ScanHashMapTest, ForEachWhileStopsImmediately) {
  ScanHashMap<std::uint64_t> map;
  for (std::uint64_t i = 0; i < 100; ++i) {
    const std::string key = "key-" + std::to_string(i);
    map.InsertOrAssign(ComputeDigest(key), key, i);
  }

  std::size_t visited = 0;
  EXPECT_FALSE(map.ForEachWhile([&](const auto&) {
    ++visited;
    return visited < 7;
  }));
  EXPECT_EQ(visited, 7u);

  visited = 0;
  EXPECT_TRUE(map.ForEachWhile([&](const auto&) {
    ++visited;
    return true;
  }));
  EXPECT_EQ(visited, map.size());
}

TEST(ScanHashMapTest, FairRandomEntrySamplesExistingEntries) {
  ScanHashMap<std::uint64_t> map;
  EXPECT_EQ(map.FairRandomEntry(1), nullptr);
  for (std::uint64_t i = 0; i < 1000; ++i) {
    const std::string key = "sample-" + std::to_string(i);
    map.InsertOrAssign(ComputeDigest(key), key, i);
  }
  std::unordered_map<std::uint64_t, unsigned> seen;
  for (std::uint64_t entropy = 1; entropy <= 1000; ++entropy) {
    auto* selected = map.FairRandomEntry(entropy * 0x9e3779b97f4a7c15ULL);
    ASSERT_NE(selected, nullptr);
    EXPECT_LT(selected->value_, 1000u);
    ++seen[selected->value_];
  }
  EXPECT_GT(seen.size(), 100u);
}
