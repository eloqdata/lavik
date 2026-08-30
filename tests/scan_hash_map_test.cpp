#include "keylane/storage/scan_hash_map.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>

#include "keylane/memory.h"
#include "keylane/storage/format.h"

namespace {

using keylane::storage::ComputeDigest;
using keylane::storage::Digest;
using keylane::storage::ScanHashMap;
using keylane::storage::ScanHashMapEntryArena;

#define ASSERT_CHECK(condition, message) ASSERT_TRUE(condition) << message

}  // namespace

TEST(ScanHashMapTest, SharedArenaReusesSlotsAndRejectsPageIdExhaustion) {
  auto arena = std::make_shared<ScanHashMapEntryArena>(1);
  ScanHashMap<std::uint64_t> first(arena);
  ScanHashMap<std::uint64_t> second(arena);

  // These short-key entries use the 32-byte class. One 64 KiB page has 2,046
  // slots after its 64-byte header; both maps must consume that same page.
  constexpr std::size_t kSlotsPerPage = 2046;
  for (std::size_t i = 0; i < kSlotsPerPage; ++i) {
    const std::string key = "k" + std::to_string(i);
    auto& map = i % 2 == 0 ? first : second;
    map.InsertNew(ComputeDigest(key), key, i);
  }
  EXPECT_EQ(arena->allocated_pages(), 1);
  EXPECT_FALSE(first.CanAllocateEntry("overflow", true, false));
  EXPECT_THROW(first.InsertNew(ComputeDigest("overflow"), "overflow", 1),
               std::bad_alloc);

  EXPECT_TRUE(first.Erase(ComputeDigest("k0"), "k0"));
  EXPECT_TRUE(first.CanAllocateEntry("replacement", true, false));
  first.InsertNew(ComputeDigest("replacement"), "replacement", 1);
  EXPECT_EQ(arena->allocated_pages(), 1);

  first.Clear();
  second.Clear();
  EXPECT_EQ(arena->allocated_pages(), 0);
  EXPECT_TRUE(first.CanAllocateEntry("after-clear", true, false));
  first.InsertNew(ComputeDigest("after-clear"), "after-clear", 1);
  EXPECT_EQ(arena->allocated_pages(), 1);
}

TEST(ScanHashMapTest, ReportsPhysicalAllocationOnlyOnSlowPath) {
  auto arena = std::make_shared<ScanHashMapEntryArena>();
  ScanHashMap<std::uint64_t> map(arena);
  const Digest first = ComputeDigest("first");
  const std::size_t first_allocation =
      map.RequiredAllocationBytes(first, "first", true, false, true);
  EXPECT_GE(first_allocation, ScanHashMapEntryArena::kSpanBytes);

  map.InsertNew(first, "first", 1);
  EXPECT_EQ(arena->allocated_pages(), 1);
  const Digest second = ComputeDigest("second");
  EXPECT_EQ(map.RequiredAllocationBytes(second, "second", true, false, true),
            0);

  map.Clear();
  EXPECT_EQ(arena->allocated_pages(), 0);
  const std::size_t after_directory_allocation =
      map.RequiredAllocationBytes(second, "second", true, false, true);
  EXPECT_GE(after_directory_allocation, ScanHashMapEntryArena::kSpanBytes);
  // The first slow path admits the initial raw 32 KiB directory array.
  // Clearing entries retains that array for reuse, so the next span no longer
  // pays or initializes directory capacity.
  EXPECT_GE(first_allocation, after_directory_allocation + 32 * 1024);
}

TEST(ScanHashMapTest, DirectoryCapacityDoesNotInflateEmptyArena) {
  // The complete handle namespace needs 16 MiB of descriptors, but the raw
  // array starts at 32 KiB and grows only as page IDs are consumed. An empty
  // map must not embed that maximum in every arena instance.
  EXPECT_LT(sizeof(ScanHashMapEntryArena), 1024);
}

TEST(ScanHashMapTest, RetainedArenaAccountingFollowsStorageLifetime) {
  ASSERT_TRUE(keylane::InitMemoryLimit(64 * 1024 * 1024, 1).ok());
  keylane::BindMemoryAccountingShard(0);
  const std::int64_t before = keylane::WorkerMemoryAccountingBytes(0);

  {
    auto arena = std::make_shared<ScanHashMapEntryArena>();
    ScanHashMap<std::uint64_t> map(arena);
    map.InsertNew(ComputeDigest("retained"), "retained", 1);
    EXPECT_GT(keylane::WorkerMemoryAccountingBytes(0), before);
  }

  EXPECT_EQ(keylane::WorkerMemoryAccountingBytes(0), before);
  keylane::BindMemoryAccountingShard(keylane::kMaxMemoryWorkers);
}

TEST(ScanHashMapTest, AccountsForOverflowPoolVectorGrowth) {
  auto arena = std::make_shared<ScanHashMapEntryArena>();
  ScanHashMap<std::uint64_t, 0> map(arena);
  constexpr std::size_t kDirectAndOverflowEntries = 12 * (1 + 512);
  for (std::size_t i = 0; i < kDirectAndOverflowEntries; ++i) {
    const std::string key = "k" + std::to_string(i);
    map.InsertNew(Digest{0}, key, i);
  }

  // The next child crosses both vectors' 512-element capacity boundary. Its
  // admission must include those backing reallocations, not only one bucket.
  const std::string next = "next";
  EXPECT_GT(map.RequiredAllocationBytes(Digest{0}, next, true, false, true),
            8ULL * 1024);
}

TEST(ScanHashMapTest, AmortizesAlignmentAcrossSixteenLogicalPages) {
  ScanHashMapEntryArena arena;
  std::vector<ScanHashMapEntryArena::Handle> handles;
  constexpr std::size_t kSlotsPerPage =
      (ScanHashMapEntryArena::kPageBytes - 64) / 4096;
  handles.reserve(ScanHashMapEntryArena::kPagesPerSpan * kSlotsPerPage);

  for (std::size_t page = 0; page < ScanHashMapEntryArena::kPagesPerSpan;
       ++page) {
    for (std::size_t slot = 0; slot < kSlotsPerPage; ++slot) {
      handles.push_back(arena.Allocate(4096).handle_);
    }
    EXPECT_EQ(arena.allocated_spans(), 1);
    if (page + 1 < ScanHashMapEntryArena::kPagesPerSpan) {
      EXPECT_LT(arena.AllocationBytesIfNewPage(4096),
                ScanHashMapEntryArena::kSpanBytes);
    }
  }
  EXPECT_GE(arena.AllocationBytesIfNewPage(4096),
            ScanHashMapEntryArena::kSpanBytes);
  EXPECT_EQ(arena.allocated_pages(), ScanHashMapEntryArena::kPagesPerSpan);
  EXPECT_EQ(arena.allocated_spans(), 1);

  for (auto handle = handles.rbegin(); handle != handles.rend(); ++handle) {
    arena.Deallocate(*handle);
  }
  EXPECT_EQ(arena.allocated_pages(), 0);
  EXPECT_EQ(arena.allocated_spans(), 0);
}

TEST(ScanHashMapTest, RehashDefersOverflowAllocationWhenAdmissionIsFull) {
  constexpr std::size_t kLimit = 4 * 1024 * 1024;
  ASSERT_TRUE(keylane::InitMemoryLimit(kLimit, 1).ok());
  keylane::BindMemoryAccountingShard(0);

  std::vector<std::pair<Digest, std::string>> colliding;
  for (std::uint64_t candidate = 0; colliding.size() < 19; ++candidate) {
    std::string key = "rehash-collision-" + std::to_string(candidate);
    Digest digest = ComputeDigest(key);
    if ((digest.value_ & 3) == 0) {
      colliding.emplace_back(digest, std::move(key));
    }
  }

  ScanHashMap<std::uint64_t> map;
  for (std::size_t index = 0; index < 10; ++index) {
    map.InsertNew(colliding[index].first, colliding[index].second, index);
  }
  ASSERT_TRUE(map.rehashing());
  ASSERT_NE(map.Find(colliding[0].first, colliding[0].second), nullptr);
  ASSERT_FALSE(map.rehashing());

  for (std::size_t index = 10; index < colliding.size(); ++index) {
    map.InsertNew(colliding[index].first, colliding[index].second, index);
  }
  ASSERT_TRUE(map.rehashing());

  keylane::RefreshMemoryStats();
  const std::uint64_t used = keylane::GetMemoryStats().used_bytes_;
  const std::uint64_t steady_limit = kLimit - kLimit / 10;
  ASSERT_LT(used, steady_limit);
  auto blocker = keylane::TryReserveMemory(steady_limit - used);
  ASSERT_TRUE(blocker.has_value());
  EXPECT_NE(map.Find(colliding[0].first, colliding[0].second), nullptr);
  EXPECT_TRUE(map.rehashing());

  blocker.reset();
  EXPECT_NE(map.Find(colliding[0].first, colliding[0].second), nullptr);
  EXPECT_TRUE(map.rehashing());
  EXPECT_NE(map.Find(colliding[0].first, colliding[0].second), nullptr);
  EXPECT_FALSE(map.rehashing());
  EXPECT_EQ(map.size(), colliding.size());

  // This test deliberately fills a small global admission limit. Restore a
  // normal limit before the following allocator-backed tests run in the same
  // process; rebinding the thread alone does not reset process policy.
  ASSERT_TRUE(keylane::InitMemoryLimit(1024ULL * 1024 * 1024, 1).ok());
  keylane::BindMemoryAccountingShard(keylane::kMaxMemoryWorkers);
}

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

  const Digest first_digest = ComputeDigest("additional-a");
  const Digest second_digest = ComputeDigest("additional-b");
  auto first = map.InsertOrAssign(first_digest, "additional-a", 11);
  auto second = map.InsertOrAssign(second_digest, "additional-b", 22);
  ASSERT_CHECK(first.inserted_ && second.inserted_ &&
                   map.Find(first_digest, "additional-a")->value_ == 11 &&
                   map.Find(second_digest, "additional-b")->value_ == 22,
               "additional key lookup failed");
  for (std::uint64_t i = 0; i < 100; ++i) {
    const std::string key = "collision-chain-" + std::to_string(i);
    map.InsertOrAssign(ComputeDigest(key), key, i);
  }
  for (std::uint64_t i = 0; i < 100; ++i) {
    const std::string key = "collision-chain-" + std::to_string(i);
    auto* found = map.Find(ComputeDigest(key), key);
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

  // A zero-bit direct table sends every real digest to one overflow chain,
  // exercising hole-filling compaction without violating the digest contract.
  ScanHashMap<std::uint64_t, 0> chained;
  for (std::uint64_t i = 0; i < 64; ++i) {
    const std::string key = "chain-" + std::to_string(i);
    chained.InsertOrAssign(ComputeDigest(key), key, i);
  }
  for (std::uint64_t i = 0; i < 64; i += 3) {
    const std::string key = "chain-" + std::to_string(i);
    ASSERT_CHECK(chained.Erase(ComputeDigest(key), key),
                 "chained erase failed");
  }
  for (std::uint64_t i = 0; i < 64; ++i) {
    const std::string key = "chain-" + std::to_string(i);
    auto* found = chained.Find(ComputeDigest(key), key);
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
    ASSERT_CHECK(chained.Erase(ComputeDigest(victim), victim),
                 "chain drain erase failed");
  }
  ASSERT_CHECK(chained.Find(ComputeDigest("chain-1"), "chain-1") == nullptr &&
                   chained.size() == 0,
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
  EXPECT_EQ(inserted.entry_->key_metadata_bytes(), 3);
  EXPECT_EQ(inserted.entry_->external_key_digest(), digest);
  const std::uintptr_t entry_address =
      reinterpret_cast<std::uintptr_t>(inserted.entry_);
  const std::uint32_t address_hash =
      ScanHashMap<std::uint64_t>::AddressHash(*inserted.entry_);
  EXPECT_EQ(map.FindAddress(entry_address, address_hash), inserted.entry_);
  EXPECT_EQ(map.Find(digest, key), inserted.entry_);
  EXPECT_TRUE(map.Erase(inserted.entry_));
  EXPECT_EQ(map.FindAddress(entry_address, address_hash), nullptr);
  EXPECT_TRUE(map.empty());
}

TEST(ScanHashMapTest, KeyMetadataVarintUsesExpectedWidths) {
  using Entry = ScanHashMap<std::uint64_t>::Entry;
  EXPECT_EQ(Entry::KeyMetadataBytesFor(0, true), 1);
  EXPECT_EQ(Entry::KeyMetadataBytesFor(63, true), 1);
  EXPECT_EQ(Entry::KeyMetadataBytesFor(64, true), 2);
  EXPECT_EQ(Entry::KeyMetadataBytesFor(8191, false), 2);
  EXPECT_EQ(Entry::KeyMetadataBytesFor(8192, false), 3);
  EXPECT_EQ(Entry::KeyMetadataBytesFor(1'048'575, true), 3);
  EXPECT_EQ(Entry::KeyMetadataBytesFor(1'048'576, true), 4);
  EXPECT_EQ(Entry::KeyMetadataBytesFor(134'217'727, false), 4);
  EXPECT_EQ(Entry::KeyMetadataBytesFor(134'217'728, false), 5);
  EXPECT_EQ(Entry::KeyMetadataBytesFor(Entry::kMaxLogicalKeySize, true), 5);

  ScanHashMap<std::uint64_t> map;
  for (const std::size_t length :
       {std::size_t{0}, std::size_t{1}, std::size_t{63}, std::size_t{64},
        std::size_t{8191}, std::size_t{8192}}) {
    const std::string key(length, static_cast<char>('a' + length % 26));
    const Digest digest = ComputeDigest(key);
    auto inserted = map.InsertOrAssign(digest, key, length);
    ASSERT_TRUE(inserted.inserted_);
    EXPECT_TRUE(inserted.entry_->key_complete());
    EXPECT_EQ(inserted.entry_->logical_key_size(), length);
    EXPECT_EQ(inserted.entry_->key(), key);
    EXPECT_EQ(inserted.entry_->key_metadata_bytes(),
              Entry::KeyMetadataBytesFor(length, true));
    EXPECT_EQ(map.Find(digest, key), inserted.entry_);
  }
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

TEST(ScanHashMapTest, StableScanResumesWithoutRepeatingEntries) {
  ScanHashMap<std::uint64_t> map;
  constexpr std::uint64_t kEntries = 4096;
  for (std::uint64_t i = 0; i < kEntries; ++i) {
    const std::string key = "stable-batch-" + std::to_string(i);
    // InsertNew intentionally leaves an incremental expansion in progress,
    // exercising traversal across both stable tables and long bucket chains.
    map.InsertNew(ComputeDigest(key), key, i);
  }

  ScanHashMap<std::uint64_t>::StableScanCursor cursor;
  std::unordered_map<std::string, unsigned> seen;
  bool exhausted = false;
  std::size_t batches = 0;
  while (!exhausted) {
    std::size_t batch_entries = 0;
    exhausted = map.ScanStableWhile(&cursor, [&](const auto& entry) {
      ++seen[std::string(entry.key())];
      ++batch_entries;
      return batch_entries < 17;
    });
    EXPECT_LE(batch_entries, 17u);
    ++batches;
  }

  EXPECT_TRUE(cursor.finished());
  EXPECT_GT(batches, 1u);
  ASSERT_EQ(seen.size(), map.size());
  for (const auto& [key, count] : seen) {
    (void)key;
    EXPECT_EQ(count, 1u);
  }

  ScanHashMap<std::uint64_t> empty;
  ScanHashMap<std::uint64_t>::StableScanCursor empty_cursor;
  EXPECT_TRUE(
      empty.ScanStableWhile(&empty_cursor, [](const auto&) { return false; }));
  EXPECT_TRUE(empty_cursor.finished());
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
