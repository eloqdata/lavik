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

#include "lavik/storage/scan_hash_map.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>

#include "lavik/memory.h"
#include "lavik/storage/format.h"

namespace {

using lavik::storage::ComputeDigest;
using lavik::storage::Digest;
using lavik::storage::ScanHashMap;
using lavik::storage::ScanHashMapEntryArena;

#define ASSERT_CHECK(condition, message) ASSERT_TRUE(condition) << message

}  // namespace

TEST(ScanHashMapTest, LookupCoversEveryFingerprintWithHolesAndCollisions) {
  using Map = ScanHashMap<std::uint64_t, 0>;
  // A saturated direct bucket puts all positions (including the final four)
  // on the lookup path. Exercise all byte values and nearby differing tags.
  for (std::uint64_t tag = 0; tag < 256; ++tag) {
    Map map;
    std::vector<Map::Entry*> entries;
    std::vector<std::uintptr_t> addresses;
    std::vector<Digest> digests;
    for (std::uint64_t slot = 0; slot < 12; ++slot) {
      const std::uint64_t actual = slot % 3 == 0 ? tag ^ 1 : tag;
      const Digest digest{(actual << 56) | slot};
      auto* entry = map.InsertNew(digest, "external", slot, false);
      ASSERT_NE(entry, nullptr);
      entries.push_back(entry);
      addresses.push_back(reinterpret_cast<std::uintptr_t>(entry));
      digests.push_back(digest);
    }
    for (std::size_t slot = 0; slot < 12; ++slot) {
      EXPECT_EQ(map.Find(digests[slot], "external"), entries[slot]);
      EXPECT_EQ(std::as_const(map).Find(digests[slot], "external"),
                entries[slot]);
      EXPECT_EQ(
          map.FindAddress(addresses[slot], Map::AddressHash(digests[slot]),
                          Map::AddressTag(digests[slot])),
          entries[slot]);
      EXPECT_EQ(map.FindCandidates(digests[slot], "external"),
                (std::vector<Map::Entry*>{entries[slot]}));
      EXPECT_EQ(map.FindCandidateIf(digests[slot], "external",
                                    [](const auto&) { return true; }),
                entries[slot]);
    }
    // Holes retain old fingerprint bytes; neither a cleared handle nor a
    // fingerprint collision may become a key match.
    for (std::size_t slot = 0; slot < 12; slot += 2) {
      ASSERT_TRUE(map.Erase(entries[slot]));
    }
    for (std::size_t slot = 0; slot < 12; ++slot) {
      EXPECT_EQ(map.Find(digests[slot], "external"),
                slot % 2 == 0 ? nullptr : entries[slot]);
      EXPECT_EQ(
          map.FindAddress(addresses[slot], Map::AddressHash(digests[slot]),
                          Map::AddressTag(digests[slot])),
          slot % 2 == 0 ? nullptr : entries[slot]);
    }
    EXPECT_EQ(map.Find(Digest{(tag << 56) | 255}, "external"), nullptr);
  }
}

TEST(ScanHashMapTest,
     TaggedAddressSurvivesChainCompactionAndRejectsStaleSlots) {
  // One bucket forces overflow chains; pairs share a tag so filtering alone
  // cannot establish identity. External keys allow controlled digest values.
  using Map = ScanHashMap<std::uint64_t, 0>;
  Map map;
  struct Saved {
    std::uintptr_t address;
    Digest digest;
  };
  std::vector<Saved> saved;
  for (std::uint64_t i = 0; i < 48; ++i) {
    const Digest digest{((i / 2) << 56) | 7};
    auto* entry = map.InsertNew(digest, "external", i, false);
    ASSERT_NE(entry, nullptr);
    saved.push_back({reinterpret_cast<std::uintptr_t>(entry), digest});
  }
  auto lookup = [&](const Saved& value) {
    return map.FindAddress(value.address, Map::AddressHash(value.digest),
                           Map::AddressTag(value.digest));
  };
  EXPECT_EQ(map.FindAddress(0, 7, 0), nullptr);
  EXPECT_EQ(map.FindAddress(1, 7, 0), nullptr);
  for (std::size_t i = 0; i < saved.size(); i += 3) {
    auto* entry = lookup(saved[i]);
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(entry->value(), i);
    ASSERT_TRUE(map.Erase(entry));
    EXPECT_EQ(lookup(saved[i]), nullptr);
  }
  for (std::size_t i = 0; i < saved.size(); ++i) {
    if (i % 3 == 0) continue;
    auto* entry = lookup(saved[i]);
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(entry->value(), i);
    EXPECT_EQ(std::as_const(map).FindAddress(saved[i].address, 7,
                                             Map::AddressTag(saved[i].digest)),
              entry);
    EXPECT_EQ(map.FindAddress(saved[i].address, 7,
                              Map::AddressTag(saved[i].digest) ^ 0x80),
              nullptr);
  }
  // The arena may reuse an erased address. A new, differently tagged key
  // must not become visible through the old receipt in that case either.
  for (std::uint64_t i = 0; i < 48; ++i) {
    ASSERT_NE(
        map.InsertNew(Digest{((128 + i) << 56) | 7}, "external", i, false),
        nullptr);
  }
  for (std::size_t i = 0; i < saved.size(); i += 3) {
    EXPECT_EQ(lookup(saved[i]), nullptr);
  }
}

TEST(ScanHashMapTest, TaggedAddressResolvesBothTablesDuringRehash) {
  using Map = ScanHashMap<std::uint64_t>;
  Map map;
  ASSERT_TRUE(map.PreallocateForExpectedSize(576));
  std::uint64_t inserted = 0;
  Map::Entry* old_entry = nullptr;
  while (!map.rehashing() && inserted < 2048) {
    auto* entry = map.InsertNew(Digest{(7ULL << 56) | inserted}, "external",
                                inserted, false);
    ASSERT_NE(entry, nullptr);
    if (inserted == 63) old_entry = entry;
    ++inserted;
  }
  ASSERT_TRUE(map.rehashing());
  ASSERT_NE(old_entry, nullptr);
  const Digest digest{(7ULL << 56) | 63};
  auto* new_entry = map.InsertNew(digest, "external", 9999, false);
  ASSERT_NE(new_entry, nullptr);
  ASSERT_TRUE(map.rehashing());
  auto check = [&] {
    for (auto* entry : {old_entry, new_entry}) {
      EXPECT_EQ(
          map.FindAddress(reinterpret_cast<std::uintptr_t>(entry),
                          Map::AddressHash(digest), Map::AddressTag(digest)),
          entry);
    }
  };
  check();
  for (unsigned i = 0; i < 512 && map.Maintain(); ++i) {
    check();
  }
  ASSERT_FALSE(map.rehashing());
  check();
}

TEST(ScanHashMapTest, CandidatePredicateOnlySeesMatchingKeys) {
  ScanHashMap<std::uint64_t> map;
  unsigned calls = 0;
  auto accept = [&](const auto&) {
    ++calls;
    return true;
  };
  EXPECT_EQ(map.FindCandidateIf(ComputeDigest("missing"), "missing", accept),
            nullptr);
  EXPECT_EQ(calls, 0);
  auto* entry = map.InsertNew(ComputeDigest("present"), "present", 42);
  ASSERT_NE(entry, nullptr);
  EXPECT_EQ(map.FindCandidateIf(ComputeDigest("missing"), "missing", accept),
            nullptr);
  EXPECT_EQ(calls, 0);
  EXPECT_EQ(map.FindCandidateIf(ComputeDigest("present"), "present", accept),
            entry);
  EXPECT_EQ(calls, 1);
  EXPECT_EQ(map.FindCandidateIf(ComputeDigest("present"), "present",
                                [](const auto&) { return false; }),
            nullptr);
  EXPECT_EQ(map.Find(ComputeDigest("present"), "present"), entry);
}

TEST(ScanHashMapTest, CandidatePredicateFiltersExternalIdentityAndStopsEarly) {
  ScanHashMap<std::uint64_t> map;
  ASSERT_TRUE(map.PreallocateForExpectedSize(576));
  const Digest digest{7};
  // External entries intentionally share the same in-memory digest/length.
  // Their values stand in for independently verified physical identities.
  for (std::uint64_t i = 0; i < 32; ++i) {
    ASSERT_NE(map.InsertNew(digest, "external", i, false), nullptr);
  }
  ASSERT_NE(map.InsertNew(digest, "different-length", 100, false), nullptr);
  auto candidates = map.FindCandidates(digest, "external");
  ASSERT_EQ(candidates.size(), 32);
  unsigned calls = 0;
  EXPECT_EQ(map.FindCandidateIf(digest, "external",
                                [&](const auto&) {
                                  ++calls;
                                  return true;
                                }),
            candidates.front());
  EXPECT_EQ(calls, 1);
  calls = 0;
  auto target = std::make_unique<std::uint64_t>(candidates[20]->value());
  EXPECT_EQ(map.FindCandidateIf(
                digest, "external",
                [wanted = std::move(target), &calls](const auto& candidate) {
                  ++calls;
                  return candidate.value() == *wanted;
                }),
            candidates[20]);
  EXPECT_EQ(calls, 21);
  calls = 0;
  EXPECT_EQ(map.FindCandidateIf(digest, "external",
                                [&](const auto&) {
                                  ++calls;
                                  return false;
                                }),
            nullptr);
  EXPECT_EQ(calls, 32);
}

TEST(ScanHashMapTest, CandidatePredicateContinuesIntoRehashTable) {
  ScanHashMap<std::uint64_t> map;
  ASSERT_TRUE(map.PreallocateForExpectedSize(576));
  std::uint64_t inserted = 0;
  while (!map.rehashing() && inserted < 2048) {
    ASSERT_NE(map.InsertNew(Digest{inserted}, "external", inserted, false),
              nullptr);
    ++inserted;
  }
  ASSERT_TRUE(map.rehashing());
  ASSERT_GT(inserted, 63);
  // Bucket 63 has not moved during the initial migration steps. Add a second
  // candidate to the new table and reject the old-table candidate first.
  auto* wanted = map.InsertNew(Digest{63}, "external", 9999, false);
  ASSERT_NE(wanted, nullptr);
  ASSERT_TRUE(map.rehashing());
  std::vector<std::uint64_t> visited;
  EXPECT_EQ(map.FindCandidateIf(Digest{63}, "external",
                                [&](const auto& entry) {
                                  visited.push_back(entry.value());
                                  return entry.value() == 9999;
                                }),
            wanted);
  EXPECT_EQ(visited, (std::vector<std::uint64_t>{63, 9999}));
  ASSERT_TRUE(map.rehashing());
  for (unsigned i = 0; i < 512 && map.Maintain(); ++i) {
  }
  ASSERT_FALSE(map.rehashing());
  EXPECT_EQ(map.FindCandidateIf(
                Digest{63}, "external",
                [](const auto& entry) { return entry.value() == 9999; }),
            wanted);
}

TEST(ScanHashMapTest, CandidatePredicateExceptionLeavesEntriesUsable) {
  ScanHashMap<std::uint64_t> map;
  auto* entry = map.InsertNew(ComputeDigest("kept"), "kept", 1);
  ASSERT_NE(entry, nullptr);
  EXPECT_THROW(map.FindCandidateIf(ComputeDigest("kept"), "kept",
                                   [](const auto&) -> bool { throw 7; }),
               int);
  EXPECT_EQ(map.Find(ComputeDigest("kept"), "kept"), entry);
  EXPECT_TRUE(map.Erase(entry));
  EXPECT_TRUE(map.empty());
}

TEST(ScanHashMapTest, EraseShrinksAndReleasesBucketsWithoutFurtherRequests) {
  ScanHashMap<std::uint64_t> map;
  ASSERT_TRUE(map.PreallocateForExpectedSize(576));
  std::vector<ScanHashMap<std::uint64_t>::Entry*> entries;
  for (std::size_t i = 0; i < 576; ++i) {
    const std::string key = "shrink-" + std::to_string(i);
    entries.push_back(map.InsertNew(ComputeDigest(key), key, i));
    ASSERT_NE(entries.back(), nullptr);
  }
  ASSERT_EQ(map.allocated_bucket_count(), 64);
  // Replacing live values with tombstone-like markers retains every slot.
  for (auto* entry : entries) entry->assign(0);
  EXPECT_FALSE(map.Maintain());
  EXPECT_EQ(map.allocated_bucket_count(), 64);
  for (std::size_t i = 1; i < entries.size(); ++i) {
    if (i % 2 == 0) {
      ASSERT_TRUE(map.Erase(entries[i]));
    } else {
      const std::string key = "shrink-" + std::to_string(i);
      ASSERT_TRUE(map.Erase(ComputeDigest(key), key));
    }
  }
  for (unsigned step = 0; step < 256 && map.Maintain(); ++step) {
  }
  EXPECT_FALSE(map.rehashing());
  EXPECT_EQ(map.allocated_bucket_count(), 1);
  EXPECT_EQ(map.FindAddress(reinterpret_cast<std::uintptr_t>(entries[0]),
                            map.AddressHash(ComputeDigest("shrink-0"))),
            entries[0]);
  EXPECT_EQ(map.FindAddress(reinterpret_cast<std::uintptr_t>(entries[0]),
                            map.AddressHash(ComputeDigest("shrink-0")),
                            map.AddressTag(ComputeDigest("shrink-0"))),
            entries[0]);
  ASSERT_TRUE(map.Erase(entries[0]));
  EXPECT_FALSE(map.has_allocated_storage());
  EXPECT_EQ(map.InsertNew(ComputeDigest("reused"), "reused", 42)->value(), 42);
}

TEST(ScanHashMapTest, ShrinkingPreservesEveryCursorPosition) {
  // Each initial bucket has one permanent entry and seven that are later
  // erased. Try every possible pause in the original reverse-bit traversal,
  // then resume before, during, and after two consecutive halvings.
  std::array<std::vector<std::string>, 8> keys;
  for (std::size_t candidate = 0, count = 0; count < 64; ++candidate) {
    std::string key = "cursor-shrink-" + std::to_string(candidate);
    auto& bucket = keys[ComputeDigest(key).value_ & 7];
    if (bucket.size() == 8) continue;
    bucket.push_back(std::move(key));
    ++count;
  }
  bool saw_duplicate = false;
  for (unsigned pause = 0; pause < 8; ++pause) {
    for (unsigned migration = 0; migration <= 16; ++migration) {
      SCOPED_TRACE(::testing::Message() << pause << "/" << migration);
      ScanHashMap<std::uint64_t> map;
      ASSERT_TRUE(map.PreallocateForExpectedSize(64));
      for (unsigned bucket = 0; bucket < 8; ++bucket) {
        for (unsigned slot = 0; slot < 8; ++slot) {
          ASSERT_NE(map.InsertNew(ComputeDigest(keys[bucket][slot]),
                                  keys[bucket][slot], slot == 0 ? bucket : 8),
                    nullptr);
        }
      }
      std::array<unsigned, 8> seen{};
      auto observe = [&](auto& entry) {
        if (entry.value() < 8) ++seen[entry.value()];
      };
      std::uint64_t cursor = 0;
      for (unsigned step = 0; step < pause; ++step) {
        cursor = map.Scan(cursor, observe);
        ASSERT_NE(cursor, 0);
      }
      for (const auto& bucket : keys) {
        for (unsigned slot = 1; slot < 8; ++slot) {
          ASSERT_TRUE(map.Erase(ComputeDigest(bucket[slot]), bucket[slot]));
        }
      }
      for (unsigned step = 0; step < migration; ++step) map.Maintain();
      unsigned scans = 0;
      do {
        cursor = map.Scan(cursor, observe);
        map.Maintain();
        ASSERT_LE(++scans, 8);
      } while (cursor != 0);
      for (unsigned count : seen) {
        EXPECT_GE(count, 1);
        saw_duplicate |= count > 1;
      }
      for (unsigned step = 0; step < 32 && map.Maintain(); ++step) {
      }
      EXPECT_EQ(map.allocated_bucket_count(), 2);
    }
  }
  EXPECT_TRUE(saw_duplicate);
}

TEST(ScanHashMapTest,
     ShrinkMergesOverflowChainsAndKeepsExternalEntryAddresses) {
  ScanHashMap<std::uint64_t> map;
  ASSERT_TRUE(map.PreallocateForExpectedSize(144));
  std::vector<ScanHashMap<std::uint64_t>::Entry*> entries;
  // Distinct external digests alternate between old buckets 0 and 8, which
  // merge into bucket 0. More than one overflow bucket is needed after merge.
  for (std::size_t i = 0; i < 40; ++i) {
    entries.push_back(map.InsertNew(Digest{i * 8}, "external", i, false));
    ASSERT_NE(entries.back(), nullptr);
  }
  ASSERT_TRUE(map.Erase(entries.back()));
  entries.pop_back();
  ASSERT_TRUE(map.rehashing());
  auto detached = map.Detach();
  ScanHashMap<std::uint64_t> moved = std::move(detached);
  for (unsigned step = 0; step < 64 && moved.Maintain(); ++step) {
  }
  ASSERT_EQ(moved.allocated_bucket_count(), 8);
  for (std::size_t i = 0; i < entries.size(); ++i) {
    EXPECT_EQ(moved.Find(Digest{i * 8}, "external"), entries[i]);
    EXPECT_EQ(entries[i]->value(), i);
  }
  EXPECT_TRUE(map.empty());
  EXPECT_FALSE(map.Maintain());
  EXPECT_TRUE(detached.empty());
  EXPECT_FALSE(detached.Maintain());
}

TEST(ScanHashMapTest, AdmissionRejectionDefersShrinkButNotErase) {
  constexpr std::uint64_t kLimit = 4 * 1024 * 1024;
  ASSERT_TRUE(lavik::InitMemoryLimit(kLimit, 1).ok());
  lavik::BindMemoryAccountingShard(0);
  struct RestoreLimit {
    ~RestoreLimit() {
      (void)lavik::InitMemoryLimit(1024ULL * 1024 * 1024, 1);
      lavik::BindMemoryAccountingShard(lavik::kMaxMemoryWorkers);
    }
  } restore;
  // Cover both allocator-owned admission and the primary-index domain whose
  // foreground writes normally have an enclosing storage admission permit.
  for (bool externally_admitted : {false, true}) {
    auto arena = std::make_shared<ScanHashMapEntryArena>(
        ScanHashMapEntryArena::kMaximumPageId, externally_admitted);
    ScanHashMap<std::uint64_t> map(arena);
    ASSERT_TRUE(map.PreallocateForExpectedSize(576));
    ASSERT_NE(map.InsertNew(ComputeDigest("keep"), "keep", 1), nullptr);
    auto* removed = map.InsertNew(ComputeDigest("remove"), "remove", 2);
    ASSERT_NE(removed, nullptr);
    const auto memory = lavik::GetWorkerMemoryStats(0);
    auto blocker = lavik::TryReserveMemory(memory.retained_limit_bytes_ -
                                           memory.retained_bytes_);
    ASSERT_TRUE(blocker.has_value());
    EXPECT_TRUE(map.Erase(removed));
    EXPECT_EQ(map.size(), 1);
    EXPECT_EQ(map.allocated_bucket_count(), 64);
    EXPECT_FALSE(map.rehashing());
    EXPECT_FALSE(map.Maintain());
    EXPECT_NE(std::as_const(map).Find(ComputeDigest("keep"), "keep"), nullptr);
    blocker.reset();
    for (unsigned step = 0; step < 256 && map.Maintain(); ++step) {
    }
    EXPECT_EQ(map.allocated_bucket_count(), 1);
    EXPECT_FALSE(map.rehashing());
  }
}

TEST(ScanHashMapTest, ShrinkMergeRetriesOverflowAdmissionWithoutLosingEntries) {
  ASSERT_TRUE(lavik::InitMemoryLimit(4 * 1024 * 1024, 1).ok());
  lavik::BindMemoryAccountingShard(0);
  struct RestoreLimit {
    ~RestoreLimit() {
      (void)lavik::InitMemoryLimit(1024ULL * 1024 * 1024, 1);
      lavik::BindMemoryAccountingShard(lavik::kMaxMemoryWorkers);
    }
  } restore;
  for (bool externally_admitted : {false, true}) {
    auto arena = std::make_shared<ScanHashMapEntryArena>(
        ScanHashMapEntryArena::kMaximumPageId, externally_admitted);
    ScanHashMap<std::uint64_t> map(arena);
    ASSERT_TRUE(map.PreallocateForExpectedSize(144));
    for (unsigned i = 0; i < 24; ++i) {
      ASSERT_NE(map.InsertNew(Digest{i * 8}, "external", i, false), nullptr);
    }
    ASSERT_TRUE(map.Erase(Digest{23 * 8}, "external"));
    // Migrate old buckets 0..7. Bucket 8 needs to merge eleven entries into
    // the already full destination chain, so no source slot may be cleared
    // until the overflow allocation (including metadata) has been admitted.
    for (unsigned i = 0; i < 8; ++i) ASSERT_TRUE(map.Maintain());
    const auto memory = lavik::GetWorkerMemoryStats(0);
    auto blocker = lavik::TryReserveMemory(memory.retained_limit_bytes_ -
                                           memory.retained_bytes_);
    ASSERT_TRUE(blocker.has_value());
    EXPECT_FALSE(map.Maintain());
    EXPECT_TRUE(map.rehashing());
    std::array<unsigned, 23> seen{};
    std::uint64_t cursor = 0;
    do {
      cursor = map.Scan(cursor, [&](auto& entry) { ++seen[entry.value()]; });
    } while (cursor != 0);
    for (unsigned count : seen) EXPECT_EQ(count, 1);
    blocker.reset();
    for (unsigned i = 0; i < 64 && map.Maintain(); ++i) {
    }
    EXPECT_FALSE(map.rehashing());
    EXPECT_EQ(map.allocated_bucket_count(), 4);
    for (unsigned i = 0; i < 23; ++i) {
      auto* entry = map.Find(Digest{i * 8}, "external");
      ASSERT_NE(entry, nullptr);
      EXPECT_EQ(entry->value(), i);
    }
  }
}

TEST(ScanHashMapTest, ScanSurvivesAlternatingGrowthAndShrinkWithNewWrites) {
  ScanHashMap<std::uint64_t> map;
  constexpr unsigned kStable = 8;
  for (unsigned i = 0; i < kStable; ++i) {
    const std::string key = "stable-" + std::to_string(i);
    ASSERT_NE(map.InsertOrAssign(ComputeDigest(key), key, i).entry_, nullptr);
  }
  for (unsigned round = 0; round < 16; ++round) {
    std::array<unsigned, kStable> seen{};
    std::uint64_t cursor = 0;
    unsigned step = 0;
    do {
      cursor = map.Scan(cursor, [&](auto& entry) {
        if (entry.value() < kStable) ++seen[entry.value()];
      });
      // Insert while any previous shrink is still active, then remove the
      // temporary population while expansion may still be migrating.
      for (unsigned i = 0; i < 300; ++i) {
        const std::string key = "churn-" + std::to_string(i);
        ASSERT_NE(map.InsertOrAssign(ComputeDigest(key), key, kStable).entry_,
                  nullptr);
      }
      for (unsigned i = 0; i < 300; ++i) {
        const std::string key = "churn-" + std::to_string(i);
        ASSERT_TRUE(map.Erase(ComputeDigest(key), key));
      }
      for (unsigned i = 0; i < round; ++i) map.Maintain();
      ASSERT_LE(++step, 128);
    } while (cursor != 0);
    for (unsigned count : seen) EXPECT_GE(count, 1);
  }
}

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
  EXPECT_EQ(first.InsertNew(ComputeDigest("overflow"), "overflow", 1), nullptr);

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

TEST(ScanHashMapTest, PreallocatesSettledTableForKnownPopulation) {
  ScanHashMap<std::uint64_t> map;
  ASSERT_TRUE(map.PreallocateForExpectedSize(91));
  EXPECT_EQ(map.allocated_bucket_count(), 16);
  EXPECT_FALSE(map.rehashing());

  for (std::size_t i = 0; i < 91; ++i) {
    const std::string key = "preallocated-" + std::to_string(i);
    map.InsertNew(ComputeDigest(key), key, i);
    EXPECT_FALSE(map.rehashing());
    EXPECT_EQ(map.allocated_bucket_count(), 16);
  }
  EXPECT_EQ(map.size(), 91);
}

TEST(ScanHashMapTest, KnownEmptyPopulationAllocatesNothing) {
  ScanHashMap<std::uint64_t> map;
  EXPECT_TRUE(map.PreallocateForExpectedSize(0));
  EXPECT_FALSE(map.has_allocated_storage());
}

TEST(ScanHashMapTest, UnrepresentablePreallocationLeavesMapEmpty) {
  ScanHashMap<std::uint64_t, 0> map;
  EXPECT_FALSE(map.PreallocateForExpectedSize(10));
  EXPECT_TRUE(map.empty());
  EXPECT_FALSE(map.has_allocated_storage());
}

TEST(ScanHashMapTest, RetainedArenaAccountingFollowsStorageLifetime) {
  ASSERT_TRUE(lavik::InitMemoryLimit(64 * 1024 * 1024, 1).ok());
  lavik::BindMemoryAccountingShard(0);
  const std::int64_t before = lavik::WorkerMemoryAccountingBytes(0);

  {
    auto arena = std::make_shared<ScanHashMapEntryArena>();
    ScanHashMap<std::uint64_t> map(arena);
    map.InsertNew(ComputeDigest("retained"), "retained", 1);
    EXPECT_GT(lavik::WorkerMemoryAccountingBytes(0), before);
  }

  EXPECT_EQ(lavik::WorkerMemoryAccountingBytes(0), before);
  lavik::BindMemoryAccountingShard(lavik::kMaxMemoryWorkers);
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
  ASSERT_TRUE(lavik::InitMemoryLimit(kLimit, 1).ok());
  lavik::BindMemoryAccountingShard(0);

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

  lavik::RefreshMemoryStats();
  const std::uint64_t used = lavik::GetMemoryStats().used_bytes_;
  const std::uint64_t steady_limit = kLimit - kLimit / 10;
  ASSERT_LT(used, steady_limit);
  auto blocker = lavik::TryReserveMemory(steady_limit - used);
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
  ASSERT_TRUE(lavik::InitMemoryLimit(1024ULL * 1024 * 1024, 1).ok());
  lavik::BindMemoryAccountingShard(lavik::kMaxMemoryWorkers);
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

TEST(ScanHashMapTest, InlineShortKeyComparisonCoversEveryByte) {
  ScanHashMap<std::uint64_t> map;
  for (const std::size_t length : {7u, 8u, 9u, 15u, 16u, 17u}) {
    std::string key(length, 'a');
    const Digest digest = ComputeDigest(key);
    auto inserted = map.InsertNew(digest, key, length);
    ASSERT_NE(inserted, nullptr);
    EXPECT_EQ(map.Find(digest, key), inserted);

    for (const std::size_t offset : {std::size_t{0}, length / 2, length - 1}) {
      std::string different = key;
      different[offset] = 'b';
      // Supply the stored digest deliberately: a digest mismatch would hide a
      // byte-comparison hole rather than exercise it.
      EXPECT_EQ(map.Find(digest, different), nullptr)
          << "length=" << length << " offset=" << offset;
    }
  }
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

TEST(ScanHashMapTest, ConstAndMutableLookupCrossBucketBoundaries) {
  // One primary bucket forces both lookup continuations through several
  // overflow buckets. Vary key sizes to cover inline and libc comparisons,
  // as well as one-byte and multi-byte length metadata.
  ScanHashMap<std::uint64_t, 0> map;
  constexpr std::size_t lengths[] = {7, 8, 16, 17, 127, 128, 16384};
  std::vector<std::string> keys;
  for (std::size_t i = 0; i < 48; ++i) {
    std::string key = "key-" + std::to_string(i);
    key.resize(lengths[i % std::size(lengths)], 'x');
    ASSERT_NE(map.InsertNew(ComputeDigest(key), key, i), nullptr);
    keys.push_back(std::move(key));
  }
  auto check = [&](std::size_t erased_stride) {
    for (std::size_t i = 0; i < keys.size(); ++i) {
      const auto digest = ComputeDigest(keys[i]);
      const auto* entry = std::as_const(map).Find(digest, keys[i]);
      EXPECT_EQ(map.Find(digest, keys[i]), entry);
      if (erased_stride != 0 && i % erased_stride == 0) {
        EXPECT_EQ(entry, nullptr);
      } else {
        ASSERT_NE(entry, nullptr);
        EXPECT_EQ(entry->value(), i);
      }
    }
  };
  check(0);
  for (std::size_t i = 0; i < keys.size(); i += 3) {
    ASSERT_TRUE(map.Erase(ComputeDigest(keys[i]), keys[i]));
  }
  check(3);
}

TEST(ScanHashMapTest, CandidateExceptionInOverflowPreservesTraversal) {
  ScanHashMap<std::uint64_t, 0> map;
  const Digest digest{0xff00000000000007ULL};
  for (std::uint64_t i = 0; i < 36; ++i) {
    ASSERT_NE(map.InsertNew(digest, "external", i, false), nullptr);
  }
  const auto before = map.FindCandidates(digest, "external");
  unsigned calls = 0;
  EXPECT_THROW(map.FindCandidateIf(digest, "external",
                                   [&](const auto&) -> bool {
                                     if (++calls == 13) throw 7;
                                     return false;
                                   }),
               int);
  EXPECT_EQ(calls, 13);
  EXPECT_EQ(map.FindCandidates(digest, "external"), before);
  EXPECT_EQ(map.FindCandidateIf(
                digest, "external",
                [](const auto& entry) { return entry.value() == 35; }),
            before.back());
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
