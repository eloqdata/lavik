#include <cstdint>
#include <iostream>
#include <string>
#include <unordered_map>

#include "keylane/storage/format.h"
#include "keylane/storage/scan_hash_map.h"

namespace {

using keylane::storage::ComputeDigest;
using keylane::storage::Digest;
using keylane::storage::ScanHashMap;

bool Check(bool condition, const char* message) {
  if (!condition) {
    std::cerr << message << '\n';
  }
  return condition;
}

}  // namespace

int main() {
  ScanHashMap<std::uint64_t> map;
  constexpr std::uint64_t kInitial = 10000;

  for (std::uint64_t i = 0; i < kInitial; ++i) {
    const std::string key = "key-" + std::to_string(i);
    auto inserted = map.InsertOrAssign(ComputeDigest(key), key, i);
    if (!Check(inserted.inserted && inserted.entry->value == i,
               "initial insert failed")) {
      return 1;
    }
  }
  if (!Check(map.size() == kInitial, "unexpected map size")) {
    return 1;
  }

  for (std::uint64_t i = 0; i < kInitial; ++i) {
    const std::string key = "key-" + std::to_string(i);
    auto* found = map.Find(ComputeDigest(key), key);
    if (!Check(found != nullptr && found->value == i, "lookup failed")) {
      return 1;
    }
  }

  Digest collision{};
  auto first = map.InsertOrAssign(collision, "collision-a", 11);
  auto second = map.InsertOrAssign(collision, "collision-b", 22);
  if (!Check(first.inserted && second.inserted &&
                 map.Find(collision, "collision-a")->value == 11 &&
                 map.Find(collision, "collision-b")->value == 22,
             "full-key collision handling failed")) {
    return 1;
  }
  for (std::uint64_t i = 0; i < 100; ++i) {
    const std::string key = "collision-chain-" + std::to_string(i);
    map.InsertOrAssign(collision, key, i);
  }
  for (std::uint64_t i = 0; i < 100; ++i) {
    const std::string key = "collision-chain-" + std::to_string(i);
    auto* found = map.Find(collision, key);
    if (!Check(found != nullptr && found->value == i,
               "overflow-chain collision lookup failed")) {
      return 1;
    }
  }

  std::unordered_map<std::string, unsigned> seen;
  std::uint64_t cursor = 0;
  do {
    cursor = map.Scan(cursor, [&](const auto& entry) { ++seen[entry.key]; });
  } while (cursor != 0);
  if (!Check(seen.size() == map.size(), "stable scan missed entries")) {
    return 1;
  }
  for (const auto& [key, count] : seen) {
    (void)key;
    if (!Check(count == 1, "stable scan returned a duplicate")) {
      return 1;
    }
  }

  seen.clear();
  cursor = map.Scan(0, [&](const auto& entry) { ++seen[entry.key]; });
  for (std::uint64_t i = kInitial; i < kInitial + 20000; ++i) {
    const std::string key = "key-" + std::to_string(i);
    map.InsertOrAssign(ComputeDigest(key), key, i);
  }
  while (cursor != 0) {
    cursor = map.Scan(cursor,
                      [&](const auto& entry) { ++seen[entry.key]; });
  }
  for (std::uint64_t i = 0; i < kInitial; ++i) {
    const std::string key = "key-" + std::to_string(i);
    if (!Check(seen.contains(key), "expanding scan missed an existing key")) {
      return 1;
    }
  }

  auto* stable = map.Find(ComputeDigest("key-0"), "key-0");
  auto assigned = map.InsertOrAssign(ComputeDigest("key-0"), "key-0", 42);
  if (!Check(stable == assigned.entry && !assigned.inserted,
             "assign changed the stable entry address")) {
    return 1;
  }
  ScanHashMap<std::uint64_t> moved(std::move(map));
  if (!Check(map.empty() &&
                 moved.Find(ComputeDigest("key-0"), "key-0")->value == 42,
             "map move failed")) {
    return 1;
  }

  // Detach hands the whole population to the caller and leaves the source empty
  // and immediately usable. Entry addresses survive the move, which is what
  // lets callers keep raw Entry pointers into a detached population.
  const std::size_t detached_size = moved.size();
  auto* before_detach = moved.Find(ComputeDigest("key-0"), "key-0");
  ScanHashMap<std::uint64_t> detached = moved.Detach();
  if (!Check(moved.empty() && moved.size() == 0,
             "detach left entries in the source")) {
    return 1;
  }
  if (!Check(moved.Find(ComputeDigest("key-0"), "key-0") == nullptr,
             "detached entry is still reachable from the source")) {
    return 1;
  }
  if (!Check(detached.size() == detached_size &&
                 detached.Find(ComputeDigest("key-0"), "key-0") ==
                     before_detach,
             "detach did not carry the population over unchanged")) {
    return 1;
  }

  // The source must accept a fresh population, including a key that the
  // detached one still holds.
  auto reinserted = moved.InsertOrAssign(ComputeDigest("key-0"), "key-0", 7);
  if (!Check(reinserted.inserted && moved.size() == 1 &&
                 moved.Find(ComputeDigest("key-0"), "key-0")->value == 7 &&
                 detached.Find(ComputeDigest("key-0"), "key-0")->value == 42,
             "source and detached populations are not independent")) {
    return 1;
  }

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
    if (!Check(erasable.Erase(ComputeDigest(key), key), "erase failed")) {
      return 1;
    }
  }
  if (!Check(erasable.size() == kErasePopulation / 2,
             "erase left a wrong population count")) {
    return 1;
  }
  for (std::uint64_t i = 0; i < kErasePopulation; ++i) {
    const std::string key = "erase-" + std::to_string(i);
    auto* found = erasable.Find(ComputeDigest(key), key);
    if (i % 2 == 0) {
      if (!Check(found == nullptr, "erased key is still reachable")) {
        return 1;
      }
    } else if (!Check(found != nullptr && found->value == i,
                      "erase disturbed a surviving key")) {
      return 1;
    }
  }
  if (!Check(!erasable.Erase(ComputeDigest("erase-0"), "erase-0"),
             "double erase reported success")) {
    return 1;
  }
  seen.clear();
  cursor = 0;
  do {
    cursor = erasable.Scan(cursor,
                           [&](const auto& entry) { ++seen[entry.key]; });
  } while (cursor != 0);
  if (!Check(seen.size() == erasable.size(),
             "scan after erase missed or duplicated survivors")) {
    return 1;
  }
  for (std::uint64_t i = 0; i < kErasePopulation; i += 2) {
    const std::string key = "erase-" + std::to_string(i);
    auto again = erasable.InsertOrAssign(ComputeDigest(key), key, i + 1);
    if (!Check(again.inserted, "reinsert after erase failed")) {
      return 1;
    }
  }
  if (!Check(erasable.size() == kErasePopulation,
             "reinsert after erase left a wrong count")) {
    return 1;
  }

  // Erase inside one overflow chain: all keys share a digest, so they pile
  // into a single bucket chain and exercise the hole-filling compaction.
  ScanHashMap<std::uint64_t> chained;
  for (std::uint64_t i = 0; i < 64; ++i) {
    const std::string key = "chain-" + std::to_string(i);
    chained.InsertOrAssign(collision, key, i);
  }
  for (std::uint64_t i = 0; i < 64; i += 3) {
    const std::string key = "chain-" + std::to_string(i);
    if (!Check(chained.Erase(collision, key), "chained erase failed")) {
      return 1;
    }
  }
  for (std::uint64_t i = 0; i < 64; ++i) {
    const std::string key = "chain-" + std::to_string(i);
    auto* found = chained.Find(collision, key);
    if (i % 3 == 0) {
      if (!Check(found == nullptr, "erased chained key is reachable")) {
        return 1;
      }
    } else if (!Check(found != nullptr && found->value == i,
                      "chain compaction lost a surviving key")) {
      return 1;
    }
  }
  while (chained.size() != 0) {
    std::string victim;
    std::uint64_t drain_cursor = 0;
    do {
      drain_cursor = chained.Scan(drain_cursor, [&](const auto& entry) {
        if (victim.empty()) {
          victim = entry.key;
        }
      });
    } while (drain_cursor != 0 && victim.empty());
    if (!Check(chained.Erase(collision, victim),
               "chain drain erase failed")) {
      return 1;
    }
  }
  if (!Check(chained.Find(collision, "chain-1") == nullptr &&
                 chained.size() == 0,
             "chain drain left residue")) {
    return 1;
  }

  return 0;
}
