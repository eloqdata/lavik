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

  return 0;
}
