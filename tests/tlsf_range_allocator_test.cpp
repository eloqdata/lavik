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

#include "lavik/tlsf_range_allocator.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <limits>
#include <numeric>
#include <random>
#include <span>
#include <vector>

#include "gtest/gtest.h"

namespace lavik {
namespace {

using Allocation = TlsfRangeAllocator::Allocation;
using Metadata = TlsfRangeAllocator::Metadata;

TEST(TlsfRangeAllocatorTest, EmptyAndInvalidRequestsLeaveCapacityUnchanged) {
  TlsfRangeAllocator allocator;
  EXPECT_FALSE(allocator.Allocate(1));
  ASSERT_TRUE(allocator.Init({}));
  EXPECT_EQ(allocator.capacity(), 0U);
  EXPECT_FALSE(allocator.Allocate(0));
  EXPECT_FALSE(allocator.Allocate(1));

  std::array<Metadata, 16> metadata;
  ASSERT_TRUE(allocator.Init(metadata));
  EXPECT_FALSE(allocator.Allocate(0));
  EXPECT_FALSE(allocator.Allocate(17));
  EXPECT_FALSE(allocator.Allocate(std::numeric_limits<std::uint32_t>::max()));
  EXPECT_EQ(allocator.free_units(), 16U);
  const auto all = allocator.Allocate(16);
  ASSERT_TRUE(all);
  EXPECT_EQ(all.offset, 0U);
  EXPECT_EQ(all.units, 16U);
  EXPECT_FALSE(allocator.Allocate(1));
  allocator.Free(all);
  EXPECT_EQ(allocator.free_units(), 16U);
}

TEST(TlsfRangeAllocatorTest, SplitsExactSizesInsteadOfRoundingToPowersOfTwo) {
  std::array<Metadata, 16> metadata;
  TlsfRangeAllocator allocator;
  ASSERT_TRUE(allocator.Init(metadata));
  const auto six = allocator.Allocate(6);
  const auto ten = allocator.Allocate(10);
  ASSERT_TRUE(six);
  ASSERT_TRUE(ten);
  EXPECT_EQ(six.offset, 0U);
  EXPECT_EQ(six.units, 6U);
  EXPECT_EQ(ten.offset, 6U);
  EXPECT_EQ(ten.units, 10U);
  EXPECT_EQ(allocator.free_units(), 0U);
  allocator.Free(six);
  EXPECT_FALSE(allocator.Allocate(7));
  allocator.Free(ten);
  const auto all = allocator.Allocate(16);
  ASSERT_TRUE(all);
  EXPECT_EQ(all.offset, 0U);
  allocator.Free(all);
}

TEST(TlsfRangeAllocatorTest, CoalescesInEveryFreeOrder) {
  constexpr std::array<std::uint32_t, 4> sizes{1, 6, 32, 257};
  std::array<unsigned, 4> order{0, 1, 2, 3};
  std::vector<Metadata> metadata(
      std::accumulate(sizes.begin(), sizes.end(), 0U));
  do {
    TlsfRangeAllocator allocator;
    ASSERT_TRUE(allocator.Init(metadata));
    std::array<Allocation, 4> allocations;
    for (unsigned i = 0; i < sizes.size(); ++i) {
      allocations[i] = allocator.Allocate(sizes[i]);
      ASSERT_TRUE(allocations[i]);
    }
    std::uint32_t free = 0;
    for (unsigned i : order) {
      allocator.Free(allocations[i]);
      free += sizes[i];
      ASSERT_EQ(allocator.free_units(), free);
    }
    const auto all = allocator.Allocate(allocator.capacity());
    ASSERT_TRUE(all);
    EXPECT_EQ(all.offset, 0U);
    allocator.Free(all);
  } while (std::next_permutation(order.begin(), order.end()));
}

TEST(TlsfRangeAllocatorTest, CoalescingRemovesMiddleAndTailFreeListEntries) {
  std::array<Metadata, 21> metadata;
  TlsfRangeAllocator allocator;
  ASSERT_TRUE(allocator.Init(metadata));
  std::array<Allocation, 3> blocks;
  std::array<Allocation, 3> guards;
  for (unsigned i = 0; i < blocks.size(); ++i) {
    blocks[i] = allocator.Allocate(6);
    guards[i] = allocator.Allocate(1);
    ASSERT_TRUE(blocks[i]);
    ASSERT_TRUE(guards[i]);
  }
  for (auto block : blocks) allocator.Free(block);
  allocator.Free(guards[0]);
  allocator.Free(guards[1]);
  allocator.Free(guards[2]);
  const auto all = allocator.Allocate(21);
  ASSERT_TRUE(all);
  EXPECT_EQ(all.offset, 0U);
  allocator.Free(all);
}

TEST(TlsfRangeAllocatorTest, SameClassRemainderPreservesFreeListLinks) {
  std::array<Metadata, 204> metadata;
  TlsfRangeAllocator allocator;
  ASSERT_TRUE(allocator.Init(metadata));
  const auto first = allocator.Allocate(101);
  const auto guard = allocator.Allocate(1);
  const auto second = allocator.Allocate(101);
  const auto tail = allocator.Allocate(1);
  ASSERT_TRUE(first && guard && second && tail);
  allocator.Free(first);
  allocator.Free(second);

  // 101 and 100 share a class. Cutting one unit replaces its head with the
  // remainder; the older entry must point back to that new head when removed.
  const auto cut = allocator.Allocate(1);
  ASSERT_TRUE(cut);
  EXPECT_EQ(cut.offset, second.offset);
  allocator.Free(guard);
  allocator.Free(cut);
  allocator.Free(tail);
  const auto whole = allocator.Allocate(204);
  ASSERT_TRUE(whole);
  EXPECT_EQ(whole.offset, 0U);
  allocator.Free(whole);
}

TEST(TlsfRangeAllocatorTest, CrossesSizeClassBoundariesAndRecoversWholePool) {
  constexpr std::array<std::uint32_t, 35> requests{
      1,    2,    3,    30,   31,   32,   33,   62,   63,   64,   65,  95,
      96,   97,   127,  128,  129,  255,  256,  257,  511,  512,  513, 1023,
      1024, 1025, 2047, 2048, 2049, 4095, 4096, 4097, 8190, 8191, 8192};
  std::vector<Metadata> metadata(8192);
  TlsfRangeAllocator allocator;
  ASSERT_TRUE(allocator.Init(metadata));
  for (std::uint32_t units : requests) {
    SCOPED_TRACE(units);
    const auto first = allocator.Allocate(units);
    ASSERT_TRUE(first);
    EXPECT_EQ(first.units, units);
    const auto remaining = allocator.free_units();
    const auto second = allocator.Allocate(remaining);
    if (remaining != 0) {
      ASSERT_TRUE(second);
    }
    EXPECT_EQ(allocator.free_units(), 0U);
    allocator.Free(first);
    if (second) allocator.Free(second);
    const auto whole = allocator.Allocate(8192);
    ASSERT_TRUE(whole);
    EXPECT_EQ(whole.offset, 0U);
    allocator.Free(whole);
  }
}

TEST(TlsfRangeAllocatorTest, AdvancesPastLastSecondaryBitToLargerClass) {
  std::array<Metadata, 256> metadata;
  TlsfRangeAllocator allocator;
  ASSERT_TRUE(allocator.Init(metadata));
  const auto small = allocator.Allocate(126);
  const auto guard = allocator.Allocate(1);
  const auto large = allocator.Allocate(128);
  const auto tail = allocator.Allocate(1);
  ASSERT_TRUE(small && guard && large && tail);
  allocator.Free(small);
  allocator.Free(large);
  const auto request = allocator.Allocate(127);
  ASSERT_TRUE(request);
  EXPECT_EQ(request.offset, large.offset);
  allocator.Free(request);
  allocator.Free(guard);
  allocator.Free(tail);
  const auto whole = allocator.Allocate(256);
  ASSERT_TRUE(whole);
  allocator.Free(whole);
}

TEST(TlsfRangeAllocatorTest, BoundedSearchCanMissFitBehindSmallerClassHead) {
  std::array<Metadata, 131> metadata;
  TlsfRangeAllocator allocator;
  ASSERT_TRUE(allocator.Init(metadata));
  const auto large = allocator.Allocate(65);
  const auto guard = allocator.Allocate(1);
  const auto small = allocator.Allocate(64);
  const auto tail = allocator.Allocate(1);
  ASSERT_TRUE(large && guard && small && tail);
  allocator.Free(large);
  allocator.Free(small);
  // Both lengths share a class. This deliberately exercises the documented
  // good-fit limit rather than demanding an unbounded search of that list.
  EXPECT_FALSE(allocator.Allocate(65));
  EXPECT_EQ(allocator.free_units(), 129U);
  const auto first = allocator.Allocate(64);
  const auto second = allocator.Allocate(65);
  ASSERT_TRUE(first && second);
  EXPECT_EQ(first.offset, small.offset);
  EXPECT_EQ(second.offset, large.offset);
  allocator.Free(first);
  allocator.Free(second);
  allocator.Free(guard);
  allocator.Free(tail);
  EXPECT_EQ(allocator.free_units(), 131U);
}

TEST(TlsfRangeAllocatorTest, FragmentedUnitsCannotSatisfyContiguousRequest) {
  std::vector<Metadata> metadata(8192);
  TlsfRangeAllocator allocator;
  ASSERT_TRUE(allocator.Init(metadata));
  std::vector<Allocation> allocations;
  for (unsigned i = 0; i < metadata.size(); ++i) {
    const auto allocation = allocator.Allocate(1);
    ASSERT_TRUE(allocation);
    allocations.push_back(allocation);
  }
  EXPECT_FALSE(allocator.Allocate(1));
  for (unsigned i = 0; i < allocations.size(); i += 2)
    allocator.Free(allocations[i]);
  EXPECT_EQ(allocator.free_units(), 4096U);
  EXPECT_FALSE(allocator.Allocate(2));
  for (unsigned i = 1; i < allocations.size(); i += 2)
    allocator.Free(allocations[i]);
  const auto whole = allocator.Allocate(8192);
  ASSERT_TRUE(whole);
  EXPECT_EQ(whole.offset, 0U);
  allocator.Free(whole);
}

TEST(TlsfRangeAllocatorTest, ReinitializesWithDifferentCapacityAfterDrain) {
  std::array<Metadata, 8191> metadata;
  TlsfRangeAllocator allocator;
  for (std::uint32_t capacity : {8191U, 13U, 1U, 0U, 4097U}) {
    ASSERT_TRUE(allocator.Init(std::span(metadata).first(capacity)));
    EXPECT_EQ(allocator.capacity(), capacity);
    EXPECT_EQ(allocator.free_units(), capacity);
    EXPECT_EQ(allocator.metadata_bytes(), capacity * sizeof(Metadata));
    const auto all = allocator.Allocate(capacity);
    if (capacity == 0) {
      EXPECT_FALSE(all);
    } else {
      ASSERT_TRUE(all);
      EXPECT_EQ(all.offset, 0U);
      allocator.Free(all);
    }
  }
}

TEST(TlsfRangeAllocatorTest, RandomOperationsRespectReferenceOccupancy) {
  constexpr std::uint32_t kCapacity = 8192;
  for (unsigned seed : {1U, 17U, 2026U}) {
    SCOPED_TRACE(seed);
    std::mt19937 random(seed);
    std::vector<Metadata> metadata(kCapacity);
    TlsfRangeAllocator allocator;
    ASSERT_TRUE(allocator.Init(metadata));
    std::vector<Allocation> live;
    std::vector<bool> occupied(kCapacity, false);
    std::uint32_t used = 0;
    unsigned successes = 0;
    for (unsigned operation = 0; operation < 50000; ++operation) {
      if (!live.empty() && random() % 100 < 45) {
        const auto index = random() % live.size();
        const auto allocation = live[index];
        auto begin = occupied.begin() + allocation.offset;
        auto end = begin + allocation.units;
        ASSERT_TRUE(std::all_of(begin, end, [](bool bit) { return bit; }));
        std::fill(begin, end, false);
        used -= allocation.units;
        allocator.Free(allocation);
        live[index] = live.back();
        live.pop_back();
      } else {
        const std::uint32_t units =
            random() % 3 == 0 ? (1U << (random() % 14)) : 1 + random() % 511;
        const auto allocation = allocator.Allocate(units);
        if (allocation) {
          ASSERT_EQ(allocation.units, units);
          ASSERT_LE(allocation.offset, kCapacity - units);
          auto begin = occupied.begin() + allocation.offset;
          auto end = begin + units;
          ASSERT_TRUE(std::none_of(begin, end, [](bool bit) { return bit; }));
          std::fill(begin, end, true);
          used += units;
          live.push_back(allocation);
          ++successes;
        }
      }
      ASSERT_EQ(allocator.free_units(), kCapacity - used);
    }
    EXPECT_GT(successes, 10000U);
    std::shuffle(live.begin(), live.end(), random);
    for (auto allocation : live) allocator.Free(allocation);
    const auto whole = allocator.Allocate(kCapacity);
    ASSERT_TRUE(whole);
    EXPECT_EQ(whole.offset, 0U);
    allocator.Free(whole);
  }
}

}  // namespace
}  // namespace lavik
