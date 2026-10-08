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

#include "lavik/containers/cow_array.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "lavik/memory.h"

namespace lavik {
namespace {

// Restore process-wide admission even after ASSERT_* exits a test early.
class ArrayMemoryScope {
 public:
  ArrayMemoryScope() : shard_(CurrentMemoryAccountingShard()) {
    EXPECT_TRUE(InitMemoryLimit(1024ULL * 1024 * 1024, 1).ok());
    BindMemoryAccountingShard(0);
  }
  ~ArrayMemoryScope() {
    EXPECT_TRUE(InitMemoryLimit(1024ULL * 1024 * 1024, 1).ok());
    BindMemoryAccountingShard(shard_ == 0 ? kMaxMemoryWorkers : shard_ - 1);
  }

 private:
  unsigned shard_;
};

TEST(CowArrayTest, SmallArraysAllocateOnlyLiveEntriesAndDetachOnlyWhenShared) {
  ArrayMemoryScope memory;
  using Array = CowArray<std::uint64_t>;
  const auto baseline = WorkerMemoryAccountingBytes(0);
  {
    auto current = Array::From(std::vector<std::uint64_t>(22, 17));
    ASSERT_TRUE(current.ok());
    const auto allocated = WorkerMemoryAccountingBytes(0) - baseline;
    // 176 bytes of elements plus ownership metadata fit one small allocation;
    // a pointer branch or a fully reserved 32-entry chunk would exceed this.
    EXPECT_LT(allocated, 256);
    const auto* data = &current->front();
    ASSERT_TRUE(InitMemoryLimit(1, 1).ok());
    EXPECT_TRUE(current->Set(21, 23).ok());
    EXPECT_EQ(&current->front(), data);
    auto pinned = *current;
    EXPECT_EQ(current->Set(0, 29).code(), absl::StatusCode::kResourceExhausted);
    EXPECT_EQ(current->front(), 17);
    EXPECT_EQ(pinned.back(), 23);
    ASSERT_TRUE(InitMemoryLimit(1024ULL * 1024 * 1024, 1).ok());
    ASSERT_TRUE(current->Set(0, 29).ok());
    EXPECT_NE(&current->front(), data);
    EXPECT_EQ(pinned.front(), 17);
    EXPECT_LT(WorkerMemoryAccountingBytes(0) - baseline, 512);
  }
  EXPECT_EQ(WorkerMemoryAccountingBytes(0), baseline);
}

struct CountedArrayValue {
  static inline int constructions = 0;
  static inline int throw_after = -1;
  int value_ = 0;
  CountedArrayValue() { ++constructions; }
  CountedArrayValue(const CountedArrayValue& other) : value_(other.value_) {
    if (throw_after == 0) throw std::runtime_error("copy failure");
    if (throw_after > 0) --throw_after;
    ++constructions;
  }
  CountedArrayValue& operator=(const CountedArrayValue&) = default;
};

TEST(CowArrayTest,
     SpareCapacityIsUninitializedAndThrowingCopiesReleaseStorage) {
  ArrayMemoryScope memory;
  std::array<CountedArrayValue, 3> values;
  values[0].value_ = 11;
  using Array = CowArray<CountedArrayValue>;
  const auto baseline = WorkerMemoryAccountingBytes(0);
  {
    CountedArrayValue::constructions = 0;
    auto current = Array::From(std::span(values).first(2));
    ASSERT_TRUE(current.ok());
    EXPECT_EQ(CountedArrayValue::constructions, 2);
    CountedArrayValue::constructions = 0;
    auto appended = current->Appended(std::span(values).last(1));
    ASSERT_TRUE(appended.ok());
    EXPECT_EQ(CountedArrayValue::constructions, 3);
    EXPECT_EQ(current->size(), 2);
    EXPECT_EQ(appended->size(), 3);
    const auto before = WorkerMemoryAccountingBytes(0);
    CountedArrayValue::throw_after = 1;
    EXPECT_THROW(current->Appended(values), std::runtime_error);
    CountedArrayValue::throw_after = -1;
    EXPECT_EQ(WorkerMemoryAccountingBytes(0), before);
    EXPECT_EQ(current->front().value_, 11);
  }
  EXPECT_EQ(WorkerMemoryAccountingBytes(0), baseline);
}

TEST(CowArrayTest, OverAlignedElementsSurviveSingleChunkPromotionAndCopies) {
  ArrayMemoryScope memory;
  struct alignas(128) Value {
    std::uint64_t value_;
  };
  using Array = CowArray<Value, 4>;
  const auto baseline = WorkerMemoryAccountingBytes(0);
  {
    const std::array<Value, 3> input{{{1}, {2}, {3}}};
    auto current = Array::From(input);
    ASSERT_TRUE(current.ok());
    for (unsigned step = 0; step < 50; ++step) {
      const auto pinned = *current;
      auto next = current->Appended(input);
      ASSERT_TRUE(next.ok());
      for (std::size_t i = 0; i < next->size(); ++i) {
        EXPECT_EQ(
            reinterpret_cast<std::uintptr_t>(&(*next)[i]) % alignof(Value), 0);
        EXPECT_EQ((*next)[i].value_, 1 + i % 3);
      }
      ASSERT_TRUE(next->Set(next->size() - 1, Value{99}).ok());
      EXPECT_EQ(pinned.back().value_, 3);
      ASSERT_TRUE(next->Set(next->size() - 1, Value{3}).ok());
      *current = std::move(*next);
    }
  }
  EXPECT_EQ(WorkerMemoryAccountingBytes(0), baseline);
}

#ifndef NDEBUG
TEST(CowArrayDeathTest, RejectsForeignThreadSharing) {
  auto array = CowArray<std::uint64_t>::From(std::array<std::uint64_t, 1>{17});
  ASSERT_TRUE(array.ok());
  EXPECT_DEATH(
      {
        std::thread reader([&] { auto copy = *array; });
        reader.join();
      },
      "owner_");
}
#endif

TEST(CowArrayTest, SparseUpdatesPreserveSnapshotsAcrossTreeLevels) {
  ArrayMemoryScope memory;
  using Array = CowArray<std::uint64_t, 4>;
  EXPECT_TRUE(Array::From({})->empty());
  // Cross chunk, pointer-page and pointer-tree boundaries, including a
  // partially populated final branch. Exercise both random access and
  // iteration.
  std::vector<std::uint64_t> values(4 * 32 * 32 + 3);
  for (std::size_t i = 0; i < values.size(); ++i) values[i] = i;
  auto original = Array::From(values);
  ASSERT_TRUE(original.ok()) << original.status();
  auto updated = *original;
  for (std::size_t index : {0, 3, 4, 127, 128, 4095, 4096, 4098}) {
    ASSERT_TRUE(updated.Set(index, index + 10000).ok());
    values[index] = index + 10000;
  }
  EXPECT_TRUE(std::equal(updated.begin(), updated.end(), values.begin()));
  for (std::size_t i = 0; i < values.size(); ++i) EXPECT_EQ((*original)[i], i);
  EXPECT_EQ(&updated[256], &(*original)[256]);
  EXPECT_NE(&updated[128], &(*original)[128]);
  auto successor = updated;
  ASSERT_TRUE(successor.Set(128, 42).ok());
  EXPECT_EQ(updated[128], 10128);
  updated = {};
  *original = {};
  EXPECT_EQ(successor[128], 42);
  EXPECT_EQ(successor[4098], 14098);
  auto moved = std::move(successor);
  EXPECT_TRUE(successor.empty());
  EXPECT_EQ(moved.back(), 14098);
}

TEST(CowArrayTest, PointUpdateChargesOnlyItsPathAndReleasesAllOwners) {
  ArrayMemoryScope memory;
  using Array = CowArray<std::uint64_t, 4>;
  const auto baseline = WorkerMemoryAccountingBytes(0);
  {
    std::vector<std::uint64_t> values(262144, 17);
    auto original = Array::From(values);
    ASSERT_TRUE(original.ok());
    const auto complete = WorkerMemoryAccountingBytes(0);
    EXPECT_GE(original->RetainedBytes(), complete - baseline);
    auto updated = *original;
    EXPECT_EQ(WorkerMemoryAccountingBytes(0), complete);
    ASSERT_TRUE(updated.Set(131072, 29).ok());
    const auto detached = WorkerMemoryAccountingBytes(0) - complete;
    EXPECT_GT(detached, 0);
    // A flat table would copy 65,536 pointers (>512 KiB). Bound actual
    // charged growth instead of inspecting the private branching layout.
    EXPECT_LT(detached, 16 * 1024);
    EXPECT_EQ((*original)[131072], 17);
    EXPECT_EQ(updated[131072], 29);
    updated = {};
    EXPECT_EQ(WorkerMemoryAccountingBytes(0), complete);
  }
  EXPECT_EQ(WorkerMemoryAccountingBytes(0), baseline);
}

TEST(CowArrayTest, PartialDetachAdmissionFailureCanBeRetried) {
  ArrayMemoryScope memory;
  using Array = CowArray<std::uint64_t, 4>;
  const auto baseline = WorkerMemoryAccountingBytes(0);
  {
    std::vector<std::uint64_t> values(4100, 17);
    auto original = Array::From(values);
    ASSERT_TRUE(original.ok());
    auto updated = *original;
    const auto before = WorkerMemoryAccountingBytes(0);
    // Admit the first pointer allocation but not the rest of a shared path.
    const auto steady = GetWorkerMemoryStats(0).retained_bytes_ + 400;
    ASSERT_TRUE(InitMemoryLimit((steady * 10 + 8) / 9, 1).ok());
    EXPECT_EQ(updated.Set(4099, 29).code(),
              absl::StatusCode::kResourceExhausted);
    EXPECT_GT(WorkerMemoryAccountingBytes(0), before);
    EXPECT_EQ(updated[4099], 17);
    EXPECT_EQ((*original)[4099], 17);
    ASSERT_TRUE(InitMemoryLimit(1024ULL * 1024 * 1024, 1).ok());
    ASSERT_TRUE(updated.Set(4099, 29).ok());
    EXPECT_EQ(updated[4099], 29);
    EXPECT_EQ((*original)[4099], 17);
  }
  EXPECT_EQ(WorkerMemoryAccountingBytes(0), baseline);
  EXPECT_EQ(GetMemoryStats().admission_pending_bytes_, 0);
}

TEST(CowArrayTest, AppendsPreservePinnedTailsAcrossTreeGrowth) {
  ArrayMemoryScope memory;
  using Array = CowArray<std::uint64_t, 4>;
  Array current;
  std::vector<std::uint64_t> expected;
  // Include partial tails, exact chunk boundaries, and two root expansions.
  for (std::size_t count : {0, 1, 3, 123, 2, 4000, 1, 8192}) {
    const auto pinned = current;
    const auto old_size = expected.size();
    std::vector<std::uint64_t> values(count);
    for (std::size_t i = 0; i < count; ++i) values[i] = old_size + i + 17;
    auto appended = current.Appended(values);
    ASSERT_TRUE(appended.ok()) << appended.status();
    expected.insert(expected.end(), values.begin(), values.end());
    EXPECT_EQ(appended->size(), expected.size());
    EXPECT_TRUE(
        std::equal(appended->begin(), appended->end(), expected.begin()));
    for (std::size_t i = 0; i < old_size; ++i) {
      EXPECT_EQ(pinned[i], i + 17);
      if (i < old_size / 4 * 4) EXPECT_EQ(&pinned[i], &(*appended)[i]);
    }
    current = std::move(*appended);
    if (!current.empty()) {
      // A later point update must also leave the previous tail intact.
      auto changed = current;
      ASSERT_TRUE(changed.Set(current.size() - 1, 999999).ok());
      EXPECT_EQ(current.back(), expected.back());
    }
  }
}

TEST(CowArrayTest, AppendAdmissionFailurePreservesViewAndAccounting) {
  ArrayMemoryScope memory;
  using Array = CowArray<std::uint64_t, 4>;
  const auto baseline = WorkerMemoryAccountingBytes(0);
  {
    auto original = Array::From(std::vector<std::uint64_t>(128, 17));
    ASSERT_TRUE(original.ok());
    const auto before = WorkerMemoryAccountingBytes(0);
    // Root growth succeeds before the new child path runs out of admission.
    const auto steady = GetWorkerMemoryStats(0).retained_bytes_ + 400;
    ASSERT_TRUE(InitMemoryLimit((steady * 10 + 8) / 9, 1).ok());
    const std::array<std::uint64_t, 1> tail{29};
    auto rejected = original->Appended(tail);
    EXPECT_EQ(rejected.status().code(), absl::StatusCode::kResourceExhausted);
    EXPECT_EQ(WorkerMemoryAccountingBytes(0), before);
    EXPECT_EQ(original->size(), 128);
    EXPECT_EQ(original->back(), 17);
    ASSERT_TRUE(InitMemoryLimit(1024ULL * 1024 * 1024, 1).ok());
    auto appended = original->Appended(tail);
    ASSERT_TRUE(appended.ok()) << appended.status();
    EXPECT_EQ(appended->back(), 29);
    EXPECT_EQ(&appended->front(), &original->front());
    EXPECT_LT(WorkerMemoryAccountingBytes(0) - before, 16 * 1024);
  }
  EXPECT_EQ(WorkerMemoryAccountingBytes(0), baseline);
  EXPECT_EQ(GetMemoryStats().admission_pending_bytes_, 0);
}

}  // namespace
}  // namespace lavik
