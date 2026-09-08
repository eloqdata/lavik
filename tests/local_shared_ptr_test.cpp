#include "keylane/local_shared_ptr.h"

#include <cstdint>
#include <stdexcept>
#include <thread>

#include "gtest/gtest.h"

namespace keylane {
namespace {

static_assert(sizeof(LocalSharedPtr<int>) == sizeof(void*));
static_assert(
    std::is_constructible_v<LocalSharedPtr<const int>, LocalSharedPtr<int>>);
static_assert(
    !std::is_constructible_v<LocalSharedPtr<int>, LocalSharedPtr<const int>>);
static_assert(std::is_nothrow_move_constructible_v<LocalSharedPtr<int>>);
static_assert(std::is_nothrow_move_assignable_v<LocalSharedPtr<int>>);

struct Tracked {
  int* destroyed_;
  int value_;

  Tracked(int* destroyed, int value) : destroyed_(destroyed), value_(value) {}
  ~Tracked() { ++*destroyed_; }
};

struct AllocationStats {
  std::size_t allocations_ = 0;
  std::size_t deallocations_ = 0;
  std::size_t live_bytes_ = 0;
  bool fail_ = false;
};

template <typename T>
struct CountingAllocator {
  using value_type = T;
  AllocationStats* stats_;

  explicit CountingAllocator(AllocationStats& stats) : stats_(&stats) {}
  template <typename U>
  CountingAllocator(const CountingAllocator<U>& other) noexcept
      : stats_(other.stats_) {}

  T* allocate(std::size_t count) {
    if (stats_->fail_) throw std::bad_alloc();
    auto* pointer = std::allocator<T>{}.allocate(count);
    ++stats_->allocations_;
    stats_->live_bytes_ += count * sizeof(T);
    return pointer;
  }
  void deallocate(T* pointer, std::size_t count) noexcept {
    ++stats_->deallocations_;
    stats_->live_bytes_ -= count * sizeof(T);
    std::allocator<T>{}.deallocate(pointer, count);
  }
};

TEST(LocalSharedPtrTest, EmptyAndNullOperationsDoNotOwnAnything) {
  LocalSharedPtr<int> empty;
  LocalSharedPtr<int> copy(empty);
  LocalSharedPtr<const int> moved(std::move(copy));
  EXPECT_EQ(empty, nullptr);
  EXPECT_EQ(copy.get(), nullptr);
  EXPECT_FALSE(moved);
  EXPECT_EQ(moved.use_count(), 0);
  empty.reset();
  empty = nullptr;
  auto value = MakeLocalShared<int>(7);
  empty.swap(value);
  EXPECT_FALSE(value);
  EXPECT_EQ(*empty, 7);
  EXPECT_EQ(empty.use_count(), 1);
}

TEST(LocalSharedPtrTest, CopyMoveAndConstConversionShareOneLifetime) {
  int destroyed = 0;
  auto value = MakeLocalShared<Tracked>(&destroyed, 17);
  auto copy = value;
  LocalSharedPtr<const Tracked> immutable = copy;
  EXPECT_EQ(value.use_count(), 3);
  EXPECT_EQ(immutable.get(), value.get());
  EXPECT_EQ(immutable->value_, 17);

  LocalSharedPtr<const Tracked> moved(std::move(copy));
  EXPECT_FALSE(copy);
  EXPECT_EQ(moved.use_count(), 3);
  LocalSharedPtr<const Tracked> assigned;
  assigned = value;
  EXPECT_EQ(value.use_count(), 4);
  // Replacing one owner by another owner of the same object must release
  // exactly one reference, including the converting move-assignment path.
  assigned = std::move(value);
  EXPECT_FALSE(value);
  EXPECT_EQ(assigned.use_count(), 3);
  immutable.reset();
  moved = nullptr;
  EXPECT_EQ(destroyed, 0);
  assigned.reset();
  EXPECT_EQ(destroyed, 1);
  EXPECT_EQ(*MakeLocalShared<const int>(23), 23);
}

TEST(LocalSharedPtrTest, AssignmentReleasesOldObjectAndHandlesSelfAssignment) {
  int destroyed = 0;
  auto first = MakeLocalShared<Tracked>(&destroyed, 1);
  auto second = MakeLocalShared<Tracked>(&destroyed, 2);
  first = second;
  EXPECT_EQ(destroyed, 1);
  EXPECT_EQ(first.use_count(), 2);
  auto& alias = first;
  first = alias;
  first = std::move(alias);
  EXPECT_EQ(first->value_, 2);
  EXPECT_EQ(first.use_count(), 2);
  second.reset();
  EXPECT_EQ(destroyed, 1);
  first.reset();
  EXPECT_EQ(destroyed, 2);
}

TEST(LocalSharedPtrTest, StatefulAllocatorOwnsOneCombinedAllocation) {
  AllocationStats stats;
  int destroyed = 0;
  {
    auto value = AllocateLocalShared<Tracked>(
        CountingAllocator<std::byte>(stats), &destroyed, 42);
    EXPECT_EQ(stats.allocations_, 1);
    EXPECT_GT(stats.live_bytes_, sizeof(Tracked));
    LocalSharedPtr<const Tracked> copy(value);
    value.reset();
    EXPECT_EQ(stats.deallocations_, 0);
    EXPECT_EQ(copy->value_, 42);
  }
  EXPECT_EQ(destroyed, 1);
  EXPECT_EQ(stats.deallocations_, 1);
  EXPECT_EQ(stats.live_bytes_, 0);
}

TEST(LocalSharedPtrTest, AllocationAndConstructorFailuresDoNotLeak) {
  struct Throwing {
    Throwing() { throw std::runtime_error("construction failed"); }
  };
  AllocationStats stats;
  CountingAllocator<std::byte> allocator(stats);
  EXPECT_THROW(AllocateLocalShared<Throwing>(allocator), std::runtime_error);
  EXPECT_EQ(stats.allocations_, 1);
  EXPECT_EQ(stats.deallocations_, 1);
  EXPECT_EQ(stats.live_bytes_, 0);
  stats.fail_ = true;
  EXPECT_THROW(AllocateLocalShared<int>(allocator, 1), std::bad_alloc);
  EXPECT_EQ(stats.allocations_, 1);
  EXPECT_EQ(stats.deallocations_, 1);
}

TEST(LocalSharedPtrTest, CombinedAllocationPreservesObjectAlignment) {
  struct alignas(256) Aligned {
    int value_ = 29;
  };
  AllocationStats stats;
  auto value = AllocateLocalShared<Aligned>(CountingAllocator<Aligned>(stats));
  EXPECT_EQ(reinterpret_cast<std::uintptr_t>(value.get()) % alignof(Aligned),
            0);
  EXPECT_EQ(value->value_, 29);
  value.reset();
  EXPECT_EQ(stats.live_bytes_, 0);
}

TEST(LocalSharedPtrTest, SharedSubtreesOutliveEitherRoot) {
  struct Node {
    int* destroyed_;
    LocalSharedPtr<const Node> child_;
    Node(int* destroyed, LocalSharedPtr<const Node> child)
        : destroyed_(destroyed), child_(std::move(child)) {}
    ~Node() { ++*destroyed_; }
  };
  int destroyed = 0;
  auto leaf = MakeLocalShared<Node>(&destroyed, nullptr);
  auto old_root = MakeLocalShared<Node>(&destroyed, leaf);
  auto new_root = MakeLocalShared<Node>(&destroyed, leaf);
  leaf.reset();
  old_root.reset();
  EXPECT_EQ(destroyed, 1);
  EXPECT_EQ(new_root->child_.use_count(), 1);
  new_root.reset();
  EXPECT_EQ(destroyed, 3);
}

#ifndef NDEBUG
// A foreign thread waits for no owner work: these deaths specifically check
// affinity, not a lucky concurrent reference-count race. Both threads would
// have the same default memory-accounting shard in these runtime-free tests.
TEST(LocalSharedPtrDeathTest, RejectsForeignCopyBeforeIncrementing) {
  auto value = MakeLocalShared<int>(1);
  EXPECT_DEATH(
      { std::thread([&] { auto copy = value; }).join(); },
      "LocalSharedPtr used outside its owner thread");
}

TEST(LocalSharedPtrDeathTest, RejectsForeignMoveBeforeTransferring) {
  auto value = MakeLocalShared<int>(1);
  EXPECT_DEATH(
      { std::thread([&] { auto moved = std::move(value); }).join(); },
      "LocalSharedPtr used outside its owner thread");
}

TEST(LocalSharedPtrDeathTest, RejectsForeignReleaseBeforeDecrementing) {
  auto value = MakeLocalShared<int>(1);
  EXPECT_DEATH(
      { std::thread([&] { value.reset(); }).join(); },
      "LocalSharedPtr used outside its owner thread");
}

TEST(LocalSharedPtrDeathTest, RejectsForeignAccess) {
  auto value = MakeLocalShared<int>(1);
  EXPECT_DEATH(
      { std::thread([&] { (void)value.get(); }).join(); },
      "LocalSharedPtr used outside its owner thread");
}
#endif

}  // namespace
}  // namespace keylane
