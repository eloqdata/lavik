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

#pragma once

#include <array>
#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>

namespace lavik {

// Allocates intervals of integer units without owning or touching their
// payload. A caller can interpret a unit as a DMA page, a byte, or another
// fixed quantum; alignment, address translation, accounting, waiting, and I/O
// remain outside this class. Metadata is supplied once and never allocated on
// the hot path.
//
// Uses TLSF-style size classes and word-sized bitmaps. Allocate/Free perform
// bounded O(1) metadata work, including immediate coalescing of free neighbors.
// This is a good-fit search, not an exhaustive best-fit search: only the head
// of the request's size class is examined before looking in larger classes. A
// suitable block deeper in that first list can therefore be missed. Failure
// does not imply that free_units() is smaller than the request.
//
// Single-owner, not synchronized. The caller must marshal cross-thread frees
// back to that owner and retain an allocation until all users, including DMA,
// have finished. No allocation moves while live.
class TlsfRangeAllocator {
 public:
  static constexpr std::uint32_t kInvalidOffset =
      std::numeric_limits<std::uint32_t>::max();
  // Reserve a second non-offset value for marking allocated block headers.
  static constexpr std::uint32_t kMaxCapacity = kInvalidOffset - 1;

  struct Allocation {
    std::uint32_t offset = kInvalidOffset;
    std::uint32_t units = 0;

    explicit operator bool() const noexcept { return offset != kInvalidOffset; }
  };

  // One descriptor per possible block start. Only descriptors at current block
  // boundaries are used; splitting never needs a second descriptor allocator.
  // Keep these outside the managed arena so headers cannot disturb alignment.
  struct Metadata {
   private:
    friend class TlsfRangeAllocator;
    std::uint32_t units_ = 0;
    std::uint32_t previous_physical_ = kInvalidOffset;
    std::uint32_t next_free_ = kMaxCapacity;
    std::uint32_t previous_free_ = kInvalidOffset;
  };

  static_assert(sizeof(Metadata) == 16);
  static_assert(sizeof(Allocation) == 8);

  TlsfRangeAllocator() = default;
  TlsfRangeAllocator(const TlsfRangeAllocator&) = delete;
  TlsfRangeAllocator& operator=(const TlsfRangeAllocator&) = delete;

  // Manage [0, metadata.size()) units. The span is exclusively borrowed and
  // must remain at the same address until the allocator is no longer used.
  // Reinitialization requires every previous allocation to have been returned.
  // Empty pools are valid. Oversized spans return false without changing state.
  [[nodiscard]] bool Init(std::span<Metadata> metadata) noexcept {
    if (metadata.size() > kMaxCapacity) return false;
    assert(free_units_ == capacity_);
    metadata_ = metadata.data();
    capacity_ = static_cast<std::uint32_t>(metadata.size());
    free_units_ = capacity_;
    first_bitmap_ = 0;
    second_bitmaps_.fill(0);
    for (auto& lists : free_lists_) lists.fill(kInvalidOffset);
    if (capacity_ != 0) {
      metadata_[0] = Metadata{};
      metadata_[0].units_ = capacity_;
      InsertFree(0);
    }
    return true;
  }

  // Returns exactly the requested number of consecutive units, never a
  // power-of-two-rounded allocation. Zero, oversized, fragmented, or exhausted
  // requests return an empty Allocation and leave the pool unchanged.
  [[nodiscard]] Allocation Allocate(std::uint32_t units) noexcept {
    if (units == 0 || units > free_units_) [[unlikely]] {
      return {};
    }
    const std::uint32_t offset = TakeFree(units);
    if (offset == kInvalidOffset) [[unlikely]] {
      return {};
    }
    free_units_ -= units;
    return {.offset = offset, .units = units};
  }

  // Requires a live allocation returned by this instance, freed exactly once.
  // As with a pointer allocator, a stale handle after offset reuse is invalid.
  // No scan or payload copy is needed: coalescing eagerly maintains the
  // invariant that no two free blocks are physically adjacent.
  void Free(Allocation allocation) noexcept {
    assert(allocation && allocation.offset < capacity_);
    std::uint32_t offset = allocation.offset;
    assert(metadata_[offset].units_ == allocation.units);
    assert(metadata_[offset].next_free_ == kAllocated);
    free_units_ += allocation.units;
    std::uint32_t units = allocation.units;
    const std::uint32_t previous = metadata_[offset].previous_physical_;
    if (previous != kInvalidOffset && IsFree(previous)) {
      RemoveFree(previous);
      units += metadata_[previous].units_;
      metadata_[offset].units_ = 0;
      offset = previous;
    }
    const std::uint32_t next = offset + units;
    if (next < capacity_ && IsFree(next)) {
      RemoveFree(next);
      units += metadata_[next].units_;
      metadata_[next].units_ = 0;
    }
    metadata_[offset].units_ = units;
    const std::uint32_t following = offset + units;
    if (following < capacity_) metadata_[following].previous_physical_ = offset;
    InsertFree(offset);
  }

  std::uint32_t capacity() const noexcept { return capacity_; }
  // Counts all free units, including separated fragments; not the largest fit.
  std::uint32_t free_units() const noexcept { return free_units_; }
  // Borrowed metadata only; the fixed bitmap/list table is part of this object.
  std::size_t metadata_bytes() const noexcept {
    return static_cast<std::size_t>(capacity_) * sizeof(Metadata);
  }

 private:
  static constexpr unsigned kSecondBits = 5;
  static constexpr unsigned kSecondCount = 1U << kSecondBits;
  static constexpr unsigned kFirstCount = 32 - kSecondBits + 1;
  static constexpr std::uint32_t kAllocated = kMaxCapacity;

  struct SizeClass {
    unsigned first;
    unsigned second;
  };

  static SizeClass ClassFor(std::uint32_t units) noexcept {
    assert(units != 0);
    // Small requests have exact one-unit classes. Above them, each power-of-
    // two interval has 32 subdivisions, represented by bits in one word.
    if (units < kSecondCount) return {0, units};
    const unsigned shift = std::bit_width(units) - 1 - kSecondBits;
    return {shift + 1, (units >> shift) - kSecondCount};
  }

  static std::uint32_t BitsAbove(unsigned bit) noexcept {
    // Unlike ~0U << (bit + 1), this remains defined when bit is 31.
    return (~std::uint32_t{1}) << bit;
  }

  std::uint32_t TakeFree(std::uint32_t units) noexcept {
    auto [first, second] = ClassFor(units);
    std::uint32_t head = free_lists_[first][second];
    // This one bounded probe also lets an arbitrary-sized whole pool or an
    // exact returned block satisfy its own size without size-class rounding.
    if (head == kInvalidOffset || metadata_[head].units_ < units) {
      std::uint32_t candidates = second_bitmaps_[first] & BitsAbove(second);
      if (candidates == 0) {
        const std::uint32_t larger = first_bitmap_ & BitsAbove(first);
        if (larger == 0) return kInvalidOffset;
        first = std::countr_zero(larger);
        candidates = second_bitmaps_[first];
      }
      assert(candidates != 0);
      second = std::countr_zero(candidates);
      head = free_lists_[first][second];
    }
    Metadata& block = metadata_[head];
    const std::uint32_t remaining = block.units_ - units;
    const std::uint32_t next = block.next_free_;
    if (remaining != 0) {
      const std::uint32_t remainder = head + units;
      Metadata& tail = metadata_[remainder];
      tail.units_ = remaining;
      tail.previous_physical_ = head;
      const std::uint32_t following = remainder + remaining;
      if (following < capacity_)
        metadata_[following].previous_physical_ = remainder;
      const auto tail_class = ClassFor(remaining);
      if (tail_class.first == first && tail_class.second == second) {
        // Small cuts from a larger block often stay in the same size class.
        // Replace that list head in place; its bitmap bits remain set.
        tail.previous_free_ = kInvalidOffset;
        tail.next_free_ = next;
        free_lists_[first][second] = remainder;
        if (next != kInvalidOffset) metadata_[next].previous_free_ = remainder;
      } else {
        RemoveHead({first, second}, next);
        InsertFree(remainder, tail_class);
      }
      block.units_ = units;
    } else {
      RemoveHead({first, second}, next);
    }
    block.next_free_ = kAllocated;
    return head;
  }

  void RemoveHead(SizeClass size_class, std::uint32_t next) noexcept {
    // Allocation knows the class and always removes a head. No repeated size
    // classification or generic doubly-linked removal is necessary.
    const auto [first, second] = size_class;
    free_lists_[first][second] = next;
    if (next != kInvalidOffset) {
      metadata_[next].previous_free_ = kInvalidOffset;
    } else {
      second_bitmaps_[first] &= ~(std::uint32_t{1} << second);
      if (second_bitmaps_[first] == 0)
        first_bitmap_ &= ~(std::uint32_t{1} << first);
    }
  }

  bool IsFree(std::uint32_t offset) const noexcept {
    assert(metadata_[offset].units_ != 0);
    return metadata_[offset].next_free_ != kAllocated;
  }

  void InsertFree(std::uint32_t offset) noexcept {
    InsertFree(offset, ClassFor(metadata_[offset].units_));
  }

  void InsertFree(std::uint32_t offset, SizeClass size_class) noexcept {
    Metadata& block = metadata_[offset];
    const auto [first, second] = size_class;
    const std::uint32_t head = free_lists_[first][second];
    block.previous_free_ = kInvalidOffset;
    block.next_free_ = head;
    if (head != kInvalidOffset) {
      metadata_[head].previous_free_ = offset;
    } else {
      second_bitmaps_[first] |= std::uint32_t{1} << second;
      first_bitmap_ |= std::uint32_t{1} << first;
    }
    free_lists_[first][second] = offset;
  }

  void RemoveFree(std::uint32_t offset) noexcept {
    Metadata& block = metadata_[offset];
    if (block.previous_free_ != kInvalidOffset) {
      // A non-head removal cannot empty its class. It needs neither a size
      // classification nor a bitmap update.
      metadata_[block.previous_free_].next_free_ = block.next_free_;
    } else {
      const auto [first, second] = ClassFor(block.units_);
      free_lists_[first][second] = block.next_free_;
      if (block.next_free_ == kInvalidOffset) {
        second_bitmaps_[first] &= ~(std::uint32_t{1} << second);
        if (second_bitmaps_[first] == 0)
          first_bitmap_ &= ~(std::uint32_t{1} << first);
      }
    }
    if (block.next_free_ != kInvalidOffset)
      metadata_[block.next_free_].previous_free_ = block.previous_free_;
    // Only coalescing uses this helper; the descriptor will be absorbed or
    // reinserted before returning to the caller, never handed out as-is.
  }

  Metadata* metadata_ = nullptr;
  std::uint32_t capacity_ = 0;
  std::uint32_t free_units_ = 0;
  std::uint32_t first_bitmap_ = 0;
  std::array<std::uint32_t, kFirstCount> second_bitmaps_{};
  std::array<std::array<std::uint32_t, kSecondCount>, kFirstCount>
      free_lists_{};
};

}  // namespace lavik
