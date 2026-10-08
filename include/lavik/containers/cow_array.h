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

#include <algorithm>
#include <array>
#include <bit>
#include <cassert>
#include <compare>
#include <cstddef>
#include <iterator>
#include <limits>
#include <memory>
#include <ranges>
#include <span>
#include <type_traits>
#include <utility>
#ifndef NDEBUG
#include <thread>
#endif

#include "absl/status/statusor.h"
#include "lavik/memory.h"
#include "lavik/retained_allocator.h"

namespace lavik {

// Owner-thread copy-on-write array. A single variable-capacity chunk is the
// root until the array exceeds ChunkEntries; larger arrays use a radix pointer
// tree. From allocates exactly the used entries, including the final chunk.
// Appends may reserve geometric tail capacity, bounded by ChunkEntries.
// Point writes detach only shared nodes on their root-to-chunk path.
//
// T must be copy constructible, copy assignable and trivially
// destructible. Elements are copied by value; referenced resources remain the
// caller's responsibility. All access and final release stay on the owner
// thread. Each allocation admits/accounts itself until its last view releases
// it. Copies share one pointer and never allocate.
template <typename T, std::size_t ChunkEntries = 32>
class CowArray {
  static_assert(ChunkEntries != 0 && std::is_trivially_destructible_v<T>);
  static constexpr unsigned kBranchBits = 5;
  static constexpr std::size_t kBranchEntries = 1 << kBranchBits;

  // The intrusive header permits one allocation for a variable-size chunk
  // and its ownership metadata. Capacity zero identifies a branch, so the
  // array itself needs no additional pointer or representation discriminator.
  struct Node {
    std::size_t references_ = 1;
    std::size_t capacity_;
    RetainedAllocationDomain domain_;
#ifndef NDEBUG
    const std::thread::id owner_ = std::this_thread::get_id();
#endif
    Node(std::size_t capacity, RetainedAllocationDomain domain)
        : capacity_(capacity), domain_(domain) {}
    void AssertOwner() const noexcept {
#ifndef NDEBUG
      assert(owner_ == std::this_thread::get_id());
#endif
    }
  };
  class Link {
   public:
    Link() = default;
    explicit Link(Node* node) : node_(node) {}
    Link(const Link& other) : node_(other.get()) {
      if (node_) {
        assert(node_->references_ != std::numeric_limits<std::size_t>::max());
        ++node_->references_;
      }
    }
    Link(Link&& other) noexcept : node_(other.get()) { other.node_ = nullptr; }
    Link& operator=(Link other) noexcept {
      std::swap(node_, other.node_);
      return *this;
    }
    ~Link() { Release(get()); }
    Node* get() const noexcept {
      if (node_) node_->AssertOwner();
      return node_;
    }
    explicit operator bool() const noexcept { return get() != nullptr; }
    std::size_t use_count() const noexcept {
      return get() ? node_->references_ : 0;
    }

   private:
    Node* node_ = nullptr;
  };
  struct Branch : Node {
    std::array<Link, kBranchEntries> children_;
    explicit Branch(RetainedAllocationDomain domain, const Branch* old)
        : Node(0, domain) {
      if (old) children_ = old->children_;
    }
  };
  struct Chunk : Node {
    std::size_t size_ = 0;
    Chunk(std::size_t capacity, RetainedAllocationDomain domain)
        : Node(capacity, domain) {}
    T* data() noexcept {
      return reinterpret_cast<T*>(reinterpret_cast<std::byte*>(this) +
                                  ChunkOffset());
    }
    const T* data() const noexcept {
      return reinterpret_cast<const T*>(
          reinterpret_cast<const std::byte*>(this) + ChunkOffset());
    }
  };
  static constexpr std::size_t kChunkAlignment =
      std::max(alignof(Chunk), alignof(T));
  static constexpr std::size_t ChunkOffset() {
    return (sizeof(Chunk) + alignof(T) - 1) / alignof(T) * alignof(T);
  }
  static void Release(Node* node) noexcept {
    if (!node || --node->references_ != 0) return;
    const auto domain = node->domain_;
    const bool chunk = node->capacity_ != 0;
    if (chunk)
      std::destroy_at(static_cast<Chunk*>(node));
    else
      std::destroy_at(static_cast<Branch*>(node));
    // T is trivially destructible; releasing its storage ends the lifetimes
    // of the constructed entries without touching uninitialized spare slots.
    DeallocateRetainedBytes(domain, node,
                            chunk ? kChunkAlignment : alignof(Branch));
  }

 public:
  class const_iterator {
   public:
    using value_type = T;
    using reference = const T&;
    using pointer = const T*;
    using difference_type = std::ptrdiff_t;
    using iterator_category = std::random_access_iterator_tag;
    using iterator_concept = std::random_access_iterator_tag;
    const_iterator() = default;
    reference operator*() const { return (*owner_)[index_]; }
    pointer operator->() const { return &**this; }
    reference operator[](difference_type n) const { return *(*this + n); }
    const_iterator& operator++() {
      ++index_;
      return *this;
    }
    const_iterator operator++(int) {
      auto old = *this;
      ++*this;
      return old;
    }
    const_iterator& operator--() {
      --index_;
      return *this;
    }
    const_iterator operator--(int) {
      auto old = *this;
      --*this;
      return old;
    }
    const_iterator& operator+=(difference_type n) {
      index_ =
          static_cast<std::size_t>(static_cast<difference_type>(index_) + n);
      return *this;
    }
    const_iterator& operator-=(difference_type n) { return *this += -n; }
    friend const_iterator operator+(const_iterator it, difference_type n) {
      return it += n;
    }
    friend const_iterator operator+(difference_type n, const_iterator it) {
      return it += n;
    }
    friend const_iterator operator-(const_iterator it, difference_type n) {
      return it -= n;
    }
    friend difference_type operator-(const_iterator a, const_iterator b) {
      assert(a.owner_ == b.owner_);
      return static_cast<difference_type>(a.index_) -
             static_cast<difference_type>(b.index_);
    }
    auto operator<=>(const const_iterator&) const = default;

   private:
    friend class CowArray;
    const_iterator(const CowArray* owner, std::size_t index)
        : owner_(owner), index_(index) {}
    const CowArray* owner_ = nullptr;
    std::size_t index_ = 0;
  };

  // Copy values into exact-capacity chunks. Admission failure releases all
  // partial storage; a single chunk needs no pointer-tree allocation.
  static absl::StatusOr<CowArray> From(std::span<const T> values) {
    return FromSpan(values);
  }
  // Construct entries directly from a contiguous source range, avoiding a
  // second full-size staging vector when the resident type drops input fields.
  template <std::ranges::contiguous_range Range>
    requires std::constructible_from<T,
                                     const std::ranges::range_value_t<Range>&>
  static absl::StatusOr<CowArray> From(const Range& values) {
    using U = std::ranges::range_value_t<Range>;
    return FromSpan(std::span<const U>(std::ranges::data(values),
                                       std::ranges::size(values)));
  }

 private:
  template <typename U>
  static absl::StatusOr<CowArray> FromSpan(std::span<const U> values) {
    CowArray result;
    if (values.empty()) return result;
    if (values.size() <= ChunkEntries) {
      auto chunk = AllocateChunk(values.size(), values);
      if (!chunk.ok()) return chunk.status();
      result.root_ = std::move(*chunk);
    } else {
      const auto chunks = 1 + (values.size() - 1) / ChunkEntries;
      for (auto remaining = (chunks - 1) >> kBranchBits; remaining != 0;
           remaining >>= kBranchBits)
        result.root_shift_ += kBranchBits;
      auto root = Build(values, result.root_shift_);
      if (!root.ok()) return root.status();
      result.root_ = std::move(*root);
    }
    result.size_ = values.size();
    return result;
  }

 public:
  // Return a longer immutable view, sharing complete chunks. The partially
  // filled tail is copied with geometric capacity for subsequent appends.
  // Failure leaves the predecessor and its borrowed references untouched.
  absl::StatusOr<CowArray> Appended(std::span<const T> values) const {
    return AppendSpan(values);
  }
  // Like From, append may convert input entries directly into resident ones.
  template <std::ranges::contiguous_range Range>
    requires std::constructible_from<T,
                                     const std::ranges::range_value_t<Range>&>
  absl::StatusOr<CowArray> Appended(const Range& values) const {
    using U = std::ranges::range_value_t<Range>;
    return AppendSpan(std::span<const U>(std::ranges::data(values),
                                         std::ranges::size(values)));
  }

 private:
  template <typename U>
  absl::StatusOr<CowArray> AppendSpan(std::span<const U> values) const {
    if (empty()) return From(values);
    if (values.empty()) return *this;
    if (values.size() > std::numeric_limits<std::size_t>::max() - size_)
      return absl::ResourceExhaustedError("copy-on-write array size overflows");
    auto result = *this;
    const auto size = size_ + values.size();
    if (size > ChunkEntries && result.root_.get()->capacity_ != 0) {
      auto root = AllocateBranch(nullptr);
      if (!root.ok()) return root.status();
      static_cast<Branch*>(root->get())->children_[0] = std::move(result.root_);
      result.root_ = std::move(*root);
    }
    const auto last_chunk = (size - 1) / ChunkEntries;
    while ((last_chunk >> result.root_shift_) >= kBranchEntries) {
      auto root = AllocateBranch(nullptr);
      if (!root.ok()) return root.status();
      static_cast<Branch*>(root->get())->children_[0] = std::move(result.root_);
      result.root_ = std::move(*root);
      result.root_shift_ += kBranchBits;
    }
    auto index = size_;
    while (!values.empty()) {
      auto slot = result.MutableChunkSlot(index / ChunkEntries);
      if (!slot.ok()) return slot.status();
      auto& link = **slot;
      const auto offset = index % ChunkEntries;
      const auto count = std::min(ChunkEntries - offset, values.size());
      const auto needed = offset + count;
      auto* chunk = static_cast<Chunk*>(link.get());
      if (!chunk || link.use_count() != 1 || chunk->capacity_ < needed) {
        const auto capacity =
            GrowthCapacity(chunk ? chunk->capacity_ : 0, needed);
        auto detached = AllocateChunk(
            capacity, chunk ? std::span<const T>(chunk->data(), chunk->size_)
                            : std::span<const T>{});
        if (!detached.ok()) return detached.status();
        link = std::move(*detached);
        chunk = static_cast<Chunk*>(link.get());
      }
      std::uninitialized_copy_n(values.begin(), count, chunk->data() + offset);
      chunk->size_ = needed;
      index += count;
      values = values.subspan(count);
    }
    result.size_ = size;
    return result;
  }

 public:
  std::size_t size() const noexcept { return root_ ? size_ : 0; }
  bool empty() const noexcept { return size() == 0; }
  const T& operator[](std::size_t index) const noexcept {
    assert(index < size());
    return FindChunk(index / ChunkEntries)->data()[index % ChunkEntries];
  }
  const T& front() const noexcept { return (*this)[0]; }
  const T& back() const noexcept { return (*this)[size() - 1]; }
  const_iterator begin() const noexcept { return {this, 0}; }
  const_iterator end() const { return {this, size()}; }

  // Mutate this view, preserving snapshots. Only shared paths/chunks detach.
  // Admission failure preserves values but may leave an identical, partially
  // detached pointer path; that accounted path can be reused by a retry.
  absl::Status Set(std::size_t index, const T& value) {
    assert(index < size());
    auto slot = MutableChunkSlot(index / ChunkEntries);
    if (!slot.ok()) return slot.status();
    auto& link = **slot;
    auto* chunk = static_cast<Chunk*>(link.get());
    if (link.use_count() != 1) {
      auto replacement = AllocateChunk(
          chunk->capacity_, std::span<const T>(chunk->data(), chunk->size_));
      if (!replacement.ok()) return replacement.status();
      link = std::move(*replacement);
      chunk = static_cast<Chunk*>(link.get());
    }
    chunk->data()[index % ChunkEntries] = value;
    return absl::OkStatus();
  }

  // Conservative complete-view footprint, including shared nodes and reserved
  // tail capacity. Allocations already own these charges; do not charge this
  // estimate again. Only the last chunk can be partial, so this is O(height).
  std::size_t RetainedBytes() const noexcept {
    if (!root_) return 0;
    const auto chunks = 1 + (size_ - 1) / ChunkEntries;
    std::size_t bytes = 0;
    const auto add = [&](std::size_t count, std::size_t unit) {
      const auto limit = std::numeric_limits<std::size_t>::max();
      bytes = count > (limit - bytes) / unit ? limit : bytes + count * unit;
    };
    add(chunks - 1, ChunkBytes(ChunkEntries));
    add(1, ChunkBytes(FindChunk(chunks - 1)->capacity_));
    if (chunks > 1) {
      for (auto count = chunks;;) {
        count = 1 + (count - 1) / kBranchEntries;
        add(count, AllocationBytes(sizeof(Branch), alignof(Branch)));
        if (count == 1) break;
      }
    }
    return bytes;
  }

 private:
  static std::size_t AllocationBytes(std::size_t bytes,
                                     std::size_t alignment) noexcept {
    const auto usable = AllocatorUsableSizeForRequest(bytes);
    const auto limit = std::numeric_limits<std::size_t>::max();
    if (alignment <= __STDCPP_DEFAULT_NEW_ALIGNMENT__) return usable;
    return usable > limit - alignment ? limit : usable + alignment;
  }
  static std::size_t ChunkBytes(std::size_t capacity) noexcept {
    if (capacity >
        (std::numeric_limits<std::size_t>::max() - ChunkOffset()) / sizeof(T))
      return std::numeric_limits<std::size_t>::max();
    return AllocationBytes(ChunkOffset() + capacity * sizeof(T),
                           kChunkAlignment);
  }
  static absl::Status Oom() {
    return absl::ResourceExhaustedError(
        "OOM copy-on-write array exceeds maxmemory");
  }
  template <typename U>
  static absl::StatusOr<Link> AllocateChunk(std::size_t capacity,
                                            std::span<const U> values) {
    assert(capacity != 0 && capacity <= ChunkEntries &&
           values.size() <= capacity);
    if (capacity >
        (std::numeric_limits<std::size_t>::max() - ChunkOffset()) / sizeof(T))
      return Oom();
    RetainedAllocationDomain domain;
    auto* storage = TryAllocateRetainedBytes(
        domain, ChunkOffset() + capacity * sizeof(T), kChunkAlignment);
    if (!storage) return Oom();
    auto* chunk =
        std::construct_at(static_cast<Chunk*>(storage), capacity, domain);
    Link result(chunk);
    std::uninitialized_copy(values.begin(), values.end(), chunk->data());
    chunk->size_ = values.size();
    return result;
  }
  static absl::StatusOr<Link> AllocateBranch(const Branch* old) {
    RetainedAllocationDomain domain;
    auto* storage =
        TryAllocateRetainedBytes(domain, sizeof(Branch), alignof(Branch));
    if (!storage) return Oom();
    return Link(std::construct_at(static_cast<Branch*>(storage), domain, old));
  }
  static std::size_t GrowthCapacity(std::size_t old, std::size_t needed) {
    const auto doubled = old > ChunkEntries / 2 ? ChunkEntries : old * 2;
    return std::max(needed, std::max<std::size_t>(1, doubled));
  }
  const Chunk* FindChunk(std::size_t index) const noexcept {
    auto* node = root_.get();
    if (node->capacity_ != 0) return static_cast<const Chunk*>(node);
    for (auto shift = root_shift_;; shift -= kBranchBits) {
      node = static_cast<const Branch*>(node)
                 ->children_[(index >> shift) & (kBranchEntries - 1)]
                 .get();
      if (shift == 0) return static_cast<const Chunk*>(node);
    }
  }
  absl::StatusOr<Link*> MutableChunkSlot(std::size_t index) {
    auto* link = &root_;
    if (link->get()->capacity_ != 0) return link;
    for (auto shift = root_shift_;; shift -= kBranchBits) {
      if (!*link || link->use_count() != 1) {
        auto replacement =
            AllocateBranch(static_cast<const Branch*>(link->get()));
        if (!replacement.ok()) return replacement.status();
        *link = std::move(*replacement);
      }
      link = &static_cast<Branch*>(link->get())
                  ->children_[(index >> shift) & (kBranchEntries - 1)];
      if (shift == 0) return link;
    }
  }
  template <typename U>
  static absl::StatusOr<Link> Build(std::span<const U> values, unsigned shift) {
    auto root = AllocateBranch(nullptr);
    if (!root.ok()) return root.status();
    auto& children = static_cast<Branch*>(root->get())->children_;
    const auto child_entries = (std::size_t{1} << shift) * ChunkEntries;
    for (std::size_t slot = 0; !values.empty(); ++slot) {
      const auto count = std::min(child_entries, values.size());
      auto child = shift == 0 ? AllocateChunk(count, values.first(count))
                              : Build(values.first(count), shift - kBranchBits);
      if (!child.ok()) return child.status();
      children[slot] = std::move(*child);
      values = values.subspan(count);
    }
    return root;
  }

  Link root_;
  std::size_t size_ = 0;
  unsigned root_shift_ = 0;
};

}  // namespace lavik
