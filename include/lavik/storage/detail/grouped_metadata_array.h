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
#include <cassert>
#include <compare>
#include <cstddef>
#include <iterator>
#include <limits>
#include <span>
#include <type_traits>
#include <utility>
#include <variant>

#include "absl/status/statusor.h"
#include "lavik/local_shared_ptr.h"
#include "lavik/memory.h"

namespace lavik::storage {

// Immutable-view routing metadata, never value payloads. Both data chunks and
// their radix pointer tree are owner-local copy-on-write allocations. Updating
// an unpublished view detaches only the root-to-chunk path, so a point write
// does not retain/release pointers to every chunk of a large collection.
// Each allocation admits/accounts itself once until its last view releases it.
// Access and destruction stay on the key owner, as with Hash routing nodes.
template <typename T, std::size_t ChunkEntries = 32>
class GroupedMetadataArray {
  static_assert(ChunkEntries != 0 && std::is_trivially_destructible_v<T>);
  static constexpr unsigned kBranchBits = 5;
  static constexpr std::size_t kBranchEntries = 1 << kBranchBits;
  struct Chunk {
    std::array<T, ChunkEntries> entries_{};
  };
  struct Branch {
    using Chunks = std::array<LocalSharedPtr<Chunk>, kBranchEntries>;
    using Children = std::array<LocalSharedPtr<Branch>, kBranchEntries>;
    explicit Branch(bool leaf) {
      if (!leaf) entries_.template emplace<Children>();
    }
    std::variant<Chunks, Children> entries_;
  };

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
    friend class GroupedMetadataArray;
    const_iterator(const GroupedMetadataArray* owner, std::size_t index)
        : owner_(owner), index_(index) {}
    const GroupedMetadataArray* owner_ = nullptr;
    std::size_t index_ = 0;
  };

  // Build only after recovery/planning has checked the complete metadata.
  static absl::StatusOr<GroupedMetadataArray> From(std::span<const T> values) {
    GroupedMetadataArray result;
    if (values.empty()) return result;
    const auto chunks = 1 + (values.size() - 1) / ChunkEntries;
    for (auto remaining = (chunks - 1) >> kBranchBits; remaining != 0;
         remaining >>= kBranchBits)
      result.root_shift_ += kBranchBits;
    auto root = Build(values, result.root_shift_);
    if (!root.ok()) return root.status();
    result.root_ = std::move(*root);
    result.size_ = values.size();
    return result;
  }

  std::size_t size() const noexcept { return root_ ? size_ : 0; }
  bool empty() const noexcept { return size() == 0; }
  const T& operator[](std::size_t index) const noexcept {
    assert(index < size());
    const auto chunk_index = index / ChunkEntries;
    const auto* branch = root_.get();
    for (auto shift = root_shift_; shift != 0; shift -= kBranchBits)
      branch =
          std::get<typename Branch::Children>(
              branch->entries_)[(chunk_index >> shift) & (kBranchEntries - 1)]
              .get();
    return std::get<typename Branch::Chunks>(
               branch->entries_)[chunk_index & (kBranchEntries - 1)]
        ->entries_[index % ChunkEntries];
  }
  const T& front() const noexcept { return (*this)[0]; }
  const T& back() const noexcept { return (*this)[size() - 1]; }
  const_iterator begin() const noexcept { return {this, 0}; }
  const_iterator end() const noexcept { return {this, size()}; }

  // Only an unpublished directory may call Set. Admission failure preserves
  // all values, but may leave an identical, partially detached pointer path.
  // That path remains valid and accounted; a retry can finish detaching it.
  absl::Status Set(std::size_t index, const T& value) {
    assert(index < size());
    const auto chunk_index = index / ChunkEntries;
    auto* link = &root_;
    for (auto shift = root_shift_;; shift -= kBranchBits) {
      if (link->use_count() != 1) {
        auto replacement = Allocate<Branch>(**link);
        if (!replacement.ok()) return replacement.status();
        *link = std::move(*replacement);
      }
      if (shift == 0) break;
      link = &std::get<typename Branch::Children>(
          (*link)->entries_)[(chunk_index >> shift) & (kBranchEntries - 1)];
    }
    auto& chunk = std::get<typename Branch::Chunks>(
        (*link)->entries_)[chunk_index & (kBranchEntries - 1)];
    if (chunk.use_count() != 1) {
      auto replacement = Allocate<Chunk>(*chunk);
      if (!replacement.ok()) return replacement.status();
      chunk = std::move(*replacement);
    }
    chunk->entries_[index % ChunkEntries] = value;
    return absl::OkStatus();
  }

  // Conservative complete-view footprint for scratch planning, including
  // shared chunks and every pointer-tree level. Ownership already accounts
  // these bytes; do not charge this estimate again on publication.
  std::size_t RetainedBytes() const noexcept {
    if (!root_) return 0;
    const auto chunks = 1 + (size_ - 1) / ChunkEntries;
    std::size_t bytes = 0;
    const auto add = [&](std::size_t count, std::size_t unit) {
      const auto limit = std::numeric_limits<std::size_t>::max();
      bytes = count > (limit - bytes) / unit ? limit : bytes + count * unit;
    };
    add(chunks, AllocatorUsableSizeForRequest(sizeof(Chunk) + 1024));
    for (auto count = chunks;;) {
      count = 1 + (count - 1) / kBranchEntries;
      add(count, AllocatorUsableSizeForRequest(sizeof(Branch) + 1024));
      if (count == 1) break;
    }
    return bytes;
  }

 private:
  template <typename U, typename... Args>
  static absl::StatusOr<LocalSharedPtr<U>> Allocate(Args&&... args) {
    auto reservation =
        TryReserveMemory(AllocatorUsableSizeForRequest(sizeof(U) + 1024));
    if (!reservation) {
      RecordMemoryRejection();
      return absl::ResourceExhaustedError(
          "OOM ordered metadata exceeds maxmemory");
    }
    RetainedAllocationDomain domain{
        .owner_shard_ = CurrentMemoryAccountingShard(),
        .externally_admitted_ = true,
        .externally_accounted_ = false};
    return AllocateLocalShared<U>(RetainedAllocator<U>(domain),
                                  std::forward<Args>(args)...);
  }

  static absl::StatusOr<LocalSharedPtr<Branch>> Build(std::span<const T> values,
                                                      unsigned shift) {
    auto branch = Allocate<Branch>(shift == 0);
    if (!branch.ok()) return branch.status();
    if (shift == 0) {
      auto& chunks = std::get<typename Branch::Chunks>((*branch)->entries_);
      for (std::size_t slot = 0; !values.empty(); ++slot) {
        auto chunk = Allocate<Chunk>();
        if (!chunk.ok()) return chunk.status();
        const auto count = std::min(ChunkEntries, values.size());
        std::copy_n(values.begin(), count, (*chunk)->entries_.begin());
        chunks[slot] = std::move(*chunk);
        values = values.subspan(count);
      }
    } else {
      auto& children = std::get<typename Branch::Children>((*branch)->entries_);
      // From derives the height from the number of complete chunks preceding
      // the last element, so this product cannot exceed the original span.
      const auto child_entries = (std::size_t{1} << shift) * ChunkEntries;
      for (std::size_t slot = 0; !values.empty(); ++slot) {
        const auto count = std::min(child_entries, values.size());
        auto child = Build(values.first(count), shift - kBranchBits);
        if (!child.ok()) return child.status();
        children[slot] = std::move(*child);
        values = values.subspan(count);
      }
    }
    return branch;
  }

  LocalSharedPtr<Branch> root_;
  std::size_t size_ = 0;
  unsigned root_shift_ = 0;
};

}  // namespace lavik::storage
