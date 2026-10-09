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
#include <cstddef>
#include <limits>
#include <optional>
#include <span>
#include <stdexcept>
#include <type_traits>
#include <utility>

#include "absl/status/statusor.h"
#include "lavik/local_shared_ptr.h"
#include "lavik/memory.h"
#include "lavik/retained_allocator.h"
#include "lavik/status_macros.h"

namespace lavik {

// Copy-on-write ordered key/value map backed by an AVL tree. Copies retain one
// root; topology updates copy only a logarithmic search path. Large maps buffer
// existing-key updates in one bounded immutable overlay. Overflow folds the
// batch into the tree, copying each shared ancestor only once; lookups and
// iteration resolve the overlay directly. Nodes and overlays admit/account
// their own lifetimes.
//
// Keys and values are trivially copyable; referenced resources remain the
// caller's responsibility. All copies, access and destruction stay on the
// allocating thread.
template <typename Key, typename Value>
class CowMap {
  static_assert(std::is_trivially_copyable_v<Key>);
  static_assert(std::is_trivially_copyable_v<Value>);
  struct Node;
  using Link = LocalSharedPtr<const Node>;
  struct Node {
    std::pair<const Key, Value> entry_;
    Link left_, right_;
    std::size_t size_;
    unsigned height_;
    Node(Key key, Value value, Link left, Link right)
        : entry_(key, value),
          left_(std::move(left)),
          right_(std::move(right)),
          size_(1 + Size(left_) + Size(right_)),
          height_(1 + std::max(Height(left_), Height(right_))) {}
  };

  // Buffer only replacements of existing keys, preserving ordering and
  // subtree sizes. The shared allocation keeps map copies cheap; old snapshots
  // retain their own overlay, and overflow never chains overlays together.
  struct Overlay {
    static constexpr std::size_t kCapacity = 8;
    using Entry = std::pair<const Key, Value>;
    std::array<std::optional<Entry>, kCapacity> entries_;
    std::size_t size_ = 0;
    explicit Overlay(const Overlay* previous) {
      if (previous) {
        size_ = previous->size_;
        for (std::size_t i = 0; i < size_; ++i)
          entries_[i].emplace(*previous->entries_[i]);
      }
    }
    const Entry* Find(Key key) const noexcept {
      for (std::size_t i = 0; i < size_; ++i)
        if (entries_[i]->first == key) return &*entries_[i];
      return nullptr;
    }
  };

 public:
  class const_iterator {
   public:
    using value_type = std::pair<const Key, Value>;
    using reference = const value_type&;
    using pointer = const value_type*;
    reference operator*() const {
      const auto& entry = path_[depth_ - 1]->entry_;
      if (overlay_)
        if (const auto* updated = overlay_->Find(entry.first)) return *updated;
      return entry;
    }
    pointer operator->() const { return &operator*(); }
    const_iterator& operator++() {
      const auto* node = path_[depth_ - 1];
      if (node->right_) {
        node = node->right_.get();
        while (node) {
          path_[depth_++] = node;
          node = node->left_.get();
        }
      } else {
        --depth_;
        while (depth_ && path_[depth_ - 1]->right_.get() == node) {
          node = path_[--depth_];
        }
      }
      return *this;
    }
    bool operator==(const const_iterator& other) const {
      return (depth_ ? path_[depth_ - 1] : nullptr) ==
             (other.depth_ ? other.path_[other.depth_ - 1] : nullptr);
    }

   private:
    friend class CowMap;
    // This stack covers an AVL tree whose node count fits a 64-bit size_t.
    // Iteration never allocates retained/scratch memory.
    const Overlay* overlay_ = nullptr;
    std::array<const Node*, 96> path_{};
    unsigned depth_ = 0;
  };

  // Conservative footprint including shared nodes; allocations already own
  // these charges. Callers may use it for scratch planning, not re-accounting.
  std::size_t RetainedBytes() const noexcept {
    const auto node = AllocationBytes<Node>();
    const auto overlay = overlay_ ? AllocationBytes<Overlay>() : 0;
    const auto limit = std::numeric_limits<std::size_t>::max();
    return size() > (limit - overlay) / node ? limit : size() * node + overlay;
  }

  std::size_t size() const noexcept { return Size(root_); }
  bool empty() const noexcept { return !root_; }
  const_iterator begin() const {
    const_iterator it;
    it.overlay_ = overlay_.get();
    const auto* node = root_.get();
    while (node) {
      it.path_[it.depth_++] = node;
      node = node->left_.get();
    }
    return it;
  }
  const_iterator end() const { return {}; }
  const_iterator find(Key key) const {
    const_iterator it;
    it.overlay_ = overlay_.get();
    const auto* node = root_.get();
    while (node) {
      it.path_[it.depth_++] = node;
      if (key == node->entry_.first) return it;
      node = key < node->entry_.first ? node->left_.get() : node->right_.get();
    }
    return end();
  }
  // Exact metadata lookup without constructing the ancestor stack needed by
  // an iterator. The returned pointer borrows this immutable tree version.
  const Value* Get(Key key) const noexcept {
    if (overlay_)
      if (const auto* entry = overlay_->Find(key)) return &entry->second;
    return GetBase(key);
  }

 private:
  const Value* GetBase(Key key) const noexcept {
    const auto* node = root_.get();
    while (node) {
      if (key == node->entry_.first) return &node->entry_.second;
      node = key < node->entry_.first ? node->left_.get() : node->right_.get();
    }
    return nullptr;
  }

 public:
  const Value& at(Key key) const {
    const auto it = find(key);
    if (it == end()) throw std::out_of_range("copy-on-write map key");
    return it->second;
  }
  // Borrows the metadata at the greatest key <= key, resolving this view's
  // overlay. No predecessor returns nullptr, rather than an empty value.
  const Value* Floor(Key key) const noexcept {
    const Node* found = nullptr;
    auto* node = root_.get();
    while (node) {
      if (node->entry_.first <= key) {
        found = node;
        node = node->right_.get();
      } else
        node = node->left_.get();
    }
    if (!found) return nullptr;
    if (overlay_)
      if (const auto* entry = overlay_->Find(found->entry_.first))
        return &entry->second;
    return &found->entry_.second;
  }
  // Insert/replace in an unpublished map. Allocation failure preserves its
  // previous root and overlay, including when a shared AVL path must rotate.
  absl::Status Set(Key key, Value value) {
    if (overlay_ && overlay_->Find(key)) return SetBuffered(key, value);
    LAVIK_ASSIGN_OR_RETURN(root_, SetNode(root_, key, value));
    return absl::OkStatus();
  }
  // Amortize repeated replacements in large maps. Small maps keep ordinary
  // path copies: the fixed eight-entry allocation and retained base tree can
  // outweigh avoiding a short path, especially with many small maps.
  // Require 1024 entries before starting an overlay; an existing overlay must
  // still resolve its entries if subsequent erases shrink the map. Bulk
  // construction uses Set directly, without an existence lookup per insertion.
  absl::Status SetBuffered(Key key, Value value) {
    const auto* current = overlay_ ? overlay_->Find(key) : nullptr;
    if (current || (size() >= 1024 && GetBase(key))) {
      if (!current && overlay_ && overlay_->size_ == Overlay::kCapacity) {
        // Fold all pending replacements together. Dispersed writes still
        // share ancestor copies, instead of paying for a full path plus an
        // overlay allocation on each eviction. All keys already exist, so
        // subtree shape, sizes and balance cannot change.
        const typename Overlay::Entry incoming(key, value);
        std::array<const typename Overlay::Entry*, Overlay::kCapacity + 1>
            writes;
        for (std::size_t i = 0; i < Overlay::kCapacity; ++i)
          writes[i] = &*overlay_->entries_[i];
        writes.back() = &incoming;
        std::sort(
            writes.begin(), writes.end(),
            [](const auto* a, const auto* b) { return a->first < b->first; });
        LAVIK_ASSIGN_OR_RETURN(root_, ReplaceNodes(root_, writes));
        overlay_ = {};
        return absl::OkStatus();
      }
      auto overlay = MakeOverlay(overlay_.get());
      LAVIK_RETURN_IF_ERROR(overlay);
      auto& updated = **overlay;
      std::size_t slot;
      if (current) {
        slot = 0;
        while (updated.entries_[slot]->first != key) ++slot;
      } else {
        slot = updated.size_++;
      }
      updated.entries_[slot].emplace(key, value);
      overlay_ = std::move(*overlay);
      return absl::OkStatus();
    }
    return Set(key, value);
  }
  // Removes both the base identity and any buffered replacement. Older views
  // retain their original nodes; allocation failure leaves this map intact.
  absl::Status Erase(Key key) {
    auto next = EraseNode(root_, key);
    LAVIK_RETURN_IF_ERROR(next);
    if (overlay_ && overlay_->Find(key)) {
      auto overlay = MakeOverlay(overlay_.get());
      LAVIK_RETURN_IF_ERROR(overlay);
      auto& updated = **overlay;
      std::size_t slot = 0;
      while (updated.entries_[slot]->first != key) ++slot;
      --updated.size_;
      if (slot != updated.size_)
        updated.entries_[slot].emplace(*updated.entries_[updated.size_]);
      updated.entries_[updated.size_].reset();
      overlay_ = updated.size_
                     ? LocalSharedPtr<const Overlay>(std::move(*overlay))
                     : LocalSharedPtr<const Overlay>{};
    }
    root_ = std::move(*next);
    return absl::OkStatus();
  }

 private:
  static absl::StatusOr<Link> ReplaceNodes(
      const Link& node,
      std::span<const typename Overlay::Entry* const> writes) {
    if (writes.empty()) return node;
    assert(node);  // Overlay keys are existing keys; Erase removes overrides.
    const auto pivot = std::lower_bound(
        writes.begin(), writes.end(), node->entry_.first,
        [](const auto* entry, Key key) { return entry->first < key; });
    const auto before = static_cast<std::size_t>(pivot - writes.begin());
    const bool replaces =
        pivot != writes.end() && (*pivot)->first == node->entry_.first;
    auto left = ReplaceNodes(node->left_, writes.first(before));
    LAVIK_RETURN_IF_ERROR(left);
    auto right = ReplaceNodes(node->right_, writes.subspan(before + replaces));
    LAVIK_RETURN_IF_ERROR(right);
    return Make(node->entry_.first,
                replaces ? (*pivot)->second : node->entry_.second,
                std::move(*left), std::move(*right));
  }

  template <typename T>
  static std::size_t AllocationBytes() noexcept {
    // Match AllocateLocalShared's combined allocation, including its control
    // block and retained allocator state. Over-aligned allocations need the
    // same alignment headroom as TryAllocateRetainedBytes.
    using Block = local_shared_detail::Allocation<T, RetainedAllocator<T>>;
    const auto bytes = AllocatorUsableSizeForRequest(sizeof(Block));
    if constexpr (alignof(Block) > __STDCPP_DEFAULT_NEW_ALIGNMENT__) {
      const auto limit = std::numeric_limits<std::size_t>::max();
      return bytes > limit - alignof(Block) ? limit : bytes + alignof(Block);
    }
    return bytes;
  }

  static absl::StatusOr<LocalSharedPtr<Overlay>> MakeOverlay(
      const Overlay* previous) {
    auto reservation = TryReserveMemory(AllocationBytes<Overlay>());
    if (!reservation) {
      RecordMemoryRejection();
      return absl::ResourceExhaustedError(
          "OOM copy-on-write map exceeds maxmemory");
    }
    RetainedAllocationDomain domain{
        .owner_shard_ = CurrentMemoryAccountingShard(),
        .externally_admitted_ = true,
        .externally_accounted_ = false};
    return AllocateLocalShared<Overlay>(RetainedAllocator<Overlay>(domain),
                                        previous);
  }
  static std::size_t Size(const Link& node) { return node ? node->size_ : 0; }
  static unsigned Height(const Link& node) { return node ? node->height_ : 0; }
  static absl::StatusOr<Link> Make(Key key, Value value, Link left,
                                   Link right) {
    auto reservation = TryReserveMemory(AllocationBytes<Node>());
    if (!reservation) {
      RecordMemoryRejection();
      return absl::ResourceExhaustedError(
          "OOM copy-on-write map exceeds maxmemory");
    }
    RetainedAllocationDomain domain{
        .owner_shard_ = CurrentMemoryAccountingShard(),
        .externally_admitted_ = true,
        .externally_accounted_ = false};
    return Link(AllocateLocalShared<Node>(RetainedAllocator<Node>(domain), key,
                                          value, std::move(left),
                                          std::move(right)));
  }
  static absl::StatusOr<Link> Balance(Key key, Value value, Link left,
                                      Link right) {
    if (Height(left) > Height(right) + 1) {
      if (Height(left->left_) >= Height(left->right_)) {
        auto next = Make(key, value, left->right_, right);
        LAVIK_RETURN_IF_ERROR(next);
        return Make(left->entry_.first, left->entry_.second, left->left_,
                    *next);
      }
      const auto pivot = left->right_;
      auto a = Make(left->entry_.first, left->entry_.second, left->left_,
                    pivot->left_);
      LAVIK_RETURN_IF_ERROR(a);
      auto b = Make(key, value, pivot->right_, right);
      LAVIK_RETURN_IF_ERROR(b);
      return Make(pivot->entry_.first, pivot->entry_.second, *a, *b);
    }
    if (Height(right) > Height(left) + 1) {
      if (Height(right->right_) >= Height(right->left_)) {
        auto next = Make(key, value, left, right->left_);
        LAVIK_RETURN_IF_ERROR(next);
        return Make(right->entry_.first, right->entry_.second, *next,
                    right->right_);
      }
      const auto pivot = right->left_;
      auto a = Make(key, value, left, pivot->left_);
      LAVIK_RETURN_IF_ERROR(a);
      auto b = Make(right->entry_.first, right->entry_.second, pivot->right_,
                    right->right_);
      LAVIK_RETURN_IF_ERROR(b);
      return Make(pivot->entry_.first, pivot->entry_.second, *a, *b);
    }
    return Make(key, value, std::move(left), std::move(right));
  }
  static absl::StatusOr<Link> SetNode(const Link& node, Key key, Value value) {
    if (!node || key == node->entry_.first) {
      return Make(key, value, node ? node->left_ : Link{},
                  node ? node->right_ : Link{});
    }
    const bool left = key < node->entry_.first;
    auto child = SetNode(left ? node->left_ : node->right_, key, value);
    LAVIK_RETURN_IF_ERROR(child);
    return Balance(node->entry_.first, node->entry_.second,
                   left ? *child : node->left_, left ? node->right_ : *child);
  }
  static absl::StatusOr<Link> EraseNode(const Link& node, Key key) {
    if (!node) return Link{};
    if (key == node->entry_.first) {
      if (!node->left_) return node->right_;
      if (!node->right_) return node->left_;
      auto* successor = node->right_.get();
      while (successor->left_) successor = successor->left_.get();
      auto right = EraseNode(node->right_, successor->entry_.first);
      LAVIK_RETURN_IF_ERROR(right);
      return Balance(successor->entry_.first, successor->entry_.second,
                     node->left_, *right);
    }
    const bool left = key < node->entry_.first;
    auto child = EraseNode(left ? node->left_ : node->right_, key);
    LAVIK_RETURN_IF_ERROR(child);
    return Balance(node->entry_.first, node->entry_.second,
                   left ? *child : node->left_, left ? node->right_ : *child);
  }
  Link root_;
  LocalSharedPtr<const Overlay> overlay_;
};

}  // namespace lavik
