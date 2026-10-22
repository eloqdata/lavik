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
#include <cassert>
#include <compare>
#include <concepts>
#include <cstddef>
#include <initializer_list>
#include <iterator>
#include <limits>
#include <ranges>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/container/inlined_vector.h"

namespace lavik {

// size_t preserves commands whose aggregate bytes exceed 4 GiB when the query
// limit is raised. Individual RESP bulk lengths still obey the protocol limit.
struct OffsetAndLength {
  std::size_t offset = 0;
  std::size_t length = 0;
};

// A borrowed argument sequence. Packed requests resolve offsets on access;
// existing owned string arrays (replay, snapshots, and internal callers) can
// use the same read-only APIs without allocating an intermediate view array.
// Iteration returns string_view by value. The source must remain alive and
// unmodified for the lifetime of this view and any derived string_view.
// Force-inline hot accessors so layered argument reads do not depend on the
// surrounding command handler's automatic inlining budget.
class CommandArgsView {
 public:
  class Iterator {
   public:
    using value_type = std::string_view;
    using reference = std::string_view;
    using pointer = void;
    using difference_type = std::ptrdiff_t;
    using iterator_category = std::random_access_iterator_tag;
    using iterator_concept = std::random_access_iterator_tag;
    Iterator() = default;
    [[gnu::always_inline]] reference operator*() const {
      if (bytes_ == nullptr)
        return static_cast<const std::string*>(elements_)[index_];
      const auto& arg = static_cast<const OffsetAndLength*>(elements_)[index_];
      return {bytes_ + arg.offset, arg.length};
    }
    [[gnu::always_inline]] reference operator[](difference_type n) const {
      return *(*this + n);
    }
    Iterator& operator++() {
      ++index_;
      return *this;
    }
    Iterator operator++(int) {
      auto old = *this;
      ++*this;
      return old;
    }
    Iterator& operator--() {
      --index_;
      return *this;
    }
    Iterator operator--(int) {
      auto old = *this;
      --*this;
      return old;
    }
    Iterator& operator+=(difference_type n) {
      index_ += n;
      return *this;
    }
    Iterator& operator-=(difference_type n) {
      index_ -= n;
      return *this;
    }
    friend Iterator operator+(Iterator it, difference_type n) {
      return it += n;
    }
    friend Iterator operator+(difference_type n, Iterator it) {
      return it += n;
    }
    friend Iterator operator-(Iterator it, difference_type n) {
      return it -= n;
    }
    friend difference_type operator-(Iterator a, Iterator b) {
      assert(a.elements_ == b.elements_ && a.bytes_ == b.bytes_);
      return a.index_ - b.index_;
    }
    friend bool operator==(Iterator a, Iterator b) = default;
    friend auto operator<=>(Iterator a, Iterator b) {
      assert(a.elements_ == b.elements_ && a.bytes_ == b.bytes_);
      return a.index_ <=> b.index_;
    }

   private:
    friend class CommandArgsView;
    Iterator(const void* elements, const char* bytes, difference_type index)
        : elements_(elements), bytes_(bytes), index_(index) {}
    const void* elements_ = nullptr;
    const char* bytes_ = nullptr;
    difference_type index_ = 0;
  };

  constexpr CommandArgsView() = default;
  CommandArgsView(std::span<const OffsetAndLength> args, const char* bytes)
      : elements_(args.data()), bytes_(bytes), size_(args.size()) {}
  CommandArgsView(const std::string* args, std::size_t count)
      : elements_(args), size_(count) {}
  template <std::ranges::contiguous_range Range>
    requires std::same_as<std::ranges::range_value_t<Range>, std::string>
  CommandArgsView(const Range& args)
      : CommandArgsView(std::ranges::data(args), std::ranges::size(args)) {}

  [[gnu::always_inline]] std::size_t size() const noexcept { return size_; }
  [[gnu::always_inline]] bool empty() const noexcept { return size_ == 0; }
  [[gnu::always_inline]] std::string_view operator[](std::size_t i) const {
    assert(i < size_);
    return begin()[i];
  }
  [[gnu::always_inline]] std::string_view front() const { return (*this)[0]; }
  [[gnu::always_inline]] std::string_view back() const {
    return (*this)[size_ - 1];
  }
  [[gnu::always_inline]] Iterator begin() const {
    return {elements_, bytes_, 0};
  }
  [[gnu::always_inline]] Iterator end() const {
    return {elements_, bytes_, static_cast<std::ptrdiff_t>(size_)};
  }
  CommandArgsView subspan(std::size_t offset,
                          std::size_t count = std::dynamic_extent) const {
    assert(offset <= size_);
    if (count == std::dynamic_extent) count = size_ - offset;
    assert(count <= size_ - offset);
    auto result = *this;
    if (offset != 0) {
      result.elements_ =
          bytes_ != nullptr
              ? static_cast<const void*>(
                    static_cast<const OffsetAndLength*>(elements_) + offset)
              : static_cast<const void*>(
                    static_cast<const std::string*>(elements_) + offset);
    }
    result.size_ = count;
    return result;
  }
  CommandArgsView first(std::size_t count) const { return subspan(0, count); }

 private:
  const void* elements_ = nullptr;
  const char* bytes_ = nullptr;
  std::size_t size_ = 0;
};

// Packed argument builder. Offsets survive buffer growth while parsing; views
// are borrowed only after parsing finishes. Mutations invalidate borrowed
// views. Bytes contain no terminators and preserve empty and binary arguments
// exactly.
class CommandArgs {
 public:
  CommandArgs() = default;
  CommandArgs(std::initializer_list<std::string_view> values) {
    assign(values.begin(), values.end());
  }
  template <typename Iterator>
  CommandArgs(Iterator first, Iterator last) {
    assign(first, last);
  }

  absl::InlinedVector<OffsetAndLength, 8> args;
  absl::InlinedVector<char, 256> bytes;

  [[gnu::always_inline]] operator CommandArgsView() const {
    return {args, bytes.data()};
  }
  [[gnu::always_inline]] std::size_t size() const noexcept {
    return args.size();
  }
  [[gnu::always_inline]] bool empty() const noexcept { return args.empty(); }
  [[gnu::always_inline]] std::size_t capacity() const noexcept {
    return args.capacity();
  }
  [[gnu::always_inline]] std::string_view operator[](std::size_t i) const {
    const auto& arg = args[i];
    return {bytes.data() + arg.offset, arg.length};
  }
  [[gnu::always_inline]] std::string_view front() const { return (*this)[0]; }
  [[gnu::always_inline]] std::string_view back() const {
    return (*this)[size() - 1];
  }
  [[gnu::always_inline]] CommandArgsView::Iterator begin() const {
    return CommandArgsView(*this).begin();
  }
  [[gnu::always_inline]] CommandArgsView::Iterator end() const {
    return CommandArgsView(*this).end();
  }
  void reserve(std::size_t count) { args.reserve(count); }
  void clear() noexcept {
    // Abseil clear() releases heap allocations and restores inline capacity.
    args.clear();
    bytes.clear();
  }
  // Begin one argument before appending any of its bytes. Only the last
  // argument may be extended, so already completed arguments need no fixups.
  void StartArgument() { args.push_back({bytes.size(), 0}); }
  void AppendLast(std::string_view value) {
    if (value.empty()) return;
    if (value.size() > bytes.max_size() - bytes.size())
      throw std::length_error("client argument storage is too large");
    // Input must not alias this buffer: RESP appends read from transport;
    // synthesized commands read another owner. Inline commands decode in place.
    bytes.insert(bytes.end(), value.begin(), value.end());
    args.back().length += value.size();
  }
  // Prepared direct-read bytes are included in the last argument's length;
  // the parser tracks the unwritten tail and never publishes it prematurely.
  void ResizeLast(std::size_t length) {
    if (length > bytes.max_size() - args.back().offset)
      throw std::length_error("client argument storage is too large");
    bytes.resize(args.back().offset + length);
    args.back().length = length;
  }
  std::span<char> MutableLast() {
    const auto& arg = args.back();
    return {bytes.data() + arg.offset, arg.length};
  }
  // RESP verbatim strings can drop their format prefix without shifting the
  // payload. The unused prefix bytes are released with the rest of the command.
  void RemoveLastPrefix(std::size_t count) {
    assert(count <= args.back().length);
    args.back().offset += count;
    args.back().length -= count;
  }
  void push_back(std::string_view value) {
    StartArgument();
    AppendLast(value);
  }
  template <typename Iterator>
  void assign(Iterator first, Iterator last) {
    clear();
    if constexpr (std::forward_iterator<Iterator>) {
      reserve(static_cast<std::size_t>(std::distance(first, last)));
      std::size_t total = 0;
      for (auto it = first; it != last; ++it) {
        const std::string_view value = *it;
        if (value.size() > bytes.max_size() - total)
          throw std::length_error("client argument storage is too large");
        total += value.size();
      }
      bytes.reserve(total);
    }
    for (; first != last; ++first) push_back(*first);
  }
  friend bool operator==(const CommandArgs& a, const CommandArgs& b) {
    return std::ranges::equal(a, b);
  }
};

// Explicit lifetime boundary for metadata that must outlive a request slot.
inline std::vector<std::string> CopyCommandArgs(CommandArgsView args) {
  return {args.begin(), args.end()};
}
inline std::vector<std::string> CopyCommandArgs(
    std::initializer_list<std::string_view> args) {
  return {args.begin(), args.end()};
}

class CommandArgsHandle;

// Never relocate a live slot: its offsets and inline bytes must survive parser,
// pipeline, MULTI, and request handoffs. A connection embeds one storage
// object; concurrent commands own separate heap objects. Parsing may grow
// buffers; completed requests are read-only.
struct CommandArgsStorage : CommandArgs {
  CommandArgsStorage() = default;
  ~CommandArgsStorage() { assert(!borrowed_); }
  CommandArgsStorage(const CommandArgsStorage&) = delete;
  CommandArgsStorage& operator=(const CommandArgsStorage&) = delete;

 private:
  friend class CommandArgsHandle;
  // True only while a handle borrows this externally owned object. Heap-owned
  // objects stay false, letting a one-pointer handle select clear vs. delete.
  bool borrowed_ = false;
};

// Exclusive access to stable argument storage. Moving transfers only a pointer.
// Borrowed storage must outlive its handle. Remote execution borrows an awaited
// request; clearing the embedded object stays on the originating worker.
// Storage selection and read access stay forced-inline for the same reason as
// CommandArgsView's accessors; copying and bulk mutation use normal inlining.
class CommandArgsHandle {
 public:
  CommandArgsHandle() noexcept = default;
  CommandArgsHandle(std::initializer_list<std::string_view> args)
      : CommandArgsHandle() {
    if (args.size() != 0) Get().assign(args.begin(), args.end());
  }
  CommandArgsHandle(CommandArgs&& args) : CommandArgsHandle() {
    if (!args.empty()) Get() = std::move(args);
  }
  template <typename Iterator>
  CommandArgsHandle(Iterator first, Iterator last) : CommandArgsHandle() {
    if (first != last) Get().assign(first, last);
  }
  CommandArgsHandle(const CommandArgsHandle&) = delete;
  CommandArgsHandle& operator=(const CommandArgsHandle&) = delete;
  CommandArgsHandle(CommandArgsHandle&& other) noexcept
      : storage_(std::exchange(other.storage_, nullptr)) {}
  CommandArgsHandle& operator=(CommandArgsHandle&& other) noexcept {
    if (this != &other) {
      Reset();
      storage_ = std::exchange(other.storage_, nullptr);
    }
    return *this;
  }
  ~CommandArgsHandle() { Reset(); }
  // Borrows an externally owned object if it is free; otherwise returns an
  // empty handle so the caller can use separate storage. The owner must remain
  // alive until this handle is released. No allocation or free list is
  // involved.
  [[nodiscard, gnu::always_inline]] static CommandArgsHandle TryBorrow(
      CommandArgsStorage& storage) {
    if (storage.borrowed_) return {};
    storage.borrowed_ = true;
    return CommandArgsHandle(&storage);
  }
  // Clears borrowed storage (including both overflow heaps), or deletes an
  // independently owned object. Additional pipeline/MULTI objects are not
  // cached.
  void Reset() noexcept;
  [[gnu::always_inline]] bool HasStorage() const noexcept {
    return storage_ != nullptr;
  }
  [[nodiscard]] CommandArgsHandle Clone() const { return {begin(), end()}; }
  // Mutable access is for parsing/synthesis and invalidates borrowed views.
  [[gnu::always_inline]] CommandArgs& Get() {
    if (storage_ == nullptr) storage_ = new CommandArgsStorage;
    return *storage_;
  }
  [[gnu::always_inline]] const CommandArgs& Get() const {
    assert(storage_ != nullptr);
    return *storage_;
  }
  [[gnu::always_inline]] std::size_t capacity() const noexcept {
    return storage_ == nullptr ? 0 : storage_->capacity();
  }
  [[gnu::always_inline]] operator CommandArgsView() const {
    return storage_ != nullptr ? CommandArgsView(*storage_) : CommandArgsView{};
  }
  [[gnu::always_inline]] CommandArgsView View() const { return *this; }
  [[gnu::always_inline]] std::size_t size() const noexcept {
    return storage_ == nullptr ? 0 : storage_->size();
  }
  [[gnu::always_inline]] bool empty() const noexcept { return size() == 0; }
  [[gnu::always_inline]] std::string_view operator[](std::size_t i) const {
    return (*storage_)[i];
  }
  [[gnu::always_inline]] std::string_view front() const { return (*this)[0]; }
  [[gnu::always_inline]] std::string_view back() const {
    return (*this)[size() - 1];
  }
  [[gnu::always_inline]] CommandArgsView::Iterator begin() const {
    return View().begin();
  }
  [[gnu::always_inline]] CommandArgsView::Iterator end() const {
    return View().end();
  }
  void clear() noexcept {
    if (storage_ != nullptr) storage_->clear();
  }
  void reserve(std::size_t count) { Get().reserve(count); }
  void push_back(std::string_view value) { Get().push_back(value); }
  template <typename Iterator>
  void assign(Iterator first, Iterator last) {
    Get().assign(first, last);
  }
  template <typename Iterator>
  void insert(CommandArgsView::Iterator pos, Iterator first, Iterator last) {
    // Synthesis only appends; arbitrary insertion would invalidate offsets and
    // borrow semantics. Rewriters build an independent command instead.
    assert(pos == end());
    for (; first != last; ++first) push_back(*first);
  }
  friend bool operator==(const CommandArgsHandle& a, const CommandArgs& b) {
    return std::ranges::equal(a, b);
  }

 private:
  explicit CommandArgsHandle(CommandArgsStorage* storage) : storage_(storage) {}
  CommandArgsStorage* storage_ = nullptr;
};

static_assert(sizeof(CommandArgsHandle) == sizeof(void*));

inline void CommandArgsHandle::Reset() noexcept {
  auto* slot = std::exchange(storage_, nullptr);
  if (slot == nullptr) return;
  if (slot->borrowed_) {
    slot->clear();
    slot->borrowed_ = false;
  } else {
    delete slot;
  }
}

}  // namespace lavik
