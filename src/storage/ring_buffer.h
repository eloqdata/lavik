#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <stdexcept>
#include <utility>
#include <vector>

namespace keylane::storage {

// A single-owner FIFO backed by one contiguous allocation. Push and pop do
// not allocate while spare slots remain. When the ring fills, Grow() allocates
// a replacement buffer and moves live entries in logical FIFO order so the
// new head starts at slot zero.
template <typename T>
class RingBuffer {
 public:
  bool empty() const noexcept { return size_ == 0; }
  std::size_t size() const noexcept { return size_; }
  std::size_t capacity() const noexcept { return slots_.size(); }

  T& front() noexcept {
    assert(!empty());
    return slots_[head_];
  }
  const T& front() const noexcept {
    assert(!empty());
    return slots_[head_];
  }

  T& operator[](std::size_t offset) noexcept {
    assert(offset < size_);
    return slots_[PhysicalIndex(offset)];
  }
  const T& operator[](std::size_t offset) const noexcept {
    assert(offset < size_);
    return slots_[PhysicalIndex(offset)];
  }

  void push_back(T value) {
    if (size_ == slots_.size()) Grow();
    slots_[PhysicalIndex(size_)] = std::move(value);
    ++size_;
  }

  void pop_front() {
    assert(!empty());
    slots_[head_] = T{};
    if (++head_ == slots_.size()) head_ = 0;
    --size_;
    if (size_ == 0) head_ = 0;
  }

  // Release the contents but retain the slots for the next burst.
  void clear() {
    for (std::size_t offset = 0; offset < size_; ++offset) {
      (*this)[offset] = T{};
    }
    head_ = 0;
    size_ = 0;
  }

 private:
  std::size_t PhysicalIndex(std::size_t offset) const noexcept {
    const std::size_t tail_slots = slots_.size() - head_;
    return offset < tail_slots ? head_ + offset : offset - tail_slots;
  }

  void Grow() {
    constexpr std::size_t kInitialSlots = 64;
    const std::size_t old_capacity = slots_.size();
    const std::size_t max_capacity = slots_.max_size();
    const std::size_t new_capacity =
        old_capacity == 0
            ? std::min(kInitialSlots, max_capacity)
            : old_capacity <= max_capacity / 2 ? old_capacity * 2
                                               : max_capacity;
    if (new_capacity <= old_capacity) {
      throw std::length_error("storage publish ring capacity exhausted");
    }
    std::vector<T> replacement(new_capacity);
    for (std::size_t offset = 0; offset < size_; ++offset) {
      replacement[offset] = std::move((*this)[offset]);
    }
    slots_.swap(replacement);
    head_ = 0;
  }

  std::vector<T> slots_;
  std::size_t head_ = 0;
  std::size_t size_ = 0;
};

}  // namespace keylane::storage
