#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <limits>
#include <memory>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

namespace keylane::storage {

// A single-owner FIFO backed by one contiguous allocation. Push and pop do
// not allocate while spare slots remain. PrepareCapacity() allocates a
// replacement buffer and moves live entries in logical FIFO order so the new
// head starts at slot zero.
template <typename T, typename Allocator = std::allocator<T>>
class RingBuffer {
 public:
  RingBuffer() = default;
  explicit RingBuffer(const Allocator& allocator) : slots_(allocator) {}

  bool empty() const noexcept { return size_ == 0; }
  std::size_t size() const noexcept { return size_; }
  std::size_t capacity() const noexcept { return slots_.size(); }

  // Requested backing bytes needed by the next push, or zero while an
  // existing slot is available. SIZE_MAX reports element-count overflow.
  std::size_t growth_bytes_if_push() const noexcept {
    if (size_ < slots_.size()) return 0;
    if (size_ == slots_.max_size()) {
      return std::numeric_limits<std::size_t>::max();
    }
    return growth_bytes_for_capacity(size_ + 1);
  }

  // Requested backing bytes for at least minimum_capacity slots. Admission
  // uses queue size plus outstanding item permits, so concurrent commands
  // cannot all claim the same spare slot. Zero means the current allocation is
  // sufficient; SIZE_MAX reports an unrepresentable request.
  std::size_t growth_bytes_for_capacity(
      std::size_t minimum_capacity) const noexcept {
    if (minimum_capacity <= slots_.size()) return 0;
    const std::size_t next = CapacityFor(minimum_capacity);
    return next < minimum_capacity ||
                   next > std::numeric_limits<std::size_t>::max() / sizeof(T)
               ? std::numeric_limits<std::size_t>::max()
               : next * sizeof(T);
  }

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
    PrepareCapacity(size_ + 1);
    push_back_prepared(std::move(value));
  }

  // Physically establishes the capacity promised by admission. This is the
  // only throwing operation needed on the normal publisher path.
  void PrepareCapacity(std::size_t minimum_capacity) {
    if (minimum_capacity <= slots_.size()) return;
    const std::size_t new_capacity = CapacityFor(minimum_capacity);
    if (new_capacity < minimum_capacity) {
      throw std::length_error("storage publish ring capacity exhausted");
    }
    std::vector<T, Allocator> replacement(new_capacity, slots_.get_allocator());
    for (std::size_t offset = 0; offset < size_; ++offset) {
      replacement[offset] = std::move((*this)[offset]);
    }
    slots_.swap(replacement);
    head_ = 0;
  }

  // Admission already owns a distinct slot. Moving into a default-constructed
  // slot must not allocate; the assertion catches permit-accounting mistakes
  // before they become post-mutation allocation failures.
  void push_back_prepared(T value) noexcept {
    static_assert(std::is_nothrow_move_assignable_v<T>);
    assert(size_ < slots_.size());
    slots_[PhysicalIndex(size_)] = std::move(value);
    ++size_;
  }

  // Administrative and recovery paths that do not carry an admission token
  // use one centralized failure boundary instead of duplicating try/catch at
  // every call site.
  bool try_push_back(T value) noexcept {
    try {
      push_back(std::move(value));
      return true;
    } catch (const std::bad_alloc&) {
      return false;
    } catch (const std::length_error&) {
      return false;
    }
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
  std::size_t CapacityFor(std::size_t minimum_capacity) const noexcept {
    constexpr std::size_t kInitialSlots = 64;
    const std::size_t max_capacity = slots_.max_size();
    if (minimum_capacity > max_capacity) return max_capacity;
    std::size_t capacity = slots_.size();
    if (capacity == 0) capacity = std::min(kInitialSlots, max_capacity);
    while (capacity < minimum_capacity) {
      if (capacity > max_capacity / 2) return max_capacity;
      capacity *= 2;
    }
    return capacity;
  }

  std::size_t PhysicalIndex(std::size_t offset) const noexcept {
    const std::size_t tail_slots = slots_.size() - head_;
    return offset < tail_slots ? head_ + offset : offset - tail_slots;
  }

  std::vector<T, Allocator> slots_;
  std::size_t head_ = 0;
  std::size_t size_ = 0;
};

}  // namespace keylane::storage
