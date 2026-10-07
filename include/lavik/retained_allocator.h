#pragma once

/*
 * Retained allocation helpers extracted from scan_hash_map.h.
 *
 * Copyright (c) 2024-present, Valkey contributors
 * Copyright (c) 2006-2020, Redis Ltd.
 * Copyright (c) 2026, Lavik contributors
 *
 * SPDX-License-Identifier: BSD-3-Clause
 * The complete license text is in third_party/valkey/COPYING.
 */

#include <mimalloc.h>

#include <cstddef>
#include <exception>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <type_traits>
#include <utility>

#include "lavik/memory.h"

namespace lavik {

// Identifies which worker owns a retained allocation and whether an enclosing
// operation already supplied admission or accounting. Primary indexes admit a
// whole mutation before touching storage but still account actual allocator
// bytes here; full-sync coverage is both admitted and conservatively accounted
// by its reusable session credit.
struct RetainedAllocationDomain {
  unsigned owner_shard_ = CurrentMemoryAccountingShard();
  bool externally_admitted_ = false;
  bool externally_accounted_ = false;
};

// Returns null only for a deterministic validation/admission rejection. A
// physical allocator failure is process-fatal; callers must not disguise real
// memory exhaustion as an ordinary command error.
inline void* TryAllocateRetainedBytes(const RetainedAllocationDomain& domain,
                                      std::size_t bytes,
                                      std::size_t alignment) {
  if (bytes == 0) bytes = 1;
  std::optional<MemoryReservation> reservation;
  if (!domain.externally_admitted_ && !domain.externally_accounted_) {
    std::size_t admission_bytes = AllocatorUsableSizeForRequest(bytes);
    if (admission_bytes == std::numeric_limits<std::size_t>::max()) {
      return nullptr;
    }
    if (alignment > __STDCPP_DEFAULT_NEW_ALIGNMENT__) {
      if (alignment >
          std::numeric_limits<std::size_t>::max() - admission_bytes) {
        return nullptr;
      }
      admission_bytes += alignment;
    }
    reservation = TryReserveMemory(admission_bytes);
    if (!reservation.has_value()) {
      RecordMemoryRejection();
      return nullptr;
    }
  }

  // Retained domains call mimalloc directly so library users and unit tests do
  // not depend on the final executable having installed the global C++
  // override. Ordinary application new/delete still use the official header.
  void* pointer = alignment > __STDCPP_DEFAULT_NEW_ALIGNMENT__
                      ? mi_malloc_aligned(bytes, alignment)
                      : mi_malloc(bytes);
  if (pointer == nullptr) std::terminate();
  if (!domain.externally_accounted_) {
    const std::size_t usable = mi_usable_size(pointer);
    if (reservation.has_value()) {
      reservation->Commit(usable);
    } else {
      AccountRetainedMemory(domain.owner_shard_, usable);
    }
  }
  return pointer;
}

inline void DeallocateRetainedBytes(const RetainedAllocationDomain& domain,
                                    void* pointer,
                                    std::size_t /*alignment*/) noexcept {
  if (pointer == nullptr) return;
  if (!domain.externally_accounted_) {
    ReleaseRetainedMemory(domain.owner_shard_, mi_usable_size(pointer));
  }
  mi_free(pointer);
}

template <typename T>
class RetainedAllocator {
 public:
  using value_type = T;
  using propagate_on_container_move_assignment = std::true_type;

  RetainedAllocator() noexcept = default;
  explicit RetainedAllocator(RetainedAllocationDomain domain) noexcept
      : domain_(domain) {}

  template <typename U>
  RetainedAllocator(const RetainedAllocator<U>& other) noexcept
      : domain_(other.domain()) {}

  [[nodiscard]] T* allocate(std::size_t count) {
    if (count > std::numeric_limits<std::size_t>::max() / sizeof(T)) {
      std::terminate();
    }
    T* pointer = static_cast<T*>(
        TryAllocateRetainedBytes(domain_, count * sizeof(T), alignof(T)));
    // std::allocator_traits has no error-return channel. Retained containers
    // therefore require their owners to complete deterministic admission
    // before calling into the allocator; violating that invariant is fatal.
    if (pointer == nullptr) std::terminate();
    return pointer;
  }

  void deallocate(T* pointer, std::size_t) noexcept {
    DeallocateRetainedBytes(domain_, pointer, alignof(T));
  }

  const RetainedAllocationDomain& domain() const noexcept { return domain_; }

  template <typename U>
  bool operator==(const RetainedAllocator<U>& other) const noexcept {
    const auto& right = other.domain();
    return domain_.owner_shard_ == right.owner_shard_ &&
           domain_.externally_admitted_ == right.externally_admitted_ &&
           domain_.externally_accounted_ == right.externally_accounted_;
  }

 private:
  RetainedAllocationDomain domain_;
};

template <typename T>
struct RetainedObjectDeleter {
  RetainedAllocator<T> allocator_;

  void operator()(T* pointer) noexcept {
    if (pointer == nullptr) return;
    std::destroy_at(pointer);
    allocator_.deallocate(pointer, 1);
  }
};

template <typename T, typename... Args>
std::unique_ptr<T, RetainedObjectDeleter<T>> TryMakeRetainedUnique(
    RetainedAllocationDomain domain, Args&&... args) {
  RetainedAllocator<T> allocator(domain);
  T* pointer =
      static_cast<T*>(TryAllocateRetainedBytes(domain, sizeof(T), alignof(T)));
  if (pointer == nullptr) {
    return {nullptr, RetainedObjectDeleter<T>{allocator}};
  }
  try {
    std::construct_at(pointer, std::forward<Args>(args)...);
  } catch (...) {
    allocator.deallocate(pointer, 1);
    throw;
  }
  return std::unique_ptr<T, RetainedObjectDeleter<T>>(
      pointer, RetainedObjectDeleter<T>{allocator});
}

template <typename T>
void DestroyRetainedObject(RetainedAllocationDomain domain,
                           T* pointer) noexcept {
  RetainedObjectDeleter<T>{RetainedAllocator<T>(domain)}(pointer);
}

}  // namespace lavik
