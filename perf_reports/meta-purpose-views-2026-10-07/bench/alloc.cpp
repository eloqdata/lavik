/* Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0 */
#include <malloc.h>

#include <cstdlib>
#include <new>

#include "metrics.h"

// Meta uses the system C++ allocator. Preserve malloc/free and record requested
// allocation bytes separately from allocator-usable bytes released by delete.
void* operator new(std::size_t size) {
  if (measurement::tracking) {
    ++measurement::counters.allocations;
    measurement::counters.bytes += size;
  }
  if (auto* p = std::malloc(size ? size : 1)) return p;
  throw std::bad_alloc();
}
void* operator new[](std::size_t size) { return ::operator new(size); }
void operator delete(void* p) noexcept {
  if (p && measurement::tracking) {
    ++measurement::counters.frees;
    measurement::counters.freed_usable_bytes += malloc_usable_size(p);
  }
  std::free(p);
}
void operator delete[](void* p) noexcept { ::operator delete(p); }
void operator delete(void* p, std::size_t) noexcept { ::operator delete(p); }
void operator delete[](void* p, std::size_t) noexcept { ::operator delete(p); }
void* operator new(std::size_t size, std::align_val_t align) {
  if (measurement::tracking) {
    ++measurement::counters.allocations;
    measurement::counters.bytes += size;
  }
  void* p = nullptr;
  if (posix_memalign(&p, static_cast<std::size_t>(align), size ? size : 1) == 0)
    return p;
  throw std::bad_alloc();
}
void* operator new[](std::size_t size, std::align_val_t align) {
  return ::operator new(size, align);
}
void operator delete(void* p, std::align_val_t) noexcept {
  ::operator delete(p);
}
void operator delete[](void* p, std::align_val_t) noexcept {
  ::operator delete(p);
}
void operator delete(void* p, std::size_t, std::align_val_t) noexcept {
  ::operator delete(p);
}
void operator delete[](void* p, std::size_t, std::align_val_t) noexcept {
  ::operator delete(p);
}
