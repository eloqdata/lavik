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

#include <mimalloc.h>

#include <cstddef>
#include <exception>
#include <limits>
#include <type_traits>

#include "lavik/memory.h"

namespace lavik::storage {

// Recovery containers grow inside a bounded scan/apply batch. Account their
// actual allocation capacities so retained-index admission and the batch-end
// check see this scratch too. Do not throw a quota exception from allocate:
// Abseil tables do not promise exception safety during rehash. A failed batch
// returns ResourceExhausted before recovery can publish readiness.
//
// Ownership belongs to the container, not the freeing thread. In particular a
// scan batch travels between workers, and WorkerStore containers start life on
// the startup thread. Carrying the shard in the allocator needs no per-record
// header and preserves the compact recovery record sizes.
template <typename T>
class RecoveryAllocator {
 public:
  using value_type = T;
  using propagate_on_container_move_assignment = std::true_type;
  using propagate_on_container_swap = std::true_type;

  RecoveryAllocator() noexcept = default;
  template <typename U>
  RecoveryAllocator(const RecoveryAllocator<U>& other) noexcept
      : owner_(other.owner()) {}

  T* allocate(std::size_t count) {
    if (count > std::numeric_limits<std::size_t>::max() / sizeof(T))
      std::terminate();
    void* data = mi_malloc_aligned(count * sizeof(T), alignof(T));
    if (data == nullptr) std::terminate();
    AccountRetainedMemory(owner_, mi_usable_size(data));
    return static_cast<T*>(data);
  }
  void deallocate(T* data, std::size_t) noexcept {
    if (data == nullptr) return;
    ReleaseRetainedMemory(owner_, mi_usable_size(data));
    mi_free(data);
  }
  unsigned owner() const noexcept { return owner_; }
  template <typename U>
  bool operator==(const RecoveryAllocator<U>& other) const noexcept {
    return owner_ == other.owner();
  }

 private:
  unsigned owner_ = CurrentMemoryAccountingShard();
};

}  // namespace lavik::storage
