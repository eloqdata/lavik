#pragma once

/*
 * ScanHashMap is derived from Valkey's src/hashtable.c at commit
 * 21c0d49a0 (Valkey 8.0.8-180-g21c0d49a0).
 *
 * Copyright (c) 2024-present, Valkey contributors
 * Copyright (c) 2006-2020, Redis Ltd.
 * Copyright (c) 2026, Keylane contributors
 *
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * The complete license text is in third_party/valkey/COPYING. This C++
 * specialization retains Valkey's cache-line bucket layout, incremental
 * two-table resizing, and stateless reverse-bit scan algorithm. It replaces
 * Valkey runtime dependencies and generic callbacks with Keylane-owned entries.
 */

#include <mimalloc.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/container/inlined_vector.h"
#include "keylane/memory.h"
#include "keylane/storage/format.h"

namespace keylane::storage {

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

// Worker-local backing store for compact hash-table entry handles. Small pages
// are carved from 1 MiB spans so mimalloc pays the 64 KiB alignment overhead
// once per 16 logical pages instead of once per page. Large entries still own
// dedicated 64 KiB-aligned allocations. Mimalloc remains responsible for OS
// memory, accounting, and purge behavior; only slots and span pages are managed
// here. A page belongs to one size class, and freed slots carry their own
// intrusive free-list link. Empty logical pages are reusable immediately, but
// their backing memory is returned only when all sixteen pages in the span are
// empty; this is the reclaim-granularity cost of amortizing alignment waste.
//
// Handles are runtime-only. Zero is invalid; the high 21 bits select a page
// directory entry and the low 11 bits select a slot in that page. Page IDs may
// be reused only after the last live slot is gone. Callers must therefore prove
// bucket membership before resolving an identity retained across suspension.
class ScanHashMapEntryArena {
 public:
  using Handle = std::uint32_t;

  struct Allocation {
    Handle handle_ = 0;
    void* pointer_ = nullptr;
  };

  static constexpr std::size_t kPageBytes = 64 * 1024;
  static constexpr std::size_t kPagesPerSpan = 16;
  static constexpr std::size_t kSpanBytes = kPagesPerSpan * kPageBytes;
  // A 64 KiB-aligned allocation may consume one extra alignment unit in the
  // allocator size class. External admission users must cover this whole
  // first-span cost because the arena deliberately skips its own reservation.
  static constexpr std::size_t kSmallSpanAdmissionBytes =
      kSpanBytes + kPageBytes;
  static constexpr unsigned kSlotBits = 11;
  static constexpr Handle kSlotMask = (Handle{1} << kSlotBits) - 1;
  static constexpr std::uint32_t kMaximumPageId =
      (std::uint32_t{1} << (32 - kSlotBits)) - 1;
  static_assert(std::has_single_bit(kPageBytes));

  explicit ScanHashMapEntryArena(
      std::uint32_t maximum_page_id = kMaximumPageId,
      bool externally_admitted = false, bool externally_accounted = false,
      unsigned owner_shard = CurrentMemoryAccountingShard());
  ScanHashMapEntryArena(const ScanHashMapEntryArena&) = delete;
  ScanHashMapEntryArena& operator=(const ScanHashMapEntryArena&) = delete;
  ~ScanHashMapEntryArena();

  // Returns false only when this allocation would need a new page and every
  // encodable page ID is already live. Physical allocator failure is fatal.
  bool CanAllocate(std::size_t bytes) const noexcept;
  // Returns conservative allocator headroom when bytes cannot reuse a live
  // page. It includes the exact retained growth of the directory array plus
  // aligned page/span storage. Availability and recycled-ID metadata are
  // intrusive and require no separate capacity growth; map admission adds room
  // for its own bucket and mutation bookkeeping.
  std::size_t AllocationBytesIfNewPage(std::size_t bytes) const noexcept;
  Allocation Allocate(std::size_t bytes);
  void Deallocate(Handle handle) noexcept;

  // Resolve translates a handle read from an occupied live bucket. Debug
  // builds validate that ownership invariant; release builds rely on it and
  // do not turn every lookup into defensive arena bookkeeping.
  void* Resolve(Handle handle) const noexcept;
  // HandleOf accepts an address owned by this arena and returns zero when the
  // aligned page belongs to a different arena.
  Handle HandleOf(const void* pointer) const noexcept;

  std::size_t allocated_pages() const noexcept { return allocated_pages_; }
  std::size_t allocated_spans() const noexcept { return allocated_spans_; }
  std::uint32_t maximum_page_id() const noexcept { return maximum_page_id_; }
  bool externally_admitted() const noexcept { return externally_admitted_; }
  bool externally_accounted() const noexcept {
    return allocation_domain_.externally_accounted_;
  }
  const RetainedAllocationDomain& allocation_domain() const noexcept {
    return allocation_domain_;
  }

 private:
  static constexpr std::size_t kPageHeaderBytes = 64;
  static constexpr std::uint16_t kNoSlot =
      std::numeric_limits<std::uint16_t>::max();
  static constexpr std::uint8_t kLargeClassMarker = 0x3f;
  static constexpr std::uint8_t kFreePageMarker = 0x3e;
  static constexpr std::size_t kSmallClassCount = 53;
  static constexpr std::size_t kInitialDirectoryCapacity = 4096;

  struct SmallSpan;

  struct alignas(64) PageHeader {
    std::uint32_t page_id_ = 0;
    std::uint32_t allocation_bytes_ = 0;
    std::uint32_t block_size_ = 0;
    std::uint32_t available_previous_ = 0;
    std::uint32_t available_next_ = 0;
    SmallSpan* small_span_ = nullptr;
    std::uint16_t capacity_ = 0;
    std::uint16_t next_unused_ = 0;
    std::uint16_t free_head_ = kNoSlot;
    std::uint16_t live_count_ = 0;
    std::uint8_t class_index_ = 0;
    std::uint8_t span_slot_ = 0;
    bool listed_available_ = false;
    std::array<std::byte, 21> padding_{};
  };

  static_assert(sizeof(PageHeader) == kPageHeaderBytes);

  struct SmallSpan {
    void* storage_ = nullptr;
    SmallSpan* all_previous_ = nullptr;
    SmallSpan* all_next_ = nullptr;
    SmallSpan* available_previous_ = nullptr;
    SmallSpan* available_next_ = nullptr;
    std::uint16_t free_page_mask_ =
        static_cast<std::uint16_t>((std::uint32_t{1} << kPagesPerSpan) - 1);
    std::uint8_t live_pages_ = 0;
    bool listed_available_ = false;
  };

  static constexpr std::array<std::uint16_t, kSmallClassCount>
  BuildClassSizes() noexcept;
  static constexpr std::array<std::uint8_t, 513> BuildClassLookup() noexcept;

  static const std::array<std::uint16_t, kSmallClassCount> kClassSizes;
  static const std::array<std::uint8_t, 513> kClassLookup;

  static std::uint8_t ClassFor(std::size_t bytes) noexcept;
  static std::size_t ClassBytes(std::uint8_t class_index) noexcept;
  static std::byte* SlotAddress(PageHeader* page, std::uint16_t slot) noexcept;
  static const std::byte* SlotAddress(const PageHeader* page,
                                      std::uint16_t slot) noexcept;

  std::uint64_t DirectoryDescriptor(std::uint32_t page_id) const noexcept;
  void SetDirectoryDescriptor(std::uint32_t page_id,
                              std::uint64_t descriptor) noexcept;
  std::size_t NextDirectoryCapacity() const noexcept;
  bool EnsureDirectoryCapacity();
  std::size_t DirectoryGrowthBytesForNextPage() const noexcept;
  std::uint32_t AllocatePageId();
  void ReleasePageId(std::uint32_t page_id) noexcept;
  SmallSpan* AllocateSmallSpan();
  void ReleaseSmallSpan(SmallSpan* span) noexcept;
  void AddAvailableSpan(SmallSpan* span);
  void RemoveAvailableSpan(SmallSpan* span) noexcept;
  PageHeader* AllocatePage(std::uint8_t class_index,
                           std::size_t requested_bytes);
  void ReleasePage(PageHeader* page) noexcept;
  void AddAvailable(PageHeader* page);
  void RemoveAvailable(PageHeader* page) noexcept;
  PageHeader* PageFor(Handle handle) const noexcept;

  std::uint32_t maximum_page_id_ = kMaximumPageId;
  bool externally_admitted_ = false;
  RetainedAllocationDomain allocation_domain_;
  std::size_t allocated_pages_ = 0;
  std::size_t allocated_spans_ = 0;
  // Directory slot zero stays empty so handle zero is the null reference.
  // A normal descriptor packs the 64 KiB-aligned page address with its small
  // block size in the low 16 bits; the large marker reads its variable size
  // from the page header. The directory is a raw, deliberately uninitialized
  // array with an explicit growth policy. Fresh IDs advance monotonically, so
  // only the copied prefix and newly published slot are ever read. Released
  // slots encode the next free ID in-band, eliminating a second
  // capacity-growing array while keeping ReleasePage noexcept.
  std::uint64_t* directory_ = nullptr;
  std::size_t directory_capacity_ = 0;
  std::uint32_t next_page_id_ = 1;
  std::uint32_t free_page_id_head_ = 0;
  std::array<std::uint32_t, kSmallClassCount> available_page_heads_{};
  SmallSpan* small_spans_head_ = nullptr;
  SmallSpan* available_spans_head_ = nullptr;
};

constexpr std::array<std::uint16_t, ScanHashMapEntryArena::kSmallClassCount>
ScanHashMapEntryArena::BuildClassSizes() noexcept {
  std::array<std::uint16_t, kSmallClassCount> sizes{};
  std::size_t index = 0;
  for (std::uint16_t size = 32; size <= 128; size += 8) {
    sizes[index++] = size;
  }
  for (std::uint16_t size = 144; size <= 256; size += 16) {
    sizes[index++] = size;
  }
  for (std::uint16_t size = 288; size <= 512; size += 32) {
    sizes[index++] = size;
  }
  for (std::uint16_t size = 576; size <= 1024; size += 64) {
    sizes[index++] = size;
  }
  for (std::uint16_t size = 1152; size <= 2048; size += 128) {
    sizes[index++] = size;
  }
  for (std::uint16_t size = 2304; size <= 4096; size += 256) {
    sizes[index++] = size;
  }
  assert(index == sizes.size());
  return sizes;
}

constexpr std::array<std::uint8_t, 513>
ScanHashMapEntryArena::BuildClassLookup() noexcept {
  std::array<std::uint8_t, 513> lookup{};
  for (std::size_t units = 0; units < lookup.size(); ++units) {
    const std::size_t bytes = units * 8;
    std::size_t index = 0;
    while (index + 1 < kClassSizes.size() && kClassSizes[index] < bytes) {
      ++index;
    }
    lookup[units] = static_cast<std::uint8_t>(index);
  }
  return lookup;
}

inline const std::array<std::uint16_t, ScanHashMapEntryArena::kSmallClassCount>
    ScanHashMapEntryArena::kClassSizes =
        ScanHashMapEntryArena::BuildClassSizes();
inline const std::array<std::uint8_t, 513> ScanHashMapEntryArena::kClassLookup =
    ScanHashMapEntryArena::BuildClassLookup();

inline ScanHashMapEntryArena::ScanHashMapEntryArena(
    std::uint32_t maximum_page_id, bool externally_admitted,
    bool externally_accounted, unsigned owner_shard)
    : maximum_page_id_(std::min(maximum_page_id, kMaximumPageId)),
      externally_admitted_(externally_admitted),
      allocation_domain_{.owner_shard_ = owner_shard,
                         .externally_admitted_ = externally_admitted,
                         .externally_accounted_ = externally_accounted} {}

inline ScanHashMapEntryArena::~ScanHashMapEntryArena() {
  for (std::uint32_t page_id = 1; page_id < next_page_id_; ++page_id) {
    const std::uint64_t descriptor = DirectoryDescriptor(page_id);
    if (descriptor == 0 ||
        (descriptor & std::uint64_t{0xffff}) == kFreePageMarker) {
      continue;
    }
    auto* page =
        reinterpret_cast<PageHeader*>(descriptor & ~std::uint64_t{0xffff});
    assert(page->live_count_ == 0 &&
           "entry arena outlived a map that still owns entries");
    if (page->small_span_ == nullptr) {
      DeallocateRetainedBytes(allocation_domain_, page, kPageBytes);
    }
  }
  while (small_spans_head_ != nullptr) {
    SmallSpan* span = small_spans_head_;
    small_spans_head_ = span->all_next_;
    DeallocateRetainedBytes(allocation_domain_, span->storage_, kPageBytes);
    DestroyRetainedObject(allocation_domain_, span);
  }
  if (directory_ != nullptr) {
    DeallocateRetainedBytes(allocation_domain_, directory_,
                            alignof(std::uint64_t));
  }
}

inline std::uint8_t ScanHashMapEntryArena::ClassFor(
    std::size_t bytes) noexcept {
  if (bytes > 4096) return kLargeClassMarker;
  const std::size_t units = (std::max<std::size_t>(bytes, 1) + 7) >> 3;
  return kClassLookup[units];
}

inline std::size_t ScanHashMapEntryArena::ClassBytes(
    std::uint8_t class_index) noexcept {
  assert(class_index < kClassSizes.size());
  return kClassSizes[class_index];
}

inline std::byte* ScanHashMapEntryArena::SlotAddress(
    PageHeader* page, std::uint16_t slot) noexcept {
  return reinterpret_cast<std::byte*>(page) + kPageHeaderBytes +
         static_cast<std::size_t>(slot) * page->block_size_;
}

inline const std::byte* ScanHashMapEntryArena::SlotAddress(
    const PageHeader* page, std::uint16_t slot) noexcept {
  return reinterpret_cast<const std::byte*>(page) + kPageHeaderBytes +
         static_cast<std::size_t>(slot) * page->block_size_;
}

inline std::uint64_t ScanHashMapEntryArena::DirectoryDescriptor(
    std::uint32_t page_id) const noexcept {
  assert(page_id != 0 && page_id < next_page_id_);
  assert(directory_ != nullptr && page_id < directory_capacity_);
  return directory_[page_id];
}

inline void ScanHashMapEntryArena::SetDirectoryDescriptor(
    std::uint32_t page_id, std::uint64_t descriptor) noexcept {
  assert(page_id != 0 && page_id < next_page_id_);
  assert(directory_ != nullptr && page_id < directory_capacity_);
  directory_[page_id] = descriptor;
}

inline std::size_t ScanHashMapEntryArena::NextDirectoryCapacity()
    const noexcept {
  const std::size_t maximum_capacity =
      static_cast<std::size_t>(maximum_page_id_) + 1;
  if (directory_capacity_ >= maximum_capacity) return directory_capacity_;
  if (directory_capacity_ == 0) {
    return std::min(kInitialDirectoryCapacity, maximum_capacity);
  }
  return std::min(directory_capacity_ * 2, maximum_capacity);
}

inline bool ScanHashMapEntryArena::EnsureDirectoryCapacity() {
  if (next_page_id_ < directory_capacity_) return true;
  const std::size_t next_capacity = NextDirectoryCapacity();
  if (next_capacity <= directory_capacity_ ||
      next_capacity >
          std::numeric_limits<std::size_t>::max() / sizeof(std::uint64_t)) {
    return false;
  }
  auto* next = static_cast<std::uint64_t*>(TryAllocateRetainedBytes(
      allocation_domain_, next_capacity * sizeof(std::uint64_t),
      alignof(std::uint64_t)));
  if (next == nullptr) return false;
  // Slot zero is permanently invalid. Copy only descriptors that have been
  // published; the unused suffix stays uninitialized until fresh IDs consume
  // it, avoiding vector value-initialization and a capacity-sized memset.
  if (directory_ != nullptr && next_page_id_ > 1) {
    std::memcpy(next + 1, directory_ + 1,
                (next_page_id_ - 1) * sizeof(std::uint64_t));
  }
  if (directory_ != nullptr) {
    DeallocateRetainedBytes(allocation_domain_, directory_,
                            alignof(std::uint64_t));
  }
  directory_ = next;
  directory_capacity_ = next_capacity;
  return true;
}

inline std::size_t ScanHashMapEntryArena::DirectoryGrowthBytesForNextPage()
    const noexcept {
  if (free_page_id_head_ != 0 || next_page_id_ > maximum_page_id_) return 0;
  if (next_page_id_ < directory_capacity_) return 0;
  const std::size_t next_capacity = NextDirectoryCapacity();
  if (next_capacity <= directory_capacity_ ||
      next_capacity >
          std::numeric_limits<std::size_t>::max() / sizeof(std::uint64_t)) {
    return std::numeric_limits<std::size_t>::max();
  }
  const std::size_t next_bytes =
      AllocatorUsableSizeForRequest(next_capacity * sizeof(std::uint64_t));
  const std::size_t current_bytes =
      directory_ == nullptr ? 0 : mi_usable_size(directory_);
  return next_bytes >= current_bytes ? next_bytes - current_bytes
                                     : std::numeric_limits<std::size_t>::max();
}

inline std::uint32_t ScanHashMapEntryArena::AllocatePageId() {
  if (free_page_id_head_ != 0) {
    const std::uint32_t page_id = free_page_id_head_;
    const std::uint64_t descriptor = DirectoryDescriptor(page_id);
    assert((descriptor & std::uint64_t{0xffff}) == kFreePageMarker);
    free_page_id_head_ = static_cast<std::uint32_t>(descriptor >> 16);
    SetDirectoryDescriptor(page_id, 0);
    return page_id;
  }
  if (next_page_id_ > maximum_page_id_) return 0;
  const std::uint32_t page_id = next_page_id_;
  if (!EnsureDirectoryCapacity()) return 0;
  ++next_page_id_;
  SetDirectoryDescriptor(page_id, 0);
  return page_id;
}

inline void ScanHashMapEntryArena::ReleasePageId(
    std::uint32_t page_id) noexcept {
  assert(page_id != 0 && page_id < next_page_id_ &&
         DirectoryDescriptor(page_id) == 0);
  SetDirectoryDescriptor(
      page_id,
      (static_cast<std::uint64_t>(free_page_id_head_) << 16) | kFreePageMarker);
  free_page_id_head_ = page_id;
}

inline void ScanHashMapEntryArena::AddAvailable(PageHeader* page) {
  assert(page != nullptr && page->class_index_ < kSmallClassCount &&
         !page->listed_available_);
  std::uint32_t& head = available_page_heads_[page->class_index_];
  page->available_previous_ = 0;
  page->available_next_ = head;
  if (head != 0) {
    PageHeader* next = PageFor(head << kSlotBits);
    assert(next != nullptr);
    next->available_previous_ = page->page_id_;
  }
  head = page->page_id_;
  page->listed_available_ = true;
}

inline void ScanHashMapEntryArena::RemoveAvailable(PageHeader* page) noexcept {
  if (!page->listed_available_) return;
  if (page->available_previous_ == 0) {
    std::uint32_t& head = available_page_heads_[page->class_index_];
    assert(head == page->page_id_);
    head = page->available_next_;
  } else {
    PageHeader* previous = PageFor(page->available_previous_ << kSlotBits);
    assert(previous != nullptr);
    previous->available_next_ = page->available_next_;
  }
  if (page->available_next_ != 0) {
    PageHeader* next = PageFor(page->available_next_ << kSlotBits);
    assert(next != nullptr);
    next->available_previous_ = page->available_previous_;
  }
  page->available_previous_ = 0;
  page->available_next_ = 0;
  page->listed_available_ = false;
}

inline void ScanHashMapEntryArena::AddAvailableSpan(SmallSpan* span) {
  assert(span != nullptr && span->free_page_mask_ != 0 &&
         !span->listed_available_);
  span->available_previous_ = nullptr;
  span->available_next_ = available_spans_head_;
  if (available_spans_head_ != nullptr) {
    available_spans_head_->available_previous_ = span;
  }
  available_spans_head_ = span;
  span->listed_available_ = true;
}

inline void ScanHashMapEntryArena::RemoveAvailableSpan(
    SmallSpan* span) noexcept {
  if (!span->listed_available_) return;
  if (span->available_previous_ == nullptr) {
    assert(available_spans_head_ == span);
    available_spans_head_ = span->available_next_;
  } else {
    span->available_previous_->available_next_ = span->available_next_;
  }
  if (span->available_next_ != nullptr) {
    span->available_next_->available_previous_ = span->available_previous_;
  }
  span->available_previous_ = nullptr;
  span->available_next_ = nullptr;
  span->listed_available_ = false;
}

inline ScanHashMapEntryArena::SmallSpan*
ScanHashMapEntryArena::AllocateSmallSpan() {
  auto span_owner = TryMakeRetainedUnique<SmallSpan>(allocation_domain_);
  if (span_owner == nullptr) return nullptr;
  auto* span = span_owner.get();
  span->storage_ =
      TryAllocateRetainedBytes(allocation_domain_, kSpanBytes, kPageBytes);
  if (span->storage_ == nullptr) return nullptr;
  span->all_previous_ = nullptr;
  span->all_next_ = small_spans_head_;
  if (small_spans_head_ != nullptr) small_spans_head_->all_previous_ = span;
  small_spans_head_ = span;
  ++allocated_spans_;
  AddAvailableSpan(span);
  return span_owner.release();
}

inline void ScanHashMapEntryArena::ReleaseSmallSpan(SmallSpan* span) noexcept {
  assert(span != nullptr && span->live_pages_ == 0);
  RemoveAvailableSpan(span);
  if (span->all_previous_ == nullptr) {
    assert(small_spans_head_ == span);
    small_spans_head_ = span->all_next_;
  } else {
    span->all_previous_->all_next_ = span->all_next_;
  }
  if (span->all_next_ != nullptr) {
    span->all_next_->all_previous_ = span->all_previous_;
  }
  assert(allocated_spans_ != 0);
  --allocated_spans_;
  DeallocateRetainedBytes(allocation_domain_, span->storage_, kPageBytes);
  DestroyRetainedObject(allocation_domain_, span);
}

inline ScanHashMapEntryArena::PageHeader* ScanHashMapEntryArena::AllocatePage(
    std::uint8_t class_index, std::size_t requested_bytes) {
  const std::uint32_t page_id = AllocatePageId();
  if (page_id == 0) return nullptr;
  const bool large = class_index == kLargeClassMarker;
  const std::size_t block_size = large
                                     ? ((requested_bytes + 7) & ~std::size_t{7})
                                     : ClassBytes(class_index);
  if (block_size > std::numeric_limits<std::size_t>::max() - kPageHeaderBytes) {
    ReleasePageId(page_id);
    return nullptr;
  }
  const std::size_t required = kPageHeaderBytes + block_size;
  const std::size_t allocation_bytes =
      large ? ((required + kPageBytes - 1) & ~(kPageBytes - 1)) : 0;
  void* storage = nullptr;
  SmallSpan* small_span = nullptr;
  unsigned span_slot = 0;
  if (large) {
    storage = TryAllocateRetainedBytes(allocation_domain_, allocation_bytes,
                                       kPageBytes);
    if (storage == nullptr) {
      ReleasePageId(page_id);
      return nullptr;
    }
  } else {
    small_span = available_spans_head_ == nullptr ? AllocateSmallSpan()
                                                  : available_spans_head_;
    if (small_span == nullptr) {
      ReleasePageId(page_id);
      return nullptr;
    }
    span_slot = std::countr_zero(small_span->free_page_mask_);
    assert(span_slot < kPagesPerSpan);
    small_span->free_page_mask_ &=
        static_cast<std::uint16_t>(~(std::uint32_t{1} << span_slot));
    ++small_span->live_pages_;
    if (small_span->free_page_mask_ == 0) RemoveAvailableSpan(small_span);
    storage =
        static_cast<std::byte*>(small_span->storage_) + span_slot * kPageBytes;
  }
  if (storage == nullptr) {
    ReleasePageId(page_id);
    return nullptr;
  }
  auto* page = new (storage) PageHeader();
  page->page_id_ = page_id;
  page->allocation_bytes_ = static_cast<std::uint32_t>(allocation_bytes);
  page->block_size_ = static_cast<std::uint32_t>(block_size);
  page->class_index_ = class_index;
  if (!large) {
    page->small_span_ = small_span;
    page->span_slot_ = static_cast<std::uint8_t>(span_slot);
  }
  page->capacity_ = large ? 1
                          : static_cast<std::uint16_t>(
                                (kPageBytes - kPageHeaderBytes) / block_size);
  assert(page->capacity_ != 0 && page->capacity_ <= (1U << kSlotBits));
  const std::uint64_t descriptor =
      reinterpret_cast<std::uintptr_t>(page) |
      static_cast<std::uint64_t>(large ? kLargeClassMarker : block_size);
  SetDirectoryDescriptor(page_id, descriptor);
  ++allocated_pages_;
  if (!large) AddAvailable(page);
  return page;
}

inline ScanHashMapEntryArena::PageHeader* ScanHashMapEntryArena::PageFor(
    Handle handle) const noexcept {
  const std::uint32_t page_id = handle >> kSlotBits;
  if (page_id == 0 || page_id >= next_page_id_) return nullptr;
  const std::uint64_t descriptor = DirectoryDescriptor(page_id);
  const std::uint64_t marker = descriptor & std::uint64_t{0xffff};
  if (descriptor == 0 || marker == kFreePageMarker) return nullptr;
  return reinterpret_cast<PageHeader*>(descriptor & ~std::uint64_t{0xffff});
}

inline bool ScanHashMapEntryArena::CanAllocate(
    std::size_t bytes) const noexcept {
  const std::uint8_t class_index = ClassFor(bytes);
  if (class_index != kLargeClassMarker &&
      available_page_heads_[class_index] != 0) {
    return true;
  }
  return free_page_id_head_ != 0 || next_page_id_ <= maximum_page_id_;
}

inline std::size_t ScanHashMapEntryArena::AllocationBytesIfNewPage(
    std::size_t bytes) const noexcept {
  const std::uint8_t class_index = ClassFor(bytes);
  if (class_index != kLargeClassMarker &&
      available_page_heads_[class_index] != 0) {
    return 0;
  }
  const std::size_t directory_bytes = DirectoryGrowthBytesForNextPage();
  auto add = [](std::size_t left, std::size_t right) noexcept {
    return right > std::numeric_limits<std::size_t>::max() - left
               ? std::numeric_limits<std::size_t>::max()
               : left + right;
  };
  if (class_index != kLargeClassMarker) {
    // Existing spans and intrusive availability lists need no heap growth. Any
    // retained directory-array growth is charged exactly; a new span retains
    // the alignment allowance because mimalloc may round the 64 KiB-aligned
    // mapping upward.
    return available_spans_head_ == nullptr
               ? add(kSmallSpanAdmissionBytes, directory_bytes)
               : directory_bytes;
  }
  const std::size_t block_size =
      bytes > std::numeric_limits<std::size_t>::max() - 7
          ? std::numeric_limits<std::size_t>::max()
          : (bytes + 7) & ~std::size_t{7};
  if (block_size > std::numeric_limits<std::size_t>::max() - kPageHeaderBytes) {
    return std::numeric_limits<std::size_t>::max();
  }
  const std::size_t required = kPageHeaderBytes + block_size;
  if (required > std::numeric_limits<std::size_t>::max() - (kPageBytes - 1)) {
    return std::numeric_limits<std::size_t>::max();
  }
  const std::size_t allocation_bytes =
      (required + kPageBytes - 1) & ~(kPageBytes - 1);
  return add(add(allocation_bytes, kPageBytes), directory_bytes);
}

inline ScanHashMapEntryArena::Allocation ScanHashMapEntryArena::Allocate(
    std::size_t bytes) {
  const std::uint8_t class_index = ClassFor(bytes);
  PageHeader* page = nullptr;
  if (class_index != kLargeClassMarker &&
      available_page_heads_[class_index] != 0) {
    page = PageFor(available_page_heads_[class_index] << kSlotBits);
    assert(page != nullptr);
  } else {
    page = AllocatePage(class_index, bytes);
  }
  if (page == nullptr) return {};

  std::uint16_t slot = 0;
  if (page->free_head_ != kNoSlot) {
    slot = page->free_head_;
    std::memcpy(&page->free_head_, SlotAddress(page, slot), sizeof(slot));
  } else {
    assert(page->next_unused_ < page->capacity_);
    slot = page->next_unused_++;
  }
  ++page->live_count_;
  if (page->live_count_ == page->capacity_ && page->listed_available_) {
    RemoveAvailable(page);
  }
  const Handle handle = (page->page_id_ << kSlotBits) | slot;
  assert(handle != 0);
  return {.handle_ = handle, .pointer_ = SlotAddress(page, slot)};
}

inline void ScanHashMapEntryArena::ReleasePage(PageHeader* page) noexcept {
  assert(page != nullptr && page->live_count_ == 0);
  RemoveAvailable(page);
  const std::uint32_t page_id = page->page_id_;
  SmallSpan* span = page->small_span_;
  const std::uint8_t span_slot = page->span_slot_;
  SetDirectoryDescriptor(page_id, 0);
  page->~PageHeader();
  if (span == nullptr) {
    DeallocateRetainedBytes(allocation_domain_, page, kPageBytes);
  } else {
    assert(span_slot < kPagesPerSpan && span->live_pages_ != 0);
    const std::uint16_t page_bit =
        static_cast<std::uint16_t>(std::uint32_t{1} << span_slot);
    assert((span->free_page_mask_ & page_bit) == 0);
    const bool was_full = span->free_page_mask_ == 0;
    span->free_page_mask_ |= page_bit;
    --span->live_pages_;
    if (span->live_pages_ == 0) {
      ReleaseSmallSpan(span);
    } else if (was_full) {
      AddAvailableSpan(span);
    }
  }
  ReleasePageId(page_id);
  assert(allocated_pages_ != 0);
  --allocated_pages_;
}

inline void ScanHashMapEntryArena::Deallocate(Handle handle) noexcept {
  PageHeader* page = PageFor(handle);
  assert(page != nullptr);
  const std::uint16_t slot = static_cast<std::uint16_t>(handle & kSlotMask);
  assert(slot < page->capacity_ && page->live_count_ != 0);
  const bool was_full = page->live_count_ == page->capacity_;
  std::memcpy(SlotAddress(page, slot), &page->free_head_, sizeof(slot));
  page->free_head_ = slot;
  --page->live_count_;
  if (was_full && page->class_index_ != kLargeClassMarker) {
    AddAvailable(page);
  }
  if (page->live_count_ == 0) ReleasePage(page);
}

inline void* ScanHashMapEntryArena::Resolve(Handle handle) const noexcept {
  const std::uint32_t page_id = handle >> kSlotBits;
  assert(page_id != 0 && page_id < next_page_id_);
  const std::uint64_t descriptor = DirectoryDescriptor(page_id);
  const std::uint64_t marker = descriptor & std::uint64_t{0xffff};
  assert(descriptor != 0 && marker != kFreePageMarker);

  const auto* page =
      reinterpret_cast<const PageHeader*>(descriptor & ~std::uint64_t{0xffff});
  const std::uint16_t slot = static_cast<std::uint16_t>(handle & kSlotMask);
  if (marker == kLargeClassMarker) {
    assert(slot < page->capacity_);
    return const_cast<std::byte*>(SlotAddress(page, slot));
  }

  // The descriptor publishes immutable layout together with the page
  // address. A live bucket handle is already known to select an allocated
  // slot, so release lookup needs neither a size-class table load nor a
  // capacity division before it can address the entry.
  const std::size_t block_size = marker;
  assert(block_size >= kClassSizes.front() &&
         block_size <= kClassSizes.back() && block_size % 8 == 0);
  assert(slot < (kPageBytes - kPageHeaderBytes) / block_size);
  return const_cast<std::byte*>(reinterpret_cast<const std::byte*>(page) +
                                kPageHeaderBytes + slot * block_size);
}

inline ScanHashMapEntryArena::Handle ScanHashMapEntryArena::HandleOf(
    const void* pointer) const noexcept {
  if (pointer == nullptr) return 0;
  const auto address = reinterpret_cast<std::uintptr_t>(pointer);
  const auto page_address = address & ~(kPageBytes - 1);
  const auto* page = reinterpret_cast<const PageHeader*>(page_address);
  const std::uintptr_t data = page_address + kPageHeaderBytes;
  if (address < data || page->page_id_ == 0 || page->block_size_ == 0 ||
      (address - data) % page->block_size_ != 0) {
    return 0;
  }
  if (page->page_id_ >= next_page_id_ ||
      (DirectoryDescriptor(page->page_id_) & ~std::uint64_t{0xffff}) !=
          page_address) {
    return 0;
  }
  const std::size_t slot = (address - data) / page->block_size_;
  if (slot >= page->capacity_) return 0;
  return (page->page_id_ << kSlotBits) | static_cast<Handle>(slot);
}

// The default policy stores every value directly in Entry. Specialized maps
// may split a value into a common hot-path prefix and an optional derived
// payload while retaining the same hash-table implementation.
template <typename Value>
struct ScanHashMapInlineEntryPolicy {
  using StoredValue = Value;
  struct Extra {};

  static constexpr bool HasExtraValue(const Value&) noexcept { return false; }
  static constexpr bool HasExtraStored(const StoredValue&) noexcept {
    return false;
  }
  static StoredValue Store(const Value& value) { return value; }
  static Extra StoreExtra(const Value&) noexcept { return {}; }
  static Value Load(const StoredValue& value, const Extra*) { return value; }
  static void Assign(StoredValue* stored, Extra*, const Value& value) {
    *stored = value;
  }
};

// Cached hashes retain the 32 address bits needed by the largest direct bucket
// table. MaxBucketExponent is configurable so tests can exercise saturation
// without allocating 2^32 buckets; production uses the full uint32_t range.
template <typename Value,
          unsigned MaxBucketExponent =
              std::numeric_limits<std::uint32_t>::digits,
          typename EntryPolicy = ScanHashMapInlineEntryPolicy<Value>>
class ScanHashMap {
 public:
  using EntryHandle = ScanHashMapEntryArena::Handle;
  // Digest arguments for inline keys must equal ComputeDigest(key). Entries do
  // not retain a bucket hash, so rehash and pointer-only mutation reconstruct
  // it from the live key. External entries retain the supplied digest tail.
  static_assert(MaxBucketExponent <=
                std::numeric_limits<std::uint32_t>::digits);
  static_assert(MaxBucketExponent < std::numeric_limits<std::size_t>::digits);

  using StoredValue = typename EntryPolicy::StoredValue;
  using EntryExtra = typename EntryPolicy::Extra;

  struct ExtendedEntry;

  struct Entry {
    static constexpr std::uint32_t kMaxLogicalKeySize =
        (std::uint32_t{1} << 31) - 1;

    // Key length and inline/external representation live in a varint directly
    // before the key tail. Keeping the aligned value first lets common entries
    // use exactly the value's footprint instead of reserving another aligned
    // word for fixed-width key metadata and a cached bucket hash.
    StoredValue value_{};

    bool has_extra() const noexcept {
      return EntryPolicy::HasExtraStored(value_);
    }

    EntryExtra* optional_extra() noexcept {
      return has_extra() ? extra() : nullptr;
    }
    const EntryExtra* optional_extra() const noexcept {
      return has_extra() ? extra() : nullptr;
    }

    Value value() const { return EntryPolicy::Load(value_, optional_extra()); }

    bool can_assign(const Value& value) const noexcept {
      return has_extra() == EntryPolicy::HasExtraValue(value);
    }

    void assign(const Value& value) {
      assert(can_assign(value));
      EntryPolicy::Assign(&value_, optional_extra(), value);
    }

    bool key_complete() const noexcept {
      return DecodeKeyMetadata(tail()).key_complete_;
    }

    std::uint32_t logical_key_size() const noexcept {
      return DecodeKeyMetadata(tail()).logical_size_;
    }

    // Returns bytes occupied by the tail key-metadata varint. Normal callers
    // should use key(), key_complete(), and logical_key_size().
    std::uint8_t key_metadata_bytes() const noexcept {
      return DecodeKeyMetadata(tail()).encoded_bytes_;
    }

    // Returns the varint width without constructing an Entry. The external
    // representation bit shares the same varint with the logical length.
    static std::uint8_t KeyMetadataBytesFor(std::uint32_t logical_size,
                                            bool key_complete) noexcept {
      assert(logical_size <= kMaxLogicalKeySize);
      return EncodedKeyMetadataBytes((logical_size << 1) |
                                     static_cast<std::uint32_t>(!key_complete));
    }

    std::string_view key() const noexcept {
      const KeyMetadata metadata = DecodeKeyMetadata(tail());
      return metadata.key_complete_
                 ? std::string_view(reinterpret_cast<const char*>(
                                        tail() + metadata.encoded_bytes_),
                                    metadata.logical_size_)
                 : std::string_view{};
    }

    Digest external_key_digest() const noexcept {
      Digest digest;
      const KeyMetadata metadata = DecodeKeyMetadata(tail());
      if (!metadata.key_complete_) {
        std::memcpy(&digest, tail() + metadata.encoded_bytes_, sizeof(digest));
      }
      return digest;
    }

    bool MatchesKey(const Digest& digest, std::string_view key) const noexcept {
      // A lookup used to decode this varint once for key_complete() and again
      // for key(). At billion-key scale the random entry load is already a
      // cache miss; decode its metadata only once after that load arrives.
      const KeyMetadata metadata = DecodeKeyMetadata(tail());
      if (metadata.logical_size_ != key.size()) return false;
      const std::byte* payload = tail() + metadata.encoded_bytes_;
      if (metadata.key_complete_) {
        // Decimal benchmark and application identifiers commonly occupy
        // 8--16 bytes. Two overlapping 8-byte comparisons cover that whole
        // range without reading beyond either object and avoid an out-of-line
        // memcmp in the random-lookup hot path. Other lengths retain libc's
        // tuned implementation.
        if (metadata.logical_size_ >= sizeof(std::uint64_t) &&
            metadata.logical_size_ <= 2 * sizeof(std::uint64_t)) {
          std::uint64_t stored_head;
          std::uint64_t sought_head;
          std::uint64_t stored_tail;
          std::uint64_t sought_tail;
          std::memcpy(&stored_head, payload, sizeof(stored_head));
          std::memcpy(&sought_head, key.data(), sizeof(sought_head));
          const std::size_t tail_offset =
              metadata.logical_size_ - sizeof(std::uint64_t);
          std::memcpy(&stored_tail, payload + tail_offset, sizeof(stored_tail));
          std::memcpy(&sought_tail, key.data() + tail_offset,
                      sizeof(sought_tail));
          return stored_head == sought_head && stored_tail == sought_tail;
        }
        return std::memcmp(payload, key.data(), metadata.logical_size_) == 0;
      }
      Digest stored;
      std::memcpy(&stored, payload, sizeof(stored));
      return stored == digest;
    }

    struct Allocation {
      Entry* entry_ = nullptr;
      EntryHandle handle_ = 0;
    };

    static Allocation Create(ScanHashMapEntryArena& arena, const Digest& digest,
                             std::string_view key, const Value& value,
                             bool key_complete = true);
    static Allocation CreateReplacement(ScanHashMapEntryArena& arena,
                                        const Entry& source,
                                        const Value& value);

   private:
    friend struct ExtendedEntry;

    struct KeyMetadata {
      std::uint32_t logical_size_ = 0;
      std::uint8_t encoded_bytes_ = 0;
      bool key_complete_ = true;
    };

    explicit Entry(const StoredValue& value) : value_(value) {}

    static std::uint8_t EncodedKeyMetadataBytes(std::uint32_t encoded) noexcept;
    static std::uint8_t EncodeKeyMetadata(std::byte* output,
                                          std::uint32_t logical_size,
                                          bool key_complete) noexcept;
    static KeyMetadata DecodeKeyMetadata(const std::byte* input) noexcept;
    std::size_t tail_bytes() const noexcept;

    EntryExtra* extra() noexcept;
    const EntryExtra* extra() const noexcept;
    std::byte* tail() noexcept;
    const std::byte* tail() const noexcept;
  };

  struct ExtendedEntry final : Entry {
    EntryExtra extra_{};

   private:
    friend struct Entry;

    ExtendedEntry(const StoredValue& value, const EntryExtra& extra)
        : Entry(value), extra_(extra) {}
  };

  struct InsertResult {
    Entry* entry_ = nullptr;
    bool inserted_ = false;
    bool rejected_ = false;
  };

  // Resumable traversal for callers that can keep the map stable between
  // batches. Unlike Scan's stateless cursor, this cursor remembers an exact
  // entry position, so pausing never repeats a bucket chain. A cursor belongs
  // to one map and becomes invalid after any insertion, erase, or rehash.
  class StableScanCursor {
   public:
    bool finished() const noexcept { return table_ == 2; }

   private:
    friend class ScanHashMap;

    std::uint8_t table_ = 0;
    std::size_t bucket_ = 0;
    std::size_t chain_ = 0;
    std::size_t slot_ = 0;
  };

  ScanHashMap() = default;
  explicit ScanHashMap(std::shared_ptr<ScanHashMapEntryArena> arena)
      : arena_(std::move(arena)) {}
  ScanHashMap(const ScanHashMap&) = delete;
  ScanHashMap& operator=(const ScanHashMap&) = delete;

  ScanHashMap(ScanHashMap&& other) noexcept { MoveFrom(std::move(other)); }

  ScanHashMap& operator=(ScanHashMap&& other) noexcept {
    if (this != &other) {
      Clear();
      MoveFrom(std::move(other));
    }
    return *this;
  }

  ~ScanHashMap() { Clear(); }

  // Binds an empty map to a worker-shared arena. Sharing keeps slab occupancy
  // independent of the 16,384 partition and 16 database boundaries. Detached
  // maps retain shared ownership so asynchronous FLUSHDB reclamation cannot
  // outlive the allocation domain.
  void SetEntryArena(std::shared_ptr<ScanHashMapEntryArena> arena) {
    assert(arena != nullptr && empty() && !has_allocated_storage());
    arena_ = std::move(arena);
  }

  // After the caller validates key length, a false result is the explicit
  // 32-bit page-ID capacity boundary. Callers that publish other state before
  // insertion must check this while mutation remains owner-serialized and
  // report ResourceExhausted instead of allowing the handle namespace to wrap.
  bool CanAllocateEntry(std::string_view key, bool key_complete,
                        bool has_extra) const noexcept {
    if (key.size() > Entry::kMaxLogicalKeySize) return false;
    const std::size_t bytes = EntryAllocationBytes(
        static_cast<std::uint32_t>(key.size()), key_complete, has_extra);
    return arena_ == nullptr || arena_->CanAllocate(bytes);
  }

  // Conservative physical allocation required before an entry replacement or
  // insertion can be published. A zero result means the existing arena page
  // and bucket storage suffice. Callers hold the returned process-memory
  // reservation across the non-suspending mutation, closing the admission to
  // allocator-accounting race without adding work to ordinary page reuse.
  std::size_t RequiredAllocationBytes(const Digest& digest,
                                      std::string_view key, bool key_complete,
                                      bool has_extra,
                                      bool inserting) const noexcept {
    auto add = [](std::size_t left, std::size_t right) noexcept {
      return right > std::numeric_limits<std::size_t>::max() - left
                 ? std::numeric_limits<std::size_t>::max()
                 : left + right;
    };

    const std::size_t entry_bytes = EntryAllocationBytes(
        static_cast<std::uint32_t>(key.size()), key_complete, has_extra);
    std::size_t required = arena_ == nullptr
                               ? ScanHashMapEntryArena::kPageBytes
                               : arena_->AllocationBytesIfNewPage(entry_bytes);
    if (required != 0) {
      required = add(required, kSlowPathBookkeepingAllowance);
    }
    if (!inserting) return required;

    if (!tables_[0].buckets_) {
      return add(required, sizeof(Bucket) + kSlowPathBookkeepingAllowance);
    }
    if (!Rehashing() &&
        tables_[0].used_ + 1 >
            BucketCount(tables_[0]) * kTargetEntriesPerBucket &&
        tables_[0].exponent_ < MaxBucketExponent) {
      const std::size_t next_buckets = std::size_t{1}
                                       << (tables_[0].exponent_ + 1);
      if (next_buckets >
          std::numeric_limits<std::size_t>::max() / sizeof(Bucket)) {
        return std::numeric_limits<std::size_t>::max();
      }
      return add(required, add(next_buckets * sizeof(Bucket),
                               kSlowPathBookkeepingAllowance));
    }

    const Table& target = Rehashing() ? tables_[1] : tables_[0];
    const std::uint64_t hash = Hash(digest);
    const Bucket* bucket = &target.buckets_[hash & BucketMask(target)];
    while (bucket != nullptr) {
      if (std::any_of(bucket->entries_.begin(), bucket->entries_.end(),
                      [](EntryHandle handle) { return handle == 0; })) {
        return required;
      }
      bucket = Chained(*bucket) ? Child(target, bucket) : nullptr;
    }
    return add(required, OverflowChildAllocationBytes(target, 1));
  }

  // Releases an old representation returned by ReplaceValue after its
  // external pointer-keyed state has been migrated.
  void DestroyDetached(Entry* entry) noexcept {
    if (entry == nullptr) return;
    assert(arena_ != nullptr);
    const EntryHandle handle = arena_->HandleOf(entry);
    assert(handle != 0);
    DestroyEntry(entry, handle);
  }

  std::size_t size() const noexcept {
    return tables_[0].used_ + tables_[1].used_;
  }

  bool empty() const noexcept { return size() == 0; }

  bool has_allocated_storage() const noexcept {
    return tables_[0].buckets_ || tables_[1].buckets_;
  }

  // Allocates the final direct-bucket table for an empty map whose population
  // is known exactly, such as an offline recovery checkpoint. Normal inserts
  // deliberately grow incrementally because their eventual population is not
  // known; recovery can avoid repeatedly allocating, walking, and retaining
  // both sides of those intermediate rehashes. The normal 75-percent target
  // remains unchanged, so the settled representation and lookup behavior are
  // identical to a map that reached the same size through ordinary growth.
  // Returns false without modifying the map if the requested population
  // cannot be represented or admitted. A physical allocation failure is
  // deliberately not converted into this validation result and remains fatal to
  // the process.
  bool PreallocateForExpectedSize(std::size_t expected_entries) {
    assert(empty() && !has_allocated_storage());
    if (expected_entries == 0) return true;

    const std::size_t required_buckets =
        expected_entries / kTargetEntriesPerBucket +
        (expected_entries % kTargetEntriesPerBucket != 0);
    const std::size_t exponent =
        required_buckets <= 1 ? 0 : std::bit_width(required_buckets - 1);
    if (exponent > MaxBucketExponent ||
        exponent >= std::numeric_limits<std::size_t>::digits) {
      return false;
    }

    ScanHashMapEntryArena& arena = EnsureArena();
    Table prepared(arena.allocation_domain());
    prepared.exponent_ = static_cast<std::uint8_t>(exponent);
    if (!prepared.AllocateBuckets(std::size_t{1} << exponent)) return false;
    tables_[0] = std::move(prepared);
    return true;
  }

  // Includes both tables during incremental resizing. Primarily useful for
  // capacity diagnostics and saturation tests.
  std::size_t allocated_bucket_count() const noexcept {
    return BucketCount(tables_[0]) + BucketCount(tables_[1]);
  }
  // Exposes incremental-rehash state for capacity diagnostics and tests.
  bool rehashing() const noexcept { return Rehashing(); }

  // Returns the low bucket-address bits callers must retain with an Entry
  // address across suspension. It is computed while the Entry is known live;
  // FindAddress can then validate membership without dereferencing stale
  // storage.
  static std::uint32_t AddressHash(const Digest& digest) noexcept {
    return static_cast<std::uint32_t>(Hash(digest));
  }
  static std::uint32_t AddressHash(const Entry& entry) noexcept {
    return static_cast<std::uint32_t>(EntryHash(entry));
  }

  Entry* Find(const Digest& digest, std::string_view key) {
    AdvanceRehashIfNeeded();
    return FindWithoutStep(digest, key);
  }

  const Entry* Find(const Digest& digest, std::string_view key) const {
    return FindWithoutStep(digest, key);
  }

  std::vector<Entry*> FindCandidates(const Digest& digest,
                                     std::string_view key) {
    AdvanceRehashIfNeeded();
    std::vector<Entry*> result;
    const std::uint64_t hash = Hash(digest);
    AppendCandidates(tables_[0], digest, key, hash, &result);
    if (Rehashing()) {
      AppendCandidates(tables_[1], digest, key, hash, &result);
    }
    return result;
  }

  // Returns the first key candidate accepted by a synchronous predicate, in
  // the same old-table/new-table order as FindCandidates. External keys still
  // require the caller's identity check: digest/length equality is not exact
  // key equality. No candidate container is allocated, though the initial
  // rehash step retains its ordinary maintenance/allocation behavior.
  // The predicate receives a const Entry and must not suspend or structurally
  // mutate this map. Async verification must snapshot candidates instead;
  // returned pointers have the same owner/lifetime constraints as Find.
  template <typename Predicate>
  Entry* FindCandidateIf(const Digest& digest, std::string_view key,
                         Predicate&& accept) {
    AdvanceRehashIfNeeded();
    const std::uint64_t hash = Hash(digest);
    if (Entry* found =
            FindCandidateInTable(tables_[0], digest, key, hash, accept)) {
      return found;
    }
    return Rehashing()
               ? FindCandidateInTable(tables_[1], digest, key, hash, accept)
               : nullptr;
  }

  bool Contains(const Entry* entry, std::uint32_t hash) const noexcept {
    return FindAddress(reinterpret_cast<std::uintptr_t>(entry), hash) !=
           nullptr;
  }

  Entry* FindAddress(std::uintptr_t address, std::uint32_t hash) noexcept {
    return const_cast<Entry*>(std::as_const(*this).FindAddress(address, hash));
  }

  // Resolves an address cached by an asynchronous owner without interpreting
  // it as an Entry first. The object may have been erased while that owner was
  // suspended; comparing integer addresses with live bucket slots makes the
  // membership check safe before any object lifetime is assumed. Callers must
  // use the returned live pointer rather than reconstructing one from address.
  const Entry* FindAddress(std::uintptr_t address,
                           std::uint32_t hash) const noexcept {
    if (address == 0) {
      return nullptr;
    }
    const int tables = Rehashing() ? 2 : 1;
    for (int t = 0; t < tables; ++t) {
      const Table& table = tables_[t];
      if (!table.buckets_) {
        continue;
      }
      const Bucket* bucket = &table.buckets_[hash & BucketMask(table)];
      while (bucket != nullptr) {
        for (std::size_t slot = 0; slot < kEntriesPerBucket; ++slot) {
          if (Occupied(*bucket, slot)) {
            const Entry* entry = Resolve(bucket->entries_[slot]);
            if (reinterpret_cast<std::uintptr_t>(entry) == address) {
              return entry;
            }
          }
        }
        bucket = Chained(*bucket) ? Child(table, bucket) : nullptr;
      }
    }
    return nullptr;
  }

  // Updates an entry without changing its address when the policy-selected
  // concrete type stays the same. If the representation changes, swaps a new
  // object into the existing bucket slot and returns the detached old object
  // through `replaced`. Before destroying it, the caller must migrate caches
  // that assume a live object; asynchronous address-only caches can instead
  // use FindAddress to validate membership before dereferencing.
  Entry* ReplaceValue(Entry* existing, const Value& value, const Digest& digest,
                      Entry** replaced) {
    assert(existing != nullptr);
    assert(replaced != nullptr);
    *replaced = nullptr;
    if (existing->can_assign(value)) {
      existing->assign(value);
      return existing;
    }

    typename Entry::Allocation replacement =
        Entry::CreateReplacement(EnsureArena(), *existing, value);
    if (replacement.entry_ == nullptr) return nullptr;
    const std::uint64_t hash = Hash(digest);
    const int tables = Rehashing() ? 2 : 1;
    for (int t = 0; t < tables; ++t) {
      Table& table = tables_[t];
      if (!table.buckets_) {
        continue;
      }
      Bucket* bucket = &table.buckets_[hash & BucketMask(table)];
      while (bucket != nullptr) {
        for (std::size_t slot = 0; slot < kEntriesPerBucket; ++slot) {
          if (Occupied(*bucket, slot) &&
              Resolve(bucket->entries_[slot]) == existing) {
            bucket->entries_[slot] = replacement.handle_;
            *replaced = existing;
            return replacement.entry_;
          }
        }
        bucket = Chained(*bucket) ? Child(table, bucket) : nullptr;
      }
    }
    DestroyEntry(replacement.entry_, replacement.handle_);
    assert(false && "replacement target must belong to this map");
    return nullptr;
  }

  InsertResult InsertOrAssign(const Digest& digest, std::string_view key,
                              const Value& value, bool key_complete = true) {
    if (Entry* existing = Find(digest, key); existing != nullptr) {
      // The inline policy never changes representation. Specialized policies
      // use ReplaceValue when a new value changes the concrete entry type.
      assert(existing->can_assign(value));
      existing->assign(value);
      return {existing, false, false};
    }

    EnsureArena();
    EnsureTable();
    MaybeStartExpansion();
    typename Entry::Allocation allocation =
        Entry::Create(EnsureArena(), digest, key, value, key_complete);
    if (allocation.entry_ == nullptr) return {nullptr, false, true};
    try {
      if (!AddToTable(Rehashing() ? tables_[1] : tables_[0], allocation.handle_,
                      Hash(digest))) {
        DestroyEntry(allocation.entry_, allocation.handle_);
        return {nullptr, false, true};
      }
    } catch (...) {
      DestroyEntry(allocation.entry_, allocation.handle_);
      throw;
    }
    return {allocation.entry_, true, false};
  }

  Entry* InsertNew(const Digest& digest, std::string_view key,
                   const Value& value, bool key_complete = true) {
    EnsureArena();
    EnsureTable();
    MaybeStartExpansion();
    typename Entry::Allocation allocation =
        Entry::Create(EnsureArena(), digest, key, value, key_complete);
    if (allocation.entry_ == nullptr) return nullptr;
    try {
      if (!AddToTable(Rehashing() ? tables_[1] : tables_[0], allocation.handle_,
                      Hash(digest))) {
        DestroyEntry(allocation.entry_, allocation.handle_);
        return nullptr;
      }
    } catch (...) {
      DestroyEntry(allocation.entry_, allocation.handle_);
      throw;
    }
    return allocation.entry_;
  }

  // Deletes the matching entry, compacting its bucket chain the way Valkey's
  // hashtablePop does so chains stay dense and emptied child buckets are
  // freed. Compaction moves entries only within their own chain, and Scan
  // emits a whole chain per cursor position, so an owner-serialized cursor
  // scan does not miss an entry that exists throughout while erases occur
  // between Scan calls. Scan and mutation are not thread-safe concurrently.
  // Actual removal enables best-effort shrinking below 25% occupancy; a
  // value replacement (including a tombstone) does not. Maintain() finishes
  // reclamation when foreground traffic stops. Entry addresses stay stable.
  bool Erase(const Digest& digest, std::string_view key) {
    AdvanceRehashIfNeeded();
    const std::uint64_t hash = Hash(digest);
    const std::uint8_t tag = HashTag(hash);
    const int tables = Rehashing() ? 2 : 1;
    for (int t = 0; t < tables; ++t) {
      Table& table = tables_[t];
      if (!table.buckets_) {
        continue;
      }
      Bucket* top = &table.buckets_[hash & BucketMask(table)];
      for (Bucket* bucket = top; bucket != nullptr;
           bucket = Chained(*bucket) ? Child(table, bucket) : nullptr) {
        for (std::size_t slot = 0; slot < kEntriesPerBucket; ++slot) {
          if (Occupied(*bucket, slot) && bucket->hashes_[slot] == tag) {
            Entry* entry = Resolve(bucket->entries_[slot]);
            if (!KeyEquals(*entry, digest, key)) continue;
            const EntryHandle handle = bucket->entries_[slot];
            ClearOccupied(bucket, slot);
            DestroyEntry(entry, handle);
            --table.used_;
            FillBucketHole(&table, top, bucket, slot);
            shrink_pending_ = true;
            MaybeStartShrink();
            return true;
          }
        }
      }
    }
    return false;
  }

  // Same reclamation and cursor guarantees as the key-based overload. The
  // supplied address must still belong to this map.
  bool Erase(Entry* entry) {
    if (entry == nullptr) {
      return false;
    }
    AdvanceRehashIfNeeded();
    const std::uint64_t hash = EntryHash(*entry);
    const int tables = Rehashing() ? 2 : 1;
    for (int t = 0; t < tables; ++t) {
      Table& table = tables_[t];
      if (!table.buckets_) {
        continue;
      }
      Bucket* top = &table.buckets_[hash & BucketMask(table)];
      for (Bucket* bucket = top; bucket != nullptr;
           bucket = Chained(*bucket) ? Child(table, bucket) : nullptr) {
        for (std::size_t slot = 0; slot < kEntriesPerBucket; ++slot) {
          if (Occupied(*bucket, slot) &&
              Resolve(bucket->entries_[slot]) == entry) {
            const EntryHandle handle = bucket->entries_[slot];
            ClearOccupied(bucket, slot);
            DestroyEntry(entry, handle);
            --table.used_;
            FillBucketHole(&table, top, bucket, slot);
            shrink_pending_ = true;
            MaybeStartShrink();
            return true;
          }
        }
      }
    }
    return false;
  }

  // Performs at most one source-chain migration, or starts a smaller table.
  // Owners may call this between Scan calls to reclaim buckets without new
  // requests. Returns false when settled or blocked by memory admission; a
  // blocked attempt leaves entries intact and can be retried later. Like a
  // mutation, this invalidates StableScanCursor but preserves Entry addresses.
  bool Maintain() {
    if (Rehashing()) return RehashStep();
    return shrink_pending_ && MaybeStartShrink();
  }

  template <typename Fn>
  void ForEach(Fn&& fn) {
    ForEachTable(tables_[0], fn);
    ForEachTable(tables_[1], fn);
  }

  // Visits entries until the callback returns false. Returns true when both
  // tables were exhausted and false when the callback stopped traversal.
  template <typename Fn>
  bool ForEachWhile(Fn&& fn) {
    return ForEachTableWhile(tables_[0], fn) &&
           ForEachTableWhile(tables_[1], fn);
  }

  // Visits a stable map from cursor's exact position. Returning false from
  // the callback pauses after the current entry; a later call with the same
  // cursor resumes at the following entry. Returns true only after both
  // tables are exhausted. Mutation between calls is unsupported because it
  // can move entries between tables or compact a bucket chain.
  template <typename Fn>
  bool ScanStableWhile(StableScanCursor* cursor, Fn&& fn) const {
    assert(cursor != nullptr);
    while (cursor->table_ < 2) {
      const Table& table = tables_[cursor->table_];
      const std::size_t bucket_count = BucketCount(table);
      if (cursor->bucket_ >= bucket_count) {
        ++cursor->table_;
        cursor->bucket_ = 0;
        cursor->chain_ = 0;
        cursor->slot_ = 0;
        continue;
      }

      const Bucket* bucket = &table.buckets_[cursor->bucket_];
      for (std::size_t chain = 0; chain < cursor->chain_; ++chain) {
        assert(Chained(*bucket));
        bucket = Child(table, bucket);
      }
      while (cursor->slot_ < kEntriesPerBucket) {
        const std::size_t slot = cursor->slot_++;
        if (Occupied(*bucket, slot) &&
            !fn(*const_cast<Entry*>(Resolve(bucket->entries_[slot])))) {
          return false;
        }
      }

      cursor->slot_ = 0;
      if (Chained(*bucket)) {
        ++cursor->chain_;
      } else {
        ++cursor->bucket_;
        cursor->chain_ = 0;
      }
    }
    return true;
  }

  // Samples a short range of buckets and chooses uniformly from the sampled
  // entries, matching the bounded-work shape of Valkey's fair random lookup.
  // The caller may reject unsuitable records and retry with fresh entropy.
  Entry* FairRandomEntry(std::uint64_t entropy) {
    if (empty()) return nullptr;
    constexpr std::size_t kSampleEntries = 15;
    constexpr std::size_t kMaximumScanSteps = 32;
    std::array<Entry*, kSampleEntries> sampled{};
    std::size_t count = 0;
    std::uint64_t cursor = entropy;
    for (std::size_t step = 0;
         step < kMaximumScanSteps && count < sampled.size(); ++step) {
      cursor = Scan(cursor, [&](Entry& entry) {
        if (count < sampled.size()) sampled[count++] = &entry;
      });
      if (cursor == 0) break;
    }
    if (count == 0) return nullptr;
    // SplitMix64 finalizer keeps the sample choice independent from the bits
    // consumed by the scan cursor.
    entropy ^= entropy >> 30;
    entropy *= 0xbf58476d1ce4e5b9ULL;
    entropy ^= entropy >> 27;
    entropy *= 0x94d049bb133111ebULL;
    entropy ^= entropy >> 31;
    return sampled[entropy % count];
  }

  // A cursor of zero starts and completes a full scan. The owner may mutate,
  // rehash, or compact the map between Scan calls, and the callback may then
  // be invoked more than once for an entry, including across shrinking. Every
  // entry present throughout a complete scan is emitted at least once.
  // Mutation from another thread
  // during this call, or from inside the callback, is unsupported.
  template <typename Fn>
  std::uint64_t Scan(std::uint64_t cursor, Fn&& fn) const {
    if (empty()) {
      return 0;
    }

    if (!Rehashing()) {
      const std::uint64_t mask = BucketMask(tables_[0]);
      EmitBucket(tables_[0], cursor & mask, fn);
      return NextCursor(cursor, mask);
    }

    // The source is always table 0, but shrinking makes it the larger table.
    // Visit a complete equivalence class under the smaller mask before moving
    // the reverse-bit cursor; merged buckets can repeat, never disappear.
    const bool shrinking = tables_[0].exponent_ > tables_[1].exponent_;
    const Table& small = tables_[shrinking ? 1 : 0];
    const Table& large = tables_[shrinking ? 0 : 1];
    const std::uint64_t small_mask = BucketMask(small);
    const std::uint64_t large_mask = BucketMask(large);

    const std::uint64_t small_index = cursor & small_mask;
    if (shrinking || small_index >= rehash_index_) {
      EmitBucket(small, small_index, fn);
    }

    do {
      const std::uint64_t large_index = cursor & large_mask;
      if (!shrinking || large_index >= rehash_index_) {
        EmitBucket(large, large_index, fn);
      }
      cursor = NextCursor(cursor, large_mask);
    } while ((cursor & (small_mask ^ large_mask)) != 0);

    return cursor;
  }

  void Clear() noexcept {
    DestroyTable(tables_[0], true);
    DestroyTable(tables_[1], true);
    rehash_index_ = kNotRehashing;
    shrink_pending_ = false;
  }

  // Moves every entry out into the returned map and leaves *this empty and
  // immediately usable. Destroying the returned map is what actually frees the
  // entries, so a caller can hand it to a background task and keep serving
  // reads from *this. O(1) — no entry is touched here.
  //
  // Entry addresses are stable across resizing and ordinary assignment.
  // ReplaceValue deliberately returns the detached old object when a storage
  // policy changes concrete type, making the exceptional invalidation
  // explicit to its caller. A raw-pointer cache must likewise detect detach
  // before dereferencing; this class does not track that, since the useful
  // granularity is whatever set of maps the caller detaches together.
  ScanHashMap Detach() noexcept {
    ScanHashMap detached(arena_);
    detached.tables_ = std::exchange(tables_, {});
    detached.rehash_index_ = std::exchange(rehash_index_, kNotRehashing);
    detached.shrink_pending_ = std::exchange(shrink_pending_, false);
    return detached;
  }

 private:
  static constexpr std::size_t kEntriesPerBucket = 12;
  static constexpr std::size_t kTargetEntriesPerBucket = 9;
  // Hysteresis leaves room for writes while a half-sized target is migrated.
  static constexpr std::size_t kShrinkEntriesPerBucket = 3;
  static constexpr std::size_t kSlowPathBookkeepingAllowance = 4096;
  static constexpr std::size_t kNotRehashing =
      std::numeric_limits<std::size_t>::max();

  struct alignas(64) Bucket {
    std::array<std::uint8_t, kEntriesPerBucket> hashes_{};
    std::array<EntryHandle, kEntriesPerBucket> entries_{};
    // One-based index into Table::overflow_. Keeping the child separate means
    // a chained bucket still carries all twelve entries.
    std::uint32_t child_ = 0;
  };

  static_assert(sizeof(Bucket) == 64);

  using BucketOwner = std::unique_ptr<Bucket, RetainedObjectDeleter<Bucket>>;
  using BucketOwnerVector =
      std::vector<BucketOwner, RetainedAllocator<BucketOwner>>;
  using OverflowFreeIds =
      std::vector<std::uint32_t, RetainedAllocator<std::uint32_t>>;
  struct BucketArrayDeleter {
    RetainedAllocationDomain domain_;
    void operator()(Bucket* buckets) const noexcept {
      static_assert(std::is_trivially_destructible_v<Bucket>);
      DeallocateRetainedBytes(domain_, buckets, alignof(Bucket));
    }
  };
  using DirectBuckets = std::unique_ptr<Bucket[], BucketArrayDeleter>;

  struct OverflowBuckets {
    explicit OverflowBuckets(RetainedAllocationDomain domain)
        : buckets_(RetainedAllocator<BucketOwner>(domain)),
          free_ids_(RetainedAllocator<std::uint32_t>(domain)) {}

    BucketOwnerVector buckets_;
    OverflowFreeIds free_ids_;
  };

  using OverflowOwner =
      std::unique_ptr<OverflowBuckets, RetainedObjectDeleter<OverflowBuckets>>;

  struct Table {
    Table() = default;
    explicit Table(RetainedAllocationDomain domain)
        : buckets_(nullptr, BucketArrayDeleter{domain}),
          overflow_(nullptr, RetainedObjectDeleter<OverflowBuckets>{
                                 RetainedAllocator<OverflowBuckets>(domain)}) {}

    // Direct tables never grow in place. An explicit array allocation gives
    // optional shrinking a rejection channel without changing vector's fatal
    // allocation contract or bypassing admission for later overflow growth.
    bool AllocateBuckets(std::size_t count,
                         bool independent_admission = false) {
      assert(!buckets_);
      auto domain = buckets_.get_deleter().domain_;
      if (independent_admission) domain.externally_admitted_ = false;
      auto* data = static_cast<Bucket*>(TryAllocateRetainedBytes(
          domain, count * sizeof(Bucket), alignof(Bucket)));
      if (data == nullptr) return false;
      std::uninitialized_value_construct_n(data, count);
      buckets_.reset(data);
      return true;
    }

    DirectBuckets buckets_{nullptr, BucketArrayDeleter{}};
    OverflowOwner overflow_;
    std::uint8_t exponent_ = 0;
    std::size_t used_ = 0;
  };

  // A rehash plan exists only within one owner-serialized RehashStep. The
  // common direct-bucket case stays inline; an adversarial overflow chain may
  // allocate temporary plan storage, but failure occurs before either table is
  // mutated and simply leaves this rehash step pending.
  struct RehashMove {
    EntryHandle handle_ = 0;
    Bucket* target_bucket_ = nullptr;
    std::uint8_t target_slot_ = 0;
    std::uint8_t tag_ = 0;
    std::uint8_t split_ = 0;
  };
  using RehashPlan = absl::InlinedVector<RehashMove, kEntriesPerBucket>;

  static bool Chained(const Bucket& bucket) noexcept {
    return bucket.child_ != 0;
  }

  static bool Occupied(const Bucket& bucket, std::size_t slot) noexcept {
    return bucket.entries_[slot] != 0;
  }

  static void ClearOccupied(Bucket* bucket, std::size_t slot) noexcept {
    bucket->entries_[slot] = 0;
  }

  static Bucket* Child(Table& table, Bucket* bucket) noexcept {
    assert(Chained(*bucket));
    assert(table.overflow_ != nullptr &&
           bucket->child_ <= table.overflow_->buckets_.size());
    return table.overflow_->buckets_[bucket->child_ - 1].get();
  }

  static const Bucket* Child(const Table& table,
                             const Bucket* bucket) noexcept {
    return Child(const_cast<Table&>(table), const_cast<Bucket*>(bucket));
  }

  struct ChildAllocation {
    Bucket* bucket_ = nullptr;
    std::uint32_t id_ = 0;
  };

  static std::size_t VectorCapacityFor(std::size_t current,
                                       std::size_t required) noexcept {
    if (required <= current) return current;
    const std::size_t doubled =
        current > std::numeric_limits<std::size_t>::max() / 2
            ? std::numeric_limits<std::size_t>::max()
            : std::max<std::size_t>(1, current * 2);
    return std::max(required, doubled);
  }

  static bool AddUsableAllocation(std::size_t requested,
                                  std::size_t* total) noexcept {
    const std::size_t usable = AllocatorUsableSizeForRequest(requested);
    if (usable == std::numeric_limits<std::size_t>::max() ||
        usable > std::numeric_limits<std::size_t>::max() - *total) {
      *total = std::numeric_limits<std::size_t>::max();
      return false;
    }
    *total += usable;
    return true;
  }

  static std::size_t OverflowChildAllocationBytes(const Table& table,
                                                  std::size_t count) noexcept {
    if (count == 0) return 0;
    std::size_t total = 0;
    const OverflowBuckets* pool = table.overflow_.get();
    if (pool == nullptr &&
        !AddUsableAllocation(sizeof(OverflowBuckets), &total)) {
      return total;
    }
    const std::size_t reusable =
        pool == nullptr ? 0 : std::min(count, pool->free_ids_.size());
    const std::size_t appended = count - reusable;
    const std::size_t current_size =
        pool == nullptr ? 0 : pool->buckets_.size();
    if (appended > std::numeric_limits<std::size_t>::max() - current_size) {
      return std::numeric_limits<std::size_t>::max();
    }
    const std::size_t final_size = current_size + appended;
    const std::size_t free_capacity =
        pool == nullptr ? 0 : pool->free_ids_.capacity();
    if (final_size > free_capacity) {
      const std::size_t capacity = VectorCapacityFor(free_capacity, final_size);
      if (capacity >
              std::numeric_limits<std::size_t>::max() / sizeof(std::uint32_t) ||
          !AddUsableAllocation(capacity * sizeof(std::uint32_t), &total)) {
        return std::numeric_limits<std::size_t>::max();
      }
    }
    const std::size_t bucket_capacity =
        pool == nullptr ? 0 : pool->buckets_.capacity();
    if (final_size > bucket_capacity) {
      const std::size_t capacity =
          VectorCapacityFor(bucket_capacity, final_size);
      if (capacity >
              std::numeric_limits<std::size_t>::max() / sizeof(BucketOwner) ||
          !AddUsableAllocation(capacity * sizeof(BucketOwner), &total)) {
        return std::numeric_limits<std::size_t>::max();
      }
    }
    // Aligned bucket allocation may consume one extra alignment unit.
    const std::size_t bucket_bytes =
        AllocatorUsableSizeForRequest(sizeof(Bucket)) + alignof(Bucket);
    if (bucket_bytes == std::numeric_limits<std::size_t>::max() ||
        count >
            (std::numeric_limits<std::size_t>::max() - total) / bucket_bytes) {
      return std::numeric_limits<std::size_t>::max();
    }
    total += count * bucket_bytes;
    return total;
  }

  template <typename T>
  static bool TryReserveOverflowVector(
      std::vector<T, RetainedAllocator<T>>* values, std::size_t capacity,
      RetainedAllocationDomain domain) {
    std::optional<MemoryReservation> reservation;
    if (!domain.externally_admitted_ && !domain.externally_accounted_) {
      if (capacity > std::numeric_limits<std::size_t>::max() / sizeof(T)) {
        return false;
      }
      reservation =
          TryReserveMemory(AllocatorUsableSizeForRequest(capacity * sizeof(T)));
      if (!reservation.has_value()) return false;
    }
    // Admission rejection must leave the source chain readable. vector's
    // allocator has no fallible reserve, so construct already-admitted storage
    // before moving its nonthrowing owners. Later growth uses the table's
    // original domain again, not this vector's one-allocation permit.
    domain.externally_admitted_ = true;
    std::vector<T, RetainedAllocator<T>> prepared{RetainedAllocator<T>(domain)};
    prepared.reserve(capacity);
    for (T& value : *values) prepared.push_back(std::move(value));
    *values = std::move(prepared);
    return true;
  }

  static bool PrepareChildCapacity(Table* table, std::size_t count) {
    if (count == 0) return true;
    const RetainedAllocationDomain domain =
        table->buckets_.get_deleter().domain_;
    if (table->overflow_ == nullptr) {
      table->overflow_ = TryMakeRetainedUnique<OverflowBuckets>(domain, domain);
      if (table->overflow_ == nullptr) return false;
    }
    auto& pool = *table->overflow_;
    const std::size_t reusable = std::min(count, pool.free_ids_.size());
    const std::size_t appended = count - reusable;
    if (appended >
        std::numeric_limits<std::size_t>::max() - pool.buckets_.size()) {
      return false;
    }
    const std::size_t final_size = pool.buckets_.size() + appended;
    if (final_size > std::numeric_limits<std::uint32_t>::max()) {
      return false;
    }
    if (final_size > pool.free_ids_.capacity()) {
      if (!TryReserveOverflowVector(
              &pool.free_ids_,
              VectorCapacityFor(pool.free_ids_.capacity(), final_size),
              domain)) {
        return false;
      }
    }
    if (final_size > pool.buckets_.capacity()) {
      if (!TryReserveOverflowVector(
              &pool.buckets_,
              VectorCapacityFor(pool.buckets_.capacity(), final_size),
              domain)) {
        return false;
      }
    }
    return true;
  }

  static ChildAllocation AllocateChild(Table* table) {
    if (!PrepareChildCapacity(table, 1)) return {};
    auto& pool = *table->overflow_;
    std::uint32_t index = 0;
    if (!pool.free_ids_.empty()) {
      auto bucket =
          TryMakeRetainedUnique<Bucket>(table->buckets_.get_deleter().domain_);
      if (bucket == nullptr) return {};
      index = pool.free_ids_.back();
      pool.free_ids_.pop_back();
      assert(index < pool.buckets_.size() && pool.buckets_[index] == nullptr);
      pool.buckets_[index] = std::move(bucket);
    } else {
      index = static_cast<std::uint32_t>(pool.buckets_.size());
      auto bucket =
          TryMakeRetainedUnique<Bucket>(table->buckets_.get_deleter().domain_);
      if (bucket == nullptr) return {};
      pool.buckets_.push_back(std::move(bucket));
    }
    return {.bucket_ = pool.buckets_[index].get(), .id_ = index + 1};
  }

  static void FreeChild(Table* table, std::uint32_t child_id) noexcept {
    assert(child_id != 0 && table->overflow_ != nullptr);
    const std::uint32_t index = child_id - 1;
    assert(index < table->overflow_->buckets_.size() &&
           table->overflow_->buckets_[index] != nullptr);
    table->overflow_->buckets_[index].reset();
    table->overflow_->free_ids_.push_back(index);
  }

  static std::size_t BucketCount(const Table& table) noexcept {
    return !table.buckets_ ? 0 : std::size_t{1} << table.exponent_;
  }

  static std::uint64_t BucketMask(const Table& table) noexcept {
    return static_cast<std::uint64_t>(BucketCount(table) - 1);
  }

  static std::uint64_t Hash(const Digest& digest) noexcept {
    return digest.value_;
  }

  ScanHashMapEntryArena& EnsureArena() {
    if (arena_ == nullptr) {
      arena_ = std::make_shared<ScanHashMapEntryArena>();
    }
    return *arena_;
  }

  Entry* Resolve(EntryHandle handle) noexcept {
    assert(arena_ != nullptr);
    return static_cast<Entry*>(arena_->Resolve(handle));
  }

  const Entry* Resolve(EntryHandle handle) const noexcept {
    assert(arena_ != nullptr);
    return static_cast<const Entry*>(arena_->Resolve(handle));
  }

  static std::size_t EntryAllocationBytes(std::uint32_t logical_size,
                                          bool key_complete,
                                          bool has_extra) noexcept {
    const std::size_t metadata =
        Entry::KeyMetadataBytesFor(logical_size, key_complete);
    const std::size_t payload = key_complete ? logical_size : sizeof(Digest);
    return (has_extra ? sizeof(ExtendedEntry) : sizeof(Entry)) + metadata +
           payload;
  }

  void DestroyEntry(Entry* entry, EntryHandle handle) noexcept {
    assert(entry != nullptr && handle != 0 && arena_ != nullptr);
    if (entry->has_extra()) {
      static_cast<ExtendedEntry*>(entry)->~ExtendedEntry();
    } else {
      entry->~Entry();
    }
    arena_->Deallocate(handle);
  }

  static std::uint8_t HashTag(std::uint64_t hash) noexcept {
    return static_cast<std::uint8_t>(hash >> 56);
  }

  static std::uint64_t EntryHash(const Entry& entry) noexcept {
    return Hash(entry.key_complete() ? ComputeDigest(entry.key())
                                     : entry.external_key_digest());
  }

  static bool KeyEquals(const Entry& entry, const Digest& digest,
                        std::string_view key) noexcept {
    return entry.MatchesKey(digest, key);
  }

  static std::uint64_t ReverseBits(std::uint64_t value) noexcept {
    value = ((value >> 1) & 0x5555555555555555ULL) |
            ((value & 0x5555555555555555ULL) << 1);
    value = ((value >> 2) & 0x3333333333333333ULL) |
            ((value & 0x3333333333333333ULL) << 2);
    value = ((value >> 4) & 0x0f0f0f0f0f0f0f0fULL) |
            ((value & 0x0f0f0f0f0f0f0f0fULL) << 4);
    return std::byteswap(value);
  }

  // Pieter Noordhuis' stateless scan cursor algorithm, as used by Valkey.
  static std::uint64_t NextCursor(std::uint64_t cursor,
                                  std::uint64_t mask) noexcept {
    cursor |= ~mask;
    cursor = ReverseBits(cursor);
    ++cursor;
    return ReverseBits(cursor);
  }

  bool Rehashing() const noexcept { return rehash_index_ != kNotRehashing; }

  // RehashStep is intentionally large and stays out of line. Keep its common
  // steady-state guard at the call site so every lookup does not pay a
  // call/return merely to discover that the recovered or settled table is not
  // resizing. Reads and mutations advance at most one source bucket; a
  // deferred shrink can also retry admission without another deletion.
  [[gnu::always_inline]] void AdvanceRehashIfNeeded() {
    if (Rehashing()) [[unlikely]] {
      RehashStep();
    } else if (shrink_pending_) [[unlikely]] {
      MaybeStartShrink();
    }
  }

  void EnsureTable() {
    if (!tables_[0].buckets_) {
      assert(arena_ != nullptr);
      tables_[0] = Table(arena_->allocation_domain());
      if (!tables_[0].AllocateBuckets(1)) std::terminate();
      tables_[0].exponent_ = 0;
    }
  }

  void MaybeStartExpansion() {
    if (Rehashing()) {
      return;
    }
    const std::size_t buckets = BucketCount(tables_[0]);
    if (tables_[0].used_ + 1 <= buckets * kTargetEntriesPerBucket) {
      return;
    }
    // Once every cached address bit is in use, retaining the current direct
    // table is safer than overflowing the entry hash. AddToTable continues to
    // accept entries through bucket chains, so saturation is a performance
    // boundary rather than a capacity or correctness failure.
    if (tables_[0].exponent_ >= MaxBucketExponent) {
      return;
    }
    assert(arena_ != nullptr);
    tables_[1] = Table(arena_->allocation_domain());
    tables_[1].exponent_ = tables_[0].exponent_ + 1;
    if (!tables_[1].AllocateBuckets(std::size_t{1} << tables_[1].exponent_)) {
      std::terminate();
    }
    tables_[1].used_ = 0;
    rehash_index_ = 0;
  }

  bool MaybeStartShrink() {
    if (Rehashing()) return false;
    if (empty()) {
      const bool released = has_allocated_storage();
      tables_ = {};
      shrink_pending_ = false;
      return released;
    }
    const std::size_t buckets = BucketCount(tables_[0]);
    if (buckets <= 1 || size() >= buckets * kShrinkEntriesPerBucket) {
      shrink_pending_ = false;
      return false;
    }
    Table prepared(arena_->allocation_domain());
    prepared.exponent_ = tables_[0].exponent_ - 1;
    // Deletion has no enclosing growth permit, even for a primary index.
    // Reserve the temporary old+new footprint before publishing the target;
    // rejection must never turn a successful Erase into a failed command.
    if (!prepared.AllocateBuckets(buckets / 2, true)) return false;
    tables_[1] = std::move(prepared);
    rehash_index_ = 0;
    return true;
  }

  bool RehashStep() {
    if (!Rehashing()) return false;

    // A source chain can split into only two target chains when the direct
    // table doubles, or merge into one target chain when it halves. Prepare
    // every overflow bucket before clearing a source slot, so deterministic
    // admission failure merely delays maintenance and
    // never leaves a partially migrated map. Physical allocator exhaustion is
    // process-fatal. This also covers rehash work initiated by reads, which
    // have no enclosing write reservation.
    RehashPlan plan;
    if (!PrepareRehashPlan(&tables_[0].buckets_[rehash_index_], &tables_[1],
                           &plan)) {
      return false;
    }

    MoveBucket(&tables_[0].buckets_[rehash_index_], &tables_[1], plan);
    ++rehash_index_;
    if (rehash_index_ == BucketCount(tables_[0])) {
      assert(tables_[0].used_ == 0);
      tables_[0] = std::move(tables_[1]);
      tables_[1] = Table(arena_->allocation_domain());
      rehash_index_ = kNotRehashing;
    }
    return true;
  }

  static std::size_t FreeSlots(const Table& table,
                               std::size_t bucket_index) noexcept {
    const Bucket* bucket = &table.buckets_[bucket_index];
    std::size_t free = 0;
    while (bucket != nullptr) {
      free += static_cast<std::size_t>(std::count(
          bucket->entries_.begin(), bucket->entries_.end(), EntryHandle{0}));
      bucket = Chained(*bucket) ? Child(table, bucket) : nullptr;
    }
    return free;
  }

  static std::size_t OverflowBucketsNeeded(const Table& table,
                                           std::size_t bucket_index,
                                           std::size_t entries) noexcept {
    const std::size_t free = FreeSlots(table, bucket_index);
    if (entries <= free) return 0;
    const std::size_t missing = entries - free;
    return (missing + kEntriesPerBucket - 1) / kEntriesPerBucket;
  }

  static bool AppendEmptyBuckets(Table* table, std::size_t bucket_index,
                                 std::size_t count) {
    Bucket* tail = &table->buckets_[bucket_index];
    while (Chained(*tail)) tail = Child(*table, tail);
    while (count-- != 0) {
      const ChildAllocation child = AllocateChild(table);
      if (child.bucket_ == nullptr) return false;
      tail->child_ = child.id_;
      tail = child.bucket_;
    }
    return true;
  }

  static bool AssignRehashDestinations(
      Table* target, const std::array<std::size_t, 2>& target_indexes,
      RehashPlan* plan) noexcept {
    struct Cursor {
      Bucket* bucket_ = nullptr;
      std::size_t slot_ = 0;
    };
    std::array<Cursor, 2> cursors = {
        Cursor{.bucket_ = &target->buckets_[target_indexes[0]]},
        Cursor{.bucket_ = &target->buckets_[target_indexes[1]]}};
    for (RehashMove& move : *plan) {
      Cursor& cursor = cursors[move.split_];
      while (cursor.bucket_ != nullptr) {
        while (cursor.slot_ < kEntriesPerBucket &&
               Occupied(*cursor.bucket_, cursor.slot_)) {
          ++cursor.slot_;
        }
        if (cursor.slot_ < kEntriesPerBucket) break;
        cursor.bucket_ =
            Chained(*cursor.bucket_) ? Child(*target, cursor.bucket_) : nullptr;
        cursor.slot_ = 0;
      }
      if (cursor.bucket_ == nullptr) return false;
      move.target_bucket_ = cursor.bucket_;
      move.target_slot_ = static_cast<std::uint8_t>(cursor.slot_++);
    }
    return true;
  }

  bool PrepareRehashPlan(Bucket* source, Table* target, RehashPlan* plan) {
    const std::size_t old_bucket_count = BucketCount(tables_[0]);
    const bool shrinking = target->exponent_ < tables_[0].exponent_;
    const std::array<std::size_t, 2> target_indexes =
        shrinking
            ? std::array<std::size_t, 2>{rehash_index_ & BucketMask(*target), 0}
            : std::array<std::size_t, 2>{rehash_index_,
                                         rehash_index_ + old_bucket_count};
    std::array<std::size_t, 2> demand{};
    for (Bucket* bucket = source; bucket != nullptr;
         bucket = Chained(*bucket) ? Child(tables_[0], bucket) : nullptr) {
      for (EntryHandle handle : bucket->entries_) {
        if (handle == 0) continue;
        const std::uint64_t hash = EntryHash(*Resolve(handle));
        const std::size_t index =
            static_cast<std::size_t>(hash & BucketMask(*target));
        assert(index == target_indexes[0] || index == target_indexes[1]);
        const std::uint8_t split =
            static_cast<std::uint8_t>(!shrinking && index == target_indexes[1]);
        plan->push_back(RehashMove{
            .handle_ = handle,
            .tag_ = HashTag(hash),
            .split_ = split,
        });
        ++demand[split];
      }
    }

    std::array<std::size_t, 2> needed{};
    for (std::size_t split = 0; split < needed.size(); ++split) {
      needed[split] =
          OverflowBucketsNeeded(*target, target_indexes[split], demand[split]);
    }
    if (needed[0] > std::numeric_limits<std::size_t>::max() - needed[1]) {
      return false;
    }
    const std::size_t total_needed = needed[0] + needed[1];
    const std::size_t allocation_bytes =
        OverflowChildAllocationBytes(*target, total_needed);
    if (allocation_bytes == std::numeric_limits<std::size_t>::max()) {
      return false;
    }

    std::optional<MemoryReservation> reservation;
    const RetainedAllocationDomain domain =
        target->buckets_.get_deleter().domain_;
    if (allocation_bytes != 0 && domain.externally_admitted_ &&
        !domain.externally_accounted_) {
      // Primary-index mutations admit composite growth at the storage layer,
      // but a later read may execute this maintenance step without such an
      // enclosing permit. Reserve the complete split before moving entries.
      reservation = TryReserveMemory(allocation_bytes);
      if (!reservation.has_value()) return false;
    }
    // Each retained allocator performs admission before it changes a vector
    // or publishes a bucket. Partial success leaves only empty capacity, so a
    // later read or write can retry without a partially moved source.
    if (!PrepareChildCapacity(target, total_needed)) {
      return false;
    }
    for (std::size_t split = 0; split < needed.size(); ++split) {
      if (!AppendEmptyBuckets(target, target_indexes[split], needed[split])) {
        // Any buckets attached before failure are empty and valid. A later
        // step counts them as free capacity and retries only the missing
        // allocation.
        return false;
      }
    }
    // All physical growth is complete. Assign exact empty slots now so plan
    // consumption performs only fixed stores and cannot discover a new
    // allocation failure after it starts clearing the source chain.
    return AssignRehashDestinations(target, target_indexes, plan);
  }

  Entry* FindInTable(Table& table, const Digest& digest, std::string_view key,
                     std::uint64_t hash) {
    if (!table.buckets_) {
      return nullptr;
    }
    Bucket* bucket = &table.buckets_[hash & BucketMask(table)];
    const std::uint8_t tag = HashTag(hash);
    while (bucket != nullptr) {
      for (std::size_t slot = 0; slot < kEntriesPerBucket; ++slot) {
        if (Occupied(*bucket, slot) && bucket->hashes_[slot] == tag) {
          Entry* entry = Resolve(bucket->entries_[slot]);
          if (KeyEquals(*entry, digest, key)) return entry;
        }
      }
      bucket = Chained(*bucket) ? Child(table, bucket) : nullptr;
    }
    return nullptr;
  }

  const Entry* FindInTable(const Table& table, const Digest& digest,
                           std::string_view key, std::uint64_t hash) const {
    if (!table.buckets_) return nullptr;
    const Bucket* bucket = &table.buckets_[hash & BucketMask(table)];
    const std::uint8_t tag = HashTag(hash);
    while (bucket != nullptr) {
      for (std::size_t slot = 0; slot < kEntriesPerBucket; ++slot) {
        if (Occupied(*bucket, slot) && bucket->hashes_[slot] == tag) {
          const Entry* entry = Resolve(bucket->entries_[slot]);
          if (KeyEquals(*entry, digest, key)) return entry;
        }
      }
      bucket = Chained(*bucket) ? Child(table, bucket) : nullptr;
    }
    return nullptr;
  }

  void AppendCandidates(Table& table, const Digest& digest,
                        std::string_view key, std::uint64_t hash,
                        std::vector<Entry*>* result) {
    if (!table.buckets_) {
      return;
    }
    Bucket* bucket = &table.buckets_[hash & BucketMask(table)];
    const std::uint8_t tag = HashTag(hash);
    while (bucket != nullptr) {
      for (std::size_t slot = 0; slot < kEntriesPerBucket; ++slot) {
        if (Occupied(*bucket, slot) && bucket->hashes_[slot] == tag) {
          Entry* entry = Resolve(bucket->entries_[slot]);
          if (KeyEquals(*entry, digest, key)) result->push_back(entry);
        }
      }
      bucket = Chained(*bucket) ? Child(table, bucket) : nullptr;
    }
  }

  template <typename Predicate>
  Entry* FindCandidateInTable(Table& table, const Digest& digest,
                              std::string_view key, std::uint64_t hash,
                              Predicate& accept) {
    if (!table.buckets_) return nullptr;
    Bucket* bucket = &table.buckets_[hash & BucketMask(table)];
    const std::uint8_t tag = HashTag(hash);
    while (bucket != nullptr) {
      for (std::size_t slot = 0; slot < kEntriesPerBucket; ++slot) {
        if (Occupied(*bucket, slot) && bucket->hashes_[slot] == tag) {
          Entry* entry = Resolve(bucket->entries_[slot]);
          if (KeyEquals(*entry, digest, key) && accept(std::as_const(*entry)))
            return entry;
        }
      }
      bucket = Chained(*bucket) ? Child(table, bucket) : nullptr;
    }
    return nullptr;
  }

  Entry* FindWithoutStep(const Digest& digest, std::string_view key) {
    const std::uint64_t hash = Hash(digest);
    if (Entry* found = FindInTable(tables_[0], digest, key, hash);
        found != nullptr) {
      return found;
    }
    if (Rehashing()) [[unlikely]] {
      return FindInRehashTable(digest, key, hash);
    }
    return nullptr;
  }

  const Entry* FindWithoutStep(const Digest& digest,
                               std::string_view key) const {
    const std::uint64_t hash = Hash(digest);
    if (const Entry* found = FindInTable(tables_[0], digest, key, hash);
        found != nullptr) {
      return found;
    }
    if (Rehashing()) [[unlikely]] {
      return FindInRehashTable(digest, key, hash);
    }
    return nullptr;
  }

  // Expansion is transient, while Find is the steady-state read hot path.
  // Keeping the second table's full lookup loop out of line avoids duplicating
  // key decoding and comparison code in the common function. The extra call is
  // paid only after the old table misses while an expansion is in progress.
  [[gnu::noinline]] Entry* FindInRehashTable(const Digest& digest,
                                             std::string_view key,
                                             std::uint64_t hash) {
    return FindInTable(tables_[1], digest, key, hash);
  }

  [[gnu::noinline]] const Entry* FindInRehashTable(const Digest& digest,
                                                   std::string_view key,
                                                   std::uint64_t hash) const {
    return FindInTable(tables_[1], digest, key, hash);
  }

  bool AddToTable(Table& table, EntryHandle entry, std::uint64_t hash) {
    const std::uint8_t tag = HashTag(hash);
    Bucket* bucket = &table.buckets_[hash & BucketMask(table)];
    while (true) {
      for (std::size_t slot = 0; slot < kEntriesPerBucket; ++slot) {
        if (!Occupied(*bucket, slot)) {
          bucket->entries_[slot] = entry;
          bucket->hashes_[slot] = tag;
          ++table.used_;
          return true;
        }
      }
      if (Chained(*bucket)) {
        bucket = Child(table, bucket);
        continue;
      }

      const ChildAllocation child = AllocateChild(&table);
      if (child.bucket_ == nullptr) return false;
      bucket->child_ = child.id_;
      bucket = child.bucket_;
    }
  }

  // Moves the last entry of the chain into the freed slot and unlinks the
  // tail bucket once it empties. Only meaningful for chained tops: holes in
  // an unchained bucket are reused by AddToTable's slot scan.
  static void FillBucketHole(Table* table, Bucket* top, Bucket* holed,
                             std::size_t hole_slot) {
    if (!Chained(*top)) {
      return;
    }
    Bucket* parent = nullptr;
    Bucket* tail = top;
    while (Chained(*tail)) {
      parent = tail;
      tail = Child(*table, tail);
    }
    std::size_t last = kEntriesPerBucket;
    for (std::size_t slot = kEntriesPerBucket; slot-- > 0;) {
      if (Occupied(*tail, slot)) {
        last = slot;
        break;
      }
    }
    if (last != kEntriesPerBucket && !(tail == holed && last == hole_slot)) {
      holed->entries_[hole_slot] = tail->entries_[last];
      holed->hashes_[hole_slot] = tail->hashes_[last];
      ClearOccupied(tail, last);
    }
    if (std::none_of(tail->entries_.begin(), tail->entries_.end(),
                     [](EntryHandle handle) { return handle != 0; })) {
      const std::uint32_t child_id = parent->child_;
      parent->child_ = 0;
      FreeChild(table, child_id);
    }
  }

  void MoveBucketEntries(Table* source, Bucket* top, Table* target,
                         const RehashPlan& plan) {
    for (const RehashMove& move : plan) {
      assert(move.target_bucket_ != nullptr &&
             move.target_slot_ < kEntriesPerBucket &&
             !Occupied(*move.target_bucket_, move.target_slot_));
      move.target_bucket_->entries_[move.target_slot_] = move.handle_;
      move.target_bucket_->hashes_[move.target_slot_] = move.tag_;
      ++target->used_;
    }

    // Target capacity and exact slots were fixed before publication. Source
    // cleanup only walks child links to return overflow objects; it no longer
    // revisits entries, resolves inline keys, or recomputes SipHash.
    Bucket* bucket = top;
    std::uint32_t bucket_id = 0;
    while (bucket != nullptr) {
      const bool chained = Chained(*bucket);
      Bucket* next = chained ? Child(*source, bucket) : nullptr;
      const std::uint32_t next_id = bucket->child_;
      bucket->child_ = 0;
      if (bucket_id != 0) FreeChild(source, bucket_id);
      bucket_id = next_id;
      bucket = next;
    }
    top->entries_.fill(0);
    top->hashes_.fill(0);
  }

  void MoveBucket(Bucket* top, Table* target, const RehashPlan& plan) {
    MoveBucketEntries(&tables_[0], top, target, plan);
    const std::size_t moved = plan.size();
    assert(tables_[0].used_ >= moved);
    tables_[0].used_ -= moved;
  }

  template <typename Fn>
  void EmitBucket(const Table& table, std::uint64_t index, Fn& fn) const {
    if (!table.buckets_) {
      return;
    }
    const Bucket* bucket = &table.buckets_[index];
    while (bucket != nullptr) {
      for (std::size_t slot = 0; slot < kEntriesPerBucket; ++slot) {
        if (Occupied(*bucket, slot)) {
          fn(*const_cast<Entry*>(Resolve(bucket->entries_[slot])));
        }
      }
      bucket = Chained(*bucket) ? Child(table, bucket) : nullptr;
    }
  }

  template <typename Fn>
  void ForEachTable(Table& table, Fn& fn) {
    const std::size_t count = BucketCount(table);
    for (std::size_t index = 0; index < count; ++index) {
      EmitBucket(table, index, fn);
    }
  }

  template <typename Fn>
  bool EmitBucketWhile(const Table& table, std::uint64_t index, Fn& fn) const {
    if (!table.buckets_) return true;
    const Bucket* bucket = &table.buckets_[index];
    while (bucket != nullptr) {
      for (std::size_t slot = 0; slot < kEntriesPerBucket; ++slot) {
        if (Occupied(*bucket, slot) &&
            !fn(*const_cast<Entry*>(Resolve(bucket->entries_[slot])))) {
          return false;
        }
      }
      bucket = Chained(*bucket) ? Child(table, bucket) : nullptr;
    }
    return true;
  }

  template <typename Fn>
  bool ForEachTableWhile(Table& table, Fn& fn) {
    const std::size_t count = BucketCount(table);
    for (std::size_t index = 0; index < count; ++index) {
      if (!EmitBucketWhile(table, index, fn)) return false;
    }
    return true;
  }

  void DestroyTable(Table& table, bool destroy_entries) noexcept {
    const std::size_t count = BucketCount(table);
    for (std::size_t index = 0; index < count; ++index) {
      Bucket* top = &table.buckets_[index];
      Bucket* bucket = top;
      while (bucket != nullptr) {
        const bool chained = Chained(*bucket);
        Bucket* next = chained ? Child(table, bucket) : nullptr;
        if (destroy_entries) {
          for (std::size_t slot = 0; slot < kEntriesPerBucket; ++slot) {
            if (Occupied(*bucket, slot)) {
              const EntryHandle handle = bucket->entries_[slot];
              DestroyEntry(Resolve(handle), handle);
            }
          }
        }
        bucket = next;
      }
    }
    table = Table{};
  }

  void MoveFrom(ScanHashMap&& other) noexcept {
    arena_ = std::move(other.arena_);
    tables_ = std::move(other.tables_);
    rehash_index_ = std::exchange(other.rehash_index_, kNotRehashing);
    shrink_pending_ = std::exchange(other.shrink_pending_, false);
    other.tables_ = {};
  }

  std::array<Table, 2> tables_{};
  std::size_t rehash_index_ = kNotRehashing;
  bool shrink_pending_ = false;
  std::shared_ptr<ScanHashMapEntryArena> arena_;
};

template <typename Value, unsigned MaxBucketExponent, typename EntryPolicy>
std::uint8_t ScanHashMap<Value, MaxBucketExponent, EntryPolicy>::Entry::
    EncodedKeyMetadataBytes(std::uint32_t encoded) noexcept {
  std::uint8_t bytes = 1;
  while (encoded >= 0x80) {
    encoded >>= 7;
    ++bytes;
  }
  return bytes;
}

template <typename Value, unsigned MaxBucketExponent, typename EntryPolicy>
std::uint8_t
ScanHashMap<Value, MaxBucketExponent, EntryPolicy>::Entry::EncodeKeyMetadata(
    std::byte* output, std::uint32_t logical_size, bool key_complete) noexcept {
  assert(output != nullptr);
  assert(logical_size <= kMaxLogicalKeySize);
  std::uint32_t encoded =
      (logical_size << 1) | static_cast<std::uint32_t>(!key_complete);
  const std::uint8_t bytes = EncodedKeyMetadataBytes(encoded);
  for (std::uint8_t index = 0; index < bytes; ++index) {
    std::uint8_t byte = static_cast<std::uint8_t>(encoded & 0x7f);
    encoded >>= 7;
    if (encoded != 0) {
      byte |= 0x80;
    }
    output[index] = static_cast<std::byte>(byte);
  }
  return bytes;
}

template <typename Value, unsigned MaxBucketExponent, typename EntryPolicy>
typename ScanHashMap<Value, MaxBucketExponent, EntryPolicy>::Entry::KeyMetadata
ScanHashMap<Value, MaxBucketExponent, EntryPolicy>::Entry::DecodeKeyMetadata(
    const std::byte* input) noexcept {
  assert(input != nullptr);
  std::uint32_t encoded = 0;
  for (std::uint8_t index = 0; index < 5; ++index) {
    const std::uint8_t byte = std::to_integer<std::uint8_t>(input[index]);
    if (index == 4) {
      assert((byte & 0xf0) == 0);
    }
    encoded |= static_cast<std::uint32_t>(byte & 0x7f) << (index * 7);
    if ((byte & 0x80) == 0) {
      return KeyMetadata{
          .logical_size_ = encoded >> 1,
          .encoded_bytes_ = static_cast<std::uint8_t>(index + 1),
          .key_complete_ = (encoded & 1) == 0,
      };
    }
  }
  assert(false && "unterminated key metadata varint");
  return {};
}

template <typename Value, unsigned MaxBucketExponent, typename EntryPolicy>
std::size_t ScanHashMap<Value, MaxBucketExponent,
                        EntryPolicy>::Entry::tail_bytes() const noexcept {
  const KeyMetadata metadata = DecodeKeyMetadata(tail());
  return metadata.encoded_bytes_ +
         (metadata.key_complete_ ? metadata.logical_size_ : sizeof(Digest));
}

template <typename Value, unsigned MaxBucketExponent, typename EntryPolicy>
typename ScanHashMap<Value, MaxBucketExponent, EntryPolicy>::Entry::Allocation
ScanHashMap<Value, MaxBucketExponent, EntryPolicy>::Entry::Create(
    ScanHashMapEntryArena& arena, const Digest& digest, std::string_view key,
    const Value& value, bool key_complete) {
  if (key.size() > kMaxLogicalKeySize) {
    return {};
  }
  const std::uint32_t logical_size = static_cast<std::uint32_t>(key.size());
  const std::uint32_t encoded_metadata =
      (logical_size << 1) | static_cast<std::uint32_t>(!key_complete);
  const std::size_t metadata_bytes = EncodedKeyMetadataBytes(encoded_metadata);
  const std::size_t payload_bytes = key_complete ? key.size() : sizeof(digest);
  const std::size_t tail_bytes = metadata_bytes + payload_bytes;
  const bool extended = EntryPolicy::HasExtraValue(value);
  const std::size_t header_bytes =
      extended ? sizeof(ExtendedEntry) : sizeof(Entry);
  if (tail_bytes > std::numeric_limits<std::size_t>::max() - header_bytes) {
    return {};
  }
  static_assert(alignof(Entry) <= __STDCPP_DEFAULT_NEW_ALIGNMENT__);
  static_assert(alignof(ExtendedEntry) <= __STDCPP_DEFAULT_NEW_ALIGNMENT__);
  const ScanHashMapEntryArena::Allocation allocation =
      arena.Allocate(header_bytes + tail_bytes);
  if (allocation.pointer_ == nullptr) return {};
  void* storage = allocation.pointer_;
  Entry* entry = nullptr;
  try {
    if (extended) {
      entry = new (storage) ExtendedEntry(EntryPolicy::Store(value),
                                          EntryPolicy::StoreExtra(value));
    } else {
      entry = new (storage) Entry(EntryPolicy::Store(value));
    }
  } catch (...) {
    arena.Deallocate(allocation.handle_);
    throw;
  }
  const std::uint8_t written_metadata =
      EncodeKeyMetadata(entry->tail(), logical_size, key_complete);
  assert(written_metadata == metadata_bytes);
  std::byte* payload = entry->tail() + written_metadata;
  if (key_complete && !key.empty()) {
    std::memcpy(payload, key.data(), key.size());
  } else if (!key_complete) {
    std::memcpy(payload, &digest, sizeof(digest));
  }
  return {.entry_ = entry, .handle_ = allocation.handle_};
}

template <typename Value, unsigned MaxBucketExponent, typename EntryPolicy>
typename ScanHashMap<Value, MaxBucketExponent, EntryPolicy>::Entry::Allocation
ScanHashMap<Value, MaxBucketExponent, EntryPolicy>::Entry::CreateReplacement(
    ScanHashMapEntryArena& arena, const Entry& source, const Value& value) {
  const std::size_t tail_bytes = source.tail_bytes();
  const bool extended = EntryPolicy::HasExtraValue(value);
  const std::size_t header_bytes =
      extended ? sizeof(ExtendedEntry) : sizeof(Entry);
  if (tail_bytes > std::numeric_limits<std::size_t>::max() - header_bytes) {
    return {};
  }
  const ScanHashMapEntryArena::Allocation allocation =
      arena.Allocate(header_bytes + tail_bytes);
  if (allocation.pointer_ == nullptr) return {};
  void* storage = allocation.pointer_;
  Entry* entry = nullptr;
  try {
    if (extended) {
      entry = new (storage) ExtendedEntry(EntryPolicy::Store(value),
                                          EntryPolicy::StoreExtra(value));
    } else {
      entry = new (storage) Entry(EntryPolicy::Store(value));
    }
  } catch (...) {
    arena.Deallocate(allocation.handle_);
    throw;
  }
  if (tail_bytes != 0) {
    std::memcpy(entry->tail(), source.tail(), tail_bytes);
  }
  return {.entry_ = entry, .handle_ = allocation.handle_};
}

template <typename Value, unsigned MaxBucketExponent, typename EntryPolicy>
typename ScanHashMap<Value, MaxBucketExponent, EntryPolicy>::EntryExtra*
ScanHashMap<Value, MaxBucketExponent, EntryPolicy>::Entry::extra() noexcept {
  return &static_cast<ExtendedEntry*>(this)->extra_;
}

template <typename Value, unsigned MaxBucketExponent, typename EntryPolicy>
const typename ScanHashMap<Value, MaxBucketExponent, EntryPolicy>::EntryExtra*
ScanHashMap<Value, MaxBucketExponent, EntryPolicy>::Entry::extra()
    const noexcept {
  return &static_cast<const ExtendedEntry*>(this)->extra_;
}

template <typename Value, unsigned MaxBucketExponent, typename EntryPolicy>
std::byte*
ScanHashMap<Value, MaxBucketExponent, EntryPolicy>::Entry::tail() noexcept {
  return reinterpret_cast<std::byte*>(this) +
         (has_extra() ? sizeof(ExtendedEntry) : sizeof(Entry));
}

template <typename Value, unsigned MaxBucketExponent, typename EntryPolicy>
const std::byte* ScanHashMap<Value, MaxBucketExponent,
                             EntryPolicy>::Entry::tail() const noexcept {
  return reinterpret_cast<const std::byte*>(this) +
         (has_extra() ? sizeof(ExtendedEntry) : sizeof(Entry));
}

}  // namespace keylane::storage
