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
 * two-table expansion, and stateless reverse-bit scan algorithm. It replaces
 * Valkey runtime dependencies and generic callbacks with Keylane-owned entries.
 */

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

#include "keylane/storage/format.h"

namespace keylane::storage {

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
  static constexpr unsigned kSlotBits = 11;
  static constexpr Handle kSlotMask = (Handle{1} << kSlotBits) - 1;
  static constexpr std::uint32_t kMaximumPageId =
      (std::uint32_t{1} << (32 - kSlotBits)) - 1;
  static_assert(std::has_single_bit(kPageBytes));

  explicit ScanHashMapEntryArena(
      std::uint32_t maximum_page_id = kMaximumPageId);
  ScanHashMapEntryArena(const ScanHashMapEntryArena&) = delete;
  ScanHashMapEntryArena& operator=(const ScanHashMapEntryArena&) = delete;
  ~ScanHashMapEntryArena();

  // Returns false only when this allocation would need a new page and every
  // encodable page ID is already live. Physical allocator failure can still
  // throw std::bad_alloc when Allocate actually obtains the page.
  bool CanAllocate(std::size_t bytes) const noexcept;
  Allocation Allocate(std::size_t bytes);
  void Deallocate(Handle handle) noexcept;

  // Resolve translates a live bucket handle; it does not independently track
  // whether the selected slot is allocated.
  void* Resolve(Handle handle) const noexcept;
  // HandleOf accepts an address owned by this arena and returns zero when the
  // aligned page belongs to a different arena.
  Handle HandleOf(const void* pointer) const noexcept;

  std::size_t allocated_pages() const noexcept { return allocated_pages_; }
  std::size_t allocated_spans() const noexcept { return small_spans_.size(); }
  std::uint32_t maximum_page_id() const noexcept { return maximum_page_id_; }

 private:
  static constexpr std::size_t kPageHeaderBytes = 64;
  static constexpr std::uint16_t kNoSlot =
      std::numeric_limits<std::uint16_t>::max();
  static constexpr std::uint8_t kLargeClassMarker = 0x3f;
  static constexpr std::size_t kSmallClassCount = 53;

  struct SmallSpan;

  struct alignas(64) PageHeader {
    std::uint32_t page_id_ = 0;
    std::uint32_t allocation_bytes_ = 0;
    std::uint32_t block_size_ = 0;
    std::uint32_t available_index_ = 0;
    SmallSpan* small_span_ = nullptr;
    std::uint16_t capacity_ = 0;
    std::uint16_t next_unused_ = 0;
    std::uint16_t free_head_ = kNoSlot;
    std::uint16_t live_count_ = 0;
    std::uint8_t class_index_ = 0;
    std::uint8_t span_slot_ = 0;
    bool listed_available_ = false;
    std::array<std::byte, 29> padding_{};
  };

  static_assert(sizeof(PageHeader) == kPageHeaderBytes);

  struct SmallSpan {
    void* storage_ = nullptr;
    std::uint32_t arena_index_ = 0;
    std::uint32_t available_index_ = 0;
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

  std::uint32_t AllocatePageId();
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
  std::size_t allocated_pages_ = 0;
  // Directory slot zero stays empty so handle zero is the null reference.
  // A normal descriptor packs the 64 KiB-aligned page address with
  // class_index+1 in its low bits; the large marker reads block size from the
  // page header. The vector grows only on the page-allocation slow path.
  std::vector<std::uint64_t> page_directory_{0};
  std::vector<std::uint32_t> free_page_ids_;
  std::array<std::vector<std::uint32_t>, kSmallClassCount> available_pages_;
  std::vector<SmallSpan*> small_spans_;
  std::vector<SmallSpan*> available_spans_;
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
    std::uint32_t maximum_page_id)
    : maximum_page_id_(std::min(maximum_page_id, kMaximumPageId)) {}

inline ScanHashMapEntryArena::~ScanHashMapEntryArena() {
  for (std::size_t page_id = 1; page_id < page_directory_.size(); ++page_id) {
    const std::uint64_t descriptor = page_directory_[page_id];
    if (descriptor == 0) continue;
    auto* page =
        reinterpret_cast<PageHeader*>(descriptor & ~std::uint64_t{0xffff});
    assert(page->live_count_ == 0 &&
           "entry arena outlived a map that still owns entries");
    if (page->small_span_ == nullptr) {
      ::operator delete(page, std::align_val_t{kPageBytes});
    }
  }
  for (SmallSpan* span : small_spans_) {
    ::operator delete(span->storage_, std::align_val_t{kPageBytes});
    delete span;
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

inline std::uint32_t ScanHashMapEntryArena::AllocatePageId() {
  if (!free_page_ids_.empty()) {
    const std::uint32_t page_id = free_page_ids_.back();
    free_page_ids_.pop_back();
    assert(page_id != 0 && page_id < page_directory_.size() &&
           page_directory_[page_id] == 0);
    return page_id;
  }
  if (page_directory_.size() > maximum_page_id_) {
    throw std::bad_alloc();
  }
  // Releasing a page is noexcept and must be able to return its ID without
  // allocating. Grow the free-ID capacity on this already-fallible slow path
  // before publishing another directory slot.
  if (free_page_ids_.capacity() < page_directory_.size()) {
    free_page_ids_.reserve(
        std::max(page_directory_.size(), free_page_ids_.capacity() * 2));
  }
  const std::uint32_t page_id =
      static_cast<std::uint32_t>(page_directory_.size());
  page_directory_.push_back(0);
  return page_id;
}

inline void ScanHashMapEntryArena::AddAvailable(PageHeader* page) {
  assert(page != nullptr && page->class_index_ < kSmallClassCount &&
         !page->listed_available_);
  auto& pages = available_pages_[page->class_index_];
  page->available_index_ = static_cast<std::uint32_t>(pages.size());
  pages.push_back(page->page_id_);
  page->listed_available_ = true;
}

inline void ScanHashMapEntryArena::RemoveAvailable(PageHeader* page) noexcept {
  if (!page->listed_available_) return;
  auto& pages = available_pages_[page->class_index_];
  const std::size_t index = page->available_index_;
  assert(index < pages.size() && pages[index] == page->page_id_);
  const std::uint32_t moved_id = pages.back();
  pages[index] = moved_id;
  pages.pop_back();
  if (index < pages.size()) {
    PageHeader* moved = PageFor(moved_id << kSlotBits);
    assert(moved != nullptr);
    moved->available_index_ = static_cast<std::uint32_t>(index);
  }
  page->listed_available_ = false;
}

inline void ScanHashMapEntryArena::AddAvailableSpan(SmallSpan* span) {
  assert(span != nullptr && span->free_page_mask_ != 0 &&
         !span->listed_available_);
  span->available_index_ =
      static_cast<std::uint32_t>(available_spans_.size());
  available_spans_.push_back(span);
  span->listed_available_ = true;
}

inline void ScanHashMapEntryArena::RemoveAvailableSpan(
    SmallSpan* span) noexcept {
  if (!span->listed_available_) return;
  const std::size_t index = span->available_index_;
  assert(index < available_spans_.size() && available_spans_[index] == span);
  SmallSpan* moved = available_spans_.back();
  available_spans_[index] = moved;
  available_spans_.pop_back();
  if (index < available_spans_.size()) {
    moved->available_index_ = static_cast<std::uint32_t>(index);
  }
  span->listed_available_ = false;
}

inline ScanHashMapEntryArena::SmallSpan*
ScanHashMapEntryArena::AllocateSmallSpan() {
  auto* span = new SmallSpan();
  try {
    span->storage_ = ::operator new(kSpanBytes, std::align_val_t{kPageBytes});
    span->arena_index_ = static_cast<std::uint32_t>(small_spans_.size());
    small_spans_.push_back(span);
    try {
      AddAvailableSpan(span);
    } catch (...) {
      small_spans_.pop_back();
      throw;
    }
  } catch (...) {
    if (span->storage_ != nullptr) {
      ::operator delete(span->storage_, std::align_val_t{kPageBytes});
    }
    delete span;
    throw;
  }
  return span;
}

inline void ScanHashMapEntryArena::ReleaseSmallSpan(
    SmallSpan* span) noexcept {
  assert(span != nullptr && span->live_pages_ == 0);
  RemoveAvailableSpan(span);
  const std::size_t index = span->arena_index_;
  assert(index < small_spans_.size() && small_spans_[index] == span);
  SmallSpan* moved = small_spans_.back();
  small_spans_[index] = moved;
  small_spans_.pop_back();
  if (index < small_spans_.size()) {
    moved->arena_index_ = static_cast<std::uint32_t>(index);
  }
  ::operator delete(span->storage_, std::align_val_t{kPageBytes});
  delete span;
}

inline ScanHashMapEntryArena::PageHeader* ScanHashMapEntryArena::AllocatePage(
    std::uint8_t class_index, std::size_t requested_bytes) {
  const std::uint32_t page_id = AllocatePageId();
  const bool large = class_index == kLargeClassMarker;
  const std::size_t block_size = large
                                     ? ((requested_bytes + 7) & ~std::size_t{7})
                                     : ClassBytes(class_index);
  if (block_size > std::numeric_limits<std::size_t>::max() - kPageHeaderBytes) {
    page_directory_[page_id] = 0;
    free_page_ids_.push_back(page_id);
    throw std::bad_alloc();
  }
  const std::size_t required = kPageHeaderBytes + block_size;
  const std::size_t allocation_bytes =
      large ? ((required + kPageBytes - 1) & ~(kPageBytes - 1)) : 0;
  void* storage = nullptr;
  SmallSpan* small_span = nullptr;
  unsigned span_slot = 0;
  try {
    if (large) {
      storage = ::operator new(allocation_bytes, std::align_val_t{kPageBytes});
    } else {
      small_span = available_spans_.empty() ? AllocateSmallSpan()
                                            : available_spans_.back();
      span_slot = std::countr_zero(small_span->free_page_mask_);
      assert(span_slot < kPagesPerSpan);
      small_span->free_page_mask_ &=
          static_cast<std::uint16_t>(~(std::uint32_t{1} << span_slot));
      ++small_span->live_pages_;
      if (small_span->free_page_mask_ == 0) RemoveAvailableSpan(small_span);
      storage = static_cast<std::byte*>(small_span->storage_) +
                span_slot * kPageBytes;
    }
  } catch (...) {
    page_directory_[page_id] = 0;
    free_page_ids_.push_back(page_id);
    throw;
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
      static_cast<std::uint64_t>(large ? kLargeClassMarker : class_index + 1);
  page_directory_[page_id] = descriptor;
  ++allocated_pages_;
  if (!large) {
    try {
      AddAvailable(page);
    } catch (...) {
      ReleasePage(page);
      throw;
    }
  }
  return page;
}

inline ScanHashMapEntryArena::PageHeader* ScanHashMapEntryArena::PageFor(
    Handle handle) const noexcept {
  const std::uint32_t page_id = handle >> kSlotBits;
  if (page_id == 0 || page_id >= page_directory_.size()) return nullptr;
  const std::uint64_t descriptor = page_directory_[page_id];
  if (descriptor == 0) return nullptr;
  return reinterpret_cast<PageHeader*>(descriptor & ~std::uint64_t{0xffff});
}

inline bool ScanHashMapEntryArena::CanAllocate(
    std::size_t bytes) const noexcept {
  const std::uint8_t class_index = ClassFor(bytes);
  if (class_index != kLargeClassMarker &&
      !available_pages_[class_index].empty()) {
    return true;
  }
  return !free_page_ids_.empty() || page_directory_.size() <= maximum_page_id_;
}

inline ScanHashMapEntryArena::Allocation ScanHashMapEntryArena::Allocate(
    std::size_t bytes) {
  const std::uint8_t class_index = ClassFor(bytes);
  PageHeader* page = nullptr;
  if (class_index != kLargeClassMarker &&
      !available_pages_[class_index].empty()) {
    page = PageFor(available_pages_[class_index].back() << kSlotBits);
    assert(page != nullptr);
  } else {
    page = AllocatePage(class_index, bytes);
  }

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
  page_directory_[page_id] = 0;
  page->~PageHeader();
  if (span == nullptr) {
    ::operator delete(page, std::align_val_t{kPageBytes});
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
  free_page_ids_.push_back(page_id);
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
  const PageHeader* page = PageFor(handle);
  if (page == nullptr) return nullptr;
  const std::uint16_t slot = static_cast<std::uint16_t>(handle & kSlotMask);
  if (slot >= page->capacity_) return nullptr;
  return const_cast<std::byte*>(SlotAddress(page, slot));
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
  if (page->page_id_ >= page_directory_.size() ||
      (page_directory_[page->page_id_] & ~std::uint64_t{0xffff}) !=
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
    return tables_[0].buckets_ != nullptr || tables_[1].buckets_ != nullptr;
  }

  // Includes both tables during incremental expansion. Primarily useful for
  // capacity diagnostics and saturation tests.
  std::size_t allocated_bucket_count() const noexcept {
    return BucketCount(tables_[0]) + BucketCount(tables_[1]);
  }

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
    RehashStep();
    return FindWithoutStep(digest, key);
  }

  const Entry* Find(const Digest& digest, std::string_view key) const {
    return FindWithoutStep(digest, key);
  }

  std::vector<Entry*> FindCandidates(const Digest& digest,
                                     std::string_view key) {
    RehashStep();
    std::vector<Entry*> result;
    const std::uint64_t hash = Hash(digest);
    AppendCandidates(tables_[0], digest, key, hash, &result);
    if (Rehashing()) {
      AppendCandidates(tables_[1], digest, key, hash, &result);
    }
    return result;
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
      if (table.buckets_ == nullptr) {
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
    const std::uint64_t hash = Hash(digest);
    const int tables = Rehashing() ? 2 : 1;
    for (int t = 0; t < tables; ++t) {
      Table& table = tables_[t];
      if (table.buckets_ == nullptr) {
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
      return {existing, false};
    }

    EnsureTable();
    MaybeStartExpansion();
    typename Entry::Allocation allocation =
        Entry::Create(EnsureArena(), digest, key, value, key_complete);
    try {
      AddToTable(Rehashing() ? tables_[1] : tables_[0], allocation.handle_,
                 Hash(digest));
    } catch (...) {
      DestroyEntry(allocation.entry_, allocation.handle_);
      throw;
    }
    return {allocation.entry_, true};
  }

  Entry* InsertNew(const Digest& digest, std::string_view key,
                   const Value& value, bool key_complete = true) {
    EnsureTable();
    MaybeStartExpansion();
    typename Entry::Allocation allocation =
        Entry::Create(EnsureArena(), digest, key, value, key_complete);
    try {
      AddToTable(Rehashing() ? tables_[1] : tables_[0], allocation.handle_,
                 Hash(digest));
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
  // The table itself never shrinks (the port dropped shrinking with deletion);
  // slots are reused by later inserts, so footprint is bounded by the peak
  // live count.
  bool Erase(const Digest& digest, std::string_view key) {
    RehashStep();
    const std::uint64_t hash = Hash(digest);
    const std::uint8_t tag = HashTag(hash);
    const int tables = Rehashing() ? 2 : 1;
    for (int t = 0; t < tables; ++t) {
      Table& table = tables_[t];
      if (table.buckets_ == nullptr) {
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
            return true;
          }
        }
      }
    }
    return false;
  }

  bool Erase(Entry* entry) {
    if (entry == nullptr) {
      return false;
    }
    RehashStep();
    const std::uint64_t hash = EntryHash(*entry);
    const int tables = Rehashing() ? 2 : 1;
    for (int t = 0; t < tables; ++t) {
      Table& table = tables_[t];
      if (table.buckets_ == nullptr) {
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
            return true;
          }
        }
      }
    }
    return false;
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
  // be invoked more than once for an entry. Mutation from another thread
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

    const Table& small = tables_[0];
    const Table& large = tables_[1];
    const std::uint64_t small_mask = BucketMask(small);
    const std::uint64_t large_mask = BucketMask(large);

    const std::uint64_t small_index = cursor & small_mask;
    if (small_index >= rehash_index_) {
      EmitBucket(small, small_index, fn);
    }

    do {
      EmitBucket(large, cursor & large_mask, fn);
      cursor = NextCursor(cursor, large_mask);
    } while ((cursor & (small_mask ^ large_mask)) != 0);

    return cursor;
  }

  void Clear() noexcept {
    DestroyTable(tables_[0], true);
    DestroyTable(tables_[1], true);
    rehash_index_ = kNotRehashing;
  }

  // Moves every entry out into the returned map and leaves *this empty and
  // immediately usable. Destroying the returned map is what actually frees the
  // entries, so a caller can hand it to a background task and keep serving
  // reads from *this. O(1) — no entry is touched here.
  //
  // Entry addresses are stable across expansion and ordinary assignment.
  // ReplaceValue deliberately returns the detached old object when a storage
  // policy changes concrete type, making the exceptional invalidation
  // explicit to its caller. A raw-pointer cache must likewise detect detach
  // before dereferencing; this class does not track that, since the useful
  // granularity is whatever set of maps the caller detaches together.
  ScanHashMap Detach() noexcept {
    ScanHashMap detached(arena_);
    detached.tables_ = std::exchange(tables_, {});
    detached.rehash_index_ = std::exchange(rehash_index_, kNotRehashing);
    return detached;
  }

 private:
  static constexpr std::size_t kEntriesPerBucket = 12;
  static constexpr std::size_t kTargetEntriesPerBucket = 9;
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

  struct OverflowBuckets {
    std::vector<std::unique_ptr<Bucket>> buckets_;
    std::vector<std::uint32_t> free_ids_;
  };

  struct Table {
    std::unique_ptr<Bucket[]> buckets_;
    std::unique_ptr<OverflowBuckets> overflow_;
    std::uint8_t exponent_ = 0;
    std::size_t used_ = 0;
  };

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

  static ChildAllocation AllocateChild(Table* table) {
    if (table->overflow_ == nullptr) {
      table->overflow_ = std::make_unique<OverflowBuckets>();
    }
    auto& pool = *table->overflow_;
    std::uint32_t index = 0;
    if (!pool.free_ids_.empty()) {
      auto bucket = std::make_unique<Bucket>();
      index = pool.free_ids_.back();
      pool.free_ids_.pop_back();
      assert(index < pool.buckets_.size() && pool.buckets_[index] == nullptr);
      pool.buckets_[index] = std::move(bucket);
    } else {
      if (pool.buckets_.size() >= std::numeric_limits<std::uint32_t>::max()) {
        throw std::bad_alloc();
      }
      // FreeChild is noexcept. Reserve its bookkeeping slot while allocation
      // is already allowed to fail, so erasing an overflow bucket never has
      // to allocate memory merely to remember the reusable ID.
      if (pool.free_ids_.capacity() < pool.buckets_.size() + 1) {
        pool.free_ids_.reserve(
            std::max(pool.buckets_.size() + 1,
                     std::max<std::size_t>(1, pool.free_ids_.capacity() * 2)));
      }
      index = static_cast<std::uint32_t>(pool.buckets_.size());
      pool.buckets_.push_back(std::make_unique<Bucket>());
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
    return table.buckets_ == nullptr ? 0 : std::size_t{1} << table.exponent_;
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
    if (entry.key_complete()) [[likely]] {
      return entry.key() == key;
    }
    return entry.logical_key_size() == key.size() &&
           entry.external_key_digest() == digest;
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

  void EnsureTable() {
    if (tables_[0].buckets_ == nullptr) {
      tables_[0].buckets_ = std::make_unique<Bucket[]>(1);
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
    tables_[1].exponent_ = tables_[0].exponent_ + 1;
    tables_[1].buckets_ =
        std::make_unique<Bucket[]>(std::size_t{1} << tables_[1].exponent_);
    tables_[1].used_ = 0;
    rehash_index_ = 0;
  }

  void RehashStep() {
    if (!Rehashing()) {
      return;
    }

    MoveBucket(&tables_[0].buckets_[rehash_index_], &tables_[1]);
    ++rehash_index_;
    if (rehash_index_ == BucketCount(tables_[0])) {
      assert(tables_[0].used_ == 0);
      tables_[0] = std::move(tables_[1]);
      tables_[1] = Table{};
      rehash_index_ = kNotRehashing;
    }
  }

  Entry* FindInTable(Table& table, const Digest& digest, std::string_view key,
                     std::uint64_t hash) {
    if (table.buckets_ == nullptr) {
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
    if (table.buckets_ == nullptr) return nullptr;
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
    if (table.buckets_ == nullptr) {
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

  Entry* FindWithoutStep(const Digest& digest, std::string_view key) {
    const std::uint64_t hash = Hash(digest);
    if (Entry* found = FindInTable(tables_[0], digest, key, hash);
        found != nullptr) {
      return found;
    }
    return Rehashing() ? FindInTable(tables_[1], digest, key, hash) : nullptr;
  }

  const Entry* FindWithoutStep(const Digest& digest,
                               std::string_view key) const {
    const std::uint64_t hash = Hash(digest);
    if (const Entry* found = FindInTable(tables_[0], digest, key, hash);
        found != nullptr) {
      return found;
    }
    return Rehashing() ? FindInTable(tables_[1], digest, key, hash) : nullptr;
  }

  void AddToTable(Table& table, EntryHandle entry, std::uint64_t hash) {
    const std::uint8_t tag = HashTag(hash);
    Bucket* bucket = &table.buckets_[hash & BucketMask(table)];
    while (true) {
      for (std::size_t slot = 0; slot < kEntriesPerBucket; ++slot) {
        if (!Occupied(*bucket, slot)) {
          bucket->entries_[slot] = entry;
          bucket->hashes_[slot] = tag;
          ++table.used_;
          return;
        }
      }
      if (Chained(*bucket)) {
        bucket = Child(table, bucket);
        continue;
      }

      const ChildAllocation child = AllocateChild(&table);
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

  void MoveBucketEntries(Table* source, Bucket* top, Table* target) {
    Bucket* bucket = top;
    std::uint32_t bucket_id = 0;
    while (bucket != nullptr) {
      const bool chained = Chained(*bucket);
      Bucket* next = chained ? Child(*source, bucket) : nullptr;
      const std::uint32_t next_id = bucket->child_;
      for (std::size_t slot = 0; slot < kEntriesPerBucket; ++slot) {
        if (Occupied(*bucket, slot)) {
          // Entries no longer retain a bucket hash. Recompute it while the
          // source object is known live; external keys already carry their
          // digest, while inline keys trade rehash CPU for the smaller steady
          // state representation.
          const EntryHandle handle = bucket->entries_[slot];
          AddToTable(*target, handle, EntryHash(*Resolve(handle)));
          ClearOccupied(bucket, slot);
        }
      }
      bucket->child_ = 0;
      if (bucket_id != 0) FreeChild(source, bucket_id);
      bucket_id = next_id;
      bucket = next;
    }
    top->entries_.fill(0);
    top->hashes_.fill(0);
  }

  void MoveBucket(Bucket* top, Table* target) {
    const std::size_t before = target->used_;
    MoveBucketEntries(&tables_[0], top, target);
    const std::size_t moved = target->used_ - before;
    assert(tables_[0].used_ >= moved);
    tables_[0].used_ -= moved;
  }

  template <typename Fn>
  void EmitBucket(const Table& table, std::uint64_t index, Fn& fn) const {
    if (table.buckets_ == nullptr) {
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
    if (table.buckets_ == nullptr) return true;
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
    other.tables_ = {};
  }

  std::array<Table, 2> tables_{};
  std::size_t rehash_index_ = kNotRehashing;
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
    throw std::bad_alloc();
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
    throw std::bad_alloc();
  }
  static_assert(alignof(Entry) <= __STDCPP_DEFAULT_NEW_ALIGNMENT__);
  static_assert(alignof(ExtendedEntry) <= __STDCPP_DEFAULT_NEW_ALIGNMENT__);
  const ScanHashMapEntryArena::Allocation allocation =
      arena.Allocate(header_bytes + tail_bytes);
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
    throw std::bad_alloc();
  }
  const ScanHashMapEntryArena::Allocation allocation =
      arena.Allocate(header_bytes + tail_bytes);
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
