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

#include <array>
#include <bit>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <limits>
#include <type_traits>

#include "lavik/memory.h"
#include "lavik/storage/detail/record_location.h"
#include "lavik/storage/format.h"
#include "lavik/storage/scan_hash_map.h"

namespace lavik::storage {

// Primary entries and fixed-width auxiliary values share this prefix. The
// logical size follows the optional expiry word: primary entries use 1-4
// bytes before their key tail; RecordIndexValue owns a full four-byte tail.
// A prefix alone does not own that tail and must not be copied out of its
// enclosing storage. Only call logical_size() on a fully initialized entry
// or RecordIndexValue.
//
// Block owner and allocation epoch remain shared in BlockState. Materialize
// a complete RecordLocation while the index entry is current before carrying
// its physical identity across suspension.
//
// Four-byte alignment prevents the compiler from rounding the prefix to 24
// bytes. Arena slots are actually eight-byte aligned; memcpy handles the
// possibly unaligned expiry and logical-size loads. No durable format changes.
#pragma pack(push, 4)
class RecordIndexPrefix {
 public:
  static constexpr unsigned kBlockIdBits = RecordLocation::kBlockIdBits;
  static constexpr std::uint64_t kBlockIdMask = RecordLocation::kBlockIdMask;
  static constexpr unsigned kLogicalSizeBits = 30;
  static constexpr std::uint32_t kLogicalSizeMask =
      (1U << kLogicalSizeBits) - 1;
  static constexpr std::uint8_t kTombstoneTypeCode =
      RecordLocation::PackedMetadata::kTombstoneTypeCode;
  static constexpr std::uint32_t kPhysicalMask = (1U << 20) - 1;
  static_assert(kBlockIdBits == 43);
  static_assert(kMaxBitmapBytes <= kLogicalSizeMask);
  static_assert(kStorageBlockBytes / kRecordAlignment - 1 <= kPhysicalMask);
  static_assert((kStorageBlockBytes - kBlockHeaderBytes) / kRecordAlignment <=
                kPhysicalMask);
  static constexpr unsigned kInMemoryShift = 20;
  static constexpr unsigned kExternalShift = 21;
  static constexpr unsigned kShieldingShift = 22;
  static constexpr unsigned kUnclaimedShift = 23;
  static constexpr unsigned kTxTaggedShift = 24;
  static constexpr unsigned kTypeCodeShift = 25;
  static constexpr unsigned kHasExpiryShift = 28;
  static constexpr unsigned kGroupedShift = 29;
  static constexpr unsigned kWidthShift = 30;

  RecordIndexPrefix() noexcept = default;
  explicit RecordIndexPrefix(const RecordLocation& value) noexcept
      : RecordIndexPrefix(value, LogicalBytesFor(value.logical_size_)) {}

  std::uint64_t mutation_sequence_ = 0;

  static unsigned LogicalBytesFor(std::uint32_t size) noexcept {
    return (std::bit_width(size | 1U) + 7) / 8;
  }
  unsigned logical_bytes() const noexcept {
    return (metadata_ >> kWidthShift) + 1;
  }
  std::uint64_t block_id() const noexcept {
    return block_and_offset_ & kBlockIdMask;
  }
  std::uint32_t logical_size() const noexcept {
    std::uint32_t word;
    std::memcpy(&word, logical_data(), sizeof(word));
    return word & logical_mask();
  }
  std::uint32_t record_offset() const noexcept {
    return ((block_and_offset_ >> 43) & kPhysicalMask) * kRecordAlignment;
  }
  std::uint32_t total_disk_bytes() const noexcept {
    return (metadata_ & kPhysicalMask) * kRecordAlignment;
  }
  bool in_memory() const noexcept { return Bit(kInMemoryShift); }
  bool external() const noexcept { return Bit(kExternalShift); }
  bool key_indirect() const noexcept { return (block_and_offset_ >> 63) != 0; }
  bool shielding() const noexcept { return Bit(kShieldingShift); }
  bool unclaimed() const noexcept { return Bit(kUnclaimedShift); }
  bool tx_tagged() const noexcept { return Bit(kTxTaggedShift); }
  bool has_expiry() const noexcept { return Bit(kHasExpiryShift); }
  bool grouped() const noexcept { return Bit(kGroupedShift); }
  RecordKind kind() const noexcept {
    return type_code() == kTombstoneTypeCode ? RecordKind::kTombstone
                                             : RecordKind::kValue;
  }
  ValueType value_type() const noexcept {
    return type_code() == kTombstoneTypeCode
               ? ValueType::kNone
               : static_cast<ValueType>(type_code());
  }
  RecordLocation::PackedMetadata MaterializeMetadata(
      std::uint16_t owner) const noexcept {
    using Runtime = RecordLocation::PackedMetadata;
    static_assert(Runtime::kOffsetBits == 20 && Runtime::kLengthBits == 20);
    static_assert(Runtime::kInMemoryShift == 50 &&
                  Runtime::kKeyIndirectShift == 52);
    static_assert(Runtime::kShieldingShift == 53 &&
                  Runtime::kGroupedShift == 60);
    assert(owner < kMaxMemoryWorkers);
    return Runtime(((block_and_offset_ >> 43) & kPhysicalMask) |
                   (std::uint64_t{metadata_ & kPhysicalMask} << 20) |
                   (std::uint64_t{owner} << Runtime::kOwnerShift) |
                   (std::uint64_t{metadata_ & (3U << 20)} << 30) |
                   ((block_and_offset_ >> 11) & (1ULL << 52)) |
                   (std::uint64_t{metadata_ & (0xffU << 22)} << 31));
  }
  void set_in_memory(bool value) noexcept { SetBit(kInMemoryShift, value); }
  void set_shielding(bool value) noexcept { SetBit(kShieldingShift, value); }
  void set_unclaimed(bool value) noexcept { SetBit(kUnclaimedShift, value); }
  void set_tx_tagged(bool value) noexcept { SetBit(kTxTaggedShift, value); }

  // Shrinking retains the allocated width: neither the key address nor the
  // entry identity changes. Growth and expiry changes require replacement and
  // must be admitted before the append becomes visible.
  void Assign(const RecordLocation& value, std::byte* logical_tail) noexcept {
    assert(value.has_expiry() == has_expiry());
    assert(LogicalBytesFor(value.logical_size_) <= logical_bytes());
    const std::uint32_t mask = logical_mask();
    std::uint32_t word;
    std::memcpy(&word, logical_tail, sizeof(word));
    word = (word & ~mask) | value.logical_size_;
    std::memcpy(logical_tail, &word, sizeof(word));
    *this = RecordIndexPrefix(value, logical_bytes());
  }

 private:
  template <typename, unsigned, typename, std::size_t>
  friend class ScanHashMap;
  friend struct RecordIndexEntryPolicy;
  friend class RecordIndexValue;
  RecordIndexPrefix(const RecordIndexPrefix&) = default;
  RecordIndexPrefix& operator=(const RecordIndexPrefix&) = default;

  RecordIndexPrefix(const RecordLocation& value,
                    unsigned logical_bytes) noexcept
      : mutation_sequence_(value.mutation_sequence_),
        block_and_offset_(
            value.block_id() |
            (std::uint64_t{value.record_offset() / kRecordAlignment} << 43) |
            (std::uint64_t{value.key_indirect()} << 63)) {
    using Runtime = RecordLocation::PackedMetadata;
    assert(value.logical_size_ <= kLogicalSizeMask);
    assert(logical_bytes >= LogicalBytesFor(value.logical_size_) &&
           logical_bytes <= 4);
    const std::uint64_t state =
        value.metadata_.bits_ >> Runtime::kInMemoryShift;
    // Move only key_indirect beside the physical offset. The remaining ten
    // state bits leave two bits for the logical-size width in this word.
    metadata_ = value.total_disk_bytes() / kRecordAlignment |
                (static_cast<std::uint32_t>(state & 3) << 20) |
                (static_cast<std::uint32_t>((state >> 3) & 0xff) << 22) |
                ((logical_bytes - 1) << kWidthShift);
  }

  bool Bit(unsigned shift) const noexcept {
    return ((metadata_ >> shift) & 1) != 0;
  }
  void SetBit(unsigned shift, bool value) noexcept {
    const auto mask = 1U << shift;
    metadata_ = value ? metadata_ | mask : metadata_ & ~mask;
  }
  std::uint8_t type_code() const noexcept {
    return (metadata_ >> kTypeCodeShift) & 7;
  }
  std::uint32_t logical_mask() const noexcept {
    return UINT32_MAX >> ((3 - (metadata_ >> kWidthShift)) * 8);
  }
  const std::byte* logical_data() const noexcept {
    return reinterpret_cast<const std::byte*>(this) + sizeof(*this) +
           static_cast<unsigned>(has_expiry()) * sizeof(std::uint64_t);
  }
  std::uint64_t block_and_offset_ = 0;
  std::uint32_t metadata_ = 0;
};
#pragma pack(pop)
static_assert(sizeof(RecordIndexPrefix) == 20);

// Fixed-stride storage for auxiliary arrays and coordinate overrides. These
// records have no independent expiry. The common prefix always advertises a
// four-byte logical tail, so ordinary copies include every retained field and
// array elements remain 24 bytes without a second metadata encoding.
class RecordIndexValue final : public RecordIndexPrefix {
 public:
  RecordIndexValue() noexcept : RecordIndexValue(RecordLocation{}) {}
  explicit RecordIndexValue(const RecordLocation& value) noexcept
      : RecordIndexPrefix(value, sizeof(logical_size_)),
        logical_size_(value.logical_size_) {
    assert(!value.has_expiry());
  }

 private:
  std::uint32_t logical_size_ = 0;
};
static_assert(sizeof(RecordIndexValue) == 24);
static_assert(std::is_trivially_copyable_v<RecordIndexValue>);

// The expiry begins at byte 20. A byte-backed word preserves its full range
// without padding the derived entry back up to an eight-byte boundary.
struct alignas(4) RecordIndexExpiry {
  RecordIndexExpiry() noexcept = default;
  RecordIndexExpiry(std::uint64_t value) noexcept { *this = value; }
  RecordIndexExpiry& operator=(std::uint64_t value) noexcept {
    std::memcpy(bytes_.data(), &value, sizeof(value));
    return *this;
  }
  operator std::uint64_t() const noexcept {
    std::uint64_t value;
    std::memcpy(&value, bytes_.data(), sizeof(value));
    return value;
  }
  std::array<std::byte, 8> bytes_{};
};
static_assert(sizeof(RecordIndexExpiry) == 8);

struct RecordIndexEntryPolicy {
  using StoredValue = RecordIndexPrefix;
  using Extra = RecordIndexExpiry;
  static constexpr std::size_t kMaxTailBytes = 4;

  static bool HasExtraValue(const RecordLocation& value) noexcept {
    return value.expire_at_ms_ != 0;
  }
  static bool HasExtraStored(const StoredValue& value) noexcept {
    return value.has_expiry();
  }
  static std::size_t TailBytesFor(const RecordLocation& value) noexcept {
    return StoredValue::LogicalBytesFor(value.logical_size_);
  }
  static std::size_t TailBytes(const StoredValue& value) noexcept {
    return value.logical_bytes();
  }
  static bool CanAssign(const StoredValue& stored,
                        const RecordLocation& value) noexcept {
    return TailBytesFor(value) <= TailBytes(stored);
  }
  // Must precede writing the key tail. A four-byte load at offset 20 or 28
  // stays within the arena's minimum 32-byte, eight-byte-rounded allocation.
  // Initialize all four bytes, including any bytes later used by key metadata,
  // so subsequent masked loads never read uninitialized padding.
  static void InitializeTail(StoredValue*, const RecordLocation& value,
                             std::byte* tail) noexcept {
    static_assert(std::endian::native == std::endian::little);
    std::memcpy(tail, &value.logical_size_, sizeof(value.logical_size_));
  }
  static StoredValue Store(const RecordLocation& value) noexcept {
    return StoredValue(value);
  }
  static Extra StoreExtra(const RecordLocation& value) noexcept {
    return value.expire_at_ms_;
  }
  static RecordLocation Load(const StoredValue& value, const Extra* extra,
                             std::uint64_t epoch,
                             std::uint16_t owner) noexcept {
    assert(value.has_expiry() == (extra != nullptr));
    return RecordLocation(
        RecordLocationCore(value.block_id(), value.mutation_sequence_, epoch,
                           value.logical_size(),
                           value.MaterializeMetadata(owner)),
        extra == nullptr ? 0 : static_cast<std::uint64_t>(*extra));
  }
  static void Assign(StoredValue* stored, Extra* extra,
                     const RecordLocation& value, std::byte* tail) noexcept {
    stored->Assign(value, tail);
    assert(stored->has_expiry() == (extra != nullptr));
    if (extra != nullptr) *extra = value.expire_at_ms_;
  }
};

// Only the primary key index reserves read stripes. Keeping every allocated
// table at least this large makes hash & 31 a stable stripe across resizing;
// auxiliary and grouped-object maps retain their owner-only layout.
using RecordIndex =
    ScanHashMap<RecordLocation, std::numeric_limits<std::uint32_t>::digits,
                RecordIndexEntryPolicy, 32>;

static_assert(static_cast<std::uint8_t>(ValueType::kStream) < (1U << 3));
static_assert(sizeof(RecordLocationCore) == 32);
static_assert(sizeof(RecordLocation) == 40);
static_assert(alignof(RecordLocation) == 8);
static_assert(sizeof(RecordIndex::Entry) == 20);
static_assert(sizeof(RecordIndex::ExtendedEntry) == 28);

}  // namespace lavik::storage
