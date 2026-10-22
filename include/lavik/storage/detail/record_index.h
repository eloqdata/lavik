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

#include "lavik/memory.h"
#include "lavik/storage/detail/record_location.h"
#include "lavik/storage/format.h"
#include "lavik/storage/scan_hash_map.h"

namespace lavik::storage {

// The index retains only key-specific location state. A block's allocation
// epoch and runtime owner are already stored once in its dense BlockState;
// repeating them in every record from the same 8 MiB allocation would cost
// another word per key. Callers materialize a full RecordLocation from this
// value and the current BlockState before carrying the identity across an
// await. Live-byte accounting keeps the allocation live while a current index
// entry references one of its records, so the shared identity remains stable
// for that operation.
class RecordIndexValue {
 public:
  static constexpr unsigned kBlockIdBits = RecordLocation::kBlockIdBits;
  static constexpr unsigned kLogicalSizeBits = 30;
  static constexpr unsigned kLogicalSizeLowBits = 64 - kBlockIdBits;
  static constexpr unsigned kLogicalSizeHighBits =
      kLogicalSizeBits - kLogicalSizeLowBits;
  static constexpr unsigned kOffsetBits = 20;
  static constexpr unsigned kLengthBits = 20;
  static constexpr unsigned kStateBits = 11;
  static constexpr unsigned kReservedBits =
      64 - kLogicalSizeHighBits - kOffsetBits - kLengthBits - kStateBits;

  static constexpr unsigned kOffsetShift = kLogicalSizeHighBits;
  static constexpr unsigned kLengthShift = kOffsetShift + kOffsetBits;
  static constexpr unsigned kInMemoryShift = kLengthShift + kLengthBits;
  static constexpr unsigned kExternalShift = kInMemoryShift + 1;
  static constexpr unsigned kKeyIndirectShift = kExternalShift + 1;
  static constexpr unsigned kShieldingShift = kKeyIndirectShift + 1;
  static constexpr unsigned kUnclaimedShift = kShieldingShift + 1;
  static constexpr unsigned kTxTaggedShift = kUnclaimedShift + 1;
  static constexpr unsigned kTypeCodeShift = kTxTaggedShift + 1;
  static constexpr unsigned kHasExpiryShift = kTypeCodeShift + 3;
  static constexpr unsigned kGroupedShift = kHasExpiryShift + 1;

  static constexpr std::uint64_t kBlockIdMask =
      (std::uint64_t{1} << kBlockIdBits) - 1;
  static constexpr std::uint64_t kLogicalSizeMask =
      (std::uint64_t{1} << kLogicalSizeBits) - 1;
  static constexpr std::uint64_t kLogicalSizeLowMask =
      (std::uint64_t{1} << kLogicalSizeLowBits) - 1;
  static constexpr std::uint64_t kLogicalSizeHighMask =
      (std::uint64_t{1} << kLogicalSizeHighBits) - 1;
  static constexpr std::uint64_t kOffsetMask =
      (std::uint64_t{1} << kOffsetBits) - 1;
  static constexpr std::uint64_t kLengthMask =
      (std::uint64_t{1} << kLengthBits) - 1;
  static constexpr std::uint8_t kTypeCodeMask = 0x7;
  static constexpr std::uint8_t kTombstoneTypeCode = 0x7;

  static_assert(kBlockIdBits == 43);
  static_assert(kLogicalSizeLowBits == 21);
  static_assert(kLogicalSizeHighBits == 9);
  static_assert(kReservedBits == 4);
  static_assert(kMaxBitmapBytes <= kLogicalSizeMask);
  static_assert(kStorageBlockBytes / kRecordAlignment - 1 <= kOffsetMask);
  static_assert((kStorageBlockBytes - kBlockHeaderBytes) / kRecordAlignment <=
                kLengthMask);

  RecordIndexValue() noexcept = default;
  explicit RecordIndexValue(const RecordLocation& value) noexcept
      : mutation_sequence_(value.mutation_sequence_),
        block_and_logical_low_(
            (value.block_id() & kBlockIdMask) |
            ((static_cast<std::uint64_t>(value.logical_size_) &
              kLogicalSizeLowMask)
             << kBlockIdBits)),
        metadata_(EncodeMetadata(value)) {
    assert(value.logical_size_ <= kLogicalSizeMask);
  }

  std::uint64_t mutation_sequence_ = 0;

  std::uint64_t block_id() const noexcept {
    return block_and_logical_low_ & kBlockIdMask;
  }
  std::uint32_t logical_size() const noexcept {
    const std::uint64_t low = block_and_logical_low_ >> kBlockIdBits;
    const std::uint64_t high = metadata_ & kLogicalSizeHighMask;
    return static_cast<std::uint32_t>(low | (high << kLogicalSizeLowBits));
  }
  std::uint32_t record_offset() const noexcept {
    return static_cast<std::uint32_t>((metadata_ >> kOffsetShift) &
                                      kOffsetMask) *
           kRecordAlignment;
  }
  std::uint32_t total_disk_bytes() const noexcept {
    return static_cast<std::uint32_t>((metadata_ >> kLengthShift) &
                                      kLengthMask) *
           kRecordAlignment;
  }
  bool in_memory() const noexcept { return Bit(kInMemoryShift); }
  bool external() const noexcept { return Bit(kExternalShift); }
  bool key_indirect() const noexcept { return Bit(kKeyIndirectShift); }
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

  // Rebuilds runtime metadata without expanding and repacking each state bit.
  // The compact and runtime layouts deliberately keep the physical fields and
  // state bits contiguous; only the logical-size prefix and owner slot
  // differ. Static assertions below make a future layout change fail here
  // instead of silently corrupting a materialized location.
  RecordLocation::PackedMetadata MaterializeMetadata(
      std::uint16_t block_owner) const noexcept {
    using Runtime = RecordLocation::PackedMetadata;
    static_assert(kLengthShift - kOffsetShift == Runtime::kLengthShift);
    static_assert(kInMemoryShift + 1 == Runtime::kInMemoryShift);
    static_assert(kExternalShift + 1 == Runtime::kExternalShift);
    static_assert(kKeyIndirectShift + 1 == Runtime::kKeyIndirectShift);
    static_assert(kShieldingShift + 1 == Runtime::kShieldingShift);
    static_assert(kUnclaimedShift + 1 == Runtime::kUnclaimedShift);
    static_assert(kTxTaggedShift + 1 == Runtime::kTxTaggedShift);
    static_assert(kTypeCodeShift + 1 == Runtime::kTypeCodeShift);
    static_assert(kHasExpiryShift + 1 == Runtime::kHasExpiryShift);
    static_assert(kGroupedShift + 1 == Runtime::kGroupedShift);
    static_assert(kTypeCodeMask == Runtime::kTypeCodeMask);
    static_assert(kTombstoneTypeCode == Runtime::kTombstoneTypeCode);
    assert(block_owner < kMaxMemoryWorkers);

    constexpr std::uint64_t kPhysicalMask =
        ((std::uint64_t{1} << (kOffsetBits + kLengthBits)) - 1) << kOffsetShift;
    constexpr std::uint64_t kStateMask = ((std::uint64_t{1} << kStateBits) - 1)
                                         << kInMemoryShift;
    const std::uint64_t runtime_bits =
        ((metadata_ & kPhysicalMask) >> kOffsetShift) |
        (static_cast<std::uint64_t>(block_owner) << Runtime::kOwnerShift) |
        ((metadata_ & kStateMask) << 1);
    return Runtime(runtime_bits);
  }

  void set_in_memory(bool value) noexcept {
    SetBit(&metadata_, kInMemoryShift, value);
  }
  void set_shielding(bool value) noexcept {
    SetBit(&metadata_, kShieldingShift, value);
  }
  void set_unclaimed(bool value) noexcept {
    SetBit(&metadata_, kUnclaimedShift, value);
  }
  void set_tx_tagged(bool value) noexcept {
    SetBit(&metadata_, kTxTaggedShift, value);
  }

 private:
  static std::uint64_t EncodeMetadata(const RecordLocation& value) noexcept {
    assert(value.record_offset() % kRecordAlignment == 0);
    assert(value.total_disk_bytes() % kRecordAlignment == 0);
    std::uint64_t bits =
        (static_cast<std::uint64_t>(value.logical_size_) >>
         kLogicalSizeLowBits) |
        (static_cast<std::uint64_t>(value.record_offset() / kRecordAlignment)
         << kOffsetShift) |
        (static_cast<std::uint64_t>(value.total_disk_bytes() / kRecordAlignment)
         << kLengthShift) |
        (static_cast<std::uint64_t>(
             value.kind() == RecordKind::kTombstone
                 ? kTombstoneTypeCode
                 : static_cast<std::uint8_t>(value.value_type()))
         << kTypeCodeShift);
    SetBit(&bits, kInMemoryShift, value.in_memory());
    SetBit(&bits, kExternalShift, value.external());
    SetBit(&bits, kKeyIndirectShift, value.key_indirect());
    SetBit(&bits, kShieldingShift, value.shielding());
    SetBit(&bits, kUnclaimedShift, value.unclaimed());
    SetBit(&bits, kTxTaggedShift, value.tx_tagged());
    SetBit(&bits, kHasExpiryShift, value.has_expiry());
    SetBit(&bits, kGroupedShift, value.grouped());
    return bits;
  }

  static void SetBit(std::uint64_t* bits, unsigned shift, bool value) noexcept {
    const std::uint64_t mask = std::uint64_t{1} << shift;
    *bits = value ? (*bits | mask) : (*bits & ~mask);
  }
  bool Bit(unsigned shift) const noexcept {
    return (metadata_ & (std::uint64_t{1} << shift)) != 0;
  }
  std::uint8_t type_code() const noexcept {
    return static_cast<std::uint8_t>((metadata_ >> kTypeCodeShift) &
                                     kTypeCodeMask);
  }

  std::uint64_t block_and_logical_low_ = 0;
  std::uint64_t metadata_ = 0;
};

static_assert(sizeof(RecordIndexValue) == 24);

// Primary entries append a byte-width logical size to this fixed prefix,
// after the optional expiry word and before the existing key tail. Grouped
// auxiliary arrays keep the self-contained RecordIndexValue above: copying a
// prefix alone does not copy its logical size. Only call logical_size() on a
// prefix embedded in a fully initialized arena entry.
//
// Four-byte alignment prevents the compiler from rounding the prefix to 24
// bytes. Arena slots are actually eight-byte aligned; memcpy handles the
// possibly unaligned expiry and logical-size loads. No durable format changes.
#pragma pack(push, 4)
class RecordIndexPrefix {
 public:
  static constexpr std::uint32_t kPhysicalMask = (1U << 20) - 1;
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
      : mutation_sequence_(value.mutation_sequence_),
        block_and_offset_(
            value.block_id() |
            (std::uint64_t{value.record_offset() / kRecordAlignment} << 43) |
            (std::uint64_t{value.key_indirect()} << 63)) {
    using Runtime = RecordLocation::PackedMetadata;
    assert(value.logical_size_ <= RecordIndexValue::kLogicalSizeMask);
    const std::uint64_t state =
        value.metadata_.bits_ >> Runtime::kInMemoryShift;
    // Move only key_indirect beside the physical offset. The remaining ten
    // state bits leave two bits for the logical-size width in this word.
    metadata_ = value.total_disk_bytes() / kRecordAlignment |
                (static_cast<std::uint32_t>(state & 3) << 20) |
                (static_cast<std::uint32_t>((state >> 3) & 0xff) << 22) |
                ((LogicalBytesFor(value.logical_size_) - 1) << kWidthShift);
  }

  std::uint64_t mutation_sequence_ = 0;

  static unsigned LogicalBytesFor(std::uint32_t size) noexcept {
    return (std::bit_width(size | 1U) + 7) / 8;
  }
  unsigned logical_bytes() const noexcept {
    return (metadata_ >> kWidthShift) + 1;
  }
  std::uint64_t block_id() const noexcept {
    return block_and_offset_ & RecordIndexValue::kBlockIdMask;
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
    return type_code() == RecordIndexValue::kTombstoneTypeCode
               ? RecordKind::kTombstone
               : RecordKind::kValue;
  }
  ValueType value_type() const noexcept {
    return type_code() == RecordIndexValue::kTombstoneTypeCode
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
    RecordIndexPrefix next(value);
    next.metadata_ = (next.metadata_ & ~(3U << kWidthShift)) |
                     (metadata_ & (3U << kWidthShift));
    *this = next;
  }

 private:
  template <typename, unsigned, typename, std::size_t>
  friend class ScanHashMap;
  friend struct RecordIndexEntryPolicy;
  RecordIndexPrefix(const RecordIndexPrefix&) = default;
  RecordIndexPrefix& operator=(const RecordIndexPrefix&) = default;

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
  std::byte* logical_data() noexcept {
    return const_cast<std::byte*>(std::as_const(*this).logical_data());
  }
  std::uint64_t block_and_offset_ = 0;
  std::uint32_t metadata_ = 0;
};
#pragma pack(pop)
static_assert(sizeof(RecordIndexPrefix) == 20);

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
  // Auxiliary group locations remain self-contained fixed-width values.
  static RecordLocation Load(const RecordIndexValue& value,
                             const std::uint64_t* extra, std::uint64_t epoch,
                             std::uint16_t owner) noexcept {
    assert(value.has_expiry() == (extra != nullptr));
    return RecordLocation(
        RecordLocationCore(value.block_id(), value.mutation_sequence_, epoch,
                           value.logical_size(),
                           value.MaterializeMetadata(owner)),
        extra == nullptr ? 0 : *extra);
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

inline bool IsNewer(const RecordLocation& candidate,
                    const RecordLocation& current) noexcept {
  if (candidate.mutation_sequence_ != current.mutation_sequence_) {
    return candidate.mutation_sequence_ > current.mutation_sequence_;
  }
  return false;
}

}  // namespace lavik::storage
