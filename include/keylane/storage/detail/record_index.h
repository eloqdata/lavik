#pragma once

#include <cassert>
#include <cstdint>
#include <limits>

#include "keylane/memory.h"
#include "keylane/storage/format.h"
#include "keylane/storage/scan_hash_map.h"

namespace keylane::storage {

class RecordIndexValue;

struct RecordLocationCore {
  // Runtime block identities need only the 27-bit local block id plus the
  // 16-bit configured device id. Pairing those 43 bits with a 53-bit
  // allocation epoch removes four bytes without weakening any practical
  // reuse horizon: at 8 MiB per allocation, the epoch spans 64 ZiB per
  // device. The durable format retains both original 64-bit fields.
  static constexpr unsigned kBlockIdBits = kLocalBlockIdBits + 16;
  static constexpr unsigned kAllocationEpochBits = 53;
  static constexpr unsigned kAllocationEpochLowBits = 64 - kBlockIdBits;
  static constexpr std::uint64_t kBlockIdMask =
      (std::uint64_t{1} << kBlockIdBits) - 1;
  static constexpr std::uint64_t kAllocationEpochMask =
      (std::uint64_t{1} << kAllocationEpochBits) - 1;

  static_assert(kBlockIdBits == 43);
  static_assert(kAllocationEpochLowBits == 21);
  static_assert(kAllocationEpochBits - kAllocationEpochLowBits == 32);

  // Offsets and record lengths are always 8-byte aligned inside an 8 MiB
  // block, so storing their alignment units preserves their complete range in
  // 20 bits each. The owner is process-local and InitMemoryLimit caps a
  // process at 1024 workers. Keep this as an explicitly masked runtime word,
  // rather than C++ bit-fields, so layout and overflow behavior are auditable.
  // Expiration and grouped-collection representation are independent bits.
  // Grouped is not external: it selects a collection directory, whereas
  // external selects the extent representation of one physical payload.
  // Uncommon state belongs in sparse side tables, not in every key's value.
  class PackedMetadata {
   public:
    static constexpr unsigned kOffsetBits = 20;
    static constexpr unsigned kLengthBits = 20;
    static constexpr unsigned kOwnerBits = 10;
    static constexpr unsigned kStateBits = 11;
    static constexpr unsigned kReservedBits = 3;

    static constexpr unsigned kLengthShift = kOffsetBits;
    static constexpr unsigned kOwnerShift = kLengthShift + kLengthBits;
    static constexpr unsigned kInMemoryShift = kOwnerShift + kOwnerBits;
    static constexpr unsigned kExternalShift = kInMemoryShift + 1;
    static constexpr unsigned kKeyExternalShift = kExternalShift + 1;
    static constexpr unsigned kShieldingShift = kKeyExternalShift + 1;
    static constexpr unsigned kUnclaimedShift = kShieldingShift + 1;
    static constexpr unsigned kTxTaggedShift = kUnclaimedShift + 1;
    static constexpr unsigned kTypeCodeShift = kTxTaggedShift + 1;
    static constexpr unsigned kHasExpiryShift = kTypeCodeShift + 3;
    static constexpr unsigned kGroupedShift = kHasExpiryShift + 1;

    static constexpr std::uint64_t kOffsetMask =
        (std::uint64_t{1} << kOffsetBits) - 1;
    static constexpr std::uint64_t kLengthMask =
        (std::uint64_t{1} << kLengthBits) - 1;
    static constexpr std::uint64_t kOwnerMask =
        (std::uint64_t{1} << kOwnerBits) - 1;
    static constexpr std::uint64_t kTypeCodeMask = 0x7;
    static constexpr std::uint8_t kTombstoneTypeCode = 0x7;

    static_assert(kStorageBlockBytes / kRecordAlignment - 1 <= kOffsetMask);
    static_assert((kStorageBlockBytes - kBlockHeaderBytes) / kRecordAlignment <=
                  kLengthMask);
    static_assert(kMaxMemoryWorkers <= (std::uint64_t{1} << kOwnerBits));

    static PackedMetadata Encode(std::uint32_t record_offset,
                                 std::uint32_t total_disk_bytes,
                                 std::uint16_t block_owner, bool in_memory,
                                 bool external, bool key_external,
                                 bool shielding, bool unclaimed, bool tx_tagged,
                                 RecordKind kind, ValueType value_type,
                                 bool has_expiry = false,
                                 bool grouped = false) noexcept {
      assert(record_offset % kRecordAlignment == 0);
      assert(total_disk_bytes % kRecordAlignment == 0);
      assert((record_offset / kRecordAlignment) <= kOffsetMask);
      assert((total_disk_bytes / kRecordAlignment) <= kLengthMask);
      assert(block_owner < kMaxMemoryWorkers);
      assert(kind == RecordKind::kValue || kind == RecordKind::kTombstone);
      assert(kind != RecordKind::kValue ||
             static_cast<std::uint8_t>(value_type) < kTombstoneTypeCode);
      assert(kind != RecordKind::kTombstone || value_type == ValueType::kNone);

      const std::uint8_t type_code =
          kind == RecordKind::kTombstone
              ? kTombstoneTypeCode
              : static_cast<std::uint8_t>(value_type);
      std::uint64_t bits =
          static_cast<std::uint64_t>(record_offset / kRecordAlignment) |
          (static_cast<std::uint64_t>(total_disk_bytes / kRecordAlignment)
           << kLengthShift) |
          (static_cast<std::uint64_t>(block_owner) << kOwnerShift) |
          (static_cast<std::uint64_t>(type_code) << kTypeCodeShift);
      SetBit(&bits, kInMemoryShift, in_memory);
      SetBit(&bits, kExternalShift, external);
      SetBit(&bits, kKeyExternalShift, key_external);
      SetBit(&bits, kShieldingShift, shielding);
      SetBit(&bits, kUnclaimedShift, unclaimed);
      SetBit(&bits, kTxTaggedShift, tx_tagged);
      SetBit(&bits, kHasExpiryShift, has_expiry);
      SetBit(&bits, kGroupedShift, grouped);
      return PackedMetadata(bits);
    }

    std::uint32_t record_offset() const noexcept {
      return static_cast<std::uint32_t>(bits_ & kOffsetMask) * kRecordAlignment;
    }
    std::uint32_t total_disk_bytes() const noexcept {
      return static_cast<std::uint32_t>((bits_ >> kLengthShift) & kLengthMask) *
             kRecordAlignment;
    }
    std::uint16_t block_owner() const noexcept {
      return static_cast<std::uint16_t>((bits_ >> kOwnerShift) & kOwnerMask);
    }
    bool in_memory() const noexcept { return Bit(kInMemoryShift); }
    bool external() const noexcept { return Bit(kExternalShift); }
    bool key_external() const noexcept { return Bit(kKeyExternalShift); }
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

    void set_in_memory(bool value) noexcept {
      SetBit(&bits_, kInMemoryShift, value);
    }
    void set_shielding(bool value) noexcept {
      SetBit(&bits_, kShieldingShift, value);
    }
    void set_unclaimed(bool value) noexcept {
      SetBit(&bits_, kUnclaimedShift, value);
    }
    void set_tx_tagged(bool value) noexcept {
      SetBit(&bits_, kTxTaggedShift, value);
    }
    void set_has_expiry(bool value) noexcept {
      SetBit(&bits_, kHasExpiryShift, value);
    }

   private:
    friend class RecordIndexValue;

    explicit constexpr PackedMetadata(std::uint64_t bits) noexcept
        : bits_(bits) {}

    static void SetBit(std::uint64_t* bits, unsigned shift,
                       bool value) noexcept {
      const std::uint64_t mask = std::uint64_t{1} << shift;
      *bits = value ? (*bits | mask) : (*bits & ~mask);
    }
    bool Bit(unsigned shift) const noexcept {
      return (bits_ & (std::uint64_t{1} << shift)) != 0;
    }
    std::uint8_t type_code() const noexcept {
      return static_cast<std::uint8_t>((bits_ >> kTypeCodeShift) &
                                       kTypeCodeMask);
    }

    std::uint64_t bits_ = 0;
  };

  static_assert(PackedMetadata::kOffsetBits + PackedMetadata::kLengthBits +
                    PackedMetadata::kOwnerBits + PackedMetadata::kStateBits +
                    PackedMetadata::kReservedBits ==
                64);

  RecordLocationCore() noexcept = default;

  RecordLocationCore(std::uint64_t block_id, std::uint64_t mutation_sequence,
                     std::uint64_t allocation_epoch, std::uint32_t logical_size,
                     PackedMetadata metadata) noexcept
      : mutation_sequence_(mutation_sequence),
        block_and_epoch_low_(
            EncodeBlockAndEpochLow(block_id, allocation_epoch)),
        allocation_epoch_high_(EncodeAllocationEpochHigh(allocation_epoch)),
        logical_size_(logical_size),
        metadata_(metadata) {
    assert(CanEncodeBlockIdentity(block_id, allocation_epoch));
  }

  static constexpr bool CanEncodeBlockIdentity(
      std::uint64_t block_id, std::uint64_t allocation_epoch) noexcept {
    return block_id <= kBlockIdMask && allocation_epoch <= kAllocationEpochMask;
  }

  std::uint64_t mutation_sequence_ = 0;
  // Low word: block id in bits [0, 42], low allocation-epoch bits in
  // [43, 63]. The remaining 32 epoch bits sit beside logical_size_, filling
  // what would otherwise be alignment padding before metadata_.
  std::uint64_t block_and_epoch_low_ = 0;
  std::uint32_t allocation_epoch_high_ = 0;
  // Exact Redis-visible bytes/cardinality.
  std::uint32_t logical_size_ = 0;
  PackedMetadata metadata_ =
      PackedMetadata::Encode(0, 0, 0, false, false, false, false, false, false,
                             RecordKind::kValue, ValueType::kNone);

  std::uint64_t block_id() const noexcept {
    return block_and_epoch_low_ & kBlockIdMask;
  }
  std::uint64_t allocation_epoch() const noexcept {
    return (block_and_epoch_low_ >> kBlockIdBits) |
           (static_cast<std::uint64_t>(allocation_epoch_high_)
            << kAllocationEpochLowBits);
  }

  std::uint32_t record_offset() const noexcept {
    return metadata_.record_offset();
  }
  std::uint32_t total_disk_bytes() const noexcept {
    return metadata_.total_disk_bytes();
  }
  std::uint16_t block_owner() const noexcept { return metadata_.block_owner(); }
  bool in_memory() const noexcept { return metadata_.in_memory(); }
  bool external() const noexcept { return metadata_.external(); }
  bool key_external() const noexcept { return metadata_.key_external(); }
  // True while an older, still-unexpired value of this key may survive on
  // disk. Erasing this entry then would un-suppress that copy: recovery
  // picks the newest surviving record, so the key would resurrect with the
  // stale value. Propagates through every overwrite — tombstones included,
  // since a superseded tombstone leaves the disk like any dead record — and
  // is rebuilt exactly during recovery, which sees every surviving record.
  bool shielding() const noexcept { return metadata_.shielding(); }
  // Tomb-raider round state: set on candidates (tombstones, shielded values)
  // when a round begins, cleared when the sweep finds an older on-disk
  // record the entry still suppresses. Whatever survives the sweep
  // unclaimed proved nothing on disk needs it. False outside rounds, and
  // any overwrite resets it, exempting concurrently-touched keys.
  bool unclaimed() const noexcept { return metadata_.unclaimed(); }
  // The on-disk record carries a nonzero transaction id. Retirement uses the
  // bit to remove its bytes from transaction-generation accounting; the
  // 32-byte index core deliberately does not retain the full txid.
  bool tx_tagged() const noexcept { return metadata_.tx_tagged(); }
  bool has_expiry() const noexcept { return metadata_.has_expiry(); }
  bool grouped() const noexcept { return metadata_.grouped(); }
  RecordKind kind() const noexcept { return metadata_.kind(); }
  ValueType value_type() const noexcept { return metadata_.value_type(); }

  void set_in_memory(bool value) noexcept { metadata_.set_in_memory(value); }
  void set_shielding(bool value) noexcept { metadata_.set_shielding(value); }
  void set_unclaimed(bool value) noexcept { metadata_.set_unclaimed(value); }
  void set_tx_tagged(bool value) noexcept { metadata_.set_tx_tagged(value); }

  bool SamePhysicalRecord(const RecordLocationCore& other) const noexcept {
    return block_id() == other.block_id() &&
           record_offset() == other.record_offset() &&
           allocation_epoch() == other.allocation_epoch();
  }

 private:
  static constexpr std::uint64_t EncodeBlockAndEpochLow(
      std::uint64_t block_id, std::uint64_t allocation_epoch) noexcept {
    return (block_id & kBlockIdMask) |
           ((allocation_epoch &
             ((std::uint64_t{1} << kAllocationEpochLowBits) - 1))
            << kBlockIdBits);
  }

  static constexpr std::uint32_t EncodeAllocationEpochHigh(
      std::uint64_t allocation_epoch) noexcept {
    return static_cast<std::uint32_t>(allocation_epoch >>
                                      kAllocationEpochLowBits);
  }
};

struct RecordLocation final : RecordLocationCore {
  RecordLocation() noexcept = default;

  RecordLocation(std::uint64_t block_id, std::uint64_t mutation_sequence,
                 std::uint64_t allocation_epoch, std::uint64_t expire_at_ms,
                 std::uint32_t logical_size, PackedMetadata metadata) noexcept
      : RecordLocationCore(block_id, mutation_sequence, allocation_epoch,
                           logical_size, metadata),
        expire_at_ms_(expire_at_ms) {
    metadata_.set_has_expiry(expire_at_ms != 0);
  }

  RecordLocation(const RecordLocationCore& core,
                 std::uint64_t expire_at_ms) noexcept
      : RecordLocationCore(core), expire_at_ms_(expire_at_ms) {
    assert(has_expiry() == (expire_at_ms != 0));
  }

  std::uint64_t expire_at_ms_ = 0;
};

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
  static constexpr unsigned kKeyExternalShift = kExternalShift + 1;
  static constexpr unsigned kShieldingShift = kKeyExternalShift + 1;
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
  bool key_external() const noexcept { return Bit(kKeyExternalShift); }
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
    static_assert(kKeyExternalShift + 1 == Runtime::kKeyExternalShift);
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
    SetBit(&bits, kKeyExternalShift, value.key_external());
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

// Record-index entries use a 24-byte common object for ordinary keys and a
// derived 32-byte object only when an expiration timestamp exists. The
// has-expiry bit is part of the compact common value, so checking it before
// the downcast makes the concrete type an explicit allocation invariant.
struct RecordIndexEntryPolicy {
  using StoredValue = RecordIndexValue;
  using Extra = std::uint64_t;

  static bool HasExtraValue(const RecordLocation& value) noexcept {
    return value.expire_at_ms_ != 0;
  }
  static bool HasExtraStored(const StoredValue& value) noexcept {
    return value.has_expiry();
  }
  static StoredValue Store(const RecordLocation& value) noexcept {
    return StoredValue(value);
  }
  static Extra StoreExtra(const RecordLocation& value) noexcept {
    return value.expire_at_ms_;
  }
  static RecordLocation Load(const StoredValue& value, const Extra* extra,
                             std::uint64_t allocation_epoch,
                             std::uint16_t block_owner) noexcept {
    assert(value.has_expiry() == (extra != nullptr));
    RecordLocationCore core(value.block_id(), value.mutation_sequence_,
                            allocation_epoch, value.logical_size(),
                            value.MaterializeMetadata(block_owner));
    // MaterializeMetadata already carries the trusted subtype discriminator;
    // this constructor validates it instead of clearing and setting the same
    // bit again on every lookup.
    return RecordLocation(core, extra == nullptr ? 0 : *extra);
  }
  static void Assign(StoredValue* stored, Extra* extra,
                     const RecordLocation& value) noexcept {
    *stored = StoredValue(value);
    assert(stored->has_expiry() == (extra != nullptr));
    if (extra != nullptr) {
      *extra = value.expire_at_ms_;
    }
  }
};

using RecordIndex =
    ScanHashMap<RecordLocation, std::numeric_limits<std::uint32_t>::digits,
                RecordIndexEntryPolicy>;

static_assert(static_cast<std::uint8_t>(ValueType::kStream) < (1U << 3));
static_assert(sizeof(RecordLocationCore) == 32);
static_assert(sizeof(RecordLocation) == 40);
static_assert(alignof(RecordLocation) == 8);
static_assert(sizeof(RecordIndex::Entry) == 24);
static_assert(sizeof(RecordIndex::ExtendedEntry) == 32);

inline bool IsNewer(const RecordLocation& candidate,
                    const RecordLocation& current) noexcept {
  if (candidate.mutation_sequence_ != current.mutation_sequence_) {
    return candidate.mutation_sequence_ > current.mutation_sequence_;
  }
  return false;
}

}  // namespace keylane::storage
