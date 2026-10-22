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

#include <cassert>
#include <cstdint>

#include "lavik/memory.h"
#include "lavik/storage/format.h"

namespace lavik::storage {

class RecordIndexPrefix;

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
    static constexpr unsigned kKeyIndirectShift = kExternalShift + 1;
    static constexpr unsigned kShieldingShift = kKeyIndirectShift + 1;
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
                                 bool external, bool key_indirect,
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
      SetBit(&bits, kKeyIndirectShift, key_indirect);
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
    friend class RecordIndexPrefix;

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
  bool key_indirect() const noexcept { return metadata_.key_indirect(); }
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
  // bit to remove its bytes from transaction-block accounting; the
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

}  // namespace lavik::storage
