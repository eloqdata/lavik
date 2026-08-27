#include <limits>

#include "../src/storage/engine/impl.h"
#include "gtest/gtest.h"

namespace keylane::storage {
namespace {

TEST(RecordLocationTest, PackedMetadataRoundTripsMaximumValues) {
  constexpr std::uint32_t offset =
      static_cast<std::uint32_t>(kStorageBlockBytes - kRecordAlignment);
  constexpr std::uint32_t length =
      static_cast<std::uint32_t>(kStorageBlockBytes - kBlockHeaderBytes);
  constexpr std::uint16_t owner = kMaxMemoryWorkers - 1;

  const RecordLocation::PackedMetadata packed =
      RecordLocation::PackedMetadata::Encode(
          offset, length, owner, true, true, true, true, true, true,
          RecordKind::kTombstone, ValueType::kNone);

  EXPECT_EQ(packed.record_offset(), offset);
  EXPECT_EQ(packed.total_disk_bytes(), length);
  EXPECT_EQ(packed.block_owner(), owner);
  EXPECT_TRUE(packed.in_memory());
  EXPECT_TRUE(packed.external());
  EXPECT_TRUE(packed.key_external());
  EXPECT_TRUE(packed.shielding());
  EXPECT_TRUE(packed.unclaimed());
  EXPECT_TRUE(packed.tx_tagged());
  EXPECT_FALSE(packed.has_expiry());
  EXPECT_EQ(packed.kind(), RecordKind::kTombstone);
  EXPECT_EQ(packed.value_type(), ValueType::kNone);
}

TEST(RecordLocationTest, PackedMetadataKeepsValueTypeAndMutableFlags) {
  RecordLocation::PackedMetadata packed =
      RecordLocation::PackedMetadata::Encode(
          kBlockHeaderBytes, AlignRecord(123), 17, false, true, false, false,
          false, false, RecordKind::kValue, ValueType::kStream);

  EXPECT_EQ(packed.record_offset(), kBlockHeaderBytes);
  EXPECT_EQ(packed.total_disk_bytes(), AlignRecord(123));
  EXPECT_EQ(packed.block_owner(), 17);
  EXPECT_EQ(packed.kind(), RecordKind::kValue);
  EXPECT_EQ(packed.value_type(), ValueType::kStream);
  EXPECT_FALSE(packed.in_memory());
  EXPECT_TRUE(packed.external());

  packed.set_in_memory(true);
  packed.set_shielding(true);
  packed.set_unclaimed(true);
  packed.set_tx_tagged(true);
  EXPECT_TRUE(packed.in_memory());
  EXPECT_TRUE(packed.shielding());
  EXPECT_TRUE(packed.unclaimed());
  EXPECT_TRUE(packed.tx_tagged());
}

TEST(RecordLocationTest, DefaultLocationRetainsEmptyValueSemantics) {
  const RecordLocation location;
  EXPECT_EQ(location.record_offset(), 0);
  EXPECT_EQ(location.total_disk_bytes(), 0);
  EXPECT_EQ(location.block_owner(), 0);
  EXPECT_EQ(location.kind(), RecordKind::kValue);
  EXPECT_EQ(location.value_type(), ValueType::kNone);
}

TEST(RecordLocationTest, PackedBlockIdentityRoundTripsMaximumValues) {
  EXPECT_FALSE(RecordLocation::CanEncodeBlockIdentity(
      RecordLocation::kBlockIdMask + 1, 1));
  EXPECT_FALSE(RecordLocation::CanEncodeBlockIdentity(
      1, RecordLocation::kAllocationEpochMask + 1));

  const RecordLocation location(
      RecordLocation::kBlockIdMask, std::numeric_limits<std::uint64_t>::max(),
      RecordLocation::kAllocationEpochMask,
      std::numeric_limits<std::uint64_t>::max(),
      std::numeric_limits<std::uint32_t>::max(),
      RecordLocation::PackedMetadata::Encode(
          kBlockHeaderBytes, kRecordAlignment, kMaxMemoryWorkers - 1, false,
          false, false, false, false, false, RecordKind::kValue,
          ValueType::kString));

  EXPECT_EQ(location.block_id(), RecordLocation::kBlockIdMask);
  EXPECT_EQ(location.allocation_epoch(), RecordLocation::kAllocationEpochMask);
  EXPECT_EQ(location.mutation_sequence_,
            std::numeric_limits<std::uint64_t>::max());
  EXPECT_EQ(location.expire_at_ms_, std::numeric_limits<std::uint64_t>::max());
  EXPECT_EQ(location.logical_size_, std::numeric_limits<std::uint32_t>::max());
}

static_assert(sizeof(RecordLocation::PackedMetadata) == sizeof(std::uint64_t));
static_assert(RecordLocation::PackedMetadata::kReservedBits == 4);
static_assert(sizeof(RecordLocationCore) == 32);
static_assert(sizeof(RecordLocation) == 40);
static_assert(sizeof(RecordIndex::Entry) == 40);
static_assert(sizeof(RecordIndex::ExtendedEntry) == 48);

TEST(RecordLocationTest, RecordIndexAllocatesExpirySubtypeOnlyWhenNeeded) {
  const auto metadata = RecordLocation::PackedMetadata::Encode(
      kBlockHeaderBytes, kRecordAlignment, 0, true, false, false, false, false,
      false, RecordKind::kValue, ValueType::kString);
  const Digest digest = ComputeDigest("key");
  RecordIndex index;

  RecordIndex::Entry* ordinary =
      index.InsertNew(digest, "key", RecordLocation(1, 2, 3, 0, 4, metadata));
  ASSERT_FALSE(ordinary->has_extra());
  EXPECT_EQ(ordinary->value().expire_at_ms_, 0);
  EXPECT_EQ(ordinary->key(), "key");

  RecordIndex::Entry* replaced = nullptr;
  RecordIndex::Entry* expiring = index.ReplaceValue(
      ordinary, RecordLocation(5, 6, 7, 1234, 8, metadata), &replaced);
  ASSERT_NE(replaced, nullptr);
  EXPECT_NE(expiring, ordinary);
  ASSERT_TRUE(expiring->has_extra());
  EXPECT_EQ(expiring->value().expire_at_ms_, 1234);
  EXPECT_EQ(expiring->key(), "key");
  const std::uint32_t ordinary_hash = replaced->hash_;
  const std::uintptr_t ordinary_address =
      reinterpret_cast<std::uintptr_t>(replaced);
  RecordIndex::Entry::Destroy(replaced);
  // FLUSH may retain this raw address after a representation change.
  // FindAddress must reject it without touching the freed object.
  EXPECT_EQ(index.FindAddress(ordinary_address, ordinary_hash), nullptr);
  EXPECT_EQ(index.FindAddress(reinterpret_cast<std::uintptr_t>(expiring),
                              expiring->hash_),
            expiring);
  EXPECT_TRUE(index.Contains(expiring, expiring->hash_));

  replaced = nullptr;
  ordinary = index.ReplaceValue(
      expiring, RecordLocation(9, 10, 11, 0, 12, metadata), &replaced);
  ASSERT_NE(replaced, nullptr);
  EXPECT_NE(ordinary, expiring);
  EXPECT_FALSE(ordinary->has_extra());
  EXPECT_EQ(ordinary->value().expire_at_ms_, 0);
  EXPECT_EQ(ordinary->key(), "key");
  const std::uint32_t expiring_hash = replaced->hash_;
  const std::uintptr_t expiring_address =
      reinterpret_cast<std::uintptr_t>(replaced);
  RecordIndex::Entry::Destroy(replaced);
  EXPECT_EQ(index.FindAddress(expiring_address, expiring_hash), nullptr);
  const RecordIndex& const_index = index;
  EXPECT_EQ(const_index.FindAddress(reinterpret_cast<std::uintptr_t>(ordinary),
                                    ordinary->hash_),
            ordinary);
  EXPECT_TRUE(index.Contains(ordinary, ordinary->hash_));
}

TEST(RecordLocationTest, TxUndoLogRetargetsSharedHandleInConstantTime) {
  const auto metadata = RecordLocation::PackedMetadata::Encode(
      kBlockHeaderBytes, kRecordAlignment, 0, true, false, false, false, false,
      true, RecordKind::kValue, ValueType::kString);
  RecordIndex index;
  RecordIndex::Entry* expiring = index.InsertNew(
      ComputeDigest("key"), "key", RecordLocation(1, 2, 3, 1234, 4, metadata));
  RecordIndex::Entry* other = index.InsertNew(
      ComputeDigest("other"), "other", RecordLocation(5, 6, 7, 0, 8, metadata));

  TxUndoLog undo;
  const std::uint32_t key_handle = undo.Track(expiring);
  EXPECT_EQ(undo.Track(expiring), key_handle);
  EXPECT_NE(undo.Track(other), key_handle);

  RecordIndex::Entry* replaced = nullptr;
  RecordIndex::Entry* ordinary = index.ReplaceValue(
      expiring, RecordLocation(9, 10, 11, 0, 12, metadata), &replaced);
  ASSERT_EQ(replaced, expiring);
  undo.Replace(replaced, ordinary);
  RecordIndex::Entry::Destroy(replaced);

  EXPECT_EQ(undo.Current(key_handle), ordinary);
  EXPECT_EQ(undo.Track(ordinary), key_handle);
}

}  // namespace
}  // namespace keylane::storage
