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
static_assert(RecordLocation::PackedMetadata::kReservedBits == 5);
static_assert(sizeof(RecordLocation) == 40);
static_assert(sizeof(RecordIndex::Entry) == 48);

}  // namespace
}  // namespace keylane::storage
