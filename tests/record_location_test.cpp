#include <atomic>
#include <limits>
#include <thread>

#include "../src/storage/engine/impl.h"
#include "gtest/gtest.h"

namespace keylane::storage {
namespace {

TEST(RecoveryMemoryTest, DividesTemporaryBatchTargetAcrossWorkers) {
  EXPECT_EQ(RecoveryWorkerBatchTargetBytes(0),
            kRecoveryProcessBatchTargetBytes);
  EXPECT_EQ(RecoveryWorkerBatchTargetBytes(1),
            kRecoveryProcessBatchTargetBytes);
  EXPECT_EQ(RecoveryWorkerBatchTargetBytes(8),
            kRecoveryProcessBatchTargetBytes / 8);
  EXPECT_EQ(RecoveryWorkerBatchTargetBytes(kMaxMemoryWorkers),
            kRecoveryProcessBatchTargetBytes / kMaxMemoryWorkers);
}

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
  EXPECT_FALSE(location.grouped());
}

TEST(RecordLocationTest, GroupedRepresentationSurvivesIndexAndExpiryChanges) {
  RecordIndex index;
  const auto digest = ComputeDigest("grouped-hash");
  for (bool external : {false, true}) {
    for (bool key_external : {false, true}) {
      for (std::uint64_t expiry : {0ULL, 123456789ULL, 0ULL}) {
        const RecordLocation location(
            17, 91, 83, expiry, 1200,
            RecordLocation::PackedMetadata::Encode(
                kBlockHeaderBytes, 256, 7, true, external, key_external, true,
                false, true, RecordKind::kValue, ValueType::kHash, false,
                true));
        auto* entry = index.Find(digest, "grouped-hash");
        if (entry == nullptr) {
          entry = index.InsertNew(digest, "grouped-hash", location);
        } else {
          RecordIndex::Entry* detached = nullptr;
          entry = index.ReplaceValue(entry, location, digest, &detached);
          if (detached != nullptr) index.DestroyDetached(detached);
        }
        ASSERT_NE(entry, nullptr);
        EXPECT_TRUE(entry->value_.grouped());
        entry->value_.set_in_memory(false);
        entry->value_.set_tx_tagged(false);
        const auto restored = RecordIndexEntryPolicy::Load(
            entry->value_, entry->optional_extra(), 83, 7);
        EXPECT_TRUE(restored.grouped());
        EXPECT_EQ(restored.value_type(), ValueType::kHash);
        EXPECT_EQ(restored.external(), external);
        EXPECT_EQ(restored.key_external(), key_external);
        EXPECT_EQ(restored.expire_at_ms_, expiry);
        EXPECT_EQ(restored.mutation_sequence_, 91);
        EXPECT_EQ(restored.block_id(), 17);
        EXPECT_EQ(restored.block_owner(), 7);
        EXPECT_FALSE(restored.in_memory());
        EXPECT_FALSE(restored.tx_tagged());
      }
    }
  }
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
static_assert(RecordLocation::PackedMetadata::kReservedBits == 3);
static_assert(sizeof(RecordLocationCore) == 32);
static_assert(sizeof(RecordLocation) == 40);
static_assert(sizeof(RecordIndexValue) == 24);
static_assert(RecordIndexValue::kReservedBits == 4);
static_assert(sizeof(RecordIndex::Entry) == 24);
static_assert(sizeof(RecordIndex::ExtendedEntry) == 32);

RecordLocation MaterializeForTest(const RecordIndex::Entry& entry,
                                  std::uint64_t allocation_epoch,
                                  std::uint16_t block_owner = 0) {
  return RecordIndexEntryPolicy::Load(entry.value_, entry.optional_extra(),
                                      allocation_epoch, block_owner);
}

TEST(RecordLocationTest, CompactIndexValueMaterializesCompleteBlockIdentity) {
  constexpr std::uint64_t expiry = 4'102'444'800'123ULL;
  constexpr std::uint16_t owner = kMaxMemoryWorkers - 1;
  const RecordLocation location(
      RecordLocation::kBlockIdMask, std::numeric_limits<std::uint64_t>::max(),
      RecordLocation::kAllocationEpochMask, expiry,
      static_cast<std::uint32_t>(kMaxBitmapBytes),
      RecordLocation::PackedMetadata::Encode(
          kStorageBlockBytes - kRecordAlignment,
          kStorageBlockBytes - kBlockHeaderBytes, owner, true, true, true, true,
          true, true, RecordKind::kTombstone, ValueType::kNone));

  const RecordIndexValue compact(location);
  const RecordLocation restored = RecordIndexEntryPolicy::Load(
      compact, &expiry, location.allocation_epoch(), owner);

  EXPECT_EQ(restored.block_id(), location.block_id());
  EXPECT_EQ(restored.allocation_epoch(), location.allocation_epoch());
  EXPECT_EQ(restored.block_owner(), owner);
  EXPECT_EQ(restored.mutation_sequence_, location.mutation_sequence_);
  EXPECT_EQ(restored.logical_size_, location.logical_size_);
  EXPECT_EQ(restored.record_offset(), location.record_offset());
  EXPECT_EQ(restored.total_disk_bytes(), location.total_disk_bytes());
  EXPECT_EQ(restored.expire_at_ms_, expiry);
  EXPECT_TRUE(restored.in_memory());
  EXPECT_TRUE(restored.external());
  EXPECT_TRUE(restored.key_external());
  EXPECT_TRUE(restored.shielding());
  EXPECT_TRUE(restored.unclaimed());
  EXPECT_TRUE(restored.tx_tagged());
  EXPECT_TRUE(restored.has_expiry());
  EXPECT_EQ(restored.kind(), RecordKind::kTombstone);
}

TEST(RecordLocationTest, RemoteMaterializationRejectsReusedBlockByEpoch) {
  constexpr std::uint64_t block_id = 37;
  constexpr std::uint64_t old_epoch = 101;
  constexpr std::uint64_t new_epoch = 102;
  constexpr std::uint16_t remote_owner = 3;
  const auto metadata = RecordLocation::PackedMetadata::Encode(
      kBlockHeaderBytes, kRecordAlignment, remote_owner, false, false, false,
      false, false, false, RecordKind::kValue, ValueType::kString);

  RecordIndex old_index;
  RecordIndex::Entry* old_entry = old_index.InsertNew(
      ComputeDigest("old"), "old",
      RecordLocation(block_id, 1, old_epoch, 0, 7, metadata));
  RecordIndex new_index;
  const RecordIndex::Entry* new_entry = new_index.InsertNew(
      ComputeDigest("new"), "new",
      RecordLocation(block_id, 2, new_epoch, 0, 7, metadata));

  BlockState state;
  state.Reset(remote_owner, old_epoch);
  state.allocated_ = true;

  std::atomic<bool> old_materialized{false};
  std::atomic<bool> block_reused{false};
  RecordLocation old_location;
  RecordLocation current_location;
  std::thread remote_reader([&] {
    old_location = MaterializePublishedIndexLocation(*old_entry, state);
    old_materialized.store(true, std::memory_order_release);
    while (!block_reused.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    current_location = MaterializePublishedIndexLocation(*new_entry, state);
  });

  while (!old_materialized.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }
  const bool retired = old_index.Erase(old_entry);
  state.Reset(kUnownedBlock);
  state.Reset(remote_owner, new_epoch);
  state.allocated_ = true;
  block_reused.store(true, std::memory_order_release);
  remote_reader.join();

  ASSERT_TRUE(retired);
  EXPECT_EQ(old_location.block_id(), current_location.block_id());
  EXPECT_EQ(old_location.block_owner(), current_location.block_owner());
  EXPECT_EQ(old_location.record_offset(), current_location.record_offset());
  EXPECT_NE(old_location.allocation_epoch(),
            current_location.allocation_epoch());
  EXPECT_FALSE(old_location.SamePhysicalRecord(current_location));

  // Physical-owner validation observes the same block and owner after reuse;
  // only the self-contained epoch prevents the old snapshot from being
  // accepted as the new allocation.
  const auto is_current_allocation = [&](const RecordLocation& location) {
    const std::uint16_t owner = state.owner_.load(std::memory_order_acquire);
    return owner == location.block_owner() &&
           state.allocation_epoch_ == location.allocation_epoch();
  };
  EXPECT_FALSE(is_current_allocation(old_location));
  EXPECT_TRUE(is_current_allocation(current_location));
}

TEST(RecordLocationTest, RecordIndexAllocatesExpirySubtypeOnlyWhenNeeded) {
  const auto metadata = RecordLocation::PackedMetadata::Encode(
      kBlockHeaderBytes, kRecordAlignment, 0, true, false, false, false, false,
      false, RecordKind::kValue, ValueType::kString);
  const Digest digest = ComputeDigest("key");
  RecordIndex index;

  RecordIndex::Entry* ordinary =
      index.InsertNew(digest, "key", RecordLocation(1, 2, 3, 0, 4, metadata));
  ASSERT_FALSE(ordinary->has_extra());
  EXPECT_EQ(MaterializeForTest(*ordinary, 3).expire_at_ms_, 0);
  EXPECT_EQ(ordinary->key(), "key");

  RecordIndex::Entry* replaced = nullptr;
  RecordIndex::Entry* expiring = index.ReplaceValue(
      ordinary, RecordLocation(5, 6, 7, 1234, 8, metadata), digest, &replaced);
  ASSERT_NE(replaced, nullptr);
  EXPECT_NE(expiring, ordinary);
  ASSERT_TRUE(expiring->has_extra());
  EXPECT_EQ(MaterializeForTest(*expiring, 7).expire_at_ms_, 1234);
  EXPECT_EQ(expiring->key(), "key");
  const std::uint32_t ordinary_hash = RecordIndex::AddressHash(*replaced);
  const std::uintptr_t ordinary_address =
      reinterpret_cast<std::uintptr_t>(replaced);
  index.DestroyDetached(replaced);
  // FLUSH may retain this raw address after a representation change.
  // FindAddress must reject it without touching the freed object.
  EXPECT_EQ(index.FindAddress(ordinary_address, ordinary_hash), nullptr);
  EXPECT_EQ(index.FindAddress(reinterpret_cast<std::uintptr_t>(expiring),
                              RecordIndex::AddressHash(*expiring)),
            expiring);
  EXPECT_TRUE(index.Contains(expiring, RecordIndex::AddressHash(*expiring)));

  replaced = nullptr;
  ordinary = index.ReplaceValue(
      expiring, RecordLocation(9, 10, 11, 0, 12, metadata), digest, &replaced);
  ASSERT_NE(replaced, nullptr);
  EXPECT_NE(ordinary, expiring);
  EXPECT_FALSE(ordinary->has_extra());
  EXPECT_EQ(MaterializeForTest(*ordinary, 11).expire_at_ms_, 0);
  EXPECT_EQ(ordinary->key(), "key");
  const std::uint32_t expiring_hash = RecordIndex::AddressHash(*replaced);
  const std::uintptr_t expiring_address =
      reinterpret_cast<std::uintptr_t>(replaced);
  index.DestroyDetached(replaced);
  EXPECT_EQ(index.FindAddress(expiring_address, expiring_hash), nullptr);
  const RecordIndex& const_index = index;
  EXPECT_EQ(const_index.FindAddress(reinterpret_cast<std::uintptr_t>(ordinary),
                                    RecordIndex::AddressHash(*ordinary)),
            ordinary);
  EXPECT_TRUE(index.Contains(ordinary, RecordIndex::AddressHash(*ordinary)));
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
  const std::optional<std::uint32_t> key_handle = undo.Track(expiring);
  ASSERT_TRUE(key_handle.has_value());
  EXPECT_EQ(undo.Track(expiring), key_handle);
  EXPECT_NE(undo.Track(other), key_handle);

  RecordIndex::Entry* replaced = nullptr;
  RecordIndex::Entry* ordinary =
      index.ReplaceValue(expiring, RecordLocation(9, 10, 11, 0, 12, metadata),
                         ComputeDigest("key"), &replaced);
  ASSERT_EQ(replaced, expiring);
  undo.Replace(replaced, ordinary);
  index.DestroyDetached(replaced);

  EXPECT_EQ(undo.Current(*key_handle), ordinary);
  EXPECT_EQ(undo.Track(ordinary), key_handle);
}

TEST(RecordLocationTest, TxUndoPrefixRetargetDoesNotRequireAddressCache) {
  const auto metadata = RecordLocation::PackedMetadata::Encode(
      kBlockHeaderBytes, kRecordAlignment, 0, true, false, false, false, false,
      true, RecordKind::kValue, ValueType::kString);
  RecordIndex index;
  auto* first = index.InsertNew(ComputeDigest("key"), "key",
                                RecordLocation(1, 2, 3, 1234, 4, metadata));
  auto* other = index.InsertNew(ComputeDigest("other"), "other",
                                RecordLocation(5, 6, 7, 0, 8, metadata));
  TxUndoLog undo;
  const auto handle = undo.Track(first);
  const auto other_handle = undo.Track(other);
  ASSERT_TRUE(handle);
  ASSERT_TRUE(other_handle);
  undo.ReserveOneEntry();
  RecordIndex::Entry* detached = nullptr;
  auto* second =
      index.ReplaceValue(first, RecordLocation(9, 10, 11, 0, 12, metadata),
                         ComputeDigest("key"), &detached);
  ASSERT_EQ(detached, first);
  undo.NoAllocReplace(first, second);
  index.DestroyDetached(detached);
  ASSERT_TRUE(undo.CanTrack(second));
  // A compensating root can replace the physical Entry again before the
  // resumed outer command ever rebuilds its reverse-address cache.
  auto* third =
      index.ReplaceValue(second, RecordLocation(13, 14, 15, 5678, 16, metadata),
                         ComputeDigest("key"), &detached);
  ASSERT_EQ(detached, second);
  undo.Replace(second, third);
  index.DestroyDetached(detached);
  EXPECT_EQ(undo.Current(*handle), third);
  EXPECT_EQ(undo.Track(third), handle);
  EXPECT_EQ(undo.Current(*other_handle), other);
  EXPECT_EQ(undo.Track(other), other_handle);
}

TEST(RecordLocationTest, IndexKeyMatchingPreservesTailAndCollisionSemantics) {
  const auto packed = RecordLocation::PackedMetadata::Encode(
      kBlockHeaderBytes, kRecordAlignment, 0, true, false, false, false, false,
      false, RecordKind::kValue, ValueType::kString);
  for (const bool expiring : {false, true}) {
    for (const bool complete : {false, true}) {
      for (const std::size_t length : {0, 1, 63, 64, 8191, 8192}) {
        RecordIndex index;
        const std::string key(length, '\0');
        const Digest digest = ComputeDigest(key);
        auto* entry = index.InsertNew(
            digest, key,
            RecordLocation(1, 2, 3, expiring ? 1234 : 0, 4, packed), complete);
        ASSERT_NE(entry, nullptr);
        EXPECT_EQ(entry->key_complete(), complete);
        EXPECT_EQ(index.Find(digest, key), entry);
        EXPECT_EQ(std::as_const(index).Find(digest, key), entry);
        // Force lookup into the same bucket/tag. Length must still match,
        // and complete keys must distinguish bytes even on a digest collision.
        EXPECT_EQ(index.Find(digest, key + "x"), nullptr);
        if (!key.empty()) {
          std::string collision = key;
          collision.back() = 'x';
          EXPECT_EQ(index.Find(digest, collision), complete ? nullptr : entry);
        }
        // Incomplete entries are candidates verified against disk by storage;
        // their stored digest remains necessary even with matching length/tag.
        if (!complete) {
          Digest other_digest = digest;
          other_digest.value_ ^= 1;
          EXPECT_EQ(index.Find(other_digest, key), nullptr);
        }
      }
    }
  }
}

}  // namespace
}  // namespace keylane::storage
