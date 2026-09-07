#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string_view>
#include <vector>

#include "keylane/storage/engine.h"
#include "keylane/storage/format.h"

TEST(StorageStateTest, PromotionBaseEqualityIncludesDurabilityFrontier) {
  using namespace keylane::storage;
  const PromotionBase base{
      .group_id_ = "group-a",
      .parent_history_id_ = "history-a",
      .parent_frontier_ = {.history_context_ = "context-a",
                           .flow_cursors_ = {4, 8}},
      .population_token_ = {.generation_ = 7, .digest_ = 11},
      .catalog_token_ = {.catalog_generation_ = 3, .dump_crc64_ = 5},
      .storage_accumulator_ = "accumulator-a",
  };
  PromotionBase equal = base;
  EXPECT_EQ(equal, base);

  equal.parent_frontier_.flow_cursors_[1] = 9;
  EXPECT_NE(equal, base);
}

TEST(StorageFormatTest, ComputesStableProcessLocalDigests) {
  using namespace keylane::storage;
  static_assert(sizeof(Digest) == sizeof(std::uint64_t));
  static_assert(kRecordHeaderBaseBytes == 72);
  static_assert(kMaxRecordFixedHeaderBytes == 88);

  const Digest first = ComputeDigest("key");
  EXPECT_TRUE(first == ComputeDigest("key"));
  EXPECT_FALSE(first == ComputeDigest("other-key"));
  EXPECT_EQ(DigestHash{}(first), first.value_);
}

TEST(StorageFormatTest, RestoresCheckpointDigestSeedBeforeHashing) {
  using namespace keylane::storage;
  const DigestSeed original = CurrentDigestSeed();
  DigestSeed checkpoint_seed{};
  checkpoint_seed.fill(0xa5);

  RestoreDigestSeed(checkpoint_seed);
  const Digest restored = ComputeDigest("checkpoint-key");
  EXPECT_EQ(CurrentDigestSeed(), checkpoint_seed);
  EXPECT_EQ(restored, ComputeDigest("checkpoint-key"));

  DigestSeed different_seed{};
  different_seed.fill(0x5a);
  RestoreDigestSeed(different_seed);
  EXPECT_NE(restored, ComputeDigest("checkpoint-key"));
  RestoreDigestSeed(original);
}

TEST(StorageFormatTest, KeepsRuntimeAndRecoveryKeyLimitsIdentical) {
  using namespace keylane::storage;
  EXPECT_TRUE(ValidRecordKeySize(MaxKeyBytes()));
  EXPECT_FALSE(ValidRecordKeySize(MaxKeyBytes() + 1));
}

TEST(StorageFormatTest, ComputesRedisClusterSlotsAndHashTags) {
  using keylane::storage::RedisSlot;

  EXPECT_EQ(RedisSlot("123456789"), 12'739);
  EXPECT_EQ(RedisSlot("foo"), 12'182);
  EXPECT_EQ(RedisSlot("bar"), 5'061);
  EXPECT_EQ(RedisSlot("{user1000}.following"), 3'443);
  EXPECT_EQ(RedisSlot("{user1000}.followers"), 3'443);

  // Redis hashes only the first non-empty {...} tag; malformed or empty tags
  // deliberately fall back to hashing the complete binary-safe key.
  EXPECT_EQ(RedisSlot("foo{bar}{zap}"), RedisSlot("bar"));
  EXPECT_EQ(RedisSlot("foo{{bar}}zap"), RedisSlot("{bar"));
  EXPECT_EQ(RedisSlot("foo{}{bar}"), RedisSlot("foo{}{bar}"));
  EXPECT_EQ(RedisSlot("foo{bar"), RedisSlot("foo{bar"));

  const std::string_view binary_tag("a{b\0c}d", 7);
  const std::string_view binary_value("b\0c", 3);
  EXPECT_EQ(RedisSlot(binary_tag), RedisSlot(binary_value));
}

TEST(StorageFormatTest, EncodesAndValidatesPersistentMetadata) {
  using namespace keylane::storage;
  static_assert(kStorageFormatVersion == 1);

  constexpr std::uint64_t device_id = kDeviceIdLimit - 2;
  constexpr std::uint32_t local_block =
      static_cast<std::uint32_t>(kLocalBlockIdLimit - 1);
  constexpr std::uint64_t block_id = MakeBlockId(device_id, local_block);
  static_assert(DeviceIdForBlock(block_id) == device_id);
  static_assert(LocalBlockId(block_id) == local_block);
  static_assert(LocalBlockOffset(block_id) ==
                static_cast<std::uint64_t>(local_block) * kStorageBlockBytes);

  DeviceLabel label{
      .magic_ = kDeviceLabelMagic,
      .version_ = kStorageFormatVersion,
      .header_bytes_ = kDirectIoAlignment,
      .storage_set_id_ = 17,
      .device_id_ = device_id,
      .capacity_blocks_ = kLocalBlockIdLimit,
      .device_count_ = 7,
      .block_bytes_ = kStorageBlockBytes,
  };
  std::array<std::byte, kDirectIoAlignment> label_page{};
  EncodeDeviceLabel(label, label_page);
  DeviceLabel decoded_label{};
  ASSERT_TRUE(DecodeDeviceLabel(label_page, &decoded_label));
  ASSERT_TRUE(decoded_label.storage_set_id_ == label.storage_set_id_);
  ASSERT_TRUE(decoded_label.device_id_ == label.device_id_);
  ASSERT_TRUE(decoded_label.capacity_blocks_ == label.capacity_blocks_);
  ASSERT_TRUE(decoded_label.device_count_ == label.device_count_);
  label_page.back() ^= std::byte{1};
  ASSERT_TRUE(!DecodeDeviceLabel(label_page, &decoded_label));

  std::array<std::byte, 37> metadata_payload{};
  for (std::size_t i = 0; i < metadata_payload.size(); ++i) {
    metadata_payload[i] = static_cast<std::byte>(i + 1);
  }
  std::array<std::byte, kDirectIoAlignment> metadata_page{};
  EncodeMetadataPage(MetadataPageKind::kEpochs, 11, 23, metadata_payload,
                     metadata_page);
  std::array<std::byte, 64> decoded_payload{};
  std::uint64_t metadata_generation = 0;
  ASSERT_TRUE(DecodeMetadataPage(metadata_page, MetadataPageKind::kEpochs, 11,
                                 &metadata_generation, decoded_payload));
  ASSERT_TRUE(metadata_generation == 23);
  ASSERT_TRUE(std::memcmp(metadata_payload.data(), decoded_payload.data(),
                          metadata_payload.size()) == 0);
  for (std::size_t i = metadata_payload.size(); i < decoded_payload.size();
       ++i) {
    ASSERT_TRUE(decoded_payload[i] == std::byte{0});
  }
  ASSERT_TRUE(!DecodeMetadataPage(metadata_page, MetadataPageKind::kScanBitmap,
                                  11, &metadata_generation, decoded_payload));
  EncodeMetadataPage(MetadataPageKind::kCheckpointBitmap, 7, 29,
                     metadata_payload, metadata_page);
  ASSERT_TRUE(DecodeMetadataPage(metadata_page,
                                 MetadataPageKind::kCheckpointBitmap, 7,
                                 &metadata_generation, decoded_payload));
  ASSERT_EQ(metadata_generation, 29);
  EncodeMetadataPage(MetadataPageKind::kEpochs, 11, 23, metadata_payload,
                     metadata_page);
  ASSERT_TRUE(!DecodeMetadataPage(metadata_page, MetadataPageKind::kEpochs, 12,
                                  &metadata_generation, decoded_payload));
  metadata_page.back() ^= std::byte{1};
  ASSERT_TRUE(!DecodeMetadataPage(metadata_page, MetadataPageKind::kEpochs, 11,
                                  &metadata_generation, decoded_payload));

  SystemStateRoot system_root{
      .magic_ = kSystemStateRootMagic,
      .version_ = kStorageFormatVersion,
      .root_bytes_ = sizeof(SystemStateRoot),
      .generation_ = 41,
      .manifest_ =
          ExtentRef{
              .block_id_ = block_id,
              .allocation_epoch_ = 19,
              .payload_bytes_ = 123,
              .payload_checksum_ = 0x12345678,
          },
      .manifest_bytes_ = 123,
  };
  std::array<std::byte, sizeof(SystemStateRoot)> system_root_bytes{};
  EncodeSystemStateRoot(system_root, system_root_bytes);
  SystemStateRoot decoded_system_root;
  ASSERT_TRUE(DecodeSystemStateRoot(system_root_bytes, &decoded_system_root));
  EXPECT_EQ(decoded_system_root, system_root);
  system_root_bytes[8] = std::byte{2};  // Unknown versions are rejected.
  EXPECT_FALSE(DecodeSystemStateRoot(system_root_bytes, &decoded_system_root));

  constexpr std::uint64_t one_pib_blocks = std::uint64_t{1} << 27;
  static_assert(ScanBitmapBytes(one_pib_blocks) == 16 * 1024 * 1024);
  static_assert(kSystemStateMetadataOffset ==
                kEpochMetadataOffset +
                    kEpochMetadataPageCount * 2 * kDirectIoAlignment);
  static_assert(kScanBitmapMetadataOffset ==
                kSystemStateMetadataOffset + 2 * kDirectIoAlignment);
  static_assert(CheckpointBitmapMetadataOffset(one_pib_blocks) >
                kScanBitmapMetadataOffset);
  static_assert(DataBlockBegin(one_pib_blocks) >= 3);
  static_assert(FixedMetadataBytes(one_pib_blocks) <=
                static_cast<std::uint64_t>(DataBlockBegin(one_pib_blocks)) *
                    kStorageBlockBytes);
  static_assert(MetadataPageSlotOffset(kEpochMetadataOffset, 1, 0) +
                    kDirectIoAlignment ==
                MetadataPageSlotOffset(kEpochMetadataOffset, 1, 1));

  BlockHeader header{
      .magic_ = kBlockMagic,
      .block_id_ = block_id,
      .version_ = kStorageFormatVersion,
      .header_bytes_ = kBlockHeaderBytes,
      .block_bytes_ = kStorageBlockBytes,
      .writer_id_ = 3,
      .allocation_epoch_ = 9,
      .committed_bytes_ = kBlockHeaderBytes,
      .record_count_ = 0,
      .max_lsn_ = 0,
      .header_sequence_ = 1,
      .checksum_ = 0,
      .layout_worker_count_ = 4,
  };
  std::array<std::byte, kBlockHeaderSlotBytes> block_page{};
  EncodeBlockHeader(header, block_page);
  BlockHeader decoded_header{};
  ASSERT_TRUE(DecodeBlockHeader(block_page, &decoded_header));
  ASSERT_TRUE(decoded_header.block_id_ == block_id);
  ASSERT_TRUE(decoded_header.allocation_epoch_ == header.allocation_epoch_);

  // Double-slot resolution: the valid slot with the larger
  // (allocation_epoch, header_sequence) wins; torn/zero slots are skipped.
  {
    std::array<std::byte, kBlockHeaderBytes> pages{};
    BlockHeader winner{};
    std::uint8_t active_slot = 9;
    ASSERT_TRUE(!DecodeBlockHeaderPages(pages, &winner, &active_slot));
    std::memcpy(pages.data(), block_page.data(), block_page.size());
    ASSERT_TRUE(DecodeBlockHeaderPages(pages, &winner, &active_slot));
    ASSERT_TRUE(active_slot == 0 &&
                winner.committed_bytes_ == kBlockHeaderBytes);
    BlockHeader newer = header;
    newer.committed_bytes_ = kBlockHeaderBytes + 4096;
    newer.header_sequence_ = 2;
    std::array<std::byte, kBlockHeaderSlotBytes> newer_page{};
    EncodeBlockHeader(newer, newer_page);
    std::memcpy(pages.data() + kBlockHeaderSlotBytes, newer_page.data(),
                newer_page.size());
    ASSERT_TRUE(DecodeBlockHeaderPages(pages, &winner, &active_slot));
    ASSERT_TRUE(active_slot == 1 &&
                winner.committed_bytes_ == kBlockHeaderBytes + 4096);
    pages[kBlockHeaderSlotBytes + 8] ^= std::byte{0xff};  // tear slot 1
    ASSERT_TRUE(DecodeBlockHeaderPages(pages, &winner, &active_slot));
    ASSERT_TRUE(active_slot == 0 &&
                winner.committed_bytes_ == kBlockHeaderBytes);

    // A flush that adds no records still restamps the header, so equal
    // committed_bytes must be broken by the sequence, not by slot order.
    BlockHeader restamped = header;
    restamped.header_sequence_ = 2;
    restamped.max_lsn_ = 77;
    EncodeBlockHeader(restamped, newer_page);
    std::memcpy(pages.data() + kBlockHeaderSlotBytes, newer_page.data(),
                newer_page.size());
    ASSERT_TRUE(DecodeBlockHeaderPages(pages, &winner, &active_slot));
    ASSERT_TRUE(active_slot == 1 && winner.max_lsn_ == 77);
  }

  BlockHeader extent_header = header;
  extent_header.kind_ = BlockKind::kPayloadExtent;
  extent_header.committed_bytes_ = kBlockHeaderBytes + 1234;
  extent_header.extent_index_ = 2;
  extent_header.extent_payload_bytes_ = 1234;
  extent_header.extent_payload_checksum_ = 0x12345678U;
  EncodeBlockHeader(extent_header, block_page);
  ASSERT_TRUE(DecodeBlockHeader(block_page, &decoded_header));
  ASSERT_TRUE(decoded_header.kind_ == BlockKind::kPayloadExtent);
  ASSERT_TRUE(decoded_header.extent_index_ == extent_header.extent_index_);
  ASSERT_TRUE(decoded_header.extent_payload_bytes_ ==
              extent_header.extent_payload_bytes_);
  ASSERT_TRUE(decoded_header.extent_payload_checksum_ ==
              extent_header.extent_payload_checksum_);

  BlockHeader transaction_header = header;
  transaction_header.kind_ = BlockKind::kTransaction;
  transaction_header.tx_generation_ = 17;
  transaction_header.record_count_ = 3;
  transaction_header.committed_bytes_ = kBlockHeaderBytes + 3 * 120;
  EncodeBlockHeader(transaction_header, block_page);
  ASSERT_TRUE(DecodeBlockHeader(block_page, &decoded_header));
  EXPECT_EQ(decoded_header.kind_, BlockKind::kTransaction);
  EXPECT_EQ(decoded_header.tx_generation_, 17);

  transaction_header.tx_generation_ = 0;
  EncodeBlockHeader(transaction_header, block_page);
  EXPECT_FALSE(DecodeBlockHeader(block_page, &decoded_header));

  BlockHeader checkpoint_header = header;
  checkpoint_header.kind_ = BlockKind::kCheckpointIndex;
  checkpoint_header.tx_generation_ = 23;
  checkpoint_header.extent_index_ = 2;
  checkpoint_header.extent_payload_bytes_ = 4096;
  checkpoint_header.extent_payload_checksum_ = 0x87654321U;
  checkpoint_header.record_count_ = 11;
  checkpoint_header.committed_bytes_ = kBlockHeaderBytes + 4096;
  EncodeBlockHeader(checkpoint_header, block_page);
  ASSERT_TRUE(DecodeBlockHeader(block_page, &decoded_header));
  EXPECT_EQ(decoded_header.kind_, BlockKind::kCheckpointIndex);
  EXPECT_EQ(decoded_header.tx_generation_, 23);
  BlockHeader records_with_generation = header;
  records_with_generation.tx_generation_ = 17;
  EncodeBlockHeader(records_with_generation, block_page);
  EXPECT_FALSE(DecodeBlockHeader(block_page, &decoded_header));

  constexpr std::string_view key = "typed-expiring-key";
  constexpr std::string_view value = "value";
  const std::size_t record_header_bytes =
      RecordHeaderBytes(key.size(), false, true, true);
  RecordHeader record{
      .header_bytes_ = static_cast<std::uint16_t>(record_header_bytes),
      .kind_ = RecordKind::kValue,
      .db_id_ = 3,
      .value_type_ = ValueType::kString,
      .external_ = false,
      .key_bytes_ = static_cast<std::uint32_t>(key.size()),
      .logical_size_ = value.size(),
      .payload_bytes_ = static_cast<std::uint32_t>(value.size()),
      .total_disk_bytes_ = static_cast<std::uint32_t>(
          AlignRecord(record_header_bytes + value.size())),
      .txid_ = 4,
      .replication_epoch_ = 5,
      .db_epoch_ = 6,
      .mutation_sequence_ = 7,
      .expire_at_ms_ = 1'900'000'000'123ULL,
      .lsn_ = 9,
      .allocation_epoch_ = 10,
      .payload_checksum_ = Crc32c(std::span<const std::byte>(
          reinterpret_cast<const std::byte*>(value.data()), value.size())),
  };
  std::array<std::byte, kMaxRecordHeaderBytes> record_page{};
  std::fill(record_page.begin(), record_page.end(), std::byte{0xa5});
  ASSERT_TRUE(EncodeRecordHeader(
      record, key,
      std::span<std::byte>(record_page.data(), record_header_bytes)));
  EXPECT_TRUE(
      std::all_of(record_page.begin() + sizeof(RecordHeader) + key.size(),
                  record_page.begin() + record_header_bytes,
                  [](std::byte byte) { return byte == std::byte{0}; }));
  RecordHeader decoded_record{};
  std::string_view decoded_key;
  ASSERT_TRUE(DecodeRecordHeader(
      std::span<const std::byte>(record_page.data(), record_header_bytes),
      &decoded_record, &decoded_key));
  ASSERT_TRUE(decoded_key == key);
  ASSERT_TRUE(decoded_record.value_type_ == ValueType::kString);
  ASSERT_TRUE(!decoded_record.external_);
  ASSERT_TRUE(decoded_record.expire_at_ms_ == record.expire_at_ms_);
  record_page[record_header_bytes - 1] ^= std::byte{1};
  ASSERT_TRUE(!DecodeRecordHeader(
      std::span<const std::byte>(record_page.data(), record_header_bytes),
      &decoded_record, &decoded_key));

  RecordHeader external_record = record;
  external_record.external_ = true;
  external_record.logical_size_ = 9ULL * 1024 * 1024;
  external_record.payload_bytes_ =
      sizeof(ExtentManifestHeader) + 2 * sizeof(ExtentRef);
  external_record.total_disk_bytes_ = static_cast<std::uint32_t>(
      AlignRecord(record_header_bytes + external_record.payload_bytes_));
  std::fill(record_page.begin(), record_page.end(), std::byte{0});
  ASSERT_TRUE(EncodeRecordHeader(
      external_record, key,
      std::span<std::byte>(record_page.data(), record_header_bytes)));
  ASSERT_TRUE(DecodeRecordHeader(
      std::span<const std::byte>(record_page.data(), record_header_bytes),
      &decoded_record, &decoded_key));
  ASSERT_TRUE(decoded_record.external_);
  ASSERT_TRUE(decoded_record.value_type_ == ValueType::kString);
  ASSERT_TRUE(decoded_record.logical_size_ == external_record.logical_size_);
  ASSERT_TRUE(decoded_record.payload_bytes_ == external_record.payload_bytes_);

  external_record.logical_size_ = kMaxBitmapBytes;
  ASSERT_TRUE(EncodeRecordHeader(
      external_record, key,
      std::span<std::byte>(record_page.data(), record_header_bytes)));
  ASSERT_TRUE(DecodeRecordHeader(
      std::span<const std::byte>(record_page.data(), record_header_bytes),
      &decoded_record, &decoded_key));
  external_record.logical_size_ = kMaxBitmapBytes + 1;
  ASSERT_TRUE(EncodeRecordHeader(
      external_record, key,
      std::span<std::byte>(record_page.data(), record_header_bytes)));
  ASSERT_FALSE(DecodeRecordHeader(
      std::span<const std::byte>(record_page.data(), record_header_bytes),
      &decoded_record, &decoded_key));
}

TEST(StorageFormatTest, EncodesOutOfIndexKeyWithoutHeaderBytes) {
  using namespace keylane::storage;

  const std::string key(8192, 'k');
  const std::size_t header_bytes = RecordHeaderBytes(key.size(), true);
  ASSERT_LE(header_bytes, kMaxRecordHeaderBytes);
  RecordHeader record{
      .header_bytes_ = static_cast<std::uint16_t>(header_bytes),
      .kind_ = RecordKind::kTombstone,
      .db_id_ = 2,
      .value_type_ = ValueType::kNone,
      .external_ = false,
      .key_external_ = true,
      .key_bytes_ = static_cast<std::uint32_t>(key.size()),
      .logical_size_ = 0,
      .payload_bytes_ = static_cast<std::uint32_t>(key.size()),
      .total_disk_bytes_ =
          static_cast<std::uint32_t>(AlignRecord(header_bytes + key.size())),
      .replication_epoch_ = 3,
      .db_epoch_ = 4,
      .mutation_sequence_ = 5,
      .allocation_epoch_ = 6,
  };
  std::array<std::byte, kMaxRecordHeaderBytes> page{};
  ASSERT_TRUE(EncodeRecordHeader(
      record, key, std::span<std::byte>(page.data(), header_bytes)));

  RecordHeader decoded{};
  std::string_view decoded_key;
  ASSERT_TRUE(
      DecodeRecordHeader(std::span<const std::byte>(page.data(), header_bytes),
                         &decoded, &decoded_key));
  EXPECT_TRUE(decoded.key_external_);
  EXPECT_TRUE(decoded_key.empty());
  EXPECT_EQ(decoded.key_bytes_, key.size());
  EXPECT_EQ(decoded.header_bytes_, kRecordHeaderBaseBytes);
  EXPECT_EQ(decoded.payload_bytes_, key.size());
}

TEST(StorageFormatTest, UsesSparseRecordHeaderExtensions) {
  using namespace keylane::storage;

  EXPECT_EQ(RecordFixedHeaderBytes(false, false), 72);
  EXPECT_EQ(RecordFixedHeaderBytes(true, false), 80);
  EXPECT_EQ(RecordFixedHeaderBytes(false, true), 80);
  EXPECT_EQ(RecordFixedHeaderBytes(true, true), 88);

  for (const std::uint64_t txid : {std::uint64_t{0}, std::uint64_t{17}}) {
    for (const std::uint64_t expiry :
         {std::uint64_t{0}, std::uint64_t{1'900'000'000'123ULL}}) {
      const std::size_t header_bytes =
          RecordHeaderBytes(0, false, txid != 0, expiry != 0);
      RecordHeader record{
          .header_bytes_ = static_cast<std::uint16_t>(header_bytes),
          .kind_ = RecordKind::kValue,
          .db_id_ = 3,
          .value_type_ = ValueType::kString,
          .key_bytes_ = 0,
          .logical_size_ = 1,
          .payload_bytes_ = 1,
          .total_disk_bytes_ =
              static_cast<std::uint32_t>(AlignRecord(header_bytes + 1)),
          .txid_ = txid,
          .replication_epoch_ = 5,
          .db_epoch_ = 6,
          .mutation_sequence_ = 7,
          .expire_at_ms_ = expiry,
          .lsn_ = 9,
          .allocation_epoch_ = 10,
      };
      std::array<std::byte, kMaxRecordFixedHeaderBytes> encoded{};
      ASSERT_TRUE(EncodeRecordHeader(
          record, {}, std::span<std::byte>(encoded.data(), header_bytes)));

      RecordHeader decoded{};
      std::string_view key;
      ASSERT_TRUE(DecodeRecordHeader(
          std::span<const std::byte>(encoded.data(), header_bytes), &decoded,
          &key));
      EXPECT_EQ(decoded.header_bytes_, header_bytes);
      EXPECT_EQ(decoded.txid_, txid);
      EXPECT_EQ(decoded.expire_at_ms_, expiry);
      EXPECT_TRUE(key.empty());
    }
  }
}

TEST(StorageFormatTest, EncodesMemoryReplicationFrames) {
  using namespace keylane::storage;

  constexpr std::string_view payload = "replication-payload";
  ReplicationFrameHeader frame{
      .header_bytes_ = sizeof(ReplicationFrameHeader),
      .kind_ = ReplicationEventKind::kMutation,
      .flags_ = ReplicationFrameFlag::kFirst | ReplicationFrameFlag::kLast,
      .lsn_ = 41,
      .partition_sequence_ = 99,
      .payload_bytes_ = static_cast<std::uint32_t>(payload.size()),
      .total_disk_bytes_ = static_cast<std::uint32_t>(
          AlignRecord(sizeof(ReplicationFrameHeader) + payload.size())),
      .fragment_index_ = 0,
      .partition_id_ = 1234,
      .payload_checksum_ = Crc32c(std::span<const std::byte>(
          reinterpret_cast<const std::byte*>(payload.data()), payload.size())),
  };
  std::array<std::byte, sizeof(ReplicationFrameHeader)> frame_bytes{};
  ASSERT_TRUE(EncodeReplicationFrameHeader(frame, frame_bytes));
  ReplicationFrameHeader decoded_frame{};
  ASSERT_TRUE(DecodeReplicationFrameHeader(frame_bytes, &decoded_frame));
  EXPECT_EQ(decoded_frame.lsn_, frame.lsn_);
  EXPECT_EQ(decoded_frame.partition_sequence_, frame.partition_sequence_);
  EXPECT_EQ(decoded_frame.partition_id_, frame.partition_id_);
  EXPECT_EQ(decoded_frame.payload_checksum_, frame.payload_checksum_);

  frame.kind_ = ReplicationEventKind::kEphemeral;
  ASSERT_TRUE(EncodeReplicationFrameHeader(frame, frame_bytes));
  ASSERT_TRUE(DecodeReplicationFrameHeader(frame_bytes, &decoded_frame));
  EXPECT_EQ(decoded_frame.kind_, ReplicationEventKind::kEphemeral);

  // Middle fragments deliberately carry neither boundary flag.
  frame.kind_ = ReplicationEventKind::kMutation;
  frame.flags_ = 0;
  frame.fragment_index_ = 1;
  ASSERT_TRUE(EncodeReplicationFrameHeader(frame, frame_bytes));
  ASSERT_TRUE(DecodeReplicationFrameHeader(frame_bytes, &decoded_frame));
  frame_bytes[7] ^= std::byte{1};
  EXPECT_FALSE(DecodeReplicationFrameHeader(frame_bytes, &decoded_frame));
}

TEST(StorageFormatTest, RejectsOversizedInlineHeaderBeforeChecksumCopy) {
  using namespace keylane::storage;

  constexpr std::uint32_t key_bytes = 60'000;
  const std::size_t header_bytes = RecordHeaderBytes(key_bytes, false);
  ASSERT_GT(header_bytes, kMaxRecordHeaderBytes);
  ASSERT_LE(header_bytes, std::numeric_limits<std::uint16_t>::max());
  std::vector<std::byte> input(header_bytes);
  // The compact layout starts with the 64-bit magic followed by key_bytes. Only
  // fields are needed to prove the decoder rejects the derived oversized
  // header before copying into its bounded checksum buffer.
  const std::uint64_t magic = kRecordMagic;
  std::memcpy(input.data(), &magic, sizeof(magic));
  std::memcpy(input.data() + sizeof(magic), &key_bytes, sizeof(key_bytes));

  RecordHeader decoded{};
  std::string_view key;
  EXPECT_FALSE(DecodeRecordHeader(input, &decoded, &key));
}
