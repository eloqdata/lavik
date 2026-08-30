#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <vector>

#include "keylane/storage/format.h"

TEST(StorageFormatTest, ComputesStableProcessLocalDigests) {
  using namespace keylane::storage;
  static_assert(sizeof(Digest) == sizeof(std::uint64_t));
  static_assert(sizeof(RecordHeader) == 104);

  const Digest first = ComputeDigest("key");
  EXPECT_TRUE(first == ComputeDigest("key"));
  EXPECT_FALSE(first == ComputeDigest("other-key"));
  EXPECT_EQ(DigestHash{}(first), first.value_);
}

TEST(StorageFormatTest, KeepsRuntimeAndRecoveryKeyLimitsIdentical) {
  using namespace keylane::storage;
  EXPECT_TRUE(ValidRecordKeySize(MaxKeyBytes()));
  EXPECT_FALSE(ValidRecordKeySize(MaxKeyBytes() + 1));
}

TEST(StorageFormatTest, ComputesRedisClusterSlots) {
  using keylane::storage::RedisSlot;

  EXPECT_EQ(RedisSlot("123456789"), 12'739);
  EXPECT_EQ(RedisSlot("foo"), 12'182);
  EXPECT_EQ(RedisSlot("{user1000}.following"), 3'443);
  EXPECT_EQ(RedisSlot("{user1000}.followers"), 3'443);
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
  ASSERT_TRUE(!DecodeMetadataPage(metadata_page, MetadataPageKind::kEpochs, 12,
                                  &metadata_generation, decoded_payload));
  metadata_page.back() ^= std::byte{1};
  ASSERT_TRUE(!DecodeMetadataPage(metadata_page, MetadataPageKind::kEpochs, 11,
                                  &metadata_generation, decoded_payload));

  constexpr std::uint64_t one_pib_blocks = std::uint64_t{1} << 27;
  static_assert(ScanBitmapBytes(one_pib_blocks) == 16 * 1024 * 1024);
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
  BlockHeader records_with_generation = header;
  records_with_generation.tx_generation_ = 17;
  EncodeBlockHeader(records_with_generation, block_page);
  EXPECT_FALSE(DecodeBlockHeader(block_page, &decoded_header));

  constexpr std::string_view key = "typed-expiring-key";
  constexpr std::string_view value = "value";
  const std::size_t record_header_bytes = RecordHeaderBytes(key.size());
  RecordHeader record{
      .magic_ = kRecordMagic,
      .version_ = kStorageFormatVersion,
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
  ASSERT_TRUE(EncodeRecordHeader(
      record, key,
      std::span<std::byte>(record_page.data(), record_header_bytes)));
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
      .magic_ = kRecordMagic,
      .version_ = kStorageFormatVersion,
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
  EXPECT_EQ(decoded.header_bytes_, AlignRecord(sizeof(RecordHeader)));
  EXPECT_EQ(decoded.payload_bytes_, key.size());
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
  RecordHeader corrupt{
      .magic_ = kRecordMagic,
      .version_ = kStorageFormatVersion,
      .header_bytes_ = static_cast<std::uint16_t>(header_bytes),
      .kind_ = RecordKind::kValue,
      .db_id_ = 0,
      .value_type_ = ValueType::kString,
      .key_bytes_ = key_bytes,
      .logical_size_ = 1,
      .payload_bytes_ = 0,
      .total_disk_bytes_ =
          static_cast<std::uint32_t>(AlignRecord(header_bytes)),
      .replication_epoch_ = 1,
      .db_epoch_ = 1,
      .mutation_sequence_ = 1,
  };
  std::memcpy(input.data(), &corrupt, sizeof(corrupt));

  RecordHeader decoded{};
  std::string_view key;
  EXPECT_FALSE(DecodeRecordHeader(input, &decoded, &key));
}
