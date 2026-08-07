#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>

#include "keylane/storage/format.h"

int main() {
  using namespace keylane::storage;

  constexpr std::uint64_t device_id = kDeviceIdLimit - 2;
  constexpr std::uint32_t local_block =
      static_cast<std::uint32_t>(kLocalBlockIdLimit - 1);
  constexpr std::uint64_t block_id = MakeBlockId(device_id, local_block);
  static_assert(DeviceIdForBlock(block_id) == device_id);
  static_assert(LocalBlockId(block_id) == local_block);
  static_assert(LocalBlockOffset(block_id) ==
                static_cast<std::uint64_t>(local_block) * kStorageBlockBytes);

  DeviceLabel label{
      .magic = kDeviceLabelMagic,
      .version = kStorageFormatVersion,
      .header_bytes = kDirectIoAlignment,
      .storage_set_id = 17,
      .device_id = device_id,
      .capacity_blocks = kLocalBlockIdLimit,
      .device_count = 7,
      .block_bytes = kStorageBlockBytes,
  };
  std::array<std::byte, kDirectIoAlignment> label_page{};
  EncodeDeviceLabel(label, label_page);
  DeviceLabel decoded_label{};
  assert(DecodeDeviceLabel(label_page, &decoded_label));
  assert(decoded_label.storage_set_id == label.storage_set_id);
  assert(decoded_label.device_id == label.device_id);
  assert(decoded_label.capacity_blocks == label.capacity_blocks);
  assert(decoded_label.device_count == label.device_count);
  label_page.back() ^= std::byte{1};
  assert(!DecodeDeviceLabel(label_page, &decoded_label));

  std::array<std::byte, 37> metadata_payload{};
  for (std::size_t i = 0; i < metadata_payload.size(); ++i) {
    metadata_payload[i] = static_cast<std::byte>(i + 1);
  }
  std::array<std::byte, kDirectIoAlignment> metadata_page{};
  EncodeMetadataPage(MetadataPageKind::kEpochs, 11, 23,
                     metadata_payload, metadata_page);
  std::array<std::byte, 64> decoded_payload{};
  std::uint64_t metadata_generation = 0;
  assert(DecodeMetadataPage(metadata_page, MetadataPageKind::kEpochs, 11,
                            &metadata_generation, decoded_payload));
  assert(metadata_generation == 23);
  assert(std::memcmp(metadata_payload.data(), decoded_payload.data(),
                     metadata_payload.size()) == 0);
  for (std::size_t i = metadata_payload.size(); i < decoded_payload.size();
       ++i) {
    assert(decoded_payload[i] == std::byte{0});
  }
  assert(!DecodeMetadataPage(metadata_page,
                             MetadataPageKind::kScanBitmap, 11,
                             &metadata_generation, decoded_payload));
  assert(!DecodeMetadataPage(metadata_page, MetadataPageKind::kEpochs, 12,
                             &metadata_generation, decoded_payload));
  metadata_page.back() ^= std::byte{1};
  assert(!DecodeMetadataPage(metadata_page, MetadataPageKind::kEpochs, 11,
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
      .magic = kBlockMagic,
      .block_id = block_id,
      .version = kStorageFormatVersion,
      .header_bytes = kBlockHeaderBytes,
      .block_bytes = kStorageBlockBytes,
      .writer_id = 3,
      .allocation_epoch = 9,
      .committed_bytes = kBlockHeaderBytes,
      .record_count = 0,
      .max_lsn = 0,
      .checksum = 0,
      .layout_worker_count = 4,
  };
  std::array<std::byte, kBlockHeaderSlotBytes> block_page{};
  EncodeBlockHeader(header, block_page);
  BlockHeader decoded_header{};
  assert(DecodeBlockHeader(block_page, &decoded_header));
  assert(decoded_header.block_id == block_id);
  assert(decoded_header.allocation_epoch == header.allocation_epoch);

  // Double-slot resolution: the valid slot with the larger
  // (allocation_epoch, committed_bytes) wins; torn/zero slots are skipped.
  {
    std::array<std::byte, kBlockHeaderBytes> pages{};
    BlockHeader winner{};
    std::uint8_t active_slot = 9;
    assert(!DecodeBlockHeaderPages(pages, &winner, &active_slot));
    std::memcpy(pages.data(), block_page.data(), block_page.size());
    assert(DecodeBlockHeaderPages(pages, &winner, &active_slot));
    assert(active_slot == 0 && winner.committed_bytes == kBlockHeaderBytes);
    BlockHeader newer = header;
    newer.committed_bytes = kBlockHeaderBytes + 4096;
    std::array<std::byte, kBlockHeaderSlotBytes> newer_page{};
    EncodeBlockHeader(newer, newer_page);
    std::memcpy(pages.data() + kBlockHeaderSlotBytes, newer_page.data(),
                newer_page.size());
    assert(DecodeBlockHeaderPages(pages, &winner, &active_slot));
    assert(active_slot == 1 &&
           winner.committed_bytes == kBlockHeaderBytes + 4096);
    pages[kBlockHeaderSlotBytes + 8] ^= std::byte{0xff};  // tear slot 1
    assert(DecodeBlockHeaderPages(pages, &winner, &active_slot));
    assert(active_slot == 0 && winner.committed_bytes == kBlockHeaderBytes);
  }

  BlockHeader extent_header = header;
  extent_header.kind = BlockKind::kValueExtent;
  extent_header.committed_bytes = kBlockHeaderBytes + 1234;
  extent_header.extent_index = 2;
  extent_header.extent_payload_bytes = 1234;
  extent_header.extent_payload_checksum = 0x12345678U;
  EncodeBlockHeader(extent_header, block_page);
  assert(DecodeBlockHeader(block_page, &decoded_header));
  assert(decoded_header.kind == BlockKind::kValueExtent);
  assert(decoded_header.extent_index == extent_header.extent_index);
  assert(decoded_header.extent_payload_bytes ==
         extent_header.extent_payload_bytes);
  assert(decoded_header.extent_payload_checksum ==
         extent_header.extent_payload_checksum);

  constexpr std::string_view key = "typed-expiring-key";
  constexpr std::string_view value = "value";
  const std::size_t record_header_bytes = RecordHeaderBytes(key.size());
  RecordHeader record{
      .magic = kRecordMagic,
      .version = kStorageFormatVersion,
      .header_bytes = static_cast<std::uint16_t>(record_header_bytes),
      .kind = RecordKind::kValue,
      .db_id = 3,
      .value_type = ValueType::kString,
      .external = false,
      .digest = ComputeDigest(key),
      .key_bytes = static_cast<std::uint32_t>(key.size()),
      .logical_size = value.size(),
      .payload_bytes = static_cast<std::uint32_t>(value.size()),
      .total_disk_bytes = static_cast<std::uint32_t>(
          AlignRecord(record_header_bytes + value.size())),
      .generation = 4,
      .replication_epoch = 5,
      .db_epoch = 6,
      .mutation_sequence = 7,
      .relocation_sequence = 8,
      .expire_at_ms = 1'900'000'000'123ULL,
      .lsn = 9,
      .allocation_epoch = 10,
      .payload_checksum = Crc32c(std::span<const std::byte>(
          reinterpret_cast<const std::byte*>(value.data()), value.size())),
  };
  std::array<std::byte, kMaxRecordHeaderBytes> record_page{};
  assert(EncodeRecordHeader(
      record, key,
      std::span<std::byte>(record_page.data(), record_header_bytes)));
  RecordHeader decoded_record{};
  std::string_view decoded_key;
  assert(DecodeRecordHeader(
      std::span<const std::byte>(record_page.data(), record_header_bytes),
      &decoded_record, &decoded_key));
  assert(decoded_key == key);
  assert(decoded_record.value_type == ValueType::kString);
  assert(!decoded_record.external);
  assert(decoded_record.expire_at_ms == record.expire_at_ms);
  record_page[record_header_bytes - 1] ^= std::byte{1};
  assert(!DecodeRecordHeader(
      std::span<const std::byte>(record_page.data(), record_header_bytes),
      &decoded_record, &decoded_key));

  RecordHeader external_record = record;
  external_record.external = true;
  external_record.logical_size = 9ULL * 1024 * 1024;
  external_record.payload_bytes = sizeof(ExtentManifestHeader) +
                                  2 * sizeof(ExtentRef);
  external_record.total_disk_bytes = static_cast<std::uint32_t>(AlignRecord(
      record_header_bytes + external_record.payload_bytes));
  std::fill(record_page.begin(), record_page.end(), std::byte{0});
  assert(EncodeRecordHeader(
      external_record, key,
      std::span<std::byte>(record_page.data(), record_header_bytes)));
  assert(DecodeRecordHeader(
      std::span<const std::byte>(record_page.data(), record_header_bytes),
      &decoded_record, &decoded_key));
  assert(decoded_record.external);
  assert(decoded_record.value_type == ValueType::kString);
  assert(decoded_record.logical_size == external_record.logical_size);
  assert(decoded_record.payload_bytes == external_record.payload_bytes);

  return 0;
}
