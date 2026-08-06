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
  std::array<std::byte, kBlockHeaderBytes> block_page{};
  EncodeBlockHeader(header, block_page);
  BlockHeader decoded_header{};
  assert(DecodeBlockHeader(block_page, &decoded_header));
  assert(decoded_header.block_id == block_id);
  assert(decoded_header.allocation_epoch == header.allocation_epoch);

  return 0;
}
