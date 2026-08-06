#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>

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
