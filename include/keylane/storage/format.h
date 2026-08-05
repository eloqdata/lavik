#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

namespace keylane::storage {

inline constexpr std::size_t kDirectIoAlignment = 4096;
inline constexpr std::size_t kBlockHeaderBytes = kDirectIoAlignment;
inline constexpr std::size_t kRecordAlignment = 8;
inline constexpr std::size_t kMaxRecordHeaderBytes = kDirectIoAlignment;
inline constexpr std::size_t kStorageBlockBytes = 8 * 1024 * 1024;
inline constexpr std::uint32_t kStorageFormatVersion = 2;
inline constexpr std::uint64_t kBlockMagic = 0x314b4c424c4f434bULL;   // KCOLBLK1
inline constexpr std::uint64_t kRecordMagic = 0x314b4c5245434f52ULL;  // ROCERLK1
inline constexpr std::uint32_t kLogicalStorageShards = 256;

struct Digest {
  std::array<std::uint8_t, 20> bytes{};

  bool operator==(const Digest&) const noexcept = default;
};

struct DigestHash {
  std::size_t operator()(const Digest& digest) const noexcept;
};

Digest ComputeDigest(std::string_view key) noexcept;
std::uint16_t RedisSlot(std::string_view key) noexcept;
std::uint32_t StorageShardForKey(std::string_view key) noexcept;

enum class RecordKind : std::uint8_t {
  kValue = 1,
  kTombstone = 2,
};

struct BlockHeader {
  std::uint64_t magic = kBlockMagic;
  std::uint32_t version = kStorageFormatVersion;
  std::uint32_t header_bytes = kBlockHeaderBytes;
  std::uint32_t block_bytes = kStorageBlockBytes;
  std::uint32_t storage_shard_id = 0;
  std::uint64_t allocation_epoch = 0;
  std::uint32_t committed_bytes = kBlockHeaderBytes;
  std::uint32_t record_count = 0;
  std::uint64_t max_lsn = 0;
  std::uint32_t checksum = 0;
  std::uint32_t reserved = 0;
};

struct RecordHeader {
  std::uint64_t magic = kRecordMagic;
  std::uint32_t version = kStorageFormatVersion;
  std::uint16_t header_bytes = 0;
  RecordKind kind = RecordKind::kValue;
  std::uint8_t flags = 0;
  Digest digest{};
  std::uint32_t key_bytes = 0;
  std::uint32_t value_bytes = 0;
  std::uint32_t value_disk_bytes = 0;
  std::uint32_t total_disk_bytes = 0;
  std::uint64_t generation = 0;
  std::uint64_t relocation_sequence = 0;
  std::uint64_t lsn = 0;
  std::uint64_t allocation_epoch = 0;
  std::uint32_t payload_checksum = 0;
  std::uint32_t header_checksum = 0;
};

static_assert(sizeof(BlockHeader) <= kBlockHeaderBytes);
static_assert(sizeof(RecordHeader) <= kMaxRecordHeaderBytes);

constexpr std::size_t AlignDirect(std::size_t size) noexcept {
  return (size + kDirectIoAlignment - 1) & ~(kDirectIoAlignment - 1);
}

constexpr std::size_t AlignRecord(std::size_t size) noexcept {
  return (size + kRecordAlignment - 1) & ~(kRecordAlignment - 1);
}

constexpr std::size_t RecordHeaderBytes(std::size_t key_bytes) noexcept {
  return AlignRecord(sizeof(RecordHeader) + key_bytes);
}

constexpr std::size_t MaxKeyBytes() noexcept {
  return kMaxRecordHeaderBytes - sizeof(RecordHeader);
}

std::uint32_t Crc32c(std::span<const std::byte> bytes) noexcept;

void EncodeBlockHeader(const BlockHeader& header,
                       std::span<std::byte, kBlockHeaderBytes> output) noexcept;
bool DecodeBlockHeader(std::span<const std::byte, kBlockHeaderBytes> input,
                       BlockHeader* header) noexcept;

bool EncodeRecordHeader(const RecordHeader& header, std::string_view key,
                        std::span<std::byte> output) noexcept;
bool DecodeRecordHeader(std::span<const std::byte> input,
                        RecordHeader* header, std::string_view* key) noexcept;

}  // namespace keylane::storage
