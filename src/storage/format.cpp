#include "keylane/storage/format.h"

#include <sys/random.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cassert>
#include <cerrno>
#include <cstdlib>
#include <cstring>

#include "absl/crc/crc32c.h"

namespace keylane::storage {
namespace {

std::uint64_t LoadLittleEndian(const std::uint8_t* input) noexcept {
  std::uint64_t value = 0;
  std::memcpy(&value, input, sizeof(value));
  if constexpr (std::endian::native == std::endian::big) {
    value = std::byteswap(value);
  }
  return value;
}

void SipRound(std::uint64_t* v0, std::uint64_t* v1, std::uint64_t* v2,
              std::uint64_t* v3) noexcept {
  *v0 += *v1;
  *v1 = std::rotl(*v1, 13);
  *v1 ^= *v0;
  *v0 = std::rotl(*v0, 32);
  *v2 += *v3;
  *v3 = std::rotl(*v3, 16);
  *v3 ^= *v2;
  *v0 += *v3;
  *v3 = std::rotl(*v3, 21);
  *v3 ^= *v0;
  *v2 += *v1;
  *v1 = std::rotl(*v1, 17);
  *v1 ^= *v2;
  *v2 = std::rotl(*v2, 32);
}

const std::array<std::uint8_t, 16>& DigestSeed() noexcept {
  static const std::array<std::uint8_t, 16> seed = [] {
    std::array<std::uint8_t, 16> generated{};
    std::size_t offset = 0;
    while (offset != generated.size()) {
      const ssize_t bytes =
          ::getrandom(generated.data() + offset, generated.size() - offset, 0);
      if (bytes > 0) {
        offset += static_cast<std::size_t>(bytes);
      } else if (bytes < 0 && errno == EINTR) {
        continue;
      } else {
        // A predictable fallback would make collision flooding possible. The
        // server is Linux-only and cannot safely run without an OS hash seed.
        std::abort();
      }
    }
    return generated;
  }();
  return seed;
}

std::uint64_t SipHash12(std::string_view input,
                        const std::array<std::uint8_t, 16>& seed) noexcept {
  // Keep Valkey's one compression round and two finalization rounds: this is
  // the hash-flooding defense on every command, so changing to SipHash-2-4 is
  // a deliberate security/performance trade rather than a format concern.
  const std::uint64_t k0 = LoadLittleEndian(seed.data());
  const std::uint64_t k1 = LoadLittleEndian(seed.data() + sizeof(k0));
  std::uint64_t v0 = 0x736f6d6570736575ULL ^ k0;
  std::uint64_t v1 = 0x646f72616e646f6dULL ^ k1;
  std::uint64_t v2 = 0x6c7967656e657261ULL ^ k0;
  std::uint64_t v3 = 0x7465646279746573ULL ^ k1;

  const auto* bytes = reinterpret_cast<const std::uint8_t*>(input.data());
  const std::size_t complete_bytes = input.size() & ~std::size_t{7};
  for (std::size_t offset = 0; offset != complete_bytes; offset += 8) {
    const std::uint64_t word = LoadLittleEndian(bytes + offset);
    v3 ^= word;
    SipRound(&v0, &v1, &v2, &v3);
    v0 ^= word;
  }

  std::uint64_t tail = static_cast<std::uint64_t>(input.size()) << 56;
  for (std::size_t index = complete_bytes; index != input.size(); ++index) {
    tail |= static_cast<std::uint64_t>(bytes[index])
            << (8 * (index - complete_bytes));
  }
  v3 ^= tail;
  SipRound(&v0, &v1, &v2, &v3);
  v0 ^= tail;
  v2 ^= 0xff;
  SipRound(&v0, &v1, &v2, &v3);
  SipRound(&v0, &v1, &v2, &v3);
  return v0 ^ v1 ^ v2 ^ v3;
}

constexpr std::array<std::uint16_t, 256> MakeRedisCrc16Table() noexcept {
  std::array<std::uint16_t, 256> table{};
  for (std::size_t byte = 0; byte < table.size(); ++byte) {
    std::uint16_t crc = static_cast<std::uint16_t>(byte << 8);
    for (unsigned bit = 0; bit < 8; ++bit) {
      crc = static_cast<std::uint16_t>(
          (crc & 0x8000U) != 0 ? (crc << 1) ^ 0x1021U : crc << 1);
    }
    table[byte] = crc;
  }
  return table;
}

constexpr auto kRedisCrc16Table = MakeRedisCrc16Table();

std::uint16_t RedisCrc16(std::string_view key) noexcept {
  std::uint16_t crc = 0;
  for (unsigned char byte : key) {
    const auto index = static_cast<std::uint8_t>((crc >> 8) ^ byte);
    crc = static_cast<std::uint16_t>((crc << 8) ^ kRedisCrc16Table[index]);
  }
  return crc;
}

std::string_view HashTag(std::string_view key) noexcept {
  const std::size_t open = key.find('{');
  if (open == std::string_view::npos) {
    return key;
  }
  const std::size_t close = key.find('}', open + 1);
  if (close == std::string_view::npos || close == open + 1) {
    return key;
  }
  return key.substr(open + 1, close - open - 1);
}

}  // namespace

std::size_t DigestHash::operator()(const Digest& digest) const noexcept {
  return static_cast<std::size_t>(digest.value_);
}

Digest ComputeDigest(std::string_view key) noexcept {
  return Digest{.value_ = SipHash12(key, DigestSeed())};
}

std::uint16_t RedisSlot(std::string_view key) noexcept {
  return static_cast<std::uint16_t>(RedisCrc16(HashTag(key)) & 0x3fffU);
}

std::uint32_t StorageShardForKey(std::string_view key) noexcept {
  return RedisSlot(key);
}

std::uint32_t Crc32c(std::span<const std::byte> bytes) noexcept {
  const absl::string_view input(reinterpret_cast<const char*>(bytes.data()),
                                bytes.size());
  return static_cast<std::uint32_t>(absl::ComputeCrc32c(input));
}

void EncodeDeviceLabel(
    const DeviceLabel& label,
    std::span<std::byte, kDirectIoAlignment> output) noexcept {
  std::fill(output.begin(), output.end(), std::byte{0});
  DeviceLabel encoded = label;
  encoded.checksum_ = 0;
  std::memcpy(output.data(), &encoded, sizeof(encoded));
  encoded.checksum_ = Crc32c(output);
  std::memcpy(output.data(), &encoded, sizeof(encoded));
}

bool DecodeDeviceLabel(std::span<const std::byte, kDirectIoAlignment> input,
                       DeviceLabel* label) noexcept {
  if (label == nullptr) {
    return false;
  }
  DeviceLabel decoded{};
  std::memcpy(&decoded, input.data(), sizeof(decoded));
  if (decoded.magic_ != kDeviceLabelMagic ||
      decoded.version_ != kStorageFormatVersion ||
      decoded.header_bytes_ != kDirectIoAlignment ||
      decoded.storage_set_id_ == 0 || decoded.device_id_ >= kDeviceIdLimit ||
      decoded.capacity_blocks_ < 2 ||
      decoded.capacity_blocks_ > kLocalBlockIdLimit ||
      decoded.device_count_ == 0 ||
      decoded.device_count_ > std::numeric_limits<std::uint16_t>::max() ||
      decoded.block_bytes_ != kStorageBlockBytes) {
    return false;
  }
  const std::uint32_t expected = decoded.checksum_;
  std::array<std::byte, kDirectIoAlignment> copy{};
  std::memcpy(copy.data(), input.data(), copy.size());
  decoded.checksum_ = 0;
  std::memcpy(copy.data(), &decoded, sizeof(decoded));
  if (Crc32c(copy) != expected) {
    return false;
  }
  *label = decoded;
  return true;
}

void EncodeMetadataPage(
    MetadataPageKind kind, std::uint32_t page_index, std::uint64_t generation,
    std::span<const std::byte> payload,
    std::span<std::byte, kDirectIoAlignment> output) noexcept {
  assert(payload.size() <= kMetadataPagePayloadBytes);
  std::fill(output.begin(), output.end(), std::byte{0});
  MetadataPageHeader header{
      .magic_ = kMetadataPageMagic,
      .version_ = kStorageFormatVersion,
      .kind_ = kind,
      .header_bytes_ = sizeof(MetadataPageHeader),
      .page_index_ = page_index,
      .payload_bytes_ = static_cast<std::uint32_t>(payload.size()),
      .generation_ = generation,
      .checksum_ = 0,
  };
  std::memcpy(output.data(), &header, sizeof(header));
  std::memcpy(output.data() + sizeof(header), payload.data(), payload.size());
  header.checksum_ = Crc32c(output);
  std::memcpy(output.data(), &header, sizeof(header));
}

bool DecodeMetadataPage(std::span<const std::byte, kDirectIoAlignment> input,
                        MetadataPageKind expected_kind,
                        std::uint32_t expected_page_index,
                        std::uint64_t* generation,
                        std::span<std::byte> payload) noexcept {
  if (generation == nullptr) {
    return false;
  }
  MetadataPageHeader header{};
  std::memcpy(&header, input.data(), sizeof(header));
  if (header.magic_ != kMetadataPageMagic ||
      header.version_ != kStorageFormatVersion ||
      header.kind_ != expected_kind ||
      header.header_bytes_ != sizeof(MetadataPageHeader) ||
      header.page_index_ != expected_page_index || header.generation_ == 0 ||
      header.payload_bytes_ > kMetadataPagePayloadBytes ||
      header.payload_bytes_ > payload.size()) {
    return false;
  }
  const std::uint32_t expected_checksum = header.checksum_;
  std::array<std::byte, kDirectIoAlignment> copy{};
  std::memcpy(copy.data(), input.data(), copy.size());
  header.checksum_ = 0;
  std::memcpy(copy.data(), &header, sizeof(header));
  if (Crc32c(copy) != expected_checksum) {
    return false;
  }
  std::fill(payload.begin(), payload.end(), std::byte{0});
  std::memcpy(payload.data(), input.data() + sizeof(MetadataPageHeader),
              header.payload_bytes_);
  *generation = header.generation_;
  return true;
}

void EncodeBlockHeader(
    const BlockHeader& header,
    std::span<std::byte, kBlockHeaderSlotBytes> output) noexcept {
  std::fill(output.begin(), output.end(), std::byte{0});
  BlockHeader encoded = header;
  encoded.checksum_ = 0;
  std::memcpy(output.data(), &encoded, sizeof(encoded));
  encoded.checksum_ = Crc32c(output);
  std::memcpy(output.data(), &encoded, sizeof(encoded));
}

bool DecodeBlockHeaderPages(std::span<const std::byte, kBlockHeaderBytes> input,
                            BlockHeader* header,
                            std::uint8_t* active_slot) noexcept {
  BlockHeader best{};
  std::uint8_t best_slot = 0;
  bool found = false;
  for (std::uint8_t slot = 0; slot < kBlockHeaderSlots; ++slot) {
    BlockHeader decoded{};
    if (!DecodeBlockHeader(std::span<const std::byte, kBlockHeaderSlotBytes>(
                               input.data() + slot * kBlockHeaderSlotBytes,
                               kBlockHeaderSlotBytes),
                           &decoded)) {
      continue;
    }
    if (!found || decoded.allocation_epoch_ > best.allocation_epoch_ ||
        (decoded.allocation_epoch_ == best.allocation_epoch_ &&
         decoded.header_sequence_ > best.header_sequence_)) {
      best = decoded;
      best_slot = slot;
      found = true;
    }
  }
  if (!found) {
    return false;
  }
  if (header != nullptr) {
    *header = best;
  }
  if (active_slot != nullptr) {
    *active_slot = best_slot;
  }
  return true;
}

bool DecodeBlockHeader(std::span<const std::byte, kBlockHeaderSlotBytes> input,
                       BlockHeader* header) noexcept {
  if (header == nullptr) {
    return false;
  }
  BlockHeader decoded{};
  std::memcpy(&decoded, input.data(), sizeof(decoded));
  if (decoded.magic_ != kBlockMagic || decoded.block_id_ == kInvalidBlockId ||
      LocalBlockId(decoded.block_id_) == 0 ||
      decoded.version_ != kStorageFormatVersion ||
      decoded.header_bytes_ != kBlockHeaderBytes ||
      decoded.block_bytes_ != kStorageBlockBytes ||
      decoded.layout_worker_count_ == 0 ||
      decoded.layout_worker_count_ > kLogicalStorageShards ||
      decoded.writer_id_ >= decoded.layout_worker_count_ ||
      decoded.committed_bytes_ < kBlockHeaderBytes ||
      decoded.committed_bytes_ > kStorageBlockBytes) {
    return false;
  }
  if ((decoded.kind_ != BlockKind::kRecords &&
       decoded.kind_ != BlockKind::kPayloadExtent &&
       decoded.kind_ != BlockKind::kTransaction) ||
      decoded.reserved_ != std::array<std::uint8_t, 3>{}) {
    return false;
  }
  if (decoded.kind_ == BlockKind::kRecords ||
      decoded.kind_ == BlockKind::kTransaction) {
    if (decoded.extent_index_ != 0 || decoded.extent_payload_bytes_ != 0 ||
        decoded.extent_payload_checksum_ != 0 ||
        decoded.reserved_runtime_ != std::array<std::uint64_t, 3>{} ||
        (decoded.kind_ == BlockKind::kRecords && decoded.tx_generation_ != 0) ||
        (decoded.kind_ == BlockKind::kTransaction &&
         decoded.tx_generation_ == 0)) {
      return false;
    }
  } else if (decoded.kind_ == BlockKind::kPayloadExtent) {
    if (decoded.record_count_ != 0 || decoded.extent_payload_bytes_ == 0 ||
        decoded.extent_payload_bytes_ > kExtentPayloadBytes ||
        decoded.committed_bytes_ !=
            kBlockHeaderBytes + decoded.extent_payload_bytes_ ||
        decoded.reserved_runtime_ != std::array<std::uint64_t, 3>{} ||
        decoded.tx_generation_ != 0) {
      return false;
    }
  }
  const std::uint32_t expected = decoded.checksum_;
  std::array<std::byte, kBlockHeaderSlotBytes> copy{};
  std::memcpy(copy.data(), input.data(), copy.size());
  decoded.checksum_ = 0;
  std::memcpy(copy.data(), &decoded, sizeof(decoded));
  if (Crc32c(copy) != expected) {
    return false;
  }
  *header = decoded;
  return true;
}

bool EncodeRecordHeader(const RecordHeader& header, std::string_view key,
                        std::span<std::byte> output) noexcept {
  const std::size_t header_bytes =
      RecordHeaderBytes(key.size(), header.key_external_);
  if (header.magic_ != kRecordMagic ||
      header.version_ != kStorageFormatVersion ||
      !ValidRecordKeySize(key.size()) || key.size() != header.key_bytes_ ||
      (header.key_external_ && key.empty()) ||
      header.db_id_ >= kLogicalDatabaseCount ||
      static_cast<std::uint8_t>(header.value_type_) >
          static_cast<std::uint8_t>(ValueType::kStream) ||
      (header.kind_ == RecordKind::kValue &&
       header.value_type_ == ValueType::kNone) ||
      (header.kind_ == RecordKind::kTombstone &&
       (header.logical_size_ != 0 || header.expire_at_ms_ != 0 ||
        header.value_type_ != ValueType::kNone ||
        (!header.key_external_ &&
         (header.payload_bytes_ != 0 || header.external_)) ||
        (header.key_external_ && !header.external_ &&
         header.payload_bytes_ != header.key_bytes_))) ||
      (header.kind_ == RecordKind::kTxCommit &&
       (header.logical_size_ != 0 || header.payload_bytes_ != 0 ||
        header.external_ || header.expire_at_ms_ != 0 ||
        header.value_type_ != ValueType::kNone || header.txid_ == 0 ||
        header.key_bytes_ != 0 || header.key_external_)) ||
      header.header_bytes_ != header_bytes || output.size() != header_bytes) {
    return false;
  }
  RecordHeader encoded = header;
  encoded.value_type_ =
      static_cast<ValueType>(static_cast<std::uint8_t>(encoded.value_type_) |
                             (encoded.external_ ? kExternalValueMask : 0));
  encoded.external_ = false;
  encoded.key_bytes_ = static_cast<std::uint32_t>(encoded.key_bytes_) |
                       (encoded.key_external_ ? kExternalKeyMask : 0);
  encoded.key_external_ = false;
  encoded.header_checksum_ = 0;
  std::memcpy(output.data(), &encoded, sizeof(encoded));
  std::size_t encoded_bytes = sizeof(encoded);
  if (!header.key_external_) {
    std::memcpy(output.data() + sizeof(encoded), key.data(), key.size());
    encoded_bytes += key.size();
  }
  // Every non-padding byte is overwritten above. Clear only the alignment
  // tail because it participates in the durable header checksum; clearing the
  // complete header first would write the header and inline key twice on every
  // record append.
  std::fill(output.begin() + encoded_bytes, output.end(), std::byte{0});
  encoded.header_checksum_ = Crc32c(output);
  std::memcpy(output.data(), &encoded, sizeof(encoded));
  return true;
}

bool DecodeRecordHeader(std::span<const std::byte> input, RecordHeader* header,
                        std::string_view* key) noexcept {
  if (header == nullptr || key == nullptr ||
      input.size() < sizeof(RecordHeader)) {
    return false;
  }
  RecordHeader decoded{};
  std::memcpy(&decoded, input.data(), sizeof(decoded));
  const std::uint8_t encoded_type =
      static_cast<std::uint8_t>(decoded.value_type_);
  decoded.external_ = (encoded_type & kExternalValueMask) != 0;
  decoded.value_type_ =
      static_cast<ValueType>(encoded_type & ~kExternalValueMask);
  const std::uint32_t encoded_key_bytes = decoded.key_bytes_;
  decoded.key_external_ = (encoded_key_bytes & kExternalKeyMask) != 0;
  decoded.key_bytes_ = encoded_key_bytes & ~kExternalKeyMask;
  if (decoded.magic_ != kRecordMagic ||
      decoded.version_ != kStorageFormatVersion ||
      (decoded.kind_ != RecordKind::kValue &&
       decoded.kind_ != RecordKind::kTombstone &&
       decoded.kind_ != RecordKind::kTxCommit) ||
      decoded.db_id_ >= kLogicalDatabaseCount ||
      static_cast<std::uint8_t>(decoded.value_type_) >
          static_cast<std::uint8_t>(ValueType::kStream) ||
      (decoded.kind_ == RecordKind::kValue &&
       decoded.value_type_ == ValueType::kNone) ||
      (decoded.kind_ == RecordKind::kValue &&
       decoded.value_type_ == ValueType::kString &&
       decoded.logical_size_ > kMaxBitmapBytes) ||
      decoded.replication_epoch_ == 0 || decoded.db_epoch_ == 0 ||
      !ValidRecordKeySize(decoded.key_bytes_) ||
      decoded.header_bytes_ !=
          RecordHeaderBytes(decoded.key_bytes_, decoded.key_external_) ||
      decoded.header_bytes_ > kMaxRecordHeaderBytes ||
      decoded.header_bytes_ > input.size() ||
      decoded.total_disk_bytes_ !=
          AlignRecord(static_cast<std::size_t>(decoded.header_bytes_) +
                      decoded.payload_bytes_) ||
      decoded.total_disk_bytes_ > kStorageBlockBytes - kBlockHeaderBytes) {
    return false;
  }
  if (decoded.kind_ != RecordKind::kValue &&
      (decoded.logical_size_ != 0 || decoded.expire_at_ms_ != 0 ||
       decoded.value_type_ != ValueType::kNone)) {
    return false;
  }
  if (decoded.kind_ == RecordKind::kTombstone &&
      ((!decoded.key_external_ &&
        (decoded.payload_bytes_ != 0 || decoded.external_)) ||
       (decoded.key_external_ && !decoded.external_ &&
        decoded.payload_bytes_ != decoded.key_bytes_))) {
    return false;
  }
  if (decoded.kind_ == RecordKind::kTxCommit &&
      (decoded.txid_ == 0 || decoded.key_bytes_ != 0 ||
       decoded.key_external_)) {
    return false;
  }
  const std::uint32_t expected = decoded.header_checksum_;
  std::array<std::byte, kMaxRecordHeaderBytes> copy{};
  std::memcpy(copy.data(), input.data(), decoded.header_bytes_);
  RecordHeader checksum_header = decoded;
  checksum_header.value_type_ = static_cast<ValueType>(encoded_type);
  checksum_header.external_ = false;
  checksum_header.key_bytes_ = encoded_key_bytes;
  checksum_header.key_external_ = false;
  checksum_header.header_checksum_ = 0;
  std::memcpy(copy.data(), &checksum_header, sizeof(checksum_header));
  if (Crc32c(std::span<const std::byte>(copy.data(), decoded.header_bytes_)) !=
      expected) {
    return false;
  }
  *header = decoded;
  if (decoded.key_external_) {
    *key = {};
  } else {
    *key = std::string_view(
        reinterpret_cast<const char*>(input.data() + sizeof(RecordHeader)),
        decoded.key_bytes_);
  }
  return true;
}

bool EncodeReplicationFrameHeader(
    const ReplicationFrameHeader& header,
    std::span<std::byte, sizeof(ReplicationFrameHeader)> output) noexcept {
  if (header.magic_ != kReplicationFrameMagic ||
      header.version_ != kStorageFormatVersion ||
      header.header_bytes_ != sizeof(ReplicationFrameHeader) ||
      header.lsn_ == 0 || header.partition_id_ >= kLogicalStorageShards ||
      header.reserved_ != 0 ||
      header.payload_bytes_ >
          kStorageBlockBytes - sizeof(ReplicationFrameHeader) ||
      header.total_disk_bytes_ !=
          AlignRecord(sizeof(ReplicationFrameHeader) + header.payload_bytes_)) {
    return false;
  }
  const std::uint8_t allowed_flags =
      ReplicationFrameFlag::kFirst | ReplicationFrameFlag::kLast;
  const bool first = (header.flags_ & static_cast<std::uint8_t>(
                                          ReplicationFrameFlag::kFirst)) != 0;
  if ((header.flags_ & ~allowed_flags) != 0 ||
      first != (header.fragment_index_ == 0)) {
    return false;
  }
  if (header.kind_ != ReplicationEventKind::kMutation &&
      header.kind_ != ReplicationEventKind::kTransaction &&
      header.kind_ != ReplicationEventKind::kControl &&
      header.kind_ != ReplicationEventKind::kEphemeral) {
    return false;
  }
  ReplicationFrameHeader encoded = header;
  encoded.header_checksum_ = 0;
  std::memcpy(output.data(), &encoded, sizeof(encoded));
  encoded.header_checksum_ = Crc32c(output);
  std::memcpy(output.data(), &encoded, sizeof(encoded));
  return true;
}

bool DecodeReplicationFrameHeader(std::span<const std::byte> input,
                                  ReplicationFrameHeader* header) noexcept {
  if (header == nullptr || input.size() < sizeof(ReplicationFrameHeader)) {
    return false;
  }
  ReplicationFrameHeader decoded{};
  std::memcpy(&decoded, input.data(), sizeof(decoded));
  std::array<std::byte, sizeof(ReplicationFrameHeader)> encoded{};
  const std::uint32_t expected = decoded.header_checksum_;
  decoded.header_checksum_ = 0;
  std::memcpy(encoded.data(), &decoded, sizeof(decoded));
  if (Crc32c(encoded) != expected) {
    return false;
  }
  decoded.header_checksum_ = expected;
  std::array<std::byte, sizeof(ReplicationFrameHeader)> validated{};
  return EncodeReplicationFrameHeader(decoded, validated) &&
                 std::memcmp(validated.data(), input.data(),
                             validated.size()) == 0
             ? (*header = decoded, true)
             : false;
}

}  // namespace keylane::storage
