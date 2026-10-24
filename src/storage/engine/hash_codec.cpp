/*
 * Copyright (C) 2026 EloqData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "lavik/storage/detail/hash_codec.h"

#include <algorithm>
#include <bit>
#include <cassert>
#include <cstring>
#include <limits>

#if defined(__AVX2__) || defined(__SSE2__)
#include <immintrin.h>
#elif defined(__aarch64__)
#include <arm_neon.h>
#endif

#include "absl/container/flat_hash_map.h"
#include "absl/container/inlined_vector.h"

namespace lavik::storage {

namespace {

bool ReadU32(std::string_view input, std::size_t* offset,
             std::uint32_t* value) {
  if (*offset > input.size() || input.size() - *offset < sizeof(*value)) {
    return false;
  }
  const auto* bytes =
      reinterpret_cast<const unsigned char*>(input.data()) + *offset;
  *value = static_cast<std::uint32_t>(bytes[0]) |
           (static_cast<std::uint32_t>(bytes[1]) << 8) |
           (static_cast<std::uint32_t>(bytes[2]) << 16) |
           (static_cast<std::uint32_t>(bytes[3]) << 24);
  *offset += sizeof(*value);
  return true;
}

bool ReadU64(std::string_view input, std::size_t* offset,
             std::uint64_t* value) {
  std::uint32_t low = 0;
  std::uint32_t high = 0;
  if (!ReadU32(input, offset, &low) || !ReadU32(input, offset, &high))
    return false;
  *value = low | (static_cast<std::uint64_t>(high) << 32);
  return true;
}

// Arrays are packed on disk: memcpy loads avoid alignment and aliasing UB.
std::uint32_t Load32(std::string_view bytes, std::size_t offset) noexcept {
  std::uint32_t value;
  std::memcpy(&value, bytes.data() + offset, sizeof(value));
  if constexpr (std::endian::native == std::endian::big)
    value = std::byteswap(value);
  return value;
}

void Put(std::span<char> out, std::size_t offset, std::uint64_t value,
         unsigned width) noexcept {
  for (unsigned i = 0; i < width; ++i)
    out[offset + i] = static_cast<char>(value >> (8 * i));
}

void Header(std::span<char> out, std::uint32_t version, std::size_t count,
            std::size_t bytes, bool empty_values) noexcept {
  Put(out, 0, kHashValueMagic, 8);
  Put(out, 8, version, 4);
  Put(out, 12, kHashValueHeaderBytes, 4);
  Put(out, 16, count, 4);
  Put(out, 20, version == kHashValueVersion && empty_values ? 1 : 0, 4);
  Put(out, 24, bytes, 8);
}

}  // namespace

absl::StatusOr<std::size_t> AppendHashEntrySize(std::size_t encoded_bytes,
                                                std::size_t field_bytes,
                                                std::size_t value_bytes,
                                                std::size_t max_bytes) {
  if (max_bytes < 8 || field_bytes > kMaxStringBytes ||
      value_bytes > kMaxStringBytes || encoded_bytes > max_bytes - 8 ||
      field_bytes > max_bytes - encoded_bytes - 8 ||
      value_bytes > max_bytes - encoded_bytes - 8 - field_bytes) {
    return absl::OutOfRangeError("Hash field or value exceeds encoding limits");
  }
  return encoded_bytes + 8 + field_bytes + value_bytes;
}

absl::StatusOr<HashValueReader> HashValueReader::OpenV1(
    std::string_view payload) {
  if (payload.size() < kHashValueHeaderBytes) {
    return absl::InternalError("Hash value is truncated");
  }
  // Do not memcpy a C++ header into durable group snapshots. Routing and
  // payload interpretation must survive a process restart independently of
  // compiler padding and host byte order.
  std::size_t offset = 0;
  std::uint64_t magic = 0;
  std::uint32_t version = 0;
  std::uint32_t header_bytes = 0;
  std::uint32_t element_count = 0;
  std::uint32_t reserved = 0;
  std::uint64_t encoded_bytes = 0;
  if (!ReadU64(payload, &offset, &magic) ||
      !ReadU32(payload, &offset, &version) ||
      !ReadU32(payload, &offset, &header_bytes) ||
      !ReadU32(payload, &offset, &element_count) ||
      !ReadU32(payload, &offset, &reserved) ||
      !ReadU64(payload, &offset, &encoded_bytes) || magic != kHashValueMagic ||
      version != kLegacyHashValueVersion ||
      header_bytes != kHashValueHeaderBytes || element_count == 0 ||
      reserved != 0 || encoded_bytes != payload.size()) {
    return absl::InternalError("invalid Hash value header");
  }
  constexpr std::size_t kMinimumEntryBytes = 2 * sizeof(std::uint32_t);
  const std::size_t encoded_entries_bytes =
      payload.size() - kHashValueHeaderBytes;
  if (element_count > encoded_entries_bytes / kMinimumEntryBytes) {
    return absl::InternalError("Hash element count exceeds encoded payload");
  }

  return HashValueReader(payload, element_count);
}

absl::StatusOr<HashEntryView> HashValueReader::NextV1() {
  if (remaining_ == 0) {
    return absl::OutOfRangeError("Hash reader is exhausted");
  }
  std::size_t offset = offset_;
  std::uint32_t field_bytes = 0;
  std::uint32_t value_bytes = 0;
  if (!ReadU32(payload_, &offset, &field_bytes) ||
      !ReadU32(payload_, &offset, &value_bytes) || offset > payload_.size() ||
      field_bytes > kMaxStringBytes || value_bytes > kMaxStringBytes ||
      field_bytes > payload_.size() - offset ||
      value_bytes > payload_.size() - offset - field_bytes) {
    return absl::InternalError("Hash entry is truncated");
  }
  HashEntryView entry{
      .field_ = payload_.substr(offset, field_bytes),
      .value_ = payload_.substr(offset + field_bytes, value_bytes)};
  offset += field_bytes + value_bytes;
  if (remaining_ == 1 && offset != payload_.size()) {
    return absl::InternalError("Hash value has trailing bytes");
  }
  offset_ = offset;
  --remaining_;
  return entry;
}

std::uint8_t HashFieldFingerprint(std::string_view field) noexcept {
  // This seed and SipHash-1-2 are frozen for payload v2. A fixed independent
  // seed keeps filtering independent of grouped prefix bits and restart seeds.
  static constexpr DigestSeed seed{0x4c, 0x61, 0x76, 0x69, 0x6b, 0x48,
                                   0x61, 0x73, 0x68, 0x56, 0x32, 0x54,
                                   0x61, 0x67, 0x21, 0x00};
  return static_cast<std::uint8_t>(ComputeDigest(field, seed).value_);
}

absl::StatusOr<HashValueEncoder> HashValueEncoder::Create(
    std::span<const HashEntry> entries, std::uint32_t version) {
  HashValueEncoder result;
  result.entries_ = entries;
  result.count_ = entries.size();
  auto status = result.Initialize(version);
  if (!status.ok()) return status;
  return result;
}

absl::StatusOr<HashValueEncoder> HashValueEncoder::Create(
    std::span<const HashEntryView> entries, std::uint32_t version) {
  HashValueEncoder result;
  result.views_ = entries;
  result.count_ = entries.size();
  auto status = result.Initialize(version);
  if (!status.ok()) return status;
  return result;
}

HashEntryView HashValueEncoder::Entry(std::size_t i) const noexcept {
  return entries_.empty()
             ? views_[i]
             : HashEntryView{entries_[i].field_, entries_[i].value_};
}

absl::Status HashValueEncoder::Initialize(std::uint32_t version) {
  if (count_ == 0 || count_ > UINT32_MAX ||
      (version != kHashValueVersion && version != kLegacyHashValueVersion))
    return absl::OutOfRangeError("invalid Hash version/element count");
  std::size_t data_bytes = 0;
  for (std::size_t i = 0; i < count_; ++i) {
    const auto entry = Entry(i);
    auto next =
        AppendHashEntrySize(data_bytes, entry.field_.size(),
                            entry.value_.size(), std::string{}.max_size());
    if (!next.ok()) return next.status();
    data_bytes = *next - 8;
    empty_values_ &= entry.value_.empty();
  }
  version_ = data_bytes > UINT32_MAX ? kLegacyHashValueVersion : version;
  const auto framing = version_ == kLegacyHashValueVersion
                           ? count_ * 8
                           : 4 + count_ * (empty_values_ ? 5 : 9);
  if (data_bytes > std::string{}.max_size() - kHashValueHeaderBytes - framing)
    return absl::OutOfRangeError("Hash payload length overflow");
  encoded_bytes_ = kHashValueHeaderBytes + data_bytes + framing;
  Header(header_, version_, count_, encoded_bytes_, empty_values_);
  return absl::OkStatus();
}

std::optional<std::string_view> HashValueEncoder::Next() noexcept {
  borrowed_ = false;
  if (phase_ == 0) {
    phase_ = 1;
    return std::string_view(header_.data(), header_.size());
  }
  if (version_ == kLegacyHashValueVersion) [[unlikely]]
    return NextV1();
  if (phase_ <= 2 && entry_ < count_) {
    const auto entry = Entry(entry_);
    borrowed_ = true;
    if (phase_ == 1) {
      phase_ = 2;
      return entry.field_;
    }
    phase_ = 1;
    ++entry_;
    return entry.value_;
  }
  if (phase_ <= 2) {
    phase_ = 3;
    entry_ = 0;
  }
  std::size_t used = 0;
  while (used < metadata_.size() && phase_ < 6) {
    const auto limit = count_ + (phase_ == 3);
    if (entry_ == limit) {
      ++phase_;
      if (phase_ == 4 && empty_values_) ++phase_;
      entry_ = 0;
      continue;
    }
    if (phase_ == 3) {
      Put(metadata_, used, offset_, 4);
      if (entry_ < count_) {
        const auto entry = Entry(entry_);
        offset_ += entry.field_.size() + entry.value_.size();
      }
      used += 4;
    } else if (phase_ == 4) {
      Put(metadata_, used, Entry(entry_).field_.size(), 4);
      used += 4;
    } else {
      metadata_[used++] =
          static_cast<char>(HashFieldFingerprint(Entry(entry_).field_));
    }
    ++entry_;
  }
  if (used == 0) return std::nullopt;
  return std::string_view(metadata_.data(), used);
}

std::optional<std::string_view> HashValueEncoder::NextV1() noexcept {
  if (entry_ == count_) return std::nullopt;
  const auto entry = Entry(entry_);
  if (phase_ == 1) {
    Put(metadata_, 0, entry.field_.size(), 4);
    Put(metadata_, 4, entry.value_.size(), 4);
    phase_ = 2;
    return std::string_view(metadata_.data(), 8);
  }
  borrowed_ = true;
  if (phase_ == 2) {
    phase_ = 3;
    return entry.field_;
  }
  phase_ = 1;
  ++entry_;
  return entry.value_;
}

absl::StatusOr<HashValueReader> HashValueReader::Open(
    std::string_view payload) {
  if (payload.size() < kHashValueHeaderBytes)
    return absl::InternalError("Hash value is truncated");
  const auto version = Load32(payload, 8);
  if (version == kLegacyHashValueVersion) [[unlikely]]
    return OpenV1(payload);
  if (version == kHashValueVersion) [[likely]] {
    const auto count = Load32(payload, 16);
    const auto flags = Load32(payload, 20);
    const auto framing =
        std::uint64_t{4} + std::uint64_t{count} * (flags == 1 ? 5 : 9);
    if (Load32(payload, 0) != static_cast<std::uint32_t>(kHashValueMagic) ||
        Load32(payload, 4) !=
            static_cast<std::uint32_t>(kHashValueMagic >> 32) ||
        Load32(payload, 12) != kHashValueHeaderBytes || count == 0 ||
        flags > 1 ||
        (std::uint64_t{Load32(payload, 24)} |
         (std::uint64_t{Load32(payload, 28)} << 32)) != payload.size() ||
        framing > payload.size() - kHashValueHeaderBytes)
      return absl::InternalError("invalid Hash value header");
    HashValueReader result(payload, count);
    result.version_ = version;
    result.empty_values_ = flags == 1;
    result.tags_begin_ = payload.size() - count;
    result.lengths_begin_ = result.tags_begin_ -
                            (result.empty_values_ ? 0 : std::size_t{count} * 4);
    result.offsets_begin_ =
        result.lengths_begin_ - (std::size_t{count} + 1) * 4;
    result.data_bytes_ = result.offsets_begin_ - kHashValueHeaderBytes;
    if (result.data_bytes_ > UINT32_MAX ||
        Load32(payload, result.offsets_begin_) != 0 ||
        Load32(payload, result.offsets_begin_ + std::size_t{count} * 4) !=
            result.data_bytes_)
      return absl::InternalError("invalid Hash offset boundaries");
    return result;
  }
  return absl::InternalError("unsupported Hash value version");
}

absl::StatusOr<HashEntryView> HashValueReader::At(std::size_t index) const {
  if (version_ != kHashValueVersion || index >= size_)
    return absl::OutOfRangeError("Hash slot is out of range");
  const auto begin = Load32(payload_, offsets_begin_ + index * 4);
  const auto end = Load32(payload_, offsets_begin_ + (index + 1) * 4);
  if (begin > end || end > data_bytes_)
    return absl::InternalError("invalid Hash entry offsets");
  const auto field_bytes = empty_values_
                               ? end - begin
                               : Load32(payload_, lengths_begin_ + index * 4);
  if (field_bytes > end - begin || field_bytes > kMaxStringBytes ||
      end - begin - field_bytes > kMaxStringBytes)
    return absl::InternalError("Hash entry is truncated");
  return HashEntryView{
      payload_.substr(kHashValueHeaderBytes + begin, field_bytes),
      payload_.substr(kHashValueHeaderBytes + begin + field_bytes,
                      end - begin - field_bytes)};
}

absl::StatusOr<HashEntryView> HashValueReader::Next() {
  if (version_ == kLegacyHashValueVersion) [[unlikely]]
    return NextV1();
  if (remaining_ == 0) return absl::OutOfRangeError("Hash reader is exhausted");
  auto entry = At(size_ - remaining_);
  if (entry.ok()) --remaining_;
  return entry;
}

absl::Status HashValueReader::ValidateFingerprints() const {
  if (version_ == kLegacyHashValueVersion) [[unlikely]]
    return absl::OkStatus();
  for (std::size_t i = 0; i < size_; ++i) {
    auto entry = At(i);
    if (!entry.ok()) return entry.status();
    if (static_cast<std::uint8_t>(payload_[tags_begin_ + i]) !=
        HashFieldFingerprint(entry->field_))
      return absl::InternalError("Hash fingerprint mismatch");
  }
  return absl::OkStatus();
}

absl::StatusOr<std::optional<HashEntryView>> HashValueReader::FindV1(
    std::string_view field, std::size_t* slot, std::size_t first) const {
  auto reader = *this;
  reader.offset_ = kHashValueHeaderBytes;
  reader.remaining_ = size_;
  while (reader.remaining_ != 0) {
    const auto index = size_ - reader.remaining_;
    auto entry = reader.NextV1();
    if (!entry.ok()) return entry.status();
    if (index >= first && entry->field_ == field) {
      if (slot) *slot = index;
      return std::optional<HashEntryView>(*entry);
    }
  }
  return std::nullopt;
}

absl::StatusOr<std::optional<HashEntryView>> HashValueReader::Find(
    std::string_view field, bool scalar_tags, std::size_t* slot,
    std::size_t first) const {
  if (first > size_) return absl::OutOfRangeError("Hash slot is out of range");
  if (first == size_) return std::nullopt;
  if (version_ == kLegacyHashValueVersion) [[unlikely]]
    return FindV1(field, slot, first);
  const auto tag = HashFieldFingerprint(field);
  const auto* tags = payload_.data() + tags_begin_;
  std::size_t i = first;
  if (!scalar_tags) {
#if defined(__AVX2__)
    constexpr std::size_t width = 32;
    const auto wanted = _mm256_set1_epi8(static_cast<char>(tag));
#elif defined(__SSE2__)
    constexpr std::size_t width = 16;
    const auto wanted = _mm_set1_epi8(static_cast<char>(tag));
#elif defined(__aarch64__)
    constexpr std::size_t width = 16;
    const auto wanted = vdupq_n_u8(tag);
#else
    constexpr std::size_t width = 0;
#endif
#if defined(__AVX2__) || defined(__SSE2__) || defined(__aarch64__)
    for (; size_ - i >= width; i += width) {
#if defined(__AVX2__)
      constexpr unsigned bits_per_lane = 1;
      std::uint32_t mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(
          _mm256_loadu_si256(reinterpret_cast<const __m256i*>(tags + i)),
          wanted));
#elif defined(__SSE2__)
      constexpr unsigned bits_per_lane = 1;
      std::uint32_t mask = _mm_movemask_epi8(_mm_cmpeq_epi8(
          _mm_loadu_si128(reinterpret_cast<const __m128i*>(tags + i)), wanted));
#else
      constexpr unsigned bits_per_lane = 4;
      const auto equal = vceqq_u8(
          vld1q_u8(reinterpret_cast<const std::uint8_t*>(tags + i)), wanted);
      // Narrow each pair of 0xff/0x00 lanes to two nibbles. This yields a
      // candidate mask directly in a general register without a scalar loop
      // over all sixteen lanes. One complete nibble is cleared per candidate.
      std::uint64_t mask = vget_lane_u64(
          vreinterpret_u64_u8(vshrn_n_u16(vreinterpretq_u16_u8(equal), 4)), 0);
#endif
      while (mask != 0) {
        const auto lane = std::countr_zero(mask) / bits_per_lane;
        const auto at = i + lane;
        auto entry = At(at);
        if (!entry.ok()) return entry.status();
        if (entry->field_ == field) {
          if (slot) *slot = at;
          return std::optional<HashEntryView>(*entry);
        }
        mask &= ~(decltype(mask){(1u << bits_per_lane) - 1}
                  << (lane * bits_per_lane));
      }
    }
#else
    (void)width;
#endif
  }
  // No SIMD padding is persisted. Short arrays and the final partial vector
  // stay strictly within the supplied payload, including at a guard page.
  for (; i < size_; ++i) {
    if (static_cast<std::uint8_t>(tags[i]) != tag) continue;
    auto entry = At(i);
    if (!entry.ok()) return entry.status();
    if (entry->field_ == field) {
      if (slot) *slot = i;
      return std::optional<HashEntryView>(*entry);
    }
  }
  return std::nullopt;
}

absl::StatusOr<HashValue> DecodeHashValue(std::string_view payload) {
  auto reader = HashValueReader::Open(payload);
  if (!reader.ok()) return reader.status();
  auto valid = reader->ValidateFingerprints();
  if (!valid.ok()) return valid;
  HashValue value;
  value.entries_.reserve(reader->size());
  for (std::size_t index = 0; index < reader->size(); ++index) {
    auto view = reader->Next();
    if (!view.ok()) return view.status();
    HashEntry entry;
    entry.field_.assign(view->field_);
    entry.value_.assign(view->value_);
    // Digests use a process-random seed and are therefore reconstructed from
    // the durable field rather than encoded in the value.
    entry.digest_ = ComputeDigest(entry.field_);
    value.entries_.push_back(std::move(entry));
  }
  return value;
}

absl::StatusOr<std::optional<std::string>> HashValueReader::SpliceValidated(
    std::size_t slot, std::optional<HashEntryView> replacement,
    std::size_t max_bytes, std::size_t prefix_bytes) const {
  assert(version_ == kHashValueVersion);
  if (slot > size_ || (slot == size_ && !replacement))
    return absl::InvalidArgumentError("invalid Hash splice slot");
  if (replacement && (replacement->field_.size() > kMaxStringBytes ||
                      replacement->value_.size() > kMaxStringBytes))
    return absl::OutOfRangeError("Hash splice entry exceeds encoding limits");
  if (prefix_bytes > std::string{}.max_size())
    return absl::OutOfRangeError("Hash splice prefix overflows");
  const bool append = slot == size_;
  const bool remove = !replacement;
  if (append && size_ == UINT32_MAX)
    return absl::OutOfRangeError("Hash splice count overflows");
  std::optional<HashEntryView> old;
  if (!append) {
    auto entry = At(slot);
    if (!entry.ok()) return entry.status();
    old = *entry;
    if (replacement && replacement->field_ != old->field_)
      return absl::InvalidArgumentError("Hash splice cannot rename a field");
  }
  // Equal-size overwrites leave every index byte intact. The new image is
  // private; replacing a value borrowed from the immutable source is safe.
  if (old && replacement && old->value_.size() == replacement->value_.size()) {
    if (payload_.size() > max_bytes) return std::nullopt;
    if (prefix_bytes > std::string{}.max_size() - payload_.size())
      return absl::OutOfRangeError("Hash splice output overflows");
    std::string output;
    output.resize_and_overwrite(
        prefix_bytes + payload_.size(), [&](char* data, std::size_t) noexcept {
          std::memset(data, 0, prefix_bytes);
          std::memcpy(data + prefix_bytes, payload_.data(), payload_.size());
          if (!replacement->value_.empty())
            std::memcpy(
                data + prefix_bytes + (old->value_.data() - payload_.data()),
                replacement->value_.data(), replacement->value_.size());
          return prefix_bytes + payload_.size();
        });
    return output;
  }
  const auto count = size_ + append - remove;
  if (count == 0) return std::string(prefix_bytes, '\0');
  const auto begin = Load32(payload_, offsets_begin_ + slot * 4);
  const auto end =
      append ? begin : Load32(payload_, offsets_begin_ + (slot + 1) * 4);
  const auto added =
      replacement ? replacement->field_.size() + replacement->value_.size() : 0;
  const std::uint64_t data_bytes = data_bytes_ - (end - begin) + added;
  if (data_bytes > UINT32_MAX) return std::nullopt;
  bool empty_values =
      empty_values_ && (!replacement || replacement->value_.empty());
  if (!empty_values_ && (!replacement || replacement->value_.empty())) {
    // Removing/replacing the last nonempty value drops the lengths array.
    // Inspect only numeric metadata, without decoding or hashing fields.
    empty_values = true;
    for (std::size_t i = 0; i < size_; ++i) {
      if (i == slot) continue;
      if (Load32(payload_, offsets_begin_ + (i + 1) * 4) -
              Load32(payload_, offsets_begin_ + i * 4) !=
          Load32(payload_, lengths_begin_ + i * 4)) {
        empty_values = false;
        break;
      }
    }
  }
  const std::uint64_t bytes = kHashValueHeaderBytes + data_bytes + 4 +
                              std::uint64_t{count} * (empty_values ? 5 : 9);
  if (bytes > max_bytes) return std::nullopt;
  if (bytes > std::string{}.max_size() ||
      prefix_bytes > std::string{}.max_size() - bytes)
    return absl::OutOfRangeError("Hash splice output overflows");
  std::string output;
  output.resize_and_overwrite(prefix_bytes + bytes, [&](char* data,
                                                        std::size_t) noexcept {
    std::memset(data, 0, prefix_bytes);
    std::span<char> out(data + prefix_bytes, bytes);
    Header(out, kHashValueVersion, count, bytes, empty_values);
    auto copy = [&](std::size_t at, std::string_view part) {
      if (!part.empty()) std::memcpy(out.data() + at, part.data(), part.size());
    };
    // Copy untouched data as two spans. Value lengths may change without
    // re-encoding each field or recalculating any surviving fingerprint.
    copy(kHashValueHeaderBytes, payload_.substr(kHashValueHeaderBytes, begin));
    if (replacement) {
      copy(kHashValueHeaderBytes + begin, replacement->field_);
      copy(kHashValueHeaderBytes + begin + replacement->field_.size(),
           replacement->value_);
    }
    copy(kHashValueHeaderBytes + begin + added,
         payload_.substr(kHashValueHeaderBytes + end, data_bytes_ - end));
    const auto offsets = kHashValueHeaderBytes + data_bytes;
    const auto lengths = offsets + (count + 1) * 4;
    const auto tags = bytes - count;
    // Offsets before the splice, including its start, are unchanged.
    copy(offsets, payload_.substr(offsets_begin_, (slot + 1) * 4));
    for (std::size_t i = slot + 1; i <= count; ++i) {
      const auto old_end =
          append ? data_bytes_
                 : Load32(payload_, offsets_begin_ + (i + remove) * 4);
      Put(out, offsets + i * 4, old_end - (end - begin) + added, 4);
    }
    const auto tail = size_ - slot - !append;
    if (!empty_values) {
      if (empty_values_) {
        // The source omitted lengths. Its offsets already delimit fields.
        for (std::size_t i = 0; i < count; ++i) {
          const auto old_slot = i + (remove && i >= slot);
          const auto length =
              i == slot && replacement
                  ? replacement->field_.size()
                  : Load32(payload_, offsets_begin_ + (old_slot + 1) * 4) -
                        Load32(payload_, offsets_begin_ + old_slot * 4);
          Put(out, lengths + i * 4, length, 4);
        }
      } else {
        copy(lengths, payload_.substr(lengths_begin_, slot * 4));
        if (replacement)
          Put(out, lengths + slot * 4, replacement->field_.size(), 4);
        copy(lengths + (slot + !remove) * 4,
             payload_.substr(lengths_begin_ + (slot + !append) * 4, tail * 4));
      }
    }
    copy(tags, payload_.substr(tags_begin_, slot));
    if (replacement)
      out[tags + slot] =
          append ? static_cast<char>(HashFieldFingerprint(replacement->field_))
                 : payload_[tags_begin_ + slot];
    copy(tags + slot + !remove,
         payload_.substr(tags_begin_ + slot + !append, tail));
    return prefix_bytes + bytes;
  });
  return output;
}

absl::StatusOr<std::optional<CompactHashEditPlan>> PlanCompactHashEdit(
    std::string_view payload, std::size_t expected_count,
    std::span<const std::string_view> fields,
    std::span<const std::string_view> values, std::size_t max_bytes,
    CompactHashEditKind kind) {
  const bool erase = kind == CompactHashEditKind::kDelete;
  if ((!erase && fields.size() != values.size()) || (erase && !values.empty()))
    return absl::InvalidArgumentError("Hash field/value mismatch");
  auto reader = HashValueReader::Open(payload);
  if (!reader.ok()) return reader.status();
  if (reader->size() != expected_count)
    return absl::InternalError(
        "Hash element count does not match record metadata");

  struct Edit {
    std::string_view field;
    std::string_view value;
    bool varied = false;
    std::size_t slot = SIZE_MAX;
    std::size_t overwrite_at = SIZE_MAX;
  };
  // Single-field HSET keeps its entire plan inline. Batch plans index only
  // operands, never every field in the old value. First-seen order determines
  // the position of new fields; varied preserves A -> B -> A write semantics.
  absl::InlinedVector<Edit, 1> edits;
  absl::flat_hash_map<std::string_view, std::size_t> positions;
  edits.reserve(fields.size());
  if (fields.size() > 1) positions.reserve(fields.size());
  for (std::size_t i = 0; i < fields.size(); ++i) {
    const auto value = erase ? std::string_view{} : values[i];
    auto size = AppendHashEntrySize(kHashValueHeaderBytes, fields[i].size(),
                                    value.size(), std::string{}.max_size());
    if (!size.ok()) return size.status();
    if (fields.size() > 1) {
      const auto [it, inserted] = positions.emplace(fields[i], edits.size());
      if (!inserted) {
        auto& edit = edits[it->second];
        edit.varied |= edit.value != value;
        edit.value = value;
        continue;
      }
    }
    edits.push_back({fields[i], value});
  }
  auto valid = reader->ValidateFingerprints();
  if (!valid.ok()) return valid;
  const auto source = *reader;
  if (source.version() == kLegacyHashValueVersion) [[unlikely]] {
    // V1 has no filter validation pass. Even a no-op must check the framing
    // after its match before accepting the old image as a write source.
    auto scan = source;
    for (std::size_t i = 0; i < source.size(); ++i) {
      auto entry = scan.Next();
      if (!entry.ok()) return entry.status();
    }
  }
  CompactHashEditPlan result{.count_ = expected_count, .payload_ = {}};
  absl::InlinedVector<std::size_t, 1> existing;
  existing.reserve(edits.size());
  for (std::size_t i = 0; i < edits.size(); ++i) {
    auto& edit = edits[i];
    auto entry = source.Find(edit.field, false, &edit.slot);
    if (!entry.ok()) return entry.status();
    if (!*entry) continue;
    // Published compact fields are unique, but preparation must still reject
    // duplicate requested fields in a malformed source. Search the remaining
    // tags instead of comparing every stored field against the operand.
    auto duplicate = source.Find(edit.field, false, nullptr, edit.slot + 1);
    if (!duplicate.ok()) return duplicate.status();
    if (*duplicate)
      return absl::DataLossError("duplicate requested Hash field");
    existing.push_back(i);
    if (!erase && source.version() == kHashValueVersion &&
        edit.value.size() == (**entry).value_.size())
      edit.overwrite_at = (**entry).value_.data() - payload.data();
    result.changed_ |= erase || edit.varied || edit.value != (**entry).value_;
    if (erase) {
      ++result.removed_;
      --result.count_;
    }
  }
  for (const auto& edit : edits) {
    if (!erase && edit.slot == SIZE_MAX) {
      if (result.count_ == UINT32_MAX)
        return absl::OutOfRangeError("invalid Hash element count");
      ++result.added_;
      ++result.count_;
      result.changed_ = true;
    }
  }
  if (!result.changed_ || result.count_ == 0) return result;
  if (!erase && std::all_of(edits.begin(), edits.end(), [](const Edit& edit) {
        return edit.overwrite_at != SIZE_MAX;
      })) {
    if (payload.size() > max_bytes) return std::nullopt;
    // Single and batched equal-size replacements share one private page copy.
    // Deduplicated final values preserve transient-write semantics without
    // sorting slots or rewriting offsets, field lengths or fingerprints.
    std::string encoded(payload);
    for (const auto& edit : edits)
      if (!edit.value.empty())
        std::memcpy(encoded.data() + edit.overwrite_at, edit.value.data(),
                    edit.value.size());
    result.payload_.AppendOwnedBytes(std::move(encoded));
    return result;
  }
  if (edits.size() == 1 && source.version() == kHashValueVersion) {
    const auto& edit = edits.front();
    auto encoded = source.SpliceValidated(
        edit.slot == SIZE_MAX ? source.size() : edit.slot,
        erase ? std::nullopt
              : std::optional<HashEntryView>{{edit.field, edit.value}},
        max_bytes);
    if (!encoded.ok()) return encoded.status();
    if (!*encoded) return std::nullopt;
    result.payload_.AppendOwnedBytes(std::move(**encoded));
    return result;
  }
  // Sort only the requested matches. Rebuilding follows physical ordinals;
  // no per-stored-field hash map or repeated field comparison is needed.
  std::sort(existing.begin(), existing.end(),
            [&](auto a, auto b) { return edits[a].slot < edits[b].slot; });
  // Revisit borrowed input rather than retaining one descriptor per stored
  // field. Scratch remains O(operands + bounded compact output), including Sets
  // with many tiny members; no decoded strings are constructed.
  auto visit_output = [&](auto visit) -> absl::Status {
    auto scan = source;
    std::size_t next_edit = 0;
    for (std::size_t i = 0; i < source.size(); ++i) {
      auto entry = scan.Next();
      if (!entry.ok()) return entry.status();
      const Edit* edit = nullptr;
      if (next_edit < existing.size() && edits[existing[next_edit]].slot == i)
        edit = &edits[existing[next_edit++]];
      if (edit && erase) continue;
      visit(HashEntryView{entry->field_, edit ? edit->value : entry->value_});
    }
    if (!erase)
      for (const auto& edit : edits)
        if (edit.slot == SIZE_MAX) visit(HashEntryView{edit.field, edit.value});
    return absl::OkStatus();
  };
  std::uint64_t data_bytes = 0;
  bool empty_values = true;
  valid = visit_output([&](HashEntryView entry) {
    data_bytes += entry.field_.size() + entry.value_.size();
    empty_values &= entry.value_.empty();
  });
  if (!valid.ok()) return valid;
  const auto bytes = kHashValueHeaderBytes + data_bytes + 4 +
                     result.count_ * (empty_values ? 5 : 9);
  if (bytes > max_bytes || data_bytes > UINT32_MAX) return std::nullopt;
  std::string encoded(bytes, '\0');
  Header(encoded, kHashValueVersion, result.count_, bytes, empty_values);
  const auto offsets = kHashValueHeaderBytes + data_bytes;
  const auto lengths = offsets + 4 * (result.count_ + 1);
  const auto tags = bytes - result.count_;
  std::size_t offset = 0, index = 0;
  valid = visit_output([&](HashEntryView entry) {
    Put(encoded, offsets + 4 * index, offset, 4);
    if (!empty_values)
      Put(encoded, lengths + 4 * index, entry.field_.size(), 4);
    encoded[tags + index] =
        static_cast<char>(HashFieldFingerprint(entry.field_));
    if (!entry.field_.empty())
      std::memcpy(encoded.data() + kHashValueHeaderBytes + offset,
                  entry.field_.data(), entry.field_.size());
    offset += entry.field_.size();
    if (!entry.value_.empty())
      std::memcpy(encoded.data() + kHashValueHeaderBytes + offset,
                  entry.value_.data(), entry.value_.size());
    offset += entry.value_.size();
    ++index;
  });
  if (!valid.ok()) return valid;
  Put(encoded, offsets + 4 * result.count_, offset, 4);
  result.payload_.AppendOwnedBytes(std::move(encoded));
  return result;
}

namespace {
absl::StatusOr<std::string> EncodeHashCursor(
    absl::StatusOr<HashValueEncoder> encoder) {
  if (!encoder.ok()) return encoder.status();
  std::string result;
  result.reserve(encoder->encoded_bytes());
  while (auto part = encoder->Next()) result.append(*part);
  return result;
}
}  // namespace

absl::StatusOr<std::string> EncodeHashValue(const HashValue& value,
                                            std::uint32_t version) {
  return EncodeHashCursor(HashValueEncoder::Create(value.entries_, version));
}

absl::StatusOr<std::string> EncodeHashValueViews(
    std::span<const HashEntryView> entries, std::uint32_t version) {
  return EncodeHashCursor(HashValueEncoder::Create(entries, version));
}

}  // namespace lavik::storage
