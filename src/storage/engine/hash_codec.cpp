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

#include <limits>

#include "absl/container/flat_hash_map.h"
#include "absl/container/inlined_vector.h"

namespace lavik::storage {

namespace {

void AppendU32(std::string* output, std::uint32_t value) {
  output->push_back(static_cast<char>(value));
  output->push_back(static_cast<char>(value >> 8));
  output->push_back(static_cast<char>(value >> 16));
  output->push_back(static_cast<char>(value >> 24));
}

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

void AppendU64(std::string* output, std::uint64_t value) {
  AppendU32(output, static_cast<std::uint32_t>(value));
  AppendU32(output, static_cast<std::uint32_t>(value >> 32));
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

absl::StatusOr<HashValueReader> HashValueReader::Open(
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
      version != kStorageFormatVersion ||
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

absl::StatusOr<HashEntryView> HashValueReader::Next() {
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

absl::StatusOr<HashValue> DecodeHashValue(std::string_view payload) {
  auto reader = HashValueReader::Open(payload);
  if (!reader.ok()) return reader.status();
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

absl::StatusOr<std::optional<CompactHashEdit>> EditCompactHash(
    std::string_view payload, std::size_t expected_count,
    std::span<const std::string_view> fields,
    std::span<const std::string_view> values, std::size_t max_bytes) {
  if (fields.size() != values.size())
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
    bool found = false;
  };
  struct Splice {
    std::size_t begin;
    std::size_t end;
    std::size_t edit;
  };
  // Single-field HSET keeps its entire plan inline. Batch plans index only
  // operands, never every field in the old value. First-seen order determines
  // the position of new fields; varied preserves A -> B -> A write semantics.
  absl::InlinedVector<Edit, 1> edits;
  absl::InlinedVector<Splice, 1> splices;
  absl::flat_hash_map<std::string_view, std::size_t> positions;
  edits.reserve(fields.size());
  if (fields.size() > 1) positions.reserve(fields.size());
  for (std::size_t i = 0; i < fields.size(); ++i) {
    auto size = AppendHashEntrySize(kHashValueHeaderBytes, fields[i].size(),
                                    values[i].size(), std::string{}.max_size());
    if (!size.ok()) return size.status();
    if (fields.size() > 1) {
      const auto [it, inserted] = positions.emplace(fields[i], edits.size());
      if (!inserted) {
        auto& edit = edits[it->second];
        edit.varied |= edit.value != values[i];
        edit.value = values[i];
        continue;
      }
    }
    edits.push_back({fields[i], values[i]});
  }
  CompactHashEdit result{.count_ = expected_count, .payload_ = {}};
  std::size_t bytes = payload.size();
  for (std::size_t i = 0; i < reader->size(); ++i) {
    auto entry = reader->Next();
    if (!entry.ok()) return entry.status();
    std::size_t index = 0;
    if (fields.size() <= 1) {
      if (edits.empty() || entry->field_ != edits[0].field) continue;
    } else {
      const auto it = positions.find(entry->field_);
      if (it == positions.end()) continue;
      index = it->second;
    }
    auto& edit = edits[index];
    if (edit.found)
      return absl::DataLossError("duplicate requested Hash field");
    edit.found = true;
    result.changed_ |= edit.varied || edit.value != entry->value_;
    if (edit.value == entry->value_) continue;
    const auto begin =
        static_cast<std::size_t>(entry->field_.data() - payload.data()) - 8;
    const auto end = begin + 8 + entry->field_.size() + entry->value_.size();
    auto size =
        AppendHashEntrySize(bytes - (end - begin), edit.field.size(),
                            edit.value.size(), std::string{}.max_size());
    if (!size.ok()) return size.status();
    bytes = *size;
    splices.push_back({begin, end, index});
  }
  for (const auto& edit : edits) {
    if (edit.found) continue;
    auto size = AppendHashEntrySize(bytes, edit.field.size(), edit.value.size(),
                                    std::string{}.max_size());
    if (!size.ok()) return size.status();
    bytes = *size;
    ++result.added_;
  }
  if (result.added_ > UINT32_MAX - result.count_)
    return absl::OutOfRangeError("Hash element count overflow");
  result.count_ += result.added_;
  result.changed_ |= result.added_ != 0;
  if (bytes > max_bytes) return std::nullopt;
  if (!result.changed_) return result;

  auto& output = result.payload_;
  output.reserve(bytes);
  AppendU64(&output, kHashValueMagic);
  AppendU32(&output, kStorageFormatVersion);
  AppendU32(&output, kHashValueHeaderBytes);
  AppendU32(&output, result.count_);
  AppendU32(&output, 0);
  AppendU64(&output, bytes);
  auto append = [&](const Edit& edit) {
    AppendU32(&output, edit.field.size());
    AppendU32(&output, edit.value.size());
    output.append(edit.field);
    output.append(edit.value);
  };
  std::size_t offset = kHashValueHeaderBytes;
  for (const auto& splice : splices) {
    output.append(payload.substr(offset, splice.begin - offset));
    append(edits[splice.edit]);
    offset = splice.end;
  }
  output.append(payload.substr(offset));
  for (const auto& edit : edits)
    if (!edit.found) append(edit);
  return result;
}

absl::StatusOr<std::string> EncodeHashValue(const HashValue& value) {
  if (value.entries_.empty() ||
      value.entries_.size() > std::numeric_limits<std::uint32_t>::max()) {
    return absl::OutOfRangeError("invalid Hash element count");
  }
  std::string output;
  std::size_t bytes = kHashValueHeaderBytes;
  for (const HashEntry& entry : value.entries_) {
    auto next = AppendHashEntrySize(bytes, entry.field_.size(),
                                    entry.value_.size(), output.max_size());
    if (!next.ok()) return next.status();
    bytes = *next;
  }

  output.reserve(bytes);
  AppendU64(&output, kHashValueMagic);
  AppendU32(&output, kStorageFormatVersion);
  AppendU32(&output, kHashValueHeaderBytes);
  AppendU32(&output, static_cast<std::uint32_t>(value.entries_.size()));
  AppendU32(&output, 0);
  AppendU64(&output, bytes);
  for (const HashEntry& entry : value.entries_) {
    AppendU32(&output, static_cast<std::uint32_t>(entry.field_.size()));
    AppendU32(&output, static_cast<std::uint32_t>(entry.value_.size()));
    output.append(entry.field_);
    output.append(entry.value_);
  }
  return output;
}

}  // namespace lavik::storage
