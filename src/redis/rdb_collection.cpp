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

#include "lavik/rdb_collection.h"

#include <bit>
#include <cmath>
#include <cstring>
#include <limits>

#include "rdb_stream_encoder.h"

namespace lavik::rdb {
namespace {

std::size_t EncodeLength(char* bytes, std::uint64_t value) noexcept {
  if (value < 64) {
    bytes[0] = static_cast<char>(value);
    return 1;
  }
  if (value < 16384) {
    bytes[0] = static_cast<char>(0x40 | (value >> 8));
    bytes[1] = static_cast<char>(value);
    return 2;
  }
  const unsigned width = value <= UINT32_MAX ? 4 : 8;
  bytes[0] = static_cast<char>(width == 4 ? 0x80 : 0x81);
  for (unsigned i = 0; i < width; ++i)
    bytes[i + 1] = static_cast<char>(value >> (8 * (width - i - 1)));
  return width + 1;
}

void LittleEndian64(char* bytes, std::uint64_t value) noexcept {
  for (unsigned i = 0; i < 8; ++i)
    bytes[i] = static_cast<char>(value >> (8 * i));
}

}  // namespace

absl::StatusOr<CollectionFileEncoder> CollectionFileEncoder::Create(
    std::uint8_t db_id, std::string_view key, storage::ValueType type,
    std::uint64_t item_count, std::uint64_t expire_at_ms) {
  if (db_id >= storage::kLogicalDatabaseCount ||
      key.size() > storage::MaxKeyBytes() ||
      (item_count == 0 && type != storage::ValueType::kStream))
    return absl::InvalidArgumentError("invalid RDB collection metadata");
  unsigned opcode;
  switch (type) {
    case storage::ValueType::kList:
      opcode = 1;
      break;
    case storage::ValueType::kSet:
      opcode = 2;
      break;
    case storage::ValueType::kHash:
      opcode = 4;
      break;
    case storage::ValueType::kSortedSet:
      opcode = 5;
      break;
    case storage::ValueType::kStream:
      opcode = 21;  // STREAM_LISTPACKS_3
      break;
    default:
      return absl::InvalidArgumentError("unsupported RDB collection type");
  }
  CollectionFileEncoder result;
  result.type_ = type;
  result.key_ = key;
  result.expected_ = item_count;
  if (type == storage::ValueType::kStream)
    result.stream_ = std::make_shared<StreamFileEncoder>(item_count);
  auto* out = result.header_.data();
  *out++ = static_cast<char>(254);  // SELECTDB scopes this complete key.
  out += EncodeLength(out, db_id);
  if (expire_at_ms != 0) {
    *out++ = static_cast<char>(252);
    LittleEndian64(out, expire_at_ms);
    out += 8;
  }
  *out++ = static_cast<char>(opcode);
  out += EncodeLength(out, key.size());
  result.header_bytes_ = static_cast<std::size_t>(out - result.header_.data());
  return result;
}

absl::StatusOr<CollectionFileEncoder> CollectionFileEncoder::CreateDump(
    storage::ValueType type, std::uint64_t item_count) {
  auto result = Create(0, {}, type, item_count, 0);
  if (!result.ok()) return result.status();
  // SELECTDB and empty key framing belong to files, not DUMP values.
  result->header_[0] = result->header_[2];
  result->header_bytes_ = 1;
  result->dump_ = true;
  return result;
}

absl::Status CollectionFileEncoder::StartPage(
    const storage::CollectionPage& page) {
  if (header_phase_ != 3 || page_ != nullptr || saw_last_)
    return absl::FailedPreconditionError("RDB collection cursor is not ready");
  if (page.value_type_ != type_ || cursor_ == UINT64_MAX ||
      page.next_cursor_ != cursor_ + 1 ||
      (type_ != storage::ValueType::kHash && !page.fields_.empty()) ||
      (type_ != storage::ValueType::kSortedSet &&
       !page.scored_members_.empty()) ||
      (type_ != storage::ValueType::kList &&
       type_ != storage::ValueType::kSet &&
       type_ != storage::ValueType::kStream && !page.elements_.empty()))
    return absl::InvalidArgumentError("invalid RDB collection page");
  if (stream_) {
    auto status = stream_->StartPage(page);
    if (!status.ok()) return status;
    cursor_ = page.next_cursor_;
    saw_last_ = page.done_;
    return absl::OkStatus();
  }
  if (page.size() > expected_ - accepted_ ||
      (page.done_ && page.size() != expected_ - accepted_))
    return absl::DataLossError("RDB collection cardinality mismatch");
  for (const auto& field : page.fields_)
    if (field.field_.size() > storage::kMaxStringBytes ||
        field.value_.size() > storage::kMaxStringBytes)
      return absl::OutOfRangeError("RDB Hash entry exceeds string limit");
  for (const auto& element : page.elements_)
    if (element.size() > storage::kMaxStringBytes)
      return absl::OutOfRangeError("RDB member exceeds string limit");
  for (const auto& member : page.scored_members_)
    if (member.member_.size() > storage::kMaxStringBytes ||
        std::isnan(member.score_))
      return absl::InvalidArgumentError("invalid RDB Sorted Set member");
  // Validation precedes any page emission. Caller cancellation can discard
  // the incomplete temp file; it cannot turn a malformed page into success.
  accepted_ += page.size();
  cursor_ = page.next_cursor_;
  saw_last_ = page.done_;
  page_ = &page;
  entry_ = 0;
  entry_phase_ = 0;
  return absl::OkStatus();
}

std::string_view CollectionFileEncoder::Length(std::uint64_t value) noexcept {
  return {metadata_.data(), EncodeLength(metadata_.data(), value)};
}

std::optional<std::string_view> CollectionFileEncoder::Next() noexcept {
  if (header_phase_ == 0) {
    ++header_phase_;
    return std::string_view(header_.data(), header_bytes_);
  }
  if (header_phase_ == 1) {
    ++header_phase_;
    if (!dump_) return key_;
  }
  if (header_phase_ == 2) {
    ++header_phase_;
    return Length(expected_);
  }
  if (stream_) return stream_->Next();
  if (page_ == nullptr) return std::nullopt;
  if (entry_ == page_->size()) {
    page_ = nullptr;
    return std::nullopt;
  }
  if (type_ == storage::ValueType::kHash) {
    const auto& field = page_->fields_[entry_];
    switch (entry_phase_++) {
      case 0:
        return Length(field.field_.size());
      case 1:
        return field.field_;
      case 2:
        return Length(field.value_.size());
      default:
        entry_phase_ = 0;
        ++entry_;
        return field.value_;
    }
  }
  if (type_ == storage::ValueType::kSortedSet) {
    const auto& member = page_->scored_members_[entry_];
    switch (entry_phase_++) {
      case 0:
        return Length(member.member_.size());
      case 1:
        return member.member_;
      default:
        entry_phase_ = 0;
        ++entry_;
        LittleEndian64(metadata_.data(),
                       std::bit_cast<std::uint64_t>(member.score_));
        return std::string_view(metadata_.data(), 8);
    }
  }
  const auto& element = page_->elements_[entry_];
  if (entry_phase_++ == 0) return Length(element.size());
  entry_phase_ = 0;
  ++entry_;
  return element;
}

absl::Status CollectionFileEncoder::Finish() const {
  if (stream_) return stream_->Finish();
  if (header_phase_ != 3 || page_ != nullptr || !saw_last_ ||
      accepted_ != expected_)
    return absl::FailedPreconditionError("RDB collection stream is incomplete");
  return absl::OkStatus();
}

}  // namespace lavik::rdb
