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

#include "lavik/storage/detail/collection_compact_stream.h"

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstring>
#include <limits>
#include <new>

#include "lavik/storage/detail/grouped_hash.h"
#include "lavik/storage/detail/stream_records.h"

namespace lavik::storage {
namespace {

bool HashWire(ValueType type) {
  return type == ValueType::kHash || type == ValueType::kSet;
}

std::size_t HeaderBytes(ValueType type) { return HashWire(type) ? 32 : 8; }

std::size_t EntryFraming(ValueType type) {
  if (HashWire(type)) return 8;
  return type == ValueType::kSortedSet ? 12 : 4;
}

void Put(char* output, std::uint64_t value, unsigned width) {
  for (unsigned i = 0; i < width; ++i)
    output[i] = static_cast<char>(value >> (8 * i));
}

std::uint64_t Get(const char* input, unsigned width) {
  std::uint64_t value = 0;
  for (unsigned i = 0; i < width; ++i)
    value |= std::uint64_t(static_cast<unsigned char>(input[i])) << (8 * i);
  return value;
}

absl::Status ValidateTotals(ValueType type, std::uint64_t count,
                            std::uint64_t bytes) {
  if ((!HashWire(type) && type != ValueType::kList &&
       type != ValueType::kSortedSet) ||
      count == 0 || count > std::numeric_limits<std::uint32_t>::max()) {
    return absl::InvalidArgumentError("invalid compact stream type/count");
  }
  const auto header = HeaderBytes(type);
  const auto minimum = EntryFraming(type);
  // These products fit uint64_t: count is uint32_t and each entry is at most
  // 1 GiB. Reject impossible metadata without reserving any payload memory.
  const std::uint64_t maximum = type == ValueType::kHash
                                    ? kMaxRecordPayloadBytes -
                                          kHashGroupHeaderBytes -
                                          kHashValueHeaderBytes
                                    : kMaxStringBytes + minimum;
  if (bytes < header + count * minimum || bytes > header + count * maximum)
    return absl::InvalidArgumentError("impossible compact stream byte length");
  return absl::OkStatus();
}

absl::Status ValidateEntry(ValueType type, std::uint64_t first,
                           std::uint64_t second, double score = 0) {
  if (first > kMaxStringBytes || second > kMaxStringBytes ||
      (type == ValueType::kSet && second != 0) || std::isnan(score)) {
    return absl::DataLossError("invalid compact stream entry length/score");
  }
  if (HashWire(type) &&
      first + second + 8 + kHashGroupHeaderBytes + kHashValueHeaderBytes >
          kMaxRecordPayloadBytes) {
    return absl::DataLossError("Hash entry cannot fit a grouped payload");
  }
  return absl::OkStatus();
}

}  // namespace

absl::StatusOr<CollectionCompactEncoder> CollectionCompactEncoder::Create(
    ValueType type, std::uint64_t count, std::uint64_t bytes) {
  auto valid =
      type == ValueType::kStream
          ? (count != 0 && count <= UINT32_MAX && bytes >= 56
                 ? absl::OkStatus()
                 : absl::InvalidArgumentError("invalid Stream record totals"))
          : ValidateTotals(type, count, bytes);
  if (!valid.ok()) return valid;
  CollectionCompactEncoder result;
  result.type_ = type;
  result.total_count_ = count;
  result.total_bytes_ = bytes;
  result.header_bytes_ = type == ValueType::kStream ? 0 : HeaderBytes(type);
  result.supplied_bytes_ = result.header_bytes_;
  char* out = result.header_.data();
  if (type == ValueType::kStream) {
    result.header_pending_ = false;
  } else if (HashWire(type)) {
    Put(out, kHashValueMagic, 8);
    Put(out + 8, kStorageFormatVersion, 4);
    Put(out + 12, kHashValueHeaderBytes, 4);
    Put(out + 16, count, 4);
    Put(out + 24, bytes, 8);
  } else {
    std::memcpy(out, type == ValueType::kList ? "LVL1" : "LZS1", 4);
    Put(out + 4, count, 4);
  }
  return result;
}

absl::StatusOr<std::uint64_t> CollectionCompactEncoder::MeasurePage(
    const CollectionPage& page) {
  const auto type = page.value_type_;
  if ((!HashWire(type) && type != ValueType::kList &&
       type != ValueType::kSortedSet && type != ValueType::kStream) ||
      (type != ValueType::kHash && !page.fields_.empty()) ||
      (type != ValueType::kSet && type != ValueType::kList &&
       type != ValueType::kStream && !page.elements_.empty()) ||
      (type != ValueType::kSortedSet && !page.scored_members_.empty())) {
    return absl::InvalidArgumentError(
        "compact page has inconsistent containers");
  }
  std::uint64_t result = 0;
  for (std::size_t i = 0; i < page.size(); ++i) {
    if (type == ValueType::kStream) {
      auto payload = StreamRecordPayload(page.elements_[i]);
      if (!payload.ok()) return payload.status();
      if (payload->size() > UINT64_MAX - result)
        return absl::OutOfRangeError("Stream page size overflow");
      result += payload->size();
      continue;
    }
    const std::uint64_t first =
        type == ValueType::kHash        ? page.fields_[i].field_.size()
        : type == ValueType::kSortedSet ? page.scored_members_[i].member_.size()
                                        : page.elements_[i].size();
    const std::uint64_t second =
        type == ValueType::kHash ? page.fields_[i].value_.size() : 0;
    auto valid = ValidateEntry(
        type, first, second,
        type == ValueType::kSortedSet ? page.scored_members_[i].score_ : 0);
    if (!valid.ok()) return valid;
    const auto bytes =
        (type == ValueType::kStream ? 0 : EntryFraming(type)) + first + second;
    if (bytes > std::numeric_limits<std::uint64_t>::max() - result)
      return absl::OutOfRangeError("compact page byte length overflow");
    result += bytes;
  }
  return result;
}

absl::Status CollectionCompactEncoder::StartPage(const CollectionPage& page) {
  if (page_ != nullptr)
    return absl::FailedPreconditionError(
        "previous compact page is not drained");
  if (page.value_type_ != type_)
    return absl::InvalidArgumentError(
        "compact page type does not match stream");
  auto measured = MeasurePage(page);
  if (!measured.ok()) return measured.status();
  if (page.size() > total_count_ - supplied_count_ ||
      *measured > total_bytes_ - supplied_bytes_)
    return absl::DataLossError("compact page exceeds declared stream totals");
  supplied_count_ += page.size();
  supplied_bytes_ += *measured;
  page_ = page.size() == 0 ? nullptr : &page;
  entry_ = 0;
  phase_ = 0;
  return absl::OkStatus();
}

std::optional<std::string_view> CollectionCompactEncoder::Next() noexcept {
  std::string_view output;
  if (header_pending_) {
    header_pending_ = false;
    output = {header_.data(), header_bytes_};
  } else if (page_ == nullptr) {
    return std::nullopt;
  } else if (type_ == ValueType::kStream) {
    output = *StreamRecordPayload(page_->elements_[entry_]);
    if (++entry_ == page_->size()) page_ = nullptr;
  } else {
    const bool hash = HashWire(type_);
    const std::string& first = type_ == ValueType::kHash
                                   ? page_->fields_[entry_].field_
                               : type_ == ValueType::kSortedSet
                                   ? page_->scored_members_[entry_].member_
                                   : page_->elements_[entry_];
    if (phase_ == 0) {
      if (hash) {
        Put(framing_.data(), first.size(), 4);
        Put(framing_.data() + 4,
            type_ == ValueType::kHash ? page_->fields_[entry_].value_.size()
                                      : 0,
            4);
      } else if (type_ == ValueType::kSortedSet) {
        Put(framing_.data(),
            std::bit_cast<std::uint64_t>(page_->scored_members_[entry_].score_),
            8);
        Put(framing_.data() + 8, first.size(), 4);
      } else {
        Put(framing_.data(), first.size(), 4);
      }
      output = {framing_.data(), EntryFraming(type_)};
    } else if (phase_ == 1) {
      output = first;
    } else {
      output = type_ == ValueType::kHash
                   ? std::string_view(page_->fields_[entry_].value_)
                   : std::string_view{};
    }
    if (++phase_ == (hash ? 3U : 2U)) {
      phase_ = 0;
      if (++entry_ == page_->size()) page_ = nullptr;
    }
  }
  emitted_bytes_ += output.size();
  return output;
}

absl::Status CollectionCompactEncoder::Finish() const {
  if (header_pending_ || page_ != nullptr || supplied_count_ != total_count_ ||
      supplied_bytes_ != total_bytes_ || emitted_bytes_ != total_bytes_)
    return absl::DataLossError("incomplete compact stream encoding");
  return absl::OkStatus();
}

absl::StatusOr<CollectionCompactDecoder> CollectionCompactDecoder::Create(
    ValueType type, std::uint64_t count, std::uint64_t bytes,
    Admission admission) {
  auto valid = ValidateTotals(type, count, bytes);
  if (!valid.ok()) return valid;
  CollectionCompactDecoder result;
  result.type_ = type;
  result.total_count_ = count;
  result.total_bytes_ = bytes;
  result.page_.value_type_ = type;
  result.admission_ = std::move(admission);
  return result;
}

absl::Status CollectionCompactDecoder::Fail(absl::Status status) {
  status_ = std::move(status);
  return status_;
}

absl::Status CollectionCompactDecoder::ReadHeader() {
  const char* in = framing_.data();
  if (HashWire(type_)) {
    if (Get(in, 8) != kHashValueMagic ||
        Get(in + 8, 4) != kStorageFormatVersion ||
        Get(in + 12, 4) != kHashValueHeaderBytes ||
        Get(in + 16, 4) != total_count_ || Get(in + 20, 4) != 0 ||
        Get(in + 24, 8) != total_bytes_)
      return absl::DataLossError("invalid compact Hash/Set stream header");
  } else if (std::memcmp(in, type_ == ValueType::kList ? "LVL1" : "LZS1", 4) !=
                 0 ||
             Get(in + 4, 4) != total_count_) {
    return absl::DataLossError("invalid ordered compact stream header");
  }
  framing_used_ = 0;
  stage_ = Stage::kEntryHeader;
  return absl::OkStatus();
}

absl::Status CollectionCompactDecoder::ReadEntryHeader() {
  const char* in = framing_.data();
  score_ =
      type_ == ValueType::kSortedSet ? std::bit_cast<double>(Get(in, 8)) : 0;
  first_bytes_ = Get(in + (type_ == ValueType::kSortedSet ? 8 : 0), 4);
  second_bytes_ = HashWire(type_) ? Get(in + 4, 4) : 0;
  auto valid = ValidateEntry(type_, first_bytes_, second_bytes_, score_);
  if (!valid.ok()) return valid;
  const std::uint64_t body = first_bytes_ + second_bytes_;
  const auto minimum_remaining =
      (total_count_ - parsed_count_ - 1) * EntryFraming(type_);
  if (minimum_remaining > total_bytes_ - consumed_bytes_ ||
      body > total_bytes_ - consumed_bytes_ - minimum_remaining)
    return absl::DataLossError("compact entry exceeds remaining stream bytes");
  entry_bytes_ = EntryFraming(type_) + body;
  framing_used_ = 0;
  stage_ = Stage::kFirst;
  // Stop before assembling an oversized entry alongside preceding entries.
  // The tiny consumed framing stays in the cursor across the caller's await.
  if (page_.size() != 0 &&
      entry_bytes_ > kCollectionStreamPageBytes - page_bytes_)
    ready_ = true;
  return absl::OkStatus();
}

absl::Status CollectionCompactDecoder::Admit(std::size_t bytes) {
  if (!admission_) return absl::OkStatus();
  if (bytes > std::numeric_limits<std::size_t>::max() - admission_count_.bytes_)
    return absl::ResourceExhaustedError("compact stream admission overflow");
  auto status = admission_(bytes);
  if (status.ok()) admission_count_.bytes_ += bytes;
  return status;
}

absl::Status CollectionCompactDecoder::CompleteEntry() {
  if (parsed_count_ + 1 == total_count_ && consumed_bytes_ != total_bytes_)
    return absl::DataLossError(
        "compact entry count ends before declared bytes");
  auto grow = [this](auto& entries) -> absl::Status {
    if (entries.size() != entries.capacity()) return absl::OkStatus();
    const auto capacity = std::max<std::size_t>(1, entries.capacity() * 2);
    auto status = Admit(capacity * sizeof(entries[0]));
    if (!status.ok()) return status;
    entries.reserve(capacity);
    return absl::OkStatus();
  };
  auto status = type_ == ValueType::kHash        ? grow(page_.fields_)
                : type_ == ValueType::kSortedSet ? grow(page_.scored_members_)
                                                 : grow(page_.elements_);
  if (!status.ok()) return status;
  if (type_ == ValueType::kHash) {
    page_.fields_.push_back({std::move(first_), std::move(second_)});
  } else if (type_ == ValueType::kSortedSet) {
    page_.scored_members_.push_back({std::move(first_), score_});
  } else {
    page_.elements_.push_back(std::move(first_));
  }
  first_ = std::string{};
  second_ = std::string{};
  first_used_ = 0;
  second_used_ = 0;
  ++parsed_count_;
  page_bytes_ += entry_bytes_;
  stage_ = parsed_count_ == total_count_ ? Stage::kDone : Stage::kEntryHeader;
  ready_ = stage_ == Stage::kDone || page_bytes_ >= kCollectionStreamPageBytes;
  return absl::OkStatus();
}

absl::StatusOr<std::size_t> CollectionCompactDecoder::Consume(
    std::string_view input) {
  if (!status_.ok()) return status_;
  std::size_t used = 0;
  try {
    while (!ready_) {
      if (stage_ == Stage::kDone) {
        if (used != input.size())
          return Fail(absl::DataLossError("trailing compact stream bytes"));
        break;
      }
      if (stage_ == Stage::kFirst || stage_ == Stage::kSecond) {
        auto& output = stage_ == Stage::kFirst ? first_ : second_;
        auto& filled = stage_ == Stage::kFirst ? first_used_ : second_used_;
        const auto need =
            stage_ == Stage::kFirst ? first_bytes_ : second_bytes_;
        if (output.size() != need) {
          // Allocate the checked individual string exactly once. In particular,
          // an attacker-controlled aggregate length never drives a reserve.
          auto status = Admit(need + 1);
          if (!status.ok()) return Fail(status);
          output = std::string(need, '\0');
        }
        const auto copy = std::min(need - filled, input.size() - used);
        if (copy != 0)
          std::memcpy(output.data() + filled, input.data() + used, copy);
        filled += copy;
        consumed_bytes_ += copy;
        used += copy;
        if (filled != need) break;
        if (stage_ == Stage::kFirst && HashWire(type_)) {
          stage_ = Stage::kSecond;
        } else {
          auto valid = CompleteEntry();
          if (!valid.ok()) return Fail(valid);
        }
        continue;
      }
      if (used == input.size()) break;
      const auto need =
          stage_ == Stage::kHeader ? HeaderBytes(type_) : EntryFraming(type_);
      const auto copy = std::min(need - framing_used_, input.size() - used);
      if (copy > total_bytes_ - consumed_bytes_)
        return Fail(
            absl::DataLossError("compact framing exceeds stream bytes"));
      std::memcpy(framing_.data() + framing_used_, input.data() + used, copy);
      framing_used_ += copy;
      consumed_bytes_ += copy;
      used += copy;
      if (framing_used_ == need) {
        auto valid =
            stage_ == Stage::kHeader ? ReadHeader() : ReadEntryHeader();
        if (!valid.ok()) return Fail(valid);
      }
    }
  } catch (const std::bad_alloc&) {
    return Fail(absl::ResourceExhaustedError("compact stream page allocation"));
  }
  return used;
}

absl::StatusOr<CollectionPage> CollectionCompactDecoder::TakePage(
    std::size_t* transferred_admission) {
  if (!status_.ok()) return status_;
  if (!ready_)
    return absl::FailedPreconditionError("compact stream page is not ready");
  if (admission_ && transferred_admission == nullptr)
    return absl::FailedPreconditionError(
        "compact page admission needs an owner");
  auto result = std::move(page_);
  result.next_cursor_ = ++next_cursor_;
  result.done_ = parsed_count_ == total_count_;
  page_ = CollectionPage{.value_type_ = type_};
  page_bytes_ = 0;
  ready_ = false;
  if (transferred_admission != nullptr)
    *transferred_admission = admission_count_.bytes_;
  admission_count_.bytes_ = 0;
  return result;
}

absl::Status CollectionCompactDecoder::Finish() {
  if (!status_.ok()) return status_;
  if (stage_ != Stage::kDone || parsed_count_ != total_count_ ||
      consumed_bytes_ != total_bytes_)
    return Fail(absl::DataLossError("truncated compact stream at EOF"));
  return absl::OkStatus();
}

}  // namespace lavik::storage
