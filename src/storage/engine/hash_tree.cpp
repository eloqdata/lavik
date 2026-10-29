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

#include <charconv>
#include <cmath>
#include <cstdlib>
#include <random>
#include <set>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/container/inlined_vector.h"
#include "absl/hash/hash.h"
#include "impl.h"
#include "lavik/error_detail.h"
#include "lavik/glob.h"
#include "lavik/memory.h"
#include "lavik/random_sample.h"
#include "lavik/redis_parse.h"
#include "lavik/storage/detail/grouped/scratch.h"
#include "lavik/storage/detail/hash_read.h"

namespace lavik::storage {

namespace {

// Positions survive vector reallocation, including moves of SSO field names.
// Resolve them through the vector object instead of retaining string_views or
// duplicating every field/member. Fields and positions must remain unchanged
// until this command-local index is destroyed; values may be replaced.
struct FieldPositionHash {
  using is_transparent = void;
  const std::vector<HashEntry>* entries;
  std::size_t operator()(std::string_view field) const {
    return absl::Hash<std::string_view>{}(field);
  }
  std::size_t operator()(std::size_t position) const {
    return (*this)((*entries)[position].field_);
  }
};

struct FieldPositionEqual {
  using is_transparent = void;
  const std::vector<HashEntry>* entries;
  bool operator()(std::size_t left, std::size_t right) const {
    return (*entries)[left].field_ == (*entries)[right].field_;
  }
  bool operator()(std::size_t left, std::string_view right) const {
    return (*entries)[left].field_ == right;
  }
  bool operator()(std::string_view left, std::size_t right) const {
    return left == (*entries)[right].field_;
  }
};

bool IsWrite(const HashOperation& operation) {
  return operation.kind_ == HashOperationKind::kSet ||
         operation.kind_ == HashOperationKind::kReplaceOnly ||
         operation.kind_ == HashOperationKind::kSetIfAbsent ||
         operation.kind_ == HashOperationKind::kDelete ||
         operation.kind_ == HashOperationKind::kPopRandom ||
         operation.kind_ == HashOperationKind::kIncrementInteger ||
         operation.kind_ == HashOperationKind::kIncrementFloat;
}

bool IsPointOperation(HashOperationKind kind) {
  switch (kind) {
    case HashOperationKind::kSet:
    case HashOperationKind::kSetIfAbsent:
    case HashOperationKind::kDelete:
    case HashOperationKind::kIncrementInteger:
    case HashOperationKind::kIncrementFloat:
    case HashOperationKind::kGet:
    case HashOperationKind::kGetMany:
    case HashOperationKind::kExists:
    case HashOperationKind::kStringLength:
      return true;
    default:
      return false;
  }
}

bool NeedsGroupedHash(const HashValue& value) {
  // Small hashes keep the compact path. Crossing the threshold publishes a
  // complete grouped graph atomically; grouped publication separately decides
  // whether a shrinking after-image can return to compact storage.
  std::uint64_t bytes = kHashValueHeaderBytes;
  for (const auto& entry : value.entries_) {
    bytes += 8 + entry.field_.size() + entry.value_.size();
    if (bytes >= kCollectionPromotionBytes) return true;
  }
  return false;
}

bool IsCompactPointRead(HashOperationKind kind) {
  // Set members use Hash fields with empty values, so membership reads share
  // the same borrowed lookup and early-completion rules.
  return kind == HashOperationKind::kGet ||
         kind == HashOperationKind::kGetMany ||
         kind == HashOperationKind::kExists ||
         kind == HashOperationKind::kStringLength;
}

absl::StatusOr<HashResult> ReadCompactHashFields(
    std::string_view payload, std::size_t count,
    const HashOperation& operation) {
  HashResult result;
  result.key_exists_ = true;
  result.length_ = count;
  if (operation.fields_.empty()) {
    auto status = FindCompactHashFields(payload, count, {});
    if (!status.ok()) return status;
    return result;
  }
  if (operation.kind_ == HashOperationKind::kExists ||
      operation.kind_ == HashOperationKind::kStringLength ||
      operation.fields_.size() == 1) {
    auto matched =
        FindCompactHashField(payload, count, operation.fields_.front());
    if (!matched.ok()) return matched.status();
    if (operation.kind_ == HashOperationKind::kExists)
      result.integer_ = matched->has_value();
    else if (operation.kind_ == HashOperationKind::kStringLength)
      result.integer_ = *matched ? (**matched).size() : 0;
    else {
      auto status = RetainHashLookupValue(result, *matched);
      if (!status.ok()) return status;
    }
    return result;
  }
  const auto size = operation.fields_.size();
  GroupedScratchBudget budget;
  if (size > SIZE_MAX / sizeof(HashFieldLookup))
    return MemoryLimitError("OOM Hash lookup plan is too large");
  auto added = budget.AddBytes(size * sizeof(HashFieldLookup));
  if (!added.ok()) return added;
  auto scratch = budget.Reserve(1);
  if (!scratch.ok()) return scratch.status();
  std::vector<HashFieldLookup> requests;
  requests.reserve(size);
  for (std::size_t i = 0; i < size; ++i)
    requests.push_back({.group_ = {},
                        .field_ = operation.fields_[i],
                        .result_index_ = i,
                        .value_ = std::nullopt});
  std::sort(requests.begin(), requests.end(),
            [](const auto& a, const auto& b) { return a.field_ < b.field_; });
  auto status = FindCompactHashFields(payload, count, requests);
  if (!status.ok()) return status;
  if (size > (SIZE_MAX - sizeof(HashResult)) / sizeof(result.values_[0]))
    return MemoryLimitError("OOM Hash output is too large");
  auto output =
      TryReserveMemory(sizeof(HashResult) + size * sizeof(result.values_[0]));
  if (!output) {
    RecordMemoryRejection();
    return MemoryLimitError("OOM Hash output retention");
  }
  result.values_.resize(size);
  const auto retained = sizeof(HashResult) +
                        result.values_.capacity() * sizeof(result.values_[0]);
  if (retained > output->bytes())
    return MemoryLimitError("OOM Hash output retention");
  result.retained_charge_.Adopt(&*output, retained);
  status = RetainHashGroupValues(result, requests);
  if (!status.ok()) return status;
  return result;
}

absl::StatusOr<HashResult> ReadHashScanResult(std::string_view payload,
                                              std::size_t count,
                                              const HashOperation& operation,
                                              ValueType type,
                                              const DigestSeed* seed = nullptr,
                                              std::uint64_t next_cursor = 0) {
  HashResult result;
  result.key_exists_ = true;
  result.length_ = count;
  result.cursor_ = next_cursor;
  if (count == 0) return result;
  auto reader = HashValueReader::Open(payload);
  if (!reader.ok()) return reader.status();
  if (reader->size() != count)
    return absl::DataLossError(
        "Hash element count does not match record metadata");
  struct ScanEntry {
    Digest digest_;
    HashEntryView entry_;
  };
  if (count > SIZE_MAX / sizeof(ScanEntry))
    return MemoryLimitError("OOM Hash scan index is too large");
  auto scratch = TryReserveMemory(count * sizeof(ScanEntry));
  if (!scratch) {
    RecordMemoryRejection();
    return MemoryLimitError("OOM Hash scan index");
  }
  // The cursor needs digest order, not owned fields. Keep only views under
  // this page's lease and copy the matched reply after selecting its range.
  std::vector<ScanEntry> entries;
  entries.reserve(count);
  for (std::size_t i = 0; i < count; ++i) {
    auto entry = reader->Next();
    if (!entry.ok()) return entry.status();
    entries.push_back({seed ? ComputeDigest(entry->field_, *seed)
                            : ComputeDigest(entry->field_),
                       *entry});
  }
  std::sort(entries.begin(), entries.end(), [](const auto& a, const auto& b) {
    return a.digest_ != b.digest_ ? a.digest_.value_ < b.digest_.value_
                                  : a.entry_.field_ < b.entry_.field_;
  });
  const auto first =
      std::lower_bound(entries.begin(), entries.end(), operation.cursor_,
                       [](const auto& entry, std::uint64_t cursor) {
                         return ScanCursorPrefix(entry.digest_) < cursor;
                       });
  const auto begin = static_cast<std::size_t>(first - entries.begin());
  auto end =
      begin + std::min<std::uint64_t>(operation.scan_count_, count - begin);
  while (end < count && end != begin &&
         entries[end].digest_ == entries[end - 1].digest_)
    ++end;
  if (end < count) result.cursor_ = ScanCursorPrefix(entries[end].digest_);
  const std::size_t width = type == ValueType::kHash ? 2 : 1;
  std::size_t matched = 0;
  std::size_t retained = sizeof(HashResult);
  const auto matches = [&](const ScanEntry& entry) {
    return operation.match_ == "*" ||
           lavik::RedisGlobMatch(operation.match_, entry.entry_.field_);
  };
  for (auto i = begin; i < end; ++i) {
    if (!matches(entries[i])) continue;
    entries[begin + matched] = entries[i];
    const auto& entry = entries[begin + matched++].entry_;
    for (const auto bytes :
         {width * sizeof(std::optional<std::string>),
          std::max(entry.field_.size(), std::string{}.capacity()) + 1,
          width == 2
              ? std::max(entry.value_.size(), std::string{}.capacity()) + 1
              : std::size_t{0}}) {
      if (bytes > SIZE_MAX - retained)
        return MemoryLimitError("OOM Hash scan output is too large");
      retained += bytes;
    }
  }
  auto output = TryReserveMemory(retained);
  if (!output) {
    RecordMemoryRejection();
    return MemoryLimitError("OOM Hash scan output");
  }
  result.values_.reserve(matched * width);
  for (auto i = begin; i < begin + matched; ++i) {
    result.values_.emplace_back(std::in_place, entries[i].entry_.field_);
    if (width == 2)
      result.values_.emplace_back(std::in_place, entries[i].entry_.value_);
  }
  retained = sizeof(HashResult) +
             result.values_.capacity() * sizeof(result.values_[0]);
  for (const auto& value : result.values_) retained += value->capacity() + 1;
  if (retained > output->bytes())
    return MemoryLimitError("OOM Hash scan output");
  result.retained_charge_.Adopt(&*output, retained);
  return result;
}

absl::StatusOr<HashResult> ReadCompactHashRandom(
    std::string_view payload, std::size_t count,
    const HashOperation& operation) {
  HashResult result;
  result.key_exists_ = true;
  result.length_ = count;
  if (operation.count_ == std::numeric_limits<std::int64_t>::min())
    return absl::OutOfRangeError("value is out of range");
  const bool repeat = operation.count_provided_ && operation.count_ < 0;
  auto requested = !operation.count_provided_
                       ? std::uint64_t{1}
                       : static_cast<std::uint64_t>(repeat ? -operation.count_
                                                           : operation.count_);
  if (!repeat) requested = std::min<std::uint64_t>(requested, count);
  if (requested == 0 || count == 0) return result;
  if (requested > SIZE_MAX / (sizeof(HashRankLookup) + sizeof(std::uint64_t)))
    return MemoryLimitError("OOM Hash random rank index is too large");
  auto scratch = TryReserveMemory(
      requested * (sizeof(HashRankLookup) + sizeof(std::uint64_t)));
  if (!scratch) {
    RecordMemoryRejection();
    return MemoryLimitError("OOM Hash random rank index");
  }
  std::vector<HashRankLookup> requests;
  requests.reserve(requested);
  if (repeat) {
    for (std::size_t i = 0; i < requested; ++i)
      requests.push_back({.rank_ = RandomRank(count, RandomSampleGenerator()),
                          .result_index_ = i});
    std::sort(requests.begin(), requests.end(),
              [](const auto& a, const auto& b) { return a.rank_ < b.rank_; });
  } else {
    auto ranks = SampleUniqueRandomRanks(count, requested, true,
                                         RandomSampleGenerator());
    for (std::size_t i = 0; i < ranks.size(); ++i)
      requests.push_back({.rank_ = ranks[i], .result_index_ = i});
  }
  auto status = FindCompactHashRanks(payload, count, requests);
  if (!status.ok()) return status;
  const std::size_t width = operation.with_values_ ? 2 : 1;
  if (requested >
      (SIZE_MAX - sizeof(HashResult)) / (width * sizeof(result.values_[0])))
    return MemoryLimitError("OOM Hash random output is too large");
  auto output = TryReserveMemory(sizeof(HashResult) +
                                 requested * width * sizeof(result.values_[0]));
  if (!output) {
    RecordMemoryRejection();
    return MemoryLimitError("OOM Hash random output");
  }
  result.values_.resize(requested * width);
  const auto retained = sizeof(HashResult) +
                        result.values_.capacity() * sizeof(result.values_[0]);
  if (retained > output->bytes())
    return MemoryLimitError("OOM Hash random output");
  result.retained_charge_.Adopt(&*output, retained);
  status = RetainHashRankValues(result, requests, operation.with_values_);
  if (!status.ok()) return status;
  return result;
}

absl::StatusOr<HashResult> ReadPackedHashResult(HashValueReader reader,
                                                std::size_t count, bool fields,
                                                bool values) {
  const std::size_t width = std::size_t{fields} + std::size_t{values};
  std::size_t payload_bytes = 0;
  auto inspect = reader;
  for (std::size_t i = 0; i < count; ++i) {
    auto entry = inspect.Next();
    if (!entry.ok()) return entry.status();
    for (const auto bytes : {fields ? entry->field_.size() : std::size_t{0},
                             values ? entry->value_.size() : std::size_t{0}}) {
      if (bytes > SIZE_MAX - payload_bytes)
        return MemoryLimitError("OOM Hash snapshot is too large");
      payload_bytes += bytes;
    }
  }
  constexpr auto overhead =
      sizeof(HashResult) + sizeof(HashReadSnapshot) + sizeof(std::string);
  const auto block_bytes =
      std::max(payload_bytes, std::string{}.capacity()) + 1;
  if (block_bytes < payload_bytes || block_bytes > SIZE_MAX - overhead ||
      count > (SIZE_MAX - overhead - block_bytes) /
                  (width * sizeof(HashReadSnapshot::Slice)))
    return MemoryLimitError("OOM Hash snapshot is too large");
  auto output = TryReserveMemory(
      overhead + block_bytes + count * width * sizeof(HashReadSnapshot::Slice));
  if (!output) {
    RecordMemoryRejection();
    return MemoryLimitError("OOM Hash snapshot retention");
  }
  HashResult result;
  result.key_exists_ = true;
  result.length_ = count;
  result.snapshot_ = std::make_unique<HashReadSnapshot>();
  auto& snapshot = *result.snapshot_;
  snapshot.blocks_.resize(1);
  snapshot.values_.reserve(count * width);
  auto& block = snapshot.blocks_.front();
  block = std::string(payload_bytes, '\0');
  std::size_t offset = 0;
  // Offsets survive SSO moves and vector relocation. The snapshot owns one
  // projected payload block, not a string allocation for every field/value.
  const auto append = [&](std::string_view value) {
    snapshot.values_.push_back(
        {.block_ = 0, .offset_ = offset, .size_ = value.size()});
    if (!value.empty())
      std::memcpy(block.data() + offset, value.data(), value.size());
    offset += value.size();
  };
  for (std::size_t i = 0; i < count; ++i) {
    auto entry = reader.Next();
    if (!entry.ok()) return entry.status();
    if (fields) append(entry->field_);
    if (values) append(entry->value_);
  }
  const auto retained =
      sizeof(HashResult) + sizeof(HashReadSnapshot) +
      snapshot.blocks_.capacity() * sizeof(std::string) +
      snapshot.values_.capacity() * sizeof(HashReadSnapshot::Slice) +
      block.capacity() + 1;
  if (retained > output->bytes())
    return MemoryLimitError("OOM Hash snapshot retention");
  result.retained_charge_.Adopt(&*output, retained);
  return result;
}

absl::StatusOr<HashResult> ReadMissingHashResult(
    const HashOperation& operation) {
  HashResult result;
  if ((operation.kind_ != HashOperationKind::kGet &&
       operation.kind_ != HashOperationKind::kGetMany) ||
      operation.fields_.empty())
    return result;
  if (operation.fields_.size() >
      (SIZE_MAX - sizeof(HashResult)) / sizeof(result.values_[0]))
    return MemoryLimitError("OOM Hash output is too large");
  auto output =
      TryReserveMemory(sizeof(HashResult) +
                       operation.fields_.size() * sizeof(result.values_[0]));
  if (!output) {
    RecordMemoryRejection();
    return MemoryLimitError("OOM Hash output retention");
  }
  result.values_.resize(operation.fields_.size());
  const auto retained = sizeof(HashResult) +
                        result.values_.capacity() * sizeof(result.values_[0]);
  if (retained > output->bytes())
    return MemoryLimitError("OOM Hash output retention");
  result.retained_charge_.Adopt(&*output, retained);
  return result;
}

absl::StatusOr<HashResult> ReadEncodedHash(std::string_view payload,
                                           std::size_t count,
                                           const HashOperation& operation,
                                           ValueType type) {
  if (operation.kind_ == HashOperationKind::kGetAll ||
      operation.kind_ == HashOperationKind::kKeys ||
      operation.kind_ == HashOperationKind::kValues ||
      operation.kind_ == HashOperationKind::kReadSnapshot)
    return ReadHashPayloadResult(payload, operation.kind_, count,
                                 operation.with_values_,
                                 operation.packed_output_);
  if (operation.kind_ == HashOperationKind::kReadPage)
    return ReadHashPayloadResult(payload,
                                 operation.with_values_
                                     ? HashOperationKind::kGetAll
                                     : HashOperationKind::kKeys,
                                 count);
  if (IsCompactPointRead(operation.kind_))
    return ReadCompactHashFields(payload, count, operation);
  if (operation.kind_ == HashOperationKind::kScan)
    return ReadHashScanResult(payload, count, operation, type);
  if (operation.kind_ == HashOperationKind::kRandomFields)
    return ReadCompactHashRandom(payload, count, operation);
  return absl::InvalidArgumentError("unsupported compact Hash read");
}

}  // namespace

absl::StatusOr<HashResult> ReadHashPayloadResult(std::string_view payload,
                                                 HashOperationKind kind,
                                                 std::uint64_t count,
                                                 bool with_values,
                                                 bool packed_output) {
  auto reader = HashValueReader::Open(payload);
  if (!reader.ok()) return reader.status();
  if (reader->size() != count) {
    return absl::InternalError(
        "Hash element count does not match record metadata");
  }
  const bool snapshot = kind == HashOperationKind::kReadSnapshot;
  const bool fields = kind != HashOperationKind::kValues;
  const bool values = snapshot ? with_values : kind != HashOperationKind::kKeys;
  if (snapshot || packed_output)
    return ReadPackedHashResult(*reader, count, fields, values);
  const std::size_t width = kind == HashOperationKind::kGetAll ? 2 : 1;
  std::size_t retained = sizeof(HashResult);
  auto add_bytes = [&](std::size_t bytes) {
    if (bytes > std::numeric_limits<std::size_t>::max() - retained)
      return false;
    retained += bytes;
    return true;
  };
  // Validate and admit before copying any strings. Full reads do not need
  // lookup digests or a mutable HashEntry array. The second traversal copies
  // only selected strings directly into the owned result; no view escapes the
  // LoadedValue lease. Use the same conservative SSO accounting as other Hash
  // results, and verify actual allocator capacities before ownership handoff.
  const std::size_t inline_capacity = std::string{}.capacity();
  auto inspect = *reader;
  for (std::size_t i = 0; i < count; ++i) {
    auto entry = inspect.Next();
    if (!entry.ok()) return entry.status();
    if (!add_bytes(width * sizeof(std::optional<std::string>)) ||
        (fields &&
         !add_bytes(std::max(entry->field_.size(), inline_capacity) + 1)) ||
        (values &&
         !add_bytes(std::max(entry->value_.size(), inline_capacity) + 1))) {
      RecordMemoryRejection();
      return MemoryLimitError("OOM Hash output is too large");
    }
  }
  auto reservation = TryReserveMemory(retained);
  if (!reservation) {
    RecordMemoryRejection();
    return MemoryLimitError("OOM Hash output retention");
  }
  HashResult result;
  result.key_exists_ = true;
  result.length_ = count;
  result.values_.reserve(count * width);
  for (std::size_t i = 0; i < count; ++i) {
    auto entry = reader->Next();
    if (!entry.ok()) return entry.status();
    if (fields) result.values_.emplace_back(std::in_place, entry->field_);
    if (values) result.values_.emplace_back(std::in_place, entry->value_);
  }
  retained = sizeof(HashResult) +
             result.values_.capacity() * sizeof(result.values_[0]);
  for (const auto& value : result.values_) retained += value->capacity() + 1;
  if (retained > reservation->bytes()) {
    RecordMemoryRejection();
    return MemoryLimitError("OOM Hash output retention");
  }
  result.retained_charge_.Adopt(&*reservation, retained);
  return result;
}

absl::StatusOr<HashResult> ReadCompactHash(const CompactReadResult& value,
                                           const HashOperation& operation,
                                           ValueType type) {
  if (!value.ok()) return value.status();
  if (IsWrite(operation))
    return absl::InvalidArgumentError("compact Hash read cannot mutate");
  if (operation.kind_ == HashOperationKind::kScan && operation.scan_count_ == 0)
    return absl::InvalidArgumentError(
        "Hash scan COUNT must be greater than zero");
  HashResult result;
  result.key_exists_ = value->has_value();
  result.length_ = *value ? (**value).logical_size_ : 0;
  if (operation.kind_ == HashOperationKind::kLength) return result;
  if (!*value) return ReadMissingHashResult(operation);
  return ReadEncodedHash((**value).encoded_, (**value).logical_size_, operation,
                         type);
}

absl::StatusOr<std::optional<std::string_view>> FindCompactHashField(
    std::string_view payload, std::size_t field_count, std::string_view field) {
  auto reader = HashValueReader::Open(payload);
  if (!reader.ok()) [[unlikely]]
    return absl::DataLossError(reader.status().message());
  if (reader->size() != field_count) [[unlikely]]
    return absl::DataLossError(
        "Hash element count does not match record metadata");

  // Published fields are unique by construction. The storage loader applies
  // the record's checksum policy; a point lookup only parses through its match.
  for (std::size_t i = 0; i < reader->size(); ++i) {
    auto entry = reader->Next();
    if (!entry.ok()) [[unlikely]]
      return absl::DataLossError(entry.status().message());
    if (entry->field_ == field)
      return std::optional<std::string_view>{entry->value_};
  }
  return std::nullopt;
}

absl::Status FindCompactHashFields(std::string_view payload,
                                   std::size_t field_count,
                                   std::span<HashFieldLookup> requests) {
  for (auto& request : requests) request.value_.reset();
  auto reader = HashValueReader::Open(payload);
  if (!reader.ok()) [[unlikely]]
    return absl::DataLossError(reader.status().message());
  if (reader->size() != field_count) [[unlikely]]
    return absl::DataLossError(
        "Hash element count does not match record metadata");
  // Count request positions, including repeated operands, so a complete reply
  // does not require parsing the unused tail of the stored Hash.
  std::size_t remaining = requests.size();
  for (std::size_t i = 0; i < reader->size() && remaining != 0; ++i) {
    auto entry = reader->Next();
    if (!entry.ok()) [[unlikely]]
      return absl::DataLossError(entry.status().message());
    // Binary search avoids hashing every unrelated encoded field. The sorted
    // request views also keep duplicate operands adjacent without owning names.
    auto match = std::lower_bound(
        requests.begin(), requests.end(), entry->field_,
        [](const HashFieldLookup& request, std::string_view field) {
          return request.field_ < field;
        });
    if (match == requests.end() || match->field_ != entry->field_ ||
        match->value_)
      continue;
    for (; match != requests.end() && match->field_ == entry->field_; ++match) {
      match->value_ = entry->value_;
      --remaining;
    }
  }
  return absl::OkStatus();
}

absl::StatusOr<std::optional<std::string_view>> FindHashGroupField(
    std::string_view payload, std::uint32_t field_count,
    std::string_view field) {
  // The loader has validated the group envelope, including empty leaves.
  if (field_count == 0) return std::nullopt;
  return FindCompactHashField(payload.substr(kHashGroupHeaderBytes),
                              field_count, field);
}

absl::Status FindHashGroupFields(std::string_view payload,
                                 std::uint32_t field_count,
                                 std::span<HashFieldLookup> requests) {
  if (field_count == 0) {
    for (auto& request : requests) request.value_.reset();
    return absl::OkStatus();
  }
  return FindCompactHashFields(payload.substr(kHashGroupHeaderBytes),
                               field_count, requests);
}

absl::Status FindCompactHashRanks(std::string_view payload,
                                  std::size_t field_count,
                                  std::span<HashRankLookup> requests) {
  auto reader = HashValueReader::Open(payload);
  if (!reader.ok()) return absl::DataLossError(reader.status().message());
  if (reader->size() != field_count)
    return absl::DataLossError(
        "Hash element count does not match record metadata");
  if (requests.empty()) return absl::OkStatus();
  if (requests.back().rank_ >= field_count)
    return absl::DataLossError("Hash sample rank exceeds page");
  std::size_t selected = 0;
  for (std::uint64_t rank = 0; selected < requests.size(); ++rank) {
    auto entry = reader->Next();
    if (!entry.ok()) return absl::DataLossError(entry.status().message());
    while (selected < requests.size() && requests[selected].rank_ == rank)
      requests[selected++].entry_ = *entry;
  }
  return absl::OkStatus();
}

absl::Status FindHashGroupRanks(std::string_view payload,
                                std::uint32_t field_count,
                                std::span<HashRankLookup> requests) {
  if (field_count == 0)
    return requests.empty()
               ? absl::OkStatus()
               : absl::DataLossError("Hash sample rank exceeds page");
  return FindCompactHashRanks(payload.substr(kHashGroupHeaderBytes),
                              field_count, requests);
}

absl::Status RetainHashRankValues(HashResult& result,
                                  std::span<const HashRankLookup> requests,
                                  bool with_values) {
  std::size_t retained = 0;
  for (const auto& request : requests) {
    for (const auto bytes :
         {std::max(request.entry_.field_.size(), std::string{}.capacity()) + 1,
          with_values ? std::max(request.entry_.value_.size(),
                                 std::string{}.capacity()) +
                            1
                      : std::size_t{0}}) {
      if (bytes > SIZE_MAX - retained)
        return MemoryLimitError("OOM Hash output is too large");
      retained += bytes;
    }
  }
  if (retained == 0) return absl::OkStatus();
  if (retained > SIZE_MAX - result.retained_charge_.bytes())
    return MemoryLimitError("OOM Hash output is too large");
  auto output = TryReserveMemory(retained);
  if (!output) {
    RecordMemoryRejection();
    return MemoryLimitError("OOM Hash output retention");
  }
  retained = 0;
  const std::size_t width = with_values ? 2 : 1;
  for (const auto& request : requests) {
    const auto slot = request.result_index_ * width;
    assert(slot < result.values_.size() && !result.values_[slot]);
    result.values_[slot].emplace(request.entry_.field_);
    if (with_values) {
      assert(slot + 1 < result.values_.size() && !result.values_[slot + 1]);
      result.values_[slot + 1].emplace(request.entry_.value_);
    }
    for (std::size_t i = 0; i < width; ++i) {
      const auto bytes = result.values_[slot + i]->capacity() + 1;
      if (bytes > output->bytes() - retained)
        return MemoryLimitError("OOM Hash output retention");
      retained += bytes;
    }
  }
  result.retained_charge_.Resize(result.retained_charge_.bytes() + retained);
  return absl::OkStatus();
}

absl::Status RetainHashGroupValues(HashResult& result,
                                   std::span<const HashFieldLookup> requests) {
  std::size_t retained = 0;
  for (const auto& request : requests) {
    if (!request.value_) continue;
    const auto bytes =
        std::max(request.value_->size(), std::string{}.capacity()) + 1;
    if (bytes > std::numeric_limits<std::size_t>::max() - retained)
        [[unlikely]] {
      RecordMemoryRejection();
      return MemoryLimitError("OOM Hash output is too large");
    }
    retained += bytes;
  }
  if (retained == 0) return absl::OkStatus();
  if (retained > std::numeric_limits<std::size_t>::max() -
                     result.retained_charge_.bytes()) [[unlikely]] {
    RecordMemoryRejection();
    return MemoryLimitError("OOM Hash output is too large");
  }
  auto reservation = TryReserveMemory(retained);
  if (!reservation) [[unlikely]] {
    RecordMemoryRejection();
    return MemoryLimitError("OOM Hash output retention");
  }
  retained = 0;
  for (const auto& request : requests) {
    if (!request.value_) continue;
    auto& value = result.values_[request.result_index_];
    assert(!value);
    value.emplace(*request.value_);
    const auto bytes = value->capacity() + 1;
    if (bytes > reservation->bytes() - retained) [[unlikely]] {
      RecordMemoryRejection();
      return MemoryLimitError("OOM Hash output retention");
    }
    retained += bytes;
  }
  // Growth is covered by the still-live reservation. Its destructor releases
  // pending admission after the existing output owner takes over the bytes.
  result.retained_charge_.Resize(result.retained_charge_.bytes() + retained);
  return absl::OkStatus();
}

absl::Status RetainHashLookupValue(HashResult& result,
                                   std::optional<std::string_view> matched) {
  assert(result.values_.capacity() == 0 &&
         result.retained_charge_.bytes() == 0);
  std::size_t retained =
      sizeof(HashResult) + sizeof(std::optional<std::string>);
  if (matched)
    retained += std::max(matched->size(), std::string{}.capacity()) + 1;
  auto reservation = TryReserveMemory(retained);
  if (!reservation) [[unlikely]] {
    RecordMemoryRejection();
    return MemoryLimitError("OOM Hash output retention");
  }
  // Reserve before copying out of the page lease, then check actual capacities
  // before transferring admission to the result. Match other Hash results'
  // conservative SSO accounting; a missing field still owns one vector slot.
  if (matched)
    result.values_.emplace_back(std::in_place, *matched);
  else
    result.values_.emplace_back(std::nullopt);
  retained = sizeof(HashResult) +
             result.values_.capacity() * sizeof(result.values_[0]);
  if (matched) retained += result.values_[0]->capacity() + 1;
  if (retained > reservation->bytes()) [[unlikely]] {
    RecordMemoryRejection();
    return MemoryLimitError("OOM Hash output retention");
  }
  result.retained_charge_.Adopt(&*reservation, retained);
  return absl::OkStatus();
}

Task<absl::StatusOr<HashResult>> StorageEngine::Impl::ReadGroupedHashFields(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    GroupedObject::Handle object, std::span<const std::string_view> fields) {
  // Keep multi-field planning in its own coroutine so single-field reads do
  // not pay for the plan's allocations or coroutine-frame storage.
  constexpr auto max_size = std::numeric_limits<std::size_t>::max();
  // Admission failures are command errors; allocator failure after admission
  // follows the runtime's fatal allocation policy. Check container limits
  // explicitly instead of relying on length_error from reserve/resize.
  if (fields.size() > std::vector<HashFieldLookup>{}.max_size() ||
      fields.size() > std::vector<std::optional<std::string>>{}.max_size() ||
      fields.size() > (max_size - sizeof(HashResult)) /
                          sizeof(std::optional<std::string>)) [[unlikely]] {
    RecordMemoryRejection();
    co_return MemoryLimitError("OOM Hash lookup plan is too large");
  }
  auto scratch = TryReserveMemory(fields.size() * sizeof(HashFieldLookup));
  if (!scratch) [[unlikely]] {
    RecordMemoryRejection();
    co_return MemoryLimitError("OOM Hash lookup plan");
  }
  std::vector<HashFieldLookup> requests;
  requests.reserve(fields.size());
  if (requests.capacity() > scratch->bytes() / sizeof(HashFieldLookup))
      [[unlikely]] {
    RecordMemoryRejection();
    co_return MemoryLimitError("OOM Hash lookup plan");
  }
  for (std::size_t i = 0; i < fields.size(); ++i) {
    const auto* route = object->directory().Find(fields[i]);
    if (route == nullptr) [[unlikely]]
      co_return absl::DataLossError("missing Hash field route");
    requests.push_back({.group_ = route->id_,
                        .field_ = fields[i],
                        .result_index_ = i,
                        .value_ = std::nullopt});
  }
  std::sort(requests.begin(), requests.end(),
            [](const auto& left, const auto& right) {
              if (left.group_ != right.group_)
                return left.group_ < right.group_;
              return left.field_ < right.field_;
            });

  auto output = TryReserveMemory(
      sizeof(HashResult) + fields.size() * sizeof(std::optional<std::string>));
  if (!output) [[unlikely]] {
    RecordMemoryRejection();
    co_return MemoryLimitError("OOM Hash output retention");
  }
  HashResult result;
  result.key_exists_ = true;
  result.length_ = object->directory().root().field_count_;
  result.values_.resize(fields.size());
  const auto retained = sizeof(HashResult) +
                        result.values_.capacity() * sizeof(result.values_[0]);
  if (retained > output->bytes()) [[unlikely]] {
    RecordMemoryRejection();
    co_return MemoryLimitError("OOM Hash output retention");
  }
  result.retained_charge_.Adopt(&*output, retained);
  for (std::size_t begin = 0; begin < requests.size();) {
    auto end = begin + 1;
    while (end < requests.size() &&
           requests[end].group_ == requests[begin].group_)
      ++end;
    auto loaded = co_await LoadHashGroupPayload(
        store, partition, db_id, key, digest, object, requests[begin].group_);
    if (!loaded.ok()) [[unlikely]]
      co_return loaded.status();
    const auto bytes = loaded->loaded_.value();
    const std::string_view payload(reinterpret_cast<const char*>(bytes.data()),
                                   bytes.size());
    auto page_requests = std::span(requests).subspan(begin, end - begin);
    auto status =
        FindHashGroupFields(payload, loaded->field_count_, page_requests);
    if (!status.ok()) [[unlikely]]
      co_return status;
    status = RetainHashGroupValues(result, page_requests);
    if (!status.ok()) [[unlikely]]
      co_return status;
    // Only owned output survives this page lease. Clear the borrowed matches
    // before loading another group, including on the buffer-reuse path.
    for (auto& request : page_requests) request.value_.reset();
    begin = end;
  }
  co_return result;
}

Task<absl::StatusOr<HashResult>> StorageEngine::Impl::ExecuteHashLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const HashOperation& operation, TxShardWrites* tx,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  return ExecuteHashLikeLocked(db_id, key, digest, operation, ValueType::kHash,
                               tx, replication, mutation_precondition);
}

Task<absl::StatusOr<HashResult>> StorageEngine::Impl::ExecuteHashLikeLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const HashOperation& operation, ValueType value_type, TxShardWrites* tx,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  // Private preparation allocations must unwind here: Bycorf terminates on
  // exceptions escaping a coroutine body, even when its caller has a catch.
  assert(db_id < kLogicalDatabaseCount);
  const bool replace = operation.kind_ == HashOperationKind::kReplaceOnly;
  if (replace && (value_type != ValueType::kHash || operation.fields_.empty() ||
                  operation.fields_.size() != operation.values_.size())) {
    co_return absl::InvalidArgumentError("invalid Hash replacement fields");
  }
  if (operation.kind_ == HashOperationKind::kScan &&
      operation.scan_count_ == 0) {
    co_return absl::InvalidArgumentError(
        "Hash scan COUNT must be greater than zero");
  }

  WorkerStore& store = CurrentStore();
  auto& partition = PartitionForKey(store, key);
  co_await store.store_state_mutex_.Lock();
  UnlockGuard unlock(&store.store_state_mutex_, store.worker_);

  auto& index = partition.indexes_[db_id];
  auto* found = index.Find(digest, key);
  if (found != nullptr && !found->key_complete()) [[unlikely]] {
    auto resolved = co_await FindVerifiedEntry(store, index, digest, key);
    if (!resolved.ok()) co_return resolved.status();
    found = *resolved;
  }
  const bool stored_value =
      found != nullptr && found->value_.kind() == RecordKind::kValue;
  const std::uint64_t now_ms =
      operation.now_ms_ == 0 ? UnixTimeMillis() : operation.now_ms_;
  const bool exists = stored_value && !IsExpired(*found, now_ms);
  if (!exists && stored_value && found->value_.grouped()) {
    auto metadata = co_await ReadKeyMetadataLocked(db_id, key, digest);
    if (!metadata.ok()) co_return metadata.status();
  }
  if (exists && found->value_.value_type() != value_type) {
    co_return WrongTypeError();
  }
  if (replace && !exists) co_return HashResult{};
  const bool read_only = !IsWrite(operation);
  // Exercise the command's storage-error reply path independently of the
  // outer maxmemory preflight. No mutation has been staged at this boundary.
  LAVIK_FAULT_INJECT(if (LAVIK_FAULT_MATCHES("LAVIK_FAIL_HASH_ADMISSION_KEY",
                                             key) &&
                         !read_only && value_type == ValueType::kHash) {
    co_return MemoryLimitError("OOM injected Hash storage admission failure");
  });
  const auto observed_write =
      CaptureCompactWriteSnapshot(store, partition, db_id);
  const auto observed_index_generation = observed_write.index_generation_;
  const auto observed_db_epoch = observed_write.db_epoch_;
  const auto observed_replication_epoch = observed_write.replication_epoch_;
  auto read_epoch_changed = [&]() {
    return read_only &&
           (store.index_generations_[db_id] != observed_index_generation ||
            EffectiveRecordDbEpoch(partition, db_id) != observed_db_epoch ||
            partition.replication_epoch_ != observed_replication_epoch);
  };
  const RecordLocation location =
      exists ? MaterializeIndexLocation(*found) : RecordLocation{};
  const ExtentManifest extents =
      exists ? ExtentsFor(store, found) : ExtentManifest{};
  const std::uint64_t expire_at_ms = exists ? location.expire_at_ms_ : 0;
  GroupedObject::Handle grouped;
  if (exists && location.grouped()) {
    auto view = partition.grouped_objects_[db_id].Lookup(
        key, GroupedObjectVersion{
                 .root_ = location,
                 .db_epoch_ = observed_db_epoch,
                 .replication_epoch_ = observed_replication_epoch,
                 .index_generation_ = partition.grouped_generations_[db_id],
             });
    if (!view.ok()) co_return view.status();
    grouped = std::move(*view);
    // Validate the preceding decision, but never inherit its fields/pages.
    // Append/CommitGroupedHashMutation still find and retire the old side
    // view; a null mutation predecessor creates a fresh incarnation.
    if (replace) grouped.reset();
  }

  HashResult result;
  result.key_exists_ = exists;
  result.length_ = exists ? location.logical_size_ : 0;

  // HLEN/SCARD are carried in the record header. Loading and decoding the
  // complete monolithic value here turns an O(1) metadata lookup into an
  // O(value-size) disk read and allocation.
  if (operation.kind_ == HashOperationKind::kLength) co_return result;

  // Ordinary small HSET/HMSET and SADD/SREM retain exclusive key intent in
  // their caller; normal command dispatch also retains database admission
  // through append's later allocation waits. Other keys need not wait for
  // this key's disk read or private decode/modify/encode work. Keep compact
  // external values, EXEC and native candidate/loading on their existing
  // paths; ordinary online replay has the same command guards and may use
  // this path too. Snapshot only value metadata before suspension, never an
  // index entry pointer. GC is allowed to relocate this same logical version
  // while the state mutex is released. Set multi-key mutation adapters always
  // provide their durable TxShardWrites; kPopRandom stays on its original
  // selection/replication path.
  const bool prepare_operation =
      (value_type == ValueType::kHash &&
       (operation.kind_ == HashOperationKind::kSet || replace)) ||
      (value_type == ValueType::kSet &&
       (operation.kind_ == HashOperationKind::kSet ||
        operation.kind_ == HashOperationKind::kDelete));
  const bool unlocked_compact_write =
      prepare_operation && exists &&
      CanPrepareCompactWriteUnlocked(store, partition, found, location, tx);
  const bool unlocked_create =
      !exists && !read_only && IsPointOperation(operation.kind_) &&
      operation.kind_ != HashOperationKind::kDelete &&
      CanPrepareCollectionCreateUnlocked(partition, tx);
  // Point writers retain their key hold even inside EXEC/Lua. Random pops use
  // a separate selection/publication adapter and retain its existing
  // contract.
  const bool unlocked_grouped_write =
      !read_only && grouped != nullptr &&
      operation.kind_ != HashOperationKind::kPopRandom &&
      CanPrepareGroupedWriteUnlocked(partition);
  if (read_only || unlocked_compact_write || unlocked_grouped_write ||
      unlocked_create) {
    found = nullptr;
    unlock.Unlock();
  }
  LAVIK_FAULT_INJECT(if (unlocked_create) {
    if (LAVIK_FAULT_MATCHES("LAVIK_FAIL_COLLECTION_CREATE_PREPARE_KEY", key)) {
      RecordMemoryRejection();
      co_return MemoryLimitError("OOM preparing collection creation");
    }
  });
  LAVIK_FAULT_INJECT(if (unlocked_grouped_write) {
    const auto paused =
        co_await PauseGroupedWriteForTest(*store.worker_, key, "prepare");
    if (!paused.ok()) co_return paused;
  });
  LAVIK_FAULT_INJECT(if (unlocked_compact_write &&
                         value_type == ValueType::kHash) {
    const char* paused_key = std::getenv("LAVIK_COMPACT_HASH_WRITE_PAUSE_KEY");
    const char* configured = std::getenv("LAVIK_COMPACT_HASH_WRITE_PAUSE_MS");
    if (paused_key != nullptr && key == paused_key && configured != nullptr) {
      std::int64_t milliseconds = 0;
      const char* end = configured + std::strlen(configured);
      const auto parsed = std::from_chars(configured, end, milliseconds);
      if (parsed.ec == std::errc{} && parsed.ptr == end && milliseconds > 0) {
        spdlog::info("compact hash write pause armed key={} milliseconds={}",
                     key, milliseconds);
        auto paused = co_await bycorf::SleepFor(
            *store.worker_, std::chrono::milliseconds(milliseconds));
        if (!paused.ok()) co_return paused;
        spdlog::info("compact hash write pause complete key={}", key);
      }
    }
  });
  LAVIK_FAULT_INJECT(if (unlocked_compact_write) {
    const auto paused = co_await PauseCompactWriteForTest(*store.worker_, key);
    if (!paused.ok()) co_return paused;
  });
  LAVIK_FAULT_INJECT(if (read_only) {
    if (const char* configured = std::getenv("LAVIK_HASH_READ_PAUSE_MS");
        configured != nullptr) {
      std::uint64_t milliseconds = 0;
      const char* end = configured + std::strlen(configured);
      const auto parsed = std::from_chars(configured, end, milliseconds);
      if (parsed.ec == std::errc{} && parsed.ptr == end && milliseconds != 0) {
        absl::Status paused = co_await bycorf::SleepFor(
            *store.worker_, std::chrono::milliseconds(milliseconds));
        if (!paused.ok()) co_return paused;
      }
    }
  });

  if (grouped != nullptr && IsCompactPointRead(operation.kind_) &&
      operation.fields_.empty())
    co_return result;
  if (grouped != nullptr &&
      (operation.fields_.size() == 1 ||
       operation.kind_ == HashOperationKind::kExists ||
       operation.kind_ == HashOperationKind::kStringLength) &&
      (operation.kind_ == HashOperationKind::kGet ||
       operation.kind_ == HashOperationKind::kGetMany ||
       operation.kind_ == HashOperationKind::kExists ||
       operation.kind_ == HashOperationKind::kStringLength)) {
    const std::string_view field = operation.fields_.front();
    const auto* route = grouped->directory().Find(field);
    if (route == nullptr) [[unlikely]]
      co_return absl::DataLossError("missing Hash field route");
    auto loaded = co_await LoadHashGroupPayload(store, partition, db_id, key,
                                                digest, grouped, route->id_);
    if (!loaded.ok()) [[unlikely]] {
      if (read_epoch_changed()) co_return HashResult{};
      co_return loaded.status();
    }
    const auto bytes = loaded->loaded_.value();
    const std::string_view payload(reinterpret_cast<const char*>(bytes.data()),
                                   bytes.size());
    auto matched = FindHashGroupField(payload, loaded->field_count_, field);
    if (!matched.ok()) [[unlikely]]
      co_return matched.status();
    switch (operation.kind_) {
      case HashOperationKind::kExists:
        result.integer_ = matched->has_value();
        break;
      case HashOperationKind::kStringLength:
        result.integer_ = *matched ? (**matched).size() : 0;
        break;
      default: {
        auto status = RetainHashLookupValue(result, *matched);
        if (!status.ok()) [[unlikely]]
          co_return status;
        break;
      }
    }
    co_return result;
  }

  if (grouped != nullptr && (operation.kind_ == HashOperationKind::kGet ||
                             operation.kind_ == HashOperationKind::kGetMany)) {
    auto values = co_await ReadGroupedHashFields(
        store, partition, db_id, key, digest, grouped, operation.fields_);
    if (!values.ok() && read_epoch_changed()) [[unlikely]]
      co_return HashResult{};
    co_return values;
  }

  if (grouped != nullptr &&
      (operation.kind_ == HashOperationKind::kRandomFields ||
       operation.kind_ == HashOperationKind::kPopRandom)) {
    co_return co_await ExecuteGroupedHashRandomLocked(
        store, partition, db_id, key, digest, operation, std::move(grouped),
        value_type, tx, replication, mutation_precondition);
  }
  if (grouped != nullptr && (operation.kind_ == HashOperationKind::kScan ||
                             operation.kind_ == HashOperationKind::kReadPage)) {
    // Public SCAN cursors are field digests under the persisted routing seed,
    // so deletes and splits cannot shift a field behind its cursor. Internal
    // page projections keep key intent for the entire traversal and need only
    // the next immutable routing prefix, without rebuilding digest order.
    const auto& groups = grouped->directory().groups();
    const auto* route = groups.Floor(operation.cursor_);
    if (route == nullptr)
      co_return absl::DataLossError("grouped scan has no routing leaf");
    auto next = groups.find(route->id_.prefix_);
    ++next;
    const auto* entry = grouped->FindGroup(route->id_);
    if (entry == nullptr)
      co_return absl::DataLossError("grouped scan has no physical page");
    auto loaded = co_await LoadHashGroupPayload(store, partition, db_id, key,
                                                digest, grouped, route->id_);
    if (!loaded.ok()) {
      if (read_epoch_changed()) co_return HashResult{};
      co_return loaded.status();
    }
    const auto bytes = loaded->loaded_.value();
    const std::string_view payload(reinterpret_cast<const char*>(bytes.data()),
                                   bytes.size());
    if (operation.kind_ == HashOperationKind::kReadPage) {
      result.cursor_ = next != groups.end() ? next->first : 0;
      if (loaded->field_count_ == 0) co_return result;
      auto page = ReadHashPayloadResult(payload.substr(kHashGroupHeaderBytes),
                                        operation.with_values_
                                            ? HashOperationKind::kGetAll
                                            : HashOperationKind::kKeys,
                                        loaded->field_count_);
      if (!page.ok() && absl::IsInternal(page.status()))
        co_return absl::DataLossError(page.status().message());
      if (page.ok()) {
        page->length_ = result.length_;
        page->cursor_ = result.cursor_;
      }
      co_return page;
    }
    auto scanned = ReadHashScanResult(
        loaded->field_count_ == 0 ? std::string_view{}
                                  : payload.substr(kHashGroupHeaderBytes),
        loaded->field_count_, operation, value_type,
        &grouped->directory().root().seed_,
        next != groups.end() ? next->first : 0);
    if (!scanned.ok() && absl::IsInternal(scanned.status()))
      co_return absl::DataLossError(scanned.status().message());
    if (scanned.ok()) scanned->length_ = result.length_;
    co_return scanned;
  }
  if (grouped != nullptr &&
      (operation.kind_ == HashOperationKind::kGetAll ||
       operation.kind_ == HashOperationKind::kKeys ||
       operation.kind_ == HashOperationKind::kValues ||
       operation.kind_ == HashOperationKind::kReadSnapshot)) {
    auto values = co_await ReadGroupedHashResult(
        store, partition, db_id, key, digest, grouped, operation.kind_,
        operation.with_values_, operation.packed_output_);
    if (!values.ok() && read_epoch_changed()) co_return HashResult{};
    co_return values;
  }

  // Point writes keep the selected leaves until planning finishes; other
  // materializing operations keep the entire decoded value. Admit that
  // peak before any leaf allocations, including copies made by the planner.
  const bool edit_leaves =
      unlocked_grouped_write &&
      (operation.kind_ == HashOperationKind::kSet ||
       operation.kind_ == HashOperationKind::kSetIfAbsent ||
       operation.kind_ == HashOperationKind::kDelete);
  if (edit_leaves && operation.kind_ != HashOperationKind::kDelete &&
      operation.fields_.size() != operation.values_.size())
    co_return absl::InvalidArgumentError("Hash field/value mismatch");
  std::set<GroupedRecordId> selected;
  std::optional<MemoryReservation> operand_scratch;
  struct RoutedEdit {
    GroupedRecordId id_;
    HashEntryView view_;
    std::size_t ordinal_;
  };
  // A single-field write needs no heap-allocated routing index. For batches,
  // sort borrowed views by leaf and original position: duplicates retain
  // command order, and one reusable page list replaces a vector per leaf.
  absl::InlinedVector<RoutedEdit, 1> leaf_edits;
  if (edit_leaves) {
    GroupedScratchBudget budget;
    if (operation.fields_.size() > SIZE_MAX / 256)
      co_return absl::ResourceExhaustedError("Hash operand index overflow");
    // Routing and per-page views, including growth rounding and empty
    // operands. Request strings themselves remain client-owned.
    auto added = budget.AddBytes(operation.fields_.size() * 256);
    if (!added.ok()) co_return added;
    auto admitted = budget.Reserve(1);
    if (!admitted.ok()) co_return admitted.status();
    operand_scratch.emplace(std::move(*admitted));
    leaf_edits.reserve(operation.fields_.size());
  }
  std::optional<MemoryReservation> grouped_scratch;
  if (replace || unlocked_create) {
    // Bound owned field copies, sorting and encoding before allocating them.
    // Request bytes have their separate client-buffer admission already.
    if (operation.fields_.size() != operation.values_.size())
      co_return absl::InvalidArgumentError("Hash field/value mismatch");
    GroupedScratchBudget budget;
    for (std::size_t i = 0; i < operation.fields_.size(); ++i) {
      if (operation.fields_[i].size() > kMaxStringBytes ||
          operation.values_[i].size() > kMaxStringBytes)
        co_return absl::OutOfRangeError("Hash field or value exceeds 512 MiB");
      for (const auto bytes : {operation.fields_[i].size(),
                               operation.values_[i].size(), std::size_t{256}}) {
        const auto added = budget.AddBytes(bytes);
        if (!added.ok()) co_return added;
      }
    }
    auto admitted = budget.Reserve(unlocked_create ? 2 : 4);
    if (!admitted.ok()) co_return admitted.status();
    grouped_scratch.emplace(std::move(*admitted));
  } else if (grouped != nullptr) {
    GroupedScratchBudget budget;
    if (IsPointOperation(operation.kind_)) {
      for (std::size_t i = 0; i < operation.fields_.size(); ++i) {
        const auto field = operation.fields_[i];
        const auto* route = grouped->directory().Find(field);
        if (route == nullptr)
          co_return absl::DataLossError("missing Hash field route");
        if (edit_leaves) {
          leaf_edits.push_back(
              {route->id_,
               {field, operation.kind_ == HashOperationKind::kDelete
                           ? std::string_view{}
                           : operation.values_[i]},
               i});
        } else {
          selected.insert(route->id_);
        }
      }
    } else {
      for (const auto& [prefix, metadata] : grouped->directory().groups())
        selected.insert(metadata.id_);
    }
    const auto add_group = [&](GroupedRecordId id) -> absl::Status {
      // Every id above came from this same immutable directory, and no await
      // separates routing from admission. Rechecking the logical AVL route
      // here adds a lookup without strengthening the physical read checks.
      const auto* entry = grouped->FindRecord(id);
      if (entry == nullptr)
        return absl::DataLossError("missing Hash scratch page");
      // Inline records cannot own a manifest. Avoid a second physical-index
      // traversal merely to obtain the null handle used by admission.
      return budget.AddGroup(*entry, entry->external() ? grouped->ExtentsFor(id)
                                                       : ExtentManifest{});
    };
    if (edit_leaves) {
      if (leaf_edits.size() > 1)
        std::sort(leaf_edits.begin(), leaf_edits.end(),
                  [](const auto& a, const auto& b) {
                    return a.id_ != b.id_ ? a.id_ < b.id_
                                          : a.ordinal_ < b.ordinal_;
                  });
      std::optional<GroupedRecordId> previous;
      for (const auto& edit : leaf_edits) {
        if (previous == edit.id_) continue;
        const auto added = add_group(edit.id_);
        if (!added.ok()) co_return added;
        previous = edit.id_;
      }
    } else {
      for (const auto id : selected) {
        const auto added = add_group(id);
        if (!added.ok()) co_return added;
      }
    }
    for (const auto field : operation.fields_) {
      const auto added = budget.AddBytes(field.size());
      if (!added.ok()) co_return added;
    }
    for (const auto value : operation.values_) {
      const auto added = budget.AddBytes(value.size());
      if (!added.ok()) co_return added;
    }
    auto admitted = budget.Reserve(4);
    if (!admitted.ok()) co_return admitted.status();
    grouped_scratch.emplace(std::move(*admitted));
  }
  HashValue compact;
  std::optional<MemoryReservation> compact_edit_scratch;
  std::optional<CompactHashEditPlan> compact_edit;
  std::optional<LoadedValue> compact_source;
  std::optional<HashGroupMutationPlan> grouped_plan;
  std::uint64_t edited_length = location.logical_size_;
  if (edit_leaves) {
    grouped_plan.emplace();
    auto& plan = *grouped_plan;
    plan.root_ = grouped->directory().root();
    plan.expected_sequence_ = grouped->directory().sequence();
    const auto kind = operation.kind_ == HashOperationKind::kDelete
                          ? HashGroupEditKind::kDelete
                      : operation.kind_ == HashOperationKind::kSet
                          ? HashGroupEditKind::kSet
                          : HashGroupEditKind::kSetIfAbsent;
    absl::InlinedVector<HashEntryView, 1> edits;
    for (std::size_t begin = 0; begin < leaf_edits.size();) {
      // Yield between pages for batch fairness, without scheduling an extra
      // turn before every single-field write. Actual IO still suspends.
      if (begin != 0) co_await bycorf::Yield(*store.worker_);
      const auto id = leaf_edits[begin].id_;
      edits.clear();
      do {
        edits.push_back(leaf_edits[begin++].view_);
      } while (begin < leaf_edits.size() && leaf_edits[begin].id_ == id);
      auto loaded = co_await LoadHashGroupPayload(store, partition, db_id, key,
                                                  digest, grouped, id);
      if (!loaded.ok()) co_return loaded.status();
      const auto bytes = loaded->loaded_.value();
      auto edited = ApplyHashGroupEdits(
          {reinterpret_cast<const char*>(bytes.data()), bytes.size()},
          plan.root_.seed_, kind, edits);
      if (!edited.ok()) co_return edited.status();
      if (!edited->changed_) continue;
      result.changed_ = plan.changed_ = true;
      edited_length += edited->added_;
      edited_length -= edited->removed_;
      result.integer_ += kind == HashGroupEditKind::kDelete ? edited->removed_
                                                            : edited->added_;
      if (edited->leaves_.size() > 1) {
        const auto extra = edited->leaves_.size() - 1;
        if (extra > UINT32_MAX - plan.root_.group_count_)
          co_return absl::OutOfRangeError("Hash directory size overflow");
        plan.root_.group_count_ += extra;
        plan.writes_.push_back({.incarnation_ = plan.root_.incarnation_,
                                .id_ = id,
                                .retired_ = true,
                                .value_ = {}});
      }
      for (auto& leaf : edited->leaves_)
        plan.writes_.push_back(std::move(leaf));
    }
    plan.root_.field_count_ = edited_length;
  } else if (grouped != nullptr) {
    if (IsPointOperation(operation.kind_)) {
      // Loading a requested leaf once is enough even when a mutation mentions
      // several fields routed to that same leaf. Unrelated large field values
      // never enter this command's scratch unless they share the affected
      // group.
      for (const auto id : selected) {
        if (unlocked_grouped_write) co_await bycorf::Yield(*store.worker_);
        auto loaded = co_await LoadHashGroupSnapshot(store, partition, db_id,
                                                     key, digest, grouped, id);
        if (!loaded.ok()) {
          if (read_epoch_changed()) co_return HashResult{};
          co_return loaded.status();
        }
        for (auto& entry : loaded->snapshot_.value_.entries_) {
          compact.entries_.push_back(std::move(entry));
        }
      }
    } else {
      auto loaded = co_await LoadGroupedHashValue(store, partition, db_id, key,
                                                  digest, grouped);
      if (!loaded.ok()) {
        if (read_epoch_changed()) co_return HashResult{};
        co_return loaded.status();
      }
      compact = std::move(*loaded);
    }
  } else if (exists && !replace) {
    auto loaded = co_await LoadValue(store, partition, db_id, key, digest,
                                     location, extents);
    if (!loaded.ok()) {
      if (read_epoch_changed()) co_return HashResult{};
      co_return loaded.status();
    }
    if (!read_only) {
      auto owned = loaded->MakeOwned(store.buffers_);
      if (!owned.ok()) co_return owned;
    }
    const auto bytes = loaded->value();
    const std::string_view payload(reinterpret_cast<const char*>(bytes.data()),
                                   bytes.size());
    if (read_only)
      co_return ReadEncodedHash(payload, location.logical_size_, operation,
                                value_type);
    // The detached source can survive lock/append waits without occupying
    // the bounded read arena.
    if ((value_type == ValueType::kHash &&
         operation.kind_ == HashOperationKind::kSet) ||
        (value_type == ValueType::kSet &&
         (operation.kind_ == HashOperationKind::kSet ||
          operation.kind_ == HashOperationKind::kDelete))) {
      // Only operand views and framing survive preparation; unchanged bytes
      // remain borrowed from compact_source through the awaited append.
      // Publication still uses the existing key intent, snapshot validation and
      // append boundary.
      GroupedScratchBudget budget;
      if (operation.fields_.size() > SIZE_MAX / 256)
        co_return MemoryLimitError("OOM Hash edit plan is too large");
      auto added = budget.AddBytes(operation.fields_.size() * 256);
      if (!added.ok()) co_return added;
      added = budget.AddBytes(kCollectionPromotionBytes * 2);
      if (!added.ok()) co_return added;
      auto admitted = budget.Reserve(1);
      // Preserve the bounded legacy workspace for shrinking at maxmemory.
      // A denied splice plan falls back before it borrows or changes data.
      if (!admitted.ok() && operation.kind_ != HashOperationKind::kDelete)
        co_return admitted.status();
      if (admitted.ok()) {
        compact_edit_scratch.emplace(std::move(*admitted));
        auto edited = PlanCompactHashEdit(
            payload, location.logical_size_, operation.fields_,
            operation.values_, kCollectionPromotionBytes - 1,
            operation.kind_ == HashOperationKind::kDelete
                ? CompactHashEditKind::kDelete
                : CompactHashEditKind::kSet);
        if (!edited.ok()) co_return edited.status();
        compact_edit = std::move(*edited);
        if (compact_edit) {
          result.changed_ = compact_edit->changed_;
          result.integer_ = operation.kind_ == HashOperationKind::kDelete
                                ? compact_edit->removed_
                                : compact_edit->added_;
          compact_source.emplace(std::move(*loaded));
        } else {
          // Promotion keeps the existing owned builder and its admission. No
          // oversized output was allocated by the bounded editor.
          compact_edit_scratch.reset();
        }
      }
    }
    if (!compact_edit) {
      auto decoded = DecodeHashValue(payload);
      if (!decoded.ok()) co_return decoded.status();
      compact = std::move(*decoded);
      if (compact.entries_.size() != location.logical_size_) {
        co_return absl::InternalError(
            "Hash element count does not match record metadata");
      }
    }
  }

  if (read_only) co_return ReadMissingHashResult(operation);

  const std::uint64_t selected_field_count = compact.entries_.size();
  std::set<GroupedRecordId> changed_groups;
  auto mark_group_changed = [&](std::string_view field) {
    if (grouped != nullptr) {
      const auto* route = grouped->directory().Find(field);
      assert(route != nullptr);
      changed_groups.insert(route->id_);
    }
  };

  auto find_entry = [&](std::string_view field) {
    const Digest field_digest = ComputeDigest(field);
    return std::find_if(compact.entries_.begin(), compact.entries_.end(),
                        [&](const HashEntry& entry) {
                          return entry.digest_ == field_digest &&
                                 entry.field_ == field;
                        });
  };
  auto lookup = [&](std::string_view field) -> HashEntry* {
    auto entry = find_entry(field);
    return entry == compact.entries_.end() ? nullptr : &*entry;
  };

  auto incremented_value = [&](std::optional<std::string_view> current)
      -> absl::StatusOr<std::string> {
    if (operation.fields_.size() != 1 || operation.values_.size() != 1) {
      return absl::InvalidArgumentError("invalid Hash increment operands");
    }
    if (operation.kind_ == HashOperationKind::kIncrementInteger) {
      std::int64_t previous = 0;
      std::int64_t increment = 0;
      if (current.has_value()) {
        const auto parsed = std::from_chars(
            current->data(), current->data() + current->size(), previous);
        if (parsed.ec != std::errc{} ||
            parsed.ptr != current->data() + current->size()) {
          return absl::InvalidArgumentError("hash value is not an integer");
        }
      }
      const std::string_view delta = operation.values_.front();
      const auto parsed =
          std::from_chars(delta.data(), delta.data() + delta.size(), increment);
      if (parsed.ec != std::errc{} ||
          parsed.ptr != delta.data() + delta.size()) {
        return absl::InvalidArgumentError(
            "value is not an integer or out of range");
      }
      std::int64_t updated = 0;
      if (__builtin_add_overflow(previous, increment, &updated)) {
        return absl::OutOfRangeError("increment or decrement would overflow");
      }
      result.signed_integer_ = updated;
      return std::to_string(updated);
    }

    long double previous = 0;
    long double increment = 0;
    if (current.has_value() && !ParseRedisLongDouble(*current, &previous)) {
      return absl::InvalidArgumentError("hash value is not a float");
    }
    const std::string_view delta = operation.values_.front();
    std::string_view special = delta;
    if (!special.empty() &&
        (special.front() == '+' || special.front() == '-')) {
      special.remove_prefix(1);
    }
    std::string special_lower(special);
    std::transform(special_lower.begin(), special_lower.end(),
                   special_lower.begin(), [](unsigned char byte) {
                     return static_cast<char>(std::tolower(byte));
                   });
    if (special_lower == "inf" || special_lower == "infinity" ||
        special_lower == "nan") {
      return absl::InvalidArgumentError("value is NaN or Infinity");
    }
    if (!ParseRedisLongDouble(delta, &increment))
      return absl::InvalidArgumentError("value is not a valid float");
    const long double updated = previous + increment;
    if (!std::isfinite(updated))
      return absl::OutOfRangeError("increment would produce NaN or Infinity");
    if (!FormatRedisLongDouble(updated, &result.scalar_)) {
      return absl::InternalError("failed to format Hash float");
    }
    return result.scalar_;
  };

  auto requested_samples = [&]() -> absl::StatusOr<std::uint64_t> {
    if (!operation.count_provided_) return 1;
    if (operation.count_ == std::numeric_limits<std::int64_t>::min())
      return absl::OutOfRangeError("value is out of range");
    const std::uint64_t requested = static_cast<std::uint64_t>(
        operation.count_ < 0 ? -operation.count_ : operation.count_);
    return requested;
  };

  switch (operation.kind_) {
    case HashOperationKind::kReplaceOnly: {
      // Sort request positions, not owned values: duplicate fields use the
      // last argument, in O(n log n) without reading any old Hash payload.
      std::vector<std::size_t> order(operation.fields_.size());
      for (std::size_t i = 0; i < order.size(); ++i) order[i] = i;
      std::sort(order.begin(), order.end(), [&](auto a, auto b) {
        return operation.fields_[a] == operation.fields_[b]
                   ? a < b
                   : operation.fields_[a] < operation.fields_[b];
      });
      compact.entries_.reserve(order.size());
      for (std::size_t i = 0; i < order.size(); ++i) {
        const auto position = order[i];
        const auto field = operation.fields_[position];
        if (i + 1 < order.size() && field == operation.fields_[order[i + 1]])
          continue;
        compact.entries_.push_back(
            HashEntry{.digest_ = ComputeDigest(field),
                      .field_ = std::string(field),
                      .value_ = std::string(operation.values_[position])});
      }
      // Identical replacements still write and invalidate WATCH. Comparing
      // old values would defeat the explicit read-free replacement
      // contract.
      result.changed_ = true;
      break;
    }
    case HashOperationKind::kSet:
    case HashOperationKind::kSetIfAbsent: {
      if (edit_leaves || compact_edit) break;
      if (operation.fields_.size() != operation.values_.size())
        co_return absl::InvalidArgumentError("Hash field/value mismatch");
      const bool indexed = operation.fields_.size() > 1;
      std::optional<MemoryReservation> lookup_scratch;
      absl::flat_hash_set<std::size_t, FieldPositionHash, FieldPositionEqual>
          field_positions(0, FieldPositionHash{&compact.entries_},
                          FieldPositionEqual{&compact.entries_});
      if (indexed) {
        // Selected groups share one vector, so scanning it for every input
        // field makes a growing batch quadratic. Index positions, and
        // destroy the index before the planner sorts/moves entries.
        // Unlocked creation may suspend but retains exclusive ownership of
        // this vector. Single-field writes need only a scan.
        GroupedScratchBudget budget;
        // Sparse slots/control bytes and allocation rounding, without
        // copies of field bytes. Count duplicates to admit the peak up
        // front. Growth rounding and load factor need fewer than four slots
        // per entry; fixed small-table padding is in GroupedScratchBudget.
        constexpr auto kPositionBytes =
            4 * (sizeof(std::size_t) + sizeof(std::uint8_t));
        constexpr auto kMaxPositions =
            std::numeric_limits<std::size_t>::max() / kPositionBytes;
        if (compact.entries_.size() > kMaxPositions ||
            operation.fields_.size() > kMaxPositions - compact.entries_.size())
          co_return absl::ResourceExhaustedError("field index size overflow");
        auto added = budget.AddBytes(
            (compact.entries_.size() + operation.fields_.size()) *
            kPositionBytes);
        if (!added.ok()) co_return added;
        auto admitted = budget.Reserve(1);
        if (!admitted.ok()) co_return admitted.status();
        lookup_scratch.emplace(std::move(*admitted));
        field_positions.reserve(compact.entries_.size() +
                                operation.fields_.size());
        for (std::size_t i = 0; i < compact.entries_.size(); ++i)
          field_positions.insert(i);
      }
      for (std::size_t i = 0; i < operation.fields_.size(); ++i) {
        if (unlocked_create && i != 0 && i % 256 == 0)
          co_await bycorf::Yield(*store.worker_);
        if (operation.fields_[i].size() > kMaxStringBytes ||
            operation.values_[i].size() > kMaxStringBytes) {
          co_return absl::OutOfRangeError(
              "Hash field or value exceeds Redis-compatible 512 MiB limit");
        }
        HashEntry* current = nullptr;
        if (indexed) {
          auto position = field_positions.find(operation.fields_[i]);
          if (position != field_positions.end())
            current = &compact.entries_[*position];
        } else {
          current = lookup(operation.fields_[i]);
        }
        if (current != nullptr) {
          if (operation.kind_ == HashOperationKind::kSetIfAbsent) continue;
          if (current->value_ != operation.values_[i]) {
            mark_group_changed(operation.fields_[i]);
            current->value_ = operation.values_[i];
            result.changed_ = true;
          }
        } else {
          mark_group_changed(operation.fields_[i]);
          compact.entries_.push_back(
              HashEntry{.digest_ = ComputeDigest(operation.fields_[i]),
                        .field_ = std::string(operation.fields_[i]),
                        .value_ = std::string(operation.values_[i])});
          if (indexed) field_positions.insert(compact.entries_.size() - 1);
          ++result.integer_;
          result.changed_ = true;
        }
      }
      break;
    }
    case HashOperationKind::kDelete:
      if (edit_leaves || compact_edit) break;
      for (std::string_view field : operation.fields_) {
        auto entry = find_entry(field);
        if (entry != compact.entries_.end()) {
          mark_group_changed(field);
          compact.entries_.erase(entry);
          ++result.integer_;
          result.changed_ = true;
        }
      }
      break;
    case HashOperationKind::kIncrementInteger:
    case HashOperationKind::kIncrementFloat: {
      if (operation.fields_.size() != 1 || operation.values_.size() != 1)
        co_return absl::InvalidArgumentError("invalid Hash increment operands");
      const std::string_view field = operation.fields_.front();
      HashEntry* current = lookup(field);
      auto updated = incremented_value(
          current == nullptr
              ? std::optional<std::string_view>{}
              : std::optional<std::string_view>{current->value_});
      if (!updated.ok()) co_return updated.status();
      mark_group_changed(field);
      if (current == nullptr) {
        compact.entries_.push_back(HashEntry{.digest_ = ComputeDigest(field),
                                             .field_ = std::string(field),
                                             .value_ = std::move(*updated)});
      } else {
        current->value_ = std::move(*updated);
      }
      result.changed_ = true;
      break;
    }
    case HashOperationKind::kPopRandom: {
      auto requested = requested_samples();
      if (!requested.ok()) co_return requested.status();
      auto ranks = SampleUniqueRandomRanks(compact.entries_.size(), *requested,
                                           false, RandomSampleGenerator());
      std::sort(ranks.begin(), ranks.end());
      for (std::uint64_t rank : ranks) {
        mark_group_changed(compact.entries_[rank].field_);
        result.values_.push_back(compact.entries_[rank].field_);
      }
      for (auto it = ranks.rbegin(); it != ranks.rend(); ++it)
        compact.entries_.erase(compact.entries_.begin() + *it);
      result.integer_ = ranks.size();
      result.changed_ = !ranks.empty();
      break;
    }

    default:
      co_return absl::InternalError("read operation reached Hash mutation");
  }

  result.length_ = compact_edit  ? compact_edit->count_
                   : edit_leaves ? edited_length
                   : grouped == nullptr
                       ? compact.entries_.size()
                       : location.logical_size_ - selected_field_count +
                             compact.entries_.size();
  result.key_exists_ = result.length_ != 0;
  if (unlocked_grouped_write) {
    if (!grouped_plan && result.changed_ && result.key_exists_) {
      const std::vector<GroupedRecordId> changed(changed_groups.begin(),
                                                 changed_groups.end());
      auto prepared =
          PrepareGroupedHashMutation(grouped, std::move(compact), changed,
                                     result.length_, grouped->revision());
      if (!prepared.ok()) co_return prepared.status();
      grouped_plan.emplace(std::move(*prepared));
    }
    co_await store.store_state_mutex_.Lock();
    unlock.Adopt();
    const auto valid = ValidateGroupedWriteSnapshot(
        store, partition, db_id, key, grouped, observed_write,
        mutation_precondition != nullptr
            ? mutation_precondition
            : (tx != nullptr ? &tx->mutation_precondition_ : nullptr));
    if (!valid.ok()) co_return valid;
  }
  std::optional<std::string> prepared_compact_payload;
  bool compact_write_promotes = false;
  if (unlocked_compact_write || unlocked_create) {
    if (result.changed_ && result.key_exists_) {
      compact_write_promotes = !compact_edit && NeedsGroupedHash(compact);
      if (unlocked_create && compact_write_promotes) {
        auto prepared = PrepareGroupedHashMutation(nullptr, std::move(compact),
                                                   {}, result.length_, 1);
        if (!prepared.ok()) co_return prepared.status();
        grouped_plan.emplace(std::move(*prepared));
      } else if (!compact_write_promotes && !compact_edit) {
        auto encoded = EncodeHashValue(compact);
        if (!encoded.ok()) co_return encoded.status();
        prepared_compact_payload.emplace(std::move(*encoded));
      }
    }

    LAVIK_FAULT_INJECT(if (unlocked_create) {
      const auto paused =
          co_await PauseGroupedWriteForTest(*store.worker_, key, "create");
      if (!paused.ok()) co_return paused;
    });
    co_await store.store_state_mutex_.Lock();
    unlock.Adopt();
    absl::Status valid;
    if (unlocked_create) {
      // External keys remain digest candidates even after verification;
      // re-resolve them rather than rejecting a valid expired predecessor.
      auto* current = index.Find(digest, key);
      if (current != nullptr && !current->key_complete()) {
        auto verified = co_await FindVerifiedEntry(store, index, digest, key);
        if (!verified.ok()) co_return verified.status();
        current = *verified;
      }
      valid = ValidateCollectionCreateSnapshot(store, partition, db_id, current,
                                               now_ms, observed_write,
                                               mutation_precondition);
    } else {
      valid = ValidateCompactWriteSnapshot(store, partition, db_id, key, digest,
                                           location, observed_write);
    }
    if (!valid.ok()) co_return valid;
  }
  if (!result.changed_) {
    if (value_type == ValueType::kHash &&
        operation.kind_ == HashOperationKind::kSet &&
        !operation.fields_.empty()) {
      const MutationPrecondition* effective_precondition =
          mutation_precondition != nullptr
              ? mutation_precondition
              : (tx != nullptr ? &tx->mutation_precondition_ : nullptr);
      if (effective_precondition != nullptr) {
        absl::Status valid = effective_precondition->Validate();
        if (!valid.ok()) co_return valid;
      }
      tx::CurrentTxShard().MarkWatched(db_id, tx::FingerprintOf(digest));
    }
    co_return result;
  }

  if (replication != nullptr && value_type == ValueType::kSet &&
      operation.kind_ == HashOperationKind::kPopRandom) {
    replication->args_.clear();
    replication->args_.reserve(result.values_.size() + 2);
    replication->args_.emplace_back("SREM");
    replication->args_.emplace_back(key);
    for (const std::optional<std::string>& member : result.values_) {
      if (member.has_value()) replication->args_.push_back(*member);
    }
  } else if (replication != nullptr && value_type == ValueType::kHash &&
             (operation.kind_ == HashOperationKind::kIncrementInteger ||
              operation.kind_ == HashOperationKind::kIncrementFloat)) {
    replication->args_ = {
        "HSET", std::string(key), std::string(operation.fields_.front()),
        operation.kind_ == HashOperationKind::kIncrementInteger
            ? std::to_string(result.signed_integer_)
            : result.scalar_};
  }
  if (result.key_exists_ &&
      (grouped != nullptr || ((unlocked_compact_write || unlocked_create)
                                  ? compact_write_promotes
                                  : NeedsGroupedHash(compact)))) {
    absl::Status written = co_await CommitGroupedHashMutationLocked(
        store, partition, db_id, key, digest, grouped, std::move(compact),
        std::vector<GroupedRecordId>(changed_groups.begin(),
                                     changed_groups.end()),
        result.length_, value_type, expire_at_ms, tx, replication,
        mutation_precondition, grouped_plan ? &*grouped_plan : nullptr);
    if (!written.ok()) co_return written;
    co_return result;
  }
  RecordKind kind = RecordKind::kValue;
  ValueType published_type = value_type;
  std::string payload;
  if (!result.key_exists_) {
    kind = RecordKind::kTombstone;
    published_type = ValueType::kNone;
  } else if (compact_edit) {
    // The borrowed plan is consumed directly into the unpublished record.
  } else if (prepared_compact_payload.has_value()) {
    payload = std::move(*prepared_compact_payload);
  } else {
    auto encoded = EncodeHashValue(compact);
    if (!encoded.ok()) co_return encoded.status();
    payload = std::move(*encoded);
  }
  std::optional<InlineRecordPayload> inline_payload;
  if (compact_edit && result.key_exists_)
    inline_payload.emplace(compact_edit->payload_);
  absl::Status written = co_await AppendLocked(
      store, partition, db_id, key, digest, payload, kind, published_type,
      kind == RecordKind::kValue ? expire_at_ms : 0, tx,
      kind == RecordKind::kValue ? result.length_ : 0, nullptr, nullptr,
      replication, true, nullptr, mutation_precondition, nullptr,
      inline_payload ? &*inline_payload : nullptr);
  if (!written.ok()) co_return written;
  co_return result;
}

}  // namespace lavik::storage
