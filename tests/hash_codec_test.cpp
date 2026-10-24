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

#include <gtest/gtest.h>
#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <random>
#include <string>
#include <vector>

#include "lavik/storage/detail/grouped/hash.h"
#include "lavik/storage/detail/hash_read.h"

namespace lavik::storage {
namespace {
void Put(std::string& bytes, std::size_t at, std::uint64_t value,
         unsigned width = 4) {
  for (unsigned i = 0; i < width; ++i)
    bytes[at + i] = static_cast<char>(value >> (8 * i));
}
std::uint32_t Get(std::string_view bytes, std::size_t at) {
  std::uint32_t value = 0;
  for (unsigned i = 0; i < 4; ++i)
    value |= std::uint32_t(static_cast<unsigned char>(bytes[at + i]))
             << (8 * i);
  return value;
}
HashValue Value(std::size_t count, bool set) {
  HashValue value;
  for (std::size_t i = 0; i < count; ++i)
    value.entries_.push_back(
        {.field_ = "field-" + std::to_string(i),
         .value_ = set ? "" : std::string(i % 41, static_cast<char>(i))});
  value.entries_[0].field_ = "";
  if (count > 1) value.entries_[1].field_ = std::string("a\0\xff", 3);
  return value;
}

TEST(HashV2, LayoutIsPackedAndVersionIsIndependent) {
  // Golden tag freezes both the seed and hash variant in the durable format.
  EXPECT_EQ(HashFieldFingerprint("f"), 0xb4);
  HashValue value{.entries_ = {{.field_ = "ab", .value_ = "XYZ"},
                               {.field_ = "", .value_ = "!"}}};
  auto encoded = EncodeHashValue(value);
  ASSERT_TRUE(encoded.ok());
  EXPECT_EQ(Get(*encoded, 8), 2);
  EXPECT_EQ(kStorageFormatVersion, 1);
  EXPECT_EQ(Get(*encoded, 20), 0);
  EXPECT_EQ(encoded->substr(32, 6), "abXYZ!");
  EXPECT_EQ(encoded->size(), 32 + 6 + 12 + 8 + 2);
  EXPECT_EQ(Get(*encoded, 38), 0);
  EXPECT_EQ(Get(*encoded, 42), 5);
  EXPECT_EQ(Get(*encoded, 46), 6);
  EXPECT_EQ(Get(*encoded, 50), 2);
  EXPECT_EQ(Get(*encoded, 54), 0);
  EXPECT_EQ(static_cast<std::uint8_t>((*encoded)[58]),
            HashFieldFingerprint("ab"));
  EXPECT_EQ(static_cast<std::uint8_t>((*encoded)[59]),
            HashFieldFingerprint(""));
  value.entries_[0].value_.clear();
  value.entries_[1].value_.clear();
  encoded = EncodeHashValue(value);
  ASSERT_TRUE(encoded.ok());
  EXPECT_EQ(Get(*encoded, 20), 1);
  EXPECT_EQ(encoded->size(), 32 + 2 + 12 + 2);
}

TEST(HashV2, LookupMatchesLegacyAcrossVectorBoundariesAndUnalignedPayloads) {
  for (bool set : {false, true})
    for (std::size_t count :
         {1, 2, 15, 16, 17, 31, 32, 33, 63, 64, 65, 128, 257}) {
      SCOPED_TRACE(count);
      auto value = Value(count, set);
      auto legacy = EncodeHashValue(value, kLegacyHashValueVersion);
      auto indexed = EncodeHashValue(value);
      ASSERT_TRUE(legacy.ok());
      ASSERT_TRUE(indexed.ok());
      for (std::size_t shift = 0; shift < 16; ++shift) {
        const auto backing = std::string(shift, '?') + *indexed;
        auto reader =
            HashValueReader::Open(std::string_view(backing).substr(shift));
        ASSERT_TRUE(reader.ok());
        auto old = HashValueReader::Open(*legacy);
        ASSERT_TRUE(old.ok());
        for (const auto& expected : value.entries_) {
          for (bool scalar : {false, true}) {
            auto found = reader->Find(expected.field_, scalar);
            ASSERT_TRUE(found.ok());
            ASSERT_TRUE(*found);
            EXPECT_EQ((**found).field_, expected.field_);
            EXPECT_EQ((**found).value_, expected.value_);
          }
          auto found = old->Find(expected.field_);
          ASSERT_TRUE(found.ok());
          ASSERT_TRUE(*found);
          EXPECT_EQ((**found).value_, expected.value_);
        }
        EXPECT_FALSE(*reader->Find("missing"));
        EXPECT_FALSE(*old->Find("missing"));
      }
      auto decoded = DecodeHashValue(*indexed);
      ASSERT_TRUE(decoded.ok());
      ASSERT_EQ(decoded->entries_.size(), count);
      for (std::size_t i = 0; i < count; ++i) {
        EXPECT_EQ(decoded->entries_[i].field_, value.entries_[i].field_);
        EXPECT_EQ(decoded->entries_[i].value_, value.entries_[i].value_);
      }
    }
}

TEST(HashV2, FingerprintCollisionsStillCompareCompleteFields) {
  HashValue value;
  const auto tag = HashFieldFingerprint("wanted");
  for (unsigned i = 0; value.entries_.size() < 40; ++i) {
    auto field = "collision-" + std::to_string(i);
    if (HashFieldFingerprint(field) == tag)
      value.entries_.push_back({.field_ = std::move(field), .value_ = "wrong"});
  }
  auto encoded = EncodeHashValue(value);
  ASSERT_TRUE(encoded.ok());
  auto reader = HashValueReader::Open(*encoded);
  ASSERT_TRUE(reader.ok());
  EXPECT_FALSE(*reader->Find("wanted"));
  value.entries_.push_back({.field_ = "wanted", .value_ = "right"});
  encoded = EncodeHashValue(value);
  reader = HashValueReader::Open(*encoded);
  auto found = reader->Find("wanted");
  ASSERT_TRUE(found.ok());
  ASSERT_TRUE(*found);
  EXPECT_EQ((**found).value_, "right");
}

TEST(HashV2, LookupReturnsPhysicalSlotsAndHonorsStart) {
  for (auto version : {kLegacyHashValueVersion, kHashValueVersion}) {
    for (std::size_t count : {1, 15, 16, 17, 31, 32, 33, 65}) {
      const auto value = Value(count, false);
      auto bytes = EncodeHashValue(value, version);
      ASSERT_TRUE(bytes.ok());
      auto reader = HashValueReader::Open(*bytes);
      ASSERT_TRUE(reader.ok());
      ASSERT_TRUE(reader->Next().ok());  // Find ignores traversal position.
      for (std::size_t first = 0; first <= count; ++first) {
        for (std::size_t target = 0; target < count; ++target) {
          for (bool scalar : {false, true}) {
            std::size_t slot = SIZE_MAX;
            auto found = reader->Find(value.entries_[target].field_, scalar,
                                      &slot, first);
            ASSERT_TRUE(found.ok());
            ASSERT_EQ(found->has_value(), target >= first);
            EXPECT_EQ(slot, target >= first ? target : SIZE_MAX);
            if (*found)
              EXPECT_EQ((**found).value_, value.entries_[target].value_);
          }
        }
      }
      std::size_t slot = SIZE_MAX;
      auto invalid = reader->Find("", false, &slot, count + 1);
      EXPECT_EQ(invalid.status().code(), absl::StatusCode::kOutOfRange);
      EXPECT_EQ(slot, SIZE_MAX);
    }
  }
}

TEST(HashV2, CompactAndGroupedEditsResolveCollidingTagsAndRepeatedOperands) {
  HashValue original{.entries_ = {{.field_ = "", .value_ = ""}}};
  const auto tag = HashFieldFingerprint("");
  for (unsigned i = 0; original.entries_.size() < 66; ++i) {
    auto field = "collision-" + std::to_string(i);
    if (HashFieldFingerprint(field) == tag)
      original.entries_.push_back(
          {.field_ = std::move(field), .value_ = "old"});
  }
  const auto added_field = original.entries_.back().field_;
  original.entries_.pop_back();
  const std::array<std::string_view, 6> fields{original.entries_[31].field_,
                                               added_field,
                                               "",
                                               added_field,
                                               original.entries_[64].field_,
                                               original.entries_[31].field_};
  const std::array<std::string_view, 6> values{
      "temporary", "first", "empty-key", "last", "tail", "old"};
  auto expected = original;
  std::vector<HashEntryView> edits;
  for (std::size_t i = 0; i < fields.size(); ++i) {
    edits.push_back({fields[i], values[i]});
    auto found = std::find_if(
        expected.entries_.begin(), expected.entries_.end(),
        [&](const auto& entry) { return entry.field_ == fields[i]; });
    if (found == expected.entries_.end())
      expected.entries_.push_back(
          {.field_ = std::string(fields[i]), .value_ = std::string(values[i])});
    else
      found->value_ = values[i];
  }
  const auto reference = EncodeHashValue(expected);
  ASSERT_TRUE(reference.ok());
  for (auto version : {kLegacyHashValueVersion, kHashValueVersion}) {
    auto bytes = EncodeHashValue(original, version);
    ASSERT_TRUE(bytes.ok());
    auto compact = EditCompactHash(*bytes, 65, fields, values, 16383);
    ASSERT_TRUE(compact.ok());
    ASSERT_TRUE(*compact);
    EXPECT_TRUE((**compact).changed_);
    EXPECT_EQ((**compact).added_, 1);
    EXPECT_EQ((**compact).payload_, *reference);
    auto grouped = EncodeHashGroup({.incarnation_ = 17, .value_ = original});
    ASSERT_TRUE(grouped.ok());
    grouped->resize(kHashGroupHeaderBytes);
    grouped->append(*bytes);
    Put(*grouped, 36, bytes->size());
    auto changed = ApplyHashGroupEdits(*grouped, DigestSeed{},
                                       HashGroupEditKind::kSet, edits);
    ASSERT_TRUE(changed.ok()) << changed.status();
    ASSERT_EQ(changed->leaves_.size(), 1);
    EXPECT_TRUE(changed->changed_);
    EXPECT_EQ(changed->added_, 1);
    auto encoded = EncodeHashGroup(changed->leaves_[0]);
    ASSERT_TRUE(encoded.ok());
    EXPECT_EQ(encoded->substr(kHashGroupHeaderBytes), *reference);
  }
}

TEST(HashV2, CompactNoopRejectsDuplicateRequestedFieldsWithValidTags) {
  auto value = Value(65, false);
  value.entries_.back().field_ = value.entries_[0].field_;
  const std::array<std::string_view, 1> fields{value.entries_[0].field_};
  const std::array<std::string_view, 1> values{value.entries_[0].value_};
  for (auto version : {kLegacyHashValueVersion, kHashValueVersion}) {
    auto bytes = EncodeHashValue(value, version);
    ASSERT_TRUE(bytes.ok());
    auto result =
        EditCompactHash(*bytes, value.entries_.size(), fields, values, 16383);
    EXPECT_EQ(result.status().code(), absl::StatusCode::kDataLoss);
  }
}

TEST(HashV2, FingerprintDoesNotDependOnProcessSeed) {
  const auto seed = CurrentDigestSeed();
  const auto value = Value(33, false);
  auto before = EncodeHashValue(value);
  auto different = seed;
  different[0] ^= 0xff;
  RestoreDigestSeed(different);
  auto after = EncodeHashValue(value);
  RestoreDigestSeed(seed);
  ASSERT_TRUE(before.ok());
  ASSERT_TRUE(after.ok());
  EXPECT_EQ(*before, *after);
}

TEST(HashV2, StrictDecodeRejectsMalformedArraysTagsAndFutureVersions) {
  auto encoded = EncodeHashValue(Value(3, false));
  ASSERT_TRUE(encoded.ok());
  const auto tags = encoded->size() - 3;
  const auto lengths = tags - 12;
  const auto offsets = lengths - 16;
  for (auto at : {std::size_t{0}, std::size_t{8}, std::size_t{12},
                  std::size_t{16}, std::size_t{20}, std::size_t{24}, offsets,
                  offsets + 4, offsets + 12, lengths, lengths + 4}) {
    auto broken = *encoded;
    Put(broken, at, UINT32_MAX);
    EXPECT_FALSE(DecodeHashValue(broken).ok()) << at;
  }
  auto broken = *encoded;
  broken[tags] ^= 1;
  EXPECT_FALSE(DecodeHashValue(broken).ok());
  for (std::size_t length = 0; length < encoded->size(); ++length) {
    broken = encoded->substr(0, length);
    if (length >= 32) Put(broken, 24, length, 8);
    EXPECT_FALSE(DecodeHashValue(broken).ok()) << length;
  }
  // Direct candidate access checks offsets even when its tag matches.
  broken = *encoded;
  Put(broken, offsets + 4, UINT32_MAX);
  auto reader = HashValueReader::Open(broken);
  ASSERT_TRUE(reader.ok());
  EXPECT_FALSE(reader->Find("").ok());
}

TEST(HashV2, SIMDDoesNotReadBeyondPayloadAtGuardPage) {
  const auto page = static_cast<std::size_t>(sysconf(_SC_PAGESIZE));
  void* memory = mmap(nullptr, 2 * page, PROT_READ | PROT_WRITE,
                      MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  ASSERT_NE(memory, MAP_FAILED);
  ASSERT_EQ(mprotect(static_cast<char*>(memory) + page, page, PROT_NONE), 0);
  for (std::size_t count : {1, 15, 16, 17, 31, 32, 33, 63, 64, 65}) {
    auto encoded = EncodeHashValue(Value(count, true));
    ASSERT_TRUE(encoded.ok());
    ASSERT_LT(encoded->size(), page);
    auto* begin = static_cast<char*>(memory) + page - encoded->size();
    std::memcpy(begin, encoded->data(), encoded->size());
    auto reader = HashValueReader::Open({begin, encoded->size()});
    ASSERT_TRUE(reader.ok());
    EXPECT_FALSE(*reader->Find("absent"));
    const auto last = count == 1 ? "" : "field-" + std::to_string(count - 1);
    EXPECT_TRUE(*reader->Find(last));
    for (std::size_t first : {std::size_t{0}, count / 2, count}) {
      std::size_t slot = SIZE_MAX;
      auto found = reader->Find(last, false, &slot, first);
      ASSERT_TRUE(found.ok());
      EXPECT_EQ(found->has_value(), first < count);
      EXPECT_EQ(slot, first < count ? count - 1 : SIZE_MAX);
      EXPECT_FALSE(*reader->Find("absent", false, nullptr, first));
    }
  }
  EXPECT_EQ(munmap(memory, 2 * page), 0);
}

TEST(HashV2, LegacyAndIndexedEditsProduceTheSameV2AfterImage) {
  const auto original = Value(65, false);
  const std::array<std::string_view, 4> fields{"field-2", "new", "new", ""};
  const std::array<std::string_view, 4> values{"changed", "temporary", "final",
                                               "empty-key"};
  std::optional<std::string> expected;
  for (auto version : {kLegacyHashValueVersion, kHashValueVersion}) {
    auto bytes = EncodeHashValue(original, version);
    ASSERT_TRUE(bytes.ok());
    auto edited = EditCompactHash(*bytes, 65, fields, values, 16383);
    ASSERT_TRUE(edited.ok());
    ASSERT_TRUE(*edited);
    EXPECT_EQ(Get((**edited).payload_, 8), kHashValueVersion);
    if (expected)
      EXPECT_EQ((**edited).payload_, *expected);
    else
      expected = (**edited).payload_;
    auto decoded = DecodeHashValue((**edited).payload_);
    ASSERT_TRUE(decoded.ok());
    EXPECT_EQ(decoded->entries_.size(), 66);
  }
}

TEST(HashV2, CompactEditsAddAndRemoveTheOptionalFieldLengthArray) {
  const auto original = Value(33, true);
  const std::array<std::string_view, 1> fields{"field-2"};
  for (auto version : {kLegacyHashValueVersion, kHashValueVersion}) {
    auto bytes = EncodeHashValue(original, version);
    ASSERT_TRUE(bytes.ok());
    for (std::string_view replacement : {"nonempty", ""}) {
      const std::array<std::string_view, 1> values{replacement};
      auto edited = EditCompactHash(*bytes, 33, fields, values, 16383);
      ASSERT_TRUE(edited.ok());
      ASSERT_TRUE(*edited);
      bytes = std::move((**edited).payload_);
      EXPECT_EQ(Get(*bytes, 20), replacement.empty() ? 1 : 0);
      auto reader = HashValueReader::Open(*bytes);
      ASSERT_TRUE(reader.ok());
      ASSERT_TRUE(reader->ValidateFingerprints().ok());
      for (const auto& entry : original.entries_) {
        auto found = reader->Find(entry.field_);
        ASSERT_TRUE(found.ok());
        ASSERT_TRUE(*found);
        EXPECT_EQ((**found).value_,
                  entry.field_ == fields[0] ? replacement : entry.value_);
      }
    }
  }
}
}  // namespace
}  // namespace lavik::storage
