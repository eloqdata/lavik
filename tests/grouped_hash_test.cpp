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

#include "lavik/storage/detail/grouped_hash.h"

#include <algorithm>
#include <array>
#include <limits>
#include <map>
#include <random>
#include <string>
#include <vector>

#include "gtest/gtest.h"

namespace lavik::storage {
namespace {

// Legacy fixture sequences model both order domains equally. Tests for replay
// below call the production API directly with deliberately different C and R.
absl::StatusOr<HashGroupDirectory> Recover(
    GroupedHashRoot root, std::uint64_t sequence,
    std::span<const RecoveredHashGroup> candidates,
    const absl::flat_hash_set<std::uint64_t>& committed) {
  root.revision_ = sequence;
  return HashGroupDirectory::Recover(root, sequence, candidates, committed);
}

DigestSeed Seed(unsigned value = 7) {
  DigestSeed seed{};
  for (std::size_t i = 0; i < seed.size(); ++i) seed[i] = value + i;
  return seed;
}

HashValue Value(std::size_t count, std::size_t length = 128) {
  HashValue value;
  for (std::size_t i = 0; i < count; ++i) {
    auto field = "field-" + std::to_string(i);
    value.entries_.push_back({.digest_ = ComputeDigest(field),
                              .field_ = std::move(field),
                              .value_ = std::string(length, 'a' + i % 26)});
  }
  return value;
}

GroupedHashRoot Root(std::uint64_t count, std::uint32_t groups = 1) {
  return {.incarnation_ = 17,
          .seed_ = Seed(),
          .field_count_ = count,
          .group_count_ = groups,
          .revision_ = 1};
}

std::vector<RecoveredHashGroup> Candidates(
    const std::vector<HashGroupSnapshot>& groups, std::uint64_t seq = 1,
    std::uint64_t txid = 0) {
  std::vector<RecoveredHashGroup> candidates;
  for (const auto& group : groups) {
    candidates.push_back({.incarnation_ = group.incarnation_,
                          .id_ = group.id_,
                          .sequence_ = seq,
                          .lsn_ = seq,
                          .txid_ = txid,
                          .field_count_ = group.value_.entries_.size(),
                          .record_token_ = candidates.size() + 1,
                          .retired_ = group.retired_});
  }
  return candidates;
}

TEST(GroupedHashTest, PrefixBoundsIncludeZeroAndFullWidth) {
  const auto maximum = std::numeric_limits<std::uint64_t>::max();
  EXPECT_TRUE((HashGroupId{}).contains(0));
  EXPECT_TRUE((HashGroupId{}).contains(maximum));
  EXPECT_EQ((HashGroupId{}).last(), maximum);
  EXPECT_FALSE((HashGroupId{1, 0}).valid());
  EXPECT_FALSE((HashGroupId{0, 65}).valid());
  EXPECT_TRUE((HashGroupId{maximum, 64}).contains(maximum));
  EXPECT_FALSE((HashGroupId{maximum, 64}).contains(maximum - 1));
  EXPECT_EQ((HashGroupId{0, 1}).last(), maximum >> 1);
}

TEST(GroupedHashTest, CompactPayloadHasAnExplicitLittleEndianHeader) {
  constexpr std::array<unsigned char, 42> fixture{
      'L', 'H', 'V', 'A', 'L', 'U', 'E', '1', 1, 0, 0,  0, 32,  0,
      0,   0,   1,   0,   0,   0,   0,   0,   0, 0, 42, 0, 0,   0,
      0,   0,   0,   0,   1,   0,   0,   0,   1, 0, 0,  0, 'f', 'v'};
  const std::string bytes(fixture.begin(), fixture.end());
  auto value = DecodeHashValue(bytes);
  ASSERT_TRUE(value.ok()) << value.status();
  ASSERT_EQ(value->entries_.size(), 1);
  EXPECT_EQ(value->entries_[0].field_, "f");
  EXPECT_EQ(value->entries_[0].value_, "v");
  auto encoded = EncodeHashValue(*value);
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  EXPECT_EQ(*encoded, bytes);
  for (std::size_t offset : {8, 12, 16, 20, 24, 32, 36}) {
    auto broken = bytes;
    broken[offset] = static_cast<char>(0xff);
    EXPECT_FALSE(DecodeHashValue(broken).ok());
  }
}

TEST(GroupedHashTest, CompactEncodingMatchesAppendReferenceAtUnalignedLengths) {
  HashValue value;
  for (const std::size_t length :
       {0, 1, 7, 15, 16, 127, 128, 255, 256, 65537}) {
    auto& entry = value.entries_.emplace_back();
    entry.field_.resize(length);
    entry.value_.resize(length + 1);
    for (std::size_t i = 0; i < entry.field_.size(); ++i)
      entry.field_[i] = static_cast<char>(i);
    for (std::size_t i = 0; i < entry.value_.size(); ++i)
      entry.value_[i] = static_cast<char>(255 - i);
  }
  // Independent append-based reference checks every durable byte, including
  // binary strings, multi-byte lengths, and headers starting at odd offsets.
  // A round trip alone could miss matching encoder/decoder format mistakes.
  std::size_t bytes = kHashValueHeaderBytes;
  for (const auto& entry : value.entries_)
    bytes += 8 + entry.field_.size() + entry.value_.size();
  std::string reference;
  auto append_integer = [&](std::uint64_t number, std::size_t width) {
    for (std::size_t i = 0; i < width; ++i)
      reference.push_back(static_cast<char>(number >> (8 * i)));
  };
  append_integer(kHashValueMagic, 8);
  append_integer(kStorageFormatVersion, 4);
  append_integer(kHashValueHeaderBytes, 4);
  append_integer(value.entries_.size(), 4);
  append_integer(0, 4);
  append_integer(bytes, 8);
  for (const auto& entry : value.entries_) {
    append_integer(entry.field_.size(), 4);
    append_integer(entry.value_.size(), 4);
    reference.append(entry.field_);
    reference.append(entry.value_);
  }
  auto encoded = EncodeHashValue(value);
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  EXPECT_EQ(*encoded, reference);
  EXPECT_EQ((*encoded)[encoded->size()], '\0');
  EXPECT_FALSE(EncodeHashValue(HashValue{}).ok());
}

TEST(GroupedHashTest, CompactReaderPreservesBinaryViewsAndCopiedPosition) {
  auto value = Value(3);
  value.entries_[0].field_ = std::string("a\0b", 3);
  value.entries_[0].value_ = std::string("\0x\xff", 3);
  value.entries_[1].field_.clear();
  value.entries_[1].value_.clear();
  auto encoded = EncodeHashValue(value);
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  auto reader = HashValueReader::Open(*encoded);
  ASSERT_TRUE(reader.ok()) << reader.status();
  ASSERT_EQ(reader->size(), value.entries_.size());
  auto copy = *reader;
  for (const auto& expected : value.entries_) {
    auto entry = reader->Next();
    ASSERT_TRUE(entry.ok()) << entry.status();
    EXPECT_EQ(entry->field_, expected.field_);
    EXPECT_EQ(entry->value_, expected.value_);
    EXPECT_GE(entry->field_.data(), encoded->data());
    EXPECT_LE(entry->value_.data() + entry->value_.size(),
              encoded->data() + encoded->size());
  }
  EXPECT_EQ(reader->Next().status().code(), absl::StatusCode::kOutOfRange);
  auto first = copy.Next();
  ASSERT_TRUE(first.ok()) << first.status();
  EXPECT_EQ(first->field_, value.entries_[0].field_);
}

TEST(GroupedHashTest, CompactReaderRejectsMalformedUnselectedBytes) {
  auto encoded = EncodeHashValue(Value(2));
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  auto check = [](std::string_view payload) {
    auto reader = HashValueReader::Open(payload);
    if (!reader.ok()) return reader.status();
    for (std::size_t i = 0; i < reader->size(); ++i) {
      auto entry = reader->Next();
      if (!entry.ok()) {
        // A failed read must remain failed rather than skipping bad bytes.
        EXPECT_EQ(reader->Next().status(), entry.status());
        return entry.status();
      }
    }
    return absl::OkStatus();
  };
  auto set_size = [](std::string& payload) {
    for (std::size_t i = 0; i < 8; ++i)
      payload[24 + i] = static_cast<char>(payload.size() >> (8 * i));
  };
  for (std::size_t length = 0; length < encoded->size(); ++length) {
    auto broken = encoded->substr(0, length);
    EXPECT_FALSE(check(broken).ok()) << length;
    if (length >= kHashValueHeaderBytes) {
      set_size(broken);
      EXPECT_FALSE(check(broken).ok()) << length;
    }
  }
  auto trailing = *encoded + "x";
  set_size(trailing);
  EXPECT_EQ(check(trailing).message(), "Hash value has trailing bytes");
  for (std::size_t offset : {0, 8, 12, 16, 20, 24, 32, 36}) {
    auto broken = *encoded;
    for (std::size_t i = 0; i < 4; ++i) broken[offset + i] = '\xff';
    EXPECT_FALSE(check(broken).ok()) << offset;
  }
  // Lowering the count can leave a syntactically valid first pair: the final
  // Next must still reject the omitted pair, even for HKEYS/HVALS selection.
  auto wrong_count = *encoded;
  wrong_count[16] = 1;
  EXPECT_EQ(check(wrong_count).message(), "Hash value has trailing bytes");
}

TEST(GroupedHashTest, RootRoundTripsAndRejectsMalformedEnvelopes) {
  const auto root = Root(100, 9);
  auto encoded = EncodeGroupedHashRoot(root);
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  EXPECT_EQ(encoded->size(), kGroupedHashRootBytes);
  auto decoded = DecodeGroupedHashRoot(*encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(*decoded, root);
  for (std::size_t length = 0; length < encoded->size(); ++length) {
    EXPECT_FALSE(DecodeGroupedHashRoot(encoded->substr(0, length)).ok());
  }
  EXPECT_FALSE(DecodeGroupedHashRoot(*encoded + "x").ok());
  for (std::size_t offset : {0, 8, 12, 52}) {
    auto broken = *encoded;
    broken[offset] ^= 0x7f;
    EXPECT_FALSE(DecodeGroupedHashRoot(broken).ok());
  }
  auto invalid = root;
  invalid.incarnation_ = 0;
  EXPECT_FALSE(EncodeGroupedHashRoot(invalid).ok());
  invalid = root;
  invalid.field_count_ = 0;
  EXPECT_FALSE(EncodeGroupedHashRoot(invalid).ok());
}

TEST(GroupedHashTest, RejectsInnerCountBeforeDecodingEntryStorage) {
  HashGroupSnapshot group{.incarnation_ = 17, .value_ = Value(1, 4096)};
  auto encoded = EncodeHashGroup(group);
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  // Still plausible relative to the payload length, but greater than the
  // captured outer count used to admit the decoded page's vector capacity.
  (*encoded)[kHashGroupHeaderBytes + 16] = static_cast<char>(128);
  auto metadata = DecodeHashGroupMetadata(*encoded, encoded->size());
  ASSERT_TRUE(metadata.ok()) << metadata.status();
  ASSERT_EQ(metadata->field_count_, 1);
  auto decoded = DecodeHashGroup(*encoded);
  ASSERT_FALSE(decoded.ok());
  EXPECT_EQ(decoded.status().code(), absl::StatusCode::kDataLoss);
  EXPECT_EQ(decoded.status().message(),
            "Hash group inner count disagrees with envelope");
}

TEST(GroupedHashTest, ReplayCommandSequenceIsIndependentOfGroupRevision) {
  auto root = Root(1);
  root.revision_ = 100;
  std::vector<RecoveredHashGroup> candidates{
      {.incarnation_ = root.incarnation_, .sequence_ = 100, .field_count_ = 1},
      {.incarnation_ = root.incarnation_, .sequence_ = 101, .field_count_ = 2}};
  auto first = HashGroupDirectory::Recover(root, 7, candidates, {});
  ASSERT_TRUE(first.ok()) << first.status();
  EXPECT_EQ(first->sequence(), 100);
  EXPECT_EQ(first->command_sequence(), 7);
  root.revision_ = 101;
  root.field_count_ = 2;
  auto next = first->Apply(root, 7, std::span(candidates).last(1));
  ASSERT_TRUE(next.ok()) << next.status();
  EXPECT_EQ(next->sequence(), 101);
  EXPECT_EQ(next->command_sequence(), 7);
  EXPECT_FALSE(next->Apply(root, 7, std::span(candidates).last(1)).ok());
  root.revision_ = 102;
  candidates.back().sequence_ = 102;
  EXPECT_FALSE(next->Apply(root, 6, std::span(candidates).last(1)).ok());
  auto recovered = HashGroupDirectory::Recover(root, 7, candidates, {});
  ASSERT_TRUE(recovered.ok()) << recovered.status();
  EXPECT_EQ(recovered->Find("field")->sequence_, 102);
  candidates.push_back(candidates.back());
  candidates.back().field_count_ = 3;
  EXPECT_FALSE(HashGroupDirectory::Recover(root, 7, candidates, {}).ok());
}

TEST(GroupedHashTest, CompleteSnapshotsAreBinarySafeAndNotMutationLogs) {
  HashGroupSnapshot group{.incarnation_ = 17, .value_ = Value(3)};
  group.value_.entries_[0].field_ = std::string("a\0b", 3);
  group.value_.entries_[0].value_ = std::string("\0x\xff", 3);
  group.value_.entries_[1].field_.clear();
  group.value_.entries_[1].value_.clear();
  auto encoded = EncodeHashGroup(group);
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  auto decoded = DecodeHashGroup(*encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  ASSERT_EQ(decoded->value_.entries_.size(), 3);
  EXPECT_EQ(decoded->incarnation_, group.incarnation_);
  EXPECT_EQ(decoded->id_, group.id_);
  for (std::size_t i = 0; i < 3; ++i) {
    EXPECT_EQ(decoded->value_.entries_[i].field_,
              group.value_.entries_[i].field_);
    EXPECT_EQ(decoded->value_.entries_[i].value_,
              group.value_.entries_[i].value_);
    EXPECT_EQ(decoded->value_.entries_[i].digest_,
              ComputeDigest(group.value_.entries_[i].field_));
  }
  for (std::size_t length = 0; length < encoded->size(); ++length) {
    EXPECT_FALSE(DecodeHashGroup(encoded->substr(0, length)).ok());
  }
  EXPECT_FALSE(DecodeHashGroup(*encoded + "x").ok());
  for (std::size_t offset : {0, 8, 12, 32, 36, 40, 41, 42}) {
    auto broken = *encoded;
    broken[offset] ^= 0x7f;
    EXPECT_FALSE(DecodeHashGroup(broken).ok()) << offset;
  }
}

TEST(GroupedHashTest, EmptyLeafAndRetiredLeafAreDifferentStates) {
  for (bool retired : {false, true}) {
    HashGroupSnapshot group{.incarnation_ = 17, .retired_ = retired};
    auto encoded = EncodeHashGroup(group);
    ASSERT_TRUE(encoded.ok()) << encoded.status();
    EXPECT_EQ(encoded->size(), 48);
    auto decoded = DecodeHashGroup(*encoded);
    ASSERT_TRUE(decoded.ok()) << decoded.status();
    EXPECT_EQ(decoded->retired_, retired);
    EXPECT_TRUE(decoded->value_.entries_.empty());
  }
  HashGroupSnapshot bad{
      .incarnation_ = 17, .retired_ = true, .value_ = Value(1)};
  EXPECT_FALSE(EncodeHashGroup(bad).ok());
}

TEST(GroupedHashTest, DuplicateFieldsAreRejectedBeforePublication) {
  auto value = Value(3);
  value.entries_[1].field_ = value.entries_[0].field_;
  EXPECT_FALSE(GroupHashValue(value, 17, Seed()).ok());
  EXPECT_FALSE(EncodeHashGroup({.incarnation_ = 17, .value_ = value}).ok());
  // Also reject duplicate fields introduced in otherwise valid compact bytes.
  auto compact = EncodeHashValue(value);
  ASSERT_TRUE(compact.ok());
  auto envelope = EncodeHashGroup({.incarnation_ = 17, .value_ = Value(3)});
  ASSERT_TRUE(envelope.ok());
  envelope->replace(48, std::string::npos, *compact);
  EXPECT_FALSE(DecodeHashGroup(*envelope).ok());
}

TEST(GroupedHashTest, PromotionRoutesEveryFieldToOneBoundedGroup) {
  for (unsigned seed_id = 0; seed_id < 16; ++seed_id) {
    const auto seed = Seed(seed_id);
    const auto value = Value(1000);
    auto groups = GroupHashValue(value, 17, seed);
    ASSERT_TRUE(groups.ok()) << groups.status();
    ASSERT_GT(groups->size(), 1);
    auto root = Root(1000, groups->size());
    root.seed_ = seed;
    auto candidates = Candidates(*groups);
    std::mt19937 random(seed_id);
    std::shuffle(candidates.begin(), candidates.end(), random);
    auto directory = Recover(root, 1, candidates, {});
    ASSERT_TRUE(directory.ok()) << directory.status();
    std::map<std::string, std::string> all;
    for (const auto& group : *groups) {
      auto encoded = EncodeHashGroup(group);
      ASSERT_TRUE(encoded.ok()) << encoded.status();
      EXPECT_LE(encoded->size(), kHashGroupTargetBytes + 48);
      for (const auto& entry : group.value_.entries_) {
        const auto* selected = directory->Find(entry.field_);
        ASSERT_NE(selected, nullptr);
        EXPECT_EQ(selected->id_, group.id_);
        EXPECT_TRUE(all.emplace(entry.field_, entry.value_).second);
      }
    }
    ASSERT_EQ(all.size(), value.entries_.size());
    for (const auto& entry : value.entries_)
      EXPECT_EQ(all.at(entry.field_), entry.value_);
  }
}

TEST(GroupedHashTest, LargeIndivisibleFieldDoesNotCauseRecursiveExplosion) {
  auto groups = GroupHashValue(Value(1, 1024 * 1024), 17, Seed());
  ASSERT_TRUE(groups.ok()) << groups.status();
  ASSERT_EQ(groups->size(), 1);
  EXPECT_EQ(groups->front().id_, HashGroupId{});
  EXPECT_EQ(groups->front().value_.entries_[0].value_.size(), 1024 * 1024);
  EXPECT_FALSE(GroupHashValue(Value(1), 17, Seed(), 0).ok());
  EXPECT_FALSE(GroupHashValue({}, 17, Seed()).ok());
}

TEST(GroupedHashTest, MaximalIndividualValueLeavesRoomForGroupFraming) {
  // Check the actual 512 MiB boundary without allocating half a gigabyte just
  // to exercise arithmetic. The group envelope must allow the field name and
  // framing in addition to the maximal individual value.
  auto size = AppendHashEntrySize(kHashValueHeaderBytes, 6, kMaxStringBytes,
                                  kHashGroupPayloadLimit);
  ASSERT_TRUE(size.ok()) << size.status();
  EXPECT_EQ(*size, kHashValueHeaderBytes + 8 + 6 + kMaxStringBytes);
  EXPECT_FALSE(AppendHashEntrySize(kHashValueHeaderBytes, 6, kMaxStringBytes,
                                   kMaxStringBytes)
                   .ok());
  EXPECT_FALSE(AppendHashEntrySize(kHashValueHeaderBytes, 6,
                                   kMaxStringBytes + 1, kHashGroupPayloadLimit)
                   .ok());
  EXPECT_FALSE(AppendHashEntrySize(kHashValueHeaderBytes, kMaxStringBytes + 1,
                                   0, kHashGroupPayloadLimit)
                   .ok());
  EXPECT_FALSE(AppendHashEntrySize(std::numeric_limits<std::size_t>::max(), 1,
                                   1, kHashGroupPayloadLimit)
                   .ok());
  EXPECT_FALSE(AppendHashEntrySize(0, 0, 0, 7).ok());
  const auto exact = AppendHashEntrySize(kHashGroupPayloadLimit - 8, 0, 0,
                                         kHashGroupPayloadLimit);
  ASSERT_TRUE(exact.ok());
  EXPECT_EQ(*exact + kHashGroupHeaderBytes, kMaxRecordPayloadBytes);
  EXPECT_FALSE(AppendHashEntrySize(*exact, 0, 0, kHashGroupPayloadLimit).ok());
}

TEST(GroupedHashTest, StreamingEncoderBorrowsTheLargeValueAndMatchesCodec) {
  HashGroupSnapshot group{.incarnation_ = 17,
                          .value_ = Value(1, kExtentPayloadBytes + 123)};
  auto encoded = EncodeHashGroup(group);
  ASSERT_TRUE(encoded.ok());
  auto cursor = HashGroupEncoder::Create(group);
  ASSERT_TRUE(cursor.ok());
  EXPECT_EQ(cursor->encoded_bytes(), encoded->size());
  const std::string& value = group.value_.entries_[0].value_;
  bool borrowed = false;
  std::string assembled;
  // Tiny destination slices deliberately cut across headers, entry lengths,
  // names and values; an extent consumer must not assume record alignment.
  while (auto span = cursor->Next()) {
    if (span->data() == value.data()) {
      borrowed = true;
      EXPECT_EQ(span->size(), value.size());
    }
    for (std::size_t offset = 0; offset < span->size(); offset += 4093) {
      assembled.append(span->substr(
          offset, std::min<std::size_t>(4093, span->size() - offset)));
    }
  }
  EXPECT_TRUE(borrowed);
  EXPECT_FALSE(cursor->Next().has_value());
  EXPECT_EQ(assembled, *encoded);
  auto decoded = DecodeHashGroup(assembled);
  ASSERT_TRUE(decoded.ok());
  ASSERT_EQ(decoded->value_.entries_.size(), 1);
  EXPECT_EQ(decoded->value_.entries_[0].value_, value);
}

TEST(GroupedHashTest, StreamingEncoderDistinguishesEmptySpansFromEnd) {
  HashGroupSnapshot group{.incarnation_ = 17};
  group.value_.entries_.push_back({.field_ = "", .value_ = ""});
  group.value_.entries_.push_back({.field_ = "other", .value_ = ""});
  auto cursor = HashGroupEncoder::Create(group);
  ASSERT_TRUE(cursor.ok());
  std::size_t empty_spans = 0;
  std::string bytes;
  while (auto span = cursor->Next()) {
    empty_spans += span->empty();
    bytes.append(*span);
  }
  EXPECT_EQ(empty_spans, 3);
  auto decoded = DecodeHashGroup(bytes);
  ASSERT_TRUE(decoded.ok());
  EXPECT_EQ(decoded->value_.entries_.size(), 2);
  for (bool retired : {false, true}) {
    HashGroupSnapshot empty{.incarnation_ = 17, .retired_ = retired};
    auto empty_cursor = HashGroupEncoder::Create(empty);
    ASSERT_TRUE(empty_cursor.ok());
    auto header = empty_cursor->Next();
    ASSERT_TRUE(header.has_value());
    EXPECT_EQ(header->size(), kHashGroupHeaderBytes);
    EXPECT_FALSE(empty_cursor->Next().has_value());
    auto decoded_empty = DecodeHashGroup(*header);
    ASSERT_TRUE(decoded_empty.ok());
    EXPECT_EQ(decoded_empty->retired_, retired);
  }
}

TEST(GroupedHashTest, StreamingEncoderValidatesBeforeEmittingAnyBytes) {
  HashGroupSnapshot group{.incarnation_ = 17, .value_ = Value(1)};
  group.value_.entries_.push_back(group.value_.entries_[0]);
  EXPECT_FALSE(HashGroupEncoder::Create(group).ok());
  group.value_.entries_.pop_back();
  group.retired_ = true;
  EXPECT_FALSE(HashGroupEncoder::Create(group).ok());
  group.retired_ = false;
  group.incarnation_ = 0;
  EXPECT_FALSE(HashGroupEncoder::Create(group).ok());
}

TEST(GroupedHashTest, LogicalEncodingSizeCanExceedStringAndRecordLimits) {
  const auto limit = std::string().max_size();
  std::size_t bytes = kHashValueHeaderBytes;
  for (unsigned i = 0; i < 3; ++i) {
    auto next = AppendHashEntrySize(bytes, 6, kMaxStringBytes, limit);
    ASSERT_TRUE(next.ok()) << next.status();
    bytes = *next;
  }
  EXPECT_GT(bytes, kMaxRecordPayloadBytes);
  EXPECT_EQ(bytes, kHashValueHeaderBytes + 3 * (8 + 6 + kMaxStringBytes));
  EXPECT_FALSE(AppendHashEntrySize(0, kMaxStringBytes + 1, 0, limit).ok());
  EXPECT_FALSE(AppendHashEntrySize(0, 0, kMaxStringBytes + 1, limit).ok());
  EXPECT_FALSE(AppendHashEntrySize(limit - 7, 0, 0, limit).ok());
  EXPECT_FALSE(AppendHashEntrySize(limit - 8, 1, 0, limit).ok());
  EXPECT_FALSE(AppendHashEntrySize(limit - 8, 0, 1, limit).ok());
  EXPECT_EQ(*AppendHashEntrySize(limit - 8, 0, 0, limit), limit);
}

TEST(GroupedHashTest, LargeLogicalValueRoundTripsBeyondRecordLimit) {
  // This regression exercises the actual codec above both historical
  // caps. Release the source strings before decoding to bound peak payload
  // memory to roughly 2 GiB instead of retaining three complete copies.
  constexpr std::size_t kValueBytes = kMaxRecordPayloadBytes / 3 + 1;
  auto value = Value(3, kValueBytes);
  auto encoded = EncodeHashValue(value);
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  ASSERT_GT(encoded->size(), kMaxRecordPayloadBytes);
  value.entries_.clear();

  auto decoded = DecodeHashValue(*encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  ASSERT_EQ(decoded->entries_.size(), 3);
  for (std::size_t i = 0; i < decoded->entries_.size(); ++i) {
    const auto& entry = decoded->entries_[i];
    EXPECT_EQ(entry.field_, "field-" + std::to_string(i));
    EXPECT_EQ(entry.digest_, ComputeDigest(entry.field_));
    EXPECT_EQ(entry.value_.size(), kValueBytes);
    EXPECT_EQ(entry.value_.find_first_not_of(static_cast<char>('a' + i)),
              std::string::npos);
  }
}

TEST(GroupedHashTest, RejectsFieldsOutsideTheLeafBeingUpdated) {
  HashGroupSnapshot group{.incarnation_ = 17, .value_ = Value(1)};
  const auto hash =
      ComputeDigest(group.value_.entries_[0].field_, Seed()).value_;
  group.id_ = {.prefix_ = hash ^ 1, .bits_ = 64};
  EXPECT_FALSE(SplitHashGroup(group, Seed()).ok());
}

TEST(GroupedHashTest, DurableRoutingDoesNotDependOnProcessLookupSeed) {
  const auto original_seed = CurrentDigestSeed();
  auto groups = GroupHashValue(Value(100), 17, Seed());
  ASSERT_TRUE(groups.ok()) << groups.status();
  auto root = Root(100, groups->size());
  auto directory = Recover(root, 1, Candidates(*groups), {});
  ASSERT_TRUE(directory.ok()) << directory.status();
  std::vector<HashGroupId> before;
  for (const auto& entry : Value(100).entries_) {
    ASSERT_NE(directory->Find(entry.field_), nullptr);
    before.push_back(directory->Find(entry.field_)->id_);
  }
  RestoreDigestSeed(Seed(99));
  for (std::size_t i = 0; i < 100; ++i) {
    const auto* group = directory->Find("field-" + std::to_string(i));
    EXPECT_NE(group, nullptr);
    if (group != nullptr) EXPECT_EQ(group->id_, before[i]);
  }
  RestoreDigestSeed(original_seed);
}

TEST(GroupedHashTest, RecoverySelectsEachGroupSequenceIndependently) {
  std::vector<RecoveredHashGroup> records{
      {.incarnation_ = 17,
       .id_ = {0, 1},
       .sequence_ = 1,
       .lsn_ = 100,
       .field_count_ = 90},
      {.incarnation_ = 17,
       .id_ = {0, 1},
       .sequence_ = 2,
       .lsn_ = 2,
       .field_count_ = 3},
      {.incarnation_ = 17,
       .id_ = {1ULL << 63, 1},
       .sequence_ = 1,
       .lsn_ = 3,
       .field_count_ = 7},
  };
  auto directory = Recover(Root(10, 2), 2, records, {});
  ASSERT_TRUE(directory.ok()) << directory.status();
  EXPECT_EQ(directory->groups().at(0).sequence_, 2);
  EXPECT_EQ(directory->groups().at(1ULL << 63).sequence_, 1);
}

TEST(GroupedHashTest, RecoveryFiltersUncommittedFutureAndOtherIncarnations) {
  std::vector<RecoveredHashGroup> records{
      {.incarnation_ = 17, .sequence_ = 1, .lsn_ = 1, .field_count_ = 10},
      {.incarnation_ = 17,
       .sequence_ = 2,
       .lsn_ = 2,
       .txid_ = 99,
       .field_count_ = 30},
      {.incarnation_ = 17, .sequence_ = 3, .lsn_ = 3, .field_count_ = 40},
      {.incarnation_ = 18, .sequence_ = 2, .lsn_ = 4, .field_count_ = 50},
  };
  auto old = Recover(Root(10), 2, records, {});
  ASSERT_TRUE(old.ok()) << old.status();
  EXPECT_EQ(old->groups().at(0).sequence_, 1);
  auto committed = Recover(Root(30), 2, records, {99});
  ASSERT_TRUE(committed.ok()) << committed.status();
  EXPECT_EQ(committed->groups().at(0).sequence_, 2);
}

TEST(GroupedHashTest, SplitRecoveryIsOldOrNewAndNeverAPartialDirectory) {
  const RecoveredHashGroup old{
      .incarnation_ = 17, .sequence_ = 1, .lsn_ = 1, .field_count_ = 10};
  std::vector<RecoveredHashGroup> writes{
      {.incarnation_ = 17,
       .sequence_ = 2,
       .lsn_ = 2,
       .txid_ = 99,
       .retired_ = true},
      {.incarnation_ = 17,
       .id_ = {0, 1},
       .sequence_ = 2,
       .lsn_ = 3,
       .txid_ = 99,
       .field_count_ = 4},
      {.incarnation_ = 17,
       .id_ = {1ULL << 63, 1},
       .sequence_ = 2,
       .lsn_ = 4,
       .txid_ = 99,
       .field_count_ = 6},
  };
  // Every subset models data pages reaching disk before the commit decision.
  for (unsigned subset = 0; subset < 8; ++subset) {
    std::vector<RecoveredHashGroup> records{old};
    for (unsigned i = 0; i < 3; ++i) {
      if (subset & (1U << i)) records.push_back(writes[i]);
    }
    auto before = Recover(Root(10), 1, records, {});
    ASSERT_TRUE(before.ok()) << before.status();
    EXPECT_EQ(before->groups().size(), 1);
    auto after = Recover(Root(10, 2), 2, records, {99});
    EXPECT_EQ(after.ok(), subset == 7) << subset << ": " << after.status();
  }
}

TEST(GroupedHashTest, EmptyLeafRemainsRoutableAfterDeletingItsLastField) {
  std::vector<RecoveredHashGroup> records{
      {.incarnation_ = 17, .id_ = {0, 1}, .sequence_ = 2, .lsn_ = 2},
      {.incarnation_ = 17,
       .id_ = {1ULL << 63, 1},
       .sequence_ = 1,
       .lsn_ = 1,
       .field_count_ = 1},
  };
  auto directory = Recover(Root(1, 2), 2, records, {});
  ASSERT_TRUE(directory.ok()) << directory.status();
  EXPECT_EQ(directory->groups().at(0).field_count_, 0);
  for (const auto& entry : Value(100).entries_)
    EXPECT_NE(directory->Find(entry.field_), nullptr);
  records[0].retired_ = true;
  EXPECT_FALSE(Recover(Root(1, 2), 2, records, {}).ok());
}

TEST(GroupedHashTest, RelocationUsesLsnOnlyWithinTheSameLogicalVersion) {
  std::vector<RecoveredHashGroup> records{
      {.incarnation_ = 17, .sequence_ = 1, .lsn_ = 100, .field_count_ = 20},
      {.incarnation_ = 17,
       .sequence_ = 2,
       .lsn_ = 2,
       .txid_ = 7,
       .field_count_ = 10,
       .record_token_ = 1},
      {.incarnation_ = 17,
       .sequence_ = 2,
       .lsn_ = 3,
       .field_count_ = 10,
       .record_token_ = 2},
  };
  auto directory = Recover(Root(10), 2, records, {7});
  ASSERT_TRUE(directory.ok()) << directory.status();
  EXPECT_EQ(directory->groups().at(0).record_token_, 2);
  records.back().field_count_ = 9;
  EXPECT_FALSE(Recover(Root(10), 2, records, {7}).ok());
}

TEST(GroupedHashTest, RejectsCountsGapsOverlapsAndMalformedIdentities) {
  std::vector<RecoveredHashGroup> records{
      {.incarnation_ = 17, .sequence_ = 1, .lsn_ = 1, .field_count_ = 10},
  };
  EXPECT_FALSE(Recover(Root(11), 1, records, {}).ok());
  EXPECT_FALSE(Recover(Root(10, 2), 1, records, {}).ok());
  EXPECT_FALSE(Recover(Root(10), 0, records, {}).ok());
  records[0].id_ = {0, 1};
  EXPECT_FALSE(Recover(Root(10), 1, records, {}).ok());
  records[0].id_ = {1, 0};
  EXPECT_FALSE(Recover(Root(10), 1, records, {}).ok());
  records[0].id_ = {0, 0};
  records.push_back({.incarnation_ = 17,
                     .id_ = {1ULL << 63, 1},
                     .sequence_ = 1,
                     .field_count_ = 1});
  EXPECT_FALSE(Recover(Root(11, 2), 1, records, {}).ok());
}

TEST(GroupedHashTest, SingleFieldUpdateWritesOnlyItsGroup) {
  auto groups = GroupHashValue(Value(1000), 17, Seed());
  ASSERT_TRUE(groups.ok()) << groups.status();
  auto directory =
      Recover(Root(1000, groups->size()), 1, Candidates(*groups), {});
  ASSERT_TRUE(directory.ok()) << directory.status();
  const auto* selected = directory->Find("field-23");
  ASSERT_NE(selected, nullptr);
  const auto& group = groups->at(selected->record_token_ - 1);
  const std::vector<std::string_view> fields{"field-23"};
  const std::vector<std::string_view> values{"replacement"};
  auto plan = PlanHashGroupMutation(
      *directory, {{1, group}}, HashGroupMutationKind::kSet, fields, values);
  ASSERT_TRUE(plan.ok()) << plan.status();
  EXPECT_EQ(plan->expected_sequence_, 1);
  EXPECT_EQ(plan->affected_fields_, 0);
  EXPECT_EQ(plan->root_, directory->root());
  ASSERT_EQ(plan->writes_.size(), 1);
  EXPECT_EQ(plan->writes_.front().id_, selected->id_);
  EXPECT_TRUE(plan->changed_);
  EXPECT_FALSE(plan->delete_key_);
  EXPECT_EQ(directory->Find("field-23")->sequence_, 1);
  // Missing/stale snapshots are rejected before any publication.
  EXPECT_FALSE(PlanHashGroupMutation(
                   *directory, {}, HashGroupMutationKind::kSet, fields, values)
                   .ok());
  EXPECT_FALSE(PlanHashGroupMutation(*directory, {{2, group}},
                                     HashGroupMutationKind::kSet, fields,
                                     values)
                   .ok());
}

TEST(GroupedHashTest, DuplicateMutationOperandsPreserveRedisCounts) {
  auto groups = GroupHashValue(Value(1), 17, Seed());
  ASSERT_TRUE(groups.ok()) << groups.status();
  auto directory = Recover(Root(1), 1, Candidates(*groups), {});
  ASSERT_TRUE(directory.ok()) << directory.status();
  const std::vector<std::string_view> fields{"new", "new", "field-0"};
  const std::vector<std::string_view> values{"one", "two", "three"};
  auto plan =
      PlanHashGroupMutation(*directory, {{1, groups->front()}},
                            HashGroupMutationKind::kSet, fields, values);
  ASSERT_TRUE(plan.ok()) << plan.status();
  EXPECT_EQ(plan->affected_fields_, 1);
  EXPECT_EQ(plan->root_.field_count_, 2);
  ASSERT_EQ(plan->writes_.size(), 1);
  std::map<std::string, std::string> after;
  for (const auto& entry : plan->writes_[0].value_.entries_)
    after[entry.field_] = entry.value_;
  EXPECT_EQ(after.at("new"), "two");
  EXPECT_EQ(after.at("field-0"), "three");

  const std::vector<std::string_view> deletes{"field-0", "field-0", "missing"};
  auto removed = PlanHashGroupMutation(*directory, {{1, groups->front()}},
                                       HashGroupMutationKind::kDelete, deletes);
  ASSERT_TRUE(removed.ok()) << removed.status();
  EXPECT_EQ(removed->affected_fields_, 1);
  EXPECT_TRUE(removed->delete_key_);
  EXPECT_TRUE(removed->writes_.empty());
}

TEST(GroupedHashTest, NoopAndSetIfAbsentDoNotRewriteGroups) {
  auto value = Value(1);
  auto groups = GroupHashValue(value, 17, Seed());
  ASSERT_TRUE(groups.ok()) << groups.status();
  auto directory = Recover(Root(1), 1, Candidates(*groups), {});
  ASSERT_TRUE(directory.ok()) << directory.status();
  const std::vector<std::string_view> fields{"field-0"};
  const std::vector<std::string_view> same{value.entries_[0].value_};
  auto plan = PlanHashGroupMutation(*directory, {{1, groups->front()}},
                                    HashGroupMutationKind::kSet, fields, same);
  ASSERT_TRUE(plan.ok()) << plan.status();
  EXPECT_FALSE(plan->changed_);
  EXPECT_TRUE(plan->writes_.empty());
  const std::vector<std::string_view> other{"not applied"};
  plan =
      PlanHashGroupMutation(*directory, {{1, groups->front()}},
                            HashGroupMutationKind::kSetIfAbsent, fields, other);
  ASSERT_TRUE(plan.ok()) << plan.status();
  EXPECT_FALSE(plan->changed_);
  EXPECT_EQ(plan->affected_fields_, 0);
}

TEST(GroupedHashTest, MutationSplitIncludesOldLeafRetirement) {
  auto groups = GroupHashValue(Value(4, 1), 17, Seed());
  ASSERT_TRUE(groups.ok()) << groups.status();
  auto directory = Recover(Root(4), 1, Candidates(*groups), {});
  ASSERT_TRUE(directory.ok()) << directory.status();
  const std::vector<std::string_view> fields{"field-0"};
  const std::string large(1024, 'x');
  const std::vector<std::string_view> values{large};
  auto plan =
      PlanHashGroupMutation(*directory, {{1, groups->front()}},
                            HashGroupMutationKind::kSet, fields, values, 256);
  ASSERT_TRUE(plan.ok()) << plan.status();
  ASSERT_GT(plan->writes_.size(), 2);
  EXPECT_TRUE(plan->writes_.front().retired_);
  EXPECT_EQ(plan->writes_.front().id_, HashGroupId{});
  auto candidates = Candidates(*groups);
  auto updates = Candidates(plan->writes_, 2, 99);
  candidates.insert(candidates.end(), updates.begin(), updates.end());
  auto next = Recover(plan->root_, 2, candidates, {99});
  ASSERT_TRUE(next.ok()) << next.status();
  EXPECT_EQ(next->root().field_count_, 4);
  EXPECT_EQ(next->groups().size() + 1, plan->writes_.size());
}

TEST(GroupedHashTest, RandomizedPartialWritesRecoverAgainstReferenceHash) {
  for (unsigned seed_id = 0; seed_id < 4; ++seed_id) {
    auto seed = Seed(seed_id);
    auto original = Value(50, 12);
    auto grouped = GroupHashValue(original, 17, seed, 256);
    ASSERT_TRUE(grouped.ok()) << grouped.status();
    std::vector<HashGroupSnapshot> media = *grouped;
    auto candidates = Candidates(media);
    auto root = Root(50, grouped->size());
    root.seed_ = seed;
    std::uint64_t root_sequence = 1;
    absl::flat_hash_set<std::uint64_t> commits;
    std::map<std::string, std::string> expected;
    for (const auto& entry : original.entries_)
      expected[entry.field_] = entry.value_;
    std::mt19937 random(seed_id);
    for (unsigned step = 0; step < 150; ++step) {
      SCOPED_TRACE("seed=" + std::to_string(seed_id) +
                   " step=" + std::to_string(step));
      auto directory = Recover(root, root_sequence, candidates, commits);
      ASSERT_TRUE(directory.ok()) << directory.status();
      const bool deleting = random() % 4 == 0;
      std::vector<std::string> owned_fields;
      std::vector<std::string> owned_values;
      const unsigned operands = 1 + random() % 8;
      for (unsigned i = 0; i < operands; ++i) {
        owned_fields.push_back("field-" + std::to_string(random() % 100));
        owned_values.push_back(
            std::string(1 + random() % 200, 'a' + random() % 26));
      }
      std::vector<std::string_view> fields(owned_fields.begin(),
                                           owned_fields.end());
      std::vector<std::string_view> values(owned_values.begin(),
                                           owned_values.end());
      if (deleting) values.clear();
      std::map<HashGroupId, LoadedHashGroup> affected;
      for (auto field : fields) {
        const auto* group = directory->Find(field);
        ASSERT_NE(group, nullptr);
        affected.try_emplace(
            group->id_, LoadedHashGroup{group->sequence_,
                                        media.at(group->record_token_ - 1)});
      }
      std::vector<LoadedHashGroup> loaded;
      for (auto& [id, group] : affected) loaded.push_back(std::move(group));
      auto plan =
          PlanHashGroupMutation(*directory, std::move(loaded),
                                deleting ? HashGroupMutationKind::kDelete
                                         : HashGroupMutationKind::kSet,
                                fields, values, 256);
      ASSERT_TRUE(plan.ok()) << plan.status();
      ASSERT_FALSE(plan->delete_key_);
      const bool commit = random() % 4 != 0;
      const std::uint64_t sequence = step + 2;
      const std::uint64_t txid = step + 100;
      for (const auto& write : plan->writes_) {
        if (!commit && random() % 2 == 0) continue;
        auto encoded = EncodeHashGroup(write);
        ASSERT_TRUE(encoded.ok()) << encoded.status();
        auto decoded = DecodeHashGroup(*encoded);
        ASSERT_TRUE(decoded.ok()) << decoded.status();
        media.push_back(std::move(*decoded));
        candidates.push_back({.incarnation_ = 17,
                              .id_ = write.id_,
                              .sequence_ = sequence,
                              .lsn_ = media.size(),
                              .txid_ = txid,
                              .field_count_ = write.value_.entries_.size(),
                              .record_token_ = media.size(),
                              .retired_ = write.retired_});
      }
      if (commit && plan->changed_) {
        commits.insert(txid);
        root = plan->root_;
        root_sequence = sequence;
        for (std::size_t i = 0; i < fields.size(); ++i) {
          if (deleting)
            expected.erase(owned_fields[i]);
          else
            expected[owned_fields[i]] = owned_values[i];
        }
      }
      // Physical scan ordering is not logical commit ordering.
      std::shuffle(candidates.begin(), candidates.end(), random);
      auto recovered = Recover(root, root_sequence, candidates, commits);
      ASSERT_TRUE(recovered.ok()) << recovered.status();
      std::map<std::string, std::string> actual;
      for (const auto& [prefix, group] : recovered->groups()) {
        for (const auto& entry :
             media.at(group.record_token_ - 1).value_.entries_) {
          EXPECT_TRUE(actual.emplace(entry.field_, entry.value_).second);
          ASSERT_NE(recovered->Find(entry.field_), nullptr);
          EXPECT_EQ(recovered->Find(entry.field_)->id_, group.id_);
        }
      }
      EXPECT_EQ(actual, expected);
    }
  }
}

}  // namespace
}  // namespace lavik::storage
