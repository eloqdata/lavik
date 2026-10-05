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

#include "lavik/storage/detail/record_payload_cursor.h"

#include <array>
#include <vector>

#include "gtest/gtest.h"
#include "lavik/storage/detail/grouped/collection.h"
#include "lavik/storage/detail/grouped/hash.h"

namespace lavik::storage {
namespace {

struct Encoder {
  std::size_t bytes_;
  std::vector<std::string_view> spans_;
  std::size_t next_ = 0;
  std::size_t encoded_bytes() const { return bytes_; }
  std::optional<std::string_view> Next() noexcept {
    return next_ == spans_.size() ? std::nullopt
                                  : std::optional(spans_[next_++]);
  }
};

TEST(RecordPayloadCursorTest, InlineEncodingChecksExactProducerLength) {
  for (const std::size_t bytes : {0, 1, 15, 16, 31, 256}) {
    const std::string input(bytes, '\xff');
    Encoder encoder{bytes, {"", input, ""}};
    auto encoded = EncodeInlineRecordPayload(encoder);
    ASSERT_TRUE(encoded.ok()) << encoded.status();
    EXPECT_EQ(*encoded, input);
  }
  for (auto spans : {std::vector<std::string_view>{"ab"},
                     std::vector<std::string_view>{"abcd"},
                     std::vector<std::string_view>{"abc", "", "d"}}) {
    Encoder encoder{3, std::move(spans)};
    EXPECT_TRUE(absl::IsDataLoss(EncodeInlineRecordPayload(encoder).status()));
  }
  Encoder nonempty{0, {"x"}};
  EXPECT_TRUE(absl::IsDataLoss(EncodeInlineRecordPayload(nonempty).status()));
}

TEST(RecordPayloadCursorTest, InlineEncodingPreservesBorrowedPageFraming) {
  OrderedGroupSnapshot list{.incarnation_ = 17, .id_ = 1};
  HashGroupSnapshot hash{.incarnation_ = 17};
  for (int i = 0; i < 100; ++i) {
    std::string value(i * 3, static_cast<char>(i));
    list.entries_.push_back({.value_ = value});
    hash.value_.entries_.push_back(
        {.field_ = std::to_string(i), .value_ = value});
  }
  auto ordered = OrderedGroupEncoder::Create(list);
  ASSERT_TRUE(ordered.ok());
  auto ordered_bytes = EncodeInlineRecordPayload(*ordered);
  ASSERT_TRUE(ordered_bytes.ok());
  EXPECT_EQ(*ordered_bytes, *EncodeOrderedGroup(list));
  auto prefix = HashGroupEncoder::Create(hash);
  ASSERT_TRUE(prefix.ok());
  auto hash_bytes = EncodeInlineRecordPayload(*prefix);
  ASSERT_TRUE(hash_bytes.ok());
  EXPECT_EQ(*hash_bytes, *EncodeHashGroup(hash));
}

TEST(RecordPayloadCursorTest, StreamsEmptySpansAndExternalKeyPrefix) {
  Encoder encoder{5, {"", "a", "", "bc", "de", ""}};
  RecordPayloadCursor cursor(encoder, "key");
  EXPECT_EQ(cursor.encoded_bytes(), 8);
  std::string bytes(8, '\0');
  for (std::size_t offset = 0; offset < bytes.size(); offset += 2) {
    EXPECT_TRUE(
        cursor.Read(std::as_writable_bytes(std::span(bytes).subspan(offset, 2)))
            .ok());
  }
  EXPECT_EQ(bytes, "keyabcde");
  EXPECT_TRUE(cursor.Finish().ok());
}

TEST(RecordPayloadCursorTest, RejectsTruncationAndStickyFailures) {
  Encoder encoder{5, {"abc"}};
  RecordPayloadCursor cursor(encoder);
  std::array<std::byte, 5> output;
  EXPECT_FALSE(cursor.Read(output).ok());
  EXPECT_FALSE(cursor.Read({}).ok());
  EXPECT_FALSE(cursor.Finish().ok());
}

TEST(RecordPayloadCursorTest, RejectsTrailingAndOversizedProducerSpans) {
  for (auto spans : {std::vector<std::string_view>{"abc", "d"},
                     std::vector<std::string_view>{"abcd"}}) {
    Encoder encoder{3, std::move(spans)};
    RecordPayloadCursor cursor(encoder);
    std::array<std::byte, 3> output;
    const auto read = cursor.Read(output);
    EXPECT_TRUE(!read.ok() || !cursor.Finish().ok());
  }
}

TEST(RecordPayloadCursorTest, RefusesUnconsumedAndOverlongOutput) {
  Encoder encoder{3, {"abc"}};
  RecordPayloadCursor cursor(encoder);
  EXPECT_FALSE(cursor.Finish().ok());
  Encoder second{3, {"abc"}};
  RecordPayloadCursor other(second);
  std::array<std::byte, 4> output;
  EXPECT_FALSE(other.Read(output).ok());
  EXPECT_EQ(second.next_, 0);
}

TEST(RecordPayloadCursorTest, ConsumesBorrowedHashFramingAcrossExtentBoundary) {
  HashGroupSnapshot snapshot{
      .incarnation_ = 17,
      .value_ = {.entries_ = {{.field_ = "field",
                               .value_ = std::string(9 * 1024 * 1024, 'v')},
                              {.field_ = "", .value_ = ""}}},
  };
  auto expected = EncodeHashGroup(snapshot);
  ASSERT_TRUE(expected.ok()) << expected.status();
  auto encoder = HashGroupEncoder::Create(snapshot);
  ASSERT_TRUE(encoder.ok()) << encoder.status();
  RecordPayloadCursor cursor(*encoder);
  std::array<std::byte, 4093> buffer;
  std::size_t offset = 0;
  while (offset < expected->size()) {
    const auto size = std::min(buffer.size(), expected->size() - offset);
    ASSERT_TRUE(cursor.Read(std::span(buffer).first(size)).ok());
    EXPECT_EQ(std::memcmp(buffer.data(), expected->data() + offset, size), 0);
    offset += size;
  }
  EXPECT_TRUE(cursor.Finish().ok());
  auto metadata = DecodeHashGroupMetadata(
      std::string_view(*expected).substr(0, kHashGroupHeaderBytes),
      expected->size());
  ASSERT_TRUE(metadata.ok()) << metadata.status();
  EXPECT_EQ(metadata->field_count_, 2);
  EXPECT_FALSE(DecodeHashGroupMetadata(*expected, expected->size() - 1).ok());
  EXPECT_FALSE(
      DecodeHashGroupMetadata(expected->substr(0, 47), expected->size()).ok());
}

}  // namespace
}  // namespace lavik::storage
