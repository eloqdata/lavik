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

#include "lavik/storage/detail/stream_records.h"

#include <algorithm>
#include <map>

#include "gtest/gtest.h"
#include "lavik/storage/detail/collection_compact_stream.h"

namespace lavik::storage {
namespace {
void Put(std::string& s, std::uint64_t v, unsigned width) {
  for (unsigned i = 0; i < width; ++i) s.push_back(v >> (8 * i));
}
void String(std::string& s, std::string_view v) {
  Put(s, v.size(), 4);
  s.append(v);
}
std::string Stream(unsigned entries, bool group = true) {
  std::string s("LXS1");
  Put(s, entries, 8);
  Put(s, 0, 8);
  s.append(16, '\0');
  Put(s, entries, 8);
  Put(s, entries, 4);
  for (unsigned i = 1; i <= entries; ++i) {
    Put(s, i, 8);
    Put(s, 0, 8);
    Put(s, 2, 4);
    String(s, "f");
    String(s, std::string(120, 'x'));
  }
  Put(s, entries ? 1 : 0, 4);
  if (entries) Put(s, entries, 4);
  Put(s, group ? 1 : 0, 4);
  if (group) {
    String(s, std::string_view("g\0z", 3));
    s.append(16, '\0');
    Put(s, 0, 8);
    Put(s, 1, 4);
    String(s, "consumer");
    Put(s, 42, 8);
    Put(s, 41, 8);
    Put(s, 1, 4);
    Put(s, 999, 8);
    Put(s, 0, 8);
    String(s, "consumer");
    Put(s, 40, 8);
    Put(s, 3, 8);
  }
  return s;
}
TEST(StreamRecords, RoundTripAndIncrementalValidation) {
  for (unsigned n : {0U, 1U, 100U}) {
    auto wire = Stream(n);
    auto records = DecodeStreamRecords(wire, n);
    ASSERT_TRUE(records.ok()) << records.status();
    StreamRecordValidator validator(n);
    for (const auto& record : *records)
      ASSERT_TRUE(validator.Read(record.value_).ok());
    EXPECT_TRUE(validator.Finish().ok());
    EXPECT_EQ(*EncodeStreamRecords(*records), wire);
    std::size_t wire_bytes = 8;
    for (const auto& record : *records) wire_bytes += 4 + record.value_.size();
    auto encoder = CollectionCompactEncoder::Create(
        ValueType::kStream, records->size(), wire_bytes);
    ASSERT_TRUE(encoder.ok()) << encoder.status();
    std::string output;
    for (const auto& record : *records) {
      CollectionPage page{.value_type_ = ValueType::kStream,
                          .elements_ = {record.value_}};
      ASSERT_TRUE(encoder->StartPage(page).ok());
      while (auto bytes = encoder->Next()) output.append(*bytes);
    }
    EXPECT_TRUE(encoder->Finish().ok());
    auto decoder =
        CollectionCompactDecoder::Create(ValueType::kStream, n, output.size());
    ASSERT_TRUE(decoder.ok()) << decoder.status();
    std::vector<OrderedCollectionEntry> decoded;
    // Exercise every framing boundary, including binary routing names.
    for (std::size_t i = 0; i < output.size();) {
      auto used = decoder->Consume(std::string_view(output).substr(i, 1));
      ASSERT_TRUE(used.ok()) << used.status();
      i += *used;
      if (decoder->page_ready()) {
        auto page = decoder->TakePage();
        ASSERT_TRUE(page.ok()) << page.status();
        for (auto& record : page->elements_)
          decoded.push_back({.value_ = std::move(record)});
      }
    }
    EXPECT_TRUE(decoder->Finish().ok());
    EXPECT_EQ(*EncodeStreamRecords(decoded), wire);
  }
}
TEST(StreamRecords, RejectsTruncationAndWrongCounts) {
  auto wire = Stream(3);
  for (std::size_t i = 0; i < wire.size(); ++i)
    EXPECT_FALSE(
        DecodeStreamRecords(std::string_view(wire).substr(0, i), 3).ok())
        << i;
  EXPECT_FALSE(DecodeStreamRecords(wire, 4).ok());
  auto records = DecodeStreamRecords(wire, 3);
  ASSERT_TRUE(records.ok());
  StreamRecordValidator incomplete(3);
  for (std::size_t i = 0; i + 1 < records->size(); ++i)
    ASSERT_TRUE(incomplete.Read((*records)[i].value_).ok());
  EXPECT_FALSE(incomplete.Finish().ok());
  StreamRecordValidator repeated(3);
  ASSERT_TRUE(repeated.Read(records->front().value_).ok());
  EXPECT_FALSE(repeated.Read(records->front().value_).ok());
  EXPECT_FALSE(repeated.Read((*records)[1].value_).ok());
}
TEST(StreamRecords, EmptyStreamRootHasIndependentRecordCardinality) {
  auto records = DecodeStreamRecords(Stream(0, false), 0);
  ASSERT_TRUE(records.ok());
  OrderedCollectionRoot root{.kind_ = OrderedCollectionKind::kStream,
                             .incarnation_ = 1,
                             .item_count_ = records->size(),
                             .first_group_ = 1,
                             .last_group_ = 1,
                             .next_group_id_ = 2,
                             .group_count_ = 1,
                             .revision_ = 1,
                             .stream_length_ = 0};
  auto wire = EncodeOrderedCollectionRoot(root);
  ASSERT_TRUE(wire.ok()) << wire.status();
  EXPECT_EQ(wire->size(), kGroupedStreamRootBytes);
  EXPECT_EQ(*DecodeOrderedCollectionRoot(*wire), root);
  EXPECT_FALSE(DecodeOrderedCollectionRoot(std::string_view(*wire).substr(
                                               0, kOrderedCollectionRootBytes))
                   .ok());
  root.stream_length_.reset();
  EXPECT_FALSE(EncodeOrderedCollectionRoot(root).ok());
}
TEST(StreamRecords, PagePayloadAndKeysMustAgree) {
  auto records = DecodeStreamRecords(Stream(2), 2);
  ASSERT_TRUE(records.ok());
  auto damaged = *records;
  damaged[1].value_[16] = 7;
  StreamRecordValidator validator(2);
  ASSERT_TRUE(validator.Read(damaged[0].value_).ok());
  EXPECT_FALSE(validator.Read(damaged[1].value_).ok());
  EXPECT_FALSE(StreamRecordKey(std::string("\1junk", 5)).ok());
}
TEST(StreamRecords, AdjacentPagesCannotRepeatARecordKey) {
  auto records = DecodeStreamRecords(Stream(2), 2);
  ASSERT_TRUE(records.ok());
  OrderedGroupSnapshot left{.kind_ = OrderedCollectionKind::kStream,
                            .incarnation_ = 1,
                            .id_ = 1,
                            .next_ = 2,
                            .entries_ = {(*records)[1]}};
  OrderedGroupSnapshot right{.kind_ = OrderedCollectionKind::kStream,
                             .incarnation_ = 1,
                             .id_ = 2,
                             .previous_ = 1,
                             .entries_ = {(*records)[2]}};
  EXPECT_TRUE(ValidateOrderedGroupBoundary(left, right).ok());
  right.entries_ = left.entries_;
  // Different field bytes do not give a duplicate message ID a new identity.
  const auto payload = StreamRecordPayload(right.entries_.front().value_);
  ASSERT_TRUE(payload.ok());
  right.entries_.front().value_[17 + payload->size() - 1] = 'y';
  EXPECT_FALSE(ValidateOrderedGroupBoundary(left, right).ok());
}
TEST(StreamRecords, BulkInsertionSplitsAndPreservesLogicalImage) {
  auto before = DecodeStreamRecords(Stream(0, false), 0);
  const auto wire = Stream(20000, false);
  auto after = DecodeStreamRecords(wire, 20000);
  ASSERT_TRUE(before.ok());
  ASSERT_TRUE(after.ok());
  OrderedCollectionRoot root{.kind_ = OrderedCollectionKind::kStream,
                             .incarnation_ = 1,
                             .item_count_ = before->size(),
                             .first_group_ = 1,
                             .last_group_ = 1,
                             .next_group_id_ = 2,
                             .group_count_ = 1,
                             .revision_ = 1,
                             .stream_length_ = 0};
  const std::vector<RecoveredOrderedGroup> metadata{
      {.incarnation_ = 1,
       .id_ = 1,
       .sequence_ = 1,
       .lsn_ = 1,
       .item_count_ = before->size(),
       .record_token_ = 1}};
  auto directory = OrderedGroupDirectory::Recover(root, 1, metadata, {});
  ASSERT_TRUE(directory.ok()) << directory.status();
  std::vector<LoadedOrderedGroup> loaded{
      {.sequence_ = 1,
       .snapshot_ = {.kind_ = OrderedCollectionKind::kStream,
                     .incarnation_ = 1,
                     .id_ = 1,
                     .entries_ = *before}}};
  std::vector<StreamRecordChange> changes;
  for (const auto& entry : *after)
    changes.push_back({.page_id_ = 1,
                       .key_ = std::string(*StreamRecordKey(entry.value_)),
                       .record_ = entry.value_});
  // Caller order is not a routing or sorting guarantee.
  std::reverse(changes.begin(), changes.end());
  auto plan = PlanStreamRecordChanges(*directory, std::move(loaded),
                                      std::move(changes), 20000);
  ASSERT_TRUE(plan.ok()) << plan.status();
  EXPECT_EQ(plan->root_.logical_size(), 20000);
  EXPECT_GT(plan->writes_.size(), 100);
  std::map<std::uint64_t, const OrderedGroupSnapshot*> pages;
  for (const auto& page : plan->writes_) pages.emplace(page.id_, &page);
  std::vector<OrderedCollectionEntry> result;
  const OrderedGroupSnapshot* previous = nullptr;
  for (auto id = plan->root_.first_group_; id != 0;) {
    ASSERT_TRUE(pages.contains(id));
    const auto* page = pages.at(id);
    if (previous)
      EXPECT_TRUE(ValidateOrderedGroupBoundary(*previous, *page).ok());
    result.insert(result.end(), page->entries_.begin(), page->entries_.end());
    previous = page;
    id = page->next_;
  }
  EXPECT_EQ(result, *after);
  EXPECT_EQ(*EncodeStreamRecords(result), wire);
}
}  // namespace
}  // namespace lavik::storage
