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

#include <unistd.h>

#include <array>
#include <cstdio>
#include <limits>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

#include "gtest/gtest.h"
#include "lavik/rdb.h"
#include "support/test_data_path.h"

namespace lavik::rdb {
namespace {

using storage::CollectionPage;
using storage::ValueType;

std::string Drain(CollectionFileEncoder& encoder) {
  std::string output;
  while (auto fragment = encoder.Next()) output.append(*fragment);
  return output;
}

TEST(RdbCollectionTest, FourTypesRoundTripAcrossPagesAndEmptyRoutes) {
  for (auto type : {ValueType::kHash, ValueType::kSet, ValueType::kList,
                    ValueType::kSortedSet}) {
    SCOPED_TRACE(static_cast<unsigned>(type));
    auto encoder = CollectionFileEncoder::Create(3, "key", type, 2, 987654321);
    ASSERT_TRUE(encoder.ok()) << encoder.status();
    std::string fragment = Drain(*encoder);
    CollectionPage empty{.value_type_ = type, .next_cursor_ = 1};
    ASSERT_TRUE(encoder->StartPage(empty).ok());
    EXPECT_TRUE(Drain(*encoder).empty());
    for (unsigned n = 0; n < 2; ++n) {
      CollectionPage page{
          .value_type_ = type, .next_cursor_ = n + 2, .done_ = n == 1};
      if (type == ValueType::kHash)
        page.fields_.push_back({n == 0 ? "" : "field", n == 0 ? "" : "value"});
      else if (type == ValueType::kSortedSet)
        page.scored_members_.push_back({n == 0 ? "" : "member", double(n)});
      else
        page.elements_.push_back(n == 0 ? "" : "member");
      ASSERT_TRUE(encoder->StartPage(page).ok());
      fragment += Drain(*encoder);
    }
    ASSERT_TRUE(encoder->Finish().ok());
    std::string path = lavik::test::TestDataPath("lavik-rdb-collection-XXXXXX");
    const int fd = ::mkstemp(path.data());
    ASSERT_GE(fd, 0);
    ::close(fd);
    struct RemoveFile {
      std::string path;
      ~RemoveFile() { std::remove(path.c_str()); }
    } cleanup{path};
    auto writer = FileWriter::Open(path.c_str());
    ASSERT_TRUE(writer.ok()) << writer.status();
    // Every possible split must remain legal at the file sink, including
    // inside length prefixes, scores and empty-string boundaries.
    for (const char byte : fragment)
      ASSERT_TRUE(writer->WriteFragment(std::string_view(&byte, 1)).ok());
    ASSERT_TRUE(writer->Finish().ok());
    auto reader = FileReader::Open(path.c_str());
    ASSERT_TRUE(reader.ok()) << reader.status();
    auto result = reader->Next();
    ASSERT_TRUE(result.ok()) << result.status();
    ASSERT_TRUE(result->has_value());
    EXPECT_EQ((**result).db_id_, 3);
    EXPECT_EQ((**result).key_, "key");
    EXPECT_EQ((**result).value_.value_type_, type);
    EXPECT_EQ((**result).value_.logical_size_, 2);
    EXPECT_EQ((**result).value_.expire_at_ms_, 987654321);
    auto end = reader->Next();
    ASSERT_TRUE(end.ok());
    EXPECT_FALSE(end->has_value());
  }
}

TEST(RdbCollectionTest, LargeMemberIsBorrowedNotSerializedAgain) {
  auto encoder =
      CollectionFileEncoder::Create(0, "key", ValueType::kHash, 1, 0);
  ASSERT_TRUE(encoder.ok());
  (void)Drain(*encoder);
  CollectionPage page{
      .value_type_ = ValueType::kHash, .next_cursor_ = 1, .done_ = true};
  page.fields_.push_back({"field", std::string(9 * 1024 * 1024, 'x')});
  ASSERT_TRUE(encoder->StartPage(page).ok());
  bool borrowed = false;
  std::size_t total = 0;
  while (auto span = encoder->Next()) {
    if (span->size() == page.fields_[0].value_.size()) {
      borrowed = true;
      EXPECT_EQ(span->data(), page.fields_[0].value_.data());
    }
    // Backpressure consumers can make bounded fragments without asking the
    // encoder to retain any output queue or a second complete page image.
    while (!span->empty()) {
      const auto fragment = span->substr(0, 1024 * 1024);
      EXPECT_LE(fragment.size(), 1024 * 1024);
      total += fragment.size();
      span->remove_prefix(fragment.size());
    }
  }
  EXPECT_TRUE(borrowed);
  EXPECT_GT(total, page.fields_[0].value_.size());
  EXPECT_TRUE(encoder->Finish().ok());
  EXPECT_LE(sizeof(CollectionFileEncoder), 256);
}

TEST(RdbCollectionTest, RejectsMalformedPagesBeforeEmission) {
  auto encoder =
      CollectionFileEncoder::Create(0, "key", ValueType::kHash, 1, 0);
  ASSERT_TRUE(encoder.ok());
  CollectionPage page{
      .value_type_ = ValueType::kHash, .next_cursor_ = 1, .done_ = true};
  page.fields_.push_back({"field", "value"});
  EXPECT_FALSE(encoder->StartPage(page).ok());  // Header not drained.
  (void)Drain(*encoder);
  page.next_cursor_ = 2;
  EXPECT_FALSE(encoder->StartPage(page).ok());
  page.next_cursor_ = 1;
  page.elements_.push_back("wrong container");
  EXPECT_FALSE(encoder->StartPage(page).ok());
  page.elements_.clear();
  page.fields_.push_back({"another", "value"});
  EXPECT_FALSE(encoder->StartPage(page).ok());
  page.fields_.pop_back();
  EXPECT_TRUE(encoder->StartPage(page).ok());
  EXPECT_FALSE(encoder->Finish().ok());  // Borrowed bytes remain unread.
  (void)Drain(*encoder);
  EXPECT_TRUE(encoder->Finish().ok());
  EXPECT_FALSE(encoder->StartPage(page).ok());
}

TEST(RdbCollectionTest, RequiresExplicitEofAndAcceptsInfinityButNotNan) {
  auto encoder =
      CollectionFileEncoder::Create(0, "key", ValueType::kSortedSet, 1, 0);
  ASSERT_TRUE(encoder.ok());
  (void)Drain(*encoder);
  CollectionPage page{.value_type_ = ValueType::kSortedSet, .next_cursor_ = 1};
  page.scored_members_.push_back(
      {"member", std::numeric_limits<double>::quiet_NaN()});
  EXPECT_FALSE(encoder->StartPage(page).ok());
  page.scored_members_[0].score_ = std::numeric_limits<double>::infinity();
  EXPECT_TRUE(encoder->StartPage(page).ok());
  (void)Drain(*encoder);
  EXPECT_FALSE(encoder->Finish().ok());
  CollectionPage last{
      .value_type_ = ValueType::kSortedSet, .next_cursor_ = 2, .done_ = true};
  EXPECT_TRUE(encoder->StartPage(last).ok());
  (void)Drain(*encoder);
  EXPECT_TRUE(encoder->Finish().ok());
}

TEST(RdbCollectionTest, AggregateCountUsesRdb64BitLengthWithoutAllocation) {
  auto encoder = CollectionFileEncoder::Create(
      0, "", ValueType::kList, std::uint64_t{UINT32_MAX} + 1, 0);
  ASSERT_TRUE(encoder.ok());
  const auto header = Drain(*encoder);
  ASSERT_GE(header.size(), 9);
  const auto count = header.substr(header.size() - 9);
  EXPECT_EQ(static_cast<unsigned char>(count[0]), 0x81);
  EXPECT_EQ(static_cast<unsigned char>(count[4]), 1);
  EXPECT_FALSE(encoder->Finish().ok());
  EXPECT_FALSE(
      CollectionFileEncoder::Create(16, "k", ValueType::kHash, 1, 0).ok());
  EXPECT_FALSE(
      CollectionFileEncoder::Create(0, "k", ValueType::kString, 1, 0).ok());
  EXPECT_FALSE(
      CollectionFileEncoder::Create(0, "k", ValueType::kList, 0, 0).ok());
}

TEST(RdbCollectionTest, PageRetentionMovesWithOutputLifetime) {
  static_assert(!std::is_copy_constructible_v<CollectionPage>);
  static_assert(std::is_move_constructible_v<CollectionPage>);
  const auto old_shard = CurrentMemoryAccountingShard();
  struct MemoryScope {
    unsigned old_shard;
    ~MemoryScope() {
      EXPECT_TRUE(InitMemoryLimit(1024ULL * 1024 * 1024, 1).ok());
      BindMemoryAccountingShard(old_shard == 0 ? kMaxMemoryWorkers
                                               : old_shard - 1);
    }
  } restore{old_shard};
  ASSERT_TRUE(InitMemoryLimit(64 * 1024 * 1024, 1).ok());
  BindMemoryAccountingShard(0);
  const auto before = WorkerMemoryAccountingBytes(0);
  {
    CollectionPage page;
    page.value_type_ = ValueType::kHash;
    page.fields_.push_back({"field", std::string(1024 * 1024, 'x')});
    const auto bytes = page.RetainedBytes();
    EXPECT_GE(bytes, 1024 * 1024);
    auto reservation = TryReserveMemory(bytes);
    ASSERT_TRUE(reservation.has_value());
    page.retained_charge_.Adopt(&*reservation, bytes);
    EXPECT_EQ(WorkerMemoryAccountingBytes(0), before + bytes);
    {
      auto output_page = std::move(page);
      EXPECT_EQ(page.retained_charge_.bytes(), 0);
      EXPECT_EQ(output_page.retained_charge_.bytes(), bytes);
      EXPECT_EQ(WorkerMemoryAccountingBytes(0), before + bytes);
      // Output backpressure owns the page after its storage token could have
      // been cancelled. Its payload, rather than that token, owns this charge.
      ASSERT_TRUE(InitMemoryLimit(1, 1).ok());
      EXPECT_FALSE(TryReserveMemory(1).has_value());
      EXPECT_EQ(output_page.fields_[0].value_.size(), 1024 * 1024);
    }
    EXPECT_EQ(WorkerMemoryAccountingBytes(0), before);
  }
  EXPECT_EQ(WorkerMemoryAccountingBytes(0), before);
}

class CollectionImportFile {
 public:
  CollectionImportFile() {
    const int fd = ::mkstemp(path_.data());
    if (fd < 0) throw std::runtime_error("RDB import test tempfile failed");
    ::close(fd);
  }
  ~CollectionImportFile() { std::remove(path_.c_str()); }
  const char* path() const { return path_.c_str(); }

 private:
  std::string path_ = lavik::test::TestDataPath("lavik-rdb-import-XXXXXX");
};

void WriteImportFixture(const char* path, ValueType type, bool duplicate) {
  constexpr unsigned count = 700;
  auto encoder =
      CollectionFileEncoder::Create(3, "key", type, count, 123456789);
  ASSERT_TRUE(encoder.ok());
  auto writer = FileWriter::Open(path);
  ASSERT_TRUE(writer.ok());
  while (auto fragment = encoder->Next())
    ASSERT_TRUE(writer->WriteFragment(*fragment).ok());
  CollectionPage page{.value_type_ = type, .next_cursor_ = 1, .done_ = true};
  for (unsigned i = 0; i < count; ++i) {
    auto member = std::to_string(duplicate && i + 1 == count ? 0 : i);
    member.resize(2048, 'x');
    if (type == ValueType::kHash)
      page.fields_.push_back({std::move(member), "value"});
    else if (type == ValueType::kSortedSet)
      page.scored_members_.push_back({std::move(member), double(count - i)});
    else
      page.elements_.push_back(std::move(member));
  }
  ASSERT_TRUE(encoder->StartPage(page).ok());
  while (auto fragment = encoder->Next())
    ASSERT_TRUE(writer->WriteFragment(*fragment).ok());
  ASSERT_TRUE(encoder->Finish().ok());
  ASSERT_TRUE(writer->Finish().ok());
}

TEST(RdbCollectionImportTest, FourTypesConsumeAdmittedPagesAndRewind) {
  for (auto type : {ValueType::kHash, ValueType::kSet, ValueType::kList,
                    ValueType::kSortedSet}) {
    SCOPED_TRACE(static_cast<unsigned>(type));
    CollectionImportFile file;
    WriteImportFixture(file.path(), type, false);
    auto reader = FileReader::Open(file.path());
    ASSERT_TRUE(reader.ok());
    auto header = reader->NextStreaming();
    ASSERT_TRUE(header.ok());
    ASSERT_TRUE(header->has_value());
    EXPECT_TRUE((**header).collection_stream_);
    EXPECT_EQ((**header).expected_items_, 700);
    EXPECT_EQ((**header).value_.expire_at_ms_, 123456789);
    EXPECT_TRUE((**header).value_.encoded_.empty());
    EXPECT_FALSE(reader->NextStreaming().ok());
    std::size_t count = 0;
    unsigned pages = 0;
    for (;;) {
      auto page = reader->ReadCollectionPage();
      ASSERT_TRUE(page.ok()) << page.status();
      EXPECT_EQ(page->value_type_, type);
      EXPECT_EQ(page->next_cursor_, ++pages);
      EXPECT_EQ(page->retained_charge_.bytes(), page->RetainedBytes());
      EXPECT_LT(page->RetainedBytes(), 2 * 1024 * 1024);
      if (type == ValueType::kSortedSet)
        EXPECT_EQ(page->scored_members_.front().score_, double(700 - count));
      count += page->size();
      if (page->done_) break;
    }
    EXPECT_EQ(count, 700);
    EXPECT_GE(pages, 2);
    auto end = reader->NextStreaming();
    ASSERT_TRUE(end.ok());
    EXPECT_FALSE(end->has_value());
    reader->Rewind();
    auto first = reader->NextStreaming();
    ASSERT_TRUE(first.ok());
    EXPECT_TRUE(reader->DrainCollection().ok());
    EXPECT_TRUE(reader->DrainCollection().ok());
    reader->Rewind();
    auto legacy = reader->Next();
    ASSERT_TRUE(legacy.ok());
    ASSERT_TRUE(legacy->has_value());
    auto payload = EncodeDump((**legacy).value_);
    ASSERT_TRUE(payload.ok());
    auto dump = DumpReader::Open(*payload);
    ASSERT_TRUE(dump.ok());
    EXPECT_TRUE(dump->collection());
    EXPECT_EQ(dump->value_type(), type);
    for (unsigned pass = 0; pass < 2; ++pass) {
      std::size_t decoded = 0;
      for (;;) {
        auto page = dump->ReadCollectionPage();
        ASSERT_TRUE(page.ok()) << page.status();
        decoded += page->size();
        if (page->done_) break;
      }
      EXPECT_EQ(decoded, 700);
      ASSERT_TRUE(dump->Rewind().ok());
    }
    payload->back() ^= 1;
    EXPECT_FALSE(DumpReader::Open(*payload).ok());
  }
}

TEST(RdbCollectionImportTest,
     RejectsDuplicateIdentityAcrossPagesBeforeMutation) {
  for (auto type : {ValueType::kHash, ValueType::kSet, ValueType::kSortedSet}) {
    CollectionImportFile file;
    WriteImportFixture(file.path(), type, true);
    auto reader = FileReader::Open(file.path());
    ASSERT_TRUE(reader.ok());
    ASSERT_TRUE(reader->NextStreaming().ok());
    auto first = reader->ReadCollectionPage();
    ASSERT_TRUE(first.ok());
    EXPECT_FALSE(first->done_);
    auto duplicate = reader->DrainCollection();
    EXPECT_FALSE(duplicate.ok());
    EXPECT_NE(duplicate.message().find("duplicate"), std::string_view::npos);
  }
}

TEST(RdbCollectionImportTest, ReportsTruncationBetweenPages) {
  CollectionImportFile file;
  WriteImportFixture(file.path(), ValueType::kList, false);
  auto reader = FileReader::Open(file.path());
  ASSERT_TRUE(reader.ok()) << reader.status();
  ASSERT_TRUE(reader->NextStreaming().ok());
  auto first = reader->ReadCollectionPage();
  ASSERT_TRUE(first.ok()) << first.status();
  ASSERT_FALSE(first->done_);
  // The first page is already decoded. A short read of the next file segment
  // must fail the import instead of exposing an incomplete final page.
  ASSERT_EQ(::truncate(file.path(), 1024 * 1024 + 5), 0);
  auto status = reader->DrainCollection();
  ASSERT_FALSE(status.ok());
  EXPECT_NE(status.message().find("truncated while reading"),
            std::string_view::npos);
  EXPECT_EQ(reader->ReadCollectionPage().status(), status);
  reader->Rewind();
  ASSERT_TRUE(reader->NextStreaming().ok());
  EXPECT_EQ(reader->DrainCollection(), status);
}

TEST(RdbCollectionImportTest, QuicklistDrainsNodesWithoutAnAggregateString) {
  CollectionImportFile file;
  auto writer = FileWriter::Open(file.path());
  ASSERT_TRUE(writer.ok());
  std::string object;
  for (unsigned char byte :
       std::array<unsigned char, 10>{18, 1, 'k', 2, 1, 1, 'a', 1, 1, 'b'})
    object.push_back(static_cast<char>(byte));
  ASSERT_TRUE(writer->WriteFragment(object).ok());
  ASSERT_TRUE(writer->Finish().ok());
  auto reader = FileReader::Open(file.path());
  ASSERT_TRUE(reader.ok());
  auto key = reader->NextStreaming();
  ASSERT_TRUE(key.ok()) << key.status();
  ASSERT_TRUE(key->has_value());
  EXPECT_FALSE((**key).expected_items_.has_value());
  auto first = reader->ReadCollectionPage();
  ASSERT_TRUE(first.ok()) << first.status();
  EXPECT_EQ(first->elements_, (std::vector<std::string>{"a"}));
  EXPECT_FALSE(first->done_);
  auto second = reader->ReadCollectionPage();
  ASSERT_TRUE(second.ok());
  EXPECT_EQ(second->elements_, (std::vector<std::string>{"b"}));
  EXPECT_TRUE(second->done_);
}

TEST(RdbCollectionImportTest, PageAdmissionRejectsBeforeAllocatingThePage) {
  CollectionImportFile file;
  WriteImportFixture(file.path(), ValueType::kHash, false);
  ASSERT_TRUE(InitMemoryLimit(1024 * 1024, 1).ok());
  struct RestoreLimit {
    ~RestoreLimit() { (void)InitMemoryLimit(1024ULL * 1024 * 1024, 1); }
  } restore;
  const auto before = GetWorkerMemoryStats(0).retained_bytes_;
  {
    auto reader = FileReader::Open(file.path());
    ASSERT_TRUE(reader.ok());
    ASSERT_TRUE(reader->NextStreaming().ok());
    auto page = reader->ReadCollectionPage();
    ASSERT_FALSE(page.ok());
    EXPECT_TRUE(absl::IsResourceExhausted(page.status()));
  }
  EXPECT_EQ(GetWorkerMemoryStats(0).retained_bytes_, before);
}

TEST(RdbCollectionImportTest, PackedHashMeasuresEntriesBeforeDecoding) {
  CollectionImportFile file;
  auto writer = FileWriter::Open(file.path());
  ASSERT_TRUE(writer.ok());
  std::string object;
  for (unsigned char byte : std::array<unsigned char, 17>{
           16, 1, 'k', 13, 13, 0, 0, 0, 2, 0, 0x81, 'f', 2, 0x81, 'v', 2, 255})
    object.push_back(static_cast<char>(byte));
  ASSERT_TRUE(writer->WriteFragment(object).ok());
  ASSERT_TRUE(writer->Finish().ok());
  auto reader = FileReader::Open(file.path());
  ASSERT_TRUE(reader.ok());
  auto header = reader->NextStreaming();
  ASSERT_TRUE(header.ok());
  auto page = reader->ReadCollectionPage();
  ASSERT_TRUE(page.ok()) << page.status();
  ASSERT_EQ(page->fields_.size(), 1);
  EXPECT_EQ(page->fields_[0].field_, "f");
  EXPECT_EQ(page->fields_[0].value_, "v");
  EXPECT_TRUE(page->done_);
}

}  // namespace
}  // namespace lavik::rdb
