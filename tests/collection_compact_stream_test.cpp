#include "keylane/storage/detail/collection_compact_stream.h"

#include <bit>
#include <limits>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "keylane/storage/detail/hash_codec.h"
#include "keylane/storage/detail/ordered_compact_codec.h"

namespace keylane::storage {
namespace {

CollectionPage Page(ValueType type) {
  CollectionPage result{.value_type_ = type};
  if (type == ValueType::kHash) {
    result.fields_ = {{"", ""},
                      {std::string("f\0x", 3), "value"},
                      {"last", std::string("v\0", 2)}};
  } else if (type == ValueType::kSortedSet) {
    result.scored_members_ = {
        {"", -std::numeric_limits<double>::infinity()},
        {std::string("m\0x", 3), -0.0},
        {"last", std::numeric_limits<double>::infinity()}};
  } else {
    result.elements_ = {"", std::string("a\0b", 3), "last"};
  }
  return result;
}

std::string Legacy(const CollectionPage& page) {
  if (page.value_type_ == ValueType::kHash ||
      page.value_type_ == ValueType::kSet) {
    HashValue value;
    for (const auto& field : page.fields_)
      value.entries_.push_back(
          {.field_ = field.field_, .value_ = field.value_});
    for (const auto& member : page.elements_)
      value.entries_.push_back({.field_ = member});
    auto result = EncodeHashValue(value, kMaxRecordPayloadBytes);
    EXPECT_TRUE(result.ok()) << result.status();
    return result.ok() ? *result : std::string{};
  }
  std::vector<OrderedCollectionEntry> entries;
  for (const auto& value : page.elements_) entries.push_back({.value_ = value});
  for (const auto& member : page.scored_members_)
    entries.push_back({.value_ = member.member_, .score_ = member.score_});
  auto result = EncodeOrderedCompactValue(
      page.value_type_ == ValueType::kList ? OrderedCollectionKind::kList
                                           : OrderedCollectionKind::kSortedSet,
      entries);
  EXPECT_TRUE(result.ok()) << result.status();
  return result.ok() ? *result : std::string{};
}

void AppendPage(CollectionPage& to, CollectionPage from) {
  for (auto& field : from.fields_) to.fields_.push_back(std::move(field));
  for (auto& value : from.elements_) to.elements_.push_back(std::move(value));
  for (auto& member : from.scored_members_)
    to.scored_members_.push_back(std::move(member));
}

void ExpectEqual(const CollectionPage& left, const CollectionPage& right) {
  ASSERT_EQ(left.value_type_, right.value_type_);
  ASSERT_EQ(left.fields_.size(), right.fields_.size());
  for (std::size_t i = 0; i < left.fields_.size(); ++i) {
    EXPECT_EQ(left.fields_[i].field_, right.fields_[i].field_);
    EXPECT_EQ(left.fields_[i].value_, right.fields_[i].value_);
  }
  EXPECT_EQ(left.elements_, right.elements_);
  ASSERT_EQ(left.scored_members_.size(), right.scored_members_.size());
  for (std::size_t i = 0; i < left.scored_members_.size(); ++i) {
    EXPECT_EQ(left.scored_members_[i].member_,
              right.scored_members_[i].member_);
    EXPECT_EQ(std::bit_cast<std::uint64_t>(left.scored_members_[i].score_),
              std::bit_cast<std::uint64_t>(right.scored_members_[i].score_));
  }
}

constexpr ValueType kTypes[] = {ValueType::kHash, ValueType::kSet,
                                ValueType::kList, ValueType::kSortedSet};

TEST(CollectionCompactStream, EncoderIsByteCompatibleWithEveryLegacyType) {
  for (auto type : kTypes) {
    const auto page = Page(type);
    const auto expected = Legacy(page);
    auto size = CollectionCompactEncoder::MeasurePage(page);
    ASSERT_TRUE(size.ok()) << size.status();
    EXPECT_EQ(
        *size + (type == ValueType::kHash || type == ValueType::kSet ? 32 : 8),
        expected.size());
    auto encoder =
        CollectionCompactEncoder::Create(type, page.size(), expected.size());
    ASSERT_TRUE(encoder.ok());
    EXPECT_FALSE(encoder->Finish().ok());
    std::string actual;
    while (auto piece = encoder->Next()) actual.append(*piece);
    EXPECT_TRUE(encoder->page_done());
    CollectionPage empty{.value_type_ = type};
    ASSERT_TRUE(encoder->StartPage(empty).ok());
    ASSERT_TRUE(encoder->StartPage(page).ok());
    EXPECT_FALSE(encoder->StartPage(page).ok());
    while (auto piece = encoder->Next()) actual.append(*piece);
    EXPECT_TRUE(encoder->Finish().ok());
    EXPECT_EQ(actual, expected);
  }
}

TEST(CollectionCompactStream, DecoderHandlesEveryTwoChunkBoundary) {
  for (auto type : kTypes) {
    const auto page = Page(type);
    const auto encoded = Legacy(page);
    for (std::size_t split = 0; split <= encoded.size(); ++split) {
      auto decoder =
          CollectionCompactDecoder::Create(type, page.size(), encoded.size());
      ASSERT_TRUE(decoder.ok());
      const auto first =
          decoder->Consume(std::string_view(encoded).substr(0, split));
      ASSERT_TRUE(first.ok()) << first.status();
      EXPECT_EQ(*first, split);
      auto second = decoder->Consume(std::string_view(encoded).substr(split));
      ASSERT_TRUE(second.ok()) << second.status();
      EXPECT_EQ(*second, encoded.size() - split);
      ASSERT_TRUE(decoder->page_ready());
      EXPECT_TRUE(decoder->Finish().ok());
      auto decoded = decoder->TakePage();
      ASSERT_TRUE(decoded.ok());
      EXPECT_EQ(decoded->next_cursor_, 1);
      EXPECT_TRUE(decoded->done_);
      ExpectEqual(*decoded, page);
      EXPECT_FALSE(decoder->TakePage().ok());
      EXPECT_TRUE(decoder->Consume("").ok());
      EXPECT_FALSE(decoder->Consume("x").ok());
      EXPECT_FALSE(decoder->Finish().ok());
    }
  }
}

TEST(CollectionCompactStream, OneByteInputAndEveryTruncatedEofAreChecked) {
  for (auto type : kTypes) {
    const auto page = Page(type);
    const auto encoded = Legacy(page);
    for (std::size_t end = 0; end <= encoded.size(); ++end) {
      auto decoder =
          CollectionCompactDecoder::Create(type, page.size(), encoded.size());
      ASSERT_TRUE(decoder.ok());
      for (std::size_t i = 0; i < end; ++i) {
        auto used = decoder->Consume(std::string_view(encoded).substr(i, 1));
        ASSERT_TRUE(used.ok()) << used.status();
        ASSERT_EQ(*used, 1);
      }
      EXPECT_EQ(decoder->Finish().ok(), end == encoded.size());
      if (end != encoded.size()) EXPECT_FALSE(decoder->Consume(encoded).ok());
    }
  }
}

TEST(CollectionCompactStream, EveryHeaderByteIsValidatedAgainstTransport) {
  for (auto type : kTypes) {
    const auto page = Page(type);
    const auto encoded = Legacy(page);
    const auto header =
        type == ValueType::kHash || type == ValueType::kSet ? 32 : 8;
    for (int i = 0; i < header; ++i) {
      auto corrupt = encoded;
      corrupt[i] ^= 1;
      auto decoder =
          CollectionCompactDecoder::Create(type, page.size(), encoded.size());
      ASSERT_TRUE(decoder.ok());
      EXPECT_FALSE(decoder->Consume(corrupt).ok()) << i;
      EXPECT_FALSE(decoder->Consume(encoded).ok());
    }
  }
}

TEST(CollectionCompactStream, SetValuesNanAndInconsistentContainersFailClosed) {
  auto hash = Page(ValueType::kHash);
  auto encoded = Legacy(hash);
  auto set = CollectionCompactDecoder::Create(ValueType::kSet, hash.size(),
                                              encoded.size());
  ASSERT_TRUE(set.ok());
  EXPECT_FALSE(set->Consume(encoded).ok());
  auto sorted = Page(ValueType::kSortedSet);
  sorted.scored_members_[0].score_ = std::numeric_limits<double>::quiet_NaN();
  EXPECT_FALSE(CollectionCompactEncoder::MeasurePage(sorted).ok());
  encoded = Legacy(Page(ValueType::kSortedSet));
  const auto bits =
      std::bit_cast<std::uint64_t>(sorted.scored_members_[0].score_);
  for (unsigned i = 0; i < 8; ++i)
    encoded[8 + i] = static_cast<char>(bits >> (8 * i));
  auto decoder = CollectionCompactDecoder::Create(ValueType::kSortedSet, 3,
                                                  encoded.size());
  ASSERT_TRUE(decoder.ok());
  EXPECT_FALSE(decoder->Consume(encoded).ok());
  hash.elements_.push_back("wrong-container");
  EXPECT_FALSE(CollectionCompactEncoder::MeasurePage(hash).ok());
  hash.value_type_ = ValueType::kNone;
  EXPECT_FALSE(CollectionCompactEncoder::MeasurePage(hash).ok());
}

TEST(CollectionCompactStream, PagesPauseBeforeLargeEntryAndTransferAdmission) {
  for (auto type : kTypes) {
    auto page = Page(type);
    std::string huge(9 * 1024 * 1024, 'x');
    if (type == ValueType::kHash)
      page.fields_[1].value_ = huge;
    else if (type == ValueType::kSortedSet)
      page.scored_members_[1].member_ = huge;
    else
      page.elements_[1] = huge;
    const auto encoded = Legacy(page);
    std::size_t charged = 0;
    auto decoder = CollectionCompactDecoder::Create(
        type, page.size(), encoded.size(), [&](std::size_t bytes) {
          charged += bytes;
          return absl::OkStatus();
        });
    ASSERT_TRUE(decoder.ok());
    CollectionPage result{.value_type_ = type};
    std::size_t offset = 0;
    std::size_t pages = 0;
    while (offset < encoded.size() || decoder->page_ready()) {
      if (!decoder->page_ready()) {
        const auto chunk = std::min<std::size_t>(4093, encoded.size() - offset);
        auto used =
            decoder->Consume(std::string_view(encoded).substr(offset, chunk));
        ASSERT_TRUE(used.ok()) << used.status();
        offset += *used;
      }
      if (decoder->page_ready()) {
        EXPECT_EQ(*decoder->Consume("ignored-until-page-taken"), 0);
        EXPECT_FALSE(
            decoder->TakePage().ok());  // Do not lose the charge owner.
        std::size_t transferred = 0;
        auto part = decoder->TakePage(&transferred);
        ASSERT_TRUE(part.ok());
        EXPECT_EQ(part->size(), 1);
        EXPECT_EQ(part->next_cursor_, ++pages);
        EXPECT_EQ(part->done_, pages == 3);
        AppendPage(result, std::move(*part));
        // The test retains pages in result, so retains their external charge.
        EXPECT_LE(transferred, charged);
        EXPECT_EQ(decoder->pending_admitted_bytes(), 0);
      }
    }
    EXPECT_EQ(pages, 3);
    EXPECT_GT(charged, huge.size());
    EXPECT_TRUE(decoder->Finish().ok());
    ExpectEqual(result, page);
  }
}

TEST(CollectionCompactStream,
     AdmissionRejectsBeforeAllocatingAnnouncedHugeString) {
  // Valid maximum-sized members must reach admission without allocating them.
  // Invalid lengths must fail even before admission is consulted.
  for (auto type : kTypes) {
    const auto header =
        type == ValueType::kHash || type == ValueType::kSet ? 32 : 8;
    const auto frame = type == ValueType::kSortedSet ? 12
                       : header == 32                ? 8
                                                     : 4;
    const std::uint64_t total = header + frame + kMaxStringBytes;
    auto encoder = CollectionCompactEncoder::Create(type, 1, total);
    ASSERT_TRUE(encoder.ok());
    std::string prefix(*encoder->Next());
    prefix.resize(header + frame);
    const auto length_offset = header + (type == ValueType::kSortedSet ? 8 : 0);
    for (unsigned i = 0; i < 4; ++i)
      prefix[length_offset + i] = static_cast<char>(kMaxStringBytes >> (8 * i));
    std::size_t requested = 0;
    auto decoder = CollectionCompactDecoder::Create(
        type, 1, total, [&](std::size_t bytes) {
          requested = bytes;
          return absl::ResourceExhaustedError("test budget");
        });
    ASSERT_TRUE(decoder.ok());
    auto used = decoder->Consume(prefix);
    ASSERT_FALSE(used.ok());
    EXPECT_EQ(used.status().code(), absl::StatusCode::kResourceExhausted);
    EXPECT_EQ(requested, kMaxStringBytes + 1);
    EXPECT_EQ(decoder->pending_admitted_bytes(), 0);
    prefix[length_offset] = 1;
    requested = 0;
    decoder = CollectionCompactDecoder::Create(type, 1, total,
                                               [&](std::size_t bytes) {
                                                 requested = bytes;
                                                 return absl::OkStatus();
                                               });
    ASSERT_TRUE(decoder.ok());
    EXPECT_FALSE(decoder->Consume(prefix).ok());
    EXPECT_EQ(requested, 0);
  }
}

TEST(CollectionCompactStream, HashGroupFramingLimitIsCheckedBeforeAllocation) {
  auto encoder = CollectionCompactEncoder::Create(
      ValueType::kHash, 2, 2 * kMaxRecordPayloadBytes - 1024);
  ASSERT_TRUE(encoder.ok());
  std::string prefix(*encoder->Next());
  prefix.resize(40);
  for (unsigned i = 0; i < 4; ++i) {
    prefix[32 + i] = static_cast<char>(kMaxStringBytes >> (8 * i));
    prefix[36 + i] = static_cast<char>(kMaxStringBytes >> (8 * i));
  }
  auto decoder = CollectionCompactDecoder::Create(
      ValueType::kHash, 2, 2 * kMaxRecordPayloadBytes - 1024, [](std::size_t) {
        ADD_FAILURE() << "malformed lengths reached admission";
        return absl::ResourceExhaustedError("unexpected");
      });
  ASSERT_TRUE(decoder.ok());
  EXPECT_FALSE(decoder->Consume(prefix).ok());
}

TEST(CollectionCompactStream, VectorAdmissionFailureAndMoveKeepOneReceipt) {
  CollectionPage page{.value_type_ = ValueType::kHash};
  page.fields_.push_back({"field", "value"});
  const auto encoded = Legacy(page);
  std::size_t charged = 0;
  auto decoder = CollectionCompactDecoder::Create(
      ValueType::kHash, 1, encoded.size(), [&](std::size_t bytes) {
        if (bytes >= sizeof(CollectionField))
          return absl::ResourceExhaustedError("deny page vector");
        charged += bytes;
        return absl::OkStatus();
      });
  ASSERT_TRUE(decoder.ok());
  auto used = decoder->Consume(encoded);
  ASSERT_FALSE(used.ok());
  EXPECT_EQ(used.status().code(), absl::StatusCode::kResourceExhausted);
  EXPECT_EQ(charged, 12);
  EXPECT_EQ(decoder->pending_admitted_bytes(), charged);
  auto moved = std::move(*decoder);
  EXPECT_EQ(decoder->pending_admitted_bytes(), 0);
  EXPECT_EQ(moved.pending_admitted_bytes(), charged);
  EXPECT_FALSE(moved.TakePage().ok());
  EXPECT_FALSE(moved.Finish().ok());
}

TEST(CollectionCompactStream, DeclaredTotalsAndPageBudgetsCannotBeBypassed) {
  for (auto type : kTypes) {
    const auto page = Page(type);
    const auto encoded = Legacy(page);
    auto encoder =
        CollectionCompactEncoder::Create(type, page.size(), encoded.size() - 1);
    ASSERT_TRUE(encoder.ok());
    EXPECT_FALSE(encoder->StartPage(page).ok());
    EXPECT_FALSE(encoder->Finish().ok());
    encoder = CollectionCompactEncoder::Create(type, page.size() + 1,
                                               encoded.size() + 12);
    ASSERT_TRUE(encoder.ok());
    ASSERT_TRUE(encoder->StartPage(page).ok());
    while (encoder->Next()) {
    }
    EXPECT_FALSE(encoder->Finish().ok());
    auto extra = encoded;
    extra.push_back('x');
    if (type == ValueType::kHash || type == ValueType::kSet) {
      for (unsigned i = 0; i < 8; ++i)
        extra[24 + i] = static_cast<char>(extra.size() >> (8 * i));
    }
    auto decoder =
        CollectionCompactDecoder::Create(type, page.size(), extra.size());
    ASSERT_TRUE(decoder.ok());
    EXPECT_FALSE(decoder->Consume(extra).ok());
    EXPECT_FALSE(decoder->page_ready());
    EXPECT_FALSE(decoder->Finish().ok());
  }
}

TEST(CollectionCompactStream,
     AggregateEncodingExceedsOneGibWithoutWholeValueBuffer) {
  CollectionPage page{.value_type_ = ValueType::kList};
  page.elements_.push_back(std::string(9 * 1024 * 1024, 'x'));
  auto measured = CollectionCompactEncoder::MeasurePage(page);
  ASSERT_TRUE(measured.ok());
  constexpr std::uint64_t count = 128;
  const auto total = 8 + count * *measured;
  ASSERT_GT(total, kMaxRecordPayloadBytes);
  auto encoder =
      CollectionCompactEncoder::Create(ValueType::kList, count, total);
  ASSERT_TRUE(encoder.ok());
  auto decoder =
      CollectionCompactDecoder::Create(ValueType::kList, count, total);
  ASSERT_TRUE(decoder.ok());
  std::uint64_t emitted = 0;
  std::uint64_t decoded_count = 0;
  for (std::uint64_t i = 0; i < count; ++i) {
    ASSERT_TRUE(encoder->StartPage(page).ok());
    while (auto piece = encoder->Next()) {
      emitted += piece->size();
      if (piece->size() == page.elements_[0].size())
        EXPECT_EQ(piece->data(), page.elements_[0].data());
      std::size_t offset = 0;
      while (offset < piece->size()) {
        auto used = decoder->Consume(piece->substr(offset));
        ASSERT_TRUE(used.ok()) << used.status();
        offset += *used;
        if (decoder->page_ready()) {
          auto decoded = decoder->TakePage();
          ASSERT_TRUE(decoded.ok());
          ASSERT_EQ(decoded->elements_.size(), 1);
          EXPECT_EQ(decoded->elements_[0], page.elements_[0]);
          ++decoded_count;
        }
      }
    }
  }
  EXPECT_EQ(emitted, total);
  EXPECT_TRUE(encoder->Finish().ok());
  EXPECT_EQ(decoded_count, count);
  EXPECT_TRUE(decoder->Finish().ok());
  EXPECT_FALSE(CollectionCompactEncoder::Create(ValueType::kList, 0, 8).ok());
  EXPECT_FALSE(
      CollectionCompactEncoder::Create(ValueType::kList, 1ULL << 32, total)
          .ok());
  EXPECT_FALSE(
      CollectionCompactDecoder::Create(ValueType::kList, 1, total).ok());
  EXPECT_FALSE(
      CollectionCompactDecoder::Create(
          ValueType::kHash, 1, std::numeric_limits<std::uint64_t>::max())
          .ok());
}

}  // namespace
}  // namespace keylane::storage
