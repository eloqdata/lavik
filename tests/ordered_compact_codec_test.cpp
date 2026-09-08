#include "keylane/storage/detail/ordered_compact_codec.h"

#include <bit>
#include <limits>

#include "gtest/gtest.h"

namespace keylane::storage {
namespace {

TEST(OrderedCompactCodecTest, ListUsesExistingLogicalWireImage) {
  const std::vector<OrderedCollectionEntry> entries{
      {.value_ = "a"}, {.value_ = std::string("b\0c", 3)}};
  auto encoded =
      EncodeOrderedCompactValue(OrderedCollectionKind::kList, entries);
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  const std::string expected(
      "KLL1\x02\0\0\0\x01\0\0\0"
      "a\x03\0\0\0"
      "b\0c",
      20);
  EXPECT_EQ(*encoded, expected);
  auto decoded =
      DecodeOrderedCompactValue(OrderedCollectionKind::kList, *encoded, 2);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(*decoded, entries);
}

TEST(OrderedCompactCodecTest, SortedScoresRetainInfinityAndNegativeZero) {
  const std::vector<OrderedCollectionEntry> entries{
      {.value_ = "lo", .score_ = -std::numeric_limits<double>::infinity()},
      {.value_ = "zero", .score_ = -0.0},
      {.value_ = "hi", .score_ = std::numeric_limits<double>::infinity()}};
  auto encoded =
      EncodeOrderedCompactValue(OrderedCollectionKind::kSortedSet, entries);
  ASSERT_TRUE(encoded.ok()) << encoded.status();
  EXPECT_TRUE(encoded->starts_with("KZS1"));
  auto decoded =
      DecodeOrderedCompactValue(OrderedCollectionKind::kSortedSet, *encoded, 3);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(*decoded, entries);
  EXPECT_EQ(std::bit_cast<std::uint64_t>((*decoded)[1].score_),
            std::bit_cast<std::uint64_t>(-0.0));
}

TEST(OrderedCompactCodecTest, BoundsAndFramingFailClosed) {
  const std::vector<OrderedCollectionEntry> entries{{.value_ = "abc"}};
  auto encoded =
      EncodeOrderedCompactValue(OrderedCollectionKind::kList, entries);
  ASSERT_TRUE(encoded.ok());
  EXPECT_FALSE(
      EncodeOrderedCompactValue(OrderedCollectionKind::kList, entries, 14)
          .ok());
  EXPECT_TRUE(
      EncodeOrderedCompactValue(OrderedCollectionKind::kList, entries, 15)
          .ok());
  for (std::size_t size = 0; size < encoded->size(); ++size) {
    EXPECT_FALSE(
        DecodeOrderedCompactValue(OrderedCollectionKind::kList,
                                  std::string_view(*encoded).substr(0, size), 1)
            .ok());
  }
  EXPECT_FALSE(
      DecodeOrderedCompactValue(OrderedCollectionKind::kList, *encoded, 2)
          .ok());
  EXPECT_FALSE(
      DecodeOrderedCompactValue(OrderedCollectionKind::kList, *encoded + "x", 1)
          .ok());
  EXPECT_FALSE(
      DecodeOrderedCompactValue(OrderedCollectionKind::kSortedSet, *encoded, 1)
          .ok());
}

TEST(OrderedCompactCodecTest, LargeItemAndInvalidScore) {
  std::vector<OrderedCollectionEntry> entries{
      {.value_ = std::string(9 * 1024 * 1024, 'x')}};
  auto encoded =
      EncodeOrderedCompactValue(OrderedCollectionKind::kList, entries);
  ASSERT_TRUE(encoded.ok());
  auto decoded =
      DecodeOrderedCompactValue(OrderedCollectionKind::kList, *encoded, 1);
  ASSERT_TRUE(decoded.ok());
  EXPECT_EQ(*decoded, entries);
  entries.front().score_ = std::numeric_limits<double>::quiet_NaN();
  EXPECT_FALSE(
      EncodeOrderedCompactValue(OrderedCollectionKind::kSortedSet, entries)
          .ok());
  entries.front().score_ = -0.0;
  EXPECT_FALSE(
      EncodeOrderedCompactValue(OrderedCollectionKind::kList, entries).ok());
}

}  // namespace
}  // namespace keylane::storage
