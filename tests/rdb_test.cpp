#include "keylane/rdb.h"

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "gtest/gtest.h"

namespace keylane::rdb {
namespace {

std::uint64_t Reflect64(std::uint64_t value) {
  std::uint64_t result = value & 1;
  for (unsigned bit = 1; bit < 64; ++bit) {
    value >>= 1;
    result = (result << 1) | (value & 1);
  }
  return result;
}

std::uint64_t RedisCrc64(std::string_view input) {
  constexpr std::uint64_t kPolynomial = 0xad93d23594c935a9ULL;
  std::uint64_t crc = 0;
  for (unsigned char byte : input) {
    for (unsigned mask = 1; mask <= 0x80; mask <<= 1) {
      bool high = (crc & (std::uint64_t{1} << 63)) != 0;
      if ((byte & mask) != 0) high = !high;
      crc <<= 1;
      if (high) crc ^= kPolynomial;
    }
  }
  return Reflect64(crc);
}

void PutLe16(std::string* output, std::uint16_t value) {
  output->push_back(static_cast<char>(value));
  output->push_back(static_cast<char>(value >> 8));
}

void PutLe32(std::string* output, std::uint32_t value) {
  for (unsigned byte = 0; byte < 4; ++byte) {
    output->push_back(static_cast<char>(value >> (byte * 8)));
  }
}

void PutLe64(std::string* output, std::uint64_t value) {
  for (unsigned byte = 0; byte < 8; ++byte) {
    output->push_back(static_cast<char>(value >> (byte * 8)));
  }
}

std::string Dump(std::string object, std::uint16_t version = 11) {
  PutLe16(&object, version);
  PutLe64(&object, RedisCrc64(object));
  return object;
}

std::string RdbString(std::string_view value) {
  EXPECT_LT(value.size(), 64);
  std::string result(1, static_cast<char>(value.size()));
  result.append(value);
  return result;
}

std::string Ziplist(const std::vector<std::string_view>& values) {
  std::string entries;
  std::uint32_t previous_size = 0;
  std::uint32_t tail = 10;
  for (std::string_view value : values) {
    EXPECT_LT(previous_size, 254);
    EXPECT_LT(value.size(), 64);
    tail = 10 + static_cast<std::uint32_t>(entries.size());
    entries.push_back(static_cast<char>(previous_size));
    entries.push_back(static_cast<char>(value.size()));
    entries.append(value);
    previous_size = 2 + static_cast<std::uint32_t>(value.size());
  }
  std::string result;
  PutLe32(&result, 10 + static_cast<std::uint32_t>(entries.size()) + 1);
  PutLe32(&result, tail);
  PutLe16(&result, static_cast<std::uint16_t>(values.size()));
  result += entries;
  result.push_back(static_cast<char>(0xff));
  return result;
}

std::string Hex(std::string_view input) {
  auto digit = [](char value) -> unsigned {
    if (value >= '0' && value <= '9') return value - '0';
    if (value >= 'a' && value <= 'f') return value - 'a' + 10;
    return value - 'A' + 10;
  };
  EXPECT_EQ(input.size() % 2, 0);
  std::string output;
  output.reserve(input.size() / 2);
  for (std::size_t i = 0; i < input.size(); i += 2) {
    output.push_back(
        static_cast<char>((digit(input[i]) << 4) | digit(input[i + 1])));
  }
  return output;
}

TEST(RdbTest, AcceptsRedisDumpVersionsOneThroughEleven) {
  for (std::uint16_t version = 1; version <= 11; ++version) {
    auto value = DecodeDump(Dump(Hex("0003666f6f"), version));
    ASSERT_TRUE(value.ok()) << value.status();
    EXPECT_EQ(value->value_type_, storage::ValueType::kString);
    EXPECT_EQ(value->encoded_, "foo");
  }

  EXPECT_FALSE(DecodeDump(Dump(Hex("0003666f6f"), 12)).ok());
}

TEST(RdbTest, AcceptsRedis72PackedFixturesAndEmitsVersionEleven) {
  struct Fixture {
    std::string_view hex_;
    storage::ValueType type_;
    std::uint64_t size_;
    std::uint8_t canonical_type_;
  };
  constexpr Fixture fixtures[] = {
      {"00036100620b003e3a9ef0d34e3e51", storage::ValueType::kString, 3, 0},
      {"1201020f0f00000003008161028162028001ff0b0014a88c640000c4d8",
       storage::ValueType::kList, 3, 1},
      {"1411110000000200836f6e65048374776f04ff0b00095f1e4ecafc45ae",
       storage::ValueType::kSet, 2, 2},
      {"0b140400000003000000feffffff04000000000001000b00b2580826b589188a",
       storage::ValueType::kSet, 3, 2},
      {"101717000000040082663103827631038266320382763203ff0b006a27f17fe84b4b35",
       storage::ValueType::kHash, 2, 4},
      {"1115150000000400816202dffe0281610283312e3504ff0b0005ea2085ce4f62d4",
       storage::ValueType::kSortedSet, 2, 5},
      {"150110000000000000000100000000000000001d1d0000000a000101000101018166020"
       "0010201000100018176020401ff0101000100000001000b0045cbe2ec26af3c4f",
       storage::ValueType::kStream, 1, 21},
      {"1501100000000000000001000000000000000529290000000f000201000101018166020"
       "001020100010001816102040102010101dffb028162020401ff0202000105000002000b"
       "006a3b198297c96c4f",
       storage::ValueType::kStream, 2, 21},
  };

  for (const Fixture& fixture : fixtures) {
    auto decoded = DecodeDump(Hex(fixture.hex_));
    ASSERT_TRUE(decoded.ok()) << decoded.status();
    EXPECT_EQ(decoded->value_type_, fixture.type_);
    EXPECT_EQ(decoded->logical_size_, fixture.size_);

    auto encoded = EncodeDump(*decoded);
    ASSERT_TRUE(encoded.ok()) << encoded.status();
    ASSERT_GE(encoded->size(), 11);
    EXPECT_EQ(static_cast<std::uint8_t>((*encoded)[0]),
              fixture.canonical_type_);
    EXPECT_EQ(static_cast<std::uint8_t>((*encoded)[encoded->size() - 10]), 11);
    EXPECT_EQ(static_cast<std::uint8_t>((*encoded)[encoded->size() - 9]), 0);
    EXPECT_TRUE(DecodeDump(*encoded).ok());
  }
}

TEST(RdbTest, AcceptsHistoricalCollectionEncodings) {
  const std::string list_ziplist = Ziplist({"a", "2"});
  const std::string hash_ziplist = Ziplist({"field", "value"});
  const std::string zset_ziplist = Ziplist({"member", "1.5"});

  std::string zipmap;
  zipmap.push_back(1);
  zipmap += Hex("056669656c64050076616c7565");
  zipmap.push_back(static_cast<char>(0xff));

  struct Historical {
    std::string payload_;
    storage::ValueType type_;
    std::uint64_t size_;
  };
  const std::vector<Historical> fixtures = {
      {Dump(Hex("09") + RdbString(zipmap), 4), storage::ValueType::kHash, 1},
      {Dump(Hex("0a") + RdbString(list_ziplist), 7), storage::ValueType::kList,
       2},
      {Dump(Hex("0c") + RdbString(zset_ziplist), 7),
       storage::ValueType::kSortedSet, 1},
      {Dump(Hex("0d") + RdbString(hash_ziplist), 7), storage::ValueType::kHash,
       1},
      {Dump(Hex("0e01") + RdbString(list_ziplist), 8),
       storage::ValueType::kList, 2},
      {Dump(Hex("0301066d656d62657203312e35"), 1),
       storage::ValueType::kSortedSet, 1},
      {Dump(Hex("00c0fb"), 1), storage::ValueType::kString, 2},
      {Dump(Hex("00c306050468656c6c6f"), 6), storage::ValueType::kString, 5},
  };

  for (const Historical& fixture : fixtures) {
    auto decoded = DecodeDump(fixture.payload_);
    ASSERT_TRUE(decoded.ok()) << decoded.status();
    EXPECT_EQ(decoded->value_type_, fixture.type_);
    EXPECT_EQ(decoded->logical_size_, fixture.size_);
    EXPECT_TRUE(EncodeDump(*decoded).ok());
  }
}

TEST(RdbTest, AcceptsAllRedisStreamEncodings) {
  const std::string node =
      Hex("0110000000000000000100000000000000001d1d0000000a000101000101018166"
          "0200010201000100018176020401ff");
  const std::vector<std::string> fixtures = {
      Dump(Hex("0f") + node + Hex("01010000"), 9),
      Dump(Hex("13") + node + Hex("010100010000000100"), 10),
      Dump(Hex("15") + node + Hex("010100010000000100"), 11),
  };
  for (const std::string& fixture : fixtures) {
    auto decoded = DecodeDump(fixture);
    ASSERT_TRUE(decoded.ok()) << decoded.status();
    EXPECT_EQ(decoded->value_type_, storage::ValueType::kStream);
    EXPECT_EQ(decoded->logical_size_, 1);
  }
}

TEST(RdbTest, RejectsChecksumAndMalformedObjects) {
  std::string valid = Dump(Hex("0003666f6f"));
  valid.back() ^= 1;
  auto corrupt = DecodeDump(valid);
  ASSERT_FALSE(corrupt.ok());
  EXPECT_EQ(corrupt.status().message(),
            "DUMP payload version or checksum are wrong");

  auto short_payload = DecodeDump("short");
  ASSERT_FALSE(short_payload.ok());
  EXPECT_EQ(short_payload.status().message(),
            "DUMP payload version or checksum are wrong");

  auto malformed = DecodeDump(Dump(Hex("7f")));
  ASSERT_FALSE(malformed.ok());
  EXPECT_TRUE(malformed.status().message().starts_with("Bad data format"));
}

}  // namespace
}  // namespace keylane::rdb
