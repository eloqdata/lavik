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

#include "lavik/rdb.h"

#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "support/test_data_path.h"

namespace lavik::rdb {
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

std::string RdbLength(std::uint32_t length) {
  if (length < 64) return std::string(1, static_cast<char>(length));
  if (length < 16384) {
    return {static_cast<char>(0x40 | (length >> 8)), static_cast<char>(length)};
  }
  std::string result(1, static_cast<char>(0x80));
  for (unsigned byte = 0; byte < 4; ++byte)
    result.push_back(static_cast<char>(length >> (24 - byte * 8)));
  return result;
}

std::string RdbString(std::string_view value) {
  EXPECT_LE(value.size(), UINT32_MAX);
  std::string result = RdbLength(static_cast<std::uint32_t>(value.size()));
  result.append(value);
  return result;
}

std::string RdbFile(std::string body, unsigned version) {
  char header[10];
  std::snprintf(header, sizeof(header), "REDIS%04u", version);
  std::string result(header, 9);
  result += body;
  result.push_back(static_cast<char>(0xff));
  if (version >= 5) PutLe64(&result, RedisCrc64(result));
  return result;
}

TEST(RdbTest, StreamEncoderSupportsRedisSevenCompatibilityHeader) {
  StreamEncoder encoder(10);
  EXPECT_EQ(encoder.Header(), "REDIS0010");
  std::string streamed(encoder.Header());
  streamed += encoder.Finish();
  EXPECT_EQ(streamed, RdbFile({}, 10));
}

TEST(RdbTest, LavikStreamV1RequiresMacroNodeCounts) {
  std::string encoded = "LXS1";
  PutLe64(&encoded, 1);  // last ID milliseconds
  PutLe64(&encoded, 0);  // last ID sequence
  PutLe64(&encoded, 0);  // max-deleted ID milliseconds
  PutLe64(&encoded, 0);  // max-deleted ID sequence
  PutLe64(&encoded, 1);  // entries added
  PutLe32(&encoded, 1);  // live entries
  PutLe64(&encoded, 1);
  PutLe64(&encoded, 0);
  PutLe32(&encoded, 2);  // field and value
  PutLe32(&encoded, 1);
  encoded += "f";
  PutLe32(&encoded, 1);
  encoded += "v";
  const auto node_offset = encoded.size();
  PutLe32(&encoded, 1);  // macro nodes
  PutLe32(&encoded, 1);  // entries in the first macro node
  PutLe32(&encoded, 0);  // consumer groups

  storage::RawValue raw{.encoded_ = std::move(encoded),
                        .logical_size_ = 1,
                        .value_type_ = storage::ValueType::kStream};
  auto dump = EncodeDump(raw);
  ASSERT_TRUE(dump.ok()) << dump.status();
  auto restored = DecodeDump(*dump);
  ASSERT_TRUE(restored.ok()) << restored.status();
  EXPECT_EQ(restored->value_type_, storage::ValueType::kStream);
  EXPECT_EQ(restored->logical_size_, 1);
  EXPECT_TRUE(restored->encoded_.starts_with("LXS1"));
  EXPECT_EQ(restored->encoded_, raw.encoded_);

  auto unsupported = raw;
  unsupported.encoded_[3] = '2';
  EXPECT_FALSE(EncodeDump(unsupported).ok());
  // A same-marker development layout without node counts is not upgraded.
  raw.encoded_.erase(node_offset, 8);
  EXPECT_FALSE(EncodeDump(raw).ok());
}

class TempFile {
 public:
  explicit TempFile(std::string_view contents) {
    std::string path = lavik::test::TestDataPath("lavik-rdb-test-XXXXXX");
    const int fd = ::mkstemp(path.data());
    EXPECT_GE(fd, 0);
    path_ = path;
    std::size_t written = 0;
    while (fd >= 0 && written < contents.size()) {
      const ssize_t count =
          ::write(fd, contents.data() + written, contents.size() - written);
      EXPECT_GT(count, 0);
      if (count <= 0) break;
      written += static_cast<std::size_t>(count);
    }
    if (fd >= 0) ::close(fd);
  }

  ~TempFile() {
    if (!path_.empty()) ::unlink(path_.c_str());
  }

  const std::string& path() const { return path_; }

 private:
  std::string path_;
};

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

TEST(RdbTest, ReadsCompleteFilesVersionsOneThroughEleven) {
  for (unsigned version = 1; version <= 11; ++version) {
    TempFile file(
        RdbFile(Hex("00") + RdbString("key") + RdbString("value"), version));
    auto reader = FileReader::Open(file.path());
    ASSERT_TRUE(reader.ok()) << reader.status();
    EXPECT_EQ(reader->version(), version);
    auto entry = reader->Next();
    ASSERT_TRUE(entry.ok()) << entry.status();
    ASSERT_TRUE(entry->has_value());
    EXPECT_EQ((*entry)->db_id_, 0);
    EXPECT_EQ((*entry)->key_, "key");
    EXPECT_EQ((*entry)->value_.value_type_, storage::ValueType::kString);
    EXPECT_EQ((*entry)->value_.encoded_, "value");
    EXPECT_EQ((*entry)->value_.expire_at_ms_, 0);
    auto eof = reader->Next();
    ASSERT_TRUE(eof.ok()) << eof.status();
    EXPECT_FALSE(eof->has_value());
  }
}

TEST(RdbTest, ReadsAcrossFileBuffersAndRewindsAfterMoveAndUnlink) {
  // Place the expiration across the 1 MiB read boundary, followed by a value
  // spanning several buffers. Values contain binary data, not just padding.
  std::string value(2 * 1024 * 1024 + 37, '\0');
  for (std::size_t i = 0; i < value.size(); ++i)
    value[i] = static_cast<char>(i % 251);
  const std::string padding(1024 * 1024 - 21, 'p');
  std::string body = Hex("00") + RdbString("pad") + RdbString(padding);
  ASSERT_EQ(body.size() + 9, 1024 * 1024 - 2);
  body.push_back(static_cast<char>(0xfc));
  PutLe64(&body, 4'102'444'800'123ULL);
  body += Hex("00") + RdbString("large") + RdbString(value);
  body += Hex("00") + RdbString("integer") + Hex("c2ffffff7f");
  for (unsigned version : {4, 11}) {
    TempFile file(RdbFile(body, version));
    auto opened = FileReader::Open(file.path());
    ASSERT_TRUE(opened.ok()) << opened.status();
    FileReader reader(std::move(*opened));
    ASSERT_EQ(::unlink(file.path().c_str()), 0);
    for (unsigned pass = 0; pass < 2; ++pass) {
      auto first = reader.Next();
      ASSERT_TRUE(first.ok()) << first.status();
      ASSERT_TRUE(first->has_value());
      EXPECT_EQ((**first).value_.encoded_, padding);
      auto large = reader.Next();
      ASSERT_TRUE(large.ok()) << large.status();
      ASSERT_TRUE(large->has_value());
      EXPECT_EQ((**large).key_, "large");
      EXPECT_EQ((**large).value_.encoded_, value);
      EXPECT_EQ((**large).value_.expire_at_ms_, 4'102'444'800'123ULL);
      auto integer = reader.Next();
      ASSERT_TRUE(integer.ok()) << integer.status();
      ASSERT_TRUE(integer->has_value());
      EXPECT_EQ((**integer).value_.encoded_, "2147483647");
      auto end = reader.Next();
      ASSERT_TRUE(end.ok()) << end.status();
      EXPECT_FALSE(end->has_value());
      reader.Rewind();
    }
  }
}

TEST(RdbTest, ChecksEveryFileBufferAndAcceptsDisabledChecksum) {
  // The checksum itself straddles a buffer boundary.
  constexpr std::size_t value_bytes = 2 * 1024 * 1024 - 26;
  std::string contents = RdbFile(
      Hex("00") + RdbString("large") + RdbString(std::string(value_bytes, 'x')),
      11);
  ASSERT_EQ(contents.size() - 8, 2 * 1024 * 1024 - 4);
  for (std::size_t at : {std::size_t{1024 * 1024 - 1}, std::size_t{1024 * 1024},
                         contents.size() - 10}) {
    contents[at] ^= 1;
    {
      TempFile file(contents);
      auto reader = FileReader::Open(file.path());
      ASSERT_FALSE(reader.ok());
      EXPECT_EQ(reader.status().message(), "RDB file checksum is invalid");
    }
    contents[at] ^= 1;
  }
  std::fill(contents.end() - 8, contents.end(), '\0');
  TempFile file(contents);
  auto reader = FileReader::Open(file.path());
  ASSERT_TRUE(reader.ok()) << reader.status();
  auto entry = reader->Next();
  ASSERT_TRUE(entry.ok()) << entry.status();
  ASSERT_TRUE(entry->has_value());
  EXPECT_EQ((**entry).value_.encoded_.size(), value_bytes);
}

TEST(RdbTest, ReportsFileTruncationDuringDecodeUntilRewind) {
  TempFile file(RdbFile(Hex("00") + RdbString("large") +
                            RdbString(std::string(2 * 1024 * 1024, 'x')),
                        11));
  auto reader = FileReader::Open(file.path());
  ASSERT_TRUE(reader.ok()) << reader.status();
  ASSERT_EQ(::truncate(file.path().c_str(), 1024 * 1024 + 5), 0);
  auto entry = reader->Next();
  ASSERT_FALSE(entry.ok());
  EXPECT_NE(entry.status().message().find("truncated while reading"),
            std::string_view::npos);
  EXPECT_EQ(reader->NextStreaming().status(), entry.status());
  reader->Rewind();
  EXPECT_EQ(reader->Next().status(), entry.status());
}

TEST(RdbTest, DecodesLzfAcrossFileBuffersWithoutBorrowedInput) {
  std::string expected(2 * 1024 * 1024 + 37, '\0');
  for (std::size_t i = 0; i < expected.size(); ++i)
    expected[i] = static_cast<char>(i % 251);
  std::string compressed;
  for (std::size_t at = 0; at < expected.size();) {
    const auto size = std::min(std::size_t{32}, expected.size() - at);
    compressed.push_back(static_cast<char>(size - 1));
    compressed.append(expected, at, size);
    at += size;
  }
  // Exercise both short and extended overlapping back-references as well as
  // literal spans that cross a refill boundary.
  compressed += Hex("2000e0ff00");
  expected.append(3 + 264, expected.back());
  const auto encoded = Hex("c3") + RdbLength(compressed.size()) +
                       RdbLength(expected.size()) + compressed;
  auto memory = DecodeDump(Dump(Hex("00") + encoded));
  ASSERT_TRUE(memory.ok()) << memory.status();
  EXPECT_EQ(memory->encoded_, expected);
  TempFile file(RdbFile(Hex("00") + RdbString("large") + encoded, 11));
  auto reader = FileReader::Open(file.path());
  ASSERT_TRUE(reader.ok()) << reader.status();
  auto entry = reader->Next();
  ASSERT_TRUE(entry.ok()) << entry.status();
  ASSERT_TRUE(entry->has_value());
  EXPECT_EQ((**entry).value_.encoded_, expected);
}

TEST(RdbTest, LzfCannotReadPastItsDeclaredCompressedLength) {
  for (const auto compressed :
       {Hex("0061e0"), Hex("0061e0ff"), Hex("2000"), Hex("026162")}) {
    const auto encoded =
        Hex("c3") + RdbLength(compressed.size()) + RdbLength(10) + compressed;
    EXPECT_FALSE(DecodeDump(Dump(Hex("00") + encoded)).ok());
    // Bytes belonging to the next key must not complete a truncated token.
    TempFile file(RdbFile(Hex("00") + RdbString("bad") + encoded + Hex("00") +
                              RdbString("next") + RdbString("v"),
                          11));
    auto reader = FileReader::Open(file.path());
    ASSERT_TRUE(reader.ok()) << reader.status();
    EXPECT_FALSE(reader->Next().ok());
  }
}

TEST(RdbTest, ReadsFileMetadataDatabasesAndExpirations) {
  std::string body;
  body.push_back(static_cast<char>(0xfa));  // AUX
  body += RdbString("redis-ver");
  body += RdbString("7.2.0");
  body.push_back(static_cast<char>(0xfe));  // SELECTDB
  body.push_back(2);
  body.push_back(static_cast<char>(0xfb));  // RESIZEDB
  body.push_back(2);
  body.push_back(1);
  body.push_back(static_cast<char>(0xfc));  // EXPIRETIME_MS
  PutLe64(&body, 4'102'444'800'123ULL);
  body.push_back(static_cast<char>(0xf8));  // IDLE
  body.push_back(7);
  body.push_back(0);  // String
  body += RdbString("future");
  body += RdbString("alive");
  body.push_back(static_cast<char>(0xf9));  // FREQ
  body.push_back(9);
  body.push_back(0);  // String
  body += RdbString("persistent");
  body += RdbString("value");

  TempFile file(RdbFile(std::move(body), 11));
  auto reader = FileReader::Open(file.path());
  ASSERT_TRUE(reader.ok()) << reader.status();

  auto future = reader->Next();
  ASSERT_TRUE(future.ok()) << future.status();
  ASSERT_TRUE(future->has_value());
  EXPECT_EQ((*future)->db_id_, 2);
  EXPECT_EQ((*future)->key_, "future");
  EXPECT_EQ((*future)->value_.encoded_, "alive");
  EXPECT_EQ((*future)->value_.expire_at_ms_, 4'102'444'800'123ULL);

  auto persistent = reader->Next();
  ASSERT_TRUE(persistent.ok()) << persistent.status();
  ASSERT_TRUE(persistent->has_value());
  EXPECT_EQ((*persistent)->db_id_, 2);
  EXPECT_EQ((*persistent)->key_, "persistent");
  EXPECT_EQ((*persistent)->value_.expire_at_ms_, 0);
  auto eof = reader->Next();
  ASSERT_TRUE(eof.ok()) << eof.status();
  EXPECT_FALSE(eof->has_value());

  reader->Rewind();
  auto rewound = reader->Next();
  ASSERT_TRUE(rewound.ok()) << rewound.status();
  EXPECT_TRUE(rewound->has_value());
}

TEST(RdbTest, WritesAtomicRedisCompatibleFileFromIndependentFragments) {
  TempFile target("");
  ASSERT_EQ(::unlink(target.path().c_str()), 0);
  auto writer = FileWriter::Open(target.path());
  ASSERT_TRUE(writer.ok()) << writer.status();

  storage::RawValue first{
      .encoded_ = "first-value",
      .logical_size_ = 11,
      .expire_at_ms_ = 4'102'444'800'123ULL,
      .value_type_ = storage::ValueType::kString,
  };
  storage::RawValue second{
      .encoded_ = "second-value",
      .logical_size_ = 12,
      .value_type_ = storage::ValueType::kString,
  };
  auto first_fragment = EncodeFileEntry(2, "first", first);
  auto second_fragment = EncodeFileEntry(0, "second", second);
  ASSERT_TRUE(first_fragment.ok()) << first_fragment.status();
  ASSERT_TRUE(second_fragment.ok()) << second_fragment.status();
  ASSERT_TRUE(writer->WriteFragment(*first_fragment).ok());
  ASSERT_TRUE(writer->WriteFragment(*second_fragment).ok());
  ASSERT_TRUE(writer->Finish().ok());

  auto reader = FileReader::Open(target.path());
  ASSERT_TRUE(reader.ok()) << reader.status();
  EXPECT_EQ(reader->version(), 11u);
  auto first_entry = reader->Next();
  ASSERT_TRUE(first_entry.ok()) << first_entry.status();
  ASSERT_TRUE(first_entry->has_value());
  EXPECT_EQ((**first_entry).db_id_, 2);
  EXPECT_EQ((**first_entry).key_, "first");
  EXPECT_EQ((**first_entry).value_.encoded_, "first-value");
  EXPECT_EQ((**first_entry).value_.expire_at_ms_, 4'102'444'800'123ULL);
  auto second_entry = reader->Next();
  ASSERT_TRUE(second_entry.ok()) << second_entry.status();
  ASSERT_TRUE(second_entry->has_value());
  EXPECT_EQ((**second_entry).db_id_, 0);
  EXPECT_EQ((**second_entry).key_, "second");
  EXPECT_EQ((**second_entry).value_.encoded_, "second-value");
  auto eof = reader->Next();
  ASSERT_TRUE(eof.ok()) << eof.status();
  EXPECT_FALSE(eof->has_value());
}

TEST(RdbTest, ReadsHistoricalSecondExpirationWithoutChecksum) {
  std::string body;
  body.push_back(static_cast<char>(0xfd));
  PutLe32(&body, 2'000'000'000U);
  body.push_back(0);
  body += RdbString("old");
  body += RdbString("value");
  TempFile file(RdbFile(std::move(body), 1));

  auto reader = FileReader::Open(file.path());
  ASSERT_TRUE(reader.ok()) << reader.status();
  auto entry = reader->Next();
  ASSERT_TRUE(entry.ok()) << entry.status();
  ASSERT_TRUE(entry->has_value());
  EXPECT_EQ((*entry)->value_.expire_at_ms_, 2'000'000'000'000ULL);
}

TEST(RdbTest, SkipsSelfDescribingUnsupportedRedisData) {
  std::string body;
  body.push_back(static_cast<char>(0xf5));  // FUNCTION2
  body += RdbString("#!lua name=library");

  body.push_back(static_cast<char>(0xf7));  // MODULE_AUX
  body.push_back(11);                       // Module id
  body.push_back(2);                        // UINT opcode for 'when'
  body.push_back(0);                        // Before-RDB phase
  body.push_back(1);                        // SINT opcode
  body.push_back(7);
  body.push_back(3);  // FLOAT opcode
  body.append(4, '\0');
  body.push_back(4);  // DOUBLE opcode
  body.append(8, '\0');
  body.push_back(5);  // STRING opcode
  body += RdbString("aux-data");
  body.push_back(0);  // Module EOF

  body.push_back(7);  // MODULE_2 object
  body += RdbString("module-key");
  body.push_back(12);  // Module id
  body.push_back(2);   // UINT opcode
  body.push_back(9);
  body.push_back(5);  // STRING opcode
  body += RdbString("module-data");
  body.push_back(0);  // Module EOF

  body.push_back(0);  // String object
  body += RdbString("kept");
  body += RdbString("value");

  TempFile file(RdbFile(std::move(body), 11));
  auto reader = FileReader::Open(file.path());
  ASSERT_TRUE(reader.ok()) << reader.status();

  auto function = reader->Next();
  ASSERT_TRUE(function.ok()) << function.status();
  ASSERT_TRUE(function->has_value());
  EXPECT_EQ((**function).kind_, FileEntryKind::kFunctionLibrary);
  EXPECT_EQ((**function).function_code_, "#!lua name=library");

  auto module_aux = reader->Next();
  ASSERT_TRUE(module_aux.ok()) << module_aux.status();
  ASSERT_TRUE(module_aux->has_value());
  EXPECT_EQ((**module_aux).kind_, FileEntryKind::kSkippedModuleAux);

  auto module_value = reader->Next();
  ASSERT_TRUE(module_value.ok()) << module_value.status();
  ASSERT_TRUE(module_value->has_value());
  EXPECT_EQ((**module_value).kind_, FileEntryKind::kSkippedModuleValue);
  EXPECT_EQ((**module_value).key_, "module-key");

  auto kept = reader->Next();
  ASSERT_TRUE(kept.ok()) << kept.status();
  ASSERT_TRUE(kept->has_value());
  EXPECT_EQ((**kept).kind_, FileEntryKind::kValue);
  EXPECT_EQ((**kept).key_, "kept");
  EXPECT_EQ((**kept).value_.encoded_, "value");
}

TEST(RdbTest, EncodesAndDecodesRedisFunctionDump) {
  const std::vector<std::string> libraries = {
      "#!lua name=one\nredis.register_function('one', function() return 1 end)",
      "#!lua name=two\nredis.register_function('two', function() return 2 end)",
  };
  const std::string payload = EncodeFunctionDump(libraries);
  ASSERT_TRUE(FunctionDumpEncodedSize(libraries).has_value());
  EXPECT_EQ(*FunctionDumpEncodedSize(libraries), payload.size());
  auto decoded = DecodeFunctionDump(payload);
  ASSERT_TRUE(decoded.ok()) << decoded.status();
  EXPECT_EQ(*decoded, libraries);

  std::string corrupt = payload;
  corrupt[1] ^= 1;
  EXPECT_FALSE(DecodeFunctionDump(corrupt).ok());
}

TEST(RdbTest, ComputesFunctionDumpSizeAcrossLengthEncodings) {
  const std::vector<std::string> empty;
  ASSERT_TRUE(FunctionDumpEncodedSize(empty).has_value());
  EXPECT_EQ(*FunctionDumpEncodedSize(empty), 10);

  const std::vector<std::string> libraries = {
      std::string(63, 'a'), std::string(64, 'b'), std::string(16384, 'c')};
  ASSERT_TRUE(FunctionDumpEncodedSize(libraries).has_value());
  EXPECT_EQ(*FunctionDumpEncodedSize(libraries), 16532);
  EXPECT_EQ(*FunctionDumpEncodedSize(libraries),
            EncodeFunctionDump(libraries).size());
}

TEST(RdbTest, RejectsCorruptAndUnsupportedCompleteFiles) {
  std::string corrupt =
      RdbFile(Hex("00") + RdbString("k") + RdbString("v"), 11);
  corrupt.back() ^= 1;
  TempFile corrupt_file(corrupt);
  EXPECT_FALSE(FileReader::Open(corrupt_file.path()).ok());

  TempFile future_file(RdbFile({}, 12));
  EXPECT_FALSE(FileReader::Open(future_file.path()).ok());

  std::string outside_db;
  outside_db.push_back(static_cast<char>(0xfe));
  outside_db.push_back(16);
  TempFile outside_file(RdbFile(std::move(outside_db), 11));
  auto reader = FileReader::Open(outside_file.path());
  ASSERT_TRUE(reader.ok()) << reader.status();
  EXPECT_FALSE(reader->Next().ok());

  std::string pre_ga_module;
  pre_ga_module.push_back(6);
  pre_ga_module += RdbString("legacy-module");
  TempFile pre_ga_file(RdbFile(std::move(pre_ga_module), 11));
  auto pre_ga_reader = FileReader::Open(pre_ga_file.path());
  ASSERT_TRUE(pre_ga_reader.ok()) << pre_ga_reader.status();
  EXPECT_FALSE(pre_ga_reader->Next().ok());
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

    // DumpReader borrows its payload, including during every page decode.
    const auto source = Hex(fixture.hex_);
    auto streaming = DumpReader::Open(source);
    ASSERT_TRUE(streaming.ok()) << streaming.status();
    EXPECT_EQ(streaming->value_type(), fixture.type_);
    if (streaming->collection()) {
      std::uint64_t count = 0;
      for (;;) {
        auto page = streaming->ReadCollectionPage();
        ASSERT_TRUE(page.ok()) << page.status();
        EXPECT_EQ(page->value_type_, fixture.type_);
        count += page->size();
        if (page->done_) break;
      }
      EXPECT_EQ(count, fixture.size_);
    }

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
    auto streaming = DumpReader::Open(fixture.payload_);
    ASSERT_TRUE(streaming.ok()) << streaming.status();
    if (streaming->collection()) {
      std::uint64_t count = 0;
      for (;;) {
        auto page = streaming->ReadCollectionPage();
        ASSERT_TRUE(page.ok()) << page.status();
        EXPECT_EQ(page->value_type_, fixture.type_);
        count += page->size();
        if (page->done_) break;
      }
      EXPECT_EQ(count, fixture.size_);
    }
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
}  // namespace lavik::rdb
