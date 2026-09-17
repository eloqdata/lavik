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

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/statusor.h"
#include "lavik/storage/engine.h"

namespace lavik::rdb {

inline constexpr unsigned kVersion = 11;

enum class FileEntryKind : std::uint8_t {
  kValue,
  kSkippedModuleValue,
  kSkippedModuleAux,
  kFunctionLibrary,
};

struct FileEntry {
  FileEntryKind kind_ = FileEntryKind::kValue;
  std::uint8_t db_id_ = 0;
  std::string key_;
  storage::RawValue value_;
  std::string function_code_;
  // A streaming collection has metadata only in value_. The caller must
  // drain ReadCollectionPage() through done_ before advancing the file.
  bool collection_stream_ = false;
  std::optional<std::uint64_t> expected_items_{};
};

// Reads Redis RDB versions 1 through 11 using bounded file I/O scratch. Open()
// validates the header and optional checksum before exposing any entries.
// The reader owns the open descriptor; callers must keep its contents immutable
// until destruction, including across validation and application passes.
// Next() materializes one value; NextStreaming() yields collection pages.
class FileReader {
 public:
  static absl::StatusOr<FileReader> Open(const std::string& path);

  FileReader(FileReader&&) noexcept;
  FileReader& operator=(FileReader&&) noexcept;
  FileReader(const FileReader&) = delete;
  FileReader& operator=(const FileReader&) = delete;
  ~FileReader();

  absl::StatusOr<std::optional<FileEntry>> Next();
  // Unlike Next(), this does not assemble a collection's aggregate payload.
  // Duplicate identity validation remains active across pages, including the
  // validation pass that must precede destructive load-rdb replacement.
  absl::StatusOr<std::optional<FileEntry>> NextStreaming();
  // Pages of one collection stay on one memory-accounting owner. A failed
  // read is terminal for that import attempt; rewind before trying again.
  absl::StatusOr<storage::CollectionPage> ReadCollectionPage();
  absl::Status DrainCollection();
  // Restarts parsing on the same open file, discarding cached bytes and any
  // read error. This does not revalidate the checksum or reopen the pathname.
  void Rewind();
  unsigned version() const noexcept;

 private:
  struct Impl;
  explicit FileReader(std::unique_ptr<Impl> impl);
  absl::StatusOr<std::optional<FileEntry>> NextImpl(bool stream_collections);

  std::unique_ptr<Impl> impl_;
};

// Borrowed value-only RDB input for RESTORE. Open validates the complete
// checksum before decoding; the caller keeps payload alive until this reader
// is destroyed. Collection pages follow the same admitted/duplicate-checked
// protocol as FileReader, while other value types retain the raw-value path.
class DumpReader {
 public:
  static absl::StatusOr<DumpReader> Open(std::string_view payload);
  DumpReader(DumpReader&&) noexcept;
  DumpReader& operator=(DumpReader&&) noexcept;
  ~DumpReader();
  bool collection() const noexcept;
  storage::ValueType value_type() const noexcept;
  std::optional<std::uint64_t> expected_items() const noexcept;
  absl::StatusOr<storage::CollectionPage> ReadCollectionPage();
  absl::StatusOr<storage::RawValue> ReadRawValue();
  absl::Status Rewind();

 private:
  struct Impl;
  explicit DumpReader(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

// Applies a streamed or ordinary file entry on its key owner. The reader and
// entry are borrowed until completion; a collection import is atomic at EOF.
bycorf::Task<absl::StatusOr<storage::RestoreRawResult>> RestoreFileEntry(
    storage::StorageEngine* storage, FileReader* reader, const FileEntry& entry,
    bool replace = false);

// Encodes and decodes the value-only payload used by Redis DUMP/RESTORE.
// Expiration is deliberately supplied by RESTORE and is not part of payload.
absl::StatusOr<std::string> EncodeDump(const storage::RawValue& value);
absl::StatusOr<storage::RawValue> DecodeDump(std::string_view payload);

// Encodes one self-contained RDB key fragment. It includes SELECTDB so
// fragments produced concurrently by different storage workers may be
// written in any order by the single checksum/file sink.
absl::StatusOr<std::string> EncodeFileEntry(std::uint8_t db_id,
                                            std::string_view key,
                                            const storage::RawValue& value);

// Redis 7 FUNCTION2 stores one complete library source as an RDB string.
// FUNCTION DUMP concatenates these entries and appends the RDB version and
// CRC64 footer used by RESTORE payloads.
std::string EncodeFunctionLibraryEntry(std::string_view code);
// Returns the exact encoded size without materializing the dump. Absence means
// the entry overhead and source lengths cannot be represented by size_t.
std::optional<std::size_t> FunctionDumpEncodedSize(
    std::span<const std::string> libraries) noexcept;
std::string EncodeFunctionDump(std::span<const std::string> libraries);
absl::StatusOr<std::vector<std::string>> DecodeFunctionDump(
    std::string_view payload);

// Stateful checksum encoder for a diskless RDB transfer. Header() must be
// sent first, every subsequently sent fragment must be passed to Account(),
// and Finish() returns the EOF opcode plus Redis' little-endian CRC64.
class StreamEncoder {
 public:
  explicit StreamEncoder(unsigned version = kVersion);

  std::string_view Header() const noexcept { return header_; }
  void Account(std::string_view fragment) noexcept;
  std::string Finish();

 private:
  std::string header_;
  std::uint64_t crc_ = 0;
  bool finished_ = false;
};

// Blocking filesystem sink used only by the backup writer thread. Open writes
// the Redis header to a same-directory temporary file; Finish appends EOF and
// checksum, fdatasyncs, atomically renames, and fsyncs the directory.
class FileWriter {
 public:
  static absl::StatusOr<FileWriter> Open(std::string target_path);

  FileWriter(FileWriter&&) noexcept;
  FileWriter& operator=(FileWriter&&) noexcept;
  FileWriter(const FileWriter&) = delete;
  FileWriter& operator=(const FileWriter&) = delete;
  ~FileWriter();

  absl::Status WriteFragment(std::string_view fragment);
  absl::Status Finish();

 private:
  struct Impl;
  explicit FileWriter(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

}  // namespace lavik::rdb
