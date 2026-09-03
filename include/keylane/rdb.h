#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/statusor.h"
#include "keylane/storage/engine.h"

namespace keylane::rdb {

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
};

// Memory-maps and validates one complete Redis RDB file. Files produced by
// RDB versions 1 through 11 are accepted. Next() materializes only one value
// at a time, so importing a large database does not retain the whole dataset
// in process memory.
class FileReader {
 public:
  static absl::StatusOr<FileReader> Open(const std::string& path);

  FileReader(FileReader&&) noexcept;
  FileReader& operator=(FileReader&&) noexcept;
  FileReader(const FileReader&) = delete;
  FileReader& operator=(const FileReader&) = delete;
  ~FileReader();

  absl::StatusOr<std::optional<FileEntry>> Next();
  void Rewind();
  unsigned version() const noexcept;

 private:
  struct Impl;
  explicit FileReader(std::unique_ptr<Impl> impl);

  std::unique_ptr<Impl> impl_;
};

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

}  // namespace keylane::rdb
