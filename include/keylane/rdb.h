#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "absl/status/statusor.h"
#include "keylane/storage/engine.h"

namespace keylane::rdb {

inline constexpr unsigned kVersion = 11;

enum class FileEntryKind : std::uint8_t {
  kValue,
  kSkippedModuleValue,
  kSkippedModuleAux,
  kSkippedFunction,
};

struct FileEntry {
  FileEntryKind kind_ = FileEntryKind::kValue;
  std::uint8_t db_id_ = 0;
  std::string key_;
  storage::RawValue value_;
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
