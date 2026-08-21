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

}  // namespace keylane::rdb
