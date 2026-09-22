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

#include <fcntl.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "lavik/memory.h"

namespace lavik::rdb {

// Redis stores a group's global PEL before its consumer-to-ID associations.
// A bounded external merge joins/reorders those records without an in-memory
// PEL index. Scratch files are unlinked on creation and close with their owner;
// they are never recovery state. Each merge holds only two admitted records.
class RecordSpool {
 public:
  struct Record {
    std::optional<MemoryReservation> reservation_;
    std::string key_, value_;
  };
  absl::Status Add(std::string_view key, std::string_view value) {
    if (finished_)
      return absl::FailedPreconditionError("RDB spool already sealed");
    auto record = Allocate(key.size(), value.size());
    if (!record.ok()) return record.status();
    record->key_.assign(key);
    record->value_.assign(value);
    buffered_bytes_ += key.size() + value.size() + 128;
    buffered_.push_back(std::move(*record));
    return buffered_bytes_ >= 1024 * 1024 ? Flush() : absl::OkStatus();
  }
  absl::Status Finish() {
    if (finished_) return absl::OkStatus();
    auto status = Flush();
    if (!status.ok()) return status;
    for (auto& run : runs_) {
      if (!run) continue;
      if (!result_)
        result_ = std::move(run);
      else {
        auto merged = Merge(std::move(result_), std::move(run));
        if (!merged.ok()) return merged.status();
        result_ = std::move(*merged);
      }
    }
    finished_ = true;
    return absl::OkStatus();
  }
  absl::StatusOr<std::optional<Record>> Next() {
    if (!finished_)
      return absl::FailedPreconditionError("RDB spool is not sealed");
    if (!result_) return std::nullopt;
    return Read(result_.get());
  }
  // Count an adjacent run without retaining it, then restore the cursor.
  absl::StatusOr<std::uint64_t> CountPrefix(std::string_view prefix) {
    if (!finished_ || !result_)
      return absl::FailedPreconditionError("RDB spool is not readable");
    const auto position = ::ftello(result_.get());
    if (position < 0) return IoError();
    std::uint64_t count = 0;
    for (;;) {
      auto row = Read(result_.get());
      if (!row.ok()) return row.status();
      if (!*row || !(**row).key_.starts_with(prefix)) break;
      ++count;
    }
    if (::fseeko(result_.get(), position, SEEK_SET) != 0) return IoError();
    return count;
  }
  // Replaying a sealed spool supports count-before-ID RDB framing without
  // retaining a consumer's complete ID array in memory.
  absl::Status Rewind() {
    if (!finished_)
      return absl::FailedPreconditionError("RDB spool is not sealed");
    return result_ && ::fseeko(result_.get(), 0, SEEK_SET) != 0
               ? IoError()
               : absl::OkStatus();
  }

 private:
  struct Close {
    void operator()(std::FILE* file) const {
      if (file) std::fclose(file);
    }
  };
  using File = std::unique_ptr<std::FILE, Close>;
  static absl::Status IoError() {
    return absl::Status(
        errno == ENOSPC ? absl::StatusCode::kResourceExhausted
                        : absl::StatusCode::kInternal,
        std::string("RDB scratch file: ") + std::strerror(errno));
  }
  static absl::StatusOr<File> Open() {
    File file(std::tmpfile());
    if (!file) return IoError();
    if (::fcntl(::fileno(file.get()), F_SETFD, FD_CLOEXEC) < 0)
      return IoError();
    return file;
  }
  static absl::StatusOr<Record> Allocate(std::size_t key, std::size_t value) {
    if (key > (SIZE_MAX - 512) / 2 || value > (SIZE_MAX - 512) / 2 - key)
      return absl::ResourceExhaustedError("RDB scratch record size overflow");
    auto reservation = TryReserveMemory(2 * (key + value) + 512);
    if (!reservation) {
      RecordMemoryRejection();
      return absl::ResourceExhaustedError("OOM RDB scratch record");
    }
    Record record{.reservation_ = std::move(reservation)};
    record.key_.reserve(key);
    record.value_.reserve(value);
    return record;
  }
  static absl::Status Write(std::FILE* file, const Record& record) {
    std::array<unsigned char, 16> header{};
    for (unsigned i = 0; i < 8; ++i) {
      header[i] = std::uint64_t(record.key_.size()) >> (8 * i);
      header[8 + i] = std::uint64_t(record.value_.size()) >> (8 * i);
    }
    if (std::fwrite(header.data(), 1, header.size(), file) != header.size() ||
        std::fwrite(record.key_.data(), 1, record.key_.size(), file) !=
            record.key_.size() ||
        std::fwrite(record.value_.data(), 1, record.value_.size(), file) !=
            record.value_.size())
      return IoError();
    return absl::OkStatus();
  }
  static absl::StatusOr<std::optional<Record>> Read(std::FILE* file) {
    std::array<unsigned char, 16> header{};
    const auto count = std::fread(header.data(), 1, header.size(), file);
    if (count == 0 && std::feof(file)) return std::nullopt;
    if (count != header.size())
      return std::ferror(file)
                 ? IoError()
                 : absl::DataLossError("truncated RDB scratch header");
    std::uint64_t key = 0, value = 0;
    for (unsigned i = 0; i < 8; ++i) {
      key |= std::uint64_t(header[i]) << (8 * i);
      value |= std::uint64_t(header[8 + i]) << (8 * i);
    }
    auto record = Allocate(key, value);
    if (!record.ok()) return record.status();
    record->key_.resize(key);
    record->value_.resize(value);
    if (std::fread(record->key_.data(), 1, key, file) != key ||
        std::fread(record->value_.data(), 1, value, file) != value)
      return std::ferror(file)
                 ? IoError()
                 : absl::DataLossError("truncated RDB scratch record");
    return std::optional(std::move(*record));
  }
  static absl::Status Seal(std::FILE* file) {
    if (std::fflush(file) != 0 || ::fseeko(file, 0, SEEK_SET) != 0)
      return IoError();
    return absl::OkStatus();
  }
  static absl::StatusOr<File> Merge(File a, File b) {
    auto output = Open();
    if (!output.ok()) return output.status();
    auto left = Read(a.get()), right = Read(b.get());
    while (left.ok() && right.ok() && (*left || *right)) {
      const bool take_left =
          !*right || (*left && (**left).key_ <= (**right).key_);
      const auto& record = take_left ? **left : **right;
      auto status = Write(output->get(), record);
      if (!status.ok()) return status;
      if (take_left) {
        left->reset();
        left = Read(a.get());
      } else {
        right->reset();
        right = Read(b.get());
      }
    }
    if (!left.ok()) return left.status();
    if (!right.ok()) return right.status();
    auto status = Seal(output->get());
    if (!status.ok()) return status;
    return std::move(*output);
  }
  absl::Status Flush() {
    if (buffered_.empty()) return absl::OkStatus();
    std::sort(buffered_.begin(), buffered_.end(),
              [](const auto& a, const auto& b) { return a.key_ < b.key_; });
    auto run = Open();
    if (!run.ok()) return run.status();
    for (const auto& record : buffered_) {
      auto status = Write(run->get(), record);
      if (!status.ok()) return status;
    }
    std::vector<Record>().swap(buffered_);
    buffered_bytes_ = 0;
    auto status = Seal(run->get());
    if (!status.ok()) return status;
    for (auto& slot : runs_) {
      if (!slot) {
        slot = std::move(*run);
        return absl::OkStatus();
      }
      run = Merge(std::move(slot), std::move(*run));
      if (!run.ok()) return run.status();
    }
    return absl::ResourceExhaustedError("RDB scratch merge level overflow");
  }
  std::vector<Record> buffered_;
  std::size_t buffered_bytes_ = 0;
  std::array<File, 64> runs_{};
  File result_;
  bool finished_ = false;
};

}  // namespace lavik::rdb
