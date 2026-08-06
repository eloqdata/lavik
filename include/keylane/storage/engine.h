#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "celer/base/status.h"
#include "celer/runtime/task.h"
#include "keylane/read_trace.h"
#include "keylane/storage/buffer_pool.h"

namespace celer {
class Worker;
}  // namespace celer

namespace keylane::storage {

struct StorageEngineOptions {
  std::vector<std::string> data_files{"keylane.data"};
  std::uint64_t file_size_bytes = 1024ULL * 1024 * 1024;
  std::uint32_t flush_max_ms = 1000;
  std::size_t flush_size_bytes = 8 * 1024 * 1024;
  bool verify_read_crc = true;
  RegisteredBufferPoolOptions buffers{};
};

// A value read directly into a registered storage buffer. network_bytes()
// contains a complete RESP bulk-string frame and remains valid until this
// move-only object is destroyed after the network send CQE.
class DiskValue {
 public:
  DiskValue() = default;
  DiskValue(ReadBufferLease lease, std::size_t network_offset,
            std::size_t network_size) noexcept
      : lease_(std::move(lease)),
        network_offset_(network_offset),
        network_size_(network_size) {}

  DiskValue(const DiskValue&) = delete;
  DiskValue& operator=(const DiskValue&) = delete;
  DiskValue(DiskValue&&) noexcept = default;
  DiskValue& operator=(DiskValue&&) noexcept = default;

  std::span<const std::byte> network_bytes() const noexcept {
    auto bytes = lease_.bytes();
    return {bytes.data() + network_offset_, network_size_};
  }

 private:
  ReadBufferLease lease_;
  std::size_t network_offset_ = 0;
  std::size_t network_size_ = 0;
};

class StorageEngine {
 public:
  explicit StorageEngine(StorageEngineOptions options);
  StorageEngine(const StorageEngine&) = delete;
  StorageEngine& operator=(const StorageEngine&) = delete;
  ~StorageEngine();

  // Runs on the main thread before Server::Start. Creates/preallocates every
  // configured file and sizes per-worker metadata, but does not perform data IO.
  celer::Status Prepare(unsigned worker_count);

  // Runs once on each worker before its listener is opened. Registers the
  // complete fixed-file table, opens every file with O_DIRECT into its fixed
  // slot, and performs parallel recovery.
  celer::Task<celer::Status> InitializeWorker(celer::Worker& worker);
  celer::Status FlushForShutdown();

  unsigned OwnerForKey(std::string_view key) const noexcept;
  unsigned worker_count() const noexcept;
  std::size_t LocalSize() const noexcept;

  // These operations must execute on OwnerForKey(key), normally through
  // SubmitTaskTo. Only digest/location metadata is retained after completion.
  celer::Task<celer::StatusOr<DiskValue>> Get(std::string_view key,
                                               ReadLatencyTrace* trace = nullptr);
  celer::Task<celer::Status> Set(std::string_view key, std::string_view value);
  celer::Task<celer::StatusOr<bool>> Delete(std::string_view key);
  celer::Task<bool> Exists(std::string_view key);
  celer::Task<celer::StatusOr<std::int64_t>> Increment(std::string_view key);

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace keylane::storage
