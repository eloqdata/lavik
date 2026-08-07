#pragma once

#include <array>
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
  std::uint32_t flush_max_ms = 1000;
  std::size_t flush_size_bytes = 8 * 1024 * 1024;
  bool verify_read_crc = true;
  RegisteredBufferPoolOptions buffers{};
};

struct ScanBatch {
  std::uint64_t cursor = 0;
  std::vector<std::string> keys;
};

struct SnapshotRecord {
  enum class Kind : std::uint8_t {
    kValue = 1,
    kDelete = 2,
    kFlushDb = 3,
  };

  Kind kind = Kind::kValue;
  std::uint8_t db_id = 0;
  std::uint64_t db_epoch = 0;
  std::uint64_t mutation_sequence = 0;
  std::string key;
  std::string value;
};

struct PartitionReplicationStart {
  std::uint64_t snapshot_sequence = 0;
  std::uint16_t nonempty_db_mask = 0;
  std::array<std::uint64_t, 16> db_epochs{};
};

struct PartitionSnapshotBatch {
  std::uint64_t cursor = 0;
  std::vector<SnapshotRecord> records;
};

struct PartitionDeltaBatch {
  std::uint64_t watermark = 0;
  bool overflow = false;
  std::vector<SnapshotRecord> records;
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
  std::size_t LocalSize(std::uint8_t db_id) const noexcept;
  // Must run on the worker owning partition_id. The cursor is stateless and
  // may return duplicate keys while the partition index is changing.
  ScanBatch ScanPartition(std::uint16_t partition_id, std::uint8_t db_id,
                          std::uint64_t cursor, std::size_t count) const;

  // Atomically invalidates one logical DB by advancing its durable epoch.
  // The command layer must prevent concurrent operations in that DB while this
  // coroutine runs.
  celer::Task<celer::Status> FlushDb(std::uint8_t db_id);
  std::uint64_t DbEpoch(std::uint8_t db_id) const noexcept;

  // Source-side partition migration primitives. Begin captures
  // a sequence fence, Snapshot reads the baseline tree, and ReadDeltas returns
  // every mutation after that fence until acknowledged.
  PartitionReplicationStart BeginPartitionReplication(
      std::uint16_t partition_id);
  celer::Task<celer::StatusOr<PartitionSnapshotBatch>> SnapshotPartition(
      std::uint16_t partition_id, std::uint8_t db_id, std::uint64_t cursor,
      std::size_t count);
  PartitionDeltaBatch ReadPartitionDeltas(std::uint16_t partition_id,
                                          std::uint64_t after_sequence,
                                          std::size_t count);
  void AcknowledgePartitionDeltas(std::uint16_t partition_id,
                                  std::uint64_t through_sequence);
  bool TryTakeReplicationReady(std::uint16_t* partition_id);

  // Replica-side primitives. Reset returns a new local replication epoch that
  // fences every record from an earlier copy of this partition.
  celer::Task<celer::StatusOr<std::uint64_t>> ResetReplicaPartition(
      std::uint16_t partition_id,
      std::span<const std::uint64_t, 16> source_db_epochs);
  celer::Task<celer::Status> ApplyReplicaRecords(
      std::uint16_t partition_id, std::uint64_t replication_epoch,
      std::span<const SnapshotRecord> records);

  // These operations must execute on OwnerForKey(key), normally through
  // SubmitTaskTo. Only digest/location metadata is retained after completion.
  celer::Task<celer::StatusOr<DiskValue>> Get(
      std::uint8_t db_id, std::string_view key,
      ReadLatencyTrace* trace = nullptr);
  celer::Task<celer::Status> Set(std::uint8_t db_id, std::string_view key,
                                 std::string_view value);
  celer::Task<celer::StatusOr<bool>> Delete(std::uint8_t db_id,
                                             std::string_view key);
  celer::Task<bool> Exists(std::uint8_t db_id, std::string_view key);
  celer::Task<celer::StatusOr<std::int64_t>> Increment(
      std::uint8_t db_id, std::string_view key);

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace keylane::storage
