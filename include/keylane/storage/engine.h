#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "celer/base/status.h"
#include "celer/runtime/task.h"
#include "keylane/read_trace.h"
#include "keylane/storage/buffer_pool.h"
#include "keylane/storage/format.h"

namespace celer {
class Worker;
}  // namespace celer

namespace keylane::storage {

struct StorageEngineOptions {
  std::vector<std::string> data_files{"keylane.data"};
  std::uint32_t flush_max_ms = 1000;
  std::size_t flush_size_bytes = 8 * 1024 * 1024;
  bool verify_read_crc = true;
  bool expiration_authority = true;
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
    kValueBegin = 4,
    kValueChunk = 5,
    kValueCommit = 6,
  };

  Kind kind = Kind::kValue;
  std::uint8_t db_id = 0;
  std::uint64_t db_epoch = 0;
  std::uint64_t mutation_sequence = 0;
  std::uint64_t expire_at_ms = 0;
  ValueType value_type = ValueType::kNone;
  std::uint64_t logical_size = 0;
  std::uint32_t chunk_index = 0;
  std::uint32_t chunk_count = 0;
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

enum class SetCondition : std::uint8_t {
  kNone,
  kIfAbsent,
  kIfPresent,
};

struct SetOptions {
  SetCondition condition = SetCondition::kNone;
  // Absolute Unix time in milliseconds. Zero clears the TTL unless
  // keep_ttl is set.
  std::uint64_t expire_at_ms = 0;
  bool keep_ttl = false;
  bool return_old_value = false;
};

struct SetResult {
  bool applied = false;
  std::optional<DiskValue> old_value;
};

enum class ExpirationCondition : std::uint8_t {
  kNone,
  kIfNoExpiration,
  kIfHasExpiration,
  kIfGreater,
  kIfLess,
};

struct ExpirationInfo {
  bool exists = false;
  std::uint64_t expire_at_ms = 0;
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
  // now_ms fixes the expiration filter timestamp (0 = current time), so a
  // multi-pass scan can see a stable notion of liveness.
  ScanBatch ScanPartition(std::uint16_t partition_id, std::uint8_t db_id,
                          std::uint64_t cursor, std::size_t count,
                          std::uint64_t now_ms = 0) const;

  // Atomically invalidates one logical DB by advancing its durable epoch and
  // taking its indexes out of service. The command layer must prevent
  // concurrent operations in that DB while this coroutine runs, and may allow
  // them again as soon as it returns: the DB is observably empty from here on.
  // Cost is bounded by the partition count, not by the number of keys.
  celer::Task<celer::Status> FlushDbDetach(std::uint8_t db_id);

  // Retires what FlushDbDetach took out of service, subtracting it from the
  // block accounting and freeing it. Safe to run with the DB open and serving.
  // `wait` distinguishes FLUSHDB SYNC from FLUSHDB ASYNC: a reclaimer runs
  // either way, and only the caller's completion differs.
  celer::Task<celer::Status> FlushDbReclaim(bool wait);
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
  celer::Task<celer::StatusOr<std::uint64_t>> StringLength(
      std::uint8_t db_id, std::string_view key);
  celer::Task<celer::StatusOr<SetResult>> Set(
      std::uint8_t db_id, std::string_view key, std::string_view value,
      SetOptions options = {});
  celer::Task<ExpirationInfo> GetExpiration(std::uint8_t db_id,
                                            std::string_view key);
  celer::Task<celer::StatusOr<bool>> UpdateExpiration(
      std::uint8_t db_id, std::string_view key,
      std::uint64_t expire_at_ms, ExpirationCondition condition);
  celer::Task<celer::StatusOr<bool>> Delete(std::uint8_t db_id,
                                             std::string_view key);
  celer::Task<bool> Exists(std::uint8_t db_id, std::string_view key);
  celer::Task<celer::StatusOr<std::int64_t>> Increment(
      std::uint8_t db_id, std::string_view key);

  // Pre-locked variants for the transaction layer. The caller must already
  // hold this worker's key lock for `digest` in the required mode (shared for
  // reads, exclusive for writes), must run on OwnerForKey(key), and `digest`
  // must equal ComputeDigest(key). Write variants take the worker's
  // writer_mutex internally and release it before returning.
  celer::Task<celer::StatusOr<DiskValue>> GetLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      ReadLatencyTrace* trace = nullptr);
  celer::Task<celer::StatusOr<std::uint64_t>> StringLengthLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest);
  celer::Task<celer::StatusOr<SetResult>> SetLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      std::string_view value, SetOptions options = {});
  celer::Task<ExpirationInfo> GetExpirationLocked(std::uint8_t db_id,
                                                  std::string_view key,
                                                  const Digest& digest);
  celer::Task<celer::StatusOr<bool>> UpdateExpirationLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      std::uint64_t expire_at_ms, ExpirationCondition condition);
  celer::Task<celer::StatusOr<bool>> DeleteLocked(std::uint8_t db_id,
                                                  std::string_view key,
                                                  const Digest& digest);
  celer::Task<bool> ExistsLocked(std::uint8_t db_id, std::string_view key,
                                 const Digest& digest);
  celer::Task<celer::StatusOr<std::int64_t>> IncrementLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest);

  // Freeze/unfreeze expiration writes for stable-count scans (KEYS). The
  // caller must already exclude client writes (closed database gate).
  celer::Task<celer::Status> QuiesceExpiration();
  void ResumeExpiration() noexcept;

  // Non-suspending index probe for WATCH: whether the key currently holds a
  // live (non-tombstone, unexpired) value. Must run on OwnerForKey(key).
  bool KeyLive(std::uint8_t db_id, std::string_view key,
               const Digest& digest) const;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace keylane::storage
