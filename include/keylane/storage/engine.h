#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/statusor.h"
#include "celer/runtime/task.h"
#include "keylane/read_trace.h"
#include "keylane/storage/buffer_pool.h"
#include "keylane/storage/format.h"

namespace celer {
class Worker;
}  // namespace celer

namespace keylane::storage {

struct StorageEngineOptions {
  std::vector<std::string> data_files_{"keylane.data"};
  std::uint32_t flush_max_ms_ = 1000;
  std::size_t flush_size_bytes_ = 128 * 1024;
  bool verify_read_crc_ = true;
  bool expiration_authority_ = true;
  // Keys at or below this size stay complete in the in-memory index. Larger
  // keys are stored in disk extents and verified on demand.
  std::size_t inline_key_max_bytes_ = kDefaultInlineKeyBytes;
  // Full-disk sweep retiring tombstones no surviving record needs. Zero
  // disables it.
  std::uint32_t tomb_raider_interval_ms_ = 86'400'000;
  // Pause after each block the sweep reads, capping its share of disk
  // bandwidth so online traffic keeps its latency.
  std::uint32_t tomb_raider_sleep_ms_ = 10;
  // Maximum number of block relocations allowed to run concurrently on one
  // device. This cannot exceed the eight-block per-device defrag reserve.
  unsigned defrag_max_active_per_device_ = 8;
  // Asynchronous cooldown after a relocation pass. The pass keeps its device
  // permit while suspended so another worker cannot bypass device pacing.
  std::uint32_t defrag_sleep_ms_ = 0;
  // Asynchronous pacing between records within one block relocation. Zero
  // keeps the cooperative-yield-only behavior.
  std::uint32_t defrag_record_sleep_us_ = 0;
  // Queue candidates without starting relocation jobs. Runtime DEFRAG RESUME
  // releases the queued work.
  bool defrag_paused_ = false;
  RegisteredBufferPoolOptions buffers_{};
};

enum class DefragConfigAction : std::uint8_t {
  kPause,
  kResume,
  kMaxActivePerDevice,
  kBlockSleep,
  kRecordSleep,
};

struct DefragConfigUpdate {
  DefragConfigAction action_ = DefragConfigAction::kMaxActivePerDevice;
  std::uint64_t value_ = 0;
};

struct DefragTotals {
  bool paused_ = false;
  unsigned max_active_per_device_ = 0;
  std::uint32_t block_sleep_ms_ = 0;
  std::uint32_t record_sleep_us_ = 0;
  unsigned active_ = 0;
  unsigned pending_ = 0;
};

// Cold-path observability for tests and operators that need to know whether
// every write acknowledged so far has crossed its crash-durability boundary.
// Dirty bytes include queued and in-flight block flushes. Pending transaction
// commits cover the interval between acknowledging a tagged write and
// appending its commit decision.
struct StorageDurabilityStats {
  std::uint64_t dirty_staging_bytes_ = 0;
  unsigned flushes_pending_ = 0;
  std::uint64_t tx_commits_pending_ = 0;

  bool pending() const noexcept {
    return dirty_staging_bytes_ != 0 || flushes_pending_ != 0 ||
           tx_commits_pending_ != 0;
  }
};

enum class TombRaiderMode : std::uint8_t {
  kOff,
  kInterval,
  kDaily,
};

enum class TombRaiderConfigAction : std::uint8_t {
  kOff,
  kOn,
  kInterval,
  kBlockSleep,
  kDaily,
};

struct TombRaiderConfigUpdate {
  TombRaiderConfigAction action_ = TombRaiderConfigAction::kOff;
  std::uint64_t value_ = 0;
};

struct TombRaiderTotals {
  std::uint64_t rounds_ = 0;
  std::uint64_t reaped_ = 0;
  std::uint64_t refreshed_ = 0;
  std::uint64_t interval_ms_ = 0;
  std::uint32_t block_sleep_ms_ = 0;
  std::uint32_t daily_second_ = 0;
  TombRaiderMode mode_ = TombRaiderMode::kOff;
  bool enabled_ = false;
  bool running_ = false;
};

struct StorageDeviceMetrics {
  std::string path_;
  std::uint64_t device_id_ = 0;
  std::uint64_t capacity_bytes_ = 0;
  // Space available to foreground writes after preserving the defrag reserve.
  std::uint64_t available_bytes_ = 0;
  std::optional<std::uint64_t> filesystem_available_bytes_;
};

struct StorageMetricsSnapshot {
  std::vector<StorageDeviceMetrics> devices_;
};

struct ScanBatch {
  std::uint64_t cursor_ = 0;
  std::vector<std::string> keys_;
  std::vector<ValueType> value_types_;
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

  Kind kind_ = Kind::kValue;
  std::uint8_t db_id_ = 0;
  std::uint64_t db_epoch_ = 0;
  std::uint64_t mutation_sequence_ = 0;
  std::uint64_t expire_at_ms_ = 0;
  ValueType value_type_ = ValueType::kNone;
  std::uint64_t logical_size_ = 0;
  std::uint32_t chunk_index_ = 0;
  std::uint32_t chunk_count_ = 0;
  std::string key_;
  std::string value_;
};

struct PartitionReplicationStart {
  std::uint64_t snapshot_sequence_ = 0;
  std::uint16_t nonempty_db_mask_ = 0;
  std::array<std::uint64_t, 16> db_epochs_{};
};

struct PartitionSnapshotBatch {
  std::uint64_t cursor_ = 0;
  std::vector<SnapshotRecord> records_;
};

struct PartitionDeltaBatch {
  std::uint64_t watermark_ = 0;
  bool overflow_ = false;
  std::vector<SnapshotRecord> records_;
};

enum class ReplicationLogState : std::uint8_t {
  kDisabled,
  kActive,
  // The backlog has a gap or an I/O/allocation failure. Primary storage may
  // continue serving, but replicas must use a new full synchronization.
  kInvalid,
};

class ReplicationLogPayloadSource {
 public:
  virtual ~ReplicationLogPayloadSource() = default;
  virtual std::uint64_t size() const noexcept = 0;
  // Must fill output exactly or return an error. The source and any storage it
  // references remain alive until AppendReplicationLog completes.
  virtual celer::Task<absl::Status> Read(std::uint64_t offset,
                                         std::span<std::byte> output) = 0;
};

struct ReplicationLogAppend {
  ReplicationEventKind kind_ = ReplicationEventKind::kMutation;
  std::uint16_t partition_id_ = 0;
  std::uint64_t partition_sequence_ = 0;
  // Exactly one payload form is used. A source allows a future external-value
  // extent reader to stream directly into replication blocks; string_view is
  // the zero-extra-copy path for values already resident in the write call.
  std::string_view payload_;
  ReplicationLogPayloadSource* payload_source_ = nullptr;
};

struct ReplicationLogCursor {
  std::uint64_t lsn_ = 1;
  std::uint32_t fragment_index_ = 0;

  bool operator==(const ReplicationLogCursor&) const noexcept = default;
};

struct ReplicationLogFrame {
  ReplicationFrameHeader header_{};
  std::string payload_;
};

struct ReplicationLogBatch {
  ReplicationLogCursor next_{};
  std::vector<ReplicationLogFrame> frames_;
  bool at_tail_ = false;
};

struct ReplicationLogInfo {
  ReplicationLogState state_ = ReplicationLogState::kDisabled;
  std::uint64_t log_epoch_ = 0;
  std::uint64_t floor_lsn_ = 1;
  std::uint64_t tail_lsn_ = 0;
  std::size_t block_count_ = 0;
  std::size_t capacity_bytes_ = 0;
  std::size_t publish_queue_bytes_ = 0;
  std::size_t publish_queue_capacity_bytes_ = 0;
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
  SetCondition condition_ = SetCondition::kNone;
  // Absolute Unix time in milliseconds. Zero clears the TTL unless
  // keep_ttl is set.
  std::uint64_t expire_at_ms_ = 0;
  bool keep_ttl_ = false;
  bool return_old_value_ = false;
};

struct SetResult {
  bool applied_ = false;
  std::optional<DiskValue> old_value_;
};

// An owned command handed from command dispatch to the per-worker asynchronous
// replication publisher. Moving request arguments into this object avoids a
// second copy of large values.
struct ReplicationCommandAppend {
  ReplicationEventKind kind_ = ReplicationEventKind::kMutation;
  std::uint8_t db_id_ = 0;
  std::uint16_t partition_id_ = 0;
  std::uint64_t partition_sequence_ = 0;
  std::vector<std::string> args_;
};

enum class ListOperationKind : std::uint8_t {
  kPushLeft,
  kPushRight,
  kPushLeftIfExists,
  kPushRightIfExists,
  kPopLeft,
  kPopRight,
  kLength,
  kIndex,
  kRange,
  kSet,
  kInsertBefore,
  kInsertAfter,
  kRemove,
  kTrim,
  kPosition,
  kMoveWithin,
};

// One single-key List operation. Views remain owned by the command request for
// the lifetime of the awaited call.
struct ListOperation {
  ListOperationKind kind_ = ListOperationKind::kLength;
  std::vector<std::string_view> values_;
  std::string_view value_;
  std::string_view pivot_;
  std::int64_t first_ = 0;
  std::int64_t second_ = 0;
  std::uint64_t count_ = 0;
  std::int64_t rank_ = 1;
  std::uint64_t max_length_ = 0;
  bool count_provided_ = false;
  bool max_length_provided_ = false;
};

struct ListResult {
  bool key_exists_ = false;
  bool changed_ = false;
  std::uint64_t length_ = 0;
  std::int64_t integer_ = 0;
  std::vector<std::string> values_;
  std::vector<std::int64_t> positions_;
};

enum class HashOperationKind : std::uint8_t {
  kSet,
  kSetIfAbsent,
  kGet,
  kGetMany,
  kDelete,
  kLength,
  kExists,
  kGetAll,
  kKeys,
  kValues,
  kStringLength,
  kIncrementInteger,
  kIncrementFloat,
  kRandomFields,
  kScan,
  kPopRandom,
};

// Views remain owned by the command request for the lifetime of the awaited
// call. Set operations use parallel fields_/values_ arrays; all other
// operations use fields_ only.
struct HashOperation {
  HashOperationKind kind_ = HashOperationKind::kLength;
  std::vector<std::string_view> fields_;
  std::vector<std::string_view> values_;
  std::int64_t count_ = 0;
  std::uint64_t cursor_ = 0;
  std::uint64_t scan_count_ = 10;
  // Zero uses the current clock. Streamed commands pin one nonzero timestamp
  // across all batches so a TTL cannot change the announced RESP cardinality.
  std::uint64_t now_ms_ = 0;
  std::string_view match_ = "*";
  bool count_provided_ = false;
  bool with_values_ = false;
};

struct HashResult {
  bool key_exists_ = false;
  bool changed_ = false;
  std::uint64_t length_ = 0;
  std::uint64_t integer_ = 0;
  std::int64_t signed_integer_ = 0;
  std::uint64_t cursor_ = 0;
  std::string scalar_;
  // HGET/HMGET use nullopt for a missing field. HGETALL returns alternating
  // field/value entries; HKEYS and HVALS return one entry per element.
  std::vector<std::optional<std::string>> values_;
};

// A compact collection is persisted as one type-tagged value record. Sorted
// sets and Streams use this generic callback path. The callback runs while the
// key's exclusive/shared intent
// lock is held, making a decode/modify/encode cycle one atomic Redis command.
struct CompactValueView {
  std::string_view encoded_;
  std::uint64_t logical_size_ = 0;
  std::uint64_t expire_at_ms_ = 0;
};

struct CompactValueUpdate {
  bool changed_ = false;
  bool erase_ = false;
  // Reuses the callback's current encoded view for metadata-only rewrites.
  // Valid only for an existing non-erased value.
  bool reuse_encoded_ = false;
  std::string encoded_;
  std::uint64_t logical_size_ = 0;
  // nullopt preserves the current deadline (or persistence for a new key).
  std::optional<std::uint64_t> expire_at_ms_;
};

using CompactValueCallback = std::function<absl::StatusOr<CompactValueUpdate>(
    std::optional<CompactValueView>)>;

enum class ExpirationCondition : std::uint8_t {
  kNone,
  kIfNoExpiration,
  kIfHasExpiration,
  kIfGreater,
  kIfLess,
};

struct ExpirationInfo {
  bool exists_ = false;
  std::uint64_t expire_at_ms_ = 0;
  ValueType value_type_ = ValueType::kNone;
};

// Type-agnostic persisted representation used by keyspace operations such as
// RENAME. Collection payloads remain encoded and are never materialized into
// their command-layer element structures.
struct RawValue {
  std::string encoded_;
  std::uint64_t logical_size_ = 0;
  std::uint64_t expire_at_ms_ = 0;
  ValueType value_type_ = ValueType::kNone;
};

// Per-owning-shard accumulator for one multi-key atomic write. The
// coordinator owns one per shard; each shard writes only its own entry, so
// no synchronization is needed.
struct TxShardWrites {
  std::uint64_t txid_ = 0;  // input: stamped into every record written

  struct Fence {  // highest staged offset per destination block
    std::uint64_t block_id_ = 0;
    std::uint64_t allocation_epoch_ = 0;
    std::uint32_t committed_bytes_ = 0;
    std::uint16_t block_owner_ = 0;
  };
  struct Retired {  // superseded previous versions, released at commit
    std::uint64_t block_id_ = 0;
    std::uint64_t allocation_epoch_ = 0;
    std::uint32_t total_disk_bytes_ = 0;
    std::uint16_t block_owner_ = 0;
    std::uint32_t record_offset_ = 0;
    std::shared_ptr<const std::vector<ExtentRef>> dependent_extents_;
    // Value-only extents can be reclaimed as soon as the transaction commit
    // is durable. External-key extents stay dependent on the stale records
    // block because recovery may still need them to identify that record.
    std::shared_ptr<const std::vector<ExtentRef>> immediate_extents_;
  };
  std::vector<Fence> fences_;
  std::vector<Retired> retirements_;
  // Journal undo state for runtime rollback (standalone MSET / multi-key
  // DEL). EXEC leaves this off: its commands report errors individually and
  // never roll back (Redis semantics), while recovery still treats the
  // whole EXEC atomically through the commit record.
  bool collect_undo_ = false;
};

class StorageEngine {
 public:
  explicit StorageEngine(StorageEngineOptions options);
  StorageEngine(const StorageEngine&) = delete;
  StorageEngine& operator=(const StorageEngine&) = delete;
  ~StorageEngine();

  // Runs on the main thread before Server::Start. Creates/preallocates every
  // configured file and sizes per-worker metadata, but does not perform data
  // IO.
  absl::Status Prepare(unsigned worker_count);

  // Runs once on each worker before its listener is opened. Registers the
  // complete fixed-file table, opens every file with O_DIRECT into its fixed
  // slot, and performs parallel recovery.
  celer::Task<absl::Status> InitializeWorker(celer::Worker& worker);
  absl::Status FlushForShutdown();

  unsigned OwnerForKey(std::string_view key) const noexcept;
  unsigned worker_count() const noexcept;
  std::size_t LocalSize(std::uint8_t db_id) const noexcept;
  // Runs on one worker and returns a random live key owned by that worker.
  // Nullopt means this worker currently has no live key in the database.
  celer::Task<absl::StatusOr<std::optional<std::string>>> RandomKeyLocal(
      std::uint8_t db_id);
  // Must run on the worker owning partition_id. The cursor is stateless and
  // may return duplicate keys while the partition index is changing.
  // now_ms fixes the expiration filter timestamp (0 = current time), so a
  // multi-pass scan can see a stable notion of liveness.
  // max_bytes bounds the accumulated key bytes of one batch (overshoot is
  // at most one bucket chain, since the scan emits whole chains), so a
  // caller assembling bounded chunks stays bounded even with huge key
  // names.
  celer::Task<absl::StatusOr<ScanBatch>> ScanPartition(
      std::uint16_t partition_id, std::uint8_t db_id, std::uint64_t cursor,
      std::size_t count, std::uint64_t now_ms = 0,
      std::size_t max_bytes = SIZE_MAX);

  // Atomically invalidates one logical DB by advancing its durable epoch and
  // taking its indexes out of service. The command layer must prevent
  // concurrent operations in that DB while this coroutine runs, and may allow
  // them again as soon as it returns: the DB is observably empty from here on.
  // Cost is bounded by the partition count, not by the number of keys.
  celer::Task<absl::Status> FlushDbDetach(std::uint8_t db_id);

  // Retires what FlushDbDetach took out of service, subtracting it from the
  // block accounting and freeing it. Safe to run with the DB open and serving.
  // `wait` distinguishes FLUSHDB SYNC from FLUSHDB ASYNC: a reclaimer runs
  // either way, and only the caller's completion differs.
  celer::Task<absl::Status> FlushDbReclaim(bool wait);
  std::uint64_t DbEpoch(std::uint8_t db_id) const noexcept;
  // Broadcasts one DB-epoch control barrier to every active source-worker
  // flow. The caller keeps the DB gate closed until this completes.
  celer::Task<absl::Status> PublishFlushDbReplication(std::uint8_t db_id,
                                                      std::uint64_t db_epoch);
  // Replica-side application after the receiver has collected this barrier
  // from every source flow.
  celer::Task<absl::Status> ApplyReplicatedFlushDb(std::uint8_t db_id,
                                                   std::uint64_t db_epoch);

  // Source-side per-partition migration primitives. Begin captures
  // a sequence fence, Snapshot reads the baseline tree, and ReadDeltas returns
  // every mutation after that fence until acknowledged.
  PartitionReplicationStart BeginPartitionReplication(
      std::uint16_t partition_id);
  // Releases the full-sync delta fence for one source partition. Must execute
  // on the owning worker and is idempotent.
  void EndPartitionReplication(std::uint16_t partition_id);
  celer::Task<absl::StatusOr<PartitionSnapshotBatch>> SnapshotPartition(
      std::uint16_t partition_id, std::uint8_t db_id, std::uint64_t cursor,
      std::size_t count);
  PartitionDeltaBatch ReadPartitionDeltas(std::uint16_t partition_id,
                                          std::uint64_t after_sequence,
                                          std::size_t count);
  void AcknowledgePartitionDeltas(std::uint16_t partition_id,
                                  std::uint64_t through_sequence);
  bool TryTakeReplicationReady(std::uint16_t* partition_id);

  // Runtime-only source replication backlog for the current storage worker.
  // These calls must execute on that worker. The log is shared by every
  // downstream replica; each replica owns only a ReplicationLogCursor.
  celer::Task<absl::Status> EnableReplicationLog(std::uint64_t log_epoch,
                                                 std::size_t capacity_bytes);
  celer::Task<absl::StatusOr<std::uint64_t>> AppendReplicationLog(
      ReplicationLogAppend event);
  celer::Task<absl::StatusOr<ReplicationLogBatch>> ReadReplicationLog(
      ReplicationLogCursor next, std::size_t max_bytes, std::size_t max_frames);
  celer::Task<absl::Status> TrimReplicationLog(std::uint64_t keep_from_lsn);
  celer::Task<absl::Status> DisableReplicationLog();
  ReplicationLogInfo LocalReplicationLogInfo() const;
  bool ReplicationLogActive() const noexcept;
  bool TryEnqueueReplicationCommand(ReplicationCommandAppend command);

  // Replica-side primitives. Reset returns a new local replication epoch that
  // fences every record from an earlier copy of this partition.
  celer::Task<absl::StatusOr<std::uint64_t>> ResetReplicaPartition(
      std::uint16_t partition_id,
      std::span<const std::uint64_t, 16> source_db_epochs);
  celer::Task<absl::Status> ApplyReplicaRecords(
      std::uint16_t partition_id, std::uint64_t replication_epoch,
      std::span<const SnapshotRecord> records);

  // These operations must execute on OwnerForKey(key), normally through
  // SubmitTaskTo. Only digest/location metadata is retained after completion.
  celer::Task<absl::StatusOr<DiskValue>> Get(std::uint8_t db_id,
                                             std::string_view key,
                                             ReadLatencyTrace* trace = nullptr);
  celer::Task<absl::StatusOr<std::uint64_t>> StringLength(std::uint8_t db_id,
                                                          std::string_view key);
  celer::Task<absl::StatusOr<SetResult>> Set(
      std::uint8_t db_id, std::string_view key, std::string_view value,
      SetOptions options = {}, ReplicationCommandAppend* replication = nullptr);
  celer::Task<absl::StatusOr<std::uint64_t>> ListPush(
      std::uint8_t db_id, std::string_view key,
      std::span<const std::string_view> values);
  celer::Task<absl::StatusOr<ListResult>> ExecuteList(
      std::uint8_t db_id, std::string_view key, const ListOperation& operation);
  celer::Task<absl::StatusOr<HashResult>> ExecuteHash(
      std::uint8_t db_id, std::string_view key, const HashOperation& operation);
  celer::Task<absl::StatusOr<HashResult>> ExecuteSet(
      std::uint8_t db_id, std::string_view key, const HashOperation& operation);
  celer::Task<absl::Status> ExecuteCompact(std::uint8_t db_id,
                                           std::string_view key,
                                           ValueType value_type, bool read_only,
                                           const CompactValueCallback& callback,
                                           std::uint64_t now_ms = 0);
  celer::Task<ExpirationInfo> GetExpiration(std::uint8_t db_id,
                                            std::string_view key);
  celer::Task<absl::StatusOr<bool>> UpdateExpiration(
      std::uint8_t db_id, std::string_view key, std::uint64_t expire_at_ms,
      ExpirationCondition condition);
  celer::Task<absl::StatusOr<bool>> Delete(
      std::uint8_t db_id, std::string_view key,
      ReplicationCommandAppend* replication = nullptr);
  celer::Task<bool> Exists(std::uint8_t db_id, std::string_view key);

  // Pre-locked variants for the transaction layer. The caller must already
  // hold this worker's key lock for `digest` in the required mode (shared for
  // reads, exclusive for writes), must run on OwnerForKey(key), and `digest`
  // must equal ComputeDigest(key). Write variants take the worker's
  // store_state_mutex internally and release it before returning.
  //
  // Multi-key atomic writes pass a TxShardWrites per owning shard: its txid
  // tags every record written through it, and the shard accumulates the
  // durability fences and superseded-record retirements the commit needs.
  // After every shard succeeded, the coordinator calls CommitTxWrites: it
  // waits until each fence's data is durable, then appends the kTxCommit
  // record that makes the transaction survive recovery, and only then lets
  // the superseded records leave their blocks' accounting. Without a commit,
  // recovery drops every tagged record — all-or-nothing.
  celer::Task<absl::StatusOr<DiskValue>> GetLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      ReadLatencyTrace* trace = nullptr);
  celer::Task<absl::StatusOr<std::uint64_t>> StringLengthLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest);
  celer::Task<absl::StatusOr<SetResult>> SetLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      std::string_view value, SetOptions options = {},
      TxShardWrites* tx = nullptr,
      ReplicationCommandAppend* replication = nullptr);
  celer::Task<absl::StatusOr<std::uint64_t>> ListPushLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      std::span<const std::string_view> values, TxShardWrites* tx = nullptr);
  celer::Task<absl::StatusOr<ListResult>> ExecuteListLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      const ListOperation& operation, TxShardWrites* tx = nullptr);
  celer::Task<absl::StatusOr<HashResult>> ExecuteHashLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      const HashOperation& operation, TxShardWrites* tx = nullptr);
  celer::Task<absl::StatusOr<HashResult>> ExecuteSetLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      const HashOperation& operation, TxShardWrites* tx = nullptr);
  celer::Task<absl::Status> ExecuteCompactLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      ValueType value_type, bool read_only,
      const CompactValueCallback& callback, TxShardWrites* tx = nullptr,
      std::uint64_t now_ms = 0);
  celer::Task<ExpirationInfo> GetExpirationLocked(std::uint8_t db_id,
                                                  std::string_view key,
                                                  const Digest& digest);
  celer::Task<absl::StatusOr<RawValue>> ReadRawValueLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest);
  celer::Task<absl::Status> WriteRawValueLocked(std::uint8_t db_id,
                                                std::string_view key,
                                                const Digest& digest,
                                                const RawValue& value,
                                                TxShardWrites* tx = nullptr);
  celer::Task<absl::StatusOr<bool>> UpdateExpirationLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      std::uint64_t expire_at_ms, ExpirationCondition condition,
      TxShardWrites* tx = nullptr);
  celer::Task<absl::StatusOr<bool>> DeleteLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      TxShardWrites* tx = nullptr,
      ReplicationCommandAppend* replication = nullptr);
  celer::Task<bool> ExistsLocked(std::uint8_t db_id, std::string_view key,
                                 const Digest& digest);

  // Appends the commit record for a transaction whose shard writes all
  // succeeded. Runs on any worker; fences and retirements come from the
  // per-shard TxShardWrites. Safe to run in the background — the client
  // reply never waits for durability.
  celer::Task<absl::Status> CommitTxWrites(std::uint64_t txid,
                                           std::vector<TxShardWrites*> shards);

  // Allocates a transaction id for tagging a multi-key write. Never zero.
  static std::uint64_t AllocateWriteTxid() noexcept;

  // Bracket a detached commit chain: Started before spawning it (so a
  // graceful shutdown that already drained client requests still waits for
  // it), Finished when the chain ends whatever its outcome.
  void NoteTxCommitStarted() noexcept;
  void NoteTxCommitFinished() noexcept;

  // Undo every journaled write of the transaction on the calling shard:
  // overwritten keys get their previous location back (and their partitions
  // force a replica re-copy, since aborted values may already have shipped),
  // freshly created keys get a normal tombstone appended. Must run on the
  // owning shard with the transaction's key locks still held.
  // With `compensation`, restore every undo entry by appending a later record
  // carrying the same transaction id. This is required for command-local
  // rollback inside EXEC: its outer commit must make the restored state, not
  // an earlier failed half-write, win again during recovery.
  celer::Task<absl::Status> RollbackTxLocal(
      std::uint64_t txid, TxShardWrites* compensation = nullptr);
  // Drop the journal without acting on it (the transaction succeeded).
  celer::Task<absl::Status> DiscardTxUndoLocal(std::uint64_t txid);

  // Freeze/unfreeze expiration writes for stable-count scans (KEYS). The
  // caller must already exclude client writes (closed database gate).
  celer::Task<absl::Status> QuiesceExpiration();
  void ResumeExpiration() noexcept;
  std::uint32_t ExpirationPauseCount() const noexcept;

  // Lifetime totals of the tomb raider (rounds run, tombstone entries
  // reaped, stale shielding bits cleared).
  TombRaiderTotals TombRaiderStats() const noexcept;
  // Reconfigures the worker-0 scheduler. An in-flight round always finishes;
  // the new schedule starts counting from that completion.
  celer::Task<absl::Status> ConfigureTombRaider(TombRaiderConfigUpdate update);
  // Runtime relocation pacing. Reducing concurrency does not cancel active
  // passes; it prevents replacements until the active count reaches the new
  // limit. Sleep changes take effect at the next checkpoint.
  DefragTotals DefragStats() const noexcept;
  celer::Task<absl::Status> ConfigureDefrag(DefragConfigUpdate update);
  celer::Task<StorageDurabilityStats> DurabilityStats() const;
  celer::Task<StorageMetricsSnapshot> CollectMetrics() const;

  // Non-suspending index probe for WATCH: whether the key currently holds a
  // live (non-tombstone, unexpired) value. Must run on OwnerForKey(key).
  celer::Task<bool> KeyLive(std::uint8_t db_id, std::string_view key,
                            const Digest& digest);

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace keylane::storage
