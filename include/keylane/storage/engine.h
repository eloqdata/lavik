#pragma once

#include <array>
#include <atomic>
#include <cassert>
#include <coroutine>
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
#include "keylane/set_trace.h"
#include "keylane/storage/buffer_pool.h"
#include "keylane/storage/format.h"

namespace celer {
class Worker;
}  // namespace celer

namespace keylane::storage {

struct StorageEngineOptions {
  std::vector<std::string> data_files_{"keylane.data"};
  // Logically resets every configured device by erasing its fixed metadata
  // before assigning a fresh storage-set identity. Data blocks are left
  // physically intact but become unreachable.
  bool reset_data_files_ = false;
  std::uint32_t flush_max_ms_ = 1000;
  // Minimum delay between transaction-generation rotations/cleaning rounds.
  // Zero disables the cleaner; it can be changed at runtime through CONFIG.
  std::uint32_t tx_cleaner_cooldown_ms_ = 60'000;
  std::size_t flush_size_bytes_ = 128 * 1024;
  // Bounded, per-worker staging memory for commands waiting to enter the
  // shared in-memory replication backlog.
  std::size_t replication_publish_queue_bytes_ = 16ULL * 1024 * 1024;
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

struct TxCleanerTotals {
  std::uint64_t rounds_ = 0;
  std::uint64_t failures_ = 0;
  std::uint64_t retired_generations_ = 0;
  std::uint64_t retired_blocks_ = 0;
  std::uint32_t cooldown_ms_ = 0;
  bool running_ = false;
};

struct TxCommitBatchTotals {
  std::uint64_t batches_ = 0;
  std::uint64_t transactions_ = 0;
  std::uint64_t input_fences_ = 0;
  std::uint64_t merged_fences_ = 0;
  std::uint64_t queue_depth_ = 0;
  std::uint64_t queue_peak_ = 0;
  std::uint64_t backpressure_waits_ = 0;
  std::uint64_t queue_high_watermark_ = 0;
};

struct StorageDeviceMetrics {
  std::string path_;
  std::uint64_t device_id_ = 0;
  std::uint64_t capacity_bytes_ = 0;
  // Space available to foreground writes after preserving the defrag reserve.
  std::uint64_t available_bytes_ = 0;
  std::optional<std::uint64_t> filesystem_available_bytes_;
};

struct StorageReplicationLogMetrics {
  unsigned worker_id_ = 0;
  std::uint64_t floor_lsn_ = 1;
  std::uint64_t tail_lsn_ = 0;
  std::uint64_t backpressure_waits_ = 0;
  std::size_t chunk_count_ = 0;
  std::size_t capacity_bytes_ = 0;
  std::size_t publish_queue_bytes_ = 0;
  std::size_t publish_queue_capacity_bytes_ = 0;
  std::size_t fullsync_publish_queue_bytes_ = 0;
  std::size_t fullsync_publisher_admitted_bytes_ = 0;
  std::size_t fullsync_publish_queue_capacity_bytes_ = 0;
  std::size_t fullsync_session_count_ = 0;
  std::size_t pinned_cursors_ = 0;
  std::uint64_t fullsync_backpressure_waits_ = 0;
  bool active_ = false;
  bool capacity_backpressured_ = false;
};

struct StorageMetricsSnapshot {
  std::vector<StorageDeviceMetrics> devices_;
  std::vector<StorageReplicationLogMetrics> replication_logs_;
};

struct ScanBatch {
  std::uint64_t cursor_ = 0;
  std::vector<std::string> keys_;
  std::vector<ValueType> value_types_;
  std::vector<std::size_t> value_bytes_;
};

// Completes inline when a partition scan only touches the in-memory index.
// A nested Task is created only for the uncommon path that must materialize
// an out-of-index key from storage.
class ScanPartitionAwaitable {
 public:
  using Result = absl::StatusOr<ScanBatch>;
  using PendingTask = celer::Task<Result>;

  explicit ScanPartitionAwaitable(Result ready) : ready_(std::move(ready)) {}
  explicit ScanPartitionAwaitable(PendingTask pending)
      : pending_(std::move(pending)) {}

  ScanPartitionAwaitable(ScanPartitionAwaitable&&) noexcept = default;
  ScanPartitionAwaitable& operator=(ScanPartitionAwaitable&&) noexcept =
      default;
  ScanPartitionAwaitable(const ScanPartitionAwaitable&) = delete;
  ScanPartitionAwaitable& operator=(const ScanPartitionAwaitable&) = delete;

  bool await_ready() const noexcept { return ready_.has_value(); }

  std::coroutine_handle<> await_suspend(
      std::coroutine_handle<> awaiting) noexcept {
    assert(pending_.has_value());
    pending_awaiter_.emplace(std::move(*pending_).operator co_await());
    return pending_awaiter_->await_suspend(awaiting);
  }

  Result await_resume() {
    if (ready_.has_value()) return std::move(*ready_);
    assert(pending_awaiter_.has_value());
    return pending_awaiter_->await_resume();
  }

 private:
  std::optional<Result> ready_;
  std::optional<PendingTask> pending_;
  std::optional<typename PendingTask::Awaiter> pending_awaiter_;
};

struct SnapshotRecord {
  enum class Kind : std::uint8_t {
    kValue = 1,
    kDelete = 2,
    kValueBegin = 3,
    kValueChunk = 4,
    kValueCommit = 5,
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
  // Source-only handle for an immutable external value pinned during full
  // sync. It is never serialized; the sender reads it in transfer chunks.
  std::uint64_t source_id_ = 0;
  std::uint64_t source_value_bytes_ = 0;
  // Source-local snapshot metadata. This is deliberately not encoded on the
  // replication wire; carrying it through the send ACK avoids hashing every
  // baseline key a second time when coverage advances to TAILING.
  Digest key_digest_{};
  std::string key_;
  std::string value_;
};

// Full-sync values use the same 2 MiB transfer granularity as the ONLINE
// backlog reader. kMaxDataFrame remains only a protocol validation ceiling.
inline constexpr std::size_t kReplicationTransferBytes = 2ULL * 1024 * 1024;
inline constexpr std::size_t kFullSyncReplacementMetadataBytes = 320;

struct PartitionReplicationStart {
  std::uint64_t baseline_version_ = 0;
  std::uint16_t nonempty_db_mask_ = 0;
  std::array<std::uint64_t, 16> db_epochs_{};
};

struct FullSyncSessionStart {
  std::array<std::uint64_t, kLogicalDatabaseCount> db_epochs_{};
};

struct PartitionSnapshotBatch {
  std::uint64_t cursor_ = 0;
  std::vector<SnapshotRecord> records_;
};

struct PartitionFullSyncBatch {
  std::vector<SnapshotRecord> records_;
};

struct ReplicaPartitionReset {
  std::uint16_t partition_id_ = 0;
  std::array<std::uint64_t, kLogicalDatabaseCount> db_epochs_{};
};

struct ReplicaPartitionEpoch {
  std::uint16_t partition_id_ = 0;
  std::uint64_t replication_epoch_ = 0;
};

enum class ReplicationLogState : std::uint8_t {
  kDisabled,
  kActive,
  // The backlog has a gap or an encoding/allocation failure. Primary storage
  // may continue serving, but replicas must use a new full synchronization.
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
  std::size_t fullsync_publish_queue_bytes_ = 0;
  std::size_t fullsync_publisher_admitted_bytes_ = 0;
  std::size_t fullsync_publish_queue_capacity_bytes_ = 0;
  std::size_t fullsync_session_count_ = 0;
  std::size_t retained_cursor_count_ = 0;
  std::uint64_t backpressure_waits_ = 0;
  std::uint64_t fullsync_backpressure_waits_ = 0;
  bool capacity_backpressured_ = false;
};

// A value read directly into a registered storage buffer. network_bytes()
// contains a complete RESP bulk-string frame and remains valid until this
// move-only object is destroyed after the network send CQE.
class DiskValue {
 public:
  DiskValue() = default;
  DiskValue(ReadBufferLease lease, std::size_t network_offset,
            std::size_t network_size, std::size_t value_offset,
            std::size_t value_size) noexcept
      : lease_(std::move(lease)),
        network_offset_(network_offset),
        network_size_(network_size),
        value_offset_(value_offset),
        value_size_(value_size) {}

  DiskValue(const DiskValue&) = delete;
  DiskValue& operator=(const DiskValue&) = delete;
  DiskValue(DiskValue&&) noexcept = default;
  DiskValue& operator=(DiskValue&&) noexcept = default;

  std::span<const std::byte> network_bytes() const noexcept {
    auto bytes = lease_.bytes();
    return {bytes.data() + network_offset_, network_size_};
  }

  std::span<const std::byte> value_bytes() const noexcept {
    auto bytes = lease_.bytes();
    return {bytes.data() + value_offset_, value_size_};
  }

 private:
  ReadBufferLease lease_;
  std::size_t network_offset_ = 0;
  std::size_t network_size_ = 0;
  std::size_t value_offset_ = 0;
  std::size_t value_size_ = 0;
};

// One string lookup in a pre-locked, worker-local batch. BatchGetLocked keeps
// the storage pipeline inside one coroutine and fans ordinary disk reads into
// one completion barrier instead of spawning one coroutine per key.
struct BatchGetRequest {
  std::string_view key_;
  Digest digest_{};
};

using BatchGetValue = absl::StatusOr<std::optional<std::string>>;

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
// replication publisher. A large canonical argument may require one owned
// staging string; that string is subsequently moved into the queue.
struct ReplicationCommandAppend {
  ReplicationEventKind kind_ = ReplicationEventKind::kMutation;
  std::uint8_t db_id_ = 0;
  std::uint16_t partition_id_ = 0;
  std::uint64_t partition_sequence_ = 0;
  std::vector<std::string> args_;
};

struct FullSyncPublishItem {
  std::uint64_t id_ = 0;
  std::shared_ptr<const ReplicationCommandAppend> command_;
  std::optional<SnapshotRecord> record_;
};

struct FullSyncPublishQueueInfo {
  std::size_t queued_bytes_ = 0;
  std::size_t admitted_bytes_ = 0;
  std::size_t capacity_bytes_ = 0;
};

struct ReplicationPublisherAdmission {
  struct UnstartedGuard {
    std::uint64_t session_id_ = 0;
    std::uint16_t partition_id_ = 0;
    std::uint8_t db_id_ = 0;
  };

  std::uint64_t log_epoch_ = 0;
  std::vector<std::uint64_t> fullsync_session_ids_;
  std::vector<UnstartedGuard> fullsync_unstarted_guards_;
};

// Optional owner-local scope for publisher admission. A simple keyed command
// supplies its exact logical destination so full-sync sessions that have not
// started that (partition, DB) do not backpressure an unrelated write. Complex
// multi-key/control operations omit the scope and reserve conservatively.
struct ReplicationPublisherTarget {
  std::uint16_t partition_id_ = 0;
  std::uint8_t db_id_ = 0;
};

enum class ReplicationTransactionResolution : std::uint8_t {
  kPending,
  kPublish,
  kDiscard,
};

// Shared by the source workers participating in one cross-key command. Each
// worker queues the same immutable envelope while the command holds its shard
// locks; resolution decides whether that queued envelope is published after
// the command's atomic outcome is known. It is not a wire-level commit state.
struct ReplicationTransaction {
  std::uint64_t id_ = 0;
  std::uint8_t db_id_ = 0;
  std::vector<unsigned> participants_;
  std::vector<std::string> envelope_args_;
  std::atomic<ReplicationTransactionResolution> resolution_{
      ReplicationTransactionResolution::kPending};
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

// One Redis-visible value materialized from an online point-in-time snapshot.
// Batches are deliberately value-oriented so the RDB encoder can run outside
// the storage worker and no physical block remains pinned while filesystem IO
// is pending.
struct RdbSnapshotValue {
  std::uint8_t db_id_ = 0;
  std::string key_;
  RawValue value_;
};

struct RdbSnapshotCursor {
  std::size_t partition_index_ = 0;
  std::uint8_t db_id_ = 0;
  std::uint64_t index_cursor_ = 0;
  std::uint64_t dirty_cursor_ = 0;
  bool finalizing_ = false;
};

struct RdbSnapshotBatch {
  RdbSnapshotCursor cursor_;
  std::vector<RdbSnapshotValue> values_;
  bool done_ = false;
};

struct RestoreRawResult {
  bool busy_ = false;
  bool changed_ = false;
  bool deleted_ = false;
};

// Per-owning-shard accumulator for one multi-key atomic write. The
// coordinator owns one per shard; each shard writes only its own entry, so
// no synchronization is needed.
struct TxShardWrites {
  std::uint64_t txid_ = 0;  // input: stamped into every record written
  // All shards of one transaction share the same generation and lease. The
  // opaque lease keeps that generation open until the last shard receipt is
  // destroyed after commit or rollback processing.
  std::uint64_t generation_ = 0;
  std::shared_ptr<void> generation_lease_;
  struct FullSyncEffect {
    std::uint16_t partition_id_ = 0;
    SnapshotRecord record_;
    // Sessions for which this participant was linearized before their
    // PARTITION_HANDOFF marker. The commit hook targets this frozen set even
    // if the session phase changes before the global transaction decision.
    std::vector<std::uint64_t> session_ids_;
  };

  struct ExpirationEffect {
    std::string key_;
    std::uint64_t expire_at_ms_ = 0;
    std::uint8_t db_id_ = 0;
    bool exists_ = false;
  };

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
    bool tx_tagged_ = false;
    bool dependency_pinned_ = false;
    std::shared_ptr<const std::vector<ExtentRef>> dependent_extents_;
    // Value-only extents can be reclaimed as soon as the transaction commit
    // is durable. External-key extents stay dependent on the stale records
    // block because recovery may still need them to identify that record.
    std::shared_ptr<const std::vector<ExtentRef>> immediate_extents_;
  };
  std::vector<Fence> fences_;
  std::vector<Retired> retirements_;
  // Final metadata is collected while the key lock is held. Replication
  // collapses repeated writes to the same key before publishing the command.
  std::vector<ExpirationEffect> expiration_effects_;
  // Collected under participant key locks, but invisible to full-sync
  // sessions until the coordinator has made the global commit decision.
  std::vector<FullSyncEffect> fullsync_effects_;
  // Successful staged logical mutations. The owner writes this local receipt;
  // CommitTxWrites publishes the sum to its coordinator worker only after the
  // transaction commit record succeeds.
  std::uint64_t dataset_changes_ = 0;
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
  // Runs on the worker's native thread after its IO and coroutine frames have
  // been torn down. Releases all state owned by that worker, including every
  // worker-local ScanHashMap.
  void FinalizeWorker(celer::Worker& worker) noexcept;
  absl::Status FlushForShutdown();

  unsigned OwnerForKey(std::string_view key) const noexcept;
  unsigned worker_count() const noexcept;
  std::size_t LocalSize(std::uint8_t db_id) const noexcept;

  // The command layer closes and drains every DB gate before invoking Begin
  // on all workers. That short cut cannot split a transaction. Scanning then
  // proceeds online: the first post-cut mutation of a not-yet-covered key
  // pins its old physical record in a partition-local ScanHashMap.
  absl::Status BeginRdbSnapshot(std::uint64_t session_id,
                                std::uint64_t snapshot_time_ms);
  celer::Task<absl::StatusOr<RdbSnapshotBatch>> ReadRdbSnapshotBatch(
      std::uint64_t session_id, RdbSnapshotCursor cursor, std::size_t count,
      std::size_t max_bytes);
  celer::Task<absl::Status> EndRdbSnapshot(std::uint64_t session_id);
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
  ScanPartitionAwaitable ScanPartition(std::uint16_t partition_id,
                                       std::uint8_t db_id, std::uint64_t cursor,
                                       std::size_t count,
                                       std::uint64_t now_ms = 0,
                                       std::size_t max_bytes = SIZE_MAX);

  // Atomically invalidates one logical DB by advancing its durable epoch and
  // taking its indexes out of service. The command layer must prevent
  // concurrent operations in that DB while this coroutine runs, and may allow
  // them again as soon as it returns: the DB is observably empty from here on.
  // Cost is bounded by the partition count, not by the number of keys.
  celer::Task<absl::Status> FlushDbDetach(std::uint8_t db_id);
  // Advances all 16 DB epochs in one metadata-page update and detaches every
  // database under one per-worker store critical section. The caller holds
  // all command DB gates.
  celer::Task<absl::Status> FlushAllDetach();

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
  // Broadcasts one control barrier carrying the complete database-epoch
  // vector. It is one logical event on every source flow, not sixteen
  // independent FLUSHDB barriers.
  celer::Task<absl::Status> PublishFlushAllReplication(
      const std::array<std::uint64_t, kLogicalDatabaseCount>& db_epochs);
  // Replica-side application after the receiver has collected this barrier
  // from every source flow.
  celer::Task<absl::Status> ApplyReplicatedFlushDb(std::uint8_t db_id,
                                                   std::uint64_t db_epoch);
  celer::Task<absl::Status> ApplyReplicatedFlushAll(
      const std::array<std::uint64_t, kLogicalDatabaseCount>& db_epochs);

  // Source-side full-sync lifetime. Begin fixes this worker's initial database
  // epoch vector before any partition scan. Any later DB epoch advance
  // invalidates the whole session; the first implementation restarts full
  // sync instead of trying to apply FLUSHDB inside a partially built root.
  absl::StatusOr<FullSyncSessionStart> BeginFullSyncSession(
      std::uint64_t session_id);
  bool FullSyncSessionValid(std::uint64_t session_id) const noexcept;
  void EndFullSyncSession(std::uint64_t session_id);

  // Source-side full-sync primitives. A partition scans one DB at a time;
  // only that DB owns a temporary ScanHashMap<KeyPhase>. Writes to an unseen
  // key coalesce a metadata-only replacement, while covered/completed keys
  // enter the session/worker publish FIFO.
  absl::StatusOr<PartitionReplicationStart> BeginPartitionReplication(
      std::uint64_t session_id, std::uint16_t partition_id);
  absl::Status BeginPartitionDbReplication(std::uint64_t session_id,
                                           std::uint16_t partition_id,
                                           std::uint8_t db_id);
  // Releases one session's full-sync subscriber. Must execute on the owning
  // worker and is idempotent.
  void EndPartitionReplication(std::uint64_t session_id,
                               std::uint16_t partition_id);
  celer::Task<absl::StatusOr<PartitionSnapshotBatch>> SnapshotPartition(
      std::uint64_t session_id, std::uint16_t partition_id, std::uint8_t db_id,
      std::uint64_t cursor, std::size_t count, std::size_t read_concurrency = 1,
      std::size_t max_bytes = kReplicationTransferBytes);
  celer::Task<absl::StatusOr<PartitionFullSyncBatch>>
  ReadPartitionFullSyncOverrides(
      std::uint64_t session_id, std::uint16_t partition_id, std::size_t count,
      std::size_t max_bytes = kReplicationTransferBytes);
  celer::Task<absl::StatusOr<SnapshotRecord>> MaterializeFullSyncPublishRecord(
      std::uint64_t session_id, std::uint16_t partition_id,
      const SnapshotRecord& requested);
  celer::Task<absl::StatusOr<std::string>> ReadFullSyncValueChunk(
      std::uint64_t session_id, std::uint16_t partition_id,
      std::uint64_t source_id, std::uint64_t offset, std::size_t max_bytes);
  void ReleaseFullSyncValue(std::uint64_t session_id,
                            std::uint16_t partition_id,
                            std::uint64_t source_id);
  void AcknowledgePartitionFullSyncOverrides(
      std::uint64_t session_id, std::uint16_t partition_id,
      std::span<const SnapshotRecord> records);
  void AcknowledgePartitionSnapshotRecords(
      std::uint64_t session_id, std::uint16_t partition_id,
      std::span<const SnapshotRecord> records);
  absl::Status CompletePartitionReplication(std::uint64_t session_id,
                                            std::uint16_t partition_id);
  absl::Status CompletePartitionDbReplication(std::uint64_t session_id,
                                              std::uint16_t partition_id,
                                              std::uint8_t db_id);
  absl::StatusOr<std::vector<FullSyncPublishItem>> PeekFullSyncPublishItems(
      std::uint64_t session_id, std::size_t max_items);
  absl::StatusOr<FullSyncPublishQueueInfo> GetFullSyncPublishQueueInfo(
      std::uint64_t session_id) const;
  void AcknowledgeFullSyncPublishItem(std::uint64_t session_id,
                                      std::uint64_t item_id);
  // Runtime-only source replication backlog for the current storage worker.
  // These calls must execute on that worker. The log is shared by every
  // downstream replica; each replica owns only a ReplicationLogCursor.
  celer::Task<absl::Status> EnableReplicationLog(std::uint64_t log_epoch,
                                                 std::size_t capacity_bytes);
  // Updates this worker flow's lazy block quota. Shrinkage drops complete
  // oldest events until the retained block count fits, except that live
  // consumer cursors stay pinned and make the smaller value a target quota;
  // growth allocates nothing until a later append needs another block.
  celer::Task<absl::Status> SetReplicationLogCapacity(
      std::size_t capacity_bytes);
  // Changes the worker-local in-memory publisher admission waterline. A
  // shrink never drops queued commands; new admissions wait for occupancy to
  // fall below the new limit. A growth wakes waiters immediately.
  celer::Task<absl::Status> SetReplicationPublishQueueCapacity(
      std::size_t capacity_bytes);
  celer::Task<absl::StatusOr<std::uint64_t>> AppendReplicationLog(
      ReplicationLogAppend event);
  // Inserts an ordered publisher fence and returns the first LSN assigned
  // after it. All commands enqueued before the fence have reached the log;
  // commands enqueued afterwards receive an LSN at or above the result.
  celer::Task<absl::StatusOr<std::uint64_t>> FenceReplicationLog();
  celer::Task<absl::StatusOr<ReplicationLogBatch>> ReadReplicationLog(
      ReplicationLogCursor next, std::size_t max_bytes, std::size_t max_frames);
  // Pins history needed by one ONLINE/downstream session. The cursor is the
  // first LSN not yet acknowledged by that session. Capacity pressure waits
  // for the slowest retained cursor instead of evicting required history.
  // Both calls are worker-local and never perform IO.
  absl::Status RetainReplicationLog(std::uint64_t session_id,
                                    std::uint64_t keep_from_lsn);
  void ReleaseReplicationLogRetention(std::uint64_t session_id);
  celer::Task<absl::Status> TrimReplicationLog(std::uint64_t keep_from_lsn);
  celer::Task<absl::Status> DisableReplicationLog();
  ReplicationLogInfo LocalReplicationLogInfo() const;
  bool ReplicationLogActive() const noexcept;
  // Worker-local high-water admission acquired before a source write enters
  // command/transaction gates. It reserves the shared backlog publisher and
  // every active full-sync session FIFO. A request larger than the normal
  // limit is admitted only when it can be the sole staged item.
  celer::Task<absl::StatusOr<ReplicationPublisherAdmission>>
  AcquireReplicationPublisherAdmission(
      std::size_t logical_bytes,
      std::optional<ReplicationPublisherTarget> target = std::nullopt);
  void ReleaseReplicationPublisherAdmission(
      const ReplicationPublisherAdmission& admission,
      std::size_t logical_bytes);
  bool TryEnqueueReplicationCommand(ReplicationCommandAppend command);
  // Publishes one runtime-only command on the worker owning partition_id. It
  // is appended to that source flow's online backlog and every active
  // full-sync FIFO, but never becomes persistent keyspace state.
  celer::Task<absl::Status> PublishEphemeralReplicationCommand(
      std::uint16_t partition_id, std::vector<std::string> args);
  bool TryEnqueueReplicationTransaction(
      std::shared_ptr<ReplicationTransaction> transaction);

  // Replica-side primitives. Reset returns a new local replication epoch that
  // fences every record from an earlier copy of this partition.
  celer::Task<absl::StatusOr<std::uint64_t>> ResetReplicaPartition(
      std::uint16_t partition_id,
      std::span<const std::uint64_t, 16> source_db_epochs);
  // Resets partitions owned by the current worker. Epoch metadata pages are
  // coalesced and persisted once for the whole batch before any new-epoch
  // replica records can be applied.
  celer::Task<absl::StatusOr<std::vector<ReplicaPartitionEpoch>>>
  ResetReplicaPartitions(std::uint64_t session_id,
                         std::span<const ReplicaPartitionReset> resets);
  // Logically empties only the selected Redis hash slots. Each partition gets
  // a new durable replication epoch and its indexes are detached in O(slots)
  // time; records sharing physical blocks with other slots remain untouched.
  // The caller must exclude command execution while this runs.
  celer::Task<absl::Status> ResetPartitionsDetach(
      std::span<const std::uint16_t> partition_ids);
  celer::Task<absl::Status> HandoffReplicaPartition(
      std::uint64_t session_id, std::uint16_t partition_id,
      std::uint64_t replication_epoch);
  celer::Task<absl::Status> BeginReplicaTailCommand(
      std::uint64_t session_id, std::uint16_t partition_id,
      std::uint64_t partition_sequence);
  celer::Task<absl::Status> EndReplicaTailCommand(
      std::uint64_t session_id, std::uint16_t partition_id,
      std::uint64_t partition_sequence);
  celer::Task<absl::Status> ApplyReplicaRecords(
      std::uint64_t session_id, std::uint16_t partition_id,
      std::uint64_t replication_epoch, std::span<const SnapshotRecord> records);
  // Publishes a completed in-place rebuild at the final cut. Abort drains
  // staged writes before detaching and reclaiming the partial population.
  celer::Task<absl::Status> PromoteReplicaRoot(std::uint64_t session_id);
  celer::Task<absl::Status> AbortReplicaRoot(std::uint64_t session_id);
  void SetReplicaLoading(bool loading) noexcept;

  // These operations must execute on OwnerForKey(key), normally through
  // SubmitTaskTo. Only digest/location metadata is retained after completion.
  celer::Task<absl::StatusOr<DiskValue>> Get(std::uint8_t db_id,
                                             std::string_view key,
                                             ReadLatencyTrace* trace = nullptr);
  celer::Task<absl::StatusOr<std::uint64_t>> StringLength(std::uint8_t db_id,
                                                          std::string_view key);
  celer::Task<absl::StatusOr<SetResult>> Set(
      std::uint8_t db_id, std::string_view key, std::string_view value,
      SetOptions options = {}, ReplicationCommandAppend* replication = nullptr,
      SetLatencyTrace* trace = nullptr);
  celer::Task<absl::StatusOr<std::uint64_t>> ListPush(
      std::uint8_t db_id, std::string_view key,
      std::span<const std::string_view> values,
      ReplicationCommandAppend* replication = nullptr);
  celer::Task<absl::StatusOr<ListResult>> ExecuteList(
      std::uint8_t db_id, std::string_view key, const ListOperation& operation,
      ReplicationCommandAppend* replication = nullptr);
  celer::Task<absl::StatusOr<HashResult>> ExecuteHash(
      std::uint8_t db_id, std::string_view key, const HashOperation& operation,
      ReplicationCommandAppend* replication = nullptr);
  celer::Task<absl::StatusOr<HashResult>> ExecuteSet(
      std::uint8_t db_id, std::string_view key, const HashOperation& operation,
      ReplicationCommandAppend* replication = nullptr);
  celer::Task<absl::Status> ExecuteCompact(
      std::uint8_t db_id, std::string_view key, ValueType value_type,
      bool read_only, const CompactValueCallback& callback,
      std::uint64_t now_ms = 0,
      ReplicationCommandAppend* replication = nullptr);
  celer::Task<ExpirationInfo> GetExpiration(std::uint8_t db_id,
                                            std::string_view key);
  celer::Task<absl::StatusOr<bool>> UpdateExpiration(
      std::uint8_t db_id, std::string_view key, std::uint64_t expire_at_ms,
      ExpirationCondition condition,
      ReplicationCommandAppend* replication = nullptr);
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
  celer::Task<std::vector<BatchGetValue>> BatchGetLocked(
      std::uint8_t db_id, std::span<const BatchGetRequest> requests);
  celer::Task<absl::StatusOr<std::uint64_t>> StringLengthLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest);
  celer::Task<absl::StatusOr<SetResult>> SetLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      std::string_view value, SetOptions options = {},
      TxShardWrites* tx = nullptr,
      ReplicationCommandAppend* replication = nullptr,
      SetLatencyTrace* trace = nullptr);
  celer::Task<absl::StatusOr<std::uint64_t>> ListPushLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      std::span<const std::string_view> values, TxShardWrites* tx = nullptr,
      ReplicationCommandAppend* replication = nullptr);
  celer::Task<absl::StatusOr<ListResult>> ExecuteListLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      const ListOperation& operation, TxShardWrites* tx = nullptr,
      ReplicationCommandAppend* replication = nullptr);
  celer::Task<absl::StatusOr<HashResult>> ExecuteHashLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      const HashOperation& operation, TxShardWrites* tx = nullptr,
      ReplicationCommandAppend* replication = nullptr);
  celer::Task<absl::StatusOr<HashResult>> ExecuteSetLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      const HashOperation& operation, TxShardWrites* tx = nullptr,
      ReplicationCommandAppend* replication = nullptr);
  celer::Task<absl::Status> ExecuteCompactLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      ValueType value_type, bool read_only,
      const CompactValueCallback& callback, TxShardWrites* tx = nullptr,
      std::uint64_t now_ms = 0,
      ReplicationCommandAppend* replication = nullptr);
  celer::Task<ExpirationInfo> GetExpirationLocked(std::uint8_t db_id,
                                                  std::string_view key,
                                                  const Digest& digest);
  celer::Task<absl::StatusOr<RawValue>> ReadRawValueLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest);
  celer::Task<absl::StatusOr<RawValue>> ReadRawValue(std::uint8_t db_id,
                                                     std::string_view key);
  celer::Task<absl::StatusOr<RestoreRawResult>> RestoreRawValue(
      std::uint8_t db_id, std::string_view key, const RawValue& value,
      bool replace, ReplicationCommandAppend* replication = nullptr);
  celer::Task<absl::StatusOr<RestoreRawResult>> RestoreRawValueLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      const RawValue& value, bool replace, TxShardWrites* tx = nullptr,
      ReplicationCommandAppend* replication = nullptr);
  celer::Task<absl::Status> WriteRawValueLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      const RawValue& value, TxShardWrites* tx = nullptr,
      ReplicationCommandAppend* replication = nullptr);
  celer::Task<absl::StatusOr<bool>> UpdateExpirationLocked(
      std::uint8_t db_id, std::string_view key, const Digest& digest,
      std::uint64_t expire_at_ms, ExpirationCondition condition,
      TxShardWrites* tx = nullptr,
      ReplicationCommandAppend* replication = nullptr);
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
  // Transfers one successful transaction's receipts to the current worker's
  // commit coordinator. At most one coordinator coroutine runs per worker;
  // it merges durability fences and appends commit decisions in batches.
  // Returns true when the caller may reply immediately. A false result means
  // the worker-local queue crossed its high watermark; the command should
  // await WaitForTxCommitCapacity() before replying. The idle fast path does
  // not create or await another coroutine.
  [[nodiscard]] bool EnqueueTxCommit(std::uint64_t txid,
                                     std::vector<TxShardWrites> writes);
  celer::Task<absl::Status> WaitForTxCommitCapacity();
  // Must run on the owning worker before participant locks are released.
  void PublishCommittedFullSyncEffects(TxShardWrites* shard);

  // Allocates a transaction id for tagging a multi-key write. Never zero.
  static std::uint64_t AllocateWriteTxid() noexcept;

  // Binds every shard receipt of one storage transaction to the current
  // transaction generation and holds one shared generation lease.
  void InitializeTxWrites(std::uint64_t txid, std::span<TxShardWrites> writes);

  // Bracket a queued commit: graceful shutdown waits for every accepted
  // transaction to leave the worker-local coordinator.
  void NoteTxCommitStarted() noexcept;
  void NoteTxCommitFinished() noexcept;

  TxCommitBatchTotals TxCommitBatchStats() const noexcept;

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
  void SetExpirationAuthority(bool authority) noexcept;
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
  TxCleanerTotals TxCleanerStats() const noexcept;
  std::uint32_t TxCleanerCooldownMs() const noexcept;
  absl::Status ConfigureTxCleanerCooldown(std::uint64_t cooldown_ms);
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
