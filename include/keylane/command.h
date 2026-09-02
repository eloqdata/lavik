#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/statusor.h"
#include "celer/runtime/task.h"
#include "keylane/read_trace.h"
#include "keylane/resp_version.h"
#include "keylane/set_trace.h"
#include "keylane/storage/engine.h"

namespace keylane {

class BlockingWakeCascade;

namespace tx {
class Transaction;
}

struct RespCommand;
struct ReplicatedCommand;
class ReplyBuilder;
class ReplicationManager;

using celer::Task;

struct CapturedReplicationCommand {
  std::uint8_t db_id_ = 0;
  std::vector<std::string> args_;
};

struct CapturedReplicationEffects {
  bool handled_ = false;
  std::vector<CapturedReplicationCommand> commands_;
};

// EXEC handlers record outcome-dependent commands here (absolute deadlines,
// selected random members, generated stream IDs, consumer-group after-images).
// A command normally writes from one worker, while XREADGROUP may visit
// several owners, so recording is safe from any participant worker.
class ReplicationCommandCapture {
 public:
  void MarkHandled();
  void Record(std::uint8_t db_id, std::vector<std::string> args);
  CapturedReplicationEffects Take();

 private:
  mutable std::mutex mutex_;
  bool handled_ = false;
  std::vector<CapturedReplicationCommand> commands_;
};

struct CapturedBlockingNotification {
  std::uint8_t db_id_ = 0;
  std::string key_;
  storage::ValueType value_type_ = storage::ValueType::kNone;
};

// EXEC handlers can run on several workers and must not expose readiness from
// an intermediate command. They record candidate keys here; the coordinator
// filters them by the transaction's final value type before waking waiters.
class BlockingNotificationCapture {
 public:
  explicit BlockingNotificationCapture(unsigned worker_count);

  void Record(std::uint8_t db_id, std::string key,
              storage::ValueType value_type);
  std::vector<CapturedBlockingNotification> Take();

 private:
  // Each runtime worker only appends to its own slot. EXEC consumes the slots
  // after all shard callbacks have joined, so this needs no cross-worker lock.
  std::vector<std::vector<CapturedBlockingNotification>> per_worker_;
};

enum class CommandKind {
  kPing,
  kEcho,
  kPublish,
  kPubSub,
  kPSubscribe,
  kPUnsubscribe,
  kSubscribe,
  kUnsubscribe,
  kQuit,
  kReset,
  kAuth,
  kHello,
  kDbSize,
  kDel,
  kUnlink,
  kRename,
  kRenameNx,
  kCopy,
  kExists,
  kTouch,
  kRandomKey,
  kFlushDb,
  kFlushAll,
  kGet,
  kGetDel,
  kGetEx,
  kGetRange,
  kGetSet,
  kAppend,
  kGetBit,
  kSetBit,
  kBitCount,
  kBitPos,
  kBitField,
  kBitFieldRo,
  kBitOp,
  kStrlen,
  kIncr,
  kIncrBy,
  kIncrByFloat,
  kDecr,
  kDecrBy,
  kSetEx,
  kPSetEx,
  kSetNx,
  kSetRange,
  kSubstr,
  kLcs,
  kExpire,
  kPExpire,
  kExpireAt,
  kPExpireAt,
  kPersist,
  kTtl,
  kPttl,
  kExpireTime,
  kPExpireTime,
  kScan,
  kType,
  kDump,
  kRestore,
  kSort,
  kSortRo,
  kSelect,
  kEval,
  kEvalSha,
  kEvalRo,
  kEvalShaRo,
  kScript,
  kFCall,
  kFCallRo,
  kFunction,
  kSet,
  kLPush,
  kLPushX,
  kRPush,
  kRPushX,
  kLPop,
  kRPop,
  kLLen,
  kLIndex,
  kLRange,
  kLSet,
  kLInsert,
  kLRem,
  kLTrim,
  kLPos,
  kLMove,
  kRPopLPush,
  kLMPop,
  kBLPop,
  kBRPop,
  kBLMove,
  kBRPopLPush,
  kBLMPop,
  kHSet,
  kHMSet,
  kHSetNx,
  kHGet,
  kHMGet,
  kHDel,
  kHLen,
  kHExists,
  kHGetAll,
  kHKeys,
  kHVals,
  kHStrlen,
  kHIncrBy,
  kHIncrByFloat,
  kHRandField,
  kHScan,
  kSAdd,
  kSCard,
  kSDiff,
  kSDiffStore,
  kSInter,
  kSInterCard,
  kSInterStore,
  kSIsMember,
  kSMembers,
  kSMIsMember,
  kSMove,
  kSPop,
  kSRandMember,
  kSRem,
  kSScan,
  kSUnion,
  kSUnionStore,
  kBZMPop,
  kBZPopMax,
  kBZPopMin,
  kZAdd,
  kZCard,
  kZCount,
  kZIncrBy,
  kZLexCount,
  kZMPop,
  kZMScore,
  kZPopMax,
  kZPopMin,
  kZRandMember,
  kZRange,
  kZRangeStore,
  kZRangeByLex,
  kZRangeByScore,
  kZRank,
  kZRem,
  kZRemRangeByLex,
  kZRemRangeByRank,
  kZRemRangeByScore,
  kZRevRange,
  kZRevRangeByLex,
  kZRevRangeByScore,
  kZRevRank,
  kZScan,
  kZScore,
  kZDiff,
  kZDiffStore,
  kZInter,
  kZInterCard,
  kZInterStore,
  kZUnion,
  kZUnionStore,
  kGeoAdd,
  kGeoDist,
  kGeoHash,
  kGeoPos,
  kGeoRadius,
  kGeoRadiusRo,
  kGeoRadiusByMember,
  kGeoRadiusByMemberRo,
  kGeoSearch,
  kGeoSearchStore,
  kXAdd,
  kXDel,
  kXLen,
  kXRange,
  kXRevRange,
  kXTrim,
  kXSetId,
  kXGroup,
  kXAck,
  kXPending,
  kXClaim,
  kXAutoClaim,
  kXInfo,
  kXRead,
  kXReadGroup,
  kMSet,
  kMSetNx,
  kMGet,
  kMulti,
  kExec,
  kDiscard,
  kWatch,
  kUnwatch,
  kClient,
  kReplicaOf,
  kAddReplicaOf,
  kConfig,
  kInfo,
  kRole,
  kWait,
  kCluster,
  kCommand,
  kReadOnly,
  kReadWrite,
  kKeys,
  kSave,
  kBgSave,
  kLastSave,
  kMonitor,
  kSlowLog,
  kTombRaider,
  kDefrag,
  kUnknown,
  kCount,
};

struct CommandSpec;

struct CommandRequest {
  CommandKind kind_ = CommandKind::kUnknown;
  std::uint8_t db_id_ = 0;
  // Reply protocol for every nested/cross-core execution path. Replication
  // and internal callers naturally default to RESP2 because their replies are
  // discarded; client dispatch overwrites this from the connection.
  RespVersion resp_version_ = RespVersion::k2;
  // Set only for commands applied from the replication stream. Such commands
  // bypass replica read-only checks and must not be published again.
  bool replication_origin_ = false;
  // Captured when a client write chooses its source-publication path. A DB
  // gate that reopens under a different role must reject the stale request
  // before mutation, including when writable replicas are enabled.
  std::optional<std::uint64_t> write_admission_role_epoch_;
  const CommandSpec* spec_ = nullptr;
  // Populated by source-write dispatch after DetermineKeys proves there is
  // exactly one key. The request remains alive across its cross-core handoff,
  // so publisher admission, command dispatch, and storage lookup can all reuse
  // the Redis slot. Consumers must also match the argument index; zero means
  // there is no reusable key route.
  std::optional<std::uint16_t> routed_partition_id_;
  std::size_t routed_key_argument_ = 0;
  std::vector<std::string> args_;
  std::shared_ptr<ReplicationCommandCapture> replication_capture_;
  std::shared_ptr<BlockingNotificationCapture> blocking_notification_capture_;
  // Non-owning: the dispatch coroutine keeps the cascade alive until every
  // waiter transitively woken by this command has finished its ready attempt.
  BlockingWakeCascade* blocking_wake_cascade_ = nullptr;
};

struct ReplicaOfRequest {
  // Empty for REPLICAOF NO ONE; otherwise identifies the requested upstream.
  std::optional<std::string> host_;
  std::uint16_t port_ = 0;
};

// Parses REPLICAOF <host> <port> and REPLICAOF NO ONE without changing role.
// Command dispatch passes the validated request to ReplicationManager.
absl::StatusOr<ReplicaOfRequest> ParseReplicaOfRequest(
    std::span<const std::string> args);

// Pulls the next chunk of a streamed reply; an empty chunk ends the stream.
// Lets unbounded replies (KEYS) reach the socket in bounded memory.
using ReplyChunkSource = std::function<Task<absl::StatusOr<std::string>>()>;

struct CommandReply {
  // Points into the connection's ReplyBuilder and remains valid until the
  // current socket write completes. DiskValue keeps the specialized
  // direct-from-read-buffer GET path.
  // TODO: Add TcpStream::WriteVAll so composite replies can send independently
  // produced fragments without flattening them into ReplyBuilder.
  std::string_view encoded_;
  std::optional<storage::DiskValue> disk_value_;
  ReplyChunkSource chunks_;  // drained after `encoded` when set
  bool close_connection_ = false;
  bool start_monitoring_ = false;
  ReadLatencyTrace read_trace_;
  SetLatencyTrace set_trace_;
  std::optional<std::uint8_t> selected_db_;
};

absl::StatusOr<CommandRequest> BuildCommandRequest(RespCommand command,
                                                   std::uint8_t db_id);

struct ConnectionContext;

// Connection-level dispatch: intercepts MULTI/EXEC/DISCARD and queueing;
// everything else falls through to ExecuteCommand.
Task<CommandReply> DispatchCommand(ConnectionContext& ctx,
                                   CommandRequest request,
                                   ReplyBuilder& reply_builder);

// Unregisters every WATCH this connection holds (connection close, UNWATCH,
// DISCARD, and the end of every EXEC).
Task<absl::Status> ReleaseConnectionWatches(ConnectionContext& ctx);

// Bind command routing to the disk engine. Call once before the server starts.
void InitStorage(storage::StorageEngine* engine,
                 ReplicationManager* replication = nullptr);

// Builds an owned command for the current worker's replication journal. The
// storage mutation consumes it at the same ordering point that assigns the
// partition mutation sequence. Returns null for replayed commands or while
// the online replication log is inactive. An empty canonical_args vector
// means to journal the original request arguments.
std::optional<storage::ReplicationCommandAppend> PrepareReplicationCommand(
    const CommandRequest& request,
    std::vector<std::string> canonical_args = {});

void CaptureReplicationCommand(const CommandRequest& request,
                               std::vector<std::string> canonical_args);
void CaptureReplicationCommand(const CommandRequest& request,
                               std::uint8_t db_id,
                               std::vector<std::string> canonical_args);
void MarkReplicationCommandHandled(const CommandRequest& request);

// Encodes several deterministic write effects as one replication command.
// Standalone multi-key commands use the same envelope as replicated EXEC so
// all effects are replayed atomically by every participant flow.
std::vector<std::string> EncodeReplicationCommandEffects(
    std::vector<CapturedReplicationCommand> commands);

// Reserves one ordered replication marker on every shard participating in a
// standalone cross-key command. Construction may fail retained-memory
// admission, so callers must check status() before scheduling the transaction
// or entering storage. Destruction aborts an unresolved marker.
class ReplicationTransactionGuard {
 public:
  using ParticipantsEnteredHook = void (*)(void*) noexcept;

  // Additional participants carry no storage keys and therefore have no
  // transaction entry hook; the caller must enter each marker explicitly.
  ReplicationTransactionGuard(
      const CommandRequest& request, tx::Transaction* transaction,
      std::vector<std::string> canonical_args = {},
      std::vector<unsigned> additional_participants = {});
  ReplicationTransactionGuard(const CommandRequest& request,
                              std::vector<unsigned> participants,
                              std::vector<std::string> canonical_args = {});
  ReplicationTransactionGuard(const ReplicationTransactionGuard&) = delete;
  ReplicationTransactionGuard& operator=(const ReplicationTransactionGuard&) =
      delete;
  ~ReplicationTransactionGuard();

  // Returns false when a participant already made publication impossible.
  bool Commit() noexcept;
  // Replaces the canonical body while the transaction is pending. The method
  // obtains retained-memory headroom before growing the shared envelope, so a
  // caller can place this boundary before making its primary mutation visible.
  absl::Status TrySetCommandArgs(
      std::vector<std::string> canonical_args) noexcept;
  void SetCommandArgs(std::vector<std::string> canonical_args) noexcept;
  void SetFinalExpirations(
      std::span<const storage::TxShardWrites> shard_writes) noexcept;
  // Runs exactly once, on the worker that enqueues the final participant
  // marker. The hook must be nonblocking and remain alive until the first
  // transaction hop completes.
  void SetParticipantsEnteredHook(ParticipantsEnteredHook hook,
                                  void* context) noexcept;
  void EnterCurrentShard() noexcept;
  bool active() const noexcept { return transaction_ != nullptr; }
  const absl::Status& status() const noexcept { return status_; }

 private:
  void Initialize(const CommandRequest& request,
                  std::vector<unsigned> participants,
                  std::vector<std::string> canonical_args);
  void InvalidatePayload() noexcept;
  void EnterShard(unsigned shard_id) noexcept;
  static void EnterShardHook(void* context, unsigned shard_id);

  std::shared_ptr<storage::ReplicationTransaction> transaction_;
  std::atomic<unsigned> entered_participants_{0};
  ParticipantsEnteredHook participants_entered_hook_ = nullptr;
  void* participants_entered_context_ = nullptr;
  int uncaught_exceptions_ = 0;
  absl::Status status_ = absl::OkStatus();
};

// Static facts INFO reports. Call once before the server starts.
void SetServerInfo(std::string bind_ip, std::uint16_t port,
                   unsigned thread_count, std::string config_file);

// Connection accounting for INFO's Clients section.
void ConnectionOpened() noexcept;
void ConnectionClosed() noexcept;

// CLIENT metadata is worker-local. Replication socket handoff unregisters on
// the accepting worker and registers the same identity on the owning worker.
void RegisterClientConnection(std::uint64_t id, int fd, std::string address,
                              bool tls, bool replica = false,
                              std::uint64_t replication_session_id = 0);
void SetClientReplicationSession(std::uint64_t id,
                                 std::uint64_t replication_session_id) noexcept;
void SetClientName(std::uint64_t id, std::string name) noexcept;
void SetClientRespVersion(std::uint64_t id, RespVersion version) noexcept;
void SetClientPubSubCounts(std::uint64_t id, std::size_t subscriptions,
                           std::size_t pattern_subscriptions) noexcept;
void SetClientBlocked(std::uint64_t id, bool blocked) noexcept;
void UnregisterClientConnection(std::uint64_t id) noexcept;

// Commands marked kCmdMayBlock hold the database gate only while performing
// one concrete attempt. Their potentially unbounded wait must not prevent
// FLUSHDB from draining in-flight database operations.
bool TryBeginCommandDbOperation(std::uint8_t db_id) noexcept;
void EndCommandDbOperation(std::uint8_t db_id) noexcept;
// True unless a client write crossed a replication role transition after its
// publication decision. Replication-origin and read-only requests have no
// captured epoch and therefore remain valid.
bool CommandWriteAdmissionIsCurrent(const CommandRequest& request) noexcept;
bool CloseAllCommandDbGates() noexcept;
void OpenAllCommandDbGates() noexcept;
bool CommandDbOperationsActive() noexcept;

// Full-sync snapshot handoff uses the same sharded gate shape as FLUSHDB:
// commands update only their coordinator worker's counter, while the rare
// snapshot cut closes and scans every worker. These functions are internal to
// command dispatch and ReplicationManager.
bool TryBeginSnapshotTransaction() noexcept;
void EndSnapshotTransaction() noexcept;
bool CloseSnapshotTransactionGate() noexcept;
void OpenSnapshotTransactionGate() noexcept;
bool SnapshotTransactionsActive() noexcept;

// Replication flows send bounded batches, then wait for complete-event ACKs
// before advancing. Keep cross-flow transactions in one global source order
// so overlapping flow subsets cannot form an arrival/ACK cycle on the
// replica. Requests that are proven to touch at most one shard skip this gate
// (see RequestSpansMultipleShards): a single-flow marker never joins a
// cross-flow rendezvous cycle.
bool TryBeginReplicationTransactionOrder() noexcept;
void EndReplicationTransactionOrder() noexcept;

// Conservative admission test for the transaction order gate above: returns
// true (keep taking the gate) unless the command kind carries
// kCmdKeyViewComplete AND every key in this request's DetermineKeys view maps
// to one shard. Unknown commands, unproven key views, arity failures, and
// empty views all answer true; the gate fails safe, never unsafe.
bool RequestSpansMultipleShards(const CommandRequest& request);

// Route `request` to the worker owning its Redis hash-slot partition. Async
// disk operations use SubmitTaskTo and return on the connection's original
// worker.
Task<CommandReply> ExecuteCommand(CommandRequest& request,
                                  ReplyBuilder& reply_builder,
                                  std::uint64_t client_id = 0,
                                  ConnectionContext* connection = nullptr);

// RDB loaders validate complete FUNCTION2 catalogs before applying keys, then
// install the same source set at the dataset cut.
Task<absl::Status> ValidateLuaFunctionCatalog(
    const std::vector<std::string>& library_codes);
Task<absl::Status> ReplaceLuaFunctionCatalog(
    const std::vector<std::string>& library_codes);

// Replays one trusted canonical command from the native replication stream.
// Transaction envelopes rendezvous on every source flow before this primitive
// applies their ordered effects atomically on the replica.
Task<absl::Status> ApplyReplicatedCommand(const ReplicatedCommand& command);

// Replays Redis's ordinary single-connection replication stream. Redis emits
// raw FLUSH commands and groups MULTI/EXEC commands without Keylane's native
// epoch/envelope metadata, so these need a distinct trusted apply path.
Task<absl::Status> ApplyRedisReplicatedCommand(
    const ReplicatedCommand& command);
Task<absl::Status> ApplyRedisReplicatedTransaction(
    std::span<const ReplicatedCommand> commands);

}  // namespace keylane
