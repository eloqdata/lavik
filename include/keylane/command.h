#pragma once

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
#include "keylane/set_trace.h"
#include "keylane/storage/engine.h"

namespace keylane {

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

enum class CommandKind {
  kPing,
  kEcho,
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
  kSelect,
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
  kReplicaOf,
  kConfig,
  kInfo,
  kCluster,
  kCommand,
  kReadOnly,
  kReadWrite,
  kKeys,
  kTombRaider,
  kDefrag,
  kUnknown,
  kCount,
};

struct CommandSpec;

struct CommandRequest {
  CommandKind kind_ = CommandKind::kUnknown;
  std::uint8_t db_id_ = 0;
  // Set only for commands applied from the replication stream. Such commands
  // bypass replica read-only checks and must not be published again.
  bool replication_origin_ = false;
  const CommandSpec* spec_ = nullptr;
  std::vector<std::string> args_;
  std::shared_ptr<ReplicationCommandCapture> replication_capture_;
};

struct ReplicaOfRequest {
  // Empty for REPLICAOF NO ONE; otherwise identifies the requested upstream.
  std::optional<std::string> host_;
  std::uint16_t port_ = 0;
};

// Parses REPLICAOF <host> <port> and REPLICAOF NO ONE without changing role.
// The replication backend will consume this request in a later change.
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

// Reserves one ordered replication marker on every shard participating in a
// standalone cross-key command. Destruction aborts an unresolved marker.
class ReplicationTransactionGuard {
 public:
  ReplicationTransactionGuard(const CommandRequest& request,
                              tx::Transaction* transaction,
                              std::vector<std::string> canonical_args = {});
  ReplicationTransactionGuard(const CommandRequest& request,
                              std::vector<unsigned> participants,
                              std::vector<std::string> canonical_args = {});
  ReplicationTransactionGuard(const ReplicationTransactionGuard&) = delete;
  ReplicationTransactionGuard& operator=(const ReplicationTransactionGuard&) =
      delete;
  ~ReplicationTransactionGuard();

  void Commit() noexcept;
  void SetCommandArgs(std::vector<std::string> canonical_args);
  void EnterCurrentShard() noexcept;
  bool active() const noexcept { return transaction_ != nullptr; }

 private:
  void Initialize(const CommandRequest& request,
                  std::vector<unsigned> participants,
                  std::vector<std::string> canonical_args);
  void EnterShard(unsigned shard_id) noexcept;
  static void EnterShardHook(void* context, unsigned shard_id);

  std::shared_ptr<storage::ReplicationTransaction> transaction_;
};

// Static facts INFO reports. Call once before the server starts.
void SetServerInfo(std::string bind_ip, std::uint16_t port,
                   unsigned thread_count);

// Connection accounting for INFO's Clients section.
void ConnectionOpened() noexcept;
void ConnectionClosed() noexcept;

// Commands marked kCmdMayBlock hold the database gate only while performing
// one concrete attempt. Their potentially unbounded wait must not prevent
// FLUSHDB from draining in-flight database operations.
bool TryBeginCommandDbOperation(std::uint8_t db_id) noexcept;
void EndCommandDbOperation(std::uint8_t db_id) noexcept;

// Full-sync snapshot handoff uses the same sharded gate shape as FLUSHDB:
// commands update only their coordinator worker's counter, while the rare
// snapshot cut closes and scans every worker. These functions are internal to
// command dispatch and ReplicationManager.
bool TryBeginSnapshotTransaction() noexcept;
void EndSnapshotTransaction() noexcept;
bool CloseSnapshotTransactionGate() noexcept;
void OpenSnapshotTransactionGate() noexcept;
bool SnapshotTransactionsActive() noexcept;

// Replication flows wait for an ACK before sending their next command. Keep
// cross-flow transactions in one global source order so overlapping flow
// subsets cannot form an arrival/ACK cycle on the replica.
bool TryBeginReplicationTransactionOrder() noexcept;
void EndReplicationTransactionOrder() noexcept;

// Route `request` to the worker owning its Redis hash-slot partition. Async
// disk operations use SubmitTaskTo and return on the connection's original
// worker.
Task<CommandReply> ExecuteCommand(const CommandRequest& request,
                                  ReplyBuilder& reply_builder);

// Replays one trusted canonical command from the native replication stream.
// Transaction envelopes rendezvous on every source flow before this primitive
// applies their ordered effects atomically on the replica.
Task<absl::Status> ApplyReplicatedCommand(const ReplicatedCommand& command);

}  // namespace keylane
