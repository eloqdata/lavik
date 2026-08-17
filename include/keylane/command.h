#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/statusor.h"
#include "celer/runtime/task.h"
#include "keylane/read_trace.h"
#include "keylane/storage/engine.h"

namespace keylane {

struct RespCommand;
class ReplyBuilder;

using celer::Task;

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
  kInfo,
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
  const CommandSpec* spec_ = nullptr;
  std::vector<std::string> args_;
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
                 bool replica_read_only = false);

// Static facts INFO reports. Call once before the server starts.
void SetServerInfo(std::uint16_t port, unsigned thread_count);

// Connection accounting for INFO's Clients section.
void ConnectionOpened() noexcept;
void ConnectionClosed() noexcept;

// Commands marked kCmdMayBlock hold the database gate only while performing
// one concrete attempt. Their potentially unbounded wait must not prevent
// FLUSHDB from draining in-flight database operations.
bool TryBeginCommandDbOperation(std::uint8_t db_id) noexcept;
void EndCommandDbOperation(std::uint8_t db_id) noexcept;

// Route `request` to the worker owning its Redis hash-slot partition. Async
// disk operations use SubmitTaskTo and return on the connection's original
// worker.
Task<CommandReply> ExecuteCommand(const CommandRequest& request,
                                  ReplyBuilder& reply_builder);

}  // namespace keylane
