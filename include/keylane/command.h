#pragma once

#include <cstdint>
#include <functional>
#include <optional>
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
  kExists,
  kFlushDb,
  kFlushAll,
  kGet,
  kStrlen,
  kIncr,
  kExpire,
  kPExpire,
  kPersist,
  kTtl,
  kPttl,
  kScan,
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
  kMSet,
  kMGet,
  kMulti,
  kExec,
  kDiscard,
  kWatch,
  kUnwatch,
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

// Route `request` to the worker owning its Redis hash-slot partition. Async
// disk operations use SubmitTaskTo and return on the connection's original
// worker.
Task<CommandReply> ExecuteCommand(const CommandRequest& request,
                                  ReplyBuilder& reply_builder);

}  // namespace keylane
