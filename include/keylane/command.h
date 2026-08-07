#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "celer/base/status.h"
#include "celer/runtime/task.h"
#include "keylane/read_trace.h"
#include "keylane/storage/engine.h"

namespace keylane {

struct RespCommand;

using celer::StatusOr;
using celer::Task;

enum class CommandKind {
  kPing,
  kEcho,
  kDbSize,
  kDel,
  kExists,
  kFlushDb,
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
  kMSet,
  kMGet,
  kMulti,
  kExec,
  kDiscard,
  kWatch,
  kUnwatch,
  kInfo,
  kKeys,
  kUnknown,
};

struct CommandSpec;

struct CommandRequest {
  CommandKind kind = CommandKind::kUnknown;
  std::uint8_t db_id = 0;
  const CommandSpec* spec = nullptr;
  std::vector<std::string> args;
};

// Pulls the next chunk of a streamed reply; an empty chunk ends the stream.
// Lets unbounded replies (KEYS) reach the socket in bounded memory.
using ReplyChunkSource =
    std::function<Task<StatusOr<std::string>>()>;

struct CommandReply {
  // TODO: Add a connection-local RESP reply builder for composite and small
  // replies. Keep DiskValue as the specialized direct-from-read-buffer GET
  // path.
  std::string encoded;
  std::optional<storage::DiskValue> disk_value;
  ReplyChunkSource chunks;  // drained after `encoded` when set
  bool close_connection = false;
  ReadLatencyTrace read_trace;
  std::optional<std::uint8_t> selected_db;
};

StatusOr<CommandRequest> BuildCommandRequest(RespCommand command,
                                             std::uint8_t db_id);

struct ConnectionContext;

// Connection-level dispatch: intercepts MULTI/EXEC/DISCARD and queueing;
// everything else falls through to ExecuteCommand.
Task<CommandReply> DispatchCommand(ConnectionContext& ctx,
                                   CommandRequest request);

// Unregisters every WATCH this connection holds (connection close, UNWATCH,
// DISCARD, and the end of every EXEC).
Task<celer::Status> ReleaseConnectionWatches(ConnectionContext& ctx);

// Bind command routing to the disk engine. Call once before the server starts.
void InitStorage(storage::StorageEngine* engine, bool replica_read_only = false);

// Static facts INFO reports. Call once before the server starts.
void SetServerInfo(std::uint16_t port, unsigned thread_count);

// Connection accounting for INFO's Clients section.
void ConnectionOpened() noexcept;
void ConnectionClosed() noexcept;

// Route `request` to the worker owning its Redis hash-slot partition. Async disk
// operations use SubmitTaskTo and return on the connection's original worker.
Task<CommandReply> ExecuteCommand(const CommandRequest& request);

}  // namespace keylane
