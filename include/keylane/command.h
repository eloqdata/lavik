#pragma once

#include <cstdint>
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
  kDbSize,
  kDel,
  kExists,
  kFlushDb,
  kGet,
  kIncr,
  kExpire,
  kPExpire,
  kPersist,
  kTtl,
  kPttl,
  kScan,
  kSelect,
  kSet,
  kUnknown,
};

struct CommandRequest {
  CommandKind kind = CommandKind::kUnknown;
  std::uint8_t db_id = 0;
  std::vector<std::string> args;
};

struct CommandReply {
  // TODO: Add a connection-local RESP reply builder for composite and small
  // replies. Keep DiskValue as the specialized direct-from-read-buffer GET
  // path.
  std::string encoded;
  std::optional<storage::DiskValue> disk_value;
  bool close_connection = false;
  ReadLatencyTrace read_trace;
  std::optional<std::uint8_t> selected_db;
};

StatusOr<CommandRequest> BuildCommandRequest(RespCommand command,
                                             std::uint8_t db_id);

// Bind command routing to the disk engine. Call once before the server starts.
void InitStorage(storage::StorageEngine* engine, bool replica_read_only = false);

// Route `request` to the worker owning its Redis hash-slot partition. Async disk
// operations use SubmitTaskTo and return on the connection's original worker.
Task<CommandReply> ExecuteCommand(const CommandRequest& request);

}  // namespace keylane
