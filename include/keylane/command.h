#pragma once

#include <optional>
#include <string>
#include <vector>

#include "celer/base/status.h"
#include "celer/runtime/task.h"
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
  kGet,
  kIncr,
  kSet,
  kUnknown,
};

struct CommandRequest {
  CommandKind kind = CommandKind::kUnknown;
  std::vector<std::string> args;
};

struct CommandReply {
  std::string encoded;
  std::optional<storage::DiskValue> disk_value;
  bool close_connection = false;
};

StatusOr<CommandRequest> BuildCommandRequest(RespCommand command);

// Bind command routing to the disk engine. Call once before the server starts.
void InitStorage(storage::StorageEngine* engine);

// Route `request` to the worker owning its 64-slot storage shard. Async disk
// operations use SubmitTaskTo and return on the connection's original worker.
Task<CommandReply> ExecuteCommand(const CommandRequest& request);

}  // namespace keylane
