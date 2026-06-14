#pragma once

#include <string>
#include <vector>

#include "celer/base/status.h"
#include "celer/runtime/task.h"

namespace keylane {

struct RespCommand;

using celer::StatusOr;
using celer::Task;

enum class CommandKind {
  kPing,
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
  bool close_connection = false;
};

StatusOr<CommandRequest> BuildCommandRequest(RespCommand command);

// Create one shard (DbShard) per worker. Call once before the server starts.
void InitShards(unsigned num_shards);

// Route `request` to the shard that owns its key (hash(key) % num_shards),
// running it locally or via cross-core SubmitTo, and return the encoded reply.
Task<CommandReply> ExecuteCommand(const CommandRequest& request);

}  // namespace keylane
