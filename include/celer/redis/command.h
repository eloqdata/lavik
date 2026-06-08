#pragma once

#include <string>
#include <vector>

#include "celer/base/status.h"
#include "celer/redis/db.h"
#include "celer/redis/resp.h"

namespace keylane {
using namespace celer;

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
CommandReply ExecuteCommand(DbShard* db, const CommandRequest& request);

}  // namespace keylane
