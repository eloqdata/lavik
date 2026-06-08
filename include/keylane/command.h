#pragma once

#include <string>
#include <vector>

#include "celer/base/status.h"

namespace keylane {

class DbShard;
struct RespCommand;

using celer::StatusOr;

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
