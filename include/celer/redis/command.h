#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "celer/base/status.h"
#include "celer/redis/db.h"
#include "celer/redis/resp.h"

namespace celer::redis {

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
  std::string name;
  std::vector<std::string> args;
};

struct CommandReply {
  std::string encoded;
  bool close_connection = false;
};

StatusOr<CommandRequest> BuildCommandRequest(RespCommand command);
CommandReply ExecuteCommand(DbShard* db, const CommandRequest& request);

}  // namespace celer::redis
