#include "keylane/command.h"

#include <cstdint>
#include <string>
#include <string_view>

#include "keylane/db.h"
#include "keylane/resp.h"

namespace keylane {
using namespace celer;

namespace {

bool CmpCaseInsensitive(std::string_view a, std::string_view b) {
  if (a.size() != b.size()) return false;
  for (std::size_t i = 0; i < a.size(); ++i) {
    unsigned char ca = static_cast<unsigned char>(a[i]);
    unsigned char cb = static_cast<unsigned char>(b[i]);
    if (ca >= 'A' && ca <= 'Z') ca += 'a' - 'A';
    if (cb >= 'A' && cb <= 'Z') cb += 'a' - 'A';
    if (ca != cb) return false;
  }
  return true;
}

CommandKind MatchCommandKind(std::string_view name) {
  switch (name.size()) {
    case 3:
      if (CmpCaseInsensitive(name, "GET")) return CommandKind::kGet;
      if (CmpCaseInsensitive(name, "SET")) return CommandKind::kSet;
      if (CmpCaseInsensitive(name, "DEL")) return CommandKind::kDel;
      break;
    case 4:
      if (CmpCaseInsensitive(name, "PING")) return CommandKind::kPing;
      if (CmpCaseInsensitive(name, "INCR")) return CommandKind::kIncr;
      break;
    case 6:
      if (CmpCaseInsensitive(name, "EXISTS")) return CommandKind::kExists;
      break;
  }
  return CommandKind::kUnknown;
}

}  // namespace

StatusOr<CommandRequest> BuildCommandRequest(RespCommand command) {
  if (command.args.empty()) {
    return Status(StatusCode::kInvalidArgument, "empty command");
  }

  CommandRequest request;
  request.kind = MatchCommandKind(command.args.front());
  request.args = std::move(command.args);
  return request;
}

CommandReply ExecuteCommand(DbShard* db, const CommandRequest& request) {
  CommandReply reply;
  const auto& args = request.args;

  switch (request.kind) {
    case CommandKind::kPing:
      if (args.size() == 1) {
        reply.encoded = EncodeSimpleString("PONG");
      } else if (args.size() == 2) {
        reply.encoded = EncodeBulkString(args[1]);
      } else {
        reply.encoded = EncodeError("ERR wrong number of arguments for 'ping' command");
      }
      return reply;

    case CommandKind::kDel:
      if (args.size() < 2) {
        reply.encoded = EncodeError("ERR wrong number of arguments for 'del' command");
        return reply;
      }
      {
        long long deleted = 0;
        for (std::size_t i = 1; i < args.size(); ++i) {
          deleted += db->Delete(args[i]) ? 1 : 0;
        }
        reply.encoded = EncodeInteger(deleted);
      }
      return reply;

    case CommandKind::kExists:
      if (args.size() < 2) {
        reply.encoded = EncodeError("ERR wrong number of arguments for 'exists' command");
        return reply;
      }
      {
        long long count = 0;
        for (std::size_t i = 1; i < args.size(); ++i) {
          count += db->Exists(args[i]) ? 1 : 0;
        }
        reply.encoded = EncodeInteger(count);
      }
      return reply;

    case CommandKind::kGet:
      if (args.size() != 2) {
        reply.encoded = EncodeError("ERR wrong number of arguments for 'get' command");
        return reply;
      }
      if (const StringValue* value = db->Get(args[1]); value != nullptr) {
        reply.encoded = EncodeBulkString(value->data);
      } else {
        reply.encoded = EncodeNullBulkString();
      }
      return reply;

    case CommandKind::kIncr:
      if (args.size() != 2) {
        reply.encoded = EncodeError("ERR wrong number of arguments for 'incr' command");
        return reply;
      }
      {
        std::int64_t value = 0;
        if (!db->Increment(args[1], &value)) {
          reply.encoded = EncodeError("ERR value is not an integer or out of range");
        } else {
          reply.encoded = EncodeInteger(value);
        }
      }
      return reply;

    case CommandKind::kSet:
      if (args.size() != 3) {
        reply.encoded = EncodeError("ERR wrong number of arguments for 'set' command");
        return reply;
      }
      db->Set(args[1], args[2]);
      reply.encoded = EncodeSimpleString("OK");
      return reply;

    case CommandKind::kUnknown:
    default:
      reply.encoded = EncodeError("ERR unknown command '" + args.front() + "'");
      return reply;
  }
}

}  // namespace keylane
