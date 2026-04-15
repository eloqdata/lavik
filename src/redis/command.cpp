#include "celer/redis/command.h"

#include <cctype>
#include <cstdint>
#include <string>
#include <string_view>

namespace celer::redis {

namespace {

std::string ToUpperAscii(std::string_view input) {
  std::string out(input);
  for (char& ch : out) {
    ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
  }
  return out;
}

}  // namespace

StatusOr<CommandRequest> BuildCommandRequest(RespCommand command) {
  if (command.args.empty()) {
    return Status(StatusCode::kInvalidArgument, "empty command");
  }

  CommandRequest request;
  request.name = ToUpperAscii(command.args.front());
  request.args = std::move(command.args);

  if (request.name == "PING") {
    request.kind = CommandKind::kPing;
  } else if (request.name == "DEL") {
    request.kind = CommandKind::kDel;
  } else if (request.name == "EXISTS") {
    request.kind = CommandKind::kExists;
  } else if (request.name == "GET") {
    request.kind = CommandKind::kGet;
  } else if (request.name == "INCR") {
    request.kind = CommandKind::kIncr;
  } else if (request.name == "SET") {
    request.kind = CommandKind::kSet;
  } else {
    request.kind = CommandKind::kUnknown;
  }

  return request;
}

CommandReply ExecuteCommand(DbShard* db, const CommandRequest& request) {
  CommandReply reply;

  switch (request.kind) {
    case CommandKind::kPing:
      if (request.args.size() == 1) {
        reply.encoded = EncodeSimpleString("PONG");
      } else if (request.args.size() == 2) {
        reply.encoded = EncodeBulkString(request.args[1]);
      } else {
        reply.encoded = EncodeError("ERR wrong number of arguments for 'ping' command");
      }
      return reply;

    case CommandKind::kDel:
      if (request.args.size() < 2) {
        reply.encoded = EncodeError("ERR wrong number of arguments for 'del' command");
        return reply;
      }
      {
        long long deleted = 0;
        for (std::size_t i = 1; i < request.args.size(); ++i) {
          deleted += db->Delete(request.args[i]) ? 1 : 0;
        }
        reply.encoded = EncodeInteger(deleted);
      }
      return reply;

    case CommandKind::kExists:
      if (request.args.size() < 2) {
        reply.encoded = EncodeError("ERR wrong number of arguments for 'exists' command");
        return reply;
      }
      {
        long long exists = 0;
        for (std::size_t i = 1; i < request.args.size(); ++i) {
          exists += db->Exists(request.args[i]) ? 1 : 0;
        }
        reply.encoded = EncodeInteger(exists);
      }
      return reply;

    case CommandKind::kGet:
      if (request.args.size() != 2) {
        reply.encoded = EncodeError("ERR wrong number of arguments for 'get' command");
        return reply;
      }
      if (const StringValue* value = db->Get(request.args[1]); value != nullptr) {
        reply.encoded = EncodeBulkString(value->data);
      } else {
        reply.encoded = EncodeNullBulkString();
      }
      return reply;

    case CommandKind::kIncr:
      if (request.args.size() != 2) {
        reply.encoded = EncodeError("ERR wrong number of arguments for 'incr' command");
        return reply;
      }
      {
        std::int64_t value = 0;
        if (!db->Increment(request.args[1], &value)) {
          reply.encoded = EncodeError("ERR value is not an integer or out of range");
        } else {
          reply.encoded = EncodeInteger(value);
        }
      }
      return reply;

    case CommandKind::kSet:
      if (request.args.size() != 3) {
        reply.encoded = EncodeError("ERR wrong number of arguments for 'set' command");
        return reply;
      }
      db->Set(request.args[1], request.args[2]);
      reply.encoded = EncodeSimpleString("OK");
      return reply;

    case CommandKind::kUnknown:
    default:
      reply.encoded = EncodeError("ERR unknown command '" + request.args.front() + "'");
      return reply;
  }
}

}  // namespace celer::redis
