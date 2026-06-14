#include "keylane/command.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <vector>

#include "celer/runtime/cross_core.h"
#include "keylane/db.h"
#include "keylane/resp.h"

namespace keylane {
using namespace celer;

namespace {

// One shard per worker. Each g_shards[i] is touched only by worker i (directly
// when local, or via SubmitTo which runs on worker i) — so no locks.
std::vector<DbShard> g_shards;
unsigned g_num_shards = 0;

unsigned ShardForKey(std::string_view key) {
  return static_cast<unsigned>(std::hash<std::string_view>{}(key) % g_num_shards);
}

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

namespace {

// Runs a single-key command (or PING / unknown) on one shard's data. Returns the
// encoded reply by value — no pointer into the shard escapes its owning thread.
CommandReply ExecuteOnShard(DbShard& db, const CommandRequest& request) {
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

    case CommandKind::kGet:
      if (args.size() != 2) {
        reply.encoded = EncodeError("ERR wrong number of arguments for 'get' command");
        return reply;
      }
      if (const StringValue* value = db.Get(args[1]); value != nullptr) {
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
        if (!db.Increment(args[1], &value)) {
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
      db.Set(args[1], args[2]);
      reply.encoded = EncodeSimpleString("OK");
      return reply;

    case CommandKind::kDel:
    case CommandKind::kExists:
    case CommandKind::kUnknown:
    default:
      reply.encoded = EncodeError("ERR unknown command '" + args.front() + "'");
      return reply;
  }
}

// DEL / EXISTS may span shards, so route each key to its owner and combine.
Task<CommandReply> RouteMultiKey(const CommandRequest& request) {
  const auto& args = request.args;
  const bool is_del = request.kind == CommandKind::kDel;
  if (args.size() < 2) {
    co_return CommandReply{
        EncodeError(is_del ? "ERR wrong number of arguments for 'del' command"
                           : "ERR wrong number of arguments for 'exists' command"),
        false};
  }

  long long count = 0;
  for (std::size_t i = 1; i < args.size(); ++i) {
    const std::string_view key = args[i];
    const unsigned target = ShardForKey(key);
    bool hit = false;
    if (target == ThisWorker().id) {
      hit = is_del ? g_shards[target].Delete(key) : g_shards[target].Exists(key);
    } else {
      hit = co_await SubmitTo(target, [target, key, is_del] {
        return is_del ? g_shards[target].Delete(key) : g_shards[target].Exists(key);
      });
    }
    if (hit) {
      ++count;
    }
  }
  co_return CommandReply{EncodeInteger(count), false};
}

}  // namespace

void InitShards(unsigned num_shards) {
  g_num_shards = num_shards;
  g_shards = std::vector<DbShard>(num_shards);
}

Task<CommandReply> ExecuteCommand(const CommandRequest& request) {
  const auto& args = request.args;
  switch (request.kind) {
    case CommandKind::kDel:
    case CommandKind::kExists:
      co_return co_await RouteMultiKey(request);

    case CommandKind::kGet:
    case CommandKind::kSet:
    case CommandKind::kIncr:
      if (args.size() >= 2) {
        const unsigned target = ShardForKey(args[1]);
        if (target != ThisWorker().id) {
          co_return co_await SubmitTo(target, [&request, target] {
            return ExecuteOnShard(g_shards[target], request);
          });
        }
      }
      [[fallthrough]];

    default:  // PING, local single-key path, unknown
      co_return ExecuteOnShard(g_shards[ThisWorker().id], request);
  }
}

}  // namespace keylane
