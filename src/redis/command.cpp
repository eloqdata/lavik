#include "keylane/command.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

#include "celer/runtime/cross_core.h"
#include "keylane/resp.h"
#include "keylane/storage/engine.h"

namespace keylane {
using namespace celer;

namespace {

storage::StorageEngine* g_storage = nullptr;

unsigned ShardForKey(std::string_view key) {
  return g_storage->OwnerForKey(key);
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
      if (CmpCaseInsensitive(name, "DBSIZE")) return CommandKind::kDbSize;
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

CommandReply ExecuteLocalCommand(const CommandRequest& request) {
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

    case CommandKind::kDbSize:
    case CommandKind::kGet:
    case CommandKind::kIncr:
    case CommandKind::kSet:
    case CommandKind::kDel:
    case CommandKind::kExists:
    case CommandKind::kUnknown:
    default:
      reply.encoded = EncodeError("ERR unknown command '" + args.front() + "'");
      return reply;
  }
}

Task<CommandReply> ExecuteDbSize(const CommandRequest& request) {
  if (request.args.size() != 1) {
    co_return CommandReply{
        EncodeError("ERR wrong number of arguments for 'dbsize' command"),
        std::nullopt, false};
  }

  std::uint64_t total = 0;
  for (unsigned target = 0; target < g_storage->worker_count(); ++target) {
    const std::size_t local_size = co_await SubmitTo(target, [] {
      return g_storage->LocalSize();
    });
    if (local_size > std::numeric_limits<std::uint64_t>::max() - total) {
      co_return CommandReply{EncodeError("ERR db size overflow"),
                             std::nullopt, false};
    }
    total += local_size;
  }
  if (total > static_cast<std::uint64_t>(std::numeric_limits<long long>::max())) {
    co_return CommandReply{EncodeError("ERR db size exceeds RESP integer range"),
                           std::nullopt, false};
  }
  co_return CommandReply{EncodeInteger(static_cast<long long>(total)),
                         std::nullopt, false};
}

Task<CommandReply> ExecuteStorageCommand(const CommandRequest& request) {
  CommandReply reply;
  const auto& args = request.args;
  switch (request.kind) {
    case CommandKind::kGet: {
      if (args.size() != 2) {
        reply.encoded =
            EncodeError("ERR wrong number of arguments for 'get' command");
        co_return reply;
      }
      auto value = co_await g_storage->Get(args[1]);
      if (!value.ok()) {
        if (value.status().code() == StatusCode::kNotFound) {
          reply.encoded = EncodeNullBulkString();
        } else {
          reply.encoded = EncodeError("ERR " + value.status().message());
        }
      } else {
        reply.disk_value.emplace(std::move(*value));
      }
      co_return reply;
    }

    case CommandKind::kSet: {
      if (args.size() != 3) {
        reply.encoded =
            EncodeError("ERR wrong number of arguments for 'set' command");
        co_return reply;
      }
      Status status = co_await g_storage->Set(args[1], args[2]);
      reply.encoded = status.ok()
                          ? EncodeSimpleString("OK")
                          : EncodeError("ERR " + status.message());
      co_return reply;
    }

    case CommandKind::kIncr: {
      if (args.size() != 2) {
        reply.encoded =
            EncodeError("ERR wrong number of arguments for 'incr' command");
        co_return reply;
      }
      auto value = co_await g_storage->Increment(args[1]);
      if (!value.ok()) {
        reply.encoded = value.status().code() == StatusCode::kInvalidArgument
                            ? EncodeError(
                                  "ERR value is not an integer or out of range")
                            : EncodeError("ERR " + value.status().message());
      } else {
        reply.encoded = EncodeInteger(*value);
      }
      co_return reply;
    }

    default:
      co_return ExecuteLocalCommand(request);
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
        std::nullopt, false};
  }

  long long count = 0;
  for (std::size_t i = 1; i < args.size(); ++i) {
    const std::string_view key = args[i];
    const unsigned target = ShardForKey(key);
    bool hit = false;
    if (target == ThisWorker().id) {
      if (is_del) {
        auto deleted = co_await g_storage->Delete(key);
        if (!deleted.ok()) {
          co_return CommandReply{EncodeError("ERR " + deleted.status().message()),
                                 std::nullopt, false};
        }
        hit = *deleted;
      } else {
        hit = co_await g_storage->Exists(key);
      }
    } else {
      if (is_del) {
        auto deleted = co_await SubmitTaskTo(
            target, [key]() -> Task<StatusOr<bool>> {
              co_return co_await g_storage->Delete(key);
            });
        if (!deleted.ok()) {
          co_return CommandReply{EncodeError("ERR " + deleted.status().message()),
                                 std::nullopt, false};
        }
        hit = *deleted;
      } else {
        hit = co_await SubmitTaskTo(
            target, [key]() -> Task<bool> {
              co_return co_await g_storage->Exists(key);
            });
      }
    }
    if (hit) {
      ++count;
    }
  }
  co_return CommandReply{EncodeInteger(count), std::nullopt, false};
}

}  // namespace

void InitStorage(storage::StorageEngine* engine) {
  g_storage = engine;
}

Task<CommandReply> ExecuteCommand(const CommandRequest& request) {
  const auto& args = request.args;
  switch (request.kind) {
    case CommandKind::kDbSize:
      co_return co_await ExecuteDbSize(request);

    case CommandKind::kDel:
    case CommandKind::kExists:
      co_return co_await RouteMultiKey(request);

    case CommandKind::kGet:
    case CommandKind::kSet:
    case CommandKind::kIncr:
      if (args.size() >= 2) {
        const unsigned target = ShardForKey(args[1]);
        if (target != ThisWorker().id) {
          co_return co_await SubmitTaskTo(
              target, [&request]() -> Task<CommandReply> {
                co_return co_await ExecuteStorageCommand(request);
              });
        }
      }
      co_return co_await ExecuteStorageCommand(request);

    default:  // PING, local single-key path, unknown
      co_return ExecuteLocalCommand(request);
  }
}

}  // namespace keylane
